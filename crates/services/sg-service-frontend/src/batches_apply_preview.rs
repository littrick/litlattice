#![forbid(unsafe_code)]

//! `BatchSpec.applyPreview` and `BatchSpec.viewerBatchChangesCodeHosts`
//! (M9.29c).
//!
//! Ports the dry-run preview subset of
//! `cmd/frontend/internal/batches/resolvers/{changeset_apply_preview*.go,
//! batch_spec.go:115-163,290-330}` plus `internal/batches/rewirer.Rewire` and
//! `internal/batches/reconciler.DeterminePlan`. Nothing is written: the
//! rewirer works on in-memory values and `ReconcileBatchChange` only looks up
//! the target batch change.
//!
//! Reads go through the M9.26a [`BatchesSpecsDatabase`] seam (the new
//! `get_rewirer_mappings` / `changesets_for_apply` /
//! `list_batch_spec_repo_ids` / `code_hosts_for_repos` methods). No table,
//! column or Redis key is added.
//!
//! Deliberate boundaries (also documented in
//! `m9_29c_batches_apply_preview_overview.md`): `credential` on code hosts
//! stays `null` and `onlyWithoutCredential` therefore keeps every host (same
//! as the existing `Query.batchChangesCodeHosts`); `search` reuses the
//! best-effort `parse_text_search`; Go's unstable `sort.Slice` for
//! equal-precedence operations is approximated by a stable sort (only
//! observable for equal-precedence combinations, which the corpus avoids).

use std::collections::HashMap;
use std::sync::{Arc, OnceLock};

use sg_graphql::exec::{Actor, Arguments, ExecError, FieldValue, InputValue, ObjectValue};
use sg_graphql::json::Json;

use crate::batches_changeset_read::{BatchChangesCodeHostObject, CodeHostRow};
use crate::batches_graphql::{
    parse_text_search, unmarshal_relay_string, validate_first_param, BatchChangeFilter,
    BatchChangeLookup, BatchesHandle, ChangesetObject, ChangesetRow, PublishedValue,
    TextSearchTerm, DEFAULT_FIRST,
};
use crate::batches_specs_graphql::{
    ApplyChangesetRow, BatchChangeAssocRow, BatchSpecRow, BatchesSpecsDatabase,
    ChangesetSpecFilter, ChangesetSpecObject, ChangesetSpecRow, RewirerMappingRow,
};
use crate::graphql_resolvers::{PageInfoObject, RepositoryData};

/// Valid `ChangesetState` values (`btypes.ChangesetState.Valid`).
const CHANGESET_STATES: &[&str] = &[
    "UNPUBLISHED",
    "SCHEDULED",
    "PROCESSING",
    "OPEN",
    "DRAFT",
    "CLOSED",
    "MERGED",
    "DELETED",
    "READONLY",
    "RETRYING",
    "FAILED",
];

/// Valid `ChangesetSpecOperation` values (`btypes.ReconcilerOperation.Valid`).
const OPERATIONS: &[&str] = &[
    "PUSH",
    "UPDATE",
    "UNDRAFT",
    "PUBLISH",
    "PUBLISH_DRAFT",
    "SYNC",
    "IMPORT",
    "CLOSE",
    "REOPEN",
    "SLEEP",
    "DETACH",
    "ARCHIVE",
    "REATTACH",
];

fn specs(db: &BatchesHandle) -> Result<&dyn BatchesSpecsDatabase, String> {
    db.specs_database()
        .ok_or_else(|| "batch specs store is not configured".to_owned())
}

fn json_str(value: impl Into<String>) -> FieldValue {
    FieldValue::Json(Json::Str(value.into()))
}

fn json_i64(value: i64) -> FieldValue {
    FieldValue::Json(Json::Int(value))
}

fn json_bool(value: bool) -> FieldValue {
    FieldValue::Json(Json::Bool(value))
}

fn go_atoi(value: &str) -> Result<i64, ExecError> {
    value
        .parse::<i64>()
        .map_err(|_| ExecError::new(format!("strconv.Atoi: parsing {value:?}: invalid syntax")))
}

// ---------------------------------------------------------------------------
// Rewirer / planner model
// ---------------------------------------------------------------------------

/// One hydrated `btypes.RewirerMapping`.
#[derive(Clone)]
struct HydratedMapping {
    row: RewirerMappingRow,
    changeset_spec: Option<ChangesetSpecRow>,
    changeset: Option<ApplyChangesetRow>,
    repo: Option<RepositoryData>,
}

/// The rewirer's wanted changeset (only the planner-visible fields).
#[derive(Clone)]
struct WantedChangeset {
    owned_by_batch_change_id: i64,
    external_service_type: String,
    publication_state: String,
    external_state: String,
    current_spec_id: i64,
    previous_spec_id: i64,
    ui_publication_state: Option<String>,
    closing: bool,
    batch_changes: Vec<BatchChangeAssocRow>,
}

impl WantedChangeset {
    fn published(&self) -> bool {
        self.publication_state == "PUBLISHED"
    }

    fn unpublished(&self) -> bool {
        self.publication_state == "UNPUBLISHED"
    }

    fn attached_to(&self, batch_change_id: i64) -> bool {
        self.batch_changes
            .iter()
            .any(|assoc| assoc.batch_change_id == batch_change_id)
    }

    /// `btypes.Changeset.SupportsDraft`
    /// (`ExternalServiceSupports(..., DraftChangesets)`).
    fn supports_draft(&self) -> bool {
        matches!(
            self.external_service_type.as_str(),
            "github" | "gitlab" | "azuredevops" | "gerrit"
        )
    }
}

/// `btypes.ReconcilerOperation` (the string values).
type Operation = &'static str;

fn operation_precedence(op: &str) -> u8 {
    match op {
        "PUSH" | "DETACH" | "ARCHIVE" | "REATTACH" => 0,
        "IMPORT" | "PUBLISH" | "PUBLISH_DRAFT" | "CLOSE" => 1,
        "REOPEN" => 2,
        "UNDRAFT" => 3,
        "UPDATE" => 4,
        "SLEEP" => 5,
        "SYNC" => 6,
        _ => 7,
    }
}

/// `Operations.ExecutionOrder()`: de-duplicate, then sort by precedence.
fn execution_order(ops: &[Operation]) -> Vec<Operation> {
    let mut unique: Vec<Operation> = Vec::new();
    for op in ops {
        if !unique.contains(op) {
            unique.push(op);
        }
    }
    unique.sort_by_key(|op| operation_precedence(op));
    unique
}

/// `reconciler.ChangesetSpecDelta`.
#[derive(Debug, Clone, Copy, Default)]
pub struct ChangesetSpecDelta {
    pub title_changed: bool,
    pub body_changed: bool,
    pub undraft: bool,
    pub base_ref_changed: bool,
    pub diff_changed: bool,
    pub commit_message_changed: bool,
    pub author_name_changed: bool,
    pub author_email_changed: bool,
}

impl ChangesetSpecDelta {
    fn need_commit_update(&self) -> bool {
        self.diff_changed
            || self.commit_message_changed
            || self.author_name_changed
            || self.author_email_changed
    }

    fn need_code_host_update(&self) -> bool {
        self.title_changed || self.body_changed || self.base_ref_changed
    }

    fn attributes_changed(&self) -> bool {
        self.need_commit_update() || self.need_code_host_update()
    }
}

struct Plan {
    ops: Vec<Operation>,
    delta: ChangesetSpecDelta,
}

// ---------------------------------------------------------------------------
// Publication-state calculator
// ---------------------------------------------------------------------------

fn spec_published(spec: &ChangesetSpecRow) -> PublishedValue {
    match &spec.published {
        None | Some(serde_json::Value::Null) => PublishedValue::Null,
        Some(serde_json::Value::Bool(true)) => PublishedValue::Bool(true),
        Some(serde_json::Value::Bool(false)) => PublishedValue::Bool(false),
        Some(serde_json::Value::String(value)) if value == "draft" => PublishedValue::Draft,
        Some(_) => PublishedValue::Null,
    }
}

/// `calculatePublicationState` reduced to the two predicates the planner
/// reads.
struct PublicationState {
    published: bool,
    draft: bool,
}

fn calculate_publication_state(spec: &PublishedValue, ui: Option<&str>) -> PublicationState {
    PublicationState {
        published: matches!(spec, PublishedValue::Bool(true))
            || (matches!(spec, PublishedValue::Null) && ui == Some("PUBLISHED")),
        draft: matches!(spec, PublishedValue::Draft)
            || (matches!(spec, PublishedValue::Null) && ui == Some("DRAFT")),
    }
}

// ---------------------------------------------------------------------------
// rewirer.Rewire (single mapping)
// ---------------------------------------------------------------------------

/// `Changeset.Attach(batchChangeID)`: noop when attached (clearing the flags),
/// otherwise append a fresh association.
fn attach_assoc(wanted: &mut WantedChangeset, batch_change_id: i64) {
    if let Some(assoc) = wanted
        .batch_changes
        .iter_mut()
        .find(|assoc| assoc.batch_change_id == batch_change_id)
    {
        assoc.detach = false;
        assoc.is_archived = false;
        assoc.archive = false;
        return;
    }
    wanted.batch_changes.push(BatchChangeAssocRow {
        batch_change_id,
        detach: false,
        archive: false,
        is_archived: false,
    });
    wanted
        .batch_changes
        .sort_by_key(|assoc| assoc.batch_change_id);
}

fn wanted_from_changeset(changeset: &ApplyChangesetRow) -> WantedChangeset {
    WantedChangeset {
        owned_by_batch_change_id: changeset.owned_by_batch_change_id,
        external_service_type: changeset.external_service_type.clone(),
        publication_state: changeset.publication_state.clone(),
        external_state: changeset.external_state.clone(),
        current_spec_id: changeset.current_spec_id,
        previous_spec_id: changeset.previous_spec_id,
        ui_publication_state: changeset.ui_publication_state.clone(),
        closing: changeset.closing,
        batch_changes: changeset.batch_changes.clone(),
    }
}

/// `updateChangesetToNewSpec`.
fn update_changeset_to_new_spec(
    changeset: &ApplyChangesetRow,
    spec: &ChangesetSpecRow,
    batch_change_id: i64,
) -> WantedChangeset {
    let mut wanted = wanted_from_changeset(changeset);
    if changeset.reconciler_state == "COMPLETED" {
        wanted.previous_spec_id = changeset.current_spec_id;
    }
    wanted.current_spec_id = spec.id;
    attach_assoc(&mut wanted, batch_change_id);
    // `ResetReconcilerState(QUEUED)` is not readable by the preview.
    wanted
}

/// `attachTrackingChangeset`.
fn attach_tracking_changeset(
    changeset: &ApplyChangesetRow,
    batch_change_id: i64,
) -> WantedChangeset {
    let mut wanted = wanted_from_changeset(changeset);
    attach_assoc(&mut wanted, batch_change_id);
    wanted
}

/// `createTrackingChangeset`: merely track the existing changeset, with no
/// current spec.
fn create_tracking_changeset(repo: &RepositoryData, batch_change_id: i64) -> WantedChangeset {
    WantedChangeset {
        owned_by_batch_change_id: 0,
        external_service_type: repo.external_service_type.clone(),
        publication_state: "UNPUBLISHED".to_owned(),
        external_state: String::new(),
        current_spec_id: 0,
        previous_spec_id: 0,
        ui_publication_state: None,
        closing: false,
        batch_changes: vec![BatchChangeAssocRow {
            batch_change_id,
            detach: false,
            archive: false,
            is_archived: false,
        }],
    }
}

/// `createChangesetForSpec`.
fn create_changeset_for_spec(
    spec: &ChangesetSpecRow,
    repo: &RepositoryData,
    batch_change_id: i64,
) -> WantedChangeset {
    WantedChangeset {
        owned_by_batch_change_id: batch_change_id,
        external_service_type: repo.external_service_type.clone(),
        publication_state: "UNPUBLISHED".to_owned(),
        external_state: String::new(),
        current_spec_id: spec.id,
        previous_spec_id: 0,
        ui_publication_state: None,
        closing: false,
        batch_changes: vec![BatchChangeAssocRow {
            batch_change_id,
            detach: false,
            archive: false,
            is_archived: false,
        }],
    }
}

/// `closeChangeset`.
fn close_changeset(changeset: &ApplyChangesetRow, batch_change_id: i64) -> WantedChangeset {
    let mut wanted = wanted_from_changeset(changeset);
    if changeset.current_spec_id != 0
        && changeset.owned_by_batch_change_id == batch_change_id
        && changeset.published()
    {
        // Store the current spec also as the previous spec (completed only).
        if changeset.reconciler_state == "COMPLETED" {
            wanted.previous_spec_id = changeset.current_spec_id;
        }
        if !changeset.archived_in(batch_change_id) {
            if let Some(assoc) = wanted
                .batch_changes
                .iter_mut()
                .find(|assoc| assoc.batch_change_id == batch_change_id)
            {
                assoc.archive = true;
            }
            if changeset.closeable() {
                wanted.closing = true;
            }
        }
    } else if let Some(assoc) = wanted
        .batch_changes
        .iter_mut()
        .find(|assoc| assoc.batch_change_id == batch_change_id)
    {
        assoc.detach = true;
    }
    wanted
}

/// `rewirer.New([]mapping, batchChangeID).Rewire()` for one mapping. `None`
/// means the Go rewirer skips the mapping (so the preview reports
/// `rewirer did not return changeset`).
fn rewire_one(
    mapping: &HydratedMapping,
    batch_change_id: i64,
) -> Result<Option<WantedChangeset>, String> {
    let Some(spec) = mapping.changeset_spec.as_ref() else {
        let Some(changeset) = mapping.changeset.as_ref() else {
            return Ok(None);
        };
        if mapping.repo.is_none() {
            return Ok(None);
        }
        if !changeset.attached_to(batch_change_id) {
            return Ok(None);
        }
        return Ok(Some(close_changeset(changeset, batch_change_id)));
    };

    let Some(repo) = mapping.repo.as_ref() else {
        return Err(format!("repo not found: id={}", mapping.row.repo_id));
    };
    if !crate::batches_graphql_store::SUPPORTED_CODE_HOSTS
        .contains(&repo.external_service_type.as_str())
    {
        return Err(format!(
            "Code host type {} of repository {:?} is currently not supported for use with \
             Batch Changes",
            repo.external_service_type, repo.name
        ));
    }

    match mapping.changeset.as_ref() {
        Some(changeset) => {
            if spec.kind == "existing" {
                Ok(Some(attach_tracking_changeset(changeset, batch_change_id)))
            } else {
                Ok(Some(update_changeset_to_new_spec(
                    changeset,
                    spec,
                    batch_change_id,
                )))
            }
        }
        None => {
            if spec.kind == "existing" {
                Ok(Some(create_tracking_changeset(repo, batch_change_id)))
            } else {
                Ok(Some(create_changeset_for_spec(spec, repo, batch_change_id)))
            }
        }
    }
}

// ---------------------------------------------------------------------------
// reconciler.DeterminePlan
// ---------------------------------------------------------------------------

fn compare_specs(
    previous: Option<&ChangesetSpecRow>,
    current: &ChangesetSpecRow,
    ui: Option<&str>,
) -> ChangesetSpecDelta {
    let Some(previous) = previous else {
        return ChangesetSpecDelta::default();
    };
    let mut delta = ChangesetSpecDelta::default();
    if previous.title != current.title {
        delta.title_changed = true;
    }
    if previous.body != current.body {
        delta.body_changed = true;
    }
    if previous.base_ref != current.base_ref {
        delta.base_ref_changed = true;
    }
    let previous_calc = calculate_publication_state(&spec_published(previous), ui);
    let current_calc = calculate_publication_state(&spec_published(current), ui);
    if previous_calc.draft && current_calc.published {
        delta.undraft = true;
    }
    if previous.diff != current.diff {
        delta.diff_changed = true;
    }
    if previous.commit_message != current.commit_message {
        delta.commit_message_changed = true;
    }
    if previous.commit_author_name != current.commit_author_name {
        delta.author_name_changed = true;
    }
    if previous.commit_author_email != current.commit_author_email {
        delta.author_email_changed = true;
    }
    delta
}

fn reopen_after_detach(wanted: &WantedChangeset) -> bool {
    let closed = matches!(wanted.external_state.as_str(), "CLOSED" | "READONLY");
    if !closed {
        return false;
    }
    if wanted.owned_by_batch_change_id == 0 {
        return false;
    }
    if wanted.closing {
        return false;
    }
    wanted.attached_to(wanted.owned_by_batch_change_id)
}

fn determine_plan(
    previous_spec: Option<&ChangesetSpecRow>,
    current_spec: Option<&ChangesetSpecRow>,
    current_changeset: Option<&ApplyChangesetRow>,
    wanted: &WantedChangeset,
) -> Result<Plan, String> {
    let mut ops: Vec<Operation> = Vec::new();
    let mut want_detach = false;
    let mut want_archive = false;
    let mut is_archived = false;
    let mut is_still_attached = false;
    let mut is_reattach = false;
    let mut want_detach_from_owner = false;
    for assoc in &wanted.batch_changes {
        if assoc.detach {
            want_detach = true;
            if assoc.batch_change_id == wanted.owned_by_batch_change_id {
                want_detach_from_owner = true;
            }
        } else if assoc.archive
            && assoc.batch_change_id == wanted.owned_by_batch_change_id
            && wanted.published()
        {
            want_archive = !assoc.is_archived;
            is_archived = assoc.is_archived;
        } else if current_changeset.is_some_and(|changeset| changeset.batch_changes.is_empty()) {
            is_reattach = true;
        } else {
            is_still_attached = true;
        }
    }
    if want_detach {
        ops = vec!["DETACH"];
    }
    if want_archive {
        ops = vec!["ARCHIVE"];
    }

    if wanted.closing {
        if wanted.external_state != "READONLY" {
            ops.push("CLOSE");
        }
        // Close is a final operation.
        return Ok(Plan {
            ops,
            delta: ChangesetSpecDelta::default(),
        });
    } else if want_detach_from_owner || want_archive || is_archived {
        return Ok(Plan {
            ops,
            delta: ChangesetSpecDelta::default(),
        });
    }

    // Without a current spec this is an imported changeset.
    let Some(current_spec) = current_spec else {
        if wanted.unpublished() && is_still_attached {
            ops.push("IMPORT");
        } else if is_reattach && !want_detach {
            ops.push("REATTACH");
        }
        return Ok(Plan {
            ops,
            delta: ChangesetSpecDelta::default(),
        });
    };

    if previous_spec.is_some() && is_reattach && !want_detach {
        ops.push("REATTACH");
    }

    let delta = compare_specs(previous_spec, current_spec, wanted.ui_publication_state.as_deref());

    match wanted.publication_state.as_str() {
        "UNPUBLISHED" => {
            let calc = calculate_publication_state(
                &spec_published(current_spec),
                wanted.ui_publication_state.as_deref(),
            );
            if calc.published {
                ops = vec!["PUBLISH", "PUSH"];
            } else if calc.draft && wanted.supports_draft() {
                ops = vec!["PUBLISH_DRAFT", "PUSH"];
            }
        }
        "PUBLISHED" => {
            if matches!(wanted.external_state.as_str(), "MERGED" | "READONLY") {
                return Ok(Plan { ops, delta });
            }
            if reopen_after_detach(wanted) {
                ops = vec!["REOPEN"];
            }
            if wanted.supports_draft() {
                let calc = calculate_publication_state(
                    &spec_published(current_spec),
                    wanted.ui_publication_state.as_deref(),
                );
                if delta.undraft || (calc.published && wanted.external_state == "DRAFT") {
                    ops.push("UNDRAFT");
                }
            }
            if delta.attributes_changed() {
                if delta.need_commit_update() {
                    ops.push("PUSH");
                }
                if delta.need_code_host_update() {
                    ops.push("UPDATE");
                } else {
                    ops.push("SLEEP");
                    ops.push("SYNC");
                }
            }
        }
        other => {
            return Err(format!("unknown changeset publication state: {other}"));
        }
    }

    Ok(Plan { ops, delta })
}

// ---------------------------------------------------------------------------
// BatchSpec field entry point
// ---------------------------------------------------------------------------

pub fn batch_spec_field(
    db: &BatchesHandle,
    row: &BatchSpecRow,
    field: &str,
    arguments: &Arguments,
    actor: Actor,
) -> Result<Option<FieldValue>, ExecError> {
    match field {
        "applyPreview" => Ok(Some(FieldValue::Object(Arc::new(
            ApplyPreviewConnectionObject::new(db.clone(), row.clone(), arguments, actor)?,
        )))),
        "viewerBatchChangesCodeHosts" => Ok(Some(FieldValue::Object(Arc::new(
            ViewerCodeHostConnectionObject::new(db.clone(), row.clone(), arguments, actor)?,
        )))),
        _ => Ok(None),
    }
}

// ---------------------------------------------------------------------------
// applyPreview connection
// ---------------------------------------------------------------------------

struct ApplyPreviewOpts {
    first: i64,
    offset: i64,
    action: Option<&'static str>,
    current_state: Option<&'static str>,
    search: Vec<TextSearchTerm>,
    publication_states: HashMap<String, PublishedValue>,
}

#[derive(Clone)]
struct PreviewMappingPage {
    mappings: Vec<HydratedMapping>,
    total_count: i64,
}

type PageKey = (i64, i64, Option<&'static str>);

pub struct ApplyPreviewConnectionObject {
    db: BatchesHandle,
    row: BatchSpecRow,
    opts: ApplyPreviewOpts,
    actor: Actor,
    batch_change_id: OnceLock<Result<i64, String>>,
    all: OnceLock<Result<Vec<HydratedMapping>, String>>,
    pages: std::sync::Mutex<HashMap<PageKey, PreviewMappingPage>>,
}

impl ApplyPreviewConnectionObject {
    fn new(
        db: BatchesHandle,
        row: BatchSpecRow,
        arguments: &Arguments,
        actor: Actor,
    ) -> Result<Self, ExecError> {
        let first = arguments.i64("first").unwrap_or(DEFAULT_FIRST);
        validate_first_param(first).map_err(ExecError::new)?;
        let offset = match arguments.string("after") {
            Some(after) => go_atoi(after)?,
            None => 0,
        };
        let current_state = match arguments.string("currentState") {
            Some(state) => {
                let Some(known) = CHANGESET_STATES.iter().find(|known| **known == state) else {
                    return Err(ExecError::new(format!("invalid currentState {state:?}")));
                };
                Some(*known)
            }
            None => None,
        };
        let action = match arguments.string("action") {
            Some(action) => {
                let Some(known) = OPERATIONS.iter().find(|known| **known == action) else {
                    return Err(ExecError::new(format!("invalid action {action:?}")));
                };
                Some(*known)
            }
            None => None,
        };
        let search = match arguments.string("search") {
            Some(search) => parse_text_search(search)
                .map_err(|error| ExecError::new(format!("parsing search: {error}")))?,
            None => Vec::new(),
        };
        let publication_states = parse_publication_states(arguments.get("publicationStates"))?;
        Ok(Self {
            db,
            row,
            opts: ApplyPreviewOpts {
                first,
                offset,
                action,
                current_state,
                search,
                publication_states,
            },
            actor,
            batch_change_id: OnceLock::new(),
            all: OnceLock::new(),
            pages: std::sync::Mutex::new(HashMap::new()),
        })
    }

    /// `service.ReconcileBatchChange` reduced to the matching batch change ID
    /// (`GetBatchChangeMatchingBatchSpec`; 0 when there is no match).
    fn batch_change_id(&self) -> Result<i64, String> {
        self.batch_change_id
            .get_or_init(|| self.compute_batch_change_id())
            .clone()
    }

    fn compute_batch_change_id(&self) -> Result<i64, String> {
        let filter = if self.row.batch_change_id != 0 {
            BatchChangeFilter {
                id: self.row.batch_change_id,
                ..BatchChangeFilter::default()
            }
        } else {
            BatchChangeFilter {
                name: Some(self.row.name.clone()),
                namespace_user_id: self.row.namespace_user_id,
                namespace_org_id: self.row.namespace_org_id,
                ..BatchChangeFilter::default()
            }
        };
        match self.db.get_batch_change(&filter)? {
            BatchChangeLookup::Found(row) => Ok(row.id),
            BatchChangeLookup::NotFound => Ok(0),
            BatchChangeLookup::DeletedNamespace => Err("namespace has been deleted".to_owned()),
        }
    }

    fn all(&self) -> Result<&Vec<HydratedMapping>, String> {
        self.all
            .get_or_init(|| self.compute())
            .as_ref()
            .map_err(Clone::clone)
    }

    fn compute(&self) -> Result<Vec<HydratedMapping>, String> {
        let db = specs(&self.db)?;
        let batch_change_id = self.batch_change_id()?;

        // `GetRewirerMappings` without LimitOffset: the Go facade rebuilds the
        // opts and drops pagination before calling the store.
        let rows = db.get_rewirer_mappings(
            self.row.id,
            batch_change_id,
            &self.opts.search,
            self.opts.current_state,
        )?;

        let mut spec_ids: Vec<i64> = rows
            .iter()
            .map(|row| row.changeset_spec_id)
            .filter(|id| *id != 0)
            .collect();
        spec_ids.sort_unstable();
        spec_ids.dedup();
        let mut specs_by_id: HashMap<i64, ChangesetSpecRow> = HashMap::new();
        if !spec_ids.is_empty() {
            let filter = ChangesetSpecFilter {
                ids: spec_ids,
                ..ChangesetSpecFilter::default()
            };
            for spec in db.list_changeset_specs(&filter)?.rows {
                specs_by_id.insert(spec.id, spec);
            }
        }

        let mut changeset_ids: Vec<i64> = rows
            .iter()
            .map(|row| row.changeset_id)
            .filter(|id| *id != 0)
            .collect();
        changeset_ids.sort_unstable();
        changeset_ids.dedup();
        let mut changesets_by_id: HashMap<i64, ApplyChangesetRow> = HashMap::new();
        if !changeset_ids.is_empty() {
            for changeset in db.changesets_for_apply(&changeset_ids)? {
                changesets_by_id.insert(changeset.id, changeset);
            }
        }

        let mut repo_ids: Vec<i32> = rows.iter().map(|row| row.repo_id).collect();
        repo_ids.sort_unstable();
        repo_ids.dedup();
        let repos = self.db.repos_by_ids(&repo_ids, self.actor)?;

        Ok(rows
            .into_iter()
            .map(|row| HydratedMapping {
                changeset_spec: specs_by_id.get(&row.changeset_spec_id).cloned(),
                changeset: changesets_by_id.get(&row.changeset_id).cloned(),
                repo: repos.get(&row.repo_id).cloned(),
                row,
            })
            .collect())
    }

    fn preview_node(&self, mapping: HydratedMapping, batch_change_id: i64) -> PreviewNode {
        PreviewNode {
            db: self.db.clone(),
            mapping,
            batch_change_id,
            publication_states: self.opts.publication_states.clone(),
            plan: OnceLock::new(),
        }
    }

    fn page(&self) -> Result<PreviewMappingPage, String> {
        let key = (self.opts.first, self.opts.offset, self.opts.action);
        if let Some(page) = self.pages.lock().expect("pages lock").get(&key) {
            return Ok(page.clone());
        }

        let all = self.all()?;
        let batch_change_id = self.batch_change_id()?;
        let filtered: Vec<HydratedMapping> = match self.opts.action {
            Some(action) => {
                let mut filtered = Vec::new();
                for mapping in all {
                    // Hidden nodes never perform operations.
                    if mapping.repo.is_none() {
                        continue;
                    }
                    let node = self.preview_node(mapping.clone(), batch_change_id);
                    if node.plan()?.ops.contains(&action) {
                        filtered.push(mapping.clone());
                    }
                }
                filtered
            }
            None => all.clone(),
        };

        let len = filtered.len() as i64;
        let page = if self.opts.first < 0 || self.opts.offset < 0 || self.opts.offset > len {
            Vec::new()
        } else if self.opts.first == 0 {
            filtered[self.opts.offset as usize..].to_vec()
        } else {
            let end = self.opts.first + self.opts.offset;
            if end > len {
                filtered[self.opts.offset as usize..].to_vec()
            } else {
                filtered[self.opts.offset as usize..end as usize].to_vec()
            }
        };
        let page = PreviewMappingPage {
            mappings: page,
            total_count: filtered.len() as i64,
        };
        self.pages
            .lock()
            .expect("pages lock")
            .insert(key, page.clone());
        Ok(page)
    }

    fn stats(&self) -> Result<ApplyPreviewStats, String> {
        let all = self.all()?;
        let batch_change_id = self.batch_change_id()?;
        let mut stats = ApplyPreviewStats::default();
        for mapping in all {
            if mapping.repo.is_none() {
                continue;
            }
            let node = self.preview_node(mapping.clone(), batch_change_id);
            let ops = execution_order(&node.plan()?.ops);
            match target_kind(mapping) {
                TargetKind::Attach => stats.added += 1,
                TargetKind::Update => {
                    let attached = mapping
                        .changeset
                        .as_ref()
                        .map(|changeset| changeset.batch_changes.len())
                        .unwrap_or(0);
                    if !ops.is_empty() && attached > 0 {
                        stats.modified += 1;
                    } else if attached == 0 {
                        stats.added += 1;
                    }
                }
                TargetKind::Detach => stats.removed += 1,
            }
            for op in ops {
                match op {
                    "PUSH" => stats.push += 1,
                    "UPDATE" => stats.update += 1,
                    "UNDRAFT" => stats.undraft += 1,
                    "PUBLISH" => stats.publish += 1,
                    "PUBLISH_DRAFT" => stats.publish_draft += 1,
                    "SYNC" => stats.sync += 1,
                    "IMPORT" => stats.import += 1,
                    "CLOSE" => stats.close += 1,
                    "REOPEN" => stats.reopen += 1,
                    "SLEEP" => stats.sleep += 1,
                    "DETACH" => stats.detach += 1,
                    "ARCHIVE" => stats.archive += 1,
                    "REATTACH" => stats.reattach += 1,
                    _ => {}
                }
            }
        }
        Ok(stats)
    }
}

impl ObjectValue for ApplyPreviewConnectionObject {
    fn type_name(&self) -> &'static str {
        "ChangesetApplyPreviewConnection"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "nodes" => {
                let page = self.page().map_err(ExecError::new)?;
                let batch_change_id = self.batch_change_id().map_err(ExecError::new)?;
                Ok(Some(FieldValue::List {
                    items: page
                        .mappings
                        .iter()
                        .map(|mapping| {
                            FieldValue::Object(Arc::new(
                                self.preview_node(mapping.clone(), batch_change_id),
                            ))
                        })
                        .collect(),
                    element_type: Some("ChangesetApplyPreview"),
                }))
            }
            "totalCount" => {
                let page = self.page().map_err(ExecError::new)?;
                Ok(Some(json_i64(page.total_count)))
            }
            "pageInfo" => {
                // Go quirk: `PageInfo` uses `len(mappings.All)`, not the
                // filtered page.
                let all = self.all().map_err(ExecError::new)?;
                let idx = self.opts.first + self.opts.offset;
                let (has_next, cursor) = if idx < all.len() as i64 {
                    (true, Some(idx.to_string()))
                } else {
                    (false, None)
                };
                Ok(Some(FieldValue::Object(Arc::new(PageInfoObject::new(
                    has_next, cursor,
                )))))
            }
            "stats" => Ok(Some(FieldValue::Object(Arc::new(ApplyPreviewStatsObject {
                stats: self.stats().map_err(ExecError::new)?,
            })))),
            _ => Ok(None),
        }
    }
}

fn parse_publication_states(
    input: Option<&InputValue>,
) -> Result<HashMap<String, PublishedValue>, ExecError> {
    let mut out = HashMap::new();
    let items = match input {
        None | Some(InputValue::Null) => return Ok(out),
        Some(InputValue::List(items)) => items,
        Some(_) => return Ok(out),
    };
    let mut errors: Vec<String> = Vec::new();
    for item in items {
        let InputValue::Object(entry) = item else {
            errors.push("malformed changeset spec ID \"\"".to_owned());
            continue;
        };
        let Some(InputValue::Str(raw)) = entry.get("changesetSpec") else {
            errors.push("malformed changeset spec ID \"\"".to_owned());
            continue;
        };
        let rand_id = match unmarshal_relay_string(raw) {
            Ok(rand_id) => rand_id,
            Err(error) => {
                errors.push(format!("malformed changeset spec ID {raw:?}: {error}"));
                continue;
            }
        };
        let value = match entry.get("publicationState") {
            None | Some(InputValue::Null) => PublishedValue::Null,
            Some(InputValue::Bool(value)) => PublishedValue::Bool(*value),
            Some(InputValue::Str(value)) if value == "draft" => PublishedValue::Draft,
            Some(_) => PublishedValue::Null,
        };
        if out.contains_key(&rand_id) {
            errors.push(format!("duplicate changeset spec ID {raw:?}"));
            continue;
        }
        out.insert(rand_id, value);
    }
    if !errors.is_empty() {
        return Err(ExecError::new(crate::batches_mutations::join_multi_errors(
            &errors,
        )));
    }
    Ok(out)
}

// ---------------------------------------------------------------------------
// One ChangesetApplyPreview node
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, PartialEq, Eq)]
enum TargetKind {
    Attach,
    Update,
    Detach,
}

fn target_kind(mapping: &HydratedMapping) -> TargetKind {
    if mapping.changeset.is_none() {
        TargetKind::Attach
    } else if mapping.changeset_spec.is_none() {
        TargetKind::Detach
    } else {
        TargetKind::Update
    }
}

struct PreviewNode {
    db: BatchesHandle,
    mapping: HydratedMapping,
    batch_change_id: i64,
    publication_states: HashMap<String, PublishedValue>,
    plan: OnceLock<Result<Plan, String>>,
}

impl PreviewNode {
    fn plan(&self) -> Result<&Plan, String> {
        self.plan
            .get_or_init(|| self.compute_plan())
            .as_ref()
            .map_err(Clone::clone)
    }

    /// `visibleChangesetApplyPreviewResolver.computePlan`.
    fn compute_plan(&self) -> Result<Plan, String> {
        let db = specs(&self.db)?;
        let wanted = rewire_one(&self.mapping, self.batch_change_id)?
            .ok_or_else(|| "rewirer did not return changeset".to_owned())?;
        let mut wanted = wanted;

        if let Some(spec) = self.mapping.changeset_spec.as_ref() {
            if let Some(state) = self.publication_states.get(&spec.rand_id) {
                if !matches!(spec_published(spec), PublishedValue::Null) {
                    return Err(format!(
                        "changeset spec {:?} has the published field set in its spec",
                        spec.rand_id
                    ));
                }
                wanted.ui_publication_state =
                    state.ui_publication_state().map(str::to_owned);
            }
        }

        let previous_spec = if wanted.previous_spec_id != 0 {
            db.get_changeset_spec(wanted.previous_spec_id, "")?
        } else {
            None
        };
        let current_spec = if wanted.current_spec_id != 0 {
            if let Some(spec) = self.mapping.changeset_spec.as_ref() {
                Some(spec.clone())
            } else {
                db.get_changeset_spec(wanted.current_spec_id, "")?
            }
        } else {
            None
        };

        determine_plan(
            previous_spec.as_ref(),
            current_spec.as_ref(),
            self.mapping.changeset.as_ref(),
            &wanted,
        )
    }
}

impl ObjectValue for PreviewNode {
    fn type_name(&self) -> &'static str {
        if self.mapping.repo.is_some() {
            "VisibleChangesetApplyPreview"
        } else {
            "HiddenChangesetApplyPreview"
        }
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "operations" => {
                let ops: Vec<String> = if self.mapping.repo.is_none() {
                    Vec::new()
                } else {
                    execution_order(&self.plan().map_err(ExecError::new)?.ops)
                        .into_iter()
                        .map(str::to_owned)
                        .collect()
                };
                Ok(Some(FieldValue::List {
                    items: ops.into_iter().map(json_str).collect(),
                    element_type: Some("ChangesetSpecOperation"),
                }))
            }
            "delta" => {
                let delta = if self.mapping.repo.is_none() {
                    ChangesetSpecDelta::default()
                } else {
                    self.plan().map_err(ExecError::new)?.delta
                };
                Ok(Some(FieldValue::Object(Arc::new(
                    ApplyPreviewDeltaObject { delta },
                ))))
            }
            "targets" => Ok(Some(FieldValue::Object(Arc::new(
                ApplyPreviewTargetsObject {
                    db: self.db.clone(),
                    mapping: self.mapping.clone(),
                },
            )))),
            _ => Ok(None),
        }
    }
}

struct ApplyPreviewDeltaObject {
    delta: ChangesetSpecDelta,
}

impl ObjectValue for ApplyPreviewDeltaObject {
    fn type_name(&self) -> &'static str {
        "ChangesetSpecDelta"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        let value = match field {
            "titleChanged" => self.delta.title_changed,
            "bodyChanged" => self.delta.body_changed,
            "undraft" => self.delta.undraft,
            "baseRefChanged" => self.delta.base_ref_changed,
            "diffChanged" => self.delta.diff_changed,
            "commitMessageChanged" => self.delta.commit_message_changed,
            "authorNameChanged" => self.delta.author_name_changed,
            "authorEmailChanged" => self.delta.author_email_changed,
            _ => return Ok(None),
        };
        Ok(Some(json_bool(value)))
    }
}

struct ApplyPreviewTargetsObject {
    db: BatchesHandle,
    mapping: HydratedMapping,
}

impl ApplyPreviewTargetsObject {
    fn kind(&self) -> TargetKind {
        target_kind(&self.mapping)
    }

    fn changeset_field(&self) -> Option<FieldValue> {
        self.mapping.changeset.as_ref().map(|changeset| {
            FieldValue::Object(Arc::new(ChangesetObject::new(
                changeset_row(changeset),
                self.db.clone(),
                self.mapping.repo.clone(),
            )))
        })
    }

    fn changeset_spec_field(&self) -> Option<FieldValue> {
        self.mapping.changeset_spec.as_ref().map(|spec| {
            FieldValue::Object(Arc::new(ChangesetSpecObject::new(
                spec.clone(),
                self.db.clone(),
                self.mapping.repo.clone(),
            )))
        })
    }
}

impl ObjectValue for ApplyPreviewTargetsObject {
    fn type_name(&self) -> &'static str {
        let visible = self.mapping.repo.is_some();
        match self.kind() {
            TargetKind::Attach => {
                if visible {
                    "VisibleApplyPreviewTargetsAttach"
                } else {
                    "HiddenApplyPreviewTargetsAttach"
                }
            }
            TargetKind::Update => {
                if visible {
                    "VisibleApplyPreviewTargetsUpdate"
                } else {
                    "HiddenApplyPreviewTargetsUpdate"
                }
            }
            TargetKind::Detach => {
                if visible {
                    "VisibleApplyPreviewTargetsDetach"
                } else {
                    "HiddenApplyPreviewTargetsDetach"
                }
            }
        }
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "changeset" => Ok(Some(
                self.changeset_field().unwrap_or(FieldValue::Null),
            )),
            "changesetSpec" => Ok(Some(
                self.changeset_spec_field().unwrap_or(FieldValue::Null),
            )),
            _ => Ok(None),
        }
    }
}

/// Build the read-model `ChangesetRow` for a target changeset. Fields the
/// preview planner does not read carry Go zero values.
fn changeset_row(row: &ApplyChangesetRow) -> ChangesetRow {
    ChangesetRow {
        id: row.id,
        repo_id: row.repo_id,
        created_at: row.created_at,
        updated_at: row.updated_at,
        metadata: serde_json::json!({}),
        external_id: row.external_id.clone(),
        external_service_type: row.external_service_type.clone(),
        external_deleted_at: None,
        external_updated_at: None,
        external_state: row.external_state.clone(),
        external_review_state: String::new(),
        external_check_state: String::new(),
        diff_stat_added: None,
        diff_stat_deleted: None,
        owned_by_batch_change_id: row.owned_by_batch_change_id,
        current_spec_id: row.current_spec_id,
        publication_state: row.publication_state.clone(),
        computed_state: row.computed_state.clone(),
        failure_message: None,
        previous_failure_message: None,
        syncer_error: None,
        external_fork_name: String::new(),
        external_fork_namespace: String::new(),
    }
}

// ---------------------------------------------------------------------------
// ChangesetApplyPreviewConnectionStats
// ---------------------------------------------------------------------------

#[derive(Default)]
struct ApplyPreviewStats {
    push: i64,
    update: i64,
    undraft: i64,
    publish: i64,
    publish_draft: i64,
    sync: i64,
    import: i64,
    close: i64,
    reopen: i64,
    sleep: i64,
    detach: i64,
    archive: i64,
    reattach: i64,
    added: i64,
    modified: i64,
    removed: i64,
}

struct ApplyPreviewStatsObject {
    stats: ApplyPreviewStats,
}

impl ObjectValue for ApplyPreviewStatsObject {
    fn type_name(&self) -> &'static str {
        "ChangesetApplyPreviewConnectionStats"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        let stats = &self.stats;
        let value = match field {
            "push" => stats.push,
            "update" => stats.update,
            "undraft" => stats.undraft,
            "publish" => stats.publish,
            "publishDraft" => stats.publish_draft,
            "sync" => stats.sync,
            "import" => stats.import,
            "close" => stats.close,
            "reopen" => stats.reopen,
            "sleep" => stats.sleep,
            "detach" => stats.detach,
            "archive" => stats.archive,
            "reattach" => stats.reattach,
            "added" => stats.added,
            "modified" => stats.modified,
            "removed" => stats.removed,
            _ => return Ok(None),
        };
        Ok(Some(json_i64(value)))
    }
}

// ---------------------------------------------------------------------------
// viewerBatchChangesCodeHosts
// ---------------------------------------------------------------------------

pub struct ViewerCodeHostConnectionObject {
    db: BatchesHandle,
    row: BatchSpecRow,
    first: i64,
    offset: i64,
    only_without_webhooks: bool,
    only_without_credential: bool,
    actor: Actor,
    hosts: OnceLock<Result<Vec<CodeHostRow>, String>>,
    credentials: OnceLock<
        Result<
            std::collections::HashMap<
                (String, String),
                crate::batches_credentials::ResolvedCredential,
            >,
            String,
        >,
    >,
}

impl ViewerCodeHostConnectionObject {
    fn new(
        db: BatchesHandle,
        row: BatchSpecRow,
        arguments: &Arguments,
        actor: Actor,
    ) -> Result<Self, ExecError> {
        if !actor.is_authenticated() {
            return Err(ExecError::new("not authenticated"));
        }
        let first = arguments.i64("first").unwrap_or(DEFAULT_FIRST);
        validate_first_param(first).map_err(ExecError::new)?;
        let offset = match arguments.string("after") {
            Some(after) => go_atoi(after)?,
            None => 0,
        };
        Ok(Self {
            db,
            row,
            first,
            offset,
            only_without_webhooks: arguments.bool("onlyWithoutWebhooks").unwrap_or(false),
            only_without_credential: arguments.bool("onlyWithoutCredential").unwrap_or(false),
            actor,
            hosts: OnceLock::new(),
            credentials: OnceLock::new(),
        })
    }

    fn credentials(
        &self,
    ) -> Result<
        &std::collections::HashMap<
            (String, String),
            crate::batches_credentials::ResolvedCredential,
        >,
        String,
    > {
        self.credentials
            .get_or_init(|| {
                crate::batches_credentials::resolve_credentials(&self.db, self.actor.uid())
            })
            .as_ref()
            .map_err(Clone::clone)
    }

    fn hosts(&self) -> Result<&Vec<CodeHostRow>, String> {
        self.hosts
            .get_or_init(|| self.compute())
            .as_ref()
            .map_err(Clone::clone)
    }

    fn compute(&self) -> Result<Vec<CodeHostRow>, String> {
        let db = specs(&self.db)?;
        let repo_ids = db.list_batch_spec_repo_ids(self.row.id, self.actor)?;
        if repo_ids.is_empty() {
            return Ok(Vec::new());
        }
        let hosts = db.code_hosts_for_repos(&repo_ids, self.only_without_webhooks)?;
        if !self.only_without_credential {
            return Ok(hosts);
        }
        let credentials = self.credentials()?;
        Ok(hosts
            .into_iter()
            .filter(|host| {
                !credentials.contains_key(&(
                    host.external_service_type.clone(),
                    host.external_service_id.clone(),
                ))
            })
            .collect())
    }

    fn page(&self) -> Result<&[CodeHostRow], String> {
        let all = self.hosts()?;
        let len = all.len() as i64;
        if self.offset >= len {
            return Ok(&[]);
        }
        if self.first <= 0 {
            return Ok(&all[self.offset as usize..]);
        }
        let mut limit = self.first;
        if limit + self.offset >= len {
            limit = len - self.offset;
        }
        Ok(&all[self.offset as usize..(limit + self.offset) as usize])
    }
}

impl ObjectValue for ViewerCodeHostConnectionObject {
    fn type_name(&self) -> &'static str {
        "BatchChangesCodeHostConnection"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "nodes" => {
                let credentials = self.credentials().map_err(ExecError::new)?;
                Ok(Some(FieldValue::List {
                    items: self
                        .page()
                        .map_err(ExecError::new)?
                        .iter()
                        .map(|host| {
                            let credential = credentials
                                .get(&(
                                    host.external_service_type.clone(),
                                    host.external_service_id.clone(),
                                ))
                                .cloned();
                            FieldValue::Object(Arc::new(
                                BatchChangesCodeHostObject::with_credential(
                                    self.db.clone(),
                                    host.clone(),
                                    credential,
                                ),
                            ))
                        })
                        .collect(),
                    element_type: Some("BatchChangesCodeHost"),
                }))
            }
            "totalCount" => Ok(Some(json_i64(
                self.hosts().map_err(ExecError::new)?.len() as i64
            ))),
            "pageInfo" => {
                let all = self.hosts().map_err(ExecError::new)?;
                let idx = self.first + self.offset;
                let (has_next, cursor) = if idx < all.len() as i64 {
                    (true, Some(idx.to_string()))
                } else {
                    (false, None)
                };
                Ok(Some(FieldValue::Object(Arc::new(PageInfoObject::new(
                    has_next, cursor,
                )))))
            }
            _ => Ok(None),
        }
    }
}
