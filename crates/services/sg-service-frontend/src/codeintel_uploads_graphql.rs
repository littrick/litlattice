#![forbid(unsafe_code)]

//! Code Intel uploads GraphQL read-only surface (M9.27d + M9.29d).
//!
//! Ports `internal/codeintel/uploads/transport/graphql`:
//!
//! - `Query.preciseIndexes` (`root_resolver_index_queries.go:26`) including
//!   `first`/`after` cursors, `repo`, `query`, `states` bifurcation,
//!   `indexerKey` expansion, `dependencyOf`/`dependentOf` CTEs,
//!   `includeDeleted` audit-log union and the upload/index merge;
//! - the `PreciseIndex` read-only fields (`precise_index_resolver.go`),
//!   including `auditLogs`, and the associated-record dataloader pre-submits
//!   (`dataloader.go`);
//! - `Query.codeIntelSummary` (`root_resolver_coverage.go`);
//! - `IndexSteps` / execution-log entries (`index_steps_resolver.go`);
//! - indexer metadata (`indexer_resolver.go`,
//!   `internal/codeintel/uploads/shared/indexers2.go`);
//! - state mapping (`util_states.go`) and Relay IDs (`util_identifiers.go`).
//!
//! The database is injected through [`CodeIntelUploadsDatabase`], a synchronous
//! seam in the same style as the batches resolver; the production adapter
//! ([`crate::codeintel_uploads_graphql_store`]) bridges to sqlx with
//! `block_in_place`. No table, column or Redis key is added.
//!
//! Deviations (also recorded in
//! `dev/rs/docs/overviews/m9_29d_codeintel_uploads_remaining_overview.md`):
//! - `projectRoot`/`tags` need gitserver and return `null`/`[]`;
//! - `retentionPolicyOverview` resolves as an empty connection;
//! - `Repository.codeIntelSummary` is not implemented;
//! - repository-permission filtering reproduces the internal-actor
//!   `AuthzQueryConds` (the differential oracle uses an internal actor).

use std::collections::HashMap;
use std::sync::Arc;

use base64::Engine as _;
use sg_graphql::exec::{Actor, Arguments, ExecError, FieldValue, InputValue, ObjectValue};
use sg_graphql::json::Json;
use time::OffsetDateTime;

use crate::graphql_resolvers::{datetime_rfc3339, unmarshal_relay_id, PageInfoObject};

/// `DefaultPageSize` (`root_resolver_index_queries.go:21`).
pub const DEFAULT_PAGE_SIZE: i32 = 50;

pub type CodeIntelUploadsHandle = Arc<dyn CodeIntelUploadsDatabase + Send + Sync>;

impl std::fmt::Debug for dyn CodeIntelUploadsDatabase + Send + Sync {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("CodeIntelUploadsDatabase")
    }
}

// ---------------------------------------------------------------------------
// Rows
// ---------------------------------------------------------------------------

/// `shared.Upload`, reduced to the columns `PreciseIndexes`/`PreciseIndex`
/// read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UploadRow {
    pub id: i64,
    pub commit: String,
    pub root: String,
    pub visible_at_tip: bool,
    pub uploaded_at: OffsetDateTime,
    pub state: String,
    pub failure_message: Option<String>,
    pub started_at: Option<OffsetDateTime>,
    pub finished_at: Option<OffsetDateTime>,
    pub repository_id: i32,
    pub indexer: String,
    pub associated_index_id: Option<i64>,
    pub should_reindex: bool,
    pub rank: Option<i32>,
}

/// `executor.ExecutionLogEntry`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutionLogEntry {
    pub key: String,
    pub command: Vec<String>,
    pub exit_code: Option<i32>,
    pub start_time: OffsetDateTime,
    pub duration_ms: Option<i32>,
}

/// `shared.DockerStep`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DockerStep {
    pub root: String,
    pub image: String,
    pub commands: Vec<String>,
}

/// `shared.AutoIndexJob`, reduced to the columns read by the surface.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AutoIndexJobRow {
    pub id: i64,
    pub commit: String,
    pub queued_at: OffsetDateTime,
    pub state: String,
    pub failure_message: Option<String>,
    pub started_at: Option<OffsetDateTime>,
    pub finished_at: Option<OffsetDateTime>,
    pub repository_id: i32,
    pub docker_steps: Vec<DockerStep>,
    pub root: String,
    pub indexer: String,
    pub indexer_args: Vec<String>,
    pub outfile: String,
    pub execution_logs: Vec<ExecutionLogEntry>,
    pub rank: Option<i32>,
    pub local_steps: Vec<String>,
    pub associated_upload_id: Option<i64>,
    pub should_reindex: bool,
    pub requested_env_vars: Vec<String>,
}

/// `shared.GetUploadsOptions` subset used by `PreciseIndexes`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct GetUploadsOptions {
    pub repository_id: i32,
    pub states: Vec<String>,
    pub term: String,
    pub indexer_names: Vec<String>,
    pub dependency_of: i64,
    pub dependent_of: i64,
    pub allow_deleted_upload: bool,
    pub limit: i32,
    pub offset: i32,
}

/// One `LSIFUploadAuditLog` (`shared.UploadLog`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuditLogRow {
    pub log_timestamp: OffsetDateTime,
    pub record_deleted_at: Option<OffsetDateTime>,
    pub upload_id: i32,
    pub commit: String,
    pub root: String,
    pub uploaded_at: OffsetDateTime,
    pub indexer: String,
    /// `{"column": ..., "old": ..., "new": ...}` per transition.
    pub transition_columns: Vec<serde_json::Value>,
    pub reason: Option<String>,
    pub operation: String,
}

/// `shared.RepositoryWithCount` (`repositoriesWithErrors`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RepositoryWithCountRow {
    pub repository_id: i32,
    pub repository_name: String,
    pub count: i64,
}

/// `shared.RepositoryWithAvailableIndexers` (`repositoriesWithConfiguration`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RepositoryWithConfigurationRow {
    pub repository_id: i32,
    pub repository_name: String,
    /// Raw `available_indexers` jsonb text.
    pub available_indexers: String,
}

/// `policiesshared.RetentionPolicyMatchCandidate` (`precise_index_resolver.go`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetentionPolicyMatchRow {
    /// `nil` only for the implicit tip-of-default-branch match.
    pub policy: Option<sg_db::codeintel_policies::ConfigurationPolicy>,
    /// `RetentionPolicyMatchCandidate.Matched`.
    pub matched: bool,
    /// `nil` for a policy that matched the upload commit directly; `Some`
    /// (possibly empty) for a visible-commit match.
    pub protecting_commits: Option<Vec<String>>,
}

/// The recent-activity rows of `Repository.codeIntelSummary`
/// (`GetRecentUploadsSummary` + `GetRecentAutoIndexJobsSummary`).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RecentActivityRows {
    pub uploads: Vec<UploadRow>,
    pub indexes: Vec<AutoIndexJobRow>,
}

/// `shared.GetAutoIndexJobsOptions` subset used by `PreciseIndexes`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct GetAutoIndexJobsOptions {
    pub repository_id: i32,
    pub states: Vec<String>,
    pub term: String,
    pub indexer_names: Vec<String>,
    pub without_upload: bool,
    pub limit: i32,
    pub offset: i32,
}

/// `shared.DeleteUploadsOptions` (`DeletePreciseIndexes`).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DeleteUploadsOptions {
    pub repository_id: i32,
    pub states: Vec<String>,
    pub indexer_names: Vec<String>,
    pub term: String,
    pub visible_at_tip: bool,
}

/// `shared.DeleteAutoIndexJobsOptions` (`DeletePreciseIndexes`).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DeleteAutoIndexJobsOptions {
    pub repository_id: i32,
    pub states: Vec<String>,
    pub indexer_names: Vec<String>,
    pub term: String,
    pub without_upload: bool,
}

/// `shared.ReindexUploadsOptions` (`ReindexPreciseIndexes`).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ReindexUploadsOptions {
    pub repository_id: i32,
    pub states: Vec<String>,
    pub indexer_names: Vec<String>,
    pub term: String,
    pub visible_at_tip: bool,
}

/// `shared.SetRerunAutoIndexJobsOptions` (`ReindexPreciseIndexes`).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SetRerunAutoIndexJobsOptions {
    pub repository_id: i32,
    pub states: Vec<String>,
    pub indexer_names: Vec<String>,
    pub term: String,
    pub without_upload: bool,
}

// ---------------------------------------------------------------------------
// Database seam
// ---------------------------------------------------------------------------

pub trait CodeIntelUploadsDatabase: Send + Sync {
    /// `store.GetUploads` (uploads half of `PreciseIndexes`), with
    /// `database.AuthzQueryConds` for the request actor.
    fn get_uploads(
        &self,
        opts: &GetUploadsOptions,
        actor: Actor,
    ) -> Result<(Vec<UploadRow>, i64), String>;
    /// `store.GetAutoIndexJobs` (`WithoutUpload: true`), with authz.
    fn get_auto_index_jobs(
        &self,
        opts: &GetAutoIndexJobsOptions,
        actor: Actor,
    ) -> Result<(Vec<AutoIndexJobRow>, i64), String>;
    /// `store.GetUploadsByIDs` (dataloader backfill), with authz.
    fn get_uploads_by_ids(&self, ids: &[i64], actor: Actor) -> Result<Vec<UploadRow>, String>;
    /// `store.GetAutoIndexJobsByIDs` (dataloader backfill), with authz.
    fn get_auto_index_jobs_by_ids(
        &self,
        ids: &[i64],
        actor: Actor,
    ) -> Result<Vec<AutoIndexJobRow>, String>;
    /// `store.GetUploadByID` (single-row `PreciseIndexByID`), with authz.
    fn get_upload_by_id(&self, id: i64, actor: Actor) -> Result<Option<UploadRow>, String>;
    /// `store.GetAutoIndexJobByID` (single-row `PreciseIndexByID`), with authz.
    fn get_auto_index_job_by_id(
        &self,
        id: i64,
        actor: Actor,
    ) -> Result<Option<AutoIndexJobRow>, String>;
    /// `store.DeleteUploadByID`.
    fn delete_upload_by_id(&self, id: i64) -> Result<(), String>;
    /// `store.DeleteAutoIndexJobByID`.
    fn delete_auto_index_job_by_id(&self, id: i64) -> Result<(), String>;
    /// `store.DeleteUploads`.
    fn delete_uploads(&self, opts: &DeleteUploadsOptions) -> Result<(), String>;
    /// `store.DeleteAutoIndexJobs`.
    fn delete_auto_index_jobs(&self, opts: &DeleteAutoIndexJobsOptions) -> Result<(), String>;
    /// `store.ReindexUploadByID`.
    fn reindex_upload_by_id(&self, id: i64) -> Result<(), String>;
    /// `store.SetRerunAutoIndexJobByID`.
    fn set_rerun_auto_index_job_by_id(&self, id: i64) -> Result<(), String>;
    /// `store.ReindexUploads`.
    fn reindex_uploads(&self, opts: &ReindexUploadsOptions) -> Result<(), String>;
    /// `store.SetRerunAutoIndexJobs`.
    fn set_rerun_auto_index_jobs(
        &self,
        opts: &SetRerunAutoIndexJobsOptions,
    ) -> Result<(), String>;
    /// `store.GetIndexers` (`Query.indexerKeys`); the repository-scoped query
    /// applies `database.AuthzQueryConds`, the global one does not.
    fn get_indexer_names(&self, repository_id: i32, actor: Actor)
        -> Result<Vec<String>, String>;
    /// `store.GetAuditLogsForUpload` (`PreciseIndex.auditLogs`).
    fn get_audit_logs_for_upload(&self, upload_id: i32) -> Result<Vec<AuditLogRow>, String>;
    /// `store.NumRepositoriesWithCodeIntelligence` (`Query.codeIntelSummary`).
    fn num_repositories_with_code_intelligence(&self) -> Result<i64, String>;
    /// `store.RepositoryIDsWithErrors` (`Query.codeIntelSummary`).
    fn repository_ids_with_errors(
        &self,
        offset: i64,
        limit: i64,
    ) -> Result<(Vec<RepositoryWithCountRow>, i64), String>;
    /// `autoindexing store.RepositoryIDsWithConfiguration`.
    fn repository_ids_with_configuration(
        &self,
        offset: i64,
        limit: i64,
    ) -> Result<(Vec<RepositoryWithConfigurationRow>, i64), String>;

    // -- M9.30d: `projectRoot` / `tags` / `retentionPolicyOverview` /
    //    `Repository.codeIntelSummary` -------------------------------------

    /// `repoStore.Get(id).Name` (`nil` when the repository is unknown).
    fn repository_name(&self, repository_id: i32) -> Result<Option<String>, String>;
    /// `gitserver.ResolveRevision(EnsureRevision: false)` via the injected
    /// [`crate::git_graphql::GitObjectSource`]. `None` is the baseline's
    /// `RevisionNotFoundError → (nil, nil)`; sources that are not injected
    /// behave like an unknown revision.
    fn resolve_revision(
        &self,
        repo_id: i32,
        repo_name: &str,
        rev: &str,
    ) -> Result<Option<String>, String>;
    /// `gitserver.ListRefs(TagsOnly: true, PointsAtCommit: [oid])`, returning
    /// `ShortName`s. An empty result is `[]`, never `None`.
    fn list_tags_at_commit(
        &self,
        repo_id: i32,
        repo_name: &str,
        oid: &str,
    ) -> Result<Vec<String>, String>;
    /// `policies.Service.GetRetentionPolicyOverview` (with its gitserver
    /// matcher). Returns the candidates in the baseline's sort order.
    fn retention_policy_overview(
        &self,
        upload: &UploadRow,
        matches_only: bool,
        first: i64,
        after: i64,
        term: &str,
    ) -> Result<Vec<RetentionPolicyMatchRow>, String>;
    /// `autoindexing store.GetLastIndexScanForRepository`.
    fn last_index_scan(&self, repository_id: i32) -> Result<Option<OffsetDateTime>, String>;
    /// `store.GetLastUploadRetentionScanForRepository`.
    fn last_upload_retention_scan(
        &self,
        repository_id: i32,
    ) -> Result<Option<OffsetDateTime>, String>;
    /// `store.GetRecentUploadsSummary` + `GetRecentAutoIndexJobsSummary`
    /// (grouped, then flattened; the caller sorts by id).
    fn recent_activity(
        &self,
        repository_id: i32,
        actor: Actor,
    ) -> Result<RecentActivityRows, String>;
    /// `conf.CodeIntelAutoIndexingEnabled`.
    fn auto_indexing_enabled(&self) -> bool {
        false
    }
}

// ---------------------------------------------------------------------------
// Relay IDs / state mapping
// ---------------------------------------------------------------------------

/// `relay.MarshalID("PreciseIndex", "<U:id>:<I:id>")`.
pub fn marshal_precise_index_id(upload_id: Option<i64>, index_id: Option<i64>) -> String {
    let mut parts: Vec<String> = Vec::new();
    if let Some(id) = upload_id {
        parts.push(format!("U:{id}"));
    }
    if let Some(id) = index_id {
        parts.push(format!("I:{id}"));
    }
    let spec = parts.join(":");
    // `relay.MarshalID` base64-encodes `Kind:` + `json.Marshal(spec)`, so the
    // spec string is JSON-quoted inside the payload.
    let quoted = serde_json::to_string(&spec).expect("precise index spec");
    base64::engine::general_purpose::URL_SAFE.encode(format!("PreciseIndex:{quoted}"))
}

/// `UnmarshalPreciseIndexGQLID` (`util_identifiers.go:11`).
///
/// Mirrors the two nested `errors.Wrap(err, "unexpected precise index ID")`
/// calls: `unmarshalRawPreciseIndexGQLID` wraps a `relay.UnmarshalSpec`
/// failure once, and `UnmarshalPreciseIndexGQLID` wraps the result again.
/// Zero ids are normalized to `None` (the Go code branches on `!= 0`).
pub fn decode_precise_index_id(id: &str) -> Result<(Option<i64>, Option<i64>), String> {
    let raw = unmarshal_raw_precise_index_id(id)
        .map_err(|error| format!("unexpected precise index ID: {error}"))?;
    if raw.0.unwrap_or(0) == 0 && raw.1.unwrap_or(0) == 0 {
        return Err(format!(
            "unexpected precise index ID: invalid precise index id {id:?}"
        ));
    }
    Ok(raw)
}

const EXPECTED_PAIRS: &str = "expected pairs of `U:<id>`, `I:<id>`";

fn unmarshal_raw_precise_index_id(
    id: &str,
) -> Result<(Option<i64>, Option<i64>), String> {
    let decoded = base64::engine::general_purpose::URL_SAFE
        .decode(id)
        .map_err(|error| format!("unexpected precise index ID: {error}"))?;
    let text = String::from_utf8_lossy(&decoded);
    let Some((_kind, payload)) = text.split_once(':') else {
        return Err("unexpected precise index ID: invalid graphql.ID".to_owned());
    };
    // `relay.UnmarshalSpec` JSON-decodes the payload into a string.
    let payload: String = serde_json::from_str(payload)
        .map_err(|error| format!("unexpected precise index ID: {error}"))?;
    let parts: Vec<&str> = payload.split(':').collect();
    if parts.len() % 2 != 0 {
        return Err(EXPECTED_PAIRS.to_owned());
    }
    let (mut upload_id, mut index_id) = (None, None);
    let mut i = 0;
    while i + 1 < parts.len() {
        let value: i64 = parts[i + 1].parse().map_err(|_| EXPECTED_PAIRS.to_owned())?;
        match parts[i] {
            "U" => upload_id = (value != 0).then_some(value),
            "I" => index_id = (value != 0).then_some(value),
            _ => return Err(EXPECTED_PAIRS.to_owned()),
        }
        i += 2;
    }
    Ok((upload_id, index_id))
}

/// `bifurcateStates` (`util_states.go:7`).
pub fn bifurcate_states(
    states: &[String],
) -> Result<(Vec<String>, Vec<String>), String> {
    let mut upload_states = Vec::new();
    let mut index_states = Vec::new();
    for state in states {
        match state.to_uppercase().as_str() {
            "QUEUED_FOR_INDEXING" => index_states.push("queued".to_owned()),
            "INDEXING" => index_states.push("processing".to_owned()),
            "INDEXING_ERRORED" => index_states.push("errored".to_owned()),

            "UPLOADING_INDEX" => upload_states.push("uploading".to_owned()),
            "QUEUED_FOR_PROCESSING" => upload_states.push("queued".to_owned()),
            "PROCESSING" => upload_states.push("processing".to_owned()),
            "PROCESSING_ERRORED" => upload_states.push("errored".to_owned()),
            "COMPLETED" => upload_states.push("completed".to_owned()),
            "DELETING" => upload_states.push("deleting".to_owned()),
            "DELETED" => upload_states.push("deleted".to_owned()),
            _ => return Err(format!("filtering by state {state:?} is unsupported")),
        }
    }
    Ok((upload_states, index_states))
}

fn upload_state(state: &str) -> Result<&'static str, String> {
    match state.to_lowercase().as_str() {
        "uploading" => Ok("UPLOADING_INDEX"),
        "queued" => Ok("QUEUED_FOR_PROCESSING"),
        "processing" => Ok("PROCESSING"),
        "failed" | "errored" => Ok("PROCESSING_ERRORED"),
        "completed" => Ok("COMPLETED"),
        "deleting" => Ok("DELETING"),
        "deleted" => Ok("DELETED"),
        _ => Err(format!("unrecognized upload state {state:?}")),
    }
}

fn index_state(state: &str) -> Result<&'static str, String> {
    match state.to_lowercase().as_str() {
        "queued" => Ok("QUEUED_FOR_INDEXING"),
        "processing" => Ok("INDEXING"),
        "failed" | "errored" => Ok("INDEXING_ERRORED"),
        "completed" => Ok("INDEXING_COMPLETED"),
        _ => Err(format!("unrecognized index state {state:?}")),
    }
}

// ---------------------------------------------------------------------------
// Indexer metadata (`shared/indexers2.go`)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CodeIntelIndexer {
    pub language_key: String,
    pub name: String,
    pub urn: String,
    /// `CodeIntelIndexer.DockerImages`; only consumed by `IndexerFromName`.
    docker_images: Vec<String>,
}

fn make_indexer(key: &str, name: &str, urn: &str, docker_images: &[&str]) -> CodeIntelIndexer {
    CodeIntelIndexer {
        language_key: key.to_owned(),
        name: name.to_owned(),
        urn: urn.to_owned(),
        docker_images: docker_images
            .iter()
            .map(|image| (*image).to_owned())
            .collect(),
    }
}

fn make_internal_indexer(key: &str, name: &str) -> CodeIntelIndexer {
    make_indexer(
        key,
        name,
        &format!("github.com/sourcegraph/{name}"),
        &[&format!("sourcegraph/{name}")],
    )
}

/// `allIndexers` (`indexers2.go:30`), including Docker image names only where
/// they differ from the indexer name (the map is used by `IndexerFromName`).
pub fn all_indexers() -> Vec<CodeIntelIndexer> {
    vec![
        make_indexer("C++", "scip-clang", "github.com/sourcegraph/scip-clang", &[]),
        make_internal_indexer("C++", "lsif-clang"),
        make_internal_indexer("C++", "lsif-cpp"),
        make_indexer("Dart", "scip-dart", "github.com/Workiva/scip-dart", &[]),
        make_internal_indexer("Dart", "lsif-dart"),
        make_indexer("Dart", "lsif_indexer", "github.com/Workiva/lsif_indexer", &[]),
        make_internal_indexer("DotNet", "scip-dotnet"),
        make_indexer("DotNet", "lsif-dotnet", "github.com/tcz717/LsifDotnet", &[]),
        make_internal_indexer("Go", "scip-go"),
        make_internal_indexer("Go", "lsif-go"),
        make_indexer("HIE", "hie-lsif", " github.com/mpickering/hie-lsif", &[]),
        make_internal_indexer("Jsonnet", "lsif-jsonnet"),
        make_internal_indexer("JVM", "scip-java"),
        make_internal_indexer("JVM", "lsif-java"),
        make_indexer("OCaml", "lsif-ocaml", "github.com/rvantonder/lsif-ocaml", &[]),
        make_indexer(
            "PHP",
            "scip-php",
            "github.com/davidrjenni/scip-php",
            &["davidrjenni/scip-php"],
        ),
        make_indexer(
            "PHP",
            "lsif-php",
            "github.com/davidrjenni/lsif-php",
            &["davidrjenni/lsif-php"],
        ),
        make_internal_indexer("Python", "scip-python"),
        make_internal_indexer("Ruby", "scip-ruby"),
        make_internal_indexer("Rust", "scip-rust"),
        make_indexer("Rust", "rust-analyzer", "github.com/rust-lang/rust-analyzer", &[]),
        make_indexer(
            "Terraform",
            "lsif-terraform",
            "github.com/juliosueiras/lsif-terraform",
            &[],
        ),
        make_internal_indexer("TypeScript", "scip-typescript"),
        make_internal_indexer("TypeScript", "lsif-node"),
    ]
}

/// `NamesForKey`.
pub fn names_for_key(key: &str) -> Vec<String> {
    all_indexers()
        .into_iter()
        .filter(|indexer| indexer.language_key == key)
        .map(|indexer| indexer.name)
        .collect()
}

/// `IndexerFromName`: strip a Docker tag / `@sha256:` digest, then look up by
/// image name or indexer name; unknown names keep an empty language key and
/// URN.
pub fn indexer_from_name(name: &str) -> CodeIntelIndexer {
    let name = name.split("@sha256:").next().unwrap_or(name);
    let name = name.split(':').next().unwrap_or(name);
    // `imageToIndexer`: every `DockerImages` entry across all indexers.
    for indexer in all_indexers() {
        if indexer.docker_images.iter().any(|image| image == name) {
            return indexer;
        }
    }
    for indexer in all_indexers() {
        if indexer.name == name {
            return indexer;
        }
    }
    CodeIntelIndexer {
        language_key: String::new(),
        name: name.to_owned(),
        urn: String::new(),
        docker_images: Vec::new(),
    }
}

// ---------------------------------------------------------------------------
// Resolver
// ---------------------------------------------------------------------------

/// One `PreciseIndex`: the paired upload and/or index records after the
/// dataloader presubmits.
#[derive(Debug, Clone)]
pub struct PreciseIndexRecord {
    pub upload: Option<UploadRow>,
    pub index: Option<AutoIndexJobRow>,
    /// `PreciseIndex.auditLogs` (`nil` when there is no upload).
    pub audit_logs: Option<Vec<AuditLogRow>>,
}

/// `rootResolver.PreciseIndexes` (`root_resolver_index_queries.go:26`).
pub fn precise_indexes(
    db: &CodeIntelUploadsHandle,
    arguments: &Arguments,
    actor: Actor,
) -> Result<FieldValue, ExecError> {
    let page_size = arguments
        .i64("first")
        .map(|value| value as i32)
        .unwrap_or(DEFAULT_PAGE_SIZE);

    let (mut upload_offset, mut index_offset) = (0i32, 0i32);
    if let Some(after) = arguments.string("after") {
        let parts: Vec<&str> = after.split(':').collect();
        if parts.len() != 2 {
            return Err(ExecError::new("invalid cursor"));
        }
        if !parts[0].is_empty() {
            upload_offset = parts[0]
                .parse()
                .map_err(|_| ExecError::new("invalid cursor"))?;
        }
        if !parts[1].is_empty() {
            index_offset = parts[1]
                .parse()
                .map_err(|_| ExecError::new("invalid cursor"))?;
        }
    }

    let requested_states: Vec<String> = match arguments.get("states") {
        Some(InputValue::List(items)) => items
            .iter()
            .filter_map(InputValue::as_str)
            .map(str::to_owned)
            .collect(),
        Some(InputValue::Null) | None => Vec::new(),
        Some(other) => other
            .as_str()
            .map(|state| vec![state.to_owned()])
            .unwrap_or_default(),
    };
    let (upload_states, index_states) =
        bifurcate_states(&requested_states).map_err(ExecError::new)?;
    let skip_uploads = upload_states.is_empty() && !index_states.is_empty();
    let mut skip_indexes = !upload_states.is_empty() && index_states.is_empty();

    let mut dependency_of = 0i64;
    if let Some(id) = arguments.string("dependencyOf") {
        let (upload_id, index_id) = decode_precise_index_id(id).map_err(ExecError::new)?;
        let upload_id = upload_id.unwrap_or(0);
        if upload_id == 0 {
            return Err(ExecError::new(format!(
                "requested dependency of precise index record without data (indexid={})",
                index_id.unwrap_or(0)
            )));
        }
        dependency_of = upload_id;
        skip_indexes = true;
    }
    let mut dependent_of = 0i64;
    if let Some(id) = arguments.string("dependentOf") {
        let (upload_id, index_id) = decode_precise_index_id(id).map_err(ExecError::new)?;
        let upload_id = upload_id.unwrap_or(0);
        if upload_id == 0 {
            return Err(ExecError::new(format!(
                "requested dependent of precise index record without data (indexid={})",
                index_id.unwrap_or(0)
            )));
        }
        dependent_of = upload_id;
        skip_indexes = true;
    }
    let allow_deleted_upload = arguments.bool("includeDeleted").unwrap_or(false);

    let repository_id = match arguments.string("repo") {
        Some(id) => unmarshal_relay_id(id).map_err(ExecError::new)?,
        None => 0,
    };
    let term = arguments.string("query").unwrap_or_default().to_owned();
    let indexer_names = arguments
        .string("indexerKey")
        .map(names_for_key)
        .unwrap_or_default();

    let mut uploads: Vec<UploadRow> = Vec::new();
    let mut total_upload_count = 0i64;
    if !skip_uploads {
        let (rows, total) = db
            .get_uploads(&GetUploadsOptions {
                repository_id,
                states: upload_states,
                term: term.clone(),
                indexer_names: indexer_names.clone(),
                dependency_of,
                dependent_of,
                allow_deleted_upload,
                limit: page_size,
                offset: upload_offset,
            }, actor)
            .map_err(ExecError::new)?;
        uploads = rows;
        total_upload_count = total;
    }

    let mut auto_index_jobs: Vec<AutoIndexJobRow> = Vec::new();
    let mut total_jobs_count = 0i64;
    if !skip_indexes {
        let (rows, total) = db
            .get_auto_index_jobs(&GetAutoIndexJobsOptions {
                repository_id,
                states: index_states,
                term: term.clone(),
                indexer_names,
                without_upload: true,
                limit: page_size,
                offset: index_offset,
            }, actor)
            .map_err(ExecError::new)?;
        auto_index_jobs = rows;
        total_jobs_count = total;
    }

    // Merge by `UploadedAt.After(QueuedAt)` (`root_resolver_index_queries.go:170`).
    let mut pairs: Vec<PreciseIndexRecord> = Vec::with_capacity(page_size.max(0) as usize);
    let mut u_idx = 0usize;
    let mut i_idx = 0usize;
    while u_idx < uploads.len()
        && i_idx < auto_index_jobs.len()
        && (u_idx + i_idx) < page_size.max(0) as usize
    {
        if uploads[u_idx].uploaded_at > auto_index_jobs[i_idx].queued_at {
            pairs.push(PreciseIndexRecord {
                upload: Some(uploads[u_idx].clone()),
                index: None,
                audit_logs: None,
            });
            u_idx += 1;
        } else {
            pairs.push(PreciseIndexRecord {
                upload: None,
                index: Some(auto_index_jobs[i_idx].clone()),
                audit_logs: None,
            });
            i_idx += 1;
        }
    }
    while u_idx < uploads.len() && (u_idx + i_idx) < page_size.max(0) as usize {
        pairs.push(PreciseIndexRecord {
            upload: Some(uploads[u_idx].clone()),
            index: None,
            audit_logs: None,
        });
        u_idx += 1;
    }
    while i_idx < auto_index_jobs.len() && (u_idx + i_idx) < page_size.max(0) as usize {
        pairs.push(PreciseIndexRecord {
            upload: None,
            index: Some(auto_index_jobs[i_idx].clone()),
            audit_logs: None,
        });
        i_idx += 1;
    }

    let mut cursor = String::new();
    if (upload_offset + u_idx as i32) < total_upload_count as i32 {
        cursor.push_str(&(upload_offset + u_idx as i32).to_string());
    }
    cursor.push(':');
    if (index_offset + i_idx as i32) < total_jobs_count as i32 {
        cursor.push_str(&(index_offset + i_idx as i32).to_string());
    }
    if cursor == ":" {
        cursor.clear();
    }

    // Dataloader pre-submits (`PresubmitAssociatedUploads` /
    // `PresubmitAssociatedAutoIndexJobs`).
    let index_ids: Vec<i64> = uploads
        .iter()
        .filter_map(|upload| upload.associated_index_id)
        .collect();
    let upload_ids: Vec<i64> = auto_index_jobs
        .iter()
        .filter_map(|job| job.associated_upload_id)
        .collect();
    let index_by_id: HashMap<i64, AutoIndexJobRow> = if index_ids.is_empty() {
        HashMap::new()
    } else {
        db.get_auto_index_jobs_by_ids(&index_ids, actor)
            .map_err(ExecError::new)?
            .into_iter()
            .map(|job| (job.id, job))
            .collect()
    };
    let upload_by_id: HashMap<i64, UploadRow> = if upload_ids.is_empty() {
        HashMap::new()
    } else {
        db.get_uploads_by_ids(&upload_ids, actor)
            .map_err(ExecError::new)?
            .into_iter()
            .map(|upload| (upload.id, upload))
            .collect()
    };

    let mut nodes: Vec<PreciseIndexObject> = Vec::with_capacity(pairs.len());
    for mut pair in pairs {
        // `newPreciseIndexResolver`: an index with an associated upload loads
        // the upload; an upload with an associated index loads the index.
        if pair.index.is_some() && pair.upload.is_none() {
            if let Some(id) = pair.index.as_ref().and_then(|job| job.associated_upload_id) {
                pair.upload = upload_by_id.get(&id).cloned();
            }
        }
        if pair.upload.is_some() && pair.index.is_none() {
            if let Some(id) = pair.upload.as_ref().and_then(|upload| upload.associated_index_id) {
                if let Some(job) = index_by_id.get(&id).cloned() {
                    pair.index = Some(job);
                }
            }
        }
        // `PreciseIndex.auditLogs` (`precise_index_resolver.go:380`): nil when
        // there is no upload, otherwise the upload's audit trail.
        if let Some(upload) = &pair.upload {
            pair.audit_logs = Some(
                db.get_audit_logs_for_upload(upload.id as i32)
                    .map_err(ExecError::new)?,
            );
        }
        nodes.push(PreciseIndexObject {
            record: pair,
            db: Some(db.clone()),
        });
    }

    Ok(FieldValue::Object(Arc::new(PreciseIndexConnectionObject {
        nodes,
        cursor: cursor.clone(),
        total_count: (total_upload_count + total_jobs_count) as i32,
    })))
}

/// `rootResolver.PreciseIndexByID` (`root_resolver_index_queries.go:213`).
///
/// Returns `None` for a missing record (GraphQL `null`), matching the Go
/// `(nil, nil)` return. An upload id wins over an index id.
pub fn precise_index_by_id(
    db: &CodeIntelUploadsHandle,
    id: &str,
    actor: Actor,
) -> Result<Option<FieldValue>, ExecError> {
    let (upload_id, index_id) = decode_precise_index_id(id).map_err(ExecError::new)?;

    if let Some(upload_id) = upload_id {
        let Some(upload) = db
            .get_upload_by_id(upload_id, actor)
            .map_err(ExecError::new)?
        else {
            return Ok(None);
        };
        // `newPreciseIndexResolver`: load the upload's associated index.
        let index = match upload.associated_index_id {
            Some(index_id) => db
                .get_auto_index_job_by_id(index_id, actor)
                .map_err(ExecError::new)?,
            None => None,
        };
        let audit_logs = Some(
            db.get_audit_logs_for_upload(upload.id as i32)
                .map_err(ExecError::new)?,
        );
        return Ok(Some(FieldValue::Object(Arc::new(PreciseIndexObject {
            record: PreciseIndexRecord {
                upload: Some(upload),
                index,
                audit_logs,
            },
            db: Some(db.clone()),
        }))));
    }

    let Some(index_id) = index_id else {
        return Err(ExecError::new("invalid identifier"));
    };
    let Some(index) = db
        .get_auto_index_job_by_id(index_id, actor)
        .map_err(ExecError::new)?
    else {
        return Ok(None);
    };
    // `newPreciseIndexResolver`: load the index's associated upload.
    let upload = match index.associated_upload_id {
        Some(upload_id) => db
            .get_upload_by_id(upload_id, actor)
            .map_err(ExecError::new)?,
        None => None,
    };
    let audit_logs = match &upload {
        Some(upload) => Some(
            db.get_audit_logs_for_upload(upload.id as i32)
                .map_err(ExecError::new)?,
        ),
        None => None,
    };
    Ok(Some(FieldValue::Object(Arc::new(PreciseIndexObject {
        record: PreciseIndexRecord {
            upload,
            index: Some(index),
            audit_logs,
        },
        db: Some(db.clone()),
    }))))
}

/// `rootResolver.IndexerKeys` (`root_resolver_index_queries.go:257`).
pub fn indexer_keys(
    db: &CodeIntelUploadsHandle,
    arguments: &Arguments,
    actor: Actor,
) -> Result<FieldValue, ExecError> {
    let repository_id = match arguments.string("repo") {
        Some(id) => unmarshal_relay_id(id).map_err(ExecError::new)?,
        None => 0,
    };
    let names = db
        .get_indexer_names(repository_id, actor)
        .map_err(ExecError::new)?;
    let mut keys: Vec<String> = names
        .into_iter()
        .map(|name| indexer_from_name(&name).language_key)
        .collect();
    keys.sort();
    keys.dedup();
    Ok(string_list(&keys))
}

/// `rootResolver.CodeIntelSummary` (`root_resolver_coverage.go:27`).
pub fn code_intel_summary(
    db: &CodeIntelUploadsHandle,
    _arguments: &Arguments,
    _actor: Actor,
) -> Result<FieldValue, ExecError> {
    let num_repositories_with_code_intelligence = db
        .num_repositories_with_code_intelligence()
        .map_err(ExecError::new)?;
    Ok(FieldValue::Object(Arc::new(CodeIntelSummaryObject {
        db: db.clone(),
        num_repositories_with_code_intelligence,
    })))
}

/// `rootResolver.RepositorySummary` (`root_resolver_coverage.go:37-160`) —
/// `Repository.codeIntelSummary`.
pub fn repository_code_intel_summary(
    db: &CodeIntelUploadsHandle,
    repository_id: i32,
    actor: Actor,
) -> Result<FieldValue, ExecError> {
    check_site_admin(actor)?;
    let last_upload_retention_scan = db
        .last_upload_retention_scan(repository_id)
        .map_err(ExecError::new)?;
    let last_index_scan = db
        .last_index_scan(repository_id)
        .map_err(ExecError::new)?;
    let activity = db
        .recent_activity(repository_id, actor)
        .map_err(ExecError::new)?;
    let nodes = repository_summary_nodes(db, activity, actor)?;
    Ok(FieldValue::Object(Arc::new(RepositorySummaryObject {
        nodes,
        last_upload_retention_scan,
        last_index_scan,
        available_indexers: db.auto_indexing_enabled(),
    })))
}

/// `repositorySummaryResolver.RecentActivity` (`root_resolver_coverage.go:359-401`):
/// pair uploads with their associated index (the dataloader fetches it from
/// the DB when it is not part of the summary), skip indexes whose associated
/// upload is already listed, then sort by the relay ID string.
fn repository_summary_nodes(
    db: &CodeIntelUploadsHandle,
    activity: RecentActivityRows,
    actor: Actor,
) -> Result<Vec<PreciseIndexObject>, ExecError> {
    let mut index_by_id: std::collections::HashMap<i64, AutoIndexJobRow> = activity
        .indexes
        .iter()
        .cloned()
        .map(|index| (index.id, index))
        .collect();
    let missing_index_ids: Vec<i64> = activity
        .uploads
        .iter()
        .filter_map(|upload| upload.associated_index_id)
        .filter(|id| !index_by_id.contains_key(id))
        .collect();
    if !missing_index_ids.is_empty() {
        for index in db
            .get_auto_index_jobs_by_ids(&missing_index_ids, actor)
            .map_err(ExecError::new)?
        {
            index_by_id.insert(index.id, index);
        }
    }

    let mut upload_by_id: std::collections::HashMap<i64, UploadRow> = activity
        .uploads
        .iter()
        .cloned()
        .map(|upload| (upload.id, upload))
        .collect();
    let missing_upload_ids: Vec<i64> = activity
        .indexes
        .iter()
        .filter_map(|index| index.associated_upload_id)
        .filter(|id| !upload_by_id.contains_key(id))
        .collect();
    if !missing_upload_ids.is_empty() {
        for upload in db
            .get_uploads_by_ids(&missing_upload_ids, actor)
            .map_err(ExecError::new)?
        {
            upload_by_id.insert(upload.id, upload);
        }
    }

    let recent_upload_ids: std::collections::HashSet<i64> =
        activity.uploads.iter().map(|upload| upload.id).collect();
    let mut nodes: Vec<PreciseIndexObject> = Vec::new();
    for upload in &activity.uploads {
        let index = upload
            .associated_index_id
            .and_then(|id| index_by_id.get(&id).cloned());
        let audit_logs = Some(
            db.get_audit_logs_for_upload(upload.id as i32)
                .map_err(ExecError::new)?,
        );
        nodes.push(PreciseIndexObject {
            record: PreciseIndexRecord {
                upload: Some(upload.clone()),
                index,
                audit_logs,
            },
            db: Some(db.clone()),
        });
    }
    for index in &activity.indexes {
        if let Some(associated_upload_id) = index.associated_upload_id {
            if recent_upload_ids.contains(&associated_upload_id) {
                continue;
            }
        }
        let upload = index
            .associated_upload_id
            .and_then(|id| upload_by_id.get(&id).cloned());
        let audit_logs = match &upload {
            Some(upload) => Some(
                db.get_audit_logs_for_upload(upload.id as i32)
                    .map_err(ExecError::new)?,
            ),
            None => None,
        };
        nodes.push(PreciseIndexObject {
            record: PreciseIndexRecord {
                upload,
                index: Some(index.clone()),
                audit_logs,
            },
            db: Some(db.clone()),
        });
    }
    nodes.sort_by(|a, b| a.id().cmp(&b.id()));
    Ok(nodes)
}

fn check_site_admin(actor: Actor) -> Result<(), ExecError> {
    crate::admin_store::check_current_user_is_site_admin(actor, false).map_err(ExecError::new)
}

fn states_argument(arguments: &Arguments) -> Vec<String> {
    match arguments.get("states") {
        Some(InputValue::List(items)) => items
            .iter()
            .filter_map(InputValue::as_str)
            .map(str::to_owned)
            .collect(),
        Some(InputValue::Null) | None => Vec::new(),
        Some(other) => other
            .as_str()
            .map(|state| vec![state.to_owned()])
            .unwrap_or_default(),
    }
}

fn indexer_names_argument(arguments: &Arguments) -> Vec<String> {
    arguments
        .string("indexerKey")
        .map(names_for_key)
        .unwrap_or_default()
}

fn repository_argument(arguments: &Arguments) -> Result<i32, ExecError> {
    match arguments.string("repository") {
        Some(id) => unmarshal_relay_id(id).map_err(ExecError::new),
        None => Ok(0),
    }
}

fn empty_response() -> FieldValue {
    FieldValue::Object(Arc::new(EmptyResponseObject))
}

/// `rootResolver.DeletePreciseIndex` (`root_resolver_index_mutations.go:15`).
pub fn delete_precise_index(
    db: &CodeIntelUploadsHandle,
    arguments: &Arguments,
    actor: Actor,
) -> Result<FieldValue, ExecError> {
    check_site_admin(actor)?;
    let id = required_id(arguments)?;
    let (upload_id, index_id) = decode_precise_index_id(&id).map_err(ExecError::new)?;
    if let Some(upload_id) = upload_id {
        db.delete_upload_by_id(upload_id).map_err(ExecError::new)?;
    } else if let Some(index_id) = index_id {
        db.delete_auto_index_job_by_id(index_id)
            .map_err(ExecError::new)?;
    }
    Ok(empty_response())
}

/// `rootResolver.DeletePreciseIndexes` (`root_resolver_index_mutations.go:43`).
pub fn delete_precise_indexes(
    db: &CodeIntelUploadsHandle,
    arguments: &Arguments,
    actor: Actor,
) -> Result<FieldValue, ExecError> {
    check_site_admin(actor)?;
    let (upload_states, index_states) =
        bifurcate_states(&states_argument(arguments)).map_err(ExecError::new)?;
    let skip_uploads = upload_states.is_empty() && !index_states.is_empty();
    let mut skip_indexes = !upload_states.is_empty() && index_states.is_empty();
    let indexer_names = indexer_names_argument(arguments);
    let repository_id = repository_argument(arguments)?;
    let term = arguments.string("query").unwrap_or_default().to_owned();
    let mut visible_at_tip = false;
    if let Some(value) = arguments.bool("isLatestForRepo") {
        visible_at_tip = value;
        skip_indexes = true;
    }

    if !skip_uploads {
        db.delete_uploads(&DeleteUploadsOptions {
            repository_id,
            states: upload_states,
            indexer_names: indexer_names.clone(),
            term: term.clone(),
            visible_at_tip,
        })
        .map_err(ExecError::new)?;
    }
    if !skip_indexes {
        db.delete_auto_index_jobs(&DeleteAutoIndexJobsOptions {
            repository_id,
            states: index_states,
            indexer_names,
            term,
            without_upload: true,
        })
        .map_err(ExecError::new)?;
    }
    Ok(empty_response())
}

/// `rootResolver.ReindexPreciseIndex` (`root_resolver_index_mutations.go:109`).
pub fn reindex_precise_index(
    db: &CodeIntelUploadsHandle,
    arguments: &Arguments,
    actor: Actor,
) -> Result<FieldValue, ExecError> {
    check_site_admin(actor)?;
    let id = required_id(arguments)?;
    let (upload_id, index_id) = decode_precise_index_id(&id).map_err(ExecError::new)?;
    if let Some(upload_id) = upload_id {
        db.reindex_upload_by_id(upload_id).map_err(ExecError::new)?;
    } else if let Some(index_id) = index_id {
        db.set_rerun_auto_index_job_by_id(index_id)
            .map_err(ExecError::new)?;
    }
    Ok(empty_response())
}

/// `rootResolver.ReindexPreciseIndexes` (`root_resolver_index_mutations.go:137`).
pub fn reindex_precise_indexes(
    db: &CodeIntelUploadsHandle,
    arguments: &Arguments,
    actor: Actor,
) -> Result<FieldValue, ExecError> {
    check_site_admin(actor)?;
    let (upload_states, index_states) =
        bifurcate_states(&states_argument(arguments)).map_err(ExecError::new)?;
    let skip_uploads = upload_states.is_empty() && !index_states.is_empty();
    let mut skip_indexes = !upload_states.is_empty() && index_states.is_empty();
    let indexer_names = indexer_names_argument(arguments);
    let repository_id = repository_argument(arguments)?;
    let term = arguments.string("query").unwrap_or_default().to_owned();
    let mut visible_at_tip = false;
    if let Some(value) = arguments.bool("isLatestForRepo") {
        visible_at_tip = value;
        skip_indexes = true;
    }

    if !skip_uploads {
        db.reindex_uploads(&ReindexUploadsOptions {
            repository_id,
            states: upload_states,
            indexer_names: indexer_names.clone(),
            term: term.clone(),
            visible_at_tip,
        })
        .map_err(ExecError::new)?;
    }
    if !skip_indexes {
        db.set_rerun_auto_index_jobs(&SetRerunAutoIndexJobsOptions {
            repository_id,
            states: index_states,
            indexer_names,
            term,
            without_upload: true,
        })
        .map_err(ExecError::new)?;
    }
    Ok(empty_response())
}

fn required_id(arguments: &Arguments) -> Result<String, ExecError> {
    arguments
        .string("id")
        .map(str::to_owned)
        .ok_or_else(|| ExecError::new("missing id argument"))
}

/// Whether this module owns the root field.
pub fn is_root_field(field: &str) -> bool {
    field == "preciseIndexes" || field == "indexerKeys" || field == "codeIntelSummary"
}

/// Root field dispatch.
pub fn root_field(
    db: &CodeIntelUploadsHandle,
    field: &str,
    arguments: &Arguments,
    actor: Actor,
) -> Result<Option<FieldValue>, ExecError> {
    match field {
        "preciseIndexes" => precise_indexes(db, arguments, actor).map(Some),
        "indexerKeys" => indexer_keys(db, arguments, actor).map(Some),
        "codeIntelSummary" => code_intel_summary(db, arguments, actor).map(Some),
        _ => Ok(None),
    }
}

/// Whether this module owns a `Mutation` field.
pub fn is_mutation_field(field: &str) -> bool {
    matches!(
        field,
        "deletePreciseIndex"
            | "deletePreciseIndexes"
            | "reindexPreciseIndex"
            | "reindexPreciseIndexes"
    )
}

/// `Mutation` dispatch (`rootResolvers` in `root_resolver_index_mutations.go`).
pub fn mutation_field(
    db: &CodeIntelUploadsHandle,
    field: &str,
    arguments: &Arguments,
    actor: Actor,
) -> Result<Option<FieldValue>, ExecError> {
    match field {
        "deletePreciseIndex" => delete_precise_index(db, arguments, actor).map(Some),
        "deletePreciseIndexes" => delete_precise_indexes(db, arguments, actor).map(Some),
        "reindexPreciseIndex" => reindex_precise_index(db, arguments, actor).map(Some),
        "reindexPreciseIndexes" => reindex_precise_indexes(db, arguments, actor).map(Some),
        _ => Ok(None),
    }
}

/// `resolverstubs.EmptyResponse` (`AlwaysNil() == nil`).
pub struct EmptyResponseObject;

impl ObjectValue for EmptyResponseObject {
    fn type_name(&self) -> &'static str {
        "EmptyResponse"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "alwaysNil" => Ok(Some(FieldValue::Null)),
            _ => Ok(None),
        }
    }
}

/// `NewCodeIntelIndexerResolver(name, imageName)` (`indexer_resolver.go:19`),
/// used by the autoindexing `AutoIndexJobDescription.indexer` field.
pub fn indexer_field(indexer: &str, image_name: Option<String>) -> FieldValue {
    FieldValue::Object(Arc::new(CodeIntelIndexerObject {
        indexer: indexer_from_name(indexer),
        image_name,
    }))
}

/// `NewAutoIndexJobStepsResolver` (`index_steps_resolver.go:13`): the same
/// `IndexSteps` shape the uploads surface serves, built from the job's
/// pre-index steps, local steps, indexer arguments and outfile.
pub fn steps_field_from_auto_index_job(job: &AutoIndexJobRow) -> FieldValue {
    FieldValue::Object(Arc::new(IndexStepsObject { job: job.clone() }))
}

/// `PreciseIndexResolverFactory.Create(..., &index)` for the jobs returned by
/// `queueAutoIndexJobsForRepo` (`root_resolver_inference.go:117`).
pub fn precise_index_from_auto_index_job(job: AutoIndexJobRow) -> FieldValue {
    FieldValue::Object(Arc::new(PreciseIndexObject {
        record: PreciseIndexRecord {
            upload: None,
            index: Some(job),
            audit_logs: None,
        },
        db: None,
    }))
}

// ---------------------------------------------------------------------------
// Objects
// ---------------------------------------------------------------------------

struct PreciseIndexConnectionObject {
    nodes: Vec<PreciseIndexObject>,
    cursor: String,
    total_count: i32,
}

impl ObjectValue for PreciseIndexConnectionObject {
    fn type_name(&self) -> &'static str {
        "PreciseIndexConnection"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "nodes" => Ok(Some(FieldValue::List {
                items: self
                    .nodes
                    .iter()
                    .map(|node| FieldValue::Object(Arc::new(node.clone())))
                    .collect(),
                element_type: Some("PreciseIndex"),
            })),
            "totalCount" => Ok(Some(FieldValue::Json(Json::Int(self.total_count as i64)))),
            "pageInfo" => {
                let has_next = !self.cursor.is_empty();
                Ok(Some(FieldValue::Object(Arc::new(PageInfoObject::new(
                    has_next,
                    has_next.then(|| self.cursor.clone()),
                )))))
            }
            _ => Ok(None),
        }
    }
}

#[derive(Clone)]
struct PreciseIndexObject {
    record: PreciseIndexRecord,
    /// The uploads-policies seam (`projectRoot`/`tags`/`retentionPolicyOverview`).
    /// `None` only for the autoindexing mutation response, where the baseline
    /// resolver still has the uploads service; those fields are left unreached
    /// there.
    db: Option<CodeIntelUploadsHandle>,
}

impl PreciseIndexObject {
    fn id(&self) -> String {
        marshal_precise_index_id(
            self.record.upload.as_ref().map(|upload| upload.id),
            self.record.index.as_ref().map(|job| job.id),
        )
    }

    fn input_commit(&self) -> String {
        match (&self.record.upload, &self.record.index) {
            (Some(upload), _) => upload.commit.clone(),
            (None, Some(job)) => job.commit.clone(),
            (None, None) => String::new(),
        }
    }

    fn input_root(&self) -> String {
        match (&self.record.upload, &self.record.index) {
            (Some(upload), _) => upload.root.clone(),
            (None, Some(job)) => job.root.clone(),
            (None, None) => String::new(),
        }
    }

    fn input_indexer(&self) -> String {
        match (&self.record.upload, &self.record.index) {
            (Some(upload), _) => upload.indexer.clone(),
            (None, Some(job)) => job.indexer.clone(),
            (None, None) => String::new(),
        }
    }

    fn failure(&self) -> Option<String> {
        if let Some(upload) = &self.record.upload {
            if upload.failure_message.is_some() {
                return upload.failure_message.clone();
            }
        }
        self.record
            .index
            .as_ref()
            .and_then(|job| job.failure_message.clone())
    }

    fn place_in_queue(&self) -> Option<i32> {
        if let Some(job) = &self.record.index {
            if job.rank.is_some() {
                return job.rank;
            }
        }
        self.record.upload.as_ref().and_then(|upload| upload.rank)
    }

    fn state(&self) -> Result<&'static str, String> {
        if let Some(upload) = &self.record.upload {
            return upload_state(&upload.state);
        }
        match &self.record.index {
            Some(job) => index_state(&job.state),
            None => Err("precise index record without data".to_owned()),
        }
    }

    fn indexer(&self) -> Option<CodeIntelIndexer> {
        if let Some(job) = &self.record.index {
            // `NewCodeIntelIndexerResolver(index.Indexer, index.Indexer)`.
            return Some(indexer_from_name(&job.indexer));
        }
        self.record
            .upload
            .as_ref()
            .map(|upload| indexer_from_name(&upload.indexer))
    }

    fn indexer_image_name(&self) -> Option<String> {
        if let Some(job) = &self.record.index {
            if !job.indexer.is_empty() {
                return Some(job.indexer.clone());
            }
        }
        None
    }

    fn should_reindex(&self) -> bool {
        if let Some(upload) = &self.record.upload {
            return upload.should_reindex
                && self
                    .record
                    .index
                    .as_ref()
                    .map(|job| job.should_reindex)
                    .unwrap_or(true);
        }
        self.record
            .index
            .as_ref()
            .map(|job| job.should_reindex)
            .unwrap_or(false)
    }

    /// `projectRootMetadata` (`precise_index_resolver.go:326-336`).
    fn project_root_metadata(&self) -> (i32, String, String) {
        if let Some(upload) = &self.record.upload {
            return (upload.repository_id, upload.commit.clone(), upload.root.clone());
        }
        match &self.record.index {
            Some(job) => (job.repository_id, job.commit.clone(), job.root.clone()),
            None => (0, String::new(), String::new()),
        }
    }

    /// `PreciseIndex.ProjectRoot` (`precise_index_resolver.go:305-314`):
    /// a cached `Path` lookup; unknown repository/commit → `null`.
    fn project_root(&self) -> Result<FieldValue, ExecError> {
        let Some(db) = &self.db else {
            return Ok(FieldValue::Null);
        };
        let (repo_id, commit, root) = self.project_root_metadata();
        let Some(repo_name) = db.repository_name(repo_id).map_err(ExecError::new)? else {
            return Ok(FieldValue::Null);
        };
        let Some(oid) = db
            .resolve_revision(repo_id, &repo_name, &commit)
            .map_err(ExecError::new)?
        else {
            return Ok(FieldValue::Null);
        };
        Ok(FieldValue::Object(Arc::new(ProjectRootObject {
            repo_id,
            repo_name,
            rev: if commit.is_empty() { oid.clone() } else { commit },
            oid,
            path: root,
        })))
    }

    /// `PreciseIndex.Tags` (`precise_index_resolver.go:316-324` +
    /// `gitresolvers/commit.go:58-76`): unknown repo/commit → `null` (the
    /// baseline's non-null violation), otherwise `ListRefs(TagsOnly,
    /// PointsAtCommit)` short names (an empty slice, never `null`).
    fn tags(&self) -> Result<FieldValue, ExecError> {
        let Some(db) = &self.db else {
            return Ok(string_list(&[]));
        };
        let (repo_id, commit, _) = self.project_root_metadata();
        let Some(repo_name) = db.repository_name(repo_id).map_err(ExecError::new)? else {
            return Ok(FieldValue::Null);
        };
        let Some(oid) = db
            .resolve_revision(repo_id, &repo_name, &commit)
            .map_err(ExecError::new)?
        else {
            return Ok(FieldValue::Null);
        };
        let tags = db
            .list_tags_at_commit(repo_id, &repo_name, &oid)
            .map_err(ExecError::new)?;
        Ok(string_list(&tags))
    }

    /// `PreciseIndex.RetentionPolicyOverview` (`precise_index_resolver.go:343`):
    /// an index-only node returns `null` (Go `(nil, nil)`).
    fn retention_overview(&self, arguments: &Arguments) -> Result<FieldValue, ExecError> {
        let Some(upload) = self.record.upload.as_ref() else {
            return Ok(FieldValue::Null);
        };
        let Some(db) = &self.db else {
            return Ok(FieldValue::Null);
        };
        let after = match arguments.string("after") {
            Some(value) => unmarshal_relay_i64(value).map_err(ExecError::new)?,
            None => 0,
        };
        let first = arguments.i64("first").unwrap_or(i64::from(
            DEFAULT_RETENTION_POLICY_MATCHES_PAGE_SIZE,
        ));
        let matches_only = arguments.bool("matchesOnly").unwrap_or(false);
        let term = arguments.string("query").unwrap_or_default();
        let rows = db
            .retention_policy_overview(upload, matches_only, first, after, term)
            .map_err(ExecError::new)?;
        let total_count = rows.len() as i64;
        let nodes = rows
            .into_iter()
            .map(|row| {
                let repository = row.policy.as_ref().and_then(|policy| {
                    policy.repository_id.and_then(|id| {
                        db.repository_name(id)
                            .ok()
                            .flatten()
                            .map(|name| (id, name))
                    })
                });
                FieldValue::Object(Arc::new(RetentionPolicyMatchObject { row, repository }))
            })
            .collect();
        Ok(FieldValue::Object(Arc::new(RetentionPolicyConnectionObject {
            nodes,
            total_count,
        })))
    }
}

/// `resolverstubs.UnmarshalID[int64]` (`relay.UnmarshalSpec` for an integer):
/// decode the URL-safe base64, skip the `<kind>:` prefix and parse the rest.
fn unmarshal_relay_i64(id: &str) -> Result<i64, String> {
    let decoded = base64::engine::general_purpose::URL_SAFE
        .decode(id)
        .map_err(|error| error.to_string())?;
    let text = String::from_utf8_lossy(&decoded);
    let Some((_, spec)) = text.split_once(':') else {
        return Err("invalid graphql.ID".to_owned());
    };
    serde_json::from_str(spec).map_err(|error| error.to_string())
}

/// `DefaultRetentionPolicyMatchesPageSize` (`precise_index_resolver.go:341`).
pub const DEFAULT_RETENTION_POLICY_MATCHES_PAGE_SIZE: i32 = 50;

/// `CodeIntelGitTree` (`projectRoot`). Only the non-lazy fields are
/// resolvable; `content` needs the tree-entry blob reader.
struct ProjectRootObject {
    repo_id: i32,
    repo_name: String,
    /// `commitResolver.rev` (`inputRev` when non-empty, else the oid).
    rev: String,
    oid: String,
    path: String,
}

impl ObjectValue for ProjectRootObject {
    fn type_name(&self) -> &'static str {
        "CodeIntelGitTree"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        let repository = || {
            FieldValue::Object(Arc::new(ProjectRootRepositoryObject {
                id: self.repo_id,
                name: self.repo_name.clone(),
            }))
        };
        let value = match field {
            "path" => FieldValue::Json(Json::Str(self.path.clone())),
            // `path.Base` (Go): "." for "", "sub" for "sub/".
            "name" => FieldValue::Json(Json::Str(go_path_base(&self.path))),
            // `treeEntryResolver.URL` = `commit.URI()` + uriSuffix, where the
            // suffix is empty for the repository root.
            "url" => FieldValue::Json(Json::Str(format!(
                "/{}@{}{}",
                self.repo_name,
                self.rev,
                go_tree_uri_suffix(&self.path)
            ))),
            "commit" => FieldValue::Object(Arc::new(ProjectRootCommitObject {
                repo_id: self.repo_id,
                repo_name: self.repo_name.clone(),
                rev: self.rev.clone(),
                oid: self.oid.clone(),
            })),
            "repository" => repository(),
            _ => return Ok(None),
        };
        Ok(Some(value))
    }
}

struct ProjectRootCommitObject {
    repo_id: i32,
    repo_name: String,
    rev: String,
    oid: String,
}

impl ObjectValue for ProjectRootCommitObject {
    fn type_name(&self) -> &'static str {
        "CodeIntelCommit"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        let value = match field {
            "id" => FieldValue::Json(Json::Str(marshal_git_commit_id(
                self.repo_id,
                &self.oid,
            ))),
            "oid" => FieldValue::Json(Json::Str(self.oid.clone())),
            "abbreviatedOID" => FieldValue::Json(Json::Str(
                self.oid.chars().take(7).collect::<String>(),
            )),
            "url" => FieldValue::Json(Json::Str(format!(
                "/{}/-/commit/{}",
                self.repo_name, self.rev
            ))),
            "repository" => FieldValue::Object(Arc::new(ProjectRootRepositoryObject {
                id: self.repo_id,
                name: self.repo_name.clone(),
            })),
            _ => return Ok(None),
        };
        Ok(Some(value))
    }
}

struct ProjectRootRepositoryObject {
    id: i32,
    name: String,
}

impl ObjectValue for ProjectRootRepositoryObject {
    fn type_name(&self) -> &'static str {
        "CodeIntelRepository"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        let value = match field {
            "id" => FieldValue::Json(Json::Str(
                crate::graphql_resolvers::RepositoryObject::relay_id(self.id),
            )),
            "name" => FieldValue::Json(Json::Str(self.name.clone())),
            "url" => FieldValue::Json(Json::Str(format!("/{}", self.name))),
            "externalRepository" => FieldValue::Null,
            _ => return Ok(None),
        };
        Ok(Some(value))
    }
}

/// `resolverstubs.MarshalID("GitCommit", map{"r": repoID, "c": oid})`.
fn marshal_git_commit_id(repo_id: i32, oid: &str) -> String {
    let spec = serde_json::json!({ "r": repo_id, "c": oid });
    base64::engine::general_purpose::URL_SAFE.encode(format!(
        "GitCommit:{}",
        serde_json::to_string(&spec).expect("git commit spec")
    ))
}

/// Go `path.Base` for the tree-entry name.
fn go_path_base(path: &str) -> String {
    if path.is_empty() {
        return ".".to_owned();
    }
    let trimmed = path.trim_end_matches('/');
    if trimmed.is_empty() {
        return "/".to_owned();
    }
    trimmed
        .rsplit('/')
        .next()
        .unwrap_or(trimmed)
        .to_owned()
}

/// `treeEntryResolver.uriSuffix`: `""` for the root, else
/// `/-/tree/<path>` (`stdpath.Clean("/"+path) != "/"`).
fn go_tree_uri_suffix(path: &str) -> String {
    // `stdpath.Clean("/" + path)` differs from "/" unless the path is empty.
    let normalized = format!("/{path}");
    let collapsed = collapse_slashes(&normalized);
    if collapsed == "/" {
        String::new()
    } else {
        // The baseline keeps the raw path (`path` is not cleaned):
        // `/-/tree/sub/` for `sub/`.
        format!("/-/tree/{path}")
    }
}

/// Minimal `path.Clean` for slash-separated paths (no `..` handling beyond
/// dropping empty and "." segments, which covers upload roots).
fn collapse_slashes(path: &str) -> String {
    let mut out = String::from("/");
    let mut parts: Vec<&str> = Vec::new();
    for segment in path.split('/') {
        if segment.is_empty() || segment == "." {
            continue;
        }
        parts.push(segment);
    }
    out.push_str(&parts.join("/"));
    out
}

impl ObjectValue for PreciseIndexObject {
    fn type_name(&self) -> &'static str {
        "PreciseIndex"
    }

    fn field(
        &self,
        field: &str,
        arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        let value = match field {
            "id" => FieldValue::Json(Json::Str(self.id())),
            "projectRoot" => self.project_root()?,
            "inputCommit" => FieldValue::Json(Json::Str(self.input_commit())),
            "inputRoot" => FieldValue::Json(Json::Str(self.input_root())),
            "inputIndexer" => FieldValue::Json(Json::Str(self.input_indexer())),
            "tags" => self.tags()?,
            "indexer" => match self.indexer() {
                Some(indexer) => FieldValue::Object(Arc::new(CodeIntelIndexerObject {
                    indexer,
                    image_name: self.indexer_image_name(),
                })),
                None => FieldValue::Null,
            },
            "state" => FieldValue::Json(Json::Str(
                self.state().map_err(ExecError::new)?.to_owned(),
            )),
            "queuedAt" => FieldValue::Json(datetime_json(
                self.record.index.as_ref().map(|job| job.queued_at),
            )),
            "indexingStartedAt" => FieldValue::Json(datetime_json(
                self.record.index.as_ref().and_then(|job| job.started_at),
            )),
            "indexingFinishedAt" => FieldValue::Json(datetime_json(
                self.record.index.as_ref().and_then(|job| job.finished_at),
            )),
            "uploadedAt" => FieldValue::Json(datetime_json(
                self.record.upload.as_ref().map(|upload| upload.uploaded_at),
            )),
            "processingStartedAt" => FieldValue::Json(datetime_json(
                self.record.upload.as_ref().and_then(|upload| upload.started_at),
            )),
            "processingFinishedAt" => FieldValue::Json(datetime_json(
                self.record.upload.as_ref().and_then(|upload| upload.finished_at),
            )),
            "failure" => FieldValue::Json(match self.failure() {
                Some(message) => Json::Str(message),
                None => Json::Null,
            }),
            "placeInQueue" => FieldValue::Json(match self.place_in_queue() {
                Some(rank) => Json::Int(rank as i64),
                None => Json::Null,
            }),
            "steps" => match &self.record.index {
                Some(job) => FieldValue::Object(Arc::new(IndexStepsObject { job: job.clone() })),
                None => FieldValue::Null,
            },
            "shouldReindex" => FieldValue::Json(Json::Bool(self.should_reindex())),
            "isLatestForRepo" => FieldValue::Json(Json::Bool(
                self.record
                    .upload
                    .as_ref()
                    .map(|upload| upload.visible_at_tip)
                    .unwrap_or(false),
            )),
            "retentionPolicyOverview" => self.retention_overview(arguments)?,
            "auditLogs" => match &self.record.audit_logs {
                None => FieldValue::Null,
                Some(logs) => FieldValue::List {
                    items: logs
                        .iter()
                        .map(|log| {
                            FieldValue::Object(Arc::new(AuditLogObject { log: log.clone() }))
                        })
                        .collect(),
                    element_type: Some("LSIFUploadAuditLog"),
                },
            },
            _ => return Ok(None),
        };
        Ok(Some(value))
    }
}

fn datetime_json(value: Option<OffsetDateTime>) -> Json {
    match value {
        Some(value) => Json::Str(datetime_rfc3339(value)),
        None => Json::Null,
    }
}

struct CodeIntelIndexerObject {
    indexer: CodeIntelIndexer,
    image_name: Option<String>,
}

impl ObjectValue for CodeIntelIndexerObject {
    fn type_name(&self) -> &'static str {
        "CodeIntelIndexer"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "key" => Ok(Some(FieldValue::Json(Json::Str(
                self.indexer.language_key.clone(),
            )))),
            "name" => Ok(Some(FieldValue::Json(Json::Str(self.indexer.name.clone())))),
            "url" => Ok(Some(FieldValue::Json(if self.indexer.urn.is_empty() {
                Json::Str(String::new())
            } else {
                Json::Str(format!("https://{}", self.indexer.urn))
            }))),
            "imageName" => Ok(Some(match &self.image_name {
                Some(name) => FieldValue::Json(Json::Str(name.clone())),
                None => FieldValue::Null,
            })),
            _ => Ok(None),
        }
    }
}

/// `lsifUploadsAuditLogResolver` (`precise_index_resolver.go:430`).
struct AuditLogObject {
    log: AuditLogRow,
}

impl ObjectValue for AuditLogObject {
    fn type_name(&self) -> &'static str {
        "LSIFUploadAuditLog"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        let value = match field {
            "logTimestamp" => FieldValue::Json(Json::Str(datetime_rfc3339(
                self.log.log_timestamp,
            ))),
            "uploadDeletedAt" => FieldValue::Json(datetime_json(self.log.record_deleted_at)),
            "reason" => FieldValue::Json(match &self.log.reason {
                Some(reason) => Json::Str(reason.clone()),
                None => Json::Null,
            }),
            "changedColumns" => FieldValue::List {
                items: self
                    .log
                    .transition_columns
                    .iter()
                    .map(|transition| {
                        FieldValue::Object(Arc::new(AuditLogColumnChangeObject {
                            transition: transition.clone(),
                        }))
                    })
                    .collect(),
                element_type: Some("AuditLogColumnChange"),
            },
            // `resolverstubs.MarshalID("LSIFUpload", r.log.UploadID)`: an int
            // spec is JSON-marshaled without quotes.
            "uploadId" => FieldValue::Json(Json::Str(
                base64::engine::general_purpose::URL_SAFE
                    .encode(format!("LSIFUpload:{}", self.log.upload_id)),
            )),
            "inputCommit" => FieldValue::Json(Json::Str(self.log.commit.clone())),
            "inputRoot" => FieldValue::Json(Json::Str(self.log.root.clone())),
            "inputIndexer" => FieldValue::Json(Json::Str(self.log.indexer.clone())),
            "uploadedAt" => FieldValue::Json(Json::Str(datetime_rfc3339(self.log.uploaded_at))),
            "operation" => FieldValue::Json(Json::Str(self.log.operation.to_uppercase())),
            _ => return Ok(None),
        };
        Ok(Some(value))
    }
}

/// `auditLogColumnChangeResolver` (`precise_index_resolver.go:473`).
struct AuditLogColumnChangeObject {
    transition: serde_json::Value,
}

impl AuditLogColumnChangeObject {
    fn lookup(&self, key: &str) -> Option<String> {
        self.transition
            .get(key)
            .and_then(serde_json::Value::as_str)
            .map(str::to_owned)
    }
}

impl ObjectValue for AuditLogColumnChangeObject {
    fn type_name(&self) -> &'static str {
        "AuditLogColumnChange"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        let value = match field {
            "column" => FieldValue::Json(Json::Str(self.lookup("column").unwrap_or_default())),
            "old" => FieldValue::Json(match self.lookup("old") {
                Some(value) => Json::Str(value),
                None => Json::Null,
            }),
            "new" => FieldValue::Json(match self.lookup("new") {
                Some(value) => Json::Str(value),
                None => Json::Null,
            }),
            _ => return Ok(None),
        };
        Ok(Some(value))
    }
}

// ---------------------------------------------------------------------------
// Query.codeIntelSummary (`root_resolver_coverage.go`)
// ---------------------------------------------------------------------------

const SUMMARY_DEFAULT_PAGE_SIZE: i64 = 25;

struct CodeIntelSummaryObject {
    db: CodeIntelUploadsHandle,
    num_repositories_with_code_intelligence: i64,
}

impl ObjectValue for CodeIntelSummaryObject {
    fn type_name(&self) -> &'static str {
        "CodeIntelSummary"
    }

    fn field(
        &self,
        field: &str,
        arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "numRepositoriesWithCodeIntelligence" => Ok(Some(FieldValue::Json(Json::Int(
                self.num_repositories_with_code_intelligence,
            )))),
            "repositoriesWithErrors" => {
                let (page_size, offset) = summary_pagination(arguments);
                let (rows, total) = self
                    .db
                    .repository_ids_with_errors(offset, page_size)
                    .map_err(ExecError::new)?;
                Ok(Some(FieldValue::Object(Arc::new(
                    CodeIntelRepositoryWithErrorConnectionObject {
                        nodes: rows,
                        total_count: total as i32,
                        end_cursor: summary_end_cursor(offset, page_size, total),
                    },
                ))))
            }
            "repositoriesWithConfiguration" => {
                let (page_size, offset) = summary_pagination(arguments);
                let (rows, total) = self
                    .db
                    .repository_ids_with_configuration(offset, page_size)
                    .map_err(ExecError::new)?;
                Ok(Some(FieldValue::Object(Arc::new(
                    CodeIntelRepositoryWithConfigurationConnectionObject {
                        nodes: rows,
                        total_count: total as i32,
                        end_cursor: summary_end_cursor(offset, page_size, total),
                    },
                ))))
            }
            _ => Ok(None),
        }
    }
}

/// `pageSize := 25; offset := Atoi(after)` (error ignored).
fn summary_pagination(arguments: &Arguments) -> (i64, i64) {
    let page_size = arguments.i64("first").unwrap_or(SUMMARY_DEFAULT_PAGE_SIZE);
    let offset = arguments
        .string("after")
        .and_then(|after| after.parse::<i64>().ok())
        .unwrap_or(0);
    (page_size, offset)
}

fn summary_end_cursor(offset: i64, page_size: i64, total: i64) -> Option<String> {
    let new_offset = offset + page_size;
    (new_offset < total).then(|| new_offset.to_string())
}

/// `CodeIntelRepositorySummary` (`root_resolver_coverage.go:360-422`).
struct RepositorySummaryObject {
    nodes: Vec<PreciseIndexObject>,
    last_upload_retention_scan: Option<OffsetDateTime>,
    last_index_scan: Option<OffsetDateTime>,
    /// `conf.CodeIntelAutoIndexingEnabled()`. Structural inference itself
    /// needs the autoindexing service, which is not part of this seam; when
    /// enabled the list stays empty (documented gap).
    available_indexers: bool,
}

impl ObjectValue for RepositorySummaryObject {
    fn type_name(&self) -> &'static str {
        "CodeIntelRepositorySummary"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        let value = match field {
            // `var resolvers []` is nil for an empty summary (schema
            // violation in the baseline; reproduced as null).
            "recentActivity" => {
                if self.nodes.is_empty() {
                    FieldValue::Null
                } else {
                    FieldValue::List {
                        items: self
                            .nodes
                            .iter()
                            .map(|node| FieldValue::Object(Arc::new(node.clone())))
                            .collect(),
                        element_type: Some("PreciseIndex"),
                    }
                }
            }
            "lastUploadRetentionScan" => {
                FieldValue::Json(datetime_json(self.last_upload_retention_scan))
            }
            "lastIndexScan" => FieldValue::Json(datetime_json(self.last_index_scan)),
            // `make([]..., 0, len(...))`: always `[]` without inference.
            "availableIndexers" => FieldValue::List {
                items: Vec::new(),
                element_type: Some("InferredAvailableIndexers"),
            },
            "limitError" => FieldValue::Null,
            _ => return Ok(None),
        };
        let _ = self.available_indexers;
        Ok(Some(value))
    }
}

struct CodeIntelRepositoryObject {
    id: i32,
    name: String,
}

impl ObjectValue for CodeIntelRepositoryObject {
    fn type_name(&self) -> &'static str {
        "CodeIntelRepository"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "id" => Ok(Some(FieldValue::Json(Json::Str(
                crate::graphql_resolvers::RepositoryObject::relay_id(self.id),
            )))),
            "name" => Ok(Some(FieldValue::Json(Json::Str(self.name.clone())))),
            "url" => Ok(Some(FieldValue::Json(Json::Str(format!("/{}", self.name))))),
            "externalRepository" => Ok(Some(FieldValue::Null)),
            _ => Ok(None),
        }
    }
}

struct CodeIntelRepositoryWithErrorObject {
    row: RepositoryWithCountRow,
}

impl ObjectValue for CodeIntelRepositoryWithErrorObject {
    fn type_name(&self) -> &'static str {
        "CodeIntelRepositoryWithError"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "repository" => Ok(Some(FieldValue::Object(Arc::new(
                CodeIntelRepositoryObject {
                    id: self.row.repository_id,
                    name: self.row.repository_name.clone(),
                },
            )))),
            "count" => Ok(Some(FieldValue::Json(Json::Int(self.row.count)))),
            _ => Ok(None),
        }
    }
}

struct CodeIntelRepositoryWithErrorConnectionObject {
    nodes: Vec<RepositoryWithCountRow>,
    total_count: i32,
    end_cursor: Option<String>,
}

impl ObjectValue for CodeIntelRepositoryWithErrorConnectionObject {
    fn type_name(&self) -> &'static str {
        "CodeIntelRepositoryWithErrorConnection"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "nodes" => Ok(Some(FieldValue::List {
                items: self
                    .nodes
                    .iter()
                    .map(|row| {
                        FieldValue::Object(Arc::new(CodeIntelRepositoryWithErrorObject {
                            row: row.clone(),
                        }))
                    })
                    .collect(),
                element_type: Some("CodeIntelRepositoryWithError"),
            })),
            "totalCount" => Ok(Some(FieldValue::Json(Json::Int(i64::from(
                self.total_count,
            ))))),
            "pageInfo" => Ok(Some(FieldValue::Object(Arc::new(PageInfoObject::new(
                self.end_cursor.is_some(),
                self.end_cursor.clone(),
            ))))),
            _ => Ok(None),
        }
    }
}

struct IndexerWithCountObject {
    indexer: CodeIntelIndexer,
    count: i32,
}

impl ObjectValue for IndexerWithCountObject {
    fn type_name(&self) -> &'static str {
        "IndexerWithCount"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "indexer" => Ok(Some(indexer_field(&self.indexer.name, None))),
            "count" => Ok(Some(FieldValue::Json(Json::Int(i64::from(self.count))))),
            _ => Ok(None),
        }
    }
}

struct CodeIntelRepositoryWithConfigurationObject {
    row: RepositoryWithConfigurationRow,
}

impl ObjectValue for CodeIntelRepositoryWithConfigurationObject {
    fn type_name(&self) -> &'static str {
        "CodeIntelRepositoryWithConfiguration"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "repository" => Ok(Some(FieldValue::Object(Arc::new(
                CodeIntelRepositoryObject {
                    id: self.row.repository_id,
                    name: self.row.repository_name.clone(),
                },
            )))),
            "indexers" => {
                // `map[string]AvailableIndexer` (Go field names, no JSON tags):
                // `Indexer.CodeIntelIndexer` / `Roots []string`.
                let parsed: serde_json::Value =
                    serde_json::from_str(&self.row.available_indexers)
                        .map_err(|error| ExecError::new(error.to_string()))?;
                let mut items = Vec::new();
                if let Some(entries) = parsed.as_object() {
                    for (name, meta) in entries {
                        let roots = meta
                            .get("Roots")
                            .and_then(serde_json::Value::as_array)
                            .map(|roots| roots.len())
                            .unwrap_or(0);
                        items.push(FieldValue::Object(Arc::new(IndexerWithCountObject {
                            indexer: indexer_from_name(name),
                            count: roots as i32,
                        })));
                    }
                }
                Ok(Some(FieldValue::List {
                    items,
                    element_type: Some("IndexerWithCount"),
                }))
            }
            _ => Ok(None),
        }
    }
}

struct CodeIntelRepositoryWithConfigurationConnectionObject {
    nodes: Vec<RepositoryWithConfigurationRow>,
    total_count: i32,
    end_cursor: Option<String>,
}

impl ObjectValue for CodeIntelRepositoryWithConfigurationConnectionObject {
    fn type_name(&self) -> &'static str {
        "CodeIntelRepositoryWithConfigurationConnection"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "nodes" => Ok(Some(FieldValue::List {
                items: self
                    .nodes
                    .iter()
                    .map(|row| {
                        FieldValue::Object(Arc::new(
                            CodeIntelRepositoryWithConfigurationObject { row: row.clone() },
                        ))
                    })
                    .collect(),
                element_type: Some("CodeIntelRepositoryWithConfiguration"),
            })),
            "totalCount" => Ok(Some(FieldValue::Json(Json::Int(i64::from(
                self.total_count,
            ))))),
            "pageInfo" => Ok(Some(FieldValue::Object(Arc::new(PageInfoObject::new(
                self.end_cursor.is_some(),
                self.end_cursor.clone(),
            ))))),
            _ => Ok(None),
        }
    }
}

// ---------------------------------------------------------------------------
// Index steps
// ---------------------------------------------------------------------------

struct IndexStepsObject {
    job: AutoIndexJobRow,
}

impl IndexStepsObject {
    fn find_log(&self, predicate: impl Fn(&str) -> bool) -> Option<&ExecutionLogEntry> {
        self.job
            .execution_logs
            .iter()
            .find(|entry| predicate(&entry.key))
    }

    fn logs_with_prefix(&self, predicate: impl Fn(&str) -> bool) -> Vec<ExecutionLogEntryObject> {
        self.job
            .execution_logs
            .iter()
            .filter(|entry| predicate(&entry.key))
            .map(|entry| ExecutionLogEntryObject {
                entry: entry.clone(),
            })
            .collect()
    }
}

fn contains_any(key: &str, needles: &[String]) -> bool {
    needles.iter().any(|needle| key.contains(needle))
}

impl ObjectValue for IndexStepsObject {
    fn type_name(&self) -> &'static str {
        "IndexSteps"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "setup" => Ok(Some(FieldValue::List {
                items: self
                    .logs_with_prefix(|key| key.starts_with("setup."))
                    .into_iter()
                    .map(|entry| FieldValue::Object(Arc::new(entry)))
                    .collect(),
                element_type: Some("ExecutionLogEntry"),
            })),
            "teardown" => Ok(Some(FieldValue::List {
                items: self
                    .logs_with_prefix(|key| key.starts_with("teardown."))
                    .into_iter()
                    .map(|entry| FieldValue::Object(Arc::new(entry)))
                    .collect(),
                element_type: Some("ExecutionLogEntry"),
            })),
            "preIndex" => {
                let mut items = Vec::new();
                for (i, step) in self.job.docker_steps.iter().enumerate() {
                    let primary = vec![
                        format!("step.docker.pre-index.{i}"),
                        format!("step.kubernetes.pre-index.{i}"),
                    ];
                    let fallback = vec![format!("step.docker.{i}"), format!("step.kubernetes.{i}")];
                    let entry = self
                        .find_log(|key| contains_any(key, &primary))
                        .or_else(|| self.find_log(|key| contains_any(key, &fallback)))
                        .cloned();
                    items.push(FieldValue::Object(Arc::new(PreIndexStepObject {
                        step: step.clone(),
                        entry,
                    })));
                }
                Ok(Some(FieldValue::List {
                    items,
                    element_type: Some("PreIndexStep"),
                }))
            }
            "index" => {
                let entry = self
                    .find_log(|key| {
                        key.starts_with("step.docker.indexer")
                            || key.starts_with("step.kubernetes.indexer")
                    })
                    .or_else(|| {
                        let n = self.job.docker_steps.len();
                        self.find_log(|key| {
                            key.starts_with(&format!("step.docker.{n}"))
                                || key.starts_with(&format!("step.kubernetes.{n}"))
                        })
                    })
                    .cloned();
                Ok(Some(FieldValue::Object(Arc::new(IndexStepObject {
                    job: self.job.clone(),
                    entry,
                }))))
            }
            "upload" => {
                let entry = self
                    .find_log(|key| {
                        key.starts_with("step.docker.upload")
                            || key.starts_with("step.kubernetes.upload")
                            || key.starts_with("step.src.upload")
                    })
                    .or_else(|| self.find_log(|key| key.starts_with("step.src.0")))
                    .cloned();
                Ok(Some(match entry {
                    Some(entry) => {
                        FieldValue::Object(Arc::new(ExecutionLogEntryObject { entry }))
                    }
                    None => FieldValue::Null,
                }))
            }
            _ => Ok(None),
        }
    }
}

struct PreIndexStepObject {
    step: DockerStep,
    entry: Option<ExecutionLogEntry>,
}

impl ObjectValue for PreIndexStepObject {
    fn type_name(&self) -> &'static str {
        "PreIndexStep"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "root" => Ok(Some(FieldValue::Json(Json::Str(self.step.root.clone())))),
            "image" => Ok(Some(FieldValue::Json(Json::Str(self.step.image.clone())))),
            "commands" => Ok(Some(string_list(&self.step.commands))),
            "logEntry" => Ok(Some(match &self.entry {
                Some(entry) => FieldValue::Object(Arc::new(ExecutionLogEntryObject {
                    entry: entry.clone(),
                })),
                None => FieldValue::Null,
            })),
            _ => Ok(None),
        }
    }
}

struct IndexStepObject {
    job: AutoIndexJobRow,
    entry: Option<ExecutionLogEntry>,
}

impl ObjectValue for IndexStepObject {
    fn type_name(&self) -> &'static str {
        "IndexStep"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "commands" => Ok(Some(string_list(&self.job.local_steps))),
            "indexerArgs" => Ok(Some(string_list(&self.job.indexer_args))),
            "outfile" => Ok(Some(FieldValue::Json(Json::opt_str(Some(
                self.job.outfile.as_str(),
            ))))),
            "requestedEnvVars" => Ok(Some(if self.job.requested_env_vars.is_empty() {
                FieldValue::Null
            } else {
                string_list(&self.job.requested_env_vars)
            })),
            "logEntry" => Ok(Some(match &self.entry {
                Some(entry) => FieldValue::Object(Arc::new(ExecutionLogEntryObject {
                    entry: entry.clone(),
                })),
                None => FieldValue::Null,
            })),
            _ => Ok(None),
        }
    }
}

struct ExecutionLogEntryObject {
    entry: ExecutionLogEntry,
}

impl ObjectValue for ExecutionLogEntryObject {
    fn type_name(&self) -> &'static str {
        "ExecutionLogEntry"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "key" => Ok(Some(FieldValue::Json(Json::Str(self.entry.key.clone())))),
            "command" => Ok(Some(string_list(&self.entry.command))),
            "exitCode" => Ok(Some(match self.entry.exit_code {
                Some(code) => FieldValue::Json(Json::Int(code as i64)),
                None => FieldValue::Null,
            })),
            "startTime" => Ok(Some(FieldValue::Json(Json::Str(datetime_rfc3339(
                self.entry.start_time,
            ))))),
            "durationMilliseconds" => Ok(Some(match self.entry.duration_ms {
                Some(duration) => FieldValue::Json(Json::Int(duration as i64)),
                None => FieldValue::Null,
            })),
            "out" => Ok(Some(FieldValue::Json(Json::Str(String::new())))),
            _ => Ok(None),
        }
    }
}

/// `CodeIntelligenceRetentionPolicyMatchesConnection`
/// (`NewTotalCountConnectionResolver(resolvers, 0, totalCount)`).
struct RetentionPolicyConnectionObject {
    nodes: Vec<FieldValue>,
    total_count: i64,
}

impl ObjectValue for RetentionPolicyConnectionObject {
    fn type_name(&self) -> &'static str {
        "CodeIntelligenceRetentionPolicyMatchesConnection"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        let value = match field {
            "nodes" => FieldValue::List {
                items: self.nodes.clone(),
                element_type: Some("CodeIntelligenceRetentionPolicyMatch"),
            },
            "totalCount" => FieldValue::Json(Json::Int(self.total_count)),
            // `offset` is always 0 and the node count equals `totalCount`, so
            // the page never reports more pages (`resolvers/utils.go:137`).
            "pageInfo" => FieldValue::Object(Arc::new(PageInfoObject::new(false, None))),
            _ => return Ok(None),
        };
        Ok(Some(value))
    }
}

/// `CodeIntelligenceRetentionPolicyMatch` (`retentionPolicyMatcherResolver`).
struct RetentionPolicyMatchObject {
    row: RetentionPolicyMatchRow,
    repository: Option<(i32, String)>,
}

impl ObjectValue for RetentionPolicyMatchObject {
    fn type_name(&self) -> &'static str {
        "CodeIntelligenceRetentionPolicyMatch"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        let value = match field {
            "configurationPolicy" => match &self.row.policy {
                Some(policy) => FieldValue::Object(Arc::new(
                    RetentionConfigurationPolicyObject {
                        policy: policy.clone(),
                        repository: self.repository.clone(),
                    },
                )),
                None => FieldValue::Null,
            },
            "matches" => FieldValue::Json(Json::Bool(self.row.matched)),
            // `ProtectingCommits` returns `&policy.ProtectingCommits`: a nil
            // slice (direct match) renders as JSON `null`, a visible-commit
            // match as an array (schema comment says "empty", implementation
            // is null).
            "protectingCommits" => match &self.row.protecting_commits {
                Some(commits) => string_list(commits),
                None => FieldValue::Null,
            },
            _ => return Ok(None),
        };
        Ok(Some(value))
    }
}

/// `policiesgraphql.NewConfigurationPolicyResolver` as reached from a
/// retention match. The repository is pre-resolved (the baseline resolves it
/// lazily through `repoStore`).
struct RetentionConfigurationPolicyObject {
    policy: sg_db::codeintel_policies::ConfigurationPolicy,
    repository: Option<(i32, String)>,
}

impl ObjectValue for RetentionConfigurationPolicyObject {
    fn type_name(&self) -> &'static str {
        "CodeIntelligenceConfigurationPolicy"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        let policy = &self.policy;
        let value = match field {
            "id" => FieldValue::Json(Json::Str(
                crate::codeintel_policies_graphql::marshal_configuration_policy_id(policy.id),
            )),
            "name" => FieldValue::Json(Json::Str(policy.name.clone().unwrap_or_default())),
            "repository" => match &self.repository {
                Some((id, name)) => FieldValue::Object(Arc::new(ProjectRootRepositoryObject {
                    id: *id,
                    name: name.clone(),
                })),
                None => FieldValue::Null,
            },
            "repositoryPatterns" => match &policy.repository_patterns {
                Some(patterns) => string_list(patterns),
                None => FieldValue::Null,
            },
            "type" => match policy.policy_type.as_str() {
                "GIT_COMMIT" | "GIT_TAG" | "GIT_TREE" => {
                    FieldValue::Json(Json::Str(policy.policy_type.clone()))
                }
                other => return Err(ExecError::new(format!("unknown git object type {other}"))),
            },
            "pattern" => FieldValue::Json(Json::Str(policy.pattern.clone())),
            "protected" => FieldValue::Json(Json::Bool(policy.protected)),
            "retentionEnabled" => FieldValue::Json(Json::Bool(policy.retention_enabled)),
            "retentionDurationHours" => match policy.retention_duration_hours {
                Some(hours) => FieldValue::Json(Json::Int(i64::from(hours))),
                None => FieldValue::Null,
            },
            "retainIntermediateCommits" => {
                FieldValue::Json(Json::Bool(policy.retain_intermediate_commits))
            }
            "indexingEnabled" => FieldValue::Json(Json::Bool(policy.indexing_enabled)),
            "syntacticIndexingEnabled" => {
                FieldValue::Json(Json::Bool(policy.syntactic_indexing_enabled))
            }
            "indexCommitMaxAgeHours" => match policy.index_commit_max_age_hours {
                Some(hours) => FieldValue::Json(Json::Int(i64::from(hours))),
                None => FieldValue::Null,
            },
            "indexIntermediateCommits" => {
                FieldValue::Json(Json::Bool(policy.index_intermediate_commits))
            }
            "embeddingsEnabled" => FieldValue::Json(Json::Bool(policy.embeddings_enabled)),
            _ => return Ok(None),
        };
        Ok(Some(value))
    }
}

fn string_list(values: &[String]) -> FieldValue {
    FieldValue::List {
        items: values
            .iter()
            .map(|value| FieldValue::Json(Json::Str(value.clone())))
            .collect(),
        element_type: Some("String"),
    }
}
