#![forbid(unsafe_code)]

//! Code Intel uploads GraphQL read-only surface (M9.27d, first phase).
//!
//! Ports `internal/codeintel/uploads/transport/graphql`:
//!
//! - `Query.preciseIndexes` (`root_resolver_index_queries.go:26`) including
//!   `first`/`after` cursors, `repo`, `query`, `states` bifurcation,
//!   `indexerKey` expansion and the upload/index merge;
//! - the `PreciseIndex` read-only fields (`precise_index_resolver.go`) and the
//!   associated-record dataloader pre-submits (`dataloader.go`);
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
//! `dev/rs/docs/overviews/m9_27d_codeintel_uploads_overview.md`):
//! - `projectRoot`/`tags` need gitserver and return `null`/`[]`;
//! - `retentionPolicyOverview` resolves as an empty connection;
//! - `auditLogs` returns `null`, `dependencyOf`/`dependentOf` and
//!   `includeDeleted` return explicit errors (CTEs not ported);
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
    pub limit: i32,
    pub offset: i32,
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

// ---------------------------------------------------------------------------
// Database seam
// ---------------------------------------------------------------------------

pub trait CodeIntelUploadsDatabase: Send + Sync {
    /// `store.GetUploads` (uploads half of `PreciseIndexes`).
    fn get_uploads(&self, opts: &GetUploadsOptions) -> Result<(Vec<UploadRow>, i64), String>;
    /// `store.GetAutoIndexJobs` (`WithoutUpload: true`).
    fn get_auto_index_jobs(
        &self,
        opts: &GetAutoIndexJobsOptions,
    ) -> Result<(Vec<AutoIndexJobRow>, i64), String>;
    /// `store.GetUploadsByIDs` (dataloader backfill).
    fn get_uploads_by_ids(&self, ids: &[i64]) -> Result<Vec<UploadRow>, String>;
    /// `store.GetAutoIndexJobsByIDs` (dataloader backfill).
    fn get_auto_index_jobs_by_ids(&self, ids: &[i64]) -> Result<Vec<AutoIndexJobRow>, String>;
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
pub fn decode_precise_index_id(id: &str) -> Result<(Option<i64>, Option<i64>), String> {
    let decoded = base64::engine::general_purpose::URL_SAFE
        .decode(id)
        .map_err(|error| format!("unexpected precise index ID: {error}"))?;
    let text = String::from_utf8_lossy(&decoded);
    let Some((_kind, payload)) = text.split_once(':') else {
        return Err("unexpected precise index ID: invalid graphql.ID".to_owned());
    };
    // `relay.UnmarshalSpec` JSON-decodes the payload into a string.
    let payload: String = serde_json::from_str(payload)
        .map_err(|_| "unexpected precise index ID: invalid graphql.ID".to_owned())?;
    let parts: Vec<&str> = payload.split(':').collect();
    if parts.len() % 2 != 0 {
        return Err(
            "unexpected precise index ID: expected pairs of `U:<id>`, `I:<id>`".to_owned(),
        );
    }
    let mut upload_id = None;
    let mut index_id = None;
    let mut i = 0;
    while i + 1 < parts.len() {
        let value: i64 = parts[i + 1]
            .parse()
            .map_err(|_| "unexpected precise index ID: expected pairs of `U:<id>`, `I:<id>`".to_owned())?;
        match parts[i] {
            "U" => upload_id = Some(value),
            "I" => index_id = Some(value),
            _ => {
                return Err(
                    "unexpected precise index ID: expected pairs of `U:<id>`, `I:<id>`"
                        .to_owned(),
                )
            }
        }
        i += 2;
    }
    if upload_id.is_none() && index_id.is_none() {
        return Err(format!("unexpected precise index ID: invalid precise index id {id:?}"));
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
}

/// `rootResolver.PreciseIndexes` (`root_resolver_index_queries.go:26`).
pub fn precise_indexes(
    db: &CodeIntelUploadsHandle,
    arguments: &Arguments,
    _actor: Actor,
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
    let skip_indexes = !upload_states.is_empty() && index_states.is_empty();

    if arguments.string("dependencyOf").is_some() {
        return Err(ExecError::new(
            "dependencyOf is not supported by the Rust code-intel port",
        ));
    }
    if arguments.string("dependentOf").is_some() {
        return Err(ExecError::new(
            "dependentOf is not supported by the Rust code-intel port",
        ));
    }
    if arguments.bool("includeDeleted") == Some(true) {
        return Err(ExecError::new(
            "includeDeleted is not supported by the Rust code-intel port",
        ));
    }

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
                limit: page_size,
                offset: upload_offset,
            })
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
            })
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
            });
            u_idx += 1;
        } else {
            pairs.push(PreciseIndexRecord {
                upload: None,
                index: Some(auto_index_jobs[i_idx].clone()),
            });
            i_idx += 1;
        }
    }
    while u_idx < uploads.len() && (u_idx + i_idx) < page_size.max(0) as usize {
        pairs.push(PreciseIndexRecord {
            upload: Some(uploads[u_idx].clone()),
            index: None,
        });
        u_idx += 1;
    }
    while i_idx < auto_index_jobs.len() && (u_idx + i_idx) < page_size.max(0) as usize {
        pairs.push(PreciseIndexRecord {
            upload: None,
            index: Some(auto_index_jobs[i_idx].clone()),
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
        db.get_auto_index_jobs_by_ids(&index_ids)
            .map_err(ExecError::new)?
            .into_iter()
            .map(|job| (job.id, job))
            .collect()
    };
    let upload_by_id: HashMap<i64, UploadRow> = if upload_ids.is_empty() {
        HashMap::new()
    } else {
        db.get_uploads_by_ids(&upload_ids)
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
        nodes.push(PreciseIndexObject { record: pair });
    }

    Ok(FieldValue::Object(Arc::new(PreciseIndexConnectionObject {
        nodes,
        cursor: cursor.clone(),
        total_count: (total_upload_count + total_jobs_count) as i32,
    })))
}

/// Whether this module owns the root field.
pub fn is_root_field(field: &str) -> bool {
    field == "preciseIndexes"
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
        _ => Ok(None),
    }
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
}

impl ObjectValue for PreciseIndexObject {
    fn type_name(&self) -> &'static str {
        "PreciseIndex"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        let value = match field {
            "id" => FieldValue::Json(Json::Str(self.id())),
            "projectRoot" => FieldValue::Null,
            "inputCommit" => FieldValue::Json(Json::Str(self.input_commit())),
            "inputRoot" => FieldValue::Json(Json::Str(self.input_root())),
            "inputIndexer" => FieldValue::Json(Json::Str(self.input_indexer())),
            "tags" => FieldValue::List {
                items: Vec::new(),
                element_type: Some("String"),
            },
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
            "retentionPolicyOverview" => {
                // Not ported in this phase; the field is non-null, so return an
                // empty connection rather than an unported error.
                FieldValue::Object(Arc::new(EmptyRetentionPolicyConnectionObject))
            }
            "auditLogs" => FieldValue::Null,
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

struct EmptyRetentionPolicyConnectionObject;

impl ObjectValue for EmptyRetentionPolicyConnectionObject {
    fn type_name(&self) -> &'static str {
        "CodeIntelligenceRetentionPolicyMatchesConnection"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "nodes" => Ok(Some(FieldValue::List {
                items: Vec::new(),
                element_type: Some("CodeIntelligenceRetentionPolicyMatch"),
            })),
            "totalCount" => Ok(Some(FieldValue::Json(Json::Int(0)))),
            "pageInfo" => Ok(Some(FieldValue::Object(Arc::new(PageInfoObject::new(
                false, None,
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

fn string_list(values: &[String]) -> FieldValue {
    FieldValue::List {
        items: values
            .iter()
            .map(|value| FieldValue::Json(Json::Str(value.clone())))
            .collect(),
        element_type: Some("String"),
    }
}
