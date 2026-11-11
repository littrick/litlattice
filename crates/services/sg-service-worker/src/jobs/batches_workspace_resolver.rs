#![forbid(unsafe_code)]

//! `cmd/worker/internal/batches/{workspace_resolver_job.go,
//! workers/batch_spec_resolution_worker.go,
//! workers/batch_spec_workspace_creator.go}` +
//! `internal/batches/service/workspace_resolver.go` +
//! `internal/batches/store/{worker_batch_spec_resolution.go,
//! batch_spec_resolution_jobs.go}`: the `batches-workspace-resolver` job.
//!
//! The baseline job owns one routine:
//!
//! | routine | interval | work |
//! |---|---|---|
//! | `batch_changes_batch_spec_resolution_worker` | 1s (NumHandlers 5, heartbeat 15s) | dequeue one `batch_spec_resolution_jobs` row, resolve the batch spec into workspaces, persist them |
//!
//! The stalled resetter lives in the `batches-janitor` job
//! (`batch_changes_batch_spec_resolution_worker_resetter`), not here, so this
//! job returns exactly one routine. Store knobs: `StalledMaxAge` 60s,
//! `MaxNumResets` 60, `RetryAfter` 5s, `MaxNumRetries` 0 (the first error is
//! terminal: `mark_errored` writes `failed`).
//!
//! Ported core:
//!
//! * the worker store state machine (`Dequeue`/`Heartbeat`/`MarkComplete`/
//!   `MarkErrored`/`MarkFailed`) in [`ResolutionJobStore`];
//! * `findWorkspaces` (glob `in` matching, duplicate-match validation errors,
//!   root workspaces, file-match prefix filtering, stable sorting),
//!   `stepsForRepo`, `setDefaultQueryCount`, `determineDefaultPatternType`;
//! * `BuildImportChangesetSpecs` (`RepoFetcher` seam, missing-repository and
//!   external-ID error aggregation);
//! * the handler pipeline shape (`get spec → secrets → resolve → imports →
//!   one persistence step`), driven through [`ResolutionService`].
//!
//! Seams (docs/overviews/m9_9i_* §4): the batch-spec YAML parser, the
//! template evaluator, gitserver/search, the repository store and the
//! executor-secrets store are not ported. The production [`ResolutionService`]
//! returns an explicit `not ported` error when one of them is actually
//! needed; job construction still succeeds.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use sg_db::batches::{
    self, BatchSpecResolutionJobRow, BatchSpecRow, BatchSpecWorkspaceInsert,
    ImportChangesetSpecInsert,
};
use sg_db::OffsetDateTime;
use sqlx::PgPool;
use tokio_util::sync::CancellationToken;

use super::batches::batches_enabled;
use super::{Job, JobDeps};
use crate::framework::dbworker::{
    default_hostname, DbBackedRoutine, DbRecord, DbWorkerHandler, DbWorkerStore, WorkerOptions,
    WorkerRoutine,
};
use crate::framework::BackgroundRoutine;

/// `workspaceResolverJob.Description`.
pub const JOB_DESCRIPTION: &str = "";

/// `workerutil.WorkerOptions.Name`.
pub const WORKER_NAME: &str = "batch_changes_batch_spec_resolution_worker";
/// `workerutil.WorkerOptions.Description`.
pub const WORKER_DESCRIPTION: &str =
    "runs the workspace resolutions for batch specs, for batch changes running server-side";
/// `workerutil.WorkerOptions.NumHandlers`.
pub const NUM_HANDLERS: usize = 5;
/// `workerutil.WorkerOptions.Interval`.
pub const INTERVAL: Duration = Duration::from_secs(1);
/// `workerutil.WorkerOptions.HeartbeatInterval`.
pub const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(15);

/// `batchSpecResolutionMaxNumResets` (used by the janitor resetter).
pub const MAX_NUM_RESETS: i32 = 60;
/// `batchSpecResolutionMaxNumRetries` (0: terminal after one error).
pub const MAX_NUM_RETRIES: i32 = 0;
/// `dbworkerstore.Options.StalledMaxAge`.
pub const STALLED_MAX_AGE: Duration = Duration::from_secs(60);
/// `dbworkerstore.Options.RetryAfter`.
pub const RETRY_AFTER: Duration = Duration::from_secs(5);

/// The `not ported` message used by the production seams.
pub const NOT_PORTED: &str = "batches-workspace-resolver dependency is not ported to Rust yet";

/// `BatchSpecResolutionJobState`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BatchSpecResolutionJobState {
    Queued,
    Processing,
    Errored,
    Failed,
    Completed,
}

impl BatchSpecResolutionJobState {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::Processing => "processing",
            Self::Errored => "errored",
            Self::Failed => "failed",
            Self::Completed => "completed",
        }
    }
}

/// `btypes.BatchSpecResolutionJob`.
#[derive(Debug, Clone, PartialEq)]
pub struct BatchSpecResolutionJob {
    pub row: BatchSpecResolutionJobRow,
}

impl DbRecord for BatchSpecResolutionJob {
    fn record_id(&self) -> i64 {
        self.row.id
    }
}

/// `store.NewBatchSpecResolutionWorkerStore`.
#[derive(Clone)]
pub struct ResolutionJobStore {
    pool: PgPool,
}

impl ResolutionJobStore {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub fn pool(&self) -> &PgPool {
        &self.pool
    }
}

#[async_trait]
impl DbWorkerStore for ResolutionJobStore {
    type Record = BatchSpecResolutionJob;

    async fn dequeue(&self, worker_hostname: &str) -> Result<Option<Self::Record>, String> {
        batches::dequeue_batch_spec_resolution_job(
            &self.pool,
            OffsetDateTime::now_utc(),
            worker_hostname,
        )
        .await
        .map(|row| row.map(|row| BatchSpecResolutionJob { row }))
        .map_err(|err| err.to_string())
    }

    async fn heartbeat(&self, ids: &[String]) -> Result<(Vec<String>, Vec<String>), String> {
        let (known, canceled) = batches::heartbeat_batch_spec_resolution_jobs(
            &self.pool,
            OffsetDateTime::now_utc(),
            ids,
        )
        .await
        .map_err(|err| err.to_string())?;
        Ok((
            known.into_iter().map(|id| id.to_string()).collect(),
            canceled.into_iter().map(|id| id.to_string()).collect(),
        ))
    }

    async fn mark_complete(&self, record: &Self::Record) -> Result<bool, String> {
        batches::mark_batch_spec_resolution_job_complete(&self.pool, record.row.id)
            .await
            .map_err(|err| err.to_string())
    }

    async fn mark_errored(
        &self,
        record: &Self::Record,
        failure_message: &str,
    ) -> Result<bool, String> {
        batches::mark_batch_spec_resolution_job_errored(&self.pool, record.row.id, failure_message)
            .await
            .map_err(|err| err.to_string())
    }

    async fn mark_failed(
        &self,
        record: &Self::Record,
        failure_message: &str,
    ) -> Result<bool, String> {
        batches::mark_batch_spec_resolution_job_failed(&self.pool, record.row.id, failure_message)
            .await
            .map_err(|err| err.to_string())
    }
}

// ---------------------------------------------------------------------------
// Resolution core (pure).
// ---------------------------------------------------------------------------

/// `batcheslib.Step` (only the fields the resolver looks at).
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct Step {
    #[serde(default)]
    pub run: String,
    #[serde(default, rename = "if")]
    pub if_condition: String,
}

impl Step {
    /// `(*Step).IfCondition`.
    pub fn if_condition(&self) -> &str {
        &self.if_condition
    }
}

/// `batcheslib.WorkspaceConfiguration`.
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct WorkspaceConfiguration {
    #[serde(default, rename = "in")]
    pub in_glob: String,
    #[serde(default, rename = "rootAtLocationOf")]
    pub root_at_location_of: String,
    #[serde(default, rename = "onlyFetchWorkspace")]
    pub only_fetch_workspace: bool,
}

/// `batcheslib.BatchSpec` (the resolver's view).
#[derive(Debug, Clone, Default, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct BatchSpec {
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub description: String,
    #[serde(default)]
    pub steps: Vec<Step>,
    #[serde(default)]
    pub workspaces: Vec<WorkspaceConfiguration>,
    #[serde(default)]
    pub version: i64,
    #[serde(default, rename = "importChangesets")]
    pub import_changesets: Vec<ImportChangeset>,
}

/// `service.RepoRevision`.
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct RepoRevision {
    #[serde(default, rename = "id")]
    pub repo_id: i32,
    #[serde(default, rename = "name")]
    pub repo_name: String,
    #[serde(default)]
    pub branch: String,
    #[serde(default)]
    pub commit: String,
    #[serde(default, rename = "fileMatches")]
    pub file_matches: Vec<String>,
}

impl RepoRevision {
    pub fn key(&self) -> RepoRevKey {
        RepoRevKey {
            repo_id: self.repo_id,
            branch: self.branch.clone(),
            commit: self.commit.clone(),
        }
    }
}

/// `service.repoRevKey`.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct RepoRevKey {
    pub repo_id: i32,
    pub branch: String,
    pub commit: String,
}

/// `service.RepoWorkspace`.
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct RepoWorkspace {
    #[serde(flatten)]
    pub repo_revision: RepoRevision,
    #[serde(default)]
    pub path: String,
    #[serde(default, rename = "onlyFetchWorkspace")]
    pub only_fetch_workspace: bool,
    #[serde(default)]
    pub ignored: bool,
    #[serde(default)]
    pub unsupported: bool,
}

/// `service.directoryFinder`: the seam over the search API.
#[async_trait]
pub trait DirectoryFinder: Send + Sync {
    async fn find_directories_in_repos(
        &self,
        file_name: &str,
        batch_spec_version: i64,
        repos: &[RepoRevision],
    ) -> Result<BTreeMap<RepoRevKey, Vec<String>>, String>;
}

/// `findWorkspaces` ([workspace_resolver.go:571](../../../origin/repo/internal/batches/service/workspace_resolver.go#L571)).
///
/// `stepsForRepo` treats every non-empty `if` as dynamic (see the module
/// errata): it never statically skips a step, so a spec's steps are only
/// empty when the spec itself has no steps.
pub async fn find_workspaces(
    spec: &BatchSpec,
    finder: &dyn DirectoryFinder,
    repo_revs: &[RepoRevision],
) -> Result<Vec<RepoWorkspace>, String> {
    // Pre-compile all globs. An empty `in` matches everything.
    let mut workspace_matchers = BTreeMap::new();
    let mut errs: Vec<String> = Vec::new();
    for (idx, conf) in spec.workspaces.iter().enumerate() {
        let pattern = if conf.in_glob.is_empty() {
            "*"
        } else {
            conf.in_glob.as_str()
        };
        match sg_codeintel::glob::Glob::compile(pattern) {
            Ok(glob) => {
                workspace_matchers.insert(idx, glob);
            }
            Err(err) => errs.push(format!("failed to compile glob {pattern:?}: {err}")),
        }
    }
    if !errs.is_empty() {
        return Err(format_appended_errors(&errs));
    }

    let mut root: Vec<RepoRevision> = Vec::new();
    let mut matched: BTreeMap<usize, Vec<RepoRevision>> = BTreeMap::new();
    for repo_rev in repo_revs {
        let mut found = false;
        for (idx, conf) in spec.workspaces.iter().enumerate() {
            if !workspace_matchers[&idx].matches(&repo_rev.repo_name) {
                continue;
            }
            if found {
                errs.push(format!(
                    "repository {} matches multiple workspaces.in globs in the batch spec. glob: {:?}",
                    repo_rev.repo_name, conf.in_glob
                ));
                continue;
            }
            matched.entry(idx).or_default().push(repo_rev.clone());
            found = true;
        }
        if !found {
            root.push(repo_rev.clone());
        }
    }
    if !errs.is_empty() {
        return Err(format_appended_errors(&errs));
    }

    struct RepoWorkspaces {
        repo_revision: RepoRevision,
        paths: Vec<String>,
        only_fetch_workspace: bool,
    }

    let mut workspaces_by_repo_rev: BTreeMap<RepoRevKey, RepoWorkspaces> = BTreeMap::new();
    for (idx, repo_revs) in &matched {
        let conf = &spec.workspaces[*idx];
        let repo_rev_dirs = finder
            .find_directories_in_repos(
                &conf.root_at_location_of,
                spec.version,
                repo_revs.as_slice(),
            )
            .await
            .map_err(ResolutionError::executor)?;

        let repo_revs_by_key: BTreeMap<RepoRevKey, RepoRevision> = repo_revs
            .iter()
            .map(|repo_rev| (repo_rev.key(), repo_rev.clone()))
            .collect();

        for (key, dirs) in repo_rev_dirs {
            if dirs.is_empty() {
                continue;
            }
            let Some(repo_revision) = repo_revs_by_key.get(&key).cloned() else {
                continue;
            };
            workspaces_by_repo_rev.insert(
                key,
                RepoWorkspaces {
                    repo_revision,
                    paths: dirs,
                    only_fetch_workspace: conf.only_fetch_workspace,
                },
            );
        }
    }

    for repo_rev in &root {
        let key = repo_rev.key();
        match workspaces_by_repo_rev.get_mut(&key) {
            Some(existing) => existing.paths.push(String::new()),
            None => {
                workspaces_by_repo_rev.insert(
                    key,
                    RepoWorkspaces {
                        repo_revision: repo_rev.clone(),
                        paths: vec![String::new()],
                        only_fetch_workspace: false,
                    },
                );
            }
        }
    }

    let mut workspaces: Vec<RepoWorkspace> = Vec::new();
    for workspace in workspaces_by_repo_rev.into_values() {
        for path in &workspace.paths {
            let fetch_workspace = workspace.only_fetch_workspace && !path.is_empty();
            let paths: Vec<String> = workspace
                .repo_revision
                .file_matches
                .iter()
                .filter(|probe| probe.starts_with(path))
                .cloned()
                .collect();

            let repo_revision = RepoRevision {
                file_matches: paths,
                ..workspace.repo_revision.clone()
            };
            let steps = steps_for_repo(spec, &repo_revision)?;
            if steps.is_empty() {
                continue;
            }
            workspaces.push(RepoWorkspace {
                repo_revision,
                path: path.clone(),
                only_fetch_workspace: fetch_workspace,
                ignored: false,
                unsupported: false,
            });
        }
    }

    // Stable sort by repository name then path.
    workspaces.sort_by(|a, b| {
        a.repo_revision
            .repo_name
            .cmp(&b.repo_revision.repo_name)
            .then_with(|| a.path.cmp(&b.path))
    });
    Ok(workspaces)
}

/// `lib/errors.Append` (`cockroachdb/errors` `MultiError`) rendering: a
/// single error keeps its message; multiple errors are prefixed with a count
/// and bulleted. Used by `findWorkspaces` and `BuildImportChangesetSpecs`.
fn format_appended_errors(errs: &[String]) -> String {
    match errs {
        [] => String::new(),
        [one] => one.clone(),
        many => {
            let mut out = format!("{} errors occurred:", many.len());
            for err in many {
                out.push_str("\n\t* ");
                out.push_str(err);
            }
            out
        }
    }
}

/// `stepsForRepo` ([workspace_resolver.go:819](../../../origin/repo/internal/batches/service/workspace_resolver.go#L819)).
///
/// Seam: `template.IsStaticBool` is not ported. A non-empty condition is
/// treated as dynamic (the step is kept), which matches every case except a
/// statically-false condition.
pub fn steps_for_repo<'a>(
    spec: &'a BatchSpec,
    _repo: &RepoRevision,
) -> Result<Vec<&'a Step>, String> {
    let mut task_steps: Vec<&Step> = Vec::with_capacity(spec.steps.len());
    for step in &spec.steps {
        if step.if_condition().is_empty() {
            task_steps.push(step);
            continue;
        }
        // Dynamic condition (or an unsupported static evaluation): keep it.
        task_steps.push(step);
    }
    Ok(task_steps)
}

/// `hardCodedCount`.
pub const HARD_CODED_COUNT: &str = " count:all";

/// `setDefaultQueryCount` ([workspace_resolver.go:467](../../../origin/repo/internal/batches/service/workspace_resolver.go#L467)).
pub fn set_default_query_count(query: &str) -> String {
    use std::sync::OnceLock;
    static RE: OnceLock<regex::Regex> = OnceLock::new();
    let re =
        RE.get_or_init(|| regex::Regex::new(r"\bcount:(\d+|all)\b").expect("query count regex"));
    if re.is_match(query) {
        return query.to_owned();
    }
    format!("{query}{HARD_CODED_COUNT}")
}

/// `determineDefaultPatternType` ([workspace_resolver.go:353](../../../origin/repo/internal/batches/service/workspace_resolver.go#L353)).
pub fn determine_default_pattern_type(batch_spec_version: i64) -> &'static str {
    if batch_spec_version <= 1 {
        "standard"
    } else {
        "keyword"
    }
}

/// `.batchignore` path.
pub const BATCH_IGNORE_FILE_PATH: &str = ".batchignore";

/// The subset of `gitserver.Client` used by the ignored-repository scan.
#[async_trait]
pub trait GitserverStat: Send + Sync {
    /// `Stat`; `Ok(None)` means `os.IsNotExist`, `Err` is any other failure.
    async fn stat(&self, repo_name: &str, commit: &str, path: &str)
        -> Result<Option<bool>, String>;
}

/// `hasBatchIgnoreFile` ([workspace_resolver.go:443](../../../origin/repo/internal/batches/service/workspace_resolver.go#L443)).
pub async fn has_batch_ignore_file(
    gitserver: &dyn GitserverStat,
    repo: &RepoRevision,
) -> Result<bool, String> {
    match gitserver
        .stat(&repo.repo_name, &repo.commit, BATCH_IGNORE_FILE_PATH)
        .await?
    {
        None => Ok(false),
        Some(is_regular) => {
            if is_regular {
                Ok(true)
            } else {
                Err(format!("not a blob: {:?}", BATCH_IGNORE_FILE_PATH))
            }
        }
    }
}

/// `batcheslib.ImportChangeset`.
#[derive(Debug, Clone, Default, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct ImportChangeset {
    #[serde(default)]
    pub repository: String,
    #[serde(default, rename = "externalIDs")]
    pub external_ids: Vec<serde_json::Value>,
}

/// `batcheslib.ChangesetSpec` as built by `BuildImportChangesetSpecs`.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct ImportChangesetSpec {
    #[serde(rename = "baseRepository")]
    pub base_repository: String,
    #[serde(rename = "externalID")]
    pub external_id: String,
}

/// `lib/batches.RepoFetcher`.
#[async_trait]
pub trait RepoFetcher: Send + Sync {
    async fn fetch(&self, repo_names: &[String]) -> Result<BTreeMap<String, String>, String>;
}

/// `ParseChangesetSpecExternalID` ([changeset_spec.go:38](../../../origin/repo/lib/batches/changeset_spec.go#L38)).
pub fn parse_changeset_spec_external_id(value: &serde_json::Value) -> Result<String, String> {
    match value {
        serde_json::Value::String(value) => Ok(value.clone()),
        serde_json::Value::Number(number) => {
            if let Some(value) = number.as_i64() {
                Ok(value.to_string())
            } else if let Some(value) = number.as_u64() {
                Ok(value.to_string())
            } else if let Some(value) = number.as_f64() {
                Ok(format_go_float(value))
            } else {
                Err("cannot convert value into a valid external ID: expected string or int".into())
            }
        }
        other => Err(format!(
            "cannot convert value of type {} into a valid external ID: expected string or int",
            go_type_name(other)
        )),
    }
}

fn format_go_float(value: f64) -> String {
    // `strconv.FormatFloat(v, 'f', -1, 64)` for the corpus' simple values.
    let mut text = format!("{value}");
    if text.contains('.') {
        while text.ends_with('0') {
            text.pop();
        }
        if text.ends_with('.') {
            text.pop();
        }
    }
    text
}

fn go_type_name(value: &serde_json::Value) -> &'static str {
    match value {
        serde_json::Value::Null => "interface {}",
        serde_json::Value::Bool(_) => "bool",
        serde_json::Value::Number(_) => "float64",
        serde_json::Value::String(_) => "string",
        serde_json::Value::Array(_) => "[]interface {}",
        serde_json::Value::Object(_) => "map[string]interface {}",
    }
}

/// `BuildImportChangesetSpecs` ([changeset_specs.go:170](../../../origin/repo/lib/batches/changeset_specs.go#L170)).
///
/// Returns the built specs and the aggregated error (if any). A fetcher error
/// aborts with no specs.
pub async fn build_import_changeset_specs(
    imports: &[ImportChangeset],
    fetcher: &dyn RepoFetcher,
) -> (Vec<ImportChangesetSpec>, Option<String>) {
    if imports.is_empty() {
        return (Vec::new(), None);
    }

    let mut repo_names = Vec::with_capacity(imports.len());
    for import in imports {
        repo_names.push(import.repository.clone());
    }

    let repo_name_ids = match fetcher.fetch(&repo_names).await {
        Ok(ids) => ids,
        Err(err) => return (Vec::new(), Some(err)),
    };

    let mut specs = Vec::new();
    let mut errs: Vec<String> = Vec::new();
    for import in imports {
        let Some(repo_id) = repo_name_ids.get(&import.repository) else {
            errs.push(format!("repository {:?} not found", import.repository));
            continue;
        };
        for external_id in &import.external_ids {
            match parse_changeset_spec_external_id(external_id) {
                Ok(external_id) => specs.push(ImportChangesetSpec {
                    base_repository: repo_id.clone(),
                    external_id,
                }),
                Err(err) => errs.push(err),
            }
        }
    }

    let error = if errs.is_empty() {
        None
    } else {
        Some(format_appended_errors(&errs))
    };
    (specs, error)
}

// ---------------------------------------------------------------------------
// Handler.
// ---------------------------------------------------------------------------

/// Which stage a resolution error came from; the worker marks every stage
/// `errored` (terminal `failed` under `MaxNumRetries = 0`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResolutionErrorKind {
    /// The workspace resolver (gitserver/search/repo store seam) failed.
    Unresolved,
    /// `importChangesets` fetching/building failed.
    Import,
    /// Any other seam (parser, secrets, persistence) failed.
    Executor,
}

/// A classified resolution error.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolutionError {
    pub kind: ResolutionErrorKind,
    pub message: String,
}

impl ResolutionError {
    pub fn new(kind: ResolutionErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
        }
    }

    pub fn executor(message: impl Into<String>) -> String {
        Self::new(ResolutionErrorKind::Executor, message).message
    }
}

/// Batches spec as read from the store (`btypes.BatchSpec` subset).
#[derive(Debug, Clone, Default, PartialEq)]
pub struct StoredBatchSpec {
    pub id: i64,
    pub raw_spec: String,
    pub user_id: Option<i32>,
    pub namespace_user_id: Option<i32>,
    pub namespace_org_id: Option<i32>,
    pub allow_unsupported: bool,
    pub allow_ignored: bool,
    pub no_cache: bool,
    /// The typed `spec` column (the resolver's input).
    pub spec: BatchSpec,
}

impl From<BatchSpecRow> for StoredBatchSpec {
    fn from(row: BatchSpecRow) -> Self {
        let spec = serde_json::from_value(row.spec).unwrap_or_default();
        Self {
            id: row.id,
            raw_spec: row.raw_spec,
            user_id: row.user_id,
            namespace_user_id: row.namespace_user_id,
            namespace_org_id: row.namespace_org_id,
            allow_unsupported: row.allow_unsupported,
            allow_ignored: row.allow_ignored,
            no_cache: row.no_cache,
            spec,
        }
    }
}

/// The persistence payload `process` hands to [`ResolutionService::persist`]:
/// the resolved workspaces plus the import-built changeset specs.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ResolutionOutput {
    pub workspaces: Vec<RepoWorkspace>,
    pub imports: Vec<ImportChangesetSpec>,
}

/// The handler's external dependencies and persistence seam.
#[async_trait]
pub trait ResolutionService: Send + Sync {
    /// `store.GetBatchSpec`.
    async fn get_batch_spec(&self, id: i64) -> Result<StoredBatchSpec, String>;

    /// `batcheslib.ParseBatchSpec(spec.RawSpec)`.
    async fn parse_batch_spec(&self, raw: &str) -> Result<BatchSpec, String>;

    /// `spec.Spec.RequiredEnvVars()` + `ExecutorSecrets.List` + `Value`.
    async fn required_env_vars(
        &self,
        spec: &StoredBatchSpec,
        parsed: &BatchSpec,
    ) -> Result<Vec<String>, String>;

    /// `service.NewWorkspaceResolver(...).ResolveWorkspacesForBatchSpec`.
    async fn resolve_workspaces(&self, spec: &BatchSpec) -> Result<Vec<RepoWorkspace>, String>;

    /// `changesetSpecsForImports`'s `Repos().List` lookup (name → GraphQL id).
    async fn import_repo_ids(
        &self,
        repo_names: &[String],
    ) -> Result<BTreeMap<String, String>, String>;

    /// The single persistence transaction.
    async fn persist(
        &self,
        spec: &StoredBatchSpec,
        output: &ResolutionOutput,
    ) -> Result<(), String>;
}

/// `batchSpecWorkspaceCreator`.
pub struct BatchSpecWorkspaceCreator {
    service: Arc<dyn ResolutionService>,
}

impl BatchSpecWorkspaceCreator {
    pub fn new(service: Arc<dyn ResolutionService>) -> Self {
        Self { service }
    }

    /// `(*batchSpecWorkspaceCreator).process`.
    ///
    /// Order: read spec → parse → secrets → resolve → imports → persist. The
    /// actor/initiator substitution happens before this call in the baseline
    /// handler; actor identity is not modelled here.
    pub async fn process(
        &self,
        job: &BatchSpecResolutionJob,
    ) -> Result<ResolutionOutput, ResolutionError> {
        let spec = self
            .service
            .get_batch_spec(i64::from(job.row.batch_spec_id))
            .await
            .map_err(|err| ResolutionError::new(ResolutionErrorKind::Executor, err))?;

        let parsed = self
            .service
            .parse_batch_spec(&spec.raw_spec)
            .await
            .map_err(|err| ResolutionError::new(ResolutionErrorKind::Executor, err))?;

        self.service
            .required_env_vars(&spec, &parsed)
            .await
            .map_err(|err| ResolutionError::new(ResolutionErrorKind::Executor, err))?;

        let workspaces = self
            .service
            .resolve_workspaces(&parsed)
            .await
            .map_err(|err| ResolutionError::new(ResolutionErrorKind::Unresolved, err))?;

        let imports = if parsed.import_changesets.is_empty() {
            (Vec::new(), None)
        } else {
            let fetcher = ServiceRepoFetcher {
                service: Arc::clone(&self.service),
            };
            build_import_changeset_specs(&parsed.import_changesets, &fetcher).await
        };
        let (imports, import_error) = imports;
        if let Some(err) = import_error {
            return Err(ResolutionError::new(ResolutionErrorKind::Import, err));
        }

        let output = ResolutionOutput {
            workspaces,
            imports,
        };
        self.service
            .persist(&spec, &output)
            .await
            .map_err(|err| ResolutionError::new(ResolutionErrorKind::Executor, err))?;
        Ok(output)
    }
}

struct ServiceRepoFetcher {
    service: Arc<dyn ResolutionService>,
}

#[async_trait]
impl RepoFetcher for ServiceRepoFetcher {
    async fn fetch(&self, repo_names: &[String]) -> Result<BTreeMap<String, String>, String> {
        self.service.import_repo_ids(repo_names).await
    }
}

/// `workerutil.HandlerFunc` adapter: a handler error (including a classified
/// resolution error) is returned as its message; the worker then marks the
/// record errored (`failed` under `MaxNumRetries = 0`).
pub struct BatchSpecResolutionHandler {
    creator: BatchSpecWorkspaceCreator,
}

impl BatchSpecResolutionHandler {
    pub fn new(service: Arc<dyn ResolutionService>) -> Self {
        Self {
            creator: BatchSpecWorkspaceCreator::new(service),
        }
    }
}

#[async_trait]
impl DbWorkerHandler<ResolutionJobStore> for BatchSpecResolutionHandler {
    async fn handle(
        &self,
        _ctx: CancellationToken,
        record: &BatchSpecResolutionJob,
    ) -> Result<(), String> {
        self.creator
            .process(record)
            .await
            .map(|_| ())
            .map_err(|err| err.message)
    }
}

// ---------------------------------------------------------------------------
// Production seams.
// ---------------------------------------------------------------------------

/// Production [`ResolutionService`]: reads the spec from `sg-db`; the parser,
/// secrets, resolver and import repo lookup return [`NOT_PORTED`] when
/// reached. Persistence writes workspaces and imports through `sg-db`.
pub struct ProductionResolutionService {
    pool: PgPool,
}

impl ProductionResolutionService {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl ResolutionService for ProductionResolutionService {
    async fn get_batch_spec(&self, id: i64) -> Result<StoredBatchSpec, String> {
        batches::get_batch_spec(&self.pool, id)
            .await
            .map_err(|err| err.to_string())?
            .map(StoredBatchSpec::from)
            .ok_or_else(|| format!("batch spec {id} not found"))
    }

    async fn parse_batch_spec(&self, _raw: &str) -> Result<BatchSpec, String> {
        Err(NOT_PORTED.to_owned())
    }

    async fn required_env_vars(
        &self,
        _spec: &StoredBatchSpec,
        _parsed: &BatchSpec,
    ) -> Result<Vec<String>, String> {
        // No `RequiredEnvVars` information is available without the parser;
        // reaching this point means the parser seam already failed.
        Err(NOT_PORTED.to_owned())
    }

    async fn resolve_workspaces(&self, _spec: &BatchSpec) -> Result<Vec<RepoWorkspace>, String> {
        Err(NOT_PORTED.to_owned())
    }

    async fn import_repo_ids(
        &self,
        _repo_names: &[String],
    ) -> Result<BTreeMap<String, String>, String> {
        Err(NOT_PORTED.to_owned())
    }

    async fn persist(
        &self,
        spec: &StoredBatchSpec,
        output: &ResolutionOutput,
    ) -> Result<(), String> {
        let workspaces: Vec<BatchSpecWorkspaceInsert> = output
            .workspaces
            .iter()
            .map(|workspace| BatchSpecWorkspaceInsert {
                batch_spec_id: spec.id,
                changeset_spec_ids: Vec::new(),
                repo_id: workspace.repo_revision.repo_id,
                branch: workspace.repo_revision.branch.clone(),
                commit: workspace.repo_revision.commit.clone(),
                path: workspace.path.clone(),
                file_matches: workspace.repo_revision.file_matches.clone(),
                only_fetch_workspace: workspace.only_fetch_workspace,
                ignored: workspace.ignored,
                unsupported: workspace.unsupported,
                skipped: false,
                cached_result_found: false,
                step_cache_results: serde_json::json!({}),
            })
            .collect();
        batches::create_batch_spec_workspaces(&self.pool, &workspaces)
            .await
            .map_err(|err| err.to_string())?;

        let imports: Vec<ImportChangesetSpecInsert> = output
            .imports
            .iter()
            .filter_map(|import| {
                // The GraphQL id encodes the repository id; the production
                // path would decode it (`relay.UnmarshalSpec`). Without the
                // GraphQL package we only persist ids we can decode.
                let repo_id = decode_repository_graphql_id(&import.base_repository)?;
                Some(ImportChangesetSpecInsert {
                    rand_id: uuid::Uuid::new_v4().to_string(),
                    batch_spec_id: spec.id,
                    repo_id,
                    user_id: spec.user_id,
                    external_id: import.external_id.clone(),
                })
            })
            .collect();
        batches::create_import_changeset_specs(&self.pool, &imports)
            .await
            .map_err(|err| err.to_string())
    }
}

/// `relay.UnmarshalID("Repository", id)`: decode the base64 GraphQL id
/// `Repository:<id>`.
fn decode_repository_graphql_id(id: &str) -> Option<i32> {
    use base64::Engine;
    let decoded = base64::engine::general_purpose::STANDARD.decode(id).ok()?;
    let decoded = String::from_utf8(decoded).ok()?;
    decoded.strip_prefix("Repository:")?.parse::<i32>().ok()
}

// ---------------------------------------------------------------------------
// Job.
// ---------------------------------------------------------------------------

/// `workspaceResolverJob`.
#[derive(Default)]
pub struct BatchesWorkspaceResolver;

impl BatchesWorkspaceResolver {
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl Job for BatchesWorkspaceResolver {
    fn description(&self) -> String {
        JOB_DESCRIPTION.to_owned()
    }

    async fn routines(
        &self,
        deps: &JobDeps,
        _setup_ctx: CancellationToken,
    ) -> Result<Vec<BackgroundRoutine>, String> {
        if !batches_enabled() {
            return Ok(Vec::new());
        }

        let store = Arc::new(ResolutionJobStore::new(deps.frontend.clone()));
        let handler = Arc::new(BatchSpecResolutionHandler::new(Arc::new(
            ProductionResolutionService::new(deps.frontend.clone()),
        )));
        let worker = WorkerRoutine::new(
            store,
            handler,
            WorkerOptions {
                name: WORKER_NAME.to_owned(),
                description: WORKER_DESCRIPTION.to_owned(),
                interval: INTERVAL,
                heartbeat_interval: HEARTBEAT_INTERVAL,
                num_handlers: NUM_HANDLERS,
                worker_hostname: default_hostname(),
            },
        );
        Ok(vec![BackgroundRoutine::DbBacked(
            Arc::new(worker) as Arc<dyn DbBackedRoutine>
        )])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    struct TestFinder {
        results: BTreeMap<RepoRevKey, Vec<String>>,
        error: Option<String>,
    }

    #[async_trait]
    impl DirectoryFinder for TestFinder {
        async fn find_directories_in_repos(
            &self,
            _file_name: &str,
            _batch_spec_version: i64,
            _repos: &[RepoRevision],
        ) -> Result<BTreeMap<RepoRevKey, Vec<String>>, String> {
            match &self.error {
                Some(error) => Err(error.clone()),
                None => Ok(self.results.clone()),
            }
        }
    }

    struct TestFetcher {
        repos: BTreeMap<String, String>,
        error: Option<String>,
    }

    #[async_trait]
    impl RepoFetcher for TestFetcher {
        async fn fetch(&self, _repo_names: &[String]) -> Result<BTreeMap<String, String>, String> {
            match &self.error {
                Some(error) => Err(error.clone()),
                None => Ok(self.repos.clone()),
            }
        }
    }

    fn repo_rev(id: i32, name: &str, file_matches: &[&str]) -> RepoRevision {
        RepoRevision {
            repo_id: id,
            repo_name: name.to_owned(),
            branch: "main".to_owned(),
            commit: "abc".to_owned(),
            file_matches: file_matches
                .iter()
                .map(|value| (*value).to_owned())
                .collect(),
        }
    }

    fn spec(steps: usize, workspaces: Vec<WorkspaceConfiguration>) -> BatchSpec {
        BatchSpec {
            version: 2,
            steps: (0..steps)
                .map(|index| Step {
                    run: format!("echo {index}"),
                    if_condition: String::new(),
                })
                .collect(),
            workspaces,
            ..BatchSpec::default()
        }
    }

    fn job(batch_spec_id: i32) -> BatchSpecResolutionJob {
        let now = OffsetDateTime::now_utc();
        BatchSpecResolutionJob {
            row: BatchSpecResolutionJobRow {
                id: 1,
                batch_spec_id,
                initiator_id: 0,
                state: "processing".to_owned(),
                failure_message: None,
                started_at: None,
                finished_at: None,
                process_after: None,
                num_resets: 0,
                num_failures: 0,
                worker_hostname: String::new(),
                created_at: now,
                updated_at: now,
            },
        }
    }

    #[test]
    fn query_count_defaults_and_pattern_type() {
        assert_eq!(set_default_query_count(""), " count:all");
        assert_eq!(set_default_query_count("count:10"), "count:10");
        assert_eq!(set_default_query_count("r:foo"), "r:foo count:all");
        assert_eq!(
            set_default_query_count("r:foo count:"),
            "r:foo count: count:all"
        );
        assert_eq!(determine_default_pattern_type(0), "standard");
        assert_eq!(determine_default_pattern_type(1), "standard");
        assert_eq!(determine_default_pattern_type(2), "keyword");
    }

    #[test]
    fn multi_error_rendering_matches_baseline() {
        assert_eq!(format_appended_errors(&["one".to_owned()]), "one");
        assert_eq!(
            format_appended_errors(&["one".to_owned(), "two".to_owned()]),
            "2 errors occurred:\n\t* one\n\t* two"
        );
    }

    #[tokio::test]
    async fn find_workspaces_roots_globs_and_file_matches() {
        let configuration = WorkspaceConfiguration {
            in_glob: "github.com/a/*".to_owned(),
            root_at_location_of: "package.json".to_owned(),
            only_fetch_workspace: true,
        };
        let mut results = BTreeMap::new();
        results.insert(
            RepoRevKey {
                repo_id: 1,
                branch: "main".to_owned(),
                commit: "abc".to_owned(),
            },
            vec!["x/y".to_owned()],
        );
        let finder = TestFinder {
            results,
            error: None,
        };
        let repos = vec![
            repo_rev(1, "github.com/a/b", &["x/y/z.go"]),
            repo_rev(2, "github.com/z/z", &[]),
        ];
        let workspaces = find_workspaces(&spec(1, vec![configuration]), &finder, &repos)
            .await
            .expect("workspaces");
        assert_eq!(workspaces.len(), 2);
        assert_eq!(workspaces[0].repo_revision.repo_name, "github.com/a/b");
        assert_eq!(workspaces[0].path, "x/y");
        assert!(workspaces[0].only_fetch_workspace);
        assert_eq!(workspaces[0].repo_revision.file_matches, vec!["x/y/z.go"]);
        assert_eq!(workspaces[1].repo_revision.repo_name, "github.com/z/z");
        assert_eq!(workspaces[1].path, "");
        assert!(!workspaces[1].only_fetch_workspace);
    }

    #[tokio::test]
    async fn find_workspaces_duplicate_and_finder_errors() {
        let duplicate = spec(
            1,
            vec![
                WorkspaceConfiguration {
                    in_glob: "github.com/a/*".to_owned(),
                    ..WorkspaceConfiguration::default()
                },
                WorkspaceConfiguration {
                    in_glob: "*".to_owned(),
                    ..WorkspaceConfiguration::default()
                },
            ],
        );
        let finder = TestFinder {
            results: BTreeMap::new(),
            error: None,
        };
        let error = find_workspaces(&duplicate, &finder, &[repo_rev(1, "github.com/a/b", &[])])
            .await
            .expect_err("duplicate");
        assert_eq!(
            error,
            "repository github.com/a/b matches multiple workspaces.in globs in the batch spec. glob: \"*\""
        );

        let finder = TestFinder {
            results: BTreeMap::new(),
            error: Some("search backend unavailable".to_owned()),
        };
        let configuration = WorkspaceConfiguration {
            in_glob: "*".to_owned(),
            ..WorkspaceConfiguration::default()
        };
        let error = find_workspaces(
            &spec(1, vec![configuration]),
            &finder,
            &[repo_rev(1, "github.com/a/b", &[])],
        )
        .await
        .expect_err("finder");
        assert_eq!(error, "search backend unavailable");
    }

    #[tokio::test]
    async fn import_specs_and_external_id_errors() {
        let imports = vec![ImportChangeset {
            repository: "github.com/a/b".to_owned(),
            external_ids: vec![serde_json::json!("123"), serde_json::json!(456)],
        }];
        let mut repos = BTreeMap::new();
        repos.insert("github.com/a/b".to_owned(), "repo-id".to_owned());
        let (specs, error) =
            build_import_changeset_specs(&imports, &TestFetcher { repos, error: None }).await;
        assert!(error.is_none());
        assert_eq!(specs.len(), 2);
        assert_eq!(specs[1].external_id, "456");

        let missing = vec![
            ImportChangeset {
                repository: "github.com/x/y".to_owned(),
                external_ids: vec![serde_json::json!("1")],
            },
            ImportChangeset {
                repository: "github.com/q/r".to_owned(),
                external_ids: vec![serde_json::json!("2")],
            },
        ];
        let (specs, error) = build_import_changeset_specs(
            &missing,
            &TestFetcher {
                repos: BTreeMap::new(),
                error: None,
            },
        )
        .await;
        assert!(specs.is_empty());
        assert_eq!(
            error.as_deref(),
            Some(
                "2 errors occurred:\n\t* repository \"github.com/x/y\" not found\n\t* repository \"github.com/q/r\" not found"
            )
        );

        let (specs, error) = build_import_changeset_specs(
            &imports,
            &TestFetcher {
                repos: BTreeMap::new(),
                error: Some("lookup failed".to_owned()),
            },
        )
        .await;
        assert!(specs.is_empty());
        assert_eq!(error.as_deref(), Some("lookup failed"));
    }

    #[test]
    fn external_id_parsing() {
        assert_eq!(
            parse_changeset_spec_external_id(&serde_json::json!("abc")).unwrap(),
            "abc"
        );
        assert_eq!(
            parse_changeset_spec_external_id(&serde_json::json!(42)).unwrap(),
            "42"
        );
        assert_eq!(
            parse_changeset_spec_external_id(&serde_json::json!(true)).unwrap_err(),
            "cannot convert value of type bool into a valid external ID: expected string or int"
        );
    }

    #[test]
    fn repository_graphql_id_decoding() {
        use base64::Engine;
        let encoded = base64::engine::general_purpose::STANDARD.encode("Repository:17");
        assert_eq!(decode_repository_graphql_id(&encoded), Some(17));
        assert_eq!(decode_repository_graphql_id("not-base64"), None);
    }

    #[test]
    fn job_metadata_matches_baseline() {
        let job = BatchesWorkspaceResolver::new();
        assert_eq!(job.description(), "");
        assert_eq!(WORKER_NAME, "batch_changes_batch_spec_resolution_worker");
        assert_eq!(NUM_HANDLERS, 5);
        assert_eq!(INTERVAL, Duration::from_secs(1));
        assert_eq!(HEARTBEAT_INTERVAL, Duration::from_secs(15));
        assert_eq!(STALLED_MAX_AGE, Duration::from_secs(60));
        assert_eq!(MAX_NUM_RESETS, 60);
        assert_eq!(RETRY_AFTER, Duration::from_secs(5));
        assert_eq!(MAX_NUM_RETRIES, 0);
    }

    struct FakeService {
        stored: Result<StoredBatchSpec, String>,
        parsed: Result<BatchSpec, String>,
        env: Result<Vec<String>, String>,
        workspaces: Result<Vec<RepoWorkspace>, String>,
        import_ids: Result<BTreeMap<String, String>, String>,
        persist_error: Option<String>,
        persisted: Mutex<usize>,
    }

    impl FakeService {
        fn ok() -> Self {
            Self {
                stored: Ok(StoredBatchSpec {
                    id: 1,
                    raw_spec: "name: test".to_owned(),
                    ..StoredBatchSpec::default()
                }),
                parsed: Ok(BatchSpec::default()),
                env: Ok(Vec::new()),
                workspaces: Ok(Vec::new()),
                import_ids: Ok(BTreeMap::new()),
                persist_error: None,
                persisted: Mutex::new(0),
            }
        }
    }

    #[async_trait]
    impl ResolutionService for FakeService {
        async fn get_batch_spec(&self, _id: i64) -> Result<StoredBatchSpec, String> {
            self.stored.clone()
        }

        async fn parse_batch_spec(&self, _raw: &str) -> Result<BatchSpec, String> {
            self.parsed.clone()
        }

        async fn required_env_vars(
            &self,
            _spec: &StoredBatchSpec,
            _parsed: &BatchSpec,
        ) -> Result<Vec<String>, String> {
            self.env.clone()
        }

        async fn resolve_workspaces(
            &self,
            _spec: &BatchSpec,
        ) -> Result<Vec<RepoWorkspace>, String> {
            self.workspaces.clone()
        }

        async fn import_repo_ids(
            &self,
            _repo_names: &[String],
        ) -> Result<BTreeMap<String, String>, String> {
            self.import_ids.clone()
        }

        async fn persist(
            &self,
            _spec: &StoredBatchSpec,
            _output: &ResolutionOutput,
        ) -> Result<(), String> {
            *self.persisted.lock().expect("mutex") += 1;
            match &self.persist_error {
                Some(error) => Err(error.clone()),
                None => Ok(()),
            }
        }
    }

    async fn process(service: FakeService) -> Result<ResolutionOutput, ResolutionError> {
        let creator = BatchSpecWorkspaceCreator::new(Arc::new(service));
        creator.process(&job(1)).await
    }

    #[tokio::test]
    async fn handler_classification_and_success() {
        let output = process(FakeService {
            workspaces: Ok(vec![RepoWorkspace {
                repo_revision: repo_rev(1, "github.com/a/b", &[]),
                path: String::new(),
                ..RepoWorkspace::default()
            }]),
            ..FakeService::ok()
        })
        .await
        .expect("success");
        assert_eq!(output.workspaces.len(), 1);

        let error = process(FakeService {
            parsed: Err("bad yaml".to_owned()),
            ..FakeService::ok()
        })
        .await
        .expect_err("parse");
        assert_eq!(error.kind, ResolutionErrorKind::Executor);
        assert_eq!(error.message, "bad yaml");

        let error = process(FakeService {
            workspaces: Err("gitserver down".to_owned()),
            ..FakeService::ok()
        })
        .await
        .expect_err("resolve");
        assert_eq!(error.kind, ResolutionErrorKind::Unresolved);
        assert_eq!(error.message, "gitserver down");

        let parsed = BatchSpec {
            import_changesets: vec![ImportChangeset {
                repository: "github.com/a/b".to_owned(),
                external_ids: vec![serde_json::json!("1")],
            }],
            ..BatchSpec::default()
        };
        let error = process(FakeService {
            parsed: Ok(parsed),
            import_ids: Err("lookup failed".to_owned()),
            ..FakeService::ok()
        })
        .await
        .expect_err("import");
        assert_eq!(error.kind, ResolutionErrorKind::Import);
        assert_eq!(error.message, "lookup failed");

        let service = FakeService {
            persist_error: Some("insert failed".to_owned()),
            ..FakeService::ok()
        };
        let creator = BatchSpecWorkspaceCreator::new(Arc::new(service));
        let error = creator.process(&job(1)).await.expect_err("persist");
        assert_eq!(error.kind, ResolutionErrorKind::Executor);
        assert_eq!(error.message, "insert failed");
    }

    #[tokio::test]
    async fn handler_returns_the_error_message() {
        let handler = BatchSpecResolutionHandler::new(Arc::new(FakeService {
            workspaces: Err("gitserver down".to_owned()),
            ..FakeService::ok()
        }));
        let error = handler
            .handle(CancellationToken::new(), &job(1))
            .await
            .expect_err("handler error");
        assert_eq!(error, "gitserver down");
    }
}
