#![forbid(unsafe_code)]

//! M9.9i differential harness (Rust side): the `batches-workspace-resolver`
//! job's pure resolution core plus the `batch_spec_resolution_jobs` worker
//! store state machine.
//!
//! Mirrors the untracked Go oracles
//! `internal/batches/service/zz_m99i_workspace_resolver_oracle_test.go` and
//! `cmd/worker/internal/batches/zz_m99i_workspace_resolver_db_oracle_test.go`.
//! Both sides read
//! `test_ws/p9-go/data/batches_workspace_resolver/corpus.json` and emit
//! canonical JSON; `test_ws/p9-go/run_batches_workspace_resolver_diff.sh`
//! normalizes and compares them. The pure sections are always written to
//! `M9I_PURE_OUT`; the store sections run only when `SG_QTEST_DSN` is set and
//! are written to `M9I_DB_OUT`.

use std::collections::BTreeMap;
use std::path::PathBuf;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use sg_db::OffsetDateTime;
use sg_service_worker::jobs::batches_workspace_resolver::{
    build_import_changeset_specs, determine_default_pattern_type, find_workspaces,
    set_default_query_count, BatchSpec, DirectoryFinder, ImportChangeset, ImportChangesetSpec,
    RepoFetcher, RepoRevKey, RepoRevision, RepoWorkspace,
};
use sqlx::{PgPool, Row};
use time::format_description::well_known::Rfc3339;

// ---------------------------------------------------------------------------
// Corpus.
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
struct Corpus {
    #[serde(rename = "queryCases", default)]
    query_cases: Vec<QueryCase>,
    #[serde(rename = "workspaceCases", default)]
    workspace_cases: Vec<WorkspaceCase>,
    #[serde(rename = "importCases", default)]
    import_cases: Vec<ImportCase>,
    #[serde(rename = "dequeueCases", default)]
    dequeue_cases: Vec<DequeueCase>,
    #[serde(rename = "markCases", default)]
    mark_cases: Vec<MarkCase>,
    #[serde(rename = "resetCases", default)]
    reset_cases: Vec<ResetCase>,
}

#[derive(Debug, Deserialize)]
struct QueryCase {
    name: String,
    query: String,
    version: i64,
}

#[derive(Debug, Deserialize)]
struct WorkspaceCase {
    name: String,
    spec: BatchSpec,
    #[serde(rename = "repoRevs", default)]
    repo_revs: Vec<RepoRevision>,
    finder: Finder,
}

#[derive(Debug, Default, Deserialize)]
struct Finder {
    #[serde(default)]
    results: BTreeMap<String, Vec<String>>,
    #[serde(default)]
    error: String,
}

#[derive(Debug, Deserialize)]
struct ImportCase {
    name: String,
    #[serde(default)]
    imports: Vec<ImportChangeset>,
    #[serde(default)]
    repos: BTreeMap<String, String>,
    #[serde(default, rename = "fetchError")]
    fetch_error: String,
}

#[derive(Debug, Deserialize)]
struct DequeueCase {
    name: String,
    worker: String,
    #[serde(default)]
    rows: Vec<SeedRow>,
}

#[derive(Debug, Deserialize)]
struct SeedRow {
    id: i64,
    state: String,
    #[serde(default, rename = "processAfter")]
    process_after: Option<String>,
    #[serde(default, rename = "updatedAt")]
    updated_at: Option<String>,
    #[serde(default, rename = "numResets")]
    num_resets: i32,
    #[serde(default, rename = "numFailures")]
    num_failures: i32,
    #[serde(default, rename = "lastHeartbeatAt")]
    last_heartbeat: Option<String>,
}

#[derive(Debug, Deserialize)]
struct MarkCase {
    name: String,
    operation: String,
    #[serde(default, rename = "numFailures")]
    num_failures: i32,
    #[serde(default, rename = "failureMessage")]
    failure_message: String,
}

#[derive(Debug, Deserialize)]
struct ResetCase {
    name: String,
    #[serde(default)]
    rows: Vec<SeedRow>,
}

// ---------------------------------------------------------------------------
// Pure output.
// ---------------------------------------------------------------------------

#[derive(Debug, Serialize)]
struct QueryOut {
    name: String,
    query: String,
    #[serde(rename = "patternType")]
    pattern_type: &'static str,
}

#[derive(Debug, Serialize)]
struct WorkspaceRow {
    #[serde(rename = "repoId")]
    repo_id: i32,
    name: String,
    branch: String,
    commit: String,
    path: String,
    #[serde(rename = "onlyFetchWorkspace")]
    only_fetch_workspace: bool,
    #[serde(rename = "fileMatches")]
    file_matches: Vec<String>,
    ignored: bool,
    unsupported: bool,
}

#[derive(Debug, Serialize)]
struct WorkspaceOut {
    name: String,
    stage: String,
    workspaces: Vec<WorkspaceRow>,
    error: Option<String>,
}

#[derive(Debug, Serialize)]
struct ImportOut {
    name: String,
    stage: String,
    specs: Vec<ImportChangesetSpec>,
    error: Option<String>,
}

#[derive(Debug, Serialize)]
struct PureOutput {
    #[serde(rename = "queryCases")]
    query_cases: Vec<QueryOut>,
    #[serde(rename = "workspaceCases")]
    workspace_cases: Vec<WorkspaceOut>,
    #[serde(rename = "importCases")]
    import_cases: Vec<ImportOut>,
}

#[derive(Debug)]
struct FakeFinder {
    results: BTreeMap<RepoRevKey, Vec<String>>,
    error: String,
}

#[async_trait]
impl DirectoryFinder for FakeFinder {
    async fn find_directories_in_repos(
        &self,
        _file_name: &str,
        _batch_spec_version: i64,
        _repos: &[RepoRevision],
    ) -> Result<BTreeMap<RepoRevKey, Vec<String>>, String> {
        if !self.error.is_empty() {
            return Err(self.error.clone());
        }
        Ok(self.results.clone())
    }
}

#[derive(Debug)]
struct FakeFetcher {
    repos: BTreeMap<String, String>,
    error: String,
}

#[async_trait]
impl RepoFetcher for FakeFetcher {
    async fn fetch(&self, _repo_names: &[String]) -> Result<BTreeMap<String, String>, String> {
        if !self.error.is_empty() {
            return Err(self.error.clone());
        }
        Ok(self.repos.clone())
    }
}

fn parse_repo_rev_key(raw: &str) -> RepoRevKey {
    let mut parts = raw.splitn(3, '|');
    let repo_id = parts
        .next()
        .and_then(|value| value.parse::<i32>().ok())
        .unwrap_or_default();
    RepoRevKey {
        repo_id,
        branch: parts.next().unwrap_or_default().to_owned(),
        commit: parts.next().unwrap_or_default().to_owned(),
    }
}

fn workspace_row(workspace: &RepoWorkspace) -> WorkspaceRow {
    WorkspaceRow {
        repo_id: workspace.repo_revision.repo_id,
        name: workspace.repo_revision.repo_name.clone(),
        branch: workspace.repo_revision.branch.clone(),
        commit: workspace.repo_revision.commit.clone(),
        path: workspace.path.clone(),
        only_fetch_workspace: workspace.only_fetch_workspace,
        file_matches: workspace.repo_revision.file_matches.clone(),
        ignored: workspace.ignored,
        unsupported: workspace.unsupported,
    }
}

async fn pure_output(corpus: &Corpus) -> PureOutput {
    let mut query_cases = Vec::new();
    for case in &corpus.query_cases {
        query_cases.push(QueryOut {
            name: case.name.clone(),
            query: set_default_query_count(&case.query),
            pattern_type: determine_default_pattern_type(case.version),
        });
    }

    let mut workspace_cases = Vec::new();
    for case in &corpus.workspace_cases {
        let mut results = BTreeMap::new();
        for (key, dirs) in &case.finder.results {
            results.insert(parse_repo_rev_key(key), dirs.clone());
        }
        let finder = FakeFinder {
            results,
            error: case.finder.error.clone(),
        };
        match find_workspaces(&case.spec, &finder, &case.repo_revs).await {
            Ok(workspaces) => workspace_cases.push(WorkspaceOut {
                name: case.name.clone(),
                stage: "workspace".to_owned(),
                workspaces: workspaces.iter().map(workspace_row).collect(),
                error: None,
            }),
            Err(error) => workspace_cases.push(WorkspaceOut {
                name: case.name.clone(),
                stage: if case.finder.error.is_empty() {
                    "unresolved".to_owned()
                } else {
                    "executor".to_owned()
                },
                workspaces: Vec::new(),
                error: Some(error),
            }),
        }
    }

    let mut import_cases = Vec::new();
    for case in &corpus.import_cases {
        let fetcher = FakeFetcher {
            repos: case.repos.clone(),
            error: case.fetch_error.clone(),
        };
        let (specs, error) = build_import_changeset_specs(&case.imports, &fetcher).await;
        import_cases.push(ImportOut {
            name: case.name.clone(),
            stage: "import".to_owned(),
            specs,
            error,
        });
    }

    PureOutput {
        query_cases,
        workspace_cases,
        import_cases,
    }
}

// ---------------------------------------------------------------------------
// Store output.
// ---------------------------------------------------------------------------

#[derive(Debug, Serialize)]
struct DequeueOut {
    name: String,
    dequeued: Option<i64>,
    state: String,
    #[serde(rename = "workerHostname")]
    worker_hostname: String,
    #[serde(rename = "numResets")]
    num_resets: i32,
    #[serde(rename = "numFailures")]
    num_failures: i32,
    #[serde(rename = "failureMessage")]
    failure_message: Option<String>,
}

#[derive(Debug, Serialize)]
struct MarkOut {
    name: String,
    updated: bool,
    state: String,
    #[serde(rename = "failureMessage")]
    failure_message: Option<String>,
    #[serde(rename = "numFailures")]
    num_failures: i32,
}

#[derive(Debug, Serialize)]
struct ResetRowOut {
    id: i64,
    state: String,
    #[serde(rename = "numResets")]
    num_resets: i32,
    #[serde(rename = "failureMessage")]
    failure_message: Option<String>,
}

#[derive(Debug, Serialize)]
struct ResetOut {
    name: String,
    rows: Vec<ResetRowOut>,
}

#[derive(Debug, Serialize)]
struct DbOutput {
    #[serde(rename = "dequeueCases")]
    dequeue_cases: Vec<DequeueOut>,
    #[serde(rename = "markCases")]
    mark_cases: Vec<MarkOut>,
    #[serde(rename = "resetCases")]
    reset_cases: Vec<ResetOut>,
}

const BATCH_SPEC_ID: i64 = 9901;
const USER_ID: i32 = 9999;

fn parse_ts(raw: &str) -> OffsetDateTime {
    OffsetDateTime::parse(raw, &Rfc3339).unwrap_or_else(|err| panic!("parse {raw:?}: {err}"))
}

async fn clean_and_seed_base(pool: &PgPool) {
    sqlx::query("DELETE FROM batch_spec_resolution_jobs WHERE id BETWEEN 9900 AND 9999")
        .execute(pool)
        .await
        .expect("clean jobs");
    sqlx::query(
        "INSERT INTO users (id, username) VALUES ($1, 'm9i-diff-user') ON CONFLICT (id) DO NOTHING",
    )
    .bind(USER_ID)
    .execute(pool)
    .await
    .expect("seed user");
    sqlx::query(
        "INSERT INTO batch_specs (id, rand_id, raw_spec, namespace_user_id) VALUES ($1, 'm9i-as', '', $2) ON CONFLICT (id) DO NOTHING",
    )
    .bind(BATCH_SPEC_ID)
    .bind(USER_ID)
    .execute(pool)
    .await
    .expect("seed batch spec");
}

async fn insert_row(pool: &PgPool, row: &SeedRow) {
    let process_after = row.process_after.as_deref().map(parse_ts);
    let last_heartbeat = row.last_heartbeat.as_deref().map(parse_ts);
    let updated = row
        .updated_at
        .as_deref()
        .map(parse_ts)
        .unwrap_or_else(OffsetDateTime::now_utc);
    // One resolution job per batch spec (unique constraint), so each seeded
    // row gets its own batch spec id.
    sqlx::query(
        "INSERT INTO batch_specs (id, rand_id, raw_spec, namespace_user_id) VALUES ($1, $2, '', $3) ON CONFLICT (id) DO NOTHING",
    )
    .bind(row.id)
    .bind(format!("m9i-as-{}", row.id))
    .bind(USER_ID)
    .execute(pool)
    .await
    .expect("seed row batch spec");
    sqlx::query(
        r#"
INSERT INTO batch_spec_resolution_jobs
    (id, batch_spec_id, initiator_id, state, process_after, updated_at, created_at,
     queued_at, num_resets, num_failures, last_heartbeat_at, worker_hostname)
VALUES ($1, $2, $3, $4, $5, $6, $6, $6, $7, $8, $9, '')
"#,
    )
    .bind(row.id)
    .bind(row.id as i32)
    .bind(USER_ID)
    .bind(&row.state)
    .bind(process_after)
    .bind(updated)
    .bind(row.num_resets)
    .bind(row.num_failures)
    .bind(last_heartbeat)
    .execute(pool)
    .await
    .expect("insert row");
}

struct StoredFields {
    state: String,
    failure_message: Option<String>,
    num_failures: i32,
    worker_hostname: String,
    num_resets: i32,
}

async fn read_row(pool: &PgPool, id: i64) -> StoredFields {
    let row = sqlx::query(
        "SELECT state, failure_message, num_failures, worker_hostname, num_resets FROM batch_spec_resolution_jobs WHERE id = $1",
    )
    .bind(id)
    .fetch_one(pool)
    .await
    .expect("read row");
    StoredFields {
        state: row.get("state"),
        failure_message: row.get("failure_message"),
        num_failures: row.get("num_failures"),
        worker_hostname: row.get("worker_hostname"),
        num_resets: row.get("num_resets"),
    }
}

async fn db_output(corpus: &Corpus, pool: &PgPool) -> DbOutput {
    let now = OffsetDateTime::now_utc();

    let mut dequeue_cases = Vec::new();
    for case in &corpus.dequeue_cases {
        clean_and_seed_base(pool).await;
        for row in &case.rows {
            insert_row(pool, row).await;
        }
        let record = sg_db::batches::dequeue_batch_spec_resolution_job(pool, now, &case.worker)
            .await
            .expect("dequeue");
        let mut out = DequeueOut {
            name: case.name.clone(),
            dequeued: None,
            state: String::new(),
            worker_hostname: String::new(),
            num_resets: 0,
            num_failures: 0,
            failure_message: None,
        };
        if let Some(record) = record {
            let fields = read_row(pool, record.id).await;
            out.dequeued = Some(record.id);
            out.state = fields.state;
            out.worker_hostname = fields.worker_hostname;
            out.num_resets = fields.num_resets;
            out.num_failures = fields.num_failures;
            out.failure_message = fields.failure_message;
        }
        dequeue_cases.push(out);
    }

    let mut mark_cases = Vec::new();
    for case in &corpus.mark_cases {
        clean_and_seed_base(pool).await;
        insert_row(
            pool,
            &SeedRow {
                id: BATCH_SPEC_ID,
                state: "processing".to_owned(),
                process_after: None,
                updated_at: Some("2024-01-01T00:00:00Z".to_owned()),
                num_resets: 0,
                num_failures: case.num_failures,
                last_heartbeat: None,
            },
        )
        .await;
        let updated = match case.operation.as_str() {
            "complete" => {
                sg_db::batches::mark_batch_spec_resolution_job_complete(pool, BATCH_SPEC_ID)
                    .await
                    .expect("mark complete")
            }
            "errored" => sg_db::batches::mark_batch_spec_resolution_job_errored(
                pool,
                BATCH_SPEC_ID,
                &case.failure_message,
            )
            .await
            .expect("mark errored"),
            "failed" => sg_db::batches::mark_batch_spec_resolution_job_failed(
                pool,
                BATCH_SPEC_ID,
                &case.failure_message,
            )
            .await
            .expect("mark failed"),
            other => panic!("unknown operation {other:?}"),
        };
        let fields = read_row(pool, BATCH_SPEC_ID).await;
        mark_cases.push(MarkOut {
            name: case.name.clone(),
            updated,
            state: fields.state,
            failure_message: fields.failure_message,
            num_failures: fields.num_failures,
        });
    }

    let mut reset_cases = Vec::new();
    for case in &corpus.reset_cases {
        clean_and_seed_base(pool).await;
        for row in &case.rows {
            insert_row(pool, row).await;
        }
        sg_db::batches_janitor::reset_stalled(
            pool,
            sg_db::batches_janitor::BATCH_SPEC_RESOLUTION_WORKER,
            now,
        )
        .await
        .expect("reset stalled");
        let mut rows = Vec::new();
        for row in &case.rows {
            let fields = read_row(pool, row.id).await;
            rows.push(ResetRowOut {
                id: row.id,
                state: fields.state,
                num_resets: fields.num_resets,
                failure_message: fields.failure_message,
            });
        }
        reset_cases.push(ResetOut {
            name: case.name.clone(),
            rows,
        });
    }

    DbOutput {
        dequeue_cases,
        mark_cases,
        reset_cases,
    }
}

fn write_json<T: Serialize>(path: &str, value: &T) {
    let encoded = serde_json::to_vec_pretty(value).expect("encode json");
    std::fs::write(PathBuf::from(path), encoded).expect("write json");
}

#[tokio::test(flavor = "multi_thread")]
async fn batches_workspace_resolver_matches_go() {
    let corpus_path = std::env::var("M9I_CORPUS").expect("M9I_CORPUS is required");
    let raw = std::fs::read(&corpus_path).expect("read corpus");
    let corpus: Corpus = serde_json::from_slice(&raw).expect("parse corpus");

    let pure = pure_output(&corpus).await;
    write_json(
        &std::env::var("M9I_PURE_OUT").expect("M9I_PURE_OUT is required"),
        &pure,
    );

    let Ok(dsn) = std::env::var("SG_QTEST_DSN") else {
        eprintln!("SG_QTEST_DSN unset; skipping store sections");
        return;
    };
    let pool = PgPool::connect(&dsn).await.expect("connect");
    let db = db_output(&corpus, &pool).await;
    write_json(
        &std::env::var("M9I_DB_OUT").expect("M9I_DB_OUT is required"),
        &db,
    );
}
