#![forbid(unsafe_code)]

//! M9.8dk differential harness (Rust side): the production adapters behind the
//! two handlers of `codeintel-autoindexing-dependency-scheduler`, mirrored
//! from
//! `internal/codeintel/autoindexing/internal/background/dependencies/adapters_harness_test.go`.
//!
//! The Go side wires the real stores and invokes `Handle`; this side wires the
//! Rust adapters added in M9.8dk and the same handlers. The index enqueuer is
//! a recording fake on both sides.
//!
//! `test_ws/p9-go/run_codeintel_dependency_scheduler_adapters_diff.sh` applies
//! `codeintel_dependency_scheduler_adapters_fixtures.sql` immediately before
//! each side and diffs the canonical `dsa ` JSON lines. Clock-derived
//! timestamps are printed as `set`/`null`; `queued_at` is `fixture` while it
//! still holds the seeded 2020 value and `now` after a requeue.

use std::collections::{HashMap, HashSet};
use std::sync::Mutex;

use serde::Serialize;
use sg_db::external_services::ExternalServiceStore;
use sg_db::gitserver_repos::GitserverReposStore;
use sg_db::repos::RepoStore;
use sg_db::OffsetDateTime;
use sg_service_worker::jobs::codeintel_autoindexing_dependency_indexing_scheduler as indexing;
use sg_service_worker::jobs::codeintel_autoindexing_dependency_scheduler::{
    DependenciesAdapter, GitserverRepoStoreAdapter, IndexingExternalServicesAdapter,
    InsertDependencyIndexingJobAdapter, ReposAdapter, SyncExternalServicesAdapter, UploadsAdapter,
};
use sg_service_worker::jobs::codeintel_autoindexing_dependency_sync_scheduler as sync;
use sg_service_worker::jobs::codeintel_autoindexing_enqueuer::VersionedPackageRepo;
use sg_service_worker::jobs::codeintel_dependency_jobs::DependencyIndexingJobStore;
use sqlx::postgres::PgPool;
use sqlx::Row;
use time::format_description::well_known::Rfc3339;

#[derive(Serialize)]
struct DsaLine {
    case: String,
    error: String,
    enqueued: Vec<String>,
    requeued: Vec<String>,
    deps: Vec<String>,
    index_jobs: Vec<String>,
    ext_services: Vec<String>,
    jobs: Vec<String>,
}

struct FakeEnqueuer {
    calls: Mutex<Vec<String>>,
    fail: Mutex<HashMap<String, String>>,
}

impl FakeEnqueuer {
    fn new() -> Self {
        Self {
            calls: Mutex::new(Vec::new()),
            fail: Mutex::new(HashMap::new()),
        }
    }

    fn fail(&self, name: &str, message: &str) {
        self.fail
            .lock()
            .expect("fail map")
            .insert(name.to_owned(), message.to_owned());
    }

    fn drain(&self) -> Vec<String> {
        let mut calls = std::mem::take(&mut *self.calls.lock().expect("calls"));
        calls.sort();
        calls
    }
}

#[async_trait::async_trait]
impl indexing::IndexEnqueuer for FakeEnqueuer {
    async fn queue_auto_index_jobs_for_package(
        &self,
        package: &VersionedPackageRepo,
    ) -> Result<(), String> {
        self.calls.lock().expect("calls").push(format!(
            "{}|{}|{}",
            package.scheme, package.name, package.version
        ));
        if let Some(message) = self.fail.lock().expect("fail map").get(&package.name) {
            return Err(message.clone());
        }
        Ok(())
    }
}

fn sync_error_text(result: Result<(), sync::SyncSchedulerError>) -> String {
    match result {
        Ok(()) => "-".to_owned(),
        Err(sync::SyncSchedulerError::Message(message)) => message,
        Err(sync::SyncSchedulerError::Multiple(mut errors)) => {
            errors.sort();
            errors.join(" | ")
        }
    }
}

fn indexing_error_text(result: Result<(), indexing::HandleError>) -> String {
    match result {
        Ok(()) => "-".to_owned(),
        Err(indexing::HandleError::Message(message)) => message,
        Err(indexing::HandleError::Multiple(mut errors)) => {
            errors.sort();
            errors.join(" | ")
        }
    }
}

fn marker(value: Option<OffsetDateTime>) -> &'static str {
    if value.is_some() {
        "set"
    } else {
        "null"
    }
}

fn queued_at(value: Option<OffsetDateTime>) -> &'static str {
    match value {
        Some(value) if value.year() < 2021 => "fixture",
        _ => "now",
    }
}

async fn deps(pool: &PgPool) -> Vec<String> {
    let rows = sqlx::query(
        r#"
SELECT lr.scheme, lr.name, lr.blocked, lr.last_checked_at, pv.version, pv.blocked AS version_blocked
FROM lsif_dependency_repos lr
LEFT JOIN package_repo_versions pv ON pv.package_id = lr.id
ORDER BY lr.id, pv.version
"#,
    )
    .fetch_all(pool)
    .await
    .expect("query deps");

    let mut lines = Vec::new();
    for row in rows {
        let scheme: String = row.get("scheme");
        let name: String = row.get("name");
        let blocked: bool = row.get("blocked");
        let checked: Option<OffsetDateTime> = row.get("last_checked_at");
        let version: Option<String> = row.get("version");
        let version_blocked: Option<bool> = row.get("version_blocked");
        if let Some(version) = version {
            lines.push(format!(
                "ver|{}|{}|{}|{}",
                scheme,
                name,
                version,
                version_blocked.unwrap_or(false)
            ));
        }
        lines.push(format!(
            "ref|{}|{}|blocked={}|checked={}",
            scheme,
            name,
            blocked,
            marker(checked)
        ));
    }
    lines.sort();
    lines
}

async fn index_jobs(pool: &PgPool) -> Vec<String> {
    let rows = sqlx::query(
        r#"
SELECT upload_id, external_service_kind, external_service_sync, state, process_after
FROM lsif_dependency_indexing_jobs
WHERE id > 100
"#,
    )
    .fetch_all(pool)
    .await
    .expect("query index jobs");

    let mut lines = Vec::new();
    for row in rows {
        let upload_id: i32 = row.get("upload_id");
        let kind: String = row.get("external_service_kind");
        let sync_time: Option<OffsetDateTime> = row.get("external_service_sync");
        let state: String = row.get("state");
        let process_after: Option<OffsetDateTime> = row.get("process_after");
        lines.push(format!(
            "job|upload={}|kind={}|sync={}|state={}|after={}",
            upload_id,
            kind,
            marker(sync_time),
            state,
            marker(process_after)
        ));
    }
    lines.sort();
    lines
}

async fn ext_services(pool: &PgPool) -> Vec<String> {
    let rows = sqlx::query(
        "SELECT id, kind, last_sync_at, next_sync_at FROM external_services ORDER BY id",
    )
    .fetch_all(pool)
    .await
    .expect("query external services");

    let mut lines = Vec::new();
    for row in rows {
        let id: i64 = row.get("id");
        let kind: String = row.get("kind");
        let last_sync: Option<OffsetDateTime> = row.get("last_sync_at");
        let next_sync: Option<OffsetDateTime> = row.get("next_sync_at");
        lines.push(format!(
            "esvc|id={}|kind={}|last={}|next={}",
            id,
            kind,
            marker(last_sync),
            marker(next_sync)
        ));
    }
    lines
}

async fn seeded_jobs(pool: &PgPool) -> Vec<String> {
    let rows = sqlx::query(
        r#"
SELECT id, state, process_after, queued_at, started_at, cancel
FROM lsif_dependency_indexing_jobs
WHERE id <= 100
ORDER BY id
"#,
    )
    .fetch_all(pool)
    .await
    .expect("query seeded jobs");

    let mut lines = Vec::new();
    for row in rows {
        let id: i32 = row.get("id");
        let state: String = row.get("state");
        let process_after: Option<OffsetDateTime> = row.get("process_after");
        let queued: Option<OffsetDateTime> = row.get("queued_at");
        let started: Option<OffsetDateTime> = row.get("started_at");
        let cancel: bool = row.get("cancel");
        lines.push(format!(
            "id={}|state={}|after={}|queued={}|started={}|cancel={}",
            id,
            state,
            marker(process_after),
            queued_at(queued),
            marker(started),
            cancel
        ));
    }
    lines
}

async fn requeued_ids(pool: &PgPool) -> HashSet<i32> {
    sqlx::query("SELECT id FROM lsif_dependency_indexing_jobs WHERE process_after IS NOT NULL")
        .fetch_all(pool)
        .await
        .expect("query requeued")
        .into_iter()
        .map(|row| row.get("id"))
        .collect()
}

async fn emit(
    pool: &PgPool,
    case: &str,
    error: String,
    enqueuer: &FakeEnqueuer,
    before: &HashSet<i32>,
) {
    let after = requeued_ids(pool).await;
    let mut requeued: Vec<String> = after.difference(before).map(|id| id.to_string()).collect();
    requeued.sort();

    let line = DsaLine {
        case: case.to_owned(),
        error,
        enqueued: enqueuer.drain(),
        requeued,
        deps: deps(pool).await,
        index_jobs: index_jobs(pool).await,
        ext_services: ext_services(pool).await,
        jobs: seeded_jobs(pool).await,
    };
    println!(
        "dsa {}",
        serde_json::to_string(&line).expect("json snapshot")
    );
}

fn parse_time(raw: &str) -> OffsetDateTime {
    OffsetDateTime::parse(raw, &Rfc3339).expect("fixture timestamp")
}

fn sync_handler(pool: &PgPool, enabled: bool) -> sync::DependencySyncSchedulerHandler {
    sync::DependencySyncSchedulerHandler {
        uploads: std::sync::Arc::new(UploadsAdapter::new(pool.clone())),
        dependencies: std::sync::Arc::new(DependenciesAdapter::new(pool.clone())),
        store: std::sync::Arc::new(InsertDependencyIndexingJobAdapter::new(pool.clone())),
        external_services: std::sync::Arc::new(SyncExternalServicesAdapter::new(
            ExternalServiceStore::new(pool.clone()),
        )),
        settings: sync::HandlerSettings {
            auto_indexing_enabled: enabled,
        },
    }
}

fn indexing_handler(
    pool: &PgPool,
    enqueuer: std::sync::Arc<FakeEnqueuer>,
    auto_indexing_enabled: bool,
    disabled: bool,
) -> indexing::DependencyIndexingSchedulerHandler {
    indexing::DependencyIndexingSchedulerHandler {
        uploads: std::sync::Arc::new(UploadsAdapter::new(pool.clone())),
        repos: std::sync::Arc::new(ReposAdapter::new(RepoStore::new(pool.clone()))),
        external_services: std::sync::Arc::new(IndexingExternalServicesAdapter::new(
            ExternalServiceStore::new(pool.clone()),
        )),
        gitserver_repos: std::sync::Arc::new(GitserverRepoStoreAdapter::new(
            GitserverReposStore::new(pool.clone()),
        )),
        index_enqueuer: enqueuer,
        worker_store: std::sync::Arc::new(DependencyIndexingJobStore::new(pool.clone())),
        settings: indexing::HandlerSettings {
            auto_indexing_enabled,
            disabled,
        },
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn dependency_scheduler_adapters_match_go_harness() {
    if std::env::var("SG_DEPENDENCY_SCHEDULER_ADAPTERS_HARNESS_ON")
        .ok()
        .as_deref()
        != Some("1")
    {
        eprintln!("SG_DEPENDENCY_SCHEDULER_ADAPTERS_HARNESS_ON is not set");
        return;
    }
    let dsn = std::env::var("SG_DEPENDENCY_SCHEDULER_ADAPTERS_TEST_DSN")
        .expect("SG_DEPENDENCY_SCHEDULER_ADAPTERS_TEST_DSN is not set");
    let pool = PgPool::connect(&dsn).await.expect("connect");

    let enqueuer = std::sync::Arc::new(FakeEnqueuer::new());

    // --- sync handler -----------------------------------------------------
    let handler = sync_handler(&pool, true);
    let cases: &[(&str, i32)] = &[
        ("sync_basic", 9001),
        ("sync_no_index", 9002),
        ("sync_scip", 9003),
        ("sync_deleted_upload", 9004),
        ("sync_missing_upload", 9999),
    ];
    for (case, upload_id) in cases {
        let before = requeued_ids(&pool).await;
        let job = sync::DependencySyncingJob {
            id: i64::from(*upload_id),
            upload_id: *upload_id,
        };
        let error = sync_error_text(handler.handle(&job).await);
        emit(&pool, case, error, &enqueuer, &before).await;
    }

    let disabled = sync_handler(&pool, false);
    let before = requeued_ids(&pool).await;
    let job = sync::DependencySyncingJob {
        id: 9001,
        upload_id: 9001,
    };
    let error = sync_error_text(disabled.handle(&job).await);
    emit(&pool, "sync_disabled", error, &enqueuer, &before).await;

    // --- indexing handler -------------------------------------------------
    let handler = indexing_handler(&pool, enqueuer.clone(), true, false);
    let indexing_cases: &[(&str, i64, i32, &str)] = &[
        ("index_visibility", 1, 9011, ""),
        ("index_cloning_requeue", 2, 9012, ""),
        ("index_outdated_service_requeue", 3, 9011, "NPMPACKAGES"),
        ("index_fresh_service", 4, 9011, "JVMPACKAGES"),
        ("index_unknown_service", 5, 9011, "UNKNOWNKIND"),
        ("index_no_repos", 6, 9013, ""),
    ];
    for (case, id, upload_id, kind) in indexing_cases {
        let before = requeued_ids(&pool).await;
        let job = indexing::DependencyIndexingJob {
            id: *id,
            upload_id: *upload_id,
            external_service_kind: (*kind).to_owned(),
            external_service_sync: Some(parse_time("2020-12-01T00:00:00Z")),
        };
        let error = indexing_error_text(handler.handle(&job).await);
        emit(&pool, case, error, &enqueuer, &before).await;
    }

    let disabled_setting = indexing_handler(&pool, enqueuer.clone(), false, false);
    let before = requeued_ids(&pool).await;
    let job = indexing::DependencyIndexingJob {
        id: 7,
        upload_id: 9014,
        external_service_kind: String::new(),
        external_service_sync: Some(parse_time("2020-12-01T00:00:00Z")),
    };
    let error = indexing_error_text(disabled_setting.handle(&job).await);
    emit(&pool, "index_disabled_setting", error, &enqueuer, &before).await;

    let disabled_env = indexing_handler(&pool, enqueuer.clone(), true, true);
    let before = requeued_ids(&pool).await;
    let job = indexing::DependencyIndexingJob {
        id: 10,
        upload_id: 9014,
        external_service_kind: String::new(),
        external_service_sync: Some(parse_time("2020-12-01T00:00:00Z")),
    };
    let error = indexing_error_text(disabled_env.handle(&job).await);
    emit(&pool, "index_disabled_env", error, &enqueuer, &before).await;

    enqueuer.fail("github.com/acme/fail-1", "boom");
    let before = requeued_ids(&pool).await;
    let job = indexing::DependencyIndexingJob {
        id: 8,
        upload_id: 9015,
        external_service_kind: String::new(),
        external_service_sync: Some(parse_time("2020-12-01T00:00:00Z")),
    };
    let error = indexing_error_text(handler.handle(&job).await);
    emit(&pool, "index_enqueue_error", error, &enqueuer, &before).await;

    enqueuer.fail("github.com/acme/fail-2", "bang");
    let before = requeued_ids(&pool).await;
    let job = indexing::DependencyIndexingJob {
        id: 9,
        upload_id: 9016,
        external_service_kind: String::new(),
        external_service_sync: Some(parse_time("2020-12-01T00:00:00Z")),
    };
    let error = indexing_error_text(handler.handle(&job).await);
    emit(&pool, "index_enqueue_errors", error, &enqueuer, &before).await;
}
