#![forbid(unsafe_code)]

//! M9.8q differential harness (Rust side): the `codeintel-upload-janitor` job.
//!
//! Mirrors `internal/codeintel/uploads/janitorharness/harness_test.go`.
//! `test_ws/p9-go/run_codeintel_upload_janitor_diff.sh` resets a scratch
//! frontend and code-intel database, applies the shared fixtures, and diffs
//! the canonical lines below against the Go harness. Both sides drive the
//! same store calls in the same order and print the resulting state.

use std::sync::Arc;

use async_trait::async_trait;
use sg_config::{LiveConfig, ServiceConnections, Unified};
use sg_core::EnvReader;
use sg_db::codeintel_codegraph;
use sg_db::codeintel_uploads;
use sg_db::codeintel_uploads::CommitDeleter;
use sg_service_worker::jobs::codeintel_upload_janitor::UploadJanitor;
use sg_service_worker::jobs::{Job, JobConfig, JobDeps};
use sqlx::postgres::{PgPool, PgPoolOptions};
use sqlx::Row;
use time::OffsetDateTime;
use tokio_util::sync::CancellationToken;

const COMMIT_GONE: &str = "2222222222222222222222222222222222222222";
const COMMIT_INDEX_GONE: &str = "4444444444444444444444444444444444444444";

struct StubDeleter;

#[async_trait]
impl CommitDeleter for StubDeleter {
    async fn should_delete(
        &self,
        _repository_id: i64,
        _repository_name: &str,
        commit: &str,
    ) -> Result<bool, String> {
        Ok(commit == COMMIT_GONE || commit == COMMIT_INDEX_GONE)
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn upload_janitor_matches_go_harness() {
    if std::env::var("SG_JANITOR_DIFF").ok().as_deref() != Some("1") {
        return;
    }

    let frontend_dsn = std::env::var("SG_JANITOR_DSN").expect("SG_JANITOR_DSN");
    let codeintel_dsn =
        std::env::var("SG_JANITOR_CODEINTEL_DSN").expect("SG_JANITOR_CODEINTEL_DSN");

    let job = UploadJanitor::new();
    job.config().load(&mut EnvReader::new());
    let config = job.config();

    println!("jmeta description={:?}", job.description());
    println!(
        "jconfig interval={} abandoned_interval={} minimum_time_since_last_check={} resolver_batch={} audit_max_age={} doc_batch={} doc_max_age={} max_commit_lag={} upload_timeout={} reconciler_batch={} failed_batch={} failed_max_age={}",
        config.interval().as_secs(),
        config.abandoned_schema_versions_interval().as_secs(),
        config.minimum_time_since_last_check().as_secs(),
        config.commit_resolver_batch_size(),
        config.audit_log_max_age().as_secs(),
        config.unreferenced_document_batch_size(),
        config.unreferenced_document_max_age().as_secs(),
        config.commit_resolver_maximum_commit_lag().as_secs(),
        config.upload_timeout().as_secs(),
        config.reconciler_batch_size(),
        config.failed_index_batch_size(),
        config.failed_index_max_age().as_secs(),
    );

    let deps = JobDeps {
        frontend: PgPoolOptions::new()
            .connect_lazy("postgres://unused@localhost/unused")
            .expect("lazy pool"),
        live_config: Arc::new(LiveConfig::new(Unified {
            service_connections: ServiceConnections {
                code_intel_postgres_dsn: codeintel_dsn.clone(),
                git_servers: vec!["127.0.0.1:3178".to_owned()],
                ..Default::default()
            },
            ..Default::default()
        })),
        keyring: Arc::new(sg_keyring::Keyring::default()),
    };
    let routines = job
        .routines(&deps, CancellationToken::new())
        .await
        .expect("routines");
    assert_eq!(routines.len(), 14);
    let mut metadata: Vec<String> = routines
        .iter()
        .map(|routine| format!("{}|{}", routine.name(), routine.description()))
        .collect();
    metadata.sort();
    for line in metadata {
        println!("jroutine {line}");
    }

    let pool = sg_db::connect(&frontend_dsn, 10)
        .await
        .expect("frontend pool");
    let codeintel = sg_db::connect(&codeintel_dsn, 10)
        .await
        .expect("codeintel pool");
    let now = OffsetDateTime::now_utc();
    let deleter = StubDeleter;

    // 1. DeletedRepositoryJanitor.
    let (total, altered) = codeintel_uploads::delete_uploads_without_repository(&pool, now)
        .await
        .expect("DeleteUploadsWithoutRepository");
    println!("delrepo total={total} altered={altered}");
    println!("delrepo uploads={}", upload_states(&pool).await);

    // 2. UnknownCommitJanitor (uploads).
    let (scanned, deleted) = codeintel_uploads::process_sourced_commits(
        &pool,
        config.minimum_time_since_last_check(),
        config.commit_resolver_maximum_commit_lag(),
        config.commit_resolver_batch_size(),
        &deleter,
        now,
    )
    .await
    .expect("ProcessSourcedCommits");
    println!("sourced scanned={scanned} deleted={deleted}");
    println!("sourced uploads={}", upload_states(&pool).await);

    // 3. AbandonedUploadJanitor.
    let (_, altered) = codeintel_uploads::delete_uploads_stuck_uploading(
        &pool,
        now - time::Duration::new(config.upload_timeout().as_secs() as i64, 0),
    )
    .await
    .expect("DeleteUploadsStuckUploading");
    println!("abandoned altered={altered}");
    println!("abandoned uploads={}", upload_states(&pool).await);

    // 4. ExpiredUploadJanitor.
    let (scanned, altered) = codeintel_uploads::soft_delete_expired_uploads(&pool, 1000)
        .await
        .expect("SoftDeleteExpiredUploads");
    println!("expired scanned={scanned} altered={altered}");
    println!("expired uploads={}", upload_states(&pool).await);

    // 5. ExpiredUploadTraversalJanitor.
    let (scanned, altered) =
        codeintel_uploads::soft_delete_expired_uploads_via_traversal(&pool, 100)
            .await
            .expect("SoftDeleteExpiredUploadsViaTraversal");
    println!("traversal scanned={scanned} altered={altered}");
    println!("traversal uploads={}", upload_states(&pool).await);

    // 6. HardDeleter.
    let mut count = 0i64;
    loop {
        let (ids, total_count) = codeintel_uploads::get_deleted_upload_ids(&pool, 100)
            .await
            .expect("GetUploads");
        codeintel_codegraph::delete_lsif_data_by_upload_ids(&codeintel, &ids)
            .await
            .expect("DeleteLsifDataByUploadIds");
        codeintel_uploads::hard_delete_uploads_by_ids(&pool, &ids)
            .await
            .expect("HardDeleteUploadsByIDs");
        count += ids.len() as i64;
        if count >= total_count {
            break;
        }
    }
    println!("harddel count={count}");
    println!("harddel uploads={}", upload_states(&pool).await);
    println!("harddel index701={}", index_exists(&pool, 701).await);

    // 7. AuditLogJanitor.
    let (_, altered) =
        codeintel_uploads::delete_old_audit_logs(&pool, config.audit_log_max_age(), now)
            .await
            .expect("DeleteOldAuditLogs");
    println!("auditlogs altered={altered}");
    println!("auditlogs rows={}", audit_rows(&pool).await);

    // 8. SCIPExpirationTask.
    let (scanned, deleted) = codeintel_codegraph::delete_unreferenced_documents(
        &codeintel,
        config.unreferenced_document_batch_size(),
        config.unreferenced_document_max_age(),
        now,
    )
    .await
    .expect("DeleteUnreferencedDocuments");
    println!("scipdocs scanned={scanned} deleted={deleted}");
    println!("scipdocs rows={}", scip_document_rows(&codeintel).await);

    // 9. AbandonedSchemaVersionsRecordsTask.
    let deleted = codeintel_codegraph::delete_abandoned_schema_versions_records(&codeintel)
        .await
        .expect("DeleteAbandonedSchemaVersionsRecords");
    println!("schemaversions deleted={deleted}");

    // 10. UnknownRepositoryJanitor (indexes).
    let (total, altered) = codeintel_uploads::delete_auto_index_jobs_without_repository(&pool, now)
        .await
        .expect("DeleteAutoIndexJobsWithoutRepository");
    println!("unknownrepo total={total} altered={altered}");
    println!("unknownrepo indexes={}", index_states(&pool).await);

    // 11. UnknownCommitJanitor2 (indexes).
    let (scanned, deleted) = codeintel_uploads::process_stale_sourced_commits(
        &pool,
        config.minimum_time_since_last_check(),
        config.commit_resolver_batch_size(),
        &deleter,
        now,
    )
    .await
    .expect("ProcessStaleSourcedCommits");
    println!("staleindex scanned={scanned} deleted={deleted}");
    println!("staleindex indexes={}", index_states(&pool).await);

    // 12. ExpiredRecordJanitor.
    let (scanned, deleted) = codeintel_uploads::expire_failed_records(
        &pool,
        config.failed_index_batch_size(),
        config.failed_index_max_age(),
        now,
    )
    .await
    .expect("ExpireFailedRecords");
    println!("failedrecords scanned={scanned} deleted={deleted}");
    println!("failedrecords indexes={}", index_states(&pool).await);

    // 13. FrontendDBReconciler.
    let candidates = codeintel_uploads::reconcile_candidates(&pool, config.reconciler_batch_size())
        .await
        .expect("ReconcileCandidates");
    let existing = codeintel_codegraph::ids_with_meta(&codeintel, &candidates)
        .await
        .expect("IDsWithMeta");
    let missing = candidates
        .iter()
        .filter(|id| !existing.contains(id))
        .count();
    println!(
        "frontend_reconciler scanned={} altered={missing}",
        candidates.len()
    );

    // 14. CodeIntelDBReconciler.
    let candidates =
        codeintel_codegraph::reconcile_candidates(&codeintel, config.reconciler_batch_size(), now)
            .await
            .expect("codegraph.ReconcileCandidates");
    let existing = codeintel_uploads::existing_upload_ids(&pool, &candidates)
        .await
        .expect("GetUploadsByIDsAllowDeleted");
    let missing_ids: Vec<i64> = candidates
        .iter()
        .copied()
        .filter(|id| !existing.contains(id))
        .collect();
    codeintel_codegraph::delete_lsif_data_by_upload_ids(&codeintel, &missing_ids)
        .await
        .expect("DeleteLsifDataByUploadIds");
    println!(
        "codeintel_reconciler scanned={} altered={}",
        candidates.len(),
        missing_ids.len()
    );
    println!(
        "codeintel_reconciler metadata={}",
        scip_metadata_ids(&codeintel).await
    );
}

async fn upload_states(pool: &PgPool) -> String {
    let rows = sqlx::query("SELECT id, state FROM lsif_uploads ORDER BY id")
        .fetch_all(pool)
        .await
        .expect("upload states");
    let rendered: Vec<String> = rows
        .iter()
        .map(|row| format!("{}:{}", row.get::<i32, _>(0), row.get::<String, _>(1)))
        .collect();
    format!("<{}>", rendered.join(","))
}

async fn index_states(pool: &PgPool) -> String {
    let rows = sqlx::query("SELECT id, state FROM lsif_indexes ORDER BY id")
        .fetch_all(pool)
        .await
        .expect("index states");
    let rendered: Vec<String> = rows
        .iter()
        .map(|row| format!("{}:{}", row.get::<i64, _>(0), row.get::<String, _>(1)))
        .collect();
    format!("<{}>", rendered.join(","))
}

async fn index_exists(pool: &PgPool, id: i64) -> bool {
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM lsif_indexes WHERE id = $1")
        .bind(id)
        .fetch_one(pool)
        .await
        .expect("count index");
    count > 0
}

async fn audit_rows(pool: &PgPool) -> String {
    let rows = sqlx::query(
        "SELECT upload_id, operation::text, COALESCE(reason, '') FROM lsif_uploads_audit_logs \
         ORDER BY upload_id, reason, operation",
    )
    .fetch_all(pool)
    .await
    .expect("audit rows");
    let rendered: Vec<String> = rows
        .iter()
        .map(|row| {
            format!(
                "{}:{}:{}",
                row.get::<i32, _>(0),
                row.get::<String, _>(1),
                row.get::<String, _>(2)
            )
        })
        .collect();
    format!("<{}>", rendered.join(","))
}

async fn scip_document_rows(codeintel: &PgPool) -> String {
    let rows = sqlx::query("SELECT id FROM codeintel_scip_documents ORDER BY id")
        .fetch_all(codeintel)
        .await
        .expect("scip documents");
    let rendered: Vec<String> = rows
        .iter()
        .map(|row| format!("{}", row.get::<i64, _>(0)))
        .collect();
    format!("<{}>", rendered.join(","))
}

async fn scip_metadata_ids(codeintel: &PgPool) -> String {
    let rows = sqlx::query("SELECT upload_id FROM codeintel_scip_metadata ORDER BY upload_id")
        .fetch_all(codeintel)
        .await
        .expect("scip metadata");
    let rendered: Vec<String> = rows
        .iter()
        .map(|row| format!("{}", row.get::<i32, _>(0)))
        .collect();
    format!("<{}>", rendered.join(","))
}
