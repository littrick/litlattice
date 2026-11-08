#![forbid(unsafe_code)]

//! Differential-style integration tests for `repos.Syncer` against a real
//! Postgres database: the same store SQL the Go baseline uses, driven by the
//! Rust syncer.
//!
//! Gated on `SG_TEST_DATABASE_DSN` (the isolated `sourcegraph_rust_test`
//! database); the dev database has a live repo-updater that would race the
//! queue and timestamp assertions.

use std::sync::Arc;
use std::time::Duration;

use sg_db::external_service_repos::ExternalServiceReposStore;
use sg_db::external_services::ExternalServiceStore;
use sg_service_repo_updater::metrics::Metrics;
use sg_service_repo_updater::scheduler::SystemClock;
use sg_service_repo_updater::syncer::{
    ExtsvcSourceFactory, NoopProgressRecorder, SyncProgress, Syncer,
};
use sqlx::postgres::PgPool;
use tokio_util::sync::CancellationToken;

fn unique_suffix() -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock")
        .as_nanos();
    format!("sync-{nanos}")
}

async fn test_pool() -> Option<PgPool> {
    let Ok(dsn) = std::env::var("SG_TEST_DATABASE_DSN") else {
        eprintln!("SG_TEST_DATABASE_DSN unset; skipping");
        return None;
    };
    Some(sg_db::connect(&dsn, 5).await.expect("connect"))
}

async fn insert_service(pool: &PgPool, config: &str) -> i64 {
    sqlx::query_scalar(
        "INSERT INTO external_services(kind, display_name, config) \
         VALUES ('OTHER', 'other-sync-test', $1) RETURNING id",
    )
    .bind(config)
    .fetch_one(pool)
    .await
    .expect("insert external service")
}

async fn cleanup(pool: &PgPool, service_id: i64, suffix: &str) {
    sqlx::query("DELETE FROM external_service_repos WHERE external_service_id = $1")
        .bind(service_id)
        .execute(pool)
        .await
        .expect("cleanup associations");
    sqlx::query("DELETE FROM repo WHERE name LIKE $1")
        .bind(format!("%{suffix}.example.com/%"))
        .execute(pool)
        .await
        .expect("cleanup repos");
    sqlx::query("DELETE FROM external_services WHERE id = $1")
        .bind(service_id)
        .execute(pool)
        .await
        .expect("cleanup service");
}

fn syncer(
    pool: &PgPool,
) -> (
    Syncer,
    tokio::sync::broadcast::Receiver<sg_extsvc::RepoSyncDiff>,
) {
    let (tx, rx) = tokio::sync::broadcast::channel(64);
    let syncer = Syncer::new(
        ExternalServiceStore::new(pool.clone()),
        ExternalServiceReposStore::new(pool.clone()),
        // `OTHER` sources configured with explicit clone URLs never touch
        // the HTTP client.
        Arc::new(ExtsvcSourceFactory::new(None)),
        Some(tx),
        Arc::new(SystemClock),
        Arc::new(Metrics::new()),
    );
    (syncer, rx)
}

async fn repo_names(pool: &PgPool, suffix: &str) -> Vec<(String, bool)> {
    let rows = sqlx::query_as::<_, (String, bool)>(
        "SELECT name, deleted_at IS NOT NULL FROM repo \
         WHERE name LIKE $1 ORDER BY name",
    )
    .bind(format!("%{suffix}.example.com/%"))
    .fetch_all(pool)
    .await
    .expect("list repos");
    rows
}

#[tokio::test]
async fn syncs_creates_updates_and_only_deletes_on_success() {
    let Some(pool) = test_pool().await else {
        return;
    };
    let suffix = unique_suffix();
    let base = format!("https://{suffix}.example.com");
    let service_id = insert_service(
        &pool,
        &format!(r#"{{"url":"{base}","repos":["/r1.git","/r2.git"]}}"#),
    )
    .await;

    let (syncer, mut diffs) = syncer(&pool);
    let token = CancellationToken::new();
    let min = Duration::from_secs(60);

    syncer
        .sync_external_service(service_id, min, &NoopProgressRecorder, &token)
        .await
        .expect("first sync");

    let names = repo_names(&pool, &suffix).await;
    assert_eq!(names.len(), 2, "{names:?}");
    assert!(names.iter().all(|(_, deleted)| !deleted));

    // One diff per created repo, each with the database id set.
    let mut added = 0;
    while let Ok(diff) = diffs.try_recv() {
        assert_eq!(diff.added.len(), 1);
        assert!(diff.added[0].id > 0);
        added += diff.added.len();
    }
    assert_eq!(added, 2);

    // The association carries the clone URL.
    let clone_urls: Vec<String> = sqlx::query_scalar(
        "SELECT clone_url FROM external_service_repos WHERE external_service_id = $1 \
         ORDER BY clone_url",
    )
    .bind(service_id)
    .fetch_all(&pool)
    .await
    .expect("associations");
    assert_eq!(
        clone_urls,
        vec![format!("{base}/r1.git"), format!("{base}/r2.git")]
    );

    // The timestamps are always written, and a first sync backs off by the
    // minimum interval.
    let (last, next): (Option<sg_db::OffsetDateTime>, Option<sg_db::OffsetDateTime>) =
        sqlx::query_as("SELECT last_sync_at, next_sync_at FROM external_services WHERE id = $1")
            .bind(service_id)
            .fetch_one(&pool)
            .await
            .expect("timestamps");
    let (last, next) = (last.expect("last_sync_at"), next.expect("next_sync_at"));
    assert_eq!(next - last, time::Duration::seconds(60));

    // A second sync with no source changes yields unmodified repos and no
    // writes.
    // `createRepoQuery` does not set `updated_at`; an unmodified sync must
    // leave it NULL.
    let updated_before: Vec<Option<sg_db::OffsetDateTime>> =
        sqlx::query_scalar("SELECT updated_at FROM repo WHERE name LIKE $1 ORDER BY name")
            .bind(format!("%{suffix}.example.com/%"))
            .fetch_all(&pool)
            .await
            .expect("updated_at");
    syncer
        .sync_external_service(service_id, min, &NoopProgressRecorder, &token)
        .await
        .expect("second sync");
    let mut unmodified = 0;
    while let Ok(diff) = diffs.try_recv() {
        unmodified += diff.unmodified.len();
    }
    assert_eq!(unmodified, 2);
    let updated_after: Vec<Option<sg_db::OffsetDateTime>> =
        sqlx::query_scalar("SELECT updated_at FROM repo WHERE name LIKE $1 ORDER BY name")
            .bind(format!("%{suffix}.example.com/%"))
            .fetch_all(&pool)
            .await
            .expect("updated_at");
    assert_eq!(updated_before, updated_after);

    // Changing the sourced set soft-deletes the repo that disappeared and
    // creates the new one.
    sqlx::query("UPDATE external_services SET config = $2 WHERE id = $1")
        .bind(service_id)
        .bind(format!(
            r#"{{"url":"{base}","repos":["/r1.git","/r3.git"]}}"#
        ))
        .execute(&pool)
        .await
        .expect("update config");
    syncer
        .sync_external_service(service_id, min, &NoopProgressRecorder, &token)
        .await
        .expect("third sync");

    let names = repo_names(&pool, &suffix).await;
    let live: Vec<&(String, bool)> = names.iter().filter(|(_, deleted)| !deleted).collect();
    assert_eq!(live.len(), 2, "{names:?}");
    let deleted = names
        .iter()
        .find(|(_, deleted)| *deleted)
        .expect("r2 soft-deleted");
    assert!(deleted.0.starts_with("DELETED-"), "{deleted:?}");
    assert!(deleted.0.ends_with(&format!("{suffix}.example.com/r2")));

    let mut deleted_ids = Vec::new();
    while let Ok(diff) = diffs.try_recv() {
        deleted_ids.extend(diff.deleted.iter().map(|repo| repo.id));
    }
    assert_eq!(deleted_ids.len(), 1);

    // Now make the source fail: the previous repos must survive.
    sqlx::query("UPDATE external_services SET config = $2 WHERE id = $1")
        .bind(service_id)
        .bind(format!(r#"{{"url":"{base}","repos":["src-expose"]}}"#))
        .execute(&pool)
        .await
        .expect("update config");
    let err = syncer
        .sync_external_service(service_id, min, &NoopProgressRecorder, &token)
        .await
        .expect_err("source failure");
    assert!(
        err.contains("src-expose HTTP client is not configured"),
        "{err}"
    );
    let live_after = repo_names(&pool, &suffix)
        .await
        .into_iter()
        .filter(|(_, deleted)| !deleted)
        .count();
    assert_eq!(live_after, 2, "an errored sync must not delete repos");

    cleanup(&pool, service_id, &suffix).await;
}

#[tokio::test]
async fn revives_soft_deleted_repos_and_reports_progress() {
    let Some(pool) = test_pool().await else {
        return;
    };
    let suffix = unique_suffix();
    let base = format!("https://{suffix}.example.com");
    let service_id =
        insert_service(&pool, &format!(r#"{{"url":"{base}","repos":["/r1.git"]}}"#)).await;

    let (syncer, _diffs) = syncer(&pool);
    let token = CancellationToken::new();
    let progress = Arc::new(std::sync::Mutex::new(Vec::<SyncProgress>::new()));

    struct Recording(Arc<std::sync::Mutex<Vec<SyncProgress>>>);
    #[async_trait::async_trait]
    impl sg_service_repo_updater::syncer::ProgressRecorder for Recording {
        async fn record(&self, progress: SyncProgress, final_call: bool) -> Result<(), String> {
            if final_call {
                self.0.lock().expect("progress").push(progress);
            }
            Ok(())
        }
    }

    syncer
        .sync_external_service(
            service_id,
            Duration::from_secs(60),
            &Recording(Arc::clone(&progress)),
            &token,
        )
        .await
        .expect("first sync");

    let recorded = progress.lock().expect("progress").clone();
    assert_eq!(recorded.len(), 1);
    assert_eq!(recorded[0].synced, 1);
    assert_eq!(recorded[0].added, 1);
    assert_eq!(recorded[0].errors, 0);

    // Soft-delete the repo out of band, then sync again: the source still
    // yields it, so the syncer clears `deleted_at` and reports it as added.
    sqlx::query("UPDATE repo SET deleted_at = NOW() WHERE name LIKE $1")
        .bind(format!("%{suffix}.example.com/%"))
        .execute(&pool)
        .await
        .expect("soft delete");
    syncer
        .sync_external_service(
            service_id,
            Duration::from_secs(60),
            &Recording(Arc::clone(&progress)),
            &token,
        )
        .await
        .expect("second sync");

    let recorded = progress.lock().expect("progress").clone();
    assert_eq!(recorded[1].added, 1, "{:?}", recorded[1]);

    let live = repo_names(&pool, &suffix)
        .await
        .into_iter()
        .filter(|(_, deleted)| !deleted)
        .count();
    assert_eq!(live, 1);

    cleanup(&pool, service_id, &suffix).await;
}

/// `Syncer.observeSync` / `ObservedSource.ListRepos` / `observeDiff` must
/// publish the same families the baseline does.
#[tokio::test]
async fn emits_syncer_and_source_metrics() {
    let Some(pool) = test_pool().await else {
        return;
    };
    let suffix = unique_suffix();
    let base = format!("https://{suffix}.example.com");
    let service_id =
        insert_service(&pool, &format!(r#"{{"url":"{base}","repos":["/r1.git"]}}"#)).await;

    let metrics = Arc::new(Metrics::new());
    let syncer = Syncer::new(
        ExternalServiceStore::new(pool.clone()),
        ExternalServiceReposStore::new(pool.clone()),
        Arc::new(ExtsvcSourceFactory::new(None)),
        None,
        Arc::new(SystemClock),
        Arc::clone(&metrics),
    );
    syncer
        .sync_external_service(
            service_id,
            Duration::from_secs(60),
            &NoopProgressRecorder,
            &CancellationToken::new(),
        )
        .await
        .expect("sync");

    let family = sg_service_repo_updater::metrics::SYNC_FAMILY;
    assert_eq!(
        metrics
            .syncer_start_sync
            .with_label_values(&[family, "site"])
            .get(),
        1.0
    );
    assert!(
        metrics
            .syncer_sync_last_time
            .with_label_values(&[family])
            .get()
            > 0.0
    );
    assert_eq!(
        metrics
            .syncer_sync_duration_seconds
            .with_label_values(&["true", family])
            .get_sample_count(),
        1
    );
    assert_eq!(
        metrics
            .syncer_sync_errors_total
            .with_label_values(&[family, "site", "unknown"])
            .get(),
        0.0
    );
    assert_eq!(
        metrics
            .syncer_synced_repos_total
            .with_label_values(&["added"])
            .get(),
        1.0
    );
    assert_eq!(metrics.source_repos_total.with_label_values(&[]).get(), 1.0);
    assert_eq!(
        metrics
            .source_duration_seconds
            .with_label_values(&[])
            .get_sample_count(),
        1
    );
    assert_eq!(
        metrics.source_errors_total.with_label_values(&[]).get(),
        0.0
    );

    cleanup(&pool, service_id, &suffix).await;
}
