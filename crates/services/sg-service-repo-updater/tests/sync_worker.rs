#![forbid(unsafe_code)]

//! End-to-end test of the external service sync worker: enqueue a job in the
//! database, run the worker loops for a moment, and check the job state,
//! progress counters, timestamps and repository rows.
//!
//! Gated on `SG_TEST_DATABASE_DSN` (isolated `sourcegraph_rust_test`).

use std::sync::Arc;
use std::time::{Duration, Instant};

use sg_config::LiveConfig;
use sg_db::external_service_repos::ExternalServiceReposStore;
use sg_db::external_service_sync_jobs::SyncJobsStore;
use sg_db::external_services::ExternalServiceStore;
use sg_service_repo_updater::metrics::Metrics;
use sg_service_repo_updater::scheduler::SystemClock;
use sg_service_repo_updater::sync_worker::SyncWorker;
use sg_service_repo_updater::syncer::{ExtsvcSourceFactory, StaticLicenseKey, Syncer};
use sqlx::postgres::PgPool;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

fn unique_suffix() -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock")
        .as_nanos();
    format!("worker-{nanos}")
}

#[tokio::test]
async fn worker_runs_a_queued_job_to_completion() {
    let Ok(dsn) = std::env::var("SG_TEST_DATABASE_DSN") else {
        eprintln!("SG_TEST_DATABASE_DSN unset; skipping");
        return;
    };
    let pool: PgPool = sg_db::connect(&dsn, 5).await.expect("connect");

    let suffix = unique_suffix();
    let base = format!("https://{suffix}.example.com");
    let service_id: i64 = sqlx::query_scalar(
        "INSERT INTO external_services(kind, display_name, config, unrestricted) \
         VALUES ('OTHER', 'worker-test', $1, true) RETURNING id",
    )
    .bind(format!(r#"{{"url":"{base}","repos":["/r1.git"]}}"#))
    .fetch_one(&pool)
    .await
    .expect("insert service");

    // Suppress the enqueue loop: the test drives the queue directly, so the
    // global "enqueue due services" scan cannot interfere.
    let unified = sg_config::Unified::parse(&sg_config::RawUnified {
        site: r#"{"disableAutoCodeHostSyncs": true}"#.to_owned(),
        ..Default::default()
    })
    .expect("site config");
    let live = Arc::new(LiveConfig::new(unified));

    let jobs = SyncJobsStore::new(pool.clone());
    jobs.enqueue_single_sync_job(service_id)
        .await
        .expect("enqueue");

    let metrics = Arc::new(Metrics::new());
    let syncer = Arc::new(Syncer::new(
        ExternalServiceStore::new(pool.clone()),
        ExternalServiceReposStore::new(pool.clone()).with_observer(
            Arc::clone(&metrics) as Arc<dyn sg_db::external_service_repos::StoreObserver>
        ),
        Arc::new(ExtsvcSourceFactory::new(
            None,
            Arc::new(sg_keyring::Keyring::default()),
        )),
        None,
        Arc::new(SystemClock),
        Arc::clone(&metrics),
        // No license key: the free plan's private-repository limit applies.
        Arc::new(StaticLicenseKey::default()),
    ));
    let worker = Arc::new(
        SyncWorker::new(
            jobs.clone(),
            syncer,
            Arc::clone(&live),
            Arc::clone(&metrics),
            "test-host",
        )
        .with_num_handlers(1)
        .with_worker_interval(Duration::from_millis(20))
        .with_cleanup_old_jobs(false),
    );

    let shutdown = CancellationToken::new();
    let tasks = TaskTracker::new();
    Arc::clone(&worker).spawn(&shutdown, &tasks);

    let state = wait_for_state(&jobs, service_id, Instant::now() + Duration::from_secs(10)).await;
    assert_eq!(state, "completed");

    let names: Vec<String> =
        sqlx::query_scalar("SELECT name FROM repo WHERE name LIKE $1 ORDER BY name")
            .bind(format!("%{suffix}.example.com/%"))
            .fetch_all(&pool)
            .await
            .expect("repos");
    assert_eq!(names, vec![format!("{suffix}.example.com/r1")]);

    let counters: (i32, i32, i32, i32) = sqlx::query_as(
        "SELECT repos_synced, repos_added, repos_modified, repo_sync_errors \
         FROM external_service_sync_jobs WHERE external_service_id = $1 \
         ORDER BY id DESC LIMIT 1",
    )
    .bind(service_id)
    .fetch_one(&pool)
    .await
    .expect("counters");
    assert_eq!(counters, (1, 1, 0, 0));

    // `workerutil`/`dbworkerstore` metrics: the Dequeue poll ran, the job
    // was handled once and marked complete. (`Heartbeat` fires every 15s, so
    // it is normally absent for a short test.)
    assert!(
        metrics
            .sync_worker_store_total
            .with_label_values(&["Dequeue"])
            .get()
            > 0.0
    );
    assert_eq!(
        metrics
            .sync_worker_store_total
            .with_label_values(&["MarkComplete"])
            .get(),
        1.0
    );
    assert_eq!(
        metrics
            .sync_worker_store_duration_seconds
            .with_label_values(&["MarkComplete"])
            .get_sample_count(),
        1
    );
    assert_eq!(
        metrics
            .sync_worker_total
            .with_label_values(&["Handle"])
            .get(),
        1.0
    );
    assert_eq!(
        metrics
            .sync_worker_errors_total
            .with_label_values(&["Handle"])
            .get(),
        0.0
    );

    // `StoreMetrics`: one transaction for the repo, one create, and the
    // (empty) `DeleteExternalServiceReposNotIn` pass.
    assert_eq!(
        metrics.store_transact_total.with_label_values(&[]).get(),
        1.0
    );
    assert_eq!(metrics.store_done_total.with_label_values(&[]).get(), 1.0);
    assert_eq!(
        metrics
            .store_create_external_service_repo_total
            .with_label_values(&[])
            .get(),
        1.0
    );
    assert_eq!(
        metrics
            .store_delete_external_service_repos_not_in_total
            .with_label_values(&[])
            .get(),
        1.0
    );

    let last_sync_at: Option<sg_db::OffsetDateTime> =
        sqlx::query_scalar("SELECT last_sync_at FROM external_services WHERE id = $1")
            .bind(service_id)
            .fetch_one(&pool)
            .await
            .expect("last_sync_at");
    assert!(last_sync_at.is_some());

    shutdown.cancel();
    tasks.close();
    tasks.wait().await;

    sqlx::query("DELETE FROM external_service_sync_jobs WHERE external_service_id = $1")
        .bind(service_id)
        .execute(&pool)
        .await
        .expect("cleanup jobs");
    sqlx::query("DELETE FROM external_service_repos WHERE external_service_id = $1")
        .bind(service_id)
        .execute(&pool)
        .await
        .expect("cleanup associations");
    sqlx::query("DELETE FROM repo WHERE name LIKE $1")
        .bind(format!("%{suffix}.example.com/%"))
        .execute(&pool)
        .await
        .expect("cleanup repos");
    sqlx::query("DELETE FROM external_services WHERE id = $1")
        .bind(service_id)
        .execute(&pool)
        .await
        .expect("cleanup service");
}

async fn wait_for_state(jobs: &SyncJobsStore, service_id: i64, deadline: Instant) -> String {
    loop {
        let state: Option<String> = sqlx::query_scalar(
            "SELECT state FROM external_service_sync_jobs WHERE external_service_id = $1 \
             ORDER BY id DESC LIMIT 1",
        )
        .bind(service_id)
        .fetch_optional(jobs.pool())
        .await
        .expect("job state");
        if let Some(state) = state {
            if state != "queued" && state != "processing" {
                return state;
            }
        }
        assert!(Instant::now() < deadline, "job did not finish in time");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}
