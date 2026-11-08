#![forbid(unsafe_code)]

//! DB-gated check of repo-updater's scrape-time gauges: `Metrics::refresh_state`
//! must publish exactly what the individual state queries return
//! (`cmd/repo-updater/shared/main.go` `mustRegisterMetrics`).
//!
//! Gated on `SG_TEST_DATABASE_DSN`. This is its own test binary, so no other
//! test mutates the state tables while the assertions run.

use sg_service_repo_updater::metrics::Metrics;
use sqlx::postgres::PgPool;

async fn test_pool() -> Option<PgPool> {
    let Ok(dsn) = std::env::var("SG_TEST_DATABASE_DSN") else {
        eprintln!("SG_TEST_DATABASE_DSN unset; skipping");
        return None;
    };
    Some(sg_db::connect(&dsn, 5).await.expect("connect"))
}

fn unique_suffix() -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock")
        .as_nanos();
    format!("metrics-{nanos}")
}

#[tokio::test]
async fn refresh_state_matches_the_state_queries() {
    let Some(pool) = test_pool().await else {
        return;
    };
    let suffix = unique_suffix();

    // One service synced 100 hours ago with a completed and an errored job
    // (the errored one is newer, so it is its latest state); one service that
    // has never synced, with a queued job.
    let synced: i64 = sqlx::query_scalar(
        "INSERT INTO external_services(kind, display_name, config, last_sync_at) \
         VALUES ('OTHER', $1, '{}', clock_timestamp() - interval '100 hours') RETURNING id",
    )
    .bind(format!("{suffix}-synced"))
    .fetch_one(&pool)
    .await
    .expect("insert synced service");
    let unsynced: i64 = sqlx::query_scalar(
        "INSERT INTO external_services(kind, display_name, config) \
         VALUES ('OTHER', $1, '{}') RETURNING id",
    )
    .bind(format!("{suffix}-unsynced"))
    .fetch_one(&pool)
    .await
    .expect("insert unsynced service");
    sqlx::query(
        "INSERT INTO external_service_sync_jobs(external_service_id, state, finished_at) \
         VALUES ($1, 'completed', clock_timestamp() - interval '2 hours')",
    )
    .bind(synced)
    .execute(&pool)
    .await
    .expect("insert completed job");
    // Newer than the completed job, so `errored` is this service's latest state.
    sqlx::query(
        "INSERT INTO external_service_sync_jobs(external_service_id, state, finished_at) \
         VALUES ($1, 'errored', clock_timestamp() - interval '1 hour')",
    )
    .bind(synced)
    .execute(&pool)
    .await
    .expect("insert errored job");
    sqlx::query(
        "INSERT INTO external_service_sync_jobs(external_service_id, state) VALUES ($1, 'queued')",
    )
    .bind(unsynced)
    .execute(&pool)
    .await
    .expect("insert queued job");

    // A repo owned by the synced service whose last fetch failed 9 hours ago.
    let repo_id: i32 = sqlx::query_scalar("INSERT INTO repo(name) VALUES ($1) RETURNING id")
        .bind(format!("{suffix}.example.com/stale"))
        .fetch_one(&pool)
        .await
        .expect("insert repo");
    sqlx::query(
        "UPDATE gitserver_repos SET last_fetched = clock_timestamp() - interval '9 hours', \
         last_error = 'boom' WHERE repo_id = $1",
    )
    .bind(repo_id)
    .execute(&pool)
    .await
    .expect("age gitserver repo");
    sqlx::query(
        "INSERT INTO external_service_repos(external_service_id, repo_id, clone_url) \
         VALUES ($1, $2, $3)",
    )
    .bind(synced)
    .bind(repo_id)
    .bind(format!("https://{suffix}.example.com/stale"))
    .execute(&pool)
    .await
    .expect("insert association");

    let metrics = Metrics::new();
    metrics.refresh_state(&pool).await;

    // Each gauge must carry the value of its own query (the baseline computes
    // them per scrape as `GaugeFunc`s).
    assert_eq!(
        metrics.external_services_total.get(),
        sg_db::repoupdater_state::external_services_total(&pool)
            .await
            .expect("external services")
    );
    assert_eq!(
        metrics.queued_sync_jobs_total.get(),
        sg_db::repoupdater_state::queued_sync_jobs_total(&pool)
            .await
            .expect("queued jobs")
    );
    assert_eq!(
        metrics.completed_sync_jobs_total.get(),
        sg_db::repoupdater_state::completed_sync_jobs_total(&pool)
            .await
            .expect("completed jobs")
    );
    assert_eq!(
        metrics.errored_sync_jobs_percentage.get(),
        sg_db::repoupdater_state::errored_sync_jobs_percentage(&pool)
            .await
            .expect("errored percentage")
    );
    // `now() - last_sync_at` grows between the two queries; compare loosely.
    let backoff = sg_db::repoupdater_state::max_sync_backoff(&pool)
        .await
        .expect("max backoff");
    assert!(
        (metrics.max_sync_backoff.get() - backoff).abs() < 1.0,
        "gauge {} vs query {backoff}",
        metrics.max_sync_backoff.get()
    );
    assert_eq!(
        metrics.stale_repos.get(),
        sg_db::repoupdater_state::stale_repos(&pool)
            .await
            .expect("stale repos")
    );

    // And the seeded rows are visible in the values.
    assert!(metrics.external_services_total.get() >= 2.0);
    assert!(metrics.queued_sync_jobs_total.get() >= 1.0);
    assert!(metrics.completed_sync_jobs_total.get() >= 1.0);
    assert!(
        metrics.errored_sync_jobs_percentage.get() > 0.0,
        "seeded an errored latest state"
    );
    assert!(
        metrics.max_sync_backoff.get() >= 100.0 * 3600.0 - 10.0,
        "seeded a 100 hour old sync: {}",
        metrics.max_sync_backoff.get()
    );
    assert!(metrics.stale_repos.get() >= 1.0);

    sqlx::query("DELETE FROM external_services WHERE id = ANY($1)")
        .bind([synced, unsynced])
        .execute(&pool)
        .await
        .expect("cleanup services");
    sqlx::query("DELETE FROM repo WHERE id = $1")
        .bind(repo_id)
        .execute(&pool)
        .await
        .expect("cleanup repo");
}
