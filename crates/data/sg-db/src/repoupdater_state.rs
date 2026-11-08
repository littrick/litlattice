#![forbid(unsafe_code)]

//! Queries behind repo-updater's scrape-time Prometheus gauges
//! (`cmd/repo-updater/shared/main.go` `mustRegisterMetrics`).
//!
//! The baseline registers `promauto.NewGaugeFunc` callbacks that run one query
//! per scrape (and per metric). The Rust debug handler calls these on every
//! `/metrics` request, so the values are just as fresh. A query error is
//! reported by the caller, which publishes `0` like the baseline.

use sqlx::postgres::PgPool;

/// `src_repoupdater_external_services_total`.
pub async fn external_services_total(pool: &PgPool) -> Result<f64, sqlx::Error> {
    let count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM external_services WHERE deleted_at IS NULL")
            .fetch_one(pool)
            .await?;
    Ok(count as f64)
}

/// `src_repoupdater_queued_sync_jobs_total`.
pub async fn queued_sync_jobs_total(pool: &PgPool) -> Result<f64, sqlx::Error> {
    count_sync_jobs(pool, "queued").await
}

/// `src_repoupdater_completed_sync_jobs_total`.
pub async fn completed_sync_jobs_total(pool: &PgPool) -> Result<f64, sqlx::Error> {
    count_sync_jobs(pool, "completed").await
}

async fn count_sync_jobs(pool: &PgPool, state: &str) -> Result<f64, sqlx::Error> {
    let count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM external_service_sync_jobs WHERE state = $1")
            .bind(state)
            .fetch_one(pool)
            .await?;
    Ok(count as f64)
}

/// `src_repoupdater_errored_sync_jobs_percentage`: the share of external
/// services whose most recent sync ended in `errored`, rounded to an integer
/// percentage. `0` when there are no jobs (`NULL` in the baseline).
pub async fn errored_sync_jobs_percentage(pool: &PgPool) -> Result<f64, sqlx::Error> {
    let percentage: f64 = sqlx::query_scalar(
        r#"
WITH latest_state AS (
    -- Get the most recent state per external service
    SELECT DISTINCT ON (external_service_id) external_service_id, state
    FROM external_service_sync_jobs
    ORDER BY external_service_id, finished_at DESC
)
SELECT COALESCE(ROUND((SELECT CAST(COUNT(*) AS FLOAT) FROM latest_state WHERE state = 'errored') /
                      NULLIF((SELECT CAST(COUNT(*) AS FLOAT) FROM latest_state), 0) * 100), 0)::float8
"#,
    )
    .fetch_one(pool)
    .await?;
    Ok(percentage)
}

/// `src_repoupdater_max_sync_backoff`: seconds since the least recently synced
/// external service, ignoring services with an unfinished sync. `0` when no
/// service has ever synced.
pub async fn max_sync_backoff(pool: &PgPool) -> Result<f64, sqlx::Error> {
    let seconds: f64 = sqlx::query_scalar(
        r#"
SELECT COALESCE(EXTRACT(EPOCH FROM MAX(NOW() - last_sync_at))::float8, 0)
FROM external_services AS es
WHERE deleted_at IS NULL
AND last_sync_at IS NOT NULL
-- Exclude any external services that are currently syncing since it's possible they may sync for more
-- than our max backoff time.
AND NOT EXISTS(SELECT FROM external_service_sync_jobs WHERE external_service_id = es.id AND finished_at IS NULL)
"#,
    )
    .fetch_one(pool)
    .await?;
    Ok(seconds)
}

/// `src_repoupdater_stale_repos`: repos owned by a live external service that
/// have not been fetched in 8 hours and whose last fetch failed.
pub async fn stale_repos(pool: &PgPool) -> Result<f64, sqlx::Error> {
    let count: i64 = sqlx::query_scalar(
        r#"
SELECT COUNT(*)
FROM gitserver_repos
WHERE last_fetched < NOW() - INTERVAL '8 hours'
  AND last_error != ''
  AND EXISTS(SELECT
             FROM external_service_repos
                      JOIN external_services es ON external_service_repos.external_service_id = es.id
                      JOIN repo r ON external_service_repos.repo_id = r.id
             WHERE gitserver_repos.repo_id = repo_id
               AND es.deleted_at IS NULL
               AND r.deleted_at IS NULL
    )
"#,
    )
    .fetch_one(pool)
    .await?;
    Ok(count as f64)
}
