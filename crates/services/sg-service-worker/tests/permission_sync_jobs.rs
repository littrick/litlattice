#![forbid(unsafe_code)]

//! M9.8e differential harness (Rust side): the permission sync job cleaner.
//!
//! Expects `test_ws/p9-go/permission_sync_jobs_fixtures.sql` to have been
//! applied to `SG_QTEST_DSN` immediately before the run. `SG_PERMS_SITE_JSON`
//! is the site configuration used for the interval / history size (default
//! `{}`). The Go counterpart is
//! `cmd/worker/internal/permissions/harness_test.go`.

use sg_config::SiteConfiguration;
use sg_service_worker::jobs::permissions::{cleanup_interval, jobs_to_keep};

#[tokio::test]
async fn permission_sync_job_cleaner_matches_go_harness() {
    let Ok(dsn) = std::env::var("SG_QTEST_DSN") else {
        return;
    };
    let site_json = std::env::var("SG_PERMS_SITE_JSON").unwrap_or_else(|_| "{}".to_owned());
    let site = SiteConfiguration::parse(&site_json).expect("parse site config");

    println!("interval_secs={}", cleanup_interval(&site).as_secs());

    let pool = sg_db::connect(&dsn, 2).await.expect("connect database");
    let deleted = sg_db::permission_sync_jobs::clean_jobs(&pool, jobs_to_keep(&site))
        .await
        .expect("clean jobs");
    println!("deleted={deleted}");

    let ids: Vec<i32> = sqlx::query_scalar(
        "SELECT id FROM permission_sync_jobs WHERE id BETWEEN 9601 AND 9699 ORDER BY id",
    )
    .fetch_all(&pool)
    .await
    .expect("select remaining jobs");
    let ids: Vec<String> = ids.iter().map(i32::to_string).collect();
    println!("remaining=[{}]", ids.join(","));
}
