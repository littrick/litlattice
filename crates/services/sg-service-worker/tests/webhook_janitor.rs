#![forbid(unsafe_code)]

//! M9.8b differential harness (Rust side): `calculateRetention` and the
//! `webhook_logs` delete performed by the janitor.
//!
//! Expects `test_ws/p9-go/webhook_janitor_fixtures.sql` to have been applied
//! to `SG_QTEST_DSN` immediately before the test runs and reads the retention
//! from `SG_JANITOR_RETENTION` (Go duration syntax). The Go counterpart is
//! `cmd/worker/internal/webhooks/janitor_harness_test.go`;
//! `test_ws/p9-go/run_webhook_janitor_diff.sh` compares the outputs.

use sg_config::SiteConfiguration;
use sg_service_worker::jobs::webhooks::{calculate_retention, delete_stale, parse_go_duration};

#[tokio::test]
async fn webhook_janitor_matches_go_harness() {
    for raw in [
        r#"{}"#,
        r#"{"webhook.logging": null}"#,
        r#"{"webhook.logging": {}}"#,
        r#"{"webhook.logging": {"retention": "48h"}}"#,
        r#"{"webhook.logging": {"retention": "90m"}}"#,
        r#"{"webhook.logging": {"retention": "soon"}}"#,
    ] {
        let site = SiteConfiguration::parse(raw).expect("parse site config");
        let retention = calculate_retention(&site);
        println!("retention json={raw:?} secs={}", retention.as_secs());
    }

    let Ok(dsn) = std::env::var("SG_QTEST_DSN") else {
        return;
    };
    let Ok(raw_retention) = std::env::var("SG_JANITOR_RETENTION") else {
        return;
    };
    let retention = parse_go_duration(&raw_retention).expect("parse SG_JANITOR_RETENTION");

    let pool = sg_db::connect(&dsn, 1).await.expect("connect database");
    delete_stale(&pool, retention)
        .await
        .expect("delete stale webhook logs");

    let remaining: Vec<i64> = sqlx::query_scalar(
        "SELECT id FROM webhook_logs WHERE id BETWEEN 9101 AND 9106 ORDER BY id",
    )
    .fetch_all(&pool)
    .await
    .expect("select remaining");

    let remaining: Vec<String> = remaining.iter().map(i64::to_string).collect();
    println!(
        "delete retention={raw_retention} remaining=[{}]",
        remaining.join(",")
    );
}
