#![forbid(unsafe_code)]

//! M9.8c differential harness (Rust side): the event logs janitor.
//!
//! Expects `test_ws/p9-go/eventlogs_fixtures.sql` to have been applied to
//! `SG_QTEST_DSN` immediately before the run. `SG_SECURITY_EVENT_LOG` selects
//! the `log.securityEventLog.location` site setting (database, all, none, or
//! unset). The Go counterpart is
//! `cmd/worker/internal/eventlogs/harness_test.go`.

use sg_config::SiteConfiguration;
use sg_service_worker::jobs::eventlogs::{delete_old_event_logs, delete_old_security_event_logs};

#[tokio::test]
async fn event_logs_janitor_matches_go_harness() {
    let Ok(dsn) = std::env::var("SG_QTEST_DSN") else {
        return;
    };
    let Ok(mode) = std::env::var("SG_SECURITY_EVENT_LOG") else {
        return;
    };

    let site_json = match mode.as_str() {
        "database" | "all" | "none" => {
            format!(r#"{{"log": {{"securityEventLog": {{"location": "{mode}"}}}}}}"#)
        }
        "unset" => "{}".to_owned(),
        other => panic!("unknown SG_SECURITY_EVENT_LOG {other:?}"),
    };
    let site = SiteConfiguration::parse(&site_json).expect("parse site config");

    let pool = sg_db::connect(&dsn, 1).await.expect("connect database");
    delete_old_event_logs(&pool)
        .await
        .expect("delete old event logs");
    delete_old_security_event_logs(&pool, &site)
        .await
        .expect("delete old security event logs");

    let event_logs: Vec<i64> =
        sqlx::query_scalar("SELECT id FROM event_logs WHERE id BETWEEN 9201 AND 9204 ORDER BY id")
            .fetch_all(&pool)
            .await
            .expect("select event_logs");
    let security_event_logs: Vec<i64> = sqlx::query_scalar(
        "SELECT id FROM security_event_logs WHERE id BETWEEN 9301 AND 9304 ORDER BY id",
    )
    .fetch_all(&pool)
    .await
    .expect("select security_event_logs");

    println!("event_logs remaining={}", ids(&event_logs));
    println!(
        "security_event_logs remaining={}",
        ids(&security_event_logs)
    );
}

fn ids(values: &[i64]) -> String {
    let values: Vec<String> = values.iter().map(i64::to_string).collect();
    format!("[{}]", values.join(","))
}
