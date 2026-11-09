#![forbid(unsafe_code)]

//! Differential harness for the insights pings emitter (M9.8cd).
//!
//! Mirror of the Go harness
//! `internal/insights/background/pings/parity_pings_harness_test.go`: same
//! fixtures, same corpus, same canonical `ip ` lines. Both sides reset the
//! scratch frontend/codeinsights databases before every corpus operation and
//! read the `event_logs` rows back.
//!
//! Gated by `SG_INSIGHTS_PINGS_CORPUS`; connections come from
//! `SG_QTEST_FRONTEND_DSN`, `SG_QTEST_INSIGHTS_DSN` and `SG_PINGS_EMPTY_DSN`.

use serde_json::Value;
use sqlx::postgres::{PgPool, PgPoolOptions};
use sqlx::types::time::OffsetDateTime;
use sqlx::Row;

use sg_service_worker::jobs::insights_pings::{
    InsightsPingEmitter, PINGS_EMITTER_DESCRIPTION, PINGS_EMITTER_INTERVAL, PINGS_EMITTER_NAME,
};

const PING_NAMES: [&str; 8] = [
    "INSIGHT_TOTAL_COUNTS",
    "INSIGHT_TOTAL_COUNT_CRITICAL",
    "INSIGHT_TIME_INTERVALS",
    "INSIGHT_ORG_VISIBLE_INSIGHTS",
    "INSIGHT_TOTAL_ORGS_WITH_DASHBOARD",
    "INSIGHT_DASHBOARD_TOTAL_COUNT",
    "INSIGHTS_PER_DASHBORD_STATS",
    "INSIGHTS_BACKFILL_TIME",
];

async fn reset(pool: &PgPool, fixture: &str) {
    sqlx::raw_sql(fixture)
        .execute(pool)
        .await
        .expect("reset fixture");
}

/// Sort every array (the baseline emits regrouped intervals in Go map order;
/// Postgres group order is not guaranteed) and re-serialize. `serde_json`'s
/// object map is a `BTreeMap`, so keys come out sorted like `json.Marshal`.
fn canonical_argument(raw: &str) -> String {
    let Ok(mut value) = serde_json::from_str::<Value>(raw) else {
        return raw.to_owned();
    };
    canonicalize(&mut value);
    serde_json::to_string(&value).unwrap_or_else(|_| raw.to_owned())
}

fn canonicalize(value: &mut Value) {
    match value {
        Value::Object(map) => {
            for nested in map.values_mut() {
                canonicalize(nested);
            }
        }
        Value::Array(items) => {
            for nested in items.iter_mut() {
                canonicalize(nested);
            }
            items.sort_by_key(|item| serde_json::to_string(item).unwrap_or_default());
        }
        _ => {}
    }
}

async fn print_state(frontend: &PgPool, label: &str, failures: Vec<&'static str>) {
    let mut failures = failures;
    failures.sort_unstable();
    for name in &failures {
        println!("ip {label} fail name={name}");
    }
    println!("ip {label} failures count={}", failures.len());

    let names: Vec<String> = PING_NAMES.iter().map(|name| (*name).to_owned()).collect();
    let rows = sqlx::query(
        "SELECT name, argument::text AS argument FROM event_logs \
         WHERE anonymous_user_id = 'backend' AND name = ANY($1) \
         ORDER BY name, argument::text",
    )
    .bind(&names)
    .fetch_all(frontend)
    .await
    .expect("read events");
    for row in &rows {
        let name: String = row.try_get("name").expect("name");
        let argument: String = row.try_get("argument").expect("argument");
        println!(
            "ip {label} event name={name} argument={}",
            canonical_argument(&argument)
        );
    }
    println!("ip {label} events count={}", rows.len());
}

#[tokio::test]
async fn insights_pings_differential() {
    let corpus_path = match std::env::var("SG_INSIGHTS_PINGS_CORPUS") {
        Ok(path) if !path.is_empty() => path,
        _ => return,
    };
    let frontend_dsn = std::env::var("SG_QTEST_FRONTEND_DSN").expect("frontend dsn");
    let insights_dsn = std::env::var("SG_QTEST_INSIGHTS_DSN").expect("insights dsn");
    let empty_dsn = std::env::var("SG_PINGS_EMPTY_DSN").expect("empty dsn");
    let insights_fixture_path =
        std::env::var("SG_INSIGHTS_PINGS_INSIGHTS_FIXTURE").expect("insights fixture");
    let frontend_fixture_path =
        std::env::var("SG_INSIGHTS_PINGS_FRONTEND_FIXTURE").expect("frontend fixture");

    let frontend = PgPoolOptions::new()
        .max_connections(4)
        .connect(&frontend_dsn)
        .await
        .expect("connect frontend");
    let insights = PgPoolOptions::new()
        .max_connections(4)
        .connect(&insights_dsn)
        .await
        .expect("connect codeinsights");
    let empty = PgPoolOptions::new()
        .max_connections(2)
        .connect(&empty_dsn)
        .await
        .expect("connect empty");

    let insights_fixture =
        std::fs::read_to_string(insights_fixture_path).expect("insights fixture");
    let frontend_fixture =
        std::fs::read_to_string(frontend_fixture_path).expect("frontend fixture");

    println!(
        "ip routine name={:?} description={:?} interval={}",
        PINGS_EMITTER_NAME,
        PINGS_EMITTER_DESCRIPTION,
        PINGS_EMITTER_INTERVAL.as_secs()
    );

    let corpus = std::fs::read_to_string(&corpus_path).expect("read corpus");
    for line in corpus.lines() {
        if line.is_empty() {
            continue;
        }
        let parts: Vec<&str> = line.split('\t').collect();
        let op = parts[0];
        let label = parts[1];
        reset(&frontend, &frontend_fixture).await;
        reset(&insights, &insights_fixture).await;

        let emitter = match op {
            "ok" => InsightsPingEmitter::new(
                frontend.clone(),
                insights.clone(),
                OffsetDateTime::now_utc,
            ),
            "broken" => {
                InsightsPingEmitter::new(frontend.clone(), empty.clone(), OffsetDateTime::now_utc)
            }
            other => panic!("unknown op {other:?}"),
        };
        let failures = emitter.emit().await;
        let names: Vec<&'static str> = failures.iter().map(|failure| failure.emitter).collect();
        print_state(&frontend, label, names).await;
    }
}
