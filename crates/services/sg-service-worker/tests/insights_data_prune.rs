#![forbid(unsafe_code)]

//! Differential harness for the insights data pruner (M9.8ce).
//!
//! Mirror of the Go harness
//! `internal/insights/background/parity_data_prune_harness_test.go`: same
//! fixtures, same corpus, same canonical `dp ` lines. Both sides reset the
//! scratch frontend/codeinsights databases before every corpus operation and
//! read the resulting table contents back.
//!
//! Gated by `SG_INSIGHTS_DATA_PRUNE_CORPUS`; connections come from
//! `SG_QTEST_FRONTEND_DSN`, `SG_QTEST_INSIGHTS_DSN` and `SG_DP_EMPTY_DSN`.

use sqlx::postgres::{PgPool, PgPoolOptions};
use sqlx::types::time::OffsetDateTime;
use sqlx::Row;

use sg_service_worker::jobs::insights_data_prune::{
    DataPruner, DATA_PRUNER_DESCRIPTION, DATA_PRUNER_INTERVAL, DATA_PRUNER_NAME,
};

async fn reset(pool: &PgPool, fixture: &str) {
    sqlx::raw_sql(fixture)
        .execute(pool)
        .await
        .expect("reset fixture");
}

/// The error text differs between lib/pq and sqlx; only the stage is
/// compared. Markers are ordered from outer to inner wrapper.
fn stage_of(message: &str) -> &'static str {
    const STAGES: [&str; 5] = [
        "GetSoftDeletedSeries",
        "deleteQueuedRecords",
        "deleteQueuedRetentionRecords",
        "DeleteForSeriesSnapshots",
        "DeleteForSeries",
    ];
    for stage in STAGES {
        if message.contains(stage) {
            return stage;
        }
    }
    "other"
}

async fn print_state(insights: &PgPool, frontend: &PgPool, label: &str) {
    let rows = sqlx::query("SELECT id, series_id FROM insight_series ORDER BY id")
        .fetch_all(insights)
        .await
        .expect("read series");
    for row in &rows {
        let id: i32 = row.try_get("id").expect("id");
        let series_id: String = row.try_get("series_id").expect("series_id");
        println!("dp {label} series id={id} series_id={series_id}");
    }
    println!("dp {label} series count={}", rows.len());

    for (name, query) in [
        ("points", "SELECT COUNT(*) FROM series_points"),
        ("snapshots", "SELECT COUNT(*) FROM series_points_snapshots"),
        (
            "recording-times",
            "SELECT COUNT(*) FROM insight_series_recording_times",
        ),
        (
            "archived-points",
            "SELECT COUNT(*) FROM archived_series_points",
        ),
        (
            "archived-recording-times",
            "SELECT COUNT(*) FROM archived_insight_series_recording_times",
        ),
    ] {
        let count: i64 = sqlx::query_scalar(query)
            .fetch_one(insights)
            .await
            .expect("count");
        println!("dp {label} {name} count={count}");
    }

    let backfills: Vec<i32> =
        sqlx::query_scalar("SELECT series_id FROM insight_series_backfill ORDER BY series_id")
            .fetch_all(insights)
            .await
            .expect("read backfills");
    for series_id in &backfills {
        println!("dp {label} backfill series_id={series_id}");
    }
    println!("dp {label} backfill count={}", backfills.len());

    let queue: Vec<String> =
        sqlx::query_scalar("SELECT series_id FROM insights_query_runner_jobs ORDER BY series_id")
            .fetch_all(frontend)
            .await
            .expect("read queue");
    for series_id in &queue {
        println!("dp {label} queue series_id={series_id}");
    }
    println!("dp {label} queue count={}", queue.len());

    let retention: Vec<String> = sqlx::query_scalar(
        "SELECT series_id_string FROM insights_data_retention_jobs ORDER BY series_id_string",
    )
    .fetch_all(insights)
    .await
    .expect("read retention");
    for series_id in &retention {
        println!("dp {label} retention series_id={series_id}");
    }
    println!("dp {label} retention count={}", retention.len());
}

#[tokio::test]
async fn insights_data_prune_differential() {
    let corpus_path = match std::env::var("SG_INSIGHTS_DATA_PRUNE_CORPUS") {
        Ok(path) if !path.is_empty() => path,
        _ => return,
    };
    let frontend_dsn = std::env::var("SG_QTEST_FRONTEND_DSN").expect("frontend dsn");
    let insights_dsn = std::env::var("SG_QTEST_INSIGHTS_DSN").expect("insights dsn");
    let empty_dsn = std::env::var("SG_DP_EMPTY_DSN").expect("empty dsn");
    let insights_fixture_path =
        std::env::var("SG_INSIGHTS_DATA_PRUNE_INSIGHTS_FIXTURE").expect("insights fixture");
    let frontend_fixture_path =
        std::env::var("SG_INSIGHTS_DATA_PRUNE_FRONTEND_FIXTURE").expect("frontend fixture");

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
        "dp routine name={:?} description={:?} interval={}",
        DATA_PRUNER_NAME,
        DATA_PRUNER_DESCRIPTION,
        DATA_PRUNER_INTERVAL.as_secs()
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

        let pruner = match op {
            "purge" => DataPruner::new(frontend.clone(), insights.clone(), OffsetDateTime::now_utc),
            "broken" => DataPruner::new(frontend.clone(), empty.clone(), OffsetDateTime::now_utc),
            other => panic!("unknown op {other:?}"),
        };
        let result = pruner
            .perform_purge(OffsetDateTime::now_utc() + DATA_PRUNER_INTERVAL)
            .await;
        match result {
            Ok(()) => println!("dp {label} purge result=ok"),
            Err(message) => println!("dp {label} purge result=error stage={}", stage_of(&message)),
        }
        print_state(&insights, &frontend, label).await;
    }
}
