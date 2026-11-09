#![forbid(unsafe_code)]

//! M9.8du differential harness (Rust side): the coordinator handler
//! composition (feature gate → cron due check → bump → `Coordinate`), mirrored
//! from `internal/codeintel/ranking/internal/background/coordinator`.
//!
//! `test_ws/p9-go/run_codeintel_ranking_coordinator_diff.sh` applies
//! `codeintel_ranking_coordinator_fixtures.sql` immediately before each side
//! and diffs the canonical `rcoord ` JSON lines. Both sides reset
//! `codeintel_ranking_progress` / `codeintel_ranking_graph_keys` per scenario
//! and only print normalized state (key kinds, counts, booleans), so UUIDs and
//! wall-clock timestamps never enter the diff.

use serde_json::{json, Value};
use sg_config::SiteConfiguration;
use sg_service_worker::jobs::codeintel_ranking_file_reference_counter::run_coordinator;
use sqlx::postgres::PgPool;
use sqlx::Row;

fn print(value: Value) {
    println!(
        "rcoord {}",
        serde_json::to_string(&value).expect("serialize")
    );
}

fn site_with(enabled: bool, expression: Option<&str>) -> SiteConfiguration {
    let mut config = serde_json::Map::new();
    config.insert(
        "codeIntelRanking.documentReferenceCountsEnabled".into(),
        json!(enabled),
    );
    config.insert(
        "codeIntelRanking.documentReferenceCountsGraphKey".into(),
        json!("rcoord"),
    );
    if let Some(expression) = expression {
        config.insert(
            "codeIntelRanking.documentReferenceCountsCronExpression".into(),
            json!(expression),
        );
    }
    SiteConfiguration::parse(&Value::Object(config).to_string()).expect("valid site config")
}

fn is_uuid(value: &str) -> bool {
    value.len() == 36 && value.matches('-').count() == 4
}

fn key_kind(key: &str) -> &'static str {
    if key == "fixture-key" {
        "fixture"
    } else if is_uuid(key) {
        "uuid"
    } else if key == "rcoord.fixture_key" {
        "fixture_derivative"
    } else if key.strip_prefix("rcoord.").is_some_and(is_uuid) {
        "uuid_derivative"
    } else {
        "other"
    }
}

async fn graph_keys(pool: &PgPool) -> (i64, String) {
    let count = sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM codeintel_ranking_graph_keys")
        .fetch_one(pool)
        .await
        .expect("count graph keys");
    let latest = sqlx::query_scalar::<_, String>(
        "SELECT graph_key FROM codeintel_ranking_graph_keys ORDER BY created_at DESC LIMIT 1",
    )
    .fetch_optional(pool)
    .await
    .expect("latest graph key");
    (
        count,
        latest
            .map(|key| key_kind(&key).to_string())
            .unwrap_or_default(),
    )
}

async fn progress(pool: &PgPool) -> Vec<Value> {
    let rows = sqlx::query(
        "SELECT graph_key, max_export_id, num_path_records_total,
                num_reference_records_total, (mappers_started_at IS NOT NULL) AS mappers_started,
                (mapper_completed_at IS NOT NULL) AS mapper_completed,
                (reducer_started_at IS NOT NULL) AS reducer_started
         FROM codeintel_ranking_progress ORDER BY id",
    )
    .fetch_all(pool)
    .await
    .expect("progress");

    rows.iter()
        .map(|row| {
            let graph_key = row.try_get::<String, _>("graph_key").expect("graph_key");
            json!({
                "graph_key_kind": key_kind(&graph_key),
                "max_export_id": row.try_get::<i64, _>("max_export_id").expect("max_export_id"),
                "num_paths": row.try_get::<Option<i32>, _>("num_path_records_total").expect("num_path_records_total").unwrap_or(0),
                "num_references": row.try_get::<Option<i32>, _>("num_reference_records_total").expect("num_reference_records_total").unwrap_or(0),
                "mappers_started": row.try_get::<bool, _>("mappers_started").expect("mappers_started"),
                "mapper_completed": row.try_get::<bool, _>("mapper_completed").expect("mapper_completed"),
                "reducer_started": row.try_get::<bool, _>("reducer_started").expect("reducer_started"),
            })
        })
        .collect()
}

async fn scenario(
    pool: &PgPool,
    name: &str,
    enabled: bool,
    expression: Option<&str>,
    key_age_seconds: Option<i64>,
) {
    sqlx::query(
        "TRUNCATE codeintel_ranking_progress, codeintel_ranking_graph_keys RESTART IDENTITY",
    )
    .execute(pool)
    .await
    .expect("reset");

    if let Some(age) = key_age_seconds {
        sqlx::query(
            "INSERT INTO codeintel_ranking_graph_keys (graph_key, created_at)
             VALUES ($1, now() - make_interval(secs => $2))",
        )
        .bind("fixture-key")
        .bind(age)
        .execute(pool)
        .await
        .expect("insert fixture key");
    }

    let site = site_with(enabled, expression);
    let errored = run_coordinator(pool, &site).await.is_err();
    let (graph_keys, latest_kind) = graph_keys(pool).await;
    print(json!({
        "name": name,
        "errored": errored,
        "graph_keys": graph_keys,
        "latest_kind": latest_kind,
        "progress": progress(pool).await,
    }));
}

#[tokio::test]
async fn ranking_coordinator_harness() {
    if std::env::var("SG_CODEINTEL_RANKING_COORDINATOR_DIFF").as_deref() != Ok("1") {
        return;
    }
    let Ok(dsn) = std::env::var("SG_CODEINTEL_RANKING_COORDINATOR_TEST_DSN") else {
        return;
    };

    let pool = PgPool::connect(&dsn).await.expect("connect database");

    let day = 24 * 3600;
    scenario(&pool, "gate_off", false, None, Some(8 * day)).await;
    scenario(&pool, "weekly_due", true, None, Some(8 * day)).await;
    scenario(&pool, "weekly_not_due", true, None, Some(0)).await;
    scenario(&pool, "minutely_due", true, Some("* * * * *"), Some(120)).await;
    scenario(
        &pool,
        "invalid_cron",
        true,
        Some("not a cron"),
        Some(8 * day),
    )
    .await;
    scenario(&pool, "empty_keys", true, None, None).await;
    scenario(&pool, "past_year", true, Some("* * * * * 1980"), Some(0)).await;
}
