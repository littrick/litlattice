#![forbid(unsafe_code)]

//! M9.8dr differential harness (Rust side): the job-level composition of the
//! ranking background routines (feature gate, graph-key derivation and
//! bump-if-missing, routine routing), mirrored from the Go harnesses in
//! `internal/codeintel/ranking/internal/background/{mapper,reducer,janitor}`
//! and `internal/codeintel/ranking/internal/store`.
//!
//! `test_ws/p9-go/run_codeintel_ranking_background_jobs_diff.sh` applies
//! `codeintel_ranking_background_fixtures.sql` immediately before each side,
//! runs the go harnesses in the order mapper → reducer → janitor → bump, and
//! diffs the canonical `rbg ` JSON lines. This test emits the same sections in
//! the same order.

use serde_json::{json, Value};
use sg_config::SiteConfiguration;
use sg_db::codeintel_ranking::{self, derivative_graph_key_or_bump};
use sg_service_worker::jobs::codeintel_ranking_file_reference_counter::{
    map_initializer_ranking_graph, map_ranking_graph, reduce_ranking_graph, run_janitor,
    JanitorKind,
};
use sqlx::postgres::PgPool;
use sqlx::Row;

fn print(value: Value) {
    println!("rbg {}", serde_json::to_string(&value).expect("serialize"));
}

fn site_with(enabled: bool) -> SiteConfiguration {
    let enabled = if enabled { "true" } else { "false" };
    SiteConfiguration::parse(&format!(
        r#"{{"codeIntelRanking.documentReferenceCountsEnabled": {enabled}, "codeIntelRanking.documentReferenceCountsGraphKey": "rbg"}}"#
    ))
    .expect("valid site config")
}

async fn dump_inputs(pool: &PgPool) -> Vec<Value> {
    let rows = sqlx::query(
        "SELECT graph_key, definition_id, count, processed FROM codeintel_ranking_path_counts_inputs ORDER BY graph_key, definition_id, count, processed",
    )
    .fetch_all(pool)
    .await
    .expect("inputs");

    rows.iter()
        .map(|row| {
            let definition_id = row
                .try_get::<Option<i64>, _>("definition_id")
                .expect("definition_id");
            json!({
                "graph_key": row.try_get::<String, _>("graph_key").expect("graph_key"),
                "definition_id": match definition_id {
                    Some(value) => json!(value),
                    None => Value::Null,
                },
                "count": row.try_get::<i32, _>("count").expect("count"),
                "processed": row.try_get::<bool, _>("processed").expect("processed"),
            })
        })
        .collect()
}

async fn dump_ranks(pool: &PgPool) -> Vec<Value> {
    let rows = sqlx::query(
        "SELECT repository_id, graph_key, payload::text AS payload FROM codeintel_path_ranks ORDER BY graph_key, repository_id",
    )
    .fetch_all(pool)
    .await
    .expect("ranks");

    rows.iter()
        .map(|row| {
            json!({
                "repository_id": row.try_get::<i32, _>("repository_id").expect("repository_id"),
                "graph_key": row.try_get::<String, _>("graph_key").expect("graph_key"),
                "payload": row.try_get::<String, _>("payload").expect("payload"),
            })
        })
        .collect()
}

async fn count(pool: &PgPool, query: &str) -> i64 {
    sqlx::query_scalar::<_, i64>(query)
        .fetch_one(pool)
        .await
        .expect("count")
}

async fn state(pool: &PgPool) -> Value {
    json!({
        "deleted_exports": count(pool, "SELECT COUNT(*) FROM codeintel_ranking_exports WHERE deleted_at IS NOT NULL").await,
        "exports": count(pool, "SELECT COUNT(*) FROM codeintel_ranking_exports").await,
        "inputs": count(pool, "SELECT COUNT(*) FROM codeintel_ranking_path_counts_inputs").await,
        "ranks": count(pool, "SELECT COUNT(*) FROM codeintel_path_ranks").await,
        "processed_refs": count(pool, "SELECT COUNT(*) FROM codeintel_ranking_references_processed").await,
        "processed_paths": count(pool, "SELECT COUNT(*) FROM codeintel_initial_path_ranks_processed").await,
        "graph_keys": count(pool, "SELECT COUNT(*) FROM codeintel_ranking_graph_keys").await,
    })
}

#[tokio::test]
async fn ranking_background_jobs_harness() {
    if std::env::var("SG_CODEINTEL_RANKING_BACKGROUND_DIFF").as_deref() != Ok("1") {
        return;
    }
    let Ok(dsn) = std::env::var("SG_CODEINTEL_RANKING_BACKGROUND_TEST_DSN") else {
        return;
    };

    let pool = PgPool::connect(&dsn).await.expect("connect database");
    let disabled = site_with(false);
    let enabled = site_with(true);

    // -- mapper / seed mapper ------------------------------------------------
    let (processed, inserted) = map_ranking_graph(&pool, &disabled, 1000)
        .await
        .expect("map ranking graph (disabled)");
    let (seed_processed, seed_inserted) = map_initializer_ranking_graph(&pool, &disabled, 1000)
        .await
        .expect("map initializer ranking graph (disabled)");
    print(json!({
        "kind": "gate_off",
        "mapper": [processed, inserted],
        "seed": [seed_processed, seed_inserted],
        "inputs": dump_inputs(&pool).await,
    }));

    let (processed, inserted) = map_ranking_graph(&pool, &enabled, 1000)
        .await
        .expect("map ranking graph");
    print(json!({
        "kind": "mapper",
        "processed": processed,
        "inserted": inserted,
        "inputs": dump_inputs(&pool).await,
    }));

    let (processed, inserted) = map_initializer_ranking_graph(&pool, &enabled, 1000)
        .await
        .expect("map initializer ranking graph");
    print(json!({
        "kind": "seed_mapper",
        "processed": processed,
        "inserted": inserted,
        "inputs": dump_inputs(&pool).await,
    }));

    // -- reducer -------------------------------------------------------------
    let (processed, inserted) = reduce_ranking_graph(&pool, &disabled, 1000)
        .await
        .expect("reduce ranking graph (disabled)");
    print(json!({
        "kind": "reducer_gate_off",
        "result": [processed, inserted],
        "ranks": dump_ranks(&pool).await,
    }));

    sqlx::query(
        "UPDATE codeintel_ranking_progress SET reducer_started_at = '2024-05-02T00:00:00Z' WHERE graph_key = 'rbg.2024_05'",
    )
    .execute(&pool)
    .await
    .expect("set reducer started");

    let (processed, inserted) = reduce_ranking_graph(&pool, &enabled, 1000)
        .await
        .expect("reduce ranking graph");
    print(json!({
        "kind": "reducer",
        "processed": processed,
        "inserted": inserted,
        "ranks": dump_ranks(&pool).await,
    }));

    // -- janitors ------------------------------------------------------------
    let mut results = Vec::new();
    for kind in JanitorKind::ALL {
        let (scanned, altered) = run_janitor(&pool, &disabled, kind)
            .await
            .expect("janitor (disabled)");
        results.push(json!({
            "name": janitor_name(kind),
            "result": [scanned, altered],
        }));
    }
    print(json!({
        "kind": "janitors_gate_off",
        "results": results,
        "state": state(&pool).await,
    }));

    for kind in JanitorKind::ALL {
        let (scanned, altered) = run_janitor(&pool, &enabled, kind).await.expect("janitor");
        print(json!({
            "kind": "janitor",
            "name": janitor_name(kind),
            "result": [scanned, altered],
            "state": state(&pool).await,
        }));
    }

    // -- graph-key bump ------------------------------------------------------
    let (prefix, _) = derivative_graph_key_or_bump(&pool)
        .await
        .expect("derivative graph key");
    print(json!({
        "kind": "bump_existing",
        "prefix": prefix,
        "graph_keys": count(&pool, "SELECT COUNT(*) FROM codeintel_ranking_graph_keys").await,
    }));

    sqlx::query("TRUNCATE codeintel_ranking_graph_keys")
        .execute(&pool)
        .await
        .expect("truncate graph keys");

    let (bumped, _) = derivative_graph_key_or_bump(&pool)
        .await
        .expect("derivative graph key (bump)");
    let (again, _) = codeintel_ranking::derivative_graph_key(&pool)
        .await
        .expect("derivative graph key (again)")
        .expect("a graph key row");
    print(json!({
        "kind": "bump_empty",
        "prefix_len": bumped.len() as i64,
        "same_again": again == bumped,
        "graph_keys": count(&pool, "SELECT COUNT(*) FROM codeintel_ranking_graph_keys").await,
    }));
}

/// The baseline routine names, used in the `janitor` section.
fn janitor_name(kind: JanitorKind) -> &'static str {
    match kind {
        JanitorKind::ExportedUploads => "exported-uploads",
        JanitorKind::DeletedExportedUploads => "deleted-uploads",
        JanitorKind::AbandonedExportedUploads => "abandoned-uploads",
        JanitorKind::ProcessedReferences => "processed-references",
        JanitorKind::ProcessedPaths => "processed-paths",
        JanitorKind::RankCounts => "rank-counts",
        JanitorKind::Rank => "rank",
    }
}
