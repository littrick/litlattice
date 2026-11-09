#![forbid(unsafe_code)]

//! M9.8dp differential harness (Rust side): the mapper/reducer half of the
//! ranking store, mirrored from
//! `internal/codeintel/ranking/internal/store/store_mapper_harness_test.go`.
//!
//! `test_ws/p9-go/run_codeintel_ranking_mapper_diff.sh` applies
//! `codeintel_ranking_mapper_fixtures.sql` immediately before each side and
//! diffs the canonical `rnkm ` JSON lines. Timestamps written from the
//! database clock are printed as `set`/`null`; fixture timestamps are printed
//! verbatim.

use serde_json::{json, Value};
use sg_db::codeintel_ranking::{
    insert_initial_path_counts, insert_path_count_inputs, insert_path_ranks, vacuum_stale_graphs,
    vacuum_stale_processed_paths, vacuum_stale_processed_references, vacuum_stale_ranks,
};
use sg_db::OffsetDateTime;
use sqlx::postgres::PgPool;
use sqlx::Row;

fn print(value: Value) {
    println!("rnkm {}", serde_json::to_string(&value).expect("serialize"));
}

fn marker(value: Option<OffsetDateTime>) -> &'static str {
    if value.is_some() {
        "set"
    } else {
        "null"
    }
}

fn integer(value: Option<i64>) -> Value {
    match value {
        Some(value) => json!(value),
        None => Value::Null,
    }
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
            json!({
                "graph_key": row.try_get::<String, _>("graph_key").expect("graph_key"),
                "definition_id": integer(row.try_get::<Option<i64>, _>("definition_id").expect("definition_id")),
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

async fn dump_reference_cursor(pool: &PgPool, derivative: &str) -> Value {
    let row = sqlx::query(
        "SELECT reference_cursor_export_id, reference_cursor_export_deleted_at, mapper_completed_at, COALESCE(num_reference_records_processed, 0) AS processed FROM codeintel_ranking_progress WHERE graph_key = $1",
    )
    .bind(derivative)
    .fetch_one(pool)
    .await
    .expect("reference cursor");

    json!({
        "cursor_id": integer(row.try_get::<Option<i32>, _>("reference_cursor_export_id").expect("cursor_id").map(i64::from)),
        "cursor_deleted": marker(row.try_get::<Option<OffsetDateTime>, _>("reference_cursor_export_deleted_at").expect("cursor_deleted")),
        "mapper_done": marker(row.try_get::<Option<OffsetDateTime>, _>("mapper_completed_at").expect("mapper_completed_at")),
        "processed": row.try_get::<i32, _>("processed").expect("processed"),
    })
}

async fn query_ids(pool: &PgPool, query: &str) -> Vec<i64> {
    sqlx::query_scalar::<_, i64>(query)
        .fetch_all(pool)
        .await
        .expect("query ids")
}

#[tokio::test]
async fn ranking_mapper_harness() {
    if std::env::var("SG_CODEINTEL_RANKING_MAPPER_DIFF").as_deref() != Ok("1") {
        return;
    }
    let Ok(dsn) = std::env::var("SG_CODEINTEL_RANKING_MAPPER_TEST_DSN") else {
        return;
    };

    let pool = PgPool::connect(&dsn).await.expect("connect database");
    const DERIVATIVE: &str = "rmap.2024_05";

    // -- reference mapper ---------------------------------------------------
    let (processed, inserted) = insert_path_count_inputs(&pool, DERIVATIVE, 1000)
        .await
        .expect("insert path count inputs");
    print(json!({
        "kind": "refs_first",
        "processed": processed,
        "inserted": inserted,
        "progress": dump_reference_cursor(&pool, DERIVATIVE).await,
        "inputs": dump_inputs(&pool).await,
        "processed_ref_ids": query_ids(&pool, "SELECT codeintel_ranking_reference_id::bigint FROM codeintel_ranking_references_processed ORDER BY id").await,
    }));

    let (processed, inserted) = insert_path_count_inputs(&pool, DERIVATIVE, 1000)
        .await
        .expect("second insert path count inputs");
    print(json!({
        "kind": "refs_second",
        "processed": processed,
        "inserted": inserted,
        "progress": dump_reference_cursor(&pool, DERIVATIVE).await,
    }));

    assert!(
        insert_path_count_inputs(&pool, "badkey", 1000)
            .await
            .is_err(),
        "insert path count inputs with a one-segment key must fail"
    );
    print(json!({"kind": "refs_bad_key", "errored": true}));

    // -- initial path mapper ------------------------------------------------
    let (processed, inserted) = insert_initial_path_counts(&pool, DERIVATIVE, 1000)
        .await
        .expect("insert initial path counts");
    let row = sqlx::query(
        "SELECT path_cursor_export_id, path_cursor_deleted_export_at, seed_mapper_completed_at, COALESCE(num_path_records_processed, 0) AS processed FROM codeintel_ranking_progress WHERE graph_key = $1",
    )
    .bind(DERIVATIVE)
    .fetch_one(&pool)
    .await
    .expect("path cursor");
    print(json!({
        "kind": "paths_first",
        "processed": processed,
        "inserted": inserted,
        "progress": {
            "cursor_id": integer(row.try_get::<Option<i32>, _>("path_cursor_export_id").expect("cursor_id").map(i64::from)),
            "cursor_deleted": marker(row.try_get::<Option<OffsetDateTime>, _>("path_cursor_deleted_export_at").expect("cursor_deleted")),
            "seed_done": marker(row.try_get::<Option<OffsetDateTime>, _>("seed_mapper_completed_at").expect("seed_completed")),
            "processed": row.try_get::<i32, _>("processed").expect("processed"),
        },
        "inputs": dump_inputs(&pool).await,
    }));

    let (processed, inserted) = insert_initial_path_counts(&pool, DERIVATIVE, 1000)
        .await
        .expect("second insert initial path counts");
    print(json!({
        "kind": "paths_second",
        "processed": processed,
        "inserted": inserted,
    }));

    // -- stale processed-record cleanup -------------------------------------
    let deleted = vacuum_stale_processed_references(&pool, DERIVATIVE, 100)
        .await
        .expect("vacuum stale processed references");
    print(json!({
        "kind": "vacuum_refs",
        "deleted": deleted,
        "remaining": query_ids(&pool, "SELECT id FROM codeintel_ranking_references_processed ORDER BY id").await,
    }));

    let deleted = vacuum_stale_processed_paths(&pool, DERIVATIVE, 100)
        .await
        .expect("vacuum stale processed paths");
    print(json!({
        "kind": "vacuum_paths",
        "deleted": deleted,
        "remaining": query_ids(&pool, "SELECT id FROM codeintel_initial_path_ranks_processed ORDER BY id").await,
    }));

    let deleted = vacuum_stale_graphs(&pool, DERIVATIVE, 100)
        .await
        .expect("vacuum stale graphs");
    print(json!({
        "kind": "vacuum_graphs",
        "deleted": deleted,
        "remaining": query_ids(&pool, "SELECT definition_id FROM codeintel_ranking_path_counts_inputs WHERE definition_id IS NOT NULL ORDER BY definition_id").await,
    }));

    // -- reducer ------------------------------------------------------------
    sqlx::query("UPDATE codeintel_ranking_progress SET reducer_started_at = '2024-05-02T00:00:00Z' WHERE graph_key = $1")
        .bind(DERIVATIVE)
        .execute(&pool)
        .await
        .expect("set reducer started");

    let (processed, inserted) = insert_path_ranks(&pool, DERIVATIVE, 1000)
        .await
        .expect("insert path ranks");
    let row = sqlx::query(
        "SELECT reducer_completed_at, COALESCE(num_count_records_processed, 0) AS num_processed FROM codeintel_ranking_progress WHERE graph_key = $1",
    )
    .bind(DERIVATIVE)
    .fetch_one(&pool)
    .await
    .expect("reducer progress");
    print(json!({
        "kind": "reducer_first",
        "processed": processed,
        "inserted": inserted,
        "progress": {
            "reducer_done": marker(row.try_get::<Option<OffsetDateTime>, _>("reducer_completed_at").expect("reducer_completed_at")),
            "num_processed": row.try_get::<i32, _>("num_processed").expect("num_processed"),
        },
        "ranks": dump_ranks(&pool).await,
    }));

    let (processed, inserted) = insert_path_ranks(&pool, DERIVATIVE, 1000)
        .await
        .expect("second insert path ranks");
    let row = sqlx::query(
        "SELECT reducer_completed_at, COALESCE(num_count_records_processed, 0) AS num_processed FROM codeintel_ranking_progress WHERE graph_key = $1",
    )
    .bind(DERIVATIVE)
    .fetch_one(&pool)
    .await
    .expect("reducer progress");
    print(json!({
        "kind": "reducer_second",
        "processed": processed,
        "inserted": inserted,
        "progress": {
            "reducer_done": marker(row.try_get::<Option<OffsetDateTime>, _>("reducer_completed_at").expect("reducer_completed_at")),
            "num_processed": row.try_get::<i32, _>("num_processed").expect("num_processed"),
        },
    }));

    assert!(
        insert_path_ranks(&pool, "badkey", 1000).await.is_err(),
        "insert path ranks with a one-segment key must fail"
    );
    print(json!({"kind": "reducer_bad_key", "errored": true}));

    // -- stale ranks cleanup ------------------------------------------------
    let (scanned, deleted) = vacuum_stale_ranks(&pool, DERIVATIVE)
        .await
        .expect("vacuum stale ranks");
    print(json!({
        "kind": "vacuum_ranks",
        "scanned": scanned,
        "deleted": deleted,
        "remaining": dump_ranks(&pool).await,
    }));
}
