#![forbid(unsafe_code)]

//! M9.8dq differential harness (Rust side): the raw export writes of the
//! ranking store — definitions, references and initial path ranks. Mirrored
//! from
//! `internal/codeintel/ranking/internal/store/store_raw_export_harness_test.go`.
//!
//! `test_ws/p9-go/run_codeintel_ranking_raw_export_diff.sh` applies
//! `codeintel_ranking_raw_export_fixtures.sql` immediately before each side and
//! diffs the canonical `rnkx ` JSON lines.

use serde_json::{json, Value};
use sg_db::codeintel_ranking::{
    insert_definitions_for_ranking, insert_initial_path_ranks, insert_references_for_ranking,
    RankingDefinition,
};
use sqlx::postgres::PgPool;
use sqlx::Row;

fn print(value: Value) {
    println!("rnkx {}", serde_json::to_string(&value).expect("serialize"));
}

fn checksum(first: u8) -> [u8; 16] {
    let mut checksum = [0u8; 16];
    checksum[0] = first;
    checksum
}

async fn count(pool: &PgPool, table: &str) -> i64 {
    sqlx::query_scalar::<_, i64>(&format!("SELECT COUNT(*) FROM {table}"))
        .fetch_one(pool)
        .await
        .expect("count")
}

async fn dump_definitions(pool: &PgPool) -> Vec<Value> {
    let rows = sqlx::query(
        "SELECT id, exported_upload_id, symbol_name, document_path, \
         encode(symbol_checksum, 'hex') AS checksum, graph_key \
         FROM codeintel_ranking_definitions ORDER BY id",
    )
    .fetch_all(pool)
    .await
    .expect("definitions");

    rows.iter()
        .map(|row| {
            json!({
                "id": row.try_get::<i64, _>("id").expect("id"),
                "exported_upload_id": row.try_get::<i32, _>("exported_upload_id").expect("exported_upload_id"),
                "symbol_name": row.try_get::<String, _>("symbol_name").expect("symbol_name"),
                "document_path": row.try_get::<String, _>("document_path").expect("document_path"),
                "checksum": row.try_get::<String, _>("checksum").expect("checksum"),
                "graph_key": row.try_get::<String, _>("graph_key").expect("graph_key"),
            })
        })
        .collect()
}

async fn dump_references(pool: &PgPool) -> Vec<Value> {
    let rows = sqlx::query(
        "SELECT id, exported_upload_id, symbol_names::text AS symbol_names, \
         (SELECT string_agg(encode(checksum, 'hex'), ',' ORDER BY ordinal) \
          FROM unnest(symbol_checksums) WITH ORDINALITY AS t(checksum, ordinal)) AS checksums, \
         graph_key \
         FROM codeintel_ranking_references ORDER BY id",
    )
    .fetch_all(pool)
    .await
    .expect("references");

    rows.iter()
        .map(|row| {
            json!({
                "id": row.try_get::<i64, _>("id").expect("id"),
                "exported_upload_id": row.try_get::<i32, _>("exported_upload_id").expect("exported_upload_id"),
                "symbol_names": row.try_get::<String, _>("symbol_names").expect("symbol_names"),
                "checksums": row.try_get::<Option<String>, _>("checksums").expect("checksums").unwrap_or_default(),
                "graph_key": row.try_get::<String, _>("graph_key").expect("graph_key"),
            })
        })
        .collect()
}

async fn dump_initial_path_ranks(pool: &PgPool) -> Vec<Value> {
    let rows = sqlx::query(
        "SELECT id, exported_upload_id, document_path, document_paths::text AS document_paths, graph_key \
         FROM codeintel_initial_path_ranks ORDER BY id",
    )
    .fetch_all(pool)
    .await
    .expect("initial path ranks");

    rows.iter()
        .map(|row| {
            json!({
                "id": row.try_get::<i64, _>("id").expect("id"),
                "exported_upload_id": row.try_get::<i32, _>("exported_upload_id").expect("exported_upload_id"),
                "document_path": row.try_get::<String, _>("document_path").expect("document_path"),
                "document_paths": row.try_get::<String, _>("document_paths").expect("document_paths"),
                "graph_key": row.try_get::<String, _>("graph_key").expect("graph_key"),
            })
        })
        .collect()
}

#[tokio::test]
async fn ranking_raw_export_harness() {
    if std::env::var("SG_CODEINTEL_RANKING_RAW_EXPORT_DIFF").as_deref() != Ok("1") {
        return;
    }
    let Ok(dsn) = std::env::var("SG_CODEINTEL_RANKING_RAW_EXPORT_TEST_DSN") else {
        return;
    };

    let pool = PgPool::connect(&dsn).await.expect("connect database");
    const GRAPH_KEY: &str = "rexp";

    // -- definitions --------------------------------------------------------
    let definitions = vec![
        RankingDefinition {
            exported_upload_id: 311,
            symbol_checksum: checksum(1),
            document_path: "a.go".into(),
        },
        RankingDefinition {
            exported_upload_id: 311,
            symbol_checksum: checksum(2),
            document_path: "b/c.go".into(),
        },
        RankingDefinition {
            exported_upload_id: 312,
            symbol_checksum: checksum(1),
            document_path: "a.go".into(),
        },
    ];
    insert_definitions_for_ranking(&pool, GRAPH_KEY, &definitions)
        .await
        .expect("insert definitions");
    print(json!({
        "kind": "definitions_first",
        "definitions": dump_definitions(&pool).await,
    }));

    insert_definitions_for_ranking(&pool, GRAPH_KEY, &[])
        .await
        .expect("empty insert definitions");
    print(json!({
        "kind": "definitions_empty",
        "count": count(&pool, "codeintel_ranking_definitions").await,
    }));

    insert_definitions_for_ranking(
        &pool,
        GRAPH_KEY,
        &[RankingDefinition {
            exported_upload_id: 312,
            symbol_checksum: checksum(3),
            document_path: "b/c.go".into(),
        }],
    )
    .await
    .expect("second insert definitions");
    print(json!({
        "kind": "definitions_second",
        "definitions": dump_definitions(&pool).await,
    }));

    // -- references ---------------------------------------------------------
    let checksums: Vec<[u8; 16]> = (1..=5).map(checksum).collect();
    insert_references_for_ranking(&pool, GRAPH_KEY, 2, 311, &checksums)
        .await
        .expect("insert references");
    print(json!({
        "kind": "references_first",
        "references": dump_references(&pool).await,
    }));

    insert_references_for_ranking(&pool, GRAPH_KEY, 2, 311, &[])
        .await
        .expect("empty insert references");
    print(json!({
        "kind": "references_empty",
        "count": count(&pool, "codeintel_ranking_references").await,
    }));

    insert_references_for_ranking(&pool, GRAPH_KEY, 10, 312, &[checksum(6), checksum(7)])
        .await
        .expect("second insert references");
    print(json!({
        "kind": "references_second",
        "references": dump_references(&pool).await,
    }));

    insert_references_for_ranking(&pool, GRAPH_KEY, 0, 312, &[checksum(8), checksum(9)])
        .await
        .expect("unbatched insert references");
    print(json!({
        "kind": "references_batch_zero",
        "references": dump_references(&pool).await,
    }));

    // -- initial path ranks -------------------------------------------------
    insert_initial_path_ranks(
        &pool,
        312,
        &["x.go".to_string(), "y.go".to_string(), "z.go".to_string()],
        2,
        GRAPH_KEY,
    )
    .await
    .expect("insert initial path ranks");
    print(json!({
        "kind": "paths_first",
        "initial_path_ranks": dump_initial_path_ranks(&pool).await,
        "definitions": dump_definitions(&pool).await,
    }));

    insert_initial_path_ranks(&pool, 312, &[], 2, GRAPH_KEY)
        .await
        .expect("empty insert initial path ranks");
    print(json!({
        "kind": "paths_empty",
        "count": count(&pool, "codeintel_initial_path_ranks").await,
        "definition_count": count(&pool, "codeintel_ranking_definitions").await,
    }));
}
