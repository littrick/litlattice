#![forbid(unsafe_code)]

//! M9.8do differential harness (Rust side): the non-mapper/reducer half of
//! the ranking store, mirrored from
//! `internal/codeintel/ranking/internal/store/store_harness_test.go`.
//!
//! `test_ws/p9-go/run_codeintel_ranking_store_diff.sh` applies
//! `codeintel_ranking_store_fixtures.sql` immediately before each side and
//! diffs the canonical `rnk ` JSON lines. Timestamps written from the process
//! or database clock are printed as `set`/`null`; fixture timestamps are
//! printed verbatim.

use serde_json::{json, Value};
use sg_db::codeintel_ranking::{
    bump_derivative_graph_key, coordinate, coverage_counts, delete_ranking_progress,
    derivative_graph_key, document_ranks, get_uploads_for_ranking, last_updated_at,
    reference_count_statistics, soft_delete_stale_exported_uploads, star_rank, summaries,
    vacuum_abandoned_exported_uploads, vacuum_deleted_exported_uploads, Progress,
};
use sg_db::OffsetDateTime;
use sqlx::postgres::PgPool;
use sqlx::Row;
use time::format_description::well_known::Rfc3339;

fn print(value: Value) {
    println!("rnk {}", serde_json::to_string(&value).expect("serialize"));
}

fn format_time(value: OffsetDateTime) -> String {
    value.format(&Rfc3339).expect("format time")
}

fn format_optional(value: Option<OffsetDateTime>) -> Option<String> {
    value.map(format_time)
}

fn marker(value: Option<OffsetDateTime>) -> &'static str {
    if value.is_some() {
        "set"
    } else {
        "null"
    }
}

fn progress(value: &Progress) -> Value {
    json!({
        "started_at": format_time(value.started_at),
        "completed_at": format_optional(value.completed_at),
        "processed": value.processed,
        "total": value.total,
    })
}

fn is_uuid_v4(value: &str) -> bool {
    let bytes = value.as_bytes();
    if bytes.len() != 36 {
        return false;
    }
    let hyphens = [8, 13, 18, 23];
    if bytes
        .iter()
        .enumerate()
        .any(|(index, byte)| hyphens.contains(&index) != (*byte == b'-'))
    {
        return false;
    }
    if !bytes
        .iter()
        .enumerate()
        .all(|(index, byte)| hyphens.contains(&index) || byte.is_ascii_hexdigit())
    {
        return false;
    }
    bytes[14] == b'4' && matches!(bytes[19], b'8' | b'9' | b'a' | b'b')
}

async fn query_int(pool: &PgPool, query: &str) -> i64 {
    sqlx::query_scalar::<_, i64>(query)
        .fetch_one(pool)
        .await
        .expect("query int")
}

async fn exec(pool: &PgPool, query: &str) {
    sqlx::query(query).execute(pool).await.expect("exec");
}

async fn exec_with_key(pool: &PgPool, query: &str, key: &str) {
    sqlx::query(query)
        .bind(key)
        .execute(pool)
        .await
        .expect("exec with key");
}

#[tokio::test]
async fn ranking_store_harness() {
    if std::env::var("SG_CODEINTEL_RANKING_STORE_DIFF").as_deref() != Ok("1") {
        return;
    }
    let Ok(dsn) = std::env::var("SG_CODEINTEL_RANKING_STORE_TEST_DSN") else {
        return;
    };

    let pool = PgPool::connect(&dsn).await.expect("connect database");

    let deriv_key = match derivative_graph_key(&pool)
        .await
        .expect("derivative graph key")
    {
        Some((key, created_at)) => (key, Some(created_at)),
        None => {
            bump_derivative_graph_key(&pool).await.expect("bump");
            let (key, created_at) = derivative_graph_key(&pool)
                .await
                .expect("derivative graph key after bump")
                .expect("graph key after bump");
            (key, Some(created_at))
        }
    };
    print(json!({
        "kind": "graph_key_empty",
        "has_key": !deriv_key.0.is_empty(),
        "key_is_uuid_v4": is_uuid_v4(&deriv_key.0),
        "created_at_set": deriv_key.1.is_some(),
        "count": query_int(&pool, "SELECT COUNT(*) FROM codeintel_ranking_graph_keys").await,
    }));

    for i in 0..12 {
        sqlx::query(
            "INSERT INTO codeintel_ranking_graph_keys (graph_key, created_at) VALUES ($1, $2)",
        )
        .bind(format!("fix-{i:02}"))
        .bind(OffsetDateTime::from_unix_timestamp(1_577_836_800 + i * 60).expect("fixture time"))
        .execute(&pool)
        .await
        .expect("insert graph key");
    }
    bump_derivative_graph_key(&pool)
        .await
        .expect("bump derivative graph key");
    let oldest_present = sqlx::query_scalar::<_, bool>(
        "SELECT EXISTS (SELECT 1 FROM codeintel_ranking_graph_keys WHERE graph_key = 'fix-00')",
    )
    .fetch_one(&pool)
    .await
    .expect("oldest query");
    print(json!({
        "kind": "bump_prune",
        "count": query_int(&pool, "SELECT COUNT(*) FROM codeintel_ranking_graph_keys").await,
        "oldest_present": oldest_present,
        "second_present": query_int(&pool, "SELECT COUNT(*) FROM codeintel_ranking_graph_keys WHERE graph_key IN ('fix-01', 'fix-02')").await,
    }));

    // -- progress coordination ---------------------------------------------
    const G3_DERIVATIVE: &str = "g3.2024_03";
    coordinate(&pool, G3_DERIVATIVE, OffsetDateTime::now_utc())
        .await
        .expect("coordinate start");
    exec_with_key(
        &pool,
        "UPDATE codeintel_ranking_progress SET mappers_started_at = '2024-03-01T00:00:00Z' WHERE graph_key = $1",
        G3_DERIVATIVE,
    )
    .await;

    let row = sqlx::query(
        "SELECT graph_key, max_export_id, mappers_started_at, mapper_completed_at, seed_mapper_completed_at, reducer_started_at, num_path_records_total, num_reference_records_total FROM codeintel_ranking_progress WHERE graph_key = $1",
    )
    .bind(G3_DERIVATIVE)
    .fetch_one(&pool)
    .await
    .expect("g3 progress");
    let mappers_started: OffsetDateTime = row
        .try_get("mappers_started_at")
        .expect("mappers_started_at");
    let mapper_completed: Option<OffsetDateTime> = row
        .try_get("mapper_completed_at")
        .expect("mapper_completed_at");
    let seed_completed: Option<OffsetDateTime> =
        row.try_get("seed_mapper_completed_at").expect("seed");
    let reducer_started: Option<OffsetDateTime> = row
        .try_get("reducer_started_at")
        .expect("reducer_started_at");
    print(json!({
        "kind": "coordinate_start",
        "graph_key": row.try_get::<String, _>("graph_key").expect("graph_key"),
        "max_export_id": row.try_get::<i64, _>("max_export_id").expect("max_export_id"),
        "mappers_started": marker(Some(mappers_started)),
        "mapper_completed": marker(mapper_completed),
        "seed_mapper_completed": marker(seed_completed),
        "reducer_started": marker(reducer_started),
        "num_path_records_total": row.try_get::<Option<i32>, _>("num_path_records_total").expect("path total").unwrap_or_default(),
        "num_reference_records_total": row.try_get::<Option<i32>, _>("num_reference_records_total").expect("reference total").unwrap_or_default(),
        "remaining_p_rows": query_int(&pool, "SELECT COUNT(*) FROM codeintel_ranking_progress WHERE graph_key LIKE 'p%'").await,
        "p00_present": query_int(&pool, "SELECT COUNT(*) FROM codeintel_ranking_progress WHERE graph_key IN ('p00', 'p01')").await,
    }));

    exec_with_key(
        &pool,
        "UPDATE codeintel_ranking_progress SET mapper_completed_at = '2024-03-02T00:00:00Z', seed_mapper_completed_at = '2024-03-02T00:00:00Z' WHERE graph_key = $1",
        G3_DERIVATIVE,
    )
    .await;
    coordinate(&pool, G3_DERIVATIVE, OffsetDateTime::now_utc())
        .await
        .expect("coordinate reducer");
    exec_with_key(
        &pool,
        "UPDATE codeintel_ranking_progress SET reducer_started_at = '2024-03-03T00:00:00Z' WHERE graph_key = $1",
        G3_DERIVATIVE,
    )
    .await;

    let row = sqlx::query(
        "SELECT reducer_started_at, num_count_records_total FROM codeintel_ranking_progress WHERE graph_key = $1",
    )
    .bind(G3_DERIVATIVE)
    .fetch_one(&pool)
    .await
    .expect("g3 reducer");
    print(json!({
        "kind": "coordinate_reducer",
        "reducer_started": marker(row.try_get::<Option<OffsetDateTime>, _>("reducer_started_at").expect("reducer_started_at")),
        "num_count_records_total": row.try_get::<Option<i32>, _>("num_count_records_total").expect("count total").unwrap_or_default(),
    }));

    assert!(
        coordinate(&pool, "badkey", OffsetDateTime::now_utc())
            .await
            .is_err(),
        "coordinate with a one-segment key must fail"
    );
    print(json!({"kind": "coordinate_bad_key", "errored": true}));

    exec(
        &pool,
        "DELETE FROM codeintel_ranking_progress WHERE graph_key LIKE 'p%'",
    )
    .await;

    // -- summaries ----------------------------------------------------------
    for summary in summaries(&pool).await.expect("summaries") {
        print(json!({
            "kind": "summary",
            "graph_key": summary.graph_key,
            "visible_to_zoekt": summary.visible_to_zoekt,
            "path_mapper": progress(&summary.path_mapper_progress),
            "reference_mapper": progress(&summary.reference_mapper_progress),
            "reducer": summary.reducer_progress.as_ref().map(progress),
        }));
    }

    delete_ranking_progress(&pool, "g1")
        .await
        .expect("delete ranking progress");
    print(json!({
        "kind": "delete_progress",
        "still_present": query_int(&pool, "SELECT COUNT(*) FROM codeintel_ranking_progress WHERE graph_key = 'g1'").await,
        "count": query_int(&pool, "SELECT COUNT(*) FROM codeintel_ranking_progress").await,
    }));

    // -- exported uploads ---------------------------------------------------
    let uploads = get_uploads_for_ranking(&pool, "gexp", 100)
        .await
        .expect("get uploads for ranking");
    let printed: Vec<Value> = uploads
        .iter()
        .map(|upload| {
            json!({
                "upload_id": upload.upload_id,
                "exported_upload_id": upload.exported_upload_id,
                "repo": &upload.repo,
                "repo_id": upload.repo_id,
                "root": &upload.root,
            })
        })
        .collect();
    print(json!({"kind": "uploads_first", "uploads": printed}));

    let uploads = get_uploads_for_ranking(&pool, "gexp", 100)
        .await
        .expect("second get uploads");
    print(json!({"kind": "uploads_second", "count": uploads.len()}));

    let uploads = get_uploads_for_ranking(&pool, "gexp2", 1)
        .await
        .expect("batch-one get uploads");
    let upload_ids: Vec<i32> = uploads.iter().map(|upload| upload.upload_id).collect();
    print(json!({"kind": "uploads_batch_one", "count": uploads.len(), "upload_ids": upload_ids}));

    // -- soft delete + hard delete -----------------------------------------
    let (scanned, deleted) = soft_delete_stale_exported_uploads(&pool, "soft")
        .await
        .expect("soft delete");
    let rows = sqlx::query(
        "SELECT id, deleted_at FROM codeintel_ranking_exports WHERE graph_key = 'soft' ORDER BY id",
    )
    .fetch_all(&pool)
    .await
    .expect("soft exports");
    let states: Vec<Value> = rows
        .iter()
        .map(|row| {
            json!({
                "id": row.try_get::<i32, _>("id").expect("id"),
                "deleted_at": marker(row.try_get::<Option<OffsetDateTime>, _>("deleted_at").expect("deleted_at")),
            })
        })
        .collect();
    print(
        json!({"kind": "soft_delete", "scanned": scanned, "deleted": deleted, "exports": states}),
    );

    let vacuumed = vacuum_deleted_exported_uploads(&pool, "softdel.2024_04")
        .await
        .expect("vacuum deleted");
    let remaining_ids: Vec<i32> = sqlx::query_scalar::<_, i32>(
        "SELECT id FROM codeintel_ranking_exports WHERE graph_key = 'softdel' ORDER BY id",
    )
    .fetch_all(&pool)
    .await
    .expect("remaining ids");
    print(json!({"kind": "vacuum_deleted", "deleted": vacuumed, "remaining_ids": remaining_ids}));

    // -- retrieval ----------------------------------------------------------
    let rank = star_rank(&pool, "github.com/a/repo")
        .await
        .expect("star rank");
    print(json!({"kind": "star_rank", "repo": "github.com/a/repo", "rank": format!("{rank:.6}")}));

    let (ranks_map, ok) = document_ranks(&pool, "github.com/a/repo")
        .await
        .expect("document ranks");
    let ranks: Vec<Value> = ranks_map
        .iter()
        .map(|(path, rank)| json!([path, format!("{rank:.6}")]))
        .collect();
    print(json!({"kind": "document_ranks", "repo": "github.com/a/repo", "ok": ok, "ranks": ranks}));

    let (missing, ok) = document_ranks(&pool, "github.com/e/noranks")
        .await
        .expect("missing document ranks");
    print(json!({"kind": "document_ranks_missing", "ok": ok, "count": missing.len()}));

    let logmean = reference_count_statistics(&pool)
        .await
        .expect("reference count statistics");
    print(json!({"kind": "reference_count_statistics", "logmean": format!("{logmean:.6}")}));

    let coverage = coverage_counts(&pool, "cov")
        .await
        .expect("coverage counts");
    print(json!({
        "kind": "coverage_counts",
        "targets": coverage.num_target_indexes,
        "exported": coverage.num_exported_indexes,
        "unindexed": coverage.num_repositories_without_current_ranks,
    }));

    let updated = last_updated_at(&pool, &[1, 2, 5])
        .await
        .expect("last updated at");
    let entries: Vec<Value> = updated
        .iter()
        .map(|(repo_id, at)| json!([*repo_id, format_time(*at)]))
        .collect();
    print(json!({"kind": "last_updated_at", "entries": entries}));

    // -- abandoned exports (deletes every other graph key) ------------------
    let abandoned = vacuum_abandoned_exported_uploads(&pool, "gexp", 100)
        .await
        .expect("vacuum abandoned");
    let remaining_keys: Vec<String> = sqlx::query_scalar::<_, String>(
        "SELECT DISTINCT graph_key FROM codeintel_ranking_exports ORDER BY graph_key",
    )
    .fetch_all(&pool)
    .await
    .expect("remaining keys");
    print(json!({
        "kind": "vacuum_abandoned",
        "deleted": abandoned,
        "remaining_keys": remaining_keys,
    }));
}
