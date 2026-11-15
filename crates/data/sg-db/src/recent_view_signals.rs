#![forbid(unsafe_code)]

//! `internal/database/recent_view_signal.go` +
//! `internal/database/event_logs_scrape_state_own.go`: the read/write half of
//! the Own `recent-views` indexer.
//!
//! Ported statements:
//!
//! * [`list_view_blob_events`]: `eventLogStore.ListAll` restricted to
//!   `name = 'ViewBlob'`, `AfterID = bookmark`, `LIMIT 5000`. Like the
//!   baseline, a positive `AfterID` orders ascending; `0` orders descending.
//! * [`get_bookmark`] / [`update_bookmark`]: the *own* scrape-state table
//!   (`event_logs_scrape_state_own`), keyed by the
//!   `own_signal_configurations.name` row. The generate branch uses the same
//!   `INSERT ... SELECT MAX(id), (SELECT id ...) FROM event_logs RETURNING`
//!   as the baseline, so an empty `event_logs` table raises the same NOT NULL
//!   violation.
//! * [`load_excluded_patterns`]: the `excluded_repo_patterns` of the first
//!   `recent-views` configuration.
//! * [`insert_paths`]: `recentViewSignalStore.InsertPaths` — ancestor counts
//!   summed along `repo_paths.parent_id`, then batched UPSERTs into
//!   `own_aggregate_recent_view`.
//!
//! No tables, columns or Redis keys are added.

use std::collections::{BTreeMap, HashMap};

use sqlx::postgres::{PgPool, PgRow};
use sqlx::{QueryBuilder, Row};

/// One `ViewBlob` event as `RecentViewSignalStore.BuildAggregateFromEvents`
/// consumes it. `public_argument` is the `jsonb` text representation, exactly
/// what the baseline scans into `json.RawMessage`.
#[derive(Debug, Clone, PartialEq)]
pub struct ViewBlobEvent {
    pub id: i64,
    pub user_id: i32,
    pub public_argument: String,
}

/// `eventLogStore.ListAll` for the `ViewBlob` signal.
pub async fn list_view_blob_events(
    pool: &PgPool,
    after_id: i32,
    limit: i64,
) -> Result<Vec<ViewBlobEvent>, String> {
    // `ListAll` builds `WHERE <conds> ORDER BY id <dir> <limit>`; a positive
    // AfterID switches the direction to ASC.
    let (filter, order) = if after_id > 0 {
        ("AND id > $1 AND name = 'ViewBlob'", "ASC")
    } else {
        ("AND name = 'ViewBlob'", "DESC")
    };
    let sql = format!(
        "SELECT id, user_id, public_argument::text AS public_argument \
         FROM event_logs WHERE TRUE {filter} ORDER BY id {order} LIMIT $2",
    );
    let rows = sqlx::query(&sql)
        .bind(after_id)
        .bind(limit)
        .fetch_all(pool)
        .await
        .map_err(|err| err.to_string())?;
    rows.iter().map(scan_view_blob_event).collect()
}

fn scan_view_blob_event(row: &PgRow) -> Result<ViewBlobEvent, String> {
    Ok(ViewBlobEvent {
        id: row.try_get("id").map_err(|err| err.to_string())?,
        user_id: row.try_get("user_id").map_err(|err| err.to_string())?,
        public_argument: row
            .try_get("public_argument")
            .map_err(|err| err.to_string())?,
    })
}

/// `eventLogsScrapeStateStore.GetBookmark` for the own table.
pub async fn get_bookmark(pool: &PgPool, signal_name: &str) -> Result<i32, String> {
    let existing: Option<i32> = sqlx::query_scalar(
        "SELECT bookmark_id FROM event_logs_scrape_state_own \
         WHERE job_type = (SELECT id FROM own_signal_configurations WHERE name = $1) \
         ORDER BY id LIMIT 1",
    )
    .bind(signal_name)
    .fetch_optional(pool)
    .await
    .map_err(|err| err.to_string())?;

    if let Some(bookmark) = existing {
        return Ok(bookmark);
    }

    sqlx::query_scalar(
        "INSERT INTO event_logs_scrape_state_own (bookmark_id, job_type) \
         SELECT MAX(id), (SELECT id FROM own_signal_configurations WHERE name = $1) \
         FROM event_logs RETURNING bookmark_id",
    )
    .bind(signal_name)
    .fetch_one(pool)
    .await
    .map_err(|err| err.to_string())
}

/// `eventLogsScrapeStateStore.UpdateBookmark` for the own table.
pub async fn update_bookmark(pool: &PgPool, value: i32, signal_name: &str) -> Result<(), String> {
    sqlx::query(
        "UPDATE event_logs_scrape_state_own SET bookmark_id = $1 \
         WHERE id = (SELECT id FROM event_logs_scrape_state_own \
                     WHERE job_type = (SELECT id FROM own_signal_configurations WHERE name = $2) \
                     ORDER BY id LIMIT 1)",
    )
    .bind(value)
    .bind(signal_name)
    .execute(pool)
    .await
    .map(|_| ())
    .map_err(|err| err.to_string())
}

/// `OwnSignalConfigurations().LoadConfigurations({Name: "recent-views"})` ->
/// `regexifyPatterns(configurations[0].ExcludedRepoPatterns)`. The `%` -> `.*`
/// conversion is left to the caller (it needs the regex crate).
pub async fn load_excluded_patterns(pool: &PgPool) -> Result<Vec<String>, String> {
    let patterns: Option<Vec<String>> = sqlx::query_scalar(
        "SELECT COALESCE(excluded_repo_patterns, '{}'::text[]) \
         FROM own_signal_configurations \
         WHERE name = 'recent-views' ORDER BY id LIMIT 1",
    )
    .fetch_optional(pool)
    .await
    .map_err(|err| err.to_string())?;
    Ok(patterns.unwrap_or_default())
}

/// `recentViewSignalStore.InsertPaths`: the production hard limit is 5000
/// entries per bulk insert.
pub async fn insert_paths(
    pool: &PgPool,
    user_id: i32,
    repo_path_id_to_count: &BTreeMap<i32, i32>,
) -> Result<(), String> {
    insert_paths_with_batch_size(pool, user_id, repo_path_id_to_count, 5000).await
}

/// `recentViewSignalStore.insertPaths`: find the parent chain for every leaf,
/// augment each ancestor with the leaf's count, then issue one
/// `max_batch_size`-row batch per chunk of an additive
/// `ON CONFLICT ... DO UPDATE` insert. `max_batch_size` is the baseline's
/// unexported parameter, exposed so the differential harness can force
/// multiple batches without seeding 5001 paths.
pub async fn insert_paths_with_batch_size(
    pool: &PgPool,
    user_id: i32,
    repo_path_id_to_count: &BTreeMap<i32, i32>,
    max_batch_size: usize,
) -> Result<(), String> {
    let batch_size = repo_path_id_to_count.len().min(max_batch_size);
    if batch_size == 0 {
        return Ok(());
    }

    // `findAncestorPathsFmtstr`: id/parent_id of the given ids and every
    // ancestor, excluding the NULL-parent row (the repo root).
    let path_ids: Vec<i32> = repo_path_id_to_count.keys().copied().collect();
    let rows = sqlx::query(
        "WITH RECURSIVE ancestor_paths AS ( \
             SELECT id, parent_id FROM repo_paths WHERE id = ANY($1::int[]) \
             UNION ALL \
             SELECT p.id, p.parent_id FROM repo_paths p \
             JOIN ancestor_paths ap ON p.id = ap.parent_id \
         ) \
         SELECT id, parent_id FROM ancestor_paths WHERE parent_id IS NOT NULL",
    )
    .bind(&path_ids)
    .fetch_all(pool)
    .await
    .map_err(|err| err.to_string())?;

    let mut parent_ids: HashMap<i32, i32> = HashMap::new();
    for row in &rows {
        let id: i32 = row.try_get("id").map_err(|err| err.to_string())?;
        let parent_id: i32 = row.try_get("parent_id").map_err(|err| err.to_string())?;
        parent_ids.insert(id, parent_id);
    }

    // Augment ancestor counts. `parent_ids.get` missing means the chain ended
    // at a row whose parent is NULL (or id 0), exactly like Go's zero value.
    let mut augmented: BTreeMap<i32, i32> = BTreeMap::new();
    for (leaf_id, count) in repo_path_id_to_count {
        let mut path_id = *leaf_id;
        while path_id != 0 {
            *augmented.entry(path_id).or_insert(0) += *count;
            path_id = parent_ids.get(&path_id).copied().unwrap_or(0);
        }
    }

    // `bulkInsertRecentViewSignalsFmtstr`, in batches of `batch_size`.
    let entries: Vec<(i32, i32)> = augmented.into_iter().collect();
    for chunk in entries.chunks(batch_size) {
        let mut builder: QueryBuilder<'_, sqlx::Postgres> = QueryBuilder::new(
            "INSERT INTO own_aggregate_recent_view(viewer_id, viewed_file_path_id, views_count) ",
        );
        builder.push_values(chunk, |mut builder, (path_id, count)| {
            builder.push_bind(user_id).push_bind(*path_id).push_bind(*count);
        });
        builder.push(
            " ON CONFLICT(viewer_id, viewed_file_path_id) DO UPDATE \
             SET views_count = EXCLUDED.views_count + own_aggregate_recent_view.views_count",
        );
        builder
            .build()
            .execute(pool)
            .await
            .map_err(|err| err.to_string())?;
    }
    Ok(())
}
