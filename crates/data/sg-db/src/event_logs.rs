//! `internal/database/event_logs.go`: the reads behind the legacy usage
//! telemetry exporter (`export-usage-telemetry`).
//!
//! Ported paths:
//!
//! - [`list_exportable_events`][]: `eventLogStore.ListExportableEvents`
//!   (`WHERE event_logs.id > $1 AND name IN (SELECT event_name FROM
//!   event_logs_export_allowlist) ORDER BY event_logs.id LIMIT $2`) plus the
//!   `getBySQL` column scan, including the `feature_flags` JSON decode;
//! - [`get_bookmark`][] / [`update_bookmark`][]: `bmStore.GetBookmark` (generate a
//!   row from `MAX(id)` when `event_logs_scrape_state` is empty) and
//!   `bmStore.UpdateBookmark` (only the first row by `id`);
//! - [`count_export_queue`][]: the `queueSizeJob` backlog count.
//!
//! Two column expressions differ from the baseline `SELECT` on purpose:
//! `public_argument` and `feature_flags` are read as `::text`. The baseline
//! scans `jsonb` into `json.RawMessage` (the server's text representation) and
//! re-marshals only the feature flags, so reading the text representation
//! yields byte-identical values. The baseline also selects `url` (and
//! `argument`); the exporter never reads them, so they are not fetched.

use std::collections::BTreeMap;

use sqlx::postgres::PgPool;
use sqlx::types::time::{Date, OffsetDateTime};
use sqlx::Row;

/// One `event_logs` row as `getBySQL` scans it.
#[derive(Debug, Clone, PartialEq)]
pub struct EventRow {
    pub id: i64,
    pub name: String,
    pub user_id: i32,
    pub anonymous_user_id: String,
    pub source: String,
    /// The server's `jsonb` text (`{"a": 1}`), i.e. exactly what the baseline
    /// stores in `json.RawMessage`.
    pub public_argument: String,
    pub version: String,
    pub timestamp: OffsetDateTime,
    /// `feature_flags` decoded from JSON. `None` covers both a NULL column and
    /// the JSON literal `null`: in both cases the baseline leaves
    /// `EvaluatedFlagSet` nil and `Json()` renders `null`.
    pub feature_flags: Option<BTreeMap<String, bool>>,
    pub cohort_id: Option<Date>,
    pub first_source_url: Option<String>,
    pub last_source_url: Option<String>,
    pub referrer: Option<String>,
    pub device_id: Option<String>,
    pub insert_id: Option<String>,
}

/// `eventLogStore.ListExportableEvents(ctx, after, limit)`.
pub async fn list_exportable_events(
    pool: &PgPool,
    after: i32,
    limit: i64,
) -> Result<Vec<EventRow>, String> {
    let rows = sqlx::query(
        "SELECT id, name, user_id, anonymous_user_id, source, \
                public_argument::text AS public_argument, version, timestamp, \
                feature_flags::text AS feature_flags, cohort_id, first_source_url, \
                last_source_url, referrer, device_id, insert_id \
         FROM event_logs \
         WHERE event_logs.id > $1 \
           AND name IN (SELECT event_name FROM event_logs_export_allowlist) \
         ORDER BY event_logs.id LIMIT $2",
    )
    .bind(after)
    .bind(limit)
    .fetch_all(pool)
    .await
    .map_err(|err| err.to_string())?;

    let mut events = Vec::with_capacity(rows.len());
    for row in rows {
        events.push(scan_event(&row)?);
    }
    Ok(events)
}

/// The `getBySQL` row scan, including `json.Unmarshal` of `feature_flags`.
fn scan_event(row: &sqlx::postgres::PgRow) -> Result<EventRow, String> {
    let raw_flags: Option<String> = row
        .try_get("feature_flags")
        .map_err(|err| err.to_string())?;
    let feature_flags = match raw_flags {
        // `json.Unmarshal` into a nil map: `null` stays nil, objects decode.
        None => None,
        Some(raw) => serde_json::from_str(&raw).map_err(|err| format!("json.Unmarshal: {err}"))?,
    };

    Ok(EventRow {
        id: row.try_get("id").map_err(|err| err.to_string())?,
        name: row.try_get("name").map_err(|err| err.to_string())?,
        user_id: row.try_get("user_id").map_err(|err| err.to_string())?,
        anonymous_user_id: row
            .try_get("anonymous_user_id")
            .map_err(|err| err.to_string())?,
        source: row.try_get("source").map_err(|err| err.to_string())?,
        public_argument: row
            .try_get("public_argument")
            .map_err(|err| err.to_string())?,
        version: row.try_get("version").map_err(|err| err.to_string())?,
        timestamp: row.try_get("timestamp").map_err(|err| err.to_string())?,
        feature_flags,
        cohort_id: row.try_get("cohort_id").map_err(|err| err.to_string())?,
        first_source_url: row
            .try_get("first_source_url")
            .map_err(|err| err.to_string())?,
        last_source_url: row
            .try_get("last_source_url")
            .map_err(|err| err.to_string())?,
        referrer: row.try_get("referrer").map_err(|err| err.to_string())?,
        device_id: row.try_get("device_id").map_err(|err| err.to_string())?,
        insert_id: row.try_get("insert_id").map_err(|err| err.to_string())?,
    })
}

/// `bmStore.GetBookmark`: read the first scrape-state row, or insert one
/// seeded with `MAX(event_logs.id)`. Both statements share one transaction,
/// so a failed insert (e.g. an empty `event_logs` table makes `MAX(id)` NULL
/// and violates the NOT NULL column) leaves the table untouched.
pub async fn get_bookmark(pool: &PgPool) -> Result<i32, String> {
    let mut tx = pool.begin().await.map_err(|err| err.to_string())?;

    let existing: Option<i32> =
        sqlx::query_scalar("SELECT bookmark_id FROM event_logs_scrape_state ORDER BY id LIMIT 1")
            .fetch_optional(&mut *tx)
            .await
            .map_err(|err| err.to_string())?;

    let value = match existing {
        Some(value) => value,
        None => sqlx::query_scalar(
            "INSERT INTO event_logs_scrape_state (bookmark_id) \
             SELECT MAX(id) FROM event_logs RETURNING bookmark_id",
        )
        .fetch_one(&mut *tx)
        .await
        .map_err(|err| err.to_string())?,
    };

    tx.commit().await.map_err(|err| err.to_string())?;
    Ok(value)
}

/// `bmStore.UpdateBookmark`: update only the first scrape-state row.
pub async fn update_bookmark(pool: &PgPool, value: i32) -> Result<(), String> {
    sqlx::query(
        "UPDATE event_logs_scrape_state SET bookmark_id = $1 \
         WHERE id = (SELECT id FROM event_logs_scrape_state ORDER BY id LIMIT 1)",
    )
    .bind(value)
    .execute(pool)
    .await
    .map_err(|err| err.to_string())?;
    Ok(())
}

/// `queueSizeJob.GetCount`: allowlisted events still waiting behind the
/// bookmark.
pub async fn count_export_queue(pool: &PgPool, bookmark: i32) -> Result<i64, String> {
    sqlx::query_scalar(
        "SELECT count(*) FROM event_logs \
         WHERE id > $1 \
           AND name IN (SELECT event_name FROM event_logs_export_allowlist)",
    )
    .bind(bookmark)
    .fetch_one(pool)
    .await
    .map_err(|err| err.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn feature_flags_decode_like_go_into_sorted_map() {
        let decoded: Option<BTreeMap<String, bool>> =
            serde_json::from_str(r#"{"b":true,"a":false}"#).expect("object");
        let decoded = decoded.expect("object");
        let keys: Vec<&String> = decoded.keys().collect();
        assert_eq!(keys, ["a", "b"], "BTreeMap keeps JSON marshal order");
        assert!(!decoded["a"]);
        assert!(decoded["b"]);

        let null: Option<BTreeMap<String, bool>> = serde_json::from_str("null").expect("null");
        assert!(null.is_none());
    }
}
