#![forbid(unsafe_code)]

//! The code-insights writes the query-runner handler needs
//! (`internal/insights/store`): `GetDataSeries`,
//! `DeleteSnapshots`/`RecordSeriesPointsAndRecordingTimes` (including the
//! `repo_names` upsert) and `AddIncompleteDatapoint`.
//!
//! Only the columns `workHandler` actually reads are selected from
//! `insight_series`; timestamps that the baseline scans but never uses are
//! left in the table. All statements target the existing codeinsights schema.

use sqlx::postgres::PgPool;
use sqlx::types::time::{OffsetDateTime, PrimitiveDateTime};
use sqlx::Row;

/// `types.InsightSeries` subset used by the query runner and the enqueuer.
///
/// `repositories` is `NULL` in the table and `[]string` (nil) in Go; both are
/// flattened to an empty vector, which is what the query builder branches on.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Series {
    pub id: i32,
    pub series_id: String,
    pub query: String,
    pub repositories: Vec<String>,
    pub sample_interval_unit: String,
    pub sample_interval_value: i32,
    pub group_by: Option<String>,
    pub repository_criteria: Option<String>,
    pub just_in_time: bool,
    pub generation_method: String,
}

/// The columns [`Series`] reads. `sample_interval_unit` is the `time_unit`
/// enum; casting to text matches how lib/pq decodes it in the baseline.
const SERIES_COLUMNS: &str = "id, series_id, query, sample_interval_unit::text AS sample_interval_unit, \
     sample_interval_value, group_by, repository_criteria, just_in_time, generation_method, repositories";

fn map_series(row: &sqlx::postgres::PgRow) -> Result<Series, String> {
    Ok(Series {
        id: row
            .try_get("id")
            .map_err(|err: sqlx::Error| err.to_string())?,
        series_id: row
            .try_get("series_id")
            .map_err(|err: sqlx::Error| err.to_string())?,
        query: row
            .try_get("query")
            .map_err(|err: sqlx::Error| err.to_string())?,
        repositories: row
            .try_get::<Option<Vec<String>>, _>("repositories")
            .map_err(|err: sqlx::Error| err.to_string())?
            .unwrap_or_default(),
        sample_interval_unit: row
            .try_get("sample_interval_unit")
            .map_err(|err: sqlx::Error| err.to_string())?,
        sample_interval_value: row
            .try_get("sample_interval_value")
            .map_err(|err: sqlx::Error| err.to_string())?,
        group_by: row
            .try_get("group_by")
            .map_err(|err: sqlx::Error| err.to_string())?,
        repository_criteria: row
            .try_get("repository_criteria")
            .map_err(|err: sqlx::Error| err.to_string())?,
        just_in_time: row
            .try_get("just_in_time")
            .map_err(|err: sqlx::Error| err.to_string())?,
        generation_method: row
            .try_get("generation_method")
            .map_err(|err: sqlx::Error| err.to_string())?,
    })
}

/// `GetDataSeriesArgs`. Zero values mean "no predicate" like the baseline
/// (`time.Time.IsZero`, `ID != 0`, string length, booleans).
#[derive(Debug, Clone, Default)]
pub struct GetDataSeriesArgs {
    pub next_recording_before: Option<OffsetDateTime>,
    pub next_snapshot_before: Option<OffsetDateTime>,
    pub include_deleted: bool,
    pub backfill_not_queued: bool,
    pub backfill_not_complete: bool,
    pub series_id: Option<String>,
    pub id: Option<i32>,
    pub global_only: bool,
    pub exclude_just_in_time: bool,
}

/// `InsightStore.GetDataSeries`: predicates joined in the baseline's order.
pub async fn get_data_series_with_args(
    pool: &PgPool,
    args: &GetDataSeriesArgs,
) -> Result<Vec<Series>, String> {
    let mut preds: Vec<String> = Vec::new();
    let mut binds: Vec<PrimitiveDateTime> = Vec::new();
    let mut text_binds: Vec<String> = Vec::new();
    let mut id_bind: Option<i32> = None;

    if let Some(before) = args.next_recording_before {
        preds.push(format!("next_recording_after < ${}", binds.len() + 1));
        binds.push(PrimitiveDateTime::new(before.date(), before.time()));
    }
    if let Some(before) = args.next_snapshot_before {
        preds.push(format!("next_snapshot_after < ${}", binds.len() + 1));
        binds.push(PrimitiveDateTime::new(before.date(), before.time()));
    }
    if !args.include_deleted {
        preds.push("deleted_at IS NULL".to_owned());
    }
    if preds.is_empty() {
        preds.push("TRUE".to_owned());
    }
    if args.backfill_not_queued {
        preds.push("backfill_queued_at IS NULL".to_owned());
    }
    if args.backfill_not_complete {
        preds.push("backfill_completed_at IS NULL".to_owned());
    }
    if let Some(series_id) = &args.series_id {
        preds.push(format!(
            "series_id = ${}",
            binds.len() + text_binds.len() + 1
        ));
        text_binds.push(series_id.clone());
    }
    if let Some(id) = args.id {
        preds.push(format!("id = ${}", binds.len() + text_binds.len() + 1));
        id_bind = Some(id);
    }
    if args.global_only {
        preds.push(
            "((repositories IS NULL OR CARDINALITY(repositories) = 0) AND repository_criteria IS NULL)"
                .to_owned(),
        );
    }
    if args.exclude_just_in_time {
        preds.push("just_in_time = false".to_owned());
    }

    let sql = format!(
        "SELECT {SERIES_COLUMNS} FROM insight_series WHERE {}",
        preds.join("\n AND ")
    );
    let mut query = sqlx::query(&sql);
    for bind in &binds {
        query = query.bind(*bind);
    }
    for bind in &text_binds {
        query = query.bind(bind.as_str());
    }
    if let Some(id) = id_bind {
        query = query.bind(id);
    }
    let rows = query.fetch_all(pool).await.map_err(|err| err.to_string())?;
    rows.iter().map(map_series).collect()
}

/// `InsightStore.GetDataSeries` restricted to `SeriesID` + `deleted_at IS
/// NULL`, used by the query runner handler.
pub async fn get_data_series(pool: &PgPool, series_id: &str) -> Result<Option<Series>, String> {
    let rows = get_data_series_with_args(
        pool,
        &GetDataSeriesArgs {
            series_id: Some(series_id.to_owned()),
            ..GetDataSeriesArgs::default()
        },
    )
    .await?;
    Ok(rows.into_iter().next())
}

/// `NextSnapshot`: the next UTC midnight strictly after `current`.
pub fn next_snapshot(current: OffsetDateTime) -> OffsetDateTime {
    let current = current.to_offset(time::UtcOffset::UTC);
    let next = current.date().next_day().expect("next snapshot date");
    PrimitiveDateTime::new(next, time::Time::MIDNIGHT).assume_utc()
}

fn interval_for(series: &Series) -> sg_insights::timeseries::TimeInterval {
    sg_insights::timeseries::TimeInterval {
        unit: sg_insights::types::IntervalUnit::from_str_value(&series.sample_interval_unit),
        value: i64::from(series.sample_interval_value),
    }
}

/// `InsightStore.StampRecording`: `last_recorded_at = now`, `next_recording_after
/// = now.StepForwards(interval)`.
pub async fn stamp_recording(
    pool: &PgPool,
    now: OffsetDateTime,
    series: &Series,
) -> Result<(), String> {
    let next = interval_for(series).step_forwards(now);
    sqlx::query(
        "UPDATE insight_series SET last_recorded_at = $1, next_recording_after = $2 WHERE id = $3",
    )
    .bind(PrimitiveDateTime::new(now.date(), now.time()))
    .bind(PrimitiveDateTime::new(next.date(), next.time()))
    .bind(series.id)
    .execute(pool)
    .await
    .map_err(|err| err.to_string())?;
    Ok(())
}

/// `InsightStore.StampSnapshot`: `last_snapshot_at = now`,
/// `next_snapshot_after = NextSnapshot(now)`.
pub async fn stamp_snapshot(
    pool: &PgPool,
    now: OffsetDateTime,
    series: &Series,
) -> Result<(), String> {
    let next = next_snapshot(now);
    sqlx::query(
        "UPDATE insight_series SET last_snapshot_at = $1, next_snapshot_after = $2 WHERE id = $3",
    )
    .bind(PrimitiveDateTime::new(now.date(), now.time()))
    .bind(PrimitiveDateTime::new(next.date(), next.time()))
    .bind(series.id)
    .execute(pool)
    .await
    .map_err(|err| err.to_string())?;
    Ok(())
}

/// `store.PersistMode`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PersistMode {
    Record,
    Snapshot,
}

impl PersistMode {
    pub fn from_raw(raw: &str) -> Option<Self> {
        match raw {
            "record" => Some(Self::Record),
            "snapshot" => Some(Self::Snapshot),
            _ => None,
        }
    }

    /// `string(store.PersistMode)` as written into the queue row.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Record => "record",
            Self::Snapshot => "snapshot",
        }
    }

    fn table(self) -> &'static str {
        match self {
            Self::Record => "series_points",
            Self::Snapshot => "series_points_snapshots",
        }
    }
}

/// `store.RecordSeriesPointArgs` after `toRecording` flattened the dependent
/// frames: every point carries its persist mode, repo and capture.
///
/// `persist_mode` stays the raw column value; like `RecordSeriesPoints`, an
/// unknown mode is only an error when there is at least one point to insert.
#[derive(Debug, Clone, PartialEq)]
pub struct RecordSeriesPoint {
    pub series_id: String,
    pub time: OffsetDateTime,
    pub value: f64,
    pub capture: Option<String>,
    pub repo_id: Option<i32>,
    pub repo_name: Option<String>,
    pub persist_mode: String,
}

/// `types.InsightSeriesRecordingTimes` for a single series.
#[derive(Debug, Clone, PartialEq)]
pub struct RecordingTime {
    pub time: OffsetDateTime,
    pub snapshot: bool,
}

/// `tx.DeleteSnapshots`: delete the snapshot points and snapshot recording
/// times for a series.
pub async fn delete_snapshots(
    conn: &mut sqlx::PgConnection,
    series: &Series,
) -> Result<(), String> {
    sqlx::query("DELETE FROM series_points_snapshots WHERE series_id = $1")
        .bind(&series.series_id)
        .execute(&mut *conn)
        .await
        .map_err(|err| {
            format!(
                "failed to delete insights snapshots for series_id: {}: {err}",
                series.series_id
            )
        })?;

    sqlx::query(
        "DELETE FROM insight_series_recording_times WHERE insight_series_id = $1 and snapshot = true",
    )
    .bind(series.id)
    .execute(&mut *conn)
    .await
    .map_err(|err| {
        format!(
            "failed to delete snapshot recording time for series_id {}: {err}",
            series.id
        )
    })?;
    Ok(())
}

/// `upsertRepoNameFmtStr`: `INSERT ... ON CONFLICT DO NOTHING RETURNING id`
/// followed by a `UNION` lookup (the insert returns no row when the name
/// already exists).
pub async fn upsert_repo_name(conn: &mut sqlx::PgConnection, name: &str) -> Result<i32, String> {
    let row = sqlx::query(
        r#"
WITH e AS(
	INSERT INTO repo_names(name)
	VALUES ($1)
	ON CONFLICT DO NOTHING
	RETURNING id
)
SELECT * FROM e
UNION
	SELECT id FROM repo_names WHERE name = $2;
"#,
    )
    .bind(name)
    .bind(name)
    .fetch_one(&mut *conn)
    .await
    .map_err(|err| format!("upserting repo name ID: {err}"))?;
    // `repo_names.id` is a bigint; the point tables store an int4 name id.
    let id: i64 = row
        .try_get("id")
        .map_err(|err: sqlx::Error| format!("repo name ID not found: {err}"))?;
    Ok(id as i32)
}

/// `RecordSeriesPoints`: insert every point (after upserting its repo name)
/// into the recording or snapshot table.
pub async fn record_series_points(
    conn: &mut sqlx::PgConnection,
    points: &[RecordSeriesPoint],
) -> Result<(), String> {
    for point in points {
        let mode = PersistMode::from_raw(&point.persist_mode).ok_or_else(|| {
            format!(
                "unsupported insights series point persist mode: {}",
                point.persist_mode
            )
        })?;
        let repo_name_id = match &point.repo_name {
            Some(name) => Some(upsert_repo_name(conn, name).await?),
            None => None,
        };
        let table = mode.table();
        let query = format!(
            "INSERT INTO {table} (series_id, \"time\", value, repo_id, repo_name_id, original_repo_name_id, capture) VALUES ($1, $2, $3, $4, $5, $5, $6)"
        );
        sqlx::query(&query)
            .bind(&point.series_id)
            .bind(point.time)
            .bind(point.value)
            .bind(point.repo_id)
            .bind(repo_name_id)
            .bind(point.capture.as_deref())
            .execute(&mut *conn)
            .await
            .map_err(|err| format!("Insert: {err}"))?;
    }
    Ok(())
}

/// `SetInsightSeriesRecordingTimes`: `ON CONFLICT DO NOTHING`.
pub async fn set_recording_times(
    conn: &mut sqlx::PgConnection,
    series_id: i32,
    times: &[RecordingTime],
) -> Result<(), String> {
    for time in times {
        sqlx::query(
            r#"
INSERT INTO insight_series_recording_times (insight_series_id, recording_time, snapshot)
VALUES ($1, $2, $3)
ON CONFLICT DO NOTHING
"#,
        )
        .bind(series_id)
        .bind(time.time)
        .bind(time.snapshot)
        .execute(&mut *conn)
        .await
        .map_err(|err| format!("Insert: {err}"))?;
    }
    Ok(())
}

/// `RecordSeriesPointsAndRecordingTimes` inside the caller's transaction
/// (`persistRecordings` already opened one for `DeleteSnapshots`).
pub async fn record_points_and_times(
    conn: &mut sqlx::PgConnection,
    points: &[RecordSeriesPoint],
    series_id: i32,
    times: &[RecordingTime],
) -> Result<(), String> {
    if !points.is_empty() {
        record_series_points(conn, points).await?;
    }
    if !times.is_empty() {
        set_recording_times(conn, series_id, times).await?;
    }
    Ok(())
}

/// `IncompleteReason`.
pub const REASON_TIMEOUT: &str = "timeout";
pub const REASON_GENERIC: &str = "generic";
pub const REASON_EXCEEDS_ERROR_LIMIT: &str = "exceeds-error-limit";

/// `AddIncompleteDatapoint`: the `time` column is `timestamp without time
/// zone`, so bind the UTC wall clock (lib/pq sends a zoned time that PG casts
/// in the session time zone).
pub async fn add_incomplete_datapoint(
    pool: &PgPool,
    series_id: i32,
    reason: &str,
    time: OffsetDateTime,
) -> Result<(), String> {
    let mut conn = pool.acquire().await.map_err(|err| err.to_string())?;
    add_incomplete_datapoint_in(&mut conn, series_id, None, reason, time).await
}

/// `AddIncompleteDatapoint` on an existing connection or transaction
/// (`h.insightsStore.WithOther(tx)` in the in-progress handler).
pub async fn add_incomplete_datapoint_in(
    conn: &mut sqlx::PgConnection,
    series_id: i32,
    repo_id: Option<i32>,
    reason: &str,
    time: OffsetDateTime,
) -> Result<(), String> {
    let time: PrimitiveDateTime = PrimitiveDateTime::new(time.date(), time.time());
    sqlx::query(
        "insert into insight_series_incomplete_points (series_id, repo_id, reason, time) values ($1, $2, $3, $4) on conflict do nothing",
    )
    .bind(series_id)
    .bind(repo_id)
    .bind(reason)
    .bind(time)
    .execute(conn)
    .await
    .map_err(|err| err.to_string())?;
    Ok(())
}

/// `SetSeriesBackfillComplete`: `UPDATE insight_series SET
/// backfill_completed_at = $1 WHERE series_id = $2`.
///
/// The Go handler calls this on the plain insight store (not on its
/// transaction), so it keeps its own autocommit connection here too.
pub async fn set_series_backfill_complete(
    pool: &PgPool,
    series_id: &str,
    timestamp: PrimitiveDateTime,
) -> Result<(), String> {
    sqlx::query("UPDATE insight_series SET backfill_completed_at = $1 WHERE series_id = $2")
        .bind(timestamp)
        .bind(series_id)
        .execute(pool)
        .await
        .map_err(|err| err.to_string())?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn persist_mode_names_match_the_baseline() {
        assert_eq!(PersistMode::from_raw("record"), Some(PersistMode::Record));
        assert_eq!(
            PersistMode::from_raw("snapshot"),
            Some(PersistMode::Snapshot)
        );
        assert_eq!(PersistMode::from_raw("other"), None);
        assert_eq!(PersistMode::Record.table(), "series_points");
        assert_eq!(PersistMode::Snapshot.table(), "series_points_snapshots");
    }
}
