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

/// `types.InsightSeries` subset used by the query runner.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Series {
    pub id: i32,
    pub series_id: String,
    pub repositories: Vec<String>,
    pub just_in_time: bool,
    pub generation_method: String,
}

/// `InsightStore.GetDataSeries` with `SeriesID` + `deleted_at IS NULL`.
pub async fn get_data_series(pool: &PgPool, series_id: &str) -> Result<Option<Series>, String> {
    let row = sqlx::query(
        r#"
SELECT id, series_id, repositories, just_in_time, generation_method
FROM insight_series
WHERE deleted_at IS NULL
  AND series_id = $1
"#,
    )
    .bind(series_id)
    .fetch_optional(pool)
    .await
    .map_err(|err| err.to_string())?;

    let Some(row) = row else {
        return Ok(None);
    };
    Ok(Some(Series {
        id: row
            .try_get("id")
            .map_err(|err: sqlx::Error| err.to_string())?,
        series_id: row
            .try_get("series_id")
            .map_err(|err: sqlx::Error| err.to_string())?,
        repositories: row
            .try_get::<Option<Vec<String>>, _>("repositories")
            .map_err(|err: sqlx::Error| err.to_string())?
            .unwrap_or_default(),
        just_in_time: row
            .try_get("just_in_time")
            .map_err(|err: sqlx::Error| err.to_string())?,
        generation_method: row
            .try_get("generation_method")
            .map_err(|err: sqlx::Error| err.to_string())?,
    }))
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

/// `AddIncompleteDatapoint`: the `time` column is `timestamp without time
/// zone`, so bind the UTC wall clock (lib/pq sends a zoned time that PG casts
/// in the session time zone).
pub async fn add_incomplete_datapoint(
    pool: &PgPool,
    series_id: i32,
    reason: &str,
    time: OffsetDateTime,
) -> Result<(), String> {
    let time: PrimitiveDateTime = PrimitiveDateTime::new(time.date(), time.time());
    sqlx::query(
        "insert into insight_series_incomplete_points (series_id, repo_id, reason, time) values ($1, $2, $3, $4) on conflict do nothing",
    )
    .bind(series_id)
    .bind(None::<i32>)
    .bind(reason)
    .bind(time)
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
