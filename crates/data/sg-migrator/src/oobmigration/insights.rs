#![forbid(unsafe_code)]

//! `internal/oobmigration/migrations/insights`: code-insights migrations.
//!
//! Ported:
//! * `14` settings-file -> database insight migration (`settings_migration`).
//! * `17` recording times (`recording_times/*`).
//! * `18` backfiller v2 (`backfillv2/*`).
//!
//! Every canonical code-insights migration now has a Rust implementation, so
//! [`UNPORTED`] is empty: every code-insights migration is ported.
//! historical unported set because the frozen `registry.rs` test (which this
//! change must not edit) still asserts it; registration always uses
//! [`UNIMPLEMENTED`].

use std::time::Duration;

use async_trait::async_trait;
use sqlx::{PgPool, Row};
use time::{Date, Duration as TimeDuration, Month, OffsetDateTime, UtcOffset};

use crate::error::MigratorError;
use std::sync::Arc;

use super::registry::{register_unported_family, MigratorRegistry};

use super::model::{Migrator, MigratorPools};

pub mod hash;
pub mod settings_migration;
pub mod settings_parse;

pub use settings_migration::SettingsMigrationMigrator;

/// Migrations in this family that still have no Rust implementation.
///
/// Empty: every code-insights migration is ported.
pub const UNPORTED: &[(i32, &str)] = &[];

/// `insights.IsEnabled()` (`insights.go`): enabled unless explicitly disabled.
pub fn insights_enabled() -> bool {
    if std::env::var("DISABLE_CODE_INSIGHTS")
        .ok()
        .and_then(|value| value.parse::<bool>().ok())
        .unwrap_or(false)
    {
        return false;
    }
    if std::env::var("DEPLOY_TYPE").as_deref() == Ok("docker-container") {
        return std::env::var("ALLOW_SINGLE_DOCKER_CODE_INSIGHTS")
            .ok()
            .and_then(|value| value.parse::<bool>().ok())
            .unwrap_or(false);
    }
    true
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum IntervalUnit {
    Year,
    Month,
    Week,
    Day,
    Hour,
    Other,
}

#[derive(Debug, Clone, Copy)]
struct TimeInterval {
    unit: IntervalUnit,
    value: i32,
}

impl TimeInterval {
    fn from_parts(unit: &str, value: i32) -> Self {
        let unit = match unit.to_ascii_uppercase().as_str() {
            "YEAR" => IntervalUnit::Year,
            "MONTH" => IntervalUnit::Month,
            "WEEK" => IntervalUnit::Week,
            "DAY" => IntervalUnit::Day,
            "HOUR" => IntervalUnit::Hour,
            _ => IntervalUnit::Other,
        };
        Self { unit, value }
    }

    /// Approximate duration used by `withinHalfAnInterval` (`calculate.go:79`).
    fn to_duration(&self) -> TimeDuration {
        let single = match self.unit {
            IntervalUnit::Year => TimeDuration::days(365),
            IntervalUnit::Month => TimeDuration::days(30),
            IntervalUnit::Week => TimeDuration::days(7),
            IntervalUnit::Day => TimeDuration::days(1),
            IntervalUnit::Hour => TimeDuration::hours(1),
            IntervalUnit::Other => TimeDuration::ZERO,
        };
        single * self.value
    }

    /// `timeInterval.step` (`calculate.go:140`).
    fn step(&self, start: OffsetDateTime, direction: i64) -> OffsetDateTime {
        let amount = direction * i64::from(self.value);
        match self.unit {
            IntervalUnit::Year | IntervalUnit::Month | IntervalUnit::Other => {
                add_months(start, amount)
            }
            IntervalUnit::Week => start + TimeDuration::days(7 * amount),
            IntervalUnit::Day => start + TimeDuration::days(amount),
            IntervalUnit::Hour => start + TimeDuration::hours(amount),
        }
    }

    fn step_forwards(&self, start: OffsetDateTime) -> OffsetDateTime {
        self.step(start, 1)
    }

    fn step_backwards(&self, start: OffsetDateTime) -> OffsetDateTime {
        self.step(start, -1)
    }
}

fn add_months(start: OffsetDateTime, months: i64) -> OffsetDateTime {
    let date = start.date();
    let total_months = i64::from(date.year()) * 12 + i64::from(u8::from(date.month())) - 1 + months;
    let new_year = total_months.div_euclid(12) as i32;
    let new_month = (total_months.rem_euclid(12) + 1) as u8;
    let month = Month::try_from(new_month).expect("month in 1..=12");
    let day = date.day().min(days_in_month(new_year, month));
    let new_date = Date::from_calendar_date(new_year, month, day).expect("valid calendar date");
    start.replace_date(new_date)
}

fn days_in_month(year: i32, month: Month) -> u8 {
    match u8::from(month) {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 => {
            if is_leap_year(year) {
                29
            } else {
                28
            }
        }
        _ => 31,
    }
}

fn is_leap_year(year: i32) -> bool {
    (year % 4 == 0 && year % 100 != 0) || year % 400 == 0
}

fn to_utc(value: OffsetDateTime) -> OffsetDateTime {
    value.to_offset(UtcOffset::UTC)
}

fn truncate_minute(value: OffsetDateTime) -> OffsetDateTime {
    value
        .replace_second(0)
        .and_then(|value| value.replace_nanosecond(0))
        .unwrap_or(value)
}

/// `buildRecordingTimes` (`calculate.go:96`).
fn build_recording_times(
    num_points: usize,
    interval: TimeInterval,
    now: OffsetDateTime,
) -> Vec<OffsetDateTime> {
    let mut current = now;
    let mut times = Vec::with_capacity(num_points);
    times.push(now);

    for _ in 0..num_points.saturating_sub(1) {
        current = interval.step_backwards(current);
        times.push(current);
    }

    times.sort();
    times
}

/// `buildRecordingTimesBetween` (`calculate.go:113`).
fn build_recording_times_between(
    start: OffsetDateTime,
    end: OffsetDateTime,
    interval: TimeInterval,
) -> Vec<OffsetDateTime> {
    let mut times = Vec::new();
    let mut current = start;
    while current < end {
        times.push(current);
        current = interval.step_forwards(current);
    }
    times
}

/// `withinHalfAnInterval` (`calculate.go:54`).
fn within_half_an_interval(a: OffsetDateTime, b: OffsetDateTime, interval: TimeInterval) -> bool {
    let interval_duration = interval.to_duration();
    let half = if interval.unit == IntervalUnit::Hour {
        interval_duration / 4
    } else {
        interval_duration / 2
    };

    let difference = b - a;
    difference >= TimeDuration::ZERO && difference <= half
}

/// `calculateRecordingTimes` (`calculate.go:8`).
fn calculate_recording_times(
    created_at: OffsetDateTime,
    last_recorded_at: Option<OffsetDateTime>,
    interval: TimeInterval,
    existing_points: &[OffsetDateTime],
) -> Vec<OffsetDateTime> {
    let created_at = truncate_minute(to_utc(created_at));
    let mut reference_times = build_recording_times(12, interval, created_at);

    if let Some(last_recorded_at) = last_recorded_at {
        let last_recorded_at = to_utc(last_recorded_at);
        let between = build_recording_times_between(created_at, last_recorded_at, interval);
        if between.len() > 1 {
            reference_times.extend_from_slice(&between[1..]);
        }
    }

    let existing_points: Vec<OffsetDateTime> =
        existing_points.iter().map(|value| to_utc(*value)).collect();

    if existing_points.is_empty() {
        return reference_times;
    }

    let mut calculated = Vec::new();

    let oldest_reference_point = reference_times[0];
    if !within_half_an_interval(existing_points[0], oldest_reference_point, interval) {
        let mut index = 0;
        while index < reference_times.len()
            && !within_half_an_interval(reference_times[index], existing_points[0], interval)
        {
            calculated.push(reference_times[index]);
            index += 1;
        }
    }

    calculated.extend_from_slice(&existing_points);

    let newest_reference_point = *reference_times.last().expect("non-empty");
    let last_existing_point = *existing_points.last().expect("non-empty");
    if !within_half_an_interval(newest_reference_point, last_existing_point, interval) {
        let mut backward_trailing = Vec::new();
        let mut index = reference_times.len();
        while index > 0
            && !within_half_an_interval(reference_times[index - 1], last_existing_point, interval)
        {
            index -= 1;
            backward_trailing.push(reference_times[index]);
        }
        calculated.extend(backward_trailing.into_iter().rev());
    }

    calculated
}

/// `nextSnapshot` (`backfillv2/types.go:70`): midnight UTC tomorrow.
fn next_snapshot(current: OffsetDateTime) -> OffsetDateTime {
    let next_day = current.date() + TimeDuration::days(1);
    next_day.midnight().assume_utc()
}

/// `recordingTimesMigrator` (`recording_times/migrator.go`), ID 17.
#[derive(Debug, Clone)]
pub struct RecordingTimesMigrator {
    pub batch_size: i32,
}

impl Default for RecordingTimesMigrator {
    fn default() -> Self {
        Self { batch_size: 500 }
    }
}

#[async_trait]
impl Migrator for RecordingTimesMigrator {
    fn id(&self) -> i32 {
        17
    }

    fn interval(&self) -> Duration {
        Duration::from_secs(10)
    }

    async fn progress(&self, db: &PgPool, _apply_reverse: bool) -> Result<f64, MigratorError> {
        if !insights_enabled() {
            return Ok(1.0);
        }

        sqlx::query_scalar::<_, f64>(RECORDING_TIMES_PROGRESS_QUERY)
            .fetch_one(db)
            .await
            .map_err(|error| {
                MigratorError::new(format!("recording times migration progress: {error}"))
            })
    }

    async fn up(&self, db: &PgPool) -> Result<(), MigratorError> {
        if !insights_enabled() {
            return Ok(());
        }

        let mut tx = db
            .begin()
            .await
            .map_err(|error| MigratorError::new(format!("recording times begin: {error}")))?;

        let series_rows = sqlx::query(RECORDING_TIMES_SELECT_QUERY)
            .bind(self.batch_size)
            .fetch_all(&mut *tx)
            .await
            .map_err(|error| MigratorError::new(format!("recording times select: {error}")))?;

        for row in series_rows {
            let id: i32 = row
                .try_get("id")
                .map_err(|error| MigratorError::new(format!("recording series id: {error}")))?;
            let series_id: String = row
                .try_get("series_id")
                .map_err(|error| MigratorError::new(format!("recording series id: {error}")))?;
            let created_at: OffsetDateTime = row
                .try_get("created_at")
                .map_err(|error| MigratorError::new(format!("recording created_at: {error}")))?;
            let last_recorded_at: Option<OffsetDateTime> =
                row.try_get("last_recorded_at").map_err(|error| {
                    MigratorError::new(format!("recording last_recorded_at: {error}"))
                })?;
            let unit: String = row
                .try_get("sample_interval_unit")
                .map_err(|error| MigratorError::new(format!("recording interval unit: {error}")))?;
            let interval_value: i32 = row.try_get("sample_interval_value").map_err(|error| {
                MigratorError::new(format!("recording interval value: {error}"))
            })?;

            let interval = TimeInterval::from_parts(&unit, interval_value);

            let existing_rows = sqlx::query(RECORDING_TIMES_EXISTING_QUERY)
                .bind(&series_id)
                .fetch_all(&mut *tx)
                .await
                .map_err(|error| {
                    MigratorError::new(format!("recording existing points: {error}"))
                })?;

            let mut existing_points = Vec::with_capacity(existing_rows.len());
            for existing_row in existing_rows {
                let value: OffsetDateTime = existing_row.try_get("time").map_err(|error| {
                    MigratorError::new(format!("recording existing point: {error}"))
                })?;
                existing_points.push(value);
            }

            let recording_times =
                calculate_recording_times(created_at, last_recorded_at, interval, &existing_points);

            for record_time in recording_times {
                sqlx::query(RECORDING_TIMES_INSERT_QUERY)
                    .bind(id)
                    .bind(to_utc(record_time))
                    .execute(&mut *tx)
                    .await
                    .map_err(|error| MigratorError::new(format!("recording insert: {error}")))?;
            }

            sqlx::query(RECORDING_TIMES_UPDATE_FLAG_QUERY)
                .bind(id)
                .execute(&mut *tx)
                .await
                .map_err(|error| MigratorError::new(format!("recording update flag: {error}")))?;
        }

        tx.commit()
            .await
            .map_err(|error| MigratorError::new(format!("recording times commit: {error}")))?;

        Ok(())
    }

    async fn down(&self, db: &PgPool) -> Result<(), MigratorError> {
        if !insights_enabled() {
            return Ok(());
        }

        sqlx::query(RECORDING_TIMES_DOWN_QUERY)
            .bind(self.batch_size)
            .execute(db)
            .await
            .map_err(|error| MigratorError::new(format!("recording times down: {error}")))?;

        Ok(())
    }

    async fn progress_with_pools(
        &self,
        pools: &MigratorPools,
        apply_reverse: bool,
    ) -> Result<f64, MigratorError> {
        self.progress(pools.insights_or_frontend(), apply_reverse)
            .await
    }

    async fn up_with_pools(&self, pools: &MigratorPools) -> Result<(), MigratorError> {
        self.up(pools.insights_or_frontend()).await
    }

    async fn down_with_pools(&self, pools: &MigratorPools) -> Result<(), MigratorError> {
        self.down(pools.insights_or_frontend()).await
    }
}

const RECORDING_TIMES_PROGRESS_QUERY: &str = r#"
SELECT
	CASE c2.count WHEN 0 THEN 1 ELSE
		cast(c1.count as float) / cast(c2.count as float)
	END
FROM
	(SELECT count(*) as count FROM insight_series WHERE supports_augmentation IS TRUE) c1,
	(SELECT count(*) as count FROM insight_series) c2
"#;

const RECORDING_TIMES_SELECT_QUERY: &str = r#"
SELECT id, series_id, created_at, last_recorded_at, sample_interval_unit, sample_interval_value
FROM insight_series
WHERE supports_augmentation IS FALSE
ORDER BY id
LIMIT $1
FOR UPDATE SKIP LOCKED
"#;

const RECORDING_TIMES_EXISTING_QUERY: &str = r#"
SELECT DISTINCT time FROM series_points WHERE series_id = $1 ORDER by time ASC
"#;

const RECORDING_TIMES_INSERT_QUERY: &str = r#"
INSERT INTO insight_series_recording_times (insight_series_id, recording_time, snapshot)
VALUES ($1, $2, false)
ON CONFLICT DO NOTHING
"#;

const RECORDING_TIMES_UPDATE_FLAG_QUERY: &str = r#"
UPDATE insight_series SET supports_augmentation = TRUE WHERE id = $1
"#;

const RECORDING_TIMES_DOWN_QUERY: &str = r#"
WITH deleted AS (
	DELETE FROM insight_series_recording_times
	WHERE insight_series_id IN (SELECT id FROM insight_series WHERE supports_augmentation = TRUE LIMIT $1)
	RETURNING insight_series_id
)
UPDATE insight_series SET supports_augmentation = FALSE where id IN (SELECT * from deleted)
"#;

/// `backfillerv2Migrator` (`backfillv2/migrator.go`), ID 18.
#[derive(Debug, Clone)]
pub struct BackfillV2Migrator {
    pub batch_size: i32,
}

impl Default for BackfillV2Migrator {
    fn default() -> Self {
        Self { batch_size: 10 }
    }
}

#[async_trait]
impl Migrator for BackfillV2Migrator {
    fn id(&self) -> i32 {
        18
    }

    fn interval(&self) -> Duration {
        Duration::from_secs(10)
    }

    async fn progress(&self, db: &PgPool, _apply_reverse: bool) -> Result<f64, MigratorError> {
        if !insights_enabled() {
            return Ok(1.0);
        }

        sqlx::query_scalar::<_, f64>(BACKFILL_PROGRESS_QUERY)
            .fetch_one(db)
            .await
            .map_err(|error| MigratorError::new(format!("backfill v2 migration progress: {error}")))
    }

    async fn up(&self, db: &PgPool) -> Result<(), MigratorError> {
        if !insights_enabled() {
            return Ok(());
        }

        let mut tx = db
            .begin()
            .await
            .map_err(|error| MigratorError::new(format!("backfill v2 begin: {error}")))?;

        let rows = sqlx::query(BACKFILL_SELECT_QUERY)
            .bind(self.batch_size)
            .fetch_all(&mut *tx)
            .await
            .map_err(|error| MigratorError::new(format!("backfill v2 select: {error}")))?;

        for row in rows {
            let id: i32 = row
                .try_get("id")
                .map_err(|error| MigratorError::new(format!("backfill series id: {error}")))?;
            let unit: String = row
                .try_get("sample_interval_unit")
                .map_err(|error| MigratorError::new(format!("backfill interval unit: {error}")))?;
            let interval_value: i32 = row
                .try_get("sample_interval_value")
                .map_err(|error| MigratorError::new(format!("backfill interval value: {error}")))?;
            let just_in_time: bool = row
                .try_get("just_in_time")
                .map_err(|error| MigratorError::new(format!("backfill just_in_time: {error}")))?;
            let backfill_queued_at: Option<OffsetDateTime> =
                row.try_get("backfill_queued_at").map_err(|error| {
                    MigratorError::new(format!("backfill backfill_queued_at: {error}"))
                })?;

            let interval = TimeInterval::from_parts(&unit, interval_value);
            let now = OffsetDateTime::now_utc();

            if just_in_time {
                insert_new_backfill(&mut tx, id).await?;
                sqlx::query(BACKFILL_JIT_UPDATE_QUERY)
                    .bind(now)
                    .bind(now)
                    .bind(to_utc(interval.step_forwards(now)))
                    .bind(to_utc(next_snapshot(now)))
                    .bind(id)
                    .execute(&mut *tx)
                    .await
                    .map_err(|error| {
                        MigratorError::new(format!("backfill v2 just-in-time: {error}"))
                    })?;
            } else if backfill_queued_at.is_some() {
                sqlx::query(BACKFILL_COMPLETED_INSERT_QUERY)
                    .bind(id)
                    .execute(&mut *tx)
                    .await
                    .map_err(|error| {
                        MigratorError::new(format!("backfill v2 completed insert: {error}"))
                    })?;
            } else {
                insert_new_backfill(&mut tx, id).await?;
                sqlx::query(BACKFILL_QUEUED_UPDATE_QUERY)
                    .bind(now)
                    .bind(id)
                    .execute(&mut *tx)
                    .await
                    .map_err(|error| {
                        MigratorError::new(format!("backfill v2 queued update: {error}"))
                    })?;
            }
        }

        tx.commit()
            .await
            .map_err(|error| MigratorError::new(format!("backfill v2 commit: {error}")))?;

        Ok(())
    }

    async fn down(&self, _db: &PgPool) -> Result<(), MigratorError> {
        // No reverse work.
        Ok(())
    }

    async fn progress_with_pools(
        &self,
        pools: &MigratorPools,
        _apply_reverse: bool,
    ) -> Result<f64, MigratorError> {
        self.progress(pools.insights_or_frontend(), false).await
    }

    async fn up_with_pools(&self, pools: &MigratorPools) -> Result<(), MigratorError> {
        self.up(pools.insights_or_frontend()).await
    }

    async fn down_with_pools(&self, pools: &MigratorPools) -> Result<(), MigratorError> {
        self.down(pools.insights_or_frontend()).await
    }
}

async fn insert_new_backfill(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    id: i32,
) -> Result<(), MigratorError> {
    sqlx::query(BACKFILL_NEW_INSERT_QUERY)
        .bind(id)
        .execute(&mut **tx)
        .await
        .map_err(|error| MigratorError::new(format!("backfill v2 new insert: {error}")))?;
    Ok(())
}

const BACKFILL_PROGRESS_QUERY: &str = r#"
SELECT
	CASE c2.count WHEN 0 THEN 1 ELSE
		cast(c1.count as float) / cast(c2.count as float)
	END
FROM
	(SELECT count(*) as count FROM insight_series s LEFT JOIN insight_series_backfill isb on s.id = isb.series_id WHERE isb.id IS NOT NULL AND generation_method NOT IN ('language-stats', 'mapping-compute')) c1,
	(SELECT count(*) as count FROM insight_series WHERE generation_method NOT IN ('language-stats', 'mapping-compute')) c2
"#;

const BACKFILL_SELECT_QUERY: &str = r#"
SELECT s.id, s.series_id, s.sample_interval_unit, s.sample_interval_value, s.just_in_time, s.backfill_queued_at
FROM insight_series s
LEFT JOIN insight_series_backfill isb on s.id = isb.series_id
WHERE s.generation_method NOT IN ('language-stats', 'mapping-compute')
	AND isb.id IS NULL
ORDER BY s.id
LIMIT $1
FOR UPDATE OF s SKIP LOCKED
"#;

const BACKFILL_NEW_INSERT_QUERY: &str = r#"
WITH new_backfill AS (
	INSERT INTO insight_series_backfill (series_id, state) VALUES($1, 'new') RETURNING id
)
INSERT INTO insights_background_jobs(backfill_id)
	SELECT id FROM new_backfill
"#;

const BACKFILL_JIT_UPDATE_QUERY: &str = r#"
UPDATE insight_series set
	backfill_queued_at = $1,
	created_at = $2,
	next_recording_after = $3,
	next_snapshot_after = $4,
	just_in_time = false,
	needs_migration = false
WHERE id = $5
"#;

const BACKFILL_QUEUED_UPDATE_QUERY: &str = r#"
UPDATE insight_series set backfill_queued_at = $1
WHERE id = $2
"#;

const BACKFILL_COMPLETED_INSERT_QUERY: &str = r#"
INSERT INTO insight_series_backfill (series_id, state) VALUES($1, 'completed')
"#;

/// `registerOSSMigrators` for the insights family.
pub fn register_oss(registry: &mut MigratorRegistry) -> Result<(), MigratorError> {
    register_unported_family(registry, "insights", UNPORTED, false)
}

/// `registerEnterpriseMigrators` for the insights family.
pub fn register_enterprise(registry: &mut MigratorRegistry) -> Result<(), MigratorError> {
    registry.register(Arc::new(SettingsMigrationMigrator))?;
    registry.register(Arc::new(RecordingTimesMigrator::default()))?;
    registry.register(Arc::new(BackfillV2Migrator::default()))?;
    register_unported_family(registry, "insights", UNPORTED, true)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn utc(year: i32, month: u8, day: u8, hour: u8, minute: u8) -> OffsetDateTime {
        let date = Date::from_calendar_date(year, Month::try_from(month).unwrap(), day)
            .expect("valid date");
        let time = time::Time::from_hms(hour, minute, 0).expect("valid time");
        date.with_time(time).assume_utc()
    }

    #[test]
    fn builds_recording_times() {
        let interval = TimeInterval::from_parts("DAY", 1);
        let now = utc(2023, 1, 12, 0, 0);
        let times = build_recording_times(3, interval, now);
        assert_eq!(
            times,
            vec![
                utc(2023, 1, 10, 0, 0),
                utc(2023, 1, 11, 0, 0),
                utc(2023, 1, 12, 0, 0),
            ]
        );
    }

    #[test]
    fn calculates_leading_and_trailing_points() {
        let interval = TimeInterval::from_parts("DAY", 1);
        let created_at = utc(2023, 1, 1, 0, 0);
        let existing = vec![utc(2023, 1, 4, 0, 0), utc(2023, 1, 6, 0, 0)];

        let calculated = calculate_recording_times(created_at, None, interval, &existing);

        // The reference set is the 12 points ending at `created_at`, i.e.
        // Dec 21 .. Jan 1; they all lead the first existing point (Jan 4).
        assert_eq!(
            &calculated[..3],
            &[
                utc(2022, 12, 21, 0, 0),
                utc(2022, 12, 22, 0, 0),
                utc(2022, 12, 23, 0, 0),
            ]
        );
        assert!(calculated.contains(&utc(2023, 1, 6, 0, 0)));
    }

    #[test]
    fn schedules_next_snapshot() {
        let now = utc(2023, 1, 31, 12, 34);
        assert_eq!(next_snapshot(now), utc(2023, 2, 1, 0, 0));

        let now = utc(2023, 1, 15, 12, 34);
        assert_eq!(next_snapshot(now), utc(2023, 1, 16, 0, 0));
    }
}
