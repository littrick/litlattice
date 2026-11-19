//! `internal/oobmigration/migrations/insights/*`: migration `14`, the
//! settings-file -> database insight migration (`migrator.go`,
//! `settings.go`, `dashboards.go`, `insights.go`,
//! `insights_language_stat.go`).
//!
//! Go registers this migrator with two stores (frontend and code-insights).
//! The Rust [`Migrator`] trait provides one pool; like the other ported
//! insights migrations, this implementation assumes the supplied pool can
//! reach both the frontend tables (`settings`,
//! `insights_settings_migration_jobs`, `org_members`, `orgs`, `users`) and the
//! code-insights tables (`dashboard*`, `insight_view*`, `insight_series`).
//!
//! Semantics mirrored from Go:
//! * global jobs run before org jobs, which run before user jobs;
//! * per-insight and per-dashboard migration failures are collected and
//!   logged, not returned, so a job still completes;
//! * each insight/dashboard is migrated in its own transaction, and the
//!   dashboard/series rows are soft-checked for prior migration by unique ID
//!   or title;
//! * the special "<subject> Insights" dashboard is always created.

use std::time::Duration;

use async_trait::async_trait;
use sqlx::{PgPool, Postgres, Row, Transaction};
use time::{Date, Duration as TimeDuration, Month, OffsetDateTime, Time};

use crate::error::MigratorError;

use super::super::model::{Migrator, MigratorPools};
use super::hash::{hash_id, ksuid};
use super::settings_parse::{
    extract_ids_from_insights, next_recording, next_snapshot, parse_insights,
    parse_time_interval_unit, parse_time_interval_value, special_case_dashboard_name, step_forward,
    LangStatsInsight, ParsedInsights, SearchInsight, SettingDashboard, Settings,
};

/// `insightsMigrator`, ID 14.
#[derive(Debug, Clone, Default)]
pub struct SettingsMigrationMigrator;

#[derive(Debug, Clone, Copy)]
struct MigrationJob {
    user_id: Option<i32>,
    org_id: Option<i32>,
    migrated_insights: i32,
    migrated_dashboards: i32,
}

#[async_trait]
impl Migrator for SettingsMigrationMigrator {
    fn id(&self) -> i32 {
        14
    }

    fn interval(&self) -> Duration {
        Duration::from_secs(10)
    }

    async fn progress(&self, db: &PgPool, _apply_reverse: bool) -> Result<f64, MigratorError> {
        if !super::insights_enabled() {
            return Ok(1.0);
        }

        sqlx::query_scalar::<_, f64>(PROGRESS_QUERY)
            .fetch_one(db)
            .await
            .map_err(|error| {
                MigratorError::new(format!("insights settings migration progress: {error}"))
            })
    }

    async fn up(&self, db: &PgPool) -> Result<(), MigratorError> {
        // Tests pass a single database; in a split deployment the frontend
        // and code-insights stores differ (see `up_with_pools`).
        self.run_up(db, db, super::insights_enabled()).await
    }

    async fn progress_with_pools(
        &self,
        pools: &MigratorPools,
        apply_reverse: bool,
    ) -> Result<f64, MigratorError> {
        self.progress(&pools.frontend, apply_reverse).await
    }

    async fn up_with_pools(&self, pools: &MigratorPools) -> Result<(), MigratorError> {
        self.run_up(
            &pools.frontend,
            pools.insights_or_frontend(),
            super::insights_enabled(),
        )
        .await
    }

    async fn down_with_pools(&self, pools: &MigratorPools) -> Result<(), MigratorError> {
        self.down(&pools.frontend).await
    }

    async fn down(&self, _db: &PgPool) -> Result<(), MigratorError> {
        Ok(())
    }
}

impl SettingsMigrationMigrator {
    async fn run_up(
        &self,
        frontend: &PgPool,
        insights: &PgPool,
        enabled: bool,
    ) -> Result<(), MigratorError> {
        if !enabled {
            return Ok(());
        }

        let mut tx = frontend
            .begin()
            .await
            .map_err(|error| MigratorError::new(format!("insights migration begin: {error}")))?;

        let rows = sqlx::query(UP_QUERY)
            .bind(100)
            .fetch_all(&mut *tx)
            .await
            .map_err(|error| {
                MigratorError::new(format!("insights migration select jobs: {error}"))
            })?;

        let mut jobs = Vec::with_capacity(rows.len());
        for row in rows {
            jobs.push(MigrationJob {
                user_id: row.try_get("user_id").map_err(|error| {
                    MigratorError::new(format!("insights migration job user_id: {error}"))
                })?,
                org_id: row.try_get("org_id").map_err(|error| {
                    MigratorError::new(format!("insights migration job org_id: {error}"))
                })?,
                migrated_insights: row.try_get("migrated_insights").map_err(|error| {
                    MigratorError::new(format!("insights migration job migrated_insights: {error}"))
                })?,
                migrated_dashboards: row.try_get("migrated_dashboards").map_err(|error| {
                    MigratorError::new(format!(
                        "insights migration job migrated_dashboards: {error}"
                    ))
                })?,
            });
        }

        for job in jobs {
            perform_migration_job(insights, &mut tx, job).await?;
        }

        tx.commit()
            .await
            .map_err(|error| MigratorError::new(format!("insights migration commit: {error}")))?;
        Ok(())
    }
}

const PROGRESS_QUERY: &str = r#"
SELECT
	CASE c2.count WHEN 0 THEN 1 ELSE
		CAST(c1.count AS float) / CAST(c2.count AS float)
	END
FROM
	(SELECT COUNT(*) AS count FROM insights_settings_migration_jobs WHERE completed_at IS NOT NULL) c1,
	(SELECT COUNT(*) AS count FROM insights_settings_migration_jobs) c2
"#;

const UP_QUERY: &str = r#"
WITH
global_jobs AS (
	SELECT id FROM insights_settings_migration_jobs
	WHERE completed_at IS NULL AND global IS TRUE
),
org_jobs AS (
	SELECT id FROM insights_settings_migration_jobs
	WHERE completed_at IS NULL AND org_id IS NOT NULL
),
user_jobs AS (
	SELECT id FROM insights_settings_migration_jobs
	WHERE completed_at IS NULL AND user_id IS NOT NULL
),
candidates AS (
	SELECT id FROM global_jobs
	UNION SELECT id FROM org_jobs WHERE
		NOT EXISTS (SELECT 1 FROM global_jobs)
	UNION SELECT id FROM user_jobs WHERE
		NOT EXISTS (SELECT 1 FROM global_jobs) AND
		NOT EXISTS (SELECT 1 FROM org_jobs)
)
SELECT
	user_id,
	org_id,
	migrated_insights,
	migrated_dashboards
FROM insights_settings_migration_jobs
WHERE id IN (SELECT id FROM candidates)
LIMIT $1
FOR UPDATE SKIP LOCKED
"#;

async fn perform_migration_job(
    db: &PgPool,
    tx: &mut Transaction<'_, Postgres>,
    job: MigrationJob,
) -> Result<(), MigratorError> {
    let (subject_name, settings) = get_settings_for_job(tx, job).await?;

    if !settings.is_empty() {
        let parsed = parse_insights(&settings[0]);

        migrate_insights_for_job(db, tx, job, &parsed).await?;

        let unique_id_suffix = make_unique_id_suffix(tx, job).await?;
        migrate_dashboards_for_job(db, tx, job, &parsed.dashboards, &unique_id_suffix).await?;

        let (all_insight_ids, duplicates) = extract_ids_from_insights(&parsed);
        for id in duplicates {
            tracing::warn!(insight_id = %id, "duplicate insight");
        }

        create_special_case_dashboard(db, job, &subject_name, &all_insight_ids, &unique_id_suffix)
            .await?;
    }

    // Go's deferred `Done` marks the job complete only when no error occurred.
    mark_job_complete(tx, job).await?;
    Ok(())
}

async fn mark_job_complete(
    tx: &mut Transaction<'_, Postgres>,
    job: MigrationJob,
) -> Result<(), MigratorError> {
    let (condition, value) = job_condition(job);
    let query = format!(
        "UPDATE insights_settings_migration_jobs SET completed_at = NOW() WHERE {condition}"
    );
    let mut statement = sqlx::query(&query);
    if let Some(value) = value {
        statement = statement.bind(value);
    }
    statement.execute(&mut **tx).await.map_err(|error| {
        MigratorError::new(format!("insights migration mark job complete: {error}"))
    })?;
    Ok(())
}

/// `makeJobCondition`: `(SQL fragment, optional integer bind)`.
fn job_condition(job: MigrationJob) -> (&'static str, Option<i32>) {
    if let Some(user_id) = job.user_id {
        ("user_id = $1", Some(user_id))
    } else if let Some(org_id) = job.org_id {
        ("org_id = $1", Some(org_id))
    } else {
        ("global IS TRUE", None)
    }
}

// ---------------------------------------------------------------------------
// Settings lookup (`settings.go`)
// ---------------------------------------------------------------------------

async fn get_settings_for_job(
    tx: &mut Transaction<'_, Postgres>,
    job: MigrationJob,
) -> Result<(String, Vec<Settings>), MigratorError> {
    if let Some(user_id) = job.user_id {
        return get_settings_for_user(tx, user_id).await;
    }
    if let Some(org_id) = job.org_id {
        return get_settings_for_org(tx, org_id).await;
    }
    Ok((
        "Global".to_owned(),
        scan_settings(tx, GLOBAL_SETTINGS_QUERY, None).await?,
    ))
}

async fn get_settings_for_user(
    tx: &mut Transaction<'_, Postgres>,
    user_id: i32,
) -> Result<(String, Vec<Settings>), MigratorError> {
    let settings = {
        let rows = sqlx::query(USER_SETTINGS_QUERY)
            .bind(user_id)
            .bind(user_id)
            .fetch_all(&mut **tx)
            .await
            .map_err(|error| {
                MigratorError::new(format!("failed to retrieve user settings: {error}"))
            })?;
        scan_settings_rows(rows)
    };

    let user = sqlx::query(USER_NAME_QUERY)
        .bind(user_id)
        .fetch_optional(&mut **tx)
        .await
        .map_err(|error| MigratorError::new(format!("failed to retrieve user by id: {error}")))?;

    let Some(user) = user else {
        return Ok((String::new(), Vec::new()));
    };

    let name: String = user
        .try_get("username")
        .map_err(|error| MigratorError::new(format!("user username: {error}")))?;
    let display_name: Option<String> = user
        .try_get("display_name")
        .map_err(|error| MigratorError::new(format!("user display_name: {error}")))?;
    let subject_name = match display_name {
        Some(display_name) if !display_name.is_empty() => display_name,
        _ => name,
    };

    Ok((subject_name, settings))
}

async fn get_settings_for_org(
    tx: &mut Transaction<'_, Postgres>,
    org_id: i32,
) -> Result<(String, Vec<Settings>), MigratorError> {
    let settings = {
        let rows = sqlx::query(ORG_SETTINGS_QUERY)
            .bind(org_id)
            .fetch_all(&mut **tx)
            .await
            .map_err(|error| {
                MigratorError::new(format!("failed to retrieve org settings: {error}"))
            })?;
        scan_settings_rows(rows)
    };

    let org = sqlx::query(ORG_NAME_QUERY)
        .bind(org_id)
        .fetch_optional(&mut **tx)
        .await
        .map_err(|error| MigratorError::new(format!("failed to retrieve org by id: {error}")))?;

    let Some(org) = org else {
        return Ok((String::new(), Vec::new()));
    };

    let name: String = org
        .try_get("name")
        .map_err(|error| MigratorError::new(format!("org name: {error}")))?;
    let display_name: Option<String> = org
        .try_get("display_name")
        .map_err(|error| MigratorError::new(format!("org display_name: {error}")))?;
    let subject_name = match display_name {
        Some(display_name) if !display_name.is_empty() => display_name,
        _ => name,
    };

    Ok((subject_name, settings))
}

async fn scan_settings(
    tx: &mut Transaction<'_, Postgres>,
    query: &str,
    bind: Option<i32>,
) -> Result<Vec<Settings>, MigratorError> {
    let mut statement = sqlx::query(query);
    if let Some(bind) = bind {
        statement = statement.bind(bind);
    }
    let rows = statement
        .fetch_all(&mut **tx)
        .await
        .map_err(|error| MigratorError::new(format!("failed to retrieve settings: {error}")))?;
    Ok(scan_settings_rows(rows))
}

fn scan_settings_rows(rows: Vec<sqlx::postgres::PgRow>) -> Vec<Settings> {
    rows.into_iter()
        .map(|row| Settings {
            id: row.get("id"),
            org: row.get("org_id"),
            user: row.get("user_id"),
            contents: row.get("contents"),
        })
        .collect()
}

const USER_SETTINGS_QUERY: &str = r#"
SELECT s.id, s.org_id, s.user_id, s.contents
FROM settings s
LEFT JOIN users ON users.id = s.author_user_id
WHERE user_id = $1 AND EXISTS (
	SELECT
	FROM users
	WHERE id = $2 AND deleted_at IS NULL
)
ORDER BY id DESC
LIMIT 1
"#;

const USER_NAME_QUERY: &str = r#"
SELECT u.username, u.display_name
FROM users u
WHERE id = $1 AND deleted_at IS NULL
LIMIT 1
"#;

const ORG_SETTINGS_QUERY: &str = r#"
SELECT s.id, s.org_id, s.user_id, s.contents
FROM settings s
LEFT JOIN users ON users.id = s.author_user_id
WHERE org_id = $1
ORDER BY id DESC
LIMIT 1
"#;

const ORG_NAME_QUERY: &str = r#"
SELECT name, display_name
FROM orgs
WHERE id = $1 AND deleted_at IS NULL
LIMIT 1
"#;

const GLOBAL_SETTINGS_QUERY: &str = r#"
SELECT s.id, s.org_id, s.user_id, s.contents
FROM settings s
LEFT JOIN users ON users.id = s.author_user_id
WHERE user_id IS NULL AND org_id IS NULL
ORDER BY id DESC
LIMIT 1
"#;

// ---------------------------------------------------------------------------
// Insight migration (`insights.go`, `insights_language_stat.go`)
// ---------------------------------------------------------------------------

async fn migrate_insights_for_job(
    db: &PgPool,
    tx: &mut Transaction<'_, Postgres>,
    job: MigrationJob,
    parsed: &ParsedInsights,
) -> Result<(), MigratorError> {
    let total = (parsed.lang_stats.len() + parsed.frontend.len() + parsed.backend.len()) as i32;
    if total == job.migrated_insights {
        return Ok(());
    }

    update_job_progress(tx, job, "insights", "total", total).await?;

    let mut migrated = 0i32;
    let mut errors = Vec::new();

    let (count, failures) = migrate_language_stats(db, &parsed.lang_stats).await;
    migrated += count;
    errors.extend(failures);

    let (count, failures) = migrate_search_insights(db, &parsed.frontend, "frontend").await;
    migrated += count;
    errors.extend(failures);

    let (count, failures) = migrate_search_insights(db, &parsed.backend, "backend").await;
    migrated += count;
    errors.extend(failures);

    update_job_progress(tx, job, "insights", "migrated", migrated).await?;

    if !errors.is_empty() {
        tracing::error!(
            errors = %errors.join("; "),
            "failed to migrate insights"
        );
    }

    Ok(())
}

async fn migrate_dashboards_for_job(
    db: &PgPool,
    tx: &mut Transaction<'_, Postgres>,
    job: MigrationJob,
    dashboards: &[SettingDashboard],
    unique_id_suffix: &str,
) -> Result<(), MigratorError> {
    let total = dashboards.len() as i32;
    if total == job.migrated_dashboards {
        return Ok(());
    }

    update_job_progress(tx, job, "dashboards", "total", total).await?;

    let mut migrated = 0i32;
    let mut errors = Vec::new();
    for dashboard in dashboards {
        match migrate_dashboard(db, job, dashboard, unique_id_suffix).await {
            Ok(()) => migrated += 1,
            Err(error) => errors.push(error),
        }
    }

    update_job_progress(tx, job, "dashboards", "migrated", migrated).await?;

    if !errors.is_empty() {
        tracing::error!(
            errors = %errors.join("; "),
            "failed to migrate dashboards"
        );
    }

    Ok(())
}

/// `invokeWithProgress`'s two column updates (`total_<suffix>` /
/// `migrated_<suffix>`). Both suffixes are internal constants, never user
/// input.
async fn update_job_progress(
    tx: &mut Transaction<'_, Postgres>,
    job: MigrationJob,
    column: &str,
    direction: &str,
    value: i32,
) -> Result<(), MigratorError> {
    let (condition, condition_value) = job_condition(job);
    // The value is `$1`, so the job identifier moves to `$2`.
    let condition = condition.replace("$1", "$2");
    let query = format!(
        "UPDATE insights_settings_migration_jobs SET {direction}_{column} = $1 WHERE {condition}"
    );
    let mut statement = sqlx::query(&query).bind(value);
    if let Some(condition_value) = condition_value {
        statement = statement.bind(condition_value);
    }
    statement.execute(&mut **tx).await.map_err(|error| {
        MigratorError::new(format!(
            "failed to update insights migration progress: {error}"
        ))
    })?;
    Ok(())
}

async fn migrate_language_stats(db: &PgPool, insights: &[LangStatsInsight]) -> (i32, Vec<String>) {
    let mut count = 0;
    let mut errors = Vec::new();
    for insight in insights {
        match migrate_language_stats_insight(db, insight).await {
            Ok(()) => count += 1,
            Err(error) => errors.push(error),
        }
    }
    (count, errors)
}

async fn migrate_language_stats_insight(
    db: &PgPool,
    insight: &LangStatsInsight,
) -> Result<(), String> {
    if insight.id.is_empty() {
        tracing::warn!(
            owner = %owner_name(insight.user_id, insight.org_id),
            "missing language-stat insight identifier"
        );
        return Ok(());
    }

    let existing: i64 = sqlx::query_scalar(LANG_STATS_COUNT_QUERY)
        .bind(&insight.id)
        .fetch_one(db)
        .await
        .map_err(|error| format!("failed to count insights: {error}"))?;
    if existing > 0 {
        return Ok(());
    }

    let mut tx = db
        .begin()
        .await
        .map_err(|error| format!("failed to begin insights transaction: {error}"))?;

    let now = OffsetDateTime::now_utc();
    let series_id = ksuid();

    let view_id: i32 = sqlx::query_scalar(LANG_STATS_INSERT_VIEW_QUERY)
        .bind(&insight.title)
        .bind(&insight.id)
        .bind(insight.other_threshold)
        .fetch_one(&mut *tx)
        .await
        .map_err(|error| format!("failed to insert view: {error}"))?;

    let insight_series_id: i32 = sqlx::query_scalar(LANG_STATS_INSERT_SERIES_QUERY)
        .bind(series_id)
        .bind(now)
        .bind(now - TimeDuration::weeks(26))
        .bind(now)
        .bind(next_snapshot(now))
        .bind(vec![insight.repository.clone()])
        .fetch_one(&mut *tx)
        .await
        .map_err(|error| format!("failed to insert series: {error}"))?;

    sqlx::query(LANG_STATS_INSERT_VIEW_SERIES_QUERY)
        .bind(insight_series_id)
        .bind(view_id)
        .execute(&mut *tx)
        .await
        .map_err(|error| format!("failed to insert view series: {error}"))?;

    insert_view_grant(
        &mut tx,
        view_id,
        grant_tuple(insight.user_id, insight.org_id),
    )
    .await
    .map_err(|error| format!("failed to insert view grant: {error}"))?;

    tx.commit()
        .await
        .map_err(|error| format!("failed to commit insights transaction: {error}"))?;
    Ok(())
}

async fn migrate_search_insights(
    db: &PgPool,
    insights: &[SearchInsight],
    batch: &str,
) -> (i32, Vec<String>) {
    let mut count = 0;
    let mut errors = Vec::new();
    for insight in insights {
        match migrate_search_insight(db, insight, batch).await {
            Ok(()) => count += 1,
            Err(error) => errors.push(error),
        }
    }
    (count, errors)
}

async fn migrate_search_insight(
    db: &PgPool,
    insight: &SearchInsight,
    batch: &str,
) -> Result<(), String> {
    if insight.id.is_empty() {
        tracing::warn!(
            owner = %owner_name(insight.user_id, insight.org_id),
            "missing insight identifier"
        );
        return Ok(());
    }
    if insight.repositories.is_none() && batch == "frontend" {
        tracing::error!(
            owner = %owner_name(insight.user_id, insight.org_id),
            "missing insight repositories"
        );
        return Ok(());
    }

    let existing: i64 = sqlx::query_scalar(SEARCH_COUNT_QUERY)
        .bind(&insight.id)
        .fetch_one(db)
        .await
        .map_err(|error| format!("failed to count insight views: {error}"))?;
    if existing > 0 {
        return Ok(());
    }

    let mut tx = db
        .begin()
        .await
        .map_err(|error| format!("failed to begin insights transaction: {error}"))?;

    let now = OffsetDateTime::now_utc();
    let (include_regex, exclude_regex) = match &insight.filters {
        Some(filters) => (
            filters.include_repo_regexp.clone(),
            filters.exclude_repo_regexp.clone(),
        ),
        None => (None, None),
    };

    let mut series_with_metadata = Vec::with_capacity(insight.series.len());
    for time_series in &insight.series {
        let mut series = SeriesRecord {
            id: 0,
            series_id: ksuid(),
            query: time_series.query.clone(),
            created_at: now,
            oldest_historical_at: now - TimeDuration::weeks(26),
            last_recorded_at: zero_time(),
            next_recording_after: now,
            last_snapshot_at: zero_time(),
            next_snapshot_after: now,
            // `nil` in Go -> SQL NULL (backend series carry no repositories).
            repositories: None,
            sample_interval_unit: String::new(),
            sample_interval_value: 0,
            generated_from_capture_groups: false,
            just_in_time: false,
            generation_method: "SEARCH".to_owned(),
        };

        if batch == "frontend" {
            let unit = parse_time_interval_unit(insight).to_owned();
            let value = parse_time_interval_value(insight);

            series.repositories = Some(insight.repositories.clone().unwrap_or_default());
            series.sample_interval_unit = unit.clone();
            series.sample_interval_value = value;
            series.just_in_time = true;
            series.next_snapshot_after = next_snapshot(now);
            series.next_recording_after = step_forward(now, &unit, value);
        } else {
            series.sample_interval_unit = "MONTH".to_owned();
            series.sample_interval_value = 1;
            series.just_in_time = false;
            series.next_snapshot_after = next_snapshot(now);
            series.next_recording_after = next_recording(now);
        }

        let migrated = get_or_create_series(&mut tx, series)
            .await
            .map_err(|error| format!("failed to migrate series: {error}"))?;
        let series_pk = migrated.id;

        if batch == "backend" {
            migrate_backend_series(db, &mut tx, &migrated, time_series, now).await?;
        }

        series_with_metadata.push((
            series_pk,
            time_series.name.clone(),
            time_series.stroke.clone(),
        ));
    }

    let view_id: i32 = sqlx::query_scalar(SEARCH_INSERT_VIEW_QUERY)
        .bind(&insight.title)
        .bind(&insight.description)
        .bind(&insight.id)
        .bind(include_regex)
        .bind(exclude_regex)
        .fetch_one(&mut *tx)
        .await
        .map_err(|error| format!("failed to insert view: {error}"))?;

    for (series_pk, label, stroke) in series_with_metadata {
        sqlx::query(SEARCH_INSERT_VIEW_SERIES_QUERY)
            .bind(series_pk)
            .bind(view_id)
            .bind(label)
            .bind(stroke)
            .execute(&mut *tx)
            .await
            .map_err(|error| format!("failed to insert view series: {error}"))?;
    }

    insert_view_grant(
        &mut tx,
        view_id,
        grant_tuple(insight.user_id, insight.org_id),
    )
    .await
    .map_err(|error| format!("failed to insert view grants: {error}"))?;

    tx.commit()
        .await
        .map_err(|error| format!("failed to commit insights transaction: {error}"))?;
    Ok(())
}

#[derive(Debug, Clone)]
struct SeriesRecord {
    id: i32,
    series_id: String,
    query: String,
    created_at: OffsetDateTime,
    oldest_historical_at: OffsetDateTime,
    last_recorded_at: OffsetDateTime,
    next_recording_after: OffsetDateTime,
    last_snapshot_at: OffsetDateTime,
    next_snapshot_after: OffsetDateTime,
    repositories: Option<Vec<String>>,
    sample_interval_unit: String,
    sample_interval_value: i32,
    generated_from_capture_groups: bool,
    just_in_time: bool,
    generation_method: String,
}

async fn get_or_create_series(
    tx: &mut Transaction<'_, Postgres>,
    series: SeriesRecord,
) -> Result<SeriesRecord, String> {
    let existing = sqlx::query(GET_OR_CREATE_SERIES_QUERY)
        .bind(&series.query)
        .bind(&series.sample_interval_unit)
        .bind(series.sample_interval_value)
        .bind(false)
        .fetch_optional(&mut **tx)
        .await
        .map_err(|error| format!("failed to select series: {error}"))?;

    if let Some(row) = existing {
        // Go re-uses the scanned series wholesale: importantly the existing
        // `series_id`, which backend series-point rewrites target.
        return Ok(SeriesRecord {
            id: row.get("id"),
            series_id: row.get("series_id"),
            ..series
        });
    }

    let id: i32 = sqlx::query_scalar(CREATE_SERIES_QUERY)
        .bind(&series.series_id)
        .bind(&series.query)
        .bind(series.created_at)
        .bind(series.oldest_historical_at)
        .bind(series.last_recorded_at)
        .bind(series.next_recording_after)
        .bind(series.last_snapshot_at)
        .bind(series.next_snapshot_after)
        .bind(series.repositories.as_deref())
        .bind(&series.sample_interval_unit)
        .bind(series.sample_interval_value)
        .bind(series.generated_from_capture_groups)
        .bind(series.just_in_time)
        .bind(&series.generation_method)
        .bind(Option::<String>::None)
        .fetch_one(&mut **tx)
        .await
        .map_err(|error| format!("failed to insert series: {error}"))?;

    Ok(SeriesRecord { id, ..series })
}

async fn migrate_backend_series(
    db: &PgPool,
    tx: &mut Transaction<'_, Postgres>,
    series: &SeriesRecord,
    time_series: &super::settings_parse::TimeSeries,
    now: OffsetDateTime,
) -> Result<(), String> {
    let old_id = hash_id(&time_series.query);

    let updated: i64 = match sqlx::query_scalar(UPDATE_SERIES_POINTS_QUERY)
        .bind(&series.series_id)
        .bind(&old_id)
        .fetch_one(db)
        .await
    {
        Ok(updated) => updated,
        Err(error) => {
            // Soft error: the insight transaction is preserved.
            tracing::error!(%error, "failed to update series points");
            return Ok(());
        }
    };
    if updated == 0 {
        return Ok(());
    }

    if let Err(error) = sqlx::query(UPDATE_JOBS_QUERY)
        .bind(&series.series_id)
        .bind(&old_id)
        .execute(db)
        .await
    {
        tracing::error!(%error, "failed to update seriesID on insights jobs");
        return Ok(());
    }

    sqlx::query(UPDATE_BACKFILL_QUEUED_AT_QUERY)
        .bind(now)
        .bind(series.id)
        .execute(&mut **tx)
        .await
        .map_err(|error| format!("failed to update backfill_queued_at: {error}"))?;

    Ok(())
}

async fn insert_view_grant(
    tx: &mut Transaction<'_, Postgres>,
    view_id: i32,
    grant: (Option<i32>, Option<i32>, Option<bool>),
) -> Result<(), sqlx::Error> {
    sqlx::query(INSERT_VIEW_GRANT_QUERY)
        .bind(view_id)
        .bind(grant.0)
        .bind(grant.1)
        .bind(grant.2)
        .execute(&mut **tx)
        .await?;
    Ok(())
}

/// `grantTiple`.
fn grant_tuple(
    user_id: Option<i32>,
    org_id: Option<i32>,
) -> (Option<i32>, Option<i32>, Option<bool>) {
    if let Some(user_id) = user_id {
        (Some(user_id), None, None)
    } else if let Some(org_id) = org_id {
        (None, Some(org_id), None)
    } else {
        (None, None, Some(true))
    }
}

fn owner_name(user_id: Option<i32>, org_id: Option<i32>) -> String {
    if let Some(user_id) = user_id {
        format!("user id {user_id}")
    } else if let Some(org_id) = org_id {
        format!("org id {org_id}")
    } else {
        "global".to_owned()
    }
}

/// Go's `time.Time{}` zero value, encoded by lib/pq as year 1.
fn zero_time() -> OffsetDateTime {
    Date::from_calendar_date(1, Month::January, 1)
        .expect("valid date")
        .with_time(Time::MIDNIGHT)
        .assume_utc()
}

// ---------------------------------------------------------------------------
// Dashboard migration (`dashboards.go`)
// ---------------------------------------------------------------------------

async fn migrate_dashboard(
    db: &PgPool,
    job: MigrationJob,
    dashboard: &SettingDashboard,
    unique_id_suffix: &str,
) -> Result<(), String> {
    if dashboard.id.is_empty() {
        tracing::warn!(
            owner = %owner_name(dashboard.user_id, dashboard.org_id),
            "missing dashboard identifier"
        );
        return Ok(());
    }

    let mut tx = db
        .begin()
        .await
        .map_err(|error| format!("failed to begin dashboard transaction: {error}"))?;

    let mut count_query =
        sqlx::query_scalar(dashboard_count_query(dashboard)).bind(&dashboard.title);
    if let Some(value) = dashboard_grant_value(dashboard) {
        count_query = count_query.bind(value);
    }
    let existing: i64 = count_query
        .fetch_one(&mut *tx)
        .await
        .map_err(|error| format!("failed to count dashboards: {error}"))?;
    if existing != 0 {
        return Ok(());
    }

    create_dashboard(
        &mut tx,
        job,
        &dashboard.title,
        &dashboard.insight_ids,
        unique_id_suffix,
    )
    .await?;

    tx.commit()
        .await
        .map_err(|error| format!("failed to commit dashboard transaction: {error}"))?;
    Ok(())
}

fn dashboard_count_query(dashboard: &SettingDashboard) -> &'static str {
    if dashboard.user_id.is_some() {
        DASHBOARD_COUNT_USER_QUERY
    } else if dashboard.org_id.is_some() {
        DASHBOARD_COUNT_ORG_QUERY
    } else {
        DASHBOARD_COUNT_GLOBAL_QUERY
    }
}

fn dashboard_grant_value(dashboard: &SettingDashboard) -> Option<i32> {
    dashboard.user_id.or(dashboard.org_id)
}

async fn create_dashboard(
    tx: &mut Transaction<'_, Postgres>,
    job: MigrationJob,
    title: &str,
    insight_ids: &[String],
    unique_id_suffix: &str,
) -> Result<(), String> {
    let mut unique_ids = Vec::with_capacity(insight_ids.len());
    for insight_id in insight_ids {
        let pattern = format!("{insight_id}-{unique_id_suffix}");
        let unique_id: Option<String> = sqlx::query_scalar(CREATE_DASHBOARD_SELECT_QUERY)
            .bind(insight_id)
            .bind(pattern)
            .fetch_optional(&mut **tx)
            .await
            .map_err(|error| format!("failed to retrieve unique id of insight view: {error}"))?;
        unique_ids.push(unique_id.unwrap_or_default());
    }

    let dashboard_id: i32 = sqlx::query_scalar(CREATE_DASHBOARD_QUERY)
        .bind(title)
        .fetch_one(&mut **tx)
        .await
        .map_err(|error| format!("failed to insert dashboard: {error}"))?;

    if !unique_ids.is_empty() {
        let orderings: Vec<i32> = (0..unique_ids.len() as i32).collect();
        sqlx::query(CREATE_DASHBOARD_INSIGHT_VIEW_QUERY)
            .bind(dashboard_id)
            .bind(unique_ids.as_slice())
            .bind(orderings.as_slice())
            .execute(&mut **tx)
            .await
            .map_err(|error| format!("failed to insert dashboard insight view: {error}"))?;
    }

    let grant = grant_tuple(job.user_id, job.org_id);
    sqlx::query(CREATE_DASHBOARD_GRANT_QUERY)
        .bind(dashboard_id)
        .bind(grant.0)
        .bind(grant.1)
        .bind(grant.2)
        .execute(&mut **tx)
        .await
        .map_err(|error| format!("failed to insert dashboard grants: {error}"))?;

    Ok(())
}

async fn create_special_case_dashboard(
    db: &PgPool,
    job: MigrationJob,
    subject_name: &str,
    insight_ids: &[String],
    unique_id_suffix: &str,
) -> Result<(), MigratorError> {
    let mut tx = db.begin().await.map_err(|error| {
        MigratorError::new(format!(
            "failed to begin special dashboard transaction: {error}"
        ))
    })?;

    create_dashboard(
        &mut tx,
        job,
        &special_case_dashboard_name(subject_name),
        insight_ids,
        unique_id_suffix,
    )
    .await
    .map_err(MigratorError::new)?;

    tx.commit().await.map_err(|error| {
        MigratorError::new(format!(
            "failed to commit special dashboard transaction: {error}"
        ))
    })?;
    Ok(())
}

/// `makeUniqueIDSuffix`: `"%(user-1|org-2)%"`.
async fn make_unique_id_suffix(
    tx: &mut Transaction<'_, Postgres>,
    job: MigrationJob,
) -> Result<String, MigratorError> {
    let (user_id, org_ids) = if let Some(user_id) = job.user_id {
        let rows = sqlx::query(USER_ORGS_QUERY)
            .bind(user_id)
            .fetch_all(&mut **tx)
            .await
            .map_err(|error| MigratorError::new(format!("failed to select user orgs: {error}")))?;
        let mut org_ids = Vec::with_capacity(rows.len());
        for row in rows {
            org_ids.push(
                row.try_get::<i32, _>("id")
                    .map_err(|error| MigratorError::new(format!("user org id: {error}")))?,
            );
        }
        (user_id, org_ids)
    } else if let Some(org_id) = job.org_id {
        (0, vec![org_id])
    } else {
        (0, Vec::new())
    };

    let mut targets = Vec::with_capacity(org_ids.len() + 1);
    if user_id != 0 {
        targets.push(format!("user-{user_id}"));
    }
    for org_id in org_ids {
        targets.push(format!("org-{org_id}"));
    }

    Ok(format!("%({})%", targets.join("|")))
}

// ---------------------------------------------------------------------------
// SQL
// ---------------------------------------------------------------------------

const LANG_STATS_COUNT_QUERY: &str = r#"
SELECT COUNT(*)
FROM (SELECT * FROM insight_view WHERE unique_id = $1 ORDER BY unique_id) iv
JOIN insight_view_series ivs ON iv.id = ivs.insight_view_id
JOIN insight_series i ON ivs.insight_series_id = i.id
WHERE i.deleted_at IS NULL
"#;

const LANG_STATS_INSERT_VIEW_QUERY: &str = r#"
INSERT INTO insight_view (title, unique_id, other_threshold, presentation_type)
VALUES ($1, $2, $3, 'PIE')
RETURNING id
"#;

const LANG_STATS_INSERT_SERIES_QUERY: &str = r#"
INSERT INTO insight_series (
	series_id,
	query,
	created_at,
	oldest_historical_at,
	next_recording_after,
	next_snapshot_after,
	repositories,
	sample_interval_unit,
	just_in_time,
	generation_method,
	needs_migration
)
VALUES ($1, '', $2, $3, $4, $5, $6, 'MONTH', true, 'language-stats', false)
RETURNING id
"#;

const LANG_STATS_INSERT_VIEW_SERIES_QUERY: &str = r#"
INSERT INTO insight_view_series (insight_series_id, insight_view_id, label, stroke)
VALUES ($1, $2, '', '')
"#;

const SEARCH_COUNT_QUERY: &str = r#"
SELECT COUNT(*)
FROM (
	SELECT *
	FROM insight_view
	WHERE unique_id = $1
	ORDER BY unique_id
) iv
JOIN insight_view_series ivs ON iv.id = ivs.insight_view_id
JOIN insight_series i ON ivs.insight_series_id = i.id
WHERE i.deleted_at IS NULL
"#;

const SEARCH_INSERT_VIEW_QUERY: &str = r#"
INSERT INTO insight_view (
	title,
	description,
	unique_id,
	default_filter_include_repo_regex,
	default_filter_exclude_repo_regex,
	presentation_type
)
VALUES ($1, $2, $3, $4, $5, 'LINE')
RETURNING id
"#;

const SEARCH_INSERT_VIEW_SERIES_QUERY: &str = r#"
INSERT INTO insight_view_series (insight_series_id, insight_view_id, label, stroke)
VALUES ($1, $2, $3, $4)
"#;

const INSERT_VIEW_GRANT_QUERY: &str = r#"
INSERT INTO insight_view_grants (insight_view_id, user_id, org_id, global)
VALUES ($1, $2, $3, $4)
"#;

const GET_OR_CREATE_SERIES_QUERY: &str = r#"
SELECT
	id,
	series_id,
	query,
	created_at,
	oldest_historical_at,
	last_recorded_at,
	next_recording_after,
	last_snapshot_at,
	next_snapshot_after,
	sample_interval_unit,
	sample_interval_value,
	generated_from_capture_groups,
	just_in_time,
	generation_method,
	repositories,
	group_by
FROM insight_series
WHERE
	(repositories = '{}' OR repositories is NULL) AND
	query = $1 AND
	sample_interval_unit = $2::time_unit AND
	sample_interval_value = $3 AND
	generated_from_capture_groups = $4 AND
	group_by IS NULL
"#;

const CREATE_SERIES_QUERY: &str = r#"
INSERT INTO insight_series (
	series_id,
	query,
	created_at,
	oldest_historical_at,
	last_recorded_at,
	next_recording_after,
	last_snapshot_at,
	next_snapshot_after,
	repositories,
	sample_interval_unit,
	sample_interval_value,
	generated_from_capture_groups,
	just_in_time,
	generation_method,
	group_by,
	needs_migration
)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10::time_unit, $11, $12, $13, $14, $15, false)
RETURNING id
"#;

const UPDATE_SERIES_POINTS_QUERY: &str = r#"
WITH updated AS (
	UPDATE series_points sp
	SET series_id = $1
	WHERE series_id = $2
	RETURNING sp.series_id
)
SELECT count(*) FROM updated
"#;

const UPDATE_JOBS_QUERY: &str = r#"
UPDATE insights_query_runner_jobs SET series_id = $1 WHERE series_id = $2
"#;

const UPDATE_BACKFILL_QUEUED_AT_QUERY: &str = r#"
UPDATE insight_series SET backfill_queued_at = $1 WHERE id = $2
"#;

const DASHBOARD_COUNT_USER_QUERY: &str = r#"
SELECT COUNT(*) from dashboard
JOIN dashboard_grants dg ON dashboard.id = dg.dashboard_id
WHERE dashboard.title = $1 AND dg.user_id = $2
"#;

const DASHBOARD_COUNT_ORG_QUERY: &str = r#"
SELECT COUNT(*) from dashboard
JOIN dashboard_grants dg ON dashboard.id = dg.dashboard_id
WHERE dashboard.title = $1 AND dg.org_id = $2
"#;

const DASHBOARD_COUNT_GLOBAL_QUERY: &str = r#"
SELECT COUNT(*) from dashboard
JOIN dashboard_grants dg ON dashboard.id = dg.dashboard_id
WHERE dashboard.title = $1 AND dg.global IS TRUE
"#;

const CREATE_DASHBOARD_SELECT_QUERY: &str = r#"
SELECT unique_id
FROM insight_view
WHERE unique_id = $1 OR unique_id SIMILAR TO $2
LIMIT 1
"#;

const CREATE_DASHBOARD_QUERY: &str = r#"
INSERT INTO dashboard (title, save, type)
VALUES ($1, true, 'standard')
RETURNING id
"#;

const CREATE_DASHBOARD_INSIGHT_VIEW_QUERY: &str = r#"
INSERT INTO dashboard_insight_view (dashboard_id, insight_view_id)
SELECT $1 AS dashboard_id, insight_view.id AS insight_view_id
FROM insight_view
JOIN UNNEST($2::text[], $3::integer[]) AS ids (id, ordering)
	ON ids.id = insight_view.unique_id
WHERE unique_id = ANY($2)
ORDER BY ids.ordering
ON CONFLICT DO NOTHING
"#;

const CREATE_DASHBOARD_GRANT_QUERY: &str = r#"
INSERT INTO dashboard_grants (dashboard_id, user_id, org_id, global) VALUES ($1, $2, $3, $4)
"#;

const USER_ORGS_QUERY: &str = r#"
SELECT orgs.id
FROM org_members
LEFT OUTER JOIN orgs ON org_members.org_id = orgs.id
WHERE user_id = $1 AND orgs.deleted_at IS NULL
"#;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn job_conditions_match_the_go_shapes() {
        assert_eq!(
            job_condition(MigrationJob {
                user_id: Some(5),
                org_id: None,
                migrated_insights: 0,
                migrated_dashboards: 0,
            }),
            ("user_id = $1", Some(5))
        );
        assert_eq!(
            job_condition(MigrationJob {
                user_id: None,
                org_id: Some(6),
                migrated_insights: 0,
                migrated_dashboards: 0,
            }),
            ("org_id = $1", Some(6))
        );
        assert_eq!(
            job_condition(MigrationJob {
                user_id: None,
                org_id: None,
                migrated_insights: 0,
                migrated_dashboards: 0,
            }),
            ("global IS TRUE", None)
        );
    }

    #[test]
    fn grant_tuples_match_the_go_shapes() {
        assert_eq!(grant_tuple(Some(1), None), (Some(1), None, None));
        assert_eq!(grant_tuple(None, Some(2)), (None, Some(2), None));
        assert_eq!(grant_tuple(None, None), (None, None, Some(true)));
    }

    #[test]
    fn dashboard_queries_are_selected_by_owner() {
        let user = SettingDashboard {
            user_id: Some(1),
            ..Default::default()
        };
        assert_eq!(dashboard_count_query(&user), DASHBOARD_COUNT_USER_QUERY);
        assert_eq!(dashboard_grant_value(&user), Some(1));

        let org = SettingDashboard {
            org_id: Some(2),
            ..Default::default()
        };
        assert_eq!(dashboard_count_query(&org), DASHBOARD_COUNT_ORG_QUERY);
        assert_eq!(dashboard_grant_value(&org), Some(2));

        let global = SettingDashboard::default();
        assert_eq!(dashboard_count_query(&global), DASHBOARD_COUNT_GLOBAL_QUERY);
        assert_eq!(dashboard_grant_value(&global), None);
    }

    #[test]
    fn zero_time_is_year_one() {
        assert_eq!(zero_time().year(), 1);
        assert_eq!(zero_time().month(), Month::January);
        assert_eq!(zero_time().day(), 1);
    }

    #[tokio::test]
    #[ignore = "requires Postgres (set DATABASE_URL)"]
    async fn progress_reflects_completed_jobs() {
        let dsn = std::env::var("DATABASE_URL")
            .unwrap_or_else(|_| "postgres://localhost/postgres".to_owned());
        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(1)
            .connect(&dsn)
            .await
            .expect("connect to Postgres");

        sqlx::query(
            r#"CREATE TEMP TABLE insights_settings_migration_jobs (
                id integer PRIMARY KEY,
                completed_at timestamptz
            )"#,
        )
        .execute(&pool)
        .await
        .expect("create temp table");
        sqlx::query(
            r#"INSERT INTO insights_settings_migration_jobs (id, completed_at)
               VALUES (1, now()), (2, NULL)"#,
        )
        .execute(&pool)
        .await
        .expect("insert jobs");

        let progress = SettingsMigrationMigrator
            .progress(&pool, false)
            .await
            .expect("progress");
        assert!((progress - 0.5).abs() < f64::EPSILON);
    }
}
