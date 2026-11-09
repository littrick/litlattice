//! `internal/insights/scheduler` storage: the backfill records and the
//! persistent repository iterator.
//!
//! This maps the SQL from `internal/insights/scheduler/backfill.go`,
//! `scheduler.go` and `internal/insights/scheduler/iterator/repo_iterator.go`
//! onto the existing `codeinsights` schema. It introduces no tables, columns
//! or Redis keys: `insight_series_backfill`, `repo_iterator`,
//! `repo_iterator_errors` and `insights_background_jobs` are the M6 storage
//! contract, and their `id` sequences are used exactly as the baseline
//! (`nextval` defaults, no manual id assignment).
//!
//! The pure decisions (state enum, error threshold, retry walk) live in
//! `sg-insights`; this module deliberately keeps its row structs close to the
//! Go ones so the storage boundary can be translated field by field.
//!
//! Not ported here: `BackfillStore.GetBackfillQueueInfo`/`GetBackfillQueueTotalCount`
//! (the paged admin-queue SQL) and the worker wiring. They will follow with
//! the backfill worker port.

use sqlx::postgres::{PgConnection, PgPool, PgRow};
use sqlx::types::time::{OffsetDateTime, PrimitiveDateTime, UtcOffset};
use sqlx::Row;

const BACKFILL_COLUMNS: &str = "id, series_id, repo_iterator_id, estimated_cost, state";

/// `scheduler.SeriesBackfill` as stored. `estimated_cost` is `NULL` until the
/// new-state handler scopes the backfill; Go scans it into a zero value.
#[derive(Debug, Clone, PartialEq)]
pub struct SeriesBackfill {
    pub id: i32,
    pub series_id: i32,
    pub repo_iterator_id: Option<i32>,
    pub estimated_cost: f64,
    pub state: String,
}

fn series_backfill_from_row(row: &PgRow) -> Result<SeriesBackfill, sqlx::Error> {
    let cost: Option<f64> = row.try_get("estimated_cost")?;
    Ok(SeriesBackfill {
        id: row.try_get("id")?,
        series_id: row.try_get("series_id")?,
        repo_iterator_id: row.try_get("repo_iterator_id")?,
        estimated_cost: cost.unwrap_or(0.0),
        state: row.try_get("state")?,
    })
}

/// `iterator.IterationError` as stored. The Go struct keeps the id unexported.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IterationError {
    pub id: i32,
    pub repo_id: i32,
    pub failure_count: i32,
    pub error_messages: Vec<String>,
}

/// `iterator.PersistentRepoIterator` as stored (the pure state methods are in
/// `sg-insights`).
///
/// The time columns are `timestamp without time zone`; like the other stores
/// here they are represented as [`PrimitiveDateTime`] (the UTC wall clock that
/// lib/pq sends), not `OffsetDateTime`.
#[derive(Debug, Clone, PartialEq)]
pub struct RepoIterator {
    pub id: i32,
    pub created_at: Option<PrimitiveDateTime>,
    pub started_at: Option<PrimitiveDateTime>,
    pub completed_at: Option<PrimitiveDateTime>,
    /// `time.Duration` in nanoseconds.
    pub runtime_duration: i64,
    pub percent_complete: f64,
    pub total_count: i32,
    pub success_count: i32,
    pub repos: Vec<i32>,
    pub cursor: i32,
    pub errors: Vec<IterationError>,
}

// -------------------------------------------------------------------------
// BackfillStore
// -------------------------------------------------------------------------

/// `BackfillStore.NewBackfill`.
pub async fn new_backfill(pool: &PgPool, series_id: i32) -> Result<SeriesBackfill, sqlx::Error> {
    let mut conn = pool.acquire().await?;
    insert_backfill(&mut conn, series_id).await
}

async fn insert_backfill(
    conn: &mut PgConnection,
    series_id: i32,
) -> Result<SeriesBackfill, sqlx::Error> {
    let row = sqlx::query(&format!(
        "INSERT INTO insight_series_backfill (series_id, state) VALUES($1, $2) \
         RETURNING {BACKFILL_COLUMNS}"
    ))
    .bind(series_id)
    .bind("new")
    .fetch_one(conn)
    .await?;
    series_backfill_from_row(&row)
}

/// `BackfillStore.LoadBackfill`.
pub async fn load_backfill(pool: &PgPool, id: i32) -> Result<SeriesBackfill, sqlx::Error> {
    let mut conn = pool.acquire().await?;
    load_backfill_on(&mut conn, id).await
}

async fn load_backfill_on(conn: &mut PgConnection, id: i32) -> Result<SeriesBackfill, sqlx::Error> {
    let row = sqlx::query(&format!(
        "SELECT {BACKFILL_COLUMNS} FROM insight_series_backfill WHERE id = $1"
    ))
    .bind(id)
    .fetch_one(conn)
    .await?;
    series_backfill_from_row(&row)
}

/// `BackfillStore.LoadSeriesBackfills`.
pub async fn load_series_backfills(
    pool: &PgPool,
    series_id: i32,
) -> Result<Vec<SeriesBackfill>, sqlx::Error> {
    let rows = sqlx::query(&format!(
        "SELECT {BACKFILL_COLUMNS} FROM insight_series_backfill WHERE series_id = $1"
    ))
    .bind(series_id)
    .fetch_all(pool)
    .await?;
    rows.iter().map(series_backfill_from_row).collect()
}

async fn set_state_on(
    conn: &mut PgConnection,
    id: i32,
    state: &str,
) -> Result<SeriesBackfill, sqlx::Error> {
    sqlx::query("UPDATE insight_series_backfill SET state = $1 WHERE id = $2")
        .bind(state)
        .bind(id)
        .execute(&mut *conn)
        .await?;
    load_backfill_on(conn, id).await
}

/// `SeriesBackfill.SetCompleted`.
pub async fn set_completed(pool: &PgPool, id: i32) -> Result<SeriesBackfill, sqlx::Error> {
    let mut conn = pool.acquire().await?;
    set_state_on(&mut conn, id, "completed").await
}

/// `SeriesBackfill.SetFailed`.
pub async fn set_failed(pool: &PgPool, id: i32) -> Result<SeriesBackfill, sqlx::Error> {
    let mut conn = pool.acquire().await?;
    set_state_on(&mut conn, id, "failed").await
}

/// `SeriesBackfill.SetScope`: create the repo iterator and point the backfill
/// at it, moving the backfill to `processing` (one transaction, as in Go).
pub async fn set_scope(
    pool: &PgPool,
    backfill_id: i32,
    repos: &[i32],
    cost: f64,
    now: OffsetDateTime,
) -> Result<(SeriesBackfill, RepoIterator), sqlx::Error> {
    let mut tx = pool.begin().await?;
    let iterator_id = insert_iterator(&mut tx, repos, now).await?;
    sqlx::query(
        "UPDATE insight_series_backfill SET repo_iterator_id = $1, estimated_cost = $2, \
         state = $3 WHERE id = $4",
    )
    .bind(iterator_id)
    .bind(cost)
    .bind("processing")
    .bind(backfill_id)
    .execute(&mut *tx)
    .await?;
    let backfill = load_backfill_on(&mut tx, backfill_id).await?;
    let iterator = load_iterator_on(&mut tx, iterator_id).await?;
    tx.commit().await?;
    Ok((backfill, iterator))
}

/// `SeriesBackfill.SetLowestPriority`: if this backfill is not already the
/// lowest priority among `new`/`processing`, double the current maximum cost.
/// Returns the stored cost after the operation.
pub async fn set_lowest_priority(
    pool: &PgPool,
    id: i32,
    current_cost: f64,
) -> Result<f64, sqlx::Error> {
    let mut tx = pool.begin().await?;
    let max: f64 = sqlx::query_scalar(
        "SELECT coalesce(max(estimated_cost), 0) FROM insight_series_backfill \
         WHERE state in ('new','processing') AND id != $1",
    )
    .bind(id)
    .fetch_one(&mut *tx)
    .await?;

    // Like Go, the comparison uses the caller's in-memory cost, and an
    // already-lowest backfill is left alone.
    if current_cost >= max {
        tx.commit().await?;
        return Ok(current_cost);
    }
    let new_cost = max * 2.0;
    sqlx::query("UPDATE insight_series_backfill SET estimated_cost = $1 WHERE id = $2")
        .bind(new_cost)
        .bind(id)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(new_cost)
}

/// `SeriesBackfill.SetHighestPriority`: set the cost to 0.
pub async fn set_highest_priority(pool: &PgPool, id: i32) -> Result<(), sqlx::Error> {
    sqlx::query("UPDATE insight_series_backfill SET estimated_cost = $1 WHERE id = $2")
        .bind(0.0_f64)
        .bind(id)
        .execute(pool)
        .await?;
    Ok(())
}

/// `SeriesBackfill.RetryBackfillAttempt`: restart the iterator, move the
/// backfill to `processing` and enqueue the next job, atomically.
pub async fn retry_backfill_attempt(pool: &PgPool, id: i32) -> Result<(), sqlx::Error> {
    let mut tx = pool.begin().await?;
    let backfill = load_backfill_on(&mut tx, id).await?;
    let iterator_id = backfill.repo_iterator_id.unwrap_or(0);
    if iterator_id == 0 {
        return Err(invalid_series_backfill(&format!(
            "invalid repo_iterator_id on backfill_id: {id}"
        )));
    }
    restart_iterator_on(&mut tx, iterator_id).await?;
    set_state_on(&mut tx, id, "processing").await?;
    enqueue_backfill_on(&mut tx, id).await?;
    tx.commit().await?;
    Ok(())
}

/// `Scheduler.InitialBackfill`: create the backfill and enqueue its first
/// job atomically.
pub async fn initial_backfill(
    pool: &PgPool,
    series_id: i32,
) -> Result<SeriesBackfill, sqlx::Error> {
    let mut tx = pool.begin().await?;
    let backfill = insert_backfill(&mut tx, series_id).await?;
    enqueue_backfill_on(&mut tx, backfill.id).await?;
    tx.commit().await?;
    Ok(backfill)
}

/// `enqueueBackfill`.
pub async fn enqueue_backfill(pool: &PgPool, backfill_id: i32) -> Result<(), sqlx::Error> {
    let mut conn = pool.acquire().await?;
    enqueue_backfill_on(&mut conn, backfill_id).await
}

async fn enqueue_backfill_on(conn: &mut PgConnection, backfill_id: i32) -> Result<(), sqlx::Error> {
    if backfill_id == 0 {
        return Err(invalid_series_backfill("invalid series backfill"));
    }
    sqlx::query("INSERT INTO insights_background_jobs (backfill_id) VALUES($1)")
        .bind(backfill_id)
        .execute(conn)
        .await?;
    Ok(())
}

fn invalid_series_backfill(message: &str) -> sqlx::Error {
    sqlx::Error::Protocol(message.to_owned())
}

// -------------------------------------------------------------------------
// PersistentRepoIterator
// -------------------------------------------------------------------------

fn utc_wall_clock(time: OffsetDateTime) -> PrimitiveDateTime {
    let utc = time.to_offset(UtcOffset::UTC);
    PrimitiveDateTime::new(utc.date(), utc.time())
}

const ITERATOR_COLUMNS: &str = "id, created_at, started_at, completed_at, runtime_duration, \
     percent_complete, total_count, success_count, repos, repo_cursor";

fn repo_iterator_from_row(row: &PgRow) -> Result<RepoIterator, sqlx::Error> {
    let repos: Option<Vec<i32>> = row.try_get("repos")?;
    let cursor: i32 = row.try_get("repo_cursor")?;
    let repos = repos.unwrap_or_default();
    if cursor > repos.len() as i32 {
        return Err(sqlx::Error::Protocol(format!(
            "invalid repo iterator state Id:{} cursor:{} length:{}",
            row.try_get::<i32, _>("id")?,
            cursor,
            repos.len()
        )));
    }
    Ok(RepoIterator {
        id: row.try_get("id")?,
        created_at: row.try_get("created_at")?,
        started_at: row.try_get("started_at")?,
        completed_at: row.try_get("completed_at")?,
        runtime_duration: row.try_get("runtime_duration")?,
        percent_complete: row.try_get("percent_complete")?,
        total_count: row.try_get("total_count")?,
        success_count: row.try_get("success_count")?,
        repos,
        cursor,
        errors: Vec::new(),
    })
}

async fn insert_iterator(
    conn: &mut PgConnection,
    repos: &[i32],
    created_at: OffsetDateTime,
) -> Result<i32, sqlx::Error> {
    sqlx::query_scalar(
        "INSERT INTO repo_iterator(repos, total_count, created_at) VALUES ($1, $2, $3) \
         RETURNING id",
    )
    .bind(repos)
    .bind(repos.len() as i32)
    .bind(utc_wall_clock(created_at))
    .fetch_one(conn)
    .await
}

/// `iterator.New` (`NewWithClock` with an explicit creation time).
pub async fn new_iterator(
    pool: &PgPool,
    repos: &[i32],
    created_at: OffsetDateTime,
) -> Result<RepoIterator, sqlx::Error> {
    let mut conn = pool.acquire().await?;
    let id = insert_iterator(&mut conn, repos, created_at).await?;
    load_iterator_on(&mut conn, id).await
}

/// `iterator.Load` (also loads `repo_iterator_errors`).
pub async fn load_iterator(pool: &PgPool, id: i32) -> Result<RepoIterator, sqlx::Error> {
    let mut conn = pool.acquire().await?;
    load_iterator_on(&mut conn, id).await
}

async fn load_iterator_on(conn: &mut PgConnection, id: i32) -> Result<RepoIterator, sqlx::Error> {
    let row = sqlx::query(&format!(
        "SELECT {ITERATOR_COLUMNS} FROM repo_iterator WHERE repo_iterator.id = $1"
    ))
    .bind(id)
    .fetch_one(&mut *conn)
    .await?;
    let iterator = repo_iterator_from_row(&row)?;
    iterator_with_errors(conn, iterator).await
}

async fn iterator_with_errors(
    conn: &mut PgConnection,
    mut iterator: RepoIterator,
) -> Result<RepoIterator, sqlx::Error> {
    let rows = sqlx::query(
        "SELECT id, repo_id, error_message, failure_count FROM repo_iterator_errors \
         WHERE repo_iterator_id = $1",
    )
    .bind(iterator.id)
    .fetch_all(conn)
    .await?;
    iterator.errors = rows
        .iter()
        .map(|row| {
            Ok::<_, sqlx::Error>(IterationError {
                id: row.try_get("id")?,
                repo_id: row.try_get("repo_id")?,
                failure_count: row.try_get("failure_count")?,
                error_messages: row.try_get("error_message")?,
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(iterator)
}

/// `PersistentRepoIterator.MarkComplete`.
pub async fn mark_complete(
    pool: &PgPool,
    id: i32,
    now: OffsetDateTime,
) -> Result<RepoIterator, sqlx::Error> {
    let mut conn = pool.acquire().await?;
    mark_complete_on(&mut conn, id, now).await?;
    load_iterator_on(&mut conn, id).await
}

async fn mark_complete_on(
    conn: &mut PgConnection,
    id: i32,
    now: OffsetDateTime,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "UPDATE repo_iterator SET percent_complete = 1, completed_at = $1, \
         last_updated_at = $2 where id = $3",
    )
    .bind(utc_wall_clock(now))
    .bind(utc_wall_clock(now))
    .bind(id)
    .execute(conn)
    .await?;
    Ok(())
}

/// `stampStartedAt`: record the first work time on the iterator.
pub async fn stamp_started_at(
    pool: &PgPool,
    id: i32,
    started_at: OffsetDateTime,
) -> Result<(), sqlx::Error> {
    sqlx::query("UPDATE repo_iterator SET started_at = $1 WHERE Id = $2")
        .bind(started_at)
        .bind(id)
        .execute(pool)
        .await?;
    Ok(())
}

/// `PersistentRepoIterator.Restart`.
pub async fn restart_iterator(pool: &PgPool, id: i32) -> Result<RepoIterator, sqlx::Error> {
    let mut tx = pool.begin().await?;
    restart_iterator_on(&mut tx, id).await?;
    let iterator = load_iterator_on(&mut tx, id).await?;
    tx.commit().await?;
    Ok(iterator)
}

async fn restart_iterator_on(conn: &mut PgConnection, id: i32) -> Result<(), sqlx::Error> {
    sqlx::query(
        "UPDATE repo_iterator SET percent_complete = 0, runtime_duration = 0, success_count = 0, \
         repo_cursor = 0, completed_at = null, started_at = null, last_updated_at = now() \
         where id = $1",
    )
    .bind(id)
    .execute(&mut *conn)
    .await?;
    // Erratum (baseline quirk, kept for compatibility): `iterator.Restart`
    // deletes `repo_iterator_errors` rows by primary key rather than by
    // `repo_iterator_id`, so it usually deletes nothing. Do not "fix" this
    // without changing the Go baseline too.
    sqlx::query("DELETE FROM repo_iterator_errors WHERE id = $1")
        .bind(id)
        .execute(conn)
        .await?;
    Ok(())
}

/// `PersistentRepoIterator.updateRepoIterator`: bump the success count,
/// cursor and runtime, then recompute the percentage in SQL exactly as the
/// baseline does (`COALESCE(... / NULLIF(total_count, 0), 0)`).
pub async fn update_repo_iterator(
    pool: &PgPool,
    id: i32,
    success_count: i32,
    cursor_offset: i32,
    runtime_duration: i64,
) -> Result<RepoIterator, sqlx::Error> {
    let mut conn = pool.acquire().await?;
    let row = sqlx::query(
        "UPDATE repo_iterator \
         SET percent_complete = COALESCE((($1 + success_count)::float / NULLIF(total_count, 0)::float), 0), \
         success_count = success_count + $1, \
         repo_cursor = repo_cursor + $2, \
         last_updated_at = NOW(), \
         runtime_duration = runtime_duration + $3 \
         WHERE id = $4 RETURNING percent_complete, success_count, repo_cursor, runtime_duration",
    )
    .bind(success_count)
    .bind(cursor_offset)
    .bind(runtime_duration)
    .bind(id)
    .fetch_one(&mut *conn)
    .await?;
    let mut iterator = load_iterator_on(&mut conn, id).await?;
    // Keep the returned progress fields (the pure callers read these).
    iterator.percent_complete = row.try_get("percent_complete")?;
    iterator.success_count = row.try_get("success_count")?;
    iterator.cursor = row.try_get("repo_cursor")?;
    iterator.runtime_duration = row.try_get("runtime_duration")?;
    Ok(iterator)
}

/// `insertIterationError`: insert the first error for a repo (failure count
/// 1), otherwise bump the stored failure count and append the message.
/// `errors` is the in-memory error map the caller loaded; like the baseline,
/// the in-memory `error_messages` is not extended on the update path.
pub async fn insert_iteration_error(
    pool: &PgPool,
    iterator_id: i32,
    errors: &mut Vec<IterationError>,
    repo_id: i32,
    message: &str,
) -> Result<(), sqlx::Error> {
    if iterator_id == 0 {
        return Err(invalid_series_backfill(
            "invalid iterator to insert iterator error",
        ));
    }
    let mut conn = pool.acquire().await?;
    if let Some(existing) = errors.iter_mut().find(|err| err.repo_id == repo_id) {
        existing.failure_count += 1;
        sqlx::query(
            "UPDATE repo_iterator_errors SET failure_count = $1, \
             error_message = array_append(error_message, $2) WHERE id = $3",
        )
        .bind(existing.failure_count)
        .bind(message)
        .bind(existing.id)
        .execute(&mut *conn)
        .await?;
        return Ok(());
    }
    let row = sqlx::query(
        "INSERT INTO repo_iterator_errors(repo_iterator_id, repo_id, error_message) \
         VALUES ($1, $2, $3) RETURNING id, repo_id, error_message, failure_count",
    )
    .bind(iterator_id)
    .bind(repo_id)
    .bind(vec![message.to_owned()])
    .fetch_one(&mut *conn)
    .await?;
    errors.push(IterationError {
        id: row.try_get("id")?,
        repo_id: row.try_get("repo_id")?,
        failure_count: row.try_get("failure_count")?,
        error_messages: row.try_get("error_message")?,
    });
    Ok(())
}

/// `doFinishN`'s delete path: drop a repo's recorded error after a retry
/// succeeds. The caller looks up the error id.
pub async fn delete_iteration_error(pool: &PgPool, error_id: i32) -> Result<(), sqlx::Error> {
    sqlx::query("DELETE FROM repo_iterator_errors WHERE id = $1")
        .bind(error_id)
        .execute(pool)
        .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn series_backfill_scan_shape() {
        let backfill = SeriesBackfill {
            id: 1,
            series_id: 2,
            repo_iterator_id: None,
            estimated_cost: 0.0,
            state: "new".to_owned(),
        };
        assert_eq!(backfill.state, "new");
        assert!(backfill.repo_iterator_id.is_none());
    }

    #[test]
    fn iterator_shape() {
        let iterator = RepoIterator {
            id: 1,
            created_at: None,
            started_at: None,
            completed_at: None,
            runtime_duration: 0,
            percent_complete: 0.0,
            total_count: 2,
            success_count: 0,
            repos: vec![1, 2],
            cursor: 0,
            errors: vec![IterationError {
                id: 1,
                repo_id: 1,
                failure_count: 1,
                error_messages: vec!["boom".to_owned()],
            }],
        };
        assert_eq!(iterator.repos.len() as i32, iterator.total_count);
        assert_eq!(iterator.errors[0].failure_count, 1);
    }
}
