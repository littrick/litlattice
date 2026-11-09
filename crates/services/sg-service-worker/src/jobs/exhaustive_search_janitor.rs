#![forbid(unsafe_code)]

//! Exhaustive search janitor (`cmd/worker/internal/search/janitor.go`) plus
//! the portable half of the service it depends on
//! (`service.GetSearchJobLogsWriterTo` / `writeSearchJobLogs`, CSV upload and
//! delete).
//!
//! The janitor lists the search jobs that have not been aggregated yet. For
//! every job whose *aggregate* state is terminal (`completed`, `failed`,
//! `canceled`) it runs one transaction that
//!
//! 1. writes the aggregate state onto `exhaustive_search_jobs.state`,
//! 2. streams the job logs as CSV and uploads them under `log-<id>.csv`,
//! 3. deletes the job's `exhaustive_search_repo_jobs` rows (revision jobs are
//!    removed by `ON DELETE CASCADE`),
//! 4. sets `is_aggregated = true`.
//!
//! If a step fails, the transaction is rolled back and the already-uploaded
//! logs are deleted best-effort, exactly like the baseline's
//! `db.WithTransact` + `svc.DeleteJobLogs` pair. Errors are accumulated across
//! jobs with `errors.Append`.
//!
//! The baseline passes a context carrying the job's initiator as the actor
//! (`actor.WithActor(ctx, actor.FromUser(job.Initiator))`). Rust has no actor
//! context, so [`JanitorService`] takes the initiator id explicitly; the
//! bundled [`PgJanitorService`] reproduces the store's access checks with
//! `Actor::user(initiator_id)`.
//!
//! Not ported: the concrete object storage (`internal/object`), which is
//! behind the injected [`JobLogsBlobStore`] trait.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use sg_db::exhaustive_search::{self as ex, Actor, GetJobLogsOptions, JobState, SearchJobLog};
use sg_db::OffsetDateTime;
use sqlx::{PgPool, Postgres, Transaction};
use tokio_util::sync::CancellationToken;

use crate::framework::periodic::{Handler, HandlerError, PeriodicRoutine};
use crate::framework::BackgroundRoutine;
use crate::jobs::exhaustive_search_job::append_errors;

/// `newJanitorJob`'s `goroutine.WithName`.
pub const JANITOR_NAME: &str = "search_jobs_janitor";
/// `newJanitorJob`'s `goroutine.WithDescription`.
pub const JANITOR_DESCRIPTION: &str = "refresh analytics cache";
/// `newJanitorJob`'s `goroutine.WithInterval`.
pub const JANITOR_INTERVAL: Duration = Duration::from_secs(8 * 60 * 60);

/// `service.JobLogsIterLimit`: rows read per page while streaming the logs.
pub const JOB_LOGS_ITER_LIMIT: i32 = 10_000;

/// The subset of `service.Service` the janitor calls.
///
/// The baseline threads the initiator through the context actor; here it is an
/// explicit argument.
#[async_trait]
pub trait JanitorService: Send + Sync {
    /// `Service.GetSearchJob(id).AggState`.
    async fn get_search_job_agg_state(
        &self,
        initiator_id: i32,
        search_job_id: i64,
    ) -> Result<JobState, String>;

    /// `Service.GetSearchJobLogsWriterTo` materialised as bytes: the CSV log
    /// file (header row included).
    async fn search_job_logs_csv(
        &self,
        initiator_id: i32,
        search_job_id: i64,
    ) -> Result<Vec<u8>, String>;

    /// `Service.UploadJobLogs`; the blob key is derived by the service.
    async fn upload_job_logs(
        &self,
        initiator_id: i32,
        search_job_id: i64,
        contents: Vec<u8>,
    ) -> Result<(), String>;

    /// `Service.DeleteJobLogs` (best-effort cleanup after a failed job).
    async fn delete_job_logs(&self, initiator_id: i32, search_job_id: i64) -> Result<(), String>;
}

/// `object.Storage.Upload` / `Delete` for the single log object.
#[async_trait]
pub trait JobLogsBlobStore: Send + Sync {
    /// `Upload(ctx, key, r)`; returns the number of bytes written.
    async fn upload(&self, key: &str, contents: Vec<u8>) -> Result<i64, String>;

    /// `Delete(ctx, key)`.
    async fn delete(&self, key: &str) -> Result<(), String>;
}

/// `getLogKey`: the key of the log object stored in the blobstore.
pub fn log_key(search_job_id: i64) -> String {
    format!("log-{search_job_id}.csv")
}

/// The production [`JanitorService`]: the exhaustive search store plus a
/// blobstore. Mirrors `service.Service`'s log methods, including their
/// `UserHasAccess` checks.
pub struct PgJanitorService {
    pool: PgPool,
    blobs: Arc<dyn JobLogsBlobStore>,
}

impl PgJanitorService {
    pub fn new(pool: PgPool, blobs: Arc<dyn JobLogsBlobStore>) -> Self {
        Self { pool, blobs }
    }

    /// `Service.GetSearchJob` under the initiator's actor.
    pub async fn agg_state(
        &self,
        initiator_id: i32,
        search_job_id: i64,
    ) -> Result<JobState, String> {
        let job =
            ex::get_exhaustive_search_job(&self.pool, Actor::user(initiator_id), search_job_id)
                .await
                .map_err(|err| err.to_string())?;
        Ok(job.agg_state)
    }

    /// `Service.getJobLogsIter`: read every log row in pages of
    /// [`JOB_LOGS_ITER_LIMIT`]. The cursor keeps the last row of a full page
    /// (the query uses `id >= cursor`) so rows are neither skipped nor
    /// duplicated.
    pub async fn all_logs(
        &self,
        initiator_id: i32,
        search_job_id: i64,
    ) -> Result<Vec<SearchJobLog>, String> {
        let actor = Actor::user(initiator_id);
        let limit = JOB_LOGS_ITER_LIMIT;
        // `cursor` is `None` once the iterator said `-1` (exhausted).
        let mut cursor = Some(0i64);
        let mut all = Vec::new();
        while let Some(from) = cursor {
            let mut logs = ex::get_job_logs(
                &self.pool,
                actor,
                search_job_id,
                Some(GetJobLogsOptions {
                    from,
                    limit: limit + 1,
                }),
            )
            .await
            .map_err(|err| err.to_string())?;
            if logs.len() as i32 > limit {
                cursor = Some(logs[logs.len() - 1].id);
                logs.truncate(limit as usize);
            } else {
                cursor = None;
            }
            all.extend(logs);
        }
        Ok(all)
    }

    /// `Service.GetSearchJobLogsWriterTo`: write the CSV log for the job.
    pub async fn logs_csv(&self, initiator_id: i32, search_job_id: i64) -> Result<Vec<u8>, String> {
        let logs = self.all_logs(initiator_id, search_job_id).await?;
        Ok(write_search_job_logs(&logs))
    }
}

#[async_trait]
impl JanitorService for PgJanitorService {
    async fn get_search_job_agg_state(
        &self,
        initiator_id: i32,
        search_job_id: i64,
    ) -> Result<JobState, String> {
        self.agg_state(initiator_id, search_job_id).await
    }

    async fn search_job_logs_csv(
        &self,
        initiator_id: i32,
        search_job_id: i64,
    ) -> Result<Vec<u8>, String> {
        self.logs_csv(initiator_id, search_job_id).await
    }

    async fn upload_job_logs(
        &self,
        initiator_id: i32,
        search_job_id: i64,
        contents: Vec<u8>,
    ) -> Result<(), String> {
        // SECURITY: only someone with access to the job may upload the logs.
        ex::user_has_access(&self.pool, Actor::user(initiator_id), search_job_id)
            .await
            .map_err(|err| err.to_string())?;
        self.blobs
            .upload(&log_key(search_job_id), contents)
            .await
            .map(|_bytes| ())
    }

    async fn delete_job_logs(&self, initiator_id: i32, search_job_id: i64) -> Result<(), String> {
        // SECURITY: only someone with access to the job may delete the logs.
        ex::user_has_access(&self.pool, Actor::user(initiator_id), search_job_id)
            .await
            .map_err(|err| err.to_string())?;
        self.blobs.delete(&log_key(search_job_id)).await
    }
}

/// `writeSearchJobLogs`: the CSV file upload by the janitor.
///
/// Header and rows match the baseline byte-for-byte:
/// `encoding/csv` with the default comma, `\n` line endings, Go's quoting
/// rules (quotes doubled; fields containing `,`, `"`, `\r` or `\n`, equal to
/// `\.`, or starting with whitespace are quoted) and `formatOrNULL`
/// (`NULL` for a zero time, otherwise RFC3339 in UTC).
pub fn write_search_job_logs(logs: &[SearchJobLog]) -> Vec<u8> {
    let mut out = Vec::new();
    write_csv_row(
        &mut out,
        &[
            "repository",
            "revision",
            "started_at",
            "finished_at",
            "status",
            "failure_message",
        ],
    );
    for log in logs {
        write_csv_row(
            &mut out,
            &[
                &log.repo_name,
                &log.revision,
                &format_or_null(log.started_at),
                &format_or_null(log.finished_at),
                log.state.as_str(),
                &log.failure_message,
            ],
        );
    }
    out
}

/// `formatOrNULL`.
fn format_or_null(timestamp: Option<OffsetDateTime>) -> String {
    match timestamp {
        None => "NULL".to_owned(),
        Some(timestamp) => format_rfc3339(timestamp),
    }
}

/// `time.Time.Format(time.RFC3339)`: no fractional seconds, `Z` for UTC.
fn format_rfc3339(timestamp: OffsetDateTime) -> String {
    let total = timestamp.offset().whole_seconds();
    let suffix = if total == 0 {
        "Z".to_owned()
    } else {
        let sign = if total < 0 { '-' } else { '+' };
        let total = total.abs();
        format!("{sign}{:02}:{:02}", total / 3600, (total % 3600) / 60)
    };
    format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}{suffix}",
        timestamp.year(),
        u8::from(timestamp.month()),
        timestamp.day(),
        timestamp.hour(),
        timestamp.minute(),
        timestamp.second(),
    )
}

fn write_csv_row(out: &mut Vec<u8>, fields: &[&str]) {
    for (index, field) in fields.iter().enumerate() {
        if index > 0 {
            out.push(b',');
        }
        if field_needs_quotes(field) {
            out.push(b'"');
            for byte in field.bytes() {
                if byte == b'"' {
                    out.extend_from_slice(b"\"\"");
                } else {
                    out.push(byte);
                }
            }
            out.push(b'"');
        } else {
            out.extend_from_slice(field.as_bytes());
        }
    }
    out.push(b'\n');
}

/// `encoding/csv.Writer.fieldNeedsQuotes` with `Comma = ','` and
/// `UseCRLF = false`.
fn field_needs_quotes(field: &str) -> bool {
    if field.is_empty() {
        return false;
    }
    if field == r"\." {
        return true;
    }
    if field
        .bytes()
        .any(|byte| matches!(byte, b'\n' | b'\r' | b'"' | b','))
    {
        return true;
    }
    field.chars().next().is_some_and(char::is_whitespace)
}

/// `runJanitor`.
pub struct SearchJobsJanitor {
    pool: PgPool,
    service: Arc<dyn JanitorService>,
}

impl SearchJobsJanitor {
    pub fn new(pool: PgPool, service: Arc<dyn JanitorService>) -> Self {
        Self { pool, service }
    }

    /// One janitor run.
    pub async fn run(&self) -> Result<(), String> {
        let jobs = ex::list_unaggregated_search_jobs(&self.pool)
            .await
            .map_err(|err| err.to_string())?;

        let mut errs: Option<String> = None;
        for job in jobs {
            // Unlike per-job failures, a failure to read the aggregate state
            // aborts the whole run (baseline `return err`).
            let agg_status = self
                .service
                .get_search_job_agg_state(job.initiator_id, job.id)
                .await?;
            if !agg_status.is_terminal() {
                continue;
            }
            if let Err(err) = self.aggregate_job(job, &agg_status).await {
                errs = append_errors(errs, Some(err));
                // Best effort cleanup of the logs uploaded inside the
                // rolled-back transaction.
                let _ = self.service.delete_job_logs(job.initiator_id, job.id).await;
            }
        }
        match errs {
            None => Ok(()),
            Some(errs) => Err(errs),
        }
    }

    /// `db.WithTransact` around update / upload / delete / mark-aggregated.
    async fn aggregate_job(
        &self,
        job: ex::UnaggregatedSearchJob,
        agg_status: &JobState,
    ) -> Result<(), String> {
        let mut tx = self.pool.begin().await.map_err(|err| err.to_string())?;
        match self.aggregate_job_in_tx(&mut tx, job, agg_status).await {
            Ok(()) => tx.commit().await.map_err(|err| err.to_string()),
            Err(err) => {
                let _ = tx.rollback().await;
                Err(err)
            }
        }
    }

    async fn aggregate_job_in_tx(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        job: ex::UnaggregatedSearchJob,
        agg_status: &JobState,
    ) -> Result<(), String> {
        ex::update_search_job_state_in_tx(tx, job.id, agg_status.as_str())
            .await
            .map_err(|err| err.to_string())?;

        let csv = self
            .service
            .search_job_logs_csv(job.initiator_id, job.id)
            .await?;
        self.service
            .upload_job_logs(job.initiator_id, job.id, csv)
            .await?;

        ex::delete_repo_jobs_in_tx(tx, job.id)
            .await
            .map_err(|err| err.to_string())?;
        ex::set_search_job_aggregated_in_tx(tx, job.id)
            .await
            .map_err(|err| err.to_string())?;
        Ok(())
    }
}

#[async_trait]
impl Handler for SearchJobsJanitor {
    async fn handle(&self, _ctx: CancellationToken) -> Result<(), HandlerError> {
        self.run().await.map_err(HandlerError::new)
    }
}

/// `newJanitorJob`: the periodic `search_jobs_janitor` routine.
pub fn new_search_jobs_janitor_routine(
    pool: PgPool,
    service: Arc<dyn JanitorService>,
) -> BackgroundRoutine {
    PeriodicRoutine::new(Arc::new(SearchJobsJanitor::new(pool, service)))
        .with_name(JANITOR_NAME)
        .with_description(JANITOR_DESCRIPTION)
        .with_interval(JANITOR_INTERVAL)
        .into()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn log(failure_message: &str) -> SearchJobLog {
        SearchJobLog {
            id: 1,
            repo_name: "github.com/acme/alpha".to_owned(),
            revision: "rev1".to_owned(),
            state: JobState::new("completed"),
            failure_message: failure_message.to_owned(),
            started_at: None,
            finished_at: None,
        }
    }

    #[test]
    fn writes_the_baseline_header() {
        assert_eq!(
            String::from_utf8(write_search_job_logs(&[])).expect("utf-8"),
            "repository,revision,started_at,finished_at,status,failure_message\n"
        );
    }

    const HEADER: &str = "repository,revision,started_at,finished_at,status,failure_message\n";

    #[test]
    fn quotes_like_encoding_csv() {
        let mut logs = vec![log("plain")];
        logs[0].started_at =
            Some(OffsetDateTime::from_unix_timestamp(1_767_323_045).expect("valid timestamp"));
        assert_eq!(
            String::from_utf8(write_search_job_logs(&logs)).expect("utf-8"),
            format!(
                "{HEADER}github.com/acme/alpha,rev1,2026-01-02T03:04:05Z,NULL,completed,plain\n"
            )
        );

        for (message, want) in [
            ("a,b", "\"a,b\""),
            ("a\"b", "\"a\"\"b\""),
            ("a\nb", "\"a\nb\""),
            (" leading", "\" leading\""),
            (r"\.", r#""\.""#),
            ("", ""),
        ] {
            let line = String::from_utf8(write_search_job_logs(&[log(message)])).expect("utf-8");
            let row = line.strip_prefix(HEADER).expect("header");
            assert_eq!(
                row,
                format!("github.com/acme/alpha,rev1,NULL,NULL,completed,{want}\n"),
                "message {message:?}"
            );
        }
    }

    #[tokio::test]
    async fn janitor_options_match_baseline() {
        let pool = sqlx::postgres::PgPoolOptions::new()
            .connect_lazy("postgres://unused@localhost/unused")
            .expect("lazy pool");
        let routine = new_search_jobs_janitor_routine(pool, Arc::new(UnusedService));
        assert_eq!(routine.name(), "search_jobs_janitor");
        assert_eq!(routine.interval(), Duration::from_secs(8 * 60 * 60));
    }

    struct UnusedService;

    #[async_trait]
    impl JanitorService for UnusedService {
        async fn get_search_job_agg_state(
            &self,
            _initiator_id: i32,
            _search_job_id: i64,
        ) -> Result<JobState, String> {
            Err("unused".to_owned())
        }

        async fn search_job_logs_csv(
            &self,
            _initiator_id: i32,
            _search_job_id: i64,
        ) -> Result<Vec<u8>, String> {
            Err("unused".to_owned())
        }

        async fn upload_job_logs(
            &self,
            _initiator_id: i32,
            _search_job_id: i64,
            _contents: Vec<u8>,
        ) -> Result<(), String> {
            Err("unused".to_owned())
        }

        async fn delete_job_logs(
            &self,
            _initiator_id: i32,
            _search_job_id: i64,
        ) -> Result<(), String> {
            Err("unused".to_owned())
        }
    }
}
