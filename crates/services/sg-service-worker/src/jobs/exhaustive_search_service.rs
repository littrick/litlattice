#![forbid(unsafe_code)]

//! `internal/search/exhaustive/service/{service.go,matchjson.go}`: the
//! exhaustive-search service layer used by the search-jobs GraphQL API and by
//! the worker's blob plumbing.
//!
//! Ported from `service.go`:
//!
//! * [`Service::validate_search_job`] / [`Service::create_search_job`]
//!   (authentication gate, query validation, create-then-get);
//! * [`Service::cancel_search_job`] / [`Service::get_search_job`] /
//!   [`Service::list_search_jobs`];
//! * the object-store helpers [`Service::upload_job_logs`],
//!   [`Service::get_job_logs`], [`Service::delete_job_logs`],
//!   [`Service::delete_search_job`];
//! * the result/log writers [`Service::get_search_job_results_writer_to`] and
//!   [`Service::get_search_job_logs_writer_to`], including
//!   `writeSearchJobJSON` ([`write_search_job_json`]) and
//!   `writeSearchJobLogs` ([`write_search_job_logs`], Go `encoding/csv`
//!   quoting);
//! * [`Service::get_aggregate_repo_rev_state`] state aggregation.
//!
//! Ported from `matchjson.go`:
//!
//! * [`MatchJSONWriter`] / [`new_json_writer`] (100 MiB buffer);
//! * [`BlobUploader`] (`prefix`, then `prefix-<shard>`) and
//!   [`BufferedWriter`] (`Append` newline-terminated JSON, flush at the size
//!   threshold, no upload for an empty buffer).
//!
//! The Go service takes a concrete `*store.Store`; this port keeps the same
//! operations behind the injected [`SearchJobStore`] trait. The production
//! [`PgSearchJobStore`] forwards to `sg_db::exhaustive_search`, so the SQL and
//! error strings stay in `sg-db`. Object storage is behind [`UploadStore`];
//! the default [`NotPortedUploadStore`] fails with an explicit not-ported
//! error because no real backend is in scope. The Go service reads the actor
//! from `context.Context`; Rust has no such context, so [`ActorContext`]
//! supplies it (mirroring the `UserContext` seam in
//! [`crate::jobs::exhaustive_search_searcher`]).
//!
//! Deviations:
//!
//! * `CreateSearchJob`/`CancelSearchJob` run inside `Store.Transact` in the
//!   baseline. `sg-db` exposes no transaction variant of these two functions,
//!   so [`PgSearchJobStore`] issues the same statements sequentially; only the
//!   single-transaction atomicity of create+get is lost.
//! * `MatchJSONWriter.Write(result.Match)` performs `search.FromMatch`, which
//!   is not part of `matchjson.go`. [`MatchJSONWriter::write`] therefore takes
//!   the already-converted event-match JSON value (the same payload shape the
//!   worker's `MatchWriter` seam uses). The buffering/sharding/flush semantics
//!   are byte-for-byte compatible.
//! * `object.Storage` streams readers; this slice materialises object values
//!   in memory (`get` returns `Vec<u8>`) because no real backend is ported.
//!
//! See `docs/overviews/m9_8hy_exhaustive_service_overview.md`.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicI32, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use async_trait::async_trait;
use sg_db::exhaustive_search::{self as ex, Actor};
use sqlx::PgPool;
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;
use tokio::io::AsyncReadExt;

use crate::jobs::exhaustive_search_job::{MatchWriter, MatchWriterFactory, NewSearcher};

/// `JobLogsIterLimit`: rows read per page from the database.
pub const JOB_LOGS_ITER_LIMIT: usize = 10_000;

/// `newBufferedWriter(1024*1024*100, ...)` in `NewJSONWriter`.
pub const BUFFERED_JSON_WRITER_FLUSH_SIZE: usize = 1024 * 1024 * 100;

/// `service.MatchWriter`'s event payload is already JSON; the writer only needs
/// to encode it. `write`/`flush` are async because [`UploadStore`] is async.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct WriteOutcome {
    pub bytes_written: i64,
    pub error: Option<String>,
}

// ---------------------------------------------------------------------------
// Store seams
// ---------------------------------------------------------------------------

/// `*store.Store` methods the service calls. Every method takes the actor that
/// the Go implementation reads from `context.Context`.
#[async_trait]
pub trait SearchJobStore: Send + Sync {
    async fn create_exhaustive_search_job(
        &self,
        actor: Actor,
        query: &str,
        initiator_id: i32,
    ) -> Result<i64, String>;

    async fn get_exhaustive_search_job(
        &self,
        actor: Actor,
        id: i64,
    ) -> Result<ex::ExhaustiveSearchJob, String>;

    async fn cancel_search_job(&self, actor: Actor, id: i64) -> Result<i64, String>;

    async fn list_exhaustive_search_jobs(
        &self,
        actor: Actor,
        args: &ex::ListArgs,
    ) -> Result<Vec<ex::ExhaustiveSearchJob>, String>;

    async fn user_has_access(&self, actor: Actor, id: i64) -> Result<(), String>;

    async fn get_job_logs(
        &self,
        actor: Actor,
        id: i64,
        options: Option<ex::GetJobLogsOptions>,
    ) -> Result<Vec<ex::SearchJobLog>, String>;

    async fn delete_exhaustive_search_job(&self, actor: Actor, id: i64) -> Result<(), String>;

    async fn get_aggregate_repo_rev_state(
        &self,
        actor: Actor,
        id: i64,
    ) -> Result<BTreeMap<String, i64>, String>;
}

/// `service.New`'s `*store.Store`: the `sg-db` implementation.
pub struct PgSearchJobStore {
    pool: PgPool,
}

impl PgSearchJobStore {
    pub fn new(pool: PgPool) -> PgSearchJobStore {
        PgSearchJobStore { pool }
    }

    pub fn pool(&self) -> &PgPool {
        &self.pool
    }
}

#[async_trait]
impl SearchJobStore for PgSearchJobStore {
    async fn create_exhaustive_search_job(
        &self,
        actor: Actor,
        query: &str,
        initiator_id: i32,
    ) -> Result<i64, String> {
        ex::create_exhaustive_search_job(&self.pool, actor, query, initiator_id)
            .await
            .map_err(|err| err.to_string())
    }

    async fn get_exhaustive_search_job(
        &self,
        actor: Actor,
        id: i64,
    ) -> Result<ex::ExhaustiveSearchJob, String> {
        ex::get_exhaustive_search_job(&self.pool, actor, id)
            .await
            .map_err(|err| err.to_string())
    }

    async fn cancel_search_job(&self, actor: Actor, id: i64) -> Result<i64, String> {
        ex::cancel_search_job(&self.pool, actor, id, OffsetDateTime::now_utc())
            .await
            .map_err(|err| err.to_string())
    }

    async fn list_exhaustive_search_jobs(
        &self,
        actor: Actor,
        args: &ex::ListArgs,
    ) -> Result<Vec<ex::ExhaustiveSearchJob>, String> {
        ex::list_exhaustive_search_jobs(&self.pool, actor, args)
            .await
            .map_err(|err| err.to_string())
    }

    async fn user_has_access(&self, actor: Actor, id: i64) -> Result<(), String> {
        ex::user_has_access(&self.pool, actor, id)
            .await
            .map_err(|err| err.to_string())
    }

    async fn get_job_logs(
        &self,
        actor: Actor,
        id: i64,
        options: Option<ex::GetJobLogsOptions>,
    ) -> Result<Vec<ex::SearchJobLog>, String> {
        ex::get_job_logs(&self.pool, actor, id, options)
            .await
            .map_err(|err| err.to_string())
    }

    async fn delete_exhaustive_search_job(&self, actor: Actor, id: i64) -> Result<(), String> {
        ex::delete_exhaustive_search_job(&self.pool, actor, id)
            .await
            .map_err(|err| err.to_string())
    }

    async fn get_aggregate_repo_rev_state(
        &self,
        actor: Actor,
        id: i64,
    ) -> Result<BTreeMap<String, i64>, String> {
        ex::get_aggregate_repo_rev_state(&self.pool, actor, id)
            .await
            .map_err(|err| err.to_string())
    }
}

/// `actor.FromContext` stand-in. The Go service authenticates `Validate` and
/// `Create` with `actor.IsAuthenticated()` (uid != 0) and forwards the uid to
/// the store and searcher.
pub trait ActorContext: Send + Sync {
    fn actor(&self) -> Actor;
}

/// No actor in context: mirrors `actor.FromContext(context.Background())`.
pub struct NoActor;

impl ActorContext for NoActor {
    fn actor(&self) -> Actor {
        Actor {
            uid: 0,
            internal: false,
        }
    }
}

// ---------------------------------------------------------------------------
// Object storage seam
// ---------------------------------------------------------------------------

/// The value part of `object.Storage.List`: Go's iterator yields keys and can
/// report a terminal `iter.Err()`. Immediate `List` errors use `Result::Err`.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct UploadList {
    pub keys: Vec<String>,
    pub error: Option<String>,
}

/// `object.Storage`, narrowed to the methods the service calls. Values are
/// materialised because no streaming backend is ported.
#[async_trait]
pub trait UploadStore: Send + Sync {
    async fn get(&self, key: &str) -> Result<Vec<u8>, String>;

    async fn upload(&self, key: &str, data: &[u8]) -> Result<i64, String>;

    async fn delete(&self, key: &str) -> Result<(), String>;

    async fn list(&self, prefix: &str) -> Result<UploadList, String>;
}

const NOT_PORTED_UPLOAD_STORE: &str = "not ported: exhaustive search object storage";

/// The default upload store until a real object-storage backend is wired.
pub struct NotPortedUploadStore;

#[async_trait]
impl UploadStore for NotPortedUploadStore {
    async fn get(&self, _key: &str) -> Result<Vec<u8>, String> {
        Err(NOT_PORTED_UPLOAD_STORE.to_owned())
    }

    async fn upload(&self, _key: &str, _data: &[u8]) -> Result<i64, String> {
        Err(NOT_PORTED_UPLOAD_STORE.to_owned())
    }

    async fn delete(&self, _key: &str) -> Result<(), String> {
        Err(NOT_PORTED_UPLOAD_STORE.to_owned())
    }

    async fn list(&self, _prefix: &str) -> Result<UploadList, String> {
        Err(NOT_PORTED_UPLOAD_STORE.to_owned())
    }
}

// ---------------------------------------------------------------------------
// Service
// ---------------------------------------------------------------------------

/// `service.Service`.
pub struct Service {
    store: Arc<dyn SearchJobStore>,
    upload_store: Arc<dyn UploadStore>,
    new_searcher: Arc<dyn NewSearcher>,
    actor_context: Arc<dyn ActorContext>,
}

impl Service {
    /// `service.New`. The Go `*observation.Context` is omitted: the Rust
    /// worker has no equivalent RED-metrics layer for this service yet.
    pub fn new(
        store: Arc<dyn SearchJobStore>,
        upload_store: Arc<dyn UploadStore>,
        new_searcher: Arc<dyn NewSearcher>,
        actor_context: Arc<dyn ActorContext>,
    ) -> Service {
        Service {
            store,
            upload_store,
            new_searcher,
            actor_context,
        }
    }

    fn actor(&self) -> Actor {
        self.actor_context.actor()
    }

    /// `Service.ValidateSearchJob`.
    pub async fn validate_search_job(&self, query: &str) -> Result<(), String> {
        let actor = self.actor();
        if !actor.is_authenticated() {
            return Err("search jobs can only be validated by an authenticated user".to_owned());
        }
        self.new_searcher
            .new_search(actor.uid, query)
            .await
            .map(|_| ())
    }

    /// `Service.CreateSearchJob`.
    pub async fn create_search_job(&self, query: &str) -> Result<ex::ExhaustiveSearchJob, String> {
        let actor = self.actor();
        if !actor.is_authenticated() {
            return Err("search jobs can only be created by an authenticated user".to_owned());
        }

        self.validate_search_job(query).await?;

        let id = self
            .store
            .create_exhaustive_search_job(actor, query, actor.uid)
            .await?;
        self.store.get_exhaustive_search_job(actor, id).await
    }

    /// `Service.CancelSearchJob`.
    pub async fn cancel_search_job(&self, id: i64) -> Result<(), String> {
        let actor = self.actor();
        self.store.cancel_search_job(actor, id).await.map(|_| ())
    }

    /// `Service.GetSearchJob`.
    pub async fn get_search_job(&self, id: i64) -> Result<ex::ExhaustiveSearchJob, String> {
        let actor = self.actor();
        self.store.get_exhaustive_search_job(actor, id).await
    }

    /// `Service.ListSearchJobs`.
    pub async fn list_search_jobs(
        &self,
        args: &ex::ListArgs,
    ) -> Result<Vec<ex::ExhaustiveSearchJob>, String> {
        let actor = self.actor();
        self.store.list_exhaustive_search_jobs(actor, args).await
    }

    /// `Service.GetSearchJobLogsWriterTo`.
    pub async fn get_search_job_logs_writer_to(
        &self,
        id: i64,
    ) -> Result<SearchJobLogsWriterTo, String> {
        let actor = self.actor();
        self.store.user_has_access(actor, id).await?;
        Ok(SearchJobLogsWriterTo {
            store: Arc::clone(&self.store),
            actor,
            id,
        })
    }

    /// `getLogKey`: `log-<id>.csv`.
    pub async fn upload_job_logs(&self, id: i64, data: &[u8]) -> Result<i64, String> {
        let actor = self.actor();
        self.store.user_has_access(actor, id).await?;
        self.upload_store.upload(&get_log_key(id), data).await
    }

    /// `Service.GetJobLogs`.
    pub async fn get_job_logs(&self, id: i64) -> Result<Vec<u8>, String> {
        let actor = self.actor();
        self.store.user_has_access(actor, id).await?;
        self.upload_store.get(&get_log_key(id)).await
    }

    /// `Service.DeleteJobLogs`.
    pub async fn delete_job_logs(&self, id: i64) -> Result<(), String> {
        let actor = self.actor();
        self.store.user_has_access(actor, id).await?;
        self.upload_store.delete(&get_log_key(id)).await
    }

    /// `Service.DeleteSearchJob`.
    pub async fn delete_search_job(&self, id: i64) -> Result<(), String> {
        let actor = self.actor();
        // 🚨 SECURITY: only someone with access to the job may delete data and
        // the db entries.
        self.store.user_has_access(actor, id).await?;

        let list = self.upload_store.list(&get_prefix(id)).await?;
        for key in &list.keys {
            if let Err(err) = self.upload_store.delete(key).await {
                // If we continued, we might end up with data in the upload
                // store without entries in the db to reference it.
                return Err(format!("deleting key {key:?}: {err}"));
            }
        }
        if let Some(err) = list.error {
            return Err(err);
        }

        // The log file is not guaranteed to exist, so we ignore the error here.
        let _ = self.upload_store.delete(&get_log_key(id)).await;

        self.store.delete_exhaustive_search_job(actor, id).await
    }

    /// `Service.GetSearchJobResultsWriterTo`.
    pub async fn get_search_job_results_writer_to(
        &self,
        id: i64,
    ) -> Result<SearchJobResultsWriterTo, String> {
        let actor = self.actor();
        // 🚨 SECURITY: only someone with access to the job may copy the blobs.
        self.store.user_has_access(actor, id).await?;

        let list = self.upload_store.list(&get_prefix(id)).await?;
        Ok(SearchJobResultsWriterTo {
            upload_store: Arc::clone(&self.upload_store),
            keys: list.keys,
            terminal_error: list.error,
        })
    }

    /// `Service.GetAggregateRepoRevState`.
    pub async fn get_aggregate_repo_rev_state(
        &self,
        id: i64,
    ) -> Result<ex::RepoRevJobStats, String> {
        let actor = self.actor();
        let counts = self.store.get_aggregate_repo_rev_state(actor, id).await?;

        let mut stats = ex::RepoRevJobStats {
            total: 0,
            completed: 0,
            failed: 0,
            in_progress: 0,
        };
        for (state, count) in counts {
            match state.as_str() {
                "completed" => stats.completed += count as i32,
                "failed" => stats.failed += count as i32,
                // errored jobs will be retried, so treat them as processing.
                "processing" | "errored" | "queued" => stats.in_progress += count as i32,
                "canceled" => stats.in_progress = 0,
                _ => return Err(format!("unknown job state {state:?}")),
            }
        }
        stats.total = stats.completed + stats.failed + stats.in_progress;
        Ok(stats)
    }
}

/// `getLogKey`.
pub fn get_log_key(search_job_id: i64) -> String {
    format!("log-{search_job_id}.csv")
}

/// `getPrefix`.
pub fn get_prefix(id: i64) -> String {
    format!("{id}-")
}

// ---------------------------------------------------------------------------
// Logs WriterTo
// ---------------------------------------------------------------------------

/// `Service.GetSearchJobLogsWriterTo`'s `writerToFunc`: the log pages are
/// fetched lazily when `write_to` runs.
pub struct SearchJobLogsWriterTo {
    store: Arc<dyn SearchJobStore>,
    actor: Actor,
    id: i64,
}

impl SearchJobLogsWriterTo {
    pub async fn write_to(&self, out: &mut Vec<u8>) -> WriteOutcome {
        let mut iter = JobLogsIter::new(Arc::clone(&self.store), self.actor, self.id);
        write_search_job_logs(&mut iter, out).await
    }
}

/// `getJobLogsIter`.
struct JobLogsIter {
    store: Arc<dyn SearchJobStore>,
    actor: Actor,
    id: i64,
    cursor: i64,
    limit: usize,
    done: bool,
}

impl JobLogsIter {
    fn new(store: Arc<dyn SearchJobStore>, actor: Actor, id: i64) -> JobLogsIter {
        JobLogsIter {
            store,
            actor,
            id,
            cursor: 0,
            limit: JOB_LOGS_ITER_LIMIT,
            done: false,
        }
    }

    async fn next_page(&mut self) -> Result<Option<Vec<ex::SearchJobLog>>, String> {
        if self.done {
            return Ok(None);
        }

        let options = ex::GetJobLogsOptions {
            from: self.cursor,
            limit: (self.limit + 1) as i32,
        };
        let mut logs = self
            .store
            .get_job_logs(self.actor, self.id, Some(options))
            .await?;

        if logs.len() > self.limit {
            self.cursor = logs[logs.len() - 1].id;
            logs.truncate(self.limit);
        } else {
            self.done = true;
        }

        Ok(Some(logs))
    }
}

/// `formatOrNULL`: `NULL` for an absent timestamp, otherwise Go `time.RFC3339`
/// (whole seconds; `Z` for UTC).
pub fn format_or_null(t: Option<OffsetDateTime>) -> String {
    match t {
        None => "NULL".to_owned(),
        Some(t) => {
            let t = t.replace_nanosecond(0).unwrap_or(t);
            t.format(&Rfc3339).unwrap_or_default()
        }
    }
}

/// `writeSearchJobLogs`: `encoding/csv` with the baseline header and rows.
async fn write_search_job_logs(iter: &mut JobLogsIter, out: &mut Vec<u8>) -> WriteOutcome {
    let start = out.len();

    csv_write_record(
        out,
        &[
            "repository",
            "revision",
            "started_at",
            "finished_at",
            "status",
            "failure_message",
        ],
    );

    loop {
        match iter.next_page().await {
            Ok(Some(logs)) => {
                for log in logs {
                    let fields = [
                        log.repo_name,
                        log.revision,
                        format_or_null(log.started_at),
                        format_or_null(log.finished_at),
                        log.state.as_str().to_owned(),
                        log.failure_message,
                    ];
                    csv_write_record(out, &fields);
                }
            }
            Ok(None) => break,
            Err(err) => {
                return WriteOutcome {
                    bytes_written: (out.len() - start) as i64,
                    error: Some(err),
                }
            }
        }
    }

    WriteOutcome {
        bytes_written: (out.len() - start) as i64,
        error: None,
    }
}

/// Go `encoding/csv` `fieldNeedsQuotes`.
fn csv_field_needs_quotes(field: &str) -> bool {
    if field.is_empty() {
        return false;
    }
    if field == "\\." {
        return true;
    }
    if field.contains(',') || field.contains('"') || field.contains('\r') || field.contains('\n') {
        return true;
    }
    field.chars().next().is_some_and(char::is_whitespace)
}

fn csv_write_field(out: &mut Vec<u8>, field: &str) {
    if csv_field_needs_quotes(field) {
        out.push(b'"');
        for byte in field.bytes() {
            if byte == b'"' {
                out.push(b'"');
            }
            out.push(byte);
        }
        out.push(b'"');
    } else {
        out.extend_from_slice(field.as_bytes());
    }
}

fn csv_write_record<S: AsRef<str>>(out: &mut Vec<u8>, fields: &[S]) {
    for (index, field) in fields.iter().enumerate() {
        if index > 0 {
            out.push(b',');
        }
        csv_write_field(out, field.as_ref());
    }
    out.push(b'\n');
}

// ---------------------------------------------------------------------------
// Results WriterTo
// ---------------------------------------------------------------------------

/// `Service.GetSearchJobResultsWriterTo`'s `writerToFunc`.
pub struct SearchJobResultsWriterTo {
    upload_store: Arc<dyn UploadStore>,
    keys: Vec<String>,
    terminal_error: Option<String>,
}

impl SearchJobResultsWriterTo {
    pub async fn write_to(&self, out: &mut Vec<u8>) -> WriteOutcome {
        write_search_job_json(
            &self.keys,
            self.terminal_error.as_deref(),
            self.upload_store.as_ref(),
            out,
        )
        .await
    }
}

/// `writeSearchJobJSON`: copy each object's bytes in key order. A read failure
/// wraps as `writing JSON for key %q`; a terminal iterator error is returned
/// after the keys are processed.
async fn write_search_job_json(
    keys: &[String],
    terminal_error: Option<&str>,
    upload_store: &dyn UploadStore,
    out: &mut Vec<u8>,
) -> WriteOutcome {
    let start = out.len();
    for key in keys {
        match upload_store.get(key).await {
            Ok(bytes) => out.extend_from_slice(&bytes),
            Err(err) => {
                return WriteOutcome {
                    bytes_written: (out.len() - start) as i64,
                    error: Some(format!("writing JSON for key {key:?}: {err}")),
                }
            }
        }
    }

    WriteOutcome {
        bytes_written: (out.len() - start) as i64,
        error: terminal_error.map(str::to_owned),
    }
}

// ---------------------------------------------------------------------------
// matchjson.go
// ---------------------------------------------------------------------------

/// `blobUploader.write`'s upload side, abstracted so [`BufferedWriter`] can be
/// tested without an object store.
#[async_trait]
pub trait BlobUpload: Send + Sync {
    async fn upload_blob(&self, data: &[u8]) -> Result<(), String>;
}

/// `blobUploader`: the key combines the prefix with the shard number, except
/// for the first shard where the number is omitted.
pub struct BlobUploader {
    store: Arc<dyn UploadStore>,
    prefix: String,
    shard: AtomicI32,
}

impl BlobUploader {
    pub fn new(store: Arc<dyn UploadStore>, prefix: impl Into<String>) -> BlobUploader {
        BlobUploader {
            store,
            prefix: prefix.into(),
            shard: AtomicI32::new(1),
        }
    }
}

#[async_trait]
impl BlobUpload for BlobUploader {
    async fn upload_blob(&self, data: &[u8]) -> Result<(), String> {
        let shard = self.shard.load(Ordering::SeqCst);
        let key = if shard == 1 {
            self.prefix.clone()
        } else {
            format!("{}-{}", self.prefix, shard)
        };

        self.store.upload(&key, data).await?;
        self.shard.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

/// `bufferedWriter`: appends newline-terminated JSON, flushing once the buffer
/// reaches `flush_size`.
pub struct BufferedWriter {
    flush_size: usize,
    buf: Vec<u8>,
    upload: Arc<dyn BlobUpload>,
}

impl BufferedWriter {
    /// `newBufferedWriter`.
    pub fn new(flush_size: usize, upload: Arc<dyn BlobUpload>) -> BufferedWriter {
        // Grow the buffer to reduce the number of small allocations caused by
        // repeatedly growing the buffer (Go: min(flushSize, 1 MiB)).
        let buf = Vec::with_capacity(flush_size.min(1024 * 1024));
        BufferedWriter {
            flush_size,
            buf,
            upload,
        }
    }

    /// `bufferedWriter.Append`: encode (with the `json.Encoder` newline), roll
    /// back on failure and flush if the threshold is reached.
    pub async fn append(&mut self, value: &serde_json::Value) -> Result<(), String> {
        let old_len = self.buf.len();
        if let Err(err) = serde_json::to_writer(&mut self.buf, value) {
            // Reset the buffer to where it was before failing to marshal.
            self.buf.truncate(old_len);
            return Err(err.to_string());
        }
        self.buf.push(b'\n');

        if self.buf.len() >= self.flush_size {
            return self.flush().await;
        }
        Ok(())
    }

    /// `bufferedWriter.Flush`: write and reset the buffer if it has data.
    pub async fn flush(&mut self) -> Result<(), String> {
        if self.buf.is_empty() {
            return Ok(());
        }

        // `split_off(0)` transfers the bytes and leaves the original capacity
        // in `self.buf`, matching Go's `bytes.Buffer.Reset`.
        let buf = self.buf.split_off(0);
        self.upload.upload_blob(&buf).await
    }

    /// `bufferedWriter.Len`.
    pub fn len(&self) -> usize {
        self.buf.len()
    }

    pub fn is_empty(&self) -> bool {
        self.buf.is_empty()
    }
}

/// `MatchJSONWriter`. `write` takes the event-match JSON (see module docs for
/// the `search.FromMatch` deviation).
pub struct MatchJSONWriter {
    w: BufferedWriter,
}

impl MatchJSONWriter {
    /// `NewJSONWriter`.
    pub fn new(upload_store: Arc<dyn UploadStore>, prefix: impl Into<String>) -> MatchJSONWriter {
        MatchJSONWriter {
            w: BufferedWriter::new(
                BUFFERED_JSON_WRITER_FLUSH_SIZE,
                Arc::new(BlobUploader::new(upload_store, prefix)),
            ),
        }
    }

    /// `MatchJSONWriter.Write`.
    pub async fn write(&mut self, event_match: &serde_json::Value) -> Result<(), String> {
        self.w.append(event_match).await
    }

    /// `MatchJSONWriter.Flush`.
    pub async fn flush(&mut self) -> Result<(), String> {
        self.w.flush().await
    }

    pub fn len(&self) -> usize {
        self.w.len()
    }

    pub fn is_empty(&self) -> bool {
        self.w.is_empty()
    }
}

/// `NewJSONWriter` (free function spelling).
pub fn new_json_writer(
    upload_store: Arc<dyn UploadStore>,
    prefix: impl Into<String>,
) -> MatchJSONWriter {
    MatchJSONWriter::new(upload_store, prefix)
}

// ---------------------------------------------------------------------------
// Object storage adapter (M9.8id)
// ---------------------------------------------------------------------------

/// `object.Storage`, mapped onto [`sg_objectstorage::ObjectStore`].
///
/// The Go interface has seven methods; `Compose` is intentionally absent (the
/// GCS composition path is out of scope). Values are materialised because the
/// worker seams ([`UploadStore`], `JobLogsBlobStore`) take bytes, not readers.
#[async_trait]
pub trait ObjectStorageBackend: Send + Sync {
    /// `object.Storage.Init`.
    async fn init(&self) -> Result<(), String>;

    /// `object.Storage.Get`.
    async fn get(&self, key: &str) -> Result<Vec<u8>, String>;

    /// `object.Storage.Upload`; returns the number of bytes uploaded.
    async fn upload(&self, key: &str, data: Vec<u8>) -> Result<i64, String>;

    /// `object.Storage.Delete`.
    async fn delete(&self, key: &str) -> Result<(), String>;

    /// `object.Storage.List`: every key with the prefix.
    async fn list_objects(&self, prefix: &str) -> Result<Vec<String>, String>;

    /// `object.Storage.ExpireObjects`.
    async fn expire_objects(&self, prefix: &str, max_age: Duration) -> Result<(), String>;
}

#[async_trait]
impl ObjectStorageBackend for sg_objectstorage::ObjectStore {
    async fn init(&self) -> Result<(), String> {
        sg_objectstorage::ObjectStore::init(self)
            .await
            .map_err(|err| err.to_string())
    }

    async fn get(&self, key: &str) -> Result<Vec<u8>, String> {
        let body = sg_objectstorage::ObjectStore::get(self, key)
            .await
            .map_err(|err| err.to_string())?;
        let mut reader = body.into_async_read();
        let mut data = Vec::new();
        reader
            .read_to_end(&mut data)
            .await
            .map_err(|err| err.to_string())?;
        Ok(data)
    }

    async fn upload(&self, key: &str, data: Vec<u8>) -> Result<i64, String> {
        sg_objectstorage::ObjectStore::upload(
            self,
            key,
            sg_objectstorage::ObjectByteStream::from(data),
        )
        .await
        .map_err(|err| err.to_string())
    }

    async fn delete(&self, key: &str) -> Result<(), String> {
        sg_objectstorage::ObjectStore::delete(self, key)
            .await
            .map_err(|err| err.to_string())
    }

    async fn list_objects(&self, prefix: &str) -> Result<Vec<String>, String> {
        sg_objectstorage::ObjectStore::list_objects(self, prefix)
            .await
            .map(|objects| objects.into_iter().map(|object| object.key).collect())
            .map_err(|err| err.to_string())
    }

    async fn expire_objects(&self, prefix: &str, max_age: Duration) -> Result<(), String> {
        sg_objectstorage::ObjectStore::expire_objects(self, prefix, max_age)
            .await
            .map_err(|err| err.to_string())
    }
}

#[async_trait]
impl ObjectStorageBackend for sg_objectstorage::GcsStore {
    async fn init(&self) -> Result<(), String> {
        sg_objectstorage::GcsStore::init(self)
            .await
            .map_err(|err| err.to_string())
    }

    async fn get(&self, key: &str) -> Result<Vec<u8>, String> {
        sg_objectstorage::GcsStore::get(self, key)
            .await
            .map_err(|err| err.to_string())
    }

    async fn upload(&self, key: &str, data: Vec<u8>) -> Result<i64, String> {
        sg_objectstorage::GcsStore::upload(self, key, &data)
            .await
            .map_err(|err| err.to_string())
    }

    async fn delete(&self, key: &str) -> Result<(), String> {
        sg_objectstorage::GcsStore::delete(self, key)
            .await
            .map_err(|err| err.to_string())
    }

    async fn list_objects(&self, prefix: &str) -> Result<Vec<String>, String> {
        sg_objectstorage::GcsStore::list_objects(self, prefix)
            .await
            .map_err(|err| err.to_string())
    }

    async fn expire_objects(&self, prefix: &str, max_age: Duration) -> Result<(), String> {
        sg_objectstorage::GcsStore::expire_objects(self, prefix, max_age)
            .await
            .map_err(|err| err.to_string())
    }
}

/// `object.NewFromConfig`: dispatch on `StorageConfig.Backend` to the S3 or GCS
/// store, wrapped in [`LazyObjectStorage`] like the baseline.
pub async fn new_object_storage(
    config: sg_objectstorage::StorageConfig,
) -> Result<Arc<dyn ObjectStorageBackend>, String> {
    match config.backend.as_str() {
        "gcs" => {
            let store = sg_objectstorage::GcsStore::new(config).map_err(|err| err.to_string())?;
            Ok(Arc::new(LazyObjectStorage::new(Arc::new(store))))
        }
        "s3" | "blobstore" => {
            let store = sg_objectstorage::ObjectStore::new(config)
                .await
                .map_err(|err| err.to_string())?;
            Ok(Arc::new(LazyObjectStorage::new(Arc::new(store))))
        }
        // `object.create`: the lookup is not normalized (config Load lowercases
        // the backend) and uses the baseline's exact error text.
        other => Err(format!("unknown upload store backend '{other}'")),
    }
}

/// `object.CreateLazyStorage`: `Init` runs on first use, and a broken bucket
/// does not fail worker startup.
pub struct LazyObjectStorage<B: ObjectStorageBackend> {
    backend: Arc<B>,
    initialized: tokio::sync::OnceCell<()>,
}

impl<B: ObjectStorageBackend> LazyObjectStorage<B> {
    pub fn new(backend: Arc<B>) -> LazyObjectStorage<B> {
        LazyObjectStorage {
            backend,
            initialized: tokio::sync::OnceCell::new(),
        }
    }

    async fn ensure_initialized(&self) -> Result<(), String> {
        self.initialized
            .get_or_try_init(|| async { self.backend.init().await })
            .await
            .map(|_| ())
    }
}

#[async_trait]
impl<B: ObjectStorageBackend> ObjectStorageBackend for LazyObjectStorage<B> {
    async fn init(&self) -> Result<(), String> {
        self.ensure_initialized().await
    }

    async fn get(&self, key: &str) -> Result<Vec<u8>, String> {
        self.ensure_initialized().await?;
        self.backend.get(key).await
    }

    async fn upload(&self, key: &str, data: Vec<u8>) -> Result<i64, String> {
        self.ensure_initialized().await?;
        self.backend.upload(key, data).await
    }

    async fn delete(&self, key: &str) -> Result<(), String> {
        self.ensure_initialized().await?;
        self.backend.delete(key).await
    }

    async fn list_objects(&self, prefix: &str) -> Result<Vec<String>, String> {
        self.ensure_initialized().await?;
        self.backend.list_objects(prefix).await
    }

    async fn expire_objects(&self, prefix: &str, max_age: Duration) -> Result<(), String> {
        self.ensure_initialized().await?;
        self.backend.expire_objects(prefix, max_age).await
    }
}

/// `object.Storage` behind the service's [`UploadStore`] seam.
pub struct ObjectStorageUploadStore<B: ObjectStorageBackend + ?Sized> {
    backend: Arc<B>,
}

impl<B: ObjectStorageBackend + ?Sized> ObjectStorageUploadStore<B> {
    pub fn new(backend: Arc<B>) -> ObjectStorageUploadStore<B> {
        ObjectStorageUploadStore { backend }
    }
}

#[async_trait]
impl<B: ObjectStorageBackend + ?Sized> UploadStore for ObjectStorageUploadStore<B> {
    async fn get(&self, key: &str) -> Result<Vec<u8>, String> {
        self.backend.get(key).await
    }

    async fn upload(&self, key: &str, data: &[u8]) -> Result<i64, String> {
        self.backend.upload(key, data.to_vec()).await
    }

    async fn delete(&self, key: &str) -> Result<(), String> {
        self.backend.delete(key).await
    }

    async fn list(&self, prefix: &str) -> Result<UploadList, String> {
        Ok(UploadList {
            keys: self.backend.list_objects(prefix).await?,
            error: None,
        })
    }
}

/// `object.Storage` behind the janitor's `JobLogsBlobStore` seam.
pub struct ObjectStorageLogBlobStore<B: ObjectStorageBackend + ?Sized> {
    backend: Arc<B>,
}

impl<B: ObjectStorageBackend + ?Sized> ObjectStorageLogBlobStore<B> {
    pub fn new(backend: Arc<B>) -> ObjectStorageLogBlobStore<B> {
        ObjectStorageLogBlobStore { backend }
    }
}

#[async_trait]
impl<B: ObjectStorageBackend + ?Sized> crate::jobs::exhaustive_search_janitor::JobLogsBlobStore
    for ObjectStorageLogBlobStore<B>
{
    async fn upload(&self, key: &str, contents: Vec<u8>) -> Result<i64, String> {
        self.backend.upload(key, contents).await
    }

    async fn delete(&self, key: &str) -> Result<(), String> {
        self.backend.delete(key).await
    }
}

/// Run an upload future from a synchronous `MatchWriter` call.
///
/// The worker runs on a multi-threaded tokio runtime, where `block_in_place`
/// lets us wait for the async object store without stalling the other worker
/// threads. A current-thread runtime (or no runtime at all) cannot be blocked,
/// so the upload is handed to a dedicated thread with its own current-thread
/// runtime.
fn run_upload_blocking(uploader: Arc<BlobUploader>, data: Vec<u8>) -> Result<(), String> {
    let upload = async move { uploader.upload_blob(&data).await };
    match tokio::runtime::Handle::try_current() {
        Ok(handle)
            if matches!(
                handle.runtime_flavor(),
                tokio::runtime::RuntimeFlavor::MultiThread
            ) =>
        {
            tokio::task::block_in_place(|| handle.block_on(upload))
        }
        Ok(_) | Err(_) => std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|err| format!("building object storage upload runtime: {err}"))?;
            runtime.block_on(upload)
        })
        .join()
        .map_err(|_| "object storage upload thread panicked".to_owned())?,
    }
}

/// `MatchWriter` over the service's object-store [`UploadStore`]: the same
/// buffering, sharding and flush rules as [`BufferedWriter`]/[`BlobUploader`],
/// exposed through the synchronous [`MatchWriter`] seam.
pub struct ObjectStorageMatchWriter {
    uploader: Arc<BlobUploader>,
    flush_size: usize,
    buf: Mutex<Vec<u8>>,
}

impl ObjectStorageMatchWriter {
    fn new(uploader: Arc<BlobUploader>, flush_size: usize) -> ObjectStorageMatchWriter {
        ObjectStorageMatchWriter {
            uploader,
            flush_size,
            // Go grows the buffer up front; do the same (capped at 1 MiB).
            buf: Mutex::new(Vec::with_capacity(flush_size.min(1024 * 1024))),
        }
    }

    fn lock_buf(&self) -> std::sync::MutexGuard<'_, Vec<u8>> {
        self.buf.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

impl MatchWriter for ObjectStorageMatchWriter {
    fn write(&self, event_match: serde_json::Value) -> Result<(), String> {
        let data = {
            let mut buf = self.lock_buf();
            let old_len = buf.len();
            if let Err(err) = serde_json::to_writer(&mut *buf, &event_match) {
                // Reset the buffer to where it was before failing to marshal.
                buf.truncate(old_len);
                return Err(err.to_string());
            }
            // `json.Encoder.Encode` terminates every value with a newline.
            buf.push(b'\n');

            if buf.len() >= self.flush_size {
                Some(buf.split_off(0))
            } else {
                None
            }
        };

        match data {
            Some(data) => run_upload_blocking(Arc::clone(&self.uploader), data),
            None => Ok(()),
        }
    }

    fn flush(&self) -> Result<(), String> {
        let data = {
            let mut buf = self.lock_buf();
            if buf.is_empty() {
                None
            } else {
                Some(buf.split_off(0))
            }
        };

        match data {
            Some(data) => run_upload_blocking(Arc::clone(&self.uploader), data),
            None => Ok(()),
        }
    }
}

/// `service.NewJSONWriter` behind the worker's [`MatchWriterFactory`] seam.
pub struct ObjectStorageMatchWriterFactory {
    store: Arc<dyn UploadStore>,
    flush_size: usize,
}

impl ObjectStorageMatchWriterFactory {
    pub fn new(store: Arc<dyn UploadStore>) -> ObjectStorageMatchWriterFactory {
        ObjectStorageMatchWriterFactory {
            store,
            flush_size: BUFFERED_JSON_WRITER_FLUSH_SIZE,
        }
    }

    /// Test hook for `newBufferedWriter(flushSize, ...)`; production always
    /// uses the 100 MiB default above.
    pub fn with_flush_size(mut self, flush_size: usize) -> ObjectStorageMatchWriterFactory {
        self.flush_size = flush_size;
        self
    }

    pub fn flush_size(&self) -> usize {
        self.flush_size
    }
}

impl MatchWriterFactory for ObjectStorageMatchWriterFactory {
    fn new_writer(&self, key: &str) -> Result<Box<dyn MatchWriter>, String> {
        Ok(Box::new(ObjectStorageMatchWriter::new(
            Arc::new(BlobUploader::new(Arc::clone(&self.store), key)),
            self.flush_size,
        )))
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    #[derive(Default)]
    struct Recorder {
        uploads: Mutex<Vec<(String, Vec<u8>)>>,
        fail: Option<String>,
    }

    #[async_trait]
    impl BlobUpload for Recorder {
        async fn upload_blob(&self, data: &[u8]) -> Result<(), String> {
            if let Some(err) = &self.fail {
                return Err(err.clone());
            }
            let mut uploads = self.uploads.lock().unwrap();
            let shard = uploads.len();
            uploads.push((format!("blob-{shard}"), data.to_vec()));
            Ok(())
        }
    }

    #[tokio::test]
    async fn buffered_writer_splits_at_threshold() {
        let recorder = Arc::new(Recorder::default());
        let mut w = BufferedWriter::new(24, recorder.clone());

        // `{"Key":"a"}\n` is 12 bytes.
        w.append(&serde_json::json!({"Key": "a"})).await.unwrap();
        assert_eq!(w.len(), 12);
        w.append(&serde_json::json!({"Key": "b"})).await.unwrap();
        // Reaching the threshold flushes immediately.
        assert!(w.is_empty());
        w.append(&serde_json::json!({"Key": "c"})).await.unwrap();
        assert_eq!(w.len(), 12);
        w.flush().await.unwrap();

        let uploads = recorder.uploads.lock().unwrap();
        assert_eq!(uploads.len(), 2);
        assert_eq!(uploads[0].0, "blob-0");
        assert_eq!(
            uploads[0].1.as_slice(),
            b"{\"Key\":\"a\"}\n{\"Key\":\"b\"}\n"
        );
        assert_eq!(uploads[1].0, "blob-1");
        assert_eq!(uploads[1].1.as_slice(), b"{\"Key\":\"c\"}\n");
    }

    #[tokio::test]
    async fn buffered_writer_skips_empty_flush() {
        let recorder = Arc::new(Recorder::default());
        let mut w = BufferedWriter::new(24, recorder.clone());
        w.flush().await.unwrap();
        assert!(recorder.uploads.lock().unwrap().is_empty());
    }

    #[test]
    fn csv_quotes_like_go() {
        assert!(!csv_field_needs_quotes("abc"));
        assert!(!csv_field_needs_quotes(""));
        assert!(csv_field_needs_quotes("a,b"));
        assert!(csv_field_needs_quotes("a\"b"));
        assert!(csv_field_needs_quotes("a\nb"));
        assert!(csv_field_needs_quotes(" leading"));
        assert!(csv_field_needs_quotes("\\."));

        let mut out = Vec::new();
        csv_write_record(&mut out, &["a,b", "plain"]);
        assert_eq!(out, b"\"a,b\",plain\n");
    }

    #[test]
    fn format_null_and_timestamp() {
        assert_eq!(format_or_null(None), "NULL");
        let t = OffsetDateTime::from_unix_timestamp(0).unwrap();
        assert_eq!(format_or_null(Some(t)), "1970-01-01T00:00:00Z");
    }
}
