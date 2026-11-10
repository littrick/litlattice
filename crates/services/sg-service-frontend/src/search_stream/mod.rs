//! `/.api/search/stream`: the frontend search event stream.
//!
//! Mirrors `cmd/frontend/internal/search/search.go` (server side) and the
//! `internal/search/streaming*` packages. Search planning/execution and the
//! repository metadata lookups sit behind [`SearchDeps`]; the production
//! implementation ([`BackendDeps`]) wires Postgres for metadata but reports
//! that the search backend itself is not ported yet.
//!
//! See `docs/overviews/m9_8ew_frontend_search_stream_overview.md`.
//!
//! The response body is assembled before it is returned (the event loop is
//! sequential: `tokio::select!` over the execution stream and the baseline's
//! flush/progress timers, no background tasks). The byte stream is still
//! identical to the baseline's; a chunked body can replace the buffer once a
//! search backend exists.

pub mod args;
pub mod convert;
pub mod events;
pub mod filters;
pub mod json;
pub mod progress;
pub mod zoekt_results;
pub mod zoekt_search;

pub use args::parse_url_query;

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use bytes::Bytes;
use flate2::write::GzEncoder;
use flate2::Compression;
use http::{header, HeaderName, HeaderValue, Request, Response};
use http_body_util::BodyExt;
use time::OffsetDateTime;
use tokio::time::{sleep_until, Duration, Instant as TokioInstant};
use tokio_stream::Stream;
use tonic::body::BoxBody;

use self::args::Args;
use self::convert::{FromMatchOptions, Match, RepoMetadata};
use self::events::{EventAlert, EventFilter, EventMatch};
use self::filters::SearchFilters;
use self::progress::{Progress, ProgressAggregator, SearchEvent};

/// Search inputs produced by planning a query (`search.Inputs`).
#[derive(Debug, Clone)]
pub struct SearchInputs {
    /// `Inputs.MaxResults()`.
    pub max_results: i64,
    pub original_query: String,
}

/// A planned search error (`*client.QueryError` or any other error).
#[derive(Debug, Clone)]
pub enum PlanError {
    /// `*client.QueryError`; the handler turns it into an `alert` event via
    /// `search.AlertForQuery`. `expected_operand` selects the specialised
    /// message for `*query.ExpectedOperand`.
    Query {
        message: String,
        expected_operand: bool,
    },
    /// Any other error: an `error` event.
    Other(String),
}

/// One item of the execution stream. `client.Execute` returns
/// `(alert, err)` after the stream ends; the equivalent tail items are
/// [`ExecuteItem::Alert`] and [`ExecuteItem::Error`].
pub enum ExecuteItem {
    Event(SearchEvent),
    /// A non-nil `*search.Alert` returned by `Execute`.
    Alert(EventAlert),
    /// `Execute` returned an error. `unsupported` mirrors
    /// `*query.UnsupportedError`, which is reported as an alert.
    Error {
        message: String,
        unsupported: bool,
    },
}

/// The seams the handler needs from the rest of the system.
#[async_trait]
pub trait SearchDeps: Send + Sync {
    /// `searchClient.Plan`.
    async fn plan(&self, args: &Args) -> Result<SearchInputs, PlanError>;

    /// `searchClient.Execute` as an item stream. The stream ends when the
    /// baseline `Execute` would return.
    async fn execute(
        &self,
        inputs: &SearchInputs,
        query: &str,
    ) -> Pin<Box<dyn Stream<Item = ExecuteItem> + Send>>;

    /// `db.Repos().Metadata(ids...)` (`getEventRepoMetadata`).
    async fn repo_metadata(&self, ids: &[i32]) -> Result<HashMap<i32, RepoMetadata>, String>;

    /// Best-effort ID → name lookup used by progress "skipped" messages
    /// (`streamclient.RepoNamer`).
    async fn repo_names(&self, ids: &[i32]) -> Vec<(i32, String)>;

    /// `actor.FromContext(ctx).IsAuthenticated()`; gates the
    /// `zoekt-search-opts` override. No actor middleware is ported, so the
    /// production implementation reports `false`.
    fn is_authenticated(&self) -> bool;
}

/// Production deps: Postgres-backed metadata; the search backend itself is
/// not ported yet, so [`SearchDeps::plan`] fails and the handler streams an
/// `error` event (see the overview's errata).
pub struct BackendDeps {
    pool: sqlx::PgPool,
}

impl BackendDeps {
    pub fn new(pool: sqlx::PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl SearchDeps for BackendDeps {
    async fn plan(&self, _args: &Args) -> Result<SearchInputs, PlanError> {
        Err(PlanError::Other(
            "search backend is not ported yet".to_owned(),
        ))
    }

    async fn execute(
        &self,
        _inputs: &SearchInputs,
        _query: &str,
    ) -> Pin<Box<dyn Stream<Item = ExecuteItem> + Send>> {
        Box::pin(tokio_stream::empty())
    }

    async fn repo_metadata(&self, ids: &[i32]) -> Result<HashMap<i32, RepoMetadata>, String> {
        let store = sg_db::repos::RepoStore::new(self.pool.clone());
        store
            .metadata(ids)
            .await
            .map(|rows| {
                rows.into_iter()
                    .map(|row| {
                        let metadata = RepoMetadata {
                            id: row.id,
                            name: row.name,
                            description: row.description,
                            fork: row.fork,
                            archived: row.archived,
                            private: row.private,
                            stars: row.stars,
                            last_fetched: row.last_fetched,
                            key_value_pairs: convert_key_value_pairs(row.key_value_pairs),
                            topics: row.topics,
                        };
                        (metadata.id, metadata)
                    })
                    .collect()
            })
            .map_err(|err| format!("fetch metadata from db: {err}"))
    }

    async fn repo_names(&self, ids: &[i32]) -> Vec<(i32, String)> {
        let store = sg_db::repos::RepoStore::new(self.pool.clone());
        match store.minimal_repo_names_by_ids(ids).await {
            Ok(names) => names,
            Err(err) => {
                tracing::warn!(error = %err, "streaming search RepoNamer failed to list names");
                Vec::new()
            }
        }
    }

    fn is_authenticated(&self) -> bool {
        false
    }
}

/// Search deps for entry points that do not have a database (tests of other
/// routes): every search request fails like an unported backend.
pub struct UnavailableDeps;

#[async_trait]
impl SearchDeps for UnavailableDeps {
    async fn plan(&self, _args: &Args) -> Result<SearchInputs, PlanError> {
        Err(PlanError::Other(
            "search backend is not ported yet".to_owned(),
        ))
    }

    async fn execute(
        &self,
        _inputs: &SearchInputs,
        _query: &str,
    ) -> Pin<Box<dyn Stream<Item = ExecuteItem> + Send>> {
        Box::pin(tokio_stream::empty())
    }

    async fn repo_metadata(&self, _ids: &[i32]) -> Result<HashMap<i32, RepoMetadata>, String> {
        Ok(HashMap::new())
    }

    async fn repo_names(&self, _ids: &[i32]) -> Vec<(i32, String)> {
        Vec::new()
    }

    fn is_authenticated(&self) -> bool {
        false
    }
}

/// `repo_kvps.Scan`: `json_object_agg` yields a `map[string]*string`.
fn convert_key_value_pairs(
    value: Option<serde_json::Value>,
) -> Option<std::collections::BTreeMap<String, Option<String>>> {
    let serde_json::Value::Object(map) = value? else {
        return None;
    };
    if map.is_empty() {
        return None;
    }
    Some(
        map.into_iter()
            .map(|(key, value)| {
                let value = match value {
                    serde_json::Value::Null => None,
                    serde_json::Value::String(value) => Some(value),
                    other => Some(other.to_string()),
                };
                (key, value)
            })
            .collect(),
    )
}

/// `ServeHTTP` (plus the gzip middleware): the entry point used by
/// [`crate::external_api`].
pub async fn search_response<B>(req: &Request<B>, deps: Arc<dyn SearchDeps>) -> Response<BoxBody> {
    let gzip = accepts_gzip(req);
    let body = serve_http(req, deps).await;
    let body = if gzip {
        Bytes::from(gzip_bytes(&body))
    } else {
        Bytes::from(body)
    };

    let mut response = Response::new(bytes_body(body));
    let headers = response.headers_mut();
    // `streamhttp.NewWriter`.
    headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("text/event-stream"),
    );
    headers.insert(header::CACHE_CONTROL, HeaderValue::from_static("no-cache"));
    headers.insert(header::CONNECTION, HeaderValue::from_static("keep-alive"));
    headers.insert(
        header::TRANSFER_ENCODING,
        HeaderValue::from_static("chunked"),
    );
    headers.insert(
        HeaderName::from_static("x-accel-buffering"),
        HeaderValue::from_static("no"),
    );
    // `gziphandler` always adds Vary, whether or not it compresses.
    headers.append(header::VARY, HeaderValue::from_static("Accept-Encoding"));
    if gzip {
        headers.insert(header::CONTENT_ENCODING, HeaderValue::from_static("gzip"));
    }
    response
}

/// A fully buffered body with an infallible error type.
fn bytes_body(bytes: Bytes) -> BoxBody {
    http_body_util::Full::new(bytes)
        .map_err(|never| match never {})
        .boxed_unsync()
}

fn gzip_bytes(data: &[u8]) -> Vec<u8> {
    use std::io::Write;
    // `gziphandler` is configured with `gzip.BestSpeed`; flate2's fastest
    // level is the closest equivalent (the bytes are not comparable across
    // implementations, only the decompressed payload is).
    let mut encoder = GzEncoder::new(Vec::new(), Compression::fast());
    let _ = encoder.write_all(data);
    encoder.finish().unwrap_or_default()
}

/// `acceptsGzip`: the parsed `Accept-Encoding` q-value for `gzip` is > 0.
fn accepts_gzip<B>(req: &Request<B>) -> bool {
    let Some(value) = req.headers().get(header::ACCEPT_ENCODING) else {
        return false;
    };
    let Ok(value) = value.to_str() else {
        return false;
    };
    let mut q = 0.0f64;
    for part in value.split(',') {
        let mut pieces = part.split(';');
        let coding = pieces.next().unwrap_or("").trim().to_ascii_lowercase();
        let mut qvalue = 1.0f64;
        for piece in pieces {
            let piece = piece.trim();
            if let Some(raw) = piece.strip_prefix("q=") {
                qvalue = raw.parse::<f64>().unwrap_or(1.0).clamp(0.0, 1.0);
            }
        }
        if coding == "gzip" {
            q = qvalue;
        }
    }
    q > 0.0
}

/// The SSE framing (`streamhttp.Writer`) writing into a buffer.
struct SseWriter {
    out: Vec<u8>,
}

impl SseWriter {
    fn new() -> Self {
        Self { out: Vec::new() }
    }

    /// `Writer.Event`: the data is already JSON-encoded.
    fn event(&mut self, name: &str, data: &str) {
        self.event_bytes(name, data.as_bytes());
    }

    /// `Writer.EventBytes`; payloads over `maxPayloadSize` are dropped like
    /// the baseline (callers ignore the returned error).
    fn event_bytes(&mut self, name: &str, data: &[u8]) {
        if 16 + name.len() + data.len() > json::MAX_PAYLOAD_SIZE {
            return;
        }
        if !name.is_empty() {
            self.out.extend_from_slice(b"event: ");
            self.out.extend_from_slice(name.as_bytes());
            self.out.push(b'\n');
        }
        self.out.extend_from_slice(b"data: ");
        self.out.extend_from_slice(data);
        self.out.extend_from_slice(b"\n\n");
    }

    fn done(&mut self) {
        self.event("done", "{}");
    }
}

/// `streamhttp.JSONArrayBuf`: accumulate marshalled matches into a JSON array,
/// flushing a `matches` event once the buffer reaches 32 KiB.
struct JsonArrayBuf {
    buf: Vec<u8>,
    flush_size: usize,
}

impl JsonArrayBuf {
    fn new(flush_size: usize) -> Self {
        Self {
            buf: Vec::new(),
            flush_size,
        }
    }

    fn append(&mut self, encoded: &[u8], writer: &mut SseWriter) {
        if self.buf.is_empty() {
            self.buf.push(b'[');
        } else {
            self.buf.push(b',');
        }
        self.buf.extend_from_slice(encoded);
        if self.buf.len() >= self.flush_size {
            self.flush(writer);
        }
    }

    fn flush(&mut self, writer: &mut SseWriter) {
        if self.buf.is_empty() {
            return;
        }
        let mut payload = std::mem::take(&mut self.buf);
        payload.push(b']');
        writer.event_bytes("matches", &payload);
    }
}

/// `eventHandler` without the background timers: the caller drives
/// [`EventHandler::flush_tick`]/[`EventHandler::progress_tick`] from the
/// event loop.
struct EventHandler {
    writer: SseWriter,
    matches_buf: JsonArrayBuf,
    filters: SearchFilters,
    progress: ProgressAggregator,
    display_filter: DisplayFilter,
    enable_chunk_matches: bool,
    max_line_len: i64,
    first: bool,
}

impl EventHandler {
    /// `eventHandler.Send`.
    async fn send(&mut self, event: SearchEvent, deps: &dyn SearchDeps) {
        self.progress.update(&event);
        self.filters.update(&event);

        // Stats have been computed; apply the display limits.
        let results = self.display_filter.limit(event.results);

        let mut ids: Vec<i32> = Vec::new();
        for match_ in &results {
            let id = match_.repo_name().id;
            if !ids.contains(&id) {
                ids.push(id);
            }
        }
        let metadata = if ids.is_empty() {
            HashMap::new()
        } else {
            match deps.repo_metadata(&ids).await {
                Ok(metadata) => metadata,
                Err(err) => {
                    tracing::error!(error = %err, "failed to get repo metadata");
                    return;
                }
            }
        };

        let now = OffsetDateTime::now_utc();
        for match_ in &results {
            let repo = match_.repo_name();
            // Drop matches without a metadata row for the same repo (a sign
            // the actor may not have access to it).
            let Some(metadata_row) = metadata.get(&repo.id) else {
                continue;
            };
            if metadata_row.name != repo.name {
                continue;
            }
            let event_match: EventMatch = convert::from_match(
                match_,
                &metadata,
                FromMatchOptions {
                    chunk_matches: self.enable_chunk_matches,
                    max_content_line_length: self.max_line_len,
                    now,
                },
            );
            let encoded = event_match.encode();
            self.matches_buf
                .append(encoded.as_bytes(), &mut self.writer);
        }

        // Instantly send results if we have not sent any yet.
        if self.first && !results.is_empty() {
            self.first = false;
            self.write_filters(false);
            self.matches_buf.flush(&mut self.writer);
        }
    }

    /// `eventHandler.flushTick`.
    async fn flush_tick(&mut self, deps: &dyn SearchDeps) {
        if self.filters.dirty {
            self.write_filters(false);
        }
        self.matches_buf.flush(&mut self.writer);
        if self.progress.dirty {
            let progress = self.progress.current(deps).await;
            self.write_progress(&progress);
        }
    }

    /// `eventHandler.progressTick`.
    async fn progress_tick(&mut self, deps: &dyn SearchDeps) {
        let progress = self.progress.current(deps).await;
        self.write_progress(&progress);
    }

    /// `eventWriter.Filters`.
    fn write_filters(&mut self, exhaustive: bool) {
        let computed = self.filters.compute();
        if computed.is_empty() {
            return;
        }
        let filters: Vec<EventFilter> = computed
            .into_iter()
            .map(|filter| EventFilter {
                value: filter.value,
                label: filter.label,
                count: filter.count,
                exhaustive,
                kind: filter.kind,
            })
            .collect();
        let encoded = events::encode_filters_json(&filters);
        self.writer.event("filters", &encoded);
    }

    /// `eventWriter.Progress`.
    fn write_progress(&mut self, progress: &Progress) {
        let encoded = progress.encode();
        self.writer.event("progress", &encoded);
    }

    /// `eventHandler.Done`.
    async fn done(&mut self, deps: &dyn SearchDeps) {
        let exhaustive = !self.progress.stats.is_limit_hit
            && !self
                .progress
                .stats
                .status
                .any(progress::repo_status::LIMIT_HIT)
            && !self
                .progress
                .stats
                .status
                .any(progress::repo_status::TIMED_OUT);
        self.write_filters(exhaustive);
        self.matches_buf.flush(&mut self.writer);
        let progress = self.progress.final_event(deps).await;
        self.write_progress(&progress);
    }
}

/// `displayFilter` (`display.go`).
struct DisplayFilter {
    match_limit: i64,
    matches_remaining: i64,
}

impl DisplayFilter {
    fn new(args: &Args, max_results: i64) -> Self {
        let mut display_limit = args.display;
        if display_limit < 0 || display_limit > max_results {
            display_limit = max_results;
        }
        Self {
            match_limit: display_limit,
            matches_remaining: display_limit,
        }
    }

    /// `Matches.Limit`: `matchLimit` is always positive here (it bounds
    /// `MaxResults`), so the "single match cannot be zero" invariant holds.
    fn limit(&mut self, matches: Vec<Match>) -> Vec<Match> {
        let mut out = Vec::with_capacity(matches.len());
        let mut remaining = self.matches_remaining;
        for mut match_ in matches {
            if remaining <= 0 {
                break;
            }
            remaining = match_.limit(remaining);
            out.push(match_);
        }
        self.matches_remaining = remaining;
        out
    }
}

/// `serveHTTP` (`search.go:115-200`): returns the complete event stream body.
async fn serve_http<B>(req: &Request<B>, deps: Arc<dyn SearchDeps>) -> Vec<u8> {
    let start = Instant::now();
    let mut writer = SseWriter::new();

    let args = match parse_url_query(req.uri().query()) {
        Ok(args) => args,
        Err(message) => {
            writer.event("error", &events::encode_error(&message));
            writer.done();
            return writer.out;
        }
    };

    let inputs = match deps.plan(&args).await {
        Ok(inputs) => inputs,
        Err(PlanError::Query {
            message,
            expected_operand,
        }) => {
            let alert = events::alert_for_query(&message, expected_operand);
            writer.event("alert", &events::encode_alert(&alert));
            writer.done();
            return writer.out;
        }
        Err(PlanError::Other(message)) => {
            writer.event("error", &events::encode_error(&message));
            writer.done();
            return writer.out;
        }
    };

    let display_filter = DisplayFilter::new(&args, inputs.max_results);
    let progress = ProgressAggregator::new(
        start,
        inputs.max_results,
        display_filter.match_limit,
        String::new(),
    );
    let mut handler = EventHandler {
        writer,
        matches_buf: JsonArrayBuf::new(32 * 1024),
        filters: SearchFilters::new(),
        progress,
        display_filter,
        enable_chunk_matches: args.enable_chunk_matches,
        max_line_len: args.max_line_len,
        first: true,
    };

    // `streaming.NewBatchingStream(50ms, eventHandler)`.
    let mut batch = BatchingStream::new(Duration::from_millis(50));
    let mut flush_timer = TokioInstant::now() + Duration::from_millis(200);
    let mut progress_timer = TokioInstant::now() + Duration::from_secs(5);

    let mut stream = deps.execute(&inputs, &args.query).await;
    let mut tail_alert: Option<EventAlert> = None;
    let mut tail_error: Option<(String, bool)> = None;

    loop {
        let batch_deadline = batch.deadline.unwrap_or_else(|| {
            // The branch is disabled when no deadline is set.
            TokioInstant::now() + Duration::from_secs(3600)
        });
        tokio::select! {
            item = next_item(stream.as_mut()) => match item {
                Some(ExecuteItem::Event(event)) => {
                    batch.push(event, &mut handler, deps.as_ref()).await;
                }
                Some(ExecuteItem::Alert(alert)) => tail_alert = Some(alert),
                Some(ExecuteItem::Error { message, unsupported }) => {
                    tail_error = Some((message, unsupported));
                }
                None => break,
            },
            _ = sleep_until(batch_deadline), if batch.deadline.is_some() => {
                batch.flush(&mut handler, deps.as_ref()).await;
            },
            _ = sleep_until(flush_timer) => {
                handler.flush_tick(deps.as_ref()).await;
                flush_timer = TokioInstant::now() + Duration::from_millis(200);
            },
            _ = sleep_until(progress_timer) => {
                handler.progress_tick(deps.as_ref()).await;
                progress_timer = TokioInstant::now() + Duration::from_secs(5);
            },
        }
    }
    // `defer batchedStream.Done()`
    batch.flush(&mut handler, deps.as_ref()).await;
    // `defer eventHandler.Done()`
    handler.done(deps.as_ref()).await;

    // `if err != nil && errors.HasType[*query.UnsupportedError](err)`.
    if let Some((message, unsupported)) = &tail_error {
        if *unsupported {
            let alert = events::alert_for_query(message, false);
            handler.writer.event("alert", &events::encode_alert(&alert));
        }
    }
    if let Some(alert) = &tail_alert {
        handler.writer.event("alert", &events::encode_alert(alert));
    }
    if let Some((message, unsupported)) = &tail_error {
        if !*unsupported {
            handler
                .writer
                .event("error", &events::encode_error(message));
        }
    }

    // `defer eventWriter.Done()`
    handler.writer.done();
    handler.writer.out
}

/// Poll one item from the pinned stream.
async fn next_item(
    mut stream: Pin<&mut (dyn Stream<Item = ExecuteItem> + Send)>,
) -> Option<ExecuteItem> {
    std::future::poll_fn(|cx| Stream::poll_next(stream.as_mut(), cx)).await
}

/// `streaming.NewBatchingStream`: the first event with results is forwarded
/// immediately, everything else is merged until 50ms pass or the stream ends.
struct BatchingStream {
    max_delay: Duration,
    sent_first_event: bool,
    dirty: bool,
    batch: SearchEvent,
    deadline: Option<TokioInstant>,
}

impl BatchingStream {
    fn new(max_delay: Duration) -> Self {
        Self {
            max_delay,
            sent_first_event: false,
            dirty: false,
            batch: SearchEvent::default(),
            deadline: None,
        }
    }

    /// `batchingStream.Send`.
    async fn push(
        &mut self,
        event: SearchEvent,
        handler: &mut EventHandler,
        deps: &dyn SearchDeps,
    ) {
        // The baseline checks the *incoming* event for the first flush, but
        // forwards the merged batch.
        let incoming_had_results = !event.results.is_empty();
        self.batch.results.extend(event.results);
        self.batch.stats.update(&event.stats);
        self.dirty = true;

        if !self.sent_first_event && incoming_had_results {
            self.sent_first_event = true;
            self.flush(handler, deps).await;
            return;
        }

        if self.deadline.is_none() {
            self.deadline = Some(TokioInstant::now() + self.max_delay);
        }
    }

    /// `batchingStream.Done`.
    async fn flush(&mut self, handler: &mut EventHandler, deps: &dyn SearchDeps) {
        self.deadline = None;
        if self.dirty {
            let event = std::mem::take(&mut self.batch);
            self.dirty = false;
            handler.send(event, deps).await;
        }
    }
}
