#![forbid(unsafe_code)]

//! The search/compute half of the query runner
//! (`internal/insights/background/queryrunner/{search.go,errors.go}` plus the
//! HTTP calls in `internal/insights/query/streaming`).
//!
//! The provider is a trait instead of the baseline's closure map so the
//! handler can be exercised without a frontend; [`HttpStreamingSearch`] is the
//! production implementation against the frontend internal API.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use sqlx::postgres::PgPool;
use sqlx::types::time::{OffsetDateTime, PrimitiveDateTime};
use sqlx::Row;
use tokio_util::sync::CancellationToken;

use sg_config::LiveConfig;
use sg_db::repos::RepoStore;
use sg_insights::discovery::{MinimalRepo, RepoQueryExecutor};

use super::insights_store::{self, RecordSeriesPoint, RecordingTime, Series};
use super::store::SNAPSHOT_MODE;
use super::streaming::{
    self, ComputeTabulationResult, RepoResult, StreamDecoderEvents, TabulationResult,
};

/// `types.GenerationMethod`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GenerationMethod {
    /// `types.Search`.
    Search,
    /// `types.SearchCompute`.
    SearchCompute,
    /// `types.MappingCompute`.
    MappingCompute,
}

impl GenerationMethod {
    pub fn from_raw(raw: &str) -> Option<Self> {
        match raw {
            "search" => Some(Self::Search),
            "search-compute" => Some(Self::SearchCompute),
            "mapping-compute" => Some(Self::MappingCompute),
            _ => None,
        }
    }
}

/// `SearchTimeoutError`.
pub const SEARCH_TIMEOUT_ERROR: &str = "search timeout";

/// Whether an error (possibly wrapped by the handler) is
/// `SearchTimeoutError` (`errors.Is`).
pub fn is_search_timeout(message: &str) -> bool {
    message == SEARCH_TIMEOUT_ERROR || message.ends_with(": search timeout")
}

/// Whether a handler error is non-retryable (`TerminalStreamingError`).
pub fn is_terminal_streaming_error(message: &str) -> bool {
    message.contains(" terminal errors: ")
}

/// `%v` formatting of a `[]string`: `[a b c]`.
fn go_slice(messages: &[String]) -> String {
    format!("[{}]", messages.join(" "))
}

/// `stringifyStreamingError`.
pub fn stringify_streaming_error(
    messages: &[String],
    method: GenerationMethod,
    terminal: bool,
) -> String {
    let retryable = if terminal { " terminal" } else { "" };
    if method == GenerationMethod::SearchCompute {
        format!(
            "compute streaming search:{retryable} errors: {}",
            go_slice(messages)
        )
    } else {
        format!(
            "streaming search:{retryable} errors: {}",
            go_slice(messages)
        )
    }
}

/// `classifiedError`: "invalid query" errors are terminal.
pub fn classified_error(messages: &[String], method: GenerationMethod) -> String {
    let terminal = messages
        .iter()
        .any(|message| message.contains("invalid query"));
    stringify_streaming_error(messages, method, terminal)
}

/// The low-level streaming operations the handler dispatches to. Errors are
/// returned raw; [`search_provider`] and friends apply the baseline's
/// `streaming.Search` wrappers.
#[async_trait]
pub trait StreamingSearch: Send + Sync + 'static {
    async fn search(
        &self,
        query: &str,
        ctx: &CancellationToken,
    ) -> Result<TabulationResult, String>;

    async fn compute_match_context(
        &self,
        query: &str,
        ctx: &CancellationToken,
    ) -> Result<ComputeTabulationResult, String>;

    async fn compute_text_extra(
        &self,
        query: &str,
        ctx: &CancellationToken,
    ) -> Result<ComputeTabulationResult, String>;

    /// `streaming.Search` with the `RepoDecoder` tabulator, used by the
    /// historical backfill's repository-scope query executor.
    async fn repo_search(
        &self,
        query: &str,
        ctx: &CancellationToken,
    ) -> Result<RepoResult, String> {
        let _ = (query, ctx);
        Err("repo search is not supported by this provider".to_owned())
    }
}

async fn search_provider(
    provider: &dyn StreamingSearch,
    query: &str,
    ctx: &CancellationToken,
) -> Result<TabulationResult, String> {
    provider
        .search(query, ctx)
        .await
        .map_err(|err| format!("streaming.Search: {err}"))
}

async fn compute_match_context_provider(
    provider: &dyn StreamingSearch,
    query: &str,
    ctx: &CancellationToken,
) -> Result<ComputeTabulationResult, String> {
    provider
        .compute_match_context(query, ctx)
        .await
        .map_err(|err| format!("streaming.Compute: {err}"))
}

async fn compute_text_provider(
    provider: &dyn StreamingSearch,
    query: &str,
    ctx: &CancellationToken,
) -> Result<ComputeTabulationResult, String> {
    provider
        .compute_text_extra(query, ctx)
        .await
        .map_err(|err| format!("streaming.ComputeText: {err}"))
}

/// `url.QueryEscape` (`net/url` query component): unreserved ASCII is kept,
/// space becomes `+`, everything else is `%XX` uppercase.
pub fn go_query_escape(raw: &str) -> String {
    let mut out = String::with_capacity(raw.len());
    for byte in raw.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(char::from(byte));
            }
            b' ' => out.push('+'),
            _ => out.push_str(&format!("%{byte:02X}")),
        }
    }
    out
}

/// The production provider: `streaming.Search` / `Compute*Stream` against
/// `SRC_FRONTEND_INTERNAL`'s `/.internal` HTTP API.
pub struct HttpStreamingSearch {
    base_url: String,
    client: reqwest::Client,
}

impl HttpStreamingSearch {
    /// `internalapi.Client.URL` uses `SRC_FRONTEND_INTERNAL` (default
    /// `sourcegraph-frontend-internal`) with the baseline's address
    /// normalization.
    pub fn from_env() -> Self {
        Self::new(sg_config::ConfigClient::from_env().addr())
    }

    pub fn new(base_url: impl Into<String>) -> Self {
        Self {
            base_url: base_url.into().trim_end_matches('/').to_owned(),
            client: reqwest::Client::new(),
        }
    }

    fn search_url(&self, query: &str) -> String {
        format!(
            "{}/.internal/search/stream?cm=t&q={}&v=V3",
            self.base_url,
            go_query_escape(query)
        )
    }

    fn compute_url(&self, query: &str) -> String {
        format!(
            "{}/.internal/compute/stream?q={}",
            self.base_url,
            go_query_escape(query)
        )
    }

    async fn get_body(&self, url: String, ctx: &CancellationToken) -> Result<String, String> {
        let request = self
            .client
            .get(url)
            .header(reqwest::header::ACCEPT, "text/event-stream")
            .header(reqwest::header::USER_AGENT, "code-insights-backend");
        let response = tokio::select! {
            _ = ctx.cancelled() => return Err("context canceled".to_owned()),
            result = request.send() => result.map_err(|err| err.to_string())?,
        };
        tokio::select! {
            _ = ctx.cancelled() => Err("context canceled".to_owned()),
            result = response.text() => result.map_err(|err| err.to_string()),
        }
    }
}

#[async_trait]
impl StreamingSearch for HttpStreamingSearch {
    async fn search(
        &self,
        query: &str,
        ctx: &CancellationToken,
    ) -> Result<TabulationResult, String> {
        let url = self.search_url(query);
        let body = self.get_body(url, ctx).await?;
        streaming::decode_search(&body)
    }

    async fn compute_match_context(
        &self,
        query: &str,
        ctx: &CancellationToken,
    ) -> Result<ComputeTabulationResult, String> {
        let url = self.compute_url(query);
        let body = self.get_body(url, ctx).await?;
        streaming::decode_compute_match_context(&body)
    }

    async fn compute_text_extra(
        &self,
        query: &str,
        ctx: &CancellationToken,
    ) -> Result<ComputeTabulationResult, String> {
        let url = self.compute_url(query);
        let body = self.get_body(url, ctx).await?;
        streaming::decode_compute_text(&body)
    }

    async fn repo_search(
        &self,
        query: &str,
        ctx: &CancellationToken,
    ) -> Result<RepoResult, String> {
        let url = self.search_url(query);
        let body = self.get_body(url, ctx).await?;
        streaming::decode_repo(&body)
    }
}

/// `query.StreamingRepoQueryExecutor`: resolves a `repo:` criteria query to
/// the repositories it matches by running it through the streaming search API
/// with the repository tabulator.
pub struct StreamingRepoQueryExecutor {
    provider: Arc<dyn StreamingSearch>,
}

impl StreamingRepoQueryExecutor {
    pub fn new(provider: Arc<dyn StreamingSearch>) -> Self {
        Self { provider }
    }
}

#[async_trait]
impl RepoQueryExecutor for StreamingRepoQueryExecutor {
    /// `StreamingRepoQueryExecutor.ExecuteRepoList`.
    async fn execute_repo_list(&self, query: &str) -> Result<Vec<MinimalRepo>, String> {
        let result = self
            .provider
            .repo_search(query, &CancellationToken::new())
            .await
            .map_err(|err| format!("RepoDecoder: {err}"))?;

        if !result.events.skipped_reasons.is_empty() {
            tracing::error!(
                reasons = %go_slice(&result.events.skipped_reasons),
                query = %query,
                "repo search encountered skipped events"
            );
        }
        if !result.events.alerts.is_empty() {
            tracing::error!(
                alerts = %go_slice(&result.events.alerts),
                query = %query,
                "repo search encountered alert events"
            );
        }
        if !result.events.errors.is_empty() {
            return Err(format!(
                "streaming repo search: errors: {}",
                go_slice(&result.events.errors)
            ));
        }
        Ok(result.repos)
    }
}

/// `queryrunner.GetSearchHandlers`: the pipeline handler map keyed by
/// generation method, backed by the streaming providers.
pub fn get_search_handlers(
    provider: Arc<dyn StreamingSearch>,
    sub_repo: Arc<dyn SubRepoChecker>,
) -> HashMap<String, Arc<dyn crate::jobs::insights_pipeline::InsightsHandler>> {
    let mut handlers: HashMap<String, Arc<dyn crate::jobs::insights_pipeline::InsightsHandler>> =
        HashMap::new();
    for (raw, method) in [
        ("search", GenerationMethod::Search),
        ("search-compute", GenerationMethod::SearchCompute),
        ("mapping-compute", GenerationMethod::MappingCompute),
    ] {
        handlers.insert(
            raw.to_owned(),
            Arc::new(StreamingHandler {
                provider: Arc::clone(&provider),
                sub_repo: Arc::clone(&sub_repo),
                method,
            }),
        );
    }
    handlers
}

/// One `GetSearchHandlers` closure: builds the query-runner job view of a
/// pipeline search job and runs the matching recordings generator.
struct StreamingHandler {
    provider: Arc<dyn StreamingSearch>,
    sub_repo: Arc<dyn SubRepoChecker>,
    method: GenerationMethod,
}

#[async_trait]
impl crate::jobs::insights_pipeline::InsightsHandler for StreamingHandler {
    async fn handle(
        &self,
        job: &crate::jobs::insights_pipeline::SearchJob,
        _series: &Series,
        record_time: OffsetDateTime,
    ) -> Result<Vec<RecordSeriesPoint>, String> {
        let store_job = super::store::Job {
            series_id: job.series_id.clone(),
            search_query: job.search_query.clone(),
            record_time: job.record_time,
            persist_mode: job.persist_mode.clone(),
            dependent_frames: job
                .dependent_frames
                .iter()
                .map(|frame| PrimitiveDateTime::new(frame.date(), frame.time()))
                .collect(),
            cost: 0,
            priority: 0,
            id: 0,
            state: String::new(),
            failure_message: None,
            started_at: None,
            finished_at: None,
            process_after: None,
            num_resets: 0,
            num_failures: 0,
        };
        let ctx = CancellationToken::new();
        match self.method {
            GenerationMethod::Search => generate_search_recordings(
                &store_job,
                record_time,
                self.provider.as_ref(),
                self.sub_repo.as_ref(),
                &ctx,
            )
            .await
            .map_err(|err| format!("searchHandler: {err}")),
            GenerationMethod::SearchCompute => generate_compute_recordings(
                &store_job,
                record_time,
                self.provider.as_ref(),
                self.sub_repo.as_ref(),
                &ctx,
                false,
            )
            .await
            .map_err(|err| format!("computeHandler: {err}")),
            GenerationMethod::MappingCompute => generate_compute_recordings(
                &store_job,
                record_time,
                self.provider.as_ref(),
                self.sub_repo.as_ref(),
                &ctx,
                true,
            )
            .await
            .map_err(|err| format!("mappingComputeHandler: {err}")),
        }
    }
}

/// `authz.SubRepoPermissionChecker`'s `EnabledForRepoID` half.
#[async_trait]
pub trait SubRepoChecker: Send + Sync + 'static {
    async fn enabled_for_repo_id(&self, repo_id: i32) -> Result<bool, String>;
}

/// The checker the worker installs
/// (`authz.DefaultSubRepoPermsChecker = srp.NewSubRepoPermsClient(db.SubRepoPerms())`).
/// `subRepoPermissions.enabled` gates the store lookup, like the client's
/// `conf.Watch`; the baseline caches results for an hour.
pub struct PgSubRepoChecker {
    pool: PgPool,
    live_config: Arc<LiveConfig>,
}

impl PgSubRepoChecker {
    pub fn new(pool: PgPool, live_config: Arc<LiveConfig>) -> Self {
        Self { pool, live_config }
    }
}

#[async_trait]
impl SubRepoChecker for PgSubRepoChecker {
    async fn enabled_for_repo_id(&self, repo_id: i32) -> Result<bool, String> {
        if !self
            .live_config
            .current()
            .site
            .sub_repo_permissions_enabled()
        {
            return Ok(false);
        }
        let row = sqlx::query(
            "SELECT EXISTS(SELECT FROM repo WHERE id = $1 AND private = TRUE AND external_service_type = 'perforce')",
        )
        .bind(repo_id)
        .fetch_one(&self.pool)
        .await
        .map_err(|err| err.to_string())?;
        row.try_get::<bool, _>(0)
            .map_err(|err: sqlx::Error| err.to_string())
    }
}

/// `toRecording`: the point plus one point per dependent frame, all sharing
/// the repo/capture/persist mode.
pub fn to_recording(
    job: &super::store::Job,
    value: f64,
    record_time: OffsetDateTime,
    repo_name: &str,
    repo_id: i32,
    capture: Option<&str>,
) -> Vec<RecordSeriesPoint> {
    let base = RecordSeriesPoint {
        series_id: job.series_id.clone(),
        time: record_time,
        value,
        capture: capture.map(str::to_owned),
        repo_id: Some(repo_id),
        repo_name: Some(repo_name.to_owned()),
        persist_mode: job.persist_mode.clone(),
    };
    let mut args = Vec::with_capacity(job.dependent_frames.len() + 1);
    args.push(base.clone());
    for dependent in &job.dependent_frames {
        let mut point = base.clone();
        point.time = dependent.assume_utc();
        args.push(point);
    }
    args
}

fn log_skipped(job: &super::store::Job, events: &StreamDecoderEvents, compute: bool) {
    if events.skipped_reasons.is_empty() {
        return;
    }
    let kind = if compute { "compute search" } else { "search" };
    tracing::error!(
        series_id = %job.series_id,
        reasons = %go_slice(&events.skipped_reasons),
        query = %job.search_query,
        "{} encountered skipped events",
        kind
    );
}

/// `generateSearchRecordingsStream`.
pub async fn generate_search_recordings(
    job: &super::store::Job,
    record_time: OffsetDateTime,
    provider: &dyn StreamingSearch,
    sub_repo: &dyn SubRepoChecker,
    ctx: &CancellationToken,
) -> Result<Vec<RecordSeriesPoint>, String> {
    let result = search_provider(provider, &job.search_query, ctx).await?;
    log_skipped(job, &result.events, false);
    if !result.events.errors.is_empty() {
        return Err(classified_error(
            &result.events.errors,
            GenerationMethod::Search,
        ));
    }
    if result.events.did_timeout {
        return Err(SEARCH_TIMEOUT_ERROR.to_owned());
    }
    if !result.events.alerts.is_empty() {
        return Err(format!(
            "streaming search: alerts: {}",
            go_slice(&result.events.alerts)
        ));
    }

    let mut recordings = Vec::new();
    for matched in result.repo_counts.values() {
        match sub_repo.enabled_for_repo_id(matched.repository_id).await {
            Err(err) => {
                tracing::error!(
                    series_id = %job.series_id,
                    repo = %matched.repository_name,
                    error = %err,
                    "sub-repo permissions check errored"
                );
                continue;
            }
            Ok(true) => continue,
            Ok(false) => {}
        }
        recordings.extend(to_recording(
            job,
            f64::from(matched.match_count),
            record_time,
            &matched.repository_name,
            matched.repository_id,
            None,
        ));
    }
    Ok(recordings)
}

/// `generateComputeRecordingsStream`.
pub async fn generate_compute_recordings(
    job: &super::store::Job,
    record_time: OffsetDateTime,
    provider: &dyn StreamingSearch,
    sub_repo: &dyn SubRepoChecker,
    ctx: &CancellationToken,
    text_extra: bool,
) -> Result<Vec<RecordSeriesPoint>, String> {
    let result = if text_extra {
        compute_text_provider(provider, &job.search_query, ctx).await?
    } else {
        compute_match_context_provider(provider, &job.search_query, ctx).await?
    };
    log_skipped(job, &result.events, true);
    if !result.events.errors.is_empty() {
        return Err(classified_error(
            &result.events.errors,
            GenerationMethod::SearchCompute,
        ));
    }
    if !result.events.alerts.is_empty() {
        return Err(format!(
            "compute streaming search: alerts: {}",
            go_slice(&result.events.alerts)
        ));
    }

    let mut recordings = Vec::new();
    for matched in result.repo_counts.values() {
        match sub_repo.enabled_for_repo_id(matched.repository_id).await {
            Err(err) => {
                tracing::error!(
                    series_id = %job.series_id,
                    repo = %matched.repository_name,
                    error = %err,
                    "sub-repo permissions check errored"
                );
                continue;
            }
            Ok(true) => continue,
            Ok(false) => {}
        }
        for (captured, count) in &matched.value_counts {
            if captured.is_empty() {
                continue;
            }
            recordings.extend(to_recording(
                job,
                f64::from(*count),
                record_time,
                &matched.repository_name,
                matched.repository_id,
                Some(captured),
            ));
        }
    }
    Ok(recordings)
}

/// `filterRecordingsBySeriesRepos`: an unscoped series keeps every recording;
/// otherwise only points for the series' repository names survive.
pub async fn filter_recordings_by_series_repos(
    repo_store: &RepoStore,
    series: &Series,
    recordings: Vec<RecordSeriesPoint>,
) -> Result<Vec<RecordSeriesPoint>, String> {
    if series.repositories.is_empty() {
        return Ok(recordings);
    }
    let series_repos = repo_store
        .list_by_names(&series.repositories)
        .await
        .map_err(|err| format!("repoStore.List: {err}"))?;
    let repos: std::collections::HashSet<i32> = series_repos.iter().map(|repo| repo.id).collect();

    Ok(recordings
        .into_iter()
        .filter(|point| match point.repo_id {
            Some(id) => repos.contains(&id),
            None => false,
        })
        .collect())
}

/// `persistRecordings`: one transaction; a snapshot persist first deletes the
/// previous snapshot, then the points and the recording time are written.
pub async fn persist_recordings(
    pool: &PgPool,
    repo_store: &RepoStore,
    job: &super::store::Job,
    series: &Series,
    recordings: Vec<RecordSeriesPoint>,
    record_time: OffsetDateTime,
) -> Result<(), String> {
    let mut tx = pool.begin().await.map_err(|err| err.to_string())?;

    let snapshot = job.persist_mode == SNAPSHOT_MODE;
    if snapshot {
        insights_store::delete_snapshots(&mut tx, series)
            .await
            .map_err(|err| format!("DeleteSnapshots: {err}"))?;
    }
    let times = vec![RecordingTime {
        time: record_time,
        snapshot,
    }];

    let filtered = filter_recordings_by_series_repos(repo_store, series, recordings)
        .await
        .map_err(|err| format!("filterRecordingsBySeriesRepos: {err}"))?;

    insights_store::record_points_and_times(&mut tx, &filtered, series.id, &times)
        .await
        .map_err(|err| format!("RecordSeriesPointsAndRecordingTimes: {err}"))?;

    tx.commit().await.map_err(|err| err.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn query_escape_matches_go() {
        assert_eq!(
            go_query_escape("repo:^github.com/a/b$ type:file"),
            "repo%3A%5Egithub.com%2Fa%2Fb%24+type%3Afile"
        );
        assert_eq!(go_query_escape("a-_.~Z9"), "a-_.~Z9");
    }

    #[test]
    fn classified_errors_match_the_baseline_strings() {
        let retryable = vec!["boom".to_owned()];
        let terminal = vec!["invalid query".to_owned(), "boom".to_owned()];
        assert_eq!(
            classified_error(&retryable, GenerationMethod::Search),
            "streaming search: errors: [boom]"
        );
        assert_eq!(
            classified_error(&terminal, GenerationMethod::Search),
            "streaming search: terminal errors: [invalid query boom]"
        );
        assert_eq!(
            classified_error(&terminal, GenerationMethod::SearchCompute),
            "compute streaming search: terminal errors: [invalid query boom]"
        );
        assert!(is_terminal_streaming_error(&classified_error(
            &terminal,
            GenerationMethod::Search
        )));
        assert!(!is_terminal_streaming_error(&classified_error(
            &retryable,
            GenerationMethod::Search
        )));
        assert!(is_search_timeout("searchHandler: search timeout"));
        assert!(!is_search_timeout("searchHandler: boom"));
    }

    #[test]
    fn generation_methods_match_the_baseline_strings() {
        assert_eq!(
            GenerationMethod::from_raw("search"),
            Some(GenerationMethod::Search)
        );
        assert_eq!(
            GenerationMethod::from_raw("search-compute"),
            Some(GenerationMethod::SearchCompute)
        );
        assert_eq!(
            GenerationMethod::from_raw("mapping-compute"),
            Some(GenerationMethod::MappingCompute)
        );
        assert_eq!(GenerationMethod::from_raw("language-stats"), None);
    }
}
