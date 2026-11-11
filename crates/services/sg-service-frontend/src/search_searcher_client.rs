//! Searcher (`index:no`) backend seam (`internal/search/searcher/client.go`)
//! and its collaborators.
//!
//! The request/response types are **not** duplicated here: this module reuses
//! [`sg_service_searcher::protocol`] verbatim. The baseline `searcher.Search`
//! couples three concerns that this seam keeps separate so tests can inject
//! fakes:
//!
//! * [`SearcherClient`] — `searcherURLs.Endpoints()` plus `Search`'s archive
//!   fetch / consistent-hash endpoint routing / gRPC streaming;
//! * [`SearcherLimiter`] — the global mutable text-search limiter
//!   (`textSearchLimiter`);
//! * [`SearcherGitserver`] — `gitserver.Client.ResolveRevision` (and
//!   `GetDefaultBranch`) used to turn a revspec into a commit id;
//! * [`SymbolsClient`] — `symbols.Client.Search` used by the symbol job.
//!
//! [`GrpcSearcherClient`] is the real transport (consistent-hash routing +
//! gRPC streaming); [`default_searcher_client`] picks it when endpoints are
//! configured and falls back to [`NotPortedSearcher`] otherwise.
//!
//! See `docs/overviews/m9_8hx_searcher_jobs_overview.md` and
//! `docs/overviews/m9_8il_searcher_grpc_client_overview.md`.

use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use sg_proto::searcher as pb;
use tonic::transport::Channel;

pub use sg_service_searcher::protocol;

use crate::search_stream::convert;

/// Message used when the searcher backend has not been wired yet.
pub const NOT_PORTED_MESSAGE: &str = "searcher backend is not ported yet";

/// Classification of an error crossing the searcher seam. Mirrors the checks
/// `search.HandleRepoSearchResult` performs (`internal/search/repo_status.go`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SearcherErrorKind {
    /// Any other error (fatal for the repo search).
    #[default]
    Other,
    /// `gitdomain.RepoNotExistError`.
    RepoNotFound {
        /// `RepoNotExistError.CloneInProgress`.
        clone_in_progress: bool,
    },
    /// `gitdomain.RevisionNotFoundError`.
    RevisionNotFound,
    /// `errcode.IsNotFound` (but not a missing repo).
    NotFound,
    /// `errcode.IsTimeout`.
    Timeout,
    /// `errcode.IsTemporary`.
    Temporary,
}

/// Error returned by the searcher seam. `message` is the baseline error text;
/// `kind` carries the classification `HandleRepoSearchResult` needs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SearcherError {
    pub kind: SearcherErrorKind,
    pub message: String,
}

impl SearcherError {
    pub fn new(kind: SearcherErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
        }
    }

    /// A plain error (fatal for the repo search).
    pub fn other(message: impl Into<String>) -> Self {
        Self::new(SearcherErrorKind::Other, message)
    }

    /// The searcher backend is not wired yet.
    pub fn not_ported() -> Self {
        Self::other(NOT_PORTED_MESSAGE)
    }
}

impl fmt::Display for SearcherError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for SearcherError {}

/// `searcherURLs` + `searcher.Search`
/// (`internal/search/searcher/client.go:29-141`).
pub trait SearcherClient {
    /// `searcherURLs.Endpoints()`. `TextSearchJob.Run` uses the length to set
    /// the limiter and propagates the error unchanged.
    fn endpoints(&self) -> Result<Vec<String>, SearcherError>;

    /// `searcher.Search`: send `request` to a searcher endpoint, invoke
    /// `on_match` for every streamed `FileMatch`, and return `Done.LimitHit`.
    fn search(
        &self,
        request: &protocol::Request,
        on_match: &mut dyn FnMut(protocol::FileMatch),
    ) -> Result<bool, SearcherError>;
}

/// The global `textSearchLimiter` (`internal/limiter.MutableLimiter`).
pub trait SearcherLimiter {
    /// `MutableLimiter.SetLimit`: `len(endpoints) * 32` in the baseline.
    fn set_limit(&self, limit: usize);

    /// `MutableLimiter.Acquire`: acquire a search slot.
    fn acquire(&self) -> Result<(), SearcherError>;
}

/// The `gitserver.Client` methods the searcher jobs call.
///
/// This maps onto [`sg_git::GitserverClient`]: `resolve_revision` is
/// `ResolveRevision(..., EnsureRevision: false)` and `default_branch` is
/// `GetDefaultBranch(repo, false)` (full ref name). As in gitserver, an empty
/// revspec resolves to `HEAD`, i.e. the repository's default branch.
pub trait SearcherGitserver {
    /// `gitserver.Client.ResolveRevision`.
    fn resolve_revision(&self, repo: &str, revspec: &str) -> Result<String, SearcherError>;

    /// `gitserver.Client.GetDefaultBranch(repo, false)` (full ref name).
    fn default_branch(&self, repo: &str) -> Result<String, SearcherError>;
}

/// `search.SymbolsParameters` (`internal/search/types.go:101-141`).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct SymbolsParameters {
    pub repo: String,
    pub commit_id: String,
    pub query: String,
    pub is_regexp: bool,
    pub is_case_sensitive: bool,
    pub include_patterns: Vec<String>,
    pub exclude_pattern: String,
    pub include_langs: Vec<String>,
    pub exclude_langs: Vec<String>,
    /// `First`: the symbol job passes `limit + 1`.
    pub first: i32,
}

/// `search.SymbolsResponse` reduced to the fields the job consumes.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct SymbolsResponse {
    pub symbols: Vec<convert::Symbol>,
    pub limit_hit: bool,
}

/// `symbols.Client.Search` (`internal/symbols/client.go:58-104`).
pub trait SymbolsClient {
    fn search(&self, params: &SymbolsParameters) -> Result<SymbolsResponse, SearcherError>;
}

/// The injected searcher collaborators. `SearchJob` runners reach this through
/// [`crate::search_job::RuntimeClients::searcher`].
pub struct SearcherClients<'a> {
    pub client: &'a dyn SearcherClient,
    pub limiter: &'a dyn SearcherLimiter,
    pub gitserver: &'a dyn SearcherGitserver,
    pub symbols: &'a dyn SymbolsClient,
}

/// Default implementations used when no real client is wired: every method
/// reports [`NOT_PORTED_MESSAGE`].
#[derive(Debug, Clone, Copy, Default)]
pub struct NotPortedSearcher;

impl SearcherClient for NotPortedSearcher {
    fn endpoints(&self) -> Result<Vec<String>, SearcherError> {
        Err(SearcherError::not_ported())
    }

    fn search(
        &self,
        _request: &protocol::Request,
        _on_match: &mut dyn FnMut(protocol::FileMatch),
    ) -> Result<bool, SearcherError> {
        Err(SearcherError::not_ported())
    }
}

impl SearcherLimiter for NotPortedSearcher {
    fn set_limit(&self, _limit: usize) {}

    fn acquire(&self) -> Result<(), SearcherError> {
        Err(SearcherError::not_ported())
    }
}

impl SearcherGitserver for NotPortedSearcher {
    fn resolve_revision(&self, _repo: &str, _revspec: &str) -> Result<String, SearcherError> {
        Err(SearcherError::not_ported())
    }

    fn default_branch(&self, _repo: &str) -> Result<String, SearcherError> {
        Err(SearcherError::not_ported())
    }
}

impl SymbolsClient for NotPortedSearcher {
    fn search(&self, _params: &SymbolsParameters) -> Result<SymbolsResponse, SearcherError> {
        Err(SearcherError::not_ported())
    }
}

/// Message used when the gRPC client has no configured endpoints.
pub const NO_ENDPOINTS_MESSAGE: &str = "searcher: no endpoints configured";

/// Connection seam for [`GrpcSearcherClient`]. Tests inject a connector that
/// returns clients dialing an in-process fake gRPC server; production uses
/// [`ChannelConnector`] (lazy plaintext HTTP/2, matching the baseline dial
/// configuration).
pub trait SearcherConnector: Send + Sync {
    fn connect(
        &self,
        endpoint: &str,
    ) -> Result<pb::searcher_service_client::SearcherServiceClient<Channel>, SearcherError>;
}

/// Default connector: `Channel::from_shared(...).connect_lazy()`.
///
/// The baseline parses the endpoint URL and dials its host; this keeps the
/// same cleartext HTTP/2 transport and does not add TLS or dial options.
#[derive(Debug, Default, Clone, Copy)]
pub struct ChannelConnector;

impl SearcherConnector for ChannelConnector {
    fn connect(
        &self,
        endpoint: &str,
    ) -> Result<pb::searcher_service_client::SearcherServiceClient<Channel>, SearcherError> {
        let uri = if endpoint.contains("://") {
            endpoint.to_owned()
        } else {
            format!("http://{endpoint}")
        };
        let channel = Channel::from_shared(uri)
            .map_err(|error| SearcherError::other(format!("failed to parse URL: {error}")))?
            .connect_lazy();
        Ok(pb::searcher_service_client::SearcherServiceClient::new(
            channel,
        ))
    }
}

/// Retry policy attached to every `Search` call (`retry_grpc.go` +
/// `defaults.RetryPolicy`): `Unavailable`/`ResourceExhausted` retried before
/// the first streamed message, bounded attempts and exponential backoff.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetryConfig {
    /// `SRC_GRPC_RETRY_MAX_ATTEMPTS` (default `20`, total attempts).
    pub max_attempts: u32,
    /// `SRC_GRPC_RETRY_DELAY_BASE` (default `50ms`).
    pub backoff_base: Duration,
    /// `SRC_GRPC_RETRY_MAX_DURATION` (default `3s`).
    pub backoff_max: Duration,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_attempts: 20,
            backoff_base: Duration::from_millis(50),
            backoff_max: Duration::from_secs(3),
        }
    }
}

impl RetryConfig {
    /// Read the baseline env knobs. The baseline applies full jitter; timing is
    /// not observable here, so this uses deterministic exponential backoff in
    /// `[base, max]`.
    pub fn from_env() -> Self {
        let default = Self::default();
        let max_attempts = std::env::var("SRC_GRPC_RETRY_MAX_ATTEMPTS")
            .ok()
            .and_then(|value| value.trim().parse::<u32>().ok())
            .filter(|attempts| *attempts > 0)
            .unwrap_or(default.max_attempts);
        let backoff_base = std::env::var("SRC_GRPC_RETRY_DELAY_BASE")
            .ok()
            .and_then(|value| parse_go_duration(&value))
            .unwrap_or(default.backoff_base);
        let backoff_max = std::env::var("SRC_GRPC_RETRY_MAX_DURATION")
            .ok()
            .and_then(|value| parse_go_duration(&value))
            .unwrap_or(default.backoff_max);
        Self {
            max_attempts,
            backoff_base,
            backoff_max: backoff_max.max(backoff_base),
        }
    }

    fn backoff(&self, attempt: u32) -> Duration {
        if attempt <= 1 {
            return self.backoff_base;
        }
        let factor = 1u64 << (attempt - 1).min(20);
        self.backoff_base
            .saturating_mul(factor.min(u64::from(u32::MAX)) as u32)
            .min(self.backoff_max)
    }
}

/// `time.ParseDuration` subset covering the baseline defaults and suffixes.
fn parse_go_duration(value: &str) -> Option<Duration> {
    let value = value.trim();
    let split = value
        .find(|c: char| c.is_ascii_alphabetic() || c == '\u{b5}')
        .unwrap_or(value.len());
    let (number, unit) = value.split_at(split);
    let number: f64 = number.parse().ok()?;
    let nanos = match unit {
        "ns" => number,
        "us" | "\u{b5}s" => number * 1_000.0,
        "ms" => number * 1_000_000.0,
        "s" => number * 1_000_000_000.0,
        "m" => number * 60.0 * 1_000_000_000.0,
        "h" => number * 3_600.0 * 1_000_000_000.0,
        _ => return None,
    };
    if !nanos.is_finite() || nanos < 0.0 {
        return None;
    }
    Some(Duration::from_nanos(nanos as u64))
}

/// Real gRPC transport for the [`SearcherClient`] seam
/// (`internal/search/searcher/client.go`).
pub struct GrpcSearcherClient {
    endpoints: Vec<String>,
    connector: Arc<dyn SearcherConnector>,
    retry: RetryConfig,
}

impl fmt::Debug for GrpcSearcherClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GrpcSearcherClient")
            .field("endpoints", &self.endpoints)
            .field("retry", &self.retry)
            .finish_non_exhaustive()
    }
}

impl GrpcSearcherClient {
    pub fn new(endpoints: Vec<String>) -> Self {
        Self {
            endpoints,
            connector: Arc::new(ChannelConnector),
            retry: RetryConfig::from_env(),
        }
    }

    /// Inject a test connector (for in-process fake gRPC servers).
    pub fn with_connector(mut self, connector: Arc<dyn SearcherConnector>) -> Self {
        self.connector = connector;
        self
    }

    pub fn with_retry_config(mut self, retry: RetryConfig) -> Self {
        self.retry = retry;
        self
    }

    pub fn is_configured(&self) -> bool {
        !self.endpoints.is_empty()
    }

    /// The configured endpoints, in `Endpoints()` order.
    pub fn configured_endpoints(&self) -> &[String] {
        &self.endpoints
    }

    /// `endpoint.Map.GetN(key, len(nodes))`: rendezvous order for
    /// `repo@commit`.
    pub fn route_order(&self, repo: &str, commit: &str) -> Vec<String> {
        rendezvous_order(&self.endpoints, &format!("{repo}@{commit}"))
    }

    fn try_search(
        &self,
        urls: &[String],
        attempt: usize,
        request: &pb::SearchRequest,
    ) -> (Vec<protocol::FileMatch>, Result<bool, SearcherError>) {
        let endpoint = urls[attempt % urls.len()].clone();
        let request = request.clone();
        run_to_completion(async move { self.search_endpoint_with_retry(&endpoint, &request).await })
    }

    async fn search_endpoint_with_retry(
        &self,
        endpoint: &str,
        request: &pb::SearchRequest,
    ) -> (Vec<protocol::FileMatch>, Result<bool, SearcherError>) {
        let max_attempts = self.retry.max_attempts.max(1);
        for attempt in 0..max_attempts {
            if attempt > 0 {
                tokio::time::sleep(self.retry.backoff(attempt)).await;
            }
            let AttemptOutcome {
                matches,
                limit_hit,
                error,
            } = self.search_endpoint(endpoint, request).await;
            match error {
                None => return (matches, Ok(limit_hit.unwrap_or(false))),
                Some(AttemptFailure::Status {
                    status,
                    received_first,
                }) => {
                    if is_retriable(status.code()) && !received_first && attempt + 1 < max_attempts
                    {
                        continue;
                    }
                    return (matches, Err(classify_status(&status)));
                }
                Some(AttemptFailure::Transport(error)) => return (matches, Err(error)),
            }
        }
        unreachable!("the retry loop always returns on its final attempt")
    }

    async fn search_endpoint(&self, endpoint: &str, request: &pb::SearchRequest) -> AttemptOutcome {
        let mut client = match self.connector.connect(endpoint) {
            Ok(client) => client,
            Err(error) => return AttemptOutcome::transport(error),
        };
        let mut stream = match client.search(request.clone()).await {
            Ok(response) => response.into_inner(),
            Err(status) => {
                return AttemptOutcome::status(status, false);
            }
        };
        let mut matches = Vec::new();
        let mut received_first = false;
        loop {
            match stream.message().await {
                Ok(Some(message)) => {
                    received_first = true;
                    match message.message {
                        Some(pb::search_response::Message::FileMatch(file_match)) => {
                            matches.push(protocol::FileMatch::from_proto(&file_match));
                        }
                        Some(pb::search_response::Message::DoneMessage(done)) => {
                            return AttemptOutcome {
                                matches,
                                limit_hit: Some(done.limit_hit),
                                error: None,
                            };
                        }
                        None => {
                            return AttemptOutcome {
                                matches,
                                limit_hit: None,
                                error: Some(AttemptFailure::Transport(SearcherError::other(
                                    "unknown SearchResponse message",
                                ))),
                            };
                        }
                    }
                }
                // Baseline treats `io.EOF` as a successful, non-limit-hit end.
                Ok(None) => {
                    return AttemptOutcome {
                        matches,
                        limit_hit: Some(false),
                        error: None,
                    };
                }
                // Matches streamed before the error already reached `onMatch`
                // in the baseline; keep them attached to the failure attempt.
                Err(status) => {
                    return AttemptOutcome {
                        matches,
                        limit_hit: None,
                        error: Some(AttemptFailure::Status {
                            status,
                            received_first,
                        }),
                    };
                }
            }
        }
    }
}

impl SearcherClient for GrpcSearcherClient {
    fn endpoints(&self) -> Result<Vec<String>, SearcherError> {
        if self.endpoints.is_empty() {
            Err(SearcherError::other(NO_ENDPOINTS_MESSAGE))
        } else {
            Ok(self.endpoints.clone())
        }
    }

    fn search(
        &self,
        request: &protocol::Request,
        on_match: &mut dyn FnMut(protocol::FileMatch),
    ) -> Result<bool, SearcherError> {
        let urls = self.route_order(&request.repo, &request.commit);
        if urls.is_empty() {
            return Err(SearcherError::other(NO_ENDPOINTS_MESSAGE));
        }
        let request = request.to_proto();

        let (mut matches, mut outcome) = self.try_search(&urls, 0, &request);
        for file_match in matches.drain(..) {
            on_match(file_match);
        }
        // Baseline `client.go:130`: `errcode.IsTemporary(err)` retries once.
        // After `contextconv`, only `DeadlineExceeded` is `Temporary`; it is
        // classified as `Timeout` here.
        if let Err(error) = &outcome {
            if error.kind == SearcherErrorKind::Timeout {
                let (mut retry_matches, retry_outcome) = self.try_search(&urls, 1, &request);
                for file_match in retry_matches.drain(..) {
                    on_match(file_match);
                }
                outcome = retry_outcome;
            }
        }
        outcome
    }
}

struct AttemptOutcome {
    matches: Vec<protocol::FileMatch>,
    limit_hit: Option<bool>,
    error: Option<AttemptFailure>,
}

impl AttemptOutcome {
    fn transport(error: SearcherError) -> Self {
        Self {
            matches: Vec::new(),
            limit_hit: None,
            error: Some(AttemptFailure::Transport(error)),
        }
    }

    fn status(status: tonic::Status, received_first: bool) -> Self {
        Self {
            matches: Vec::new(),
            limit_hit: None,
            error: Some(AttemptFailure::Status {
                status,
                received_first,
            }),
        }
    }
}

enum AttemptFailure {
    Status {
        status: tonic::Status,
        received_first: bool,
    },
    Transport(SearcherError),
}

/// Map a gRPC status to the seam's error classification. This is the
/// code-oriented contract used by [`crate::search_searcher_jobs`]:
/// `NotFound` -> missing repo, `DeadlineExceeded` -> timeout,
/// `Unavailable`/`ResourceExhausted` -> temporary, everything else fatal.
pub fn classify_status(status: &tonic::Status) -> SearcherError {
    let code = status.code();
    let kind = match code {
        tonic::Code::NotFound => SearcherErrorKind::NotFound,
        tonic::Code::DeadlineExceeded => SearcherErrorKind::Timeout,
        tonic::Code::Unavailable | tonic::Code::ResourceExhausted => SearcherErrorKind::Temporary,
        _ => SearcherErrorKind::Other,
    };
    let message = match code {
        // `contextconv` maps these to the std context errors on the Go side.
        tonic::Code::Cancelled => "context canceled".to_owned(),
        tonic::Code::DeadlineExceeded => "context deadline exceeded".to_owned(),
        _ => status.message().to_owned(),
    };
    SearcherError::new(kind, message)
}

fn is_retriable(code: tonic::Code) -> bool {
    matches!(
        code,
        tonic::Code::Unavailable | tonic::Code::ResourceExhausted
    )
}

/// Drive `future` to completion from a synchronous context (same runtime
/// strategy as `sg_zoekt::streamer`): `block_in_place` on a multi-thread
/// runtime, otherwise a dedicated thread with its own current-thread runtime.
pub(crate) fn run_to_completion<F>(future: F) -> F::Output
where
    F: std::future::Future + Send,
    F::Output: Send,
{
    match tokio::runtime::Handle::try_current() {
        Ok(handle) if handle.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread => {
            tokio::task::block_in_place(|| handle.block_on(future))
        }
        _ => std::thread::scope(|scope| {
            scope
                .spawn(move || {
                    let runtime = tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()
                        .expect("building the searcher blocking bridge runtime cannot fail");
                    runtime.block_on(future)
                })
                .join()
                .unwrap_or_else(|payload| std::panic::resume_unwind(payload))
        }),
    }
}

/// `endpoint.Map.GetN(key, len(nodes))` over the rendezvous hash
/// (`go-rendezvous`).
pub fn rendezvous_order(nodes: &[String], key: &str) -> Vec<String> {
    if nodes.is_empty() {
        return Vec::new();
    }
    let key_hash = xxhash64(key.as_bytes());
    let hashes: Vec<u64> = nodes
        .iter()
        .map(|node| xorshift_mult64(key_hash ^ xxhash64(node.as_bytes())))
        .collect();
    let mut order = Vec::with_capacity(nodes.len());
    let mut current = u64::MAX;
    for _ in 0..nodes.len() {
        let mut best = 0usize;
        for (index, hash) in hashes.iter().enumerate() {
            if *hash > hashes[best] && *hash < current {
                best = index;
            }
        }
        order.push(nodes[best].clone());
        current = hashes[best];
    }
    order
}

fn xorshift_mult64(mut value: u64) -> u64 {
    value ^= value >> 12;
    value ^= value << 25;
    value ^= value >> 27;
    value.wrapping_mul(2_685_821_657_736_338_717)
}

const PRIME64_1: u64 = 0x9E37_79B1_85EB_CA87;
const PRIME64_2: u64 = 0xC2B2_AE3D_27D4_EB4F;
const PRIME64_3: u64 = 0x1656_67B1_9E37_79F9;
const PRIME64_4: u64 = 0x85EB_CA77_C2B2_AE63;
const PRIME64_5: u64 = 0x27D4_EB2F_1656_67C5;

/// xxHash64 with seed 0, matching Go `xxhash.Sum64String`.
fn xxhash64(input: &[u8]) -> u64 {
    let len = input.len();
    let mut index = 0usize;
    let mut hash;
    if len >= 32 {
        let mut v1 = PRIME64_1.wrapping_add(PRIME64_2);
        let mut v2 = PRIME64_2;
        let mut v3 = 0u64;
        let mut v4 = 0u64.wrapping_sub(PRIME64_1);
        while index + 32 <= len {
            v1 = round64(v1, read_u64(&input[index..index + 8]));
            v2 = round64(v2, read_u64(&input[index + 8..index + 16]));
            v3 = round64(v3, read_u64(&input[index + 16..index + 24]));
            v4 = round64(v4, read_u64(&input[index + 24..index + 32]));
            index += 32;
        }
        hash = v1
            .rotate_left(1)
            .wrapping_add(v2.rotate_left(7))
            .wrapping_add(v3.rotate_left(12))
            .wrapping_add(v4.rotate_left(18));
        hash = merge_round64(hash, v1);
        hash = merge_round64(hash, v2);
        hash = merge_round64(hash, v3);
        hash = merge_round64(hash, v4);
    } else {
        hash = PRIME64_5;
    }
    hash = hash.wrapping_add(len as u64);
    while index + 8 <= len {
        hash ^= round64(0, read_u64(&input[index..index + 8]));
        hash = hash
            .rotate_left(27)
            .wrapping_mul(PRIME64_1)
            .wrapping_add(PRIME64_4);
        index += 8;
    }
    if index + 4 <= len {
        hash ^= u64::from(read_u32(&input[index..index + 4])).wrapping_mul(PRIME64_1);
        hash = hash
            .rotate_left(23)
            .wrapping_mul(PRIME64_2)
            .wrapping_add(PRIME64_3);
        index += 4;
    }
    while index < len {
        hash ^= u64::from(input[index]).wrapping_mul(PRIME64_5);
        hash = hash.rotate_left(11).wrapping_mul(PRIME64_1);
        index += 1;
    }
    hash ^= hash >> 33;
    hash = hash.wrapping_mul(PRIME64_2);
    hash ^= hash >> 29;
    hash = hash.wrapping_mul(PRIME64_3);
    hash ^= hash >> 32;
    hash
}

fn round64(accumulator: u64, input: u64) -> u64 {
    accumulator
        .wrapping_add(input.wrapping_mul(PRIME64_2))
        .rotate_left(31)
        .wrapping_mul(PRIME64_1)
}

fn merge_round64(accumulator: u64, value: u64) -> u64 {
    (accumulator ^ round64(0, value))
        .wrapping_mul(PRIME64_1)
        .wrapping_add(PRIME64_4)
}

fn read_u64(bytes: &[u8]) -> u64 {
    u64::from_le_bytes(bytes.try_into().expect("8-byte slice"))
}

fn read_u32(bytes: &[u8]) -> u32 {
    u32::from_le_bytes(bytes.try_into().expect("4-byte slice"))
}

/// Production-default constructor: a real gRPC client when endpoints are
/// configured, otherwise `None` (callers keep [`NotPortedSearcher`]).
pub fn grpc_client_from_endpoints(endpoints: Vec<String>) -> Option<GrpcSearcherClient> {
    if endpoints.is_empty() {
        None
    } else {
        Some(GrpcSearcherClient::new(endpoints))
    }
}

/// Boxed production default: gRPC transport with endpoints, `NotPortedSearcher`
/// without them.
pub fn default_searcher_client(endpoints: Vec<String>) -> Box<dyn SearcherClient + Send + Sync> {
    match grpc_client_from_endpoints(endpoints) {
        Some(client) => Box::new(client),
        None => Box::new(NotPortedSearcher),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn not_ported_reports_the_explicit_error() {
        let searcher = NotPortedSearcher;
        assert_eq!(searcher.endpoints(), Err(SearcherError::not_ported()));
        assert_eq!(
            searcher.resolve_revision("repo", "HEAD"),
            Err(SearcherError::not_ported())
        );
        assert_eq!(
            searcher.default_branch("repo"),
            Err(SearcherError::not_ported())
        );
        let request = protocol::Request {
            repo: "repo".to_owned(),
            repo_id: 7,
            commit: "abc".to_owned(),
            branch: "HEAD".to_owned(),
            pattern_info: protocol::PatternInfo::default(),
            fetch_timeout: std::time::Duration::from_secs(1),
            indexed: false,
            num_context_lines: 0,
        };
        assert_eq!(
            SearcherClient::search(&searcher, &request, &mut |_| {}),
            Err(SearcherError::not_ported())
        );
        assert_eq!(searcher.acquire(), Err(SearcherError::not_ported()));
        assert_eq!(
            SymbolsClient::search(&searcher, &SymbolsParameters::default()),
            Err(SearcherError::not_ported())
        );
        assert_eq!(SearcherError::not_ported().to_string(), NOT_PORTED_MESSAGE);
    }

    #[test]
    fn xxhash64_matches_go_vectors() {
        assert_eq!(xxhash64(b""), 0xEF46_DB37_51D8_E999);
        assert_eq!(xxhash64(b"a"), 0xD24E_C4F1_A98C_6E5B);
        assert_eq!(xxhash64(b"abc"), 0x44BC_2CF5_AD77_0999);
        assert_eq!(xxhash64(b"hello"), 0x26C7_827D_889F_6DA3);
    }

    #[test]
    fn rendezvous_order_is_a_permutation_and_depends_on_the_key() {
        let nodes = vec![
            "http://127.0.0.1:39001".to_owned(),
            "http://127.0.0.1:39002".to_owned(),
            "http://127.0.0.1:39003".to_owned(),
        ];
        let first = rendezvous_order(&nodes, "repo@commit");
        let second = rendezvous_order(&nodes, "repo@commit");
        assert_eq!(first, second);
        assert_eq!(first.len(), nodes.len());

        let mut sorted = first.clone();
        sorted.sort();
        let mut expected = nodes.clone();
        expected.sort();
        assert_eq!(sorted, expected);

        let other = rendezvous_order(&nodes, "repo@other");
        assert_eq!(other.len(), nodes.len());
        let mut other_sorted = other.clone();
        other_sorted.sort();
        assert_eq!(other_sorted, expected);
        assert!(rendezvous_order(&[], "repo@commit").is_empty());
    }

    #[test]
    fn parses_go_durations() {
        assert_eq!(parse_go_duration("50ms"), Some(Duration::from_millis(50)));
        assert_eq!(parse_go_duration("3s"), Some(Duration::from_secs(3)));
        assert_eq!(parse_go_duration("1m"), Some(Duration::from_secs(60)));
        assert_eq!(parse_go_duration("nope"), None);
    }

    #[test]
    fn classifies_grpc_statuses() {
        let cases = [
            (
                tonic::Code::NotFound,
                SearcherErrorKind::NotFound,
                "missing",
            ),
            (
                tonic::Code::DeadlineExceeded,
                SearcherErrorKind::Timeout,
                "context deadline exceeded",
            ),
            (
                tonic::Code::Unavailable,
                SearcherErrorKind::Temporary,
                "unavailable",
            ),
            (
                tonic::Code::ResourceExhausted,
                SearcherErrorKind::Temporary,
                "exhausted",
            ),
            (tonic::Code::Internal, SearcherErrorKind::Other, "boom"),
            (
                tonic::Code::Cancelled,
                SearcherErrorKind::Other,
                "context canceled",
            ),
        ];
        for (code, kind, message) in cases {
            let status = tonic::Status::new(code, message);
            let error = classify_status(&status);
            assert_eq!(error.kind, kind, "{code:?}");
            assert_eq!(error.message, message, "{code:?}");
        }
    }

    #[test]
    fn default_constructor_falls_back_without_endpoints() {
        assert!(grpc_client_from_endpoints(Vec::new()).is_none());
        let client = default_searcher_client(Vec::new());
        assert_eq!(client.endpoints(), Err(SearcherError::not_ported()));

        let client = grpc_client_from_endpoints(vec!["http://127.0.0.1:1".to_owned()])
            .expect("configured client");
        assert!(client.is_configured());
        assert_eq!(
            client.endpoints().unwrap(),
            vec!["http://127.0.0.1:1".to_owned()]
        );
    }

    #[test]
    fn empty_client_reports_no_endpoints() {
        let client = GrpcSearcherClient::new(Vec::new());
        assert!(!client.is_configured());
        assert_eq!(
            client.endpoints(),
            Err(SearcherError::other(NO_ENDPOINTS_MESSAGE))
        );
        let request = protocol::Request {
            repo: "repo".to_owned(),
            repo_id: 1,
            commit: "c".to_owned(),
            branch: String::new(),
            pattern_info: protocol::PatternInfo::default(),
            fetch_timeout: Duration::ZERO,
            indexed: false,
            num_context_lines: 0,
        };
        assert_eq!(
            SearcherClient::search(&client, &request, &mut |_| {}),
            Err(SearcherError::other(NO_ENDPOINTS_MESSAGE))
        );
    }
}
