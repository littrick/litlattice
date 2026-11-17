//! `symbols.Client` (`internal/symbols/client.go`): the gRPC symbols backend
//! behind [`crate::search_searcher_client::SymbolsClient`].
//!
//! The wire types already exist in [`sg_proto::symbols`] (the baseline
//! `internal/symbols/v1` proto), so this module only adds the transport:
//!
//! * endpoint selection with the same rendezvous order as the searcher client
//!   (`endpoint.Map.Get(repo)`; `internal/endpoint/endpoint.go`);
//! * the baseline gRPC retry policy (`Unavailable`/`ResourceExhausted`,
//!   bounded attempts, exponential backoff; `internal/symbols/retry.go`);
//! * `translateGRPCError` (`Canceled`/`DeadlineExceeded` become the std
//!   context errors) and the `executing symbols search request` wrap;
//! * `response.Err` propagation and `LimitHit`;
//! * the sub-repo permissions seam (`ActorPermissions` in the baseline),
//!   injected as [`SubRepoPermsChecker`] and disabled by default;
//! * `LocalCodeIntel` (`internal/symbols/client.go:124-176`, server-streaming;
//!   `Unimplemented` becomes `Ok(None)`) and `SymbolInfo` (`:178-229`, single
//!   RPC with `authz.FilterActorPath` filtering), which back
//!   `GitBlob.localCodeIntel` / `GitBlob.symbolInfo` (M9.51a).
//!
//! Since M9.18b `search_runtime::assemble` always constructs
//! [`GrpcSymbolsClient`]; with no endpoints the `search` call fails with Go's
//! `getting symbols server address for repo ...: endpoint.Map(conf-based) is
//! empty` wrap instead of an assembly-time not-ported error. See
//! `docs/overviews/m9_8iq_symbols_client_overview.md`.

use std::sync::Arc;
use std::time::{Duration, Instant};

use prost_types::Duration as ProtoDuration;
use sg_proto::symbols as pb;
use sg_zoekt::cached::Clock;
use tonic::transport::Channel;

use crate::search_env::{GrpcChannelSource, GrpcConnectionCache, SearcherGrpcConnectionCache};
use crate::search_searcher_client::{
    classify_status, is_retriable, rendezvous_order, run_to_completion, RetryConfig,
    RetryStreamAttempt, SearcherError, SearcherErrorKind, SymbolsClient, SymbolsParameters,
    SymbolsResponse,
};
use crate::search_stream::convert;

/// The baseline empty-map error (`endpoint.EmptyError` with the
/// `endpoint.ConfBased` URL spec).
pub const NO_SYMBOLS_ENDPOINTS_MESSAGE: &str = "endpoint.Map(conf-based) is empty";

/// Go `%q` for a string (the subset needed for repo names): JSON quote syntax
/// is identical for printable ASCII.
fn go_quote(value: &str) -> String {
    serde_json::to_string(value).unwrap_or_else(|_| format!("{value:?}"))
}

// ---------------------------------------------------------------------------
// `internal/types/codeintel.go` codenav payload types (M9.51a)
// ---------------------------------------------------------------------------

/// `types.RepoCommitPath` (`internal/types/codeintel.go:132-140`).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RepoCommitPath {
    pub repo: String,
    pub commit: String,
    pub path: String,
}

/// `types.RepoCommitPathPoint` (`internal/types/codeintel.go:156-164`).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RepoCommitPathPoint {
    pub repo: String,
    pub commit: String,
    pub path: String,
    pub row: i32,
    pub column: i32,
}

/// `types.Range` (`internal/types/codeintel.go:177-181`): `row`/`column`/
/// `length`, not a start/end pair.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CodeIntelRange {
    pub row: i32,
    pub column: i32,
    pub length: i32,
}

/// `types.Symbol` (`internal/types/codeintel.go:166-171`).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LocalCodeIntelSymbol {
    pub name: String,
    pub hover: String,
    /// Go `Def Range` is a value field: `omitempty` never omits a struct, so
    /// `def` is always present in the JSON payload.
    pub def: CodeIntelRange,
    pub refs: Vec<CodeIntelRange>,
}

/// `types.LocalCodeIntelPayload` (`internal/types/codeintel.go:142-144`).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LocalCodeIntelPayload {
    pub symbols: Vec<LocalCodeIntelSymbol>,
}

/// `types.RepoCommitPathMaybeRange` (`internal/types/codeintel.go:151-154`):
/// the embedded `*Range` is `None` when the definition point is a directory.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RepoCommitPathMaybeRange {
    pub repo: String,
    pub commit: String,
    pub path: String,
    pub range: Option<CodeIntelRange>,
}

/// `types.SymbolInfo` (`internal/types/codeintel.go:187-190`).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SymbolInfoResult {
    pub definition: RepoCommitPathMaybeRange,
    pub hover: Option<String>,
}

impl LocalCodeIntelSymbol {
    /// `json.Marshal` of one `types.Symbol`: field order `name`, `hover`,
    /// `def`, `refs`; `hover` omitted when empty, `refs` omitted when empty,
    /// `def` always present.
    fn to_json(&self) -> sg_graphql::json::Json {
        use sg_graphql::json::Json;
        let mut entries = vec![
            ("name", Json::Str(self.name.clone())),
        ];
        if !self.hover.is_empty() {
            entries.push(("hover", Json::Str(self.hover.clone())));
        }
        entries.push(("def", range_json(self.def)));
        if !self.refs.is_empty() {
            entries.push((
                "refs",
                Json::Arr(self.refs.iter().copied().map(range_json).collect()),
            ));
        }
        Json::obj(entries)
    }
}

fn range_json(range: CodeIntelRange) -> sg_graphql::json::Json {
    use sg_graphql::json::Json;
    Json::obj(vec![
        ("row", Json::Int(i64::from(range.row))),
        ("column", Json::Int(i64::from(range.column))),
        ("length", Json::Int(i64::from(range.length))),
    ])
}

impl LocalCodeIntelPayload {
    /// `json.Marshal` of `*types.LocalCodeIntelPayload`.
    ///
    /// The baseline client builds `out.Symbols` with `append`, so a payload
    /// with no symbols marshals `symbols` as `null` (not `[]`); an empty
    /// stream still returns the non-nil `&out`, i.e. `{"symbols":null}`.
    pub fn to_go_json(&self) -> String {
        use sg_graphql::json::Json;
        let symbols = if self.symbols.is_empty() {
            Json::Null
        } else {
            Json::Arr(self.symbols.iter().map(LocalCodeIntelSymbol::to_json).collect())
        };
        Json::obj(vec![("symbols", symbols)]).to_compact_string()
    }
}

/// Sub-repo permission seam standing in for
/// `authz.SubRepoPermissionChecker`/`authz.ActorPermissions`.
///
/// The baseline enables the check only when the checker is non-nil and
/// `authz.SubRepoEnabled` reports it; [`NoSubRepoPerms`] models the disabled
/// production default (the port has no authz middleware yet). Differential
/// harnesses inject a fake that records and filters by path.
pub trait SubRepoPermsChecker: Send + Sync {
    /// `authz.SubRepoEnabled(checker)`; `false` disables filtering entirely.
    fn enabled(&self) -> bool;

    /// `authz.ActorPermissions(...).Include(authz.Read)` for one
    /// `(repo, path)` pair. The error is the checker failure text.
    fn include(&self, repo: &str, path: &str) -> Result<bool, String>;
}

/// Production default: sub-repo permissions are not enforced.
#[derive(Debug, Default, Clone, Copy)]
pub struct NoSubRepoPerms;

impl SubRepoPermsChecker for NoSubRepoPerms {
    fn enabled(&self) -> bool {
        false
    }

    fn include(&self, _repo: &str, _path: &str) -> Result<bool, String> {
        Ok(true)
    }
}

/// Connection seam for [`GrpcSymbolsClient`], mirroring
/// [`crate::search_searcher_client::SearcherConnector`].
pub trait SymbolsConnector: Send + Sync {
    fn connect(
        &self,
        endpoint: &str,
    ) -> Result<pb::symbols_service_client::SymbolsServiceClient<Channel>, SearcherError>;
}

/// Default connector: `Channel::from_shared(...).connect_lazy()` (cleartext
/// HTTP/2, the same dial configuration as the searcher client).
#[derive(Debug, Default, Clone, Copy)]
pub struct ChannelConnector;

impl SymbolsConnector for ChannelConnector {
    fn connect(
        &self,
        endpoint: &str,
    ) -> Result<pb::symbols_service_client::SymbolsServiceClient<Channel>, SearcherError> {
        let uri = if endpoint.contains("://") {
            endpoint.to_owned()
        } else {
            format!("http://{endpoint}")
        };
        let channel = Channel::from_shared(uri)
            .map_err(|error| SearcherError::other(format!("failed to parse URL: {error}")))?
            .connect_lazy();
        Ok(pb::symbols_service_client::SymbolsServiceClient::new(
            channel,
        ))
    }
}

/// Production symbols connector: fetch the lazy channel from the symbols
/// client's own [`GrpcConnectionCache`] instance.
///
/// Unlike the searcher (keyed by `url.Host`), the baseline symbols client keys
/// the cache by the full `Endpoints.Get(repo)` address string, so this passes
/// `endpoint` through unchanged. The symbols cache is a separate instance from
/// the searcher's (`symbolsConnectionCache` vs `searcherGRPCConnectionCache`).
pub struct CachedSymbolsConnector {
    source: Arc<dyn GrpcChannelSource>,
}

impl CachedSymbolsConnector {
    pub fn new(source: Arc<dyn GrpcChannelSource>) -> Self {
        Self { source }
    }
}

impl SymbolsConnector for CachedSymbolsConnector {
    fn connect(
        &self,
        endpoint: &str,
    ) -> Result<pb::symbols_service_client::SymbolsServiceClient<Channel>, SearcherError> {
        let channel = self.source.channel(endpoint)?;
        Ok(pb::symbols_service_client::SymbolsServiceClient::new(
            channel,
        ))
    }
}

/// Real gRPC transport for [`SymbolsClient`] (`internal/symbols/client.go`).
pub struct GrpcSymbolsClient {
    endpoints: Vec<String>,
    connector: Arc<dyn SymbolsConnector>,
    retry: RetryConfig,
    sub_repo_perms: Arc<dyn SubRepoPermsChecker>,
}

impl std::fmt::Debug for GrpcSymbolsClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GrpcSymbolsClient")
            .field("endpoints", &self.endpoints)
            .field("retry", &self.retry)
            .finish_non_exhaustive()
    }
}

impl GrpcSymbolsClient {
    pub fn new(endpoints: Vec<String>) -> Self {
        // `symbols.LoadConfig` creates its own `symbolsConnectionCache`
        // instance; it is intentionally not shared with the searcher cache.
        let cache = Arc::new(SearcherGrpcConnectionCache::new());
        Self {
            endpoints,
            connector: Arc::new(CachedSymbolsConnector::new(cache)),
            retry: RetryConfig::from_env(),
            sub_repo_perms: Arc::new(NoSubRepoPerms),
        }
    }

    /// Inject the symbols connection cache (a distinct instance from the
    /// searcher's; useful for tests and for wiring an externally owned cache).
    pub fn with_connection_cache<C: Clock>(mut self, cache: Arc<GrpcConnectionCache<C>>) -> Self {
        self.connector = Arc::new(CachedSymbolsConnector::new(cache));
        self
    }

    /// Inject a test connector (in-process fake gRPC servers).
    pub fn with_connector(mut self, connector: Arc<dyn SymbolsConnector>) -> Self {
        self.connector = connector;
        self
    }

    pub fn with_retry_config(mut self, retry: RetryConfig) -> Self {
        self.retry = retry;
        self
    }

    pub fn with_sub_repo_perms(mut self, checker: Arc<dyn SubRepoPermsChecker>) -> Self {
        self.sub_repo_perms = checker;
        self
    }

    pub fn is_configured(&self) -> bool {
        !self.endpoints.is_empty()
    }

    pub fn configured_endpoints(&self) -> &[String] {
        &self.endpoints
    }

    /// `endpoint.Map.Get(repo)`: the first endpoint in rendezvous order.
    pub fn route(&self, repo: &str) -> Option<String> {
        rendezvous_order(&self.endpoints, repo).into_iter().next()
    }

    /// `Client.LocalCodeIntel` (`internal/symbols/client.go:124-176`): a
    /// server-streaming RPC whose chunks are concatenated. `Unimplemented`
    /// (on the initial call or mid-stream) becomes `Ok(None)`, matching the
    /// baseline's deliberate error swallow.
    ///
    /// `defaults.RetryPolicy` retries `Unavailable`/`ResourceExhausted` only
    /// until the first message arrives (`internal/grpc/defaults/retry.go:31-34`);
    /// mid-stream failures are returned to the caller unchanged. The connection
    /// step itself is outside the retry loop, as in `Client.gRPCClient`.
    pub fn local_code_intel(
        &self,
        path: &RepoCommitPath,
    ) -> Result<Option<LocalCodeIntelPayload>, SearcherError> {
        let connector = Arc::clone(&self.connector);
        let endpoint = self.route(&path.repo);
        let path = path.clone();
        let retry = self.retry;
        run_to_completion(async move {
            let Some(endpoint) = endpoint else {
                return Err(SearcherError::other(format!(
                    "getting gRPC symbols client: getting symbols server address for repo {}: \
                     {NO_SYMBOLS_ENDPOINTS_MESSAGE}",
                    go_quote(&path.repo)
                )));
            };
            let client = connect_symbol_client(connector, &endpoint)?;
            let request = pb::LocalCodeIntelRequest {
                repo_commit_path: Some(pb::RepoCommitPath {
                    repo: path.repo,
                    commit: path.commit,
                    path: path.path,
                }),
            };
            retry
                .run_stream(|| {
                    let mut client = client.clone();
                    let request = request.clone();
                    async move {
                        let mut stream = match client.local_code_intel(request).await {
                            Ok(response) => response.into_inner(),
                            Err(status) if status.code() == tonic::Code::Unimplemented => {
                                return RetryStreamAttempt::Unimplemented;
                            }
                            Err(status) if is_retriable(status.code()) => {
                                // No message has been received yet.
                                return RetryStreamAttempt::Retry(translate_status(&status));
                            }
                            Err(status) => {
                                return RetryStreamAttempt::Done(Err(translate_status(&status)));
                            }
                        };
                        let mut symbols = Vec::new();
                        let mut received_first = false;
                        loop {
                            match stream.message().await {
                                Ok(Some(response)) => {
                                    received_first = true;
                                    symbols.extend(
                                        response
                                            .symbols
                                            .into_iter()
                                            .map(convert_local_code_intel_symbol),
                                    );
                                }
                                Ok(None) => {
                                    return RetryStreamAttempt::Done(Ok(LocalCodeIntelPayload {
                                        symbols,
                                    }));
                                }
                                Err(status) if status.code() == tonic::Code::Unimplemented => {
                                    // The baseline discards any partially
                                    // received chunks.
                                    return RetryStreamAttempt::Unimplemented;
                                }
                                Err(status) if !received_first && is_retriable(status.code()) => {
                                    // Only the first-message window is retried.
                                    return RetryStreamAttempt::Retry(translate_status(&status));
                                }
                                Err(status) => {
                                    return RetryStreamAttempt::Done(Err(translate_status(
                                        &status,
                                    )));
                                }
                            }
                        }
                    }
                })
                .await
        })
    }

    /// `Client.SymbolInfo` (`internal/symbols/client.go:178-229`): one RPC,
    /// `Unimplemented` -> `Ok(None)`, then `authz.FilterActorPath` filtering
    /// (which the baseline applies even when the RPC returned no result).
    pub fn symbol_info(
        &self,
        point: &RepoCommitPathPoint,
    ) -> Result<Option<SymbolInfoResult>, SearcherError> {
        let result = self
            .symbol_info_grpc(point)
            .map_err(|error| {
                SearcherError::new(
                    error.kind,
                    format!("executing symbol info request: {}", error.message),
                )
            })?;

        // `authz.FilterActorPath`: disabled checker allows everything.
        if self.sub_repo_perms.enabled() {
            match self.sub_repo_perms.include(&point.repo, &point.path) {
                Ok(true) => {}
                Ok(false) => return Ok(None),
                Err(error) => {
                    return Err(SearcherError::other(format!(
                        "checking sub-repo permissions: {error}"
                    )))
                }
            }
        }

        Ok(result)
    }

    fn symbol_info_grpc(
        &self,
        point: &RepoCommitPathPoint,
    ) -> Result<Option<SymbolInfoResult>, SearcherError> {
        let connector = Arc::clone(&self.connector);
        let endpoint = self.route(&point.repo);
        let point = point.clone();
        let retry = self.retry;
        run_to_completion(async move {
            let Some(endpoint) = endpoint else {
                return Err(SearcherError::other(format!(
                    "getting gRPC symbols client: getting symbols server address for repo {}: \
                     {NO_SYMBOLS_ENDPOINTS_MESSAGE}",
                    go_quote(&point.repo)
                )));
            };
            let client = connect_symbol_client(connector, &endpoint)?;
            let request = pb::SymbolInfoRequest {
                repo_commit_path: Some(pb::RepoCommitPath {
                    repo: point.repo,
                    commit: point.commit,
                    path: point.path,
                }),
                point: Some(pb::Point {
                    row: point.row,
                    column: point.column,
                }),
            };
            // `automaticRetryClient.SymbolInfo`: one unary RPC with
            // `defaults.RetryPolicy`. `Unimplemented` is not a retriable code
            // and the baseline client turns it into `(nil, nil)`.
            let response = retry
                .run_unary(None, None, || {
                    let mut client = client.clone();
                    let request = request.clone();
                    async move {
                        match client.symbol_info(request).await {
                            Ok(response) => Ok(Some(response.into_inner())),
                            Err(status) if status.code() == tonic::Code::Unimplemented => Ok(None),
                            Err(status) => Err(translate_status(&status)),
                        }
                    }
                })
                .await?;
            Ok(response.and_then(convert_symbol_info))
        })
    }

    async fn search_endpoint(
        connector: Arc<dyn SymbolsConnector>,
        endpoint: String,
        request: &pb::SearchRequest,
        deadline: Option<Instant>,
    ) -> Result<pb::SearchResponse, SearcherError> {
        let mut client = connector
            .connect(&endpoint)
            .map_err(|error| SearcherError::new(error.kind, with_grpc_context(error.message)))?;
        let call = client.search(request.clone());
        let response = match deadline {
            Some(deadline) => {
                let deadline = tokio::time::Instant::from_std(deadline);
                match tokio::time::timeout_at(deadline, call).await {
                    Ok(result) => result,
                    Err(_) => {
                        return Err(with_rpc_context(SearcherError::new(
                            SearcherErrorKind::Timeout,
                            "context deadline exceeded",
                        )))
                    }
                }
            }
            None => call.await,
        };
        match response {
            Ok(response) => Ok(response.into_inner()),
            Err(status) => Err(with_rpc_context(translate_status(&status))),
        }
    }
}

impl SymbolsClient for GrpcSymbolsClient {
    fn search(&self, params: &SymbolsParameters) -> Result<SymbolsResponse, SearcherError> {
        let Some(endpoint) = self.route(&params.repo) else {
            // `symbols.Client.Search` -> `searchGRPC` -> `gRPCClient` ->
            // `Endpoints.Get`, each wrapping the previous error
            // (`internal/symbols/client.go:58-104,120-130,229-233`) around
            // `endpoint.EmptyError{URLSpec: "conf-based"}`.
            return Err(SearcherError::other(format!(
                "executing symbols search request: getting gRPC symbols client: \
                 getting symbols server address for repo {}: {NO_SYMBOLS_ENDPOINTS_MESSAGE}",
                go_quote(&params.repo)
            )));
        };
        let request = to_proto_request(params);
        let connector = Arc::clone(&self.connector);
        let retry = self.retry;
        let deadline = params.deadline;

        // `automaticRetryClient.Search`: the shared unary policy
        // (`Unavailable`/`ResourceExhausted`, bounded attempts, exponential
        // backoff). `search_endpoint` keeps the Go result/error text.
        let response = run_to_completion(async move {
            retry
                .run_unary(
                    deadline,
                    Some("executing symbols search request: "),
                    || {
                        Self::search_endpoint(
                            Arc::clone(&connector),
                            endpoint.clone(),
                            &request,
                            deadline,
                        )
                    },
                )
                .await
        })?;

        if let Some(error) = response.error.as_deref() {
            if !error.is_empty() {
                return Err(SearcherError::other(error));
            }
        }

        // `result.Symbol.FileLimited` travels alongside the reduced
        // `convert::Symbol` so the GraphQL symbols resolvers can expose
        // `fileLocal` without widening the shared search-job result type.
        let mut pairs: Vec<(convert::Symbol, bool)> = response
            .symbols
            .into_iter()
            .map(|symbol| {
                (
                    convert::Symbol {
                        name: symbol.name,
                        path: symbol.path,
                        line: symbol.line,
                        character: symbol.character,
                        kind: symbol.kind,
                        language: symbol.language,
                        parent: symbol.parent,
                        parent_kind: symbol.parent_kind,
                    },
                    symbol.file_limited,
                )
            })
            .collect();

        if self.sub_repo_perms.enabled() {
            let mut filtered = Vec::with_capacity(pairs.len());
            for (symbol, file_limited) in pairs.drain(..) {
                match self.sub_repo_perms.include(&params.repo, &symbol.path) {
                    Ok(true) => filtered.push((symbol, file_limited)),
                    Ok(false) => {}
                    Err(error) => {
                        return Err(SearcherError::other(format!(
                            "checking sub-repo permissions: {error}"
                        )))
                    }
                }
            }
            pairs = filtered;
        }

        let (symbols, file_limited) = pairs.into_iter().unzip();
        Ok(SymbolsResponse {
            symbols,
            limit_hit: response.limit_hit,
            file_limited,
        })
    }
}

/// `proto.SearchRequest.FromInternal` (`internal/symbols/v1/conversion.go`).
fn to_proto_request(params: &SymbolsParameters) -> pb::SearchRequest {
    pb::SearchRequest {
        repo: params.repo.clone(),
        commit_id: params.commit_id.clone(),
        query: params.query.clone(),
        is_reg_exp: params.is_regexp,
        is_case_sensitive: params.is_case_sensitive,
        include_patterns: params.include_patterns.clone(),
        exclude_pattern: params.exclude_pattern.clone(),
        first: params.first,
        timeout: Some(duration_to_proto(params.timeout)),
        include_langs: params.include_langs.clone(),
        exclude_langs: params.exclude_langs.clone(),
    }
}

fn duration_to_proto(duration: Duration) -> ProtoDuration {
    ProtoDuration {
        seconds: duration.as_secs() as i64,
        nanos: duration.subsec_nanos() as i32,
    }
}

/// `gRPCClient`'s connection step (`internal/symbols/client.go:231-243`):
/// `getting gRPC symbols client: getting gRPC connection to symbols server at
/// "<address>": ...`.
fn connect_symbol_client(
    connector: Arc<dyn SymbolsConnector>,
    endpoint: &str,
) -> Result<pb::symbols_service_client::SymbolsServiceClient<Channel>, SearcherError> {
    connector.connect(endpoint).map_err(|error| {
        SearcherError::new(
            error.kind,
            format!(
                "getting gRPC symbols client: getting gRPC connection to symbols server at {}: {}",
                go_quote(endpoint),
                error.message
            ),
        )
    })
}

/// `proto.Range.ToInternal` (`internal/symbols/v1/conversion.go`).
fn convert_range(range: pb::Range) -> CodeIntelRange {
    CodeIntelRange {
        row: range.row,
        column: range.column,
        length: range.length,
    }
}

/// `LocalCodeIntelResponse_Symbol.ToInternal`. A missing proto `def` message
/// becomes the zero `types.Range` (Go `GetDef().ToInternal()` on nil).
fn convert_local_code_intel_symbol(
    symbol: pb::local_code_intel_response::Symbol,
) -> LocalCodeIntelSymbol {
    LocalCodeIntelSymbol {
        name: symbol.name,
        hover: symbol.hover,
        def: symbol.def.map(convert_range).unwrap_or_default(),
        refs: symbol.refs.into_iter().map(convert_range).collect(),
    }
}

/// `SymbolInfoResponse.ToInternal`: a nil `result` means "no symbol" (GraphQL
/// `null`); otherwise the definition is always present (empty strings when
/// the proto definition is missing).
fn convert_symbol_info(response: pb::SymbolInfoResponse) -> Option<SymbolInfoResult> {
    let result = response.result?;
    let definition = result.definition.unwrap_or_default();
    let path = definition.repo_commit_path.unwrap_or_default();
    Some(SymbolInfoResult {
        definition: RepoCommitPathMaybeRange {
            repo: path.repo,
            commit: path.commit,
            path: path.path,
            range: definition.range.map(convert_range),
        },
        // `Hover` keeps the proto presence: `Some("")` renders an empty
        // string, `None` renders GraphQL null.
        hover: result.hover,
    })
}

/// `errors.Wrap(err, "executing symbols search request")`; connection failures
/// additionally carry `getting gRPC symbols client`.
fn with_grpc_context(message: String) -> String {
    format!("executing symbols search request: getting gRPC symbols client: {message}")
}

fn with_rpc_context(error: SearcherError) -> SearcherError {
    SearcherError::new(
        error.kind,
        format!("executing symbols search request: {}", error.message),
    )
}

/// `translateGRPCError`: `Canceled`/`DeadlineExceeded` become the std context
/// errors; other statuses keep the Go `status.Error` rendering.
fn translate_status(status: &tonic::Status) -> SearcherError {
    match status.code() {
        tonic::Code::Cancelled => SearcherError::other("context canceled"),
        tonic::Code::DeadlineExceeded => {
            SearcherError::new(SearcherErrorKind::Timeout, "context deadline exceeded")
        }
        code => {
            let kind = classify_status(status).kind;
            SearcherError::new(
                kind,
                format!(
                    "rpc error: code = {} desc = {}",
                    go_code_name(code),
                    status.message()
                ),
            )
        }
    }
}

/// The Go `codes.Code.String()` names (`Canceled` has one `l`).
fn go_code_name(code: tonic::Code) -> &'static str {
    match code {
        tonic::Code::Ok => "OK",
        tonic::Code::Cancelled => "Canceled",
        tonic::Code::Unknown => "Unknown",
        tonic::Code::InvalidArgument => "InvalidArgument",
        tonic::Code::DeadlineExceeded => "DeadlineExceeded",
        tonic::Code::NotFound => "NotFound",
        tonic::Code::AlreadyExists => "AlreadyExists",
        tonic::Code::PermissionDenied => "PermissionDenied",
        tonic::Code::ResourceExhausted => "ResourceExhausted",
        tonic::Code::FailedPrecondition => "FailedPrecondition",
        tonic::Code::Aborted => "Aborted",
        tonic::Code::OutOfRange => "OutOfRange",
        tonic::Code::Unimplemented => "Unimplemented",
        tonic::Code::Internal => "Internal",
        tonic::Code::Unavailable => "Unavailable",
        tonic::Code::DataLoss => "DataLoss",
        tonic::Code::Unauthenticated => "Unauthenticated",
    }
}

/// Constructor kept for callers that model "is there a configured client?".
pub fn grpc_symbols_client_from_endpoints(endpoints: Vec<String>) -> Option<GrpcSymbolsClient> {
    if endpoints.is_empty() {
        None
    } else {
        Some(GrpcSymbolsClient::new(endpoints))
    }
}

/// Boxed production default: gRPC transport with endpoints, `None` without
/// them. Callers that must mirror the baseline's always-present
/// `symbols.DefaultClient` construct [`GrpcSymbolsClient::new`] directly (see
/// `crate::search_runtime`); an empty map then reports the baseline error at
/// call time.
pub fn default_symbols_client(
    endpoints: Vec<String>,
) -> Option<Box<dyn SymbolsClient + Send + Sync>> {
    grpc_symbols_client_from_endpoints(endpoints)
        .map(|client| Box::new(client) as Box<dyn SymbolsClient + Send + Sync>)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_fall_back_without_endpoints() {
        assert!(grpc_symbols_client_from_endpoints(Vec::new()).is_none());
        assert!(default_symbols_client(Vec::new()).is_none());
        let client = grpc_symbols_client_from_endpoints(vec!["http://127.0.0.1:1".to_owned()])
            .expect("configured");
        assert!(client.is_configured());
        assert_eq!(client.route("repo"), Some("http://127.0.0.1:1".to_owned()));
    }

    #[test]
    fn empty_client_reports_the_baseline_empty_map_error() {
        let client = GrpcSymbolsClient::new(Vec::new());
        let mut params = SymbolsParameters::default();
        params.repo = "github.com/o/r".to_owned();
        assert_eq!(
            client.search(&params),
            Err(SearcherError::other(
                "executing symbols search request: getting gRPC symbols client: \
                 getting symbols server address for repo \"github.com/o/r\": \
                 endpoint.Map(conf-based) is empty"
            ))
        );
    }

    #[test]
    fn request_conversion_includes_timeout_and_filters() {
        let params = SymbolsParameters {
            repo: "r".to_owned(),
            commit_id: "c".to_owned(),
            query: "q".to_owned(),
            is_regexp: true,
            is_case_sensitive: true,
            include_patterns: vec!["a".to_owned()],
            exclude_pattern: "b".to_owned(),
            include_langs: vec!["Go".to_owned()],
            exclude_langs: vec!["C".to_owned()],
            first: 101,
            timeout: Duration::from_secs(5),
            deadline: None,
        };
        let request = to_proto_request(&params);
        assert_eq!(request.repo, "r");
        assert_eq!(request.first, 101);
        assert_eq!(
            request.timeout,
            Some(ProtoDuration {
                seconds: 5,
                nanos: 0
            })
        );
        assert_eq!(request.include_patterns, vec!["a"]);
        assert_eq!(request.include_langs, vec!["Go"]);
    }

    #[test]
    fn grpc_error_translation_matches_the_baseline() {
        let canceled = translate_status(&tonic::Status::cancelled("x"));
        assert_eq!(canceled, SearcherError::other("context canceled"));
        let deadline = translate_status(&tonic::Status::deadline_exceeded("x"));
        assert_eq!(
            deadline,
            SearcherError::new(SearcherErrorKind::Timeout, "context deadline exceeded")
        );
        let unavailable = translate_status(&tonic::Status::unavailable("down"));
        assert_eq!(unavailable.kind, SearcherErrorKind::Temporary);
        assert_eq!(
            unavailable.message,
            "rpc error: code = Unavailable desc = down"
        );
        let wrapped = with_rpc_context(canceled);
        assert_eq!(
            wrapped.message,
            "executing symbols search request: context canceled"
        );
    }

    #[test]
    fn sub_repo_filter_reports_checker_errors() {
        struct Deny;
        impl SubRepoPermsChecker for Deny {
            fn enabled(&self) -> bool {
                true
            }
            fn include(&self, _repo: &str, path: &str) -> Result<bool, String> {
                if path == "secret" {
                    Err("boom".to_owned())
                } else {
                    Ok(path != "hidden")
                }
            }
        }
        let checker: Arc<dyn SubRepoPermsChecker> = Arc::new(Deny);
        assert!(checker.enabled());
        assert_eq!(checker.include("r", "hidden"), Ok(false));
        assert_eq!(checker.include("r", "secret"), Err("boom".to_owned()));
        assert!(!NoSubRepoPerms.enabled());
    }

    #[test]
    fn local_code_intel_payload_marshals_like_go() {
        // `append` into a nil slice, then `json.Marshal`: null, not [].
        assert_eq!(
            LocalCodeIntelPayload::default().to_go_json(),
            r#"{"symbols":null}"#
        );

        // Field order name/hover/def/refs, `omitempty` on hover/refs, and Go's
        // HTML escaping inside the strings.
        let payload = LocalCodeIntelPayload {
            symbols: vec![LocalCodeIntelSymbol {
                name: "f".to_owned(),
                hover: "a b".to_owned(),
                def: CodeIntelRange {
                    row: 1,
                    column: 2,
                    length: 3,
                },
                refs: vec![CodeIntelRange {
                    row: 4,
                    column: 5,
                    length: 6,
                }],
            }],
        };
        assert_eq!(
            payload.to_go_json(),
            concat!(
                r#"{"symbols":[{"name":"f","hover":"a b","#,
                r#""def":{"row":1,"column":2,"length":3},"#,
                r#""refs":[{"row":4,"column":5,"length":6}]}]}"#
            )
        );

        // `Def` is a value field: it is always present, even when zero.
        let no_hover = LocalCodeIntelPayload {
            symbols: vec![LocalCodeIntelSymbol {
                name: "g".to_owned(),
                ..Default::default()
            }],
        };
        assert_eq!(
            no_hover.to_go_json(),
            r#"{"symbols":[{"name":"g","def":{"row":0,"column":0,"length":0}}]}"#
        );
    }

    #[test]
    fn symbol_info_conversion_keeps_proto_presence() {
        assert_eq!(
            convert_symbol_info(pb::SymbolInfoResponse { result: None }),
            None
        );

        let info = convert_symbol_info(pb::SymbolInfoResponse {
            result: Some(pb::symbol_info_response::DefinitionResult {
                definition: Some(pb::symbol_info_response::Definition {
                    repo_commit_path: Some(pb::RepoCommitPath {
                        repo: "r".to_owned(),
                        commit: "c".to_owned(),
                        path: "p".to_owned(),
                    }),
                    range: None,
                }),
                hover: None,
            }),
        })
        .expect("result");
        assert_eq!(info.definition.repo, "r");
        assert_eq!(info.definition.commit, "c");
        assert_eq!(info.definition.path, "p");
        assert_eq!(info.definition.range, None);
        assert_eq!(info.hover, None);
    }
}
