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
//!   injected as [`SubRepoPermsChecker`] and disabled by default.
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
    classify_status, rendezvous_order, run_to_completion, RetryConfig, SearcherError,
    SearcherErrorKind, SymbolsClient, SymbolsParameters, SymbolsResponse,
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

        let response = run_to_completion(async move {
            let max_attempts = retry.max_attempts.max(1);
            let mut last_error = None;
            for attempt in 0..max_attempts {
                if attempt > 0 {
                    if deadline.is_some_and(|deadline| Instant::now() >= deadline) {
                        return Err(with_rpc_context(SearcherError::new(
                            SearcherErrorKind::Timeout,
                            "context deadline exceeded",
                        )));
                    }
                    tokio::time::sleep(retry.backoff(attempt)).await;
                }
                if deadline.is_some_and(|deadline| Instant::now() >= deadline) {
                    return Err(SearcherError::new(
                        SearcherErrorKind::Timeout,
                        "context deadline exceeded",
                    ));
                }
                match Self::search_endpoint(
                    Arc::clone(&connector),
                    endpoint.clone(),
                    &request,
                    deadline,
                )
                .await
                {
                    Ok(response) => return Ok(response),
                    Err(error) => {
                        // `defaults.RetryPolicy` only retries
                        // `Unavailable`/`ResourceExhausted`, which
                        // `classify_status` maps to `Temporary`.
                        if error.kind == SearcherErrorKind::Temporary && attempt + 1 < max_attempts
                        {
                            last_error = Some(error);
                            continue;
                        }
                        return Err(error);
                    }
                }
            }
            Err(last_error.unwrap_or_else(|| SearcherError::other(NO_SYMBOLS_ENDPOINTS_MESSAGE)))
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
}
