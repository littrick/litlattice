#![forbid(unsafe_code)]

//! Frontend search-backend environment wiring (`internal/search/env.go`).
//!
//! The baseline builds the process-wide searchers from the live configuration
//! and a handful of environment variables:
//!
//! * [`SearcherUrls`] — `SearcherURLs()`, a conf-based consistent-hash endpoint
//!   map over `ServiceConnections.Searchers`;
//! * [`SearcherGrpcConnectionCache`] — `SearcherGRPCConnectionCache()`, a
//!   TTL-cached set of lazy tonic channels keyed by endpoint (the baseline
//!   caches `grpc.ClientConn`), with an adapter for the pre-M9.8iw zoekt use;
//! * [`compose_indexed`] / [`IndexedJobStreamer`]: `Indexed()`,
//!   `cachedSearcher(ttl) → meteredSearcher("") → HorizontalSearcher` over the
//!   conf-based zoekt endpoints with a cached per-endpoint dialer
//!   (`getIndexedDialer`);
//! * [`list_all_indexed`] — `ListAllIndexed`;
//! * [`parse_drain_servers`], [`new_endpoint_map_drain`], [`indexers_from_source`]:
//!   `Indexers()`, including `INDEXED_SEARCH_DRAIN_SERVERS` and
//!   `reposAtEndpoint`.
//!
//! ## Rust-specific design
//!
//! [`sg_zoekt::streamer::Streamer`] is a chunk-level trait while
//! [`sg_zoekt::horizontal_searcher::HorizontalSearcher`] is result-level (it
//! needs full [`SearchResult`]s to deduplicate and flush). [`HorizontalStreamer`]
//! bridges the two and [`DialedClient`] adapts a cached per-endpoint
//! [`Streamer`] to the result-level [`HorizontalClient`] trait.
//!
//! Types are static (no `Arc<dyn ...>` here) so the wrappers keep their
//! concrete `Streamer` implementations; [`BlockingStreamer`] turns the final
//! async stack into the synchronous [`JobStreamer`] used by the job tree.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Mutex, MutexGuard, PoisonError};
use std::time::{Duration, Instant};

use sg_config::ServiceConnections;
use sg_zoekt::cached::{CachedStreamer, Clock, SystemClock};
use sg_zoekt::client::{Client as ZoektClient, Error as ZoektError};
use sg_zoekt::dialer::{CachedZoektDialer, CloseableStreamer, ZoektDialer};
use sg_zoekt::horizontal_searcher::{Client as HorizontalClient, HorizontalSearcher};
use sg_zoekt::indexers::{EndpointMap, Indexers, IndexersError, ReposMap};
use sg_zoekt::metered::{ErrorClassifier, MeteredStreamer};
use sg_zoekt::query::Query;
use sg_zoekt::streamer::{BlockingStreamer, Streamer};
use sg_zoekt::types::{
    ListOptions, ListResponse, MinimalRepoListEntry, RepoListField, SearchChunk, SearchOptions,
    SearchResult,
};
use tonic::transport::Channel;

use crate::search_job::{JobError, JobStreamer};
use crate::search_searcher_client::{rendezvous_order, run_to_completion, SearcherError};

/// `grpc.DefaultConnectionCache`'s TTL (10 minutes of inactivity).
pub const GRPC_CONNECTION_CACHE_TTL: Duration = Duration::from_secs(10 * 60);

/// `INDEXED_SEARCH_DRAIN_SERVERS`.
pub const INDEXED_SEARCH_DRAIN_SERVERS_VAR: &str = "INDEXED_SEARCH_DRAIN_SERVERS";

// ------------------------------------------------------------------ endpoint maps

/// `endpoint.ConfBased(getter)`: an endpoint map backed by a configuration
/// getter. The getter is evaluated on every call, so a caller that wraps a
/// live configuration sees endpoint changes without rebuilding the map.
pub struct ConfEndpointMap {
    source: Box<dyn Fn() -> Vec<String> + Send + Sync>,
}

impl ConfEndpointMap {
    pub fn new(source: impl Fn() -> Vec<String> + Send + Sync + 'static) -> Self {
        Self {
            source: Box::new(source),
        }
    }

    /// A map over a fixed endpoint list (useful for assembly-time snapshots).
    pub fn from_endpoints(endpoints: Vec<String>) -> Self {
        Self::new(move || endpoints.clone())
    }

    /// The raw configured endpoints (before any drain filtering).
    pub fn configured_endpoints(&self) -> Vec<String> {
        (self.source)()
    }
}

impl EndpointMap for ConfEndpointMap {
    fn endpoints(&self) -> Result<Vec<String>, IndexersError> {
        Ok((self.source)())
    }

    fn get(&self, key: &str) -> Result<String, IndexersError> {
        rendezvous_lookup(&(self.source)(), key)
    }
}

/// `endpoint.Map.Get`: the rendezvous winner, or the baseline's empty-map
/// error (`endpoint.EmptyError{URLSpec: "conf-based"}`).
fn rendezvous_lookup(nodes: &[String], key: &str) -> Result<String, IndexersError> {
    let Some(endpoint) = rendezvous_order(nodes, key).into_iter().next() else {
        return Err(IndexersError::new("endpoint.Map(conf-based) is empty"));
    };
    Ok(endpoint)
}

/// `endpointMapDrain`: endpoints in `drain` still appear in `Endpoints()` but
/// are never chosen by `Get`.
pub struct EndpointMapDrain {
    active: ConfEndpointMap,
    drain: Vec<String>,
}

impl EndpointMapDrain {
    pub fn new(
        source: impl Fn() -> Vec<String> + Send + Sync + 'static,
        drain: Vec<String>,
    ) -> Self {
        Self {
            active: ConfEndpointMap::new(source),
            drain,
        }
    }

    /// The conf endpoints minus the drain set, preserving configuration order
    /// (Go's `collections.Set.Difference().Values()` order is unspecified; any
    /// order is legal).
    fn active_endpoints(&self) -> Vec<String> {
        let drain: HashSet<&str> = self.drain.iter().map(String::as_str).collect();
        self.active
            .configured_endpoints()
            .into_iter()
            .filter(|endpoint| !drain.contains(endpoint.as_str()))
            .collect()
    }
}

impl EndpointMap for EndpointMapDrain {
    fn endpoints(&self) -> Result<Vec<String>, IndexersError> {
        let mut endpoints = self.active_endpoints();
        endpoints.extend(self.drain.iter().cloned());
        Ok(endpoints)
    }

    fn get(&self, key: &str) -> Result<String, IndexersError> {
        // `endpointMapDrain.Get` delegates to the *active* map.
        rendezvous_lookup(&self.active_endpoints(), key)
    }
}

/// `newEndpointMapDrain`; when nothing drains it returns the plain conf map.
pub fn new_endpoint_map_drain(
    source: impl Fn() -> Vec<String> + Send + Sync + 'static,
    drain: Vec<String>,
) -> Box<dyn EndpointMap> {
    if drain.is_empty() {
        Box::new(ConfEndpointMap::new(source))
    } else {
        Box::new(EndpointMapDrain::new(source, drain))
    }
}

/// `strings.FieldsFunc(raw, r == ',' || r == ' ')`: split on commas/spaces and
/// drop empty fields.
pub fn parse_drain_servers(raw: &str) -> Vec<String> {
    raw.split([',', ' '])
        .filter(|field| !field.is_empty())
        .map(str::to_owned)
        .collect()
}

/// `SearcherURLs()`: a conf-based map over `ServiceConnections.Searchers`.
pub struct SearcherUrls {
    map: ConfEndpointMap,
}

impl SearcherUrls {
    pub fn new(source: impl Fn() -> Vec<String> + Send + Sync + 'static) -> Self {
        Self {
            map: ConfEndpointMap::new(source),
        }
    }

    /// `endpoint.ConfBased(func(conns) []string { return conns.Searchers })`.
    pub fn from_connections(connections: &ServiceConnections) -> Self {
        Self::from_endpoints(connections.searchers.clone())
    }

    pub fn from_endpoints(endpoints: Vec<String>) -> Self {
        Self {
            map: ConfEndpointMap::from_endpoints(endpoints),
        }
    }

    pub fn endpoints(&self) -> Vec<String> {
        self.map.configured_endpoints()
    }

    pub fn get(&self, key: &str) -> Result<String, IndexersError> {
        self.map.get(key)
    }
}

// ------------------------------------------------------- searcher connection cache

/// One cached connection: the lazy tonic channel (or its parse error) and the
/// last access instant (the baseline `ttlcache` expires on inactivity).
struct CachedConnection {
    channel: Result<Channel, SearcherError>,
    last_used: Instant,
}

/// `grpc.DefaultConnectionCache` (`internal/grpc/defaults/cache.go`).
///
/// The baseline caches a lazy `grpc.ClientConn` per address and also caches
/// address-parsing/dial errors (`connAndError`). The Rust port caches a lazy
/// tonic [`Channel`] plus its `SearcherError` using the same semantics: hits
/// refresh the inactivity deadline and are shared by every caller of the same
/// cache instance.
///
/// Baseline caveat: Go expires entries from a background reaper (every minute,
/// 10 minute TTL), so an already-expired but not-yet-reaped entry is still
/// returned by `Get` and refreshes its deadline. This port has no background
/// reaper and expires lazily on access (`now - last_used >= ttl`), which makes
/// the TTL testable with an injected clock; the only observable difference is
/// a connection idle for `[10m, 11m)` may be re-dialed instead of reused.
pub struct GrpcConnectionCache<C: Clock = SystemClock> {
    ttl: Duration,
    clock: C,
    created: AtomicUsize,
    connections: Mutex<HashMap<String, CachedConnection>>,
}

/// The baseline singleton name used by the searcher path
/// (`search.SearcherGRPCConnectionCache()`).
pub type SearcherGrpcConnectionCache<C = SystemClock> = GrpcConnectionCache<C>;

impl GrpcConnectionCache<SystemClock> {
    /// `NewConnectionCache`: 10 minute inactivity TTL.
    pub fn new() -> Self {
        Self::with_clock_and_ttl(GRPC_CONNECTION_CACHE_TTL, SystemClock)
    }
}

impl Default for GrpcConnectionCache<SystemClock> {
    fn default() -> Self {
        Self::new()
    }
}

impl<C: Clock> GrpcConnectionCache<C> {
    pub fn with_clock_and_ttl(ttl: Duration, clock: C) -> Self {
        Self {
            ttl,
            clock,
            created: AtomicUsize::new(0),
            connections: Mutex::new(HashMap::new()),
        }
    }

    /// `ConnectionCache.GetConnection`: build (lazily) on a miss and cache the
    /// result. A hit refreshes the inactivity deadline. Parse/dial errors are
    /// cached exactly like successful channels.
    pub fn channel(&self, endpoint: &str) -> Result<Channel, SearcherError> {
        self.channel_with_creation(endpoint).0
    }

    /// [`Self::channel`] plus whether this call constructed a new channel
    /// (a cache miss that parsed). Hits and cached errors return `false`.
    /// Differential harnesses use this to attribute creations per endpoint
    /// without racing on the aggregate counter.
    pub fn channel_with_creation(&self, endpoint: &str) -> (Result<Channel, SearcherError>, bool) {
        let now = self.clock.now();
        let mut connections = lock(&self.connections);
        if let Some(connection) = connections.get_mut(endpoint) {
            if now.saturating_duration_since(connection.last_used) < self.ttl {
                connection.last_used = now;
                return (connection.channel.clone(), false);
            }
        }

        let channel = build_channel(endpoint);
        let created = channel.is_ok();
        if created {
            self.created.fetch_add(1, Ordering::SeqCst);
        }
        connections.insert(
            endpoint.to_owned(),
            CachedConnection {
                channel: channel.clone(),
                last_used: now,
            },
        );
        (channel, created)
    }

    /// Adapter preserving the pre-M9.8iw zoekt use: the same channel cache
    /// wrapped in a lazy zoekt client (`ZoektClient::new`).
    pub fn zoekt_client(&self, endpoint: &str) -> Result<ZoektClient, ZoektError> {
        match self.channel(endpoint) {
            Ok(channel) => Ok(ZoektClient::new(channel)),
            Err(error) => Err(ZoektError::InvalidEndpoint(error.message)),
        }
    }

    /// Backwards-compatible zoekt accessor (`zoekt_client`).
    pub fn client(&self, endpoint: &str) -> Result<ZoektClient, ZoektError> {
        self.zoekt_client(endpoint)
    }

    /// Number of successful channel constructions (cache misses that parsed).
    /// Observation aid for differential tests; the baseline analogue is the
    /// number of distinct `grpc.ClientConn`s dialed.
    pub fn channel_creations(&self) -> usize {
        self.created.load(Ordering::SeqCst)
    }

    /// Drop a cached entry, reporting whether one existed.
    ///
    /// The baseline never evicts explicitly (grpc-go's `ClientConn` transparently
    /// reconnects after a transport failure). A lazily-built tonic [`Channel`]
    /// does not: it belongs to the Tokio runtime that created it, so once a
    /// failed RPC tears the connection down — or the creating runtime is gone —
    /// the cached channel can never serve another call. Callers therefore evict
    /// on failure so the next attempt re-dials, which is observably what the
    /// baseline's reconnecting `ClientConn` does.
    pub fn remove(&self, endpoint: &str) -> bool {
        lock(&self.connections).remove(endpoint).is_some()
    }

    /// Number of cached addresses (observation aid).
    pub fn len(&self) -> usize {
        lock(&self.connections).len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// Build a lazily-connected cleartext channel. The baseline parses the address
/// and dials `u.Host` with `insecure.NewCredentials()` (`defaults.Dial`), so a
/// bare `host:port` gets an `http://` scheme and an explicit `https://` scheme
/// is still transported in cleartext.
fn build_channel(endpoint: &str) -> Result<Channel, SearcherError> {
    let uri = if endpoint.contains("://") {
        endpoint.to_owned()
    } else {
        format!("http://{endpoint}")
    };
    Channel::from_shared(uri)
        .map(|endpoint| endpoint.connect_lazy())
        .map_err(|error| SearcherError::other(format!("failed to parse URL: {error}")))
}

/// A cache-like source of tonic channels. Connectors depend on this trait so
/// tests can inject a counting source without changing the transport wiring.
pub trait GrpcChannelSource: Send + Sync {
    fn channel(&self, endpoint: &str) -> Result<Channel, SearcherError>;

    /// Forget the cached connection for `endpoint` after a failed RPC, so the
    /// next attempt starts from a fresh dial (see
    /// [`GrpcConnectionCache::remove`]). Default: nothing to do.
    fn invalidate(&self, _endpoint: &str) {}
}

impl<C: Clock> GrpcChannelSource for GrpcConnectionCache<C> {
    fn channel(&self, endpoint: &str) -> Result<Channel, SearcherError> {
        GrpcConnectionCache::channel(self, endpoint)
    }

    fn invalidate(&self, endpoint: &str) {
        GrpcConnectionCache::remove(self, endpoint);
    }
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

// ------------------------------------------------------------------ zoekt dialing

/// `zoektGRPCClient`: a lazy zoekt client that keeps its dial error until the
/// first RPC (`ZoektDial` never fails).
#[derive(Debug, Clone)]
pub struct EnvClient {
    inner: Result<ZoektClient, ZoektError>,
}

impl EnvClient {
    pub fn new(endpoint: &str) -> Self {
        Self {
            inner: ZoektClient::connect_lazy(endpoint),
        }
    }

    pub fn from_result(inner: Result<ZoektClient, ZoektError>) -> Self {
        Self { inner }
    }
}

impl Streamer for EnvClient {
    type Error = ZoektError;

    async fn list(
        &self,
        query: &Query,
        opts: Option<ListOptions>,
    ) -> Result<ListResponse, ZoektError> {
        match &self.inner {
            // `zoekt.Streamer.List` (the proto-default option for `None`).
            Ok(client) => Streamer::list(client, query, opts).await,
            Err(error) => Err(error.clone()),
        }
    }

    async fn stream_search(
        &self,
        query: &Query,
        options: &SearchOptions,
        sender: &mut (dyn FnMut(SearchChunk) + Send),
    ) -> Result<(), ZoektError> {
        match &self.inner {
            Ok(client) => Streamer::stream_search(client, query, options, sender).await,
            Err(error) => Err(error.clone()),
        }
    }
}

impl CloseableStreamer for EnvClient {
    fn close(&self) {}
}

/// `NewCachedSearcher(ttl, ZoektDial(endpoint))` as a closeable streamer.
///
/// [`CachedStreamer`] has no `Close` of its own (the cached `List` needs no
/// transport teardown); closing the per-endpoint streamer is a no-op, like
/// `cachedSearcher.Close` delegating to the wrapped gRPC client.
pub struct EnvEndpointStreamer {
    inner: CachedStreamer<EnvClient>,
}

impl Streamer for EnvEndpointStreamer {
    type Error = ZoektError;

    async fn list(
        &self,
        query: &Query,
        opts: Option<ListOptions>,
    ) -> Result<ListResponse, ZoektError> {
        self.inner.list(query, opts).await
    }

    async fn stream_search(
        &self,
        query: &Query,
        options: &SearchOptions,
        sender: &mut (dyn FnMut(SearchChunk) + Send),
    ) -> Result<(), ZoektError> {
        self.inner.stream_search(query, options, sender).await
    }
}

impl CloseableStreamer for EnvEndpointStreamer {
    fn close(&self) {}
}

/// `getIndexedDialer`'s inner function: `NewCachedSearcher(ttl,
/// ZoektDial(endpoint))`.
#[derive(Debug, Clone)]
pub struct EnvDialer {
    ttl: Duration,
}

impl EnvDialer {
    pub fn new(ttl: Duration) -> Self {
        Self { ttl }
    }
}

impl ZoektDialer for EnvDialer {
    type Streamer = EnvEndpointStreamer;

    fn dial(&self, endpoint: &str) -> Self::Streamer {
        EnvEndpointStreamer {
            inner: CachedStreamer::new(self.ttl, EnvClient::new(endpoint)),
        }
    }
}

/// Result-level adapter over a cached per-endpoint [`Streamer`].
pub struct DialedClient<S: CloseableStreamer> {
    streamer: S,
}

impl<S: CloseableStreamer> DialedClient<S> {
    pub fn new(streamer: S) -> Self {
        Self { streamer }
    }

    pub fn streamer(&self) -> &S {
        &self.streamer
    }
}

impl<S: CloseableStreamer> HorizontalClient for DialedClient<S>
where
    S::Error: sg_zoekt::horizontal_searcher::ClientError,
{
    type Error = S::Error;

    fn list<'a>(
        &'a self,
        query: &'a Query,
        opts: Option<ListOptions>,
    ) -> Pin<Box<dyn Future<Output = Result<ListResponse, Self::Error>> + Send + 'a>> {
        Box::pin(async move { Streamer::list(&self.streamer, query, opts).await })
    }

    fn stream_search<'a>(
        &'a self,
        query: &'a Query,
        options: Option<&'a SearchOptions>,
        sender: &'a mut (dyn FnMut(SearchResult) + Send),
    ) -> Pin<Box<dyn Future<Output = Result<(), Self::Error>> + Send + 'a>> {
        Box::pin(async move {
            // The job tree always passes options; the result-level trait's
            // `None` maps to the proto default, like a nil Go options pointer.
            let options = options.cloned().unwrap_or_default();
            let mut on_chunk = |chunk: SearchChunk| sender(chunk.to_search_result());
            self.streamer
                .stream_search(query, &options, &mut on_chunk)
                .await
        })
    }

    fn close(&self) {
        self.streamer.close();
    }
}

// ------------------------------------------------------------- indexed composition

/// Bridges the result-level [`HorizontalSearcher`] to a chunk-level
/// [`Streamer`]. `SearchChunk` and `SearchResult` differ only in the baseline's
/// always-empty `RepoURLs`/`LineFragments` maps, so the round trip is lossless
/// for the fields that cross this boundary.
pub struct HorizontalStreamer<C: HorizontalClient> {
    searcher: HorizontalSearcher<C>,
}

impl<C: HorizontalClient> HorizontalStreamer<C> {
    pub fn new(searcher: HorizontalSearcher<C>) -> Self {
        Self { searcher }
    }

    pub fn searcher(&self) -> &HorizontalSearcher<C> {
        &self.searcher
    }
}

impl<C: HorizontalClient> Streamer for HorizontalStreamer<C>
where
    C::Error: Clone,
{
    type Error = C::Error;

    async fn list(
        &self,
        query: &Query,
        opts: Option<ListOptions>,
    ) -> Result<ListResponse, Self::Error> {
        self.searcher.list(query, opts).await
    }

    async fn stream_search(
        &self,
        query: &Query,
        options: &SearchOptions,
        sender: &mut (dyn FnMut(SearchChunk) + Send),
    ) -> Result<(), Self::Error> {
        let mut on_result = |result: SearchResult| {
            sender(SearchChunk {
                crashes: result.stats.crashes,
                stats: result.stats,
                progress: result.progress,
                files: result.files,
            });
        };
        self.searcher
            .stream_search(query, Some(options), &mut on_result)
            .await
    }
}

/// `CachedSearcher(ttl, MeteredSearcher("", HorizontalSearcher))`.
pub type IndexedStreamer<C, K = SystemClock> =
    CachedStreamer<MeteredStreamer<HorizontalStreamer<C>>, K>;

/// `Indexed()`'s wrapper stack around an arbitrary result-level client.
pub fn compose_indexed<C, F, D>(endpoints: F, dial: D, ttl: Duration) -> IndexedStreamer<C>
where
    C: HorizontalClient + 'static,
    C::Error: Clone + ErrorClassifier,
    F: Fn() -> Result<Vec<String>, C::Error> + Send + Sync + 'static,
    D: Fn(&str) -> C + Send + Sync + 'static,
{
    compose_indexed_with_clock_and_jitter(endpoints, dial, ttl, SystemClock, |_| 0)
}

/// [`compose_indexed`] with injectable time/jitter for differential tests.
pub fn compose_indexed_with_clock_and_jitter<C, F, D, K, J>(
    endpoints: F,
    dial: D,
    ttl: Duration,
    clock: K,
    jitter: J,
) -> IndexedStreamer<C, K>
where
    C: HorizontalClient + 'static,
    C::Error: Clone + ErrorClassifier,
    F: Fn() -> Result<Vec<String>, C::Error> + Send + Sync + 'static,
    D: Fn(&str) -> C + Send + Sync + 'static,
    K: Clock,
    J: Fn(Duration) -> i128 + Send + Sync + 'static,
{
    let searcher = HorizontalSearcher::new(endpoints, dial);
    let metered = MeteredStreamer::new("", HorizontalStreamer::new(searcher));
    CachedStreamer::with_clock_and_jitter(ttl, metered, clock, jitter)
}

/// The production per-endpoint client: `cachedZoektDialer(EnvDialer)`.
pub type ProductionEndpointClient = DialedClient<sg_zoekt::dialer::CachedStreamerCloser<EnvDialer>>;
/// The production `Indexed()` streamer.
pub type ProductionIndexedStreamer = IndexedStreamer<ProductionEndpointClient>;

/// Failure to build the production `Indexed()` streamer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IndexedError {
    /// No zoekt endpoints configured.
    MissingZoekt,
    /// An endpoint could not be parsed into a lazy client.
    Endpoint(String),
}

impl fmt::Display for IndexedError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingZoekt => formatter.write_str("search: no zoekt endpoints configured"),
            Self::Endpoint(message) => formatter.write_str(message),
        }
    }
}

impl std::error::Error for IndexedError {}

/// The production [`JobStreamer`]: `Indexed()` driven through the M9.8ig
/// blocking bridge.
pub struct IndexedJobStreamer {
    streamer: BlockingStreamer<ProductionIndexedStreamer>,
}

impl IndexedJobStreamer {
    /// Build `Indexed()` from the configured endpoints and list TTL.
    ///
    /// Endpoint parsing is validated eagerly (the baseline's dial is
    /// infallible because it stores the parse error in `dialErr`; the streamer
    /// still defers connection failures to the first RPC).
    pub fn new(endpoints: &[String], ttl: Duration) -> Result<Self, IndexedError> {
        if endpoints.is_empty() {
            return Err(IndexedError::MissingZoekt);
        }
        for endpoint in endpoints {
            ZoektClient::connect_lazy(endpoint)
                .map_err(|error| IndexedError::Endpoint(error.to_string()))?;
        }

        let endpoints = endpoints.to_vec();
        let dialer = CachedZoektDialer::new(EnvDialer::new(ttl));
        let streamer = compose_indexed(
            move || Ok(endpoints.clone()),
            move |endpoint: &str| DialedClient::new(dialer.dial(endpoint)),
            ttl,
        );
        Ok(Self {
            streamer: BlockingStreamer::new(streamer),
        })
    }

    pub fn from_connections(connections: &ServiceConnections) -> Result<Self, IndexedError> {
        Self::new(&connections.zoekts, connections.zoekt_list_ttl)
    }

    pub fn streamer(&self) -> &BlockingStreamer<ProductionIndexedStreamer> {
        &self.streamer
    }
}

impl JobStreamer for IndexedJobStreamer {
    fn stream_search(
        &self,
        query: &Query,
        options: &SearchOptions,
        sender: &mut dyn FnMut(SearchResult),
    ) -> Result<(), JobError> {
        self.streamer
            .stream_search_blocking(query, options, sender)
            .map_err(|error| JobError(error.to_string()))
    }
}

// ---------------------------------------------------------------- indexed listing

/// `ZoektAllIndexed.Stats` (`zoekt.RepoStats`), including the line counts the
/// `RepositoryStats.indexedLinesCount` resolver sums.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RepoStats {
    pub repos: usize,
    pub shards: i64,
    pub documents: i64,
    pub index_bytes: i64,
    pub content_bytes: i64,
    pub new_lines_count: u64,
    pub default_branch_new_lines_count: u64,
    pub other_branches_new_lines_count: u64,
}

impl RepoStats {
    /// Copy the upstream `zoekt.RepoStats` fields (`Repos` included; the
    /// horizontal searcher already set it to `len(Repos)+len(ReposMap)`).
    pub fn from_zoekt(stats: &sg_zoekt::types::RepoStats) -> Self {
        Self {
            repos: usize::try_from(stats.repos).unwrap_or(0),
            shards: stats.shards,
            documents: stats.documents,
            index_bytes: stats.index_bytes,
            content_bytes: stats.content_bytes,
            new_lines_count: stats.new_lines_count,
            default_branch_new_lines_count: stats.default_branch_new_lines_count,
            other_branches_new_lines_count: stats.other_branches_new_lines_count,
        }
    }
}

/// `search.ZoektAllIndexed`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ZoektAllIndexed {
    pub repos_map: BTreeMap<u32, MinimalRepoListEntry>,
    pub crashes: i64,
    pub stats: RepoStats,
}

/// `ListAllIndexed(ctx, zs)`: the `Const{Value:true}` + `RepoListFieldReposMap`
/// list, reduced to the fields `ZoektAllIndexed` exposes.
pub async fn list_all_indexed<S: Streamer>(zs: &S) -> Result<ZoektAllIndexed, S::Error> {
    let list = zs
        .list(
            &Query::Const(true),
            Some(ListOptions {
                field: RepoListField::ReposMap,
            }),
        )
        .await?;
    Ok(ZoektAllIndexed {
        repos_map: list.repos_map,
        crashes: list.crashes,
        stats: RepoStats::from_zoekt(&list.stats),
    })
}

// -------------------------------------------------------------------- indexers

/// `reposAtEndpoint(dial)`: list one endpoint's indexed repositories,
/// returning an empty set on failure.
pub fn repos_at_endpoint<S, D>(dial: D) -> impl Fn(&str) -> ReposMap + Send + Sync
where
    S: Streamer + 'static,
    D: Fn(&str) -> S + Send + Sync + 'static,
{
    move |endpoint: &str| {
        let streamer = dial(endpoint);
        let listed = run_to_completion(async move {
            streamer
                .list(
                    &Query::Const(true),
                    Some(ListOptions {
                        field: RepoListField::ReposMap,
                    }),
                )
                .await
        });
        match listed {
            Ok(list) => list.repos_map.keys().copied().collect(),
            Err(_) => ReposMap::new(),
        }
    }
}

/// `Indexers()`: the drain-aware endpoint map plus `reposAtEndpoint` over the
/// cached indexed dialer.
pub fn indexers_from_source<F>(source: F, drain: Vec<String>, ttl: Duration) -> Indexers
where
    F: Fn() -> Vec<String> + Send + Sync + 'static,
{
    let map = new_endpoint_map_drain(source, drain);
    let dialer = CachedZoektDialer::new(EnvDialer::new(ttl));
    Indexers {
        map: Some(map),
        indexed: Box::new(repos_at_endpoint(move |endpoint: &str| {
            dialer.dial(endpoint)
        })),
    }
}

/// `Indexers()` over an assembly-time configuration snapshot.
pub fn indexers_from_connections(connections: &ServiceConnections, drain: Vec<String>) -> Indexers {
    let endpoints = connections.zoekts.clone();
    indexers_from_source(move || endpoints.clone(), drain, connections.zoekt_list_ttl)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn drain_parsing_matches_fields_func() {
        assert_eq!(parse_drain_servers(" a, b ,,c  d "), ["a", "b", "c", "d"]);
        assert!(parse_drain_servers("").is_empty());
        assert!(parse_drain_servers(" , ").is_empty());
    }

    #[test]
    fn drain_map_keeps_drained_endpoints_out_of_get() {
        let drain = vec!["b".to_owned()];
        let map = EndpointMapDrain::new(
            || vec!["a".to_owned(), "b".to_owned(), "c".to_owned()],
            drain,
        );
        assert_eq!(
            map.endpoints().unwrap(),
            ["a", "c", "b"],
            "active endpoints keep config order, drain is appended"
        );
        for key in ["r1", "r2", "r3"] {
            assert_ne!(map.get(key).unwrap(), "b");
        }
    }

    #[test]
    fn drain_map_get_matches_rendezvous_over_active_nodes() {
        let conf = vec!["a".to_owned(), "b".to_owned(), "c".to_owned()];
        let drain = vec!["b".to_owned()];
        let source = conf.clone();
        let map = EndpointMapDrain::new(move || source.clone(), drain);
        let active = vec!["a".to_owned(), "c".to_owned()];
        for key in ["r1", "r2", "r3"] {
            assert_eq!(
                map.get(key).unwrap(),
                rendezvous_order(&active, key)[0],
                "drained nodes must not participate in the hash"
            );
        }
    }

    #[test]
    fn empty_conf_map_reports_the_baseline_error() {
        let map = ConfEndpointMap::from_endpoints(Vec::new());
        assert_eq!(
            map.get("x").unwrap_err().to_string(),
            "endpoint.Map(conf-based) is empty"
        );
    }

    #[test]
    fn searcher_urls_wrap_a_conf_getter() {
        let urls = SearcherUrls::from_endpoints(vec!["http://a:1".to_owned()]);
        assert_eq!(urls.endpoints(), ["http://a:1"]);
        assert_eq!(urls.get("repo@rev").unwrap(), "http://a:1");
    }

    #[derive(Clone)]
    struct TestClock(std::sync::Arc<Mutex<Instant>>);

    impl TestClock {
        fn new(now: Instant) -> Self {
            Self(std::sync::Arc::new(Mutex::new(now)))
        }

        fn advance(&self, duration: Duration) {
            let mut now = self.0.lock().unwrap();
            *now += duration;
        }
    }

    impl Clock for TestClock {
        fn now(&self) -> Instant {
            *self.0.lock().unwrap()
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn connection_cache_reuses_entries_and_caches_errors() {
        let clock = TestClock::new(Instant::now());
        let cache = SearcherGrpcConnectionCache::with_clock_and_ttl(Duration::from_secs(10), clock);
        assert!(cache.channel("127.0.0.1:1").is_ok());
        assert_eq!(cache.channel_creations(), 1);
        assert_eq!(cache.len(), 1);
        assert!(cache.channel("127.0.0.1:1").is_ok());
        assert_eq!(cache.channel_creations(), 1);
        assert_eq!(cache.len(), 1);
        // Parse errors are cached alongside channels but never dial.
        assert!(cache.channel("bad endpoint").is_err());
        assert!(cache.channel("bad endpoint").is_err());
        assert_eq!(cache.channel_creations(), 1);
        assert_eq!(cache.len(), 2);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn connection_cache_expires_after_the_idle_ttl() {
        let start = Instant::now();
        let clock = TestClock::new(start);
        let cache =
            SearcherGrpcConnectionCache::with_clock_and_ttl(Duration::from_secs(10), clock.clone());
        assert!(cache.channel("127.0.0.1:1").is_ok());
        assert_eq!(cache.channel_creations(), 1);

        clock.advance(Duration::from_secs(9));
        assert!(cache.channel("127.0.0.1:1").is_ok());
        assert_eq!(
            cache.channel_creations(),
            1,
            "an access refreshes the deadline"
        );

        clock.advance(Duration::from_secs(10));
        assert!(cache.channel("127.0.0.1:1").is_ok());
        assert_eq!(
            cache.channel_creations(),
            2,
            "idle for the whole TTL re-dials"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn zoekt_adapter_reuses_the_channel_cache() {
        let cache = SearcherGrpcConnectionCache::new();
        assert!(cache.client("127.0.0.1:1").is_ok());
        assert!(cache.client("127.0.0.1:1").is_ok());
        assert_eq!(cache.channel_creations(), 1);
        assert!(cache
            .client("bad endpoint")
            .unwrap_err()
            .to_string()
            .starts_with("failed to parse URL"));
    }

    #[test]
    fn env_client_keeps_the_dial_error_lazy() {
        let client = EnvClient::from_result(Err(ZoektError::InvalidEndpoint("bad".to_owned())));
        let error = run_to_completion(client.list(&Query::Const(true), None)).unwrap_err();
        assert_eq!(error.to_string(), "bad");
    }

    #[test]
    fn indexers_from_source_has_a_drain_map() {
        let indexers = indexers_from_source(
            || vec!["a".to_owned(), "b".to_owned()],
            vec!["b".to_owned()],
            Duration::from_secs(5),
        );
        assert!(indexers.enabled());
        assert_eq!(
            indexers.map.as_ref().unwrap().endpoints().unwrap(),
            ["a", "b"]
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn cached_dialer_reports_endpoints() {
        let dialer = CachedZoektDialer::new(EnvDialer::new(Duration::from_secs(5)));
        assert_eq!(dialer.cache_len(), 0);
        let _ = dialer.dial("a");
        assert_eq!(dialer.cache_len(), 1);
    }
}
