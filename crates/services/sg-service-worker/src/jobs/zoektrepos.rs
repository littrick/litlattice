#![forbid(unsafe_code)]

//! `cmd/worker/internal/zoektrepos`: the `zoekt-repos-updater` job.
//!
//! Every hour the worker lists every indexed repository from every configured
//! zoekt endpoint — `search.ListAllIndexed(ctx, search.Indexed())`, i.e. a
//! `query.Const{Value:true}` list with `ListOptions{Field:
//! RepoListFieldReposMap}` — and reconciles `zoekt_repos` with the result
//! (`db.ZoektRepos().UpdateIndexStatuses`). A handler error is only logged
//! (`error updating zoekt repos`); the routine keeps running.
//!
//! The listing layers reproduced here are `backend.HorizontalSearcher.List`:
//! one concurrent `List` per endpoint, results merged in arrival order with
//! later endpoints overwriting earlier ones, and endpoints whose failure
//! matches `isZoektRolloutError` (DNS not found, dial timeout, connection
//! refused, read failed) skipped instead of failing the whole reconciliation.
//!
//! Not ported: the surrounding `backend.NewCachedSearcher` /
//! `backend.NewMeteredSearcher` wrappers (`SRC_INDEXED_SEARCH_LIST_CACHE_TTL`
//! defaults to 5s, irrelevant to an hourly job; the RED metrics have no Rust
//! counterpart in this slice) and `HorizontalSearcher`'s persistent
//! per-endpoint client cache (clients are dialled once per reconciliation).

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use sg_config::LiveConfig;
use sg_db::zoekt_repos::{update_index_statuses, MinimalRepoListEntry, RepositoryBranch};
use sg_zoekt::client::{Client, Error};
use sg_zoekt::query::Query;
use sg_zoekt::types::{ListOptions, ListResponse, RepoListField};
use sqlx::PgPool;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

use super::{Job, JobDeps};
use crate::framework::periodic::{Handler, HandlerError, PeriodicRoutine};
use crate::framework::BackgroundRoutine;

/// `updater.Description`.
pub const JOB_DESCRIPTION: &str = "zoektrepos.Updater updates the zoekt_repos table periodically to reflect the search-index status of each repository.";
/// `goroutine.WithName`.
pub const ROUTINE_NAME: &str = "search.index-status-reconciler";
/// `goroutine.WithDescription`.
pub const ROUTINE_DESCRIPTION: &str = "reconciles indexed status between zoekt and postgres";
/// `goroutine.WithInterval(1*time.Hour)`.
pub const INTERVAL: Duration = Duration::from_secs(60 * 60);

/// `zoektrepos.updater`.
pub struct ZoektReposUpdater;

#[async_trait]
impl Job for ZoektReposUpdater {
    fn description(&self) -> String {
        JOB_DESCRIPTION.to_owned()
    }

    async fn routines(
        &self,
        deps: &JobDeps,
        _setup_ctx: CancellationToken,
    ) -> Result<Vec<BackgroundRoutine>, String> {
        Ok(vec![PeriodicRoutine::new(Arc::new(ZoektReposHandler::new(
            deps.frontend.clone(),
            Arc::clone(&deps.live_config),
        )) as Arc<dyn Handler>)
        .with_name(ROUTINE_NAME)
        .with_description(ROUTINE_DESCRIPTION)
        .with_interval(INTERVAL)
        .into()])
    }
}

/// The per-endpoint half of `search.Indexed()`: the addresses
/// `HorizontalSearcher.Map.Endpoints()` yields and one endpoint's
/// `zoekt.Streamer.List`. Kept as a trait so the differential harness can
/// script endpoints (and their failures) without a zoekt server.
#[async_trait]
pub trait EndpointSource: Send + Sync {
    /// `endpoint.ConfBased(func(conns) []string { return conns.Zoekts })`.
    async fn endpoints(&self) -> Result<Vec<String>, String>;

    /// `zoekt.Streamer.List(ctx, &query.Const{Value:true}, &zoekt.ListOptions{
    /// Field: zoekt.RepoListFieldReposMap})`.
    async fn list(&self, endpoint: &str, ctx: &CancellationToken) -> Result<ListResponse, Error>;
}

/// The production [`EndpointSource`]: the live configuration's zoekt
/// addresses and the gRPC client for each.
pub struct ZoektIndexedRepos {
    live_config: Arc<LiveConfig>,
}

impl ZoektIndexedRepos {
    pub fn new(live_config: Arc<LiveConfig>) -> Self {
        Self { live_config }
    }
}

#[async_trait]
impl EndpointSource for ZoektIndexedRepos {
    async fn endpoints(&self) -> Result<Vec<String>, String> {
        // `conf.Get().ServiceConnections().Zoekts`, read on every call so a
        // configuration change is picked up without restarting the worker.
        Ok(self
            .live_config
            .current()
            .service_connections
            .zoekts
            .clone())
    }

    async fn list(&self, endpoint: &str, ctx: &CancellationToken) -> Result<ListResponse, Error> {
        // `backend.ZoektDial(endpoint)`: the baseline dials lazily and lets
        // the RPC surface connection failures.
        let client = Client::connect_lazy(endpoint)?;
        tokio::select! {
            () = ctx.cancelled() => Err(Error::Canceled),
            result = client.list(
                &Query::Const(true),
                ListOptions { field: RepoListField::ReposMap },
            ) => result,
        }
    }
}

/// `search.ListAllIndexed(ctx, search.Indexed())`, minus the response fields
/// the updater does not read (`Repos`, `Crashes`, `Stats`).
pub async fn list_all_indexed(
    source: Arc<dyn EndpointSource>,
    ctx: &CancellationToken,
) -> Result<BTreeMap<u32, MinimalRepoListEntry>, String> {
    let mut endpoints: Vec<String> = Vec::new();
    for endpoint in source.endpoints().await? {
        // `HorizontalSearcher.searchers` keys its clients by address, so a
        // duplicate address is dialled once.
        if !endpoints.contains(&endpoint) {
            endpoints.push(endpoint);
        }
    }

    // `HorizontalSearcher.List`: one goroutine per endpoint. The results are
    // applied in endpoint order rather than arrival order so repeated runs
    // are deterministic; the baseline is racy only when the same repository
    // appears on two endpoints (a rollout/rebalancing state).
    let mut tasks = JoinSet::new();
    for (index, endpoint) in endpoints.iter().cloned().enumerate() {
        let source = Arc::clone(&source);
        let ctx = ctx.clone();
        tasks.spawn(async move { (index, source.list(&endpoint, &ctx).await) });
    }
    let mut results: Vec<Option<Result<ListResponse, Error>>> =
        endpoints.iter().map(|_| None).collect();
    while let Some(joined) = tasks.join_next().await {
        let (index, result) = joined.map_err(|error| error.to_string())?;
        results[index] = Some(result);
    }

    aggregate(
        results
            .into_iter()
            .map(|result| result.expect("every spawned task stores its result"))
            .collect(),
    )
}

/// The aggregation half of `HorizontalSearcher.List`: merge `ReposMap`s with
/// later endpoints overwriting earlier ones, skip rollout errors and fail on
/// everything else.
pub fn aggregate(
    results: Vec<Result<ListResponse, Error>>,
) -> Result<BTreeMap<u32, MinimalRepoListEntry>, String> {
    let mut repos_map = BTreeMap::new();
    for result in results {
        let response = match result {
            Ok(response) => response,
            Err(error) => {
                if is_rollout_error(&error) {
                    continue;
                }
                return Err(error.to_string());
            }
        };
        for (id, entry) in response.repos_map {
            repos_map.insert(
                id,
                MinimalRepoListEntry {
                    has_symbols: entry.has_symbols,
                    branches: entry
                        .branches
                        .into_iter()
                        .map(|branch| RepositoryBranch {
                            name: branch.name,
                            version: branch.version,
                        })
                        .collect(),
                    index_time_unix: entry.index_time_unix,
                },
            );
        }
    }
    Ok(repos_map)
}

/// `isZoektRolloutError`: only very specific failures are attributed to a
/// zoekt rollout (and skipped). The baseline inspects `*net.DNSError` and
/// `*net.OpError`; the gRPC path converts those into status errors, which the
/// baseline's `errors.As` does not match either, so this remains an
/// approximation keyed on the message.
pub fn is_rollout_error(error: &Error) -> bool {
    match error {
        Error::Transport(message) => rollout_reason(message).is_some(),
        Error::Status(status) => rollout_reason(status.message()).is_some(),
        // `context.Canceled`, `context.DeadlineExceeded` and (in the
        // baseline) a dial error that is not one of the four reasons below.
        Error::Canceled | Error::DeadlineExceeded | Error::InvalidEndpoint(_) => false,
    }
}

/// `zoektRolloutReason`.
fn rollout_reason(message: &str) -> Option<&'static str> {
    let message = message.to_ascii_lowercase();

    if message.contains("dns")
        || message.contains("failed to lookup")
        || message.contains("name or service not known")
        || message.contains("no such host")
    {
        return Some("dns-not-found");
    }

    if message.contains("connection refused") {
        return Some("dial-refused");
    }

    if message.contains("connect") && message.contains("timed out") {
        return Some("dial-timeout");
    }

    // `opErr.Op == "read"`: the connection died mid-request, which happens
    // when zoekt restarts during a rollout.
    if message.contains("error reading a body")
        || message.contains("connection reset")
        || message.contains("broken pipe")
        || message.contains("unexpected eof")
    {
        return Some("read-failed");
    }

    None
}

/// `zoektrepos.handler`.
pub struct ZoektReposHandler {
    pool: PgPool,
    source: Arc<dyn EndpointSource>,
}

impl ZoektReposHandler {
    pub fn new(pool: PgPool, live_config: Arc<LiveConfig>) -> Self {
        Self {
            pool,
            source: Arc::new(ZoektIndexedRepos::new(live_config)),
        }
    }

    /// The harness seam: script the endpoints instead of dialling zoekt.
    pub fn with_source(pool: PgPool, source: Arc<dyn EndpointSource>) -> Self {
        Self { pool, source }
    }
}

#[async_trait]
impl Handler for ZoektReposHandler {
    async fn handle(&self, ctx: CancellationToken) -> Result<(), HandlerError> {
        let indexed = list_all_indexed(Arc::clone(&self.source), &ctx)
            .await
            .map_err(HandlerError::new)?;
        update_index_statuses(&self.pool, &indexed)
            .await
            .map_err(|error| HandlerError::new(error.to_string()))
    }

    fn handle_error(&self, error: &HandlerError) {
        // `h.logger.Error("error updating zoekt repos", log.Error(err))`.
        tracing::error!(error = %error, "error updating zoekt repos");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn deps() -> JobDeps {
        JobDeps {
            frontend: sqlx::postgres::PgPoolOptions::new()
                .connect_lazy("postgres://unused@localhost/unused")
                .expect("lazy pool"),
            live_config: Arc::new(sg_config::LiveConfig::new(sg_config::Unified::default())),
            keyring: Arc::new(sg_keyring::Keyring::default()),
        }
    }

    fn entry(
        name: &str,
        version: &str,
        index_time_unix: i64,
    ) -> sg_zoekt::types::MinimalRepoListEntry {
        sg_zoekt::types::MinimalRepoListEntry {
            has_symbols: true,
            branches: vec![sg_zoekt::types::RepositoryBranch {
                name: name.to_owned(),
                version: version.to_owned(),
            }],
            index_time_unix,
        }
    }

    fn response(entries: &[(u32, &str, &str, i64)]) -> Result<ListResponse, Error> {
        Ok(ListResponse {
            repos: Vec::new(),
            repos_map: entries
                .iter()
                .map(|(id, name, version, index_time_unix)| {
                    (*id, entry(name, version, *index_time_unix))
                })
                .collect(),
            crashes: 0,
        })
    }

    #[tokio::test]
    async fn job_constructs_the_baseline_reconciler() {
        let job = ZoektReposUpdater;
        assert_eq!(job.description(), JOB_DESCRIPTION);
        assert!(job.configs().is_empty());

        let routines = job
            .routines(&deps(), CancellationToken::new())
            .await
            .expect("routines");
        assert_eq!(routines.len(), 1);
        assert_eq!(routines[0].name(), ROUTINE_NAME);
        assert_eq!(routines[0].description(), ROUTINE_DESCRIPTION);
        assert_eq!(routines[0].interval(), Duration::from_secs(3600));
    }

    #[test]
    fn aggregate_lets_later_endpoints_overwrite_earlier_ones() {
        let merged = aggregate(vec![
            response(&[(1, "main", "aaa", 10), (7, "main", "old", 5)]),
            response(&[(7, "main", "new", 20), (2, "dev", "bbb", 30)]),
        ])
        .expect("aggregate");

        assert_eq!(merged.len(), 3);
        assert_eq!(merged[&7].branches[0].version, "new");
        assert_eq!(merged[&7].index_time_unix, 20);
        assert_eq!(merged[&2].branches[0].name, "dev");
    }

    #[test]
    fn aggregate_skips_rollout_errors_and_keeps_other_endpoints() {
        let merged = aggregate(vec![
            response(&[(1, "main", "aaa", 10)]),
            Err(Error::Transport(
                "dial tcp 10.0.0.1:6070: connect: connection refused".to_owned(),
            )),
            response(&[(2, "main", "bbb", 20)]),
        ])
        .expect("aggregate");

        assert_eq!(merged.keys().copied().collect::<Vec<_>>(), vec![1, 2]);
    }

    #[test]
    fn aggregate_fails_on_a_non_rollout_error() {
        let error = aggregate(vec![
            response(&[(1, "main", "aaa", 10)]),
            Err(Error::Status(Box::new(tonic::Status::invalid_argument(
                "index corrupt",
            )))),
        ])
        .expect_err("a non-rollout error aborts the reconciliation");

        assert_eq!(error, "index corrupt");
    }

    #[test]
    fn rollout_reasons_match_the_baseline() {
        assert_eq!(
            rollout_reason("dial tcp: lookup zoekt on 127.0.0.53:53: no such host"),
            Some("dns-not-found")
        );
        assert_eq!(
            rollout_reason("dial tcp 10.0.0.1:6070: connect: connection refused"),
            Some("dial-refused")
        );
        assert_eq!(
            rollout_reason("dial tcp 10.0.0.1:6070: i/o timeout: connect timed out"),
            Some("dial-timeout")
        );
        assert_eq!(
            rollout_reason("error reading a body from connection"),
            Some("read-failed")
        );
        assert_eq!(rollout_reason("index corrupt"), None);
    }

    #[test]
    fn cancellation_and_deadlines_are_not_rollout_errors() {
        assert!(!is_rollout_error(&Error::Canceled));
        assert!(!is_rollout_error(&Error::DeadlineExceeded));
        assert!(!is_rollout_error(&Error::InvalidEndpoint(
            "bad endpoint".to_owned()
        )));
    }
}
