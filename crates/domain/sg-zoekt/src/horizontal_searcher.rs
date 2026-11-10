#![forbid(unsafe_code)]

//! `HorizontalSearcher` (`internal/search/backend/horizontal.go:33-260`): fan a
//! search out over every zoekt endpoint and aggregate the results.
//!
//! The baseline `HorizontalSearcher` is a `zoekt.Streamer` that:
//!
//! - discovers endpoints from a `Map` (`Endpoints()`), dialing and closing
//!   clients as the set changes ([`HorizontalSearcher::searchers`] /
//!   [`HorizontalSearcher::sync_searchers`]);
//! - runs `StreamSearch` concurrently on every client, deduplicating repository
//!   matches with [`crate::horizontal::Deduper`] and feeding the shared
//!   [`crate::aggregate::FlushSender`];
//! - treats rollout errors as a synthetic `crashEvent` instead of an error;
//! - aggregates `List` results and `Search` (`AggregateStreamSearch`).
//!
//! The pieces shared with M9.8gt (`AggregateStreamSearch` helpers, `Deduper`,
//! `crashEvent`, rollout classification) live in [`crate::horizontal`]; this
//! module only adds the searcher itself.
//!
//! ## Rust-specific design
//!
//! [`crate::streamer::Streamer`] is RPITIT (not dyn-compatible) and its
//! callback receives wire-level [`crate::types::SearchChunk`]s (files plus a
//! crash count). The Go `zoekt.Streamer.StreamSearch` callback receives full
//! `*zoekt.SearchResult`s. Because `HorizontalSearcher` needs the full result
//! for deduplication and flush aggregation, this module defines a small,
//! object-safe result-level [`Client`] trait (boxed futures) instead.
//!
//! Concurrency does not spawn tokio tasks: the clients may borrow their
//! surroundings, so [`join_all_ordered`] polls every future in a single task
//! and collects outputs in input order. Events are still delivered as they are
//! produced: the flush sender writes into an unbounded channel while a
//! forwarding future feeds the caller's [`crate::horizontal::Sender`].

use std::collections::{BTreeMap, HashSet};
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::task::Poll;

use crate::aggregate::{new_flush_collect_sender, FlushSender, ResultSender};
use crate::cached::{Clock, SystemClock};
use crate::horizontal::{crash_event, is_zoekt_rollout_error, Deduper, RolloutError, Sender};
use crate::query::Query;
use crate::types::{ListOptions, ListResponse, SearchOptions, SearchResult};

/// An endpoint or transport error with a rollout classification.
///
/// The baseline's `isZoektRolloutError` unwraps `*net.DNSError` /
/// `*net.OpError` with `errors.As`. Rust clients classify their own errors
/// here instead; the default is [`RolloutError::Other`] (not ignored).
pub trait ClientError: std::error::Error + Send + Sync + 'static {
    fn rollout_error(&self) -> RolloutError {
        RolloutError::Other
    }
}

/// The result-level `zoekt.Streamer` view `HorizontalSearcher` needs.
///
/// This is a deliberately smaller, object-safe mirror of the Go interface:
/// `StreamSearch` hands the callback complete [`SearchResult`] events (not
/// wire chunks), `List` returns a [`ListResponse`], and `Close` releases the
/// transport. Futures are boxed so endpoints can be stored and polled
/// together.
pub trait Client: Send + Sync + 'static {
    /// The transport error type.
    type Error: ClientError;

    /// `zoekt.Streamer.List`.
    fn list<'a>(
        &'a self,
        query: &'a Query,
        opts: Option<ListOptions>,
    ) -> Pin<Box<dyn Future<Output = Result<ListResponse, Self::Error>> + Send + 'a>>;

    /// `zoekt.Streamer.StreamSearch`.
    ///
    /// `options` is `None` when the caller passed `nil` in the baseline.
    fn stream_search<'a>(
        &'a self,
        query: &'a Query,
        options: Option<&'a SearchOptions>,
        sender: &'a mut (dyn FnMut(SearchResult) + Send),
    ) -> Pin<Box<dyn Future<Output = Result<(), Self::Error>> + Send + 'a>>;

    /// `zoekt.Streamer.Close`.
    fn close(&self);
}

/// Aggregates searches over `Map`, managing connections as endpoints change.
pub struct HorizontalSearcher<C: Client> {
    /// `Map.Endpoints`: the endpoints to dial over time.
    endpoints: Box<dyn Fn() -> Result<Vec<String>, C::Error> + Send + Sync>,
    /// `Dial`: build a client for an endpoint (infallible in the baseline too).
    dial: Box<dyn Fn(&str) -> C + Send + Sync>,
    /// `addr -> client`. Held behind an [`Arc`] so the slow path can replace
    /// the whole map without invalidating in-flight searches (mirroring Go's
    /// whole-map replacement); entries are [`Arc`]s so clients can be reused
    /// across replacements without moving them out of a shared map.
    clients: Mutex<Arc<BTreeMap<String, Arc<C>>>>,
    /// `conf.RankingMaxQueueSizeBytes()` (default `-1`).
    max_queue_size_bytes: i64,
}

impl<C: Client> HorizontalSearcher<C> {
    /// Build a searcher over `endpoints` (the baseline `Map`) with `dial`.
    pub fn new(
        endpoints: impl Fn() -> Result<Vec<String>, C::Error> + Send + Sync + 'static,
        dial: impl Fn(&str) -> C + Send + Sync + 'static,
    ) -> Self {
        Self {
            endpoints: Box::new(endpoints),
            dial: Box::new(dial),
            clients: Mutex::new(Arc::new(BTreeMap::new())),
            max_queue_size_bytes: -1,
        }
    }

    /// Override `conf.RankingMaxQueueSizeBytes()`.
    pub fn with_max_queue_size_bytes(mut self, max_queue_size_bytes: i64) -> Self {
        self.max_queue_size_bytes = max_queue_size_bytes;
        self
    }

    /// `conf.RankingMaxQueueSizeBytes()` used for the flush collector.
    pub fn max_queue_size_bytes(&self) -> i64 {
        self.max_queue_size_bytes
    }

    /// `(*HorizontalSearcher).StreamSearch`.
    ///
    /// Runs every client's `stream_search` concurrently. Each event is
    /// deduplicated against the other endpoints (under the same mutex the
    /// baseline uses) before being handed to the flush sender. A rollout error
    /// becomes a [`crash_event`]; any other error is sent back after
    /// `send_done`. The first error (in endpoint order) is returned.
    pub async fn stream_search<S>(
        &self,
        query: &Query,
        options: Option<&SearchOptions>,
        sender: &mut S,
    ) -> Result<(), C::Error>
    where
        S: Sender + Send,
    {
        let clients = self.searchers()?;
        let endpoints: Vec<String> = clients.keys().cloned().collect();

        // Bridge the caller's borrowed sender to the `'static` `ResultSender`
        // the flush collector needs. Events produced by the (concurrent)
        // clients are forwarded as they arrive; `drive` closes the channel
        // after flushing so the forwarder can terminate.
        let (event_sender, mut event_receiver) =
            tokio::sync::mpsc::unbounded_channel::<SearchResult>();
        let flush: Arc<dyn FlushSender> = Arc::from(new_flush_collect_sender(
            options.cloned(),
            &endpoints,
            self.max_queue_size_bytes,
            Arc::new(ChannelResultSender {
                sender: event_sender,
            }),
        ));
        let deduper = Arc::new(Mutex::new(Deduper::new()));

        let futures: Vec<_> = clients
            .iter()
            .map(|(endpoint, client)| {
                let endpoint = endpoint.clone();
                let client = Arc::clone(client);
                let flush = Arc::clone(&flush);
                let deduper = Arc::clone(&deduper);
                async move {
                    let callback_endpoint = endpoint.clone();
                    let callback_flush = Arc::clone(&flush);
                    let mut callback = move |mut result: SearchResult| {
                        {
                            let mut deduper =
                                deduper.lock().unwrap_or_else(PoisonError::into_inner);
                            deduper.dedup(&callback_endpoint, &mut result.files);
                        }
                        callback_flush.send(&callback_endpoint, result);
                    };
                    match client.stream_search(query, options, &mut callback).await {
                        Ok(()) => {
                            flush.send_done(&endpoint);
                            Ok(())
                        }
                        Err(error) if is_zoekt_rollout_error(&error.rollout_error()) => {
                            // The baseline sends a crash event and *not* a
                            // `SendDone`, treating the endpoint as successful.
                            flush.send(&endpoint, crash_event());
                            Ok(())
                        }
                        Err(error) => {
                            flush.send_done(&endpoint);
                            Err(error)
                        }
                    }
                }
            })
            .collect();

        let forward = async move {
            while let Some(event) = event_receiver.recv().await {
                sender.send(event);
            }
        };
        let drive = async move {
            let outcomes = join_all_ordered(futures).await;
            flush.flush();
            let first_error = outcomes.into_iter().find_map(|outcome| outcome.err());
            // Drop the last flush-sender handle so the channel closes and
            // `forward` finishes after draining.
            drop(flush);
            first_error
        };

        let (first_error, ()) = tokio::join!(drive, forward);
        match first_error {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }

    /// `(*HorizontalSearcher).Search`: `AggregateStreamSearch`.
    ///
    /// The M9.8gt [`crate::horizontal::aggregate_stream_search`] drives a
    /// *synchronous* callback, while the transport is async, so the same
    /// aggregation (append files, add stats, stamp `Duration` on success) is
    /// performed inline here.
    pub async fn search(
        &self,
        query: &Query,
        options: Option<&SearchOptions>,
    ) -> Result<SearchResult, C::Error> {
        let start = SystemClock.now();
        let mut aggregate = SearchResult::default();
        {
            let mut sender = |event: SearchResult| {
                aggregate.stats.add(&event.stats);
                aggregate.files.extend(event.files);
            };
            self.stream_search(query, options, &mut sender).await?;
        }
        aggregate.stats.duration = SystemClock.now().saturating_duration_since(start);
        Ok(aggregate)
    }

    /// `(*HorizontalSearcher).List`.
    ///
    /// Aggregates `Repos`, `Crashes` and `ReposMap` across every client. A
    /// rollout error counts as a crash and continues; any other error is
    /// returned. The baseline then sets `Stats.Repos = len(Repos) +
    /// len(ReposMap)`; [`crate::types::ListResponse`] has no `Stats` field, so
    /// callers derive that count from the returned lengths (the intermediate
    /// `RepoStats.Add` is not observable in the final result).
    pub async fn list(
        &self,
        query: &Query,
        options: Option<ListOptions>,
    ) -> Result<ListResponse, C::Error> {
        let clients = self.searchers()?;
        let futures: Vec<_> = clients
            .values()
            .map(|client| {
                let client = Arc::clone(client);
                async move { client.list(query, options).await }
            })
            .collect();

        let mut aggregate = ListResponse::default();
        for result in join_all_ordered(futures).await {
            match result {
                Err(error) if is_zoekt_rollout_error(&error.rollout_error()) => {
                    aggregate.crashes += 1;
                }
                Err(error) => return Err(error),
                Ok(list) => {
                    aggregate.repos.extend(list.repos);
                    aggregate.crashes += list.crashes;
                    aggregate.repos_map.extend(list.repos_map);
                }
            }
        }
        Ok(aggregate)
    }

    /// `(*HorizontalSearcher).Close`: close every client and clear the map.
    pub fn close(&self) {
        let clients = {
            let mut guard = lock(&self.clients);
            std::mem::replace(&mut *guard, Arc::new(BTreeMap::new()))
        };
        for client in clients.values() {
            client.close();
        }
    }

    /// Return the clients to aggregate over.
    ///
    /// Fast path: if `Endpoints()` matches the current key set, reuse the map
    /// as-is. Otherwise fall through to [`Self::sync_searchers`]. Public so
    /// callers/tests can force a synchronization without running a search.
    pub fn searchers(&self) -> Result<Arc<BTreeMap<String, Arc<C>>>, C::Error> {
        let endpoints = (self.endpoints)()?;
        let clients = lock(&self.clients).clone();
        if equal_keys(&clients, &endpoints) {
            return Ok(clients);
        }
        self.sync_searchers()
    }

    /// Slow path of [`Self::searchers`]: close removed clients, reuse kept
    /// ones and dial new endpoints, then replace the map.
    pub fn sync_searchers(&self) -> Result<Arc<BTreeMap<String, Arc<C>>>, C::Error> {
        let mut guard = lock(&self.clients);

        // Double check: someone may have updated while we waited for the lock.
        let endpoints = (self.endpoints)()?;
        if equal_keys(&guard, &endpoints) {
            return Ok(Arc::clone(&guard));
        }

        let set: HashSet<&str> = endpoints.iter().map(String::as_str).collect();

        // Disconnect first.
        let previous = Arc::clone(&guard);
        for (address, client) in previous.iter() {
            if !set.contains(address.as_str()) {
                client.close();
            }
        }

        // Reuse clients where possible, dial the rest. A new map avoids read
        // conflicts with in-flight searches holding the previous map.
        let mut clients = BTreeMap::new();
        for address in &endpoints {
            let client = match previous.get(address) {
                Some(client) => Arc::clone(client),
                None => Arc::new((self.dial)(address)),
            };
            clients.insert(address.clone(), client);
        }

        let replacement = Arc::new(clients);
        *guard = Arc::clone(&replacement);
        Ok(replacement)
    }
}

impl<C: Client> fmt::Display for HorizontalSearcher<C> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let clients = lock(&self.clients).clone();
        formatter.write_str("HorizontalSearcher{[")?;
        for (index, address) in clients.keys().enumerate() {
            if index != 0 {
                formatter.write_str(" ")?;
            }
            formatter.write_str(address)?;
        }
        formatter.write_str("]}")
    }
}

/// The subset of `equalKeys`: same length and every endpoint is a key.
fn equal_keys<C>(clients: &BTreeMap<String, Arc<C>>, endpoints: &[String]) -> bool {
    clients.len() == endpoints.len()
        && endpoints
            .iter()
            .all(|endpoint| clients.contains_key(endpoint))
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Adapter from the borrowed caller sender to the `'static` [`ResultSender`]
/// the aggregate module requires. Unbounded, so `send` never blocks or drops.
struct ChannelResultSender {
    sender: tokio::sync::mpsc::UnboundedSender<SearchResult>,
}

impl ResultSender for ChannelResultSender {
    fn send(&self, result: SearchResult) {
        // The receiver lives until `drive` drops the flush sender, which
        // happens after every client has finished; a closed channel here is
        // unreachable in practice.
        let _ = self.sender.send(result);
    }
}

/// Join `futures`, polling them concurrently in this task and returning their
/// outputs in input order.
///
/// This is the `futures::future::join_all` shape without the `futures`
/// dependency. Boxed futures are `!Unpin`; pinning each one lets the whole
/// `Vec` move while the futures stay in place. When a future completes, its
/// slot is dropped (releasing captured senders), so the event channel can
/// close once all endpoints are done.
async fn join_all_ordered<F: Future>(futures: Vec<F>) -> Vec<F::Output> {
    let mut slots: Vec<Option<Pin<Box<F>>>> = futures
        .into_iter()
        .map(|future| Some(Box::pin(future)))
        .collect();
    let mut outputs: Vec<Option<F::Output>> =
        std::iter::repeat_with(|| None).take(slots.len()).collect();

    std::future::poll_fn(|context| {
        let mut pending = false;
        for (slot, output) in slots.iter_mut().zip(outputs.iter_mut()) {
            let poll = {
                let Some(future) = slot.as_mut() else {
                    continue;
                };
                future.as_mut().poll(context)
            };
            match poll {
                Poll::Ready(value) => {
                    *output = Some(value);
                    *slot = None;
                }
                Poll::Pending => pending = true,
            }
        }
        if pending {
            Poll::Pending
        } else {
            Poll::Ready(())
        }
    })
    .await;

    outputs
        .into_iter()
        .map(|output| output.expect("every future was polled to completion"))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Mutex as StdMutex;

    use crate::types::{FileMatch, RepoListEntry, Repository, Stats};

    #[derive(Debug, Clone)]
    struct FakeError {
        message: String,
        rollout: RolloutError,
    }

    impl FakeError {
        fn plain(message: &str) -> Self {
            Self {
                message: message.to_owned(),
                rollout: RolloutError::Other,
            }
        }

        fn rollout(rollout: RolloutError) -> Self {
            Self {
                message: "rollout".to_owned(),
                rollout,
            }
        }
    }

    impl fmt::Display for FakeError {
        fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str(&self.message)
        }
    }

    impl std::error::Error for FakeError {}

    impl ClientError for FakeError {
        fn rollout_error(&self) -> RolloutError {
            self.rollout
        }
    }

    #[derive(Debug, Default)]
    struct FakeState {
        dials: StdMutex<HashMap<String, usize>>,
        closed: AtomicUsize,
    }

    #[derive(Debug)]
    struct FakeClient {
        endpoint: String,
        events: Vec<SearchResult>,
        list: ListResponse,
        error: Option<FakeError>,
        state: Arc<FakeState>,
    }

    impl FakeClient {
        fn new(endpoint: &str, state: Arc<FakeState>) -> Self {
            Self {
                endpoint: endpoint.to_owned(),
                events: Vec::new(),
                list: ListResponse::default(),
                error: None,
                state,
            }
        }

        fn with_events(mut self, events: Vec<SearchResult>) -> Self {
            self.events = events;
            self
        }

        fn with_list(mut self, list: ListResponse) -> Self {
            self.list = list;
            self
        }

        fn with_error(mut self, error: FakeError) -> Self {
            self.error = Some(error);
            self
        }
    }

    impl Client for FakeClient {
        type Error = FakeError;

        fn list<'a>(
            &'a self,
            _query: &'a Query,
            _opts: Option<ListOptions>,
        ) -> Pin<Box<dyn Future<Output = Result<ListResponse, FakeError>> + Send + 'a>> {
            Box::pin(async move {
                match &self.error {
                    Some(error) => Err(error.clone()),
                    None => Ok(self.list.clone()),
                }
            })
        }

        fn stream_search<'a>(
            &'a self,
            _query: &'a Query,
            _options: Option<&'a SearchOptions>,
            sender: &'a mut (dyn FnMut(SearchResult) + Send),
        ) -> Pin<Box<dyn Future<Output = Result<(), FakeError>> + Send + 'a>> {
            Box::pin(async move {
                if let Some(error) = &self.error {
                    return Err(error.clone());
                }
                for event in &self.events {
                    sender(event.clone());
                }
                Ok(())
            })
        }

        fn close(&self) {
            self.state.closed.fetch_add(1, Ordering::SeqCst);
        }
    }

    use std::collections::HashMap;

    fn file(repository: &str, name: &str) -> FileMatch {
        FileMatch {
            repository: repository.to_owned(),
            file_name: name.as_bytes().to_vec(),
            ..FileMatch::default()
        }
    }

    fn event(files: Vec<FileMatch>, crashes: i64) -> SearchResult {
        SearchResult {
            files,
            stats: Stats {
                crashes,
                ..Stats::default()
            },
            ..SearchResult::default()
        }
    }

    fn marker() -> HorizontalSearcher<FakeClient> {
        // A searcher whose `dial` panics: tests that use it install clients
        // directly.
        HorizontalSearcher {
            endpoints: Box::new(|| Ok(Vec::new())),
            dial: Box::new(|_: &str| -> FakeClient { panic!("unexpected dial") }),
            clients: Mutex::new(Arc::new(BTreeMap::new())),
            max_queue_size_bytes: -1,
        }
    }

    fn searcher_with(clients: Vec<FakeClient>) -> HorizontalSearcher<FakeClient> {
        let endpoints: Vec<String> = clients
            .iter()
            .map(|client| client.endpoint.clone())
            .collect();
        let map: BTreeMap<String, Arc<FakeClient>> = clients
            .into_iter()
            .map(|client| (client.endpoint.clone(), Arc::new(client)))
            .collect();
        HorizontalSearcher {
            endpoints: Box::new(move || Ok(endpoints.clone())),
            dial: Box::new(|_: &str| -> FakeClient { panic!("unexpected dial") }),
            clients: Mutex::new(Arc::new(map)),
            max_queue_size_bytes: -1,
        }
    }

    fn state_client(endpoint: &str, state: &Arc<FakeState>) -> FakeClient {
        FakeClient::new(endpoint, Arc::clone(state))
    }

    #[test]
    fn display_lists_endpoints_in_sorted_order() {
        let state = Arc::new(FakeState::default());
        let searcher = searcher_with(vec![state_client("b", &state), state_client("a", &state)]);
        assert_eq!(searcher.to_string(), "HorizontalSearcher{[a b]}");
    }

    #[test]
    fn display_is_empty_without_clients() {
        assert_eq!(marker().to_string(), "HorizontalSearcher{[]}");
    }

    #[test]
    fn searchers_fast_path_reuses_clients() {
        let endpoints = Arc::new(StdMutex::new(vec!["a".to_owned(), "b".to_owned()]));
        let state = Arc::new(FakeState::default());
        let source = Arc::clone(&endpoints);
        let dial_state = Arc::clone(&state);
        let searcher = HorizontalSearcher::new(
            move || Ok(source.lock().unwrap().clone()),
            move |endpoint: &str| {
                *dial_state
                    .dials
                    .lock()
                    .unwrap()
                    .entry(endpoint.to_owned())
                    .or_insert(0) += 1;
                state_client(endpoint, &dial_state)
            },
        );

        searcher.searchers().unwrap();
        searcher.searchers().unwrap();
        let dials = state.dials.lock().unwrap();
        assert_eq!(dials.get("a"), Some(&1));
        assert_eq!(dials.get("b"), Some(&1));
        assert_eq!(state.closed.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn sync_closes_removed_reuses_kept_and_dials_new() {
        let endpoints = Arc::new(StdMutex::new(vec!["a".to_owned()]));
        let state = Arc::new(FakeState::default());
        let source = Arc::clone(&endpoints);
        let dial_state = Arc::clone(&state);
        let searcher = HorizontalSearcher::new(
            move || Ok(source.lock().unwrap().clone()),
            move |endpoint: &str| {
                *dial_state
                    .dials
                    .lock()
                    .unwrap()
                    .entry(endpoint.to_owned())
                    .or_insert(0) += 1;
                state_client(endpoint, &dial_state)
            },
        );

        searcher.searchers().unwrap();

        // Add b: reuse a, dial b.
        endpoints.lock().unwrap().push("b".to_owned());
        searcher.searchers().unwrap();
        {
            let dials = state.dials.lock().unwrap();
            assert_eq!(dials.get("a"), Some(&1));
            assert_eq!(dials.get("b"), Some(&1));
        }

        // Lose a: close it, keep b.
        *endpoints.lock().unwrap() = vec!["b".to_owned()];
        searcher.searchers().unwrap();
        assert_eq!(state.closed.load(Ordering::SeqCst), 1);
        {
            let dials = state.dials.lock().unwrap();
            assert_eq!(dials.get("a"), Some(&1));
            assert_eq!(dials.get("b"), Some(&1));
        }
    }

    #[test]
    fn searchers_propagates_endpoints_error() {
        let searcher = HorizontalSearcher {
            endpoints: Box::new(|| Err(FakeError::plain("boom"))),
            dial: Box::new(|_: &str| -> FakeClient { panic!("unexpected dial") }),
            clients: Mutex::new(Arc::new(BTreeMap::new())),
            max_queue_size_bytes: -1,
        };
        assert_eq!(searcher.searchers().unwrap_err().to_string(), "boom");
    }

    #[test]
    fn close_closes_every_client_and_clears_the_map() {
        let state = Arc::new(FakeState::default());
        let searcher = searcher_with(vec![state_client("a", &state), state_client("b", &state)]);
        searcher.close();
        assert_eq!(state.closed.load(Ordering::SeqCst), 2);
        assert_eq!(searcher.to_string(), "HorizontalSearcher{[]}");
    }

    #[tokio::test]
    async fn stream_search_dedups_across_endpoints() {
        let state = Arc::new(FakeState::default());
        let searcher = searcher_with(vec![
            state_client("a", &state).with_events(vec![event(vec![file("r1", "a.go")], 0)]),
            state_client("b", &state).with_events(vec![event(vec![file("r1", "a.go")], 0)]),
        ]);

        let mut events = Vec::new();
        let mut sender = |event: SearchResult| events.push(event);
        searcher
            .stream_search(&Query::Const(true), None, &mut sender)
            .await
            .unwrap();

        let mut files: Vec<String> = events
            .iter()
            .flat_map(|event| {
                event.files.iter().map(|file| {
                    format!(
                        "{}:{}",
                        file.repository,
                        String::from_utf8_lossy(&file.file_name)
                    )
                })
            })
            .collect();
        files.sort();
        assert_eq!(files, ["r1:a.go"]);
        assert_eq!(events.len(), 2);
    }

    #[tokio::test]
    async fn stream_search_sends_a_crash_event_for_rollout_errors() {
        let state = Arc::new(FakeState::default());
        let searcher = searcher_with(vec![
            state_client("a", &state).with_error(FakeError::rollout(RolloutError::ReadFailed))
        ]);

        let mut events = Vec::new();
        let mut sender = |event: SearchResult| events.push(event);
        searcher
            .stream_search(&Query::Const(true), None, &mut sender)
            .await
            .unwrap();

        assert_eq!(events.len(), 1);
        assert_eq!(events[0].stats.crashes, 1);
        assert!(events[0].files.is_empty());
    }

    #[tokio::test]
    async fn stream_search_propagates_plain_errors() {
        let state = Arc::new(FakeState::default());
        let searcher = searcher_with(vec![
            state_client("a", &state).with_error(FakeError::plain("boom"))
        ]);

        let mut events = Vec::new();
        let mut sender = |event: SearchResult| events.push(event);
        let error = searcher
            .stream_search(&Query::Const(true), None, &mut sender)
            .await
            .unwrap_err();
        assert_eq!(error.to_string(), "boom");
        assert!(events.is_empty());
    }

    #[tokio::test]
    async fn list_aggregates_repos_crashes_and_repos_map() {
        let state = Arc::new(FakeState::default());
        let mut first = ListResponse {
            repos: vec![RepoListEntry {
                repository: Repository {
                    name: "r1".to_owned(),
                    ..Repository::default()
                },
                ..RepoListEntry::default()
            }],
            crashes: 1,
            ..ListResponse::default()
        };
        first.repos_map.insert(1, Default::default());
        let mut second = ListResponse {
            repos: vec![RepoListEntry {
                repository: Repository {
                    name: "r2".to_owned(),
                    ..Repository::default()
                },
                ..RepoListEntry::default()
            }],
            crashes: 2,
            ..ListResponse::default()
        };
        second.repos_map.insert(2, Default::default());

        let searcher = searcher_with(vec![
            state_client("a", &state).with_list(first),
            state_client("b", &state).with_list(second),
        ]);

        let list = searcher.list(&Query::Const(true), None).await.unwrap();
        let mut names: Vec<&str> = list
            .repos
            .iter()
            .map(|entry| entry.repository.name.as_str())
            .collect();
        names.sort();
        assert_eq!(names, ["r1", "r2"]);
        assert_eq!(list.crashes, 3);
        assert_eq!(list.repos_map.keys().copied().collect::<Vec<_>>(), [1, 2]);
        assert_eq!(list.repos.len() + list.repos_map.len(), 4);
    }

    #[tokio::test]
    async fn list_counts_rollout_errors_as_crashes() {
        let state = Arc::new(FakeState::default());
        let searcher = searcher_with(vec![
            state_client("a", &state).with_error(FakeError::rollout(RolloutError::DialRefused)),
            state_client("b", &state).with_list(ListResponse::default()),
        ]);
        let list = searcher.list(&Query::Const(true), None).await.unwrap();
        assert_eq!(list.crashes, 1);
    }

    #[tokio::test]
    async fn list_propagates_plain_errors() {
        let state = Arc::new(FakeState::default());
        let searcher = searcher_with(vec![
            state_client("a", &state).with_error(FakeError::plain("boom"))
        ]);
        let error = searcher.list(&Query::Const(true), None).await.unwrap_err();
        assert_eq!(error.to_string(), "boom");
    }

    #[tokio::test]
    async fn search_aggregates_files_and_stats() {
        let state = Arc::new(FakeState::default());
        let mut first = event(vec![file("r1", "a.go")], 0);
        first.stats.match_count = 1;
        let mut second = event(vec![file("r2", "b.go")], 0);
        second.stats.match_count = 2;

        let searcher = searcher_with(vec![
            state_client("a", &state).with_events(vec![first]),
            state_client("b", &state).with_events(vec![second]),
        ]);

        let result = searcher.search(&Query::Const(true), None).await.unwrap();
        assert_eq!(result.files.len(), 2);
        assert_eq!(result.stats.match_count, 3);
    }

    #[test]
    fn max_queue_size_defaults_to_minus_one() {
        assert_eq!(marker().max_queue_size_bytes(), -1);
    }
}
