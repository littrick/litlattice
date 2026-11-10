#![forbid(unsafe_code)]

//! Zoekt streamer construction and per-endpoint dial caching
//! (`internal/search/backend/zoekt.go:16-100`).
//!
//! The baseline declares `ZoektDialer func(endpoint string) zoekt.Streamer`
//! and `NewCachedZoektDialer` wraps it with a mutex-guarded cache: the first
//! `Dial` for an endpoint calls the underlying dialer while holding the write
//! lock (so concurrent callers dial at most once), and the returned
//! `cachedStreamerCloser.Close` removes the endpoint from the cache before
//! closing the transport.
//!
//! Two Rust-specific adjustments:
//!
//! * [`crate::streamer::Streamer`] uses return-position `impl Trait` in trait
//!   methods and is therefore not dyn-compatible, so [`ZoektDialer`] uses an
//!   associated type instead of returning `Box<dyn Streamer>`.
//! * That trait has no `Close` method (the Go `zoekt.Streamer` embeds
//!   `Searcher`, which does). [`CloseableStreamer`] adds it without touching
//!   the shared `streamer` module.
//!
//! As in the baseline, `dial` is infallible: `ZoektDial` stores any connection
//! error inside the streamer (`dialErr`) rather than returning it.

use std::collections::HashMap;
use std::future::Future;
use std::sync::{Arc, RwLock, Weak};

use crate::horizontal::Sender;
use crate::query::Query;
use crate::streamer::Streamer;
use crate::types::{ListOptions, ListResponse, SearchChunk, SearchOptions, SearchResult};

/// `ZoektStreamFunc`: adapts a function into a [`Sender`].
pub struct ZoektStreamFunc<F>(F);

impl<F> ZoektStreamFunc<F> {
    pub fn new(func: F) -> Self {
        Self(func)
    }
}

impl<F: FnMut(SearchResult)> Sender for ZoektStreamFunc<F> {
    fn send(&mut self, event: SearchResult) {
        (self.0)(event);
    }
}

/// The `Close` half of the Go `zoekt.Streamer` (`Searcher.Close`).
pub trait CloseableStreamer: Streamer {
    /// `Close` releases the transport. Callers must not use the streamer
    /// afterwards.
    fn close(&self);
}

/// `ZoektDialer`: returns a [`CloseableStreamer`] for an endpoint.
///
/// The baseline type is `func(endpoint string) zoekt.Streamer`; the associated
/// type replaces the returned interface because `Streamer` is not
/// dyn-compatible.
pub trait ZoektDialer: Send + Sync + 'static {
    type Streamer: CloseableStreamer;

    fn dial(&self, endpoint: &str) -> Self::Streamer;
}

struct CloserInner<D: ZoektDialer> {
    /// Weak link back to the cache owner; closing must not keep it alive.
    dialer: Weak<CachedDialerInner<D>>,
    endpoint: String,
    streamer: D::Streamer,
}

/// `cachedStreamerCloser`: a cached streamer that evicts itself on close.
///
/// Cloning shares the same underlying streamer, mirroring the baseline's
/// pointer identity (see [`CachedStreamerCloser::ptr_eq`]).
pub struct CachedStreamerCloser<D: ZoektDialer> {
    inner: Arc<CloserInner<D>>,
}

impl<D: ZoektDialer> Clone for CachedStreamerCloser<D> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<D: ZoektDialer> CachedStreamerCloser<D> {
    /// True when both handles come from the same cached `Dial` result.
    pub fn ptr_eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.inner, &other.inner)
    }

    /// The endpoint this streamer was dialed for.
    pub fn endpoint(&self) -> &str {
        &self.inner.endpoint
    }

    /// `cachedStreamerCloser.Close`: drop the cache entry, then close the
    /// underlying streamer. The baseline deletes the endpoint
    /// unconditionally, so a stale closer can evict a newer entry; this is
    /// kept verbatim.
    pub fn close(&self) {
        if let Some(dialer) = self.inner.dialer.upgrade() {
            let mut streamers = dialer
                .streamers
                .write()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            streamers.remove(&self.inner.endpoint);
        }
        self.inner.streamer.close();
    }
}

impl<D: ZoektDialer> CloseableStreamer for CachedStreamerCloser<D> {
    fn close(&self) {
        CachedStreamerCloser::close(self);
    }
}

impl<D: ZoektDialer> Streamer for CachedStreamerCloser<D> {
    type Error = <D::Streamer as Streamer>::Error;

    fn list(
        &self,
        query: &Query,
        opts: Option<ListOptions>,
    ) -> impl Future<Output = Result<ListResponse, Self::Error>> + Send {
        self.inner.streamer.list(query, opts)
    }

    fn stream_search(
        &self,
        query: &Query,
        options: &SearchOptions,
        sender: &mut (dyn FnMut(SearchChunk) + Send),
    ) -> impl Future<Output = Result<(), Self::Error>> + Send {
        self.inner.streamer.stream_search(query, options, sender)
    }
}

struct CachedDialerInner<D: ZoektDialer> {
    streamers: RwLock<HashMap<String, CachedStreamerCloser<D>>>,
    dial: D,
}

/// `cachedZoektDialer`: caches one streamer per endpoint.
pub struct CachedZoektDialer<D: ZoektDialer> {
    inner: Arc<CachedDialerInner<D>>,
}

impl<D: ZoektDialer> Clone for CachedZoektDialer<D> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<D: ZoektDialer> CachedZoektDialer<D> {
    /// `NewCachedZoektDialer`: wrap `dial` with per-endpoint caching.
    pub fn new(dial: D) -> Self {
        Self {
            inner: Arc::new(CachedDialerInner {
                streamers: RwLock::new(HashMap::new()),
                dial,
            }),
        }
    }

    /// Number of cached endpoints (observation/differential-testing aid).
    pub fn cache_len(&self) -> usize {
        self.inner
            .streamers
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .len()
    }

    /// `cachedZoektDialer.Dial`.
    pub fn dial(&self, endpoint: &str) -> CachedStreamerCloser<D> {
        {
            let streamers = self
                .inner
                .streamers
                .read()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if let Some(streamer) = streamers.get(endpoint) {
                return streamer.clone();
            }
        }

        let mut streamers = self
            .inner
            .streamers
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        // Double check: another caller may have dialed while we waited for the
        // write lock. The dial itself happens under the write lock, so only
        // one call per endpoint runs concurrently.
        if let Some(streamer) = streamers.get(endpoint) {
            return streamer.clone();
        }

        let streamer = CachedStreamerCloser {
            inner: Arc::new(CloserInner {
                dialer: Arc::downgrade(&self.inner),
                endpoint: endpoint.to_owned(),
                streamer: self.inner.dial.dial(endpoint),
            }),
        };
        streamers.insert(endpoint.to_owned(), streamer.clone());
        streamer
    }
}

impl<D: ZoektDialer> ZoektDialer for CachedZoektDialer<D> {
    type Streamer = CachedStreamerCloser<D>;

    fn dial(&self, endpoint: &str) -> Self::Streamer {
        CachedZoektDialer::dial(self, endpoint)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::convert::Infallible;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Barrier, Mutex};
    use std::time::Duration;

    #[derive(Default)]
    struct FakeDialer {
        calls: Mutex<HashMap<String, usize>>,
        closed: AtomicUsize,
        delay: Duration,
    }

    impl FakeDialer {
        fn call_count(&self, endpoint: &str) -> usize {
            *self.calls.lock().unwrap().get(endpoint).unwrap_or(&0)
        }

        fn closed_count(&self) -> usize {
            self.closed.load(Ordering::SeqCst)
        }
    }

    struct FakeStreamer {
        dialer: Arc<FakeDialer>,
    }

    impl Streamer for FakeStreamer {
        type Error = Infallible;

        async fn list(
            &self,
            _query: &Query,
            _opts: Option<ListOptions>,
        ) -> Result<ListResponse, Self::Error> {
            Ok(ListResponse::default())
        }

        async fn stream_search(
            &self,
            _query: &Query,
            _options: &SearchOptions,
            _sender: &mut (dyn FnMut(SearchChunk) + Send),
        ) -> Result<(), Self::Error> {
            Ok(())
        }
    }

    impl CloseableStreamer for FakeStreamer {
        fn close(&self) {
            self.dialer.closed.fetch_add(1, Ordering::SeqCst);
        }
    }

    impl ZoektDialer for Arc<FakeDialer> {
        type Streamer = FakeStreamer;

        fn dial(&self, endpoint: &str) -> FakeStreamer {
            *self
                .calls
                .lock()
                .unwrap()
                .entry(endpoint.to_owned())
                .or_insert(0) += 1;
            if !self.delay.is_zero() {
                std::thread::sleep(self.delay);
            }
            FakeStreamer {
                dialer: Arc::clone(self),
            }
        }
    }

    fn dialer() -> (Arc<FakeDialer>, CachedZoektDialer<Arc<FakeDialer>>) {
        let fake = Arc::new(FakeDialer::default());
        (Arc::clone(&fake), CachedZoektDialer::new(fake))
    }

    #[test]
    fn second_dial_hits_the_cache_and_returns_the_same_streamer() {
        let (fake, dialer) = dialer();
        let first = dialer.dial("a");
        let second = dialer.dial("a");
        assert!(first.ptr_eq(&second));
        assert_eq!(fake.call_count("a"), 1);
        assert_eq!(dialer.cache_len(), 1);
    }

    #[test]
    fn different_endpoints_dial_independently() {
        let (fake, dialer) = dialer();
        let a = dialer.dial("a");
        let b = dialer.dial("b");
        assert!(!a.ptr_eq(&b));
        assert_eq!(fake.call_count("a"), 1);
        assert_eq!(fake.call_count("b"), 1);
        assert_eq!(dialer.cache_len(), 2);
    }

    #[test]
    fn close_evicts_and_allows_a_redial() {
        let (fake, dialer) = dialer();
        let first = dialer.dial("a");
        first.close();
        assert_eq!(fake.closed_count(), 1);
        assert_eq!(dialer.cache_len(), 0);

        let second = dialer.dial("a");
        assert!(!first.ptr_eq(&second));
        assert_eq!(fake.call_count("a"), 2);
        assert_eq!(dialer.cache_len(), 1);
    }

    #[test]
    fn close_of_one_endpoint_keeps_the_other_cached() {
        let (fake, dialer) = dialer();
        dialer.dial("a");
        let b = dialer.dial("b");
        b.close();

        let a = dialer.dial("a");
        let b_again = dialer.dial("b");
        assert_eq!(fake.call_count("a"), 1);
        assert_eq!(fake.call_count("b"), 2);
        assert_eq!(fake.closed_count(), 1);
        assert!(a.ptr_eq(&dialer.dial("a")));
        assert!(!b.ptr_eq(&b_again));
        assert_eq!(dialer.cache_len(), 2);
    }

    #[test]
    fn concurrent_dials_of_one_endpoint_call_the_dialer_once() {
        let fake = Arc::new(FakeDialer {
            delay: Duration::from_millis(10),
            ..FakeDialer::default()
        });
        let dialer = Arc::new(CachedZoektDialer::new(Arc::clone(&fake)));
        let threads = 8;
        let barrier = Arc::new(Barrier::new(threads));
        let mut handles = Vec::with_capacity(threads);

        for _ in 0..threads {
            let dialer = Arc::clone(&dialer);
            let barrier = Arc::clone(&barrier);
            handles.push(std::thread::spawn(move || {
                barrier.wait();
                dialer.dial("a")
            }));
        }

        let streamers: Vec<_> = handles
            .into_iter()
            .map(|handle| handle.join().unwrap())
            .collect();
        assert_eq!(fake.call_count("a"), 1);
        assert!(streamers
            .iter()
            .all(|streamer| streamer.ptr_eq(&streamers[0])));
    }

    #[test]
    fn zoekt_stream_func_forwards_events() {
        let mut events = Vec::new();
        {
            let mut sender = ZoektStreamFunc::new(|event: SearchResult| events.push(event));
            sender.send(SearchResult {
                stats: crate::types::Stats {
                    file_count: 3,
                    ..crate::types::Stats::default()
                },
                ..SearchResult::default()
            });
        }
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].stats.file_count, 3);
    }
}
