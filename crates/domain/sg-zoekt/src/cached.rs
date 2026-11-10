#![forbid(unsafe_code)]

//! `cachedSearcher` (`internal/search/backend/cached.go`): caches `List`
//! results for the only query that is guaranteed to match every shard,
//! `&zoektquery.Const{Value: true}`.
//!
//! Differences from the Go baseline, all deliberate:
//!
//! * the clock and jitter source are injectable so behavioral diffs are
//!   deterministic;
//! * the cache is keyed by the `ListOptions` field's Go numeric value rather
//!   than by a `Hash`-able `ListOptions` (the public type stays untouched);
//! * a `nil` opts call shares the zero-value key with
//!   `ListOptions{Field: Repos}`, matching Go's `listCacheKey{}`;
//! * updates are serialized with a `tokio::sync::Mutex` held across the
//!   upstream call, reproducing Go's write lock, and the returned futures are
//!   `Send` so refreshes can be spawned.

use std::collections::HashMap;
use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::Mutex;

use crate::query::Query;
use crate::streamer::Streamer;
use crate::types::{ListOptions, ListResponse, RepoListField, SearchChunk, SearchOptions};

/// The `/4` TTL reduction applied after errors or crashes.
const ERROR_TTL_DIVISOR: u32 = 4;

/// Injectable time source (`cachedSearcher.now`).
pub trait Clock: Send + Sync + 'static {
    fn now(&self) -> Instant;
}

/// Wall-clock time (`time.Now`).
#[derive(Debug, Clone, Copy, Default)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> Instant {
        Instant::now()
    }
}

/// A monotonic clock whose reading is `base + offset`, for tests. Cloning
/// shares the offset, so a handle can drive a clock owned by a wrapper.
#[derive(Debug, Clone)]
pub struct ManualClock {
    base: Instant,
    offset_nanos: Arc<AtomicU64>,
}

impl ManualClock {
    pub fn new() -> Self {
        Self {
            base: Instant::now(),
            offset_nanos: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Set the elapsed offset since construction.
    pub fn set(&self, offset: Duration) {
        self.offset_nanos
            .store(offset.as_nanos() as u64, Ordering::SeqCst);
    }

    /// Advance the clock by `delta`.
    pub fn advance(&self, delta: Duration) {
        self.offset_nanos
            .fetch_add(delta.as_nanos() as u64, Ordering::SeqCst);
    }
}

impl Default for ManualClock {
    fn default() -> Self {
        Self::new()
    }
}

impl Clock for ManualClock {
    fn now(&self) -> Instant {
        self.base + Duration::from_nanos(self.offset_nanos.load(Ordering::SeqCst))
    }
}

/// Returns the jitter delta in nanoseconds: `randInterval` adds it to the TTL.
type JitterFn = Box<dyn Fn(Duration) -> i128 + Send + Sync>;

struct CacheValue<E> {
    list: Option<ListResponse>,
    err: Option<E>,
    ts: Instant,
    ttl: Duration,
}

struct Inner<S: Streamer, C: Clock> {
    streamer: S,
    ttl: Duration,
    clock: C,
    jitter: JitterFn,
    cache: Mutex<HashMap<u8, Arc<CacheValue<S::Error>>>>,
    pending: std::sync::atomic::AtomicUsize,
}

impl<S: Streamer, C: Clock> Inner<S, C> {
    /// `randInterval(ttl, 5s)` as an absolute nanosecond duration. `i128`
    /// keeps Go's signed-duration behavior when the jitter exceeds the TTL.
    fn interval_nanos(&self, ttl: Duration) -> i128 {
        ttl.as_nanos() as i128 + (self.jitter)(ttl)
    }

    /// `listCacheValue.stale`.
    fn stale(&self, value: &CacheValue<S::Error>) -> bool {
        let elapsed = self.clock.now().saturating_duration_since(value.ts);
        elapsed.as_nanos() as i128 >= self.interval_nanos(value.ttl)
    }

    /// `cachedSearcher.update`: caller must already hold an `Arc<Inner>` for
    /// spawning; the double check runs under the cache lock.
    async fn update(&self, query: &Query, key: u8, opts: ListOptions) -> Arc<CacheValue<S::Error>> {
        let mut cache = self.cache.lock().await;
        if let Some(value) = cache.get(&key) {
            if value.err.is_none() && !self.stale(value) {
                // Someone beat us to the update.
                return Arc::clone(value);
            }
        }

        let result = self.streamer.list(query, Some(opts)).await;
        let ts = self.clock.now();
        let (list, err) = match result {
            Ok(list) => (Some(list), None),
            Err(err) => (None, Some(err)),
        };

        // If we encountered an error or a crash, shorten how long we wait
        // before refreshing the cache.
        let crashes = list.as_ref().map(|list| list.crashes).unwrap_or(0);
        let mut ttl = self.ttl;
        if err.is_some() || crashes > 0 {
            ttl /= ERROR_TTL_DIVISOR;
        }

        let value = Arc::new(CacheValue { list, err, ts, ttl });
        cache.insert(key, Arc::clone(&value));
        value
    }
}

/// A snapshot of one cache entry, for differential testing of `ttl /= 4`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DebugCacheEntry {
    /// The Go numeric `ListOptions.Field` this entry is keyed by
    /// (`0` = Repos/nil, `2` = ReposMap, `3` = unknown).
    pub key: u8,
    pub ttl: Duration,
    pub has_error: bool,
    pub crashes: i64,
}

/// `cachedSearcher`: only `List` is cached.
pub struct CachedStreamer<S: Streamer, C: Clock = SystemClock> {
    inner: Arc<Inner<S, C>>,
}

impl<S: Streamer, C: Clock> Clone for CachedStreamer<S, C> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<S: Streamer> CachedStreamer<S, SystemClock> {
    /// `NewCachedSearcher`: wall clock and randomized jitter.
    pub fn new(ttl: Duration, streamer: S) -> Self {
        Self::with_clock(ttl, streamer, SystemClock)
    }
}

impl<S: Streamer, C: Clock> CachedStreamer<S, C> {
    pub fn with_clock(ttl: Duration, streamer: S, clock: C) -> Self {
        Self::with_clock_and_jitter(ttl, streamer, clock, default_jitter_delta)
    }

    pub fn with_clock_and_jitter(
        ttl: Duration,
        streamer: S,
        clock: C,
        jitter: impl Fn(Duration) -> i128 + Send + Sync + 'static,
    ) -> Self {
        Self {
            inner: Arc::new(Inner {
                streamer,
                ttl,
                clock,
                jitter: Box::new(jitter),
                cache: Mutex::new(HashMap::new()),
                pending: std::sync::atomic::AtomicUsize::new(0),
            }),
        }
    }

    /// `cachedSearcher.List`.
    pub async fn list(
        &self,
        query: &Query,
        opts: Option<ListOptions>,
    ) -> Result<ListResponse, S::Error> {
        if !is_true_query(query) {
            // Cache pass-through for anything that isn't "ListAll", minimal or
            // not.
            return self.inner.streamer.list(query, opts).await;
        }

        let key = cache_key(opts);
        let key_opts = cache_opts(opts);

        let cached = {
            let cache = self.inner.cache.lock().await;
            cache.get(&key).cloned()
        };

        let value = match cached {
            None => self.inner.update(query, key, key_opts).await,
            Some(value) if value.err.is_some() => self.inner.update(query, key, key_opts).await,
            Some(value) if self.inner.stale(&value) => {
                self.spawn_refresh(query, key, key_opts);
                value
            }
            Some(value) => value,
        };

        match &value.err {
            Some(err) => Err(err.clone()),
            None => Ok(value.list.clone().unwrap_or_default()),
        }
    }

    /// Call `update` directly, exercising its double check without the stale
    /// scheduling. Differential-testing aid.
    pub async fn debug_update(
        &self,
        query: &Query,
        opts: Option<ListOptions>,
    ) -> Result<ListResponse, S::Error> {
        let value = self
            .inner
            .update(query, cache_key(opts), cache_opts(opts))
            .await;
        match &value.err {
            Some(err) => Err(err.clone()),
            None => Ok(value.list.clone().unwrap_or_default()),
        }
    }

    /// Wait until all spawned refreshes have completed.
    pub async fn wait_for_refresh(&self) {
        while self.inner.pending.load(Ordering::SeqCst) != 0 {
            tokio::task::yield_now().await;
        }
    }

    /// Snapshot of the cache, used to verify the `ttl /= 4` behavior.
    pub async fn debug_cache_entries(&self) -> Vec<DebugCacheEntry> {
        let cache = self.inner.cache.lock().await;
        let mut entries: Vec<DebugCacheEntry> = cache
            .iter()
            .map(|(key, value)| DebugCacheEntry {
                key: *key,
                ttl: value.ttl,
                has_error: value.err.is_some(),
                crashes: value.list.as_ref().map(|list| list.crashes).unwrap_or(0),
            })
            .collect();
        entries.sort_by_key(|entry| entry.key);
        entries
    }

    fn spawn_refresh(&self, query: &Query, key: u8, opts: ListOptions) {
        self.inner.pending.fetch_add(1, Ordering::SeqCst);
        let inner = Arc::clone(&self.inner);
        let query = query.clone();
        tokio::spawn(async move {
            let _ = inner.update(&query, key, opts).await;
            inner.pending.fetch_sub(1, Ordering::SeqCst);
        });
    }
}

impl<S: Streamer, C: Clock> Streamer for CachedStreamer<S, C> {
    type Error = S::Error;

    fn list(
        &self,
        query: &Query,
        opts: Option<ListOptions>,
    ) -> impl Future<Output = Result<ListResponse, Self::Error>> + Send {
        CachedStreamer::list(self, query, opts)
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

/// `isTrueQuery`: only `*zoektquery.Const` with `Value == true` is cached.
pub fn is_true_query(query: &Query) -> bool {
    matches!(query, Query::Const(true))
}

/// Go `listCacheKey`: nil and the zero value share an entry.
fn cache_key(opts: Option<ListOptions>) -> u8 {
    match opts {
        None => 0,
        Some(opts) => match opts.field {
            RepoListField::Repos => 0,
            RepoListField::ReposMap => 2,
            RepoListField::Unknown => 3,
        },
    }
}

/// The options passed to the upstream `List`: Go always passes `&k.opts`,
/// even when the caller passed nil.
fn cache_opts(opts: Option<ListOptions>) -> ListOptions {
    opts.unwrap_or(ListOptions {
        field: RepoListField::Repos,
    })
}

/// `randInterval`'s default: `rand.Int63n(jitter) - jitter/2`.
fn default_jitter_delta(jitter: Duration) -> i128 {
    let nanos = jitter.as_nanos() as u64;
    if nanos == 0 {
        return 0;
    }
    let value = next_random() % nanos;
    value as i128 - i128::from(nanos) / 2
}

static RNG_STATE: AtomicU64 = AtomicU64::new(0x9e37_79b9_7f4a_7c15);

fn next_random() -> u64 {
    let mut current = RNG_STATE.load(Ordering::Relaxed);
    loop {
        let mut next = current;
        next ^= next << 13;
        next ^= next >> 7;
        next ^= next << 17;
        match RNG_STATE.compare_exchange_weak(current, next, Ordering::Relaxed, Ordering::Relaxed) {
            Ok(_) => return next,
            Err(observed) => current = observed,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_const_true_is_cached() {
        assert!(is_true_query(&Query::Const(true)));
        assert!(!is_true_query(&Query::Const(false)));
        assert!(!is_true_query(&Query::Repo("foo".to_owned())));
    }

    #[test]
    fn nil_and_repos_share_the_cache_key() {
        let repos = ListOptions {
            field: RepoListField::Repos,
        };
        let repos_map = ListOptions {
            field: RepoListField::ReposMap,
        };
        assert_eq!(cache_key(None), cache_key(Some(repos)));
        assert_ne!(cache_key(None), cache_key(Some(repos_map)));
    }

    #[test]
    fn default_jitter_stays_in_range() {
        let jitter = Duration::from_secs(5);
        for _ in 0..64 {
            let delta = default_jitter_delta(jitter);
            assert!(delta > -2_500_000_000);
            assert!(delta < 2_500_000_000);
        }
    }
}
