#![forbid(unsafe_code)]

//! `cmd/frontend/graphqlbackend/rate_limit.go:409-487` and the
//! `github.com/throttled/throttled/v2 v2.12.0` GCRA rate limiter plus its
//! Redis store (`store/redigostore`).
//!
//! The limiter is disabled by default: `BasicLimitWatcher::update_from_config`
//! receives `conf.Get().ExperimentalFeatures.RateLimitAnonymous` and only
//! enables itself for a positive hourly limit. Enabled requests are limited
//! only when they are anonymous, carry no `?<requestName>` query string
//! (`requestName == "unknown"`) and are not from a browser
//! (`requestSource == "other"`), always with key `basic` and quantity `1`.
//!
//! Time and TTLs are i64 nanoseconds, matching Go's `time.Duration`; the
//! arithmetic uses wrapping operations because `time.Duration` overflow is
//! defined as two's-complement wrap-around.

use std::sync::{Arc, RwLock};

use async_trait::async_trait;

use crate::graphql_api::{LimiterArgs, RateLimitOutcome};
use crate::search_source::SOURCE_OTHER;

/// `BasicLimiter.RateLimit`'s fixed key (`rate_limit.go:484`).
pub const BASIC_KEY: &str = "basic";

/// `maxCASAttempts` (`throttled/rate.go:12`).
const MAX_CAS_ATTEMPTS: i32 = 10;

/// `redigostore.NewCtx(pool, "gql:rl:", 0)` (`serve_cmd.go:441`). The
/// [`KeyValue`](sg_redis::KeyValue) prefix helper appends the trailing colon.
pub const REDIS_PREFIX: &str = "gql:rl";

const NANOS_PER_SECOND: i64 = 1_000_000_000;
const NANOS_PER_HOUR: i64 = 3_600 * NANOS_PER_SECOND;

/// `throttled.GCRAStoreCtx` with the package's two operations returning
/// `Err(String)` (the message is only logged, like the baseline's raw error).
#[async_trait]
pub trait GcraStore: Send + Sync {
    /// `GetWithTime`: `(tat, now)` in nanoseconds; `tat == -1` when the key is
    /// absent.
    async fn get_with_time(&self, key: &str) -> Result<(i64, i64), String>;

    /// `SetIfNotExistsWithTTL`.
    async fn set_if_not_exists_with_ttl(
        &self,
        key: &str,
        value: i64,
        ttl_ns: i64,
    ) -> Result<bool, String>;

    /// `CompareAndSwapWithTTL`; a missing key is `Ok(false)`.
    async fn compare_and_swap_with_ttl(
        &self,
        key: &str,
        old: i64,
        new: i64,
        ttl_ns: i64,
    ) -> Result<bool, String>;
}

/// `throttled.RateLimitResult`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RateLimitResult {
    pub limit: i64,
    pub remaining: i64,
    pub reset_after_ns: i64,
    /// `-1` until the limit is exceeded (`rate.go:197`).
    pub retry_after_ns: i64,
}

/// A store or CAS-exhaustion failure. The baseline's `RateLimitCtx` still
/// returns its partial result alongside the error, so tests can compare it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RateLimitError {
    pub message: String,
    pub result: RateLimitResult,
}

impl std::fmt::Display for RateLimitError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for RateLimitError {}

/// `throttled.GCRARateLimiterCtx`.
pub struct GcraRateLimiter {
    limit: i64,
    delay_variation_tolerance: i64,
    emission_interval: i64,
    store: Arc<dyn GcraStore>,
    max_cas_attempts: i32,
}

impl GcraRateLimiter {
    /// `NewGCRARateLimiterCtx` (`rate.go:150-171`). Only the two validation
    /// errors are produced; the baseline's `%#v` quota rendering is not
    /// reproduced because the message is only logged.
    pub fn new(
        store: Arc<dyn GcraStore>,
        emission_interval_ns: i64,
        max_burst: i64,
    ) -> Result<Self, String> {
        if max_burst < 0 {
            return Err("MaxBurst must be greater than zero".to_owned());
        }
        if emission_interval_ns <= 0 {
            return Err("MaxRate must be greater than zero".to_owned());
        }
        Ok(Self {
            limit: max_burst.wrapping_add(1),
            delay_variation_tolerance: emission_interval_ns.wrapping_mul(max_burst.wrapping_add(1)),
            emission_interval: emission_interval_ns,
            store,
            max_cas_attempts: MAX_CAS_ATTEMPTS,
        })
    }

    /// `BasicLimitWatcher.updateFromConfig`'s quota:
    /// `MaxRate: PerHour(limit)` (integer nanosecond division),
    /// `MaxBurst: int(float64(limit) * 0.2)`.
    pub fn from_hourly_limit(store: Arc<dyn GcraStore>, limit: i64) -> Result<Self, String> {
        let period = NANOS_PER_HOUR / limit;
        let max_burst = (limit as f64 * 0.2) as i64;
        Self::new(store, period, max_burst)
    }

    /// `GCRARateLimiterCtx.RateLimitCtx` (`rate.go:183-274`).
    pub async fn rate_limit(
        &self,
        key: &str,
        quantity: i64,
    ) -> Result<(bool, RateLimitResult), RateLimitError> {
        let mut result = RateLimitResult {
            limit: self.limit,
            retry_after_ns: -1,
            ..RateLimitResult::default()
        };
        let mut limited = false;
        // Go declares `ttl` outside the loop; a limited request with
        // `increment > delayVariationTolerance` leaves it zero (`rate.go:186,215-218`).
        let mut ttl = 0i64;
        let mut i = 0i32;
        loop {
            let (tat_val, now) = self
                .store
                .get_with_time(key)
                .await
                .map_err(|message| RateLimitError { message, result })?;
            let tat = if tat_val == -1 { now } else { tat_val };

            let increment = quantity.wrapping_mul(self.emission_interval);
            let new_tat = if now > tat {
                now.wrapping_add(increment)
            } else {
                tat.wrapping_add(increment)
            };

            // Block the request if the next permitted time is in the future.
            let allow_at = new_tat.wrapping_sub(self.delay_variation_tolerance);
            let diff = now.wrapping_sub(allow_at);
            if diff < 0 {
                if increment <= self.delay_variation_tolerance {
                    result.retry_after_ns = diff.wrapping_neg();
                    ttl = tat.wrapping_sub(now);
                }
                limited = true;
                break;
            }

            ttl = new_tat.wrapping_sub(now);
            let updated = if tat_val == -1 {
                self.store
                    .set_if_not_exists_with_ttl(key, new_tat, ttl)
                    .await
                    .map_err(|message| RateLimitError { message, result })?
            } else {
                self.store
                    .compare_and_swap_with_ttl(key, tat_val, new_tat, ttl)
                    .await
                    .map_err(|message| RateLimitError { message, result })?
            };
            if updated {
                break;
            }

            i += 1;
            if i >= self.max_cas_attempts {
                return Err(RateLimitError {
                    message: format!(
                        "Failed to store updated rate limit data for key {key} after {i} attempts"
                    ),
                    result,
                });
            }
        }

        // `rate.go:264-270`: executed for allowed and limited requests alike.
        let next = self.delay_variation_tolerance.wrapping_sub(ttl);
        if next > -self.emission_interval {
            result.remaining = next / self.emission_interval;
        }
        result.reset_after_ns = ttl;
        Ok((limited, result))
    }
}

/// `BasicLimitWatcher` (`rate_limit.go:439-473`): the latest limiter, or
/// `None` when `updateFromConfig` disabled it (`limit <= 0` or an invalid
/// quota).
pub struct BasicLimitWatcher {
    store: Arc<dyn GcraStore>,
    limiter: RwLock<Option<Arc<GcraRateLimiter>>>,
}

impl BasicLimitWatcher {
    pub fn new(store: Arc<dyn GcraStore>) -> Self {
        Self {
            store,
            limiter: RwLock::new(None),
        }
    }

    /// `updateFromConfig` (`rate_limit.go:444-465`).
    pub fn update_from_config(&self, limit: i64) {
        let limiter = if limit <= 0 {
            None
        } else {
            GcraRateLimiter::from_hourly_limit(Arc::clone(&self.store), limit)
                .ok()
                .map(Arc::new)
        };
        *self.limiter.write().expect("rate limiter lock poisoned") = limiter;
    }

    /// `Get`'s `enabled` bit.
    pub fn enabled(&self) -> bool {
        self.limiter
            .read()
            .expect("rate limiter lock poisoned")
            .is_some()
    }

    /// `rlw.Get()` + `BasicLimiter.RateLimit` (`rate_limit.go:467-487`).
    /// `None` is the disabled watcher (the handler skips the limiter).
    pub async fn check(&self, args: LimiterArgs<'_>) -> Option<RateLimitOutcome> {
        self.check_detailed(args).await.map(|(outcome, _)| outcome)
    }

    /// Like [`Self::check`], but also returns the `throttled.RateLimitResult`
    /// that `BasicLimiter.RateLimit` discards. The differential harness uses
    /// it; non-matching calls return the zero result, exactly like the
    /// baseline's `(false, throttled.RateLimitResult{}, nil)`.
    pub async fn check_detailed(
        &self,
        args: LimiterArgs<'_>,
    ) -> Option<(RateLimitOutcome, RateLimitResult)> {
        let limiter = Arc::clone(
            self.limiter
                .read()
                .expect("rate limiter lock poisoned")
                .as_ref()?,
        );
        if !(args.anonymous
            && args.request_name == "unknown"
            && args.request_source == SOURCE_OTHER)
        {
            return Some((RateLimitOutcome::NotLimited, RateLimitResult::default()));
        }
        match limiter.rate_limit(BASIC_KEY, 1).await {
            Ok((true, result)) => Some((
                RateLimitOutcome::Limited {
                    retry_after_seconds: result.retry_after_ns / NANOS_PER_SECOND,
                },
                result,
            )),
            Ok((false, result)) => Some((RateLimitOutcome::NotLimited, result)),
            Err(error) => {
                // The baseline logs and continues without limiting
                // (`graphql.go:205-207`).
                tracing::error!(error = %error, "checking GraphQL rate limit");
                Some((RateLimitOutcome::NotLimited, RateLimitResult::default()))
            }
        }
    }
}

/// `redisCASScript` (`redigostore.go:14-23`), byte for byte.
const REDIS_CAS_SCRIPT: &str = r#"
local v = redis.call('get', KEYS[1])
if v == false then
  return redis.error_reply("key does not exist")
end
if v ~= ARGV[1] then
  return 0
end
redis.call('setex', KEYS[1], ARGV[3], ARGV[2])
return 1
"#;

/// `redigostore.RedigoStore` on top of [`sg_redis::KeyValue`].
///
/// The baseline sends `SELECT 0` before every operation and concatenates the
/// prefix directly (`"gql:rl:" + key`); a [`KeyValue`](sg_redis::KeyValue)
/// using the default database and `with_prefix("gql:rl")` produces the same
/// key names (`gql:rl:<key>`) without the `SELECT`.
pub struct RedisGcraStore {
    kv: sg_redis::KeyValue,
}

impl RedisGcraStore {
    pub fn new(kv: sg_redis::KeyValue) -> Self {
        Self::with_key_prefix(kv, REDIS_PREFIX)
    }

    /// `redigostore.NewCtx(pool, "<prefix>:", 0)` with an explicit prefix; the
    /// differential harness uses `gql:rl:m9fu` to isolate its keys.
    pub fn with_key_prefix(kv: sg_redis::KeyValue, prefix: &str) -> Self {
        Self {
            kv: kv.with_prefix(prefix),
        }
    }

    /// `int(ttl.Seconds())` truncated toward zero, minimum one second
    /// (`redigostore.go:123-130,148-155`).
    fn ttl_seconds(ttl_ns: i64) -> i64 {
        (ttl_ns / NANOS_PER_SECOND).max(1)
    }
}

#[async_trait]
impl GcraStore for RedisGcraStore {
    async fn get_with_time(&self, key: &str) -> Result<(i64, i64), String> {
        let (seconds, micros) = self.kv.time().await.map_err(|err| err.to_string())?;
        let value = self.kv.get_int(key).await.map_err(|err| err.to_string())?;
        let now = seconds
            .wrapping_mul(NANOS_PER_SECOND)
            .wrapping_add(micros.wrapping_mul(1_000));
        Ok((value.unwrap_or(-1), now))
    }

    async fn set_if_not_exists_with_ttl(
        &self,
        key: &str,
        value: i64,
        ttl_ns: i64,
    ) -> Result<bool, String> {
        let updated = self
            .kv
            .set_nx(key, value)
            .await
            .map_err(|err| err.to_string())?;
        self.kv
            .expire(key, Self::ttl_seconds(ttl_ns))
            .await
            .map_err(|err| err.to_string())?;
        Ok(updated)
    }

    async fn compare_and_swap_with_ttl(
        &self,
        key: &str,
        old: i64,
        new: i64,
        ttl_ns: i64,
    ) -> Result<bool, String> {
        let ttl_seconds = Self::ttl_seconds(ttl_ns);
        match self
            .kv
            .eval_int(REDIS_CAS_SCRIPT, key, &[old, new, ttl_seconds])
            .await
        {
            Ok(value) => Ok(value != 0),
            // Go matches `strings.Contains(err.Error(), "key does not exist")`
            // (redigostore.go:166); redis-rs renders the server error as
            // "key: does not exist", so match the stable tail instead.
            Err(err) if err.to_string().contains("does not exist") => Ok(false),
            Err(err) => Err(err.to_string()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;
    use std::sync::Mutex;

    /// Deterministic store: `GetWithTime` advances the clock 1ms per call and
    /// ignores TTL expiry, like the differential corpus' Go fake.
    struct FakeStore {
        values: Mutex<BTreeMap<String, i64>>,
        now: Mutex<i64>,
        fail_cas: bool,
    }

    #[async_trait]
    impl GcraStore for FakeStore {
        async fn get_with_time(&self, key: &str) -> Result<(i64, i64), String> {
            let mut now = self.now.lock().expect("now");
            *now += 1_000_000;
            Ok((
                self.values
                    .lock()
                    .expect("values")
                    .get(key)
                    .copied()
                    .unwrap_or(-1),
                *now,
            ))
        }

        async fn set_if_not_exists_with_ttl(
            &self,
            key: &str,
            value: i64,
            _ttl_ns: i64,
        ) -> Result<bool, String> {
            let mut values = self.values.lock().expect("values");
            if values.contains_key(key) {
                return Ok(false);
            }
            values.insert(key.to_owned(), value);
            Ok(true)
        }

        async fn compare_and_swap_with_ttl(
            &self,
            key: &str,
            old: i64,
            new: i64,
            _ttl_ns: i64,
        ) -> Result<bool, String> {
            if self.fail_cas {
                return Ok(false);
            }
            let mut values = self.values.lock().expect("values");
            match values.get(key) {
                Some(value) if *value == old => {
                    values.insert(key.to_owned(), new);
                    Ok(true)
                }
                _ => Ok(false),
            }
        }
    }

    fn store() -> Arc<FakeStore> {
        Arc::new(FakeStore {
            values: Mutex::new(BTreeMap::new()),
            now: Mutex::new(1_600_000_000_000_000_000),
            fail_cas: false,
        })
    }

    #[tokio::test]
    async fn allows_the_burst_then_limits() {
        let store = store();
        let limiter = GcraRateLimiter::from_hourly_limit(store, 10).expect("limiter");
        // PerHour(10) => 360s emission, MaxBurst 2 => Limit 3.
        for _ in 0..3 {
            let (limited, result) = limiter.rate_limit(BASIC_KEY, 1).await.expect("call");
            assert!(!limited);
            assert_eq!(result.limit, 3);
        }
        let (limited, result) = limiter.rate_limit(BASIC_KEY, 1).await.expect("call");
        assert!(limited);
        assert_eq!(result.limit, 3);
        // DVT is 3 * 360s; the fourth request waits one emission interval
        // minus the 3ms the fake clock advanced during the first three calls.
        assert_eq!(result.retry_after_ns, 360 * NANOS_PER_SECOND - 3_000_000);
        assert_eq!(result.retry_after_ns / NANOS_PER_SECOND, 359);
        assert_eq!(result.reset_after_ns, 1080 * NANOS_PER_SECOND - 3_000_000);
    }

    #[tokio::test]
    async fn concurrent_swap_failures_stop_after_ten_attempts() {
        let store = Arc::new(FakeStore {
            values: Mutex::new(BTreeMap::new()),
            now: Mutex::new(0),
            fail_cas: true,
        });
        store
            .values
            .lock()
            .expect("values")
            .insert(BASIC_KEY.to_owned(), -1_000_000);
        let limiter = GcraRateLimiter::from_hourly_limit(store, 10).expect("limiter");
        let error = limiter.rate_limit(BASIC_KEY, 1).await.expect_err("error");
        assert_eq!(
            error.message,
            "Failed to store updated rate limit data for key basic after 10 attempts"
        );
    }

    #[tokio::test]
    async fn disabled_and_non_matching_requests_do_not_touch_the_store() {
        let store = store();
        let watcher = BasicLimitWatcher::new(store.clone());
        watcher.update_from_config(0);
        assert!(!watcher.enabled());
        assert_eq!(
            watcher
                .check(LimiterArgs {
                    is_ip: false,
                    anonymous: true,
                    request_name: "unknown",
                    request_source: SOURCE_OTHER,
                })
                .await,
            None
        );

        watcher.update_from_config(10);
        assert!(watcher.enabled());
        assert_eq!(
            watcher
                .check(LimiterArgs {
                    is_ip: false,
                    anonymous: false,
                    request_name: "unknown",
                    request_source: SOURCE_OTHER,
                })
                .await,
            Some(RateLimitOutcome::NotLimited)
        );
        assert_eq!(
            watcher
                .check(LimiterArgs {
                    is_ip: false,
                    anonymous: true,
                    request_name: "Repositories",
                    request_source: SOURCE_OTHER,
                })
                .await,
            Some(RateLimitOutcome::NotLimited)
        );
        assert_eq!(
            watcher
                .check(LimiterArgs {
                    is_ip: false,
                    anonymous: true,
                    request_name: "unknown",
                    request_source: "browser",
                })
                .await,
            Some(RateLimitOutcome::NotLimited)
        );
        // The matching call is the first one to touch the store.
        assert_eq!(store.values.lock().expect("values").len(), 0);
        watcher
            .check(LimiterArgs {
                is_ip: false,
                anonymous: true,
                request_name: "unknown",
                request_source: SOURCE_OTHER,
            })
            .await;
        assert_eq!(store.values.lock().expect("values").len(), 1);
    }
}
