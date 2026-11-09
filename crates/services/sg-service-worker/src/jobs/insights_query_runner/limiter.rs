#![forbid(unsafe_code)]

//! `internal/insights/background/limiter/search_query.go`: a token bucket
//! shared by every query-runner handler in the process.
//!
//! `searchQueryRate` defaults to 20 queries/s with a burst of 20; the site
//! configuration can override both (`insights.query.worker.rateLimit`,
//! `insights.query.worker.rateLimitBurst`). The baseline rebuilds the limiter
//! when the configuration changes (`conf.Watch`); this port reads the values
//! when the job's routines are created.

use std::sync::Mutex;
use std::time::{Duration, Instant};

use tokio_util::sync::CancellationToken;

/// `defaultRateLimit := rate.Limit(20.0)`.
pub const DEFAULT_RATE_LIMIT: f64 = 20.0;
/// `defaultBurst := 20`.
pub const DEFAULT_BURST: i64 = 20;
/// `defaultRateLimit := rate.Limit(20.0)` in `limiter/historical.go`.
pub const HISTORICAL_DEFAULT_RATE_LIMIT: f64 = 20.0;
/// `defaultBurst := 20` in `limiter/historical.go`.
pub const HISTORICAL_DEFAULT_BURST: i64 = 20;

struct Bucket {
    tokens: f64,
    last: Instant,
}

/// `ratelimit.InstrumentedLimiter` around `rate.Limiter`.
pub struct SearchQueryLimiter {
    rate: f64,
    burst: f64,
    bucket: Mutex<Bucket>,
}

impl SearchQueryLimiter {
    pub fn new(rate: f64, burst: i64) -> Self {
        let burst = if burst <= 0 {
            DEFAULT_BURST as f64
        } else {
            burst as f64
        };
        Self {
            rate,
            burst,
            bucket: Mutex::new(Bucket {
                tokens: burst,
                last: Instant::now(),
            }),
        }
    }

    /// `getSearchQueryRateLimit`: the configured rate or 20/s; a non-positive
    /// burst falls back to 20.
    pub fn from_site(site: &sg_config::SiteConfiguration) -> Self {
        let rate = site
            .insights_query_worker_rate_limit()
            .unwrap_or(DEFAULT_RATE_LIMIT);
        Self::new(rate, site.insights_query_worker_rate_limit_burst())
    }

    /// `limiter.HistoricalWorkRate`'s `getHistoricalWorkerRateLimit`: the
    /// configured rate or 20 frames/s; a non-positive burst falls back to 20
    /// (the same default value as the search-query limiter; the baseline
    /// keeps the two limiters independent but with equal defaults).
    pub fn historical_from_site(site: &sg_config::SiteConfiguration) -> Self {
        let rate = site
            .insights_historical_worker_rate_limit()
            .unwrap_or(HISTORICAL_DEFAULT_RATE_LIMIT);
        Self::new(rate, site.insights_historical_worker_rate_limit_burst())
    }

    /// `Limiter.Wait`: block until one token is available. Cancellation
    /// surfaces as the context error the baseline returns.
    pub async fn wait(&self, ctx: &CancellationToken) -> Result<(), String> {
        loop {
            let delay = {
                let mut bucket = self.bucket.lock().expect("limiter lock");
                let now = Instant::now();
                let elapsed = now.duration_since(bucket.last).as_secs_f64();
                bucket.last = now;
                bucket.tokens = (bucket.tokens + elapsed * self.rate).min(self.burst);
                if bucket.tokens >= 1.0 {
                    bucket.tokens -= 1.0;
                    return Ok(());
                }
                if self.rate <= 0.0 {
                    None
                } else {
                    Some((1.0 - bucket.tokens) / self.rate)
                }
            };

            let sleep = match delay {
                Some(seconds) => {
                    let capped = seconds.min(1.0);
                    tokio::time::sleep(Duration::from_secs_f64(capped))
                }
                None => tokio::time::sleep(Duration::from_secs(1)),
            };
            tokio::select! {
                _ = ctx.cancelled() => return Err("context canceled".to_owned()),
                _ = sleep => {}
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn default_burst_is_immediate_then_rate_limited() {
        let limiter = SearchQueryLimiter::new(1000.0, 2);
        let ctx = CancellationToken::new();
        let started = Instant::now();
        for _ in 0..2 {
            limiter.wait(&ctx).await.expect("burst token");
        }
        assert!(started.elapsed() < Duration::from_millis(100));
        // The third token has to wait for the bucket to refill.
        limiter.wait(&ctx).await.expect("refilled token");
    }

    #[tokio::test]
    async fn zero_burst_uses_the_default() {
        let limiter = SearchQueryLimiter::new(1.0, 0);
        let ctx = CancellationToken::new();
        for _ in 0..DEFAULT_BURST {
            limiter.wait(&ctx).await.expect("burst token");
        }
    }

    #[tokio::test]
    async fn wait_is_cancellable() {
        let limiter = SearchQueryLimiter::new(0.0, 1);
        let ctx = CancellationToken::new();
        limiter.wait(&ctx).await.expect("initial token");
        ctx.cancel();
        assert_eq!(limiter.wait(&ctx).await.unwrap_err(), "context canceled");
    }
}
