#![forbid(unsafe_code)]

//! `internal/extsvc` rate-limit extraction: `GetLimitFromConfig`,
//! `GetDefaultRateLimit`, `limitOrInf` and the `Variant` table they switch on.
//!
//! The caller decrypts `external_services.config` first (`svc.Config.Decrypt`)
//! and hands the plaintext JSON here, mirroring
//! `ParseEncryptableConfig` + `GetLimitFromConfig` in the baseline. `rate.Limit`
//! is a float64 in requests per *second*; `rate.Inf` is `math.MaxFloat64`
//! (`golang.org/x/time/rate`: `const Inf = Limit(math.MaxFloat64)`), *not*
//! IEEE infinity — hence [`RATE_INF`].

/// `golang.org/x/time/rate.Inf`: the sentinel for "no limit". It is
/// `math.MaxFloat64`, not IEEE infinity, so comparisons and arithmetic that
/// flow through `rate.Limit` must use this value.
pub const RATE_INF: f64 = f64::MAX;

/// `extsvc.Variant`: the external-service variants that have a schema
/// prototype (`variantValuesMap`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Variant {
    AwsCodeCommit,
    AzureDevOps,
    BitbucketCloud,
    BitbucketServer,
    Gerrit,
    GitHub,
    GitLab,
    Gitolite,
    GoPackages,
    JvmPackages,
    NpmPackages,
    Other,
    Pagure,
    Perforce,
    Phabricator,
    PythonPackages,
    RubyPackages,
    RustPackages,
}

impl Variant {
    /// `Variant.AsKind`: the value stored in `external_services.kind`.
    pub fn kind(self) -> &'static str {
        match self {
            Self::AwsCodeCommit => "AWSCODECOMMIT",
            Self::AzureDevOps => "AZUREDEVOPS",
            Self::BitbucketCloud => "BITBUCKETCLOUD",
            Self::BitbucketServer => "BITBUCKETSERVER",
            Self::Gerrit => "GERRIT",
            Self::GitHub => "GITHUB",
            Self::GitLab => "GITLAB",
            Self::Gitolite => "GITOLITE",
            Self::GoPackages => "GOMODULES",
            Self::JvmPackages => "JVMPACKAGES",
            Self::NpmPackages => "NPMPACKAGES",
            Self::Other => "OTHER",
            Self::Pagure => "PAGURE",
            Self::Perforce => "PERFORCE",
            Self::Phabricator => "PHABRICATOR",
            Self::PythonPackages => "PYTHONPACKAGES",
            Self::RubyPackages => "RUBYPACKAGES",
            Self::RustPackages => "RUSTPACKAGES",
        }
    }

    /// `Variant.AsType`: the value stored in `repo.external_service_type`.
    pub fn type_name(self) -> &'static str {
        match self {
            Self::AwsCodeCommit => "awscodecommit",
            Self::AzureDevOps => "azuredevops",
            Self::BitbucketCloud => "bitbucketCloud",
            Self::BitbucketServer => "bitbucketServer",
            Self::Gerrit => "gerrit",
            Self::GitHub => "github",
            Self::GitLab => "gitlab",
            Self::Gitolite => "gitolite",
            Self::GoPackages => "goModules",
            Self::JvmPackages => "jvmPackages",
            Self::NpmPackages => "npmPackages",
            Self::Other => "other",
            Self::Pagure => "pagure",
            Self::Perforce => "perforce",
            Self::Phabricator => "phabricator",
            Self::PythonPackages => "pythonPackages",
            Self::RubyPackages => "rubyPackages",
            Self::RustPackages => "rustPackages",
        }
    }

    /// `extsvc.VariantValueOf`: case-insensitive match against either the kind
    /// or the type spelling; `None` is the baseline's "no Variant found".
    pub fn from_kind(input: &str) -> Option<Self> {
        ALL_VARIANTS.iter().copied().find(|variant| {
            variant.kind().eq_ignore_ascii_case(input)
                || variant.type_name().eq_ignore_ascii_case(input)
        })
    }
}

/// Every variant in `variantValuesMap`; no two share a kind or type spelling,
/// so the lookup order is irrelevant.
pub const ALL_VARIANTS: [Variant; 18] = [
    Variant::AwsCodeCommit,
    Variant::AzureDevOps,
    Variant::BitbucketCloud,
    Variant::BitbucketServer,
    Variant::Gerrit,
    Variant::GitHub,
    Variant::GitLab,
    Variant::Gitolite,
    Variant::GoPackages,
    Variant::JvmPackages,
    Variant::NpmPackages,
    Variant::Other,
    Variant::Pagure,
    Variant::Perforce,
    Variant::Phabricator,
    Variant::PythonPackages,
    Variant::RubyPackages,
    Variant::RustPackages,
];

/// `extsvc.ErrRateLimitUnsupported`: rate limiting is not implemented for the
/// code host. `syncServices` skips these services silently.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ErrRateLimitUnsupported {
    pub codehost_kind: String,
}

impl std::fmt::Display for ErrRateLimitUnsupported {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "internal rate limiting not supported for {}",
            self.codehost_kind
        )
    }
}

impl std::error::Error for ErrRateLimitUnsupported {}

/// `limitOrInf`: an enabled limit is "requests per hour" divided down to
/// requests per second; a disabled one is unlimited.
pub fn limit_or_inf(enabled: bool, per_hour: f64) -> f64 {
    if enabled {
        per_hour / 3600.0
    } else {
        RATE_INF
    }
}

/// `GetDefaultRateLimit`: the requests-per-second limit per code host, in the
/// baseline's own order (the default is `rate.Inf`).
pub fn get_default_rate_limit(variant: Variant) -> f64 {
    match variant {
        // Use an infinite rate limiter. GitHub has an external rate limiter we
        // obey.
        Variant::GitHub => RATE_INF,
        Variant::GitLab => RATE_INF,
        // 8/s is the default limit we enforce.
        Variant::BitbucketServer => 8.0,
        Variant::BitbucketCloud => 2.0,
        Variant::Perforce => 5000.0 / 3600.0,
        Variant::JvmPackages => 2.0,
        Variant::Pagure => 8.0,
        Variant::NpmPackages => 6000.0 / 3600.0,
        // Unlike the GitHub or GitLab APIs, the public npm registry (i.e.
        // https://proxy.golang.org) doesn't document an enforced req/s rate
        // limit AND we do a lot more individual requests in comparison since
        // they don't offer enough batch APIs.
        Variant::GoPackages => 57600.0 / 3600.0,
        // Unlike the GitHub or GitLab APIs, the pypi.org doesn't document an
        // enforced req/s rate limit.
        Variant::PythonPackages => 57600.0 / 3600.0,
        // The crates.io CDN has no rate limits.
        Variant::RustPackages => 100.0,
        // The rubygems.org API allows 10 rps.
        Variant::RubyPackages => 10.0,
        Variant::AzureDevOps => RATE_INF,
        _ => RATE_INF,
    }
}

/// Where the per-connection rate-limit block lives in the parsed config.
#[derive(Debug, Clone, Copy)]
enum RateLimitPath {
    /// `config.rateLimit`.
    RateLimit,
    /// `config.maven.rateLimit` (`JVMPackagesConnection.Maven.RateLimit`).
    MavenRateLimit,
}

/// `GetLimitFromConfig`: the configured limit if the service defines one,
/// else the code-host default. Returns `(limit, is_default)`.
///
/// The baseline switches on the *parsed prototype type*, which is selected by
/// the kind through `getConfigPrototype`; the two are equivalent, and the set
/// of kinds with a case here is exactly the set the baseline handles.
pub fn get_limit_from_config(
    config: &serde_json::Value,
    variant: Variant,
) -> Result<(f64, bool), ErrRateLimitUnsupported> {
    let path = match variant {
        Variant::GitLab
        | Variant::GitHub
        | Variant::BitbucketServer
        | Variant::BitbucketCloud
        | Variant::Pagure
        | Variant::NpmPackages
        | Variant::GoPackages
        | Variant::PythonPackages
        | Variant::RustPackages
        | Variant::RubyPackages
        | Variant::AzureDevOps => RateLimitPath::RateLimit,
        Variant::JvmPackages => RateLimitPath::MavenRateLimit,
        other => {
            return Err(ErrRateLimitUnsupported {
                codehost_kind: other.kind().to_owned(),
            })
        }
    };

    let default_limit = get_default_rate_limit(variant);
    let rate_limit = match path {
        RateLimitPath::RateLimit => config.get("rateLimit"),
        RateLimitPath::MavenRateLimit => {
            config.get("maven").and_then(|maven| maven.get("rateLimit"))
        }
    };
    let Some(rate_limit) = rate_limit.and_then(serde_json::Value::as_object) else {
        // Not defined: the code host default applies.
        return Ok((default_limit, true));
    };

    // Defined: `enabled` / `requestsPerHour` are the struct's zero values when
    // absent, like `json.Unmarshal` into `schema.*RateLimit`.
    let enabled = rate_limit
        .get("enabled")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false);
    let per_hour = rate_limit
        .get("requestsPerHour")
        .and_then(serde_json::Value::as_f64)
        .unwrap_or(0.0);
    Ok((limit_or_inf(enabled, per_hour), false))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn variants_match_kind_and_type_case_insensitively() {
        assert_eq!(Variant::from_kind("GITHUB"), Some(Variant::GitHub));
        assert_eq!(Variant::from_kind("github"), Some(Variant::GitHub));
        assert_eq!(Variant::from_kind("GitHub"), Some(Variant::GitHub));
        assert_eq!(
            Variant::from_kind("bitbucketServer"),
            Some(Variant::BitbucketServer)
        );
        assert_eq!(Variant::from_kind("GOMODULES"), Some(Variant::GoPackages));
        assert_eq!(Variant::from_kind("goModules"), Some(Variant::GoPackages));
        assert_eq!(Variant::from_kind("FOO"), None);
    }

    #[test]
    fn default_limits_match_the_baseline_table() {
        assert_eq!(get_default_rate_limit(Variant::GitHub), RATE_INF);
        assert_eq!(get_default_rate_limit(Variant::GitLab), RATE_INF);
        assert_eq!(get_default_rate_limit(Variant::BitbucketServer), 8.0);
        assert_eq!(get_default_rate_limit(Variant::BitbucketCloud), 2.0);
        assert_eq!(get_default_rate_limit(Variant::JvmPackages), 2.0);
        assert_eq!(get_default_rate_limit(Variant::Pagure), 8.0);
        assert_eq!(get_default_rate_limit(Variant::RustPackages), 100.0);
        assert_eq!(get_default_rate_limit(Variant::RubyPackages), 10.0);
        assert_eq!(get_default_rate_limit(Variant::AzureDevOps), RATE_INF);
        assert_eq!(get_default_rate_limit(Variant::Perforce), 5000.0 / 3600.0);
        assert_eq!(get_default_rate_limit(Variant::GoPackages), 16.0);
    }

    #[test]
    fn absent_rate_limit_uses_the_default() {
        let config = json!({"url": "https://github.com"});
        assert_eq!(
            get_limit_from_config(&config, Variant::GitHub),
            Ok((RATE_INF, true))
        );
        assert_eq!(
            get_limit_from_config(&config, Variant::BitbucketServer),
            Ok((8.0, true))
        );
        // `rateLimit: null` leaves the pointer nil, i.e. the default.
        let config = json!({"rateLimit": null});
        assert_eq!(
            get_limit_from_config(&config, Variant::GitLab),
            Ok((RATE_INF, true))
        );
    }

    #[test]
    fn configured_rate_limit_is_divided_down_to_per_second() {
        let config = json!({"rateLimit": {"enabled": true, "requestsPerHour": 150.0}});
        assert_eq!(
            get_limit_from_config(&config, Variant::GitHub),
            Ok((150.0 / 3600.0, false))
        );
        // Disabled means unlimited regardless of requestsPerHour.
        let config = json!({"rateLimit": {"enabled": false, "requestsPerHour": 150.0}});
        assert_eq!(
            get_limit_from_config(&config, Variant::GitLab),
            Ok((RATE_INF, false))
        );
        // The struct's zero values when the fields are absent: a defined but
        // disabled rate limit (`enabled` defaults to false) is unlimited.
        let config = json!({"rateLimit": {}});
        assert_eq!(
            get_limit_from_config(&config, Variant::BitbucketCloud),
            Ok((RATE_INF, false))
        );
    }

    #[test]
    fn jvm_packages_read_the_maven_rate_limit() {
        let config = json!({"maven": {"rateLimit": {"enabled": true, "requestsPerHour": 7200}}});
        assert_eq!(
            get_limit_from_config(&config, Variant::JvmPackages),
            Ok((2.0, false))
        );
        let config = json!({"maven": {}});
        assert_eq!(
            get_limit_from_config(&config, Variant::JvmPackages),
            Ok((2.0, true))
        );
    }

    #[test]
    fn kinds_without_a_case_are_unsupported() {
        for variant in [
            Variant::AwsCodeCommit,
            Variant::Gerrit,
            Variant::Gitolite,
            Variant::Other,
            Variant::Perforce,
            Variant::Phabricator,
        ] {
            let err = get_limit_from_config(&json!({}), variant).expect_err("unsupported");
            assert_eq!(
                err.to_string(),
                format!(
                    "internal rate limiting not supported for {}",
                    variant.kind()
                )
            );
        }
    }

    #[test]
    fn per_hour_to_per_second_conversion_truncates_like_go() {
        // `int32(limit * 3600)` in `syncServices`; float64 round-trips here.
        for (per_hour, expected) in [(150.0, 150), (500.0, 500), (999.0, 999), (0.5, 0)] {
            let limit = limit_or_inf(true, per_hour);
            assert_eq!((limit * 3600.0) as i32, expected, "per_hour={per_hour}");
        }
    }
}

// ---------------------------------------------------------------------------
// `internal/ratelimit` — the limiter, monitor and global-limiter core.
// ---------------------------------------------------------------------------
//
// The baseline has three layers:
//
// * `InstrumentedLimiter` wraps any `Limiter` (`rate.Limiter` in practice)
//   and turns a zero limit/burst into the sentinel `ErrBlockAll`;
// * `GlobalLimiter` is the Redis token bucket driven by
//   `globallimitergettokens.lua`, with an in-memory fallback;
// * `Monitor` reads `RateLimit-*`/`Retry-After` response headers and advises
//   how long a background operation should wait.
//
// This module ports the in-memory limiter, the instrumentation, the key
// layout, the error mapping and the whole `Monitor` surface. The Redis-backed
// `GlobalLimiter` (the Lua token bucket) is represented by the
// [`GlobalLimiter`] trait; its Redis implementation lives where the worker
// already owns a `sg-redis` connection, and the in-memory fallback
// ([`InMemoryGlobalLimiter`]) is provided here so app mode matches the
// baseline's `getInMemoryLimiter`.

use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};
use std::time::Duration;

use async_trait::async_trait;

/// `ratelimit.defaultBurst`.
pub const DEFAULT_BURST: usize = 10;

/// `ratelimit.tokenBucketGlobalPrefix`.
pub const TOKEN_BUCKET_GLOBAL_PREFIX: &str = "v2:rate_limiters";

/// `ratelimit.GitRPSLimiterBucketName`.
pub const GIT_RPS_LIMITER_BUCKET_NAME: &str = "git-rps";

pub const BUCKET_LAST_REPLENISHMENT_TIMESTAMP_KEY_SUFFIX: &str = "last_replenishment_timestamp";
pub const BUCKET_ALLOWED_BURST_KEY_SUFFIX: &str = "allowed_burst";
pub const BUCKET_RATE_CONFIG_KEY_SUFFIX: &str = "config:bucket_rate";
pub const BUCKET_REPLENISHMENT_CONFIG_KEY_SUFFIX: &str =
    "config:bucket_replenishment_interval_seconds";

/// `ratelimit.ErrBlockAll`: the limiter is configured to permit nothing.
pub const ERR_BLOCK_ALL_MESSAGE: &str = "ratelimit: limit and burst are zero";

/// Error returned when `n` exceeds the limiter's burst (`rate: Wait(n=%d)
/// exceeds limiter's burst %d`).
#[derive(Debug, Clone, PartialEq)]
pub struct WaitExceedsBurst {
    pub n: usize,
    pub burst: usize,
}

impl std::fmt::Display for WaitExceedsBurst {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "rate: Wait(n={}) exceeds limiter's burst {}",
            self.n, self.burst
        )
    }
}

impl std::error::Error for WaitExceedsBurst {}

/// A limiter error; [`Self::BlockAll`] is `ratelimit.ErrBlockAll`.
#[derive(Debug, Clone, PartialEq)]
pub enum LimiterError {
    BlockAll,
    ExceedsBurst(WaitExceedsBurst),
    Other(String),
}

impl std::fmt::Display for LimiterError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::BlockAll => f.write_str(ERR_BLOCK_ALL_MESSAGE),
            Self::ExceedsBurst(err) => err.fmt(f),
            Self::Other(message) => f.write_str(message),
        }
    }
}

impl std::error::Error for LimiterError {}

/// `ratelimit.Limiter` / `InspectableLimiter`. `limit_burst` returns `None`
/// for limiters that cannot be inspected.
#[async_trait]
pub trait Limiter: Send + Sync {
    async fn wait_n(&self, n: usize) -> Result<(), LimiterError>;

    /// `InspectableLimiter.Limit()`/`Burst()`: `(limit, burst)` in
    /// requests/second and tokens.
    fn limit_burst(&self) -> Option<(f64, usize)> {
        None
    }

    /// `Wait` is shorthand for `WaitN(ctx, 1)`.
    async fn wait(&self) -> Result<(), LimiterError> {
        self.wait_n(1).await
    }
}

/// `golang.org/x/time/rate.Limiter`: a token bucket. The bucket starts full
/// (`rate.NewLimiter` semantics). Times are measured from an arbitrary epoch
/// via [`Duration`], so the differential harness can supply a fake clock.
pub struct InMemoryLimiter {
    state: Mutex<TokenBucketState>,
}

#[derive(Debug, Clone, Copy)]
struct TokenBucketState {
    limit: f64,
    burst: usize,
    tokens: f64,
    last: Duration,
}

impl InMemoryLimiter {
    pub fn new(limit_per_second: f64, burst: usize) -> Self {
        Self {
            state: Mutex::new(TokenBucketState {
                limit: limit_per_second,
                burst,
                tokens: burst as f64,
                last: Duration::ZERO,
            }),
        }
    }

    pub fn limit(&self) -> f64 {
        self.state.lock().expect("limiter state").limit
    }

    pub fn burst(&self) -> usize {
        self.state.lock().expect("limiter state").burst
    }

    /// `rate.Limiter.SetLimit` + `SetBurst`; the bucket is not refilled.
    pub fn set_limit_and_burst(&self, limit: f64, burst: usize) {
        let mut state = self.state.lock().expect("limiter state");
        state.limit = limit;
        state.burst = burst;
    }

    /// `Limiter.reserveN` at a supplied clock: returns the duration the caller
    /// must wait before the `n` tokens may be used.
    pub fn reserve_n_at(&self, now: Duration, n: usize) -> Result<Duration, LimiterError> {
        let mut state = self.state.lock().expect("limiter state");
        if n > state.burst && state.limit != RATE_INF {
            return Err(LimiterError::ExceedsBurst(WaitExceedsBurst {
                n,
                burst: state.burst,
            }));
        }

        let limit = state.limit;
        let burst = state.burst;
        let elapsed = now.saturating_sub(state.last);
        if limit == RATE_INF {
            state.last = now;
            state.tokens = burst as f64;
            return Ok(Duration::ZERO);
        }

        // `rate.Limiter.advance`: replenish, clamp to burst, then keep the
        // (possibly negative) token debt after the reservation. Keeping the
        // debt is what makes a later reservation at `now + wait` see the bucket
        // recovered exactly like `golang.org/x/time/rate`.
        state.tokens = (state.tokens + elapsed.as_secs_f64() * limit).min(burst as f64);
        state.last = now;

        if limit == 0.0 {
            // `durationFromTokens` is +Inf; the baseline's WaitN reports a
            // deadline error. Callers reach limit=0 through the instrumented
            // limiter, which preflights block-all.
            return Err(LimiterError::BlockAll);
        }
        let wait = if state.tokens < n as f64 {
            // `rate.Limit.durationFromTokens` truncates toward zero.
            let seconds = (n as f64 - state.tokens) / limit;
            Duration::from_nanos((seconds * 1_000_000_000.0) as u64)
        } else {
            Duration::ZERO
        };
        state.tokens -= n as f64;
        Ok(wait)
    }
}

#[async_trait]
impl Limiter for InMemoryLimiter {
    async fn wait_n(&self, n: usize) -> Result<(), LimiterError> {
        // `rate.Limiter.WaitN` uses a monotonic clock; the differential path
        // uses `reserve_n_at` with an injected clock.
        let now = monotonic_epoch();
        let wait = self.reserve_n_at(now, n)?;
        if !wait.is_zero() {
            tokio::time::sleep(wait).await;
        }
        Ok(())
    }

    fn limit_burst(&self) -> Option<(f64, usize)> {
        Some((self.limit(), self.burst()))
    }
}

/// A process-wide monotonic epoch so every `InMemoryLimiter` shares a clock,
/// matching `rate.NewLimiter`'s use of `time.Now` (monotonic).
fn monotonic_epoch() -> Duration {
    static EPOCH: OnceLock<std::time::Instant> = OnceLock::new();
    EPOCH.get_or_init(std::time::Instant::now).elapsed()
}

/// `ratelimit.InstrumentedLimiter`.
pub struct InstrumentedLimiter {
    inner: std::sync::Arc<dyn Limiter>,
    urn: String,
}

impl InstrumentedLimiter {
    /// `NewInstrumentedLimiter`.
    pub fn new(urn: impl Into<String>, limiter: std::sync::Arc<dyn Limiter>) -> Self {
        Self {
            inner: limiter,
            urn: urn.into(),
        }
    }

    pub fn urn(&self) -> &str {
        &self.urn
    }

    /// `InstrumentedLimiter.Wait`.
    pub async fn wait(&self) -> Result<(), LimiterError> {
        self.wait_n(1).await
    }

    /// `InstrumentedLimiter.WaitN`: an inspectable `(0, 0)` limiter is
    /// reported as `ErrBlockAll` before the underlying wait, and
    /// `AllBlockedError` from the Redis script is mapped back to it.
    pub async fn wait_n(&self, n: usize) -> Result<(), LimiterError> {
        if let Some((limit, burst)) = self.inner.limit_burst() {
            if limit == 0.0 && burst == 0 {
                return Err(LimiterError::BlockAll);
            }
        }
        match self.inner.wait_n(n).await {
            Ok(()) => Ok(()),
            Err(LimiterError::Other(message)) if message.contains("rate is 0") => {
                Err(LimiterError::BlockAll)
            }
            Err(err) => Err(err),
        }
    }
}

/// `ratelimit.rateLimitBucketConfigKeys`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RateLimitBucketConfigKeys {
    pub bucket_key: String,
    pub rate_key: String,
    pub replenishment_interval_seconds_key: String,
    pub last_replenishment_timestamp_key: String,
    pub burst_key: String,
}

/// `ratelimit.getRateLimiterKeys`.
pub fn get_rate_limiter_keys(prefix: &str, bucket_name: &str) -> RateLimitBucketConfigKeys {
    let bucket_key = format!("{prefix}:{bucket_name}");
    RateLimitBucketConfigKeys {
        rate_key: format!("{bucket_key}:{BUCKET_RATE_CONFIG_KEY_SUFFIX}"),
        replenishment_interval_seconds_key: format!(
            "{bucket_key}:{BUCKET_REPLENISHMENT_CONFIG_KEY_SUFFIX}"
        ),
        last_replenishment_timestamp_key: format!(
            "{bucket_key}:{BUCKET_LAST_REPLENISHMENT_TIMESTAMP_KEY_SUFFIX}"
        ),
        burst_key: format!("{bucket_key}:{BUCKET_ALLOWED_BURST_KEY_SUFFIX}"),
        bucket_key,
    }
}

/// `ratelimit.getTokenGrantType` (`tokenGranted = 1`, `waitTimeExceedsDeadline
/// = -1`, `negativeTimeDifference = -2`, `allBlocked = -3`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TokenGrantType {
    Granted,
    WaitTimeExceedsDeadline,
    NegativeTimeDifference,
    AllBlocked,
    Unexpected,
}

impl TokenGrantType {
    pub fn from_code(code: i64) -> Self {
        match code {
            1 => Self::Granted,
            -1 => Self::WaitTimeExceedsDeadline,
            -2 => Self::NegativeTimeDifference,
            -3 => Self::AllBlocked,
            _ => Self::Unexpected,
        }
    }
}

/// `ratelimit.WaitTimeExceedsDeadlineError`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WaitTimeExceedsDeadlineError {
    pub token_bucket_key: String,
    pub time_to_wait: Duration,
}

impl std::fmt::Display for WaitTimeExceedsDeadlineError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "bucket:{}, acquiring token would require a wait of {} which exceeds the context deadline",
            self.token_bucket_key,
            go_duration_string(self.time_to_wait)
        )
    }
}

impl std::error::Error for WaitTimeExceedsDeadlineError {}

/// `ratelimit.NegativeTimeDifferenceError`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NegativeTimeDifferenceError {
    pub token_bucket_key: String,
}

impl std::fmt::Display for NegativeTimeDifferenceError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "bucket:{}, time difference between now and the last replenishment is negative",
            self.token_bucket_key
        )
    }
}

impl std::error::Error for NegativeTimeDifferenceError {}

/// `ratelimit.AllBlockedError`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AllBlockedError {
    pub token_bucket_key: String,
}

impl std::fmt::Display for AllBlockedError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "bucket:{}, rate is 0, no requests permitted",
            self.token_bucket_key
        )
    }
}

impl std::error::Error for AllBlockedError {}

/// `ratelimit.UnexpectedRateLimitReturnError`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnexpectedRateLimitReturnError {
    pub token_bucket_key: String,
}

impl std::fmt::Display for UnexpectedRateLimitReturnError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "bucket:{}, unexpected return code from rate limit script",
            self.token_bucket_key
        )
    }
}

impl std::error::Error for UnexpectedRateLimitReturnError {}

/// `ratelimit.getTokenBucketError`: the `(allowed, timeToWait)` pair the Lua
/// script returns, mapped to Go's error values. `Granted` maps to `Ok(())`.
#[derive(Debug, Clone, PartialEq)]
pub enum TokenBucketError {
    Granted,
    WaitTimeExceedsDeadline(WaitTimeExceedsDeadlineError),
    NegativeTimeDifference(NegativeTimeDifferenceError),
    AllBlocked(AllBlockedError),
    Unexpected(UnexpectedRateLimitReturnError),
}

impl TokenBucketError {
    pub fn from_script_result(
        bucket_key: &str,
        allowed: i64,
        time_to_wait: Duration,
    ) -> Self {
        match TokenGrantType::from_code(allowed) {
            TokenGrantType::Granted => Self::Granted,
            TokenGrantType::WaitTimeExceedsDeadline => {
                Self::WaitTimeExceedsDeadline(WaitTimeExceedsDeadlineError {
                    token_bucket_key: bucket_key.to_owned(),
                    time_to_wait,
                })
            }
            TokenGrantType::NegativeTimeDifference => {
                Self::NegativeTimeDifference(NegativeTimeDifferenceError {
                    token_bucket_key: bucket_key.to_owned(),
                })
            }
            TokenGrantType::AllBlocked => Self::AllBlocked(AllBlockedError {
                token_bucket_key: bucket_key.to_owned(),
            }),
            TokenGrantType::Unexpected => {
                Self::Unexpected(UnexpectedRateLimitReturnError {
                    token_bucket_key: bucket_key.to_owned(),
                })
            }
        }
    }

    pub fn is_granted(&self) -> bool {
        matches!(self, Self::Granted)
    }
}

impl std::fmt::Display for TokenBucketError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Granted => f.write_str("granted"),
            Self::WaitTimeExceedsDeadline(err) => err.fmt(f),
            Self::NegativeTimeDifference(err) => err.fmt(f),
            Self::AllBlocked(err) => err.fmt(f),
            Self::Unexpected(err) => err.fmt(f),
        }
    }
}

impl std::error::Error for TokenBucketError {}

/// `ratelimit.GlobalLimiter` (the read side; the worker owns the write side).
#[async_trait]
pub trait GlobalLimiter: Send + Sync {
    async fn wait(&self) -> Result<(), TokenBucketError>;
    async fn wait_n(&self, n: usize) -> Result<(), TokenBucketError>;
    async fn set_token_bucket_config(
        &self,
        bucket_quota: i32,
        bucket_replenish_interval: Duration,
    ) -> Result<(), String>;
}

/// `ratelimit.inMemoryLimiter`: the app-mode/fallback token bucket.
pub struct InMemoryGlobalLimiter {
    limiter: InMemoryLimiter,
}

impl InMemoryGlobalLimiter {
    /// `getInMemoryLimiter(name, defaultPerHour)`: `rate = defaultPerHour/3600`
    /// (`rate.Inf` when negative), burst `defaultBurst`.
    pub fn new(default_per_hour: i64) -> Self {
        let limit = if default_per_hour < 0 {
            RATE_INF
        } else {
            // The baseline divides two ints here (`rate.Limit(defaultPerHour / 3600)`),
            // so fractional requests-per-second are truncated away.
            (default_per_hour / 3600) as f64
        };
        Self {
            limiter: InMemoryLimiter::new(limit, DEFAULT_BURST),
        }
    }
}

#[async_trait]
impl GlobalLimiter for InMemoryGlobalLimiter {
    async fn wait(&self) -> Result<(), TokenBucketError> {
        self.wait_n(1).await
    }

    async fn wait_n(&self, n: usize) -> Result<(), TokenBucketError> {
        self.limiter
            .wait_n(n)
            .await
            .map_err(|err| match err {
                LimiterError::BlockAll => TokenBucketError::AllBlocked(AllBlockedError {
                    token_bucket_key: String::new(),
                }),
                other => TokenBucketError::Unexpected(UnexpectedRateLimitReturnError {
                    token_bucket_key: other.to_string(),
                }),
            })
    }

    async fn set_token_bucket_config(
        &self,
        bucket_quota: i32,
        bucket_replenish_interval: Duration,
    ) -> Result<(), String> {
        // `inMemoryLimiter.SetTokenBucketConfig`:
        // `rate := rate.Limit(bucketQuota) / rate.Limit(interval.Seconds())`.
        let seconds = bucket_replenish_interval.as_secs_f64();
        let limit = if seconds == 0.0 {
            f64::INFINITY
        } else {
            bucket_quota as f64 / seconds
        };
        self.limiter.set_limit_and_burst(limit, DEFAULT_BURST);
        Ok(())
    }
}

/// `getInMemoryLimiter`'s process-wide registry.
fn in_memory_limiters() -> &'static Mutex<HashMap<String, std::sync::Arc<InMemoryGlobalLimiter>>> {
    static REGISTRY: OnceLock<
        Mutex<HashMap<String, std::sync::Arc<InMemoryGlobalLimiter>>>,
    > = OnceLock::new();
    REGISTRY.get_or_init(|| Mutex::new(HashMap::new()))
}

/// `ratelimit.getInMemoryLimiter`: one limiter per bucket name.
pub fn get_in_memory_limiter(name: &str, default_per_hour: i64) -> std::sync::Arc<InMemoryGlobalLimiter> {
    let mut registry = in_memory_limiters().lock().expect("in-memory limiters");
    registry
        .entry(name.to_owned())
        .or_insert_with(|| std::sync::Arc::new(InMemoryGlobalLimiter::new(default_per_hour)))
        .clone()
}

// --- Monitor ---------------------------------------------------------------

/// `ratelimit.normaliseURL`.
pub fn normalise_url(raw_url: &str) -> String {
    match url::Url::parse(raw_url) {
        Ok(mut parsed) => {
            if let Some(host) = parsed.host_str() {
                let lowered = host.to_ascii_lowercase();
                if lowered != host {
                    let _ = parsed.set_host(Some(&lowered));
                }
            }
            let path = parsed.path().to_owned();
            if !path.ends_with('/') {
                parsed.set_path(&format!("{path}/"));
            }
            parsed.to_string()
        }
        Err(_) => {
            // `url::Url::parse` rejects scheme-less references that Go's
            // `url.Parse` accepts. If the input looks absolute, assume Go hit a
            // parse error too and return it lower-cased (Go's fallback). If it
            // is a relative reference, apply Go's normalisation by hand: lower
            // the host, append `/` to the path, and escape with `encodePath`.
            if raw_url.contains("://") || looks_absolute(raw_url) {
                raw_url.to_ascii_lowercase()
            } else {
                normalise_relative_url(raw_url)
            }
        }
    }
}

/// Whether `raw_url` starts with `scheme:` (Go's `getScheme` grammar).
fn looks_absolute(raw_url: &str) -> bool {
    match raw_url.find(':') {
        Some(index) if index > 0 => {
            let scheme = &raw_url[..index];
            let mut chars = scheme.chars();
            chars.next().is_some_and(|c| c.is_ascii_alphabetic())
                && chars.all(|c| c.is_ascii_alphanumeric() || matches!(c, '+' | '-' | '.'))
        }
        _ => false,
    }
}

/// Go's `escape(_, encodePath)`: unreserved characters plus the path-safe
/// reserved set stay literal; everything else is `%XX` (uppercase) per byte.
fn go_escape_path(path: &str) -> String {
    let mut out = String::with_capacity(path.len());
    for byte in path.bytes() {
        let allowed = byte.is_ascii_alphanumeric()
            || matches!(
                byte,
                b'-' | b'_'
                    | b'.'
                    | b'~'
                    | b'$'
                    | b'&'
                    | b'+'
                    | b','
                    | b'/'
                    | b':'
                    | b';'
                    | b'='
                    | b'@'
            );
        if allowed {
            out.push(byte as char);
        } else {
            out.push_str(&format!("%{byte:02X}"));
        }
    }
    out
}

/// `normaliseURL` for a scheme-less reference: split off fragment/query, treat
/// a leading `//authority` as the host, lower-case it, append `/` to the path
/// and re-escape it the way Go's `URL.String` does.
fn normalise_relative_url(raw_url: &str) -> String {
    let (before_fragment, fragment) = match raw_url.split_once('#') {
        Some((before, after)) => (before, Some(after)),
        None => (raw_url, None),
    };
    let (path_part, query) = match before_fragment.split_once('?') {
        Some((before, after)) => (before, Some(after)),
        None => (before_fragment, None),
    };
    let (host, raw_path) = match path_part.strip_prefix("//") {
        Some(rest) => match rest.split_once('/') {
            Some((host, path)) => (Some(host.to_ascii_lowercase()), format!("/{path}")),
            None => (Some(rest.to_ascii_lowercase()), String::new()),
        },
        None => (None, path_part.to_owned()),
    };
    let mut path = raw_path;
    if !path.ends_with('/') {
        path.push('/');
    }

    let mut out = String::new();
    if let Some(host) = host {
        out.push_str("//");
        out.push_str(&host);
    }
    out.push_str(&go_escape_path(&path));
    if let Some(query) = query {
        out.push('?');
        out.push_str(query);
    }
    if let Some(fragment) = fragment {
        out.push('#');
        out.push_str(fragment);
    }
    out
}

/// `ratelimit.MetricsCollector`.
#[derive(Default)]
pub struct MetricsCollector {
    pub remaining: Option<Box<dyn Fn(f64) + Send + Sync>>,
    pub wait_duration: Option<Box<dyn Fn(Duration) + Send + Sync>>,
}

/// `ratelimit.Monitor`.
pub struct Monitor {
    pub header_prefix: String,
    known: bool,
    limit: i64,
    remaining: i64,
    reset: Option<Duration>,
    retry: Option<Duration>,
    collector: Option<MetricsCollector>,
    clock: Option<Box<dyn Fn() -> Duration + Send + Sync>>,
}

impl Default for Monitor {
    fn default() -> Self {
        Self {
            header_prefix: String::new(),
            known: false,
            limit: 0,
            remaining: 0,
            reset: None,
            retry: None,
            collector: None,
            clock: None,
        }
    }
}

fn header_value<'a>(headers: &'a [(String, String)], name: &str) -> Option<&'a str> {
    headers
        .iter()
        .find(|(key, _)| key.eq_ignore_ascii_case(name))
        .map(|(_, value)| value.as_str())
}

fn signed_duration(later: Option<Duration>, now: Duration) -> i64 {
    match later {
        Some(later) => {
            let diff = later.as_nanos() as i128 - now.as_nanos() as i128;
            if diff > i64::MAX as i128 {
                i64::MAX
            } else if diff < i64::MIN as i128 {
                i64::MIN
            } else {
                diff as i64
            }
        }
        // Go's zero `time.Time` minus a modern time saturates to MinInt64.
        None => i64::MIN,
    }
}

impl Monitor {
    /// `Monitor.clock` (test hook) / `time.Now`.
    pub fn with_clock<F>(mut self, clock: F) -> Self
    where
        F: Fn() -> Duration + Send + Sync + 'static,
    {
        self.clock = Some(Box::new(clock));
        self
    }

    pub fn set_collector(&mut self, collector: MetricsCollector) {
        self.collector = Some(collector);
    }

    fn now(&self) -> Duration {
        match &self.clock {
            Some(clock) => clock(),
            None => std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default(),
        }
    }

    /// `Monitor.Get`: `(remaining, reset-now, retry-now, known)` in
    /// nanoseconds (Go `time.Duration`).
    pub fn get(&self) -> (i64, i64, i64, bool) {
        let now = self.now();
        (
            self.remaining,
            signed_duration(self.reset, now),
            signed_duration(self.retry, now),
            self.known,
        )
    }

    /// `Monitor.Update`.
    pub fn update(&mut self, headers: &[(String, String)]) {
        if header_value(headers, "X-From-Cache").map(|v| !v.is_empty()).unwrap_or(false) {
            // Cached responses have stale RateLimit headers.
            return;
        }

        let now = self.now();
        if let Some(retry) = header_value(headers, "Retry-After")
            .and_then(|value| value.trim().parse::<i64>().ok())
            .filter(|value| *value > 0)
        {
            self.retry = Some(now + Duration::from_secs(retry as u64));
        }

        let limit = header_value(headers, &format!("{}RateLimit-Limit", self.header_prefix))
            .and_then(|value| value.trim().parse::<i64>().ok());
        let remaining =
            header_value(headers, &format!("{}RateLimit-Remaining", self.header_prefix))
                .and_then(|value| value.trim().parse::<i64>().ok());
        let reset = header_value(headers, &format!("{}RateLimit-Reset", self.header_prefix))
            .and_then(|value| value.trim().parse::<i64>().ok());

        let (Some(limit), Some(remaining), Some(reset)) = (limit, remaining, reset) else {
            self.known = false;
            return;
        };

        self.known = true;
        self.limit = limit;
        self.remaining = remaining;
        self.reset = Some(Duration::from_secs(reset.max(0) as u64));

        if let Some(collector) = &self.collector {
            if let Some(remaining_fn) = &collector.remaining {
                remaining_fn(self.remaining as f64);
            }
        }
    }

    /// `Monitor.RecommendedWaitForBackgroundOp`.
    pub fn recommended_wait_for_background_op(&self, cost: i64) -> i64 {
        let now = self.now();
        if let Some(retry) = self.retry {
            let remaining = signed_duration(Some(retry), now);
            if remaining > 0 {
                return remaining;
            }
        }

        if !self.known {
            return 0;
        }

        let mut limit_remaining = self.remaining as f64;
        let mut reset_at = self.reset.unwrap_or(Duration::ZERO);
        if now > reset_at {
            limit_remaining = self.limit as f64;
            reset_at = now + Duration::from_secs(3600);
        }

        // Be conservative.
        limit_remaining *= 0.8;
        let mut time_remaining = signed_duration(Some(reset_at), now)
            .saturating_add(Duration::from_secs(180).as_nanos() as i64);

        let n = if cost == 0 {
            f64::INFINITY
        } else {
            limit_remaining / cost as f64
        };
        if n < 1.0 {
            return time_remaining;
        }
        if n > 500.0 {
            return 0;
        }
        if n > 250.0 {
            return Duration::from_millis(200).as_nanos() as i64;
        }
        // `timeRemaining * time.Duration(cost) / time.Duration(limitRemaining)`:
        // the float64 conversion truncates toward zero, and Go's `time.Duration`
        // multiply/add wrap on int64 overflow (two's complement) rather than
        // saturating, so mirror that here.
        let denominator = limit_remaining as i64;
        if denominator == 0 {
            return time_remaining;
        }
        time_remaining = time_remaining.wrapping_mul(cost).wrapping_div(denominator);
        time_remaining
    }

    /// `Monitor.calcRateLimitWaitTime`.
    pub fn calc_rate_limit_wait_time(&self, cost: i64) -> i64 {
        let now = self.now();
        if let Some(retry) = self.retry {
            let remaining = signed_duration(Some(retry), now);
            if remaining > 0 {
                return remaining;
            }
        }

        if !self.known || self.remaining >= cost || cost > self.limit {
            return 0;
        }

        if let Some(reset) = self.reset {
            let remaining = signed_duration(Some(reset), now);
            if remaining > 0 {
                return remaining;
            }
        }
        0
    }

    /// `Monitor.WaitForRateLimit`: sleeps when a wait is warranted and reports
    /// whether rate limiting applied.
    pub async fn wait_for_rate_limit(&self, cost: i64) -> bool {
        let sleep = self.calc_rate_limit_wait_time(cost);
        if sleep == 0 {
            return false;
        }
        tokio::time::sleep(Duration::from_nanos(sleep.max(0) as u64)).await;
        true
    }
}

/// `ratelimit.MonitorRegistry`.
#[derive(Default)]
pub struct MonitorRegistry {
    monitors: Mutex<HashMap<String, std::sync::Arc<Monitor>>>,
}

impl MonitorRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// `MonitorRegistry.GetOrSet`.
    pub fn get_or_set(
        &self,
        base_url: &str,
        auth_hash: &str,
        resource: &str,
        monitor: std::sync::Arc<Monitor>,
    ) -> std::sync::Arc<Monitor> {
        let base_url = normalise_url(base_url);
        let mut key = format!("{base_url}:{auth_hash}");
        if !resource.is_empty() {
            key.push(':');
            key.push_str(resource);
        }
        let mut monitors = self.monitors.lock().expect("monitor registry");
        monitors
            .entry(key)
            .or_insert(monitor)
            .clone()
    }

    /// `MonitorRegistry.Count`.
    pub fn count(&self) -> usize {
        self.monitors.lock().expect("monitor registry").len()
    }
}

/// `time.Duration.String()` for the whole-second durations the Lua script
/// returns (e.g. `1m0s`, `45s`). Go prints `0s` for zero.
pub fn go_duration_string(duration: Duration) -> String {
    let total = duration.as_secs();
    if total == 0 {
        return "0s".to_owned();
    }
    let hours = total / 3600;
    let minutes = (total % 3600) / 60;
    let seconds = total % 60;
    let mut out = String::new();
    if hours > 0 {
        out.push_str(&format!("{hours}h"));
    }
    if minutes > 0 {
        out.push_str(&format!("{minutes}m"));
    }
    if seconds > 0 {
        out.push_str(&format!("{seconds}s"));
    }
    out
}
