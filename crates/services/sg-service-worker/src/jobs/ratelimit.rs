#![forbid(unsafe_code)]

//! `cmd/worker/internal/ratelimit` + the configuration-writing half of
//! `internal/ratelimit.GlobalLimiter`: the `rate-limit-config` job.
//!
//! Every 30 seconds the job copies the rate-limit configuration into Redis:
//!
//! * the `git-rps` bucket gets the site's
//!   `gitMaxCodehostRequestsPerSecond` (default `-1`, i.e. `rate.Inf`) with a
//!   one-second replenishment interval;
//! * every external service gets a `extsvc:<kind>:<id>` bucket whose rate is
//!   its configured `rateLimit.requestsPerHour` (or the code-host default),
//!   stored as requests *per hour*, with a one-hour replenishment interval.
//!
//! The bucket layout is `v2:rate_limiters:<bucket>` with the `config:bucket_rate`,
//! `config:bucket_replenishment_interval_seconds` and `allowed_burst` keys,
//! each with a 24h TTL — the same three keys the baseline's
//! `setTokenBucketReplenishmentLuaScript` writes.
//!
//! The bucket configuration is written to `REDIS_STORE_ENDPOINT`
//! (`redispool.Store`, `ratelimit.kv()`), not the cache pool.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use serde_json::Value;
use sg_config::LiveConfig;
use sg_db::external_services::{ExternalService, ExternalServiceStore};
use sg_extsvc::ratelimit::{get_limit_from_config, Variant};
use sg_extsvc::urn;
use sg_keyring::Keyring;
use sg_redis::KeyValue;
use tokio::sync::OnceCell;
use tokio_util::sync::CancellationToken;

use super::{Job, JobDeps};
use crate::config::combine_errors;
use crate::framework::periodic::{Handler, HandlerError, PeriodicRoutine};
use crate::framework::BackgroundRoutine;

/// `ratelimit.tokenBucketGlobalPrefix`.
pub const TOKEN_BUCKET_GLOBAL_PREFIX: &str = "v2:rate_limiters";
/// `ratelimit.GitRPSLimiterBucketName`.
pub const GIT_RPS_LIMITER_BUCKET_NAME: &str = "git-rps";
pub const BUCKET_LAST_REPLENISHMENT_TIMESTAMP_KEY_SUFFIX: &str = "last_replenishment_timestamp";
pub const BUCKET_ALLOWED_BURST_KEY_SUFFIX: &str = "allowed_burst";
pub const BUCKET_RATE_CONFIG_KEY_SUFFIX: &str = "config:bucket_rate";
pub const BUCKET_REPLENISHMENT_CONFIG_KEY_SUFFIX: &str =
    "config:bucket_replenishment_interval_seconds";
/// `ratelimit.defaultBurst`.
pub const DEFAULT_BURST: i64 = 10;
/// `SET key value EX 86400` in the baseline's Lua script.
pub const CONFIG_EXPIRY_SECONDS: i64 = 86_400;

pub const ROUTINE_NAME: &str = "rate_limit_config_worker";
pub const ROUTINE_DESCRIPTION: &str =
    "copies the rate limit configurations from the database to Redis";
pub const ROUTINE_INTERVAL: Duration = Duration::from_secs(30);

/// `rateLimitConfigJob.Description`.
pub const JOB_DESCRIPTION: &str =
    "Copies the rate limit configurations from the database to Redis.";

/// `ratelimit.rateLimitBucketConfigKeys`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RateLimitBucketConfigKeys {
    /// `v2:rate_limiters:<bucket>`.
    pub bucket_key: String,
    /// `...:config:bucket_rate`.
    pub rate_key: String,
    /// `...:config:bucket_replenishment_interval_seconds`.
    pub replenishment_interval_seconds_key: String,
    /// `...:last_replenishment_timestamp`.
    pub last_replenishment_timestamp_key: String,
    /// `...:allowed_burst`.
    pub burst_key: String,
}

/// `ratelimit.getRateLimiterKeys`.
pub fn rate_limiter_keys(prefix: &str, bucket_name: &str) -> RateLimitBucketConfigKeys {
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

/// `GlobalLimiter.SetTokenBucketConfig`, the only limiter method worker calls.
#[async_trait]
pub trait GlobalLimiter: Send + Sync {
    async fn set_token_bucket_config(
        &self,
        bucket_quota: i32,
        bucket_replenish_interval: Duration,
    ) -> Result<(), String>;
}

/// `handler.newRateLimiterFunc`.
pub type NewRateLimiterFunc = Arc<dyn Fn(String) -> Arc<dyn GlobalLimiter> + Send + Sync>;

/// `setReplenishmentScript.DoContext`: three `SET ... EX 86400` writes.
///
/// The baseline performs them inside one Lua script; issuing three `SETEX`
/// commands leaves the same state, but the three keys are visible
/// individually for a few hundred microseconds. Recorded as an erratum in
/// docs/modules/06_worker.md.
pub async fn set_token_bucket_config(
    kv: &KeyValue,
    prefix: &str,
    bucket_name: &str,
    bucket_quota: i32,
    bucket_replenish_interval: Duration,
) -> Result<(), String> {
    let keys = rate_limiter_keys(prefix, bucket_name);
    kv.set_ex(
        &keys.rate_key,
        CONFIG_EXPIRY_SECONDS,
        bucket_quota.to_string(),
    )
    .await
    .map_err(|err| err.to_string())?;
    kv.set_ex(
        &keys.replenishment_interval_seconds_key,
        CONFIG_EXPIRY_SECONDS,
        go_format_float(bucket_replenish_interval.as_secs_f64()),
    )
    .await
    .map_err(|err| err.to_string())?;
    kv.set_ex(
        &keys.burst_key,
        CONFIG_EXPIRY_SECONDS,
        DEFAULT_BURST.to_string(),
    )
    .await
    .map_err(|err| err.to_string())?;
    Ok(())
}

/// redigo renders a float64 argument with `strconv.FormatFloat(v, 'g', -1,
/// 64)`, which has no decimal point for integral values. Rust's `Display` for
/// f64 does the same for the interval values this job writes (`1`, `3600`).
fn go_format_float(value: f64) -> String {
    format!("{value}")
}

/// A `KeyValue` that dials on first use, like `redispool.Store`'s lazy pool.
/// A failed dial is retried on the next call.
struct LazyStore {
    endpoint: String,
    cell: OnceCell<KeyValue>,
}

impl LazyStore {
    async fn get(&self) -> Result<&KeyValue, String> {
        self.cell
            .get_or_try_init(|| async {
                KeyValue::connect(&self.endpoint)
                    .await
                    .map_err(|err| err.to_string())
            })
            .await
    }
}

/// `ratelimit.globalRateLimiter` for the configuration-writing methods.
struct RedisGlobalLimiter {
    store: Arc<LazyStore>,
    prefix: String,
    bucket_name: String,
}

#[async_trait]
impl GlobalLimiter for RedisGlobalLimiter {
    async fn set_token_bucket_config(
        &self,
        bucket_quota: i32,
        bucket_replenish_interval: Duration,
    ) -> Result<(), String> {
        let kv = self.store.get().await?;
        set_token_bucket_config(
            kv,
            &self.prefix,
            &self.bucket_name,
            bucket_quota,
            bucket_replenish_interval,
        )
        .await
        .map_err(|err| {
            format!(
                "error while setting token bucket replenishment for bucket {}: {err}",
                self.bucket_name
            )
        })
    }
}

/// `handler.newRateLimiterFunc` for the process: `NewGlobalRateLimiter` over
/// `redispool.Store` with the given prefix.
pub fn redis_rate_limiter_factory(endpoint: String, prefix: String) -> NewRateLimiterFunc {
    let store = Arc::new(LazyStore {
        endpoint,
        cell: OnceCell::new(),
    });
    Arc::new(move |bucket_name: String| -> Arc<dyn GlobalLimiter> {
        Arc::new(RedisGlobalLimiter {
            store: Arc::clone(&store),
            prefix: prefix.clone(),
            bucket_name,
        })
    })
}

/// `rateLimitConfigJob`.
pub struct RateLimitConfigJob;

impl RateLimitConfigJob {
    pub fn new() -> Self {
        Self
    }
}

impl Default for RateLimitConfigJob {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Job for RateLimitConfigJob {
    fn description(&self) -> String {
        JOB_DESCRIPTION.to_owned()
    }

    async fn routines(
        &self,
        deps: &JobDeps,
        _setup_ctx: CancellationToken,
    ) -> Result<Vec<BackgroundRoutine>, String> {
        let handler = RateLimitConfigHandler::new(
            ExternalServiceStore::new(deps.frontend.clone()),
            Arc::clone(&deps.keyring),
            Arc::clone(&deps.live_config),
            redis_rate_limiter_factory(
                sg_redis::store_endpoint(),
                TOKEN_BUCKET_GLOBAL_PREFIX.to_owned(),
            ),
        );
        Ok(vec![PeriodicRoutine::new(Arc::new(handler))
            .with_name(ROUTINE_NAME)
            .with_description(ROUTINE_DESCRIPTION)
            .with_interval(ROUTINE_INTERVAL)
            .into()])
    }
}

/// `ratelimit.handler`.
pub struct RateLimitConfigHandler {
    external_services: ExternalServiceStore,
    keyring: Arc<Keyring>,
    live_config: Arc<LiveConfig>,
    new_rate_limiter: NewRateLimiterFunc,
}

impl RateLimitConfigHandler {
    pub fn new(
        external_services: ExternalServiceStore,
        keyring: Arc<Keyring>,
        live_config: Arc<LiveConfig>,
        new_rate_limiter: NewRateLimiterFunc,
    ) -> Self {
        Self {
            external_services,
            keyring,
            live_config,
            new_rate_limiter,
        }
    }

    /// `handler.Handle`'s return value; the caller logs it.
    pub async fn handle_once(&self) -> Result<(), MultiError> {
        let site = self.live_config.current();
        // -1 is `rate.Inf`: unlimited.
        let mut default_git_quota: i32 = -1;
        if let Some(quota) = site.site.git_max_codehost_requests_per_second() {
            default_git_quota = quota as i32;
        }

        let git_rate_limiter = (self.new_rate_limiter)(GIT_RPS_LIMITER_BUCKET_NAME.to_owned());
        git_rate_limiter
            .set_token_bucket_config(default_git_quota, Duration::from_secs(1))
            .await
            .map_err(MultiError::single)?;

        let services = self
            .external_services
            .list()
            .await
            .map_err(|err| MultiError::single(err.to_string()))?;

        let errs = sync_services(&self.keyring, &services, &self.new_rate_limiter).await;
        if errs.is_empty() {
            Ok(())
        } else {
            Err(MultiError(errs))
        }
    }
}

/// `lib/errors.MultiError`: the appends `syncServices` collects. `Display`
/// is the worker's `N errors occurred:` text (`combine_errors`); a single
/// entry renders as its bare message, like `errors.Append(nil, err)`'s
/// one-element list does for `Handler.Handle`'s early returns.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MultiError(pub Vec<String>);

impl MultiError {
    fn single(message: String) -> Self {
        Self(vec![message])
    }
}

impl std::fmt::Display for MultiError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match combine_errors(&self.0) {
            Some(message) => f.write_str(&message),
            None => Ok(()),
        }
    }
}

impl std::error::Error for MultiError {}

#[async_trait]
impl Handler for RateLimitConfigHandler {
    async fn handle(&self, _ctx: CancellationToken) -> Result<(), HandlerError> {
        self.handle_once()
            .await
            .map_err(|err| HandlerError::new(err.to_string()))
    }

    fn handle_error(&self, error: &HandlerError) {
        // `defer func() { if err != nil { h.logger.Error(...) } }()`.
        tracing::error!(error = %error, "failed to sync rate limit configs to redis");
    }
}

/// `syncServices`: copy one bucket per service, skipping kinds whose rate
/// limit is not modelled and collecting every other failure.
pub async fn sync_services(
    keyring: &Keyring,
    services: &[ExternalService],
    new_rate_limiter: &NewRateLimiterFunc,
) -> Vec<String> {
    let mut errs: Vec<String> = Vec::new();
    for service in services {
        let limit = match extract_encryptable_rate_limit(keyring, service).await {
            Ok(Some(limit)) => limit,
            // `errors.HasType[extsvc.ErrRateLimitUnsupported]`: skip silently.
            Ok(None) => continue,
            Err(err) => {
                errs.push(format!("getting rate limit configuration: {err}"));
                continue;
            }
        };

        let rate_limiter = new_rate_limiter(urn(&service.kind, service.id));
        // `rate.Inf` is stored as -1; configured limits are per hour.
        let mut lim: i32 = -1;
        if limit != f64::INFINITY {
            lim = (limit * 3600.0) as i32;
        }
        if let Err(err) = rate_limiter
            .set_token_bucket_config(lim, Duration::from_secs(3600))
            .await
        {
            errs.push(err);
        }
    }
    errs
}

/// `extsvc.ExtractEncryptableRateLimit`: `Ok(None)` for an unsupported code
/// host, `Err` for a configuration that cannot be read.
async fn extract_encryptable_rate_limit(
    keyring: &Keyring,
    service: &ExternalService,
) -> Result<Option<f64>, String> {
    // `ParseEncryptableConfig` resolves the schema prototype before it
    // decrypts, so an unknown kind is reported first.
    let Some(variant) = Variant::from_kind(&service.kind) else {
        return Err(format!(
            "loading service configuration: unknown external service kind {:?}",
            service.kind
        ));
    };

    let config = sg_encryption::maybe_decrypt(
        keyring.external_service_key().as_deref(),
        &service.config,
        &service.encryption_key_id,
    )
    .await
    .map_err(|err| format!("loading service configuration: {err}"))?;

    let config = parse_config(&config)
        .map_err(|err| format!("loading service configuration: failed to parse JSON: {err}"))?;

    match get_limit_from_config(&config, variant) {
        Ok((limit, _is_default)) => Ok(Some(limit)),
        Err(_unsupported) => Ok(None),
    }
}

/// `jsonc.Unmarshal`'s comment/trailing-comma tolerance, then `encoding/json`.
fn parse_config(config: &str) -> Result<Value, String> {
    let stripped = sg_config::strip_jsonc(config);
    serde_json::from_str(&stripped).map_err(|err| err.to_string())
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn bucket_keys_match_the_baseline_layout() {
        let keys = rate_limiter_keys("v2:rate_limiters", "git-rps");
        assert_eq!(keys.bucket_key, "v2:rate_limiters:git-rps");
        assert_eq!(keys.rate_key, "v2:rate_limiters:git-rps:config:bucket_rate");
        assert_eq!(
            keys.replenishment_interval_seconds_key,
            "v2:rate_limiters:git-rps:config:bucket_replenishment_interval_seconds"
        );
        assert_eq!(
            keys.last_replenishment_timestamp_key,
            "v2:rate_limiters:git-rps:last_replenishment_timestamp"
        );
        assert_eq!(keys.burst_key, "v2:rate_limiters:git-rps:allowed_burst");
    }

    #[test]
    fn go_float_formatting_has_no_decimal_point() {
        assert_eq!(go_format_float(1.0), "1");
        assert_eq!(go_format_float(3600.0), "3600");
        assert_eq!(go_format_float(1.5), "1.5");
    }

    #[test]
    fn job_metadata_matches_the_baseline() {
        let job = RateLimitConfigJob::new();
        assert_eq!(
            job.description(),
            "Copies the rate limit configurations from the database to Redis."
        );
        assert!(job.configs().is_empty());
        assert_eq!(ROUTINE_NAME, "rate_limit_config_worker");
        assert_eq!(
            ROUTINE_DESCRIPTION,
            "copies the rate limit configurations from the database to Redis"
        );
        assert_eq!(ROUTINE_INTERVAL, Duration::from_secs(30));
    }

    #[test]
    fn unknown_kinds_are_reported_and_unsupported_kinds_are_skipped() {
        let keyring = Keyring::default();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("runtime");
        runtime.block_on(async {
            let unknown = ExternalService {
                id: 7,
                kind: "FOO".to_owned(),
                display_name: "foo".to_owned(),
                config: "{}".to_owned(),
                encryption_key_id: String::new(),
                last_sync_at: None,
                next_sync_at: None,
                unrestricted: false,
                has_webhooks: None,
                code_host_id: None,
            };
            let err = extract_encryptable_rate_limit(&keyring, &unknown)
                .await
                .expect_err("unknown kind");
            assert_eq!(
                err,
                "loading service configuration: unknown external service kind \"FOO\""
            );

            let perforce = ExternalService {
                kind: "PERFORCE".to_owned(),
                ..unknown.clone()
            };
            assert_eq!(
                extract_encryptable_rate_limit(&keyring, &perforce).await,
                Ok(None)
            );

            let github = ExternalService {
                kind: "GITHUB".to_owned(),
                config: json!({"rateLimit": {"enabled": true, "requestsPerHour": 150}}).to_string(),
                ..unknown
            };
            assert_eq!(
                extract_encryptable_rate_limit(&keyring, &github).await,
                Ok(Some(150.0 / 3600.0))
            );
        });
    }

    #[test]
    fn configs_accept_jsonc_comments_and_trailing_commas() {
        let config = parse_config(
            r#"{
                // the rate limit
                "rateLimit": {"enabled": true, "requestsPerHour": 3600,},
            }"#,
        )
        .expect("parse");
        assert_eq!(
            get_limit_from_config(&config, Variant::GitHub),
            Ok((1.0, false))
        );
    }

    #[test]
    fn multierror_text_matches_the_worker_convention() {
        assert_eq!(combine_errors(&[]), None);
        assert_eq!(combine_errors(&["a".to_owned()]).as_deref(), Some("a"));
        assert_eq!(
            combine_errors(&["a".to_owned(), "b".to_owned()]).as_deref(),
            Some("2 errors occurred:\n\t* a\n\t* b")
        );
        // `errorMultiError`'s Display is `combine_errors`; one item renders
        // as its bare message, like `Handle`'s early returns.
        assert_eq!(MultiError(vec!["a".to_owned()]).to_string(), "a");
        assert_eq!(
            MultiError(vec!["a".to_owned(), "b".to_owned()]).to_string(),
            "2 errors occurred:\n\t* a\n\t* b"
        );
        assert_eq!(MultiError(Vec::new()).to_string(), "");
    }
}
