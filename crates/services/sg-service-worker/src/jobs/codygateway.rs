#![forbid(unsafe_code)]

//! `cmd/worker/internal/codygateway/usageworker.go`: the
//! `cody-gateway-usage-check` job.
//!
//! The job owns no configuration (`Config() == nil`) and builds one
//! `PeriodicGoroutine`:
//!
//! | field | value |
//! |---|---|
//! | name | `CodyGatewayUsageWorker` |
//! | description | `Fetches current usage data from Cody Gateway to render alerts about LLM token exhaustion` |
//! | interval | 30 minutes |
//!
//! Every run resolves the completions configuration from the live site
//! configuration (`conf.GetCompletionsConfig`). When Cody is not served by
//! Cody Gateway the run is a no-op that logs `Not checking Cody Gateway usage,
//! disabled`. Otherwise the worker calls `GET <endpoint>/v1/limits` with a
//! bearer token, keeps the three known features (`code_completions`,
//! `chat_completions`, `embeddings`), and writes
//! `v1:cody_gateway_usage_percent:<feature>` = percentage used with a TTL of
//! 35 minutes, shortened to the time until the limit resets when that is
//! sooner (and to 1 second when the reset time has already passed).
//!
//! Not ported: the `observation.Op` RED metrics registered as
//! `cody_gateway_usage_worker`; the tracing/metrics plumbing is not part of
//! the Rust worker yet (see docs/modules/06_worker.md).
//!
//! Errata:
//!
//! * `httpcli.ExternalDoer` (proxy environment, TLS settings) is replaced by
//!   the minimal [`sg_http::HttpClient`]; its connector does not read
//!   `HTTPS_PROXY`/`NO_PROXY`.
//! * `GetCompletionsConfig` resolves per-provider model defaults that this job
//!   never reads; only the gateway-relevant subset (whether Cody is enabled,
//!   the provider name, and the endpoint/access token used when the provider
//!   is `sourcegraph`) is reproduced. For every other provider the job only
//!   observes `cc.Provider != "sourcegraph"`, so the missing defaults cannot
//!   change behaviour.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use sg_config::{LiveConfig, SiteConfiguration};
use sg_http::HttpClient;
use sg_redis::KeyValue;
use sha2::Digest;
use tokio::sync::OnceCell;
use tokio_util::sync::CancellationToken;

use super::{Job, JobDeps};
use crate::framework::periodic::{Handler, HandlerError, PeriodicRoutine};
use crate::framework::BackgroundRoutine;

/// `usageJob.Description`.
pub const JOB_DESCRIPTION: &str =
    "Background worker occasionally reading Cody Gateway usage and writing to redis.";

/// `goroutine.WithName("CodyGatewayUsageWorker")`.
pub const ROUTINE_NAME: &str = "CodyGatewayUsageWorker";

/// `goroutine.WithDescription(...)`.
pub const ROUTINE_DESCRIPTION: &str =
    "Fetches current usage data from Cody Gateway to render alerts about LLM token exhaustion";

/// `checkIntervalMinutes = 30`.
pub const INTERVAL: Duration = Duration::from_secs(30 * 60);

/// `redisTTLMinutes = 35`.
pub const REDIS_TTL_SECONDS: i64 = 35 * 60;

/// `codygateway.CodyGatewayUsageRedisKeyPrefix`.
pub const REDIS_KEY_PREFIX: &str = "v1:cody_gateway_usage_percent";

/// `conf.CodyGatewayProdEndpoint`.
pub const CODY_GATEWAY_PROD_ENDPOINT: &str = "https://cody-gateway.sourcegraph.com";

/// `license.LicenseKeyBasedAccessTokenPrefix`.
pub const LICENSE_KEY_BASED_ACCESS_TOKEN_PREFIX: &str = "slk_";

/// `types.CompletionsFeatureCode`.
pub const FEATURE_CODE_COMPLETIONS: &str = "code_completions";
/// `types.CompletionsFeatureChat`.
pub const FEATURE_CHAT_COMPLETIONS: &str = "chat_completions";
/// `codygateway.FeatureEmbeddings`.
pub const FEATURE_EMBEDDINGS: &str = "embeddings";

/// `codygateway.Feature.IsValid`.
pub fn is_valid_feature(feature: &str) -> bool {
    matches!(
        feature,
        FEATURE_CODE_COMPLETIONS | FEATURE_CHAT_COMPLETIONS | FEATURE_EMBEDDINGS
    )
}

/// `cmd/worker/internal/codygateway.usageJob`.
pub struct CodyGatewayUsageCheck;

#[async_trait]
impl Job for CodyGatewayUsageCheck {
    fn description(&self) -> String {
        JOB_DESCRIPTION.to_owned()
    }

    async fn routines(
        &self,
        deps: &JobDeps,
        _setup_ctx: CancellationToken,
    ) -> Result<Vec<BackgroundRoutine>, String> {
        let handler = Arc::new(CodyGatewayUsageHandler::new(Arc::clone(&deps.live_config)));
        Ok(vec![PeriodicRoutine::new(handler)
            .with_name(ROUTINE_NAME)
            .with_description(ROUTINE_DESCRIPTION)
            .with_interval(INTERVAL)
            .into()])
    }
}

/// `codygateway.LimitStatus`, including the feature name that is not part of
/// the JSON payload.
#[derive(Debug, Clone, PartialEq)]
pub struct LimitStatus {
    pub feature: String,
    /// `limit`.
    pub interval_limit: i64,
    /// `usage`.
    pub interval_usage: i64,
    /// `interval`.
    pub time_interval: String,
    /// `expiry`: Go decodes it with `time.Time`'s RFC3339 parser.
    pub expiry: Option<sg_db::OffsetDateTime>,
}

impl LimitStatus {
    /// `LimitStatus.PercentUsed`: `math.Ceil(usage/limit*100)`, or 100 when the
    /// limit is zero.
    pub fn percent_used(&self) -> i64 {
        if self.interval_limit == 0 {
            return 100;
        }
        ((self.interval_usage as f64) / (self.interval_limit as f64) * 100.0).ceil() as i64
    }
}

/// The JSON payload of one feature before the feature name and expiry are
/// decoded (`time.Time` only accepts RFC3339).
#[derive(Debug, serde::Deserialize)]
struct RawLimitStatus {
    #[serde(default)]
    limit: i64,
    #[serde(default)]
    usage: i64,
    #[serde(default)]
    interval: String,
    #[serde(default)]
    expiry: Option<String>,
}

/// `Client.GetLimits`'s decode-and-filter half: unknown features are dropped
/// and the result is sorted by feature name (Go sorts because the JSON object
/// is a map).
pub fn parse_limits(body: &[u8]) -> Result<Vec<LimitStatus>, String> {
    let raw: HashMap<String, RawLimitStatus> =
        serde_json::from_slice(body).map_err(|err| err.to_string())?;
    let mut limits = Vec::with_capacity(raw.len());
    for (feature, limit) in raw {
        if !is_valid_feature(&feature) {
            continue;
        }
        let expiry = match limit.expiry.as_deref() {
            Some(raw) if !raw.is_empty() => Some(
                sg_db::OffsetDateTime::parse(raw, &time::format_description::well_known::Rfc3339)
                    .map_err(|err| format!("parsing time {raw:?} as RFC3339: {err}"))?,
            ),
            _ => {
                // Go's `time.Time` unmarshals a JSON null into the zero value;
                // the field is a `*time.Time`, so null means nil. An empty
                // string is treated as absent here (the baseline errors on it).
                None
            }
        };
        limits.push(LimitStatus {
            feature,
            interval_limit: limit.limit,
            interval_usage: limit.usage,
            time_interval: limit.interval,
            expiry,
        });
    }
    limits.sort_by(|a, b| a.feature.cmp(&b.feature));
    Ok(limits)
}

/// The subset of `conftypes.CompletionsConfig` this job reads.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompletionsConfig {
    pub provider: String,
    pub endpoint: String,
    pub access_token: String,
}

/// `conf.newCodyEnabled`: only the explicit `cody.enabled` flag.
fn new_cody_enabled(site: &SiteConfiguration) -> bool {
    site.raw()
        .get("cody.enabled")
        .and_then(serde_json::Value::as_bool)
        == Some(true)
}

/// `conf.codyEnabled`.
fn cody_enabled(site: &SiteConfiguration) -> bool {
    let raw = site.raw();
    let enabled = raw.get("cody.enabled").and_then(serde_json::Value::as_bool);
    let completions = raw.get("completions").filter(|value| !value.is_null());
    match (enabled, completions) {
        (Some(false), _) => false,
        (Some(true), _) => true,
        // `enabled == nil && completions != nil`: the deprecated
        // `completions.enabled` flag decides (unset means false).
        (None, Some(completions)) => {
            completions
                .get("enabled")
                .and_then(serde_json::Value::as_bool)
                == Some(true)
        }
        (None, None) => false,
    }
}

fn string_field(value: &serde_json::Value, key: &str) -> Option<String> {
    value
        .get(key)
        .and_then(serde_json::Value::as_str)
        .map(str::to_owned)
}

/// `license.GenerateLicenseKeyBasedAccessToken`:
/// `slk_$hex($sha256(licenseKey))`.
pub fn license_key_based_access_token(license_key: &str) -> String {
    let digest = sha2::Sha256::digest(license_key.as_bytes());
    let mut hex = String::with_capacity(digest.len() * 2);
    for byte in digest {
        use std::fmt::Write as _;
        let _ = write!(hex, "{byte:02x}");
    }
    format!("{LICENSE_KEY_BASED_ACCESS_TOKEN_PREFIX}{hex}")
}

/// `conf.GetCompletionsConfig`, restricted to the fields this job reads.
///
/// Returns `None` when the baseline returns a nil config (Cody disabled, the
/// provider has no usable credentials, …).
pub fn get_completions_config(site: &SiteConfiguration) -> Option<CompletionsConfig> {
    if !cody_enabled(site) {
        return None;
    }

    let completions = site
        .raw()
        .get("completions")
        .filter(|value| !value.is_null());
    let mut provider = completions
        .and_then(|value| string_field(value, "provider"))
        .unwrap_or_default();
    if provider.is_empty() {
        // An unset provider means the Sourcegraph provider, but only when the
        // new `cody.enabled` flag is set explicitly.
        if !new_cody_enabled(site) {
            return None;
        }
        provider = "sourcegraph".to_owned();
    }

    let configured_endpoint = completions
        .and_then(|value| string_field(value, "endpoint"))
        .unwrap_or_default();
    let configured_token = completions
        .and_then(|value| string_field(value, "accessToken"))
        .unwrap_or_default();

    if provider != "sourcegraph" {
        // The job never calls other providers; their defaults are irrelevant.
        return Some(CompletionsConfig {
            provider,
            endpoint: configured_endpoint,
            access_token: configured_token,
        });
    }

    let endpoint = if configured_endpoint.is_empty() {
        CODY_GATEWAY_PROD_ENDPOINT.to_owned()
    } else {
        configured_endpoint
    };
    // `getSourcegraphProviderAccessToken`: an explicit token wins, otherwise
    // one is derived from the site license key.
    let access_token = if !configured_token.is_empty() {
        configured_token
    } else {
        let license_key = string_field(site.raw(), "licenseKey").unwrap_or_default();
        if license_key.is_empty() {
            // Authing with Cody Gateway is impossible without a token.
            return None;
        }
        license_key_based_access_token(&license_key)
    };

    Some(CompletionsConfig {
        provider,
        endpoint,
        access_token,
    })
}

/// `url.Parse(endpoint)` followed by `u.Path = "v1/limits"` and `u.String()`.
///
/// Go keeps the query and drops the configured path; the request always goes
/// to `<scheme>://<host>[:port]/v1/limits`.
pub fn limits_url(endpoint: &str) -> Result<String, String> {
    let uri: http::Uri = endpoint
        .parse()
        .map_err(|err| format!("parsing endpoint {endpoint:?}: {err}"))?;
    let scheme = uri
        .scheme_str()
        .ok_or_else(|| format!("endpoint {endpoint:?} has no scheme"))?;
    let authority = uri
        .authority()
        .ok_or_else(|| format!("endpoint {endpoint:?} has no host"))?;
    let mut url = format!("{scheme}://{authority}/v1/limits");
    if let Some(query) = uri.query() {
        url.push('?');
        url.push_str(query);
    }
    Ok(url)
}

/// The TTL written for a limit: 35 minutes, shortened by the time until the
/// limit resets.
///
/// The baseline has two independent `if`s: `timeToReset <= 0` sets `ttl = 1`,
/// and then `timeToReset < ttl` sets `ttl = timeToReset` again — so a reset
/// time in the past yields a *negative* TTL (Redis rejects `SETEX` with
/// "invalid expire time"), and exactly-now yields 0. The Rust port keeps the
/// baseline's arithmetic verbatim; the failure is surfaced as a handler error
/// on both sides.
///
/// `int(time.Until(expiry).Seconds())` truncates toward zero, which Rust's
/// `as` cast also does.
pub fn usage_ttl(expiry: Option<sg_db::OffsetDateTime>, now: sg_db::OffsetDateTime) -> i64 {
    let mut ttl = REDIS_TTL_SECONDS;
    if let Some(expiry) = expiry {
        let time_to_reset = (expiry - now).as_seconds_f64() as i64;
        if time_to_reset <= 0 {
            ttl = 1;
        }
        if time_to_reset < ttl {
            ttl = time_to_reset;
        }
    }
    ttl
}

/// `cmd/worker/internal/codygateway`'s handler closure plus the client it
/// builds each run.
pub struct CodyGatewayUsageHandler {
    live_config: Arc<LiveConfig>,
    http: HttpClient,
    store: LazyStore,
}

impl CodyGatewayUsageHandler {
    pub fn new(live_config: Arc<LiveConfig>) -> Self {
        Self {
            live_config,
            http: HttpClient::new(),
            store: LazyStore::new(sg_redis::store_endpoint()),
        }
    }

    /// One handler invocation, for the differential harness.
    pub async fn handle_once(&self) -> Result<(), HandlerError> {
        self.handle(CancellationToken::new()).await
    }

    /// `codygateway.Client.GetLimits`.
    async fn get_limits(&self, config: &CompletionsConfig) -> Result<Vec<LimitStatus>, String> {
        let url = limits_url(&config.endpoint)?;
        let headers = [(
            "Authorization".to_owned(),
            format!("Bearer {}", config.access_token),
        )];
        let response = self.http.get_with_headers(&url, &headers).await?;
        if response.status != 200 {
            return Err(format!("request failed with status: {}", response.status));
        }
        parse_limits(&response.body)
    }
}

#[async_trait]
impl Handler for CodyGatewayUsageHandler {
    async fn handle(&self, _ctx: CancellationToken) -> Result<(), HandlerError> {
        let config = get_completions_config(&self.live_config.current().site);
        let Some(config) = config.filter(|config| config.provider == "sourcegraph") else {
            // If no client is configured, skip this iteration.
            tracing::info!("Not checking Cody Gateway usage, disabled");
            return Ok(());
        };

        tracing::info!("Checking Cody Gateway usage");
        let limits = self.get_limits(&config).await.map_err(|err| {
            HandlerError::new(format!("failed to get cody gateway limits: {err}"))
        })?;

        let mut errs: Vec<String> = Vec::new();
        for limit in limits {
            let ttl = usage_ttl(limit.expiry, sg_db::OffsetDateTime::now_utc());
            let key = format!("{REDIS_KEY_PREFIX}:{}", limit.feature);
            let value = limit.percent_used().to_string();
            let result = match self.store.get().await {
                Ok(store) => store
                    .set_ex(&key, ttl, value)
                    .await
                    .map_err(|err| err.to_string()),
                Err(err) => Err(err),
            };
            if let Err(err) = result {
                tracing::error!(
                    err = %err,
                    "failed to store rate limit usage for cody gateway"
                );
                errs.push(err);
            }
        }

        if errs.is_empty() {
            Ok(())
        } else {
            Err(HandlerError::new(errs.join("\n")))
        }
    }
}

/// A `KeyValue` that dials on first use, like `redispool.Store`'s lazy pool.
struct LazyStore {
    endpoint: String,
    cell: OnceCell<KeyValue>,
}

impl LazyStore {
    fn new(endpoint: String) -> Self {
        Self {
            endpoint,
            cell: OnceCell::new(),
        }
    }

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

#[cfg(test)]
mod tests {
    use super::*;

    fn site(json: &str) -> SiteConfiguration {
        SiteConfiguration::parse(json).expect("valid site config")
    }

    #[test]
    fn job_metadata_matches_the_baseline() {
        let job = CodyGatewayUsageCheck;
        assert_eq!(job.description(), JOB_DESCRIPTION);
        assert_eq!(ROUTINE_NAME, "CodyGatewayUsageWorker");
        assert_eq!(INTERVAL, Duration::from_secs(1800));
        assert_eq!(REDIS_TTL_SECONDS, 2100);
    }

    #[test]
    fn percent_used_matches_the_baseline() {
        let limit = |limit, usage| LimitStatus {
            feature: FEATURE_CHAT_COMPLETIONS.to_owned(),
            interval_limit: limit,
            interval_usage: usage,
            time_interval: String::new(),
            expiry: None,
        };
        assert_eq!(limit(0, 0).percent_used(), 100);
        assert_eq!(limit(100, 42).percent_used(), 42);
        assert_eq!(limit(200, 250).percent_used(), 125);
        // 1/3 rounds up to 34.
        assert_eq!(limit(3, 1).percent_used(), 34);
        assert_eq!(limit(3, 0).percent_used(), 0);
    }

    #[test]
    fn parse_limits_drops_unknown_features_and_sorts() {
        let body = br#"{
            "unknown_feature": {"limit": 1, "usage": 1, "interval": "monthly"},
            "embeddings": {"limit": 200, "usage": 250, "interval": "monthly"},
            "chat_completions": {"limit": 100, "usage": 42, "interval": "monthly"}
        }"#;
        let limits = parse_limits(body).expect("parse");
        let features: Vec<&str> = limits.iter().map(|limit| limit.feature.as_str()).collect();
        assert_eq!(features, [FEATURE_CHAT_COMPLETIONS, FEATURE_EMBEDDINGS]);
        assert_eq!(limits[0].interval_limit, 100);
        assert_eq!(limits[0].interval_usage, 42);
        assert_eq!(limits[0].time_interval, "monthly");
    }

    #[test]
    fn parse_limits_reads_rfc3339_expiry() {
        let body = br#"{"code_completions": {"limit": 1, "usage": 1, "interval": "monthly",
            "expiry": "2026-11-08T00:00:00Z"}}"#;
        let limits = parse_limits(body).expect("parse");
        assert_eq!(
            limits[0].expiry,
            Some(
                sg_db::OffsetDateTime::parse(
                    "2026-11-08T00:00:00Z",
                    &time::format_description::well_known::Rfc3339
                )
                .expect("parse")
            )
        );
        // A JSON null is a nil pointer in the baseline.
        let limits =
            parse_limits(br#"{"code_completions": {"limit": 1, "usage": 1, "expiry": null}}"#)
                .expect("parse");
        assert_eq!(limits[0].expiry, None);
    }

    #[test]
    fn usage_ttl_matches_the_baseline() {
        let now = sg_db::OffsetDateTime::parse(
            "2026-11-08T00:00:00Z",
            &time::format_description::well_known::Rfc3339,
        )
        .expect("parse");
        assert_eq!(usage_ttl(None, now), REDIS_TTL_SECONDS);
        // 10 minutes until reset is sooner than 35 minutes.
        assert_eq!(
            usage_ttl(Some(now + time::Duration::seconds(600)), now),
            600
        );
        // A reset further away than 35 minutes keeps the 35-minute TTL.
        assert_eq!(
            usage_ttl(Some(now + time::Duration::hours(2)), now),
            REDIS_TTL_SECONDS
        );
        // The baseline's second `if` re-applies a past `timeToReset`, so a
        // reset in the past produces a negative TTL (Redis rejects it) and
        // exactly-now produces 0.
        assert_eq!(usage_ttl(Some(now - time::Duration::seconds(10)), now), -10);
        assert_eq!(usage_ttl(Some(now), now), 0);
        assert_eq!(
            usage_ttl(Some(now + time::Duration::milliseconds(500)), now),
            0
        );
    }

    #[test]
    fn completions_config_matches_the_baseline() {
        // Cody disabled: nil.
        assert_eq!(
            get_completions_config(&site(r#"{"cody.enabled": false}"#)),
            None
        );
        // No cody flag at all: nil.
        assert_eq!(get_completions_config(&site("{}")), None);
        // Legacy `completions.enabled` without `cody.enabled`.
        assert!(get_completions_config(&site(
            r#"{"completions": {"enabled": true, "provider": "sourcegraph", "accessToken": "t"}}"#
        ))
        .is_some());
        assert_eq!(
            get_completions_config(&site(
                r#"{"completions": {"enabled": false, "provider": "sourcegraph", "accessToken": "t"}}"#
            )),
            None
        );
        // An unset provider requires the new flag and defaults to sourcegraph.
        let config = get_completions_config(&site(
            r#"{"cody.enabled": true, "completions": {"accessToken": "t"}}"#,
        ))
        .expect("config");
        assert_eq!(config.provider, "sourcegraph");
        assert_eq!(config.endpoint, CODY_GATEWAY_PROD_ENDPOINT);
        assert_eq!(config.access_token, "t");
        // A non-sourcegraph provider is returned but never used by the job.
        let config = get_completions_config(&site(
            r#"{"cody.enabled": true, "completions": {"provider": "openai", "accessToken": "t"}}"#,
        ))
        .expect("config");
        assert_eq!(config.provider, "openai");
        // No token and no license key: nil.
        assert_eq!(
            get_completions_config(&site(
                r#"{"cody.enabled": true, "completions": {"provider": "sourcegraph"}}"#
            )),
            None
        );
        // A license key derives the token.
        let config = get_completions_config(&site(
            r#"{"cody.enabled": true, "licenseKey": "test-license",
                "completions": {"provider": "sourcegraph"}}"#,
        ))
        .expect("config");
        assert_eq!(
            config.access_token,
            license_key_based_access_token("test-license")
        );
        assert!(config.access_token.starts_with("slk_"));
        assert_eq!(config.access_token.len(), 4 + 64);
    }

    #[test]
    fn limits_url_drops_the_configured_path_and_keeps_the_query() {
        assert_eq!(
            limits_url("http://127.0.0.1:1234").expect("url"),
            "http://127.0.0.1:1234/v1/limits"
        );
        assert_eq!(
            limits_url("https://example.com/base/path?x=1").expect("url"),
            "https://example.com/v1/limits?x=1"
        );
        assert!(limits_url("not a url").is_err());
    }
}
