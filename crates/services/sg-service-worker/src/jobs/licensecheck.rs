#![forbid(unsafe_code)]

//! `cmd/worker/internal/licensecheck`: the `license-check` job.
//!
//! Two periodic routines (the second only off Sourcegraph.com):
//!
//! - `licensecheck.maxusercount` (6h): when a licensed instance is configured,
//!   counts active users (excluding `sourcegraph-operator` service accounts)
//!   and records the highest count seen in the `license_user_count:max` Redis
//!   hash, keyed by the license signature;
//! - `licensing.check-license-validity` (15m, 1m initial delay): asks the
//!   license server whether the configured key is still valid and stores the
//!   answer in Redis. Air-gapped, `dev`, `internal` and free licenses skip the
//!   network call; an unchanged key is re-checked at most every 12 hours.
//!
//! The HTTP endpoint is `SOURCEGRAPH_API_URL` (default `https://sourcegraph.com`)
//! `/.api/license/check`; the request bearer token is
//! `slk_$hex(sha256(licenseKey))`.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use serde_json::json;
use sg_config::LiveConfig;
use sg_http::client::HttpClient;
use sg_licensing::{
    configured_license_info_with_signature, license_key_based_access_token, Info,
    LICENSE_CHECK_INTERVAL_SECS, LICENSE_INVALID_REASON, LICENSE_VALIDITY_STORE_KEY, MAX_USERS_KEY,
    MAX_USERS_TIME_KEY,
};
use sg_redis::KeyValue;
use sqlx::PgPool;
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;
use tokio::sync::OnceCell;
use tokio_util::sync::CancellationToken;

use super::{Job, JobDeps};
use crate::framework::periodic::{Handler, HandlerError, PeriodicRoutine};
use crate::framework::BackgroundRoutine;

/// `licenseCheckJob.Description`.
pub const JOB_DESCRIPTION: &str = "License check job";
/// `newMaxUserCountRoutine`'s `goroutine.WithName`.
pub const MAX_USER_COUNT_NAME: &str = "licensecheck.maxusercount";
pub const MAX_USER_COUNT_DESCRIPTION: &str =
    "Periodically checks for a new count of max user accounts";
/// `goroutine.WithInterval(360 * time.Minute)`.
pub const MAX_USER_COUNT_INTERVAL: Duration = Duration::from_secs(360 * 60);
/// `newLicenseChecker`'s `goroutine.WithName`.
pub const LICENSE_CHECK_NAME: &str = "licensing.check-license-validity";
pub const LICENSE_CHECK_DESCRIPTION: &str = "check if license is valid from sourcegraph.com";
/// `goroutine.WithInterval(15 * time.Minute)`.
pub const LICENSE_CHECK_INTERVAL: Duration = Duration::from_secs(15 * 60);
/// `goroutine.WithInitialDelay(1 * time.Minute)`.
pub const LICENSE_CHECK_INITIAL_DELAY: Duration = Duration::from_secs(60);
/// `lastCalledAtStoreKey`.
pub const LAST_CALLED_AT_STORE_KEY: &str = "licensing:last_called_at";
/// `prevLicenseTokenKey`.
pub const PREV_LICENSE_TOKEN_KEY: &str = "licensing:prev_license_hash";
/// `baseUrl`'s default (`SOURCEGRAPH_API_URL`).
pub const DEFAULT_API_URL: &str = "https://sourcegraph.com";
/// The license check endpoint path (`url.JoinPath(baseUrl, "/.api/license/check")`).
pub const LICENSE_CHECK_PATH: &str = "/.api/license/check";
/// The `checkMaxUsers` context timeout.
pub const MAX_USER_COUNT_TIMEOUT: Duration = Duration::from_secs(15);

/// `licenseCheckJob`.
pub struct LicenseCheckJob;

#[async_trait]
impl Job for LicenseCheckJob {
    fn description(&self) -> String {
        JOB_DESCRIPTION.to_owned()
    }

    async fn routines(
        &self,
        deps: &JobDeps,
        _setup_ctx: CancellationToken,
    ) -> Result<Vec<BackgroundRoutine>, String> {
        Ok(self.build_routines(deps, dotcom_mode(), api_url()))
    }
}

impl LicenseCheckJob {
    /// `createBackgroundRoutines`: the user count always runs; the license
    /// validity check only off Sourcegraph.com.
    fn build_routines(
        &self,
        deps: &JobDeps,
        dotcom: bool,
        api_url: String,
    ) -> Vec<BackgroundRoutine> {
        let store = Arc::new(SharedStore::new(sg_redis::store_endpoint()));
        let license: Arc<dyn LicenseSource> = Arc::new(ConfiguredLicense {
            live_config: Arc::clone(&deps.live_config),
        });

        let mut routines: Vec<BackgroundRoutine> =
            vec![
                PeriodicRoutine::new(Arc::new(MaxUserCountHandler::with_store(
                    deps.frontend.clone(),
                    Arc::clone(&store),
                    Arc::clone(&license),
                )) as Arc<dyn Handler>)
                .with_name(MAX_USER_COUNT_NAME)
                .with_description(MAX_USER_COUNT_DESCRIPTION)
                .with_interval(MAX_USER_COUNT_INTERVAL)
                .into(),
            ];

        if !dotcom {
            routines.push(
                PeriodicRoutine::new(Arc::new(LicenseCheckerHandler::new(
                    deps.frontend.clone(),
                    Arc::clone(&deps.live_config),
                    Arc::clone(&store),
                    Arc::new(HttpDoer::new()),
                    api_url,
                    license,
                )) as Arc<dyn Handler>)
                .with_name(LICENSE_CHECK_NAME)
                .with_description(LICENSE_CHECK_DESCRIPTION)
                .with_interval(LICENSE_CHECK_INTERVAL)
                .with_initial_delay(LICENSE_CHECK_INITIAL_DELAY)
                .into(),
            );
        }

        routines
    }
}

/// `dotcom.SourcegraphDotComMode()` (`SOURCEGRAPHDOTCOM_MODE`, a Go bool).
fn dotcom_mode() -> bool {
    parse_go_bool(std::env::var("SOURCEGRAPHDOTCOM_MODE").ok().as_deref())
}

/// `strconv.ParseBool`: only these spellings are true; everything else
/// (including unset) is false.
fn parse_go_bool(raw: Option<&str>) -> bool {
    matches!(raw, Some("1" | "t" | "T" | "TRUE" | "true" | "True"))
}

/// `baseUrl`: `SOURCEGRAPH_API_URL`, default `https://sourcegraph.com`.
fn api_url() -> String {
    api_url_from(std::env::var("SOURCEGRAPH_API_URL").ok())
}

fn api_url_from(raw: Option<String>) -> String {
    match raw {
        Some(url) if !url.is_empty() => url,
        _ => DEFAULT_API_URL.to_owned(),
    }
}

/// `url.JoinPath(baseUrl, "/.api/license/check")`.
fn license_check_url(base: &str) -> String {
    format!("{}{}", base.trim_end_matches('/'), LICENSE_CHECK_PATH)
}

/// `licensing.GetConfiguredProductLicenseInfoWithSignature`, behind a seam so
/// the differential harness can script license claims like the baseline's
/// `licensing.MockGetConfiguredProductLicenseInfo`.
#[async_trait]
pub trait LicenseSource: Send + Sync {
    async fn configured_info(&self) -> Result<(Info, Vec<u8>), String>;
}

/// The production source: parse the site configuration's license key.
pub struct ConfiguredLicense {
    live_config: Arc<LiveConfig>,
}

#[async_trait]
impl LicenseSource for ConfiguredLicense {
    async fn configured_info(&self) -> Result<(Info, Vec<u8>), String> {
        configured_license_info_with_signature(&self.live_config.current().site.license_key())
    }
}

/// A scripted source for tests.
pub struct StaticLicenseSource {
    result: Result<(Info, Vec<u8>), String>,
}

impl StaticLicenseSource {
    pub fn new(info: Info, signature: Vec<u8>) -> Self {
        Self {
            result: Ok((info, signature)),
        }
    }

    /// The parse-error path (`GetConfiguredProductLicenseInfo` failing).
    pub fn error(message: impl Into<String>) -> Self {
        Self {
            result: Err(message.into()),
        }
    }
}

#[async_trait]
impl LicenseSource for StaticLicenseSource {
    async fn configured_info(&self) -> Result<(Info, Vec<u8>), String> {
        self.result.clone()
    }
}

/// `httpcli.Doer` for the license check request.
#[async_trait]
pub trait LicenseCheckDoer: Send + Sync {
    async fn post(
        &self,
        url: &str,
        headers: &[(String, String)],
        body: Vec<u8>,
    ) -> Result<sg_http::client::HttpResponse, String>;
}

/// The production doer (`httpcli.ExternalDoer`).
pub struct HttpDoer {
    client: HttpClient,
}

impl HttpDoer {
    pub fn new() -> Self {
        Self {
            client: HttpClient::new(),
        }
    }
}

impl Default for HttpDoer {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl LicenseCheckDoer for HttpDoer {
    async fn post(
        &self,
        url: &str,
        headers: &[(String, String)],
        body: Vec<u8>,
    ) -> Result<sg_http::client::HttpResponse, String> {
        self.client.post(url, headers, body).await
    }
}

/// `license_user_count:max` bookkeeping.
async fn get_max_users(kv: &KeyValue, signature: &[u8]) -> Result<(i64, String), String> {
    let last_max = match kv
        .hget_bytes(MAX_USERS_KEY, signature)
        .await
        .map_err(|error| error.to_string())?
    {
        Some(bytes) => {
            let raw = String::from_utf8_lossy(&bytes);
            // `getMaxUsers` only parses a non-empty value (`lastMax != ""`).
            if raw.is_empty() {
                0
            } else {
                raw.parse::<i64>()
                    .map_err(|error| format!("invalid max user count: {error}"))?
            }
        }
        None => 0,
    };
    let last_time = kv
        .hget_bytes(MAX_USERS_TIME_KEY, signature)
        .await
        .map_err(|error| error.to_string())?
        .map(|bytes| String::from_utf8_lossy(&bytes).into_owned())
        .unwrap_or_default();
    Ok((last_max, last_time))
}

/// `setMaxUsers`: only a new high-water mark is recorded.
async fn set_max_users(kv: &KeyValue, signature: &[u8], count: i64) -> Result<(), String> {
    let (last_max, _) = get_max_users(kv, signature).await?;
    if count > last_max {
        kv.hset(MAX_USERS_KEY, signature, count.to_string())
            .await
            .map_err(|error| error.to_string())?;
        kv.hset(
            MAX_USERS_TIME_KEY,
            signature,
            go_max_users_time(OffsetDateTime::now_utc()),
        )
        .await
        .map_err(|error| error.to_string())?;
    }
    Ok(())
}

/// `time.Now().Format(time.RFC3339)`: seconds precision, like Go's layout
/// (the `time` crate would otherwise append nanoseconds).
fn rfc3339_now() -> String {
    OffsetDateTime::now_utc()
        .replace_nanosecond(0)
        .expect("zero nanoseconds")
        .format(&Rfc3339)
        .unwrap_or_default()
}

/// `time.Format("2006-01-02 15:04:05 UTC")`.
fn go_max_users_time(now: OffsetDateTime) -> String {
    format!(
        "{:04}-{:02}-{:02} {:02}:{:02}:{:02} UTC",
        now.year(),
        u8::from(now.month()),
        now.day(),
        now.hour(),
        now.minute(),
        now.second()
    )
}

/// `newMaxUserCountRoutine`'s handler.
pub struct MaxUserCountHandler {
    pool: PgPool,
    store: Arc<SharedStore>,
    license: Arc<dyn LicenseSource>,
}

impl MaxUserCountHandler {
    /// A connected-store constructor for tests and the differential harness.
    pub fn new(pool: PgPool, kv: KeyValue, license: Arc<dyn LicenseSource>) -> Self {
        Self {
            pool,
            store: Arc::new(SharedStore::ready(kv)),
            license,
        }
    }

    fn with_store(pool: PgPool, store: Arc<SharedStore>, license: Arc<dyn LicenseSource>) -> Self {
        Self {
            pool,
            store,
            license,
        }
    }
}

#[async_trait]
impl Handler for MaxUserCountHandler {
    async fn handle(&self, _ctx: CancellationToken) -> Result<(), HandlerError> {
        let (_, signature) = self.license.configured_info().await.map_err(|error| {
            HandlerError::new(format!("error getting configured license info: {error}"))
        })?;
        if signature.is_empty() {
            // No license key is in use.
            return Ok(());
        }

        let kv = self
            .store
            .get()
            .await
            .map_err(|error| HandlerError::new(format!("connect to redis store: {error}")))?;

        // The baseline wraps `checkMaxUsers` (count + write) in a 15s context.
        tokio::time::timeout(
            MAX_USER_COUNT_TIMEOUT,
            check_max_users(&self.pool, kv, &signature),
        )
        .await
        .map_err(|_| HandlerError::new("failed to check max users: context deadline exceeded"))?
        .map_err(|error| HandlerError::new(format!("failed to check max users: {error}")))
    }
}

/// `checkMaxUsers`: count the users, then raise the recorded high-water mark.
async fn check_max_users(pool: &PgPool, kv: &KeyValue, signature: &[u8]) -> Result<(), String> {
    let count = sg_db::global_state::users_count_excluding_sourcegraph_operators(pool)
        .await
        .map_err(|error| error.to_string())?;
    set_max_users(kv, signature, count).await
}

/// `newLicenseChecker`'s handler.
pub struct LicenseCheckerHandler {
    pool: PgPool,
    live_config: Arc<LiveConfig>,
    store: Arc<SharedStore>,
    doer: Arc<dyn LicenseCheckDoer>,
    api_url: String,
    license: Arc<dyn LicenseSource>,
}

impl LicenseCheckerHandler {
    pub fn new(
        pool: PgPool,
        live_config: Arc<LiveConfig>,
        store: Arc<SharedStore>,
        doer: Arc<dyn LicenseCheckDoer>,
        api_url: String,
        license: Arc<dyn LicenseSource>,
    ) -> Self {
        Self {
            pool,
            live_config,
            store,
            doer,
            api_url,
            license,
        }
    }

    /// The harness seam: a scripted license source and doer.
    #[allow(clippy::too_many_arguments)]
    pub fn with_script(
        pool: PgPool,
        live_config: Arc<LiveConfig>,
        kv: KeyValue,
        doer: Arc<dyn LicenseCheckDoer>,
        api_url: String,
        license: Arc<dyn LicenseSource>,
    ) -> Self {
        Self::new(
            pool,
            live_config,
            Arc::new(SharedStore::ready(kv)),
            doer,
            api_url,
            license,
        )
    }
}

#[async_trait]
impl Handler for LicenseCheckerHandler {
    async fn handle(&self, _ctx: CancellationToken) -> Result<(), HandlerError> {
        // `l.db.GlobalState().Get(ctx)` runs before any license inspection in
        // the baseline, so a missing `global_state` row fails every branch --
        // including air-gapped and free licenses.
        let global_state = sg_db::global_state::get(&self.pool)
            .await
            .map_err(|error| {
                HandlerError::new(format!("error reading global state from DB: {error}"))
            })?;

        let kv = self
            .store
            .get()
            .await
            .map_err(|error| HandlerError::new(format!("connect to redis store: {error}")))?;

        // `licensing.Check(licensing.FeatureAllowAirGapped)`: the license must
        // parse, the stored validity flag must not be false, and the plan (or
        // an explicit tag) must allow air-gapped operation. Any error here
        // means "not air-gapped", not "abort" -- notably a license that fails
        // to parse is reported by the *unwrapped* second call below, exactly
        // like the baseline, which discards Check's error.
        let air_gapped = match self.license.configured_info().await {
            Ok((info, _)) => is_license_valid(kv).await && info.allows_air_gapped(),
            Err(_) => false,
        };
        if air_gapped {
            self.mark_valid(kv).await?;
            return Ok(());
        }

        // `licensing.GetConfiguredProductLicenseInfo()` a second time; its
        // error is returned unwrapped.
        let info = self
            .license
            .configured_info()
            .await
            .map_err(HandlerError::new)?
            .0;
        if info.has_tag("dev") || info.has_tag("internal") || info.is_free_plan() {
            self.mark_valid(kv).await?;
            return Ok(());
        }

        let site = self.live_config.current();
        let license_token = license_key_based_access_token(&site.site.license_key());

        // Only re-check an unchanged key about every 12 hours.
        let prev = kv
            .get_string(PREV_LICENSE_TOKEN_KEY)
            .await
            .map_err(|error| HandlerError::new(error.to_string()))?;
        if prev.as_deref() == Some(license_token.as_str()) {
            let last_called_at = kv
                .get_string(LAST_CALLED_AT_STORE_KEY)
                .await
                .map_err(|error| HandlerError::new(error.to_string()))?;
            if calc_duration_since_last_called(last_called_at.as_deref(), OffsetDateTime::now_utc())
                > 0
            {
                return Ok(());
            }
        }

        // Continue running with the new license key. Failing to store the
        // token is logged but not fatal in the baseline.
        let _ = kv
            .set(PREV_LICENSE_TOKEN_KEY, license_token.as_bytes())
            .await;
        kv.set(LAST_CALLED_AT_STORE_KEY, rfc3339_now().as_bytes())
            .await
            .map_err(|error| HandlerError::new(error.to_string()))?;

        let body = json!({ "siteID": global_state.site_id }).to_string();
        let url = license_check_url(&self.api_url);
        let headers = vec![
            (
                "Authorization".to_owned(),
                format!("Bearer {license_token}"),
            ),
            ("Content-Type".to_owned(), "application/json".to_owned()),
        ];

        let response = self
            .doer
            .post(&url, &headers, body.into_bytes())
            .await
            .map_err(HandlerError::new)?;

        if response.status != 200 {
            return Err(HandlerError::new(format!(
                "Failed to check license, status code: {}",
                response.status
            )));
        }

        let body: LicenseCheckResponse = serde_json::from_slice(&response.body)
            .map_err(|error| HandlerError::new(error.to_string()))?;

        if !body.error.is_empty() {
            return Err(HandlerError::new(body.error));
        }
        let Some(data) = body.data else {
            return Err(HandlerError::new("No data returned from license check"));
        };

        // Best effort, ignore errors here.
        let _ = kv.set(LICENSE_INVALID_REASON, data.reason.as_bytes()).await;
        kv.set_bool(LICENSE_VALIDITY_STORE_KEY, data.is_valid)
            .await
            .map_err(|error| HandlerError::new(error.to_string()))?;
        Ok(())
    }
}

impl LicenseCheckerHandler {
    /// The air-gapped / dev / internal / free short circuit: record the
    /// attempt and mark the license valid.
    async fn mark_valid(&self, kv: &KeyValue) -> Result<(), HandlerError> {
        kv.set(LAST_CALLED_AT_STORE_KEY, rfc3339_now().as_bytes())
            .await
            .map_err(|error| HandlerError::new(error.to_string()))?;
        kv.set_bool(LICENSE_VALIDITY_STORE_KEY, true)
            .await
            .map_err(|error| HandlerError::new(error.to_string()))
    }
}

/// `licensing.LicenseCheckResponse`.
#[derive(Debug, serde::Deserialize)]
struct LicenseCheckResponse {
    #[serde(default)]
    error: String,
    data: Option<LicenseCheckResponseData>,
}

/// `licensing.LicenseCheckResponseData`.
#[derive(Debug, serde::Deserialize)]
struct LicenseCheckResponseData {
    #[serde(default)]
    is_valid: bool,
    #[serde(default)]
    reason: String,
}

/// `IsLicenseValid`: a missing flag means valid; unparseable values also mean
/// valid (the baseline's `Value.Bool()` error path).
pub(crate) async fn is_license_valid(kv: &KeyValue) -> bool {
    match kv.get_string(LICENSE_VALIDITY_STORE_KEY).await {
        Ok(None) => true,
        Ok(Some(raw)) => match raw.as_str() {
            "1" => true,
            "0" => false,
            _ => true,
        },
        Err(_) => true,
    }
}

/// `calcDurationSinceLastCalled`: seconds until the next allowed check; 0 means
/// run now. Errors are ignored by the caller (they also mean "run now").
fn calc_duration_since_last_called(last_called_at: Option<&str>, now: OffsetDateTime) -> i64 {
    let Some(raw) = last_called_at else {
        return 0;
    };
    let Ok(parsed) = OffsetDateTime::parse(raw, &Rfc3339) else {
        return 0;
    };
    if parsed > now {
        return 0;
    }
    let elapsed = (now - parsed).whole_seconds();
    if elapsed > LICENSE_CHECK_INTERVAL_SECS {
        0
    } else {
        LICENSE_CHECK_INTERVAL_SECS - elapsed
    }
}

/// `redispool.Store` is lazily dialled in the baseline; [`KeyValue::connect`]
/// dials eagerly, so defer it to the first handler run.
pub struct SharedStore {
    endpoint: String,
    cell: OnceCell<KeyValue>,
}

impl SharedStore {
    pub fn new(endpoint: String) -> Self {
        Self {
            endpoint,
            cell: OnceCell::new(),
        }
    }

    pub fn ready(kv: KeyValue) -> Self {
        let cell = OnceCell::new();
        let _ = cell.set(kv);
        Self {
            endpoint: String::new(),
            cell,
        }
    }

    pub async fn get(&self) -> Result<&KeyValue, String> {
        self.cell
            .get_or_try_init(|| async {
                KeyValue::connect(&self.endpoint)
                    .await
                    .map_err(|error| error.to_string())
            })
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sg_licensing::Plan;

    fn deps() -> JobDeps {
        JobDeps {
            frontend: sqlx::postgres::PgPoolOptions::new()
                .connect_lazy("postgres://unused@localhost/unused")
                .expect("lazy pool"),
            live_config: Arc::new(sg_config::LiveConfig::new(sg_config::Unified::default())),
            keyring: Arc::new(sg_keyring::Keyring::default()),
        }
    }

    #[tokio::test]
    async fn job_constructs_the_baseline_routines() {
        let job = LicenseCheckJob;
        assert_eq!(job.description(), JOB_DESCRIPTION);
        assert!(job.configs().is_empty());

        let routines = job.build_routines(&deps(), false, DEFAULT_API_URL.to_owned());
        assert_eq!(routines.len(), 2);
        assert_eq!(routines[0].name(), MAX_USER_COUNT_NAME);
        assert_eq!(routines[0].description(), MAX_USER_COUNT_DESCRIPTION);
        assert_eq!(routines[0].interval(), Duration::from_secs(360 * 60));
        assert_eq!(routines[1].name(), LICENSE_CHECK_NAME);
        assert_eq!(routines[1].description(), LICENSE_CHECK_DESCRIPTION);
        assert_eq!(routines[1].interval(), Duration::from_secs(15 * 60));
    }

    #[tokio::test]
    async fn dotcom_only_runs_the_user_count() {
        let routines = LicenseCheckJob.build_routines(&deps(), true, DEFAULT_API_URL.to_owned());
        assert_eq!(routines.len(), 1);
        assert_eq!(routines[0].name(), MAX_USER_COUNT_NAME);
    }

    #[test]
    fn dotcom_mode_parses_go_bools() {
        for raw in ["1", "t", "T", "TRUE", "true", "True"] {
            assert!(parse_go_bool(Some(raw)), "{raw}");
        }
        for raw in ["0", "f", "FALSE", "false", "", "yes"] {
            assert!(!parse_go_bool(Some(raw)), "{raw}");
        }
        assert!(!parse_go_bool(None));
    }

    #[test]
    fn api_url_defaults_and_overrides() {
        assert_eq!(api_url_from(None), DEFAULT_API_URL);
        assert_eq!(api_url_from(Some(String::new())), DEFAULT_API_URL);
        assert_eq!(
            api_url_from(Some("http://localhost:1234".to_owned())),
            "http://localhost:1234"
        );
    }

    #[test]
    fn url_join_matches_the_baseline() {
        assert_eq!(
            license_check_url("https://sourcegraph.com"),
            "https://sourcegraph.com/.api/license/check"
        );
        assert_eq!(
            license_check_url("http://127.0.0.1:9999/"),
            "http://127.0.0.1:9999/.api/license/check"
        );
    }

    #[test]
    fn max_users_time_uses_go_layout() {
        let now = OffsetDateTime::from_unix_timestamp(1_700_000_000).expect("timestamp");
        assert_eq!(go_max_users_time(now), "2023-11-14 22:13:20 UTC");
    }

    #[test]
    fn duration_since_last_called_matches_the_baseline() {
        let now = OffsetDateTime::from_unix_timestamp(1_700_000_000).expect("timestamp");
        // Missing / invalid / future all mean "run now".
        assert_eq!(calc_duration_since_last_called(None, now), 0);
        assert_eq!(calc_duration_since_last_called(Some("invalid"), now), 0);
        let future = (now + time::Duration::hours(1))
            .format(&Rfc3339)
            .expect("format");
        assert_eq!(calc_duration_since_last_called(Some(&future), now), 0);
        // Older than the interval: run now.
        let old = (now - time::Duration::hours(13))
            .format(&Rfc3339)
            .expect("format");
        assert_eq!(calc_duration_since_last_called(Some(&old), now), 0);
        // Fresh: wait the remainder.
        let fresh = (now - time::Duration::hours(1))
            .format(&Rfc3339)
            .expect("format");
        assert_eq!(
            calc_duration_since_last_called(Some(&fresh), now),
            11 * 60 * 60
        );
    }

    #[test]
    fn free_and_air_gapped_classification() {
        let free = Info {
            tags: vec!["plan:free-1".to_owned()],
            ..Info::default()
        };
        assert!(free.is_free_plan());
        assert!(!free.allows_air_gapped());
        assert_eq!(free.plan(), Plan::Free1);

        let air_gapped = Info {
            tags: vec!["plan:enterprise-air-gap-0".to_owned()],
            ..Info::default()
        };
        assert!(air_gapped.allows_air_gapped());
        let tagged = Info {
            tags: vec!["allow-air-gapped".to_owned()],
            ..Info::default()
        };
        assert!(tagged.allows_air_gapped());
    }
}
