//! `window.context` (`cmd/frontend/internal/app/jscontext/jscontext.go`).
//!
//! The SPA reads this object from `sourcegraph/app/context`; the baseline
//! injects it as `window.context = {{.Context}}` (`app.html:67`). Go's
//! `html/template` serializes the value with `encoding/json` and then applies
//! its JS escaper, so the emitted text is JSON with `<`, `>`, `&`, U+2028 and
//! U+2029 written as ASCII `\uXXXX` escapes.
//!
//! This module implements the page-startup subset of the baseline context.
//! Fields that depend on subsystems outside this slice (license-key parsing,
//! SvelteKit route discovery, Cody Pro/dotcom configuration, restart markers,
//! non-builtin auth providers other than http-header/openidconnect, authz
//! `requiredForAuthz` matching) keep their zero/empty values and are recorded
//! as GAP/KNOWN_DIFF in the overview. Private site configuration is never
//! copied into the public `site` subset.
//!
//! No new tables, columns or Redis keys: user/org/settings/temporary-settings
//! reads go through the existing M6 storage in `sg-db`; the two queries that
//! need columns the stores do not expose (primary e-mail, permission nodes)
//! read the existing schema only.

use std::collections::BTreeMap;

use serde::Serialize;
use serde_json::Value;
use sha2::{Digest, Sha256};
use sqlx::PgPool;

use sg_graphql::exec::Actor;

use crate::app_handlers::{AppConfig, AppEnv};

/// `authProviderInfo` (`jscontext.go:44-54`).
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct AuthProviderInfo {
    #[serde(rename = "isBuiltin")]
    pub is_builtin: bool,
    #[serde(rename = "noSignIn")]
    pub no_sign_in: bool,
    #[serde(rename = "displayName")]
    pub display_name: String,
    #[serde(rename = "displayPrefix")]
    pub display_prefix: Option<String>,
    #[serde(rename = "serviceType")]
    pub service_type: String,
    #[serde(rename = "authenticationURL")]
    pub authentication_url: String,
    #[serde(rename = "serviceID")]
    pub service_id: String,
    #[serde(rename = "clientID")]
    pub client_id: String,
    #[serde(rename = "requiredForAuthz")]
    pub required_for_authz: bool,
}

/// `authPasswordPolicy` (`jscontext.go:57-62`).
#[derive(Debug, Clone, Default, Serialize, PartialEq, Eq)]
pub struct AuthPasswordPolicy {
    pub enabled: bool,
    #[serde(rename = "numberOfSpecialCharacters")]
    pub number_of_special_characters: i64,
    #[serde(rename = "requireAtLeastOneNumber")]
    pub require_at_least_one_number: bool,
    #[serde(rename = "requireUpperAndLowerCase")]
    pub require_upper_and_lower_case: bool,
}

/// `UserLatestSettings` (`jscontext.go:63-66`).
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct UserLatestSettings {
    pub id: i32,
    pub contents: String,
}

/// `UserOrganization` (`jscontext.go:67-72`).
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct UserOrganization {
    #[serde(rename = "__typename")]
    pub typename: String,
    pub id: String,
    pub name: String,
    pub url: String,
    #[serde(rename = "settingsURL")]
    pub settings_url: Option<String>,
}

/// `UserOrganizationsConnection` (`jscontext.go:73-76`).
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct UserOrganizationsConnection {
    #[serde(rename = "__typename")]
    pub typename: String,
    pub nodes: Vec<UserOrganization>,
}

/// `UserEmail` (`jscontext.go:77-81`).
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct UserEmail {
    pub email: String,
    #[serde(rename = "isPrimary")]
    pub is_primary: bool,
    pub verified: bool,
}

/// `UserSession` (`jscontext.go:82-84`).
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct UserSession {
    #[serde(rename = "canSignOut")]
    pub can_sign_out: bool,
}

/// `TemporarySettings` (`jscontext.go:86-89`).
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct TemporarySettings {
    #[serde(rename = "__typename")]
    pub typename: String,
    pub contents: String,
}

/// `Permission` (`jscontext.go:91-95`).
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct Permission {
    #[serde(rename = "__typename")]
    pub typename: String,
    pub id: String,
    #[serde(rename = "displayName")]
    pub display_name: String,
}

/// `PermissionsConnection` (`jscontext.go:97-100`).
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct PermissionsConnection {
    #[serde(rename = "__typename")]
    pub typename: String,
    pub nodes: Vec<Permission>,
}

/// `CurrentUser` (`jscontext.go:101-122`).
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct CurrentUser {
    #[serde(rename = "__typename")]
    pub typename: String,
    pub id: String,
    #[serde(rename = "databaseID")]
    pub database_id: i32,
    pub username: String,
    #[serde(rename = "avatarURL")]
    pub avatar_url: Option<String>,
    #[serde(rename = "displayName")]
    pub display_name: String,
    #[serde(rename = "siteAdmin")]
    pub site_admin: bool,
    pub url: String,
    #[serde(rename = "settingsURL")]
    pub settings_url: String,
    #[serde(rename = "viewerCanAdminister")]
    pub viewer_can_administer: bool,
    #[serde(rename = "tosAccepted")]
    pub tos_accepted: bool,
    #[serde(rename = "hasVerifiedEmail")]
    pub has_verified_email: bool,
    pub organizations: Option<UserOrganizationsConnection>,
    pub session: Option<UserSession>,
    pub emails: Option<Vec<UserEmail>>,
    #[serde(rename = "latestSettings")]
    pub latest_settings: Option<UserLatestSettings>,
    pub permissions: PermissionsConnection,
}

/// `FeatureBatchChanges` (`jscontext.go:124-132`).
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct FeatureBatchChanges {
    pub unrestricted: bool,
    #[serde(rename = "maxNumChangesets")]
    pub max_num_changesets: i64,
}

/// `LicenseInfo` (`jscontext.go:134-137`).
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct LicenseInfo {
    #[serde(rename = "batchChanges")]
    pub batch_changes: Option<FeatureBatchChanges>,
}

/// `sveltekit.JSContext` (`ui/sveltekit/sveltekit.go:25-38`).
///
/// KNOWN_DIFF/GAP: the baseline derives `knownRoutes` from the gorilla router
/// at startup and `enabledRoutes` from the `web-next*` feature flags. Route
/// discovery belongs to the SvelteKit slice, so the arrays stay empty here;
/// `showToggle` is the feature-flag default (`GetBoolOr(..., true)`).
#[derive(Debug, Clone, Default, Serialize, PartialEq, Eq)]
pub struct SvelteKitJsContext {
    #[serde(rename = "enabledRoutes")]
    pub enabled_routes: Vec<i64>,
    #[serde(rename = "showToggle")]
    pub show_toggle: bool,
    #[serde(rename = "knownRoutes")]
    pub known_routes: Vec<String>,
}

/// `JSContext` (`jscontext.go:148-260`), in Go struct declaration order.
#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct JsContext {
    /// `appRoot,omitempty`; the baseline leaves it empty.
    #[serde(rename = "appRoot", skip_serializing_if = "String::is_empty")]
    pub app_root: String,
    /// `externalURL,omitempty`; `conf.ExternalURLParsed()` defaults to
    /// `http://example.com`, so this is normally non-empty.
    #[serde(rename = "externalURL", skip_serializing_if = "String::is_empty")]
    pub external_url: String,
    #[serde(rename = "xhrHeaders")]
    pub xhr_headers: BTreeMap<String, String>,
    #[serde(rename = "userAgentIsBot")]
    pub user_agent_is_bot: bool,
    #[serde(rename = "assetsRoot")]
    pub assets_root: String,
    pub version: String,

    #[serde(rename = "isAuthenticatedUser")]
    pub is_authenticated_user: bool,
    #[serde(rename = "currentUser")]
    pub current_user: Option<CurrentUser>,
    #[serde(rename = "temporarySettings")]
    pub temporary_settings: Option<TemporarySettings>,

    #[serde(rename = "sentryDSN")]
    pub sentry_dsn: Option<String>,
    #[serde(rename = "openTelemetry")]
    pub open_telemetry: Option<Value>,

    #[serde(rename = "siteID")]
    pub site_id: String,
    #[serde(rename = "siteGQLID")]
    pub site_gql_id: String,
    pub debug: bool,
    #[serde(rename = "needsSiteInit")]
    pub needs_site_init: bool,
    #[serde(rename = "emailEnabled")]
    pub email_enabled: bool,

    /// Public subset of the site configuration (`publicSiteConfiguration`,
    /// `jscontext.go:657-665`): only `auth.public`,
    /// `authz.enforceForSiteAdmins`, `disableNonCriticalTelemetry` and
    /// `update.channel` (which defaults to `release`).
    pub site: Value,
    #[serde(rename = "needServerRestart")]
    pub need_server_restart: bool,
    #[serde(rename = "deployType")]
    pub deploy_type: String,

    #[serde(rename = "sourcegraphDotComMode")]
    pub sourcegraph_dot_com_mode: bool,

    #[serde(rename = "accessTokensAllow")]
    pub access_tokens_allow: String,
    #[serde(rename = "accessTokensAllowNoExpiration")]
    pub access_tokens_allow_no_expiration: bool,
    #[serde(rename = "accessTokensExpirationDaysDefault")]
    pub access_tokens_expiration_days_default: i64,
    #[serde(rename = "accessTokensExpirationDaysOptions")]
    pub access_tokens_expiration_days_options: Vec<i64>,

    #[serde(rename = "allowSignup")]
    pub allow_signup: bool,

    #[serde(rename = "resetPasswordEnabled")]
    pub reset_password_enabled: bool,

    #[serde(rename = "authMinPasswordLength")]
    pub auth_min_password_length: i64,
    #[serde(rename = "authPasswordPolicy")]
    pub auth_password_policy: AuthPasswordPolicy,

    #[serde(rename = "authProviders")]
    pub auth_providers: Vec<AuthProviderInfo>,
    #[serde(rename = "primaryLoginProvidersCount")]
    pub primary_login_providers_count: i64,

    #[serde(rename = "authAccessRequest")]
    pub auth_access_request: Option<Value>,

    pub branding: Branding,

    #[serde(rename = "batchChangesEnabled")]
    pub batch_changes_enabled: bool,
    #[serde(rename = "batchChangesDisableWebhooksWarning")]
    pub batch_changes_disable_webhooks_warning: bool,
    #[serde(rename = "batchChangesWebhookLogsEnabled")]
    pub batch_changes_webhook_logs_enabled: bool,

    #[serde(rename = "codyEnabledOnInstance")]
    pub cody_enabled_on_instance: bool,
    #[serde(rename = "codyEnabledForCurrentUser")]
    pub cody_enabled_for_current_user: bool,
    #[serde(rename = "codyRequiresVerifiedEmail")]
    pub cody_requires_verified_email: bool,

    #[serde(rename = "codeSearchEnabledOnInstance")]
    pub code_search_enabled_on_instance: bool,

    #[serde(rename = "executorsEnabled")]
    pub executors_enabled: bool,
    #[serde(rename = "codeIntelAutoIndexingEnabled")]
    pub code_intel_auto_indexing_enabled: bool,
    #[serde(rename = "codeIntelAutoIndexingAllowGlobalPolicies")]
    pub code_intel_auto_indexing_allow_global_policies: bool,
    #[serde(rename = "codeIntelRankingDocumentReferenceCountsEnabled")]
    pub code_intel_ranking_document_reference_counts_enabled: bool,

    #[serde(rename = "codeInsightsEnabled")]
    pub code_insights_enabled: bool,
    #[serde(rename = "applianceUpdateTarget")]
    pub appliance_update_target: String,
    #[serde(rename = "applianceMenuTarget")]
    pub appliance_menu_target: String,
    #[serde(rename = "codeIntelligenceEnabled")]
    pub code_intelligence_enabled: bool,
    #[serde(rename = "searchContextsEnabled")]
    pub search_contexts_enabled: bool,
    #[serde(rename = "notebooksEnabled")]
    pub notebooks_enabled: bool,
    #[serde(rename = "codeMonitoringEnabled")]
    pub code_monitoring_enabled: bool,
    #[serde(rename = "searchAggregationEnabled")]
    pub search_aggregation_enabled: bool,
    #[serde(rename = "ownEnabled")]
    pub own_enabled: bool,
    #[serde(rename = "searchJobsEnabled")]
    pub search_jobs_enabled: bool,

    #[serde(rename = "RedirectUnsupportedBrowser")]
    pub redirect_unsupported_browser: bool,

    #[serde(rename = "productResearchPageEnabled")]
    pub product_research_page_enabled: bool,

    #[serde(rename = "experimentalFeatures")]
    pub experimental_features: Value,

    #[serde(rename = "licenseInfo")]
    pub license_info: LicenseInfo,

    #[serde(rename = "hashedLicenseKey")]
    pub hashed_license_key: String,

    #[serde(rename = "outboundRequestLogLimit")]
    pub outbound_request_log_limit: i64,

    #[serde(rename = "disableFeedbackSurvey")]
    pub disable_feedback_survey: bool,

    #[serde(rename = "needsRepositoryConfiguration")]
    pub needs_repository_configuration: bool,

    #[serde(rename = "extsvcConfigFileExists")]
    pub extsvc_config_file_exists: bool,

    #[serde(rename = "extsvcConfigAllowEdits")]
    pub extsvc_config_allow_edits: bool,

    #[serde(rename = "runningOnMacOS")]
    pub running_on_macos: bool,

    #[serde(rename = "svelteKit")]
    pub svelte_kit: SvelteKitJsContext,

    /// `frontendCodyProConfig` is only set on dotcom; GAP: always null here.
    #[serde(rename = "frontendCodyProConfig")]
    pub frontend_cody_pro_config: Option<Value>,
}

/// `schema.Branding` (`schema/schema.go:586-596`), declaration order.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct Branding {
    #[serde(rename = "brandName", skip_serializing_if = "String::is_empty")]
    pub brand_name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dark: Option<BrandAssets>,
    #[serde(rename = "disableSymbolSpin", skip_serializing_if = "is_false")]
    pub disable_symbol_spin: bool,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub favicon: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub light: Option<BrandAssets>,
}

/// `schema.BrandAssets` (`schema/schema.go:579-584`).
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct BrandAssets {
    #[serde(skip_serializing_if = "String::is_empty")]
    pub logo: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub symbol: String,
}

fn is_false(value: &bool) -> bool {
    !*value
}

/// Request-derived inputs to [`JsContext`] (`NewJSContextFromRequest`).
#[derive(Debug, Clone, Copy, Default)]
pub struct RequestMeta<'a> {
    /// `User-Agent` (`isBot`).
    pub user_agent: Option<&'a str>,
    /// `Cache-Control`; `no-cache`/`max-age=0` are propagated in `XHRHeaders`.
    pub cache_control: Option<&'a str>,
    /// `actor.FromSessionCookie`: whether the `sgs` session cookie
    /// authenticated this request (`session.canSignOut`).
    pub from_session_cookie: bool,
}

impl JsContext {
    /// `html/template`'s JS value escaping (`jsValEscaper`) over
    /// `encoding/json` output: `<`, `>`, `&`, U+2028 and U+2029 become ASCII
    /// `\uXXXX` escapes (lowercase hex, exactly like Go). Everything else is
    /// serde_json's encoding, which shares Go's rules for quotes, control
    /// characters, raw UTF-8 and sorted map keys.
    pub fn json_for_script(&self) -> String {
        let json = serde_json::to_string(self).unwrap_or_else(|_| "null".to_owned());
        json.replace('<', "\\u003c")
            .replace('>', "\\u003e")
            .replace('&', "\\u0026")
            .replace('\u{2028}', "\\u2028")
            .replace('\u{2029}', "\\u2029")
    }
}

/// Applies the baseline's `if !codyLicensed { … }` force-disable
/// (`jscontext.go:497-501`). The free plan never licenses Cody.
fn cody_gate(mut context: JsContext, cody_licensed: bool) -> JsContext {
    if !cody_licensed {
        context.cody_enabled_on_instance = false;
        context.cody_enabled_for_current_user = false;
    }
    context
}

fn get<'a>(value: &'a Value, key: &str) -> Option<&'a Value> {
    value.get(key).filter(|value| !value.is_null())
}

fn get_str<'a>(value: &'a Value, key: &str) -> Option<&'a str> {
    get(value, key).and_then(Value::as_str)
}

fn get_bool(value: &Value, key: &str) -> bool {
    get(value, key).and_then(Value::as_bool).unwrap_or(false)
}

fn get_bool_opt(value: &Value, key: &str) -> Option<bool> {
    get(value, key).and_then(Value::as_bool)
}

fn get_i64(value: &Value, key: &str) -> i64 {
    get(value, key).and_then(Value::as_i64).unwrap_or(0)
}

/// `strconv.ParseBool` (used by the baseline for its `DISABLE_*` env flags).
fn go_parse_bool(value: &str) -> bool {
    matches!(value, "1" | "t" | "T" | "TRUE" | "true" | "True")
}

fn env_flag(name: &str) -> bool {
    std::env::var(name)
        .map(|value| go_parse_bool(&value))
        .unwrap_or(false)
}

/// `webhooks.LoggingEnabled` (`cmd/frontend/internal/webhooks/middleware.go:162-186`):
/// `webhook.logging.enabled` when set, otherwise enabled unless at least one
/// encryption key is configured.
fn webhook_logs_enabled(raw: &Value) -> bool {
    if let Some(enabled) =
        get(raw, "webhook.logging").and_then(|logging| get_bool_opt(logging, "enabled"))
    {
        return enabled;
    }
    match get(raw, "encryption.keys") {
        None => true,
        Some(keys) => ![
            "batchChangesCredentialKey",
            "externalServiceKey",
            "userExternalAccountKey",
            "webhookLogKey",
        ]
        .iter()
        .any(|key| get(keys, key).is_some()),
    }
}

/// `licenseInfo()` (`jscontext.go:685-709`) for the only license this slice
/// models: no configured license key, i.e. the automatic free `plan:free-1`
/// (`internal/licensing/data.go`), which has `FeatureBatchChanges` with
/// `MaxNumChangesets: 10` and `FeatureCodeSearch` enabled but not
/// `FeatureCody`.
///
/// GAP: `licenseKey` contents are not parsed/verified; instances with a paid
/// license will report the free-plan feature values (the hash below still
/// covers the configured key). On dotcom the baseline leaves
/// `LicenseInfo.BatchChanges` nil.
fn free_license_info(dotcom: bool) -> (LicenseInfo, bool, bool) {
    let batch_changes = (!dotcom).then_some(FeatureBatchChanges {
        unrestricted: false,
        max_num_changesets: 10,
    });
    (
        LicenseInfo { batch_changes },
        true,  // licensing.Check(FeatureCodeSearch) == nil
        false, // licensing.Check(FeatureCody) != nil
    )
}

/// `conf.HashedLicenseKeyForAnalytics` (`computed.go:769-777`):
/// `hex(sha256("event-logging-telemetry-prefix" + licenseKey))`.
fn hashed_license_key(license_key: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"event-logging-telemetry-prefix");
    hasher.update(license_key.as_bytes());
    hex_lower(&hasher.finalize())
}

fn hex_lower(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    out
}

/// `publicSiteConfiguration()` (`jscontext.go:657-665`): exactly these four
/// keys, so private site configuration cannot leak into anonymous shells.
/// Their declaration order in `schema.SiteConfiguration` (auth.public,
/// authz.enforceForSiteAdmins, disableNonCriticalTelemetry, update.channel)
/// matches `serde_json::Map`'s sorted order.
fn public_site_configuration(raw: &Value) -> Value {
    let mut site = serde_json::Map::new();
    if get_bool(raw, "auth.public") {
        site.insert("auth.public".to_owned(), Value::Bool(true));
    }
    if get_bool(raw, "authz.enforceForSiteAdmins") {
        site.insert("authz.enforceForSiteAdmins".to_owned(), Value::Bool(true));
    }
    if get_bool(raw, "disableNonCriticalTelemetry") {
        site.insert(
            "disableNonCriticalTelemetry".to_owned(),
            Value::Bool(true),
        );
    }
    // `conf.UpdateChannel()` defaults to "release" and is always non-empty.
    site.insert(
        "update.channel".to_owned(),
        Value::String(
            get_str(raw, "update.channel")
                .filter(|channel| !channel.is_empty())
                .unwrap_or("release")
                .to_owned(),
        ),
    );
    Value::Object(site)
}

/// `conf.AccessTokensAllow()` (`computed.go:69-84`).
fn access_tokens_allow(raw: &Value) -> &'static str {
    let Some(config) = get(raw, "auth.accessTokens") else {
        return "all-users-create";
    };
    match get_str(config, "allow").unwrap_or("") {
        "" | "all-users-create" => "all-users-create",
        "site-admin-create" => "site-admin-create",
        _ => "none",
    }
}

/// `conf.AccessTokensExpirationOptions()` (`computed.go:105-140`).
fn access_token_expiration(raw: &Value) -> (i64, Vec<i64>) {
    let default_options = vec![7, 14, 30, 60, 90];
    let Some(config) = get(raw, "auth.accessTokens") else {
        return (90, default_options);
    };
    let default_days = config
        .get("defaultExpirationDays")
        .and_then(Value::as_i64)
        .unwrap_or(90);
    let mut options: Vec<i64> = config
        .get("expirationOptionDays")
        .and_then(Value::as_array)
        .filter(|options| !options.is_empty())
        .map(|options| options.iter().filter_map(Value::as_i64).collect())
        .unwrap_or(default_options);
    if !options.contains(&default_days) {
        options.push(default_days);
    }
    options.sort_unstable();
    (default_days, options)
}

/// `conf.AuthPasswordPolicy()` (`computed.go:629-665`), including the
/// deprecated `experimentalFeatures.passwordPolicy` fallback.
fn auth_password_policy(raw: &Value) -> AuthPasswordPolicy {
    let configured = get(raw, "auth.passwordPolicy")
        .or_else(|| get(raw, "experimentalFeatures").and_then(|features| get(features, "passwordPolicy")));
    let Some(policy) = configured else {
        return AuthPasswordPolicy::default();
    };
    AuthPasswordPolicy {
        enabled: get_bool(policy, "enabled"),
        number_of_special_characters: get_i64(policy, "numberOfSpecialCharacters"),
        require_at_least_one_number: get_bool(policy, "requireAtLeastOneNumber"),
        require_upper_and_lower_case: get_bool(policy, "requireUpperandLowerCase"),
    }
}

fn auth_provider_entries(raw: &Value) -> impl Iterator<Item = &Value> {
    raw.get("auth.providers")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
}

fn builtin_configs(raw: &Value) -> impl Iterator<Item = &Value> {
    auth_provider_entries(raw).filter(|provider| get_str(provider, "type") == Some("builtin"))
}

/// `conf.AuthAllowSignup()` (`internal/conf/auth.go:37-45`).
fn auth_allow_signup(raw: &Value) -> bool {
    builtin_configs(raw).any(|provider| get_bool(provider, "allowSignup"))
}

/// `userpasswd.ResetPasswordEnabled()` (`userpasswd/config.go:15-22`):
/// exactly one builtin provider configured.
fn reset_password_enabled(raw: &Value) -> bool {
    let mut builtin = builtin_configs(raw);
    builtin.next().is_some() && builtin.next().is_none()
}

/// `conf.AuthPrimaryLoginProvidersCount()` (`computed.go:526-535`).
fn primary_login_providers_count(raw: &Value) -> i64 {
    let count = get_i64(raw, "auth.primaryLoginProvidersCount");
    if count == 0 {
        3
    } else {
        count
    }
}

/// `conf.Branding()` (`computed.go:1459-1471`): a default `Sourcegraph` name
/// when branding is absent/empty; only the schema fields are kept (unknown
/// keys are dropped, as the typed Go struct does).
fn branding(raw: &Value) -> Branding {
    let Some(configured) = get(raw, "branding") else {
        return Branding {
            brand_name: "Sourcegraph".to_owned(),
            dark: None,
            disable_symbol_spin: false,
            favicon: String::new(),
            light: None,
        };
    };
    let assets = |key: &str| {
        get(configured, key).map(|assets| BrandAssets {
            logo: get_str(assets, "logo").unwrap_or_default().to_owned(),
            symbol: get_str(assets, "symbol").unwrap_or_default().to_owned(),
        })
    };
    Branding {
        brand_name: get_str(configured, "brandName")
            .filter(|name| !name.is_empty())
            .unwrap_or("Sourcegraph")
            .to_owned(),
        dark: assets("dark"),
        disable_symbol_spin: get_bool(configured, "disableSymbolSpin"),
        favicon: get_str(configured, "favicon").unwrap_or_default().to_owned(),
        light: assets("light"),
    }
}

/// `schema.AuthProviderCommon` (`schema/schema.go:148-159`) extracted the way
/// `providers.GetAuthProviderCommon` (`auth/providers/providers.go:253-291`)
/// does: only providers whose typed config embeds the common properties have
/// them; `BuiltinAuthProvider` and `HTTPHeaderAuthProvider` do not.
struct AuthProviderCommonConfig {
    display_name: Option<String>,
    display_prefix: Option<String>,
    hidden: bool,
    order: i64,
    no_sign_in: bool,
}

fn auth_provider_common(provider: &Value, service_type: &str) -> AuthProviderCommonConfig {
    if matches!(service_type, "builtin" | "http-header") {
        return AuthProviderCommonConfig {
            display_name: None,
            display_prefix: None,
            hidden: false,
            order: 0,
            no_sign_in: false,
        };
    }
    AuthProviderCommonConfig {
        display_name: get_str(provider, "displayName")
            .filter(|name| !name.is_empty())
            .map(str::to_owned),
        display_prefix: get_str(provider, "displayPrefix").map(str::to_owned),
        hidden: get_bool(provider, "hidden"),
        order: get_i64(provider, "order"),
        no_sign_in: get_bool(provider, "noSignIn"),
    }
}

/// `providers.Info` (`auth/providers/providers.go:82-94`).
struct ProviderCachedInfo {
    display_name: String,
    service_id: String,
    client_id: String,
    authentication_url: String,
}

/// `p.CachedInfo()` for the modeled providers.
fn auth_provider_cached_info(provider: &Value, service_type: &str) -> Option<ProviderCachedInfo> {
    match service_type {
        "builtin" => Some(ProviderCachedInfo {
            display_name: "Builtin username-password authentication".to_owned(),
            service_id: String::new(),
            client_id: String::new(),
            authentication_url: String::new(),
        }),
        "http-header" => {
            let header = get_str(provider, "usernameHeader").unwrap_or_default();
            Some(ProviderCachedInfo {
                display_name: format!(
                    "HTTP authentication proxy (\"{}\" header)",
                    canonical_mime_header_key(header)
                ),
                service_id: String::new(),
                client_id: String::new(),
                authentication_url: String::new(),
            })
        }
        "openidconnect" => Some(ProviderCachedInfo {
            display_name: get_str(provider, "displayName")
                .filter(|name| !name.is_empty())
                .unwrap_or("OpenID Connect")
                .to_owned(),
            service_id: get_str(provider, "issuer").unwrap_or_default().to_owned(),
            client_id: get_str(provider, "clientID").unwrap_or_default().to_owned(),
            authentication_url: openidconnect_authentication_url(provider),
        }),
        // GAP: SAML/OAuth/... CachedInfo values are provider-specific
        // (SAML metadata probing, OAuth auth prefixes); only the raw config
        // stand-ins are used here. See the overview §5.
        _ => Some(ProviderCachedInfo {
            display_name: get_str(provider, "displayName")
                .filter(|name| !name.is_empty())
                .unwrap_or(service_type)
                .to_owned(),
            service_id: get_str(provider, "issuer").unwrap_or_default().to_owned(),
            client_id: get_str(provider, "clientID").unwrap_or_default().to_owned(),
            authentication_url: String::new(),
        }),
    }
}

/// `(*Provider).CachedInfo`'s authentication URL
/// (`auth/openidconnect/provider.go:66-79`):
/// `path.Join(authPrefix, "login")` + `pc=<providerConfigID>`.
fn openidconnect_authentication_url(provider: &Value) -> String {
    let config_id = openidconnect_provider_config_id(provider);
    let mut query = url::form_urlencoded::Serializer::new(String::new());
    query.append_pair("pc", &config_id);
    format!("/.auth/openidconnect/login?{}", query.finish())
}

/// `providerConfigID` (`auth/openidconnect/config.go:134-148`): the explicit
/// `configID`, otherwise `base64.RawURLEncoding(sha256(json.Marshal(config))[:16])`.
fn openidconnect_provider_config_id(provider: &Value) -> String {
    if let Some(config_id) = get_str(provider, "configID").filter(|value| !value.is_empty()) {
        return config_id.to_owned();
    }
    let canonical = openidconnect_config_json(provider);
    let digest = Sha256::digest(canonical.as_bytes());
    use base64::Engine as _;
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(&digest[..16])
}

/// The typed `schema.OpenIDConnectAuthProvider` re-marshaled by Go: struct
/// field order, `omitempty` for the optional fields, unknown keys dropped.
#[derive(Serialize)]
struct OpenIDConnectConfigJson {
    #[serde(rename = "allowSignup", skip_serializing_if = "Option::is_none")]
    allow_signup: Option<bool>,
    #[serde(rename = "clientID")]
    client_id: String,
    #[serde(rename = "clientSecret")]
    client_secret: String,
    #[serde(rename = "configID", skip_serializing_if = "String::is_empty")]
    config_id: String,
    #[serde(rename = "displayName", skip_serializing_if = "String::is_empty")]
    display_name: String,
    #[serde(rename = "displayPrefix", skip_serializing_if = "Option::is_none")]
    display_prefix: Option<String>,
    #[serde(rename = "hidden", skip_serializing_if = "is_false")]
    hidden: bool,
    #[serde(rename = "issuer")]
    issuer: String,
    #[serde(rename = "noSignIn", skip_serializing_if = "is_false")]
    no_sign_in: bool,
    #[serde(rename = "order", skip_serializing_if = "is_zero")]
    order: i64,
    #[serde(rename = "requireEmailDomain", skip_serializing_if = "String::is_empty")]
    require_email_domain: String,
    #[serde(rename = "singleIdentityPerUser", skip_serializing_if = "is_false")]
    single_identity_per_user: bool,
    #[serde(rename = "type")]
    service_type: String,
}

fn is_zero(value: &i64) -> bool {
    *value == 0
}

fn openidconnect_config_json(provider: &Value) -> String {
    let config = OpenIDConnectConfigJson {
        allow_signup: get_bool_opt(provider, "allowSignup"),
        client_id: get_str(provider, "clientID").unwrap_or_default().to_owned(),
        client_secret: get_str(provider, "clientSecret").unwrap_or_default().to_owned(),
        config_id: get_str(provider, "configID").unwrap_or_default().to_owned(),
        display_name: get_str(provider, "displayName").unwrap_or_default().to_owned(),
        display_prefix: get_str(provider, "displayPrefix").map(str::to_owned),
        hidden: get_bool(provider, "hidden"),
        issuer: get_str(provider, "issuer").unwrap_or_default().to_owned(),
        no_sign_in: get_bool(provider, "noSignIn"),
        order: get_i64(provider, "order"),
        require_email_domain: get_str(provider, "requireEmailDomain")
            .unwrap_or_default()
            .to_owned(),
        single_identity_per_user: get_bool(provider, "singleIdentityPerUser"),
        service_type: "openidconnect".to_owned(),
    };
    // `encoding/json` HTML-escapes these; the hash must cover the same bytes.
    serde_json::to_string(&config)
        .unwrap_or_else(|_| "null".to_owned())
        .replace('<', "\\u003c")
        .replace('>', "\\u003e")
        .replace('&', "\\u0026")
        .replace('\u{2028}', "\\u2028")
        .replace('\u{2029}', "\\u2029")
}

/// `ConfigID().ID` for the sort tie-break (only OIDC has a modeled ID).
fn auth_provider_config_id(provider: &Value, service_type: &str) -> String {
    match service_type {
        "openidconnect" => openidconnect_provider_config_id(provider),
        _ => String::new(),
    }
}

/// `providers.SortedProviders()` (`auth/providers/providers.go:216-247`) and
/// the `NewJSContextFromRequest` projection (`jscontext.go:297-330`).
///
/// GAP: `requiredForAuthz` is always false here because the modeled providers
/// (builtin/http-header/openidconnect) are never authz providers; authz
/// provider matching (`jscontext.go:317-321`) for GitHub/GitLab/... remains a
/// GAP, as do their `CachedInfo` runtime values.
fn auth_providers(raw: &Value) -> Vec<AuthProviderInfo> {
    let mut infos: Vec<(i64, bool, String, String, AuthProviderInfo)> = Vec::new();
    for provider in auth_provider_entries(raw) {
        let service_type = match get_str(provider, "type").filter(|value| !value.is_empty()) {
            Some(service_type) => service_type.to_owned(),
            None => continue,
        };
        // `GetAuthProviderCommon` + the `Hidden` skip (`jscontext.go:310-315`);
        // `noSignIn` providers stay listed.
        let common = auth_provider_common(provider, &service_type);
        if common.hidden {
            continue;
        }
        // `info := p.CachedInfo(); if info == nil { continue }`
        // (`jscontext.go:317-319`) — no modeled provider returns nil.
        let Some(cached) = auth_provider_cached_info(provider, &service_type) else {
            continue;
        };
        let info = AuthProviderInfo {
            is_builtin: service_type == "builtin",
            no_sign_in: common.no_sign_in,
            display_name: common.display_name.unwrap_or(cached.display_name),
            display_prefix: common.display_prefix,
            service_type: service_type.clone(),
            authentication_url: cached.authentication_url,
            service_id: cached.service_id,
            client_id: cached.client_id,
            required_for_authz: false,
        };
        infos.push((
            common.order,
            service_type == "builtin",
            service_type.clone(),
            auth_provider_config_id(provider, &service_type),
            info,
        ));
    }
    infos.sort_by(|left, right| {
        let (left_order, left_builtin, left_type, left_id, _) = left;
        let (right_order, right_builtin, right_type, right_id, _) = right;
        if *left_order != 0 && *right_order != 0 {
            return left_order.cmp(right_order);
        }
        if *left_order != 0 {
            return std::cmp::Ordering::Less;
        }
        if *right_order != 0 {
            return std::cmp::Ordering::Greater;
        }
        if *left_builtin && !*right_builtin {
            return std::cmp::Ordering::Less;
        }
        if *right_builtin && !*left_builtin {
            return std::cmp::Ordering::Greater;
        }
        left_type
            .cmp(right_type)
            .then_with(|| left_id.cmp(right_id))
    });
    infos.into_iter().map(|(_, _, _, _, info)| info).collect()
}

/// `textproto.CanonicalMIMEHeaderKey` for the HTTP-header provider name.
fn canonical_mime_header_key(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    let mut upper = true;
    for byte in value.bytes() {
        if byte == b'-' {
            upper = true;
            out.push('-');
        } else if upper {
            out.push((byte as char).to_ascii_uppercase());
            upper = false;
        } else {
            out.push((byte as char).to_ascii_lowercase());
        }
    }
    out
}

/// `userResolver.Session` (`graphqlbackend/user_session.go:11-33`): the actor
/// can sign out when it came from the session cookie and at least one
/// configured provider is not the HTTP-header provider. The raw configuration
/// list is consulted (as Go does), including hidden providers.
fn can_sign_out(raw: &Value, from_session_cookie: bool) -> bool {
    from_session_cookie
        && auth_provider_entries(raw)
            .any(|provider| get_str(provider, "type") != Some("http-header"))
}

fn is_bot(user_agent: &str) -> bool {
    static BOT: std::sync::OnceLock<regex::Regex> = std::sync::OnceLock::new();
    let pattern = BOT.get_or_init(|| {
        regex::Regex::new(
            r"(?i:googlecloudmonitoring|pingdom.com|go .* package http|sourcegraph e2etest|bot|crawl|slurp|spider|feed|rss|camo asset proxy|http-client|sourcegraph-client)",
        )
        .expect("static bot regex")
    });
    pattern.is_match(user_agent)
}

/// `siteid.Get` (`internal/siteid/siteid.go:29-53`): `TRACKING_APP_ID` takes
/// precedence over the `global_state` row.
fn site_id_override() -> Option<String> {
    std::env::var("TRACKING_APP_ID")
        .ok()
        .filter(|value| !value.is_empty())
}

/// `needsRepositoryConfiguration` (`graphqlbackend/site_flags.go:26-41`): site
/// admins only, `true` when no live code-host external service exists.
async fn needs_repository_configuration(pool: &PgPool, is_site_admin: bool) -> bool {
    if !is_site_admin {
        return false;
    }
    let kinds: Vec<String> =
        sqlx::query_scalar("SELECT kind FROM external_services WHERE deleted_at IS NULL")
            .fetch_all(pool)
            .await
            .unwrap_or_default();
    !kinds.iter().any(|kind| is_code_host_kind(kind))
}

/// `database.ExternalServiceKinds` entries with `CodeHost: true`
/// (`internal/database/external_services.go:205-224`), compared
/// case-insensitively because rows store the upper-case kind constants.
fn is_code_host_kind(kind: &str) -> bool {
    matches!(
        kind.to_ascii_lowercase().as_str(),
        "awscodecommit"
            | "azuredevops"
            | "bitbucketcloud"
            | "bitbucketserver"
            | "gerrit"
            | "github"
            | "gitlab"
            | "gitolite"
            | "gomodules"
            | "jvmpackages"
            | "npmpackages"
            | "other"
            | "pagure"
            | "perforce"
            | "phabricator"
            | "pythonpackages"
            | "rustpackages"
            | "rubypackages"
    )
}

/// `userEmailsStore.HasVerifiedEmail` (`internal/database/user_emails.go:117-121`).
async fn has_verified_email(pool: &PgPool, user_id: i32) -> bool {
    sqlx::query_scalar::<_, bool>(
        "SELECT EXISTS (SELECT 1 FROM user_emails WHERE user_id = $1 AND verified_at IS NOT NULL)",
    )
    .bind(user_id)
    .fetch_one(pool)
    .await
    .unwrap_or(false)
}

/// `userEmailsStore.ListByUser` (`internal/database/user_emails.go:346-356`):
/// `ORDER BY created_at ASC, email ASC`, with the `is_primary`/`verified_at`
/// columns that the shared `user_emails` store does not expose.
async fn user_emails(pool: &PgPool, user_id: i32) -> Option<Vec<UserEmail>> {
    let rows = sqlx::query(
        "SELECT email, is_primary, (verified_at IS NOT NULL) AS verified \
         FROM user_emails WHERE user_id = $1 ORDER BY created_at ASC, email ASC",
    )
    .bind(user_id)
    .fetch_all(pool)
    .await
    .ok()?;
    Some(
        rows.iter()
            .map(|row| {
                use sqlx::Row;
                UserEmail {
                    email: row.get("email"),
                    is_primary: row.get("is_primary"),
                    verified: row.get("verified"),
                }
            })
            .collect(),
    )
}

/// `permissionConnectionStore` (`graphqlbackend/user.go:785-801`,
/// `permission_connection_store.go:44-61`) over the M6 `permissions`/`roles`
/// tables: distinct permissions for the user, default pagination order
/// (`ORDER BY id DESC`, `internal/database/helpers.go:145-206`).
async fn user_permissions(pool: &PgPool, user_id: i32) -> Vec<Permission> {
    let rows = sqlx::query(
        "SELECT permissions.id, permissions.namespace, permissions.action \
         FROM permissions \
         INNER JOIN role_permissions ON role_permissions.permission_id = permissions.id \
         INNER JOIN user_roles ON user_roles.role_id = role_permissions.role_id \
         WHERE user_roles.user_id = $1 \
         GROUP BY permissions.id \
         ORDER BY permissions.id DESC",
    )
    .bind(user_id)
    .fetch_all(pool)
    .await
    .unwrap_or_default();
    rows.iter()
        .map(|row| {
            use sqlx::Row;
            let id: i32 = row.get("id");
            let namespace: String = row.get("namespace");
            let action: String = row.get("action");
            Permission {
                typename: "Permission".to_owned(),
                id: relay_id("Permission", &id.to_string()),
                display_name: format!("{namespace}#{action}"),
            }
        })
        .collect()
}

/// `relay.MarshalID`: base64 URL-safe (padded) of `Kind:id`.
fn relay_id(kind: &str, id: &str) -> String {
    use base64::Engine as _;
    base64::engine::general_purpose::URL_SAFE.encode(format!("{kind}:{id}"))
}

/// `createCurrentUser` (`jscontext.go:520-580`) for the current actor's own
/// user row. The actor owns the account, so `viewerCanAdminister` is true.
async fn create_current_user(
    pool: &PgPool,
    user: &sg_db::users::UserSessionRecord,
    from_session_cookie: bool,
    raw: &Value,
) -> CurrentUser {
    let user_id = user.id;
    let verified = has_verified_email(pool, user_id).await;
    let emails = user_emails(pool, user_id).await;
    let organizations = match sg_db::orgs::orgs_for_user(pool, user_id).await {
        Ok(orgs) => Some(UserOrganizationsConnection {
            typename: "OrgConnection".to_owned(),
            nodes: orgs
                .into_iter()
                .map(|org| UserOrganization {
                    typename: "Org".to_owned(),
                    id: relay_id("Org", &org.id.to_string()),
                    url: format!("/organizations/{}", org.name),
                    settings_url: Some(format!("/organizations/{}/settings", org.name)),
                    name: org.name,
                })
                .collect(),
        }),
        Err(_) => None,
    };
    let latest_settings = sg_db::settings::latest_for_user(pool, user_id)
        .await
        .ok()
        .flatten()
        .map(|settings| UserLatestSettings {
            id: settings.id,
            contents: settings.contents,
        });
    let url = format!("/users/{}", user.username);
    CurrentUser {
        typename: "User".to_owned(),
        id: relay_id("User", &user_id.to_string()),
        database_id: user_id,
        username: user.username.clone(),
        avatar_url: (!user.avatar_url.is_empty()).then(|| user.avatar_url.clone()),
        display_name: user.display_name.clone(),
        site_admin: user.site_admin,
        url: url.clone(),
        settings_url: format!("{url}/settings"),
        viewer_can_administer: true,
        tos_accepted: user.tos_accepted,
        has_verified_email: verified,
        organizations,
        session: Some(UserSession {
            can_sign_out: can_sign_out(raw, from_session_cookie),
        }),
        emails,
        latest_settings,
        permissions: PermissionsConnection {
            typename: "PermissionConnection".to_owned(),
            nodes: user_permissions(pool, user_id).await,
        },
    }
}

/// `NewJSContextFromRequest` (`jscontext.go:330-510`) for the page-startup
/// subset. DB reads are best-effort and mirror the baseline's error handling
/// (a failed lookup degrades the field, it never fails the request).
pub async fn new_js_context(
    pool: &PgPool,
    actor: Actor,
    meta: RequestMeta<'_>,
    env: &AppEnv,
    config: &AppConfig,
) -> JsContext {
    let raw: Value = serde_json::from_str(&config.site_raw).unwrap_or(Value::Null);
    let external = config.external_url_parsed();

    let mut headers = BTreeMap::new();
    headers.insert("x-sourcegraph-client".to_owned(), external.url.clone());
    headers.insert("X-Requested-With".to_owned(), "Sourcegraph".to_owned());
    if let Some(cache_control) = meta.cache_control {
        if cache_control.contains("no-cache") || cache_control.contains("max-age=0") {
            headers.insert("Cache-Control".to_owned(), "no-cache".to_owned());
        }
    }

    let (site_id, needs_site_init) = match site_id_override() {
        Some(override_id) => {
            // `siteid.Get` short-circuits before `GlobalState.Get`, so the
            // `SiteInitialized` query sees the table as-is.
            let initialized: bool = sqlx::query_scalar(
                "SELECT coalesce(bool_or(gs.initialized), false) FROM global_state gs",
            )
            .fetch_one(pool)
            .await
            .unwrap_or(false);
            (override_id, !initialized)
        }
        None => match sg_db::global_state::get(pool).await {
            Ok(state) => (state.site_id, !state.initialized),
            Err(_) => (String::new(), false),
        },
    };

    let user = match actor.uid() {
        Some(uid) => sg_db::users::get_session_user(pool, uid).await.ok().flatten(),
        None => None,
    };

    let current_user = match &user {
        Some(user) => Some(create_current_user(pool, user, meta.from_session_cookie, &raw).await),
        None => None,
    };
    let temporary_settings = match &user {
        Some(user) => {
            let contents = sg_db::temporary_settings::get_temporary_settings(pool, user.id)
                .await
                .unwrap_or_else(|_| "{}".to_owned());
            Some(TemporarySettings {
                typename: "TemporarySettings".to_owned(),
                contents,
            })
        }
        None => Some(TemporarySettings {
            typename: "TemporarySettings".to_owned(),
            contents: "{}".to_owned(),
        }),
    };

    let (license_info, code_search_licensed, cody_licensed) = free_license_info(env.dotcom);
    let experimental_features = get(&raw, "experimentalFeatures")
        .cloned()
        .unwrap_or_else(|| Value::Object(serde_json::Map::new()));
    let search_jobs_site_enabled = get_bool_opt(&experimental_features, "searchJobs")
        .unwrap_or_else(|| !env_flag("DISABLE_SEARCH_JOBS"));
    let access_token_expiration = access_token_expiration(&raw);
    let code_insights_enabled = !env_flag("DISABLE_CODE_INSIGHTS")
        && (env.deploy_type != "docker-container"
            || env_flag("ALLOW_SINGLE_DOCKER_CODE_INSIGHTS"));
    let batch_changes_restricted =
        get_bool_opt(&raw, "batchChanges.restrictToAdmins").unwrap_or(false);
    let is_site_admin = user.as_ref().is_some_and(|user| user.site_admin);
    let batch_changes_user_allowed = !batch_changes_restricted || is_site_admin;
    let batch_changes_enabled = !env.dotcom
        && code_search_licensed
        && !env_flag("DISABLE_BATCH_CHANGES")
        && get_bool_opt(&raw, "batchChanges.enabled").unwrap_or(true)
        && batch_changes_user_allowed;
    let needs_repository_configuration = needs_repository_configuration(pool, is_site_admin).await;

    let context = JsContext {
        app_root: String::new(),
        external_url: external.url,
        xhr_headers: headers,
        user_agent_is_bot: meta.user_agent.is_some_and(is_bot),
        assets_root: env.assets_root.clone(),
        version: env.build_version.clone(),

        is_authenticated_user: actor.is_authenticated(),
        current_user,
        temporary_settings,

        sentry_dsn: get(&raw, "log")
            .and_then(|log| get(log, "sentry"))
            .and_then(|sentry| get_str(sentry, "dsn"))
            .filter(|dsn| !dsn.is_empty())
            .map(str::to_owned),
        open_telemetry: get(&raw, "observability.client")
            .and_then(|client| get(client, "openTelemetry"))
            .map(|otel| {
                let mut out = serde_json::Map::new();
                if let Some(endpoint) = get_str(otel, "endpoint") {
                    if !endpoint.is_empty() {
                        out.insert("endpoint".to_owned(), Value::String(endpoint.to_owned()));
                    }
                }
                Value::Object(out)
            }),

        site_id,
        site_gql_id: relay_id("Site", "\"site\""),
        debug: env.insecure_dev,
        needs_site_init,
        email_enabled: get(&raw, "email.smtp").is_some(),

        site: public_site_configuration(&raw),
        // GAP: `configurationServer.NeedServerRestart()` is not modelled.
        need_server_restart: false,
        deploy_type: env.deploy_type.clone(),

        sourcegraph_dot_com_mode: env.dotcom,

        access_tokens_allow: access_tokens_allow(&raw).to_owned(),
        access_tokens_allow_no_expiration: get(&raw, "auth.accessTokens")
            .and_then(|tokens| get_bool_opt(tokens, "allowNoExpiration"))
            .unwrap_or(true),
        access_tokens_expiration_days_default: access_token_expiration.0,
        access_tokens_expiration_days_options: access_token_expiration.1,

        allow_signup: auth_allow_signup(&raw),

        reset_password_enabled: reset_password_enabled(&raw),

        auth_min_password_length: {
            let configured = get_i64(&raw, "auth.minPasswordLength");
            if configured <= 0 {
                12
            } else {
                configured
            }
        },
        auth_password_policy: auth_password_policy(&raw),

        auth_providers: auth_providers(&raw),
        primary_login_providers_count: primary_login_providers_count(&raw),

        auth_access_request: get(&raw, "auth.accessRequest").map(|request| {
            let mut out = serde_json::Map::new();
            if let Some(enabled) = get_bool_opt(request, "enabled") {
                out.insert("enabled".to_owned(), Value::Bool(enabled));
            }
            Value::Object(out)
        }),

        branding: branding(&raw),

        // `batches.IsEnabled() && enterprise.BatchChangesEnabledForUser` with
        // the baseline's `!codeSearchLicensed` force-disable applied.
        batch_changes_enabled,
        batch_changes_disable_webhooks_warning: get_bool(
            &raw,
            "batchChanges.disableWebhooksWarning",
        ),
        batch_changes_webhook_logs_enabled: webhook_logs_enabled(&raw),

        // GAP: Cody runtime configuration (`conf.CodyEnabled`, dotcom) is out
        // of scope; on the free plan the baseline force-disables both after
        // `!codyLicensed`.
        cody_enabled_on_instance: false,
        cody_enabled_for_current_user: false,
        cody_requires_verified_email: false,

        code_search_enabled_on_instance: code_search_licensed,

        executors_enabled: code_search_licensed
            && get_str(&raw, "executors.accessToken").is_some_and(|token| !token.is_empty()),
        code_intel_auto_indexing_enabled: get_bool_opt(&raw, "codeIntelAutoIndexing.enabled")
            .unwrap_or(false),
        code_intel_auto_indexing_allow_global_policies: get_bool_opt(
            &raw,
            "codeIntelAutoIndexing.allowGlobalPolicies",
        )
        .unwrap_or(false),
        code_intel_ranking_document_reference_counts_enabled: get_bool_opt(
            &raw,
            "codeIntelRanking.documentReferenceCountsEnabled",
        )
        .unwrap_or(false),

        code_insights_enabled: code_insights_enabled && code_search_licensed,
        appliance_update_target: std::env::var("APPLIANCE_UPDATE_TARGET").unwrap_or_default(),
        appliance_menu_target: std::env::var("APPLIANCE_MENU_TARGET").unwrap_or_default(),
        code_intelligence_enabled: code_search_licensed,
        search_contexts_enabled: code_search_licensed && !env_flag("DISABLE_SEARCH_CONTEXTS"),
        notebooks_enabled: code_search_licensed && !env_flag("DISABLE_NOTEBOOKS"),
        code_monitoring_enabled: code_search_licensed && !env_flag("DISABLE_CODE_MONITORS"),
        search_aggregation_enabled: code_search_licensed,
        own_enabled: code_search_licensed && !env_flag("DISABLE_OWN"),
        search_jobs_enabled: code_search_licensed && search_jobs_site_enabled,

        redirect_unsupported_browser: get_bool(&raw, "RedirectUnsupportedBrowser"),

        product_research_page_enabled: get_bool_opt(&raw, "productResearchPage.enabled")
            .unwrap_or(true),

        experimental_features,

        license_info,

        hashed_license_key: hashed_license_key(get_str(&raw, "licenseKey").unwrap_or_default()),

        outbound_request_log_limit: get_i64(&raw, "outboundRequestLogLimit"),

        disable_feedback_survey: get_bool(&raw, "disableFeedbackSurvey"),

        needs_repository_configuration,

        extsvc_config_file_exists: std::env::var("EXTSVC_CONFIG_FILE")
            .is_ok_and(|value| !value.is_empty()),
        extsvc_config_allow_edits: std::env::var("EXTSVC_CONFIG_ALLOW_EDITS")
            .map(|value| go_parse_bool(&value))
            .unwrap_or(false),

        running_on_macos: cfg!(target_os = "macos"),

        svelte_kit: SvelteKitJsContext {
            enabled_routes: Vec::new(),
            show_toggle: true,
            known_routes: Vec::new(),
        },

        frontend_cody_pro_config: None,
    };
    cody_gate(context, cody_licensed)
}
