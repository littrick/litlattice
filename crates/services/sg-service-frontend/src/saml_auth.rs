#![forbid(unsafe_code)]

//! SAML 2.0 service provider (`cmd/frontend/internal/auth/saml`).
//!
//! Ports the baseline configuration parsing/validation (`config.go`), IdP
//! metadata fetching and SP construction (`provider.go`), the HTTP endpoints
//! (`middleware.go`) and the AuthnResponse validation / user provisioning
//! (`user.go`, `session.go`).
//!
//! Design notes and the differential-test contract live in
//! `docs/overviews/m9_8jt_saml_overview.md`. Deliberate seams:
//!
//! * IdP metadata HTTP access is injected ([`SamlHttpClient`]); no test touches
//!   the network.
//! * XML signatures are verified with a targeted c14n 1.0/1.1 + RSA PKCS#1 v1.5
//!   implementation (the algorithms `goxmldsig` accepts for RSA). ECDSA
//!   signatures are not supported (errata).
//! * User creation reuses [`crate::user_provisioning`]; the external-account
//!   data blob is not written (same seam limitation as M9.8jp).
//! * No XML external entities or remote schemas are loaded.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use async_trait::async_trait;
use base64::Engine;
use serde_json::{Map, Value};
use time::format_description::well_known::Rfc3339;
use time::{Date, Month, OffsetDateTime, PrimitiveDateTime, Time, UtcOffset};

use sg_config::SiteConfiguration;
use sg_session::write::{SessionManager, SetCookie, WriteOptions};
use sg_session::{Actor, GoTime, SessionIdSource, SessionStore};

use crate::auth_providers_graphql::{go_json_saml, hash_config_id, saml_service_provider_issuer};
use crate::oauth_login::{
    add_post_auth_redirect_parameters_to_string, encode_query_sorted, go_parse_url, go_url_string,
    parse_query, query_get, safe_redirect_url,
};
use crate::user_provisioning::{
    self, ExternalIdentity, OsUsernameSuffix, ProvisionDeps, ProvisionError, ProvisionOutcome,
    UsernameSuffixSource,
};
use sha2::Digest as _;

use rsa::pkcs1::DecodeRsaPrivateKey as _;
use rsa::pkcs8::DecodePrivateKey as _;

/// `authPrefix` (`saml/middleware.go:23`).
pub const AUTH_PREFIX: &str = "/.auth/saml";
const PROVIDER_TYPE: &str = "saml";
const DEFAULT_NAME_ID_FORMAT: &str = "urn:oasis:names:tc:SAML:2.0:nameid-format:persistent";
const SUCCESS_STATUS: &str = "urn:oasis:names:tc:SAML:2.0:status:Success";
const BEARER_METHOD: &str = "urn:oasis:names:tc:SAML:2.0:cm:bearer";
const DSIG_NS: &str = "http://www.w3.org/2000/09/xmldsig#";

const ERR_MISCONFIGURED: &str = "Misconfigured SAML auth provider";
const ERR_GET_PROVIDER: &str = "Unexpected error getting SAML authentication provider. This may indicate that the SAML IdP does not exist. Ask a site admin to check the server \"frontend\" logs for \"Error getting SAML auth provider\".";
const ERR_VALIDATE_ASSERTIONS: &str = "Error validating SAML assertions. Try signing in again. If the problem persists, a site admin must check the configuration.";
const ERR_NOT_IN_GROUPS: &str = "Error authorizing SAML-authenticated user. The user does not belong to one of the configured groups.";
const ERR_START_SESSION: &str = "Error starting SAML-authenticated session. Try signing in again.";
const ERR_BUILD_AUTH_URL: &str = "Unexpected error in SAML authentication provider.";
const ERR_LOGOUT_VALIDATE: &str = "Error validating SAML logout response.";
const ERR_LOGOUT_SESSION: &str = "Error signing out of SAML-authenticated session.";

// ---------------------------------------------------------------------------
// config.go
// ---------------------------------------------------------------------------

/// `schema.SAMLAuthProvider` plus the derived issuer and canonical JSON.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SamlProviderConfig {
    /// `nil` and `[]` are distinct (see `allowSignin`).
    pub allow_groups: Option<Vec<String>>,
    pub allow_signup: Option<bool>,
    pub config_id: String,
    pub display_name: String,
    pub display_prefix: Option<String>,
    pub groups_attribute_name: String,
    pub hidden: bool,
    pub identity_provider_metadata: String,
    pub identity_provider_metadata_url: String,
    pub insecure_skip_assertion_signature_validation: bool,
    pub name_id_format: String,
    pub no_sign_in: bool,
    pub order: i32,
    pub service_provider_certificate: String,
    pub service_provider_issuer: String,
    pub service_provider_private_key: String,
    pub sign_requests: Option<bool>,
    pub username_attribute_names: Option<Vec<String>>,
    /// `json.Marshal` of the defaulted config, used for provider IDs and
    /// duplicate detection.
    pub canonical_json: String,
}

fn string_field(obj: &Map<String, Value>, key: &str) -> String {
    obj.get(key)
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_owned()
}

fn bool_field(obj: &Map<String, Value>, key: &str) -> bool {
    obj.get(key).and_then(Value::as_bool).unwrap_or(false)
}

fn pointer_bool_field(obj: &Map<String, Value>, key: &str) -> Option<bool> {
    obj.get(key).and_then(Value::as_bool)
}

fn string_list_field(obj: &Map<String, Value>, key: &str) -> Option<Vec<String>> {
    obj.get(key).and_then(Value::as_array).map(|items| {
        items
            .iter()
            .filter_map(Value::as_str)
            .map(str::to_owned)
            .collect()
    })
}

/// `saml.withConfigDefaults` (`config.go:144-158`).
pub fn with_config_defaults(obj: &Map<String, Value>, external_url: &str) -> SamlProviderConfig {
    let mut config = SamlProviderConfig {
        allow_groups: string_list_field(obj, "allowGroups"),
        allow_signup: pointer_bool_field(obj, "allowSignup"),
        config_id: string_field(obj, "configID"),
        display_name: string_field(obj, "displayName"),
        display_prefix: obj
            .get("displayPrefix")
            .and_then(Value::as_str)
            .map(str::to_owned),
        groups_attribute_name: string_field(obj, "groupsAttributeName"),
        hidden: bool_field(obj, "hidden"),
        identity_provider_metadata: string_field(obj, "identityProviderMetadata"),
        identity_provider_metadata_url: string_field(obj, "identityProviderMetadataURL"),
        insecure_skip_assertion_signature_validation: bool_field(
            obj,
            "insecureSkipAssertionSignatureValidation",
        ),
        name_id_format: string_field(obj, "nameIDFormat"),
        no_sign_in: bool_field(obj, "noSignIn"),
        order: obj.get("order").and_then(Value::as_i64).unwrap_or(0) as i32,
        service_provider_certificate: string_field(obj, "serviceProviderCertificate"),
        service_provider_issuer: string_field(obj, "serviceProviderIssuer"),
        service_provider_private_key: string_field(obj, "serviceProviderPrivateKey"),
        sign_requests: pointer_bool_field(obj, "signRequests"),
        username_attribute_names: string_list_field(obj, "usernameAttributeNames"),
        canonical_json: String::new(),
    };

    let default_issuer = saml_service_provider_issuer(obj, external_url);
    if config.service_provider_issuer.is_empty() {
        config.service_provider_issuer = default_issuer;
    }
    config.canonical_json = go_json_saml(obj, &config.service_provider_issuer);
    config
}

impl SamlProviderConfig {
    /// `getNameIDFormat` (`config.go:160-168`).
    pub fn name_id_format(&self) -> String {
        if self.name_id_format.is_empty() {
            DEFAULT_NAME_ID_FORMAT.to_owned()
        } else {
            self.name_id_format.clone()
        }
    }

    /// `providerConfigID` (`config.go:176-189`).
    pub fn provider_config_id(&self, multiple: bool) -> String {
        if !self.config_id.is_empty() {
            return self.config_id.clone();
        }
        if !multiple {
            return String::new();
        }
        hash_config_id(&self.canonical_json)
    }
}

/// All SAML entries from `auth.providers` (`getProviders`, `config.go:101-116`).
pub fn saml_provider_configs(site: &SiteConfiguration) -> (Vec<SamlProviderConfig>, bool) {
    let raw = site.raw();
    let external_url = raw
        .get("externalURL")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let entries = raw
        .get("auth.providers")
        .and_then(Value::as_array)
        .map(Vec::as_slice)
        .unwrap_or_default();
    let mut configs = Vec::new();
    for entry in entries {
        let Some(obj) = entry.as_object() else {
            continue;
        };
        if obj.get("type").and_then(Value::as_str) != Some(PROVIDER_TYPE) {
            continue;
        }
        configs.push(with_config_defaults(obj, external_url));
    }
    let multiple = configs.len() >= 2;
    (configs, multiple)
}

/// `saml.validateConfig` (`config.go:118-142`).
pub fn validate_config(site: &SiteConfiguration) -> Vec<String> {
    let raw = site.raw();
    let external_url = raw
        .get("externalURL")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let entries = raw
        .get("auth.providers")
        .and_then(Value::as_array)
        .map(Vec::as_slice)
        .unwrap_or_default();

    let mut problems = Vec::new();
    let mut logged_needs_external_url = false;
    for entry in entries {
        let Some(obj) = entry.as_object() else {
            continue;
        };
        if obj.get("type").and_then(Value::as_str) == Some(PROVIDER_TYPE)
            && external_url.is_empty()
            && !logged_needs_external_url
        {
            problems.push("saml auth provider requires `externalURL` to be set to the external URL of your site (example: https://sourcegraph.example.com)".to_owned());
            logged_needs_external_url = true;
        }
    }

    let mut seen: BTreeMap<String, usize> = BTreeMap::new();
    for (index, entry) in entries.iter().enumerate() {
        let Some(obj) = entry.as_object() else {
            continue;
        };
        if obj.get("type").and_then(Value::as_str) != Some(PROVIDER_TYPE) {
            continue;
        }
        // `json.Marshal(*p.Saml)` on the un-defaulted config.
        let key = go_json_saml(obj, "");
        if let Some(first) = seen.get(&key) {
            problems.push(format!(
                "SAML auth provider at index {index} is duplicate of index {first}, ignoring"
            ));
        } else {
            seen.insert(key, index);
        }
    }
    problems
}

/// `relayState` (`middleware.go:265-290`).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RelayState {
    pub provider_id: String,
    pub return_to_url: String,
}

impl RelayState {
    pub fn encode(&self) -> String {
        let json = format!(
            "{{\"k\":{},\"r\":{}}}",
            go_json_string(&self.provider_id),
            go_json_string(&self.return_to_url)
        );
        base64::engine::general_purpose::STANDARD.encode(json.as_bytes())
    }

    pub fn decode(encoded: &str) -> Self {
        if encoded.starts_with("http://") || encoded.starts_with("https://") || encoded.is_empty() {
            return Self {
                provider_id: String::new(),
                return_to_url: encoded.to_owned(),
            };
        }
        let Ok(bytes) = base64::engine::general_purpose::STANDARD.decode(encoded) else {
            return Self::default();
        };
        let Ok(value) = serde_json::from_slice::<Value>(&bytes) else {
            return Self::default();
        };
        let Some(obj) = value.as_object() else {
            return Self::default();
        };
        let provider_id = match obj.get("k") {
            None => String::new(),
            Some(Value::String(value)) => value.clone(),
            Some(_) => return Self::default(),
        };
        let return_to_url = match obj.get("r") {
            None => String::new(),
            Some(Value::String(value)) => value.clone(),
            Some(_) => return Self::default(),
        };
        Self {
            provider_id,
            return_to_url,
        }
    }
}

fn go_json_string(value: &str) -> String {
    serde_json::to_string(value).expect("string serializes")
}

/// `allowSignin` (`middleware.go:292-303`).
pub fn allow_signin(config: &SamlProviderConfig, groups: &BTreeMap<String, bool>) -> bool {
    let Some(allow_groups) = &config.allow_groups else {
        return true;
    };
    allow_groups
        .iter()
        .any(|group| groups.get(group).copied().unwrap_or(false))
}

// ---------------------------------------------------------------------------
// IdP metadata / provider.go
// ---------------------------------------------------------------------------

/// One injected HTTP response for IdP metadata.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SamlMetadataResponse {
    pub status: u16,
    pub body: Vec<u8>,
}

/// `httpcli.ExternalClient` seam (`provider.go:284-311`).
#[async_trait]
pub trait SamlHttpClient: Send + Sync {
    async fn fetch(&self, url: &str) -> Result<SamlMetadataResponse, String>;
}

/// Production client (no redirects beyond reqwest's defaults).
pub struct ReqwestSamlHttpClient {
    client: reqwest::Client,
}

impl ReqwestSamlHttpClient {
    pub fn new() -> Self {
        Self {
            client: reqwest::Client::new(),
        }
    }
}

impl Default for ReqwestSamlHttpClient {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl SamlHttpClient for ReqwestSamlHttpClient {
    async fn fetch(&self, url: &str) -> Result<SamlMetadataResponse, String> {
        let response = self
            .client
            .get(url)
            .send()
            .await
            .map_err(|err| err.to_string())?;
        let status = response.status().as_u16();
        let body = response
            .bytes()
            .await
            .map_err(|err| err.to_string())?
            .to_vec();
        Ok(SamlMetadataResponse { status, body })
    }
}

/// The `saml2.SAMLServiceProvider` subset the middleware reads
/// (`provider.go:112-191`).
#[derive(Debug, Clone, Default)]
pub struct SamlServiceProvider {
    pub identity_provider_sso_url: String,
    pub identity_provider_issuer: String,
    pub service_provider_issuer: String,
    pub name_id_format: String,
    pub skip_signature_validation: bool,
    pub assertion_consumer_service_url: String,
    pub audience_uri: String,
    pub sign_authn_requests: bool,
    pub idp_certificates: Vec<Vec<u8>>,
    pub sp_certificate: Option<Vec<u8>>,
    pub sp_private_key: Option<Vec<u8>>,
}

/// `providers.Info` subset (`provider.go:79-104`).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SamlProviderInfo {
    pub display_name: String,
    pub authentication_url: String,
    pub service_id: String,
    pub client_id: String,
}

/// `provider` (`provider.go:31-59`).
pub struct SamlProvider {
    pub config: SamlProviderConfig,
    pub multiple: bool,
    http: Arc<dyn SamlHttpClient>,
    state: tokio::sync::Mutex<ProviderState>,
}

#[derive(Default)]
struct ProviderState {
    service_provider: Option<SamlServiceProvider>,
    refresh_error: Option<String>,
}

impl SamlProvider {
    pub fn new(config: SamlProviderConfig, multiple: bool, http: Arc<dyn SamlHttpClient>) -> Self {
        Self {
            config,
            multiple,
            http,
            state: tokio::sync::Mutex::new(ProviderState::default()),
        }
    }

    pub fn config_id(&self) -> String {
        self.config.provider_config_id(self.multiple)
    }

    /// `Refresh` (`provider.go:61-66`).
    pub async fn refresh(&self, external_url: &str) -> Result<(), String> {
        let result = get_service_provider(&self.config, self.http.as_ref(), external_url).await;
        let mut state = self.state.lock().await;
        match result {
            Ok(service_provider) => {
                state.service_provider = Some(service_provider);
                state.refresh_error = None;
                Ok(())
            }
            Err(error) => {
                state.service_provider = None;
                state.refresh_error = Some(error.clone());
                Err(error)
            }
        }
    }

    /// `getCachedInfoAndError` (`provider.go:79-104`).
    pub async fn cached_info(&self) -> (SamlProviderInfo, Option<String>) {
        let mut authentication_url = "/.auth/saml/login".to_owned();
        if self.multiple {
            authentication_url.push_str("?pc=");
            authentication_url.push_str(&url_query_escape(&self.config_id()));
        }
        let mut info = SamlProviderInfo {
            display_name: self.config.display_name.clone(),
            authentication_url,
            ..SamlProviderInfo::default()
        };
        if info.display_name.is_empty() {
            info.display_name = "SAML".to_owned();
        }
        let state = self.state.lock().await;
        let error = match (&state.refresh_error, &state.service_provider) {
            (Some(error), _) => Some(format!(
                "failed to initialize SAML Service Provider: {error}"
            )),
            (None, None) => Some("SAML Service Provider is not yet initialized".to_owned()),
            (None, Some(_)) => None,
        };
        if let Some(service_provider) = &state.service_provider {
            info.service_id = service_provider.identity_provider_issuer.clone();
            info.client_id = service_provider.service_provider_issuer.clone();
        }
        (info, error)
    }

    async fn service_provider(&self) -> Result<SamlServiceProvider, String> {
        let state = self.state.lock().await;
        match (&state.refresh_error, &state.service_provider) {
            (Some(error), _) => Err(format!(
                "failed to initialize SAML Service Provider: {error}"
            )),
            (None, None) => Err("SAML Service Provider is not yet initialized".to_owned()),
            (None, Some(service_provider)) => Ok(service_provider.clone()),
        }
    }

    /// `Config()` (`provider.go:52-55`). `schema.AuthProviders.MarshalJSON`
    /// emits the SAML config itself, which is exactly the defaulted config's
    /// canonical JSON.
    pub fn config_json(&self) -> &str {
        &self.config.canonical_json
    }

    /// `Type()` (`provider.go:57-59`).
    pub fn provider_type(&self) -> &'static str {
        PROVIDER_TYPE
    }

    /// `ExternalAccountInfo` (`provider.go:68-70`): delegates to
    /// `GetPublicExternalAccountData` over the account's `Data` blob.
    pub async fn external_account_info(
        &self,
        account_data: &SamlAccountData,
        key: Option<&dyn sg_encryption::Key>,
    ) -> Result<Option<SamlPublicAccountData>, String> {
        get_public_external_account_data(account_data, key).await
    }
}

/// The registered SAML provider set.
pub struct SamlProviderSet {
    providers: Vec<Arc<SamlProvider>>,
    multiple: bool,
}

impl SamlProviderSet {
    pub fn from_configs(configs: Vec<SamlProviderConfig>, http: Arc<dyn SamlHttpClient>) -> Self {
        let multiple = configs.len() >= 2;
        let providers = configs
            .into_iter()
            .map(|config| Arc::new(SamlProvider::new(config, multiple, http.clone())))
            .collect();
        Self {
            providers,
            multiple,
        }
    }

    pub fn from_site(site: &SiteConfiguration, http: Arc<dyn SamlHttpClient>) -> Self {
        let (configs, _) = saml_provider_configs(site);
        Self::from_configs(configs, http)
    }

    pub fn is_multiple(&self) -> bool {
        self.multiple
    }

    pub fn providers(&self) -> &[Arc<SamlProvider>] {
        &self.providers
    }

    /// `getProvider` (`config.go:30-51`): exact config ID first, then the
    /// single-provider fallback.
    pub fn get_provider(&self, pc_id: &str) -> Option<Arc<SamlProvider>> {
        if let Some(provider) = self
            .providers
            .iter()
            .find(|provider| provider.config_id() == pc_id)
        {
            return Some(provider.clone());
        }
        if self.providers.len() == 1 {
            return Some(self.providers[0].clone());
        }
        None
    }
}

// ---------------------------------------------------------------------------
// user.go:250-321 — external account public data
// ---------------------------------------------------------------------------

/// `SAMLValue` (`user.go:246-248`). The struct has no JSON tag; Go's
/// `encoding/json` matches the `value` key case-insensitively.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SamlValue {
    pub value: String,
}

/// `SAMLAttribute` (`user.go:242-244`).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SamlAttribute {
    pub values: Vec<SamlValue>,
}

/// `SAMLValues` (`user.go:238-240`). A `None` map is Go's nil map.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SamlValues {
    pub values: Option<BTreeMap<String, SamlAttribute>>,
}

/// The subset of `extsvc.PublicAccountData` SAML ever populates
/// (`internal/extsvc/types.go:54-59`). `Login`/`URL` stay empty, which the
/// GraphQL resolver renders as null.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SamlPublicAccountData {
    pub display_name: String,
}

/// `extsvc.EncryptableData` (`internal/extsvc/types.go:46-64`): either a
/// plaintext value (`NewUnencryptedData`) or ciphertext plus the
/// `user_external_accounts.encryption_key_id`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SamlEncryptableData {
    Unencrypted(Vec<u8>),
    Encrypted { cipher: Vec<u8>, key_id: String },
}

/// `extsvc.AccountData`. SAML reads only [`SamlAccountData::data`] (the
/// `account_data` column); `auth_data` is carried so callers can prove it is
/// ignored.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SamlAccountData {
    pub auth_data: Option<SamlEncryptableData>,
    pub data: Option<SamlEncryptableData>,
}

/// Case-insensitive object key lookup, matching Go's `encoding/json` field
/// matching (`"values"` and `"VALUES"` both select struct field `Values`).
fn find_case_insensitive<'a>(obj: &'a Map<String, Value>, key: &str) -> Option<&'a Value> {
    if let Some(value) = obj.get(key) {
        return Some(value);
    }
    obj.iter()
        .find(|(candidate, _)| candidate.eq_ignore_ascii_case(key))
        .map(|(_, value)| value)
}

/// Go `encoding/json` type-kind word used in `UnmarshalTypeError` texts.
fn go_json_kind(value: &Value) -> &'static str {
    match value {
        Value::Null => "null",
        Value::Bool(_) => "bool",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}

/// Go's `strconv.Quote`-style rendering of the offending byte in
/// `encoding/json` syntax errors (only the bytes reachable from JSON text).
fn go_json_char(byte: u8) -> String {
    match byte {
        b'\'' => "'\\''".to_owned(),
        b'\\' => "'\\\\'".to_owned(),
        b'\n' => "'\\n'".to_owned(),
        b'\r' => "'\\r'".to_owned(),
        b'\t' => "'\\t'".to_owned(),
        _ => format!("'{}'", byte as char),
    }
}

/// Byte offset serde reported, falling back to the last byte.
fn serde_error_offset(bytes: &[u8], error: &serde_json::Error) -> usize {
    let column = error.column().saturating_sub(1);
    if error.line() <= 1 {
        return column.min(bytes.len().saturating_sub(1));
    }
    let mut offset = 0usize;
    let mut line = 1usize;
    while line < error.line() {
        match bytes[offset..].iter().position(|byte| *byte == b'\n') {
            Some(position) => {
                offset += position + 1;
                line += 1;
            }
            None => return bytes.len().saturating_sub(1),
        }
    }
    (offset + column).min(bytes.len().saturating_sub(1))
}

/// Map a `serde_json` syntax error onto the `encoding/json` text for the
/// corpus shapes (invalid first value, object key/value, trailing data).
/// Truncated input always reports `unexpected end of JSON input`.
fn go_json_unmarshal_error(bytes: &[u8], error: &serde_json::Error) -> String {
    let text = String::from_utf8_lossy(bytes);
    if text.trim().is_empty() || error.is_eof() {
        return "unexpected end of JSON input".to_owned();
    }
    let message = error.to_string();
    let offset = serde_error_offset(bytes, error);
    let byte = bytes
        .get(offset)
        .copied()
        .unwrap_or_else(|| text.as_bytes().last().copied().unwrap_or(b' '));
    let character = go_json_char(byte);
    if message.contains("key must be a string") {
        return format!("invalid character {character} looking for beginning of object key string");
    }
    if message.contains("expected value") {
        return format!("invalid character {character} looking for beginning of value");
    }
    if message.contains("trailing characters") {
        return format!("invalid character {character} after top-level value");
    }
    if message.contains("expected `,` or `}`") {
        return format!("invalid character {character} after object key:value pair");
    }
    if message.contains("expected `,` or `]`") {
        return format!("invalid character {character} after array element");
    }
    if message.contains("expected `:`") {
        return format!("invalid character {character} after object key");
    }
    // Remaining serde diagnostics (strings/escapes/numbers) are outside the
    // differential corpus; report the first offending value byte like Go.
    format!("invalid character {character} looking for beginning of value")
}

/// `json.Unmarshal([]byte(...), &SAMLValues{})` with Go's field matching and
/// type-error texts.
fn parse_saml_values(bytes: &[u8]) -> Result<SamlValues, String> {
    let value: Value =
        serde_json::from_slice(bytes).map_err(|error| go_json_unmarshal_error(bytes, &error))?;
    let object = match value {
        Value::Null => return Ok(SamlValues::default()),
        Value::Object(object) => object,
        other => {
            return Err(format!(
                "json: cannot unmarshal {} into Go value of type saml.SAMLValues",
                go_json_kind(&other)
            ))
        }
    };
    let Some(values) = find_case_insensitive(&object, "Values") else {
        return Ok(SamlValues::default());
    };
    if values.is_null() {
        return Ok(SamlValues::default());
    }
    let Some(attributes) = values.as_object() else {
        return Err(format!(
            "json: cannot unmarshal {} into Go struct field SAMLValues.Values of type map[string]saml.SAMLAttribute",
            go_json_kind(values)
        ));
    };
    let mut parsed = BTreeMap::new();
    for (name, attribute) in attributes {
        let Some(attribute) = attribute.as_object() else {
            return Err(format!(
                "json: cannot unmarshal {} into Go value of type saml.SAMLAttribute",
                go_json_kind(attribute)
            ));
        };
        let mut values = Vec::new();
        if let Some(items) = find_case_insensitive(attribute, "Values") {
            if !items.is_null() {
                let Some(items) = items.as_array() else {
                    return Err(format!(
                        "json: cannot unmarshal {} into Go struct field SAMLAttribute.Values of type []saml.SAMLValue",
                        go_json_kind(items)
                    ));
                };
                for item in items {
                    let Some(item) = item.as_object() else {
                        return Err(format!(
                            "json: cannot unmarshal {} into Go value of type saml.SAMLValue",
                            go_json_kind(item)
                        ));
                    };
                    let mut value = SamlValue::default();
                    if let Some(raw) = find_case_insensitive(item, "Value") {
                        if !raw.is_null() {
                            let Some(raw) = raw.as_str() else {
                                return Err(format!(
                                    "json: cannot unmarshal {} into Go struct field SAMLValue.Values.Value of type string",
                                    go_json_kind(raw)
                                ));
                            };
                            value.value = raw.to_owned();
                        }
                    }
                    values.push(value);
                }
            }
        }
        parsed.insert(name.clone(), SamlAttribute { values });
    }
    Ok(SamlValues {
        values: Some(parsed),
    })
}

/// `GetExternalAccountData` (`user.go:250-263`).
pub async fn get_external_account_data(
    account_data: &SamlAccountData,
    key: Option<&dyn sg_encryption::Key>,
) -> Result<SamlValues, String> {
    let Some(data) = &account_data.data else {
        return Err("could not find data for the external account".to_owned());
    };
    let plaintext = match data {
        SamlEncryptableData::Unencrypted(bytes) => String::from_utf8_lossy(bytes).into_owned(),
        SamlEncryptableData::Encrypted { cipher, key_id } => {
            let cipher = String::from_utf8_lossy(cipher);
            sg_encryption::maybe_decrypt(key, &cipher, key_id)
                .await
                .map_err(|error| error.to_string())?
        }
    };
    parse_saml_values(plaintext.as_bytes())
}

/// `GetPublicExternalAccountData` (`user.go:265-309`). Returns `Ok(None)` when
/// no candidate attribute carries a non-empty first value.
pub async fn get_public_external_account_data(
    account_data: &SamlAccountData,
    key: Option<&dyn sg_encryption::Key>,
) -> Result<Option<SamlPublicAccountData>, String> {
    let data = get_external_account_data(account_data, key).await?;
    let Some(values) = &data.values else {
        return Err("could not find data values for external account".to_owned());
    };

    // `strings.ToLower` over the keys, then the fixed candidate order.
    let mut lower_case_values: BTreeMap<String, &SamlAttribute> = BTreeMap::new();
    for (key, value) in values {
        lower_case_values.insert(key.to_lowercase(), value);
    }
    const CANDIDATES: &[&str] = &[
        "nickname",
        "login",
        "username",
        "name",
        "http://schemas.xmlsoap.org/claims/name",
        "http://schemas.xmlsoap.org/ws/2005/05/identity/claims/name",
        "email",
        "emailaddress",
        "http://schemas.xmlsoap.org/claims/emailaddress",
        "http://schemas.xmlsoap.org/ws/2005/05/identity/claims/emailaddress",
    ];
    for candidate in CANDIDATES {
        if let Some(attribute) = lower_case_values.get(*candidate) {
            if let Some(first) = attribute.values.first() {
                if !first.value.is_empty() {
                    return Ok(Some(SamlPublicAccountData {
                        display_name: first.value.clone(),
                    }));
                }
            }
        }
    }
    Ok(None)
}

struct ProviderFileConfig {
    key_pair: Option<(Vec<u8>, Vec<u8>)>,
    metadata_url: Option<String>,
    metadata: Option<Vec<u8>>,
}

/// `readProviderConfig` (`provider.go:234-282`).
fn read_provider_config(config: &SamlProviderConfig) -> Result<ProviderFileConfig, String> {
    let mut key_pair = None;
    if !config.service_provider_certificate.is_empty()
        && !config.service_provider_private_key.is_empty()
    {
        let certificate = pem_der(&config.service_provider_certificate)
            .ok_or_else(|| "failed to parse service provider certificate".to_owned())?;
        let private_key = private_key_der(&config.service_provider_private_key)
            .ok_or_else(|| "failed to parse service provider private key".to_owned())?;
        key_pair = Some((private_key, certificate));
    }

    match (
        !config.identity_provider_metadata_url.is_empty(),
        !config.identity_provider_metadata.is_empty(),
    ) {
        (true, true) => Err("invalid SAML configuration: set either identityProviderMetadataURL or identityProviderMetadata, not both".to_owned()),
        (true, false) => Ok(ProviderFileConfig {
            key_pair,
            metadata_url: Some(config.identity_provider_metadata_url.clone()),
            metadata: None,
        }),
        (false, true) => Ok(ProviderFileConfig {
            key_pair,
            metadata_url: None,
            metadata: Some(config.identity_provider_metadata.as_bytes().to_vec()),
        }),
        (false, false) => Err("invalid SAML configuration: must provide the SAML metadata, using either identityProviderMetadataURL (URL where XML file is available) or identityProviderMetadata (XML file contents)".to_owned()),
    }
}

/// `getServiceProvider` (`provider.go:112-191`).
pub async fn get_service_provider(
    config: &SamlProviderConfig,
    http: &dyn SamlHttpClient,
    external_url: &str,
) -> Result<SamlServiceProvider, String> {
    let file_config = read_provider_config(config)?;

    let metadata_bytes = match (&file_config.metadata, &file_config.metadata_url) {
        (Some(metadata), _) => metadata.clone(),
        (None, Some(url)) => match http.fetch(url).await {
            Ok(response) if response.status == 200 => response.body,
            Ok(_) => {
                return Err(format!(
                    "non-200 HTTP response for SAML Identity Provider metadata URL: {url}"
                ))
            }
            Err(error) => return Err(format!("fetching SAML Identity Provider metadata: {error}")),
        },
        (None, None) => unreachable!("read_provider_config enforces exactly one source"),
    };

    let metadata = unmarshal_entity_descriptor(&metadata_bytes)
        .map_err(|error| format!("parsing SAML Identity Provider metadata: {error}"))?;

    let mut service_provider = SamlServiceProvider {
        identity_provider_sso_url: metadata.sso_url.clone(),
        identity_provider_issuer: metadata.entity_id.clone(),
        name_id_format: config.name_id_format(),
        skip_signature_validation: config.insecure_skip_assertion_signature_validation,
        ..SamlServiceProvider::default()
    };

    for descriptor in &metadata.key_descriptors {
        for (index, certificate_text) in descriptor.iter().enumerate() {
            if certificate_text.is_empty() {
                return Err(format!(
                    "SAML Identity Provider metadata certificate {index} is empty"
                ));
            }
            let der = decode_base64_whitespace(certificate_text).map_err(|error| {
                format!("decoding SAML Identity Provider metadata certificate {index}: {error}")
            })?;
            let info = parse_certificate(&der).map_err(|error| {
                format!(
                    "parsing SAML Identity Provider metadata certificate {index} X.509 data: {error}"
                )
            })?;
            service_provider.idp_certificates.push(info.der);
        }
    }

    if let Some((private_key, certificate)) = file_config.key_pair {
        service_provider.sp_private_key = Some(private_key);
        service_provider.sp_certificate = Some(certificate);
        service_provider.sign_authn_requests = config.sign_requests.unwrap_or(true);
    } else if config.sign_requests == Some(true) {
        return Err(
            "signRequests is true for SAML Service Provider but no private key and cert are given"
                .to_owned(),
        );
    }

    service_provider.service_provider_issuer = config.service_provider_issuer.clone();
    if config.service_provider_issuer.is_empty() {
        return Err("invalid SAML Service Provider configuration: issuer is empty (and default issuer could not be derived from empty externalURL)".to_owned());
    }
    let parsed = go_parse_url(external_url)
        .ok_or_else(|| "parsing external URL for SAML Service Provider".to_owned())?;
    service_provider.assertion_consumer_service_url =
        GoUrlBuilder::from(&parsed).join("/.auth/saml/acs");
    service_provider.audience_uri = service_provider.service_provider_issuer.clone();
    Ok(service_provider)
}

struct GoUrlBuilder {
    scheme: String,
    host: String,
}

impl From<&crate::oauth_login::GoUrl> for GoUrlBuilder {
    fn from(url: &crate::oauth_login::GoUrl) -> Self {
        Self {
            scheme: url.scheme.clone(),
            host: url.host.clone(),
        }
    }
}

impl GoUrlBuilder {
    fn join(&self, path: &str) -> String {
        if self.scheme.is_empty() {
            path.to_owned()
        } else {
            format!("{}://{}{}", self.scheme, self.host, path)
        }
    }
}

struct EntityDescriptor {
    entity_id: String,
    sso_url: String,
    key_descriptors: Vec<Vec<String>>,
}

/// `unmarshalEntityDescriptor` (`provider.go:193-232`).
fn unmarshal_entity_descriptor(data: &[u8]) -> Result<EntityDescriptor, String> {
    let root = parse_xml(data)?;
    if root.local == "EntityDescriptor" {
        return parse_entity_descriptor(&root);
    }
    if root.local != "EntitiesDescriptor" {
        return Err(format!(
            "expected element type <EntityDescriptor> but have <{}>",
            root.local
        ));
    }
    for child in root.element_children() {
        if child.local == "EntityDescriptor" && child.element_child("IDPSSODescriptor").is_some() {
            return parse_entity_descriptor(child);
        }
    }
    Err("no entity found with IDPSSODescriptor".to_owned())
}

fn parse_entity_descriptor(element: &XmlElement) -> Result<EntityDescriptor, String> {
    let entity_id = element.attr("entityID").unwrap_or_default().to_owned();
    let descriptor = element
        .element_child("IDPSSODescriptor")
        .ok_or_else(|| "missing IDPSSODescriptor".to_owned())?;
    let sso_url = descriptor
        .element_children()
        .find(|child| child.local == "SingleSignOnService")
        .and_then(|child| child.attr("Location"))
        .ok_or_else(|| "missing SingleSignOnService".to_owned())?
        .to_owned();
    let mut key_descriptors = Vec::new();
    for key_descriptor in descriptor
        .element_children()
        .filter(|child| child.local == "KeyDescriptor")
    {
        let mut certificates = Vec::new();
        if let Some(key_info) = key_descriptor.element_child("KeyInfo") {
            if let Some(x509_data) = key_info.element_child("X509Data") {
                for certificate in x509_data
                    .element_children()
                    .filter(|child| child.local == "X509Certificate")
                {
                    certificates.push(certificate.text_content());
                }
            }
        }
        key_descriptors.push(certificates);
    }
    Ok(EntityDescriptor {
        entity_id,
        sso_url,
        key_descriptors,
    })
}

// ---------------------------------------------------------------------------
// Minimal XML tree with the c14n subset goxmldsig needs
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct XmlAttr {
    /// Namespace prefix (or `xmlns` for namespace declarations).
    pub space: String,
    pub key: String,
    pub value: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum XmlNode {
    Element(XmlElement),
    Text(String),
    Comment(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct XmlElement {
    pub prefix: String,
    pub local: String,
    pub attrs: Vec<XmlAttr>,
    pub children: Vec<XmlNode>,
}

impl XmlElement {
    fn attr(&self, key: &str) -> Option<&str> {
        self.attrs
            .iter()
            .find(|attr| attr.space.is_empty() && attr.key == key)
            .map(|attr| attr.value.as_str())
    }

    fn element_children(&self) -> impl Iterator<Item = &XmlElement> {
        self.children.iter().filter_map(|child| match child {
            XmlNode::Element(element) => Some(element),
            _ => None,
        })
    }

    fn element_child(&self, local: &str) -> Option<&XmlElement> {
        self.element_children().find(|child| child.local == local)
    }

    fn text_content(&self) -> String {
        let mut out = String::new();
        for child in &self.children {
            match child {
                XmlNode::Text(text) => out.push_str(text),
                XmlNode::Element(element) => out.push_str(&element.text_content()),
                XmlNode::Comment(_) => {}
            }
        }
        out
    }

    fn trimmed_text(&self) -> String {
        self.text_content().trim().to_owned()
    }
}

/// Parse an XML document into an [`XmlElement`] tree, preserving attribute
/// order, text (including whitespace) and comments. Namespace declarations are
/// kept as attributes exactly as written; no DTD/entities are resolved.
pub fn parse_xml(data: &[u8]) -> Result<XmlElement, String> {
    let mut reader = quick_xml::Reader::from_reader(data);
    reader.trim_text(false);
    reader.check_end_names(true);
    let mut stack: Vec<XmlElement> = Vec::new();
    let mut root: Option<XmlElement> = None;
    loop {
        match reader.read_event() {
            Ok(quick_xml::events::Event::Start(start)) => {
                stack.push(element_from_start(&start)?);
            }
            Ok(quick_xml::events::Event::Empty(start)) => {
                let element = element_from_start(&start)?;
                attach_element(element, &mut stack, &mut root)?;
            }
            Ok(quick_xml::events::Event::End(_)) => {
                let element = stack.pop().ok_or_else(|| "unexpected end tag".to_owned())?;
                attach_element(element, &mut stack, &mut root)?;
            }
            Ok(quick_xml::events::Event::Text(text)) => {
                if let Some(parent) = stack.last_mut() {
                    let value = text
                        .unescape()
                        .map_err(|error| error.to_string())?
                        .into_owned();
                    parent.children.push(XmlNode::Text(value));
                }
            }
            Ok(quick_xml::events::Event::CData(text)) => {
                if let Some(parent) = stack.last_mut() {
                    let value = String::from_utf8_lossy(text.as_ref()).into_owned();
                    parent.children.push(XmlNode::Text(value));
                }
            }
            Ok(quick_xml::events::Event::Comment(text)) => {
                if let Some(parent) = stack.last_mut() {
                    let value = String::from_utf8_lossy(text.as_ref()).into_owned();
                    parent.children.push(XmlNode::Comment(value));
                }
            }
            Ok(quick_xml::events::Event::Decl(_))
            | Ok(quick_xml::events::Event::PI(_))
            | Ok(quick_xml::events::Event::DocType(_)) => {}
            Ok(quick_xml::events::Event::Eof) => break,
            Err(error) => return Err(error.to_string()),
        }
    }
    if !stack.is_empty() {
        return Err("unclosed element".to_owned());
    }
    root.ok_or_else(|| "unable to parse response".to_owned())
}

fn element_from_start(start: &quick_xml::events::BytesStart<'_>) -> Result<XmlElement, String> {
    let (prefix, local) = split_name(start.name().as_ref());
    let mut attrs = Vec::new();
    for attribute in start.attributes() {
        let attribute = attribute.map_err(|error| error.to_string())?;
        let (space, key) = split_name(attribute.key.as_ref());
        let value = attribute
            .unescape_value()
            .map_err(|error| error.to_string())?
            .into_owned();
        attrs.push(XmlAttr { space, key, value });
    }
    Ok(XmlElement {
        prefix,
        local,
        attrs,
        children: Vec::new(),
    })
}

fn attach_element(
    element: XmlElement,
    stack: &mut [XmlElement],
    root: &mut Option<XmlElement>,
) -> Result<(), String> {
    match stack.last_mut() {
        Some(parent) => {
            parent.children.push(XmlNode::Element(element));
        }
        None => {
            if root.is_some() {
                return Err("multiple root elements".to_owned());
            }
            *root = Some(element);
        }
    }
    Ok(())
}

fn split_name(name: &[u8]) -> (String, String) {
    let name = String::from_utf8_lossy(name);
    match name.split_once(':') {
        Some((prefix, local)) => (prefix.to_owned(), local.to_owned()),
        None => (String::new(), name.into_owned()),
    }
}

fn qname(element: &XmlElement) -> String {
    if element.prefix.is_empty() {
        element.local.clone()
    } else {
        format!("{}:{}", element.prefix, element.local)
    }
}

fn attr_name(attr: &XmlAttr) -> String {
    if attr.space == "xmlns" {
        format!("xmlns:{}", attr.key)
    } else if attr.space.is_empty() && attr.key == "xmlns" {
        "xmlns".to_owned()
    } else if !attr.space.is_empty() {
        format!("{}:{}", attr.space, attr.key)
    } else {
        attr.key.clone()
    }
}

/// The `SortedAttrs.Less` ordering from `goxmldsig/etreeutils/sort.go`.
fn attr_less(a: &XmlAttr, b: &XmlAttr) -> bool {
    let a_default = a.space.is_empty() && a.key == "xmlns";
    let b_default = b.space.is_empty() && b.key == "xmlns";
    if b_default {
        return false;
    }
    if a_default {
        return true;
    }
    if a.space == "xmlns" {
        if b.space == "xmlns" {
            return a.key < b.key;
        }
        return true;
    }
    if b.space == "xmlns" {
        return false;
    }
    if a.space.is_empty() {
        if b.space.is_empty() {
            return a.key < b.key;
        }
        return true;
    }
    if b.space.is_empty() {
        return false;
    }
    if a.space == b.space {
        return a.key < b.key;
    }
    if a.key == b.key {
        return a.space < b.space;
    }
    a.key < b.key
}

fn sort_attrs(attrs: &mut [XmlAttr]) {
    attrs.sort_by(|a, b| {
        if attr_less(a, b) {
            std::cmp::Ordering::Less
        } else if attr_less(b, a) {
            std::cmp::Ordering::Greater
        } else {
            std::cmp::Ordering::Equal
        }
    });
}

fn escape_text_into(value: &str, out: &mut Vec<u8>) {
    for ch in value.chars() {
        match ch {
            '&' => out.extend_from_slice(b"&amp;"),
            '<' => out.extend_from_slice(b"&lt;"),
            '>' => out.extend_from_slice(b"&gt;"),
            '\r' => out.extend_from_slice(b"&#xD;"),
            _ => {
                let mut buffer = [0u8; 4];
                out.extend_from_slice(ch.encode_utf8(&mut buffer).as_bytes());
            }
        }
    }
}

fn escape_attr_into(value: &str, out: &mut Vec<u8>) {
    for ch in value.chars() {
        match ch {
            '&' => out.extend_from_slice(b"&amp;"),
            '<' => out.extend_from_slice(b"&lt;"),
            '"' => out.extend_from_slice(b"&quot;"),
            '\t' => out.extend_from_slice(b"&#x9;"),
            '\n' => out.extend_from_slice(b"&#xA;"),
            '\r' => out.extend_from_slice(b"&#xD;"),
            _ => {
                let mut buffer = [0u8; 4];
                out.extend_from_slice(ch.encode_utf8(&mut buffer).as_bytes());
            }
        }
    }
}

/// `canonicalSerialize` (`goxmldsig/canonicalize.go:...`).
fn serialize_canonical(element: &XmlElement) -> Vec<u8> {
    let mut out = Vec::new();
    write_canonical(element, &mut out);
    out
}

fn write_canonical(element: &XmlElement, out: &mut Vec<u8>) {
    out.push(b'<');
    out.extend_from_slice(qname(element).as_bytes());
    for attr in &element.attrs {
        out.push(b' ');
        out.extend_from_slice(attr_name(attr).as_bytes());
        out.extend_from_slice(b"=\"");
        escape_attr_into(&attr.value, out);
        out.push(b'"');
    }
    out.push(b'>');
    for child in &element.children {
        match child {
            XmlNode::Text(text) => escape_text_into(text, out),
            XmlNode::Comment(comment) => {
                out.extend_from_slice(b"<!--");
                out.extend_from_slice(comment.as_bytes());
                out.extend_from_slice(b"-->");
            }
            XmlNode::Element(child) => write_canonical(child, out),
        }
    }
    out.extend_from_slice(b"</");
    out.extend_from_slice(qname(element).as_bytes());
    out.push(b'>');
}

/// Non-canonical serialization used for the generated AuthnRequest and
/// metadata documents (self-closing empty elements, insertion attribute order).
pub fn serialize_document(element: &XmlElement, out: &mut String) {
    out.push('<');
    out.push_str(&qname(element));
    for attr in &element.attrs {
        out.push(' ');
        out.push_str(&attr_name(attr));
        out.push_str("=\"");
        let mut escaped = Vec::new();
        escape_attr_into(&attr.value, &mut escaped);
        out.push_str(&String::from_utf8_lossy(&escaped));
        out.push('"');
    }
    if element.children.is_empty() {
        out.push_str("/>");
        return;
    }
    out.push('>');
    for child in &element.children {
        match child {
            XmlNode::Text(text) => {
                let mut escaped = Vec::new();
                escape_text_into(text, &mut escaped);
                out.push_str(&String::from_utf8_lossy(&escaped));
            }
            XmlNode::Comment(comment) => {
                out.push_str("<!--");
                out.push_str(comment);
                out.push_str("-->");
            }
            XmlNode::Element(child) => serialize_document(child, out),
        }
    }
    out.push_str("</");
    out.push_str(&qname(element));
    out.push('>');
}

/// `canonicalPrep` (`goxmldsig/canonicalize.go:161-230`).
fn canonical_prep(
    element: &XmlElement,
    seen_so_far: &BTreeMap<String, String>,
    comments: bool,
) -> XmlElement {
    let mut next = element.clone();
    sort_attrs(&mut next.attrs);
    let mut seen = seen_so_far.clone();
    let mut kept = Vec::with_capacity(next.attrs.len());
    for attr in std::mem::take(&mut next.attrs) {
        if attr.space == "xmlns" {
            let key = format!("xmlns:{}", attr.key);
            if seen.get(&key) != Some(&attr.value) {
                seen.insert(key, attr.value.clone());
                kept.push(attr);
            }
        } else if attr.space.is_empty() && attr.key == "xmlns" {
            let first = !seen.contains_key("xmlns");
            let keep = if first {
                !attr.value.is_empty()
            } else {
                seen.get("xmlns") != Some(&attr.value)
            };
            if keep {
                seen.insert("xmlns".to_owned(), attr.value.clone());
                kept.push(attr);
            }
        } else {
            kept.push(attr);
        }
    }
    next.attrs = kept;
    if !comments {
        next.children
            .retain(|child| !matches!(child, XmlNode::Comment(_)));
    }
    for child in next.children.iter_mut() {
        if let XmlNode::Element(element) = child {
            *element = canonical_prep(element, &seen, comments);
        }
    }
    next
}

/// Namespace context equivalent to `goxmldsig/etreeutils.NSContext`.
#[derive(Debug, Clone)]
struct NsCtx {
    prefixes: BTreeMap<String, String>,
}

impl NsCtx {
    fn new_default() -> Self {
        let mut prefixes = BTreeMap::new();
        prefixes.insert(
            String::new(),
            "http://www.w3.org/XML/1998/namespace".to_owned(),
        );
        prefixes.insert(
            "xml".to_owned(),
            "http://www.w3.org/XML/1998/namespace".to_owned(),
        );
        prefixes.insert(
            "xmlns".to_owned(),
            "http://www.w3.org/2000/xmlns/".to_owned(),
        );
        Self { prefixes }
    }

    fn sub_context(&self, element: &XmlElement) -> Result<Self, String> {
        let mut next = self.clone();
        for attr in &element.attrs {
            if attr.space == "xmlns" {
                if attr.key == "xml" && attr.value != "http://www.w3.org/XML/1998/namespace" {
                    return Err("disallowed declaration of reserved namespace".to_owned());
                }
                if attr.key == "xmlns" {
                    return Err("disallowed declaration of reserved namespace".to_owned());
                }
                next.prefixes.insert(attr.key.clone(), attr.value.clone());
            } else if attr.space.is_empty() && attr.key == "xmlns" {
                if attr.value == "http://www.w3.org/2000/xmlns/" {
                    return Err("invalid default namespace declaration".to_owned());
                }
                next.prefixes.insert(String::new(), attr.value.clone());
            }
        }
        Ok(next)
    }

    fn lookup(&self, prefix: &str) -> Result<String, String> {
        self.prefixes
            .get(prefix)
            .cloned()
            .ok_or_else(|| format!("undeclared namespace prefix: '{prefix}'"))
    }

    fn namespace_of(&self, element: &XmlElement) -> Result<String, String> {
        self.lookup(&element.prefix)
    }
}

fn declare_attr(prefix: &str, namespace: &str) -> XmlAttr {
    if prefix.is_empty() {
        XmlAttr {
            space: String::new(),
            key: "xmlns".to_owned(),
            value: namespace.to_owned(),
        }
    } else {
        XmlAttr {
            space: "xmlns".to_owned(),
            key: prefix.to_owned(),
            value: namespace.to_owned(),
        }
    }
}

/// `etreeutils.TransformExcC14n` (`etreeutils/canonicalize.go:11-112`).
fn transform_exc_c14n(
    ctx: &NsCtx,
    declared: &NsCtx,
    element: &mut XmlElement,
    inclusive_prefixes: &BTreeSet<String>,
    comments: bool,
) -> Result<(), String> {
    let scope = ctx.sub_context(element)?;
    let mut visibly_utilized: BTreeSet<String> = BTreeSet::new();
    visibly_utilized.insert(element.prefix.clone());

    let mut filtered = Vec::with_capacity(element.attrs.len());
    for attr in std::mem::take(&mut element.attrs) {
        if attr.space == "xmlns" {
            if inclusive_prefixes.contains(&attr.key) {
                visibly_utilized.insert(attr.key.clone());
            }
        } else if attr.space.is_empty() && attr.key == "xmlns" {
            if inclusive_prefixes.contains("") {
                visibly_utilized.insert(String::new());
            }
        } else {
            if !attr.space.is_empty() {
                visibly_utilized.insert(attr.space.clone());
            }
            filtered.push(attr);
        }
    }
    element.attrs = filtered;

    let mut local_declared = declared.clone();
    for prefix in visibly_utilized {
        if let (Some(declared_value), Some(scope_value)) =
            (declared.prefixes.get(&prefix), scope.prefixes.get(&prefix))
        {
            if declared_value == scope_value {
                continue;
            }
        }
        let namespace = scope.lookup(&prefix)?;
        element.attrs.push(declare_attr(&prefix, &namespace));
        local_declared.prefixes.insert(prefix, namespace);
    }
    sort_attrs(&mut element.attrs);

    if !comments {
        element
            .children
            .retain(|child| !matches!(child, XmlNode::Comment(_)));
    }

    for child in element.children.iter_mut() {
        if let XmlNode::Element(child) = child {
            transform_exc_c14n(&scope, &local_declared, child, inclusive_prefixes, comments)?;
        }
    }
    Ok(())
}

/// `etreeutils.NSDetatch` (`etreeutils/namespace.go:185-231`).
fn ns_detach(ctx: &NsCtx, element: &XmlElement) -> Result<XmlElement, String> {
    let ctx = ctx.sub_context(element)?;
    let mut detached = element.clone();
    let mut attrs: Vec<XmlAttr> = detached
        .attrs
        .into_iter()
        .filter(|attr| !(attr.space == "xmlns" || (attr.space.is_empty() && attr.key == "xmlns")))
        .collect();
    for (prefix, namespace) in &ctx.prefixes {
        if prefix == "xmlns" || prefix == "xml" {
            continue;
        }
        if prefix.is_empty() && namespace == "http://www.w3.org/XML/1998/namespace" {
            continue;
        }
        attrs.push(declare_attr(prefix, namespace));
    }
    sort_attrs(&mut attrs);
    detached.attrs = attrs;
    Ok(detached)
}

// ---------------------------------------------------------------------------
// X.509 / RSA helpers
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
struct CertificateInfo {
    der: Vec<u8>,
    not_before: OffsetDateTime,
    not_after: OffsetDateTime,
    subject_public_key_info: Vec<u8>,
}

fn decode_base64_whitespace(value: &str) -> Result<Vec<u8>, String> {
    let compact: String = value
        .chars()
        .filter(|ch| !ch.is_ascii_whitespace())
        .collect();
    base64::engine::general_purpose::STANDARD
        .decode(compact)
        .map_err(|error| error.to_string())
}

fn pem_der(pem: &str) -> Option<Vec<u8>> {
    let body: String = pem
        .lines()
        .filter(|line| !line.trim_start().starts_with("-----"))
        .collect();
    base64::engine::general_purpose::STANDARD.decode(body).ok()
}

fn private_key_der(pem: &str) -> Option<Vec<u8>> {
    let der = pem_der(pem)?;
    if rsa::RsaPrivateKey::from_pkcs1_der(&der).is_ok() {
        return Some(der);
    }
    if rsa::RsaPrivateKey::from_pkcs8_der(&der).is_ok() {
        return Some(der);
    }
    None
}

fn rsa_private_key(der: &[u8]) -> Result<rsa::RsaPrivateKey, String> {
    if let Ok(key) = rsa::RsaPrivateKey::from_pkcs1_der(der) {
        return Ok(key);
    }
    rsa::RsaPrivateKey::from_pkcs8_der(der).map_err(|error| error.to_string())
}

fn rsa_public_key(der: &[u8]) -> Result<rsa::RsaPublicKey, String> {
    rsa::pkcs8::DecodePublicKey::from_public_key_der(der).map_err(|error| error.to_string())
}

struct DerValue<'a> {
    tag: u8,
    content: &'a [u8],
    full: &'a [u8],
}

fn der_next(input: &[u8]) -> Result<(DerValue<'_>, &[u8]), String> {
    if input.len() < 2 {
        return Err("truncated DER value".to_owned());
    }
    let tag = input[0];
    let first = input[1];
    let (length, header_len) = if first & 0x80 == 0 {
        (first as usize, 2)
    } else {
        let count = (first & 0x7f) as usize;
        if count == 0 || count > 4 || input.len() < 2 + count {
            return Err("bad DER length".to_owned());
        }
        let mut length = 0usize;
        for byte in &input[2..2 + count] {
            length = (length << 8) | *byte as usize;
        }
        (length, 2 + count)
    };
    if input.len() < header_len + length {
        return Err("truncated DER content".to_owned());
    }
    Ok((
        DerValue {
            tag,
            content: &input[header_len..header_len + length],
            full: &input[..header_len + length],
        },
        &input[header_len + length..],
    ))
}

/// Minimal X.509 parsing: DER equality, validity window and SPKI extraction.
fn parse_certificate(der: &[u8]) -> Result<CertificateInfo, String> {
    let (certificate, _) = der_next(der)?;
    if certificate.tag != 0x30 {
        return Err("certificate is not a SEQUENCE".to_owned());
    }
    let (tbs, _) = der_next(certificate.content)?;
    if tbs.tag != 0x30 {
        return Err("tbsCertificate is not a SEQUENCE".to_owned());
    }
    let mut fields = Vec::new();
    let mut rest = tbs.content;
    while !rest.is_empty() {
        let (value, next) = der_next(rest)?;
        fields.push(value);
        rest = next;
    }
    let mut index = 0;
    if fields.first().is_some_and(|field| field.tag == 0xA0) {
        index += 1;
    }
    // serialNumber, signature, issuer, validity, subject, subjectPublicKeyInfo
    let validity = fields
        .get(index + 3)
        .ok_or_else(|| "certificate validity missing".to_owned())?;
    if validity.tag != 0x30 {
        return Err("certificate validity is not a SEQUENCE".to_owned());
    }
    let (not_before, rest) = der_next(validity.content)?;
    let (not_after, _) = der_next(rest)?;
    let not_before = parse_der_time(&not_before)?;
    let not_after = parse_der_time(&not_after)?;
    let spki = fields
        .get(index + 5)
        .ok_or_else(|| "certificate SPKI missing".to_owned())?;
    if spki.tag != 0x30 {
        return Err("SPKI is not a SEQUENCE".to_owned());
    }
    Ok(CertificateInfo {
        der: der.to_vec(),
        not_before,
        not_after,
        subject_public_key_info: spki.full.to_vec(),
    })
}

fn parse_der_time(value: &DerValue<'_>) -> Result<OffsetDateTime, String> {
    let text = std::str::from_utf8(value.content).map_err(|error| error.to_string())?;
    match value.tag {
        0x17 => parse_utc_time(text),
        0x18 => parse_generalized_time(text),
        _ => Err("unsupported certificate time encoding".to_owned()),
    }
}

fn parse_utc_time(text: &str) -> Result<OffsetDateTime, String> {
    let text = text.trim_end_matches('Z');
    if text.len() != 12 {
        return Err("bad UTCTime".to_owned());
    }
    let year: i32 = text[0..2].parse().map_err(|_| "bad UTCTime".to_owned())?;
    let year = if year >= 50 { 1900 + year } else { 2000 + year };
    let month: u8 = text[2..4].parse().map_err(|_| "bad UTCTime".to_owned())?;
    let day: u8 = text[4..6].parse().map_err(|_| "bad UTCTime".to_owned())?;
    let hour: u8 = text[6..8].parse().map_err(|_| "bad UTCTime".to_owned())?;
    let minute: u8 = text[8..10].parse().map_err(|_| "bad UTCTime".to_owned())?;
    let second: u8 = text[10..12].parse().map_err(|_| "bad UTCTime".to_owned())?;
    build_datetime(year, month, day, hour, minute, second)
}

fn parse_generalized_time(text: &str) -> Result<OffsetDateTime, String> {
    let text = text.trim_end_matches('Z');
    let (whole, fraction) = text
        .split_once('.')
        .map_or((text, ""), |(whole, fraction)| (whole, fraction));
    if whole.len() != 14 {
        return Err("bad GeneralizedTime".to_owned());
    }
    let year: i32 = whole[0..4]
        .parse()
        .map_err(|_| "bad GeneralizedTime".to_owned())?;
    let month: u8 = whole[4..6]
        .parse()
        .map_err(|_| "bad GeneralizedTime".to_owned())?;
    let day: u8 = whole[6..8]
        .parse()
        .map_err(|_| "bad GeneralizedTime".to_owned())?;
    let hour: u8 = whole[8..10]
        .parse()
        .map_err(|_| "bad GeneralizedTime".to_owned())?;
    let minute: u8 = whole[10..12]
        .parse()
        .map_err(|_| "bad GeneralizedTime".to_owned())?;
    let second: u8 = whole[12..14]
        .parse()
        .map_err(|_| "bad GeneralizedTime".to_owned())?;
    let mut datetime = build_datetime(year, month, day, hour, minute, second)?;
    if !fraction.is_empty() {
        let nanos: u32 = format!("{:0<9}", fraction.chars().take(9).collect::<String>())
            .parse()
            .unwrap_or(0);
        datetime += time::Duration::nanoseconds(i64::from(nanos));
    }
    Ok(datetime)
}

fn build_datetime(
    year: i32,
    month: u8,
    day: u8,
    hour: u8,
    minute: u8,
    second: u8,
) -> Result<OffsetDateTime, String> {
    let month = Month::try_from(month).map_err(|_| "bad month".to_owned())?;
    let date = Date::from_calendar_date(year, month, day).map_err(|error| error.to_string())?;
    let time = Time::from_hms(hour, minute, second).map_err(|error| error.to_string())?;
    Ok(PrimitiveDateTime::new(date, time).assume_utc())
}

fn hash_bytes(algorithm: &str, data: &[u8]) -> Result<Vec<u8>, String> {
    use sha2::Digest as _;
    match algorithm {
        "http://www.w3.org/2000/09/xmldsig#sha1" => Ok(sha1::Sha1::digest(data).to_vec()),
        "http://www.w3.org/2001/04/xmlenc#sha256" => Ok(sha2::Sha256::digest(data).to_vec()),
        "http://www.w3.org/2001/04/xmldsig-more#sha384" => Ok(sha2::Sha384::digest(data).to_vec()),
        "http://www.w3.org/2001/04/xmlenc#sha512" => Ok(sha2::Sha512::digest(data).to_vec()),
        other => Err(format!("Unknown digest algorithm: {other}")),
    }
}

fn verify_rsa_signature(
    signature_algorithm: &str,
    public_key: &rsa::RsaPublicKey,
    signed_data: &[u8],
    signature: &[u8],
) -> Result<(), String> {
    let digest = match signature_algorithm {
        "http://www.w3.org/2000/09/xmldsig#rsa-sha1" => sha1::Sha1::digest(signed_data).to_vec(),
        "http://www.w3.org/2001/04/xmldsig-more#rsa-sha256" => {
            sha2::Sha256::digest(signed_data).to_vec()
        }
        "http://www.w3.org/2001/04/xmldsig-more#rsa-sha384" => {
            sha2::Sha384::digest(signed_data).to_vec()
        }
        "http://www.w3.org/2001/04/xmldsig-more#rsa-sha512" => {
            sha2::Sha512::digest(signed_data).to_vec()
        }
        other => return Err(format!("Unknown signature method: {other}")),
    };
    let result = match signature_algorithm {
        "http://www.w3.org/2000/09/xmldsig#rsa-sha1" => {
            public_key.verify(rsa::Pkcs1v15Sign::new::<sha1::Sha1>(), &digest, signature)
        }
        "http://www.w3.org/2001/04/xmldsig-more#rsa-sha256" => {
            public_key.verify(rsa::Pkcs1v15Sign::new::<sha2::Sha256>(), &digest, signature)
        }
        "http://www.w3.org/2001/04/xmldsig-more#rsa-sha384" => {
            public_key.verify(rsa::Pkcs1v15Sign::new::<sha2::Sha384>(), &digest, signature)
        }
        "http://www.w3.org/2001/04/xmldsig-more#rsa-sha512" => {
            public_key.verify(rsa::Pkcs1v15Sign::new::<sha2::Sha512>(), &digest, signature)
        }
        _ => unreachable!(),
    };
    result.map_err(|_| "Signature could not be verified".to_owned())
}

#[derive(Debug, Clone)]
enum Canonicalizer {
    Exclusive { prefix_list: String, comments: bool },
    Inclusive { comments: bool },
    Null,
}

impl Canonicalizer {
    fn canonicalize(&self, element: &XmlElement) -> Result<Vec<u8>, String> {
        match self {
            Canonicalizer::Exclusive {
                prefix_list,
                comments,
            } => {
                let prefixes: BTreeSet<String> =
                    prefix_list.split_whitespace().map(str::to_owned).collect();
                let mut copy = element.clone();
                transform_exc_c14n(
                    &NsCtx::new_default(),
                    &NsCtx::new_default(),
                    &mut copy,
                    &prefixes,
                    *comments,
                )?;
                Ok(serialize_canonical(&copy))
            }
            Canonicalizer::Inclusive { comments } => {
                let prepared = canonical_prep(element, &BTreeMap::new(), *comments);
                Ok(serialize_canonical(&prepared))
            }
            Canonicalizer::Null => {
                let prepared = canonical_prep(element, &BTreeMap::new(), false);
                Ok(serialize_canonical(&prepared))
            }
        }
    }
}

fn canonicalizer_from_algorithm(algorithm: &str) -> Result<Canonicalizer, String> {
    match algorithm {
        "http://www.w3.org/2001/10/xml-exc-c14n#" => Ok(Canonicalizer::Exclusive {
            prefix_list: String::new(),
            comments: false,
        }),
        "http://www.w3.org/2001/10/xml-exc-c14n#WithComments" => Ok(Canonicalizer::Exclusive {
            prefix_list: String::new(),
            comments: true,
        }),
        "http://www.w3.org/2006/12/xml-c14n11" => Ok(Canonicalizer::Inclusive { comments: false }),
        "http://www.w3.org/2006/12/xml-c14n11#WithComments" => {
            Ok(Canonicalizer::Inclusive { comments: true })
        }
        "http://www.w3.org/TR/2001/REC-xml-c14n-20010315" => {
            Ok(Canonicalizer::Inclusive { comments: false })
        }
        "http://www.w3.org/TR/2001/REC-xml-c14n-20010315#WithComments" => {
            Ok(Canonicalizer::Inclusive { comments: true })
        }
        "http://www.w3.org/2000/09/xmldsig#enveloped-signature" => Ok(Canonicalizer::Null),
        other => Err(format!("Unknown Transform Algorithm: {other}")),
    }
}

struct SignatureReference {
    digest_algorithm: String,
    digest_value: String,
    canonicalizer: Canonicalizer,
}

/// Verify the enveloped signature on `root` (or report it missing).
fn verify_enveloped_signature(
    root: &XmlElement,
    trusted: &[Vec<u8>],
    now: OffsetDateTime,
) -> Result<bool, String> {
    let root_id = root.attr("ID").unwrap_or_default().to_owned();
    let mut path = Vec::new();
    let mut found = None;
    find_signature(root, &NsCtx::new_default(), &mut path, &root_id, &mut found)?;
    let Some((signature_path, signature_context)) = found else {
        return Ok(false);
    };

    // Re-locate the signature element from its path.
    let signature = element_at_path(root, &signature_path)
        .ok_or_else(|| "signature element disappeared".to_owned())?
        .clone();

    if !signature_shape_valid(&signature) {
        return Err("Invalid Signature".to_owned());
    }
    let signed_info = signature
        .element_child("SignedInfo")
        .ok_or_else(|| "Missing SignedInfo".to_owned())?;
    let signed_info_context = signature_context.sub_context(signed_info)?;
    let detached_signed_info = ns_detach(&signed_info_context, signed_info)?;

    let canonicalization_method = signed_info
        .element_child("CanonicalizationMethod")
        .ok_or_else(|| "missing CanonicalizationMethod on Signature".to_owned())?;
    let canonicalization_algorithm = canonicalization_method
        .attr("Algorithm")
        .unwrap_or_default();
    let inclusive_prefix_list = canonicalization_method
        .element_child("InclusiveNamespaces")
        .and_then(|element| element.attr("PrefixList"))
        .unwrap_or_default()
        .to_owned();

    let signed_info_bytes = match canonicalization_algorithm {
        "http://www.w3.org/2001/10/xml-exc-c14n#" => {
            let mut copy = detached_signed_info.clone();
            let prefixes: BTreeSet<String> = inclusive_prefix_list
                .split_whitespace()
                .map(str::to_owned)
                .collect();
            transform_exc_c14n(
                &NsCtx::new_default(),
                &NsCtx::new_default(),
                &mut copy,
                &prefixes,
                false,
            )?;
            serialize_canonical(&copy)
        }
        "http://www.w3.org/2001/10/xml-exc-c14n#WithComments" => {
            let mut copy = detached_signed_info.clone();
            let prefixes: BTreeSet<String> = inclusive_prefix_list
                .split_whitespace()
                .map(str::to_owned)
                .collect();
            transform_exc_c14n(
                &NsCtx::new_default(),
                &NsCtx::new_default(),
                &mut copy,
                &prefixes,
                true,
            )?;
            serialize_canonical(&copy)
        }
        "http://www.w3.org/2006/12/xml-c14n11"
        | "http://www.w3.org/TR/2001/REC-xml-c14n-20010315" => serialize_canonical(
            &canonical_prep(&detached_signed_info, &BTreeMap::new(), false),
        ),
        "http://www.w3.org/2006/12/xml-c14n11#WithComments"
        | "http://www.w3.org/TR/2001/REC-xml-c14n-20010315#WithComments" => serialize_canonical(
            &canonical_prep(&detached_signed_info, &BTreeMap::new(), true),
        ),
        other => {
            return Err(format!(
                "invalid CanonicalizationMethod on Signature: {other}"
            ))
        }
    };

    let reference = find_reference(signed_info, &root_id)?;
    let mut transformed = root.clone();
    let _ = remove_element_at_path(&mut transformed, &signature_path);
    let canonicalized = reference.canonicalizer.canonicalize(&transformed)?;
    let digest = hash_bytes(&reference.digest_algorithm, &canonicalized)?;
    let expected = decode_base64_whitespace(&reference.digest_value)?;
    if digest != expected {
        return Err("Signature could not be verified".to_owned());
    }

    let signature_value = signature
        .element_child("SignatureValue")
        .ok_or_else(|| "Signature could not be verified".to_owned())?;
    let decoded_signature = decode_base64_whitespace(&signature_value.trimmed_text())?;

    let certificate = verify_certificate(&signature, trusted, now)?;
    let public_key = rsa_public_key(&certificate.subject_public_key_info)?;
    let signature_algorithm = signed_info
        .element_child("SignatureMethod")
        .and_then(|element| element.attr("Algorithm"))
        .ok_or_else(|| "Missing SignatureMethod".to_owned())?;
    verify_rsa_signature(
        signature_algorithm,
        &public_key,
        &signed_info_bytes,
        &decoded_signature,
    )?;
    Ok(true)
}

fn signature_shape_valid(signature: &XmlElement) -> bool {
    let mut signed_info = 0usize;
    let mut key_info = 0usize;
    let mut signature_value = 0usize;
    for child in signature.element_children() {
        match child.local.as_str() {
            "SignedInfo" => signed_info += 1,
            "KeyInfo" => key_info += 1,
            "SignatureValue" => signature_value += 1,
            _ => {}
        }
    }
    signed_info == 1 && key_info <= 1 && signature_value == 1
}

fn find_signature(
    element: &XmlElement,
    ctx: &NsCtx,
    path: &mut Vec<usize>,
    root_id: &str,
    found: &mut Option<(Vec<usize>, NsCtx)>,
) -> Result<(), String> {
    let ctx = ctx.sub_context(element)?;
    if element.local == "Signature" && ctx.namespace_of(element)? == DSIG_NS {
        if let Ok(reference) = find_reference_optional(element, root_id) {
            if reference {
                *found = Some((path.clone(), ctx.clone()));
                return Ok(());
            }
        }
    }
    for (index, child) in element.children.iter().enumerate() {
        if let XmlNode::Element(child) = child {
            path.push(index);
            find_signature(child, &ctx, path, root_id, found)?;
            path.pop();
            if found.is_some() {
                return Ok(());
            }
        }
    }
    Ok(())
}

fn find_reference_optional(signature: &XmlElement, root_id: &str) -> Result<bool, String> {
    if !signature_shape_valid(signature) {
        return Ok(false);
    }
    let Some(signed_info) = signature.element_child("SignedInfo") else {
        return Ok(false);
    };
    for reference in signed_info
        .element_children()
        .filter(|child| child.local == "Reference")
    {
        let uri = reference.attr("URI").unwrap_or_default();
        if uri.is_empty() || uri.strip_prefix('#') == Some(root_id) {
            return Ok(true);
        }
    }
    Ok(false)
}

fn find_reference(signed_info: &XmlElement, root_id: &str) -> Result<SignatureReference, String> {
    let mut selected = None;
    for reference in signed_info
        .element_children()
        .filter(|child| child.local == "Reference")
    {
        let uri = reference.attr("URI").unwrap_or_default();
        if uri.is_empty() || uri.strip_prefix('#') == Some(root_id) {
            selected = Some(reference);
        }
    }
    let reference = selected.ok_or_else(|| "Missing Reference".to_owned())?;
    let digest_algorithm = reference
        .element_child("DigestMethod")
        .and_then(|element| element.attr("Algorithm"))
        .ok_or_else(|| "Missing DigestMethod".to_owned())?
        .to_owned();
    let digest_value = reference
        .element_child("DigestValue")
        .map(XmlElement::trimmed_text)
        .ok_or_else(|| "Missing DigestValue".to_owned())?;

    let mut canonicalizer = Canonicalizer::Null;
    if let Some(transforms) = reference.element_child("Transforms") {
        for transform in transforms
            .element_children()
            .filter(|child| child.local == "Transform")
        {
            let algorithm = transform.attr("Algorithm").unwrap_or_default();
            canonicalizer = canonicalizer_from_algorithm(algorithm)?;
            if let Canonicalizer::Exclusive {
                prefix_list,
                comments,
            } = &mut canonicalizer
            {
                *prefix_list = transform
                    .element_child("InclusiveNamespaces")
                    .and_then(|element| element.attr("PrefixList"))
                    .unwrap_or_default()
                    .to_owned();
                let _ = comments;
            }
        }
    }

    Ok(SignatureReference {
        digest_algorithm,
        digest_value,
        canonicalizer,
    })
}

fn verify_certificate(
    signature: &XmlElement,
    trusted: &[Vec<u8>],
    now: OffsetDateTime,
) -> Result<CertificateInfo, String> {
    let certificate = if let Some(key_info) = signature.element_child("KeyInfo") {
        let certificates = key_info
            .element_child("X509Data")
            .map(|element| {
                element
                    .element_children()
                    .filter(|child| child.local == "X509Certificate")
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        let Some(first) = certificates.first() else {
            return Err("missing X509Certificate within KeyInfo".to_owned());
        };
        let text = first.trimmed_text();
        if text.is_empty() {
            return Err("missing X509Certificate within KeyInfo".to_owned());
        }
        let der = decode_base64_whitespace(&text)
            .map_err(|_| "Failed to parse certificate".to_owned())?;
        parse_certificate(&der)?
    } else if trusted.len() == 1 {
        parse_certificate(&trusted[0])?
    } else {
        return Err("Missing x509 Element".to_owned());
    };

    if !trusted.iter().any(|root| root == &certificate.der) {
        return Err("Could not verify certificate against trusted certs".to_owned());
    }
    if now < certificate.not_before || now > certificate.not_after {
        return Err("Cert is not valid at this time".to_owned());
    }
    Ok(certificate)
}

fn element_at_path<'a>(root: &'a XmlElement, path: &[usize]) -> Option<&'a XmlElement> {
    let mut current = root;
    for index in path {
        match current.children.get(*index) {
            Some(XmlNode::Element(child)) => current = child,
            _ => return None,
        }
    }
    Some(current)
}

fn remove_element_at_path(element: &mut XmlElement, path: &[usize]) -> bool {
    let Some((first, rest)) = path.split_first() else {
        return false;
    };
    if element.children.len() <= *first {
        return false;
    }
    if rest.is_empty() {
        if matches!(element.children.get(*first), Some(XmlNode::Element(_))) {
            element.children.remove(*first);
            return true;
        }
        return false;
    }
    match element.children.get_mut(*first) {
        Some(XmlNode::Element(child)) => remove_element_at_path(child, rest),
        _ => false,
    }
}

// ---------------------------------------------------------------------------
// user.go — response validation and attribute extraction
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
struct AssertionAttribute {
    name: String,
    friendly_name: String,
    values: Vec<String>,
}

#[derive(Debug, Clone, Default)]
struct AssertionData {
    name_id: String,
    attributes: Vec<AssertionAttribute>,
}

#[derive(Debug, Clone)]
struct ResponseData {
    destination: String,
    version: String,
    issuer: Option<String>,
    status_code: Option<String>,
    assertions: Vec<AssertionElement>,
}

#[derive(Debug, Clone, Default)]
struct AssertionElement {
    issuer: Option<String>,
    name_id: Option<String>,
    subject_confirmation: Option<SubjectConfirmation>,
    conditions: Option<Conditions>,
    attributes: Vec<AssertionAttribute>,
}

#[derive(Debug, Clone, Default)]
struct SubjectConfirmation {
    method: String,
    recipient: Option<String>,
    not_on_or_after: Option<String>,
}

#[derive(Debug, Clone, Default)]
struct Conditions {
    not_before: Option<String>,
    not_on_or_after: Option<String>,
    audience_restrictions: Vec<Vec<String>>,
}

#[derive(Debug, Clone, Default)]
pub struct WarningInfo {
    pub invalid_time: bool,
    pub not_in_audience: bool,
}

/// `RetrieveAssertionInfo` (`gosaml2/retrieve_assertion.go:49-111`).
fn retrieve_assertion_info(
    encoded_response: &str,
    service_provider: &SamlServiceProvider,
    now: OffsetDateTime,
) -> Result<(AssertionData, WarningInfo), String> {
    let response = validate_encoded_response(encoded_response, service_provider, now)?;
    let first = response
        .assertions
        .first()
        .ok_or_else(|| "missing Assertion element".to_owned())?;
    let warning = verify_assertion_conditions(first, service_provider, now)?;
    let name_id = first
        .name_id
        .clone()
        .ok_or_else(|| "missing NameID element".to_owned())?;
    let mut data = AssertionData {
        name_id,
        attributes: first.attributes.clone(),
    };
    data.attributes.sort_by(|a, b| {
        a.name
            .cmp(&b.name)
            .then_with(|| a.friendly_name.cmp(&b.friendly_name))
    });
    Ok((data, warning))
}

fn validate_encoded_response(
    encoded_response: &str,
    service_provider: &SamlServiceProvider,
    now: OffsetDateTime,
) -> Result<ResponseData, String> {
    let raw = base64::engine::general_purpose::STANDARD
        .decode(encoded_response)
        .map_err(|error| error.to_string())?;
    let root = parse_response_bytes(&raw)?;

    let mut response_signed = false;
    if !service_provider.skip_signature_validation {
        response_signed =
            verify_enveloped_signature(&root, &service_provider.idp_certificates, now)?;
    }

    if root.element_child("EncryptedAssertion").is_some() {
        return Err("encrypted assertions are not supported".to_owned());
    }

    if !service_provider.skip_signature_validation {
        let mut signed = 0usize;
        let mut unsigned = 0usize;
        for assertion in root
            .element_children()
            .filter(|child| child.local == "Assertion")
        {
            match verify_enveloped_signature(assertion, &service_provider.idp_certificates, now) {
                Ok(true) => signed += 1,
                Ok(false) => unsigned += 1,
                Err(error) => return Err(error),
            }
        }
        if signed > 0 && unsigned > 0 {
            return Err("invalid to have both signed and unsigned assertions".to_owned());
        }
        if signed < 1 && !response_signed {
            return Err("response and/or assertions must be signed".to_owned());
        }
    }

    let response = parse_response_data(&root);
    validate_response(&response, service_provider, now)?;
    Ok(response)
}

fn parse_response_bytes(raw: &[u8]) -> Result<XmlElement, String> {
    match parse_xml(raw) {
        Ok(root) => Ok(root),
        Err(first_error) => {
            let mut decompressed = Vec::new();
            let mut decoder = flate2::read::DeflateDecoder::new(raw);
            use std::io::Read as _;
            match decoder.read_to_end(&mut decompressed) {
                Ok(_) => parse_xml(&decompressed),
                Err(_) => Err(first_error),
            }
        }
    }
}

fn parse_response_data(root: &XmlElement) -> ResponseData {
    let mut response = ResponseData {
        destination: root.attr("Destination").unwrap_or_default().to_owned(),
        version: root.attr("Version").unwrap_or_default().to_owned(),
        issuer: root.element_child("Issuer").map(XmlElement::trimmed_text),
        status_code: root
            .element_child("Status")
            .and_then(|status| status.element_child("StatusCode"))
            .and_then(|status_code| status_code.attr("Value"))
            .map(str::to_owned),
        assertions: Vec::new(),
    };
    for assertion in root
        .element_children()
        .filter(|child| child.local == "Assertion")
    {
        response.assertions.push(parse_assertion(assertion));
    }
    response
}

fn parse_assertion(assertion: &XmlElement) -> AssertionElement {
    let subject = assertion.element_child("Subject");
    let name_id = subject
        .and_then(|subject| subject.element_child("NameID"))
        .map(XmlElement::trimmed_text);
    let subject_confirmation = subject
        .and_then(|subject| subject.element_child("SubjectConfirmation"))
        .map(|confirmation| SubjectConfirmation {
            method: confirmation.attr("Method").unwrap_or_default().to_owned(),
            recipient: confirmation
                .element_child("SubjectConfirmationData")
                .and_then(|data| data.attr("Recipient"))
                .map(str::to_owned),
            not_on_or_after: confirmation
                .element_child("SubjectConfirmationData")
                .and_then(|data| data.attr("NotOnOrAfter"))
                .map(str::to_owned),
        });
    let conditions = assertion.element_child("Conditions").map(|conditions| {
        let mut audience_restrictions = Vec::new();
        for restriction in conditions
            .element_children()
            .filter(|child| child.local == "AudienceRestriction")
        {
            audience_restrictions.push(
                restriction
                    .element_children()
                    .filter(|child| child.local == "Audience")
                    .map(XmlElement::trimmed_text)
                    .collect(),
            );
        }
        Conditions {
            not_before: conditions.attr("NotBefore").map(str::to_owned),
            not_on_or_after: conditions.attr("NotOnOrAfter").map(str::to_owned),
            audience_restrictions,
        }
    });
    let mut attributes = Vec::new();
    if let Some(statement) = assertion.element_child("AttributeStatement") {
        for attribute in statement
            .element_children()
            .filter(|child| child.local == "Attribute")
        {
            attributes.push(AssertionAttribute {
                name: attribute.attr("Name").unwrap_or_default().to_owned(),
                friendly_name: attribute
                    .attr("FriendlyName")
                    .unwrap_or_default()
                    .to_owned(),
                values: attribute
                    .element_children()
                    .filter(|child| child.local == "AttributeValue")
                    .map(XmlElement::trimmed_text)
                    .collect(),
            });
        }
    }
    AssertionElement {
        issuer: assertion
            .element_child("Issuer")
            .map(XmlElement::trimmed_text),
        name_id,
        subject_confirmation,
        conditions,
        attributes,
    }
}

fn validate_response(
    response: &ResponseData,
    service_provider: &SamlServiceProvider,
    now: OffsetDateTime,
) -> Result<(), String> {
    if !response.destination.is_empty()
        && response.destination != service_provider.assertion_consumer_service_url
    {
        return Err("Destination value mismatch".to_owned());
    }
    if response.version != "2.0" {
        return Err("Unsupported SAML version".to_owned());
    }
    if response.assertions.is_empty() {
        return Err("missing Assertion element".to_owned());
    }
    let issuer = response
        .issuer
        .as_ref()
        .ok_or_else(|| "missing Issuer element".to_owned())?;
    if !service_provider.identity_provider_issuer.is_empty()
        && issuer != &service_provider.identity_provider_issuer
    {
        return Err("Issuer value mismatch".to_owned());
    }
    let status = response
        .status_code
        .as_ref()
        .ok_or_else(|| "missing StatusCode element".to_owned())?;
    if status != SUCCESS_STATUS {
        return Err("StatusCode value mismatch".to_owned());
    }
    for assertion in &response.assertions {
        let issuer = assertion
            .issuer
            .as_ref()
            .ok_or_else(|| "missing Issuer element".to_owned())?;
        if !service_provider.identity_provider_issuer.is_empty()
            && issuer != &service_provider.identity_provider_issuer
        {
            return Err("Issuer value mismatch".to_owned());
        }
        let confirmation = assertion
            .subject_confirmation
            .as_ref()
            .ok_or_else(|| "missing SubjectConfirmation element".to_owned())?;
        if confirmation.method != BEARER_METHOD {
            return Err("Unsupported SubjectConfirmation value".to_owned());
        }
        let recipient = confirmation
            .recipient
            .as_ref()
            .ok_or_else(|| "missing SubjectConfirmationData element".to_owned())?;
        if recipient != &service_provider.assertion_consumer_service_url {
            return Err("Recipient value mismatch".to_owned());
        }
        let not_on_or_after = confirmation.not_on_or_after.as_ref().ok_or_else(|| {
            "missing NotOnOrAfter attribute on SubjectConfirmationData element".to_owned()
        })?;
        let not_on_or_after =
            OffsetDateTime::parse(not_on_or_after, &Rfc3339).map_err(|error| error.to_string())?;
        if now > not_on_or_after {
            return Err("Expired NotOnOrAfter value".to_owned());
        }
    }
    Ok(())
}

fn verify_assertion_conditions(
    assertion: &AssertionElement,
    service_provider: &SamlServiceProvider,
    now: OffsetDateTime,
) -> Result<WarningInfo, String> {
    let mut warning = WarningInfo::default();
    let conditions = assertion
        .conditions
        .as_ref()
        .ok_or_else(|| "missing Conditions element".to_owned())?;
    let not_before = conditions
        .not_before
        .as_ref()
        .ok_or_else(|| "missing NotBefore attribute on Conditions element".to_owned())?;
    let not_before =
        OffsetDateTime::parse(not_before, &Rfc3339).map_err(|error| error.to_string())?;
    if now < not_before {
        warning.invalid_time = true;
    }
    let not_on_or_after = conditions
        .not_on_or_after
        .as_ref()
        .ok_or_else(|| "missing NotOnOrAfter attribute on Conditions element".to_owned())?;
    let not_on_or_after =
        OffsetDateTime::parse(not_on_or_after, &Rfc3339).map_err(|error| error.to_string())?;
    if now > not_on_or_after {
        warning.invalid_time = true;
    }
    for restriction in &conditions.audience_restrictions {
        if !restriction
            .iter()
            .any(|audience| audience == &service_provider.audience_uri)
        {
            warning.not_in_audience = true;
            break;
        }
    }
    Ok(warning)
}

fn first_non_empty(values: &[String]) -> String {
    for value in values {
        let trimmed = value.trim();
        if !trimmed.is_empty() {
            return trimmed.to_owned();
        }
    }
    String::new()
}

fn attribute_get(attributes: &[AssertionAttribute], key: &str) -> String {
    for attribute in attributes {
        if attribute.name == key || attribute.friendly_name == key {
            return attribute.values.first().cloned().unwrap_or_default();
        }
    }
    String::new()
}

fn attribute_first_non_empty(attributes: &[AssertionAttribute], keys: &[&str]) -> String {
    for key in keys {
        let value = attribute_get(attributes, key);
        if !value.is_empty() {
            return value;
        }
    }
    String::new()
}

fn attribute_map(attributes: &[AssertionAttribute], key: &str) -> BTreeMap<String, bool> {
    for attribute in attributes {
        if attribute.name == key || attribute.friendly_name == key {
            let mut output = BTreeMap::new();
            for value in &attribute.values {
                output.insert(value.clone(), true);
            }
            return output;
        }
    }
    BTreeMap::new()
}

fn might_be_email(value: &str) -> bool {
    value.matches('@').count() == 1
}

fn get_email(attributes: &[AssertionAttribute], name_id: &str) -> String {
    let email = first_non_empty(&[
        attribute_get(attributes, "email"),
        attribute_get(attributes, "emailaddress"),
        attribute_get(
            attributes,
            "http://schemas.xmlsoap.org/ws/2005/05/identity/claims/emailaddress",
        ),
        attribute_get(attributes, "http://schemas.xmlsoap.org/claims/EmailAddress"),
    ]);
    if !email.is_empty() {
        return email;
    }
    let principal_name = attribute_get(attributes, "eduPersonPrincipalName");
    if might_be_email(&principal_name) {
        return principal_name;
    }
    if might_be_email(name_id) {
        return name_id.to_owned();
    }
    email
}

fn get_unnormalized_username(
    attributes: &[AssertionAttribute],
    email: &str,
    username_keys: Option<&[String]>,
) -> String {
    if let Some(keys) = username_keys {
        if !keys.is_empty() {
            let refs: Vec<&str> = keys.iter().map(String::as_str).collect();
            return attribute_first_non_empty(attributes, &refs);
        }
    }
    let login = attribute_get(attributes, "login");
    let uid = attribute_get(attributes, "uid");
    let username = attribute_get(attributes, "username");
    let claims_name = attribute_get(
        attributes,
        "http://schemas.xmlsoap.org/ws/2005/05/identity/claims/name",
    );
    first_non_empty(&[login, uid, username, claims_name, email.to_owned()])
}

fn get_display_name(attributes: &[AssertionAttribute]) -> String {
    let given = attribute_get(attributes, "givenName");
    let surname = attribute_get(attributes, "surname");
    let combined = format!("{given} {surname}");
    first_non_empty(&[
        attribute_get(attributes, "displayName"),
        combined,
        attribute_get(attributes, "http://schemas.xmlsoap.org/claims/CommonName"),
        attribute_get(
            attributes,
            "http://schemas.xmlsoap.org/ws/2005/05/identity/claims/givenname",
        ),
    ])
}

/// The values [`crate::user_provisioning`] needs after validation
/// (`user.go:31-81`).
#[derive(Debug, Clone)]
pub struct AuthnResponseInfo {
    pub service_id: String,
    pub client_id: String,
    pub account_id: String,
    pub email: String,
    pub unnormalized_username: String,
    pub display_name: String,
    pub groups: BTreeMap<String, bool>,
}

pub async fn read_authn_response(
    provider: &SamlProvider,
    encoded_response: &str,
    now: OffsetDateTime,
) -> Result<AuthnResponseInfo, String> {
    let service_provider = provider.service_provider().await?;
    let (assertion, warning) = retrieve_assertion_info(encoded_response, &service_provider, now)
        .map_err(|error| format!("reading AuthnResponse assertions: {error}"))?;
    if warning.invalid_time || warning.not_in_audience {
        // Go prints `WarningInfo` with `%+v`; only these fields are ever set.
        return Err(format!(
            "invalid SAML AuthnResponse: &{{OneTimeUse:false ProxyRestriction:<nil> NotInAudience:{} InvalidTime:{}}}",
            warning.not_in_audience, warning.invalid_time
        ));
    }
    if assertion.name_id.is_empty() {
        return Err("the SAML response did not contain a valid NameID".to_owned());
    }
    let email = get_email(&assertion.attributes, &assertion.name_id);
    if email.is_empty() {
        return Err("the SAML response did not contain an email attribute".to_owned());
    }
    let unnormalized_username = get_unnormalized_username(
        &assertion.attributes,
        &email,
        provider.config.username_attribute_names.as_deref(),
    );
    if unnormalized_username.is_empty() {
        return Err("the SAML response did not contain a username attribute".to_owned());
    }
    let (info, error) = provider.cached_info().await;
    if let Some(error) = error {
        return Err(error);
    }
    let groups_attribute = first_non_empty(&[
        provider.config.groups_attribute_name.clone(),
        "groups".to_owned(),
    ]);
    Ok(AuthnResponseInfo {
        service_id: info.service_id,
        client_id: info.client_id,
        account_id: assertion.name_id,
        email,
        unnormalized_username,
        display_name: get_display_name(&assertion.attributes),
        groups: attribute_map(&assertion.attributes, &groups_attribute),
    })
}

// ---------------------------------------------------------------------------
// Session / provisioning seams
// ---------------------------------------------------------------------------

/// The normalized user properties handed to the provisioner.
#[derive(Debug, Clone)]
pub struct SamlNewUser<'a> {
    pub username: &'a str,
    pub email: &'a str,
    pub display_name: &'a str,
    pub service_id: &'a str,
    pub client_id: &'a str,
    pub account_id: &'a str,
    pub create_if_not_exist: bool,
}

#[async_trait]
pub trait SamlProvisioner: Send + Sync {
    async fn get_or_create_user(
        &self,
        user: &SamlNewUser<'_>,
    ) -> Result<ProvisionOutcome, ProvisionError>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SamlUser {
    pub id: i32,
    pub created_at: OffsetDateTime,
    pub site_admin: bool,
}

#[async_trait]
pub trait SamlUserStore: Send + Sync {
    async fn get_by_id(&self, id: i32) -> Result<Option<SamlUser>, String>;
}

/// Production provisioner over [`PgProvisionStore`].
pub struct PgSamlProvisioner {
    store: PgProvisionStore,
    suffix: OsUsernameSuffix,
    sourcegraph_dotcom: bool,
}

impl PgSamlProvisioner {
    pub fn new(pool: sqlx::PgPool, sourcegraph_dotcom: bool) -> Self {
        Self {
            store: PgProvisionStore::new(pool),
            suffix: OsUsernameSuffix,
            sourcegraph_dotcom,
        }
    }
}

#[async_trait]
impl SamlProvisioner for PgSamlProvisioner {
    async fn get_or_create_user(
        &self,
        user: &SamlNewUser<'_>,
    ) -> Result<ProvisionOutcome, ProvisionError> {
        let deps = ProvisionDeps {
            store: &self.store,
            suffix: &self.suffix,
            sourcegraph_dotcom: self.sourcegraph_dotcom,
        };
        user_provisioning::get_or_create_user(
            &deps,
            &ExternalIdentity {
                service_type: PROVIDER_TYPE,
                service_id: user.service_id,
                client_id: user.client_id,
                account_id: user.account_id,
                login: user.username,
                email: user.email,
                email_verified: !user.email.is_empty(),
                display_name: user.display_name,
                avatar_url: "",
                create_if_not_exist: user.create_if_not_exist,
            },
        )
        .await
    }
}

/// Production `Users().GetByID` for the post-login session.
pub struct PgSamlUsers {
    pool: sqlx::PgPool,
}

impl PgSamlUsers {
    pub fn new(pool: sqlx::PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl SamlUserStore for PgSamlUsers {
    async fn get_by_id(&self, id: i32) -> Result<Option<SamlUser>, String> {
        sg_db::users::get_session_user(&self.pool, id)
            .await
            .map_err(|error| error.to_string())
            .map(|record| {
                record.map(|record| SamlUser {
                    id: record.id,
                    created_at: record.created_at,
                    site_admin: record.site_admin,
                })
            })
    }
}

// ---------------------------------------------------------------------------
// middleware.go — HTTP endpoints
// ---------------------------------------------------------------------------

/// One sign-in provider as seen by the middleware's auto-redirect condition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SignInProviderInfo {
    pub provider_type: String,
    pub config_id: String,
    pub hidden: bool,
    pub no_sign_in: bool,
}

/// `providers.SignInProviders` for the providers configured in site config.
///
/// Only the identity of the single SAML provider is needed; OAuth/OIDC
/// entries are counted with their own config IDs (or an empty ID when there is
/// only one entry of that type).
pub fn sign_in_providers_from_site(
    site: &SiteConfiguration,
    skip_sourcegraph_operator: bool,
) -> Vec<SignInProviderInfo> {
    let raw = site.raw();
    let entries = raw
        .get("auth.providers")
        .and_then(Value::as_array)
        .map(Vec::as_slice)
        .unwrap_or_default();
    if entries.is_empty() {
        return vec![SignInProviderInfo {
            provider_type: "builtin".to_owned(),
            config_id: String::new(),
            hidden: false,
            no_sign_in: false,
        }];
    }
    let saml_count = entries
        .iter()
        .filter(|entry| entry.get("type").and_then(Value::as_str) == Some(PROVIDER_TYPE))
        .count();
    let mut providers = Vec::new();
    for entry in entries {
        let Some(obj) = entry.as_object() else {
            continue;
        };
        let provider_type = obj
            .get("type")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_owned();
        if skip_sourcegraph_operator && provider_type == "sourcegraphOperator" {
            continue;
        }
        let config_id = if provider_type == PROVIDER_TYPE {
            let external_url = raw
                .get("externalURL")
                .and_then(Value::as_str)
                .unwrap_or_default();
            with_config_defaults(obj, external_url).provider_config_id(saml_count >= 2)
        } else {
            String::new()
        };
        providers.push(SignInProviderInfo {
            provider_type,
            config_id,
            hidden: bool_field(obj, "hidden"),
            no_sign_in: bool_field(obj, "noSignIn"),
        });
    }
    providers
}

/// One HTTP request as seen by the SAML middleware.
#[derive(Debug, Clone, Default)]
pub struct SamlHttpRequest {
    pub method: String,
    pub path: String,
    /// `r.URL.String()` (used for post-login safe redirects).
    pub url_string: String,
    pub query: Vec<(String, String)>,
    /// Parsed `application/x-www-form-urlencoded` POST body.
    pub form: Vec<(String, String)>,
    pub referer: String,
    pub session_cookie: Option<String>,
    pub has_sign_out_cookie: bool,
    pub authenticated: bool,
    pub is_api_request: bool,
}

#[derive(Debug, Clone)]
pub struct SamlHttpResponse {
    pub status: u16,
    pub body: Vec<u8>,
    pub content_type: String,
    pub location: String,
    pub set_cookies: Vec<SetCookie>,
}

impl SamlHttpResponse {
    fn error(status: u16, message: &str) -> Self {
        Self {
            status,
            body: format!("{message}\n").into_bytes(),
            content_type: "text/plain; charset=utf-8".to_owned(),
            location: String::new(),
            set_cookies: Vec::new(),
        }
    }

    fn redirect(location: String) -> Self {
        Self {
            status: 302,
            body: Vec::new(),
            content_type: String::new(),
            location,
            set_cookies: Vec::new(),
        }
    }

    /// `http.Redirect` for a GET request writes the familiar HTML body.
    fn redirect_get(location: String) -> Self {
        let body = format!("<a href=\"{}\">Found</a>.\n\n", go_html_escape(&location));
        Self {
            status: 302,
            body: body.into_bytes(),
            content_type: "text/html; charset=utf-8".to_owned(),
            location,
            set_cookies: Vec::new(),
        }
    }
}

/// `html.EscapeString`.
fn go_html_escape(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for ch in value.chars() {
        match ch {
            '&' => out.push_str("&amp;"),
            '\'' => out.push_str("&#39;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&#34;"),
            _ => out.push(ch),
        }
    }
    out
}

/// All dependencies of the SAML middleware.
pub struct SamlDeps<'a> {
    pub providers: &'a SamlProviderSet,
    pub sign_in_providers: &'a [SignInProviderInfo],
    pub auth_public: bool,
    pub external_url: &'a str,
    pub store: &'a dyn SessionStore,
    pub ids: &'a dyn SessionIdSource,
    pub cookie_key: &'a [u8],
    pub secure: bool,
    pub configured_expiry: Option<&'a str>,
    pub now: OffsetDateTime,
    pub provisioner: &'a dyn SamlProvisioner,
    pub users: &'a dyn SamlUserStore,
    pub suffix: &'a dyn UsernameSuffixSource,
    pub sourcegraph_dotcom: bool,
    pub license_expired: bool,
}

/// `authHandler` (`middleware.go:48-78`).
pub async fn saml_middleware(
    deps: &SamlDeps<'_>,
    request: &SamlHttpRequest,
) -> Option<SamlHttpResponse> {
    if !request.is_api_request && request.path.starts_with("/.auth/saml/") {
        return Some(saml_endpoint(deps, request).await);
    }
    if request.authenticated {
        return None;
    }
    let sign_in: Vec<&SignInProviderInfo> = deps
        .sign_in_providers
        .iter()
        .filter(|provider| !provider.hidden && !provider.no_sign_in)
        .collect();
    if !deps.auth_public
        && sign_in.len() == 1
        && sign_in[0].provider_type == PROVIDER_TYPE
        && !request.has_sign_out_cookie
        && !request.is_api_request
    {
        let provider = match handle_get_provider(deps, &sign_in[0].config_id).await {
            Ok(provider) => provider,
            Err(response) => return Some(response),
        };
        let return_to = safe_redirect_url(&request.url_string);
        return Some(
            match redirect_to_auth_url(&provider, &return_to, deps.external_url, deps.now).await {
                Ok(response) => response,
                Err(response) => response,
            },
        );
    }
    None
}

async fn saml_endpoint(deps: &SamlDeps<'_>, request: &SamlHttpRequest) -> SamlHttpResponse {
    let request_path = request
        .path
        .strip_prefix(AUTH_PREFIX)
        .unwrap_or(&request.path)
        .to_owned();

    if request.method == "GET" {
        let provider = match handle_get_provider(deps, &query_get(&request.query, "pc")).await {
            Ok(provider) => provider,
            Err(response) => return response,
        };
        match request_path.as_str() {
            "/metadata" => {
                let service_provider = match provider.service_provider().await {
                    Ok(service_provider) => service_provider,
                    Err(_) => return SamlHttpResponse::error(500, ""),
                };
                let metadata = service_provider_metadata(&service_provider, deps.now);
                return SamlHttpResponse {
                    status: 200,
                    body: metadata.into_bytes(),
                    content_type: "application/samlmetadata+xml; charset=utf-8".to_owned(),
                    location: String::new(),
                    set_cookies: Vec::new(),
                };
            }
            "/login" => {
                return match redirect_to_auth_url(
                    &provider,
                    &request.referer,
                    deps.external_url,
                    deps.now,
                )
                .await
                {
                    Ok(response) | Err(response) => response,
                };
            }
            _ => {}
        }
    }

    if request.method != "POST" {
        return SamlHttpResponse::error(405, "");
    }

    let relay_state = RelayState::decode(&form_value(request, "RelayState"));
    let provider = match handle_get_provider(deps, &relay_state.provider_id).await {
        Ok(provider) => provider,
        Err(response) => return response,
    };

    match request_path.as_str() {
        "/acs" => handle_acs(deps, request, &provider, &relay_state).await,
        "/logout" => handle_logout(deps, request, &provider).await,
        _ => SamlHttpResponse::error(404, ""),
    }
}

fn form_value(request: &SamlHttpRequest, key: &str) -> String {
    if let Some((_, value)) = request.form.iter().find(|(name, _)| name == key) {
        return value.clone();
    }
    query_get(&request.query, key)
}

async fn handle_get_provider(
    deps: &SamlDeps<'_>,
    pc_id: &str,
) -> Result<Arc<SamlProvider>, SamlHttpResponse> {
    let Some(provider) = deps.providers.get_provider(pc_id) else {
        return Err(SamlHttpResponse::error(500, ERR_MISCONFIGURED));
    };
    if provider.refresh(deps.external_url).await.is_err() {
        return Err(SamlHttpResponse::error(500, ERR_GET_PROVIDER));
    }
    Ok(provider)
}

async fn handle_acs(
    deps: &SamlDeps<'_>,
    request: &SamlHttpRequest,
    provider: &SamlProvider,
    relay_state: &RelayState,
) -> SamlHttpResponse {
    let encoded_response = form_value(request, "SAMLResponse");
    let info = match read_authn_response(provider, &encoded_response, deps.now).await {
        Ok(info) => info,
        Err(error) => {
            tracing::error!(
                err = %error,
                "Error validating SAML assertions. Set the env var INSECURE_SAML_LOG_TRACES=1 to log all SAML requests and responses."
            );
            return SamlHttpResponse::error(403, ERR_VALIDATE_ASSERTIONS);
        }
    };

    if !allow_signin(&provider.config, &info.groups) {
        tracing::warn!(
            account_id = %info.account_id,
            expected_groups = ?provider.config.allow_groups,
            got_groups = ?info.groups,
            "Error authorizing SAML-authenticated user."
        );
        return SamlHttpResponse::error(403, ERR_NOT_IN_GROUPS);
    }

    let allow_signup = provider.config.allow_signup.unwrap_or(true);
    let username = match user_provisioning::normalize_username(
        &info.unnormalized_username,
        deps.suffix,
        deps.sourcegraph_dotcom,
    ) {
        Ok(username) => username,
        Err(_) => {
            return SamlHttpResponse::error(
                500,
                &format!(
                    "Error normalizing the username {}. See https://sourcegraph.com/docs/admin/auth/#username-normalization.",
                    go_json_string(&info.unnormalized_username)
                ),
            )
        }
    };

    let new_user = SamlNewUser {
        username: &username,
        email: &info.email,
        display_name: &info.display_name,
        service_id: &info.service_id,
        client_id: &info.client_id,
        account_id: &info.account_id,
        create_if_not_exist: allow_signup,
    };
    let outcome = match deps.provisioner.get_or_create_user(&new_user).await {
        Ok(outcome) => outcome,
        Err(error) => {
            tracing::error!(
                err = %error.message,
                user_err = %error.safe_message,
                "Error looking up SAML-authenticated user."
            );
            return SamlHttpResponse::error(500, &error.safe_message);
        }
    };

    let user = match deps.users.get_by_id(outcome.user_id()).await {
        Ok(Some(user)) => user,
        Ok(None) => {
            tracing::error!(
                error = "user not found",
                "Error retrieving SAML-authenticated user from database."
            );
            return SamlHttpResponse::error(500, "Failed to retrieve user: user not found");
        }
        Err(error) => {
            tracing::error!(
                error = %error,
                "Error retrieving SAML-authenticated user from database."
            );
            return SamlHttpResponse::error(500, &format!("Failed to retrieve user: {error}"));
        }
    };

    if deps.license_expired && !user.site_admin {
        return SamlHttpResponse::error(500, ERR_START_SESSION);
    }

    let mut manager = SessionManager::new(
        deps.store,
        deps.ids,
        deps.cookie_key,
        WriteOptions::new(deps.secure),
        deps.now,
        request.session_cookie.as_deref(),
        request.has_sign_out_cookie,
    );
    let actor = Actor {
        uid: user.id,
        ..Actor::default()
    };
    if manager
        .set_actor(
            Some(&actor),
            0,
            deps.configured_expiry,
            GoTime(user.created_at),
        )
        .await
        .is_err()
    {
        tracing::error!("Error setting SAML-authenticated actor in session.");
        return SamlHttpResponse::error(500, ERR_START_SESSION);
    }

    let redirect_url = add_post_auth_redirect_parameters_to_string(
        &relay_state.return_to_url,
        outcome.new_user_created(),
        "SAML",
    );
    let mut response = SamlHttpResponse::redirect(safe_redirect_url(&redirect_url));
    response.set_cookies = manager.cookies;
    response
}

async fn handle_logout(
    deps: &SamlDeps<'_>,
    request: &SamlHttpRequest,
    provider: &SamlProvider,
) -> SamlHttpResponse {
    let encoded_response = form_value(request, "SAMLResponse");
    // Go ignores `unable to unmarshal response:` failures here (the TODO above
    // notes that the LogoutResponse is intentionally only lightly validated).
    let Ok(raw) = base64::engine::general_purpose::STANDARD.decode(&encoded_response) else {
        tracing::error!(
            err = "illegal base64 data",
            "Error validating SAML logout response."
        );
        return SamlHttpResponse::error(403, ERR_LOGOUT_VALIDATE);
    };
    if parse_xml(&raw).is_ok() {
        if let Ok(service_provider) = provider.service_provider().await {
            if let Err(error) =
                validate_encoded_response(&encoded_response, &service_provider, deps.now)
            {
                tracing::error!(err = %error, "Error validating SAML logout response.");
                return SamlHttpResponse::error(403, ERR_LOGOUT_VALIDATE);
            }
        }
    }

    let mut manager = SessionManager::new(
        deps.store,
        deps.ids,
        deps.cookie_key,
        WriteOptions::new(deps.secure),
        deps.now,
        request.session_cookie.as_deref(),
        request.has_sign_out_cookie,
    );
    if manager
        .set_actor(None, 0, None, GoTime(deps.now))
        .await
        .is_err()
    {
        tracing::error!("Error clearing actor from session in SAML logout handler.");
        return SamlHttpResponse::error(500, ERR_LOGOUT_SESSION);
    }
    let mut response = SamlHttpResponse::redirect("/".to_owned());
    response.set_cookies = manager.cookies;
    response
}

async fn redirect_to_auth_url(
    provider: &SamlProvider,
    return_to: &str,
    external_url: &str,
    now: OffsetDateTime,
) -> Result<SamlHttpResponse, SamlHttpResponse> {
    let relay_state = RelayState {
        provider_id: provider.config_id(),
        return_to_url: safe_redirect_url(return_to),
    };
    match build_auth_url_redirect(provider, &relay_state, external_url, now).await {
        Ok(url) => Ok(SamlHttpResponse::redirect_get(url)),
        Err(error) => {
            tracing::error!(err = %error, "Failed to build SAML auth URL.");
            Err(SamlHttpResponse::error(500, ERR_BUILD_AUTH_URL))
        }
    }
}

// ---------------------------------------------------------------------------
// AuthnRequest construction (build_request.go)
// ---------------------------------------------------------------------------

fn xml_attr(key: &str, value: &str) -> XmlAttr {
    XmlAttr {
        space: String::new(),
        key: key.to_owned(),
        value: value.to_owned(),
    }
}

fn xml_element(
    prefix: &str,
    local: &str,
    attrs: Vec<XmlAttr>,
    children: Vec<XmlNode>,
) -> XmlElement {
    XmlElement {
        prefix: prefix.to_owned(),
        local: local.to_owned(),
        attrs,
        children,
    }
}

fn text_node(value: &str) -> XmlNode {
    XmlNode::Text(value.to_owned())
}

fn go_issue_instant(now: OffsetDateTime) -> String {
    now.to_offset(UtcOffset::UTC)
        .format(&Rfc3339)
        .unwrap_or_default()
}

/// `buildAuthnRequest` (`gosaml2/build_request.go:28-95`), including the
/// enveloped XML signature when `SignAuthnRequests` is set.
/// The subset of an incoming `AuthnRequest` the middleware/login tests inspect
/// (the Go side unmarshals into `saml.AuthnRequest`).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DecodedAuthnRequest {
    pub version: String,
    pub destination: String,
    pub assertion_consumer_service_url: String,
    pub protocol_binding: String,
    pub issuer: String,
    pub name_id_format: String,
    pub allow_create: bool,
    pub signed: bool,
}

/// Decode the HTTP-Redirect `SAMLRequest` query value (base64 + DEFLATE + XML).
pub fn decode_authn_request(encoded: &str) -> Result<DecodedAuthnRequest, String> {
    let compressed = base64::engine::general_purpose::STANDARD
        .decode(encoded)
        .map_err(|error| error.to_string())?;
    let mut decoder = flate2::read::DeflateDecoder::new(compressed.as_slice());
    let mut xml = Vec::new();
    use std::io::Read as _;
    decoder
        .read_to_end(&mut xml)
        .map_err(|error| error.to_string())?;
    let root = parse_xml(&xml)?;
    if root.local != "AuthnRequest" {
        return Err("not an AuthnRequest".to_owned());
    }
    let mut decoded = DecodedAuthnRequest {
        version: root.attr("Version").unwrap_or_default().to_owned(),
        destination: root.attr("Destination").unwrap_or_default().to_owned(),
        assertion_consumer_service_url: root
            .attr("AssertionConsumerServiceURL")
            .unwrap_or_default()
            .to_owned(),
        protocol_binding: root.attr("ProtocolBinding").unwrap_or_default().to_owned(),
        ..DecodedAuthnRequest::default()
    };
    for child in root.element_children() {
        match child.local.as_str() {
            "Issuer" => decoded.issuer = child.text_content(),
            "NameIDPolicy" => {
                decoded.name_id_format = child.attr("Format").unwrap_or_default().to_owned();
                decoded.allow_create = child.attr("AllowCreate").unwrap_or("false") == "true";
            }
            "Signature" => decoded.signed = true,
            _ => {}
        }
    }
    Ok(decoded)
}

fn build_authn_request(
    service_provider: &SamlServiceProvider,
    now: OffsetDateTime,
) -> Result<XmlElement, String> {
    let id = format!("_{}", uuid::Uuid::new_v4());
    let mut attrs = vec![
        xml_attr("xmlns:samlp", "urn:oasis:names:tc:SAML:2.0:protocol"),
        xml_attr("xmlns:saml", "urn:oasis:names:tc:SAML:2.0:assertion"),
        xml_attr("ID", &id),
        xml_attr("Version", "2.0"),
        xml_attr(
            "ProtocolBinding",
            "urn:oasis:names:tc:SAML:2.0:bindings:HTTP-POST",
        ),
        xml_attr(
            "AssertionConsumerServiceURL",
            &service_provider.assertion_consumer_service_url,
        ),
        xml_attr("IssueInstant", &go_issue_instant(now)),
        xml_attr("Destination", &service_provider.identity_provider_sso_url),
    ];
    let mut children = Vec::new();
    let issuer_value = if service_provider.service_provider_issuer.is_empty() {
        service_provider.identity_provider_issuer.clone()
    } else {
        service_provider.service_provider_issuer.clone()
    };
    children.push(XmlNode::Element(xml_element(
        "saml",
        "Issuer",
        Vec::new(),
        vec![text_node(&issuer_value)],
    )));
    let mut name_id_attrs = vec![xml_attr("AllowCreate", "true")];
    if !service_provider.name_id_format.is_empty() {
        name_id_attrs.push(xml_attr("Format", &service_provider.name_id_format));
    }
    children.push(XmlNode::Element(xml_element(
        "samlp",
        "NameIDPolicy",
        name_id_attrs,
        Vec::new(),
    )));
    let mut root = xml_element(
        "samlp",
        "AuthnRequest",
        std::mem::take(&mut attrs),
        children,
    );

    if service_provider.sign_authn_requests {
        let signature = sign_element(&root, service_provider)?;
        root.children.insert(1, XmlNode::Element(signature));
    }
    Ok(root)
}

fn sign_element(
    root: &XmlElement,
    service_provider: &SamlServiceProvider,
) -> Result<XmlElement, String> {
    let Some(private_key_der) = &service_provider.sp_private_key else {
        return Err("no SP private key".to_owned());
    };
    let Some(certificate) = &service_provider.sp_certificate else {
        return Err("no SP certificate".to_owned());
    };
    let private_key = rsa_private_key(private_key_der)?;
    let id = root.attr("ID").unwrap_or_default().to_owned();

    let canonical = canonical_prep(root, &BTreeMap::new(), false);
    let digest = sha2::Sha256::digest(serialize_canonical(&canonical)).to_vec();

    let digest_value = base64::engine::general_purpose::STANDARD.encode(digest);
    let signed_info = xml_element(
        "ds",
        "SignedInfo",
        Vec::new(),
        vec![
            XmlNode::Element(xml_element(
                "ds",
                "CanonicalizationMethod",
                vec![xml_attr(
                    "Algorithm",
                    "http://www.w3.org/2006/12/xml-c14n11",
                )],
                Vec::new(),
            )),
            XmlNode::Element(xml_element(
                "ds",
                "SignatureMethod",
                vec![xml_attr(
                    "Algorithm",
                    "http://www.w3.org/2001/04/xmldsig-more#rsa-sha256",
                )],
                Vec::new(),
            )),
            XmlNode::Element(xml_element(
                "ds",
                "Reference",
                vec![xml_attr("URI", &format!("#{id}"))],
                vec![
                    XmlNode::Element(xml_element(
                        "ds",
                        "Transforms",
                        Vec::new(),
                        vec![
                            XmlNode::Element(xml_element(
                                "ds",
                                "Transform",
                                vec![xml_attr(
                                    "Algorithm",
                                    "http://www.w3.org/2000/09/xmldsig#enveloped-signature",
                                )],
                                Vec::new(),
                            )),
                            XmlNode::Element(xml_element(
                                "ds",
                                "Transform",
                                vec![xml_attr(
                                    "Algorithm",
                                    "http://www.w3.org/2006/12/xml-c14n11",
                                )],
                                Vec::new(),
                            )),
                        ],
                    )),
                    XmlNode::Element(xml_element(
                        "ds",
                        "DigestMethod",
                        vec![xml_attr(
                            "Algorithm",
                            "http://www.w3.org/2001/04/xmlenc#sha256",
                        )],
                        Vec::new(),
                    )),
                    XmlNode::Element(xml_element(
                        "ds",
                        "DigestValue",
                        Vec::new(),
                        vec![text_node(&digest_value)],
                    )),
                ],
            )),
        ],
    );

    // The Signature element (with `xmlns:ds`) provides the SignatureMethod's
    // in-scope namespace during detaching.
    let signature_wrapper = xml_element(
        "ds",
        "Signature",
        vec![xml_attr("xmlns:ds", DSIG_NS)],
        Vec::new(),
    );
    let mut root_ctx = NsCtx::new_default().sub_context(root)?;
    root_ctx = root_ctx.sub_context(&signature_wrapper)?;
    let detached = ns_detach(&root_ctx, &signed_info)?;
    let signed_info_canonical =
        serialize_canonical(&canonical_prep(&detached, &BTreeMap::new(), false));
    let signature_digest = sha2::Sha256::digest(&signed_info_canonical).to_vec();
    let signature = private_key
        .sign(rsa::Pkcs1v15Sign::new::<sha2::Sha256>(), &signature_digest)
        .map_err(|error| error.to_string())?;
    let signature_value = base64::engine::general_purpose::STANDARD.encode(signature);
    let certificate_base64 = base64::engine::general_purpose::STANDARD.encode(certificate);

    Ok(xml_element(
        "ds",
        "Signature",
        vec![xml_attr("xmlns:ds", DSIG_NS)],
        vec![
            XmlNode::Element(signed_info),
            XmlNode::Element(xml_element(
                "ds",
                "SignatureValue",
                Vec::new(),
                vec![text_node(&signature_value)],
            )),
            XmlNode::Element(xml_element(
                "ds",
                "KeyInfo",
                Vec::new(),
                vec![XmlNode::Element(xml_element(
                    "ds",
                    "X509Data",
                    Vec::new(),
                    vec![XmlNode::Element(xml_element(
                        "ds",
                        "X509Certificate",
                        Vec::new(),
                        vec![text_node(&certificate_base64)],
                    ))],
                ))],
            )),
        ],
    ))
}

async fn build_auth_url_redirect(
    provider: &SamlProvider,
    relay_state: &RelayState,
    _external_url: &str,
    now: OffsetDateTime,
) -> Result<String, String> {
    let service_provider = provider.service_provider().await?;
    let document = build_authn_request(&service_provider, now)?;
    let mut xml = String::new();
    serialize_document(&document, &mut xml);

    let mut encoder =
        flate2::write::DeflateEncoder::new(Vec::new(), flate2::Compression::default());
    use std::io::Write as _;
    encoder
        .write_all(xml.as_bytes())
        .map_err(|error| error.to_string())?;
    let compressed = encoder.finish().map_err(|error| error.to_string())?;
    let saml_request = base64::engine::general_purpose::STANDARD.encode(compressed);

    let relay_state_value = relay_state.encode();
    let parsed = go_parse_url(&service_provider.identity_provider_sso_url)
        .ok_or_else(|| "invalid IdP SSO URL".to_owned())?;
    let mut pairs = parse_query(&parsed.raw_query);
    pairs.push(("SAMLRequest".to_owned(), saml_request.clone()));
    if !relay_state_value.is_empty() {
        pairs.push(("RelayState".to_owned(), relay_state_value.clone()));
    }
    if service_provider.sign_authn_requests {
        let sig_alg = "http://www.w3.org/2001/04/xmldsig-more#rsa-sha256";
        pairs.push(("SigAlg".to_owned(), sig_alg.to_owned()));
        let input = signature_input_string(&saml_request, &relay_state_value, sig_alg);
        let Some(private_key_der) = &service_provider.sp_private_key else {
            return Err("no SP private key".to_owned());
        };
        let private_key = rsa_private_key(private_key_der)?;
        let digest = sha2::Sha256::digest(input.as_bytes()).to_vec();
        let signature = private_key
            .sign(rsa::Pkcs1v15Sign::new::<sha2::Sha256>(), &digest)
            .map_err(|error| error.to_string())?;
        pairs.push((
            "Signature".to_owned(),
            base64::engine::general_purpose::STANDARD.encode(signature),
        ));
    }
    let raw_query = encode_query_sorted(&pairs);
    Ok(go_url_string(&crate::oauth_login::GoUrl {
        scheme: parsed.scheme,
        host: parsed.host,
        path: parsed.path,
        raw_query,
    }))
}

fn signature_input_string(saml_request: &str, relay_state: &str, sig_alg: &str) -> String {
    let mut params: Vec<(&str, &str)> = vec![("SAMLRequest", saml_request)];
    if !relay_state.is_empty() {
        params.push(("RelayState", relay_state));
    }
    params.push(("SigAlg", sig_alg));
    params
        .iter()
        .map(|(key, value)| format!("{}={}", url_query_escape(key), url_query_escape(value)))
        .collect::<Vec<_>>()
        .join("&")
}

fn url_query_escape(value: &str) -> String {
    crate::oauth_login::query_escape(value)
}

/// `SAMLServiceProvider.Metadata` (`gosaml2/saml.go:103-160`) serialized as the
/// endpoint's `application/samlmetadata+xml` body.
fn service_provider_metadata(
    service_provider: &SamlServiceProvider,
    now: OffsetDateTime,
) -> String {
    let mut key_descriptors = Vec::new();
    if let Some(certificate) = &service_provider.sp_certificate {
        let data = base64::engine::general_purpose::STANDARD.encode(certificate);
        for use_type in ["signing", "encryption"] {
            let mut key_info_children = vec![XmlNode::Element(xml_element(
                "ds",
                "X509Data",
                Vec::new(),
                vec![XmlNode::Element(xml_element(
                    "ds",
                    "X509Certificate",
                    Vec::new(),
                    vec![text_node(&data)],
                ))],
            ))];
            let mut children = vec![XmlNode::Element(xml_element(
                "ds",
                "KeyInfo",
                Vec::new(),
                std::mem::take(&mut key_info_children),
            ))];
            if use_type == "encryption" {
                for method in [
                    "http://www.w3.org/2009/xmlenc11#aes128-gcm",
                    "http://www.w3.org/2009/xmlenc11#aes192-gcm",
                    "http://www.w3.org/2009/xmlenc11#aes256-gcm",
                    "http://www.w3.org/2001/04/xmlenc#aes128-cbc",
                    "http://www.w3.org/2001/04/xmlenc#aes256-cbc",
                ] {
                    children.push(XmlNode::Element(xml_element(
                        "md",
                        "EncryptionMethod",
                        vec![xml_attr("Algorithm", method)],
                        Vec::new(),
                    )));
                }
            }
            key_descriptors.push(XmlNode::Element(xml_element(
                "md",
                "KeyDescriptor",
                vec![xml_attr("use", use_type)],
                children,
            )));
        }
    }

    let mut sp_children = key_descriptors;
    sp_children.push(XmlNode::Element(xml_element(
        "md",
        "AssertionConsumerService",
        vec![
            xml_attr("Binding", "urn:oasis:names:tc:SAML:2.0:bindings:HTTP-POST"),
            xml_attr("Location", &service_provider.assertion_consumer_service_url),
            xml_attr("index", "1"),
        ],
        Vec::new(),
    )));
    let sp_descriptor = xml_element(
        "md",
        "SPSSODescriptor",
        vec![
            xml_attr(
                "AuthnRequestsSigned",
                if service_provider.sign_authn_requests {
                    "true"
                } else {
                    "false"
                },
            ),
            xml_attr(
                "WantAssertionsSigned",
                if service_provider.skip_signature_validation {
                    "false"
                } else {
                    "true"
                },
            ),
            xml_attr(
                "protocolSupportEnumeration",
                "urn:oasis:names:tc:SAML:2.0:protocol",
            ),
        ],
        sp_children,
    );
    let valid_until = go_issue_instant(now + time::Duration::days(7));
    let descriptor = xml_element(
        "md",
        "EntityDescriptor",
        vec![
            xml_attr("xmlns:md", "urn:oasis:names:tc:SAML:2.0:metadata"),
            xml_attr("validUntil", &valid_until),
            xml_attr("entityID", &service_provider.service_provider_issuer),
        ],
        vec![XmlNode::Element(sp_descriptor)],
    );
    let mut out = String::from("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
    serialize_document(&indent_element(&descriptor, 0), &mut out);
    out
}

fn indent_element(element: &XmlElement, depth: usize) -> XmlElement {
    let indent = "  ".repeat(depth);
    let child_indent = "  ".repeat(depth + 1);
    let mut copy = element.clone();
    if copy.children.is_empty() {
        return copy;
    }
    let mut children = Vec::new();
    for child in copy.children {
        children.push(XmlNode::Text(format!("\n{child_indent}")));
        children.push(match child {
            XmlNode::Element(child) => XmlNode::Element(indent_element(&child, depth + 1)),
            other => other,
        });
    }
    children.push(XmlNode::Text(format!("\n{indent}")));
    copy.children = children;
    copy
}

// ---------------------------------------------------------------------------
// Production wiring helpers
// ---------------------------------------------------------------------------

use crate::user_provisioning::PgProvisionStore;
