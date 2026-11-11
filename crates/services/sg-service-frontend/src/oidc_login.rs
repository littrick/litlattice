#![forbid(unsafe_code)]

//! OpenID Connect provider (`cmd/frontend/internal/auth/openidconnect`) and
//! the `OAuthUserResolver` that resolves/creates a Sourcegraph user from an
//! OIDC id_token plus the OP's userinfo endpoint.
//!
//! The baseline talks to the OP through `github.com/coreos/go-oidc/v3`:
//! discovery at `<issuer>/.well-known/openid-configuration`, a lazily fetched
//! remote JWKS, id_token signature/claim verification and `UserInfo`. This
//! port keeps every network access behind [`OidcHttpClient`] (a fixed
//! discovery document can be supplied instead), so the default behaviour is
//! offline and the differential harness can pin the whole OP.
//!
//! Scope: discovery/JWKS/RS256+ES256 verification, `iss`/`aud`/`exp`/`nonce`/
//! `email_verified`/`email` validation, the provider config ID hash, and the
//! OIDC resolver. User creation (SQL, allowSignup, username normalization) is
//! **not** implemented here: [`OidcLoginResolver`] calls an injected
//! [`Provisioner`] whose input has the same shape as the M9.8jp
//! `user_provisioning` seam (see the overview).

use std::sync::Arc;

use async_trait::async_trait;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine as _;
use ring::signature::{
    RsaPublicKeyComponents, UnparsedPublicKey, ECDSA_P256_SHA256_FIXED, RSA_PKCS1_2048_8192_SHA256,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use time::{OffsetDateTime, UtcOffset};

use crate::oauth_login::{OAuthToken, OAuthUserResolver, UserResolution};

// ---------------------------------------------------------------------------
// Provider config (`schema.OpenIDConnectAuthProvider`) and config ID
// ---------------------------------------------------------------------------

/// `schema.OpenIDConnectAuthProvider` (`schema/schema.go:2130-2157`).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct OpenIdConnectAuthProvider {
    #[serde(
        rename = "allowSignup",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub allow_signup: Option<bool>,
    #[serde(rename = "clientID", default)]
    pub client_id: String,
    #[serde(rename = "clientSecret", default)]
    pub client_secret: String,
    #[serde(rename = "configID", default)]
    pub config_id: String,
    #[serde(rename = "displayName", default)]
    pub display_name: String,
    #[serde(rename = "displayPrefix", default)]
    pub display_prefix: Option<String>,
    #[serde(default)]
    pub hidden: bool,
    #[serde(default)]
    pub issuer: String,
    #[serde(rename = "noSignIn", default)]
    pub no_sign_in: bool,
    #[serde(default)]
    pub order: i64,
    #[serde(rename = "requireEmailDomain", default)]
    pub require_email_domain: String,
    #[serde(rename = "singleIdentityPerUser", default)]
    pub single_identity_per_user: bool,
    #[serde(rename = "type", default)]
    pub provider_type: String,
}

/// Go `encoding/json` string escaping (HTML escaping included).
fn go_json_string(value: &str) -> String {
    let mut out = String::with_capacity(value.len() + 2);
    out.push('"');
    for ch in value.chars() {
        match ch {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            '\u{08}' => out.push_str("\\b"),
            '\u{0c}' => out.push_str("\\f"),
            '<' => out.push_str("\\u003c"),
            '>' => out.push_str("\\u003e"),
            '&' => out.push_str("\\u0026"),
            '\u{2028}' => out.push_str("\\u2028"),
            '\u{2029}' => out.push_str("\\u2029"),
            c if (c as u32) < 0x20 => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

fn go_quote(value: &str) -> String {
    let mut out = String::with_capacity(value.len() + 2);
    out.push('"');
    for ch in value.chars() {
        match ch {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => out.push_str(&format!("\\x{:02x}", c as u32)),
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

/// `json.Marshal(config)` for `providerConfigID`: declaration order and
/// `omitempty` semantics of `schema.OpenIDConnectAuthProvider`.
pub fn config_to_go_json(config: &OpenIdConnectAuthProvider) -> String {
    let mut fields: Vec<String> = Vec::with_capacity(13);
    let mut field = |name: &str, value: String| fields.push(format!("\"{name}\":{value}"));
    if let Some(value) = config.allow_signup {
        field("allowSignup", value.to_string());
    }
    field("clientID", go_json_string(&config.client_id));
    field("clientSecret", go_json_string(&config.client_secret));
    if !config.config_id.is_empty() {
        field("configID", go_json_string(&config.config_id));
    }
    if !config.display_name.is_empty() {
        field("displayName", go_json_string(&config.display_name));
    }
    if let Some(prefix) = &config.display_prefix {
        field("displayPrefix", go_json_string(prefix));
    }
    if config.hidden {
        field("hidden", "true".to_owned());
    }
    field("issuer", go_json_string(&config.issuer));
    if config.no_sign_in {
        field("noSignIn", "true".to_owned());
    }
    if config.order != 0 {
        field("order", config.order.to_string());
    }
    if !config.require_email_domain.is_empty() {
        field(
            "requireEmailDomain",
            go_json_string(&config.require_email_domain),
        );
    }
    if config.single_identity_per_user {
        field("singleIdentityPerUser", "true".to_owned());
    }
    field("type", go_json_string(&config.provider_type));
    format!("{{{}}}", fields.join(","))
}

/// `providerConfigID` (`config.go:134-144`): explicit `configID`, otherwise
/// `base64.RawURLEncoding(sha256(json(config))[:16])`.
pub fn provider_config_id(config: &OpenIdConnectAuthProvider) -> String {
    if !config.config_id.is_empty() {
        return config.config_id.clone();
    }
    let digest = Sha256::digest(config_to_go_json(config).as_bytes());
    URL_SAFE_NO_PAD.encode(&digest[..16])
}

// ---------------------------------------------------------------------------
// Discovery
// ---------------------------------------------------------------------------

/// One GET response from the OP (exactly status + body: enough for go-oidc's
/// status/body error texts).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OidcHttpResponse {
    pub status: u16,
    pub body: Vec<u8>,
}

/// All OP network access. Production wires `reqwest`; the differential
/// harness serves fixed documents.
#[async_trait]
pub trait OidcHttpClient: Send + Sync {
    async fn get(&self, url: &str, bearer: Option<&str>) -> Result<OidcHttpResponse, String>;
}

/// Discovery document (`go-oidc` `providerJSON` + `providerExtraClaims`).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct OidcDiscoveryDocument {
    #[serde(default)]
    pub issuer: String,
    #[serde(default, rename = "authorization_endpoint")]
    pub authorization_endpoint: String,
    #[serde(default, rename = "token_endpoint")]
    pub token_endpoint: String,
    #[serde(default, rename = "jwks_uri")]
    pub jwks_uri: String,
    #[serde(default, rename = "userinfo_endpoint")]
    pub userinfo_endpoint: String,
    #[serde(default, rename = "id_token_signing_alg_values_supported")]
    pub algorithms: Vec<String>,
    #[serde(default, rename = "end_session_endpoint")]
    pub end_session_endpoint: String,
    #[serde(default, rename = "revocation_endpoint")]
    pub revocation_endpoint: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct OidcProvider {
    pub issuer: String,
    pub document: OidcDiscoveryDocument,
}

/// go-oidc `supportedAlgorithms` (`oidc.go:118-130`).
const SUPPORTED_ALGORITHMS: &[&str] = &[
    "RS256", "RS384", "RS512", "ES256", "ES384", "ES512", "PS256", "PS384", "PS512",
];

impl OidcProvider {
    /// Algorithms retained from the discovery document.
    pub fn algorithms(&self) -> Vec<String> {
        self.document
            .algorithms
            .iter()
            .filter(|alg| SUPPORTED_ALGORITHMS.contains(&alg.as_str()))
            .cloned()
            .collect()
    }

    /// `(authorization_endpoint, token_endpoint)` (`oidc.Provider.Endpoint`).
    pub fn endpoint(&self) -> (String, String) {
        (
            self.document.authorization_endpoint.clone(),
            self.document.token_endpoint.clone(),
        )
    }
}

fn status_text(status: u16) -> &'static str {
    match status {
        200 => "OK",
        201 => "Created",
        204 => "No Content",
        301 => "Moved Permanently",
        302 => "Found",
        303 => "See Other",
        307 => "Temporary Redirect",
        308 => "Permanent Redirect",
        400 => "Bad Request",
        401 => "Unauthorized",
        403 => "Forbidden",
        404 => "Not Found",
        405 => "Method Not Allowed",
        409 => "Conflict",
        410 => "Gone",
        422 => "Unprocessable Entity",
        429 => "Too Many Requests",
        500 => "Internal Server Error",
        502 => "Bad Gateway",
        503 => "Service Unavailable",
        504 => "Gateway Timeout",
        _ => "",
    }
}

fn status_line(status: u16) -> String {
    let text = status_text(status);
    if text.is_empty() {
        status.to_string()
    } else {
        format!("{status} {text}")
    }
}

fn decode_discovery(body: &[u8]) -> Result<OidcDiscoveryDocument, String> {
    serde_json::from_slice(body)
        .map_err(|error| format!("oidc: failed to decode provider discovery object: {error}"))
}

fn check_discovery_issuer(
    issuer: &str,
    document: OidcDiscoveryDocument,
) -> Result<OidcProvider, String> {
    if document.issuer != issuer {
        return Err(format!(
            "oidc: issuer did not match the issuer returned by provider, expected {} got {}",
            go_quote(issuer),
            go_quote(&document.issuer)
        ));
    }
    Ok(OidcProvider {
        issuer: document.issuer.clone(),
        document,
    })
}

/// `oidc.NewProvider`: GET `<issuer>/.well-known/openid-configuration`.
pub async fn discover(client: &dyn OidcHttpClient, issuer: &str) -> Result<OidcProvider, String> {
    let well_known = format!(
        "{}/.well-known/openid-configuration",
        issuer.trim_end_matches('/')
    );
    let response = client.get(&well_known, None).await?;
    if response.status != 200 {
        return Err(format!(
            "{}: {}",
            status_line(response.status),
            String::from_utf8_lossy(&response.body)
        ));
    }
    check_discovery_issuer(issuer, decode_discovery(&response.body)?)
}

/// Fixed discovery document (no network): same validation as `discover`.
pub fn provider_from_document(issuer: &str, document: &[u8]) -> Result<OidcProvider, String> {
    check_discovery_issuer(issuer, decode_discovery(document)?)
}

// ---------------------------------------------------------------------------
// JWKS + id_token verification
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JwkMaterial {
    Rsa { n: Vec<u8>, e: Vec<u8> },
    EcP256 { x: Vec<u8>, y: Vec<u8> },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JwkKey {
    pub kid: String,
    pub alg: String,
    pub material: JwkMaterial,
}

#[derive(Debug, Deserialize)]
struct JwkSetJson {
    #[serde(default)]
    keys: Vec<JwkJson>,
}

#[derive(Debug, Deserialize)]
struct JwkJson {
    #[serde(default)]
    kty: String,
    #[serde(default)]
    kid: String,
    #[serde(default)]
    alg: String,
    #[serde(default)]
    n: String,
    #[serde(default)]
    e: String,
    #[serde(default)]
    crv: String,
    #[serde(default)]
    x: String,
    #[serde(default)]
    y: String,
}

fn b64url(value: &str) -> Option<Vec<u8>> {
    URL_SAFE_NO_PAD.decode(value).ok()
}

fn parse_jwks(body: &[u8]) -> Result<Vec<JwkKey>, String> {
    let set: JwkSetJson = serde_json::from_slice(body)
        .map_err(|error| format!("oidc: failed to decode keys: {error}"))?;
    let mut keys = Vec::new();
    for key in set.keys {
        let material = match key.kty.as_str() {
            "RSA" => match (b64url(&key.n), b64url(&key.e)) {
                (Some(n), Some(e)) => JwkMaterial::Rsa { n, e },
                _ => continue,
            },
            "EC" if key.crv == "P-256" => match (b64url(&key.x), b64url(&key.y)) {
                (Some(x), Some(y)) => JwkMaterial::EcP256 { x, y },
                _ => continue,
            },
            _ => continue,
        };
        keys.push(JwkKey {
            kid: key.kid,
            alg: key.alg,
            material,
        });
    }
    Ok(keys)
}

async fn fetch_jwks(client: &dyn OidcHttpClient, url: &str) -> Result<Vec<JwkKey>, String> {
    let response = client
        .get(url, None)
        .await
        .map_err(|error| format!("oidc: get keys failed {error}"))?;
    if response.status != 200 {
        return Err(format!(
            "oidc: get keys failed: {} {}",
            status_line(response.status),
            String::from_utf8_lossy(&response.body)
        ));
    }
    parse_jwks(&response.body)
}

fn pad32(value: &[u8]) -> Vec<u8> {
    let mut out = vec![0u8; 32usize.saturating_sub(value.len())];
    out.extend_from_slice(value);
    out
}

fn verify_jwk_signature(key: &JwkKey, algorithm: &str, message: &[u8], signature: &[u8]) -> bool {
    if !key.alg.is_empty() && key.alg != algorithm {
        return false;
    }
    match (&key.material, algorithm) {
        (JwkMaterial::Rsa { n, e }, "RS256") => {
            let components = RsaPublicKeyComponents { n, e };
            components
                .verify(&RSA_PKCS1_2048_8192_SHA256, message, signature)
                .is_ok()
        }
        (JwkMaterial::EcP256 { x, y }, "ES256") => {
            let mut point = Vec::with_capacity(65);
            point.push(0x04);
            point.extend_from_slice(&pad32(x));
            point.extend_from_slice(&pad32(y));
            UnparsedPublicKey::new(&ECDSA_P256_SHA256_FIXED, point)
                .verify(message, signature)
                .is_ok()
        }
        _ => false,
    }
}

/// `userClaims` (`middleware.go:44-52`).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct UserClaims {
    #[serde(default)]
    pub name: String,
    #[serde(rename = "given_name", default)]
    pub given_name: String,
    #[serde(rename = "family_name", default)]
    pub family_name: String,
    #[serde(rename = "preferred_username", default)]
    pub preferred_username: String,
    #[serde(default)]
    pub picture: String,
    #[serde(rename = "email_verified", default)]
    pub email_verified: Option<bool>,
}

impl UserClaims {
    /// Best-effort equivalent of `json.Unmarshal(raw, &userClaims)`: the
    /// baseline logs and ignores claim decode errors, leaving zero values.
    pub fn from_json(value: &Value) -> Self {
        let string = |key: &str| {
            value
                .get(key)
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_owned()
        };
        Self {
            name: string("name"),
            given_name: string("given_name"),
            family_name: string("family_name"),
            preferred_username: string("preferred_username"),
            picture: string("picture"),
            email_verified: value.get("email_verified").and_then(Value::as_bool),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerifiedIdToken {
    pub issuer: String,
    pub subject: String,
    pub audience: Vec<String>,
    pub expiry_unix: i64,
    pub issued_at_unix: i64,
    pub nonce: String,
    pub claims: UserClaims,
}

fn go_time_string(unix: i64) -> String {
    let datetime = OffsetDateTime::from_unix_timestamp(unix)
        .unwrap_or(OffsetDateTime::UNIX_EPOCH)
        .to_offset(UtcOffset::UTC);
    format!(
        "{:04}-{:02}-{:02} {:02}:{:02}:{:02} +0000 UTC",
        datetime.year(),
        u8::from(datetime.month()),
        datetime.day(),
        datetime.hour(),
        datetime.minute(),
        datetime.second()
    )
}

fn go_quote_list(values: &[String]) -> String {
    format!(
        "[{}]",
        values
            .iter()
            .map(|value| go_quote(value))
            .collect::<Vec<_>>()
            .join(" ")
    )
}

fn decode_segment(segment: &str) -> Option<Vec<u8>> {
    URL_SAFE_NO_PAD.decode(segment).ok()
}

/// `oidc.IDTokenVerifier.Verify` (+ caller-side nonce check is separate).
#[allow(clippy::too_many_arguments)]
pub async fn verify_id_token(
    provider: &OidcProvider,
    client: &dyn OidcHttpClient,
    raw_id_token: &str,
    client_id: &str,
    now_unix: i64,
) -> Result<VerifiedIdToken, String> {
    let parts: Vec<&str> = raw_id_token.split('.').collect();
    if parts.len() != 3 {
        return Err(
            "oidc: malformed jwt: go-jose/go-jose: compact JWS format must have three parts"
                .to_owned(),
        );
    }
    let header: Value = decode_segment(parts[0])
        .and_then(|bytes| serde_json::from_slice(&bytes).ok())
        .ok_or_else(|| "oidc: malformed jwt: go-jose/go-jose: invalid header".to_owned())?;
    let payload_bytes = decode_segment(parts[1])
        .ok_or_else(|| "oidc: malformed jwt: oidc: malformed jwt payload".to_owned())?;
    let payload: Value = serde_json::from_slice(&payload_bytes)
        .map_err(|error| format!("oidc: failed to unmarshal claims: {error}"))?;

    let issuer = payload
        .get("iss")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let subject = payload
        .get("sub")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let audience: Vec<String> = match payload.get("aud") {
        Some(Value::String(value)) => vec![value.clone()],
        Some(Value::Array(values)) => values
            .iter()
            .filter_map(Value::as_str)
            .map(str::to_owned)
            .collect(),
        _ => Vec::new(),
    };
    let expiry_unix = payload
        .get("exp")
        .and_then(Value::as_i64)
        .unwrap_or_default();
    let issued_at_unix = payload
        .get("iat")
        .and_then(Value::as_i64)
        .unwrap_or_default();
    let nonce = payload
        .get("nonce")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_owned();

    if issuer != provider.issuer {
        return Err(format!(
            "oidc: id token issued by a different provider, expected {} got {}",
            go_quote(&provider.issuer),
            go_quote(issuer)
        ));
    }
    if !audience.iter().any(|value| value == client_id) {
        return Err(format!(
            "oidc: expected audience {} got {}",
            go_quote(client_id),
            go_quote_list(&audience)
        ));
    }
    if expiry_unix < now_unix {
        return Err(format!(
            "oidc: token is expired (Token Expiry: {})",
            go_time_string(expiry_unix)
        ));
    }
    if let Some(not_before) = payload.get("nbf").and_then(Value::as_i64) {
        if now_unix + 60 < not_before {
            return Err(format!(
                "oidc: current time {} before the nbf (not before) time: {}",
                go_time_string(now_unix),
                go_time_string(not_before)
            ));
        }
    }

    let header_algorithm = header
        .get("alg")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_owned();
    let header_kid = header
        .get("kid")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_owned();

    let mut algorithms = provider.algorithms();
    if algorithms.is_empty() {
        algorithms.push("RS256".to_owned());
    }
    if !algorithms.iter().any(|alg| alg == &header_algorithm) {
        return Err(format!(
            "oidc: id token signed with unsupported algorithm, expected {} got {}",
            go_quote_list(&algorithms),
            go_quote(&header_algorithm)
        ));
    }

    let keys = fetch_jwks(client, &provider.document.jwks_uri)
        .await
        .map_err(|error| format!("failed to verify signature: fetching keys {error}"))?;
    let signature = decode_segment(parts[2]).ok_or_else(|| {
        "failed to verify signature: failed to verify id token signature".to_owned()
    })?;
    let signing_input = raw_id_token
        .rfind('.')
        .map(|index| &raw_id_token[..index])
        .unwrap_or(raw_id_token)
        .as_bytes();
    let verified = keys.iter().any(|key| {
        (header_kid.is_empty() || key.kid == header_kid)
            && verify_jwk_signature(key, &header_algorithm, signing_input, &signature)
    });
    if !verified {
        return Err("failed to verify signature: failed to verify id token signature".to_owned());
    }

    Ok(VerifiedIdToken {
        issuer: issuer.to_owned(),
        subject: subject.to_owned(),
        audience,
        expiry_unix,
        issued_at_unix,
        nonce,
        claims: UserClaims::from_json(&payload),
    })
}

/// Caller-side nonce check (`AuthCallback`: `idToken.Nonce != stateParam`).
pub const NONCE_SAFE_ERROR: &str = "Authentication failed. Try signing in again (and clearing cookies for the current site). The error was: OpenID Connect nonce is incorrect (possible replay attack).";

pub fn check_nonce(actual: &str, expected: &str) -> Result<(), String> {
    if actual == expected {
        Ok(())
    } else {
        Err(NONCE_SAFE_ERROR.to_owned())
    }
}

// ---------------------------------------------------------------------------
// userinfo
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OidcUserInfo {
    pub subject: String,
    pub profile: String,
    pub email: String,
    /// `oidc.UserInfo.EmailVerified` (`bool`, missing = false).
    pub email_verified: bool,
    pub claims: UserClaims,
}

#[derive(Debug, Deserialize)]
struct UserInfoJson {
    #[serde(default)]
    sub: String,
    #[serde(default)]
    profile: String,
    #[serde(default)]
    email: String,
    #[serde(default)]
    email_verified: bool,
}

/// `oidc.Provider.UserInfo`.
pub async fn fetch_user_info(
    client: &dyn OidcHttpClient,
    userinfo_url: &str,
    access_token: &str,
    token_type: &str,
) -> Result<OidcUserInfo, String> {
    if userinfo_url.is_empty() {
        return Err("oidc: user info endpoint is not supported by this provider".to_owned());
    }
    let token_type = if token_type.is_empty() {
        "Bearer"
    } else {
        token_type
    };
    let authorization = format!("{token_type} {access_token}");
    let response = client.get(userinfo_url, Some(&authorization)).await?;
    if response.status != 200 {
        return Err(format!(
            "{}: {}",
            status_line(response.status),
            String::from_utf8_lossy(&response.body)
        ));
    }
    let value: Value = serde_json::from_slice(&response.body)
        .map_err(|error| format!("oidc: failed to decode userinfo: {error}"))?;
    let userinfo: UserInfoJson = serde_json::from_value(value.clone())
        .map_err(|error| format!("oidc: failed to decode userinfo: {error}"))?;
    Ok(OidcUserInfo {
        subject: userinfo.sub,
        profile: userinfo.profile,
        email: userinfo.email,
        email_verified: userinfo.email_verified,
        claims: UserClaims::from_json(&value),
    })
}

// ---------------------------------------------------------------------------
// Resolver + provisioning seam
// ---------------------------------------------------------------------------

/// Input of the user-provisioning seam; same shape as the M9.8jp
/// `user_provisioning::ExternalIdentity` (tasks/39 appendix).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExternalIdentity<'a> {
    pub service_type: &'a str,
    pub service_id: &'a str,
    /// `extsvc.AccountSpec.ClientID` (`p.ClientID`).
    pub client_id: &'a str,
    pub account_id: &'a str,
    pub login: &'a str,
    pub email: &'a str,
    pub email_verified: bool,
    pub display_name: Option<&'a str>,
    /// `UserProps.AvatarURL` (`claims.Picture`).
    pub avatar_url: &'a str,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProvisionOutcome {
    Created { user_id: i32 },
    Existing { user_id: i32 },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProvisionError {
    pub safe_message: String,
    pub message: String,
}

/// `auth.GetAndSaveUser` seam (M9.8jp `user_provisioning`). Implemented by the
/// DB-backed provisioner in production and by a fake in the differential.
#[async_trait]
pub trait Provisioner: Send + Sync {
    async fn get_or_create_user(
        &self,
        identity: &ExternalIdentity<'_>,
    ) -> Result<ProvisionOutcome, ProvisionError>;
}

pub const SUCCESS_EVENT_NAME: &str = "SecurityEventOIDCLoginSucceeded";
pub const FAILURE_EVENT_NAME: &str = "SecurityEventOIDCLoginFailed";

pub const MISCONFIGURED_SAFE: &str = "Misconfigured authentication provider.";
pub const DISCOVERY_SAFE: &str = "Unexpected error creating authentication provider. This may be due to an incorrect issuer URL. Check the logs for more details.";
pub const NO_ID_TOKEN_SAFE: &str = "Authentication failed. Try signing in again. The error was: the issuer's authorization response did not contain an ID token.";
pub const VERIFY_SAFE: &str = "Authentication failed. Try signing in again. The error was: OpenID Connect ID token could not be verified.";
pub const NO_EMAIL_SAFE: &str = "Only users with an email address may authenticate to Sourcegraph.";

pub fn unverified_email_safe(email: &str) -> String {
    format!(
        "Only users with verified email addresses may authenticate to Sourcegraph. The email address {} is not verified on the external authentication provider.",
        go_quote(email)
    )
}

pub fn require_email_domain_safe(domain: &str) -> String {
    format!(
        "Authentication failed. Only users in {} are allowed.",
        go_quote(domain)
    )
}

/// `getLogin` (`middleware.go:55-61`): preferred username, else userinfo email.
pub fn get_login(claims: &UserClaims, userinfo: &OidcUserInfo) -> String {
    if !claims.preferred_username.is_empty() {
        return claims.preferred_username.clone();
    }
    userinfo.email.clone()
}

/// `getDisplayName` (`middleware.go:63-75`): given name, name, else default.
pub fn get_display_name(claims: &UserClaims, default_name: &str) -> String {
    if !claims.given_name.is_empty() {
        return claims.given_name.clone();
    }
    if !claims.name.is_empty() {
        return claims.name.clone();
    }
    default_name.to_owned()
}

/// OIDC provider + `OAuthUserResolver` implementation.
pub struct OidcLoginResolver {
    config: OpenIdConnectAuthProvider,
    http: Arc<dyn OidcHttpClient>,
    provisioner: Arc<dyn Provisioner>,
    fixed_discovery: Option<Vec<u8>>,
    expected_nonce: Option<String>,
    username_prefix: String,
    now_unix: i64,
}

impl OidcLoginResolver {
    pub fn new(
        config: OpenIdConnectAuthProvider,
        http: Arc<dyn OidcHttpClient>,
        provisioner: Arc<dyn Provisioner>,
        now_unix: i64,
    ) -> Self {
        Self {
            config,
            http,
            provisioner,
            fixed_discovery: None,
            expected_nonce: None,
            username_prefix: String::new(),
            now_unix,
        }
    }

    /// Uses a fixed discovery document instead of GET-ing the well-known URL
    /// (`provider_from_document` semantics).
    pub fn with_fixed_discovery(mut self, document: impl Into<Vec<u8>>) -> Self {
        self.fixed_discovery = Some(document.into());
        self
    }

    /// The callback compares the id_token nonce with the state parameter; when
    /// configured here the resolver performs that check (`check_nonce`).
    pub fn with_expected_nonce(mut self, nonce: impl Into<String>) -> Self {
        self.expected_nonce = Some(nonce.into());
        self
    }

    pub fn with_username_prefix(mut self, prefix: impl Into<String>) -> Self {
        self.username_prefix = prefix.into();
        self
    }

    pub fn config(&self) -> &OpenIdConnectAuthProvider {
        &self.config
    }

    /// Runs discovery → id_token verification → nonce → userinfo → email
    /// checks, exposing every intermediate result (the differential records
    /// them; production callers use [`Self::resolve`]).
    pub async fn authorize(
        &self,
        access_token: &str,
        token_type: &str,
        raw_id_token: &str,
    ) -> OidcAuthResult {
        let mut result = OidcAuthResult::default();
        if self.config.issuer.is_empty() {
            result.safe_message = Some(MISCONFIGURED_SAFE.to_owned());
            return result;
        }
        let provider = match &self.fixed_discovery {
            Some(document) => provider_from_document(&self.config.issuer, document),
            None => discover(&*self.http, &self.config.issuer).await,
        };
        let provider = match provider {
            Ok(provider) => provider,
            Err(error) => {
                // `GetProviderAndClient` wraps: `creating authentication
                // provider with ID %q: <discovery error>` where `%q` is applied
                // to `providers.ConfigID{Type, ID}`.
                result.discovery_error = format!(
                    "creating authentication provider with ID {{{} {}}}: {error}",
                    go_quote(&self.config.provider_type),
                    go_quote(&provider_config_id(&self.config))
                );
                result.safe_message = Some(DISCOVERY_SAFE.to_owned());
                return result;
            }
        };
        self.authorize_with_provider(&provider, access_token, token_type, raw_id_token)
            .await
    }

    /// The part of [`Self::authorize`] after discovery. The OIDC middleware has
    /// already created the provider (one discovery request for the whole
    /// callback, like `GetProviderAndClient` + `Verifier` + `UserInfo` in the
    /// baseline), so it calls this directly. Validation order and messages are
    /// identical to [`Self::authorize`].
    pub async fn authorize_with_provider(
        &self,
        provider: &OidcProvider,
        access_token: &str,
        token_type: &str,
        raw_id_token: &str,
    ) -> OidcAuthResult {
        let mut result = OidcAuthResult {
            provider: Some(provider.clone()),
            ..Default::default()
        };

        let id_token = match verify_id_token(
            provider,
            &*self.http,
            raw_id_token,
            &self.config.client_id,
            self.now_unix,
        )
        .await
        {
            Ok(token) => token,
            Err(error) => {
                result.verify_error = error;
                result.safe_message = Some(VERIFY_SAFE.to_owned());
                return result;
            }
        };
        result.id_token = Some(id_token.clone());

        if let Some(expected) = &self.expected_nonce {
            result.nonce_checked = true;
            if id_token.nonce == *expected {
                result.nonce_ok = true;
            } else {
                result.safe_message = Some(NONCE_SAFE_ERROR.to_owned());
                return result;
            }
        }

        let userinfo = match fetch_user_info(
            &*self.http,
            &provider.document.userinfo_endpoint,
            access_token,
            token_type,
        )
        .await
        {
            Ok(userinfo) => userinfo,
            Err(error) => {
                result.safe_message = Some(format!("Failed to get userinfo: {error}"));
                result.userinfo_error = error;
                return result;
            }
        };
        result.userinfo = Some(userinfo.clone());

        if !self.config.require_email_domain.is_empty()
            && !userinfo
                .email
                .ends_with(&format!("@{}", self.config.require_email_domain))
        {
            result.safe_message =
                Some(require_email_domain_safe(&self.config.require_email_domain));
            return result;
        }
        if userinfo.email.is_empty() {
            result.safe_message = Some(NO_EMAIL_SAFE.to_owned());
            return result;
        }
        if userinfo.claims.email_verified == Some(false) {
            result.safe_message = Some(unverified_email_safe(&userinfo.email));
            return result;
        }
        result
    }

    /// Full OIDC claim validation + user resolution.
    pub async fn resolve(
        &self,
        access_token: &str,
        token_type: &str,
        raw_id_token: &str,
    ) -> UserResolution {
        let auth = self.authorize(access_token, token_type, raw_id_token).await;
        if let Some(message) = &auth.safe_message {
            return failed(message);
        }
        let Some(id_token) = auth.id_token else {
            return failed(VERIFY_SAFE);
        };
        let Some(userinfo) = auth.userinfo else {
            return failed(VERIFY_SAFE);
        };

        let mut login = get_login(&userinfo.claims, &userinfo);
        if !self.username_prefix.is_empty() {
            login = format!("{}{login}", self.username_prefix);
        }
        let display_name = get_display_name(&userinfo.claims, &login);
        let identity = ExternalIdentity {
            service_type: &self.config.provider_type,
            service_id: &self.config.issuer,
            client_id: &self.config.client_id,
            account_id: &id_token.subject,
            login: &login,
            email: &userinfo.email,
            email_verified: true,
            display_name: Some(&display_name),
            avatar_url: &userinfo.claims.picture,
        };
        match self.provisioner.get_or_create_user(&identity).await {
            Ok(ProvisionOutcome::Created { user_id }) => UserResolution::Created {
                user_id,
                new_user: true,
            },
            Ok(ProvisionOutcome::Existing { user_id }) => UserResolution::Created {
                user_id,
                new_user: false,
            },
            Err(error) => failed(&error.safe_message),
        }
    }
}

/// Intermediate outcome of [`OidcLoginResolver::authorize`].
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct OidcAuthResult {
    /// Raw discovery error (`""` on success).
    pub discovery_error: String,
    pub provider: Option<OidcProvider>,
    /// Raw id_token verification error (`""` on success).
    pub verify_error: String,
    pub id_token: Option<VerifiedIdToken>,
    pub nonce_checked: bool,
    pub nonce_ok: bool,
    /// Raw userinfo error (`""` on success), also embedded in `safe_message`.
    pub userinfo_error: String,
    pub userinfo: Option<OidcUserInfo>,
    /// First failing stage's `safeErrMsg` (`None` means all claim checks
    /// passed).
    pub safe_message: Option<String>,
}

fn failed(message: &str) -> UserResolution {
    UserResolution::Failed {
        safe_message: message.to_owned(),
    }
}

#[async_trait]
impl OAuthUserResolver for OidcLoginResolver {
    fn service_id(&self) -> &str {
        &self.config.issuer
    }

    fn success_event_name(&self) -> &str {
        SUCCESS_EVENT_NAME
    }

    fn failure_event_name(&self) -> &str {
        FAILURE_EVENT_NAME
    }

    async fn get_or_create_user(&self, token: &OAuthToken) -> UserResolution {
        let raw_id_token = token.extra_str("id_token");
        if raw_id_token.is_empty() {
            return failed(NO_ID_TOKEN_SAFE);
        }
        self.resolve(&token.access_token, &token.token_type, &raw_id_token)
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_json_field_order_and_omitempty_match_go() {
        let config = OpenIdConnectAuthProvider {
            allow_signup: Some(false),
            client_id: "cid".to_owned(),
            client_secret: "secret".to_owned(),
            config_id: String::new(),
            display_name: "IdP & <example>".to_owned(),
            display_prefix: None,
            hidden: false,
            issuer: "https://idp.example".to_owned(),
            no_sign_in: false,
            order: 0,
            require_email_domain: String::new(),
            single_identity_per_user: false,
            provider_type: "openidconnect".to_owned(),
        };
        assert_eq!(
            config_to_go_json(&config),
            "{\"allowSignup\":false,\"clientID\":\"cid\",\"clientSecret\":\"secret\",\"displayName\":\"IdP \\u0026 \\u003cexample\\u003e\",\"issuer\":\"https://idp.example\",\"type\":\"openidconnect\"}"
        );
    }

    #[test]
    fn explicit_config_id_wins() {
        let config = OpenIdConnectAuthProvider {
            config_id: "explicit-id".to_owned(),
            client_id: "cid".to_owned(),
            ..Default::default()
        };
        assert_eq!(provider_config_id(&config), "explicit-id");
    }

    #[test]
    fn hashed_config_id_is_22_char_base64url() {
        let config = OpenIdConnectAuthProvider {
            client_id: "cid".to_owned(),
            client_secret: "secret".to_owned(),
            issuer: "https://idp.example".to_owned(),
            provider_type: "openidconnect".to_owned(),
            ..Default::default()
        };
        let id = provider_config_id(&config);
        assert_eq!(id.len(), 22);
        assert!(id
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_'));
    }

    #[test]
    fn nonce_check_matches_baseline() {
        assert!(check_nonce("s", "s").is_ok());
        assert_eq!(check_nonce("s", "other").unwrap_err(), NONCE_SAFE_ERROR);
    }
}
