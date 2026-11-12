#![forbid(unsafe_code)]

//! `internal/extsvc/auth`: authenticators that mutate an outbound request.
//!
//! The baseline interface takes a `*http.Request`; this port passes the
//! request method and URL plus a mutable header list. Implemented
//! authenticators: basic auth (plain and with SSH keys), OAuth bearer tokens
//! (plain and with SSH keys), OAuth 1 RSA-SHA1/HMAC-SHA1 request signing
//! (`internal/extsvc/auth/oauth1.go` plus `gomodule/oauth1`) and GitHub App
//! JWTs / installation access tokens (`internal/github_apps/auth`).
//!
//! The GitHub App installation token cache and its HTTP transport are
//! injectable ([`InstallationTokenCache`], [`crate::HttpDoer`]) so the
//! differential harness can run offline with fixed keys and recorded
//! responses.

use std::sync::Arc;

use base64::Engine as _;
use sha2::{Digest, Sha256};

/// `auth.Authenticator`.
pub trait Authenticator: Send + Sync {
    /// Mutates the request headers to authenticate it.
    fn authenticate(
        &self,
        method: &str,
        url: &str,
        headers: &mut Vec<(String, String)>,
    ) -> Result<(), String>;

    /// `Hash` uniquely identifies the authenticator for internal caching.
    fn hash(&self) -> String;
}

/// Set a header in place, matching `http.Header.Set` (replace all values).
pub fn set_header(headers: &mut Vec<(String, String)>, name: &str, value: &str) {
    headers.retain(|(existing, _)| !existing.eq_ignore_ascii_case(name));
    headers.push((name.to_owned(), value.to_owned()));
}

/// Add a header, matching `http.Header.Add` (append another value).
pub fn add_header(headers: &mut Vec<(String, String)>, name: &str, value: &str) {
    headers.push((name.to_owned(), value.to_owned()));
}

fn sha256_hex(value: &str) -> String {
    let digest = Sha256::digest(value.as_bytes());
    let mut out = String::with_capacity(digest.len() * 2);
    for byte in digest {
        out.push_str(&format!("{byte:02x}"));
    }
    out
}

/// `auth.BasicAuth`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct BasicAuth {
    pub username: String,
    pub password: String,
}

impl Authenticator for BasicAuth {
    fn authenticate(
        &self,
        _method: &str,
        _url: &str,
        headers: &mut Vec<(String, String)>,
    ) -> Result<(), String> {
        let encoded = base64::engine::general_purpose::STANDARD
            .encode(format!("{}:{}", self.username, self.password));
        set_header(headers, "Authorization", &format!("Basic {encoded}"));
        Ok(())
    }

    fn hash(&self) -> String {
        format!(
            "{}{}",
            sha256_hex(&self.username),
            sha256_hex(&self.password)
        )
    }
}

/// `auth.BasicAuthWithSSH`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct BasicAuthWithSSH {
    pub basic: BasicAuth,
    pub private_key: String,
    pub public_key: String,
    pub passphrase: String,
}

impl BasicAuthWithSSH {
    /// `AuthAuthenticatorWithSSH.SSHPrivateKey`.
    pub fn ssh_private_key(&self) -> (&str, &str) {
        (&self.private_key, &self.passphrase)
    }

    /// `AuthAuthenticatorWithSSH.SSHPublicKey`.
    pub fn ssh_public_key(&self) -> &str {
        &self.public_key
    }
}

impl Authenticator for BasicAuthWithSSH {
    fn authenticate(
        &self,
        method: &str,
        url: &str,
        headers: &mut Vec<(String, String)>,
    ) -> Result<(), String> {
        self.basic.authenticate(method, url, headers)
    }

    fn hash(&self) -> String {
        sha256_hex(&format!(
            "{}{}{}{}{}",
            self.basic.username,
            self.basic.password,
            self.private_key,
            self.passphrase,
            self.public_key
        ))
    }
}

/// `auth.OAuthBearerToken`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct OAuthBearerToken {
    pub token: String,
    pub token_type: String,
    pub refresh_token: String,
    /// RFC 3339 expiry; `None` is Go's zero time.
    pub expiry: Option<time::OffsetDateTime>,
    /// `NeedsRefreshBuffer` minutes.
    pub needs_refresh_buffer: i64,
}

impl OAuthBearerToken {
    /// `OAuthBearerToken.WithToken`: a fresh authenticator carrying only the
    /// new token.
    pub fn with_token(&self, new_token: impl Into<String>) -> Self {
        Self {
            token: new_token.into(),
            ..Self::default()
        }
    }

    /// `OAuthBearerToken.NeedsRefresh`.
    pub fn needs_refresh(&self, now: time::OffsetDateTime) -> bool {
        if self.refresh_token.is_empty() {
            return false;
        }
        match self.expiry {
            Some(expiry) => expiry - now <= time::Duration::minutes(self.needs_refresh_buffer),
            None => false,
        }
    }
}

impl Authenticator for OAuthBearerToken {
    fn authenticate(
        &self,
        _method: &str,
        _url: &str,
        headers: &mut Vec<(String, String)>,
    ) -> Result<(), String> {
        set_header(headers, "Authorization", &format!("Bearer {}", self.token));
        Ok(())
    }

    fn hash(&self) -> String {
        sha256_hex(&self.token)
    }
}

/// `auth.OAuthBearerTokenWithSSH`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct OAuthBearerTokenWithSSH {
    pub token: OAuthBearerToken,
    pub private_key: String,
    pub public_key: String,
    pub passphrase: String,
}

impl OAuthBearerTokenWithSSH {
    pub fn ssh_private_key(&self) -> (&str, &str) {
        (&self.private_key, &self.passphrase)
    }

    pub fn ssh_public_key(&self) -> &str {
        &self.public_key
    }
}

impl Authenticator for OAuthBearerTokenWithSSH {
    fn authenticate(
        &self,
        method: &str,
        url: &str,
        headers: &mut Vec<(String, String)>,
    ) -> Result<(), String> {
        self.token.authenticate(method, url, headers)
    }

    fn hash(&self) -> String {
        sha256_hex(&format!(
            "{}{}{}{}",
            self.token.token, self.private_key, self.passphrase, self.public_key
        ))
    }
}

/// `oauth.SignatureMethod`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum OAuth1SignatureMethod {
    /// `oauth.RSASHA1`; the only method `repos.BitbucketServerSource` uses.
    #[default]
    RsaSha1,
    /// `oauth.HMACSHA1`.
    HmacSha1,
}

impl OAuth1SignatureMethod {
    fn as_str(self) -> &'static str {
        match self {
            Self::RsaSha1 => "RSA-SHA1",
            Self::HmacSha1 => "HMAC-SHA1",
        }
    }
}

/// `auth.OAuthClient` (OAuth 1), a port of `gomodule/oauth1`'s
/// `SetAuthorizationHeader` for RSA-SHA1 and HMAC-SHA1.
///
/// `nonce` and `timestamp` are test/diff overrides; the baseline always uses
/// a process-wide counter and `time.Now()`. When `None`, a random 16-byte
/// nonce and the current Unix time are used.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct OAuthClient {
    /// `oauth.Client.Credentials.Token` (the OAuth consumer key).
    pub consumer_token: String,
    /// `oauth.Client.Credentials.Secret` (unused for RSA-SHA1; part of `Hash`).
    pub consumer_secret: String,
    /// PEM-encoded RSA private key (`oauth.Client.PrivateKey`).
    pub private_key_pem: String,
    /// `oauth.Client.SignatureMethod`.
    pub signature_method: OAuth1SignatureMethod,
    /// Request token credentials (`oauth.Credentials` passed to
    /// `SetAuthorizationHeader`). Bitbucket Server passes an empty token.
    pub token: String,
    pub token_secret: String,
    /// Test/diff override for `oauth_nonce`.
    pub nonce: Option<String>,
    /// Test/diff override for `oauth_timestamp` (Unix seconds).
    pub timestamp: Option<i64>,
}

/// OAuth request parameters in the order the baseline emits them in the
/// `Authorization` header (`oauthKeys`).
const OAUTH_HEADER_KEYS: [&str; 10] = [
    "oauth_consumer_key",
    "oauth_nonce",
    "oauth_signature",
    "oauth_signature_method",
    "oauth_timestamp",
    "oauth_token",
    "oauth_version",
    "oauth_callback",
    "oauth_verifier",
    "oauth_session_handle",
];

/// Section 3.6 percent-encoding: everything outside the unreserved set is
/// escaped with uppercase hex. `double` encodes `%` as `%25` first, matching
/// the baseline's single-step double encoding.
fn oauth_encode(value: &str, double: bool) -> String {
    const HEX: &[u8; 16] = b"0123456789ABCDEF";
    let mut out = String::with_capacity(value.len());
    for &byte in value.as_bytes() {
        let unreserved = byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'.' | b'_' | b'~');
        if unreserved {
            out.push(byte as char);
        } else if double {
            out.push_str("%25");
            out.push(HEX[usize::from(byte >> 4)] as char);
            out.push(HEX[usize::from(byte & 0xf)] as char);
        } else {
            out.push('%');
            out.push(HEX[usize::from(byte >> 4)] as char);
            out.push(HEX[usize::from(byte & 0xf)] as char);
        }
    }
    out
}

impl OAuthClient {
    /// `bitbucketserver.Client.SetOAuth`: `signingKey` is a base64-encoded
    /// PEM PKCS#1 RSA private key; the constructor validates it.
    pub fn new_rsa_sha1(
        consumer_key: impl Into<String>,
        signing_key_base64: &str,
    ) -> Result<Self, String> {
        use rsa::pkcs1::DecodeRsaPrivateKey;
        let decoded = base64::engine::general_purpose::STANDARD
            .decode(signing_key_base64)
            .map_err(|err| err.to_string())?;
        let pem = String::from_utf8(decoded)
            .map_err(|_| "failed to parse PEM block containing the signing key".to_owned())?;
        rsa::RsaPrivateKey::from_pkcs1_pem(pem.trim())
            .map_err(|_| "failed to parse PEM block containing the signing key".to_owned())?;
        Ok(Self {
            consumer_token: consumer_key.into(),
            private_key_pem: pem,
            signature_method: OAuth1SignatureMethod::RsaSha1,
            ..Self::default()
        })
    }

    /// `oauth.writeBaseString` with no form parameters.
    pub fn signature_base_string(
        &self,
        method: &str,
        url: &str,
        nonce: &str,
        timestamp: i64,
    ) -> Result<String, String> {
        let parsed = url::Url::parse(url).map_err(|err| err.to_string())?;
        let scheme = parsed.scheme().to_ascii_lowercase();
        let mut host = parsed.host_str().unwrap_or_default().to_ascii_lowercase();
        if let Some(port) = parsed.port() {
            host.push(':');
            host.push_str(&port.to_string());
        }
        match (scheme.as_str(), host.as_str()) {
            ("http", h) if h.ends_with(":80") => host.truncate(host.len() - 3),
            ("https", h) if h.ends_with(":443") => host.truncate(host.len() - 4),
            _ => {}
        }
        // `uNoQuery.RequestURI()`: the escaped path (no query).
        let path = parsed.path();

        let mut params: Vec<(String, String)> = Vec::new();
        for (key, value) in parsed.query_pairs() {
            params.push((oauth_encode(&key, true), oauth_encode(&value, true)));
        }
        let mut oauth_params = vec![
            ("oauth_consumer_key", self.consumer_token.clone()),
            ("oauth_signature_method", self.signature_method.as_str().to_owned()),
            ("oauth_version", "1.0".to_owned()),
            ("oauth_timestamp", timestamp.to_string()),
            ("oauth_nonce", nonce.to_owned()),
        ];
        // `oauthParams` always sets `oauth_token` when request credentials are
        // supplied, even for an empty token.
        oauth_params.push(("oauth_token", self.token.clone()));
        for (key, value) in oauth_params {
            params.push((oauth_encode(key, true), oauth_encode(&value, true)));
        }
        params.sort();

        let mut out = String::new();
        out.push_str(&oauth_encode(&method.to_ascii_uppercase(), false));
        out.push('&');
        out.push_str(&oauth_encode(&scheme, false));
        out.push_str(&oauth_encode("://", false));
        out.push_str(&oauth_encode(&host, false));
        out.push_str(&oauth_encode(path, false));
        out.push('&');
        for (index, (key, value)) in params.iter().enumerate() {
            if index > 0 {
                out.push_str(&oauth_encode("&", false));
            }
            out.push_str(key);
            out.push_str(&oauth_encode("=", false));
            out.push_str(value);
        }
        Ok(out)
    }

    fn generate_nonce() -> Result<String, String> {
        let mut bytes = [0u8; 16];
        getrandom_fill(&mut bytes)?;
        let mut out = String::with_capacity(32);
        for byte in bytes {
            out.push_str(&format!("{byte:02x}"));
        }
        Ok(out)
    }

    /// `oauth.Client.authorizationHeader`: the `Authorization` header value.
    pub fn authorization_header(
        &self,
        method: &str,
        url: &str,
        nonce: &str,
        timestamp: i64,
    ) -> Result<String, String> {
        let base = self.signature_base_string(method, url, nonce, timestamp)?;
        let signature = match self.signature_method {
            OAuth1SignatureMethod::RsaSha1 => rsa_sha1_signature(&self.private_key_pem, &base)?,
            OAuth1SignatureMethod::HmacSha1 => {
                hmac_sha1_signature(&self.consumer_secret, &self.token_secret, &base)
            }
        };
        let params: Vec<(&str, String)> = vec![
            ("oauth_consumer_key", self.consumer_token.clone()),
            ("oauth_nonce", nonce.to_owned()),
            ("oauth_signature", signature),
            (
                "oauth_signature_method",
                self.signature_method.as_str().to_owned(),
            ),
            ("oauth_timestamp", timestamp.to_string()),
            ("oauth_version", "1.0".to_owned()),
            ("oauth_token", self.token.clone()),
        ];
        let mut header = String::from("OAuth ");
        let mut first = true;
        for key in OAUTH_HEADER_KEYS {
            let Some((_, value)) = params.iter().find(|(name, _)| *name == key) else {
                continue;
            };
            if !first {
                header.push_str(", ");
            }
            first = false;
            header.push_str(key);
            header.push_str("=\"");
            header.push_str(&oauth_encode(value, false));
            header.push('"');
        }
        Ok(header)
    }
}

fn getrandom_fill(bytes: &mut [u8]) -> Result<(), String> {
    use std::io::Read;
    std::fs::File::open("/dev/urandom")
        .and_then(|mut file| file.read_exact(bytes))
        .map_err(|err| format!("failed to read random bytes: {err}"))
}

/// Parse a PEM RSA private key, accepting PKCS#1 (`RSA PRIVATE KEY`) and
/// PKCS#8 (`PRIVATE KEY`), like `jwt.ParseRSAPrivateKeyFromPEM`.
fn parse_rsa_private_key(private_key_pem: &str) -> Result<rsa::RsaPrivateKey, String> {
    use rsa::pkcs1::DecodeRsaPrivateKey;
    use rsa::pkcs8::DecodePrivateKey;

    let trimmed = private_key_pem.trim();
    rsa::RsaPrivateKey::from_pkcs1_pem(trimmed)
        .or_else(|_| rsa::RsaPrivateKey::from_pkcs8_pem(trimmed))
        .map_err(|err| format!("parse private key: {err}"))
}

fn rsa_sha1_signature(private_key_pem: &str, message: &str) -> Result<String, String> {
    use rsa::pkcs1v15::SigningKey;
    use rsa::signature::{SignatureEncoding, Signer};
    use sha1::Sha1;

    let key = parse_rsa_private_key(private_key_pem)?;
    let signing_key = SigningKey::<Sha1>::new(key);
    let signature = signing_key.sign(message.as_bytes());
    Ok(base64::engine::general_purpose::STANDARD.encode(signature.to_bytes()))
}

fn hmac_sha1_signature(secret: &str, token_secret: &str, message: &str) -> String {
    use hmac::Mac;
    let key = format!(
        "{}&{}",
        oauth_encode(secret, false),
        oauth_encode(token_secret, false)
    );
    let mut mac = hmac::Hmac::<sha1::Sha1>::new_from_slice(key.as_bytes())
        .expect("HMAC accepts any key length");
    mac.update(message.as_bytes());
    base64::engine::general_purpose::STANDARD.encode(mac.finalize().into_bytes())
}

impl OAuthClient {
    /// `oauth.OAuthClient.Authenticate`: signs the request URL with empty
    /// request credentials, like the baseline.
    pub fn sign_request(
        &self,
        method: &str,
        url: &str,
    ) -> Result<String, String> {
        let nonce = match &self.nonce {
            Some(nonce) => nonce.clone(),
            None => Self::generate_nonce()?,
        };
        let timestamp = self.timestamp.unwrap_or_else(|| {
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|duration| duration.as_secs() as i64)
                .unwrap_or_default()
        });
        self.authorization_header(method, url, &nonce, timestamp)
    }
}

impl Authenticator for OAuthClient {
    fn authenticate(
        &self,
        method: &str,
        url: &str,
        headers: &mut Vec<(String, String)>,
    ) -> Result<(), String> {
        let header = self.sign_request(method, url)?;
        set_header(headers, "Authorization", &header);
        Ok(())
    }

    fn hash(&self) -> String {
        format!(
            "{}{}",
            sha256_hex(&self.consumer_secret),
            sha256_hex(&self.consumer_token)
        )
    }
}

/// Credentials for one GitHub App, as returned by `github_apps.GetByAppID`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct GitHubAppCredentials {
    /// The GitHub App ID (JWT `iss`).
    pub app_id: i32,
    /// PEM-encoded RSA private key.
    pub private_key: String,
}

/// `github_apps.Store.GetByAppID`: the injectable boundary the GitHub source
/// uses to resolve `gitHubAppDetails.appID` to a private key. The Rust db
/// store does not implement this query yet, so production wiring supplies an
/// adapter (M9.16c/17); sources without one reject App configs like before.
#[async_trait::async_trait]
pub trait GitHubAppStore: Send + Sync {
    async fn get_by_app_id(
        &self,
        app_id: i32,
        base_url: &str,
    ) -> Result<GitHubAppCredentials, String>;
}

/// `rcache.Cache` for installation access tokens. Values are the JSON-encoded
/// `installationAccessToken`; encryption is the implementation's concern (the
/// baseline passes the site keyring's `GitHubAppKey`, and a `None` key means
/// plaintext).
pub trait InstallationTokenCache: Send + Sync {
    fn get(&self, key: &str) -> Option<Vec<u8>>;
    fn set(&self, key: &str, value: Vec<u8>);
}

/// `github_apps/auth.GitHubAppAuthenticator`: signs requests as the App with
/// a short-lived RS256 JWT.
#[derive(Debug, Clone)]
pub struct GitHubAppAuthenticator {
    app_id: i32,
    private_key_pem: String,
}

impl GitHubAppAuthenticator {
    /// `github_apps/auth.NewGitHubAppAuthenticator`. Validates the key up
    /// front, like the baseline.
    pub fn new(app_id: i32, private_key_pem: impl Into<String>) -> Result<Self, String> {
        let private_key_pem = private_key_pem.into();
        // Parse once so an invalid key fails at construction.
        parse_rsa_private_key(&private_key_pem)?;
        Ok(Self {
            app_id,
            private_key_pem,
        })
    }

    /// `GitHubAppAuthenticator.generateJWT` at a fixed instant (Unix seconds).
    /// `iat` is truncated to whole seconds and backdated one minute; `exp` is
    /// ten minutes after `iat`.
    pub fn generate_jwt_at(&self, now_unix: i64) -> Result<String, String> {
        let iat = now_unix - 60;
        // `jwt.RegisteredClaims` marshals struct fields in declaration order:
        // iss, sub, aud, exp, nbf, iat, jti; empty omitempty fields are absent.
        // Only iss/exp/iat are set, so emit exactly that byte sequence.
        let claims = format!(
            "{{\"iss\":\"{}\",\"exp\":{},\"iat\":{}}}",
            self.app_id,
            iat + 600,
            iat
        );
        let header = serde_json::json!({"typ": "JWT", "alg": "RS256"});
        let encoding = base64::engine::general_purpose::URL_SAFE_NO_PAD;
        let signing_input = format!(
            "{}.{}",
            encoding.encode(header.to_string()),
            encoding.encode(claims)
        );

        use rsa::pkcs1v15::SigningKey;
        use rsa::signature::{SignatureEncoding, Signer};
        use sha2::Sha256;
        let key = parse_rsa_private_key(&self.private_key_pem)?;
        let signature = SigningKey::<Sha256>::new(key).sign(signing_input.as_bytes());
        Ok(format!("{signing_input}.{}", encoding.encode(signature.to_vec())))
    }

    /// `generateJWT` using the current time.
    pub fn generate_jwt(&self) -> Result<String, String> {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|duration| duration.as_secs() as i64)
            .unwrap_or_default();
        self.generate_jwt_at(now)
    }
}

impl Authenticator for GitHubAppAuthenticator {
    fn authenticate(
        &self,
        _method: &str,
        _url: &str,
        headers: &mut Vec<(String, String)>,
    ) -> Result<(), String> {
        let token = self.generate_jwt()?;
        set_header(headers, "Authorization", &format!("Bearer {token}"));
        Ok(())
    }

    fn hash(&self) -> String {
        sha256_hex(&self.private_key_pem)
    }
}

/// `github_apps/auth.InstallationAccessToken`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct InstallationAccessToken {
    pub token: String,
    /// RFC 3339 expiry; `None` is Go's zero time.
    pub expires_at: Option<time::OffsetDateTime>,
}

/// `github_apps/auth.InstallationAuthenticator`: authenticates with an
/// installation access token, refreshing it through the App-authenticated
/// `POST /app/installations/{id}/access_tokens` endpoint.
pub struct InstallationAuthenticator {
    installation_id: i32,
    token: InstallationAccessToken,
    base_url: url::Url,
    app: Arc<GitHubAppAuthenticator>,
    cache: Option<Arc<dyn InstallationTokenCache>>,
    doer: Arc<dyn crate::HttpDoer>,
    now: Box<dyn Fn() -> time::OffsetDateTime + Send + Sync>,
}

impl std::fmt::Debug for InstallationAuthenticator {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("InstallationAuthenticator")
            .field("installation_id", &self.installation_id)
            .field("base_url", &self.base_url.as_str())
            .finish_non_exhaustive()
    }
}

impl InstallationAuthenticator {
    /// `github_apps/auth.NewInstallationAccessToken`. The baseline always
    /// installs a Redis-backed cache; here the cache is optional so tests can
    /// run without one.
    pub fn new(
        base_url: url::Url,
        installation_id: i32,
        app: Arc<GitHubAppAuthenticator>,
        cache: Option<Arc<dyn InstallationTokenCache>>,
        doer: Arc<dyn crate::HttpDoer>,
    ) -> Self {
        Self {
            installation_id,
            token: InstallationAccessToken::default(),
            base_url,
            app,
            cache,
            doer,
            now: Box::new(time::OffsetDateTime::now_utc),
        }
    }

    /// Test hook for `time.Now()`.
    pub fn with_clock(
        mut self,
        now: impl Fn() -> time::OffsetDateTime + Send + Sync + 'static,
    ) -> Self {
        self.now = Box::new(now);
        self
    }

    fn cache_key(&self) -> String {
        format!(
            "{}{}",
            self.base_url,
            self.installation_id
        )
    }

    fn get_from_cache(&self) -> Option<InstallationAccessToken> {
        let cache = self.cache.as_ref()?;
        let raw = cache.get(&self.cache_key())?;
        serde_json::from_slice::<serde_json::Value>(&raw).ok().and_then(|value| {
            let token = value.get("token")?.as_str()?.to_owned();
            let expires_at = value
                .get("expires_at")
                .and_then(|value| value.as_str())
                .and_then(|value| {
                    time::OffsetDateTime::parse(value, &time::format_description::well_known::Rfc3339).ok()
                });
            Some(InstallationAccessToken { token, expires_at })
        })
    }

    fn store_in_cache(&self) {
        let Some(cache) = self.cache.as_ref() else {
            return;
        };
        let expires_at = self
            .token
            .expires_at
            .map(|value| {
                value
                    .format(&time::format_description::well_known::Rfc3339)
                    .unwrap_or_default()
            })
            .unwrap_or_default();
        let value = serde_json::json!({
            "token": self.token.token,
            "expires_at": expires_at,
        });
        if let Ok(bytes) = serde_json::to_vec(&value) {
            cache.set(&self.cache_key(), bytes);
        }
    }

    /// `InstallationAuthenticator.NeedsRefresh`. An empty token always needs
    /// a refresh; a zero expiry never does; otherwise refresh within five
    /// minutes of expiry.
    pub fn needs_refresh_at(&self, now: time::OffsetDateTime) -> bool {
        if self.token.token.is_empty() {
            return true;
        }
        match self.token.expires_at {
            None => false,
            Some(expires_at) => expires_at - now < time::Duration::minutes(5),
        }
    }

    /// `NeedsRefresh` at the current time.
    pub fn needs_refresh(&self) -> bool {
        self.needs_refresh_at((self.now)())
    }

    /// `InstallationAuthenticator.Refresh`. A cache hit for a different token
    /// is adopted before contacting GitHub; only a token that still needs a
    /// refresh triggers the HTTP call.
    pub async fn refresh(&mut self) -> Result<(), String> {
        let now = (self.now)();
        if let Some(cached) = self.get_from_cache() {
            if cached.token != self.token.token {
                let previous = self.token.clone();
                self.token = cached;
                if !self.needs_refresh_at(now) {
                    return Ok(());
                }
                // The cached token is stale; restore so we can report the
                // fresh response, matching the baseline's flow.
                self.token = previous;
            }
        }

        let (mut api_url, _) = crate::github::api_root(&self.base_url);
        let path = format!("/app/installations/{}/access_tokens", self.installation_id);
        api_url = api_url
            .join(&path)
            .map_err(|err| format!("invalid API URL: {err}"))?;

        let mut headers = Vec::new();
        self.app
            .authenticate("POST", api_url.as_str(), &mut headers)?;
        let request = crate::HttpRequest {
            method: "POST".to_owned(),
            url: api_url.to_string(),
            headers,
            body: Vec::new(),
        };
        let response = self.doer.do_request(request).await?;
        if response.status != 201 {
            return Err(format!(
                "failed to refresh the access token: {}",
                response.status
            ));
        }
        let value: serde_json::Value = serde_json::from_slice(&response.body)
            .map_err(|err| err.to_string())?;
        let token = value
            .get("token")
            .and_then(|value| value.as_str())
            .unwrap_or_default()
            .to_owned();
        let expires_at = value
            .get("expires_at")
            .and_then(|value| value.as_str())
            .and_then(|value| {
                time::OffsetDateTime::parse(value, &time::format_description::well_known::Rfc3339).ok()
            });
        self.token = InstallationAccessToken { token, expires_at };
        self.store_in_cache();
        Ok(())
    }

    /// `InstallationAuthenticator.SetURLUser`.
    pub fn set_url_user(&self, url: &mut url::Url) {
        let _ = url.set_username("x-access-token");
        let _ = url.set_password(Some(&self.token.token));
    }

    /// `InstallationAuthenticator.GetToken`.
    pub fn get_token(&self) -> InstallationAccessToken {
        self.token.clone()
    }

    /// `InstallationAuthenticator.InstallationID`.
    pub fn installation_id(&self) -> i32 {
        self.installation_id
    }

    /// Test hook: set the cached token directly.
    pub fn set_token(&mut self, token: InstallationAccessToken) {
        self.token = token;
    }
}

impl Authenticator for InstallationAuthenticator {
    fn authenticate(
        &self,
        _method: &str,
        _url: &str,
        headers: &mut Vec<(String, String)>,
    ) -> Result<(), String> {
        set_header(
            headers,
            "Authorization",
            &format!("Bearer {}", self.token.token),
        );
        Ok(())
    }

    fn hash(&self) -> String {
        // The installation ID, not the short-lived token.
        sha256_hex(&self.installation_id.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn basic_auth_sets_the_header_and_hashes_both_fields() {
        let auth = BasicAuth {
            username: "user".to_owned(),
            password: "pass".to_owned(),
        };
        let mut headers = Vec::new();
        auth.authenticate("GET", "https://example.com", &mut headers)
            .expect("authenticate");
        assert_eq!(
            headers,
            vec![("Authorization".to_owned(), "Basic dXNlcjpwYXNz".to_owned())]
        );
        assert_eq!(auth.hash().len(), 128);
        assert_ne!(auth.hash(), BasicAuth::default().hash());
    }

    #[test]
    fn oauth_bearer_sets_bearer_token() {
        let auth = OAuthBearerToken {
            token: "abc".to_owned(),
            ..OAuthBearerToken::default()
        };
        let mut headers = Vec::new();
        auth.authenticate("POST", "https://example.com", &mut headers)
            .expect("authenticate");
        assert_eq!(
            headers,
            vec![("Authorization".to_owned(), "Bearer abc".to_owned())]
        );
        assert_eq!(auth.hash(), sha256_hex("abc"));
    }

    #[test]
    fn empty_token_still_sets_the_bearer_header() {
        let auth = OAuthBearerToken::default();
        let mut headers = Vec::new();
        auth.authenticate("GET", "https://example.com", &mut headers)
            .expect("authenticate");
        assert_eq!(
            headers,
            vec![("Authorization".to_owned(), "Bearer ".to_owned())]
        );
    }

    #[test]
    fn needs_refresh_requires_a_refresh_token_and_a_nonzero_expiry() {
        let now = time::OffsetDateTime::now_utc();
        let mut auth = OAuthBearerToken::default();
        assert!(!auth.needs_refresh(now));
        auth.refresh_token = "refresh".to_owned();
        assert!(!auth.needs_refresh(now));
        auth.expiry = Some(now + time::Duration::minutes(5));
        auth.needs_refresh_buffer = 10;
        assert!(auth.needs_refresh(now));
        auth.needs_refresh_buffer = 1;
        assert!(!auth.needs_refresh(now));
    }

    #[test]
    fn with_token_only_keeps_the_new_token() {
        let auth = OAuthBearerToken {
            token: "old".to_owned(),
            refresh_token: "refresh".to_owned(),
            ..OAuthBearerToken::default()
        };
        let next = auth.with_token("new");
        assert_eq!(next.token, "new");
        assert!(next.refresh_token.is_empty());
    }

    #[test]
    fn ssh_hashes_include_the_key_material() {
        let auth = BasicAuthWithSSH {
            basic: BasicAuth {
                username: "u".to_owned(),
                password: "p".to_owned(),
            },
            private_key: "priv".to_owned(),
            public_key: "pub".to_owned(),
            passphrase: "phrase".to_owned(),
        };
        assert_eq!(auth.hash().len(), 64);
        assert_eq!(auth.ssh_private_key(), ("priv", "phrase"));
        assert_eq!(auth.ssh_public_key(), "pub");
    }

    /// Random valid private key from the baseline's
    /// `internal/github_apps/auth/auth_test.go`; used for signing tests only.
    const TEST_PRIVATE_KEY: &str = "-----BEGIN RSA PRIVATE KEY-----
MIIEpAIBAAKCAQEA1LqMqnchtoTHiRfFds2RWWji43R5mHT65WZpZXpBuBwsgSWr
rN5VwTHZ4dxWk+XlyVDYsri7vlVWNX4EIt0jwxvh/OBXCFJXTL+byNHCimRIKvur
ofoT1eF3z+5WpH5ddHNPOkGZV0Chyd5kvUcNuFA7q203HRVEOloHEs4fqJrGPHIF
zc8Sug5qkOtZTS5xiHgTtmZkDLuLZ26H5Gfx3zZk5Gv2Jy/+fLsGninaiwTRsZf6
RgPgmdlkuM8OhSm4GtlpzoK0D3iZEhf4pITo1CK2U4Cs7vzkU0UkQ+J/z6dDmVBJ
gkalH1SHsboRqjNxkStEqGnbWwdtal01skbGOwIDAQABAoIBAQCls54oll17V5g5
0Htu3BdxBsNdG3gv6kcY85n7gqy4ZbHA83/zSsiPkW4/gasqzzQbiU8Sf9U2IDDj
wAImygy2SPzSRklk4QbBcKs/VSztMcoJOTprFGno+xShsexpe0j+kWdQYJK6JU0g
+ouL6FHmlRC1qn/4tn0L2t6Rpl+Aq4peDLqdwFHXj8GxGv0S10qMQ4/ER7onP6f0
99WDTvNQR5DugKqHxooOV5HfUP70scqhCcFhp2zc7/aYQFVt/k4hDOMu/w4HhkD3
r34y4EJoZsugGD1kPaJCw2rbSdoTtQHCqG5tfidY+XUIoC9mfmN8243jeRrhayeT
4ewiDuNhAoGBAPszeqN/+V8EVrlbBiBG+xVYGzWU0KgHu1TUiIrOSmKa6rTwaYMb
dKI8N4gYwFtb24AeDLfGnpaZAKSvNnrf8OapyLik7zXDylY0DBU7tRxQiUvNsVTs
7CYjxih5GWzUeP/xgpfVbHIIGdTHaZ6JWiDHWOolAw3hQyw6V/uQTDtxAoGBANjK
6vlctX55MjE2tuPk3ZCtCjgDFmWQjvFuiYYE/2cP4v4UBqgZn1vOaLRCnFm10ycl
peBLxPVpeeNBWc2ep2YNnJ+hm+SavhIDesLJTxuhC4wtcKMVAtq83VQmMQTU5wRO
KcUpviXLv2Z0UfbMWcohR4fJY1SkREwaxneHZc5rAoGBAIpT8c/BNBhPslYFutzh
WXiKeQlLdo9hGpZ/JuWQ7cNY3bBfxyqMXvDLyiSmxJ5KehgV9BjrRf9WJ9WIKq8F
TIooqsCLCrMHqw9HP/QdWgFKlCBrF6DVisEB6Cf3b7nPUwZV/v0PaNVugpL6cL39
kuUEAYGGeiUVi8D6K+L6tg/xAoGATlQqyAQ+Mz8Y6n0pYXfssfxDh+9dpT6w1vyo
RbsCiLtNuZ2EtjHjySjv3cl/ck5mx2sr3rmhpUYB2yFekBN1ykK6x1Z93AApEpsd
PMm9gm8SnAhC/Tl3OY8prODLr0I5Ye3X27v0TvWp5xu6DaDSBF032hDiic98Ob8m
3EMYfpcCgYBySPGnPmwqimiSyZRn+gJh+cZRF1aOKBqdqsfdcQrNpaZuZuQ4aYLo
cEoKFPr8HjXXUVCa3Q84tf9nGb4iUFslRSbS6RCP6Nm+JsfbCTtzyglYuPRKITGm
jSzka5UER3Dj1lSLMk9DkU+jrBxUsFeeiQOYhzQBaZxguvwYRIYHpg==
-----END RSA PRIVATE KEY-----";

    #[test]
    fn oauth1_base_string_matches_the_oauth1_reference_vector() {
        let client = OAuthClient {
            consumer_token: "abcd".to_owned(),
            consumer_secret: "efgh".to_owned(),
            token: "ijkl".to_owned(),
            token_secret: "mnop".to_owned(),
            signature_method: OAuth1SignatureMethod::HmacSha1,
            ..OAuthClient::default()
        };
        // `gomodule/oauth1`'s "query string in URL" case, minus its form
        // parameter (the baseline authenticator signs an empty form).
        let base = client
            .signature_base_string(
                "GET",
                "http://EXAMPLE.COM:80/Space%20Craft?name=value",
                "Ix4U1Ei3RFL",
                1327384901,
            )
            .expect("base string");
        assert_eq!(
            base,
            "GET&http%3A%2F%2Fexample.com%2FSpace%2520Craft&name%3Dvalue%26oauth_consumer_key%3Dabcd%26oauth_nonce%3DIx4U1Ei3RFL%26oauth_signature_method%3DHMAC-SHA1%26oauth_timestamp%3D1327384901%26oauth_token%3Dijkl%26oauth_version%3D1.0"
        );
    }

    #[test]
    fn oauth1_rsa_sha1_header_is_signed_and_ordered() {
        let client = OAuthClient {
            nonce: Some("nonce".to_owned()),
            timestamp: Some(1700000000),
            ..oauth1_rsa_client("c")
        };
        let header = client
            .authorization_header("GET", "https://bitbucket.example.com/rest/api/1.0/repos", "nonce", 1700000000)
            .expect("header");
        assert!(header.starts_with(
            "OAuth oauth_consumer_key=\"c\", oauth_nonce=\"nonce\", oauth_signature=\""
        ));
        assert!(header.contains("oauth_signature_method=\"RSA-SHA1\""));
        // `oauthParams` always includes `oauth_token`, even when empty.
        assert!(header.ends_with(
            "oauth_timestamp=\"1700000000\", oauth_token=\"\", oauth_version=\"1.0\""
        ));
        // The signature verifies against the key's public half.
        let signature_b64 = header
            .split("oauth_signature=\"")
            .nth(1)
            .unwrap()
            .split('"')
            .next()
            .unwrap()
            .replace("%2B", "+")
            .replace("%2F", "/")
            .replace("%3D", "=");
        let signature = base64::engine::general_purpose::STANDARD
            .decode(signature_b64)
            .expect("base64");
        let base = client
            .signature_base_string("GET", "https://bitbucket.example.com/rest/api/1.0/repos", "nonce", 1700000000)
            .expect("base");
        use rsa::pkcs1::DecodeRsaPrivateKey;
        use rsa::pkcs1v15::VerifyingKey;
        use rsa::signature::Verifier;
        use sha1::Sha1;
        let private = rsa::RsaPrivateKey::from_pkcs1_pem(TEST_PRIVATE_KEY.trim()).unwrap();
        let verifying = VerifyingKey::<Sha1>::new(private.to_public_key());
        let signature = rsa::pkcs1v15::Signature::try_from(signature.as_slice()).unwrap();
        verifying.verify(base.as_bytes(), &signature).expect("valid signature");
    }

    fn oauth1_rsa_client(consumer_key: &str) -> OAuthClient {
        let signing_key = base64::engine::general_purpose::STANDARD.encode(TEST_PRIVATE_KEY);
        OAuthClient::new_rsa_sha1(consumer_key, &signing_key).expect("oauth1 client")
    }

    #[test]
    fn oauth1_hash_hashes_consumer_secret_and_token() {
        let client = OAuthClient {
            consumer_token: "token".to_owned(),
            consumer_secret: "secret".to_owned(),
            ..OAuthClient::default()
        };
        assert_eq!(
            client.hash(),
            format!("{}{}", sha256_hex("secret"), sha256_hex("token"))
        );
    }

    #[test]
    fn github_app_jwt_has_truncated_claims() {
        let app = GitHubAppAuthenticator::new(1234, TEST_PRIVATE_KEY).expect("app");
        let jwt = app.generate_jwt_at(1_700_000_000).expect("jwt");
        let parts: Vec<&str> = jwt.split('.').collect();
        assert_eq!(parts.len(), 3);
        let decode = |part: &str| {
            let padded = match part.len() % 4 {
                0 => part.to_owned(),
                rest => format!("{part}{}", "=".repeat(4 - rest)),
            };
            base64::engine::general_purpose::URL_SAFE_NO_PAD
                .decode(part)
                .or_else(|_| base64::engine::general_purpose::STANDARD.decode(padded))
                .expect("jwt part")
        };
        let header: serde_json::Value = serde_json::from_slice(&decode(parts[0])).unwrap();
        let claims: serde_json::Value = serde_json::from_slice(&decode(parts[1])).unwrap();
        assert_eq!(header["alg"], "RS256");
        assert_eq!(header["typ"], "JWT");
        assert_eq!(claims["iss"], "1234");
        assert_eq!(claims["iat"], 1_699_999_940);
        assert_eq!(claims["exp"], 1_700_000_540);
        assert_eq!(app.hash(), sha256_hex(TEST_PRIVATE_KEY));
    }

    #[test]
    fn github_app_rejects_an_invalid_key() {
        assert!(GitHubAppAuthenticator::new(1, "not a key").is_err());
    }

    struct MemoryCache(std::sync::Mutex<std::collections::HashMap<String, Vec<u8>>>);

    impl InstallationTokenCache for MemoryCache {
        fn get(&self, key: &str) -> Option<Vec<u8>> {
            self.0.lock().unwrap().get(key).cloned()
        }
        fn set(&self, key: &str, value: Vec<u8>) {
            self.0.lock().unwrap().insert(key.to_owned(), value);
        }
    }

    struct FakeDoer {
        status: u16,
        body: Vec<u8>,
        requests: std::sync::Mutex<Vec<crate::HttpRequest>>,
    }

    #[async_trait::async_trait]
    impl crate::HttpDoer for FakeDoer {
        async fn do_request(
            &self,
            request: crate::HttpRequest,
        ) -> Result<crate::HttpResponse, String> {
            self.requests.lock().unwrap().push(request);
            Ok(crate::HttpResponse {
                status: self.status,
                headers: Vec::new(),
                body: self.body.clone(),
            })
        }
    }

    #[tokio::test]
    async fn installation_refresh_posts_and_stores_the_token() {
        let app = Arc::new(GitHubAppAuthenticator::new(1234, TEST_PRIVATE_KEY).unwrap());
        let cache = Arc::new(MemoryCache(std::sync::Mutex::new(
            std::collections::HashMap::new(),
        )));
        let doer = Arc::new(FakeDoer {
            status: 201,
            body: br#"{"token":"installation-token","expires_at":"2023-11-14T22:23:20Z"}"#.to_vec(),
            requests: std::sync::Mutex::new(Vec::new()),
        });
        let mut auth = InstallationAuthenticator::new(
            url::Url::parse("https://github.com").unwrap(),
            7,
            app,
            Some(cache.clone()),
            doer.clone(),
        );
        assert!(auth.needs_refresh());
        auth.refresh().await.expect("refresh");
        assert_eq!(auth.get_token().token, "installation-token");
        assert!(!auth.needs_refresh_at(
            time::OffsetDateTime::parse(
                "2023-11-14T22:17:00Z",
                &time::format_description::well_known::Rfc3339
            )
            .unwrap()
        ));
        assert!(auth.needs_refresh_at(
            time::OffsetDateTime::parse(
                "2023-11-14T22:19:00Z",
                &time::format_description::well_known::Rfc3339
            )
            .unwrap()
        ));
        let requests = doer.requests.lock().unwrap();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].method, "POST");
        assert_eq!(
            requests[0].url,
            "https://api.github.com/app/installations/7/access_tokens"
        );
        assert!(requests[0]
            .headers
            .iter()
            .any(|(name, value)| name == "Authorization" && value.starts_with("Bearer ")));
        // The cache holds the same token under the base URL + ID key.
        assert!(cache
            .get("https://github.com/7")
            .is_some_and(|raw| String::from_utf8_lossy(&raw).contains("installation-token")));
    }

    #[tokio::test]
    async fn installation_refresh_requires_created_status() {
        let app = Arc::new(GitHubAppAuthenticator::new(1234, TEST_PRIVATE_KEY).unwrap());
        let doer = Arc::new(FakeDoer {
            status: 401,
            body: Vec::new(),
            requests: std::sync::Mutex::new(Vec::new()),
        });
        let mut auth = InstallationAuthenticator::new(
            url::Url::parse("https://github.com").unwrap(),
            7,
            app,
            None,
            doer,
        );
        assert_eq!(
            auth.refresh().await.unwrap_err(),
            "failed to refresh the access token: 401"
        );
    }

    #[test]
    fn installation_auth_sets_bearer_url_user_and_hashes_the_id() {
        let app = Arc::new(GitHubAppAuthenticator::new(1234, TEST_PRIVATE_KEY).unwrap());
        let doer = Arc::new(FakeDoer {
            status: 500,
            body: Vec::new(),
            requests: std::sync::Mutex::new(Vec::new()),
        });
        let mut auth = InstallationAuthenticator::new(
            url::Url::parse("https://github.com").unwrap(),
            42,
            app,
            None,
            doer,
        );
        auth.set_token(InstallationAccessToken {
            token: "abc123".to_owned(),
            expires_at: None,
        });
        let mut headers = Vec::new();
        auth.authenticate("GET", "https://api.github.com", &mut headers)
            .unwrap();
        assert_eq!(
            headers,
            vec![("Authorization".to_owned(), "Bearer abc123".to_owned())]
        );
        assert!(!auth.needs_refresh());
        let mut url = url::Url::parse("https://example.com").unwrap();
        auth.set_url_user(&mut url);
        assert_eq!(url.username(), "x-access-token");
        assert_eq!(url.password(), Some("abc123"));
        assert_eq!(auth.hash(), sha256_hex("42"));
        assert_eq!(auth.installation_id(), 42);
    }
}
