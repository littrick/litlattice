#![forbid(unsafe_code)]

//! `internal/extsvc/auth`: authenticators that mutate an outbound request.
//!
//! The baseline interface takes a `*http.Request`; this port passes the
//! request method and URL plus a mutable header list. Only the authenticators
//! the GitHub source needs are implemented: basic auth (plain and with SSH
//! keys) and OAuth bearer tokens (plain and with SSH keys). OAuth 1 request
//! signing (`internal/extsvc/auth/oauth1.go`) is not ported.

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

/// `auth.OAuthClient` (OAuth 1). The type and hash are ported so callers can
/// carry credentials; request signing is not ported.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct OAuthClient {
    /// `oauth.Client.Credentials.Token`.
    pub consumer_token: String,
    /// `oauth.Client.Credentials.Secret`.
    pub consumer_secret: String,
    pub token: String,
    pub token_secret: String,
}

impl Authenticator for OAuthClient {
    fn authenticate(
        &self,
        _method: &str,
        _url: &str,
        _headers: &mut Vec<(String, String)>,
    ) -> Result<(), String> {
        Err("OAuth 1 request signing is not ported".to_owned())
    }

    fn hash(&self) -> String {
        format!(
            "{}{}",
            sha256_hex(&self.consumer_secret),
            sha256_hex(&self.consumer_token)
        )
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
}
