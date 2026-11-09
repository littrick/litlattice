#![forbid(unsafe_code)]

//! The `internal/extsvc/bitbucketserver` subset the
//! `bitbucket-project-permissions` worker needs: config decoding, client
//! construction (`NewClient`) and `ProjectRepos` pagination.
//!
//! The baseline client (`internal/extsvc/bitbucketserver/client.go`) is large;
//! this port keeps only the request path the job uses:
//!
//! * authentication selection exactly as `NewClient`:
//!   `authorization == nil || authorization.oauth2` → `token` becomes an
//!   `OAuthBearerToken`, otherwise `username`/`password` become `BasicAuth`;
//!   `authorization.oauth` (two-legged OAuth 1.0a) requires RSA request
//!   signing, which this port does not implement (docs §74.3);
//! * the URL is resolved the way `(*Client).do` does: join the base path with
//!   the relative API path, then take scheme/host from the configured URL;
//! * `ProjectRepos` walks `rest/api/1.0/projects/{key}/repos` with
//!   `limit=1000` and follows `nextPageStart` while `isLastPage` is false;
//! * a non-2xx response is a `Bitbucket API HTTP error: code=%d url=%q
//!   body=%q`, made non-retryable for the same status codes
//!   `errcode.MaybeMakeNonRetryable` covers.
//!
//! Not ported: the `certificate` client option (custom CA roots), rate
//! limiting (`ratelimit.InstrumentedLimiter`), request metrics and tracing.
//! All are observability/TLS-configuration concerns that do not change the
//! request sequence for the job.

use base64::Engine;
use serde::Deserialize;
use url::Url;

/// `schema.BitbucketServerConnection`, reduced to the fields used here.
#[derive(Debug, Clone, Default, Deserialize, PartialEq, Eq)]
pub struct BitbucketServerConnection {
    #[serde(default)]
    pub authorization: Option<BitbucketServerAuthorization>,
    #[serde(default)]
    pub certificate: String,
    #[serde(default)]
    pub password: String,
    #[serde(default)]
    pub token: String,
    #[serde(default)]
    pub url: String,
    #[serde(default)]
    pub username: String,
}

/// `schema.BitbucketServerAuthorization`.
#[derive(Debug, Clone, Default, Deserialize, PartialEq, Eq)]
pub struct BitbucketServerAuthorization {
    #[serde(default)]
    pub oauth2: bool,
    #[serde(default)]
    pub oauth: Option<serde_json::Value>,
}

/// `bitbucketserver.Repo`, reduced to the identifier the handler matches
/// against `repo.external_id`.
#[derive(Debug, Clone, Default, Deserialize, PartialEq, Eq)]
pub struct Repo {
    #[serde(default)]
    pub id: i64,
    #[serde(default)]
    pub slug: String,
    #[serde(default)]
    pub name: String,
}

/// `bitbucketserver.PageToken`.
#[derive(Debug, Clone, Default, Deserialize)]
struct PageToken {
    #[serde(default)]
    limit: i64,
    #[serde(default, rename = "isLastPage")]
    is_last_page: bool,
    #[serde(default, rename = "nextPageStart")]
    next_page_start: i64,
}

impl PageToken {
    fn has_more(&self) -> bool {
        !self.is_last_page
    }

    /// `PageToken.Values`: `start` only when non-zero, `limit` only when
    /// non-zero. `url.Values.Encode` sorts the keys, so `limit` precedes
    /// `start`.
    fn query(&self) -> Vec<(String, String)> {
        let mut query = Vec::new();
        if self.next_page_start != 0 {
            query.push(("start".to_owned(), self.next_page_start.to_string()));
        }
        if self.limit != 0 {
            query.push(("limit".to_owned(), self.limit.to_string()));
        }
        query.sort();
        query
    }
}

#[derive(Debug, Clone)]
enum Auth {
    Basic { username: String, password: String },
    Bearer { token: String },
}

/// A `bitbucketserver.Client` limited to `ProjectRepos`.
#[derive(Debug, Clone)]
pub struct BitbucketServerClient {
    base: Url,
    auth: Auth,
    http: reqwest::Client,
}

impl BitbucketServerClient {
    /// `bitbucketserver.NewClient`.
    pub fn new(
        config: &BitbucketServerConnection,
        http: reqwest::Client,
    ) -> Result<Self, BitbucketServerError> {
        let base = Url::parse(&config.url)
            .map_err(|err| BitbucketServerError::retryable(err.to_string()))?;

        let auth = match &config.authorization {
            None => Self::default_auth(config),
            Some(authorization) => {
                if authorization.oauth2 {
                    Self::default_auth(config)
                } else if authorization.oauth.is_some() {
                    return Err(BitbucketServerError::retryable(
                        "authorization.oauth.signingKey: OAuth 1.0 request signing is not implemented by the Rust worker"
                            .to_owned(),
                    ));
                } else {
                    // The schema is a tagged union; `authorization: {}` does not
                    // select either authenticator and leaves `client.Auth` nil
                    // in the baseline (which panics on the first request). Model
                    // it as basic auth with empty credentials.
                    Self::default_auth(config)
                }
            }
        };

        Ok(Self { base, auth, http })
    }

    /// `NewClient`'s `config.Token != "" ? OAuthBearerToken : BasicAuth`.
    fn default_auth(config: &BitbucketServerConnection) -> Auth {
        if config.token.is_empty() {
            Auth::Basic {
                username: config.username.clone(),
                password: config.password.clone(),
            }
        } else {
            Auth::Bearer {
                token: config.token.clone(),
            }
        }
    }

    fn auth_header(&self) -> String {
        match &self.auth {
            Auth::Basic { username, password } => format!(
                "Basic {}",
                base64::engine::general_purpose::STANDARD.encode(format!("{username}:{password}"))
            ),
            Auth::Bearer { token } => format!("Bearer {token}"),
        }
    }

    /// `(*Client).do`: join the base path with the relative path, keep the
    /// configured scheme/host, then issue the request.
    fn request_url(&self, path: &str, query: &[(String, String)]) -> Url {
        let mut url = self.base.clone();
        let base_path = self.base.path();
        let joined = if base_path.ends_with('/') {
            format!("{base_path}{path}")
        } else {
            format!("{base_path}/{path}")
        };
        url.set_path(&joined);
        url.set_query(None);
        if !query.is_empty() {
            let mut pairs = url.query_pairs_mut();
            for (key, value) in query {
                pairs.append_pair(key, value);
            }
        }
        url
    }

    /// `ProjectRepos`: every page of `rest/api/1.0/projects/{key}/repos`.
    pub async fn project_repos(
        &self,
        project_key: &str,
    ) -> Result<Vec<Repo>, BitbucketServerError> {
        if project_key.is_empty() {
            return Err(BitbucketServerError::retryable(
                "project key empty".to_owned(),
            ));
        }

        let path = format!("rest/api/1.0/projects/{project_key}/repos");
        let mut token = PageToken {
            limit: 1000,
            ..PageToken::default()
        };
        let mut repos = Vec::new();

        loop {
            let url = self.request_url(&path, &token.query());
            let body = self.send(url).await?;
            let page: PageToken = match serde_json::from_slice::<Page<Repo>>(&body) {
                Ok(page) => {
                    repos.extend(page.values);
                    page.token
                }
                Err(err) => {
                    return Err(BitbucketServerError::retryable(format!(
                        "failed to unmarshal response to JSON: {err}"
                    )))
                }
            };
            if !page.has_more() {
                return Ok(repos);
            }
            token = page;
        }
    }

    async fn send(&self, url: Url) -> Result<Vec<u8>, BitbucketServerError> {
        let response = self
            .http
            .get(url.clone())
            .header("Authorization", self.auth_header())
            .header("Content-Type", "application/json; charset=utf-8")
            .send()
            .await
            .map_err(|err| BitbucketServerError::retryable(err.to_string()))?;

        let status = response.status();
        let body = response
            .bytes()
            .await
            .map_err(|err| BitbucketServerError::retryable(err.to_string()))?;

        if !status.is_success() {
            return Err(BitbucketServerError::http(status.as_u16(), &url, &body));
        }
        Ok(body.to_vec())
    }
}

/// Deserialises `{..., "values": [...]}` while keeping the page token fields
/// at the same level.
#[derive(Debug, Deserialize)]
struct Page<T> {
    #[serde(flatten)]
    token: PageToken,
    #[serde(default)]
    values: Vec<T>,
}

/// `httpError` plus the `MaybeMakeNonRetryable` classification.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BitbucketServerError {
    pub message: String,
    /// `Some(status)` for `httpError`; `None` for transport/JSON errors.
    pub status: Option<u16>,
}

impl BitbucketServerError {
    pub fn retryable(message: String) -> Self {
        Self {
            message,
            status: None,
        }
    }

    fn http(status: u16, url: &Url, body: &[u8]) -> Self {
        Self {
            message: format!(
                "Bitbucket API HTTP error: code={status} url={} body={}",
                go_quote(url.as_str()),
                go_quote(&String::from_utf8_lossy(body))
            ),
            status: Some(status),
        }
    }

    /// `errcode.MaybeMakeNonRetryable` for the statuses `(*Client).do` sees:
    /// 1xx, 3xx, 4xx, 501 and >= 600 are non-retryable.
    pub fn is_non_retryable(&self) -> bool {
        match self.status {
            Some(status) => {
                status < 200 || (300..500).contains(&status) || status == 501 || status >= 600
            }
            None => false,
        }
    }
}

impl std::fmt::Display for BitbucketServerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

/// Go's `%q` for a string (`strconv.Quote` for the ASCII bodies the client
/// reports): double quotes, backslash escapes for quotes/backslashes/control
/// characters.
pub fn go_quote(value: &str) -> String {
    let mut out = String::from("\"");
    for character in value.chars() {
        match character {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            character if u32::from(character) < 0x20 || u32::from(character) == 0x7f => {
                out.push_str(&format!("\\x{:02x}", u32::from(character)));
            }
            character => out.push(character),
        }
    }
    out.push('"');
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config(json: &str) -> BitbucketServerConnection {
        serde_json::from_str(json).expect("config")
    }

    fn client(json: &str) -> BitbucketServerClient {
        BitbucketServerClient::new(&config(json), reqwest::Client::new()).expect("client")
    }

    #[test]
    fn token_auth_wins_over_username_password() {
        let client = client(
            r#"{"url":"https://bbs.example.com/","token":"t","username":"u","password":"p"}"#,
        );
        assert_eq!(client.auth_header(), "Bearer t");
    }

    #[test]
    fn username_password_become_basic_auth() {
        let client = client(r#"{"url":"https://bbs.example.com/","username":"u","password":"p"}"#);
        assert_eq!(client.auth_header(), "Basic dTpw");
    }

    #[test]
    fn oauth2_selection_uses_token_like_nil_authorization() {
        let client = client(
            r#"{"url":"https://bbs.example.com/","token":"t","authorization":{"oauth2":true}}"#,
        );
        assert_eq!(client.auth_header(), "Bearer t");
    }

    #[test]
    fn oauth1_is_explicitly_unsupported() {
        let err = BitbucketServerClient::new(
            &config(
                r#"{"url":"https://bbs.example.com/","authorization":{"oauth":{"consumerKey":"k","signingKey":"s"}}}"#,
            ),
            reqwest::Client::new(),
        )
        .expect_err("oauth1 must fail");
        assert!(
            err.message.starts_with("authorization.oauth.signingKey: "),
            "{err}"
        );
        assert!(!err.is_non_retryable());
    }

    #[test]
    fn page_query_matches_url_values_encoding() {
        assert_eq!(
            PageToken {
                limit: 1000,
                ..PageToken::default()
            }
            .query(),
            vec![("limit".to_owned(), "1000".to_owned())]
        );
        assert_eq!(
            PageToken {
                limit: 2,
                next_page_start: 2,
                ..PageToken::default()
            }
            .query(),
            vec![
                ("limit".to_owned(), "2".to_owned()),
                ("start".to_owned(), "2".to_owned())
            ]
        );
    }

    #[test]
    fn request_url_keeps_the_configured_base_path() {
        let client = bitbucket_client_for("https://bbs.example.com/bitbucket");
        let url = client.request_url("rest/api/1.0/projects/PROJ/repos", &[]);
        assert_eq!(
            url.as_str(),
            "https://bbs.example.com/bitbucket/rest/api/1.0/projects/PROJ/repos"
        );

        let client = bitbucket_client_for("https://bbs.example.com/");
        let url = client.request_url("rest/api/1.0/projects/PROJ/repos", &[]);
        assert_eq!(
            url.as_str(),
            "https://bbs.example.com/rest/api/1.0/projects/PROJ/repos"
        );
    }

    fn bitbucket_client_for(url: &str) -> BitbucketServerClient {
        BitbucketServerClient::new(
            &BitbucketServerConnection {
                url: url.to_owned(),
                token: "t".to_owned(),
                ..BitbucketServerConnection::default()
            },
            reqwest::Client::new(),
        )
        .expect("client")
    }

    #[test]
    fn http_error_status_classification_matches_errcode() {
        let url = Url::parse("https://bbs.example.com/x").expect("url");
        for (status, expected) in [
            (400, true),
            (401, true),
            (404, true),
            (429, true),
            (500, false),
            (501, true),
            (502, false),
        ] {
            let error = BitbucketServerError::http(status, &url, b"boom");
            assert_eq!(error.is_non_retryable(), expected, "status {status}");
        }
        assert_eq!(
            BitbucketServerError::http(404, &url, b"").message,
            r#"Bitbucket API HTTP error: code=404 url="https://bbs.example.com/x" body="""#
        );
    }

    #[test]
    fn go_quote_escapes_control_characters() {
        assert_eq!(go_quote("a\"b\\c\nd"), r#""a\"b\\c\nd""#);
    }
}
