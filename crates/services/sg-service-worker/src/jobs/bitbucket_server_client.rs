#![forbid(unsafe_code)]

//! The `internal/extsvc/bitbucketserver` subset used by the worker:
//! configuration decoding, client construction (`NewClient`), pagination and
//! the API calls the authz provider and the `bitbucket-project-permissions`
//! job need.
//!
//! Requirement/response semantics follow `client.go`:
//!
//! * authentication selection exactly as `NewClient`:
//!   `authorization == nil || authorization.oauth2` → `token` becomes an
//!   `OAuthBearerToken`, otherwise `username`/`password` become `BasicAuth`;
//!   `authorization.oauth` (two-legged OAuth 1.0a) requires RSA request
//!   signing, which this port does not implement (docs §74.3); constructing
//!   such a client is an error, which the provider registry reports as a
//!   serious problem (deny-by-default);
//! * the URL is resolved the way `(*Client).do` does: join the base path with
//!   the relative API path, then take scheme/host from the configured URL;
//! * page queries are `url.Values.Encode`-equivalent (`limit`/`start` sorted,
//!   filters sorted by key);
//! * a non-2xx response is a `Bitbucket API HTTP error: code=%d url=%q
//!   body=%q`, made non-retryable for the same status codes
//!   `errcode.MaybeMakeNonRetryable` covers.
//!
//! Not ported: the `certificate` client option (custom CA roots), rate
//! limiting (`ratelimit.InstrumentedLimiter`), request metrics and tracing.
//! All are observability/TLS-configuration concerns that do not change the
//! request sequence.

use std::io::Cursor;

use base64::Engine;
use roaring::RoaringBitmap;
use serde::Deserialize;
use serde_json::Value;
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
    pub plugin: Option<BitbucketServerPlugin>,
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
    #[serde(default, rename = "identityProvider")]
    pub identity_provider: Option<Value>,
    #[serde(default)]
    pub oauth: Option<BitbucketServerOAuth>,
    #[serde(default)]
    pub oauth2: bool,
}

/// `schema.BitbucketServerOAuth`.
#[derive(Debug, Clone, Default, Deserialize, PartialEq, Eq)]
pub struct BitbucketServerOAuth {
    #[serde(default, rename = "consumerKey")]
    pub consumer_key: String,
    #[serde(default, rename = "signingKey")]
    pub signing_key: String,
}

/// `schema.BitbucketServerPlugin`.
#[derive(Debug, Clone, Default, Deserialize, PartialEq, Eq)]
pub struct BitbucketServerPlugin {
    #[serde(default)]
    pub permissions: String,
}

impl BitbucketServerAuthorization {
    /// `identityProvider.username` (the schema is a tagged union with only
    /// this member).
    pub fn username_identity(&self) -> bool {
        self.identity_provider
            .as_ref()
            .and_then(|idp| idp.get("type"))
            .and_then(Value::as_str)
            == Some("username")
    }

    pub fn has_identity_provider(&self) -> bool {
        self.identity_provider
            .as_ref()
            .is_some_and(|value| !value.is_null())
    }
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

/// `bitbucketserver.User`.
#[derive(Debug, Clone, Default, Deserialize, PartialEq, Eq)]
pub struct User {
    #[serde(default)]
    pub name: String,
    #[serde(default, rename = "emailAddress")]
    pub email_address: String,
    #[serde(default)]
    pub id: i64,
    #[serde(default, rename = "displayName")]
    pub display_name: String,
    #[serde(default)]
    pub active: bool,
    #[serde(default)]
    pub slug: String,
    #[serde(default, rename = "type")]
    pub user_type: String,
}

impl User {
    /// `json.Marshal(User)`: Go's struct tags omit empty fields except `id`
    /// is `omitempty` as well (so `""`, `0`, `false` are dropped). Rust
    /// serialises every field, so the provider builds the account-data JSON
    /// explicitly with [`Self::to_account_data_json`].
    pub fn to_account_data_json(&self) -> String {
        // `json.Marshal` emits struct fields in declaration order (a plain
        // `serde_json::Map` would sort the keys), so build the object in
        // source order.
        let escaped = |value: &str| Value::String(value.to_owned()).to_string();
        let mut parts: Vec<String> = Vec::new();
        if !self.name.is_empty() {
            parts.push(format!("\"name\":{}", escaped(&self.name)));
        }
        if !self.email_address.is_empty() {
            parts.push(format!("\"emailAddress\":{}", escaped(&self.email_address)));
        }
        if self.id != 0 {
            parts.push(format!("\"id\":{}", self.id));
        }
        if !self.display_name.is_empty() {
            parts.push(format!("\"displayName\":{}", escaped(&self.display_name)));
        }
        if self.active {
            parts.push("\"active\":true".to_owned());
        }
        if !self.slug.is_empty() {
            parts.push(format!("\"slug\":{}", escaped(&self.slug)));
        }
        if !self.user_type.is_empty() {
            parts.push(format!("\"type\":{}", escaped(&self.user_type)));
        }
        format!("{{{}}}", parts.join(","))
    }
}

/// `bitbucketserver.PageToken`.
#[derive(Debug, Clone, Default, Deserialize, PartialEq, Eq)]
pub struct PageToken {
    #[serde(default)]
    pub limit: i64,
    #[serde(default, rename = "isLastPage")]
    pub is_last_page: bool,
    #[serde(default, rename = "nextPageStart")]
    pub next_page_start: i64,
}

impl PageToken {
    pub fn has_more(&self) -> bool {
        !self.is_last_page
    }

    /// `PageToken.Values`: `start` only when non-zero, `limit` only when
    /// non-zero. `url.Values.Encode` sorts the keys, so `limit` precedes
    /// `start`.
    pub fn query(&self) -> Vec<(String, String)> {
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

/// `bitbucketserver.UserFilter`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct UserFilter {
    /// `Filter`: substring match on username, name or email.
    pub filter: String,
    /// `Group`.
    pub group: String,
    /// `Permission`.
    pub permission: PermissionFilter,
}

/// `bitbucketserver.PermissionFilter`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PermissionFilter {
    pub root: String,
    pub project_id: String,
    pub project_key: String,
    pub repository_id: String,
    pub repository_slug: String,
    /// `index` is 1-based (`UserFilters.EncodeTo` increments before assigning),
    /// so the first permission filter uses `permission.1`.
    pub index: usize,
}

impl UserFilter {
    pub fn by_filter(username: &str) -> Self {
        Self {
            filter: username.to_owned(),
            ..Self::default()
        }
    }

    pub fn by_repo_read(repository_id: &str) -> Self {
        Self {
            permission: PermissionFilter {
                root: "REPO_READ".to_owned(),
                repository_id: repository_id.to_owned(),
                index: 1,
                ..PermissionFilter::default()
            },
            ..Self::default()
        }
    }

    /// `UserFilter.EncodeTo` appending to the encoded query pairs.
    fn encode_into(&self, query: &mut Vec<(String, String)>) {
        if !self.filter.is_empty() {
            query.push(("filter".to_owned(), self.filter.clone()));
        }
        if !self.group.is_empty() {
            query.push(("group".to_owned(), self.group.clone()));
        }
        if !self.permission.root.is_empty() {
            let mut key = "permission".to_owned();
            if self.permission.index != 0 {
                key.push('.');
                key.push_str(&self.permission.index.to_string());
            }
            query.push((key.clone(), self.permission.root.clone()));
            if !self.permission.project_id.is_empty() {
                query.push((format!("{key}.projectId"), self.permission.project_id.clone()));
            }
            if !self.permission.project_key.is_empty() {
                query.push((format!("{key}.projectKey"), self.permission.project_key.clone()));
            }
            if !self.permission.repository_id.is_empty() {
                query.push((
                    format!("{key}.repositoryId"),
                    self.permission.repository_id.clone(),
                ));
            }
            if !self.permission.repository_slug.is_empty() {
                query.push((
                    format!("{key}.repositorySlug"),
                    self.permission.repository_slug.clone(),
                ));
            }
        }
    }
}

/// `bitbucketserver.Client.Auth`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Auth {
    Basic { username: String, password: String },
    Bearer { token: String },
}

/// A `bitbucketserver.Client` limited to the authz/worker calls.
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
                    // OAuth1 request signing (RSA-SHA1) is not ported; the
                    // baseline's `SetOAuth` accepts a valid key pair and then
                    // signs each request with a random nonce, which cannot be
                    // reproduced deterministically. Deny by construction.
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

    /// The `httpcli.Doer` the client would use (`httpcli.ExternalClient` in
    /// the baseline); exposed so OAuth token refresh posts with the same
    /// transport.
    pub fn http_client(&self) -> reqwest::Client {
        self.http.clone()
    }

    /// `(*Client).WithAuthenticator`.
    pub fn with_authenticator(&self, auth: Auth) -> Self {
        Self {
            base: self.base.clone(),
            auth,
            http: self.http.clone(),
        }
    }

    /// `(*Client).WithAuthenticator(&auth.OAuthBearerToken{Token: token})`.
    pub fn with_bearer_token(&self, token: &str) -> Self {
        self.with_authenticator(Auth::Bearer {
            token: token.to_owned(),
        })
    }

    /// `(*Client).Sudo`: only OAuth1 clients are sudoable in the baseline;
    /// this port never constructs one, so this always returns the baseline's
    /// error text for non-OAuth clients.
    pub fn sudo(&self, _username: &str) -> Result<Self, BitbucketServerError> {
        Err(BitbucketServerError::retryable(
            "bitbucketserver.Client: OAuth not configured".to_owned(),
        ))
    }

    /// `(*Client).Username`.
    pub fn username(&self) -> Result<String, BitbucketServerError> {
        match &self.auth {
            Auth::Basic { username, .. } => Ok(username.clone()),
            Auth::Bearer { .. } => Err(BitbucketServerError::retryable(
                "bitbucketserver.Client: authentication method does not include a username"
                    .to_owned(),
            )),
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
            // `url.Values.Encode` sorts by key; values within one key keep
            // their insertion order (stable sort).
            let mut query = query.to_vec();
            query.sort_by(|left, right| left.0.cmp(&right.0));
            let mut pairs = url.query_pairs_mut();
            for (key, value) in &query {
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
            let page = self.decode_page::<Repo>(&body)?;
            repos.extend(page.values);
            if !page.token.has_more() {
                return Ok(repos);
            }
            token = page.token;
        }
    }

    /// `Client.Users`: one page of `rest/api/1.0/users` with the filters.
    pub async fn users(
        &self,
        token: &PageToken,
        filters: &[UserFilter],
    ) -> Result<(Vec<User>, PageToken), BitbucketServerError> {
        let mut query = Vec::new();
        let mut permission_count = 0;
        for filter in filters {
            let mut filter = filter.clone();
            // `UserFilters.EncodeTo` numbers permission filters starting at 1,
            // so even a single filter uses `permission.1=...`.
            if !filter.permission.root.is_empty() {
                permission_count += 1;
                filter.permission.index = permission_count;
            }
            filter.encode_into(&mut query);
        }
        query.extend(token.query());
        let url = self.request_url("rest/api/1.0/users", &query);
        let body = self.send(url).await?;
        let page = self.decode_page::<User>(&body)?;
        Ok((page.values, page.token))
    }

    /// `Client.UserPermissions`: global permissions of `username`.
    pub async fn user_permissions(
        &self,
        username: &str,
    ) -> Result<Vec<String>, BitbucketServerError> {
        #[derive(Deserialize)]
        struct PermissionEntry {
            #[serde(default)]
            user: Option<User>,
            #[serde(default)]
            permission: String,
        }

        #[derive(Deserialize)]
        struct Response {
            #[serde(default)]
            values: Vec<PermissionEntry>,
        }

        let url = self.request_url(
            "rest/api/1.0/admin/permissions/users",
            &[("filter".to_owned(), username.to_owned())],
        );
        let body = self.send(url).await?;
        let response: Response = serde_json::from_slice(&body).map_err(|err| {
            BitbucketServerError::retryable(format!(
                "failed to unmarshal response to JSON: {err}"
            ))
        })?;
        Ok(response
            .values
            .into_iter()
            .filter(|entry| {
                entry
                    .user
                    .as_ref()
                    .is_some_and(|user| user.name == username)
            })
            .map(|entry| entry.permission)
            .collect())
    }

    /// `Client.Repos`: one page of `rest/api/1.0/repos`, merging the
    /// caller's query strings (e.g. `?visibility=private`).
    pub async fn repos(
        &self,
        token: &PageToken,
        search_queries: &[&str],
    ) -> Result<(Vec<Repo>, PageToken), BitbucketServerError> {
        let mut query = parse_query_strings(search_queries)?;
        query.extend(token.query());
        let url = self.request_url("rest/api/1.0/repos", &query);
        let body = self.send(url).await?;
        let page = self.decode_page::<Repo>(&body)?;
        Ok((page.values, page.token))
    }

    /// `Client.RepoIDs`: the roaring-bitmap plugin endpoint.
    pub async fn repo_ids(&self, permission: &str) -> Result<Vec<u32>, BitbucketServerError> {
        let url = self.request_url(
            "rest/sourcegraph-admin/1.0/permissions/repositories",
            &[("permission".to_owned(), permission.to_owned())],
        );
        let body = self.send(url).await?;
        let bitmap = RoaringBitmap::deserialize_from(Cursor::new(&body))
            .map_err(|err| BitbucketServerError::retryable(err.to_string()))?;
        Ok(bitmap.iter().collect())
    }

    fn decode_page<T: for<'de> Deserialize<'de>>(
        &self,
        body: &[u8],
    ) -> Result<Page<T>, BitbucketServerError> {
        serde_json::from_slice::<Page<T>>(body).map_err(|err| {
            BitbucketServerError::retryable(format!(
                "failed to unmarshal response to JSON: {err}"
            ))
        })
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

/// `parseQueryStrings`: merge `?a=b&c=d` fragments. `url.ParseQuery`
/// percent-decodes and treats `+` as space; `form_urlencoded` matches.
fn parse_query_strings(query_strings: &[&str]) -> Result<Vec<(String, String)>, BitbucketServerError> {
    let mut query = Vec::new();
    for raw in query_strings {
        let trimmed = raw.strip_prefix('?').unwrap_or(raw);
        for (key, value) in url::form_urlencoded::parse(trimmed.as_bytes()) {
            query.push((key.into_owned(), value.into_owned()));
        }
    }
    Ok(query)
}

/// Deserialises `{..., "values": [...]}` while keeping the page token fields
/// at the same level.
#[derive(Debug, Deserialize)]
#[serde(bound(deserialize = "T: Deserialize<'de>"))]
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
        assert!(client.username().is_err());
    }

    #[test]
    fn username_password_become_basic_auth() {
        let client = client(r#"{"url":"https://bbs.example.com/","username":"u","password":"p"}"#);
        assert_eq!(client.auth_header(), "Basic dTpw");
        assert_eq!(client.username().expect("username"), "u");
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
    fn sudo_requires_oauth1() {
        let client = client(r#"{"url":"https://bbs.example.com/","username":"u","password":"p"}"#);
        let err = client.sudo("alice").expect_err("not sudoable");
        assert_eq!(err.message, "bitbucketserver.Client: OAuth not configured");
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
    fn user_filter_encoding_matches_url_values() {
        let mut query = Vec::new();
        UserFilter::by_filter("alice").encode_into(&mut query);
        assert_eq!(query, vec![("filter".to_owned(), "alice".to_owned())]);

        let mut query = Vec::new();
        UserFilter::by_repo_read("42").encode_into(&mut query);
        query.sort();
        assert_eq!(
            query,
            vec![
                ("permission.1".to_owned(), "REPO_READ".to_owned()),
                ("permission.1.repositoryId".to_owned(), "42".to_owned()),
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

    #[test]
    fn user_account_data_json_omits_empty_fields_like_go() {
        let user = User {
            name: "alice".to_owned(),
            id: 7,
            active: true,
            ..User::default()
        };
        assert_eq!(
            user.to_account_data_json(),
            r#"{"name":"alice","id":7,"active":true}"#
        );
    }
}
