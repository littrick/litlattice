#![forbid(unsafe_code)]

//! The Bitbucket Cloud REST subset `internal/authz/providers/bitbucketcloud`
//! needs, ported from `internal/extsvc/bitbucketcloud/client.go`,
//! `repositories.go` and `user.go`:
//!
//! * `NewClient` (`/2.0/` root, `ApiURL` or `https://api.bitbucket.org`,
//!   `AccessToken` → `Authorization: Bearer`, otherwise basic auth),
//! * `WithAuthenticator` (the user-centric OAuth token),
//! * `Repos`, `Repo`, `ListExplicitUserPermsForRepo` with the `values`/`next`
//!   pagination envelope and `FetchAll`,
//! * `oauthutil.DoRequest`'s refresh contract (pre-emptive `NeedsRefresh`
//!   refresh, one refresh-and-retry on 401, a failed refresh keeps the
//!   original response),
//! * the error shape `Bitbucket Cloud API HTTP error: code=%d url=%q body=%q`.
//!
//! Not ported: the self-imposed rate limiter, the 429 exponential back-off,
//! the request counter and tracing.

use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use reqwest::header::{HeaderValue, AUTHORIZATION};
use reqwest::StatusCode;
use serde::de::DeserializeOwned;
use serde::Deserialize;

use super::authz_github_user::OAuthBearerToken;

/// `extsvc.TypeBitbucketCloud`.
/// `extsvc.TypeBitbucketCloud` (`VariantBitbucketCloud.AsType()`).
pub const TYPE_BITBUCKETCLOUD: &str = "bitbucketCloud";
/// `UrlFromConfig`'s default.
pub const DEFAULT_API_URL: &str = "https://api.bitbucket.org";

/// `bitbucketcloud.Account`, reduced to the fields the authz provider reads.
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
pub struct Account {
    #[serde(default)]
    pub uuid: String,
    #[serde(default)]
    pub username: String,
    #[serde(default)]
    pub display_name: String,
}

/// `bitbucketcloud.Repo`, reduced to the fields the authz provider reads.
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
pub struct Repo {
    #[serde(default)]
    pub uuid: String,
    #[serde(default)]
    pub full_name: String,
    #[serde(default)]
    pub owner: Option<Account>,
}

/// `bitbucketcloud.PageToken`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
pub struct PageToken {
    #[serde(default)]
    pub size: i64,
    #[serde(default)]
    pub page: i64,
    #[serde(default)]
    pub pagelen: i64,
    #[serde(default)]
    pub next: String,
}

impl PageToken {
    /// `HasMore`.
    pub fn has_more(&self) -> bool {
        !self.next.is_empty()
    }
}

/// The JSON envelope `reqPage` decodes.
#[derive(Debug, Deserialize)]
struct Page<T> {
    #[serde(default)]
    values: Option<Vec<T>>,
    #[serde(flatten)]
    token: PageToken,
}

/// `ExplicitUserPermsResponse`.
#[derive(Debug, Default, Deserialize)]
struct ExplicitUserPermsResponse {
    #[serde(default)]
    user: Option<Account>,
}

/// The two authenticators the provider uses: the connection's configured
/// basic auth / access token, or the user's OAuth token.
#[derive(Clone)]
pub enum BitbucketCloudAuth {
    /// `auth.BasicAuth{Username, Password}`.
    Basic { username: String, password: String },
    /// `auth.OAuthBearerToken` with `oauthutil.DoRequest` refresh semantics.
    Bearer(Arc<OAuthBearerToken>),
}

/// A client-level failure, retaining the HTTP status so the syncer's
/// `errcode.IsUnauthorized` / `IsNotFound` classification can be reproduced.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BitbucketCloudError {
    pub message: String,
    pub status: Option<u16>,
}

impl BitbucketCloudError {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            status: None,
        }
    }
}

/// `bitbucketcloud.Client`, reduced to the authz methods. Each instance is
/// bound to one authenticator, like `WithAuthenticator`.
#[async_trait]
pub trait BitbucketCloudClient: Send + Sync {
    /// `WithAuthenticator(oauthToken)`.
    fn with_bearer_token(&self, token: Arc<OAuthBearerToken>) -> Arc<dyn BitbucketCloudClient>;

    /// `Repos`: repositories of the authenticated user when `account_name` is
    /// empty, otherwise of that account. `fetch_all` follows `next`.
    async fn repos(
        &self,
        account_name: &str,
        role: Option<&str>,
        fetch_all: bool,
    ) -> Result<Vec<Repo>, BitbucketCloudError>;

    /// `Repo`: one repository by namespace and slug.
    async fn repo(&self, namespace: &str, slug: &str) -> Result<Repo, BitbucketCloudError>;

    /// `ListExplicitUserPermsForRepo`.
    async fn list_explicit_user_perms_for_repo(
        &self,
        namespace: &str,
        slug: &str,
        fetch_all: bool,
    ) -> Result<Vec<Account>, BitbucketCloudError>;
}

/// `bitbucketcloud.NewClient` + `do`/`page`/`fetchAll`.
pub struct BitbucketCloudRestClient {
    http: reqwest::Client,
    /// `extsvc.NormalizeBaseURL(UrlFromConfig(config))`.
    base_url: reqwest::Url,
    auth: BitbucketCloudAuth,
}

impl BitbucketCloudRestClient {
    /// `NewClient(urn, config, nil)`: the default HTTP client honours proxy
    /// environment variables like `httpcli.ExternalDoer`.
    pub fn new(api_url: Option<&str>, auth: BitbucketCloudAuth) -> Result<Self, String> {
        Self::with_http_client(reqwest::Client::new(), api_url, auth)
    }

    pub fn with_http_client(
        http: reqwest::Client,
        api_url: Option<&str>,
        auth: BitbucketCloudAuth,
    ) -> Result<Self, String> {
        // `UrlFromConfig`: an unparsable `apiURL` is an error only in
        // `NewClient`; `NewProvider` would otherwise silently decline.
        let parsed = reqwest::Url::parse(api_url.unwrap_or(DEFAULT_API_URL))
            .map_err(|err| err.to_string())?;
        Ok(Self {
            http,
            base_url: normalize_base_url(&parsed),
            auth,
        })
    }

    fn bearer(&self) -> Option<&Arc<OAuthBearerToken>> {
        match &self.auth {
            BitbucketCloudAuth::Bearer(token) => Some(token),
            BitbucketCloudAuth::Basic { .. } => None,
        }
    }

    /// `oauthutil.DoRequest`: pre-emptive refresh, then the request; a 401
    /// with a refreshable token refreshes and retries exactly once. A failed
    /// refresh keeps the original response.
    async fn do_request(
        &self,
        url: &reqwest::Url,
    ) -> Result<reqwest::Response, BitbucketCloudError> {
        if let Some(token) = self.bearer() {
            if token.needs_refresh() {
                if let Err(err) = token.refresh().await {
                    tracing::warn!(error = %err, "doRequest: token refresh failed");
                }
            }
        }

        let response = self.send(url).await?;
        if response.status() == StatusCode::UNAUTHORIZED {
            if let Some(token) = self.bearer() {
                if token.refresh().await.is_ok() {
                    return self.send(url).await;
                }
            }
        }
        Ok(response)
    }

    fn build_request(
        &self,
        url: &reqwest::Url,
    ) -> Result<reqwest::RequestBuilder, BitbucketCloudError> {
        let mut request = self.http.get(url.clone());
        match &self.auth {
            BitbucketCloudAuth::Basic { username, password } => {
                request = request.basic_auth(username, Some(password));
            }
            BitbucketCloudAuth::Bearer(token) => {
                let value =
                    HeaderValue::from_str(&format!("Bearer {}", token.token().access_token))
                        .map_err(|err| {
                            BitbucketCloudError::new(format!("authenticating request: {err}"))
                        })?;
                request = request.header(AUTHORIZATION, value);
            }
        }
        Ok(request)
    }

    async fn send(&self, url: &reqwest::Url) -> Result<reqwest::Response, BitbucketCloudError> {
        self.build_request(url)?
            .send()
            .await
            .map_err(|err| BitbucketCloudError::new(err.to_string()))
    }

    /// `c.do`: resolve against the API root, send, decode the body and turn a
    /// non-2xx response into `httpError`.
    async fn do_json<T: DeserializeOwned>(
        &self,
        url: &reqwest::Url,
    ) -> Result<T, BitbucketCloudError> {
        let response = self.do_request(url).await?;
        let status = response.status();
        let body = response
            .text()
            .await
            .map_err(|err| BitbucketCloudError::new(err.to_string()))?;

        if !status.is_success() {
            return Err(BitbucketCloudError {
                message: format!(
                    "Bitbucket Cloud API HTTP error: code={} url={:?} body={:?}",
                    status.as_u16(),
                    url.as_str(),
                    body
                ),
                status: Some(status.as_u16()),
            });
        }

        serde_json::from_str(&body).map_err(|err| BitbucketCloudError::new(err.to_string()))
    }

    /// `page`: merge the token's query values and pagelen, then `reqPage`.
    async fn page<T: DeserializeOwned + Default>(
        &self,
        path: &str,
        extra_query: &[(String, String)],
        token: Option<&PageToken>,
    ) -> Result<(PageToken, Vec<T>), BitbucketCloudError> {
        let mut query: BTreeMap<String, Vec<String>> = BTreeMap::new();
        for (key, value) in extra_query {
            query.entry(key.clone()).or_default().push(value.clone());
        }
        if let Some(token) = token {
            if !token.next.is_empty() {
                // `PageToken.Values()` parses the next link's query when it
                // parses; an unparsable link contributes nothing.
                if let Ok(next) = reqwest::Url::parse(&token.next) {
                    for (key, value) in next.query_pairs() {
                        query
                            .entry(key.into_owned())
                            .or_default()
                            .push(value.into_owned());
                    }
                }
            }
            if token.pagelen != 0 {
                // `url.Values.Set` replaces the next link's own `pagelen`.
                query.insert("pagelen".to_owned(), vec![token.pagelen.to_string()]);
            }
        }

        let mut url = self
            .base_url
            .join(path)
            .map_err(|err| BitbucketCloudError::new(format!("creating request: {err}")))?;
        if !query.is_empty() {
            let mut serializer = url::form_urlencoded::Serializer::new(String::new());
            for (key, values) in &query {
                for value in values {
                    serializer.append_pair(key, value);
                }
            }
            url.set_query(Some(&serializer.finish()));
        }

        let page: Page<T> = self.do_json(&url).await?;
        Ok((page.token, page.values.unwrap_or_default()))
    }

    /// `fetchAll`: follow `next` links until exhausted, accumulating values.
    /// Each iteration is `page(nextURL.Path, nil, next, …)`, so the previous
    /// response's `pagelen` and the next link's query are re-sent.
    async fn fetch_all<T: DeserializeOwned + Default>(
        &self,
        mut token: PageToken,
        mut values: Vec<T>,
    ) -> Result<Vec<T>, BitbucketCloudError> {
        while token.has_more() {
            let next_url = reqwest::Url::parse(&token.next)
                .map_err(|err| BitbucketCloudError::new(err.to_string()))?;
            let (next_token, mut page) = self.page::<T>(next_url.path(), &[], Some(&token)).await?;
            token = next_token;
            values.append(&mut page);
        }
        Ok(values)
    }
}

#[async_trait]
impl BitbucketCloudClient for BitbucketCloudRestClient {
    fn with_bearer_token(&self, token: Arc<OAuthBearerToken>) -> Arc<dyn BitbucketCloudClient> {
        Arc::new(Self {
            http: self.http.clone(),
            base_url: self.base_url.clone(),
            auth: BitbucketCloudAuth::Bearer(token),
        })
    }

    async fn repos(
        &self,
        account_name: &str,
        role: Option<&str>,
        fetch_all: bool,
    ) -> Result<Vec<Repo>, BitbucketCloudError> {
        let path = if account_name.is_empty() {
            "/2.0/repositories".to_owned()
        } else {
            format!("/2.0/repositories/{account_name}")
        };
        let mut extra_query = Vec::new();
        if let Some(role) = role.filter(|role| !role.is_empty()) {
            extra_query.push(("role".to_owned(), role.to_owned()));
        }
        // The provider passes `PageToken{Pagelen: 100}`.
        let token = PageToken {
            pagelen: 100,
            ..PageToken::default()
        };
        let (next, repos) = self.page::<Repo>(&path, &extra_query, Some(&token)).await?;
        if fetch_all {
            self.fetch_all(next, repos).await
        } else {
            Ok(repos)
        }
    }

    async fn repo(&self, namespace: &str, slug: &str) -> Result<Repo, BitbucketCloudError> {
        let path = format!("/2.0/repositories/{namespace}/{slug}");
        let url = self
            .base_url
            .join(&path)
            .map_err(|err| BitbucketCloudError::new(format!("creating request: {err}")))?;
        self.do_json(&url).await
    }

    async fn list_explicit_user_perms_for_repo(
        &self,
        namespace: &str,
        slug: &str,
        fetch_all: bool,
    ) -> Result<Vec<Account>, BitbucketCloudError> {
        let path = format!("/2.0/repositories/{namespace}/{slug}/permissions-config/users");
        let token = PageToken {
            pagelen: 100,
            ..PageToken::default()
        };
        let (next, responses) = self
            .page::<ExplicitUserPermsResponse>(&path, &[], Some(&token))
            .await?;
        let responses = if fetch_all {
            self.fetch_all(next, responses).await?
        } else {
            responses
        };
        Ok(responses
            .into_iter()
            .filter_map(|response| response.user)
            .collect())
    }
}

/// `extsvc.NormalizeBaseURL`: lowercase the host and ensure a trailing slash.
pub fn normalize_base_url(url: &reqwest::Url) -> reqwest::Url {
    let mut normalized = url.clone();
    if let Some(host) = normalized.host_str() {
        let _ = normalized.set_host(Some(&host.to_lowercase()));
    }
    if !normalized.path().ends_with('/') {
        normalized.set_path(&format!("{}/", normalized.path()));
    }
    normalized
}
