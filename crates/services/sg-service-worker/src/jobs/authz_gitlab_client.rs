#![forbid(unsafe_code)]

//! The GitLab REST subset `internal/authz/providers/gitlab` needs, ported from
//! `internal/extsvc/gitlab/client.go`, `projects.go`, `members.go` and
//! `users.go`:
//!
//! * `NewClientProvider`'s `/api/v4/` root (`path.Join(baseURL.Path,
//!   "api/v4") + "/"`),
//! * `GetPATClient(token, sudo)` (`Private-Token`, plus `Sudo` when set; an
//!   empty token means an unauthenticated client),
//! * `ListProjects`, `ListMembers`, `ListUsers` with the `Link: rel="next"`
//!   pagination header,
//! * `doWithBaseURL`'s headers (`Content-Type: application/json;
//!   charset=utf-8`, `Cache-Control: max-age=0`) and error shape
//!   (`request failed: …`, `read response body: …`, `unexpected response from
//!   GitLab API (<url>): HTTP error status <code>`).
//!
//! Not ported: the rate-limit monitors and 429 retries, the Redis-backed
//! project metadata cache, the request counter, and OAuth token refresh.

use std::sync::Arc;

use async_trait::async_trait;
use reqwest::header::{HeaderName, HeaderValue, CACHE_CONTROL, CONTENT_TYPE, LINK};
use reqwest::StatusCode;
use serde::de::DeserializeOwned;

use super::authz_gitlab::{
    AuthUser, GitlabClient, GitlabClientError, GitlabClientProvider, Member, Project,
};

/// `InitializeSudoableToken`'s header (`SudoableToken.Authenticate`).
const PRIVATE_TOKEN: HeaderName = HeaderName::from_static("private-token");
const SUDO: HeaderName = HeaderName::from_static("sudo");

/// `ClientProvider` reduced to the PAT clients the authz provider requests.
pub struct GitlabRestClientProvider {
    http: reqwest::Client,
    base_url: reqwest::Url,
    /// `SudoProvider.sudoToken`; empty means unauthenticated.
    token: String,
}

impl GitlabRestClientProvider {
    /// Build with the default HTTP client (which honours proxy environment
    /// variables, like the baseline's `httpcli.ExternalDoer`).
    pub fn new(base_url: &reqwest::Url, token: Option<String>) -> Self {
        Self::with_http_client(reqwest::Client::new(), base_url, token)
    }

    /// Build with an injected HTTP client (tests use a no-proxy client).
    pub fn with_http_client(
        http: reqwest::Client,
        base_url: &reqwest::Url,
        token: Option<String>,
    ) -> Self {
        Self {
            http,
            base_url: base_url.clone(),
            token: token.unwrap_or_default(),
        }
    }
}

impl GitlabClientProvider for GitlabRestClientProvider {
    fn pat_client(&self, sudo: &str) -> Arc<dyn GitlabClient> {
        Arc::new(GitlabRestClient::with_http_client(
            self.http.clone(),
            &self.base_url,
            &self.token,
            sudo,
        ))
    }
}

/// `gitlab.Client`, reduced to the authz methods. Each instance is bound to one
/// (token, sudo) pair; URLs are resolved against the instance's API root.
pub struct GitlabRestClient {
    http: reqwest::Client,
    api_base: reqwest::Url,
    token: String,
    sudo: String,
}

impl GitlabRestClient {
    /// `NewClientProvider(urn, baseURL, nil).GetPATClient(token, sudo)`.
    pub fn new(base_url: &reqwest::Url, token: &str, sudo: &str) -> Self {
        Self::with_http_client(reqwest::Client::new(), base_url, token, sudo)
    }

    pub fn with_http_client(
        http: reqwest::Client,
        base_url: &reqwest::Url,
        token: &str,
        sudo: &str,
    ) -> Self {
        Self {
            http,
            api_base: api_base_url(base_url),
            token: token.to_owned(),
            sudo: sudo.to_owned(),
        }
    }

    /// The `/api/v4/` root requests are resolved against.
    pub fn api_base(&self) -> &reqwest::Url {
        &self.api_base
    }

    async fn get_json<T: DeserializeOwned + Default>(
        &self,
        url_str: &str,
    ) -> Result<(T, Option<String>), GitlabClientError> {
        // `c.baseURL.ResolveReference(req.URL)`: relative paths resolve under
        // the API root, absolute `Link` targets (and other absolute URLs)
        // replace it.
        let url = self
            .api_base
            .join(url_str)
            .map_err(|err| GitlabClientError::new(format!("parse URL: {err}")))?;

        let mut request = self
            .http
            .get(url.clone())
            .header(CONTENT_TYPE, "application/json; charset=utf-8")
            .header(CACHE_CONTROL, "max-age=0");
        if !self.token.is_empty() {
            let value = HeaderValue::from_str(&self.token)
                .map_err(|err| GitlabClientError::new(format!("authenticating request: {err}")))?;
            request = request.header(PRIVATE_TOKEN, value);
            if !self.sudo.is_empty() {
                let value = HeaderValue::from_str(&self.sudo).map_err(|err| {
                    GitlabClientError::new(format!("authenticating request: {err}"))
                })?;
                request = request.header(SUDO, value);
            }
        }

        let response = request
            .send()
            .await
            .map_err(|err| GitlabClientError::new(format!("request failed: {err}")))?;

        let next = next_page_url(response.headers().get(LINK).and_then(|v| v.to_str().ok()));
        let status = response.status();
        let body = response
            .text()
            .await
            .map_err(|err| GitlabClientError::new(format!("read response body: {err}")))?;

        if !status.is_success() {
            // `errors.Wrap(NewHTTPError(code, body), "unexpected response from
            // GitLab API (<url>)")`: only the status appears in the message.
            return Err(GitlabClientError {
                message: format!(
                    "unexpected response from GitLab API ({url}): HTTP error status {}",
                    status.as_u16()
                ),
                not_found: status == StatusCode::NOT_FOUND,
                context_deadline: false,
            });
        }

        let value: Option<T> = serde_json::from_str(&body)
            .map_err(|err| GitlabClientError::new(format!("decode response: {err}")))?;
        Ok((value.unwrap_or_default(), next))
    }
}

#[async_trait]
impl GitlabClient for GitlabRestClient {
    async fn list_projects(
        &self,
        url: &str,
    ) -> Result<(Vec<Project>, Option<String>), GitlabClientError> {
        self.get_json(url).await
    }

    async fn list_members(
        &self,
        url: &str,
    ) -> Result<(Vec<Member>, Option<String>), GitlabClientError> {
        self.get_json(url).await
    }

    async fn list_users(
        &self,
        url: &str,
    ) -> Result<(Vec<AuthUser>, Option<String>), GitlabClientError> {
        self.get_json(url).await
    }
}

/// `NewClientProvider`'s API root:
/// `baseURL.ResolveReference(&url.URL{Path: path.Join(baseURL.Path, "api/v4") + "/"})`.
pub fn api_base_url(base_url: &reqwest::Url) -> reqwest::Url {
    let mut url = base_url.clone();
    url.set_path(&format!("{}/", join_path(base_url.path(), "api/v4")));
    url.set_query(None);
    url.set_fragment(None);
    url
}

/// Go's `path.Join` for the segments this client builds: drops empty and `.`
/// segments, resolves `..` and always returns a rooted path.
fn join_path(base: &str, extra: &str) -> String {
    let mut parts: Vec<&str> = Vec::new();
    for segment in base.split('/').chain(extra.split('/')) {
        match segment {
            "" | "." => {}
            ".." => {
                parts.pop();
            }
            other => parts.push(other),
        }
    }
    format!("/{}", parts.join("/"))
}

/// `link.Parse(header)["next"].URI`: the URI of the `rel="next"` entry, if
/// any. Entries are comma-separated `<uri>; rel="…"` values.
fn next_page_url(header: Option<&str>) -> Option<String> {
    let header = header?;
    for entry in header.split(',') {
        let mut pieces = entry.split(';');
        let Some(uri) = pieces.next() else {
            continue;
        };
        let uri = uri.trim();
        let Some(uri) = uri.strip_prefix('<').and_then(|uri| uri.strip_suffix('>')) else {
            continue;
        };
        for parameter in pieces {
            let Some((name, value)) = parameter.trim().split_once('=') else {
                continue;
            };
            if name.trim() == "rel" && value.trim().trim_matches('"') == "next" {
                return Some(uri.to_owned());
            }
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    fn url(raw: &str) -> reqwest::Url {
        reqwest::Url::parse(raw).expect("url")
    }

    #[test]
    fn api_base_appends_api_v4_root() {
        assert_eq!(
            api_base_url(&url("https://gitlab.example.com")).as_str(),
            "https://gitlab.example.com/api/v4/"
        );
        assert_eq!(
            api_base_url(&url("https://gitlab.example.com/sub")).as_str(),
            "https://gitlab.example.com/sub/api/v4/"
        );
        assert_eq!(
            api_base_url(&url("https://gitlab.example.com/sub/")).as_str(),
            "https://gitlab.example.com/sub/api/v4/"
        );
        assert_eq!(
            api_base_url(&url("https://gitlab.example.com/sub/../other")).as_str(),
            "https://gitlab.example.com/other/api/v4/"
        );
    }

    #[test]
    fn next_page_parsing() {
        assert_eq!(
            next_page_url(Some(
                "<projects?page=2>; rel=\"next\", <projects?page=5>; rel=\"last\""
            )),
            Some("projects?page=2".to_owned())
        );
        assert_eq!(
            next_page_url(Some("<projects?page=1>; rel=\"prev\">; rel=\"last\"")),
            None
        );
        assert_eq!(next_page_url(None), None);
        assert_eq!(next_page_url(Some("")), None);
    }
}
