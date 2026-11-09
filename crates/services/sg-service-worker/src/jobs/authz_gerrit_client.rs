#![forbid(unsafe_code)]

//! The Gerrit REST subset `internal/authz/providers/gerrit` needs, ported from
//! `internal/extsvc/gerrit/client.go` and `projects.go`:
//!
//! * `NewClient` / `WithAuthenticator` (basic auth; the authz provider swaps in
//!   the user's own credentials),
//! * `ListProjects` (`a/projects/` with `n`/`S` pagination; the authz provider
//!   requests all project types, so `listAllProjects` semantics apply),
//! * `GetGroup` (`a/groups/{name}`, used by `ValidateConnection`),
//! * the `)]}'` anti-XSSI prefix Gerrit puts in front of JSON responses,
//! * the error shape `Gerrit API HTTP error: code=%d url=%q body=%q`.
//!
//! Not ported: the self-imposed rate limiter, tracing, and the change
//! management methods the authz provider never calls.

use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use serde::de::DeserializeOwned;
use serde::Deserialize;

/// `extsvc.TypeGerrit` (`VariantGerrit.AsType()`).
pub const TYPE_GERRIT: &str = "gerrit";

/// `gerrit.Project`, reduced to the field the authz provider reads.
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
pub struct Project {
    #[serde(default)]
    pub id: String,
    #[serde(default)]
    pub name: String,
}

/// `gerrit.ListProjectsResponse`: project name → project. JSON `null` values
/// (the baseline's `*Project` nil) decode as `None`.
pub type ListProjectsResponse = BTreeMap<String, Option<Project>>;

/// `gerrit.Group`, reduced to the fields `ValidateConnection` checks.
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
pub struct Group {
    #[serde(default)]
    pub id: String,
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub created_on: String,
}

/// One `ListProjects` page: the decoded projects and `nextPage`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ProjectsPage {
    pub projects: ListProjectsResponse,
    pub next_page: bool,
}

/// `gerrit.httpError`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GerritError {
    pub message: String,
    pub status: Option<u16>,
}

impl GerritError {
    fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            status: None,
        }
    }
}

/// The subset of `gerrit.Client` the authz provider uses.
#[async_trait]
pub trait GerritClient: Send + Sync {
    /// `WithAuthenticator(&auth.BasicAuth{...})`.
    fn with_basic_auth(&self, username: &str, password: &str) -> Arc<dyn GerritClient>;

    /// `ListProjects(ctx, ListProjectsArgs{Cursor:{PerPage, Page}})`: all
    /// project types (`OnlyCodeProjects` false), `nextPage` when the page is
    /// full.
    async fn list_projects(&self, per_page: i64, page: i64) -> Result<ProjectsPage, GerritError>;

    /// `GetGroup`.
    async fn get_group(&self, group_name: &str) -> Result<Group, GerritError>;
}

/// `gerrit.NewClient` + `do`/`doHTTP`.
pub struct GerritRestClient {
    http: reqwest::Client,
    /// `client.URL`: requests are resolved against it like
    /// `(*)url.URL.ResolveReference`.
    base_url: reqwest::Url,
    username: String,
    password: String,
}

impl GerritRestClient {
    /// `NewClient(urn, url, &AccountCredentials{...}, nil)`.
    pub fn new(
        url: reqwest::Url,
        username: impl Into<String>,
        password: impl Into<String>,
    ) -> Self {
        Self::with_http_client(reqwest::Client::new(), url, username, password)
    }

    pub fn with_http_client(
        http: reqwest::Client,
        url: reqwest::Url,
        username: impl Into<String>,
        password: impl Into<String>,
    ) -> Self {
        Self {
            http,
            base_url: url,
            username: username.into(),
            password: password.into(),
        }
    }

    /// `doHTTP` + `do`: resolve the relative request, authenticate, send, cut
    /// the `)]}'` prefix and decode JSON; a response outside 200–399 becomes
    /// a `httpError` (the baseline's `code < 200 || code >= 400`).
    async fn do_json<T: DeserializeOwned>(&self, path_and_query: &str) -> Result<T, GerritError> {
        let url = self
            .base_url
            .join(path_and_query)
            .map_err(|err| GerritError::new(format!("creating request: {err}")))?;
        let response = self
            .http
            .get(url.clone())
            .basic_auth(&self.username, Some(&self.password))
            .send()
            .await
            .map_err(|err| GerritError::new(err.to_string()))?;

        let status = response.status();
        let body = response
            .text()
            .await
            .map_err(|err| GerritError::new(err.to_string()))?;

        // `doHTTP`: `statusCode < 200 || statusCode >= 400` is an error, so a
        // 3xx that survived redirect handling is not.
        if !(200..400).contains(&status.as_u16()) {
            return Err(GerritError {
                message: format!(
                    "Gerrit API HTTP error: code={} url={:?} body={:?}",
                    status.as_u16(),
                    url.as_str(),
                    body
                ),
                status: Some(status.as_u16()),
            });
        }

        // `bytes.CutPrefix(bs, []byte(")]}'"))`; the trailing newline is left
        // for the JSON decoder to skip.
        let body = body.strip_prefix(")]}'").unwrap_or(&body);
        serde_json::from_str(body).map_err(|err| GerritError::new(err.to_string()))
    }
}

#[async_trait]
impl GerritClient for GerritRestClient {
    fn with_basic_auth(&self, username: &str, password: &str) -> Arc<dyn GerritClient> {
        Arc::new(Self {
            http: self.http.clone(),
            base_url: self.base_url.clone(),
            username: username.to_owned(),
            password: password.to_owned(),
        })
    }

    async fn list_projects(&self, per_page: i64, page: i64) -> Result<ProjectsPage, GerritError> {
        // `listAllProjects`: `n`/`S`, `S = (Page-1)*PerPage`; `url.Values.Encode`
        // sorts the keys (`S` before `n`).
        let skip = (page - 1).saturating_mul(per_page);
        let query = format!("S={skip}&n={per_page}");
        let projects: ListProjectsResponse = self.do_json(&format!("a/projects/?{query}")).await?;
        let next_page = projects.len() as i64 == per_page;
        Ok(ProjectsPage {
            projects,
            next_page,
        })
    }

    async fn get_group(&self, group_name: &str) -> Result<Group, GerritError> {
        self.do_json(&format!("a/groups/{group_name}")).await
    }
}
