#![forbid(unsafe_code)]

//! `internal/extsvc/github`: GitHub REST v3 and GraphQL v4 clients.
//!
//! Only the surface `internal/repos/github.go` needs is ported. Request URL
//! construction, headers, pagination (`Link`), API/GraphQL error handling and
//! rate limit headers follow the baseline. Pull requests, webhooks, GitHub App
//! authentication and the OAuth1 signing helper are not ported (see the
//! slice overview).

use std::fmt;
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use time::{format_description::FormatItem, macros::format_description, OffsetDateTime};
use url::Url;

use crate::auth::{add_header, set_header, Authenticator};
use crate::HttpDoer;

/// `extsvc.TypeGitHub`.
pub const TYPE_GITHUB: &str = "github";

/// `github.VisibilityAll`.
pub const VISIBILITY_ALL: &str = "all";
/// `github.VisibilityInternal`.
pub const VISIBILITY_INTERNAL: &str = "internal";

/// `github.URLIsGitHubDotCom`.
pub fn url_is_github_dot_com(api_url: &Url) -> bool {
    let host = api_url.host_str().unwrap_or_default().to_ascii_lowercase();
    host == "api.github.com" || host == "github.com" || host == "www.github.com"
}

/// `github.canonicalizedURL`.
pub fn canonicalized_url(api_url: &Url) -> Url {
    if url_is_github_dot_com(api_url) {
        Url::parse("https://api.github.com/").expect("static url")
    } else {
        api_url.clone()
    }
}

/// `github.APIRoot`.
pub fn api_root(base_url: &Url) -> (Url, bool) {
    let hostname = base_url.host_str().unwrap_or_default().to_ascii_lowercase();
    if hostname == "github.com" || hostname == "www.github.com" {
        return (
            Url::parse("https://api.github.com/").expect("static url"),
            true,
        );
    }
    if base_url.path().is_empty() || base_url.path() == "/" {
        return (
            base_url
                .join("/api/v3")
                .unwrap_or_else(|_| base_url.clone()),
            false,
        );
    }
    (
        base_url.join("api").unwrap_or_else(|_| base_url.clone()),
        false,
    )
}

/// `github.SplitRepositoryNameWithOwner`.
pub fn split_repository_name_with_owner(name_with_owner: &str) -> Result<(String, String), String> {
    let mut parts = name_with_owner.splitn(2, '/');
    let owner = parts.next().unwrap_or_default();
    let name = parts.next();
    match name {
        Some(name) if !name.contains('/') && !owner.is_empty() && !name.is_empty() => {
            Ok((owner.to_owned(), name.to_owned()))
        }
        _ => Err(format!(
            "invalid GitHub repository \"owner/name\" string: {name_with_owner:?}"
        )),
    }
}

/// `github.ExternalRepoSpec` inputs: the GraphQL id and normalized base URL.
pub fn external_repo_spec(repo: &Repository, base_url: &Url) -> (String, String) {
    (repo.id.clone(), normalize_base_url(base_url).to_string())
}

/// `extsvc.NormalizeBaseURL`: lowercase the host and keep a trailing `/`.
pub fn normalize_base_url(base_url: &Url) -> Url {
    let mut url = base_url.clone();
    if let Some(host) = url.host_str() {
        let host = host.to_ascii_lowercase();
        let _ = url.set_host(Some(&host));
    }
    let mut path = url.path().to_owned();
    if !path.ends_with('/') {
        path.push('/');
    }
    url.set_path(&path);
    url
}

/// `github.RepositoryTopic` + `Topic`, keeping the GraphQL shape.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RepositoryTopic {
    #[serde(default)]
    pub topic: Topic,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Topic {
    #[serde(default)]
    pub name: String,
}

/// `github.ParentRepository`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ParentRepository {
    #[serde(rename = "nameWithOwner", default)]
    pub name_with_owner: String,
    #[serde(rename = "isFork", default)]
    pub is_fork: bool,
}

/// `github.Repository`. Field names mirror the Go struct because the syncer
/// stores this value as `repo.metadata`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Repository {
    pub id: String,
    pub database_id: i64,
    pub name_with_owner: String,
    pub description: String,
    pub url: String,
    pub is_private: bool,
    pub is_fork: bool,
    pub is_archived: bool,
    pub is_locked: bool,
    pub is_disabled: bool,
    pub viewer_permission: String,
    pub repository_topics: Vec<RepositoryTopic>,
    pub stargazer_count: i32,
    pub fork_count: i32,
    pub visibility: String,
    pub disk_usage_kibibytes: i64,
    pub parent: Option<ParentRepository>,
}

/// `github.PublicRepository`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PublicRepository {
    pub id: String,
    pub database_id: i64,
    pub name_with_owner: String,
    pub description: String,
    pub url: String,
    pub is_private: bool,
    pub is_fork: bool,
}

impl Repository {
    pub fn size_bytes(&self) -> i64 {
        self.disk_usage_kibibytes * 1024
    }
}

/// `github.RestRepository` (`restRepository` in the baseline).
#[derive(Debug, Clone, Default, Deserialize)]
pub struct RestRepository {
    #[serde(default)]
    pub node_id: String,
    #[serde(default)]
    pub id: i64,
    #[serde(default)]
    pub full_name: String,
    #[serde(default)]
    pub description: String,
    #[serde(default)]
    pub html_url: String,
    #[serde(default)]
    pub private: bool,
    #[serde(default)]
    pub fork: bool,
    #[serde(default)]
    pub archived: bool,
    #[serde(default)]
    pub locked: bool,
    #[serde(default)]
    pub disabled: bool,
    #[serde(default)]
    pub permissions: RestRepositoryPermissions,
    #[serde(default)]
    pub stargazers_count: i32,
    #[serde(default)]
    pub forks_count: i32,
    #[serde(default)]
    pub visibility: String,
    #[serde(default)]
    pub topics: Vec<String>,
    #[serde(default)]
    pub size: i64,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct RestRepositoryPermissions {
    #[serde(default)]
    pub admin: bool,
    #[serde(default)]
    pub push: bool,
    #[serde(default)]
    pub pull: bool,
}

/// `github.RestPublicRepository`.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct RestPublicRepository {
    #[serde(default)]
    pub node_id: String,
    #[serde(default)]
    pub id: i64,
    #[serde(default)]
    pub full_name: String,
    #[serde(default)]
    pub description: String,
    #[serde(default)]
    pub html_url: String,
    #[serde(default)]
    pub private: bool,
    #[serde(default)]
    pub fork: bool,
}

/// `github.restCommitTree` (REST commit tree object).
#[derive(Debug, Clone, Default, Deserialize, PartialEq, Eq)]
pub struct RestCommitTree {
    #[serde(default)]
    pub url: String,
    #[serde(default)]
    pub sha: String,
}

/// `github.restCommitParent`.
#[derive(Debug, Clone, Default, Deserialize, PartialEq, Eq)]
pub struct RestCommitParent {
    #[serde(default)]
    pub url: String,
    #[serde(default)]
    pub sha: String,
}

/// `github.restAuthorCommiter`.
#[derive(Debug, Clone, Default, Deserialize, PartialEq, Eq)]
pub struct RestAuthorCommitter {
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub email: String,
    #[serde(default)]
    pub date: String,
}

/// `github.Verification` (REST commit verification object).
#[derive(Debug, Clone, Default, Deserialize, PartialEq, Eq)]
pub struct Verification {
    #[serde(default)]
    pub verified: bool,
    #[serde(default)]
    pub reason: String,
    #[serde(default)]
    pub signature: String,
    #[serde(default)]
    pub payload: String,
}

/// The nested `commit` object of [`RestCommitRef`].
#[derive(Debug, Clone, Default, Deserialize, PartialEq, Eq)]
pub struct RestCommitRefCommit {
    #[serde(default)]
    pub url: String,
    #[serde(default)]
    pub author: Option<RestAuthorCommitter>,
    #[serde(default)]
    pub committer: Option<RestAuthorCommitter>,
    #[serde(default)]
    pub message: String,
    #[serde(default)]
    pub tree: RestCommitTree,
}

/// `github.restCommitRef` — from `GET repos/{owner}/{repo}/commits/{ref}`.
#[derive(Debug, Clone, Default, Deserialize, PartialEq, Eq)]
pub struct RestCommitRef {
    #[serde(default)]
    pub url: String,
    #[serde(default)]
    pub sha: String,
    #[serde(default)]
    pub node_id: String,
    #[serde(default)]
    pub commit: RestCommitRefCommit,
    #[serde(default)]
    pub parents: Vec<RestCommitParent>,
}

/// `github.RestCommit` — from `POST repos/{owner}/{repo}/git/commits`.
#[derive(Debug, Clone, Default, Deserialize, PartialEq, Eq)]
pub struct RestCommit {
    #[serde(default)]
    pub url: String,
    #[serde(default)]
    pub sha: String,
    #[serde(default)]
    pub node_id: String,
    #[serde(default)]
    pub author: Option<RestAuthorCommitter>,
    #[serde(default)]
    pub committer: Option<RestAuthorCommitter>,
    #[serde(default)]
    pub message: String,
    #[serde(default)]
    pub tree: RestCommitTree,
    #[serde(default)]
    pub parents: Vec<RestCommitParent>,
    #[serde(default)]
    pub verification: Verification,
}

/// `github.restUpdatedRef.object`.
#[derive(Debug, Clone, Default, Deserialize, PartialEq, Eq)]
pub struct RestUpdatedRefObject {
    #[serde(rename = "type", default)]
    pub type_: String,
    #[serde(default)]
    pub sha: String,
    #[serde(default)]
    pub url: String,
}

/// `github.restUpdatedRef` — from `PATCH repos/{owner}/{repo}/git/{ref}`.
#[derive(Debug, Clone, Default, Deserialize, PartialEq, Eq)]
pub struct RestUpdatedRef {
    #[serde(default)]
    pub r#ref: String,
    #[serde(default)]
    pub node_id: String,
    #[serde(default)]
    pub url: String,
    #[serde(default)]
    pub object: RestUpdatedRefObject,
}

/// `github.convertRestRepoPermissions`.
fn convert_rest_repo_permissions(permissions: &RestRepositoryPermissions) -> String {
    if permissions.admin {
        "ADMIN"
    } else if permissions.push {
        "WRITE"
    } else if permissions.pull {
        "READ"
    } else {
        ""
    }
    .to_owned()
}

/// `github.convertRestRepo`.
pub fn convert_rest_repo(rest: RestRepository) -> Repository {
    let topics = rest
        .topics
        .into_iter()
        .map(|name| RepositoryTopic {
            topic: Topic { name },
        })
        .collect();
    Repository {
        id: rest.node_id,
        database_id: rest.id,
        name_with_owner: rest.full_name,
        description: rest.description,
        url: rest.html_url,
        is_private: rest.private,
        is_fork: rest.fork,
        is_archived: rest.archived,
        is_locked: rest.locked,
        is_disabled: rest.disabled,
        viewer_permission: convert_rest_repo_permissions(&rest.permissions),
        repository_topics: topics,
        stargazer_count: rest.stargazers_count,
        fork_count: rest.forks_count,
        visibility: rest.visibility,
        disk_usage_kibibytes: rest.size,
        parent: None,
    }
}

/// `github.convertRestPublicRepo`.
pub fn convert_rest_public_repo(rest: RestPublicRepository) -> PublicRepository {
    PublicRepository {
        id: rest.node_id,
        database_id: rest.id,
        name_with_owner: rest.full_name,
        description: rest.description,
        url: rest.html_url,
        is_private: rest.private,
        is_fork: rest.fork,
    }
}

/// `github.APIError`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ApiError {
    pub url: String,
    pub code: u16,
    pub message: String,
    pub documentation_url: String,
}

impl fmt::Display for ApiError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "request to {} returned status {}: {}",
            self.url, self.code, self.message
        )
    }
}

impl ApiError {
    pub fn unauthorized(&self) -> bool {
        self.code == 401
    }

    pub fn account_suspended(&self) -> bool {
        self.code == 403 && self.message.contains("account was suspended")
    }
}

/// `github.graphqlError`.
#[derive(Debug, Clone, Default, Deserialize, PartialEq, Eq)]
pub struct GraphQlError {
    #[serde(default)]
    pub message: String,
    #[serde(rename = "type", default)]
    pub type_: String,
    #[serde(default)]
    pub path: Vec<Value>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct GraphQlErrors(pub Vec<GraphQlError>);

impl fmt::Display for GraphQlErrors {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "error in GraphQL response: {}",
            self.0.first().map(|err| err.message.as_str()).unwrap_or("")
        )
    }
}

pub const GRAPHQL_ERR_TYPE_NOT_FOUND: &str = "NOT_FOUND";
pub const GRAPHQL_ERR_TYPE_RATE_LIMITED: &str = "RATE_LIMITED";

/// Errors returned by the GitHub clients.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GithubError {
    Api(ApiError),
    GraphQl {
        errors: GraphQlErrors,
        data: Option<Value>,
    },
    Http(String),
    Json(String),
    /// `github.ErrBatchTooLarge`.
    BatchTooLarge,
    /// `github.ErrIncompleteResults`.
    IncompleteResults,
    /// `github.ErrRepoNotFound`.
    RepoNotFound,
}

impl fmt::Display for GithubError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            GithubError::Api(err) => write!(f, "{err}"),
            GithubError::GraphQl { errors, .. } => write!(f, "{errors}"),
            GithubError::Http(err) | GithubError::Json(err) => f.write_str(err),
            GithubError::BatchTooLarge => {
                f.write_str("requested batch of GitHub repositories too large")
            }
            GithubError::IncompleteResults => f.write_str(
                "github repository search returned incomplete results. This is an ephemeral \
                 error from GitHub, so does not indicate a problem with your configuration. \
                 See https://developer.github.com/changes/2014-04-07-understanding-search-results-and-potential-timeouts/ for more information",
            ),
            GithubError::RepoNotFound => f.write_str("GitHub repository not found"),
        }
    }
}

/// `github.HTTPErrorCode`.
pub fn http_error_code(err: &GithubError) -> u16 {
    match err {
        GithubError::Api(api) => api.code,
        _ => 0,
    }
}

/// `github.IsNotFound`.
pub fn is_not_found(err: &GithubError) -> bool {
    match err {
        GithubError::RepoNotFound => true,
        GithubError::Api(api) => api.code == 404,
        GithubError::GraphQl { errors, .. } => errors
            .0
            .iter()
            .any(|err| err.type_ == GRAPHQL_ERR_TYPE_NOT_FOUND),
        _ => false,
    }
}

/// `github.IsRateLimitExceeded`.
pub fn is_rate_limit_exceeded(err: &GithubError) -> bool {
    match err {
        GithubError::Api(api) => {
            api.message.contains("API rate limit exceeded")
                || api.documentation_url.contains("#rate-limiting")
        }
        GithubError::GraphQl { errors, .. } => errors.0.iter().any(|err| {
            err.type_ == GRAPHQL_ERR_TYPE_RATE_LIMITED
                || err.message.contains("API rate limit exceeded")
        }),
        _ => false,
    }
}

/// `github.httpResponseState`.
#[derive(Debug, Clone, Default)]
pub struct ResponseState {
    pub status: u16,
    pub headers: Vec<(String, String)>,
}

impl ResponseState {
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(key, _)| key.eq_ignore_ascii_case(name))
            .map(|(_, value)| value.as_str())
    }

    /// `httpResponseState.hasNextPage`.
    pub fn has_next_page(&self) -> bool {
        self.header("Link")
            .is_some_and(|link| link.contains("rel=\"next\""))
    }
}

/// The subset of `ratelimit.Monitor` this port keeps: the latest headers.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RateLimitState {
    pub remaining: i64,
    pub limit: i64,
    pub used: i64,
    pub reset: Option<OffsetDateTime>,
    pub retry_after: Option<time::Duration>,
    /// `Monitor.known`: both `RateLimit-Limit` and `RateLimit-Remaining`
    /// parsed successfully in the latest response.
    pub known: bool,
    /// `Monitor.retry`, the absolute `Retry-After` deadline.
    pub retry_at: Option<OffsetDateTime>,
}

impl RateLimitState {
    /// `Monitor.Update`.
    pub fn update(&mut self, state: &ResponseState) {
        let now = OffsetDateTime::now_utc();
        if let Some(value) = state.header("X-RateLimit-Remaining") {
            if let Ok(remaining) = value.trim().parse() {
                self.remaining = remaining;
            }
        }
        if let Some(value) = state.header("X-RateLimit-Limit") {
            if let Ok(limit) = value.trim().parse() {
                self.limit = limit;
            }
        }
        if let Some(value) = state.header("X-RateLimit-Used") {
            if let Ok(used) = value.trim().parse() {
                self.used = used;
            }
        }
        if let Some(value) = state.header("X-RateLimit-Reset") {
            if let Ok(epoch) = value.trim().parse::<i64>() {
                self.reset = OffsetDateTime::from_unix_timestamp(epoch).ok();
            }
        }
        if let Some(value) = state.header("Retry-After") {
            if let Ok(seconds) = value.trim().parse::<i64>() {
                if seconds > 0 {
                    self.retry_after = Some(time::Duration::seconds(seconds));
                    self.retry_at = Some(now + time::Duration::seconds(seconds));
                }
            }
        }

        // `Monitor.Update` marks the monitor unknown unless all three headers
        // parse; `X-RateLimit-Used` is informational only.
        let known = state
            .header("X-RateLimit-Limit")
            .and_then(|value| value.trim().parse::<i64>().ok())
            .is_some()
            && state
                .header("X-RateLimit-Remaining")
                .and_then(|value| value.trim().parse::<i64>().ok())
                .is_some()
            && state
                .header("X-RateLimit-Reset")
                .and_then(|value| value.trim().parse::<i64>().ok())
                .is_some();
        self.known = known;
    }

    /// `Monitor.calcRateLimitWaitTime`, in nanoseconds (Go `time.Duration`).
    /// Returns 0 when no wait is warranted.
    pub fn calc_rate_limit_wait_time(&self, cost: i64) -> i64 {
        let now = OffsetDateTime::now_utc();
        if let Some(retry_at) = self.retry_at {
            let remaining = retry_at - now;
            if remaining > time::Duration::ZERO {
                return remaining.whole_nanoseconds() as i64;
            }
        }

        // Unknown limit, enough remaining tokens, or a cost larger than the
        // entire limit (there will never be enough): no wait.
        if !self.known || self.remaining >= cost || cost > self.limit {
            return 0;
        }

        // Only wait when the reset is still in the future; a past reset is
        // stale information.
        if let Some(reset) = self.reset {
            let remaining = reset - now;
            if remaining > time::Duration::ZERO {
                return remaining.whole_nanoseconds() as i64;
            }
        }

        0
    }
}

fn go_path_join(base: &str, request: &str) -> String {
    let mut parts: Vec<&str> = Vec::new();
    for part in base.split('/').chain(request.split('/')) {
        match part {
            "" | "." => {}
            ".." => {
                parts.pop();
            }
            other => parts.push(other),
        }
    }
    let joined = parts.join("/");
    if base.starts_with('/') {
        format!("/{joined}")
    } else {
        joined
    }
}

/// Split a request URI the way `http.NewRequest` does (path and raw query).
fn split_request_uri(request_uri: &str) -> (&str, Option<&str>) {
    let without_fragment = request_uri
        .split_once('#')
        .map(|(before, _)| before)
        .unwrap_or(request_uri);
    match without_fragment.split_once('?') {
        Some((path, query)) => (path, Some(query)),
        None => (without_fragment, None),
    }
}

/// `doRequest`'s URL rewrite: `path.Join(apiURL.Path, req.URL.Path)` then
/// `apiURL.ResolveReference`.
fn resolve_request_url(api_url: &Url, request_uri: &str) -> Result<Url, GithubError> {
    let (path, query) = split_request_uri(request_uri);
    let mut url = api_url.clone();
    url.set_path(&go_path_join(api_url.path(), path));
    url.set_query(query.filter(|query| !query.is_empty()));
    Ok(url)
}

struct RequestParts {
    url: Url,
    headers: Vec<(String, String)>,
}

fn build_request(
    api_url: &Url,
    auth: Option<&Arc<dyn Authenticator>>,
    method: &str,
    request_uri: &str,
    body: &[u8],
    graphql: bool,
) -> Result<(RequestParts, Vec<u8>), GithubError> {
    let url = resolve_request_url(api_url, request_uri)?;
    let mut headers = Vec::new();
    add_header(
        &mut headers,
        "Content-Type",
        "application/json; charset=utf-8",
    );
    add_header(&mut headers, "Cache-Control", "max-age=0");
    if graphql {
        // `requestGraphQL` enables the Checks API preview.
        add_header(
            &mut headers,
            "Accept",
            "application/vnd.github.antiope-preview+json",
        );
    } else {
        // `V3Client.request` previews (node ids, topics, GitHub Apps).
        add_header(
            &mut headers,
            "Accept",
            "application/vnd.github.jean-grey-preview+json,application/vnd.github.mercy-preview+json",
        );
        add_header(
            &mut headers,
            "Accept",
            "application/vnd.github.machine-man-preview+json",
        );
    }
    if let Some(auth) = auth {
        auth.authenticate(method, url.as_str(), &mut headers)
            .map_err(GithubError::Http)?;
    }
    Ok((RequestParts { url, headers }, body.to_vec()))
}

fn parse_api_error(state: &ResponseState, url: &str, body: &[u8]) -> ApiError {
    #[derive(Deserialize)]
    struct Body {
        #[serde(default)]
        message: String,
        #[serde(default)]
        documentation_url: String,
    }
    let limited = &body[..body.len().min(8192)];
    let mut err = ApiError {
        url: url.to_owned(),
        code: state.status,
        message: String::new(),
        documentation_url: String::new(),
    };
    match serde_json::from_slice::<Body>(limited) {
        Ok(decoded) => {
            err.message = decoded.message;
            err.documentation_url = decoded.documentation_url;
        }
        Err(dec_err) => {
            err.message = format!(
                "failed to decode error response from GitHub API: {dec_err}: {:?}",
                String::from_utf8_lossy(limited)
            );
        }
    }
    err
}

async fn send(
    http: &Arc<dyn HttpDoer>,
    api_url: &Url,
    auth: Option<&Arc<dyn Authenticator>>,
    method: &str,
    request_uri: &str,
    body: Vec<u8>,
    graphql: bool,
    rate_limit: &std::sync::Mutex<RateLimitState>,
) -> Result<(ResponseState, Vec<u8>), GithubError> {
    let (parts, body) = build_request(api_url, auth, method, request_uri, &body, graphql)?;
    let response = http
        .do_request(crate::HttpRequest {
            method: method.to_owned(),
            url: parts.url.to_string(),
            headers: parts.headers,
            body,
        })
        .await
        .map_err(|err| GithubError::Http(format!("request failed: {err}")))?;
    let state = ResponseState {
        status: response.status,
        headers: response.headers,
    };
    // `doRequest` updates the rate limit monitor before it classifies the
    // response as an error, and skips 401s (their remaining count is 0 and
    // would block for an hour).
    if state.status != 401 {
        rate_limit.lock().expect("rate limit state").update(&state);
    }
    if state.status < 200 || state.status >= 400 {
        return Err(GithubError::Api(parse_api_error(
            &state,
            parts.url.as_str(),
            &response.body,
        )));
    }
    Ok((state, response.body))
}

/// `github.V3Client`.
pub struct V3Client {
    api_url: Url,
    github_dot_com: bool,
    auth: Option<Arc<dyn Authenticator>>,
    http: Arc<dyn HttpDoer>,
    resource: String,
    state: std::sync::Mutex<RateLimitState>,
}

impl V3Client {
    /// `github.NewV3Client`.
    pub fn new(
        api_url: Url,
        auth: Option<Arc<dyn Authenticator>>,
        http: Arc<dyn HttpDoer>,
    ) -> Self {
        Self::with_resource(api_url, auth, http, "rest")
    }

    /// `github.NewV3SearchClient`.
    pub fn new_search(
        api_url: Url,
        auth: Option<Arc<dyn Authenticator>>,
        http: Arc<dyn HttpDoer>,
    ) -> Self {
        Self::with_resource(api_url, auth, http, "search")
    }

    fn with_resource(
        api_url: Url,
        auth: Option<Arc<dyn Authenticator>>,
        http: Arc<dyn HttpDoer>,
        resource: &str,
    ) -> Self {
        let api_url = canonicalized_url(&api_url);
        Self {
            github_dot_com: url_is_github_dot_com(&api_url),
            api_url,
            auth,
            http,
            resource: resource.to_owned(),
            state: std::sync::Mutex::new(RateLimitState::default()),
        }
    }

    /// `V3Client.WithAuthenticator`.
    pub fn with_authenticator(&self, auth: Arc<dyn Authenticator>) -> Self {
        Self::with_resource(
            self.api_url.clone(),
            Some(auth),
            self.http.clone(),
            &self.resource,
        )
    }

    /// `V3Client.ExternalRateLimiter().Get()`.
    pub fn rate_limit_state(&self) -> RateLimitState {
        self.state.lock().expect("rate limit state").clone()
    }

    /// `Monitor.WaitForRateLimit`: sleeps when the latest rate-limit headers
    /// warrant it and reports whether a wait happened.
    async fn wait_for_rate_limit(&self, cost: i64) -> bool {
        let wait = self
            .state
            .lock()
            .expect("rate limit state")
            .calc_rate_limit_wait_time(cost);
        if wait <= 0 {
            return false;
        }
        tokio::time::sleep(std::time::Duration::from_nanos(wait as u64)).await;
        true
    }

    async fn get(&self, request_uri: &str) -> Result<(ResponseState, Vec<u8>), GithubError> {
        self.request("GET", request_uri, Vec::new()).await
    }

    /// Kept for the unported write APIs (`internal/extsvc/github` posts
    /// mutations); the source only reads.
    #[allow(dead_code)]
    async fn post(
        &self,
        request_uri: &str,
        payload: &Value,
    ) -> Result<(ResponseState, Vec<u8>), GithubError> {
        let body = serde_json::to_vec(payload)
            .map_err(|err| GithubError::Json(format!("marshalling payload: {err}")))?;
        self.request("POST", request_uri, body).await
    }

    async fn request(
        &self,
        method: &str,
        request_uri: &str,
        body: Vec<u8>,
    ) -> Result<(ResponseState, Vec<u8>), GithubError> {
        // `V3Client.request`: the external rate limiter's preventative wait,
        // then retry 403s while `WaitForRateLimit` says the limit is being
        // applied, up to `maxRateLimitRetries`.
        const MAX_RATE_LIMIT_RETRIES: i32 = 2;

        self.wait_for_rate_limit(1).await;

        let mut response = send(
            &self.http,
            &self.api_url,
            self.auth.as_ref(),
            method,
            request_uri,
            body.clone(),
            false,
            &self.state,
        )
        .await;

        let mut num_retries = 0;
        while num_retries < MAX_RATE_LIMIT_RETRIES
            && matches!(&response, Err(GithubError::Api(api)) if api.code == 403)
        {
            // The response's rate-limit headers were already recorded by
            // `send`; a wait here means the 403 really is rate limiting.
            if !self.wait_for_rate_limit(1).await {
                break;
            }
            response = send(
                &self.http,
                &self.api_url,
                self.auth.as_ref(),
                method,
                request_uri,
                body.clone(),
                false,
                &self.state,
            )
            .await;
            num_retries += 1;
        }

        response
    }

    async fn get_json<T: serde::de::DeserializeOwned>(
        &self,
        request_uri: &str,
    ) -> Result<(ResponseState, Option<T>), GithubError> {
        let (state, body) = self.get(request_uri).await?;
        if state.status == 204 || state.status == 304 {
            return Ok((state, None));
        }
        if body.is_empty() {
            // `json.NewDecoder(...).Decode` returns io.EOF ("EOF") for an
            // empty 200 body; the port must not silently treat it as null.
            return Err(GithubError::Json("EOF".to_owned()));
        }
        let value = serde_json::from_slice(&body)
            .map_err(|err| GithubError::Json(format!("json: {err}")))?;
        Ok((state, Some(value)))
    }

    /// `V3Client.GetVersion`.
    pub async fn get_version(&self) -> Result<String, GithubError> {
        if self.github_dot_com {
            return Ok("unknown".to_owned());
        }
        let (state, _) = self.get("/").await?;
        Ok(state
            .header("X-GitHub-Enterprise-Version")
            .unwrap_or_default()
            .to_owned())
    }

    /// `V3Client.GetAuthenticatedUser`.
    pub async fn get_authenticated_user(&self) -> Result<User, GithubError> {
        let (_, user) = self.get_json::<User>("/user").await?;
        Ok(user.unwrap_or_default())
    }

    /// `V3Client.GetAuthenticatedOAuthScopes`.
    pub async fn get_authenticated_oauth_scopes(&self) -> Result<Vec<String>, GithubError> {
        let (state, _) = self.get("/").await?;
        let scope = state.header("X-OAuth-Scopes").unwrap_or_default();
        if scope.is_empty() {
            return Ok(Vec::new());
        }
        Ok(scope.split(", ").map(str::to_owned).collect())
    }

    /// `V3Client.GetAuthenticatedUserOrgs`.
    pub async fn get_authenticated_user_orgs(
        &self,
        page: i32,
    ) -> Result<(Vec<Org>, bool), GithubError> {
        let (state, orgs) = self
            .get_json::<Vec<Org>>(&format!("/user/orgs?per_page=100&page={page}"))
            .await?;
        Ok((orgs.unwrap_or_default(), state.has_next_page()))
    }

    /// `V3Client.ListOrgRepositories`.
    pub async fn list_org_repositories(
        &self,
        org: &str,
        page: i32,
        repo_type: &str,
    ) -> Result<(Vec<Repository>, bool), GithubError> {
        self.list_repositories(&format!(
            "orgs/{org}/repos?sort=created&page={page}&per_page=100&type={repo_type}"
        ))
        .await
    }

    /// `V3Client.ListUserRepositories`.
    pub async fn list_user_repositories(
        &self,
        user: &str,
        page: i32,
    ) -> Result<(Vec<Repository>, bool), GithubError> {
        self.list_repositories(&format!(
            "users/{user}/repos?sort=created&type=owner&page={page}&per_page=100"
        ))
        .await
    }

    /// `V3Client.ListAffiliatedRepositories`.
    pub async fn list_affiliated_repositories(
        &self,
        visibility: &str,
        page: i32,
        per_page: i32,
    ) -> Result<(Vec<Repository>, bool), GithubError> {
        self.list_repositories(&format!(
            "user/repos?sort=created&visibility={visibility}&page={page}&per_page={per_page}"
        ))
        .await
    }

    /// `V3Client.ListInstallationRepositories`.
    pub async fn list_installation_repositories(
        &self,
        page: i32,
    ) -> Result<(Vec<Repository>, bool), GithubError> {
        #[derive(Deserialize, Default)]
        struct Response {
            #[serde(default)]
            repositories: Vec<RestRepository>,
        }
        let (state, response) = self
            .get_json::<Response>(&format!(
                "installation/repositories?page={page}&per_page=100"
            ))
            .await?;
        let repos = response
            .unwrap_or_default()
            .repositories
            .into_iter()
            .map(convert_rest_repo)
            .collect();
        Ok((repos, state.has_next_page()))
    }

    /// `V3Client.ListPublicRepositories`.
    pub async fn list_public_repositories(
        &self,
        since_repo_id: i64,
    ) -> Result<(Vec<PublicRepository>, bool), GithubError> {
        let path = if since_repo_id > 0 {
            format!("repositories?per_page=100&since={since_repo_id}")
        } else {
            "repositories".to_owned()
        };
        let (state, rest) = self.get_json::<Vec<RestPublicRepository>>(&path).await?;
        let repos = rest
            .unwrap_or_default()
            .into_iter()
            .filter(|repo| !repo.node_id.is_empty())
            .map(convert_rest_public_repo)
            .collect();
        Ok((repos, state.has_next_page()))
    }

    async fn list_repositories(
        &self,
        request_uri: &str,
    ) -> Result<(Vec<Repository>, bool), GithubError> {
        let (state, rest) = self.get_json::<Vec<RestRepository>>(request_uri).await?;
        let repos = rest
            .unwrap_or_default()
            .into_iter()
            // GitHub sometimes returns null entries; Go skips zero-ID repos.
            .filter(|repo| !repo.node_id.is_empty())
            .map(convert_rest_repo)
            .collect();
        Ok((repos, state.has_next_page()))
    }

    /// `V3Client.GetRepository` / `getRepositoryFromAPI`.
    pub async fn get_repository(&self, owner: &str, name: &str) -> Result<Repository, GithubError> {
        match self
            .get_json::<RestRepository>(&format!("/repos/{owner}/{name}"))
            .await
        {
            Ok((_, Some(repo))) => Ok(convert_rest_repo(repo)),
            Ok((_, None)) => Ok(Repository::default()),
            Err(err) if http_error_code(&err) == 404 => Err(GithubError::RepoNotFound),
            Err(err) => Err(err),
        }
    }

    /// `V3Client.GetRepo`.
    pub async fn get_repo(&self, owner: &str, name: &str) -> Result<Repository, GithubError> {
        let (_, rest) = self
            .get_json::<RestRepository>(&format!("repos/{owner}/{name}"))
            .await?;
        Ok(convert_rest_repo(rest.unwrap_or_default()))
    }

    /// `V3Client.ListRepositoriesForSearch`.
    pub async fn list_repositories_for_search(
        &self,
        search_string: &str,
        page: i32,
    ) -> Result<RepositoryListPage, GithubError> {
        // `url.Values.Encode` sorts the keys: page, per_page, q.
        let encoded = url::form_urlencoded::Serializer::new(String::new())
            .append_pair("page", &page.to_string())
            .append_pair("per_page", "100")
            .append_pair("q", search_string)
            .finish();
        let (_, response) = self
            .get_json::<RestSearchResponse>(&format!("search/repositories?{encoded}"))
            .await?;
        let response = response.unwrap_or_default();
        if response.incomplete_results {
            return Err(GithubError::IncompleteResults);
        }
        let repos = response.items.into_iter().map(convert_rest_repo).collect();
        Ok(RepositoryListPage {
            total_count: response.total_count,
            repos,
            has_next_page: page * 100 < response.total_count,
        })
    }

    /// `V3Client.patch`: the `PATCH` verb, used by `UpdateRef`.
    async fn patch(
        &self,
        request_uri: &str,
        payload: &Value,
    ) -> Result<(ResponseState, Vec<u8>), GithubError> {
        let body = serde_json::to_vec(payload)
            .map_err(|err| GithubError::Json(format!("marshalling payload: {err}")))?;
        self.request("PATCH", request_uri, body).await
    }

    /// `V3Client.GetRef`: `GET repos/{owner}/{repo}/commits/{ref}`.
    pub async fn get_ref(
        &self,
        owner: &str,
        repo: &str,
        reference: &str,
    ) -> Result<RestCommitRef, GithubError> {
        let (_, commit) = self
            .get_json::<RestCommitRef>(&format!("repos/{owner}/{repo}/commits/{reference}"))
            .await?;
        Ok(commit.unwrap_or_default())
    }

    /// `V3Client.CreateCommit`: `POST repos/{owner}/{repo}/git/commits`.
    #[allow(clippy::too_many_arguments)]
    pub async fn create_commit(
        &self,
        owner: &str,
        repo: &str,
        message: &str,
        tree: &str,
        parents: &[String],
        author: Option<&RestAuthorCommitter>,
        committer: Option<&RestAuthorCommitter>,
    ) -> Result<RestCommit, GithubError> {
        // `encoding/json` with `omitempty` drops nil author/committer.
        let mut payload = serde_json::Map::new();
        payload.insert("message".into(), json!(message));
        payload.insert("tree".into(), json!(tree));
        payload.insert("parents".into(), json!(parents));
        if let Some(author) = author {
            payload.insert(
                "author".into(),
                json!({"name": author.name, "email": author.email, "date": author.date}),
            );
        }
        if let Some(committer) = committer {
            payload.insert(
                "committer".into(),
                json!({"name": committer.name, "email": committer.email, "date": committer.date}),
            );
        }
        let (_, commit) = self
            .post_json::<RestCommit>(
                &format!("repos/{owner}/{repo}/git/commits"),
                &Value::Object(payload),
            )
            .await?;
        Ok(commit.unwrap_or_default())
    }

    /// `V3Client.UpdateRef`: `PATCH repos/{owner}/{repo}/git/{ref}` with
    /// `{sha, force: true}`.
    pub async fn update_ref(
        &self,
        owner: &str,
        repo: &str,
        reference: &str,
        commit: &str,
    ) -> Result<RestUpdatedRef, GithubError> {
        let payload = json!({"sha": commit, "force": true});
        let (_, updated) = self
            .patch_json::<RestUpdatedRef>(
                &format!("repos/{owner}/{repo}/git/{reference}"),
                &payload,
            )
            .await?;
        Ok(updated.unwrap_or_default())
    }

    async fn post_json<T: serde::de::DeserializeOwned>(
        &self,
        request_uri: &str,
        payload: &Value,
    ) -> Result<(ResponseState, Option<T>), GithubError> {
        let (state, body) = self.post(request_uri, payload).await?;
        decode_json::<T>(state, body)
    }

    async fn patch_json<T: serde::de::DeserializeOwned>(
        &self,
        request_uri: &str,
        payload: &Value,
    ) -> Result<(ResponseState, Option<T>), GithubError> {
        let (state, body) = self.patch(request_uri, payload).await?;
        decode_json::<T>(state, body)
    }
}

/// `decode_json` mirrors `get_json`'s empty-body handling for the REST write
/// endpoints.
fn decode_json<T: serde::de::DeserializeOwned>(
    state: ResponseState,
    body: Vec<u8>,
) -> Result<(ResponseState, Option<T>), GithubError> {
    if state.status == 204 || state.status == 304 {
        return Ok((state, None));
    }
    if body.is_empty() {
        return Err(GithubError::Json("EOF".to_owned()));
    }
    let value = serde_json::from_slice(&body)
        .map_err(|err| GithubError::Json(format!("json: {err}")))?;
    Ok((state, Some(value)))
}

/// `github.User`.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct User {
    #[serde(default)]
    pub login: String,
    #[serde(default)]
    pub id: i64,
    #[serde(default)]
    pub node_id: String,
    #[serde(default)]
    pub avatar_url: String,
    #[serde(default)]
    pub url: String,
}

/// `github.Org`.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct Org {
    #[serde(default)]
    pub login: String,
    #[serde(default)]
    pub id: i64,
    #[serde(default)]
    pub node_id: String,
}

/// `github.RepositoryListPage`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RepositoryListPage {
    pub total_count: i32,
    pub repos: Vec<Repository>,
    pub has_next_page: bool,
}

#[derive(Debug, Clone, Default, Deserialize)]
struct RestSearchResponse {
    #[serde(default)]
    total_count: i32,
    #[serde(default)]
    incomplete_results: bool,
    #[serde(default)]
    items: Vec<RestRepository>,
}

/// `github.V4Client`.
pub struct V4Client {
    api_url: Url,
    github_dot_com: bool,
    auth: Option<Arc<dyn Authenticator>>,
    http: Arc<dyn HttpDoer>,
    state: std::sync::Mutex<RateLimitState>,
    /// `determineGitHubVersion` cache: `None` before the first fetch.
    version: tokio::sync::Mutex<Option<Option<String>>>,
}

impl V4Client {
    /// `github.NewV4Client`.
    pub fn new(
        api_url: Url,
        auth: Option<Arc<dyn Authenticator>>,
        http: Arc<dyn HttpDoer>,
    ) -> Self {
        let api_url = canonicalized_url(&api_url);
        Self {
            github_dot_com: url_is_github_dot_com(&api_url),
            api_url,
            auth,
            http,
            state: std::sync::Mutex::new(RateLimitState::default()),
            version: tokio::sync::Mutex::new(None),
        }
    }

    /// `V4Client.WithAuthenticator`.
    pub fn with_authenticator(&self, auth: Arc<dyn Authenticator>) -> Self {
        Self::new(self.api_url.clone(), Some(auth), self.http.clone())
    }

    /// `V4Client.ExternalRateLimiter().Get()`.
    pub fn rate_limit_state(&self) -> RateLimitState {
        self.state.lock().expect("rate limit state").clone()
    }

    async fn request_graphql(&self, query: &str, variables: Value) -> Result<Value, GithubError> {
        let endpoint = if self.github_dot_com {
            "/graphql"
        } else {
            "../graphql"
        };
        let payload = json!({"query": query, "variables": variables});
        let body = serde_json::to_vec(&payload)
            .map_err(|err| GithubError::Json(format!("marshalling payload: {err}")))?;
        let (_, body) = send(
            &self.http,
            &self.api_url,
            self.auth.as_ref(),
            "POST",
            endpoint,
            body,
            true,
            &self.state,
        )
        .await?;
        #[derive(Deserialize, Default)]
        struct Response {
            #[serde(default)]
            data: Option<Value>,
            #[serde(default)]
            errors: Vec<GraphQlError>,
        }
        let response: Response = if body.is_empty() {
            // `doRequest` decodes with `json.Decoder`, which yields EOF.
            return Err(GithubError::Json("EOF".to_owned()));
        } else {
            serde_json::from_slice(&body)
                .map_err(|err| GithubError::Json(format!("json: {err}")))?
        };
        if !response.errors.is_empty() {
            return Err(GithubError::GraphQl {
                errors: GraphQlErrors(response.errors),
                data: response.data,
            });
        }
        Ok(response.data.unwrap_or(Value::Null))
    }

    /// `V4Client.GetAuthenticatedUser`.
    pub async fn get_authenticated_user(&self) -> Result<Actor, GithubError> {
        #[derive(Deserialize, Default)]
        struct Response {
            #[serde(default)]
            viewer: Actor,
        }
        let data = self
            .request_graphql(
                "query GetAuthenticatedUser {\n    viewer {\n        login\n        avatarUrl\n        url\n    }\n}",
                Value::Null,
            )
            .await?;
        let response: Response =
            serde_json::from_value(data).map_err(|err| GithubError::Json(err.to_string()))?;
        Ok(response.viewer)
    }

    async fn fetch_version(&self) -> Option<String> {
        let mut cached = self.version.lock().await;
        if let Some(version) = cached.as_ref() {
            return version.clone();
        }
        let version = if self.github_dot_com {
            None
        } else {
            let client = V3Client::new(self.api_url.clone(), self.auth.clone(), self.http.clone());
            client.get_version().await.ok()
        };
        *cached = Some(version.clone());
        version
    }

    /// `V4Client.repositoryFieldsGraphQLFragment`.
    pub async fn repository_fields_graphql_fragment(&self) -> String {
        if self.github_dot_com {
            return GITHUB_DOT_COM_REPOSITORY_FIELDS.to_owned();
        }
        let Some(version) = self.fetch_version().await else {
            return format_ghe_repository_fields(true, true);
        };
        let parsed = parse_version(&version);
        match parsed {
            Some((major, minor, patch)) => format_ghe_repository_fields(
                version_at_least((major, minor, patch), (3, 0, 0)),
                version_at_least((major, minor, patch), (3, 3, 0)),
            ),
            // `fetchGitHubVersion` falls back to allMatchingSemver.
            None => format_ghe_repository_fields(true, true),
        }
    }

    /// `V4Client.GetReposByNameWithOwner`.
    pub async fn get_repos_by_name_with_owner(
        &self,
        names_with_owners: &[String],
    ) -> Result<Vec<Repository>, GithubError> {
        if names_with_owners.len() > 30 {
            return Err(GithubError::BatchTooLarge);
        }
        let query = self.build_get_repos_batch_query(names_with_owners).await?;
        let data = match self.request_graphql(&query, json!({})).await {
            Ok(data) => data,
            Err(GithubError::GraphQl { errors, data }) => {
                if errors
                    .0
                    .iter()
                    .all(|err| err.type_ == GRAPHQL_ERR_TYPE_NOT_FOUND)
                {
                    data.unwrap_or(Value::Null)
                } else {
                    return Err(GithubError::GraphQl { errors, data });
                }
            }
            Err(err) => return Err(err),
        };
        let Some(object) = data.as_object() else {
            return Ok(Vec::new());
        };
        let mut repos = Vec::new();
        for value in object.values() {
            if value.is_null() {
                continue;
            }
            let gql: GraphQlRepository = serde_json::from_value(value.clone())
                .map_err(|err| GithubError::Json(err.to_string()))?;
            repos.push(gql.into_repository());
        }
        Ok(repos)
    }

    async fn build_get_repos_batch_query(
        &self,
        names_with_owners: &[String],
    ) -> Result<String, GithubError> {
        let mut query = self.repository_fields_graphql_fragment().await;
        query.push_str("query {\n");
        for (index, pair) in names_with_owners.iter().enumerate() {
            let (owner, name) =
                split_repository_name_with_owner(pair).map_err(GithubError::Http)?;
            query.push_str(&format!(
                "repo{index}: repository(owner: {owner:?}, name: {name:?}) {{ ",
            ));
            query.push_str(
                "... on Repository { ...RepositoryFields parent { nameWithOwner, isFork } } }\n",
            );
        }
        query.push('}');
        Ok(query)
    }

    /// `V4Client.SearchRepos`.
    pub async fn search_repos(
        &self,
        params: SearchReposParams,
    ) -> Result<SearchReposResults, GithubError> {
        let first = if params.first == 0 { 100 } else { params.first };
        let mut variables = json!({
            "query": params.query,
            "type": "REPOSITORY",
            "first": first,
        });
        if !params.after.is_empty() {
            variables["after"] = json!(params.after);
        }
        let query = self.build_search_repos_query().await;
        let data = self.request_graphql(&query, variables).await?;
        #[derive(Deserialize, Default)]
        struct Search {
            #[serde(rename = "repositoryCount", default)]
            repository_count: i32,
            #[serde(rename = "pageInfo", default)]
            page_info: PageInfo,
            #[serde(default)]
            nodes: Vec<GraphQlRepository>,
        }
        #[derive(Deserialize, Default)]
        struct Response {
            #[serde(default)]
            search: Search,
        }
        let response: Response =
            serde_json::from_value(data).map_err(|err| GithubError::Json(err.to_string()))?;
        Ok(SearchReposResults {
            repos: response
                .search
                .nodes
                .into_iter()
                .map(GraphQlRepository::into_repository)
                .collect(),
            total_count: response.search.repository_count,
            end_cursor: if response.search.page_info.has_next_page {
                response.search.page_info.end_cursor
            } else {
                String::new()
            },
        })
    }

    async fn build_search_repos_query(&self) -> String {
        let mut query = self.repository_fields_graphql_fragment().await;
        query.push_str(
            r#"
query($query: String!, $type: SearchType!, $after: String, $first: Int!) {
	search(query: $query, type: $type, after: $after, first: $first) {
		repositoryCount
		pageInfo { hasNextPage,  endCursor }
		nodes { ... on Repository { ...RepositoryFields } }
	}
}"#,
        );
        query
    }
}

const GITHUB_DOT_COM_REPOSITORY_FIELDS: &str = r#"
fragment RepositoryFields on Repository {
	id
	databaseId
	nameWithOwner
	description
	url
	isPrivate
	isFork
	isArchived
	isLocked
	isDisabled
	viewerPermission
	stargazerCount
	visibility
	forkCount
	diskUsage
	repositoryTopics(first:100) {
		nodes {
			topic {
				name
			}
		}
	}
}
	"#;

fn format_ghe_repository_fields(with_stargazer_count: bool, with_visibility: bool) -> String {
    let mut conditional = Vec::new();
    if with_stargazer_count {
        conditional.push("stargazerCount");
    }
    if with_visibility {
        conditional.push("visibility");
    }
    format!(
        r#"
fragment RepositoryFields on Repository {{
	id
	databaseId
	nameWithOwner
	description
	url
	isPrivate
	isFork
	isArchived
	isLocked
	isDisabled
	forkCount
	diskUsage
	repositoryTopics(first:100) {{
		nodes {{
			topic {{
				name
			}}
		}}
	}}
	{}
}}
	"#,
        conditional.join("\n\t")
    )
}

fn parse_version(version: &str) -> Option<(u64, u64, u64)> {
    let version = version.trim().trim_start_matches('v');
    let mut parts = version.split('.');
    let major = parts.next()?.parse().ok()?;
    let minor = parts.next()?.parse().ok()?;
    let patch = parts
        .next()
        .map(|part| {
            part.split(|ch: char| !ch.is_ascii_digit())
                .next()
                .unwrap_or("0")
                .parse()
                .unwrap_or(0)
        })
        .unwrap_or(0);
    Some((major, minor, patch))
}

fn version_at_least(have: (u64, u64, u64), want: (u64, u64, u64)) -> bool {
    have >= want
}

/// `github.Actor`.
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct Actor {
    #[serde(default)]
    pub login: String,
    #[serde(default)]
    pub avatar_url: String,
    #[serde(default)]
    pub url: String,
}

/// `github.Cursor`.
pub type Cursor = String;

/// `github.SearchReposParams`.
#[derive(Debug, Clone, Default)]
pub struct SearchReposParams {
    pub query: String,
    pub after: Cursor,
    pub first: i32,
}

/// `github.SearchReposResults`.
#[derive(Debug, Clone, Default)]
pub struct SearchReposResults {
    pub repos: Vec<Repository>,
    pub total_count: i32,
    pub end_cursor: Cursor,
}

#[derive(Debug, Clone, Default, Deserialize)]
struct PageInfo {
    #[serde(rename = "hasNextPage", default)]
    has_next_page: bool,
    #[serde(rename = "endCursor", default)]
    end_cursor: String,
}

#[derive(Debug, Clone, Default, Deserialize)]
struct GraphQlRepositoryTopics {
    #[serde(default)]
    nodes: Vec<RepositoryTopic>,
}

#[derive(Debug, Clone, Default, Deserialize)]
struct GraphQlRepository {
    #[serde(default)]
    id: String,
    #[serde(rename = "databaseId", default)]
    database_id: i64,
    #[serde(rename = "nameWithOwner", default)]
    name_with_owner: String,
    #[serde(default)]
    description: String,
    #[serde(default)]
    url: String,
    #[serde(rename = "isPrivate", default)]
    is_private: bool,
    #[serde(rename = "isFork", default)]
    is_fork: bool,
    #[serde(rename = "isArchived", default)]
    is_archived: bool,
    #[serde(rename = "isLocked", default)]
    is_locked: bool,
    #[serde(rename = "isDisabled", default)]
    is_disabled: bool,
    #[serde(rename = "viewerPermission", default)]
    viewer_permission: String,
    #[serde(rename = "stargazerCount", default)]
    stargazer_count: i32,
    #[serde(rename = "forkCount", default)]
    fork_count: i32,
    #[serde(default)]
    visibility: String,
    #[serde(rename = "diskUsage", default)]
    disk_usage: i64,
    #[serde(rename = "repositoryTopics", default)]
    repository_topics: GraphQlRepositoryTopics,
    #[serde(default)]
    parent: Option<ParentRepository>,
}

impl GraphQlRepository {
    fn into_repository(self) -> Repository {
        Repository {
            id: self.id,
            database_id: self.database_id,
            name_with_owner: self.name_with_owner,
            description: self.description,
            url: self.url,
            is_private: self.is_private,
            is_fork: self.is_fork,
            is_archived: self.is_archived,
            is_locked: self.is_locked,
            is_disabled: self.is_disabled,
            viewer_permission: self.viewer_permission,
            repository_topics: self.repository_topics.nodes,
            stargazer_count: self.stargazer_count,
            fork_count: self.fork_count,
            visibility: self.visibility,
            disk_usage_kibibytes: self.disk_usage,
            parent: self.parent,
        }
    }
}

/// Metadata JSON matching Go's `json.Marshal(github.Repository)` field names
/// and `omitempty` tags.
pub fn repository_metadata(repo: &Repository) -> Value {
    let mut map = serde_json::Map::new();
    map.insert("ID".to_owned(), json!(repo.id));
    map.insert("DatabaseID".to_owned(), json!(repo.database_id));
    map.insert("NameWithOwner".to_owned(), json!(repo.name_with_owner));
    map.insert("Description".to_owned(), json!(repo.description));
    map.insert("URL".to_owned(), json!(repo.url));
    map.insert("IsPrivate".to_owned(), json!(repo.is_private));
    map.insert("IsFork".to_owned(), json!(repo.is_fork));
    map.insert("IsArchived".to_owned(), json!(repo.is_archived));
    map.insert("IsLocked".to_owned(), json!(repo.is_locked));
    map.insert("IsDisabled".to_owned(), json!(repo.is_disabled));
    // `makeRepo` always clears this field before storing metadata.
    map.insert("ViewerPermission".to_owned(), json!(""));
    map.insert(
        "RepositoryTopics".to_owned(),
        json!({
            "Nodes": repo
                .repository_topics
                .iter()
                .map(|topic| json!({"Topic": {"Name": topic.topic.name}}))
                .collect::<Vec<_>>(),
        }),
    );
    if repo.stargazer_count != 0 {
        map.insert("StargazerCount".to_owned(), json!(repo.stargazer_count));
    }
    if repo.fork_count != 0 {
        map.insert("ForkCount".to_owned(), json!(repo.fork_count));
    }
    if !repo.visibility.is_empty() {
        map.insert("visibility".to_owned(), json!(repo.visibility));
    }
    if repo.disk_usage_kibibytes != 0 {
        map.insert("DiskUsage".to_owned(), json!(repo.disk_usage_kibibytes));
    }
    Value::Object(map)
}

/// Exposed for tests: parse the `Link` header exactly like the baseline.
pub fn response_has_next_page(headers: &[(String, String)]) -> bool {
    ResponseState {
        status: 200,
        headers: headers.to_vec(),
    }
    .has_next_page()
}

/// Exposed for tests: rate limit state parsing.
pub fn parse_rate_limit(headers: &[(String, String)]) -> RateLimitState {
    let mut state = RateLimitState::default();
    state.update(&ResponseState {
        status: 200,
        headers: headers.to_vec(),
    });
    state
}

/// Date format used by `dateRange.String`: `2006-01-02T15:04:05-07:00`.
pub const DATE_RANGE_FORMAT: &[FormatItem<'static>] = format_description!(
    "[year]-[month]-[day]T[hour]:[minute]:[second][offset_hour sign:mandatory]:[offset_minute]"
);

/// Set an arbitrary header (used by the source's tests).
pub fn header_map_add(headers: &mut Vec<(String, String)>, name: &str, value: &str) {
    add_header(headers, name, value);
}

/// Set an arbitrary header (used by the source's tests).
pub fn header_map_set(headers: &mut Vec<(String, String)>, name: &str, value: &str) {
    set_header(headers, name, value);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn api_root_matches_github_dot_com_and_enterprise() {
        let (api, dotcom) = api_root(&Url::parse("https://github.com").expect("url"));
        assert!(dotcom);
        assert_eq!(api.as_str(), "https://api.github.com/");

        let (api, dotcom) = api_root(&Url::parse("https://github.example.com").expect("url"));
        assert!(!dotcom);
        assert_eq!(api.as_str(), "https://github.example.com/api/v3");

        // `APIRoot` is called with `NormalizeBaseURL` output, i.e. a path
        // ending in "/", where `ResolveReference` keeps the prefix.
        let (api, _) = api_root(&Url::parse("https://github.example.com/prefix/").expect("url"));
        assert_eq!(api.as_str(), "https://github.example.com/prefix/api");

        // Without the trailing slash Go's relative reference replaces the
        // last path segment; keep that exact `ResolveReference` behavior.
        let (api, _) = api_root(&Url::parse("https://github.example.com/prefix").expect("url"));
        assert_eq!(api.as_str(), "https://github.example.com/api");
    }

    #[test]
    fn request_urls_join_paths_like_go() {
        let api = Url::parse("https://github.example.com/api/v3").expect("url");
        let url = resolve_request_url(
            &api,
            "orgs/acme/repos?sort=created&page=1&per_page=100&type=all",
        )
        .expect("url");
        assert_eq!(
            url.as_str(),
            "https://github.example.com/api/v3/orgs/acme/repos?sort=created&page=1&per_page=100&type=all"
        );

        let url = resolve_request_url(&api, "../graphql").expect("url");
        assert_eq!(url.as_str(), "https://github.example.com/api/graphql");

        let api = Url::parse("https://api.github.com/").expect("url");
        let url = resolve_request_url(&api, "/repos/o/r").expect("url");
        assert_eq!(url.as_str(), "https://api.github.com/repos/o/r");
    }

    #[test]
    fn split_repository_names_validates() {
        assert_eq!(
            split_repository_name_with_owner("o/r").expect("ok"),
            ("o".to_owned(), "r".to_owned())
        );
        assert!(split_repository_name_with_owner("o").is_err());
        assert!(split_repository_name_with_owner("o/r/s").is_err());
        assert!(split_repository_name_with_owner("/r").is_err());
    }

    #[test]
    fn link_and_rate_limit_headers_parse() {
        assert!(response_has_next_page(&[(
            "Link".to_owned(),
            "<https://api.github.com/x?page=2>; rel=\"next\"".to_owned()
        )]));
        assert!(!response_has_next_page(&[(
            "Link".to_owned(),
            "<https://api.github.com/x?page=1>; rel=\"prev\"".to_owned()
        )]));
        let state = parse_rate_limit(&[
            ("X-RateLimit-Remaining".to_owned(), "12".to_owned()),
            ("X-RateLimit-Limit".to_owned(), "60".to_owned()),
            ("X-RateLimit-Reset".to_owned(), "1700000000".to_owned()),
        ]);
        assert_eq!(state.remaining, 12);
        assert_eq!(state.limit, 60);
        assert_eq!(
            state.reset,
            Some(OffsetDateTime::from_unix_timestamp(1_700_000_000).expect("epoch"))
        );
    }

    #[test]
    fn rest_repository_conversion_matches_the_baseline() {
        let rest: RestRepository = serde_json::from_value(json!({
            "node_id": "R_1",
            "id": 42,
            "full_name": "acme/widgets",
            "description": "d",
            "html_url": "https://github.example.com/acme/widgets",
            "private": true,
            "fork": false,
            "archived": true,
            "locked": false,
            "disabled": false,
            "permissions": {"admin": false, "push": true, "pull": true},
            "stargazers_count": 7,
            "forks_count": 3,
            "visibility": "private",
            "topics": ["a", "b"],
            "size": 2048
        }))
        .expect("rest repo");
        let repo = convert_rest_repo(rest);
        assert_eq!(repo.id, "R_1");
        assert_eq!(repo.database_id, 42);
        assert_eq!(repo.viewer_permission, "WRITE");
        assert_eq!(repo.size_bytes(), 2 * 1024 * 1024);
        assert_eq!(repo.repository_topics.len(), 2);
    }

    #[test]
    fn ghe_fields_follow_version_thresholds() {
        let fields = format_ghe_repository_fields(false, false);
        assert!(!fields.contains("stargazerCount"));
        assert!(!fields.contains("visibility"));
        let fields = format_ghe_repository_fields(true, false);
        assert!(fields.contains("stargazerCount"));
        assert!(!fields.contains("visibility"));
        let fields = format_ghe_repository_fields(true, true);
        assert!(fields.contains("stargazerCount"));
        assert!(fields.contains("visibility"));
    }
}
