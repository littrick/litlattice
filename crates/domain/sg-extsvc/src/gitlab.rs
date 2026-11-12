#![forbid(unsafe_code)]

//! `internal/extsvc/gitlab`: the read-only surface the repo-updater GitLab
//! source needs.
//!
//! Ported: configuration shape, PAT/OAuth authentication, request building
//! (`api/v4` base, `Content-Type`/`Cache-Control`, relative/absolute URL
//! resolution), `Link`-header pagination, `GetProject`, `ListProjects`,
//! `GetUser`, `GetVersion`, `HTTPError`/`ProjectNotFoundError` text, and the
//! Go-compatible `url.PathEscape` and `url.Values` encoding used by
//! `repos.projectQueryToURL`.
//!
//! Not ported (see the M9.12a overview): the Redis project cache, the
//! internal/external rate limiters, OAuth token refresh, the authenticated
//! `/oauth/token/info` scope probe, and every write/merge-request/group
//! endpoint. Those are side channels; repository enumeration semantics do not
//! depend on them.

use std::collections::BTreeMap;
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use url::Url;

use crate::auth::set_header;
use crate::{HttpDoer, HttpRequest, HttpResponse};

/// `extsvc.TypeGitLab` (stored in `repo.external_service_type`).
pub const TYPE_GITLAB: &str = "gitlab";

/// `gitlab.TokenTypePAT`/`TokenTypeOAuth`, selected from the config's
/// `"token.type"` field. An unset type falls back to PAT like the baseline's
/// `switch` default.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub enum TokenType {
    #[default]
    PersonalAccessToken,
    OAuth,
}

impl TokenType {
    pub fn from_config(value: &str) -> Self {
        if value == "oauth" {
            Self::OAuth
        } else {
            Self::PersonalAccessToken
        }
    }
}

/// The authenticator a [`Client`] applies, mirroring
/// `gitlab.ClientProvider.GetPATClient`/`GetOAuthClient`: an empty token means
/// an unauthenticated client (Go passes a `nil` authenticator).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GitLabAuth {
    None,
    PersonalAccessToken(String),
    OAuthBearerToken(String),
}

impl Default for GitLabAuth {
    fn default() -> Self {
        Self::None
    }
}

impl GitLabAuth {
    /// `SudoableToken.Authenticate` / `auth.OAuthBearerToken.Authenticate`.
    /// The source passes an empty `sudo`, so no `Sudo` header is emitted.
    pub fn authenticate(&self, headers: &mut Vec<(String, String)>) {
        match self {
            Self::None => {}
            Self::PersonalAccessToken(token) => {
                set_header(headers, "Private-Token", token);
            }
            Self::OAuthBearerToken(token) => {
                set_header(headers, "Authorization", &format!("Bearer {token}"));
            }
        }
    }
}

/// `schema.GitLabNameTransformation`.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct GitLabNameTransformation {
    #[serde(default)]
    pub regex: String,
    #[serde(default)]
    pub replacement: String,
}

/// `schema.GitLabProject`.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct GitLabProject {
    #[serde(default)]
    pub id: i64,
    #[serde(default)]
    pub name: String,
}

/// `schema.ExcludedGitLabProject`.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct ExcludedGitLabProject {
    #[serde(rename = "emptyRepos", default)]
    pub empty_repos: bool,
    #[serde(default)]
    pub id: i64,
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub pattern: String,
}

/// `schema.GitLabConnection` (the fields the source reads).
#[derive(Debug, Clone, Default, Deserialize)]
pub struct GitLabConnection {
    #[serde(default)]
    pub url: String,
    #[serde(default)]
    pub token: String,
    /// The schema tag really is `"token.type"`, not `"tokenType"`.
    #[serde(rename = "token.type", default)]
    pub token_type: String,
    #[serde(default)]
    pub projects: Vec<GitLabProject>,
    #[serde(default, rename = "projectQuery")]
    pub project_query: Vec<String>,
    #[serde(default)]
    pub exclude: Vec<ExcludedGitLabProject>,
    #[serde(default, rename = "gitURLType")]
    pub git_url_type: String,
    #[serde(default, rename = "repositoryPathPattern")]
    pub repository_path_pattern: String,
    #[serde(default, rename = "nameTransformations")]
    pub name_transformations: Vec<GitLabNameTransformation>,
    #[serde(default, rename = "markInternalReposAsPublic")]
    pub mark_internal_repos_as_public: bool,
}

/// `gitlab.ProjectCommon`. Field order and the JSON shape match the Go
/// struct because the value is stored in `repo.metadata`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProjectCommon {
    #[serde(default)]
    pub id: i64,
    #[serde(default)]
    pub path_with_namespace: String,
    #[serde(default)]
    pub description: String,
    #[serde(default)]
    pub web_url: String,
    #[serde(default)]
    pub http_url_to_repo: String,
    #[serde(default)]
    pub ssh_url_to_repo: String,
}

/// `gitlab.Visibility` values used by the source.
pub const VISIBILITY_PRIVATE: &str = "private";
pub const VISIBILITY_INTERNAL: &str = "internal";

/// `gitlab.Project`. Serialisation mirrors Go's JSON tags: zero values are
/// emitted, `forked_from_project` is omitted when nil, `topics` nil becomes
/// `null`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Project {
    #[serde(flatten)]
    pub common: ProjectCommon,
    #[serde(default)]
    pub visibility: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub forked_from_project: Option<ProjectCommon>,
    #[serde(default)]
    pub archived: bool,
    #[serde(default)]
    pub star_count: i64,
    #[serde(default)]
    pub forks_count: i64,
    #[serde(default)]
    pub empty_repo: bool,
    #[serde(default)]
    pub default_branch: String,
    #[serde(default)]
    pub topics: Option<Vec<String>>,
}

impl Project {
    /// The fields `repos.makeRepo` reads, exposed directly.
    pub fn path_with_namespace(&self) -> &str {
        &self.common.path_with_namespace
    }
}

/// `gitlab.AuthUser`, reduced to the fields that matter for a connection
/// check (the baseline only requires a decodable object).
#[derive(Debug, Clone, Default, Deserialize)]
pub struct AuthUser {
    #[serde(default)]
    pub id: i64,
    #[serde(default)]
    pub username: String,
    #[serde(default)]
    pub name: String,
}

/// `gitlab.HTTPError`: the status code and body of a non-2xx/3xx response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HTTPError {
    pub code: u16,
    pub body: Vec<u8>,
}

impl HTTPError {
    /// `HTTPError.Message`: best-effort `{"message": ...}` extraction. Not
    /// part of `Error()`, but useful for logs and parity tests.
    pub fn message(&self) -> String {
        #[derive(Deserialize, Default)]
        struct Body {
            #[serde(default)]
            message: String,
        }
        serde_json::from_slice::<Body>(&self.body)
            .map(|body| body.message)
            .unwrap_or_default()
    }
}

/// Errors produced by the client. `Display` reproduces the Go wrapping that
/// the source puts into `SourceResult.Err`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClientError {
    /// A non-2xx/3xx response, wrapped like
    /// `errors.Wrap(err, "unexpected response from GitLab API (URL)")`.
    Api {
        url: String,
        code: u16,
        body: Vec<u8>,
    },
    /// A transport or JSON error. The baseline wraps transport errors with
    /// `"request failed"`.
    Other(String),
    /// `gitlab.ProjectNotFoundError`.
    ProjectNotFound { name: String },
}

impl ClientError {
    pub fn is_not_found(&self) -> bool {
        match self {
            Self::ProjectNotFound { .. } => true,
            Self::Api { code, .. } => *code == 404,
            Self::Other(_) => false,
        }
    }

    /// `HTTPErrorCode`: the status code for an API error.
    pub fn http_error_code(&self) -> Option<u16> {
        match self {
            Self::Api { code, .. } => Some(*code),
            _ => None,
        }
    }
}

impl std::fmt::Display for ClientError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Api { url, code, .. } => write!(
                f,
                "unexpected response from GitLab API ({url}): HTTP error status {code}"
            ),
            Self::Other(err) => f.write_str(err),
            Self::ProjectNotFound { name } => {
                if name.is_empty() {
                    f.write_str("GitLab project not found")
                } else {
                    write!(f, "GitLab project {name:?} not found")
                }
            }
        }
    }
}

impl std::error::Error for ClientError {}

/// `gitlab.Client` for one authenticator.
#[derive(Clone)]
pub struct Client {
    /// The `api/v4/` base URL (trailing slash), like
    /// `ClientProvider.baseURL`.
    api_url: Url,
    auth: GitLabAuth,
    http: Arc<dyn HttpDoer>,
}

impl Client {
    /// `ClientProvider.NewClient`: computes `path.Join(base.Path, "api/v4")`
    /// on top of the normalized web URL.
    pub fn new(base_url: &Url, auth: GitLabAuth, http: Arc<dyn HttpDoer>) -> Self {
        Self {
            api_url: api_v4_url(base_url),
            auth,
            http,
        }
    }

    pub fn api_url(&self) -> &Url {
        &self.api_url
    }

    pub fn authenticator(&self) -> &GitLabAuth {
        &self.auth
    }

    /// `Client.WithAuthenticator`.
    pub fn with_authenticator(&self, auth: GitLabAuth) -> Self {
        Self {
            api_url: self.api_url.clone(),
            auth,
            http: Arc::clone(&self.http),
        }
    }

    /// `Client.do` + `doWithBaseURL`, minus the rate limiters. A 429 is
    /// retried at most twice, like `maxRateLimitRetries`.
    async fn do_request(
        &self,
        method: &str,
        request_uri: &str,
    ) -> Result<(String, HttpResponse), ClientError> {
        let url = self
            .api_url
            .join(request_uri)
            .map_err(|err| ClientError::Other(err.to_string()))?;
        let url_string = url.to_string();

        let mut retries = 0u8;
        loop {
            let mut headers = Vec::new();
            set_header(
                &mut headers,
                "Content-Type",
                "application/json; charset=utf-8",
            );
            set_header(&mut headers, "Cache-Control", "max-age=0");
            self.auth.authenticate(&mut headers);

            let response = self
                .http
                .do_request(HttpRequest {
                    method: method.to_owned(),
                    url: url_string.clone(),
                    headers,
                    body: Vec::new(),
                })
                .await
                .map_err(|err| ClientError::Other(format!("request failed: {err}")))?;

            if response.status >= 200 && response.status < 400 {
                return Ok((url_string, response));
            }
            if response.status == 429 && retries < 2 {
                retries += 1;
                continue;
            }
            return Err(ClientError::Api {
                url: url_string,
                code: response.status,
                body: response.body,
            });
        }
    }

    /// `getProjectFromAPI` + `GetProject` without the Redis cache.
    ///
    /// A 404 becomes `ProjectNotFoundError{Name: path_with_namespace}`, just
    /// like `getProjectFromAPI` (note: when selecting by ID the name is empty).
    pub async fn get_project(
        &self,
        id: i64,
        path_with_namespace: &str,
    ) -> Result<Project, ClientError> {
        if id != 0 && !path_with_namespace.is_empty() {
            return Err(ClientError::Other(
                "invalid args (specify exactly one of id and pathWithNamespace)".to_owned(),
            ));
        }
        let url_param = if id != 0 {
            id.to_string()
        } else {
            path_escape(path_with_namespace)
        };
        let request_uri = format!("projects/{url_param}");
        let (_, response) =
            self.do_request("GET", &request_uri)
                .await
                .map_err(|err| match err {
                    ClientError::Api { code: 404, .. } => ClientError::ProjectNotFound {
                        name: path_with_namespace.to_owned(),
                    },
                    other => other,
                })?;
        decode_project(&response.body)
    }

    /// `ListProjects`: returns the projects and the `rel="next"` link.
    pub async fn list_projects(
        &self,
        url_str: &str,
    ) -> Result<(Vec<Project>, Option<String>), ClientError> {
        let (_, response) = self.do_request("GET", url_str).await?;
        let projects = decode_project_list(&response.body)?;
        let next = next_page_url(&response.headers);
        Ok((projects, next))
    }

    /// `GetUser`: an empty ID fetches the authenticated user.
    pub async fn get_user(&self, id: &str) -> Result<AuthUser, ClientError> {
        let url_str = if id.is_empty() {
            "user".to_owned()
        } else {
            format!("users/{id}")
        };
        let (_, response) = self.do_request("GET", &url_str).await?;
        serde_json::from_slice::<Option<AuthUser>>(&response.body)
            .map(|user| user.unwrap_or_default())
            .map_err(|err| ClientError::Other(err.to_string()))
    }

    /// `GetVersion`.
    pub async fn get_version(&self) -> Result<String, ClientError> {
        #[derive(Default, Deserialize)]
        struct Version {
            #[serde(default)]
            version: String,
        }
        let (_, response) = self.do_request("GET", "version").await?;
        serde_json::from_slice::<Version>(&response.body)
            .map(|version| version.version)
            .map_err(|err| ClientError::Other(format!("requesting version: {err}")))
    }
}

fn decode_project(body: &[u8]) -> Result<Project, ClientError> {
    serde_json::from_slice::<Option<Project>>(body)
        .map(|project| project.unwrap_or_default())
        .map_err(|err| ClientError::Other(err.to_string()))
}

fn decode_project_list(body: &[u8]) -> Result<Vec<Project>, ClientError> {
    // Go decodes into `[]*Project`; JSON `null` becomes a nil slice.
    serde_json::from_slice::<Option<Vec<Project>>>(body)
        .map(|projects| projects.unwrap_or_default())
        .map_err(|err| ClientError::Other(err.to_string()))
}

/// `ClientProvider`'s `baseURL.ResolveReference(&url.URL{Path: path.Join(base.Path, "api/v4") + "/"})`.
pub fn api_v4_url(base_url: &Url) -> Url {
    let joined = go_path_join(base_url.path(), "api/v4");
    let mut url = base_url.clone();
    url.set_query(None);
    url.set_fragment(None);
    url.set_path(&format!("{}/", joined.trim_end_matches('/')));
    url
}

/// `path.Join` for the two-segment joins the baseline performs.
pub fn go_path_join(base: &str, request: &str) -> String {
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

/// `net/url.PathEscape`: escapes everything except RFC 3986 unreserved
/// characters and the sub-delimiters Go leaves alone. Notably `/` becomes
/// `%2F`, which is what GitLab requires for `pathWithNamespace`.
pub fn path_escape(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    for byte in input.bytes() {
        let keep = byte.is_ascii_alphanumeric()
            || matches!(
                byte,
                b'-' | b'_' | b'.' | b'~' | b'$' | b'&' | b'+' | b'=' | b':' | b'@'
            );
        if keep {
            out.push(byte as char);
        } else {
            out.push_str(&format!("%{byte:02X}"));
        }
    }
    out
}

/// `link.Parse(header)["next"].URI`: the URI of the `rel="next"` link.
pub fn next_page_url(headers: &[(String, String)]) -> Option<String> {
    for (name, value) in headers {
        if !name.eq_ignore_ascii_case("Link") {
            continue;
        }
        for part in value.split(',') {
            let part = part.trim();
            let Some((uri, params)) = part.split_once(';') else {
                continue;
            };
            let uri = uri.trim();
            let Some(uri) = uri.strip_prefix('<').and_then(|uri| uri.strip_suffix('>')) else {
                continue;
            };
            let is_next = params.split(';').any(|param| {
                param
                    .trim()
                    .strip_prefix("rel=")
                    .map(|rel| rel.trim_matches('"') == "next")
                    .unwrap_or(false)
            });
            if is_next {
                return Some(uri.to_owned());
            }
        }
    }
    None
}

/// `repos.projectQueryToURL`: prepend `projects` for a bare query, reject a
/// non-empty scheme/host, set `per_page`, then encode with sorted keys like
/// `url.Values.Encode`.
pub fn project_query_to_url(project_query: &str, per_page: i64) -> Result<String, String> {
    let mut query = project_query.to_owned();
    if query.starts_with('?') {
        query = format!("projects{query}");
    } else if query.is_empty() {
        query = "projects".to_owned();
    }

    if scheme_or_host_not_empty(&query) {
        return Err("scheme and host should be empty".to_owned());
    }

    if let Some((before, fragment)) = query.split_once('#') {
        let (path, raw_query) = before.split_once('?').unwrap_or((before, ""));
        return Ok(encode_query_with_fragment(
            path,
            raw_query,
            Some(fragment),
            per_page,
        ));
    }
    let (path, raw_query) = query.split_once('?').unwrap_or((query.as_str(), ""));
    Ok(encode_query_with_fragment(path, raw_query, None, per_page))
}

fn encode_query_with_fragment(
    path: &str,
    raw_query: &str,
    fragment: Option<&str>,
    per_page: i64,
) -> String {
    let mut values: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for (key, value) in url::form_urlencoded::parse(raw_query.as_bytes()) {
        values
            .entry(key.into_owned())
            .or_default()
            .push(value.into_owned());
    }
    values.insert("per_page".to_owned(), vec![per_page.to_string()]);

    let mut serializer = url::form_urlencoded::Serializer::new(String::new());
    for (key, entries) in &values {
        for value in entries {
            serializer.append_pair(key, value);
        }
    }
    let encoded = serializer.finish();

    let mut out = path.to_owned();
    if !encoded.is_empty() {
        out.push('?');
        out.push_str(&encoded);
    }
    if let Some(fragment) = fragment {
        out.push('#');
        out.push_str(fragment);
    }
    out
}

/// `url.Parse(input).Scheme != "" || .Host != ""`.
fn scheme_or_host_not_empty(input: &str) -> bool {
    if input.starts_with("//") {
        return true;
    }
    let Some((scheme, _rest)) = input.split_once(':') else {
        return false;
    };
    let mut chars = scheme.chars();
    match chars.next() {
        Some(first) if first.is_ascii_alphabetic() => {}
        _ => return false,
    }
    chars.all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '+' | '-' | '.'))
}

/// Registry of known GitLab `Visibility` values, kept for callers that want
/// the constants without string literals.
pub fn is_private_visibility(visibility: &str) -> bool {
    visibility == VISIBILITY_PRIVATE || visibility == VISIBILITY_INTERNAL
}
