#![forbid(unsafe_code)]

//! The GitHub REST subset `internal/authz/providers/github` needs, ported from
//! `internal/extsvc/github/v3.go` / `common.go`'s `doRequest`:
//!
//! * `GetRepository` (`/repos/{owner}/{name}`),
//! * `GetOrganization` (`/orgs/{login}`, with its 404 → `OrgNotFoundError`),
//! * `ListRepositoryCollaborators`, `ListRepositoryTeams`,
//!   `ListOrganizationMembers`, `ListTeamMembers`.
//!
//! Request shape matches the baseline: `path.Join(apiURL.Path, path)`
//! resolved against the API root, the two preview `Accept` values (plus the
//! nebula preview when the internal-repo-visibility flag is on),
//! `Content-Type: application/json; charset=utf-8`, `Cache-Control:
//! max-age=0`, and `Authorization: Bearer <token>`. Pagination follows the
//! `Link: <…>; rel="next"` header. Non-2xx responses become the baseline's
//! `APIError` message shape (`request to <url> returned status <code>:
//! <message>`), and 404s are flagged so the provider can classify them.
//!
//! Not ported: the rate-limit monitors/retries, the request counter, the
//! Redis/ETag transport cache, OAuth token refresh, and the GraphQL API (this
//! baseline's `GetRepository` uses the REST endpoint).

use std::sync::Arc;

use async_trait::async_trait;
use reqwest::header::{HeaderValue, ACCEPT, AUTHORIZATION, CACHE_CONTROL, CONTENT_TYPE, LINK};
use reqwest::StatusCode;
use serde::de::DeserializeOwned;
use serde::Deserialize;

use super::authz_github::{
    Collaborator, GithubClient, GithubClientError, GithubUserClient, Org, OrgDetails,
    OrgDetailsAndMembership, OrgMembership, Repository, Team,
};
use super::authz_github_user::OAuthBearerToken;
use super::github_apps_installation::api_root;

/// `github.V3Client`'s unconditional preview `Accept` values. The baseline
/// adds jean-grey and mercy as one comma-joined value and machine-man as a
/// second; three separate values are equivalent on the wire.
const ACCEPT_PREVIEW: [&str; 3] = [
    "application/vnd.github.jean-grey-preview+json",
    "application/vnd.github.mercy-preview+json",
    "application/vnd.github.machine-man-preview+json",
];
/// Added when `ExperimentalFeatures.EnableGithubInternalRepoVisibility` is on.
const ACCEPT_NEBULA: &str = "application/vnd.github.nebula-preview+json";

/// How the client authenticates: the external service's static token, or
/// `OAuthBearerToken{Token, RefreshToken, Expiry, RefreshFunc, NeedsRefreshBuffer: 5}`
/// (`WithAuthenticator`) for the user-centric path.
#[derive(Clone)]
pub enum Auth {
    /// `auth.OAuthBearerToken{Token: c.Token}`; `Some("")` still sends
    /// `Authorization: Bearer ` like the baseline.
    Static(Option<String>),
    OAuth(Arc<OAuthBearerToken>),
}

/// `internal/extsvc/github.V3Client`, reduced to the authz methods.
pub struct GithubRestClient {
    http: reqwest::Client,
    api_url: reqwest::Url,
    auth: Auth,
    enable_github_internal_repo_visibility: bool,
}

impl GithubRestClient {
    /// Build with the default HTTP client (which honours proxy environment
    /// variables, like the baseline's `httpcli.ExternalDoer`).
    pub fn new(
        base_url: &reqwest::Url,
        token: Option<String>,
        enable_github_internal_repo_visibility: bool,
    ) -> Self {
        Self::with_http_client(
            reqwest::Client::new(),
            base_url,
            token,
            enable_github_internal_repo_visibility,
        )
    }

    /// Build with an injected HTTP client (tests use a no-proxy client).
    pub fn with_http_client(
        http: reqwest::Client,
        base_url: &reqwest::Url,
        token: Option<String>,
        enable_github_internal_repo_visibility: bool,
    ) -> Self {
        // `newV3Client` runs the API root through `canonicalizedURL`: any
        // GitHub.com host collapses to `https://api.github.com/`. A
        // `https://github.com/` root is already canonical from `api_root`;
        // this also covers a root explicitly given as `api.github.com`.
        let mut api_url = api_root(base_url);
        let host = api_url.host_str().unwrap_or_default().to_ascii_lowercase();
        if host == "api.github.com" {
            api_url = reqwest::Url::parse("https://api.github.com/").expect("static API URL");
        }
        Self {
            http,
            api_url,
            auth: Auth::Static(token),
            enable_github_internal_repo_visibility,
        }
    }

    /// `ClientAdapter.WithAuthenticator(oauthToken)`: the same client with the
    /// user's OAuth bearer token.
    pub fn with_oauth_token(&self, token: Arc<OAuthBearerToken>) -> Self {
        Self {
            http: self.http.clone(),
            api_url: self.api_url.clone(),
            auth: Auth::OAuth(token),
            enable_github_internal_repo_visibility: self.enable_github_internal_repo_visibility,
        }
    }

    pub fn api_url(&self) -> &reqwest::Url {
        &self.api_url
    }

    /// The underlying HTTP client, shared with the OAuth token endpoint so the
    /// refresh request uses the same proxy/TLS settings as the API calls (the
    /// baseline's `httpcli.ExternalDoer`).
    pub fn http_client(&self) -> reqwest::Client {
        self.http.clone()
    }

    /// `path.Join(apiURL.Path, path)` resolved against the API root. The
    /// query string is appended verbatim, matching the baseline's
    /// `fmt.Sprintf("…?page=%d&per_page=100")` paths.
    fn request_url(&self, path: &str, query: Option<&str>) -> reqwest::Url {
        let mut url = self.api_url.clone();
        url.set_path(&join_path(self.api_url.path(), path));
        if let Some(query) = query {
            url.set_query(Some(query));
        } else {
            url.set_query(None);
        }
        url
    }

    /// `oauthutil.DoRequest`: a pre-emptive refresh when the token is known to
    /// be expiring (`NeedsRefresh`), then the request; a 401 with an
    /// `OAuthBearerToken` refreshes and retries exactly once. A failed
    /// refresh leaves the original response in place (the 401 APIError).
    async fn do_request(&self, url: &reqwest::Url) -> Result<reqwest::Response, GithubClientError> {
        if let Auth::OAuth(token) = &self.auth {
            if token.needs_refresh() {
                if let Err(err) = token.refresh().await {
                    tracing::warn!(error = %err, "doRequest: token refresh failed");
                }
            }
        }

        let response = self.send(url).await?;
        if response.status() == StatusCode::UNAUTHORIZED {
            if let Auth::OAuth(token) = &self.auth {
                if token.refresh().await.is_ok() {
                    // Re-authorize and re-do the request.
                    return self.send(url).await;
                }
            }
        }
        Ok(response)
    }

    fn build_request(
        &self,
        url: &reqwest::Url,
    ) -> Result<reqwest::RequestBuilder, GithubClientError> {
        let mut request = self
            .http
            .get(url.clone())
            .header(ACCEPT, ACCEPT_PREVIEW[0])
            .header(ACCEPT, ACCEPT_PREVIEW[1])
            .header(ACCEPT, ACCEPT_PREVIEW[2])
            .header(CONTENT_TYPE, "application/json; charset=utf-8")
            .header(CACHE_CONTROL, "max-age=0");
        if self.enable_github_internal_repo_visibility {
            request = request.header(ACCEPT, ACCEPT_NEBULA);
        }
        let token = match &self.auth {
            Auth::Static(token) => token.clone(),
            Auth::OAuth(token) => Some(token.token().access_token),
        };
        if let Some(token) = token {
            let value = HeaderValue::from_str(&format!("Bearer {token}"))
                .map_err(|err| GithubClientError::new(format!("authenticating request: {err}")))?;
            request = request.header(AUTHORIZATION, value);
        }
        Ok(request)
    }

    async fn send(&self, url: &reqwest::Url) -> Result<reqwest::Response, GithubClientError> {
        self.build_request(url)?
            .send()
            .await
            .map_err(|err| GithubClientError::new(format!("request failed: {err}")))
    }

    /// `doRequest` + `json.Decoder.Decode`: returns the decoded body and
    /// whether the `Link` header advertises a next page.
    async fn get_json<T: DeserializeOwned>(
        &self,
        path: &str,
        query: Option<&str>,
    ) -> Result<(T, bool), GithubClientError> {
        let url = self.request_url(path, query);

        let response = self.do_request(&url).await?;

        let has_next_page = response
            .headers()
            .get(LINK)
            .and_then(|value| value.to_str().ok())
            .is_some_and(|link| link.contains("rel=\"next\""));

        let status = response.status();
        if !status.is_success() {
            return Err(api_error(&url, status, response).await);
        }

        let body = response
            .json::<T>()
            .await
            .map_err(|err| GithubClientError::new(err.to_string()))?;
        Ok((body, has_next_page))
    }
}

/// `doRequest`'s error branch: decode `{message, documentation_url}` and
/// format `APIError.Error()`.
async fn api_error(
    url: &reqwest::Url,
    status: StatusCode,
    response: reqwest::Response,
) -> GithubClientError {
    let (message, documentation_url) = match response.text().await {
        Ok(body) => match serde_json::from_str::<RawError>(&body) {
            Ok(error) => (
                error.message.unwrap_or_default(),
                error.documentation_url.unwrap_or_default(),
            ),
            Err(err) => (
                format!("failed to decode error response from GitHub API: {err}: {body:?}"),
                String::new(),
            ),
        },
        Err(err) => (
            format!("failed to read error response from GitHub API: {err}"),
            String::new(),
        ),
    };
    GithubClientError {
        message: format!(
            "request to {url} returned status {}: {message}",
            status.as_u16()
        ),
        not_found: status == StatusCode::NOT_FOUND,
        status: status.as_u16(),
        api_message: message,
        documentation_url,
    }
}

/// `path.Join` for the two path components the client combines.
fn join_path(base: &str, rest: &str) -> String {
    let joined = format!(
        "{}/{}",
        base.trim_end_matches('/'),
        rest.trim_start_matches('/')
    );
    if joined.starts_with('/') {
        joined
    } else {
        format!("/{joined}")
    }
}

#[derive(Debug, Deserialize)]
struct RawError {
    #[serde(default)]
    message: Option<String>,
    #[serde(default)]
    #[allow(dead_code)]
    documentation_url: Option<String>,
}

#[derive(Debug, Deserialize)]
struct RawCollaborator {
    #[serde(default)]
    id: i64,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)] // `name`/`url` are decoded for parity but not converted.
struct RawTeam {
    #[serde(default)]
    name: String,
    #[serde(default)]
    slug: String,
    #[serde(default)]
    url: String,
    #[serde(default)]
    repos_count: i64,
    #[serde(default)]
    organization: Option<RawOrg>,
}

#[derive(Debug, Deserialize)]
struct RawOrg {
    #[serde(default)]
    login: String,
}

#[derive(Debug, Deserialize)]
struct RawOrgDetails {
    #[serde(default)]
    login: String,
    #[serde(default)]
    default_repository_permission: String,
}

#[derive(Debug, Deserialize)]
struct RawRepository {
    #[serde(default)]
    node_id: String,
    #[serde(default)]
    visibility: String,
}

#[async_trait]
impl GithubClient for GithubRestClient {
    async fn list_repository_collaborators(
        &self,
        owner: &str,
        repo: &str,
        page: i32,
        affiliation: Option<&str>,
    ) -> Result<(Vec<Collaborator>, bool), GithubClientError> {
        let mut query = format!("page={page}&per_page=100");
        if let Some(affiliation) = affiliation {
            query.push_str("&affiliation=");
            query.push_str(affiliation);
        }
        let (users, has_next): (Vec<RawCollaborator>, bool) = self
            .get_json(
                &format!("/repos/{owner}/{repo}/collaborators"),
                Some(&query),
            )
            .await?;
        Ok((
            users
                .into_iter()
                .map(|user| Collaborator {
                    database_id: user.id,
                })
                .collect(),
            has_next,
        ))
    }

    async fn list_repository_teams(
        &self,
        owner: &str,
        repo: &str,
        page: i32,
    ) -> Result<(Vec<Team>, bool), GithubClientError> {
        let (teams, has_next): (Vec<RawTeam>, bool) = self
            .get_json(
                &format!("/repos/{owner}/{repo}/teams"),
                Some(&format!("page={page}&per_page=100")),
            )
            .await?;
        Ok((
            teams
                .into_iter()
                .map(|team| Team {
                    slug: team.slug,
                    repos_count: team.repos_count,
                    organization: team.organization.map(|org| Org { login: org.login }),
                })
                .collect(),
            has_next,
        ))
    }

    async fn list_organization_members(
        &self,
        owner: &str,
        page: i32,
        admins_only: bool,
    ) -> Result<(Vec<Collaborator>, bool), GithubClientError> {
        let mut query = format!("page={page}&per_page=100");
        if admins_only {
            query.push_str("&role=admin");
        }
        let (users, has_next): (Vec<RawCollaborator>, bool) = self
            .get_json(&format!("/orgs/{owner}/members"), Some(&query))
            .await?;
        Ok((
            users
                .into_iter()
                .map(|user| Collaborator {
                    database_id: user.id,
                })
                .collect(),
            has_next,
        ))
    }

    async fn list_team_members(
        &self,
        owner: &str,
        team: &str,
        page: i32,
    ) -> Result<(Vec<Collaborator>, bool), GithubClientError> {
        let (users, has_next): (Vec<RawCollaborator>, bool) = self
            .get_json(
                &format!("/orgs/{owner}/teams/{team}/members"),
                Some(&format!("page={page}&per_page=100")),
            )
            .await?;
        Ok((
            users
                .into_iter()
                .map(|user| Collaborator {
                    database_id: user.id,
                })
                .collect(),
            has_next,
        ))
    }

    async fn get_organization(&self, login: &str) -> Result<OrgDetails, GithubClientError> {
        match self
            .get_json::<RawOrgDetails>(&format!("/orgs/{login}"), None)
            .await
        {
            Ok((org, _)) => Ok(OrgDetails {
                login: org.login,
                default_repository_permission: org.default_repository_permission,
            }),
            // `GetOrganization`: a 404 becomes `OrgNotFoundError`.
            Err(err) if err.not_found => Err(GithubClientError {
                message: "GitHub organization not found".to_owned(),
                not_found: true,
                status: 404,
                api_message: "GitHub organization not found".to_owned(),
                documentation_url: String::new(),
            }),
            Err(err) => Err(err),
        }
    }

    async fn get_repository(
        &self,
        owner: &str,
        name: &str,
    ) -> Result<Repository, GithubClientError> {
        match self
            .get_json::<RawRepository>(&format!("/repos/{owner}/{name}"), None)
            .await
        {
            Ok((repo, _)) => Ok(Repository {
                id: repo.node_id,
                visibility: repo.visibility,
            }),
            // `getRepositoryFromAPI`: a 404 becomes `ErrRepoNotFound`.
            Err(err) if err.not_found => Err(GithubClientError {
                message: "GitHub repository not found".to_owned(),
                not_found: true,
                status: 404,
                api_message: "GitHub repository not found".to_owned(),
                documentation_url: String::new(),
            }),
            Err(err) => Err(err),
        }
    }

    async fn get_authenticated_oauth_scopes(&self) -> Result<Vec<String>, GithubClientError> {
        // `c.get(ctx, "/", &struct{}{})`: `path.Join(apiURL.Path, "/")`, i.e.
        // the API root without a trailing slash.
        let mut url = self.api_url.clone();
        url.set_path(&join_root_path(self.api_url.path()));
        url.set_query(None);

        let response = self.do_request(&url).await?;
        let status = response.status();
        if !status.is_success() {
            return Err(api_error(&url, status, response).await);
        }
        let headers = response.headers().clone();

        // `doRequest` decodes the body into `&struct{}{}` before
        // `GetAuthenticatedOAuthScopes` reads the headers: an object or
        // `null` is accepted, anything else is a decode error. The decode
        // error text itself comes from serde and differs from Go's.
        if status != StatusCode::NO_CONTENT && status != StatusCode::NOT_MODIFIED {
            let body = response.text().await.map_err(|err| {
                GithubClientError::new(format!("failed to read response body: {err}"))
            })?;
            serde_json::from_str::<Option<serde_json::Map<String, serde_json::Value>>>(&body)
                .map_err(|err| GithubClientError::new(err.to_string()))?;
        }

        let scope = headers
            .get("x-oauth-scopes")
            .and_then(|value| value.to_str().ok())
            .unwrap_or("");
        if scope.is_empty() {
            return Ok(Vec::new());
        }
        // `strings.Split(scope, ", ")`: a comma without a following space
        // does not split, exactly like the baseline.
        Ok(scope.split(", ").map(str::to_owned).collect())
    }
}

/// `path.Join(base, "/")` for the API-root request.
fn join_root_path(base: &str) -> String {
    let base = base.trim_end_matches('/');
    if base.is_empty() {
        "/".to_owned()
    } else {
        base.to_owned()
    }
}

/// `convertRestRepo` reduced to the fields the authz provider reads. A
/// `null` entry or a repository without a `node_id` is skipped, like
/// `listRepositories`.
fn convert_rest_repos(rest: Vec<Option<RawRepositoryListItem>>) -> Vec<Repository> {
    // `null` entries decode to zero-valued structs in Go and are dropped for
    // their empty `node_id`; serde needs the explicit `Option`.
    rest.into_iter()
        .flatten()
        .filter(|repo| !repo.node_id.is_empty())
        .map(|repo| Repository {
            id: repo.node_id,
            visibility: repo.visibility,
        })
        .collect()
}

#[derive(Debug, Deserialize)]
struct RawRepositoryListItem {
    #[serde(default)]
    node_id: String,
    #[serde(default)]
    visibility: String,
}

#[derive(Debug, Deserialize)]
struct RawOrgMembership {
    #[serde(default)]
    state: String,
    #[serde(default)]
    role: String,
}

#[async_trait]
impl GithubUserClient for GithubRestClient {
    async fn list_affiliated_repositories(
        &self,
        page: i32,
        affiliations: &[&str],
    ) -> Result<(Vec<Repository>, bool), GithubClientError> {
        // `ListAffiliatedRepositories`: `user/repos?sort=created&visibility=private&page=N&per_page=100[&affiliation=a,b]`.
        let mut query = format!("sort=created&visibility=private&page={page}&per_page=100");
        if !affiliations.is_empty() {
            query.push_str("&affiliation=");
            query.push_str(&affiliations.join(","));
        }
        let (repos, has_next): (Vec<Option<RawRepositoryListItem>>, bool) =
            self.get_json("user/repos", Some(&query)).await?;
        Ok((convert_rest_repos(repos), has_next))
    }

    async fn get_authenticated_user_orgs_details_and_membership(
        &self,
        page: i32,
    ) -> Result<(Vec<OrgDetailsAndMembership>, bool), GithubClientError> {
        let (orgs, has_next) = self.get_authenticated_user_orgs(page).await?;
        let mut with_membership = Vec::with_capacity(orgs.len());
        for org in orgs {
            // The baseline calls `c.get` directly here (not `GetOrganization`),
            // so a 404 keeps its raw `APIError` text.
            let (details, _) = self
                .get_json::<Option<RawOrgDetails>>(&format!("/orgs/{}", org.login), None)
                .await?;
            let (membership, _) = self
                .get_json::<Option<RawOrgMembership>>(
                    &format!("/user/memberships/orgs/{}", org.login),
                    None,
                )
                .await?;
            // The baseline unmarshals into the embedded pointer fields, so a
            // JSON `null` leaves them nil.
            with_membership.push(OrgDetailsAndMembership {
                details: details.map(|details| OrgDetails {
                    login: details.login,
                    default_repository_permission: details.default_repository_permission,
                }),
                membership: membership.map(|membership| OrgMembership {
                    state: membership.state,
                    role: membership.role,
                }),
            });
        }
        Ok((with_membership, has_next))
    }

    async fn get_authenticated_user_orgs(
        &self,
        page: i32,
    ) -> Result<(Vec<Org>, bool), GithubClientError> {
        let (orgs, has_next): (Vec<RawOrg>, bool) = self
            .get_json("/user/orgs", Some(&format!("per_page=100&page={page}")))
            .await?;
        Ok((
            orgs.into_iter()
                .map(|org| Org { login: org.login })
                .collect(),
            has_next,
        ))
    }

    async fn get_authenticated_user_teams(
        &self,
        page: i32,
    ) -> Result<(Vec<Team>, bool), GithubClientError> {
        let (teams, has_next): (Vec<RawTeam>, bool) = self
            .get_json("/user/teams", Some(&format!("per_page=100&page={page}")))
            .await?;
        Ok((
            teams
                .into_iter()
                .map(|team| Team {
                    slug: team.slug,
                    repos_count: team.repos_count,
                    organization: team.organization.map(|org| Org { login: org.login }),
                })
                .collect(),
            has_next,
        ))
    }

    async fn list_org_repositories(
        &self,
        org: &str,
        page: i32,
        repo_type: &str,
    ) -> Result<(Vec<Repository>, bool), GithubClientError> {
        let (repos, has_next): (Vec<Option<RawRepositoryListItem>>, bool) = self
            .get_json(
                &format!("/orgs/{org}/repos"),
                Some(&format!(
                    "sort=created&page={page}&per_page=100&type={repo_type}"
                )),
            )
            .await?;
        Ok((convert_rest_repos(repos), has_next))
    }

    async fn list_team_repositories(
        &self,
        org: &str,
        team: &str,
        page: i32,
    ) -> Result<(Vec<Repository>, bool), GithubClientError> {
        let (repos, has_next): (Vec<Option<RawRepositoryListItem>>, bool) = self
            .get_json(
                &format!("/orgs/{org}/teams/{team}/repos"),
                Some(&format!("page={page}&per_page=100")),
            )
            .await?;
        Ok((convert_rest_repos(repos), has_next))
    }
}
