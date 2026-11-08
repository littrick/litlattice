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

use async_trait::async_trait;
use reqwest::header::{HeaderValue, ACCEPT, AUTHORIZATION, CACHE_CONTROL, CONTENT_TYPE, LINK};
use reqwest::StatusCode;
use serde::de::DeserializeOwned;
use serde::Deserialize;

use super::authz_github::{
    Collaborator, GithubClient, GithubClientError, Org, OrgDetails, Repository, Team,
};
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

/// `internal/extsvc/github.V3Client`, reduced to the authz methods.
pub struct GithubRestClient {
    http: reqwest::Client,
    api_url: reqwest::Url,
    token: Option<String>,
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
            token,
            enable_github_internal_repo_visibility,
        }
    }

    pub fn api_url(&self) -> &reqwest::Url {
        &self.api_url
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

    /// `doRequest` + `json.Decoder.Decode`: returns the decoded body and
    /// whether the `Link` header advertises a next page.
    async fn get_json<T: DeserializeOwned>(
        &self,
        path: &str,
        query: Option<&str>,
    ) -> Result<(T, bool), GithubClientError> {
        let url = self.request_url(path, query);

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
        if let Some(token) = &self.token {
            let value = HeaderValue::from_str(&format!("Bearer {token}"))
                .map_err(|err| GithubClientError::new(format!("authenticating request: {err}")))?;
            request = request.header(AUTHORIZATION, value);
        }

        let response = request
            .send()
            .await
            .map_err(|err| GithubClientError::new(format!("request failed: {err}")))?;

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
    let message = match response.text().await {
        Ok(body) => match serde_json::from_str::<RawError>(&body) {
            Ok(error) => error.message.unwrap_or_default(),
            Err(err) => format!("failed to decode error response from GitHub API: {err}: {body:?}"),
        },
        Err(err) => format!("failed to read error response from GitHub API: {err}"),
    };
    GithubClientError {
        message: format!(
            "request to {url} returned status {}: {message}",
            status.as_u16()
        ),
        not_found: status == StatusCode::NOT_FOUND,
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
            }),
            Err(err) => Err(err),
        }
    }
}
