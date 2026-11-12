#![forbid(unsafe_code)]

//! `internal/extsvc/bitbucketserver`: the Bitbucket Server / Data Center REST
//! surface the repo-updater source needs.
//!
//! Ported: the client boundary (base URL + authenticator + injected HTTP),
//! `Repo`/`Project`/`Links` wire shapes, `PageToken` pagination, `Repos`
//! (repository queries), `ProjectRepos`, `LabeledRepos`, `Repo`,
//! `AuthenticatedUsername` (`X-Ausername`), `GetVersion`, and the exact
//! `Bitbucket API HTTP error: code=%d url=%q body=%q` text with
//! `IsNotFound`/`IsNoSuchLabel` classification.
//!
//! Not ported: OAuth 1 request signing (`SudoableOAuthClient`), the
//! self-imposed rate limiter, the metrics wrapper, custom TLS certificates,
//! and the pull-request/activity client surface. OAuth 1 configs fail at
//! construction instead of silently sending unauthenticated requests.

use std::collections::BTreeMap;
use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::auth::Authenticator;
use crate::{HttpDoer, HttpRequest, HttpResponse};

/// `extsvc.TypeBitbucketServer`.
pub const TYPE_BITBUCKETSERVER: &str = "bitbucketServer";

/// `bitbucketserver.Link`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Link {
    #[serde(default)]
    pub href: String,
    #[serde(default)]
    pub name: String,
}

/// `bitbucketserver.RepoLinks`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RepoLinks {
    /// Go `[]Link` with no `omitempty`: nil marshals as `null`.
    #[serde(default)]
    pub clone: Option<Vec<Link>>,
    #[serde(default, rename = "self")]
    pub self_: Option<Vec<SelfLink>>,
}

/// An anonymous `self` link.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SelfLink {
    #[serde(default)]
    pub href: String,
}

/// `bitbucketserver.Project`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Project {
    #[serde(default)]
    pub key: String,
    #[serde(default)]
    pub id: i32,
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub public: bool,
    #[serde(default, rename = "type")]
    pub kind: String,
    #[serde(default)]
    pub links: ProjectLinks,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProjectLinks {
    #[serde(default, rename = "self")]
    pub self_: Option<Vec<SelfLink>>,
}

/// `bitbucketserver.Repo`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Repo {
    #[serde(default)]
    pub slug: String,
    #[serde(default)]
    pub id: i32,
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub description: String,
    #[serde(default, rename = "scmId")]
    pub scm_id: String,
    #[serde(default)]
    pub state: String,
    #[serde(default, rename = "statusMessage")]
    pub status_message: String,
    #[serde(default)]
    pub forkable: bool,
    #[serde(default)]
    pub origin: Option<Box<Repo>>,
    #[serde(default)]
    pub project: Option<Project>,
    #[serde(default)]
    pub public: bool,
    #[serde(default)]
    pub links: RepoLinks,
}

impl Repo {
    /// `Repo.IsPersonalRepository`.
    pub fn is_personal_repository(&self) -> bool {
        self.project
            .as_ref()
            .map(|project| project.kind == "PERSONAL")
            .unwrap_or(false)
    }
}

/// `bitbucketserver.PageToken`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PageToken {
    #[serde(default)]
    pub size: i64,
    #[serde(default)]
    pub limit: i64,
    #[serde(default, rename = "isLastPage")]
    pub is_last_page: bool,
    #[serde(default)]
    pub start: i64,
    #[serde(default, rename = "nextPageStart")]
    pub next_page_start: i64,
}

impl PageToken {
    pub fn first(limit: i64) -> Self {
        Self {
            limit,
            ..Self::default()
        }
    }

    /// `PageToken.HasMore`.
    pub fn has_more(&self) -> bool {
        !self.is_last_page
    }

    /// `PageToken.Values`: `start` (when non-zero) and `limit`.
    pub fn values(&self) -> BTreeMap<String, Vec<String>> {
        let mut values = BTreeMap::new();
        if self.next_page_start != 0 {
            values.insert(
                "start".to_owned(),
                vec![self.next_page_start.to_string()],
            );
        }
        if self.limit != 0 {
            values.insert("limit".to_owned(), vec![self.limit.to_string()]);
        }
        values
    }
}

/// Go `%+v` formatting of `*PageToken` used in the repository-query error.
impl std::fmt::Display for PageToken {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{{Size:{} Limit:{} IsLastPage:{} Start:{} NextPageStart:{}}}",
            self.size, self.limit, self.is_last_page, self.start, self.next_page_start
        )
    }
}

/// `bitbucketserver.httpError`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HttpError {
    pub status_code: u16,
    pub url: String,
    pub body: String,
}

impl std::fmt::Display for HttpError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Bitbucket API HTTP error: code={} url={:?} body={:?}",
            self.status_code, self.url, self.body
        )
    }
}

/// Bitbucket Server client errors.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BitbucketError {
    Http(HttpError),
    Transport(String),
    Json(String),
    Auth(String),
    Invalid(String),
}

impl BitbucketError {
    /// `bitbucketserver.IsNotFound`.
    pub fn is_not_found(&self) -> bool {
        matches!(self, Self::Http(err) if err.status_code == 404)
    }

    /// `bitbucketserver.IsUnauthorized`.
    pub fn is_unauthorized(&self) -> bool {
        matches!(self, Self::Http(err) if err.status_code == 401)
    }

    /// `bitbucketserver.IsNoSuchLabel`.
    pub fn is_no_such_label(&self) -> bool {
        matches!(
            self,
            Self::Http(err) if err
                .body
                .contains("com.atlassian.bitbucket.label.NoSuchLabelException")
        )
    }
}

impl std::fmt::Display for BitbucketError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Http(err) => err.fmt(f),
            Self::Transport(err) | Self::Json(err) | Self::Auth(err) | Self::Invalid(err) => {
                f.write_str(err)
            }
        }
    }
}

impl std::error::Error for BitbucketError {}

/// `bitbucketserver.Client`.
#[derive(Clone)]
pub struct Client {
    url: url::Url,
    auth: Option<Arc<dyn Authenticator>>,
    http: Arc<dyn HttpDoer>,
}

#[derive(Debug, Deserialize)]
struct Page<T> {
    #[serde(flatten)]
    token: PageToken,
    #[serde(default)]
    values: Option<Vec<T>>,
}

impl Client {
    /// `bitbucketserver.NewClient` with the authenticator and transport
    /// injected.
    pub fn new(
        base_url: &str,
        auth: Option<Arc<dyn Authenticator>>,
        http: Arc<dyn HttpDoer>,
    ) -> Result<Self, String> {
        let url = url::Url::parse(base_url).map_err(|err| err.to_string())?;
        Ok(Self { url, auth, http })
    }

    pub fn base_url(&self) -> &url::Url {
        &self.url
    }

    /// `Client.Repos`: one page of a repository query.
    pub async fn repos(
        &self,
        token: &PageToken,
        queries: &[String],
    ) -> Result<(Vec<Repo>, PageToken), BitbucketError> {
        let query = parse_query_strings(queries)?;
        self.page("rest/api/1.0/repos", query, token).await
    }

    /// `Client.ProjectRepos`: every page of one project.
    pub async fn project_repos(
        &self,
        project_key: &str,
    ) -> Result<Vec<Repo>, BitbucketError> {
        if project_key.is_empty() {
            return Err(BitbucketError::Invalid("project key empty".to_owned()));
        }
        let path = format!("rest/api/1.0/projects/{project_key}/repos");
        let mut token = PageToken::first(1000);
        let mut repos = Vec::new();
        while token.has_more() {
            let (page, next) = self.page(&path, BTreeMap::new(), &token).await?;
            repos.extend(page);
            token = next;
        }
        Ok(repos)
    }

    /// `Client.Repo`.
    pub async fn repo(
        &self,
        project_key: &str,
        repo_slug: &str,
    ) -> Result<Repo, BitbucketError> {
        let path = format!("rest/api/1.0/projects/{project_key}/repos/{repo_slug}");
        let response = self.do_request("GET", &path, &BTreeMap::new()).await?;
        serde_json::from_slice(&response.body).map_err(|err| BitbucketError::Json(err.to_string()))
    }

    /// `Client.LabeledRepos`: one page of repositories carrying a label.
    pub async fn labeled_repos(
        &self,
        token: &PageToken,
        label: &str,
    ) -> Result<(Vec<Repo>, PageToken), BitbucketError> {
        let path = format!("rest/api/1.0/labels/{label}/labeled");
        let mut query = BTreeMap::new();
        query.insert("REPOSITORY".to_owned(), vec![String::new()]);
        self.page(&path, query, token).await
    }

    /// `Client.AuthenticatedUsername`: `X-Ausername` from `users?limit=1`.
    pub async fn authenticated_username(&self) -> Result<String, BitbucketError> {
        let mut query = BTreeMap::new();
        query.insert("limit".to_owned(), vec!["1".to_owned()]);
        let response = self
            .do_request("GET", "rest/api/1.0/users", &query)
            .await?;
        let username = response
            .headers
            .iter()
            .find(|(name, _)| name.eq_ignore_ascii_case("X-Ausername"))
            .map(|(_, value)| value.clone())
            .unwrap_or_default();
        if username.is_empty() {
            return Err(BitbucketError::Invalid(
                "no username in X-Ausername header".to_owned(),
            ));
        }
        Ok(username)
    }

    /// `Client.GetVersion`.
    pub async fn get_version(&self) -> Result<String, BitbucketError> {
        let response = self
            .do_request(
                "GET",
                "/rest/api/1.0/application-properties",
                &BTreeMap::new(),
            )
            .await?;
        let value: serde_json::Value = serde_json::from_slice(&response.body)
            .map_err(|err| BitbucketError::Json(err.to_string()))?;
        Ok(value
            .get("version")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default()
            .to_owned())
    }

    async fn page(
        &self,
        path: &str,
        query: BTreeMap<String, Vec<String>>,
        token: &PageToken,
    ) -> Result<(Vec<Repo>, PageToken), BitbucketError> {
        // `Client.page` merges the page-token query values (`start` when set,
        // `limit`) into every page request, including `ProjectRepos`, whose
        // first request is `?limit=1000`.
        let mut query = query;
        merge_values(&mut query, token.values());
        let response = self.do_request("GET", path, &query).await?;
        let page: Page<Repo> = serde_json::from_slice(&response.body)
            .map_err(|err| BitbucketError::Json(err.to_string()))?;
        Ok((page.values.unwrap_or_default(), page.token))
    }

    /// `Client.do`: join the base path, authenticate, send, and map non-2xx
    /// responses to `httpError`.
    async fn do_request(
        &self,
        method: &str,
        path: &str,
        query: &BTreeMap<String, Vec<String>>,
    ) -> Result<HttpResponse, BitbucketError> {
        let url = self.request_url(path, query);
        let mut headers = vec![(
            "Content-Type".to_owned(),
            "application/json; charset=utf-8".to_owned(),
        )];

        if let Some(auth) = &self.auth {
            auth.authenticate(method, &url, &mut headers)
                .map_err(BitbucketError::Auth)?;
        }

        let response = self
            .http
            .do_request(HttpRequest {
                method: method.to_owned(),
                url: url.clone(),
                headers,
                body: Vec::new(),
            })
            .await
            .map_err(BitbucketError::Transport)?;

        if !(200..400).contains(&response.status) {
            return Err(BitbucketError::Http(HttpError {
                status_code: response.status,
                url,
                body: String::from_utf8_lossy(&response.body).into_owned(),
            }));
        }
        Ok(response)
    }

    /// `url.JoinPath(c.URL.Path, path)` + `ResolveReference`.
    fn request_url(&self, path: &str, query: &BTreeMap<String, Vec<String>>) -> String {
        let joined = join_path(self.url.path(), path);
        let mut url = self.url.clone();
        url.set_path(&joined);
        url.set_query(None);
        let encoded = encode_values(query);
        if !encoded.is_empty() {
            url.set_query(Some(&encoded));
        }
        url.to_string()
    }
}

/// Go `url.JoinPath` path joining, including dot-segment cleaning. Query and
/// fragment are not part of the client's paths.
pub fn join_path(base: &str, element: &str) -> String {
    let combined = format!(
        "{}/{}",
        base.trim_end_matches('/'),
        element.trim_start_matches('/')
    );
    clean_path(&combined)
}

fn clean_path(path: &str) -> String {
    let mut segments: Vec<&str> = Vec::new();
    for segment in path.split('/') {
        match segment {
            "" | "." => continue,
            ".." => {
                segments.pop();
            }
            other => segments.push(other),
        }
    }
    let mut cleaned = String::from("/");
    cleaned.push_str(&segments.join("/"));
    if path.ends_with('/') && !cleaned.ends_with('/') {
        cleaned.push('/');
    }
    cleaned
}

/// `url.ParseQuery(strings.TrimPrefix(q, "?"))`, merged into one value set.
pub fn parse_query_strings(queries: &[String]) -> Result<BTreeMap<String, Vec<String>>, BitbucketError> {
    let mut values: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for query in queries {
        for (key, value) in
            url::form_urlencoded::parse(query.trim_start_matches('?').as_bytes())
        {
            values
                .entry(key.into_owned())
                .or_default()
                .push(value.into_owned());
        }
    }
    Ok(values)
}

/// `url.Values.Encode` over the ordered map.
pub fn encode_values(values: &BTreeMap<String, Vec<String>>) -> String {
    let mut serializer = url::form_urlencoded::Serializer::new(String::new());
    for (key, entries) in values {
        for value in entries {
            serializer.append_pair(key, value);
        }
    }
    serializer.finish()
}

fn merge_values(
    target: &mut BTreeMap<String, Vec<String>>,
    source: BTreeMap<String, Vec<String>>,
) {
    for (key, values) in source {
        target.entry(key).or_default().extend(values);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn page_token_values_and_display() {
        let token = PageToken {
            size: 1,
            limit: 1000,
            is_last_page: false,
            start: 0,
            next_page_start: 50,
        };
        assert_eq!(
            encode_values(&token.values()),
            "limit=1000&start=50"
        );
        assert_eq!(
            token.to_string(),
            "{Size:1 Limit:1000 IsLastPage:false Start:0 NextPageStart:50}"
        );
        assert!(token.has_more());
    }

    #[test]
    fn repo_metadata_serializes_go_field_names() {
        let repo = Repo {
            slug: "r".to_owned(),
            id: 3,
            project: Some(Project {
                key: "P".to_owned(),
                ..Project::default()
            }),
            ..Repo::default()
        };
        let value = serde_json::to_value(&repo).unwrap();
        assert_eq!(value["scmId"], "");
        assert_eq!(value["project"]["key"], "P");
        assert!(value["origin"].is_null());
        assert!(value["links"]["clone"].is_null());
        assert!(value["links"]["self"].is_null());
    }

    #[test]
    fn http_error_text() {
        let err = HttpError {
            status_code: 404,
            url: "https://b/rest/api/1.0/repos".to_owned(),
            body: "nope".to_owned(),
        };
        assert_eq!(
            err.to_string(),
            "Bitbucket API HTTP error: code=404 url=\"https://b/rest/api/1.0/repos\" body=\"nope\""
        );
    }

    #[test]
    fn join_path_keeps_the_base_path() {
        assert_eq!(
            join_path("/bitbucket/", "/rest/api/1.0/repos"),
            "/bitbucket/rest/api/1.0/repos"
        );
        assert_eq!(join_path("/", "rest/api/1.0/repos"), "/rest/api/1.0/repos");
        assert_eq!(
            join_path("/base", "rest//api/./1.0/../1.0/repos"),
            "/base/rest/api/1.0/repos"
        );
    }
}
