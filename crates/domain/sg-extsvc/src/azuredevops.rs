#![forbid(unsafe_code)]

//! `internal/extsvc/azuredevops`: the code-host client shared by the
//! repo-updater source (M9.13c) and the Batch Changes changeset source
//! (M9.24b).
//!
//! Ported: configuration shape, `urlx.Parse` base-URL normalization, Basic
//! authentication (PAT as the password), request building (`api-version=7.0`
//! merging plus Go's `ResolveReference` semantics), the
//! `x-ms-continuationtoken` capture used by branch listing, `429` retry
//! (bounded like `maxRateLimitRetries`), `HTTPError` text, repository and
//! project listings, `GetAuthorizedProfile` (used by `CheckConnection`), and
//! the pull-request/fork write surface (`getPullRequest`,
//! `createPullRequest`, `updatePullRequest`, `abandonPullRequest`,
//! `completePullRequest`, `createPullRequestCommentThread`,
//! `getPullRequestStatuses`, `forkRepository`).
//!
//! Not ported: the internal/external rate limiters, OAuth token refresh,
//! organization listing and the webhook event parser (the Batch Changes
//! source does not consume them).

use std::collections::BTreeMap;
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use url::Url;

use crate::auth::{Authenticator, BasicAuth, BasicAuthWithSSH};
use crate::{HttpDoer, HttpRequest, HttpResponse};

/// `extsvc.TypeAzureDevOps` (stored in `repo.external_service_type`).
pub const TYPE_AZUREDEVOPS: &str = "azuredevops";

/// `azuredevops.AzureDevOpsAPIURL`.
pub const AZURE_DEVOPS_API_URL: &str = "https://dev.azure.com/";
/// `azuredevops.VisualStudioAppURL`.
pub const VISUAL_STUDIO_APP_URL: &str = "https://app.vssps.visualstudio.com/";
/// `azuredevops.apiVersion`.
pub const API_VERSION: &str = "7.0";
/// `azuredevops.continuationTokenHeader`.
pub const CONTINUATION_TOKEN_HEADER: &str = "x-ms-continuationtoken";

/// `azuredevops.PullRequestStatusActive`.
pub const PULL_REQUEST_STATUS_ACTIVE: &str = "active";
/// `azuredevops.PullRequestStatusAbandoned`.
pub const PULL_REQUEST_STATUS_ABANDONED: &str = "abandoned";
/// `azuredevops.PullRequestStatusCompleted`.
pub const PULL_REQUEST_STATUS_COMPLETED: &str = "completed";
/// `azuredevops.PullRequestMergeStrategySquash`.
pub const PULL_REQUEST_MERGE_STRATEGY_SQUASH: &str = "squash";

/// The authenticator an [`AzureDevOpsAuth`] applies. The baseline only
/// supports `auth.BasicAuth` and `auth.BasicAuthWithSSH`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AzureDevOpsAuth {
    Basic(BasicAuth),
    BasicWithSsh(BasicAuthWithSSH),
}

impl Default for AzureDevOpsAuth {
    fn default() -> Self {
        Self::Basic(BasicAuth::default())
    }
}

impl AzureDevOpsAuth {
    pub fn authenticate(
        &self,
        method: &str,
        url: &str,
        headers: &mut Vec<(String, String)>,
    ) -> Result<(), String> {
        match self {
            Self::Basic(auth) => auth.authenticate(method, url, headers),
            Self::BasicWithSsh(auth) => auth.authenticate(method, url, headers),
        }
    }
}

/// `schema.ExcludedAzureDevOpsServerRepo`.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct ExcludedAzureDevOpsServerRepo {
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub pattern: String,
}

/// `azuredevops.Project`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Project {
    #[serde(default)]
    pub id: String,
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub state: String,
    #[serde(default)]
    pub revision: i64,
    #[serde(default)]
    pub visibility: String,
    #[serde(default)]
    pub url: String,
}

/// `azuredevops.Repository`. Field names mirror the Go JSON tags because the
/// value is stored in `repo.metadata`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Repository {
    #[serde(default)]
    pub id: String,
    #[serde(default)]
    pub name: String,
    #[serde(rename = "remoteURL", default)]
    pub remote_url: String,
    /// Go's `APIURL` has the JSON tag `url`.
    #[serde(rename = "url", default)]
    pub api_url: String,
    #[serde(rename = "sshUrl", default)]
    pub ssh_url: String,
    #[serde(rename = "webUrl", default)]
    pub web_url: String,
    #[serde(rename = "isDisabled", default)]
    pub is_disabled: bool,
    #[serde(rename = "isFork", default)]
    pub is_fork: bool,
    #[serde(default)]
    pub project: Project,
}

impl Repository {
    /// `Repository.GetOrganization`: split the API URL path into
    /// `["", org, rest]` and require exactly three segments.
    pub fn get_organization(&self) -> Result<String, String> {
        let url = Url::parse(&self.api_url).map_err(|err| err.to_string())?;
        let parts: Vec<&str> = url.path().splitn(3, '/').collect();
        if parts.len() != 3 {
            return Err(format!(
                "unable to parse Azure DevOps organization from repo URL: {}",
                self.api_url
            ));
        }
        Ok(parts[1].to_owned())
    }

    /// `Repository.Namespace`.
    pub fn namespace(&self) -> String {
        self.project.name.clone()
    }
}

/// `azuredevops.ListRepositoriesResponse`.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct ListRepositoriesResponse {
    #[serde(default)]
    pub value: Vec<Repository>,
    #[serde(default)]
    pub count: i64,
}

/// `azuredevops.ListRefsResponse`.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct ListRefsResponse {
    #[serde(default)]
    pub value: Vec<Ref>,
    #[serde(default)]
    pub count: i64,
}

/// `azuredevops.Ref`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Ref {
    #[serde(default)]
    pub name: String,
    #[serde(rename = "objectId", default)]
    pub commit_sha: String,
}

/// `azuredevops.Profile` (the fields the connection check decodes).
#[derive(Debug, Clone, Default, Deserialize)]
pub struct Profile {
    #[serde(default)]
    pub id: String,
    #[serde(rename = "displayName", default)]
    pub display_name: String,
    #[serde(rename = "emailAddress", default)]
    pub email_address: String,
    #[serde(rename = "publicAlias", default)]
    pub public_alias: String,
}

/// `azuredevops.CreatorInfo`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct CreatorInfo {
    #[serde(default)]
    pub id: String,
    #[serde(rename = "displayName", default)]
    pub display_name: String,
    #[serde(rename = "uniqueName", default)]
    pub unique_name: String,
    #[serde(default)]
    pub url: String,
    #[serde(rename = "imageUrl", default)]
    pub image_url: String,
}

/// `azuredevops.Reviewer`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Reviewer {
    #[serde(default)]
    pub vote: i64,
    #[serde(default)]
    pub id: String,
    #[serde(rename = "hasDeclined", default)]
    pub has_declined: bool,
    #[serde(rename = "isRequired", default)]
    pub is_required: bool,
    #[serde(rename = "uniqueName", default)]
    pub unique_name: String,
}

/// `azuredevops.PullRequestCommit`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PullRequestCommit {
    #[serde(rename = "commitId", default)]
    pub commit_id: String,
    #[serde(default)]
    pub url: String,
}

/// `azuredevops.ForkRef`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ForkRef {
    #[serde(default)]
    pub repository: Repository,
    #[serde(default)]
    pub name: String,
    /// Go's field is `URl` but the JSON tag is `url`.
    #[serde(default)]
    pub url: String,
}

/// `azuredevops.PullRequest`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PullRequest {
    #[serde(default)]
    pub repository: Repository,
    #[serde(rename = "pullRequestId", default)]
    pub id: i64,
    #[serde(rename = "codeReviewId", default)]
    pub code_review_id: i64,
    #[serde(default)]
    pub status: String,
    #[serde(rename = "creationDate", default)]
    pub creation_date: String,
    #[serde(default)]
    pub title: String,
    #[serde(default)]
    pub description: String,
    #[serde(rename = "createdBy", default)]
    pub created_by: CreatorInfo,
    #[serde(rename = "sourceRefName", default)]
    pub source_ref_name: String,
    #[serde(rename = "targetRefName", default)]
    pub target_ref_name: String,
    #[serde(rename = "mergeStatus", default)]
    pub merge_status: String,
    #[serde(rename = "mergeId", default)]
    pub merge_id: String,
    #[serde(rename = "lastMergeSourceCommit", default)]
    pub last_merge_source_commit: PullRequestCommit,
    #[serde(rename = "lastMergeTargetCommit", default)]
    pub last_merge_target_commit: PullRequestCommit,
    #[serde(rename = "supportsIterations", default)]
    pub supports_iterations: bool,
    #[serde(rename = "artifactId", default)]
    pub artifact_id: String,
    #[serde(default)]
    pub reviewers: Vec<Reviewer>,
    #[serde(rename = "forkSource", default)]
    pub fork_source: Option<ForkRef>,
    #[serde(default)]
    pub url: String,
    #[serde(rename = "isDraft", default)]
    pub is_draft: bool,
}

/// `azuredevops.PullRequestCompletionOptions`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PullRequestCompletionOptions {
    #[serde(rename = "mergeStrategy", default, skip_serializing_if = "String::is_empty")]
    pub merge_strategy: String,
    #[serde(
        rename = "deleteSourceBranch",
        default,
        skip_serializing_if = "is_false"
    )]
    pub delete_source_branch: bool,
    #[serde(rename = "mergeCommitMessage", default)]
    pub merge_commit_message: String,
}

fn is_false(value: &bool) -> bool {
    !*value
}

/// `azuredevops.CreatePullRequestInput`. Fields without `omitempty` in the
/// baseline (including nil slices/pointers) serialise as JSON `null`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct CreatePullRequestInput {
    #[serde(rename = "sourceRefName")]
    pub source_ref_name: String,
    #[serde(rename = "targetRefName")]
    pub target_ref_name: String,
    #[serde(default)]
    pub title: String,
    #[serde(default)]
    pub description: String,
    #[serde(default)]
    pub reviewers: Option<Vec<Reviewer>>,
    #[serde(rename = "forkSource", default)]
    pub fork_source: Option<ForkRef>,
    #[serde(rename = "isDraft", default)]
    pub is_draft: bool,
    #[serde(rename = "completionOptions", default)]
    pub completion_options: Option<PullRequestCompletionOptions>,
}

/// `azuredevops.PullRequestUpdateInput`. Every field is a pointer in Go, so
/// `None` serialises as `null`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PullRequestUpdateInput {
    #[serde(default)]
    pub status: Option<String>,
    #[serde(default)]
    pub title: Option<String>,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(rename = "mergeOptions", default)]
    pub merge_options: Option<serde_json::Value>,
    #[serde(rename = "lastMergeSourceCommit", default)]
    pub last_merge_source_commit: Option<PullRequestCommit>,
    #[serde(rename = "targetRefName", default)]
    pub target_ref_name: Option<String>,
    #[serde(rename = "isDraft", default)]
    pub is_draft: Option<bool>,
    #[serde(rename = "completionOptions", default)]
    pub completion_options: Option<PullRequestCompletionOptions>,
}

/// `azuredevops.PullRequestCompleteInput`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PullRequestCompleteInput {
    pub commit_id: String,
    pub merge_strategy: Option<String>,
    pub delete_source_branch: bool,
}

/// `azuredevops.PullRequestCommentForInput`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PullRequestCommentForInput {
    #[serde(rename = "parentCommentId", default)]
    pub parent_comment_id: i64,
    #[serde(default)]
    pub content: String,
    #[serde(rename = "commentType", default)]
    pub comment_type: i64,
}

/// `azuredevops.PullRequestCommentInput`. Note the capitalised `Comments`
/// key, matching the Go struct tag.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PullRequestCommentInput {
    #[serde(rename = "Comments", default)]
    pub comments: Vec<PullRequestCommentForInput>,
}

/// `azuredevops.PullRequestCommentResponse` (only the fields the source does
/// not consume are omitted).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PullRequestCommentResponse {
    #[serde(default)]
    pub id: i64,
    #[serde(rename = "Comments", default)]
    pub comments: Vec<PullRequestCommentForResponse>,
    #[serde(rename = "publishedDate", default)]
    pub published_date: String,
    #[serde(rename = "lastUpdatedOn", default)]
    pub last_updated_on: String,
    #[serde(rename = "isDeleted", default)]
    pub is_deleted: bool,
}

/// `azuredevops.PullRequestCommentForResponse`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PullRequestCommentForResponse {
    #[serde(default)]
    pub id: i64,
    #[serde(rename = "publishedDate", default)]
    pub published_date: String,
    #[serde(rename = "lastUpdatedOn", default)]
    pub last_updated_on: String,
    #[serde(default)]
    pub content: String,
    #[serde(rename = "commentType", default)]
    pub comment_type: String,
}

/// `azuredevops.PullRequestStatuses`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PullRequestStatuses {
    #[serde(default)]
    pub value: Vec<PullRequestBuildStatus>,
    #[serde(default)]
    pub count: i64,
}

/// `azuredevops.PullRequestBuildStatus`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PullRequestBuildStatus {
    #[serde(default)]
    pub id: i64,
    #[serde(default)]
    pub state: String,
    #[serde(default)]
    pub description: String,
    #[serde(rename = "creationDate", default)]
    pub creation_date: String,
    #[serde(rename = "updatedDate", default)]
    pub updated_date: String,
    #[serde(rename = "createdBy", default)]
    pub created_by: CreatorInfo,
}

/// `azuredevops.ForkRepositoryInputProject`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ForkRepositoryInputProject {
    #[serde(default)]
    pub id: String,
}

/// `azuredevops.ForkRepositoryInputParentRepository`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ForkRepositoryInputParentRepository {
    #[serde(default)]
    pub id: String,
    #[serde(default)]
    pub project: ForkRepositoryInputProject,
}

/// `azuredevops.ForkRepositoryInput`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ForkRepositoryInput {
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub project: ForkRepositoryInputProject,
    #[serde(rename = "parentRepository", default)]
    pub parent_repository: ForkRepositoryInputParentRepository,
}

/// `azuredevops.HTTPError`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HTTPError {
    pub status_code: u16,
    pub url: String,
    pub body: Vec<u8>,
}

impl HTTPError {
    pub fn unauthorized(&self) -> bool {
        self.status_code == 401
    }

    pub fn not_found(&self) -> bool {
        self.status_code == 404
    }
}

impl std::fmt::Display for HTTPError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Azure DevOps API HTTP error: code={} url={:?}",
            self.status_code, self.url
        )
    }
}

impl std::error::Error for HTTPError {}

/// Errors produced by the client.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClientError {
    /// A non-2xx/3xx response.
    Http(HTTPError),
    /// A transport, URL or JSON error.
    Other(String),
}

impl std::fmt::Display for ClientError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Http(err) => err.fmt(f),
            Self::Other(err) => f.write_str(err),
        }
    }
}

impl std::error::Error for ClientError {}

impl ClientError {
    /// `errcode.IsNotFound`: the baseline's `HTTPError.NotFound`.
    pub fn is_not_found(&self) -> bool {
        matches!(self, Self::Http(err) if err.status_code == 404)
    }
}

impl From<HTTPError> for ClientError {
    fn from(err: HTTPError) -> Self {
        Self::Http(err)
    }
}

/// A `urlx.Parse` result: the parsed [`Url`] plus the Go `url.URL.String()`
/// rendering. Rust's `Url` always materialises a `/` path, while Go keeps an
/// empty path for `https://host`; the source compares and concatenates the Go
/// rendering, so both representations are kept.
#[derive(Debug, Clone)]
pub struct BaseUrl {
    pub url: Url,
    pub string: String,
}

impl BaseUrl {
    pub fn parse(raw: &str) -> Result<Self, String> {
        let mut value = raw.to_owned();
        if value.starts_with("//") {
            value = format!("http:{value}");
        } else if !value.contains("://") {
            value = format!("http://{value}");
        }
        let url = Url::parse(&value).map_err(|err| err.to_string())?;

        // `had_path` mirrors Go's `URL.String()`, which only emits the path
        // when it is non-empty (a bare "https://host" has an empty path).
        let after_scheme = &value[value.find("://").map(|index| index + 3).unwrap_or(0)..];
        let authority_end = after_scheme.find('/').unwrap_or(after_scheme.len());
        let had_path = after_scheme[authority_end..].starts_with('/');

        let mut string = String::new();
        string.push_str(url.scheme());
        string.push_str("://");
        if !url.username().is_empty() || url.password().is_some() {
            string.push_str(url.username());
            if let Some(password) = url.password() {
                string.push(':');
                string.push_str(password);
            }
            string.push('@');
        }
        if let Some(host) = url.host_str() {
            string.push_str(host);
        }
        if let Some(port) = url.port() {
            string.push(':');
            string.push_str(&port.to_string());
        }
        if had_path {
            string.push_str(url.path());
        }
        if let Some(query) = url.query() {
            string.push('?');
            string.push_str(query);
        }
        if let Some(fragment) = url.fragment() {
            string.push('#');
            string.push_str(fragment);
        }

        Ok(Self { url, string })
    }

    /// `extsvc.NormalizeBaseURL`. The baseline source constructor calls it on
    /// the client's `*url.URL`, which *mutates* the client in place: a URL
    /// without a trailing slash gains one, and `makeRepo` later concatenates
    /// `GetURL().String()`. Return the normalized value so both the parsed URL
    /// and the Go rendering stay in sync.
    pub fn normalized(&self) -> Self {
        let url = crate::github::normalize_base_url(&self.url);
        let string = url.to_string();
        Self { url, string }
    }
}

/// `urlx.Parse`: force a default `http` scheme and let `Url::parse` lowercase
/// the scheme and host. Host validation/punycode normalization is not
/// reproduced.
pub fn parse_urlx(raw: &str) -> Result<BaseUrl, String> {
    BaseUrl::parse(raw)
}

/// `url.Values.Encode` over an ordered key set: keys sorted lexicographically
/// and values percent-encoded like Go's `QueryEscape`.
pub fn encode_query(pairs: &[(&str, String)]) -> String {
    let mut values: BTreeMap<&str, &str> = BTreeMap::new();
    for (key, value) in pairs {
        values.insert(key, value);
    }
    let mut serializer = url::form_urlencoded::Serializer::new(String::new());
    for (key, value) in &values {
        serializer.append_pair(key, value);
    }
    serializer.finish()
}

/// `azuredevops.Client` for one authenticator.
#[derive(Clone)]
pub struct Client {
    base_url: BaseUrl,
    auth: AzureDevOpsAuth,
    http: Arc<dyn HttpDoer>,
}

impl Client {
    /// `azuredevops.NewClient`, minus the rate limiters.
    pub fn new(base_url: BaseUrl, auth: AzureDevOpsAuth, http: Arc<dyn HttpDoer>) -> Self {
        Self {
            base_url,
            auth,
            http,
        }
    }

    pub fn base_url(&self) -> &Url {
        &self.base_url.url
    }

    /// The Go `url.URL.String()` rendering used by `makeRepo`.
    pub fn base_url_string(&self) -> &str {
        &self.base_url.string
    }

    pub fn authenticator(&self) -> &AzureDevOpsAuth {
        &self.auth
    }

    /// `Client.WithAuthenticator`.
    pub fn with_authenticator(&self, auth: AzureDevOpsAuth) -> Self {
        Self {
            base_url: self.base_url.clone(),
            auth,
            http: Arc::clone(&self.http),
        }
    }

    /// `IsAzureDevOpsServices`: an exact string compare against
    /// `https://dev.azure.com/` (the constant keeps the trailing slash), like
    /// Go.
    pub fn is_azure_devops_services(&self) -> bool {
        self.base_url.string == AZURE_DEVOPS_API_URL
    }

    /// `Client.do`: resolve the relative request against the base (or the
    /// override), add `api-version`, authenticate and decode. Returns the
    /// `x-ms-continuationtoken` header value.
    async fn do_request(
        &self,
        method: &str,
        relative: &str,
        body: Option<Vec<u8>>,
        url_override: Option<&str>,
    ) -> Result<(Option<String>, Vec<u8>), ClientError> {
        let base = match url_override {
            Some(raw) => Url::parse(raw).map_err(|err| ClientError::Other(err.to_string()))?,
            None => self.base_url.url.clone(),
        };
        let mut url = base
            .join(relative)
            .map_err(|err| ClientError::Other(err.to_string()))?;

        // `queryParams.Set("api-version", apiVersion); RawQuery = Encode()`.
        let mut values: BTreeMap<String, Vec<String>> = BTreeMap::new();
        for (key, value) in url.query_pairs() {
            values
                .entry(key.into_owned())
                .or_default()
                .push(value.into_owned());
        }
        values.insert(API_VERSION_KEY.to_owned(), vec![API_VERSION.to_owned()]);
        let encoded_query = {
            let mut serializer = url::form_urlencoded::Serializer::new(String::new());
            for (key, entries) in &values {
                for value in entries {
                    serializer.append_pair(key, value);
                }
            }
            serializer.finish()
        };
        url.set_query(Some(&encoded_query));
        let url_string = url.to_string();

        let mut retries = 0u8;
        loop {
            let mut headers = Vec::new();
            if body.is_some() {
                headers.push(("Content-Type".to_owned(), "application/json".to_owned()));
            }
            self.auth
                .authenticate(method, &url_string, &mut headers)
                .map_err(ClientError::Other)?;

            let response = self
                .http
                .do_request(HttpRequest {
                    method: method.to_owned(),
                    url: url_string.clone(),
                    headers,
                    body: body.clone().unwrap_or_default(),
                })
                .await
                .map_err(ClientError::Other)?;

            // `for c.waitForRateLimit && resp.StatusCode == 429 && numRetries < 2`.
            if response.status == 429 && retries < 2 {
                retries += 1;
                continue;
            }

            if !(200..400).contains(&response.status) {
                return Err(ClientError::Http(HTTPError {
                    status_code: response.status,
                    url: url_string,
                    body: response.body,
                }));
            }

            let continuation = header_value(&response, CONTINUATION_TOKEN_HEADER);
            return Ok((continuation, response.body));
        }
    }

    async fn do_get(
        &self,
        relative: &str,
        url_override: Option<&str>,
    ) -> Result<(Option<String>, Vec<u8>), ClientError> {
        self.do_request("GET", relative, None, url_override).await
    }

    /// `ListRepositoriesByProjectOrOrg`. The baseline does not follow the
    /// continuation token here.
    pub async fn list_repositories_by_project_or_org(
        &self,
        project_or_org_name: &str,
    ) -> Result<Vec<Repository>, ClientError> {
        let relative = format!("{project_or_org_name}/_apis/git/repositories");
        let (_, body) = self.do_get(&relative, None).await?;
        let response: ListRepositoriesResponse = serde_json::from_slice(&body)
            .map_err(|err| ClientError::Other(err.to_string()))?;
        Ok(response.value)
    }

    /// `GetRepo`.
    pub async fn get_repo(
        &self,
        org: &str,
        project: &str,
        repo_name_or_id: &str,
    ) -> Result<Repository, ClientError> {
        let relative = format!("{org}/{project}/_apis/git/repositories/{repo_name_or_id}");
        let (_, body) = self.do_get(&relative, None).await?;
        serde_json::from_slice(&body).map_err(|err| ClientError::Other(err.to_string()))
    }

    /// `GetProject`.
    pub async fn get_project(&self, org: &str, project: &str) -> Result<Project, ClientError> {
        let relative = format!("{org}/_apis/projects/{project}");
        let (_, body) = self.do_get(&relative, None).await?;
        serde_json::from_slice(&body).map_err(|err| ClientError::Other(err.to_string()))
    }

    /// `GetRepositoryBranch`: the only continuation-token paginated endpoint
    /// the client exposes.
    pub async fn get_repository_branch(
        &self,
        org: &str,
        project: &str,
        repo_name_or_id: &str,
        branch_name: &str,
    ) -> Result<Ref, ClientError> {
        let mut all_refs = Vec::new();
        let mut continuation_token = String::new();
        loop {
            let mut pairs = vec![("filter", format!("heads/{branch_name}"))];
            if !continuation_token.is_empty() {
                pairs.push(("continuationToken", continuation_token.clone()));
            }
            let query = encode_query(&pairs);
            let relative = format!(
                "{org}/{project}/_apis/git/repositories/{repo_name_or_id}/refs?{query}"
            );
            let (next, body) = self.do_get(&relative, None).await?;
            let refs: ListRefsResponse = serde_json::from_slice(&body)
                .map_err(|err| ClientError::Other(err.to_string()))?;
            all_refs.extend(refs.value);
            continuation_token = next.unwrap_or_default();
            if continuation_token.is_empty() {
                break;
            }
        }

        let wanted = format!("refs/heads/{branch_name}");
        for reference in all_refs {
            if reference.name == wanted {
                return Ok(reference);
            }
        }
        Err(ClientError::Other(format!(
            "branch {branch_name:?} not found"
        )))
    }

    /// `GetAuthorizedProfile`. The request host is `VisualStudioAppURL`, not
    /// the configured Azure DevOps URL.
    pub async fn get_authorized_profile(&self) -> Result<Profile, ClientError> {
        let (_, body) = self
            .do_get("/_apis/profile/profiles/me", Some(VISUAL_STUDIO_APP_URL))
            .await?;
        serde_json::from_slice(&body).map_err(|err| ClientError::Other(err.to_string()))
    }

    /// `GetPullRequest`.
    pub async fn get_pull_request(
        &self,
        org: &str,
        project: &str,
        repo_name_or_id: &str,
        pull_request_id: &str,
    ) -> Result<PullRequest, ClientError> {
        let relative = pull_request_path(org, project, repo_name_or_id, pull_request_id);
        let (_, body) = self.do_request("GET", &relative, None, None).await?;
        decode_body(&body)
    }

    /// `GetPullRequestStatuses`.
    pub async fn get_pull_request_statuses(
        &self,
        org: &str,
        project: &str,
        repo_name_or_id: &str,
        pull_request_id: &str,
    ) -> Result<Vec<PullRequestBuildStatus>, ClientError> {
        let relative = format!(
            "{}/statuses",
            pull_request_path(org, project, repo_name_or_id, pull_request_id)
        );
        let (_, body) = self.do_request("GET", &relative, None, None).await?;
        let statuses: PullRequestStatuses = decode_body(&body)?;
        Ok(statuses.value)
    }

    /// `CreatePullRequest`. The baseline documents that the API wants the
    /// repository ID, but the source passes the repository name.
    pub async fn create_pull_request(
        &self,
        org: &str,
        project: &str,
        repo_name_or_id: &str,
        input: &CreatePullRequestInput,
    ) -> Result<PullRequest, ClientError> {
        let relative = format!("{org}/{project}/_apis/git/repositories/{repo_name_or_id}/pullrequests");
        let (_, body) = self
            .do_request("POST", &relative, Some(encode_body(input)?), None)
            .await?;
        decode_body(&body)
    }

    /// `UpdatePullRequest`.
    pub async fn update_pull_request(
        &self,
        org: &str,
        project: &str,
        repo_name_or_id: &str,
        pull_request_id: &str,
        input: &PullRequestUpdateInput,
    ) -> Result<PullRequest, ClientError> {
        let relative = pull_request_path(org, project, repo_name_or_id, pull_request_id);
        let (_, body) = self
            .do_request("PATCH", &relative, Some(encode_body(input)?), None)
            .await?;
        decode_body(&body)
    }

    /// `AbandonPullRequest`.
    pub async fn abandon_pull_request(
        &self,
        org: &str,
        project: &str,
        repo_name_or_id: &str,
        pull_request_id: &str,
    ) -> Result<PullRequest, ClientError> {
        let relative = pull_request_path(org, project, repo_name_or_id, pull_request_id);
        let input = PullRequestUpdateInput {
            status: Some(PULL_REQUEST_STATUS_ABANDONED.to_owned()),
            ..Default::default()
        };
        let (_, body) = self
            .do_request("PATCH", &relative, Some(encode_body(&input)?), None)
            .await?;
        decode_body(&body)
    }

    /// `CreatePullRequestCommentThread`.
    pub async fn create_pull_request_comment_thread(
        &self,
        org: &str,
        project: &str,
        repo_name_or_id: &str,
        pull_request_id: &str,
        input: &PullRequestCommentInput,
    ) -> Result<PullRequestCommentResponse, ClientError> {
        let relative = format!(
            "{}/threads",
            pull_request_path(org, project, repo_name_or_id, pull_request_id)
        );
        let (_, body) = self
            .do_request("POST", &relative, Some(encode_body(input)?), None)
            .await?;
        decode_body(&body)
    }

    /// `CompletePullRequest`.
    pub async fn complete_pull_request(
        &self,
        org: &str,
        project: &str,
        repo_name_or_id: &str,
        pull_request_id: &str,
        input: &PullRequestCompleteInput,
    ) -> Result<PullRequest, ClientError> {
        let relative = pull_request_path(org, project, repo_name_or_id, pull_request_id);
        let mut completion = PullRequestCompletionOptions {
            delete_source_branch: input.delete_source_branch,
            ..Default::default()
        };
        if let Some(strategy) = &input.merge_strategy {
            completion.merge_strategy = strategy.clone();
        }
        let update = PullRequestUpdateInput {
            status: Some(PULL_REQUEST_STATUS_COMPLETED.to_owned()),
            last_merge_source_commit: Some(PullRequestCommit {
                commit_id: input.commit_id.clone(),
                url: String::new(),
            }),
            completion_options: Some(completion),
            ..Default::default()
        };
        let (_, body) = self
            .do_request("PATCH", &relative, Some(encode_body(&update)?), None)
            .await?;
        decode_body(&body)
    }

    /// `ForkRepository`.
    pub async fn fork_repository(
        &self,
        org: &str,
        input: &ForkRepositoryInput,
    ) -> Result<Repository, ClientError> {
        let relative = format!("{org}/_apis/git/repositories");
        let (_, body) = self
            .do_request("POST", &relative, Some(encode_body(input)?), None)
            .await?;
        decode_body(&body)
    }
}

fn pull_request_path(
    org: &str,
    project: &str,
    repo_name_or_id: &str,
    pull_request_id: &str,
) -> String {
    format!(
        "{org}/{project}/_apis/git/repositories/{repo_name_or_id}/pullrequests/{pull_request_id}"
    )
}

fn encode_body<T: Serialize>(value: &T) -> Result<Vec<u8>, ClientError> {
    serde_json::to_vec(value).map_err(|err| ClientError::Other(format!("marshalling request: {err}")))
}

fn decode_body<T: for<'de> Deserialize<'de>>(body: &[u8]) -> Result<T, ClientError> {
    serde_json::from_slice(body).map_err(|err| ClientError::Other(err.to_string()))
}

const API_VERSION_KEY: &str = "api-version";

fn header_value(response: &HttpResponse, name: &str) -> Option<String> {
    response
        .headers
        .iter()
        .find(|(key, _)| key.eq_ignore_ascii_case(name))
        .map(|(_, value)| value.clone())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    struct RecordingDoer {
        responses: Mutex<Vec<HttpResponse>>,
        requests: Mutex<Vec<HttpRequest>>,
    }

    #[async_trait::async_trait]
    impl HttpDoer for RecordingDoer {
        async fn do_request(&self, request: HttpRequest) -> Result<HttpResponse, String> {
            self.requests.lock().unwrap().push(request);
            let mut responses = self.responses.lock().unwrap();
            if responses.is_empty() {
                return Ok(HttpResponse::default());
            }
            Ok(responses.remove(0))
        }
    }

    fn client(doer: Arc<RecordingDoer>) -> Client {
        let base = parse_urlx("https://dev.azure.com/").unwrap();
        Client::new(
            base,
            AzureDevOpsAuth::Basic(BasicAuth {
                username: "user".to_owned(),
                password: "pat".to_owned(),
            }),
            doer,
        )
    }

    #[test]
    fn parse_urlx_defaults_the_scheme_and_lowercases_the_host() {
        assert_eq!(
            parse_urlx("Dev.Azure.com").unwrap().string,
            "http://dev.azure.com"
        );
        assert_eq!(
            parse_urlx("https://Dev.Azure.com").unwrap().string,
            "https://dev.azure.com"
        );
        assert_eq!(
            parse_urlx("https://Dev.Azure.com/").unwrap().string,
            "https://dev.azure.com/"
        );
    }

    #[test]
    fn organization_parsing_matches_splitn() {
        let repo = Repository {
            api_url: "https://dev.azure.com/org/_apis/git/repositories/id".to_owned(),
            ..Repository::default()
        };
        assert_eq!(repo.get_organization().unwrap(), "org");
        let bad = Repository {
            api_url: "https://dev.azure.com/tooshort".to_owned(),
            ..Repository::default()
        };
        assert_eq!(
            bad.get_organization().unwrap_err(),
            "unable to parse Azure DevOps organization from repo URL: \
             https://dev.azure.com/tooshort"
        );
    }

    #[test]
    fn is_azure_devops_services_requires_the_trailing_slash() {
        let doer = Arc::new(RecordingDoer {
            responses: Mutex::new(Vec::new()),
            requests: Mutex::new(Vec::new()),
        });
        assert!(client(doer.clone()).is_azure_devops_services());
        let no_slash = Client::new(
            parse_urlx("https://dev.azure.com").unwrap(),
            AzureDevOpsAuth::default(),
            doer,
        );
        assert!(!no_slash.is_azure_devops_services());
    }

    #[tokio::test]
    async fn requests_carry_api_version_and_basic_auth() {
        let doer = Arc::new(RecordingDoer {
            responses: Mutex::new(vec![HttpResponse {
                status: 200,
                headers: Vec::new(),
                body: br#"{"value":[]}"#.to_vec(),
            }]),
            requests: Mutex::new(Vec::new()),
        });
        let client = client(doer.clone());
        client
            .list_repositories_by_project_or_org("org/proj")
            .await
            .expect("list");
        let requests = doer.requests.lock().unwrap();
        assert_eq!(requests.len(), 1);
        assert_eq!(
            requests[0].url,
            "https://dev.azure.com/org/proj/_apis/git/repositories?api-version=7.0"
        );
        let auth = requests[0]
            .headers
            .iter()
            .find(|(name, _)| name.eq_ignore_ascii_case("authorization"))
            .map(|(_, value)| value.as_str());
        assert_eq!(auth, Some("Basic dXNlcjpwYXQ="));
    }

    #[tokio::test]
    async fn continuation_tokens_are_returned_from_headers() {
        let doer = Arc::new(RecordingDoer {
            responses: Mutex::new(vec![HttpResponse {
                status: 200,
                headers: vec![(CONTINUATION_TOKEN_HEADER.to_owned(), "next".to_owned())],
                body: br#"{"value":[]}"#.to_vec(),
            }]),
            requests: Mutex::new(Vec::new()),
        });
        let client = client(doer);
        let (token, _) = client
            .do_get("org/_apis/git/repositories", None)
            .await
            .expect("request");
        assert_eq!(token.as_deref(), Some("next"));
    }

    #[tokio::test]
    async fn http_errors_match_the_baseline_text() {
        let doer = Arc::new(RecordingDoer {
            responses: Mutex::new(vec![HttpResponse {
                status: 500,
                headers: Vec::new(),
                body: Vec::new(),
            }]),
            requests: Mutex::new(Vec::new()),
        });
        let err = client(doer)
            .list_repositories_by_project_or_org("org")
            .await
            .unwrap_err();
        assert_eq!(
            err.to_string(),
            "Azure DevOps API HTTP error: code=500 \
             url=\"https://dev.azure.com/org/_apis/git/repositories?api-version=7.0\""
        );
    }
}
