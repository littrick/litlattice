#![forbid(unsafe_code)]

//! `internal/extsvc/bitbucketcloud`: the read-only surface the repo-updater
//! Bitbucket Cloud source needs.
//!
//! Ported: configuration shape, Basic/OAuth authentication, `api.bitbucket.org`
//! default, `PageToken`/`next` pagination, `Repo`/`Repos`, and the exact
//! `Bitbucket Cloud API HTTP error: code=%d url=%q body=%q` text.
//!
//! Not ported (see the M9.12a overview): the self-imposed rate limiter,
//! pull-request/comment/fork/events endpoints, `CurrentUser*`,
//! `ListExplicitUserPermsForRepo`, and `FetchAll`. Only `GET`s are issued.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use base64::Engine as _;
use serde::{Deserialize, Serialize};
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;
use url::Url;

use crate::auth::set_header;
use crate::github::normalize_base_url;
use crate::gitlab::path_escape;
use crate::{HttpDoer, HttpRequest, HttpResponse};

/// `extsvc.TypeBitbucketCloud`. Note the camelCase (unlike `gitlab`).
pub const TYPE_BITBUCKETCLOUD: &str = "bitbucketCloud";

const DEFAULT_API_URL: &str = "https://api.bitbucket.org";
const ZERO_TIME: &str = "0001-01-01T00:00:00Z";

/// `schema.ExcludedBitbucketCloudRepo`.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct ExcludedBitbucketCloudRepo {
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub pattern: String,
    #[serde(default)]
    pub uuid: String,
}

/// `schema.BitbucketCloudConnection` (the fields the source reads).
#[derive(Debug, Clone, Default, Deserialize)]
pub struct BitbucketCloudConnection {
    #[serde(rename = "accessToken", default)]
    pub access_token: String,
    #[serde(rename = "apiURL", default)]
    pub api_url: String,
    #[serde(rename = "appPassword", default)]
    pub app_password: String,
    #[serde(default)]
    pub exclude: Vec<ExcludedBitbucketCloudRepo>,
    #[serde(rename = "gitURLType", default)]
    pub git_url_type: String,
    #[serde(default)]
    pub repos: Vec<String>,
    #[serde(rename = "repositoryPathPattern", default)]
    pub repository_path_pattern: String,
    #[serde(default)]
    pub teams: Vec<String>,
    #[serde(default)]
    pub url: String,
    #[serde(default)]
    pub username: String,
}

/// `bitbucketcloud.Link`. `name` is `omitempty`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Link {
    #[serde(default)]
    pub href: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub name: String,
}

/// `bitbucketcloud.Links`: a JSON object keyed by relation.
pub type Links = BTreeMap<String, Link>;

/// `bitbucketcloud.Account`. `links` is a Go map, so a nil map marshals as
/// `null` (hence `Option`).
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Account {
    #[serde(default)]
    pub links: Option<Links>,
    #[serde(default)]
    pub username: String,
    #[serde(default)]
    pub nickname: String,
    #[serde(default)]
    pub account_status: String,
    #[serde(default)]
    pub display_name: String,
    #[serde(default)]
    pub website: String,
    #[serde(
        default,
        deserialize_with = "deserialize_created_on",
        serialize_with = "serialize_created_on"
    )]
    pub created_on: Option<OffsetDateTime>,
    #[serde(default)]
    pub uuid: String,
}

fn deserialize_created_on<'de, D>(deserializer: D) -> Result<Option<OffsetDateTime>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let value = Option::<String>::deserialize(deserializer)?;
    match value {
        Some(raw) => OffsetDateTime::parse(&raw, &Rfc3339)
            .map(Some)
            .map_err(serde::de::Error::custom),
        None => Ok(None),
    }
}

fn serialize_created_on<S>(value: &Option<OffsetDateTime>, serializer: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    let rendered = match value {
        Some(value) => value.format(&Rfc3339).map_err(serde::ser::Error::custom)?,
        // `time.Time{}` marshals as its zero value.
        None => ZERO_TIME.to_owned(),
    };
    serializer.serialize_str(&rendered)
}

/// `bitbucketcloud.RepoLinks`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RepoLinks {
    /// `CloneLinks` is a slice; nil marshals as `null`.
    #[serde(default)]
    pub clone: Option<Vec<Link>>,
    #[serde(default)]
    pub html: Link,
}

impl RepoLinks {
    /// `CloneLinks.HTTPS`.
    pub fn https(&self) -> Option<&str> {
        self.clone
            .as_ref()?
            .iter()
            .find(|link| link.name == "https")
            .map(|link| link.href.as_str())
    }
}

/// `bitbucketcloud.Repo`, with the exact JSON shape because it is stored as
/// `repo.metadata`.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Repo {
    #[serde(default)]
    pub slug: String,
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub full_name: String,
    #[serde(default)]
    pub uuid: String,
    #[serde(default)]
    pub scm: String,
    #[serde(default)]
    pub description: String,
    #[serde(default)]
    pub parent: Option<Box<Repo>>,
    #[serde(default)]
    pub is_private: bool,
    #[serde(default)]
    pub links: RepoLinks,
    #[serde(default)]
    pub fork_policy: String,
    #[serde(default)]
    pub owner: Option<Account>,
}

impl Repo {
    /// `Repo.Namespace`: the segment before the first `/` in `FullName`.
    pub fn namespace(&self) -> Result<String, String> {
        match self.full_name.split_once('/') {
            Some((namespace, _)) => Ok(namespace.to_owned()),
            None => Err("cannot split namespace from repo name".to_owned()),
        }
    }
}

/// `bitbucketcloud.RenderedMarkup`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RenderedMarkup {
    #[serde(default)]
    pub raw: String,
    #[serde(default)]
    pub markup: String,
    #[serde(default)]
    pub html: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub r#type: String,
}

/// `bitbucketcloud.PullRequestCommit`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PullRequestCommit {
    #[serde(default)]
    pub hash: String,
}

/// `bitbucketcloud.PullRequestBranch`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PullRequestBranch {
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub merge_strategies: Option<Vec<String>>,
    #[serde(default)]
    pub default_merge_strategy: String,
}

/// `bitbucketcloud.PullRequestEndpoint`.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct PullRequestEndpoint {
    #[serde(rename = "repository", default)]
    pub repo: Repo,
    #[serde(default)]
    pub branch: PullRequestBranch,
    #[serde(default)]
    pub commit: PullRequestCommit,
}

/// `bitbucketcloud.PullRequest` (the fields the changeset source reads).
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct PullRequest {
    #[serde(default)]
    pub links: Option<Links>,
    #[serde(default)]
    pub id: i64,
    #[serde(default)]
    pub title: String,
    #[serde(default)]
    pub state: String,
    #[serde(default)]
    pub author: Account,
    #[serde(default)]
    pub source: PullRequestEndpoint,
    #[serde(default)]
    pub destination: PullRequestEndpoint,
    #[serde(rename = "merge_commit", default)]
    pub merge_commit: Option<PullRequestCommit>,
    #[serde(default)]
    pub comment_count: i64,
    #[serde(default)]
    pub task_count: i64,
    #[serde(rename = "close_source_branch", default)]
    pub close_source_branch: bool,
    #[serde(rename = "created_on", default)]
    pub created_on: String,
    #[serde(rename = "updated_on", default)]
    pub updated_on: String,
    #[serde(default)]
    pub reviewers: Option<Vec<Account>>,
}

/// `bitbucketcloud.PullRequestStatus`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PullRequestStatus {
    #[serde(default)]
    pub links: Option<Links>,
    #[serde(default)]
    pub uuid: String,
    #[serde(rename = "key", default)]
    pub status_key: String,
    #[serde(default)]
    pub refname: String,
    #[serde(rename = "url", default)]
    pub url: String,
    #[serde(default)]
    pub state: String,
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub description: String,
    #[serde(rename = "created_on", default)]
    pub created_on: String,
    #[serde(rename = "updated_on", default)]
    pub updated_on: String,
}

/// `bitbucketcloud.User`.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct User {
    #[serde(flatten)]
    pub account: Account,
    #[serde(rename = "is_staff", default)]
    pub is_staff: bool,
    #[serde(rename = "account_id", default)]
    pub account_id: String,
}

/// `bitbucketcloud.Comment`.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Comment {
    #[serde(default)]
    pub id: i64,
    #[serde(default)]
    pub content: RenderedMarkup,
    #[serde(default)]
    pub deleted: bool,
}

/// `bitbucketcloud.PullRequestInput`; only the fields the source sets.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct PullRequestInput {
    pub title: String,
    pub description: String,
    pub source_branch: String,
    pub reviewers: Vec<Account>,
    pub source_repo: Option<Repo>,
    pub destination_branch: Option<String>,
    pub close_source_branch: bool,
}

impl PullRequestInput {
    /// `(*PullRequestInput).MarshalJSON`.
    pub fn to_json(&self) -> serde_json::Value {
        let mut source = serde_json::Map::new();
        source.insert(
            "branch".to_owned(),
            serde_json::json!({"name": self.source_branch}),
        );
        if let Some(repo) = &self.source_repo {
            source.insert(
                "repository".to_owned(),
                serde_json::json!({"full_name": repo.full_name}),
            );
        }
        let mut request = serde_json::Map::new();
        request.insert("title".to_owned(), serde_json::json!(self.title));
        if !self.description.is_empty() {
            request.insert(
                "description".to_owned(),
                serde_json::json!(self.description),
            );
        }
        request.insert("source".to_owned(), serde_json::Value::Object(source));
        if let Some(branch) = &self.destination_branch {
            request.insert(
                "destination".to_owned(),
                serde_json::json!({"branch": {"name": branch}}),
            );
        }
        if self.close_source_branch {
            request.insert(
                "close_source_branch".to_owned(),
                serde_json::json!(true),
            );
        }
        serde_json::Value::Object(request)
    }
}

/// `bitbucketcloud.CommentInput`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CommentInput {
    pub content: String,
}

impl CommentInput {
    /// `(*CommentInput).MarshalJSON`.
    pub fn to_json(&self) -> serde_json::Value {
        serde_json::json!({"content": {"raw": self.content}})
    }
}

/// `bitbucketcloud.MergePullRequestOpts`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MergePullRequestOpts {
    pub message: Option<String>,
    pub close_source_branch: Option<bool>,
    pub merge_strategy: Option<String>,
}

impl MergePullRequestOpts {
    /// `json.Marshal` with `omitempty` on every field.
    pub fn to_json(&self) -> serde_json::Value {
        let mut out = serde_json::Map::new();
        if let Some(message) = &self.message {
            out.insert("message".to_owned(), serde_json::json!(message));
        }
        if let Some(close) = self.close_source_branch {
            out.insert("close_source_branch".to_owned(), serde_json::json!(close));
        }
        if let Some(strategy) = &self.merge_strategy {
            out.insert("merge_strategy".to_owned(), serde_json::json!(strategy));
        }
        serde_json::Value::Object(out)
    }
}

/// `bitbucketcloud.MergeStrategySquash`.
pub const MERGE_STRATEGY_SQUASH: &str = "squash";

/// `bitbucketcloud.ForkInput`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ForkInput {
    pub name: Option<String>,
    pub workspace: String,
}

impl ForkInput {
    /// `json.Marshal` with the custom `ForkInputWorkspace` shape.
    pub fn to_json(&self) -> serde_json::Value {
        let mut out = serde_json::Map::new();
        if let Some(name) = &self.name {
            out.insert("name".to_owned(), serde_json::json!(name));
        }
        out.insert(
            "workspace".to_owned(),
            serde_json::json!({"slug": self.workspace}),
        );
        serde_json::Value::Object(out)
    }
}

/// `bitbucketcloud.PageToken`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
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
    /// `PageToken.HasMore`.
    pub fn has_more(&self) -> bool {
        !self.next.is_empty()
    }

    /// `PageToken.Values`: the `next` URL's query plus `pagelen`.
    pub fn values(&self) -> BTreeMap<String, Vec<String>> {
        let mut values = BTreeMap::new();
        if !self.next.is_empty() {
            if let Ok(next) = Url::parse(&self.next) {
                for (key, value) in
                    url::form_urlencoded::parse(next.query().unwrap_or_default().as_bytes())
                {
                    values
                        .entry(key.into_owned())
                        .or_insert_with(Vec::new)
                        .push(value.into_owned());
                }
            }
        }
        if self.pagelen != 0 {
            values.insert("pagelen".to_owned(), vec![self.pagelen.to_string()]);
        }
        values
    }
}

/// The authenticator selected by `bitbucketcloud.newClient`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BitbucketCloudAuth {
    Basic { username: String, password: String },
    OAuthBearerToken(String),
}

impl Default for BitbucketCloudAuth {
    fn default() -> Self {
        Self::Basic {
            username: String::new(),
            password: String::new(),
        }
    }
}

impl BitbucketCloudAuth {
    pub fn authenticate(&self, headers: &mut Vec<(String, String)>) {
        match self {
            Self::Basic { username, password } => {
                let encoded = base64::engine::general_purpose::STANDARD
                    .encode(format!("{username}:{password}"));
                set_header(headers, "Authorization", &format!("Basic {encoded}"));
            }
            Self::OAuthBearerToken(token) => {
                set_header(headers, "Authorization", &format!("Bearer {token}"));
            }
        }
    }
}

/// `bitbucketcloud.httpError`.
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
            "Bitbucket Cloud API HTTP error: code={} url={:?} body={:?}",
            self.status_code, self.url, self.body
        )
    }
}

/// Client errors. `Wrapped` mirrors `errors.Wrap` so the source's messages
/// keep the baseline prefixes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BitbucketError {
    Http(HttpError),
    Transport(String),
    Json(String),
    Wrapped {
        prefix: String,
        inner: Box<BitbucketError>,
    },
}

impl BitbucketError {
    pub fn with_prefix(self, prefix: &str) -> Self {
        Self::Wrapped {
            prefix: prefix.to_owned(),
            inner: Box::new(self),
        }
    }

    /// `errcode.IsNotFound`.
    pub fn is_not_found(&self) -> bool {
        match self {
            Self::Http(err) => err.status_code == 404,
            Self::Wrapped { inner, .. } => inner.is_not_found(),
            _ => false,
        }
    }

    pub fn is_unauthorized(&self) -> bool {
        match self {
            Self::Http(err) => err.status_code == 401,
            Self::Wrapped { inner, .. } => inner.is_unauthorized(),
            _ => false,
        }
    }
}

impl std::fmt::Display for BitbucketError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Http(err) => err.fmt(f),
            Self::Transport(err) | Self::Json(err) => f.write_str(err),
            Self::Wrapped { prefix, inner } => write!(f, "{prefix}: {inner}"),
        }
    }
}

impl std::error::Error for BitbucketError {}

/// `bitbucketcloud.client` for one authenticator.
#[derive(Clone)]
pub struct Client {
    url: Url,
    auth: BitbucketCloudAuth,
    http: Arc<dyn HttpDoer>,
}

impl Client {
    /// `bitbucketcloud.NewClient`: `UrlFromConfig` + `NormalizeBaseURL`.
    pub fn new(config: &BitbucketCloudConnection, http: Arc<dyn HttpDoer>) -> Result<Self, String> {
        let api_url = if config.api_url.is_empty() {
            DEFAULT_API_URL
        } else {
            config.api_url.as_str()
        };
        let url = Url::parse(api_url).map_err(|err| err.to_string())?;
        let url = normalize_base_url(&url);

        let auth = if !config.access_token.is_empty() {
            BitbucketCloudAuth::OAuthBearerToken(config.access_token.clone())
        } else {
            BitbucketCloudAuth::Basic {
                username: config.username.clone(),
                password: config.app_password.clone(),
            }
        };

        Ok(Self { url, auth, http })
    }

    pub fn url(&self) -> &Url {
        &self.url
    }

    pub fn authenticator(&self) -> &BitbucketCloudAuth {
        &self.auth
    }

    /// `NewClient` after `UrlFromConfig`/`NormalizeBaseURL`, for callers that
    /// resolved the API URL and authenticator themselves (the Batch Changes
    /// factory).
    pub fn with_base_url_and_auth(
        base_url: Url,
        auth: BitbucketCloudAuth,
        http: Arc<dyn HttpDoer>,
    ) -> Self {
        Self {
            url: base_url,
            auth,
            http,
        }
    }

    /// `client.do`: resolve the URL, add `Content-Type` only for a body,
    /// authenticate, retry 429 with exponential backoff, then turn non-2xx/3xx
    /// into `httpError`.
    async fn do_request(
        &self,
        method: &str,
        request_uri: &str,
        body: &[u8],
    ) -> Result<HttpResponse, BitbucketError> {
        let url = self
            .url
            .join(request_uri)
            .map_err(|err| BitbucketError::Transport(err.to_string()))?;
        let url_string = url.to_string();

        let mut sleep_seconds = 10u64;
        loop {
            let mut headers = Vec::new();
            if !body.is_empty() {
                set_header(
                    &mut headers,
                    "Content-Type",
                    "application/json; charset=utf-8",
                );
            }
            self.auth.authenticate(&mut headers);

            let response = self
                .http
                .do_request(HttpRequest {
                    method: method.to_owned(),
                    url: url_string.clone(),
                    headers,
                    body: body.to_vec(),
                })
                .await
                .map_err(BitbucketError::Transport)?;

            if response.status != 429 {
                return self.finish(response, &url_string);
            }

            tokio::time::sleep(Duration::from_secs(sleep_seconds)).await;
            sleep_seconds *= 2;
            // The baseline gives up after more than 160 seconds of backoff
            // and reports the last 429 response.
            if sleep_seconds > 160 {
                return self.finish(response, &url_string);
            }
        }
    }

    fn finish(&self, response: HttpResponse, url: &str) -> Result<HttpResponse, BitbucketError> {
        if response.status < 200 || response.status >= 400 {
            return Err(BitbucketError::Http(HttpError {
                status_code: response.status,
                url: url.to_owned(),
                body: String::from_utf8_lossy(&response.body).into_owned(),
            }));
        }
        Ok(response)
    }

    /// `client.Ping`: `GET /2.0/`, where a 404 still means the credentials
    /// were accepted (Bitbucket checks auth before routing).
    pub async fn ping(&self) -> Result<(), BitbucketError> {
        match self.do_request("GET", "/2.0/", &[]).await {
            Ok(_) => Ok(()),
            Err(err) if err.is_not_found() => Ok(()),
            Err(err) => Err(err),
        }
    }

    /// `client.Repo`: `/2.0/repositories/{namespace}/{slug}` (the baseline
    /// does not escape either component here).
    pub async fn repo(&self, namespace: &str, slug: &str) -> Result<Repo, BitbucketError> {
        let request_uri = format!("/2.0/repositories/{namespace}/{slug}");
        let response = self
            .do_request("GET", &request_uri, &[])
            .await
            .map_err(|err| err.with_prefix("sending request"))?;
        serde_json::from_slice::<Repo>(&response.body)
            .map_err(|err| BitbucketError::Json(err.to_string()).with_prefix("sending request"))
    }

    /// `client.Repos`.
    pub async fn repos(
        &self,
        page_token: Option<&PageToken>,
        account_name: &str,
        role: Option<&str>,
    ) -> Result<(Vec<Repo>, PageToken), BitbucketError> {
        if let Some(token) = page_token {
            if token.has_more() {
                let response = self.do_request("GET", &token.next, &[]).await?;
                return decode_page(&response.body);
            }
        }

        let path = if account_name.is_empty() {
            "/2.0/repositories".to_owned()
        } else {
            format!("/2.0/repositories/{}", path_escape(account_name))
        };

        let mut query: BTreeMap<String, Vec<String>> = BTreeMap::new();
        if let Some(role) = role.filter(|role| !role.is_empty()) {
            query.insert("role".to_owned(), vec![role.to_owned()]);
        }
        if let Some(token) = page_token {
            for (key, values) in token.values() {
                query.entry(key).or_default().extend(values);
            }
        }

        let path = encode_go_path(&path);
        let request_uri = if query.is_empty() {
            path
        } else {
            format!("{path}?{}", encode_values(&query))
        };
        let response = self.do_request("GET", &request_uri, &[]).await?;
        decode_page(&response.body)
    }

    /// `client.GetPullRequest`.
    pub async fn get_pull_request(
        &self,
        repo_full_name: &str,
        id: i64,
    ) -> Result<PullRequest, BitbucketError> {
        let request_uri = format!("/2.0/repositories/{repo_full_name}/pullrequests/{id}");
        self.json_request("GET", &request_uri, None).await
    }

    /// `client.CreatePullRequest`.
    pub async fn create_pull_request(
        &self,
        repo_full_name: &str,
        input: &PullRequestInput,
    ) -> Result<PullRequest, BitbucketError> {
        let request_uri = format!("/2.0/repositories/{repo_full_name}/pullrequests");
        self.json_request("POST", &request_uri, Some(input.to_json()))
            .await
    }

    /// `client.DeclinePullRequest`.
    pub async fn decline_pull_request(
        &self,
        repo_full_name: &str,
        id: i64,
    ) -> Result<PullRequest, BitbucketError> {
        let request_uri =
            format!("/2.0/repositories/{repo_full_name}/pullrequests/{id}/decline");
        self.json_request("POST", &request_uri, None).await
    }

    /// `client.UpdatePullRequest`.
    pub async fn update_pull_request(
        &self,
        repo_full_name: &str,
        id: i64,
        input: &PullRequestInput,
    ) -> Result<PullRequest, BitbucketError> {
        let request_uri = format!("/2.0/repositories/{repo_full_name}/pullrequests/{id}");
        self.json_request("PUT", &request_uri, Some(input.to_json()))
            .await
    }

    /// `client.CreatePullRequestComment`.
    pub async fn create_pull_request_comment(
        &self,
        repo_full_name: &str,
        id: i64,
        input: &CommentInput,
    ) -> Result<Comment, BitbucketError> {
        let request_uri =
            format!("/2.0/repositories/{repo_full_name}/pullrequests/{id}/comments");
        self.json_request("POST", &request_uri, Some(input.to_json()))
            .await
    }

    /// `client.MergePullRequest`.
    pub async fn merge_pull_request(
        &self,
        repo_full_name: &str,
        id: i64,
        opts: &MergePullRequestOpts,
    ) -> Result<PullRequest, BitbucketError> {
        let request_uri =
            format!("/2.0/repositories/{repo_full_name}/pullrequests/{id}/merge");
        self.json_request("POST", &request_uri, Some(opts.to_json()))
            .await
    }

    /// `client.CurrentUser`.
    pub async fn current_user(&self) -> Result<User, BitbucketError> {
        self.json_request("GET", "/2.0/user", None).await
    }

    /// `client.ForkRepository`.
    pub async fn fork_repository(
        &self,
        upstream: &Repo,
        input: &ForkInput,
    ) -> Result<Repo, BitbucketError> {
        let request_uri = format!("/2.0/repositories/{}/forks", upstream.full_name);
        self.json_request("POST", &request_uri, Some(input.to_json()))
            .await
    }

    /// `client.GetPullRequestStatuses(...).All(ctx)`: walks every page.
    pub async fn get_pull_request_statuses(
        &self,
        repo_full_name: &str,
        id: i64,
    ) -> Result<Vec<PullRequestStatus>, BitbucketError> {
        let mut request_uri =
            format!("/2.0/repositories/{repo_full_name}/pullrequests/{id}/statuses");
        let mut statuses = Vec::new();
        loop {
            let response = self.do_request("GET", &request_uri, &[]).await?;
            let page: StatusesPage = serde_json::from_slice(&response.body)
                .map_err(|err| BitbucketError::Json(err.to_string()))?;
            statuses.extend(page.values);
            if page.next.is_empty() {
                return Ok(statuses);
            }
            request_uri = page.next;
        }
    }

    /// `client.do` + JSON decode, with the baseline's `sending request` wrap.
    async fn json_request<T: serde::de::DeserializeOwned>(
        &self,
        method: &str,
        request_uri: &str,
        body: Option<serde_json::Value>,
    ) -> Result<T, BitbucketError> {
        let body = match body {
            Some(value) => serde_json::to_vec(&value)
                .map_err(|err| BitbucketError::Json(err.to_string()))?,
            None => Vec::new(),
        };
        let response = self
            .do_request(method, request_uri, &body)
            .await
            .map_err(|err| err.with_prefix("sending request"))?;
        serde_json::from_slice(&response.body)
            .map_err(|err| BitbucketError::Json(err.to_string()).with_prefix("sending request"))
    }
}

#[derive(Debug, Default, Deserialize)]
struct StatusesPage {
    #[serde(default)]
    next: String,
    #[serde(default)]
    values: Vec<PullRequestStatus>,
}

/// `url.URL{Path: p}.String()`'s path escaping. Bitbucket builds a path with
/// `PathEscape` and then hands it to `url.URL{Path: ...}`, which escapes the
/// `%` signs again: `my team` becomes `my%2520team`. Reproduce that so request
/// URIs match the baseline byte for byte.
pub fn encode_go_path(path: &str) -> String {
    let mut out = String::with_capacity(path.len());
    for byte in path.bytes() {
        let keep = byte.is_ascii_alphanumeric()
            || matches!(
                byte,
                b'-' | b'_'
                    | b'.'
                    | b'~'
                    | b'$'
                    | b'&'
                    | b'+'
                    | b','
                    | b'/'
                    | b':'
                    | b';'
                    | b'='
                    | b'@'
            );
        if keep {
            out.push(byte as char);
        } else {
            out.push_str(&format!("%{byte:02X}"));
        }
    }
    out
}

/// `url.URL{Host: h}.String()`'s host escaping (used by the remote-URL
/// fallback, which treats the configured web URL as a host).
pub fn encode_go_host(host: &str) -> String {
    let mut out = String::with_capacity(host.len());
    for byte in host.bytes() {
        let keep = byte.is_ascii_alphanumeric()
            || matches!(
                byte,
                b'-' | b'_'
                    | b'.'
                    | b'~'
                    | b'!'
                    | b'$'
                    | b'&'
                    | b'\''
                    | b'('
                    | b')'
                    | b'*'
                    | b'+'
                    | b','
                    | b';'
                    | b'='
                    | b':'
                    | b'['
                    | b']'
            );
        if keep {
            out.push(byte as char);
        } else {
            out.push_str(&format!("%{byte:02X}"));
        }
    }
    out
}

fn encode_values(values: &BTreeMap<String, Vec<String>>) -> String {
    let mut serializer = url::form_urlencoded::Serializer::new(String::new());
    for (key, entries) in values {
        for value in entries {
            serializer.append_pair(key, value);
        }
    }
    serializer.finish()
}

#[derive(Debug, Default, Deserialize)]
struct ReposPage {
    #[serde(default)]
    size: i64,
    #[serde(default)]
    page: i64,
    #[serde(default)]
    pagelen: i64,
    #[serde(default)]
    next: String,
    #[serde(default)]
    values: Vec<Repo>,
}

fn decode_page(body: &[u8]) -> Result<(Vec<Repo>, PageToken), BitbucketError> {
    let page: ReposPage =
        serde_json::from_slice(body).map_err(|err| BitbucketError::Json(err.to_string()))?;
    Ok((
        page.values,
        PageToken {
            size: page.size,
            page: page.page,
            pagelen: page.pagelen,
            next: page.next,
        },
    ))
}
