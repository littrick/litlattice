#![forbid(unsafe_code)]

//! `internal/extsvc/gerrit`: the read-only surface the repo-updater Gerrit
//! source needs.
//!
//! Ported: configuration shape, Basic authentication (always sent, even for
//! empty credentials), request building with Go's `ResolveReference`
//! semantics, the `)]}'` anti-XSSI prefix strip, `httpError` text, the
//! `n`/`S`/`type=CODE` offset pagination (including the "probe all projects to
//! decide whether another page exists" heuristic), `ssh_info`, and
//! `GetAuthenticatedUserAccount`.
//!
//! Not ported (see the M9.13c overview): the internal rate limiter and every
//! change/group write endpoint.

use std::collections::BTreeMap;
use std::sync::Arc;

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use url::Url;

use crate::auth::{set_header, Authenticator, BasicAuth, BasicAuthWithSSH};
use crate::{HttpDoer, HttpRequest, HttpResponse};

/// `extsvc.TypeGerrit` (stored in `repo.external_service_type`).
pub const TYPE_GERRIT: &str = "gerrit";

/// The authenticator a [`Client`] applies. The baseline only supports
/// `auth.BasicAuth` and `auth.BasicAuthWithSSH`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GerritAuth {
    Basic(BasicAuth),
    BasicWithSsh(BasicAuthWithSSH),
}

impl Default for GerritAuth {
    fn default() -> Self {
        Self::Basic(BasicAuth::default())
    }
}

impl GerritAuth {
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

/// `gerrit.AccountCredentials`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AccountCredentials {
    pub username: String,
    pub password: String,
}

/// `schema.GerritAuthorization`.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct GerritAuthorization {
    #[serde(rename = "identityProvider", default)]
    pub identity_provider: String,
}

/// `schema.ExcludedGerritProject`.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct ExcludedGerritProject {
    #[serde(default)]
    pub name: String,
}

/// `schema.GerritConnection` (the fields the source reads).
#[derive(Debug, Clone, Default, Deserialize)]
pub struct GerritConnection {
    #[serde(default)]
    pub authorization: Option<GerritAuthorization>,
    #[serde(default)]
    pub exclude: Vec<ExcludedGerritProject>,
    #[serde(rename = "gitURLType", default)]
    pub git_url_type: String,
    #[serde(default)]
    pub password: String,
    #[serde(default)]
    pub projects: Vec<String>,
    #[serde(rename = "repositoryPathPattern", default)]
    pub repository_path_pattern: String,
    #[serde(default)]
    pub url: String,
    #[serde(default)]
    pub username: String,
}

/// `gerrit.Label`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Label {
    #[serde(default)]
    pub values: Option<BTreeMap<String, String>>,
    #[serde(default)]
    pub default_value: String,
}

/// `gerrit.Project`. The `http_url_to_repo`/`ssh_url_to_repo` fields are
/// computed in `repos.GerritSource.ListRepos` and stored in `repo.metadata`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Project {
    #[serde(default)]
    pub description: String,
    #[serde(default)]
    pub id: String,
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub parent: String,
    #[serde(default)]
    pub state: String,
    #[serde(default)]
    pub branches: Option<BTreeMap<String, String>>,
    #[serde(default)]
    pub labels: Option<BTreeMap<String, Label>>,
    #[serde(rename = "http_url_to_repo", default)]
    pub http_url_to_repo: String,
    #[serde(rename = "ssh_url_to_repo", default)]
    pub ssh_url_to_repo: String,
}

/// `gerrit.Account`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Account {
    #[serde(rename = "_account_id", default)]
    pub id: i64,
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub display_name: String,
    #[serde(default)]
    pub email: String,
    #[serde(default)]
    pub username: String,
}

/// `gerrit.Change`. `created`/`updated` are decoded from the wire strings and
/// re-emitted with the baseline's `2006-01-02 15:04:05.000000000` layout (see
/// [`format_change_time`]).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Change {
    #[serde(default)]
    pub id: String,
    #[serde(default)]
    pub project: String,
    #[serde(default)]
    pub branch: String,
    #[serde(rename = "change_id", default)]
    pub change_id: String,
    #[serde(default)]
    pub topic: String,
    #[serde(default)]
    pub subject: String,
    #[serde(default)]
    pub status: String,
    #[serde(default)]
    pub reviewed: bool,
    #[serde(rename = "work_in_progress", default)]
    pub work_in_progress: bool,
    #[serde(default)]
    pub hashtags: Option<Vec<String>>,
    #[serde(rename = "_number", default)]
    pub change_number: i64,
    #[serde(default)]
    pub owner: ChangeOwner,
    #[serde(default)]
    pub created: String,
    #[serde(default)]
    pub updated: String,
}

/// `gerrit.Change.Owner`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChangeOwner {
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub email: String,
    #[serde(default)]
    pub username: String,
}

/// `gerrit.Reviewer`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Reviewer {
    #[serde(default)]
    pub approvals: Option<BTreeMap<String, String>>,
    #[serde(rename = "_account_id", default)]
    pub account_id: i64,
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub email: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub username: String,
}

/// `gerrit.ChangeReviewComment`; only `message` is ever set by the source.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct ChangeReviewComment {
    pub message: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub tag: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub labels: Option<BTreeMap<String, i64>>,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub notify: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub on_behalf_of: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub comments: Option<BTreeMap<String, String>>,
}

/// `gerrit.MoveChangePayload`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct MoveChangePayload {
    pub destination_branch: String,
}

/// `gerrit.SetCommitMessagePayload`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct SetCommitMessagePayload {
    pub message: String,
}

/// `gerrit.Pagination`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Pagination {
    pub per_page: i64,
    pub page: i64,
    pub skip: i64,
}

/// `gerrit.ListProjectsArgs`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ListProjectsArgs {
    pub cursor: Pagination,
    pub only_code_projects: bool,
}

/// `gerrit.ListProjectsResponse` (`map[string]*Project`).
pub type ListProjectsResponse = BTreeMap<String, Project>;

/// `gerrit.httpError`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HttpError {
    pub status_code: u16,
    pub url: String,
    pub body: Vec<u8>,
}

impl HttpError {
    pub fn unauthorized(&self) -> bool {
        self.status_code == 401
    }

    pub fn not_found(&self) -> bool {
        self.status_code == 404
    }
}

impl std::fmt::Display for HttpError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Gerrit API HTTP error: code={} url={:?} body={:?}",
            self.status_code,
            self.url,
            String::from_utf8_lossy(&self.body)
        )
    }
}

impl std::error::Error for HttpError {}

/// Errors produced by the client.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClientError {
    Http(HttpError),
    /// `gerrit.MultipleChangesError`.
    MultipleChanges { id: String },
    Other(String),
}

impl std::fmt::Display for ClientError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Http(err) => err.fmt(f),
            Self::MultipleChanges { id } => {
                write!(f, "Multiple changes found with ID {id} not found")
            }
            Self::Other(err) => f.write_str(err),
        }
    }
}

impl std::error::Error for ClientError {}

/// `url.Values.Encode`: keys sorted lexicographically (so uppercase `S` sorts
/// before lowercase `n`/`type`), values `QueryEscape`-encoded.
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

/// `gerrit.Client` for one authenticator.
#[derive(Clone)]
pub struct Client {
    base_url: Url,
    auth: GerritAuth,
    http: Arc<dyn HttpDoer>,
}

impl Client {
    /// `gerrit.NewClient`, minus the internal rate limiter.
    pub fn new(base_url: Url, creds: &AccountCredentials, http: Arc<dyn HttpDoer>) -> Self {
        Self {
            base_url,
            auth: GerritAuth::Basic(BasicAuth {
                username: creds.username.clone(),
                password: creds.password.clone(),
            }),
            http,
        }
    }

    pub fn url(&self) -> &Url {
        &self.base_url
    }

    pub fn authenticator(&self) -> &GerritAuth {
        &self.auth
    }

    /// `Client.WithAuthenticator`.
    pub fn with_authenticator(&self, auth: GerritAuth) -> Self {
        Self {
            base_url: self.base_url.clone(),
            auth,
            http: Arc::clone(&self.http),
        }
    }

    /// `doHTTP`: resolve the relative request against the instance URL,
    /// authenticate, and turn non-2xx/3xx responses into [`HttpError`].
    async fn do_http(&self, method: &str, relative: &str) -> Result<(Url, HttpResponse), ClientError> {
        self.do_http_full(method, relative, &[], &[]).await
    }

    /// `doHTTP` with a request body and extra headers (the change writes).
    async fn do_http_full(
        &self,
        method: &str,
        relative: &str,
        body: &[u8],
        extra_headers: &[(&str, &str)],
    ) -> Result<(Url, HttpResponse), ClientError> {
        let url = self
            .base_url
            .join(relative)
            .map_err(|err| ClientError::Other(err.to_string()))?;
        let url_string = url.to_string();

        let mut headers = Vec::new();
        self.auth
            .authenticate(method, &url_string, &mut headers)
            .map_err(ClientError::Other)?;
        for (name, value) in extra_headers {
            set_header(&mut headers, name, value);
        }

        let response = self
            .http
            .do_request(HttpRequest {
                method: method.to_owned(),
                url: url_string.clone(),
                headers,
                body: body.to_vec(),
            })
            .await
            .map_err(ClientError::Other)?;

        if !(200..400).contains(&response.status) {
            return Err(ClientError::Http(HttpError {
                status_code: response.status,
                url: url_string,
                body: response.body,
            }));
        }

        Ok((url, response))
    }

    /// `do`: strip the `)]}'` anti-XSSI prefix and decode JSON.
    async fn do_json<T: DeserializeOwned>(&self, relative: &str) -> Result<T, ClientError> {
        let (_, response) = self.do_http("GET", relative).await?;
        let body = strip_xssi_prefix(&response.body);
        serde_json::from_slice(body).map_err(|err| ClientError::Other(err.to_string()))
    }

    /// `do` for a non-GET request with an optional JSON body and headers.
    async fn do_json_full<T: DeserializeOwned>(
        &self,
        method: &str,
        relative: &str,
        body: &[u8],
        extra_headers: &[(&str, &str)],
    ) -> Result<T, ClientError> {
        let (_, response) = self
            .do_http_full(method, relative, body, extra_headers)
            .await?;
        let body = strip_xssi_prefix(&response.body);
        serde_json::from_slice(body).map_err(|err| ClientError::Other(err.to_string()))
    }

    /// `GetChange`.
    pub async fn get_change(&self, change_id: &str) -> Result<Change, ClientError> {
        match self
            .do_json_full::<Change>("GET", &change_path(change_id), &[], &[])
            .await
        {
            Ok(change) => Ok(change),
            Err(err) => {
                // `GetChange` maps the fringe "multiple changes" body to
                // `MultipleChangesError`.
                if err.to_string().contains("changes found for") {
                    Err(ClientError::MultipleChanges {
                        id: change_id.to_owned(),
                    })
                } else {
                    Err(err)
                }
            }
        }
    }

    /// `AbandonChange`.
    pub async fn abandon_change(&self, change_id: &str) -> Result<Change, ClientError> {
        self.do_json_full(
            "POST",
            &format!("{}/abandon", change_path(change_id)),
            &[],
            &[],
        )
        .await
    }

    /// `DeleteChange`.
    pub async fn delete_change(&self, change_id: &str) -> Result<(), ClientError> {
        self.do_http_full("DELETE", &change_path(change_id), &[], &[])
            .await
            .map(|_| ())
    }

    /// `SubmitChange`.
    pub async fn submit_change(&self, change_id: &str) -> Result<Change, ClientError> {
        self.do_json_full(
            "POST",
            &format!("{}/submit", change_path(change_id)),
            &[],
            &[],
        )
        .await
    }

    /// `RestoreChange`.
    pub async fn restore_change(&self, change_id: &str) -> Result<Change, ClientError> {
        self.do_json_full(
            "POST",
            &format!("{}/restore", change_path(change_id)),
            &[],
            &[],
        )
        .await
    }

    /// `SetReadyForReview`.
    pub async fn set_ready_for_review(&self, change_id: &str) -> Result<(), ClientError> {
        self.do_http_full(
            "POST",
            &format!("{}/ready", change_path(change_id)),
            &[],
            &[],
        )
        .await
        .map(|_| ())
    }

    /// `SetWIP`.
    pub async fn set_wip(&self, change_id: &str) -> Result<(), ClientError> {
        self.do_http_full("POST", &format!("{}/wip", change_path(change_id)), &[], &[])
            .await
            .map(|_| ())
    }

    /// `WriteReviewComment`. The baseline sets
    /// `Content-Type: text/plain; charset=UTF-8` even though the body is JSON.
    pub async fn write_review_comment(
        &self,
        change_id: &str,
        comment: &ChangeReviewComment,
    ) -> Result<(), ClientError> {
        let body = serde_json::to_vec(comment)
            .map_err(|err| ClientError::Other(err.to_string()))?;
        self.do_http_full(
            "POST",
            &format!("{}/revisions/current/review", change_path(change_id)),
            &body,
            &[("Content-Type", "text/plain; charset=UTF-8")],
        )
        .await
        .map(|_| ())
    }

    /// `GetChangeReviews`.
    pub async fn get_change_reviews(
        &self,
        change_id: &str,
    ) -> Result<Vec<Reviewer>, ClientError> {
        self.do_json_full(
            "GET",
            &format!("{}/revisions/current/reviewers", change_path(change_id)),
            &[],
            &[("Content-Type", "text/plain; charset=UTF-8")],
        )
        .await
    }

    /// `MoveChange`.
    pub async fn move_change(
        &self,
        change_id: &str,
        input: &MoveChangePayload,
    ) -> Result<Change, ClientError> {
        let body =
            serde_json::to_vec(input).map_err(|err| ClientError::Other(err.to_string()))?;
        self.do_json_full(
            "POST",
            &format!("{}/move", change_path(change_id)),
            &body,
            &[("Content-Type", "application/json")],
        )
        .await
    }

    /// `SetCommitMessage`.
    pub async fn set_commit_message(
        &self,
        change_id: &str,
        input: &SetCommitMessagePayload,
    ) -> Result<(), ClientError> {
        let body =
            serde_json::to_vec(input).map_err(|err| ClientError::Other(err.to_string()))?;
        self.do_http_full(
            "PUT",
            &format!("{}/message", change_path(change_id)),
            &body,
            &[("Content-Type", "application/json")],
        )
        .await
        .map(|_| ())
    }

    /// `GetSSHInfo`. A body that does not split into exactly two fields is
    /// reported as `("", 0)` without an error, mirroring the baseline's
    /// `errors.Wrapf(nil, …)` return value.
    pub async fn get_ssh_info(&self) -> Result<(String, i64), ClientError> {
        let (_, response) = self.do_http("GET", "ssh_info").await?;
        let text = String::from_utf8_lossy(&response.body).into_owned();
        let fields: Vec<&str> = text.split_whitespace().collect();
        if fields.len() != 2 {
            return Ok((String::new(), 0));
        }
        let port = fields[1].parse::<i64>().map_err(|err| {
            ClientError::Other(format!(
                "got invalid port from Gerrit for ssh_info {text:?}: {err}"
            ))
        })?;
        Ok((fields[0].to_owned(), port))
    }

    /// `GetAuthenticatedUserAccount`.
    pub async fn get_authenticated_user_account(&self) -> Result<Account, ClientError> {
        match self.do_json::<Account>("a/accounts/self").await {
            Ok(account) => Ok(account),
            Err(ClientError::Http(err)) if err.unauthorized() => {
                Err(ClientError::Other("Invalid username or password.".to_owned()))
            }
            Err(err) => Err(err),
        }
    }

    /// `ListProjects`.
    pub async fn list_projects(
        &self,
        args: &ListProjectsArgs,
    ) -> Result<(ListProjectsResponse, bool), ClientError> {
        if args.only_code_projects {
            self.list_code_projects(&args.cursor).await
        } else {
            self.list_all_projects(&args.cursor).await
        }
    }

    async fn list_code_projects(
        &self,
        cursor: &Pagination,
    ) -> Result<(ListProjectsResponse, bool), ClientError> {
        let query = encode_query(&[
            ("n", cursor.per_page.to_string()),
            ("S", ((cursor.page - 1) * cursor.per_page).to_string()),
            ("type", "CODE".to_owned()),
        ]);
        let projects: ListProjectsResponse = self.do_json(&format!("a/projects/?{query}")).await?;

        // An empty CODE page does not imply the end: probe the next ALL page.
        if projects.is_empty() {
            let probe = Pagination {
                per_page: 1,
                page: 0,
                skip: cursor.page * cursor.per_page,
            };
            let (next_page, _) = self.list_all_projects(&probe).await?;
            if next_page.is_empty() {
                return Ok((projects, false));
            }
        }

        // Otherwise always assume there is another page.
        Ok((projects, true))
    }

    async fn list_all_projects(
        &self,
        cursor: &Pagination,
    ) -> Result<(ListProjectsResponse, bool), ClientError> {
        let skip = if cursor.skip > 0 {
            cursor.skip
        } else {
            (cursor.page - 1) * cursor.per_page
        };
        let query = encode_query(&[
            ("n", cursor.per_page.to_string()),
            ("S", skip.to_string()),
        ]);
        let projects: ListProjectsResponse = self.do_json(&format!("a/projects/?{query}")).await?;
        let next = projects.len() as i64 == cursor.per_page;
        Ok((projects, next))
    }
}

/// `bytes.CutPrefix(bs, []byte(")]}'"))`.
pub fn strip_xssi_prefix(body: &[u8]) -> &[u8] {
    body.strip_prefix(b")]}'").unwrap_or(body)
}

/// `net/url.PathEscape` (`encodePathSegment`): leaves alphanumerics and
/// `-_.~$&+:=@` untouched, escapes every other byte.
pub fn path_escape_segment(value: &str) -> String {
    const HEX: &[u8; 16] = b"0123456789ABCDEF";
    let mut out = String::with_capacity(value.len());
    for &byte in value.as_bytes() {
        let keep = byte.is_ascii_alphanumeric()
            || matches!(
                byte,
                b'-' | b'_'
                    | b'.'
                    | b'~'
                    | b'$'
                    | b'&'
                    | b'+'
                    | b':'
                    | b'='
                    | b'@'
            );
        if keep {
            out.push(byte as char);
        } else {
            out.push('%');
            out.push(HEX[usize::from(byte >> 4)] as char);
            out.push(HEX[usize::from(byte & 0xf)] as char);
        }
    }
    out
}

/// `url.JoinPath("a/changes", url.PathEscape(id))` followed by
/// `url.URL{Path: …}.String()`. Go's `URL.String` re-encodes the already
/// escaped segment with `encodePath`, so `%` becomes `%25`.
pub fn change_path(change_id: &str) -> String {
    let raw = format!("a/changes/{}", path_escape_segment(change_id));
    crate::bitbucketcloud::encode_go_path(&raw)
}

/// `Change.Created`/`Change.Updated` are re-emitted with the baseline's
/// timestamp layout; an absent/invalid value is the zero time.
pub fn format_change_time(raw: &str) -> String {
    if raw.is_empty() {
        "0001-01-01 00:00:00.000000000".to_owned()
    } else {
        raw.to_owned()
    }
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
        Client::new(
            Url::parse("https://gerrit.example.com").unwrap(),
            &AccountCredentials {
                username: "user".to_owned(),
                password: "pass".to_owned(),
            },
            doer,
        )
    }

    #[test]
    fn query_encoding_matches_url_values() {
        assert_eq!(
            encode_query(&[
                ("n", "100".to_owned()),
                ("S", "0".to_owned()),
                ("type", "CODE".to_owned()),
            ]),
            "S=0&n=100&type=CODE"
        );
    }

    #[test]
    fn xssi_prefix_is_stripped() {
        assert_eq!(strip_xssi_prefix(b")]}'\n{}"), b"\n{}");
        assert_eq!(strip_xssi_prefix(b"{}"), b"{}");
    }

    #[tokio::test]
    async fn code_projects_request_shape_and_pagination() {
        let doer = Arc::new(RecordingDoer {
            responses: Mutex::new(vec![HttpResponse {
                status: 200,
                headers: Vec::new(),
                body: b")]}'\n{\"a\":{\"id\":\"a\",\"name\":\"a\"}}".to_vec(),
            }]),
            requests: Mutex::new(Vec::new()),
        });
        let client = client(doer.clone());
        let (projects, next) = client
            .list_projects(&ListProjectsArgs {
                cursor: Pagination {
                    per_page: 100,
                    page: 1,
                    skip: 0,
                },
                only_code_projects: true,
            })
            .await
            .unwrap();
        assert_eq!(projects.len(), 1);
        assert!(next);
        let requests = doer.requests.lock().unwrap();
        assert_eq!(
            requests[0].url,
            "https://gerrit.example.com/a/projects/?S=0&n=100&type=CODE"
        );
        assert_eq!(
            requests[0].headers[0],
            (
                "Authorization".to_owned(),
                "Basic dXNlcjpwYXNz".to_owned()
            )
        );
        // A non-empty CODE page always claims a next page and does not probe.
        assert_eq!(requests.len(), 1);
    }

    #[tokio::test]
    async fn empty_code_page_probes_the_next_all_projects_page() {
        let doer = Arc::new(RecordingDoer {
            responses: Mutex::new(vec![
                HttpResponse {
                    status: 200,
                    headers: Vec::new(),
                    body: b")]}'\n{}".to_vec(),
                },
                HttpResponse {
                    status: 200,
                    headers: Vec::new(),
                    body: b")]}'\n{\"b\":{\"id\":\"b\",\"name\":\"b\"}}".to_vec(),
                },
            ]),
            requests: Mutex::new(Vec::new()),
        });
        let client = client(doer.clone());
        let (projects, next) = client
            .list_projects(&ListProjectsArgs {
                cursor: Pagination {
                    per_page: 100,
                    page: 1,
                    skip: 0,
                },
                only_code_projects: true,
            })
            .await
            .unwrap();
        assert!(projects.is_empty());
        assert!(next);
        let requests = doer.requests.lock().unwrap();
        assert_eq!(
            requests[0].url,
            "https://gerrit.example.com/a/projects/?S=0&n=100&type=CODE"
        );
        assert_eq!(
            requests[1].url,
            "https://gerrit.example.com/a/projects/?S=100&n=1"
        );
    }

    #[tokio::test]
    async fn ssh_info_parses_host_and_port() {
        let doer = Arc::new(RecordingDoer {
            responses: Mutex::new(vec![HttpResponse {
                status: 200,
                headers: Vec::new(),
                body: b"gerrit.example.com 29418\n".to_vec(),
            }]),
            requests: Mutex::new(Vec::new()),
        });
        let client = client(doer);
        assert_eq!(
            client.get_ssh_info().await.unwrap(),
            ("gerrit.example.com".to_owned(), 29418)
        );
    }
}
