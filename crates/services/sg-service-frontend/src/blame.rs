#![forbid(unsafe_code)]

//! `handleStreamBlame` (`cmd/frontend/internal/httpapi/stream_blame.go:31-176`)
//! for `/.api/blame/{Repo}{@rev}/-/stream/{Path:.*}`.
//!
//! The handler logic is dependency-injected ([`BlameDeps`]) so the
//! differential harness drives the production code with fake repositories,
//! revisions, hunks and users, mirroring the Go harness's
//! `backend.Mocks`/`dbmocks`/`gitserver.MockClient` setup. [`BackendDeps`] is
//! the production wiring: `repo`/`users` from `sg-db` and
//! `ResolveRevision`/`Blame` from `sg-git`, with the shard address resolved
//! through the site configuration's pins (`streamhttp`'s five headers and the
//! `event: hunk|done` wire format are reproduced byte for byte).
//!
//! ## Baseline contract
//!
//! - `GetRepoAndRev` errors write only a status code (404 for not
//!   found/blocked/revision-not-found, 401 for unauthorized, 500 otherwise);
//! - a missing blamed file is `*os.PathError` (`os.IsNotExist`) and becomes
//!   `http.Error(w, err.Error(), 404)` *before* the stream writer exists;
//! - otherwise the SSE writer starts with `Content-Type: text/event-stream`,
//!   `Cache-Control: no-cache`, `Connection: keep-alive`,
//!   `Transfer-Encoding: chunked`, `X-Accel-Buffering: no`;
//! - each hunk is `event: hunk\ndata: [<hunk>]\n\n`; end of stream is
//!   `event: done\ndata: {}\n\n`; a read error mid-stream appends the
//!   HTML-escaped error text followed by `\n` and ends the response.

use std::collections::HashMap;
use std::convert::Infallible;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use bytes::Bytes;
use http::{header, HeaderValue, Response, StatusCode};
use http_body_util::StreamBody;
use hyper::body::Frame;
use prost_types::Timestamp;
use sqlx::PgPool;
use time::OffsetDateTime;
use tokio_stream::{Stream, StreamExt};
use tonic::body::BoxBody;

use sg_config::LiveConfig;
use sg_db::users::UserProfile;
use sg_git::{addresses::GitserverAddresses, BlameHunk, GitserverClient, Signature};

use crate::internal_api::{empty_response, text_response};

/// `stream_blame.go`'s error mapping inputs for `GetRepoAndRev`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RepoError {
    /// `database.RepoNotFoundError` (`errcode.IsNotFound`).
    NotFound,
    /// `types.BlockedRepoError` (`errcode.IsBlocked`).
    Blocked,
    /// An `errcode.IsUnauthorized` error.
    Unauthorized,
    /// Anything else (500).
    Other(String),
}

/// `ResolveRev` / `getRepoRev` error mapping inputs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RevError {
    /// `gitdomain.RevisionNotFoundError`, or any error `errcode.IsNotFound`
    /// matches (including `RepoNotCloneableErr` with `NotFound`).
    NotFound,
    /// An `errcode.IsUnauthorized` error. The gitserver client never produces
    /// one, but the baseline keeps the branch.
    Unauthorized,
    /// Anything else (500).
    Other(String),
}

/// `gitserver.StreamBlameFile`'s error surface.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BlameStreamError {
    /// The `*os.PathError{Op:"open", Err:os.ErrNotExist}` returned for a
    /// `FileNotFoundPayload` (`os.IsNotExist`).
    FileNotExist { path: String },
    /// Anything else.
    Other(String),
}

impl std::fmt::Display for BlameStreamError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            // `os.PathError.Error()`.
            Self::FileNotExist { path } => write!(f, "open {path}: file does not exist"),
            Self::Other(message) => f.write_str(message),
        }
    }
}

/// `db.Users().GetByVerifiedEmail` failure (non-not-found only).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UserError(pub String);

/// `BlameHunkUserResponse`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlameHunkUser {
    pub username: String,
    pub display_name: Option<String>,
    pub avatar_url: Option<String>,
}

/// Canonical repository identity (`types.Repo.Name`/`ID`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RepoInfo {
    pub name: String,
}

/// One hunk with the stream's error type.
pub type BlameStream = Pin<Box<dyn Stream<Item = Result<BlameHunk, BlameStreamError>> + Send>>;

/// The handler's four dependencies (`handlerutil`, `database.Users` and
/// `gitserver.Client`).
#[async_trait]
pub trait BlameDeps: Send + Sync {
    /// `handlerutil.GetRepo`'s `backend.NewRepos(...).GetByName`.
    async fn get_repo(&self, name: &str) -> Result<RepoInfo, RepoError>;

    /// `backend.NewRepos(...).ResolveRev` (`ResolveRevision` with
    /// `EnsureRevision=true`).
    async fn resolve_rev(&self, repo: &str, rev: &str) -> Result<String, RevError>;

    /// `gitserver.Client.StreamBlameFile` with `NewestCommit`.
    async fn stream_blame(
        &self,
        repo: &str,
        path: &str,
        newest_commit: &str,
    ) -> Result<BlameStream, BlameStreamError>;

    /// `db.Users().GetByVerifiedEmail`; `Ok(None)` is the baseline's
    /// not-found error (the handler caches a nil user for it).
    async fn user_by_verified_email(&self, email: &str)
        -> Result<Option<BlameHunkUser>, UserError>;
}

/// Production wiring for [`BlameDeps`].
pub struct BackendDeps {
    pool: PgPool,
    live: Arc<LiveConfig>,
    /// Connected clients keyed by the shard list they were built for, so a
    /// live-configuration change to the gitserver addresses starts a new
    /// client while ordinary requests reuse channels.
    clients: tokio::sync::Mutex<HashMap<Vec<String>, Arc<GitserverClient>>>,
}

impl BackendDeps {
    pub fn new(pool: PgPool, live: Arc<LiveConfig>) -> Self {
        Self {
            pool,
            live,
            clients: tokio::sync::Mutex::new(HashMap::new()),
        }
    }

    /// `ClientForRepo` under the live configuration: the pinned address wins
    /// (`connection.GitserverAddresses`), otherwise the shard list is hashed.
    async fn client_for(&self, repo: &str) -> Result<(Arc<GitserverClient>, String), String> {
        let unified = self.live.current();
        let addresses = GitserverAddresses::from_unified(&unified);
        let addr = addresses
            .addr_for_repo(repo)
            .ok_or_else(|| "no gitserver configured".to_owned())?
            .to_owned();
        let mut clients = self.clients.lock().await;
        let client = match clients.get(&addresses.addresses) {
            Some(client) => Arc::clone(client),
            None => {
                let client = Arc::new(GitserverClient::new(addresses.addresses.clone()));
                clients.insert(addresses.addresses.clone(), Arc::clone(&client));
                client
            }
        };
        Ok((client, addr))
    }
}

#[async_trait]
impl BlameDeps for BackendDeps {
    async fn get_repo(&self, name: &str) -> Result<RepoInfo, RepoError> {
        let store = sg_db::repos::RepoStore::new(self.pool.clone());
        match store.get_by_name(name).await {
            Ok(None) => Err(RepoError::NotFound),
            Ok(Some(repo)) if repo.blocked.is_some() => Err(RepoError::Blocked),
            Ok(Some(repo)) => Ok(RepoInfo { name: repo.name }),
            Err(err) => Err(RepoError::Other(err.to_string())),
        }
    }

    async fn resolve_rev(&self, repo: &str, rev: &str) -> Result<String, RevError> {
        let (client, addr) = self.client_for(repo).await.map_err(RevError::Other)?;
        client
            .resolve_revision_at(&addr, repo, rev, true)
            .await
            .map_err(|err| {
                if err.is_not_found() {
                    RevError::NotFound
                } else {
                    RevError::Other(err.to_string())
                }
            })
    }

    async fn stream_blame(
        &self,
        repo: &str,
        path: &str,
        newest_commit: &str,
    ) -> Result<BlameStream, BlameStreamError> {
        let (client, addr) = self
            .client_for(repo)
            .await
            .map_err(BlameStreamError::Other)?;
        let stream = client
            .stream_blame_file_at(&addr, repo, path, newest_commit)
            .await
            .map_err(|err| match err {
                sg_git::Error::FileNotExist { path } => BlameStreamError::FileNotExist { path },
                other => BlameStreamError::Other(other.to_string()),
            })?;
        Ok(Box::pin(stream.map(|item| {
            item.map_err(|err| match err {
                sg_git::Error::FileNotExist { path } => BlameStreamError::FileNotExist { path },
                other => BlameStreamError::Other(other.to_string()),
            })
        })))
    }

    async fn user_by_verified_email(
        &self,
        email: &str,
    ) -> Result<Option<BlameHunkUser>, UserError> {
        let user = sg_db::users::get_by_verified_email(&self.pool, email)
            .await
            .map_err(|err| UserError(err.to_string()))?;
        Ok(user.map(blame_user))
    }
}

/// `BlameHunkUserResponse`: the baseline only sets `displayName`/`avatarURL`
/// when the stored value is non-empty.
fn blame_user(user: UserProfile) -> BlameHunkUser {
    BlameHunkUser {
        username: user.username,
        display_name: (!user.display_name.is_empty()).then_some(user.display_name),
        avatar_url: (!user.avatar_url.is_empty()).then_some(user.avatar_url),
    }
}

/// `handleStreamBlame`'s request pipeline. `repo` is the decoded `Repo` route
/// variable, `rev` the `Rev` variable with its `@` removed (`None` when the
/// URL had none), `path` the raw `Path` variable.
pub async fn blame_response<B>(
    req: &http::Request<B>,
    repo: &str,
    rev: Option<&str>,
    path: &str,
    deps: Arc<dyn BlameDeps>,
) -> Response<BoxBody> {
    debug_assert_eq!(req.method(), http::Method::GET);

    let repo_name = match deps.get_repo(repo).await {
        Ok(info) => {
            // `handlerutil.GetRepo`: a canonical name that differs from the
            // requested one is `URLMovedError`, which falls through to 500.
            if info.name != repo {
                return empty_response(StatusCode::INTERNAL_SERVER_ERROR);
            }
            info.name
        }
        Err(RepoError::NotFound | RepoError::Blocked) => {
            return empty_response(StatusCode::NOT_FOUND)
        }
        Err(RepoError::Unauthorized) => return empty_response(StatusCode::UNAUTHORIZED),
        Err(RepoError::Other(_)) => return empty_response(StatusCode::INTERNAL_SERVER_ERROR),
    };

    let commit = match deps.resolve_rev(&repo_name, rev.unwrap_or("")).await {
        Ok(commit) => commit,
        Err(RevError::NotFound) => return empty_response(StatusCode::NOT_FOUND),
        Err(RevError::Unauthorized) => return empty_response(StatusCode::UNAUTHORIZED),
        Err(RevError::Other(_)) => return empty_response(StatusCode::INTERNAL_SERVER_ERROR),
    };

    // `strings.TrimPrefix(requestedPath, "/")`: exactly one leading slash.
    let requested_path = path.strip_prefix('/').unwrap_or(path);

    let hunks = match deps.stream_blame(&repo_name, requested_path, &commit).await {
        Ok(hunks) => hunks,
        Err(BlameStreamError::FileNotExist { .. }) => {
            return http_error(
                StatusCode::NOT_FOUND,
                &BlameStreamError::FileNotExist {
                    path: requested_path.to_owned(),
                },
            );
        }
        Err(err) => return http_error(StatusCode::INTERNAL_SERVER_ERROR, &err),
    };

    sse_response(hunks, repo_name, deps).await
}

/// `http.Error(w, msg, code)`: text/plain, nosniff, message plus a newline.
fn http_error(status: StatusCode, err: &BlameStreamError) -> Response<BoxBody> {
    text_response(
        status,
        "text/plain; charset=utf-8",
        &format!("{err}\n"),
        true,
    )
}

/// `http.Error(w, html.EscapeString(msg), status)` after `streamhttp.NewWriter`
/// has already set the five SSE headers: the writer's headers stay on the
/// response, but `http.Error` overwrites `Content-Type`.
fn sse_error(status: StatusCode, message: &str) -> Response<BoxBody> {
    let mut response = text_response(
        status,
        "text/plain; charset=utf-8",
        &format!("{}\n", html_escape(message)),
        true,
    );
    let headers = response.headers_mut();
    headers.insert(header::CACHE_CONTROL, HeaderValue::from_static("no-cache"));
    headers.insert(header::CONNECTION, HeaderValue::from_static("keep-alive"));
    headers.insert(
        header::TRANSFER_ENCODING,
        HeaderValue::from_static("chunked"),
    );
    headers.insert(
        header::HeaderName::from_static("x-accel-buffering"),
        HeaderValue::from_static("no"),
    );
    response
}

/// `streamhttp.NewWriter` + the hunk loop.
async fn sse_response(
    mut hunks: BlameStream,
    repo: String,
    deps: Arc<dyn BlameDeps>,
) -> Response<BoxBody> {
    let terminated = Arc::new(AtomicBool::new(false));
    let cache: Arc<Mutex<HashMap<String, Option<BlameHunkUser>>>> =
        Arc::new(Mutex::new(HashMap::new()));

    // Go writes the status at its first `Write`; the first hunk's author
    // lookup and any first-read error therefore still produce a 500 with
    // `http.Error`'s plain-text body. Resolve that first frame eagerly so the
    // response status matches.
    let first_frame = match hunks.next().await {
        None => None,
        Some(Ok(hunk)) => match hunk_user(&hunk.author, &cache, deps.as_ref()).await {
            Ok(user) => Some(Frame::data(Bytes::from(format!(
                "event: hunk\ndata: [{}]\n\n",
                encode_hunk(&hunk, user.as_ref(), &repo)
            )))),
            Err(UserError(message)) => {
                return sse_error(StatusCode::INTERNAL_SERVER_ERROR, &message);
            }
        },
        Some(Err(err)) => {
            return sse_error(StatusCode::INTERNAL_SERVER_ERROR, &err.to_string());
        }
    };

    let events = {
        let terminated = Arc::clone(&terminated);
        let cache = Arc::clone(&cache);
        let repo = repo.clone();
        hunks
            .then(move |item| {
                let terminated = Arc::clone(&terminated);
                let cache = Arc::clone(&cache);
                let deps = Arc::clone(&deps);
                let repo = repo.clone();
                async move {
                    if terminated.load(Ordering::SeqCst) {
                        return None;
                    }
                    match item {
                        Ok(hunk) => {
                            let user = match hunk_user(&hunk.author, &cache, deps.as_ref()).await {
                                Ok(user) => user,
                                // `http.Error(w, html.EscapeString(err), 500)`.
                                Err(UserError(message)) => {
                                    terminated.store(true, Ordering::SeqCst);
                                    return Some(Frame::data(Bytes::from(format!(
                                        "{}\n",
                                        html_escape(&message)
                                    ))));
                                }
                            };
                            let json = encode_hunk(&hunk, user.as_ref(), &repo);
                            Some(Frame::data(Bytes::from(format!(
                                "event: hunk\ndata: [{json}]\n\n"
                            ))))
                        }
                        Err(err) => {
                            terminated.store(true, Ordering::SeqCst);
                            Some(Frame::data(Bytes::from(format!(
                                "{}\n",
                                html_escape(&err.to_string())
                            ))))
                        }
                    }
                }
            })
            .filter_map(|frame| frame)
    };

    // `streamWriter.Event("done", map[string]any{})`, only on a clean EOF.
    let done = {
        let terminated = Arc::clone(&terminated);
        tokio_stream::once(())
            .then(move |()| {
                let terminated = Arc::clone(&terminated);
                async move {
                    (!terminated.load(Ordering::SeqCst))
                        .then(|| Frame::data(Bytes::from_static(b"event: done\ndata: {}\n\n")))
                }
            })
            .filter_map(|frame| frame)
    };

    let body_stream = tokio_stream::iter(first_frame)
        .chain(events)
        .chain(done)
        .map(Ok::<_, Infallible>);
    let mut response = Response::new(tonic::body::boxed(StreamBody::new(body_stream)));
    let headers = response.headers_mut();
    headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("text/event-stream"),
    );
    headers.insert(header::CACHE_CONTROL, HeaderValue::from_static("no-cache"));
    headers.insert(header::CONNECTION, HeaderValue::from_static("keep-alive"));
    headers.insert(
        header::TRANSFER_ENCODING,
        HeaderValue::from_static("chunked"),
    );
    headers.insert(
        header::HeaderName::from_static("x-accel-buffering"),
        HeaderValue::from_static("no"),
    );
    response
}

/// The author-cache path of the hunk loop: look the verified email up once
/// per address (including negative results), exactly like `authorCache`.
async fn hunk_user(
    author: &Signature,
    cache: &Mutex<HashMap<String, Option<BlameHunkUser>>>,
    deps: &dyn BlameDeps,
) -> Result<Option<BlameHunkUser>, UserError> {
    if author.email.is_empty() {
        return Ok(None);
    }
    if let Some(cached) = cache
        .lock()
        .expect("author cache poisoned")
        .get(&author.email)
        .cloned()
    {
        return Ok(cached);
    }
    let user = deps.user_by_verified_email(&author.email).await?;
    cache
        .lock()
        .expect("author cache poisoned")
        .insert(author.email.clone(), user.clone());
    Ok(user)
}

/// Go `html.EscapeString`: a single pass over the five special characters.
fn html_escape(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for ch in value.chars() {
        match ch {
            '&' => out.push_str("&amp;"),
            '\'' => out.push_str("&#39;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&#34;"),
            other => out.push(other),
        }
    }
    out
}

/// `[]BlameHunkResponse{…}` element, field order and `omitempty` matching
/// `encoding/json`.
fn encode_hunk(hunk: &BlameHunk, user: Option<&BlameHunkUser>, repo: &str) -> String {
    let mut out = String::from("{\"commitID\":");
    push_json_string(&mut out, &hunk.commit_id);
    out.push_str(",\"startLine\":");
    out.push_str(&hunk.start_line.to_string());
    out.push_str(",\"endLine\":");
    out.push_str(&hunk.end_line.to_string());

    out.push_str(",\"author\":{");
    let mut first = true;
    if !hunk.author.name.is_empty() {
        push_key(&mut out, &mut first, "Name");
        push_json_string(&mut out, &hunk.author.name);
    }
    if !hunk.author.email.is_empty() {
        push_key(&mut out, &mut first, "Email");
        push_json_string(&mut out, &hunk.author.email);
    }
    push_key(&mut out, &mut first, "Date");
    push_json_string(&mut out, &go_time_rfc3339_nano(hunk.author.date.as_ref()));
    out.push('}');

    out.push_str(",\"message\":");
    push_json_string(&mut out, &hunk.message);
    out.push_str(",\"filename\":");
    push_json_string(&mut out, &hunk.filename);

    out.push_str(",\"commit\":{");
    let mut first = true;
    if let Some(previous) = &hunk.previous_commit {
        push_key(&mut out, &mut first, "previous");
        out.push_str("{\"commitID\":");
        push_json_string(&mut out, &previous.commit_id);
        out.push_str(",\"filename\":");
        push_json_string(&mut out, &previous.filename);
        out.push('}');
    }
    push_key(&mut out, &mut first, "url");
    push_json_string(&mut out, &format!("{repo}/-/commit/{}", hunk.commit_id));
    out.push('}');

    if let Some(user) = user {
        out.push_str(",\"user\":{");
        out.push_str("\"username\":");
        push_json_string(&mut out, &user.username);
        // `DisplayName`/`AvatarURL` have no `omitempty`: the pointers are
        // always emitted, as `null` when unset.
        out.push_str(",\"displayName\":");
        match &user.display_name {
            Some(display_name) => push_json_string(&mut out, display_name),
            None => out.push_str("null"),
        }
        out.push_str(",\"avatarURL\":");
        match &user.avatar_url {
            Some(avatar_url) => push_json_string(&mut out, avatar_url),
            None => out.push_str("null"),
        }
        out.push('}');
    }

    out.push('}');
    out
}

fn push_key(out: &mut String, first: &mut bool, key: &str) {
    if !*first {
        out.push(',');
    }
    *first = false;
    push_json_string(out, key);
    out.push(':');
}

/// `encoding/json`'s `string` encoding, including its HTML escaping
/// (`<`, `>`, `&` become `<`, `>`, `&`) and the
/// U+2028/U+2029 escapes.
fn push_json_string(out: &mut String, value: &str) {
    out.push('"');
    for ch in value.chars() {
        match ch {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            '\u{8}' => out.push_str("\\b"),
            '\u{c}' => out.push_str("\\f"),
            '<' => out.push_str("\\u003c"),
            '>' => out.push_str("\\u003e"),
            '&' => out.push_str("\\u0026"),
            '\u{2028}' => out.push_str("\\u2028"),
            '\u{2029}' => out.push_str("\\u2029"),
            ch if (ch as u32) < 0x20 => {
                out.push_str(&format!("\\u{:04x}", ch as u32));
            }
            ch => out.push(ch),
        }
    }
    out.push('"');
}

/// `time.Time.MarshalJSON` = `RFC3339Nano` in UTC: fractional seconds are
/// trimmed of trailing zeros, and an absent timestamp is the Unix epoch
/// (`(*timestamppb.Timestamp)(nil).AsTime()`).
fn go_time_rfc3339_nano(date: Option<&Timestamp>) -> String {
    let seconds = date.map_or(0, |date| date.seconds);
    let nanos = date.map_or(0, |date| date.nanos);
    let Ok(time) = OffsetDateTime::from_unix_timestamp(seconds) else {
        // `timestamppb` guarantees 0001..9999, which `time` supports.
        return "1970-01-01T00:00:00Z".to_owned();
    };
    let time = time
        .replace_nanosecond(u32::try_from(nanos).unwrap_or(0))
        .unwrap_or(time);
    let mut out = format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}",
        time.year(),
        u8::from(time.month()),
        time.day(),
        time.hour(),
        time.minute(),
        time.second(),
    );
    if nanos != 0 {
        let fraction = format!("{:09}", nanos.max(0));
        out.push('.');
        out.push_str(fraction.trim_end_matches('0'));
    }
    out.push('Z');
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::external_api::{api_response_with_deps, route_for_test, Route};
    use http::Request;
    use http_body_util::BodyExt;
    use sg_config::{ServiceConnections, SiteConfiguration, Unified};
    use std::sync::Mutex as StdMutex;

    struct FakeDeps {
        repo: StdMutex<Result<RepoInfo, RepoError>>,
        rev: StdMutex<Result<String, RevError>>,
        hunks: StdMutex<Result<Vec<Result<BlameHunk, BlameStreamError>>, BlameStreamError>>,
        users: StdMutex<HashMap<String, Result<Option<BlameHunkUser>, UserError>>>,
        calls: StdMutex<Vec<String>>,
    }

    impl Default for FakeDeps {
        fn default() -> Self {
            Self {
                repo: StdMutex::new(Ok(RepoInfo {
                    name: String::new(),
                })),
                rev: StdMutex::new(Ok(String::new())),
                hunks: StdMutex::new(Ok(Vec::new())),
                users: StdMutex::new(HashMap::new()),
                calls: StdMutex::new(Vec::new()),
            }
        }
    }

    #[async_trait]
    impl BlameDeps for FakeDeps {
        async fn get_repo(&self, name: &str) -> Result<RepoInfo, RepoError> {
            self.calls.lock().unwrap().push(format!("get_repo {name}"));
            self.repo.lock().unwrap().clone()
        }

        async fn resolve_rev(&self, repo: &str, rev: &str) -> Result<String, RevError> {
            self.calls
                .lock()
                .unwrap()
                .push(format!("resolve_rev {repo} {rev}"));
            self.rev.lock().unwrap().clone()
        }

        async fn stream_blame(
            &self,
            repo: &str,
            path: &str,
            newest_commit: &str,
        ) -> Result<BlameStream, BlameStreamError> {
            self.calls
                .lock()
                .unwrap()
                .push(format!("stream_blame {repo} {path} {newest_commit}"));
            let items = self.hunks.lock().unwrap().clone()?;
            Ok(Box::pin(tokio_stream::iter(items)))
        }

        async fn user_by_verified_email(
            &self,
            email: &str,
        ) -> Result<Option<BlameHunkUser>, UserError> {
            self.calls.lock().unwrap().push(format!("user {email}"));
            self.users
                .lock()
                .unwrap()
                .get(email)
                .cloned()
                .unwrap_or(Ok(None))
        }
    }

    fn live() -> LiveConfig {
        LiveConfig::new(Unified {
            site: SiteConfiguration::parse("{}").expect("site"),
            service_connections: ServiceConnections::default(),
        })
    }

    fn hunk() -> BlameHunk {
        BlameHunk {
            start_line: 1,
            end_line: 2,
            start_byte: 0,
            end_byte: 10,
            commit_id: "abc".to_owned(),
            previous_commit: None,
            author: Signature {
                name: "A <B>".to_owned(),
                email: "a@example.com".to_owned(),
                date: Some(Timestamp {
                    seconds: 1_700_000_000,
                    nanos: 0,
                }),
            },
            message: "msg".to_owned(),
            filename: "f.go".to_owned(),
        }
    }

    fn request(target: &str) -> Request<()> {
        Request::builder()
            .method(http::Method::GET)
            .uri(target)
            .body(())
            .expect("request")
    }

    async fn run(deps: &Arc<FakeDeps>, target: &str) -> (StatusCode, http::HeaderMap, String) {
        let deps: Arc<dyn BlameDeps> = deps.clone();
        let response = api_response_with_deps(&request(target), &live(), deps).await;
        let status = response.status();
        let headers = response.headers().clone();
        let body = response
            .into_body()
            .collect()
            .await
            .expect("body")
            .to_bytes();
        (status, headers, String::from_utf8_lossy(&body).into_owned())
    }

    #[tokio::test]
    async fn streams_hunks_and_done() {
        let deps = FakeDeps::default();
        *deps.repo.lock().unwrap() = Ok(RepoInfo {
            name: "github.com/foo/bar".to_owned(),
        });
        *deps.rev.lock().unwrap() = Ok("commit1".to_owned());
        *deps.hunks.lock().unwrap() = Ok(vec![Ok(hunk()), Ok(hunk())]);
        deps.users.lock().unwrap().insert(
            "a@example.com".to_owned(),
            Ok(Some(BlameHunkUser {
                username: "alice".to_owned(),
                display_name: Some("Alice".to_owned()),
                avatar_url: None,
            })),
        );
        let deps = Arc::new(deps);
        let (status, headers, body) =
            run(&deps, "/.api/blame/github.com/foo/bar/-/stream/f.go").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(headers[header::CONTENT_TYPE], "text/event-stream");
        assert_eq!(headers[header::CACHE_CONTROL], "no-cache");
        assert_eq!(headers[header::CONNECTION], "keep-alive");
        assert_eq!(headers[header::TRANSFER_ENCODING], "chunked");
        assert_eq!(headers["x-accel-buffering"], "no");
        assert!(body.contains("\"commitID\":\"abc\""));
        assert!(body.contains("\"Name\":\"A \\u003cB\\u003e\""));
        assert!(body.contains("\"Date\":\"2023-11-14T22:13:20Z\""));
        assert!(body.contains("\"url\":\"github.com/foo/bar/-/commit/abc\""));
        assert!(body.contains("\"displayName\":\"Alice\""));
        // No `omitempty` on the pointer fields.
        assert!(body.contains("\"avatarURL\":null"));
        assert_eq!(body.matches("event: hunk").count(), 2);
        assert!(body.ends_with("event: done\ndata: {}\n\n"));
        // One lookup for the shared email.
        assert_eq!(
            deps.calls
                .lock()
                .unwrap()
                .iter()
                .filter(|call| call.starts_with("user "))
                .count(),
            1
        );
    }

    #[tokio::test]
    async fn file_not_found_is_404_with_path_error() {
        let deps = FakeDeps::default();
        *deps.repo.lock().unwrap() = Ok(RepoInfo {
            name: "r".to_owned(),
        });
        *deps.rev.lock().unwrap() = Ok("c".to_owned());
        *deps.hunks.lock().unwrap() = Err(BlameStreamError::FileNotExist {
            path: "f.go".to_owned(),
        });
        let deps = Arc::new(deps);
        let (status, headers, body) = run(&deps, "/.api/blame/r/-/stream/f.go").await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(headers[header::CONTENT_TYPE], "text/plain; charset=utf-8");
        assert_eq!(headers[header::X_CONTENT_TYPE_OPTIONS], "nosniff");
        assert_eq!(body, "open f.go: file does not exist\n");
    }

    #[tokio::test]
    async fn revision_not_found_is_404_without_body() {
        let deps = FakeDeps::default();
        *deps.repo.lock().unwrap() = Ok(RepoInfo {
            name: "r".to_owned(),
        });
        *deps.rev.lock().unwrap() = Err(RevError::NotFound);
        let deps = Arc::new(deps);
        let (status, headers, body) = run(&deps, "/.api/blame/r@nope/-/stream/f.go").await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert!(headers.get(header::CONTENT_TYPE).is_none());
        assert!(body.is_empty());
    }

    #[tokio::test]
    async fn moved_repo_is_500() {
        let deps = FakeDeps::default();
        *deps.repo.lock().unwrap() = Ok(RepoInfo {
            name: "canonical".to_owned(),
        });
        let deps = Arc::new(deps);
        let (status, _, body) = run(&deps, "/.api/blame/orig/-/stream/f.go").await;
        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
        assert!(body.is_empty());
    }

    #[tokio::test]
    async fn stream_error_mid_way_appends_escaped_text() {
        let deps = FakeDeps::default();
        *deps.repo.lock().unwrap() = Ok(RepoInfo {
            name: "r".to_owned(),
        });
        *deps.rev.lock().unwrap() = Ok("c".to_owned());
        *deps.hunks.lock().unwrap() = Ok(vec![
            Ok(hunk()),
            Err(BlameStreamError::Other("bad <thing>".to_owned())),
        ]);
        let deps = Arc::new(deps);
        let (status, _, body) = run(&deps, "/.api/blame/r/-/stream/f.go").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body.matches("event: hunk").count(), 1);
        assert!(body.ends_with("bad &lt;thing&gt;\n"));
    }

    #[test]
    fn route_captures_rev_and_path() {
        assert_eq!(
            route_for_test("/.api/blame/github.com/foo/bar@main/-/stream/a/b.go"),
            Some(Route::BlameStream {
                repo: "github.com/foo/bar".to_owned(),
                rev: Some("main".to_owned()),
                path: "a/b.go".to_owned(),
            })
        );
        // gorilla's literal `/stream/` cannot match without the slash; the
        // trailing-slash case matches here and is redirected by
        // `route_request` before the handler sees it.
        assert_eq!(route_for_test("/.api/blame/foo/-/stream"), None);
        assert_eq!(
            route_for_test("/.api/blame/foo/-/stream/"),
            Some(Route::BlameStream {
                repo: "foo".to_owned(),
                rev: None,
                path: String::new(),
            })
        );
    }

    #[test]
    fn times_match_go_rfc3339_nano() {
        assert_eq!(
            go_time_rfc3339_nano(Some(&Timestamp {
                seconds: 0,
                nanos: 0
            })),
            "1970-01-01T00:00:00Z"
        );
        assert_eq!(
            go_time_rfc3339_nano(Some(&Timestamp {
                seconds: 1,
                nanos: 500_000_000
            })),
            "1970-01-01T00:00:01.5Z"
        );
        assert_eq!(
            go_time_rfc3339_nano(Some(&Timestamp {
                seconds: 1,
                nanos: 123_456_789
            })),
            "1970-01-01T00:00:01.123456789Z"
        );
        assert_eq!(go_time_rfc3339_nano(None), "1970-01-01T00:00:00Z");
    }
}
