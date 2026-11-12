#![forbid(unsafe_code)]

//! `POST /.api/repos/{Repo}/-/refresh` (`cmd/frontend/internal/httpapi/
//! httpapi.go:322-323`, `repo_refresh.go:15-27`).
//!
//! The gorilla route is registered on the repo subrouter, so the request goes
//! through `JsonMiddleware`:
//!
//! * success is a 200 with `Content-Type: application/json` and an empty body
//!   (`HandlerWithErrorReturn` writes nothing when the handler returns nil);
//! * a handler error is mapped by `errcode.HTTP` (repo not found 404, clone in
//!   progress 202, deadline exceeded 408, otherwise 500) and rendered by
//!   `ErrorHandler.Handle`: `cache-control: no-cache, max-age=0`, then
//!   `http.Error` (`text/plain; charset=utf-8`, `nosniff`, body `"\n"` unless
//!   `WriteErrBody`);
//! * a renamed repo is `handlerutil.URLMovedError`, which
//!   `RedirectToNewRepoName` turns into a 301 with the `Repo` path variable
//!   substituted. The baseline keeps `JsonMiddleware`'s content type on that
//!   redirect, so the response still says `application/json`.
//!
//! `handlerutil.GetRepo` (`repo.go:22-34`) and
//! `repoupdater.Client.EnqueueRepoUpdate` (`internal/repoupdater/client.go:97`)
//! are behind [`RepoRefreshDeps`].

use async_trait::async_trait;
use bytes::Bytes;
use http::{header, HeaderValue, Request, Response, StatusCode};
use http_body_util::Full;
use tonic::body::BoxBody;

use crate::internal_api::{hex_escape_non_ascii, located, text_response};

/// The failures `serveRepoRefresh` can surface, already classified the way
/// `errcode.HTTP` (`internal/errcode/code.go:22-40`) would.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RepoRefreshError {
    /// `gitdomain.RepoNotExistError`.
    NotFound,
    /// `gitdomain.RepoNotExistError{CloneInProgress: true}`.
    CloneInProgress,
    /// `context.DeadlineExceeded`.
    DeadlineExceeded,
    /// Any other error -> 500.
    Other(String),
}

impl RepoRefreshError {
    /// `errcode.HTTP`.
    pub fn status(&self) -> StatusCode {
        match self {
            RepoRefreshError::NotFound => StatusCode::NOT_FOUND,
            RepoRefreshError::CloneInProgress => StatusCode::ACCEPTED,
            RepoRefreshError::DeadlineExceeded => StatusCode::REQUEST_TIMEOUT,
            RepoRefreshError::Other(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}

impl From<String> for RepoRefreshError {
    fn from(message: String) -> Self {
        RepoRefreshError::Other(message)
    }
}

/// The two baseline calls behind the handler.
#[async_trait]
pub trait RepoRefreshDeps: Send + Sync {
    /// `handlerutil.GetRepo`: the canonical repository name. A name that
    /// differs from the requested one is the baseline's `URLMovedError`.
    async fn get_repo(&self, repo: &str) -> Result<String, RepoRefreshError>;

    /// `repoupdater.DefaultClient.EnqueueRepoUpdate`.
    async fn enqueue_repo_update(&self, repo: &str) -> Result<(), RepoRefreshError>;
}

/// Placeholder wiring until the repoupdater client is ported: every request
/// is the baseline's "gitserver unavailable" 500.
pub struct UnavailableRepoRefreshDeps;

#[async_trait]
impl RepoRefreshDeps for UnavailableRepoRefreshDeps {
    async fn get_repo(&self, _repo: &str) -> Result<String, RepoRefreshError> {
        Err(RepoRefreshError::Other(
            "repo refresh is not wired in this build".to_owned(),
        ))
    }

    async fn enqueue_repo_update(&self, _repo: &str) -> Result<(), RepoRefreshError> {
        Err(RepoRefreshError::Other(
            "repo refresh is not wired in this build".to_owned(),
        ))
    }
}

/// `serveRepoRefresh`.
pub async fn repo_refresh_response<B>(
    req: &Request<B>,
    repo: &str,
    deps: &dyn RepoRefreshDeps,
) -> Response<BoxBody> {
    debug_assert_eq!(req.method(), http::Method::POST);
    debug_assert!(!repo.is_empty());

    let canonical = match deps.get_repo(repo).await {
        Ok(name) => name,
        Err(error) => return json_error(error.status()),
    };

    if canonical != repo {
        // `URLMovedError` -> `RedirectToNewRepoName`.
        return moved_response(req, &canonical);
    }

    match deps.enqueue_repo_update(&canonical).await {
        Ok(()) => json_success(),
        Err(error) => json_error(error.status()),
    }
}

/// The `JsonMiddleware` success response: 200, `application/json`, no body.
fn json_success() -> Response<BoxBody> {
    let mut response = Response::new(tonic::body::boxed(Full::new(Bytes::new())));
    response
        .headers_mut()
        .insert(header::CONTENT_TYPE, HeaderValue::from_static("application/json"));
    response
}

/// `ErrorHandler.Handle` with `WriteErrBody=false`.
fn json_error(status: StatusCode) -> Response<BoxBody> {
    let mut response = text_response(status, "text/plain; charset=utf-8", "\n", true);
    response.headers_mut().insert(
        header::CACHE_CONTROL,
        HeaderValue::from_static("no-cache, max-age=0"),
    );
    response
}

/// `http.Redirect(..., StatusMovedPermanently)` for a POST request: the
/// `Location` is the route URL with `Repo` replaced, the query is retained,
/// and no `Content-Type` is written (so `JsonMiddleware`'s stays).
fn moved_response<B>(req: &Request<B>, new_name: &str) -> Response<BoxBody> {
    let mut location = format!("/.api/repos/{new_name}/-/refresh");
    if let Some(query) = req.uri().query() {
        location.push('?');
        location.push_str(query);
    }
    moved_location(&location)
}

fn moved_location(location: &str) -> Response<BoxBody> {
    located(
        StatusCode::MOVED_PERMANENTLY,
        hex_escape_non_ascii(location),
        Some("application/json"),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use http_body_util::BodyExt;

    struct Fake {
        canonical: Result<String, RepoRefreshError>,
        enqueue: Result<(), RepoRefreshError>,
    }

    #[async_trait]
    impl RepoRefreshDeps for Fake {
        async fn get_repo(&self, _repo: &str) -> Result<String, RepoRefreshError> {
            self.canonical.clone()
        }

        async fn enqueue_repo_update(&self, _repo: &str) -> Result<(), RepoRefreshError> {
            self.enqueue.clone()
        }
    }

    async fn run(canonical: Result<String, RepoRefreshError>, enqueue: Result<(), RepoRefreshError>) -> Response<BoxBody> {
        let request = Request::builder()
            .method("POST")
            .uri("/.api/repos/github.com/foo/bar/-/refresh?x=1")
            .body(())
            .expect("request");
        repo_refresh_response(
            &request,
            "github.com/foo/bar",
            &Fake { canonical, enqueue },
        )
        .await
    }

    #[tokio::test]
    async fn success_is_an_empty_json_200() {
        let response = run(Ok("github.com/foo/bar".to_owned()), Ok(())).await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers().get(header::CONTENT_TYPE).unwrap(),
            "application/json"
        );
        assert!(response.into_body().collect().await.unwrap().to_bytes().is_empty());
    }

    #[tokio::test]
    async fn missing_repo_is_404_with_error_contract() {
        let response = run(Err(RepoRefreshError::NotFound), Ok(())).await;
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        assert_eq!(
            response.headers().get(header::CONTENT_TYPE).unwrap(),
            "text/plain; charset=utf-8"
        );
        assert_eq!(
            response.headers().get(header::CACHE_CONTROL).unwrap(),
            "no-cache, max-age=0"
        );
        assert_eq!(
            response.headers().get(header::X_CONTENT_TYPE_OPTIONS).unwrap(),
            "nosniff"
        );
        assert_eq!(
            response.into_body().collect().await.unwrap().to_bytes(),
            Bytes::from_static(b"\n")
        );
    }

    #[tokio::test]
    async fn clone_in_progress_is_202() {
        let response = run(Err(RepoRefreshError::CloneInProgress), Ok(())).await;
        assert_eq!(response.status(), StatusCode::ACCEPTED);
    }

    #[tokio::test]
    async fn renamed_repo_is_a_301_redirect() {
        let response = run(Ok("github.com/foo/baz".to_owned()), Ok(())).await;
        assert_eq!(response.status(), StatusCode::MOVED_PERMANENTLY);
        assert_eq!(
            response.headers().get(header::LOCATION).unwrap(),
            "/.api/repos/github.com/foo/baz/-/refresh?x=1"
        );
        assert_eq!(
            response.headers().get(header::CONTENT_TYPE).unwrap(),
            "application/json"
        );
    }
}
