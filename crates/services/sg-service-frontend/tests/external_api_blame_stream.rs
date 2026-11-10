//! M9.8ev differential harness: the external
//! `/.api/blame/{Repo}{@rev}/-/stream/{Path:.*}` endpoint, mirroring
//! `cmd/frontend/internal/httpapi/zz_blame_stream_harness_test.go`.
//!
//! Prints one `bs ` line per case with the status, `Location`,
//! `Content-Type`, `Cache-Control`, `Connection`, `Transfer-Encoding`,
//! `X-Accel-Buffering`, `X-Content-Type-Options` and body.

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use http::{header, Method, Request};
use http_body_util::{BodyExt, Full};
use prost_types::Timestamp;
use sg_config::{LiveConfig, ServiceConnections, SiteConfiguration, Unified};
use sg_git::{BlameHunk, PreviousCommit, Signature};
use sg_service_frontend::blame::{
    BlameDeps, BlameHunkUser, BlameStream, BlameStreamError, RepoError, RepoInfo, RevError,
    UserError,
};
use sg_service_frontend::external_api::api_response_with_deps;
use sg_service_frontend::search_stream::{SearchDeps, UnavailableDeps};

fn live() -> LiveConfig {
    let site = SiteConfiguration::parse("{}").expect("site");
    LiveConfig::new(Unified {
        site,
        service_connections: ServiceConnections::default(),
    })
}

fn timestamp(seconds: i64, nanos: i32) -> Option<Timestamp> {
    Some(Timestamp { seconds, nanos })
}

#[allow(clippy::too_many_arguments)] // Mirrors `gitdomain.Hunk`'s fields.
fn hunk(
    start: u32,
    end: u32,
    commit: &str,
    name: &str,
    email: &str,
    date: Option<Timestamp>,
    message: &str,
    filename: &str,
    previous: Option<(&str, &str)>,
) -> BlameHunk {
    BlameHunk {
        start_line: start,
        end_line: end,
        start_byte: 0,
        end_byte: 0,
        commit_id: commit.to_owned(),
        previous_commit: previous.map(|(commit_id, filename)| PreviousCommit {
            commit_id: commit_id.to_owned(),
            filename: filename.to_owned(),
        }),
        author: Signature {
            name: name.to_owned(),
            email: email.to_owned(),
            date,
        },
        message: message.to_owned(),
        filename: filename.to_owned(),
    }
}

fn full_hunk() -> BlameHunk {
    hunk(
        1,
        2,
        "abcd",
        "A <B> & \"C\"",
        "bob@internet.com",
        timestamp(1600000000, 123456789),
        "one <line> & \"quoted\"",
        "foo.c",
        Some(("prev1", "old.c")),
    )
}

fn minimal_hunk() -> BlameHunk {
    hunk(
        3,
        4,
        "ijkl",
        "",
        "dave@example.com",
        timestamp(1609556645, 0),
        "two",
        "foo.c",
        None,
    )
}

fn nil_user_hunk() -> BlameHunk {
    hunk(
        5,
        5,
        "mnop",
        "",
        "carol@example.com",
        timestamp(1643861106, 0),
        "three",
        "foo.c",
        None,
    )
}

fn empty_author_hunk() -> BlameHunk {
    hunk(
        1,
        1,
        "wxyz",
        "",
        "",
        timestamp(0, 0),
        "four",
        "bar.go",
        None,
    )
}

fn failing_user_hunk() -> BlameHunk {
    hunk(
        1,
        1,
        "fail",
        "",
        "fail@example.com",
        timestamp(1677906367, 0),
        "five",
        "usererr.go",
        None,
    )
}

/// `backend.Mocks` + `dbmocks` + `gitserver.MockClient`.
struct FakeDeps {
    calls: Mutex<Vec<String>>,
}

impl FakeDeps {
    fn new() -> Self {
        Self {
            calls: Mutex::new(Vec::new()),
        }
    }

    fn record(&self, call: String) {
        self.calls.lock().expect("calls").push(call);
    }
}

#[async_trait]
impl BlameDeps for FakeDeps {
    async fn get_repo(&self, name: &str) -> Result<RepoInfo, RepoError> {
        self.record(format!("repo {name}"));
        match name {
            "github.com/foo/bar" => Ok(RepoInfo {
                name: name.to_owned(),
            }),
            "foo" => Ok(RepoInfo {
                name: "moved/foo".to_owned(),
            }),
            _ => Err(RepoError::NotFound),
        }
    }

    async fn resolve_rev(&self, repo: &str, rev: &str) -> Result<String, RevError> {
        self.record(format!("rev {repo} {rev}"));
        match rev {
            "" => Ok("abcd".to_owned()),
            "1234" => Ok("efgh".to_owned()),
            _ => Err(RevError::NotFound),
        }
    }

    async fn stream_blame(
        &self,
        repo: &str,
        path: &str,
        newest_commit: &str,
    ) -> Result<BlameStream, BlameStreamError> {
        self.record(format!("blame {repo} {path} {newest_commit}"));
        let items: Vec<Result<BlameHunk, BlameStreamError>> = match path {
            "foo.c" => vec![Ok(full_hunk()), Ok(minimal_hunk()), Ok(nil_user_hunk())],
            "dir/bar.go" => vec![Ok(empty_author_hunk())],
            "broken.go" => vec![
                Ok(full_hunk()),
                Err(BlameStreamError::Other("boom <x> & \"y\"".to_owned())),
            ],
            "usererr.go" => vec![Ok(failing_user_hunk())],
            "missing.go" => {
                return Err(BlameStreamError::FileNotExist {
                    path: path.to_owned(),
                });
            }
            _ => vec![],
        };
        Ok(Box::pin(tokio_stream::iter(items)))
    }

    async fn user_by_verified_email(
        &self,
        email: &str,
    ) -> Result<Option<BlameHunkUser>, UserError> {
        self.record(format!("user {email}"));
        match email {
            "bob@internet.com" => Ok(Some(BlameHunkUser {
                username: "bob".to_owned(),
                display_name: Some("Bob".to_owned()),
                avatar_url: Some("https://example.com/bob.png".to_owned()),
            })),
            "dave@example.com" => Ok(Some(BlameHunkUser {
                username: "dave".to_owned(),
                display_name: None,
                avatar_url: None,
            })),
            "fail@example.com" => Err(UserError("db \"down\" <oops>".to_owned())),
            _ => Ok(None),
        }
    }
}

async fn run(deps: &Arc<FakeDeps>, name: &str, method: &str, target: &str) {
    let mut request = Request::builder()
        .method(Method::from_bytes(method.as_bytes()).expect("method"))
        .uri(target)
        .body(Full::new(bytes::Bytes::new()))
        .expect("request");
    let search_deps: Arc<dyn SearchDeps> = Arc::new(UnavailableDeps);
    let graphql_deps: Arc<dyn sg_service_frontend::graphql_api::GraphqlDeps> =
        Arc::new(sg_service_frontend::graphql_api::UnportedGraphqlDeps);
    let response = api_response_with_deps(
        &mut request,
        &live(),
        Arc::clone(deps) as Arc<dyn BlameDeps>,
        search_deps,
        graphql_deps,
    )
    .await;
    let status = response.status().as_u16();
    let field = |name: &http::HeaderName| match response.headers().get(name) {
        None => "-".to_owned(),
        Some(value) => format!("{value:?}"),
    };
    let location = field(&header::LOCATION);
    let ctype = field(&header::CONTENT_TYPE);
    let cache = field(&header::CACHE_CONTROL);
    let conn = field(&header::CONNECTION);
    let te = field(&header::TRANSFER_ENCODING);
    let xab = field(&http::HeaderName::from_static("x-accel-buffering"));
    let xcto = field(&header::X_CONTENT_TYPE_OPTIONS);
    let body = response
        .into_body()
        .collect()
        .await
        .expect("body")
        .to_bytes();
    let body = String::from_utf8_lossy(&body);
    println!(
        "bs {name} status={status} location={location} ctype={ctype} cache={cache} conn={conn} te={te} xab={xab} xcto={xcto} body={body:?}"
    );
}

#[tokio::test]
async fn external_api_blame_stream_harness() {
    if std::env::var("SG_BLAME_HARNESS").as_deref() != Ok("1") {
        return;
    }
    let deps = Arc::new(FakeDeps::new());
    let cases = [
        (
            "plain",
            "GET",
            "/.api/blame/github.com/foo/bar/-/stream/foo.c",
        ),
        (
            "rev",
            "GET",
            "/.api/blame/github.com/foo/bar@1234/-/stream/dir/bar.go",
        ),
        ("moved", "GET", "/.api/blame/foo/-/stream/foo.c"),
        ("repo-missing", "GET", "/.api/blame/nope/-/stream/foo.c"),
        (
            "rev-missing",
            "GET",
            "/.api/blame/github.com/foo/bar@void/-/stream/foo.c",
        ),
        (
            "at-no-rev",
            "GET",
            "/.api/blame/github.com/foo/bar@/-/stream/foo.c",
        ),
        (
            "file-missing",
            "GET",
            "/.api/blame/github.com/foo/bar/-/stream/missing.go",
        ),
        (
            "stream-error",
            "GET",
            "/.api/blame/github.com/foo/bar/-/stream/broken.go",
        ),
        (
            "user-error",
            "GET",
            "/.api/blame/github.com/foo/bar/-/stream/usererr.go",
        ),
        ("no-path", "GET", "/.api/blame/github.com/foo/bar/-/stream"),
        (
            "trailing",
            "GET",
            "/.api/blame/github.com/foo/bar/-/stream/foo.c/",
        ),
        (
            "empty-path",
            "GET",
            "/.api/blame/github.com/foo/bar/-/stream/",
        ),
        (
            "double-slash",
            "GET",
            "/.api/blame/github.com/foo/bar/-/stream//foo.c",
        ),
        (
            "post",
            "POST",
            "/.api/blame/github.com/foo/bar/-/stream/foo.c",
        ),
        (
            "encoded",
            "GET",
            "/.api/blame/github.com/foo%2Fbar/-/stream/foo.c",
        ),
        (
            "query",
            "GET",
            "/.api/blame/github.com/foo/bar/-/stream/foo.c?x=1",
        ),
        ("unknown", "GET", "/.api/nope"),
    ];
    for (name, method, target) in cases {
        run(&deps, name, method, target).await;
    }
}
