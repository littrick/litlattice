#![forbid(unsafe_code)]

//! M9.8bd differential harness (Rust side): the GitLab `SudoProvider`'s
//! `ValidateConnection`, `FetchRepoPerms` and `FetchAccount` against a
//! scripted GitLab API.
//!
//! Mirrors `internal/authz/providers/gitlab/sudo_harness_test.go`; outputs are
//! compared line for line by
//! `test_ws/p9-go/run_authz_gitlab_provider_diff.sh`. The server URL is
//! normalised to `SERVER` in error messages and recorded requests because the
//! two harnesses bind different ports.

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use http_body_util::BodyExt;
use sg_db::users::UserRecord;
use sg_http::HttpHandler;
use sg_service_worker::jobs::authz_github_user::OAuthBearerToken;
use sg_service_worker::jobs::authz_gitlab::{
    AuthUser, GitlabClient, GitlabClientError, GitlabClientProvider, Member, Project, SudoProvider,
};
use sg_service_worker::jobs::authz_gitlab_client::GitlabRestClientProvider;
use sg_service_worker::jobs::codehost_version_syncing::normalize_base_url;
use sg_service_worker::jobs::perms_syncer::{AuthzProvider, FetchPermsOptions, FetchRepo};
use tokio_util::sync::CancellationToken;

/// Server response for `GET /api/v4/projects`, selected per scenario.
type Mode = Arc<Mutex<String>>;

fn route(status: u16, link: Option<&str>, body: &str) -> http::Response<tonic::body::BoxBody> {
    let mut response = http::Response::builder().status(status);
    if let Some(link) = link {
        response = response.header("Link", link);
    }
    response
        .header("Content-Type", "application/json")
        .body(tonic::body::boxed(http_body_util::Full::new(
            hyper::body::Bytes::from(body.to_owned()),
        )))
        .expect("response")
}

async fn start_server(mode: Mode, records: Arc<Mutex<Vec<String>>>) -> (String, CancellationToken) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind scripted server");
    let addr = listener.local_addr().expect("server addr");
    let http: HttpHandler = Arc::new(move |req: http::Request<hyper::body::Incoming>| {
        let mode = Arc::clone(&mode);
        let records = Arc::clone(&records);
        Box::pin(async move {
            let target = req
                .uri()
                .path_and_query()
                .map(|path| path.as_str())
                .unwrap_or("")
                .to_owned();
            let header_or_none = |name: &str| {
                req.headers()
                    .get(name)
                    .and_then(|value| value.to_str().ok())
                    .filter(|value| !value.is_empty())
                    .unwrap_or("<none>")
                    .to_owned()
            };
            records.lock().expect("lock").push(format!(
                "(token={},sudo={},ctype={},cache={}) {} {target}",
                header_or_none("private-token"),
                header_or_none("sudo"),
                header_or_none("content-type"),
                header_or_none("cache-control"),
                req.method(),
            ));
            let _ = req.into_body().collect().await;

            // A GitLab instance mounted under a path prefix, used by the
            // repo-perms-subpath scenario (`path.Join(base.Path, "api/v4")`).
            let route_target = target.strip_prefix("/sub").unwrap_or(&target);
            if route_target.starts_with("/api/v4/projects/9/members/all") {
                if target.contains("page=2") {
                    return route(200, None, r#"[{"id":14,"access_level":10}]"#);
                }
                return route(
                    200,
                    Some(r#"<projects/9/members/all?page=2&per_page=100>; rel="next""#),
                    r#"[{"id":11,"access_level":20},{"id":12,"access_level":19},{"id":13,"access_level":50}]"#,
                );
            }
            if route_target.starts_with("/api/v4/projects/partial/members/all") {
                if target.contains("page=2") {
                    return route(500, None, r#"{"message":"boom"}"#);
                }
                return route(
                    200,
                    Some(r#"<projects/partial/members/all?page=2&per_page=100>; rel="next""#),
                    r#"[{"id":21,"access_level":30}]"#,
                );
            }

            let path = route_target.split('?').next().unwrap_or("");
            match path {
                "/api/v4/projects" => match mode.lock().expect("lock").as_str() {
                    "notfound" => route(404, None, r#"{"message":"404 Project Not Found"}"#),
                    "forbidden" => route(403, None, r#"{"message":"403 Forbidden"}"#),
                    _ => route(
                        200,
                        None,
                        r#"[{"id":1,"visibility":"private","default_branch":"main"}]"#,
                    ),
                },
                "/api/v4/users" => {
                    let username = query_value(&target, "username").unwrap_or_default();
                    match username.as_str() {
                        "alice" => route(200, None, r#"[{"id":42,"username":"alice"}]"#),
                        "dupe" => route(
                            200,
                            None,
                            r#"[{"id":42,"username":"dupe1"},{"id":43,"username":"dupe2"}]"#,
                        ),
                        "boom" => route(500, None, r#"{"message":"boom"}"#),
                        _ => route(200, None, "[]"),
                    }
                }
                _ => route(404, None, r#"{"message":"404 Not Found"}"#),
            }
        }) as sg_http::HttpFuture
    });
    let shutdown = CancellationToken::new();
    tokio::spawn(sg_http::serve_http(listener, http, shutdown.clone()));
    (format!("http://{addr}"), shutdown)
}

/// The query component of `target`, decoded like Go's `URL.Query().Get`.
fn query_value(target: &str, key: &str) -> Option<String> {
    let query = target.split_once('?')?.1;
    url::form_urlencoded::parse(query.as_bytes())
        .find(|(name, _)| name == key)
        .map(|(_, value)| value.into_owned())
}

/// `ValidateConnection`'s deadline branch: a client that returns the request
/// context's own error (`err == ctx.Err()` in the baseline).
struct DeadlineProvider;

struct DeadlineClient;

#[async_trait]
impl GitlabClient for DeadlineClient {
    async fn list_projects(
        &self,
        _url: &str,
    ) -> Result<(Vec<Project>, Option<String>), GitlabClientError> {
        Err(GitlabClientError::context_deadline())
    }

    async fn list_members(
        &self,
        _url: &str,
    ) -> Result<(Vec<Member>, Option<String>), GitlabClientError> {
        Err(GitlabClientError::new("unused"))
    }

    async fn list_users(
        &self,
        _url: &str,
    ) -> Result<(Vec<AuthUser>, Option<String>), GitlabClientError> {
        Err(GitlabClientError::new("unused"))
    }
}

impl GitlabClientProvider for DeadlineProvider {
    fn pat_client(&self, _sudo: &str) -> Arc<dyn GitlabClient> {
        Arc::new(DeadlineClient)
    }

    fn oauth_client(&self, _token: Arc<OAuthBearerToken>) -> Arc<dyn GitlabClient> {
        Arc::new(DeadlineClient)
    }
}

fn err_text(err: &str) -> &str {
    err
}

#[tokio::test(flavor = "multi_thread")]
async fn authz_gitlab_provider_matches_go_harness() {
    let mode: Mode = Arc::new(Mutex::new("ok".to_owned()));
    let records = Arc::new(Mutex::new(Vec::<String>::new()));
    let (server, shutdown) = start_server(Arc::clone(&mode), Arc::clone(&records)).await;

    let base = reqwest::Url::parse(&format!("{server}/")).expect("server URL");
    let service_id = normalize_base_url(&format!("{server}/")).expect("service id");
    let http = reqwest::Client::builder()
        .no_proxy()
        .build()
        .expect("http client");

    let new_provider = |service_id: String| {
        let client_provider: Arc<dyn GitlabClientProvider> =
            Arc::new(GitlabRestClientProvider::with_http_client(
                http.clone(),
                &base,
                Some("secret".to_owned()),
            ));
        SudoProvider::new("extsvc:gitlab:2", service_id, client_provider, true)
    };

    let normalize = |text: String| text.replace(&server, "SERVER");
    let join_requests = || {
        let records = records.lock().expect("lock");
        if records.is_empty() {
            "<none>".to_owned()
        } else {
            normalize(records.join(" >> "))
        }
    };
    let reset = || records.lock().expect("lock").clear();

    for (name, scenario_mode) in [
        ("validate-ok", "ok"),
        ("validate-not-found", "notfound"),
        ("validate-forbidden", "forbidden"),
    ] {
        *mode.lock().expect("lock") = scenario_mode.to_owned();
        reset();
        let provider = new_provider(service_id.clone());
        let error = match provider.validate_connection().await {
            Ok(()) => "none".to_owned(),
            Err(err) => normalize(err),
        };
        println!(
            "gvp scenario={name} err={} requests={}",
            err_text(&error),
            join_requests()
        );
    }

    // The deadline branch is only reachable when the client returns the
    // context's own error; the mock reproduces that (the baseline's 5s
    // `context.WithTimeout` is what makes it reachable in production).
    *mode.lock().expect("lock") = "ok".to_owned();
    reset();
    let provider = SudoProvider::new(
        "extsvc:gitlab:2",
        service_id.clone(),
        Arc::new(DeadlineProvider),
        true,
    );
    let error = match provider.validate_connection().await {
        Ok(()) => "none".to_owned(),
        Err(err) => normalize(err),
    };
    println!(
        "gvp scenario=validate-timeout err={} requests={}",
        err_text(&error),
        join_requests()
    );

    let sub_base = reqwest::Url::parse(&format!("{server}/sub/")).expect("sub base URL");
    let sub_service_id = normalize_base_url(&format!("{server}/sub/")).expect("sub service id");
    for (name, id, provider_service_id, repo_service_id, repo_base) in [
        (
            "repo-perms",
            "9",
            service_id.clone(),
            service_id.clone(),
            base.clone(),
        ),
        (
            "repo-perms-subpath",
            "9",
            sub_service_id.clone(),
            sub_service_id,
            sub_base,
        ),
        (
            "repo-perms-partial",
            "partial",
            service_id.clone(),
            service_id.clone(),
            base.clone(),
        ),
        (
            "repo-perms-host-mismatch",
            "9",
            service_id.clone(),
            "https://other.example.com/".to_owned(),
            base.clone(),
        ),
    ] {
        reset();
        let client_provider: Arc<dyn GitlabClientProvider> =
            Arc::new(GitlabRestClientProvider::with_http_client(
                http.clone(),
                &repo_base,
                Some("secret".to_owned()),
            ));
        let provider = SudoProvider::new(
            "extsvc:gitlab:2",
            provider_service_id,
            client_provider,
            true,
        );
        let repo = FetchRepo {
            uri: String::new(),
            external_repo_id: id.to_owned(),
            service_type: "gitlab".to_owned(),
            service_id: repo_service_id,
        };
        let (ids, error) = match provider
            .fetch_repo_perms(&repo, &FetchPermsOptions::default())
            .await
        {
            Ok(ids) => (join_ids(&ids), "none".to_owned()),
            Err(err) => {
                let (ids, message) = match err {
                    sg_service_worker::jobs::perms_syncer::FetchError::Failed {
                        message,
                        partial_account_ids,
                    } => (join_ids(&partial_account_ids), message),
                    other => (String::new(), format!("{other:?}")),
                };
                (ids, normalize(message))
            }
        };
        println!(
            "gvp scenario={name} ids={ids} err={} requests={}",
            err_text(&error),
            join_requests()
        );
    }

    for (name, username) in [
        ("account-one", "alice"),
        ("account-none", "nobody"),
        ("account-multiple", "dupe"),
        ("account-error", "boom"),
        ("account-encoded", "a b/c@d"),
    ] {
        reset();
        let provider = new_provider(service_id.clone());
        let user = UserRecord {
            id: 1,
            username: username.to_owned(),
            created_at: time::OffsetDateTime::now_utc(),
        };
        let (account_id, error) = match provider.fetch_account(&user).await {
            Ok(Some(spec)) => (spec.account_id, "none".to_owned()),
            Ok(None) => ("none".to_owned(), "none".to_owned()),
            Err(err) => ("none".to_owned(), normalize(err)),
        };
        println!(
            "gvp scenario={name} account={account_id} err={} requests={}",
            err_text(&error),
            join_requests()
        );
    }

    shutdown.cancel();
}

fn join_ids(ids: &[String]) -> String {
    if ids.is_empty() {
        "<empty>".to_owned()
    } else {
        ids.join(",")
    }
}
