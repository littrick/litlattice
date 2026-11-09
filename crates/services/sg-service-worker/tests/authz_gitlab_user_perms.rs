#![forbid(unsafe_code)]

//! M9.8be differential harness (Rust side): `SudoProvider.FetchUserPerms` /
//! `listProjects` against a scripted GitLab API, including the
//! `gitLabProjectVisibilityExperimental` feature flag and
//! `syncInternalRepoPermissions`.
//!
//! Mirrors `internal/authz/providers/gitlab/sudo_user_perms_harness_test.go`;
//! outputs are compared line for line by
//! `test_ws/p9-go/run_authz_gitlab_user_perms_diff.sh`. The server URL is
//! normalised to `SERVER` because the two harnesses bind different ports.

use std::sync::{Arc, Mutex};

use http_body_util::BodyExt;
use sg_db::user_external_accounts::ExternalAccount;
use sg_http::HttpHandler;
use sg_service_worker::jobs::authz_gitlab::{
    GitlabClientProvider, StaticGitlabFeatureFlags, SudoProvider,
};
use sg_service_worker::jobs::authz_gitlab_client::GitlabRestClientProvider;
use sg_service_worker::jobs::codehost_version_syncing::normalize_base_url;
use sg_service_worker::jobs::perms_syncer::{AuthzProvider, FetchPermsOptions, UserPermsError};
use tokio_util::sync::CancellationToken;

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

fn query_value(target: &str, key: &str) -> Option<String> {
    let query = target.split_once('?')?.1;
    url::form_urlencoded::parse(query.as_bytes())
        .find(|(name, _)| name == key)
        .map(|(_, value)| value.into_owned())
}

async fn start_server(
    mode: Arc<Mutex<String>>,
    records: Arc<Mutex<Vec<String>>>,
) -> (String, CancellationToken) {
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

            let path = target.split('?').next().unwrap_or("");
            if path != "/api/v4/projects" {
                return route(404, None, r#"{"message":"404 Not Found"}"#);
            }
            let visibility = query_value(&target, "visibility").unwrap_or_default();
            let page = query_value(&target, "page").unwrap_or_default();
            // The experimental flag drops the `min_access_level` filter and
            // lets projects without contents through the API; the provider
            // must skip them.
            let experimental = query_value(&target, "min_access_level").is_none();
            if mode.lock().expect("lock").as_str() == "partial" && page == "2" {
                return route(500, None, r#"{"message":"boom"}"#);
            }
            if page == "2" {
                return route(200, None, r#"[{"id":5,"default_branch":"main"}]"#);
            }
            if visibility == "private" && experimental {
                return route(200, None, r#"[{"id":1,"default_branch":"main"},{"id":2}]"#);
            }
            if visibility == "private" {
                return route(
                    200,
                    Some(
                        r#"<projects?min_access_level=20&per_page=100&simple=true&visibility=private&page=2>; rel="next""#,
                    ),
                    r#"[{"id":1,"default_branch":"main"},{"id":2,"default_branch":"main"}]"#,
                );
            }
            if experimental {
                return route(200, None, r#"[{"id":3,"default_branch":"main"},{"id":4}]"#);
            }
            route(
                200,
                None,
                r#"[{"id":3,"default_branch":"main"},{"id":4,"default_branch":"main"}]"#,
            )
        }) as sg_http::HttpFuture
    });
    let shutdown = CancellationToken::new();
    tokio::spawn(sg_http::serve_http(listener, http, shutdown.clone()));
    (format!("http://{addr}"), shutdown)
}

fn join_ids(ids: &[String]) -> String {
    if ids.is_empty() {
        "<empty>".to_owned()
    } else {
        ids.join(",")
    }
}

/// Run one `FetchUserPerms` and reduce it to the harness's `(ids, error)`
/// pair, preserving the partial results the provider returns with an error.
async fn fetch(provider: &SudoProvider, account: &ExternalAccount) -> (String, String) {
    match provider
        .fetch_user_perms(account, &FetchPermsOptions::default())
        .await
    {
        Ok(Some(perms)) => (join_ids(&perms.exacts), "none".to_owned()),
        Ok(None) => ("<empty>".to_owned(), "none".to_owned()),
        Err(UserPermsError::Failed { message, partial }) => (
            partial
                .as_ref()
                .map(|perms| join_ids(&perms.exacts))
                .unwrap_or_else(|| "<empty>".to_owned()),
            message,
        ),
        Err(other) => ("<empty>".to_owned(), format!("{other:?}")),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn authz_gitlab_user_perms_matches_go_harness() {
    let mode: Arc<Mutex<String>> = Arc::new(Mutex::new("ok".to_owned()));
    let records = Arc::new(Mutex::new(Vec::<String>::new()));
    let (server, shutdown) = start_server(Arc::clone(&mode), Arc::clone(&records)).await;

    let base = reqwest::Url::parse(&format!("{server}/")).expect("server URL");
    let service_id = normalize_base_url(&format!("{server}/")).expect("service id");
    let http = reqwest::Client::builder()
        .no_proxy()
        .build()
        .expect("http client");

    let make_provider = |sync_internal: bool, experimental: bool| {
        let client_provider: Arc<dyn GitlabClientProvider> =
            Arc::new(GitlabRestClientProvider::with_http_client(
                http.clone(),
                &base,
                Some("secret".to_owned()),
            ));
        SudoProvider::new(
            "extsvc:gitlab:2",
            service_id.clone(),
            client_provider,
            sync_internal,
        )
        .with_feature_flags(Arc::new(StaticGitlabFeatureFlags::new(experimental)))
    };

    let account = |service_id: &str| ExternalAccount {
        id: 1,
        user_id: 1,
        service_type: "gitlab".to_owned(),
        service_id: service_id.to_owned(),
        client_id: String::new(),
        account_id: "999".to_owned(),
        auth_data: None,
        account_data: Some(r#"{"id": 999}"#.to_owned()),
        encryption_key_id: String::new(),
        expired: false,
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

    for (name, sync_internal, experimental) in [
        ("user-perms-default", false, false),
        ("user-perms-internal", true, false),
        ("user-perms-experimental", true, true),
    ] {
        reset();
        let provider = make_provider(sync_internal, experimental);
        let (ids, error) = fetch(&provider, &account(&service_id)).await;
        println!(
            "glup scenario={name} ids={ids} err={} requests={}",
            normalize(error),
            join_requests()
        );
    }

    // The page-2 failure returns the projects fetched from page 1 alongside
    // the error.
    *mode.lock().expect("lock") = "partial".to_owned();
    reset();
    let provider_instance = make_provider(false, false);
    let (ids, error) = fetch(&provider_instance, &account(&service_id)).await;
    println!(
        "glup scenario=user-perms-partial ids={ids} err={} requests={}",
        normalize(error),
        join_requests()
    );
    *mode.lock().expect("lock") = "ok".to_owned();

    reset();
    let provider_instance = make_provider(false, false);
    let (ids, error) = fetch(&provider_instance, &account("https://other.example.com/")).await;
    println!(
        "glup scenario=user-perms-host-mismatch ids={ids} err={} requests={}",
        normalize(error),
        join_requests()
    );

    shutdown.cancel();
}
