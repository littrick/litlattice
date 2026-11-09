#![forbid(unsafe_code)]

//! M9.8bh differential harness (Rust side): the Gerrit authz `Provider` and
//! the REST subset it uses (`ListProjects` over `a/projects/?S=&n=` with the
//! `)]}'` prefix, `GetGroup`) against a scripted Gerrit API.
//!
//! Mirrors `internal/authz/providers/gerrit/provider_harness_test.go`;
//! outputs are compared line for line by
//! `test_ws/p9-go/run_authz_gerrit_diff.sh`. The server URL is normalised to
//! `SERVER` because the two harnesses bind different ports, and project IDs
//! are sorted because the baseline collects them from a Go map.

use std::sync::{Arc, Mutex};

use sg_db::user_external_accounts::ExternalAccount;
use sg_http::HttpHandler;
use sg_service_worker::jobs::authz_gerrit::{GerritProvider, TYPE_GERRIT};
use sg_service_worker::jobs::authz_gerrit_client::GerritRestClient;
use sg_service_worker::jobs::perms_syncer::{
    AuthzProvider, FetchError, FetchPermsOptions, FetchRepo, UserPermsError,
};
use tokio_util::sync::CancellationToken;

fn route(status: u16, body: &str) -> http::Response<tonic::body::BoxBody> {
    http::Response::builder()
        .status(status)
        .header("Content-Type", "application/json")
        .body(tonic::body::boxed(http_body_util::Full::new(
            hyper::body::Bytes::from(body.to_owned()),
        )))
        .expect("response")
}

/// The prefixed JSON body Gerrit returns.
fn gerrit_json(body: &str) -> String {
    format!(")]}}'\n{body}")
}

fn project_page(start: usize, count: usize) -> String {
    let mut body = String::from("{");
    for i in 0..count {
        if i > 0 {
            body.push(',');
        }
        body.push_str(&format!(
            r#""project-{:03}":{{"id":"id-{:03}","name":"project-{:03}"}}"#,
            start + i,
            start + i,
            start + i
        ));
    }
    body.push('}');
    gerrit_json(&body)
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
    let server = format!("http://{addr}");
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
            let method = req.method().clone();
            let auth = req
                .headers()
                .get("authorization")
                .and_then(|value| value.to_str().ok())
                .filter(|value| !value.is_empty())
                .unwrap_or("<none>")
                .to_owned();
            records
                .lock()
                .expect("lock")
                .push(format!("(auth={auth}) {method} {target}"));

            let path = target.split('?').next().unwrap_or("");
            let current = mode.lock().expect("lock").clone();
            match path {
                "/a/projects/" => match current.as_str() {
                    "401" => route(401, &gerrit_json(r#"{"error":"unauthorized"}"#)),
                    "api-error" => route(500, &gerrit_json(r#"{"error":"boom"}"#)),
                    _ => {
                        // `listAllProjects`: `S` is the skip offset.
                        if query_value(&target, "S").as_deref() == Some("100") {
                            route(200, &project_page(100, 2))
                        } else {
                            route(200, &project_page(0, 100))
                        }
                    }
                },
                "/a/groups/Administrators" => match current.as_str() {
                    "group-404" => route(404, &gerrit_json(r#"{"error":"not found"}"#)),
                    "group-invalid" => {
                        route(200, &gerrit_json(r#"{"id":"g1","name":"Administrators"}"#))
                    }
                    _ => route(
                        200,
                        &gerrit_json(
                            r#"{"id":"g1","name":"Administrators","created_on":"2020-01-01 00:00:00.000000000"}"#,
                        ),
                    ),
                },
                _ => route(404, &gerrit_json(r#"{"error":"not found"}"#)),
            }
        }) as sg_http::HttpFuture
    });
    let shutdown = CancellationToken::new();
    tokio::spawn(sg_http::serve_http(listener, http, shutdown.clone()));
    (server, shutdown)
}

fn join_ids(ids: &[String]) -> String {
    let mut ids = ids.to_vec();
    ids.sort();
    if ids.is_empty() {
        "<empty>".to_owned()
    } else {
        ids.join(",")
    }
}

fn account(service_id: &str, auth_data: Option<&str>, with_data: bool) -> ExternalAccount {
    ExternalAccount {
        id: 1,
        user_id: 1,
        service_type: TYPE_GERRIT.to_owned(),
        service_id: service_id.to_owned(),
        client_id: String::new(),
        account_id: "999".to_owned(),
        auth_data: auth_data.map(str::to_owned),
        account_data: with_data.then(|| r#"{"name":"User"}"#.to_owned()),
        encryption_key_id: String::new(),
        expired: false,
    }
}

const CREDS: &str = r#"{"username":"user","password":"pass"}"#;

#[tokio::test(flavor = "multi_thread")]
async fn authz_gerrit_matches_go_harness() {
    let mode: Arc<Mutex<String>> = Arc::new(Mutex::new("ok".to_owned()));
    let records = Arc::new(Mutex::new(Vec::<String>::new()));
    let (server, shutdown) = start_server(Arc::clone(&mode), Arc::clone(&records)).await;

    let service_id = format!("{server}/");
    let http = reqwest::Client::builder()
        .no_proxy()
        .build()
        .expect("http client");

    let provider = || -> GerritProvider {
        // `NewProvider` normalises the parsed URL before requests are made.
        let client = GerritRestClient::with_http_client(
            http.clone(),
            reqwest::Url::parse(&service_id).expect("URL"),
            "admin",
            "secret",
        );
        GerritProvider::new("extsvc:gerrit:1", service_id.clone(), Arc::new(client))
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
    let reset = || {
        records.lock().expect("lock").clear();
    };
    let set_mode = |next: &str| {
        *mode.lock().expect("lock") = next.to_owned();
    };

    run_user(
        "user-perms",
        &provider(),
        &account(&service_id, Some(CREDS), true),
        &records,
        &server,
    )
    .await;

    set_mode("401");
    run_user(
        "user-perms-401",
        &provider(),
        &account(&service_id, Some(CREDS), true),
        &records,
        &server,
    )
    .await;
    set_mode("ok");

    set_mode("api-error");
    run_user(
        "user-perms-api-error",
        &provider(),
        &account(&service_id, Some(CREDS), true),
        &records,
        &server,
    )
    .await;
    set_mode("ok");

    run_user(
        "user-perms-no-data",
        &provider(),
        &account(&service_id, Some(CREDS), false),
        &records,
        &server,
    )
    .await;

    run_user(
        "user-perms-no-auth-data",
        &provider(),
        &account(&service_id, None, true),
        &records,
        &server,
    )
    .await;

    run_user(
        "user-perms-null-auth",
        &provider(),
        &account(&service_id, Some("null"), true),
        &records,
        &server,
    )
    .await;

    run_user(
        "user-perms-host-mismatch",
        &provider(),
        &account("https://other.example.com/", Some(CREDS), true),
        &records,
        &server,
    )
    .await;

    // `FetchRepoPerms` is unimplemented.
    reset();
    let repo = FetchRepo {
        uri: "gerrit.example.com/project-000".to_owned(),
        external_repo_id: String::new(),
        service_type: String::new(),
        service_id: String::new(),
    };
    let (ids, error, kind) = fetch_repo(&provider(), &repo).await;
    println!(
        "ger scenario=repo-perms ids={ids} err={} kind={kind} requests={}",
        normalize(error),
        join_requests()
    );

    // `ValidateConnection` uses the connection credentials (admin/secret).
    run_validate("validate-ok", &provider(), &records, &server).await;

    set_mode("group-404");
    run_validate("validate-group-404", &provider(), &records, &server).await;
    set_mode("ok");

    set_mode("group-invalid");
    run_validate("validate-group-invalid", &provider(), &records, &server).await;
    set_mode("ok");

    shutdown.cancel();
}

/// Run one `FetchUserPerms` and print the harness line.
async fn run_user(
    name: &str,
    provider: &GerritProvider,
    account: &ExternalAccount,
    records: &Arc<Mutex<Vec<String>>>,
    server: &str,
) {
    records.lock().expect("lock").clear();
    let (ids, error, kind) = match provider
        .fetch_user_perms(account, &FetchPermsOptions::default())
        .await
    {
        Ok(Some(perms)) => (join_ids(&perms.exacts), "none".to_owned(), "none"),
        Ok(None) => ("<empty>".to_owned(), "none".to_owned(), "none"),
        Err(UserPermsError::Unauthorized { message }) => {
            ("<empty>".to_owned(), message, "unauthorized")
        }
        Err(err) => ("<empty>".to_owned(), err.message().to_owned(), "failed"),
    };
    let requests = {
        let records = records.lock().expect("lock");
        if records.is_empty() {
            "<none>".to_owned()
        } else {
            records.join(" >> ")
        }
    };
    println!(
        "ger scenario={name} ids={ids} err={} kind={kind} requests={}",
        error.replace(server, "SERVER"),
        requests.replace(server, "SERVER")
    );
}

/// Run one `ValidateConnection` and print the harness line.
async fn run_validate(
    name: &str,
    provider: &GerritProvider,
    records: &Arc<Mutex<Vec<String>>>,
    server: &str,
) {
    records.lock().expect("lock").clear();
    let error = match provider.validate_connection().await {
        Ok(()) => "none".to_owned(),
        Err(err) => err,
    };
    let requests = {
        let records = records.lock().expect("lock");
        if records.is_empty() {
            "<none>".to_owned()
        } else {
            records.join(" >> ")
        }
    };
    println!(
        "ger scenario={name} err={} requests={}",
        error.replace(server, "SERVER"),
        requests.replace(server, "SERVER")
    );
}

async fn fetch_repo(provider: &GerritProvider, repo: &FetchRepo) -> (String, String, &'static str) {
    match provider
        .fetch_repo_perms(repo, &FetchPermsOptions::default())
        .await
    {
        Ok(ids) => (join_ids(&ids), "none".to_owned(), "none"),
        Err(FetchError::Unimplemented) => (
            "<empty>".to_owned(),
            "gerrit.FetchRepoPerms is unimplemented".to_owned(),
            "unimplemented",
        ),
        Err(FetchError::NotFound) => ("<empty>".to_owned(), "not found".to_owned(), "not-found"),
        Err(FetchError::Failed { message, .. }) => ("<empty>".to_owned(), message, "failed"),
    }
}
