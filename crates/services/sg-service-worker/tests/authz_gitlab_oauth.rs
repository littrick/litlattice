#![forbid(unsafe_code)]

//! M9.8bf differential harness (Rust side): the GitLab `OAuthProvider`
//! (`oauth.go`) against a scripted GitLab API and token endpoint, including the
//! pre-emptive refresh, the 401 refresh-and-retry, a failed refresh and the
//! unimplemented `FetchRepoPerms`.
//!
//! Mirrors `internal/authz/providers/gitlab/oauth_harness_test.go`; outputs
//! are compared line for line by
//! `test_ws/p9-go/run_authz_gitlab_oauth_diff.sh`. The server URL is
//! normalised to `SERVER` because the two harnesses bind different ports.

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use http_body_util::BodyExt;
use sg_db::user_external_accounts::ExternalAccount;
use sg_http::HttpHandler;
use sg_service_worker::jobs::authz_github_user::OAuthTokenStore;
use sg_service_worker::jobs::authz_gitlab::{
    GitlabClientProvider, GitlabOAuthProvider, OAuthProvider,
};
use sg_service_worker::jobs::authz_gitlab_client::GitlabRestClientProvider;
use sg_service_worker::jobs::codehost_version_syncing::normalize_base_url;
use sg_service_worker::jobs::perms_syncer::{
    AuthzProvider, FetchError, FetchPermsOptions, FetchRepo, UserPermsError,
};
use tokio_util::sync::CancellationToken;

/// `dbmocks.MockUserExternalAccountsStore`: one mutable account row.
struct MockStore {
    account: Mutex<Option<ExternalAccount>>,
}

#[async_trait]
impl OAuthTokenStore for MockStore {
    async fn get(&self, _id: i32) -> Result<Option<ExternalAccount>, String> {
        Ok(self.account.lock().expect("lock").clone())
    }

    async fn update(&self, account: &ExternalAccount, auth_data: &str) -> Result<(), String> {
        let mut updated = account.clone();
        updated.auth_data = Some(auth_data.to_owned());
        *self.account.lock().expect("lock") = Some(updated);
        Ok(())
    }
}

fn route(status: u16, body: &str) -> http::Response<tonic::body::BoxBody> {
    http::Response::builder()
        .status(status)
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

/// `mode` selects the scripted token endpoint / projects endpoint behaviour;
/// `unauthorized_seen` makes the `401-once` mode fail only the first request.
async fn start_server(
    mode: Arc<Mutex<String>>,
    unauthorized_seen: Arc<Mutex<bool>>,
    records: Arc<Mutex<Vec<String>>>,
) -> (String, CancellationToken) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind scripted server");
    let addr = listener.local_addr().expect("server addr");
    let http: HttpHandler = Arc::new(move |req: http::Request<hyper::body::Incoming>| {
        let mode = Arc::clone(&mode);
        let unauthorized_seen = Arc::clone(&unauthorized_seen);
        let records = Arc::clone(&records);
        Box::pin(async move {
            let target = req
                .uri()
                .path_and_query()
                .map(|path| path.as_str())
                .unwrap_or("")
                .to_owned();
            let method = req.method().clone();
            let header_or_none = |name: &str| {
                req.headers()
                    .get(name)
                    .and_then(|value| value.to_str().ok())
                    .filter(|value| !value.is_empty())
                    .unwrap_or("<none>")
                    .to_owned()
            };
            let headers = format!(
                "(auth={},private-token={},sudo={},ctype={},cache={})",
                header_or_none("authorization"),
                header_or_none("private-token"),
                header_or_none("sudo"),
                header_or_none("content-type"),
                header_or_none("cache-control"),
            );
            let body = req
                .into_body()
                .collect()
                .await
                .map(|collected| collected.to_bytes())
                .unwrap_or_default();
            let mut record = format!("{headers} {method} {target}");
            if method == http::Method::POST {
                record.push_str(&format!(" body={}", String::from_utf8_lossy(&body)));
            }
            records.lock().expect("lock").push(record);

            let path = target.split('?').next().unwrap_or("");
            match path {
                "/oauth/token" => {
                    if mode.lock().expect("lock").as_str() == "refresh-fail" {
                        return route(
                            400,
                            r#"{"error":"invalid_grant","error_description":"bad refresh"}"#,
                        );
                    }
                    route(
                        200,
                        r#"{"access_token":"fresh-token","token_type":"bearer","refresh_token":"fresh-refresh","expires_in":3600}"#,
                    )
                }
                "/api/v4/projects" => {
                    let current = mode.lock().expect("lock").clone();
                    match current.as_str() {
                        "401-once" => {
                            let mut seen = unauthorized_seen.lock().expect("lock");
                            if !*seen {
                                *seen = true;
                                return route(401, r#"{"message":"401 Unauthorized"}"#);
                            }
                            route(200, r#"[{"id":7,"default_branch":"main"}]"#)
                        }
                        "refresh-fail" => route(401, r#"{"message":"401 Unauthorized"}"#),
                        _ => {
                            if query_value(&target, "visibility").as_deref() == Some("internal") {
                                route(
                                    200,
                                    r#"[{"id":3,"default_branch":"main"},{"id":4,"default_branch":"main"}]"#,
                                )
                            } else {
                                route(
                                    200,
                                    r#"[{"id":1,"default_branch":"main"},{"id":2,"default_branch":"main"}]"#,
                                )
                            }
                        }
                    }
                }
                _ => route(404, r#"{"message":"404 Not Found"}"#),
            }
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

fn account(service_id: &str, auth_data: Option<&str>) -> ExternalAccount {
    ExternalAccount {
        id: 1,
        user_id: 1,
        service_type: "gitlab".to_owned(),
        service_id: service_id.to_owned(),
        client_id: String::new(),
        account_id: "999".to_owned(),
        auth_data: auth_data.map(str::to_owned),
        account_data: Some(r#"{"id": 999}"#.to_owned()),
        encryption_key_id: String::new(),
        expired: false,
    }
}

const FRESH_TOKEN: &str = r#"{"access_token":"initial-token","token_type":"bearer","refresh_token":"refresh-token","expiry":"0001-01-01T00:00:00Z"}"#;
const EXPIRED_TOKEN: &str = r#"{"access_token":"initial-token","token_type":"bearer","refresh_token":"refresh-token","expiry":"2020-01-01T00:00:00Z"}"#;

#[tokio::test(flavor = "multi_thread")]
async fn authz_gitlab_oauth_matches_go_harness() {
    let mode: Arc<Mutex<String>> = Arc::new(Mutex::new("ok".to_owned()));
    let unauthorized_seen = Arc::new(Mutex::new(false));
    let records = Arc::new(Mutex::new(Vec::<String>::new()));
    let (server, shutdown) = start_server(
        Arc::clone(&mode),
        Arc::clone(&unauthorized_seen),
        Arc::clone(&records),
    )
    .await;

    let base = reqwest::Url::parse(&format!("{server}/")).expect("server URL");
    let service_id = normalize_base_url(&format!("{server}/")).expect("service id");
    let http = reqwest::Client::builder()
        .no_proxy()
        .build()
        .expect("http client");

    let oauth_providers = vec![GitlabOAuthProvider {
        url: server.clone(),
        client_id: "client-id".to_owned(),
        client_secret: "client-secret".to_owned(),
        sync_internal_repo_permissions: None,
    }];

    // The MockStore is created per provider so each scenario starts from its
    // own account row.
    let make_provider_with_store = |sync_internal: bool| -> (OAuthProvider, Arc<MockStore>) {
        let client_provider: Arc<dyn GitlabClientProvider> =
            Arc::new(GitlabRestClientProvider::with_http_client(
                http.clone(),
                &base,
                Some("secret".to_owned()),
            ));
        let store = Arc::new(MockStore {
            account: Mutex::new(None),
        });
        let provider = OAuthProvider::new(
            "extsvc:gitlab:2",
            service_id.clone(),
            client_provider,
            sync_internal,
            store.clone(),
            http.clone(),
            oauth_providers.clone(),
        );
        (provider, store)
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
        *unauthorized_seen.lock().expect("lock") = false;
    };
    let set_mode = |next: &str| {
        *mode.lock().expect("lock") = next.to_owned();
    };

    for (name, sync_internal, auth, mode_name) in [
        ("oauth-list", false, FRESH_TOKEN, "ok"),
        ("oauth-internal", true, FRESH_TOKEN, "ok"),
        ("oauth-preemptive-refresh", false, EXPIRED_TOKEN, "ok"),
        ("oauth-401-retry", false, FRESH_TOKEN, "401-once"),
        ("oauth-refresh-fail", false, FRESH_TOKEN, "refresh-fail"),
    ] {
        set_mode(mode_name);
        reset();
        let (provider, store) = make_provider_with_store(sync_internal);
        *store.account.lock().expect("lock") = Some(account(&service_id, Some(auth)));
        let acct = account(&service_id, Some(auth));
        let (ids, error) = fetch(&provider, &acct).await;
        println!(
            "glo scenario={name} ids={ids} err={} requests={}",
            normalize(error),
            join_requests()
        );
    }
    set_mode("ok");

    // No stored token.
    reset();
    let (provider, _store) = make_provider_with_store(false);
    let (ids, error) = fetch(&provider, &account(&service_id, None)).await;
    println!(
        "glo scenario=oauth-no-token ids={ids} err={} requests={}",
        normalize(error),
        join_requests()
    );

    // Account from a different code host.
    reset();
    let (provider, _store) = make_provider_with_store(false);
    let (ids, error) = fetch(
        &provider,
        &account("https://other.example.com/", Some(FRESH_TOKEN)),
    )
    .await;
    println!(
        "glo scenario=oauth-host-mismatch ids={ids} err={} requests={}",
        normalize(error),
        join_requests()
    );

    // `FetchRepoPerms` is unimplemented.
    reset();
    let (provider, _store) = make_provider_with_store(false);
    let repo = FetchRepo {
        uri: String::new(),
        external_repo_id: String::new(),
        service_type: String::new(),
        service_id: String::new(),
    };
    let error = match provider
        .fetch_repo_perms(&repo, &FetchPermsOptions::default())
        .await
    {
        Err(FetchError::Unimplemented) => "unimplemented".to_owned(),
        Err(other) => format!("{other:?}"),
        Ok(_) => "none".to_owned(),
    };
    println!(
        "glo scenario=oauth-fetch-repo-perms ids=<empty> err={error} requests={}",
        join_requests()
    );

    shutdown.cancel();
}

/// Run one `FetchUserPerms` and reduce it to the harness's `(ids, error)` pair,
/// preserving the partial results the provider returns with an error.
async fn fetch(provider: &OAuthProvider, account: &ExternalAccount) -> (String, String) {
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
