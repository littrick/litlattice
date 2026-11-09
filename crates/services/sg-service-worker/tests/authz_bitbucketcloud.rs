#![forbid(unsafe_code)]

//! M9.8bg differential harness (Rust side): the Bitbucket Cloud authz
//! `Provider` and the REST subset it uses (`Repos` with pagination, `Repo`,
//! `ListExplicitUserPermsForRepo`) against a scripted Bitbucket Cloud API and
//! token endpoint.
//!
//! Mirrors `internal/authz/providers/bitbucketcloud/provider_harness_test.go`;
//! outputs are compared line for line by
//! `test_ws/p9-go/run_authz_bitbucketcloud_diff.sh`. The server URL is
//! normalised to `SERVER` because the two harnesses bind different ports.

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use http_body_util::BodyExt;
use sg_db::user_external_accounts::ExternalAccount;
use sg_http::HttpHandler;
use sg_service_worker::jobs::authz_bitbucketcloud::{
    BitbucketCloudOAuthProvider, BitbucketCloudProvider, TYPE_BITBUCKETCLOUD,
};
use sg_service_worker::jobs::authz_bitbucketcloud_client::{
    normalize_base_url, BitbucketCloudAuth, BitbucketCloudClient, BitbucketCloudRestClient,
};
use sg_service_worker::jobs::authz_github_user::OAuthTokenStore;
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

/// `mode` selects the scripted token endpoint / repositories endpoint
/// behaviour; `unauthorized_seen` makes the `401-once` mode fail only the
/// first request.
async fn start_server(
    mode: Arc<Mutex<String>>,
    unauthorized_seen: Arc<Mutex<bool>>,
    records: Arc<Mutex<Vec<String>>>,
) -> (String, CancellationToken) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind scripted server");
    let addr = listener.local_addr().expect("server addr");
    let server = format!("http://{addr}");
    let server_for_handler = server.clone();
    let http: HttpHandler = Arc::new(move |req: http::Request<hyper::body::Incoming>| {
        let mode = Arc::clone(&mode);
        let unauthorized_seen = Arc::clone(&unauthorized_seen);
        let records = Arc::clone(&records);
        let server = server_for_handler.clone();
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
                "(auth={},ctype={})",
                header_or_none("authorization"),
                header_or_none("content-type"),
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
            let current = mode.lock().expect("lock").clone();
            match path {
                "/site/oauth2/access_token" => {
                    if current == "refresh-fail" {
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
                "/2.0/repositories" => match current.as_str() {
                    "401-once" => {
                        let mut seen = unauthorized_seen.lock().expect("lock");
                        if !*seen {
                            *seen = true;
                            return route(401, r#"{"message":"401 Unauthorized"}"#);
                        }
                        if query_value(&target, "page").as_deref() == Some("2") {
                            route(200, r#"{"values":[{"uuid":"3"}],"pagelen":100,"page":2}"#)
                        } else {
                            route(
                                200,
                                &format!(
                                    r#"{{"values":[{{"uuid":"1"}},{{"uuid":"2"}}],"pagelen":100,"page":1,"next":"{server}/2.0/repositories?page=2&pagelen=100"}}"#
                                ),
                            )
                        }
                    }
                    "refresh-fail" => route(401, r#"{"message":"401 Unauthorized"}"#),
                    "api-error" => route(500, r#"{"type":"error"}"#),
                    _ => {
                        if query_value(&target, "page").as_deref() == Some("2") {
                            route(200, r#"{"values":[{"uuid":"3"}],"pagelen":100,"page":2}"#)
                        } else {
                            route(
                                200,
                                &format!(
                                    r#"{{"values":[{{"uuid":"1"}},{{"uuid":"2"}}],"pagelen":100,"page":1,"next":"{server}/2.0/repositories?page=2&pagelen=100"}}"#
                                ),
                            )
                        }
                    }
                },
                "/2.0/repositories/user/repo/permissions-config/users" => {
                    if current == "users-404" {
                        return route(404, r#"{"error":"not found"}"#);
                    }
                    route(
                        200,
                        r#"{"values":[{"user":{"uuid":"1"}},{"user":{"uuid":"2"}}],"pagelen":100,"page":1}"#,
                    )
                }
                "/2.0/repositories/user/repo" => route(
                    200,
                    r#"{"uuid":"repo-uuid","full_name":"user/repo","owner":{"uuid":"4"}}"#,
                ),
                _ => route(404, r#"{"error":"not found"}"#),
            }
        }) as sg_http::HttpFuture
    });
    let shutdown = CancellationToken::new();
    tokio::spawn(sg_http::serve_http(listener, http, shutdown.clone()));
    (server, shutdown)
}

fn join_ids(ids: &[String]) -> String {
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
        service_type: TYPE_BITBUCKETCLOUD.to_owned(),
        service_id: service_id.to_owned(),
        client_id: String::new(),
        account_id: "999".to_owned(),
        auth_data: auth_data.map(str::to_owned),
        account_data: with_data.then(|| r#"{"uuid":"999"}"#.to_owned()),
        encryption_key_id: String::new(),
        expired: false,
    }
}

const FRESH_TOKEN: &str = r#"{"access_token":"initial-token","token_type":"bearer","refresh_token":"refresh-token","expiry":"0001-01-01T00:00:00Z"}"#;
const EXPIRED_TOKEN: &str = r#"{"access_token":"initial-token","token_type":"bearer","refresh_token":"refresh-token","expiry":"2020-01-01T00:00:00Z"}"#;

#[tokio::test(flavor = "multi_thread")]
async fn authz_bitbucketcloud_matches_go_harness() {
    let mode: Arc<Mutex<String>> = Arc::new(Mutex::new("ok".to_owned()));
    let unauthorized_seen = Arc::new(Mutex::new(false));
    let records = Arc::new(Mutex::new(Vec::<String>::new()));
    let (server, shutdown) = start_server(
        Arc::clone(&mode),
        Arc::clone(&unauthorized_seen),
        Arc::clone(&records),
    )
    .await;

    let service_id = normalize_base_url(&reqwest::Url::parse(&format!("{server}/")).expect("URL"));
    let service_id = service_id.as_str().to_owned();
    let http = reqwest::Client::builder()
        .no_proxy()
        .build()
        .expect("http client");

    let oauth_providers = vec![BitbucketCloudOAuthProvider {
        url: server.clone(),
        client_key: "client-key".to_owned(),
        client_secret: "client-secret".to_owned(),
    }];

    // The MockStore is created per provider so each scenario starts from its
    // own account row.
    let make_provider = || -> (BitbucketCloudProvider, Arc<MockStore>) {
        let client: Arc<dyn BitbucketCloudClient> = Arc::new(
            BitbucketCloudRestClient::with_http_client(
                http.clone(),
                Some(&server),
                BitbucketCloudAuth::Basic {
                    username: "user".to_owned(),
                    password: "pass".to_owned(),
                },
            )
            .expect("client"),
        );
        let store = Arc::new(MockStore {
            account: Mutex::new(None),
        });
        let provider = BitbucketCloudProvider::new(
            "extsvc:bitbucketcloud:1",
            service_id.clone(),
            client,
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

    for (name, auth) in [
        ("user-perms", FRESH_TOKEN),
        ("user-perms-preemptive-refresh", EXPIRED_TOKEN),
    ] {
        reset();
        let (provider, store) = make_provider();
        *store.account.lock().expect("lock") = Some(account(&service_id, Some(auth), true));
        run_user(
            name,
            &provider,
            &account(&service_id, Some(auth), true),
            &records,
            &server,
        )
        .await;
    }

    // A 401 triggers one refresh-and-retry.
    set_mode("401-once");
    reset();
    let (provider, store) = make_provider();
    *store.account.lock().expect("lock") = Some(account(&service_id, Some(FRESH_TOKEN), true));
    run_user(
        "user-perms-401-retry",
        &provider,
        &account(&service_id, Some(FRESH_TOKEN), true),
        &records,
        &server,
    )
    .await;
    set_mode("ok");

    // A failed refresh keeps the original 401 response.
    set_mode("refresh-fail");
    reset();
    let (provider, store) = make_provider();
    *store.account.lock().expect("lock") = Some(account(&service_id, Some(FRESH_TOKEN), true));
    run_user(
        "user-perms-401-refresh-fail",
        &provider,
        &account(&service_id, Some(FRESH_TOKEN), true),
        &records,
        &server,
    )
    .await;
    set_mode("ok");

    // A non-2xx response becomes `httpError`.
    set_mode("api-error");
    reset();
    let (provider, store) = make_provider();
    *store.account.lock().expect("lock") = Some(account(&service_id, Some(FRESH_TOKEN), true));
    run_user(
        "user-perms-api-error",
        &provider,
        &account(&service_id, Some(FRESH_TOKEN), true),
        &records,
        &server,
    )
    .await;
    set_mode("ok");

    // No `account_data` at all.
    reset();
    let (provider, _store) = make_provider();
    run_user(
        "user-perms-no-data",
        &provider,
        &account(&service_id, Some(FRESH_TOKEN), false),
        &records,
        &server,
    )
    .await;

    // Account from a different code host.
    reset();
    let (provider, _store) = make_provider();
    run_user(
        "user-perms-host-mismatch",
        &provider,
        &account("https://other.example.com/", Some(FRESH_TOKEN), true),
        &records,
        &server,
    )
    .await;

    // `FetchRepoPerms`: explicit users plus the repository owner, fetched with
    // the connection's basic credentials.
    reset();
    let (provider, _store) = make_provider();
    let repo = FetchRepo {
        uri: "bitbucket.org/user/repo".to_owned(),
        external_repo_id: String::new(),
        service_type: String::new(),
        service_id: String::new(),
    };
    let (ids, error) = fetch_repo(&provider, &repo).await;
    println!(
        "gbc scenario=repo-perms ids={ids} err={} requests={}",
        normalize(error),
        join_requests()
    );

    // A 404 on the permissions endpoint is surfaced verbatim (the syncer's
    // `github.APIError` special case does not apply).
    reset();
    let (provider, _store) = make_provider();
    set_mode("users-404");
    let (ids, error) = fetch_repo(&provider, &repo).await;
    println!(
        "gbc scenario=repo-perms-404 ids={ids} err={} requests={}",
        normalize(error),
        join_requests()
    );
    set_mode("ok");

    shutdown.cancel();
}

/// Run one `FetchUserPerms` scenario and print the harness line.
async fn run_user(
    name: &str,
    provider: &BitbucketCloudProvider,
    account: &ExternalAccount,
    records: &Arc<Mutex<Vec<String>>>,
    server: &str,
) {
    let (ids, error) = fetch(provider, account).await;
    let requests = {
        let records = records.lock().expect("lock");
        if records.is_empty() {
            "<none>".to_owned()
        } else {
            records.join(" >> ")
        }
    };
    println!(
        "gbc scenario={name} ids={ids} err={} requests={}",
        error.replace(server, "SERVER"),
        requests.replace(server, "SERVER")
    );
}

/// Run one `FetchUserPerms` and reduce it to the harness's `(ids, error)` pair,
/// preserving the partial results the provider returns with an error.
async fn fetch(provider: &BitbucketCloudProvider, account: &ExternalAccount) -> (String, String) {
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
        Err(other) => ("<empty>".to_owned(), other.message().to_owned()),
    }
}

async fn fetch_repo(provider: &BitbucketCloudProvider, repo: &FetchRepo) -> (String, String) {
    match provider
        .fetch_repo_perms(repo, &FetchPermsOptions::default())
        .await
    {
        Ok(ids) => (join_ids(&ids), "none".to_owned()),
        Err(FetchError::Failed {
            message,
            partial_account_ids,
        }) => (join_ids(&partial_account_ids), message),
        Err(other) => ("<empty>".to_owned(), format!("{other:?}")),
    }
}
