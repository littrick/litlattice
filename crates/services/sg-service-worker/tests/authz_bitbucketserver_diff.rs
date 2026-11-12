#![forbid(unsafe_code)]

//! M9.12c differential harness (Rust side): the Bitbucket Server authz
//! providers (`Provider` for username identity and `OAuth2Provider`) against a
//! scripted Bitbucket Server API.
//!
//! Mirrors
//! `internal/authz/providers/bitbucketserver/zz_m9_12c_authz_oracle_test.go`
//! (untracked Go oracle). Both sides read
//! `test_ws/p9-go/data/authz_bitbucketserver/corpus.json` (via
//! `BBS_CORPUS`) and print one `bbs ` line per case; the server URL is
//! normalised to `SERVER`. `test_ws/p9-go/run_authz_bitbucketserver_diff.sh`
//! compares the output line for line.
//!
//! OAuth1 (`authorization.oauth`) cannot be reproduced deterministically
//! (RSA-SHA1 with a random nonce), so it is not a shared corpus case; the
//! registry-level denial is covered by the `authz_providers` differential and
//! documented in the M9.12c overview.

use std::sync::{Arc, Mutex};

use http_body_util::BodyExt;
use serde::Deserialize;
use sg_db::user_external_accounts::ExternalAccount;
use sg_http::HttpHandler;
use sg_service_worker::jobs::authz_bitbucketcloud_client::normalize_base_url;
use sg_service_worker::jobs::authz_bitbucketserver::{
    BitbucketServerOAuth2Provider, BitbucketServerProvider, TYPE_BITBUCKETSERVER,
};
use sg_service_worker::jobs::bitbucket_server_client::{
    BitbucketServerClient, BitbucketServerConnection,
};
use sg_service_worker::jobs::perms_syncer::{
    AuthzProvider, FetchError, FetchPermsOptions, FetchRepo, UserPermsError,
};
use tokio_util::sync::CancellationToken;

#[derive(Debug, Deserialize)]
struct Corpus {
    cases: Vec<Case>,
}

#[derive(Debug, Deserialize)]
struct Case {
    name: String,
    provider: String,
    op: String,
    #[serde(default)]
    script: String,
    #[serde(default = "default_page_size")]
    #[serde(rename = "pageSize")]
    page_size: i64,
    #[serde(default = "default_account_data")]
    #[serde(rename = "accountData")]
    account_data: String,
    #[serde(default = "default_auth_data")]
    #[serde(rename = "authData")]
    auth_data: String,
    #[serde(default = "default_host")]
    host: String,
    #[serde(default)]
    plugin: bool,
}

fn default_page_size() -> i64 {
    1000
}

fn default_account_data() -> String {
    "user".to_owned()
}

fn default_auth_data() -> String {
    "token".to_owned()
}

fn default_host() -> String {
    "match".to_owned()
}

fn route(status: u16, content_type: &str, body: Vec<u8>) -> http::Response<tonic::body::BoxBody> {
    http::Response::builder()
        .status(status)
        .header("Content-Type", content_type)
        .body(tonic::body::boxed(http_body_util::Full::new(
            hyper::body::Bytes::from(body),
        )))
        .expect("response")
}

fn json_route(body: &str) -> http::Response<tonic::body::BoxBody> {
    route(200, "application/json", body.as_bytes().to_vec())
}

fn query_value(target: &str, key: &str) -> Option<String> {
    let query = target.split_once('?')?.1;
    url::form_urlencoded::parse(query.as_bytes())
        .find(|(name, _)| name == key)
        .map(|(_, value)| value.into_owned())
}

fn join_ids(ids: &[String]) -> String {
    if ids.is_empty() {
        "<empty>".to_owned()
    } else {
        ids.join(",")
    }
}

fn corpus_path() -> String {
    std::env::var("BBS_CORPUS")
        .expect("BBS_CORPUS must point at data/authz_bitbucketserver/corpus.json")
}

const USER_DATA: &str = r#"{"name":"alice"}"#;
const TOKEN_DATA: &str = r#"{"access_token":"tok","token_type":"bearer","refresh_token":"refresh","expiry":"0001-01-01T00:00:00Z"}"#;

#[tokio::test(flavor = "multi_thread")]
async fn authz_bitbucketserver_matches_go_harness() {
    let raw = std::fs::read_to_string(corpus_path()).expect("read corpus");
    let corpus: Corpus = serde_json::from_str(&raw).expect("parse corpus");
    assert!(!corpus.cases.is_empty(), "corpus is empty");

    let mode: Arc<Mutex<String>> = Arc::new(Mutex::new("none".to_owned()));
    let records = Arc::new(Mutex::new(Vec::<String>::new()));
    let (server, shutdown) = start_server(Arc::clone(&mode), Arc::clone(&records)).await;
    let service_id = normalize_base_url(&reqwest::Url::parse(&format!("{server}/")).expect("URL"))
        .to_string();
    let http = reqwest::Client::builder()
        .no_proxy()
        .build()
        .expect("http client");

    for case in &corpus.cases {
        *mode.lock().expect("lock") = case.script.clone();
        records.lock().expect("lock").clear();

        let account = make_account(&case.account_data, &case.auth_data, if case.host == "match" {
            &service_id
        } else {
            "https://other.example.com/"
        });
        let repo = FetchRepo {
            uri: "bbs.example.com/repo".to_owned(),
            external_repo_id: "42".to_owned(),
            service_type: TYPE_BITBUCKETSERVER.to_owned(),
            service_id: if case.host == "match" {
                service_id.clone()
            } else {
                "https://other.example.com/".to_owned()
            },
        };

        let provider = make_provider(case, &server, &service_id, &http);
        let normalize = |text: String| text.replace(&server, "SERVER");
        let requests = || {
            let records = records.lock().expect("lock");
            if records.is_empty() {
                "<none>".to_owned()
            } else {
                normalize(records.join(" >> "))
            }
        };

        match case.op.as_str() {
            "fetch-account" => {
                let (account_text, err) = match provider.fetch_account_with_data(&user()).await {
                    Ok(Some(fetched)) => (
                        format!(
                            "{}|{}|{}|{}",
                            fetched.spec.service_type,
                            normalise_server(&fetched.spec.service_id, &server),
                            fetched.spec.account_id,
                            String::from_utf8_lossy(&fetched.account_data)
                        ),
                        "none".to_owned(),
                    ),
                    Ok(None) => ("<none>".to_owned(), "none".to_owned()),
                    Err(message) => ("<none>".to_owned(), normalize(message)),
                };
                println!(
                    "bbs case={} op=fetch-account account={} err={} requests={}",
                    case.name, account_text, err, requests()
                );
            }
            "fetch-user-perms" => {
                let (ids, err) = match provider
                    .fetch_user_perms(&account, &FetchPermsOptions::default())
                    .await
                {
                    Ok(Some(perms)) => (join_ids(&perms.exacts), "none".to_owned()),
                    Ok(None) => ("<none>".to_owned(), "none".to_owned()),
                    Err(UserPermsError::Failed { message, partial }) => (
                        partial
                            .as_ref()
                            .map(|perms| join_ids(&perms.exacts))
                            .unwrap_or_else(|| "<empty>".to_owned()),
                        normalize(message),
                    ),
                    Err(other) => ("<empty>".to_owned(), normalize(other.message().to_owned())),
                };
                println!(
                    "bbs case={} op=fetch-user-perms ids={} err={} requests={}",
                    case.name, ids, err, requests()
                );
            }
            "fetch-repo-perms" => {
                let (ids, err) = match provider
                    .fetch_repo_perms(&repo, &FetchPermsOptions::default())
                    .await
                {
                    Ok(ids) => (join_ids(&ids), "none".to_owned()),
                    Err(FetchError::Failed {
                        message,
                        partial_account_ids,
                    }) => (join_ids(&partial_account_ids), normalize(message)),
                    Err(FetchError::Unimplemented) => (
                        "<empty>".to_owned(),
                        "FetchRepoPerms is unimplemented".to_owned(),
                    ),
                    Err(FetchError::NotFound) => {
                        ("<empty>".to_owned(), "not found".to_owned())
                    }
                };
                println!(
                    "bbs case={} op=fetch-repo-perms ids={} err={} requests={}",
                    case.name, ids, err, requests()
                );
            }
            "validate" => match provider.validate_connection().await {
                Ok(()) => println!(
                    "bbs case={} op=validate err=none requests={}",
                    case.name, requests()
                ),
                Err(message) => println!(
                    "bbs case={} op=validate err={} requests={}",
                    case.name,
                    normalize(message),
                    requests()
                ),
            },
            other => panic!("unknown op {other} for case {}", case.name),
        }
    }

    shutdown.cancel();
}

fn normalise_server(value: &str, server: &str) -> String {
    value.replace(server, "SERVER")
}

fn user() -> sg_db::users::UserRecord {
    sg_db::users::UserRecord {
        id: 1,
        username: "alice".to_owned(),
        created_at: time::OffsetDateTime::UNIX_EPOCH,
    }
}

fn make_account(account_data: &str, auth_data: &str, service_id: &str) -> ExternalAccount {
    let account_data = match account_data {
        "user" => Some(USER_DATA.to_owned()),
        _ => None,
    };
    let auth_data = match auth_data {
        "token" => Some(TOKEN_DATA.to_owned()),
        _ => None,
    };
    ExternalAccount {
        id: 1,
        user_id: 1,
        service_type: TYPE_BITBUCKETSERVER.to_owned(),
        service_id: service_id.to_owned(),
        client_id: String::new(),
        account_id: "101".to_owned(),
        auth_data,
        account_data,
        encryption_key_id: String::new(),
        expired: false,
    }
}

fn make_provider(
    case: &Case,
    server: &str,
    service_id: &str,
    http: &reqwest::Client,
) -> Arc<dyn AuthzProvider> {
    let config = BitbucketServerConnection {
        url: server.to_owned(),
        username: "u".to_owned(),
        password: "p".to_owned(),
        ..Default::default()
    };
    let client = BitbucketServerClient::new(&config, http.clone()).expect("client");
    if case.provider == "oauth2" {
        Arc::new(
            BitbucketServerOAuth2Provider::new(
                "extsvc:bitbucketserver:1",
                service_id,
                client,
                case.plugin,
                None,
            )
            .with_page_size(case.page_size),
        )
    } else {
        Arc::new(
            BitbucketServerProvider::new(
                "extsvc:bitbucketserver:1",
                service_id,
                client,
                case.plugin,
            )
            .with_page_size(case.page_size),
        )
    }
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

            let path = target.split('?').next().unwrap_or("").to_owned();
            let current = mode.lock().expect("lock").clone();
            match path.as_str() {
                "/rest/api/1.0/users" => match current.as_str() {
                    "users-paged" => {
                        if query_value(&target, "start").is_none() {
                            json_route(
                                r#"{"values":[{"name":"alice-other","id":100}],"isLastPage":false,"nextPageStart":1}"#,
                            )
                        } else {
                            json_route(
                                r#"{"values":[{"name":"alice","emailAddress":"alice@example.com","id":101,"displayName":"Alice","active":true,"slug":"alice","type":"NORMAL"}],"isLastPage":true}"#,
                            )
                        }
                    }
                    "users-none" => json_route(r#"{"values":[],"isLastPage":true}"#),
                    "users-perm-paged" => {
                        if query_value(&target, "start").is_none() {
                            json_route(r#"{"values":[{"id":10}],"isLastPage":false,"nextPageStart":1}"#)
                        } else {
                            json_route(r#"{"values":[{"id":11},{"id":12}],"isLastPage":true}"#)
                        }
                    }
                    "validate-500" | "http-500" => {
                        route(500, "application/json", br#"{"errors":[]}"#.to_vec())
                    }
                    _ => route(404, "application/json", br#"{"errors":[]}"#.to_vec()),
                },
                "/rest/api/1.0/repos" => match current.as_str() {
                    "repos-paged" => {
                        if query_value(&target, "start").is_none() {
                            json_route(r#"{"values":[{"id":3},{"id":1}],"isLastPage":false,"nextPageStart":1}"#)
                        } else {
                            json_route(r#"{"values":[{"id":2}],"isLastPage":true}"#)
                        }
                    }
                    "repos-empty" => json_route(r#"{"values":[],"isLastPage":true}"#),
                    "http-401" => route(401, "application/json", br#"{"errors":[]}"#.to_vec()),
                    "http-500" => route(500, "application/json", br#"{"errors":[]}"#.to_vec()),
                    _ => route(404, "application/json", br#"{"errors":[]}"#.to_vec()),
                },
                "/rest/api/1.0/admin/permissions/users" => match current.as_str() {
                    "validate-500" => route(500, "application/json", br#"{"errors":[]}"#.to_vec()),
                    _ => json_route(
                        r#"{"values":[{"user":{"name":"u"},"permission":"SYS_ADMIN"}]}"#,
                    ),
                },
                "/rest/sourcegraph-admin/1.0/permissions/repositories" => {
                    match current.as_str() {
                        "plugin-empty" => {
                            let mut bytes = Vec::new();
                            roaring::RoaringBitmap::new()
                                .serialize_into(&mut bytes)
                                .expect("serialize bitmap");
                            route(200, "application/octet-stream", bytes)
                        }
                        _ => {
                            let bitmap =
                                roaring::RoaringBitmap::from_iter([1u32, 3, 7]);
                            let mut bytes = Vec::new();
                            bitmap
                                .serialize_into(&mut bytes)
                                .expect("serialize bitmap");
                            route(200, "application/octet-stream", bytes)
                        }
                    }
                }
                _ => route(404, "application/json", br#"{"errors":[]}"#.to_vec()),
            }
        }) as sg_http::HttpFuture
    });
    let shutdown = CancellationToken::new();
    tokio::spawn(sg_http::serve_http(listener, http, shutdown.clone()));
    (server, shutdown)
}
