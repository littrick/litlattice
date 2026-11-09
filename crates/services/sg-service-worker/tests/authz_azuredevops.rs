#![forbid(unsafe_code)]

//! M9.8bi differential harness (Rust side): the Azure DevOps authz `Provider`
//! and the `internal/extsvc/azuredevops` REST subset it uses (`_apis/accounts`,
//! `_apis/profile/profiles/me`, `{org}/_apis/git/repositories`, all with
//! `api-version=7.0`).
//!
//! Mirrors `internal/authz/providers/azuredevops/provider_harness_test.go`;
//! outputs are compared line for line by
//! `test_ws/p9-go/run_authz_azuredevops_diff.sh`. The server URL is normalised
//! to `SERVER` because the two harnesses bind different ports, newlines inside
//! error values are escaped, and the JSON decoder's error text is masked
//! (Go `encoding/json` vs `serde_json`, docs §73.3).
//!
//! The wiring-level cases (`enforcePermissions` filter, ACLs license gate) are
//! unit-tested inside `authz_providers`; this harness always builds the
//! provider from already-filtered connections, like `NewAuthzProviders` does.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use sg_db::user_external_accounts::ExternalAccount;
use sg_http::HttpHandler;
use sg_service_worker::jobs::authz_azuredevops::{
    AzureDevOpsConnection, AzureDevOpsProvider, AzureDevOpsProviderOptions,
};
use sg_service_worker::jobs::authz_azuredevops_client::AzureDevOpsOAuthProvider;
use sg_service_worker::jobs::authz_github_user::OAuthTokenStore;
use sg_service_worker::jobs::perms_syncer::{AuthzProvider, FetchPermsOptions, UserPermsError};
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

const PROFILE_JSON: &str = concat!(
    r#"{"id":"1","displayName":"Harness User","emailAddress":"harness@example.com","#,
    r#""timestamp":"0001-01-01T00:00:00Z","publicAlias":"alias-1"}"#
);
const TOKEN_JSON: &str = r#"{"access_token":"at-1","refresh_token":""}"#;

fn orgs_json(orgs: &[(&'static str, &'static str)]) -> String {
    let mut value = String::from("{\"count\":");
    value.push_str(&orgs.len().to_string());
    value.push_str(",\"value\":[");
    for (index, (id, name)) in orgs.iter().enumerate() {
        if index > 0 {
            value.push(',');
        }
        value.push_str(&format!(
            r#"{{"accountId":"{id}","accountUri":"","accountName":"{name}"}}"#
        ));
    }
    value.push_str("]}");
    value
}

fn repos_json(repos: &[&'static str]) -> String {
    let mut value = String::from("{\"value\":[");
    for (index, id) in repos.iter().enumerate() {
        if index > 0 {
            value.push(',');
        }
        value.push_str(&format!(r#"{{"id":"{id}"}}"#));
    }
    value.push_str("],\"count\":");
    value.push_str(&repos.len().to_string());
    value.push('}');
    value
}

#[derive(Clone)]
struct Scenario {
    name: &'static str,
    conns: Vec<AzureDevOpsConnection>,
    account: Option<ExternalAccount>,
    /// `auth.providers` contains an Azure DevOps entry.
    oauth: bool,
    orgs: Vec<(&'static str, &'static str)>,
    repos: BTreeMap<&'static str, Vec<&'static str>>,
    validate: bool,
}

fn conn(orgs: &[&str], projects: &[&str], token: &str) -> AzureDevOpsConnection {
    AzureDevOpsConnection {
        urn: "urn:1".to_owned(),
        username: String::new(),
        token: token.to_owned(),
        orgs: orgs.iter().map(|value| (*value).to_owned()).collect(),
        projects: projects.iter().map(|value| (*value).to_owned()).collect(),
    }
}

fn account(account_data: Option<&str>, auth_data: &str) -> ExternalAccount {
    ExternalAccount {
        id: 7,
        user_id: 1,
        service_type: "azuredevops".to_owned(),
        service_id: "https://dev.azure.com/".to_owned(),
        client_id: String::new(),
        account_id: "1".to_owned(),
        auth_data: Some(auth_data.to_owned()),
        account_data: account_data.map(str::to_owned),
        encryption_key_id: String::new(),
        expired: false,
    }
}

fn solar() -> Vec<(&'static str, &'static str)> {
    vec![
        ("id-solarsystem", "solarsystem"),
        ("id-solar", "solar"),
        ("id-unconfigured", "unconfigured"),
    ]
}

fn solar_repos() -> BTreeMap<&'static str, Vec<&'static str>> {
    let mut repos = BTreeMap::new();
    repos.insert("solarsystem", vec!["1", "2"]);
    repos.insert("solar", vec!["3"]);
    repos
}

fn scenarios() -> Vec<Scenario> {
    vec![
        Scenario {
            name: "ok",
            conns: vec![conn(&["solarsystem"], &["solar/system"], "")],
            account: Some(account(Some(PROFILE_JSON), TOKEN_JSON)),
            oauth: true,
            orgs: solar(),
            repos: solar_repos(),
            validate: false,
        },
        Scenario {
            name: "no-profile",
            conns: vec![conn(&["solarsystem", "solar"], &[], "")],
            account: Some(account(None, TOKEN_JSON)),
            oauth: true,
            orgs: solar(),
            repos: solar_repos(),
            validate: false,
        },
        Scenario {
            name: "no-auth-provider",
            conns: vec![conn(&["solarsystem"], &[], "")],
            account: Some(account(Some(PROFILE_JSON), TOKEN_JSON)),
            oauth: false,
            orgs: Vec::new(),
            repos: BTreeMap::new(),
            validate: false,
        },
        Scenario {
            name: "malformed-auth-data",
            conns: vec![conn(&["solarsystem"], &[], "")],
            account: Some(account(Some(PROFILE_JSON), r#"{"access_token":}"#)),
            oauth: true,
            orgs: Vec::new(),
            repos: BTreeMap::new(),
            validate: false,
        },
        Scenario {
            name: "org-4xx",
            conns: vec![conn(
                &[
                    "solarsystem",
                    "simulate-401",
                    "simulate-403",
                    "simulate-404",
                ],
                &[],
                "",
            )],
            account: Some(account(Some(PROFILE_JSON), TOKEN_JSON)),
            oauth: true,
            orgs: vec![
                ("id-solarsystem", "solarsystem"),
                ("id-simulate-401", "simulate-401"),
                ("id-simulate-403", "simulate-403"),
                ("id-simulate-404", "simulate-404"),
            ],
            repos: solar_repos(),
            validate: false,
        },
        Scenario {
            name: "org-500",
            conns: vec![conn(&["solarsystem", "simulate-500"], &[], "")],
            account: Some(account(Some(PROFILE_JSON), TOKEN_JSON)),
            oauth: true,
            orgs: vec![
                ("id-solarsystem", "solarsystem"),
                ("id-simulate-500", "simulate-500"),
            ],
            repos: solar_repos(),
            validate: false,
        },
        Scenario {
            name: "project-invalid",
            conns: vec![conn(&["solarsystem"], &["bad"], "")],
            account: Some(account(Some(PROFILE_JSON), TOKEN_JSON)),
            oauth: true,
            orgs: solar(),
            repos: solar_repos(),
            validate: false,
        },
        Scenario {
            name: "no-orgs-configured",
            conns: vec![conn(&[], &[], "")],
            account: Some(account(Some(PROFILE_JSON), TOKEN_JSON)),
            oauth: true,
            orgs: solar(),
            repos: solar_repos(),
            validate: false,
        },
        Scenario {
            name: "validate-ok",
            conns: vec![
                conn(&["solarsystem"], &[], "t1"),
                conn(&["solarsystem"], &[], "t2"),
            ],
            account: None,
            oauth: true,
            orgs: Vec::new(),
            repos: BTreeMap::new(),
            validate: true,
        },
        Scenario {
            name: "validate-error",
            conns: vec![
                conn(&["solarsystem"], &[], "t1"),
                conn(&["solarsystem"], &[], "t2"),
            ],
            account: None,
            oauth: true,
            orgs: Vec::new(),
            repos: BTreeMap::new(),
            validate: true,
        },
    ]
}

async fn start_server(
    scenario: &Scenario,
    records: Arc<Mutex<Vec<String>>>,
) -> (String, CancellationToken) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind scripted server");
    let addr = listener.local_addr().expect("server addr");
    let server = format!("http://{addr}");
    let scenario = scenario.clone();
    let http: HttpHandler = Arc::new(move |req: http::Request<hyper::body::Incoming>| {
        let records = Arc::clone(&records);
        let scenario = scenario.clone();
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
                .push(format!("{method} {target} auth={auth}"));

            let path = target.split('?').next().unwrap_or("");
            match path {
                "/_apis/profile/profiles/me" => {
                    if scenario.name == "validate-error" {
                        route(400, "")
                    } else {
                        route(200, PROFILE_JSON)
                    }
                }
                "/_apis/accounts" => route(200, &orgs_json(&scenario.orgs)),
                path if path.ends_with("/_apis/git/repositories") => {
                    let org = path
                        .trim_start_matches('/')
                        .trim_end_matches("/_apis/git/repositories");
                    match org {
                        "simulate-401" => route(401, ""),
                        "simulate-403" => route(403, ""),
                        "simulate-404" => route(404, ""),
                        "simulate-500" => route(500, ""),
                        org => route(
                            200,
                            &repos_json(scenario.repos.get(org).map(Vec::as_slice).unwrap_or(&[])),
                        ),
                    }
                }
                _ => route(404, ""),
            }
        }) as sg_http::HttpFuture
    });
    let shutdown = CancellationToken::new();
    tokio::spawn(sg_http::serve_http(listener, http, shutdown.clone()));
    (server, shutdown)
}

fn error_kind(err: Option<&UserPermsError>) -> &'static str {
    match err {
        None => "none",
        Some(UserPermsError::Unauthorized { .. }) => "unauthorized",
        Some(UserPermsError::Forbidden { .. }) => "forbidden",
        Some(UserPermsError::AccountSuspended { .. }) => "account-suspended",
        Some(UserPermsError::Temporary { .. }) => "temporary",
        _ => "failed",
    }
}

struct NoopTokenStore;

#[async_trait]
impl OAuthTokenStore for NoopTokenStore {
    async fn get(&self, _id: i32) -> Result<Option<ExternalAccount>, String> {
        Ok(None)
    }

    async fn update(&self, _account: &ExternalAccount, _auth_data: &str) -> Result<(), String> {
        Ok(())
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn authz_azuredevops_matches_go_harness() {
    for scenario in scenarios() {
        let records = Arc::new(Mutex::new(Vec::<String>::new()));
        let (server, shutdown) = start_server(&scenario, Arc::clone(&records)).await;

        let http = reqwest::Client::builder()
            .no_proxy()
            .build()
            .expect("http client");
        let options = AzureDevOpsProviderOptions {
            api_url: server.clone(),
            visual_studio_app_url: Some(reqwest::Url::parse(&server).expect("URL")),
            oauth_providers: if scenario.oauth {
                vec![AzureDevOpsOAuthProvider {
                    client_id: "unique-id".to_owned(),
                    client_secret: "strong secret".to_owned(),
                }]
            } else {
                Vec::new()
            },
            external_url: "https://sourcegraph.example.com".to_owned(),
        };
        let provider = AzureDevOpsProvider::new(
            scenario.conns.clone(),
            options,
            Arc::new(NoopTokenStore),
            None,
            http,
        )
        .expect("provider");

        println!(
            "ado scenario={} providers=1 problems=0 invalid=0",
            scenario.name
        );
        println!(
            "ado scenario={} provider=0 urn={} type={} id={}",
            scenario.name,
            provider.urn(),
            provider.service_type(),
            provider.service_id()
        );

        records.lock().expect("lock").clear();
        let requests = |records: &Arc<Mutex<Vec<String>>>, server: &str| {
            let records = records.lock().expect("lock");
            if records.is_empty() {
                "<none>".to_owned()
            } else {
                records.join(" >> ").replace(server, "SERVER")
            }
        };
        let canon = |text: String, server: &str| {
            let text = text.replace(server, "SERVER").replace('\n', "\\n");
            if scenario.name == "malformed-auth-data" {
                const PREFIX: &str = "failed to load external account data from database with external account with ID: 7: ";
                if text.starts_with(PREFIX) {
                    return format!("{PREFIX}<json-error>");
                }
            }
            text
        };

        if scenario.validate {
            let err = provider.validate_connection().await;
            let err_text = match &err {
                Err(message) => canon(message.clone(), &server),
                Ok(()) => "<none>".to_owned(),
            };
            println!(
                "ado scenario={} validate={} requests={}",
                scenario.name,
                err_text,
                requests(&records, &server)
            );
        } else {
            let result = provider
                .fetch_user_perms(
                    scenario.account.as_ref().expect("account"),
                    &FetchPermsOptions {
                        invalidate_caches: false,
                    },
                )
                .await;
            let (ids, err) = match &result {
                Ok(perms) => {
                    let ids = perms
                        .as_ref()
                        .map(|perms| perms.exacts.join(","))
                        .filter(|ids| !ids.is_empty())
                        .unwrap_or_else(|| "<empty>".to_owned());
                    (ids, None)
                }
                Err(err) => ("<empty>".to_owned(), Some(err)),
            };
            let err_text = err
                .map(|err| canon(err.message().to_owned(), &server))
                .unwrap_or_else(|| "<none>".to_owned());
            println!(
                "ado scenario={} ids={} err={} kind={} requests={}",
                scenario.name,
                ids,
                err_text,
                error_kind(err),
                requests(&records, &server)
            );
        }

        shutdown.cancel();
    }
}
