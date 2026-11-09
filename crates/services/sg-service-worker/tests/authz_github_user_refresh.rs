#![forbid(unsafe_code)]

//! M9.8bb differential harness (Rust side): the account token refresher the
//! GitHub user-centric authz provider installs as the OAuth token's
//! `RefreshFunc`.
//!
//! Mirrors `internal/oauthtoken/authz_user_refresh_harness_test.go`; outputs
//! are compared line for line by
//! `test_ws/p9-go/run_authz_github_user_refresh_diff.sh`.

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use http_body_util::BodyExt;
use sg_db::user_external_accounts::ExternalAccount;
use sg_http::HttpHandler;
use sg_service_worker::jobs::authz_github_user::{
    AccountTokenRefresher, OAuthContext, OAuthToken, OAuthTokenStore, TokenRefresher,
};
use tokio_util::sync::CancellationToken;

#[derive(Clone)]
struct Route {
    status: u16,
    content_type: String,
    body: String,
}

#[derive(Clone)]
struct Scenario {
    name: &'static str,
    stored_auth_data: Option<String>,
    original_token: &'static str,
    original_refresh: &'static str,
    route: Route,
    get_error: Option<&'static str>,
    update_error: Option<&'static str>,
}

fn no_route() -> Route {
    Route {
        status: 200,
        content_type: "application/json".to_owned(),
        body: String::new(),
    }
}

fn json_route(body: &str) -> Route {
    Route {
        status: 200,
        content_type: "application/json".to_owned(),
        body: body.to_owned(),
    }
}

fn form_route(content_type: &str, body: &str) -> Route {
    Route {
        status: 200,
        content_type: content_type.to_owned(),
        body: body.to_owned(),
    }
}

fn stored_token_json(access: &str, refresh: &str, expiry: &str) -> String {
    format!(
        r#"{{"access_token":"{access}","token_type":"bearer","refresh_token":"{refresh}","expiry":"{expiry}"}}"#
    )
}

fn describe_stored(auth_data: &str) -> String {
    let Ok(value) = serde_json::from_str::<serde_json::Value>(auth_data) else {
        return "<undecodable>".to_owned();
    };
    let field = |name: &str| {
        value
            .get(name)
            .and_then(serde_json::Value::as_str)
            .unwrap_or("")
            .to_owned()
    };
    let expiry = field("expiry");
    let expiry = if expiry.is_empty() || expiry == "0001-01-01T00:00:00Z" {
        "zero"
    } else {
        "set"
    };
    format!(
        "access={} refresh={} type={} expiry={}",
        field("access_token"),
        field("refresh_token"),
        field("token_type"),
        expiry
    )
}

struct MockStore {
    account: Option<ExternalAccount>,
    get_error: Option<String>,
    update_error: Option<String>,
    updated: Mutex<Option<String>>,
}

#[async_trait]
impl OAuthTokenStore for MockStore {
    async fn get(&self, _id: i32) -> Result<Option<ExternalAccount>, String> {
        if let Some(error) = &self.get_error {
            return Err(error.clone());
        }
        Ok(self.account.clone())
    }

    async fn update(&self, _account: &ExternalAccount, auth_data: &str) -> Result<(), String> {
        *self.updated.lock().expect("lock") = Some(auth_data.to_owned());
        if let Some(error) = &self.update_error {
            return Err(error.clone());
        }
        Ok(())
    }
}

async fn start_server(
    route: Route,
    records: Arc<Mutex<Vec<String>>>,
) -> (String, CancellationToken) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind scripted server");
    let addr = listener.local_addr().expect("server addr");
    let http: HttpHandler = Arc::new(move |req: http::Request<hyper::body::Incoming>| {
        let route = route.clone();
        let records = Arc::clone(&records);
        Box::pin(async move {
            let method = req.method().to_string();
            let path = req.uri().path().to_owned();
            let content_type = req
                .headers()
                .get(http::header::CONTENT_TYPE)
                .and_then(|value| value.to_str().ok())
                .unwrap_or("")
                .to_owned();
            let body = req
                .into_body()
                .collect()
                .await
                .map(|collected| collected.to_bytes())
                .unwrap_or_default();
            records.lock().expect("lock").push(format!(
                "req method={method} path={path} ctype={content_type} body={}",
                String::from_utf8_lossy(&body)
            ));

            http::Response::builder()
                .status(route.status)
                .header(http::header::CONTENT_TYPE, route.content_type.clone())
                .body(tonic::body::boxed(http_body_util::Full::new(
                    hyper::body::Bytes::from(route.body.clone()),
                )))
                .expect("response")
        }) as sg_http::HttpFuture
    });
    let shutdown = CancellationToken::new();
    tokio::spawn(sg_http::serve_http(listener, http, shutdown.clone()));
    (format!("http://{addr}"), shutdown)
}

fn account(stored_auth_data: Option<&str>) -> ExternalAccount {
    ExternalAccount {
        id: 1,
        user_id: 1,
        service_type: "github".to_owned(),
        service_id: "https://github.com/".to_owned(),
        client_id: "client".to_owned(),
        account_id: "42".to_owned(),
        auth_data: stored_auth_data.map(str::to_owned),
        account_data: None,
        encryption_key_id: String::new(),
        expired: false,
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn authz_github_user_refresh_matches_go_harness() {
    let expired = stored_token_json("T2", "R2", "2020-01-01T00:00:00Z");
    let fresh = stored_token_json("T2", "R2", "0001-01-01T00:00:00Z");
    let plain = stored_token_json("T1", "R1", "0001-01-01T00:00:00Z");

    let scenarios = vec![
        Scenario {
            name: "refresh-json",
            stored_auth_data: Some(plain.clone()),
            original_token: "T1",
            original_refresh: "R1",
            route: json_route(
                r#"{"access_token":"T2","token_type":"bearer","refresh_token":"R2","expires_in":3600}"#,
            ),
            get_error: None,
            update_error: None,
        },
        Scenario {
            name: "refresh-form",
            stored_auth_data: Some(plain.clone()),
            original_token: "T1",
            original_refresh: "R1",
            route: form_route(
                "application/x-www-form-urlencoded",
                "access_token=T2&token_type=bearer&refresh_token=R2&expires_in=3600",
            ),
            get_error: None,
            update_error: None,
        },
        Scenario {
            name: "refresh-text-plain",
            stored_auth_data: Some(plain.clone()),
            original_token: "T1",
            original_refresh: "R1",
            route: form_route(
                "text/plain",
                "access_token=T2&token_type=bearer&refresh_token=R2&expires_in=60",
            ),
            get_error: None,
            update_error: None,
        },
        Scenario {
            name: "refresh-no-expires",
            stored_auth_data: Some(plain.clone()),
            original_token: "T1",
            original_refresh: "R1",
            route: json_route(
                r#"{"access_token":"T2","token_type":"bearer","refresh_token":"R2"}"#,
            ),
            get_error: None,
            update_error: None,
        },
        Scenario {
            name: "refresh-negative-expires",
            stored_auth_data: Some(plain.clone()),
            original_token: "T1",
            original_refresh: "R1",
            route: json_route(
                r#"{"access_token":"T2","token_type":"bearer","refresh_token":"R2","expires_in":-60}"#,
            ),
            get_error: None,
            update_error: None,
        },
        Scenario {
            name: "refresh-huge-expires",
            stored_auth_data: Some(plain.clone()),
            original_token: "T1",
            original_refresh: "R1",
            route: json_route(
                r#"{"access_token":"T2","token_type":"bearer","refresh_token":"R2","expires_in":4294967296}"#,
            ),
            get_error: None,
            update_error: None,
        },
        Scenario {
            name: "refresh-missing-refresh",
            stored_auth_data: Some(plain.clone()),
            original_token: "T1",
            original_refresh: "R1",
            route: json_route(r#"{"access_token":"T2","token_type":"bearer"}"#),
            get_error: None,
            update_error: None,
        },
        Scenario {
            name: "missing-access-token",
            stored_auth_data: Some(plain.clone()),
            original_token: "T1",
            original_refresh: "R1",
            route: json_route(r#"{"token_type":"bearer","refresh_token":"R2"}"#),
            get_error: None,
            update_error: None,
        },
        Scenario {
            name: "http-400",
            stored_auth_data: Some(plain.clone()),
            original_token: "T1",
            original_refresh: "R1",
            route: Route {
                status: 400,
                content_type: "application/json".to_owned(),
                body: r#"{"error":"invalid_grant","error_description":"bad code","error_uri":"https://example/err"}"#
                    .to_owned(),
            },
            get_error: None,
            update_error: None,
        },
        Scenario {
            name: "token-error-json",
            stored_auth_data: Some(plain.clone()),
            original_token: "T1",
            original_refresh: "R1",
            route: json_route(
                r#"{"error":"invalid_grant","error_description":"bad code","error_uri":"https://example/err"}"#,
            ),
            get_error: None,
            update_error: None,
        },
        Scenario {
            name: "token-error-form",
            stored_auth_data: Some(plain.clone()),
            original_token: "T1",
            original_refresh: "R1",
            route: form_route(
                "application/x-www-form-urlencoded",
                "error=invalid_grant&error_description=bad+code&error_uri=https%3A%2F%2Fexample%2Ferr",
            ),
            get_error: None,
            update_error: None,
        },
        Scenario {
            name: "already-refreshed",
            stored_auth_data: Some(fresh.clone()),
            original_token: "T1",
            original_refresh: "R1",
            route: no_route(),
            get_error: None,
            update_error: None,
        },
        Scenario {
            name: "already-refreshed-expired",
            stored_auth_data: Some(expired.clone()),
            original_token: "T1",
            original_refresh: "R1",
            route: json_route(
                r#"{"access_token":"T3","token_type":"bearer","refresh_token":"R3"}"#,
            ),
            get_error: None,
            update_error: None,
        },
        Scenario {
            name: "same-token-no-expiry",
            stored_auth_data: Some(plain.clone()),
            original_token: "T1",
            original_refresh: "R1",
            route: json_route(
                r#"{"access_token":"T2","token_type":"bearer","refresh_token":"R2"}"#,
            ),
            get_error: None,
            update_error: None,
        },
        Scenario {
            name: "no-auth-data",
            stored_auth_data: None,
            original_token: "T1",
            original_refresh: "R1",
            route: no_route(),
            get_error: None,
            update_error: None,
        },
        Scenario {
            name: "get-error",
            stored_auth_data: Some(plain.clone()),
            original_token: "T1",
            original_refresh: "R1",
            route: no_route(),
            get_error: Some("boom"),
            update_error: None,
        },
        Scenario {
            name: "update-error",
            stored_auth_data: Some(plain.clone()),
            original_token: "T1",
            original_refresh: "R1",
            route: json_route(
                r#"{"access_token":"T2","token_type":"bearer","refresh_token":"R2"}"#,
            ),
            get_error: None,
            update_error: Some("boom"),
        },
    ];

    for scenario in scenarios {
        let records = Arc::new(Mutex::new(Vec::new()));
        let (base_url, shutdown) = start_server(scenario.route.clone(), Arc::clone(&records)).await;

        let store = Arc::new(MockStore {
            account: Some(account(scenario.stored_auth_data.as_deref())),
            get_error: scenario.get_error.map(str::to_owned),
            update_error: scenario.update_error.map(str::to_owned),
            updated: Mutex::new(None),
        });
        let refresher = AccountTokenRefresher::new(
            store.clone(),
            None,
            1,
            OAuthContext {
                client_id: "client".to_owned(),
                client_secret: "secret".to_owned(),
                token_url: format!("{base_url}/login/oauth/access_token"),
                custom_query_params: std::collections::BTreeMap::new(),
            },
            reqwest::Client::new(),
        );
        let original = OAuthToken {
            access_token: scenario.original_token.to_owned(),
            token_type: String::new(),
            refresh_token: scenario.original_refresh.to_owned(),
            expiry: None,
        };

        let name = scenario.name;
        let (access, refresh, expiry, error_text) = match refresher.refresh(&original).await {
            Ok(token) => (
                token.access_token,
                token.refresh_token,
                if token.expiry.is_some() {
                    "set"
                } else {
                    "zero"
                },
                "none".to_owned(),
            ),
            Err(error) => ("<none>".to_owned(), "<none>".to_owned(), "zero", error),
        };
        println!("gur scenario={name} access={access} refresh={refresh} expiry={expiry} err={error_text}");

        let records = records.lock().expect("lock").clone();
        if records.is_empty() {
            println!("gur scenario={name} req=none");
        }
        for record in records {
            println!("gur scenario={name} {record}");
        }

        let updated = store.updated.lock().expect("lock").clone();
        match updated {
            None => println!("gur scenario={name} update=none"),
            Some(auth_data) => println!(
                "gur scenario={name} update=stored stored={}",
                describe_stored(&auth_data)
            ),
        }

        shutdown.cancel();
    }
}
