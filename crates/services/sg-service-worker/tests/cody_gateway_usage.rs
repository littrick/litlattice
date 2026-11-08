#![forbid(unsafe_code)]

//! M9.8w differential harness (Rust side): the `cody-gateway-usage-check` job.
//!
//! Mirrors `cmd/worker/internal/codygateway/usage_harness_test.go`. Both sides
//! point at their own local gateway server (`SG_CG_ENDPOINT`) that records the
//! method, path and `Authorization` header, and both read the resulting
//! `v1:cody_gateway_usage_percent:*` keys from `REDIS_STORE_ENDPOINT` (the
//! shared scratch Redis), printing them with TTLs bucketed to whole minutes.
//!
//! `test_ws/p9-go/run_cody_gateway_usage_diff.sh` runs both harnesses and
//! compares the canonical lines.
//!
//! Handler error values are not printed: the Go harness drives the production
//! `PeriodicGoroutine`, whose errors are only logged. Observable behaviour
//! (HTTP requests and Redis state) is compared instead; an erroring `SETEX`
//! therefore shows up as a missing key on both sides.

use std::sync::{Arc, Mutex};

use http_body_util::BodyExt;
use sg_config::{LiveConfig, ServiceConnections, SiteConfiguration, Unified};
use sg_service_worker::jobs::codygateway::{CodyGatewayUsageCheck, CodyGatewayUsageHandler};
use sg_service_worker::jobs::{Job, JobDeps};
use tokio_util::sync::CancellationToken;

#[tokio::test(flavor = "multi_thread")]
async fn cody_gateway_usage_matches_the_go_harness() {
    if std::env::var("SG_CG_HARNESS_ON").as_deref() != Ok("1") {
        return;
    }
    let Ok(store) = sg_redis::KeyValue::connect(&sg_redis::store_endpoint()).await else {
        eprintln!("cannot connect to REDIS_STORE_ENDPOINT");
        return;
    };

    let job = CodyGatewayUsageCheck;
    println!("jjob description={:?}", job.description());
    println!("jconfig count={}", job.configs().len());

    let deps = JobDeps {
        frontend: sqlx::postgres::PgPoolOptions::new()
            .connect_lazy("postgres://unused@localhost/unused")
            .expect("lazy pool"),
        live_config: Arc::new(LiveConfig::new(Unified {
            site: SiteConfiguration::default(),
            service_connections: ServiceConnections::default(),
        })),
        keyring: Arc::new(sg_keyring::Keyring::default()),
    };
    let routines = job
        .routines(&deps, CancellationToken::new())
        .await
        .expect("routines");
    assert_eq!(routines.len(), 1);
    println!("jname name={}", routines[0].name());
    println!("jdesc description={:?}", routines[0].description());
    println!("jinterval secs={}", routines[0].interval().as_secs());

    let server_now = sg_db::OffsetDateTime::now_utc();
    let rfc3339 = time::format_description::well_known::Rfc3339;
    let gateway_response = format!(
        r#"{{
            "chat_completions": {{"limit": 100, "usage": 42, "interval": "monthly"}},
            "code_completions": {{"limit": 0, "usage": 0, "interval": "monthly"}},
            "embeddings": {{"limit": 200, "usage": 250, "interval": "monthly",
                "expiry": {:?}}},
            "unknown_feature": {{"limit": 1, "usage": 1, "interval": "monthly"}}
        }}"#,
        (server_now + time::Duration::minutes(10))
            .format(&rfc3339)
            .expect("format")
    );
    let expired_response = format!(
        r#"{{
            "chat_completions": {{"limit": 100, "usage": 42, "interval": "monthly",
                "expiry": {:?}}},
            "code_completions": {{"limit": 4, "usage": 1, "interval": "monthly"}}
        }}"#,
        (server_now - time::Duration::minutes(10))
            .format(&rfc3339)
            .expect("format")
    );

    let requests: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let (endpoint, shutdown) =
        start_gateway(gateway_response, expired_response, Arc::clone(&requests)).await;

    let scenarios = [
        ("gateway", site_json(&endpoint, "\"good\"", "")),
        ("expire-past", site_json(&endpoint, "\"expire-past\"", "")),
        ("server-error", site_json(&endpoint, "\"501\"", "")),
        ("invalid-json", site_json(&endpoint, "\"invalid-json\"", "")),
        ("disabled-provider", provider_site_json(&endpoint)),
        ("disabled-cody", cody_disabled_site_json(&endpoint)),
        ("default-provider", default_provider_site_json(&endpoint)),
        ("license-token", license_site_json(&endpoint)),
    ];

    for (name, site) in scenarios {
        clear_keys(&store).await;
        let handler = CodyGatewayUsageHandler::new(Arc::new(LiveConfig::new(Unified {
            site: SiteConfiguration::parse(&site).expect("site config"),
            service_connections: ServiceConnections::default(),
        })));
        // `handle_once` runs exactly what the periodic routine runs.
        let _ = handler.handle_once().await;

        println!("run scenario={name} keys={}", format_keys(&store).await);
    }

    shutdown.cancel();
    let sorted = {
        let mut requests = requests.lock().expect("lock").clone();
        requests.sort();
        requests
    };
    for line in sorted {
        println!("{line}");
    }
}

/// A local gateway: `/v1/limits` responses are selected by bearer token.
async fn start_gateway(
    gateway_response: String,
    expired_response: String,
    requests: Arc<Mutex<Vec<String>>>,
) -> (String, CancellationToken) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind gateway");
    let addr = listener.local_addr().expect("addr");
    let http: sg_http::HttpHandler = Arc::new(move |req: http::Request<hyper::body::Incoming>| {
        let gateway_response = gateway_response.clone();
        let expired_response = expired_response.clone();
        let requests = Arc::clone(&requests);
        Box::pin(async move {
            let method = req.method().to_string();
            let path = req.uri().path().to_owned();
            let auth = req
                .headers()
                .get(http::header::AUTHORIZATION)
                .and_then(|value| value.to_str().ok())
                .unwrap_or("")
                .to_owned();
            requests
                .lock()
                .expect("lock")
                .push(format!("req method={method} path={path} auth={auth}"));
            let _ = req.into_body().collect().await;

            let (status, body) = match auth.as_str() {
                // 501 is the baseline's explicitly non-retried 5xx status
                // (`httpcli.ExternalDoer` retries 5xx up to 20 times).
                "Bearer 501" => (501u16, "nope".to_owned()),
                "Bearer invalid-json" => (200, "not json".to_owned()),
                "Bearer expire-past" => (200, expired_response),
                _ => (200, gateway_response),
            };
            http::Response::builder()
                .status(status)
                .header("Cache-Control", "no-store")
                .body(tonic::body::boxed(http_body_util::Full::new(
                    hyper::body::Bytes::from(body),
                )))
                .expect("response")
        }) as sg_http::HttpFuture
    });
    let shutdown = CancellationToken::new();
    let shutdown_child = shutdown.clone();
    tokio::spawn(sg_http::serve_http(listener, http, shutdown_child));
    (format!("http://{addr}"), shutdown)
}

fn site_json(endpoint: &str, token: &str, extra: &str) -> String {
    format!(
        r#"{{"cody.enabled": true, "completions": {{"provider": "sourcegraph", "endpoint": {endpoint:?}, "accessToken": {token}}}{extra}}}"#
    )
}

fn provider_site_json(endpoint: &str) -> String {
    format!(
        r#"{{"cody.enabled": true, "completions": {{"provider": "openai", "endpoint": {endpoint:?}, "accessToken": "good"}}}}"#
    )
}

fn cody_disabled_site_json(endpoint: &str) -> String {
    format!(
        r#"{{"cody.enabled": false, "completions": {{"provider": "sourcegraph", "endpoint": {endpoint:?}, "accessToken": "good"}}}}"#
    )
}

fn default_provider_site_json(endpoint: &str) -> String {
    format!(
        r#"{{"cody.enabled": true, "completions": {{"endpoint": {endpoint:?}, "accessToken": "good"}}}}"#
    )
}

fn license_site_json(endpoint: &str) -> String {
    format!(
        r#"{{"cody.enabled": true, "licenseKey": "test-license", "completions": {{"provider": "sourcegraph", "endpoint": {endpoint:?}}}}}"#
    )
}

async fn clear_keys(store: &sg_redis::KeyValue) {
    let keys = store
        .keys("v1:cody_gateway_usage_percent:*")
        .await
        .expect("list keys");
    for key in keys {
        store.del(&key).await.expect("delete key");
    }
}

async fn format_keys(store: &sg_redis::KeyValue) -> String {
    let mut keys = store
        .keys("v1:cody_gateway_usage_percent:*")
        .await
        .expect("list keys");
    keys.sort();
    if keys.is_empty() {
        return "<none>".to_owned();
    }
    let mut parts = Vec::with_capacity(keys.len());
    for key in keys {
        let value = store
            .get_string(&key)
            .await
            .expect("get key")
            .unwrap_or_default();
        let ttl = store.ttl(&key).await.expect("ttl");
        // Bucket to whole minutes (ceil), matching the Go harness.
        let bucket = (ttl + 59) / 60;
        let feature = key
            .strip_prefix("v1:cody_gateway_usage_percent:")
            .unwrap_or(&key);
        parts.push(format!("{feature}={value}:{bucket}"));
    }
    parts.join(",")
}
