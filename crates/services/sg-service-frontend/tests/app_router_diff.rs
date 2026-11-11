//! M9.8iu differential harness: replays the same corpus as the Go oracle
//! (`cmd/frontend/internal/app/zz_m9iu_oracle_test.go`) through
//! `app_router::dispatch_async` and emits the same normalized JSON.
//!
//! The test is gated on `M9IU_CORPUS`; `run_app_router_diff.sh` sets it and
//! points `M9IU_OUT` at the output file. Like the Go side, the harness drives
//! the app handler alone (without the outer `secureHeadersMiddleware`).

use std::sync::Arc;

use http::{Method, Request};
use http_body_util::BodyExt;
use sg_service_frontend::app_handlers::{AppConfig, AppDeps, AppEnv, GoImportersMock};
use sg_service_frontend::app_router;

#[derive(Debug, serde::Deserialize)]
struct Case {
    name: String,
    method: String,
    target: String,
    robots_allow: String,
    dotcom: bool,
    deploy_type: String,
    external_url: String,
    branding_favicon: String,
    insecure_dev: bool,
    badge_mock: Option<i64>,
    headers: Vec<(String, String)>,
}

#[derive(Debug, serde::Serialize)]
struct ResultRow {
    name: String,
    status: u16,
    location: String,
    content_type: String,
    cache_control: String,
    x_content_type_options: String,
    body: String,
}

fn header_value(headers: &http::HeaderMap, name: &str) -> String {
    match headers.get_all(name).iter().next() {
        None => "-".to_owned(),
        Some(_) => {
            let mut values: Vec<String> = Vec::new();
            for value in headers.get_all(name).iter() {
                values.push(String::from_utf8_lossy(value.as_bytes()).into_owned());
            }
            values.join(",")
        }
    }
}

fn request(case: &Case) -> Request<()> {
    let method = Method::from_bytes(case.method.as_bytes()).expect("method");
    let mut builder = Request::builder().method(method).uri(case.target.as_str());
    for (name, value) in &case.headers {
        builder = builder.header(name.as_str(), value.as_str());
    }
    builder.body(()).expect("request")
}

fn deps(case: &Case) -> AppDeps {
    match case.badge_mock {
        Some(count) => {
            let mock: GoImportersMock = Arc::new(move |_repo: &str| Ok(count));
            AppDeps::with_mock(mock)
        }
        None => AppDeps::default(),
    }
}

#[tokio::test]
async fn app_router_diff() {
    let Ok(corpus_path) = std::env::var("M9IU_CORPUS") else {
        eprintln!("M9IU_CORPUS not set; skipping app router differential");
        return;
    };
    let corpus = std::fs::read_to_string(&corpus_path).expect("read corpus");
    let cases: Vec<Case> = serde_json::from_str(&corpus).expect("parse corpus");

    let mut rows = Vec::with_capacity(cases.len());
    for case in &cases {
        let env = AppEnv {
            robots_allow: case.robots_allow.clone(),
            dotcom: case.dotcom,
            deploy_type: case.deploy_type.clone(),
            assets_root: "/.assets".to_owned(),
            insecure_dev: case.insecure_dev,
            internal_api_url: "http://sourcegraph-frontend-internal:80".to_owned(),
        };
        let config = AppConfig {
            external_url: case.external_url.clone(),
            branding_favicon: case.branding_favicon.clone(),
        };
        let deps = deps(case);
        let mut req = request(case);
        let response = app_router::dispatch_async(&mut req, &env, &config, &deps).await;

        let status = response.status().as_u16();
        let headers = response.headers().clone();
        let location = header_value(&headers, "location");
        let content_type = header_value(&headers, "content-type");
        let cache_control = header_value(&headers, "cache-control");
        let x_content_type_options = header_value(&headers, "x-content-type-options");
        let body = response
            .into_body()
            .collect()
            .await
            .expect("body")
            .to_bytes();
        let body = String::from_utf8_lossy(&body).into_owned();

        rows.push(ResultRow {
            name: case.name.clone(),
            status,
            location,
            content_type,
            cache_control,
            x_content_type_options,
            body,
        });
    }

    let encoded = serde_json::to_string_pretty(&rows).expect("serialize results");
    // Go's `encoding/json` escapes these three characters by default; match it
    // so the diff script can compare the files byte for byte.
    let encoded = encoded
        .replace('<', "\\u003c")
        .replace('>', "\\u003e")
        .replace('&', "\\u0026");

    if let Ok(out_path) = std::env::var("M9IU_OUT") {
        std::fs::write(out_path, encoded).expect("write results");
    } else {
        println!("{encoded}");
    }
}
