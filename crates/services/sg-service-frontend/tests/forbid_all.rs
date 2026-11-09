//! M9.8ec differential harness: prints the same normalized `fb` lines as
//! `cmd/frontend/internal/cli/zz_frontend_forbid_all_harness_test.go`.
//!
//! The chain's next handler returns 204 with no body so the Go recorder does
//! not sniff a Content-Type that hyper would not produce.

use bytes::Bytes;
use http::{header, HeaderMap, Request, Response, StatusCode};
use http_body_util::{BodyExt, Full};
use sg_config::SiteConfiguration;
use sg_service_frontend::auth;
use sg_service_frontend::middleware::external_response_with_ipallowlist;
use tonic::body::BoxBody;

fn hdr(headers: &HeaderMap, name: &str) -> String {
    match headers.get(name) {
        Some(value) => format!("{:?}", value.to_str().unwrap_or("")),
        None => "-".to_owned(),
    }
}

async fn body_of(response: http::Response<BoxBody>) -> String {
    let bytes = response
        .into_body()
        .collect()
        .await
        .expect("body")
        .to_bytes();
    String::from_utf8_lossy(&bytes).into_owned()
}

async fn run_case(name: &str, json: &str) {
    let site = SiteConfiguration::parse(json).expect("site config");
    let mut req = Request::builder()
        .uri("http://harness.invalid/")
        .body(())
        .expect("request");
    let response = external_response_with_ipallowlist(
        &mut req,
        &Bytes::from_static(b"v"),
        None,
        auth::forbidden(&site),
        "",
        |_req| {
            Response::builder()
                .status(StatusCode::NO_CONTENT)
                .body(tonic::body::boxed(Full::new(Bytes::new())))
                .expect("next response")
        },
    );
    let status = response.status().as_u16();
    let ctype = hdr(response.headers(), header::CONTENT_TYPE.as_str());
    let xcto = hdr(response.headers(), header::X_CONTENT_TYPE_OPTIONS.as_str());
    let body = body_of(response).await;
    println!("fb {name} status={status} ctype={ctype} xcto={xcto} body={body:?}");
}

#[tokio::test]
async fn forbid_all_harness() {
    run_case("none", "{}").await;
    run_case("empty-array", r#"{"auth.providers":[]}"#).await;
    run_case("builtin", r#"{"auth.providers":[{"type":"builtin"}]}"#).await;
    run_case(
        "two",
        r#"{"auth.providers":[{"type":"builtin"},{"type":"github"}]}"#,
    )
    .await;
    run_case("zero-element", r#"{"auth.providers":[null]}"#).await;
}
