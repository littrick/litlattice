//! M9.8ea differential harness: prints the same normalized `tg` lines as
//! `cmd/frontend/internal/cli/zz_frontend_tenant_goget_harness_test.go`.
//!
//! Two normalizations mirror the Go recorder's behavior:
//! - the "next" handler writes no body, so no Content-Type is sniffed;
//! - a 200 go-get response has its sniffed Content-Type reported as `-`
//!   (the Rust implementation sets it explicitly for real clients).

use http::{header, HeaderMap, Request, StatusCode};
use http_body_util::BodyExt;
use sg_service_frontend::goimportpath;
use sg_service_frontend::tenant::{external_tenant_from_hostname, TenantId, TenantLookupError};
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

async fn tenant_case(label: &str, host: &str, mapper_result: Result<i64, TenantLookupError>) {
    let mut req = Request::builder()
        .uri("http://harness.invalid/")
        .header(header::HOST, host)
        .body(())
        .expect("request");

    let mut mapper_host = None;
    let outcome = external_tenant_from_hostname(&mut req, |host| {
        mapper_host = Some(host.to_owned());
        mapper_result
    });

    let (status, ctype, xcto, tenant, body) = match outcome {
        Ok(()) => {
            let tenant = req
                .extensions()
                .get::<TenantId>()
                .map(|tenant| tenant.0.to_string())
                .unwrap_or_else(|| "err".to_owned());
            (
                200u16,
                "-".to_owned(),
                "-".to_owned(),
                tenant,
                String::new(),
            )
        }
        Err(response) => {
            let status = response.status().as_u16();
            let ctype = hdr(response.headers(), "content-type");
            let xcto = hdr(response.headers(), "x-content-type-options");
            let body = body_of(response).await;
            (status, ctype, xcto, "-1".to_owned(), body)
        }
    };
    println!(
        "tg tenant/{label} status={status} ctype={ctype} xcto={xcto} tenant={tenant} mapperHost={:?} body={body:?}",
        mapper_host.unwrap_or_default()
    );
}

async fn goget_case(label: &str, target: &str, external_url: &str) {
    let req = Request::builder().uri(target).body(()).expect("request");

    match goimportpath::response(&req, external_url) {
        None => println!("tg goget/{label} status=200 ctype=- xcto=- body=\"\""),
        Some(response) => {
            let status = response.status().as_u16();
            let ctype = if response.status() == StatusCode::OK {
                "-".to_owned()
            } else {
                hdr(response.headers(), "content-type")
            };
            let xcto = hdr(response.headers(), "x-content-type-options");
            let body = body_of(response).await;
            println!("tg goget/{label} status={status} ctype={ctype} xcto={xcto} body={body:?}");
        }
    }
}

#[tokio::test]
async fn tenant_and_goget_harness() {
    tenant_case("hosts/port", "example.com:3080", Ok(1)).await;
    tenant_case("hosts/no-port", "example.com", Ok(1)).await;
    tenant_case("hosts/custom-id", "tenant7.example.com:3080", Ok(7)).await;
    tenant_case("hosts/ipv6-literals", "::1", Ok(1)).await;
    tenant_case("hosts/empty", "", Ok(1)).await;
    tenant_case(
        "hosts/not-found",
        "unknown.example.com:3080",
        Err(TenantLookupError::NotFound),
    )
    .await;
    tenant_case(
        "hosts/other-error",
        "boom.example.com:3080",
        Err(TenantLookupError::Other),
    )
    .await;

    let external = "https://sourcegraph.com/";
    for (label, target) in [
        ("default/no-param", "/sourcegraph/foo"),
        ("default/sourcegraph", "/sourcegraph/foo?go-get=1"),
        ("default/sqs", "/sqs/bar?go-get=1"),
        ("default/extra-elements", "/sourcegraph/foo/extra?go-get=1"),
        ("default/other-user", "/other/repo?go-get=1"),
        ("default/single-element", "/sourcegraph?go-get=1"),
        (
            "default/first-value-wins",
            "/sourcegraph/foo?go-get=1&go-get=2",
        ),
        (
            "default/first-value-zero",
            "/sourcegraph/foo?go-get=0&go-get=1",
        ),
        ("default/plus-path", "/sourcegraph/foo+bar?go-get=1"),
        (
            "default/percent-encoded-slash",
            "/sourcegraph%2Ffoo?go-get=1",
        ),
        ("default/encoded-param-name", "/sqs/bar?go%2Dget=1"),
        ("default/query-extra", "/sqs/bar?x=1&go-get=1&y=2"),
    ] {
        goget_case(label, target, external).await;
    }

    goget_case(
        "port/sourcegraph",
        "/sourcegraph/foo?go-get=1",
        "https://example.com:3443/",
    )
    .await;
    goget_case(
        "userinfo/sourcegraph",
        "/sourcegraph/foo?go-get=1",
        "https://user:pass@internal.example/x",
    )
    .await;
    goget_case("empty/sourcegraph", "/sourcegraph/foo?go-get=1", "").await;
    goget_case(
        "no-scheme/sourcegraph",
        "/sourcegraph/foo?go-get=1",
        "sourcegraph.com",
    )
    .await;
}
