//! M9.8et differential harness: the external `/.api/opencodegraph` endpoint
//! with the default feature flags and the `JsonMiddleware`/`ErrorHandler`
//! error contract, mirroring
//! `cmd/frontend/internal/httpapi/zz_opencodegraph_harness_test.go`.
//!
//! Prints one `co ` line per case with the status, `Location`,
//! `Content-Type`, `X-Content-Type-Options`, `Cache-Control` and body.

use http::header;
use http::Request;
use http_body_util::BodyExt;
use sg_config::{LiveConfig, ServiceConnections, SiteConfiguration, Unified};
use sg_service_frontend::external_api::api_response;

fn live() -> LiveConfig {
    let site = SiteConfiguration::parse("{}").expect("site");
    LiveConfig::new(Unified {
        site,
        service_connections: ServiceConnections::default(),
    })
}

async fn run(name: &str, method: &str, target: &str) {
    let request = Request::builder()
        .method(method)
        .uri(target)
        .body(())
        .expect("request");
    let response = api_response(&request, &live());
    let status = response.status().as_u16();
    let field = |name: &http::HeaderName| match response.headers().get(name) {
        None => "-".to_owned(),
        Some(value) => format!("{value:?}"),
    };
    let location = field(&header::LOCATION);
    let ctype = field(&header::CONTENT_TYPE);
    let xcto = field(&header::X_CONTENT_TYPE_OPTIONS);
    let cache = field(&header::CACHE_CONTROL);
    let body = response
        .into_body()
        .collect()
        .await
        .expect("body")
        .to_bytes();
    let body = String::from_utf8_lossy(&body);
    println!(
        "co {name} status={status} location={location} ctype={ctype} xcto={xcto} cache={cache} body={body:?}"
    );
}

#[tokio::test]
async fn external_api_opencodegraph_harness() {
    if std::env::var("SG_OPENCODEGRAPH_HARNESS").as_deref() != Ok("1") {
        return;
    }
    let cases = [
        ("post", "POST", "/.api/opencodegraph"),
        ("post-body", "POST", "/.api/opencodegraph?x=1"),
        ("get", "GET", "/.api/opencodegraph"),
        ("put", "PUT", "/.api/opencodegraph"),
        ("trailing", "POST", "/.api/opencodegraph/"),
        ("extra-segment", "POST", "/.api/opencodegraph/x"),
        ("unknown", "POST", "/.api/nope"),
    ];
    for (name, method, target) in cases {
        run(name, method, target).await;
    }
}
