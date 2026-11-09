//! M9.8ep differential harness: the external `/.api/` subrouter restricted to
//! its deterministic OSS routes (SCIM, lsif/scip upload, compute stream),
//! mirroring `cmd/frontend/internal/httpapi/zz_external_api_harness_test.go`.
//!
//! Prints one `ca ` line per case with the status, `Location`,
//! `Content-Type`, `X-Content-Type-Options` and body.

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

async fn run(live: &LiveConfig, name: &str, method: &str, target: &str) {
    let request = Request::builder()
        .method(method)
        .uri(target)
        .body(())
        .expect("request");
    let response = api_response(&request, live);
    let status = response.status().as_u16();
    let field = |name: &http::HeaderName| match response.headers().get(name) {
        None => "-".to_owned(),
        Some(value) => format!("{value:?}"),
    };
    let location = field(&header::LOCATION);
    let ctype = field(&header::CONTENT_TYPE);
    let xcto = field(&header::X_CONTENT_TYPE_OPTIONS);
    let body = response
        .into_body()
        .collect()
        .await
        .expect("body")
        .to_bytes();
    let body = String::from_utf8_lossy(&body);
    println!(
        "ca {name} status={status} location={location} ctype={ctype} xcto={xcto} body={body:?}"
    );
}

#[tokio::test]
async fn external_api_harness() {
    if std::env::var("SG_EXTERNAL_API_HARNESS").as_deref() != Ok("1") {
        return;
    }
    let live = live();
    let cases = [
        ("scim-get", "GET", "/.api/scim/v2/Users"),
        ("scim-post", "POST", "/.api/scim/v2/Users"),
        ("scim-put", "PUT", "/.api/scim/v2/Users"),
        ("scim-head", "HEAD", "/.api/scim/v2/Users"),
        ("scim-prefix-extra", "GET", "/.api/scim/v2extra"),
        ("scim-trailing", "GET", "/.api/scim/v2/"),
        ("lsif-post", "POST", "/.api/lsif/upload"),
        ("lsif-get", "GET", "/.api/lsif/upload"),
        ("lsif-get-trailing", "GET", "/.api/lsif/upload/"),
        ("lsif-trailing", "POST", "/.api/lsif/upload/"),
        ("scip-post", "POST", "/.api/scip/upload"),
        ("scip-head", "HEAD", "/.api/scip/upload"),
        ("scip-get", "GET", "/.api/scip/upload"),
        ("compute-get", "GET", "/.api/compute/stream"),
        ("compute-post", "POST", "/.api/compute/stream"),
        ("compute-head", "HEAD", "/.api/compute/stream"),
        ("compute-trailing", "GET", "/.api/compute/stream/"),
        ("unknown", "GET", "/.api/nope"),
        ("clean-double-slash", "POST", "/.api/lsif//upload"),
    ];
    for (name, method, target) in cases {
        run(&live, name, method, target).await;
    }
}
