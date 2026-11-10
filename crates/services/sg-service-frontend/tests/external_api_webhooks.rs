//! M9.8es differential harness: the old external `/.api/` webhook endpoints
//! and the UUID webhook endpoint, mirroring
//! `cmd/frontend/internal/httpapi/zz_webhook_endpoints_harness_test.go`.
//!
//! Prints one `ch ` line per case with the status, `Location`,
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
    let body = response
        .into_body()
        .collect()
        .await
        .expect("body")
        .to_bytes();
    let body = String::from_utf8_lossy(&body);
    println!(
        "ch {name} status={status} location={location} ctype={ctype} xcto={xcto} body={body:?}"
    );
}

#[tokio::test]
async fn external_api_webhooks_harness() {
    if std::env::var("SG_WEBHOOK_ENDPOINTS_HARNESS").as_deref() != Ok("1") {
        return;
    }
    let cases = [
        ("github-post", "POST", "/.api/github-webhooks"),
        ("github-post-body", "POST", "/.api/github-webhooks?x=1"),
        ("github-get", "GET", "/.api/github-webhooks"),
        ("github-trailing", "POST", "/.api/github-webhooks/"),
        ("github-unknown", "GET", "/.api/github-webhooks/x"),
        ("gitlab-post", "POST", "/.api/gitlab-webhooks"),
        ("gitlab-get", "GET", "/.api/gitlab-webhooks"),
        ("gitlab-trailing", "POST", "/.api/gitlab-webhooks/"),
        (
            "bitbucket-server-post",
            "POST",
            "/.api/bitbucket-server-webhooks",
        ),
        (
            "bitbucket-server-get",
            "GET",
            "/.api/bitbucket-server-webhooks",
        ),
        (
            "bitbucket-cloud-post",
            "POST",
            "/.api/bitbucket-cloud-webhooks",
        ),
        (
            "bitbucket-cloud-get",
            "GET",
            "/.api/bitbucket-cloud-webhooks",
        ),
        ("uuid-invalid", "POST", "/.api/webhooks/not-a-uuid"),
        ("uuid-invalid-urn", "POST", "/.api/webhooks/urn:uuid:xyz"),
        (
            "uuid-missing",
            "POST",
            "/.api/webhooks/00000000-0000-0000-0000-000000000000",
        ),
        (
            "uuid-simple",
            "POST",
            "/.api/webhooks/00000000000000000000000000000000",
        ),
        (
            "uuid-urn",
            "POST",
            "/.api/webhooks/urn:uuid:00000000-0000-0000-0000-000000000000",
        ),
        (
            "uuid-braced",
            "POST",
            "/.api/webhooks/%7B00000000-0000-0000-0000-000000000000%7D",
        ),
        (
            "uuid-uppercase",
            "POST",
            "/.api/webhooks/00000000-0000-0000-0000-00000000000A",
        ),
        (
            "uuid-get",
            "GET",
            "/.api/webhooks/00000000-0000-0000-0000-000000000000",
        ),
        (
            "uuid-trailing",
            "POST",
            "/.api/webhooks/00000000-0000-0000-0000-000000000000/",
        ),
        ("uuid-root", "POST", "/.api/webhooks/"),
        ("uuid-empty", "POST", "/.api/webhooks"),
    ];
    for (name, method, target) in cases {
        run(name, method, target).await;
    }
}
