//! M9.8eq differential harness: the remaining external `/.api/` endpoints
//! whose OSS defaults are deterministic (registry non-dotcom, batch-changes
//! files, insights/search exports, completions), mirroring
//! `cmd/frontend/internal/httpapi/zz_external_api_enterprise_harness_test.go`.
//!
//! Prints one `cb ` line per case with the status, `Location`,
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
    // `net/http` (and hyper) drop the body of a HEAD response on the wire;
    // the Go harness observes through a real client, so mirror that here.
    let body = if method == "HEAD" { &[][..] } else { &body[..] };
    let body = String::from_utf8_lossy(body);
    println!(
        "cb {name} status={status} location={location} ctype={ctype} xcto={xcto} body={body:?}"
    );
}

#[tokio::test]
async fn external_api_enterprise_harness() {
    if std::env::var("SG_EXTERNAL_API_ENTERPRISE_HARNESS").as_deref() != Ok("1") {
        return;
    }
    let live = live();
    let cases = [
        ("registry-get", "GET", "/.api/registry"),
        (
            "registry-extensions",
            "GET",
            "/.api/registry/extensions?q=x",
        ),
        ("registry-prefix-extra", "GET", "/.api/registryfoo"),
        ("registry-trailing", "GET", "/.api/registry/"),
        ("registry-post", "POST", "/.api/registry"),
        (
            "batch-get",
            "GET",
            "/.api/files/batch-changes/spec1/file.txt",
        ),
        (
            "batch-exists",
            "HEAD",
            "/.api/files/batch-changes/spec1/file.txt",
        ),
        (
            "batch-get-post",
            "POST",
            "/.api/files/batch-changes/spec1/file.txt",
        ),
        ("batch-upload", "POST", "/.api/files/batch-changes/spec1"),
        ("batch-upload-get", "GET", "/.api/files/batch-changes/spec1"),
        (
            "batch-trailing",
            "GET",
            "/.api/files/batch-changes/spec1/file.txt/",
        ),
        (
            "batch-trailing-post",
            "POST",
            "/.api/files/batch-changes/spec1/",
        ),
        (
            "batch-three-segments",
            "GET",
            "/.api/files/batch-changes/a/b/c",
        ),
        ("insights-get", "GET", "/.api/insights/export/7"),
        ("insights-post", "POST", "/.api/insights/export/7"),
        ("search-jsonl-get", "GET", "/.api/search/export/3.jsonl"),
        ("search-jsonl-post", "POST", "/.api/search/export/3.jsonl"),
        ("search-log-get", "GET", "/.api/search/export/3.log"),
        (
            "completions-stream-post",
            "POST",
            "/.api/completions/stream",
        ),
        ("completions-stream-get", "GET", "/.api/completions/stream"),
        ("completions-code-post", "POST", "/.api/completions/code"),
        ("completions-code-get", "GET", "/.api/completions/code"),
        ("unknown", "GET", "/.api/nope"),
    ];
    for (name, method, target) in cases {
        run(&live, name, method, target).await;
    }
}
