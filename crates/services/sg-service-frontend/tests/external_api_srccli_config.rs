//! M9.8er differential harness: external `/.api/` src-cli version/download,
//! release cache (disabled by default), client-config and modelconfig,
//! mirroring `cmd/frontend/internal/httpapi/zz_external_api_srccli_config_harness_test.go`.
//!
//! Prints one `cc ` line per case with the status, `Location`,
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
        "cc {name} status={status} location={location} ctype={ctype} xcto={xcto} body={body:?}"
    );
}

#[tokio::test]
async fn external_api_srccli_config_harness() {
    if std::env::var("SG_EXTERNAL_API_SRCCLI_CONFIG_HARNESS").as_deref() != Ok("1") {
        return;
    }
    let live = live();
    let cases = [
        ("versions-get", "GET", "/.api/src-cli/versions/5.5"),
        ("versions-post", "POST", "/.api/src-cli/versions/5.5"),
        ("versions-root", "GET", "/.api/src-cli/versions/"),
        ("versions-put", "PUT", "/.api/src-cli/versions/5.5"),
        ("versions-no-slash", "GET", "/.api/src-cli/versions"),
        ("version-get", "GET", "/.api/src-cli/version"),
        ("version-post", "POST", "/.api/src-cli/version"),
        ("version-slash", "GET", "/.api/src-cli/version/"),
        ("download-get", "GET", "/.api/src-cli/src_linux_amd64"),
        ("download-head", "HEAD", "/.api/src-cli/src_linux_amd64"),
        (
            "download-windows",
            "GET",
            "/.api/src-cli/src_windows_arm64.exe",
        ),
        ("download-post", "POST", "/.api/src-cli/src_linux_amd64"),
        ("srccli-invalid", "GET", "/.api/src-cli/nope"),
        ("srccli-empty", "GET", "/.api/src-cli/"),
        ("srccli-no-slash", "GET", "/.api/src-cli"),
        ("client-config-get", "GET", "/.api/client-config"),
        ("client-config-post", "POST", "/.api/client-config"),
        (
            "modelconfig-get",
            "GET",
            "/.api/modelconfig/supported-models.json",
        ),
        (
            "modelconfig-post",
            "POST",
            "/.api/modelconfig/supported-models.json",
        ),
        ("modelconfig-other", "GET", "/.api/modelconfig/other.json"),
    ];
    for (name, method, target) in cases {
        run(&live, name, method, target).await;
    }
}
