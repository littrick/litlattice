//! M9.8eu differential harness: the external `/.api/repos/{Repo}/-/shield`
//! endpoint and the `routevar.Repo` gorilla template semantics, mirroring
//! `cmd/frontend/internal/httpapi/zz_repo_shield_harness_test.go`.
//!
//! Prints one `sh ` line per case with the status, `Location`,
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
        "sh {name} status={status} location={location} ctype={ctype} xcto={xcto} cache={cache} body={body:?}"
    );
}

#[tokio::test]
async fn external_api_repo_shield_harness() {
    if std::env::var("SG_REPO_SHIELD_HARNESS").as_deref() != Ok("1") {
        return;
    }
    let cases = [
        ("repo", "GET", "/.api/repos/github.com/foo/bar/-/shield"),
        ("single-segment", "GET", "/.api/repos/foo/-/shield"),
        ("dashed", "GET", "/.api/repos/foo-bar/-/shield"),
        ("double-dash", "GET", "/.api/repos/foo/--/bar/-/shield"),
        ("query", "GET", "/.api/repos/foo/-/shield?x=1"),
        ("encoded-slash", "GET", "/.api/repos/foo%2Fbar/-/shield"),
        ("trailing", "GET", "/.api/repos/foo/bar/-/shield/"),
        ("post", "POST", "/.api/repos/foo/-/shield"),
        ("head", "HEAD", "/.api/repos/foo/-/shield"),
        ("post-trailing", "POST", "/.api/repos/foo/-/shield/"),
        ("dash-component", "GET", "/.api/repos/-/-/shield"),
        ("at-component", "GET", "/.api/repos/foo@bar/-/shield"),
        ("empty-component", "GET", "/.api/repos//-/shield"),
        ("extra-segment", "GET", "/.api/repos/foo/-/shield/extra"),
        ("unknown", "GET", "/.api/repos/foo/-/other"),
        ("no-route", "GET", "/.api/nope"),
    ];
    for (name, method, target) in cases {
        run(name, method, target).await;
    }
}
