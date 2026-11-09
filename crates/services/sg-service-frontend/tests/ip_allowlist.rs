//! M9.8dy differential harness: the Rust side of
//! `test_ws/p9-go/run_ip_allowlist_diff.sh`.
//!
//! Prints the canonical `ipal` lines produced by the real Go harnesses
//! (`TestIpAllowlistCheckerHarness` for configuration/decisions and
//! `TestIpAllowlistMiddleware{Default,Template}Page` for the 403 page).

use bytes::Bytes;
use http::{header, Request, Response, StatusCode};
use http_body_util::{BodyExt, Full};
use sg_config::SiteConfiguration;
use sg_http::RemoteAddr;
use sg_service_frontend::ipallowlist::{self, AllowlistEnv, IpAllowlistConfig};
use tonic::body::BoxBody;

fn site(json: &str) -> SiteConfiguration {
    SiteConfiguration::parse(json).expect("site config")
}

fn request(remote: &str, headers: &[(&str, &str)]) -> Request<()> {
    let mut builder = Request::builder().uri("/");
    for (name, value) in headers {
        builder = builder.header(*name, *value);
    }
    let mut request = builder.body(()).expect("request");
    request
        .extensions_mut()
        .insert(RemoteAddr(remote.to_owned()));
    request
}

fn joined(mut values: Vec<String>) -> String {
    if values.is_empty() {
        return "-".to_owned();
    }
    values.sort();
    values.join(";")
}

fn print_config(name: &str, config: &IpAllowlistConfig) {
    let user = joined(
        config
            .authorized_user_ips
            .iter()
            .map(ToString::to_string)
            .collect(),
    );
    let user_ranges = joined(
        config
            .authorized_user_ranges
            .iter()
            .map(ipallowlist::IpNet::network_string)
            .collect(),
    );
    let client = joined(
        config
            .authorized_client_ips
            .iter()
            .map(ToString::to_string)
            .collect(),
    );
    let client_ranges = joined(
        config
            .authorized_client_ranges
            .iter()
            .map(ipallowlist::IpNet::network_string)
            .collect(),
    );
    let trusted = joined(
        config
            .trusted_client_ips
            .iter()
            .map(ToString::to_string)
            .collect(),
    );
    let trusted_ranges = joined(
        config
            .trusted_client_ranges
            .iter()
            .map(ipallowlist::IpNet::network_string)
            .collect(),
    );
    let headers = joined(config.user_headers.clone());
    let template = if config.error_message_template.is_some() {
        "yes"
    } else {
        "-"
    };
    let error = config
        .error
        .as_deref()
        .map(|error| format!("{error:?}"))
        .unwrap_or_else(|| "-".to_owned());
    println!(
        "ipal cfg {name} user={user} urange={user_ranges} client={client} crange={client_ranges} trusted={trusted} trange={trusted_ranges} headers={headers} tmpl={template} err={error}"
    );
}

/// A `(header, value)` pair and a `(case, remote addr, headers)` case.
type HeaderCase<'a> = (&'a str, &'a str);
type RequestCase<'a> = (&'a str, &'a str, &'a [HeaderCase<'a>]);

fn print_requests(group: &str, config: &IpAllowlistConfig, cases: &[RequestCase<'_>]) {
    for (name, remote, headers) in cases {
        match config.is_authorized(&request(remote, headers)) {
            Ok(()) => println!("ipal req {group}/{name} ok"),
            Err(error) => println!("ipal req {group}/{name} err={error:?}"),
        }
    }
}

fn compile(site: &SiteConfiguration, env: AllowlistEnv) -> IpAllowlistConfig {
    ipallowlist::compile(site, &env).expect("allowlist enabled")
}

fn next_response() -> Response<BoxBody> {
    let mut response = Response::new(tonic::body::boxed(Full::new(Bytes::from_static(b"next"))));
    *response.status_mut() = StatusCode::OK;
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        header::HeaderValue::from_static("text/plain; charset=utf-8"),
    );
    response
}

async fn print_http(group: &str, config: &IpAllowlistConfig) {
    let cases = [
        ("allowed", "10.1.2.3:1234", "/healthz"),
        ("denied", "1.2.3.4:1234", "/healthz"),
        ("denied-other-path", "1.2.3.4:1234", "/"),
    ];
    for (name, remote, path) in cases {
        let request = Request::builder()
            .uri(path)
            .body(())
            .map(|mut request| {
                request
                    .extensions_mut()
                    .insert(RemoteAddr(remote.to_owned()));
                request
            })
            .expect("request");
        // The Go harness wraps a dummy `next` with `Middleware.Handle`; it
        // does not add BlackHole/healthCheck.
        let response = match config.is_authorized(&request) {
            Ok(()) => next_response(),
            Err(error) => ipallowlist::forbidden_response(&error),
        };
        let status = response.status().as_u16();
        let body = response
            .into_body()
            .collect()
            .await
            .expect("body")
            .to_bytes();
        let body = String::from_utf8_lossy(&body);
        println!("ipal http {group}/{name} {status} {body:?}");
    }
}

#[tokio::test]
async fn ip_allowlist_harness() {
    // matrix
    let config = compile(
        &site(
            r#"{"auth.allowedIpAddress":{"enabled":true,"clientIpAddress":["10.0.0.0/8","1.2.3.4"],"trustedClientIpAddress":["172.16.0.0/12"],"userIpAddress":["10.0.0.1","203.0.113.0/24","2001:db8::/32"],"userIpRequestHeaders":["X-Forwarded-For","X-Real-IP"]}}"#,
        ),
        AllowlistEnv::default(),
    );
    print_config("matrix", &config);
    print_requests(
        "matrix",
        &config,
        &[
            (
                "client-and-user-allowed",
                "10.9.9.9:1",
                &[("X-Forwarded-For", "203.0.113.7")],
            ),
            ("user-not-allowed", "10.9.9.9:1", &[]),
            ("trusted-bypasses", "172.16.5.5:1", &[]),
            (
                "trusted-bypasses-with-header",
                "172.16.5.5:1",
                &[("X-Forwarded-For", "8.8.8.8")],
            ),
            ("client-ip-allowed-user-denied", "1.2.3.4:1", &[]),
            ("localhost-trusted", "127.0.0.1:1", &[]),
            (
                "left-most-header-value-wins",
                "192.168.1.1:1",
                &[("X-Forwarded-For", "203.0.113.7, 8.8.8.8")],
            ),
            (
                "first-header-empty",
                "192.168.1.1:1",
                &[("X-Forwarded-For", ", 203.0.113.7")],
            ),
            (
                "second-header-used",
                "192.168.1.1:1",
                &[("X-Real-IP", "203.0.113.7")],
            ),
            ("ipv6-user-allowed", "[2001:db8::1]:80", &[]),
            (
                "invalid-user-header",
                "192.168.1.1:1",
                &[("X-Forwarded-For", "not-an-ip")],
            ),
            ("invalid-remote", "bad:1", &[]),
            ("missing-port", "1.2.3.4", &[]),
            ("too-many-colons", "::1:80", &[]),
        ],
    );

    // env
    let config = compile(
        &site(
            r#"{"auth.allowedIpAddress":{"enabled":true,"clientIpAddress":["1.1.1.1"],"userIpAddress":["10.0.0.1"],"userIpRequestHeaders":["X-Forwarded-For"]}}"#,
        ),
        AllowlistEnv {
            stable_client_ip_allowlist: vec!["192.168.0.0/16".to_owned(), "10.10.10.10".to_owned()],
            stable_trusted_client_ip_allowlist: vec!["10.20.0.0/16".to_owned()],
            user_ip_request_headers_override: vec!["CF-Connecting-IP".to_owned()],
            ..AllowlistEnv::default()
        },
    );
    print_config("env", &config);
    print_requests(
        "env",
        &config,
        &[
            (
                "stable-client-range",
                "192.168.4.4:1",
                &[("CF-Connecting-IP", "10.0.0.1")],
            ),
            (
                "override-header-used",
                "8.8.8.8:1",
                &[("CF-Connecting-IP", "10.0.0.1")],
            ),
            (
                "config-header-ignored",
                "1.1.1.1:1",
                &[("X-Forwarded-For", "10.0.0.1")],
            ),
            ("stable-trusted", "10.20.1.2:1", &[]),
        ],
    );

    // invalid
    let config = compile(
        &site(
            r#"{"auth.allowedIpAddress":{"enabled":true,"userIpAddress":["invalid-ip","10.0.0.1"],"clientIpAddress":["10.0.0.0/33"]}}"#,
        ),
        AllowlistEnv::default(),
    );
    print_config("invalid", &config);
    print_requests(
        "invalid",
        &config,
        &[
            ("denied-by-remaining-user-rule", "1.2.3.4:1", &[]),
            ("allowed", "1.2.3.4:1", &[("X-Forwarded-For", "10.0.0.1")]),
        ],
    );

    // template
    let config = compile(
        &site(
            r#"{"auth.allowedIpAddress":{"enabled":true,"userIpAddress":["10.0.0.1"],"errorMessageTemplate":"Blocked {{.UserIP}}: {{.Error}}"}}"#,
        ),
        AllowlistEnv::default(),
    );
    print_config("template", &config);
    print_requests(
        "template",
        &config,
        &[("denied-with-template", "1.2.3.4:1", &[])],
    );

    // open
    let config = compile(
        &site(r#"{"auth.allowedIpAddress":{"enabled":true}}"#),
        AllowlistEnv::default(),
    );
    print_config("open", &config);
    print_requests("open", &config, &[("allowed", "1.2.3.4:1", &[])]);

    // middleware pages (each Go test function runs in its own process).
    let default_config = compile(
        &site(r#"{"auth.allowedIpAddress":{"enabled":true,"clientIpAddress":["10.0.0.0/8"]}}"#),
        AllowlistEnv::default(),
    );
    print_http("default", &default_config).await;

    let template_config = compile(
        &site(
            r#"{"auth.allowedIpAddress":{"enabled":true,"clientIpAddress":["10.0.0.0/8"],"errorMessageTemplate":"Blocked {{.UserIP}}: {{.Error}}"}}"#,
        ),
        AllowlistEnv::default(),
    );
    print_http("template", &template_config).await;
}
