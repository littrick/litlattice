//! M9.8dz differential harness: mirrors
//! `cmd/frontend/internal/cli/zz_frontend_mux_harness_test.go` — the same mux
//! wiring with marker sub-handlers and the same normalized `mz ` lines.

use http::header::{self, HeaderMap};
use http::{Method, Request, Response, StatusCode};
use http_body_util::BodyExt;
use sg_config::SiteConfiguration;
use sg_service_frontend::middleware;
use sg_service_frontend::mux::{self, Route, SecurityConfig};
use tonic::body::BoxBody;

fn request(method: Method, target: &str, origin: Option<&str>) -> Request<()> {
    let mut builder = Request::builder().method(method).uri(target);
    if let Some(origin) = origin {
        builder = builder.header(header::ORIGIN, origin);
    }
    builder.body(()).expect("request")
}

fn dispatch(route: Route) -> Response<BoxBody> {
    let body = match route {
        Route::Api => "api".to_owned(),
        Route::Executors => "executors".to_owned(),
        Route::App => "app".to_owned(),
        Route::Assets { path } => format!("asset:{path}"),
    };
    middleware::text_response(
        StatusCode::OK,
        "text/plain; charset=utf-8",
        body.into_bytes(),
    )
}

fn header_value(headers: &HeaderMap, name: &str) -> String {
    match headers.get(name) {
        None => "-".to_owned(),
        Some(value) => format!("{:?}", value.to_str().unwrap_or("<binary>")),
    }
}

struct Case<'a> {
    name: &'a str,
    method: Method,
    target: &'a str,
    origin: Option<&'a str>,
}

async fn run(group: &str, config: &SecurityConfig, cases: &[Case<'_>]) {
    for case in cases {
        let req = request(case.method.clone(), case.target, case.origin);
        let response = mux::route(&req, config, dispatch);
        let status = response.status().as_u16();
        let headers = response.headers();
        let fields = [
            header_value(headers, "location"),
            header_value(headers, "content-type"),
            header_value(headers, "x-content-type-options"),
            header_value(headers, "x-xss-protection"),
            header_value(headers, "x-frame-options"),
            header_value(headers, "cache-control"),
            header_value(headers, "access-control-allow-credentials"),
            header_value(headers, "access-control-allow-origin"),
            header_value(headers, "access-control-allow-methods"),
            header_value(headers, "access-control-allow-headers"),
        ];
        let body = response
            .into_body()
            .collect()
            .await
            .expect("body")
            .to_bytes();
        let body = String::from_utf8_lossy(&body);
        println!(
            "mz {group}/{} status={status} location={} ctype={} xcto={} xxss={} xfo={} cache={} acac={} acao={} acam={} acah={} body={:?}",
            case.name,
            fields[0],
            fields[1],
            fields[2],
            fields[3],
            fields[4],
            fields[5],
            fields[6],
            fields[7],
            fields[8],
            fields[9],
            body.as_ref(),
        );
    }
}

fn config(json: &str) -> SecurityConfig {
    SecurityConfig::from_site_config(&SiteConfiguration::parse(json).expect("site config"))
}

#[tokio::test]
async fn frontend_mux_harness() {
    let default = SecurityConfig::from_site_config(&SiteConfiguration::default());
    run(
        "default",
        &default,
        &[
            Case {
                name: "api-get",
                method: Method::GET,
                target: "/.api/graphql",
                origin: None,
            },
            Case {
                name: "api-post-origin",
                method: Method::POST,
                target: "/.api/graphql",
                origin: Some("https://evil.example"),
            },
            Case {
                name: "api-options-untrusted",
                method: Method::OPTIONS,
                target: "/.api/graphql",
                origin: Some("https://evil.example"),
            },
            Case {
                name: "executors-get-origin",
                method: Method::GET,
                target: "/.executors/run",
                origin: Some("https://evil.example"),
            },
            Case {
                name: "executors-options",
                method: Method::OPTIONS,
                target: "/.executors/run",
                origin: Some("https://evil.example"),
            },
            Case {
                name: "assets-extension",
                method: Method::GET,
                target: "/.assets/extension/a.js",
                origin: None,
            },
            Case {
                name: "assets-extension-options",
                method: Method::OPTIONS,
                target: "/.assets/extension/a.js",
                origin: Some("https://evil.example"),
            },
            Case {
                name: "assets-other",
                method: Method::GET,
                target: "/.assets/other/a.js",
                origin: None,
            },
            Case {
                name: "assets-other-options",
                method: Method::OPTIONS,
                target: "/.assets/other/a.js",
                origin: Some("https://evil.example"),
            },
            Case {
                name: "app-root",
                method: Method::GET,
                target: "/",
                origin: None,
            },
            Case {
                name: "app-tree",
                method: Method::GET,
                target: "/tree",
                origin: None,
            },
            Case {
                name: "api-trailing-slash-redirect",
                method: Method::GET,
                target: "/.api",
                origin: None,
            },
            Case {
                name: "executors-trailing-slash-redirect",
                method: Method::GET,
                target: "/.executors",
                origin: None,
            },
            Case {
                name: "assets-trailing-slash-redirect",
                method: Method::GET,
                target: "/.assets",
                origin: None,
            },
            Case {
                name: "api-post-redirect",
                method: Method::POST,
                target: "/.api",
                origin: None,
            },
            Case {
                name: "clean-path-redirect",
                method: Method::GET,
                target: "/foo/../bar?x=1",
                origin: None,
            },
            Case {
                name: "clean-path-redirect-subtree",
                method: Method::GET,
                target: "/foo/../.api?x=1",
                origin: None,
            },
            Case {
                name: "double-slash-redirect",
                method: Method::GET,
                target: "/a//b",
                origin: None,
            },
        ],
    )
    .await;

    let origins = config(
        r#"{"externalURL":"https://sourcegraph.example.com/","corsOrigin":"https://a.example"}"#,
    );
    run(
        "origins",
        &origins,
        &[
            Case {
                name: "api-options-allowlisted",
                method: Method::OPTIONS,
                target: "/.api/graphql",
                origin: Some("https://a.example"),
            },
            Case {
                name: "api-options-external-url",
                method: Method::OPTIONS,
                target: "/.api/graphql",
                origin: Some("https://sourcegraph.example.com"),
            },
            Case {
                name: "api-options-dev-extension",
                method: Method::OPTIONS,
                target: "/.api/graphql",
                origin: Some(mux::DEV_EXTENSION),
            },
            Case {
                name: "api-options-prod-extension",
                method: Method::OPTIONS,
                target: "/.api/graphql",
                origin: Some(mux::PROD_EXTENSION),
            },
            Case {
                name: "api-options-external-url-trailing-slash",
                method: Method::OPTIONS,
                target: "/.api/graphql",
                origin: Some("https://sourcegraph.example.com/"),
            },
            Case {
                name: "api-options-unlisted",
                method: Method::OPTIONS,
                target: "/.api/graphql",
                origin: Some("https://b.example"),
            },
            Case {
                name: "api-get-allowlisted-echo",
                method: Method::GET,
                target: "/.api/graphql",
                origin: Some("https://a.example"),
            },
        ],
    )
    .await;

    let star = config(r#"{"corsOrigin":"*"}"#);
    run(
        "star",
        &star,
        &[Case {
            name: "api-options-anything",
            method: Method::OPTIONS,
            target: "/.api/graphql",
            origin: Some("https://anything.example"),
        }],
    )
    .await;
}
