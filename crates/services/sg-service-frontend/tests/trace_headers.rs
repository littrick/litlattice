//! M9.8ed differential harness: prints the same `tu`/`th` lines as the Go
//! `internal/tracer` and `cmd/frontend/internal/cli` trace harnesses.
//!
//! `tu` compares the URL renderer against `trace.URL` + `setupTraceURL`;
//! `th` drives the production chain segment (tenant succeeded -> ForbidAll ->
//! next) with random IDs normalized to `<traceid>`/`<spanid>`, mirroring the
//! fixed URL renderer registered by the Go harness. The next handler returns
//! 204 with no body so the Go recorder does not sniff a Content-Type that
//! hyper would not produce.

use bytes::Bytes;
use http::{header, HeaderMap, Request, Response, StatusCode};
use http_body_util::{BodyExt, Full};
use sg_config::SiteConfiguration;
use sg_service_frontend::auth;
use sg_service_frontend::middleware::external_response_with_ipallowlist;
use sg_service_frontend::trace_headers;

fn hdr(headers: &HeaderMap, name: &str) -> String {
    match headers.get(name) {
        Some(value) => format!("{:?}", value.to_str().unwrap_or("")),
        None => "-".to_owned(),
    }
}

#[test]
fn url_renderer_harness() {
    let cases = [
        ("none", "{}"),
        ("empty", r#"{"observability.tracing":{"urlTemplate":""}}"#),
        (
            "field",
            r#"{"observability.tracing":{"urlTemplate":"https://traces.example/{{ .TraceID }}"}}"#,
        ),
        (
            "both",
            r#"{"observability.tracing":{"urlTemplate":"{{.ExternalURL}}/trace/{{.TraceID}}"}}"#,
        ),
        (
            "trim",
            r#"{"observability.tracing":{"urlTemplate":"pre{{- .TraceID -}}post"}}"#,
        ),
        (
            "static",
            r#"{"observability.tracing":{"urlTemplate":"static"}}"#,
        ),
        (
            "unknown",
            r#"{"observability.tracing":{"urlTemplate":"{{ .Foo }}"}}"#,
        ),
        (
            "unclosed",
            r#"{"observability.tracing":{"urlTemplate":"{{ .TraceID"}}"#,
        ),
    ];
    for (name, json) in cases {
        let site = SiteConfiguration::parse(json).expect("site config");
        let url = trace_headers::render_url(
            trace_headers::url_template(&site).as_deref(),
            "abc123",
            "https://ext.example",
        );
        println!("tu {name} url={url:?}");
    }
}

async fn run_case(name: &str, path: &str, json: &str) -> String {
    let site = SiteConfiguration::parse(json).expect("site config");
    let template = trace_headers::url_template(&site);
    let mut req = Request::builder()
        .uri(format!("http://harness.invalid{path}"))
        .body(())
        .expect("request");
    let response = external_response_with_ipallowlist(
        &mut req,
        &Bytes::from_static(b"v"),
        None,
        auth::forbidden(&site),
        template.as_deref(),
        "https://ext.example",
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
    let trace = match response.headers().get(trace_headers::TRACE_HEADER) {
        Some(value) => {
            assert_eq!(value.to_str().unwrap().len(), 32);
            "<traceid>".to_owned()
        }
        None => "-".to_owned(),
    };
    let span = match response.headers().get(trace_headers::TRACE_SPAN_HEADER) {
        Some(value) => {
            assert_eq!(value.to_str().unwrap().len(), 16);
            "<spanid>".to_owned()
        }
        None => "-".to_owned(),
    };
    let url = match response.headers().get(trace_headers::TRACE_URL_HEADER) {
        Some(value) => {
            let raw = value.to_str().unwrap();
            let trace_id = response
                .headers()
                .get(trace_headers::TRACE_HEADER)
                .and_then(|value| value.to_str().ok())
                .unwrap_or("");
            format!("{:?}", raw.replace(trace_id, "<traceid>"))
        }
        None => "-".to_owned(),
    };
    // Drain the body so the response is fully materialized like the recorder.
    let _ = response.into_body().collect().await.expect("body");
    format!("th {name} path={path} status={status} ctype={ctype} xcto={xcto} trace={trace} span={span} url={url}")
}

#[tokio::test]
async fn trace_chain_harness() {
    let builtin = r#"{"auth.providers":[{"type":"builtin"}],"observability.tracing":{"urlTemplate":"https://traces.example/{{ .TraceID }}"}}"#;
    let none =
        r#"{"observability.tracing":{"urlTemplate":"https://traces.example/{{ .TraceID }}"}}"#;

    for (name, json) in [("builtin", builtin), ("none", none)] {
        for path in ["/", "/.api/graphql", "/some/path"] {
            println!("{}", run_case(name, path, json).await);
        }
    }
}
