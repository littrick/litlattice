//! M9.8gf differential harness: the internal listener's `GET /search/stream`.
//! Mirrors
//! `cmd/frontend/internal/httpapi/zz_m9gf_internal_search_stream_harness_test.go`,
//! which registers the real `RegisterInternalServices` mux.
//!
//! Prints one `is ` line per case with the status, selected headers and the
//! (gunzipped) body. The cases only exercise the deterministic pre-search
//! paths (argument errors and routing), so `UnavailableDeps` is never called.

use std::io::Read;
use std::sync::Arc;

use bytes::Bytes;
use http::{header, Method, Request};
use http_body_util::{BodyExt, Full};
use sg_config::{LiveConfig, ServiceConnections, SiteConfiguration, Unified};
use sg_service_frontend::graphql_api::UnportedGraphqlDeps;
use sg_service_frontend::internal_api::internal_response_with_deps;
use sg_service_frontend::search_stream::{SearchDeps, UnavailableDeps};

fn live() -> LiveConfig {
    let site = SiteConfiguration::parse("{}").expect("site");
    let service_connections = ServiceConnections {
        git_servers: vec!["gitserver-0:3178".to_owned()],
        ..ServiceConnections::default()
    };
    LiveConfig::new(Unified {
        site,
        service_connections,
    })
}

fn field(headers: &http::HeaderMap, name: &str) -> String {
    match headers.get(name) {
        None => "-".to_owned(),
        Some(value) => format!("{value:?}"),
    }
}

fn gunzip(body: &[u8]) -> Vec<u8> {
    let mut decoder = flate2::read::GzDecoder::new(body);
    let mut out = Vec::new();
    decoder.read_to_end(&mut out).expect("gunzip");
    out
}

struct Case {
    name: &'static str,
    method: Method,
    target: &'static str,
    accept: Option<&'static str>,
}

fn cases() -> Vec<Case> {
    vec![
        Case {
            name: "stream-noquery",
            method: Method::GET,
            target: "/search/stream",
            accept: None,
        },
        Case {
            name: "stream-gzip",
            method: Method::GET,
            target: "/search/stream?q=",
            accept: Some("gzip"),
        },
        Case {
            name: "stream-display",
            method: Method::GET,
            target: "/search/stream?q=a&display=x",
            accept: None,
        },
        Case {
            name: "stream-trailing",
            method: Method::GET,
            target: "/search/stream/",
            accept: None,
        },
        Case {
            name: "stream-trailing-query",
            method: Method::GET,
            target: "/search/stream/?q=a",
            accept: None,
        },
        Case {
            name: "stream-post",
            method: Method::POST,
            target: "/search/stream",
            accept: None,
        },
        Case {
            name: "stream-trailing-post",
            method: Method::POST,
            target: "/search/stream/",
            accept: None,
        },
        Case {
            name: "stream-unknown",
            method: Method::GET,
            target: "/search/unknown",
            accept: None,
        },
    ]
}

#[tokio::test]
async fn internal_search_stream_harness() {
    if std::env::var("SG_M9GF_DIFF").as_deref() != Ok("1") {
        return;
    }
    let live = live();
    for case in cases() {
        let mut builder = Request::builder().method(case.method).uri(case.target);
        if let Some(accept) = case.accept {
            builder = builder.header(header::ACCEPT_ENCODING, accept);
        }
        let mut request = builder.body(Full::new(Bytes::new())).expect("request");
        let graphql_deps = Arc::new(UnportedGraphqlDeps);
        let search_deps: Arc<dyn SearchDeps> = Arc::new(UnavailableDeps);
        let response =
            internal_response_with_deps(&mut request, &live, graphql_deps, search_deps).await;
        let status = response.status().as_u16();
        let headers = response.headers().clone();
        let body = response
            .into_body()
            .collect()
            .await
            .expect("body")
            .to_bytes();
        let body = if headers.get(header::CONTENT_ENCODING).is_some() {
            gunzip(&body)
        } else {
            body.to_vec()
        };
        let body = String::from_utf8_lossy(&body);
        println!(
            "is {} status={} location={} ctype={} cache={} conn={} te={} xab={} vary={} ce={} body={body:?}",
            case.name,
            status,
            field(&headers, "location"),
            field(&headers, "content-type"),
            field(&headers, "cache-control"),
            field(&headers, "connection"),
            field(&headers, "transfer-encoding"),
            field(&headers, "x-accel-buffering"),
            field(&headers, "vary"),
            field(&headers, "content-encoding"),
        );
    }
}
