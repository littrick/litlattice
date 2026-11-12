//! M9.8ey differential harness: the external/internal GraphQL HTTP transport,
//! mirroring `cmd/frontend/internal/httpapi/zz_graphql_harness_test.go`.
//!
//! Reads the shared corpus (`GRAPHQL_HTTP_CORPUS`) and prints one `gql ` line
//! per case with the status, `Content-Type`, `Cache-Control`,
//! `X-Content-Type-Options`, `Retry-After`, `Location` and base64 body. Schema
//! execution is scripted from each case's `response` field so the transport
//! byte contract is compared without a GraphQL executor; the Go side runs the
//! real graph-gophers schema.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use base64::Engine;
use flate2::write::GzEncoder;
use flate2::Compression;
use http::{header, Method, Request};
use http_body_util::{BodyExt, Full};
use serde::Deserialize;
use serde_json::Value;
use sg_config::{LiveConfig, ServiceConnections, SiteConfiguration, Unified};
use sg_service_frontend::blame::{
    BlameDeps, BlameHunkUser, BlameStream, BlameStreamError, RepoError, RepoInfo, RevError,
    UserError,
};
use sg_service_frontend::external_api::api_response_with_deps;
use sg_service_frontend::graphql_api::{
    GraphqlDeps, GraphqlExecRequest, GraphqlRateLimitRequest, GraphqlResponse, QueryError,
    RateLimitOutcome,
};
use sg_service_frontend::internal_api::internal_response_with_deps;
use sg_service_frontend::search_stream::{SearchDeps, UnavailableDeps};

#[derive(Debug, Deserialize)]
struct Case {
    name: String,
    #[serde(default)]
    internal: bool,
    #[serde(default)]
    method: String,
    #[serde(default)]
    path: String,
    #[serde(default)]
    headers: HashMap<String, String>,
    #[serde(default)]
    body: String,
    #[serde(default, rename = "bodyBase64")]
    body_base64: String,
    #[serde(default)]
    gzip: bool,
    #[serde(default, rename = "externalURL")]
    external_url: Option<String>,
    #[serde(default, rename = "rateLimits")]
    rate_limits: Option<Value>,
    #[serde(default)]
    limited: Option<Limited>,
    #[serde(default)]
    response: Option<Value>,
}

#[derive(Debug, Deserialize)]
struct Limited {
    #[serde(rename = "retryAfter")]
    retry_after: i64,
}

struct CaseDeps {
    response: Option<GraphqlResponse>,
    limited: Option<i64>,
}

#[async_trait]
impl GraphqlDeps for CaseDeps {
    async fn exec(&self, _request: GraphqlExecRequest<'_>) -> GraphqlResponse {
        self.response.clone().unwrap_or_else(|| GraphqlResponse {
            errors: vec![QueryError::new("harness: unexpected schema exec")],
            ..GraphqlResponse::default()
        })
    }

    async fn rate_limit(&self, _request: GraphqlRateLimitRequest<'_>) -> Option<RateLimitOutcome> {
        self.limited
            .map(|retry_after_seconds| RateLimitOutcome::Limited {
                retry_after_seconds,
            })
    }
}

/// `response` keeps `data: null` distinct from an absent field.
fn graphql_response(value: &Value) -> GraphqlResponse {
    let mut response = GraphqlResponse::default();
    if let Some(errors) = value.get("errors") {
        response.errors = serde_json::from_value(errors.clone()).expect("errors");
    }
    if let Some(data) = value.get("data") {
        // The baseline embeds the executor's `json.RawMessage`; serialize the
        // corpus value with the same Go-compatible writer so escaping and
        // object key order are compared too.
        response.data = Some(
            sg_graphql::Json::from_serde(data)
                .expect("data must be representable")
                .to_compact_string(),
        );
    }
    if let Some(extensions) = value.get("extensions") {
        response.extensions = serde_json::from_value(extensions.clone()).ok();
    }
    response
}

struct NoBlame;

#[async_trait]
impl BlameDeps for NoBlame {
    async fn get_repo(&self, _name: &str) -> Result<RepoInfo, RepoError> {
        panic!("GraphQL corpus must not resolve a repo")
    }

    async fn resolve_rev(&self, _repo: &str, _rev: &str) -> Result<String, RevError> {
        panic!("GraphQL corpus must not resolve a rev")
    }

    async fn stream_blame(
        &self,
        _repo: &str,
        _path: &str,
        _newest_commit: &str,
    ) -> Result<BlameStream, BlameStreamError> {
        panic!("GraphQL corpus must not stream blame")
    }

    async fn user_by_verified_email(
        &self,
        _email: &str,
    ) -> Result<Option<BlameHunkUser>, UserError> {
        panic!("GraphQL corpus must not look up users")
    }
}

fn live(case: &Case) -> LiveConfig {
    let mut site = serde_json::Map::new();
    if let Some(external_url) = &case.external_url {
        site.insert(
            "externalURL".to_owned(),
            Value::String(external_url.clone()),
        );
    }
    if let Some(rate_limits) = &case.rate_limits {
        site.insert("rateLimits".to_owned(), rate_limits.clone());
    }
    let site = SiteConfiguration::parse(&Value::Object(site).to_string()).expect("site");
    LiveConfig::new(Unified {
        site,
        service_connections: ServiceConnections::default(),
    })
}

fn body(case: &Case) -> Vec<u8> {
    let raw = if case.body_base64.is_empty() {
        case.body.as_bytes().to_vec()
    } else {
        base64::engine::general_purpose::STANDARD
            .decode(&case.body_base64)
            .expect("bodyBase64")
    };
    if !case.gzip {
        return raw;
    }
    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    std::io::Write::write_all(&mut encoder, &raw).expect("gzip");
    encoder.finish().expect("gzip")
}

fn header_field(headers: &http::HeaderMap, name: header::HeaderName) -> String {
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned)
        .unwrap_or_else(|| "-".to_owned())
}

#[tokio::test]
async fn graphql_http_harness() {
    if std::env::var("SG_GRAPHQL_HTTP_HARNESS").as_deref() != Ok("1") {
        return;
    }
    let corpus = std::env::var("GRAPHQL_HTTP_CORPUS").expect("GRAPHQL_HTTP_CORPUS");
    let data = std::fs::read(corpus).expect("corpus");
    let cases: Vec<Case> = serde_json::from_slice(&data).expect("corpus json");

    for case in &cases {
        let method = if case.method.is_empty() {
            Method::POST
        } else {
            Method::from_bytes(case.method.as_bytes()).expect("method")
        };
        let path = if case.path.is_empty() {
            if case.internal {
                "/graphql".to_owned()
            } else {
                "/.api/graphql".to_owned()
            }
        } else {
            case.path.clone()
        };

        let mut builder = Request::builder().method(method).uri(&path);
        for (name, value) in &case.headers {
            builder = builder.header(name, value);
        }
        if case.gzip {
            builder = builder.header(header::CONTENT_ENCODING, "gzip");
        }
        let mut request = builder
            .body(Full::new(bytes::Bytes::from(body(case))))
            .expect("request");

        let deps: Arc<dyn GraphqlDeps> = Arc::new(CaseDeps {
            response: case.response.as_ref().map(graphql_response),
            limited: case.limited.as_ref().map(|limited| limited.retry_after),
        });
        let live = live(case);
        let search: Arc<dyn SearchDeps> = Arc::new(UnavailableDeps);
        let response = if case.internal {
            internal_response_with_deps(&mut request, &live, deps, search).await
        } else {
            let blame: Arc<dyn BlameDeps> = Arc::new(NoBlame);
            api_response_with_deps(
                &mut request,
                &live,
                blame,
                search,
                deps,
                Arc::new(sg_service_frontend::repo_refresh::UnavailableRepoRefreshDeps),
                Arc::new(sg_service_frontend::llm_api::LlmDeps::unavailable()),
            )
            .await
        };

        let status = response.status().as_u16();
        let headers = response.headers().clone();
        let raw = response
            .into_body()
            .collect()
            .await
            .expect("body")
            .to_bytes();
        let encoded = base64::engine::general_purpose::STANDARD.encode(raw);
        println!(
            "gql {} status={} ct={} cc={} nosniff={} retry={} loc={} body={}",
            case.name,
            status,
            header_field(&headers, header::CONTENT_TYPE),
            header_field(&headers, header::CACHE_CONTROL),
            header_field(&headers, header::X_CONTENT_TYPE_OPTIONS),
            header_field(&headers, header::RETRY_AFTER),
            header_field(&headers, header::LOCATION),
            encoded,
        );
    }
}
