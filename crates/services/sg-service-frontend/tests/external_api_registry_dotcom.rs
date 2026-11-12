//! M9.19c differential harness: the dotcom extension registry
//! (`cmd/frontend/internal/registry/http_api.go`).
//!
//! The Go oracle (`cmd/frontend/internal/registry/zz_m919c_registry_oracle_test.go`,
//! UNTRACKED, gated by `SG_M919C_REGISTRY=1`) drives the real `HandleRegistry`
//! with the embedded frozen data. This harness injects the same file through
//! [`StaticRegistrySource`] (`M919C_FROZEN_REGISTRY`) and emits the same JSON
//! document.

use http::{header, Request};
use http_body_util::BodyExt;
use serde::Deserialize;
use serde_json::json;
use sg_service_frontend::external_api::{
    registry_response_with, RegistryExtension, StaticRegistrySource,
};

#[derive(Deserialize)]
struct Corpus {
    accept: String,
    cases: Vec<Case>,
}

#[derive(Deserialize)]
struct Case {
    name: String,
    path: String,
    accept: Option<String>,
    dotcom: bool,
}

fn header_value<B>(response: &http::Response<B>, name: &str) -> String {
    response
        .headers()
        .get(name)
        .and_then(|value| value.to_str().ok())
        .unwrap_or("")
        .to_owned()
}

#[tokio::test]
async fn registry_dotcom_diff() {
    let (Ok(corpus_path), Ok(out_path), Ok(frozen_path)) = (
        std::env::var("M919C_REGISTRY_CORPUS"),
        std::env::var("M919C_REGISTRY_OUT"),
        std::env::var("M919C_FROZEN_REGISTRY"),
    ) else {
        eprintln!("M919C_REGISTRY_CORPUS/M919C_REGISTRY_OUT/M919C_FROZEN_REGISTRY not set; skipping");
        return;
    };
    let corpus: Corpus =
        serde_json::from_str(&std::fs::read_to_string(&corpus_path).expect("read corpus"))
            .expect("parse corpus");
    let extensions: Vec<RegistryExtension> = serde_json::from_str(
        &std::fs::read_to_string(&frozen_path).expect("read frozen registry"),
    )
    .expect("parse frozen registry");

    let mut results = Vec::new();
    for case in &corpus.cases {
        let source = StaticRegistrySource {
            dotcom: case.dotcom,
            extensions: extensions.clone(),
        };
        let accept = case.accept.clone().unwrap_or_else(|| corpus.accept.clone());
        let mut builder = Request::builder()
            .method("GET")
            .uri(case.path.as_str());
        if !accept.is_empty() {
            builder = builder.header(header::ACCEPT, accept);
        }
        let request = builder.body(()).expect("request");
        let response = registry_response_with(&request, &source);
        let status = response.status().as_u16();
        let content_type = header_value(&response, "content-type");
        let cache_control = header_value(&response, "cache-control");
        let vary = header_value(&response, "vary");
        let media_type = header_value(&response, "x-sourcegraph-media-type");
        let body = response
            .into_body()
            .collect()
            .await
            .expect("body")
            .to_bytes();
        results.push(json!({
            "name": case.name,
            "status": status,
            "content_type": content_type,
            "cache_control": cache_control,
            "vary": vary,
            "media_type": media_type,
            "body": String::from_utf8_lossy(&body),
        }));
    }

    std::fs::write(&out_path, format!("{}\n", json!(results))).expect("write output");
}
