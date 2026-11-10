//! M9.8fy differential of `Query.clientConfiguration` against the Go
//! baseline (`cmd/frontend/graphqlbackend/client_configuration.go:36-95`).
//!
//! Both sides read the same `external_services` fixtures from a scratch
//! database and set the same site configuration per corpus case. Each case
//! writes `{contentScriptUrls (sorted), parentSourcegraphUrl, error}` with
//! `encoding/json`-compatible encoding, compared byte-for-byte.
//!
//! Env:
//!
//! * `SG_M9FY_DIFF=1`
//! * `M9FY_CORPUS` — JSON `[{name, siteConfig}]`
//! * `M9FY_OUT` — output JSON file
//! * `M9FY_DSN` — postgres DSN of the scratch database

#![allow(clippy::unwrap_used)]

use std::sync::Arc;

use serde::Deserialize;
use serde_json::{json, Value};
use sqlx::PgPool;

use sg_config::SiteConfiguration;
use sg_encryption::Key;
use sg_service_frontend::client_configuration::{
    client_configuration, parent_sourcegraph_url, PgExternalServiceConfigSource,
};
use sg_service_frontend::settings_merge::marshal_go;

#[derive(Deserialize)]
struct Case {
    #[allow(dead_code)]
    name: String,
    #[serde(default, rename = "siteConfig")]
    site_config: Value,
}

#[derive(Deserialize)]
struct Corpus {
    cases: Vec<Case>,
}

#[tokio::test]
async fn client_configuration_matches_the_go_baseline() {
    if std::env::var("SG_M9FY_DIFF").as_deref() != Ok("1") {
        return;
    }
    let corpus_path = std::env::var("M9FY_CORPUS").expect("M9FY_CORPUS");
    let out = std::env::var("M9FY_OUT").expect("M9FY_OUT");
    let dsn = std::env::var("M9FY_DSN").expect("M9FY_DSN");

    let corpus: Corpus =
        serde_json::from_str(&std::fs::read_to_string(corpus_path).expect("corpus"))
            .expect("parse corpus");
    let pool = PgPool::connect(&dsn).await.expect("connect");
    let source = PgExternalServiceConfigSource::new(pool);
    // The encrypted fixture uses the `noop` key.
    let key: Arc<dyn Key> = Arc::new(sg_encryption::NoopKey::default());

    let mut outputs = Vec::with_capacity(corpus.cases.len());
    for case in &corpus.cases {
        let site = SiteConfiguration::parse(&case.site_config.to_string()).expect("site config");
        let entry = match client_configuration(
            &source,
            Some(Arc::clone(&key)),
            parent_sourcegraph_url(&site),
        )
        .await
        {
            Ok(details) => {
                let mut urls = details.content_script_urls.clone();
                urls.sort();
                json!({
                    "contentScriptUrls": urls,
                    "parentSourcegraphUrl": details.parent_sourcegraph_url,
                    "error": "",
                })
            }
            Err(message) => json!({
                "contentScriptUrls": Vec::<String>::new(),
                "parentSourcegraphUrl": "",
                "error": message,
            }),
        };
        outputs.push(entry);
    }

    let encoded = marshal_go(&Value::Array(outputs));
    std::fs::write(out, &encoded).expect("write");
    println!("RUST client-configuration len={}", encoded.len());
}
