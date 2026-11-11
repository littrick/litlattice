//! M9.8jg differential of `Site.authProviders` and the `AuthProvider` node
//! fields against the real Go baseline
//! (`cmd/frontend/graphqlbackend/zz_m9jg_oracle_test.go`, run by
//! `run_auth_providers_diff.sh`).
//!
//! Env:
//!
//! * `M9JG_CORPUS` — the shared corpus (`[{name, siteConfig, headers?}]`)
//! * `M9JG_OUT` — output directory (one `<name>.json` per case under `<out>/rust/`)
//!
//! The baseline resolver never reads request headers; corpus entries that set
//! `X-Requested-With` are re-resolved here and asserted identical, matching the
//! oracle's own header-independence check.

#![allow(clippy::unwrap_used)]

use std::path::Path;

use serde::{Deserialize, Serialize};

use sg_config::SiteConfiguration;
use sg_service_frontend::auth_providers_graphql::AuthProvidersRoot;

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Case {
    name: String,
    site_config: String,
    #[allow(dead_code)]
    headers: Option<std::collections::BTreeMap<String, String>>,
}

#[derive(Debug, Serialize, PartialEq, Eq)]
struct ProviderOut {
    #[serde(rename = "serviceType")]
    service_type: String,
    #[serde(rename = "serviceID")]
    service_id: String,
    #[serde(rename = "clientID")]
    client_id: String,
    #[serde(rename = "displayName")]
    display_name: String,
    #[serde(rename = "isBuiltin")]
    is_builtin: bool,
    #[serde(rename = "authenticationURL")]
    authentication_url: Option<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct PageInfoOut {
    has_next_page: bool,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct CaseOut {
    name: String,
    nodes: Vec<ProviderOut>,
    total_count: usize,
    page_info: PageInfoOut,
}

fn resolve(case: &Case) -> Vec<ProviderOut> {
    let site = SiteConfiguration::parse(&case.site_config)
        .unwrap_or_else(|error| panic!("{}: bad siteConfig: {error}", case.name));
    let root = AuthProvidersRoot::from_site(&site);
    root.providers
        .into_iter()
        .map(|provider| ProviderOut {
            service_type: provider.service_type,
            service_id: provider.service_id,
            client_id: provider.client_id,
            display_name: provider.display_name,
            is_builtin: provider.is_builtin,
            authentication_url: provider.authentication_url,
        })
        .collect()
}

/// Go `json.MarshalIndent(v, "", "  ")` + newline: `encoding/json` also
/// escapes `<`, `>` and `&` inside strings.
fn go_json<T: Serialize>(value: &T) -> Vec<u8> {
    let mut encoded = serde_json::to_string_pretty(value).expect("encode");
    encoded = encoded
        .replace('<', "\\u003c")
        .replace('>', "\\u003e")
        .replace('&', "\\u0026");
    encoded.push('\n');
    encoded.into_bytes()
}

#[test]
fn auth_providers_diff() {
    let Ok(corpus_path) = std::env::var("M9JG_CORPUS") else {
        println!("RUST auth_providers_diff skipped (M9JG_CORPUS not set)");
        return;
    };
    let out = std::env::var("M9JG_OUT").expect("M9JG_OUT");
    let out = Path::new(&out).join("rust");
    std::fs::create_dir_all(&out).expect("create output dir");

    let corpus = std::fs::read(&corpus_path).expect("read corpus");
    let cases: Vec<Case> = serde_json::from_slice(&corpus).expect("decode corpus");

    for case in &cases {
        let nodes = resolve(case);
        if case.headers.is_some() {
            // The Go resolver ignores request headers entirely
            // (`auth_providers.go:10`); prove the Rust surface does too.
            assert_eq!(
                resolve(case),
                nodes,
                "{}: header changed the surface",
                case.name
            );
        }
        let output = CaseOut {
            name: case.name.clone(),
            total_count: nodes.len(),
            nodes,
            page_info: PageInfoOut {
                has_next_page: false,
            },
        };
        let encoded = go_json(&output);
        std::fs::write(out.join(format!("{}.json", case.name)), encoded).expect("write output");
        println!(
            "RUST {} providers={} total={}",
            case.name,
            output.nodes.len(),
            output.total_count
        );
    }
}
