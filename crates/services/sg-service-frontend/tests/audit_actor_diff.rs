//! M9.20c differential of the internal-listener actor middleware feeding the
//! GraphQL audit record against the real Go baseline
//! (`cmd/frontend/internal/httpapi/zz_m920c_audit_actor_oracle_test.go`, run by
//! `run_audit_actor_diff.sh`).
//!
//! Env:
//!
//! * `SG_M920C_DIFF=1` — enables the harness
//! * `M920C_CORPUS` — `corpus.json` written by the Go oracle
//! * `M920C_OUT` — output directory (one `<name>.json` per case under
//!   `<out>/rust/`)
//!
//! Client metadata is deliberately absent: the internal chain has no
//! `requestclient`, so `audit.Log`'s client is nil (Go reports
//! `ip`/`userAgent` as "unknown"). The compared projection is the actor
//! selection only.

#![allow(clippy::unwrap_used)]

use std::collections::BTreeMap;
use std::path::Path;

use http::{HeaderMap, HeaderName, HeaderValue, Request};
use serde::{Deserialize, Serialize};

use sg_config::SiteConfiguration;
use sg_service_frontend::actor_chain::ChainActor;
use sg_service_frontend::actor_middleware::{apply_actor_header, Actor, ActorHeaderDecision};
use sg_service_frontend::audit::{build_graphql_record, GraphqlAuditContext};
use sg_service_frontend::graphql_api::graphql_audit_actor;

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(rename_all = "camelCase", default)]
struct AuditLog {
    #[serde(rename = "graphQL")]
    graph_ql: bool,
    internal_traffic: bool,
}

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(rename_all = "camelCase", default)]
struct Case {
    name: String,
    headers: BTreeMap<String, String>,
    cookie: String,
    audit_log: AuditLog,
}

#[derive(Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
struct ActorOut {
    uid: i32,
    anonymous_uid: String,
    internal: bool,
}

impl From<&Actor> for ActorOut {
    fn from(actor: &Actor) -> Self {
        Self {
            uid: actor.uid,
            anonymous_uid: actor.anonymous_uid.clone(),
            internal: actor.internal,
        }
    }
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct Output {
    name: String,
    status: u16,
    body: String,
    actor: Option<ActorOut>,
    actor_id: String,
    audit_logged: bool,
    audit_actor_uid: Option<String>,
}

fn go_json<T: Serialize>(value: &T) -> Vec<u8> {
    serde_json::to_string(value)
        .expect("encode")
        .replace('<', "\\u003c")
        .replace('>', "\\u003e")
        .replace('&', "\\u0026")
        .into_bytes()
}

fn build_headers(case: &Case) -> HeaderMap {
    let mut headers = HeaderMap::new();
    for (name, value) in &case.headers {
        headers.insert(
            HeaderName::from_bytes(name.as_bytes()).expect("header name"),
            HeaderValue::from_str(value).expect("header value"),
        );
    }
    if !case.cookie.is_empty() {
        headers.insert(
            http::header::COOKIE,
            HeaderValue::from_str(&case.cookie).expect("cookie"),
        );
    }
    headers
}

fn audit_actor_uid(record: &sg_service_frontend::audit::AuditRecord) -> Option<String> {
    record.fields["audit"]["actor"]["actorUID"]
        .as_str()
        .map(str::to_owned)
}

#[test]
fn audit_actor_diff() {
    if std::env::var("SG_M920C_DIFF").as_deref() != Ok("1") {
        return;
    }
    let corpus_path = std::env::var("M920C_CORPUS").expect("M920C_CORPUS");
    let out_root = std::env::var("M920C_OUT").expect("M920C_OUT");
    let out = Path::new(&out_root).join("rust");
    std::fs::create_dir_all(&out).expect("create rust output dir");

    let cases: Vec<Case> = serde_json::from_str(
        &std::fs::read_to_string(&corpus_path).expect("read corpus"),
    )
    .expect("parse corpus");

    for case in &cases {
        let headers = build_headers(case);
        let site_json = serde_json::json!({
            "log": {
                "auditLog": {
                    "graphQL": case.audit_log.graph_ql,
                    "internalTraffic": case.audit_log.internal_traffic,
                }
            }
        })
        .to_string();
        let site = SiteConfiguration::parse(&site_json).expect("site config");

        let output = match apply_actor_header(&headers) {
            ActorHeaderDecision::Reject { status, body } => Output {
                name: case.name.clone(),
                status,
                body,
                actor: None,
                actor_id: String::new(),
                audit_logged: false,
                audit_actor_uid: None,
            },
            ActorHeaderDecision::Proceed(actor) => {
                let mut request = Request::builder()
                    .method("POST")
                    .uri("/.internal/graphql")
                    .body(())
                    .expect("request");
                *request.headers_mut() = headers;
                request.extensions_mut().insert(ChainActor(actor.clone()));

                let audit_actor = graphql_audit_actor(&request, actor.to_graphql());
                let context = GraphqlAuditContext {
                    actor: audit_actor.clone(),
                    // Internal listener: no requestclient middleware.
                    client: None,
                    query: "query { x }".to_owned(),
                    variables_json: "null".to_owned(),
                    request_name: String::new(),
                    request_source: String::new(),
                    successful: true,
                };
                let record = build_graphql_record(&site, &context, "00000000-0000-4000-8000-000000000000");
                Output {
                    name: case.name.clone(),
                    status: 200,
                    body: "ok".to_owned(),
                    actor: Some(ActorOut::from(&actor)),
                    actor_id: audit_actor.id(),
                    audit_logged: record.is_some(),
                    audit_actor_uid: record.as_ref().and_then(audit_actor_uid),
                }
            }
        };

        std::fs::write(
            out.join(format!("{}.json", case.name)),
            go_json(&output),
        )
        .expect("write output");
        println!(
            "RUST {} status={} auditLogged={} actorID={}",
            output.name, output.status, output.audit_logged, output.actor_id
        );
    }
}
