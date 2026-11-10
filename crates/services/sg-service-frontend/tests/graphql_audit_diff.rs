//! M9.8fw differential of the GraphQL audit log against
//! `cmd/frontend/internal/httpapi/zz_m9fw_audit_oracle_test.go`.
//!
//! Both sides feed the same corpus through the baseline's
//! `recordAuditLog`/`audit.Log` equivalents and emit
//! `{logged, level, message, fields}` per case. The audit ID is pinned to
//! `<uuid>` (the Go oracle normalises the generated v4 UUID) so the output is
//! deterministic; the envelope is serialized with the Go-compatible encoder
//! so `encoding/json` and this side agree byte for byte.
//!
//! Env:
//!
//! * `SG_M9FW_DIFF=1`
//! * `M9FW_CORPUS`, `M9FW_OUT`

#![allow(clippy::unwrap_used)]

use std::path::Path;

use serde::Deserialize;
use serde_json::{Map, Value};

use sg_config::SiteConfiguration;
use sg_service_frontend::audit::{
    build_graphql_record, logger_severity, AuditActor, ClientInfo, GraphqlAuditContext,
};
use sg_service_frontend::settings_merge::marshal_go;

#[derive(Deserialize, Default)]
struct ActorCase {
    #[serde(default)]
    uid: Option<i32>,
    #[serde(rename = "anonymousUid", default)]
    anonymous_uid: Option<String>,
    #[serde(default)]
    internal: bool,
}

#[derive(Deserialize)]
struct ClientCase {
    ip: String,
    #[serde(rename = "forwardedFor")]
    forwarded_for: String,
    #[serde(rename = "userAgent")]
    user_agent: String,
    #[serde(rename = "forwardedForUserAgent")]
    forwarded_for_user_agent: String,
}

#[derive(Deserialize)]
struct Case {
    site: Value,
    #[serde(default)]
    actor: ActorCase,
    #[serde(default)]
    client: Option<ClientCase>,
    #[serde(rename = "requestName", default)]
    request_name: String,
    #[serde(rename = "requestSource", default)]
    request_source: String,
    #[serde(default)]
    query: String,
    /// Absent or `null` is Go's nil map (`"null"`); `{}` is `"{}"`.
    #[serde(default)]
    variables: Option<Map<String, Value>>,
    #[serde(default)]
    successful: bool,
}

#[derive(Deserialize)]
struct Corpus {
    cases: Vec<Case>,
}

fn envelope(case: &Case) -> Value {
    let site = SiteConfiguration::parse(&serde_json::to_string(&case.site).unwrap()).unwrap();
    let actor = AuditActor {
        uid: case.actor.uid,
        anonymous_uid: case.actor.anonymous_uid.clone().unwrap_or_default(),
        internal: case.actor.internal,
    };
    let client = case.client.as_ref().map(|client| ClientInfo {
        ip: client.ip.clone(),
        forwarded_for: client.forwarded_for.clone(),
        user_agent: client.user_agent.clone(),
        forwarded_for_user_agent: client.forwarded_for_user_agent.clone(),
    });
    let context = GraphqlAuditContext {
        actor,
        client,
        query: case.query.clone(),
        variables_json: GraphqlAuditContext::variables_json(case.variables.as_ref()),
        request_name: case.request_name.clone(),
        request_source: case.request_source.clone(),
        successful: case.successful,
    };
    let record = build_graphql_record(&site, &context, "<uuid>");

    // The Go oracle reads `env.LogLevel`, whose per-process override key is
    // derived from the test binary name (`SRC_LOG_LEVEL_httpapitest`), so it
    // effectively observes `SRC_LOG_LEVEL`; the harness reads the same value
    // directly to stay independent of the frontend's override key.
    let log_level = std::env::var("SRC_LOG_LEVEL").unwrap_or_else(|_| "warn".to_owned());
    let mut out = Map::new();
    out.insert("logged".to_owned(), Value::Bool(record.is_some()));
    out.insert(
        "level".to_owned(),
        Value::String(
            record
                .as_ref()
                .map(|_| logger_severity(&log_level).as_str())
                .unwrap_or_default()
                .to_owned(),
        ),
    );
    out.insert(
        "message".to_owned(),
        Value::String(
            record
                .as_ref()
                .map(|record| record.message.clone())
                .unwrap_or_default(),
        ),
    );
    out.insert(
        "fields".to_owned(),
        record
            .as_ref()
            .map(|record| record.fields.clone())
            .unwrap_or(Value::Null),
    );
    Value::Object(out)
}

#[test]
fn graphql_audit_harness() {
    if std::env::var("SG_M9FW_DIFF").as_deref() != Ok("1") {
        return;
    }
    let corpus_path = std::env::var("M9FW_CORPUS").expect("M9FW_CORPUS");
    let out_path = std::env::var("M9FW_OUT").expect("M9FW_OUT");
    let corpus: Corpus =
        serde_json::from_slice(&std::fs::read(corpus_path).expect("corpus")).expect("corpus json");

    let results: Vec<Value> = corpus.cases.iter().map(envelope).collect();
    let encoded = marshal_go(&Value::Array(results));
    std::fs::write(Path::new(&out_path), &encoded).expect("write audit");
    println!("RUST audit len={}", encoded.len());
}
