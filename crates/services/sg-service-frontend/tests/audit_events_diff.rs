//! M9.8jn differential harness: replays the shared corpus through
//! [`sg_service_frontend::security_event_logs::prepare_security_events`] and
//! emits the same envelope as the Go oracle in
//! `cmd/frontend/graphqlbackend/zz_m9jn_oracle_test.go`. The script compares
//! the two outputs byte-for-byte.

use serde::{Deserialize, Serialize};
use serde_json::Value;
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;

use sg_config::SiteConfiguration;
use sg_service_frontend::audit::{
    format_rfc3339_nano, log_level_from_env, logger_severity, AuditActor, ClientInfo,
};
use sg_service_frontend::security_event_logs::{
    prepare_security_events, SecurityEventInput, SecurityEventPolicy,
};

const VERSION: &str = "<version>";
const AUDIT_ID: &str = "<uuid>";

#[derive(Debug, Deserialize)]
struct Corpus {
    cases: Vec<Case>,
}

#[derive(Debug, Deserialize)]
struct Case {
    name: String,
    #[serde(default)]
    site: Value,
    #[serde(default)]
    actor: ActorCase,
    #[serde(default)]
    client: Option<ClientCase>,
    #[serde(default)]
    events: Vec<EventCase>,
}

#[derive(Debug, Default, Deserialize)]
struct ActorCase {
    #[serde(default)]
    uid: i32,
    #[serde(default, rename = "anonymousUid")]
    anonymous_uid: String,
    #[serde(default)]
    internal: bool,
    #[serde(default, rename = "sourcegraphOperator")]
    sourcegraph_operator: bool,
}

#[derive(Debug, Deserialize)]
struct ClientCase {
    ip: String,
    #[serde(rename = "forwardedFor")]
    forwarded_for: String,
    #[serde(rename = "userAgent")]
    user_agent: String,
    #[serde(rename = "forwardedForUserAgent")]
    forwarded_for_user_agent: String,
}

#[derive(Debug, Deserialize)]
struct EventCase {
    name: String,
    url: String,
    #[serde(rename = "userId")]
    user_id: i32,
    #[serde(rename = "anonymousUserId")]
    anonymous_user_id: String,
    source: String,
    #[serde(default)]
    argument: Option<Value>,
    timestamp: String,
}

#[derive(Debug, Serialize)]
struct Row {
    name: String,
    url: String,
    #[serde(rename = "userId")]
    user_id: i32,
    #[serde(rename = "anonymousUserId")]
    anonymous_user_id: String,
    source: String,
    argument: String,
    version: String,
    timestamp: String,
}

#[derive(Debug, Serialize)]
struct Database {
    written: bool,
    rows: Vec<Row>,
}

#[derive(Debug, Serialize)]
struct AuditEntry {
    level: String,
    message: String,
    fields: Value,
}

#[derive(Debug, Serialize)]
struct CaseResult {
    name: String,
    error: String,
    database: Database,
    audit: Vec<AuditEntry>,
}

fn parse_site(site: &Value) -> SiteConfiguration {
    let text = if site.is_null() {
        "{}".to_owned()
    } else {
        serde_json::to_string(site).expect("serialize site")
    };
    SiteConfiguration::parse(&text).expect("site configuration")
}

fn run_case(case: &Case, log_level: &str) -> CaseResult {
    let site = parse_site(&case.site);
    let policy = SecurityEventPolicy::from_site(&site);
    let actor = AuditActor {
        uid: (case.actor.uid > 0).then_some(case.actor.uid),
        anonymous_uid: case.actor.anonymous_uid.clone(),
        internal: case.actor.internal,
    };
    let client = case.client.as_ref().map(|client| ClientInfo {
        ip: client.ip.clone(),
        forwarded_for: client.forwarded_for.clone(),
        user_agent: client.user_agent.clone(),
        forwarded_for_user_agent: client.forwarded_for_user_agent.clone(),
    });

    let mut events = Vec::with_capacity(case.events.len());
    for event in &case.events {
        let timestamp = OffsetDateTime::parse(&event.timestamp, &Rfc3339)
            .unwrap_or_else(|error| panic!("{}: timestamp: {error}", case.name));
        events.push(SecurityEventInput::new(
            event.name.clone(),
            event.url.clone(),
            event.user_id,
            event.anonymous_user_id.clone(),
            event.source.clone(),
            event.argument.clone(),
            timestamp,
        ));
    }

    let prepared = match prepare_security_events(
        &site,
        policy,
        &actor,
        case.actor.sourcegraph_operator,
        client.as_ref(),
        &events,
        VERSION,
        || AUDIT_ID.to_owned(),
    ) {
        Ok(prepared) => prepared,
        Err(error) => {
            return CaseResult {
                name: case.name.clone(),
                error,
                database: Database {
                    written: false,
                    rows: Vec::new(),
                },
                audit: Vec::new(),
            }
        }
    };

    let written = prepared.database && !prepared.rows.is_empty();
    let rows = if written {
        prepared
            .rows
            .iter()
            .map(|row| Row {
                name: row.name.clone(),
                url: row.url.clone(),
                user_id: row.user_id,
                anonymous_user_id: row.anonymous_user_id.clone(),
                source: row.source.clone(),
                argument: row.argument.clone(),
                version: row.version.clone(),
                timestamp: format_rfc3339_nano(row.timestamp),
            })
            .collect()
    } else {
        Vec::new()
    };
    let audit = prepared
        .audits
        .iter()
        .map(|record| AuditEntry {
            level: logger_severity(log_level).as_str().to_owned(),
            message: record.message.clone(),
            fields: record.fields.clone(),
        })
        .collect();

    CaseResult {
        name: case.name.clone(),
        error: String::new(),
        database: Database { written, rows },
        audit,
    }
}

#[test]
fn audit_events_match_go_oracle() {
    let corpus_path = std::env::var("M9JN_CORPUS").expect("M9JN_CORPUS");
    let out_path = std::env::var("M9JN_OUT").expect("M9JN_OUT");
    let data = std::fs::read_to_string(&corpus_path).expect("read corpus");
    let corpus: Corpus = serde_json::from_str(&data).expect("parse corpus");
    let log_level = log_level_from_env();

    let results: Vec<CaseResult> = corpus
        .cases
        .iter()
        .map(|case| run_case(case, &log_level))
        .collect();
    // Go's `encoding/json` HTML-escapes `<`, `>` and `&` inside strings; the
    // normalization placeholders (`<uuid>`, `<version>`) make those visible.
    let out = serde_json::to_string(&results)
        .expect("serialize results")
        .replace('<', "\\u003c")
        .replace('>', "\\u003e")
        .replace('&', "\\u0026");
    std::fs::write(&out_path, out).expect("write results");
    println!("RUST security events len={}", results.len());
}
