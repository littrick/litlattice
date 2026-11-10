//! M9.8he differential of `Site.configuration` and
//! `siteConfigurationResolver.{ID,EffectiveContents}`
//! (`cmd/frontend/graphqlbackend/site.go:100-138,198-231`).
//!
//! Both sides consume the same corpus. The Go oracle
//! (`zz_m9he_site_config_oracle_test.go`) drives the real resolver with a
//! mock DB and `conf.Mock`; this harness drives
//! [`site_configuration_query`] through a [`FakeBackend`]. A case emits
//! `<name>.json` (compact, sorted keys) and the runner compares the two
//! directories byte for byte.
//!
//! Env:
//!
//! * `SG_M9HE_DIFF=1`
//! * `M9HE_CORPUS` — JSON array of cases (see [`Case`])
//! * `M9HE_OUT` — output directory (one `<name>.json` per case)

#![allow(clippy::unwrap_used)]

use std::sync::Mutex;

use async_trait::async_trait;
use serde::Deserialize;
use serde_json::{json, Value};

use sg_graphql::exec::Actor;
use sg_service_frontend::site_configuration_query::{
    configuration, effective_contents, id, SecurityEvent, SiteConfigRow, SiteConfigurationBackend,
};

#[derive(Deserialize)]
struct Case {
    name: String,
    /// `admin`, `user` or `anonymous`.
    viewer: String,
    #[serde(default)]
    uid: i32,
    #[serde(default)]
    return_safe_configs_only: Option<bool>,
    #[serde(default)]
    auditlog_expansion: bool,
    /// The `conf.Raw().Site` JSONC snapshot. For non-`parse_error` cases the
    /// oracle asserts it is the fixed point of the typed Go marshal.
    #[serde(default)]
    raw: String,
    /// The Go oracle cannot `conf.Mock` an unparseable configuration; it calls
    /// `conf.ReturnSafeConfigs`/`conf.RedactSecrets` directly for these cases
    /// (`EffectiveContents` is a two-line delegation to them). Rust needs no
    /// special handling: the injected snapshot is already invalid JSONC.
    #[serde(default)]
    #[allow(dead_code)]
    parse_error: bool,
    #[serde(default)]
    site_get_latest: SiteGetLatestSpec,
}

#[derive(Deserialize)]
#[serde(untagged)]
enum SiteGetLatestSpec {
    Ok { id: i32 },
    Err { error: String },
}

impl Default for SiteGetLatestSpec {
    fn default() -> Self {
        Self::Ok { id: 1 }
    }
}

struct FakeBackend {
    viewer_site_admin: bool,
    auditlog_expansion: bool,
    raw_site: String,
    site_get_latest: Result<i32, String>,
    events: Mutex<Vec<SecurityEvent>>,
}

impl FakeBackend {
    fn events(&self) -> Vec<SecurityEvent> {
        self.events.lock().expect("events").clone()
    }
}

#[async_trait]
impl SiteConfigurationBackend for FakeBackend {
    async fn check_current_user_is_site_admin(&self, actor: Actor) -> Result<(), String> {
        match actor {
            Actor::Anonymous => Err("not authenticated".to_owned()),
            Actor::User { .. } if !self.viewer_site_admin => Err("must be site admin".to_owned()),
            Actor::User { .. } => Ok(()),
        }
    }

    async fn auditlog_expansion(&self, _actor: Actor) -> bool {
        self.auditlog_expansion
    }

    async fn log_security_event(&self, event: SecurityEvent) -> Result<(), String> {
        self.events.lock().expect("events").push(event);
        Ok(())
    }

    async fn site_get_latest(&self) -> Result<SiteConfigRow, String> {
        self.site_get_latest.clone().map(|id| SiteConfigRow { id })
    }

    fn raw_site(&self) -> String {
        self.raw_site.clone()
    }
}

/// Keep stable resolver texts; mask the JSON-parse details that differ
/// between the Go and Rust parsers (mirrors the Go oracle).
fn normalize_error(error: String) -> String {
    if error.starts_with("parse config") {
        "parse config".to_owned()
    } else {
        error
    }
}

fn actor_for(viewer: &str, uid: i32) -> (Actor, bool) {
    match viewer {
        "admin" => (Actor::User { uid }, true),
        "user" => (Actor::User { uid }, false),
        "anonymous" => (Actor::Anonymous, false),
        other => panic!("unknown viewer {other:?}"),
    }
}

fn event_json(event: &SecurityEvent) -> Value {
    json!({
        "name": event.name,
        "url": event.url,
        "user_id": event.user_id,
        "anonymous_user_id": event.anonymous_user_id,
        "source": event.source,
        "argument": event.argument,
    })
}

#[tokio::test]
async fn site_configuration_query_matches_the_go_baseline() {
    if std::env::var("SG_M9HE_DIFF").as_deref() != Ok("1") {
        return;
    }
    let corpus_path = std::env::var("M9HE_CORPUS").expect("M9HE_CORPUS");
    let out_dir = std::env::var("M9HE_OUT").expect("M9HE_OUT");
    std::fs::create_dir_all(&out_dir).expect("create output dir");

    let cases: Vec<Case> =
        serde_json::from_str(&std::fs::read_to_string(&corpus_path).expect("read corpus"))
            .expect("parse corpus");

    for case in &cases {
        let (actor, viewer_site_admin) = actor_for(&case.viewer, case.uid);
        let site_get_latest = match &case.site_get_latest {
            SiteGetLatestSpec::Ok { id } => Ok(*id),
            SiteGetLatestSpec::Err { error } => Err(error.clone()),
        };
        let backend = FakeBackend {
            viewer_site_admin,
            auditlog_expansion: case.auditlog_expansion,
            raw_site: case.raw.clone(),
            site_get_latest,
            events: Mutex::new(Vec::new()),
        };

        let configuration_entry;
        let mut id_entry = Value::Null;
        let mut contents_entry = Value::Null;

        match configuration(&backend, actor, case.return_safe_configs_only).await {
            Ok(resolver) => {
                configuration_entry = json!({"ok": true});
                id_entry = match id(&backend, actor, &resolver).await {
                    Ok(value) => json!({"ok": true, "value": value}),
                    Err(error) => json!({"ok": false, "error": normalize_error(error)}),
                };
                contents_entry = match effective_contents(&backend, actor, &resolver).await {
                    Ok(contents) => json!({"ok": true, "contents": contents}),
                    Err(error) => json!({
                        "ok": false,
                        "contents": "",
                        "error": normalize_error(error),
                    }),
                };
            }
            Err(error) => {
                configuration_entry = json!({
                    "ok": false,
                    "error": normalize_error(error),
                });
            }
        }

        let events: Vec<Value> = backend.events().iter().map(event_json).collect();
        let envelope = json!({
            "name": case.name,
            "configuration": configuration_entry,
            "id": id_entry,
            "effective_contents": contents_entry,
            "security_events": events,
        });

        let encoded = serde_json::to_string(&envelope).expect("encode");
        std::fs::write(
            std::path::Path::new(&out_dir).join(format!("{}.json", case.name)),
            encoded,
        )
        .expect("write case");
    }

    println!("RUST site-configuration-query cases={}", cases.len());
}
