//! M9.8jh differential of the remaining configuration-write GraphQL semantics:
//! `SettingsSubject(site).configuration`, the `latestSettings` subtree
//! (`Settings.id/contents/configuration.contents/messages`) and the offline
//! subset of `validateCustomRaw` (`internal/conf/validate_custom.go` plus the
//! auth contributed validators).
//!
//! Both sides execute the same query string through their real executors. The
//! Go oracle (`zz_m9jh_oracle_test.go`, untracked) parses a minimal SDL against
//! the real `siteResolver` / `schemaResolver.SettingsSubject` with `dbmocks` and
//! `conf.Mock`; this harness drives [`prefetch_site`],
//! [`prefetch_settings_subject_site_configuration`] and [`prefetch_nested`] over
//! fake seams and replays the prefetched roots through
//! [`FrontendRootResolver`]. A case with `raw_after_write` runs the same query a
//! second time after swapping the fake backend's raw config (write-then-read).
//!
//! A case emits `<name>.json` (compact, sorted keys) and the runner compares the
//! two directories byte for byte.
//!
//! Env:
//!
//! * `SG_M9JH_DIFF=1`
//! * `M9JH_CORPUS` — JSON array of cases (see [`Case`])
//! * `M9JH_OUT` — output directory (one `<name>.json` per case)

#![allow(clippy::unwrap_used)]

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use serde::Deserialize;
use serde_json::{json, Map, Value};
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;

use sg_graphql::exec::{execute_with, root_fields, Actor, ExecRequest};
use sg_graphql::schema::assemble_sdl;
use sg_service_frontend::config_graphql::{
    prefetch_settings_subject_site_configuration, prefetch_site, validate_site_against_raw,
    LicenseInfoProvider, SiteConfigValidator, SiteRoot,
};
use sg_service_frontend::graphql_resolvers::{
    prefetch_nested, FrontendRootResolver, MembersArgs, MembersConnectionData, MembershipData,
    NamespaceName, OrgData, OrgMembersStore, OrgMembershipsStore, SettingsData,
    UserOrganizationsStore,
};
use sg_service_frontend::site_config_change::{SiteConfigHistoryStore, SiteConfigRow};
use sg_service_frontend::site_configuration_query::{
    SecurityEvent, SiteConfigRow as LatestRow, SiteConfigurationBackend,
};

const SDL: &str = "
schema { query: Query }

scalar DateTime
scalar JSONCString

type Query {
  site: Site!
  settingsSubject(id: ID!): SettingsSubject
}

interface SettingsSubject {
  id: ID!
  latestSettings: Settings
  settingsURL: String
  viewerCanAdminister: Boolean!
}

type Site implements SettingsSubject {
  id: ID!
  latestSettings: Settings
  settingsURL: String
  viewerCanAdminister: Boolean!
  configuration(returnSafeConfigsOnly: Boolean): SiteConfiguration
}

type Settings {
  id: Int!
  subject: SettingsSubject!
  contents: JSONCString!
  configuration: Configuration!
}

type Configuration {
  contents: JSONCString!
  messages: [String!]!
}

type SiteConfiguration {
  id: Int!
  effectiveContents: JSONCString!
  validationMessages: [String!]!
}
";

#[derive(Deserialize)]
struct Case {
    name: String,
    query: String,
    /// `admin`, `user` or `anonymous`.
    viewer: String,
    #[serde(default)]
    uid: i32,
    raw: String,
    #[serde(default)]
    raw_after_write: Option<String>,
    #[serde(default)]
    settings: Option<SettingsSpec>,
}

#[derive(Deserialize)]
struct SettingsSpec {
    id: i32,
    contents: String,
    created_at: String,
}

struct FakeBackend {
    viewer_site_admin: bool,
    raw_site: Arc<Mutex<String>>,
}

#[async_trait]
impl SiteConfigurationBackend for FakeBackend {
    async fn check_current_user_is_site_admin(&self, actor: Actor) -> Result<(), String> {
        match actor {
            Actor::Anonymous => Err("not authenticated".to_owned()),
            Actor::User { .. } if !self.viewer_site_admin => Err("must be site admin".to_owned()),
            Actor::User { .. } => Ok(()),
            Actor::Internal => Ok(()),
        }
    }

    async fn auditlog_expansion(&self, _actor: Actor) -> bool {
        false
    }

    async fn log_security_event(&self, _event: SecurityEvent) -> Result<(), String> {
        Ok(())
    }

    async fn site_get_latest(&self) -> Result<LatestRow, String> {
        Ok(LatestRow { id: 1 })
    }

    fn raw_site(&self) -> String {
        self.raw_site.lock().unwrap().clone()
    }
}

/// The production validator adapter's synchronous core.
struct FakeValidator {
    raw_site: Arc<Mutex<String>>,
}

#[async_trait]
impl SiteConfigValidator for FakeValidator {
    async fn validate_site(&self, contents: &str) -> Result<Vec<String>, String> {
        let raw = self.raw_site.lock().unwrap().clone();
        validate_site_against_raw(contents, &raw)
    }
}

struct FakeHistory;

#[async_trait]
impl SiteConfigHistoryStore for FakeHistory {
    async fn get_site_configs(&self) -> Result<Vec<SiteConfigRow>, String> {
        Ok(Vec::new())
    }

    async fn get_site_config_count(&self) -> Result<usize, String> {
        Ok(0)
    }
}

struct FakeLicenses;

#[async_trait]
impl LicenseInfoProvider for FakeLicenses {
    async fn configured_license(
        &self,
    ) -> Result<Option<sg_service_frontend::config_graphql::LicenseInfo>, String> {
        Ok(None)
    }
}

struct FakeNested {
    site_settings: Option<SettingsData>,
}

#[async_trait]
impl UserOrganizationsStore for FakeNested {
    async fn organizations_for_user(
        &self,
        _user_id: i32,
        _viewer_uid: i32,
        _viewer_site_admin: bool,
    ) -> Result<Vec<OrgData>, String> {
        Ok(Vec::new())
    }
}

#[async_trait]
impl OrgMembersStore for FakeNested {
    async fn members(
        &self,
        _org_id: i32,
        _args: &MembersArgs,
    ) -> Result<MembersConnectionData, String> {
        Err("not used".to_owned())
    }
}

#[async_trait]
impl OrgMembershipsStore for FakeNested {
    async fn memberships_for_user(&self, _user_id: i32) -> Result<Vec<MembershipData>, String> {
        Ok(Vec::new())
    }

    async fn namespace_by_name(&self, _name: &str) -> Result<Option<NamespaceName>, String> {
        Ok(None)
    }

    async fn org_by_id(
        &self,
        org_id: i32,
        _viewer_uid: i32,
        _viewer_site_admin: bool,
    ) -> Result<OrgData, String> {
        Err(format!("org not found: {org_id}"))
    }

    async fn user_by_id(
        &self,
        user_id: i32,
    ) -> Result<sg_service_frontend::graphql_resolvers::UserData, String> {
        Err(format!("user not found: [{user_id}]"))
    }

    async fn latest_settings_for_user(
        &self,
        _user_id: i32,
    ) -> Result<Option<SettingsData>, String> {
        Ok(None)
    }

    async fn latest_settings_for_org(&self, _org_id: i32) -> Result<Option<SettingsData>, String> {
        Ok(None)
    }

    async fn latest_settings_for_site(&self) -> Result<Option<SettingsData>, String> {
        Ok(self.site_settings.clone())
    }
}

fn parse_time(value: &str) -> OffsetDateTime {
    OffsetDateTime::parse(value, &Rfc3339).expect("RFC3339 timestamp")
}

fn mask_response(response: &str) -> Value {
    let mut value: Value = serde_json::from_str(response).expect("response json");
    if let Some(errors) = value.get_mut("errors").and_then(Value::as_array_mut) {
        if !errors.is_empty() {
            // Error multiplicity and paths differ between the executors; only
            // the presence of an error is compared.
            *errors = vec![json!({"message": "error"})];
        }
    }
    // graph-gophers always emits `data`, even when it is null; the Rust
    // executor omits it for some pre-execution failures. Normalize to null.
    if let Value::Object(object) = &mut value {
        object.entry("data".to_owned()).or_insert(Value::Null);
    }
    value
}

async fn run_once(
    schema: &sg_graphql::Schema,
    case: &Case,
    actor: Actor,
    viewer_site_admin: bool,
    raw_site: &Arc<Mutex<String>>,
    settings: Option<SettingsData>,
) -> Value {
    let backend = FakeBackend {
        viewer_site_admin,
        raw_site: Arc::clone(raw_site),
    };
    let validator = FakeValidator {
        raw_site: Arc::clone(raw_site),
    };
    let history = FakeHistory;
    let licenses = FakeLicenses;
    let nested_store = FakeNested {
        site_settings: settings,
    };

    let variables = Map::new();
    let request = ExecRequest {
        query: &case.query,
        operation_name: "",
        variables: &variables,
        actor,
    };
    let fields = root_fields(schema, &request);

    let site = prefetch_site(
        &backend, &licenses, &validator, &history, None, &fields, actor,
    )
    .await;
    let settings_subject_site = prefetch_settings_subject_site_configuration(
        &backend, &licenses, &validator, &history, None, &fields, actor,
    )
    .await
    .map(|configuration| SiteRoot {
        configuration: Some(configuration),
    });
    let nested = Arc::new(
        prefetch_nested(
            &nested_store,
            &nested_store,
            &nested_store,
            &fields,
            None,
            &HashMap::new(),
            &HashMap::new(),
            &HashMap::new(),
            &HashMap::new(),
            &Default::default(),
            actor,
            viewer_site_admin,
        )
        .await,
    );

    let roots = FrontendRootResolver::with_prefetched_organizations(
        None,
        HashMap::new(),
        HashMap::new(),
        HashMap::new(),
        HashMap::new(),
        HashMap::new(),
        viewer_site_admin,
    )
    .with_nested(nested)
    .with_site(site)
    .with_settings_subject_site_configuration(settings_subject_site);

    let response = execute_with(schema, request, Some(&roots));
    mask_response(&response.to_json())
}

#[tokio::test]
async fn config_write_matches_the_go_baseline() {
    if std::env::var("SG_M9JH_DIFF").as_deref() != Ok("1") {
        return;
    }
    let corpus_path = std::env::var("M9JH_CORPUS").expect("M9JH_CORPUS");
    let out_dir = std::env::var("M9JH_OUT").expect("M9JH_OUT");
    std::fs::create_dir_all(&out_dir).expect("create output dir");

    let cases: Vec<Case> =
        serde_json::from_str(&std::fs::read_to_string(&corpus_path).expect("read corpus"))
            .expect("parse corpus");
    let schema = assemble_sdl(SDL).expect("schema");

    for case in &cases {
        let (actor, viewer_site_admin) = match case.viewer.as_str() {
            "admin" => (Actor::User { uid: case.uid }, true),
            "user" => (Actor::User { uid: case.uid }, false),
            "anonymous" => (Actor::Anonymous, false),
            other => panic!("unknown viewer {other:?}"),
        };
        let settings = case.settings.as_ref().map(|settings| SettingsData {
            id: settings.id,
            contents: settings.contents.clone(),
            created_at: parse_time(&settings.created_at),
            author_user_id: None,
        });

        let raw_site = Arc::new(Mutex::new(case.raw.clone()));
        let response = run_once(
            &schema,
            case,
            actor,
            viewer_site_admin,
            &raw_site,
            settings.clone(),
        )
        .await;

        let mut entry = json!({
            "name": case.name,
            "response": response,
        });
        if let Some(after) = &case.raw_after_write {
            *raw_site.lock().unwrap() = after.clone();
            let after_response = run_once(
                &schema,
                case,
                actor,
                viewer_site_admin,
                &raw_site,
                settings.clone(),
            )
            .await;
            entry
                .as_object_mut()
                .expect("entry object")
                .insert("response_after_write".to_owned(), after_response);
        }

        let encoded = serde_json::to_string(&entry).expect("encode");
        std::fs::write(
            std::path::Path::new(&out_dir).join(format!("{}.json", case.name)),
            encoded,
        )
        .expect("write case");
    }

    println!("RUST config-write cases={}", cases.len());
}
