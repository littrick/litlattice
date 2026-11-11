//! M9.8jl differential of `SettingsCascade.subjects` Site `configuration` and
//! the remaining offline `validateCustomRaw` contributed validators
//! (encryption mounted/noop, symbols, txemail).
//!
//! Both sides execute the same query string through their real executors. The
//! Go oracle (`zz_m9jl_oracle_test.go`, untracked) parses a minimal SDL against
//! the real resolvers with `dbmocks` and `conf.Mock`; this harness drives
//! [`prefetch_cascade_subject_site_configuration`] and [`prefetch_nested`] over
//! fake seams and replays the prefetched roots through [`FrontendRootResolver`].
//!
//! A case emits `<name>.json` (compact, sorted keys) and the runner compares the
//! two directories byte for byte.
//!
//! Env:
//!
//! * `SG_M9JL_DIFF=1`
//! * `M9JL_CORPUS` — JSON array of cases (see [`Case`])
//! * `M9JL_OUT` — output directory (one `<name>.json` per case)

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
use sg_service_frontend::auth_providers_graphql::AuthProvidersRoot;
use sg_service_frontend::config_graphql::{
    prefetch_cascade_subject_site_configuration, validate_site_against_raw, LicenseInfoProvider,
    SiteConfigValidator,
};
use sg_service_frontend::graphql_resolvers::{
    prefetch_nested, FrontendRootResolver, MembersArgs, MembersConnectionData, MembershipData,
    NamespaceName, OrgData, OrgMembersStore, OrgMembershipsStore, SettingsData, UserData,
    UserObject, UserOrganizationsStore,
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
  viewerSettings: SettingsCascade!
  viewerConfiguration: ConfigurationCascade!
  settingsSubject(id: ID!): SettingsSubject
  currentUser: User
}

interface SettingsSubject {
  id: ID!
  settingsCascade: SettingsCascade!
  configurationCascade: ConfigurationCascade!
  viewerCanAdminister: Boolean!
}

type Site implements SettingsSubject {
  id: ID!
  settingsCascade: SettingsCascade!
  configurationCascade: ConfigurationCascade!
  viewerCanAdminister: Boolean!
  configuration(returnSafeConfigsOnly: Boolean): SiteConfiguration
  authProviders: AuthProviderConnection!
}

type User implements SettingsSubject {
  id: ID!
  username: String!
  settingsCascade: SettingsCascade!
  configurationCascade: ConfigurationCascade!
  viewerCanAdminister: Boolean!
}

type SettingsCascade {
  subjects: [SettingsSubject!]!
  final: String!
  merged: Configuration!
}

type ConfigurationCascade {
  subjects: [SettingsSubject!]!
  merged: Configuration!
}

type SiteConfiguration {
  id: Int!
  effectiveContents: JSONCString!
  validationMessages: [String!]!
}

type AuthProviderConnection {
  nodes: [AuthProvider!]!
  totalCount: Int!
}

type AuthProvider {
  serviceType: String!
  serviceID: String!
  clientID: String!
  displayName: String!
  isBuiltin: Boolean!
  authenticationURL: String
}

type Configuration {
  contents: JSONCString!
  messages: [String!]!
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
    auditlog_expansion: bool,
}

struct FakeBackend {
    viewer_site_admin: bool,
    raw_site: Arc<Mutex<String>>,
    auditlog_expansion: bool,
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
        self.auditlog_expansion
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
    viewer: Option<UserData>,
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

    async fn user_by_id(&self, user_id: i32) -> Result<UserData, String> {
        match &self.viewer {
            Some(user) if user.id == user_id => Ok(user.clone()),
            _ => Err(format!("user not found: [{user_id}]")),
        }
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
        Ok(None)
    }
}

fn viewer_user(case: &Case) -> Option<UserData> {
    if case.viewer == "anonymous" {
        return None;
    }
    let created_at =
        OffsetDateTime::parse("2020-01-02T03:04:05Z", &Rfc3339).expect("fixed timestamp");
    Some(UserData {
        id: case.uid,
        username: format!("user{}", case.uid),
        display_name: format!("user{}", case.uid),
        avatar_url: String::new(),
        created_at,
        updated_at: created_at,
        site_admin: case.viewer == "admin",
        tos_accepted: false,
        primary_email: String::new(),
    })
}

fn mask_response(response: &str) -> Value {
    let mut value: Value = serde_json::from_str(response).expect("response json");
    if let Some(errors) = value.get_mut("errors").and_then(Value::as_array_mut) {
        if !errors.is_empty() {
            *errors = vec![json!({"message": "error"})];
        }
    }
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
) -> Value {
    let backend = FakeBackend {
        viewer_site_admin,
        raw_site: Arc::clone(raw_site),
        auditlog_expansion: case.auditlog_expansion,
    };
    let validator = FakeValidator {
        raw_site: Arc::clone(raw_site),
    };
    let history = FakeHistory;
    let licenses = FakeLicenses;
    let viewer = viewer_user(case);
    let nested_store = FakeNested {
        viewer: viewer.clone(),
    };

    let variables = Map::new();
    let request = ExecRequest {
        query: &case.query,
        operation_name: "",
        variables: &variables,
        actor,
    };
    let fields = root_fields(schema, &request);

    let cascade_site_configuration = prefetch_cascade_subject_site_configuration(
        &backend, &licenses, &validator, &history, None, &fields, actor,
    )
    .await;
    let mut nested = prefetch_nested(
        &nested_store,
        &nested_store,
        &nested_store,
        &fields,
        viewer.as_ref(),
        &HashMap::new(),
        &HashMap::new(),
        &HashMap::new(),
        &HashMap::new(),
        &Default::default(),
        actor,
        viewer_site_admin,
    )
    .await;
    nested.cascade_site_configuration = cascade_site_configuration;
    // The oracle process never runs the provider packages' `conf.Watch`
    // callbacks, so `providers.SortedProviders()` is empty there. Mirror the
    // same (empty) directory; derivation from `auth.providers` is covered by
    // the M9.8jg diff.
    let auth_providers = Arc::new(AuthProvidersRoot::default());
    nested.auth_providers = Some(Arc::clone(&auth_providers));
    let nested = Arc::new(nested);

    let user = viewer.map(UserObject::from_data);
    let roots = FrontendRootResolver::with_prefetched_organizations(
        user,
        HashMap::new(),
        HashMap::new(),
        HashMap::new(),
        HashMap::new(),
        HashMap::new(),
        viewer_site_admin,
    )
    .with_nested(nested)
    .with_auth_providers(Some(auth_providers));

    let response = execute_with(schema, request, Some(&roots));
    mask_response(&response.to_json())
}

#[tokio::test]
async fn settings_cascade_config_matches_the_go_baseline() {
    if std::env::var("SG_M9JL_DIFF").as_deref() != Ok("1") {
        return;
    }
    let corpus_path = std::env::var("M9JL_CORPUS").expect("M9JL_CORPUS");
    let out_dir = std::env::var("M9JL_OUT").expect("M9JL_OUT");
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

        let raw_site = Arc::new(Mutex::new(case.raw.clone()));
        let response = run_once(&schema, case, actor, viewer_site_admin, &raw_site).await;

        let entry = json!({
            "name": case.name,
            "response": response,
        });
        let encoded = serde_json::to_string(&entry).expect("encode");
        std::fs::write(
            std::path::Path::new(&out_dir).join(format!("{}.json", case.name)),
            encoded,
        )
        .expect("write case");
    }

    println!("RUST settings-cascade-config cases={}", cases.len());
}
