//! M9.8je differential of the wired read-only configuration GraphQL surface
//! (`cmd/frontend/graphqlbackend/site.go:100-292`): `Site.configuration`,
//! `siteConfigurationResolver.{ID,EffectiveContents,ValidationMessages,
//! LicenseInfo,History}` and `SiteConfigurationChangeConnection` pagination,
//! plus the license decision the M9.8ja seams share.
//!
//! Both sides execute the same query string through their real executors. The
//! Go oracle (`zz_m9je_oracle_test.go`, untracked) parses a minimal SDL
//! against the real `siteResolver` + `dbmocks` + `conf.Mock` +
//! `licensing.MockGetConfiguredProductLicenseInfo`; this harness drives
//! [`prefetch_site`] and replays the prefetched roots through
//! [`FrontendRootResolver::with_site`]. A case emits `<name>.json` (compact,
//! sorted keys) and the runner compares the two directories byte for byte.
//!
//! Env:
//!
//! * `SG_M9JE_DIFF=1`
//! * `M9JE_CORPUS` — JSON array of cases (see [`Case`])
//! * `M9JE_OUT` — output directory (one `<name>.json` per case)

#![allow(clippy::unwrap_used)]

use std::sync::Arc;

use async_trait::async_trait;
use serde::Deserialize;
use serde_json::{json, Map, Value};
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;

use sg_graphql::exec::{execute_with, root_fields, Actor, ExecRequest};
use sg_graphql::schema::assemble_sdl;
use sg_service_frontend::authz::LicenseState;
use sg_service_frontend::config_graphql::{
    prefetch_site, validate_site_against_raw, SiteConfigValidator,
};
use sg_service_frontend::graphql_resolvers::{
    FrontendRootResolver, UserData, UserKey, UserLookupStore,
};
use sg_service_frontend::license_config::{LicenseConfig, ProductLicense, ProductLicenseStore};
use sg_service_frontend::site_config_change::{
    visible_history, SiteConfigHistoryStore, SiteConfigRow,
};
use sg_service_frontend::site_configuration_query::{
    SecurityEvent, SiteConfigRow as LatestRow, SiteConfigurationBackend,
};

const SDL: &str = "
schema { query: Query }

type Query {
  site: Site!
}

type Site {
  configuration(returnSafeConfigsOnly: Boolean): SiteConfiguration
}

type SiteConfiguration {
  id: Int!
  effectiveContents: JSONCString!
  validationMessages: [String!]!
  licenseInfo: LicenseInfo!
  history(first: Int, last: Int, after: String, before: String): SiteConfigurationChangeConnection!
}

scalar JSONCString
scalar DateTime

type LicenseInfo {
  tags: [String!]!
  userCount: Int!
  expiresAt: DateTime!
}

type SiteConfigurationChangeConnection {
  nodes: [SiteConfigurationChange!]!
  totalCount: Int!
  pageInfo: ConnectionPageInfo!
}

type ConnectionPageInfo {
  endCursor: String
  startCursor: String
  hasNextPage: Boolean!
  hasPreviousPage: Boolean!
}

type SiteConfigurationChange {
  id: ID!
  author: User
  diff: String!
  createdAt: DateTime!
  updatedAt: DateTime!
}

type User {
  id: ID!
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
    license: LicenseSpec,
    #[serde(default)]
    history: Vec<HistoryRow>,
    #[serde(default)]
    users: Vec<i32>,
}

#[derive(Deserialize)]
struct LicenseSpec {
    kind: String,
    #[serde(default)]
    tags: Vec<String>,
    #[serde(default)]
    user_count: i32,
    #[serde(default)]
    expires_at: String,
    #[serde(default)]
    error: String,
}

#[derive(Deserialize)]
struct HistoryRow {
    id: i32,
    #[serde(default)]
    author_user_id: i32,
    #[serde(default)]
    contents: String,
    redacted_contents: String,
    created_at: String,
    updated_at: String,
}

struct FakeBackend {
    viewer_site_admin: bool,
    raw_site: String,
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
        self.raw_site.clone()
    }
}

/// The production validator adapter's synchronous core.
struct FakeValidator {
    raw_site: String,
}

#[async_trait]
impl SiteConfigValidator for FakeValidator {
    async fn validate_site(&self, contents: &str) -> Result<Vec<String>, String> {
        validate_site_against_raw(contents, &self.raw_site)
    }
}

struct FakeHistory {
    rows: Vec<SiteConfigRow>,
    count: usize,
}

#[async_trait]
impl SiteConfigHistoryStore for FakeHistory {
    async fn get_site_configs(&self) -> Result<Vec<SiteConfigRow>, String> {
        Ok(self.rows.clone())
    }

    async fn get_site_config_count(&self) -> Result<usize, String> {
        Ok(self.count)
    }
}

struct FakeUsers {
    ids: Vec<i32>,
}

#[async_trait]
impl UserLookupStore for FakeUsers {
    async fn lookup(&self, key: &UserKey) -> Result<Option<UserData>, String> {
        let UserKey::DatabaseId(id) = key else {
            return Ok(None);
        };
        if !self.ids.contains(id) {
            return Ok(None);
        }
        Ok(Some(UserData {
            id: *id,
            username: String::new(),
            display_name: String::new(),
            avatar_url: String::new(),
            created_at: OffsetDateTime::UNIX_EPOCH,
            updated_at: OffsetDateTime::UNIX_EPOCH,
            site_admin: false,
            tos_accepted: false,
            primary_email: String::new(),
        }))
    }
}

struct FakeLicenses {
    license: Result<Option<ProductLicense>, String>,
}

#[async_trait]
impl ProductLicenseStore for FakeLicenses {
    async fn current_license(&self) -> Result<Option<ProductLicense>, String> {
        self.license.clone()
    }
}

fn parse_time(value: &str) -> OffsetDateTime {
    OffsetDateTime::parse(value, &Rfc3339).expect("RFC3339 timestamp")
}

fn license_state_name(state: LicenseState) -> &'static str {
    match state {
        LicenseState::Error => "error",
        LicenseState::None => "none",
        LicenseState::Expired => "expired",
        LicenseState::Valid => "valid",
    }
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

#[tokio::test]
async fn config_graphql_matches_the_go_baseline() {
    if std::env::var("SG_M9JE_DIFF").as_deref() != Ok("1") {
        return;
    }
    let corpus_path = std::env::var("M9JE_CORPUS").expect("M9JE_CORPUS");
    let out_dir = std::env::var("M9JE_OUT").expect("M9JE_OUT");
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

        let backend = FakeBackend {
            viewer_site_admin,
            raw_site: case.raw.clone(),
        };
        let history_rows: Vec<SiteConfigRow> = case
            .history
            .iter()
            .map(|row| SiteConfigRow {
                id: row.id,
                author_user_id: row.author_user_id,
                contents: row.contents.clone(),
                redacted_contents: Some(row.redacted_contents.clone()),
                created_at: parse_time(&row.created_at),
                updated_at: parse_time(&row.updated_at),
            })
            .collect();
        let count = visible_history(&history_rows).len();
        let history = FakeHistory {
            rows: history_rows,
            count,
        };
        let users = FakeUsers {
            ids: case.users.clone(),
        };
        let license = match case.license.kind.as_str() {
            "error" => Err(case.license.error.clone()),
            "valid" | "expired" => Ok(Some(ProductLicense {
                license_key: format!("key-{}", case.name),
                tags: case.license.tags.clone(),
                user_count: case.license.user_count,
                expires_at: Some(parse_time(&case.license.expires_at)),
            })),
            other => panic!("unknown license kind {other:?}"),
        };
        let licenses = LicenseConfig::new(Arc::new(FakeLicenses {
            license: license.clone(),
        }));

        let variables = Map::new();
        let request = ExecRequest {
            query: &case.query,
            operation_name: "",
            variables: &variables,
            actor,
        };
        let fields = root_fields(&schema, &request);
        let site = prefetch_site(
            &backend,
            &licenses,
            &FakeValidator {
                raw_site: case.raw.clone(),
            },
            &history,
            Some(&users),
            &fields,
            actor,
        )
        .await;

        let roots = FrontendRootResolver::new().with_site(site);
        let response = execute_with(&schema, request, Some(&roots));
        let state = licenses.license_state().await;

        let envelope = json!({
            "name": case.name,
            "license_decision": license_state_name(state),
            "response": mask_response(&response.to_json()),
        });
        let encoded = serde_json::to_string(&envelope).expect("encode");
        std::fs::write(
            std::path::Path::new(&out_dir).join(format!("{}.json", case.name)),
            encoded,
        )
        .expect("write case");
    }

    println!("RUST config-graphql cases={}", cases.len());
}
