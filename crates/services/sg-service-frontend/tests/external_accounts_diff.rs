//! M9.8ka differential of the external-account GraphQL surface:
//! `User.externalAccounts` (pagination/permission/next-page probe),
//! `Query.node`'s `ExternalAccount` dispatch, the `ExternalAccount` node
//! fields, and `publicAccountDataFromJSON` (known/unknown provider and empty
//! fields), against the Go oracle
//! (`cmd/frontend/graphqlbackend/zz_m9ka_oracle_test.go`, untracked).
//!
//! Both sides run the same corpus. The Go oracle uses `dbmocks` + fake
//! `providers.MockProviders`; this harness uses in-memory seams and the same
//! `root_fields` + `prefetch_*` + `execute_with` pipeline as production.
//!
//! Env:
//!
//! * `SG_M9KA_DIFF=1`
//! * `M9KA_CORPUS` — JSON `{users, accounts, cases}` (see [`Corpus`])
//! * `M9KA_OUT` — output directory (one `<name>.json` per case)

#![allow(clippy::unwrap_used, clippy::too_many_arguments)]

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Map;
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;

use sg_encryption::Key;
use sg_graphql::exec::{execute_with, root_fields, Actor, ExecRequest};
use sg_graphql::schema::assemble_sdl;
use sg_service_frontend::external_accounts_graphql::{
    marshal_external_account_id, prefetch_external_account_nodes, public_account_data_from_json,
    CodySubscriptionConfig, ExternalAccountInfoProvider, ExternalAccountProviderRegistry,
    ExternalAccountRow, ExternalAccountsStore, PublicAccountData,
};
use sg_service_frontend::graphql_resolvers::{
    prefetch_nested, prefetch_user_external_accounts, prefetch_users, FrontendRootResolver,
    MembersArgs, MembersConnectionData, MembershipData, NamespaceName, NodeRoots, OrgData,
    OrgMembersStore, OrgMembershipsStore, SettingsData, UserData, UserKey, UserLookupStore,
    UserObject, UserOrganizationsStore,
};

const SDL: &str = "
schema { query: Query }

scalar DateTime

type Query {
  node(id: ID!): Node
  user(username: String!): User
}

interface Node {
  id: ID!
}

type User implements Node {
  id: ID!
  username: String!
  externalAccounts(first: Int): ExternalAccountConnection!
}

type ExternalAccount implements Node {
  id: ID!
  user: User!
  serviceType: String!
  serviceID: String!
  clientID: String!
  accountID: String!
  createdAt: DateTime!
  updatedAt: DateTime!
  refreshURL: String
  publicAccountData: PublicExternalAccountData
  codySubscription: CodySubscription
}

type ExternalAccountConnection {
  nodes: [ExternalAccount!]!
  totalCount: Int!
  pageInfo: PageInfo!
}

type PageInfo {
  hasNextPage: Boolean!
  endCursor: String
}

type PublicExternalAccountData {
  displayName: String
  login: String
  url: String
}

type CodySubscription {
  status: CodySubscriptionStatus!
}

enum CodySubscriptionStatus {
  ACTIVE
  PAST_DUE
  UNPAID
  CANCELED
  TRIALING
  OTHER
}
";

const CONNECTION_FIELDS: &str = "nodes { id user { username } serviceType serviceID clientID accountID createdAt updatedAt refreshURL publicAccountData { displayName login url } } totalCount pageInfo { hasNextPage endCursor }";
const NODE_FIELDS: &str = "id user { username } serviceType serviceID clientID accountID createdAt updatedAt refreshURL publicAccountData { displayName login url }";

#[derive(Deserialize)]
struct UserFixture {
    id: i32,
    username: String,
    #[serde(default, rename = "siteAdmin")]
    site_admin: bool,
}

#[derive(Deserialize)]
struct AccountFixture {
    id: i32,
    #[serde(rename = "userId")]
    user_id: i32,
    #[serde(rename = "serviceType")]
    service_type: String,
    #[serde(rename = "serviceId")]
    service_id: String,
    #[serde(rename = "clientId")]
    client_id: String,
    #[serde(rename = "accountId")]
    account_id: String,
    #[serde(default, rename = "accountData")]
    account_data: Option<String>,
    #[serde(default, rename = "encryptionKeyId")]
    encryption_key_id: String,
    #[serde(rename = "createdAt")]
    created_at: String,
    #[serde(rename = "updatedAt")]
    updated_at: String,
}

#[derive(Deserialize)]
struct Case {
    name: String,
    kind: String,
    #[serde(default)]
    uid: i32,
    #[serde(default)]
    username: Option<String>,
    #[serde(default)]
    first: Option<i32>,
    #[serde(default, rename = "accountId")]
    account_id: Option<i32>,
    #[serde(default, rename = "serviceType")]
    service_type: Option<String>,
}

#[derive(Deserialize)]
struct Corpus {
    users: Vec<UserFixture>,
    accounts: Vec<AccountFixture>,
    cases: Vec<Case>,
}

fn parse_time(value: &str) -> OffsetDateTime {
    OffsetDateTime::parse(value, &Rfc3339).expect("RFC3339 timestamp")
}

fn account_row(fixture: &AccountFixture) -> ExternalAccountRow {
    ExternalAccountRow {
        id: fixture.id,
        user_id: fixture.user_id,
        service_type: fixture.service_type.clone(),
        service_id: fixture.service_id.clone(),
        client_id: fixture.client_id.clone(),
        account_id: fixture.account_id.clone(),
        account_data: fixture.account_data.clone(),
        encryption_key_id: fixture.encryption_key_id.clone(),
        created_at: parse_time(&fixture.created_at),
        updated_at: parse_time(&fixture.updated_at),
    }
}

struct InMemoryExternalAccounts {
    accounts: Vec<ExternalAccountRow>,
}

#[async_trait]
impl ExternalAccountsStore for InMemoryExternalAccounts {
    async fn list_for_user(&self, user_id: i32) -> Result<Vec<ExternalAccountRow>, String> {
        let mut rows: Vec<ExternalAccountRow> = self
            .accounts
            .iter()
            .filter(|row| row.user_id == user_id)
            .cloned()
            .collect();
        rows.sort_by_key(|row| row.id);
        Ok(rows)
    }

    async fn count_for_user(&self, user_id: i32) -> Result<i64, String> {
        Ok(self
            .accounts
            .iter()
            .filter(|row| row.user_id == user_id)
            .count() as i64)
    }

    async fn get_by_id(&self, id: i32) -> Result<Option<ExternalAccountRow>, String> {
        Ok(self.accounts.iter().find(|row| row.id == id).cloned())
    }
}

struct FakeProvider {
    service_type: String,
    data: Option<PublicAccountData>,
    error: Option<String>,
}

#[async_trait]
impl ExternalAccountInfoProvider for FakeProvider {
    fn service_type(&self) -> &str {
        &self.service_type
    }

    async fn external_account_info(
        &self,
        _account: &ExternalAccountRow,
        _key: Option<&dyn Key>,
    ) -> Result<Option<PublicAccountData>, String> {
        match &self.error {
            Some(error) => Err(error.clone()),
            None => Ok(self.data.clone()),
        }
    }
}

fn fake_registry() -> ExternalAccountProviderRegistry {
    let mut registry = ExternalAccountProviderRegistry::new();
    registry.register(Arc::new(FakeProvider {
        service_type: "known".to_owned(),
        data: Some(PublicAccountData {
            display_name: "Known Display".to_owned(),
            login: "known-login".to_owned(),
            url: "https://example.com/known".to_owned(),
        }),
        error: None,
    }));
    registry.register(Arc::new(FakeProvider {
        service_type: "empty".to_owned(),
        data: Some(PublicAccountData::default()),
        error: None,
    }));
    registry.register(Arc::new(FakeProvider {
        service_type: "nil".to_owned(),
        data: None,
        error: None,
    }));
    registry.register(Arc::new(FakeProvider {
        service_type: "boom".to_owned(),
        data: None,
        error: Some("provider exploded".to_owned()),
    }));
    registry
}

struct InMemoryUsers {
    by_username: HashMap<String, UserData>,
    by_id: HashMap<i32, UserData>,
}

fn user_data(fixture: &UserFixture) -> UserData {
    UserData {
        id: fixture.id,
        username: fixture.username.clone(),
        display_name: fixture.username.clone(),
        avatar_url: String::new(),
        created_at: parse_time("2024-01-02T03:04:05Z"),
        updated_at: parse_time("2024-02-03T04:05:06Z"),
        site_admin: fixture.site_admin,
        tos_accepted: false,
        primary_email: String::new(),
    }
}

#[async_trait]
impl UserLookupStore for InMemoryUsers {
    async fn lookup(&self, key: &UserKey) -> Result<Option<UserData>, String> {
        Ok(match key {
            UserKey::Username(username) => self.by_username.get(username).cloned(),
            UserKey::DatabaseId(id) => self.by_id.get(id).cloned(),
            UserKey::Email(_) => None,
        })
    }
}

struct FakeNested;

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
        Ok(None)
    }
}

/// Field order matches the Go oracle's `m9kaDirectJSON`.
#[derive(Serialize, Default)]
struct DirectEnvelope {
    data: Option<DirectData>,
    error: String,
}

#[derive(Serialize)]
struct DirectData {
    #[serde(rename = "displayName")]
    display_name: String,
    login: String,
    url: String,
}

fn query_for(case: &Case) -> String {
    match case.kind.as_str() {
        "connection" => {
            let first = case
                .first
                .map(|first| format!("(first: {first})"))
                .unwrap_or_default();
            format!(
                r#"query {{ user(username: {:?}) {{ externalAccounts{first} {{ {CONNECTION_FIELDS} }} }} }}"#,
                case.username.as_deref().expect("username")
            )
        }
        "node" => {
            let id = marshal_external_account_id(case.account_id.expect("accountId"));
            format!(
                r#"query {{ node(id: {id:?}) {{ ... on ExternalAccount {{ {NODE_FIELDS} }} }} }}"#
            )
        }
        "node_cody" => {
            let id = marshal_external_account_id(case.account_id.expect("accountId"));
            format!(
                r#"query {{ node(id: {id:?}) {{ ... on ExternalAccount {{ codySubscription {{ status }} }} }} }}"#
            )
        }
        other => panic!("unknown kind {other}"),
    }
}

#[tokio::test]
async fn external_accounts_graphql_harness() {
    if std::env::var("SG_M9KA_DIFF").as_deref() != Ok("1") {
        return;
    }
    let corpus_path = std::env::var("M9KA_CORPUS").expect("M9KA_CORPUS");
    let out = std::env::var("M9KA_OUT").expect("M9KA_OUT");
    std::fs::create_dir_all(&out).expect("out dir");
    let corpus: Corpus =
        serde_json::from_slice(&std::fs::read(corpus_path).expect("corpus")).expect("corpus json");

    let store = InMemoryExternalAccounts {
        accounts: corpus.accounts.iter().map(account_row).collect(),
    };
    let registry = fake_registry();
    let users_store = InMemoryUsers {
        by_username: corpus
            .users
            .iter()
            .map(|fixture| (fixture.username.clone(), user_data(fixture)))
            .collect(),
        by_id: corpus
            .users
            .iter()
            .map(|fixture| (fixture.id, user_data(fixture)))
            .collect(),
    };
    let nested_store = FakeNested;
    let schema = assemble_sdl(SDL).expect("schema");
    let cody_config = Arc::new(CodySubscriptionConfig {
        dotcom: false,
        sams_service_id: "https://accounts.sourcegraph.com".to_owned(),
    });

    for case in &corpus.cases {
        let actor = if case.uid == 0 {
            Actor::Anonymous
        } else {
            Actor::User { uid: case.uid }
        };
        let current_user = if case.uid == 0 {
            None
        } else {
            users_store
                .lookup(&UserKey::DatabaseId(case.uid))
                .await
                .expect("user lookup")
        };
        let viewer_site_admin = current_user
            .as_ref()
            .map(|user| user.site_admin)
            .unwrap_or(false);

        if case.kind == "public_data" {
            let row = ExternalAccountRow {
                id: 0,
                user_id: 0,
                service_type: case
                    .service_type
                    .clone()
                    .expect("serviceType for public_data"),
                service_id: String::new(),
                client_id: String::new(),
                account_id: String::new(),
                account_data: None,
                encryption_key_id: String::new(),
                created_at: parse_time("2024-01-02T03:04:05Z"),
                updated_at: parse_time("2024-02-03T04:05:06Z"),
            };
            let mut envelope = DirectEnvelope::default();
            match public_account_data_from_json(&registry, &row, None).await {
                Ok(Some(data)) => {
                    envelope.data = Some(DirectData {
                        display_name: data.display_name,
                        login: data.login,
                        url: data.url,
                    });
                }
                Ok(None) => {}
                Err(error) => envelope.error = error,
            }
            let body = serde_json::to_string(&envelope).expect("encode");
            std::fs::write(Path::new(&out).join(format!("{}.json", case.name)), &body)
                .expect("write");
            println!("RUST {} len={}", case.name, body.len());
            continue;
        }

        let variables = Map::new();
        let query = query_for(case);
        let request = ExecRequest {
            query: &query,
            operation_name: "",
            variables: &variables,
            actor,
        };
        let fields = root_fields(&schema, &request);
        let users = prefetch_users(&users_store, &fields).await;
        let nodes = NodeRoots {
            external_accounts: prefetch_external_account_nodes(
                &store,
                &registry,
                None,
                Some(&users_store),
                &fields,
                actor,
                viewer_site_admin,
            )
            .await,
            ..NodeRoots::default()
        };
        let mut nested = prefetch_nested(
            &nested_store,
            &nested_store,
            &nested_store,
            &fields,
            current_user.as_ref(),
            &users,
            &HashMap::new(),
            &HashMap::new(),
            &HashMap::new(),
            &nodes,
            actor,
            viewer_site_admin,
        )
        .await;
        nested.user_external_accounts = prefetch_user_external_accounts(
            &store,
            &registry,
            None,
            &fields,
            current_user.as_ref(),
            &users,
            &HashMap::new(),
            &nodes,
            actor,
            viewer_site_admin,
        )
        .await;
        nested.external_account_cody = Some(cody_config.clone());

        let roots = FrontendRootResolver::with_prefetched_organizations(
            current_user.clone().map(UserObject::from_data),
            HashMap::new(),
            users,
            HashMap::new(),
            HashMap::new(),
            HashMap::new(),
            viewer_site_admin,
        )
        .with_nested(Arc::new(nested))
        .with_nodes(nodes);
        let response = execute_with(&schema, request, Some(&roots));
        let body = response.to_json();
        std::fs::write(Path::new(&out).join(format!("{}.json", case.name)), &body).expect("write");
        println!("RUST {} len={}", case.name, body.len());
    }
}
