//! M9.13b differential of the executor-secrets GraphQL surface.
//!
//! The Go oracle (`cmd/frontend/graphqlbackend/zz_m9_13b_executor_secrets_
//! oracle_test.go`) drives the real resolvers against `dbmocks.MockDB`; this
//! harness drives the ported prefetch functions against an in-memory
//! [`ExecutorSecretsStore`]/[`UserLookupStore`]/[`OrganizationStore`]/
//! [`OrgMembershipsStore`] implementing the same fixture semantics. Both emit
//! one canonical JSON file per case: `{data, errors, store}` where `store`
//! records the write effects the fake performed.
//!
//! Env:
//!
//! * `SG_M9_13B_EXECUTOR_SECRETS_DIFF=1`
//! * `M9_13B_EXECUTOR_SECRETS_CORPUS` — JSON `[{name, query, uid, internal}]`
//! * `M9_13B_EXECUTOR_SECRETS_OUT` — output directory (one `<name>.json`/case)

#![allow(clippy::unwrap_used)]

use std::collections::{BTreeMap, HashMap};
use std::path::Path;
use std::sync::Mutex;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::types::time::OffsetDateTime;

use sg_graphql::exec::{execute_with, is_mutation_operation, root_fields, Actor, ExecRequest};
use sg_graphql::schema::assemble_sdl;
use sg_service_frontend::executor_secrets_graphql::{
    prefetch_executor_secret_mutations, prefetch_executor_secret_nested,
    prefetch_executor_secret_nodes, prefetch_executor_secret_roots, prefetch_selected_secrets,
    selected_from_mutations, selected_from_roots, ExecutorSecretAccessLogRow,
    ExecutorSecretAccessLogsListOptions, ExecutorSecretRow, ExecutorSecretsListOptions,
    ExecutorSecretsStore, SecretViewer, DUPLICATE_EXECUTOR_SECRET,
};
use sg_service_frontend::graphql_resolvers::{
    prefetch_organizations, prefetch_users, FrontendRootResolver, MembershipData, NamespaceName,
    NestedPrefetch, NodeRoots, OrgByIdLookup, OrgData, OrgMembershipsStore, OrganizationStore,
    SettingsData, UserByIdLookup, UserData, UserKey, UserLookupStore, UserObject,
};

const SDL: &str = "
schema { query: Query mutation: Mutation }

type Query {
  node(id: ID!): Node
  currentUser: User
  user(username: String, databaseID: ID, email: String): User
  organization(name: String!): Org
  executorSecrets(scope: ExecutorSecretScope!, first: Int = 50, after: String): ExecutorSecretConnection!
}

type Mutation {
  createExecutorSecret(scope: ExecutorSecretScope!, key: String!, value: String!, namespace: ID): ExecutorSecret!
  updateExecutorSecret(scope: ExecutorSecretScope!, id: ID!, value: String!): ExecutorSecret!
  deleteExecutorSecret(scope: ExecutorSecretScope!, id: ID!): EmptyResponse
}

enum ExecutorSecretScope { BATCHES CODEINTEL }

interface Node { id: ID! }
interface Namespace { id: ID! }

type User implements Node & Namespace {
  id: ID!
  username: String!
  siteAdmin: Boolean!
  executorSecrets(scope: ExecutorSecretScope!, first: Int = 50, after: String): ExecutorSecretConnection!
}

type Org implements Node & Namespace {
  id: ID!
  name: String!
  executorSecrets(scope: ExecutorSecretScope!, first: Int = 50, after: String): ExecutorSecretConnection!
}

type ExecutorSecret implements Node {
  id: ID!
  key: String!
  scope: ExecutorSecretScope!
  overwritesGlobalSecret: Boolean!
  namespace: Namespace
  creator: User
  createdAt: DateTime!
  updatedAt: DateTime!
  accessLogs(first: Int = 50, after: String): ExecutorSecretAccessLogConnection!
}

type ExecutorSecretAccessLog implements Node {
  id: ID!
  executorSecret: ExecutorSecret!
  user: User
  machineUser: String!
  createdAt: DateTime!
}

type ExecutorSecretConnection {
  nodes: [ExecutorSecret!]!
  totalCount: Int!
  pageInfo: PageInfo!
}

type ExecutorSecretAccessLogConnection {
  nodes: [ExecutorSecretAccessLog!]!
  totalCount: Int!
  pageInfo: PageInfo!
}

type PageInfo {
  hasNextPage: Boolean!
  endCursor: String
}

type EmptyResponse { alwaysNil: String }

scalar DateTime
";

#[derive(Deserialize)]
struct Case {
    name: String,
    query: String,
    uid: i32,
    #[serde(default)]
    internal: bool,
}

#[derive(Serialize)]
struct Envelope {
    data: Value,
    errors: Vec<String>,
    store: Vec<String>,
}

#[derive(Clone)]
struct Secret {
    id: i64,
    key: String,
    scope: String,
    namespace_user_id: i32,
    namespace_org_id: i32,
    creator_id: i32,
    created_at: OffsetDateTime,
    updated_at: OffsetDateTime,
}

#[derive(Clone)]
struct Log {
    id: i64,
    executor_secret_id: i64,
    user_id: Option<i32>,
    created_at: OffsetDateTime,
}

struct World {
    users: BTreeMap<i32, UserData>,
    orgs: BTreeMap<i32, OrgData>,
    members: Vec<(i32, i32)>,
    secrets: Vec<Secret>,
    logs: Vec<Log>,
    next_secret: i64,
    store: Vec<String>,
}

fn epoch() -> OffsetDateTime {
    OffsetDateTime::from_unix_timestamp(1_700_000_000).unwrap()
}

fn now() -> OffsetDateTime {
    OffsetDateTime::from_unix_timestamp(1_700_000_100).unwrap()
}

fn log_time(offset: i64) -> OffsetDateTime {
    OffsetDateTime::from_unix_timestamp(1_700_000_000 + offset).unwrap()
}

fn user(id: i32, username: &str, display_name: &str, site_admin: bool) -> UserData {
    UserData {
        id,
        username: username.to_owned(),
        display_name: display_name.to_owned(),
        avatar_url: String::new(),
        created_at: epoch(),
        updated_at: epoch(),
        site_admin,
        tos_accepted: false,
        builtin_auth: false,
        scim_controlled: false,
        primary_email: String::new(),
    }
}

fn org(id: i32, name: &str, display_name: &str) -> OrgData {
    OrgData {
        id,
        name: name.to_owned(),
        display_name: Some(display_name.to_owned()),
        created_at: epoch(),
        viewer_can_administer: false,
        viewer_is_member: false,
    }
}

impl World {
    fn new() -> Self {
        let mut users = BTreeMap::new();
        users.insert(1, user(1, "admin", "Admin", true));
        users.insert(3, user(3, "alice", "Alice", false));
        users.insert(4, user(4, "bob", "Bob", false));
        users.insert(5, user(5, "carol", "Carol", false));
        let mut orgs = BTreeMap::new();
        orgs.insert(7, org(7, "acme", "Acme"));
        orgs.insert(8, org(8, "solo", "Solo"));
        orgs.insert(9, org(9, "adminonly", "Admin Only"));
        let secret = |id: i64, key: &str, scope: &str, user_id: i32, org_id: i32, creator: i32| {
            Secret {
                id,
                key: key.to_owned(),
                scope: scope.to_owned(),
                namespace_user_id: user_id,
                namespace_org_id: org_id,
                creator_id: creator,
                created_at: epoch(),
                updated_at: epoch(),
            }
        };
        World {
            users,
            orgs,
            members: vec![(7, 3), (7, 4), (8, 5), (9, 1)],
            secrets: vec![
                secret(100, "GLOBAL_ONE", "batches", 0, 0, 1),
                secret(101, "KEY_A", "batches", 3, 0, 3),
                secret(102, "KEY_A", "batches", 0, 0, 1),
                secret(103, "ORG_KEY", "batches", 0, 7, 1),
                secret(104, "ORG_KEY", "batches", 0, 0, 1),
                secret(105, "CI_KEY", "codeintel", 3, 0, 3),
                secret(106, "BOB_KEY", "batches", 4, 0, 4),
                secret(107, "ORG8_KEY", "batches", 0, 8, 0),
                secret(108, "ZETA", "batches", 0, 0, 1),
                secret(109, "OVERWRITE_ME", "batches", 0, 0, 1),
                secret(110, "OVERWRITE_ME", "batches", 3, 0, 3),
            ],
            logs: vec![
                Log {
                    id: 200,
                    executor_secret_id: 101,
                    user_id: Some(3),
                    created_at: log_time(1),
                },
                Log {
                    id: 201,
                    executor_secret_id: 101,
                    user_id: Some(4),
                    created_at: log_time(2),
                },
                Log {
                    id: 202,
                    executor_secret_id: 101,
                    user_id: None,
                    created_at: log_time(3),
                },
                Log {
                    id: 203,
                    executor_secret_id: 102,
                    user_id: Some(3),
                    created_at: log_time(4),
                },
                Log {
                    id: 204,
                    executor_secret_id: 105,
                    user_id: Some(3),
                    created_at: log_time(5),
                },
            ],
            next_secret: 1000,
            store: Vec::new(),
        }
    }

    fn user(&self, id: i32) -> Option<&UserData> {
        self.users.get(&id)
    }

    fn is_member(&self, org_id: i32, user_id: i32) -> bool {
        self.members
            .iter()
            .any(|(org, member)| *org == org_id && *member == user_id)
    }

    fn member_of_any_org(&self, user_id: i32) -> bool {
        self.members.iter().any(|(_, member)| *member == user_id)
    }

    fn org_data(&self, org: &OrgData, viewer_uid: i32, viewer_site_admin: bool) -> OrgData {
        let member = self.is_member(org.id, viewer_uid);
        OrgData {
            viewer_is_member: member,
            viewer_can_administer: member || viewer_site_admin,
            ..org.clone()
        }
    }

    fn by_id(&self, id: i64) -> Option<&Secret> {
        self.secrets.iter().find(|secret| secret.id == id)
    }

    fn visible(&self, secret: &Secret, viewer: SecretViewer) -> bool {
        if viewer.internal {
            return true;
        }
        if secret.namespace_user_id == 0 && secret.namespace_org_id == 0 {
            return true;
        }
        if secret.namespace_user_id != 0 && secret.namespace_user_id == viewer.uid {
            return true;
        }
        if secret.namespace_org_id != 0 && self.member_of_any_org(viewer.uid) {
            return true;
        }
        viewer.site_admin
    }

    fn namespace_match(secret: &Secret, opts: &ExecutorSecretsListOptions) -> bool {
        let global = secret.namespace_user_id == 0 && secret.namespace_org_id == 0;
        if opts.namespace_org_id != 0 {
            secret.namespace_org_id == opts.namespace_org_id || global
        } else if opts.namespace_user_id != 0 {
            secret.namespace_user_id == opts.namespace_user_id || global
        } else {
            global
        }
    }

    fn rank(secret: &Secret) -> (u8, i32) {
        if secret.namespace_user_id != 0 {
            (0, secret.namespace_user_id)
        } else if secret.namespace_org_id != 0 {
            (1, secret.namespace_org_id)
        } else {
            (2, 0)
        }
    }

    fn deduped(
        &self,
        scope: &str,
        opts: &ExecutorSecretsListOptions,
        viewer: SecretViewer,
    ) -> Vec<Secret> {
        let mut best: BTreeMap<String, Secret> = BTreeMap::new();
        for secret in &self.secrets {
            if secret.scope != scope
                || !Self::namespace_match(secret, opts)
                || !self.visible(secret, viewer)
            {
                continue;
            }
            match best.get(&secret.key) {
                None => {
                    best.insert(secret.key.clone(), secret.clone());
                }
                Some(current) => {
                    let (current_rank, current_namespace) = Self::rank(current);
                    let (rank, namespace) = Self::rank(secret);
                    if rank < current_rank || (rank == current_rank && namespace < current_namespace) {
                        best.insert(secret.key.clone(), secret.clone());
                    }
                }
            }
        }
        best.into_values().collect()
    }

    fn overwrites_global(&self, secret: &Secret) -> bool {
        self.secrets.iter().any(|other| {
            other.id != secret.id
                && other.key == secret.key
                && other.scope == secret.scope
                && other.namespace_user_id == 0
                && other.namespace_org_id == 0
        })
    }

    fn row(&self, secret: &Secret) -> ExecutorSecretRow {
        ExecutorSecretRow {
            id: secret.id,
            key: secret.key.clone(),
            scope: secret.scope.clone(),
            overwrites_global_secret: self.overwrites_global(secret),
            creator_id: secret.creator_id,
            namespace_user_id: secret.namespace_user_id,
            namespace_org_id: secret.namespace_org_id,
            created_at: secret.created_at,
            updated_at: secret.updated_at,
        }
    }

    fn log_row(log: &Log) -> ExecutorSecretAccessLogRow {
        ExecutorSecretAccessLogRow {
            id: log.id,
            executor_secret_id: log.executor_secret_id,
            user_id: log.user_id,
            machine_user: String::new(),
            created_at: log.created_at,
        }
    }
}

struct Fake {
    world: Mutex<World>,
}

impl Fake {
    fn new() -> Self {
        Self {
            world: Mutex::new(World::new()),
        }
    }

    fn snapshot(&self) -> (BTreeMap<i32, UserData>, BTreeMap<i32, OrgData>, Vec<(i32, i32)>) {
        let world = self.world.lock().unwrap();
        (world.users.clone(), world.orgs.clone(), world.members.clone())
    }

    fn take_store(&self) -> Vec<String> {
        std::mem::take(&mut self.world.lock().unwrap().store)
    }
}

#[async_trait]
impl ExecutorSecretsStore for Fake {
    async fn create(
        &self,
        scope: &str,
        key: &str,
        value: &str,
        creator_id: i32,
        namespace_user_id: i32,
        namespace_org_id: i32,
        _viewer: SecretViewer,
    ) -> Result<ExecutorSecretRow, String> {
        let mut world = self.world.lock().unwrap();
        if world.secrets.iter().any(|existing| {
            existing.scope == scope
                && existing.key == key
                && existing.namespace_user_id == namespace_user_id
                && existing.namespace_org_id == namespace_org_id
        }) {
            return Err(DUPLICATE_EXECUTOR_SECRET.to_owned());
        }
        let row = Secret {
            id: world.next_secret,
            key: key.to_owned(),
            scope: scope.to_owned(),
            namespace_user_id,
            namespace_org_id,
            creator_id,
            created_at: now(),
            updated_at: now(),
        };
        world.next_secret += 1;
        world.secrets.push(row.clone());
        world.store.push(format!(
            "create scope={} key={} creator={} user={} org={} value.len={}",
            scope,
            key,
            creator_id,
            namespace_user_id,
            namespace_org_id,
            value.len()
        ));
        Ok(world.row(&row))
    }

    async fn update(
        &self,
        scope: &str,
        secret: &ExecutorSecretRow,
        value: &str,
        _viewer: SecretViewer,
    ) -> Result<ExecutorSecretRow, String> {
        let mut world = self.world.lock().unwrap();
        let Some(row) = world.secrets.iter_mut().find(|row| row.id == secret.id) else {
            return Err("sql: no rows in result set".to_owned());
        };
        row.updated_at = now();
        let updated = row.clone();
        world.store.push(format!(
            "update id={} scope={} value.len={}",
            secret.id,
            scope,
            value.len()
        ));
        Ok(world.row(&updated))
    }

    async fn delete(
        &self,
        scope: &str,
        id: i64,
        _viewer: SecretViewer,
    ) -> Result<(), String> {
        let mut world = self.world.lock().unwrap();
        world.secrets.retain(|secret| secret.id != id);
        world.store.push(format!("delete id={} scope={}", id, scope));
        Ok(())
    }

    async fn get_by_id(
        &self,
        _scope: &str,
        id: i64,
        viewer: SecretViewer,
    ) -> Result<Option<ExecutorSecretRow>, String> {
        let world = self.world.lock().unwrap();
        Ok(world
            .by_id(id)
            .filter(|secret| world.visible(secret, viewer))
            .map(|secret| world.row(secret)))
    }

    async fn list(
        &self,
        scope: &str,
        opts: &ExecutorSecretsListOptions,
        viewer: SecretViewer,
    ) -> Result<(Vec<ExecutorSecretRow>, i32), String> {
        let world = self.world.lock().unwrap();
        let mut rows = world.deduped(scope, opts, viewer);
        let mut next = 0;
        if opts.limit != 0 {
            let start = usize::try_from(opts.offset.max(0)).unwrap_or(0).min(rows.len());
            rows = rows.split_off(start);
            let limit = usize::try_from(opts.limit).unwrap_or(0);
            if rows.len() > limit + 1 {
                rows.truncate(limit + 1);
            }
            if rows.len() == limit + 1 {
                next = opts.offset + opts.limit;
                rows.truncate(limit);
            }
        }
        let out = rows.iter().map(|secret| world.row(secret)).collect();
        Ok((out, next))
    }

    async fn count(
        &self,
        scope: &str,
        opts: &ExecutorSecretsListOptions,
        viewer: SecretViewer,
    ) -> Result<i32, String> {
        let world = self.world.lock().unwrap();
        Ok(i32::try_from(world.deduped(scope, opts, viewer).len()).unwrap_or(i32::MAX))
    }

    async fn list_access_logs(
        &self,
        opts: &ExecutorSecretAccessLogsListOptions,
    ) -> Result<(Vec<ExecutorSecretAccessLogRow>, i32), String> {
        let world = self.world.lock().unwrap();
        let mut rows: Vec<&Log> = world
            .logs
            .iter()
            .filter(|log| {
                opts.executor_secret_id == 0 || log.executor_secret_id == opts.executor_secret_id
            })
            .collect();
        rows.sort_by(|a, b| b.created_at.cmp(&a.created_at));
        let mut next = 0;
        if opts.limit != 0 {
            let start = usize::try_from(opts.offset.max(0)).unwrap_or(0).min(rows.len());
            rows = rows.split_off(start);
            let limit = usize::try_from(opts.limit).unwrap_or(0);
            if rows.len() > limit + 1 {
                rows.truncate(limit + 1);
            }
            if rows.len() == limit + 1 {
                next = opts.offset + opts.limit;
                rows.truncate(limit);
            }
        }
        let out = rows.iter().map(|log| World::log_row(log)).collect();
        Ok((out, next))
    }

    async fn count_access_logs(
        &self,
        opts: &ExecutorSecretAccessLogsListOptions,
    ) -> Result<i32, String> {
        let world = self.world.lock().unwrap();
        let count = world
            .logs
            .iter()
            .filter(|log| {
                opts.executor_secret_id == 0 || log.executor_secret_id == opts.executor_secret_id
            })
            .count();
        Ok(i32::try_from(count).unwrap_or(i32::MAX))
    }

    async fn get_access_log_by_id(
        &self,
        id: i64,
    ) -> Result<Option<ExecutorSecretAccessLogRow>, String> {
        let world = self.world.lock().unwrap();
        Ok(world
            .logs
            .iter()
            .find(|log| log.id == id)
            .map(World::log_row))
    }

    async fn is_org_member(&self, org_id: i32, user_id: i32) -> Result<bool, String> {
        let world = self.world.lock().unwrap();
        Ok(world.is_member(org_id, user_id))
    }
}

#[async_trait]
impl UserLookupStore for Fake {
    async fn lookup(&self, key: &UserKey) -> Result<Option<UserData>, String> {
        let world = self.world.lock().unwrap();
        let found = match key {
            UserKey::Username(username) => world
                .users
                .values()
                .find(|user| &user.username == username)
                .cloned(),
            UserKey::DatabaseId(id) => world.users.get(id).cloned(),
            UserKey::Email(email) => world
                .users
                .values()
                .find(|user| &user.primary_email == email)
                .cloned(),
        };
        Ok(found)
    }
}

#[async_trait]
impl OrganizationStore for Fake {
    async fn organization(
        &self,
        name: &str,
        viewer_uid: i32,
        viewer_site_admin: bool,
    ) -> Result<Option<OrgData>, String> {
        let world = self.world.lock().unwrap();
        Ok(world
            .orgs
            .values()
            .find(|org| org.name == name)
            .map(|org| world.org_data(org, viewer_uid, viewer_site_admin)))
    }
}

#[async_trait]
impl OrgMembershipsStore for Fake {
    async fn memberships_for_user(&self, user_id: i32) -> Result<Vec<MembershipData>, String> {
        let world = self.world.lock().unwrap();
        Ok(world
            .members
            .iter()
            .enumerate()
            .filter(|(_, (_, member))| *member == user_id)
            .map(|(index, (org_id, _))| MembershipData {
                id: i32::try_from(index).unwrap_or(0) + 1,
                org_id: *org_id,
                user_id,
                created_at: epoch(),
                updated_at: epoch(),
            })
            .collect())
    }

    async fn namespace_by_name(&self, name: &str) -> Result<Option<NamespaceName>, String> {
        let world = self.world.lock().unwrap();
        if let Some(user) = world
            .users
            .values()
            .find(|user| user.username.eq_ignore_ascii_case(name))
        {
            return Ok(Some(NamespaceName {
                name: user.username.clone(),
                user_id: user.id,
                org_id: 0,
            }));
        }
        Ok(world
            .orgs
            .values()
            .find(|org| org.name.eq_ignore_ascii_case(name))
            .map(|org| NamespaceName {
                name: org.name.clone(),
                user_id: 0,
                org_id: org.id,
            }))
    }

    async fn org_by_id(
        &self,
        org_id: i32,
        viewer_uid: i32,
        viewer_site_admin: bool,
    ) -> Result<OrgData, String> {
        let world = self.world.lock().unwrap();
        world
            .orgs
            .get(&org_id)
            .map(|org| world.org_data(org, viewer_uid, viewer_site_admin))
            .ok_or_else(|| format!("org not found: id {org_id}"))
    }

    async fn user_by_id(&self, user_id: i32) -> Result<UserData, String> {
        let world = self.world.lock().unwrap();
        world
            .user(user_id)
            .cloned()
            .ok_or_else(|| format!("user not found: [{user_id}]"))
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

fn canonical_response(response: sg_graphql::exec::ExecResponse) -> (Value, Vec<String>) {
    let value: Value = serde_json::from_str(&response.to_json()).unwrap_or(Value::Null);
    let data = value.get("data").cloned().unwrap_or(Value::Null);
    let errors = value
        .get("errors")
        .and_then(Value::as_array)
        .map(|items| {
            items
                .iter()
                .map(|item| {
                    item.get("message")
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                        .to_owned()
                })
                .collect()
        })
        .unwrap_or_default();
    (data, errors)
}

#[tokio::test]
async fn executor_secrets_diff_harness() {
    if std::env::var("SG_M9_13B_EXECUTOR_SECRETS_DIFF").as_deref() != Ok("1") {
        return;
    }
    let corpus_path = std::env::var("M9_13B_EXECUTOR_SECRETS_CORPUS").expect("corpus env");
    let out = std::env::var("M9_13B_EXECUTOR_SECRETS_OUT").expect("out env");
    std::fs::create_dir_all(&out).expect("out dir");
    let cases: Vec<Case> =
        serde_json::from_slice(&std::fs::read(corpus_path).expect("corpus")).expect("corpus json");
    let schema = assemble_sdl(SDL).expect("schema");

    for case in cases {
        let actor = if case.internal {
            Actor::Internal
        } else if case.uid == 0 {
            Actor::Anonymous
        } else {
            Actor::User { uid: case.uid }
        };
        let viewer_site_admin = case.internal || matches!(case.uid, 1);
        let request = ExecRequest {
            query: &case.query,
            operation_name: "",
            variables: &serde_json::Map::new(),
            actor,
        };
        let fields = root_fields(&schema, &request);
        let fake = Fake::new();
        let (world_users, world_orgs, _members) = fake.snapshot();

        let users = prefetch_users(&fake, &fields).await;
        let organizations = prefetch_organizations(&fake, &fields, actor, viewer_site_admin).await;
        let users_connections = HashMap::new();

        let mut nodes = NodeRoots::default();
        for (id, data) in &world_users {
            nodes.users.insert(*id, UserByIdLookup::Found(data.clone()));
        }
        for (id, data) in &world_orgs {
            nodes.orgs.insert(
                *id,
                OrgByIdLookup::Found(org_viewer(data, case.uid, viewer_site_admin)),
            );
        }

        let current_user = if case.uid == 0 {
            None
        } else {
            world_users.get(&case.uid).cloned()
        };

        let executor_secret_roots =
            prefetch_executor_secret_roots(&fake, &fields, actor, viewer_site_admin).await;
        let mut selected = selected_from_roots(&executor_secret_roots, &fields);
        let mut nested = NestedPrefetch::default();
        let mut nested_selected = prefetch_executor_secret_nested(
            &fake,
            &fields,
            current_user.as_ref(),
            &users,
            &users_connections,
            &organizations,
            &nodes,
            actor,
            viewer_site_admin,
            &mut nested,
        )
        .await;
        selected.secrets.append(&mut nested_selected.secrets);
        selected.access_logs.append(&mut nested_selected.access_logs);

        let (secret_nodes, mut node_selected) =
            prefetch_executor_secret_nodes(&fake, &fields, actor, viewer_site_admin).await;
        selected.secrets.append(&mut node_selected.secrets);
        selected.access_logs.append(&mut node_selected.access_logs);
        if !secret_nodes.secrets.is_empty() {
            nodes.executor_secrets = secret_nodes.secrets;
        }
        if !secret_nodes.access_logs.is_empty() {
            nodes.executor_secret_access_logs = secret_nodes.access_logs;
        }

        let mut mutations = Vec::new();
        if is_mutation_operation(&request) {
            mutations = prefetch_executor_secret_mutations(&fake, &fields, actor, viewer_site_admin)
                .await;
            let mut mutation_selected = selected_from_mutations(&mutations, &fields);
            selected.secrets.append(&mut mutation_selected.secrets);
            selected.access_logs.append(&mut mutation_selected.access_logs);
        }
        prefetch_selected_secrets(
            &fake,
            Some(&fake as &dyn UserLookupStore),
            Some(&fake as &dyn OrgMembershipsStore),
            &selected,
            case.uid,
            viewer_site_admin,
            &mut nested,
        )
        .await;

        let user = current_user
            .map(|data| UserObject::from_data_as_viewer(data, viewer_site_admin));
        let nested = std::sync::Arc::new(nested);
        let mutation_nested = std::sync::Arc::clone(&nested);
        let mut roots = FrontendRootResolver::with_prefetched_organizations(
            user,
            HashMap::new(),
            users,
            users_connections,
            organizations,
            HashMap::new(),
            viewer_site_admin,
        )
        .with_nested(nested)
        .with_nodes(nodes)
        .with_executor_secret_roots(executor_secret_roots);
        if !mutations.is_empty() {
            roots = roots.with_executor_secret_mutations(
                viewer_site_admin,
                mutation_nested,
                mutations,
            );
        }

        let response = execute_with(&schema, request, Some(&roots));
        let (data, errors) = canonical_response(response);
        let envelope = Envelope {
            data,
            errors,
            store: fake.take_store(),
        };
        let file = serde_json::to_string(&envelope).expect("encode");
        std::fs::write(Path::new(&out).join(format!("{}.json", case.name)), &file).expect("write");
        println!("RUST {} len={}", case.name, file.len());
    }
}

fn org_viewer(org: &OrgData, viewer_uid: i32, viewer_site_admin: bool) -> OrgData {
    let member = org.name == "acme" && matches!(viewer_uid, 3 | 4)
        || org.name == "solo" && viewer_uid == 5
        || org.name == "adminonly" && viewer_uid == 1;
    OrgData {
        viewer_is_member: member,
        viewer_can_administer: member || viewer_site_admin,
        ..org.clone()
    }
}
