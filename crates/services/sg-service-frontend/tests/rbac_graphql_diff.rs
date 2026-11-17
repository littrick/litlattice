//! M9.13b differential of the RBAC read-only GraphQL surface.
//!
//! The Go oracle (`cmd/frontend/graphqlbackend/zz_m9_13b_rbac_oracle_test.go`)
//! drives the real resolvers against `dbmocks.MockDB`; this harness drives the
//! ported prefetch functions against an in-memory [`RbacStore`] implementing
//! the same fixture semantics. Both emit one canonical JSON file per case:
//! `{data, errors}`.
//!
//! Env:
//!
//! * `SG_M9_13B_RBAC_DIFF=1`
//! * `M9_13B_RBAC_CORPUS` — JSON `[{name, query, uid, internal}]`
//! * `M9_13B_RBAC_OUT` — output directory (one `<name>.json` per case)

#![allow(clippy::unwrap_used)]

use std::collections::{BTreeMap, HashMap};
use std::path::Path;
use std::sync::Mutex;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::types::time::OffsetDateTime;

use sg_graphql::exec::{execute_with, root_fields, Actor, ExecRequest};
use sg_graphql::schema::assemble_sdl;
use sg_service_frontend::graphql_resolvers::{
    prefetch_users, FrontendRootResolver, NestedPrefetch, NodeRoots, UserData, UserKey,
    UserLookupStore,
};
use sg_service_frontend::rbac_graphql::{
    prefetch_rbac_nested, prefetch_rbac_nodes, prefetch_rbac_roots, OrderKind,
    PermissionListOptions, PermissionRow, RbacStore, RoleRow, RolesListOptions,
};

const SDL: &str = "
schema { query: Query }

type Query {
  node(id: ID!): Node
  currentUser: User
  user(username: String, databaseID: ID, email: String): User
  roles(first: Int, last: Int, after: String, before: String): RoleConnection!
  permissions(first: Int, last: Int, after: String, before: String): PermissionConnection!
}

interface Node { id: ID! }

type User implements Node {
  id: ID!
  username: String!
  roles(first: Int, last: Int, after: String, before: String): RoleConnection!
  permissions(first: Int, last: Int, after: String, before: String): PermissionConnection!
}

type Role implements Node {
  id: ID!
  name: String!
  system: Boolean!
  createdAt: DateTime!
  permissions(first: Int, last: Int, after: String, before: String): PermissionConnection!
}

type Permission implements Node {
  id: ID!
  namespace: PermissionNamespace!
  displayName: String!
  action: String!
  createdAt: DateTime!
}

enum PermissionNamespace { BATCH_CHANGES OWNERSHIP REPO_METADATA CODY PRODUCT_SUBSCRIPTIONS }

type RoleConnection {
  nodes: [Role!]!
  totalCount: Int!
  pageInfo: ConnectionPageInfo!
}

type PermissionConnection {
  nodes: [Permission!]!
  totalCount: Int!
  pageInfo: ConnectionPageInfo!
}

type ConnectionPageInfo {
  hasNextPage: Boolean!
  hasPreviousPage: Boolean!
  startCursor: String
  endCursor: String
}

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
}

struct World {
    users: BTreeMap<i32, UserData>,
    roles: Vec<RoleRow>,
    permissions: Vec<PermissionRow>,
    user_roles: HashMap<i32, Vec<i32>>,
    role_perms: HashMap<i32, Vec<i32>>,
}

fn time(offset: i64) -> OffsetDateTime {
    OffsetDateTime::from_unix_timestamp(1_700_000_000 + offset).unwrap()
}

fn user(id: i32, username: &str, display_name: &str, site_admin: bool) -> UserData {
    UserData {
        id,
        username: username.to_owned(),
        display_name: display_name.to_owned(),
        avatar_url: String::new(),
        created_at: time(0),
        updated_at: time(0),
        site_admin,
        tos_accepted: false,
        builtin_auth: false,
        scim_controlled: false,
        completions_quota: None,
        code_completions_quota: None,
        primary_email: String::new(),
    }
}

fn role(id: i32, name: &str, system: bool, offset: i64) -> RoleRow {
    RoleRow {
        id,
        name: name.to_owned(),
        system,
        created_at: time(offset),
    }
}

fn permission(id: i32, namespace: &str, action: &str, offset: i64) -> PermissionRow {
    PermissionRow {
        id,
        namespace: namespace.to_owned(),
        action: action.to_owned(),
        created_at: time(offset),
    }
}

impl World {
    fn new() -> Self {
        let mut users = BTreeMap::new();
        users.insert(1, user(1, "admin", "Admin", true));
        users.insert(3, user(3, "alice", "Alice", false));
        users.insert(4, user(4, "bob", "Bob", false));
        let mut user_roles = HashMap::new();
        user_roles.insert(1, vec![1, 2]);
        user_roles.insert(3, vec![1, 3]);
        user_roles.insert(4, vec![2]);
        let mut role_perms = HashMap::new();
        role_perms.insert(1, vec![10, 12]);
        role_perms.insert(2, vec![13]);
        role_perms.insert(3, vec![11]);
        World {
            users,
            roles: vec![
                role(1, "SYSTEM_READER", true, 1),
                role(2, "SYSTEM_ADMIN", true, 2),
                role(3, "CUSTOM_WRITER", false, 3),
                role(4, "CUSTOM_EMPTY", false, 4),
            ],
            permissions: vec![
                permission(10, "BATCH_CHANGES", "read", 1),
                permission(11, "BATCH_CHANGES", "write", 2),
                permission(12, "OWNERSHIP", "read", 3),
                permission(13, "CODY", "access", 4),
                permission(14, "BOGUS", "x", 5),
            ],
            user_roles,
            role_perms,
        }
    }

    fn has_role(&self, user_id: i32, role_id: i32) -> bool {
        self.user_roles
            .get(&user_id)
            .is_some_and(|ids| ids.contains(&role_id))
    }

    fn role(&self, id: i32) -> Option<&RoleRow> {
        self.roles.iter().find(|role| role.id == id)
    }

    fn permission(&self, id: i32) -> Option<&PermissionRow> {
        self.permissions.iter().find(|permission| permission.id == id)
    }

    fn role_less(order: OrderKind, a: &RoleRow, b: &RoleRow) -> bool {
        match order {
            OrderKind::RoleId => a.id > b.id,
            _ => {
                if a.system != b.system {
                    return a.system && !b.system;
                }
                if a.created_at != b.created_at {
                    return a.created_at > b.created_at;
                }
                a.id > b.id
            }
        }
    }

    fn role_after_reference(role: &RoleRow, reference: &RoleRow) -> bool {
        if role.system != reference.system {
            return !role.system && reference.system;
        }
        if role.created_at != reference.created_at {
            return role.created_at < reference.created_at;
        }
        role.id < reference.id
    }

    fn role_before_reference(role: &RoleRow, reference: &RoleRow) -> bool {
        if role.system != reference.system {
            return role.system && !reference.system;
        }
        if role.created_at != reference.created_at {
            return role.created_at > reference.created_at;
        }
        role.id > reference.id
    }

    fn role_list(&self, opts: &RolesListOptions) -> Result<Vec<RoleRow>, String> {
        let mut filtered: Vec<RoleRow> = self
            .roles
            .iter()
            .filter(|role| !(opts.system && !role.system))
            .filter(|role| opts.user_id == 0 || self.has_role(opts.user_id, role.id))
            .cloned()
            .collect();
        let mut order = OrderKind::RoleSystemCreatedAt;
        if let Some(pagination) = opts.pagination {
            order = pagination.order;
            if let Some(cursor) = pagination.after {
                if order == OrderKind::RoleId {
                    filtered.retain(|role| role.id < cursor);
                } else {
                    let Some(reference) = self.role(cursor) else {
                        return Err("unsupported role cursor".to_owned());
                    };
                    filtered.retain(|role| Self::role_after_reference(role, reference));
                }
            }
            if let Some(cursor) = pagination.before {
                if order == OrderKind::RoleId {
                    filtered.retain(|role| role.id > cursor);
                } else {
                    let Some(reference) = self.role(cursor) else {
                        return Err("unsupported role cursor".to_owned());
                    };
                    filtered.retain(|role| Self::role_before_reference(role, reference));
                }
            }
        }
        if opts.pagination.is_some_and(|pagination| pagination.last.is_some()) {
            filtered.sort_by(|a, b| {
                if Self::role_less(order, b, a) {
                    std::cmp::Ordering::Less
                } else if Self::role_less(order, a, b) {
                    std::cmp::Ordering::Greater
                } else {
                    std::cmp::Ordering::Equal
                }
            });
        } else {
            filtered.sort_by(|a, b| {
                if Self::role_less(order, a, b) {
                    std::cmp::Ordering::Less
                } else if Self::role_less(order, b, a) {
                    std::cmp::Ordering::Greater
                } else {
                    std::cmp::Ordering::Equal
                }
            });
        }
        if let Some(pagination) = opts.pagination {
            if let Some(first) = pagination.first {
                if filtered.len() > usize::try_from(first).unwrap_or(0) {
                    filtered.truncate(usize::try_from(first).unwrap_or(0));
                }
            } else if let Some(last) = pagination.last {
                if filtered.len() > usize::try_from(last).unwrap_or(0) {
                    filtered.truncate(usize::try_from(last).unwrap_or(0));
                }
            }
        }
        Ok(filtered)
    }

    fn permission_list(&self, opts: &PermissionListOptions) -> Vec<PermissionRow> {
        let mut seen = std::collections::BTreeSet::new();
        let mut filtered: Vec<PermissionRow> = Vec::new();
        for permission in &self.permissions {
            if opts.role_id != 0
                && !self
                    .role_perms
                    .get(&opts.role_id)
                    .is_some_and(|ids| ids.contains(&permission.id))
            {
                continue;
            }
            if opts.user_id != 0 {
                let found = self.user_roles.get(&opts.user_id).is_some_and(|roles| {
                    roles.iter().any(|role_id| {
                        self.role_perms
                            .get(role_id)
                            .is_some_and(|ids| ids.contains(&permission.id))
                    })
                });
                if !found {
                    continue;
                }
            }
            if !seen.insert(permission.id) {
                continue;
            }
            filtered.push(permission.clone());
        }
        if let Some(pagination) = opts.pagination {
            if let Some(cursor) = pagination.after {
                filtered.retain(|permission| permission.id < cursor);
            }
            if let Some(cursor) = pagination.before {
                filtered.retain(|permission| permission.id > cursor);
            }
        }
        if opts.pagination.is_some_and(|pagination| pagination.last.is_some()) {
            filtered.sort_by(|a, b| a.id.cmp(&b.id));
        } else {
            filtered.sort_by(|a, b| b.id.cmp(&a.id));
        }
        if let Some(pagination) = opts.pagination {
            if let Some(first) = pagination.first {
                if filtered.len() > usize::try_from(first).unwrap_or(0) {
                    filtered.truncate(usize::try_from(first).unwrap_or(0));
                }
            } else if let Some(last) = pagination.last {
                if filtered.len() > usize::try_from(last).unwrap_or(0) {
                    filtered.truncate(usize::try_from(last).unwrap_or(0));
                }
            }
        }
        filtered
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
}

#[async_trait]
impl RbacStore for Fake {
    async fn roles_count(&self, opts: &RolesListOptions) -> Result<i64, String> {
        let world = self.world.lock().unwrap();
        let count = world.role_list(&RolesListOptions {
            user_id: opts.user_id,
            ..RolesListOptions::default()
        })?;
        Ok(i64::try_from(count.len()).unwrap_or(i64::MAX))
    }

    async fn roles_list(&self, opts: &RolesListOptions) -> Result<Vec<RoleRow>, String> {
        let world = self.world.lock().unwrap();
        world.role_list(opts)
    }

    async fn role_get(&self, id: i32) -> Result<Option<RoleRow>, String> {
        let world = self.world.lock().unwrap();
        Ok(world.role(id).cloned())
    }

    async fn permissions_count(&self, opts: &PermissionListOptions) -> Result<i64, String> {
        let world = self.world.lock().unwrap();
        let count = world.permission_list(&PermissionListOptions {
            role_id: opts.role_id,
            user_id: opts.user_id,
            ..PermissionListOptions::default()
        });
        Ok(i64::try_from(count.len()).unwrap_or(i64::MAX))
    }

    async fn permissions_list(
        &self,
        opts: &PermissionListOptions,
    ) -> Result<Vec<PermissionRow>, String> {
        let world = self.world.lock().unwrap();
        Ok(world.permission_list(opts))
    }

    async fn permission_get(&self, id: i32) -> Result<Option<PermissionRow>, String> {
        let world = self.world.lock().unwrap();
        Ok(world.permission(id).cloned())
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
async fn rbac_graphql_diff_harness() {
    if std::env::var("SG_M9_13B_RBAC_DIFF").as_deref() != Ok("1") {
        return;
    }
    let corpus_path = std::env::var("M9_13B_RBAC_CORPUS").expect("corpus env");
    let out = std::env::var("M9_13B_RBAC_OUT").expect("out env");
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

        let users = prefetch_users(&fake, &fields).await;
        let users_connections = HashMap::new();
        let nodes = NodeRoots::default();

        let current_user = {
            let world = fake.world.lock().unwrap();
            if case.uid == 0 {
                None
            } else {
                world.users.get(&case.uid).cloned()
            }
        };

        let rbac_roots = prefetch_rbac_roots(&fake, &fields, actor, viewer_site_admin).await;
        let mut nested = NestedPrefetch::default();
        prefetch_rbac_nested(
            &fake,
            &fields,
            current_user.as_ref(),
            &users,
            &users_connections,
            &nodes,
            actor,
            viewer_site_admin,
            false,
            &mut nested,
        )
        .await;
        let rbac_nodes = prefetch_rbac_nodes(&fake, &fields, actor, viewer_site_admin).await;
        let mut nodes = nodes;
        if !rbac_nodes.is_empty() {
            nodes.rbac_nodes = rbac_nodes;
        }

        let user = current_user.map(|data| {
            sg_service_frontend::graphql_resolvers::UserObject::from_data_as_viewer(
                data,
                viewer_site_admin,
            )
        });
        let roots = FrontendRootResolver::with_prefetched_organizations(
            user,
            HashMap::new(),
            users,
            users_connections,
            HashMap::new(),
            HashMap::new(),
            viewer_site_admin,
        )
        .with_nested(std::sync::Arc::new(nested))
        .with_nodes(nodes)
        .with_rbac_roots(rbac_roots);

        let response = execute_with(&schema, request, Some(&roots));
        let (data, errors) = canonical_response(response);
        let envelope = Envelope { data, errors };
        let file = serde_json::to_string(&envelope).expect("encode");
        std::fs::write(Path::new(&out).join(format!("{}.json", case.name)), &file).expect("write");
        println!("RUST {} len={}", case.name, file.len());
    }
}
