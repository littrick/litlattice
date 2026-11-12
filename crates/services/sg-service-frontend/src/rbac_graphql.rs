#![forbid(unsafe_code)]

//! RBAC read-only GraphQL surface (M9.13b): the Rust port of
//! `cmd/frontend/graphqlbackend/{rbac.go,roles.go,role.go,permissions.go,
//! permission.go,role_connection_store.go,permission_connection_store.go}`,
//! `internal/database/{roles.go,permissions.go}` and the generic
//! `gqlutil.ConnectionResolver` (`internal/gqlutil/connection_resolver.go`).
//!
//! Ported fields:
//! - `Query.roles` / `Query.permissions` (site-admin only; the vendored SDL
//!   has no `user`/`system`/`role` arguments).
//! - `User.roles` (dotcom gate) / `User.permissions`
//!   (`CheckSiteAdminOrSameUserFromActor`).
//! - `Role.permissions` (site-admin only) and the `Role`/`Permission` node
//!   fields.
//! - `Query.node(kind: Role | Permission)`.
//! - `gqlutil.ConnectionResolver` pagination: `MaxPageSize = 100`,
//!   `First`/`Last` + `after`/`before` cursors (cursor = node Relay id),
//!   `Last` reversal/trim, `ConnectionPageInfo` formulas and `AllowNoLimit`.
//!
//! Not ported: the RBAC mutations (`createRole`, `deleteRole`, `setRoles`,
//! `setPermissions`) only exist in `rbac.graphql`, which is not part of the
//! vendored SDL (overview §4).

use async_trait::async_trait;
use base64::Engine as _;
use std::collections::HashMap;
use std::sync::Arc;
use time::OffsetDateTime;

use sg_graphql::exec::{Actor, Arguments, ExecError, FieldValue, ObjectValue, RootField};
use sg_graphql::json::Json;

use crate::admin_store::{
    check_current_user_is_site_admin, check_site_admin_or_same_user, MUST_BE_SITE_ADMIN_OR_SAME_USER,
};
use crate::graphql_resolvers::{
    datetime_rfc3339, relay_kind, unmarshal_relay_id, ConnectionPageInfoObject, NestedPrefetch,
    NodeRoots, UserData, UserKey, UserLookup, UsersArgs, UsersConnectionLookup,
};

/// `gqlutil.DefaultMaxPageSize`.
pub const DEFAULT_MAX_PAGE_SIZE: i32 = 100;

/// One `roles` row (`internal/types/types.go`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RoleRow {
    pub id: i32,
    pub name: String,
    pub system: bool,
    pub created_at: OffsetDateTime,
}

/// One `permissions` row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PermissionRow {
    pub id: i32,
    pub namespace: String,
    pub action: String,
    pub created_at: OffsetDateTime,
}

/// `rtypes.PermissionNamespace.Valid`.
pub fn permission_namespace_is_valid(namespace: &str) -> bool {
    matches!(
        namespace,
        "BATCH_CHANGES" | "OWNERSHIP" | "REPO_METADATA" | "CODY" | "PRODUCT_SUBSCRIPTIONS"
    )
}

/// The `order by` a connection store uses. `Query.roles` uses
/// `roles.system, roles.created_at`; `User.roles` is constructed without an
/// `OrderBy`, so `NewConnectionResolver` fills in `id`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderKind {
    RoleSystemCreatedAt,
    RoleId,
    PermissionId,
}

/// `database.PaginationArgs` restricted to the values these stores use. The
/// cursor is the node id (a single SQL value).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PaginationArgs {
    pub first: Option<i32>,
    pub last: Option<i32>,
    pub after: Option<i32>,
    pub before: Option<i32>,
    pub ascending: bool,
    pub order: OrderKind,
}

/// `database.RolesListOptions`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RolesListOptions {
    pub pagination: Option<PaginationArgs>,
    pub system: bool,
    pub user_id: i32,
}

/// `database.PermissionListOpts`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PermissionListOptions {
    pub pagination: Option<PaginationArgs>,
    pub role_id: i32,
    pub user_id: i32,
}

/// The store seam for `roles` / `permissions` / `role_permissions` /
/// `user_roles`. The production implementation is [`PgRbacStore`]; the
/// differential harness substitutes an in-memory fake.
#[async_trait]
pub trait RbacStore: Send + Sync {
    async fn roles_count(&self, opts: &RolesListOptions) -> Result<i64, String>;
    async fn roles_list(&self, opts: &RolesListOptions) -> Result<Vec<RoleRow>, String>;
    async fn role_get(&self, id: i32) -> Result<Option<RoleRow>, String>;
    async fn permissions_count(&self, opts: &PermissionListOptions) -> Result<i64, String>;
    async fn permissions_list(
        &self,
        opts: &PermissionListOptions,
    ) -> Result<Vec<PermissionRow>, String>;
    async fn permission_get(&self, id: i32) -> Result<Option<PermissionRow>, String>;
}

// ---------------------------------------------------------------------------
// Relay ids
// ---------------------------------------------------------------------------

/// `MarshalRoleID`.
pub fn marshal_role_id(id: i32) -> String {
    base64::engine::general_purpose::URL_SAFE.encode(format!("Role:{id}"))
}

/// `UnmarshalRoleID` (`relay.UnmarshalSpec`; the kind is not validated).
pub fn unmarshal_role_id(raw: &str) -> Result<i32, String> {
    unmarshal_relay_id(raw)
}

/// `MarshalPermissionID`.
pub fn marshal_permission_id(id: i32) -> String {
    base64::engine::general_purpose::URL_SAFE.encode(format!("Permission:{id}"))
}

/// `UnmarshalPermissionID`.
pub fn unmarshal_permission_id(raw: &str) -> Result<i32, String> {
    unmarshal_relay_id(raw)
}

// ---------------------------------------------------------------------------
// Connection arguments + gqlutil semantics
// ---------------------------------------------------------------------------

/// `gqlutil.ConnectionResolverArgs` from the GraphQL arguments.
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash)]
pub struct RoleConnectionArgs {
    pub first: Option<i32>,
    pub last: Option<i32>,
    pub after: Option<String>,
    pub before: Option<String>,
}

/// Read the `first`/`last`/`after`/`before` arguments.
pub fn role_connection_args(arguments: &Arguments) -> RoleConnectionArgs {
    RoleConnectionArgs {
        first: arguments
            .i64("first")
            .and_then(|value| i32::try_from(value).ok()),
        last: arguments
            .i64("last")
            .and_then(|value| i32::try_from(value).ok()),
        after: arguments.string("after").map(str::to_owned),
        before: arguments.string("before").map(str::to_owned),
    }
}

/// Which roles a connection lists.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RoleFilter {
    pub system: bool,
    pub user_id: i32,
}

/// Which permissions a connection lists.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PermissionFilter {
    pub role_id: i32,
    pub user_id: i32,
}

/// `gqlutil.ConnectionPageInfo`: booleans use the pre-trim node count, the
/// cursors use the trimmed page.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RbacPageInfo {
    pub start_cursor: Option<String>,
    pub end_cursor: Option<String>,
    pub has_next_page: bool,
    pub has_previous_page: bool,
}

/// The `RoleConnection` payload.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RoleConnectionData {
    pub roles: Vec<RoleRow>,
    pub total_count: i32,
    pub page_info: RbacPageInfo,
}

/// The `PermissionConnection` payload.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PermissionConnectionData {
    pub permissions: Vec<PermissionRow>,
    pub total_count: i32,
    pub page_info: RbacPageInfo,
}

/// `ConnectionResolverArgs.Limit` + `ApplyMaxPageSize`.
fn page_size(args: &RoleConnectionArgs) -> i32 {
    let limit = args.first.or(args.last).unwrap_or(0);
    if limit == 0 {
        DEFAULT_MAX_PAGE_SIZE
    } else if limit < DEFAULT_MAX_PAGE_SIZE {
        limit
    } else {
        DEFAULT_MAX_PAGE_SIZE
    }
}

/// Build `database.PaginationArgs` from the resolver arguments, mirroring
/// `ConnectionResolver.paginationArgs` (`First`/`Last` + one extra probe).
fn pagination_args(
    args: &RoleConnectionArgs,
    order: OrderKind,
    ascending: bool,
) -> Result<PaginationArgs, String> {
    let limit = page_size(args).wrapping_add(1);
    let (first, last) = if args.first.is_some() {
        (Some(limit), None)
    } else if args.last.is_some() {
        (None, Some(limit))
    } else {
        // `AllowNoLimit` is true for every resolver in this module.
        (None, None)
    };
    let unmarshal = |raw: &str| match order {
        OrderKind::PermissionId => unmarshal_permission_id(raw),
        OrderKind::RoleSystemCreatedAt | OrderKind::RoleId => unmarshal_role_id(raw),
    };
    let after = args.after.as_deref().map(unmarshal).transpose()?;
    let before = args.before.as_deref().map(unmarshal).transpose()?;
    Ok(PaginationArgs {
        first,
        last,
        after,
        before,
        ascending,
        order,
    })
}

fn page_info(
    args: &RoleConnectionArgs,
    page_size: i32,
    fetched_count: usize,
    has_nodes: bool,
    start_cursor: Option<String>,
    end_cursor: Option<String>,
) -> RbacPageInfo {
    let fetched_count = fetched_count as i64;
    let page_size = i64::from(page_size);
    let has_next_page = if args.first.is_some() {
        fetched_count > page_size
    } else if fetched_count == 0 {
        false
    } else {
        args.before.is_some()
    };
    let has_previous_page = if args.last.is_some() {
        fetched_count > page_size
    } else if fetched_count == 0 {
        false
    } else {
        args.after.is_some()
    };
    RbacPageInfo {
        has_next_page,
        has_previous_page,
        start_cursor: if has_nodes { start_cursor } else { None },
        end_cursor: if has_nodes { end_cursor } else { None },
    }
}

/// `gqlutil.ConnectionResolver[Nodes/PageInfo/TotalCount]` for roles.
pub async fn compute_role_connection(
    store: &dyn RbacStore,
    args: &RoleConnectionArgs,
    filter: RoleFilter,
    order: OrderKind,
) -> Result<RoleConnectionData, String> {
    let size = page_size(args);
    let pagination = pagination_args(args, order, false)?;
    let opts = RolesListOptions {
        pagination: Some(pagination),
        system: filter.system,
        user_id: filter.user_id,
    };
    let mut roles = store.roles_list(&opts).await?;
    let fetched_count = roles.len();
    // The store reverses the SQL order for `Last`; restore it here.
    if args.last.is_some() {
        roles.reverse();
    }
    if roles.len() as i64 > i64::from(size) {
        if args.last.is_some() {
            if !roles.is_empty() {
                roles.remove(0);
            }
        } else {
            roles.pop();
        }
    }
    let total_count = i32::try_from(store.roles_count(&opts).await?).unwrap_or(i32::MAX);
    let start_cursor = roles.first().map(|role| marshal_role_id(role.id));
    let end_cursor = roles.last().map(|role| marshal_role_id(role.id));
    let page_info = page_info(
        args,
        size,
        fetched_count,
        !roles.is_empty(),
        start_cursor,
        end_cursor,
    );
    Ok(RoleConnectionData {
        roles,
        total_count,
        page_info,
    })
}

/// `gqlutil.ConnectionResolver[Nodes/PageInfo/TotalCount]` for permissions.
pub async fn compute_permission_connection(
    store: &dyn RbacStore,
    args: &RoleConnectionArgs,
    filter: PermissionFilter,
) -> Result<PermissionConnectionData, String> {
    let size = page_size(args);
    let pagination = pagination_args(args, OrderKind::PermissionId, false)?;
    let opts = PermissionListOptions {
        pagination: Some(pagination),
        role_id: filter.role_id,
        user_id: filter.user_id,
    };
    let mut permissions = store.permissions_list(&opts).await?;
    let fetched_count = permissions.len();
    if args.last.is_some() {
        permissions.reverse();
    }
    if permissions.len() as i64 > i64::from(size) {
        if args.last.is_some() {
            if !permissions.is_empty() {
                permissions.remove(0);
            }
        } else {
            permissions.pop();
        }
    }
    let total_count = i32::try_from(store.permissions_count(&opts).await?).unwrap_or(i32::MAX);
    let start_cursor = permissions
        .first()
        .map(|permission| marshal_permission_id(permission.id));
    let end_cursor = permissions
        .last()
        .map(|permission| marshal_permission_id(permission.id));
    let page_info = page_info(
        args,
        size,
        fetched_count,
        !permissions.is_empty(),
        start_cursor,
        end_cursor,
    );
    Ok(PermissionConnectionData {
        permissions,
        total_count,
        page_info,
    })
}

// ---------------------------------------------------------------------------
// Prefetch
// ---------------------------------------------------------------------------

/// Prefetched `Query.roles` / `Query.permissions` lookups, keyed by the
/// connection arguments.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct RbacRoots {
    pub roles: HashMap<RoleConnectionArgs, Result<RoleConnectionData, String>>,
    pub permissions: HashMap<RoleConnectionArgs, Result<PermissionConnectionData, String>>,
}

/// Prefetch `Query.roles`/`Query.permissions`. Both are site-admin gated.
pub async fn prefetch_rbac_roots(
    store: &dyn RbacStore,
    fields: &[RootField],
    actor: Actor,
    viewer_site_admin: bool,
) -> RbacRoots {
    let mut roots = RbacRoots::default();
    for field in fields {
        match field.name.as_str() {
            "roles" => {
                let args = role_connection_args(&field.arguments);
                if roots.roles.contains_key(&args) {
                    continue;
                }
                let lookup = match check_current_user_is_site_admin(actor, viewer_site_admin) {
                    Ok(()) => {
                        compute_role_connection(
                            store,
                            &args,
                            RoleFilter {
                                system: false,
                                user_id: 0,
                            },
                            OrderKind::RoleSystemCreatedAt,
                        )
                        .await
                    }
                    Err(error) => Err(error),
                };
                roots.roles.insert(args, lookup);
            }
            "permissions" => {
                let args = role_connection_args(&field.arguments);
                if roots.permissions.contains_key(&args) {
                    continue;
                }
                let lookup = match check_current_user_is_site_admin(actor, viewer_site_admin) {
                    Ok(()) => {
                        compute_permission_connection(
                            store,
                            &args,
                            PermissionFilter {
                                role_id: 0,
                                user_id: 0,
                            },
                        )
                        .await
                    }
                    Err(error) => Err(error),
                };
                roots.permissions.insert(args, lookup);
            }
            _ => {}
        }
    }
    roots
}

/// `User.roles`'s dotcom gate (`user.go:766-783`).
pub fn user_roles_gate(actor: Actor, viewer_site_admin: bool, dotcom: bool) -> Result<(), String> {
    if dotcom && check_current_user_is_site_admin(actor, viewer_site_admin).is_err() {
        return Err("unauthorized".to_owned());
    }
    Ok(())
}

/// `User.permissions`'s gate: `CheckSiteAdminOrSameUserFromActor` folds every
/// failure into `ErrMustBeSiteAdminOrSameUser`.
pub fn user_permissions_gate(
    actor: Actor,
    viewer_site_admin: bool,
    user_id: i32,
) -> Result<(), String> {
    check_site_admin_or_same_user(actor, viewer_site_admin, user_id)
        .map_err(|_| MUST_BE_SITE_ADMIN_OR_SAME_USER.to_owned())
}

/// Flatten the selection set of a parent field, including inline fragments on
/// `User`/`Role`/`Node`.
fn flatten_selections(field: &RootField) -> Vec<RootField> {
    let mut selections = field.selections.clone();
    for (condition, sub) in &field.type_conditions {
        if matches!(condition.as_str(), "User" | "Role" | "Node") {
            selections.extend(sub.clone());
        }
    }
    selections
}

/// Push the `roles`/`permissions` selections of one user object.
fn push_user_selection(
    by_user: &mut HashMap<i32, Vec<RootField>>,
    user_id: i32,
    selections: &[RootField],
) {
    let relevant: Vec<RootField> = selections
        .iter()
        .filter(|selection| selection.name == "roles" || selection.name == "permissions")
        .cloned()
        .collect();
    if !relevant.is_empty() {
        by_user.entry(user_id).or_default().extend(relevant);
    }
}

/// Prefetch `User.roles` / `User.permissions` and `Role.permissions`, writing
/// the results into the request's [`NestedPrefetch`].
#[allow(clippy::too_many_arguments)]
pub async fn prefetch_rbac_nested(
    store: &dyn RbacStore,
    fields: &[RootField],
    current_user: Option<&UserData>,
    users: &HashMap<UserKey, UserLookup>,
    users_connections: &HashMap<UsersArgs, UsersConnectionLookup>,
    nodes: &NodeRoots,
    actor: Actor,
    viewer_site_admin: bool,
    dotcom: bool,
    nested: &mut NestedPrefetch,
) {
    let mut selections_by_user: HashMap<i32, Vec<RootField>> = HashMap::new();
    for field in fields {
        match field.name.as_str() {
            "currentUser" => {
                if let Some(user) = current_user {
                    push_user_selection(&mut selections_by_user, user.id, &field.selections);
                }
            }
            "user" => {
                if let Some(key) = user_arg_key(&field.arguments) {
                    if let Some(UserLookup::Found(user)) = users.get(&key) {
                        push_user_selection(&mut selections_by_user, user.id, &field.selections);
                    }
                }
            }
            "users" => {
                let args = crate::graphql_resolvers::users_args(&field.arguments);
                let Some(UsersConnectionLookup::Found(data)) = users_connections.get(&args) else {
                    continue;
                };
                for nodes_selection in field
                    .selections
                    .iter()
                    .filter(|selection| selection.name == "nodes")
                {
                    for user in &data.users {
                        push_user_selection(
                            &mut selections_by_user,
                            user.id,
                            &nodes_selection.selections,
                        );
                    }
                }
            }
            "node" => {
                let Some(id) = field.arguments.string("id") else {
                    continue;
                };
                if relay_kind(id).as_deref() != Some("User") {
                    continue;
                }
                let Ok(node_id) = unmarshal_relay_id(id) else {
                    continue;
                };
                let Some(crate::graphql_resolvers::UserByIdLookup::Found(user)) =
                    nodes.users.get(&node_id)
                else {
                    continue;
                };
                push_user_selection(&mut selections_by_user, user.id, &flatten_selections(field));
            }
            _ => {}
        }
    }

    for (user_id, selections) in selections_by_user {
        for selection in selections {
            match selection.name.as_str() {
                "roles" => {
                    let args = role_connection_args(&selection.arguments);
                    if nested.user_roles.contains_key(&(user_id, args.clone())) {
                        continue;
                    }
                    let lookup = match user_roles_gate(actor, viewer_site_admin, dotcom) {
                        Ok(()) => {
                            compute_role_connection(
                                store,
                                &args,
                                RoleFilter {
                                    system: false,
                                    user_id,
                                },
                                OrderKind::RoleId,
                            )
                            .await
                        }
                        Err(error) => Err(error),
                    };
                    // `Role.permissions` inside the returned nodes.
                    if let Ok(data) = &lookup {
                        for nodes_selection in selection
                            .selections
                            .iter()
                            .filter(|selection| selection.name == "nodes")
                        {
                            if nodes_selection
                                .selections
                                .iter()
                                .any(|selection| selection.name == "permissions")
                            {
                                for role in &data.roles {
                                    prefetch_role_permissions(
                                        store,
                                        nested,
                                        role.id,
                                        &nodes_selection.selections,
                                        actor,
                                        viewer_site_admin,
                                    )
                                    .await;
                                }
                            }
                        }
                    }
                    nested.user_roles.insert((user_id, args), lookup);
                }
                "permissions" => {
                    if nested.user_permissions.contains_key(&user_id) {
                        continue;
                    }
                    let lookup = match user_permissions_gate(actor, viewer_site_admin, user_id) {
                        Ok(()) => {
                            compute_permission_connection(
                                store,
                                &RoleConnectionArgs::default(),
                                PermissionFilter {
                                    role_id: 0,
                                    user_id,
                                },
                            )
                            .await
                        }
                        Err(error) => Err(error),
                    };
                    nested.user_permissions.insert(user_id, lookup);
                }
                _ => {}
            }
        }
    }

    // `Role.permissions` reached through `Query.roles.nodes` and
    // `Query.node(Role)`.
    for field in fields {
        match field.name.as_str() {
            "roles" => {
                let Some(nodes_selection) = field
                    .selections
                    .iter()
                    .find(|selection| selection.name == "nodes")
                else {
                    continue;
                };
                if !nodes_selection
                    .selections
                    .iter()
                    .any(|selection| selection.name == "permissions")
                {
                    continue;
                }
                let args = role_connection_args(&field.arguments);
                let lookup = match check_current_user_is_site_admin(actor, viewer_site_admin) {
                    Ok(()) => {
                        compute_role_connection(
                            store,
                            &args,
                            RoleFilter {
                                system: false,
                                user_id: 0,
                            },
                            OrderKind::RoleSystemCreatedAt,
                        )
                        .await
                    }
                    Err(error) => Err(error),
                };
                if let Ok(data) = lookup {
                    for role in &data.roles {
                        prefetch_role_permissions(
                            store,
                            nested,
                            role.id,
                            &nodes_selection.selections,
                            actor,
                            viewer_site_admin,
                        )
                        .await;
                    }
                }
            }
            "node" => {
                let Some(id) = field.arguments.string("id") else {
                    continue;
                };
                if relay_kind(id).as_deref() != Some("Role") {
                    continue;
                }
                let Ok(role_id) = unmarshal_role_id(id) else {
                    continue;
                };
                let selections = flatten_selections(field);
                if !selections
                    .iter()
                    .any(|selection| selection.name == "permissions")
                {
                    continue;
                }
                prefetch_role_permissions(
                    store,
                    nested,
                    role_id,
                    &selections,
                    actor,
                    viewer_site_admin,
                )
                .await;
            }
            _ => {}
        }
    }
}

async fn prefetch_role_permissions(
    store: &dyn RbacStore,
    nested: &mut NestedPrefetch,
    role_id: i32,
    selections: &[RootField],
    actor: Actor,
    viewer_site_admin: bool,
) {
    for selection in selections
        .iter()
        .filter(|selection| selection.name == "permissions")
    {
        let args = role_connection_args(&selection.arguments);
        if nested
            .role_permissions
            .contains_key(&(role_id, args.clone()))
        {
            continue;
        }
        let lookup = match check_current_user_is_site_admin(actor, viewer_site_admin) {
            Ok(()) => {
                compute_permission_connection(
                    store,
                    &args,
                    PermissionFilter {
                        role_id,
                        user_id: 0,
                    },
                )
                .await
            }
            Err(error) => Err(error),
        };
        nested.role_permissions.insert((role_id, args), lookup);
    }
}

fn user_arg_key(arguments: &Arguments) -> Option<UserKey> {
    if let Some(username) = arguments.string("username") {
        return Some(UserKey::Username(username.to_owned()));
    }
    if let Some(id) = arguments
        .i64("databaseID")
        .and_then(|id| i32::try_from(id).ok())
    {
        return Some(UserKey::DatabaseId(id));
    }
    if let Some(email) = arguments.string("email") {
        return Some(UserKey::Email(email.to_owned()));
    }
    None
}

/// One prefetched `Query.node` result for the `Role`/`Permission` kinds.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RbacNodeLookup {
    Role(RoleRow),
    Permission(PermissionRow),
    Error(String),
}

/// Prefetch `Query.node(kind: Role | Permission)` keyed by the raw Relay id
/// (`roles.go:57-77`, `permissions.go:17-38`). Both check the site-admin gate
/// before decoding the id.
pub async fn prefetch_rbac_nodes(
    store: &dyn RbacStore,
    fields: &[RootField],
    actor: Actor,
    viewer_site_admin: bool,
) -> HashMap<String, RbacNodeLookup> {
    let mut nodes = HashMap::new();
    for field in fields {
        if field.name != "node" {
            continue;
        }
        let Some(id) = field.arguments.string("id") else {
            continue;
        };
        let Some(kind) = relay_kind(id) else {
            continue;
        };
        if nodes.contains_key(id) {
            continue;
        }
        let lookup = match kind.as_str() {
            "Role" => match check_current_user_is_site_admin(actor, viewer_site_admin) {
                Err(error) => RbacNodeLookup::Error(error),
                Ok(()) => match unmarshal_role_id(id) {
                    Err(error) => RbacNodeLookup::Error(error),
                    Ok(0) => RbacNodeLookup::Error("invalid node id".to_owned()),
                    Ok(role_id) => match store.role_get(role_id).await {
                        Err(error) => RbacNodeLookup::Error(error),
                        Ok(None) => {
                            RbacNodeLookup::Error(format!("role with ID {role_id} not found"))
                        }
                        Ok(Some(role)) => RbacNodeLookup::Role(role),
                    },
                },
            },
            "Permission" => match check_current_user_is_site_admin(actor, viewer_site_admin) {
                Err(error) => RbacNodeLookup::Error(error),
                Ok(()) => match unmarshal_permission_id(id) {
                    Err(error) => RbacNodeLookup::Error(error),
                    Ok(0) => RbacNodeLookup::Error("invalid node id".to_owned()),
                    Ok(permission_id) => match store.permission_get(permission_id).await {
                        Err(error) => RbacNodeLookup::Error(error),
                        Ok(None) => RbacNodeLookup::Error(format!(
                            "permission with ID {permission_id} not found"
                        )),
                        Ok(Some(permission)) => RbacNodeLookup::Permission(permission),
                    },
                },
            },
            _ => continue,
        };
        nodes.insert(id.to_owned(), lookup);
    }
    nodes
}

// ---------------------------------------------------------------------------
// GraphQL objects
// ---------------------------------------------------------------------------

/// `roleResolver` (`role.go:88-114`).
pub struct RoleObject {
    pub role: RoleRow,
    pub viewer_site_admin: bool,
    pub nested: Arc<NestedPrefetch>,
}

impl ObjectValue for RoleObject {
    fn type_name(&self) -> &'static str {
        "Role"
    }

    fn field(
        &self,
        field: &str,
        arguments: &Arguments,
        actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "id" => Ok(Some(FieldValue::Json(Json::Str(marshal_role_id(
                self.role.id,
            ))))),
            "name" => Ok(Some(FieldValue::Json(Json::Str(self.role.name.clone())))),
            "system" => Ok(Some(FieldValue::Json(Json::Bool(self.role.system)))),
            "createdAt" => Ok(Some(FieldValue::Json(Json::Str(datetime_rfc3339(
                self.role.created_at,
            ))))),
            "permissions" => {
                if let Err(error) = check_current_user_is_site_admin(actor, self.viewer_site_admin)
                {
                    return Err(ExecError::new(error));
                }
                let args = role_connection_args(arguments);
                match self
                    .nested
                    .role_permissions
                    .get(&(self.role.id, args))
                {
                    Some(Ok(data)) => Ok(Some(FieldValue::Object(Arc::new(
                        PermissionConnectionObject::from_data(
                            data.clone(),
                            self.viewer_site_admin,
                            self.nested.clone(),
                        ),
                    )))),
                    Some(Err(error)) => Err(ExecError::new(error.clone())),
                    None => Ok(None),
                }
            }
            _ => Ok(None),
        }
    }
}

/// `permissionResolver` (`permission.go:24-56`).
pub struct PermissionObject {
    pub permission: PermissionRow,
}

impl ObjectValue for PermissionObject {
    fn type_name(&self) -> &'static str {
        "Permission"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "id" => Ok(Some(FieldValue::Json(Json::Str(marshal_permission_id(
                self.permission.id,
            ))))),
            "namespace" => {
                if !permission_namespace_is_valid(&self.permission.namespace) {
                    return Err(ExecError::new("invalid namespace"));
                }
                Ok(Some(FieldValue::Json(Json::Str(
                    self.permission.namespace.clone(),
                ))))
            }
            "action" => Ok(Some(FieldValue::Json(Json::Str(
                self.permission.action.clone(),
            )))),
            "displayName" => Ok(Some(FieldValue::Json(Json::Str(format!(
                "{}#{}",
                self.permission.namespace, self.permission.action
            ))))),
            "createdAt" => Ok(Some(FieldValue::Json(Json::Str(datetime_rfc3339(
                self.permission.created_at,
            ))))),
            _ => Ok(None),
        }
    }
}

/// `RoleConnection` (`RoleConnectionResolver` + `gqlutil.ConnectionResolver`).
pub struct RoleConnectionObject {
    pub data: RoleConnectionData,
    pub viewer_site_admin: bool,
    pub nested: Arc<NestedPrefetch>,
}

impl ObjectValue for RoleConnectionObject {
    fn type_name(&self) -> &'static str {
        "RoleConnection"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "nodes" => Ok(Some(FieldValue::List {
                element_type: Some("Role"),
                items: self
                    .data
                    .roles
                    .iter()
                    .map(|role| {
                        FieldValue::Object(Arc::new(RoleObject {
                            role: role.clone(),
                            viewer_site_admin: self.viewer_site_admin,
                            nested: self.nested.clone(),
                        }))
                    })
                    .collect(),
            })),
            "totalCount" => Ok(Some(FieldValue::Json(Json::Int(i64::from(
                self.data.total_count,
            ))))),
            "pageInfo" => Ok(Some(FieldValue::Object(Arc::new(
                ConnectionPageInfoObject::new(
                    self.data.page_info.start_cursor.clone(),
                    self.data.page_info.end_cursor.clone(),
                    self.data.page_info.has_next_page,
                    self.data.page_info.has_previous_page,
                ),
            )))),
            _ => Ok(None),
        }
    }
}

/// `PermissionConnection`.
pub struct PermissionConnectionObject {
    pub data: PermissionConnectionData,
    /// Kept for symmetry with [`RoleConnectionObject`]; not read by fields.
    pub viewer_site_admin: bool,
    pub nested: Arc<NestedPrefetch>,
}

impl PermissionConnectionObject {
    pub fn from_data(
        data: PermissionConnectionData,
        viewer_site_admin: bool,
        nested: Arc<NestedPrefetch>,
    ) -> Self {
        Self {
            data,
            viewer_site_admin,
            nested,
        }
    }
}

impl ObjectValue for PermissionConnectionObject {
    fn type_name(&self) -> &'static str {
        "PermissionConnection"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "nodes" => Ok(Some(FieldValue::List {
                element_type: Some("Permission"),
                items: self
                    .data
                    .permissions
                    .iter()
                    .map(|permission| {
                        FieldValue::Object(Arc::new(PermissionObject {
                            permission: permission.clone(),
                        }))
                    })
                    .collect(),
            })),
            "totalCount" => Ok(Some(FieldValue::Json(Json::Int(i64::from(
                self.data.total_count,
            ))))),
            "pageInfo" => Ok(Some(FieldValue::Object(Arc::new(
                ConnectionPageInfoObject::new(
                    self.data.page_info.start_cursor.clone(),
                    self.data.page_info.end_cursor.clone(),
                    self.data.page_info.has_next_page,
                    self.data.page_info.has_previous_page,
                ),
            )))),
            _ => Ok(None),
        }
    }
}

/// Replay a prefetched `Query.roles` lookup.
pub fn root_role_connection(
    roots: &RbacRoots,
    args: &RoleConnectionArgs,
    viewer_site_admin: bool,
    nested: Arc<NestedPrefetch>,
) -> Result<Option<FieldValue>, ExecError> {
    match roots.roles.get(args) {
        Some(Ok(data)) => Ok(Some(FieldValue::Object(Arc::new(RoleConnectionObject {
            data: data.clone(),
            viewer_site_admin,
            nested,
        })))),
        Some(Err(error)) => Err(ExecError::new(error.clone())),
        None => Ok(None),
    }
}

/// Replay a prefetched `Query.permissions` lookup.
pub fn root_permission_connection(
    roots: &RbacRoots,
    args: &RoleConnectionArgs,
    viewer_site_admin: bool,
    nested: Arc<NestedPrefetch>,
) -> Result<Option<FieldValue>, ExecError> {
    match roots.permissions.get(args) {
        Some(Ok(data)) => Ok(Some(FieldValue::Object(Arc::new(
            PermissionConnectionObject::from_data(data.clone(), viewer_site_admin, nested),
        )))),
        Some(Err(error)) => Err(ExecError::new(error.clone())),
        None => Ok(None),
    }
}

/// Replay a prefetched `User.roles` lookup.
pub fn user_role_connection(
    nested: &Arc<NestedPrefetch>,
    user_id: i32,
    args: &RoleConnectionArgs,
    viewer_site_admin: bool,
) -> Result<Option<FieldValue>, ExecError> {
    match nested.user_roles.get(&(user_id, args.clone())) {
        Some(Ok(data)) => Ok(Some(FieldValue::Object(Arc::new(RoleConnectionObject {
            data: data.clone(),
            viewer_site_admin,
            nested: nested.clone(),
        })))),
        Some(Err(error)) => Err(ExecError::new(error.clone())),
        None => Ok(None),
    }
}

/// Replay a prefetched `User.permissions` lookup.
pub fn user_permission_connection(
    nested: &Arc<NestedPrefetch>,
    user_id: i32,
    viewer_site_admin: bool,
) -> Result<Option<FieldValue>, ExecError> {
    match nested.user_permissions.get(&user_id) {
        Some(Ok(data)) => Ok(Some(FieldValue::Object(Arc::new(
            PermissionConnectionObject::from_data(data.clone(), viewer_site_admin, nested.clone()),
        )))),
        Some(Err(error)) => Err(ExecError::new(error.clone())),
        None => Ok(None),
    }
}

/// Replay a prefetched `Query.node(Role|Permission)` lookup.
pub fn rbac_node_value(
    nodes: &HashMap<String, RbacNodeLookup>,
    id: &str,
    viewer_site_admin: bool,
    nested: Arc<NestedPrefetch>,
) -> Result<Option<FieldValue>, ExecError> {
    match nodes.get(id) {
        Some(RbacNodeLookup::Role(role)) => Ok(Some(FieldValue::Object(Arc::new(RoleObject {
            role: role.clone(),
            viewer_site_admin,
            nested,
        })))),
        Some(RbacNodeLookup::Permission(permission)) => {
            Ok(Some(FieldValue::Object(Arc::new(PermissionObject {
                permission: permission.clone(),
            }))))
        }
        Some(RbacNodeLookup::Error(error)) => Err(ExecError::new(error.clone())),
        None => Ok(None),
    }
}

// ---------------------------------------------------------------------------
// Production store
// ---------------------------------------------------------------------------

/// `PgPool`-backed [`RbacStore`] over the existing `roles`, `permissions`,
/// `role_permissions` and `user_roles` tables. No schema changes.
pub struct PgRbacStore {
    pool: sqlx::PgPool,
}

impl PgRbacStore {
    pub fn new(pool: sqlx::PgPool) -> Self {
        Self { pool }
    }
}

fn row_to_role(row: &sqlx::postgres::PgRow) -> Result<RoleRow, String> {
    use sqlx::Row;
    Ok(RoleRow {
        id: row.try_get("id").map_err(|e| e.to_string())?,
        name: row.try_get("name").map_err(|e| e.to_string())?,
        system: row.try_get("system").map_err(|e| e.to_string())?,
        created_at: row.try_get("created_at").map_err(|e| e.to_string())?,
    })
}

fn row_to_permission(row: &sqlx::postgres::PgRow) -> Result<PermissionRow, String> {
    use sqlx::Row;
    Ok(PermissionRow {
        id: row.try_get("id").map_err(|e| e.to_string())?,
        namespace: row.try_get("namespace").map_err(|e| e.to_string())?,
        action: row.try_get("action").map_err(|e| e.to_string())?,
        created_at: row.try_get("created_at").map_err(|e| e.to_string())?,
    })
}

fn roles_conditions(opts: &RolesListOptions) -> (Vec<String>, &'static str) {
    let mut conds = Vec::new();
    if opts.system {
        conds.push("roles.system IS TRUE".to_owned());
    }
    let mut join = "";
    if opts.user_id != 0 {
        join = "INNER JOIN user_roles ON user_roles.role_id = roles.id";
        conds.push(format!("user_roles.user_id = {}", opts.user_id));
    }
    (conds, join)
}

fn roles_sql(opts: &RolesListOptions) -> String {
    let (mut conds, join) = roles_conditions(opts);
    let mut order = match opts.pagination.map(|p| p.order) {
        Some(OrderKind::RoleId) => " ORDER BY roles.id DESC".to_owned(),
        _ => " ORDER BY roles.system DESC, roles.created_at DESC".to_owned(),
    };
    let mut limit = String::new();
    if let Some(p) = &opts.pagination {
        // The cursor stores a node id. Resolve it to the ordering tuple so the
        // comparison matches the composite `order by` (the baseline compares a
        // single value against both columns, which is an errata; see overview
        // §4).
        match p.order {
            OrderKind::RoleId => {
                if let Some(after) = p.after {
                    conds.push(format!("roles.id < {after}"));
                }
                if let Some(before) = p.before {
                    conds.push(format!("roles.id > {before}"));
                }
            }
            _ => {
                if let Some(after) = p.after {
                    conds.push(format!(
                        "(roles.system, roles.created_at) < (SELECT system, created_at FROM roles WHERE id = {after})"
                    ));
                }
                if let Some(before) = p.before {
                    conds.push(format!(
                        "(roles.system, roles.created_at) > (SELECT system, created_at FROM roles WHERE id = {before})"
                    ));
                }
            }
        }
        if p.last.is_some() {
            order = match p.order {
                OrderKind::RoleId => " ORDER BY roles.id ASC".to_owned(),
                _ => " ORDER BY roles.system ASC, roles.created_at ASC".to_owned(),
            };
        }
        if let Some(first) = p.first {
            limit = format!(" LIMIT {first}");
        } else if let Some(last) = p.last {
            limit = format!(" LIMIT {last}");
        }
    }
    if conds.is_empty() {
        conds.push("TRUE".to_owned());
    }
    format!(
        "SELECT roles.id, roles.name, roles.system, roles.created_at FROM roles {join} WHERE {}{order}{limit}",
        conds.join(" AND ")
    )
}

fn permission_conditions(opts: &PermissionListOptions) -> Vec<String> {
    let mut conds = Vec::new();
    if opts.role_id != 0 {
        conds.push(format!("role_permissions.role_id = {}", opts.role_id));
    }
    if opts.user_id != 0 {
        conds.push(format!("user_roles.user_id = {}", opts.user_id));
    }
    conds
}

fn permission_joins(opts: &PermissionListOptions) -> &'static str {
    if opts.user_id != 0 {
        "INNER JOIN role_permissions ON role_permissions.permission_id = permissions.id \
         INNER JOIN user_roles ON user_roles.role_id = role_permissions.role_id"
    } else if opts.role_id != 0 {
        "INNER JOIN role_permissions ON role_permissions.permission_id = permissions.id"
    } else {
        ""
    }
}

fn permissions_sql(opts: &PermissionListOptions) -> String {
    let mut conds = permission_conditions(opts);
    let mut order = " ORDER BY permissions.id DESC".to_owned();
    let mut limit = String::new();
    if let Some(p) = &opts.pagination {
        if let Some(after) = p.after {
            conds.push(format!("permissions.id < {after}"));
        }
        if let Some(before) = p.before {
            conds.push(format!("permissions.id > {before}"));
        }
        if p.last.is_some() {
            order = " ORDER BY permissions.id ASC".to_owned();
        }
        if let Some(first) = p.first {
            limit = format!(" LIMIT {first}");
        } else if let Some(last) = p.last {
            limit = format!(" LIMIT {last}");
        }
    }
    if conds.is_empty() {
        conds.push("TRUE".to_owned());
    }
    let group = if opts.user_id != 0 {
        " GROUP BY permissions.id"
    } else {
        ""
    };
    format!(
        "SELECT permissions.id, permissions.namespace, permissions.action, permissions.created_at FROM permissions {} WHERE {}{group}{order}{limit}",
        permission_joins(opts),
        conds.join(" AND ")
    )
}

fn role_count_sql(opts: &RolesListOptions) -> String {
    let (mut conds, join) = roles_conditions(opts);
    if conds.is_empty() {
        conds.push("TRUE".to_owned());
    }
    format!(
        "SELECT COUNT(1) FROM roles {join} WHERE {}",
        conds.join(" AND ")
    )
}

fn permission_count_sql(opts: &PermissionListOptions) -> String {
    let mut conds = permission_conditions(opts);
    if conds.is_empty() {
        conds.push("TRUE".to_owned());
    }
    format!(
        "SELECT COUNT(DISTINCT permissions.id) FROM permissions {} WHERE {}",
        permission_joins(opts),
        conds.join(" AND ")
    )
}

#[async_trait]
impl RbacStore for PgRbacStore {
    async fn roles_count(&self, opts: &RolesListOptions) -> Result<i64, String> {
        use sqlx::Row;
        let row = sqlx::query(&role_count_sql(opts))
            .fetch_one(&self.pool)
            .await
            .map_err(|e| e.to_string())?;
        row.try_get::<i64, _>(0).map_err(|e| e.to_string())
    }

    async fn roles_list(&self, opts: &RolesListOptions) -> Result<Vec<RoleRow>, String> {
        let rows = sqlx::query(&roles_sql(opts))
            .fetch_all(&self.pool)
            .await
            .map_err(|e| e.to_string())?;
        rows.iter().map(row_to_role).collect()
    }

    async fn role_get(&self, id: i32) -> Result<Option<RoleRow>, String> {
        let row = sqlx::query("SELECT id, name, system, created_at FROM roles WHERE id = $1 LIMIT 1")
            .bind(id)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| e.to_string())?;
        row.as_ref().map(row_to_role).transpose()
    }

    async fn permissions_count(&self, opts: &PermissionListOptions) -> Result<i64, String> {
        use sqlx::Row;
        let row = sqlx::query(&permission_count_sql(opts))
            .fetch_one(&self.pool)
            .await
            .map_err(|e| e.to_string())?;
        row.try_get::<i64, _>(0).map_err(|e| e.to_string())
    }

    async fn permissions_list(
        &self,
        opts: &PermissionListOptions,
    ) -> Result<Vec<PermissionRow>, String> {
        let rows = sqlx::query(&permissions_sql(opts))
            .fetch_all(&self.pool)
            .await
            .map_err(|e| e.to_string())?;
        rows.iter().map(row_to_permission).collect()
    }

    async fn permission_get(&self, id: i32) -> Result<Option<PermissionRow>, String> {
        let row = sqlx::query(
            "SELECT id, namespace, action, created_at FROM permissions WHERE id = $1 LIMIT 1",
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| e.to_string())?;
        row.as_ref().map(row_to_permission).transpose()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn role_relay_id_round_trips() {
        let raw = marshal_role_id(42);
        assert_eq!(unmarshal_role_id(&raw), Ok(42));
        assert_eq!(relay_kind(&raw).as_deref(), Some("Role"));
        let raw = marshal_permission_id(7);
        assert_eq!(unmarshal_permission_id(&raw), Ok(7));
    }

    #[test]
    fn page_size_matches_apply_max_page_size() {
        let args = |first, last| RoleConnectionArgs {
            first,
            last,
            after: None,
            before: None,
        };
        assert_eq!(page_size(&args(None, None)), 100);
        assert_eq!(page_size(&args(Some(0), None)), 100);
        assert_eq!(page_size(&args(Some(5), None)), 5);
        assert_eq!(page_size(&args(None, Some(500))), 100);
    }
}
