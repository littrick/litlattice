#![forbid(unsafe_code)]

//! Business GraphQL resolvers for the frontend schema.
//!
//! Only fields backed by the session/current-user slice are ported here. The
//! executor owns selection merging, nullability and JSON; these objects just
//! answer one field at a time. Unported fields return `Ok(None)` so the
//! executor reports `no ported resolver` instead of fabricating data.
//!
//! `Query.currentUser` is `graphqlbackend.CurrentUser`:
//! `db.Users().GetByCurrentAuthUser(ctx)`, which is `nil` for anonymous actors
//! (`user.go:535-544`). The session middleware supplies
//! [`Actor::User`](sg_graphql::exec::Actor::User) for authenticated requests;
//! [`SchemaGraphqlDeps`](crate::graphql_api::SchemaGraphqlDeps) prefetches the
//! user row and passes it to [`FrontendRootResolver::with_user`].
//!
//! `User.id` is the Relay ID: `relay.MarshalID("User", id)` =
//! `base64.URLEncoding(kind + ":" + json.Marshal(id))` (`relay/relay.go:14-19`).

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use async_trait::async_trait;
use base64::Engine;
use sg_db::feature_flags::{FeatureFlag, FeatureFlagKind, FeatureFlagOverride};
use sg_encryption::Key;
use sg_graphql::exec::{
    Actor, Arguments, ExecError, FieldValue, InputValue, ObjectValue, RootField, RootResolver,
};
use sg_graphql::json::Json;
use time::{OffsetDateTime, UtcOffset};

use crate::search_contexts::RequestContext;
use crate::search_contexts_graphql::{
    NamespaceResolver, PageInfo, RepositoryResolver, SearchContextEditInputArgs,
    SearchContextInputArgs, SearchContextRepositoryRevisionsInputArgs,
    SearchContextRepositoryRevisionsResolver, SearchContextResolver, SearchContextsGraphqlDatabase,
    SearchContextsOrderBy,
};
use crate::search_contexts_schema::{
    SearchContextsFieldArgs, SearchContextsFieldValue, SearchContextsOperation,
    SearchContextsResolverDispatch,
};
use crate::search_stream::convert::MinimalRepo;
use crate::settings_merge::{final_settings, DEFAULT_SETTINGS_CONTENTS};

/// A search contexts GraphQL database handle. The extra auto-trait bounds keep
/// the handle usable from [`ObjectValue`] (whose implementors are `Send +
/// Sync`) without changing the M9.8hq trait itself.
pub type SearchContextsDatabaseHandle = Arc<dyn SearchContextsGraphqlDatabase + Send + Sync>;

/// The ranking root resolver injected by the frontend (M9.8ic). `None` in
/// [`SchemaGraphqlDeps`](crate::graphql_api::SchemaGraphqlDeps) reproduces an
/// absent `CodeIntelResolver`/ranking surface (SDL not appended).
pub type RankingResolverHandle = Arc<crate::ranking_graphql::RootResolver>;

/// The ranking root fields prefetched by
/// [`crate::graphql_api::SchemaGraphqlDeps::exec`] (async) and replayed by
/// [`QueryRoot`]/[`MutationRoot`] (sync).
#[derive(Debug, Clone, Default)]
pub struct RankingRoots {
    /// `Query.rankingSummary`; `None` when the field was not selected (or the
    /// resolver is not injected).
    pub summary: Option<Result<crate::ranking_graphql::GlobalRankingSummary, String>>,
    /// Mutation outcomes keyed by root field name. `Ok(())` renders the
    /// baseline non-nil `EmptyResponse` object.
    pub mutations: HashMap<String, Result<(), String>>,
}

/// Run the selected ranking root operations before GraphQL execution, like the
/// other async prefetch passes (settings/log events/feature flags). Only the
/// selected root fields run; the `RootResolver` keeps the baseline's
/// site-admin-before-service order and error strings.
pub async fn prefetch_ranking_roots(
    resolver: &crate::ranking_graphql::RootResolver,
    fields: &[RootField],
    is_mutation: bool,
) -> RankingRoots {
    let mut roots = RankingRoots::default();
    for field in fields {
        if !is_mutation && field.name == "rankingSummary" {
            roots.summary = Some(
                resolver
                    .ranking_summary()
                    .await
                    .map_err(|error| error.to_string()),
            );
        } else if is_mutation && field.name == "bumpDerivativeGraphKey" {
            roots.mutations.insert(
                field.name.clone(),
                resolver
                    .bump_derivative_graph_key()
                    .await
                    .map(|_| ())
                    .map_err(|error| error.to_string()),
            );
        } else if is_mutation && field.name == "deleteRankingProgress" {
            let args = crate::ranking_graphql::DeleteRankingProgressArgs {
                graph_key: field
                    .arguments
                    .string("graphKey")
                    .unwrap_or_default()
                    .to_owned(),
            };
            roots.mutations.insert(
                field.name.clone(),
                resolver
                    .delete_ranking_progress(&args)
                    .await
                    .map(|_| ())
                    .map_err(|error| error.to_string()),
            );
        }
    }
    roots
}

/// The `users` row fields the DB-backed `User` resolvers read, plus the
/// primary `user_emails.email` (`userEmailsStore.GetPrimaryEmail`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UserData {
    pub id: i32,
    pub username: String,
    pub display_name: String,
    pub avatar_url: String,
    pub created_at: OffsetDateTime,
    pub updated_at: OffsetDateTime,
    pub site_admin: bool,
    pub tos_accepted: bool,
    pub primary_email: String,
}

/// `gqlutil.DateTime.MarshalJSON`: `json.Marshal(t.UTC().Format(time.RFC3339))`
/// — UTC, seconds precision, no fractional part.
pub(crate) fn datetime_rfc3339(t: OffsetDateTime) -> String {
    let t = t.to_offset(UtcOffset::UTC);
    format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}Z",
        t.year(),
        t.month() as u8,
        t.day(),
        t.hour(),
        t.minute(),
        t.second()
    )
}

/// The database seam for `Query.currentUser` / `User` (`db.Users()`).
#[async_trait]
pub trait UserProfiles: Send + Sync {
    async fn profile(&self, uid: i32) -> Result<Option<UserData>, String>;
}

/// `Query.user`'s lookup key. The baseline checks the arguments in this
/// order: `username`, then `databaseID`, then `email` (`user.go:38-56`).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum UserKey {
    Username(String),
    DatabaseId(i32),
    Email(String),
}

/// The result of prefetching one `Query.user` lookup.
#[derive(Debug, Clone, PartialEq)]
pub enum UserLookup {
    Found(UserData),
    /// `errcode.IsNotFound` → GraphQL `null`.
    NotFound,
    /// A database failure, surfaced as the resolver error.
    Error(String),
}

/// The database seam for `Query.user` (`db.Users().GetByUsername` /
/// `GetByID` / `GetByVerifiedEmail`).
#[async_trait]
pub trait UserLookupStore: Send + Sync {
    async fn lookup(&self, key: &UserKey) -> Result<Option<UserData>, String>;
}

/// What `Query.user` arguments ask for.
enum UserArg {
    Key(UserKey),
    /// No argument: the baseline errors even though `databaseID` is
    /// supported (`user.go:56-57`).
    Missing,
}

fn user_arg(arguments: &Arguments) -> UserArg {
    if let Some(username) = arguments.string("username") {
        return UserArg::Key(UserKey::Username(username.to_owned()));
    }
    if let Some(id) = arguments
        .i64("databaseID")
        .and_then(|id| i32::try_from(id).ok())
    {
        return UserArg::Key(UserKey::DatabaseId(id));
    }
    if let Some(email) = arguments.string("email") {
        return UserArg::Key(UserKey::Email(email.to_owned()));
    }
    UserArg::Missing
}

/// Look up every `Query.user` root field in `fields`, deduplicated by key.
pub async fn prefetch_users(
    store: &dyn UserLookupStore,
    fields: &[RootField],
) -> HashMap<UserKey, UserLookup> {
    let mut users = HashMap::new();
    for field in fields {
        if field.name != "user" {
            continue;
        }
        let UserArg::Key(key) = user_arg(&field.arguments) else {
            continue;
        };
        if users.contains_key(&key) {
            continue;
        }
        let lookup = match store.lookup(&key).await {
            Ok(Some(data)) => UserLookup::Found(data),
            Ok(None) => UserLookup::NotFound,
            Err(error) => UserLookup::Error(error),
        };
        users.insert(key, lookup);
    }
    users
}

/// `Query.users` arguments (`usersArgs`, `users.go:17-23`). `activePeriod`
/// stays a raw string because the baseline stores it as `*string`; unknown
/// values error at compute time.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct UsersArgs {
    pub first: Option<i32>,
    pub after: Option<String>,
    pub query: Option<String>,
    pub active_period: Option<String>,
    /// `gqlutil.DateTime` input; parsed by the store so invalid scalar input
    /// surfaces as a database-seam error.
    pub inactive_since: Option<String>,
}

/// `userConnectionResolver.compute`'s result: the node rows and the count for
/// the same options.
#[derive(Debug, Clone, PartialEq)]
pub struct UsersConnectionData {
    pub users: Vec<UserData>,
    pub total_count: i32,
    /// `UsersListOptions.LimitOffset.Limit` (`None` when unpaginated).
    pub limit: Option<i32>,
    /// `UsersListOptions.LimitOffset.Offset` (the parsed `after` cursor).
    pub offset: i32,
}

/// The result of prefetching one `Query.users` root field.
#[derive(Debug, Clone, PartialEq)]
pub enum UsersConnectionLookup {
    Found(UsersConnectionData),
    /// A database/argument failure, surfaced as the resolver error.
    Error(String),
}

/// The database seam for `Query.users` (`db.Users().List`/`Count`).
#[async_trait]
pub trait UsersConnectionStore: Send + Sync {
    async fn list(&self, args: &UsersArgs) -> Result<UsersConnectionData, String>;
}

/// `usersArgs` from the GraphQL arguments. `first` is an `Int` (int32 in
/// graph-gophers); only reachable values are kept.
pub fn users_args(arguments: &Arguments) -> UsersArgs {
    UsersArgs {
        first: arguments
            .i64("first")
            .and_then(|first| i32::try_from(first).ok()),
        after: arguments.string("after").map(str::to_owned),
        query: arguments.string("query").map(str::to_owned),
        active_period: arguments.string("activePeriod").map(str::to_owned),
        inactive_since: arguments.string("inactiveSince").map(str::to_owned),
    }
}

/// `database.maybeQueryIsID` (`internal/database/helpers.go:32-44`): a plain
/// int32 or a graph-gophers Relay ID
/// (`base64.URLEncoding(kind + ":" + json)`, kind not validated).
pub fn maybe_query_is_id(query: &str) -> Option<i32> {
    if let Ok(id) = query.parse::<i32>() {
        return Some(id);
    }
    let decoded = base64::engine::general_purpose::URL_SAFE
        .decode(query)
        .ok()?;
    let text = std::str::from_utf8(&decoded).ok()?;
    let (_, spec) = text.split_once(':')?;
    serde_json::from_str(spec).ok()
}

/// Look up every `Query.users` root field in `fields`, deduplicated by
/// argument set.
pub async fn prefetch_users_connections(
    store: &dyn UsersConnectionStore,
    fields: &[RootField],
) -> HashMap<UsersArgs, UsersConnectionLookup> {
    let mut connections = HashMap::new();
    for field in fields {
        if field.name != "users" {
            continue;
        }
        let args = users_args(&field.arguments);
        if connections.contains_key(&args) {
            continue;
        }
        let lookup = match store.list(&args).await {
            Ok(data) => UsersConnectionLookup::Found(data),
            Err(error) => UsersConnectionLookup::Error(error),
        };
        connections.insert(args, lookup);
    }
    connections
}

/// The `orgs` row fields the `Organization` resolvers read plus the viewer's
/// authorization flags (`types.Org` + `auth.CheckOrgAccessOrSiteAdmin`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrgData {
    pub id: i32,
    pub name: String,
    /// `NULL` stays `None`; `Org.displayName` is nullable.
    pub display_name: Option<String>,
    pub created_at: OffsetDateTime,
    /// `Org.viewerCanAdminister`: a site admin or an organization member.
    pub viewer_can_administer: bool,
    /// `Org.viewerIsMember`: the actor is an organization member.
    pub viewer_is_member: bool,
}

/// One `names` row (`internal/database/namespaces.go:17-25`): exactly one of
/// `user_id`/`org_id` is non-zero for namespaces the GraphQL schema exposes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NamespaceName {
    pub name: String,
    pub user_id: i32,
    pub org_id: i32,
}

/// The result of prefetching one `Query.organization` root field.
#[derive(Debug, Clone, PartialEq)]
pub enum OrganizationLookup {
    Found(OrgData),
    /// `OrgNotFoundError` (`org not found: name <name>`), surfaced as the
    /// resolver error; the field itself is nullable.
    Error(String),
}

/// The database seam for `Query.organization` (`db.Orgs().GetByName` plus the
/// viewer's membership row).
#[async_trait]
pub trait OrganizationStore: Send + Sync {
    async fn organization(
        &self,
        name: &str,
        viewer_uid: i32,
        viewer_site_admin: bool,
    ) -> Result<Option<OrgData>, String>;
}

/// `Query.organizations` arguments (`orgs.go:15-25`): `first` and `query`.
/// `OrgConnection` has no `after`/`pageInfo`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct OrganizationsArgs {
    pub first: Option<i32>,
    pub query: Option<String>,
}

/// `orgConnectionResolver`'s nodes plus the count for the same options.
#[derive(Debug, Clone, PartialEq)]
pub struct OrganizationsConnectionData {
    pub orgs: Vec<OrgData>,
    pub total_count: i32,
}

/// The result of prefetching one `Query.organizations` root field.
#[derive(Debug, Clone, PartialEq)]
pub enum OrganizationsConnectionLookup {
    Found(OrganizationsConnectionData),
    Error(String),
}

/// The database seam for `Query.organizations` (`db.Orgs().List`/`Count`).
#[async_trait]
pub trait OrganizationsConnectionStore: Send + Sync {
    async fn organizations(
        &self,
        args: &OrganizationsArgs,
        viewer_uid: i32,
        viewer_site_admin: bool,
    ) -> Result<OrganizationsConnectionData, String>;
}

/// The result of prefetching `User.organizations` for one user id
/// (`UserResolver.Organizations`, `user.go:546-562`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UserOrganizationsLookup {
    Found(Vec<OrgData>),
    /// A database failure, surfaced as the resolver error.
    Error(String),
}

/// `Org.members` arguments (`org.go:100-121`): gqlutil's bidirectional
/// connection args plus the `query` user filter.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct MembersArgs {
    pub first: Option<i32>,
    pub last: Option<i32>,
    pub after: Option<String>,
    pub before: Option<String>,
    pub query: Option<String>,
}

/// The page gqlutil's `ConnectionResolver` returns for one `Org.members`
/// argument set (`internal/gqlutil/connection_resolver.go:100-330`),
/// already reversed/trimmed as `Nodes()` would return it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MembersConnectionData {
    /// `Nodes()` after the `last` reversal and the over-fetch trim.
    pub users: Vec<UserData>,
    pub total_count: i32,
    /// `pageSize()` after `ApplyMaxPageSize`.
    pub page_size: i32,
    /// `len(r.data.nodes)`: the count *before* the over-fetch trim.
    pub fetched_count: i32,
    pub first_present: bool,
    pub last_present: bool,
    pub after_present: bool,
    pub before_present: bool,
}

/// The result of prefetching one `Org.members` argument set.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MembersConnectionLookup {
    Found(MembersConnectionData),
    /// A database/cursor failure, surfaced as the resolver error.
    Error(String),
}

/// The database seam for `Org.members` (`db.Users().ListByOrg` / `Count`).
#[async_trait]
pub trait OrgMembersStore: Send + Sync {
    async fn members(
        &self,
        org_id: i32,
        args: &MembersArgs,
    ) -> Result<MembersConnectionData, String>;
}

/// One `org_members` row (`internal/types/types.go:955-961`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MembershipData {
    pub id: i32,
    pub org_id: i32,
    pub user_id: i32,
    pub created_at: OffsetDateTime,
    pub updated_at: OffsetDateTime,
}

/// The result of prefetching one user's `org_members` rows
/// (`db.OrgMembers().GetByUserID`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UserMembershipsLookup {
    Found(Vec<MembershipData>),
    /// A database failure, surfaced as the resolver error.
    Error(String),
}

/// A prefetched `OrgByIDInt32` result for `OrganizationMembership.organization`
/// (a missing live org is the baseline's `org not found: id <n>` error).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OrgByIdLookup {
    Found(OrgData),
    Error(String),
}

/// A prefetched `UserByIDInt32` result for `OrganizationMembership.user`
/// (a missing live user is `user not found: [<n>]`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UserByIdLookup {
    Found(UserData),
    Error(String),
}

/// The database seam for `User.organizationMemberships`
/// (`db.OrgMembers().GetByUserID`) plus the membership nodes' own lookups
/// (`OrgByIDInt32` / `UserByIDInt32`).
#[async_trait]
pub trait OrgMembershipsStore: Send + Sync {
    async fn memberships_for_user(&self, user_id: i32) -> Result<Vec<MembershipData>, String>;

    /// `db.Namespaces().GetByName` (`internal/database/namespaces.go:92-104`):
    /// a missing name is `Ok(None)`. `names.name` is `citext`.
    async fn namespace_by_name(&self, name: &str) -> Result<Option<NamespaceName>, String>;

    /// `OrgByIDInt32`: the viewer-relative `viewerCanAdminister`/
    /// `viewerIsMember` flags are computed for the membership's `Org` node.
    async fn org_by_id(
        &self,
        org_id: i32,
        viewer_uid: i32,
        viewer_site_admin: bool,
    ) -> Result<OrgData, String>;

    /// `UserByIDInt32` with the primary email (`UserSessionRecord`).
    async fn user_by_id(&self, user_id: i32) -> Result<UserData, String>;

    /// `settingsStore.GetLatest` for `User.latestSettings`
    /// (`internal/database/settings.go:88-120`).
    async fn latest_settings_for_user(&self, user_id: i32) -> Result<Option<SettingsData>, String>;

    /// `settingsStore.GetLatest` for `Org.latestSettings` (`org_id = <id>`).
    async fn latest_settings_for_org(&self, org_id: i32) -> Result<Option<SettingsData>, String>;

    /// `settingsStore.GetLatest` for `Site.latestSettings` (global settings:
    /// `user_id IS NULL AND org_id IS NULL`).
    async fn latest_settings_for_site(&self) -> Result<Option<SettingsData>, String>;
}

/// Prefetched nodes for `Query.node` (`schemaResolver.Node`,
/// `node.go:21-39`) and the `node`/`org` root fields' nested objects.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct NodeRoots {
    /// `UserByID` results keyed by users.id.
    pub users: HashMap<i32, UserByIdLookup>,
    /// `OrgByID` results keyed by orgs.id.
    pub orgs: HashMap<i32, OrgByIdLookup>,
    /// `repositoryByID` results keyed by repo.id.
    pub repositories: HashMap<i32, RepoLookup>,
    /// `externalServiceByID` results keyed by the raw Relay ID (the gate and
    /// ID validation errors are preserved per ID).
    pub external_services:
        HashMap<String, Result<crate::external_services::ExternalServiceNodeData, String>>,
    /// `webhookLogByID` results keyed by the raw Relay ID.
    pub webhook_logs: HashMap<String, Result<crate::webhook_logs::WebhookLogNodeData, String>>,
    /// `externalAccountByID` results keyed by the raw id (M9.8ka).
    pub external_accounts:
        HashMap<i32, crate::external_accounts_graphql::ExternalAccountNodeLookup>,
    /// `roleByID` / `permissionByID` results keyed by the raw Relay id
    /// (M9.13b).
    pub rbac_nodes: HashMap<String, crate::rbac_graphql::RbacNodeLookup>,
    /// `executorSecretByID` results keyed by the raw Relay id (M9.13b).
    pub executor_secrets:
        HashMap<String, crate::executor_secrets_graphql::ExecutorSecretNodeLookup>,
    /// `executorSecretAccessLogByID` results keyed by the raw Relay id
    /// (M9.13b).
    pub executor_secret_access_logs:
        HashMap<String, crate::executor_secrets_graphql::ExecutorSecretAccessLogNodeLookup>,
}

/// `relay.UnmarshalKind`: URL-safe base64 decode and take the prefix before
/// the first `:`; `None` when decoding fails or there is no `:` (the baseline
/// then looks up the empty kind and reports `invalid id`).
pub fn relay_kind(id: &str) -> Option<String> {
    let decoded = base64::engine::general_purpose::URL_SAFE.decode(id).ok()?;
    let text = String::from_utf8_lossy(&decoded);
    let (kind, _) = text.split_once(':')?;
    Some(kind.to_owned())
}

/// `relay.UnmarshalSpec` for an int32 ID: decode, skip the kind prefix (the
/// kind is **not** validated here) and parse the remaining JSON as an int32.
pub fn unmarshal_relay_id(id: &str) -> Result<i32, String> {
    let decoded = base64::engine::general_purpose::URL_SAFE
        .decode(id)
        .map_err(|error| error.to_string())?;
    let text = String::from_utf8_lossy(&decoded);
    let Some((_, spec)) = text.split_once(':') else {
        return Err("invalid graphql.ID".to_owned());
    };
    serde_json::from_str(spec).map_err(|error| error.to_string())
}

/// `relay.UnmarshalSpec` for a string ID (used by `Site` IDs).
pub fn unmarshal_relay_spec_string(id: &str) -> Result<String, String> {
    let decoded = base64::engine::general_purpose::URL_SAFE
        .decode(id)
        .map_err(|error| error.to_string())?;
    let text = String::from_utf8_lossy(&decoded);
    let Some((_, spec)) = text.split_once(':') else {
        return Err("invalid graphql.ID".to_owned());
    };
    serde_json::from_str(spec).map_err(|error| error.to_string())
}

/// Look up every `Query.node` root field in `fields`, dispatching on the Relay
/// kind. Ported kinds are `User` (`UserByID`), `Org` (`OrgByID`),
/// `Repository` (`repositoryByID`), `ExternalService` (`externalServiceByID`)
/// and `WebhookLog` (`webhookLogByID`); other kinds are left to the resolver,
/// which reports `invalid id` (errata: the baseline dispatches to resolvers
/// that are not ported yet).
#[allow(clippy::too_many_arguments)]
pub async fn prefetch_node_roots(
    memberships_store: &dyn OrgMembershipsStore,
    repositories: &dyn RepositoryLookup,
    external_services: Option<&dyn crate::external_services::ExternalServiceConnectionStore>,
    webhook_logs: Option<&dyn crate::webhook_logs::WebhookLogConnectionStore>,
    users: Option<&dyn UserLookupStore>,
    external_service_key: Option<Arc<dyn Key>>,
    webhook_key: Option<Arc<dyn Key>>,
    external_url: &str,
    fields: &[RootField],
    actor: Actor,
    viewer_site_admin: bool,
) -> NodeRoots {
    let mut roots = NodeRoots::default();
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
        match kind.as_str() {
            "ExternalService" => {
                let Some(store) = external_services else {
                    continue;
                };
                if roots.external_services.contains_key(id) {
                    continue;
                }
                let lookup = crate::external_services::external_service_node_by_id(
                    store,
                    users,
                    external_service_key.clone(),
                    external_url,
                    id,
                    actor,
                    viewer_site_admin,
                )
                .await;
                roots.external_services.insert(id.to_owned(), lookup);
            }
            "WebhookLog" => {
                let (Some(logs_store), Some(services_store)) = (webhook_logs, external_services)
                else {
                    continue;
                };
                if roots.webhook_logs.contains_key(id) {
                    continue;
                }
                let lookup = crate::webhook_logs::webhook_log_node_by_id(
                    logs_store,
                    services_store,
                    users,
                    webhook_key.clone(),
                    external_service_key.clone(),
                    external_url,
                    id,
                    actor,
                    viewer_site_admin,
                )
                .await;

                roots.webhook_logs.insert(id.to_owned(), lookup);
            }
            // `gitCommitByID`/`gitRefByID` both call `repositoryByID`
            // (`db.Repos().Get`) before resolving the git data; prefetch the
            // repository row here so `QueryRoot::field` only replays it.
            "GitCommit" | "GitRef" => {
                let repo_relay = if kind == "GitCommit" {
                    crate::git_graphql::parse_git_commit_relay_id(id).map(|spec| spec.repository)
                } else {
                    crate::git_graphql::parse_git_ref_relay_id(id).map(|spec| spec.repository)
                };
                let Ok(repo_relay) = repo_relay else {
                    continue;
                };
                let Ok(repo_id) = unmarshal_relay_id(&repo_relay) else {
                    continue;
                };
                if roots.repositories.contains_key(&repo_id) {
                    continue;
                }
                let lookup = match repositories
                    .by_id(repo_id, viewer_uid(actor), viewer_site_admin)
                    .await
                {
                    Ok(lookup) => lookup,
                    Err(error) => RepoLookup::Error(error),
                };
                roots.repositories.insert(repo_id, lookup);
            }
            _ => {
                let Ok(node_id) = unmarshal_relay_id(id) else {
                    continue;
                };
                match kind.as_str() {
                    "User" => {
                        if roots.users.contains_key(&node_id) {
                            continue;
                        }
                        let lookup = match memberships_store.user_by_id(node_id).await {
                            Ok(user) => UserByIdLookup::Found(user),
                            Err(error) => UserByIdLookup::Error(error),
                        };
                        roots.users.insert(node_id, lookup);
                    }
                    // `OrgByID` does not validate the spec's kind
                    // (`UnmarshalOrgID` is only used by the deprecated
                    // `Query.org`, which the schema does not expose).
                    "Org" => {
                        if roots.orgs.contains_key(&node_id) {
                            continue;
                        }
                        let lookup = match memberships_store
                            .org_by_id(node_id, viewer_uid(actor), viewer_site_admin)
                            .await
                        {
                            Ok(org) => OrgByIdLookup::Found(org),
                            Err(error) => OrgByIdLookup::Error(error),
                        };
                        roots.orgs.insert(node_id, lookup);
                    }
                    "Repository" => {
                        if roots.repositories.contains_key(&node_id) {
                            continue;
                        }
                        let lookup = match repositories
                            .by_id(node_id, viewer_uid(actor), viewer_site_admin)
                            .await
                        {
                            Ok(lookup) => lookup,
                            Err(error) => RepoLookup::Error(error),
                        };
                        roots.repositories.insert(node_id, lookup);
                    }
                    _ => {}
                }
            }
        }
    }
    roots
}

/// Prefetched `Query.namespace` / `Query.namespaceByName` results
/// (`namespaces.go:24-133`). Namespaces are `User` or `Org` objects.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct NamespaceRoots {
    /// `namespace(id)` results keyed by the Relay ID string.
    pub by_id: HashMap<String, NamespaceLookup>,
    /// `namespaceByName(name)` results keyed by the argument string.
    pub by_name: HashMap<String, NamespaceLookup>,
}

/// One prefetched namespace. `NotFound` is only reachable through
/// `namespaceByName` (the baseline returns `nil`); `UserByID`/`OrgByID` errors
/// are kept verbatim.
#[derive(Debug, Clone, PartialEq)]
pub enum NamespaceLookup {
    User(UserData),
    Org(OrgData),
    NotFound,
    Error(String),
}

/// Look up every `Query.namespace` / `Query.namespaceByName` root field in
/// `fields`.
///
/// `namespace(id)` dispatches on the Relay kind; anything but `User`/`Org` is
/// `InvalidNamespaceIDErr` (`invalid ID "<id>" for namespace`). A missing
/// user/org is the underlying store error, not `null`. `namespaceByName`
/// consults the `names` table first: a missing name is `null`, while a row
/// with neither a user nor an org ID is the baseline's panic (reported as an
/// error here; see the overview's errata).
pub async fn prefetch_namespace_roots(
    store: &dyn OrgMembershipsStore,
    fields: &[RootField],
    actor: Actor,
    viewer_site_admin: bool,
) -> NamespaceRoots {
    let mut roots = NamespaceRoots::default();
    for field in fields {
        match field.name.as_str() {
            "namespace" => {
                let Some(id) = field.arguments.string("id") else {
                    continue;
                };
                if roots.by_id.contains_key(id) {
                    continue;
                }
                let lookup = namespace_by_id(store, id, actor, viewer_site_admin).await;
                roots.by_id.insert(id.to_owned(), lookup);
            }
            "namespaceByName" => {
                let Some(name) = field.arguments.string("name") else {
                    continue;
                };
                if roots.by_name.contains_key(name) {
                    continue;
                }
                let lookup = namespace_by_name(store, name, actor, viewer_site_admin).await;
                roots.by_name.insert(name.to_owned(), lookup);
            }
            _ => {}
        }
    }
    roots
}

async fn namespace_by_id(
    store: &dyn OrgMembershipsStore,
    id: &str,
    actor: Actor,
    viewer_site_admin: bool,
) -> NamespaceLookup {
    // `relay.UnmarshalKind` ignores decode errors and yields ""; both that
    // and an unknown kind fall into `NamespaceByID`'s default branch.
    let Some(kind) = relay_kind(id) else {
        return NamespaceLookup::Error(format!("invalid ID {id:?} for namespace"));
    };
    match kind.as_str() {
        "User" => match unmarshal_relay_id(id) {
            Err(error) => NamespaceLookup::Error(error),
            Ok(user_id) => match store.user_by_id(user_id).await {
                Ok(user) => NamespaceLookup::User(user),
                Err(error) => NamespaceLookup::Error(error),
            },
        },
        "Org" => match unmarshal_relay_id(id) {
            Err(error) => NamespaceLookup::Error(error),
            Ok(org_id) => match store
                .org_by_id(org_id, viewer_uid(actor), viewer_site_admin)
                .await
            {
                Ok(org) => NamespaceLookup::Org(org),
                Err(error) => NamespaceLookup::Error(error),
            },
        },
        _ => NamespaceLookup::Error(format!("invalid ID {id:?} for namespace")),
    }
}

async fn namespace_by_name(
    store: &dyn OrgMembershipsStore,
    name: &str,
    actor: Actor,
    viewer_site_admin: bool,
) -> NamespaceLookup {
    let namespace = match store.namespace_by_name(name).await {
        Ok(namespace) => namespace,
        Err(error) => return NamespaceLookup::Error(error),
    };
    let Some(namespace) = namespace else {
        return NamespaceLookup::NotFound;
    };
    if namespace.user_id != 0 {
        return match store.user_by_id(namespace.user_id).await {
            Ok(user) => NamespaceLookup::User(user),
            Err(error) => NamespaceLookup::Error(error),
        };
    }
    if namespace.org_id != 0 {
        return match store
            .org_by_id(namespace.org_id, viewer_uid(actor), viewer_site_admin)
            .await
        {
            Ok(org) => NamespaceLookup::Org(org),
            Err(error) => NamespaceLookup::Error(error),
        };
    }
    // The baseline panics here (`invalid namespace (neither user nor
    // organization)`); a row with neither ID belongs to a team namespace.
    NamespaceLookup::Error("invalid namespace (neither user nor organization)".to_owned())
}

/// `membersConnectionStore` arguments from the GraphQL field arguments.
pub fn members_args(arguments: &Arguments) -> MembersArgs {
    MembersArgs {
        first: arguments
            .i64("first")
            .and_then(|first| i32::try_from(first).ok()),
        last: arguments
            .i64("last")
            .and_then(|last| i32::try_from(last).ok()),
        after: arguments.string("after").map(str::to_owned),
        before: arguments.string("before").map(str::to_owned),
        query: arguments.string("query").map(str::to_owned),
    }
}

/// A `settings` row as `settingsResolver` sees it (`settings.go:17-60`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SettingsData {
    pub id: i32,
    /// Raw JSONC contents (comments/trailing commas preserved).
    pub contents: String,
    pub created_at: OffsetDateTime,
    /// Soft-deleted authors are reported as "no author" (SQL CASE).
    pub author_user_id: Option<i32>,
}

/// `db.Settings().GetLatest` result for a subject: `Found(None)` is the
/// baseline's `nil, nil` (no settings row).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LatestSettingsLookup {
    Found(Option<SettingsData>),
    Error(String),
}

/// The checked `SettingsSubject` a `Query.settingsSubject` or
/// `Settings.subject` resolves to (`settings_subject.go:93-123`). `Default`
/// is only reachable through `SettingsCascade.subjects`
/// (`defaultSettingsResolver`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SettingsSubjectData {
    Default,
    User(UserData),
    Org(OrgData),
    Site,
}

/// A prefetched `Query.settingsSubject(id)` result, keyed by the raw Relay ID.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SettingsSubjectLookup {
    Found(SettingsSubjectData),
    Error(String),
}

/// The subject a `settingsCascade` belongs to (`settings_cascade.go:17-28`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CascadeKey {
    Default,
    Site,
    User(i32),
    Org(i32),
}

/// One entry of `SettingsCascade.subjects`: the checked subject and its
/// prefetched `latestSettings` (`resolversForSubjects`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CascadeSubjectEntry {
    pub subject: SettingsSubjectData,
    pub latest: LatestSettingsLookup,
}

/// `settings.Final`: the merged JSON string or the error it would report.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CascadeFinalLookup {
    Found(String),
    Error(String),
}

/// A prefetched `settingsCascade` (`settings_cascade.go:17-99`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SettingsCascadeData {
    pub entries: Vec<CascadeSubjectEntry>,
    pub final_lookup: CascadeFinalLookup,
}

/// A prefetched `SettingsCascade` result, keyed by [`CascadeKey`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CascadeLookup {
    Found(SettingsCascadeData),
    Error(String),
}

/// `defaultSettingsResolver`'s synthetic `latestSettings` row
/// (`default_settings.go:35-45`): ID 0, Go zero `createdAt`, no author.
fn default_settings_data() -> SettingsData {
    SettingsData {
        id: 0,
        contents: DEFAULT_SETTINGS_CONTENTS.to_owned(),
        // 0001-01-01T00:00:00Z, Go's `time.Time{}`.
        created_at: OffsetDateTime::from_unix_timestamp(-62_135_596_800)
            .expect("0001-01-01 is representable"),
        author_user_id: None,
    }
}

/// Data for nested resolver fields that need their own async lookups,
/// prefetched once per request and shared by every object through an `Arc`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct NestedPrefetch {
    pub user_organizations: HashMap<i32, UserOrganizationsLookup>,
    /// Keyed by organization id + argument set.
    pub org_members: HashMap<(i32, MembersArgs), MembersConnectionLookup>,
    /// `User.organizationMemberships` keyed by user id.
    pub user_memberships: HashMap<i32, UserMembershipsLookup>,
    /// `OrganizationMembership.organization` (`OrgByIDInt32`) keyed by org id.
    pub orgs_by_id: HashMap<i32, OrgByIdLookup>,
    /// `OrganizationMembership.user` (`UserByIDInt32`) and
    /// `Settings.author` (`db.Users().GetByID`) keyed by user id.
    pub users_by_id: HashMap<i32, UserByIdLookup>,
    /// `User.latestSettings` keyed by user id.
    pub latest_settings: HashMap<i32, LatestSettingsLookup>,
    /// `Org.latestSettings` keyed by org id.
    pub org_latest_settings: HashMap<i32, LatestSettingsLookup>,
    /// `Site.latestSettings` (global settings, `None` when not prefetched).
    pub site_latest_settings: Option<LatestSettingsLookup>,
    /// `Query.settingsSubject` results keyed by the raw Relay ID.
    pub settings_subjects: HashMap<String, SettingsSubjectLookup>,
    /// Prefetched `settingsCascade` values keyed by subject.
    pub settings_cascades: HashMap<CascadeKey, CascadeLookup>,
    /// `ExternalService.webhookLogs` connections keyed by external service id
    /// and argument set.
    pub external_service_webhook_logs: HashMap<
        (i64, crate::webhook_logs::WebhookLogsArgs),
        crate::webhook_logs::WebhookLogsConnectionLookup,
    >,
    /// Prefetched `SettingsCascade.subjects[... on Site].configuration`
    /// (M9.8jl). `None` when the operation did not select it.
    pub cascade_site_configuration:
        Option<Result<crate::config_graphql::SiteConfigurationRoot, String>>,
    /// Live `auth.providers` snapshot for the Site subject reached through a
    /// settings cascade (M9.8jl); `Site.authProviders`.
    pub auth_providers: Option<Arc<crate::auth_providers_graphql::AuthProvidersRoot>>,
    /// `User.externalAccounts` connections keyed by user id and the `first`
    /// argument (M9.8ka).
    pub user_external_accounts:
        HashMap<(i32, Option<i32>), crate::external_accounts_graphql::ExternalAccountsLookup>,
    /// `CodySubscription` inputs for `ExternalAccount` nodes (M9.8ka).
    pub external_account_cody:
        Option<Arc<crate::external_accounts_graphql::CodySubscriptionConfig>>,
    /// `Site.externalAccounts` connections keyed by their argument set
    /// (M9.9b).
    pub site_external_accounts: HashMap<
        crate::external_accounts_graphql::SiteExternalAccountsArgs,
        crate::external_accounts_graphql::SiteExternalAccountsLookup,
    >,
    /// `User.roles` connections keyed by user id and the argument set
    /// (M9.13b).
    pub user_roles: HashMap<
        (i32, crate::rbac_graphql::RoleConnectionArgs),
        Result<crate::rbac_graphql::RoleConnectionData, String>,
    >,
    /// `User.permissions` connections keyed by user id (M9.13b).
    pub user_permissions:
        HashMap<i32, Result<crate::rbac_graphql::PermissionConnectionData, String>>,
    /// `Role.permissions` connections keyed by role id and the argument set
    /// (M9.13b).
    pub role_permissions: HashMap<
        (i32, crate::rbac_graphql::RoleConnectionArgs),
        Result<crate::rbac_graphql::PermissionConnectionData, String>,
    >,
    /// `User.executorSecrets` connections keyed by user id and the argument
    /// set (M9.13b).
    pub user_executor_secrets: HashMap<
        (i32, crate::executor_secrets_graphql::ExecutorSecretsArgs),
        Result<crate::executor_secrets_graphql::ExecutorSecretConnectionData, String>,
    >,
    /// `Org.executorSecrets` connections keyed by org id and the argument set
    /// (M9.13b).
    pub org_executor_secrets: HashMap<
        (i32, crate::executor_secrets_graphql::ExecutorSecretsArgs),
        Result<crate::executor_secrets_graphql::ExecutorSecretConnectionData, String>,
    >,
    /// `Repository.embeddingExists` lookups (M9.14c). `None` keeps the field
    /// unported when no embeddings seam was injected.
    pub repo_embeddings: Option<Arc<crate::embeddings_graphql::EmbeddingsPrefetch>>,
    /// `Query.statusMessages` / `repositoryStats` / `backgroundJobs` /
    /// `gitservers` snapshots (M9.14c). `None` keeps all four unported when no
    /// diagnostics seam was injected.
    pub diagnostics: Option<Arc<crate::diagnostics_graphql::DiagnosticsPrefetch>>,
    /// `ExecutorSecret.accessLogs` connections keyed by secret id and the
    /// argument set (M9.13b).
    pub executor_secret_access_logs: HashMap<
        (
            i64,
            crate::executor_secrets_graphql::ExecutorSecretAccessLogsArgs,
        ),
        Result<crate::executor_secrets_graphql::ExecutorSecretAccessLogsConnectionData, String>,
    >,
    /// `ExecutorSecretAccessLog.user` nodes keyed by user id (M9.13b).
    pub executor_secret_access_log_users:
        HashMap<i32, crate::executor_secrets_graphql::AccessLogUserLookup>,
    /// `Query.savedSearches` surface (M9.13a).
    pub saved_searches: Option<crate::saved_searches_graphql::SavedSearchesHandleWrapper>,
    /// `Query.batchChanges` surface (M9.24c).
    pub batches: Option<crate::batches_graphql::BatchesHandleWrapper>,
    /// `User.monitors` surface (M9.13a).
    pub code_monitors: Option<crate::code_monitors_graphql::CodeMonitorsHandleWrapper>,
    /// `User.teams` / `Team` surface (M9.27a).
    pub teams: Option<crate::teams_graphql::TeamsHandleWrapper>,
    /// `User.emails` / `primaryEmail` / `accessTokens` / `session` /
    /// `hasVerifiedEmail` per-user account data (M9.18a).
    pub user_accounts: HashMap<i32, crate::graphql_inventory_c::UserAccountsData>,
    /// `Webhook.webhookLogs` connections keyed by webhook id and argument set
    /// (M9.19a). Always queried with `WebhookLogsAllExternalServices`.
    pub webhook_webhook_logs: HashMap<
        (i32, crate::webhook_logs::WebhookLogsArgs),
        crate::webhook_logs::WebhookLogsConnectionLookup,
    >,
}

/// The database seam for `User.organizations` (`db.Orgs().GetByUserID` plus
/// the viewer's organization memberships).
#[async_trait]
pub trait UserOrganizationsStore: Send + Sync {
    async fn organizations_for_user(
        &self,
        user_id: i32,
        viewer_uid: i32,
        viewer_site_admin: bool,
    ) -> Result<Vec<OrgData>, String>;
}

/// `organizationsArgs` from the GraphQL arguments (`orgs.go:15-25`).
pub fn organizations_args(arguments: &Arguments) -> OrganizationsArgs {
    OrganizationsArgs {
        first: arguments
            .i64("first")
            .and_then(|first| i32::try_from(first).ok()),
        query: arguments.string("query").map(str::to_owned),
    }
}

/// The viewer's database ID for membership checks (`0` = anonymous).
fn viewer_uid(actor: Actor) -> i32 {
    actor.uid().unwrap_or(0)
}

/// `auth.CheckOrgAccessOrSiteAdmin`'s error for an unauthorized viewer
/// (`internal/auth/orgs.go:12,43`).
pub(crate) fn org_access_error(actor: Actor) -> &'static str {
    match actor {
        // Unreachable for internal actors: `CheckOrgAccessOrSiteAdmin`
        // returns nil (`internal/auth/orgs.go:27-29`) before this error is
        // built.
        Actor::Internal => "current user is not an org member",
        Actor::Anonymous => "not authenticated",
        Actor::User { .. } => "current user is not an org member",
    }
}

/// `settings.RelevantSubjects` + `settings.Final` + `resolversForSubjects`
/// (`internal/settings/settings.go:137-290`,
/// `cmd/frontend/graphqlbackend/settings_cascade.go:30-58`): builds the
/// ordered subject list, fetches each subject's latest settings (filling the
/// shared `latest_settings`/`org_latest_settings`/`site_latest_settings`
/// maps), prefetches the authors, and computes the merged `final` JSON.
// The data is fetched once per cascade key; the long parameter list mirrors
// the stores each branch needs.
#[allow(clippy::too_many_arguments)]
async fn prefetch_cascade(
    user_orgs: &dyn UserOrganizationsStore,
    memberships: &dyn OrgMembershipsStore,
    key: CascadeKey,
    known_user: Option<&UserData>,
    known_org: Option<&OrgData>,
    nested: &mut NestedPrefetch,
    viewer_uid: i32,
    viewer_site_admin: bool,
    actor: Actor,
) -> CascadeLookup {
    let mut subjects: Vec<SettingsSubjectData> = vec![SettingsSubjectData::Default];
    let mut latest: Vec<LatestSettingsLookup> =
        vec![LatestSettingsLookup::Found(Some(default_settings_data()))];
    if key != CascadeKey::Default {
        let site_lookup = match nested.site_latest_settings.clone() {
            Some(lookup) => lookup,
            None => {
                let lookup = match memberships.latest_settings_for_site().await {
                    Ok(settings) => LatestSettingsLookup::Found(settings),
                    Err(error) => LatestSettingsLookup::Error(error),
                };
                nested.site_latest_settings = Some(lookup.clone());
                lookup
            }
        };
        subjects.push(SettingsSubjectData::Site);
        latest.push(site_lookup);
    }
    match key {
        CascadeKey::Default | CascadeKey::Site => {}
        CascadeKey::Org(org_id) => {
            let org = match known_org {
                Some(org) => org.clone(),
                None => match memberships
                    .org_by_id(org_id, viewer_uid, viewer_site_admin)
                    .await
                {
                    Ok(org) => org,
                    Err(error) => return CascadeLookup::Error(error),
                },
            };
            let lookup = match nested.org_latest_settings.get(&org_id) {
                Some(lookup) => lookup.clone(),
                None => {
                    let lookup = match memberships.latest_settings_for_org(org_id).await {
                        Ok(settings) => LatestSettingsLookup::Found(settings),
                        Err(error) => LatestSettingsLookup::Error(error),
                    };
                    nested.org_latest_settings.insert(org_id, lookup.clone());
                    lookup
                }
            };
            subjects.push(SettingsSubjectData::Org(org));
            latest.push(lookup);
        }
        CascadeKey::User(user_id) => {
            let user = match known_user {
                Some(user) => user.clone(),
                None => match memberships.user_by_id(user_id).await {
                    Ok(user) => user,
                    Err(error) => return CascadeLookup::Error(error),
                },
            };
            // `db.Orgs().GetByUserID`, then the stable sort by id.
            let mut orgs = match user_orgs
                .organizations_for_user(user_id, viewer_uid, viewer_site_admin)
                .await
            {
                Ok(orgs) => orgs,
                Err(error) => return CascadeLookup::Error(error),
            };
            orgs.sort_by_key(|org| org.id);
            for org in &orgs {
                // `resolversForSubjects` runs
                // `settingsSubjectForNodeAndCheckAccess` on every org.
                if !org.viewer_can_administer {
                    return CascadeLookup::Error(org_access_error(actor).to_owned());
                }
                let lookup = match nested.org_latest_settings.get(&org.id) {
                    Some(lookup) => lookup.clone(),
                    None => {
                        let lookup = match memberships.latest_settings_for_org(org.id).await {
                            Ok(settings) => LatestSettingsLookup::Found(settings),
                            Err(error) => LatestSettingsLookup::Error(error),
                        };
                        nested.org_latest_settings.insert(org.id, lookup.clone());
                        lookup
                    }
                };
                subjects.push(SettingsSubjectData::Org(org.clone()));
                latest.push(lookup);
            }
            let lookup = match nested.latest_settings.get(&user_id) {
                Some(lookup) => lookup.clone(),
                None => {
                    let lookup = match memberships.latest_settings_for_user(user_id).await {
                        Ok(settings) => LatestSettingsLookup::Found(settings),
                        Err(error) => LatestSettingsLookup::Error(error),
                    };
                    nested.latest_settings.insert(user_id, lookup.clone());
                    lookup
                }
            };
            subjects.push(SettingsSubjectData::User(user));
            latest.push(lookup);
        }
    }

    let mut contents: Vec<Option<&str>> = Vec::with_capacity(latest.len());
    for lookup in &latest {
        match lookup {
            LatestSettingsLookup::Found(Some(settings)) => {
                contents.push(Some(settings.contents.as_str()))
            }
            LatestSettingsLookup::Found(None) => contents.push(None),
            LatestSettingsLookup::Error(error) => {
                return CascadeLookup::Error(error.clone());
            }
        }
    }
    let final_lookup = match final_settings(&contents) {
        Ok(merged) => CascadeFinalLookup::Found(merged),
        Err(error) => CascadeFinalLookup::Error(error),
    };

    // `Settings.author` for every subject in `subjects`.
    for lookup in &latest {
        let LatestSettingsLookup::Found(Some(settings)) = lookup else {
            continue;
        };
        let Some(author_id) = settings.author_user_id else {
            continue;
        };
        if nested.users_by_id.contains_key(&author_id) {
            continue;
        }
        let author = match memberships.user_by_id(author_id).await {
            Ok(user) => UserByIdLookup::Found(user),
            Err(error) => UserByIdLookup::Error(error),
        };
        nested.users_by_id.insert(author_id, author);
    }

    CascadeLookup::Found(SettingsCascadeData {
        entries: subjects
            .into_iter()
            .zip(latest)
            .map(|(subject, latest)| CascadeSubjectEntry { subject, latest })
            .collect(),
        final_lookup,
    })
}

/// Look up every `Query.organization` root field in `fields`, deduplicated by
/// name. The viewer's membership decides the two authorization flags.
pub async fn prefetch_organizations(
    store: &dyn OrganizationStore,
    fields: &[RootField],
    actor: Actor,
    viewer_site_admin: bool,
) -> HashMap<String, OrganizationLookup> {
    let mut organizations = HashMap::new();
    for field in fields {
        if field.name != "organization" {
            continue;
        }
        let Some(name) = field.arguments.string("name") else {
            continue;
        };
        let name = name.to_owned();
        if organizations.contains_key(&name) {
            continue;
        }
        let lookup = match store
            .organization(&name, viewer_uid(actor), viewer_site_admin)
            .await
        {
            Ok(Some(data)) => OrganizationLookup::Found(data),
            Ok(None) => OrganizationLookup::Error(format!("org not found: name {name}")),
            Err(error) => OrganizationLookup::Error(error),
        };
        organizations.insert(name, lookup);
    }
    organizations
}

/// Look up every `Query.organizations` root field in `fields`, deduplicated by
/// argument set.
pub async fn prefetch_organizations_connections(
    store: &dyn OrganizationsConnectionStore,
    fields: &[RootField],
    actor: Actor,
    viewer_site_admin: bool,
) -> HashMap<OrganizationsArgs, OrganizationsConnectionLookup> {
    let mut connections = HashMap::new();
    for field in fields {
        if field.name != "organizations" {
            continue;
        }
        let args = organizations_args(&field.arguments);
        if connections.contains_key(&args) {
            continue;
        }
        let lookup = match store
            .organizations(&args, viewer_uid(actor), viewer_site_admin)
            .await
        {
            Ok(data) => OrganizationsConnectionLookup::Found(data),
            Err(error) => OrganizationsConnectionLookup::Error(error),
        };
        connections.insert(args, lookup);
    }
    connections
}

/// True when `name` is selected directly, or through an inline fragment /
/// fragment spread (`RootField::type_conditions`).
fn selects_field(selections: &[RootField], name: &str) -> bool {
    selections.iter().any(|selection| selection.name == name)
        || selections.iter().any(|selection| {
            selection
                .type_conditions
                .iter()
                .any(|(_, nested)| selects_field(nested, name))
        })
}

/// `auth.CheckCurrentUserIsSiteAdmin` as a predicate over the already-resolved
/// request actor (`OrgConnectionObject::check_site_admin`).
fn actor_is_site_admin(actor: Actor, viewer_site_admin: bool) -> bool {
    match actor {
        Actor::Internal => true,
        Actor::Anonymous => false,
        Actor::User { .. } => viewer_site_admin,
    }
}

/// M9.22c: write the organization read-path security events after the
/// prefetches have resolved.
///
/// * `Query.organization` (`org.go:30-36`): `OrganizationViewed` on a
///   successful lookup (any actor).
/// * `Query.organizations` (`orgs.go:50-58`): `OrganizationListViewed` only
///   when the connection's `nodes` field is selected — the Go event lives in
///   `Nodes()`, after the site-admin check and `Orgs().List`; `TotalCount()`
///   does not log.
///
/// `auditlog_expansion` is `featureflag.FromContext(ctx).GetBoolOr(...)`.
/// Anonymous/internal actors are skipped: Go writes `user_id=0`/empty
/// anonymous UID, which the `security_event_logs_check_has_user` constraint
/// rejects (internal actors are also skipped by `InsertList` unless
/// `internalTraffic` is enabled).
pub async fn write_org_security_events(
    pool: &sqlx::PgPool,
    fields: &[RootField],
    organizations: &HashMap<String, OrganizationLookup>,
    organizations_connections: &HashMap<OrganizationsArgs, OrganizationsConnectionLookup>,
    actor: Actor,
    viewer_site_admin: bool,
    auditlog_expansion: bool,
) {
    if !auditlog_expansion {
        return;
    }
    let Some(user_id) = actor.uid() else {
        return;
    };
    for field in fields {
        match field.name.as_str() {
            "organization" => {
                let Some(name) = field.arguments.string("name") else {
                    continue;
                };
                if !matches!(organizations.get(name), Some(OrganizationLookup::Found(_))) {
                    continue;
                }
                crate::security_event_logs::write_backend_security_event(
                    pool,
                    crate::security_event_logs::SECURITY_EVENT_ORGANIZATION_VIEWED,
                    user_id,
                    serde_json::json!({ "Name": name }),
                )
                .await;
            }
            "organizations" => {
                if !selects_field(&field.selections, "nodes")
                    || !actor_is_site_admin(actor, viewer_site_admin)
                {
                    continue;
                }
                let args = organizations_args(&field.arguments);
                if !matches!(
                    organizations_connections.get(&args),
                    Some(OrganizationsConnectionLookup::Found(_))
                ) {
                    continue;
                }
                crate::security_event_logs::write_backend_security_event(
                    pool,
                    crate::security_event_logs::SECURITY_EVENT_ORGANIZATION_LIST_VIEWED,
                    user_id,
                    serde_json::json!({}),
                )
                .await;
            }
            _ => {}
        }
    }
}

/// One `User` object waiting for its `organizations`/
/// `organizationMemberships` fields to be prefetched.
struct UserJob {
    user: UserData,
    selections: Vec<RootField>,
}

/// One `Org` object waiting for its `members` field to be prefetched.
struct OrgJob {
    org: OrgData,
    selections: Vec<RootField>,
}

/// One `OrganizationMembership` waiting for its `organization`/`user` fields to
/// be prefetched.
struct MembershipJob {
    membership: MembershipData,
    selections: Vec<RootField>,
}

/// Prefetch nested resolver data for every `User`/`Org` reachable from the
/// already prefetched root fields. This covers `User.organizations`
/// (`user.go:546-562`), `User.organizationMemberships`
/// (`org_members.go:12-30`) with its `OrganizationMembership.organization` /
/// `.user` nodes, and `Org.members` (`org.go:100-121`); the alternating
/// worklist keeps the traversal iterative so deeper object graphs
/// (`User.organizationMemberships.nodes.organization.members`,
/// `User.organizations.nodes.members`, ...) reuse it.
///
/// `User.organizations` / `User.organizationMemberships` authorization mirrors
/// `auth.CheckSiteAdminOrSameUserFromActor`: the data is only fetched when the
/// baseline would reach the query, the field resolver still reports the
/// folded error otherwise. `Org.members` has no authorization in OSS mode
/// (`dotcom` gate is always false there).
// `contains_key` guards the single fetch per id, but the loop must still fall
// through to enqueue the cached organizations' `nodes` selections (and skip
// unauthorized users), so the `Entry` API does not express it cleanly.
#[allow(clippy::too_many_arguments, clippy::map_entry)]
pub async fn prefetch_nested(
    store: &dyn UserOrganizationsStore,
    members_store: &dyn OrgMembersStore,
    memberships_store: &dyn OrgMembershipsStore,
    fields: &[RootField],
    current_user: Option<&UserData>,
    users: &HashMap<UserKey, UserLookup>,
    users_connections: &HashMap<UsersArgs, UsersConnectionLookup>,
    organizations: &HashMap<String, OrganizationLookup>,
    organizations_connections: &HashMap<OrganizationsArgs, OrganizationsConnectionLookup>,
    nodes: &NodeRoots,
    actor: Actor,
    viewer_site_admin: bool,
) -> NestedPrefetch {
    let mut user_queue: Vec<UserJob> = Vec::new();
    let mut org_queue: Vec<OrgJob> = Vec::new();
    let mut membership_queue: Vec<MembershipJob> = Vec::new();
    // The prefetch is built incrementally: `Query.settingsSubject` results,
    // the global (Site) settings row and the `settingsCascade` values are
    // written straight into `nested` so later lookups can reuse them.
    let mut nested = NestedPrefetch::default();
    let mut site_settings_fetched = false;
    let mut site_author_ids: Vec<i32> = Vec::new();
    for field in fields {
        match field.name.as_str() {
            "currentUser" => {
                if let Some(user) = current_user {
                    user_queue.push(UserJob {
                        user: user.clone(),
                        selections: field.selections.clone(),
                    });
                }
            }
            "user" => {
                let UserArg::Key(key) = user_arg(&field.arguments) else {
                    continue;
                };
                if let Some(UserLookup::Found(user)) = users.get(&key) {
                    user_queue.push(UserJob {
                        user: user.clone(),
                        selections: field.selections.clone(),
                    });
                }
            }
            "users" => {
                let args = users_args(&field.arguments);
                let Some(UsersConnectionLookup::Found(data)) = users_connections.get(&args) else {
                    continue;
                };
                for nodes in field
                    .selections
                    .iter()
                    .filter(|selection| selection.name == "nodes")
                {
                    for user in &data.users {
                        user_queue.push(UserJob {
                            user: user.clone(),
                            selections: nodes.selections.clone(),
                        });
                    }
                }
            }
            // `Query.organization` returns the prefetched `orgs` row, whose
            // selections may include `members`.
            "organization" => {
                let Some(name) = field.arguments.string("name") else {
                    continue;
                };
                if let Some(OrganizationLookup::Found(org)) = organizations.get(name) {
                    org_queue.push(OrgJob {
                        org: org.clone(),
                        selections: field.selections.clone(),
                    });
                }
            }
            // `Query.organizations.nodes` are `Org` objects too; the
            // site-admin gate lives in the field resolver, prefetching the
            // members of every returned row is harmless.
            "organizations" => {
                let args = organizations_args(&field.arguments);
                let Some(OrganizationsConnectionLookup::Found(data)) =
                    organizations_connections.get(&args)
                else {
                    continue;
                };
                for nodes in field
                    .selections
                    .iter()
                    .filter(|selection| selection.name == "nodes")
                {
                    for org in &data.orgs {
                        org_queue.push(OrgJob {
                            org: org.clone(),
                            selections: nodes.selections.clone(),
                        });
                    }
                }
            }
            // `Query.node` returns `User`/`Org`/`Repository`; the nested
            // fields live under the matching `... on <type>` fragment
            // (`RootField.type_conditions`). `Repository` has no nested
            // lookups in this framework.
            "node" => {
                let Some(id) = field.arguments.string("id") else {
                    continue;
                };
                let Some(kind) = relay_kind(id) else {
                    continue;
                };
                let Ok(node_id) = unmarshal_relay_id(id) else {
                    continue;
                };
                let mut selections = field.selections.clone();
                for (condition, sub_selections) in &field.type_conditions {
                    // `Query.node` is declared as the `Node` interface;
                    // fragments on `Node` apply to every runtime kind,
                    // fragments on the concrete type only to it
                    // (`selected.go:150-210`).
                    if condition.as_str() == kind || condition.as_str() == "Node" {
                        selections.extend(sub_selections.clone());
                    }
                }
                match kind.as_str() {
                    "User" => {
                        if let Some(UserByIdLookup::Found(user)) = nodes.users.get(&node_id) {
                            user_queue.push(UserJob {
                                user: user.clone(),
                                selections,
                            });
                        }
                    }
                    "Org" => {
                        if let Some(OrgByIdLookup::Found(org)) = nodes.orgs.get(&node_id) {
                            org_queue.push(OrgJob {
                                org: org.clone(),
                                selections,
                            });
                        }
                    }
                    _ => {}
                }
            }
            // `Query.settingsSubject(id)` (`settings_subject.go:16-23`):
            // `nodeByID` dispatch, then `settingsSubjectForNodeAndCheckAccess`.
            // User/Org access failures are reported on this field, exactly like
            // the baseline; `Site` has no access check.
            "settingsSubject" => {
                let Some(id) = field.arguments.string("id") else {
                    continue;
                };
                if nested.settings_subjects.contains_key(id) {
                    continue;
                }
                let Some(kind) = relay_kind(id) else {
                    continue;
                };
                // `SettingsSubject` is an interface: fields declared only on
                // the concrete subject type (`settingsCascade`,
                // `latestSettings`) live in the matching inline fragment.
                let mut subject_selections = field.selections.clone();
                for (type_condition, selections) in &field.type_conditions {
                    if type_condition == &kind {
                        subject_selections.extend(selections.iter().cloned());
                    }
                }
                let lookup = match kind.as_str() {
                    "User" => match unmarshal_relay_id(id) {
                        Err(error) => SettingsSubjectLookup::Error(error),
                        Ok(user_id) => match memberships_store.user_by_id(user_id).await {
                            Err(error) => SettingsSubjectLookup::Error(error),
                            Ok(user) => {
                                let authorized = match actor {
                                    Actor::User { uid } if uid == user.id => true,
                                    _ => viewer_site_admin,
                                };
                                if !authorized {
                                    SettingsSubjectLookup::Error(
                                        MUST_BE_SITE_ADMIN_OR_SAME_USER.to_owned(),
                                    )
                                } else {
                                    user_queue.push(UserJob {
                                        user: user.clone(),
                                        selections: subject_selections.clone(),
                                    });
                                    SettingsSubjectLookup::Found(SettingsSubjectData::User(user))
                                }
                            }
                        },
                    },
                    "Org" => match unmarshal_relay_id(id) {
                        Err(error) => SettingsSubjectLookup::Error(error),
                        Ok(org_id) => match memberships_store
                            .org_by_id(org_id, viewer_uid(actor), viewer_site_admin)
                            .await
                        {
                            Err(error) => SettingsSubjectLookup::Error(error),
                            Ok(org) => {
                                if !org.viewer_can_administer {
                                    SettingsSubjectLookup::Error(org_access_error(actor).to_owned())
                                } else {
                                    org_queue.push(OrgJob {
                                        org: org.clone(),
                                        selections: field.selections.clone(),
                                    });
                                    SettingsSubjectLookup::Found(SettingsSubjectData::Org(org))
                                }
                            }
                        },
                    },
                    // `siteByGQLID` (`site.go:51-61`): the spec must be the
                    // `"site"` singleton.
                    "Site" => match unmarshal_relay_spec_string(id) {
                        Err(error) => SettingsSubjectLookup::Error(error),
                        Ok(site_id) if site_id != "site" => {
                            SettingsSubjectLookup::Error(format!("site not found: {site_id:?}"))
                        }
                        Ok(_) => {
                            let wants_settings = subject_selections
                                .iter()
                                .any(|selection| selection.name == "latestSettings");
                            if wants_settings && !site_settings_fetched {
                                site_settings_fetched = true;
                                let lookup =
                                    match memberships_store.latest_settings_for_site().await {
                                        Ok(settings) => LatestSettingsLookup::Found(settings),
                                        Err(error) => LatestSettingsLookup::Error(error),
                                    };
                                if let LatestSettingsLookup::Found(Some(settings)) = &lookup {
                                    if let Some(author_id) = settings.author_user_id {
                                        site_author_ids.push(author_id);
                                    }
                                }
                                nested.site_latest_settings = Some(lookup);
                            }
                            // `Site.SettingsCascade` (`site.go:161-171`):
                            // no access check for the site subject.
                            let wants_cascade = subject_selections.iter().any(|selection| {
                                matches!(
                                    selection.name.as_str(),
                                    "settingsCascade" | "configurationCascade"
                                )
                            });
                            if wants_cascade
                                && !nested.settings_cascades.contains_key(&CascadeKey::Site)
                            {
                                let cascade = prefetch_cascade(
                                    store,
                                    memberships_store,
                                    CascadeKey::Site,
                                    None,
                                    None,
                                    &mut nested,
                                    viewer_uid(actor),
                                    viewer_site_admin,
                                    actor,
                                )
                                .await;
                                nested.settings_cascades.insert(CascadeKey::Site, cascade);
                            }
                            SettingsSubjectLookup::Found(SettingsSubjectData::Site)
                        }
                    },
                    _ => SettingsSubjectLookup::Error("invalid id".to_owned()),
                };
                nested.settings_subjects.insert(id.to_owned(), lookup);
            }
            // `Query.viewerSettings` (`settings_cascade.go:75-93`): the
            // current user's subject, or the site subject when anonymous /
            // the row is gone.
            "viewerSettings" | "viewerConfiguration" => {
                let key = match (actor, current_user) {
                    (Actor::User { uid }, Some(user)) if user.id == uid => CascadeKey::User(uid),
                    _ => CascadeKey::Site,
                };
                if !nested.settings_cascades.contains_key(&key) {
                    let cascade = prefetch_cascade(
                        store,
                        memberships_store,
                        key,
                        current_user,
                        None,
                        &mut nested,
                        viewer_uid(actor),
                        viewer_site_admin,
                        actor,
                    )
                    .await;
                    nested.settings_cascades.insert(key, cascade);
                }
            }
            _ => {}
        }
    }

    for author_id in site_author_ids {
        if nested.users_by_id.contains_key(&author_id) {
            continue;
        }
        let lookup = match memberships_store.user_by_id(author_id).await {
            Ok(user) => UserByIdLookup::Found(user),
            Err(error) => UserByIdLookup::Error(error),
        };
        nested.users_by_id.insert(author_id, lookup);
    }
    loop {
        let mut progress = false;
        // User level: `User.organizations` and `User.organizationMemberships`;
        // every returned organization becomes an org job and every membership
        // a membership job when the corresponding `nodes` selection is present.
        while let Some(job) = user_queue.pop() {
            let authorized = match actor {
                Actor::User { uid } if uid == job.user.id => true,
                _ => viewer_site_admin,
            };
            for selection in job
                .selections
                .iter()
                .filter(|selection| selection.name == "organizations")
            {
                if !nested.user_organizations.contains_key(&job.user.id) {
                    if !authorized {
                        continue;
                    }
                    let lookup = match store
                        .organizations_for_user(job.user.id, viewer_uid(actor), viewer_site_admin)
                        .await
                    {
                        Ok(orgs) => UserOrganizationsLookup::Found(orgs),
                        Err(error) => UserOrganizationsLookup::Error(error),
                    };
                    nested.user_organizations.insert(job.user.id, lookup);
                    progress = true;
                }
                let Some(UserOrganizationsLookup::Found(orgs)) =
                    nested.user_organizations.get(&job.user.id)
                else {
                    continue;
                };
                for nodes in selection
                    .selections
                    .iter()
                    .filter(|selection| selection.name == "nodes")
                {
                    for org in orgs {
                        org_queue.push(OrgJob {
                            org: org.clone(),
                            selections: nodes.selections.clone(),
                        });
                    }
                }
            }
            for selection in job
                .selections
                .iter()
                .filter(|selection| selection.name == "organizationMemberships")
            {
                if !nested.user_memberships.contains_key(&job.user.id) {
                    if !authorized {
                        continue;
                    }
                    let lookup = match memberships_store.memberships_for_user(job.user.id).await {
                        Ok(memberships) => UserMembershipsLookup::Found(memberships),
                        Err(error) => UserMembershipsLookup::Error(error),
                    };
                    nested.user_memberships.insert(job.user.id, lookup);
                    progress = true;
                }
                let Some(UserMembershipsLookup::Found(memberships)) =
                    nested.user_memberships.get(&job.user.id)
                else {
                    continue;
                };
                for nodes in selection
                    .selections
                    .iter()
                    .filter(|selection| selection.name == "nodes")
                {
                    for membership in memberships {
                        membership_queue.push(MembershipJob {
                            membership: membership.clone(),
                            selections: nodes.selections.clone(),
                        });
                    }
                }
            }
            // `User.latestSettings` (`user.go:374-389`): the same folded
            // access check as `organizations`; the `settings.author` row is
            // prefetched alongside (`settings.go:48-60`).
            for _selection in job
                .selections
                .iter()
                .filter(|selection| selection.name == "latestSettings")
            {
                if !authorized {
                    continue;
                }
                if !nested.latest_settings.contains_key(&job.user.id) {
                    let lookup = match memberships_store
                        .latest_settings_for_user(job.user.id)
                        .await
                    {
                        Ok(settings) => LatestSettingsLookup::Found(settings),
                        Err(error) => LatestSettingsLookup::Error(error),
                    };
                    nested.latest_settings.insert(job.user.id, lookup);
                    progress = true;
                }
                let author_id = match nested.latest_settings.get(&job.user.id) {
                    Some(LatestSettingsLookup::Found(Some(settings))) => settings.author_user_id,
                    _ => continue,
                };
                let Some(author_id) = author_id else {
                    continue;
                };
                if !nested.users_by_id.contains_key(&author_id) {
                    let lookup = match memberships_store.user_by_id(author_id).await {
                        Ok(user) => UserByIdLookup::Found(user),
                        Err(error) => UserByIdLookup::Error(error),
                    };
                    nested.users_by_id.insert(author_id, lookup);
                    progress = true;
                }
            }
            // `User.SettingsCascade` (`user.go:391-401`): the same folded
            // access check as `latestSettings`; the cascade is prefetched for
            // the authorized viewer.
            for _selection in job.selections.iter().filter(|selection| {
                matches!(
                    selection.name.as_str(),
                    "settingsCascade" | "configurationCascade"
                )
            }) {
                if !authorized {
                    continue;
                }
                let key = CascadeKey::User(job.user.id);
                if !nested.settings_cascades.contains_key(&key) {
                    let cascade = prefetch_cascade(
                        store,
                        memberships_store,
                        key,
                        Some(&job.user),
                        None,
                        &mut nested,
                        viewer_uid(actor),
                        viewer_site_admin,
                        actor,
                    )
                    .await;
                    nested.settings_cascades.insert(key, cascade);
                    progress = true;
                }
            }
        }
        // Org level: `Org.members`; every returned user whose `nodes`
        // selection is present becomes a user job (their `organizations` may
        // be selected too).
        while let Some(job) = org_queue.pop() {
            for selection in job
                .selections
                .iter()
                .filter(|selection| selection.name == "members")
            {
                let args = members_args(&selection.arguments);
                let key = (job.org.id, args.clone());
                if !nested.org_members.contains_key(&key) {
                    let lookup = match members_store.members(job.org.id, &args).await {
                        Ok(data) => MembersConnectionLookup::Found(data),
                        Err(error) => MembersConnectionLookup::Error(error),
                    };
                    nested.org_members.insert(key.clone(), lookup);
                    progress = true;
                }
                let Some(MembersConnectionLookup::Found(data)) = nested.org_members.get(&key)
                else {
                    continue;
                };
                for nodes in selection
                    .selections
                    .iter()
                    .filter(|selection| selection.name == "nodes")
                {
                    for user in &data.users {
                        user_queue.push(UserJob {
                            user: user.clone(),
                            selections: nodes.selections.clone(),
                        });
                    }
                }
            }
            // `Org.latestSettings` (`org.go:176-195`): the same org access
            // check as `settingsSubjectForNodeAndCheckAccess`; the author row
            // is prefetched alongside (`settings.go:48-60`).
            for _selection in job
                .selections
                .iter()
                .filter(|selection| selection.name == "latestSettings")
            {
                if !job.org.viewer_can_administer {
                    continue;
                }
                if !nested.org_latest_settings.contains_key(&job.org.id) {
                    let lookup = match memberships_store.latest_settings_for_org(job.org.id).await {
                        Ok(settings) => LatestSettingsLookup::Found(settings),
                        Err(error) => LatestSettingsLookup::Error(error),
                    };
                    if let LatestSettingsLookup::Found(Some(settings)) = &lookup {
                        if let Some(author_id) = settings.author_user_id {
                            if !nested.users_by_id.contains_key(&author_id) {
                                let author = match memberships_store.user_by_id(author_id).await {
                                    Ok(user) => UserByIdLookup::Found(user),
                                    Err(error) => UserByIdLookup::Error(error),
                                };
                                nested.users_by_id.insert(author_id, author);
                            }
                        }
                    }
                    nested.org_latest_settings.insert(job.org.id, lookup);
                    progress = true;
                }
            }
            // `Org.SettingsCascade` (`org.go:194-204`): the org access check
            // then the cascade for this org.
            for _selection in job.selections.iter().filter(|selection| {
                matches!(
                    selection.name.as_str(),
                    "settingsCascade" | "configurationCascade"
                )
            }) {
                if !job.org.viewer_can_administer {
                    continue;
                }
                let key = CascadeKey::Org(job.org.id);
                if !nested.settings_cascades.contains_key(&key) {
                    let cascade = prefetch_cascade(
                        store,
                        memberships_store,
                        key,
                        None,
                        Some(&job.org),
                        &mut nested,
                        viewer_uid(actor),
                        viewer_site_admin,
                        actor,
                    )
                    .await;
                    nested.settings_cascades.insert(key, cascade);
                    progress = true;
                }
            }
        }
        // Membership level: `OrganizationMembership.organization`/`.user`
        // (`OrgByIDInt32`/`UserByIDInt32`); the resolved nodes join the
        // org/user queues so their own nested fields keep being prefetched.
        while let Some(job) = membership_queue.pop() {
            for selection in job
                .selections
                .iter()
                .filter(|selection| selection.name == "organization")
            {
                if !nested.orgs_by_id.contains_key(&job.membership.org_id) {
                    let lookup = match memberships_store
                        .org_by_id(job.membership.org_id, viewer_uid(actor), viewer_site_admin)
                        .await
                    {
                        Ok(org) => OrgByIdLookup::Found(org),
                        Err(error) => OrgByIdLookup::Error(error),
                    };
                    nested.orgs_by_id.insert(job.membership.org_id, lookup);
                    progress = true;
                }
                if let Some(OrgByIdLookup::Found(org)) =
                    nested.orgs_by_id.get(&job.membership.org_id)
                {
                    org_queue.push(OrgJob {
                        org: org.clone(),
                        selections: selection.selections.clone(),
                    });
                }
            }
            for selection in job
                .selections
                .iter()
                .filter(|selection| selection.name == "user")
            {
                if !nested.users_by_id.contains_key(&job.membership.user_id) {
                    let lookup = match memberships_store.user_by_id(job.membership.user_id).await {
                        Ok(user) => UserByIdLookup::Found(user),
                        Err(error) => UserByIdLookup::Error(error),
                    };
                    nested.users_by_id.insert(job.membership.user_id, lookup);
                    progress = true;
                }
                if let Some(UserByIdLookup::Found(user)) =
                    nested.users_by_id.get(&job.membership.user_id)
                {
                    user_queue.push(UserJob {
                        user: user.clone(),
                        selections: selection.selections.clone(),
                    });
                }
            }
        }
        if !progress {
            break;
        }
    }

    nested
}

/// The `repo` row fields the `Repository` resolvers read (one
/// `repoStore.GetByName` row).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RepositoryData {
    pub id: i32,
    pub name: String,
    pub uri: String,
    pub description: String,
    pub fork: bool,
    pub archived: bool,
    pub private: bool,
    pub stars: i32,
    pub external_id: String,
    pub external_service_type: String,
    pub external_service_id: String,
}

/// The result of prefetching one `Query.repository` argument set.
#[derive(Debug, Clone, PartialEq)]
pub enum RepoLookup {
    Found(RepositoryData),
    /// `errcode.IsNotFound` → GraphQL `null`.
    NotFound,
    /// `BlockedRepoError` → GraphQL error (the reason is part of the message).
    Blocked {
        name: String,
        reason: String,
    },
    /// A database failure, surfaced as the resolver error.
    Error(String),
}

/// `Query.repository`'s lookup key. Name and URI are queried separately so a
/// name match is preferred (`repoStore.GetByName`).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum RepoKey {
    Name(String),
    Uri(String),
    /// A `cloneURL` resolved to this repository name by [`CloneUrlResolver`].
    CloneUrl(String),
}

impl RepoKey {
    fn value(&self) -> &str {
        match self {
            RepoKey::Name(value) | RepoKey::Uri(value) | RepoKey::CloneUrl(value) => value,
        }
    }
}

/// The database seam for `Query.repository` (`backend.Repos().GetByName` with
/// the repository permission predicate).
#[async_trait]
pub trait RepositoryLookup: Send + Sync {
    async fn lookup(
        &self,
        name_or_uri: &str,
        authenticated_user_id: i32,
        bypass_authz: bool,
    ) -> Result<RepoLookup, String>;

    /// `repoStore.Get` (`internal/database/repos.go:145-166`) for
    /// `Query.node`'s `Repository` kind: the same `AuthzQueryConds` predicate
    /// as `GetByName` (site admins bypass unless `authz.enforceForSiteAdmins`
    /// is enabled), blocked repositories returned as errors, missing/deleted
    /// → `repo not found`.
    async fn by_id(
        &self,
        repo_id: i32,
        authenticated_user_id: i32,
        bypass_authz: bool,
    ) -> Result<RepoLookup, String>;

    /// `repoStore.GetByHashedName` (`internal/database/repos.go:242-263`) for
    /// `Query.repositoryRedirect(hashedName:)`. Unlike by-name lookups a
    /// missing repository is an error (`RepoNotFoundErr`), not `null`; the
    /// same authz predicate and blocked-repository handling apply.
    async fn by_hashed_name(
        &self,
        hashed_name: &str,
        authenticated_user_id: i32,
        bypass_authz: bool,
    ) -> Result<RepoLookup, String>;
}

/// `cloneurls.RepoSourceCloneURLToRepoName`'s seam: map a git clone URL to a
/// repository name (`Ok(None)` when no configured code host matches).
#[async_trait]
pub trait CloneUrlResolver: Send + Sync {
    async fn repo_name_by_clone_url(&self, clone_url: &str) -> Result<Option<String>, String>;
}

/// What `Query.repository` arguments ask for.
pub(crate) enum RepositoryArg {
    Key(RepoKey),
    /// `cloneURL` mapped through [`CloneUrlResolver`].
    CloneUrl(String),
    /// Neither `name`/`uri` nor `cloneURL`: the baseline errors.
    Missing,
}

/// `schemaResolver.Repository`: `uri` is a deprecated alias for `name` when
/// `name` is absent (`graphqlbackend.go:847-856`).
pub(crate) fn repository_arg(arguments: &Arguments) -> RepositoryArg {
    if let Some(name) = arguments.string("name") {
        return RepositoryArg::Key(RepoKey::Name(name.to_owned()));
    }
    if let Some(uri) = arguments.string("uri") {
        return RepositoryArg::Key(RepoKey::Uri(uri.to_owned()));
    }
    if let Some(clone_url) = arguments.string("cloneURL") {
        return RepositoryArg::CloneUrl(clone_url.to_owned());
    }
    RepositoryArg::Missing
}

/// Prefetch every `User.externalAccounts` selection (M9.8ka).
///
/// `User` objects are reached through `currentUser`, `user`, `users.nodes` and
/// `node(... on User)` — the same entry points [`prefetch_nested`] collects.
/// The baseline checks permission before listing, so unauthorized viewers get
/// no lookup and [`UserObject::field`] returns the folded authorization error.
#[allow(clippy::too_many_arguments)]
pub async fn prefetch_user_external_accounts(
    store: &dyn crate::external_accounts_graphql::ExternalAccountsStore,
    registry: &crate::external_accounts_graphql::ExternalAccountProviderRegistry,
    key: Option<Arc<dyn Key>>,
    fields: &[RootField],
    current_user: Option<&UserData>,
    users: &HashMap<UserKey, UserLookup>,
    users_connections: &HashMap<UsersArgs, UsersConnectionLookup>,
    nodes: &NodeRoots,
    actor: Actor,
    viewer_site_admin: bool,
) -> HashMap<(i32, Option<i32>), crate::external_accounts_graphql::ExternalAccountsLookup> {
    fn push_selections(
        selections_by_user: &mut HashMap<i32, Vec<RootField>>,
        user_id: i32,
        selections: &[RootField],
    ) {
        for selection in selections
            .iter()
            .filter(|selection| selection.name == "externalAccounts")
        {
            selections_by_user
                .entry(user_id)
                .or_default()
                .push(selection.clone());
        }
    }

    let mut selections_by_user: HashMap<i32, Vec<RootField>> = HashMap::new();
    for field in fields {
        match field.name.as_str() {
            "currentUser" => {
                if let Some(user) = current_user {
                    push_selections(&mut selections_by_user, user.id, &field.selections);
                }
            }
            "user" => {
                let UserArg::Key(key) = user_arg(&field.arguments) else {
                    continue;
                };
                if let Some(UserLookup::Found(user)) = users.get(&key) {
                    push_selections(&mut selections_by_user, user.id, &field.selections);
                }
            }
            "users" => {
                let args = users_args(&field.arguments);
                let Some(UsersConnectionLookup::Found(data)) = users_connections.get(&args) else {
                    continue;
                };
                for nodes_selection in field
                    .selections
                    .iter()
                    .filter(|selection| selection.name == "nodes")
                {
                    for user in &data.users {
                        push_selections(
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
                let Some(kind) = relay_kind(id) else {
                    continue;
                };
                if kind != "User" {
                    continue;
                }
                let Ok(node_id) = unmarshal_relay_id(id) else {
                    continue;
                };
                let Some(UserByIdLookup::Found(user)) = nodes.users.get(&node_id) else {
                    continue;
                };
                let mut selections = field.selections.clone();
                for (condition, sub_selections) in &field.type_conditions {
                    if condition.as_str() == "User" || condition.as_str() == "Node" {
                        selections.extend(sub_selections.clone());
                    }
                }
                push_selections(&mut selections_by_user, user.id, &selections);
            }
            _ => {}
        }
    }

    let mut results = HashMap::new();
    for (user_id, selections) in selections_by_user {
        if !crate::external_accounts_graphql::can_view_external_account(
            actor,
            viewer_site_admin,
            user_id,
        ) {
            continue;
        }
        for selection in selections {
            let first = crate::external_accounts_graphql::first_argument(&selection.arguments);
            if results.contains_key(&(user_id, first)) {
                continue;
            }
            let lookup = crate::external_accounts_graphql::external_accounts_connection(
                store,
                registry,
                key.clone(),
                user_id,
                first,
                actor,
                viewer_site_admin,
            )
            .await;
            results.insert((user_id, first), lookup);
        }
    }
    results
}

/// Look up every `Query.repository` root field in `fields`, deduplicated by
/// key. `actor_site_admin` is the authenticated actor's `users.site_admin`
/// (false for anonymous actors); site admins bypass repository permissions
/// unless `authz.enforceForSiteAdmins` is enabled (not read here).
pub async fn prefetch_repositories(
    store: &dyn RepositoryLookup,
    clone_urls: Option<&dyn CloneUrlResolver>,
    fields: &[RootField],
    actor: Actor,
    actor_site_admin: bool,
) -> HashMap<RepoKey, RepoLookup> {
    let authenticated_user_id = actor.uid().unwrap_or(0);
    // `GetAuthzQueryParameters` bypasses authz for internal actors
    // (`internal/database/repos_perm.go:42-45`) and for site admins.
    let bypass_authz = actor.is_internal() || (actor.is_authenticated() && actor_site_admin);
    let mut repositories = HashMap::new();
    for field in fields {
        if field.name != "repository" {
            continue;
        }
        let (key, lookup) = match repository_arg(&field.arguments) {
            RepositoryArg::Key(key) => {
                let lookup = store
                    .lookup(key.value(), authenticated_user_id, bypass_authz)
                    .await
                    .unwrap_or_else(RepoLookup::Error);
                (key, lookup)
            }
            RepositoryArg::CloneUrl(clone_url) => {
                let Some(lookup) = lookup_clone_url(
                    store,
                    clone_urls,
                    &clone_url,
                    authenticated_user_id,
                    bypass_authz,
                )
                .await
                else {
                    continue;
                };
                (RepoKey::CloneUrl(clone_url), lookup)
            }
            RepositoryArg::Missing => continue,
        };
        repositories.entry(key).or_insert(lookup);
    }
    repositories
}

/// `RepoSourceCloneURLToRepoName` followed by `GetByName`: an empty mapping
/// is `null`, a mapping that fails to resolve is the resolver error, and a
/// missing resolver (`None`) leaves the field unported for transport tests.
async fn lookup_clone_url(
    store: &dyn RepositoryLookup,
    clone_urls: Option<&dyn CloneUrlResolver>,
    clone_url: &str,
    authenticated_user_id: i32,
    bypass_authz: bool,
) -> Option<RepoLookup> {
    let resolver = clone_urls?;
    Some(match resolver.repo_name_by_clone_url(clone_url).await {
        Ok(Some(name)) => store
            .lookup(&name, authenticated_user_id, bypass_authz)
            .await
            .unwrap_or_else(RepoLookup::Error),
        Ok(None) => RepoLookup::NotFound,
        Err(error) => RepoLookup::Error(error),
    })
}

/// `Query.repositoryRedirect`'s lookup key. `hashedName` is checked first
/// (`graphqlbackend.go:961-975`); `name` uses the same `GetByName` lookup as
/// `Query.repository`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum RepositoryRedirectKey {
    Name(String),
    Hashed(String),
    /// A `cloneURL` resolved through [`CloneUrlResolver`] (the key keeps the
    /// original URL so dispatch can match it).
    CloneUrl(String),
}

/// What `Query.repositoryRedirect` arguments ask for.
pub enum RepositoryRedirectArg {
    Key(RepositoryRedirectKey),
    /// `cloneURL` mapped through [`CloneUrlResolver`].
    CloneUrl(String),
    /// Neither `name` nor `cloneURL` (nor `hashedName`): the baseline errors.
    Missing,
}

/// `schemaResolver.RepositoryRedirect` argument precedence
/// (`graphqlbackend.go:961-1001`): `hashedName`, then `name`, then
/// `cloneURL`.
pub fn repository_redirect_arg(arguments: &Arguments) -> RepositoryRedirectArg {
    if let Some(hashed_name) = arguments.string("hashedName") {
        return RepositoryRedirectArg::Key(RepositoryRedirectKey::Hashed(hashed_name.to_owned()));
    }
    if let Some(name) = arguments.string("name") {
        return RepositoryRedirectArg::Key(RepositoryRedirectKey::Name(name.to_owned()));
    }
    if let Some(clone_url) = arguments.string("cloneURL") {
        return RepositoryRedirectArg::CloneUrl(clone_url.to_owned());
    }
    RepositoryRedirectArg::Missing
}

/// Look up every `Query.repositoryRedirect` root field in `fields`.
/// `hashedName` misses are `RepoLookup::Error` (the baseline's
/// `RepoNotFoundErr`), while `name` misses stay `RepoLookup::NotFound`
/// (`errcode.IsNotFound` → GraphQL `null`).
pub async fn prefetch_repository_redirects(
    store: &dyn RepositoryLookup,
    clone_urls: Option<&dyn CloneUrlResolver>,
    fields: &[RootField],
    actor: Actor,
    actor_site_admin: bool,
) -> HashMap<RepositoryRedirectKey, RepoLookup> {
    let authenticated_user_id = actor.uid().unwrap_or(0);
    // Internal actors bypass authz (`internal/database/repos_perm.go:42-45`).
    let bypass_authz = actor.is_internal() || (actor.is_authenticated() && actor_site_admin);
    let mut redirects = HashMap::new();
    for field in fields {
        if field.name != "repositoryRedirect" {
            continue;
        }
        let (key, lookup) = match repository_redirect_arg(&field.arguments) {
            RepositoryRedirectArg::Key(key) => {
                let lookup = match &key {
                    RepositoryRedirectKey::Hashed(hashed_name) => {
                        store
                            .by_hashed_name(hashed_name, authenticated_user_id, bypass_authz)
                            .await
                    }
                    RepositoryRedirectKey::Name(name) => {
                        store
                            .lookup(name, authenticated_user_id, bypass_authz)
                            .await
                    }
                    RepositoryRedirectKey::CloneUrl(value) => {
                        store
                            .lookup(value, authenticated_user_id, bypass_authz)
                            .await
                    }
                };
                (key, lookup.unwrap_or_else(RepoLookup::Error))
            }
            RepositoryRedirectArg::CloneUrl(clone_url) => {
                let Some(lookup) = lookup_clone_url(
                    store,
                    clone_urls,
                    &clone_url,
                    authenticated_user_id,
                    bypass_authz,
                )
                .await
                else {
                    continue;
                };
                (RepositoryRedirectKey::CloneUrl(clone_url), lookup)
            }
            RepositoryRedirectArg::Missing => continue,
        };
        redirects.entry(key).or_insert(lookup);
    }
    redirects
}

/// Root objects for the assembled OSS schema.
pub struct FrontendRootResolver {
    query: QueryRoot,
    /// `Mutation` root fields (`Mutation.settingsMutation` plus the optional
    /// search contexts surface).
    mutation: MutationRoot,
}

impl FrontendRootResolver {
    /// Transport tests without a database: anonymous `currentUser` is `null`;
    /// an authenticated actor has "no ported resolver" because the row cannot
    /// be loaded.
    pub fn new() -> Self {
        Self::with_state(
            None,
            HashMap::new(),
            HashMap::new(),
            HashMap::new(),
            HashMap::new(),
            HashMap::new(),
            false,
        )
    }

    /// The production resolver: `user` is the prefetched current user, if the
    /// actor is authenticated and the row still exists.
    pub fn with_user(user: Option<UserObject>) -> Self {
        let viewer_site_admin = user.as_ref().is_some_and(|user| user.data.site_admin);
        Self::with_state(
            user,
            HashMap::new(),
            HashMap::new(),
            HashMap::new(),
            HashMap::new(),
            HashMap::new(),
            viewer_site_admin,
        )
    }

    /// The production resolver with the prefetched `Query.repository` rows.
    pub fn with_user_and_repositories(
        user: Option<UserObject>,
        repositories: HashMap<RepoKey, RepoLookup>,
    ) -> Self {
        let viewer_site_admin = user.as_ref().is_some_and(|user| user.data.site_admin);
        Self::with_state(
            user,
            repositories,
            HashMap::new(),
            HashMap::new(),
            HashMap::new(),
            HashMap::new(),
            viewer_site_admin,
        )
    }

    /// The production resolver with prefetched `Query.user` lookups and the
    /// viewer's `users.site_admin` flag (which `User.siteAdmin`,
    /// `User.email` and `User.viewerCanAdminister` consult).
    pub fn with_user_and_repositories_and_users(
        user: Option<UserObject>,
        repositories: HashMap<RepoKey, RepoLookup>,
        users: HashMap<UserKey, UserLookup>,
        viewer_site_admin: bool,
    ) -> Self {
        Self::with_state(
            user,
            repositories,
            users,
            HashMap::new(),
            HashMap::new(),
            HashMap::new(),
            viewer_site_admin,
        )
    }

    /// The production resolver with the prefetched `Query.users`
    /// connections as well.
    pub fn with_user_and_repositories_and_users_connections(
        user: Option<UserObject>,
        repositories: HashMap<RepoKey, RepoLookup>,
        users: HashMap<UserKey, UserLookup>,
        users_connections: HashMap<UsersArgs, UsersConnectionLookup>,
        viewer_site_admin: bool,
    ) -> Self {
        Self::with_state(
            user,
            repositories,
            users,
            users_connections,
            HashMap::new(),
            HashMap::new(),
            viewer_site_admin,
        )
    }

    /// The production resolver with the prefetched `Query.organization` /
    /// `Query.organizations` results as well.
    #[allow(clippy::too_many_arguments)]
    pub fn with_prefetched_organizations(
        user: Option<UserObject>,
        repositories: HashMap<RepoKey, RepoLookup>,
        users: HashMap<UserKey, UserLookup>,
        users_connections: HashMap<UsersArgs, UsersConnectionLookup>,
        organizations: HashMap<String, OrganizationLookup>,
        organizations_connections: HashMap<OrganizationsArgs, OrganizationsConnectionLookup>,
        viewer_site_admin: bool,
    ) -> Self {
        Self::with_state(
            user,
            repositories,
            users,
            users_connections,
            organizations,
            organizations_connections,
            viewer_site_admin,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn with_state(
        user: Option<UserObject>,
        repositories: HashMap<RepoKey, RepoLookup>,
        users: HashMap<UserKey, UserLookup>,
        users_connections: HashMap<UsersArgs, UsersConnectionLookup>,
        organizations: HashMap<String, OrganizationLookup>,
        organizations_connections: HashMap<OrganizationsArgs, OrganizationsConnectionLookup>,
        viewer_site_admin: bool,
    ) -> Self {
        Self {
            query: QueryRoot {
                inventory: Arc::new(InventoryRoots::default()),
                user,
                repositories,
                users,
                users_connections,
                organizations,
                organizations_connections,
                viewer_site_admin,
                nested: Arc::new(NestedPrefetch::default()),
                nodes: NodeRoots::default(),
                namespaces: NamespaceRoots::default(),
                repository_redirects: HashMap::new(),
                temporary_settings: None,
                client_configuration: None,
                feature_flags: FeatureFlagRoots::default(),
                external_services: HashMap::new(),
                webhook_logs: HashMap::new(),
                repositories_connections: HashMap::new(),
                search_contexts: None,
                ranking_summary: None,
                search: None,
                git: None,
                slow_requests: None,
                slow_requests_prefetch: HashMap::new(),
                site: None,
                settings_subject_site: None,
                auth_providers: None,
                rbac: crate::rbac_graphql::RbacRoots::default(),
                executor_secret_roots:
                    crate::executor_secrets_graphql::ExecutorSecretRoots::default(),
                saved_searches: None,
                code_monitors: None,
                codeintel_uploads: None,
                codeintel_autoindexing: None,
                codeintel_policies: None,
                notebooks: None,
                insights: None,
                batches: None,
                github_apps: None,
                teams: None,
                outbound_webhooks: None,
            },
            mutation: MutationRoot {
                inventory: Arc::new(InventoryRoots::default()),
                teams_viewer_site_admin: viewer_site_admin,
                ..MutationRoot::default()
            },
        }
    }

    /// Attach the request's `Site.authProviders` directory (M9.8jg).
    pub fn with_auth_providers(
        mut self,
        providers: Option<Arc<crate::auth_providers_graphql::AuthProvidersRoot>>,
    ) -> Self {
        self.query.auth_providers = providers;
        self
    }

    /// Attach the request's prefetched `Query.search` result (M9.8ie).
    pub fn with_search_roots(mut self, search: Option<crate::search_graphql::SearchRoot>) -> Self {
        self.query.search = search;
        self
    }

    /// Attach the request's prefetched ranking root fields (M9.8ic).
    pub fn with_ranking_roots(mut self, roots: RankingRoots) -> Self {
        self.query.ranking_summary = roots.summary;
        self.mutation.ranking_mutations = roots.mutations;
        self
    }

    /// Attach the request's pre-executed `Mutation` root fields.
    pub fn with_mutation(mut self, mutation: crate::settings_mutation::MutationRoot) -> Self {
        self.mutation.settings = mutation;
        self
    }

    /// Attach the prefetched M9.12b admin/account write outcomes.
    pub fn with_admin_mutations(
        mut self,
        viewer_site_admin: bool,
        external_url: impl Into<String>,
        outcomes: Vec<(String, crate::admin_store::AdminMutationOutcome)>,
    ) -> Self {
        let external_url = external_url.into();
        for (field, outcome) in outcomes {
            let mutation = std::mem::take(&mut self.mutation);
            self.mutation = mutation.with_admin_mutation(
                viewer_site_admin,
                external_url.clone(),
                field,
                outcome,
            );
        }
        self
    }

    /// Attach the prefetched `Query.roles` / `Query.permissions` roots
    /// (M9.13b).
    pub fn with_rbac_roots(mut self, roots: crate::rbac_graphql::RbacRoots) -> Self {
        self.query.rbac = roots;
        self
    }

    /// Attach the prefetched `Query.executorSecrets` roots (M9.13b).
    pub fn with_executor_secret_roots(
        mut self,
        roots: crate::executor_secrets_graphql::ExecutorSecretRoots,
    ) -> Self {
        self.query.executor_secret_roots = roots;
        self
    }

    /// Attach the prefetched `create/update/deleteExecutorSecret` outcomes
    /// (M9.13b).
    pub fn with_executor_secret_mutations(
        mut self,
        viewer_site_admin: bool,
        nested: Arc<NestedPrefetch>,
        outcomes: Vec<(
            String,
            crate::executor_secrets_graphql::ExecutorSecretMutationOutcome,
        )>,
    ) -> Self {
        self.mutation.executor_secret_viewer_site_admin = viewer_site_admin;
        self.mutation.executor_secret_nested = nested;
        self.mutation.executor_secret_mutations = outcomes.into_iter().collect();
        self
    }

    /// Attach the optional search contexts database (M9.8hz). `None` keeps the
    /// search contexts surface absent, matching a nil
    /// `OptionalResolver.SearchContextsResolver`.
    pub fn with_search_contexts(
        mut self,
        search_contexts: Option<SearchContextsDatabaseHandle>,
    ) -> Self {
        self.query.search_contexts = search_contexts.clone();
        self.mutation.search_contexts = search_contexts;
        self
    }

    /// Attach the optional saved-searches surface (M9.13a).
    pub fn with_saved_searches(
        mut self,
        saved_searches: Option<crate::saved_searches_graphql::SavedSearchesHandle>,
    ) -> Self {
        self.query.saved_searches = saved_searches.clone();
        self.mutation.saved_searches = saved_searches.clone();
        let nested = Arc::make_mut(&mut self.query.nested);
        nested.saved_searches =
            saved_searches.map(crate::saved_searches_graphql::SavedSearchesHandleWrapper);
        self
    }

    /// Attach the optional Batch Changes read surface (M9.24c) and write
    /// mutations (M9.25b).
    pub fn with_batches(
        mut self,
        batches: Option<crate::batches_graphql::BatchesHandle>,
    ) -> Self {
        self.query.batches = batches.clone();
        self.mutation.batches = batches.clone();
        let nested = Arc::make_mut(&mut self.query.nested);
        nested.batches = batches.map(crate::batches_graphql::BatchesHandleWrapper);
        self
    }

    /// Attach the optional GitHub Apps read surface (M9.25c) and write
    /// mutations (M9.26d).
    pub fn with_github_apps(
        mut self,
        github_apps: Option<crate::github_apps_graphql::GitHubAppsHandle>,
    ) -> Self {
        self.query.github_apps = github_apps.clone();
        self.mutation.github_apps = github_apps;
        self
    }

    /// Attach the optional outbound webhook surface (M9.27b).
    pub fn with_outbound_webhooks(
        mut self,
        outbound_webhooks: Option<crate::outbound_webhooks_graphql::OutboundWebhooksHandle>,
    ) -> Self {
        self.query.outbound_webhooks = outbound_webhooks.clone();
        self.mutation.outbound_webhooks = outbound_webhooks;
        self
    }

    /// Attach the injectable GitHub REST transport used by
    /// `Mutation.refreshGitHubApp` (M9.26d). `None` uses `reqwest`.
    pub fn with_github_apps_http(
        mut self,
        http: Option<Arc<dyn crate::github_oauth::GithubHttpTransport>>,
    ) -> Self {
        self.mutation.github_apps_http = http;
        self
    }

    /// Attach the optional Teams surface (M9.27a).
    pub fn with_teams(
        mut self,
        teams: Option<crate::teams_graphql::TeamsHandle>,
    ) -> Self {
        self.query.teams = teams.clone();
        self.mutation.teams = teams.clone();
        let user_handle = teams.clone();
        self.query.user = self
            .query
            .user
            .map(|user| user.with_teams(user_handle.clone()));
        let nested = Arc::make_mut(&mut self.query.nested);
        nested.teams = teams.map(crate::teams_graphql::TeamsHandleWrapper);
        self
    }

    /// Attach the optional code-monitor resolver (M9.13a).
    pub fn with_code_monitors(
        mut self,
        code_monitors: Option<Arc<crate::code_monitors_graphql::CodeMonitorsResolver>>,
    ) -> Self {
        self.query.code_monitors = code_monitors.clone();
        self.mutation.code_monitors = code_monitors.clone();
        let nested = Arc::make_mut(&mut self.query.nested);
        nested.code_monitors =
            code_monitors.map(crate::code_monitors_graphql::CodeMonitorsHandleWrapper);
        self
    }

    /// Attach the optional code-intel uploads database (M9.27d; mutations
    /// added in M9.28a).
    pub fn with_codeintel_uploads(
        mut self,
        codeintel_uploads: Option<crate::codeintel_uploads_graphql::CodeIntelUploadsHandle>,
    ) -> Self {
        self.query.codeintel_uploads = codeintel_uploads.clone();
        self.mutation.codeintel_uploads = codeintel_uploads;
        self
    }

    /// Attach the optional code-intel autoindexing database (M9.28b).
    pub fn with_codeintel_autoindexing(
        mut self,
        codeintel_autoindexing: Option<
            crate::codeintel_autoindexing_graphql::CodeIntelAutoindexingHandle,
        >,
    ) -> Self {
        self.query.codeintel_autoindexing = codeintel_autoindexing.clone();
        self.mutation.codeintel_autoindexing = codeintel_autoindexing;
        self
    }

    /// Attach the optional code-intel configuration-policy surface (M9.28c).
    pub fn with_codeintel_policies(
        mut self,
        codeintel_policies: Option<crate::codeintel_policies_graphql::CodeIntelPoliciesHandle>,
    ) -> Self {
        self.query.codeintel_policies = codeintel_policies.clone();
        self.mutation.codeintel_policies = codeintel_policies;
        self
    }

    /// Attach the optional Notebooks surface (M9.29a).
    pub fn with_notebooks(
        mut self,
        notebooks: Option<crate::notebooks_graphql::NotebooksHandle>,
    ) -> Self {
        self.query.notebooks = notebooks.clone();
        self.mutation.notebooks = notebooks;
        self
    }

    /// Attach the Code Insights surface (read-only M9.29b; dashboard writes
    /// M9.30a).
    pub fn with_insights(
        mut self,
        insights: Option<crate::insights_graphql::InsightsHandle>,
    ) -> Self {
        self.query.insights = insights.clone();
        self.mutation.insights = insights;
        self
    }

    /// Attach the request's prefetched git object/commit data (M9.8ix).
    pub fn with_git(mut self, git: Option<Arc<crate::git_graphql::GitPrefetch>>) -> Self {
        self.query.git = git;
        self
    }

    /// Attach the optional `Query.slowRequests` surface (M9.8jf).
    pub fn with_slow_requests(
        mut self,
        surface: Arc<crate::slow_requests_tracer::SlowRequestsSurface>,
    ) -> Self {
        self.query.slow_requests = Some(surface);
        self
    }

    /// Attach the request's prefetched `Query.slowRequests` connections
    /// (including resolved `user`/`repository` nodes), keyed by `after`.
    pub fn with_slow_requests_prefetch(
        mut self,
        prefetch: HashMap<
            String,
            Result<Arc<crate::slow_requests_tracer::SlowRequestConnectionObject>, String>,
        >,
    ) -> Self {
        self.query.slow_requests_prefetch = prefetch;
        self
    }

    /// Attach the request's prefetched read-only `Query.site` data (M9.8je).
    pub fn with_site(mut self, site: Option<crate::config_graphql::SiteRoot>) -> Self {
        self.query.site = site.map(Arc::new);
        self
    }

    /// Attach the request's prefetched
    /// `Query.settingsSubject(id: <Site>).configuration` data (M9.8jh).
    pub fn with_settings_subject_site_configuration(
        mut self,
        site: Option<crate::config_graphql::SiteRoot>,
    ) -> Self {
        self.query.settings_subject_site = site.map(Arc::new);
        self
    }

    /// Attach the request's prefetched `Query.repositoryRedirect` results.
    pub fn with_repository_redirects(
        mut self,
        redirects: HashMap<RepositoryRedirectKey, RepoLookup>,
    ) -> Self {
        self.query.repository_redirects = redirects;
        self
    }

    /// Attach the request's prefetched `Query.namespace` results.
    pub fn with_namespaces(mut self, namespaces: NamespaceRoots) -> Self {
        self.query.namespaces = namespaces;
        self
    }

    /// Attach the request's prefetched `Query.temporarySettings`.
    pub fn with_temporary_settings(
        mut self,
        temporary_settings: Option<Result<String, String>>,
    ) -> Self {
        self.query.temporary_settings = temporary_settings;
        self
    }

    /// Attach the request's prefetched `Query.clientConfiguration`.
    pub fn with_client_configuration(
        mut self,
        client_configuration: crate::client_configuration::ClientConfigurationLookup,
    ) -> Self {
        self.query.client_configuration = Some(client_configuration);
        self
    }

    /// Attach the request's prefetched `Query.featureFlags` /
    /// `Query.featureFlag` results.
    pub fn with_feature_flags(mut self, feature_flags: FeatureFlagRoots) -> Self {
        self.query.feature_flags = feature_flags;
        self
    }

    /// Attach the request's prefetched M9.16a inventory roots.
    pub fn with_inventory(mut self, inventory: Arc<InventoryRoots>) -> Self {
        self.query.inventory = inventory.clone();
        self.mutation.inventory = inventory;
        self
    }
}

impl Default for FrontendRootResolver {
    fn default() -> Self {
        Self::new()
    }
}

impl RootResolver for FrontendRootResolver {
    fn root(&self, root_type: &str) -> Option<&dyn ObjectValue> {
        if root_type == self.query.type_name() {
            Some(&self.query)
        } else if root_type == self.mutation.type_name() {
            Some(&self.mutation)
        } else {
            None
        }
    }
}

/// Prefetched M9.16a inventory roots. `Option`/map absence is the "no ported
/// resolver" state; the production wiring is M9.16c's.
#[derive(Debug, Clone, Default)]
pub struct InventoryRoots {
    /// `Query.outOfBandMigrations`.
    pub out_of_band_migrations:
        Option<Result<Vec<crate::out_of_band_migrations_graphql::MigrationData>, String>>,
    /// `Query.surveyResponses`.
    pub survey_responses: Option<crate::survey_responses::SurveyResponsesPrefetch>,
    /// `Query.phabricatorRepo` keyed by the raw (non-lowercased) argument.
    pub phabricator:
        HashMap<String, Result<crate::phabricator_graphql::PhabricatorRepoData, String>>,
    /// `Query.codeHostSyncDue` keyed by `(relay ids, seconds)`.
    pub code_host_sync_due: HashMap<(Vec<String>, i32), Result<bool, String>>,
    /// `Query.packageRepoFilters` keyed by `(behaviour, kind)`.
    pub package_repo_filters:
        HashMap<String, Result<Vec<crate::package_repo_filters::PackageRepoFilterData>, String>>,
    /// The evaluated feature-flag business surface.
    pub feature_flags: crate::feature_flags_graphql::FeatureFlagBusinessRoots,
    /// `Mutation.setMigrationDirection` outcome.
    pub migration_direction: Option<Result<(), String>>,
    /// `submitSurvey` / `submitHappinessFeedback`.
    pub survey_mutations: HashMap<String, crate::survey_responses::SurveyMutationOutcome>,
    /// `setUser*Quota` / `setAutoUpgrade`.
    pub admin_mutations:
        HashMap<String, crate::repository_metadata_graphql::InventoryMutationOutcome>,
    /// M9.17a batch-B business roots (`packageRepoReferences`,
    /// `packageRepoReferencesMatchingFilter`, `indexedSearchInstances`,
    /// `outboundRequests`, `invitationByToken` and the batch-B mutations).
    pub business_b: crate::graphql_inventory_b::InventoryBRoots,
    /// M9.18a batch-C business roots (`externalServiceNamespaces`,
    /// `externalServiceRepositories`, `renderMarkdown`, `highlightCode`,
    /// `webhooks` and the batch-C mutations).
    pub business_c: crate::graphql_inventory_c::InventoryCRoots,
    /// M9.19a repository object types (`metadata`, `gitRefs`, `contributors`,
    /// `textSearchIndex`, `externalURLs` and `Query.repoMeta`).
    pub graphql_d: Arc<crate::graphql_inventory_d::InventoryDRoots>,
}

impl InventoryRoots {
    /// One inventory root, defaulting to "no ported resolver".
    pub fn empty() -> Self {
        Self::default()
    }
}

/// `Query.codeHostSyncDue` (`graphqlbackend.go:1026`,
/// `external_services.go:1595-1629`). Empty id lists error before any DB
/// access; unmarshal failures collapse to `unable to unmarshal id`.
pub async fn prefetch_code_host_sync_due(
    pool: &sqlx::PgPool,
    fields: &[RootField],
) -> HashMap<(Vec<String>, i32), Result<bool, String>> {
    let mut lookups = HashMap::new();
    for field in fields {
        if field.name != "codeHostSyncDue" {
            continue;
        }
        let raw_ids: Vec<String> = match field.arguments.get("ids") {
            Some(InputValue::List(items)) => items
                .iter()
                .filter_map(|item| item.as_str().map(str::to_owned))
                .collect(),
            _ => Vec::new(),
        };
        let seconds = field.arguments.i64("seconds").unwrap_or(0) as i32;
        let key = (raw_ids.clone(), seconds);
        if lookups.contains_key(&key) {
            continue;
        }
        if raw_ids.is_empty() {
            lookups.insert(key, Err("no ids supplied".to_owned()));
            continue;
        }
        let mut ids = Vec::with_capacity(raw_ids.len());
        let mut invalid = false;
        for raw in &raw_ids {
            // `UnmarshalExternalServiceID` rejects any other relay kind.
            let kind_ok = matches!(relay_kind(raw).as_deref(), Some("ExternalService"));
            match unmarshal_relay_id(raw) {
                Ok(id) if kind_ok => ids.push(i64::from(id)),
                _ => {
                    invalid = true;
                    break;
                }
            }
        }
        if invalid {
            lookups.insert(key, Err("unable to unmarshal id".to_owned()));
            continue;
        }
        let deadline =
            time::OffsetDateTime::now_utc() + time::Duration::seconds(i64::from(seconds));
        let result = sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS( \
                 SELECT 1 FROM external_services \
                 WHERE id = ANY($1) AND (next_sync_at IS NULL OR next_sync_at <= $2) \
             ) OR EXISTS( \
                 SELECT 1 FROM external_service_sync_jobs \
                 WHERE external_service_id = ANY($1) AND state IN ('queued', 'processing') \
             )",
        )
        .bind(&ids)
        .bind(deadline)
        .fetch_one(pool)
        .await
        .map_err(|error| error.to_string());
        lookups.insert(key, result);
    }
    lookups
}

/// `schemaResolver` (`cmd/frontend/graphqlbackend/graphqlbackend.go:674`).
#[derive(Clone)]
struct QueryRoot {
    /// M9.16a business roots (see [`InventoryRoots`]).
    inventory: Arc<InventoryRoots>,
    /// `None` when the actor is anonymous, the row is gone, or no lookup is
    /// available. The `Actor` decides which case applies.
    user: Option<UserObject>,
    /// Prefetched `Query.repository` results, keyed by the looked-up name/URI.
    repositories: HashMap<RepoKey, RepoLookup>,
    /// Prefetched `Query.user` results, keyed by the lookup argument.
    users: HashMap<UserKey, UserLookup>,
    /// Prefetched `Query.users` connections, keyed by the argument set.
    users_connections: HashMap<UsersArgs, UsersConnectionLookup>,
    /// Prefetched `Query.organization` results, keyed by the looked-up name.
    organizations: HashMap<String, OrganizationLookup>,
    /// Prefetched `Query.organizations` connections, keyed by the argument set.
    organizations_connections: HashMap<OrganizationsArgs, OrganizationsConnectionLookup>,
    /// The viewer's `users.site_admin`; see
    /// `auth.CheckSiteAdminOrSameUserFromActor`.
    viewer_site_admin: bool,
    /// Nested resolver data prefetched for this request (see
    /// [`prefetch_nested`]); attached to every `User` object the root builds.
    nested: Arc<NestedPrefetch>,
    /// Prefetched `Query.node` results (see [`prefetch_node_roots`]).
    nodes: NodeRoots,
    /// Prefetched `Query.namespace`/`Query.namespaceByName` results (see
    /// [`prefetch_namespace_roots`]).
    namespaces: NamespaceRoots,
    /// Prefetched `Query.repositoryRedirect` results (see
    /// [`prefetch_repository_redirects`]).
    repository_redirects: HashMap<RepositoryRedirectKey, RepoLookup>,
    /// Prefetched `Query.temporarySettings` contents (see
    /// [`crate::temporary_settings::prefetch_temporary_settings`]).
    temporary_settings: Option<Result<String, String>>,
    /// Prefetched `Query.clientConfiguration` (see
    /// [`crate::client_configuration::prefetch_client_configuration`]).
    client_configuration: Option<crate::client_configuration::ClientConfigurationLookup>,
    /// Prefetched `Query.featureFlags` / `Query.featureFlag` results (see
    /// [`prefetch_feature_flags`]).
    feature_flags: FeatureFlagRoots,
    /// Prefetched `Query.externalServices` results (see
    /// [`crate::external_services::prefetch_external_services`]).
    external_services: HashMap<
        crate::external_services::ExternalServicesArgs,
        crate::external_services::ExternalServicesConnectionLookup,
    >,
    /// Prefetched `Query.webhookLogs` results (see
    /// [`crate::webhook_logs::prefetch_webhook_logs`]).
    webhook_logs: HashMap<
        crate::webhook_logs::GlobalWebhookLogsArgs,
        crate::webhook_logs::WebhookLogsConnectionLookup,
    >,
    /// Prefetched `Query.repositories` connections (see
    /// [`crate::repositories::prefetch_repositories_connections`]).
    repositories_connections: HashMap<
        crate::repositories::RepositoriesArgs,
        crate::repositories::RepositoriesConnectionLookup,
    >,
    /// Optional search contexts database (M9.8hz). `None` means the SDL was
    /// not appended and the root fields are absent.
    search_contexts: Option<SearchContextsDatabaseHandle>,
    /// Prefetched `Query.rankingSummary` (M9.8ic); `None` when not selected or
    /// the ranking resolver is not injected.
    ranking_summary: Option<Result<crate::ranking_graphql::GlobalRankingSummary, String>>,
    /// Prefetched `Query.search` (M9.8ie); `None` when the field was not
    /// selected or no search executor is injected.
    search: Option<crate::search_graphql::SearchRoot>,
    /// Prefetched gitserver object/commit data (M9.8ix); `None` when no
    /// `GitObjectSource` was injected (git fields stay unported).
    git: Option<Arc<crate::git_graphql::GitPrefetch>>,
    /// Optional `Query.slowRequests` surface (M9.8jf); `None` keeps the field
    /// unported (the baseline's "capture not enabled"/absent-resolver state).
    slow_requests: Option<Arc<crate::slow_requests_tracer::SlowRequestsSurface>>,
    /// Prefetched `Query.slowRequests` connections, keyed by the `after` cursor
    /// (see [`crate::slow_requests_tracer::prefetch_slow_requests`]).
    slow_requests_prefetch: HashMap<
        String,
        Result<Arc<crate::slow_requests_tracer::SlowRequestConnectionObject>, String>,
    >,
    /// Prefetched read-only `Query.site.configuration` data (M9.8je); `None`
    /// when the configuration surface was not injected or not selected.
    site: Option<Arc<crate::config_graphql::SiteRoot>>,
    /// Prefetched `Query.settingsSubject(id: <Site>).configuration` data
    /// (M9.8jh). This is a separate prefetch from [`Self::site`] because the
    /// baseline runs the admin check and security-event audit per resolver.
    settings_subject_site: Option<Arc<crate::config_graphql::SiteRoot>>,
    /// `Site.authProviders` derived from the live `auth.providers`
    /// configuration (M9.8jg); `None` when the directory was not injected.
    auth_providers: Option<Arc<crate::auth_providers_graphql::AuthProvidersRoot>>,
    /// Prefetched `Query.roles` / `Query.permissions` (M9.13b).
    rbac: crate::rbac_graphql::RbacRoots,
    /// Prefetched `Query.executorSecrets` (M9.13b).
    executor_secret_roots: crate::executor_secrets_graphql::ExecutorSecretRoots,
    /// Optional saved-searches database (M9.13a). `None` means the SDL was not
    /// appended and the root field is absent.
    saved_searches: Option<crate::saved_searches_graphql::SavedSearchesHandle>,
    /// Optional Batch Changes database (M9.24c). `None` means the SDL was not
    /// appended and the root fields are absent.
    batches: Option<crate::batches_graphql::BatchesHandle>,
    /// Optional GitHub Apps database (M9.25c). `None` reproduces an absent
    /// `GitHubAppsResolver`: the `githubapps.graphql` document is not appended
    /// and `Query.gitHubApps` / `gitHubApp` / `gitHubAppByAppID` /
    /// `node(GitHubApp)` are absent.
    github_apps: Option<crate::github_apps_graphql::GitHubAppsHandle>,
    /// Optional code-monitor resolver (M9.13a).
    code_monitors: Option<Arc<crate::code_monitors_graphql::CodeMonitorsResolver>>,
    /// Optional Teams surface (M9.27a). `None` keeps `Query.team`/`teams` and
    /// `node(Team)` unported.
    teams: Option<crate::teams_graphql::TeamsHandle>,
    /// Optional code-intel uploads database (M9.27d). `None` means
    /// `codeintel.autoindexing.graphql` was not appended and
    /// `Query.preciseIndexes` is absent.
    codeintel_uploads: Option<crate::codeintel_uploads_graphql::CodeIntelUploadsHandle>,
    /// Optional code-intel autoindexing database (M9.28b). `None` keeps the
    /// autoindexing root fields and `Repository.indexConfiguration` unported.
    codeintel_autoindexing:
        Option<crate::codeintel_autoindexing_graphql::CodeIntelAutoindexingHandle>,
    /// Optional configuration-policy surface (M9.28c). `None` leaves the
    /// `codeintel.policies.graphql` fields unported.
    codeintel_policies: Option<crate::codeintel_policies_graphql::CodeIntelPoliciesHandle>,
    /// Optional Notebooks surface (M9.29a). `None` leaves the
    /// `notebooks.graphql` fields unported.
    notebooks: Option<crate::notebooks_graphql::NotebooksHandle>,
    /// Optional Code Insights read-only surface (M9.29b). `None` leaves
    /// `Query.insightViews` / `Query.insightsDashboards` unported.
    insights: Option<crate::insights_graphql::InsightsHandle>,
    /// Optional outbound webhook surface (M9.27b). `None` leaves
    /// `Query.outboundWebhooks` / `outboundWebhookEventTypes` and
    /// `node(OutboundWebhook)` unported.
    outbound_webhooks: Option<crate::outbound_webhooks_graphql::OutboundWebhooksHandle>,
}

/// `Mutation` root: `settingsMutation`/log events/temporary settings/feature
/// flags/site configuration from [`crate::settings_mutation::MutationRoot`],
/// plus the optional search contexts mutations (M9.8hz).
#[derive(Default)]
pub struct MutationRoot {
    /// M9.16a business mutations (see [`InventoryRoots`]).
    inventory: Arc<InventoryRoots>,
    /// Viewer `site_admin` for the M9.18a batch-C mutation responses.
    business_c_viewer_site_admin: bool,
    /// Nested prefetch attached to the users/webhooks batch-C mutations
    /// return.
    business_c_nested: Arc<NestedPrefetch>,
    settings: crate::settings_mutation::MutationRoot,
    search_contexts: Option<SearchContextsDatabaseHandle>,
    /// Prefetched `Mutation.bumpDerivativeGraphKey` /
    /// `Mutation.deleteRankingProgress` outcomes (M9.8ic).
    ranking_mutations: HashMap<String, Result<(), String>>,
    /// Prefetched admin/account write outcomes (M9.12b).
    admin_mutations: HashMap<String, crate::admin_store::AdminMutationOutcome>,
    /// The viewer's `site_admin` flag for the objects the writes return.
    admin_viewer_site_admin: bool,
    /// `ExternalService.webhookURL` in mutation responses.
    admin_external_url: String,
    /// Prefetched `create/update/deleteExecutorSecret` outcomes (M9.13b).
    executor_secret_mutations:
        HashMap<String, crate::executor_secrets_graphql::ExecutorSecretMutationOutcome>,
    /// The viewer's `site_admin` and nested data for the secrets returned by
    /// those mutations.
    executor_secret_viewer_site_admin: bool,
    executor_secret_nested: Arc<NestedPrefetch>,
    /// Optional saved-searches database (M9.13a).
    saved_searches: Option<crate::saved_searches_graphql::SavedSearchesHandle>,
    /// Optional code-monitor resolver (M9.13a).
    code_monitors: Option<Arc<crate::code_monitors_graphql::CodeMonitorsResolver>>,
    /// Optional Batch Changes write surface (M9.25b).
    batches: Option<crate::batches_graphql::BatchesHandle>,
    /// Optional GitHub Apps write surface (M9.26d).
    github_apps: Option<crate::github_apps_graphql::GitHubAppsHandle>,
    /// Optional Teams mutation surface (M9.27a).
    teams: Option<crate::teams_graphql::TeamsHandle>,
    /// The viewer's `users.site_admin` for `createTeam(readonly: true)` and
    /// `canModifyTeam`.
    teams_viewer_site_admin: bool,
    /// Injectable GitHub REST transport for `refreshGitHubApp`; `None` uses
    /// the production `reqwest` client.
    github_apps_http: Option<Arc<dyn crate::github_oauth::GithubHttpTransport>>,
    /// Optional outbound webhook mutations (M9.27b).
    outbound_webhooks: Option<crate::outbound_webhooks_graphql::OutboundWebhooksHandle>,
    /// Optional code-intel uploads mutations (M9.28a): `deletePreciseIndex(es)`
    /// / `reindexPreciseIndex(es)`.
    codeintel_uploads: Option<crate::codeintel_uploads_graphql::CodeIntelUploadsHandle>,
    /// Optional code-intel autoindexing mutations (M9.28b).
    codeintel_autoindexing:
        Option<crate::codeintel_autoindexing_graphql::CodeIntelAutoindexingHandle>,
    /// Optional configuration-policy mutations (M9.28c).
    codeintel_policies: Option<crate::codeintel_policies_graphql::CodeIntelPoliciesHandle>,
    /// Optional Notebooks mutations (M9.29a).
    notebooks: Option<crate::notebooks_graphql::NotebooksHandle>,
    /// Optional Code Insights dashboard mutations (M9.30a).
    insights: Option<crate::insights_graphql::InsightsHandle>,
}

impl MutationRoot {
    /// Attach one prefetched M9.12b admin/account write outcome.
    pub fn with_admin_mutation(
        mut self,
        viewer_site_admin: bool,
        external_url: impl Into<String>,
        field: impl Into<String>,
        outcome: crate::admin_store::AdminMutationOutcome,
    ) -> Self {
        self.admin_viewer_site_admin = viewer_site_admin;
        self.admin_external_url = external_url.into();
        self.admin_mutations.insert(field.into(), outcome);
        self
    }

    /// Render one prefetched admin mutation outcome.
    fn admin_mutation_value(
        &self,
        outcome: &crate::admin_store::AdminMutationOutcome,
    ) -> Result<Option<FieldValue>, ExecError> {
        use crate::admin_store::AdminMutationOutcome as Outcome;
        match outcome {
            Outcome::CreateUser {
                user,
                reset_password_url,
            } => Ok(Some(FieldValue::Object(Arc::new(
                crate::admin_mutations::CreateUserResultObject {
                    user: user.clone(),
                    reset_password_url: reset_password_url.clone(),
                    viewer_site_admin: self.admin_viewer_site_admin,
                },
            )))),
            Outcome::User(user) => Ok(Some(FieldValue::Object(Arc::new(
                UserObject::from_data_as_viewer(user.user_data(), self.admin_viewer_site_admin),
            )))),
            Outcome::Org {
                org,
                viewer_can_administer,
                viewer_is_member,
            } => Ok(Some(FieldValue::Object(Arc::new(
                OrgObject::from_data_as_viewer(
                    org.org_data(*viewer_can_administer, *viewer_is_member),
                    self.admin_viewer_site_admin,
                ),
            )))),
            Outcome::AccessToken(created) => Ok(Some(FieldValue::Object(Arc::new(
                crate::admin_mutations::CreateAccessTokenResultObject {
                    created: created.clone(),
                },
            )))),
            Outcome::ExternalService { row, warning } => Ok(Some(FieldValue::Object(Arc::new(
                crate::external_services::ExternalServiceObject::from_mutation_row(
                    row,
                    warning.clone(),
                    self.admin_viewer_site_admin,
                    &self.admin_external_url,
                ),
            )))),
            Outcome::Empty => Ok(Some(FieldValue::Object(Arc::new(
                crate::temporary_settings::EmptyResponseObject,
            )))),
            Outcome::Null => Ok(Some(FieldValue::Null)),
            Outcome::Error(message) => Err(ExecError::new(message.clone())),
        }
    }
}

impl ObjectValue for MutationRoot {
    fn type_name(&self) -> &'static str {
        "Mutation"
    }

    fn field(
        &self,
        field: &str,
        arguments: &Arguments,
        actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        // M9.26a Batch Changes spec mutations (`createBatchSpec`,
        // `createChangesetSpec`, `createChangesetSpecs`).
        if let Some(batches) = &self.batches {
            if crate::batches_specs_graphql::is_mutation_field(field) {
                return crate::batches_specs_graphql::mutation_field(
                    batches,
                    field,
                    arguments,
                    actor,
                );
            }
        }
        // M9.25b Batch Changes mutations. The resolver runs the enterprise
        // gate, RBAC check and database work directly (the store bridges with
        // `block_in_place`), matching the Go `Resolver` methods.
        if let Some(batches) = &self.batches {
            if crate::batches_mutations::is_mutation_field(field) {
                return crate::batches_mutations::mutation_field(batches, field, arguments, actor);
            }
        }
        // M9.27c Batch Changes bulk operations (`detachChangesets`,
        // `createChangesetComments`, `mergeChangesets`, `closeChangesets`,
        // `publishChangesets`); `reenqueueChangesets` stays above.
        if let Some(batches) = &self.batches {
            if crate::batches_bulk_mutations::is_mutation_field(field) {
                return crate::batches_bulk_mutations::mutation_field(
                    batches, field, arguments, actor,
                );
            }
        }
        // M9.26d GitHub Apps mutations: `deleteGitHubApp` / `refreshGitHubApp`.
        // `refreshGitHubApp` replays the GitHub REST sync through the injected
        // transport (production: `reqwest`).
        if let Some(github_apps) = &self.github_apps {
            if crate::github_apps_graphql::is_mutation_field(field) {
                return crate::github_apps_graphql::mutation_field(
                    github_apps,
                    self.github_apps_http.as_deref(),
                    field,
                    arguments,
                    actor,
                );
            }
        }
        // M9.28b Code Intel autoindexing mutations.
        if let Some(codeintel_autoindexing) = &self.codeintel_autoindexing {
            if crate::codeintel_autoindexing_graphql::is_mutation_field(field) {
                return crate::codeintel_autoindexing_graphql::mutation_field(
                    codeintel_autoindexing,
                    field,
                    arguments,
                    actor,
                );
            }
        }
        // M9.27b outbound webhook mutations.
        if let Some(outbound_webhooks) = &self.outbound_webhooks {
            if crate::outbound_webhooks_graphql::is_mutation_field(field) {
                return crate::outbound_webhooks_graphql::mutation_field(
                    outbound_webhooks,
                    field,
                    arguments,
                    actor,
                );
            }
        }
        // M9.28a code-intel uploads mutations.
        if let Some(codeintel_uploads) = &self.codeintel_uploads {
            if crate::codeintel_uploads_graphql::is_mutation_field(field) {
                return crate::codeintel_uploads_graphql::mutation_field(
                    codeintel_uploads,
                    field,
                    arguments,
                    actor,
                );
            }
        }
        // M9.28c configuration-policy mutations.
        if let Some(codeintel_policies) = &self.codeintel_policies {
            if crate::codeintel_policies_graphql::is_mutation_field(field) {
                return crate::codeintel_policies_graphql::mutation_field(
                    codeintel_policies,
                    field,
                    arguments,
                    actor,
                );
            }
        }
        // M9.29a Notebooks mutations.
        if let Some(notebooks) = &self.notebooks {
            if crate::notebooks_graphql::is_mutation_field(field) {
                return crate::notebooks_graphql::mutation_field(
                    notebooks, field, arguments, actor,
                );
            }
        }
        // M9.30a Code Insights dashboard mutations.
        if let Some(insights) = &self.insights {
            if crate::insights_graphql::is_mutation_field(field) {
                return crate::insights_graphql::mutation_field(
                    insights, field, arguments, actor,
                );
            }
        }
        if self.search_contexts.is_some()
            && crate::search_contexts_schema::field_handler(
                SearchContextsOperation::Mutation,
                field,
            )
            .is_some()
        {
            return dispatch_search_context_field(
                self.search_contexts.as_ref(),
                SearchContextsOperation::Mutation,
                field,
                arguments,
                actor,
            );
        }
        // `CodeIntelResolver.BumpDerivativeGraphKey` / `DeleteRankingProgress`
        // (M9.8ic): prefetched outcome replayed as the baseline non-nil
        // `EmptyResponse` (`alwaysNil: null`) or the resolver error.
        if let Some(outcome) = self.ranking_mutations.get(field) {
            return match outcome {
                Ok(()) => Ok(Some(FieldValue::Object(Arc::new(
                    crate::temporary_settings::EmptyResponseObject,
                )))),
                Err(message) => Err(ExecError::new(message.clone())),
            };
        }
        if let Some(outcome) = self.admin_mutations.get(field) {
            return self.admin_mutation_value(outcome);
        }
        if let Some(outcome) = self.executor_secret_mutations.get(field) {
            use crate::executor_secrets_graphql::ExecutorSecretMutationOutcome as Outcome;
            return match outcome {
                Outcome::Secret(secret) => Ok(Some(FieldValue::Object(Arc::new(
                    crate::executor_secrets_graphql::ExecutorSecretObject {
                        secret: secret.clone(),
                        viewer_site_admin: self.executor_secret_viewer_site_admin,
                        nested: self.executor_secret_nested.clone(),
                    },
                )))),
                Outcome::Empty => Ok(Some(FieldValue::Object(Arc::new(
                    crate::temporary_settings::EmptyResponseObject,
                )))),
                Outcome::Error(message) => Err(ExecError::new(message.clone())),
            };
        }
        // M9.16a feature-flag override mutations.
        if let Some(outcome) = self.inventory.feature_flags.mutations.get(field) {
            use crate::feature_flags_graphql::OverrideMutationOutcome as Outcome;
            return match outcome {
                Outcome::Override { data, registry } => Ok(Some(FieldValue::Object(Arc::new(
                    crate::feature_flags_graphql::OrgOverrideObject {
                        data: data.clone(),
                        registry: registry.clone(),
                    },
                )))),
                Outcome::Empty => Ok(Some(FieldValue::Object(Arc::new(
                    crate::temporary_settings::EmptyResponseObject,
                )))),
                Outcome::Error(message) => Err(ExecError::new(message.clone())),
            };
        }
        // M9.16a `setMigrationDirection`.
        if field == "setMigrationDirection" {
            if let Some(outcome) = &self.inventory.migration_direction {
                return match outcome {
                    Ok(()) => Ok(Some(FieldValue::Object(Arc::new(
                        crate::temporary_settings::EmptyResponseObject,
                    )))),
                    Err(message) => Err(ExecError::new(message.clone())),
                };
            }
        }
        // M9.16a survey mutations.
        if let Some(outcome) = self.inventory.survey_mutations.get(field) {
            use crate::survey_responses::SurveyMutationOutcome as Outcome;
            return match outcome {
                Outcome::Empty => Ok(Some(FieldValue::Object(Arc::new(
                    crate::temporary_settings::EmptyResponseObject,
                )))),
                Outcome::Error(message) => Err(ExecError::new(message.clone())),
            };
        }
        // M9.16a quota / auto-upgrade mutations.
        if let Some(outcome) = self.inventory.admin_mutations.get(field) {
            use crate::repository_metadata_graphql::InventoryMutationOutcome as Outcome;
            return match outcome {
                Outcome::User(data) => Ok(Some(FieldValue::Object(Arc::new(
                    crate::repository_metadata_graphql::user_object(
                        data,
                        self.admin_viewer_site_admin,
                    ),
                )))),
                Outcome::Empty => Ok(Some(FieldValue::Object(Arc::new(
                    crate::temporary_settings::EmptyResponseObject,
                )))),
                Outcome::Error(message) => Err(ExecError::new(message.clone())),
            };
        }
        // M9.17a batch-B mutations.
        if let Some(outcome) = self.inventory.business_b.mutations.get(field) {
            return crate::graphql_inventory_b::InventoryBRoots::mutation_value(outcome);
        }
        // M9.18a batch-C mutations.
        if let Some(outcome) = self.inventory.business_c.mutations.get(field) {
            return crate::graphql_inventory_c::InventoryCRoots::mutation_value(
                outcome,
                self.business_c_viewer_site_admin,
                &self.business_c_nested,
            );
        }
        if let Some(saved_searches) = &self.saved_searches {
            if crate::saved_searches_graphql::is_root_field(field) {
                return crate::saved_searches_graphql::root_field(
                    saved_searches,
                    field,
                    arguments,
                    actor,
                );
            }
        }
        if let Some(code_monitors) = &self.code_monitors {
            if crate::code_monitors_graphql::is_root_field(field) {
                return crate::code_monitors_graphql::root_field(
                    code_monitors,
                    field,
                    arguments,
                    actor,
                );
            }
        }
        // M9.27a Teams mutations.
        if let Some(teams) = &self.teams {
            if crate::teams_graphql::is_mutation_field(field) {
                return crate::teams_graphql::mutation_field(
                    teams,
                    field,
                    arguments,
                    actor,
                    self.teams_viewer_site_admin,
                );
            }
        }
        self.settings.field(field, arguments, actor)
    }
}

impl FrontendRootResolver {
    /// Attach the request's nested prefetch data.
    pub fn with_nested(mut self, nested: Arc<NestedPrefetch>) -> Self {
        self.mutation.business_c_nested = Arc::clone(&nested);
        self.query.nested = nested;
        self
    }

    /// Attach the viewer `site_admin` flag for the M9.18a batch-C mutation
    /// responses.
    pub fn with_inventory_c_viewer(mut self, viewer_site_admin: bool) -> Self {
        self.mutation.business_c_viewer_site_admin = viewer_site_admin;
        self
    }

    /// Attach the request's `Query.node` prefetch data.
    pub fn with_nodes(mut self, nodes: NodeRoots) -> Self {
        self.query.nodes = nodes;
        self
    }

    /// Attach the request's `Query.externalServices` prefetch data.
    pub fn with_external_services(
        mut self,
        external_services: HashMap<
            crate::external_services::ExternalServicesArgs,
            crate::external_services::ExternalServicesConnectionLookup,
        >,
    ) -> Self {
        self.query.external_services = external_services;
        self
    }

    /// Attach the request's `Query.webhookLogs` prefetch data.
    pub fn with_webhook_logs(
        mut self,
        webhook_logs: HashMap<
            crate::webhook_logs::GlobalWebhookLogsArgs,
            crate::webhook_logs::WebhookLogsConnectionLookup,
        >,
    ) -> Self {
        self.query.webhook_logs = webhook_logs;
        self
    }

    /// Attach the request's `Query.repositories` prefetch data.
    pub fn with_repositories_connections(
        mut self,
        repositories_connections: HashMap<
            crate::repositories::RepositoriesArgs,
            crate::repositories::RepositoriesConnectionLookup,
        >,
    ) -> Self {
        self.query.repositories_connections = repositories_connections;
        self
    }
}

/// Render a prefetched `NamespaceLookup` as the concrete `User`/`Org` object,
/// `null` (`namespaceByName` miss) or the lookup error.
fn namespace_value(
    root: &QueryRoot,
    lookup: Option<&NamespaceLookup>,
) -> Result<Option<FieldValue>, ExecError> {
    match lookup {
        Some(NamespaceLookup::User(data)) => Ok(Some(FieldValue::Object(Arc::new(
            UserObject::from_data_as_viewer(data.clone(), root.viewer_site_admin)
                .with_nested(root.nested.clone()),
        )))),
        Some(NamespaceLookup::Org(data)) => Ok(Some(FieldValue::Object(Arc::new(
            OrgObject::from_data_as_viewer(data.clone(), root.viewer_site_admin)
                .with_nested(root.nested.clone()),
        )))),
        Some(NamespaceLookup::NotFound) => Ok(Some(FieldValue::Null)),
        Some(NamespaceLookup::Error(message)) => Err(ExecError::new(message.clone())),
        None => Ok(None),
    }
}

impl QueryRoot {
    /// The int32-spec `Query.node` kinds. `relay.UnmarshalSpec` is shared by
    /// User/Org/Repository; none validates the Relay kind.
    fn node_int32(&self, kind: &str, id: &str) -> Result<Option<FieldValue>, ExecError> {
        match kind {
            "User" => {
                let node_id = unmarshal_relay_id(id).map_err(ExecError::new)?;
                match self.nodes.users.get(&node_id) {
                    Some(UserByIdLookup::Found(data)) => Ok(Some(FieldValue::Object(Arc::new(
                        UserObject::from_data_as_viewer(data.clone(), self.viewer_site_admin)
                            .with_nested(self.nested.clone()),
                    )))),
                    Some(UserByIdLookup::Error(message)) => Err(ExecError::new(message.clone())),
                    None => Ok(None),
                }
            }
            "Org" => {
                let node_id = unmarshal_relay_id(id).map_err(ExecError::new)?;
                match self.nodes.orgs.get(&node_id) {
                    Some(OrgByIdLookup::Found(data)) => Ok(Some(FieldValue::Object(Arc::new(
                        OrgObject::from_data_as_viewer(data.clone(), self.viewer_site_admin)
                            .with_nested(self.nested.clone()),
                    )))),
                    Some(OrgByIdLookup::Error(message)) => Err(ExecError::new(message.clone())),
                    None => Ok(None),
                }
            }
            "Repository" => {
                let node_id = unmarshal_relay_id(id).map_err(ExecError::new)?;
                match self.nodes.repositories.get(&node_id) {
                    Some(RepoLookup::Found(data)) => Ok(Some(FieldValue::Object(Arc::new(
                        RepositoryObject::from_data(data.clone())
                            .with_git(self.git.clone())
                            .with_embeddings(self.nested.repo_embeddings.clone())
                            .with_inventory_d(Some(self.inventory.graphql_d.clone()))
                            .with_autoindexing(self.codeintel_autoindexing.clone())
                            .with_codeintel_policies(self.codeintel_policies.clone()),
                    )))),
                    Some(RepoLookup::NotFound) => Ok(Some(FieldValue::Null)),
                    Some(RepoLookup::Blocked { name, reason }) => Err(ExecError::new(format!(
                        "repository {name} has been blocked. reason: {reason}"
                    ))),
                    Some(RepoLookup::Error(message)) => Err(ExecError::new(message.clone())),
                    None => Ok(None),
                }
            }
            _ => Err(ExecError::new("invalid id")),
        }
    }
}

impl ObjectValue for QueryRoot {
    fn type_name(&self) -> &'static str {
        "Query"
    }

    fn field(
        &self,
        field: &str,
        arguments: &Arguments,
        actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            // `schemaResolver.Root` (`graphqlbackend.go:843`) returns the
            // same resolver; the snapshot is cloneable, so replay the whole
            // root object.
            "root" => Ok(Some(FieldValue::Object(Arc::new(self.clone())))),
            // `Query.outOfBandMigrations` (`oobmigrations.go:38-85`).
            "outOfBandMigrations" => match &self.inventory.out_of_band_migrations {
                None => Ok(None),
                Some(Err(message)) => Err(ExecError::new(message.clone())),
                Some(Ok(migrations)) => Ok(Some(FieldValue::List {
                    element_type: Some("OutOfBandMigration"),
                    items: migrations
                        .iter()
                        .cloned()
                        .map(|migration| {
                            FieldValue::Object(Arc::new(
                                crate::out_of_band_migrations_graphql::OutOfBandMigrationObject {
                                    migration,
                                },
                            ))
                        })
                        .collect(),
                })),
            },
            // `Query.evaluateFeatureFlag` (`feature_flags.go:145-153`).
            "evaluateFeatureFlag" => {
                let Some(name) = arguments.string("flagName") else {
                    return Ok(None);
                };
                match self.inventory.feature_flags.evaluate.get(name) {
                    None => Ok(None),
                    Some(Err(message)) => Err(ExecError::new(message.clone())),
                    Some(Ok(value)) => Ok(Some(crate::feature_flags_graphql::evaluate_nullable(
                        *value,
                    ))),
                }
            }
            // `Query.evaluatedFeatureFlags` (`feature_flags.go:155-165`):
            // the values earlier evaluations recorded in the Redis cache for
            // this request actor (`internal/featureflag/cache.go:16-32`).
            "evaluatedFeatureFlags" => {
                let items = self
                    .inventory
                    .feature_flags
                    .evaluated_flags
                    .as_deref()
                    .unwrap_or(&[])
                    .iter()
                    .map(|(name, value)| {
                        FieldValue::Object(Arc::new(
                            crate::feature_flags_graphql::EvaluatedFeatureFlagObject {
                                name: name.clone(),
                                value: *value,
                            },
                        ))
                    })
                    .collect();
                Ok(Some(FieldValue::List {
                    element_type: Some("EvaluatedFeatureFlag"),
                    items,
                }))
            }
            // `Query.organizationFeatureFlagValue` (`feature_flags.go:167-185`).
            "organizationFeatureFlagValue" => {
                let Some(org_id) = arguments
                    .string("orgID")
                    .and_then(|id| unmarshal_relay_id(id).ok())
                else {
                    return Err(ExecError::new("invalid graphql.ID"));
                };
                let Some(flag_name) = arguments.string("flagName") else {
                    return Ok(None);
                };
                match self
                    .inventory
                    .feature_flags
                    .org_value
                    .get(&(org_id, flag_name.to_owned()))
                {
                    None => Ok(None),
                    Some(Err(message)) => Err(ExecError::new(message.clone())),
                    Some(Ok(value)) => Ok(Some(FieldValue::Json(Json::Bool(*value)))),
                }
            }
            // `Query.organizationFeatureFlagOverrides`
            // (`feature_flags.go:187-200`).
            "organizationFeatureFlagOverrides" => match &self.inventory.feature_flags.org_overrides
            {
                None => Ok(None),
                Some(Err(message)) => Err(ExecError::new(message.clone())),
                Some(Ok(overrides)) => Ok(Some(FieldValue::List {
                    element_type: Some("FeatureFlagOverride"),
                    items: overrides
                        .iter()
                        .cloned()
                        .map(|data| {
                            FieldValue::Object(Arc::new(
                                crate::feature_flags_graphql::OrgOverrideObject {
                                    data,
                                    registry: self
                                        .inventory
                                        .feature_flags
                                        .org_override_registry
                                        .clone(),
                                },
                            ))
                        })
                        .collect(),
                })),
            },
            // `Query.surveyResponses` (`survey_responses.go:11-77`).
            "surveyResponses" => match &self.inventory.survey_responses {
                None => Ok(None),
                Some(prefetch) => Ok(Some(FieldValue::Object(Arc::new(
                    crate::survey_responses::SurveyResponseConnectionObject {
                        data: prefetch.clone(),
                    },
                )))),
            },
            // `Query.phabricatorRepo` (`graphqlbackend.go:1003`).
            "phabricatorRepo" => {
                let Some(name) = arguments.string("name").or_else(|| arguments.string("uri"))
                else {
                    return Ok(None);
                };
                match self.inventory.phabricator.get(name) {
                    None => Ok(None),
                    Some(Err(message)) => Err(ExecError::new(message.clone())),
                    Some(Ok(repo)) => Ok(Some(FieldValue::Object(Arc::new(
                        crate::phabricator_graphql::PhabricatorRepoObject { repo: repo.clone() },
                    )))),
                }
            }
            // `Query.packageRepoFilters` (`package_repo_filters.go:215-250`).
            "packageRepoFilters" => {
                let key = format!(
                    "{:?}:{:?}",
                    arguments.string("behaviour"),
                    arguments.string("kind")
                );
                match self.inventory.package_repo_filters.get(&key) {
                    None => Ok(None),
                    Some(Err(message)) => Err(ExecError::new(message.clone())),
                    Some(Ok(filters)) => Ok(Some(FieldValue::List {
                        element_type: Some("PackageFilter"),
                        items: filters
                            .iter()
                            .cloned()
                            .map(|filter| {
                                FieldValue::Object(Arc::new(
                                    crate::package_repo_filters::PackageRepoFilterObject { filter },
                                ))
                            })
                            .collect(),
                    })),
                }
            }
            // `Query.codeHostSyncDue` (`graphqlbackend.go:1026`).
            "codeHostSyncDue" => {
                let raw_ids: Vec<String> = match arguments.get("ids") {
                    Some(InputValue::List(items)) => items
                        .iter()
                        .filter_map(|item| item.as_str().map(str::to_owned))
                        .collect(),
                    _ => Vec::new(),
                };
                let seconds = arguments.i64("seconds").unwrap_or(0) as i32;
                match self.inventory.code_host_sync_due.get(&(raw_ids, seconds)) {
                    None => Ok(None),
                    Some(Err(message)) => Err(ExecError::new(message.clone())),
                    Some(Ok(value)) => Ok(Some(FieldValue::Json(Json::Bool(*value)))),
                }
            }
            // `Query.packageRepoReferences` (`package_repos.go:47-80`).
            "packageRepoReferences" => {
                let args = crate::graphql_inventory_b::package_refs_args(arguments);
                match self.inventory.business_b.package_references.get(&args.key) {
                    None => Ok(None),
                    Some(Err(message)) => Err(ExecError::new(message.clone())),
                    Some(Ok(data)) => Ok(Some(FieldValue::Object(Arc::new(
                        crate::graphql_inventory_b::PackageRepoReferenceConnectionObject {
                            data: data.clone(),
                        },
                    )))),
                }
            }
            // `Query.packageRepoReferencesMatchingFilter`
            // (`package_repo_filters.go:40-94`).
            "packageRepoReferencesMatchingFilter" => {
                let args = crate::graphql_inventory_b::matching_filter_args(arguments);
                match self.inventory.business_b.matching_filter.get(&args.key) {
                    None => Ok(None),
                    Some(Err(message)) => Err(ExecError::new(message.clone())),
                    Some(Ok(crate::graphql_inventory_b::MatchingFilterResult::Packages(data))) => {
                        Ok(Some(FieldValue::Object(Arc::new(
                            crate::graphql_inventory_b::PackageRepoReferenceConnectionObject {
                                data: data.clone(),
                            },
                        ))))
                    }
                    Some(Ok(crate::graphql_inventory_b::MatchingFilterResult::Versions(data))) => {
                        Ok(Some(FieldValue::Object(Arc::new(
                            crate::graphql_inventory_b::PackageRepoReferenceVersionConnectionObject {
                                data: data.clone(),
                            },
                        ))))
                    }
                }
            }
            // `Query.indexedSearchInstances` (`search.go:109-130`).
            "indexedSearchInstances" => match &self.inventory.business_b.indexed_search_instances {
                None => Ok(None),
                Some(Err(message)) => Err(ExecError::new(message.clone())),
                Some(Ok(addresses)) => Ok(Some(FieldValue::Object(Arc::new(
                    crate::graphql_inventory_b::IndexedSearchInstanceConnectionObject {
                        addresses: addresses.clone(),
                    },
                )))),
            },
            // `Query.outboundRequests` (`outbound_requests.go:50-78`).
            "outboundRequests" => match &self.inventory.business_b.outbound_requests {
                None => Ok(None),
                Some(Err(message)) => Err(ExecError::new(message.clone())),
                Some(Ok(items)) => Ok(Some(FieldValue::Object(Arc::new(
                    crate::graphql_inventory_b::OutboundRequestConnectionObject {
                        items: items.clone(),
                        first: arguments.i64("first"),
                    },
                )))),
            },
            // `Query.invitationByToken` (`org_invitations.go:125-164`).
            "invitationByToken" => match &self.inventory.business_b.invitation_by_token {
                None => Ok(None),
                Some(Err(message)) => Err(ExecError::new(message.clone())),
                Some(Ok(Some(data))) => Ok(Some(FieldValue::Object(Arc::new(
                    crate::graphql_inventory_b::OrganizationInvitationObject {
                        data: data.clone(),
                        config: self.inventory.business_b.org_invitation_config.clone(),
                    },
                )))),
                Some(Ok(None)) => Ok(Some(FieldValue::Null)),
            },
            // `Query.webhooks` (`internal/repos/webhooks/resolvers/resolver.go`).
            "webhooks" => {
                let key = crate::graphql_inventory_c::webhooks_key(arguments);
                match self.inventory.business_c.webhooks.get(&key) {
                    None => Ok(None),
                    Some(Err(message)) => Err(ExecError::new(message.clone())),
                    Some(Ok(data)) => Ok(Some(FieldValue::Object(Arc::new(
                        crate::graphql_inventory_d::WebhookConnectionObjectD {
                            data: data.clone(),
                            viewer_site_admin: self.viewer_site_admin,
                            nested: self.nested.clone(),
                        },
                    )))),
                }
            }
            // `Query.repoMeta` (`repository_metadata.go:180-185`): always a
            // resolver (possibly with an empty selection).
            "repoMeta" => {
                if !self.inventory.graphql_d.repo_meta_selected {
                    return Ok(None);
                }
                Ok(Some(FieldValue::Object(Arc::new(
                    crate::graphql_inventory_d::repo_metadata_object(&self.inventory.graphql_d),
                ))))
            }
            // `Query.externalServiceNamespaces` (`external_services.go:535-590`).
            "externalServiceNamespaces" => {
                let key = crate::graphql_inventory_c::external_namespaces_key(arguments);
                match self
                    .inventory
                    .business_c
                    .external_namespaces
                    .get(&key)
                {
                    None => Ok(None),
                    Some(Err(message)) => Err(ExecError::new(message.clone())),
                    Some(Ok(nodes)) => Ok(Some(FieldValue::Object(Arc::new(
                        crate::graphql_inventory_c::ExternalServiceNamespaceConnectionObject {
                            nodes: nodes.clone(),
                        },
                    )))),
                }
            }
            // `Query.externalServiceRepositories` (`external_services.go:597-660`).
            "externalServiceRepositories" => {
                let key = crate::graphql_inventory_c::external_repositories_key(arguments);
                match self
                    .inventory
                    .business_c
                    .external_repositories
                    .get(&key)
                {
                    None => Ok(None),
                    Some(Err(message)) => Err(ExecError::new(message.clone())),
                    Some(Ok(nodes)) => Ok(Some(FieldValue::Object(Arc::new(
                        crate::graphql_inventory_c::ExternalServiceRepositoryConnectionObject {
                            nodes: nodes.clone(),
                        },
                    )))),
                }
            }
            // `Query.renderMarkdown` (`file.go:48-52`).
            "renderMarkdown" => {
                let key = crate::graphql_inventory_c::markdown_key(arguments);
                match self.inventory.business_c.markdown.get(&key) {
                    None => Ok(None),
                    Some(Err(message)) => Err(ExecError::new(message.clone())),
                    Some(Ok(html)) => Ok(Some(FieldValue::Json(Json::Str(html.clone())))),
                }
            }
            // `Query.highlightCode` (`file.go:55-80`).
            "highlightCode" => {
                let key = crate::graphql_inventory_c::highlight_key(arguments);
                match self.inventory.business_c.highlight.get(&key) {
                    None => Ok(None),
                    Some(Err(message)) => Err(ExecError::new(message.clone())),
                    Some(Ok(html)) => Ok(Some(FieldValue::Json(Json::Str(html.clone())))),
                }
            }
            // `schemaResolver.Site` (`site.go:74-76`): no arguments and no
            // access check; `Site.configuration` re-checks admin itself.
            "site" => Ok(Some(FieldValue::Object(Arc::new(SiteObject {
                viewer_site_admin: self.viewer_site_admin,
                nested: self.nested.clone(),
                site: self.site.clone(),
                auth_providers: self.auth_providers.clone(),
            })))),
            // `schemaResolver.StatusMessages` / `RepositoryStats` /
            // `BackgroundJobs` / `Gitservers` (`status_messages.go`,
            // `repository_stats.go`, `background_jobs.go`, `gitserver.go`):
            // these are `Query` fields in the schema, site-admin gated, and
            // prefetched (M9.14c). `None` keeps the pre-slice unported
            // behaviour.
            "statusMessages" => match self
                .nested
                .diagnostics
                .as_ref()
                .and_then(|diagnostics| diagnostics.status_messages.as_ref())
            {
                None => Ok(None),
                Some(Err(message)) => Err(ExecError::new(message.clone())),
                Some(Ok(messages)) => Ok(Some(FieldValue::List {
                    element_type: Some("StatusMessage"),
                    items: messages
                        .iter()
                        .cloned()
                        .map(|message| {
                            FieldValue::Object(Arc::new(
                                crate::diagnostics_graphql::StatusMessageObject { message },
                            ))
                        })
                        .collect(),
                })),
            },
            "repositoryStats" => match self
                .nested
                .diagnostics
                .as_ref()
                .and_then(|diagnostics| diagnostics.repository_stats.clone())
            {
                None => Ok(None),
                Some(data) => Ok(Some(FieldValue::Object(Arc::new(
                    crate::diagnostics_graphql::RepositoryStatsObject { data },
                )))),
            },
            "backgroundJobs" => {
                let args =
                    crate::diagnostics_graphql::BackgroundJobsArgs::from_arguments(arguments);
                match self
                    .nested
                    .diagnostics
                    .as_ref()
                    .and_then(|diagnostics| diagnostics.background_jobs.get(&args))
                {
                    None => Ok(None),
                    Some(Err(message)) => Err(ExecError::new(message.clone())),
                    Some(Ok(data)) => Ok(Some(FieldValue::Object(Arc::new(
                        crate::diagnostics_graphql::BackgroundJobConnectionObject {
                            data: data.clone(),
                        },
                    )))),
                }
            }
            "gitservers" => match self
                .nested
                .diagnostics
                .as_ref()
                .and_then(|diagnostics| diagnostics.gitservers.as_ref())
            {
                None => Ok(None),
                Some(Err(message)) => Err(ExecError::new(message.clone())),
                Some(Ok(servers)) => Ok(Some(FieldValue::Object(Arc::new(
                    crate::diagnostics_graphql::GitserverInstanceConnectionObject {
                        servers: servers.clone(),
                    },
                )))),
            },
            // `schemaResolver.SettingsSubject` (`settings_subject.go:16-23`):
            // `nodeByID` dispatch + `settingsSubjectForNodeAndCheckAccess`;
            // User/Org access failures are reported here.
            "settingsSubject" => {
                let Some(id) = arguments.string("id") else {
                    return Ok(None);
                };
                match self.nested.settings_subjects.get(id) {
                    Some(SettingsSubjectLookup::Found(data)) => {
                        let object = match data {
                            // The site subject's `configuration` is prefetched
                            // separately from `Query.site` (M9.8jh).
                            SettingsSubjectData::Site => SettingsSubjectObject::Site(SiteObject {
                                viewer_site_admin: self.viewer_site_admin,
                                nested: self.nested.clone(),
                                site: self.settings_subject_site.clone(),
                                auth_providers: self.auth_providers.clone(),
                            }),
                            other => SettingsSubjectObject::from_data(
                                other.clone(),
                                self.viewer_site_admin,
                                self.nested.clone(),
                            ),
                        };
                        Ok(Some(FieldValue::Object(Arc::new(object))))
                    }
                    Some(SettingsSubjectLookup::Error(message)) => {
                        Err(ExecError::new(message.clone()))
                    }
                    None => Ok(None),
                }
            }
            // `schemaResolver.SlowRequests` (`slow_requests_tracer.go:63`):
            // site-admin gated. `None` keeps the surface unported.
            "slowRequests" => {
                let after = arguments.string("after").unwrap_or("0");
                if let Some(result) = self.slow_requests_prefetch.get(after) {
                    return match result {
                        Ok(connection) => Ok(Some(FieldValue::Object(
                            Arc::clone(connection) as Arc<dyn ObjectValue>
                        ))),
                        Err(error) => Err(ExecError::new(error.clone())),
                    };
                }
                match &self.slow_requests {
                    Some(surface) => surface
                        .resolve(actor, self.viewer_site_admin, arguments.string("after"))
                        .map(|connection| Some(FieldValue::Object(Arc::new(connection))))
                        .map_err(ExecError::new),
                    None => Ok(None),
                }
            }
            // `CurrentUser` -> `db.Users().GetByCurrentAuthUser`: anonymous
            // actors get `nil`; an authenticated actor gets the prefetched row
            // (or `nil` if it disappeared between authentication and here).
            "currentUser" => match actor {
                // Internal actors have `UID == 0`, so
                // `GetByCurrentAuthUser` finds no user.
                Actor::Anonymous | Actor::Internal => Ok(Some(FieldValue::Null)),
                Actor::User { .. } => match &self.user {
                    Some(user) => Ok(Some(FieldValue::Object(Arc::new(
                        user.clone().with_nested(self.nested.clone()),
                    )))),
                    None => Ok(Some(FieldValue::Null)),
                },
            },
            // `schemaResolver.Repository` -> `RepositoryRedirect`: a missing
            // repository is `null`; a blocked repository is an error.
            "repository" => match repository_arg(arguments) {
                RepositoryArg::Key(key) => match self.repositories.get(&key) {
                    Some(RepoLookup::Found(data)) => {
                        let mut object = RepositoryObject::from_data(data.clone())
                            .with_git(self.git.clone())
                            .with_embeddings(self.nested.repo_embeddings.clone())
                            .with_inventory_d(Some(self.inventory.graphql_d.clone()))
                            .with_codeintel_policies(self.codeintel_policies.clone());
                        // `Repository.batchChanges` (M9.24c): the batch-changes
                        // resolver is attached to the object, like the
                        // `User`/`Org` root fields which get it through
                        // `NestedPrefetch::batches`.
                        if let Some(batches) = self.batches.clone() {
                            object = object.with_batches(batches);
                        }
                        object = object.with_autoindexing(self.codeintel_autoindexing.clone());
                        Ok(Some(FieldValue::Object(Arc::new(object))))
                    }
                    Some(RepoLookup::NotFound) => Ok(Some(FieldValue::Null)),
                    Some(RepoLookup::Blocked { name, reason }) => Err(ExecError::new(format!(
                        "repository {name} has been blocked. reason: {reason}"
                    ))),
                    Some(RepoLookup::Error(message)) => Err(ExecError::new(message.clone())),
                    // No prefetch was performed for this request (e.g. the
                    // transport-only resolver); report the field as unported.
                    None => Ok(None),
                },
                RepositoryArg::CloneUrl(clone_url) => {
                    match self.repositories.get(&RepoKey::CloneUrl(clone_url)) {
                        Some(RepoLookup::Found(data)) => {
                            let mut object = RepositoryObject::from_data(data.clone())
                                .with_git(self.git.clone())
                                .with_embeddings(self.nested.repo_embeddings.clone())
                                .with_inventory_d(Some(self.inventory.graphql_d.clone()))
                                .with_codeintel_policies(self.codeintel_policies.clone());
                            if let Some(batches) = self.batches.clone() {
                                object = object.with_batches(batches);
                            }
                            object = object.with_autoindexing(self.codeintel_autoindexing.clone());
                            Ok(Some(FieldValue::Object(Arc::new(object))))
                        }
                        Some(RepoLookup::NotFound) => Ok(Some(FieldValue::Null)),
                        Some(RepoLookup::Blocked { name, reason }) => Err(ExecError::new(format!(
                            "repository {name} has been blocked. reason: {reason}"
                        ))),
                        Some(RepoLookup::Error(message)) => Err(ExecError::new(message.clone())),
                        // No prefetch (transport-only resolver).
                        None => Ok(None),
                    }
                }
                RepositoryArg::Missing => Err(ExecError::new("neither name nor cloneURL given")),
            },
            // `schemaResolver.RepositoryRedirect`
            // (`graphqlbackend.go:961-1001`): `hashedName` misses are errors,
            // `name` misses are `null`; the `Redirect` union member is
            // unreachable in this snapshot (`ErrRepoSeeOther` is never
            // constructed).
            "repositoryRedirect" => match repository_redirect_arg(arguments) {
                RepositoryRedirectArg::Key(key) => match self.repository_redirects.get(&key) {
                    Some(RepoLookup::Found(data)) => Ok(Some(FieldValue::Object(Arc::new(
                        RepositoryObject::from_data(data.clone())
                            .with_git(self.git.clone())
                            .with_embeddings(self.nested.repo_embeddings.clone())
                            .with_inventory_d(Some(self.inventory.graphql_d.clone()))
                            .with_autoindexing(self.codeintel_autoindexing.clone())
                            .with_codeintel_policies(self.codeintel_policies.clone()),
                    )))),
                    Some(RepoLookup::NotFound) => Ok(Some(FieldValue::Null)),
                    Some(RepoLookup::Blocked { name, reason }) => Err(ExecError::new(format!(
                        "repository {name} has been blocked. reason: {reason}"
                    ))),
                    Some(RepoLookup::Error(message)) => Err(ExecError::new(message.clone())),
                    // No prefetch was performed for this request.
                    None => Ok(None),
                },
                RepositoryRedirectArg::CloneUrl(clone_url) => {
                    match self
                        .repository_redirects
                        .get(&RepositoryRedirectKey::CloneUrl(clone_url))
                    {
                        Some(RepoLookup::Found(data)) => Ok(Some(FieldValue::Object(Arc::new(
                            RepositoryObject::from_data(data.clone())
                                .with_git(self.git.clone())
                                .with_embeddings(self.nested.repo_embeddings.clone())
                                .with_inventory_d(Some(self.inventory.graphql_d.clone()))
                            .with_autoindexing(self.codeintel_autoindexing.clone())
                                .with_codeintel_policies(self.codeintel_policies.clone()),
                        )))),
                        Some(RepoLookup::NotFound) => Ok(Some(FieldValue::Null)),
                        Some(RepoLookup::Blocked { name, reason }) => Err(ExecError::new(format!(
                            "repository {name} has been blocked. reason: {reason}"
                        ))),
                        Some(RepoLookup::Error(message)) => Err(ExecError::new(message.clone())),
                        None => Ok(None),
                    }
                }
                RepositoryRedirectArg::Missing => {
                    Err(ExecError::new("neither name nor cloneURL given"))
                }
            },
            // `schemaResolver.Repositories` (`repositories.go:105-292`): the
            // connection's pages and total are computed asynchronously and
            // prefetched.
            "repositories" => {
                let args = crate::repositories::repositories_args(arguments);
                match self.repositories_connections.get(&args) {
                    Some(crate::repositories::RepositoriesConnectionLookup::Found(data)) => {
                        Ok(Some(FieldValue::Object(Arc::new(
                            crate::repositories::RepositoriesConnectionObject::from_data(
                                data.clone(),
                            )
                            .with_embeddings(self.nested.repo_embeddings.clone()),
                        ))))
                    }
                    Some(crate::repositories::RepositoriesConnectionLookup::Error(message)) => {
                        Err(ExecError::new(message.clone()))
                    }
                    None => Ok(None),
                }
            }
            // `schemaResolver.User` (`user.go:30-73`): username, then
            // databaseID, then verified email; not-found is `null`.
            "user" => match user_arg(arguments) {
                UserArg::Key(key) => match self.users.get(&key) {
                    Some(UserLookup::Found(data)) => Ok(Some(FieldValue::Object(Arc::new(
                        UserObject::from_data_as_viewer(data.clone(), self.viewer_site_admin)
                            .with_nested(self.nested.clone()),
                    )))),
                    Some(UserLookup::NotFound) => Ok(Some(FieldValue::Null)),
                    Some(UserLookup::Error(message)) => Err(ExecError::new(message.clone())),
                    // No prefetch was performed for this request.
                    None => Ok(None),
                },
                UserArg::Missing => Err(ExecError::new(
                    "must specify either username or email to look up a user",
                )),
            },
            // `schemaResolver.Users` (`users.go:25-52`): the connection is
            // computed asynchronously and prefetched.
            "users" => {
                let args = users_args(arguments);
                match self.users_connections.get(&args) {
                    Some(UsersConnectionLookup::Found(data)) => {
                        Ok(Some(FieldValue::Object(Arc::new(
                            UsersConnectionObject::from_data(data.clone(), self.viewer_site_admin)
                                .with_nested(self.nested.clone()),
                        ))))
                    }
                    Some(UsersConnectionLookup::Error(message)) => {
                        Err(ExecError::new(message.clone()))
                    }
                    None => Ok(None),
                }
            }
            // `schemaResolver.Organization` (`org.go:24-38`): not-found is a
            // resolver error (`org not found: name <name>`), not `null`.
            "organization" => {
                let Some(name) = arguments.string("name") else {
                    return Ok(None);
                };
                match self.organizations.get(name) {
                    Some(OrganizationLookup::Found(data)) => {
                        Ok(Some(FieldValue::Object(Arc::new(
                            OrgObject::from_data_as_viewer(data.clone(), self.viewer_site_admin)
                                .with_nested(self.nested.clone()),
                        ))))
                    }
                    Some(OrganizationLookup::Error(message)) => {
                        Err(ExecError::new(message.clone()))
                    }
                    None => Ok(None),
                }
            }
            // `schemaResolver.Organizations` (`orgs.go:15-30`): the
            // site-admin check lives in `nodes`/`totalCount`.
            "organizations" => {
                let args = organizations_args(arguments);
                match self.organizations_connections.get(&args) {
                    Some(OrganizationsConnectionLookup::Found(data)) => {
                        Ok(Some(FieldValue::Object(Arc::new(
                            OrgConnectionObject::from_data(data.clone(), self.viewer_site_admin)
                                .with_nested(self.nested.clone()),
                        ))))
                    }
                    Some(OrganizationsConnectionLookup::Error(message)) => {
                        Err(ExecError::new(message.clone()))
                    }
                    None => Ok(None),
                }
            }
            // `schemaResolver.Roles` / `Permissions` (M9.13b; site-admin gated
            // at prefetch time).
            "roles" => crate::rbac_graphql::root_role_connection(
                &self.rbac,
                &crate::rbac_graphql::role_connection_args(arguments),
                self.viewer_site_admin,
                self.nested.clone(),
            ),
            "permissions" => crate::rbac_graphql::root_permission_connection(
                &self.rbac,
                &crate::rbac_graphql::role_connection_args(arguments),
                self.viewer_site_admin,
                self.nested.clone(),
            ),
            // `schemaResolver.ExecutorSecrets` (M9.13b).
            "executorSecrets" => {
                let args =
                    crate::executor_secrets_graphql::ExecutorSecretsArgs::from_arguments(arguments);
                match args {
                    Ok(args) => match self.executor_secret_roots.global.get(&args) {
                        Some(Ok(data)) => Ok(Some(
                            crate::executor_secrets_graphql::executor_secret_connection_value(
                                data,
                                args.scope,
                                self.viewer_site_admin,
                                self.nested.clone(),
                            ),
                        )),
                        Some(Err(message)) => Err(ExecError::new(message.clone())),
                        None => Ok(None),
                    },
                    // The baseline validates the cursor inside the resolver;
                    // a malformed `after` is surfaced as an error.
                    Err(message) => Err(ExecError::new(message)),
                }
            }
            // `schemaResolver.Node` (`node.go:21-39`): dispatch on the Relay
            // kind. Ported kinds: User/Org/Repository/ExternalService/
            // WebhookLog; unknown kinds are `invalid id` (the baseline only
            // knows its registered kinds).
            "node" => {
                let Some(id) = arguments.string("id") else {
                    return Ok(None);
                };
                let Some(kind) = relay_kind(id) else {
                    return Err(ExecError::new("invalid id"));
                };
                // The int64 kinds (`relay.UnmarshalSpec` into int64) and the
                // int32 kinds are parsed per branch: `UnmarshalExternalServiceID`
                // validates the Relay kind while the others do not.
                match kind.as_str() {
                    // `NodeResolvers()["SearchContext"]` is only registered
                    // when the optional resolver is present; otherwise the
                    // kind is unknown and `Node` reports `invalid id`.
                    "SearchContext" if self.search_contexts.is_some() => {
                        let db = self
                            .search_contexts
                            .as_ref()
                            .expect("checked search contexts database");
                        let ctx = search_contexts_request_context(actor);
                        let dispatch = SearchContextsResolverDispatch::new(db.as_ref());
                        match dispatch.node_by_id(&ctx, "SearchContext", id) {
                            Ok(Some(resolver)) => Ok(Some(FieldValue::Object(
                                search_context_snapshot(&ctx, &resolver),
                            ))),
                            Ok(None) => Ok(None),
                            Err(error) => Err(ExecError::new(error.to_string())),
                        }
                    }
                    "ExternalService" => match self.nodes.external_services.get(id) {
                        Some(Ok(data)) => Ok(Some(FieldValue::Object(Arc::new(
                            crate::external_services::ExternalServiceObject::from_data(
                                data.clone(),
                                self.viewer_site_admin,
                                self.nested.clone(),
                            ),
                        )))),
                        Some(Err(message)) => Err(ExecError::new(message.clone())),
                        None => Ok(None),
                    },
                    "WebhookLog" => match self.nodes.webhook_logs.get(id) {
                        Some(Ok(data)) => Ok(Some(FieldValue::Object(Arc::new(
                            crate::webhook_logs::WebhookLogObject::from_data(
                                data.clone(),
                                self.viewer_site_admin,
                                self.nested.clone(),
                            ),
                        )))),
                        Some(Err(message)) => Err(ExecError::new(message.clone())),
                        None => Ok(None),
                    },
                    // `NodeResolvers()["PreciseIndex"]` → `PreciseIndexByID`
                    // (M9.28a). The uploads store bridges with `block_in_place`,
                    // so the synchronous call is safe inside the executor.
                    "PreciseIndex" if self.codeintel_uploads.is_some() => {
                        crate::codeintel_uploads_graphql::precise_index_by_id(
                            self.codeintel_uploads
                                .as_ref()
                                .expect("checked codeintel uploads"),
                            id,
                            actor,
                        )
                    }
                    // `NodeResolvers()["Notebook"]` → `NotebookByID` (M9.29a).
                    "Notebook" if self.notebooks.is_some() => {
                        crate::notebooks_graphql::node_field(
                            self.notebooks.as_ref().expect("checked notebooks"),
                            id,
                            actor,
                        )
                    }
                    // `NodeResolvers()["GitCommit"]` → `gitCommitByID`
                    // (`git_commit.go:34-39`): `repositoryByID` then
                    // `repo.Commit(Rev: oid)`. A missing revision is `null`.
                    "GitCommit" => {
                        let spec = crate::git_graphql::parse_git_commit_relay_id(id)
                            .map_err(ExecError::new)?;
                        let repo_id =
                            unmarshal_relay_id(&spec.repository).map_err(ExecError::new)?;
                        let data = match self.nodes.repositories.get(&repo_id) {
                            Some(RepoLookup::Found(data)) => data.clone(),
                            Some(RepoLookup::NotFound) => return Ok(Some(FieldValue::Null)),
                            Some(RepoLookup::Blocked { name, reason }) => {
                                return Err(ExecError::new(format!(
                                    "repository {name} has been blocked. reason: {reason}"
                                )))
                            }
                            Some(RepoLookup::Error(message)) => {
                                return Err(ExecError::new(message.clone()))
                            }
                            None => return Ok(None),
                        };
                        let Some(git) = &self.git else {
                            return Err(ExecError::new(
                                "GitCommit requires the gitserver GitSource (not wired for this request)",
                            ));
                        };
                        match git.revisions.get(&(repo_id, spec.commit.clone())) {
                            Some(crate::git_graphql::RevisionLookup::Found(oid)) => {
                                let (commit, commit_error) = git.commit_data(repo_id, oid);
                                Ok(Some(FieldValue::Object(Arc::new(
                                    crate::git_graphql::GitCommitResolverObject {
                                        repo: MinimalRepo {
                                            id: data.id,
                                            name: data.name.clone(),
                                        },
                                        oid: oid.clone(),
                                        // `CommitFromID` uses `args.Rev`, the
                                        // relay's "c" member.
                                        input_rev: Some(spec.commit),
                                        commit,
                                        commit_error,
                                        git: self.git.clone(),
                                    },
                                ))))
                            }
                            Some(crate::git_graphql::RevisionLookup::NotFound) => {
                                Ok(Some(FieldValue::Null))
                            }
                            Some(crate::git_graphql::RevisionLookup::Error(message)) => {
                                Err(ExecError::new(message.clone()))
                            }
                            None => Ok(None),
                        }
                    }
                    // `NodeResolvers()["GitRef"]` → `gitRefByID`
                    // (`git_ref.go:58-69`): a lazy `GitRefResolver`.
                    "GitRef" => {
                        let spec = crate::git_graphql::parse_git_ref_relay_id(id)
                            .map_err(ExecError::new)?;
                        let repo_id =
                            unmarshal_relay_id(&spec.repository).map_err(ExecError::new)?;
                        match self.nodes.repositories.get(&repo_id) {
                            Some(RepoLookup::Found(data)) => Ok(Some(FieldValue::Object(
                                Arc::new(crate::search_graphql::GitRefObject::new(
                                    MinimalRepo {
                                        id: data.id,
                                        name: data.name.clone(),
                                    },
                                    spec.rev,
                                    self.git.clone(),
                                )),
                            ))),
                            Some(RepoLookup::NotFound) => Ok(Some(FieldValue::Null)),
                            Some(RepoLookup::Blocked { name, reason }) => Err(ExecError::new(
                                format!("repository {name} has been blocked. reason: {reason}"),
                            )),
                            Some(RepoLookup::Error(message)) => {
                                Err(ExecError::new(message.clone()))
                            }
                            None => Ok(None),
                        }
                    }
                    // `NodeResolvers()["ExternalAccount"]` →
                    // `externalAccountByID` (M9.8ka): the Relay kind is not
                    // validated, the row is fetched before the permission
                    // check.
                    "ExternalAccount" => {
                        let node_id = unmarshal_relay_id(id).map_err(ExecError::new)?;
                        match self.nodes.external_accounts.get(&node_id) {
                            Some(
                                crate::external_accounts_graphql::ExternalAccountNodeLookup::Found(
                                    data,
                                ),
                            ) => Ok(Some(FieldValue::Object(Arc::new(
                                crate::external_accounts_graphql::ExternalAccountObject::from_data(
                                    data.clone(),
                                    None,
                                    self.viewer_site_admin,
                                    self.nested.clone(),
                                ),
                            )))),
                            Some(
                                crate::external_accounts_graphql::ExternalAccountNodeLookup::Error(
                                    message,
                                ),
                            ) => Err(ExecError::new(message.clone())),
                            None => Ok(None),
                        }
                    }
                    // `NodeResolvers()["Role"]` / `["Permission"]` (M9.13b).
                    "Role" | "Permission" => crate::rbac_graphql::rbac_node_value(
                        &self.nodes.rbac_nodes,
                        id,
                        self.viewer_site_admin,
                        self.nested.clone(),
                    ),
                    // `NodeResolvers()["ExecutorSecret"]` (M9.13b).
                    "ExecutorSecret" => {
                        crate::executor_secrets_graphql::executor_secret_node_value(
                            &self.nodes.executor_secrets,
                            id,
                            self.viewer_site_admin,
                            self.nested.clone(),
                        )
                    }
                    // `NodeResolvers()["ExecutorSecretAccessLog"]` (M9.13b).
                    "ExecutorSecretAccessLog" => {
                        crate::executor_secrets_graphql::executor_secret_access_log_node_value(
                            &self.nodes.executor_secret_access_logs,
                            id,
                            self.viewer_site_admin,
                            self.nested.clone(),
                        )
                    }
                    // `NodeResolvers()["SavedSearch"]` (M9.13a): registered
                    // only when the optional resolver is present.
                    "SavedSearch" if self.saved_searches.is_some() => {
                        crate::saved_searches_graphql::saved_search_node(
                            self.saved_searches
                                .as_ref()
                                .expect("checked saved searches"),
                            id,
                            actor,
                        )
                    }
                    // `NodeResolvers()["CodeMonitor"]` (M9.13a).
                    "CodeMonitor" if self.code_monitors.is_some() => {
                        crate::code_monitors_graphql::monitor_node(
                            self.code_monitors
                                .as_ref()
                                .expect("checked code monitors")
                                .as_ref(),
                            actor,
                            id,
                        )
                    }
                    // `NodeResolvers()["BatchChange"]` / `["Changeset"]`
                    // (M9.24c): registered only when the optional resolver is
                    // present.
                    _ if self.batches.is_some()
                        && crate::batches_graphql::is_node_kind(kind.as_str()) =>
                    {
                        crate::batches_graphql::node_field(
                            self.batches.as_ref().expect("checked batches"),
                            kind.as_str(),
                            id,
                            actor,
                        )
                    }
                    // `NodeResolvers()["BatchSpec"]` / `["ChangesetSpec"]` /
                    // `["BatchSpecWorkspace"]` (M9.26a).
                    _ if self.batches.is_some()
                        && crate::batches_specs_graphql::is_node_kind(kind.as_str()) =>
                    {
                        crate::batches_specs_graphql::node_field(
                            self.batches.as_ref().expect("checked batches"),
                            kind.as_str(),
                            id,
                            actor,
                        )
                    }
                    // `NodeResolvers()["GitHubApp"]` (M9.25c): registered only
                    // when the optional resolver is present.
                    _ if self.github_apps.is_some()
                        && crate::github_apps_graphql::is_node_kind(kind.as_str()) =>
                    {
                        crate::github_apps_graphql::node_field(
                            self.github_apps.as_ref().expect("checked github apps"),
                            kind.as_str(),
                            id,
                            actor,
                        )
                    }
                    // `NodeResolvers()["Team"]` (M9.27a).
                    _ if self.teams.is_some()
                        && crate::teams_graphql::is_node_kind(kind.as_str()) =>
                    {
                        crate::teams_graphql::node_field(
                            self.teams.as_ref().expect("checked teams"),
                            id,
                            self.viewer_site_admin,
                        )
                    }
                    // `NodeResolvers()["OutboundWebhook"]` (M9.27b): registered
                    // only when the optional resolver is present.
                    _ if self.outbound_webhooks.is_some()
                        && crate::outbound_webhooks_graphql::is_node_kind(kind.as_str()) =>
                    {
                        crate::outbound_webhooks_graphql::node_field(
                            self.outbound_webhooks
                                .as_ref()
                                .expect("checked outbound webhooks"),
                            kind.as_str(),
                            id,
                            actor,
                        )
                    }
                    // `NodeResolvers()["CodeIntelligenceConfigurationPolicy"]`
                    // (M9.28c).
                    _ if self.codeintel_policies.is_some()
                        && crate::codeintel_policies_graphql::is_node_kind(kind.as_str()) =>
                    {
                        crate::codeintel_policies_graphql::node_field(
                            self.codeintel_policies
                                .as_ref()
                                .expect("checked codeintel policies"),
                            id,
                            actor,
                        )
                    }
                    _ => self.node_int32(kind.as_str(), id),
                }
            }
            // `schemaResolver.ParseSearchQuery` (`parse_search_query.go:49-61`):
            // synchronous, no database. `PARSE_TREE` is ported; `JOB_TREE`
            // reports an explicit "not ported" error (see the module docs).
            "parseSearchQuery" => {
                let args = crate::parse_search_query::ParseSearchQueryArgs {
                    query: arguments.string("query").unwrap_or_default().to_owned(),
                    pattern_type: arguments
                        .string("patternType")
                        .unwrap_or("standard")
                        .to_owned(),
                    output_phase: arguments
                        .string("outputPhase")
                        .unwrap_or(crate::parse_search_query::PARSE_TREE)
                        .to_owned(),
                    output_format: arguments
                        .string("outputFormat")
                        .unwrap_or(crate::parse_search_query::JSON)
                        .to_owned(),
                    output_verbosity: arguments
                        .string("outputVerbosity")
                        .unwrap_or(crate::parse_search_query::BASIC)
                        .to_owned(),
                };
                match crate::parse_search_query::parse_search_query(&args) {
                    Ok(json) => Ok(Some(FieldValue::Json(Json::Str(json)))),
                    Err(message) => Err(ExecError::new(message)),
                }
            }
            // `schemaResolver.Namespace` (`namespaces.go:24-29`) and
            // `NamespaceByName` (`:113-133`): the prefetched lookup carries
            // the concrete User/Org object, `null`, or the lookup error.
            "namespace" => {
                let Some(id) = arguments.string("id") else {
                    return Ok(None);
                };
                namespace_value(self, self.namespaces.by_id.get(id))
            }
            "namespaceByName" => {
                let Some(name) = arguments.string("name") else {
                    return Ok(None);
                };
                namespace_value(self, self.namespaces.by_name.get(name))
            }
            // `schemaResolver.TemporarySettings` (`temporary_settings.go:18-28`):
            // the authenticated user's stored jsonb text, `{}` when absent.
            "temporarySettings" => match &self.temporary_settings {
                Some(Ok(contents)) => Ok(Some(FieldValue::Object(Arc::new(
                    crate::temporary_settings::TemporarySettingsObject {
                        contents: contents.clone(),
                    },
                )))),
                Some(Err(message)) => Err(ExecError::new(message.clone())),
                None => Ok(None),
            },
            // `schemaResolver.ClientConfiguration`
            // (`client_configuration.go:36-95`): read-only; the prefetched
            // result carries either the details or the database/config error.
            "clientConfiguration" => match &self.client_configuration {
                Some(crate::client_configuration::ClientConfigurationLookup::Found(details)) => {
                    Ok(Some(FieldValue::Object(Arc::new(
                        crate::client_configuration::ClientConfigurationObject::new(
                            details.clone(),
                        ),
                    ))))
                }
                Some(crate::client_configuration::ClientConfigurationLookup::Error(message)) => {
                    Err(ExecError::new(message.clone()))
                }
                None => Ok(None),
            },
            // `schemaResolver.ExternalServices`
            // (`external_services.go:376-455`): site-admin only; the
            // prefetched lookup carries the connection or its error.
            "externalServices" => {
                let args = crate::external_services::external_services_args(arguments);
                match self.external_services.get(&args) {
                    Some(crate::external_services::ExternalServicesConnectionLookup::Found(
                        data,
                    )) => Ok(Some(FieldValue::Object(Arc::new(
                        crate::external_services::ExternalServicesConnectionObject::from_data(
                            data.clone(),
                            self.viewer_site_admin,
                            self.nested.clone(),
                        ),
                    )))),
                    Some(crate::external_services::ExternalServicesConnectionLookup::Error(
                        message,
                    )) => Err(ExecError::new(message.clone())),
                    None => Ok(None),
                }
            }
            // `schemaResolver.WebhookLogs` (`webhook_logs.go:133-141`):
            // site-admin only; the prefetched lookup carries the connection,
            // its filters or the authorization/argument error.
            "webhookLogs" => {
                let args = crate::webhook_logs::global_webhook_logs_args(arguments);
                match self.webhook_logs.get(&args) {
                    Some(crate::webhook_logs::WebhookLogsConnectionLookup::Found(data)) => {
                        Ok(Some(FieldValue::Object(Arc::new(
                            crate::webhook_logs::WebhookLogConnectionObject::from_data(
                                data.clone(),
                                self.viewer_site_admin,
                                self.nested.clone(),
                            ),
                        ))))
                    }
                    Some(crate::webhook_logs::WebhookLogsConnectionLookup::Error(message)) => {
                        Err(ExecError::new(message.clone()))
                    }
                    None => Ok(None),
                }
            }
            // `schemaResolver.ViewerSettings` (`settings_cascade.go:75-93`):
            // the current user's cascade, or the site cascade when anonymous
            // (or the row disappeared).
            "viewerSettings" | "viewerConfiguration" => {
                let key = match (actor, &self.user) {
                    (Actor::User { uid }, Some(user)) if user.data.id == uid => {
                        CascadeKey::User(uid)
                    }
                    _ => CascadeKey::Site,
                };
                cascade_field(
                    &self.nested,
                    key,
                    self.viewer_site_admin,
                    field == "viewerConfiguration",
                )
            }
            // `schemaResolver.FeatureFlags` / `FeatureFlag`
            // (`feature_flags.go:202-226`): both are site-admin gated; the
            // prefetch carries the gate result and the database rows.
            "featureFlags" => match &self.feature_flags.flags {
                Some(FeatureFlagsLookup::Found(names)) => Ok(Some(FieldValue::List {
                    element_type: Some("FeatureFlag"),
                    items: names
                        .iter()
                        .filter_map(|name| self.feature_flags.registry.flags.get(name))
                        .map(|data| {
                            FieldValue::Object(Arc::new(FeatureFlagObject {
                                data: data.clone(),
                                registry: self.feature_flags.registry.clone(),
                            }))
                        })
                        .collect(),
                })),
                Some(FeatureFlagsLookup::Error(message)) => Err(ExecError::new(message.clone())),
                None => Ok(None),
            },
            "featureFlag" => {
                let Some(name) = arguments.string("name") else {
                    return Ok(None);
                };
                match self.feature_flags.flag.get(name) {
                    Some(FeatureFlagLookup::Found(flag_name)) => {
                        match self.feature_flags.registry.flags.get(flag_name) {
                            Some(data) => {
                                Ok(Some(FieldValue::Object(Arc::new(FeatureFlagObject {
                                    data: data.clone(),
                                    registry: self.feature_flags.registry.clone(),
                                }))))
                            }
                            None => Err(ExecError::new("sql: no rows in result set")),
                        }
                    }
                    Some(FeatureFlagLookup::Error(message)) => Err(ExecError::new(message.clone())),
                    None => Ok(None),
                }
            }
            // `CodeIntelResolver.RankingSummary` (M9.8ic): the async resolver
            // result was prefetched in `SchemaGraphqlDeps::exec`; the owned
            // snapshot answers the object fields.
            "rankingSummary" => match &self.ranking_summary {
                Some(Ok(summary)) => Ok(Some(FieldValue::Object(Arc::new(summary.clone())))),
                Some(Err(message)) => Err(ExecError::new(message.clone())),
                None => Ok(None),
            },
            // `schemaResolver.Search` (M9.8ie): the async plan/execute result
            // was prefetched in `SchemaGraphqlDeps::exec`.
            "search" => match &self.search {
                Some(root) => crate::search_graphql::search_field(root, self.git.clone()),
                None => Ok(None),
            },
            // `SearchContextsResolver` root fields (`search_contexts.go:20-39`):
            // dispatched through the M9.8hu registration layer (M9.8hz).
            "searchContexts"
            | "searchContextBySpec"
            | "isSearchContextAvailable"
            | "defaultSearchContext" => dispatch_search_context_field(
                self.search_contexts.as_ref(),
                SearchContextsOperation::Query,
                field,
                arguments,
                actor,
            ),
            // `SavedSearchesResolver` root fields (M9.13a): only present when
            // the optional resolver was registered.
            _ if self.saved_searches.is_some()
                && crate::saved_searches_graphql::is_root_field(field) =>
            {
                crate::saved_searches_graphql::root_field(
                    self.saved_searches
                        .as_ref()
                        .expect("checked saved searches"),
                    field,
                    arguments,
                    actor,
                )
            }
            // `Query.batchSpecs` (M9.26a).
            _ if self.batches.is_some()
                && crate::batches_specs_graphql::is_root_field(field) =>
            {
                crate::batches_specs_graphql::root_field(
                    self.batches.as_ref().expect("checked batches"),
                    field,
                    arguments,
                    actor,
                )
            }
            // `BatchChangesResolver` root fields (`Query.batchChanges` /
            // `Query.batchChange`, M9.24c).
            _ if self.batches.is_some()
                && crate::batches_graphql::is_root_field(field) =>
            {
                crate::batches_graphql::root_field(
                    self.batches.as_ref().expect("checked batches"),
                    field,
                    arguments,
                    actor,
                )
            }
            // `GitHubAppsResolver` root fields (`Query.gitHubApps` /
            // `Query.gitHubApp` / `Query.gitHubAppByAppID`, M9.25c).
            _ if self.github_apps.is_some()
                && crate::github_apps_graphql::is_root_field(field) =>
            {
                crate::github_apps_graphql::root_field(
                    self.github_apps.as_ref().expect("checked github apps"),
                    field,
                    arguments,
                    actor,
                )
            }
            // `UploadsServiceResolver` root field (`Query.preciseIndexes`,
            // M9.27d).
            _ if self.codeintel_uploads.is_some()
                && crate::codeintel_uploads_graphql::is_root_field(field) =>
            {
                crate::codeintel_uploads_graphql::root_field(
                    self.codeintel_uploads
                        .as_ref()
                        .expect("checked codeintel uploads"),
                    field,
                    arguments,
                    actor,
                )
            }
            // Code Intel autoindexing root fields
            // (`Query.codeIntelligenceInferenceScript` /
            // `Query.inferAutoIndexJobsForRepo`, M9.28b).
            _ if self.codeintel_autoindexing.is_some()
                && crate::codeintel_autoindexing_graphql::is_root_field(field) =>
            {
                crate::codeintel_autoindexing_graphql::root_field(
                    self.codeintel_autoindexing
                        .as_ref()
                        .expect("checked codeintel autoindexing"),
                    field,
                    arguments,
                    actor,
                )
            }
            // Outbound webhook root fields (`Query.outboundWebhooks` /
            // `Query.outboundWebhookEventTypes`, M9.27b).
            _ if self.outbound_webhooks.is_some()
                && crate::outbound_webhooks_graphql::is_root_field(field) =>
            {
                crate::outbound_webhooks_graphql::root_field(
                    self.outbound_webhooks
                        .as_ref()
                        .expect("checked outbound webhooks"),
                    field,
                    arguments,
                    actor,
                )
            }
            // Configuration-policy root fields
            // (`Query.codeIntelligenceConfigurationPolicies`,
            // `Query.previewRepositoryFilter`, M9.28c).
            _ if self.codeintel_policies.is_some()
                && crate::codeintel_policies_graphql::is_root_field(field) =>
            {
                crate::codeintel_policies_graphql::root_field(
                    self.codeintel_policies
                        .as_ref()
                        .expect("checked codeintel policies"),
                    field,
                    arguments,
                    actor,
                )
            }
            // Notebooks root fields (`Query.notebooks`, M9.29a).
            _ if self.notebooks.is_some()
                && crate::notebooks_graphql::is_root_field(field) =>
            {
                crate::notebooks_graphql::root_field(
                    self.notebooks.as_ref().expect("checked notebooks"),
                    field,
                    arguments,
                    actor,
                )
            }
            // Code Insights read-only root fields (`Query.insightViews`,
            // `Query.insightsDashboards`, M9.29b).
            _ if self.insights.is_some() && crate::insights_graphql::is_root_field(field) => {
                crate::insights_graphql::root_field(
                    self.insights.as_ref().expect("checked insights"),
                    field,
                    arguments,
                    actor,
                )
            }
            // `CodeMonitorsResolver` root fields (`Query.monitors`, M9.13a).
            _ if self.code_monitors.is_some()
                && crate::code_monitors_graphql::is_root_field(field) =>
            {
                crate::code_monitors_graphql::root_field(
                    self.code_monitors.as_ref().expect("checked code monitors"),
                    field,
                    arguments,
                    actor,
                )
            }
            // `schemaResolver.Team` / `Teams` (M9.27a).
            _ if self.teams.is_some() && crate::teams_graphql::is_root_field(field) => {
                crate::teams_graphql::root_field(
                    self.teams.as_ref().expect("checked teams"),
                    field,
                    arguments,
                    self.viewer_site_admin,
                )
            }
            _ => Ok(None),
        }
    }
}

/// `auth.ErrMustBeSiteAdminOrSameUser`. `CheckSiteAdminOrSameUserFromActor`
/// collapses every failure (unauthenticated, non-admin, DB error) to this
/// message (`internal/auth/site_admin.go:127-136`).
pub(crate) const MUST_BE_SITE_ADMIN_OR_SAME_USER: &str =
    "must be authenticated as the authorized user or site admin";

/// `UserResolver` (`cmd/frontend/graphqlbackend/user.go:122-168`), limited to
/// the fields backed by the user row and viewer authorization.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UserObject {
    data: UserData,
    /// The viewer's `users.site_admin` (`false` for anonymous actors and for
    /// objects built without a viewer, e.g. transport tests).
    viewer_site_admin: bool,
    /// Nested resolver data prefetched for this request (initially empty for
    /// objects built by unit tests/transport tests).
    nested: Arc<NestedPrefetch>,
    /// `User.teams` handle (M9.27a). Also available through
    /// `nested.teams` for objects built by the request prefetch.
    teams: Option<crate::teams_graphql::TeamsHandleWrapper>,
}

impl UserObject {
    pub fn from_data(data: UserData) -> Self {
        Self {
            data,
            viewer_site_admin: false,
            nested: Arc::new(NestedPrefetch::default()),
            teams: None,
        }
    }

    pub fn from_data_as_viewer(data: UserData, viewer_site_admin: bool) -> Self {
        Self {
            data,
            viewer_site_admin,
            nested: Arc::new(NestedPrefetch::default()),
            teams: None,
        }
    }

    /// Attach the Teams handle so `User.teams` resolves (M9.27a).
    pub fn with_teams(
        mut self,
        teams: Option<crate::teams_graphql::TeamsHandle>,
    ) -> Self {
        self.teams = teams.map(crate::teams_graphql::TeamsHandleWrapper);
        self
    }

    /// Attach the request's nested prefetch data (see [`prefetch_nested`]).
    pub fn with_nested(mut self, nested: Arc<NestedPrefetch>) -> Self {
        self.nested = nested;
        self
    }

    pub fn user_data(&self) -> &UserData {
        &self.data
    }

    /// `auth.CheckSiteAdminOrSameUserFromActor`: the internal actor and
    /// `actor.UID == subjectUserID` pass, otherwise the viewer must be a site
    /// admin (resolved at prefetch time).
    fn can_administer(&self, actor: Actor) -> bool {
        match actor {
            Actor::User { uid } if uid == self.data.id => true,
            _ => self.viewer_site_admin,
        }
    }

    /// `relay.MarshalID("User", id)`: `base64.URLEncoding("User:" + json(id))`.
    pub fn relay_id(id: i32) -> String {
        base64::engine::general_purpose::URL_SAFE.encode(format!("User:{id}"))
    }
}

impl ObjectValue for UserObject {
    fn type_name(&self) -> &'static str {
        "User"
    }

    fn field(
        &self,
        field: &str,
        arguments: &Arguments,
        actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "id" => Ok(Some(FieldValue::Json(Json::Str(Self::relay_id(
                self.data.id,
            ))))),
            "databaseID" => Ok(Some(FieldValue::Json(Json::Int(self.data.id as i64)))),
            "username" => Ok(Some(FieldValue::Json(Json::Str(
                self.data.username.clone(),
            )))),
            // `UserResolver.NamespaceName` (`user.go:601`): the raw username.
            "namespaceName" => Ok(Some(FieldValue::Json(Json::Str(
                self.data.username.clone(),
            )))),
            // `DisplayName`/`AvatarURL` return nil for the empty string.
            "displayName" => Ok(Some(if self.data.display_name.is_empty() {
                FieldValue::Null
            } else {
                FieldValue::Json(Json::Str(self.data.display_name.clone()))
            })),
            "avatarURL" => Ok(Some(if self.data.avatar_url.is_empty() {
                FieldValue::Null
            } else {
                FieldValue::Json(Json::Str(self.data.avatar_url.clone()))
            })),
            // `UserResolver.URL`: `"/users/" + user.Username`.
            "url" => Ok(Some(FieldValue::Json(Json::Str(format!(
                "/users/{}",
                self.data.username
            ))))),
            // `SettingsURL` is `url() + "/settings"` and is always non-nil.
            "settingsURL" => Ok(Some(FieldValue::Json(Json::Str(format!(
                "/users/{}/settings",
                self.data.username
            ))))),
            "createdAt" => Ok(Some(FieldValue::Json(Json::Str(datetime_rfc3339(
                self.data.created_at,
            ))))),
            // `UpdatedAt` always returns a non-nil DateTime (the users column
            // is NOT NULL), although the schema field is nullable.
            "updatedAt" => Ok(Some(FieldValue::Json(Json::Str(datetime_rfc3339(
                self.data.updated_at,
            ))))),
            // `UserResolver.Email`: the primary email, visible to the user
            // and site admins (`user.go:136-148`).
            "email" => {
                if !self.can_administer(actor) {
                    return Err(ExecError::new(MUST_BE_SITE_ADMIN_OR_SAME_USER));
                }
                Ok(Some(FieldValue::Json(Json::Str(
                    self.data.primary_email.clone(),
                ))))
            }
            // `UserResolver.SiteAdmin` (`user.go:404-411`): same authorization
            // as `email`.
            "siteAdmin" => {
                if !self.can_administer(actor) {
                    return Err(ExecError::new(MUST_BE_SITE_ADMIN_OR_SAME_USER));
                }
                Ok(Some(FieldValue::Json(Json::Bool(self.data.site_admin))))
            }
            // `UserResolver.ViewerCanAdminister` (`user.go:574-588`):
            // unauthorized becomes `false` instead of an error.
            "viewerCanAdminister" => Ok(Some(FieldValue::Json(Json::Bool(
                self.can_administer(actor),
            )))),
            // `UserResolver.Organizations` (`user.go:546-562`): the same
            // folded authorization error, then the static `GetByUserID`
            // connection.
            "organizations" => {
                if !self.can_administer(actor) {
                    return Err(ExecError::new(MUST_BE_SITE_ADMIN_OR_SAME_USER));
                }
                match self.nested.user_organizations.get(&self.data.id) {
                    Some(UserOrganizationsLookup::Found(orgs)) => Ok(Some(FieldValue::Object(
                        Arc::new(OrgConnectionStaticObject::from_nodes(
                            orgs.clone(),
                            self.viewer_site_admin,
                            self.nested.clone(),
                        )),
                    ))),
                    Some(UserOrganizationsLookup::Error(message)) => {
                        Err(ExecError::new(message.clone()))
                    }
                    None => Ok(None),
                }
            }
            // `UserResolver.Teams` (M9.27a): `areTeamEndpointsAvailable` then
            // a root-only `TeamConnection`. The Go resolver never sets
            // `ForUserMember`, so membership is ignored.
            "teams" => {
                let handle = self
                    .teams
                    .as_ref()
                    .map(|wrapper| &wrapper.0)
                    .or_else(|| self.nested.teams.as_ref().map(|wrapper| &wrapper.0));
                let Some(handle) = handle else {
                    return Ok(None);
                };
                crate::teams_graphql::user_teams(handle, arguments, self.viewer_site_admin)
            }
            // `UserResolver.Roles` / `Permissions` (M9.13b; the authorization
            // results are prefetched per viewer).
            "roles" => crate::rbac_graphql::user_role_connection(
                &self.nested,
                self.data.id,
                &crate::rbac_graphql::role_connection_args(arguments),
                self.viewer_site_admin,
            ),
            "permissions" => crate::rbac_graphql::user_permission_connection(
                &self.nested,
                self.data.id,
                self.viewer_site_admin,
            ),
            // `UserResolver.ExecutorSecrets` (M9.13b).
            "executorSecrets" => {
                let args =
                    match crate::executor_secrets_graphql::ExecutorSecretsArgs::from_arguments(
                        arguments,
                    ) {
                        Ok(args) => args,
                        Err(message) => return Err(ExecError::new(message)),
                    };
                match self
                    .nested
                    .user_executor_secrets
                    .get(&(self.data.id, args.clone()))
                {
                    Some(Ok(data)) => Ok(Some(
                        crate::executor_secrets_graphql::executor_secret_connection_value(
                            data,
                            args.scope,
                            self.viewer_site_admin,
                            self.nested.clone(),
                        ),
                    )),
                    Some(Err(message)) => Err(ExecError::new(message.clone())),
                    None => Ok(None),
                }
            }
            // `UserResolver.OrganizationMemberships` (`org_members.go:12-30`):
            // `auth.CheckSiteAdminOrSameUser` then the static `GetByUserID`
            // connection.
            "organizationMemberships" => {
                if !self.can_administer(actor) {
                    return Err(ExecError::new(MUST_BE_SITE_ADMIN_OR_SAME_USER));
                }
                match self.nested.user_memberships.get(&self.data.id) {
                    Some(UserMembershipsLookup::Found(memberships)) => {
                        Ok(Some(FieldValue::Object(Arc::new(
                            OrganizationMembershipConnectionObject::from_memberships(
                                memberships.clone(),
                                self.viewer_site_admin,
                                self.nested.clone(),
                            ),
                        ))))
                    }
                    Some(UserMembershipsLookup::Error(message)) => {
                        Err(ExecError::new(message.clone()))
                    }
                    None => Ok(None),
                }
            }
            // `UserResolver.ExternalAccounts` (`external_accounts.go:55-73`):
            // the same folded authorization error, then the id-ascending
            // connection (M9.8ka).
            "externalAccounts" => {
                if !self.can_administer(actor) {
                    return Err(ExecError::new(MUST_BE_SITE_ADMIN_OR_SAME_USER));
                }
                let first = crate::external_accounts_graphql::first_argument(arguments);
                match self.nested.user_external_accounts.get(&(self.data.id, first)) {
                    Some(crate::external_accounts_graphql::ExternalAccountsLookup::Found(data)) => {
                        Ok(Some(FieldValue::Object(Arc::new(
                            crate::external_accounts_graphql::ExternalAccountsConnectionObject::from_data(
                                data.clone(),
                                Some(self.data.clone()),
                                self.viewer_site_admin,
                                self.nested.clone(),
                            ),
                        ))))
                    }
                    Some(crate::external_accounts_graphql::ExternalAccountsLookup::Error(message)) => {
                        Err(ExecError::new(message.clone()))
                    }
                    None => Ok(None),
                }
            }
            // `UserResolver.LatestSettings` (`user.go:374-389`): the same
            // folded authorization error as `email`, then the newest
            // `settings` row for this user (null when none exists).
            "latestSettings" => {
                if !self.can_administer(actor) {
                    return Err(ExecError::new(MUST_BE_SITE_ADMIN_OR_SAME_USER));
                }
                match self.nested.latest_settings.get(&self.data.id) {
                    Some(LatestSettingsLookup::Found(settings)) => Ok(Some(match settings {
                        Some(settings) => FieldValue::Object(Arc::new(SettingsObject {
                            data: settings.clone(),
                            subject: SettingsSubjectObject::User(self.clone()),
                            viewer_site_admin: self.viewer_site_admin,
                            nested: self.nested.clone(),
                        })),
                        None => FieldValue::Null,
                    })),
                    Some(LatestSettingsLookup::Error(message)) => {
                        Err(ExecError::new(message.clone()))
                    }
                    None => Ok(None),
                }
            }
            "tosAccepted" => Ok(Some(FieldValue::Json(Json::Bool(self.data.tos_accepted)))),
            // `UserResolver.SettingsCascade` (`user.go:391-401`): the folded
            // access check, then the prefetched cascade.
            "settingsCascade" | "configurationCascade" => {
                if !self.can_administer(actor) {
                    return Err(ExecError::new(MUST_BE_SITE_ADMIN_OR_SAME_USER));
                }
                cascade_field(
                    &self.nested,
                    CascadeKey::User(self.data.id),
                    self.viewer_site_admin,
                    field == "configurationCascade",
                )
            }
            // `UserResolver.Monitors` (`code_monitors.go:24-29`, M9.13a):
            // only present when the optional code-monitor resolver was
            // registered; `auth.CheckSameUserFromActor` then lists the user's
            // monitors.
            "monitors" => match &self.nested.code_monitors {
                Some(handle) => crate::code_monitors_graphql::user_monitors(
                    handle.0.as_ref(),
                    actor,
                    self.data.id,
                    arguments,
                )
                .map(Some),
                None => Ok(None),
            },
            // `UserResolver.Emails` / `PrimaryEmail` / `AccessTokens` /
            // `Session` / `HasVerifiedEmail` (M9.18a; prefetched per user).
            "emails" => match self.nested.user_accounts.get(&self.data.id) {
                None => Ok(None),
                Some(account) => match &account.emails {
                    Err(message) => Err(ExecError::new(message.clone())),
                    Ok(emails) => Ok(Some(FieldValue::List {
                        element_type: Some("UserEmail"),
                        items: emails
                            .iter()
                            .cloned()
                            .map(|data| {
                                FieldValue::Object(Arc::new(
                                    crate::graphql_inventory_c::UserEmailObject {
                                        data,
                                        user: self.clone(),
                                        viewer_site_admin: self.viewer_site_admin,
                                    },
                                ))
                            })
                            .collect(),
                    })),
                },
            },
            "primaryEmail" => match self.nested.user_accounts.get(&self.data.id) {
                None => Ok(None),
                Some(account) => match &account.primary_email {
                    Err(message) => Err(ExecError::new(message.clone())),
                    Ok(found) => Ok(Some(match found {
                        Some(data) => FieldValue::Object(Arc::new(
                            crate::graphql_inventory_c::UserEmailObject {
                                data: data.clone(),
                                user: self.clone(),
                                viewer_site_admin: self.viewer_site_admin,
                            },
                        )),
                        None => FieldValue::Null,
                    })),
                },
            },
            "accessTokens" => match self.nested.user_accounts.get(&self.data.id) {
                None => Ok(None),
                Some(account) => match &account.access_tokens {
                    Err(message) => Err(ExecError::new(message.clone())),
                    Ok(full) => Ok(Some(FieldValue::Object(Arc::new(
                        crate::graphql_inventory_c::AccessTokenConnectionObject {
                            full: full.clone(),
                            first: arguments
                                .i64("first")
                                .and_then(|first| i32::try_from(first).ok()),
                            viewer_site_admin: self.viewer_site_admin,
                            nested: self.nested.clone(),
                        },
                    )))),
                },
            },
            "session" => match self.nested.user_accounts.get(&self.data.id) {
                None => Ok(None),
                Some(account) => match &account.session_can_sign_out {
                    Err(message) => Err(ExecError::new(message.clone())),
                    Ok(can_sign_out) => Ok(Some(FieldValue::Object(Arc::new(
                        crate::graphql_inventory_c::SessionObject {
                            can_sign_out: *can_sign_out,
                        },
                    )))),
                },
            },
            "hasVerifiedEmail" => match self.nested.user_accounts.get(&self.data.id) {
                None => Ok(None),
                Some(account) => match &account.has_verified_email {
                    Err(message) => Err(ExecError::new(message.clone())),
                    Ok(verified) => Ok(Some(FieldValue::Json(Json::Bool(*verified)))),
                },
            },
            // `UserResolver.BatchChanges` (M9.24c): the extension resolver
            // supplies the namespace and delegates.
            "batchChanges" => match &self.nested.batches {
                Some(batches) => crate::batches_graphql::namespace_batch_changes(
                    &batches.0,
                    crate::batches_graphql::NamespaceIds {
                        user: self.data.id,
                        org: 0,
                    },
                    arguments,
                    actor,
                )
                .map(Some),
                None => Ok(None),
            },
            _ => Ok(None),
        }
    }
}

/// `settingsResolver` (`cmd/frontend/graphqlbackend/settings.go:17-60`),
/// GraphQL type `Settings`.
#[derive(Debug, Clone, PartialEq)]
pub struct SettingsObject {
    data: SettingsData,
    /// The checked settings subject these settings belong to.
    subject: SettingsSubjectObject,
    viewer_site_admin: bool,
    nested: Arc<NestedPrefetch>,
}

impl ObjectValue for SettingsObject {
    fn type_name(&self) -> &'static str {
        "Settings"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "id" => Ok(Some(FieldValue::Json(Json::Int(i64::from(self.data.id))))),
            // `SettingsResolver.Subject` returns the access-checked subject
            // (`settings_subject.go:203-218`); for `User.latestSettings` it is
            // this user.
            "subject" => Ok(Some(FieldValue::Object(Arc::new(self.subject.clone())))),
            // `SettingsResolver.Author` (`settings.go:48-60`): the author row,
            // null when there is no author or the author was soft-deleted.
            "author" => match self.data.author_user_id {
                None => Ok(Some(FieldValue::Null)),
                Some(author_id) => match self.nested.users_by_id.get(&author_id) {
                    Some(UserByIdLookup::Found(user)) => Ok(Some(FieldValue::Object(Arc::new(
                        UserObject::from_data_as_viewer(user.clone(), self.viewer_site_admin)
                            .with_nested(self.nested.clone()),
                    )))),
                    Some(UserByIdLookup::Error(message)) => Err(ExecError::new(message.clone())),
                    None => Ok(None),
                },
            },
            "createdAt" => Ok(Some(FieldValue::Json(Json::Str(datetime_rfc3339(
                self.data.created_at,
            ))))),
            "contents" => Ok(Some(FieldValue::Json(Json::Str(
                self.data.contents.clone(),
            )))),
            // Deprecated alias (`settings.go:36-38`): same raw contents.
            "configuration" => Ok(Some(FieldValue::Object(Arc::new(
                ConfigurationObject::new(self.data.contents.clone()),
            )))),
            _ => Ok(None),
        }
    }
}

/// A checked `SettingsSubject` (`settings_subject.go:93-123`): the interface
/// field selections are delegated to the concrete subject object.
#[derive(Debug, Clone, PartialEq)]
pub enum SettingsSubjectObject {
    Default(DefaultSettingsObject),
    User(UserObject),
    Org(OrgObject),
    Site(SiteObject),
}

impl SettingsSubjectObject {
    /// Build the GraphQL object for a prefetched subject, attaching the
    /// request's nested data.
    pub fn from_data(
        data: SettingsSubjectData,
        viewer_site_admin: bool,
        nested: Arc<NestedPrefetch>,
    ) -> Self {
        match data {
            SettingsSubjectData::Default => Self::Default(DefaultSettingsObject {
                viewer_site_admin,
                nested,
            }),
            SettingsSubjectData::User(data) => Self::User(
                UserObject::from_data_as_viewer(data, viewer_site_admin).with_nested(nested),
            ),
            SettingsSubjectData::Org(data) => Self::Org(
                OrgObject::from_data_as_viewer(data, viewer_site_admin).with_nested(nested),
            ),
            SettingsSubjectData::Site => {
                // `SettingsCascade.subjects` reaches the Site subject through
                // this path; its `configuration` was prefetched by
                // `prefetch_cascade_subject_site_configuration` (M9.8jl).
                let site = nested
                    .cascade_site_configuration
                    .clone()
                    .map(|configuration| {
                        Arc::new(crate::config_graphql::SiteRoot {
                            configuration: Some(configuration),
                        })
                    });
                let auth_providers = nested.auth_providers.clone();
                Self::Site(SiteObject {
                    viewer_site_admin,
                    nested,
                    site,
                    auth_providers,
                })
            }
        }
    }
}

impl ObjectValue for SettingsSubjectObject {
    fn type_name(&self) -> &'static str {
        match self {
            Self::Default(default) => default.type_name(),
            Self::User(user) => user.type_name(),
            Self::Org(org) => org.type_name(),
            Self::Site(site) => site.type_name(),
        }
    }

    fn field(
        &self,
        field: &str,
        arguments: &Arguments,
        actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match self {
            Self::Default(default) => default.field(field, arguments, actor),
            Self::User(user) => user.field(field, arguments, actor),
            Self::Org(org) => org.field(field, arguments, actor),
            Self::Site(site) => site.field(field, arguments, actor),
        }
    }
}

/// `siteResolver` as a `SettingsSubject` (`site.go:92,134-157,166`): only the
/// interface fields are ported. `Query.settingsSubject` performs no access
/// check for the site subject (`settings_subject.go:93-123`).
#[derive(Debug, Clone, PartialEq)]
pub struct SiteObject {
    viewer_site_admin: bool,
    nested: Arc<NestedPrefetch>,
    /// Prefetched `configuration` data: `Query.site` (M9.8je) or
    /// `Query.settingsSubject(id: <Site>)` (M9.8jh, a separate prefetch).
    site: Option<Arc<crate::config_graphql::SiteRoot>>,
    /// `Site.authProviders` derived from the live `auth.providers` site
    /// configuration (M9.8jg). `None` when the provider directory was not
    /// injected (transport-only tests), leaving the field unported.
    auth_providers: Option<Arc<crate::auth_providers_graphql::AuthProvidersRoot>>,
}

impl SiteObject {
    /// `relay.MarshalID("Site", "site")`.
    pub fn relay_id() -> String {
        base64::engine::general_purpose::URL_SAFE.encode("Site:\"site\"")
    }

    fn latest_settings(&self) -> Result<Option<FieldValue>, ExecError> {
        match &self.nested.site_latest_settings {
            Some(LatestSettingsLookup::Found(Some(settings))) => {
                Ok(Some(FieldValue::Object(Arc::new(SettingsObject {
                    data: settings.clone(),
                    subject: SettingsSubjectObject::Site(self.clone()),
                    viewer_site_admin: self.viewer_site_admin,
                    nested: self.nested.clone(),
                }))))
            }
            Some(LatestSettingsLookup::Found(None)) => Ok(Some(FieldValue::Null)),
            Some(LatestSettingsLookup::Error(message)) => Err(ExecError::new(message.clone())),
            None => Ok(None),
        }
    }
}

impl ObjectValue for SiteObject {
    fn type_name(&self) -> &'static str {
        "Site"
    }

    fn field(
        &self,
        field: &str,
        arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            // `siteResolver.ExternalAccounts` (`external_accounts.go:21-53`):
            // the site-admin gate and connection are prefetched (M9.9b).
            "externalAccounts" => {
                let args =
                    crate::external_accounts_graphql::SiteExternalAccountsArgs::from_arguments(
                        arguments,
                    );
                Ok(match self.nested.site_external_accounts.get(&args) {
                    Some(crate::external_accounts_graphql::SiteExternalAccountsLookup::Found(
                        data,
                    )) => Some(FieldValue::Object(Arc::new(
                        crate::external_accounts_graphql::SiteExternalAccountsConnectionObject::from_data(
                            data.clone(),
                            self.viewer_site_admin,
                            self.nested.clone(),
                        ),
                    ))),
                    Some(crate::external_accounts_graphql::SiteExternalAccountsLookup::Error(
                        message,
                    )) => return Err(ExecError::new(message.clone())),
                    None => None,
                })
            }
            "id" => Ok(Some(FieldValue::Json(Json::Str(Self::relay_id())))),
            "settingsURL" => Ok(Some(FieldValue::Json(Json::Str(
                "/site-admin/global-settings".to_owned(),
            )))),
            "viewerCanAdminister" => Ok(Some(FieldValue::Json(Json::Bool(self.viewer_site_admin)))),
            "latestSettings" => self.latest_settings(),
            // `siteResolver.SettingsCascade` (`site.go:161-171`): no access
            // check for the site subject.
            "settingsCascade" | "configurationCascade" => cascade_field(
                &self.nested,
                CascadeKey::Site,
                self.viewer_site_admin,
                field == "configurationCascade",
            ),
            // `siteResolver.Configuration` (`site.go:100-138`); the result is
            // prefetched because the admin check and `conf.Raw()` are async
            // (M9.8je).
            "configuration" => match &self.site {
                None => Ok(None),
                Some(site) => match &site.configuration {
                    None => Ok(None),
                    Some(Err(message)) => Err(ExecError::new(message.clone())),
                    Some(Ok(root)) => Ok(Some(FieldValue::Object(Arc::new(
                        crate::config_graphql::SiteConfigurationObject::new(root.clone()),
                    )))),
                },
            },
            // `siteResolver.AuthProviders` (`auth_providers.go:10-16`): no
            // access check, no request-header dependency.
            "authProviders" => Ok(self.auth_providers.as_ref().map(|root| {
                FieldValue::Object(Arc::new(
                    crate::auth_providers_graphql::AuthProviderConnectionObject::new(
                        root.providers.clone(),
                    ),
                ))
            })),
            _ => Ok(None),
        }
    }
}

/// Deprecated `Configuration` (`schema.graphql:7764-7779`): `contents`
/// mirrors `Settings.contents` / `SettingsCascade.final`; `messages` is empty
/// except for `SettingsCascade.merged`'s error text.
#[derive(Debug, Clone, PartialEq)]
pub struct ConfigurationObject {
    contents: String,
    messages: Vec<String>,
}

impl ConfigurationObject {
    fn new(contents: String) -> Self {
        Self {
            contents,
            messages: Vec::new(),
        }
    }
}

impl ObjectValue for ConfigurationObject {
    fn type_name(&self) -> &'static str {
        "Configuration"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "contents" => Ok(Some(FieldValue::Json(Json::Str(self.contents.clone())))),
            "messages" => Ok(Some(FieldValue::List {
                items: self
                    .messages
                    .iter()
                    .map(|message| FieldValue::Json(Json::Str(message.clone())))
                    .collect(),
                element_type: Some("String"),
            })),
            _ => Ok(None),
        }
    }
}

/// `settingsCascade` (`settings_cascade.go:17-58`): the relevant subjects,
/// the merged `final` JSON and the deprecated `merged` `Configuration`.
#[derive(Debug, Clone, PartialEq)]
pub struct SettingsCascadeObject {
    entries: Vec<CascadeSubjectEntry>,
    final_lookup: CascadeFinalLookup,
    viewer_site_admin: bool,
    nested: Arc<NestedPrefetch>,
}

impl SettingsCascadeObject {
    fn from_data(
        data: &SettingsCascadeData,
        viewer_site_admin: bool,
        nested: Arc<NestedPrefetch>,
    ) -> Self {
        Self {
            entries: data.entries.clone(),
            final_lookup: data.final_lookup.clone(),
            viewer_site_admin,
            nested,
        }
    }

    fn subjects(&self) -> FieldValue {
        FieldValue::List {
            items: self
                .entries
                .iter()
                .map(|entry| {
                    FieldValue::Object(Arc::new(SettingsSubjectObject::from_data(
                        entry.subject.clone(),
                        self.viewer_site_admin,
                        self.nested.clone(),
                    )))
                })
                .collect(),
            element_type: Some("SettingsSubject"),
        }
    }

    /// `SettingsCascade.Final` (`settings_cascade.go:46-58`).
    fn final_contents(&self) -> Result<String, ExecError> {
        match &self.final_lookup {
            CascadeFinalLookup::Found(merged) => Ok(merged.clone()),
            CascadeFinalLookup::Error(message) => Err(ExecError::new(message.clone())),
        }
    }

    /// `SettingsCascade.Merged` (`settings_cascade.go:61-73`): errors become
    /// `messages` and an empty `contents`; the field itself never fails.
    fn merged(&self) -> ConfigurationObject {
        match &self.final_lookup {
            CascadeFinalLookup::Found(merged) => ConfigurationObject::new(merged.clone()),
            CascadeFinalLookup::Error(message) => ConfigurationObject {
                contents: String::new(),
                messages: vec![message.clone()],
            },
        }
    }
}

impl ObjectValue for SettingsCascadeObject {
    fn type_name(&self) -> &'static str {
        "SettingsCascade"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "subjects" => Ok(Some(self.subjects())),
            "final" => Ok(Some(FieldValue::Json(Json::Str(self.final_contents()?)))),
            "merged" => Ok(Some(FieldValue::Object(Arc::new(self.merged())))),
            _ => Ok(None),
        }
    }
}

/// Deprecated `ConfigurationCascade` (`schema.graphql:7723-7735`): the same
/// object without the `final` field.
#[derive(Debug, Clone, PartialEq)]
pub struct ConfigurationCascadeObject {
    inner: SettingsCascadeObject,
}

impl ObjectValue for ConfigurationCascadeObject {
    fn type_name(&self) -> &'static str {
        "ConfigurationCascade"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "subjects" => Ok(Some(self.inner.subjects())),
            "merged" => Ok(Some(FieldValue::Object(Arc::new(self.inner.merged())))),
            _ => Ok(None),
        }
    }
}

/// Look up a prefetched cascade for `key` and wrap it as the requested
/// GraphQL type (`SettingsCascade` or the deprecated `ConfigurationCascade`).
fn cascade_field(
    nested: &Arc<NestedPrefetch>,
    key: CascadeKey,
    viewer_site_admin: bool,
    configuration: bool,
) -> Result<Option<FieldValue>, ExecError> {
    match nested.settings_cascades.get(&key) {
        Some(CascadeLookup::Found(data)) => {
            let cascade = SettingsCascadeObject::from_data(data, viewer_site_admin, nested.clone());
            Ok(Some(if configuration {
                FieldValue::Object(Arc::new(ConfigurationCascadeObject { inner: cascade }))
            } else {
                FieldValue::Object(Arc::new(cascade))
            }))
        }
        Some(CascadeLookup::Error(message)) => Err(ExecError::new(message.clone())),
        None => Ok(None),
    }
}

/// `defaultSettingsResolver`'s GraphQL object (`default_settings.go`): only
/// reachable through `SettingsCascade.subjects`.
#[derive(Debug, Clone, PartialEq)]
pub struct DefaultSettingsObject {
    viewer_site_admin: bool,
    nested: Arc<NestedPrefetch>,
}

impl DefaultSettingsObject {
    /// `relay.MarshalID("DefaultSettings", "DefaultSettings")`.
    pub fn relay_id() -> String {
        base64::engine::general_purpose::URL_SAFE.encode("DefaultSettings:\"DefaultSettings\"")
    }
}

impl ObjectValue for DefaultSettingsObject {
    fn type_name(&self) -> &'static str {
        "DefaultSettings"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "id" => Ok(Some(FieldValue::Json(Json::Str(Self::relay_id())))),
            // `LatestSettings` (`default_settings.go:35-45`) returns the
            // synthetic default row without touching the database.
            "latestSettings" => Ok(Some(FieldValue::Object(Arc::new(SettingsObject {
                data: default_settings_data(),
                subject: SettingsSubjectObject::Default(self.clone()),
                viewer_site_admin: self.viewer_site_admin,
                nested: self.nested.clone(),
            })))),
            "settingsURL" => Ok(Some(FieldValue::Null)),
            "viewerCanAdminister" => Ok(Some(FieldValue::Json(Json::Bool(false)))),
            "settingsCascade" | "configurationCascade" => cascade_field(
                &self.nested,
                CascadeKey::Default,
                self.viewer_site_admin,
                field == "configurationCascade",
            ),
            _ => Ok(None),
        }
    }
}

/// `userConnectionResolver` (`users.go:62-144`): `nodes`, `totalCount` and
/// `pageInfo`.
#[derive(Debug, Clone, PartialEq)]
pub struct UsersConnectionObject {
    data: UsersConnectionData,
    viewer_site_admin: bool,
    nested: Arc<NestedPrefetch>,
}

impl UsersConnectionObject {
    pub fn from_data(data: UsersConnectionData, viewer_site_admin: bool) -> Self {
        Self {
            data,
            viewer_site_admin,
            nested: Arc::new(NestedPrefetch::default()),
        }
    }

    /// Attach the request's nested prefetch data (see [`prefetch_nested`]).
    pub fn with_nested(mut self, nested: Arc<NestedPrefetch>) -> Self {
        self.nested = nested;
        self
    }

    /// `userConnectionResolver.PageInfo` (`users.go:122-144`). Returns
    /// `(hasNextPage, endCursor)`.
    fn page_info(&self) -> (bool, Option<String>) {
        // Without a limit we would have had all results.
        let Some(limit) = self.data.limit else {
            return (false, None);
        };
        let after = self.data.offset + self.data.users.len() as i32;
        // Fewer results than the limit means we have everything.
        if after < limit {
            return (false, None);
        }
        if self.data.total_count > after {
            return (true, Some(after.to_string()));
        }
        (false, None)
    }
}

impl ObjectValue for UsersConnectionObject {
    fn type_name(&self) -> &'static str {
        "UserConnection"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "nodes" => Ok(Some(FieldValue::List {
                items: self
                    .data
                    .users
                    .iter()
                    .map(|user| {
                        FieldValue::Object(Arc::new(
                            UserObject::from_data_as_viewer(user.clone(), self.viewer_site_admin)
                                .with_nested(self.nested.clone()),
                        ))
                    })
                    .collect(),
                element_type: Some("User"),
            })),
            "totalCount" => Ok(Some(FieldValue::Json(Json::Int(i64::from(
                self.data.total_count,
            ))))),
            "pageInfo" => {
                let (has_next_page, end_cursor) = self.page_info();
                Ok(Some(FieldValue::Object(Arc::new(PageInfoObject {
                    has_next_page,
                    end_cursor,
                }))))
            }
            _ => Ok(None),
        }
    }
}

/// `gqlutil.PageInfo` (`internal/gqlutil/page_info.go:3-21`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PageInfoObject {
    has_next_page: bool,
    end_cursor: Option<String>,
}

impl PageInfoObject {
    /// Build a `PageInfo` from an already-computed page boundary.
    pub fn new(has_next_page: bool, end_cursor: Option<String>) -> Self {
        Self {
            has_next_page,
            end_cursor,
        }
    }
}

impl ObjectValue for PageInfoObject {
    fn type_name(&self) -> &'static str {
        "PageInfo"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "hasNextPage" => Ok(Some(FieldValue::Json(Json::Bool(self.has_next_page)))),
            "endCursor" => Ok(Some(match &self.end_cursor {
                Some(cursor) => FieldValue::Json(Json::Str(cursor.clone())),
                None => FieldValue::Null,
            })),
            _ => Ok(None),
        }
    }
}

/// `auth.ErrNotAuthenticated` (`internal/auth/orgs.go:12`).
const NOT_AUTHENTICATED: &str = "not authenticated";

/// `auth.ErrMustBeSiteAdmin` (`internal/auth/site_admin.go:14`).
const MUST_BE_SITE_ADMIN: &str = "must be site admin";

/// `OrgResolver` (`cmd/frontend/graphqlbackend/org.go:65`), limited to the
/// fields backed by the `orgs` row and viewer authorization.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrgObject {
    data: OrgData,
    /// The viewer's `users.site_admin`; member `User` objects built by
    /// `Org.members` need it for their own viewer checks.
    viewer_site_admin: bool,
    /// Nested resolver data prefetched for this request (see
    /// [`prefetch_nested`]).
    nested: Arc<NestedPrefetch>,
}

impl OrgObject {
    pub fn from_data(data: OrgData) -> Self {
        Self {
            data,
            viewer_site_admin: false,
            nested: Arc::new(NestedPrefetch::default()),
        }
    }

    pub fn from_data_as_viewer(data: OrgData, viewer_site_admin: bool) -> Self {
        Self {
            data,
            viewer_site_admin,
            nested: Arc::new(NestedPrefetch::default()),
        }
    }

    /// Attach the request's nested prefetch data (see [`prefetch_nested`]).
    pub fn with_nested(mut self, nested: Arc<NestedPrefetch>) -> Self {
        self.nested = nested;
        self
    }

    /// `relay.MarshalID("Org", id)`: `base64.URLEncoding("Org:" + json(id))`.
    pub fn relay_id(id: i32) -> String {
        base64::engine::general_purpose::URL_SAFE.encode(format!("Org:{id}"))
    }
}

impl ObjectValue for OrgObject {
    fn type_name(&self) -> &'static str {
        "Org"
    }

    fn field(
        &self,
        field: &str,
        arguments: &Arguments,
        actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "id" => Ok(Some(FieldValue::Json(Json::Str(Self::relay_id(
                self.data.id,
            ))))),
            "name" => Ok(Some(FieldValue::Json(Json::Str(self.data.name.clone())))),
            // `OrgResolver.LatestSettings` (`org.go:176-195`): the org access
            // check (`settingsSubjectForNodeAndCheckAccess`) then `GetLatest`.
            "latestSettings" => {
                if !self.data.viewer_can_administer {
                    return Err(ExecError::new(org_access_error(actor)));
                }
                match self.nested.org_latest_settings.get(&self.data.id) {
                    Some(LatestSettingsLookup::Found(Some(settings))) => {
                        Ok(Some(FieldValue::Object(Arc::new(SettingsObject {
                            data: settings.clone(),
                            subject: SettingsSubjectObject::Org(self.clone()),
                            viewer_site_admin: self.viewer_site_admin,
                            nested: self.nested.clone(),
                        }))))
                    }
                    Some(LatestSettingsLookup::Found(None)) => Ok(Some(FieldValue::Null)),
                    Some(LatestSettingsLookup::Error(message)) => {
                        Err(ExecError::new(message.clone()))
                    }
                    None => Ok(None),
                }
            }
            // `OrgResolver.DisplayName` returns the row pointer as-is: NULL is
            // `null`, an empty string stays `""`.
            "displayName" => Ok(Some(match &self.data.display_name {
                Some(display_name) => FieldValue::Json(Json::Str(display_name.clone())),
                None => FieldValue::Null,
            })),
            "createdAt" => Ok(Some(FieldValue::Json(Json::Str(datetime_rfc3339(
                self.data.created_at,
            ))))),
            // `OrgResolver.URL`: `"/organizations/" + name`.
            "url" => Ok(Some(FieldValue::Json(Json::Str(format!(
                "/organizations/{}",
                self.data.name
            ))))),
            "settingsURL" => Ok(Some(FieldValue::Json(Json::Str(format!(
                "/organizations/{}/settings",
                self.data.name
            ))))),
            "namespaceName" => Ok(Some(FieldValue::Json(Json::Str(self.data.name.clone())))),
            "viewerCanAdminister" => Ok(Some(FieldValue::Json(Json::Bool(
                self.data.viewer_can_administer,
            )))),
            "viewerIsMember" => Ok(Some(FieldValue::Json(Json::Bool(
                self.data.viewer_is_member,
            )))),
            // `OrgResolver.SettingsCascade` (`org.go:194-204`): the org
            // access check, then the prefetched cascade.
            "settingsCascade" | "configurationCascade" => {
                if !self.data.viewer_can_administer {
                    return Err(ExecError::new(org_access_error(actor)));
                }
                cascade_field(
                    &self.nested,
                    CascadeKey::Org(self.data.id),
                    self.viewer_site_admin,
                    field == "configurationCascade",
                )
            }
            // `OrgResolver.Members` (`org.go:100-121`): a gqlutil
            // `ConnectionResolver`, prefetched once per argument set. In OSS
            // mode there is no dotcom membership gate.
            "members" => {
                let args = members_args(arguments);
                match self.nested.org_members.get(&(self.data.id, args)) {
                    // The baseline's `Members` never fails here: cursor and
                    // database errors surface from `Nodes`/`PageInfo`
                    // (`ConnectionResolver.Nodes`), so the error lives on
                    // the connection object.
                    Some(MembersConnectionLookup::Found(data)) => Ok(Some(FieldValue::Object(
                        Arc::new(OrgMembersConnectionObject::found(
                            data.clone(),
                            self.viewer_site_admin,
                            self.nested.clone(),
                        )),
                    ))),
                    Some(MembersConnectionLookup::Error(message)) => Ok(Some(FieldValue::Object(
                        Arc::new(OrgMembersConnectionObject::errored(
                            message.clone(),
                            self.viewer_site_admin,
                            self.nested.clone(),
                        )),
                    ))),
                    None => Ok(None),
                }
            }
            // `OrgResolver.ExecutorSecrets` (M9.13b).
            "executorSecrets" => {
                let args =
                    match crate::executor_secrets_graphql::ExecutorSecretsArgs::from_arguments(
                        arguments,
                    ) {
                        Ok(args) => args,
                        Err(message) => return Err(ExecError::new(message)),
                    };
                match self
                    .nested
                    .org_executor_secrets
                    .get(&(self.data.id, args.clone()))
                {
                    Some(Ok(data)) => Ok(Some(
                        crate::executor_secrets_graphql::executor_secret_connection_value(
                            data,
                            args.scope,
                            self.viewer_site_admin,
                            self.nested.clone(),
                        ),
                    )),
                    Some(Err(message)) => Err(ExecError::new(message.clone())),
                    None => Ok(None),
                }
            }
            // `OrgResolver.BatchChanges` (M9.24c): the extension resolver
            // supplies the namespace and delegates.
            "batchChanges" => match &self.nested.batches {
                Some(batches) => crate::batches_graphql::namespace_batch_changes(
                    &batches.0,
                    crate::batches_graphql::NamespaceIds {
                        user: 0,
                        org: self.data.id,
                    },
                    arguments,
                    actor,
                )
                .map(Some),
                None => Ok(None),
            },
            _ => Ok(None),
        }
    }
}

/// `gqlutil.ConnectionResolver`'s `NewUsersConnection` (`org.go:117-121`,
/// `connection_resolver.go:250-330`): `nodes`, `totalCount` and the
/// bidirectional `ConnectionPageInfo`.
#[derive(Debug, Clone, PartialEq)]
pub struct OrgMembersConnectionObject {
    data: Option<MembersConnectionData>,
    /// `Nodes()`/`PageInfo()` surface the store failure (the baseline caches
    /// it in `nodesError`); `totalCount` uses the same message here because
    /// the store seam does not distinguish the two phases.
    error: Option<String>,
    viewer_site_admin: bool,
    nested: Arc<NestedPrefetch>,
}

impl OrgMembersConnectionObject {
    fn found(
        data: MembersConnectionData,
        viewer_site_admin: bool,
        nested: Arc<NestedPrefetch>,
    ) -> Self {
        Self {
            data: Some(data),
            error: None,
            viewer_site_admin,
            nested,
        }
    }

    fn errored(message: String, viewer_site_admin: bool, nested: Arc<NestedPrefetch>) -> Self {
        Self {
            data: None,
            error: Some(message),
            viewer_site_admin,
            nested,
        }
    }

    fn data(&self) -> Result<&MembersConnectionData, ExecError> {
        match (&self.data, &self.error) {
            (Some(data), _) => Ok(data),
            (None, Some(error)) => Err(ExecError::new(error.clone())),
            (None, None) => Err(ExecError::new("members connection has no data")),
        }
    }
}

impl ObjectValue for OrgMembersConnectionObject {
    fn type_name(&self) -> &'static str {
        "NewUsersConnection"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "nodes" => {
                let data = self.data()?;
                Ok(Some(FieldValue::List {
                    element_type: Some("User"),
                    items: data
                        .users
                        .iter()
                        .map(|user| {
                            FieldValue::Object(Arc::new(
                                UserObject::from_data_as_viewer(
                                    user.clone(),
                                    self.viewer_site_admin,
                                )
                                .with_nested(self.nested.clone()),
                            ))
                        })
                        .collect(),
                }))
            }
            "totalCount" => Ok(Some(FieldValue::Json(Json::Int(i64::from(
                self.data()?.total_count,
            ))))),
            "pageInfo" => Ok(Some(FieldValue::Object(Arc::new(
                ConnectionPageInfoObject::from_data(self.data()?),
            )))),
            _ => Ok(None),
        }
    }
}

/// `organizationMembershipConnectionResolver` (`org_members.go:33-45`). The
/// SDL exposes only `nodes`/`totalCount`; the baseline's
/// `PageInfo() = HasNextPage(false)` is unreachable.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrganizationMembershipConnectionObject {
    memberships: Vec<MembershipData>,
    viewer_site_admin: bool,
    nested: Arc<NestedPrefetch>,
}

impl OrganizationMembershipConnectionObject {
    fn from_memberships(
        memberships: Vec<MembershipData>,
        viewer_site_admin: bool,
        nested: Arc<NestedPrefetch>,
    ) -> Self {
        Self {
            memberships,
            viewer_site_admin,
            nested,
        }
    }
}

impl ObjectValue for OrganizationMembershipConnectionObject {
    fn type_name(&self) -> &'static str {
        "OrganizationMembershipConnection"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "nodes" => Ok(Some(FieldValue::List {
                element_type: Some("OrganizationMembership"),
                items: self
                    .memberships
                    .iter()
                    .map(|membership| {
                        FieldValue::Object(Arc::new(OrganizationMembershipObject {
                            membership: membership.clone(),
                            viewer_site_admin: self.viewer_site_admin,
                            nested: self.nested.clone(),
                        }))
                    })
                    .collect(),
            })),
            "totalCount" => Ok(Some(FieldValue::Json(Json::Int(
                self.memberships.len() as i64
            )))),
            _ => Ok(None),
        }
    }
}

/// `organizationMembershipResolver` (`org_members.go:47-61`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrganizationMembershipObject {
    membership: MembershipData,
    viewer_site_admin: bool,
    nested: Arc<NestedPrefetch>,
}

impl ObjectValue for OrganizationMembershipObject {
    fn type_name(&self) -> &'static str {
        "OrganizationMembership"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            // `Organization()`: `OrgByIDInt32`; a missing live org is the
            // baseline's `org not found: id <n>` error.
            "organization" => match self.nested.orgs_by_id.get(&self.membership.org_id) {
                Some(OrgByIdLookup::Found(org)) => Ok(Some(FieldValue::Object(Arc::new(
                    OrgObject::from_data_as_viewer(org.clone(), self.viewer_site_admin)
                        .with_nested(self.nested.clone()),
                )))),
                Some(OrgByIdLookup::Error(message)) => Err(ExecError::new(message.clone())),
                None => Ok(None),
            },
            // `User()`: `UserByIDInt32`.
            "user" => match self.nested.users_by_id.get(&self.membership.user_id) {
                Some(UserByIdLookup::Found(user)) => Ok(Some(FieldValue::Object(Arc::new(
                    UserObject::from_data_as_viewer(user.clone(), self.viewer_site_admin)
                        .with_nested(self.nested.clone()),
                )))),
                Some(UserByIdLookup::Error(message)) => Err(ExecError::new(message.clone())),
                None => Ok(None),
            },
            "createdAt" => Ok(Some(FieldValue::Json(Json::Str(datetime_rfc3339(
                self.membership.created_at,
            ))))),
            "updatedAt" => Ok(Some(FieldValue::Json(Json::Str(datetime_rfc3339(
                self.membership.updated_at,
            ))))),
            _ => Ok(None),
        }
    }
}

/// `gqlutil.ConnectionPageInfo` (`connection_resolver.go:236-330`). The
/// booleans use the *pre-trim* node count; the cursors use the trimmed page.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConnectionPageInfoObject {
    end_cursor: Option<String>,
    start_cursor: Option<String>,
    has_next_page: bool,
    has_previous_page: bool,
}

impl ConnectionPageInfoObject {
    /// Build the page info from an already-trimmed page boundary
    /// (`gqlutil.ConnectionPageInfo` for `Query.repositories`).
    pub fn new(
        start_cursor: Option<String>,
        end_cursor: Option<String>,
        has_next_page: bool,
        has_previous_page: bool,
    ) -> Self {
        Self {
            end_cursor,
            start_cursor,
            has_next_page,
            has_previous_page,
        }
    }

    fn from_data(data: &MembersConnectionData) -> Self {
        let has_next_page = if data.first_present {
            data.fetched_count > data.page_size
        } else if data.fetched_count == 0 {
            false
        } else {
            data.before_present
        };
        let has_previous_page = if data.last_present {
            data.fetched_count > data.page_size
        } else if data.fetched_count == 0 {
            false
        } else {
            data.after_present
        };
        Self {
            end_cursor: data.users.last().map(|user| UserObject::relay_id(user.id)),
            start_cursor: data.users.first().map(|user| UserObject::relay_id(user.id)),
            has_next_page,
            has_previous_page,
        }
    }
}

impl ObjectValue for ConnectionPageInfoObject {
    fn type_name(&self) -> &'static str {
        "ConnectionPageInfo"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "endCursor" => Ok(Some(optional_cursor(&self.end_cursor))),
            "startCursor" => Ok(Some(optional_cursor(&self.start_cursor))),
            "hasNextPage" => Ok(Some(FieldValue::Json(Json::Bool(self.has_next_page)))),
            "hasPreviousPage" => Ok(Some(FieldValue::Json(Json::Bool(self.has_previous_page)))),
            _ => Ok(None),
        }
    }
}

fn optional_cursor(cursor: &Option<String>) -> FieldValue {
    match cursor {
        Some(cursor) => FieldValue::Json(Json::Str(cursor.clone())),
        None => FieldValue::Null,
    }
}

/// `orgConnectionStaticResolver` (`orgs.go:71-82`): `nodes` and `totalCount`
/// from an already-fetched slice. The SDL `OrgConnection` type exposes no
/// `pageInfo` field, so the baseline's `HasNextPage(false)` is not reachable.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrgConnectionStaticObject {
    nodes: Vec<OrgData>,
    viewer_site_admin: bool,
    nested: Arc<NestedPrefetch>,
}

impl OrgConnectionStaticObject {
    /// The `User.organizations` construction site: every node is an `Org`
    /// object that can itself select `members`.
    pub fn from_nodes(
        nodes: Vec<OrgData>,
        viewer_site_admin: bool,
        nested: Arc<NestedPrefetch>,
    ) -> Self {
        Self {
            nodes,
            viewer_site_admin,
            nested,
        }
    }
}

impl ObjectValue for OrgConnectionStaticObject {
    fn type_name(&self) -> &'static str {
        "OrgConnection"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "nodes" => Ok(Some(FieldValue::List {
                element_type: Some("Org"),
                items: self
                    .nodes
                    .iter()
                    .map(|org| {
                        FieldValue::Object(Arc::new(
                            OrgObject::from_data_as_viewer(org.clone(), self.viewer_site_admin)
                                .with_nested(self.nested.clone()),
                        ))
                    })
                    .collect(),
            })),
            "totalCount" => Ok(Some(FieldValue::Json(Json::Int(self.nodes.len() as i64)))),
            _ => Ok(None),
        }
    }
}

/// `orgConnectionResolver` (`orgs.go:27-69`): `nodes` and `totalCount` each
/// repeat the site-admin check before returning the prefetched data.
#[derive(Debug, Clone, PartialEq)]
pub struct OrgConnectionObject {
    data: OrganizationsConnectionData,
    viewer_site_admin: bool,
    nested: Arc<NestedPrefetch>,
}

impl OrgConnectionObject {
    pub fn from_data(data: OrganizationsConnectionData, viewer_site_admin: bool) -> Self {
        Self {
            data,
            viewer_site_admin,
            nested: Arc::new(NestedPrefetch::default()),
        }
    }

    /// Attach the request's nested prefetch data (see [`prefetch_nested`]).
    pub fn with_nested(mut self, nested: Arc<NestedPrefetch>) -> Self {
        self.nested = nested;
        self
    }

    /// `auth.CheckCurrentUserIsSiteAdmin`: anonymous is `not authenticated`,
    /// an authenticated non-admin is `must be site admin`.
    fn check_site_admin(&self, actor: Actor) -> Result<(), ExecError> {
        match actor {
            Actor::Internal => Ok(()),
            Actor::Anonymous => Err(ExecError::new(NOT_AUTHENTICATED)),
            Actor::User { .. } if !self.viewer_site_admin => {
                Err(ExecError::new(MUST_BE_SITE_ADMIN))
            }
            Actor::User { .. } => Ok(()),
        }
    }
}

impl ObjectValue for OrgConnectionObject {
    fn type_name(&self) -> &'static str {
        "OrgConnection"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "nodes" => {
                self.check_site_admin(actor)?;
                Ok(Some(FieldValue::List {
                    element_type: Some("Org"),
                    items: self
                        .data
                        .orgs
                        .iter()
                        .map(|org| {
                            FieldValue::Object(Arc::new(
                                OrgObject::from_data_as_viewer(org.clone(), self.viewer_site_admin)
                                    .with_nested(self.nested.clone()),
                            ))
                        })
                        .collect(),
                }))
            }
            "totalCount" => {
                self.check_site_admin(actor)?;
                Ok(Some(FieldValue::Json(Json::Int(
                    self.data.total_count as i64,
                ))))
            }
            _ => Ok(None),
        }
    }
}

/// `RepositoryResolver` (`cmd/frontend/graphqlbackend/repository.go`), limited
/// to the fields present in the row returned by `repoStore.GetByName`.
// `GitPrefetch` holds the symbols client (a trait object), so it cannot be
// compared; `RepositoryObject`'s equality had no callers.
#[derive(Debug, Clone)]
pub struct RepositoryObject {
    data: RepositoryData,
    /// The request's git prefetch data (M9.8ix); `None` leaves `commit` and
    /// `GitRef.target` unported, matching a transport-only resolver.
    git: Option<Arc<crate::git_graphql::GitPrefetch>>,
    /// The request's embeddings snapshot (M9.14c); `None` leaves
    /// `embeddingExists` unported.
    embeddings: Option<Arc<crate::embeddings_graphql::EmbeddingsPrefetch>>,
    /// The request's M9.19a repository object-type snapshot.
    inventory_d: Option<Arc<crate::graphql_inventory_d::InventoryDRoots>>,
    /// The Batch Changes read surface (M9.24c); `None` leaves
    /// `Repository.batchChanges` unported.
    batches: Option<crate::batches_graphql::BatchesHandleWrapper>,
    /// The Code Intel autoindexing surface (M9.28b); `None` leaves
    /// `Repository.indexConfiguration` unported.
    autoindexing: Option<
        crate::codeintel_autoindexing_graphql::CodeIntelAutoindexingHandleWrapper,
    >,
    /// The configuration-policy surface (M9.28c); `None` leaves
    /// `Repository.previewGitObjectFilter` unported.
    codeintel_policies: Option<crate::codeintel_policies_graphql::CodeIntelPoliciesHandle>,
}

impl RepositoryObject {
    pub fn from_data(data: RepositoryData) -> Self {
        Self {
            data,
            git: None,
            embeddings: None,
            inventory_d: None,
            batches: None,
            autoindexing: None,
            codeintel_policies: None,
        }
    }

    /// Attach the Batch Changes read surface (M9.24c).
    pub fn with_batches(mut self, batches: crate::batches_graphql::BatchesHandle) -> Self {
        self.batches = Some(crate::batches_graphql::BatchesHandleWrapper(batches));
        self
    }

    /// Attach the Code Intel autoindexing surface (M9.28b).
    pub fn with_autoindexing(
        mut self,
        autoindexing: Option<
            crate::codeintel_autoindexing_graphql::CodeIntelAutoindexingHandle,
        >,
    ) -> Self {
        self.autoindexing = autoindexing.map(
            crate::codeintel_autoindexing_graphql::CodeIntelAutoindexingHandleWrapper,
        );
        self
    }

    /// Attach the configuration-policy surface (M9.28c).
    pub fn with_codeintel_policies(
        mut self,
        codeintel_policies: Option<crate::codeintel_policies_graphql::CodeIntelPoliciesHandle>,
    ) -> Self {
        self.codeintel_policies = codeintel_policies;
        self
    }

    /// Attach the request's prefetched git data (M9.8ix).
    pub fn with_git(mut self, git: Option<Arc<crate::git_graphql::GitPrefetch>>) -> Self {
        self.git = git;
        self
    }

    /// Attach the request's prefetched embeddings data (M9.14c).
    pub fn with_embeddings(
        mut self,
        embeddings: Option<Arc<crate::embeddings_graphql::EmbeddingsPrefetch>>,
    ) -> Self {
        self.embeddings = embeddings;
        self
    }

    /// Attach the request's prefetched M9.19a object data.
    pub fn with_inventory_d(
        mut self,
        inventory_d: Option<Arc<crate::graphql_inventory_d::InventoryDRoots>>,
    ) -> Self {
        self.inventory_d = inventory_d;
        self
    }

    /// The repository's `(id, name)` as the git resolvers key their data.
    pub(crate) fn minimal_repo(&self) -> crate::search_stream::convert::MinimalRepo {
        crate::search_stream::convert::MinimalRepo {
            id: self.data.id,
            name: self.data.name.clone(),
        }
    }

    /// `relay.MarshalID("Repository", id)`:
    /// `base64.URLEncoding("Repository:" + json(id))`.
    pub fn relay_id(id: i32) -> String {
        base64::engine::general_purpose::URL_SAFE.encode(format!("Repository:{id}"))
    }
}

impl ObjectValue for RepositoryObject {
    fn type_name(&self) -> &'static str {
        "Repository"
    }

    fn field(
        &self,
        field: &str,
        arguments: &Arguments,
        actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            // `Repository.indexConfiguration` (M9.28b).
            "indexConfiguration" => match &self.autoindexing {
                Some(db) => crate::codeintel_autoindexing_graphql::repository_field(
                    &db.0,
                    self.data.id,
                    field,
                    arguments,
                    actor,
                ),
                None => Ok(None),
            },
            // `RepositoryResolver.PreviewGitObjectFilter` (M9.28c).
            "previewGitObjectFilter" => match &self.codeintel_policies {
                Some(database) => crate::codeintel_policies_graphql::preview_git_object_filter(
                    database,
                    self.data.id,
                    arguments,
                )
                .map(Some),
                None => Ok(None),
            },
            // `RepositoryResolver.BatchChanges` (M9.24c).
            "batchChanges" => match &self.batches {
                Some(batches) => crate::batches_graphql::repository_batch_changes(
                    &batches.0,
                    self.data.id,
                    arguments,
                    actor,
                )
                .map(Some),
                None => Ok(None),
            },
            // `RepositoryResolver.EmbeddingExists` (`repository.go:116-122`):
            // the gate and the per-repo lookup are prefetched (M9.14c).
            "embeddingExists" => {
                let Some(embeddings) = &self.embeddings else {
                    return Ok(None);
                };
                if !embeddings.enabled {
                    return Ok(Some(FieldValue::Json(Json::Bool(false))));
                }
                match embeddings.exists.get(&self.data.id) {
                    Some(Ok(exists)) => Ok(Some(FieldValue::Json(Json::Bool(*exists)))),
                    Some(Err(message)) => Err(ExecError::new(message.clone())),
                    // Not prefetched for this object (e.g. a repo reached only
                    // through a git object); report the field as unported.
                    None => Ok(None),
                }
            }
            // `RepositoryResolver.Commit` (`repository.go:296-310`): the rev is
            // resolved with `ResolveRev`; a missing revision is `null`.
            "commit" => {
                let Some(rev) = arguments.string("rev") else {
                    return Ok(None);
                };
                let Some(git) = &self.git else {
                    return Ok(None);
                };
                let input_rev = arguments
                    .string("inputRevspec")
                    .map(str::to_owned)
                    .unwrap_or_else(|| rev.to_owned());
                match git.revisions.get(&(self.data.id, rev.to_owned())) {
                    Some(crate::git_graphql::RevisionLookup::Found(oid)) => {
                        let (commit, commit_error) = git.commit_data(self.data.id, oid);
                        Ok(Some(FieldValue::Object(Arc::new(
                            crate::git_graphql::GitCommitResolverObject {
                                repo: self.minimal_repo(),
                                oid: oid.clone(),
                                input_rev: Some(input_rev),
                                commit,
                                commit_error,
                                git: self.git.clone(),
                            },
                        ))))
                    }
                    Some(crate::git_graphql::RevisionLookup::NotFound) => {
                        Ok(Some(FieldValue::Null))
                    }
                    Some(crate::git_graphql::RevisionLookup::Error(message)) => {
                        Err(ExecError::new(message.clone()))
                    }
                    None => Ok(None),
                }
            }
            // `RepositoryResolver.Comparison` (`repository_comparison.go:132`):
            // `NewRepositoryComparison` is resolved eagerly during prefetch.
            "comparison" => {
                let Some(git) = &self.git else {
                    return Ok(None);
                };
                let base = arguments.string("base").unwrap_or("HEAD").to_owned();
                let head = arguments.string("head").unwrap_or("HEAD").to_owned();
                match git
                    .comparison
                    .repository_comparisons
                    .get(&(self.data.id, base, head))
                {
                    Some(Ok(data)) => Ok(Some(FieldValue::Object(Arc::new(
                        crate::git_commit_extra::RepositoryComparisonObject {
                            repo: self.minimal_repo(),
                            data: data.clone(),
                            git: self.git.clone(),
                        },
                    )))),
                    Some(Err(message)) => Err(ExecError::new(message.clone())),
                    None => Err(ExecError::new("comparison is not prefetched")),
                }
            }
            // `Repository.KeyValuePairs`/`Metadata` (`repository.go:533-552`):
            // both replay the `repo_kvps` rows prefetched into `repo_kvps`.
            "keyValuePairs" | "metadata" => {
                let Some(inventory) = &self.inventory_d else {
                    return Ok(None);
                };
                match inventory.repo_kvps.get(&self.data.id) {
                    Some(Err(message)) => Err(ExecError::new(message.clone())),
                    Some(Ok(rows)) => Ok(Some(FieldValue::List {
                        element_type: Some("KeyValuePair"),
                        items: rows
                            .iter()
                            .map(|row| {
                                FieldValue::Object(Arc::new(
                                    crate::graphql_inventory_d::KeyValuePairObject {
                                        data: row.clone(),
                                    },
                                ))
                            })
                            .collect(),
                    })),
                    None => Ok(None),
                }
            }
            // `Repository.ExternalURLs` (`repository.go:501-507`): only the
            // Phabricator link is available without code-host metadata.
            "externalURLs" => {
                let Some(inventory) = &self.inventory_d else {
                    return Ok(None);
                };
                match inventory.repo_external_links.get(&self.data.id) {
                    Some(Err(message)) => Err(ExecError::new(message.clone())),
                    Some(Ok(links)) => Ok(Some(FieldValue::List {
                        element_type: Some("ExternalLink"),
                        items: links
                            .iter()
                            .map(|link| {
                                FieldValue::Object(Arc::new(
                                    crate::graphql_inventory_d::ExternalLinkObject {
                                        data: link.clone(),
                                    },
                                ))
                            })
                            .collect(),
                    })),
                    None => Ok(None),
                }
            }
            // `Repository.GitRefs`/`Branches`/`Tags`
            // (`repository_git_refs.go:20-74`).
            "gitRefs" | "branches" | "tags" => {
                let (heads_only, tags_only) = if field == "gitRefs" {
                    crate::graphql_inventory_d::refs_type(arguments)
                } else {
                    (field == "branches", field == "tags")
                };
                let first = arguments
                    .i64("first")
                    .and_then(|value| i32::try_from(value).ok());
                match crate::graphql_inventory_d::git_ref_connection(
                    self.minimal_repo(),
                    self.git.clone(),
                    heads_only,
                    tags_only,
                    arguments.string("query"),
                    first,
                )? {
                    Some(connection) => Ok(Some(FieldValue::Object(Arc::new(connection)))),
                    None => Ok(None),
                }
            }
            // `Repository.Contributors` (`repository_contributors.go:25-51`).
            "contributors" => {
                match crate::graphql_inventory_d::contributors_connection(
                    self.minimal_repo(),
                    self.git.clone(),
                    arguments,
                    self.inventory_d.clone(),
                )? {
                    Some(connection) => Ok(Some(FieldValue::Object(Arc::new(connection)))),
                    None => Ok(None),
                }
            }
            // `Repository.TextSearchIndex` (`repository_text_search_index.go:18-27`).
            "textSearchIndex" => {
                let Some(inventory) = &self.inventory_d else {
                    return Ok(None);
                };
                Ok(Some(FieldValue::Object(Arc::new(
                    crate::graphql_inventory_d::RepositoryTextSearchIndexObject {
                        repo: self.minimal_repo(),
                        git: self.git.clone(),
                        zoekt: Arc::new(inventory.zoekt.clone()),
                        inventory_d: self.inventory_d.clone(),
                    },
                ))))
            }
            "id" => Ok(Some(FieldValue::Json(Json::Str(Self::relay_id(
                self.data.id,
            ))))),
            "name" => Ok(Some(FieldValue::Json(Json::Str(self.data.name.clone())))),
            "uri" => Ok(Some(FieldValue::Json(Json::Str(self.data.uri.clone())))),
            "description" => Ok(Some(FieldValue::Json(Json::Str(
                self.data.description.clone(),
            )))),
            "isFork" => Ok(Some(FieldValue::Json(Json::Bool(self.data.fork)))),
            "isArchived" => Ok(Some(FieldValue::Json(Json::Bool(self.data.archived)))),
            "isPrivate" => Ok(Some(FieldValue::Json(Json::Bool(self.data.private)))),
            "stars" => Ok(Some(FieldValue::Json(Json::Int(i64::from(
                self.data.stars,
            ))))),
            // `RepositoryResolver.URL` = `(&url.URL{Path: "/" + name}).String()`.
            "url" => Ok(Some(FieldValue::Json(Json::Str(go_url_path(&format!(
                "/{}",
                self.data.name
            )))))),
            // `UpdatedAt` always returns nil.
            "updatedAt" => Ok(Some(FieldValue::Null)),
            "enabled" => Ok(Some(FieldValue::Json(Json::Bool(true)))),
            // `SourceType`: `external_service_type == "perforce"` is a depot.
            "sourceType" => Ok(Some(FieldValue::Json(Json::Str(
                if self.data.external_service_type == "perforce" {
                    "PERFORCE_DEPOT"
                } else {
                    "GIT_REPOSITORY"
                }
                .to_owned(),
            )))),
            "externalRepository" => Ok(Some(FieldValue::Object(Arc::new(
                ExternalRepositoryObject {
                    id: self.data.external_id.clone(),
                    service_type: self.data.external_service_type.clone(),
                    service_id: self.data.external_service_id.clone(),
                },
            )))),
            _ => Ok(None),
        }
    }
}

/// `api.ExternalRepoSpec` (`ExternalRepository` in the GraphQL schema).
struct ExternalRepositoryObject {
    id: String,
    service_type: String,
    service_id: String,
}

impl ObjectValue for ExternalRepositoryObject {
    fn type_name(&self) -> &'static str {
        "ExternalRepository"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "id" => Ok(Some(FieldValue::Json(Json::Str(self.id.clone())))),
            "serviceType" => Ok(Some(FieldValue::Json(Json::Str(self.service_type.clone())))),
            "serviceID" => Ok(Some(FieldValue::Json(Json::Str(self.service_id.clone())))),
            _ => Ok(None),
        }
    }
}

/// Go's `url.URL{Path: path}.String()` path escaping (`encodePath` mode):
/// unreserved characters and `/` plus the path-allowed reserved characters
/// stay literal; `?`, `#`, `%`, spaces and non-ASCII bytes become uppercase
/// `%XX` escapes.
fn go_url_path(path: &str) -> String {
    let mut escaped = String::with_capacity(path.len());
    for byte in path.bytes() {
        let literal = byte.is_ascii_alphanumeric()
            || matches!(
                byte,
                b'-' | b'_'
                    | b'.'
                    | b'~'
                    | b'$'
                    | b'&'
                    | b'+'
                    | b','
                    | b'/'
                    | b':'
                    | b';'
                    | b'='
                    | b'@'
            );
        if literal {
            escaped.push(char::from(byte));
        } else {
            escaped.push_str(&format!("%{byte:02X}"));
        }
    }
    escaped
}

/// The database seam for `Query.featureFlags` / `Query.featureFlag` and the
/// create/update/delete mutations (`internal/database/feature_flags.go`).
/// `Ok(None)` is the baseline's `sql.ErrNoRows`; every other failure is
/// surfaced verbatim.
#[async_trait]
pub trait FeatureFlagStore: Send + Sync {
    async fn get_feature_flag(&self, name: &str) -> Result<Option<FeatureFlag>, String>;
    async fn get_feature_flags(&self) -> Result<Vec<FeatureFlag>, String>;
    async fn get_overrides_for_flag(&self, name: &str) -> Result<Vec<FeatureFlagOverride>, String>;
    async fn create_feature_flag(
        &self,
        name: &str,
        kind: FeatureFlagKind,
    ) -> Result<FeatureFlag, String>;
    async fn update_feature_flag(
        &self,
        name: &str,
        kind: FeatureFlagKind,
    ) -> Result<FeatureFlag, String>;
    async fn delete_feature_flag(&self, name: &str) -> Result<(), String>;

    /// `GetUserFlags` (`internal/database/feature_flags.go:490-576`).
    async fn get_user_flags(
        &self,
        user_id: i32,
    ) -> Result<std::collections::HashMap<String, bool>, String>;
    /// `GetAnonymousUserFlags` (`internal/database/feature_flags.go:578-591`).
    async fn get_anonymous_flags(
        &self,
        anonymous_uid: &str,
    ) -> Result<std::collections::HashMap<String, bool>, String>;
    /// `GetGlobalFeatureFlags` (`internal/database/feature_flags.go:592-604`).
    async fn get_global_flags(&self) -> Result<std::collections::HashMap<String, bool>, String>;
    /// `GetOrgOverrideForFlag` (`internal/database/feature_flags.go:441-462`).
    async fn get_org_override_for_flag(
        &self,
        org_id: i32,
        flag_name: &str,
    ) -> Result<Option<FeatureFlagOverride>, String>;
    /// `GetOrgOverridesForUser` (`internal/database/feature_flags.go:416-438`).
    async fn get_org_overrides_for_user(
        &self,
        user_id: i32,
    ) -> Result<Vec<FeatureFlagOverride>, String>;
    /// `CreateOverride` (`internal/database/feature_flags.go:278-314`).
    async fn create_override(
        &self,
        user_id: Option<i32>,
        org_id: Option<i32>,
        flag_name: &str,
        value: bool,
    ) -> Result<FeatureFlagOverride, String>;
    /// `UpdateOverride` (`internal/database/feature_flags.go:340-370`).
    async fn update_override(
        &self,
        user_id: Option<i32>,
        org_id: Option<i32>,
        flag_name: &str,
        value: bool,
    ) -> Result<FeatureFlagOverride, String>;
    /// `DeleteOverride` (`internal/database/feature_flags.go:316-338`).
    async fn delete_override(
        &self,
        user_id: Option<i32>,
        org_id: Option<i32>,
        flag_name: &str,
    ) -> Result<(), String>;
}

/// One `FeatureFlagOverride` plus the prefetched `namespace` lookup
/// (`FeatureFlagOverrideResolver.Namespace`).
#[derive(Debug, Clone, PartialEq)]
pub struct FeatureFlagOverrideData {
    pub user_id: Option<i32>,
    pub org_id: Option<i32>,
    pub flag_name: String,
    pub value: bool,
    pub namespace: NamespaceLookup,
}

/// One `featureflag.FeatureFlag` plus its overrides. `overrides` keeps the
/// database error so it is only surfaced when the field is selected, like the
/// baseline's lazy `GetOverridesForFlag`.
#[derive(Debug, Clone, PartialEq)]
pub struct FeatureFlagData {
    pub name: String,
    pub bool_value: Option<bool>,
    pub rollout: Option<i32>,
    pub created_at: OffsetDateTime,
    pub updated_at: OffsetDateTime,
    pub overrides: Result<Vec<FeatureFlagOverrideData>, String>,
}

/// Prefetched flags keyed by `flag_name`, shared by the root fields and by
/// `FeatureFlagOverride.targetFlag` (which reuses the same request snapshot).
#[derive(Debug, Clone, Default, PartialEq)]
pub struct FeatureFlagRegistry {
    pub flags: HashMap<String, FeatureFlagData>,
}

/// One prefetched `Query.featureFlags` result: flag names in database order,
/// or the root error.
#[derive(Debug, Clone, PartialEq)]
pub enum FeatureFlagsLookup {
    Found(Vec<String>),
    Error(String),
}

/// One prefetched `Query.featureFlag(name)` result: the canonical flag name
/// (a key into [`FeatureFlagRegistry`]) or the root error.
#[derive(Debug, Clone, PartialEq)]
pub enum FeatureFlagLookup {
    Found(String),
    Error(String),
}

/// Prefetched `Query.featureFlags` / `Query.featureFlag` results.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct FeatureFlagRoots {
    pub flags: Option<FeatureFlagsLookup>,
    pub flag: HashMap<String, FeatureFlagLookup>,
    pub registry: Arc<FeatureFlagRegistry>,
}

/// `auth.CheckCurrentUserIsSiteAdmin` (`internal/auth/site_admin.go:20-39`).
pub(crate) fn check_current_user_is_site_admin(
    actor: Actor,
    viewer_site_admin: bool,
) -> Result<(), String> {
    match actor {
        // `CheckCurrentUserIsSiteAdmin` returns nil for internal actors
        // (`internal/auth/site_admin.go:19-21`).
        Actor::Internal => Ok(()),
        Actor::Anonymous => Err(NOT_AUTHENTICATED.to_owned()),
        Actor::User { .. } if !viewer_site_admin => Err(MUST_BE_SITE_ADMIN.to_owned()),
        Actor::User { .. } => Ok(()),
    }
}

/// Prefetch every `Query.featureFlags` / `Query.featureFlag` field in
/// `fields`. Both roots are site-admin gated before any database access.
pub async fn prefetch_feature_flags(
    flags_store: &dyn FeatureFlagStore,
    namespaces: &dyn OrgMembershipsStore,
    fields: &[RootField],
    actor: Actor,
    viewer_site_admin: bool,
) -> FeatureFlagRoots {
    let mut registry: HashMap<String, FeatureFlagData> = HashMap::new();
    let mut roots = FeatureFlagRoots::default();
    for field in fields {
        match field.name.as_str() {
            "featureFlags" => {
                if roots.flags.is_some() {
                    continue;
                }
                if let Err(message) = check_current_user_is_site_admin(actor, viewer_site_admin) {
                    roots.flags = Some(FeatureFlagsLookup::Error(message));
                    continue;
                }
                match flags_store.get_feature_flags().await {
                    Ok(flags) => {
                        let mut names = Vec::with_capacity(flags.len());
                        for flag in flags {
                            let name = flag.name.clone();
                            if !registry.contains_key(&name) {
                                let data = feature_flag_data(
                                    flags_store,
                                    namespaces,
                                    flag,
                                    actor,
                                    viewer_site_admin,
                                )
                                .await;
                                registry.insert(name.clone(), data);
                            }
                            names.push(name);
                        }
                        roots.flags = Some(FeatureFlagsLookup::Found(names));
                    }
                    Err(message) => roots.flags = Some(FeatureFlagsLookup::Error(message)),
                }
            }
            "featureFlag" => {
                let Some(name) = field.arguments.string("name") else {
                    continue;
                };
                if roots.flag.contains_key(name) {
                    continue;
                }
                if let Err(message) = check_current_user_is_site_admin(actor, viewer_site_admin) {
                    roots
                        .flag
                        .insert(name.to_owned(), FeatureFlagLookup::Error(message));
                    continue;
                }
                match flags_store.get_feature_flag(name).await {
                    Ok(Some(flag)) => {
                        let flag_name = flag.name.clone();
                        if !registry.contains_key(&flag_name) {
                            let data = feature_flag_data(
                                flags_store,
                                namespaces,
                                flag,
                                actor,
                                viewer_site_admin,
                            )
                            .await;
                            registry.insert(flag_name.clone(), data);
                        }
                        roots
                            .flag
                            .insert(name.to_owned(), FeatureFlagLookup::Found(flag_name));
                    }
                    // `scanFeatureFlag` returns `sql.ErrNoRows`.
                    Ok(None) => {
                        roots.flag.insert(
                            name.to_owned(),
                            FeatureFlagLookup::Error("sql: no rows in result set".to_owned()),
                        );
                    }
                    Err(message) => {
                        roots
                            .flag
                            .insert(name.to_owned(), FeatureFlagLookup::Error(message));
                    }
                };
            }
            _ => {}
        }
    }
    roots.registry = Arc::new(FeatureFlagRegistry { flags: registry });
    roots
}

/// Load one flag's overrides and their `User`/`Org` namespaces. An overrides
/// query failure is kept for the `overrides` field; a missing namespace is
/// the baseline's `UserByIDInt32`/`OrgByIDInt32` error.
pub(crate) async fn feature_flag_data(
    flags_store: &dyn FeatureFlagStore,
    namespaces: &dyn OrgMembershipsStore,
    flag: FeatureFlag,
    actor: Actor,
    viewer_site_admin: bool,
) -> FeatureFlagData {
    let overrides = match flags_store.get_overrides_for_flag(&flag.name).await {
        Ok(overrides) => {
            let mut data = Vec::with_capacity(overrides.len());
            for override_row in overrides {
                let namespace = match (override_row.user_id, override_row.org_id) {
                    (Some(user_id), _) => match namespaces.user_by_id(user_id).await {
                        Ok(user) => NamespaceLookup::User(user),
                        Err(message) => NamespaceLookup::Error(message),
                    },
                    (None, Some(org_id)) => {
                        match namespaces
                            .org_by_id(org_id, viewer_uid(actor), viewer_site_admin)
                            .await
                        {
                            Ok(org) => NamespaceLookup::Org(org),
                            Err(message) => NamespaceLookup::Error(message),
                        }
                    }
                    (None, None) => {
                        NamespaceLookup::Error("one of userID or orgID must be set".to_owned())
                    }
                };
                data.push(FeatureFlagOverrideData {
                    user_id: override_row.user_id,
                    org_id: override_row.org_id,
                    flag_name: override_row.flag_name,
                    value: override_row.value,
                    namespace,
                });
            }
            Ok(data)
        }
        Err(message) => Err(message),
    };
    FeatureFlagData {
        name: flag.name,
        bool_value: flag.bool_value,
        rollout: flag.rollout,
        created_at: flag.created_at,
        updated_at: flag.updated_at,
        overrides,
    }
}

/// `relay.MarshalID("FeatureFlagOverride", overrideSpec{...})`
/// (`feature_flags.go:110-130`): Go's `json.Marshal` field order is `UserID`,
/// `OrgID`, `FlagName`; nil pointers are `null`.
pub(crate) fn marshal_feature_flag_override_id(
    user_id: Option<i32>,
    org_id: Option<i32>,
    flag_name: &str,
) -> String {
    let user_id = user_id.map_or_else(|| "null".to_owned(), |id| id.to_string());
    let org_id = org_id.map_or_else(|| "null".to_owned(), |id| id.to_string());
    let flag_name = match serde_json::to_string(flag_name) {
        Ok(encoded) => encoded,
        Err(_) => "\"\"".to_owned(),
    };
    base64::engine::general_purpose::URL_SAFE.encode(format!(
        "FeatureFlagOverride:{{\"UserID\":{user_id},\"OrgID\":{org_id},\"FlagName\":{flag_name}}}"
    ))
}

/// The concrete `FeatureFlagBoolean` / `FeatureFlagRollout` union members
/// (`feature_flags.go:19-80`).
pub struct FeatureFlagObject {
    data: FeatureFlagData,
    registry: Arc<FeatureFlagRegistry>,
}

impl FeatureFlagObject {
    pub(crate) fn new(data: FeatureFlagData, registry: Arc<FeatureFlagRegistry>) -> Self {
        Self { data, registry }
    }
}

impl ObjectValue for FeatureFlagObject {
    fn type_name(&self) -> &'static str {
        if self.data.bool_value.is_some() {
            "FeatureFlagBoolean"
        } else {
            "FeatureFlagRollout"
        }
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "name" => Ok(Some(FieldValue::Json(Json::Str(self.data.name.clone())))),
            "value" => match self.data.bool_value {
                Some(value) => Ok(Some(FieldValue::Json(Json::Bool(value)))),
                None => Ok(None),
            },
            "rolloutBasisPoints" => match self.data.rollout {
                Some(rollout) => Ok(Some(FieldValue::Json(Json::Int(rollout as i64)))),
                None => Ok(None),
            },
            "createdAt" => Ok(Some(FieldValue::Json(Json::Str(datetime_rfc3339(
                self.data.created_at,
            ))))),
            "updatedAt" => Ok(Some(FieldValue::Json(Json::Str(datetime_rfc3339(
                self.data.updated_at,
            ))))),
            "overrides" => match &self.data.overrides {
                Ok(overrides) => Ok(Some(FieldValue::List {
                    element_type: Some("FeatureFlagOverride"),
                    items: overrides
                        .iter()
                        .map(|override_data| {
                            FieldValue::Object(Arc::new(FeatureFlagOverrideObject {
                                data: override_data.clone(),
                                registry: self.registry.clone(),
                            }))
                        })
                        .collect(),
                })),
                Err(message) => Err(ExecError::new(message.clone())),
            },
            _ => Ok(None),
        }
    }
}

/// `FeatureFlagOverrideResolver` (`feature_flags.go:90-116`).
pub struct FeatureFlagOverrideObject {
    data: FeatureFlagOverrideData,
    registry: Arc<FeatureFlagRegistry>,
}

impl ObjectValue for FeatureFlagOverrideObject {
    fn type_name(&self) -> &'static str {
        "FeatureFlagOverride"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "id" => Ok(Some(FieldValue::Json(Json::Str(
                marshal_feature_flag_override_id(
                    self.data.user_id,
                    self.data.org_id,
                    &self.data.flag_name,
                ),
            )))),
            "value" => Ok(Some(FieldValue::Json(Json::Bool(self.data.value)))),
            "namespace" => match &self.data.namespace {
                NamespaceLookup::User(data) => Ok(Some(FieldValue::Object(Arc::new(
                    UserObject::from_data_as_viewer(data.clone(), false),
                )))),
                NamespaceLookup::Org(data) => Ok(Some(FieldValue::Object(Arc::new(
                    OrgObject::from_data_as_viewer(data.clone(), false),
                )))),
                NamespaceLookup::NotFound => Err(ExecError::new("namespace not found")),
                NamespaceLookup::Error(message) => Err(ExecError::new(message.clone())),
            },
            // `FeatureFlagOverrideResolver.TargetFlag` re-queries the owning
            // flag; the request snapshot already contains it.
            "targetFlag" => match self.registry.flags.get(&self.data.flag_name) {
                Some(data) => Ok(Some(FieldValue::Object(Arc::new(FeatureFlagObject {
                    data: data.clone(),
                    registry: self.registry.clone(),
                })))),
                None => Err(ExecError::new("sql: no rows in result set")),
            },
            _ => Ok(None),
        }
    }
}

// ---------------------------------------------------------------------------
// Search contexts GraphQL executor wiring (M9.8hz)
// ---------------------------------------------------------------------------

/// `actor.FromContext` + `dotcom.SourcegraphDotComMode()` as the
/// [`RequestContext`] the M9.8hq resolver expects.
fn search_contexts_request_context(actor: Actor) -> RequestContext {
    let getenv = |key: &str| std::env::var(key).ok();
    RequestContext {
        actor_uid: actor.uid().unwrap_or(0),
        internal: false,
        sourcegraph_dot_com_mode: crate::service_connections::sourcegraph_dotcom_mode(&getenv),
    }
}

fn input_string(value: &InputValue) -> Option<String> {
    value.as_str().map(str::to_owned)
}

fn required_input_string(
    map: &BTreeMap<String, InputValue>,
    name: &str,
) -> Result<String, ExecError> {
    match map.get(name).and_then(InputValue::as_str) {
        Some(value) => Ok(value.to_owned()),
        None => Err(ExecError::new(format!("missing required argument: {name}"))),
    }
}

fn required_input_bool(map: &BTreeMap<String, InputValue>, name: &str) -> Result<bool, ExecError> {
    match map.get(name).and_then(InputValue::as_bool) {
        Some(value) => Ok(value),
        None => Err(ExecError::new(format!("missing required argument: {name}"))),
    }
}

fn input_string_list(value: Option<&InputValue>) -> Result<Vec<String>, ExecError> {
    match value {
        None | Some(InputValue::Null) => Ok(Vec::new()),
        Some(InputValue::List(items)) => items
            .iter()
            .map(|item| input_string(item).ok_or_else(|| ExecError::new("invalid list argument")))
            .collect(),
        Some(_) => Err(ExecError::new("invalid list argument")),
    }
}

fn input_optional_string_list(
    value: Option<&InputValue>,
) -> Result<Vec<Option<String>>, ExecError> {
    match value {
        None | Some(InputValue::Null) => Ok(Vec::new()),
        Some(InputValue::List(items)) => items
            .iter()
            .map(|item| match item {
                InputValue::Null => Ok(None),
                other => input_string(other)
                    .map(Some)
                    .ok_or_else(|| ExecError::new("invalid namespaces argument")),
            })
            .collect(),
        Some(_) => Err(ExecError::new("invalid namespaces argument")),
    }
}

fn input_order_by(value: Option<&InputValue>) -> Option<SearchContextsOrderBy> {
    match value.and_then(InputValue::as_str) {
        Some("SEARCH_CONTEXT_SPEC") => Some(SearchContextsOrderBy::Spec),
        Some("SEARCH_CONTEXT_UPDATED_AT") => Some(SearchContextsOrderBy::UpdatedAt),
        _ => None,
    }
}

fn search_context_create_input(
    map: &BTreeMap<String, InputValue>,
) -> Result<SearchContextInputArgs, ExecError> {
    Ok(SearchContextInputArgs {
        name: required_input_string(map, "name")?,
        description: required_input_string(map, "description")?,
        public: required_input_bool(map, "public")?,
        namespace: map
            .get("namespace")
            .and_then(InputValue::as_str)
            .map(str::to_owned),
        query: required_input_string(map, "query")?,
    })
}

fn search_context_edit_input(
    map: &BTreeMap<String, InputValue>,
) -> Result<SearchContextEditInputArgs, ExecError> {
    Ok(SearchContextEditInputArgs {
        name: required_input_string(map, "name")?,
        description: required_input_string(map, "description")?,
        public: required_input_bool(map, "public")?,
        query: required_input_string(map, "query")?,
    })
}

fn search_context_repositories_input(
    value: Option<&InputValue>,
) -> Result<Vec<SearchContextRepositoryRevisionsInputArgs>, ExecError> {
    let items = match value {
        None | Some(InputValue::Null) => return Ok(Vec::new()),
        Some(InputValue::List(items)) => items,
        Some(_) => return Err(ExecError::new("invalid repositories argument")),
    };
    items
        .iter()
        .map(|item| {
            let InputValue::Object(map) = item else {
                return Err(ExecError::new("invalid repositories argument"));
            };
            Ok(SearchContextRepositoryRevisionsInputArgs {
                repository_id: required_input_string(map, "repositoryID")?,
                revisions: input_string_list(map.get("revisions"))?,
            })
        })
        .collect()
}

/// Map the executor's resolved [`Arguments`] (schema defaults already applied)
/// onto the M9.8hu dispatch struct.
fn search_contexts_field_args(arguments: &Arguments) -> Result<SearchContextsFieldArgs, ExecError> {
    let mut args = SearchContextsFieldArgs {
        id: arguments.string("id").map(str::to_owned),
        spec: arguments.string("spec").map(str::to_owned),
        first: arguments.i64("first").map(|first| first as i32),
        after: arguments.string("after").map(str::to_owned),
        query: arguments.string("query").map(str::to_owned),
        namespaces: input_optional_string_list(arguments.get("namespaces"))?,
        order_by: input_order_by(arguments.get("orderBy")),
        descending: arguments.bool("descending"),
        search_context: None,
        search_context_edit: None,
        repositories: search_context_repositories_input(arguments.get("repositories"))?,
        search_context_id: arguments.string("searchContextID").map(str::to_owned),
        user_id: arguments.string("userID").map(str::to_owned),
    };
    if let Some(InputValue::Object(map)) = arguments.get("searchContext") {
        // Create and update share the argument name but use different input
        // types; populate both and let the dispatch pick the right one.
        args.search_context = Some(search_context_create_input(map)?);
        args.search_context_edit = Some(search_context_edit_input(map)?);
    }
    Ok(args)
}

/// Dispatch one search-contexts root field through the M9.8hu registration
/// layer and convert the borrowed resolver values into owned snapshots.
fn dispatch_search_context_field(
    db: Option<&SearchContextsDatabaseHandle>,
    operation: SearchContextsOperation,
    field: &str,
    arguments: &Arguments,
    actor: Actor,
) -> Result<Option<FieldValue>, ExecError> {
    let Some(db) = db else {
        return Ok(None);
    };
    let ctx = search_contexts_request_context(actor);
    let args = search_contexts_field_args(arguments)?;
    let dispatch = SearchContextsResolverDispatch::new(db.as_ref());
    match dispatch.field(operation, field, &args, &ctx) {
        Ok(Some(value)) => Ok(Some(search_contexts_field_value(&ctx, value))),
        Ok(None) => Ok(None),
        Err(error) => Err(ExecError::new(error.to_string())),
    }
}

fn search_contexts_field_value(
    ctx: &RequestContext,
    value: SearchContextsFieldValue<'_>,
) -> FieldValue {
    match value {
        SearchContextsFieldValue::SearchContext(resolver) => {
            FieldValue::Object(search_context_snapshot(ctx, &resolver))
        }
        SearchContextsFieldValue::Connection(connection) => {
            FieldValue::Object(Arc::new(SearchContextConnectionObject {
                nodes: connection
                    .nodes()
                    .iter()
                    .map(|resolver| search_context_snapshot(ctx, resolver))
                    .collect(),
                total_count: connection.total_count(),
                page_info: connection.page_info(),
            }))
        }
        SearchContextsFieldValue::Bool(value) => FieldValue::Json(Json::Bool(value)),
        SearchContextsFieldValue::EmptyResponse => {
            FieldValue::Object(Arc::new(crate::temporary_settings::EmptyResponseObject))
        }
    }
}

/// Owned snapshot of `searchContextResolver`. The M9.8hq resolver borrows the
/// database and cannot be stored in `Arc<dyn ObjectValue>`; namespace and
/// repository lookup errors are captured here and surface only when those
/// fields are selected.
fn search_context_snapshot(
    ctx: &RequestContext,
    resolver: &SearchContextResolver<'_>,
) -> Arc<SearchContextObject> {
    Arc::new(SearchContextObject {
        id: resolver.id(),
        name: resolver.name().to_owned(),
        description: resolver.description().to_owned(),
        spec: resolver.spec(),
        auto_defined: resolver.auto_defined(),
        query: resolver.query().to_owned(),
        public: resolver.public(),
        updated_at: crate::search_contexts_graphql::format_gqlutil_datetime(resolver.updated_at()),
        viewer_can_manage: resolver.viewer_can_manage(ctx),
        viewer_has_as_default: resolver.viewer_has_as_default(),
        viewer_has_starred: resolver.viewer_has_starred(),
        namespace: resolver.namespace(ctx).map_err(|error| error.to_string()),
        repositories: resolver.repositories().map_err(|error| error.to_string()),
    })
}

struct SearchContextObject {
    id: String,
    name: String,
    description: String,
    spec: String,
    auto_defined: bool,
    query: String,
    public: bool,
    updated_at: String,
    viewer_can_manage: bool,
    viewer_has_as_default: bool,
    viewer_has_starred: bool,
    namespace: Result<Option<NamespaceResolver>, String>,
    repositories: Result<Vec<SearchContextRepositoryRevisionsResolver>, String>,
}

impl ObjectValue for SearchContextObject {
    fn type_name(&self) -> &'static str {
        "SearchContext"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "id" => Ok(Some(FieldValue::Json(Json::Str(self.id.clone())))),
            "name" => Ok(Some(FieldValue::Json(Json::Str(self.name.clone())))),
            "namespace" => match &self.namespace {
                Ok(None) => Ok(Some(FieldValue::Null)),
                Ok(Some(namespace)) => Ok(Some(FieldValue::Object(Arc::new(NamespaceObject {
                    namespace: namespace.clone(),
                })))),
                Err(message) => Err(ExecError::new(message.clone())),
            },
            "description" => Ok(Some(FieldValue::Json(Json::Str(self.description.clone())))),
            "spec" => Ok(Some(FieldValue::Json(Json::Str(self.spec.clone())))),
            "autoDefined" => Ok(Some(FieldValue::Json(Json::Bool(self.auto_defined)))),
            "query" => Ok(Some(FieldValue::Json(Json::Str(self.query.clone())))),
            "repositories" => match &self.repositories {
                Ok(repositories) => Ok(Some(FieldValue::List {
                    items: repositories
                        .iter()
                        .map(|revisions| {
                            FieldValue::Object(Arc::new(SearchContextRepositoryRevisionsObject {
                                repository: revisions.repository().clone(),
                                revisions: revisions.revisions().to_vec(),
                            }))
                        })
                        .collect(),
                    element_type: Some("SearchContextRepositoryRevisions"),
                })),
                Err(message) => Err(ExecError::new(message.clone())),
            },
            "public" => Ok(Some(FieldValue::Json(Json::Bool(self.public)))),
            "updatedAt" => Ok(Some(FieldValue::Json(Json::Str(self.updated_at.clone())))),
            "viewerCanManage" => Ok(Some(FieldValue::Json(Json::Bool(self.viewer_can_manage)))),
            "viewerHasAsDefault" => Ok(Some(FieldValue::Json(Json::Bool(
                self.viewer_has_as_default,
            )))),
            "viewerHasStarred" => Ok(Some(FieldValue::Json(Json::Bool(self.viewer_has_starred)))),
            _ => Ok(None),
        }
    }
}

/// `Namespace` interface value. The M9.8hq database seam exposes only the
/// `id`/`username` (user) or `id`/`name` (org) columns, so fields beyond the
/// interface contract are not ported here.
struct NamespaceObject {
    namespace: NamespaceResolver,
}

impl ObjectValue for NamespaceObject {
    fn type_name(&self) -> &'static str {
        match self.namespace {
            NamespaceResolver::User(_) => "User",
            NamespaceResolver::Org(_) => "Org",
        }
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "id" => Ok(Some(FieldValue::Json(Json::Str(self.namespace.id())))),
            "namespaceName" => Ok(Some(FieldValue::Json(Json::Str(
                self.namespace.namespace_name().to_owned(),
            )))),
            "url" => Ok(Some(FieldValue::Json(Json::Str(self.namespace.url())))),
            "username" => Ok(self
                .namespace
                .as_user()
                .map(|user| FieldValue::Json(Json::Str(user.username.clone())))),
            "name" => Ok(self
                .namespace
                .as_org()
                .map(|org| FieldValue::Json(Json::Str(org.name.clone())))),
            _ => Ok(None),
        }
    }
}

struct SearchContextConnectionObject {
    nodes: Vec<Arc<SearchContextObject>>,
    total_count: i32,
    page_info: PageInfo,
}

impl ObjectValue for SearchContextConnectionObject {
    fn type_name(&self) -> &'static str {
        "SearchContextConnection"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "nodes" => Ok(Some(FieldValue::List {
                items: self
                    .nodes
                    .iter()
                    .map(|node| FieldValue::Object(node.clone()))
                    .collect(),
                element_type: Some("SearchContext"),
            })),
            "totalCount" => Ok(Some(FieldValue::Json(Json::Int(i64::from(
                self.total_count,
            ))))),
            "pageInfo" => Ok(Some(FieldValue::Object(Arc::new(PageInfoObject::new(
                self.page_info.has_next_page,
                self.page_info.end_cursor.clone(),
            ))))),
            _ => Ok(None),
        }
    }
}

struct SearchContextRepositoryRevisionsObject {
    repository: RepositoryResolver,
    revisions: Vec<String>,
}

impl ObjectValue for SearchContextRepositoryRevisionsObject {
    fn type_name(&self) -> &'static str {
        "SearchContextRepositoryRevisions"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "repository" => Ok(Some(FieldValue::Object(Arc::new(
                RepositorySummaryObject {
                    repository: self.repository.clone(),
                },
            )))),
            "revisions" => Ok(Some(FieldValue::List {
                items: self
                    .revisions
                    .iter()
                    .map(|revision| FieldValue::Json(Json::Str(revision.clone())))
                    .collect(),
                element_type: Some("String"),
            })),
            _ => Ok(None),
        }
    }
}

/// `MinimalRepositoryResolver`-backed `Repository` value (the M9.8hq seam only
/// carries `id` and `name`).
struct RepositorySummaryObject {
    repository: RepositoryResolver,
}

impl ObjectValue for RepositorySummaryObject {
    fn type_name(&self) -> &'static str {
        "Repository"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "id" => Ok(Some(FieldValue::Json(Json::Str(self.repository.id())))),
            "name" => Ok(Some(FieldValue::Json(Json::Str(
                self.repository.name().to_owned(),
            )))),
            "url" => Ok(Some(FieldValue::Json(Json::Str(self.repository.url())))),
            _ => Ok(None),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::Map;
    use sg_graphql::exec::{execute_with, ExecRequest};
    use sg_graphql::schema::assemble_sdl;

    fn schema() -> sg_graphql::Schema {
        assemble_sdl(
            "type Query { currentUser: User \
                          user(username: String, email: String, databaseID: Int): User \
                          users(first: Int, after: String, query: String, \
                                activePeriod: UserActivePeriod, inactiveSince: DateTime): UserConnection! \
                          organization(name: String!): Org \
                          organizations(first: Int, query: String): OrgConnection! \
                          node(id: ID!): Node \
                          namespace(id: ID!): Namespace \
                          namespaceByName(name: String!): Namespace \
                          parseSearchQuery(query: String, patternType: SearchPatternType, \
                                           outputPhase: SearchQueryOutputPhase, \
                                           outputFormat: SearchQueryOutputFormat, \
                                           outputVerbosity: SearchQueryOutputVerbosity): String! \
                          clientConfiguration: ClientConfigurationDetails! \
                          other: String } \
             type ClientConfigurationDetails { \
                 contentScriptUrls: [String!]! \
                 parentSourcegraph: ParentSourcegraphDetails! } \
             type ParentSourcegraphDetails { url: String! } \
             interface Node { id: ID! } \
             interface Namespace { id: ID! namespaceName: String! url: String! } \
             type Repository implements Node { id: ID! name: String! \
                                               uri: String! isPrivate: Boolean! } \
             type User implements Node & Namespace { id: ID! databaseID: Int! username: String! \
                         namespaceName: String! \
                         displayName: String avatarURL: String \
                         url: String! settingsURL: String \
                         createdAt: DateTime! updatedAt: DateTime \
                         siteAdmin: Boolean! tosAccepted: Boolean! \
                         builtinAuth: Boolean! email: String! \
                         viewerCanAdminister: Boolean! \
                         organizations: OrgConnection! \
                         organizationMemberships: OrganizationMembershipConnection! } \
             type OrganizationMembershipConnection { \
                 nodes: [OrganizationMembership!]! totalCount: Int! } \
             type OrganizationMembership { organization: Org! user: User! \
                                          createdAt: DateTime! updatedAt: DateTime! } \
             type UserConnection { nodes: [User!]! totalCount: Int! pageInfo: PageInfo! } \
             type PageInfo { endCursor: String hasNextPage: Boolean! } \
             enum UserActivePeriod { TODAY THIS_WEEK THIS_MONTH ALL_TIME } \
             type Org implements Node & Namespace { id: ID! name: String! displayName: String \
                        createdAt: DateTime! url: String! settingsURL: String \
                        namespaceName: String! viewerCanAdminister: Boolean! \
                        viewerIsMember: Boolean! \
                        members(first: Int, last: Int, after: String, before: String, \
                                query: String): NewUsersConnection! } \
             type OrgConnection { nodes: [Org!]! totalCount: Int! } \
             type NewUsersConnection { nodes: [User!]! totalCount: Int! \
                                        pageInfo: ConnectionPageInfo! } \
             type ConnectionPageInfo { endCursor: String startCursor: String \
                                       hasNextPage: Boolean! hasPreviousPage: Boolean! } \
             enum SearchPatternType { standard literal regexp structural keyword } \
             enum SearchQueryOutputPhase { PARSE_TREE JOB_TREE } \
             enum SearchQueryOutputFormat { JSON SEXP MERMAID } \
             enum SearchQueryOutputVerbosity { MINIMAL BASIC MAXIMAL } \
             scalar DateTime",
        )
        .expect("schema")
    }

    fn run(
        schema: &sg_graphql::Schema,
        query: &str,
        actor: Actor,
        roots: &FrontendRootResolver,
    ) -> String {
        let variables = Map::new();
        execute_with(
            schema,
            ExecRequest {
                query,
                operation_name: "",
                variables: &variables,
                actor,
            },
            Some(roots),
        )
        .to_json()
    }

    fn at(s: &str) -> OffsetDateTime {
        use time::format_description::well_known::Rfc3339;
        OffsetDateTime::parse(s, &Rfc3339).unwrap()
    }

    fn user() -> UserObject {
        UserObject::from_data(UserData {
            id: 1,
            username: "alice".to_owned(),
            display_name: "Alice A.".to_owned(),
            avatar_url: "https://example.com/a.png".to_owned(),
            created_at: at("2026-01-02T03:04:05.123456789Z"),
            updated_at: at("2026-02-03T04:05:06+02:00"),
            site_admin: true,
            tos_accepted: true,
            primary_email: "alice@example.com".to_owned(),
        })
    }

    #[test]
    fn anonymous_current_user_is_null() {
        let schema = schema();
        assert_eq!(
            run(
                &schema,
                "{ currentUser { id username } }",
                Actor::Anonymous,
                &FrontendRootResolver::new()
            ),
            r#"{"data":{"currentUser":null}}"#
        );
    }

    #[test]
    fn parse_search_query_uses_schema_defaults_and_returns_the_tree() {
        let schema = schema();
        let response = run(
            &schema,
            r#"{ parseSearchQuery(query: "foo") }"#,
            Actor::Anonymous,
            &FrontendRootResolver::new(),
        );
        let value: serde_json::Value = serde_json::from_str(&response).unwrap();
        assert_eq!(
            value["data"]["parseSearchQuery"],
            serde_json::Value::String(
                r#"[{"value":"foo","negated":false,"labels":["Literal"],"range":{"start":{"line":0,"column":0},"end":{"line":0,"column":3}}}]"#
                    .to_owned()
            )
        );
    }

    #[test]
    fn parse_search_query_reports_unsupported_output_options() {
        let schema = schema();
        let response = run(
            &schema,
            r#"{ parseSearchQuery(query: "foo", outputFormat: SEXP) }"#,
            Actor::Anonymous,
            &FrontendRootResolver::new(),
        );
        let value: serde_json::Value = serde_json::from_str(&response).unwrap();
        assert_eq!(
            value["errors"][0]["message"],
            serde_json::Value::String(
                "unsupported output options for PARSE_TREE, only JSON output with BASIC verbosity is supported"
                    .to_owned()
            )
        );
    }

    #[test]
    fn client_configuration_returns_urls_and_the_parent_instance() {
        let schema = schema();
        let roots = FrontendRootResolver::new().with_client_configuration(
            crate::client_configuration::ClientConfigurationLookup::Found(
                crate::client_configuration::ClientConfigurationDetails {
                    content_script_urls: vec![
                        "https://github.example.com".to_owned(),
                        "https://gitlab.example.com".to_owned(),
                    ],
                    parent_sourcegraph_url: "https://sourcegraph.example.com".to_owned(),
                },
            ),
        );
        assert_eq!(
            run(
                &schema,
                "{ clientConfiguration { contentScriptUrls parentSourcegraph { url } } }",
                Actor::Anonymous,
                &roots,
            ),
            r#"{"data":{"clientConfiguration":{"contentScriptUrls":["https://github.example.com","https://gitlab.example.com"],"parentSourcegraph":{"url":"https://sourcegraph.example.com"}}}}"#
        );
    }

    #[test]
    fn client_configuration_errors_are_surfaced() {
        let schema = schema();
        let roots = FrontendRootResolver::new().with_client_configuration(
            crate::client_configuration::ClientConfigurationLookup::Error(
                "failed to parse JSON: [ValueExpected]".to_owned(),
            ),
        );
        let response = run(
            &schema,
            "{ clientConfiguration { contentScriptUrls } }",
            Actor::Anonymous,
            &roots,
        );
        let value: serde_json::Value = serde_json::from_str(&response).unwrap();
        assert_eq!(
            value["errors"][0]["message"],
            serde_json::Value::String("failed to parse JSON: [ValueExpected]".to_owned())
        );
    }

    #[test]
    fn authenticated_current_user_without_row_is_null() {
        // `GetByCurrentAuthUser` maps not-found to `(nil, nil)`.
        let schema = schema();
        assert_eq!(
            run(
                &schema,
                "{ currentUser { id } }",
                Actor::User { uid: 1 },
                &FrontendRootResolver::new()
            ),
            r#"{"data":{"currentUser":null}}"#
        );
    }

    #[test]
    fn authenticated_current_user_resolves_basic_fields() {
        let schema = schema();
        assert_eq!(
            run(
                &schema,
                "{ currentUser { id databaseID username displayName avatarURL } }",
                Actor::User { uid: 1 },
                &FrontendRootResolver::with_user(Some(user()))
            ),
            r#"{"data":{"currentUser":{"id":"VXNlcjox","databaseID":1,"username":"alice","displayName":"Alice A.","avatarURL":"https://example.com/a.png"}}}"#
        );
    }

    #[test]
    fn empty_display_name_and_avatar_are_null() {
        let schema = schema();
        let user = UserObject::from_data(UserData {
            id: 123,
            username: "bob".to_owned(),
            display_name: String::new(),
            avatar_url: String::new(),
            created_at: at("2026-01-02T03:04:05Z"),
            updated_at: at("2026-01-02T03:04:05Z"),
            site_admin: false,
            tos_accepted: false,
            primary_email: String::new(),
        });
        assert_eq!(
            run(
                &schema,
                "{ currentUser { id displayName avatarURL } }",
                Actor::User { uid: 123 },
                &FrontendRootResolver::with_user(Some(user))
            ),
            r#"{"data":{"currentUser":{"id":"VXNlcjoxMjM=","displayName":null,"avatarURL":null}}}"#
        );
    }

    #[test]
    fn user_url_created_updated_and_flags_resolve() {
        let schema = schema();
        // `DateTime.MarshalJSON` is UTC RFC3339 without fractional seconds; the
        // `updatedAt` corpus value is +02:00 and must be converted.
        assert_eq!(
            run(
                &schema,
                "{ currentUser { url settingsURL createdAt updatedAt siteAdmin tosAccepted } }",
                Actor::User { uid: 1 },
                &FrontendRootResolver::with_user(Some(user()))
            ),
            r#"{"data":{"currentUser":{"url":"/users/alice","settingsURL":"/users/alice/settings","createdAt":"2026-01-02T03:04:05Z","updatedAt":"2026-02-03T02:05:06Z","siteAdmin":true,"tosAccepted":true}}}"#
        );
    }

    fn roots_with_users(
        users: &[(UserKey, UserLookup)],
        viewer_site_admin: bool,
    ) -> FrontendRootResolver {
        FrontendRootResolver::with_user_and_repositories_and_users(
            None,
            HashMap::new(),
            users.iter().cloned().collect(),
            viewer_site_admin,
        )
    }

    fn bob() -> UserData {
        UserData {
            id: 2,
            username: "bob".to_owned(),
            display_name: "Bob B.".to_owned(),
            avatar_url: String::new(),
            created_at: at("2026-01-02T03:04:05Z"),
            updated_at: at("2026-01-02T03:04:05Z"),
            site_admin: false,
            tos_accepted: false,
            primary_email: "bob@example.com".to_owned(),
        }
    }

    #[test]
    fn query_user_resolves_each_lookup_key() {
        let schema = schema();
        let alice = user().data;
        let roots = roots_with_users(
            &[
                (
                    UserKey::Username("alice".to_owned()),
                    UserLookup::Found(alice.clone()),
                ),
                (UserKey::DatabaseId(1), UserLookup::Found(alice.clone())),
                (
                    UserKey::Email("alice@example.com".to_owned()),
                    UserLookup::Found(alice),
                ),
            ],
            true,
        );
        assert_eq!(
            run(
                &schema,
                "{ byName: user(username: \"alice\") { username } \
                   byID: user(databaseID: 1) { username } \
                   byEmail: user(email: \"alice@example.com\") { username } }",
                Actor::Anonymous,
                &roots,
            ),
            r#"{"data":{"byName":{"username":"alice"},"byID":{"username":"alice"},"byEmail":{"username":"alice"}}}"#
        );
    }

    #[test]
    fn query_user_prefers_username_over_email() {
        // `user.go:38-56` checks username, then databaseID, then email.
        let schema = schema();
        let roots = roots_with_users(
            &[(
                UserKey::Username("alice".to_owned()),
                UserLookup::Found(user().data),
            )],
            false,
        );
        assert_eq!(
            run(
                &schema,
                "{ user(username: \"alice\", email: \"nobody@example.com\") { username } }",
                Actor::Anonymous,
                &roots,
            ),
            r#"{"data":{"user":{"username":"alice"}}}"#
        );
    }

    #[test]
    fn query_user_not_found_is_null() {
        let schema = schema();
        let roots = roots_with_users(
            &[(UserKey::Username("ghost".to_owned()), UserLookup::NotFound)],
            false,
        );
        assert_eq!(
            run(
                &schema,
                "{ user(username: \"ghost\") { username } }",
                Actor::Anonymous,
                &roots,
            ),
            r#"{"data":{"user":null}}"#
        );
    }

    #[test]
    fn query_user_without_arguments_errors() {
        // `user.go:56-57`: "must specify either username or email to look up
        // a user" (even though `databaseID` is supported).
        let schema = schema();
        assert_eq!(
            run(
                &schema,
                "{ user { username } }",
                Actor::Anonymous,
                &roots_with_users(&[], false),
            ),
            r#"{"errors":[{"message":"must specify either username or email to look up a user","path":["user"]}],"data":{"user":null}}"#
        );
    }

    #[test]
    fn query_user_database_error_is_reported() {
        let schema = schema();
        let roots = roots_with_users(
            &[(
                UserKey::Username("broken".to_owned()),
                UserLookup::Error("connection reset".to_owned()),
            )],
            false,
        );
        assert_eq!(
            run(
                &schema,
                "{ user(username: \"broken\") { username } }",
                Actor::Anonymous,
                &roots,
            ),
            r#"{"errors":[{"message":"connection reset","path":["user"]}],"data":{"user":null}}"#
        );
    }

    #[test]
    fn email_and_site_admin_require_self_or_site_admin() {
        let schema = schema();
        let bob = UserData { id: 2, ..bob() };
        let roots = roots_with_users(
            &[(UserKey::Username("bob".to_owned()), UserLookup::Found(bob))],
            false,
        );
        // Another non-admin actor: the baseline collapses to the shared error.
        assert_eq!(
            run(
                &schema,
                "{ user(username: \"bob\") { email siteAdmin } }",
                Actor::User { uid: 1 },
                &roots,
            ),
            r#"{"errors":[{"message":"must be authenticated as the authorized user or site admin","path":["user","email"]}],"data":{"user":null}}"#
        );
        // The same user sees the primary email and flag.
        assert_eq!(
            run(
                &schema,
                "{ user(username: \"bob\") { username email siteAdmin } }",
                Actor::User { uid: 2 },
                &roots,
            ),
            r#"{"data":{"user":{"username":"bob","email":"bob@example.com","siteAdmin":false}}}"#
        );
    }

    #[test]
    fn site_admin_viewer_can_read_other_users_email_and_flag() {
        // A site-admin viewer passes `CheckSiteAdminOrSameUserFromActor`.
        let schema = schema();
        let roots = roots_with_users(
            &[(
                UserKey::Username("bob".to_owned()),
                UserLookup::Found(bob()),
            )],
            true,
        );
        assert_eq!(
            run(
                &schema,
                "{ user(username: \"bob\") { email siteAdmin } }",
                Actor::User { uid: 1 },
                &roots,
            ),
            r#"{"data":{"user":{"email":"bob@example.com","siteAdmin":false}}}"#
        );
    }

    #[test]
    fn viewer_can_administer_is_false_without_erroring() {
        let schema = schema();
        let roots = roots_with_users(
            &[(
                UserKey::Username("bob".to_owned()),
                UserLookup::Found(bob()),
            )],
            false,
        );
        assert_eq!(
            run(
                &schema,
                "{ user(username: \"bob\") { viewerCanAdminister } }",
                Actor::User { uid: 1 },
                &roots,
            ),
            r#"{"data":{"user":{"viewerCanAdminister":false}}}"#
        );
        assert_eq!(
            run(
                &schema,
                "{ user(username: \"bob\") { viewerCanAdminister } }",
                Actor::User { uid: 2 },
                &roots,
            ),
            r#"{"data":{"user":{"viewerCanAdminister":true}}}"#
        );
        assert_eq!(
            run(
                &schema,
                "{ user(username: \"bob\") { viewerCanAdminister } }",
                Actor::Anonymous,
                &roots,
            ),
            r#"{"data":{"user":{"viewerCanAdminister":false}}}"#
        );
    }

    fn connection(
        users: Vec<UserData>,
        total_count: i32,
        limit: Option<i32>,
        after: Option<&str>,
        offset: i32,
    ) -> (UsersArgs, UsersConnectionLookup) {
        let args = UsersArgs {
            first: limit,
            after: after.map(str::to_owned),
            query: None,
            active_period: None,
            inactive_since: None,
        };
        (
            args,
            UsersConnectionLookup::Found(UsersConnectionData {
                users,
                total_count,
                limit,
                offset,
            }),
        )
    }

    fn roots_with_connections(
        connections: &[(UsersArgs, UsersConnectionLookup)],
    ) -> FrontendRootResolver {
        FrontendRootResolver::with_user_and_repositories_and_users_connections(
            None,
            HashMap::new(),
            HashMap::new(),
            connections.iter().cloned().collect(),
            false,
        )
    }

    fn org(id: i32, name: &str, display_name: Option<&str>) -> OrgData {
        OrgData {
            id,
            name: name.to_owned(),
            display_name: display_name.map(str::to_owned),
            created_at: at("2026-01-02T03:04:05.123456789Z"),
            viewer_can_administer: false,
            viewer_is_member: false,
        }
    }

    fn roots_with_orgs(
        organizations: &[(String, OrganizationLookup)],
        organizations_connections: &[(OrganizationsArgs, OrganizationsConnectionLookup)],
        viewer_site_admin: bool,
    ) -> FrontendRootResolver {
        FrontendRootResolver::with_prefetched_organizations(
            None,
            HashMap::new(),
            HashMap::new(),
            HashMap::new(),
            organizations.iter().cloned().collect(),
            organizations_connections.iter().cloned().collect(),
            viewer_site_admin,
        )
    }

    #[test]
    fn query_organization_returns_ported_fields() {
        let schema = schema();
        let mut data = org(7, "m9fj-team", Some("Team"));
        data.viewer_can_administer = true;
        data.viewer_is_member = true;
        let roots = roots_with_orgs(
            &[("m9fj-team".to_owned(), OrganizationLookup::Found(data))],
            &[],
            false,
        );
        assert_eq!(
            run(
                &schema,
                "{ organization(name: \"m9fj-team\") { id name displayName createdAt url \
                  settingsURL namespaceName viewerCanAdminister viewerIsMember } }",
                Actor::Anonymous,
                &roots,
            ),
            r#"{"data":{"organization":{"id":"T3JnOjc=","name":"m9fj-team","displayName":"Team","createdAt":"2026-01-02T03:04:05Z","url":"/organizations/m9fj-team","settingsURL":"/organizations/m9fj-team/settings","namespaceName":"m9fj-team","viewerCanAdminister":true,"viewerIsMember":true}}}"#
        );
    }

    #[test]
    fn query_organization_not_found_is_an_error() {
        let schema = schema();
        let roots = roots_with_orgs(
            &[(
                "missing".to_owned(),
                OrganizationLookup::Error("org not found: name missing".to_owned()),
            )],
            &[],
            false,
        );
        assert_eq!(
            run(
                &schema,
                "{ organization(name: \"missing\") { name } }",
                Actor::Anonymous,
                &roots,
            ),
            r#"{"errors":[{"message":"org not found: name missing","path":["organization"]}],"data":{"organization":null}}"#
        );
    }

    fn member_user(id: i32, username: &str) -> UserData {
        UserData {
            id,
            username: username.to_owned(),
            display_name: String::new(),
            avatar_url: String::new(),
            created_at: at("2026-01-02T03:04:05Z"),
            updated_at: at("2026-01-02T03:04:05Z"),
            site_admin: false,
            tos_accepted: false,
            primary_email: format!("{username}@example.com"),
        }
    }

    fn members_args() -> MembersArgs {
        MembersArgs {
            first: None,
            last: None,
            after: None,
            before: None,
            query: None,
        }
    }

    fn roots_with_org_members(
        org: OrgData,
        viewer_site_admin: bool,
        members: Vec<((i32, MembersArgs), MembersConnectionLookup)>,
    ) -> FrontendRootResolver {
        let nested = Arc::new(NestedPrefetch {
            org_members: members.into_iter().collect(),
            ..NestedPrefetch::default()
        });
        FrontendRootResolver::with_prefetched_organizations(
            None,
            HashMap::new(),
            HashMap::new(),
            HashMap::new(),
            HashMap::from([("team".to_owned(), OrganizationLookup::Found(org))]),
            HashMap::new(),
            viewer_site_admin,
        )
        .with_nested(nested)
    }

    #[test]
    fn org_members_returns_new_users_connection() {
        let schema = schema();
        let data = MembersConnectionData {
            users: vec![member_user(5, "bob")],
            total_count: 3,
            page_size: 10,
            fetched_count: 1,
            first_present: true,
            last_present: false,
            after_present: false,
            before_present: false,
        };
        let roots = roots_with_org_members(
            org(7, "team", Some("Team")),
            false,
            vec![((7, members_args()), MembersConnectionLookup::Found(data))],
        );
        assert_eq!(
            run(
                &schema,
                "{ organization(name: \"team\") { members { nodes { id username } totalCount \
                  pageInfo { startCursor endCursor hasNextPage hasPreviousPage } } } }",
                Actor::Anonymous,
                &roots,
            ),
            r#"{"data":{"organization":{"members":{"nodes":[{"id":"VXNlcjo1","username":"bob"}],"totalCount":3,"pageInfo":{"startCursor":"VXNlcjo1","endCursor":"VXNlcjo1","hasNextPage":false,"hasPreviousPage":false}}}}}"#
        );
    }

    #[test]
    fn org_members_page_info_uses_untrimmed_count() {
        let base = || MembersConnectionData {
            users: vec![member_user(9, "carol")],
            total_count: 12,
            page_size: 1,
            fetched_count: 2,
            first_present: false,
            last_present: false,
            after_present: false,
            before_present: false,
        };
        // `first` over-fetches one row: `hasNextPage` is true, the trimmed
        // page still has a single node (the baseline drops the extra tail).
        let next = ConnectionPageInfoObject::from_data(&MembersConnectionData {
            first_present: true,
            ..base()
        });
        assert!(next.has_next_page);
        assert!(!next.has_previous_page);
        assert_eq!(next.start_cursor.as_deref(), Some("VXNlcjo5"));

        // `last` flips the `hasPreviousPage` formula (over-fetch counts as a
        // previous page); with `before` set, `hasNextPage` is true even
        // though the page was fetched backwards.
        let last = ConnectionPageInfoObject::from_data(&MembersConnectionData {
            last_present: true,
            before_present: true,
            ..base()
        });
        assert!(last.has_next_page);
        assert!(last.has_previous_page);

        // `last` without `before`/`after`: `hasNextPage` is false.
        let last_without_cursor = ConnectionPageInfoObject::from_data(&MembersConnectionData {
            last_present: true,
            ..base()
        });
        assert!(!last_without_cursor.has_next_page);
        assert!(last_without_cursor.has_previous_page);
    }

    #[test]
    fn org_members_lookup_error_is_reported() {
        let schema = schema();
        let roots = roots_with_org_members(
            org(7, "team", None),
            false,
            vec![(
                (7, members_args()),
                MembersConnectionLookup::Error("boom".to_owned()),
            )],
        );
        assert_eq!(
            run(
                &schema,
                "{ organization(name: \"team\") { members { nodes { username } } } }",
                Actor::Anonymous,
                &roots,
            ),
            r#"{"errors":[{"message":"boom","path":["organization","members","nodes"]}],"data":{"organization":null}}"#
        );
    }

    fn membership(org_id: i32, user_id: i32) -> MembershipData {
        MembershipData {
            id: 1,
            org_id,
            user_id,
            created_at: at("2026-01-02T03:04:05Z"),
            updated_at: at("2026-01-02T03:04:05Z"),
        }
    }

    fn roots_with_memberships(
        current_user: Option<UserObject>,
        nested: NestedPrefetch,
        viewer_site_admin: bool,
    ) -> FrontendRootResolver {
        FrontendRootResolver::with_prefetched_organizations(
            current_user,
            HashMap::new(),
            HashMap::new(),
            HashMap::new(),
            HashMap::new(),
            HashMap::new(),
            viewer_site_admin,
        )
        .with_nested(Arc::new(nested))
    }

    #[test]
    fn organization_memberships_returns_static_connection() {
        let schema = schema();
        let bob = member_user(5, "bob");
        let org = org(7, "team", Some("Team"));
        let nested = NestedPrefetch {
            user_memberships: HashMap::from([(
                5,
                UserMembershipsLookup::Found(vec![membership(7, 5)]),
            )]),
            orgs_by_id: HashMap::from([(7, OrgByIdLookup::Found(org))]),
            users_by_id: HashMap::from([(5, UserByIdLookup::Found(bob.clone()))]),
            ..NestedPrefetch::default()
        };
        let roots = roots_with_memberships(
            Some(UserObject::from_data_as_viewer(bob, true)),
            nested,
            true,
        );
        assert_eq!(
            run(
                &schema,
                "{ currentUser { organizationMemberships { nodes { organization { name \
                  displayName } user { username } createdAt updatedAt } totalCount } } }",
                Actor::User { uid: 5 },
                &roots,
            ),
            r#"{"data":{"currentUser":{"organizationMemberships":{"nodes":[{"organization":{"name":"team","displayName":"Team"},"user":{"username":"bob"},"createdAt":"2026-01-02T03:04:05Z","updatedAt":"2026-01-02T03:04:05Z"}],"totalCount":1}}}}"#
        );
    }

    #[test]
    fn organization_memberships_uses_folded_authorization_error() {
        let schema = schema();
        let roots = roots_with_memberships(
            Some(UserObject::from_data(member_user(5, "bob"))),
            NestedPrefetch::default(),
            false,
        );
        assert_eq!(
            run(
                &schema,
                "{ currentUser { organizationMemberships { totalCount } } }",
                Actor::User { uid: 9 },
                &roots,
            ),
            r#"{"errors":[{"message":"must be authenticated as the authorized user or site admin","path":["currentUser","organizationMemberships"]}],"data":{"currentUser":null}}"#
        );
    }

    #[test]
    fn organization_membership_org_lookup_error_is_reported() {
        let schema = schema();
        let bob = member_user(5, "bob");
        let nested = NestedPrefetch {
            user_memberships: HashMap::from([(
                5,
                UserMembershipsLookup::Found(vec![membership(7, 5)]),
            )]),
            orgs_by_id: HashMap::from([(
                7,
                OrgByIdLookup::Error("org not found: id 7".to_owned()),
            )]),
            ..NestedPrefetch::default()
        };
        let roots = roots_with_memberships(
            Some(UserObject::from_data_as_viewer(bob, true)),
            nested,
            true,
        );
        assert_eq!(
            run(
                &schema,
                "{ currentUser { organizationMemberships { nodes { organization { name } } } } }",
                Actor::User { uid: 5 },
                &roots,
            ),
            r#"{"errors":[{"message":"org not found: id 7","path":["currentUser","organizationMemberships","nodes",0,"organization"]}],"data":{"currentUser":null}}"#
        );
    }

    struct FakeUserOrganizationsStore(HashMap<i32, Vec<OrgData>>);

    #[async_trait]
    impl UserOrganizationsStore for FakeUserOrganizationsStore {
        async fn organizations_for_user(
            &self,
            user_id: i32,
            _viewer_uid: i32,
            _viewer_site_admin: bool,
        ) -> Result<Vec<OrgData>, String> {
            Ok(self.0.get(&user_id).cloned().unwrap_or_default())
        }
    }

    struct FakeOrgMembersStore(HashMap<(i32, MembersArgs), MembersConnectionData>);

    #[async_trait]
    impl OrgMembersStore for FakeOrgMembersStore {
        async fn members(
            &self,
            org_id: i32,
            args: &MembersArgs,
        ) -> Result<MembersConnectionData, String> {
            Ok(self
                .0
                .get(&(org_id, args.clone()))
                .cloned()
                .unwrap_or(MembersConnectionData {
                    users: Vec::new(),
                    total_count: 0,
                    page_size: 100,
                    fetched_count: 0,
                    first_present: false,
                    last_present: false,
                    after_present: false,
                    before_present: false,
                }))
        }
    }

    #[derive(Default)]
    struct FakeOrgMembershipsStore {
        memberships: HashMap<i32, Vec<MembershipData>>,
        orgs: HashMap<i32, OrgData>,
        users: HashMap<i32, UserData>,
        namespaces: HashMap<String, NamespaceName>,
    }

    #[async_trait]
    impl OrgMembershipsStore for FakeOrgMembershipsStore {
        async fn memberships_for_user(&self, user_id: i32) -> Result<Vec<MembershipData>, String> {
            Ok(self.memberships.get(&user_id).cloned().unwrap_or_default())
        }

        async fn namespace_by_name(&self, name: &str) -> Result<Option<NamespaceName>, String> {
            Ok(self
                .namespaces
                .iter()
                .find(|(existing, _)| existing.eq_ignore_ascii_case(name))
                .map(|(_, namespace)| namespace.clone()))
        }

        async fn org_by_id(
            &self,
            org_id: i32,
            _viewer_uid: i32,
            _viewer_site_admin: bool,
        ) -> Result<OrgData, String> {
            self.orgs
                .get(&org_id)
                .cloned()
                .ok_or_else(|| format!("org not found: id {org_id}"))
        }

        async fn user_by_id(&self, user_id: i32) -> Result<UserData, String> {
            self.users
                .get(&user_id)
                .cloned()
                .ok_or_else(|| format!("user not found: [{user_id}]"))
        }

        async fn latest_settings_for_user(
            &self,
            _user_id: i32,
        ) -> Result<Option<SettingsData>, String> {
            Ok(None)
        }

        async fn latest_settings_for_org(
            &self,
            _org_id: i32,
        ) -> Result<Option<SettingsData>, String> {
            Ok(None)
        }

        async fn latest_settings_for_site(&self) -> Result<Option<SettingsData>, String> {
            Ok(None)
        }
    }

    #[tokio::test]
    async fn nested_prefetch_traverses_memberships_and_member_organizations() {
        let schema = schema();
        let bob = member_user(5, "bob");
        let memberships_store = FakeOrgMembershipsStore {
            memberships: HashMap::from([(5, vec![membership(7, 5)])]),
            orgs: HashMap::from([(7, org(7, "team", Some("Team")))]),
            users: HashMap::from([(5, bob.clone())]),
            ..FakeOrgMembershipsStore::default()
        };
        let user_store =
            FakeUserOrganizationsStore(HashMap::from([(9, vec![org(8, "sub", None)])]));
        let members_store = FakeOrgMembersStore(HashMap::from([(
            (7, members_args()),
            MembersConnectionData {
                users: vec![member_user(9, "carol")],
                total_count: 1,
                page_size: 100,
                fetched_count: 1,
                first_present: false,
                last_present: false,
                after_present: false,
                before_present: false,
            },
        )]));
        let actor = Actor::User { uid: 5 };
        let variables = Map::new();
        let request = ExecRequest {
            query: "{ currentUser { organizationMemberships { nodes { organization { members { \
                    nodes { username organizations { nodes { name } } } } } } } } }",
            operation_name: "",
            variables: &variables,
            actor,
        };
        let fields = sg_graphql::exec::root_fields(&schema, &request);
        let nested = prefetch_nested(
            &user_store,
            &members_store,
            &memberships_store,
            &fields,
            Some(&bob),
            &HashMap::new(),
            &HashMap::new(),
            &HashMap::new(),
            &HashMap::new(),
            &NodeRoots::default(),
            actor,
            true,
        )
        .await;
        let roots = roots_with_memberships(
            Some(UserObject::from_data_as_viewer(bob, true)),
            nested,
            true,
        );
        assert_eq!(
            run(&schema, request.query, actor, &roots),
            r#"{"data":{"currentUser":{"organizationMemberships":{"nodes":[{"organization":{"members":{"nodes":[{"username":"carol","organizations":{"nodes":[{"name":"sub"}]}}]}}}]}}}}"#
        );
    }

    #[tokio::test]
    async fn nested_prefetch_traverses_members_and_their_organizations() {
        let schema = schema();
        let mut team = org(7, "team", None);
        team.viewer_can_administer = true;
        let organizations = HashMap::from([("team".to_owned(), OrganizationLookup::Found(team))]);
        let data = MembersConnectionData {
            users: vec![member_user(5, "bob")],
            total_count: 1,
            page_size: 100,
            fetched_count: 1,
            first_present: false,
            last_present: false,
            after_present: false,
            before_present: false,
        };
        let members_store = FakeOrgMembersStore(HashMap::from([((7, members_args()), data)]));
        let user_store =
            FakeUserOrganizationsStore(HashMap::from([(5, vec![org(8, "sub", None)])]));
        let actor = Actor::User { uid: 5 };
        let variables = Map::new();
        let request = ExecRequest {
            query: "{ organization(name: \"team\") { members { nodes { username \
                    organizations { nodes { name } } } } } }",
            operation_name: "",
            variables: &variables,
            actor,
        };
        let fields = sg_graphql::exec::root_fields(&schema, &request);
        let nested = prefetch_nested(
            &user_store,
            &members_store,
            &FakeOrgMembershipsStore::default(),
            &fields,
            None,
            &HashMap::new(),
            &HashMap::new(),
            &organizations,
            &HashMap::new(),
            &NodeRoots::default(),
            actor,
            true,
        )
        .await;
        let roots = FrontendRootResolver::with_prefetched_organizations(
            None,
            HashMap::new(),
            HashMap::new(),
            HashMap::new(),
            organizations,
            HashMap::new(),
            true,
        )
        .with_nested(Arc::new(nested));
        assert_eq!(
            run(&schema, request.query, actor, &roots),
            r#"{"data":{"organization":{"members":{"nodes":[{"username":"bob","organizations":{"nodes":[{"name":"sub"}]}}]}}}}"#
        );
    }

    #[test]
    fn query_organizations_requires_site_admin() {
        let schema = schema();
        let args = OrganizationsArgs {
            first: None,
            query: None,
        };
        let lookup = OrganizationsConnectionLookup::Found(OrganizationsConnectionData {
            orgs: vec![org(7, "m9fj-team", None)],
            total_count: 1,
        });
        let anonymous = roots_with_orgs(&[], &[(args.clone(), lookup.clone())], false);
        assert_eq!(
            run(
                &schema,
                "{ organizations { nodes { name } totalCount } }",
                Actor::Anonymous,
                &anonymous,
            ),
            r#"{"errors":[{"message":"not authenticated","path":["organizations","nodes"]}],"data":null}"#
        );
        let non_admin = roots_with_orgs(&[], &[(args.clone(), lookup.clone())], false);
        assert_eq!(
            run(
                &schema,
                "{ organizations { nodes { name } } }",
                Actor::User { uid: 1 },
                &non_admin,
            ),
            r#"{"errors":[{"message":"must be site admin","path":["organizations","nodes"]}],"data":null}"#
        );
        let admin = roots_with_orgs(&[], &[(args, lookup)], true);
        assert_eq!(
            run(
                &schema,
                "{ organizations { nodes { name } totalCount } }",
                Actor::User { uid: 1 },
                &admin,
            ),
            r#"{"data":{"organizations":{"nodes":[{"name":"m9fj-team"}],"totalCount":1}}}"#
        );
    }

    fn roots_with_user_organizations(
        user: UserData,
        viewer_site_admin: bool,
        organizations: Vec<(i32, UserOrganizationsLookup)>,
    ) -> FrontendRootResolver {
        let nested = Arc::new(NestedPrefetch {
            user_organizations: organizations.into_iter().collect(),
            ..NestedPrefetch::default()
        });
        let users = HashMap::from([(
            UserKey::Username(user.username.clone()),
            UserLookup::Found(user),
        )]);
        FrontendRootResolver::with_prefetched_organizations(
            None,
            HashMap::new(),
            users,
            HashMap::new(),
            HashMap::new(),
            HashMap::new(),
            viewer_site_admin,
        )
        .with_nested(nested)
    }

    #[test]
    fn user_organizations_returns_static_org_connection() {
        let schema = schema();
        let mut team = org(7, "team", Some("Team"));
        team.viewer_can_administer = true;
        team.viewer_is_member = true;
        let roots = roots_with_user_organizations(
            user().data,
            false,
            vec![(
                1,
                UserOrganizationsLookup::Found(vec![team, org(9, "other", None)]),
            )],
        );
        assert_eq!(
            run(
                &schema,
                "{ user(username: \"alice\") { organizations { nodes { name displayName \
                  viewerCanAdminister viewerIsMember } totalCount } } }",
                Actor::User { uid: 1 },
                &roots,
            ),
            r#"{"data":{"user":{"organizations":{"nodes":[{"name":"team","displayName":"Team","viewerCanAdminister":true,"viewerIsMember":true},{"name":"other","displayName":null,"viewerCanAdminister":false,"viewerIsMember":false}],"totalCount":2}}}}"#
        );
    }

    #[test]
    fn user_organizations_requires_same_user_or_site_admin() {
        let schema = schema();
        let roots = roots_with_user_organizations(user().data, false, vec![]);
        let expected = r#"{"errors":[{"message":"must be authenticated as the authorized user or site admin","path":["user","organizations"]}],"data":{"user":null}}"#;
        assert_eq!(
            run(
                &schema,
                "{ user(username: \"alice\") { organizations { nodes { name } } } }",
                Actor::User { uid: 2 },
                &roots,
            ),
            expected
        );
        assert_eq!(
            run(
                &schema,
                "{ user(username: \"alice\") { organizations { nodes { name } } } }",
                Actor::Anonymous,
                &roots,
            ),
            expected
        );
    }

    #[test]
    fn user_organizations_lookup_error_is_reported() {
        let schema = schema();
        let roots = roots_with_user_organizations(
            user().data,
            false,
            vec![(1, UserOrganizationsLookup::Error("boom".to_owned()))],
        );
        assert_eq!(
            run(
                &schema,
                "{ user(username: \"alice\") { organizations { nodes { name } } } }",
                Actor::User { uid: 1 },
                &roots,
            ),
            r#"{"errors":[{"message":"boom","path":["user","organizations"]}],"data":{"user":null}}"#
        );
    }

    #[test]
    fn current_user_organizations_use_nested_prefetch() {
        let schema = schema();
        let nested = Arc::new(NestedPrefetch {
            user_organizations: HashMap::from([(
                1,
                UserOrganizationsLookup::Found(vec![org(7, "team", Some("Team"))]),
            )]),
            ..NestedPrefetch::default()
        });
        let roots = FrontendRootResolver::with_prefetched_organizations(
            Some(user()),
            HashMap::new(),
            HashMap::new(),
            HashMap::new(),
            HashMap::new(),
            HashMap::new(),
            false,
        )
        .with_nested(nested);
        assert_eq!(
            run(
                &schema,
                "{ currentUser { organizations { nodes { name } totalCount } } }",
                Actor::User { uid: 1 },
                &roots,
            ),
            r#"{"data":{"currentUser":{"organizations":{"nodes":[{"name":"team"}],"totalCount":1}}}}"#
        );
    }

    #[test]
    fn query_users_lists_nodes_and_total_count() {
        let schema = schema();
        let alice = user().data;
        let roots = roots_with_connections(&[connection(vec![alice, bob()], 2, None, None, 0)]);
        assert_eq!(
            run(
                &schema,
                "{ users { nodes { username } totalCount pageInfo { hasNextPage endCursor } } }",
                Actor::Anonymous,
                &roots,
            ),
            r#"{"data":{"users":{"nodes":[{"username":"alice"},{"username":"bob"}],"totalCount":2,"pageInfo":{"hasNextPage":false,"endCursor":null}}}}"#
        );
    }

    #[test]
    fn query_users_page_info_matches_users_go() {
        let schema = schema();
        // `first: 2, after: "2"` over 10 users: after = 4 > total? no -> cursor.
        let next_page = connection(vec![bob(), bob()], 10, Some(2), Some("2"), 2);
        let roots = roots_with_connections(&[next_page]);
        assert_eq!(
            run(
                &schema,
                "{ users(first: 2, after: \"2\") { pageInfo { hasNextPage endCursor } } }",
                Actor::Anonymous,
                &roots,
            ),
            r#"{"data":{"users":{"pageInfo":{"hasNextPage":true,"endCursor":"4"}}}}"#
        );

        // A partial page: `after = offset + len(users)` is below the limit.
        let last_page = connection(vec![bob(), bob()], 2, Some(5), None, 0);
        let roots = roots_with_connections(&[last_page]);
        assert_eq!(
            run(
                &schema,
                "{ users(first: 5) { pageInfo { hasNextPage endCursor } } }",
                Actor::Anonymous,
                &roots,
            ),
            r#"{"data":{"users":{"pageInfo":{"hasNextPage":false,"endCursor":null}}}}"#
        );

        // Total equals the cursor: no next page.
        let exhausted = connection(vec![bob(), bob()], 4, Some(2), Some("2"), 2);
        let roots = roots_with_connections(&[exhausted]);
        assert_eq!(
            run(
                &schema,
                "{ users(first: 2, after: \"2\") { pageInfo { hasNextPage endCursor } } }",
                Actor::Anonymous,
                &roots,
            ),
            r#"{"data":{"users":{"pageInfo":{"hasNextPage":false,"endCursor":null}}}}"#
        );
    }

    #[test]
    fn query_users_without_prefetch_is_unported() {
        let schema = schema();
        assert_eq!(
            run(
                &schema,
                "{ users { totalCount } }",
                Actor::Anonymous,
                &FrontendRootResolver::new(),
            ),
            r#"{"errors":[{"message":"GraphQL field \"users\" of \"Query\" has no ported resolver","path":["users"]}],"data":null}"#
        );
    }

    #[test]
    fn maybe_query_is_id_matches_the_baseline() {
        assert_eq!(maybe_query_is_id("42"), Some(42));
        assert_eq!(maybe_query_is_id("VXNlcjox"), Some(1));
        assert_eq!(maybe_query_is_id("not-an-id"), None);
        // graph-gophers does not validate the kind.
        assert_eq!(maybe_query_is_id("UmVwb3NpdG9yeTo0Mg=="), Some(42));
    }

    #[test]
    fn builtin_auth_is_unported() {
        let schema = schema();
        assert_eq!(
            run(
                &schema,
                "{ currentUser { builtinAuth } }",
                Actor::User { uid: 1 },
                &FrontendRootResolver::with_user(Some(user()))
            ),
            r#"{"errors":[{"message":"GraphQL field \"builtinAuth\" of \"User\" has no ported resolver","path":["currentUser","builtinAuth"]}],"data":{"currentUser":null}}"#
        );
    }

    #[test]
    fn unported_root_fields_report_their_name() {
        let schema = schema();
        assert_eq!(
            run(
                &schema,
                "{ other }",
                Actor::Anonymous,
                &FrontendRootResolver::new()
            ),
            r#"{"errors":[{"message":"GraphQL field \"other\" of \"Query\" has no ported resolver","path":["other"]}],"data":{"other":null}}"#
        );
    }

    #[test]
    fn relay_id_matches_go() {
        // `base64.URLEncoding.EncodeToString([]byte("User:1"))`.
        assert_eq!(UserObject::relay_id(1), "VXNlcjox");
        assert_eq!(UserObject::relay_id(123), "VXNlcjoxMjM=");
        assert_eq!(RepositoryObject::relay_id(42), "UmVwb3NpdG9yeTo0Mg==");
    }

    fn repository_schema() -> sg_graphql::Schema {
        assemble_sdl(
            "type Query { \
                 repository(name: String, uri: String, cloneURL: String): Repository \
             } \
             type Repository { \
                 id: ID! name: String! uri: String! description: String! \
                 isFork: Boolean! isArchived: Boolean! isPrivate: Boolean! \
                 stars: Int! url: String! updatedAt: DateTime enabled: Boolean! \
                 sourceType: RepositoryType! externalRepository: ExternalRepository! \
             } \
             type ExternalRepository { id: String! serviceType: String! serviceID: String! } \
             enum RepositoryType { GIT_REPOSITORY PERFORCE_DEPOT } \
             scalar DateTime",
        )
        .expect("schema")
    }

    fn repository_data(name: &str) -> RepositoryData {
        RepositoryData {
            id: 42,
            name: name.to_owned(),
            uri: "github.com/a/b".to_owned(),
            description: "A repository".to_owned(),
            fork: true,
            archived: false,
            private: true,
            stars: 7,
            external_id: "ext-1".to_owned(),
            external_service_type: "github".to_owned(),
            external_service_id: "https://github.com/".to_owned(),
        }
    }

    struct FakeRepositories(HashMap<String, RepoLookup>);

    #[async_trait]
    impl RepositoryLookup for FakeRepositories {
        async fn lookup(
            &self,
            name_or_uri: &str,
            _authenticated_user_id: i32,
            _bypass_authz: bool,
        ) -> Result<RepoLookup, String> {
            Ok(self
                .0
                .get(name_or_uri)
                .cloned()
                .unwrap_or(RepoLookup::NotFound))
        }

        async fn by_id(
            &self,
            _repo_id: i32,
            _authenticated_user_id: i32,
            _bypass_authz: bool,
        ) -> Result<RepoLookup, String> {
            Ok(RepoLookup::NotFound)
        }

        async fn by_hashed_name(
            &self,
            hashed_name: &str,
            _authenticated_user_id: i32,
            _bypass_authz: bool,
        ) -> Result<RepoLookup, String> {
            Ok(self
                .0
                .get(hashed_name)
                .cloned()
                .unwrap_or_else(|| RepoLookup::Error("repo not found".to_owned())))
        }
    }

    fn redirect_schema() -> sg_graphql::Schema {
        assemble_sdl(
            "type Query { \
                 repositoryRedirect(name: String, cloneURL: String, hashedName: String): RepositoryRedirect \
             } \
             union RepositoryRedirect = Repository | Redirect \
             type Repository { id: ID! name: String! url: String! } \
             type Redirect { url: String! }",
        )
        .expect("schema")
    }

    async fn run_repository_redirect(query: &str, lookups: &[(&str, RepoLookup)]) -> String {
        let schema = redirect_schema();
        let variables = Map::new();
        let request = ExecRequest {
            query,
            operation_name: "",
            variables: &variables,
            actor: Actor::Anonymous,
        };
        let fields = sg_graphql::exec::root_fields(&schema, &request);
        let store = FakeRepositories(
            lookups
                .iter()
                .map(|(key, value)| ((*key).to_owned(), value.clone()))
                .collect(),
        );
        let redirects =
            prefetch_repository_redirects(&store, None, &fields, Actor::Anonymous, false).await;
        let roots = FrontendRootResolver::new().with_repository_redirects(redirects);
        execute_with(&schema, request, Some(&roots)).to_json()
    }

    #[tokio::test]
    async fn repository_redirect_name_hit_and_miss() {
        let body = "{ repositoryRedirect(name: \"github.com/a/b\") { \
                      __typename ... on Repository { name url } ... on Redirect { url } } }";
        assert_eq!(
            run_repository_redirect(
                body,
                &[(
                    "github.com/a/b",
                    RepoLookup::Found(repository_data("github.com/a/b")),
                )]
            )
            .await,
            r#"{"data":{"repositoryRedirect":{"__typename":"Repository","name":"github.com/a/b","url":"/github.com/a/b"}}}"#
        );
        assert_eq!(
            run_repository_redirect(body, &[]).await,
            r#"{"data":{"repositoryRedirect":null}}"#
        );
    }

    #[tokio::test]
    async fn repository_redirect_hashed_name_errors_when_missing() {
        let body = "{ repositoryRedirect(hashedName: \"abc\") { ... on Repository { name } } }";
        assert_eq!(
            run_repository_redirect(body, &[]).await,
            r#"{"errors":[{"message":"repo not found","path":["repositoryRedirect"]}],"data":{"repositoryRedirect":null}}"#
        );
        // `hashedName` wins over `name` (`graphqlbackend.go:961-975`).
        assert_eq!(
            run_repository_redirect(
                "{ repositoryRedirect(name: \"ignored\", hashedName: \"abc\") { \
                   ... on Repository { name } } }",
                &[
                    ("ignored", RepoLookup::Found(repository_data("ignored"))),
                    ("abc", RepoLookup::Found(repository_data("github.com/h"))),
                ]
            )
            .await,
            r#"{"data":{"repositoryRedirect":{"name":"github.com/h"}}}"#
        );
    }

    #[tokio::test]
    async fn repository_redirect_argument_errors_and_clone_url_gap() {
        assert_eq!(
            run_repository_redirect("{ repositoryRedirect { ... on Repository { name } } }", &[])
                .await,
            r#"{"errors":[{"message":"neither name nor cloneURL given","path":["repositoryRedirect"]}],"data":{"repositoryRedirect":null}}"#
        );
        assert_eq!(
            run_repository_redirect(
                "{ repositoryRedirect(cloneURL: \"https://example.com/r.git\") { \
                   ... on Repository { name } } }",
                &[]
            )
            .await,
            r#"{"errors":[{"message":"GraphQL field \"repositoryRedirect\" of \"Query\" has no ported resolver","path":["repositoryRedirect"]}],"data":{"repositoryRedirect":null}}"#
        );
    }

    async fn run_repository(query: &str, actor: Actor, lookups: &[(&str, RepoLookup)]) -> String {
        let schema = repository_schema();
        let variables = Map::new();
        let request = ExecRequest {
            query,
            operation_name: "",
            variables: &variables,
            actor,
        };
        let fields = sg_graphql::exec::root_fields(&schema, &request);
        let store = FakeRepositories(
            lookups
                .iter()
                .map(|(key, value)| ((*key).to_owned(), value.clone()))
                .collect(),
        );
        let repositories = prefetch_repositories(&store, None, &fields, actor, false).await;
        let roots = FrontendRootResolver::with_user_and_repositories(None, repositories);
        execute_with(&schema, request, Some(&roots)).to_json()
    }

    #[tokio::test]
    async fn repository_fields_resolve() {
        let lookups = [(
            "github.com/a/b",
            RepoLookup::Found(repository_data("github.com/a/b")),
        )];
        assert_eq!(
            run_repository(
                "{ repository(name: \"github.com/a/b\") { id name uri description isFork \
                  isArchived isPrivate stars url updatedAt enabled sourceType \
                  externalRepository { id serviceType serviceID } } }",
                Actor::Anonymous,
                &lookups,
            )
            .await,
            r#"{"data":{"repository":{"id":"UmVwb3NpdG9yeTo0Mg==","name":"github.com/a/b","uri":"github.com/a/b","description":"A repository","isFork":true,"isArchived":false,"isPrivate":true,"stars":7,"url":"/github.com/a/b","updatedAt":null,"enabled":true,"sourceType":"GIT_REPOSITORY","externalRepository":{"id":"ext-1","serviceType":"github","serviceID":"https://github.com/"}}}}"#
        );
    }

    #[tokio::test]
    async fn repository_uri_alias_and_url_escaping() {
        // `uri` is the deprecated alias for `name`; only when `name` is absent.
        let lookups = [("m9/uri", RepoLookup::Found(repository_data("a b/c?d")))];
        assert_eq!(
            run_repository(
                "{ repository(uri: \"m9/uri\") { name url } }",
                Actor::Anonymous,
                &lookups,
            )
            .await,
            r#"{"data":{"repository":{"name":"a b/c?d","url":"/a%20b/c%3Fd"}}}"#
        );
    }

    #[tokio::test]
    async fn missing_and_blocked_repositories() {
        assert_eq!(
            run_repository(
                "{ repository(name: \"r/x\") { name } }",
                Actor::Anonymous,
                &[],
            )
            .await,
            r#"{"data":{"repository":null}}"#
        );
        let blocked = [(
            "r/x",
            RepoLookup::Blocked {
                name: "r/x".to_owned(),
                reason: "dmca".to_owned(),
            },
        )];
        assert_eq!(
            run_repository(
                "{ repository(name: \"r/x\") { name } }",
                Actor::Anonymous,
                &blocked,
            )
            .await,
            r#"{"errors":[{"message":"repository r/x has been blocked. reason: dmca","path":["repository"]}],"data":{"repository":null}}"#
        );
    }

    #[tokio::test]
    async fn clone_url_and_missing_arguments() {
        assert_eq!(
            run_repository(
                "{ repository(cloneURL: \"https://example.com/r.git\") { name } }",
                Actor::Anonymous,
                &[],
            )
            .await,
            r#"{"errors":[{"message":"GraphQL field \"repository\" of \"Query\" has no ported resolver","path":["repository"]}],"data":{"repository":null}}"#
        );
        assert_eq!(
            run_repository("{ repository { name } }", Actor::Anonymous, &[]).await,
            r#"{"errors":[{"message":"neither name nor cloneURL given","path":["repository"]}],"data":{"repository":null}}"#
        );
    }

    #[tokio::test]
    async fn perforce_repositories_have_a_depot_source_type() {
        let mut data = repository_data("depot/main");
        data.external_service_type = "perforce".to_owned();
        let lookups = [("depot/main", RepoLookup::Found(data))];
        assert_eq!(
            run_repository(
                "{ repository(name: \"depot/main\") { sourceType } }",
                Actor::Anonymous,
                &lookups,
            )
            .await,
            r#"{"data":{"repository":{"sourceType":"PERFORCE_DEPOT"}}}"#
        );
    }

    #[test]
    fn go_url_path_matches_url_url_string() {
        assert_eq!(go_url_path("/github.com/a/b"), "/github.com/a/b");
        // `$&+,/:;=@` stay literal in Go's path mode; `?` and `#` escape.
        assert_eq!(
            go_url_path("/a$&+,/:;=@?b#c%d e"),
            "/a$&+,/:;=@%3Fb%23c%25d%20e"
        );
        assert_eq!(go_url_path("/ä"), "/%C3%A4");
    }

    fn relay_id(kind: &str, id: i32) -> String {
        base64::engine::general_purpose::URL_SAFE.encode(format!("{kind}:{id}"))
    }

    fn empty_roots() -> FrontendRootResolver {
        FrontendRootResolver::with_prefetched_organizations(
            None,
            HashMap::new(),
            HashMap::new(),
            HashMap::new(),
            HashMap::new(),
            HashMap::new(),
            false,
        )
    }

    #[tokio::test]
    async fn query_node_dispatches_ported_kinds() {
        let schema = schema();
        let nodes = NodeRoots {
            users: HashMap::from([(1, UserByIdLookup::Found(member_user(1, "alice")))]),
            orgs: HashMap::from([(7, OrgByIdLookup::Found(org(7, "team", Some("Team"))))]),
            repositories: HashMap::from([(9, RepoLookup::Found(repository_data("a/b")))]),
            ..NodeRoots::default()
        };
        let user_id = relay_id("User", 1);
        let org_id = relay_id("Org", 7);
        let repo_id = relay_id("Repository", 9);
        let query = format!(
            "{{ u: node(id: \"{user_id}\") {{ __typename id ... on User {{ username }} }} \
               o: node(id: \"{org_id}\") {{ __typename ... on Org {{ name displayName }} }} \
               r: node(id: \"{repo_id}\") {{ __typename ... on Repository {{ name isPrivate }} }} }}"
        );
        let roots = empty_roots().with_nodes(nodes);
        let expected = format!(
            r#"{{"data":{{"u":{{"__typename":"User","id":"{user_id}","username":"alice"}},"o":{{"__typename":"Org","name":"team","displayName":"Team"}},"r":{{"__typename":"Repository","name":"a/b","isPrivate":true}}}}}}"#
        );
        assert_eq!(run(&schema, &query, Actor::Anonymous, &roots), expected);
    }

    #[tokio::test]
    async fn query_node_reports_lookup_errors() {
        let schema = schema();
        let nodes = NodeRoots {
            users: HashMap::from([(5, UserByIdLookup::Error("user not found: [5]".to_owned()))]),
            orgs: HashMap::from([(7, OrgByIdLookup::Error("org not found: id 7".to_owned()))]),
            repositories: HashMap::from([(
                9,
                RepoLookup::Blocked {
                    name: "a/b".to_owned(),
                    reason: "dmca".to_owned(),
                },
            )]),
            ..NodeRoots::default()
        };
        let roots = empty_roots().with_nodes(nodes.clone());
        let query = format!("{{ node(id: \"{}\") {{ id }} }}", relay_id("User", 5));
        assert_eq!(
            run(&schema, &query, Actor::Anonymous, &roots),
            r#"{"errors":[{"message":"user not found: [5]","path":["node"]}],"data":{"node":null}}"#
        );
        let query = format!("{{ node(id: \"{}\") {{ id }} }}", relay_id("Org", 7));
        assert_eq!(
            run(&schema, &query, Actor::Anonymous, &roots),
            r#"{"errors":[{"message":"org not found: id 7","path":["node"]}],"data":{"node":null}}"#
        );
        let query = format!("{{ node(id: \"{}\") {{ id }} }}", relay_id("Repository", 9));
        assert_eq!(
            run(&schema, &query, Actor::Anonymous, &roots),
            r#"{"errors":[{"message":"repository a/b has been blocked. reason: dmca","path":["node"]}],"data":{"node":null}}"#
        );
        // A kind that is not ported (and an unparsable Relay ID) is
        // `invalid id`, matching the baseline's missing registry entry.
        let query = format!(
            "{{ node(id: \"{}\") {{ id }} }}",
            relay_id("AccessToken", 1)
        );
        assert_eq!(
            run(&schema, &query, Actor::Anonymous, &roots),
            r#"{"errors":[{"message":"invalid id","path":["node"]}],"data":{"node":null}}"#
        );
        assert_eq!(
            run(
                &schema,
                "{ node(id: \"bad\") { id } }",
                Actor::Anonymous,
                &roots
            ),
            r#"{"errors":[{"message":"invalid id","path":["node"]}],"data":{"node":null}}"#
        );
        // Without a prefetch (empty registries) the field is unported.
        assert_eq!(
            run(
                &schema,
                &format!("{{ node(id: \"{}\") {{ id }} }}", relay_id("User", 1)),
                Actor::Anonymous,
                &empty_roots()
            ),
            r#"{"errors":[{"message":"GraphQL field \"node\" of \"Query\" has no ported resolver","path":["node"]}],"data":{"node":null}}"#
        );
    }

    #[tokio::test]
    async fn query_node_prefetches_nested_organizations() {
        let schema = schema();
        let bob = member_user(5, "bob");
        let memberships_store = FakeOrgMembershipsStore {
            users: HashMap::from([(5, bob)]),
            ..FakeOrgMembershipsStore::default()
        };
        let repositories = FakeRepositories(HashMap::new());
        let actor = Actor::User { uid: 5 };
        let id = relay_id("User", 5);
        let variables = Map::new();
        let request = ExecRequest {
            query: &format!(
                "{{ node(id: \"{id}\") {{ ... on User {{ username \
                   organizations {{ nodes {{ name }} }} }} }} }}"
            ),
            operation_name: "",
            variables: &variables,
            actor,
        };
        let fields = sg_graphql::exec::root_fields(&schema, &request);
        assert_eq!(fields.len(), 1);
        assert_eq!(fields[0].type_conditions.len(), 1);
        assert_eq!(fields[0].type_conditions[0].0, "User");

        let nodes = prefetch_node_roots(
            &memberships_store,
            &repositories,
            None,
            None,
            None,
            None,
            None,
            "",
            &fields,
            actor,
            false,
        )
        .await;
        let user_store =
            FakeUserOrganizationsStore(HashMap::from([(5, vec![org(8, "sub", None)])]));
        let members_store = FakeOrgMembersStore(HashMap::new());
        let nested = prefetch_nested(
            &user_store,
            &members_store,
            &memberships_store,
            &fields,
            None,
            &HashMap::new(),
            &HashMap::new(),
            &HashMap::new(),
            &HashMap::new(),
            &nodes,
            actor,
            false,
        )
        .await;
        let roots = empty_roots()
            .with_nested(Arc::new(nested))
            .with_nodes(nodes);
        assert_eq!(
            run(
                &schema,
                &format!(
                    "{{ node(id: \"{id}\") {{ ... on User {{ username \
                       organizations {{ nodes {{ name }} }} }} }} }}"
                ),
                actor,
                &roots
            ),
            r#"{"data":{"node":{"username":"bob","organizations":{"nodes":[{"name":"sub"}]}}}}"#
        );
    }

    #[tokio::test]
    async fn namespace_roots_resolve_users_and_orgs() {
        let schema = schema();
        let memberships_store = FakeOrgMembershipsStore {
            users: HashMap::from([(5, member_user(5, "bob"))]),
            orgs: HashMap::from([(7, org(7, "team", Some("Team")))]),
            namespaces: HashMap::from([(
                "team".to_owned(),
                NamespaceName {
                    name: "team".to_owned(),
                    user_id: 0,
                    org_id: 7,
                },
            )]),
            ..FakeOrgMembershipsStore::default()
        };
        let actor = Actor::User { uid: 5 };
        let user_id = relay_id("User", 5);
        let query = format!(
            "{{ namespace(id: \"{user_id}\") {{ __typename id namespaceName url \
               ... on User {{ username }} }} \
               namespaceByName(name: \"TEAM\") {{ __typename namespaceName url \
               ... on Org {{ name }} }} }}"
        );
        let variables = Map::new();
        let request = ExecRequest {
            query: &query,
            operation_name: "",
            variables: &variables,
            actor,
        };
        let fields = sg_graphql::exec::root_fields(&schema, &request);
        let namespaces = prefetch_namespace_roots(&memberships_store, &fields, actor, false).await;
        let roots = empty_roots().with_namespaces(namespaces);
        assert_eq!(
            run(&schema, &query, actor, &roots),
            format!(
                r#"{{"data":{{"namespace":{{"__typename":"User","id":"{user_id}","namespaceName":"bob","url":"/users/bob","username":"bob"}},"namespaceByName":{{"__typename":"Org","namespaceName":"team","url":"/organizations/team","name":"team"}}}}}}"#
            )
        );
    }

    #[tokio::test]
    async fn namespace_roots_report_baseline_errors() {
        let schema = schema();
        let memberships_store = FakeOrgMembershipsStore {
            // A team row has neither a user nor an org ID; the baseline panics.
            namespaces: HashMap::from([(
                "team".to_owned(),
                NamespaceName {
                    name: "team".to_owned(),
                    user_id: 0,
                    org_id: 0,
                },
            )]),
            ..FakeOrgMembershipsStore::default()
        };
        let actor = Actor::Anonymous;
        let unknown = base64::engine::general_purpose::URL_SAFE.encode("Repository:1");
        let query = format!(
            "{{ invalid: namespace(id: \"{unknown}\") {{ id }} \
               missing: namespaceByName(name: \"absent\") {{ id }} \
               team: namespaceByName(name: \"team\") {{ id }} }}"
        );
        let variables = Map::new();
        let request = ExecRequest {
            query: &query,
            operation_name: "",
            variables: &variables,
            actor,
        };
        let fields = sg_graphql::exec::root_fields(&schema, &request);
        let namespaces = prefetch_namespace_roots(&memberships_store, &fields, actor, false).await;
        let roots = empty_roots().with_namespaces(namespaces);
        let response = run(&schema, &query, actor, &roots);
        assert!(response.contains(r#""invalid":null"#), "{response}");
        assert!(response.contains("invalid ID"), "{response}");
        assert!(response.contains(r#""missing":null"#), "{response}");
        assert!(
            response.contains("neither user nor organization"),
            "{response}"
        );
        assert!(response.contains(r#""team":null"#), "{response}");
    }
}
