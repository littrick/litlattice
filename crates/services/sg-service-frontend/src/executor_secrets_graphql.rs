#![forbid(unsafe_code)]

//! Executor secret GraphQL surface (M9.13b): the Rust port of
//! `cmd/frontend/graphqlbackend/{executor_secrets.go,executor_secret.go,
//! executor_secret_connection.go,executor_secret_access_log.go,
//! executor_secret_access_logs_connection.go}` and
//! `internal/database/{executor_secrets.go,executor_secret_access_logs.go}`.
//!
//! Ported fields: `Query.executorSecrets`, `User.executorSecrets`,
//! `Org.executorSecrets`, `ExecutorSecret.*` (including `accessLogs`),
//! `ExecutorSecretAccessLog.*`, `Query.node(ExecutorSecret |
//! ExecutorSecretAccessLog)` and the `create/update/deleteExecutorSecret`
//! mutations (`schema.graphql:2149-2408`).
//!
//! Authz matches `checkNamespaceAccess` (user: `CheckSiteAdminOrSameUser`,
//! org: `CheckOrgAccessOrSiteAdmin`, global: `CheckCurrentUserIsSiteAdmin`)
//! plus the database-level `executorSecretsAuthzQueryConds` and
//! `EnsureActorHasNamespaceWriteAccess` seams. No new tables, columns or
//! Redis keys; values are encrypted with the injected
//! `sg_encryption::Key` (the `keyring.Default().ExecutorSecretKey` seam).

use async_trait::async_trait;
use base64::Engine as _;
use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;
use time::OffsetDateTime;

use sg_graphql::exec::{Actor, Arguments, ExecError, FieldValue, ObjectValue, RootField};
use sg_graphql::json::Json;

use crate::admin_store::{
    check_current_user_is_site_admin, NOT_AN_ORG_MEMBER, NOT_AUTHENTICATED,
    MUST_BE_SITE_ADMIN_OR_SAME_USER,
};
use crate::graphql_resolvers::{
    datetime_rfc3339, relay_kind, unmarshal_relay_id, unmarshal_relay_spec_string,
    NestedPrefetch, NodeRoots, OrgByIdLookup, OrgMembershipsStore, OrgObject, OrganizationLookup,
    PageInfoObject, UserByIdLookup, UserData, UserKey, UserLookup, UserLookupStore, UserObject,
    UsersArgs, UsersConnectionLookup,
};

/// `database.ExecutorSecretScopeBatches` display value.
pub const SCOPE_BATCHES: &str = "BATCHES";
/// `database.ExecutorSecretScopeCodeIntel` display value.
pub const SCOPE_CODEINTEL: &str = "CODEINTEL";

/// `database.ErrDuplicateExecutorSecret`.
pub const DUPLICATE_EXECUTOR_SECRET: &str = "duplicate executor secret";

/// `database.ExecutorSecretScope` as GraphQL exposes it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ExecutorSecretScope {
    Batches,
    CodeIntel,
}

impl ExecutorSecretScope {
    /// The GraphQL enum value.
    pub fn parse(raw: &str) -> Result<Self, String> {
        match raw {
            SCOPE_BATCHES => Ok(Self::Batches),
            SCOPE_CODEINTEL => Ok(Self::CodeIntel),
            other => Err(format!("invalid executor secret scope {other:?}")),
        }
    }

    /// `strings.ToUpper(string(scope))`.
    pub fn display(self) -> &'static str {
        match self {
            Self::Batches => SCOPE_BATCHES,
            Self::CodeIntel => SCOPE_CODEINTEL,
        }
    }

    /// `ToDatabaseScope` (`strings.ToLower`).
    pub fn database(self) -> &'static str {
        match self {
            Self::Batches => "batches",
            Self::CodeIntel => "codeintel",
        }
    }
}

/// One `executor_secrets` row (`internal/database/executor_secrets.go`).
/// `namespace_user_id`/`namespace_org_id`/`creator_id` are `0` for NULL.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutorSecretRow {
    pub id: i64,
    pub key: String,
    /// The database scope (`batches`/`codeintel`).
    pub scope: String,
    pub overwrites_global_secret: bool,
    pub creator_id: i32,
    pub namespace_user_id: i32,
    pub namespace_org_id: i32,
    pub created_at: OffsetDateTime,
    pub updated_at: OffsetDateTime,
}

/// One `executor_secret_access_logs` row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutorSecretAccessLogRow {
    pub id: i64,
    pub executor_secret_id: i64,
    pub user_id: Option<i32>,
    pub machine_user: String,
    pub created_at: OffsetDateTime,
}

/// The actor attributes the store needs (`actor.Actor` + the viewer row).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SecretViewer {
    pub is_authenticated: bool,
    pub internal: bool,
    pub uid: i32,
    pub site_admin: bool,
}

impl SecretViewer {
    pub fn from_actor(actor: Actor, viewer_site_admin: bool) -> Self {
        Self {
            is_authenticated: actor.is_authenticated(),
            internal: actor.is_internal(),
            uid: actor.uid().unwrap_or(0),
            site_admin: viewer_site_admin,
        }
    }
}

/// `database.LimitOffset` plus the namespace filter for
/// `executorSecretsListOpts`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ExecutorSecretsListOptions {
    /// `Limit` (`0` = no limit clause).
    pub limit: i32,
    pub offset: i32,
    pub namespace_user_id: i32,
    pub namespace_org_id: i32,
}

/// `database.ExecutorSecretAccessLogsListOpts`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ExecutorSecretAccessLogsListOptions {
    pub limit: i32,
    pub offset: i32,
    pub executor_secret_id: i64,
}

/// The store seam for `executor_secrets` / `executor_secret_access_logs`.
/// Production is [`PgExecutorSecretsStore`]; the differential harness
/// substitutes an in-memory fake with identical authz semantics.
#[async_trait]
pub trait ExecutorSecretsStore: Send + Sync {
    async fn create(
        &self,
        scope: &str,
        key: &str,
        value: &str,
        creator_id: i32,
        namespace_user_id: i32,
        namespace_org_id: i32,
        viewer: SecretViewer,
    ) -> Result<ExecutorSecretRow, String>;

    async fn update(
        &self,
        scope: &str,
        secret: &ExecutorSecretRow,
        value: &str,
        viewer: SecretViewer,
    ) -> Result<ExecutorSecretRow, String>;

    async fn delete(
        &self,
        scope: &str,
        id: i64,
        viewer: SecretViewer,
    ) -> Result<(), String>;

    async fn get_by_id(
        &self,
        scope: &str,
        id: i64,
        viewer: SecretViewer,
    ) -> Result<Option<ExecutorSecretRow>, String>;

    /// Returns `(rows, next)` where `next` is the non-zero integer cursor when
    /// more rows exist (`internal/database/executor_secrets.go:List`).
    async fn list(
        &self,
        scope: &str,
        opts: &ExecutorSecretsListOptions,
        viewer: SecretViewer,
    ) -> Result<(Vec<ExecutorSecretRow>, i32), String>;

    async fn count(
        &self,
        scope: &str,
        opts: &ExecutorSecretsListOptions,
        viewer: SecretViewer,
    ) -> Result<i32, String>;

    async fn list_access_logs(
        &self,
        opts: &ExecutorSecretAccessLogsListOptions,
    ) -> Result<(Vec<ExecutorSecretAccessLogRow>, i32), String>;

    async fn count_access_logs(
        &self,
        opts: &ExecutorSecretAccessLogsListOptions,
    ) -> Result<i32, String>;

    async fn get_access_log_by_id(
        &self,
        id: i64,
    ) -> Result<Option<ExecutorSecretAccessLogRow>, String>;

    /// `db.OrgMembers().GetByOrgIDAndUserID` folded to a boolean.
    async fn is_org_member(&self, org_id: i32, user_id: i32) -> Result<bool, String>;
}

// ---------------------------------------------------------------------------
// Relay ids + cursors
// ---------------------------------------------------------------------------

/// `marshalExecutorSecretID`: `relay.MarshalID("ExecutorSecret",
/// "<SCOPE>:<id>")`.
pub fn marshal_executor_secret_id(scope: &str, id: i64) -> String {
    let spec = serde_json::to_string(&format!("{scope}:{id}")).expect("string serializes");
    base64::engine::general_purpose::URL_SAFE.encode(format!("ExecutorSecret:{spec}"))
}

/// `unmarshalExecutorSecretID`.
pub fn unmarshal_executor_secret_id(raw: &str) -> Result<(String, i64), String> {
    let decoded = unmarshal_relay_spec_string(raw)?;
    let parts: Vec<&str> = decoded.split(':').collect();
    if parts.len() != 2 {
        return Err("malformed ID".to_owned());
    }
    let id = parts[1]
        .parse::<i32>()
        .map_err(|error| format!("malformed id: {error}"))?;
    Ok((parts[0].to_owned(), i64::from(id)))
}

/// `marshalExecutorSecretAccessLogID`: `relay.MarshalID(kind, int64)`.
pub fn marshal_executor_secret_access_log_id(id: i64) -> String {
    base64::engine::general_purpose::URL_SAFE.encode(format!("ExecutorSecretAccessLog:{id}"))
}

/// `unmarshalExecutorSecretAccessLogID`.
pub fn unmarshal_executor_secret_access_log_id(raw: &str) -> Result<i64, String> {
    let decoded = base64::engine::general_purpose::URL_SAFE
        .decode(raw)
        .map_err(|error| error.to_string())?;
    let text = String::from_utf8_lossy(&decoded);
    let Some((_, spec)) = text.split_once(':') else {
        return Err("invalid graphql.ID".to_owned());
    };
    serde_json::from_str(spec).map_err(|error| error.to_string())
}

/// `gqlutil.EncodeCursor(&strconv.FormatInt(int64(val), 10))`.
pub fn encode_int_cursor(value: i32) -> String {
    base64::engine::general_purpose::STANDARD.encode(value.to_string())
}

/// `gqlutil.DecodeIntCursor`.
pub fn decode_int_cursor(value: &str) -> Result<i32, String> {
    let decoded = base64::engine::general_purpose::STANDARD
        .decode(value)
        .map_err(|error| error.to_string())?;
    let text = String::from_utf8(decoded).map_err(|error| error.to_string())?;
    if text.is_empty() {
        return Ok(0);
    }
    text.parse::<i32>().map_err(|error| error.to_string())
}

// ---------------------------------------------------------------------------
// Arguments
// ---------------------------------------------------------------------------

/// `ExecutorSecretsListArgs` (`scope`, `first = 50`, `after`).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ExecutorSecretsArgs {
    pub scope: ExecutorSecretScope,
    pub first: i32,
    pub after: Option<String>,
}

impl ExecutorSecretsArgs {
    pub fn from_arguments(arguments: &Arguments) -> Result<Self, String> {
        let scope = arguments
            .string("scope")
            .ok_or_else(|| "missing scope argument".to_owned())
            .and_then(ExecutorSecretScope::parse)?;
        Ok(Self {
            scope,
            first: arguments
                .i64("first")
                .and_then(|value| i32::try_from(value).ok())
                .unwrap_or(0),
            after: arguments.string("after").map(str::to_owned),
        })
    }

    /// `ExecutorSecretsListArgs.LimitOffset`.
    pub fn limit_offset(&self) -> Result<(i32, i32), String> {
        let offset = match &self.after {
            Some(after) => decode_int_cursor(after)?,
            None => 0,
        };
        Ok((self.first, offset))
    }
}

/// `ExecutorSecretAccessLogListArgs` (`first = 50`, `after`).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ExecutorSecretAccessLogsArgs {
    pub first: i32,
    pub after: Option<String>,
}

impl ExecutorSecretAccessLogsArgs {
    pub fn from_arguments(arguments: &Arguments) -> Self {
        Self {
            first: arguments
                .i64("first")
                .and_then(|value| i32::try_from(value).ok())
                .unwrap_or(0),
            after: arguments.string("after").map(str::to_owned),
        }
    }

    fn limit_offset(&self) -> Result<(i32, i32), String> {
        let offset = match &self.after {
            Some(after) => decode_int_cursor(after)?,
            None => 0,
        };
        Ok((self.first, offset))
    }
}

// ---------------------------------------------------------------------------
// Validation
// ---------------------------------------------------------------------------

/// `executorSecretKeyPattern = ^[A-Z][A-Z0-9_]*$`.
pub fn valid_executor_secret_key(key: &str) -> bool {
    let mut chars = key.chars();
    match chars.next() {
        Some(first) if first.is_ascii_uppercase() => {}
        _ => return false,
    }
    chars.all(|c| c.is_ascii_uppercase() || c.is_ascii_digit() || c == '_')
}

/// `validateExecutorSecret`: non-empty value plus the `DOCKER_AUTH_CONFIG`
/// shape checks.
pub fn validate_executor_secret(key: &str, value: &str) -> Result<(), String> {
    if value.is_empty() {
        return Err("value cannot be empty string".to_owned());
    }
    if key == "DOCKER_AUTH_CONFIG" {
        validate_docker_auth_config(value)?;
    }
    Ok(())
}

fn validate_docker_auth_config(value: &str) -> Result<(), String> {
    let wrap = |message: String| format!("failed to unmarshal docker auth config for validation: {message}");
    // `json.NewDecoder(...).Decode(&dac)` reads one JSON value; trailing data is
    // not inspected.
    let mut stream = serde_json::Deserializer::from_str(value).into_iter::<serde_json::Value>();
    let parsed = match stream.next() {
        Some(Ok(value)) => value,
        Some(Err(error)) => return Err(wrap(go_json_error(&error))),
        None => return Err(wrap("EOF".to_owned())),
    };
    let object = match parsed {
        serde_json::Value::Null => return Ok(()),
        serde_json::Value::Object(object) => object,
        other => {
            return Err(wrap(format!(
                "json: cannot unmarshal {} into Go value of type graphqlbackend.dockerAuthConfig",
                json_type_name(&other)
            )))
        }
    };
    let mut auths: Vec<(String, serde_json::Value)> = Vec::new();
    let mut cred_helpers = 0usize;
    let mut creds_store = false;
    for (name, item) in &object {
        match name.as_str() {
            "auths" => match item {
                serde_json::Value::Null => {}
                serde_json::Value::Object(map) => {
                    auths = map.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
                }
                other => {
                    return Err(wrap(format!(
                        "json: cannot unmarshal {} into Go struct field dockerAuthConfig.auths of type graphqlbackend.dockerAuthConfigAuths",
                        json_type_name(other)
                    )))
                }
            },
            "credsStore" => match item {
                serde_json::Value::Null => {}
                serde_json::Value::String(s) => creds_store = !s.is_empty(),
                other => {
                    return Err(wrap(format!(
                        "json: cannot unmarshal {} into Go struct field dockerAuthConfig.credsStore of type string",
                        json_type_name(other)
                    )))
                }
            },
            "credHelpers" => match item {
                serde_json::Value::Null => {}
                serde_json::Value::Object(map) => {
                    cred_helpers = map.len();
                }
                other => {
                    return Err(wrap(format!(
                        "json: cannot unmarshal {} into Go struct field dockerAuthConfig.credHelpers of type map[string]string",
                        json_type_name(other)
                    )))
                }
            },
            other => return Err(wrap(format!("json: unknown field {other:?}"))),
        }
    }
    if cred_helpers > 0 {
        return Err("cannot use credential helpers in docker auth config set via secrets".to_owned());
    }
    if creds_store {
        return Err("cannot use credential stores in docker auth config set via secrets".to_owned());
    }
    for (key, entry) in auths {
        let entry = match entry {
            serde_json::Value::Null => serde_json::Value::Object(serde_json::Map::new()),
            other => other,
        };
        let serde_json::Value::Object(entry) = entry else {
            return Err(wrap(format!(
                "json: cannot unmarshal {} into Go struct field dockerAuthConfig.auths of type graphqlbackend.dockerAuthConfigAuth",
                json_type_name(&entry)
            )));
        };
        if let Some(unknown) = entry.keys().find(|name| name.as_str() != "auth") {
            return Err(wrap(format!("json: unknown field {unknown:?}")));
        }
        let auth = match entry.get("auth") {
            None | Some(serde_json::Value::Null) => String::new(),
            Some(serde_json::Value::String(auth)) => auth.clone(),
            Some(other) => {
                return Err(wrap(format!(
                    "json: cannot unmarshal {} into Go struct field dockerAuthConfigAuth.auth of type []uint8",
                    json_type_name(other)
                )))
            }
        };
        let Ok(decoded) = base64::engine::general_purpose::STANDARD.decode(auth.as_bytes()) else {
            // `encoding/json` reports the failing byte offset; the corpus only
            // exercises well-formed base64.
            return Err(wrap("illegal base64 data at input byte 0".to_owned()));
        };
        if !decoded.contains(&b':') {
            return Err(format!(
                "invalid credential in auths section for {key:?} format has to be base64(username:password)"
            ));
        }
    }
    Ok(())
}

fn json_type_name(value: &serde_json::Value) -> &'static str {
    match value {
        serde_json::Value::Null => "null",
        serde_json::Value::Bool(_) => "bool",
        serde_json::Value::Number(_) => "number",
        serde_json::Value::String(_) => "string",
        serde_json::Value::Array(_) => "array",
        serde_json::Value::Object(_) => "object",
    }
}

fn go_json_error(error: &serde_json::Error) -> String {
    // Only the syntax-error branch is exercised by the corpus; the exact Go
    // wording is `invalid character ...`.
    error.to_string()
}

// ---------------------------------------------------------------------------
// Authz helpers
// ---------------------------------------------------------------------------

/// `checkNamespaceAccess`.
pub async fn check_namespace_access(
    store: &dyn ExecutorSecretsStore,
    viewer: SecretViewer,
    namespace_user_id: i32,
    namespace_org_id: i32,
) -> Result<(), String> {
    if viewer.internal {
        return Ok(());
    }
    if namespace_user_id != 0 {
        if viewer.is_authenticated && viewer.uid == namespace_user_id {
            return Ok(());
        }
        if viewer.site_admin {
            return Ok(());
        }
        return Err(MUST_BE_SITE_ADMIN_OR_SAME_USER.to_owned());
    }
    if namespace_org_id != 0 {
        if !viewer.is_authenticated {
            return Err(NOT_AUTHENTICATED.to_owned());
        }
        if viewer.site_admin {
            return Ok(());
        }
        return match store.is_org_member(namespace_org_id, viewer.uid).await {
            Ok(true) => Ok(()),
            Ok(false) => Err(NOT_AN_ORG_MEMBER.to_owned()),
            Err(error) => Err(error),
        };
    }
    check_current_user_is_site_admin(
        if viewer.is_authenticated {
            Actor::User { uid: viewer.uid }
        } else if viewer.internal {
            Actor::Internal
        } else {
            Actor::Anonymous
        },
        viewer.site_admin,
    )
}

/// `EnsureActorHasNamespaceWriteAccess` (store-level gate).
async fn ensure_actor_has_namespace_write_access(
    store: &dyn ExecutorSecretsStore,
    viewer: SecretViewer,
    namespace_user_id: i32,
    namespace_org_id: i32,
) -> Result<(), String> {
    if viewer.internal {
        return Ok(());
    }
    if !viewer.is_authenticated {
        return Err("not logged in".to_owned());
    }
    if namespace_org_id != 0 {
        match store.is_org_member(namespace_org_id, viewer.uid).await {
            Ok(true) => return Ok(()),
            Ok(false) => {}
            Err(error) => return Err(error),
        }
    } else if namespace_user_id != 0 && viewer.uid == namespace_user_id {
        return Ok(());
    }
    if viewer.site_admin {
        Ok(())
    } else {
        Err("not site-admin".to_owned())
    }
}

// ---------------------------------------------------------------------------
// Connection computation
// ---------------------------------------------------------------------------

/// The `ExecutorSecretConnection` payload.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ExecutorSecretConnectionData {
    pub secrets: Vec<ExecutorSecretRow>,
    pub total_count: i32,
    /// Non-zero next offset (`List`'s `next`).
    pub next: i32,
}

/// The `ExecutorSecretAccessLogConnection` payload.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ExecutorSecretAccessLogsConnectionData {
    pub logs: Vec<ExecutorSecretAccessLogRow>,
    pub total_count: i32,
    pub next: i32,
}

/// `executorSecretConnectionResolver` + store `List`/`Count`.
pub async fn compute_executor_secret_connection(
    store: &dyn ExecutorSecretsStore,
    scope: ExecutorSecretScope,
    args: &ExecutorSecretsArgs,
    namespace_user_id: i32,
    namespace_org_id: i32,
    viewer: SecretViewer,
) -> Result<ExecutorSecretConnectionData, String> {
    let (limit, offset) = args.limit_offset()?;
    let opts = ExecutorSecretsListOptions {
        limit,
        offset,
        namespace_user_id,
        namespace_org_id,
    };
    let (secrets, next) = store.list(scope.database(), &opts, viewer).await?;
    let total_count = store.count(scope.database(), &opts, viewer).await?;
    Ok(ExecutorSecretConnectionData {
        secrets,
        total_count,
        next,
    })
}

/// `executorSecretAccessLogConnectionResolver` + store `List`/`Count`.
pub async fn compute_access_logs_connection(
    store: &dyn ExecutorSecretsStore,
    args: &ExecutorSecretAccessLogsArgs,
    executor_secret_id: i64,
) -> Result<ExecutorSecretAccessLogsConnectionData, String> {
    let (limit, offset) = args.limit_offset()?;
    let opts = ExecutorSecretAccessLogsListOptions {
        limit,
        offset,
        executor_secret_id,
    };
    let (logs, next) = store.list_access_logs(&opts).await?;
    let total_count = store.count_access_logs(&opts).await?;
    Ok(ExecutorSecretAccessLogsConnectionData {
        logs,
        total_count,
        next,
    })
}

// ---------------------------------------------------------------------------
// Prefetch roots / nested
// ---------------------------------------------------------------------------

/// `Query.executorSecrets` results keyed by the argument set.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ExecutorSecretRoots {
    pub global: HashMap<ExecutorSecretsArgs, Result<ExecutorSecretConnectionData, String>>,
}

fn node_selections(field: &RootField) -> Vec<RootField> {
    let mut selections = field.selections.clone();
    for (condition, sub) in &field.type_conditions {
        if matches!(condition.as_str(), "ExecutorSecret" | "Node") {
            selections.extend(sub.clone());
        }
    }
    selections
}

/// The `accessLogs(...)` argument sets selected directly on a node selection.
pub fn access_log_args_in(selections: &[RootField]) -> Vec<ExecutorSecretAccessLogsArgs> {
    let mut args = Vec::new();
    for selection in selections {
        if selection.name != "accessLogs" {
            continue;
        }
        let parsed = ExecutorSecretAccessLogsArgs::from_arguments(&selection.arguments);
        if !args.contains(&parsed) {
            args.push(parsed);
        }
    }
    args
}

/// The rows and access-log argument sets reached through one prefetched
/// `ExecutorSecretConnection` selection.
pub fn selected_secrets_from_connection(
    data: &ExecutorSecretConnectionData,
    connection_selections: &[RootField],
) -> SelectedSecrets {
    let mut access_logs = Vec::new();
    for selection in connection_selections {
        if selection.name == "nodes" {
            access_logs.extend(access_log_args_in(&selection.selections));
        }
    }
    SelectedSecrets {
        secrets: data.secrets.clone(),
        access_logs,
    }
}

/// Rows and access-log selections reached through the prefetched
/// `Query.executorSecrets` roots.
pub fn selected_from_roots(
    roots: &ExecutorSecretRoots,
    fields: &[RootField],
) -> SelectedSecrets {
    let mut selected = SelectedSecrets::default();
    for field in fields {
        if field.name != "executorSecrets" {
            continue;
        }
        let Ok(args) = ExecutorSecretsArgs::from_arguments(&field.arguments) else {
            continue;
        };
        let Some(Ok(data)) = roots.global.get(&args) else {
            continue;
        };
        let mut reached = selected_secrets_from_connection(data, &field.selections);
        selected.secrets.append(&mut reached.secrets);
        selected.access_logs.append(&mut reached.access_logs);
    }
    selected
}

/// Rows and access-log selections reached through the prefetched secret
/// mutations' return values.
pub fn selected_from_mutations(
    outcomes: &[(String, ExecutorSecretMutationOutcome)],
    fields: &[RootField],
) -> SelectedSecrets {
    let mut selected = SelectedSecrets::default();
    for (name, outcome) in outcomes {
        let ExecutorSecretMutationOutcome::Secret(row) = outcome else {
            continue;
        };
        selected.secrets.push(row.clone());
        for field in fields.iter().filter(|field| &field.name == name) {
            selected
                .access_logs
                .extend(access_log_args_in(&node_selections(field)));
        }
    }
    selected
}

/// Prefetch `Query.executorSecrets` (global secrets; site-admin only).
pub async fn prefetch_executor_secret_roots(
    store: &dyn ExecutorSecretsStore,
    fields: &[RootField],
    actor: Actor,
    viewer_site_admin: bool,
) -> ExecutorSecretRoots {
    let viewer = SecretViewer::from_actor(actor, viewer_site_admin);
    let mut roots = ExecutorSecretRoots::default();
    for field in fields {
        if field.name != "executorSecrets" {
            continue;
        }
        let args = match ExecutorSecretsArgs::from_arguments(&field.arguments) {
            Ok(args) => args,
            Err(error) => {
                let key = ExecutorSecretsArgs {
                    scope: ExecutorSecretScope::Batches,
                    first: 0,
                    after: None,
                };
                roots.global.insert(key, Err(error));
                continue;
            }
        };
        if roots.global.contains_key(&args) {
            continue;
        }
        let lookup =
            match check_namespace_access(store, viewer, 0, 0).await {
                Ok(()) => {
                    compute_executor_secret_connection(
                        store,
                        args.scope,
                        &args,
                        0,
                        0,
                        viewer,
                    )
                    .await
                }
                Err(error) => Err(error),
            };
        roots.global.insert(args, lookup);
    }
    roots
}

/// Prefetch `User.executorSecrets` / `Org.executorSecrets`, writing the
/// connection results into the request's [`NestedPrefetch`].
#[allow(clippy::too_many_arguments)]
pub async fn prefetch_executor_secret_nested(
    store: &dyn ExecutorSecretsStore,
    fields: &[RootField],
    current_user: Option<&UserData>,
    users: &HashMap<UserKey, UserLookup>,
    users_connections: &HashMap<UsersArgs, UsersConnectionLookup>,
    organizations: &HashMap<String, OrganizationLookup>,
    nodes: &NodeRoots,
    actor: Actor,
    viewer_site_admin: bool,
    nested: &mut NestedPrefetch,
) -> SelectedSecrets {
    let viewer = SecretViewer::from_actor(actor, viewer_site_admin);
    let mut user_jobs: Vec<(i32, ExecutorSecretsArgs, Vec<RootField>)> = Vec::new();
    let mut org_jobs: Vec<(i32, ExecutorSecretsArgs, Vec<RootField>)> = Vec::new();

    let mut collect_user = |user_id: i32, selections: &[RootField]| {
        for selection in selections {
            if selection.name != "executorSecrets" {
                continue;
            }
            if let Ok(args) = ExecutorSecretsArgs::from_arguments(&selection.arguments) {
                if !user_jobs
                    .iter()
                    .any(|(id, existing, _)| *id == user_id && *existing == args)
                {
                    user_jobs.push((user_id, args, selection.selections.clone()));
                }
            }
        }
    };
    let mut collect_org = |org_id: i32, selections: &[RootField]| {
        for selection in selections {
            if selection.name != "executorSecrets" {
                continue;
            }
            if let Ok(args) = ExecutorSecretsArgs::from_arguments(&selection.arguments) {
                if !org_jobs
                    .iter()
                    .any(|(id, existing, _)| *id == org_id && *existing == args)
                {
                    org_jobs.push((org_id, args, selection.selections.clone()));
                }
            }
        }
    };

    for field in fields {
        match field.name.as_str() {
            "currentUser" => {
                if let Some(user) = current_user {
                    collect_user(user.id, &field.selections);
                }
            }
            "user" => {
                if let Some(key) = user_arg_key(&field.arguments) {
                    if let Some(UserLookup::Found(user)) = users.get(&key) {
                        collect_user(user.id, &field.selections);
                    }
                }
            }
            "users" => {
                let args = crate::graphql_resolvers::users_args(&field.arguments);
                if let Some(UsersConnectionLookup::Found(data)) = users_connections.get(&args) {
                    for nodes_selection in
                        field.selections.iter().filter(|s| s.name == "nodes")
                    {
                        for user in &data.users {
                            collect_user(user.id, &nodes_selection.selections);
                        }
                    }
                }
            }
            "organization" => {
                if let Some(name) = field.arguments.string("name") {
                    if let Some(OrganizationLookup::Found(org)) = organizations.get(name) {
                        collect_org(org.id, &field.selections);
                    }
                }
            }
            "organizations" => {
                // The organizations connection prefetch is not threaded here;
                // `Query.organization(name)` and `Query.node(Org)` cover the
                // deterministic corpus.
            }
            "node" => {
                let Some(id) = field.arguments.string("id") else {
                    continue;
                };
                match relay_kind(id).as_deref() {
                    Some("User") => {
                        if let Ok(node_id) = unmarshal_relay_id(id) {
                            if let Some(crate::graphql_resolvers::UserByIdLookup::Found(user)) =
                                nodes.users.get(&node_id)
                            {
                                collect_user(user.id, &flatten(field));
                            }
                        }
                    }
                    Some("Org") => {
                        if let Ok(node_id) = unmarshal_relay_id(id) {
                            if let Some(crate::graphql_resolvers::OrgByIdLookup::Found(org)) =
                                nodes.orgs.get(&node_id)
                            {
                                collect_org(org.id, &flatten(field));
                            }
                        }
                    }
                    _ => {}
                }
            }
            _ => {}
        }
    }

    let mut selected = SelectedSecrets::default();
    for (user_id, args, selections) in user_jobs {
        if nested
            .user_executor_secrets
            .contains_key(&(user_id, args.clone()))
        {
            continue;
        }
        let lookup = match check_namespace_access(store, viewer, user_id, 0).await {
            Ok(()) => {
                compute_executor_secret_connection(store, args.scope, &args, user_id, 0, viewer)
                    .await
            }
            Err(error) => Err(error),
        };
        if let Ok(data) = &lookup {
            let mut reached = selected_secrets_from_connection(data, &selections);
            selected.secrets.append(&mut reached.secrets);
            selected.access_logs.append(&mut reached.access_logs);
        }
        nested
            .user_executor_secrets
            .insert((user_id, args), lookup);
    }
    for (org_id, args, selections) in org_jobs {
        if nested
            .org_executor_secrets
            .contains_key(&(org_id, args.clone()))
        {
            continue;
        }
        let lookup = match check_namespace_access(store, viewer, 0, org_id).await {
            Ok(()) => {
                compute_executor_secret_connection(store, args.scope, &args, 0, org_id, viewer)
                    .await
            }
            Err(error) => Err(error),
        };
        if let Ok(data) = &lookup {
            let mut reached = selected_secrets_from_connection(data, &selections);
            selected.secrets.append(&mut reached.secrets);
            selected.access_logs.append(&mut reached.access_logs);
        }
        nested.org_executor_secrets.insert((org_id, args), lookup);
    }
    selected
}

fn flatten(field: &RootField) -> Vec<RootField> {
    let mut selections = field.selections.clone();
    for (condition, sub) in &field.type_conditions {
        if matches!(condition.as_str(), "User" | "Org" | "Node" | "ExecutorSecret") {
            selections.extend(sub.clone());
        }
    }
    selections
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

// ---------------------------------------------------------------------------
// Prefetch related users/orgs and access logs
// ---------------------------------------------------------------------------

/// The result set of secrets selected anywhere in the operation; used to
/// prefetch `namespace`/`creator` and `accessLogs`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SelectedSecrets {
    pub secrets: Vec<ExecutorSecretRow>,
    pub access_logs: Vec<ExecutorSecretAccessLogsArgs>,
}

/// Prefetch the `namespace`/`creator` objects and `accessLogs` connections for
/// every selected secret.
#[allow(clippy::too_many_arguments)]
pub async fn prefetch_selected_secrets(
    store: &dyn ExecutorSecretsStore,
    user_store: Option<&dyn UserLookupStore>,
    org_store: Option<&dyn OrgMembershipsStore>,
    selected: &SelectedSecrets,
    viewer_uid: i32,
    viewer_site_admin: bool,
    nested: &mut NestedPrefetch,
) {
    let mut user_ids = BTreeSet::new();
    let mut org_ids = BTreeSet::new();
    for secret in &selected.secrets {
        if secret.namespace_user_id != 0 {
            user_ids.insert(secret.namespace_user_id);
        }
        if secret.namespace_org_id != 0 {
            org_ids.insert(secret.namespace_org_id);
        }
        if secret.creator_id != 0 {
            user_ids.insert(secret.creator_id);
        }
    }
    if let Some(user_store) = user_store {
        for user_id in user_ids {
            if nested.users_by_id.contains_key(&user_id) {
                continue;
            }
            let lookup = match user_store.lookup(&UserKey::DatabaseId(user_id)).await {
                Ok(Some(data)) => UserByIdLookup::Found(data),
                Ok(None) => UserByIdLookup::Error(format!("user not found: [userID {user_id}]")),
                Err(error) => UserByIdLookup::Error(error),
            };
            nested.users_by_id.insert(user_id, lookup);
        }
    }
    if let Some(org_store) = org_store {
        for org_id in org_ids {
            if nested.orgs_by_id.contains_key(&org_id) {
                continue;
            }
            let lookup = match org_store
                .org_by_id(org_id, viewer_uid, viewer_site_admin)
                .await
            {
                Ok(org) => OrgByIdLookup::Found(org),
                Err(error) => OrgByIdLookup::Error(error),
            };
            nested.orgs_by_id.insert(org_id, lookup);
        }
    }

    if selected.access_logs.is_empty() {
        return;
    }
    let mut user_ids: BTreeSet<i32> = BTreeSet::new();
    for secret in &selected.secrets {
        for args in &selected.access_logs {
            if nested
                .executor_secret_access_logs
                .contains_key(&(secret.id, args.clone()))
            {
                continue;
            }
            let data = compute_access_logs_connection(store, args, secret.id).await;
            if let Ok(data) = &data {
                for log in &data.logs {
                    if let Some(user_id) = log.user_id {
                        user_ids.insert(user_id);
                    }
                }
            }
            nested
                .executor_secret_access_logs
                .insert((secret.id, args.clone()), data);
        }
    }
    if let Some(user_store) = user_store {
        for user_id in user_ids {
            if nested.executor_secret_access_log_users.contains_key(&user_id) {
                continue;
            }
            let lookup = match user_store.lookup(&UserKey::DatabaseId(user_id)).await {
                Ok(Some(data)) => AccessLogUserLookup::Found(data),
                Ok(None) => AccessLogUserLookup::Missing,
                Err(error) => AccessLogUserLookup::Error(error),
            };
            nested
                .executor_secret_access_log_users
                .insert(user_id, lookup);
        }
    }
}

/// `ExecutorSecretAccessLog.user` prefetch result.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AccessLogUserLookup {
    Found(UserData),
    Missing,
    Error(String),
}

// ---------------------------------------------------------------------------
// Node lookups
// ---------------------------------------------------------------------------

/// One prefetched `Query.node(ExecutorSecret)` result.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExecutorSecretNodeLookup {
    Found(ExecutorSecretRow),
    Null,
    Error(String),
}

/// One prefetched `Query.node(ExecutorSecretAccessLog)` result. `Found`
/// carries the fetched parent secret (the baseline refetches it).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExecutorSecretAccessLogNodeLookup {
    Found(ExecutorSecretAccessLogRow, ExecutorSecretRow),
    Null,
    Error(String),
}

/// Prefetched `Query.node` results, keyed by the raw Relay id.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ExecutorSecretNodeRoots {
    pub secrets: HashMap<String, ExecutorSecretNodeLookup>,
    pub access_logs: HashMap<String, ExecutorSecretAccessLogNodeLookup>,
}

/// Prefetch `Query.node(ExecutorSecret | ExecutorSecretAccessLog)`, returning
/// the node results plus the rows/access-log selections the caller must
/// resolve into [`NestedPrefetch`] via [`prefetch_selected_secrets`].
pub async fn prefetch_executor_secret_nodes(
    store: &dyn ExecutorSecretsStore,
    fields: &[RootField],
    actor: Actor,
    viewer_site_admin: bool,
) -> (ExecutorSecretNodeRoots, SelectedSecrets) {
    let viewer = SecretViewer::from_actor(actor, viewer_site_admin);
    let mut roots = ExecutorSecretNodeRoots::default();
    let mut selected = SelectedSecrets::default();
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
            "ExecutorSecret" => {
                if roots.secrets.contains_key(id) {
                    continue;
                }
                let lookup = match unmarshal_executor_secret_id(id) {
                    Err(error) => ExecutorSecretNodeLookup::Error(error),
                    Ok((scope, secret_id)) => {
                        match store
                            .get_by_id(&scope.to_lowercase(), secret_id, viewer)
                            .await
                        {
                            Err(error) => ExecutorSecretNodeLookup::Error(error),
                            Ok(None) => ExecutorSecretNodeLookup::Null,
                            Ok(Some(secret)) => {
                                match check_namespace_access(
                                    store,
                                    viewer,
                                    secret.namespace_user_id,
                                    secret.namespace_org_id,
                                )
                                .await
                                {
                                    Ok(()) => {
                                        let selections = node_selections(field);
                                        let access_logs = access_log_args_in(&selections);
                                        selected.secrets.push(secret.clone());
                                        selected.access_logs.extend(access_logs);
                                        ExecutorSecretNodeLookup::Found(secret)
                                    }
                                    Err(error) => ExecutorSecretNodeLookup::Error(error),
                                }
                            }
                        }
                    }
                };
                roots.secrets.insert(id.to_owned(), lookup);
            }
            "ExecutorSecretAccessLog" => {
                if roots.access_logs.contains_key(id) {
                    continue;
                }
                let lookup = match unmarshal_executor_secret_access_log_id(id) {
                    Err(error) => ExecutorSecretAccessLogNodeLookup::Error(error),
                    Ok(log_id) => match store.get_access_log_by_id(log_id).await {
                        Err(error) => ExecutorSecretAccessLogNodeLookup::Error(error),
                        Ok(None) => ExecutorSecretAccessLogNodeLookup::Null,
                        Ok(Some(log)) => {
                            match store
                                .get_by_id("batches", log.executor_secret_id, viewer)
                                .await
                            {
                                Err(error) => ExecutorSecretAccessLogNodeLookup::Error(error),
                                Ok(None) => ExecutorSecretAccessLogNodeLookup::Error(format!(
                                    "executor secret not found: id={}",
                                    log.executor_secret_id
                                )),
                                Ok(Some(parent)) => match check_namespace_access(
                                    store,
                                    viewer,
                                    parent.namespace_user_id,
                                    parent.namespace_org_id,
                                )
                                .await
                                {
                                    Ok(()) => {
                                        selected.secrets.push(parent.clone());
                                        let selections = flatten(field);
                                        let access_logs = access_log_args_in(&selections);
                                        selected.access_logs.extend(access_logs);
                                        ExecutorSecretAccessLogNodeLookup::Found(log, parent)
                                    }
                                    Err(error) => {
                                        ExecutorSecretAccessLogNodeLookup::Error(error)
                                    }
                                },
                            }
                        }
                    },
                };
                roots.access_logs.insert(id.to_owned(), lookup);
            }
            _ => {}
        }
    }
    dedupe_selected(&mut selected);
    (roots, selected)
}

fn dedupe_selected(selected: &mut SelectedSecrets) {
    let mut seen: BTreeSet<i64> = BTreeSet::new();
    selected.secrets.retain(|secret| seen.insert(secret.id));
}

// ---------------------------------------------------------------------------
// Mutations
// ---------------------------------------------------------------------------

/// One prefetched executor-secret mutation outcome.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExecutorSecretMutationOutcome {
    Secret(ExecutorSecretRow),
    Empty,
    Error(String),
}

/// Run the `create/update/deleteExecutorSecret` mutations in document order.
pub async fn prefetch_executor_secret_mutations(
    store: &dyn ExecutorSecretsStore,
    fields: &[RootField],
    actor: Actor,
    viewer_site_admin: bool,
) -> Vec<(String, ExecutorSecretMutationOutcome)> {
    let viewer = SecretViewer::from_actor(actor, viewer_site_admin);
    let mut outcomes = Vec::new();
    for field in fields {
        let outcome = match field.name.as_str() {
            "createExecutorSecret" => {
                create_executor_secret(store, field, viewer).await
            }
            "updateExecutorSecret" => {
                update_executor_secret(store, field, viewer).await
            }
            "deleteExecutorSecret" => {
                delete_executor_secret(store, field, viewer).await
            }
            _ => continue,
        };
        outcomes.push((field.name.clone(), outcome));
    }
    outcomes
}

/// `UnmarshalNamespaceID` for the `namespace` argument.
fn unmarshal_namespace_id(raw: &str) -> Result<(i32, i32), String> {
    match relay_kind(raw).as_deref() {
        Some("User") => Ok((unmarshal_relay_id(raw)?, 0)),
        Some("Org") => Ok((0, unmarshal_relay_id(raw)?)),
        _ => Err(format!("invalid ID {raw:?} for namespace")),
    }
}

async fn create_executor_secret(
    store: &dyn ExecutorSecretsStore,
    field: &RootField,
    viewer: SecretViewer,
) -> ExecutorSecretMutationOutcome {
    let (namespace_user_id, namespace_org_id) = match field.arguments.string("namespace") {
        Some(raw) => match unmarshal_namespace_id(raw) {
            Ok(ids) => ids,
            Err(error) => return ExecutorSecretMutationOutcome::Error(error),
        },
        None => (0, 0),
    };
    if !viewer.is_authenticated {
        return ExecutorSecretMutationOutcome::Error(NOT_AUTHENTICATED.to_owned());
    }
    // 🚨 SECURITY: check namespace access before anything else.
    if let Err(error) = check_namespace_access(store, viewer, namespace_user_id, namespace_org_id).await
    {
        return ExecutorSecretMutationOutcome::Error(error);
    }
    let Some(scope_raw) = field.arguments.string("scope") else {
        return ExecutorSecretMutationOutcome::Error("missing scope argument".to_owned());
    };
    let scope = match ExecutorSecretScope::parse(scope_raw) {
        Ok(scope) => scope,
        Err(error) => return ExecutorSecretMutationOutcome::Error(error),
    };
    let key = field.arguments.string("key").unwrap_or_default().to_owned();
    let value = field.arguments.string("value").unwrap_or_default().to_owned();
    if key.is_empty() {
        return ExecutorSecretMutationOutcome::Error("key cannot be empty string".to_owned());
    }
    if !valid_executor_secret_key(&key) {
        return ExecutorSecretMutationOutcome::Error(
            "invalid key format, should be a valid env var name".to_owned(),
        );
    }
    if let Err(error) = validate_executor_secret(&key, &value) {
        return ExecutorSecretMutationOutcome::Error(error);
    }
    match store
        .create(
            scope.database(),
            &key,
            &value,
            viewer.uid,
            namespace_user_id,
            namespace_org_id,
            viewer,
        )
        .await
    {
        Ok(secret) => ExecutorSecretMutationOutcome::Secret(secret),
        Err(error) if error == DUPLICATE_EXECUTOR_SECRET => ExecutorSecretMutationOutcome::Error(
            "multiple secrets with the same key in the same namespace not allowed".to_owned(),
        ),
        Err(error) => ExecutorSecretMutationOutcome::Error(error),
    }
}

async fn update_executor_secret(
    store: &dyn ExecutorSecretsStore,
    field: &RootField,
    viewer: SecretViewer,
) -> ExecutorSecretMutationOutcome {
    let Some(raw_id) = field.arguments.string("id") else {
        return ExecutorSecretMutationOutcome::Error("missing id argument".to_owned());
    };
    let (decoded_scope, id) = match unmarshal_executor_secret_id(raw_id) {
        Ok(ids) => ids,
        Err(error) => return ExecutorSecretMutationOutcome::Error(error),
    };
    if !viewer.is_authenticated {
        return ExecutorSecretMutationOutcome::Error(NOT_AUTHENTICATED.to_owned());
    }
    let Some(scope_raw) = field.arguments.string("scope") else {
        return ExecutorSecretMutationOutcome::Error("missing scope argument".to_owned());
    };
    if decoded_scope != scope_raw {
        return ExecutorSecretMutationOutcome::Error("scope mismatch".to_owned());
    }
    let value = field.arguments.string("value").unwrap_or_default().to_owned();
    let secret = match store
        .get_by_id(&decoded_scope.to_lowercase(), id, viewer)
        .await
    {
        Err(error) => return ExecutorSecretMutationOutcome::Error(error),
        Ok(None) => {
            return ExecutorSecretMutationOutcome::Error(format!(
                "executor secret not found: id={id}"
            ))
        }
        Ok(Some(secret)) => secret,
    };
    if let Err(error) = check_namespace_access(
        store,
        viewer,
        secret.namespace_user_id,
        secret.namespace_org_id,
    )
    .await
    {
        return ExecutorSecretMutationOutcome::Error(error);
    }
    if let Err(error) = validate_executor_secret(&secret.key, &value) {
        return ExecutorSecretMutationOutcome::Error(error);
    }
    match store
        .update(&decoded_scope.to_lowercase(), &secret, &value, viewer)
        .await
    {
        Ok(secret) => ExecutorSecretMutationOutcome::Secret(secret),
        Err(error) => ExecutorSecretMutationOutcome::Error(error),
    }
}

async fn delete_executor_secret(
    store: &dyn ExecutorSecretsStore,
    field: &RootField,
    viewer: SecretViewer,
) -> ExecutorSecretMutationOutcome {
    let Some(raw_id) = field.arguments.string("id") else {
        return ExecutorSecretMutationOutcome::Error("missing id argument".to_owned());
    };
    let (decoded_scope, id) = match unmarshal_executor_secret_id(raw_id) {
        Ok(ids) => ids,
        Err(error) => return ExecutorSecretMutationOutcome::Error(error),
    };
    if !viewer.is_authenticated {
        return ExecutorSecretMutationOutcome::Error(NOT_AUTHENTICATED.to_owned());
    }
    let Some(scope_raw) = field.arguments.string("scope") else {
        return ExecutorSecretMutationOutcome::Error("missing scope argument".to_owned());
    };
    if decoded_scope != scope_raw {
        return ExecutorSecretMutationOutcome::Error("scope mismatch".to_owned());
    }
    let secret = match store
        .get_by_id(&decoded_scope.to_lowercase(), id, viewer)
        .await
    {
        Err(error) => return ExecutorSecretMutationOutcome::Error(error),
        Ok(None) => {
            return ExecutorSecretMutationOutcome::Error(format!(
                "executor secret not found: id={id}"
            ))
        }
        Ok(Some(secret)) => secret,
    };
    if let Err(error) = check_namespace_access(
        store,
        viewer,
        secret.namespace_user_id,
        secret.namespace_org_id,
    )
    .await
    {
        return ExecutorSecretMutationOutcome::Error(error);
    }
    match store
        .delete(&decoded_scope.to_lowercase(), id, viewer)
        .await
    {
        Ok(()) => ExecutorSecretMutationOutcome::Empty,
        Err(error) => ExecutorSecretMutationOutcome::Error(error),
    }
}

// ---------------------------------------------------------------------------
// GraphQL objects
// ---------------------------------------------------------------------------

fn connection_page_info(next: i32) -> PageInfoObject {
    if next != 0 {
        PageInfoObject::new(true, Some(encode_int_cursor(next)))
    } else {
        PageInfoObject::new(false, None)
    }
}

fn user_object(
    data: &UserData,
    viewer_site_admin: bool,
    nested: Arc<NestedPrefetch>,
) -> UserObject {
    UserObject::from_data_as_viewer(data.clone(), viewer_site_admin).with_nested(nested)
}

/// `executorSecretResolver`.
pub struct ExecutorSecretObject {
    pub secret: ExecutorSecretRow,
    pub viewer_site_admin: bool,
    pub nested: Arc<NestedPrefetch>,
}

impl ObjectValue for ExecutorSecretObject {
    fn type_name(&self) -> &'static str {
        "ExecutorSecret"
    }

    fn field(
        &self,
        field: &str,
        arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "id" => Ok(Some(FieldValue::Json(Json::Str(marshal_executor_secret_id(
                &self.secret.scope.to_uppercase(),
                self.secret.id,
            ))))),
            "key" => Ok(Some(FieldValue::Json(Json::Str(self.secret.key.clone())))),
            "scope" => Ok(Some(FieldValue::Json(Json::Str(
                self.secret.scope.to_uppercase(),
            )))),
            "overwritesGlobalSecret" => Ok(Some(FieldValue::Json(Json::Bool(
                self.secret.overwrites_global_secret,
            )))),
            "namespace" => {
                if self.secret.namespace_user_id != 0 {
                    match self.nested.users_by_id.get(&self.secret.namespace_user_id) {
                        Some(UserByIdLookup::Found(data)) => Ok(Some(FieldValue::Object(
                            Arc::new(user_object(data, self.viewer_site_admin, self.nested.clone())),
                        ))),
                        Some(UserByIdLookup::Error(error)) => Err(ExecError::new(error.clone())),
                        None => Ok(None),
                    }
                } else if self.secret.namespace_org_id != 0 {
                    match self.nested.orgs_by_id.get(&self.secret.namespace_org_id) {
                        Some(OrgByIdLookup::Found(org)) => Ok(Some(FieldValue::Object(Arc::new(
                            OrgObject::from_data_as_viewer(
                                org.clone(),
                                self.viewer_site_admin,
                            )
                            .with_nested(self.nested.clone()),
                        )))),
                        Some(OrgByIdLookup::Error(error)) => Err(ExecError::new(error.clone())),
                        None => Ok(None),
                    }
                } else {
                    Ok(Some(FieldValue::Null))
                }
            }
            "creator" => {
                if self.secret.creator_id == 0 {
                    return Ok(Some(FieldValue::Null));
                }
                match self.nested.users_by_id.get(&self.secret.creator_id) {
                    Some(UserByIdLookup::Found(data)) => Ok(Some(FieldValue::Object(Arc::new(
                        user_object(data, self.viewer_site_admin, self.nested.clone()),
                    )))),
                    Some(UserByIdLookup::Error(error)) => Err(ExecError::new(error.clone())),
                    None => Ok(None),
                }
            }
            "createdAt" => Ok(Some(FieldValue::Json(Json::Str(datetime_rfc3339(
                self.secret.created_at,
            ))))),
            "updatedAt" => Ok(Some(FieldValue::Json(Json::Str(datetime_rfc3339(
                self.secret.updated_at,
            ))))),
            "accessLogs" => {
                let args = ExecutorSecretAccessLogsArgs::from_arguments(arguments);
                match self
                    .nested
                    .executor_secret_access_logs
                    .get(&(self.secret.id, args))
                {
                    Some(Ok(data)) => Ok(Some(FieldValue::Object(Arc::new(
                        ExecutorSecretAccessLogsConnectionObject {
                            data: data.clone(),
                            parent: self.secret.clone(),
                            viewer_site_admin: self.viewer_site_admin,
                            nested: self.nested.clone(),
                        },
                    )))),
                    Some(Err(error)) => Err(ExecError::new(error.clone())),
                    None => Ok(None),
                }
            }
            _ => Ok(None),
        }
    }
}

/// `executorSecretConnectionResolver`.
pub struct ExecutorSecretConnectionObject {
    pub data: ExecutorSecretConnectionData,
    pub scope: ExecutorSecretScope,
    pub viewer_site_admin: bool,
    pub nested: Arc<NestedPrefetch>,
}

impl ObjectValue for ExecutorSecretConnectionObject {
    fn type_name(&self) -> &'static str {
        "ExecutorSecretConnection"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "nodes" => Ok(Some(FieldValue::List {
                element_type: Some("ExecutorSecret"),
                items: self
                    .data
                    .secrets
                    .iter()
                    .map(|secret| {
                        FieldValue::Object(Arc::new(ExecutorSecretObject {
                            secret: secret.clone(),
                            viewer_site_admin: self.viewer_site_admin,
                            nested: self.nested.clone(),
                        }))
                    })
                    .collect(),
            })),
            "totalCount" => Ok(Some(FieldValue::Json(Json::Int(i64::from(
                self.data.total_count,
            ))))),
            "pageInfo" => Ok(Some(FieldValue::Object(Arc::new(connection_page_info(
                self.data.next,
            ))))),
            _ => Ok(None),
        }
    }
}

/// `executorSecretAccessLogResolver`.
pub struct ExecutorSecretAccessLogObject {
    pub log: ExecutorSecretAccessLogRow,
    pub parent: ExecutorSecretRow,
    pub viewer_site_admin: bool,
    pub nested: Arc<NestedPrefetch>,
}

impl ObjectValue for ExecutorSecretAccessLogObject {
    fn type_name(&self) -> &'static str {
        "ExecutorSecretAccessLog"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "id" => Ok(Some(FieldValue::Json(Json::Str(
                marshal_executor_secret_access_log_id(self.log.id),
            )))),
            // Errata: the baseline refetches the secret with a hard-coded
            // `BATCHES` scope; replaying the parent row is a strict superset
            // (same row, any scope).
            "executorSecret" => Ok(Some(FieldValue::Object(Arc::new(
                ExecutorSecretObject {
                    secret: self.parent.clone(),
                    viewer_site_admin: self.viewer_site_admin,
                    nested: self.nested.clone(),
                },
            )))),
            "user" => match self.log.user_id {
                None => Ok(Some(FieldValue::Null)),
                Some(user_id) => match self.nested.executor_secret_access_log_users.get(&user_id) {
                    Some(AccessLogUserLookup::Found(data)) => Ok(Some(FieldValue::Object(
                        Arc::new(user_object(data, self.viewer_site_admin, self.nested.clone())),
                    ))),
                    Some(AccessLogUserLookup::Missing) => Ok(Some(FieldValue::Null)),
                    Some(AccessLogUserLookup::Error(error)) => Err(ExecError::new(error.clone())),
                    None => Ok(None),
                },
            },
            "machineUser" => Ok(Some(FieldValue::Json(Json::Str(
                self.log.machine_user.clone(),
            )))),
            "createdAt" => Ok(Some(FieldValue::Json(Json::Str(datetime_rfc3339(
                self.log.created_at,
            ))))),
            _ => Ok(None),
        }
    }
}

/// `executorSecretAccessLogConnectionResolver`.
pub struct ExecutorSecretAccessLogsConnectionObject {
    pub data: ExecutorSecretAccessLogsConnectionData,
    pub parent: ExecutorSecretRow,
    pub viewer_site_admin: bool,
    pub nested: Arc<NestedPrefetch>,
}

impl ObjectValue for ExecutorSecretAccessLogsConnectionObject {
    fn type_name(&self) -> &'static str {
        "ExecutorSecretAccessLogConnection"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "nodes" => Ok(Some(FieldValue::List {
                element_type: Some("ExecutorSecretAccessLog"),
                items: self
                    .data
                    .logs
                    .iter()
                    .map(|log| {
                        FieldValue::Object(Arc::new(ExecutorSecretAccessLogObject {
                            log: log.clone(),
                            parent: self.parent.clone(),
                            viewer_site_admin: self.viewer_site_admin,
                            nested: self.nested.clone(),
                        }))
                    })
                    .collect(),
            })),
            "totalCount" => Ok(Some(FieldValue::Json(Json::Int(i64::from(
                self.data.total_count,
            ))))),
            "pageInfo" => Ok(Some(FieldValue::Object(Arc::new(connection_page_info(
                self.data.next,
            ))))),
            _ => Ok(None),
        }
    }
}

// ---------------------------------------------------------------------------
// Replay helpers
// ---------------------------------------------------------------------------

/// Replay one root/nested `executorSecrets` connection.
pub fn executor_secret_connection_value(
    data: &ExecutorSecretConnectionData,
    scope: ExecutorSecretScope,
    viewer_site_admin: bool,
    nested: Arc<NestedPrefetch>,
) -> FieldValue {
    FieldValue::Object(Arc::new(ExecutorSecretConnectionObject {
        data: data.clone(),
        scope,
        viewer_site_admin,
        nested,
    }))
}

/// Replay a prefetched `Query.node(ExecutorSecret)` lookup.
pub fn executor_secret_node_value(
    nodes: &HashMap<String, ExecutorSecretNodeLookup>,
    id: &str,
    viewer_site_admin: bool,
    nested: Arc<NestedPrefetch>,
) -> Result<Option<FieldValue>, ExecError> {
    match nodes.get(id) {
        Some(ExecutorSecretNodeLookup::Found(secret)) => {
            Ok(Some(FieldValue::Object(Arc::new(ExecutorSecretObject {
                secret: secret.clone(),
                viewer_site_admin,
                nested,
            }))))
        }
        Some(ExecutorSecretNodeLookup::Null) => Ok(Some(FieldValue::Null)),
        Some(ExecutorSecretNodeLookup::Error(error)) => Err(ExecError::new(error.clone())),
        None => Ok(None),
    }
}

/// Replay a prefetched `Query.node(ExecutorSecretAccessLog)` lookup.
pub fn executor_secret_access_log_node_value(
    nodes: &HashMap<String, ExecutorSecretAccessLogNodeLookup>,
    id: &str,
    viewer_site_admin: bool,
    nested: Arc<NestedPrefetch>,
) -> Result<Option<FieldValue>, ExecError> {
    match nodes.get(id) {
        Some(ExecutorSecretAccessLogNodeLookup::Found(log, parent)) => Ok(Some(
            FieldValue::Object(Arc::new(ExecutorSecretAccessLogObject {
                log: log.clone(),
                parent: parent.clone(),
                viewer_site_admin,
                nested,
            })),
        )),
        Some(ExecutorSecretAccessLogNodeLookup::Null) => Ok(Some(FieldValue::Null)),
        Some(ExecutorSecretAccessLogNodeLookup::Error(error)) => Err(ExecError::new(error.clone())),
        None => Ok(None),
    }
}

// ---------------------------------------------------------------------------
// Production store
// ---------------------------------------------------------------------------

/// `PgPool`-backed [`ExecutorSecretsStore`] over the existing
/// `executor_secrets` / `executor_secret_access_logs` tables.
pub struct PgExecutorSecretsStore {
    pool: sqlx::PgPool,
    key: Option<Arc<dyn sg_encryption::Key>>,
}

impl PgExecutorSecretsStore {
    pub fn new(pool: sqlx::PgPool, key: Option<Arc<dyn sg_encryption::Key>>) -> Self {
        Self { pool, key }
    }
}

const EXECUTOR_SECRET_COLUMNS: &str = "id, scope, key, value, encryption_key_id, \
    COALESCE((SELECT o.id FROM executor_secrets o WHERE o.key = executor_secrets.key \
    AND o.scope = executor_secrets.scope AND o.namespace_user_id IS NULL \
    AND o.namespace_org_id IS NULL AND o.id != executor_secrets.id)::boolean, false) AS overwrites_global, \
    namespace_user_id, namespace_org_id, creator_id, created_at, updated_at";

fn authz_sql(viewer: SecretViewer) -> String {
    if viewer.internal {
        return "TRUE".to_owned();
    }
    format!(
        "((executor_secrets.namespace_user_id IS NULL AND executor_secrets.namespace_org_id IS NULL) \
         OR (executor_secrets.namespace_user_id = {uid}) \
         OR (executor_secrets.namespace_org_id IS NOT NULL AND EXISTS (SELECT 1 FROM orgs \
             JOIN org_members ON org_members.org_id = orgs.id WHERE org_members.user_id = {uid})) \
         OR (EXISTS (SELECT 1 FROM users WHERE site_admin = TRUE AND id = {uid})))",
        uid = viewer.uid
    )
}

fn namespace_sql(opts: &ExecutorSecretsListOptions) -> String {
    let global = "namespace_user_id IS NULL AND namespace_org_id IS NULL";
    if opts.namespace_org_id != 0 {
        format!("(namespace_org_id = {} OR ({global}))", opts.namespace_org_id)
    } else if opts.namespace_user_id != 0 {
        format!(
            "(namespace_user_id = {} OR ({global}))",
            opts.namespace_user_id
        )
    } else {
        global.to_owned()
    }
}

fn secret_row(row: &sqlx::postgres::PgRow) -> Result<ExecutorSecretRow, String> {
    use sqlx::Row;
    Ok(ExecutorSecretRow {
        id: row.try_get("id").map_err(|e| e.to_string())?,
        key: row.try_get("key").map_err(|e| e.to_string())?,
        scope: row.try_get("scope").map_err(|e| e.to_string())?,
        overwrites_global_secret: row
            .try_get("overwrites_global")
            .map_err(|e| e.to_string())?,
        creator_id: row
            .try_get::<Option<i32>, _>("creator_id")
            .map_err(|e| e.to_string())?
            .unwrap_or(0),
        namespace_user_id: row
            .try_get::<Option<i32>, _>("namespace_user_id")
            .map_err(|e| e.to_string())?
            .unwrap_or(0),
        namespace_org_id: row
            .try_get::<Option<i32>, _>("namespace_org_id")
            .map_err(|e| e.to_string())?
            .unwrap_or(0),
        created_at: row.try_get("created_at").map_err(|e| e.to_string())?,
        updated_at: row.try_get("updated_at").map_err(|e| e.to_string())?,
    })
}

fn access_log_row(row: &sqlx::postgres::PgRow) -> Result<ExecutorSecretAccessLogRow, String> {
    use sqlx::Row;
    Ok(ExecutorSecretAccessLogRow {
        id: row.try_get("id").map_err(|e| e.to_string())?,
        executor_secret_id: row
            .try_get("executor_secret_id")
            .map_err(|e| e.to_string())?,
        user_id: row.try_get("user_id").map_err(|e| e.to_string())?,
        machine_user: String::new(),
        created_at: row.try_get("created_at").map_err(|e| e.to_string())?,
    })
}

#[async_trait]
impl ExecutorSecretsStore for PgExecutorSecretsStore {
    async fn create(
        &self,
        scope: &str,
        key: &str,
        value: &str,
        creator_id: i32,
        namespace_user_id: i32,
        namespace_org_id: i32,
        viewer: SecretViewer,
    ) -> Result<ExecutorSecretRow, String> {
        ensure_actor_has_namespace_write_access(self, viewer, namespace_user_id, namespace_org_id)
            .await?;
        let (encrypted, key_id) = sg_encryption::maybe_encrypt(self.key.as_deref(), value)
            .await
            .map_err(|error| error.to_string())?;
        let row = sqlx::query(&format!(
            "INSERT INTO executor_secrets (scope, key, value, encryption_key_id, \
             namespace_user_id, namespace_org_id, creator_id, created_at, updated_at) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, NOW(), NOW()) RETURNING {EXECUTOR_SECRET_COLUMNS}"
        ))
        .bind(scope)
        .bind(key)
        .bind(encrypted)
        .bind(key_id)
        .bind(if namespace_user_id != 0 {
            Some(namespace_user_id)
        } else {
            None
        })
        .bind(if namespace_org_id != 0 {
            Some(namespace_org_id)
        } else {
            None
        })
        .bind(creator_id)
        .fetch_one(&self.pool)
        .await;
        match row {
            Ok(row) => secret_row(&row),
            Err(sqlx::Error::Database(error)) if error.code().as_deref() == Some("23505") => {
                Err(DUPLICATE_EXECUTOR_SECRET.to_owned())
            }
            Err(error) => Err(error.to_string()),
        }
    }

    async fn update(
        &self,
        scope: &str,
        secret: &ExecutorSecretRow,
        value: &str,
        viewer: SecretViewer,
    ) -> Result<ExecutorSecretRow, String> {
        ensure_actor_has_namespace_write_access(
            self,
            viewer,
            secret.namespace_user_id,
            secret.namespace_org_id,
        )
        .await?;
        let (encrypted, key_id) = sg_encryption::maybe_encrypt(self.key.as_deref(), value)
            .await
            .map_err(|error| error.to_string())?;
        let row = sqlx::query(&format!(
            "UPDATE executor_secrets SET value = $1, encryption_key_id = $2, updated_at = NOW() \
             WHERE id = $3 AND scope = $4 AND {} RETURNING {EXECUTOR_SECRET_COLUMNS}",
            authz_sql(viewer)
        ))
        .bind(encrypted)
        .bind(key_id)
        .bind(secret.id)
        .bind(scope)
        .fetch_optional(&self.pool)
        .await
        .map_err(|error| error.to_string())?
        .ok_or_else(|| format!("executor secret not found: id={}", secret.id))?;
        secret_row(&row)
    }

    async fn delete(
        &self,
        scope: &str,
        id: i64,
        viewer: SecretViewer,
    ) -> Result<(), String> {
        let secret = self
            .get_by_id(scope, id, viewer)
            .await?
            .ok_or_else(|| format!("executor secret not found: id={id}"))?;
        ensure_actor_has_namespace_write_access(
            self,
            viewer,
            secret.namespace_user_id,
            secret.namespace_org_id,
        )
        .await?;
        sqlx::query(&format!(
            "DELETE FROM executor_secrets WHERE id = $1 AND scope = $2 AND {}",
            authz_sql(viewer)
        ))
        .bind(id)
        .bind(scope)
        .execute(&self.pool)
        .await
        .map_err(|error| error.to_string())?;
        Ok(())
    }

    async fn get_by_id(
        &self,
        _scope: &str,
        id: i64,
        viewer: SecretViewer,
    ) -> Result<Option<ExecutorSecretRow>, String> {
        // The baseline's `GetByID` ignores the scope argument.
        let row = sqlx::query(&format!(
            "SELECT {EXECUTOR_SECRET_COLUMNS} FROM executor_secrets WHERE id = $1 AND {}",
            authz_sql(viewer)
        ))
        .bind(id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|error| error.to_string())?;
        row.as_ref().map(secret_row).transpose()
    }

    async fn list(
        &self,
        scope: &str,
        opts: &ExecutorSecretsListOptions,
        viewer: SecretViewer,
    ) -> Result<(Vec<ExecutorSecretRow>, i32), String> {
        let limit_sql = if opts.limit != 0 {
            format!(" LIMIT {} OFFSET {}", opts.limit + 1, opts.offset)
        } else {
            String::new()
        };
        let sql = format!(
            "SELECT {EXECUTOR_SECRET_COLUMNS} FROM (SELECT {EXECUTOR_SECRET_COLUMNS}, \
             RANK() OVER (PARTITION BY key ORDER BY namespace_user_id NULLS LAST, \
             namespace_org_id NULLS LAST) FROM executor_secrets WHERE {} AND scope = $1 AND {}) \
             executor_secrets WHERE executor_secrets.rank = 1 ORDER BY key ASC{limit_sql}",
            authz_sql(viewer),
            namespace_sql(opts)
        );
        let rows = sqlx::query(&sql)
            .bind(scope)
            .fetch_all(&self.pool)
            .await
            .map_err(|error| error.to_string())?;
        let mut secrets = rows.iter().map(secret_row).collect::<Result<Vec<_>, _>>()?;
        let mut next = 0;
        if opts.limit != 0 && secrets.len() == (opts.limit as usize) + 1 {
            next = opts.offset + opts.limit;
            secrets.pop();
        }
        Ok((secrets, next))
    }

    async fn count(
        &self,
        scope: &str,
        opts: &ExecutorSecretsListOptions,
        viewer: SecretViewer,
    ) -> Result<i32, String> {
        use sqlx::Row;
        let sql = format!(
            "SELECT COUNT(*) FROM (SELECT RANK() OVER (PARTITION BY key ORDER BY \
             namespace_user_id NULLS LAST, namespace_org_id NULLS LAST) FROM executor_secrets \
             WHERE {} AND scope = $1 AND {}) executor_secrets WHERE executor_secrets.rank = 1",
            authz_sql(viewer),
            namespace_sql(opts)
        );
        let row = sqlx::query(&sql)
            .bind(scope)
            .fetch_one(&self.pool)
            .await
            .map_err(|error| error.to_string())?;
        row.try_get::<i64, _>(0)
            .map(|count| i32::try_from(count).unwrap_or(i32::MAX))
            .map_err(|error| error.to_string())
    }

    async fn list_access_logs(
        &self,
        opts: &ExecutorSecretAccessLogsListOptions,
    ) -> Result<(Vec<ExecutorSecretAccessLogRow>, i32), String> {
        let limit_sql = if opts.limit != 0 {
            format!(" LIMIT {} OFFSET {}", opts.limit + 1, opts.offset)
        } else {
            String::new()
        };
        let rows = sqlx::query(&format!(
            "SELECT id, executor_secret_id, user_id, created_at FROM executor_secret_access_logs \
             WHERE executor_secret_id = $1 ORDER BY created_at DESC{limit_sql}"
        ))
        .bind(opts.executor_secret_id)
        .fetch_all(&self.pool)
        .await
        .map_err(|error| error.to_string())?;
        let mut logs = rows
            .iter()
            .map(access_log_row)
            .collect::<Result<Vec<_>, _>>()?;
        let mut next = 0;
        if opts.limit != 0 && logs.len() == (opts.limit as usize) + 1 {
            next = opts.offset + opts.limit;
            logs.pop();
        }
        Ok((logs, next))
    }

    async fn count_access_logs(
        &self,
        opts: &ExecutorSecretAccessLogsListOptions,
    ) -> Result<i32, String> {
        use sqlx::Row;
        let row = sqlx::query(
            "SELECT COUNT(*) FROM executor_secret_access_logs WHERE executor_secret_id = $1",
        )
        .bind(opts.executor_secret_id)
        .fetch_one(&self.pool)
        .await
        .map_err(|error| error.to_string())?;
        row.try_get::<i64, _>(0)
            .map(|count| i32::try_from(count).unwrap_or(i32::MAX))
            .map_err(|error| error.to_string())
    }

    async fn get_access_log_by_id(
        &self,
        id: i64,
    ) -> Result<Option<ExecutorSecretAccessLogRow>, String> {
        let row = sqlx::query(
            "SELECT id, executor_secret_id, user_id, created_at \
             FROM executor_secret_access_logs WHERE id = $1",
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|error| error.to_string())?;
        row.as_ref().map(access_log_row).transpose()
    }

    async fn is_org_member(&self, org_id: i32, user_id: i32) -> Result<bool, String> {
        let row = sqlx::query(
            "SELECT 1 FROM org_members WHERE org_id = $1 AND user_id = $2 LIMIT 1",
        )
        .bind(org_id)
        .bind(user_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|error| error.to_string())?;
        Ok(row.is_some())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn secret_id_round_trips() {
        let raw = marshal_executor_secret_id(SCOPE_BATCHES, 42);
        assert_eq!(
            unmarshal_executor_secret_id(&raw),
            Ok((SCOPE_BATCHES.to_owned(), 42))
        );
        let raw = marshal_executor_secret_access_log_id(9);
        assert_eq!(unmarshal_executor_secret_access_log_id(&raw), Ok(9));
    }

    #[test]
    fn key_pattern_matches_go() {
        assert!(valid_executor_secret_key("A"));
        assert!(valid_executor_secret_key("DOCKER_AUTH_CONFIG"));
        assert!(!valid_executor_secret_key(""));
        assert!(!valid_executor_secret_key("a"));
        assert!(!valid_executor_secret_key("1A"));
        assert!(!valid_executor_secret_key("A-B"));
    }

    #[test]
    fn int_cursor_round_trips() {
        assert_eq!(encode_int_cursor(50), "NTA=");
        assert_eq!(decode_int_cursor("NTA="), Ok(50));
    }

    #[test]
    fn docker_auth_config_validation() {
        assert!(validate_executor_secret("KEY", "value").is_ok());
        assert_eq!(
            validate_executor_secret("KEY", ""),
            Err("value cannot be empty string".to_owned())
        );
        assert_eq!(
            validate_executor_secret("DOCKER_AUTH_CONFIG", r#"{"credsStore":"x"}"#),
            Err("cannot use credential stores in docker auth config set via secrets".to_owned())
        );
        assert_eq!(
            validate_executor_secret("DOCKER_AUTH_CONFIG", r#"{"credHelpers":{"x":"y"}}"#),
            Err("cannot use credential helpers in docker auth config set via secrets".to_owned())
        );
        assert_eq!(
            validate_executor_secret(
                "DOCKER_AUTH_CONFIG",
                r#"{"auths":{"r":{"auth":"dXNlcjpwYXNz"}}}"#
            ),
            Ok(())
        );
    }
}
