#![forbid(unsafe_code)]

//! `Query.repositories`: the `NewRepositoryConnection` connection
//! (`cmd/frontend/graphqlbackend/repositories.go:105-280`) on top of
//! `gqlutil.ConnectionResolver` (`internal/gqlutil/connection_resolver.go`).
//!
//! The argument conversion keeps the baseline's check order and error text,
//! the cursor codec reproduces `MarshalRepositoryCursor`'s
//! `base64("RepositoryCursor:" + {"c","v","d"})` encoding, and pagination
//! follows `Limit`/`ApplyMaxPageSize`/`paginationArgs`/`Nodes`/`PageInfo`
//! exactly (including the `pageSize + 1` over-fetch and the `last` reversal).
//! Only site admins get a non-zero `totalCount`.

use std::sync::Arc;

use async_trait::async_trait;
use base64::Engine as _;
use serde::{Deserialize, Serialize};

use sg_db::repositories_connection::{
    RepoCursor, RepoCursorValue, RepoListOptions, RepoListOrder, RepoListRecord, RepoOrderColumn,
    RepoPagination,
};
use sg_graphql::exec::{
    Actor, Arguments, ExecError, FieldValue, InputValue, ObjectValue, RootField,
};
use sg_graphql::json::Json;

use crate::graphql_resolvers::{
    datetime_rfc3339, ConnectionPageInfoObject, RepositoryData, RepositoryObject,
};

/// `repositoryArgs` after graph-gophers applied the schema defaults
/// (`repositories.go:28-140`).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct RepositoriesArgs {
    pub query: Option<String>,
    pub names: Option<Vec<String>>,
    pub cloned: bool,
    pub not_cloned: bool,
    pub indexed: bool,
    pub not_indexed: bool,
    pub embedded: bool,
    pub not_embedded: bool,
    pub clone_status: Option<String>,
    pub failed_fetch: bool,
    pub corrupted: bool,
    pub external_service: Option<String>,
    pub order_by: Option<String>,
    pub descending: bool,
    pub first: Option<i64>,
    pub last: Option<i64>,
    pub after: Option<String>,
    pub before: Option<String>,
}

/// `repositoryArgs` from the executor's resolved arguments, applying the
/// schema defaults for the booleans and `orderBy` (schema.graphql:1510-1585).
pub fn repositories_args(arguments: &Arguments) -> RepositoriesArgs {
    let names = match arguments.get("names") {
        Some(InputValue::List(items)) => Some(
            items
                .iter()
                .filter_map(|item| item.as_str().map(str::to_owned))
                .collect(),
        ),
        Some(InputValue::Null) | None => None,
        Some(_) => None,
    };
    RepositoriesArgs {
        query: arguments.string("query").map(str::to_owned),
        names,
        cloned: arguments.bool("cloned").unwrap_or(true),
        not_cloned: arguments.bool("notCloned").unwrap_or(true),
        indexed: arguments.bool("indexed").unwrap_or(true),
        not_indexed: arguments.bool("notIndexed").unwrap_or(true),
        embedded: arguments.bool("embedded").unwrap_or(true),
        not_embedded: arguments.bool("notEmbedded").unwrap_or(true),
        clone_status: arguments.string("cloneStatus").map(str::to_owned),
        failed_fetch: arguments.bool("failedFetch").unwrap_or(false),
        corrupted: arguments.bool("corrupted").unwrap_or(false),
        external_service: arguments.string("externalService").map(str::to_owned),
        order_by: arguments.string("orderBy").map(str::to_owned),
        descending: arguments.bool("descending").unwrap_or(false),
        first: arguments.i64("first"),
        last: arguments.i64("last"),
        after: arguments.string("after").map(str::to_owned),
        before: arguments.string("before").map(str::to_owned),
    }
}

/// `repositoryArgs.toReposListOptions` (`repositories.go:45-115`), with the
/// same predicate order and error strings.
pub fn to_list_options(args: &RepositoriesArgs) -> Result<RepoListOptions, String> {
    let mut options = RepoListOptions::default();

    if let Some(query) = &args.query {
        if !query.is_empty() {
            options.query = Some(query.clone());
            options.query_id = maybe_query_is_id(query);
        }
    }

    if let Some(names) = &args.names {
        options.names = Some(names.clone());
    }

    if let Some(raw) = &args.clone_status {
        if let Some(status) = parse_clone_status(raw) {
            options.clone_status = Some(status.to_owned());
        }
    }

    options.failed_fetch = args.failed_fetch;
    options.only_corrupted = args.corrupted;

    if !args.cloned && !args.not_cloned {
        return Err("excluding cloned and not cloned repos leaves an empty set".to_owned());
    }
    if !args.cloned {
        options.no_cloned = true;
    }
    if !args.not_cloned {
        // notCloned is true by default; this only triggers when explicitly set.
        options.only_cloned = true;
    }

    if !args.indexed && !args.not_indexed {
        return Err("excluding indexed and not indexed repos leaves an empty set".to_owned());
    }
    if !args.indexed {
        options.no_indexed = true;
    }
    if !args.not_indexed {
        options.only_indexed = true;
    }

    if !args.embedded && !args.not_embedded {
        return Err("excluding embedded and not embedded repos leaves an empty set".to_owned());
    }
    if !args.embedded {
        options.no_embedded = true;
    }
    if !args.not_embedded {
        options.only_embedded = true;
    }

    if let Some(raw) = &args.external_service {
        let id = crate::external_services::unmarshal_external_service_id(raw)?;
        options.external_service_ids.push(id);
    }

    Ok(options)
}

/// `types.ParseCloneStatusFromGraphQL`: lowercase, then the three known
/// values; anything else (including the schema's absent value) is unknown.
fn parse_clone_status(raw: &str) -> Option<&'static str> {
    match raw.to_lowercase().as_str() {
        "not_cloned" => Some("not_cloned"),
        "cloning" => Some("cloning"),
        "cloned" => Some("cloned"),
        _ => None,
    }
}

/// `maybeQueryIsID` (`internal/database/helpers.go:34-44`): a decimal int32,
/// then a Relay ID whose (unvalidated) spec is an int32.
fn maybe_query_is_id(query: &str) -> Option<i32> {
    if let Ok(id) = query.parse::<i32>() {
        return Some(id);
    }
    crate::graphql_resolvers::unmarshal_relay_id(query).ok()
}

/// `toDBRepoListColumn` (`repositories.go:283-292`).
fn to_db_repo_list_column(order_by: &str) -> RepoOrderColumn {
    match order_by {
        "REPO_URI" | "REPOSITORY_NAME" => RepoOrderColumn::Name,
        "REPO_CREATED_AT" | "REPOSITORY_CREATED_AT" => RepoOrderColumn::CreatedAt,
        "SIZE" => RepoOrderColumn::Size,
        // Unreachable through the schema enum; the baseline's `""` column
        // would fail on the first cursor (see overview §3.1).
        _ => RepoOrderColumn::Name,
    }
}

/// `types.Cursor` (`internal/types/cursor.go`) with the same JSON field
/// order (`c`, `v`, `d`) and no `omitempty`.
#[derive(Serialize, Deserialize)]
struct CursorSpec {
    #[serde(default)]
    c: String,
    #[serde(default)]
    v: String,
    #[serde(default)]
    d: String,
}

/// `MarshalRepositoryCursor` (`repository_cursor.go:18-20`):
/// `base64.URLEncoding("RepositoryCursor:" + json)`.
pub fn marshal_repository_cursor(column: &str, value: &str, repo_id: i32) -> String {
    let spec = CursorSpec {
        c: column.to_owned(),
        v: format!("{value}@{repo_id}"),
        d: String::new(),
    };
    let json = serde_json::to_string(&spec).expect("cursor spec serializes");
    base64::engine::general_purpose::URL_SAFE.encode(format!("RepositoryCursor:{json}"))
}

/// `repositoriesConnectionStore.UnmarshalCursor` (`repositories.go:164-220`).
pub fn unmarshal_repository_cursor(
    raw: &str,
    order: RepoOrderColumn,
) -> Result<RepoCursor, String> {
    let decoded = match base64::engine::general_purpose::URL_SAFE.decode(raw) {
        Ok(decoded) => decoded,
        // `relay.UnmarshalKind` yields `""` when decoding fails.
        Err(_) => {
            return Err("cannot unmarshal repository cursor type: \"\"".to_owned());
        }
    };
    let text = String::from_utf8_lossy(&decoded);
    let Some((kind, spec)) = text.split_once(':') else {
        return Err("cannot unmarshal repository cursor type: \"\"".to_owned());
    };
    if kind != "RepositoryCursor" {
        return Err(format!("cannot unmarshal repository cursor type: {kind:?}"));
    }
    let cursor: CursorSpec = serde_json::from_str(spec).map_err(|error| error.to_string())?;

    let column = order.cursor_column();
    if cursor.c != column {
        return Err(format!(
            "Invalid cursor. Expected: {column} Actual: {}",
            cursor.c
        ));
    }

    let values: Vec<&str> = cursor.v.split('@').collect();
    if values.len() != 2 {
        return Err(format!(
            "Invalid cursor. Expected Value: <{column}>@<id> Actual Value: {}",
            cursor.v
        ));
    }

    let repo_id: i32 = values[1]
        .parse()
        .map_err(|error: std::num::ParseIntError| error.to_string())?;
    let value = match order {
        RepoOrderColumn::Name | RepoOrderColumn::CreatedAt => {
            RepoCursorValue::Text(values[0].to_owned())
        }
        RepoOrderColumn::Size => RepoCursorValue::Size(
            values[0]
                .parse()
                .map_err(|error: std::num::ParseIntError| error.to_string())?,
        ),
    };
    Ok(RepoCursor { value, repo_id })
}

/// `gqlutil.ConnectionResolverOptions.ApplyMaxPageSize` with the connection's
/// `MaxPageSize = 1000`.
fn apply_max_page_size(limit: i64) -> i64 {
    const MAX_PAGE_SIZE: i64 = 1000;
    if limit == 0 {
        return MAX_PAGE_SIZE;
    }
    if limit < MAX_PAGE_SIZE {
        return limit;
    }
    MAX_PAGE_SIZE
}

/// One resolved `Query.repositories` page: the trimmed nodes plus the
/// `ConnectionPageInfo` values (`gqlutil.ConnectionPageInfo`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RepositoriesPage {
    pub nodes: Vec<RepositoryData>,
    pub has_next_page: bool,
    pub has_previous_page: bool,
    pub start_cursor: Option<String>,
    pub end_cursor: Option<String>,
}

/// The two lazily-independent halves of `NewRepositoryConnection`:
/// `Nodes`/`pageInfo` and `totalCount` can fail separately.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RepositoriesConnectionData {
    pub page: Result<RepositoriesPage, String>,
    pub total_count: Result<i64, String>,
}

/// The result of resolving one `Query.repositories` field.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RepositoriesConnectionLookup {
    Found(RepositoriesConnectionData),
    /// An argument error from `toReposListOptions`.
    Error(String),
}

/// The database seam for `Query.repositories` (`db.Repos().List`/`Count`).
#[async_trait]
pub trait RepositoriesConnectionStore: Send + Sync {
    async fn list(
        &self,
        authenticated_user_id: i32,
        bypass_authz: bool,
        options: &RepoListOptions,
        order: RepoListOrder,
        pagination: &RepoPagination,
    ) -> Result<Vec<RepoListRecord>, String>;

    async fn count(
        &self,
        authenticated_user_id: i32,
        bypass_authz: bool,
        options: &RepoListOptions,
    ) -> Result<i64, String>;
}

/// Production adapter over [`sg_db::repositories_connection`].
pub struct PgRepositoriesConnectionStore {
    pool: sqlx::PgPool,
}

impl PgRepositoriesConnectionStore {
    pub fn new(pool: sqlx::PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl RepositoriesConnectionStore for PgRepositoriesConnectionStore {
    async fn list(
        &self,
        authenticated_user_id: i32,
        bypass_authz: bool,
        options: &RepoListOptions,
        order: RepoListOrder,
        pagination: &RepoPagination,
    ) -> Result<Vec<RepoListRecord>, String> {
        sg_db::repositories_connection::list(
            &self.pool,
            authenticated_user_id,
            bypass_authz,
            options,
            order,
            pagination,
        )
        .await
        .map_err(|error| error.to_string())
    }

    async fn count(
        &self,
        authenticated_user_id: i32,
        bypass_authz: bool,
        options: &RepoListOptions,
    ) -> Result<i64, String> {
        sg_db::repositories_connection::count(
            &self.pool,
            authenticated_user_id,
            bypass_authz,
            options,
        )
        .await
        .map_err(|error| error.to_string())
    }
}

/// `schemaResolver.Repositories` (`repositories.go:105-140`) plus the
/// connection store's `ComputeTotal`/`ComputeNodes`/`PageInfo`
/// (`repositories.go:222-280`).
pub async fn repositories(
    args: &RepositoriesArgs,
    store: &dyn RepositoriesConnectionStore,
    actor: Actor,
    viewer_site_admin: bool,
) -> RepositoriesConnectionLookup {
    let options = match to_list_options(args) {
        Ok(options) => options,
        Err(message) => return RepositoriesConnectionLookup::Error(message),
    };

    let column = to_db_repo_list_column(args.order_by.as_deref().unwrap_or("REPOSITORY_NAME"));
    let order = RepoListOrder {
        column,
        ascending: !args.descending,
    };
    let authenticated_user_id = actor.uid().unwrap_or(0);
    // `AuthzQueryConds` bypasses for site admins (authz.enforceForSiteAdmins
    // defaults to false); anonymous actors never bypass.
    let is_site_admin = actor.is_authenticated() && viewer_site_admin;

    // `ComputeTotal`: only site admins see the unrestricted count; everyone
    // else gets 0 (even when the row filters would match).
    let total_count = if is_site_admin {
        store
            .count(authenticated_user_id, true, &options)
            .await
            .map_err(|error| error.to_string())
    } else {
        Ok(0)
    };

    let page = match compute_page(
        args,
        store,
        authenticated_user_id,
        is_site_admin,
        &options,
        order,
    )
    .await
    {
        Ok(page) => page,
        Err(message) => {
            return RepositoriesConnectionLookup::Found(RepositoriesConnectionData {
                page: Err(message),
                total_count,
            })
        }
    };

    RepositoriesConnectionLookup::Found(RepositoriesConnectionData {
        page: Ok(page),
        total_count,
    })
}

/// `ConnectionResolver.Nodes` + `PageInfo`
/// (`connection_resolver.go:102-230`): `pageSize + 1` over-fetch, `last`
/// reversal/trim, the `first`/`last` cursor comparison directions and the
/// `hasNextPage`/`hasPreviousPage` formulas.
async fn compute_page(
    args: &RepositoriesArgs,
    store: &dyn RepositoriesConnectionStore,
    authenticated_user_id: i32,
    bypass_authz: bool,
    options: &RepoListOptions,
    order: RepoListOrder,
) -> Result<RepositoriesPage, String> {
    if args.first.is_none() && args.last.is_none() {
        return Err("you must provide a `first` or `last` value to properly paginate".to_owned());
    }
    let page_size = match (args.first, args.last) {
        (Some(first), _) => apply_max_page_size(first),
        (None, Some(last)) => apply_max_page_size(last),
        (None, None) => unreachable!("checked above"),
    };
    let limit = page_size + 1;

    let after = match &args.after {
        Some(raw) => Some(unmarshal_repository_cursor(raw, order.column)?),
        None => None,
    };
    let before = match &args.before {
        Some(raw) => Some(unmarshal_repository_cursor(raw, order.column)?),
        None => None,
    };

    // `paginationArgs`: `first` wins; `last` only sets `Last` when `first` is
    // absent. Both directions use the same over-fetch limit.
    let (first, last) = if args.first.is_some() {
        (Some(limit), None)
    } else {
        (None, Some(limit))
    };
    let pagination = RepoPagination {
        first,
        last,
        after,
        before,
    };

    let mut nodes = store
        .list(
            authenticated_user_id,
            bypass_authz,
            options,
            order,
            &pagination,
        )
        .await?;

    // `fetchedNodesCount` is `len(r.data.nodes)`, before the trim.
    let fetched_count = nodes.len() as i64;

    // `Nodes`: a `last` request fetched in the opposite order; reverse, then
    // drop the over-fetched element (first for `last`, last for `first`).
    if args.last.is_some() {
        nodes.reverse();
    }
    if nodes.len() as i64 > page_size {
        if args.last.is_some() {
            nodes.remove(0);
        } else {
            nodes.pop();
        }
    }

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

    // `ConnectionPageInfo.EndCursor`/`StartCursor` marshal the trimmed page.
    let start_cursor = nodes
        .first()
        .map(|node| marshal_node_cursor(order.column, node));
    let end_cursor = nodes
        .last()
        .map(|node| marshal_node_cursor(order.column, node));

    Ok(RepositoriesPage {
        nodes: nodes.iter().map(repository_data).collect(),
        has_next_page,
        has_previous_page,
        start_cursor,
        end_cursor,
    })
}

/// `repositoriesConnectionStore.MarshalCursor` (`repositories.go:143-164`).
fn marshal_node_cursor(column: RepoOrderColumn, node: &RepoListRecord) -> String {
    let value = match column {
        RepoOrderColumn::Name => node.name.clone(),
        RepoOrderColumn::CreatedAt => datetime_rfc3339(node.created_at),
        RepoOrderColumn::Size => node.repo_size_bytes.to_string(),
    };
    marshal_repository_cursor(column.cursor_column(), &value, node.id)
}

fn repository_data(record: &RepoListRecord) -> RepositoryData {
    RepositoryData {
        id: record.id,
        name: record.name.clone(),
        uri: record.uri.clone(),
        description: record.description.clone(),
        fork: record.fork,
        archived: record.archived,
        private: record.private,
        stars: record.stars,
        external_id: record.external_id.clone(),
        external_service_type: record.external_service_type.clone(),
        external_service_id: record.external_service_id.clone(),
    }
}

/// Prefetch every selected `Query.repositories` field, deduplicated by
/// argument set.
pub async fn prefetch_repositories_connections(
    store: &dyn RepositoriesConnectionStore,
    fields: &[RootField],
    actor: Actor,
    viewer_site_admin: bool,
) -> std::collections::HashMap<RepositoriesArgs, RepositoriesConnectionLookup> {
    let mut connections = std::collections::HashMap::new();
    for field in fields {
        if field.name != "repositories" {
            continue;
        }
        let args = repositories_args(&field.arguments);
        if connections.contains_key(&args) {
            continue;
        }
        let lookup = repositories(&args, store, actor, viewer_site_admin).await;
        connections.insert(args, lookup);
    }
    connections
}

/// `NewRepositoryConnection` (`schema.graphql:3513-3530`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RepositoriesConnectionObject {
    data: RepositoriesConnectionData,
}

impl RepositoriesConnectionObject {
    pub fn from_data(data: RepositoriesConnectionData) -> Self {
        Self { data }
    }
}

impl ObjectValue for RepositoriesConnectionObject {
    fn type_name(&self) -> &'static str {
        "NewRepositoryConnection"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "nodes" => match &self.data.page {
                Ok(page) => Ok(Some(FieldValue::List {
                    element_type: Some("Repository"),
                    items: page
                        .nodes
                        .iter()
                        .cloned()
                        .map(|data| FieldValue::Object(Arc::new(RepositoryObject::from_data(data))))
                        .collect(),
                })),
                Err(message) => Err(ExecError::new(message.clone())),
            },
            "totalCount" => match &self.data.total_count {
                Ok(count) => Ok(Some(FieldValue::Json(Json::Int(*count)))),
                Err(message) => Err(ExecError::new(message.clone())),
            },
            "pageInfo" => match &self.data.page {
                Ok(page) => Ok(Some(FieldValue::Object(Arc::new(
                    ConnectionPageInfoObject::new(
                        page.start_cursor.clone(),
                        page.end_cursor.clone(),
                        page.has_next_page,
                        page.has_previous_page,
                    ),
                )))),
                Err(message) => Err(ExecError::new(message.clone())),
            },
            _ => Ok(None),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn decode_cursor(cursor: &str) -> String {
        let bytes = base64::engine::general_purpose::URL_SAFE
            .decode(cursor)
            .expect("base64");
        String::from_utf8(bytes).expect("utf8")
    }

    #[test]
    fn cursor_matches_go_encoding() {
        // `relay.MarshalID("RepositoryCursor", &types.Cursor{...})`.
        let cursor = marshal_repository_cursor("name", "github.com/a/b", 7);
        assert_eq!(
            decode_cursor(&cursor),
            "RepositoryCursor:{\"c\":\"name\",\"v\":\"github.com/a/b@7\",\"d\":\"\"}"
        );
    }

    #[test]
    fn cursor_round_trips() {
        let cursor = marshal_repository_cursor("created_at", "2024-01-02T03:04:05Z", 9);
        let decoded = unmarshal_repository_cursor(&cursor, RepoOrderColumn::CreatedAt).unwrap();
        assert_eq!(
            decoded,
            RepoCursor {
                value: RepoCursorValue::Text("2024-01-02T03:04:05Z".to_owned()),
                repo_id: 9
            }
        );
    }

    #[test]
    fn cursor_column_mismatch_is_an_error() {
        let cursor = marshal_repository_cursor("name", "a/b", 1);
        let error = unmarshal_repository_cursor(&cursor, RepoOrderColumn::CreatedAt).unwrap_err();
        assert_eq!(error, "Invalid cursor. Expected: created_at Actual: name");
    }
}
