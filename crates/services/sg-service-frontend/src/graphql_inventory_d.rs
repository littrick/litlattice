//! M9.19a: the remaining object types reachable from the main SDL.
//!
//! Go baseline: `cmd/frontend/graphqlbackend/{repository_metadata.go,
//! repository_git_refs.go, repository_contributors.go,
//! repository_contributor.go, repository_text_search_index.go, externallink,
//! hunk.go, git_blob.go}` and
//! `cmd/frontend/internal/repos/webhooks/resolvers/resolver.go`.
//!
//! `Diff`, `RepositoryConnection` and `LocationConnection` have no field
//! references in the main SDL, so they are unreachable and deliberately not
//! implemented (see the slice overview).

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use sg_graphql::exec::{Actor, Arguments, ExecError, FieldValue, ObjectValue, RootField};
use sg_graphql::json::Json;
use sqlx::{PgPool, Row};
use time::OffsetDateTime;

use crate::graphql_resolvers::{
    datetime_rfc3339, NodeRoots, PageInfoObject, RepoKey, RepoLookup, RepositoryRedirectKey,
};
use crate::search_stream::convert::MinimalRepo;

// ---------------------------------------------------------------------------
// Relay helpers
// ---------------------------------------------------------------------------

/// `relay.MarshalID(kind, string)`.
pub fn marshal_relay_id_str(kind: &str, spec: &str) -> String {
    use base64::Engine as _;
    base64::engine::general_purpose::URL_SAFE.encode(format!("{kind}:{spec}"))
}

/// `relay.UnmarshalSpec(id, &string)` (`internal/graphqlbackend/relay.go` in
/// graph-gophers). `null` leaves the zero value (`""`), matching
/// `json.Unmarshal`.
pub fn unmarshal_relay_str(raw: &str) -> Result<String, String> {
    use base64::Engine as _;
    let decoded = base64::engine::general_purpose::URL_SAFE
        .decode(raw)
        .map_err(|error| error.to_string())?;
    let Ok(text) = String::from_utf8(decoded) else {
        return Err("invalid graphql.ID".to_owned());
    };
    let Some((_, spec)) = text.split_once(':') else {
        return Err("invalid graphql.ID".to_owned());
    };
    let value: serde_json::Value =
        serde_json::from_str(spec).map_err(|error| error.to_string())?;
    match value {
        serde_json::Value::String(value) => Ok(value),
        serde_json::Value::Null => Ok(String::new()),
        other => Err(format!(
            "json: cannot unmarshal {} into Go value of type string",
            go_json_kind(&other)
        )),
    }
}

fn go_json_kind(value: &serde_json::Value) -> &'static str {
    match value {
        serde_json::Value::Null => "null",
        serde_json::Value::Bool(_) => "boolean",
        serde_json::Value::Number(_) => "number",
        serde_json::Value::String(_) => "string",
        serde_json::Value::Array(_) => "array",
        serde_json::Value::Object(_) => "object",
    }
}

// ---------------------------------------------------------------------------
// KeyValuePair / ExternalLink
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyValuePairData {
    pub key: String,
    pub value: Option<String>,
}

/// `keyValuePairResolver` (`repository_metadata.go:16-27`).
pub struct KeyValuePairObject {
    pub data: KeyValuePairData,
}

impl ObjectValue for KeyValuePairObject {
    fn type_name(&self) -> &'static str {
        "KeyValuePair"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "key" => Ok(Some(FieldValue::Json(Json::Str(self.data.key.clone())))),
            "value" => Ok(Some(match &self.data.value {
                Some(value) => FieldValue::Json(Json::Str(value.clone())),
                None => FieldValue::Null,
            })),
            _ => Ok(None),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExternalLinkData {
    pub url: String,
    /// `extsvc.TypeToKind(serviceType)`; the empty string maps to `null`.
    pub service_kind: String,
    pub service_type: String,
}

/// `externallink.Resolver`.
pub struct ExternalLinkObject {
    pub data: ExternalLinkData,
}

impl ObjectValue for ExternalLinkObject {
    fn type_name(&self) -> &'static str {
        "ExternalLink"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "url" => Ok(Some(FieldValue::Json(Json::Str(self.data.url.clone())))),
            "serviceKind" => Ok(Some(if self.data.service_kind.is_empty() {
                FieldValue::Null
            } else {
                FieldValue::Json(Json::Str(self.data.service_kind.clone()))
            })),
            "serviceType" => Ok(Some(if self.data.service_type.is_empty() {
                FieldValue::Null
            } else {
                FieldValue::Json(Json::Str(self.data.service_type.clone()))
            })),
            _ => Ok(None),
        }
    }
}

// ---------------------------------------------------------------------------
// Repository metadata / repo_kvps
// ---------------------------------------------------------------------------

/// The repository's `keyValuePairs`/`metadata` rows.
pub type RepoKvpsLookup = Result<Vec<KeyValuePairData>, String>;

#[derive(Debug, Clone, Default, PartialEq, Eq, Hash)]
pub struct RepoMetadataArgs {
    pub query: Option<String>,
    pub first: Option<i64>,
    pub last: Option<i64>,
    pub after: Option<String>,
    pub before: Option<String>,
}

impl RepoMetadataArgs {
    pub fn from_arguments(arguments: &Arguments) -> Self {
        Self {
            query: arguments.string("query").map(str::to_owned),
            first: arguments.i64("first"),
            last: arguments.i64("last"),
            after: arguments.string("after").map(str::to_owned),
            before: arguments.string("before").map(str::to_owned),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BidirectionalPageInfoData {
    pub has_next_page: bool,
    pub has_previous_page: bool,
    pub start_cursor: Option<String>,
    pub end_cursor: Option<String>,
}

impl BidirectionalPageInfoData {
    fn empty() -> Self {
        Self {
            has_next_page: false,
            has_previous_page: false,
            start_cursor: None,
            end_cursor: None,
        }
    }
}

impl ObjectValue for BidirectionalPageInfoData {
    fn type_name(&self) -> &'static str {
        "BidirectionalPageInfo"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        let optional = |value: &Option<String>| match value {
            Some(value) => FieldValue::Json(Json::Str(value.clone())),
            None => FieldValue::Null,
        };
        match field {
            "hasNextPage" => Ok(Some(FieldValue::Json(Json::Bool(self.has_next_page)))),
            "hasPreviousPage" => Ok(Some(FieldValue::Json(Json::Bool(
                self.has_previous_page,
            )))),
            "startCursor" => Ok(Some(optional(&self.start_cursor))),
            "endCursor" => Ok(Some(optional(&self.end_cursor))),
            _ => Ok(None),
        }
    }
}

/// One prefetched `RepoMetadata.keys` / `values` result.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RepoMetadataConnectionLookup {
    Found(RepoMetadataConnectionData),
    Error(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RepoMetadataConnectionData {
    /// `ComputeTotal`; its error is reported by `totalCount`.
    pub total_count: Result<i64, String>,
    /// `ComputeNodes`/`PageInfo`; a pagination-argument or database error is
    /// reported by `nodes` and `pageInfo`.
    pub page: Result<(Vec<String>, BidirectionalPageInfoData), String>,
}

/// `type RepoMetadataKeyOrValueConnection`.
pub struct RepoMetadataConnectionObject {
    pub data: RepoMetadataConnectionData,
}

impl ObjectValue for RepoMetadataConnectionObject {
    fn type_name(&self) -> &'static str {
        "RepoMetadataKeyOrValueConnection"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "totalCount" => match &self.data.total_count {
                Ok(total) => Ok(Some(FieldValue::Json(Json::Int(*total)))),
                Err(message) => Err(ExecError::new(message.clone())),
            },
            "nodes" => match &self.data.page {
                Ok((nodes, _)) => Ok(Some(FieldValue::List {
                    items: nodes
                        .iter()
                        .map(|node| FieldValue::Json(Json::Str(node.clone())))
                        .collect(),
                    element_type: Some("String"),
                })),
                Err(message) => Err(ExecError::new(message.clone())),
            },
            "pageInfo" => match &self.data.page {
                Ok((_, page_info)) => Ok(Some(FieldValue::Object(Arc::new(
                    page_info.clone(),
                )))),
                Err(message) => Err(ExecError::new(message.clone())),
            },
            _ => Ok(None),
        }
    }
}

pub struct RepoMetadataKeyResultObject {
    pub values: HashMap<RepoMetadataArgs, RepoMetadataConnectionLookup>,
}

impl ObjectValue for RepoMetadataKeyResultObject {
    fn type_name(&self) -> &'static str {
        "RepoMetadataKeyResult"
    }

    fn field(
        &self,
        field: &str,
        arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "values" => {
                let args = RepoMetadataArgs::from_arguments(arguments);
                match self.values.get(&args) {
                    Some(RepoMetadataConnectionLookup::Found(data)) => Ok(Some(
                        FieldValue::Object(Arc::new(RepoMetadataConnectionObject {
                            data: data.clone(),
                        })),
                    )),
                    Some(RepoMetadataConnectionLookup::Error(message)) => {
                        Err(ExecError::new(message.clone()))
                    }
                    None => Ok(None),
                }
            }
            _ => Ok(None),
        }
    }
}

pub struct RepoMetadataObject {
    pub keys: HashMap<RepoMetadataArgs, RepoMetadataConnectionLookup>,
    pub values: HashMap<String, HashMap<RepoMetadataArgs, RepoMetadataConnectionLookup>>,
}

impl ObjectValue for RepoMetadataObject {
    fn type_name(&self) -> &'static str {
        "RepoMetadata"
    }

    fn field(
        &self,
        field: &str,
        arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "keys" => {
                let args = RepoMetadataArgs::from_arguments(arguments);
                match self.keys.get(&args) {
                    Some(RepoMetadataConnectionLookup::Found(data)) => Ok(Some(
                        FieldValue::Object(Arc::new(RepoMetadataConnectionObject {
                            data: data.clone(),
                        })),
                    )),
                    Some(RepoMetadataConnectionLookup::Error(message)) => {
                        Err(ExecError::new(message.clone()))
                    }
                    None => Ok(None),
                }
            }
            "key" => {
                let Some(key) = arguments.string("key") else {
                    return Ok(None);
                };
                Ok(Some(FieldValue::Object(Arc::new(
                    RepoMetadataKeyResultObject {
                        values: self.values.get(key).cloned().unwrap_or_default(),
                    },
                ))))
            }
            _ => Ok(None),
        }
    }
}

/// The database seam for `db.RepoKVPs()` and the repository metadata RBAC
/// check.
#[async_trait]
pub trait RepoMetadataStore: Send + Sync {
    /// `auth.CurrentUser`'s existence check (`users.deleted_at IS NULL`).
    async fn user_exists(&self, user_id: i32) -> Result<bool, String>;
    /// Sorted distinct keys (`ORDER BY key ASC`).
    async fn list_keys(&self, query: Option<&str>) -> Result<Vec<String>, String>;
    /// Sorted distinct non-null values for `key` (`ORDER BY value ASC`).
    async fn list_values(&self, key: &str, query: Option<&str>) -> Result<Vec<String>, String>;
    /// `(key, value)` rows for one repository.
    async fn repo_kvps(&self, repo_id: i32) -> Result<Vec<(String, Option<String>)>, String>;
    /// `rbac.GetPermissionForUser(REPO_METADATA, WRITE)`.
    async fn has_repo_metadata_write(&self, user_id: i32) -> Result<bool, String>;
    /// `db.Phabricator().GetByName`'s SQL fallback: `(url, callsign)`.
    async fn phabricator_repo(&self, repo_name: &str) -> Result<Option<(String, String)>, String>;
}

/// Production adapter over the existing `repo_kvps`, `permissions`,
/// `role_permissions`, `user_roles`, `users` and `phabricator_repos` tables.
pub struct PgRepoMetadataStore {
    pool: PgPool,
}

impl PgRepoMetadataStore {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl RepoMetadataStore for PgRepoMetadataStore {
    async fn user_exists(&self, user_id: i32) -> Result<bool, String> {
        sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS(SELECT 1 FROM users WHERE id = $1 AND deleted_at IS NULL)",
        )
        .bind(user_id)
        .fetch_one(&self.pool)
        .await
        .map_err(|error| error.to_string())
    }

    async fn list_keys(&self, query: Option<&str>) -> Result<Vec<String>, String> {
        let rows = sqlx::query(
            "SELECT key FROM repo_kvps \
             WHERE (($1::text IS NULL) OR key ILIKE '%' || $1 || '%') \
             GROUP BY key ORDER BY key ASC",
        )
        .bind(query)
        .fetch_all(&self.pool)
        .await
        .map_err(|error| error.to_string())?;
        Ok(rows.iter().map(|row| row.get::<String, _>("key")).collect())
    }

    async fn list_values(&self, key: &str, query: Option<&str>) -> Result<Vec<String>, String> {
        let rows = sqlx::query(
            "SELECT DISTINCT value FROM repo_kvps \
             WHERE key = $1 AND value IS NOT NULL \
               AND (($2::text IS NULL) OR value ILIKE '%' || $2 || '%') \
             ORDER BY value ASC",
        )
        .bind(key)
        .bind(query)
        .fetch_all(&self.pool)
        .await
        .map_err(|error| error.to_string())?;
        Ok(rows
            .iter()
            .map(|row| row.get::<String, _>("value"))
            .collect())
    }

    async fn repo_kvps(&self, repo_id: i32) -> Result<Vec<(String, Option<String>)>, String> {
        let rows =
            sqlx::query("SELECT key, value FROM repo_kvps WHERE repo_id = $1 ORDER BY key ASC")
                .bind(repo_id)
                .fetch_all(&self.pool)
                .await
                .map_err(|error| error.to_string())?;
        Ok(rows
            .iter()
            .map(|row| {
                (
                    row.get::<String, _>("key"),
                    row.get::<Option<String>, _>("value"),
                )
            })
            .collect())
    }

    async fn has_repo_metadata_write(&self, user_id: i32) -> Result<bool, String> {
        let found = sqlx::query_scalar::<_, i32>(
            "SELECT permissions.id FROM permissions \
             INNER JOIN role_permissions ON role_permissions.permission_id = permissions.id \
             INNER JOIN user_roles ON user_roles.role_id = role_permissions.role_id \
             WHERE permissions.action = 'WRITE' AND permissions.namespace = 'REPO_METADATA' \
               AND user_roles.user_id = $1 LIMIT 1",
        )
        .bind(user_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|error| error.to_string())?;
        Ok(found.is_some())
    }

    async fn phabricator_repo(&self, repo_name: &str) -> Result<Option<(String, String)>, String> {
        // `getOneBySQL("WHERE repo_name=$1")`: the baseline does not filter
        // `deleted_at`.
        let row = sqlx::query("SELECT url, callsign FROM phabricator_repos WHERE repo_name = $1")
            .bind(repo_name)
            .fetch_optional(&self.pool)
            .await
            .map_err(|error| error.to_string())?;
        Ok(row.map(|row| {
            (
                row.get::<String, _>("url"),
                row.get::<String, _>("callsign"),
            )
        }))
    }
}

/// `rbac.CheckCurrentUserHasPermission(REPO_METADATA#WRITE)`.
pub async fn check_repo_metadata_write(
    store: &dyn RepoMetadataStore,
    actor: Actor,
) -> Result<(), String> {
    match actor {
        Actor::Internal => Ok(()),
        Actor::Anonymous => Err("not authenticated".to_owned()),
        Actor::User { uid } => {
            if !store.user_exists(uid).await? {
                return Err("not authenticated".to_owned());
            }
            match store.has_repo_metadata_write(uid).await? {
                true => Ok(()),
                false => Err("user is missing permission REPO_METADATA#WRITE".to_owned()),
            }
        }
    }
}

/// `gqlutil.ConnectionResolverOptions.ApplyMaxPageSize`.
fn apply_max_page_size(limit: i64) -> i64 {
    if limit == 0 {
        100
    } else if limit < 100 {
        limit
    } else {
        100
    }
}

/// Page a fully-ordered string list the way `gqlutil.ConnectionResolver`
/// pages `RepoKVPs.ListKeys`/`ListValues` (first/after, last/before;
/// `Reverse: false`).
pub fn paginate_strings(
    full: &[String],
    args: &RepoMetadataArgs,
    cursor_kind: &str,
) -> Result<(Vec<String>, BidirectionalPageInfoData), String> {
    let (page_size, first_mode) = match (args.first, args.last) {
        (Some(first), _) => (apply_max_page_size(first), true),
        (None, Some(last)) => (apply_max_page_size(last), false),
        (None, None) => {
            return Err(
                "you must provide a `first` or `last` value to properly paginate".to_owned(),
            )
        }
    };
    let limit = page_size + 1;
    let after_value = match &args.after {
        Some(cursor) => Some(unmarshal_relay_str(cursor)?),
        None => None,
    };
    let before_value = match &args.before {
        Some(cursor) => Some(unmarshal_relay_str(cursor)?),
        None => None,
    };

    let candidates: Vec<String> = full
        .iter()
        .filter(|value| {
            after_value
                .as_ref()
                .is_none_or(|after| value.as_str() > after.as_str())
        })
        .filter(|value| {
            before_value
                .as_ref()
                .is_none_or(|before| value.as_str() < before.as_str())
        })
        .cloned()
        .collect();

    let (nodes, page_info) = if first_mode {
        let raw: Vec<String> = candidates.into_iter().take(limit.max(0) as usize).collect();
        let fetched = raw.len() as i64;
        let mut nodes = raw;
        if fetched > page_size {
            nodes.truncate(page_size.max(0) as usize);
        }
        let page_info = BidirectionalPageInfoData {
            has_next_page: fetched > page_size,
            has_previous_page: if fetched == 0 {
                false
            } else {
                args.after.is_some()
            },
            start_cursor: nodes
                .first()
                .map(|node| marshal_relay_id_str(cursor_kind, node)),
            end_cursor: nodes
                .last()
                .map(|node| marshal_relay_id_str(cursor_kind, node)),
        };
        (nodes, page_info)
    } else {
        // SQL `ORDER BY key DESC LIMIT limit` with `Reverse=false`: the tail
        // is returned newest-first.
        let mut raw: Vec<String> = candidates.into_iter().rev().collect();
        raw.truncate(limit.max(0) as usize);
        let fetched = raw.len() as i64;
        let mut nodes = raw;
        if fetched > page_size {
            nodes.remove(0);
        }
        let page_info = BidirectionalPageInfoData {
            has_next_page: if fetched == 0 {
                false
            } else {
                args.before.is_some()
            },
            has_previous_page: fetched > page_size,
            start_cursor: nodes
                .first()
                .map(|node| marshal_relay_id_str(cursor_kind, node)),
            end_cursor: nodes
                .last()
                .map(|node| marshal_relay_id_str(cursor_kind, node)),
        };
        (nodes, page_info)
    };
    Ok((nodes, page_info))
}

// ---------------------------------------------------------------------------
// Git refs / contributors (replayed from git_graphql::GitPrefetch)
// ---------------------------------------------------------------------------

pub struct GitRefConnectionObject {
    pub repo: MinimalRepo,
    pub git: Option<Arc<crate::git_graphql::GitPrefetch>>,
    /// The full (unpaginated, query-filtered) refs.
    pub refs: Vec<(String, Option<String>)>,
    pub first: Option<i32>,
}

impl ObjectValue for GitRefConnectionObject {
    fn type_name(&self) -> &'static str {
        "GitRefConnection"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        let nodes: Vec<&(String, Option<String>)> = match self.first {
            Some(first) if first >= 0 && self.refs.len() > first as usize => {
                self.refs[..first as usize].iter().collect()
            }
            _ => self.refs.iter().collect(),
        };
        match field {
            "nodes" => Ok(Some(FieldValue::List {
                items: nodes
                    .iter()
                    .map(|(name, target)| {
                        FieldValue::Object(Arc::new(
                            crate::search_graphql::GitRefObject::with_target(
                                self.repo.clone(),
                                name.clone(),
                                target.clone(),
                                self.git.clone(),
                            ),
                        ))
                    })
                    .collect(),
                element_type: Some("GitRef"),
            })),
            "totalCount" => Ok(Some(FieldValue::Json(Json::Int(self.refs.len() as i64)))),
            "pageInfo" => Ok(Some(FieldValue::Object(Arc::new(PageInfoObject::new(
                self.first
                    .is_some_and(|first| (first as usize) < self.refs.len()),
                None,
            ))))),
            _ => Ok(None),
        }
    }
}

pub use crate::git_graphql::ContributorCountData;

pub struct RepositoryContributorConnectionObject {
    pub repo: MinimalRepo,
    pub git: Option<Arc<crate::git_graphql::GitPrefetch>>,
    pub data: ContributorsConnectionData,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContributorsConnectionData {
    pub total_count: i64,
    pub page: Result<(Vec<(usize, ContributorCountData)>, BidirectionalPageInfoData), String>,
}

impl ObjectValue for RepositoryContributorConnectionObject {
    fn type_name(&self) -> &'static str {
        "RepositoryContributorConnection"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "totalCount" => Ok(Some(FieldValue::Json(Json::Int(self.data.total_count)))),
            "nodes" => match &self.data.page {
                Ok((nodes, _)) => Ok(Some(FieldValue::List {
                    items: nodes
                        .iter()
                        .map(|(_, contributor)| {
                            FieldValue::Object(Arc::new(RepositoryContributorObject {
                                repo: self.repo.clone(),
                                git: self.git.clone(),
                                data: contributor.clone(),
                            }))
                        })
                        .collect(),
                    element_type: Some("RepositoryContributor"),
                })),
                Err(message) => Err(ExecError::new(message.clone())),
            },
            "pageInfo" => match &self.data.page {
                Ok((_, page_info)) => Ok(Some(FieldValue::Object(Arc::new(
                    page_info.clone(),
                )))),
                Err(message) => Err(ExecError::new(message.clone())),
            },
            _ => Ok(None),
        }
    }
}

pub struct RepositoryContributorObject {
    pub repo: MinimalRepo,
    pub git: Option<Arc<crate::git_graphql::GitPrefetch>>,
    pub data: ContributorCountData,
}

impl ObjectValue for RepositoryContributorObject {
    fn type_name(&self) -> &'static str {
        "RepositoryContributor"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "person" => Ok(Some(FieldValue::Object(Arc::new(
                crate::git_graphql::PersonObject {
                    name: self.data.name.clone(),
                    email: self.data.email.clone(),
                },
            )))),
            "count" => Ok(Some(FieldValue::Json(Json::Int(i64::from(self.data.count))))),
            "repository" => Ok(Some(FieldValue::Object(Arc::new(
                crate::graphql_resolvers::RepositoryObject::from_data(
                    crate::git_graphql::repository_data(&self.repo),
                )
                .with_git(self.git.clone()),
            )))),
            // `repositoryContributorResolver.Commits` needs a `git log`
            // author search; no such seam exists in this snapshot.
            "commits" => Err(ExecError::new(
                "RepositoryContributor.commits is not ported (M9.19a)",
            )),
            _ => Ok(None),
        }
    }
}

/// `strconv.Atoi` for the contributor cursor.
fn parse_go_int(raw: &str) -> Result<i64, String> {
    match raw.parse::<i64>() {
        Ok(value) => Ok(value),
        Err(error) => {
            let reason = match error.kind() {
                std::num::IntErrorKind::PosOverflow | std::num::IntErrorKind::NegOverflow => {
                    "value out of range"
                }
                _ => "invalid syntax",
            };
            Err(format!("strconv.Atoi: parsing {raw:?}: {reason}"))
        }
    }
}

/// `database.OffsetBasedCursorSlice` over the contributor list, followed by
/// the connection resolver's trimming and page info.
pub fn page_contributors(
    full: &[ContributorCountData],
    first: Option<i64>,
    last: Option<i64>,
    after: Option<&str>,
    before: Option<&str>,
) -> Result<(Vec<(usize, ContributorCountData)>, BidirectionalPageInfoData), String> {
    let (page_size, first_mode) = match (first, last) {
        (Some(first), _) => (apply_max_page_size(first), true),
        (None, Some(last)) => (apply_max_page_size(last), false),
        (None, None) => {
            return Err(
                "you must provide a `first` or `last` value to properly paginate".to_owned(),
            )
        }
    };
    let limit = page_size + 1;
    let after_index = match after {
        Some(cursor) => Some(parse_go_int(cursor)?),
        None => None,
    };
    let before_index = match before {
        Some(cursor) => Some(parse_go_int(cursor)?),
        None => None,
    };
    let total = full.len() as i64;
    let (start, end) = if first_mode {
        let start = match after_index {
            Some(after) => (after.saturating_add(1)).clamp(0, total),
            None => 0,
        };
        let end = start.saturating_add(limit).clamp(start, total);
        (start, end)
    } else {
        let end = match before_index {
            Some(before) => before.clamp(0, total),
            None => total,
        };
        let start = end.saturating_sub(limit).clamp(0, end);
        (start, end)
    };
    let mut nodes: Vec<(usize, ContributorCountData)> = (start..end)
        .map(|index| (index as usize, full[index as usize].clone()))
        .collect();
    let fetched = nodes.len() as i64;
    if fetched > page_size {
        if first_mode {
            nodes.truncate(page_size.max(0) as usize);
        } else {
            nodes.remove(0);
        }
    }
    let page_info = BidirectionalPageInfoData {
        has_next_page: if first_mode {
            fetched > page_size
        } else if fetched == 0 {
            false
        } else {
            before.is_some()
        },
        has_previous_page: if first_mode {
            if fetched == 0 {
                false
            } else {
                after.is_some()
            }
        } else {
            fetched > page_size
        },
        start_cursor: nodes.first().map(|(index, _)| index.to_string()),
        end_cursor: nodes.last().map(|(index, _)| index.to_string()),
    };
    Ok((nodes, page_info))
}

// ---------------------------------------------------------------------------
// Text search index
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TextSearchIndexBranch {
    pub name: String,
    pub version: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TextSearchIndexEntry {
    pub index_time: OffsetDateTime,
    pub content_bytes: i64,
    pub documents: i32,
    pub index_bytes: i32,
    pub shards: i32,
    pub new_lines: i32,
    pub default_branch_new_lines: i32,
    pub other_branches_new_lines: i32,
    pub branches: Vec<TextSearchIndexBranch>,
}

/// The zoekt read seam (`search.Indexed()`, `searchzoekt.GetIndexserverHost`
/// and the ref resolution `Refs`/`Current` perform through gitserver).
#[async_trait]
pub trait ZoektIndexSource: Send + Sync {
    async fn repo_entry(&self, repo_id: i32) -> Result<Option<TextSearchIndexEntry>, String>;
    /// `GetIndexserverHost`; errors are swallowed by the resolver (nil).
    async fn indexserver_host(&self, repo_name: &str) -> Option<String>;
    async fn skipped_count(&self, repo_id: i32, branch: &str) -> Result<i64, String>;
    /// The `refs/heads/...`/`refs/tags/...` canonicalization `refByName`
    /// performs via `ResolveRevision`; the input is returned unchanged when
    /// neither resolves.
    async fn canonical_ref(&self, repo_name: &str, name: &str) -> String;
    /// `ref.Target().Commit` for `Current`: the ref's current OID.
    async fn ref_target_oid(
        &self,
        repo_id: i32,
        repo_name: &str,
        ref_name: &str,
    ) -> Result<String, String>;
}

/// No zoekt endpoints configured: the Go zero-value shape.
#[derive(Debug, Default, Clone, Copy)]
pub struct EmptyZoektIndexSource;

#[async_trait]
impl ZoektIndexSource for EmptyZoektIndexSource {
    async fn repo_entry(&self, _repo_id: i32) -> Result<Option<TextSearchIndexEntry>, String> {
        Ok(None)
    }

    async fn indexserver_host(&self, _repo_name: &str) -> Option<String> {
        None
    }

    async fn skipped_count(&self, _repo_id: i32, _branch: &str) -> Result<i64, String> {
        Ok(0)
    }

    async fn canonical_ref(&self, _repo_name: &str, name: &str) -> String {
        name.to_owned()
    }

    async fn ref_target_oid(
        &self,
        _repo_id: i32,
        _repo_name: &str,
        _ref_name: &str,
    ) -> Result<String, String> {
        Err("no text search index".to_owned())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TextSearchIndexRefData {
    pub name: String,
    pub target: Option<String>,
    pub indexed_commit: String,
    pub current: Result<bool, String>,
    /// `(raw zoekt branch, skipped count)`.
    pub skipped: Option<Result<(String, i64), String>>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TextSearchIndexPrefetch {
    pub entries: HashMap<i32, Result<Option<TextSearchIndexEntry>, String>>,
    pub hosts: HashMap<i32, Option<String>>,
    pub refs: HashMap<i32, Result<Vec<TextSearchIndexRefData>, String>>,
}

pub struct RepositoryTextSearchIndexObject {
    pub repo: MinimalRepo,
    pub git: Option<Arc<crate::git_graphql::GitPrefetch>>,
    pub zoekt: Arc<TextSearchIndexPrefetch>,
}

impl ObjectValue for RepositoryTextSearchIndexObject {
    fn type_name(&self) -> &'static str {
        "RepositoryTextSearchIndex"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "repository" => Ok(Some(FieldValue::Object(Arc::new(
                crate::graphql_resolvers::RepositoryObject::from_data(
                    crate::git_graphql::repository_data(&self.repo),
                )
                .with_git(self.git.clone()),
            )))),
            "status" => match self.zoekt.entries.get(&self.repo.id) {
                Some(Ok(Some(entry))) => Ok(Some(FieldValue::Object(Arc::new(
                    RepositoryTextSearchIndexStatusObject {
                        entry: entry.clone(),
                    },
                )))),
                Some(Ok(None)) | None => Ok(Some(FieldValue::Null)),
                Some(Err(message)) => Err(ExecError::new(message.clone())),
            },
            "host" => match self.zoekt.hosts.get(&self.repo.id).cloned().flatten() {
                Some(name) => Ok(Some(FieldValue::Object(Arc::new(
                    RepositoryIndexserverHostObject { name },
                )))),
                None => Ok(Some(FieldValue::Null)),
            },
            "refs" => match self.zoekt.refs.get(&self.repo.id) {
                Some(Ok(refs)) => Ok(Some(FieldValue::List {
                    items: refs
                        .iter()
                        .map(|data| FieldValue::Object(Arc::new(
                            RepositoryTextSearchIndexedRefObject {
                                repo: self.repo.clone(),
                                git: self.git.clone(),
                                data: data.clone(),
                            },
                        )))
                        .collect(),
                    element_type: Some("RepositoryTextSearchIndexedRef"),
                })),
                Some(Err(message)) => Err(ExecError::new(message.clone())),
                None => Ok(None),
            },
            _ => Ok(None),
        }
    }
}

pub struct RepositoryTextSearchIndexStatusObject {
    pub entry: TextSearchIndexEntry,
}

impl ObjectValue for RepositoryTextSearchIndexStatusObject {
    fn type_name(&self) -> &'static str {
        "RepositoryTextSearchIndexStatus"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        let int = |value: i32| FieldValue::Json(Json::Int(i64::from(value)));
        match field {
            "updatedAt" => Ok(Some(FieldValue::Json(Json::Str(datetime_rfc3339(
                self.entry.index_time,
            ))))),
            "contentByteSize" => Ok(Some(FieldValue::Json(Json::Int(
                self.entry.content_bytes,
            )))),
            "contentFilesCount" => Ok(Some(int(self.entry.documents))),
            "indexByteSize" => Ok(Some(int(self.entry.index_bytes))),
            "indexShardsCount" => Ok(Some(int(self.entry.shards))),
            "newLinesCount" => Ok(Some(int(self.entry.new_lines))),
            "defaultBranchNewLinesCount" => {
                Ok(Some(int(self.entry.default_branch_new_lines)))
            }
            "otherBranchesNewLinesCount" => {
                Ok(Some(int(self.entry.other_branches_new_lines)))
            }
            _ => Ok(None),
        }
    }
}

pub struct RepositoryIndexserverHostObject {
    pub name: String,
}

impl ObjectValue for RepositoryIndexserverHostObject {
    fn type_name(&self) -> &'static str {
        "repositoryIndexserverHost"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "name" => Ok(Some(FieldValue::Json(Json::Str(self.name.clone())))),
            _ => Ok(None),
        }
    }
}

pub struct RepositoryTextSearchIndexedRefObject {
    pub repo: MinimalRepo,
    pub git: Option<Arc<crate::git_graphql::GitPrefetch>>,
    pub data: TextSearchIndexRefData,
}

impl ObjectValue for RepositoryTextSearchIndexedRefObject {
    fn type_name(&self) -> &'static str {
        "RepositoryTextSearchIndexedRef"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "ref" => Ok(Some(FieldValue::Object(Arc::new(
                crate::search_graphql::GitRefObject::with_target(
                    self.repo.clone(),
                    self.data.name.clone(),
                    self.data.target.clone(),
                    self.git.clone(),
                ),
            )))),
            "indexed" => Ok(Some(FieldValue::Json(Json::Bool(
                !self.data.indexed_commit.is_empty(),
            )))),
            "current" => match &self.data.current {
                Ok(current) => Ok(Some(FieldValue::Json(Json::Bool(*current)))),
                Err(message) => Err(ExecError::new(message.clone())),
            },
            "indexedCommit" => {
                if self.data.indexed_commit.is_empty() {
                    Ok(Some(FieldValue::Null))
                } else {
                    Ok(Some(FieldValue::Object(Arc::new(
                        crate::git_graphql::GitObjectResolverObject::from_object(
                            self.repo.clone(),
                            sg_git::GitObject {
                                oid: self.data.indexed_commit.clone(),
                                object_type: sg_git::GitObjectType::Commit,
                            },
                            self.git.clone(),
                        ),
                    ))))
                }
            }
            "skippedIndexed" => match &self.data.skipped {
                Some(skipped) => Ok(Some(FieldValue::Object(Arc::new(SkippedIndexedObject {
                    skipped: skipped.clone(),
                    repo_name: self.repo.name.clone(),
                })))),
                None => Ok(Some(FieldValue::Null)),
            },
            _ => Ok(None),
        }
    }
}

pub struct SkippedIndexedObject {
    /// `(raw zoekt branch, StreamSearch FileCount)`.
    pub skipped: Result<(String, i64), String>,
    pub repo_name: String,
}

impl ObjectValue for SkippedIndexedObject {
    fn type_name(&self) -> &'static str {
        "SkippedIndexed"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "count" => match &self.skipped {
                Ok((_, count)) => Ok(Some(FieldValue::Json(Json::Int(*count)))),
                Err(message) => Err(ExecError::new(message.clone())),
            },
            "query" => match &self.skipped {
                Ok((branch, _)) => Ok(Some(FieldValue::Json(Json::Str(format!(
                    "r:^{}$@{} type:file select:file index:only patternType:regexp ^NOT-INDEXED:",
                    quote_meta(&self.repo_name),
                    branch
                ))))),
                Err(message) => Err(ExecError::new(message.clone())),
            },
            _ => Ok(None),
        }
    }
}

/// `regexp.QuoteMeta`.
fn quote_meta(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for ch in value.chars() {
        if "\\.+*?()|[]{}^$".contains(ch) {
            out.push('\\');
        }
        out.push(ch);
    }
    out
}

// ---------------------------------------------------------------------------
// Hunk
// ---------------------------------------------------------------------------

/// A `GitBlob` wrapper that adds the `blame` field while delegating every
/// other field to the ported tree-entry object.
pub struct GitBlobD {
    pub inner: Arc<dyn ObjectValue>,
    pub repo: MinimalRepo,
    pub oid: String,
    pub path: String,
    pub git: Option<Arc<crate::git_graphql::GitPrefetch>>,
}

impl ObjectValue for GitBlobD {
    fn type_name(&self) -> &'static str {
        self.inner.type_name()
    }

    fn field(
        &self,
        field: &str,
        arguments: &Arguments,
        actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        if field != "blame" {
            return self.inner.field(field, arguments, actor);
        }
        let start_line = arguments.i64("startLine").map(|line| line as i32);
        let end_line = arguments.i64("endLine").map(|line| line as i32);
        if start_line.is_some() != end_line.is_some() {
            return Err(ExecError::new(
                "both startLine and endLine must be specified or neither",
            ));
        }
        let ignore_whitespace = arguments.bool("ignoreWhitespace").unwrap_or(false);
        let Some(git) = &self.git else {
            return Err(ExecError::new(
                "blame requires the gitserver GitSource (not wired for this request)",
            ));
        };
        let key = crate::git_graphql::BlameKey {
            repo_id: self.repo.id,
            oid: self.oid.clone(),
            path: self.path.clone(),
            start_line,
            end_line,
            ignore_whitespace,
        };
        match git.blame(&key) {
            Some(crate::git_graphql::BlameLookup::Found(hunks)) => {
                Ok(Some(FieldValue::List {
                    items: hunks
                        .iter()
                        .map(|hunk| {
                            FieldValue::Object(Arc::new(HunkObject {
                                repo: self.repo.clone(),
                                git: self.git.clone(),
                                hunk: hunk.clone(),
                            }))
                        })
                        .collect(),
                    element_type: Some("Hunk"),
                }))
            }
            Some(crate::git_graphql::BlameLookup::Error(message)) => {
                Err(ExecError::new(message.clone()))
            }
            None => Err(ExecError::new("blame is not prefetched")),
        }
    }
}

/// `hunkResolver` (`hunk.go`). `StartByte` returns `EndLine`: the baseline
/// bug at `hunk.go:31-33` is reproduced verbatim.
pub struct HunkObject {
    pub repo: MinimalRepo,
    pub git: Option<Arc<crate::git_graphql::GitPrefetch>>,
    pub hunk: sg_git::BlameHunk,
}

impl ObjectValue for HunkObject {
    fn type_name(&self) -> &'static str {
        "Hunk"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        let int = |value: u32| FieldValue::Json(Json::Int(i64::from(value)));
        match field {
            "author" => Ok(Some(FieldValue::Object(Arc::new(
                crate::git_graphql::SignatureObject {
                    name: self.hunk.author.name.clone(),
                    email: self.hunk.author.email.clone(),
                    date: self.hunk.author.date,
                },
            )))),
            "startLine" => Ok(Some(int(self.hunk.start_line))),
            "endLine" => Ok(Some(int(self.hunk.end_line))),
            // Baseline bug: `StartByte` returns `hunk.EndLine`.
            "startByte" => Ok(Some(int(self.hunk.end_line))),
            "endByte" => Ok(Some(int(self.hunk.end_byte))),
            "rev" => Ok(Some(FieldValue::Json(Json::Str(
                self.hunk.commit_id.clone(),
            )))),
            "message" => Ok(Some(FieldValue::Json(Json::Str(self.hunk.message.clone())))),
            "filename" => Ok(Some(FieldValue::Json(Json::Str(
                self.hunk.filename.clone(),
            )))),
            "commit" => {
                let (commit, commit_error) = match &self.git {
                    Some(git) => git.commit_data(self.repo.id, &self.hunk.commit_id),
                    None => (None, None),
                };
                Ok(Some(FieldValue::Object(Arc::new(
                    crate::git_graphql::GitCommitResolverObject {
                        repo: self.repo.clone(),
                        oid: self.hunk.commit_id.clone(),
                        input_rev: None,
                        commit,
                        commit_error,
                        git: self.git.clone(),
                    },
                ))))
            }
            _ => Ok(None),
        }
    }
}

// ---------------------------------------------------------------------------
// Webhook.webhookLogs
// ---------------------------------------------------------------------------

pub struct WebhookObjectD {
    pub inner: crate::graphql_inventory_c::WebhookObject,
    pub nested: Arc<crate::graphql_resolvers::NestedPrefetch>,
}

impl ObjectValue for WebhookObjectD {
    fn type_name(&self) -> &'static str {
        "Webhook"
    }

    fn field(
        &self,
        field: &str,
        arguments: &Arguments,
        actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        if field != "webhookLogs" {
            return self.inner.field(field, arguments, actor);
        }
        let args = crate::webhook_logs::webhook_logs_args(arguments);
        match self
            .nested
            .webhook_webhook_logs
            .get(&(self.inner.data.id, args))
        {
            Some(crate::webhook_logs::WebhookLogsConnectionLookup::Found(data)) => {
                Ok(Some(FieldValue::Object(Arc::new(
                    crate::webhook_logs::WebhookLogConnectionObject::from_data(
                        data.clone(),
                        self.inner.viewer_site_admin,
                        self.inner.nested.clone(),
                    ),
                ))))
            }
            Some(crate::webhook_logs::WebhookLogsConnectionLookup::Error(message)) => {
                Err(ExecError::new(message.clone()))
            }
            None => Ok(None),
        }
    }
}

pub struct WebhookConnectionObjectD {
    pub data: crate::graphql_inventory_c::WebhookConnectionData,
    pub viewer_site_admin: bool,
    pub nested: Arc<crate::graphql_resolvers::NestedPrefetch>,
}

impl ObjectValue for WebhookConnectionObjectD {
    fn type_name(&self) -> &'static str {
        "WebhookConnection"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "nodes" => Ok(Some(FieldValue::List {
                element_type: Some("Webhook"),
                items: self
                    .data
                    .nodes
                    .iter()
                    .cloned()
                    .map(|data| {
                        FieldValue::Object(Arc::new(WebhookObjectD {
                            inner: crate::graphql_inventory_c::WebhookObject {
                                data,
                                viewer_site_admin: self.viewer_site_admin,
                                nested: self.nested.clone(),
                            },
                            nested: self.nested.clone(),
                        }))
                    })
                    .collect(),
            })),
            "totalCount" => Ok(Some(FieldValue::Json(Json::Int(self.data.total_count)))),
            "pageInfo" => Ok(Some(FieldValue::Object(Arc::new(PageInfoObject::new(
                self.data.has_next_page,
                self.data.end_cursor.clone(),
            ))))),
            _ => Ok(None),
        }
    }
}

// ---------------------------------------------------------------------------
// Prefetch roots
// ---------------------------------------------------------------------------

#[derive(Debug, Default)]
pub struct InventoryDRoots {
    pub repo_kvps: HashMap<i32, RepoKvpsLookup>,
    pub repo_external_links: HashMap<i32, Result<Vec<ExternalLinkData>, String>>,
    pub repo_git_refs: HashMap<i32, Result<GitRefsData, String>>,
    pub repo_contributors: HashMap<i32, Result<ContributorsConnectionData, String>>,
    pub zoekt: TextSearchIndexPrefetch,
    pub repo_meta_keys: HashMap<RepoMetadataArgs, RepoMetadataConnectionLookup>,
    pub repo_meta_values:
        HashMap<(String, RepoMetadataArgs), RepoMetadataConnectionLookup>,
}

/// The unpaginated, query-filtered ref list plus the `first` argument.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GitRefsData {
    pub refs: Vec<(String, Option<String>)>,
    pub first: Option<i32>,
}

fn mentions(fields: &[RootField], name: &str) -> bool {
    fields.iter().any(|field| {
        field.name == name
            || mentions(&field.selections, name)
            || field
                .type_conditions
                .iter()
                .any(|(_, nested)| mentions(nested, name))
    })
}

fn collect_repo(out: &mut HashMap<i32, (String, String)>, lookup: &RepoLookup) {
    if let RepoLookup::Found(data) = lookup {
        out.entry(data.id)
            .or_insert_with(|| (data.name.clone(), data.uri.clone()));
    }
}

/// Convert one repository `ListRefs` result into the resolver's `refsArgs`
/// shape (`repository_git_refs.go:28-55`).
fn build_git_refs(
    refs: &[sg_git::GitRef],
    query: Option<&str>,
    first: Option<i32>,
) -> GitRefsData {
    let query = query.map(str::to_lowercase).unwrap_or_default();
    let filtered = refs
        .iter()
        .filter(|reference| {
            query.is_empty() || reference.short_name.to_lowercase().contains(&query)
        })
        .map(|reference| (reference.name.clone(), Some(reference.target_commit.clone())))
        .collect();
    GitRefsData {
        refs: filtered,
        first,
    }
}

/// Build the text-search-index ref list (`repository_text_search_index.go:121-176`).
async fn build_text_index_refs(
    repo: &MinimalRepo,
    zoekt: &dyn ZoektIndexSource,
    default_branch: &crate::git_graphql::DefaultBranchLookup,
    entry: Option<&TextSearchIndexEntry>,
) -> Result<Vec<TextSearchIndexRefData>, String> {
    let default_name = match default_branch {
        crate::git_graphql::DefaultBranchLookup::Found(name) => name.clone(),
        crate::git_graphql::DefaultBranchLookup::NotFound => return Ok(Vec::new()),
        crate::git_graphql::DefaultBranchLookup::Error(message) => {
            return Err(message.clone())
        }
    };
    let mut refs: Vec<TextSearchIndexRefData> = vec![TextSearchIndexRefData {
        name: default_name.clone(),
        target: None,
        indexed_commit: String::new(),
        current: Ok(false),
        skipped: None,
    }];
    if let Some(entry) = entry {
        let mut skipped_counts: Vec<(String, Result<i64, String>)> = Vec::new();
        for branch in &entry.branches {
            skipped_counts.push((
                branch.name.clone(),
                zoekt.skipped_count(repo.id, &branch.name).await,
            ));
            let name = if branch.name == "HEAD" {
                default_name.clone()
            } else {
                branch.name.clone()
            };
            let canonical = zoekt.canonical_ref(&repo.name, &name).await;
            let skipped = skipped_counts
                .last()
                .cloned()
                .map(|(raw, count)| count.map(|count| (raw, count)));
            if let Some(existing) = refs.iter_mut().find(|data| data.name == canonical) {
                existing.indexed_commit = branch.version.clone();
                existing.skipped = skipped;
            } else {
                refs.push(TextSearchIndexRefData {
                    name: canonical,
                    target: None,
                    indexed_commit: branch.version.clone(),
                    current: Ok(false),
                    skipped,
                });
            }
        }
        for data in refs.iter_mut() {
            let target = zoekt
                .ref_target_oid(repo.id, &repo.name, &data.name)
                .await;
            match target {
                Ok(oid) => {
                    if !data.indexed_commit.is_empty() {
                        data.current = Ok(oid == data.indexed_commit);
                    }
                    data.target = Some(oid);
                }
                Err(message) => {
                    data.current = Err(message);
                }
            }
        }
    }
    Ok(refs)
}

/// The M9.19a repository/text-index prefetch. Runs only for the selected
/// repository roots; `None` selections stay absent (the resolver reports the
/// field as unported).
#[allow(clippy::too_many_arguments)]
pub async fn prefetch_repository_inventory_d(
    store: &dyn RepoMetadataStore,
    zoekt: &dyn ZoektIndexSource,
    git: Option<&crate::git_graphql::GitPrefetch>,
    repositories: &HashMap<RepoKey, RepoLookup>,
    repository_redirects: &HashMap<RepositoryRedirectKey, RepoLookup>,
    nodes: &NodeRoots,
    fields: &[RootField],
    actor: Actor,
) -> InventoryDRoots {
    let mut roots = InventoryDRoots::default();
    let selects_kvps = mentions(fields, "metadata") || mentions(fields, "keyValuePairs");
    let selects_links = mentions(fields, "externalURLs");
    let selects_zoekt = mentions(fields, "textSearchIndex");
    let mut refs_selections: Vec<(bool, bool, Option<String>, Option<i32>)> = Vec::new();
    let mut contributor_selections: Vec<(
        Option<String>,
        Option<String>,
        Option<String>,
        Option<i64>,
        Option<i64>,
        Option<String>,
        Option<String>,
    )> = Vec::new();
    collect_object_selections(
        fields,
        &mut refs_selections,
        &mut contributor_selections,
    );
    let selects_repo_meta = fields.iter().any(|field| field.name == "repoMeta");

    if !selects_kvps
        && !selects_links
        && !selects_zoekt
        && refs_selections.is_empty()
        && contributor_selections.is_empty()
        && !selects_repo_meta
    {
        return roots;
    }

    let mut repos: HashMap<i32, (String, String)> = HashMap::new();
    for lookup in repositories.values() {
        collect_repo(&mut repos, lookup);
    }
    for lookup in repository_redirects.values() {
        collect_repo(&mut repos, lookup);
    }
    for lookup in nodes.repositories.values() {
        collect_repo(&mut repos, lookup);
    }

    if selects_kvps {
        for repo_id in repos.keys() {
            roots.repo_kvps.insert(
                *repo_id,
                store.repo_kvps(*repo_id).await.map(|rows| {
                    rows.into_iter()
                        .map(|(key, value)| KeyValuePairData { key, value })
                        .collect()
                }),
            );
        }
    }
    if selects_links {
        if let Some(git) = git {
            for (repo_id, (repo_name, _)) in &repos {
                let lookup = match git.default_branches.get(repo_id) {
                    Some(crate::git_graphql::DefaultBranchLookup::Error(message)) => {
                        Err(message.clone())
                    }
                    _ => {
                        // `NewRepositoryLinker`: a Phabricator store error is
                        // logged and ignored (the `Root` code-host link is not
                        // available without `types.Repo.Metadata`).
                        match store.phabricator_repo(repo_name).await {
                            Ok(phab) => Ok(phab
                                .map(|(url, callsign)| ExternalLinkData {
                                    url: format!(
                                        "{}/diffusion/{}",
                                        url.trim_end_matches('/'),
                                        callsign
                                    ),
                                    service_kind: "PHABRICATOR".to_owned(),
                                    service_type: "phabricator".to_owned(),
                                })
                                .into_iter()
                                .collect()),
                            Err(_) => Ok(Vec::new()),
                        }
                    }
                };
                roots.repo_external_links.insert(*repo_id, lookup);
            }
        }
    }
    if selects_zoekt {
        let mut repo_names: Vec<(i32, String)> = repos
            .iter()
            .map(|(id, (name, _))| (*id, name.clone()))
            .collect();
        repo_names.sort();
        for (repo_id, repo_name) in repo_names {
            let entry = zoekt.repo_entry(repo_id).await;
            let host = zoekt.indexserver_host(&repo_name).await;
            let default_branch = git.and_then(|git| git.default_branches.get(&repo_id).cloned());
            let refs = match (&entry, default_branch) {
                (Err(message), _) => Err(message.clone()),
                (Ok(entry), Some(default_branch)) => {
                    let repo = MinimalRepo {
                        id: repo_id,
                        name: repo_name.clone(),
                    };
                    build_text_index_refs(&repo, zoekt, &default_branch, entry.as_ref()).await
                }
                (Ok(_), None) => Err("repository default branch is not prefetched".to_owned()),
            };
            roots.zoekt.entries.insert(repo_id, entry);
            roots.zoekt.hosts.insert(repo_id, host);
            roots.zoekt.refs.insert(repo_id, refs);
        }
    }
    if let Some(git) = git {
        if !refs_selections.is_empty() {
            for (repo_id, _) in &repos {
                let mut entries: Vec<GitRefsData> = Vec::new();
                for (heads, tags, query, first) in &refs_selections {
                    let key = crate::git_graphql::RefsKey {
                        repo_id: *repo_id,
                        heads_only: *heads,
                        tags_only: *tags,
                    };
                    match git.refs.get(&key) {
                        Some(Ok(refs)) => entries.push(build_git_refs(
                            refs,
                            query.as_deref(),
                            *first,
                        )),
                        Some(Err(message)) => {
                            roots.repo_git_refs.insert(*repo_id, Err(message.clone()));
                            entries.clear();
                            break;
                        }
                        None => {
                            roots.repo_git_refs.insert(
                                *repo_id,
                                Err("git refs are not prefetched".to_owned()),
                            );
                            entries.clear();
                            break;
                        }
                    }
                }
                if roots.repo_git_refs.contains_key(repo_id) {
                    continue;
                }
                if let Some(entry) = entries.into_iter().next() {
                    roots.repo_git_refs.insert(*repo_id, Ok(entry));
                }
            }
        }
        if !contributor_selections.is_empty() {
            for (repo_id, _) in &repos {
                let mut computed: Option<Result<ContributorsConnectionData, String>> = None;
                for (revision_range, after_date, path, first, last, after, before) in
                    &contributor_selections
                {
                    let key = crate::git_graphql::ContributorsKey {
                        repo_id: *repo_id,
                        revision_range: revision_range.clone(),
                        after_date: after_date.clone(),
                        path: path.clone(),
                    };
                    let lookup = match git.contributors.get(&key) {
                        Some(Ok(counts)) => Ok(ContributorsConnectionData {
                            total_count: counts.len() as i64,
                            page: page_contributors(
                                counts,
                                *first,
                                *last,
                                after.as_deref(),
                                before.as_deref(),
                            ),
                        }),
                        Some(Err(message)) => Err(message.clone()),
                        None => Err("contributors are not prefetched".to_owned()),
                    };
                    if lookup.is_err() {
                        computed = Some(lookup);
                        break;
                    }
                    computed = Some(lookup);
                }
                if let Some(lookup) = computed {
                    roots.repo_contributors.insert(*repo_id, lookup);
                }
            }
        }
    }
    if selects_repo_meta {
        prefetch_repo_metadata(store, fields, actor, &mut roots).await;
    }
    roots
}

/// The ref/contributor argument shapes collected from the selection set.
#[allow(clippy::type_complexity)]
fn collect_object_selections(
    fields: &[RootField],
    refs: &mut Vec<(bool, bool, Option<String>, Option<i32>)>,
    contributors: &mut Vec<(
        Option<String>,
        Option<String>,
        Option<String>,
        Option<i64>,
        Option<i64>,
        Option<String>,
        Option<String>,
    )>,
) {
    for field in fields {
        match field.name.as_str() {
            "gitRefs" => {
                let arguments = &field.arguments;
                let (heads, tags) = match arguments.string("type") {
                    None => (true, true),
                    Some("GIT_BRANCH") => (true, false),
                    Some("GIT_TAG") => (false, true),
                    Some(_) => (false, false),
                };
                refs.push((
                    heads,
                    tags,
                    arguments.string("query").map(str::to_owned),
                    arguments
                        .i64("first")
                        .and_then(|value| i32::try_from(value).ok()),
                ));
            }
            "branches" | "tags" => {
                let arguments = &field.arguments;
                refs.push((
                    field.name == "branches",
                    field.name == "tags",
                    arguments.string("query").map(str::to_owned),
                    arguments
                        .i64("first")
                        .and_then(|value| i32::try_from(value).ok()),
                ));
            }
            "contributors" => {
                let arguments = &field.arguments;
                contributors.push((
                    arguments.string("revisionRange").map(str::to_owned),
                    arguments
                        .string("afterDate")
                        .filter(|value| !value.is_empty())
                        .map(str::to_owned),
                    arguments.string("path").map(str::to_owned),
                    arguments.i64("first"),
                    arguments.i64("last"),
                    arguments.string("after").map(str::to_owned),
                    arguments.string("before").map(str::to_owned),
                ));
            }
            _ => {}
        }
        collect_object_selections(&field.selections, refs, contributors);
        for (_, nested) in &field.type_conditions {
            collect_object_selections(nested, refs, contributors);
        }
    }
}

async fn prefetch_repo_metadata(
    store: &dyn RepoMetadataStore,
    fields: &[RootField],
    actor: Actor,
    roots: &mut InventoryDRoots,
) {
    for field in fields.iter().filter(|field| field.name == "repoMeta") {
        for keys in field
            .selections
            .iter()
            .filter(|selection| selection.name == "keys")
        {
            let args = RepoMetadataArgs::from_arguments(&keys.arguments);
            if roots.repo_meta_keys.contains_key(&args) {
                continue;
            }
            let lookup = match check_repo_metadata_write(store, actor).await {
                Err(message) => RepoMetadataConnectionLookup::Error(message),
                Ok(()) => match store.list_keys(args.query.as_deref()).await {
                    Err(message) => RepoMetadataConnectionLookup::Found(
                        RepoMetadataConnectionData {
                            total_count: Err(message.clone()),
                            page: Err(message),
                        },
                    ),
                    Ok(full) => RepoMetadataConnectionLookup::Found(RepoMetadataConnectionData {
                        total_count: Ok(full.len() as i64),
                        page: paginate_strings(&full, &args, "RepositoryMetadataKeyCursor"),
                    }),
                },
            };
            roots.repo_meta_keys.insert(args, lookup);
        }
        for key_field in field
            .selections
            .iter()
            .filter(|selection| selection.name == "key")
        {
            let Some(key) = key_field.arguments.string("key") else {
                continue;
            };
            for values in key_field
                .selections
                .iter()
                .filter(|selection| selection.name == "values")
            {
                let args = RepoMetadataArgs::from_arguments(&values.arguments);
                let cache_key = (key.to_owned(), args.clone());
                if roots.repo_meta_values.contains_key(&cache_key) {
                    continue;
                }
                let lookup = match check_repo_metadata_write(store, actor).await {
                    Err(message) => RepoMetadataConnectionLookup::Error(message),
                    Ok(()) => match store.list_values(key, args.query.as_deref()).await {
                        Err(message) => RepoMetadataConnectionLookup::Found(
                            RepoMetadataConnectionData {
                                total_count: Err(message.clone()),
                                page: Err(message),
                            },
                        ),
                        Ok(full) => {
                            RepoMetadataConnectionLookup::Found(RepoMetadataConnectionData {
                                total_count: Ok(full.len() as i64),
                                page: paginate_strings(
                                    &full,
                                    &args,
                                    "RepositoryMetadataValueCursor",
                                ),
                            })
                        }
                    },
                };
                roots.repo_meta_values.insert(cache_key, lookup);
            }
        }
    }
}

/// Build the `RepoMetadata` root for `Query.repoMeta`.
pub fn repo_metadata_object(roots: &InventoryDRoots) -> RepoMetadataObject {
    let mut values: HashMap<String, HashMap<RepoMetadataArgs, RepoMetadataConnectionLookup>> =
        HashMap::new();
    for ((key, args), lookup) in &roots.repo_meta_values {
        values
            .entry(key.clone())
            .or_default()
            .insert(args.clone(), lookup.clone());
    }
    RepoMetadataObject {
        keys: roots.repo_meta_keys.clone(),
        values,
    }
}

/// `Webhook.webhookLogs` for every selected webhook node.
#[allow(clippy::too_many_arguments)]
pub async fn prefetch_webhook_webhook_logs(
    fields: &[RootField],
    webhooks: &HashMap<
        String,
        Result<crate::graphql_inventory_c::WebhookConnectionData, String>,
    >,
    logs: &dyn crate::webhook_logs::WebhookLogConnectionStore,
    external_services: &dyn crate::external_services::ExternalServiceConnectionStore,
    users: Option<&dyn crate::graphql_resolvers::UserLookupStore>,
    webhook_key: Option<Arc<dyn sg_encryption::Key>>,
    external_service_key: Option<Arc<dyn sg_encryption::Key>>,
    external_url: &str,
    actor: Actor,
    viewer_site_admin: bool,
) -> HashMap<(i32, crate::webhook_logs::WebhookLogsArgs), crate::webhook_logs::WebhookLogsConnectionLookup>
{
    let mut prefetched: HashMap<
        (i32, crate::webhook_logs::WebhookLogsArgs),
        crate::webhook_logs::WebhookLogsConnectionLookup,
    > = HashMap::new();
    for field in fields.iter().filter(|field| field.name == "webhooks") {
        let key = crate::graphql_inventory_c::webhooks_key(&field.arguments);
        let Some(Ok(data)) = webhooks.get(&key) else {
            continue;
        };
        let Some(nodes_selection) = field
            .selections
            .iter()
            .find(|selection| selection.name == "nodes")
        else {
            continue;
        };
        let log_selections: Vec<&RootField> = nodes_selection
            .selections
            .iter()
            .flat_map(|selection| {
                let mut out: Vec<&RootField> = selection
                    .selections
                    .iter()
                    .filter(|nested| nested.name == "webhookLogs")
                    .collect();
                for (_, nested) in &selection.type_conditions {
                    out.extend(nested.iter().filter(|nested| nested.name == "webhookLogs"));
                }
                out
            })
            .collect();
        if log_selections.is_empty() {
            continue;
        }
        for webhook in &data.nodes {
            for log_selection in &log_selections {
                let mut args = crate::webhook_logs::webhook_logs_args(&log_selection.arguments);
                args.webhook_id = Some(crate::graphql_inventory_c::marshal_relay_int_id(
                    "Webhook",
                    i64::from(webhook.id),
                ));
                let cache_key = (webhook.id, args.clone());
                if prefetched.contains_key(&cache_key) {
                    continue;
                }
                let lookup = crate::webhook_logs::webhook_logs(
                    &args,
                    crate::webhook_logs::ALL_EXTERNAL_SERVICES,
                    logs,
                    external_services,
                    users,
                    webhook_key.clone(),
                    external_service_key.clone(),
                    external_url,
                    actor,
                    viewer_site_admin,
                )
                .await;
                prefetched.insert(cache_key, lookup);
            }
        }
    }
    prefetched
}
