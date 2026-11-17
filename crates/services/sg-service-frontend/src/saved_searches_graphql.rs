#![forbid(unsafe_code)]

//! Saved-search GraphQL surface (M9.13a).
//!
//! Ports `cmd/frontend/internal/savedsearches/resolvers/{resolvers,connection}.go`
//! plus `cmd/frontend/graphqlbackend/saved_searches.go`:
//!
//! - `Query.savedSearches` with the `gqlutil.ConnectionResolver` pagination
//!   math (default/max page size 100, `first`/`last`, composite cursors);
//! - `Query.node` kind `SavedSearch`;
//! - the five mutations (`createSavedSearch`, `updateSavedSearch`,
//!   `deleteSavedSearch`, `transferSavedSearchOwnership`,
//!   `changeSavedSearchVisibility`);
//! - the `SavedSearch` node fields.
//!
//! Authorization and error texts are copied from `internal/auth` and
//! `graphqlbackend/namespaces.go`; see the overview for the exact anchors.
//! The database is injected through [`SavedSearchesGraphqlDatabase`], a
//! synchronous seam in the same style as the search-context resolver: the
//! production adapter bridges to sqlx with
//! `tokio::task::block_in_place` + `Handle::block_on`.
//!
//! No table, column or Redis key is added: every operation reuses the existing
//! `saved_searches`, `users`, `orgs` and `org_members` tables.

use std::sync::{Arc, OnceLock};

use base64::Engine as _;
use serde::{Deserialize, Serialize};
use sg_graphql::exec::{Actor, Arguments, ExecError, FieldValue, ObjectValue};
use sg_graphql::json::Json;
use time::OffsetDateTime;

use crate::graphql_resolvers::{
    datetime_rfc3339, relay_kind, unmarshal_relay_id, ConnectionPageInfoObject, OrgData, OrgObject,
    UserData, UserObject, MUST_BE_SITE_ADMIN_OR_SAME_USER,
};

/// `resolvers.SavedSearchKind`.
pub const SAVED_SEARCH_KIND: &str = "SavedSearch";

/// `connection.go:18` `cursorKind`.
pub const SAVED_SEARCH_CURSOR_KIND: &str = "SavedSearchCursor";

/// `resolvers.go:381` `errMissingPatternType`.
pub const ERR_MISSING_PATTERN_TYPE: &str = "a `patternType:` filter is required in the query for all saved searches. `patternType` can be \"keyword\", \"standard\", \"literal\", or \"regexp\"";

/// `gqlutil.DefaultMaxPageSize`.
pub const DEFAULT_MAX_PAGE_SIZE: i64 = 100;

// ---------------------------------------------------------------------------
// Rows and arguments
// ---------------------------------------------------------------------------

/// One `saved_searches` row (`internal/types/saved_searches.go:6`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SavedSearchRow {
    pub id: i32,
    pub description: String,
    pub query: String,
    pub draft: bool,
    pub user_id: Option<i32>,
    pub org_id: Option<i32>,
    pub visibility_secret: bool,
    pub created_by_user: Option<i32>,
    pub created_at: OffsetDateTime,
    pub updated_by_user: Option<i32>,
    pub updated_at: OffsetDateTime,
    pub notify_owner: bool,
    pub notify_slack: bool,
    pub slack_webhook_url: Option<String>,
}

impl SavedSearchRow {
    /// `types.Namespace` as the row stores it. `None` for both is the
    /// baseline's "no owner" error state.
    pub fn owner(&self) -> NamespaceIds {
        NamespaceIds {
            user: self.user_id,
            org: self.org_id,
        }
    }
}

/// `types.Namespace` (`{User *int32, Org *int32}`).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct NamespaceIds {
    pub user: Option<i32>,
    pub org: Option<i32>,
}

impl NamespaceIds {
    /// The baseline's `CheckAuthorizedForNamespaceByIDs` rejects a namespace
    /// with neither id (`namespaces.go:104-112`).
    pub fn is_empty(&self) -> bool {
        self.user.is_none() && self.org.is_none()
    }
}

/// `database.SavedSearchListArgs`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SavedSearchListArgs {
    pub query: String,
    pub affiliated_user: Option<i32>,
    pub public_only: bool,
    pub owner: Option<NamespaceIds>,
    pub hide_drafts: bool,
}

/// `database.SavedSearchesOrderBy`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SavedSearchesOrderBy {
    UpdatedAt,
    Description,
}

impl SavedSearchesOrderBy {
    /// `ToOptions`.
    pub fn ascending(self) -> bool {
        matches!(self, Self::Description)
    }

    pub fn column(self) -> &'static str {
        match self {
            Self::UpdatedAt => "updated_at",
            Self::Description => "description",
        }
    }
}

/// One entry of `types.MultiCursor` (`internal/types/cursor.go:10-14`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct CursorEntry {
    c: String,
    v: String,
    d: String,
}

/// The values extracted by `UnmarshalValuesFromCursor`, in cursor order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SavedSearchCursorValue {
    Id(i32),
    Description(String),
    UpdatedAt(i64),
}

/// `database.SavedSearchStore.MarshalToCursor`.
pub fn marshal_saved_search_cursor(
    row: &SavedSearchRow,
    order: SavedSearchesOrderBy,
) -> String {
    let entry = match order {
        SavedSearchesOrderBy::Description => CursorEntry {
            c: "description".to_owned(),
            v: row.description.clone(),
            d: String::new(),
        },
        SavedSearchesOrderBy::UpdatedAt => CursorEntry {
            c: "updated_at".to_owned(),
            v: row.updated_at.unix_timestamp_nanos().to_string(),
            d: String::new(),
        },
    };
    let json = serde_json::to_string(&vec![entry]).expect("cursor JSON");
    let json = crate::oauth_login::go_json_escape(&json);
    base64::engine::general_purpose::URL_SAFE.encode(format!("{SAVED_SEARCH_CURSOR_KIND}:{json}"))
}

/// `connection.go:22-32` `unmarshalCursor`.
pub fn unmarshal_saved_search_cursor(
    cursor: &str,
) -> Result<Vec<SavedSearchCursorValue>, String> {
    if relay_kind(cursor).as_deref() != Some(SAVED_SEARCH_CURSOR_KIND) {
        return Err(format!(
            "invalid cursor type {:?} (expected {:?})",
            relay_kind(cursor).unwrap_or_default(),
            SAVED_SEARCH_CURSOR_KIND
        ));
    }
    let decoded = base64::engine::general_purpose::URL_SAFE
        .decode(cursor)
        .map_err(|error| error.to_string())?;
    let text = String::from_utf8_lossy(&decoded);
    let Some((_, spec)) = text.split_once(':') else {
        return Err("invalid graphql.ID".to_owned());
    };
    let entries: Vec<CursorEntry> =
        serde_json::from_str(spec).map_err(|error| error.to_string())?;
    entries
        .into_iter()
        .map(|entry| match entry.c.as_str() {
            "id" => entry.v.parse::<i32>().map(SavedSearchCursorValue::Id).map_err(|error| {
                format!("ParseInt(id): {error}")
            }),
            "description" => Ok(SavedSearchCursorValue::Description(entry.v)),
            "updated_at" => entry
                .v
                .parse::<i64>()
                .map(SavedSearchCursorValue::UpdatedAt)
                .map_err(|error| format!("ParseInt(updated_at): {error}")),
            _ => Err("unexpected orderBy column".to_owned()),
        })
        .collect()
}

/// The values bound for one cursor in the SQL comparison.
pub type CursorValues = Vec<SavedSearchCursorValue>;

// ---------------------------------------------------------------------------
// Database seam
// ---------------------------------------------------------------------------

/// Synchronous `database.SavedSearchStore` seam. `Ok(None)` from
/// [`Self::get_saved_search`] is the baseline `saved search not found` /
/// `errcode.IsNotFound` state; calls that the baseline lets bubble up as an
/// error use `Err("saved search not found")`.
pub trait SavedSearchesGraphqlDatabase: Send + Sync {
    fn get_saved_search(&self, id: i32) -> Result<Option<SavedSearchRow>, String>;

    /// `List`. `first`/`last` are the already-incremented connection probe and
    /// are mutually exclusive; with `last` the store must order in the
    /// opposite direction (the caller reverses the result).
    #[allow(clippy::too_many_arguments)]
    fn list_saved_searches(
        &self,
        args: &SavedSearchListArgs,
        order: SavedSearchesOrderBy,
        after: Option<&CursorValues>,
        before: Option<&CursorValues>,
        first: Option<i64>,
        last: Option<i64>,
    ) -> Result<Vec<SavedSearchRow>, String>;

    fn count_saved_searches(&self, args: &SavedSearchListArgs) -> Result<i32, String>;

    fn create_saved_search(
        &self,
        description: &str,
        query: &str,
        draft: bool,
        owner: NamespaceIds,
        visibility_secret: bool,
        actor_uid: i32,
        notify_owner: bool,
        notify_slack: bool,
        slack_webhook_url: Option<&str>,
    ) -> Result<SavedSearchRow, String>;

    fn update_saved_search(
        &self,
        id: i32,
        description: &str,
        query: &str,
        draft: bool,
        actor_uid: i32,
        notify_owner: bool,
        notify_slack: bool,
        slack_webhook_url: Option<&str>,
    ) -> Result<SavedSearchRow, String>;

    fn update_saved_search_owner(
        &self,
        id: i32,
        owner: NamespaceIds,
        actor_uid: i32,
    ) -> Result<SavedSearchRow, String>;

    fn update_saved_search_visibility(
        &self,
        id: i32,
        visibility_secret: bool,
        actor_uid: i32,
    ) -> Result<SavedSearchRow, String>;

    fn delete_saved_search(&self, id: i32) -> Result<(), String>;

    /// `db.Users().GetByID` (live users only).
    fn user_by_id(&self, id: i32) -> Result<Option<UserData>, String>;
    /// `db.Orgs().GetByID` (live organizations only).
    fn org_by_id(&self, id: i32) -> Result<Option<OrgData>, String>;
    /// `auth.CurrentUser`: `None` for anonymous, internal and missing users.
    fn current_user(&self, uid: i32) -> Result<Option<UserData>, String>;
    /// `users.site_admin`.
    fn is_site_admin(&self, uid: i32) -> Result<bool, String>;
    /// `org_members.GetByOrgIDAndUserID`; `Ok(false)` is the not-found case.
    fn is_org_member(&self, org_id: i32, user_id: i32) -> Result<bool, String>;
}

/// The injected handle.
pub type SavedSearchesHandle = Arc<dyn SavedSearchesGraphqlDatabase + Send + Sync>;

/// A handle usable from [`crate::graphql_resolvers::NestedPrefetch`]'s derives
/// (`Debug`/`PartialEq`/`Eq` by pointer).
#[derive(Clone)]
pub struct SavedSearchesHandleWrapper(pub SavedSearchesHandle);

impl std::fmt::Debug for SavedSearchesHandleWrapper {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("SavedSearchesHandle")
    }
}

impl PartialEq for SavedSearchesHandleWrapper {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}

impl Eq for SavedSearchesHandleWrapper {}

// ---------------------------------------------------------------------------
// Authorization (`internal/auth`, `graphqlbackend/namespaces.go`)
// ---------------------------------------------------------------------------

/// `auth.CheckCurrentUserIsSiteAdmin` (`site_admin.go:16-32`).
fn check_current_user_is_site_admin(
    db: &dyn SavedSearchesGraphqlDatabase,
    actor: Actor,
) -> Result<(), ExecError> {
    if actor.is_internal() {
        return Ok(());
    }
    let Some(uid) = actor.uid() else {
        return Err(ExecError::new("not authenticated"));
    };
    let user = db
        .current_user(uid)
        .map_err(ExecError::new)?
        .ok_or_else(|| ExecError::new("not authenticated"))?;
    if !user.site_admin {
        return Err(ExecError::new("must be site admin"));
    }
    Ok(())
}

/// `auth.CheckSiteAdminOrSameUser` (`site_admin.go:105-136`).
fn check_site_admin_or_same_user(
    db: &dyn SavedSearchesGraphqlDatabase,
    actor: Actor,
    subject_user_id: i32,
) -> Result<(), ExecError> {
    if actor.is_internal() || actor.uid() == Some(subject_user_id) {
        return Ok(());
    }
    match check_current_user_is_site_admin(db, actor) {
        Ok(()) => Ok(()),
        Err(_) => Err(ExecError::new(MUST_BE_SITE_ADMIN_OR_SAME_USER)),
    }
}

/// `auth.CheckOrgAccessOrSiteAdmin` (`orgs.go:16-48`).
fn check_org_access_or_site_admin(
    db: &dyn SavedSearchesGraphqlDatabase,
    actor: Actor,
    org_id: i32,
) -> Result<(), ExecError> {
    if actor.is_internal() {
        return Ok(());
    }
    let current = match actor.uid() {
        Some(uid) => db
            .current_user(uid)
            .map_err(ExecError::new)?
            .ok_or_else(|| ExecError::new("not authenticated"))?,
        None => return Err(ExecError::new("not authenticated")),
    };
    if current.site_admin {
        return Ok(());
    }
    match db
        .is_org_member(org_id, current.id)
        .map_err(ExecError::new)?
    {
        true => Ok(()),
        false => Err(ExecError::new("current user is not an org member")),
    }
}

/// `CheckAuthorizedForNamespaceByIDs` (`namespaces.go:93-112`).
pub fn check_authorized_for_namespace_by_ids(
    db: &dyn SavedSearchesGraphqlDatabase,
    actor: Actor,
    namespace: NamespaceIds,
) -> Result<(), ExecError> {
    if let Some(user) = namespace.user {
        return check_site_admin_or_same_user(db, actor, user);
    }
    if let Some(org) = namespace.org {
        return check_org_access_or_site_admin(db, actor, org);
    }
    Err(ExecError::new(
        "namespace is required (User or Organization ID)",
    ))
}

/// `InvalidNamespaceIDErr.Error()` (`namespaces.go:36-40`).
fn invalid_namespace_id(id: &str) -> String {
    format!("invalid ID \"{id}\" for namespace")
}

/// `UnmarshalNamespaceToIDs` (`namespaces.go:53-73`): a User/Org kind with a
/// JSON `null` spec leaves both ids nil and is reported as an invalid id.
pub fn unmarshal_namespace_to_ids(id: &str) -> Result<NamespaceIds, String> {
    let kind = relay_kind(id).ok_or_else(|| invalid_namespace_id(id))?;
    let decoded = base64::engine::general_purpose::URL_SAFE
        .decode(id)
        .map_err(|error| error.to_string())?;
    let text = String::from_utf8_lossy(&decoded);
    let Some((_, spec)) = text.split_once(':') else {
        return Err(invalid_namespace_id(id));
    };
    let value: serde_json::Value = serde_json::from_str(spec).map_err(|error| error.to_string())?;
    let parsed = match value {
        serde_json::Value::Null => None,
        serde_json::Value::Number(number) => number.as_i64().map(|n| n as i32),
        _ => return Err(invalid_namespace_id(id)),
    };
    let namespace = match kind.as_str() {
        "User" => NamespaceIds {
            user: parsed,
            org: None,
        },
        "Org" => NamespaceIds {
            user: None,
            org: parsed,
        },
        _ => return Err(invalid_namespace_id(id)),
    };
    if namespace.is_empty() {
        return Err(invalid_namespace_id(id));
    }
    Ok(namespace)
}

/// `CheckAuthorizedForNamespace` (`namespaces.go:75-91`).
pub fn check_authorized_for_namespace(
    db: &dyn SavedSearchesGraphqlDatabase,
    actor: Actor,
    id: &str,
) -> Result<NamespaceIds, ExecError> {
    let namespace = unmarshal_namespace_to_ids(id).map_err(ExecError::new)?;
    check_authorized_for_namespace_by_ids(db, actor, namespace)?;
    Ok(namespace)
}

// ---------------------------------------------------------------------------
// Pattern-type check
// ---------------------------------------------------------------------------

/// `resolvers.go:380-384` `queryHasPatternType`
/// (`(?i)\bpatternType:(literal|regexp|structural|standard|keyword)\b`).
pub fn query_has_pattern_type(query: &str) -> bool {
    let lower = query.to_ascii_lowercase();
    for (index, _) in lower.match_indices("patterntype:") {
        let before_ok = index == 0
            || !lower.as_bytes()[index - 1].is_ascii_alphanumeric()
                && lower.as_bytes()[index - 1] != b'_';
        if !before_ok {
            continue;
        }
        let rest = &lower[index + "patterntype:".len()..];
        for value in ["literal", "regexp", "structural", "standard", "keyword"] {
            if let Some(after) = rest.strip_prefix(value) {
                let after_ok = after
                    .chars()
                    .next()
                    .map(|c| !c.is_ascii_alphanumeric() && c != '_')
                    .unwrap_or(true);
                // `\b` before the keyword is guaranteed by the `:` prefix.
                if after_ok {
                    return true;
                }
            }
        }
    }
    false
}

// ---------------------------------------------------------------------------
// SavedSearch object
// ---------------------------------------------------------------------------

/// `savedSearchResolver` (`resolvers.go:60-166`).
pub struct SavedSearchObject {
    pub row: SavedSearchRow,
    pub db: SavedSearchesHandle,
}

impl SavedSearchObject {
    pub fn new(row: SavedSearchRow, db: SavedSearchesHandle) -> Self {
        Self { row, db }
    }

    /// `relay.MarshalID("SavedSearch", id)`.
    pub fn relay_id(id: i32) -> String {
        base64::engine::general_purpose::URL_SAFE.encode(format!("{SAVED_SEARCH_KIND}:{id}"))
    }

    fn user_object(&self, id: i32, actor: Actor) -> Result<FieldValue, ExecError> {
        let user = self
            .db
            .user_by_id(id)
            .map_err(ExecError::new)?
            .ok_or_else(|| ExecError::new(format!("user not found: userID {id}")))?;
        let viewer_site_admin = match actor.uid() {
            Some(uid) => self.db.is_site_admin(uid).unwrap_or(false),
            None => false,
        };
        Ok(FieldValue::Object(Arc::new(
            UserObject::from_data_as_viewer(user, viewer_site_admin),
        )))
    }

    fn org_object(
        &self,
        id: i32,
        _actor: Actor,
    ) -> Result<FieldValue, ExecError> {
        let org = self
            .db
            .org_by_id(id)
            .map_err(ExecError::new)?
            .ok_or_else(|| ExecError::new(format!("org not found: {id}")))?;
        Ok(FieldValue::Object(Arc::new(OrgObject::from_data(org))))
    }
}

impl ObjectValue for SavedSearchObject {
    fn type_name(&self) -> &'static str {
        "SavedSearch"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "id" => Ok(Some(FieldValue::Json(Json::Str(Self::relay_id(
                self.row.id,
            ))))),
            "description" => Ok(Some(FieldValue::Json(Json::Str(
                self.row.description.clone(),
            )))),
            "query" => Ok(Some(FieldValue::Json(Json::Str(self.row.query.clone())))),
            "draft" => Ok(Some(FieldValue::Json(Json::Bool(self.row.draft)))),
            "notify" => Ok(Some(FieldValue::Json(Json::Bool(self.row.notify_owner)))),
            "notifySlack" => Ok(Some(FieldValue::Json(Json::Bool(self.row.notify_slack)))),
            "slackWebhookURL" => Ok(Some(match &self.row.slack_webhook_url {
                Some(url) => FieldValue::Json(Json::Str(url.clone())),
                None => FieldValue::Null,
            })),
            "namespace" => {
                if let Some(user) = self.row.user_id {
                    return self.user_object(user, actor).map(Some);
                }
                if let Some(org) = self.row.org_id {
                    return self.org_object(org, actor).map(Some);
                }
                Err(ExecError::new("no owner"))
            }
            "visibility" => Ok(Some(FieldValue::Json(Json::Str(
                if self.row.visibility_secret {
                    "SECRET".to_owned()
                } else {
                    "PUBLIC".to_owned()
                },
            )))),
            "owner" => {
                if let Some(user) = self.row.user_id {
                    return self.user_object(user, actor).map(Some);
                }
                if let Some(org) = self.row.org_id {
                    return self.org_object(org, actor).map(Some);
                }
                Err(ExecError::new("no owner"))
            }
            "createdBy" => match self.row.created_by_user {
                Some(user) => self.user_object(user, actor).map(Some),
                None => Ok(Some(FieldValue::Null)),
            },
            "updatedBy" => match self.row.updated_by_user {
                Some(user) => self.user_object(user, actor).map(Some),
                None => Ok(Some(FieldValue::Null)),
            },
            "createdAt" => Ok(Some(FieldValue::Json(Json::Str(datetime_rfc3339(
                self.row.created_at,
            ))))),
            "updatedAt" => Ok(Some(FieldValue::Json(Json::Str(datetime_rfc3339(
                self.row.updated_at,
            ))))),
            // `"/saved-searches/" + string(r.ID())`.
            "url" => Ok(Some(FieldValue::Json(Json::Str(format!(
                "/saved-searches/{}",
                Self::relay_id(self.row.id)
            ))))),
            "viewerCanAdminister" => {
                let allowed = check_authorized_for_namespace_by_ids(
                    self.db.as_ref(),
                    actor,
                    self.row.owner(),
                )
                .is_ok();
                Ok(Some(FieldValue::Json(Json::Bool(allowed))))
            }
            _ => Ok(None),
        }
    }
}

/// `savedSearchResolver.Owner` also accepts an org, where `OrgData` needs the
/// viewer flags computed by the adapter. This helper is used by the adapter.
pub fn org_data_with_viewer(
    record: OrgData,
    actor: Actor,
    db: &dyn SavedSearchesGraphqlDatabase,
) -> Result<OrgData, String> {
    let (is_admin, is_member) = match actor.uid() {
        Some(uid) => {
            let admin = db.is_site_admin(uid)?;
            let member = if admin {
                true
            } else {
                db.is_org_member(record.id, uid)?
            };
            (admin, member)
        }
        None => (false, false),
    };
    Ok(OrgData {
        viewer_can_administer: is_admin || is_member,
        viewer_is_member: is_member,
        ..record
    })
}

// ---------------------------------------------------------------------------
// Connection
// ---------------------------------------------------------------------------

/// Arguments shared by the connection resolver (`graphqlbackend.SavedSearchesArgs`).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SavedSearchConnectionArgs {
    pub first: Option<i64>,
    pub last: Option<i64>,
    pub after: Option<String>,
    pub before: Option<String>,
}

/// `SavedSearchConnectionResolver = gqlutil.ConnectionResolver[SavedSearchResolver]`.
pub struct SavedSearchesConnectionObject {
    db: SavedSearchesHandle,
    list_args: SavedSearchListArgs,
    order: SavedSearchesOrderBy,
    args: SavedSearchConnectionArgs,
    total: OnceLock<Result<i32, String>>,
    /// Rows fetched from the store, before the +1 probe is trimmed.
    fetched: OnceLock<Result<Vec<SavedSearchRow>, String>>,
}

impl SavedSearchesConnectionObject {
    pub fn new(
        db: SavedSearchesHandle,
        list_args: SavedSearchListArgs,
        order: SavedSearchesOrderBy,
        args: SavedSearchConnectionArgs,
    ) -> Self {
        Self {
            db,
            list_args,
            order,
            args,
            total: OnceLock::new(),
            fetched: OnceLock::new(),
        }
    }

    fn page_size(&self) -> i64 {
        let limit = self.args.first.or(self.args.last).unwrap_or(0);
        if limit == 0 {
            DEFAULT_MAX_PAGE_SIZE
        } else {
            limit.min(DEFAULT_MAX_PAGE_SIZE)
        }
    }

    /// `gqlutil.ConnectionResolver.pageSize`.
    fn total_count(&self) -> Result<i32, String> {
        self.total
            .get_or_init(|| self.db.count_saved_searches(&self.list_args))
            .clone()
    }

    fn fetched(&self) -> Result<&Vec<SavedSearchRow>, String> {
        let result = self.fetched.get_or_init(|| self.fetch());
        match result {
            Ok(rows) => Ok(rows),
            Err(message) => Err(message.clone()),
        }
    }

    fn fetch(&self) -> Result<Vec<SavedSearchRow>, String> {
        let page_size = self.page_size();
        let limit = page_size + 1;
        let (first, last) = match (self.args.first, self.args.last) {
            (Some(_), _) => (Some(limit), None),
            (_, Some(_)) => (None, Some(limit)),
            (None, None) => {
                return Err("you must provide a `first` or a `last` value to properly paginate"
                    .to_owned())
            }
        };
        let after = match &self.args.after {
            Some(cursor) => Some(unmarshal_saved_search_cursor(cursor)?),
            None => None,
        };
        let before = match &self.args.before {
            Some(cursor) => Some(unmarshal_saved_search_cursor(cursor)?),
            None => None,
        };
        let mut rows = self.db.list_saved_searches(
            &self.list_args,
            self.order,
            after.as_ref(),
            before.as_ref(),
            first,
            last,
        )?;
        // `gqlutil.ConnectionResolver.Nodes` reverses backward pages because
        // the SQL ordered them in the opposite direction.
        if self.args.last.is_some() {
            rows.reverse();
        }
        Ok(rows)
    }

    /// `gqlutil.ConnectionResolver.Nodes` (post-trim).
    fn nodes(&self) -> Result<Vec<SavedSearchRow>, String> {
        let fetched = self.fetched()?;
        let page_size = self.page_size() as usize;
        let mut nodes = fetched.clone();
        if nodes.len() > page_size {
            if self.args.last.is_some() {
                nodes.remove(0);
            } else {
                nodes.pop();
            }
        }
        Ok(nodes)
    }

    fn end_cursor(&self) -> Result<Option<String>, String> {
        let nodes = self.nodes()?;
        Ok(nodes
            .last()
            .map(|row| marshal_saved_search_cursor(row, self.order)))
    }

    fn start_cursor(&self) -> Result<Option<String>, String> {
        let nodes = self.nodes()?;
        Ok(nodes
            .first()
            .map(|row| marshal_saved_search_cursor(row, self.order)))
    }

    fn has_next_page(&self) -> Result<bool, String> {
        let fetched = self.fetched()?.len();
        let page_size = self.page_size() as usize;
        if self.args.first.is_some() {
            return Ok(fetched > page_size);
        }
        if fetched == 0 {
            return Ok(false);
        }
        Ok(self.args.before.is_some())
    }

    fn has_previous_page(&self) -> Result<bool, String> {
        let fetched = self.fetched()?.len();
        let page_size = self.page_size() as usize;
        if self.args.last.is_some() {
            return Ok(fetched > page_size);
        }
        if fetched == 0 {
            return Ok(false);
        }
        Ok(self.args.after.is_some())
    }
}

impl ObjectValue for SavedSearchesConnectionObject {
    fn type_name(&self) -> &'static str {
        "SavedSearchesConnection"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "nodes" => {
                let nodes = self.nodes().map_err(ExecError::new)?;
                Ok(Some(FieldValue::List {
                    items: nodes
                        .into_iter()
                        .map(|row| {
                            FieldValue::Object(Arc::new(SavedSearchObject::new(
                                row,
                                Arc::clone(&self.db),
                            )))
                        })
                        .collect(),
                    element_type: Some("SavedSearch"),
                }))
            }
            "totalCount" => Ok(Some(FieldValue::Json(Json::Int(
                self.total_count().map_err(ExecError::new)? as i64,
            )))),
            "pageInfo" => {
                let start = self.start_cursor().map_err(ExecError::new)?;
                let end = self.end_cursor().map_err(ExecError::new)?;
                let has_next = self.has_next_page().map_err(ExecError::new)?;
                let has_previous = self.has_previous_page().map_err(ExecError::new)?;
                let _ = actor;
                Ok(Some(FieldValue::Object(Arc::new(
                    ConnectionPageInfoObject::new(start, end, has_next, has_previous),
                ))))
            }
            _ => Ok(None),
        }
    }
}

// ---------------------------------------------------------------------------
// Root field dispatch
// ---------------------------------------------------------------------------

fn exec_error(message: String) -> ExecError {
    ExecError::new(message)
}

fn is_secret(visibility: &str) -> bool {
    visibility != "PUBLIC"
}

/// `Query.savedSearches` (`resolvers.go:172-228`).
pub fn saved_searches(
    db: &SavedSearchesHandle,
    arguments: &Arguments,
    actor: Actor,
) -> Result<FieldValue, ExecError> {
    let mut list_args = SavedSearchListArgs {
        query: arguments.string("query").unwrap_or_default().to_owned(),
        hide_drafts: !arguments.bool("includeDrafts").unwrap_or(true),
        ..SavedSearchListArgs::default()
    };

    if let Some(owner) = arguments
        .string("owner")
        .or_else(|| arguments.string("namespace"))
    {
        let namespace = check_authorized_for_namespace(db.as_ref(), actor, owner)?;
        list_args.owner = Some(namespace);
    }

    if arguments.bool("viewerIsAffiliated").unwrap_or(false) {
        let current = match actor.uid() {
            Some(uid) => db.current_user(uid).map_err(exec_error)?,
            None => None,
        };
        match current {
            Some(user) => list_args.affiliated_user = Some(user.id),
            None => list_args.public_only = true,
        }
    }

    if list_args.owner.is_none() && list_args.affiliated_user.is_none() && !list_args.public_only {
        check_current_user_is_site_admin(db.as_ref(), actor)
            .map_err(|error| ExecError::new(format!("must specify owner or viewerIsAffiliated args: {}", error.message)))?;
    }

    let order = match arguments.string("orderBy").unwrap_or("SAVED_SEARCH_UPDATED_AT") {
        "SAVED_SEARCH_DESCRIPTION" => SavedSearchesOrderBy::Description,
        "SAVED_SEARCH_UPDATED_AT" => SavedSearchesOrderBy::UpdatedAt,
        _ => return Err(ExecError::new("invalid orderBy")),
    };

    let connection = SavedSearchesConnectionObject::new(
        Arc::clone(db),
        list_args,
        order,
        SavedSearchConnectionArgs {
            first: arguments.i64("first"),
            last: arguments.i64("last"),
            after: arguments.string("after").map(str::to_owned),
            before: arguments.string("before").map(str::to_owned),
        },
    );
    Ok(FieldValue::Object(Arc::new(connection)))
}

/// `Query.node` kind `SavedSearch` (`SavedSearchByID`). `Ok(None)` is the
/// baseline `nil, nil` for a missing row.
pub fn saved_search_node(
    db: &SavedSearchesHandle,
    id: &str,
    actor: Actor,
) -> Result<Option<FieldValue>, ExecError> {
    let saved_search_id = unmarshal_relay_id(id).map_err(exec_error)?;
    let row = db.get_saved_search(saved_search_id).map_err(exec_error)?;
    let Some(row) = row else {
        return Ok(None);
    };
    // `checkActorCanViewSavedSearch`: public rows are visible to anyone;
    // secret rows require namespace authorization.
    if row.visibility_secret {
        check_authorized_for_namespace_by_ids(db.as_ref(), actor, row.owner())?;
    }
    Ok(Some(FieldValue::Object(Arc::new(SavedSearchObject::new(
        row,
        Arc::clone(db),
    )))))
}

fn get_existing(
    db: &dyn SavedSearchesGraphqlDatabase,
    id: i32,
) -> Result<SavedSearchRow, ExecError> {
    db.get_saved_search(id)
        .map_err(exec_error)?
        .ok_or_else(|| ExecError::new("saved search not found"))
}

/// Parse one namespace ID argument (`userID`/`orgID`): Relay ID, or a plain
/// database ID string.
fn namespace_id_argument(arguments: &Arguments, name: &str) -> Option<i32> {
    let value = arguments.string(name)?;
    unmarshal_relay_id(value)
        .or_else(|_| value.parse::<i32>().map_err(|error| error.to_string()))
        .ok()
}

/// Owner from the flat 5.5 arguments (`userID`/`orgID`), if supplied.
fn flat_owner(arguments: &Arguments) -> Option<NamespaceIds> {
    if let Some(user) = namespace_id_argument(arguments, "userID") {
        return Some(NamespaceIds {
            user: Some(user),
            org: None,
        });
    }
    namespace_id_argument(arguments, "orgID").map(|org| NamespaceIds {
        user: None,
        org: Some(org),
    })
}

/// `Mutation.createSavedSearch` (`resolvers.go:230-261`). Supports both the
/// baseline `input` argument and the flat 5.5 UI arguments.
pub fn create_saved_search(
    db: &SavedSearchesHandle,
    arguments: &Arguments,
    actor: Actor,
) -> Result<FieldValue, ExecError> {
    let input = match arguments.get("input") {
        Some(sg_graphql::exec::InputValue::Object(input)) => Some(input),
        _ => None,
    };
    let namespace = if let Some(owner) = flat_owner(arguments) {
        check_authorized_for_namespace_by_ids(db.as_ref(), actor, owner)?;
        owner
    } else if let Some(input) = input {
        let owner_id = input
            .get("owner")
            .and_then(|value| value.as_str())
            .ok_or_else(|| ExecError::new("missing owner"))?;
        check_authorized_for_namespace(db.as_ref(), actor, owner_id)?
    } else {
        return Err(ExecError::new("missing owner"));
    };
    let field = |name: &str| input.and_then(|input| input.get(name));
    let query = field("query")
        .and_then(|value| value.as_str())
        .or_else(|| arguments.string("query"))
        .unwrap_or_default();
    if !query_has_pattern_type(query) {
        return Err(ExecError::new(ERR_MISSING_PATTERN_TYPE));
    }
    let visibility = field("visibility")
        .and_then(|value| value.as_str())
        .unwrap_or("SECRET");
    if !is_secret(visibility) {
        check_current_user_is_site_admin(db.as_ref(), actor)?;
    }
    let description = field("description")
        .and_then(|value| value.as_str())
        .or_else(|| arguments.string("description"))
        .unwrap_or_default();
    let draft = field("draft")
        .and_then(|value| value.as_bool())
        .unwrap_or(false);
    let notify_owner = arguments.bool("notifyOwner").unwrap_or(false);
    let notify_slack = arguments.bool("notifySlack").unwrap_or(false);
    let slack_webhook_url = arguments.string("slackWebhookURL");
    let row = db
        .create_saved_search(
            description,
            query,
            draft,
            namespace,
            is_secret(visibility),
            actor.uid().unwrap_or(0),
            notify_owner,
            notify_slack,
            slack_webhook_url,
        )
        .map_err(exec_error)?;
    Ok(FieldValue::Object(Arc::new(SavedSearchObject::new(
        row,
        Arc::clone(db),
    ))))
}

/// `Mutation.updateSavedSearch` (`resolvers.go:263-296`). Supports both the
/// baseline `input` argument and the flat 5.5 UI arguments.
pub fn update_saved_search(
    db: &SavedSearchesHandle,
    arguments: &Arguments,
    actor: Actor,
) -> Result<FieldValue, ExecError> {
    let id = arguments
        .string("id")
        .ok_or_else(|| ExecError::new("missing id"))?;
    let saved_search_id = unmarshal_relay_id(id).map_err(exec_error)?;
    let old = db
        .get_saved_search(saved_search_id)
        .map_err(|error| ExecError::new(format!("get existing saved search: {error}")))?
        .ok_or_else(|| ExecError::new("get existing saved search: saved search not found"))?;
    check_authorized_for_namespace_by_ids(db.as_ref(), actor, old.owner())?;
    let input = match arguments.get("input") {
        Some(sg_graphql::exec::InputValue::Object(input)) => Some(input),
        _ => None,
    };
    let field = |name: &str| input.and_then(|input| input.get(name));
    let query = field("query")
        .and_then(|value| value.as_str())
        .or_else(|| arguments.string("query"))
        .unwrap_or(old.query.as_str());
    if !query_has_pattern_type(query) {
        return Err(ExecError::new(ERR_MISSING_PATTERN_TYPE));
    }
    let description = field("description")
        .and_then(|value| value.as_str())
        .or_else(|| arguments.string("description"))
        .unwrap_or(old.description.as_str());
    let draft = field("draft")
        .and_then(|value| value.as_bool())
        .unwrap_or(old.draft);
    let notify_owner = arguments
        .bool("notifyOwner")
        .unwrap_or(old.notify_owner);
    let notify_slack = arguments
        .bool("notifySlack")
        .unwrap_or(old.notify_slack);
    let slack_webhook_url = arguments
        .string("slackWebhookURL")
        .map(str::to_owned)
        .or_else(|| old.slack_webhook_url.clone());
    let row = db
        .update_saved_search(
            saved_search_id,
            description,
            query,
            draft,
            actor.uid().unwrap_or(0),
            notify_owner,
            notify_slack,
            slack_webhook_url.as_deref(),
        )
        .map_err(exec_error)?;
    // The 5.5 UI may move the saved search to another namespace.
    let row = match flat_owner(arguments) {
        Some(owner) if owner != old.owner() => {
            check_authorized_for_namespace_by_ids(db.as_ref(), actor, owner)?;
            db.update_saved_search_owner(saved_search_id, owner, actor.uid().unwrap_or(0))
                .map_err(exec_error)?
        }
        _ => row,
    };
    Ok(FieldValue::Object(Arc::new(SavedSearchObject::new(
        row,
        Arc::clone(db),
    ))))
}

/// `Mutation.transferSavedSearchOwnership` (`resolvers.go:298-327`).
pub fn transfer_saved_search_ownership(
    db: &SavedSearchesHandle,
    arguments: &Arguments,
    actor: Actor,
) -> Result<FieldValue, ExecError> {
    let id = arguments
        .string("id")
        .ok_or_else(|| ExecError::new("missing id"))?;
    let saved_search_id = unmarshal_relay_id(id).map_err(exec_error)?;
    let row = get_existing(db.as_ref(), saved_search_id)?;
    check_authorized_for_namespace_by_ids(db.as_ref(), actor, row.owner())?;
    let new_owner_id = arguments
        .string("newOwner")
        .ok_or_else(|| ExecError::new("missing newOwner"))?;
    let new_owner = check_authorized_for_namespace(db.as_ref(), actor, new_owner_id)?;
    let row = db
        .update_saved_search_owner(saved_search_id, new_owner, actor.uid().unwrap_or(0))
        .map_err(exec_error)?;
    Ok(FieldValue::Object(Arc::new(SavedSearchObject::new(
        row,
        Arc::clone(db),
    ))))
}

/// `Mutation.changeSavedSearchVisibility` (`resolvers.go:329-354`).
pub fn change_saved_search_visibility(
    db: &SavedSearchesHandle,
    arguments: &Arguments,
    actor: Actor,
) -> Result<FieldValue, ExecError> {
    check_current_user_is_site_admin(db.as_ref(), actor)?;
    let id = arguments
        .string("id")
        .ok_or_else(|| ExecError::new("missing id"))?;
    let saved_search_id = unmarshal_relay_id(id).map_err(exec_error)?;
    let row = get_existing(db.as_ref(), saved_search_id)?;
    check_authorized_for_namespace_by_ids(db.as_ref(), actor, row.owner())?;
    let visibility = arguments
        .string("newVisibility")
        .ok_or_else(|| ExecError::new("missing newVisibility"))?;
    let row = db
        .update_saved_search_visibility(
            saved_search_id,
            is_secret(visibility),
            actor.uid().unwrap_or(0),
        )
        .map_err(exec_error)?;
    Ok(FieldValue::Object(Arc::new(SavedSearchObject::new(
        row,
        Arc::clone(db),
    ))))
}

/// `Mutation.deleteSavedSearch` (`resolvers.go:356-378`).
pub fn delete_saved_search(
    db: &SavedSearchesHandle,
    arguments: &Arguments,
    actor: Actor,
) -> Result<FieldValue, ExecError> {
    let id = arguments
        .string("id")
        .ok_or_else(|| ExecError::new("missing id"))?;
    let saved_search_id = unmarshal_relay_id(id).map_err(exec_error)?;
    let row = get_existing(db.as_ref(), saved_search_id)?;
    check_authorized_for_namespace_by_ids(db.as_ref(), actor, row.owner())?;
    db.delete_saved_search(saved_search_id).map_err(exec_error)?;
    Ok(FieldValue::Object(Arc::new(
        crate::temporary_settings::EmptyResponseObject,
    )))
}

/// Dispatch one `Query`/`Mutation` root field. `Ok(None)` means the field is
/// not part of this surface.
pub fn root_field(
    db: &SavedSearchesHandle,
    field: &str,
    arguments: &Arguments,
    actor: Actor,
) -> Result<Option<FieldValue>, ExecError> {
    let value = match field {
        "savedSearches" => saved_searches(db, arguments, actor)?,
        "createSavedSearch" => create_saved_search(db, arguments, actor)?,
        "updateSavedSearch" => update_saved_search(db, arguments, actor)?,
        "deleteSavedSearch" => delete_saved_search(db, arguments, actor)?,
        "transferSavedSearchOwnership" => {
            transfer_saved_search_ownership(db, arguments, actor)?
        }
        "changeSavedSearchVisibility" => {
            change_saved_search_visibility(db, arguments, actor)?
        }
        _ => return Ok(None),
    };
    Ok(Some(value))
}

/// Every root field owned by this surface, used for dispatch gating.
pub const ROOT_FIELDS: &[&str] = &[
    "savedSearches",
    "createSavedSearch",
    "updateSavedSearch",
    "deleteSavedSearch",
    "transferSavedSearchOwnership",
    "changeSavedSearchVisibility",
];

/// Whether `field` is one of [`ROOT_FIELDS`].
pub fn is_root_field(field: &str) -> bool {
    ROOT_FIELDS.contains(&field)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pattern_type_matches_go_regexp() {
        for query in [
            "patternType:literal foo",
            "PatternType:REGEXP foo",
            "x patternType:standard",
            "patternType:structural",
            "patternType:keyword",
        ] {
            assert!(query_has_pattern_type(query), "{query}");
        }
        for query in [
            "patternType:",
            "patternType:literalx",
            "xpatternType:literal",
            "patternType:commit",
        ] {
            assert!(!query_has_pattern_type(query), "{query}");
        }
    }

    #[test]
    fn namespace_ids_round_trip() {
        let user = base64::engine::general_purpose::URL_SAFE.encode("User:7");
        assert_eq!(
            unmarshal_namespace_to_ids(&user).unwrap(),
            NamespaceIds {
                user: Some(7),
                org: None
            }
        );
        let org = base64::engine::general_purpose::URL_SAFE.encode("Org:7");
        assert_eq!(
            unmarshal_namespace_to_ids(&org).unwrap(),
            NamespaceIds {
                user: None,
                org: Some(7)
            }
        );
        let bad = base64::engine::general_purpose::URL_SAFE.encode("Repository:7");
        assert!(unmarshal_namespace_to_ids(&bad).is_err());
    }
}
