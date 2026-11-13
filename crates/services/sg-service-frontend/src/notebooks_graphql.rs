#![forbid(unsafe_code)]

//! Notebooks GraphQL surface (M9.29a).
//!
//! Ports the baseline frontend resolvers:
//!
//! - `cmd/frontend/internal/notebooks/resolvers/{resolvers,stars_resolvers,
//!   permissions}.go`
//! - `cmd/frontend/graphqlbackend/notebooks.go` (input types)
//! - `internal/notebooks/{store,types,validate,conf}.go`
//!
//! Surface: `Query.notebooks(first, after, query, creatorUserID,
//! starredByUserID, namespace, orderBy, descending)`, `Query.notebookByID` /
//! `Query.node(Notebook)`, the `Notebook`/`NotebookBlock` fields, the star
//! connection, and the `create/update/delete/star/unstar` mutations.
//!
//! Storage is injected through the synchronous [`NotebooksGraphqlDatabase`]
//! seam (production adapter: [`crate::notebooks_store::PgNotebooksStore`]).
//! Only the existing `notebooks`/`notebook_stars` tables are used.

use std::sync::Arc;

use base64::Engine as _;
use serde::{Deserialize, Serialize};
use sg_graphql::exec::{Actor, Arguments, ExecError, FieldValue, InputValue, ObjectValue};
use sg_graphql::Json;
use time::OffsetDateTime;

use crate::graphql_resolvers::{
    datetime_rfc3339, unmarshal_relay_id, OrgData, OrgObject, PageInfoObject, UserData, UserObject,
};

pub type NotebooksHandle = Arc<dyn NotebooksGraphqlDatabase + Send + Sync>;

/// Wrapper so [`crate::graphql_resolvers::NestedPrefetch`] can keep its
/// `PartialEq`/`Eq` derive (handles compare by pointer).
#[derive(Clone)]
pub struct NotebooksHandleWrapper(pub NotebooksHandle);

impl std::fmt::Debug for NotebooksHandleWrapper {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("NotebooksHandle")
    }
}

impl PartialEq for NotebooksHandleWrapper {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}

impl Eq for NotebooksHandleWrapper {}

// ---------------------------------------------------------------------------
// Rows
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct TextInput {
    pub text: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct LineRange {
    pub start_line: i32,
    pub end_line: i32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct FileBlockInput {
    pub repository_name: String,
    pub file_path: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub revision: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub line_range: Option<LineRange>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct SymbolBlockInput {
    pub repository_name: String,
    pub file_path: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub revision: Option<String>,
    pub line_context: i32,
    pub symbol_name: String,
    pub symbol_container_name: String,
    pub symbol_kind: String,
}

/// One jsonb `notebooks.blocks` element (`notebooks.NotebookBlock`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct NotebookBlock {
    pub id: String,
    #[serde(rename = "type")]
    pub block_type: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub query_input: Option<TextInput>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub markdown_input: Option<TextInput>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub file_input: Option<FileBlockInput>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub symbol_input: Option<SymbolBlockInput>,
}

/// Internal block type discriminators (`internal/notebooks/types.go`).
pub const BLOCK_MARKDOWN: &str = "md";
pub const BLOCK_QUERY: &str = "query";
pub const BLOCK_FILE: &str = "file";
pub const BLOCK_SYMBOL: &str = "symbol";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NotebookData {
    pub id: i64,
    pub title: String,
    pub blocks: Vec<NotebookBlock>,
    pub public: bool,
    pub creator_user_id: i32,
    pub updater_user_id: i32,
    pub namespace_user_id: i32,
    pub namespace_org_id: i32,
    pub created_at: OffsetDateTime,
    pub updated_at: OffsetDateTime,
    pub pattern_type: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NotebookStarData {
    pub notebook_id: i64,
    pub user_id: i32,
    pub created_at: OffsetDateTime,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NotebooksOrderBy {
    Id,
    UpdatedAt,
    CreatedAt,
    StarCount,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ListNotebooksPageOptions {
    pub first: i32,
    pub after: i64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ListNotebookStarsPageOptions {
    pub first: i32,
    pub after: i64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ListNotebooksOptions {
    pub query: String,
    pub creator_user_id: i32,
    pub starred_by_user_id: i32,
    pub namespace_user_id: i32,
    pub namespace_org_id: i32,
    pub order_by: Option<NotebooksOrderBy>,
    pub order_by_descending: bool,
}

// ---------------------------------------------------------------------------
// Database seam
// ---------------------------------------------------------------------------

/// Synchronous seam over the notebooks store plus the user/org lookups the
/// resolvers perform. Implementations bridge to sqlx with `block_in_place`.
pub trait NotebooksGraphqlDatabase: Send + Sync {
    fn get_notebook(&self, actor: Actor, id: i64) -> Result<Option<NotebookData>, String>;
    fn create_notebook(&self, notebook: &NotebookData) -> Result<NotebookData, String>;
    fn update_notebook(&self, notebook: &NotebookData) -> Result<NotebookData, String>;
    fn delete_notebook(&self, id: i64) -> Result<(), String>;
    fn list_notebooks(
        &self,
        actor: Actor,
        page: &ListNotebooksPageOptions,
        opts: &ListNotebooksOptions,
    ) -> Result<Vec<NotebookData>, String>;
    fn count_notebooks(
        &self,
        actor: Actor,
        opts: &ListNotebooksOptions,
    ) -> Result<i64, String>;

    fn get_notebook_star(
        &self,
        notebook_id: i64,
        user_id: i32,
    ) -> Result<Option<NotebookStarData>, String>;
    fn create_notebook_star(
        &self,
        notebook_id: i64,
        user_id: i32,
    ) -> Result<NotebookStarData, String>;
    fn delete_notebook_star(&self, notebook_id: i64, user_id: i32) -> Result<(), String>;
    fn list_notebook_stars(
        &self,
        page: &ListNotebookStarsPageOptions,
        notebook_id: i64,
    ) -> Result<Vec<NotebookStarData>, String>;
    fn count_notebook_stars(&self, notebook_id: i64) -> Result<i64, String>;

    /// `Users().GetByID` (`None` = not found / soft-deleted).
    fn user_by_id(&self, id: i32) -> Result<Option<UserData>, String>;
    /// `Orgs().GetByID` plus the viewer's membership flags. `None` = not found.
    fn org_by_id(
        &self,
        id: i32,
        viewer_uid: i32,
        viewer_site_admin: bool,
    ) -> Result<Option<OrgData>, String>;
    /// `OrgMembers().GetByOrgIDAndUserID` (any membership row).
    fn is_org_member(&self, org_id: i32, user_id: i32) -> Result<bool, String>;
    /// `Users().GetByCurrentAuthUser`: `Ok(None)` is `ErrNoCurrentUser`
    /// ("no current user"); `Err` is any lookup failure.
    fn current_user(&self, actor: Actor) -> Result<Option<UserData>, String>;

    /// `dotcom.SourcegraphDotComMode()`; false for this deployment.
    fn is_dotcom(&self) -> bool {
        false
    }
}

// ---------------------------------------------------------------------------
// Relay IDs / cursors
// ---------------------------------------------------------------------------

pub const NOTEBOOK_KIND: &str = "Notebook";
pub const ERR_NOTEBOOK_STAR_NOT_FOUND: &str = "notebook star not found";
pub const ERR_NO_CURRENT_USER: &str = "no current user";

/// `relay.MarshalID("Notebook", id)`.
pub fn marshal_notebook_id(id: i64) -> String {
    base64::engine::general_purpose::URL_SAFE.encode(format!("Notebook:{id}"))
}

/// `relay.MarshalID("NotebookCursor", cursor)` for the offset cursor.
pub fn marshal_notebook_cursor(cursor: i64) -> String {
    base64::engine::general_purpose::URL_SAFE.encode(format!("NotebookCursor:{cursor}"))
}

/// `relay.MarshalID("NotebookStarCursor", cursor)`.
pub fn marshal_notebook_star_cursor(cursor: i64) -> String {
    base64::engine::general_purpose::URL_SAFE.encode(format!("NotebookStarCursor:{cursor}"))
}

/// `encoding/base64.CorruptInputError`: Go words decode failures as
/// `illegal base64 data at input byte N`.
fn go_base64_error(error: &base64::DecodeError) -> String {
    match error {
        base64::DecodeError::InvalidByte(index, _)
        | base64::DecodeError::InvalidLastSymbol(index, _) => {
            format!("illegal base64 data at input byte {index}")
        }
        other => other.to_string(),
    }
}

/// `relay.UnmarshalSpec` into an `int64`.
fn unmarshal_spec_i64(id: &str) -> Result<i64, String> {
    let decoded = base64::engine::general_purpose::URL_SAFE
        .decode(id)
        .map_err(|error| go_base64_error(&error))?;
    let text = String::from_utf8_lossy(&decoded);
    let Some((_, spec)) = text.split_once(':') else {
        return Err("invalid graphql.ID".to_owned());
    };
    serde_json::from_str(spec).map_err(|error| error.to_string())
}

/// `unmarshalNotebookID`: kind must be `Notebook`, then an int64 spec.
pub fn unmarshal_notebook_id(id: &str) -> Result<i64, String> {
    let kind = crate::graphql_resolvers::relay_kind(id).unwrap_or_default();
    if kind != NOTEBOOK_KIND {
        return Err(format!(
            "expected graphql ID to have kind {NOTEBOOK_KIND:?}; got {kind:?}"
        ));
    }
    unmarshal_spec_i64(id)
}

/// `unmarshalNotebookCursor`: no kind validation, `None` -> 0.
fn unmarshal_notebook_cursor(cursor: Option<&str>) -> Result<i64, String> {
    match cursor {
        None => Ok(0),
        Some(cursor) => unmarshal_spec_i64(cursor),
    }
}

fn unmarshal_notebook_star_cursor(cursor: Option<&str>) -> Result<i64, String> {
    match cursor {
        None => Ok(0),
        Some(cursor) => unmarshal_spec_i64(cursor),
    }
}

/// `graphqlbackend.UnmarshalUserID`: `relay.UnmarshalSpec` into an int32
/// (the kind is not validated).
fn unmarshal_user_id(id: &str) -> Result<i32, String> {
    unmarshal_relay_id(id)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NamespaceIds {
    pub user_id: i32,
    pub org_id: i32,
}

/// `graphqlbackend.UnmarshalNamespaceID`: `User`/`Org` kind, otherwise the
/// verbatim `invalid ID "<id>" for namespace` error.
pub fn unmarshal_namespace_id(id: &str) -> Result<NamespaceIds, String> {
    let kind = crate::graphql_resolvers::relay_kind(id).unwrap_or_default();
    match kind.as_str() {
        "User" => Ok(NamespaceIds {
            user_id: unmarshal_relay_id(id)?,
            org_id: 0,
        }),
        "Org" => Ok(NamespaceIds {
            user_id: 0,
            org_id: unmarshal_relay_id(id)?,
        }),
        _ => Err(format!("invalid ID {id:?} for namespace")),
    }
}

// ---------------------------------------------------------------------------
// Argument parsing / conversion
// ---------------------------------------------------------------------------

fn object_arg<'a>(arguments: &'a Arguments, name: &str) -> Option<&'a std::collections::BTreeMap<String, InputValue>> {
    match arguments.get(name) {
        Some(InputValue::Object(map)) => Some(map),
        _ => None,
    }
}

fn input_string(map: &std::collections::BTreeMap<String, InputValue>, name: &str) -> Option<String> {
    match map.get(name) {
        Some(InputValue::Str(value)) | Some(InputValue::Enum(value)) => Some(value.clone()),
        _ => None,
    }
}

fn input_bool(map: &std::collections::BTreeMap<String, InputValue>, name: &str) -> Option<bool> {
    match map.get(name) {
        Some(InputValue::Bool(value)) => Some(*value),
        _ => None,
    }
}

fn input_int(map: &std::collections::BTreeMap<String, InputValue>, name: &str) -> Option<i32> {
    match map.get(name) {
        Some(InputValue::Int(value)) => Some(*value as i32),
        _ => None,
    }
}

fn input_object<'a>(
    map: &'a std::collections::BTreeMap<String, InputValue>,
    name: &str,
) -> Option<&'a std::collections::BTreeMap<String, InputValue>> {
    match map.get(name) {
        Some(InputValue::Object(value)) => Some(value),
        _ => None,
    }
}

fn convert_line_range(
    input: Option<&std::collections::BTreeMap<String, InputValue>>,
) -> Option<LineRange> {
    let input = input?;
    Some(LineRange {
        start_line: input_int(input, "startLine").unwrap_or_default(),
        end_line: input_int(input, "endLine").unwrap_or_default(),
    })
}

/// `convertNotebookBlockInput`.
fn convert_notebook_block_input(
    input: &std::collections::BTreeMap<String, InputValue>,
) -> Result<NotebookBlock, String> {
    let id = input_string(input, "id").unwrap_or_default();
    let block_type = input_string(input, "type").unwrap_or_default();
    let mut block = NotebookBlock {
        id: id.clone(),
        ..NotebookBlock::default()
    };
    match block_type.as_str() {
        "MARKDOWN" => {
            let Some(text) = input_string(input, "markdownInput") else {
                return Err(format!("markdown block with id {id} is missing input"));
            };
            block.block_type = BLOCK_MARKDOWN.to_owned();
            block.markdown_input = Some(TextInput { text });
        }
        "QUERY" => {
            let Some(text) = input_string(input, "queryInput") else {
                return Err(format!("query block with id {id} is missing input"));
            };
            block.block_type = BLOCK_QUERY.to_owned();
            block.query_input = Some(TextInput { text });
        }
        "FILE" => {
            let Some(file) = input_object(input, "fileInput") else {
                return Err(format!("file block with id {id} is missing input"));
            };
            block.block_type = BLOCK_FILE.to_owned();
            block.file_input = Some(FileBlockInput {
                repository_name: input_string(file, "repositoryName").unwrap_or_default(),
                file_path: input_string(file, "filePath").unwrap_or_default(),
                revision: input_string(file, "revision"),
                line_range: convert_line_range(input_object(file, "lineRange")),
            });
        }
        "SYMBOL" => {
            let Some(symbol) = input_object(input, "symbolInput") else {
                return Err(format!("symbol block with id {id} is missing input"));
            };
            block.block_type = BLOCK_SYMBOL.to_owned();
            block.symbol_input = Some(SymbolBlockInput {
                repository_name: input_string(symbol, "repositoryName").unwrap_or_default(),
                file_path: input_string(symbol, "filePath").unwrap_or_default(),
                revision: input_string(symbol, "revision"),
                line_context: input_int(symbol, "lineContext").unwrap_or_default(),
                symbol_name: input_string(symbol, "symbolName").unwrap_or_default(),
                symbol_container_name: input_string(symbol, "symbolContainerName")
                    .unwrap_or_default(),
                symbol_kind: input_string(symbol, "symbolKind").unwrap_or_default(),
            });
        }
        other => return Err(format!("invalid block type: {other}")),
    }
    Ok(block)
}

/// The parsed `NotebookInput` plus the converted blocks.
struct NotebookInput {
    title: String,
    blocks: Vec<NotebookBlock>,
    public: bool,
    namespace: String,
}

fn parse_notebook_input(arguments: &Arguments) -> Result<NotebookInput, String> {
    let Some(input) = object_arg(arguments, "notebook") else {
        return Err("notebook input is required".to_owned());
    };
    let title = input_string(input, "title").unwrap_or_default();
    let public = input_bool(input, "public").unwrap_or(false);
    let namespace = input_string(input, "namespace").unwrap_or_default();
    let mut blocks = Vec::new();
    if let Some(InputValue::List(items)) = input.get("blocks") {
        for item in items {
            let InputValue::Object(block) = item else {
                continue;
            };
            blocks.push(convert_notebook_block_input(block)?);
        }
    }
    Ok(NotebookInput {
        title,
        blocks,
        public,
        namespace,
    })
}

// ---------------------------------------------------------------------------
// Validation
// ---------------------------------------------------------------------------

/// `validateNotebookBlock`.
fn validate_notebook_block(block: &NotebookBlock) -> Result<(), String> {
    if block.block_type != BLOCK_QUERY
        && block.block_type != BLOCK_MARKDOWN
        && block.block_type != BLOCK_FILE
        && block.block_type != BLOCK_SYMBOL
    {
        return Err(format!("invalid block type: {}", block.block_type));
    }
    if block.block_type == BLOCK_QUERY && block.query_input.is_none() {
        return Err(format!("invalid query block with id: {}", block.id));
    } else if block.block_type == BLOCK_MARKDOWN && block.markdown_input.is_none() {
        return Err(format!("invalid markdown block with id: {}", block.id));
    } else if block.block_type == BLOCK_FILE && block.file_input.is_none() {
        return Err(format!("invalid file block with id: {}", block.id));
    } else if block.block_type == BLOCK_SYMBOL && block.symbol_input.is_none() {
        return Err(format!("invalid symbol block with id: {}", block.id));
    }
    if block.block_type == BLOCK_SYMBOL {
        if let Some(symbol) = &block.symbol_input {
            if symbol.line_context < 0 {
                return Err(format!(
                    "symbol block line context cannot be negative, block id: {}",
                    block.id
                ));
            }
        }
    }
    Ok(())
}

/// `validateNotebookBlocks`.
pub fn validate_notebook_blocks(blocks: &[NotebookBlock]) -> Result<(), String> {
    let mut block_ids = std::collections::HashSet::new();
    for block in blocks {
        validate_notebook_block(block)?;
        if !block_ids.insert(block.id.clone()) {
            return Err(format!("duplicate block id found: {}", block.id));
        }
    }
    Ok(())
}

/// `validateNotebookWritePermissionsForUser`.
pub fn validate_notebook_write_permissions_for_user(
    db: &dyn NotebooksGraphqlDatabase,
    notebook: &NotebookData,
    user_id: i32,
) -> Result<(), String> {
    if notebook.namespace_user_id != 0 && notebook.namespace_user_id != user_id {
        return Err("user does not match the notebook user namespace".to_owned());
    } else if notebook.namespace_org_id != 0 {
        if !db.is_org_member(notebook.namespace_org_id, user_id)? {
            return Err(
                "user is not a member of the notebook organization namespace".to_owned(),
            );
        }
    } else if notebook.namespace_user_id == 0 && notebook.namespace_org_id == 0 {
        return Err("cannot update notebook without a namespace".to_owned());
    }
    Ok(())
}

/// `Users().GetByCurrentAuthUser`, mapping `ErrNoCurrentUser` to
/// `Ok(None)`.
fn current_user(
    db: &dyn NotebooksGraphqlDatabase,
    actor: Actor,
) -> Result<Option<UserData>, String> {
    db.current_user(actor)
}

// ---------------------------------------------------------------------------
// Object resolvers
// ---------------------------------------------------------------------------

struct NotebookConnectionObject {
    notebooks: Vec<Arc<NotebookObject>>,
    total_count: i32,
    has_next_page: bool,
    end_cursor: Option<String>,
}

impl ObjectValue for NotebookConnectionObject {
    fn type_name(&self) -> &'static str {
        "NotebookConnection"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "nodes" => Ok(Some(FieldValue::List {
                element_type: Some("Notebook"),
                items: self
                    .notebooks
                    .iter()
                    .map(|notebook| FieldValue::Object(notebook.clone()))
                    .collect(),
            })),
            "totalCount" => Ok(Some(FieldValue::Json(Json::Int(self.total_count as i64)))),
            "pageInfo" => Ok(Some(FieldValue::Object(Arc::new(
                PageInfoObject::new(self.has_next_page, self.end_cursor.clone()),
            )))),
            _ => Ok(None),
        }
    }
}

struct NotebookStarConnectionObject {
    stars: Vec<Arc<NotebookStarObject>>,
    total_count: i32,
    has_next_page: bool,
    end_cursor: Option<String>,
}

impl ObjectValue for NotebookStarConnectionObject {
    fn type_name(&self) -> &'static str {
        "NotebookStarConnection"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "nodes" => Ok(Some(FieldValue::List {
                element_type: Some("NotebookStar"),
                items: self
                    .stars
                    .iter()
                    .map(|star| FieldValue::Object(star.clone()))
                    .collect(),
            })),
            "totalCount" => Ok(Some(FieldValue::Json(Json::Int(self.total_count as i64)))),
            "pageInfo" => Ok(Some(FieldValue::Object(Arc::new(
                PageInfoObject::new(self.has_next_page, self.end_cursor.clone()),
            )))),
            _ => Ok(None),
        }
    }
}

struct NotebookStarObject {
    db: NotebooksHandle,
    star: NotebookStarData,
}

impl ObjectValue for NotebookStarObject {
    fn type_name(&self) -> &'static str {
        "NotebookStar"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            // `notebookStarResolver.User`: `UserByIDInt32` (errors propagate).
            "user" => {
                let user = self
                    .db
                    .user_by_id(self.star.user_id)
                    .map_err(ExecError::new)?;
                Ok(Some(match user {
                    Some(user) => FieldValue::Object(Arc::new(UserObject::from_data(user))),
                    None => FieldValue::Null,
                }))
            }
            "createdAt" => Ok(Some(FieldValue::Json(Json::Str(datetime_rfc3339(
                self.star.created_at,
            ))))),
            _ => Ok(None),
        }
    }
}

struct NotebookObject {
    db: NotebooksHandle,
    notebook: NotebookData,
    viewer_site_admin: bool,
}

impl NotebookObject {
    fn new(
        db: NotebooksHandle,
        notebook: NotebookData,
        viewer_site_admin: bool,
    ) -> Self {
        Self {
            db,
            notebook,
            viewer_site_admin,
        }
    }
}

impl ObjectValue for NotebookObject {
    fn type_name(&self) -> &'static str {
        "Notebook"
    }

    fn field(
        &self,
        field: &str,
        arguments: &Arguments,
        actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "id" => Ok(Some(FieldValue::Json(Json::Str(marshal_notebook_id(
                self.notebook.id,
            ))))),
            "title" => Ok(Some(FieldValue::Json(Json::Str(
                self.notebook.title.clone(),
            )))),
            "blocks" => Ok(Some(FieldValue::List {
                element_type: Some("NotebookBlock"),
                items: self
                    .notebook
                    .blocks
                    .iter()
                    .cloned()
                    .map(|block| FieldValue::Object(Arc::new(NotebookBlockObject { block })))
                    .collect(),
            })),
            "creator" => {
                // `UserByIDInt32`; soft-deleted/unknown -> null.
                if self.notebook.creator_user_id == 0 {
                    return Ok(Some(FieldValue::Null));
                }
                let user = self
                    .db
                    .user_by_id(self.notebook.creator_user_id)
                    .map_err(ExecError::new)?;
                Ok(Some(match user {
                    Some(user) => FieldValue::Object(Arc::new(UserObject::from_data(user))),
                    None => FieldValue::Null,
                }))
            }
            "updater" => {
                if self.notebook.updater_user_id == 0 {
                    return Ok(Some(FieldValue::Null));
                }
                let user = self
                    .db
                    .user_by_id(self.notebook.updater_user_id)
                    .map_err(ExecError::new)?;
                Ok(Some(match user {
                    Some(user) => FieldValue::Object(Arc::new(UserObject::from_data(user))),
                    None => FieldValue::Null,
                }))
            }
            "namespace" => self.namespace_field(actor),
            "public" => Ok(Some(FieldValue::Json(Json::Bool(self.notebook.public)))),
            "updatedAt" => Ok(Some(FieldValue::Json(Json::Str(datetime_rfc3339(
                self.notebook.updated_at,
            ))))),
            "createdAt" => Ok(Some(FieldValue::Json(Json::Str(datetime_rfc3339(
                self.notebook.created_at,
            ))))),
            "viewerCanManage" => {
                let user = current_user(self.db.as_ref(), actor).map_err(ExecError::new)?;
                let Some(user) = user else {
                    return Ok(Some(FieldValue::Json(Json::Bool(false))));
                };
                let can_manage =
                    validate_notebook_write_permissions_for_user(
                        self.db.as_ref(),
                        &self.notebook,
                        user.id,
                    )
                    .is_ok();
                Ok(Some(FieldValue::Json(Json::Bool(can_manage))))
            }
            "viewerHasStarred" => {
                let user = current_user(self.db.as_ref(), actor).map_err(ExecError::new)?;
                let Some(user) = user else {
                    return Ok(Some(FieldValue::Json(Json::Bool(false))));
                };
                let star = self
                    .db
                    .get_notebook_star(self.notebook.id, user.id)
                    .map_err(ExecError::new)?;
                Ok(Some(FieldValue::Json(Json::Bool(star.is_some()))))
            }
            "stars" => self.stars_field(arguments),
            "patternType" => Ok(Some(FieldValue::Json(Json::Str(
                self.notebook.pattern_type.clone(),
            )))),
            _ => Ok(None),
        }
    }
}

impl NotebookObject {
    fn namespace_field(
        &self,
        actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        if self.notebook.namespace_user_id != 0 {
            let user = self
                .db
                .user_by_id(self.notebook.namespace_user_id)
                .map_err(ExecError::new)?;
            return Ok(Some(match user {
                Some(user) => FieldValue::Object(Arc::new(UserObject::from_data(user))),
                None => FieldValue::Null,
            }));
        }
        if self.notebook.namespace_org_id != 0 {
            let viewer_uid = actor.uid().unwrap_or(0);
            let org = self
                .db
                .org_by_id(self.notebook.namespace_org_id, viewer_uid, self.viewer_site_admin)
                .map_err(ExecError::new)?;
            return Ok(Some(match org {
                Some(org) => FieldValue::Object(Arc::new(OrgObject::from_data_as_viewer(
                    org,
                    self.viewer_site_admin,
                ))),
                None => {
                    if self.db.is_dotcom() {
                        FieldValue::Null
                    } else {
                        return Err(ExecError::new(format!(
                            "org not found: id {}",
                            self.notebook.namespace_org_id
                        )));
                    }
                }
            }));
        }
        Ok(Some(FieldValue::Null))
    }

    fn stars_field(&self, arguments: &Arguments) -> Result<Option<FieldValue>, ExecError> {
        let first = arguments.i64("first").unwrap_or(50) as i32;
        let after = unmarshal_notebook_star_cursor(arguments.string("after"))
            .map_err(ExecError::new)?;
        let page = ListNotebookStarsPageOptions {
            first: first + 1,
            after,
        };
        let mut stars = self
            .db
            .list_notebook_stars(&page, self.notebook.id)
            .map_err(ExecError::new)?;
        let count = self
            .db
            .count_notebook_stars(self.notebook.id)
            .map_err(ExecError::new)?;
        let has_next_page = stars.len() == first as usize + 1;
        if has_next_page {
            stars.truncate(first as usize);
        }
        // `PageInfo` reports no next page when the truncated page is empty.
        let has_next_page = has_next_page && !stars.is_empty();
        let end_cursor = if stars.is_empty() || !has_next_page {
            None
        } else {
            Some(marshal_notebook_star_cursor(
                after + stars.len() as i64,
            ))
        };
        Ok(Some(FieldValue::Object(Arc::new(
            NotebookStarConnectionObject {
                stars: stars
                    .into_iter()
                    .map(|star| {
                        Arc::new(NotebookStarObject {
                            db: self.db.clone(),
                            star,
                        })
                    })
                    .collect(),
                total_count: count as i32,
                has_next_page,
                end_cursor,
            },
        ))))
    }
}

struct NotebookBlockObject {
    block: NotebookBlock,
}

impl ObjectValue for NotebookBlockObject {
    fn type_name(&self) -> &'static str {
        match self.block.block_type.as_str() {
            BLOCK_QUERY => "QueryBlock",
            BLOCK_FILE => "FileBlock",
            BLOCK_SYMBOL => "SymbolBlock",
            _ => "MarkdownBlock",
        }
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match (self.block.block_type.as_str(), field) {
            (BLOCK_MARKDOWN, "id") => Ok(Some(FieldValue::Json(Json::Str(
                self.block.id.clone(),
            )))),
            (BLOCK_MARKDOWN, "markdownInput") => Ok(Some(FieldValue::Json(Json::Str(
                self.block
                    .markdown_input
                    .as_ref()
                    .map(|input| input.text.clone())
                    .unwrap_or_default(),
            )))),
            (BLOCK_QUERY, "id") => Ok(Some(FieldValue::Json(Json::Str(
                self.block.id.clone(),
            )))),
            (BLOCK_QUERY, "queryInput") => Ok(Some(FieldValue::Json(Json::Str(
                self.block
                    .query_input
                    .as_ref()
                    .map(|input| input.text.clone())
                    .unwrap_or_default(),
            )))),
            (BLOCK_FILE, "id") => Ok(Some(FieldValue::Json(Json::Str(
                self.block.id.clone(),
            )))),
            (BLOCK_FILE, "fileInput") => Ok(Some(match &self.block.file_input {
                Some(input) => FieldValue::Object(Arc::new(FileBlockInputObject {
                    input: input.clone(),
                })),
                None => FieldValue::Null,
            })),
            (BLOCK_SYMBOL, "id") => Ok(Some(FieldValue::Json(Json::Str(
                self.block.id.clone(),
            )))),
            (BLOCK_SYMBOL, "symbolInput") => Ok(Some(match &self.block.symbol_input {
                Some(input) => FieldValue::Object(Arc::new(SymbolBlockInputObject {
                    input: input.clone(),
                })),
                None => FieldValue::Null,
            })),
            _ => Ok(None),
        }
    }
}

struct FileBlockInputObject {
    input: FileBlockInput,
}

impl ObjectValue for FileBlockInputObject {
    fn type_name(&self) -> &'static str {
        "FileBlockInput"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "repositoryName" => Ok(Some(FieldValue::Json(Json::Str(
                self.input.repository_name.clone(),
            )))),
            "filePath" => Ok(Some(FieldValue::Json(Json::Str(
                self.input.file_path.clone(),
            )))),
            "revision" => Ok(Some(match &self.input.revision {
                Some(revision) => FieldValue::Json(Json::Str(revision.clone())),
                None => FieldValue::Null,
            })),
            "lineRange" => Ok(Some(match &self.input.line_range {
                Some(range) => FieldValue::Object(Arc::new(FileBlockLineRangeObject {
                    range: range.clone(),
                })),
                None => FieldValue::Null,
            })),
            _ => Ok(None),
        }
    }
}

struct FileBlockLineRangeObject {
    range: LineRange,
}

impl ObjectValue for FileBlockLineRangeObject {
    fn type_name(&self) -> &'static str {
        "FileBlockLineRange"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "startLine" => Ok(Some(FieldValue::Json(Json::Int(
                self.range.start_line as i64,
            )))),
            "endLine" => Ok(Some(FieldValue::Json(Json::Int(
                self.range.end_line as i64,
            )))),
            _ => Ok(None),
        }
    }
}

struct SymbolBlockInputObject {
    input: SymbolBlockInput,
}

impl ObjectValue for SymbolBlockInputObject {
    fn type_name(&self) -> &'static str {
        "SymbolBlockInput"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "repositoryName" => Ok(Some(FieldValue::Json(Json::Str(
                self.input.repository_name.clone(),
            )))),
            "filePath" => Ok(Some(FieldValue::Json(Json::Str(
                self.input.file_path.clone(),
            )))),
            "revision" => Ok(Some(match &self.input.revision {
                Some(revision) => FieldValue::Json(Json::Str(revision.clone())),
                None => FieldValue::Null,
            })),
            "lineContext" => Ok(Some(FieldValue::Json(Json::Int(
                self.input.line_context as i64,
            )))),
            "symbolName" => Ok(Some(FieldValue::Json(Json::Str(
                self.input.symbol_name.clone(),
            )))),
            "symbolContainerName" => Ok(Some(FieldValue::Json(Json::Str(
                self.input.symbol_container_name.clone(),
            )))),
            "symbolKind" => Ok(Some(FieldValue::Json(Json::Str(
                self.input.symbol_kind.clone(),
            )))),
            _ => Ok(None),
        }
    }
}

// ---------------------------------------------------------------------------
// Root / mutation / node dispatch
// ---------------------------------------------------------------------------

pub fn is_root_field(field: &str) -> bool {
    matches!(field, "notebooks" | "notebookByID")
}

pub fn root_field(
    database: &NotebooksHandle,
    field: &str,
    arguments: &Arguments,
    actor: Actor,
) -> Result<Option<FieldValue>, ExecError> {
    match field {
        "notebooks" => Ok(Some(notebooks(database, arguments, actor)?)),
        "notebookByID" => {
            let id = arguments.string("id").unwrap_or_default();
            notebook_by_id(database, id, actor)
        }
        _ => Ok(None),
    }
}

pub fn is_mutation_field(field: &str) -> bool {
    matches!(
        field,
        "createNotebook"
            | "updateNotebook"
            | "deleteNotebook"
            | "createNotebookStar"
            | "deleteNotebookStar"
    )
}

pub fn mutation_field(
    database: &NotebooksHandle,
    field: &str,
    arguments: &Arguments,
    actor: Actor,
) -> Result<Option<FieldValue>, ExecError> {
    match field {
        "createNotebook" => Ok(Some(create_notebook(database, arguments, actor)?)),
        "updateNotebook" => Ok(Some(update_notebook(database, arguments, actor)?)),
        "deleteNotebook" => Ok(Some(delete_notebook(database, arguments, actor)?)),
        "createNotebookStar" => Ok(Some(create_notebook_star(
            database, arguments, actor,
        )?)),
        "deleteNotebookStar" => Ok(Some(delete_notebook_star(
            database, arguments, actor,
        )?)),
        _ => Ok(None),
    }
}

/// `Query.node(Notebook)` (the executor dispatches by Relay kind).
pub fn node_field(
    database: &NotebooksHandle,
    id: &str,
    actor: Actor,
) -> Result<Option<FieldValue>, ExecError> {
    notebook_by_id(database, id, actor)
}

// ---------------------------------------------------------------------------
// Query resolvers
// ---------------------------------------------------------------------------

pub fn notebook_by_id(
    database: &NotebooksHandle,
    id: &str,
    actor: Actor,
) -> Result<Option<FieldValue>, ExecError> {
    let notebook_id = unmarshal_notebook_id(id).map_err(ExecError::new)?;
    let notebook = database
        .get_notebook(actor, notebook_id)
        .map_err(ExecError::new)?;
    Ok(notebook.map(|notebook| {
        FieldValue::Object(Arc::new(NotebookObject::new(
            database.clone(),
            notebook,
            false,
        )))
    }))
}

pub fn notebooks(
    database: &NotebooksHandle,
    arguments: &Arguments,
    actor: Actor,
) -> Result<FieldValue, ExecError> {
    let first = arguments.i64("first").unwrap_or(50) as i32;
    let order_by = match arguments.string("orderBy") {
        Some("NOTEBOOK_CREATED_AT") => NotebooksOrderBy::CreatedAt,
        Some("NOTEBOOK_STAR_COUNT") => NotebooksOrderBy::StarCount,
        _ => NotebooksOrderBy::UpdatedAt,
    };
    let after = unmarshal_notebook_cursor(arguments.string("after"))
        .map_err(ExecError::new)?;
    let mut creator_user_id = 0;
    if let Some(id) = arguments.string("creatorUserID") {
        creator_user_id = unmarshal_user_id(id).map_err(ExecError::new)?;
    }
    let mut starred_by_user_id = 0;
    if let Some(id) = arguments.string("starredByUserID") {
        starred_by_user_id = unmarshal_user_id(id).map_err(ExecError::new)?;
    }
    let query = arguments.string("query").unwrap_or_default().to_owned();
    let mut opts = ListNotebooksOptions {
        query,
        creator_user_id,
        starred_by_user_id,
        order_by: Some(order_by),
        order_by_descending: arguments.bool("descending").unwrap_or(false),
        ..ListNotebooksOptions::default()
    };
    if let Some(namespace) = arguments.string("namespace") {
        let ids = unmarshal_namespace_id(namespace).map_err(ExecError::new)?;
        opts.namespace_user_id = ids.user_id;
        opts.namespace_org_id = ids.org_id;
    }

    let page = ListNotebooksPageOptions {
        first: first + 1,
        after,
    };
    let notebooks = database
        .list_notebooks(actor, &page, &opts)
        .map_err(ExecError::new)?;
    let count = database
        .count_notebooks(actor, &opts)
        .map_err(ExecError::new)?;

    let mut notebooks = notebooks;
    let has_next_page = notebooks.len() == first as usize + 1;
    if has_next_page {
        notebooks.truncate(first as usize);
    }
    // `PageInfo` reports no next page when the truncated page is empty.
    let has_next_page = has_next_page && !notebooks.is_empty();
    let end_cursor = if notebooks.is_empty() || !has_next_page {
        None
    } else {
        Some(marshal_notebook_cursor(after + notebooks.len() as i64))
    };
    Ok(FieldValue::Object(Arc::new(NotebookConnectionObject {
        notebooks: notebooks
            .into_iter()
            .map(|notebook| {
                Arc::new(NotebookObject::new(database.clone(), notebook, false))
            })
            .collect(),
        total_count: count as i32,
        has_next_page,
        end_cursor,
    })))
}

// ---------------------------------------------------------------------------
// Mutations
// ---------------------------------------------------------------------------

pub fn create_notebook(
    database: &NotebooksHandle,
    arguments: &Arguments,
    actor: Actor,
) -> Result<FieldValue, ExecError> {
    let user = match current_user(database.as_ref(), actor).map_err(ExecError::new)? {
        Some(user) => user,
        None => return Err(ExecError::new(ERR_NO_CURRENT_USER)),
    };
    let input = parse_notebook_input(arguments).map_err(ExecError::new)?;
    let mut notebook = NotebookData {
        id: 0,
        title: input.title,
        blocks: input.blocks,
        public: input.public,
        creator_user_id: user.id,
        updater_user_id: user.id,
        namespace_user_id: 0,
        namespace_org_id: 0,
        created_at: OffsetDateTime::UNIX_EPOCH,
        updated_at: OffsetDateTime::UNIX_EPOCH,
        pattern_type: String::new(),
    };
    let namespace = unmarshal_namespace_id(&input.namespace).map_err(ExecError::new)?;
    notebook.namespace_user_id = namespace.user_id;
    notebook.namespace_org_id = namespace.org_id;
    validate_notebook_write_permissions_for_user(
        database.as_ref(),
        &notebook,
        user.id,
    )
    .map_err(ExecError::new)?;
    validate_notebook_blocks(&notebook.blocks).map_err(ExecError::new)?;
    let created = database
        .create_notebook(&notebook)
        .map_err(ExecError::new)?;
    Ok(FieldValue::Object(Arc::new(NotebookObject::new(
        database.clone(),
        created,
        false,
    ))))
}

pub fn update_notebook(
    database: &NotebooksHandle,
    arguments: &Arguments,
    actor: Actor,
) -> Result<FieldValue, ExecError> {
    let user = match current_user(database.as_ref(), actor).map_err(ExecError::new)? {
        Some(user) => user,
        None => return Err(ExecError::new(ERR_NO_CURRENT_USER)),
    };
    let id = arguments.string("id").unwrap_or_default();
    let notebook_id = unmarshal_notebook_id(id).map_err(ExecError::new)?;
    let mut notebook = database
        .get_notebook(actor, notebook_id)
        .map_err(ExecError::new)?
        .ok_or_else(|| ExecError::new("notebook not found"))?;
    validate_notebook_write_permissions_for_user(
        database.as_ref(),
        &notebook,
        user.id,
    )
    .map_err(ExecError::new)?;

    let input = parse_notebook_input(arguments).map_err(ExecError::new)?;
    notebook.title = input.title;
    notebook.public = input.public;
    notebook.blocks = input.blocks;
    notebook.updater_user_id = user.id;
    let namespace = unmarshal_namespace_id(&input.namespace).map_err(ExecError::new)?;
    notebook.namespace_user_id = namespace.user_id;
    notebook.namespace_org_id = namespace.org_id;
    // The current user needs write access to both the old and new namespace.
    validate_notebook_write_permissions_for_user(
        database.as_ref(),
        &notebook,
        user.id,
    )
    .map_err(ExecError::new)?;
    validate_notebook_blocks(&notebook.blocks).map_err(ExecError::new)?;
    let updated = database
        .update_notebook(&notebook)
        .map_err(ExecError::new)?;
    Ok(FieldValue::Object(Arc::new(NotebookObject::new(
        database.clone(),
        updated,
        false,
    ))))
}

pub fn delete_notebook(
    database: &NotebooksHandle,
    arguments: &Arguments,
    actor: Actor,
) -> Result<FieldValue, ExecError> {
    let user = match current_user(database.as_ref(), actor).map_err(ExecError::new)? {
        Some(user) => user,
        None => return Err(ExecError::new(ERR_NO_CURRENT_USER)),
    };
    let id = arguments.string("id").unwrap_or_default();
    let notebook_id = unmarshal_notebook_id(id).map_err(ExecError::new)?;
    let notebook = database
        .get_notebook(actor, notebook_id)
        .map_err(ExecError::new)?
        .ok_or_else(|| ExecError::new("notebook not found"))?;
    validate_notebook_write_permissions_for_user(
        database.as_ref(),
        &notebook,
        user.id,
    )
    .map_err(ExecError::new)?;
    database
        .delete_notebook(notebook_id)
        .map_err(ExecError::new)?;
    Ok(FieldValue::Object(Arc::new(
        crate::temporary_settings::EmptyResponseObject,
    )))
}

pub fn create_notebook_star(
    database: &NotebooksHandle,
    arguments: &Arguments,
    actor: Actor,
) -> Result<FieldValue, ExecError> {
    let user = match current_user(database.as_ref(), actor).map_err(ExecError::new)? {
        Some(user) => user,
        None => return Err(ExecError::new(ERR_NO_CURRENT_USER)),
    };
    let id = arguments.string("notebookID").unwrap_or_default();
    let notebook_id = unmarshal_notebook_id(id).map_err(ExecError::new)?;
    let notebook = database
        .get_notebook(actor, notebook_id)
        .map_err(ExecError::new)?
        .ok_or_else(|| ExecError::new("notebook not found"))?;
    let star = database
        .create_notebook_star(notebook.id, user.id)
        .map_err(ExecError::new)?;
    Ok(FieldValue::Object(Arc::new(NotebookStarObject {
        db: database.clone(),
        star,
    })))
}

pub fn delete_notebook_star(
    database: &NotebooksHandle,
    arguments: &Arguments,
    actor: Actor,
) -> Result<FieldValue, ExecError> {
    let user = match current_user(database.as_ref(), actor).map_err(ExecError::new)? {
        Some(user) => user,
        None => return Err(ExecError::new(ERR_NO_CURRENT_USER)),
    };
    let id = arguments.string("notebookID").unwrap_or_default();
    let notebook_id = unmarshal_notebook_id(id).map_err(ExecError::new)?;
    let notebook = database
        .get_notebook(actor, notebook_id)
        .map_err(ExecError::new)?
        .ok_or_else(|| ExecError::new("notebook not found"))?;
    database
        .delete_notebook_star(notebook.id, user.id)
        .map_err(ExecError::new)?;
    Ok(FieldValue::Object(Arc::new(
        crate::temporary_settings::EmptyResponseObject,
    )))
}
