#![forbid(unsafe_code)]

//! `Query.externalServices` and the read-only `ExternalService` fields
//! (`cmd/frontend/graphqlbackend/external_services.go:368-455`,
//! `external_service.go:120-290`).
//!
//! The connection is site-admin only. It lists live `external_services`
//! (newest id first), optionally restricted to a repo, with the baseline's
//! `after`/`first` pagination and `PageInfo` logic. Node fields read the
//! `external_services` row, decrypt/redact `config`
//! (`internal/types/secret.go:39-119`) and lazily fetch `creator`,
//! `lastUpdater`, `repoCount` and `lastSyncError`.
//!
//! Not ported (see the M9.8gh overview §3.1): `url`, `webhookURL`,
//! `rateLimiterState`, `checkConnection`, `hasConnectionCheck`, `syncJobs`,
//! `webhookLogs`, and every mutation.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use base64::Engine as _;
use sg_db::external_services::{ExternalServiceListRecord, ExternalServiceStore};
use sg_encryption::{maybe_decrypt, Key};
use sg_graphql::exec::{Actor, Arguments, ExecError, FieldValue, ObjectValue, RootField};
use sg_graphql::json::Json;
use sg_jsonx::{
    apply_edits, compute_property_edit, go_json_string, parse, EditValue, FormatOptions, JsonValue,
    ParseOptions, Path, Segment,
};

use crate::graphql_resolvers::{
    datetime_rfc3339, relay_kind, PageInfoObject, UserData, UserKey, UserLookupStore, UserObject,
};

/// `types.RedactedSecret`.
const REDACTED_SECRET: &str = "REDACTED";

/// `relay.MarshalID("ExternalService", id)`.
pub fn marshal_external_service_id(id: i64) -> String {
    base64::engine::general_purpose::URL_SAFE.encode(format!("ExternalService:{id}"))
}

/// `UnmarshalExternalServiceID`: validate the Relay kind, then parse the
/// int64 spec (`external_service.go:90-99`).
pub fn unmarshal_external_service_id(raw: &str) -> Result<i64, String> {
    let kind = relay_kind(raw).unwrap_or_default();
    if kind != "ExternalService" {
        return Err(format!(
            "expected graphql ID to have kind \"ExternalService\"; got \"{kind}\""
        ));
    }
    relay_spec_i64(raw)
}

/// `UnmarshalRepositoryID` (`repository.go:142-145`): the kind is not
/// validated, only the int32 spec is parsed.
pub fn unmarshal_repository_id(raw: &str) -> Result<i32, String> {
    let value = relay_spec_i64(raw)?;
    i32::try_from(value).map_err(|_| "value out of range".to_owned())
}

fn relay_spec_i64(raw: &str) -> Result<i64, String> {
    let decoded = base64::engine::general_purpose::URL_SAFE
        .decode(raw)
        .map_err(|error| error.to_string())?;
    let text = String::from_utf8_lossy(&decoded);
    let Some((_, spec)) = text.split_once(':') else {
        return Err("invalid graphql.ID".to_owned());
    };
    serde_json::from_str(spec).map_err(|error| error.to_string())
}

/// The argument set of one `Query.externalServices` field.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ExternalServicesArgs {
    pub first: Option<i32>,
    pub after: Option<String>,
    pub repo: Option<String>,
}

/// `ExternalServicesArgs` from the executor's resolved arguments.
pub fn external_services_args(arguments: &Arguments) -> ExternalServicesArgs {
    ExternalServicesArgs {
        first: arguments
            .i64("first")
            .and_then(|value| i32::try_from(value).ok()),
        after: arguments.string("after").map(str::to_owned),
        repo: arguments.string("repo").map(str::to_owned),
    }
}

/// The database seam for `Query.externalServices` (`db.ExternalServices()`).
#[async_trait]
pub trait ExternalServiceConnectionStore: Send + Sync {
    async fn list(
        &self,
        after_id: i64,
        repo_id: i32,
        limit: Option<i64>,
    ) -> Result<Vec<ExternalServiceListRecord>, String>;

    async fn count(&self, after_id: i64, repo_id: i32) -> Result<i64, String>;

    async fn repo_count(&self, id: i64) -> Result<i32, String>;

    async fn last_sync_error(&self, id: i64) -> Result<Option<String>, String>;

    /// `db.ExternalServices().GetByID` (`ExternalService.webhookLogs` and
    /// `WebhookLog.externalService` need a single live service).
    async fn get_by_id(&self, id: i64) -> Result<ExternalServiceListRecord, String>;
}

/// Production adapter over [`sg_db::external_services::ExternalServiceStore`].
pub struct PgExternalServiceConnectionStore {
    store: ExternalServiceStore,
}

impl PgExternalServiceConnectionStore {
    pub fn new(pool: sqlx::PgPool) -> Self {
        Self {
            store: ExternalServiceStore::new(pool),
        }
    }
}

#[async_trait]
impl ExternalServiceConnectionStore for PgExternalServiceConnectionStore {
    async fn list(
        &self,
        after_id: i64,
        repo_id: i32,
        limit: Option<i64>,
    ) -> Result<Vec<ExternalServiceListRecord>, String> {
        self.store
            .list_connection(after_id, repo_id, limit)
            .await
            .map_err(|error| error.to_string())
    }

    async fn count(&self, after_id: i64, repo_id: i32) -> Result<i64, String> {
        self.store
            .count_connection(after_id, repo_id)
            .await
            .map_err(|error| error.to_string())
    }

    async fn repo_count(&self, id: i64) -> Result<i32, String> {
        self.store
            .repo_count(id)
            .await
            .map_err(|error| error.to_string())
    }

    async fn last_sync_error(&self, id: i64) -> Result<Option<String>, String> {
        self.store
            .last_sync_error(id)
            .await
            .map_err(|error| error.to_string())
    }

    async fn get_by_id(&self, id: i64) -> Result<ExternalServiceListRecord, String> {
        self.store
            .get_list_record_by_id(id)
            .await
            .map_err(|error| error.to_string())
    }
}

/// One `ExternalService` node, with every lazily-resolved field precomputed.
#[derive(Debug, Clone, PartialEq)]
pub struct ExternalServiceNodeData {
    pub id: i64,
    pub kind: String,
    pub display_name: String,
    /// `RedactedConfig` (decrypted, secrets replaced by [`REDACTED_SECRET`]).
    pub config: String,
    pub created_at: time::OffsetDateTime,
    pub updated_at: time::OffsetDateTime,
    pub creator: Option<UserData>,
    pub last_updater: Option<UserData>,
    pub warning: Option<String>,
    pub last_sync_error: Option<String>,
    pub repo_count: i32,
    pub last_sync_at: Option<time::OffsetDateTime>,
    pub next_sync_at: Option<time::OffsetDateTime>,
    pub unrestricted: bool,
    pub supports_repo_exclusion: bool,
}

/// An `ExternalServiceConnection` page.
#[derive(Debug, Clone, PartialEq)]
pub struct ExternalServicesConnectionData {
    pub nodes: Vec<ExternalServiceNodeData>,
    pub total_count: i64,
    pub has_next_page: bool,
    pub end_cursor: Option<String>,
}

/// The result of resolving one `Query.externalServices` field.
#[derive(Debug, Clone, PartialEq)]
pub enum ExternalServicesConnectionLookup {
    Found(ExternalServicesConnectionData),
    Error(String),
}

/// `schemaResolver.ExternalServices` (`external_services.go:376-398`),
/// including the site-admin gate, cursor parsing and the connection's
/// `Nodes`/`TotalCount`/`PageInfo` computation.
pub async fn external_services(
    args: &ExternalServicesArgs,
    store: &dyn ExternalServiceConnectionStore,
    users: Option<&dyn UserLookupStore>,
    key: Option<Arc<dyn Key>>,
    actor: Actor,
    current_user: Option<&UserData>,
) -> ExternalServicesConnectionLookup {
    // `auth.CheckCurrentUserIsSiteAdmin`.
    match actor {
        Actor::Anonymous => {
            return ExternalServicesConnectionLookup::Error("not authenticated".to_owned())
        }
        Actor::User { .. } => match current_user {
            None => return ExternalServicesConnectionLookup::Error("not authenticated".to_owned()),
            Some(user) if !user.site_admin => {
                return ExternalServicesConnectionLookup::Error("must be site admin".to_owned())
            }
            Some(_) => {}
        },
    }

    let after_id = match &args.after {
        Some(raw) => match unmarshal_external_service_id(raw) {
            Ok(id) => id,
            Err(message) => return ExternalServicesConnectionLookup::Error(message),
        },
        None => 0,
    };
    let repo_id = match &args.repo {
        Some(raw) => match unmarshal_repository_id(raw) {
            Ok(id) => id,
            Err(message) => return ExternalServicesConnectionLookup::Error(message),
        },
        None => 0,
    };
    let limit = args.first.map(i64::from);

    let records = match store.list(after_id, repo_id, limit).await {
        Ok(records) => records,
        Err(message) => return ExternalServicesConnectionLookup::Error(message),
    };
    // `TotalCount` resets `AfterID` but keeps the repo filter.
    let total_count = match store.count(0, repo_id).await {
        Ok(count) => count,
        Err(message) => return ExternalServicesConnectionLookup::Error(message),
    };
    // `externalServiceConnectionResolver.PageInfo`.
    let mut has_next_page = false;
    let mut end_cursor = None;
    if let Some(limit) = limit {
        if records.len() as i64 >= limit {
            let count = match store.count(after_id, repo_id).await {
                Ok(count) => count,
                Err(message) => return ExternalServicesConnectionLookup::Error(message),
            };
            if count > records.len() as i64 {
                if let Some(last) = records.last() {
                    has_next_page = true;
                    end_cursor = Some(marshal_external_service_id(last.id));
                }
            }
        }
    }

    let mut user_cache: HashMap<i32, Result<Option<UserData>, String>> = HashMap::new();
    let mut nodes = Vec::with_capacity(records.len());
    for record in records {
        let config = match redacted_config(&record, key.as_deref()).await {
            Ok(config) => config,
            Err(message) => return ExternalServicesConnectionLookup::Error(message),
        };
        let repo_count = match store.repo_count(record.id).await {
            Ok(count) => count,
            Err(message) => return ExternalServicesConnectionLookup::Error(message),
        };
        let last_sync_error = match store.last_sync_error(record.id).await {
            Ok(error) => error,
            Err(message) => return ExternalServicesConnectionLookup::Error(message),
        };
        let creator = match lookup_user(users, &mut user_cache, record.creator_id).await {
            Ok(user) => user,
            Err(message) => return ExternalServicesConnectionLookup::Error(message),
        };
        let last_updater = match lookup_user(users, &mut user_cache, record.last_updater_id).await {
            Ok(user) => user,
            Err(message) => return ExternalServicesConnectionLookup::Error(message),
        };
        nodes.push(ExternalServiceNodeData {
            id: record.id,
            supports_repo_exclusion: supports_repo_exclusion(&record.kind),
            kind: record.kind,
            display_name: record.display_name,
            config,
            created_at: record.created_at,
            updated_at: record.updated_at,
            creator,
            last_updater,
            // `List` never populates the resolver's `warning` field.
            warning: None,
            last_sync_error,
            repo_count,
            last_sync_at: record.last_sync_at,
            next_sync_at: record.next_sync_at,
            unrestricted: record.unrestricted,
        });
    }

    ExternalServicesConnectionLookup::Found(ExternalServicesConnectionData {
        nodes,
        total_count,
        has_next_page,
        end_cursor,
    })
}

/// `externalServiceByID` (`external_service.go:71-84`) reduced to the node
/// data the read-only `ExternalService` fields need. The site-admin gate is
/// the caller's responsibility (`WebhookLog.externalService` resolves only
/// after the connection's gate).
pub async fn external_service_by_id(
    store: &dyn ExternalServiceConnectionStore,
    users: Option<&dyn UserLookupStore>,
    key: Option<Arc<dyn Key>>,
    id: i64,
) -> Result<ExternalServiceNodeData, String> {
    let record = store.get_by_id(id).await?;
    let config = redacted_config(&record, key.as_deref()).await?;
    let repo_count = store.repo_count(record.id).await?;
    let last_sync_error = store.last_sync_error(record.id).await?;
    let mut user_cache: HashMap<i32, Result<Option<UserData>, String>> = HashMap::new();
    let creator = lookup_user(users, &mut user_cache, record.creator_id).await?;
    let last_updater = lookup_user(users, &mut user_cache, record.last_updater_id).await?;
    Ok(ExternalServiceNodeData {
        id: record.id,
        supports_repo_exclusion: supports_repo_exclusion(&record.kind),
        kind: record.kind,
        display_name: record.display_name,
        config,
        created_at: record.created_at,
        updated_at: record.updated_at,
        creator,
        last_updater,
        warning: None,
        last_sync_error,
        repo_count,
        last_sync_at: record.last_sync_at,
        next_sync_at: record.next_sync_at,
        unrestricted: record.unrestricted,
    })
}

async fn lookup_user(
    users: Option<&dyn UserLookupStore>,
    cache: &mut HashMap<i32, Result<Option<UserData>, String>>,
    id: Option<i32>,
) -> Result<Option<UserData>, String> {
    let Some(id) = id else {
        return Ok(None);
    };
    if let Some(cached) = cache.get(&id) {
        return cached.clone();
    }
    let result = match users {
        Some(users) => users.lookup(&UserKey::DatabaseId(id)).await,
        None => Ok(None),
    };
    cache.insert(id, result.clone());
    result
}

/// Prefetch every selected `Query.externalServices` field.
pub async fn prefetch_external_services(
    store: &dyn ExternalServiceConnectionStore,
    users: Option<&dyn UserLookupStore>,
    key: Option<Arc<dyn Key>>,
    fields: &[RootField],
    actor: Actor,
    current_user: Option<&UserData>,
) -> HashMap<ExternalServicesArgs, ExternalServicesConnectionLookup> {
    let mut connections = HashMap::new();
    for field in fields {
        if field.name != "externalServices" {
            continue;
        }
        let args = external_services_args(&field.arguments);
        if connections.contains_key(&args) {
            continue;
        }
        let lookup = external_services(&args, store, users, key.clone(), actor, current_user).await;
        connections.insert(args, lookup);
    }
    connections
}

/// `extsvc.SupportsRepoExclusion` (`internal/extsvc/types.go:417-424`).
fn supports_repo_exclusion(kind: &str) -> bool {
    matches!(
        kind.to_ascii_uppercase().as_str(),
        "AWSCODECOMMIT"
            | "AZUREDEVOPS"
            | "BITBUCKETCLOUD"
            | "BITBUCKETSERVER"
            | "GERRIT"
            | "GITHUB"
            | "GITLAB"
            | "GITOLITE"
    )
}

/// `ExternalService.RedactedConfig` (`internal/types/secret.go:39-119`).
async fn redacted_config(
    record: &ExternalServiceListRecord,
    key: Option<&dyn Key>,
) -> Result<String, String> {
    let raw = maybe_decrypt(key, &record.config, &record.encryption_key_id)
        .await
        .map_err(|error| error.to_string())?;
    if raw.is_empty() {
        return Ok(raw);
    }
    redact_config(&record.kind, &raw)
}

/// Replace the kind's secret properties in the raw JSONC document, keeping
/// its formatting (`jsonx.ComputePropertyEdit` + `ApplyEdits`).
fn redact_config(kind: &str, raw: &str) -> Result<String, String> {
    let (value, errors) = parse(
        raw,
        ParseOptions {
            comments: true,
            trailing_commas: true,
        },
    );
    if let Some(error) = errors.first() {
        return Err(format!(
            "parsing external service config: {}",
            error.code_name()
        ));
    }
    let Some(value) = value else {
        return Err("parsing external service config: unexpected end of JSON input".to_owned());
    };

    let mut edits: Vec<(Path, EditValue)> = Vec::new();
    let kind = kind.to_ascii_uppercase();
    match kind.as_str() {
        "GITHUB" => push_string_edit(&mut edits, &value, &["token"])?,
        "GITLAB" => {
            push_string_edit(&mut edits, &value, &["token"])?;
            push_string_edit(&mut edits, &value, &["token.oauth.refresh"])?;
        }
        "AZUREDEVOPS" => push_string_edit(&mut edits, &value, &["token"])?,
        "GERRIT" => push_string_edit(&mut edits, &value, &["password"])?,
        "BITBUCKETSERVER" => {
            push_string_edit(&mut edits, &value, &["password"])?;
            push_string_edit(&mut edits, &value, &["token"])?;
        }
        "BITBUCKETCLOUD" => {
            push_string_edit(&mut edits, &value, &["appPassword"])?;
            push_string_edit(&mut edits, &value, &["accessToken"])?;
        }
        "AWSCODECOMMIT" => {
            push_string_edit(&mut edits, &value, &["secretAccessKey"])?;
            push_string_edit(&mut edits, &value, &["gitCredentials", "password"])?;
        }
        "PHABRICATOR" => push_string_edit(&mut edits, &value, &["token"])?,
        "PERFORCE" => push_string_edit(&mut edits, &value, &["p4.passwd"])?,
        "GITOLITE" | "RUSTPACKAGES" => {}
        "GOMODULES" | "PYTHONPACKAGES" => {
            let urls = string_array(&value, "urls")?;
            for (index, url) in urls.iter().enumerate() {
                if url.is_empty() {
                    continue;
                }
                let redacted = redacted_url(url)?;
                edits.push((
                    vec![
                        Segment::Property("urls".to_owned()),
                        Segment::Index(index as i64),
                    ],
                    EditValue::Json(go_json_string(&redacted)),
                ));
            }
        }
        "RUBYPACKAGES" => push_string_edit(&mut edits, &value, &["repository"])?,
        "JVMPACKAGES" => push_string_edit(&mut edits, &value, &["maven", "credentials"])?,
        "PAGURE" => push_string_edit(&mut edits, &value, &["token"])?,
        "NPMPACKAGES" => push_string_edit(&mut edits, &value, &["credentials"])?,
        "OTHER" => push_url_edit(&mut edits, &value, &["url"])?,
        _ => {
            return Err(format!(
                "Unrecognized ExternalServiceConfig for redaction: kind {kind} not implemented"
            ))
        }
    }

    let options = redaction_format_options();
    let mut output = raw.to_owned();
    for (path, value) in edits {
        let compiled = compute_property_edit(&output, &path, value, &options)?;
        output = apply_edits(&output, &compiled)?;
    }
    Ok(output)
}

/// `jsonc.DefaultFormatOptions` (`{InsertSpaces:true, TabSize:2, EOL:""}`).
fn redaction_format_options() -> FormatOptions {
    FormatOptions {
        tab_size: 2,
        insert_spaces: true,
        eol: String::new(),
    }
}

fn push_string_edit(
    edits: &mut Vec<(Path, EditValue)>,
    value: &JsonValue,
    path: &[&str],
) -> Result<(), String> {
    let Some(secret) = string_at(value, path)? else {
        return Ok(());
    };
    if secret.is_empty() {
        return Ok(());
    }
    edits.push((
        property_edit_path(path),
        EditValue::Json(go_json_string(REDACTED_SECRET)),
    ));
    Ok(())
}

fn push_url_edit(
    edits: &mut Vec<(Path, EditValue)>,
    value: &JsonValue,
    path: &[&str],
) -> Result<(), String> {
    let Some(raw) = string_at(value, path)? else {
        return Ok(());
    };
    if raw.is_empty() {
        return Ok(());
    }
    let redacted = redacted_url(&raw)?;
    edits.push((
        property_edit_path(path),
        EditValue::Json(go_json_string(&redacted)),
    ));
    Ok(())
}

/// `jsonx.PropertyPath`: every component is a single property name (dots are
/// not separators, matching the flat `token.oauth.refresh` / `p4.passwd` tags).
fn property_edit_path(path: &[&str]) -> Path {
    path.iter()
        .map(|property| Segment::Property((*property).to_owned()))
        .collect()
}

fn value_at<'a>(value: &'a JsonValue, path: &[Segment]) -> Option<&'a JsonValue> {
    let mut current = value;
    for segment in path {
        current = match (segment, current) {
            (Segment::Property(property), JsonValue::Object(object)) => object.get(property)?,
            (Segment::Index(index), JsonValue::Array(items)) => {
                items.get(usize::try_from(*index).ok()?)?
            }
            _ => return None,
        };
    }
    Some(current)
}

fn string_at(value: &JsonValue, path: &[&str]) -> Result<Option<String>, String> {
    let path = property_edit_path(path);
    match value_at(value, &path) {
        None => Ok(None),
        Some(JsonValue::String(text)) => Ok(Some(text.clone())),
        Some(_) => Err("external service config: expected a string".to_owned()),
    }
}

fn string_array(value: &JsonValue, property: &str) -> Result<Vec<String>, String> {
    let path = vec![Segment::Property(property.to_owned())];
    match value_at(value, &path) {
        None => Ok(Vec::new()),
        Some(JsonValue::Array(items)) => items
            .iter()
            .map(|item| match item {
                JsonValue::String(text) => Ok(text.clone()),
                _ => Err("external service config: expected an array of strings".to_owned()),
            })
            .collect(),
        Some(_) => Err("external service config: expected an array".to_owned()),
    }
}

/// `redactedURL`: replace a URL's password with [`REDACTED_SECRET`].
/// Without a password the raw text is kept (overview §3.4).
fn redacted_url(raw: &str) -> Result<String, String> {
    let mut parsed = url::Url::parse(raw).map_err(|error| error.to_string())?;
    if parsed.password().is_none() {
        return Ok(raw.to_owned());
    }
    parsed
        .set_password(Some(REDACTED_SECRET))
        .map_err(|_| "external service config: cannot set URL password".to_owned())?;
    Ok(parsed.to_string())
}

/// `externalServiceConnectionResolver`.
pub struct ExternalServicesConnectionObject {
    data: ExternalServicesConnectionData,
    viewer_site_admin: bool,
    nested: Arc<crate::graphql_resolvers::NestedPrefetch>,
}

impl ExternalServicesConnectionObject {
    pub fn from_data(
        data: ExternalServicesConnectionData,
        viewer_site_admin: bool,
        nested: Arc<crate::graphql_resolvers::NestedPrefetch>,
    ) -> Self {
        Self {
            data,
            viewer_site_admin,
            nested,
        }
    }
}

impl ObjectValue for ExternalServicesConnectionObject {
    fn type_name(&self) -> &'static str {
        "ExternalServiceConnection"
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
                    .nodes
                    .iter()
                    .map(|node| {
                        FieldValue::Object(Arc::new(ExternalServiceObject {
                            data: node.clone(),
                            viewer_site_admin: self.viewer_site_admin,
                            nested: self.nested.clone(),
                        }))
                    })
                    .collect(),
                element_type: Some("ExternalService"),
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

/// `externalServiceResolver`, limited to the read-only base fields.
pub struct ExternalServiceObject {
    data: ExternalServiceNodeData,
    viewer_site_admin: bool,
    nested: Arc<crate::graphql_resolvers::NestedPrefetch>,
}

impl ExternalServiceObject {
    /// Build the node object from pre-fetched data (used by
    /// `WebhookLog.externalService`).
    pub fn from_data(
        data: ExternalServiceNodeData,
        viewer_site_admin: bool,
        nested: Arc<crate::graphql_resolvers::NestedPrefetch>,
    ) -> Self {
        Self {
            data,
            viewer_site_admin,
            nested,
        }
    }
}

impl ObjectValue for ExternalServiceObject {
    fn type_name(&self) -> &'static str {
        "ExternalService"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "id" => Ok(Some(FieldValue::Json(Json::Str(
                marshal_external_service_id(self.data.id),
            )))),
            "kind" => Ok(Some(FieldValue::Json(Json::Str(self.data.kind.clone())))),
            "displayName" => Ok(Some(FieldValue::Json(Json::Str(
                self.data.display_name.clone(),
            )))),
            "config" => Ok(Some(FieldValue::Json(Json::Str(self.data.config.clone())))),
            "createdAt" => Ok(Some(FieldValue::Json(Json::Str(datetime_rfc3339(
                self.data.created_at,
            ))))),
            "updatedAt" => Ok(Some(FieldValue::Json(Json::Str(datetime_rfc3339(
                self.data.updated_at,
            ))))),
            "creator" => Ok(Some(user_field(
                &self.data.creator,
                self.viewer_site_admin,
                self.nested.clone(),
            ))),
            "lastUpdater" => Ok(Some(user_field(
                &self.data.last_updater,
                self.viewer_site_admin,
                self.nested.clone(),
            ))),
            "warning" => Ok(Some(optional_string(&self.data.warning))),
            "lastSyncError" => Ok(Some(optional_string(&self.data.last_sync_error))),
            "repoCount" => Ok(Some(FieldValue::Json(Json::Int(i64::from(
                self.data.repo_count,
            ))))),
            "lastSyncAt" => Ok(Some(optional_datetime(self.data.last_sync_at))),
            "nextSyncAt" => Ok(Some(optional_datetime(self.data.next_sync_at))),
            "unrestricted" => Ok(Some(FieldValue::Json(Json::Bool(self.data.unrestricted)))),
            "supportsRepoExclusion" => Ok(Some(FieldValue::Json(Json::Bool(
                self.data.supports_repo_exclusion,
            )))),
            // Explicit "not ported" rather than the executor's generic
            // missing-resolver error (overview §3.1).
            "url" | "webhookURL" | "rateLimiterState" | "checkConnection"
            | "hasConnectionCheck" | "syncJobs" | "webhookLogs" => Err(ExecError::new(format!(
                "ExternalService.{field} is not ported (M9.8gh)"
            ))),
            _ => Ok(None),
        }
    }
}

fn user_field(
    user: &Option<UserData>,
    viewer_site_admin: bool,
    nested: Arc<crate::graphql_resolvers::NestedPrefetch>,
) -> FieldValue {
    match user {
        Some(user) => FieldValue::Object(Arc::new(
            UserObject::from_data_as_viewer(user.clone(), viewer_site_admin).with_nested(nested),
        )),
        None => FieldValue::Null,
    }
}

fn optional_string(value: &Option<String>) -> FieldValue {
    match value {
        Some(value) => FieldValue::Json(Json::Str(value.clone())),
        None => FieldValue::Null,
    }
}

fn optional_datetime(value: Option<time::OffsetDateTime>) -> FieldValue {
    match value {
        Some(value) => FieldValue::Json(Json::Str(datetime_rfc3339(value))),
        None => FieldValue::Null,
    }
}
