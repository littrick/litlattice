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
//! M9.8gk ports the remaining read-only node fields: `url`
//! (`extsvc.UniqueCodeHostIdentifier`), `webhookURL` (per-kind
//! `extsvc.WebhookURL` + enable switch), `rateLimiterState` (always `null` in
//! this snapshot; see the M9.8gk overview §3.1), `hasConnectionCheck`
//! (`availabilityCheck`) and `webhookLogs` (the M9.8gi connection).
//!
//! Not ported: `checkConnection` (code-host probing), `syncJobs` (syncer
//! write path), and every mutation. Note that `webhookURL` and
//! `webhookLogs` are not part of the OSS SDL in this snapshot (overview §0).

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
    check_current_user_is_site_admin, datetime_rfc3339, relay_kind, PageInfoObject, UserData,
    UserKey, UserLookupStore, UserObject,
};
use crate::webhook_logs::{
    webhook_logs_args, WebhookLogConnectionObject, WebhookLogsConnectionLookup,
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

pub(crate) fn relay_spec_i64(raw: &str) -> Result<i64, String> {
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
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExternalServiceNodeData {
    pub id: i64,
    pub kind: String,
    pub display_name: String,
    /// `RedactedConfig` (decrypted, secrets replaced by [`REDACTED_SECRET`]).
    pub config: String,
    /// `extsvc.UniqueCodeHostIdentifier` (`ExternalService.url`).
    pub url: Result<String, String>,
    /// `ExternalService.webhookURL`; `Ok(None)` is the baseline's `nil`
    /// ("no webhook URL can be built for this kind / webhooks disabled").
    pub webhook_url: Result<Option<String>, String>,
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
    /// `availabilityCheck[kind]` (`ExternalService.hasConnectionCheck`).
    pub has_connection_check: bool,
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
#[allow(clippy::too_many_arguments)]
pub async fn external_services(
    args: &ExternalServicesArgs,
    store: &dyn ExternalServiceConnectionStore,
    users: Option<&dyn UserLookupStore>,
    key: Option<Arc<dyn Key>>,
    external_url: &str,
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
        let node = match node_data_from_record(
            record,
            store,
            users,
            key.clone(),
            external_url,
            &mut user_cache,
        )
        .await
        {
            Ok(node) => node,
            Err(message) => return ExternalServicesConnectionLookup::Error(message),
        };
        nodes.push(node);
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
    external_url: &str,
    id: i64,
) -> Result<ExternalServiceNodeData, String> {
    let record = store.get_by_id(id).await?;
    let mut user_cache: HashMap<i32, Result<Option<UserData>, String>> = HashMap::new();
    node_data_from_record(record, store, users, key, external_url, &mut user_cache).await
}

/// `schemaResolver.Node`'s `externalServiceByID` branch (`node.go`,
/// `external_service.go:71-84`, `:90-99`): site-admin gate, Relay kind
/// validation, then `GetByID` (soft-deleted services are not found).
#[allow(clippy::too_many_arguments)]
pub async fn external_service_node_by_id(
    store: &dyn ExternalServiceConnectionStore,
    users: Option<&dyn UserLookupStore>,
    key: Option<Arc<dyn Key>>,
    external_url: &str,
    raw_id: &str,
    actor: Actor,
    viewer_site_admin: bool,
) -> Result<ExternalServiceNodeData, String> {
    check_current_user_is_site_admin(actor, viewer_site_admin)?;
    let id = unmarshal_external_service_id(raw_id)?;
    external_service_by_id(store, users, key, external_url, id).await
}

/// Build one node's data: decrypt `config` once, then derive the redacted
/// config, `url` (`extsvc.UniqueCodeHostIdentifier`), `webhookURL` and the
/// eagerly-prefetched relation counts.
async fn node_data_from_record(
    record: ExternalServiceListRecord,
    store: &dyn ExternalServiceConnectionStore,
    users: Option<&dyn UserLookupStore>,
    key: Option<Arc<dyn Key>>,
    external_url: &str,
    user_cache: &mut HashMap<i32, Result<Option<UserData>, String>>,
) -> Result<ExternalServiceNodeData, String> {
    let raw = maybe_decrypt(key.as_deref(), &record.config, &record.encryption_key_id)
        .await
        .map_err(|error| error.to_string())?;
    // `ExternalService.RedactedConfig` short-circuits on an empty config.
    let config = if raw.is_empty() {
        raw.clone()
    } else {
        redact_config(&record.kind, &raw)?
    };
    let url = unique_code_host_identifier(&record.kind, &raw);
    let webhook_url = webhook_url(&record.kind, record.id, &raw, external_url);
    let repo_count = store.repo_count(record.id).await?;
    let last_sync_error = store.last_sync_error(record.id).await?;
    let creator = lookup_user(users, user_cache, record.creator_id).await?;
    let last_updater = lookup_user(users, user_cache, record.last_updater_id).await?;
    Ok(ExternalServiceNodeData {
        id: record.id,
        supports_repo_exclusion: supports_repo_exclusion(&record.kind),
        has_connection_check: has_connection_check(&record.kind),
        kind: record.kind,
        display_name: record.display_name,
        config,
        url,
        webhook_url,
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
#[allow(clippy::too_many_arguments)]
pub async fn prefetch_external_services(
    store: &dyn ExternalServiceConnectionStore,
    users: Option<&dyn UserLookupStore>,
    key: Option<Arc<dyn Key>>,
    external_url: &str,
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
        let lookup = external_services(
            &args,
            store,
            users,
            key.clone(),
            external_url,
            actor,
            current_user,
        )
        .await;
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

/// `availabilityCheck` (`external_service.go:345-347`).
fn has_connection_check(kind: &str) -> bool {
    matches!(
        kind.to_ascii_uppercase().as_str(),
        "AZUREDEVOPS" | "BITBUCKETCLOUD" | "BITBUCKETSERVER" | "GITHUB" | "GITLAB" | "PERFORCE"
    )
}

/// `extsvc.Variant` (`internal/extsvc/types.go:180-290`). `AsKind` is upper
/// case and `AsType` is the camelCase form of the same letters, so a single
/// case-insensitive match against the concatenation covers both.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Variant {
    AwsCodeCommit,
    AzureDevOps,
    BitbucketCloud,
    BitbucketServer,
    Gerrit,
    GitHub,
    GitLab,
    Gitolite,
    GoModules,
    JvmPackages,
    NpmPackages,
    Other,
    Pagure,
    Perforce,
    Phabricator,
    PythonPackages,
    RubyPackages,
    RustPackages,
}

impl Variant {
    /// `VariantValueOf` (`types.go:265-273`): case-insensitive `AsKind` or
    /// `AsType`; both lowercase to the same string.
    fn from_kind(kind: &str) -> Option<Self> {
        match kind.to_ascii_lowercase().as_str() {
            "awscodecommit" => Some(Self::AwsCodeCommit),
            "azuredevops" => Some(Self::AzureDevOps),
            "bitbucketcloud" => Some(Self::BitbucketCloud),
            "bitbucketserver" => Some(Self::BitbucketServer),
            "gerrit" => Some(Self::Gerrit),
            "github" => Some(Self::GitHub),
            "gitlab" => Some(Self::GitLab),
            "gitolite" => Some(Self::Gitolite),
            "gomodules" => Some(Self::GoModules),
            "jvmpackages" => Some(Self::JvmPackages),
            "npmpackages" => Some(Self::NpmPackages),
            "other" => Some(Self::Other),
            "pagure" => Some(Self::Pagure),
            "perforce" => Some(Self::Perforce),
            "phabricator" => Some(Self::Phabricator),
            "pythonpackages" => Some(Self::PythonPackages),
            "rubypackages" => Some(Self::RubyPackages),
            "rustpackages" => Some(Self::RustPackages),
            _ => None,
        }
    }

    /// `Variant.AsKind()` (`types.go:240-242`).
    fn as_kind(self) -> &'static str {
        match self {
            Self::AwsCodeCommit => "AWSCODECOMMIT",
            Self::AzureDevOps => "AZUREDEVOPS",
            Self::BitbucketCloud => "BITBUCKETCLOUD",
            Self::BitbucketServer => "BITBUCKETSERVER",
            Self::Gerrit => "GERRIT",
            Self::GitHub => "GITHUB",
            Self::GitLab => "GITLAB",
            Self::Gitolite => "GITOLITE",
            Self::GoModules => "GOMODULES",
            Self::JvmPackages => "JVMPACKAGES",
            Self::NpmPackages => "NPMPACKAGES",
            Self::Other => "OTHER",
            Self::Pagure => "PAGURE",
            Self::Perforce => "PERFORCE",
            Self::Phabricator => "PHABRICATOR",
            Self::PythonPackages => "PYTHONPACKAGES",
            Self::RubyPackages => "RUBYPACKAGES",
            Self::RustPackages => "RUSTPACKAGES",
        }
    }

    /// `Variant.WebhookURLPath()` (`types.go:257-259`); empty for kinds that
    /// do not support webhooks.
    fn webhook_url_path(self) -> &'static str {
        match self {
            Self::BitbucketCloud => "bitbucket-cloud-webhooks",
            Self::BitbucketServer => "bitbucket-server-webhooks",
            Self::GitHub => "github-webhooks",
            Self::GitLab => "gitlab-webhooks",
            _ => "",
        }
    }
}

/// `jsonc.Unmarshal` on an external service config: comments and trailing
/// commas are accepted; syntax errors and an empty document are errors.
fn parse_config(config: &str) -> Result<JsonValue, String> {
    let (value, errors) = parse(
        config,
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
    value.ok_or_else(|| "parsing external service config: unexpected end of JSON input".to_owned())
}

fn config_property<'a>(value: &'a JsonValue, property: &str) -> Option<&'a JsonValue> {
    match value {
        JsonValue::Object(object) => object.get(property),
        _ => None,
    }
}

/// A config string property; missing/null is the Go zero value `""`, a
/// non-string value is an unmarshal error.
fn config_string(value: &JsonValue, property: &str) -> Result<String, String> {
    match config_property(value, property) {
        None | Some(JsonValue::Null) => Ok(String::new()),
        Some(JsonValue::String(text)) => Ok(text.clone()),
        Some(_) => Err("parsing external service config: expected a string".to_owned()),
    }
}

/// `extsvc.NormalizeBaseURL` (`internal/extsvc/codehost.go:86-93`): host
/// lowercased, path gets a trailing `/`. `url.Parse` accepts relative values
/// too (e.g. a gitolite `host`); those are treated as a bare path.
fn normalize_base_url(raw: &str) -> String {
    match url::Url::parse(raw) {
        Ok(mut parsed) => {
            let path = parsed.path();
            if !path.ends_with('/') {
                let with_slash = format!("{path}/");
                parsed.set_path(&with_slash);
            }
            parsed.to_string()
        }
        Err(_) => {
            if raw.ends_with('/') {
                raw.to_owned()
            } else {
                format!("{raw}/")
            }
        }
    }
}

/// `extsvc.UniqueCodeHostIdentifier` (`internal/extsvc/types.go:796-856`).
fn unique_code_host_identifier(kind: &str, config: &str) -> Result<String, String> {
    let Some(variant) = Variant::from_kind(kind) else {
        return Err(format!("unknown external service kind {kind:?}"));
    };
    let value = parse_config(config)?;
    match variant {
        Variant::AwsCodeCommit => Ok(format!(
            "{}:{}",
            config_string(&value, "region")?,
            config_string(&value, "accessKeyID")?
        )),
        Variant::Perforce => config_string(&value, "p4.port"),
        Variant::GoModules
        | Variant::JvmPackages
        | Variant::NpmPackages
        | Variant::PythonPackages
        | Variant::RubyPackages
        | Variant::RustPackages => Ok(variant.as_kind().to_owned()),
        Variant::Gitolite => Ok(normalize_base_url(&config_string(&value, "host")?)),
        Variant::GitLab
        | Variant::GitHub
        | Variant::AzureDevOps
        | Variant::BitbucketServer
        | Variant::BitbucketCloud
        | Variant::Gerrit
        | Variant::Phabricator
        | Variant::Other
        | Variant::Pagure => Ok(normalize_base_url(&config_string(&value, "url")?)),
    }
}

/// True when the property is present and not JSON `null` (Go pointer/slice
/// nil checks after `jsonc.Unmarshal`).
fn config_non_null(value: &JsonValue, property: &str) -> Result<bool, String> {
    match config_property(value, property) {
        None | Some(JsonValue::Null) => Ok(false),
        Some(JsonValue::Object(_)) => Ok(true),
        Some(_) => Err("parsing external service config: expected an object".to_owned()),
    }
}

/// `len(config.Webhooks) > 0` for the slice-valued webhook config fields.
fn config_webhooks_present(value: &JsonValue, property: &str) -> Result<bool, String> {
    match config_property(value, property) {
        None | Some(JsonValue::Null) => Ok(false),
        Some(JsonValue::Array(items)) => Ok(!items.is_empty()),
        Some(_) => Err("parsing external service config: expected an array".to_owned()),
    }
}

/// `extsvc.WebhookURL` (`internal/extsvc/types.go:481-530`) plus the
/// per-kind enable switch in `external_service.go:208-246`. `Ok(None)` is
/// the baseline's `nil` (unsupported kind or webhooks not configured).
fn webhook_url(
    kind: &str,
    id: i64,
    config: &str,
    external_url: &str,
) -> Result<Option<String>, String> {
    let Some(variant) = Variant::from_kind(kind) else {
        return Err(format!("unknown external service kind {kind:?}"));
    };
    let path = variant.webhook_url_path();
    if path.is_empty() {
        return Ok(None);
    }
    let value = parse_config(config)?;

    // `url.Parse(externalURL)`; keep the authority and any existing query
    // parameters, then replace the path (`u.Path = ...`).
    let mut params: std::collections::BTreeMap<String, Vec<String>> =
        std::collections::BTreeMap::new();
    let mut base = String::new();
    match url::Url::parse(external_url) {
        Ok(mut parsed) => {
            for (key, value) in url::form_urlencoded::parse(parsed.query().unwrap_or("").as_bytes())
            {
                params
                    .entry(key.into_owned())
                    .or_default()
                    .push(value.into_owned());
            }
            parsed.set_path(&format!("/.api/{path}"));
            let rendered = parsed.to_string();
            let end = rendered.find(['?', '#']).unwrap_or(rendered.len());
            base.push_str(&rendered[..end]);
        }
        // Go's `url.Parse` accepts relative values, but its `String()` would
        // only contain the (replaced) path, so the relative form is the same.
        Err(_) => base.push_str(&format!(".api/{path}")),
    }
    params.insert("externalServiceID".to_owned(), vec![id.to_string()]);
    if variant == Variant::BitbucketCloud {
        params.insert(
            "secret".to_owned(),
            vec![config_string(&value, "webhookSecret")?],
        );
    }
    let mut serializer = url::form_urlencoded::Serializer::new(String::new());
    for (key, values) in &params {
        for value in values {
            serializer.append_pair(key, value);
        }
    }
    let encoded = serializer.finish();
    let url = format!("{base}?{encoded}");

    // `external_service.go:225-243`: only the four webhook-capable kinds are
    // ever assigned, and only when the config enables webhooks.
    let enabled = match variant {
        Variant::BitbucketCloud => !config_string(&value, "webhookSecret")?.is_empty(),
        Variant::BitbucketServer => {
            let direct = config_non_null(&value, "webhooks")?;
            let via_plugin = match config_property(&value, "plugin") {
                None | Some(JsonValue::Null) => false,
                Some(plugin @ JsonValue::Object(_)) => config_non_null(plugin, "webhooks")?,
                Some(_) => {
                    return Err("parsing external service config: expected an object".to_owned())
                }
            };
            direct || via_plugin
        }
        Variant::GitHub | Variant::GitLab => config_webhooks_present(&value, "webhooks")?,
        _ => false,
    };
    Ok(enabled.then_some(url))
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
        arguments: &Arguments,
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
            // `ExternalService.URL` (`external_service.go:128-144`).
            "url" => match &self.data.url {
                Ok(url) => Ok(Some(FieldValue::Json(Json::Str(url.clone())))),
                Err(message) => Err(ExecError::new(message.clone())),
            },
            // `ExternalService.WebhookURL` (`external_service.go:208-246`).
            "webhookURL" => match &self.data.webhook_url {
                Ok(Some(url)) => Ok(Some(FieldValue::Json(Json::Str(url.clone())))),
                Ok(None) => Ok(Some(FieldValue::Null)),
                Err(message) => Err(ExecError::new(message.clone())),
            },
            // `ExternalService.RateLimiterState` (`external_service.go:146-158`)
            // reads the Redis-backed global limiter; this snapshot has no such
            // state source, so "no limiter state" is `null` (overview §3.1).
            "rateLimiterState" => Ok(Some(FieldValue::Null)),
            // `ExternalService.HasConnectionCheck` (`external_service.go:345-347`).
            "hasConnectionCheck" => Ok(Some(FieldValue::Json(Json::Bool(
                self.data.has_connection_check,
            )))),
            // `ExternalService.WebhookLogs` (`external_service.go:287-293`):
            // the M9.8gi connection restricted to this service id.
            "webhookLogs" => {
                let args = webhook_logs_args(arguments);
                match self
                    .nested
                    .external_service_webhook_logs
                    .get(&(self.data.id, args))
                {
                    Some(WebhookLogsConnectionLookup::Found(data)) => Ok(Some(FieldValue::Object(
                        Arc::new(WebhookLogConnectionObject::from_data(
                            data.clone(),
                            self.viewer_site_admin,
                            self.nested.clone(),
                        )),
                    ))),
                    Some(WebhookLogsConnectionLookup::Error(message)) => {
                        Err(ExecError::new(message.clone()))
                    }
                    None => Ok(None),
                }
            }
            // Explicit "not ported" rather than the executor's generic
            // missing-resolver error.
            "checkConnection" | "syncJobs" => Err(ExecError::new(format!(
                "ExternalService.{field} is not ported (M9.8gk)"
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
