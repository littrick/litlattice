#![forbid(unsafe_code)]

//! The remaining read-only `SiteConfiguration` GraphQL fields
//! (`cmd/frontend/graphqlbackend/site.go:198-292`):
//!
//! - `ValidationMessages` — admin check, then `EffectiveContents` and
//!   `conf.ValidateSite`;
//! - `LicenseInfo` — admin check, then
//!   `licensing.GetConfiguredProductLicenseInfo`;
//! - `History` — admin check, then a `SiteConfigurationChangeConnection`.
//!
//! `ID` and `EffectiveContents` live in [`crate::site_configuration_query`]
//! and are reused, not reimplemented. The write path
//! (`UpdateSiteConfiguration`) lives in
//! [`crate::site_configuration_mutation`] and is untouched.
//!
//! As with the other resolver cores in this crate, the module is not wired
//! into [`crate::graphql_resolvers::QueryRoot`]; it exposes plain async
//! functions over injected seams so the parent slice can attach it to the
//! resolver tree.

use std::sync::Arc;

use async_trait::async_trait;
use serde_json::Value;
use sg_config::store::ConfigStore;
use sg_config::RawUnified;
use sg_db::conf as conf_db;
use sg_db::feature_flags::{get_global_feature_flags, get_user_flags};
use sg_db::security_event_logs::{
    insert_security_event, security_event_logs_in_database, SecurityEvent as DbSecurityEvent,
};
use sg_graphql::exec::{Actor, Arguments, ExecError, FieldValue, ObjectValue, RootField};
use sg_graphql::json::Json;
use sqlx::types::time::OffsetDateTime;
use sqlx::PgPool;

use crate::graphql_resolvers::{
    check_current_user_is_site_admin, ConnectionPageInfoObject, UserData, UserKey, UserLookupStore,
    UserObject,
};
use crate::license_config::{LicenseConfig, PgProductLicenseStore, ProductLicenseStore};
use crate::site_config_change::{
    ConnectionPageInfo, ConnectionResolverArgs, ConnectionResolverOptions, SiteConfigHistoryStore,
    SiteConfigRow as ChangeRow, SiteConfigurationChangeConnection, SiteConfigurationChangeResolver,
};
use crate::site_configuration_query::{
    configuration, effective_contents, id as site_configuration_id, SecurityEvent,
    SiteConfigRow as LatestRow, SiteConfigurationBackend, SiteConfigurationResolver,
};

/// `licenseInfoResolver` (`site.go:233-243`), with `expiresAt` already
/// formatted as `gqlutil.DateTime`'s UTC RFC3339 string.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LicenseInfo {
    pub tags: Vec<String>,
    pub user_count: i32,
    pub expires_at: String,
}

/// `licensing.GetConfiguredProductLicenseInfo`.
///
/// `Ok(None)` is the Go `nil` license (GraphQL `null`); `Err` carries the
/// baseline's error text (e.g. an unparseable license key).
#[async_trait]
pub trait LicenseInfoProvider: Send + Sync {
    async fn configured_license(&self) -> Result<Option<LicenseInfo>, String>;
}

/// `conf.ValidateSite` (`internal/conf/validate.go`).
#[async_trait]
pub trait SiteConfigValidator: Send + Sync {
    async fn validate_site(&self, contents: &str) -> Result<Vec<String>, String>;
}

/// `siteConfigurationResolver.ValidationMessages` (`site.go:245-257`).
pub async fn validation_messages(
    backend: &dyn SiteConfigurationBackend,
    validator: &dyn SiteConfigValidator,
    actor: Actor,
    resolver: &SiteConfigurationResolver,
) -> Result<Vec<String>, String> {
    // 🚨 SECURITY: the site configuration contains secret tokens and
    // credentials, so only admins may validate it.
    backend.check_current_user_is_site_admin(actor).await?;
    let contents = effective_contents(backend, actor, resolver).await?;
    validator.validate_site(&contents).await
}

/// `siteConfigurationResolver.LicenseInfo` (`site.go:235-250`).
pub async fn license_info(
    backend: &dyn SiteConfigurationBackend,
    licenses: &dyn LicenseInfoProvider,
    actor: Actor,
) -> Result<Option<LicenseInfo>, String> {
    // 🚨 SECURITY: only site admins can view license information.
    backend.check_current_user_is_site_admin(actor).await?;
    licenses.configured_license().await
}

/// `siteConfigurationResolver.History` (`site.go:259-272`).
///
/// The returned connection borrows `store`; `nodes`, `page_info` and
/// `total_count` are the same methods the `site_config_change` differential
/// already covers.
pub async fn history<'a>(
    backend: &dyn SiteConfigurationBackend,
    store: &'a dyn SiteConfigHistoryStore,
    actor: Actor,
    args: Option<ConnectionResolverArgs>,
    options: ConnectionResolverOptions,
) -> Result<SiteConfigurationChangeConnection<'a>, String> {
    // 🚨 SECURITY: only site admins may view the history.
    backend.check_current_user_is_site_admin(actor).await?;
    Ok(SiteConfigurationChangeConnection::new(store, args, options))
}

// ---------------------------------------------------------------------------
// Production adapters
// ---------------------------------------------------------------------------

/// `conf.ValidateSite` over the process' live raw configuration.
pub struct ConfigStoreSiteConfigValidator {
    store: Arc<ConfigStore>,
}

impl ConfigStoreSiteConfigValidator {
    pub fn new(store: Arc<ConfigStore>) -> Self {
        Self { store }
    }
}

#[async_trait]
impl SiteConfigValidator for ConfigStoreSiteConfigValidator {
    async fn validate_site(&self, contents: &str) -> Result<Vec<String>, String> {
        let store = Arc::clone(&self.store);
        let contents = contents.to_owned();
        // `conf.Raw()` blocks on the configuration store; keep it off the async
        // executor.
        tokio::task::spawn_blocking(move || validate_site_against_raw(&contents, &store.raw().site))
            .await
            .map_err(|err| format!("validation task failed: {err}"))?
    }
}

/// The synchronous core of `conf.ValidateSite`:
/// `UnredactSecrets(contents, conf.Raw())` then the JSON Schema validation.
///
/// `validateCustomRaw` is not ported (M9.8gp errata).
pub fn validate_site_against_raw(contents: &str, raw_site: &str) -> Result<Vec<String>, String> {
    let raw = RawUnified {
        site: raw_site.to_owned(),
        ..RawUnified::default()
    };
    let unredacted = crate::site_configuration_secrets::unredact_secrets(contents, &raw)?;
    sg_site_schema::validate_site(&unredacted).map_err(|err| err.to_string())
}

/// `siteConfigurationResolver`'s `Database`-backed dependencies
/// (`auth.CheckCurrentUserIsSiteAdmin`, `featureflag`, `SecurityEventLogs`,
/// `Conf().SiteGetLatest`, `conf.Raw()`).
#[derive(Clone)]
pub struct ProductionSiteConfigurationBackend {
    pool: PgPool,
    store: Arc<ConfigStore>,
    viewer_site_admin: bool,
}

impl ProductionSiteConfigurationBackend {
    pub fn new(pool: PgPool, store: Arc<ConfigStore>, viewer_site_admin: bool) -> Self {
        Self {
            pool,
            store,
            viewer_site_admin,
        }
    }
}

#[async_trait]
impl SiteConfigurationBackend for ProductionSiteConfigurationBackend {
    async fn check_current_user_is_site_admin(&self, actor: Actor) -> Result<(), String> {
        check_current_user_is_site_admin(actor, self.viewer_site_admin)
    }

    async fn auditlog_expansion(&self, actor: Actor) -> bool {
        auditlog_expansion(&self.pool, actor).await
    }

    async fn log_security_event(&self, event: SecurityEvent) -> Result<(), String> {
        // `securityEventLogsStore.InsertList` is a no-op unless
        // `log.securityEventLog.location` is `database` or `all`.
        let site = self
            .store
            .last_valid()
            .map(|unified| unified.site.raw().clone())
            .unwrap_or(Value::Null);
        if !security_event_logs_in_database(&site) {
            return Ok(());
        }

        let version = crate::version::Version::load();
        let version = String::from_utf8_lossy(version.as_bytes()).into_owned();
        insert_security_event(
            &self.pool,
            &DbSecurityEvent {
                name: event.name,
                url: &event.url,
                user_id: event.user_id,
                anonymous_user_id: &event.anonymous_user_id,
                source: event.source,
                argument: event.argument,
                version: &version,
                timestamp: OffsetDateTime::now_utc(),
            },
        )
        .await
        .map_err(|err| err.to_string())
    }

    async fn site_get_latest(&self) -> Result<LatestRow, String> {
        let (default_site, default_redacted) = default_site_pair()?;
        let row = conf_db::site_get_latest(&self.pool, 0, default_site, default_redacted)
            .await
            .map_err(|err| format!("ConfStore.SiteGetLatest: {err}"))?;
        Ok(LatestRow { id: row.id })
    }

    fn raw_site(&self) -> String {
        self.store.raw().site
    }
}

/// `defaultConfigForDeployment` resolved from `DEPLOY_TYPE`.
fn default_site_pair() -> Result<(&'static str, &'static str), String> {
    let getenv: &dyn Fn(&str) -> Option<String> = &|key| std::env::var(key).ok();
    let deploy_type = crate::service_connections::deploy_type(getenv);
    crate::deploy_defaults::default_site_config_pair(&deploy_type)
}

/// `featureflag.FromContext(ctx).GetBoolOr("auditlog-expansion", false)`.
async fn auditlog_expansion(pool: &PgPool, actor: Actor) -> bool {
    match actor {
        Actor::User { uid } => match get_user_flags(pool, uid).await {
            Ok(flags) => flags.get("auditlog-expansion").copied().unwrap_or(false),
            Err(_) => global_auditlog_expansion(pool).await,
        },
        // Internal actors have `UID == 0`; the featureflag middleware falls
        // through to the global flags (the anonymous-UID header path is not
        // representable in `sg_graphql::exec::Actor` — see overview §2.3).
        Actor::Anonymous | Actor::Internal => global_auditlog_expansion(pool).await,
    }
}

async fn global_auditlog_expansion(pool: &PgPool) -> bool {
    match get_global_feature_flags(pool).await {
        Ok(flags) => flags.get("auditlog-expansion").copied().unwrap_or(false),
        Err(_) => false,
    }
}

/// `db.Conf().ListSiteConfigs` / `GetSiteConfigCount` over
/// `critical_and_site_config`.
///
/// The portable [`SiteConfigHistoryStore`] layer applies the baseline's `LAG`
/// de-duplication, ordering and pagination, so this store returns the raw
/// history in ascending id order.
#[derive(Clone)]
pub struct PgSiteConfigHistoryStore {
    pool: PgPool,
}

impl PgSiteConfigHistoryStore {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl SiteConfigHistoryStore for PgSiteConfigHistoryStore {
    async fn get_site_configs(&self) -> Result<Vec<ChangeRow>, String> {
        let rows = sqlx::query_as::<
            _,
            (
                i32,
                i32,
                String,
                Option<String>,
                OffsetDateTime,
                OffsetDateTime,
            ),
        >(
            "SELECT id, author_user_id, contents, redacted_contents, created_at, updated_at \
             FROM critical_and_site_config WHERE type = 'site' ORDER BY id",
        )
        .fetch_all(&self.pool)
        .await
        .map_err(|err| format!("ConfStore.ListSiteConfigs: {err}"))?;
        Ok(rows
            .into_iter()
            .map(
                |(id, author_user_id, contents, redacted_contents, created_at, updated_at)| {
                    ChangeRow {
                        id,
                        author_user_id,
                        contents,
                        redacted_contents,
                        created_at,
                        updated_at,
                    }
                },
            )
            .collect())
    }

    async fn get_site_config_count(&self) -> Result<usize, String> {
        let count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM ( \
                 SELECT *, LAG(redacted_contents) OVER (ORDER BY id) AS prev_redacted_contents \
                 FROM critical_and_site_config) t \
             WHERE (prev_redacted_contents IS NULL \
                    OR redacted_contents != prev_redacted_contents) \
             AND redacted_contents IS NOT NULL AND type = 'site'",
        )
        .fetch_one(&self.pool)
        .await
        .map_err(|err| format!("ConfStore.GetSiteConfigCount: {err}"))?;
        usize::try_from(count).map_err(|_| format!("negative site config count: {count}"))
    }
}

/// The production seams `SchemaGraphqlDeps` needs for the configuration
/// surface. `licenses` is shared with the M9.8ja PostAuth/access-token gates.
#[derive(Clone)]
pub struct ConfigGraphqlDeps {
    pub pool: PgPool,
    pub store: Arc<ConfigStore>,
    pub licenses: Arc<LicenseConfig>,
}

impl ConfigGraphqlDeps {
    pub fn new(pool: PgPool, store: Arc<ConfigStore>) -> Self {
        let licenses = Arc::new(LicenseConfig::new(Arc::new(PgProductLicenseStore::new(
            pool.clone(),
        ))));
        Self {
            pool,
            store,
            licenses,
        }
    }

    /// Test hook: replace the `product_licenses` source.
    pub fn with_license_store(mut self, store: Arc<dyn ProductLicenseStore>) -> Self {
        self.licenses = Arc::new(LicenseConfig::new(store));
        self
    }
}

// ---------------------------------------------------------------------------
// GraphQL prefetch + synchronous replay
// ---------------------------------------------------------------------------

/// Prefetched `Query.site` data. `configuration` is `None` when the field was
/// not selected.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct SiteRoot {
    pub configuration: Option<Result<SiteConfigurationRoot, String>>,
}

/// Prefetched `SiteConfiguration` fields (only the selected ones are `Some`).
#[derive(Debug, Clone, PartialEq)]
pub struct SiteConfigurationRoot {
    pub return_safe_configs_only: bool,
    pub id: Option<Result<i32, String>>,
    pub effective_contents: Option<Result<String, String>>,
    pub validation_messages: Option<Result<Vec<String>, String>>,
    pub license_info: Option<Result<Option<LicenseInfo>, String>>,
    pub history: Option<Result<HistoryRoot, String>>,
}

/// Prefetched `SiteConfigurationChangeConnection` fields.
#[derive(Debug, Clone, PartialEq)]
pub struct HistoryRoot {
    pub total_count: Option<Result<i32, String>>,
    pub page_info: Option<Result<ConnectionPageInfo, String>>,
    pub nodes: Option<Result<Vec<ChangeNodeRoot>, String>>,
}

/// One prefetched `SiteConfigurationChange` node and its `author`.
#[derive(Debug, Clone, PartialEq)]
pub struct ChangeNodeRoot {
    pub node: SiteConfigurationChangeResolver,
    pub author: Option<Result<Option<UserData>, String>>,
}

fn selected<'a>(fields: &'a [RootField], name: &str) -> Option<&'a RootField> {
    fields.iter().find(|field| field.name == name)
}

/// Prefetch every read-only field of `Query.site.configuration`.
///
/// `None` means the operation did not select `Query.site` (or the executor has
/// no site resolver). The caller injects the result through
/// `FrontendRootResolver::with_site`.
pub async fn prefetch_site(
    backend: &dyn SiteConfigurationBackend,
    licenses: &dyn LicenseInfoProvider,
    validator: &dyn SiteConfigValidator,
    history_store: &dyn SiteConfigHistoryStore,
    users: Option<&dyn UserLookupStore>,
    fields: &[RootField],
    actor: Actor,
) -> Option<SiteRoot> {
    let site = selected(fields, "site")?;
    let Some(configuration_field) = selected(&site.selections, "configuration") else {
        return Some(SiteRoot::default());
    };

    let return_safe_configs_only = configuration_field
        .arguments
        .bool("returnSafeConfigsOnly")
        .unwrap_or(false);
    let configuration = match configuration(
        backend,
        actor,
        configuration_field.arguments.bool("returnSafeConfigsOnly"),
    )
    .await
    {
        Err(message) => Err(message),
        Ok(resolver) => {
            let selections = &configuration_field.selections;
            let want_id = selected(selections, "id").is_some();
            let want_effective = selected(selections, "effectiveContents").is_some();
            let want_validation = selected(selections, "validationMessages").is_some();
            let want_license = selected(selections, "licenseInfo").is_some();
            let want_history = selected(selections, "history").is_some();

            // `ValidationMessages` calls `EffectiveContents` internally; share
            // the lookup when both fields are selected.
            let effective = if want_effective || want_validation {
                Some(effective_contents(backend, actor, &resolver).await)
            } else {
                None
            };

            let id = if want_id {
                Some(site_configuration_id(backend, actor, &resolver).await)
            } else {
                None
            };

            let validation_messages = if want_validation {
                match &effective {
                    Some(Ok(contents)) => Some(validator.validate_site(contents).await),
                    Some(Err(message)) => Some(Err(message.clone())),
                    None => None,
                }
            } else {
                None
            };

            let license_info = if want_license {
                Some(license_info(backend, licenses, actor).await)
            } else {
                None
            };

            let history = if want_history {
                Some(
                    prefetch_history(
                        backend,
                        history_store,
                        users,
                        actor,
                        selected(selections, "history").expect("history selected"),
                    )
                    .await,
                )
            } else {
                None
            };

            Ok(SiteConfigurationRoot {
                return_safe_configs_only,
                id,
                effective_contents: effective,
                validation_messages,
                license_info,
                history,
            })
        }
    };

    Some(SiteRoot {
        configuration: Some(configuration),
    })
}

async fn prefetch_history(
    backend: &dyn SiteConfigurationBackend,
    store: &dyn SiteConfigHistoryStore,
    users: Option<&dyn UserLookupStore>,
    actor: Actor,
    field: &RootField,
) -> Result<HistoryRoot, String> {
    let args = connection_args(field)?;
    let connection = history(
        backend,
        store,
        actor,
        Some(args),
        ConnectionResolverOptions::default(),
    )
    .await?;

    let want_nodes = selected(&field.selections, "nodes").is_some();
    let want_total = selected(&field.selections, "totalCount").is_some();
    let want_page = selected(&field.selections, "pageInfo").is_some();

    let total_count = if want_total {
        Some(connection.total_count().await)
    } else {
        None
    };
    let page_info = if want_page {
        Some(connection.page_info().await)
    } else {
        None
    };

    let nodes = if want_nodes {
        let nodes_field = selected(&field.selections, "nodes").expect("nodes selected");
        let want_author = selected(&nodes_field.selections, "author").is_some();
        match connection.nodes().await {
            Err(message) => Some(Err(message)),
            Ok(nodes) => {
                let mut prefetched = Vec::with_capacity(nodes.len());
                for node in nodes {
                    let author = if want_author {
                        Some(author_lookup(users, node.author_user_id()).await)
                    } else {
                        None
                    };
                    prefetched.push(ChangeNodeRoot { node, author });
                }
                Some(Ok(prefetched))
            }
        }
    } else {
        None
    };

    Ok(HistoryRoot {
        total_count,
        page_info,
        nodes,
    })
}

async fn author_lookup(
    users: Option<&dyn UserLookupStore>,
    author_user_id: i32,
) -> Result<Option<UserData>, String> {
    if author_user_id == 0 {
        return Ok(None);
    }
    let Some(users) = users else {
        return Err("user lookup unavailable".to_owned());
    };
    users.lookup(&UserKey::DatabaseId(author_user_id)).await
}

fn connection_args(field: &RootField) -> Result<ConnectionResolverArgs, String> {
    let int32 = |name: &str| -> Result<Option<i32>, String> {
        field
            .arguments
            .i64(name)
            .map(|value| {
                i32::try_from(value).map_err(|_| format!("{name} is not a 32-bit integer"))
            })
            .transpose()
    };
    Ok(ConnectionResolverArgs {
        first: int32("first")?,
        last: int32("last")?,
        after: field.arguments.string("after").map(str::to_owned),
        before: field.arguments.string("before").map(str::to_owned),
    })
}

/// `siteConfigurationResolver` replay.
#[derive(Debug, Clone, PartialEq)]
pub struct SiteConfigurationObject {
    root: SiteConfigurationRoot,
}

impl SiteConfigurationObject {
    pub fn new(root: SiteConfigurationRoot) -> Self {
        Self { root }
    }
}

impl ObjectValue for SiteConfigurationObject {
    fn type_name(&self) -> &'static str {
        "SiteConfiguration"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "id" => replay(&self.root.id, |value| {
                FieldValue::Json(Json::Int(i64::from(*value)))
            }),
            "effectiveContents" => replay(&self.root.effective_contents, |contents| {
                FieldValue::Json(Json::Str(contents.clone()))
            }),
            "validationMessages" => replay(&self.root.validation_messages, |messages| {
                FieldValue::List {
                    items: messages
                        .iter()
                        .map(|message| FieldValue::Json(Json::Str(message.clone())))
                        .collect(),
                    element_type: Some("String"),
                }
            }),
            "licenseInfo" => match &self.root.license_info {
                None => Ok(None),
                Some(Err(message)) => Err(ExecError::new(message.clone())),
                Some(Ok(None)) => Ok(Some(FieldValue::Null)),
                Some(Ok(Some(info))) => Ok(Some(FieldValue::Object(Arc::new(
                    LicenseInfoObject::new(info.clone()),
                )))),
            },
            "history" => match &self.root.history {
                None => Ok(None),
                Some(Err(message)) => Err(ExecError::new(message.clone())),
                Some(Ok(root)) => Ok(Some(FieldValue::Object(Arc::new(
                    SiteConfigurationChangeConnectionObject::new(root.clone()),
                )))),
            },
            _ => Ok(None),
        }
    }
}

fn replay<T: Clone>(
    value: &Option<Result<T, String>>,
    build: impl FnOnce(&T) -> FieldValue,
) -> Result<Option<FieldValue>, ExecError> {
    match value {
        None => Ok(None),
        Some(Err(message)) => Err(ExecError::new(message.clone())),
        Some(Ok(value)) => Ok(Some(build(value))),
    }
}

/// `licenseInfoResolver` (`site.go:233-243`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LicenseInfoObject {
    info: LicenseInfo,
}

impl LicenseInfoObject {
    pub fn new(info: LicenseInfo) -> Self {
        Self { info }
    }
}

impl ObjectValue for LicenseInfoObject {
    fn type_name(&self) -> &'static str {
        "LicenseInfo"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "tags" => Ok(Some(FieldValue::List {
                items: self
                    .info
                    .tags
                    .iter()
                    .map(|tag| FieldValue::Json(Json::Str(tag.clone())))
                    .collect(),
                element_type: Some("String"),
            })),
            "userCount" => Ok(Some(FieldValue::Json(Json::Int(i64::from(
                self.info.user_count,
            ))))),
            "expiresAt" => Ok(Some(FieldValue::Json(Json::Str(
                self.info.expires_at.clone(),
            )))),
            _ => Ok(None),
        }
    }
}

/// `gqlutil.ConnectionResolver[*SiteConfigurationChangeResolver]` replay.
#[derive(Debug, Clone, PartialEq)]
pub struct SiteConfigurationChangeConnectionObject {
    root: HistoryRoot,
}

impl SiteConfigurationChangeConnectionObject {
    pub fn new(root: HistoryRoot) -> Self {
        Self { root }
    }
}

impl ObjectValue for SiteConfigurationChangeConnectionObject {
    fn type_name(&self) -> &'static str {
        "SiteConfigurationChangeConnection"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "nodes" => match &self.root.nodes {
                None => Ok(None),
                Some(Err(message)) => Err(ExecError::new(message.clone())),
                Some(Ok(nodes)) => Ok(Some(FieldValue::List {
                    items: nodes
                        .iter()
                        .map(|node| {
                            FieldValue::Object(Arc::new(SiteConfigurationChangeObject::new(
                                node.clone(),
                            )))
                        })
                        .collect(),
                    element_type: Some("SiteConfigurationChange"),
                })),
            },
            "totalCount" => replay(&self.root.total_count, |count| {
                FieldValue::Json(Json::Int(i64::from(*count)))
            }),
            "pageInfo" => replay(&self.root.page_info, |page_info| {
                FieldValue::Object(Arc::new(ConnectionPageInfoObject::new(
                    page_info.start_cursor.clone(),
                    page_info.end_cursor.clone(),
                    page_info.has_next_page,
                    page_info.has_previous_page,
                )))
            }),
            _ => Ok(None),
        }
    }
}

/// `SiteConfigurationChange` node.
#[derive(Debug, Clone, PartialEq)]
pub struct SiteConfigurationChangeObject {
    root: ChangeNodeRoot,
}

impl SiteConfigurationChangeObject {
    pub fn new(root: ChangeNodeRoot) -> Self {
        Self { root }
    }
}

impl ObjectValue for SiteConfigurationChangeObject {
    fn type_name(&self) -> &'static str {
        "SiteConfigurationChange"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "id" => Ok(Some(FieldValue::Json(Json::Str(self.root.node.id())))),
            "diff" => Ok(Some(FieldValue::Json(Json::Str(self.root.node.diff())))),
            "createdAt" => Ok(Some(FieldValue::Json(Json::Str(
                self.root.node.created_at(),
            )))),
            "updatedAt" => Ok(Some(FieldValue::Json(Json::Str(
                self.root.node.updated_at(),
            )))),
            "author" => match &self.root.author {
                None => Ok(None),
                Some(Err(message)) => Err(ExecError::new(message.clone())),
                Some(Ok(None)) => Ok(Some(FieldValue::Null)),
                Some(Ok(Some(user))) => Ok(Some(FieldValue::Object(Arc::new(
                    UserObject::from_data(user.clone()),
                )))),
            },
            _ => Ok(None),
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::site_config_change::SiteConfigRow;
    use crate::site_configuration_query::SiteConfigRow as LatestRow;
    use time::OffsetDateTime;

    struct Backend {
        admin: bool,
        raw: String,
    }

    #[async_trait]
    impl SiteConfigurationBackend for Backend {
        async fn check_current_user_is_site_admin(&self, _actor: Actor) -> Result<(), String> {
            if self.admin {
                Ok(())
            } else {
                Err("must be site admin".to_owned())
            }
        }

        async fn auditlog_expansion(&self, _actor: Actor) -> bool {
            false
        }

        async fn log_security_event(
            &self,
            _event: crate::site_configuration_query::SecurityEvent,
        ) -> Result<(), String> {
            Ok(())
        }

        async fn site_get_latest(&self) -> Result<LatestRow, String> {
            Ok(LatestRow { id: 3 })
        }

        fn raw_site(&self) -> String {
            self.raw.clone()
        }
    }

    struct Validator;

    #[async_trait]
    impl SiteConfigValidator for Validator {
        async fn validate_site(&self, contents: &str) -> Result<Vec<String>, String> {
            if contents.contains("bad") {
                Ok(vec!["bad value".to_owned()])
            } else {
                Ok(Vec::new())
            }
        }
    }

    struct Licenses;

    #[async_trait]
    impl LicenseInfoProvider for Licenses {
        async fn configured_license(&self) -> Result<Option<LicenseInfo>, String> {
            Ok(Some(LicenseInfo {
                tags: vec!["plan:enterprise".to_owned()],
                user_count: 10,
                expires_at: "2030-01-02T03:04:05Z".to_owned(),
            }))
        }
    }

    struct HistoryStore;

    #[async_trait]
    impl SiteConfigHistoryStore for HistoryStore {
        async fn get_site_configs(&self) -> Result<Vec<SiteConfigRow>, String> {
            Ok(vec![SiteConfigRow {
                id: 1,
                author_user_id: 2,
                contents: "{}".to_owned(),
                redacted_contents: Some("{}".to_owned()),
                created_at: OffsetDateTime::UNIX_EPOCH,
                updated_at: OffsetDateTime::UNIX_EPOCH,
            }])
        }

        async fn get_site_config_count(&self) -> Result<usize, String> {
            Ok(1)
        }
    }

    fn resolver() -> SiteConfigurationResolver {
        SiteConfigurationResolver {
            return_safe_configs_only: false,
        }
    }

    #[tokio::test]
    async fn non_admins_are_denied_every_field() {
        let backend = Backend {
            admin: false,
            raw: "{}".to_owned(),
        };
        let actor = Actor::Anonymous;
        assert!(
            validation_messages(&backend, &Validator, actor, &resolver())
                .await
                .is_err()
        );
        assert!(license_info(&backend, &Licenses, actor).await.is_err());
        let store = HistoryStore;
        assert!(history(
            &backend,
            &store,
            actor,
            None,
            ConnectionResolverOptions::default()
        )
        .await
        .is_err());
    }

    #[tokio::test]
    async fn admins_get_validation_messages() {
        let backend = Backend {
            admin: true,
            raw: r#"{"bad": true}"#.to_owned(),
        };
        let messages =
            validation_messages(&backend, &Validator, Actor::User { uid: 1 }, &resolver())
                .await
                .expect("messages");
        assert_eq!(messages, vec!["bad value".to_owned()]);
    }

    #[tokio::test]
    async fn admins_get_license_info() {
        let backend = Backend {
            admin: true,
            raw: "{}".to_owned(),
        };
        let info = license_info(&backend, &Licenses, Actor::User { uid: 1 })
            .await
            .expect("license")
            .expect("some");
        assert_eq!(info.user_count, 10);
        assert_eq!(info.expires_at, "2030-01-02T03:04:05Z");
    }

    #[tokio::test]
    async fn admins_get_history_connection() {
        let backend = Backend {
            admin: true,
            raw: "{}".to_owned(),
        };
        let store = HistoryStore;
        let connection = history(
            &backend,
            &store,
            Actor::User { uid: 1 },
            Some(ConnectionResolverArgs {
                first: Some(10),
                ..ConnectionResolverArgs::default()
            }),
            ConnectionResolverOptions::default(),
        )
        .await
        .expect("history");
        assert_eq!(connection.total_count().await.expect("count"), 1);
        assert_eq!(connection.nodes().await.expect("nodes").len(), 1);
    }
}
