#![forbid(unsafe_code)]

//! GitHub Apps GraphQL surface (M9.25c).
//!
//! Ports the read paths of `cmd/frontend/internal/githubapp/resolver.go` on
//! the vendored `githubapps.graphql` document (the SDL bytes are unchanged):
//!
//! - `Query.gitHubApps(domain:)`, `Query.gitHubApp(id:)`,
//!   `Query.gitHubAppByAppID(appID:, baseURL:)` and the
//!   `Query.node` kind `GitHubApp`, all behind
//!   `auth.CheckCurrentUserIsSiteAdmin`;
//! - `GitHubApp` fields (`id` Relay ID, `appID`, `name`, `domain`, `slug`,
//!   `baseURL`, `appURL`, `clientID`, `clientSecret`, `logo`, `createdAt`,
//!   `updatedAt`, `installations`);
//! - `Installation` (`id`, `url`, `account`) and its
//!   `externalServices(first:)` connection, matching the GitHub App ID /
//!   installation ID / base URL against the GitHub external services, exactly
//!   like `gitHubAppResolver.compute` (`resolver.go:285-334`).
//!
//! Data comes from the existing `github_apps`, `github_app_installs` and
//! `external_services` tables; no table, column or Redis key is added.
//!
//! Unported sub-surfaces return `Ok(None)` (the executor reports the field as
//! unported): `GitHubApp.webhook`, the `externalServices` `ExternalService`
//! fields beyond the base read columns, and the two mutations. See
//! `m9_25c_githubapps_and_chroma_overview.md` for the full list.
//!
//! The database is injected through [`GitHubAppsGraphqlDatabase`], a
//! synchronous seam in the same style as the batches resolver; the production
//! adapter ([`crate::github_apps_graphql_store`]) bridges to sqlx with
//! `tokio::task::block_in_place` + `Handle::block_on`.

use std::collections::HashMap;
use std::sync::Arc;

use base64::Engine as _;
use sg_graphql::exec::{Actor, Arguments, ExecError, FieldValue, ObjectValue};
use sg_graphql::json::Json;
use time::OffsetDateTime;

use crate::external_services::{
    marshal_external_service_id, redact_config, unique_code_host_identifier,
};
use crate::graphql_resolvers::{datetime_rfc3339, PageInfoObject};

/// `githubapp.gitHubAppIDKind` (`resolver.go:39`).
pub const GITHUB_APP_KIND: &str = "GitHubApp";

// ---------------------------------------------------------------------------
// Handle
// ---------------------------------------------------------------------------

pub type GitHubAppsHandle = Arc<dyn GitHubAppsGraphqlDatabase + Send + Sync>;

/// Wrapper so [`crate::graphql_resolvers::NestedPrefetch`] can keep its
/// `PartialEq`/`Eq` derive (handles compare by pointer).
#[derive(Clone)]
pub struct GitHubAppsHandleWrapper(pub GitHubAppsHandle);

impl std::fmt::Debug for GitHubAppsHandleWrapper {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("GitHubAppsHandle")
    }
}

impl PartialEq for GitHubAppsHandleWrapper {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}

impl Eq for GitHubAppsHandleWrapper {}

// ---------------------------------------------------------------------------
// Rows
// ---------------------------------------------------------------------------

/// One `github_apps` row, with `client_secret`/`private_key` already decrypted
/// (`gitHubAppsStore.decrypt`, `store.go:242-259`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GitHubAppData {
    pub id: i64,
    pub app_id: i64,
    pub name: String,
    /// Stored `domain` (`repos` / `batches`); `domain` output upper-cases it.
    pub domain: String,
    pub slug: String,
    pub base_url: String,
    pub app_url: String,
    pub client_id: String,
    pub client_secret: String,
    pub private_key: String,
    pub logo: String,
    pub webhook_id: Option<i64>,
    pub created_at: OffsetDateTime,
    pub updated_at: OffsetDateTime,
}

impl GitHubAppData {
    /// `types.GitHubAppDomain.ToGraphQL` (`types.go:102`).
    pub fn domain_graphql(&self) -> String {
        self.domain.to_uppercase()
    }
}

/// One `github_app_installs` row (`githubAppInstallColumns`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GitHubAppInstallationData {
    pub installation_id: i64,
    pub url: String,
    pub account_login: String,
    pub account_avatar_url: String,
    pub account_url: String,
    pub account_type: String,
}

/// One live `GITHUB` external service, as `ExternalServices().List` returns it.
/// `config` is the decrypted JSON when decryption succeeds (matching
/// `ParseEncryptableConfig`; decryption failures are skipped by the caller).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GitHubExternalServiceData {
    pub id: i64,
    pub kind: String,
    pub display_name: String,
    pub config: Option<String>,
    pub created_at: OffsetDateTime,
    pub updated_at: OffsetDateTime,
}

// ---------------------------------------------------------------------------
// Database seam
// ---------------------------------------------------------------------------

/// `db.GitHubApps()` / `db.ExternalServices()` / `db.Users()` read paths used
/// by the GitHub Apps resolver.
pub trait GitHubAppsGraphqlDatabase: Send + Sync {
    /// `users.site_admin` of the actor: `None` for anonymous or a missing
    /// user, `Some(true)` for internal actors.
    fn current_user_site_admin(&self, actor: Actor) -> Result<Option<bool>, String>;

    /// `gitHubAppsStore.List` (`store.go:436-442`): no `ORDER BY`.
    fn list_apps(&self, domain: Option<&str>) -> Result<Vec<GitHubAppData>, String>;

    /// `gitHubAppsStore.GetByID`: `None` when no row matches.
    fn get_app_by_id(&self, id: i64) -> Result<Option<GitHubAppData>, String>;

    /// `gitHubAppsStore.GetByAppID` (`app_id = ? AND trim(trailing '/' from
    /// base_url) = ?`): `None` when no row matches.
    fn get_app_by_app_id(
        &self,
        app_id: i64,
        base_url: &str,
    ) -> Result<Option<GitHubAppData>, String>;

    /// `gitHubAppsStore.GetInstallations`: all rows for the database app ID.
    fn get_installations(
        &self,
        app_id: i64,
    ) -> Result<Vec<GitHubAppInstallationData>, String>;

    /// `ExternalServices().List(ctx, {Kinds: [GITHUB]})`, ordered by `id DESC`.
    fn list_github_external_services(&self) -> Result<Vec<GitHubExternalServiceData>, String>;
}

// ---------------------------------------------------------------------------
// Authorization / ID helpers
// ---------------------------------------------------------------------------

/// `auth.CheckCurrentUserIsSiteAdmin` (`site_admin.go:16-32`).
pub fn check_current_user_is_site_admin(
    db: &dyn GitHubAppsGraphqlDatabase,
    actor: Actor,
) -> Result<(), String> {
    if actor.is_internal() {
        return Ok(());
    }
    match db.current_user_site_admin(actor)? {
        None => Err("not authenticated".to_owned()),
        Some(false) => Err("must be site admin".to_owned()),
        Some(true) => Ok(()),
    }
}

/// `MarshalGitHubAppID` (`resolver.go:42-44`).
pub fn marshal_github_app_id(id: i64) -> String {
    base64::engine::general_purpose::URL_SAFE.encode(format!("GitHubApp:{id}"))
}

/// `UnmarshalGitHubAppID` (`resolver.go:47-55`): kind validation first, then
/// the JSON integer spec.
pub fn unmarshal_github_app_id(raw: &str) -> Result<i64, String> {
    let decoded = base64::engine::general_purpose::URL_SAFE.decode(raw);
    let kind = decoded
        .as_ref()
        .ok()
        .map(|bytes| String::from_utf8_lossy(bytes).into_owned())
        .and_then(|text| text.split_once(':').map(|(kind, _)| kind.to_owned()))
        .unwrap_or_default();
    if kind != GITHUB_APP_KIND {
        return Err(format!(
            "expected graph ID to have kind \"GitHubApp\"; got \"{kind}\""
        ));
    }
    let text = String::from_utf8_lossy(decoded.as_ref().expect("checked above")).into_owned();
    let Some((_, spec)) = text.split_once(':') else {
        return Err("invalid graphql.ID".to_owned());
    };
    serde_json::from_str::<i64>(spec).map_err(|error| error.to_string())
}

/// `ErrNoGitHubAppFound.Error()` (`store.go:30-32`).
pub fn no_app_matching(criteria: &str) -> String {
    format!("no app exists matching criteria: '{criteria}'")
}

/// `parseDomain` (`resolver.go:116-130`): the case-insensitive domain enum.
fn parse_domain(raw: Option<&str>) -> Result<Option<&'static str>, String> {
    let Some(raw) = raw else {
        return Ok(None);
    };
    match raw.to_uppercase().as_str() {
        "REPOS" => Ok(Some("repos")),
        "BATCHES" => Ok(Some("batches")),
        _ => Err(format!("unknown domain \"{raw}\"")),
    }
}

// ---------------------------------------------------------------------------
// Query root fields
// ---------------------------------------------------------------------------

/// `GitHubAppsResolver` root fields: only present when the optional resolver
/// is registered.
pub fn root_field(
    db: &GitHubAppsHandle,
    field: &str,
    arguments: &Arguments,
    actor: Actor,
) -> Result<Option<FieldValue>, ExecError> {
    let value = match field {
        "gitHubApps" => git_hub_apps(db, arguments, actor)?,
        "gitHubApp" => git_hub_app(db, arguments, actor)?,
        "gitHubAppByAppID" => git_hub_app_by_app_id(db, arguments, actor)?,
        _ => return Ok(None),
    };
    Ok(Some(value))
}

/// Whether `field` is owned by this surface.
pub fn is_root_field(field: &str) -> bool {
    matches!(field, "gitHubApps" | "gitHubApp" | "gitHubAppByAppID")
}

fn git_hub_apps(
    db: &GitHubAppsHandle,
    arguments: &Arguments,
    actor: Actor,
) -> Result<FieldValue, ExecError> {
    check_current_user_is_site_admin(db.as_ref(), actor).map_err(ExecError::new)?;
    let domain = parse_domain(arguments.string("domain")).map_err(ExecError::new)?;
    let apps = db.list_apps(domain).map_err(ExecError::new)?;
    Ok(FieldValue::Object(Arc::new(GitHubAppConnectionObject::new(
        apps, db.clone(),
    ))))
}

fn git_hub_app(
    db: &GitHubAppsHandle,
    arguments: &Arguments,
    actor: Actor,
) -> Result<FieldValue, ExecError> {
    check_current_user_is_site_admin(db.as_ref(), actor).map_err(ExecError::new)?;
    let raw = arguments.string("id").unwrap_or_default();
    let id = unmarshal_github_app_id(raw).map_err(ExecError::new)?;
    let app = db
        .get_app_by_id(id)
        .map_err(ExecError::new)?
        .ok_or_else(|| ExecError::new(no_app_matching(&format!("id = {id}"))))?;
    Ok(FieldValue::Object(Arc::new(GitHubAppObject::new(app, db.clone()))))
}

fn git_hub_app_by_app_id(
    db: &GitHubAppsHandle,
    arguments: &Arguments,
    actor: Actor,
) -> Result<FieldValue, ExecError> {
    check_current_user_is_site_admin(db.as_ref(), actor).map_err(ExecError::new)?;
    let app_id = arguments.i64("appID").unwrap_or(0);
    let base_url = arguments.string("baseURL").unwrap_or_default().to_owned();
    let app = db
        .get_app_by_app_id(app_id, &base_url)
        .map_err(ExecError::new)?
        .ok_or_else(|| {
            ExecError::new(no_app_matching(&format!(
                "app_id = {app_id} AND trim(trailing '/' from base_url) = {}",
                base_url.trim_end_matches('/')
            )))
        })?;
    Ok(FieldValue::Object(Arc::new(GitHubAppObject::new(app, db.clone()))))
}

/// `Query.node` kind `GitHubApp`.
pub fn node_field(
    db: &GitHubAppsHandle,
    kind: &str,
    id: &str,
    actor: Actor,
) -> Result<Option<FieldValue>, ExecError> {
    if kind != GITHUB_APP_KIND {
        return Ok(None);
    }
    check_current_user_is_site_admin(db.as_ref(), actor).map_err(ExecError::new)?;
    let app_id = unmarshal_github_app_id(id).map_err(ExecError::new)?;
    let app = db
        .get_app_by_id(app_id)
        .map_err(ExecError::new)?
        .ok_or_else(|| ExecError::new(no_app_matching(&format!("id = {app_id}"))))?;
    Ok(Some(FieldValue::Object(Arc::new(GitHubAppObject::new(
        app,
        db.clone(),
    )))))
}

/// Whether `kind` is the GitHub App node kind.
pub fn is_node_kind(kind: &str) -> bool {
    kind == GITHUB_APP_KIND
}

// ---------------------------------------------------------------------------
// Objects
// ---------------------------------------------------------------------------

pub struct GitHubAppConnectionObject {
    apps: Vec<GitHubAppData>,
    db: GitHubAppsHandle,
}

impl GitHubAppConnectionObject {
    pub fn new(apps: Vec<GitHubAppData>, db: GitHubAppsHandle) -> Self {
        Self { apps, db }
    }
}

impl ObjectValue for GitHubAppConnectionObject {
    fn type_name(&self) -> &'static str {
        "GitHubAppConnection"
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
                    .apps
                    .iter()
                    .map(|app| {
                        FieldValue::Object(Arc::new(GitHubAppObject::new(
                            app.clone(),
                            self.db.clone(),
                        )))
                    })
                    .collect(),
                element_type: Some("GitHubApp"),
            })),
            "totalCount" => Ok(Some(FieldValue::Json(Json::Int(self.apps.len() as i64)))),
            _ => Ok(None),
        }
    }
}

pub struct GitHubAppObject {
    app: GitHubAppData,
    db: GitHubAppsHandle,
}

impl GitHubAppObject {
    pub fn new(app: GitHubAppData, db: GitHubAppsHandle) -> Self {
        Self { app, db }
    }
}

impl ObjectValue for GitHubAppObject {
    fn type_name(&self) -> &'static str {
        "GitHubApp"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "id" => Ok(Some(FieldValue::Json(Json::Str(marshal_github_app_id(
                self.app.id,
            ))))),
            "appID" => Ok(Some(FieldValue::Json(Json::Int(self.app.app_id)))),
            "name" => Ok(Some(FieldValue::Json(Json::Str(self.app.name.clone())))),
            "domain" => Ok(Some(FieldValue::Json(Json::Str(
                self.app.domain_graphql(),
            )))),
            "slug" => Ok(Some(FieldValue::Json(Json::Str(self.app.slug.clone())))),
            "baseURL" => Ok(Some(FieldValue::Json(Json::Str(
                self.app.base_url.clone(),
            )))),
            "appURL" => Ok(Some(FieldValue::Json(Json::Str(
                self.app.app_url.clone(),
            )))),
            "clientID" => Ok(Some(FieldValue::Json(Json::Str(
                self.app.client_id.clone(),
            )))),
            "clientSecret" => Ok(Some(FieldValue::Json(Json::Str(
                self.app.client_secret.clone(),
            )))),
            "logo" => Ok(Some(FieldValue::Json(Json::Str(self.app.logo.clone())))),
            "createdAt" => Ok(Some(FieldValue::Json(Json::Str(datetime_rfc3339(
                self.app.created_at,
            ))))),
            "updatedAt" => Ok(Some(FieldValue::Json(Json::Str(datetime_rfc3339(
                self.app.updated_at,
            ))))),
            "installations" => {
                let installations = self
                    .db
                    .get_installations(self.app.id)
                    .map_err(ExecError::new)?;
                let external_services = self
                    .db
                    .list_github_external_services()
                    .map_err(ExecError::new)?;
                Ok(Some(FieldValue::List {
                    items: installations
                        .into_iter()
                        .map(|install| {
                            let matched = match_external_services(
                                &self.app,
                                &install,
                                &external_services,
                            );
                            FieldValue::Object(Arc::new(InstallationObject {
                                install,
                                matched,
                            }))
                        })
                        .collect(),
                    element_type: Some("Installation"),
                }))
            }
            // `Webhook` needs the outbound-webhook store and the keyring.
            "webhook" => Ok(None),
            _ => Ok(None),
        }
    }
}

/// `gitHubAppResolver.compute`'s matching loop (`resolver.go:305-317`).
fn match_external_services(
    app: &GitHubAppData,
    install: &GitHubAppInstallationData,
    services: &[GitHubExternalServiceData],
) -> Vec<GitHubExternalServiceData> {
    let mut matched = Vec::new();
    for service in services {
        let Some(config) = service.config.as_deref() else {
            continue;
        };
        let Ok(sg_jsonx::JsonValue::Object(value)) = crate::external_services::parse_config(config)
        else {
            continue;
        };
        let Some(sg_jsonx::JsonValue::Object(details)) = value.get("gitHubAppDetails") else {
            continue;
        };
        let url = match value.get("url") {
            Some(sg_jsonx::JsonValue::String(url)) => url.as_str(),
            _ => "",
        };
        if json_int(details.get("appID")) != Some(app.app_id) {
            continue;
        }
        if url != app.base_url {
            continue;
        }
        if json_int(details.get("installationID")) != Some(install.installation_id) {
            continue;
        }
        matched.push(service.clone());
    }
    matched
}

/// `json.Unmarshal` of a number into a Go `int` field: only integer literals
/// with an exact `int` range succeed.
fn json_int(value: Option<&sg_jsonx::JsonValue>) -> Option<i64> {
    match value {
        Some(sg_jsonx::JsonValue::Number(number)) => number.parse().ok(),
        _ => None,
    }
}

pub struct InstallationObject {
    install: GitHubAppInstallationData,
    matched: Vec<GitHubExternalServiceData>,
}

impl ObjectValue for InstallationObject {
    fn type_name(&self) -> &'static str {
        "Installation"
    }

    fn field(
        &self,
        field: &str,
        arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "id" => Ok(Some(FieldValue::Json(Json::Int(
                self.install.installation_id,
            )))),
            "url" => Ok(Some(FieldValue::Json(Json::Str(
                self.install.url.clone(),
            )))),
            "account" => Ok(Some(FieldValue::Object(Arc::new(GitHubAccountObject {
                login: self.install.account_login.clone(),
                avatar_url: self.install.account_avatar_url.clone(),
                url: self.install.account_url.clone(),
                kind: self.install.account_type.clone(),
            })))),
            "externalServices" => {
                let first = arguments.i64("first");
                Ok(Some(FieldValue::Object(Arc::new(
                    ComputedExternalServiceConnectionObject {
                        services: self.matched.clone(),
                        first,
                    },
                ))))
            }
            _ => Ok(None),
        }
    }
}

pub struct GitHubAccountObject {
    login: String,
    avatar_url: String,
    url: String,
    kind: String,
}

impl ObjectValue for GitHubAccountObject {
    fn type_name(&self) -> &'static str {
        "GitHubAccount"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "login" => Ok(Some(FieldValue::Json(Json::Str(self.login.clone())))),
            "avatarURL" => Ok(Some(FieldValue::Json(Json::Str(self.avatar_url.clone())))),
            "url" => Ok(Some(FieldValue::Json(Json::Str(self.url.clone())))),
            "type" => Ok(Some(FieldValue::Json(Json::Str(self.kind.clone())))),
            _ => Ok(None),
        }
    }
}

/// `ComputedExternalServiceConnectionResolver` (`external_services.go:468-500`).
pub struct ComputedExternalServiceConnectionObject {
    services: Vec<GitHubExternalServiceData>,
    first: Option<i64>,
}

impl ObjectValue for ComputedExternalServiceConnectionObject {
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
            "nodes" => {
                let limit = self
                    .first
                    .filter(|first| *first >= 0)
                    .map(|first| first as usize)
                    .unwrap_or(self.services.len());
                Ok(Some(FieldValue::List {
                    items: self
                        .services
                        .iter()
                        .take(limit)
                        .map(|service| {
                            FieldValue::Object(Arc::new(ComputedExternalServiceObject::new(
                                service.clone(),
                            )))
                        })
                        .collect(),
                    element_type: Some("ExternalService"),
                }))
            }
            "totalCount" => Ok(Some(FieldValue::Json(Json::Int(
                self.services.len() as i64,
            )))),
            "pageInfo" => {
                let has_next = matches!(self.first, Some(first) if self.services.len() as i64 >= first);
                Ok(Some(FieldValue::Object(Arc::new(PageInfoObject::new(
                    has_next, None,
                )))))
            }
            _ => Ok(None),
        }
    }
}

/// One `ExternalService` node of the computed connection, limited to the base
/// read columns (`external_service.go`). Relation counts, sync jobs, creator
/// and the connection check are left unported (`Ok(None)`).
pub struct ComputedExternalServiceObject {
    service: GitHubExternalServiceData,
}

impl ComputedExternalServiceObject {
    pub fn new(service: GitHubExternalServiceData) -> Self {
        Self { service }
    }
}

impl ObjectValue for ComputedExternalServiceObject {
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
                marshal_external_service_id(self.service.id),
            )))),
            "kind" => Ok(Some(FieldValue::Json(Json::Str(
                self.service.kind.clone(),
            )))),
            "displayName" => Ok(Some(FieldValue::Json(Json::Str(
                self.service.display_name.clone(),
            )))),
            "config" => match self.service.config.as_deref() {
                Some(raw) if !raw.is_empty() => Ok(Some(FieldValue::Json(Json::Str(
                    redact_config(&self.service.kind, raw).map_err(ExecError::new)?,
                )))),
                Some(_) => Ok(Some(FieldValue::Json(Json::Str(String::new())))),
                None => Ok(None),
            },
            "url" => match self.service.config.as_deref() {
                Some(raw) => Ok(Some(FieldValue::Json(Json::Str(
                    unique_code_host_identifier(&self.service.kind, raw)
                        .map_err(ExecError::new)?,
                )))),
                None => Ok(None),
            },
            "createdAt" => Ok(Some(FieldValue::Json(Json::Str(datetime_rfc3339(
                self.service.created_at,
            ))))),
            "updatedAt" => Ok(Some(FieldValue::Json(Json::Str(datetime_rfc3339(
                self.service.updated_at,
            ))))),
            _ => Ok(None),
        }
    }
}

/// `NestedPrefetch` uses `HashMap`-free lookups for this surface; kept for
/// symmetry with the other optional resolvers.
pub fn prefetch() -> HashMap<String, ()> {
    HashMap::new()
}
