#![forbid(unsafe_code)]

//! External-account GraphQL surface (M9.8ka):
//! `cmd/frontend/graphqlbackend/{external_accounts.go,external_account.go,
//! external_account_data_resolver.go}`.
//!
//! Ported:
//! - `User.externalAccounts(first: Int)` with the baseline's permission check
//!   (`auth.CheckSiteAdminOrSameUser`), id-ascending order, `totalCount` and
//!   the +1 next-page probe (the probe row is **not** trimmed: the baseline's
//!   `Nodes` returns every fetched row).
//! - `externalAccountByID` / `Query.node(kind: "ExternalAccount")` with the
//!   Relay id (kind is **not** validated, matching `unmarshalExternalAccountID`)
//!   and the same permission check.
//! - Node fields `id`/`user`/`serviceType`/`serviceID`/`clientID`/`accountID`/
//!   `createdAt`/`updatedAt`/`refreshURL` (always `null`, the baseline TODO)/
//!   `publicAccountData`.
//! - `publicAccountDataFromJSON`: look the provider up by
//!   `account.ServiceType`; an unknown type produces the baseline's exact
//!   `cannot find authorization provider for the external account, service
//!   type: %s` error. The GraphQL `publicAccountData` field swallows provider
//!   errors and renders `null`, exactly like `ExternalAccount.PublicAccountData`.
//! - `CodySubscription`: non-dotcom guard and the service-type/service-id
//!   mismatch returning `null`; the matching dotcom subscription lookup is an
//!   explicit not-ported seam (see the overview §4).
//!
//! Not ported: `Site.externalAccounts`, `AddExternalAccount`/
//! `DeleteExternalAccount`, `ExternalAccount.accountData` (raw decrypted JSON)
//! and the code-host/OIDC provider `ExternalAccountInfo` parsers. The
//! production provider registry only registers `saml`, `http-header` and
//! `builtin`, so other service types report the baseline's unknown-provider
//! error.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use base64::Engine as _;
use sg_config::SiteConfiguration;
use sg_encryption::Key;
use sg_graphql::exec::{Actor, Arguments, ExecError, FieldValue, ObjectValue, RootField};
use sg_graphql::json::Json;
use time::OffsetDateTime;

use crate::graphql_resolvers::{
    datetime_rfc3339, unmarshal_relay_id, PageInfoObject, UserData, UserKey, UserLookupStore,
    UserObject, MUST_BE_SITE_ADMIN_OR_SAME_USER,
};
use crate::saml_auth::{
    ReqwestSamlHttpClient, SamlAccountData, SamlEncryptableData, SamlProvider, SamlProviderSet,
};

/// One `user_external_accounts` row as the GraphQL node reads it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExternalAccountRow {
    pub id: i32,
    pub user_id: i32,
    pub service_type: String,
    pub service_id: String,
    pub client_id: String,
    pub account_id: String,
    /// `account_data` as stored (possibly ciphertext). `None` is SQL NULL.
    pub account_data: Option<String>,
    pub encryption_key_id: String,
    pub created_at: OffsetDateTime,
    pub updated_at: OffsetDateTime,
}

/// `extsvc.PublicAccountData` (`internal/extsvc/types.go:54-59`).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PublicAccountData {
    pub display_name: String,
    pub login: String,
    pub url: String,
}

/// The read-only store seam for `user_external_accounts`.
#[async_trait]
pub trait ExternalAccountsStore: Send + Sync {
    /// `List` with only `UserID` set (id ascending, live rows).
    async fn list_for_user(&self, user_id: i32) -> Result<Vec<ExternalAccountRow>, String>;
    /// `Count` with only `UserID` set.
    async fn count_for_user(&self, user_id: i32) -> Result<i64, String>;
    /// `Get(id)`: live row or `None`.
    async fn get_by_id(&self, id: i32) -> Result<Option<ExternalAccountRow>, String>;
}

/// One `providers.Provider`'s `ExternalAccountInfo` seam.
#[async_trait]
pub trait ExternalAccountInfoProvider: Send + Sync {
    fn service_type(&self) -> &str;
    async fn external_account_info(
        &self,
        account: &ExternalAccountRow,
        key: Option<&dyn Key>,
    ) -> Result<Option<PublicAccountData>, String>;
}

/// `providers.GetProviderbyServiceType`: the first registered provider whose
/// `ServiceType` matches.
#[derive(Default, Clone)]
pub struct ExternalAccountProviderRegistry {
    providers: Vec<Arc<dyn ExternalAccountInfoProvider>>,
}

impl ExternalAccountProviderRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register(&mut self, provider: Arc<dyn ExternalAccountInfoProvider>) {
        self.providers.push(provider);
    }

    pub fn get(&self, service_type: &str) -> Option<&dyn ExternalAccountInfoProvider> {
        self.providers
            .iter()
            .find(|provider| provider.service_type() == service_type)
            .map(|provider| provider.as_ref())
    }

    pub fn is_empty(&self) -> bool {
        self.providers.is_empty()
    }
}

impl std::fmt::Debug for ExternalAccountProviderRegistry {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ExternalAccountProviderRegistry")
            .field("providers", &self.providers.len())
            .finish()
    }
}

/// `publicAccountDataFromJSON`
/// (`external_account_data_resolver.go:25-33`).
pub async fn public_account_data_from_json(
    registry: &ExternalAccountProviderRegistry,
    account: &ExternalAccountRow,
    key: Option<&dyn Key>,
) -> Result<Option<PublicAccountData>, String> {
    let Some(provider) = registry.get(&account.service_type) else {
        return Err(format!(
            "cannot find authorization provider for the external account, service type: {}",
            account.service_type
        ));
    };
    provider.external_account_info(account, key).await
}

/// The result of prefetching one `User.externalAccounts(first:)` field.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExternalAccountsLookup {
    Found(ExternalAccountsConnectionData),
    Error(String),
}

/// `externalAccountConnectionResolver`'s cached values.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExternalAccountsConnectionData {
    /// The rows `Nodes` returns. With `first: n` the baseline fetches `n+1`
    /// rows for next-page detection and returns all of them, so this holds
    /// `n+1` rows when another page exists.
    pub nodes: Vec<ExternalAccountNodeData>,
    pub total_count: i64,
    pub has_next_page: bool,
}

/// One `ExternalAccount` node.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExternalAccountNodeData {
    pub row: ExternalAccountRow,
    /// `ExternalAccount.user`. Filled from the connection's owner or the
    /// `Query.node` user lookup.
    pub user: Option<UserData>,
    /// `PublicAccountData`, with provider errors already swallowed (the
    /// baseline returns `(nil, nil)` on resolver errors).
    pub public_account_data: Option<PublicAccountData>,
}

/// The result of prefetching one `Query.node(kind: "ExternalAccount")`.
#[allow(clippy::large_enum_variant)] // `Found` is the common path; mirrors the baseline's (node, error) pair.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExternalAccountNodeLookup {
    Found(ExternalAccountNodeData),
    Error(String),
}

/// `CodySubscription`'s observable outcomes in this slice.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CodySubscriptionOutcome {
    /// `!dotcom`: the baseline's `this feature is only available on
    /// sourcegraph.com` error.
    Unavailable,
    /// Dotcom but not the SAML/openidconnect account: `(nil, nil)`.
    Mismatch,
    /// The matching dotcom account: subscription lookup not ported.
    NotPorted,
}

/// `dotcom.SourcegraphDotComMode()` plus `ssc.GetSAMSServiceID()`, reduced to
/// the values `ExternalAccount.CodySubscription` reads.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CodySubscriptionConfig {
    pub dotcom: bool,
    pub sams_service_id: String,
}

/// `ssc.GetSAMSServiceID()` (`cmd/frontend/internal/ssc/ssc.go:143-162`).
pub fn sams_service_id(site: &SiteConfiguration) -> String {
    let raw = site.raw();
    if let Some(origin) = raw
        .pointer("/dotcom/codyProConfig/samsBackendOrigin")
        .and_then(serde_json::Value::as_str)
    {
        return origin.to_owned();
    }
    match raw
        .get("ssc.samsHostName")
        .and_then(serde_json::Value::as_str)
    {
        Some(host) if !host.is_empty() => format!("https://{host}"),
        _ => "https://accounts.sourcegraph.com".to_owned(),
    }
}

/// `externalAccountResolver.CodySubscription` (`external_account.go:60-79`).
pub fn cody_subscription_outcome(
    config: &CodySubscriptionConfig,
    service_type: &str,
    service_id: &str,
) -> CodySubscriptionOutcome {
    if !config.dotcom {
        return CodySubscriptionOutcome::Unavailable;
    }
    if service_type != "openidconnect" || service_id != config.sams_service_id {
        return CodySubscriptionOutcome::Mismatch;
    }
    CodySubscriptionOutcome::NotPorted
}

/// `gqlutil.ConnectionArgs`'s single `first` argument.
pub fn first_argument(arguments: &Arguments) -> Option<i32> {
    arguments
        .i64("first")
        .and_then(|first| i32::try_from(first).ok())
}

/// `auth.CheckSiteAdminOrSameUserFromActor`: internal actors and the subject
/// user pass; otherwise the viewer must be a site admin.
pub fn can_view_external_account(
    actor: Actor,
    viewer_site_admin: bool,
    subject_user_id: i32,
) -> bool {
    match actor {
        Actor::Internal => true,
        Actor::User { uid } if uid == subject_user_id => true,
        _ => viewer_site_admin,
    }
}

/// `relay.MarshalID("ExternalAccount", id)`.
pub fn marshal_external_account_id(id: i32) -> String {
    base64::engine::general_purpose::URL_SAFE.encode(format!("ExternalAccount:{id}"))
}

async fn node_data(
    row: ExternalAccountRow,
    user: Option<UserData>,
    registry: &ExternalAccountProviderRegistry,
    key: Option<&dyn Key>,
) -> ExternalAccountNodeData {
    // `ExternalAccount.PublicAccountData` returns `(nil, nil)` when the
    // provider lookup or `ExternalAccountInfo` fails.
    let public_account_data = public_account_data_from_json(registry, &row, key)
        .await
        .unwrap_or(None);
    ExternalAccountNodeData {
        row,
        user,
        public_account_data,
    }
}

/// `User.externalAccounts`'s connection resolution.
pub async fn external_accounts_connection(
    store: &dyn ExternalAccountsStore,
    registry: &ExternalAccountProviderRegistry,
    key: Option<Arc<dyn Key>>,
    user_id: i32,
    first: Option<i32>,
) -> ExternalAccountsLookup {
    let rows = match store.list_for_user(user_id).await {
        Ok(rows) => rows,
        Err(error) => return ExternalAccountsLookup::Error(error),
    };
    let total_count = match store.count_for_user(user_id).await {
        Ok(count) => count,
        Err(error) => return ExternalAccountsLookup::Error(error),
    };

    // `compute`: `opt2.Limit++` so the extra row reveals the next page.
    // Postgres rejects negative limits; `first == -1` becomes `LIMIT 0` in the
    // baseline, smaller values error there (see the overview §4).
    let probe = match first {
        None => rows.len(),
        Some(first) => {
            let probe = i64::from(first) + 1;
            if probe < 0 {
                return ExternalAccountsLookup::Error("LIMIT must not be negative".to_owned());
            }
            usize::try_from(probe).unwrap_or(usize::MAX)
        }
    };
    let mut nodes = Vec::with_capacity(rows.len().min(probe));
    for row in rows.into_iter().take(probe) {
        nodes.push(node_data(row, None, registry, key.as_deref()).await);
    }
    let has_next_page = match first {
        None => false,
        Some(first) => i64::try_from(nodes.len()).unwrap_or(i64::MAX) > i64::from(first),
    };

    ExternalAccountsLookup::Found(ExternalAccountsConnectionData {
        nodes,
        total_count,
        has_next_page,
    })
}

/// Prefetch one `Query.node(kind: "ExternalAccount")` lookup.
pub async fn external_account_node_by_id(
    store: &dyn ExternalAccountsStore,
    registry: &ExternalAccountProviderRegistry,
    key: Option<Arc<dyn Key>>,
    users: Option<&dyn UserLookupStore>,
    id: i32,
    actor: Actor,
    viewer_site_admin: bool,
) -> ExternalAccountNodeLookup {
    let row = match store.get_by_id(id).await {
        Ok(Some(row)) => row,
        Ok(None) => {
            // `userExternalAccountNotFoundError.Error()`:
            // `fmt.Sprintf("user external account not found: %v", args)`.
            return ExternalAccountNodeLookup::Error(format!(
                "user external account not found: [{id}]"
            ));
        }
        Err(error) => return ExternalAccountNodeLookup::Error(error),
    };
    if !can_view_external_account(actor, viewer_site_admin, row.user_id) {
        return ExternalAccountNodeLookup::Error(MUST_BE_SITE_ADMIN_OR_SAME_USER.to_owned());
    }
    let user = match users {
        Some(users) => match users.lookup(&UserKey::DatabaseId(row.user_id)).await {
            Ok(Some(user)) => Some(user),
            _ => None,
        },
        None => None,
    };
    ExternalAccountNodeLookup::Found(node_data(row, user, registry, key.as_deref()).await)
}

/// Prefetch every selected `node(id:)` of kind `ExternalAccount`.
pub async fn prefetch_external_account_nodes(
    store: &dyn ExternalAccountsStore,
    registry: &ExternalAccountProviderRegistry,
    key: Option<Arc<dyn Key>>,
    users: Option<&dyn UserLookupStore>,
    fields: &[RootField],
    actor: Actor,
    viewer_site_admin: bool,
) -> HashMap<i32, ExternalAccountNodeLookup> {
    let mut lookups = HashMap::new();
    for field in fields {
        if field.name != "node" {
            continue;
        }
        let Some(id) = field.arguments.string("id") else {
            continue;
        };
        let Some(kind) = crate::graphql_resolvers::relay_kind(id) else {
            continue;
        };
        if kind != "ExternalAccount" {
            continue;
        }
        let Ok(node_id) = unmarshal_relay_id(id) else {
            // `QueryRoot::node` re-parses and reports the same error.
            continue;
        };
        if lookups.contains_key(&node_id) {
            continue;
        }
        let lookup = external_account_node_by_id(
            store,
            registry,
            key.clone(),
            users,
            node_id,
            actor,
            viewer_site_admin,
        )
        .await;
        lookups.insert(node_id, lookup);
    }
    lookups
}

// ---------------------------------------------------------------------------
// Production store and providers
// ---------------------------------------------------------------------------

/// `PgPool`-backed [`ExternalAccountsStore`].
pub struct PgExternalAccountsStore {
    pool: sqlx::PgPool,
}

impl PgExternalAccountsStore {
    pub fn new(pool: sqlx::PgPool) -> Self {
        Self { pool }
    }
}

impl From<sg_db::user_external_accounts::ExternalAccountWithTimestamps> for ExternalAccountRow {
    fn from(row: sg_db::user_external_accounts::ExternalAccountWithTimestamps) -> Self {
        Self {
            id: row.id,
            user_id: row.user_id,
            service_type: row.service_type,
            service_id: row.service_id,
            client_id: row.client_id,
            account_id: row.account_id,
            account_data: row.account_data,
            encryption_key_id: row.encryption_key_id,
            created_at: row.created_at,
            updated_at: row.updated_at,
        }
    }
}

#[async_trait]
impl ExternalAccountsStore for PgExternalAccountsStore {
    async fn list_for_user(&self, user_id: i32) -> Result<Vec<ExternalAccountRow>, String> {
        sg_db::user_external_accounts::list_for_user_with_timestamps(&self.pool, user_id)
            .await
            .map(|rows| rows.into_iter().map(ExternalAccountRow::from).collect())
            .map_err(|error| error.to_string())
    }

    async fn count_for_user(&self, user_id: i32) -> Result<i64, String> {
        sg_db::user_external_accounts::count_for_user(&self.pool, user_id)
            .await
            .map_err(|error| error.to_string())
    }

    async fn get_by_id(&self, id: i32) -> Result<Option<ExternalAccountRow>, String> {
        sg_db::user_external_accounts::get_with_timestamps(&self.pool, id)
            .await
            .map(|row| row.map(ExternalAccountRow::from))
            .map_err(|error| error.to_string())
    }
}

/// `userpasswd.provider.ExternalAccountInfo`: always the baseline's error.
struct BuiltinProvider;

#[async_trait]
impl ExternalAccountInfoProvider for BuiltinProvider {
    fn service_type(&self) -> &str {
        "builtin"
    }

    async fn external_account_info(
        &self,
        _account: &ExternalAccountRow,
        _key: Option<&dyn Key>,
    ) -> Result<Option<PublicAccountData>, String> {
        Err("not an external account, cannot provide external account info".to_owned())
    }
}

/// `httpheader.provider.ExternalAccountInfo`: `DisplayName = AccountID`.
struct HttpHeaderProvider;

#[async_trait]
impl ExternalAccountInfoProvider for HttpHeaderProvider {
    fn service_type(&self) -> &str {
        "http-header"
    }

    async fn external_account_info(
        &self,
        account: &ExternalAccountRow,
        _key: Option<&dyn Key>,
    ) -> Result<Option<PublicAccountData>, String> {
        Ok(Some(PublicAccountData {
            display_name: account.account_id.clone(),
            login: String::new(),
            url: String::new(),
        }))
    }
}

/// `saml.provider.ExternalAccountInfo` (M9.8jx).
struct SamlExternalAccountProvider {
    provider: Arc<SamlProvider>,
}

fn saml_account_data(account: &ExternalAccountRow) -> SamlAccountData {
    SamlAccountData {
        auth_data: None,
        data: account.account_data.as_ref().map(|text| {
            let bytes = text.as_bytes().to_vec();
            if account.encryption_key_id.is_empty()
                || account.encryption_key_id == sg_encryption::UNMIGRATED_ENCRYPTION_KEY_ID
            {
                SamlEncryptableData::Unencrypted(bytes)
            } else {
                SamlEncryptableData::Encrypted {
                    cipher: bytes,
                    key_id: account.encryption_key_id.clone(),
                }
            }
        }),
    }
}

#[async_trait]
impl ExternalAccountInfoProvider for SamlExternalAccountProvider {
    fn service_type(&self) -> &str {
        self.provider.provider_type()
    }

    async fn external_account_info(
        &self,
        account: &ExternalAccountRow,
        key: Option<&dyn Key>,
    ) -> Result<Option<PublicAccountData>, String> {
        let public = self
            .provider
            .external_account_info(&saml_account_data(account), key)
            .await?;
        Ok(public.map(|data| PublicAccountData {
            display_name: data.display_name,
            login: String::new(),
            url: String::new(),
        }))
    }
}

/// Build the production registry from the live `auth.providers` snapshot,
/// mirroring `providers.GetProviderbyServiceType`'s first-match lookup.
///
/// Only `saml`, `http-header` and `builtin` are registered (see the module
/// docs); the remaining provider types report the unknown-provider error.
pub fn registry_from_site(site: &SiteConfiguration) -> ExternalAccountProviderRegistry {
    let mut registry = ExternalAccountProviderRegistry::new();
    let mut builtin = false;
    let mut http_header = false;
    let mut saml = false;
    // Built lazily: most sites have no SAML provider, and constructing the set
    // also constructs the HTTP client used by the provider.
    let mut saml_set: Option<SamlProviderSet> = None;
    if let Some(providers) = site
        .raw()
        .get("auth.providers")
        .and_then(serde_json::Value::as_array)
    {
        for entry in providers {
            match entry
                .get("type")
                .and_then(serde_json::Value::as_str)
                .unwrap_or_default()
            {
                "builtin" if !builtin => {
                    registry.register(Arc::new(BuiltinProvider));
                    builtin = true;
                }
                "http-header" if !http_header => {
                    registry.register(Arc::new(HttpHeaderProvider));
                    http_header = true;
                }
                "saml" if !saml => {
                    let set = saml_set.get_or_insert_with(|| {
                        SamlProviderSet::from_site(site, Arc::new(ReqwestSamlHttpClient::new()))
                    });
                    if let Some(provider) = set.providers().first() {
                        registry.register(Arc::new(SamlExternalAccountProvider {
                            provider: provider.clone(),
                        }));
                        saml = true;
                    }
                }
                _ => {}
            }
        }
    }
    registry
}

// ---------------------------------------------------------------------------
// GraphQL objects
// ---------------------------------------------------------------------------

/// `externalAccountConnectionResolver` (`external_accounts.go:75-121`).
pub struct ExternalAccountsConnectionObject {
    data: ExternalAccountsConnectionData,
    /// The user whose accounts these are; every node's `user` field points at
    /// it when the prefetched node has no explicit user.
    owner: Option<UserData>,
    viewer_site_admin: bool,
    nested: Arc<crate::graphql_resolvers::NestedPrefetch>,
}

impl ExternalAccountsConnectionObject {
    pub fn from_data(
        data: ExternalAccountsConnectionData,
        owner: Option<UserData>,
        viewer_site_admin: bool,
        nested: Arc<crate::graphql_resolvers::NestedPrefetch>,
    ) -> Self {
        Self {
            data,
            owner,
            viewer_site_admin,
            nested,
        }
    }
}

impl ObjectValue for ExternalAccountsConnectionObject {
    fn type_name(&self) -> &'static str {
        "ExternalAccountConnection"
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
                        FieldValue::Object(Arc::new(ExternalAccountObject::from_data(
                            node.clone(),
                            self.owner.clone(),
                            self.viewer_site_admin,
                            self.nested.clone(),
                        )))
                    })
                    .collect(),
                element_type: Some("ExternalAccount"),
            })),
            "totalCount" => Ok(Some(FieldValue::Json(Json::Int(self.data.total_count)))),
            "pageInfo" => Ok(Some(FieldValue::Object(Arc::new(PageInfoObject::new(
                self.data.has_next_page,
                None,
            ))))),
            _ => Ok(None),
        }
    }
}

/// `externalAccountResolver` (`external_account.go:18-140`).
pub struct ExternalAccountObject {
    data: ExternalAccountNodeData,
    viewer_site_admin: bool,
    nested: Arc<crate::graphql_resolvers::NestedPrefetch>,
}

impl ExternalAccountObject {
    pub fn from_data(
        mut data: ExternalAccountNodeData,
        owner: Option<UserData>,
        viewer_site_admin: bool,
        nested: Arc<crate::graphql_resolvers::NestedPrefetch>,
    ) -> Self {
        if data.user.is_none() {
            data.user = owner;
        }
        Self {
            data,
            viewer_site_admin,
            nested,
        }
    }
}

fn optional_string_field(value: &str) -> FieldValue {
    if value.is_empty() {
        FieldValue::Null
    } else {
        FieldValue::Json(Json::Str(value.to_owned()))
    }
}

impl ObjectValue for ExternalAccountObject {
    fn type_name(&self) -> &'static str {
        "ExternalAccount"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        let row = &self.data.row;
        match field {
            "id" => Ok(Some(FieldValue::Json(Json::Str(
                marshal_external_account_id(row.id),
            )))),
            "user" => Ok(Some(match &self.data.user {
                Some(user) => FieldValue::Object(Arc::new(
                    UserObject::from_data_as_viewer(user.clone(), self.viewer_site_admin)
                        .with_nested(self.nested.clone()),
                )),
                None => FieldValue::Null,
            })),
            "serviceType" => Ok(Some(FieldValue::Json(Json::Str(row.service_type.clone())))),
            "serviceID" => Ok(Some(FieldValue::Json(Json::Str(row.service_id.clone())))),
            "clientID" => Ok(Some(FieldValue::Json(Json::Str(row.client_id.clone())))),
            "accountID" => Ok(Some(FieldValue::Json(Json::Str(row.account_id.clone())))),
            "createdAt" => Ok(Some(FieldValue::Json(Json::Str(datetime_rfc3339(
                row.created_at,
            ))))),
            "updatedAt" => Ok(Some(FieldValue::Json(Json::Str(datetime_rfc3339(
                row.updated_at,
            ))))),
            // `RefreshURL` always returns nil (the baseline TODO).
            "refreshURL" => Ok(Some(FieldValue::Null)),
            // Not ported: the decrypted raw JSON value.
            "accountData" => Err(ExecError::new(
                "ExternalAccount.accountData is not ported (M9.8ka)",
            )),
            "publicAccountData" => Ok(Some(match &self.data.public_account_data {
                Some(data) => FieldValue::Object(Arc::new(PublicExternalAccountDataObject {
                    data: data.clone(),
                })),
                None => FieldValue::Null,
            })),
            "codySubscription" => {
                let Some(config) = &self.nested.external_account_cody else {
                    return Ok(None);
                };
                match cody_subscription_outcome(config, &row.service_type, &row.service_id) {
                    CodySubscriptionOutcome::Unavailable => Err(ExecError::new(
                        "this feature is only available on sourcegraph.com",
                    )),
                    CodySubscriptionOutcome::Mismatch => Ok(Some(FieldValue::Null)),
                    CodySubscriptionOutcome::NotPorted => Err(ExecError::new(
                        "CodySubscription for SAML accounts is not ported (M9.8ka)",
                    )),
                }
            }
            _ => Ok(None),
        }
    }
}

/// `externalAccountDataResolver` (`external_account_data_resolver.go:11-58`):
/// empty `DisplayName`/`Login`/`URL` render as `null`.
pub struct PublicExternalAccountDataObject {
    data: PublicAccountData,
}

impl ObjectValue for PublicExternalAccountDataObject {
    fn type_name(&self) -> &'static str {
        "PublicExternalAccountData"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "displayName" => Ok(Some(optional_string_field(&self.data.display_name))),
            "login" => Ok(Some(optional_string_field(&self.data.login))),
            "url" => Ok(Some(optional_string_field(&self.data.url))),
            _ => Ok(None),
        }
    }
}
