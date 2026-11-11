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

/// `database.ExternalAccountsListOptions` without the pagination fields.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ExternalAccountsListFilter {
    pub user_id: Option<i32>,
    pub service_type: Option<String>,
    pub service_id: Option<String>,
    pub client_id: Option<String>,
}

/// `extsvc.AccountSpec` for a store write.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExternalAccountSpec {
    pub service_type: String,
    pub service_id: String,
    pub client_id: String,
    pub account_id: String,
}

/// The store seam for `user_external_accounts`.
#[async_trait]
pub trait ExternalAccountsStore: Send + Sync {
    /// `List` with only `UserID` set (id ascending, live rows).
    async fn list_for_user(&self, user_id: i32) -> Result<Vec<ExternalAccountRow>, String>;
    /// `Count` with only `UserID` set.
    async fn count_for_user(&self, user_id: i32) -> Result<i64, String>;
    /// `Get(id)`: live row or `None`.
    async fn get_by_id(&self, id: i32) -> Result<Option<ExternalAccountRow>, String>;
    /// `List` with arbitrary filters; `limit` is the already-incremented
    /// connection probe (`None` means no `LIMIT`).
    async fn list_filtered(
        &self,
        filter: &ExternalAccountsListFilter,
        limit: Option<i64>,
    ) -> Result<Vec<ExternalAccountRow>, String>;
    /// `Count` with arbitrary filters (ignores pagination).
    async fn count_filtered(&self, filter: &ExternalAccountsListFilter) -> Result<i64, String>;
    /// `Delete(ExternalAccountsDeleteOptions{IDs: [id]})` (soft delete).
    async fn delete_by_id(&self, id: i32) -> Result<(), String>;
    /// `Upsert`: `auth_data`/`account_data` are plaintext; the production
    /// implementation encrypts them with `key`.
    async fn upsert_with_data(
        &self,
        user_id: i32,
        spec: &ExternalAccountSpec,
        auth_data: Option<&str>,
        account_data: Option<&str>,
        key: Option<&dyn Key>,
    ) -> Result<(), String>;
}

/// `authz/permssync.SchedulePermsSync`'s user-job seam.
#[async_trait]
pub trait PermsSyncScheduler: Send + Sync {
    /// One `ScheduleSyncOpts{UserIDs, Reason}` call. Errors are logged by the
    /// baseline and do not fail the mutation.
    async fn schedule(&self, user_ids: &[i32], reason: &str) -> Result<(), String>;
}

/// `featureflag.FromContext(ctx).GetBoolOr("disallow-user-external-account-deletion", false)`.
#[async_trait]
pub trait ExternalAccountFeatureFlags: Send + Sync {
    async fn disallow_user_external_account_deletion(
        &self,
        actor: Actor,
        viewer_site_admin: bool,
    ) -> bool;
}

/// `gerrit/externalaccount.AddGerritExternalAccount`'s network verification
/// seam (see the overview §4: not ported).
#[async_trait]
pub trait GerritAccountService: Send + Sync {
    async fn add(
        &self,
        store: &dyn ExternalAccountsStore,
        user_id: i32,
        service_id: &str,
        account_details: &str,
        key: Option<&dyn Key>,
    ) -> Result<(), String>;
}

/// `database.ReasonExternalAccountAdded` / `...Deleted`.
pub const REASON_EXTERNAL_ACCOUNT_ADDED: &str = "REASON_EXTERNAL_ACCOUNT_ADDED";
pub const REASON_EXTERNAL_ACCOUNT_DELETED: &str = "REASON_EXTERNAL_ACCOUNT_DELETED";

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
    /// `providers.GetProviderByConfigID`-visible `auth.providers` entries:
    /// `(type, config ID) -> CachedInfo().ClientID`.
    provider_configs: HashMap<(String, String), String>,
}

impl ExternalAccountProviderRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register(&mut self, provider: Arc<dyn ExternalAccountInfoProvider>) {
        self.providers.push(provider);
    }

    /// Register one `auth.providers` entry by its `ConfigID`.
    pub fn register_provider_config(
        &mut self,
        provider_type: impl Into<String>,
        config_id: impl Into<String>,
        client_id: impl Into<String>,
    ) {
        self.provider_configs
            .insert((provider_type.into(), config_id.into()), client_id.into());
    }

    /// `providers.GetProviderByConfigID(...)`: `None` is the baseline's
    /// missing-provider case.
    pub fn client_id_by_config_id(&self, provider_type: &str, config_id: &str) -> Option<&str> {
        self.provider_configs
            .get(&(provider_type.to_owned(), config_id.to_owned()))
            .map(String::as_str)
    }

    pub fn get(&self, service_type: &str) -> Option<&dyn ExternalAccountInfoProvider> {
        self.providers
            .iter()
            .find(|provider| provider.service_type() == service_type)
            .map(|provider| provider.as_ref())
    }

    pub fn is_empty(&self) -> bool {
        self.providers.is_empty() && self.provider_configs.is_empty()
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
    /// `ExternalAccount.accountData`: null, the still-serialized decrypted
    /// JSON value, or the permission/decryption/parse error.
    pub account_data: AccountDataLookup,
}

/// `ExternalAccount.accountData`'s prefetched outcome.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AccountDataLookup {
    /// `account.Data == nil` (SQL NULL or empty cipher with empty key ID).
    Null,
    /// The decrypted JSON text (validated in the prefetch).
    Json(String),
    /// The baseline resolver's error (permission, decryption or JSON parse).
    Error(String),
}

/// `ExternalAccount.accountData`'s `auth.CheckSiteAdminOrSameUser` /
/// `auth.CheckUserIsSiteAdmin` gate (`external_account.go:94-110`).
///
/// The GitHub/GitLab branch passes `actor.UID` as the subject user ID, so every
/// authenticated actor (including non-admins) passes; see the overview §1.4.
fn account_data_permission(
    actor: Actor,
    viewer_site_admin: bool,
    service_type: &str,
) -> Result<(), String> {
    if actor.is_internal() {
        return Ok(());
    }
    if service_type == "github" || service_type == "gitlab" {
        // `CheckSiteAdminOrSameUser(ctx, db, actor.UID)`: same-user always true.
        if actor.is_authenticated() {
            return Ok(());
        }
        return Err(MUST_BE_SITE_ADMIN_OR_SAME_USER.to_owned());
    }
    // `CheckUserIsSiteAdmin(ctx, db, actor.UID)`: anonymous UID 0 is not found.
    if !actor.is_authenticated() {
        return Err("not authenticated".to_owned());
    }
    if viewer_site_admin {
        Ok(())
    } else {
        Err("must be site admin".to_owned())
    }
}

async fn account_data_lookup(
    row: &ExternalAccountRow,
    key: Option<&dyn Key>,
    actor: Actor,
    viewer_site_admin: bool,
) -> AccountDataLookup {
    if let Err(message) = account_data_permission(actor, viewer_site_admin, &row.service_type) {
        return AccountDataLookup::Error(message);
    }
    let Some(cipher) = &row.account_data else {
        return AccountDataLookup::Null;
    };
    // `extsvc.NewEncryptedData`: an empty cipher with an empty key ID is nil.
    if cipher.is_empty() && row.encryption_key_id.is_empty() {
        return AccountDataLookup::Null;
    }
    let plaintext = match sg_encryption::maybe_decrypt(key, cipher, &row.encryption_key_id).await {
        Ok(plaintext) => plaintext,
        Err(error) => return AccountDataLookup::Error(error.to_string()),
    };
    // `JSONEncryptable[any].Decrypt`: `json.Unmarshal` into `any`.
    match serde_json::from_str::<serde_json::Value>(&plaintext) {
        Ok(_) => AccountDataLookup::Json(plaintext),
        Err(error) => AccountDataLookup::Error(error.to_string()),
    }
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
    actor: Actor,
    viewer_site_admin: bool,
) -> ExternalAccountNodeData {
    // `ExternalAccount.PublicAccountData` returns `(nil, nil)` when the
    // provider lookup or `ExternalAccountInfo` fails.
    let public_account_data = public_account_data_from_json(registry, &row, key)
        .await
        .unwrap_or(None);
    let account_data = account_data_lookup(&row, key, actor, viewer_site_admin).await;
    ExternalAccountNodeData {
        row,
        user,
        public_account_data,
        account_data,
    }
}

/// `User.externalAccounts`'s connection resolution.
pub async fn external_accounts_connection(
    store: &dyn ExternalAccountsStore,
    registry: &ExternalAccountProviderRegistry,
    key: Option<Arc<dyn Key>>,
    user_id: i32,
    first: Option<i32>,
    actor: Actor,
    viewer_site_admin: bool,
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
        nodes.push(
            node_data(
                row,
                None,
                registry,
                key.as_deref(),
                actor,
                viewer_site_admin,
            )
            .await,
        );
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
    ExternalAccountNodeLookup::Found(
        node_data(
            row,
            user,
            registry,
            key.as_deref(),
            actor,
            viewer_site_admin,
        )
        .await,
    )
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
// Site.externalAccounts
// ---------------------------------------------------------------------------

/// `Site.externalAccounts` arguments (`external_accounts.go:21-53`).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SiteExternalAccountsArgs {
    pub first: Option<i32>,
    pub user: Option<SiteExternalAccountsUser>,
    pub service_type: Option<String>,
    pub service_id: Option<String>,
    pub client_id: Option<String>,
}

/// The `user:` argument after `UnmarshalUserID` (`relay.UnmarshalSpec`).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum SiteExternalAccountsUser {
    Id(i32),
    Invalid(String),
}

impl SiteExternalAccountsArgs {
    pub fn from_arguments(arguments: &Arguments) -> Self {
        let user = arguments
            .string("user")
            .map(|id| match unmarshal_relay_id(id) {
                Ok(id) => SiteExternalAccountsUser::Id(id),
                Err(error) => SiteExternalAccountsUser::Invalid(error),
            });
        Self {
            first: first_argument(arguments),
            user,
            service_type: arguments.string("serviceType").map(str::to_owned),
            service_id: arguments.string("serviceID").map(str::to_owned),
            client_id: arguments.string("clientID").map(str::to_owned),
        }
    }

    fn filter(&self) -> ExternalAccountsListFilter {
        ExternalAccountsListFilter {
            user_id: match &self.user {
                Some(SiteExternalAccountsUser::Id(id)) => Some(*id),
                _ => None,
            },
            service_type: self.service_type.clone(),
            service_id: self.service_id.clone(),
            client_id: self.client_id.clone(),
        }
    }
}

/// `Site.externalAccounts`'s prefetched connection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SiteExternalAccountsLookup {
    Found(SiteExternalAccountsData),
    /// The resolver returned an error before constructing the connection
    /// (site-admin check or an invalid `user` ID).
    Error(String),
}

/// `externalAccountConnectionResolver`'s cached values for `Site`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SiteExternalAccountsData {
    pub nodes: Result<Vec<ExternalAccountNodeData>, String>,
    pub total_count: Result<i64, String>,
    /// The original `first` argument (used by `PageInfo`).
    pub first: Option<i32>,
}

/// `auth.CheckCurrentUserIsSiteAdmin` folded into the executor's actor.
fn can_list_all_external_accounts(actor: Actor, viewer_site_admin: bool) -> Result<(), String> {
    match actor {
        Actor::Internal => Ok(()),
        Actor::Anonymous => Err("not authenticated".to_owned()),
        Actor::User { .. } if viewer_site_admin => Ok(()),
        Actor::User { .. } => Err("must be site admin".to_owned()),
    }
}

/// `Site.externalAccounts`'s connection resolution.
pub async fn site_external_accounts_connection(
    store: &dyn ExternalAccountsStore,
    registry: &ExternalAccountProviderRegistry,
    key: Option<Arc<dyn Key>>,
    users: Option<&dyn UserLookupStore>,
    args: &SiteExternalAccountsArgs,
    actor: Actor,
    viewer_site_admin: bool,
) -> SiteExternalAccountsLookup {
    if let Err(message) = can_list_all_external_accounts(actor, viewer_site_admin) {
        return SiteExternalAccountsLookup::Error(message);
    }
    if let Some(SiteExternalAccountsUser::Invalid(message)) = &args.user {
        return SiteExternalAccountsLookup::Error(message.clone());
    }
    let filter = args.filter();
    // `compute`: `opt2.Limit++` so the extra row reveals the next page.
    let probe = args.first.map(|first| i64::from(first) + 1);
    let rows = match store.list_filtered(&filter, probe).await {
        Ok(rows) => rows,
        Err(error) => {
            return SiteExternalAccountsLookup::Found(SiteExternalAccountsData {
                nodes: Err(error),
                total_count: store.count_filtered(&filter).await,
                first: args.first,
            });
        }
    };
    let total_count = store.count_filtered(&filter).await;
    let mut nodes = Vec::with_capacity(rows.len());
    for row in rows {
        let user = match users {
            Some(users) => match users.lookup(&UserKey::DatabaseId(row.user_id)).await {
                Ok(Some(user)) => Some(user),
                _ => None,
            },
            None => None,
        };
        nodes.push(
            node_data(
                row,
                user,
                registry,
                key.as_deref(),
                actor,
                viewer_site_admin,
            )
            .await,
        );
    }
    SiteExternalAccountsLookup::Found(SiteExternalAccountsData {
        nodes: Ok(nodes),
        total_count,
        first: args.first,
    })
}

fn collect_site_external_account_selections(fields: &[RootField], out: &mut Vec<RootField>) {
    fn walk(selections: &[RootField], in_site: bool, out: &mut Vec<RootField>) {
        for field in selections {
            if in_site && field.name == "externalAccounts" {
                out.push(field.clone());
            }
            for (condition, sub_selections) in &field.type_conditions {
                walk(sub_selections, in_site || condition == "Site", out);
            }
            // Do not descend into a nested `site` field spuriously: only the
            // root `Query.site` and Site-typed fragments enable the audit.
            walk(&field.selections, in_site, out);
        }
    }

    fn walk_children(field: &RootField, in_site: bool, out: &mut Vec<RootField>) {
        for (condition, sub_selections) in &field.type_conditions {
            walk(sub_selections, in_site || condition == "Site", out);
        }
        walk(&field.selections, in_site, out);
    }

    let site_id = crate::graphql_resolvers::SiteObject::relay_id();
    for field in fields {
        match field.name.as_str() {
            "site" => walk_children(field, true, out),
            "settingsSubject" if field.arguments.string("id") == Some(site_id.as_str()) => {
                walk_children(field, true, out)
            }
            _ => walk_children(field, false, out),
        }
    }
}

/// Prefetch every selected `Site.externalAccounts` connection.
pub async fn prefetch_site_external_accounts(
    store: &dyn ExternalAccountsStore,
    registry: &ExternalAccountProviderRegistry,
    key: Option<Arc<dyn Key>>,
    users: Option<&dyn UserLookupStore>,
    fields: &[RootField],
    actor: Actor,
    viewer_site_admin: bool,
) -> HashMap<SiteExternalAccountsArgs, SiteExternalAccountsLookup> {
    let mut selections = Vec::new();
    collect_site_external_account_selections(fields, &mut selections);
    let mut lookups = HashMap::new();
    for selection in selections {
        let args = SiteExternalAccountsArgs::from_arguments(&selection.arguments);
        if lookups.contains_key(&args) {
            continue;
        }
        let lookup = site_external_accounts_connection(
            store,
            registry,
            key.clone(),
            users,
            &args,
            actor,
            viewer_site_admin,
        )
        .await;
        lookups.insert(args, lookup);
    }
    lookups
}

// ---------------------------------------------------------------------------
// AddExternalAccount / DeleteExternalAccount
// ---------------------------------------------------------------------------

/// `unsupported service type %q`'s Go-quoted string.
fn go_quote(value: &str) -> String {
    format!("{value:?}")
}

/// `accountDetailsBody` (`sourcegraphoperator/associate.go:15-20`).
#[derive(Debug, serde::Deserialize)]
struct SoapAccountDetails {
    #[serde(rename = "clientID", default)]
    client_id: String,
    #[serde(rename = "accountID", default)]
    account_id: String,
    #[serde(rename = "serviceAccount", default)]
    service_account: bool,
}

/// `sourcegraphoperator.ExternalAccountData` (`MarshalAccountData`).
#[derive(Debug, serde::Serialize)]
struct SoapAccountData {
    #[serde(rename = "serviceAccount")]
    service_account: bool,
}

async fn add_soap_external_account(
    store: &dyn ExternalAccountsStore,
    registry: &ExternalAccountProviderRegistry,
    key: Option<&dyn Key>,
    viewer_site_admin: bool,
    user_id: i32,
    service_id: &str,
    account_details: &str,
) -> Result<(), String> {
    // 🚨 SECURITY: Caller must be a site admin.
    if !viewer_site_admin {
        return Err("must be site admin".to_owned());
    }
    let Some(client_id) = registry.client_id_by_config_id("sourcegraph-operator", service_id)
    else {
        return Err("provider does not exist".to_owned());
    };
    if account_details.is_empty() {
        return Err("account details are required".to_owned());
    }
    let details: SoapAccountDetails = serde_json::from_str(account_details)
        .map_err(|error| format!("invalid account details: {error}"))?;
    // The service ID was checked by the provider lookup; additionally check
    // the client ID.
    if details.client_id != client_id {
        return Err(format!(
            "unknown client ID {}",
            go_quote(&details.client_id)
        ));
    }
    let count = store
        .count_filtered(&ExternalAccountsListFilter {
            user_id: Some(user_id),
            service_type: Some("sourcegraph-operator".to_owned()),
            service_id: None,
            client_id: None,
        })
        .await
        .map_err(|error| {
            format!("failed to check for an existing Sourcegraph Operator accounts: {error}")
        })?;
    if count > 0 {
        return Err("user already has an associated Sourcegraph Operator account".to_owned());
    }
    let account_data = serde_json::to_string(&SoapAccountData {
        service_account: details.service_account,
    })
    .map_err(|error| format!("failed to marshal account data: {error}"))?;
    let spec = ExternalAccountSpec {
        service_type: "sourcegraph-operator".to_owned(),
        service_id: service_id.to_owned(),
        client_id: details.client_id,
        account_id: details.account_id,
    };
    store
        .upsert_with_data(user_id, &spec, None, Some(&account_data), key)
        .await
        .map_err(|error| {
            format!("failed to associate user with Sourcegraph Operator provider: {error}")
        })
}

async fn schedule_perms_sync(
    scheduler: Option<&dyn PermsSyncScheduler>,
    user_ids: &[i32],
    reason: &str,
) {
    if let Some(scheduler) = scheduler {
        // The baseline logs scheduling failures and continues.
        let _ = scheduler.schedule(user_ids, reason).await;
    }
}

#[allow(clippy::too_many_arguments)]
async fn add_external_account(
    store: &dyn ExternalAccountsStore,
    registry: &ExternalAccountProviderRegistry,
    key: Option<Arc<dyn Key>>,
    scheduler: Option<&dyn PermsSyncScheduler>,
    gerrit: Option<&dyn GerritAccountService>,
    field: &RootField,
    actor: Actor,
    viewer_site_admin: bool,
) -> Result<(), String> {
    if !actor.is_authenticated() || actor.is_internal() {
        return Err("not authenticated".to_owned());
    }
    let uid = actor.uid().unwrap_or(0);
    let service_type = field.arguments.string("serviceType").unwrap_or_default();
    let service_id = field.arguments.string("serviceID").unwrap_or_default();
    let account_details = field.arguments.string("accountDetails").unwrap_or_default();
    match service_type {
        "gerrit" => match gerrit {
            Some(gerrit) => {
                gerrit
                    .add(store, uid, service_id, account_details, key.as_deref())
                    .await?;
            }
            None => {
                return Err("gerrit external account additions are not ported (M9.9b)".to_owned())
            }
        },
        "sourcegraph-operator" => {
            add_soap_external_account(
                store,
                registry,
                key.as_deref(),
                viewer_site_admin,
                uid,
                service_id,
                account_details,
            )
            .await
            .map_err(|error| {
                format!("failed to add Sourcegraph Operator external account: {error}")
            })?;
        }
        other => {
            return Err(format!("unsupported service type {}", go_quote(other)));
        }
    }
    schedule_perms_sync(scheduler, &[uid], REASON_EXTERNAL_ACCOUNT_ADDED).await;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn delete_external_account(
    store: &dyn ExternalAccountsStore,
    scheduler: Option<&dyn PermsSyncScheduler>,
    feature_flags: Option<&dyn ExternalAccountFeatureFlags>,
    field: &RootField,
    actor: Actor,
    viewer_site_admin: bool,
) -> Result<(), String> {
    let disallow = match feature_flags {
        Some(flags) => {
            flags
                .disallow_user_external_account_deletion(actor, viewer_site_admin)
                .await
        }
        None => false,
    };
    if disallow {
        // NOTE: The error message is shown directly to the user.
        return Err(
            "Self-serve unlinking external account is not allowed, please contact \
             Sourcegraph support."
                .to_owned(),
        );
    }

    let raw = field
        .arguments
        .string("externalAccount")
        .unwrap_or_default();
    let id = crate::graphql_resolvers::unmarshal_relay_id(raw)?;
    let Some(account) = store.get_by_id(id).await? else {
        return Err(format!("user external account not found: [{id}]"));
    };
    // 🚨 SECURITY: Only the user and site admins may delete.
    if !can_view_external_account(actor, viewer_site_admin, account.user_id) {
        return Err(MUST_BE_SITE_ADMIN_OR_SAME_USER.to_owned());
    }
    store.delete_by_id(account.id).await?;
    schedule_perms_sync(
        scheduler,
        &[account.user_id],
        REASON_EXTERNAL_ACCOUNT_DELETED,
    )
    .await;
    Ok(())
}

/// Prefetch `Mutation.addExternalAccount` / `Mutation.deleteExternalAccount`.
#[allow(clippy::too_many_arguments)]
pub async fn prefetch_external_account_mutations(
    store: &dyn ExternalAccountsStore,
    registry: &ExternalAccountProviderRegistry,
    key: Option<Arc<dyn Key>>,
    scheduler: Option<&dyn PermsSyncScheduler>,
    feature_flags: Option<&dyn ExternalAccountFeatureFlags>,
    gerrit: Option<&dyn GerritAccountService>,
    fields: &[RootField],
    actor: Actor,
    viewer_site_admin: bool,
) -> HashMap<String, Result<(), String>> {
    let mut outcomes = HashMap::new();
    for field in fields {
        let outcome = match field.name.as_str() {
            "addExternalAccount" => {
                add_external_account(
                    store,
                    registry,
                    key.clone(),
                    scheduler,
                    gerrit,
                    field,
                    actor,
                    viewer_site_admin,
                )
                .await
            }
            "deleteExternalAccount" => {
                delete_external_account(
                    store,
                    scheduler,
                    feature_flags,
                    field,
                    actor,
                    viewer_site_admin,
                )
                .await
            }
            _ => continue,
        };
        outcomes.entry(field.name.clone()).or_insert(outcome);
    }
    outcomes
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

    async fn list_filtered(
        &self,
        filter: &ExternalAccountsListFilter,
        limit: Option<i64>,
    ) -> Result<Vec<ExternalAccountRow>, String> {
        let options = sg_db::user_external_accounts::ExternalAccountsListOptions {
            user_id: filter.user_id,
            service_type: filter.service_type.clone(),
            service_id: filter.service_id.clone(),
            client_id: filter.client_id.clone(),
            account_id: None,
            exclude_expired: false,
            only_expired: false,
        };
        sg_db::user_external_accounts::list_with_timestamps(&self.pool, &options, limit)
            .await
            .map(|rows| rows.into_iter().map(ExternalAccountRow::from).collect())
            .map_err(|error| error.to_string())
    }

    async fn count_filtered(&self, filter: &ExternalAccountsListFilter) -> Result<i64, String> {
        let options = sg_db::user_external_accounts::ExternalAccountsListOptions {
            user_id: filter.user_id,
            service_type: filter.service_type.clone(),
            service_id: filter.service_id.clone(),
            client_id: filter.client_id.clone(),
            account_id: None,
            exclude_expired: false,
            only_expired: false,
        };
        sg_db::user_external_accounts::count_matching(&self.pool, &options)
            .await
            .map_err(|error| error.to_string())
    }

    async fn delete_by_id(&self, id: i32) -> Result<(), String> {
        sg_db::user_external_accounts::soft_delete(&self.pool, id)
            .await
            .map_err(|error| error.message)
    }

    async fn upsert_with_data(
        &self,
        user_id: i32,
        spec: &ExternalAccountSpec,
        auth_data: Option<&str>,
        account_data: Option<&str>,
        key: Option<&dyn Key>,
    ) -> Result<(), String> {
        let (auth_cipher, auth_key) = match auth_data {
            Some(value) => sg_encryption::maybe_encrypt(key, value)
                .await
                .map_err(|error| error.to_string())?,
            None => (String::new(), String::new()),
        };
        let (data_cipher, data_key) = match account_data {
            Some(value) => sg_encryption::maybe_encrypt(key, value)
                .await
                .map_err(|error| error.to_string())?,
            None => (String::new(), String::new()),
        };
        // The baseline's `encryptData` overwrites `keyID` with the last
        // `Encrypt` call: `Data` when present, otherwise `AuthData`.
        let key_id = if account_data.is_some() {
            data_key
        } else {
            auth_key
        };
        sg_db::user_external_accounts::upsert_with_data(
            &self.pool,
            user_id,
            &sg_db::user_external_accounts::ExternalAccountSpec {
                service_type: spec.service_type.clone(),
                service_id: spec.service_id.clone(),
                client_id: spec.client_id.clone(),
                account_id: spec.account_id.clone(),
            },
            &auth_cipher,
            &data_cipher,
            &key_id,
        )
        .await
        .map(|_| ())
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

/// `permission_sync_jobs`-backed [`PermsSyncScheduler`] (high priority = 10).
pub struct PgPermsSyncScheduler {
    pool: sqlx::PgPool,
}

impl PgPermsSyncScheduler {
    pub fn new(pool: sqlx::PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl PermsSyncScheduler for PgPermsSyncScheduler {
    async fn schedule(&self, user_ids: &[i32], reason: &str) -> Result<(), String> {
        let now = OffsetDateTime::now_utc();
        for user_id in user_ids {
            sg_db::permission_sync_jobs::create_user_sync_job(
                &self.pool, *user_id, reason, 10, false, now,
            )
            .await
            .map_err(|error| error.to_string())?;
        }
        Ok(())
    }
}

/// `feature_flags`-backed [`ExternalAccountFeatureFlags`], mirroring the
/// request middleware's user → anonymous → global fallback (the anonymous-UID
/// header is not representable in `sg_graphql::exec::Actor`; see the overview
/// §4).
pub struct PgExternalAccountFeatureFlags {
    pool: sqlx::PgPool,
}

impl PgExternalAccountFeatureFlags {
    pub fn new(pool: sqlx::PgPool) -> Self {
        Self { pool }
    }

    async fn global(&self) -> bool {
        sg_db::feature_flags::get_global_feature_flags(&self.pool)
            .await
            .ok()
            .and_then(|flags| {
                flags
                    .get("disallow-user-external-account-deletion")
                    .copied()
            })
            .unwrap_or(false)
    }
}

#[async_trait]
impl ExternalAccountFeatureFlags for PgExternalAccountFeatureFlags {
    async fn disallow_user_external_account_deletion(
        &self,
        actor: Actor,
        _viewer_site_admin: bool,
    ) -> bool {
        match actor {
            Actor::User { uid } => {
                match sg_db::feature_flags::get_user_flags(&self.pool, uid).await {
                    Ok(flags) => flags
                        .get("disallow-user-external-account-deletion")
                        .copied()
                        .unwrap_or(false),
                    Err(_) => self.global().await,
                }
            }
            Actor::Anonymous | Actor::Internal => self.global().await,
        }
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
                // Sourcegraph Operator providers are always visible through
                // `providers.GetProviderByConfigID` (their service ID is the
                // fixed provider type, not the configured issuer).
                "sourcegraph-operator" => {
                    let client_id = entry
                        .get("clientID")
                        .and_then(serde_json::Value::as_str)
                        .unwrap_or_default();
                    registry.register_provider_config(
                        "sourcegraph-operator",
                        "sourcegraph-operator",
                        client_id,
                    );
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

/// `Site.externalAccounts`'s `externalAccountConnectionResolver`
/// (`external_accounts.go:75-121`).
pub struct SiteExternalAccountsConnectionObject {
    data: SiteExternalAccountsData,
    viewer_site_admin: bool,
    nested: Arc<crate::graphql_resolvers::NestedPrefetch>,
}

impl SiteExternalAccountsConnectionObject {
    pub fn from_data(
        data: SiteExternalAccountsData,
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

impl ObjectValue for SiteExternalAccountsConnectionObject {
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
            "nodes" => match &self.data.nodes {
                Ok(nodes) => Ok(Some(FieldValue::List {
                    items: nodes
                        .iter()
                        .map(|node| {
                            FieldValue::Object(Arc::new(ExternalAccountObject::from_data(
                                node.clone(),
                                None,
                                self.viewer_site_admin,
                                self.nested.clone(),
                            )))
                        })
                        .collect(),
                    element_type: Some("ExternalAccount"),
                })),
                Err(message) => Err(ExecError::new(message.clone())),
            },
            "totalCount" => match &self.data.total_count {
                Ok(count) => Ok(Some(FieldValue::Json(Json::Int(*count)))),
                Err(message) => Err(ExecError::new(message.clone())),
            },
            "pageInfo" => match &self.data.nodes {
                Ok(nodes) => {
                    let has_next_page = match self.data.first {
                        None => false,
                        Some(first) => {
                            i64::try_from(nodes.len()).unwrap_or(i64::MAX) > i64::from(first)
                        }
                    };
                    Ok(Some(FieldValue::Object(Arc::new(PageInfoObject::new(
                        has_next_page,
                        None,
                    )))))
                }
                Err(message) => Err(ExecError::new(message.clone())),
            },
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
            // `ExternalAccount.AccountData` (`external_account.go:94-121`).
            "accountData" => match &self.data.account_data {
                AccountDataLookup::Null => Ok(Some(FieldValue::Null)),
                AccountDataLookup::Error(message) => Err(ExecError::new(message.clone())),
                AccountDataLookup::Json(raw) => {
                    let value = serde_json::from_str::<serde_json::Value>(raw)
                        .map_err(|error| ExecError::new(error.to_string()))?;
                    let value = Json::from_serde(&value).map_err(ExecError::new)?;
                    Ok(Some(FieldValue::Json(value)))
                }
            },
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
