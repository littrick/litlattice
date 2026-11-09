#![forbid(unsafe_code)]

//! `cmd/worker/internal/sourcegraphaccounts`: the
//! `sourcegraph-accounts-notifications-subscriber` job.
//!
//! The job subscribes to Sourcegraph Accounts (SAMS) notifications over GCP
//! Pub/Sub and reacts to `UserDeleted` events by hard-deleting the local users
//! that still have an external account for the deleted SAMS account.
//!
//! Ported as-is:
//!
//! * the `Routines` gate, in the baseline's order: not dotcom → disabled;
//!   `SOURCEGRAPH_ACCOUNTS_CREDENTIALS_FILE` empty → disabled; the first
//!   `openidconnect` auth provider whose `clientID` starts with `sams_cid_`
//!   absent → disabled. Disabled jobs return no routines and no error;
//! * `notificationsSubscriberConfig.Load`, including env names, defaults and
//!   descriptions;
//! * `os.ReadFile`'s `open <path>: <reason>` error text for a missing or
//!   unreadable credentials file, wrapped as `read GCP credentials file: ...`;
//! * `onUserDeleted`: re-check the user with SAMS (`get user by ID`), list the
//!   local external accounts matching `{ServiceType: provider.Type,
//!   ServiceID: provider.Issuer, AccountID: accountID}` (`list user external
//!   accounts`), then hard-delete every matched user (`hard-delete users`).
//!   The store's real implementation is `user_external_accounts::List` +
//!   `users::HardDeleteList` with the anonymous backend actor, so the deletion
//!   writes the same analytics/security events as the baseline.
//!
//! Not ported (documented in docs §77.3): the GCP Pub/Sub transport and the
//! SAMS API client. A dotcom deployment with a SAMS provider configured and a
//! credentials file present therefore fails loudly with
//! [`PUBSUB_NOT_PORTED`] instead of silently accepting notifications; the
//! public `on_user_deleted` handler and its database store are complete and
//! differential-tested through an injected [`NotificationsStore`].

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use sg_config::{LiveConfig, SiteConfiguration};
use sg_core::{ConfigError, EnvReader};
use sg_db::user_external_accounts::{self, ExternalAccount, ExternalAccountsListOptions};
use sg_db::users::{self, BackendActor, DeletionEventContext};
use sqlx::types::time::OffsetDateTime;
use sqlx::PgPool;
use tokio_util::sync::CancellationToken;

use super::{Job, JobConfig, JobDeps};
use crate::framework::BackgroundRoutine;
use crate::jobs::eventlogs::security_event_logs_in_database;

/// `notificationsSubscriber.Description`.
pub const JOB_DESCRIPTION: &str = "Receives notifications from Sourcegraph Accounts.";

/// `notificationsSubscriberConfig.Load`'s env vars.
pub const CREDENTIALS_FILE_VAR: &str = "SOURCEGRAPH_ACCOUNTS_CREDENTIALS_FILE";
pub const PROJECT_ID_VAR: &str = "SOURCEGRAPH_ACCOUNTS_NOTIFICATIONS_PROJECT";
pub const SUBSCRIPTION_ID_VAR: &str = "SOURCEGRAPH_ACCOUNTS_NOTIFICATIONS_SUBSCRIPTION";

/// `SOURCEGRAPH_ACCOUNTS_NOTIFICATIONS_PROJECT`'s default.
pub const DEFAULT_PROJECT_ID: &str = "sourcegraph-dev";
/// `SOURCEGRAPH_ACCOUNTS_NOTIFICATIONS_SUBSCRIPTION`'s default.
pub const DEFAULT_SUBSCRIPTION_ID: &str = "sams-notifications";

const CREDENTIALS_FILE_DESCRIPTION: &str = "Path to the Google Cloud credentials file";
const PROJECT_ID_DESCRIPTION: &str = "The GCP project that the service is running in";
const SUBSCRIPTION_ID_DESCRIPTION: &str =
    "GCP Pub/Sub subscription ID to receive SAMS notifications from";

/// `strings.HasPrefix(p.Openidconnect.ClientID, "sams_cid_")`.
pub const SAMS_CLIENT_ID_PREFIX: &str = "sams_cid_";

/// `samsProvider.Type` for the only auth provider kind the job accepts.
pub const OPENIDCONNECT_PROVIDER_TYPE: &str = "openidconnect";

/// The `Routines` error raised for the one path the Rust port cannot serve.
/// The baseline creates the SAMS client, reads the credentials file and then
/// constructs a Pub/Sub subscriber; the Rust worker implements the handler and
/// the store but neither of the two clients.
pub const PUBSUB_NOT_PORTED: &str =
    "create notifications subscriber: the GCP Pub/Sub subscriber is not ported to the Rust worker yet";

/// `notificationsSubscriberStore.GetSAMSUserByID`'s not-ported error.
pub const SAMS_CLIENT_NOT_PORTED: &str = "SAMS API client is not ported to the Rust worker yet";

/// `notificationsSubscriberConfig` (`env.BaseConfig`).
#[derive(Debug, Default)]
pub struct NotificationsSubscriberConfig {
    credentials_file: Mutex<String>,
    project_id: Mutex<String>,
    subscription_id: Mutex<String>,
}

impl NotificationsSubscriberConfig {
    /// `c.GCP.CredentialsFile`.
    pub fn credentials_file(&self) -> String {
        self.credentials_file.lock().expect("config mutex").clone()
    }

    /// `c.GCP.ProjectID`.
    pub fn project_id(&self) -> String {
        self.project_id.lock().expect("config mutex").clone()
    }

    /// `c.GCP.SubscriptionID`.
    pub fn subscription_id(&self) -> String {
        self.subscription_id.lock().expect("config mutex").clone()
    }
}

impl JobConfig for NotificationsSubscriberConfig {
    fn load(&self, env: &mut EnvReader) {
        *self.credentials_file.lock().expect("config mutex") =
            env.get(CREDENTIALS_FILE_VAR, "", CREDENTIALS_FILE_DESCRIPTION);
        *self.project_id.lock().expect("config mutex") =
            env.get(PROJECT_ID_VAR, DEFAULT_PROJECT_ID, PROJECT_ID_DESCRIPTION);
        *self.subscription_id.lock().expect("config mutex") = env.get(
            SUBSCRIPTION_ID_VAR,
            DEFAULT_SUBSCRIPTION_ID,
            SUBSCRIPTION_ID_DESCRIPTION,
        );
    }

    fn validate(&self) -> Result<(), ConfigError> {
        Ok(())
    }
}

/// The `schema.OpenIDConnectAuthProvider` fields the job reads.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SamsProvider {
    /// `samsProvider.Type` (the provider JSON's `type`, always
    /// [`OPENIDCONNECT_PROVIDER_TYPE`] for the providers this job accepts).
    pub provider_type: String,
    /// `samsProvider.Issuer`.
    pub issuer: String,
    /// `samsProvider.ClientID`.
    pub client_id: String,
    /// `samsProvider.ClientSecret`.
    pub client_secret: String,
}

/// `conf.Get().SiteConfig().AuthProviders`, reduced to the SAMS lookup:
///
/// ```go
/// for _, p := range authProviders {
///     if p.Openidconnect != nil && strings.HasPrefix(p.Openidconnect.ClientID, "sams_cid_") {
///         samsProvider = p.Openidconnect
///         break
///     }
/// }
/// ```
///
/// Site configuration entries are flat objects discriminated by `type`
/// (`schema.AuthProviders.UnmarshalJSON`), so `openidconnect` plus a
/// `clientID` prefix selects the provider.
pub fn find_sams_provider(site: &SiteConfiguration) -> Option<SamsProvider> {
    let providers = site.raw().get("auth.providers")?.as_array()?;
    for provider in providers {
        if provider.get("type").and_then(serde_json::Value::as_str)
            != Some(OPENIDCONNECT_PROVIDER_TYPE)
        {
            continue;
        }
        let client_id = provider
            .get("clientID")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default();
        if !client_id.starts_with(SAMS_CLIENT_ID_PREFIX) {
            continue;
        }
        return Some(SamsProvider {
            provider_type: OPENIDCONNECT_PROVIDER_TYPE.to_owned(),
            issuer: provider
                .get("issuer")
                .and_then(serde_json::Value::as_str)
                .unwrap_or_default()
                .to_owned(),
            client_id: client_id.to_owned(),
            client_secret: provider
                .get("clientSecret")
                .and_then(serde_json::Value::as_str)
                .unwrap_or_default()
                .to_owned(),
        });
    }
    None
}

/// `notificationsSubscriber.Routines`' opening gate, in the baseline's order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SubscriberGate {
    /// `!dotcom.SourcegraphDotComMode()`.
    NotDotcom,
    /// `s.config.GCP.CredentialsFile == ""`.
    NoCredentialsFile,
    /// `samsProvider == nil`.
    NoProvider,
    /// Everything the baseline checks before creating clients is in place.
    Enabled(SamsProvider),
}

/// [`SubscriberGate`] as a pure function (the job reads the env var in
/// [`Job::routines`]).
pub fn subscriber_gate(
    dotcom: bool,
    credentials_file: &str,
    site: &SiteConfiguration,
) -> SubscriberGate {
    if !dotcom {
        return SubscriberGate::NotDotcom;
    }
    if credentials_file.is_empty() {
        return SubscriberGate::NoCredentialsFile;
    }
    match find_sams_provider(site) {
        Some(provider) => SubscriberGate::Enabled(provider),
        None => SubscriberGate::NoProvider,
    }
}

/// `cockroachdb/errors.Wrap`: renders `"{context}: {err}"`, dropping a single
/// trailing newline from the inner error.
fn wrap_error(context: &str, err: &str) -> String {
    format!("{context}: {}", err.strip_suffix('\n').unwrap_or(err))
}

/// `os.ReadFile`'s error surface for the credentials file. Go renders the
/// syscall error (`open <path>: no such file or directory`); Rust's
/// `io::Error` spells it differently, so the two common cases are mapped back.
fn read_credentials_file(path: &str) -> Result<Vec<u8>, String> {
    std::fs::read(path).map_err(|err| {
        let reason = match err.kind() {
            std::io::ErrorKind::NotFound => "no such file or directory".to_owned(),
            std::io::ErrorKind::PermissionDenied => "permission denied".to_owned(),
            _ => err.to_string(),
        };
        wrap_error(
            "read GCP credentials file",
            &format!("open {path}: {reason}"),
        )
    })
}

/// `notificationsSubscriber.Routines` with the dotcom flag made explicit.
///
/// The disabled paths return no routines - exactly the baseline's `nil, nil`.
/// The enabled path reproduces the credentials-file error surface and then
/// fails loudly with [`PUBSUB_NOT_PORTED`] (docs §77.3).
pub async fn subscriber_routines(
    deps: &JobDeps,
    dotcom: bool,
    config: &NotificationsSubscriberConfig,
) -> Result<Vec<BackgroundRoutine>, String> {
    let site = deps.live_config.current().site.clone();
    let credentials_file = config.credentials_file();

    match subscriber_gate(dotcom, &credentials_file, &site) {
        SubscriberGate::NotDotcom
        | SubscriberGate::NoCredentialsFile
        | SubscriberGate::NoProvider => Ok(Vec::new()),
        SubscriberGate::Enabled(provider) => {
            tracing::info!(
                sams_provider_issuer = %provider.issuer,
                sams_provider_client_id = %provider.client_id,
                "worker enabled"
            );
            // The baseline reads the file before constructing the subscriber;
            // keep the error surface for a missing/unreadable file.
            let _credentials = read_credentials_file(&credentials_file)?;
            Err(PUBSUB_NOT_PORTED.to_owned())
        }
    }
}

/// `clientsv1.User`: only its existence is observed by the handler.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SamsUser {
    pub id: String,
}

/// `notificationsSubscriberStore`. `GetSAMSUserByID` returns `Ok(None)` for
/// `sams.ErrNotFound` and `Err(message)` for every other failure, so the
/// handler can reproduce the baseline's `errors.Is(err, sams.ErrNotFound)`
/// branch.
#[async_trait]
pub trait NotificationsStore: Send + Sync {
    async fn get_sams_user_by_id(&self, account_id: &str) -> Result<Option<SamsUser>, String>;

    async fn list_user_external_accounts(
        &self,
        options: &ExternalAccountsListOptions,
    ) -> Result<Vec<ExternalAccount>, String>;

    async fn hard_delete_users(&self, user_ids: &[i32]) -> Result<(), String>;
}

/// `notificationsSubscriberHandlers.onUserDeleted`.
///
/// A notification for a user SAMS still knows is ignored (the baseline logs an
/// error and returns nil). Otherwise every local user with a live external
/// account for the SAMS account is hard-deleted - deliberately without
/// filtering on `clientID`, because rotating the OAuth client must not leave
/// orphaned users behind.
pub async fn on_user_deleted(
    store: &dyn NotificationsStore,
    provider: &SamsProvider,
    account_id: &str,
) -> Result<(), String> {
    match store.get_sams_user_by_id(account_id).await {
        Ok(Some(_)) => {
            tracing::error!(
                account_id,
                "Received user deleted notification for a user still exists"
            );
            return Ok(());
        }
        Ok(None) => {}
        Err(err) => return Err(wrap_error("get user by ID", &err)),
    }

    let options = ExternalAccountsListOptions {
        service_type: Some(provider.provider_type.clone()),
        service_id: Some(provider.issuer.clone()),
        account_id: Some(account_id.to_owned()),
        ..ExternalAccountsListOptions::default()
    };
    let accounts = store
        .list_user_external_accounts(&options)
        .await
        .map_err(|err| wrap_error("list user external accounts", &err))?;

    let user_ids: Vec<i32> = accounts.iter().map(|account| account.user_id).collect();
    store
        .hard_delete_users(&user_ids)
        .await
        .map_err(|err| wrap_error("hard-delete users", &err))?;

    tracing::debug!(
        account_id,
        records_count = user_ids.len(),
        "User hard-deleted"
    );
    Ok(())
}

/// `notificationsSubscriberStoreImpl` backed by the frontend database.
/// `HardDeleteList` runs with the anonymous backend actor (the SAMS
/// notification carries no actor), exactly like the baseline's request
/// context.
pub struct DbNotificationsStore {
    pool: PgPool,
    live_config: Arc<LiveConfig>,
}

impl DbNotificationsStore {
    pub fn new(pool: PgPool, live_config: Arc<LiveConfig>) -> Self {
        Self { pool, live_config }
    }
}

#[async_trait]
impl NotificationsStore for DbNotificationsStore {
    async fn get_sams_user_by_id(&self, _account_id: &str) -> Result<Option<SamsUser>, String> {
        Err(SAMS_CLIENT_NOT_PORTED.to_owned())
    }

    async fn list_user_external_accounts(
        &self,
        options: &ExternalAccountsListOptions,
    ) -> Result<Vec<ExternalAccount>, String> {
        user_external_accounts::list(&self.pool, options)
            .await
            .map_err(|err| err.to_string())
    }

    async fn hard_delete_users(&self, user_ids: &[i32]) -> Result<(), String> {
        let site = self.live_config.current().site.clone();
        let event_context = DeletionEventContext {
            actor: BackendActor {
                uid: 0,
                sourcegraph_operator: false,
            },
            version: crate::telemetry::VERSION,
            now: OffsetDateTime::now_utc(),
            security_event_logs_in_database: security_event_logs_in_database(&site),
        };
        users::hard_delete_list(&self.pool, user_ids, &event_context)
            .await
            .map_err(|err| err.to_string())
    }
}

/// `NewNotificationsSubscriber`.
#[derive(Debug)]
pub struct SourcegraphAccountsNotificationsSubscriber {
    config: Arc<NotificationsSubscriberConfig>,
}

impl SourcegraphAccountsNotificationsSubscriber {
    pub fn new() -> Self {
        Self {
            config: Arc::new(NotificationsSubscriberConfig::default()),
        }
    }
}

impl Default for SourcegraphAccountsNotificationsSubscriber {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Job for SourcegraphAccountsNotificationsSubscriber {
    fn description(&self) -> String {
        JOB_DESCRIPTION.to_owned()
    }

    fn configs(&self) -> Vec<Arc<dyn JobConfig>> {
        vec![self.config.clone()]
    }

    async fn routines(
        &self,
        deps: &JobDeps,
        _setup_ctx: CancellationToken,
    ) -> Result<Vec<BackgroundRoutine>, String> {
        let dotcom = crate::telemetry::env_bool("SOURCEGRAPHDOTCOM_MODE");
        subscriber_routines(deps, dotcom, &self.config).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn site(json: &str) -> SiteConfiguration {
        SiteConfiguration::parse(json).expect("valid site config")
    }

    #[test]
    fn config_defaults_match_the_baseline() {
        static ENV_LOCK: Mutex<()> = Mutex::new(());
        let _guard = ENV_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        for name in [CREDENTIALS_FILE_VAR, PROJECT_ID_VAR, SUBSCRIPTION_ID_VAR] {
            std::env::remove_var(name);
        }
        let config = NotificationsSubscriberConfig::default();
        config.load(&mut EnvReader::new());
        assert_eq!(config.credentials_file(), "");
        assert_eq!(config.project_id(), "sourcegraph-dev");
        assert_eq!(config.subscription_id(), "sams-notifications");
    }

    #[test]
    fn finds_the_first_sams_provider() {
        let site = site(
            r#"{
                "auth.providers": [
                    {"type": "openidconnect", "clientID": "other", "issuer": "https://a", "clientSecret": "s"},
                    {"type": "github", "clientID": "sams_cid_start", "issuer": "https://gh"},
                    {"type": "openidconnect", "clientID": "sams_cid_one", "issuer": "https://accounts.sourcegraph.com/one", "clientSecret": "one-secret"},
                    {"type": "openidconnect", "clientID": "sams_cid_two", "issuer": "https://accounts.sourcegraph.com/two", "clientSecret": "two-secret"}
                ]
            }"#,
        );
        assert_eq!(
            find_sams_provider(&site),
            Some(SamsProvider {
                provider_type: "openidconnect".to_owned(),
                issuer: "https://accounts.sourcegraph.com/one".to_owned(),
                client_id: "sams_cid_one".to_owned(),
                client_secret: "one-secret".to_owned(),
            })
        );
    }

    #[test]
    fn gate_follows_the_baseline_order() {
        let sams_site = site(
            r#"{"auth.providers": [{"type": "openidconnect", "clientID": "sams_cid_x", "issuer": "https://accounts.sourcegraph.com", "clientSecret": "s"}]}"#,
        );
        assert_eq!(
            subscriber_gate(false, "", &sams_site),
            SubscriberGate::NotDotcom
        );
        assert_eq!(
            subscriber_gate(true, "", &sams_site),
            SubscriberGate::NoCredentialsFile
        );
        assert_eq!(
            subscriber_gate(true, "/tmp/creds.json", &SiteConfiguration::default()),
            SubscriberGate::NoProvider
        );
        assert!(matches!(
            subscriber_gate(true, "/tmp/creds.json", &sams_site),
            SubscriberGate::Enabled(_)
        ));
    }

    #[test]
    fn missing_credentials_file_uses_the_go_error_text() {
        let err = read_credentials_file("/nonexistent/creds.json").unwrap_err();
        assert_eq!(
            err,
            "read GCP credentials file: open /nonexistent/creds.json: no such file or directory"
        );
    }
}
