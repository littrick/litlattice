#![forbid(unsafe_code)]

//! `cmd/worker/internal/auth/sourcegraph_operator_cleaner.go`: the
//! `auth-sourcegraph-operator-cleaner` job.
//!
//! The job only exists when the signed Sourcegraph Cloud site configuration
//! enables the Sourcegraph Operator auth provider
//! (`cloud.SiteConfig().SourcegraphOperatorAuthProviderEnabled()`), in which
//! case it runs `auth.expired-soap-cleaner` every minute.
//!
//! Each tick selects users whose live `sourcegraph-operator` external account
//! is older than the configured lifecycle (`lifecycleDuration` minutes,
//! default 60) and then, per user:
//!
//! - a SOAP service account (`account_data.serviceAccount == true`) is left
//!   untouched;
//! - a user with any other external account is demoted (site admin revoked,
//!   `user_roles` role deleted) and only their SOAP account is hard-deleted;
//! - a user with no other external account is hard-deleted with every
//!   resource `HardDeleteList` owns.
//!
//! Every deletion runs with the synthetic `SourcegraphOperator` actor, so the
//! analytics rows carry `public_argument.sourcegraph_operator = true` and are
//! excluded from usage analytics by the baseline's own filters.
//!
//! Not ported: the RED metrics/observation operation and log lines.

use std::time::Duration;

use async_trait::async_trait;
use serde::Deserialize;
use sg_config::cloud::{self, SchemaSiteConfig};
use sg_config::LiveConfig;
use sg_db::user_external_accounts::{self, UserExternalAccount};
use sg_db::users::{self, BackendActor, DeletionEventContext, SOURCEGRAPH_OPERATOR_PROVIDER_TYPE};
use sg_encryption::{maybe_decrypt, Key};
use sg_keyring::Keyring;
use sqlx::types::time::OffsetDateTime;
use sqlx::PgPool;
use time::Duration as SqlDuration;
use tokio_util::sync::CancellationToken;

use super::{Job, JobDeps};
use crate::config::combine_errors;
use crate::framework::periodic::{Handler, HandlerError, PeriodicRoutine};
use crate::framework::BackgroundRoutine;
use crate::jobs::eventlogs::security_event_logs_in_database;

/// `sourcegraphOperatorCleaner.Description`.
pub const JOB_DESCRIPTION: &str = "Cleans up expired Sourcegraph Operator user accounts.";

/// `goroutine.WithName` / `WithDescription`.
pub const ROUTINE_NAME: &str = "auth.expired-soap-cleaner";
pub const ROUTINE_DESCRIPTION: &str = "deletes expired SOAP operator user accounts";

/// `goroutine.WithInterval(time.Minute)`.
pub const INTERVAL: Duration = Duration::from_secs(60);

/// `sourcegraphoperator.ExternalAccountData`.
#[derive(Debug, Clone, Copy, Default, Deserialize)]
struct ExternalAccountData {
    #[serde(rename = "serviceAccount", default)]
    service_account: bool,
}

/// The `Routines` gate: the provider must be configured, and its
/// `lifecycleDuration` (minutes) becomes the expiry window. A non-positive
/// value means the 60-minute default.
pub fn enabled_lifecycle(site: &SchemaSiteConfig) -> Option<Duration> {
    if !site.sourcegraph_operator_auth_provider_enabled() {
        return None;
    }
    let minutes = site
        .auth_providers
        .as_ref()
        .and_then(|providers| providers.sourcegraph_operator.as_ref())
        .map(|operator| operator.lifecycle_duration)
        .unwrap_or(0);
    Some(cloud::lifecycle_duration(minutes))
}

/// `sourcegraphOperatorCleanHandler.Handle` with the pieces of the request
/// context made explicit.
pub async fn clean_expired_sourcegraph_operator_users(
    frontend: &PgPool,
    keyring: &Keyring,
    live_config: &LiveConfig,
    lifecycle_duration: Duration,
) -> Result<(), String> {
    let cutoff = OffsetDateTime::now_utc()
        - SqlDuration::seconds(i64::try_from(lifecycle_duration.as_secs()).unwrap_or(i64::MAX));

    let user_ids = users::list_expired_sourcegraph_operator_user_ids(frontend, cutoff)
        .await
        .map_err(|err| format!("query expired SOAP users: {err}"))?;

    let encryption_key = keyring.user_external_account_key();

    let mut delete_user_ids = Vec::new();
    let mut demote_user_ids = Vec::new();
    let mut delete_external_account_ids = Vec::new();

    for user_id in user_ids {
        let accounts = user_external_accounts::list_for_user(frontend, user_id)
            .await
            .map_err(|err| format!("list external accounts for user {user_id}: {err}"))?;

        let mut is_service_account = false;
        let mut soap_external_account_id: Option<i32> = None;
        for account in &accounts {
            if account.service_type == SOURCEGRAPH_OPERATOR_PROVIDER_TYPE {
                soap_external_account_id = Some(account.id);
                if is_service_account_account(account, encryption_key.as_deref()).await {
                    is_service_account = true;
                    break;
                }
            }
        }
        if is_service_account {
            continue;
        }

        if accounts.len() > 1 {
            demote_user_ids.push(user_id);
            delete_external_account_ids.push(soap_external_account_id.unwrap_or(0));
        } else {
            delete_user_ids.push(user_id);
        }
    }

    // Help exclude Sourcegraph operator related events from analytics.
    let event_context = DeletionEventContext {
        actor: BackendActor {
            uid: 0,
            sourcegraph_operator: true,
        },
        version: crate::telemetry::VERSION,
        now: OffsetDateTime::now_utc(),
        security_event_logs_in_database: security_event_logs_in_database(
            &live_config.current().site,
        ),
    };

    // Hard delete users with only the expired SOAP account.
    if !delete_user_ids.is_empty() {
        match users::hard_delete_list(frontend, &delete_user_ids, &event_context).await {
            Ok(()) => {}
            Err(err) if err.is_not_found() => {}
            Err(err) => return Err(format!("hard delete users: {err}")),
        }
    }

    // Demote users: remove their SOAP account, and make sure they are not a
    // site admin. Errors are accumulated; the SOAP accounts are only deleted
    // when every demotion succeeded.
    if !demote_user_ids.is_empty() {
        let mut demote_errors = Vec::new();
        for user_id in demote_user_ids {
            if let Err(err) =
                users::set_is_site_admin(frontend, user_id, false, &event_context).await
            {
                if !err.is_not_found() {
                    demote_errors.push(format!("revoke site admin: {err}"));
                }
            }
        }
        if let Some(message) = combine_errors(&demote_errors) {
            return Err(message);
        }
    }
    if !delete_external_account_ids.is_empty() {
        match user_external_accounts::hard_delete(
            frontend,
            &delete_external_account_ids,
            SOURCEGRAPH_OPERATOR_PROVIDER_TYPE,
        )
        .await
        {
            Ok(()) => {}
            Err(err) if err.is_not_found() => {}
            Err(err) => return Err(format!("remove SOAP accounts: {err}")),
        }
    }

    Ok(())
}

/// `sourcegraphoperator.GetAccountData`: parse (after decrypting when needed)
/// the account data and report `ServiceAccount`. Any error - including a nil
/// `account_data` column - is "not a service account", like the baseline's
/// ignored error.
async fn is_service_account_account(account: &UserExternalAccount, key: Option<&dyn Key>) -> bool {
    let Some(data) = account.account_data.as_deref() else {
        return false;
    };
    let Ok(plain) = maybe_decrypt(key, data, &account.encryption_key_id).await else {
        return false;
    };
    serde_json::from_str::<ExternalAccountData>(&plain)
        .map(|data| data.service_account)
        .unwrap_or(false)
}

/// `sourcegraphOperatorCleanHandler` as a periodic handler.
pub struct CleanerHandler {
    frontend: PgPool,
    keyring: std::sync::Arc<Keyring>,
    live_config: std::sync::Arc<LiveConfig>,
    lifecycle_duration: Duration,
}

impl CleanerHandler {
    pub fn new(
        frontend: PgPool,
        keyring: std::sync::Arc<Keyring>,
        live_config: std::sync::Arc<LiveConfig>,
        lifecycle_duration: Duration,
    ) -> Self {
        Self {
            frontend,
            keyring,
            live_config,
            lifecycle_duration,
        }
    }
}

#[async_trait]
impl Handler for CleanerHandler {
    async fn handle(&self, _ctx: CancellationToken) -> Result<(), HandlerError> {
        clean_expired_sourcegraph_operator_users(
            &self.frontend,
            &self.keyring,
            &self.live_config,
            self.lifecycle_duration,
        )
        .await
        .map_err(HandlerError::new)
    }
}

/// `cmd/worker/internal/auth.NewSourcegraphOperatorCleaner`.
#[derive(Debug, Default)]
pub struct SourcegraphOperatorCleaner;

impl SourcegraphOperatorCleaner {
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl Job for SourcegraphOperatorCleaner {
    fn description(&self) -> String {
        JOB_DESCRIPTION.to_owned()
    }

    async fn routines(
        &self,
        deps: &JobDeps,
        _setup_ctx: CancellationToken,
    ) -> Result<Vec<BackgroundRoutine>, String> {
        let site_config = cloud::site_config().map_err(str::to_owned)?;
        let Some(lifecycle_duration) = enabled_lifecycle(site_config) else {
            return Ok(Vec::new());
        };

        let routine = PeriodicRoutine::new(std::sync::Arc::new(CleanerHandler::new(
            deps.frontend.clone(),
            deps.keyring.clone(),
            deps.live_config.clone(),
            lifecycle_duration,
        )))
        .with_name(ROUTINE_NAME)
        .with_description(ROUTINE_DESCRIPTION)
        .with_interval(INTERVAL);

        Ok(vec![routine.into()])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sg_config::cloud::{
        SchemaAuthProviderSourcegraphOperator, SchemaAuthProviders, SiteConfigAllowlistSpec,
    };

    fn site_with_operator(lifecycle_duration: i64) -> SchemaSiteConfig {
        SchemaSiteConfig {
            auth_providers: Some(SchemaAuthProviders {
                sourcegraph_operator: Some(SchemaAuthProviderSourcegraphOperator {
                    lifecycle_duration,
                    ..SchemaAuthProviderSourcegraphOperator::default()
                }),
            }),
            site_config_allowlist: SiteConfigAllowlistSpec::default(),
        }
    }

    #[test]
    fn gate_requires_the_operator_provider() {
        assert_eq!(enabled_lifecycle(&SchemaSiteConfig::default()), None);
        assert_eq!(
            enabled_lifecycle(&site_with_operator(0)),
            Some(Duration::from_secs(3600))
        );
        assert_eq!(
            enabled_lifecycle(&site_with_operator(90)),
            Some(Duration::from_secs(5400))
        );
    }

    #[tokio::test]
    async fn account_data_requires_the_service_account_flag() {
        let account = |data: Option<&str>, key_id: &str| UserExternalAccount {
            id: 1,
            user_id: 1,
            service_type: SOURCEGRAPH_OPERATOR_PROVIDER_TYPE.to_owned(),
            account_data: data.map(str::to_owned),
            encryption_key_id: key_id.to_owned(),
        };

        assert!(
            is_service_account_account(&account(Some(r#"{"serviceAccount":true}"#), ""), None)
                .await
        );
        assert!(
            !is_service_account_account(&account(Some(r#"{"serviceAccount":false}"#), ""), None)
                .await
        );
        assert!(!is_service_account_account(&account(None, ""), None).await);
        assert!(!is_service_account_account(&account(Some("not json"), ""), None).await);
    }

    #[test]
    fn job_metadata_matches_the_baseline() {
        let job = SourcegraphOperatorCleaner::new();
        assert_eq!(job.description(), JOB_DESCRIPTION);
        assert!(job.configs().is_empty());
        assert_eq!(ROUTINE_NAME, "auth.expired-soap-cleaner");
        assert_eq!(INTERVAL, Duration::from_secs(60));
    }
}
