#![forbid(unsafe_code)]

//! User-centric half of `cmd/worker/internal/authz/perms_syncer.go`:
//! `permsSyncerImpl.fetchUserPermsViaExternalAccounts`.
//!
//! * enumerate the user's live external accounts, plus expired GitLab
//!   accounts (they can be refreshed);
//! * ask every configured authz provider without an account for one
//!   (`FetchAccount`) and persist it (`UserExternalAccounts.Upsert`);
//! * call `FetchUserPerms` per account, classifying 401 / 403 / suspended /
//!   unimplemented / temporary errors exactly like the baseline;
//! * on a temporary error, retain the repositories previously synced for the
//!   account (`FetchReposByExternalAccount`);
//! * translate the provider's external repository specs into internal
//!   repository IDs (exact matches, then inclusion/exclusion `SIMILAR TO`
//!   matches, private repositories only).
//!
//! Sub-repository permissions are deliberately not ported here: the
//! `sub_repo_permissions` storage is a later slice, so the temporary-error
//! retention covers repository-level permissions only. Account-data encryption
//! is also not ported (the upsert writes empty auth/account data); neither
//! omission changes account identity or the repository set.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use sg_db::perms_store::{fetch_repos_by_external_account, CodeHostState};
use sg_db::repos::{ExternalRepoSpec, RepoStore};
use sg_db::user_external_accounts::{self, ExternalAccountsListOptions};
use sg_db::users::UserRecord;
use sqlx::postgres::PgPool;

use super::perms_syncer::{
    AuthzProvider, ExternalUserPermissions, FetchPermsOptions, ProviderRegistry, UserPermsError,
    UserPermsFetch, UserPermsFetcher,
};

/// `extsvc.TypeGitLab`.
pub const TYPE_GITLAB: &str = "gitlab";

/// `database.ReposListOptions` chunk size used by
/// `listPrivateRepoNamesBySpecs` (Postgres bind-parameter limit).
const EXTERNAL_SPEC_CHUNK: usize = 10_000;

/// Real [`UserPermsFetcher`] backed by the database and the configured authz
/// providers.
pub struct ExternalAccountPermsFetcher {
    pool: PgPool,
    repo_store: RepoStore,
    registry: Arc<dyn ProviderRegistry>,
    /// `dotcom.SourcegraphDotComMode()`.
    dotcom_mode: bool,
}

impl ExternalAccountPermsFetcher {
    pub fn new(pool: PgPool, registry: Arc<dyn ProviderRegistry>) -> Self {
        let dotcom_mode = std::env::var("SOURCEGRAPHDOTCOM_MODE")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(false);
        Self {
            repo_store: RepoStore::new(pool.clone()),
            pool,
            registry,
            dotcom_mode,
        }
    }

    /// Override the dot-com mode (the baseline reads the environment once at
    /// process start).
    pub fn with_dotcom_mode(mut self, dotcom_mode: bool) -> Self {
        self.dotcom_mode = dotcom_mode;
        self
    }

    fn providers_by_service_id(&self) -> HashMap<String, Arc<dyn AuthzProvider>> {
        self.registry
            .providers()
            .into_iter()
            .map(|provider| (provider.service_id().to_owned(), provider))
            .collect()
    }

    /// `listPrivateRepoNamesBySpecs`: exact matches, chunked.
    async fn list_private_repo_names_by_specs(
        &self,
        specs: &[ExternalRepoSpec],
    ) -> Result<Vec<i32>, sqlx::Error> {
        let mut repo_names = Vec::with_capacity(specs.len());
        for chunk in specs.chunks(EXTERNAL_SPEC_CHUNK) {
            repo_names.extend(
                self.repo_store
                    .list_private_repos_by_external_specs(chunk)
                    .await?,
            );
        }
        Ok(repo_names)
    }
}

#[async_trait]
impl UserPermsFetcher for ExternalAccountPermsFetcher {
    async fn fetch_user_perms(
        &self,
        user: &UserRecord,
        no_perms: bool,
        opts: &FetchPermsOptions,
    ) -> UserPermsFetch {
        let mut results = UserPermsFetch::default();

        // `dotcom.SourcegraphDotComMode()`: OAuth scope on sourcegraph.com
        // cannot read private repositories.
        if self.dotcom_mode {
            return results;
        }

        let live_accounts = match user_external_accounts::list(
            &self.pool,
            &ExternalAccountsListOptions {
                user_id: Some(user.id),
                exclude_expired: true,
                ..Default::default()
            },
        )
        .await
        {
            Ok(accounts) => accounts,
            Err(err) => {
                results.error = Some(format!("list external accounts: {err}"));
                return results;
            }
        };

        // Expired GitLab accounts can be refreshed by the provider.
        let expired_gitlab_accounts = match user_external_accounts::list(
            &self.pool,
            &ExternalAccountsListOptions {
                user_id: Some(user.id),
                service_type: Some(TYPE_GITLAB.to_owned()),
                only_expired: true,
                ..Default::default()
            },
        )
        .await
        {
            Ok(accounts) => accounts,
            Err(err) => {
                results.error = Some(format!("list expired gitlab external accounts: {err}"));
                return results;
            }
        };

        let mut accounts = live_accounts;
        accounts.extend(expired_gitlab_accounts);

        let mut service_to_account: HashMap<String, ()> = HashMap::new();
        for account in &accounts {
            service_to_account.insert(
                format!("{}:{}", account.service_type, account.service_id),
                (),
            );
        }

        let by_service_id = self.providers_by_service_id();

        // Ask every provider without an account for this user whether it can
        // determine one.
        for provider in by_service_id.values() {
            let service_key = format!("{}:{}", provider.service_type(), provider.service_id());
            if service_to_account.contains_key(&service_key) {
                continue;
            }

            match provider.fetch_account(user).await {
                Err(message) => {
                    results.statuses.push(CodeHostState::new_provider_status(
                        provider.service_id(),
                        provider.service_type(),
                        Some(&message),
                        "FetchAccount",
                    ));
                }
                Ok(None) => {}
                Ok(Some(spec)) => {
                    match user_external_accounts::upsert(&self.pool, user.id, &spec).await {
                        // Baseline only logs upsert errors.
                        Err(_) => {}
                        Ok(account) => accounts.push(account),
                    }
                }
            }
        }

        results.repo_perms = Default::default();

        for account in &accounts {
            let Some(provider) = by_service_id.get(&account.service_id).cloned() else {
                // No authz provider configured for this external account.
                continue;
            };

            let (ext_perms, fetch_error) = match provider.fetch_user_perms(account, opts).await {
                Ok(perms) => {
                    results.statuses.push(CodeHostState::new_provider_status(
                        provider.service_id(),
                        provider.service_type(),
                        None,
                        "FetchUserPerms",
                    ));
                    (perms, None)
                }
                Err(err) => {
                    results.statuses.push(CodeHostState::new_provider_status(
                        provider.service_id(),
                        provider.service_type(),
                        Some(err.message()),
                        "FetchUserPerms",
                    ));
                    // The baseline provider may return partial permissions
                    // together with the error.
                    let partial = match &err {
                        UserPermsError::Failed { partial, .. } => partial.clone(),
                        _ => None,
                    };
                    (partial, Some(err))
                }
            };

            let mut ext_perms = ext_perms;
            if let Some(err) = fetch_error {
                match err {
                    UserPermsError::Unauthorized { .. }
                    | UserPermsError::Forbidden { .. }
                    | UserPermsError::AccountSuspended { .. } => {
                        // Fatal for this account: treat it as having no
                        // access from now on.
                        if let Err(err) =
                            user_external_accounts::touch_expired(&self.pool, &[account.id]).await
                        {
                            results.error = Some(format!(
                                "set expired for external account ID {}: {err}",
                                account.id
                            ));
                            return results;
                        }
                        continue;
                    }
                    UserPermsError::Unimplemented { .. } => continue,
                    UserPermsError::Temporary { .. } => {
                        // Retain previously synced permissions rather than
                        // temporarily revoking access on an intermittent
                        // error.
                        ext_perms = Some(ExternalUserPermissions::default());
                        let current_repos =
                            match fetch_repos_by_external_account(&self.pool, account.id).await {
                                Ok(repos) => repos,
                                Err(err) => {
                                    results.error =
                                        Some(format!("fetching existing repo permissions: {err}"));
                                    return results;
                                }
                            };
                        results
                            .repo_perms
                            .entry(account.id)
                            .or_default()
                            .extend(current_repos);
                    }
                    UserPermsError::Failed { .. } => {}
                }

                // Process partial results on an initial fetch only.
                if !no_perms {
                    results.error = Some(format!(
                        "fetch user permissions for external account {}: {}",
                        account.id,
                        err.message()
                    ));
                    return results;
                }
            } else {
                // Happy path.
                if let Err(err) =
                    user_external_accounts::touch_last_valid(&self.pool, account.id).await
                {
                    results.error = Some(format!(
                        "set last valid for external account {}: {err}",
                        account.id
                    ));
                    return results;
                }
            }

            let Some(perms) = ext_perms else {
                continue;
            };

            let mut repo_specs = Vec::with_capacity(perms.exacts.len());
            for exact in perms.exacts {
                repo_specs.push(ExternalRepoSpec {
                    id: exact,
                    service_type: provider.service_type().to_owned(),
                    service_id: provider.service_id().to_owned(),
                });
            }

            let mut repo_names = match self.list_private_repo_names_by_specs(&repo_specs).await {
                Ok(names) => names,
                Err(err) => {
                    results.error = Some(format!(
                        "list private repositories by exact matching: {err}"
                    ));
                    return results;
                }
            };

            // Sub-repository permissions would be recorded here (not ported).

            let include_specs: Vec<ExternalRepoSpec> = perms
                .include_contains
                .into_iter()
                .map(|id| ExternalRepoSpec {
                    id,
                    service_type: provider.service_type().to_owned(),
                    service_id: provider.service_id().to_owned(),
                })
                .collect();
            let exclude_specs: Vec<ExternalRepoSpec> = perms
                .exclude_contains
                .into_iter()
                .map(|id| ExternalRepoSpec {
                    id,
                    service_type: provider.service_type().to_owned(),
                    service_id: provider.service_id().to_owned(),
                })
                .collect();

            // Exclusions are relative to inclusions.
            if !include_specs.is_empty() {
                match self
                    .repo_store
                    .list_private_repos_by_external_contains(&include_specs, &exclude_specs)
                    .await
                {
                    Ok(ids) => repo_names.extend(ids),
                    Err(err) => {
                        results.error = Some(format!(
                            "list external repositories by contains matching: {err}"
                        ));
                        return results;
                    }
                }
            }

            // Keep repos already retained from a temporary error.
            let repo_perms = results.repo_perms.entry(account.id).or_default();
            repo_perms.extend(repo_names);
        }

        results
    }
}
