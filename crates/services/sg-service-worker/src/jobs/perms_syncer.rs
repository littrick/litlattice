#![forbid(unsafe_code)]

//! Repository-centric half of `cmd/worker/internal/authz/perms_syncer.go`
//! (`permsSyncerImpl.syncRepoPerms`).
//!
//! The provider itself is injected through [`AuthzProvider`]: the code-host
//! implementations (GitHub, GitLab, ...) and the `providers.ProvidersFromConfig`
//! construction are later slices. This module owns the parts that are
//! provider-independent:
//!
//! * select the provider whose URN matches one of the repository's sources
//!   (only for private repositories);
//! * call `FetchRepoPerms`, mapping the baseline's 404 / unimplemented
//!   special cases to a successful provider status;
//! * map external account IDs to internal user IDs and write
//!   `user_repo_permissions` + `repo_permissions` (`SetRepoPerms`);
//! * record the accounts without an internal user as pending permissions;
//! * serialise all permission-table writes with `permsUpdateLock` and keep the
//!   two writes in one transaction.
//!
//! Metrics, tracing and the delay log fields are not ported (consistent with
//! the other worker slices). The user-centric path (`syncUserPerms`) and the
//! worker job registration are later slices.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use async_trait::async_trait;
use sg_db::perms_store::{
    get_latest_finished_sync_job, get_user_ids_by_external_accounts,
    set_repo_pending_permissions_tx, set_repo_perms_tx, set_user_external_account_perms,
    CodeHostState, SetPermissionsResult, UserIdWithExternalAccountId, PERM_READ, SOURCE_REPO_SYNC,
    SOURCE_USER_SYNC,
};
use sg_db::repos::{ExternalRepoSpec, RepoStore};
use sg_db::sub_repo_perms::{
    upsert_with_spec, upsert_with_spec_with_ips, SubRepoPermissions, SubRepoPermissionsWithIps,
};
use sg_db::user_external_accounts::{ExternalAccount, ExternalAccountSpec};
use sg_db::users::{get_by_id as get_user_by_id, UserRecord};
use sqlx::postgres::PgPool;
use sqlx::types::time::OffsetDateTime;
use tokio::sync::Mutex;

/// `authz.FetchPermsOptions`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct FetchPermsOptions {
    pub invalidate_caches: bool,
}

/// The repository identity passed to `FetchRepoPerms`
/// (`extsvc.Repository` + `api.ExternalRepoSpec`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FetchRepo {
    pub uri: String,
    pub external_repo_id: String,
    pub service_type: String,
    pub service_id: String,
}

/// The three `FetchRepoPerms` outcomes the syncer treats specially.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FetchError {
    /// `github.APIError{Code: 404}`: a token with only read access cannot
    /// fetch permissions. Treated as success so the scheduler does not retry.
    NotFound,
    /// `authz.ErrUnimplemented`: skipped, treated as success.
    Unimplemented,
    /// Any other error. Like the baseline, the provider may return the
    /// partial account list it managed to fetch alongside the error; those
    /// partial results are used when `noPerms` is true.
    Failed {
        message: String,
        partial_account_ids: Vec<String>,
    },
}

/// `authz.ExternalUserPermissions`, without the sub-repository map (see
/// [`UserPermsFetcher`] for the sub-repository gap).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ExternalUserPermissions {
    pub exacts: Vec<String>,
    pub include_contains: Vec<String>,
    pub exclude_contains: Vec<String>,
    /// Keyed by external repository ID.
    pub sub_repo_permissions: BTreeMap<String, SubRepoPermissionsWithIps>,
}

/// One sub-repository permission write, preserving whether the provider
/// supplied IP addresses. The temporary-error retention path falls back to the
/// path-only store when the existing rows predate IP syncing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SubRepoUpsert {
    Paths(SubRepoPermissions),
    WithIps(SubRepoPermissionsWithIps),
}

/// The `FetchUserPerms` error classes the syncer branches on
/// (`errcode.IsUnauthorized` / `IsForbidden` / `IsAccountSuspended` /
/// `IsTemporary`, `authz.ErrUnimplemented`, everything else).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UserPermsError {
    Unauthorized {
        message: String,
    },
    Forbidden {
        message: String,
    },
    AccountSuspended {
        message: String,
    },
    /// `errcode.IsTemporary`: previously synced permissions are retained.
    Temporary {
        message: String,
    },
    Unimplemented {
        message: String,
    },
    /// Any other error; like the baseline the provider may return the partial
    /// permissions it managed to fetch alongside the error.
    Failed {
        message: String,
        partial: Option<ExternalUserPermissions>,
    },
}

impl UserPermsError {
    /// The error text used for the provider status and the wrapped error.
    pub fn message(&self) -> &str {
        match self {
            Self::Unauthorized { message }
            | Self::Forbidden { message }
            | Self::AccountSuspended { message }
            | Self::Temporary { message }
            | Self::Unimplemented { message }
            | Self::Failed { message, .. } => message,
        }
    }
}

/// `authz.Provider` subset used by the syncer.
#[async_trait]
pub trait AuthzProvider: Send + Sync {
    /// `ServiceType`, e.g. `github`.
    fn service_type(&self) -> &str;
    /// `ServiceID`, e.g. `https://github.com/`.
    fn service_id(&self) -> &str;
    /// `URN`, e.g. `extsvc:github:1`.
    fn urn(&self) -> &str;
    /// `FetchRepoPerms`: external account IDs with read access.
    async fn fetch_repo_perms(
        &self,
        repo: &FetchRepo,
        opts: &FetchPermsOptions,
    ) -> Result<Vec<String>, FetchError>;
    /// `FetchAccount`: the provider-specific account identity for a user.
    /// `Ok(None)` means the provider cannot determine one (not a failure).
    async fn fetch_account(
        &self,
        _user: &UserRecord,
    ) -> Result<Option<ExternalAccountSpec>, String> {
        Ok(None)
    }
    /// `FetchUserPerms`: private repositories the account can read.
    async fn fetch_user_perms(
        &self,
        _account: &ExternalAccount,
        _opts: &FetchPermsOptions,
    ) -> Result<Option<ExternalUserPermissions>, UserPermsError> {
        Err(UserPermsError::Unimplemented {
            message: "FetchUserPerms is not implemented".to_owned(),
        })
    }
}

/// `providers.ProvidersFromConfig` as seen by the syncer. The registry is
/// consulted on every sync, matching the baseline's `providerFactory`.
pub trait ProviderRegistry: Send + Sync {
    fn providers(&self) -> Vec<Arc<dyn AuthzProvider>>;
}

/// Result of one `syncRepoPerms` call. `result` is `None` when the baseline
/// would return a nil `*database.SetPermissionsResult`.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct RepoSyncOutcome {
    pub result: Option<SetPermissionsResult>,
    pub statuses: Vec<CodeHostState>,
    pub error: Option<String>,
}

/// `fetchUserPermsViaExternalAccountsResults`, reduced to the values the
/// `syncUserPerms` orchestration consumes: internal repository IDs per
/// external account, the provider statuses and the fetch error.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct UserPermsFetch {
    pub repo_perms: BTreeMap<i32, Vec<i32>>,
    pub sub_repo_perms: BTreeMap<ExternalRepoSpec, SubRepoUpsert>,
    pub statuses: Vec<CodeHostState>,
    pub error: Option<String>,
}

/// The user-centric half of `fetchUserPermsViaExternalAccounts`.
///
/// The concrete implementation (external-account listing, provider
/// `FetchAccount`/`FetchUserPerms`, sub-repository permissions and the
/// external-spec to internal-repo-ID lookup) is a later slice. Injecting it
/// keeps the orchestration independently verifiable, exactly like a fake
/// `authz.Provider` does for the repository-centric path.
#[async_trait]
pub trait UserPermsFetcher: Send + Sync {
    async fn fetch_user_perms(
        &self,
        user: &UserRecord,
        no_perms: bool,
        opts: &FetchPermsOptions,
    ) -> UserPermsFetch;
}

/// Result of one `syncUserPerms` call; mirrors [`RepoSyncOutcome`].
#[derive(Debug, Clone, Default, PartialEq)]
pub struct UserSyncOutcome {
    pub result: Option<SetPermissionsResult>,
    pub statuses: Vec<CodeHostState>,
    pub error: Option<String>,
}

/// `permsSyncerImpl` for repository- and user-centric sync.
pub struct PermsSyncer {
    pool: PgPool,
    repo_store: RepoStore,
    registry: Arc<dyn ProviderRegistry>,
    user_fetcher: Arc<dyn UserPermsFetcher>,
    /// `permsUpdateLock`: only one permission-table write at a time.
    perms_update_lock: Mutex<()>,
}

impl PermsSyncer {
    pub fn new(
        pool: PgPool,
        registry: Arc<dyn ProviderRegistry>,
        user_fetcher: Arc<dyn UserPermsFetcher>,
    ) -> Self {
        Self {
            repo_store: RepoStore::new(pool.clone()),
            pool,
            registry,
            user_fetcher,
            perms_update_lock: Mutex::new(()),
        }
    }

    /// `permsSyncerImpl.syncRepoPerms`. When `no_perms` is true, partial
    /// `FetchRepoPerms` results are written even if the provider failed.
    pub async fn sync_repo_perms(
        &self,
        repo_id: i32,
        no_perms: bool,
        fetch_opts: FetchPermsOptions,
    ) -> RepoSyncOutcome {
        let mut outcome = RepoSyncOutcome::default();

        let repo = match self.repo_store.get_by_id(repo_id).await {
            Ok(Some(repo)) => repo,
            Ok(None) => return outcome,
            Err(err) => {
                outcome.error = Some(format!("get repository: {err}"));
                return outcome;
            }
        };

        let provider = if repo.private {
            let by_urn: HashMap<String, Arc<dyn AuthzProvider>> = self
                .registry
                .providers()
                .into_iter()
                .map(|provider| (provider.urn().to_owned(), provider))
                .collect();
            repo.sources.iter().find_map(|source| {
                let urn = format!("extsvc:{}:{}", source.kind, source.external_service_id);
                by_urn.get(&urn).cloned()
            })
        } else {
            None
        };

        let Some(provider) = provider else {
            // No authz provider is configured for this repository: the
            // baseline skips the fetch and leaves permissions untouched.
            return outcome;
        };

        let fetch_repo = FetchRepo {
            uri: repo.uri.clone().unwrap_or_default(),
            external_repo_id: repo.external_id.clone().unwrap_or_default(),
            service_type: repo.external_service_type.clone().unwrap_or_default(),
            service_id: repo.external_service_id.clone().unwrap_or_default(),
        };

        let ext_account_ids = match provider.fetch_repo_perms(&fetch_repo, &fetch_opts).await {
            Ok(ids) => {
                // The baseline appends a status for every provider call,
                // including successful ones.
                outcome.statuses.push(CodeHostState::new_provider_status(
                    provider.service_id(),
                    provider.service_type(),
                    None,
                    "FetchRepoPerms",
                ));
                ids
            }
            Err(FetchError::NotFound) => {
                outcome.statuses.push(CodeHostState::new_provider_status(
                    provider.service_id(),
                    provider.service_type(),
                    None,
                    "FetchRepoPerms",
                ));
                return outcome;
            }
            Err(FetchError::Unimplemented) => {
                outcome.statuses.push(CodeHostState::new_provider_status(
                    provider.service_id(),
                    provider.service_type(),
                    None,
                    "FetchRepoPerms",
                ));
                return outcome;
            }
            Err(FetchError::Failed {
                message,
                partial_account_ids,
            }) => {
                outcome.statuses.push(CodeHostState::new_provider_status(
                    provider.service_id(),
                    provider.service_type(),
                    Some(&message),
                    "FetchRepoPerms",
                ));
                if !no_perms {
                    outcome.error = Some(format!(
                        "fetch repository permissions for repository {:?} (id: {}): {message}",
                        repo.name, repo.id
                    ));
                    return outcome;
                }
                partial_account_ids
            }
        };

        let mut accounts_to_users: HashMap<String, UserIdWithExternalAccountId> = HashMap::new();
        if !ext_account_ids.is_empty() {
            match get_user_ids_by_external_accounts(
                &self.pool,
                provider.service_type(),
                provider.service_id(),
                &ext_account_ids,
            )
            .await
            {
                Ok(users) => accounts_to_users = users,
                Err(err) => {
                    outcome.error = Some(format!(
                        "get user IDs by external accounts for repository {:?} (id: {}): {err}",
                        repo.name, repo.id
                    ));
                    return outcome;
                }
            }
        }

        // The baseline ignores errors from this lookup.
        let _last_sync_job = get_latest_finished_sync_job(&self.pool, None, Some(repo_id), true)
            .await
            .unwrap_or(None);

        let mut pending_account_ids: Vec<String> = ext_account_ids
            .iter()
            .filter(|account_id| !accounts_to_users.contains_key(*account_id))
            .cloned()
            .collect();
        pending_account_ids.sort();
        pending_account_ids.dedup();

        let mut users: Vec<UserIdWithExternalAccountId> =
            accounts_to_users.values().copied().collect();
        users.sort_by_key(|user| (user.user_id, user.external_account_id));

        let now = OffsetDateTime::now_utc();
        let _guard = self.perms_update_lock.lock().await;
        let mut tx = match self.pool.begin().await {
            Ok(tx) => tx,
            Err(err) => {
                outcome.error = Some(format!(
                    "start transaction for repository {:?} (id: {}): {err}",
                    repo.name, repo.id
                ));
                return outcome;
            }
        };

        match set_repo_perms_tx(&mut tx, repo_id, &users, SOURCE_REPO_SYNC, now).await {
            Ok(result) => outcome.result = Some(result),
            Err(err) => {
                outcome.error = Some(format!(
                    "set user repo permissions for repository {:?} (id: {}): {err}",
                    repo.name, repo.id
                ));
                return outcome;
            }
        }

        if let Err(err) = set_repo_pending_permissions_tx(
            &mut tx,
            repo_id,
            PERM_READ,
            provider.service_type(),
            provider.service_id(),
            &pending_account_ids,
            now,
        )
        .await
        {
            outcome.error = Some(format!(
                "set repository pending permissions for repository {:?} (id: {}): {err}",
                repo.name, repo.id
            ));
            return outcome;
        }

        if let Err(err) = tx.commit().await {
            outcome.error = Some(format!(
                "commit transaction for repository {:?} (id: {}): {err}",
                repo.name, repo.id
            ));
        }
        outcome
    }

    /// `permsSyncerImpl.syncUserPerms`: fetch the user's accessible private
    /// repositories through [`UserPermsFetcher`], then replace
    /// `user_repo_permissions` for every external account.
    ///
    /// Sub-repository permissions are not written by this slice: the fetcher
    /// contract does not carry them yet (the baseline upserts
    /// `sub_repo_permissions` between the fetch and the save loop).
    pub async fn sync_user_perms(
        &self,
        user_id: i32,
        no_perms: bool,
        fetch_opts: FetchPermsOptions,
    ) -> UserSyncOutcome {
        let mut outcome = UserSyncOutcome::default();

        let user = match get_user_by_id(&self.pool, user_id).await {
            Ok(Some(user)) => user,
            Ok(None) => {
                outcome.error = Some(format!("get user: user not found: id={user_id}"));
                return outcome;
            }
            Err(err) => {
                outcome.error = Some(format!("get user: {err}"));
                return outcome;
            }
        };

        let fetched = self
            .user_fetcher
            .fetch_user_perms(&user, no_perms, &fetch_opts)
            .await;
        outcome.statuses = fetched.statuses;
        if let Some(err) = fetched.error {
            // The baseline drops the (nil) result on a fetch error.
            outcome.error = Some(format!(
                "fetch permissions via external accounts for user {:?} (id: {}): {err}",
                user.username, user.id
            ));
            return outcome;
        }

        // The baseline logs and ignores errors from this lookup; it only feeds
        // the consecutive-sync delay metric.
        let _latest_sync_job = get_latest_finished_sync_job(&self.pool, Some(user_id), None, true)
            .await
            .unwrap_or(None);

        let now = OffsetDateTime::now_utc();
        let mut result = SetPermissionsResult::default();
        for (account_id, repo_ids) in &fetched.repo_perms {
            match self
                .save_user_perms_for_account(user_id, *account_id, repo_ids, now)
                .await
            {
                Ok(stats) => {
                    result.added += stats.added;
                    result.found += stats.found;
                    result.removed += stats.removed;
                }
                Err(err) => {
                    outcome.result = Some(result);
                    outcome.error = Some(format!(
                        "set user repo permissions for user {:?} (id: {}, external_account_id: {}): {err}",
                        user.username, user.id, account_id
                    ));
                    return outcome;
                }
            }
        }
        outcome.result = Some(result);

        // Set sub-repository permissions (the baseline does this after the
        // per-account repository writes and before the final lock).
        for (spec, upsert) in &fetched.sub_repo_perms {
            let write = match upsert {
                SubRepoUpsert::Paths(perms) => {
                    upsert_with_spec(&self.pool, user_id, spec, perms).await
                }
                SubRepoUpsert::WithIps(perms) => {
                    upsert_with_spec_with_ips(&self.pool, user_id, spec, perms).await
                }
            };
            if let Err(err) = write {
                outcome.error = Some(format!(
                    "upserting sub repo perms {spec} for user {:?} (id: {}): {err}",
                    user.username, user.id
                ));
                return outcome;
            }
        }

        // Mirrors the baseline's second acquisition of `permsUpdateLock`
        // around the metrics/logging block (no database writes there).
        let _guard = self.perms_update_lock.lock().await;
        outcome
    }

    /// `permsSyncerImpl.saveUserPermsForAccount`: replace one external
    /// account's `user_repo_permissions` rows. The baseline takes
    /// `permsUpdateLock` around each account's write.
    async fn save_user_perms_for_account(
        &self,
        user_id: i32,
        account_id: i32,
        repo_ids: &[i32],
        now: OffsetDateTime,
    ) -> Result<SetPermissionsResult, sqlx::Error> {
        let _guard = self.perms_update_lock.lock().await;
        set_user_external_account_perms(
            &self.pool,
            UserIdWithExternalAccountId {
                user_id,
                external_account_id: account_id,
            },
            repo_ids,
            SOURCE_USER_SYNC,
            now,
        )
        .await
    }
}
