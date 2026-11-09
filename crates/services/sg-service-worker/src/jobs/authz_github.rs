#![forbid(unsafe_code)]

//! `internal/authz/providers/github/github.go`'s repository-centric half:
//! `Provider.FetchRepoPerms` and `getRepoAffiliatedGroups`, plus
//! `cache.go`'s `cachedGroup`/`cachedGroups`.
//!
//! The GitHub REST client and the Redis-backed group cache are injected
//! through [`GithubClient`] and [`GroupsCache`]; [`RedisGroupsCache`] mirrors
//! the baseline's `rcache.NewWithTTL` key space. This module owns the
//! permission computation, deduplication and cache semantics.
//!
//! The user-centric `FetchUserPerms` path additionally needs external-account
//! decryption and OAuth token refresh, so it is injected as
//! [`GithubUserPermsProvider`] (see [`GithubRepoPermsProvider::with_user_perms`]).
//!
//! Also ported here: `Provider.FetchAccount` (always nil for GitHub, whose API
//! cannot map a user by external SSO account) and `Provider.ValidateConnection`
//! (`requiredAuthScopes` + `GetAuthenticatedOAuthScopes`).

use std::collections::{BTreeMap, HashSet};
use std::fmt;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use sg_db::user_external_accounts::{ExternalAccount, ExternalAccountSpec};
use sg_db::users::UserRecord;

use super::authz_github_user::GithubUserPermsProvider;
use super::perms_syncer::{
    AuthzProvider, ExternalUserPermissions, FetchError, FetchPermsOptions, FetchRepo,
    UserPermsError,
};

/// `extsvc.TypeGitHub`.
pub const TYPE_GITHUB: &str = "github";
/// `github.AffiliationDirect`.
pub const AFFILIATION_DIRECT: &str = "direct";

/// `cache.go`'s `cacheVersion`.
const CACHE_VERSION: &str = "v1";

/// `github.Collaborator` reduced to the field permissions use.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Collaborator {
    /// `DatabaseID`, serialized as the external account ID.
    pub database_id: i64,
}

/// `github.Team` reduced to the fields the provider reads.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Team {
    /// Team slug (not name).
    pub slug: String,
    pub repos_count: i64,
    pub organization: Option<Org>,
}

/// `github.Org`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Org {
    pub login: String,
}

/// `github.OrgDetails`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrgDetails {
    pub login: String,
    pub default_repository_permission: String,
}

/// `github.OrgMembership`: only the fields `canViewOrgRepos` reads.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrgMembership {
    pub state: String,
    pub role: String,
}

/// `github.OrgDetailsAndMembership`: the org details and the authenticated
/// user's membership in it. `None` mirrors the embedded nil pointers the
/// baseline checks for.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct OrgDetailsAndMembership {
    pub details: Option<OrgDetails>,
    pub membership: Option<OrgMembership>,
}

/// `github.Repository` reduced to the fields permissions use.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Repository {
    /// GitHub GraphQL node ID (`extsvc.RepoID` on the wire).
    pub id: String,
    /// `github.Visibility`; `"internal"` is the only value the provider
    /// compares against.
    pub visibility: String,
}

/// `github.APIError`-ish error: the message is already formatted like the
/// baseline's `err.Error()`, `not_found` mirrors `github.IsNotFound(err)`.
///
/// `status`/`api_message`/`documentation_url` carry the raw GitHub error so
/// callers can answer the `errcode` predicates (`Unauthorized`,
/// `AccountSuspended`, `Temporary`) the syncer classifies on, exactly as
/// `errors.As` would find the original `*github.APIError` through the
/// baseline's `errors.Wrap` chain.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GithubClientError {
    pub message: String,
    pub not_found: bool,
    /// `APIError.Code`; 0 when the error is not an HTTP API error.
    pub status: u16,
    /// `APIError.Message` (unwrapped).
    pub api_message: String,
    /// `APIError.DocumentationURL`.
    pub documentation_url: String,
}

impl GithubClientError {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            not_found: false,
            status: 0,
            api_message: String::new(),
            documentation_url: String::new(),
        }
    }

    /// `github.OrgNotFoundError` / `&github.APIError{Code: 404}` reduced to
    /// the predicate `github.IsNotFound` answers.
    pub fn not_found() -> Self {
        Self {
            message: "not found".to_owned(),
            not_found: true,
            status: 404,
            api_message: "not found".to_owned(),
            documentation_url: String::new(),
        }
    }

    /// `(*github.APIError).Unauthorized`.
    pub fn unauthorized(&self) -> bool {
        self.status == 401
    }

    /// `(*github.APIError).AccountSuspended`.
    pub fn account_suspended(&self) -> bool {
        self.status == 403 && self.api_message.contains("account was suspended")
    }

    /// `github.IsRateLimitExceeded` for an `APIError`.
    pub fn rate_limit_exceeded(&self) -> bool {
        self.status != 0
            && (self.api_message.contains("API rate limit exceeded")
                || self.documentation_url.contains("#rate-limiting"))
    }
}

impl fmt::Display for GithubClientError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for GithubClientError {}

/// The subset of `github.V3Client` / `ClientAdapter`'s `client` interface the
/// user-centric (`FetchUserPerms`) path calls. Requests are authenticated with
/// the user's OAuth token (`WithAuthenticator`), not the external service
/// token.
#[async_trait]
pub trait GithubUserClient: Send + Sync {
    /// `ListAffiliatedRepositories(VisibilityPrivate, page, 100, affiliations...)`.
    async fn list_affiliated_repositories(
        &self,
        page: i32,
        affiliations: &[&str],
    ) -> Result<(Vec<Repository>, bool), GithubClientError>;

    /// `GetAuthenticatedUserOrgsDetailsAndMembership`.
    async fn get_authenticated_user_orgs_details_and_membership(
        &self,
        page: i32,
    ) -> Result<(Vec<OrgDetailsAndMembership>, bool), GithubClientError>;

    /// `GetAuthenticatedUserOrgs`.
    async fn get_authenticated_user_orgs(
        &self,
        page: i32,
    ) -> Result<(Vec<Org>, bool), GithubClientError>;

    /// `GetAuthenticatedUserTeams`.
    async fn get_authenticated_user_teams(
        &self,
        page: i32,
    ) -> Result<(Vec<Team>, bool), GithubClientError>;

    /// `ListOrgRepositories(org, page, repoType)`, `type=` empty means all.
    async fn list_org_repositories(
        &self,
        org: &str,
        page: i32,
        repo_type: &str,
    ) -> Result<(Vec<Repository>, bool), GithubClientError>;

    /// `ListTeamRepositories(org, team, page)`.
    async fn list_team_repositories(
        &self,
        org: &str,
        team: &str,
        page: i32,
    ) -> Result<(Vec<Repository>, bool), GithubClientError>;
}

/// The subset of `github.V3Client` / `ClientAdapter` the repository-centric
/// provider calls. The real client is a later slice; tests inject a mock.
#[async_trait]
pub trait GithubClient: Send + Sync {
    async fn list_repository_collaborators(
        &self,
        owner: &str,
        repo: &str,
        page: i32,
        affiliation: Option<&str>,
    ) -> Result<(Vec<Collaborator>, bool), GithubClientError>;

    async fn list_repository_teams(
        &self,
        owner: &str,
        repo: &str,
        page: i32,
    ) -> Result<(Vec<Team>, bool), GithubClientError>;

    async fn list_organization_members(
        &self,
        owner: &str,
        page: i32,
        admins_only: bool,
    ) -> Result<(Vec<Collaborator>, bool), GithubClientError>;

    async fn list_team_members(
        &self,
        owner: &str,
        team: &str,
        page: i32,
    ) -> Result<(Vec<Collaborator>, bool), GithubClientError>;

    async fn get_organization(&self, login: &str) -> Result<OrgDetails, GithubClientError>;

    async fn get_repository(
        &self,
        owner: &str,
        name: &str,
    ) -> Result<Repository, GithubClientError>;

    /// `GetAuthenticatedOAuthScopes`: `GET /` and the `x-oauth-scopes`
    /// response header, split on `", "`.
    async fn get_authenticated_oauth_scopes(&self) -> Result<Vec<String>, GithubClientError>;
}

/// `Provider.requiredAuthScopes`'s message when the token lacks an org scope.
pub const REQUIRED_ORGANIZATION_SCOPES_MESSAGE: &str = "Scope `read:org`, `write:org`, or `admin:org` is required to enable `authorization.groupsCacheTTL` - please provide a `token` with the required scopes, or try updating the [**site configuration**](/site-admin/configuration)'s corresponding entry in [`auth.providers`](https://sourcegraph.com/docs/admin/auth) to enable `allowGroupsPermissionsSync`.";

/// `cache.go`'s `cachedGroup`. The JSON field names and `null` vs `[]` for
/// the two slices match the baseline's `json.Marshal` output byte for byte.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct CachedGroup {
    #[serde(rename = "Org", default)]
    pub org: String,
    #[serde(rename = "Team", default)]
    pub team: String,
    #[serde(rename = "Repositories", default)]
    pub repositories: Option<Vec<String>>,
    #[serde(rename = "Users", default)]
    pub users: Option<Vec<String>>,
}

impl CachedGroup {
    pub fn new(org: impl Into<String>, team: impl Into<String>) -> Self {
        Self {
            org: org.into(),
            team: team.into(),
            repositories: None,
            users: None,
        }
    }

    /// `cachedGroup.key`.
    pub fn key(&self) -> String {
        let mut key = format!("{CACHE_VERSION}/{}", self.org);
        if !self.team.is_empty() {
            key.push('/');
            key.push_str(&self.team);
        }
        key
    }
}

/// `cache.go`'s `httpcache.Cache` as used by `cachedGroups`. Production uses
/// [`RedisGroupsCache`]; tests use [`MemoryGroupsCache`]. The trait is async
/// because the `rcache` port is async; the baseline's `httpcache.Cache`
/// methods are synchronous but return no errors (they log and continue).
#[async_trait]
pub trait GroupsCache: Send + Sync {
    /// Always returns a group with `org`/`team` populated, even on a miss.
    async fn get(&self, org: &str, team: &str) -> (CachedGroup, bool);
    async fn set(&self, group: &CachedGroup) -> Result<(), String>;
    /// Deletes the key and clears the cached values.
    async fn invalidate(&self, group: &mut CachedGroup);
}

/// In-memory `httpcache.Cache` stand-in that stores the same JSON bytes the
/// baseline would write to Redis, so a miss/parse failure can be distinguished
/// from a cached empty value.
#[derive(Default)]
pub struct MemoryGroupsCache {
    entries: Mutex<BTreeMap<String, Vec<u8>>>,
}

impl MemoryGroupsCache {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn seed(&self, group: &CachedGroup) {
        let bytes = serde_json::to_vec(group).expect("serialize cached group");
        self.entries.lock().unwrap().insert(group.key(), bytes);
    }

    /// The cache keys the same way `httpcache` would expose them (sorted).
    pub fn keys(&self) -> Vec<String> {
        self.entries.lock().unwrap().keys().cloned().collect()
    }

    pub fn groups(&self) -> Vec<CachedGroup> {
        self.entries
            .lock()
            .unwrap()
            .values()
            .filter_map(|bytes| serde_json::from_slice(bytes).ok())
            .collect()
    }
}

#[async_trait]
impl GroupsCache for MemoryGroupsCache {
    async fn get(&self, org: &str, team: &str) -> (CachedGroup, bool) {
        let raw = CachedGroup::new(org, team);
        let Some(bytes) = self.entries.lock().unwrap().get(&raw.key()).cloned() else {
            return (raw, false);
        };
        match serde_json::from_slice::<CachedGroup>(&bytes) {
            Ok(group) => (group, true),
            Err(_) => (raw, false),
        }
    }

    async fn set(&self, group: &CachedGroup) -> Result<(), String> {
        let bytes = serde_json::to_vec(group).map_err(|err| err.to_string())?;
        self.entries.lock().unwrap().insert(group.key(), bytes);
        Ok(())
    }

    async fn invalidate(&self, group: &mut CachedGroup) {
        self.entries.lock().unwrap().remove(&group.key());
        group.repositories = None;
        group.users = None;
    }
}

/// `NewProvider`'s cache namespace:
/// `fmt.Sprintf("gh_groups_perms:%s:%s", codeHost.ServiceID, urn)`.
pub fn groups_cache_key_prefix(service_id: &str, urn: &str) -> String {
    format!("gh_groups_perms:{service_id}:{urn}")
}

/// The `rcache`-backed `cachedGroups`. Every value lives under
/// `v2:<prefix>:<v1/<org>[/<team>]>`, the baseline's exact key space
/// (`rcache.dataVersion` + `Cache.rkeyPrefix` + `cachedGroup.key`). With a
/// positive TTL `Set` issues `SETEX`; TTL 0 issues a plain `SET`, matching
/// `rcache.NewWithTTL`.
pub struct RedisGroupsCache {
    cache: sg_rcache::Cache,
}

impl RedisGroupsCache {
    pub fn new(cache: sg_rcache::Cache) -> Self {
        Self { cache }
    }

    /// `NewProvider`: `rcache.NewWithTTL(redispool.Cache, key_prefix, TTL seconds)`.
    pub fn with_ttl(kv: sg_redis::KeyValue, service_id: &str, urn: &str, ttl_seconds: i64) -> Self {
        Self::new(sg_rcache::Cache::new_with_ttl(
            kv,
            &groups_cache_key_prefix(service_id, urn),
            ttl_seconds,
        ))
    }

    pub fn cache(&self) -> &sg_rcache::Cache {
        &self.cache
    }
}

#[async_trait]
impl GroupsCache for RedisGroupsCache {
    async fn get(&self, org: &str, team: &str) -> (CachedGroup, bool) {
        let raw = CachedGroup::new(org, team);
        // `rcache.Cache.Get` logs Redis failures and reports a miss; callers
        // cannot distinguish an error from an absent key.
        let bytes = match self.cache.get(&raw.key()).await {
            Ok(bytes) => bytes,
            Err(err) => {
                tracing::warn!(key = %raw.key(), error = %err, "failed to execute redis command");
                None
            }
        };
        match bytes {
            Some(bytes) => match serde_json::from_slice::<CachedGroup>(&bytes) {
                Ok(group) => (group, true),
                Err(_) => (raw, false),
            },
            None => (raw, false),
        }
    }

    async fn set(&self, group: &CachedGroup) -> Result<(), String> {
        let bytes = serde_json::to_vec(group).map_err(|err| err.to_string())?;
        self.cache
            .set(&group.key(), &bytes)
            .await
            .map_err(|err| err.to_string())
    }

    async fn invalidate(&self, group: &mut CachedGroup) {
        if let Err(err) = self.cache.delete(&group.key()).await {
            tracing::warn!(key = %group.key(), error = %err, "failed to execute redis command");
        }
        group.repositories = None;
        group.users = None;
    }
}

/// `github.canViewOrgRepos`: membership admin, or an org default repository
/// permission of read/write/admin.
pub fn can_view_org_repos(org: Option<&OrgDetails>, membership: Option<&OrgMembership>) -> bool {
    let Some(org) = org else {
        return false;
    };
    if let Some(membership) = membership {
        if membership.state == "active" && membership.role == "admin" {
            return true;
        }
    }
    matches!(
        org.default_repository_permission.as_str(),
        "read" | "write" | "admin"
    )
}

/// `internal/authz/providers/github/client.go`'s `canViewOrgRepos`, which
/// takes the combined org details/membership value.
pub fn can_view_org_repos_details(org: &OrgDetailsAndMembership) -> bool {
    if let Some(membership) = &org.membership {
        if membership.state == "active" && membership.role == "admin" {
            return true;
        }
    }
    org.details.as_ref().is_some_and(|details| {
        matches!(
            details.default_repository_permission.as_str(),
            "read" | "write" | "admin"
        )
    })
}

/// `github.SplitRepositoryNameWithOwner`.
fn split_repository_name_with_owner(name_with_owner: &str) -> Result<(String, String), String> {
    let parts: Vec<&str> = name_with_owner.splitn(2, '/').collect();
    if parts.len() != 2 || parts[1].contains('/') || parts[0].is_empty() || parts[1].is_empty() {
        return Err(format!(
            "invalid GitHub repository \"owner/name\" string: {name_with_owner:?}"
        ));
    }
    Ok((parts[0].to_owned(), parts[1].to_owned()))
}

struct RepoAffiliatedGroup {
    group: CachedGroup,
    /// `repoAffiliatedGroup.adminsOnly`: sync only org admins rather than all
    /// org members.
    admins_only: bool,
}

/// GitHub authz provider (repository-centric half). The constructor mirrors
/// the subset of `github.Provider` state `FetchRepoPerms` reads.
pub struct GithubRepoPermsProvider {
    urn: String,
    service_id: String,
    service_type: String,
    /// `codeHost.BaseURL.Hostname()`, used to strip the host from `repo.URI`.
    base_host: String,
    client: Arc<dyn GithubClient>,
    /// `nil` when `authorization.groupsCacheTTL <= 0`.
    groups_cache: Option<Arc<dyn GroupsCache>>,
    enable_github_internal_repo_visibility: bool,
    /// `Provider.FetchUserPerms` needs the database and OAuth configuration,
    /// so it lives in a separately constructed component. `None` (tests that
    /// only exercise the repository-centric path) leaves the trait default,
    /// `Unimplemented`.
    user_perms: Option<Arc<dyn GithubUserPermsProvider>>,
}

impl GithubRepoPermsProvider {
    pub fn new(
        urn: impl Into<String>,
        service_id: impl Into<String>,
        service_type: impl Into<String>,
        base_host: impl Into<String>,
        client: Arc<dyn GithubClient>,
        groups_cache: Option<Arc<dyn GroupsCache>>,
        enable_github_internal_repo_visibility: bool,
    ) -> Self {
        Self {
            urn: urn.into(),
            service_id: service_id.into(),
            service_type: service_type.into(),
            base_host: base_host.into(),
            client,
            groups_cache,
            enable_github_internal_repo_visibility,
            user_perms: None,
        }
    }

    /// Attach the user-centric `FetchUserPerms` implementation (the
    /// `ProvidersFromConfig` construction does this; unit tests for the
    /// repository-centric path do not).
    pub fn with_user_perms(mut self, user_perms: Arc<dyn GithubUserPermsProvider>) -> Self {
        self.user_perms = Some(user_perms);
        self
    }

    /// The groups cache, for the user-centric component (`fetchCachedAuthenticatedUserPerms`).
    pub fn groups_cache(&self) -> Option<Arc<dyn GroupsCache>> {
        self.groups_cache.clone()
    }

    /// Whether a groups cache is attached (i.e. `authorization.groupsCacheTTL`
    /// is positive and the login provider did not force it off). Used by the
    /// `ProvidersFromConfig` wiring tests.
    pub fn groups_cache_enabled(&self) -> bool {
        self.groups_cache.is_some()
    }

    /// `Provider.requiredAuthScopes`: pulling group permissions needs one of
    /// the org scopes, but only when the groups cache is enabled. `None` means
    /// `ValidateConnection` has nothing to check.
    fn required_auth_scopes(&self) -> Option<[&'static str; 3]> {
        self.groups_cache.as_ref()?;
        Some(["read:org", "write:org", "admin:org"])
    }

    /// `getRepoAffiliatedGroups`: the org (admins-only when the org's default
    /// permission is not visible to all members) and, in that case, every team
    /// attached to the repository. A 404 from `GetOrganization` means the
    /// owner is a user, which has no groups and is not an error.
    async fn get_repo_affiliated_groups(
        &self,
        owner: &str,
        name: &str,
        opts: &FetchPermsOptions,
    ) -> Result<Vec<RepoAffiliatedGroup>, String> {
        let Some(cache) = self.groups_cache.as_deref() else {
            return Ok(Vec::new());
        };

        let org = match self.client.get_organization(owner).await {
            Ok(org) => org,
            Err(err) if err.not_found => return Ok(Vec::new()),
            Err(err) => return Err(err.message),
        };

        let mut groups = Vec::new();
        // `syncGroup` in the baseline.
        async fn sync_group(
            cache: &dyn GroupsCache,
            owner: &str,
            team: &str,
            admins_only: bool,
            invalidate_caches: bool,
        ) -> RepoAffiliatedGroup {
            let (mut group, exists) = cache.get(owner, team).await;
            if exists && invalidate_caches {
                cache.invalidate(&mut group).await;
            }
            RepoAffiliatedGroup { group, admins_only }
        }

        // If this repo is an internal repo, allow everyone in the org to read
        // it (behind the temporary feature flag).
        let mut is_repo_internally_visible = false;
        if self.enable_github_internal_repo_visibility {
            let repo = self
                .client
                .get_repository(owner, name)
                .await
                .map_err(|err| err.message)?;
            if repo.visibility == "internal" {
                is_repo_internally_visible = true;
            }
        }

        let all_org_members_can_read =
            is_repo_internally_visible || can_view_org_repos(Some(&org), None);
        if all_org_members_can_read {
            groups.push(sync_group(cache, owner, "", false, opts.invalidate_caches).await);
        } else {
            groups.push(sync_group(cache, owner, "", true, opts.invalidate_caches).await);

            let mut page = 1;
            loop {
                let (teams, has_next) = self
                    .client
                    .list_repository_teams(owner, name, page)
                    .await
                    .map_err(|err| err.message)?;
                for team in teams {
                    groups.push(
                        sync_group(cache, owner, &team.slug, false, opts.invalidate_caches).await,
                    );
                }
                if !has_next {
                    break;
                }
                page += 1;
            }
        }

        Ok(groups)
    }
}

#[async_trait]
impl AuthzProvider for GithubRepoPermsProvider {
    fn service_type(&self) -> &str {
        &self.service_type
    }

    fn service_id(&self) -> &str {
        &self.service_id
    }

    fn urn(&self) -> &str {
        &self.urn
    }

    /// `Provider.FetchAccount`: the GitHub API has no way to look up a user by
    /// external SSO account, so the baseline always returns nil.
    async fn fetch_account(
        &self,
        _user: &UserRecord,
    ) -> Result<Option<ExternalAccountSpec>, String> {
        Ok(None)
    }

    /// `Provider.FetchUserPerms`: private repositories the account can read.
    async fn fetch_user_perms(
        &self,
        account: &ExternalAccount,
        opts: &FetchPermsOptions,
    ) -> Result<Option<ExternalUserPermissions>, UserPermsError> {
        match &self.user_perms {
            Some(user_perms) => user_perms.fetch_user_perms(account, opts).await,
            None => Err(UserPermsError::Unimplemented {
                message: "FetchUserPerms is not implemented".to_owned(),
            }),
        }
    }

    /// `Provider.FetchRepoPerms`. May return a partial account list together
    /// with an error, exactly like the baseline.
    async fn fetch_repo_perms(
        &self,
        repo: &FetchRepo,
        opts: &FetchPermsOptions,
    ) -> Result<Vec<String>, FetchError> {
        if repo.service_id != self.service_id || repo.service_type != self.service_type {
            return Err(FetchError::Failed {
                message: format!(
                    "not a code host of the repository: want {:?} but have {:?}",
                    repo.service_id, self.service_id
                ),
                partial_account_ids: Vec::new(),
            });
        }

        let name_with_owner = repo.uri.strip_prefix(&self.base_host).unwrap_or(&repo.uri);
        let name_with_owner = name_with_owner.strip_prefix('/').unwrap_or(name_with_owner);
        let (owner, name) = split_repository_name_with_owner(name_with_owner).map_err(|err| {
            FetchError::Failed {
                message: format!("split nameWithOwner: {err}"),
                partial_account_ids: Vec::new(),
            }
        })?;

        let mut user_ids: Vec<String> = Vec::new();
        // `seenUsers` is only allocated when groups caching is enabled.
        let mut seen_users: Option<HashSet<String>> =
            self.groups_cache.as_ref().map(|_| HashSet::new());
        let affiliation = self.groups_cache.as_ref().map(|_| AFFILIATION_DIRECT);

        // Sync collaborators.
        let mut page = 1;
        loop {
            let (users, has_next) = match self
                .client
                .list_repository_collaborators(&owner, &name, page, affiliation)
                .await
            {
                Ok(result) => result,
                Err(err) => {
                    return Err(client_error("list users for repo", err, user_ids));
                }
            };
            for user in users {
                let id = user.database_id.to_string();
                match seen_users.as_mut() {
                    Some(seen) => {
                        if seen.insert(id.clone()) {
                            user_ids.push(id);
                        }
                    }
                    None => user_ids.push(id),
                }
            }
            if !has_next {
                break;
            }
            page += 1;
        }

        let Some(cache) = self.groups_cache.as_deref() else {
            return Ok(user_ids);
        };

        let groups = match self.get_repo_affiliated_groups(&owner, &name, opts).await {
            Ok(groups) => groups,
            Err(err) => {
                return Err(FetchError::Failed {
                    message: format!("get groups affiliated with repo: {err}"),
                    partial_account_ids: user_ids,
                });
            }
        };

        let repo_id = repo.external_repo_id.clone();
        for mut group in groups {
            // If this is a partial cache, add self to the group.
            if let Some(repositories) = group.group.repositories.as_mut() {
                if !repositories.is_empty() && !repositories.contains(&repo_id) {
                    repositories.push(repo_id.clone());
                    // The baseline ignores the cache write error here.
                    let _ = cache.set(&group.group).await;
                }
            }

            // A valid cached value is non-empty; use it and continue. An empty
            // list is treated as unpopulated, matching `len(group.Users) > 0`.
            if let Some(users) = group.group.users.as_ref() {
                if !users.is_empty() {
                    for user in users {
                        match seen_users.as_mut() {
                            Some(seen) => {
                                if seen.insert(user.clone()) {
                                    user_ids.push(user.clone());
                                }
                            }
                            None => user_ids.push(user.clone()),
                        }
                    }
                    continue;
                }
            }

            // Perform a full sync.
            let mut page = 1;
            loop {
                let (members, has_next) = if group.group.team.is_empty() {
                    match self
                        .client
                        .list_organization_members(&owner, page, group.admins_only)
                        .await
                    {
                        Ok(result) => result,
                        Err(err) => {
                            return Err(client_error("list users for group", err, user_ids));
                        }
                    }
                } else {
                    match self
                        .client
                        .list_team_members(&owner, &group.group.team, page)
                        .await
                    {
                        Ok(result) => result,
                        Err(err) => {
                            return Err(client_error("list users for group", err, user_ids));
                        }
                    }
                };

                let group_users = group.group.users.get_or_insert_with(Vec::new);
                for member in members {
                    let id = member.database_id.to_string();
                    group_users.push(id.clone());
                    match seen_users.as_mut() {
                        Some(seen) => {
                            if seen.insert(id.clone()) {
                                user_ids.push(id);
                            }
                        }
                        None => user_ids.push(id),
                    }
                }
                if !has_next {
                    break;
                }
                page += 1;
            }

            // Persist the group; the baseline ignores this error.
            let _ = cache.set(&group.group).await;
        }

        Ok(user_ids)
    }

    /// `Provider.ValidateConnection`. The baseline's `p.client()` cannot fail
    /// here (the client is built by the constructor), so the `"unable to get
    /// client"` branch has no Rust counterpart.
    async fn validate_connection(&self) -> Result<(), String> {
        let Some(required) = self.required_auth_scopes() else {
            return Ok(());
        };

        let scopes = self
            .client
            .get_authenticated_oauth_scopes()
            .await
            .map_err(|err| {
                format!(
                    "additional OAuth scopes are required, but failed to get available scopes: {err}"
                )
            })?;

        let got: HashSet<&str> = scopes.iter().map(String::as_str).collect();
        if required.iter().any(|scope| got.contains(scope)) {
            Ok(())
        } else {
            Err(REQUIRED_ORGANIZATION_SCOPES_MESSAGE.to_owned())
        }
    }
}

/// Map a client failure the way the syncer classifies it: a 404 discards the
/// partial list (`FetchError::NotFound`), anything else keeps it.
fn client_error(context: &str, err: GithubClientError, partial: Vec<String>) -> FetchError {
    if err.not_found {
        FetchError::NotFound
    } else {
        FetchError::Failed {
            message: format!("{context}: {}", err.message),
            partial_account_ids: partial,
        }
    }
}
