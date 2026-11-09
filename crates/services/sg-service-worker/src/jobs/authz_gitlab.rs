#![forbid(unsafe_code)]

//! `internal/authz/providers/gitlab/sudo.go`: the `SudoProvider` half of the
//! GitLab authz provider.
//!
//! Ported here:
//!
//! * `SudoProvider.FetchRepoPerms` / `listMembers` — every member of the
//!   project with at least Reporter access (access level 20), direct and
//!   inherited, paged with `per_page=100`. Partial results are returned
//!   together with the error, exactly like the baseline.
//! * `SudoProvider.FetchAccount` / `fetchAccountByUsername` — the unique GitLab
//!   user matching the Sourcegraph username (`users?per_page=2&username=…`).
//! * `SudoProvider.ValidateConnection` — a `projects` request under a 5s
//!   deadline; only the unwrapped context error counts as a timeout.
//! * `SudoProvider.FetchUserPerms` / `listProjects` — the private (and,
//!   depending on `syncInternalRepoPermissions`, internal) projects the
//!   impersonated user can read, with GitLab's `min_access_level=20` filter or
//!   the `gitLabProjectVisibilityExperimental` feature flag's
//!   `ContentsVisible` filter.
//!
//! The GitLab REST client and the per-sudo client construction are injected
//! through [`GitlabClientProvider`] / [`GitlabClient`], mirroring
//! `ClientProvider.GetPATClient(sudoToken, sudo)`; [`super::authz_gitlab_client`]
//! is the production implementation.
//!
//! [`OAuthProvider`] (`oauth.go`) is the user-centric half: `FetchUserPerms`
//! reads the user's stored OAuth token, refreshes it through
//! `oauthutil`/`externalAccountTokenRefresher` when needed, and lists projects
//! with that token. `FetchRepoPerms` is intentionally unimplemented. The
//! `auth.providers` lookup that builds it (`newAuthzProvider`'s `oauth` branch)
//! lives in [`super::authz_providers`]; [`gitlab_oauth_providers`] and
//! [`get_oauth_context`] mirror `gitlab.GetOAuthContext`.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use sg_config::SiteConfiguration;
use sg_db::user_external_accounts::{ExternalAccount, ExternalAccountSpec};
use sg_db::users::UserRecord;
use sg_encryption::Key;

use super::authz_github_user::{
    decrypt_stored_token, AccountTokenRefresher, OAuthBearerToken, OAuthContext, OAuthTokenStore,
    TokenRefresher,
};
use super::perms_syncer::{
    AuthzProvider, ExternalUserPermissions, FetchError, FetchPermsOptions, FetchRepo,
    UserPermsError,
};

/// `extsvc.TypeGitLab`.
pub const TYPE_GITLAB: &str = "gitlab";

/// `sudo.listMembers`'s minimum access level: 20 is Reporter access, i.e. read
/// access to the project code.
pub const REPORTER_ACCESS_LEVEL: i64 = 20;

/// `SudoProvider.ValidateConnection`'s deadline.
pub const VALIDATE_CONNECTION_TIMEOUT: Duration = Duration::from_secs(5);

/// `gitlab.Project`, reduced to the fields permissions use.
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Deserialize)]
#[serde(default)]
pub struct Project {
    /// `ProjectCommon.ID` (a Go `int`).
    pub id: i64,
    /// `Project.Visibility`: `"private"`, `"internal"` or `"public"`.
    pub visibility: String,
    /// `Project.DefaultBranch`.
    pub default_branch: String,
}

impl Project {
    /// `Project.ContentsVisible`: a project whose default branch is known.
    pub fn contents_visible(&self) -> bool {
        !self.default_branch.is_empty()
    }
}

/// `gitlab.Member`, reduced to the fields permissions use.
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Deserialize)]
#[serde(default)]
pub struct Member {
    /// `Member.ID` (a Go `int32`).
    pub id: i64,
    /// `Member.AccessLevel`.
    pub access_level: i64,
}

/// `gitlab.AuthUser`, reduced to the fields the provider reads.
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Deserialize)]
#[serde(default)]
pub struct AuthUser {
    /// `AuthUser.ID` (a Go `int32`).
    pub id: i64,
    pub username: String,
}

/// A GitLab API error whose `message` is already formatted like the baseline's
/// `err.Error()`; `not_found` mirrors `gitlab.IsNotFound(err)`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GitlabClientError {
    pub message: String,
    pub not_found: bool,
    /// `err == ctx.Err()`: the error value *is* the request context's error.
    /// `ValidateConnection` branches on this exact equality, so only a client
    /// that returns the context error itself sets it.
    pub context_deadline: bool,
}

impl GitlabClientError {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            not_found: false,
            context_deadline: false,
        }
    }

    /// A plain `HTTP 404` (`gitlab.HTTPErrorCode(err) == 404`).
    pub fn not_found() -> Self {
        Self {
            message: "HTTP error status 404".to_owned(),
            not_found: true,
            context_deadline: false,
        }
    }

    /// The request context's own error (`context.DeadlineExceeded`).
    pub fn context_deadline() -> Self {
        Self {
            message: "context deadline exceeded".to_owned(),
            not_found: false,
            context_deadline: true,
        }
    }
}

impl std::fmt::Display for GitlabClientError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for GitlabClientError {}

/// The subset of `gitlab.Client` the provider calls, bound to one
/// (token, sudo) pair. URLs are the baseline's relative strings (`"projects"`,
/// `"projects/<id>/members/all?per_page=100"`), resolved against the instance's
/// `/api/v4/` root by the client.
#[async_trait]
pub trait GitlabClient: Send + Sync {
    /// `ListProjects(ctx, urlStr)`.
    async fn list_projects(
        &self,
        url: &str,
    ) -> Result<(Vec<Project>, Option<String>), GitlabClientError>;

    /// `ListMembers(ctx, urlStr)`.
    async fn list_members(
        &self,
        url: &str,
    ) -> Result<(Vec<Member>, Option<String>), GitlabClientError>;

    /// `ListUsers(ctx, urlStr)`.
    async fn list_users(
        &self,
        url: &str,
    ) -> Result<(Vec<AuthUser>, Option<String>), GitlabClientError>;
}

/// `gitlab.ClientProvider.GetPATClient(sudoToken, sudo)`: a client
/// authenticated with the provider's sudo token, impersonating `sudo` when it
/// is non-empty. An empty token yields the unauthenticated client, which
/// ignores `sudo` (the baseline returns `getClient(nil)`).
pub trait GitlabClientProvider: Send + Sync {
    fn pat_client(&self, sudo: &str) -> Arc<dyn GitlabClient>;

    /// `ClientProvider.NewClient(oauthToken)`: a client that authenticates
    /// with the user's OAuth bearer token (refreshing it when needed).
    fn oauth_client(&self, token: Arc<OAuthBearerToken>) -> Arc<dyn GitlabClient>;
}

/// One `auth.providers` entry of type `gitlab`. `url` is the raw configured
/// value (empty when unset), which `GetOAuthContext` uses for its prefix
/// match.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GitlabOAuthProvider {
    pub url: String,
    pub client_id: String,
    pub client_secret: String,
    /// `authnProvider.Gitlab.SyncInternalRepoPermissions`; `None` means the
    /// baseline's default (`true`).
    pub sync_internal_repo_permissions: Option<bool>,
}

/// `conf.SiteConfig().AuthProviders`, filtered to GitLab entries.
pub fn gitlab_oauth_providers(site: &SiteConfiguration) -> Vec<GitlabOAuthProvider> {
    let Some(entries) = site.raw().get("auth.providers").and_then(|v| v.as_array()) else {
        return Vec::new();
    };
    entries
        .iter()
        .filter(|entry| entry.get("type").and_then(|v| v.as_str()) == Some("gitlab"))
        .map(|entry| GitlabOAuthProvider {
            url: entry
                .get("url")
                .and_then(|v| v.as_str())
                .unwrap_or_default()
                .to_owned(),
            client_id: entry
                .get("clientID")
                .and_then(|v| v.as_str())
                .unwrap_or_default()
                .to_owned(),
            client_secret: entry
                .get("clientSecret")
                .and_then(|v| v.as_str())
                .unwrap_or_default()
                .to_owned(),
            sync_internal_repo_permissions: entry
                .get("syncInternalRepoPermissions")
                .and_then(|v| v.as_bool()),
        })
        .collect()
}

/// `gitlab.GetOAuthContext(baseURL)`: the first GitLab authn provider whose
/// URL (one trailing slash trimmed) prefixes `base_url`. An empty configured
/// URL matches everything, exactly like the baseline's `strings.HasPrefix`.
pub fn get_oauth_context(
    providers: &[GitlabOAuthProvider],
    base_url: &str,
) -> Option<OAuthContext> {
    for provider in providers {
        let gl_url = provider.url.strip_suffix('/').unwrap_or(&provider.url);
        if !base_url.starts_with(gl_url) {
            continue;
        }
        return Some(OAuthContext {
            client_id: provider.client_id.clone(),
            client_secret: provider.client_secret.clone(),
            token_url: format!("{gl_url}/oauth/token"),
            custom_query_params: std::collections::BTreeMap::new(),
        });
    }
    None
}

/// `gitLabProjectVisibilityExperimental`, read by `listProjects` through
/// `featureflag.FromContext(ctx).GetBoolOr(…, false)`. The perms syncer
/// attaches the database-backed global store; callers without one see the
/// default (`false`).
pub const GITLAB_PROJECT_VISIBILITY_EXPERIMENTAL: &str = "gitLabProjectVisibilityExperimental";

/// The feature flag lookup `listProjects` needs.
#[async_trait]
pub trait GitlabFeatureFlags: Send + Sync {
    /// `GetBoolOr("gitLabProjectVisibilityExperimental", false)`.
    async fn project_visibility_experimental(&self) -> bool;
}

/// A fixed flag set for tests and callers without a database.
pub struct StaticGitlabFeatureFlags {
    pub project_visibility_experimental: bool,
}

impl StaticGitlabFeatureFlags {
    pub fn new(project_visibility_experimental: bool) -> Self {
        Self {
            project_visibility_experimental,
        }
    }
}

#[async_trait]
impl GitlabFeatureFlags for StaticGitlabFeatureFlags {
    async fn project_visibility_experimental(&self) -> bool {
        self.project_visibility_experimental
    }
}

/// `listMembers`'s error: the baseline returns the partial account list it
/// managed to fetch alongside the error.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListMembersError {
    pub message: String,
    pub partial_account_ids: Vec<String>,
}

/// `listMembers`: every project member with at least Reporter access,
/// following the `Link: rel="next"` pagination until the last page. Partial
/// results are returned with the error.
pub async fn list_members(
    client: &dyn GitlabClient,
    repo_id: &str,
) -> Result<Vec<String>, ListMembersError> {
    let mut next_url = format!("projects/{repo_id}/members/all?per_page=100");
    let mut user_ids: Vec<String> = Vec::new();

    loop {
        let (members, next) = match client.list_members(&next_url).await {
            Ok(result) => result,
            Err(err) => {
                return Err(ListMembersError {
                    message: err.message,
                    partial_account_ids: user_ids,
                })
            }
        };

        for member in members {
            // Members with access level 20 (i.e. Reporter) have access to the
            // project code.
            if member.access_level < REPORTER_ACCESS_LEVEL {
                continue;
            }
            user_ids.push(member.id.to_string());
        }

        match next {
            Some(next) => next_url = next,
            None => break,
        }
    }

    Ok(user_ids)
}

/// `listProjects`'s error: like `listMembers`, the baseline returns the partial
/// project list it managed to fetch alongside the error.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListProjectsError {
    pub message: String,
    pub partial_repo_ids: Vec<String>,
}

/// `listProjects`: the private projects (and, when `list_internal_repos`,
/// internal ones) the authenticated or impersonated user has Reporter access
/// to. `experimental_visibility` replaces GitLab's `min_access_level=20` filter
/// with the visibility rules plus the `ContentsVisible` check.
pub async fn list_projects(
    client: &dyn GitlabClient,
    list_internal_repos: bool,
    experimental_visibility: bool,
) -> Result<Vec<String>, ListProjectsError> {
    // `url.Values.Encode` sorts keys; `visibility` is set per iteration.
    let base_query = if experimental_visibility {
        "per_page=100&simple=true"
    } else {
        "min_access_level=20&per_page=100&simple=true"
    };

    let mut visibilities = vec!["private"];
    if list_internal_repos {
        visibilities.push("internal");
    }

    // 100 matches the maximum page size, thus a good default (the baseline's
    // initial slice capacity).
    let mut project_ids: Vec<String> = Vec::with_capacity(100);
    for visibility in visibilities {
        let mut next_url = format!("projects?{base_query}&visibility={visibility}");
        loop {
            let (projects, next) = match client.list_projects(&next_url).await {
                Ok(result) => result,
                Err(err) => {
                    return Err(ListProjectsError {
                        message: err.message,
                        partial_repo_ids: project_ids,
                    })
                }
            };

            for project in projects {
                // With the experimental flag, GitLab returns projects the user
                // can see but whose contents they cannot read; skip those.
                if experimental_visibility && !project.contents_visible() {
                    continue;
                }
                project_ids.push(project.id.to_string());
            }

            match next {
                Some(next) => next_url = next,
                None => break,
            }
        }
    }

    Ok(project_ids)
}

/// `GetExternalAccountData`'s `Data != nil` check, including the empty cipher
/// with an empty key ID (which `extsvc.NewEncryptedData` maps to nil).
fn encryptable_account_data(account: &ExternalAccount) -> Option<&str> {
    let data = account.account_data.as_deref()?;
    if data.is_empty() && account.encryption_key_id.is_empty() {
        return None;
    }
    Some(data)
}

/// `gitlab.GetExternalAccountData`'s user half. `Ok(None)` means the account
/// has no `Data`; any decryption or JSON error is returned as the baseline
/// propagates it.
async fn get_external_account_user(
    key: Option<&dyn Key>,
    account: &ExternalAccount,
) -> Result<Option<AuthUser>, String> {
    let Some(data) = encryptable_account_data(account) else {
        return Ok(None);
    };
    let plain = sg_encryption::maybe_decrypt(key, data, &account.encryption_key_id)
        .await
        .map_err(|err| err.to_string())?;
    // `json.Unmarshal` into a struct pointer accepts a JSON `null`.
    serde_json::from_str::<Option<AuthUser>>(&plain).map_err(|err| err.to_string())
}

/// `SudoProvider.ValidateConnection`'s generic failure text.
fn insufficient_privileges_message() -> String {
    "access token did not have sufficient privileges, requires scopes \"sudo\" and \"api\""
        .to_owned()
}

/// `url.QueryEscape`: unreserved characters (`A-Za-z0-9-_.~`) stay, a space
/// becomes `+`, everything else is percent-encoded.
pub fn query_escape(value: &str) -> String {
    let mut escaped = String::with_capacity(value.len());
    for byte in value.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                escaped.push(byte as char)
            }
            b' ' => escaped.push('+'),
            _ => escaped.push_str(&format!("%{byte:02X}")),
        }
    }
    escaped
}

/// `SudoProvider`: GitLab permissions computed with a sudo-scoped token that
/// impersonates the queried user. Only the repository-centric half is ported;
/// the constructor mirrors the state `FetchRepoPerms`, `FetchAccount` and
/// `ValidateConnection` read.
pub struct SudoProvider {
    urn: String,
    service_id: String,
    client_provider: Arc<dyn GitlabClientProvider>,
    /// `!connection.MarkInternalReposAsPublic`: `listProjects`'s
    /// `listInternalRepos`.
    sync_internal_repo_permissions: bool,
    /// The user-external-account key `FetchUserPerms` decrypts `account_data`
    /// with (`GetExternalAccountData`).
    user_external_account_key: Option<Arc<dyn Key>>,
    feature_flags: Arc<dyn GitlabFeatureFlags>,
}

impl SudoProvider {
    pub fn new(
        urn: impl Into<String>,
        service_id: impl Into<String>,
        client_provider: Arc<dyn GitlabClientProvider>,
        sync_internal_repo_permissions: bool,
    ) -> Self {
        Self {
            urn: urn.into(),
            service_id: service_id.into(),
            client_provider,
            sync_internal_repo_permissions,
            user_external_account_key: None,
            feature_flags: Arc::new(StaticGitlabFeatureFlags::new(false)),
        }
    }

    /// Wire the user-external-account key used to decrypt `account_data`.
    pub fn with_user_external_account_key(mut self, key: Option<Arc<dyn Key>>) -> Self {
        self.user_external_account_key = key;
        self
    }

    /// Wire the `gitLabProjectVisibilityExperimental` lookup.
    pub fn with_feature_flags(mut self, feature_flags: Arc<dyn GitlabFeatureFlags>) -> Self {
        self.feature_flags = feature_flags;
        self
    }

    /// `SudoProvider.syncInternalRepoPermissions`, read by `FetchUserPerms`
    /// (`listProjects`).
    pub fn sync_internal_repo_permissions(&self) -> bool {
        self.sync_internal_repo_permissions
    }

    /// `fetchAccountByUsername`.
    async fn fetch_account_by_username(&self, username: &str) -> Result<Option<AuthUser>, String> {
        // `url.Values.Encode` sorts keys: `per_page` before `username`.
        let query = format!("per_page=2&username={}", query_escape(username));
        let client = self.client_provider.pat_client("");
        let (users, _) = client
            .list_users(&format!("users?{query}"))
            .await
            .map_err(|err| err.message)?;
        if users.len() >= 2 {
            return Err(format!(
                "failed to determine unique GitLab user for query {query:?}"
            ));
        }
        Ok(users.into_iter().next())
    }
}

#[async_trait]
impl AuthzProvider for SudoProvider {
    fn service_type(&self) -> &str {
        TYPE_GITLAB
    }

    fn service_id(&self) -> &str {
        &self.service_id
    }

    fn urn(&self) -> &str {
        &self.urn
    }

    /// `FetchRepoPerms`: the project's members with read access, direct and
    /// inherited.
    async fn fetch_repo_perms(
        &self,
        repo: &FetchRepo,
        _opts: &FetchPermsOptions,
    ) -> Result<Vec<String>, FetchError> {
        if repo.service_id != self.service_id || repo.service_type != TYPE_GITLAB {
            return Err(FetchError::Failed {
                message: format!(
                    "not a code host of the repository: want {:?} but have {:?}",
                    repo.service_id, self.service_id
                ),
                partial_account_ids: Vec::new(),
            });
        }

        let client = self.client_provider.pat_client("");
        list_members(client.as_ref(), &repo.external_repo_id)
            .await
            .map_err(|err| FetchError::Failed {
                message: err.message,
                partial_account_ids: err.partial_account_ids,
            })
    }

    /// `FetchAccount`: the unique GitLab user with the Sourcegraph username.
    async fn fetch_account(
        &self,
        user: &UserRecord,
    ) -> Result<Option<ExternalAccountSpec>, String> {
        let Some(gitlab_user) = self.fetch_account_by_username(&user.username).await? else {
            return Ok(None);
        };
        Ok(Some(ExternalAccountSpec {
            service_type: TYPE_GITLAB.to_owned(),
            service_id: self.service_id.clone(),
            client_id: String::new(),
            account_id: gitlab_user.id.to_string(),
        }))
    }

    /// `FetchUserPerms`: the projects the account's GitLab user can read,
    /// fetched with a sudo client impersonating the user ID stored encrypted
    /// in `account_data`.
    async fn fetch_user_perms(
        &self,
        account: &ExternalAccount,
        _opts: &FetchPermsOptions,
    ) -> Result<Option<ExternalUserPermissions>, UserPermsError> {
        let failed = |message: String| UserPermsError::Failed {
            message,
            partial: None,
        };

        if account.service_id != self.service_id || account.service_type != TYPE_GITLAB {
            return Err(failed(format!(
                "not a code host of the account: want {:?} but have {:?}",
                account.service_id, self.service_id
            )));
        }

        let user =
            match get_external_account_user(self.user_external_account_key.as_deref(), account)
                .await
            {
                Ok(Some(user)) => user,
                // The baseline dereferences the nil user here; report it instead of
                // panicking (docs §69.3).
                Ok(None) => {
                    return Err(failed(
                        "get external account data: no account data".to_owned(),
                    ))
                }
                Err(err) => return Err(failed(format!("get external account data: {err}"))),
            };

        let experimental_visibility = self.feature_flags.project_visibility_experimental().await;
        let client = self.client_provider.pat_client(&user.id.to_string());
        match list_projects(
            client.as_ref(),
            self.sync_internal_repo_permissions,
            experimental_visibility,
        )
        .await
        {
            Ok(exacts) => Ok(Some(ExternalUserPermissions {
                exacts,
                ..ExternalUserPermissions::default()
            })),
            Err(err) => Err(UserPermsError::Failed {
                message: err.message,
                partial: Some(ExternalUserPermissions {
                    exacts: err.partial_repo_ids,
                    ..ExternalUserPermissions::default()
                }),
            }),
        }
    }

    /// `ValidateConnection`: a `projects` request proves the token has the
    /// `sudo` and `api` scopes. A 404 is tolerated (an empty instance); any
    /// other error means the token is insufficient. Only an error identical to
    /// the request context's error is reported as a timeout.
    async fn validate_connection(&self) -> Result<(), String> {
        let client = self.client_provider.pat_client("1");
        // The baseline passes a 5s `context.WithTimeout`. A transport timeout
        // surfaces *wrapped* (`request failed: …`), so `err == ctx.Err()` is
        // false and it takes the generic branch; an actual elapse here maps to
        // that same branch. The timeout message is reserved for a client that
        // returns the context's own error.
        let result = match tokio::time::timeout(
            VALIDATE_CONNECTION_TIMEOUT,
            client.list_projects("projects"),
        )
        .await
        {
            Ok(result) => result,
            Err(_) => return Err(insufficient_privileges_message()),
        };

        match result {
            Ok(_) => Ok(()),
            Err(err) if err.context_deadline => Err(format!(
                "GitLab API did not respond within 5s: {}",
                err.message
            )),
            Err(err) if err.not_found => Ok(()),
            Err(_) => Err(insufficient_privileges_message()),
        }
    }
}

/// `OAuthProvider` (`oauth.go`): GitLab permissions computed with the user's
/// own OAuth token. Repository-centric syncing is not implemented; the perms
/// syncer relies on the user-centric `FetchUserPerms` instead.
pub struct OAuthProvider {
    urn: String,
    service_id: String,
    client_provider: Arc<dyn GitlabClientProvider>,
    /// `!connection.MarkInternalReposAsPublic` (from the matching
    /// `auth.providers` entry): `listProjects`'s `listInternalRepos`.
    sync_internal_repo_permissions: bool,
    feature_flags: Arc<dyn GitlabFeatureFlags>,
    /// The user-external-account key `GetExternalAccountData` decrypts the
    /// account's `account_data`/`auth_data` with.
    user_external_account_key: Option<Arc<dyn Key>>,
    /// The store `externalAccountTokenRefresher` re-reads and updates.
    token_store: Arc<dyn OAuthTokenStore>,
    /// The HTTP client the token refresher posts with.
    http: reqwest::Client,
    /// `conf.SiteConfig().AuthProviders` entries of type `gitlab`, used for the
    /// runtime `gitlab.GetOAuthContext` lookup.
    oauth_providers: Vec<GitlabOAuthProvider>,
}

impl OAuthProvider {
    pub fn new(
        urn: impl Into<String>,
        service_id: impl Into<String>,
        client_provider: Arc<dyn GitlabClientProvider>,
        sync_internal_repo_permissions: bool,
        token_store: Arc<dyn OAuthTokenStore>,
        http: reqwest::Client,
        oauth_providers: Vec<GitlabOAuthProvider>,
    ) -> Self {
        Self {
            urn: urn.into(),
            service_id: service_id.into(),
            client_provider,
            sync_internal_repo_permissions,
            feature_flags: Arc::new(StaticGitlabFeatureFlags::new(false)),
            user_external_account_key: None,
            token_store,
            http,
            oauth_providers,
        }
    }

    /// Wire the user-external-account key used to decrypt
    /// `account_data`/`auth_data`.
    pub fn with_user_external_account_key(mut self, key: Option<Arc<dyn Key>>) -> Self {
        self.user_external_account_key = key;
        self
    }

    /// Wire the `gitLabProjectVisibilityExperimental` lookup.
    pub fn with_feature_flags(mut self, feature_flags: Arc<dyn GitlabFeatureFlags>) -> Self {
        self.feature_flags = feature_flags;
        self
    }

    pub fn sync_internal_repo_permissions(&self) -> bool {
        self.sync_internal_repo_permissions
    }
}

#[async_trait]
impl AuthzProvider for OAuthProvider {
    fn service_type(&self) -> &str {
        TYPE_GITLAB
    }

    fn service_id(&self) -> &str {
        &self.service_id
    }

    fn urn(&self) -> &str {
        &self.urn
    }

    /// `FetchRepoPerms`: `authz.ErrUnimplemented{}`; OAuth connections rely on
    /// user-centric syncs.
    async fn fetch_repo_perms(
        &self,
        _repo: &FetchRepo,
        _opts: &FetchPermsOptions,
    ) -> Result<Vec<String>, FetchError> {
        Err(FetchError::Unimplemented)
    }

    /// `FetchUserPerms`: the projects the user's stored OAuth token can read.
    /// The client refreshes the token (`oauthutil.DoRequest`) when it is
    /// expiring or the API answers 401.
    async fn fetch_user_perms(
        &self,
        account: &ExternalAccount,
        _opts: &FetchPermsOptions,
    ) -> Result<Option<ExternalUserPermissions>, UserPermsError> {
        let failed = |message: String| UserPermsError::Failed {
            message,
            partial: None,
        };

        if account.service_id != self.service_id || account.service_type != TYPE_GITLAB {
            return Err(failed(format!(
                "not a code host of the account: want {:?} but have {:?}",
                account.service_id, self.service_id
            )));
        }

        // `gitlab.GetExternalAccountData` decrypts the account data (as an
        // `AuthUser`) before reading the token; its value is discarded but a
        // decryption/decoding error still fails the fetch.
        if let Err(err) =
            get_external_account_user(self.user_external_account_key.as_deref(), account).await
        {
            return Err(failed(format!("get external account data: {err}")));
        }
        let token =
            match decrypt_stored_token(self.user_external_account_key.as_deref(), account).await {
                Ok(token) => token,
                Err(err) => return Err(failed(format!("get external account data: {err}"))),
            };
        let Some(token) = token else {
            return Err(failed(
                "no token found in the external account data".to_owned(),
            ));
        };

        // `GetAccountRefreshAndStoreOAuthTokenFunc(…, gitlab.GetOAuthContext(
        // strings.TrimSuffix(p.ServiceID(), "/")))`. Without a matching
        // `auth.providers` entry the baseline's nil context panics inside the
        // refresh closure; we instead build the token without a refresher, so
        // a refresh attempt reports "refresh not implemented" (docs §70.3).
        let base_url = self
            .service_id
            .strip_suffix('/')
            .unwrap_or(&self.service_id);
        let refresher: Option<Arc<dyn TokenRefresher>> =
            get_oauth_context(&self.oauth_providers, base_url).map(|oauth| {
                Arc::new(AccountTokenRefresher::new(
                    self.token_store.clone(),
                    self.user_external_account_key.clone(),
                    account.id,
                    oauth,
                    self.http.clone(),
                )) as Arc<dyn TokenRefresher>
            });
        let bearer = Arc::new(OAuthBearerToken::new(token, refresher));
        let client = self.client_provider.oauth_client(bearer);
        let experimental_visibility = self.feature_flags.project_visibility_experimental().await;

        match list_projects(
            client.as_ref(),
            self.sync_internal_repo_permissions,
            experimental_visibility,
        )
        .await
        {
            Ok(exacts) => Ok(Some(ExternalUserPermissions {
                exacts,
                ..ExternalUserPermissions::default()
            })),
            Err(err) => Err(UserPermsError::Failed {
                message: err.message,
                partial: Some(ExternalUserPermissions {
                    exacts: err.partial_repo_ids,
                    ..ExternalUserPermissions::default()
                }),
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn query_escape_matches_go() {
        assert_eq!(query_escape("alice"), "alice");
        assert_eq!(query_escape("a b"), "a+b");
        assert_eq!(query_escape("a/b@c"), "a%2Fb%40c");
        assert_eq!(query_escape("-_.~"), "-_.~");
        assert_eq!(query_escape("ü"), "%C3%BC");
    }

    #[test]
    fn oauth_providers_filter_gitlab_entries() {
        let site = SiteConfiguration::parse(
            r#"{"auth.providers":[
                {"type":"github","url":"https://github.com"},
                {"type":"gitlab","url":"https://gitlab.example.com/","clientID":"id","clientSecret":"secret","syncInternalRepoPermissions":false}
            ]}"#,
        )
        .expect("site config");
        assert_eq!(
            gitlab_oauth_providers(&site),
            vec![GitlabOAuthProvider {
                url: "https://gitlab.example.com/".to_owned(),
                client_id: "id".to_owned(),
                client_secret: "secret".to_owned(),
                sync_internal_repo_permissions: Some(false),
            }]
        );
    }

    #[test]
    fn oauth_context_uses_a_prefix_match() {
        let providers = vec![
            GitlabOAuthProvider {
                url: "https://gitlab.example.com/".to_owned(),
                client_id: "one".to_owned(),
                client_secret: "s1".to_owned(),
                sync_internal_repo_permissions: None,
            },
            GitlabOAuthProvider {
                url: String::new(),
                client_id: "any".to_owned(),
                client_secret: "s2".to_owned(),
                sync_internal_repo_permissions: None,
            },
        ];
        let context = get_oauth_context(&providers, "https://gitlab.example.com").expect("context");
        assert_eq!(context.client_id, "one");
        assert_eq!(context.token_url, "https://gitlab.example.com/oauth/token");
        // An empty configured URL prefixes everything (the later entry matches
        // once the earlier ones do not).
        let context = get_oauth_context(&providers, "https://other.example.com").expect("context");
        assert_eq!(context.client_id, "any");
        assert_eq!(context.token_url, "/oauth/token");
        assert!(get_oauth_context(&[], "https://gitlab.example.com").is_none());
    }

    #[test]
    fn project_contents_visible_with_default_branch() {
        assert!(!Project::default().contents_visible());
        assert!(Project {
            default_branch: "main".to_owned(),
            ..Project::default()
        }
        .contents_visible());
    }
}
