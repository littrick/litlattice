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
//!
//! The GitLab REST client and the per-sudo client construction are injected
//! through [`GitlabClientProvider`] / [`GitlabClient`], mirroring
//! `ClientProvider.GetPATClient(sudoToken, sudo)`; [`super::authz_gitlab_client`]
//! is the production implementation. `Provider.FetchUserPerms` (`listProjects`)
//! and the OAuth identity provider are later slices.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use sg_db::user_external_accounts::ExternalAccountSpec;
use sg_db::users::UserRecord;

use super::perms_syncer::{AuthzProvider, FetchError, FetchPermsOptions, FetchRepo};

/// `extsvc.TypeGitLab`.
pub const TYPE_GITLAB: &str = "gitlab";

/// `sudo.listMembers`'s minimum access level: 20 is Reporter access, i.e. read
/// access to the project code.
pub const REPORTER_ACCESS_LEVEL: i64 = 20;

/// `SudoProvider.ValidateConnection`'s deadline.
pub const VALIDATE_CONNECTION_TIMEOUT: Duration = Duration::from_secs(5);

/// `gitlab.Project`, reduced to the fields permissions use.
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Deserialize)]
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
pub struct Member {
    /// `Member.ID` (a Go `int32`).
    pub id: i64,
    /// `Member.AccessLevel`.
    pub access_level: i64,
}

/// `gitlab.AuthUser`, reduced to the fields the provider reads.
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Deserialize)]
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
    /// `!connection.MarkInternalReposAsPublic`. `listProjects` uses this as
    /// `listInternalRepos`; the repository-centric half does not read it yet.
    sync_internal_repo_permissions: bool,
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
        }
    }

    /// `SudoProvider.syncInternalRepoPermissions`, read by `FetchUserPerms`
    /// (`listProjects`) in a later slice.
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
    fn project_contents_visible_with_default_branch() {
        assert!(!Project::default().contents_visible());
        assert!(Project {
            default_branch: "main".to_owned(),
            ..Project::default()
        }
        .contents_visible());
    }
}
