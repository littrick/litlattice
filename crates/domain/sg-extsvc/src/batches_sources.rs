#![forbid(unsafe_code)]

//! `internal/batches/sources`: code-host source abstraction and the
//! GitHub / GitLab / Bitbucket Server implementations, driven through an
//! injectable [`HttpDoer`] (no network in tests).
//!
//! Ported from the OSS baseline:
//!
//! * `sources.go` — `Sourcer` error taxonomy, `GitserverPushConfig` and the
//!   per-code-host URL credential rules, `GetRemoteRepo`, `DefaultForkName`,
//!   `CopyRepoAsFork`, `buildChangesetSource` kind dispatch.
//! * `common.go` — `ChangesetNotFoundError` / `ChangesetNotMergeableError`,
//!   the source traits, `IsOutdated`, `BuildCommitOptsCommon`.
//! * `util.go` — `UnsupportedAuthenticatorError`.
//! * `github.go` — GraphQL load/create/create-draft/update/close/reopen/
//!   undraft/comment/merge for `github.com` and GitHub Enterprise.
//! * `gitlab.go` — REST merge-request operations. Notes/resource-state-event/
//!   pipeline decoration (`decorateMergeRequestData`) is an explicit seam: the
//!   base merge request is loaded and events are reported as not-ported.
//! * `bitbucketserver.go` — REST pull-request operations. Activity/commit/
//!   build-status decoration is likewise a seam.
//! * `azuredevops.go` — REST pull-request operations through
//!   [`crate::azuredevops::Client`] (load/create/create-draft/update/undraft/
//!   close/reopen/comment/merge/fork), including the build-status annotation
//!   the metadata carries.
//!
//! The batches-domain GitHub App installation-token authenticator
//! (`withGitHubAppAuthenticator`) is ported through the
//! [`BatchesGitHubAppStore`] / [`GitHubAppTokenSeams`] boundaries; commit
//! signing itself (gitserver push + REST duplicate) remains a seam.
//!
//! Explicit not-ported (enterprise / credential-store boundaries): Gerrit /
//! Perforce / Bitbucket Cloud sources, outbound webhook GraphQL payloads, and
//! `state.SetDerivedState` (gitserver).

use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use url::Url;

use crate::auth::{
    Authenticator as _, GitHubAppAuthenticator, InstallationAuthenticator, InstallationTokenCache,
};
use crate::azuredevops::{self, AzureDevOpsAuth, BaseUrl};
use crate::bitbucketcloud as bbc;
use crate::gerrit;
use crate::github;
use crate::gitlab::GitLabAuth;
use crate::perforce;
use crate::perforce_source::PerforceConnection;
use crate::HttpDoer;

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/// `sources.ChangesetNotFoundError`.
pub const CHANGESET_NOT_FOUND_PREFIX: &str = "Changeset with external ID ";
/// `sources.ChangesetNotMergeableError`.
pub const CHANGESET_NOT_MERGEABLE_PREFIX: &str = "changeset cannot be merged:\n";
/// `sources.ErrNoPushCredentials`.
pub const NO_PUSH_CREDENTIALS: &str = "invalid authenticator provided to push commits";
/// `sources.ErrNoSSHCredential`.
pub const NO_SSH_CREDENTIAL: &str = "authenticator doesn't support SSH";
/// `sources.ErrChangesetSourceCannotFork`.
pub const CANNOT_FORK: &str = "forking is enabled, but the changeset source does not support forks";
/// `sources.ErrMissingCredentials`.
pub const MISSING_CREDENTIALS: &str = "no credential found that can authenticate to the code host";
/// `sources.ErrExternalServiceNotGitHub`.
pub const EXTERNAL_SERVICE_NOT_GITHUB: &str =
    "cannot use GitHub App authentication with non-GitHub external service";
/// `sources.ErrNoGitHubAppConfigured`.
pub const NO_GITHUB_APP_CONFIGURED: &str =
    "no batches GitHub App found that can authenticate to this code host";
/// `sources.ErrNoGitHubAppInstallation`.
pub const NO_GITHUB_APP_INSTALLATION: &str =
    "no installations of GitHub App found for this account namespace";
/// `ToDraftChangesetSource`'s failure text.
pub const NO_DRAFT_SUPPORT: &str = "changeset source doesn't implement DraftChangesetSource";

/// How a source error maps onto the baseline's error interfaces.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SourceErrorKind {
    /// A plain error.
    Other,
    /// `errcode.MakeNonRetryable`.
    NonRetryable,
    /// `ChangesetNotFoundError`.
    NotFound,
    /// `errcode.IsArchived`.
    Archived,
    /// `ChangesetNotMergeableError`.
    NotMergeable,
}

/// The error type returned by a [`ChangesetSource`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceError {
    pub message: String,
    pub kind: SourceErrorKind,
}

impl SourceError {
    pub fn other(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            kind: SourceErrorKind::Other,
        }
    }

    pub fn non_retryable(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            kind: SourceErrorKind::NonRetryable,
        }
    }

    pub fn not_found(external_id: &str) -> Self {
        Self {
            message: format!("{CHANGESET_NOT_FOUND_PREFIX}{external_id} not found"),
            kind: SourceErrorKind::NotFound,
        }
    }

    pub fn archived(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            kind: SourceErrorKind::Archived,
        }
    }

    pub fn not_mergeable(message: impl Into<String>) -> Self {
        Self {
            message: format!("{CHANGESET_NOT_MERGEABLE_PREFIX}{}", message.into()),
            kind: SourceErrorKind::NotMergeable,
        }
    }

    pub fn not_ported() -> Self {
        Self::non_retryable("batches code-host source is not ported")
    }

    pub fn with_prefix(self, prefix: &str) -> Self {
        Self {
            message: format!("{prefix}: {}", self.message),
            kind: self.kind,
        }
    }
}

impl std::fmt::Display for SourceError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for SourceError {}

/// `sources.UnsupportedAuthenticatorError`.
pub fn unsupported_authenticator_error(source: &str, have: &str) -> SourceError {
    SourceError::other(format!(
        "authenticator type unsupported for {source} sources: {have}"
    ))
}

// ---------------------------------------------------------------------------
// Domain model
// ---------------------------------------------------------------------------

/// `types.SourceInfo`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RepoSource {
    #[serde(rename = "ID", default)]
    pub id: String,
    #[serde(rename = "CloneURL", default)]
    pub clone_url: String,
}

/// `api.ExternalRepoSpec`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExternalRepoSpec {
    #[serde(rename = "ID", default)]
    pub id: String,
    #[serde(rename = "ServiceType", default)]
    pub service_type: String,
    #[serde(rename = "ServiceID", default)]
    pub service_id: String,
}

/// `types.Repo`, reduced to the fields the sources and push config use.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Repo {
    pub id: i32,
    pub name: String,
    #[serde(default)]
    pub external_repo: ExternalRepoSpec,
    #[serde(default)]
    pub sources: BTreeMap<String, RepoSource>,
    /// `repo.metadata` (code-host repository object; the corpus passes it as
    /// plain JSON because it is only consumed by the matching source kind).
    #[serde(default)]
    pub metadata: Value,
}

impl Repo {
    /// `types.Repo.CloneURLs`, in `Sources` iteration order (Go map order is
    /// not stable; the caller uses [`get_clone_url`] which sorts HTTPS first
    /// stably).
    pub fn clone_urls(&self) -> Vec<String> {
        self.sources
            .values()
            .filter(|source| !source.clone_url.is_empty())
            .map(|source| source.clone_url.clone())
            .collect()
    }

    /// `repo.ExternalServiceIDs` is not used by the ported helpers; the
    /// external service identity comes from `external_repo`.
    pub fn external_service_ids(&self) -> Vec<String> {
        if self.external_repo.service_id.is_empty() {
            Vec::new()
        } else {
            vec![self.external_repo.service_id.clone()]
        }
    }
}

/// `protocol.PushConfig`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PushConfig {
    pub remote_url: String,
    pub private_key: String,
    pub passphrase: String,
}

/// `protocol.PatchCommitInfo`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PatchCommitInfo {
    pub messages: Vec<String>,
    pub author_name: String,
    pub author_email: String,
    /// ISO-8601 timestamp; the corpus passes the Go `time.Time` JSON form.
    pub date: String,
}

/// `protocol.CreateCommitFromPatchRequest`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct CreateCommitFromPatchRequest {
    pub repo: String,
    pub base_commit: String,
    pub patch: Vec<u8>,
    pub target_ref: String,
    pub commit_info: PatchCommitInfo,
    pub patch_filenames_no_prefix: bool,
    pub push: Option<PushConfig>,
}

/// `btypes.ChangesetSpec`, reduced to `BuildCommitOptsCommon`'s inputs.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ChangesetSpec {
    pub diff: Vec<u8>,
    pub base_rev: String,
    pub head_ref: String,
    pub commit_message: String,
    pub commit_author_name: String,
    pub commit_author_email: String,
    pub created_at: String,
    pub fork_namespace: Option<String>,
}

impl ChangesetSpec {
    /// `ChangesetSpec.IsFork`.
    pub fn is_fork(&self) -> bool {
        self.fork_namespace.is_some()
    }

    /// `ChangesetSpec.GetForkNamespace`.
    pub fn fork_namespace(&self) -> Option<&String> {
        self.fork_namespace.as_ref()
    }
}

/// `btypes.Changeset`, reduced to the fields sources touch.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Changeset {
    pub id: i64,
    pub external_id: String,
    pub external_branch: String,
    pub external_fork_namespace: String,
    pub external_fork_name: String,
    /// Code-host metadata as canonical JSON (see [`ChangesetMetadata`]).
    pub metadata: Value,
}

/// `sources.Changeset`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SourceChangeset {
    pub title: String,
    pub body: String,
    pub head_ref: String,
    pub base_ref: String,
    pub remote_repo: Repo,
    pub target_repo: Repo,
    pub changeset: Changeset,
}

impl SourceChangeset {
    /// `sources.Changeset.SetMetadata`.
    pub fn set_metadata(&mut self, metadata: ChangesetMetadata) {
        self.changeset.metadata = serde_json::to_value(metadata).unwrap_or(Value::Null);
    }

    /// `sources.Changeset.Metadata` (canonical JSON).
    pub fn metadata(&self) -> Value {
        self.changeset.metadata.clone()
    }
}

/// Canonical cross-source metadata used by the differential harness.
///
/// The baseline stores the concrete code-host type (`*github.PullRequest`,
/// `*gitlab.MergeRequest`, `*bitbucketserver.PullRequest`); this port keeps a
/// normalized projection because only the reconciler reads these fields.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChangesetMetadata {
    pub kind: String,
    pub external_id: String,
    pub title: String,
    pub body: String,
    pub state: String,
    pub url: String,
    pub head_ref_name: String,
    pub base_ref_name: String,
    pub head_ref_oid: String,
    pub base_ref_oid: String,
    pub number: i64,
    pub is_draft: bool,
    pub author_login: String,
}

// ---------------------------------------------------------------------------
// Pure helpers (`sources.go` / `common.go`)
// ---------------------------------------------------------------------------

/// `gitdomain.AbbreviateRef`.
pub fn abbreviate_ref(reference: &str) -> String {
    reference
        .strip_prefix("refs/heads/")
        .unwrap_or(reference)
        .to_owned()
}

/// `gitdomain.EnsureRefPrefix`.
pub fn ensure_ref_prefix(reference: &str) -> String {
    format!(
        "refs/heads/{}",
        reference.strip_prefix("refs/heads/").unwrap_or(reference)
    )
}

/// `sources.Changeset.IsOutdated`.
pub fn is_outdated(current: &ChangesetMetadata, cs: &SourceChangeset) -> bool {
    if current.title != cs.title {
        return true;
    }
    if current.body != cs.body {
        return true;
    }
    ensure_ref_prefix(&current.base_ref_name) != ensure_ref_prefix(&cs.base_ref)
}

/// `sources.BuildCommitOptsCommon`.
pub fn build_commit_opts_common(
    repo: &Repo,
    spec: &ChangesetSpec,
    push_opts: &PushConfig,
) -> CreateCommitFromPatchRequest {
    // IMPORTANT: a trailing newline keeps `git apply` from reporting
    // "corrupt patch at line <N>".
    let mut patch = spec.diff.clone();
    patch.push(b'\n');
    CreateCommitFromPatchRequest {
        repo: repo.name.clone(),
        base_commit: spec.base_rev.clone(),
        patch,
        target_ref: spec.head_ref.clone(),
        commit_info: PatchCommitInfo {
            messages: vec![spec.commit_message.clone()],
            author_name: spec.commit_author_name.clone(),
            author_email: spec.commit_author_email.clone(),
            date: spec.created_at.clone(),
        },
        // Unified diffs have no `a/`/`b/` prefixes; `-p0` on git apply.
        patch_filenames_no_prefix: true,
        push: Some(push_opts.clone()),
    }
}

/// `sources.DefaultForkName`.
pub fn default_fork_name(namespace: &str, name: &str) -> String {
    format!("{namespace}-{name}")
}

/// `sources.CopyRepoAsFork`. Note the baseline only rewrites clone URLs and
/// metadata; name/external repo/fork flag stay untouched (errata in the
/// overview).
pub fn copy_repo_as_fork(
    repo: &Repo,
    metadata: Value,
    name_and_owner: &str,
    fork_name_and_owner: &str,
) -> Result<Repo, SourceError> {
    if repo.sources.is_empty() {
        return Err(SourceError::other("repo has no sources"));
    }
    let lower_name = name_and_owner.to_lowercase();
    let lower_fork = fork_name_and_owner.to_lowercase();
    let mut fork = repo.clone();
    fork.sources = repo
        .sources
        .iter()
        .filter(|(_, source)| !source.clone_url.is_empty())
        .map(|(urn, source)| {
            let fork_url = source.clone_url.to_lowercase().replacen(
                &lower_name,
                &lower_fork,
                1,
            );
            (
                urn.clone(),
                RepoSource {
                    id: source.id.clone(),
                    clone_url: fork_url,
                },
            )
        })
        .collect();
    fork.metadata = metadata;
    Ok(fork)
}

/// `sources.ToDraftChangesetSource`.
pub fn to_draft_changeset_source(source: &dyn ChangesetSource) -> Result<(), SourceError> {
    if source.supports_draft() {
        Ok(())
    } else {
        Err(SourceError::non_retryable(NO_DRAFT_SUPPORT))
    }
}

/// `sources.GetRemoteRepo`.
pub async fn get_remote_repo(
    source: &dyn ChangesetSource,
    target_repo: &Repo,
    changeset: &Changeset,
    spec: Option<&ChangesetSpec>,
) -> Result<Repo, SourceError> {
    if changeset.external_fork_namespace.is_empty()
        && (spec.is_none() || !spec.map(ChangesetSpec::is_fork).unwrap_or(false))
    {
        return Ok(target_repo.clone());
    }

    if !source.supports_fork() {
        return Err(SourceError::other(CANNOT_FORK));
    }

    if !changeset.external_fork_namespace.is_empty() {
        return source
            .get_fork(
                target_repo,
                Some(&changeset.external_fork_namespace),
                Some(&changeset.external_fork_name),
            )
            .await
            .map_err(|err| err.with_prefix("getting fork for changeset"));
    }

    source
        .get_fork(
            target_repo,
            spec.and_then(ChangesetSpec::fork_namespace),
            None,
        )
        .await
        .map_err(|err| err.with_prefix("getting fork for changeset spec"))
}

/// The canonical `Authenticator` kinds the push config understands.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PushAuthenticator {
    OAuthBearerToken { token: String },
    OAuthBearerTokenWithSSH {
        token: String,
        private_key: String,
        passphrase: String,
    },
    BasicAuth { username: String, password: String },
    BasicAuthWithSSH {
        username: String,
        password: String,
        private_key: String,
        passphrase: String,
    },
    GitHubAppInstallationToken { token: String },
}

/// `sources.getCloneURL`: HTTPS clone URLs sort before SSH ones (stable).
pub fn get_clone_url(repo: &Repo) -> Result<Url, SourceError> {
    let mut urls: Vec<(bool, Url)> = Vec::new();
    for raw in repo.clone_urls() {
        let parsed = Url::parse(&raw)
            .map_err(|err| SourceError::other(format!("parsing clone URL: {err}")))?;
        // `vcs.URL.IsSSH`: scheme is `ssh` (or empty for scp-like URLs).
        let is_ssh = parsed.scheme() == "ssh" || parsed.scheme().is_empty();
        urls.push((is_ssh, parsed));
    }
    if urls.is_empty() {
        return Err(SourceError::other("no clone URLs found for repo"));
    }
    // Stable sort: non-SSH first.
    urls.sort_by_key(|(is_ssh, _)| *is_ssh);
    Ok(urls.remove(0).1)
}

/// `sources.GitserverPushConfig`.
pub fn gitserver_push_config(
    repo: &Repo,
    auth: Option<&PushAuthenticator>,
) -> Result<PushConfig, SourceError> {
    let Some(auth) = auth else {
        return Err(SourceError::non_retryable(NO_PUSH_CREDENTIALS));
    };
    let mut clone_url =
        get_clone_url(repo).map_err(|err| err.with_prefix("getting clone URL"))?;
    if clone_url.scheme() == "ssh" || clone_url.scheme().is_empty() {
        return match auth {
            PushAuthenticator::OAuthBearerTokenWithSSH {
                private_key,
                passphrase,
                ..
            }
            | PushAuthenticator::BasicAuthWithSSH {
                private_key,
                passphrase,
                ..
            } => Ok(PushConfig {
                remote_url: clone_url.to_string(),
                private_key: private_key.clone(),
                passphrase: passphrase.clone(),
            }),
            _ => Err(SourceError::other(NO_SSH_CREDENTIAL)),
        };
    }

    let ext_svc_type = repo.external_repo.service_type.as_str();
    match auth {
        PushAuthenticator::OAuthBearerToken { token }
        | PushAuthenticator::OAuthBearerTokenWithSSH { token, .. } => {
            set_oauth_token_auth(&mut clone_url, ext_svc_type, token)?;
        }
        PushAuthenticator::BasicAuth { username, password }
        | PushAuthenticator::BasicAuthWithSSH {
            username, password, ..
        } => {
            set_basic_auth(&mut clone_url, ext_svc_type, username, password)?;
        }
        PushAuthenticator::GitHubAppInstallationToken { token } => {
            set_github_app_installation_token(&mut clone_url, ext_svc_type, token)?;
        }
    }
    Ok(PushConfig {
        remote_url: clone_url.to_string(),
        private_key: String::new(),
        passphrase: String::new(),
    })
}

/// `sources.setOAuthTokenAuth`.
pub fn set_oauth_token_auth(
    url: &mut Url,
    ext_svc_type: &str,
    token: &str,
) -> Result<(), SourceError> {
    let trimmed = token.trim();
    match ext_svc_type {
        "github" => {
            url.set_username(trimmed)
                .map_err(|_| SourceError::other("setting URL username"))?;
            url.set_password(None)
                .map_err(|_| SourceError::other("clearing URL password"))?;
        }
        "gitlab" => {
            url.set_username("git")
                .map_err(|_| SourceError::other("setting URL username"))?;
            url.set_password(Some(trimmed))
                .map_err(|_| SourceError::other("setting URL password"))?;
        }
        "bitbucketServer" => {
            return Err(SourceError::other(
                "require username/token to push commits to BitbucketServer",
            ))
        }
        other => {
            return Err(SourceError::other(format!(
                "setOAuthTokenAuth: invalid external service type {other:?}"
            )))
        }
    }
    Ok(())
}

/// `sources.setBasicAuth`.
pub fn set_basic_auth(
    url: &mut Url,
    ext_svc_type: &str,
    username: &str,
    password: &str,
) -> Result<(), SourceError> {
    match ext_svc_type {
        "github" | "gitlab" => Err(SourceError::other(format!(
            "need token to push commits to {ext_svc_type}"
        ))),
        "bitbucketServer" | "bitbucketCloud" | "azureDevops" | "gerrit" => {
            url.set_username(username)
                .map_err(|_| SourceError::other("setting URL username"))?;
            url.set_password(Some(password))
                .map_err(|_| SourceError::other("setting URL password"))?;
            Ok(())
        }
        other => Err(SourceError::other(format!(
            "setBasicAuth: invalid external service type {other:?}"
        ))),
    }
}

/// `sources.setGitHubAppInstallationToken`.
pub fn set_github_app_installation_token(
    url: &mut Url,
    ext_svc_type: &str,
    token: &str,
) -> Result<(), SourceError> {
    match ext_svc_type {
        "github" => {
            url.set_username("x-access-token")
                .map_err(|_| SourceError::other("setting URL username"))?;
            url.set_password(Some(token))
                .map_err(|_| SourceError::other("setting URL password"))?;
            Ok(())
        }
        other => Err(SourceError::other(format!(
            "setGitHubAppInstallationToken: invalid external service type {other:?}"
        ))),
    }
}

/// `github.buildCreatePullRequestInput` (head ref fork prefix).
pub fn build_create_pull_request_input(
    cs: &SourceChangeset,
) -> Result<Value, SourceError> {
    let mut head_ref = abbreviate_ref(&cs.head_ref);
    if cs.remote_repo.name != cs.target_repo.name {
        let owner = cs
            .remote_repo
            .metadata
            .get("owner")
            .and_then(Value::as_str)
            .or_else(|| {
                cs.remote_repo
                    .metadata
                    .get("name_with_owner")
                    .and_then(Value::as_str)
                    .and_then(|value| value.split('/').next())
            })
            .ok_or_else(|| SourceError::other("github repository owner missing"))?;
        head_ref = format!("{owner}:{head_ref}");
    }
    Ok(json!({
        "repositoryId": cs.target_repo.metadata.get("id").cloned().unwrap_or(Value::Null),
        "title": cs.title,
        "body": cs.body,
        "headRefName": head_ref,
        "baseRefName": abbreviate_ref(&cs.base_ref),
        "draft": false,
    }))
}

/// `sources.loadExternalService`'s accepted kinds.
pub fn is_batches_source_kind(kind: &str) -> bool {
    matches!(
        kind,
        "GITHUB" | "BITBUCKETSERVER" | "GITLAB" | "BITBUCKETCLOUD" | "AZUREDEVOPS" | "GERRIT"
            | "PERFORCE"
    )
}

/// `extsvc.KindToType`.
pub fn kind_to_type(kind: &str) -> String {
    match kind {
        "GITHUB" => "github",
        "GITLAB" => "gitlab",
        "BITBUCKETSERVER" => "bitbucketServer",
        "BITBUCKETCLOUD" => "bitbucketCloud",
        "AZUREDEVOPS" => "azureDevops",
        "GERRIT" => "gerrit",
        "PERFORCE" => "perforce",
        "OTHER" => "other",
        other => return other.to_lowercase(),
    }
    .to_owned()
}

/// `sources.buildChangesetSource`'s default branch.
pub fn unsupported_source_kind(kind: &str) -> SourceError {
    SourceError::other(format!(
        "unsupported external service type {:?}",
        kind_to_type(kind)
    ))
}

// ---------------------------------------------------------------------------
// Source traits
// ---------------------------------------------------------------------------

/// `sources.ChangesetSource`.
#[async_trait]
pub trait ChangesetSource: Send + Sync {
    fn kind(&self) -> &'static str;

    async fn load_changeset(&self, cs: &mut SourceChangeset) -> Result<(), SourceError>;

    /// `true` when the changeset already existed.
    async fn create_changeset(&self, cs: &mut SourceChangeset) -> Result<bool, SourceError>;

    /// Default: [`NO_DRAFT_SUPPORT`].
    async fn create_draft_changeset(&self, _cs: &mut SourceChangeset) -> Result<bool, SourceError> {
        Err(SourceError::non_retryable(NO_DRAFT_SUPPORT))
    }

    async fn update_changeset(&self, cs: &mut SourceChangeset) -> Result<(), SourceError>;

    async fn close_changeset(&self, cs: &mut SourceChangeset) -> Result<(), SourceError>;

    async fn reopen_changeset(&self, cs: &mut SourceChangeset) -> Result<(), SourceError>;

    /// Default: [`NO_DRAFT_SUPPORT`].
    async fn undraft_changeset(&self, _cs: &mut SourceChangeset) -> Result<(), SourceError> {
        Err(SourceError::non_retryable(NO_DRAFT_SUPPORT))
    }

    async fn create_comment(&self, cs: &SourceChangeset, text: &str) -> Result<(), SourceError>;

    async fn merge_changeset(
        &self,
        cs: &mut SourceChangeset,
        squash: bool,
    ) -> Result<(), SourceError>;

    fn supports_draft(&self) -> bool {
        false
    }

    fn supports_fork(&self) -> bool {
        false
    }

    async fn get_fork(
        &self,
        _target_repo: &Repo,
        _namespace: Option<&String>,
        _name: Option<&String>,
    ) -> Result<Repo, SourceError> {
        Err(SourceError::other(CANNOT_FORK))
    }

    /// `sources.Changeset.IsOutdated`.
    fn is_outdated(&self, current: &ChangesetMetadata, cs: &SourceChangeset) -> bool {
        is_outdated(current, cs)
    }

    /// `BuildCommitOpts`; all three ported sources use the common form.
    fn build_commit_opts(
        &self,
        repo: &Repo,
        spec: &ChangesetSpec,
        push_opts: &PushConfig,
    ) -> CreateCommitFromPatchRequest {
        build_commit_opts_common(repo, spec, push_opts)
    }

    /// `IsPushResponseArchived` (archivable sources override this).
    fn is_push_response_archived(&self, _output: &str) -> bool {
        false
    }

    /// `sources.Changeset.Events()`; the ported sources have no derived
    /// events without the full decoration API (see module docs).
    fn external_id(&self) -> Option<&str> {
        None
    }
}

// ---------------------------------------------------------------------------
// Shared HTTP machinery
// ---------------------------------------------------------------------------

fn header_set(headers: &mut Vec<(String, String)>, name: &str, value: &str) {
    headers.retain(|(existing, _)| !existing.eq_ignore_ascii_case(name));
    headers.push((name.to_owned(), value.to_owned()));
}

fn header_get<'a>(headers: &'a [(String, String)], name: &str) -> Option<&'a str> {
    headers
        .iter()
        .find(|(existing, _)| existing.eq_ignore_ascii_case(name))
        .map(|(_, value)| value.as_str())
}

/// The authenticator/source authentication mode.
#[derive(Debug, Clone)]
pub enum SourceAuth {
    None,
    /// GitHub OAuth/PAT (`token <token>`) or GitLab OAuth bearer.
    OAuthToken(String),
    /// GitLab personal access token (`PRIVATE-TOKEN`).
    GitLabPrivateToken(String),
    /// Basic auth (Bitbucket Server).
    Basic { username: String, password: String },
    /// Bitbucket Server bearer token.
    BearerToken(String),
    /// `github_apps/auth.InstallationAuthenticator`: the batches-domain GitHub
    /// App installation token installed by `withGitHubAppAuthenticator`.
    GitHubAppInstallation(Arc<crate::auth::InstallationAuthenticator>),
}

impl PartialEq for SourceAuth {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (SourceAuth::None, SourceAuth::None) => true,
            (SourceAuth::OAuthToken(a), SourceAuth::OAuthToken(b)) => a == b,
            (SourceAuth::GitLabPrivateToken(a), SourceAuth::GitLabPrivateToken(b)) => a == b,
            (
                SourceAuth::Basic {
                    username: ua,
                    password: pa,
                },
                SourceAuth::Basic {
                    username: ub,
                    password: pb,
                },
            ) => ua == ub && pa == pb,
            (SourceAuth::BearerToken(a), SourceAuth::BearerToken(b)) => a == b,
            // Installation access tokens are short-lived; compare the identity
            // the baseline's `Hash` uses (installation ID + App base URL).
            (
                SourceAuth::GitHubAppInstallation(a),
                SourceAuth::GitHubAppInstallation(b),
            ) => {
                a.installation_id() == b.installation_id() && a.base_url() == b.base_url()
            }
            _ => false,
        }
    }
}

impl Eq for SourceAuth {}

impl SourceAuth {
    fn apply(&self, headers: &mut Vec<(String, String)>) {
        match self {
            SourceAuth::None => {}
            SourceAuth::OAuthToken(token) => {
                header_set(headers, "Authorization", &format!("token {token}"));
            }
            SourceAuth::GitLabPrivateToken(token) => {
                header_set(headers, "PRIVATE-TOKEN", token);
            }
            SourceAuth::Basic { username, password } => {
                let auth = crate::auth::BasicAuth {
                    username: username.clone(),
                    password: password.clone(),
                };
                let _ = auth.authenticate("", "", headers);
            }
            SourceAuth::BearerToken(token) => {
                header_set(headers, "Authorization", &format!("Bearer {token}"));
            }
            SourceAuth::GitHubAppInstallation(auth) => {
                // Like the baseline client, use the currently-held token; the
                // gitserver push path refreshes it lazily.
                let _ = auth.authenticate("", "", headers);
            }
        }
    }
}

async fn send(
    http: &dyn HttpDoer,
    method: &str,
    url: &str,
    auth: &SourceAuth,
    body: Option<&Value>,
) -> Result<(u16, Vec<u8>), SourceError> {
    let mut headers = vec![
        ("Accept".to_owned(), "application/json".to_owned()),
        (
            "User-Agent".to_owned(),
            "sourcegraph-batches-sources".to_owned(),
        ),
    ];
    auth.apply(&mut headers);
    let body_bytes = match body {
        Some(value) => {
            header_set(&mut headers, "Content-Type", "application/json");
            serde_json::to_vec(value)
                .map_err(|err| SourceError::other(format!("marshalling request: {err}")))?
        }
        None => Vec::new(),
    };
    let response = http
        .do_request(crate::HttpRequest {
            method: method.to_owned(),
            url: url.to_owned(),
            headers,
            body: body_bytes,
        })
        .await
        .map_err(|err| SourceError::other(format!("request failed: {err}")))?;
    Ok((response.status, response.body))
}

fn parse_json(body: &[u8]) -> Result<Value, SourceError> {
    if body.is_empty() {
        return Ok(Value::Null);
    }
    serde_json::from_slice(body)
        .map_err(|err| SourceError::other(format!("decoding response: {err}")))
}

fn status_error(status: u16, body: &[u8], url: &str) -> SourceError {
    SourceError::other(format!("API HTTP error: code={status} url={url:?} body={body:?}"))
}

fn is_not_found_status(status: u16) -> bool {
    status == 404
}

fn value_str(value: &Value, key: &str) -> String {
    value
        .get(key)
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_owned()
}

fn value_i64(value: &Value, key: &str) -> i64 {
    match value.get(key) {
        Some(Value::Number(number)) => number.as_i64().unwrap_or_default(),
        Some(Value::String(text)) => text.parse().unwrap_or_default(),
        _ => 0,
    }
}

fn value_bool(value: &Value, key: &str) -> bool {
    value.get(key).and_then(Value::as_bool).unwrap_or(false)
}

fn nested<'a>(value: &'a Value, path: &[&str]) -> Option<&'a Value> {
    let mut cursor = value;
    for key in path {
        cursor = cursor.get(*key)?;
    }
    Some(cursor)
}

// ---------------------------------------------------------------------------
// GitHub (GraphQL)
// ---------------------------------------------------------------------------

/// The GraphQL endpoint for a GitHub API URL (`/graphql` on github.com,
/// `../graphql` on enterprise).
pub fn github_graphql_endpoint(api_url: &Url) -> Result<Url, SourceError> {
    let canonical = github::canonicalized_url(api_url);
    if github::url_is_github_dot_com(&canonical) {
        canonical
            .join("graphql")
            .map_err(|err| SourceError::other(err.to_string()))
    } else {
        canonical
            .join("../graphql")
            .map_err(|err| SourceError::other(err.to_string()))
    }
}

/// `github.V4Client` reduced to the source surface.
pub struct GitHubSource {
    api_url: Url,
    auth: SourceAuth,
    http: Arc<dyn HttpDoer>,
    auto_delete_branch: bool,
}

impl GitHubSource {
    pub fn new(api_url: Url, auth: SourceAuth, http: Arc<dyn HttpDoer>) -> Self {
        Self {
            api_url,
            auth,
            http,
            auto_delete_branch: false,
        }
    }

    pub fn with_auto_delete_branch(mut self, enabled: bool) -> Self {
        self.auto_delete_branch = enabled;
        self
    }

    async fn graphql(&self, query: &str, variables: Value) -> Result<Value, SourceError> {
        let endpoint = github_graphql_endpoint(&self.api_url)?;
        let payload = json!({"query": query, "variables": variables});
        let (status, body) = send(
            self.http.as_ref(),
            "POST",
            endpoint.as_str(),
            &self.auth,
            Some(&payload),
        )
        .await?;
        if status < 200 || status >= 400 {
            if is_not_found_status(status) {
                return Err(SourceError::not_found(""));
            }
            return Err(status_error(status, &body, endpoint.as_str()));
        }
        let value = parse_json(&body)?;
        if let Some(errors) = value.get("errors").and_then(Value::as_array) {
            if !errors.is_empty() {
                let message = errors
                    .iter()
                    .filter_map(|error| error.get("message").and_then(Value::as_str))
                    .collect::<Vec<_>>()
                    .join("\n");
                let lower = message.to_lowercase();
                if lower.contains("not found") {
                    return Err(SourceError::not_found(""));
                }
                return Err(SourceError::other(message));
            }
        }
        Ok(value.get("data").cloned().unwrap_or(Value::Null))
    }

    fn pull_request_metadata(pr: &Value) -> ChangesetMetadata {
        let number = value_i64(pr, "number");
        ChangesetMetadata {
            kind: "github".to_owned(),
            external_id: if number != 0 {
                number.to_string()
            } else {
                value_str(pr, "id")
            },
            title: value_str(pr, "title"),
            body: value_str(pr, "body"),
            state: value_str(pr, "state"),
            url: value_str(pr, "url"),
            head_ref_name: value_str(pr, "headRefName"),
            // `btypes.Changeset.BaseRef` prepends `refs/heads/` for GitHub.
            base_ref_name: ensure_ref_prefix(&value_str(pr, "baseRefName")),
            head_ref_oid: value_str(pr, "headRefOid"),
            base_ref_oid: value_str(pr, "baseRefOid"),
            number,
            is_draft: value_bool(pr, "isDraft"),
            author_login: nested(pr, &["author", "login"])
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_owned(),
        }
    }

    fn pull_request_id(cs: &SourceChangeset) -> Result<String, SourceError> {
        cs.changeset
            .metadata
            .get("id")
            .and_then(Value::as_str)
            .map(str::to_owned)
            .ok_or_else(|| SourceError::other("Changeset is not a GitHub pull request"))
    }

    fn require_github_metadata(
        cs: &SourceChangeset,
    ) -> Result<&Value, SourceError> {
        if cs.changeset.metadata.get("id").is_none() {
            return Err(SourceError::other("Changeset is not a GitHub pull request"));
        }
        Ok(&cs.changeset.metadata)
    }

    fn repo_name_with_owner(repo: &Repo) -> Result<String, SourceError> {
        repo.metadata
            .get("name_with_owner")
            .and_then(Value::as_str)
            .map(str::to_owned)
            .ok_or_else(|| SourceError::other("github repository name_with_owner missing"))
    }
}

#[async_trait]
impl ChangesetSource for GitHubSource {
    fn kind(&self) -> &'static str {
        "github"
    }

    async fn load_changeset(&self, cs: &mut SourceChangeset) -> Result<(), SourceError> {
        let repo = Self::repo_name_with_owner(&cs.target_repo)?;
        let (owner, name) = github::split_repository_name_with_owner(&repo)
            .map_err(SourceError::other)?;
        let number: i64 = cs
            .changeset
            .external_id
            .parse()
            .map_err(|err| SourceError::other(format!("parsing changeset external id: {err}")))?;
        let query = "query($owner:String!,$name:String!,$number:Int!){repository(owner:$owner,name:$name){pullRequest(number:$number){id title body state url headRefOid baseRefOid headRefName baseRefName number isDraft author{login}}}}";
        let data = self
            .graphql(
                query,
                json!({"owner": owner, "name": name, "number": number}),
            )
            .await
            .map_err(|err| {
                if err.kind == SourceErrorKind::NotFound {
                    SourceError::not_found(&cs.changeset.external_id)
                } else {
                    err
                }
            })?;
        let pr = nested(&data, &["repository", "pullRequest"])
            .ok_or_else(|| SourceError::not_found(&cs.changeset.external_id))?;
        let metadata = Self::pull_request_metadata(pr);
        cs.changeset.external_id = metadata.external_id.clone();
        cs.set_metadata(metadata);
        Ok(())
    }

    async fn create_changeset(&self, cs: &mut SourceChangeset) -> Result<bool, SourceError> {
        let input = build_create_pull_request_input(cs)?;
        let query = "mutation($input:CreatePullRequestInput!){createPullRequest(input:$input){pullRequest{id title body state url headRefOid baseRefOid headRefName baseRefName number isDraft author{login}}}}";
        let data = self.graphql(query, json!({"input": input})).await?;
        let pr = nested(&data, &["createPullRequest", "pullRequest"])
            .ok_or_else(|| SourceError::other("creating pull request: empty response"))?;
        let metadata = Self::pull_request_metadata(pr);
        cs.changeset.external_id = metadata.external_id.clone();
        cs.set_metadata(metadata);
        Ok(false)
    }

    async fn create_draft_changeset(
        &self,
        cs: &mut SourceChangeset,
    ) -> Result<bool, SourceError> {
        let mut input = build_create_pull_request_input(cs)?;
        input["draft"] = Value::Bool(true);
        let query = "mutation($input:CreatePullRequestInput!){createPullRequest(input:$input){pullRequest{id title body state url headRefOid baseRefOid headRefName baseRefName number isDraft author{login}}}}";
        let data = self.graphql(query, json!({"input": input})).await?;
        let pr = nested(&data, &["createPullRequest", "pullRequest"])
            .ok_or_else(|| SourceError::other("creating pull request: empty response"))?;
        let metadata = Self::pull_request_metadata(pr);
        cs.changeset.external_id = metadata.external_id.clone();
        cs.set_metadata(metadata);
        Ok(false)
    }

    async fn update_changeset(&self, cs: &mut SourceChangeset) -> Result<(), SourceError> {
        let _ = Self::require_github_metadata(cs)?;
        let id = Self::pull_request_id(cs)?;
        let input = json!({
            "pullRequestId": id,
            "title": cs.title,
            "body": cs.body,
            "baseRefName": abbreviate_ref(&cs.base_ref),
        });
        let query = "mutation($input:UpdatePullRequestInput!){updatePullRequest(input:$input){pullRequest{id title body state url headRefOid baseRefOid headRefName baseRefName number isDraft author{login}}}}";
        let data = self.graphql(query, json!({"input": input})).await?;
        let pr = nested(&data, &["updatePullRequest", "pullRequest"])
            .ok_or_else(|| SourceError::other("updating pull request: empty response"))?;
        let metadata = Self::pull_request_metadata(pr);
        cs.set_metadata(metadata);
        Ok(())
    }

    async fn close_changeset(&self, cs: &mut SourceChangeset) -> Result<(), SourceError> {
        let _ = Self::require_github_metadata(cs)?;
        let id = Self::pull_request_id(cs)?;
        let query = "mutation($input:ClosePullRequestInput!){closePullRequest(input:$input){pullRequest{id title body state url headRefOid baseRefOid headRefName baseRefName number isDraft author{login}}}}";
        let data = self
            .graphql(query, json!({"input": {"pullRequestId": id}}))
            .await?;
        let pr = nested(&data, &["closePullRequest", "pullRequest"])
            .ok_or_else(|| SourceError::other("closing pull request: empty response"))?;
        let metadata = Self::pull_request_metadata(pr);
        cs.set_metadata(metadata);
        Ok(())
    }

    async fn reopen_changeset(&self, cs: &mut SourceChangeset) -> Result<(), SourceError> {
        let _ = Self::require_github_metadata(cs)?;
        let id = Self::pull_request_id(cs)?;
        let query = "mutation($input:ReopenPullRequestInput!){reopenPullRequest(input:$input){pullRequest{id title body state url headRefOid baseRefOid headRefName baseRefName number isDraft author{login}}}}";
        let data = self
            .graphql(query, json!({"input": {"pullRequestId": id}}))
            .await?;
        let pr = nested(&data, &["reopenPullRequest", "pullRequest"])
            .ok_or_else(|| SourceError::other("reopening pull request: empty response"))?;
        let metadata = Self::pull_request_metadata(pr);
        cs.set_metadata(metadata);
        Ok(())
    }

    async fn undraft_changeset(&self, cs: &mut SourceChangeset) -> Result<(), SourceError> {
        let _ = Self::require_github_metadata(cs)?;
        let id = Self::pull_request_id(cs)?;
        let query = "mutation($input:MarkPullRequestReadyForReviewInput!){markPullRequestReadyForReview(input:$input){pullRequest{id title body state url headRefOid baseRefOid headRefName baseRefName number isDraft author{login}}}}";
        let data = self
            .graphql(query, json!({"input": {"pullRequestId": id}}))
            .await?;
        let pr = nested(&data, &["markPullRequestReadyForReview", "pullRequest"])
            .ok_or_else(|| SourceError::other("undrafting pull request: empty response"))?;
        let metadata = Self::pull_request_metadata(pr);
        cs.set_metadata(metadata);
        Ok(())
    }

    async fn create_comment(
        &self,
        cs: &SourceChangeset,
        text: &str,
    ) -> Result<(), SourceError> {
        let _ = Self::require_github_metadata(cs)?;
        let id = Self::pull_request_id(cs)?;
        let query = "mutation($input:AddCommentInput!){addComment(input:$input){subject{id}}}";
        self.graphql(query, json!({"input": {"subjectId": id, "body": text}}))
            .await?;
        Ok(())
    }

    async fn merge_changeset(
        &self,
        cs: &mut SourceChangeset,
        squash: bool,
    ) -> Result<(), SourceError> {
        let _ = Self::require_github_metadata(cs)?;
        let id = Self::pull_request_id(cs)?;
        let method = if squash { "SQUASH" } else { "MERGE" };
        let query = "mutation($input:MergePullRequestInput!){mergePullRequest(input:$input){pullRequest{id title body state url headRefOid baseRefOid headRefName baseRefName number isDraft author{login}}}}";
        let data = self
            .graphql(
                query,
                json!({"input": {"pullRequestId": id, "mergeMethod": method}}),
            )
            .await
            .map_err(|err| {
                if err.message.to_lowercase().contains("not mergeable")
                    || err.message.to_lowercase().contains("merge conflict")
                {
                    SourceError::not_mergeable(err.message)
                } else {
                    err
                }
            })?;
        let pr = nested(&data, &["mergePullRequest", "pullRequest"])
            .ok_or_else(|| SourceError::other("merging pull request: empty response"))?;
        let metadata = Self::pull_request_metadata(pr);
        cs.set_metadata(metadata);
        Ok(())
    }

    fn supports_draft(&self) -> bool {
        true
    }

    fn supports_fork(&self) -> bool {
        true
    }

    async fn get_fork(
        &self,
        target_repo: &Repo,
        namespace: Option<&String>,
        name: Option<&String>,
    ) -> Result<Repo, SourceError> {
        let repo_name = Self::repo_name_with_owner(target_repo)?;
        let (owner, project) = github::split_repository_name_with_owner(&repo_name)
            .map_err(SourceError::other)?;
        let fork_namespace = namespace.cloned().unwrap_or_else(|| owner.clone());
        let fork_name = name
            .cloned()
            .unwrap_or_else(|| default_fork_name(&owner, &project));
        // The fork API is a GraphQL mutation in the baseline; the source only
        // needs a repo with rewritten clone URLs for the push path.
        let fork_name_and_owner = format!("{fork_namespace}/{fork_name}");
        let metadata = json!({
            "name_with_owner": fork_name_and_owner,
            "owner": fork_namespace,
        });
        copy_repo_as_fork(
            target_repo,
            metadata,
            &repo_name,
            &fork_name_and_owner,
        )
    }

    fn is_push_response_archived(&self, output: &str) -> bool {
        output.contains("This repository was archived so it is read-only.")
    }

    fn external_id(&self) -> Option<&str> {
        None
    }
}

// ---------------------------------------------------------------------------
// GitLab (REST)
// ---------------------------------------------------------------------------

/// `gitlab.Client` reduced to the merge-request surface.
pub struct GitLabSource {
    api_url: Url,
    auth: SourceAuth,
    http: Arc<dyn HttpDoer>,
}

impl GitLabSource {
    pub fn new(base_url: &Url, auth: SourceAuth, http: Arc<dyn HttpDoer>) -> Self {
        Self {
            api_url: crate::gitlab::api_v4_url(base_url),
            auth,
            http,
        }
    }

    fn project_path(repo: &Repo) -> Result<String, SourceError> {
        repo.metadata
            .get("path_with_namespace")
            .and_then(Value::as_str)
            .or_else(|| repo.metadata.get("name").and_then(Value::as_str))
            .map(str::to_owned)
            .ok_or_else(|| SourceError::other("gitlab project path missing"))
    }

    fn endpoint(&self, path: &str) -> Result<Url, SourceError> {
        self.api_url
            .join(path)
            .map_err(|err| SourceError::other(err.to_string()))
    }

    fn metadata(mr: &Value) -> ChangesetMetadata {
        ChangesetMetadata {
            kind: "gitlab".to_owned(),
            external_id: value_i64(mr, "iid").to_string(),
            title: value_str(mr, "title"),
            body: value_str(mr, "description"),
            state: value_str(mr, "state"),
            url: value_str(mr, "web_url"),
            head_ref_name: value_str(mr, "source_branch"),
            // `btypes.Changeset.BaseRef` prepends `refs/heads/` for GitLab.
            base_ref_name: ensure_ref_prefix(&value_str(mr, "target_branch")),
            head_ref_oid: String::new(),
            base_ref_oid: String::new(),
            number: value_i64(mr, "iid"),
            is_draft: value_bool(mr, "work_in_progress") || value_bool(mr, "draft"),
            author_login: nested(mr, &["author", "username"])
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_owned(),
        }
    }

    fn mr_metadata(cs: &SourceChangeset) -> Result<&Value, SourceError> {
        if cs.changeset.metadata.get("iid").is_none() {
            return Err(SourceError::other(
                "Changeset is not a GitLab merge request",
            ));
        }
        Ok(&cs.changeset.metadata)
    }

    async fn request(
        &self,
        method: &str,
        url: &Url,
        body: Option<&Value>,
    ) -> Result<(u16, Vec<u8>), SourceError> {
        send(self.http.as_ref(), method, url.as_str(), &self.auth, body).await
    }

    async fn decode_mr(
        &self,
        response: (u16, Vec<u8>),
        url: &Url,
        external_id: &str,
    ) -> Result<Value, SourceError> {
        let (status, body) = response;
        if status < 200 || status >= 400 {
            if is_not_found_status(status) {
                return Err(SourceError::not_found(external_id));
            }
            return Err(status_error(status, &body, url.as_str()));
        }
        parse_json(&body)
    }
}

#[async_trait]
impl ChangesetSource for GitLabSource {
    fn kind(&self) -> &'static str {
        "gitlab"
    }

    async fn load_changeset(&self, cs: &mut SourceChangeset) -> Result<(), SourceError> {
        let project = Self::project_path(&cs.target_repo)?;
        let iid: i64 = cs.changeset.external_id.parse().map_err(|err| {
            SourceError::other(format!(
                "parsing changeset external ID {}: {err}",
                cs.changeset.external_id
            ))
        })?;
        let url = self.endpoint(&format!(
            "projects/{}/merge_requests/{iid}",
            crate::gitlab::path_escape(&project)
        ))?;
        let response = self.request("GET", &url, None).await?;
        let mr = self.decode_mr(response, &url, &cs.changeset.external_id).await?;
        let metadata = Self::metadata(&mr);
        cs.set_metadata(metadata);
        Ok(())
    }

    async fn create_changeset(&self, cs: &mut SourceChangeset) -> Result<bool, SourceError> {
        let remote = Self::project_path(&cs.remote_repo)?;
        let target_project_id = if cs.remote_repo.name != cs.target_repo.name {
            cs.target_repo
                .metadata
                .get("id")
                .and_then(Value::as_i64)
                .unwrap_or_default()
        } else {
            0
        };
        let payload = json!({
            "source_branch": abbreviate_ref(&cs.head_ref),
            "target_branch": abbreviate_ref(&cs.base_ref),
            "target_project_id": target_project_id,
            "title": cs.title,
            "description": cs.body,
            "remove_source_branch": false,
        });
        let url = self.endpoint(&format!(
            "projects/{}/merge_requests",
            crate::gitlab::path_escape(&remote)
        ))?;
        let response = self.request("POST", &url, Some(&payload)).await?;
        let mr = self.decode_mr(response, &url, "").await?;
        let metadata = Self::metadata(&mr);
        cs.changeset.external_id = metadata.external_id.clone();
        cs.set_metadata(metadata);
        Ok(false)
    }

    async fn update_changeset(&self, cs: &mut SourceChangeset) -> Result<(), SourceError> {
        let project = Self::project_path(&cs.target_repo)?;
        let mr = Self::mr_metadata(cs)?.clone();
        let iid = value_i64(&mr, "iid");
        let payload = json!({
            "title": cs.title,
            "description": cs.body,
            "target_branch": abbreviate_ref(&cs.base_ref),
            "remove_source_branch": false,
        });
        let url = self.endpoint(&format!(
            "projects/{}/merge_requests/{iid}",
            crate::gitlab::path_escape(&project)
        ))?;
        let response = self.request("PUT", &url, Some(&payload)).await?;
        let updated = self.decode_mr(response, &url, "").await?;
        cs.set_metadata(Self::metadata(&updated));
        Ok(())
    }

    async fn close_changeset(&self, cs: &mut SourceChangeset) -> Result<(), SourceError> {
        let project = Self::project_path(&cs.target_repo)?;
        let mr = Self::mr_metadata(cs)?.clone();
        let iid = value_i64(&mr, "iid");
        let payload = json!({
            "title": value_str(&mr, "title"),
            "target_branch": value_str(&mr, "target_branch"),
            "state_event": "close",
            "remove_source_branch": false,
        });
        let url = self.endpoint(&format!(
            "projects/{}/merge_requests/{iid}",
            crate::gitlab::path_escape(&project)
        ))?;
        let response = self.request("PUT", &url, Some(&payload)).await?;
        let updated = self.decode_mr(response, &url, "").await?;
        cs.set_metadata(Self::metadata(&updated));
        Ok(())
    }

    async fn reopen_changeset(&self, cs: &mut SourceChangeset) -> Result<(), SourceError> {
        let project = Self::project_path(&cs.target_repo)?;
        let mr = Self::mr_metadata(cs)?.clone();
        let iid = value_i64(&mr, "iid");
        let payload = json!({
            "title": value_str(&mr, "title"),
            "target_branch": value_str(&mr, "target_branch"),
            "state_event": "reopen",
            "remove_source_branch": false,
        });
        let url = self.endpoint(&format!(
            "projects/{}/merge_requests/{iid}",
            crate::gitlab::path_escape(&project)
        ))?;
        let response = self.request("PUT", &url, Some(&payload)).await?;
        let updated = self.decode_mr(response, &url, "").await?;
        cs.set_metadata(Self::metadata(&updated));
        Ok(())
    }

    async fn undraft_changeset(&self, cs: &mut SourceChangeset) -> Result<(), SourceError> {
        let _ = Self::mr_metadata(cs)?;
        cs.title = cs
            .title
            .strip_prefix("Draft:")
            .or_else(|| cs.title.strip_prefix("WIP:"))
            .unwrap_or(&cs.title)
            .trim()
            .to_owned();
        // Both flags mirror each other in GitLab; clear them before update.
        if let Some(object) = cs.changeset.metadata.as_object_mut() {
            object.insert("draft".to_owned(), Value::Bool(false));
            object.insert("work_in_progress".to_owned(), Value::Bool(false));
        }
        self.update_changeset(cs).await
    }

    async fn create_comment(
        &self,
        cs: &SourceChangeset,
        text: &str,
    ) -> Result<(), SourceError> {
        let project = Self::project_path(&cs.target_repo)?;
        let mr = Self::mr_metadata(cs)?;
        let iid = value_i64(mr, "iid");
        let url = self.endpoint(&format!(
            "projects/{}/merge_requests/{iid}/notes",
            crate::gitlab::path_escape(&project)
        ))?;
        let (status, body) = self
            .request("POST", &url, Some(&json!({"body": text})))
            .await?;
        if status < 200 || status >= 400 {
            return Err(status_error(status, &body, url.as_str()));
        }
        Ok(())
    }

    async fn merge_changeset(
        &self,
        cs: &mut SourceChangeset,
        squash: bool,
    ) -> Result<(), SourceError> {
        let project = Self::project_path(&cs.target_repo)?;
        let mr = Self::mr_metadata(cs)?.clone();
        let iid = value_i64(&mr, "iid");
        let url = self.endpoint(&format!(
            "projects/{}/merge_requests/{iid}/merge",
            crate::gitlab::path_escape(&project)
        ))?;
        let response = self
            .request("PUT", &url, Some(&json!({"squash": squash})))
            .await?;
        let updated = self
            .decode_mr(response, &url, "")
            .await
            .map_err(|err| {
                if err.message.contains("not mergeable") || err.message.contains("conflict") {
                    SourceError::not_mergeable(err.message)
                } else {
                    err
                }
            })?;
        cs.set_metadata(Self::metadata(&updated));
        Ok(())
    }

    fn supports_draft(&self) -> bool {
        true
    }

    fn supports_fork(&self) -> bool {
        true
    }

    async fn get_fork(
        &self,
        target_repo: &Repo,
        namespace: Option<&String>,
        name: Option<&String>,
    ) -> Result<Repo, SourceError> {
        let project = Self::project_path(target_repo)?;
        let mut parts = project.rsplitn(2, '/');
        let original_name = parts.next().unwrap_or(&project);
        let original_namespace = parts.next().unwrap_or_default();
        let fork_namespace = namespace
            .cloned()
            .unwrap_or_else(|| original_namespace.to_owned());
        let fork_name = name
            .cloned()
            .unwrap_or_else(|| default_fork_name(original_namespace, original_name));
        let fork_name_and_owner = format!("{fork_namespace}/{fork_name}");
        let metadata = json!({
            "path_with_namespace": fork_name_and_owner,
            "name": fork_name,
        });
        copy_repo_as_fork(
            target_repo,
            metadata,
            &project,
            &fork_name_and_owner,
        )
    }

    fn is_push_response_archived(&self, output: &str) -> bool {
        output.contains("ERROR: You are not allowed to push code to this project")
    }
}

// ---------------------------------------------------------------------------
// Bitbucket Server (REST)
// ---------------------------------------------------------------------------

/// `bitbucketserver.Client` reduced to the pull-request surface.
pub struct BitbucketServerSource {
    base_url: Url,
    auth: SourceAuth,
    http: Arc<dyn HttpDoer>,
}

impl BitbucketServerSource {
    pub fn new(base_url: &Url, auth: SourceAuth, http: Arc<dyn HttpDoer>) -> Self {
        Self {
            base_url: base_url.clone(),
            auth,
            http,
        }
    }

    fn repo_slug(repo: &Repo) -> Result<String, SourceError> {
        repo.metadata
            .get("slug")
            .and_then(Value::as_str)
            .map(str::to_owned)
            .ok_or_else(|| SourceError::other("bitbucket repository slug missing"))
    }

    fn project_key(repo: &Repo) -> Result<String, SourceError> {
        nested(&repo.metadata, &["project", "key"])
            .and_then(Value::as_str)
            .map(str::to_owned)
            .ok_or_else(|| SourceError::other("bitbucket project key missing"))
    }

    fn pull_request_url(&self, cs: &SourceChangeset, suffix: &str) -> Result<Url, SourceError> {
        let project = Self::project_key(&cs.target_repo)?;
        let slug = Self::repo_slug(&cs.target_repo)?;
        let id = value_i64(&cs.changeset.metadata, "id");
        self.base_url
            .join(&format!(
                "rest/api/1.0/projects/{project}/repos/{slug}/pull-requests/{id}{suffix}"
            ))
            .map_err(|err| SourceError::other(err.to_string()))
    }

    fn metadata(pr: &Value) -> ChangesetMetadata {
        ChangesetMetadata {
            kind: "bitbucketserver".to_owned(),
            external_id: value_i64(pr, "id").to_string(),
            title: value_str(pr, "title"),
            body: value_str(pr, "description"),
            state: value_str(pr, "state"),
            url: nested(pr, &["links", "self"])
                .and_then(Value::as_array)
                .and_then(|links| links.first())
                .and_then(|link| link.get("href"))
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_owned(),
            head_ref_name: nested(pr, &["fromRef", "id"])
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_owned(),
            base_ref_name: nested(pr, &["toRef", "id"])
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_owned(),
            head_ref_oid: nested(pr, &["fromRef", "latestCommit"])
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_owned(),
            base_ref_oid: nested(pr, &["toRef", "latestCommit"])
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_owned(),
            number: value_i64(pr, "id"),
            is_draft: false,
            author_login: nested(pr, &["author", "user", "name"])
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_owned(),
        }
    }

    fn require_pr(cs: &SourceChangeset) -> Result<&Value, SourceError> {
        if cs.changeset.metadata.get("id").is_none() {
            return Err(SourceError::other(
                "Changeset is not a Bitbucket Server pull request",
            ));
        }
        Ok(&cs.changeset.metadata)
    }

    async fn request(
        &self,
        method: &str,
        url: &Url,
        body: Option<&Value>,
    ) -> Result<(u16, Vec<u8>), SourceError> {
        send(self.http.as_ref(), method, url.as_str(), &self.auth, body).await
    }

    async fn decode_pr(
        &self,
        response: (u16, Vec<u8>),
        url: &Url,
        external_id: &str,
    ) -> Result<Value, SourceError> {
        let (status, body) = response;
        if status < 200 || status >= 400 {
            if is_not_found_status(status) {
                return Err(SourceError::not_found(external_id));
            }
            return Err(status_error(status, &body, url.as_str()));
        }
        parse_json(&body)
    }
}

#[async_trait]
impl ChangesetSource for BitbucketServerSource {
    fn kind(&self) -> &'static str {
        "bitbucketserver"
    }

    async fn load_changeset(&self, cs: &mut SourceChangeset) -> Result<(), SourceError> {
        let project = Self::project_key(&cs.target_repo)?;
        let slug = Self::repo_slug(&cs.target_repo)?;
        let id: i64 = cs
            .changeset
            .external_id
            .parse()
            .map_err(|err| SourceError::other(format!("parsing changeset external id: {err}")))?;
        let url = self
            .base_url
            .join(&format!(
                "rest/api/1.0/projects/{project}/repos/{slug}/pull-requests/{id}"
            ))
            .map_err(|err| SourceError::other(err.to_string()))?;
        let response = self.request("GET", &url, None).await?;
        let pr = self.decode_pr(response, &url, &cs.changeset.external_id).await?;
        cs.set_metadata(Self::metadata(&pr));
        Ok(())
    }

    async fn create_changeset(&self, cs: &mut SourceChangeset) -> Result<bool, SourceError> {
        let to_project = Self::project_key(&cs.target_repo)?;
        let to_slug = Self::repo_slug(&cs.target_repo)?;
        let target_id = cs.target_repo.metadata.get("id").cloned().unwrap_or(Value::Null);
        let from_project = Self::project_key(&cs.remote_repo)?;
        let from_slug = Self::repo_slug(&cs.remote_repo)?;
        let remote_id = cs.remote_repo.metadata.get("id").cloned().unwrap_or(Value::Null);
        let payload = json!({
            "title": cs.title,
            "description": cs.body,
            "toRef": {
                "id": ensure_ref_prefix(&cs.base_ref),
                "repository": {
                    "slug": to_slug,
                    "id": target_id,
                    "project": {"key": to_project},
                },
            },
            "fromRef": {
                "id": ensure_ref_prefix(&cs.head_ref),
                "repository": {
                    "slug": from_slug,
                    "id": remote_id,
                    "project": {"key": from_project},
                },
            },
        });
        let url = self
            .base_url
            .join(&format!(
                "rest/api/1.0/projects/{to_project}/repos/{to_slug}/pull-requests"
            ))
            .map_err(|err| SourceError::other(err.to_string()))?;
        let response = self.request("POST", &url, Some(&payload)).await?;
        let pr = self.decode_pr(response, &url, "").await?;
        let metadata = Self::metadata(&pr);
        cs.changeset.external_id = metadata.external_id.clone();
        cs.set_metadata(metadata);
        Ok(false)
    }

    async fn update_changeset(&self, cs: &mut SourceChangeset) -> Result<(), SourceError> {
        let pr = Self::require_pr(cs)?.clone();
        let url = self.pull_request_url(cs, "")?;
        let version = value_i64(&pr, "version");
        let payload = json!({
            "id": value_i64(&pr, "id"),
            "version": version,
            "title": cs.title,
            "description": cs.body,
            "toRef": {
                "id": cs.base_ref,
                "repository": {
                    "slug": Self::repo_slug(&cs.target_repo)?,
                    "project": {"key": Self::project_key(&cs.target_repo)?},
                },
            },
            "reviewers": pr.get("reviewers").cloned().unwrap_or(json!([])),
        });
        let response = self.request("PUT", &url, Some(&payload)).await?;
        let updated = self.decode_pr(response, &url, "").await?;
        cs.set_metadata(Self::metadata(&updated));
        Ok(())
    }

    async fn close_changeset(&self, cs: &mut SourceChangeset) -> Result<(), SourceError> {
        let pr = Self::require_pr(cs)?.clone();
        let version = value_i64(&pr, "version");
        let mut url = self.pull_request_url(cs, "/decline")?;
        url.query_pairs_mut()
            .append_pair("version", &version.to_string());
        let response = self.request("POST", &url, None).await?;
        let updated = self.decode_pr(response, &url, "").await?;
        cs.set_metadata(Self::metadata(&updated));
        Ok(())
    }

    async fn reopen_changeset(&self, cs: &mut SourceChangeset) -> Result<(), SourceError> {
        let pr = Self::require_pr(cs)?.clone();
        let version = value_i64(&pr, "version");
        let mut url = self.pull_request_url(cs, "/reopen")?;
        url.query_pairs_mut()
            .append_pair("version", &version.to_string());
        let response = self.request("POST", &url, None).await?;
        let updated = self.decode_pr(response, &url, "").await?;
        cs.set_metadata(Self::metadata(&updated));
        Ok(())
    }

    async fn create_comment(
        &self,
        cs: &SourceChangeset,
        text: &str,
    ) -> Result<(), SourceError> {
        let pr = Self::require_pr(cs)?;
        let version = value_i64(pr, "version");
        let mut url = self.pull_request_url(cs, "/comments")?;
        url.query_pairs_mut()
            .append_pair("version", &version.to_string());
        let (status, body) = self
            .request("POST", &url, Some(&json!({"text": text})))
            .await?;
        if status < 200 || status >= 400 {
            return Err(status_error(status, &body, url.as_str()));
        }
        Ok(())
    }

    async fn merge_changeset(
        &self,
        cs: &mut SourceChangeset,
        _squash: bool,
    ) -> Result<(), SourceError> {
        let pr = Self::require_pr(cs)?.clone();
        let version = value_i64(&pr, "version");
        let mut url = self.pull_request_url(cs, "/merge")?;
        url.query_pairs_mut()
            .append_pair("version", &version.to_string());
        let response = self.request("POST", &url, None).await?;
        let updated = self
            .decode_pr(response, &url, "")
            .await
            .map_err(|err| {
                if err.message.contains("precondition") || err.message.contains("conflict") {
                    SourceError::not_mergeable(err.message)
                } else {
                    err
                }
            })?;
        cs.set_metadata(Self::metadata(&updated));
        Ok(())
    }

    fn supports_fork(&self) -> bool {
        true
    }

    async fn get_fork(
        &self,
        target_repo: &Repo,
        namespace: Option<&String>,
        name: Option<&String>,
    ) -> Result<Repo, SourceError> {
        let slug = Self::repo_slug(target_repo)?;
        let project = Self::project_key(target_repo)?;
        let fork_namespace = namespace.cloned().unwrap_or_else(|| project.clone());
        let fork_slug = name.cloned().unwrap_or_else(|| default_fork_name(&project, &slug));
        let fork_name_and_owner = format!("{fork_namespace}/{fork_slug}");
        let metadata = json!({
            "slug": fork_slug,
            "project": {"key": fork_namespace},
        });
        copy_repo_as_fork(
            target_repo,
            metadata,
            &format!("{project}/{slug}"),
            &fork_name_and_owner,
        )
    }
}

// ---------------------------------------------------------------------------
// Azure DevOps (REST)
// ---------------------------------------------------------------------------

/// `sources.AzureDevOpsSource` backed by the [`azuredevops::Client`] REST
/// surface.
pub struct AzureDevOpsSource {
    client: azuredevops::Client,
    auto_delete_branch: bool,
}

impl AzureDevOpsSource {
    /// `sources.NewAzureDevOpsSource` after the config/authenticator
    /// resolution: `azuredevops.NewClient(urn, url, BasicAuth{username,
    /// token}, doer)`.
    pub fn new(
        base_url: &Url,
        auth: SourceAuth,
        http: Arc<dyn HttpDoer>,
    ) -> Result<Self, SourceError> {
        let parsed = BaseUrl::parse(base_url.as_str())
            .map_err(|err| SourceError::other(format!("creating Azure DevOps client: {err}")))?;
        let auth = match auth {
            SourceAuth::Basic { username, password } => AzureDevOpsAuth::Basic(
                crate::auth::BasicAuth { username, password },
            ),
            other => {
                return Err(unsupported_authenticator_error(
                    "Azure DevOps",
                    &format!("{other:?}"),
                ))
            }
        };
        Ok(Self {
            client: azuredevops::Client::new(parsed, auth, http),
            auto_delete_branch: false,
        })
    }

    pub fn with_client(client: azuredevops::Client) -> Self {
        Self {
            client,
            auto_delete_branch: false,
        }
    }

    pub fn with_auto_delete_branch(mut self, enabled: bool) -> Self {
        self.auto_delete_branch = enabled;
        self
    }

    /// `AzureDevOpsSource.ValidateAuthenticator` (`GetAuthorizedProfile`).
    pub async fn validate_authenticator(&self) -> Result<(), SourceError> {
        self.client
            .get_authorized_profile()
            .await
            .map(|_| ())
            .map_err(|err| SourceError::other(err.to_string()))
    }

    /// `Repository.GetOrganization` with the source's wrapping text.
    fn organization(repo: &azuredevops::Repository) -> Result<String, SourceError> {
        repo.get_organization().map_err(|err| SourceError::other(err).with_prefix(
            "getting Azure DevOps organization from project",
        ))
    }

    fn repo_metadata(repo: &Repo) -> Result<azuredevops::Repository, SourceError> {
        serde_json::from_value(repo.metadata.clone()).map_err(|err| {
            SourceError::other(format!("decoding Azure DevOps repository metadata: {err}"))
        })
    }

    /// `createCommonPullRequestArgs`.
    fn common_args(
        repo: &azuredevops::Repository,
        cs: &SourceChangeset,
    ) -> Result<(String, String, String, String), SourceError> {
        let org = Self::organization(repo)?;
        Ok((
            cs.changeset.external_id.clone(),
            org,
            repo.project.name.clone(),
            repo.name.clone(),
        ))
    }

    /// `annotatePullRequest`: always fetch the PR build statuses.
    async fn annotate_pull_request(
        &self,
        repo: &azuredevops::Repository,
        pr: &azuredevops::PullRequest,
    ) -> Result<Vec<azuredevops::PullRequestBuildStatus>, SourceError> {
        let org = Self::organization(repo)?;
        self.client
            .get_pull_request_statuses(
                &org,
                &repo.project.name,
                &repo.name,
                &pr.id.to_string(),
            )
            .await
            .map_err(|err| {
                SourceError::other(err.to_string())
                    .with_prefix("getting pull request statuses")
            })
    }

    /// `setChangesetMetadata`.
    async fn set_changeset_metadata(
        &self,
        repo: &azuredevops::Repository,
        pr: &azuredevops::PullRequest,
        cs: &mut SourceChangeset,
    ) -> Result<(), SourceError> {
        let statuses = self
            .annotate_pull_request(repo, pr)
            .await
            .map_err(|err| err.with_prefix("annotating pull request"))?;

        cs.changeset.external_id = pr.id.to_string();
        cs.changeset.external_branch = ensure_ref_prefix(&pr.source_ref_name);
        if let Some(fork) = &pr.fork_source {
            cs.changeset.external_fork_namespace = fork.repository.namespace();
            cs.changeset.external_fork_name = fork.repository.name.clone();
        } else {
            cs.changeset.external_fork_namespace.clear();
            cs.changeset.external_fork_name.clear();
        }
        cs.changeset.metadata = azure_devops_metadata(pr, &statuses);
        Ok(())
    }

    /// `changesetToPullRequestInput`.
    fn changeset_to_pull_request_input(
        &self,
        cs: &SourceChangeset,
    ) -> Result<azuredevops::CreatePullRequestInput, SourceError> {
        let completion_options = azuredevops::PullRequestCompletionOptions {
            delete_source_branch: self.auto_delete_branch,
            ..Default::default()
        };
        // The baseline compares the `*types.Repo` pointers: equal when no fork
        // is in use, different when a fork repo was resolved.
        let fork_source = if cs.remote_repo != cs.target_repo {
            Some(azuredevops::ForkRef {
                repository: Self::repo_metadata(&cs.remote_repo)?,
                name: String::new(),
                url: String::new(),
            })
        } else {
            None
        };
        Ok(azuredevops::CreatePullRequestInput {
            source_ref_name: cs.head_ref.clone(),
            target_ref_name: cs.base_ref.clone(),
            title: cs.title.clone(),
            description: cs.body.clone(),
            reviewers: None,
            fork_source,
            is_draft: false,
            completion_options: Some(completion_options),
        })
    }

    /// `changesetToUpdatePullRequestInput`.
    fn changeset_to_update_pull_request_input(
        &self,
        cs: &SourceChangeset,
        target_ref_changed: bool,
    ) -> azuredevops::PullRequestUpdateInput {
        if target_ref_changed {
            return azuredevops::PullRequestUpdateInput {
                target_ref_name: Some(ensure_ref_prefix(&cs.base_ref)),
                ..Default::default()
            };
        }
        azuredevops::PullRequestUpdateInput {
            title: Some(cs.title.clone()),
            description: Some(cs.body.clone()),
            completion_options: Some(azuredevops::PullRequestCompletionOptions {
                delete_source_branch: self.auto_delete_branch,
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    /// `createChangeset` (also used by `CreateDraftChangeset`).
    async fn create_changeset_with(
        &self,
        cs: &mut SourceChangeset,
        input: azuredevops::CreatePullRequestInput,
    ) -> Result<bool, SourceError> {
        let repo = Self::repo_metadata(&cs.target_repo)?;
        let org = Self::organization(&repo)?;
        let pr = self
            .client
            .create_pull_request(&org, &repo.project.name, &repo.name, &input)
            .await
            .map_err(|err| {
                SourceError::other(err.to_string()).with_prefix("creating pull request")
            })?;
        self.set_changeset_metadata(&repo, &pr, cs)
            .await
            .map_err(|err| err.with_prefix("setting Azure DevOps changeset metadata"))?;
        Ok(true)
    }

    /// `checkAndCopy`.
    fn check_and_copy(target_repo: &Repo, fork: azuredevops::Repository) -> Result<Repo, SourceError> {
        if !fork.is_fork {
            return Err(SourceError::other("repo is not a fork"));
        }
        let fork_namespace = fork.namespace();
        let fork_name = fork.name.clone();
        copy_azure_devops_repo_as_fork(target_repo, fork, &fork_namespace, &fork_name)
            .map_err(|err| err.with_prefix("updating target repo sources"))
    }
}

/// `azureDevOpsMetadata` normalised projection used by this port's
/// [`ChangesetMetadata`]-style `changeset.metadata` slot.
fn azure_devops_metadata(
    pr: &azuredevops::PullRequest,
    statuses: &[azuredevops::PullRequestBuildStatus],
) -> Value {
    let statuses: Vec<Value> = statuses
        .iter()
        .map(|status| {
            json!({
                "id": status.id,
                "state": status.state,
                "description": status.description,
            })
        })
        .collect();
    let fork_source = match &pr.fork_source {
        Some(fork) => json!({
            "repository": {
                "id": fork.repository.id,
                "name": fork.repository.name,
                "project": {
                    "id": fork.repository.project.id,
                    "name": fork.repository.project.name,
                },
            },
            "name": fork.name,
            "url": fork.url,
        }),
        None => Value::Null,
    };
    json!({
        "kind": "azuredevops",
        "id": pr.id,
        "codeReviewId": pr.code_review_id,
        "status": pr.status,
        "title": pr.title,
        "description": pr.description,
        "sourceRefName": pr.source_ref_name,
        "targetRefName": pr.target_ref_name,
        "mergeStatus": pr.merge_status,
        "isDraft": pr.is_draft,
        "url": pr.url,
        "lastMergeSourceCommit": {
            "commitId": pr.last_merge_source_commit.commit_id,
            "url": pr.last_merge_source_commit.url,
        },
        "lastMergeTargetCommit": {
            "commitId": pr.last_merge_target_commit.commit_id,
            "url": pr.last_merge_target_commit.url,
        },
        "createdBy": {
            "id": pr.created_by.id,
            "displayName": pr.created_by.display_name,
            "uniqueName": pr.created_by.unique_name,
        },
        "forkSource": fork_source,
        "statuses": statuses,
    })
}

/// `copyAzureDevOpsRepoAsFork`: rewrite the clone URL path
/// `/org/project/_git/repo` (`project` is the namespace) and store the fork
/// repository as metadata. Unlike the generic `CopyRepoAsFork`, this also
/// keeps `Name` unchanged, like the baseline.
pub fn copy_azure_devops_repo_as_fork(
    repo: &Repo,
    fork: azuredevops::Repository,
    fork_namespace: &str,
    fork_name: &str,
) -> Result<Repo, SourceError> {
    if repo.sources.is_empty() {
        return Err(SourceError::other("repo has no sources"));
    }
    let mut fork_repo = repo.clone();
    let mut sources = BTreeMap::new();
    for (urn, source) in &repo.sources {
        if source.clone_url.is_empty() {
            continue;
        }
        let mut url = Url::parse(&source.clone_url)
            .map_err(|err| SourceError::other(err.to_string()))?;
        // Will look like: /org/project/_git/repo, project is our namespace.
        let parts: Vec<&str> = url.path().splitn(5, '/').collect();
        if parts.len() < 5 {
            return Err(SourceError::other(format!(
                "repo has malformed clone url: {}",
                source.clone_url
            )));
        }
        let mut rewritten = parts.clone();
        rewritten[2] = fork_namespace;
        rewritten[4] = fork_name;
        url.set_path(&rewritten.join("/"));
        sources.insert(
            urn.clone(),
            RepoSource {
                id: source.id.clone(),
                clone_url: url.to_string(),
            },
        );
    }
    fork_repo.sources = sources;
    fork_repo.metadata = serde_json::to_value(&fork).unwrap_or(Value::Null);
    Ok(fork_repo)
}

#[async_trait]
impl ChangesetSource for AzureDevOpsSource {
    fn kind(&self) -> &'static str {
        "azuredevops"
    }

    async fn load_changeset(&self, cs: &mut SourceChangeset) -> Result<(), SourceError> {
        let repo = Self::repo_metadata(&cs.target_repo)?;
        let (id, org, project, repo_name) = Self::common_args(&repo, cs)?;
        let pr = match self
            .client
            .get_pull_request(&org, &project, &repo_name, &id)
            .await
        {
            Ok(pr) => pr,
            Err(err) if err.is_not_found() => {
                return Err(SourceError::not_found(&cs.changeset.external_id))
            }
            Err(err) => {
                return Err(
                    SourceError::other(err.to_string()).with_prefix("getting pull request")
                )
            }
        };
        self.set_changeset_metadata(&repo, &pr, cs)
            .await
            .map_err(|err| err.with_prefix("setting Azure DevOps changeset metadata"))
    }

    async fn create_changeset(&self, cs: &mut SourceChangeset) -> Result<bool, SourceError> {
        let input = self.changeset_to_pull_request_input(cs)?;
        self.create_changeset_with(cs, input).await
    }

    async fn create_draft_changeset(
        &self,
        cs: &mut SourceChangeset,
    ) -> Result<bool, SourceError> {
        let mut input = self.changeset_to_pull_request_input(cs)?;
        input.is_draft = true;
        self.create_changeset_with(cs, input).await
    }

    async fn update_changeset(&self, cs: &mut SourceChangeset) -> Result<(), SourceError> {
        let repo = Self::repo_metadata(&cs.target_repo)?;
        let (id, org, project, repo_name) = Self::common_args(&repo, cs)?;
        let pr = match self
            .client
            .get_pull_request(&org, &project, &repo_name, &id)
            .await
        {
            Ok(pr) => pr,
            Err(err) if err.is_not_found() => {
                return Err(SourceError::not_found(&cs.changeset.external_id))
            }
            Err(err) => {
                return Err(
                    SourceError::other(err.to_string()).with_prefix("getting pull request")
                )
            }
        };

        // ADO does not support updating the target branch alongside other
        // fields, so check it separately and make a second call.
        if pr.target_ref_name != cs.base_ref {
            let input = self.changeset_to_update_pull_request_input(cs, true);
            self.client
                .update_pull_request(&org, &project, &repo_name, &id, &input)
                .await
                .map_err(|err| {
                    SourceError::other(err.to_string()).with_prefix("updating pull request")
                })?;
        }

        let input = self.changeset_to_update_pull_request_input(cs, false);
        let updated = self
            .client
            .update_pull_request(&org, &project, &repo_name, &id, &input)
            .await
            .map_err(|err| {
                SourceError::other(err.to_string()).with_prefix("updating pull request")
            })?;
        self.set_changeset_metadata(&repo, &updated, cs)
            .await
            .map_err(|err| err.with_prefix("setting Azure DevOps changeset metadata"))
    }

    async fn close_changeset(&self, cs: &mut SourceChangeset) -> Result<(), SourceError> {
        let repo = Self::repo_metadata(&cs.target_repo)?;
        let (id, org, project, repo_name) = Self::common_args(&repo, cs)?;
        let updated = self
            .client
            .abandon_pull_request(&org, &project, &repo_name, &id)
            .await
            .map_err(|err| {
                SourceError::other(err.to_string()).with_prefix("abandoning pull request")
            })?;
        self.set_changeset_metadata(&repo, &updated, cs)
            .await
            .map_err(|err| err.with_prefix("setting Azure DevOps changeset metadata"))
    }

    async fn reopen_changeset(&self, cs: &mut SourceChangeset) -> Result<(), SourceError> {
        let repo = Self::repo_metadata(&cs.target_repo)?;
        let (id, org, project, repo_name) = Self::common_args(&repo, cs)?;
        let input = azuredevops::PullRequestUpdateInput {
            status: Some(azuredevops::PULL_REQUEST_STATUS_ACTIVE.to_owned()),
            completion_options: Some(azuredevops::PullRequestCompletionOptions {
                delete_source_branch: self.auto_delete_branch,
                ..Default::default()
            }),
            ..Default::default()
        };
        let updated = self
            .client
            .update_pull_request(&org, &project, &repo_name, &id, &input)
            .await
            .map_err(|err| {
                SourceError::other(err.to_string()).with_prefix("updating pull request")
            })?;
        self.set_changeset_metadata(&repo, &updated, cs)
            .await
            .map_err(|err| err.with_prefix("setting Azure DevOps changeset metadata"))
    }

    async fn undraft_changeset(&self, cs: &mut SourceChangeset) -> Result<(), SourceError> {
        let repo = Self::repo_metadata(&cs.target_repo)?;
        let (id, org, project, repo_name) = Self::common_args(&repo, cs)?;
        let mut input = self.changeset_to_update_pull_request_input(cs, false);
        input.is_draft = Some(false);
        let updated = self
            .client
            .update_pull_request(&org, &project, &repo_name, &id, &input)
            .await
            .map_err(|err| {
                SourceError::other(err.to_string()).with_prefix("updating pull request")
            })?;
        self.set_changeset_metadata(&repo, &updated, cs)
            .await
            .map_err(|err| err.with_prefix("setting Azure DevOps changeset metadata"))
    }

    async fn create_comment(
        &self,
        cs: &SourceChangeset,
        text: &str,
    ) -> Result<(), SourceError> {
        let repo = Self::repo_metadata(&cs.target_repo)?;
        let (id, org, project, repo_name) = Self::common_args(&repo, cs)?;
        let input = azuredevops::PullRequestCommentInput {
            comments: vec![azuredevops::PullRequestCommentForInput {
                parent_comment_id: 0,
                content: text.to_owned(),
                comment_type: 1,
            }],
        };
        self.client
            .create_pull_request_comment_thread(&org, &project, &repo_name, &id, &input)
            .await
            .map(|_| ())
            .map_err(|err| SourceError::other(err.to_string()))
    }

    async fn merge_changeset(
        &self,
        cs: &mut SourceChangeset,
        squash: bool,
    ) -> Result<(), SourceError> {
        let repo = Self::repo_metadata(&cs.target_repo)?;
        let (id, org, project, repo_name) = Self::common_args(&repo, cs)?;
        let merge_strategy = if squash {
            Some(azuredevops::PULL_REQUEST_MERGE_STRATEGY_SQUASH.to_owned())
        } else {
            None
        };
        // `btypes.Changeset.SyncState.HeadRefOid`: the reduced changeset in
        // this port carries the code-host metadata, whose
        // `lastMergeSourceCommit.commitId` is what the syncer stores there.
        let commit_id = nested(&cs.changeset.metadata, &["lastMergeSourceCommit", "commitId"])
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_owned();
        let input = azuredevops::PullRequestCompleteInput {
            commit_id,
            merge_strategy,
            delete_source_branch: self.auto_delete_branch,
        };
        let updated = match self
            .client
            .complete_pull_request(&org, &project, &repo_name, &id, &input)
            .await
        {
            Ok(updated) => updated,
            Err(err) if err.is_not_found() => {
                return Err(
                    SourceError::other(err.to_string()).with_prefix("merging pull request")
                )
            }
            Err(err) => return Err(SourceError::not_mergeable(err.to_string())),
        };
        self.set_changeset_metadata(&repo, &updated, cs)
            .await
            .map_err(|err| err.with_prefix("setting Azure DevOps changeset metadata"))
    }

    fn supports_draft(&self) -> bool {
        true
    }

    fn supports_fork(&self) -> bool {
        true
    }

    async fn get_fork(
        &self,
        target_repo: &Repo,
        namespace: Option<&String>,
        name: Option<&String>,
    ) -> Result<Repo, SourceError> {
        let repo = Self::repo_metadata(target_repo)?;
        let namespace = namespace
            .cloned()
            .unwrap_or_else(|| repo.namespace());
        let target_namespace = repo.namespace();
        let fork_name = name
            .cloned()
            .unwrap_or_else(|| default_fork_name(&target_namespace, &repo.name));
        let org = Self::organization(&repo)?;

        // Figure out if we already have a fork of the repo in the namespace.
        match self
            .client
            .get_repo(&org, &namespace, &fork_name)
            .await
        {
            Ok(fork) => return Self::check_and_copy(target_repo, fork),
            Err(err) if err.is_not_found() => {}
            Err(err) => {
                return Err(SourceError::other(err.to_string())
                    .with_prefix("checking for fork existence"))
            }
        }

        let mut project = repo.project.clone();
        // If the fork is in a different namespace (project), fetch it so we
        // have its ID.
        if namespace != repo.namespace() {
            project = self
                .client
                .get_project(&org, &namespace)
                .await
                .map_err(|err| SourceError::other(err.to_string()))?;
        }

        let fork = self
            .client
            .fork_repository(
                &org,
                &azuredevops::ForkRepositoryInput {
                    name: fork_name,
                    project: azuredevops::ForkRepositoryInputProject {
                        id: project.id.clone(),
                    },
                    parent_repository: azuredevops::ForkRepositoryInputParentRepository {
                        id: repo.id.clone(),
                        project: azuredevops::ForkRepositoryInputProject {
                            id: repo.project.id.clone(),
                        },
                    },
                },
            )
            .await
            .map_err(|err| {
                SourceError::other(err.to_string()).with_prefix("forking repository")
            })?;

        Self::check_and_copy(target_repo, fork)
    }
}

// ---------------------------------------------------------------------------
// Gerrit (REST)
// ---------------------------------------------------------------------------

/// `sources.GerritSource`.
pub struct GerritSource {
    client: gerrit::Client,
    /// `conf.Get().BatchChangesAutoDeleteBranch`.
    auto_delete_branch: bool,
}

impl GerritSource {
    /// `sources.NewGerritSource` after config decoding: `gerrit.NewClient`
    /// with `BasicAuth{username, password}`.
    pub fn new(
        base_url: &Url,
        auth: SourceAuth,
        http: Arc<dyn HttpDoer>,
    ) -> Result<Self, SourceError> {
        let (username, password) = match auth {
            SourceAuth::Basic { username, password } => (username, password),
            other => {
                return Err(unsupported_authenticator_error(
                    "Gerrit",
                    &format!("{other:?}"),
                ))
            }
        };
        Ok(Self {
            client: gerrit::Client::new(
                base_url.clone(),
                &gerrit::AccountCredentials { username, password },
                http,
            ),
            auto_delete_branch: false,
        })
    }

    pub fn with_client(client: gerrit::Client) -> Self {
        Self {
            client,
            auto_delete_branch: false,
        }
    }

    pub fn with_auto_delete_branch(mut self, enabled: bool) -> Self {
        self.auto_delete_branch = enabled;
        self
    }

    /// `GerritSource.ValidateAuthenticator` (`GetAuthenticatedUserAccount`).
    pub async fn validate_authenticator(&self) -> Result<(), SourceError> {
        self.client
            .get_authenticated_user_account()
            .await
            .map(|_| ())
            .map_err(|err| SourceError::other(err.to_string()))
    }

    /// `annotateChange`: fetch the reviewers and build the metadata blob.
    async fn annotate_change(
        &self,
        change: &gerrit::Change,
    ) -> Result<Value, SourceError> {
        let reviewers = self
            .client
            .get_change_reviews(&change.change_id)
            .await
            .map_err(|err| SourceError::other(err.to_string()))?;
        Ok(gerrit_metadata(change, &reviewers, self.client.url()))
    }

    /// `setChangesetMetadata`.
    async fn set_changeset_metadata(
        &self,
        change: &gerrit::Change,
        cs: &mut SourceChangeset,
    ) -> Result<(), SourceError> {
        let metadata = self
            .annotate_change(change)
            .await
            .map_err(|err| err.with_prefix("annotating Change"))?;
        cs.changeset.external_id = change.change_id.clone();
        cs.changeset.external_branch = ensure_ref_prefix(&change.branch);
        cs.changeset.metadata = metadata;
        Ok(())
    }
}

/// The canonical Gerrit metadata projection (`gerritbatches.AnnotatedChange`).
///
/// `Change.Created`/`Updated` carry `json:"-"` in the Go type, so they are
/// absent here; `CodeHostURL` is a `url.URL`, which marshals as its exported
/// struct fields rather than as a string.
fn gerrit_metadata(
    change: &gerrit::Change,
    reviewers: &[gerrit::Reviewer],
    code_host_url: &Url,
) -> Value {
    json!({
        "change": {
            "id": change.id,
            "project": change.project,
            "branch": change.branch,
            "change_id": change.change_id,
            "topic": change.topic,
            "subject": change.subject,
            "status": change.status,
            "created": gerrit::format_change_time(&change.created),
            "updated": gerrit::format_change_time(&change.updated),
            "reviewed": change.reviewed,
            "work_in_progress": change.work_in_progress,
            "hashtags": change.hashtags,
            "_number": change.change_number,
            "owner": {
                "name": change.owner.name,
                "email": change.owner.email,
                "username": change.owner.username,
            },
        },
        "reviewers": reviewers.iter().map(|reviewer| {
            let mut value = json!({
                "approvals": reviewer.approvals,
                "_account_id": reviewer.account_id,
                "name": reviewer.name,
                "email": reviewer.email,
            });
            if !reviewer.username.is_empty() {
                value["username"] = json!(reviewer.username);
            }
            value
        }).collect::<Vec<_>>(),
        "codeHostURL": go_url_json(code_host_url),
    })
}

/// `json.Marshal` of a Go `url.URL`. `Userinfo` has no exported fields, so a
/// non-nil user marshals as `{}`.
pub fn go_url_json(url: &Url) -> Value {
    let user = if url.username().is_empty() && url.password().is_none() {
        Value::Null
    } else {
        json!({})
    };
    json!({
        "Scheme": url.scheme(),
        "Opaque": "",
        "User": user,
        "Host": url.host_str().map(|host| match url.port() {
            Some(port) => format!("{host}:{port}"),
            None => host.to_owned(),
        }).unwrap_or_default(),
        "Path": url.path(),
        "RawPath": "",
        "OmitHost": false,
        "ForceQuery": false,
        "RawQuery": url.query().unwrap_or_default(),
        "Fragment": url.fragment().unwrap_or_default(),
        "RawFragment": "",
    })
}

/// `sources.GenerateGerritChangeID`: sha256 over the JSON encoding of the Go
/// `btypes.Changeset` (see the overview for the exact field set), truncated to
/// 40 hex characters and prefixed with `I`.
pub fn generate_gerrit_change_id(cs: &SourceChangeset) -> String {
    use sha2::{Digest, Sha256};
    let quote = |value: &str| serde_json::to_string(value).expect("string serialises");
    let metadata =
        serde_json::to_string(&cs.changeset.metadata).expect("metadata serialises");
    let json = format!(
        concat!(
            "{{\"ID\":{},\"RepoID\":0,",
            "\"CreatedAt\":\"0001-01-01T00:00:00Z\",",
            "\"UpdatedAt\":\"0001-01-01T00:00:00Z\",",
            "\"Metadata\":{},\"BatchChanges\":null,",
            "\"ExternalID\":{},\"ExternalServiceType\":\"\",",
            "\"ExternalBranch\":{},\"ExternalForkName\":{},",
            "\"ExternalForkNamespace\":{},\"ExternalDeletedAt\":",
            "\"0001-01-01T00:00:00Z\",\"ExternalUpdatedAt\":",
            "\"0001-01-01T00:00:00Z\",\"ExternalState\":\"\",",
            "\"ExternalReviewState\":\"\",\"ExternalCheckState\":\"\",",
            "\"CommitVerification\":null,\"DiffStatAdded\":null,",
            "\"DiffStatDeleted\":null,",
            "\"SyncState\":{{\"BaseRefOid\":\"\",\"HeadRefOid\":\"\",",
            "\"IsComplete\":false}},",
            "\"OwnedByBatchChangeID\":0,\"CurrentSpecID\":0,",
            "\"PreviousSpecID\":0,\"PublicationState\":\"\",",
            "\"UiPublicationState\":null,\"State\":\"\",",
            "\"ReconcilerState\":\"\",\"FailureMessage\":null,",
            "\"StartedAt\":\"0001-01-01T00:00:00Z\",",
            "\"FinishedAt\":\"0001-01-01T00:00:00Z\",",
            "\"ProcessAfter\":\"0001-01-01T00:00:00Z\",",
            "\"NumResets\":0,\"NumFailures\":0,",
            "\"SyncErrorMessage\":null,\"PreviousFailureMessage\":null,",
            "\"Closing\":false,\"DetachedAt\":\"0001-01-01T00:00:00Z\"}}"
        ),
        cs.changeset.id,
        metadata,
        quote(&cs.changeset.external_id),
        quote(&cs.changeset.external_branch),
        quote(&cs.changeset.external_fork_name),
        quote(&cs.changeset.external_fork_namespace),
    );
    let hash = Sha256::digest(json.as_bytes());
    let hex: String = hash.iter().map(|byte| format!("{byte:02x}")).collect();
    format!("I{}", &hex[..40])
}

#[async_trait]
impl ChangesetSource for GerritSource {
    fn kind(&self) -> &'static str {
        "gerrit"
    }

    async fn load_changeset(&self, cs: &mut SourceChangeset) -> Result<(), SourceError> {
        let change = match self.client.get_change(&cs.changeset.external_id).await {
            Ok(change) => change,
            Err(gerrit::ClientError::Http(err)) if err.not_found() => {
                return Err(SourceError::not_found(&cs.changeset.external_id))
            }
            Err(err) => {
                return Err(SourceError::other(err.to_string()).with_prefix("getting change"))
            }
        };
        self.set_changeset_metadata(&change, cs)
            .await
            .map_err(|err| err.with_prefix("setting Gerrit changeset metadata"))
    }

    async fn create_changeset(
        &self,
        cs: &mut SourceChangeset,
    ) -> Result<bool, SourceError> {
        let change_id = generate_gerrit_change_id(cs);
        let change = match self.client.get_change(&change_id).await {
            Ok(change) => change,
            Err(gerrit::ClientError::Http(err)) if err.not_found() => {
                return Err(SourceError::not_found(&cs.changeset.external_id))
            }
            Err(err) => {
                return Err(SourceError::other(err.to_string()).with_prefix("getting change"))
            }
        };
        // The change exists at push time, but the baseline reports `false` so
        // the publish event is emitted instead of an update event.
        self.set_changeset_metadata(&change, cs)
            .await
            .map_err(|err| err.with_prefix("setting Gerrit changeset metadata"))?;
        Ok(false)
    }

    async fn create_draft_changeset(
        &self,
        cs: &mut SourceChangeset,
    ) -> Result<bool, SourceError> {
        let change_id = generate_gerrit_change_id(cs);
        if let Err(err) = self.client.set_wip(&change_id).await {
            return match err {
                gerrit::ClientError::Http(err) if err.not_found() => {
                    Err(SourceError::not_found(&cs.changeset.external_id))
                }
                err => Err(
                    SourceError::other(err.to_string()).with_prefix("making change WIP")
                ),
            };
        }
        let change = match self.client.get_change(&change_id).await {
            Ok(change) => change,
            Err(gerrit::ClientError::Http(err)) if err.not_found() => {
                return Err(SourceError::not_found(&cs.changeset.external_id))
            }
            Err(err) => {
                return Err(SourceError::other(err.to_string()).with_prefix("getting change"))
            }
        };
        self.set_changeset_metadata(&change, cs)
            .await
            .map_err(|err| err.with_prefix("setting Gerrit changeset metadata"))?;
        Ok(false)
    }

    async fn undraft_changeset(&self, cs: &mut SourceChangeset) -> Result<(), SourceError> {
        if let Err(err) = self
            .client
            .set_ready_for_review(&cs.changeset.external_id)
            .await
        {
            return match err {
                gerrit::ClientError::Http(err) if err.not_found() => {
                    Err(SourceError::not_found(&cs.changeset.external_id))
                }
                err => Err(SourceError::other(err.to_string())
                    .with_prefix("setting change as ready")),
            };
        }
        self.load_changeset(cs)
            .await
            .map_err(|err| err.with_prefix("getting change"))
    }

    async fn close_changeset(&self, cs: &mut SourceChangeset) -> Result<(), SourceError> {
        let updated = self
            .client
            .abandon_change(&cs.changeset.external_id)
            .await
            .map_err(|err| {
                SourceError::other(err.to_string()).with_prefix("abandoning change")
            })?;
        if self.auto_delete_branch {
            self.client
                .delete_change(&cs.changeset.external_id)
                .await
                .map_err(|err| {
                    SourceError::other(err.to_string()).with_prefix("deleting change")
                })?;
        }
        self.set_changeset_metadata(&updated, cs)
            .await
            .map_err(|err| err.with_prefix("setting Gerrit changeset metadata"))
    }

    async fn update_changeset(&self, cs: &mut SourceChangeset) -> Result<(), SourceError> {
        let change = match self.client.get_change(&cs.changeset.external_id).await {
            Ok(change) => change,
            Err(gerrit::ClientError::MultipleChanges { .. }) => {
                // A push created two changes with the same Change-Id. Delete
                // the original one, keep WIP state, then reload.
                let original = &cs.changeset.metadata["change"];
                let original_id = original
                    .get("id")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_owned();
                let work_in_progress = value_bool(original, "work_in_progress");
                self.client.delete_change(&original_id).await.map_err(|err| {
                    SourceError::other(err.to_string()).with_prefix("deleting change")
                })?;
                if work_in_progress {
                    self.client
                        .set_wip(&cs.changeset.external_id)
                        .await
                        .map_err(|err| {
                            SourceError::other(err.to_string())
                                .with_prefix("setting updated change as WIP")
                        })?;
                }
                return self.load_changeset(cs).await;
            }
            Err(gerrit::ClientError::Http(err)) if err.not_found() => {
                return Err(SourceError::not_found(&cs.changeset.external_id))
            }
            Err(err) => {
                return Err(
                    SourceError::other(err.to_string()).with_prefix("getting newer change")
                )
            }
        };

        if change.branch != cs.base_ref {
            self.client
                .move_change(
                    &cs.changeset.external_id,
                    &gerrit::MoveChangePayload {
                        destination_branch: cs.base_ref.clone(),
                    },
                )
                .await
                .map_err(|err| {
                    SourceError::other(err.to_string()).with_prefix("moving change")
                })?;
        }
        if change.subject != cs.title {
            self.client
                .set_commit_message(
                    &cs.changeset.external_id,
                    &gerrit::SetCommitMessagePayload {
                        message: format!(
                            "{}\n\nChange-Id: {}\n",
                            cs.title, cs.changeset.external_id
                        ),
                    },
                )
                .await
                .map_err(|err| {
                    SourceError::other(err.to_string())
                        .with_prefix("setting change commit message")
                })?;
        }
        self.load_changeset(cs).await
    }

    async fn reopen_changeset(&self, cs: &mut SourceChangeset) -> Result<(), SourceError> {
        let updated = self
            .client
            .restore_change(&cs.changeset.external_id)
            .await
            .map_err(|err| {
                SourceError::other(err.to_string()).with_prefix("restoring change")
            })?;
        self.set_changeset_metadata(&updated, cs)
            .await
            .map_err(|err| err.with_prefix("setting Gerrit changeset metadata"))
    }

    async fn create_comment(
        &self,
        cs: &SourceChangeset,
        text: &str,
    ) -> Result<(), SourceError> {
        self.client
            .write_review_comment(
                &cs.changeset.external_id,
                &gerrit::ChangeReviewComment {
                    message: text.to_owned(),
                    ..Default::default()
                },
            )
            .await
            .map_err(|err| SourceError::other(err.to_string()))
    }

    async fn merge_changeset(
        &self,
        cs: &mut SourceChangeset,
        _squash: bool,
    ) -> Result<(), SourceError> {
        let updated = match self.client.submit_change(&cs.changeset.external_id).await {
            Ok(updated) => updated,
            Err(gerrit::ClientError::Http(err)) if err.not_found() => {
                return Err(
                    SourceError::other(err.to_string()).with_prefix("submitting change")
                )
            }
            Err(err) => return Err(SourceError::not_mergeable(err.to_string())),
        };
        self.set_changeset_metadata(&updated, cs)
            .await
            .map_err(|err| err.with_prefix("setting Gerrit changeset metadata"))
    }

    fn supports_draft(&self) -> bool {
        true
    }
}

// ---------------------------------------------------------------------------
// Bitbucket Cloud (REST)
// ---------------------------------------------------------------------------

/// `sources.BitbucketCloudSource`.
pub struct BitbucketCloudSource {
    client: bbc::Client,
    auto_delete_branch: bool,
}

impl BitbucketCloudSource {
    /// `sources.NewBitbucketCloudSource` after config decoding.
    pub fn new(
        base_url: &Url,
        auth: SourceAuth,
        http: Arc<dyn HttpDoer>,
    ) -> Result<Self, SourceError> {
        let auth = match auth {
            SourceAuth::Basic { username, password } => {
                bbc::BitbucketCloudAuth::Basic { username, password }
            }
            SourceAuth::BearerToken(token) => {
                bbc::BitbucketCloudAuth::OAuthBearerToken(token)
            }
            other => {
                return Err(unsupported_authenticator_error(
                    "BitbucketCloud",
                    &format!("{other:?}"),
                ))
            }
        };
        Ok(Self {
            client: bbc::Client::with_base_url_and_auth(base_url.clone(), auth, http),
            auto_delete_branch: false,
        })
    }

    pub fn with_client(client: bbc::Client) -> Self {
        Self {
            client,
            auto_delete_branch: false,
        }
    }

    pub fn with_auto_delete_branch(mut self, enabled: bool) -> Self {
        self.auto_delete_branch = enabled;
        self
    }

    /// `BitbucketCloudSource.ValidateAuthenticator` (`Ping`).
    pub async fn validate_authenticator(&self) -> Result<(), SourceError> {
        self.client
            .ping()
            .await
            .map_err(|err| SourceError::other(err.to_string()))
    }

    fn repo_metadata(repo: &Repo) -> Result<bbc::Repo, SourceError> {
        serde_json::from_value(repo.metadata.clone()).map_err(|err| {
            SourceError::other(format!("decoding Bitbucket Cloud repository metadata: {err}"))
        })
    }

    /// `changesetToPullRequestInput`.
    fn changeset_to_pull_request_input(&self, cs: &SourceChangeset) -> bbc::PullRequestInput {
        let destination_branch = abbreviate_ref(&cs.base_ref);
        let mut opts = bbc::PullRequestInput {
            title: cs.title.clone(),
            description: cs.body.clone(),
            source_branch: abbreviate_ref(&cs.head_ref),
            reviewers: Vec::new(),
            source_repo: None,
            destination_branch: Some(destination_branch),
            close_source_branch: self.auto_delete_branch,
        };
        if cs.remote_repo != cs.target_repo {
            if let Ok(remote) = Self::repo_metadata(&cs.remote_repo) {
                opts.source_repo = Some(remote);
            }
        }
        opts
    }

    /// The stored metadata's pull-request id (the baseline reads
    /// `cs.Metadata.(*bbcs.AnnotatedPullRequest).ID`).
    fn metadata_pr_id(cs: &SourceChangeset) -> Result<i64, SourceError> {
        if let Some(id) = cs.changeset.metadata.get("id").and_then(Value::as_i64) {
            return Ok(id);
        }
        cs.changeset
            .external_id
            .parse::<i64>()
            .map_err(|err| SourceError::other(format!("converting external ID: {err}")))
    }

    /// `annotatePullRequest`.
    async fn annotate_pull_request(
        &self,
        repo: &bbc::Repo,
        pr: &bbc::PullRequest,
    ) -> Result<Value, SourceError> {
        let statuses = match self
            .client
            .get_pull_request_statuses(&repo.full_name, pr.id)
            .await
        {
            Ok(statuses) => statuses,
            Err(err) => {
                return Err(SourceError::other(err.to_string())
                    .with_prefix("getting pull request statuses as slice"))
            }
        };
        Ok(bitbucket_cloud_metadata(pr, &statuses))
    }

    /// `setChangesetMetadata`.
    async fn set_changeset_metadata(
        &self,
        repo: &bbc::Repo,
        pr: &bbc::PullRequest,
        cs: &mut SourceChangeset,
    ) -> Result<(), SourceError> {
        let metadata = self
            .annotate_pull_request(repo, pr)
            .await
            .map_err(|err| err.with_prefix("annotating pull request"))?;

        cs.changeset.external_id = pr.id.to_string();
        cs.changeset.external_branch = ensure_ref_prefix(&pr.source.branch.name);
        cs.changeset.metadata = metadata;

        if pr.source.repo.uuid != pr.destination.repo.uuid {
            let namespace = pr
                .source
                .repo
                .namespace()
                .map_err(|err| SourceError::other(err).with_prefix("determining fork namespace"))?;
            cs.changeset.external_fork_namespace = namespace;
            cs.changeset.external_fork_name = pr.source.repo.name.clone();
        } else {
            cs.changeset.external_fork_namespace.clear();
            cs.changeset.external_fork_name.clear();
        }
        Ok(())
    }

    /// `checkAndCopy`.
    fn check_and_copy(target_repo: &Repo, fork: bbc::Repo) -> Result<Repo, SourceError> {
        let target = Self::repo_metadata(target_repo)?;
        let Some(parent) = &fork.parent else {
            return Err(SourceError::other("repo is not a fork"));
        };
        if parent.uuid != target.uuid {
            return Err(SourceError::other(
                "repo was not forked from the given parent",
            ));
        }
        copy_repo_as_fork(target_repo, serde_json::to_value(&fork).unwrap_or(Value::Null), &target.full_name, &fork.full_name)
            .map_err(|err| err.with_prefix("updating target repo sources and metadata"))
    }
}

/// The canonical Bitbucket Cloud metadata projection
/// (`bbcs.AnnotatedPullRequest`).
fn bitbucket_cloud_metadata(
    pr: &bbc::PullRequest,
    statuses: &[bbc::PullRequestStatus],
) -> Value {
    json!({
        "kind": "bitbucketcloud",
        "id": pr.id,
        "title": pr.title,
        "state": pr.state,
        "source": {
            "branch": pr.source.branch.name,
            "repo": {
                "fullName": pr.source.repo.full_name,
                "uuid": pr.source.repo.uuid,
                "name": pr.source.repo.name,
            },
        },
        "destination": {
            "branch": pr.destination.branch.name,
            "repo": {
                "fullName": pr.destination.repo.full_name,
                "uuid": pr.destination.repo.uuid,
                "name": pr.destination.repo.name,
            },
        },
        "closeSourceBranch": pr.close_source_branch,
        "reviewers": pr.reviewers.as_deref().unwrap_or_default().iter().map(|reviewer| {
            json!({
                "displayName": reviewer.display_name,
                "uuid": reviewer.uuid,
                "nickname": reviewer.nickname,
            })
        }).collect::<Vec<_>>(),
        "statuses": statuses.iter().map(|status| {
            json!({
                "uuid": status.uuid,
                "key": status.status_key,
                "state": status.state,
                "name": status.name,
                "description": status.description,
            })
        }).collect::<Vec<_>>(),
    })
}

#[async_trait]
impl ChangesetSource for BitbucketCloudSource {
    fn kind(&self) -> &'static str {
        "bitbucketCloud"
    }

    async fn load_changeset(&self, cs: &mut SourceChangeset) -> Result<(), SourceError> {
        let repo = Self::repo_metadata(&cs.target_repo)?;
        let number = cs.changeset.external_id.parse::<i64>().map_err(|err| {
            SourceError::other(format!(
                "converting external ID {:?}: {err}",
                cs.changeset.external_id
            ))
        })?;
        let pr = match self.client.get_pull_request(&repo.full_name, number).await {
            Ok(pr) => pr,
            Err(err) if err.is_not_found() => {
                return Err(SourceError::not_found(&cs.changeset.external_id))
            }
            Err(err) => {
                return Err(
                    SourceError::other(err.to_string()).with_prefix("getting pull request")
                )
            }
        };
        self.set_changeset_metadata(&repo, &pr, cs).await
    }

    async fn create_changeset(
        &self,
        cs: &mut SourceChangeset,
    ) -> Result<bool, SourceError> {
        let opts = self.changeset_to_pull_request_input(cs);
        let repo = Self::repo_metadata(&cs.target_repo)?;
        let pr = self
            .client
            .create_pull_request(&repo.full_name, &opts)
            .await
            .map_err(|err| {
                SourceError::other(err.to_string()).with_prefix("creating pull request")
            })?;
        self.set_changeset_metadata(&repo, &pr, cs).await?;
        // Bitbucket silently updates an existing PR, so the baseline reports
        // that it existed.
        Ok(true)
    }

    async fn update_changeset(&self, cs: &mut SourceChangeset) -> Result<(), SourceError> {
        let opts = self.changeset_to_pull_request_input(cs);
        let repo = Self::repo_metadata(&cs.target_repo)?;
        let id = Self::metadata_pr_id(cs)?;
        let updated = self
            .client
            .update_pull_request(&repo.full_name, id, &opts)
            .await
            .map_err(|err| {
                SourceError::other(err.to_string()).with_prefix("updating pull request")
            })?;
        self.set_changeset_metadata(&repo, &updated, cs).await
    }

    async fn close_changeset(&self, cs: &mut SourceChangeset) -> Result<(), SourceError> {
        let repo = Self::repo_metadata(&cs.target_repo)?;
        let id = Self::metadata_pr_id(cs)?;
        let updated = self
            .client
            .decline_pull_request(&repo.full_name, id)
            .await
            .map_err(|err| {
                SourceError::other(err.to_string()).with_prefix("declining pull request")
            })?;
        self.set_changeset_metadata(&repo, &updated, cs).await
    }

    async fn reopen_changeset(&self, cs: &mut SourceChangeset) -> Result<(), SourceError> {
        // Bitbucket Cloud cannot reopen a declined PR; recreating it is the
        // baseline's behaviour (and a no-op for open PRs).
        self.create_changeset(cs).await.map(|_| ())
    }

    async fn create_comment(
        &self,
        cs: &SourceChangeset,
        text: &str,
    ) -> Result<(), SourceError> {
        let repo = Self::repo_metadata(&cs.target_repo)?;
        let id = Self::metadata_pr_id(cs)?;
        self.client
            .create_pull_request_comment(
                &repo.full_name,
                id,
                &bbc::CommentInput {
                    content: text.to_owned(),
                },
            )
            .await
            .map(|_| ())
            .map_err(|err| SourceError::other(err.to_string()))
    }

    async fn merge_changeset(
        &self,
        cs: &mut SourceChangeset,
        squash: bool,
    ) -> Result<(), SourceError> {
        let repo = Self::repo_metadata(&cs.target_repo)?;
        let id = Self::metadata_pr_id(cs)?;
        let merge_strategy = if squash {
            Some(bbc::MERGE_STRATEGY_SQUASH.to_owned())
        } else {
            None
        };
        let updated = match self
            .client
            .merge_pull_request(
                &repo.full_name,
                id,
                &bbc::MergePullRequestOpts {
                    merge_strategy,
                    ..Default::default()
                },
            )
            .await
        {
            Ok(updated) => updated,
            Err(err) if err.is_not_found() => {
                return Err(
                    SourceError::other(err.to_string()).with_prefix("merging pull request")
                )
            }
            Err(err) => return Err(SourceError::not_mergeable(err.to_string())),
        };
        self.set_changeset_metadata(&repo, &updated, cs).await
    }

    fn supports_fork(&self) -> bool {
        true
    }

    async fn get_fork(
        &self,
        target_repo: &Repo,
        namespace: Option<&String>,
        name: Option<&String>,
    ) -> Result<Repo, SourceError> {
        let namespace = match namespace {
            Some(namespace) => namespace.clone(),
            None => {
                let user = self
                    .client
                    .current_user()
                    .await
                    .map_err(|err| {
                        SourceError::other(err.to_string())
                            .with_prefix("getting the current user")
                    })?;
                user.account.username
            }
        };
        let target = Self::repo_metadata(target_repo)?;
        let target_namespace = target
            .namespace()
            .map_err(|err| {
                SourceError::other(err).with_prefix("getting target repo namespace")
            })?;
        let fork_name = name
            .cloned()
            .unwrap_or_else(|| default_fork_name(&target_namespace, &target.slug));

        match self.client.repo(&namespace, &fork_name).await {
            Ok(fork) => return Self::check_and_copy(target_repo, fork),
            Err(err) if err.is_not_found() => {}
            Err(err) => {
                return Err(SourceError::other(err.to_string())
                    .with_prefix("checking for fork existence"))
            }
        }

        let fork = self
            .client
            .fork_repository(
                &target,
                &bbc::ForkInput {
                    name: Some(fork_name),
                    workspace: namespace,
                },
            )
            .await
            .map_err(|err| {
                SourceError::other(err.to_string()).with_prefix("forking repository")
            })?;
        Self::check_and_copy(target_repo, fork)
    }
}

// ---------------------------------------------------------------------------
// Perforce (gitserver RPC seam)
// ---------------------------------------------------------------------------

/// `no credentials set for Perforce Source` (the baseline's sentinel).
pub const PERFORCE_NO_CREDENTIALS: &str = "no credentials set for Perforce Source";
/// The baseline's placeholder for the unimplemented Perforce operations.
pub const PERFORCE_NOT_IMPLEMENTED: &str = "not implemented";
/// Explicit not-ported text for the missing `PerforceGetChangelist` RPC.
pub const PERFORCE_NOT_PORTED_GET_CHANGELIST: &str =
    "perforce gitserver RPC PerforceGetChangelist is not ported to the Rust gitserver";
/// Explicit not-ported text for the missing `CheckPerforceCredentials` RPC.
pub const PERFORCE_NOT_PORTED_CHECK_CREDENTIALS: &str =
    "perforce gitserver RPC CheckPerforceCredentials is not ported to the Rust gitserver";

/// The gitserver RPC boundary the Perforce changeset source needs. The Rust
/// gitserver does not expose these RPCs; the source accepts an injected
/// implementation so the gitserver-backed paths are testable.
#[async_trait]
pub trait PerforceChangesetGitserver: Send + Sync {
    /// `gitserver.Client.PerforceGetChangelist`.
    async fn perforce_get_changelist(
        &self,
        connection: &perforce::PerforceConnectionDetails,
        changelist: &str,
    ) -> Result<perforce::Changelist, String>;

    /// `gitserver.Client.CheckPerforceCredentials`.
    async fn check_perforce_credentials(
        &self,
        connection: &perforce::PerforceConnectionDetails,
    ) -> Result<(), String>;
}

/// `sources.PerforceSource`.
pub struct PerforceSource {
    conn: PerforceConnection,
    gitserver: Option<Arc<dyn PerforceChangesetGitserver>>,
    perforce_creds: Option<perforce::PerforceConnectionDetails>,
}

impl PerforceSource {
    /// `sources.NewPerforceSource` config decoding, without the gitserver
    /// client (see the module docs and the slice overview).
    pub fn from_config(config: &Value) -> Result<Self, SourceError> {
        let conn: PerforceConnection = serde_json::from_value(config.clone()).map_err(|err| {
            SourceError::other(format!("decoding Perforce configuration: {err}"))
        })?;
        Ok(Self {
            conn,
            gitserver: None,
            perforce_creds: None,
        })
    }

    /// A source with the default (empty) configuration, used by the Batch
    /// Changes factory, which does not receive the decoded config.
    pub fn new_empty() -> Self {
        Self {
            conn: PerforceConnection::default(),
            gitserver: None,
            perforce_creds: None,
        }
    }

    pub fn with_gitserver(mut self, gitserver: Arc<dyn PerforceChangesetGitserver>) -> Self {
        self.gitserver = Some(gitserver);
        self
    }

    pub fn config(&self) -> &PerforceConnection {
        &self.conn
    }

    /// `PerforceSource.WithAuthenticator`: both basic variants install the
    /// same credentials; any other authenticator conflicts.
    pub fn with_authenticator(mut self, auth: SourceAuth) -> Result<Self, SourceError> {
        match auth {
            SourceAuth::Basic { username, password } => {
                self.perforce_creds = Some(perforce::PerforceConnectionDetails {
                    p4_port: self.conn.p4_port.clone(),
                    p4_user: username,
                    p4_passwd: password,
                });
                Ok(self)
            }
            _ => Err(SourceError::other(
                "unexpected auther type for Perforce Source",
            )),
        }
    }

    pub fn with_credentials(mut self, username: &str, password: &str) -> Self {
        self.perforce_creds = Some(perforce::PerforceConnectionDetails {
            p4_port: self.conn.p4_port.clone(),
            p4_user: username.to_owned(),
            p4_passwd: password.to_owned(),
        });
        self
    }

    /// `PerforceSource.ValidateAuthenticator`.
    pub async fn validate_authenticator(&self) -> Result<(), SourceError> {
        let Some(creds) = &self.perforce_creds else {
            return Err(SourceError::other(PERFORCE_NO_CREDENTIALS));
        };
        match &self.gitserver {
            Some(gitserver) => gitserver
                .check_perforce_credentials(creds)
                .await
                .map_err(SourceError::other),
            None => Err(SourceError::other(PERFORCE_NOT_PORTED_CHECK_CREDENTIALS)),
        }
    }

    fn set_changeset_metadata(
        changelist: &perforce::Changelist,
        cs: &mut SourceChangeset,
    ) -> Result<(), SourceError> {
        cs.changeset.external_id = changelist.id.clone();
        cs.changeset.metadata =
            serde_json::to_value(changelist).unwrap_or(Value::Null);
        Ok(())
    }
}

#[async_trait]
impl ChangesetSource for PerforceSource {
    fn kind(&self) -> &'static str {
        "perforce"
    }

    async fn load_changeset(&self, cs: &mut SourceChangeset) -> Result<(), SourceError> {
        let Some(creds) = &self.perforce_creds else {
            return Err(SourceError::other(PERFORCE_NO_CREDENTIALS));
        };
        let Some(gitserver) = &self.gitserver else {
            return Err(SourceError::other(format!(
                "getting changelist: {PERFORCE_NOT_PORTED_GET_CHANGELIST}"
            )));
        };
        let changelist = gitserver
            .perforce_get_changelist(creds, &cs.changeset.external_id)
            .await
            .map_err(|err| {
                SourceError::other(err).with_prefix("getting changelist")
            })?;
        Self::set_changeset_metadata(&changelist, cs)
            .map_err(|err| err.with_prefix("setting perforce changeset metadata"))?;
        Ok(())
    }

    async fn create_changeset(
        &self,
        cs: &mut SourceChangeset,
    ) -> Result<bool, SourceError> {
        self.load_changeset(cs).await.map(|_| false)
    }

    async fn create_draft_changeset(
        &self,
        _cs: &mut SourceChangeset,
    ) -> Result<bool, SourceError> {
        Err(SourceError::other(PERFORCE_NOT_IMPLEMENTED))
    }

    async fn undraft_changeset(&self, _cs: &mut SourceChangeset) -> Result<(), SourceError> {
        Err(SourceError::other(PERFORCE_NOT_IMPLEMENTED))
    }

    async fn close_changeset(&self, _cs: &mut SourceChangeset) -> Result<(), SourceError> {
        Err(SourceError::other(PERFORCE_NOT_IMPLEMENTED))
    }

    async fn update_changeset(&self, _cs: &mut SourceChangeset) -> Result<(), SourceError> {
        Err(SourceError::other(PERFORCE_NOT_IMPLEMENTED))
    }

    async fn reopen_changeset(&self, _cs: &mut SourceChangeset) -> Result<(), SourceError> {
        Err(SourceError::other(PERFORCE_NOT_IMPLEMENTED))
    }

    async fn create_comment(
        &self,
        _cs: &SourceChangeset,
        _text: &str,
    ) -> Result<(), SourceError> {
        Err(SourceError::other(PERFORCE_NOT_IMPLEMENTED))
    }

    async fn merge_changeset(
        &self,
        _cs: &mut SourceChangeset,
        _squash: bool,
    ) -> Result<(), SourceError> {
        Err(SourceError::other(PERFORCE_NOT_IMPLEMENTED))
    }

    fn supports_draft(&self) -> bool {
        true
    }
}

// ---------------------------------------------------------------------------
// Factory (`buildChangesetSource`)
// ---------------------------------------------------------------------------

/// The external-service kinds this port can build sources for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SourceKind {
    GitHub,
    GitLab,
    BitbucketServer,
    AzureDevOps,
    BitbucketCloud,
    Gerrit,
    Perforce,
}

impl SourceKind {
    pub fn from_external_service_kind(kind: &str) -> Option<Self> {
        match kind {
            "GITHUB" => Some(Self::GitHub),
            "GITLAB" => Some(Self::GitLab),
            "BITBUCKETSERVER" => Some(Self::BitbucketServer),
            "AZUREDEVOPS" => Some(Self::AzureDevOps),
            "BITBUCKETCLOUD" => Some(Self::BitbucketCloud),
            "GERRIT" => Some(Self::Gerrit),
            "PERFORCE" => Some(Self::Perforce),
            _ => None,
        }
    }
}

/// Build a source for a supported kind. `base_url` is the code-host web URL;
/// the GitHub source derives the API URL with `github::api_root` like
/// `newGitHubSource`.
pub fn new_source(
    kind: &str,
    base_url: &Url,
    auth: SourceAuth,
    http: Arc<dyn HttpDoer>,
) -> Result<Arc<dyn ChangesetSource>, SourceError> {
    match SourceKind::from_external_service_kind(kind) {
        Some(SourceKind::GitHub) => {
            let (api_url, _) = github::api_root(base_url);
            Ok(Arc::new(GitHubSource::new(api_url, auth, http)))
        }
        Some(SourceKind::GitLab) => Ok(Arc::new(GitLabSource::new(base_url, auth, http))),
        Some(SourceKind::BitbucketServer) => {
            Ok(Arc::new(BitbucketServerSource::new(base_url, auth, http)))
        }
        Some(SourceKind::AzureDevOps) => Ok(Arc::new(AzureDevOpsSource::new(
            base_url, auth, http,
        )?)),
        Some(SourceKind::BitbucketCloud) => Ok(Arc::new(BitbucketCloudSource::new(
            base_url, auth, http,
        )?)),
        Some(SourceKind::Gerrit) => Ok(Arc::new(GerritSource::new(base_url, auth, http)?)),
        // The Perforce source needs the gitserver client and the decoded
        // config, neither of which this factory receives; callers that have
        // them construct the source with `PerforceSource::from_config`.
        Some(SourceKind::Perforce) => Ok(Arc::new(PerforceSource::new_empty())),
        None => {
            if is_batches_source_kind(kind) {
                // Gerrit / Perforce / Bitbucket Cloud are OSS kinds whose
                // source constructors are still not ported.
                Err(SourceError::non_retryable(format!(
                    "building changeset source: code-host source for kind {kind} is not ported"
                )))
            } else {
                Err(unsupported_source_kind(kind))
            }
        }
    }
}

/// The connection a `sources.New<Kind>Source` constructor resolves before it
/// builds its client: the external-service kind, the base URL the constructor
/// uses, and the authenticator it installs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceConnection {
    /// The canonical external-service kind (`GITHUB`, `GITLAB`,
    /// `BITBUCKETSERVER`), i.e. the key [`new_source`] dispatches on.
    pub kind: String,
    /// The web base URL handed to the source constructor. GitHub and GitLab are
    /// normalized with `extsvc.NormalizeBaseURL`; Bitbucket Server keeps the
    /// raw parsed URL like `bitbucketserver.NewClient`.
    pub base_url: Url,
    /// The authenticator derived from the connection config.
    pub auth: SourceAuth,
}

/// Resolve the plaintext external-service configuration exactly like the
/// code-host source constructors do before their first request:
///
/// * `github.go:35-79` (`ghauth.FromConnection` → `OAuthBearerToken`),
/// * `gitlab.go:37-82` (`token.type == "oauth"` → OAuth bearer, otherwise
///   `gitlab.SudoableToken`; no token → no authenticator),
/// * `gitbucketserver/client.go:66-92` (`token` → OAuth bearer, otherwise
///   `BasicAuth` with `username`/`password`; OAuth 1 is a seam),
/// * `azuredevops.go:31-54` (`urlx.Parse` on `url`; `BasicAuth` with
///   `username`/`token` — the PAT is the password).
///
/// GitHub App connections and Bitbucket Server OAuth 1 are not ported and are
/// reported as non-retryable errors (see the slice overview §4).
pub fn source_connection_from_config(
    external_service_kind: &str,
    config: &Value,
) -> Result<SourceConnection, SourceError> {
    let Some(source_kind) = SourceKind::from_external_service_kind(external_service_kind) else {
        return Err(if is_batches_source_kind(external_service_kind) {
            SourceError::non_retryable(format!(
                "building changeset source: code-host source for kind {external_service_kind} is not ported"
            ))
        } else {
            unsupported_source_kind(external_service_kind)
        });
    };

    let raw_url = config
        .get("url")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let parse_std = |raw: &str| {
        Url::parse(raw)
            .map_err(|err| SourceError::other(format!("parsing external service url: {err}")))
    };

    let (base_url, auth) = match source_kind {
        SourceKind::GitHub => {
            if config
                .get("gitHubAppDetails")
                .map(|details| !details.is_null())
                .unwrap_or(false)
            {
                return Err(SourceError::non_retryable(
                    "building changeset source: GitHub App authentication is not ported",
                ));
            }
            let token = config
                .get("token")
                .and_then(Value::as_str)
                .unwrap_or_default();
            (
                github::normalize_base_url(&parse_std(raw_url)?),
                SourceAuth::OAuthToken(token.to_owned()),
            )
        }
        SourceKind::GitLab => {
            let token = config
                .get("token")
                .and_then(Value::as_str)
                .unwrap_or_default();
            let auth = if token.is_empty() {
                SourceAuth::None
            } else if config.get("token.type").and_then(Value::as_str) == Some("oauth") {
                SourceAuth::OAuthToken(token.to_owned())
            } else {
                SourceAuth::GitLabPrivateToken(token.to_owned())
            };
            (github::normalize_base_url(&parse_std(raw_url)?), auth)
        }
        SourceKind::BitbucketServer => {
            let authorization = config.get("authorization").filter(|value| !value.is_null());
            let oauth2 = authorization
                .and_then(|value| value.get("oauth2"))
                .and_then(Value::as_bool)
                .unwrap_or(false);
            let has_oauth1 = authorization
                .and_then(|value| value.get("oauth"))
                .map(|value| !value.is_null())
                .unwrap_or(false);
            let auth = if authorization.is_none() || oauth2 {
                let token = config
                    .get("token")
                    .and_then(Value::as_str)
                    .unwrap_or_default();
                if token.is_empty() {
                    SourceAuth::Basic {
                        username: config
                            .get("username")
                            .and_then(Value::as_str)
                            .unwrap_or_default()
                            .to_owned(),
                        password: config
                            .get("password")
                            .and_then(Value::as_str)
                            .unwrap_or_default()
                            .to_owned(),
                    }
                } else {
                    SourceAuth::BearerToken(token.to_owned())
                }
            } else if has_oauth1 {
                return Err(SourceError::non_retryable(
                    "building changeset source: Bitbucket Server OAuth 1 authentication is not ported",
                ));
            } else {
                // `bitbucketserver.NewClient` leaves `Auth` nil when
                // `authorization.oauth2` is false and no OAuth 1 block exists.
                SourceAuth::None
            };
            // `newClient` parses `config.Url` without `NormalizeBaseURL`.
            (parse_std(raw_url)?, auth)
        }
        SourceKind::BitbucketCloud => {
            // `bitbucketcloud.NewClient` reads `apiURL` (default
            // `https://api.bitbucket.org`), normalizes it, and authenticates
            // with the access token when set, else username/app password.
            let raw_api_url = config
                .get("apiURL")
                .and_then(Value::as_str)
                .filter(|value| !value.is_empty())
                .unwrap_or("https://api.bitbucket.org");
            let auth = match config.get("accessToken").and_then(Value::as_str) {
                Some(token) if !token.is_empty() => SourceAuth::BearerToken(token.to_owned()),
                _ => SourceAuth::Basic {
                    username: config
                        .get("username")
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                        .to_owned(),
                    password: config
                        .get("appPassword")
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                        .to_owned(),
                },
            };
            (github::normalize_base_url(&parse_std(raw_api_url)?), auth)
        }
        SourceKind::Gerrit => {
            // `sources.NewGerritSource` parses `config.Url` and uses
            // `BasicAuth{username, password}`.
            (
                parse_std(raw_url)?,
                SourceAuth::Basic {
                    username: config
                        .get("username")
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                        .to_owned(),
                    password: config
                        .get("password")
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                        .to_owned(),
                },
            )
        }
        SourceKind::Perforce => {
            // Perforce has no HTTP API. Its `p4.port` (e.g. `ssl:host:1666`)
            // is not a URL authority, so the connection carries a fixed
            // placeholder URL; the source reads the depot connection from
            // `config` instead.
            (
                Url::parse("perforce://perforce.invalid/").expect("static URL parses"),
                SourceAuth::None,
            )
        }
        SourceKind::AzureDevOps => {
            // `sources.NewAzureDevOpsSource` parses `config.Url` with
            // `urlx.Parse` and authenticates with
            // `auth.BasicAuth{Username: config.username, Password: config.token}`
            // (the PAT is the password).
            let parsed = BaseUrl::parse(raw_url).map_err(|err| {
                SourceError::other(format!("parsing external service url: {err}"))
            })?;
            (
                parsed.url,
                SourceAuth::Basic {
                    username: config
                        .get("username")
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                        .to_owned(),
                    password: config
                        .get("token")
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                        .to_owned(),
                },
            )
        }
    };

    Ok(SourceConnection {
        kind: external_service_kind.to_owned(),
        base_url,
        auth,
    })
}

// ---------------------------------------------------------------------------
// Batches-domain GitHub App runtime authentication
// (`sources.withGitHubAppAuthenticator`)
// ---------------------------------------------------------------------------

/// The domain used by Batch Changes GitHub Apps (`internal/types`:
/// `BatchesGitHubAppDomain`).
pub const BATCHES_GITHUB_APP_DOMAIN: &str = "batches";

/// The subset of `github_apps` that `withGitHubAppAuthenticator` reads.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct BatchesGitHubApp {
    pub app_id: i32,
    /// PEM-encoded RSA private key (decrypted).
    pub private_key: String,
    /// The App's stored base URL (not the external-service URL).
    pub base_url: String,
}

/// `github_apps.Store` subset used by the batches GitHub App authenticator:
/// `GetByDomainAndKind` + `GetInstallID`.
///
/// The production Postgres/Redis adapter lives with the existing GitHub Apps
/// database wiring; this seam carries the exact semantics:
///
/// * `get_by_domain_and_kind(domain, kind, base_url)` compares
///   `trim(trailing '/' from github_apps.base_url)` and returns any error for a
///   missing row.
/// * `get_install_id(app_id, account)` returns the newest
///   `github_app_installs.installation_id` for `app_id` whose lowercased
///   `account_login` matches `account`; a missing install is an error.
#[async_trait]
pub trait BatchesGitHubAppStore: Send + Sync {
    async fn get_by_domain_and_kind(
        &self,
        domain: &str,
        kind: &str,
        base_url: &str,
    ) -> Result<BatchesGitHubApp, String>;

    async fn get_install_id(&self, app_id: i32, account: &str) -> Result<i32, String>;
}

/// Injectable pieces of the installation-authenticator construction. The
/// baseline always passes the site keyring's `GitHubAppKey` and a Redis cache;
/// both are seams here, and `doer` is only used if the token is refreshed.
#[derive(Clone)]
pub struct GitHubAppTokenSeams {
    pub cache: Option<Arc<dyn InstallationTokenCache>>,
    pub doer: Arc<dyn HttpDoer>,
}

impl GitHubAppTokenSeams {
    pub fn offline(doer: Arc<dyn HttpDoer>) -> Self {
        Self { cache: None, doer }
    }
}

/// `sources.ForChangeset`'s early rejection: a `GITHUB_APP` strategy can only
/// be used with a GitHub connection, checked before the source is built.
pub fn reject_non_github_app_authentication(
    strategy: &str,
    external_service_kind: &str,
) -> Result<(), SourceError> {
    if strategy == "GITHUB_APP" && external_service_kind != "GITHUB" {
        return Err(SourceError::other(EXTERNAL_SERVICE_NOT_GITHUB));
    }
    Ok(())
}

/// `sources.withGitHubAppAuthenticator`: resolve the batches-domain GitHub App
/// for the connection's normalized base URL, look up the account's installation
/// and return its installation-token authenticator.
///
/// Error priority and text match the baseline exactly:
/// non-GitHub kind → `ErrExternalServiceNotGitHub`; unparsable connection URL →
/// `"parsing GitHub connection URL: ..."`; any app lookup error →
/// `ErrNoGitHubAppConfigured`; install lookup error or ID `0` →
/// `ErrNoGitHubAppInstallation`; bad key → `"creating GitHub App
/// authenticator: ..."`; unparsable App base URL → `"parsing GitHub App base
/// URL: ..."`.
pub async fn with_github_app_authenticator(
    external_service_kind: &str,
    config: &Value,
    store: &dyn BatchesGitHubAppStore,
    account: &str,
    kind: &str,
    seams: GitHubAppTokenSeams,
) -> Result<SourceAuth, SourceError> {
    if external_service_kind != "GITHUB" {
        return Err(SourceError::other(EXTERNAL_SERVICE_NOT_GITHUB));
    }

    let raw_url = config
        .get("url")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let connection_url = Url::parse(raw_url).map_err(|err| {
        SourceError::other(format!("parsing GitHub connection URL: {err}"))
    })?;
    let connection_url = github::normalize_base_url(&connection_url);

    let app = store
        .get_by_domain_and_kind(
            BATCHES_GITHUB_APP_DOMAIN,
            kind,
            connection_url.as_str(),
        )
        .await
        .map_err(|_| SourceError::other(NO_GITHUB_APP_CONFIGURED))?;

    let installation_id = store
        .get_install_id(app.app_id, account)
        .await
        .map_err(|_| SourceError::other(NO_GITHUB_APP_INSTALLATION))?;
    if installation_id == 0 {
        return Err(SourceError::other(NO_GITHUB_APP_INSTALLATION));
    }

    let app_authenticator = Arc::new(
        GitHubAppAuthenticator::new(app.app_id, app.private_key).map_err(|err| {
            SourceError::other(format!("creating GitHub App authenticator: {err}"))
        })?,
    );

    let app_base_url = Url::parse(&app.base_url).map_err(|err| {
        SourceError::other(format!("parsing GitHub App base URL: {err}"))
    })?;

    Ok(SourceAuth::GitHubAppInstallation(Arc::new(
        InstallationAuthenticator::new(
            app_base_url,
            installation_id,
            app_authenticator,
            seams.cache,
            seams.doer,
        ),
    )))
}

/// Runtime source selection for `Sourcer.ForChangeset` / `ForExternalService`:
/// build the connection from config, then let a `GITHUB_APP` strategy replace
/// the config-derived authenticator with the batches-domain installation token.
///
/// The GitHub App path takes priority over the connection's PAT; a
/// `USER_CREDENTIAL` strategy keeps the config-derived authenticator and never
/// touches the App store. An empty `app_kind` is rejected with the baseline's
/// caller-level message (the store lookup itself is not attempted).
pub async fn resolve_changeset_source_connection(
    external_service_kind: &str,
    config: &Value,
    strategy: &str,
    app_kind: Option<&str>,
    account: &str,
    store: Option<&dyn BatchesGitHubAppStore>,
    seams: GitHubAppTokenSeams,
) -> Result<SourceConnection, SourceError> {
    reject_non_github_app_authentication(strategy, external_service_kind)?;

    if strategy != "GITHUB_APP" {
        return source_connection_from_config(external_service_kind, config);
    }

    let app_kind = app_kind.unwrap_or_default();
    if app_kind.is_empty() {
        return Err(SourceError::other(
            "ForChangeset with AuthenticationStrategyGitHubApp must be called with a gitHubAppKind, but was called with empty string.",
        ));
    }

    let Some(store) = store else {
        return Err(SourceError::other(NO_GITHUB_APP_CONFIGURED));
    };
    let auth = with_github_app_authenticator(
        external_service_kind,
        config,
        store,
        account,
        app_kind,
        seams,
    )
    .await?;

    // The source is still built from the external-service connection URL;
    // only its authenticator is replaced (Go: `css.WithAuthenticator`).
    let raw_url = config
        .get("url")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let base_url = Url::parse(raw_url).map_err(|err| {
        SourceError::other(format!("parsing GitHub connection URL: {err}"))
    })?;
    Ok(SourceConnection {
        kind: external_service_kind.to_owned(),
        base_url: github::normalize_base_url(&base_url),
        auth,
    })
}

// ---------------------------------------------------------------------------
// M9.33b runtime caller wiring (`internal/batches/syncer`,
// `internal/batches/reconciler/executor.go`)
// ---------------------------------------------------------------------------

/// `sources.AuthenticationStrategyUserCredential`.
pub const AUTHENTICATION_STRATEGY_USER_CREDENTIAL: &str = "USER_CREDENTIAL";
/// `sources.AuthenticationStrategyGitHubApp`.
pub const AUTHENTICATION_STRATEGY_GITHUB_APP: &str = "GITHUB_APP";
/// `ghtypes.CommitSigningGitHubAppKind`.
pub const COMMIT_SIGNING_GITHUB_APP_KIND: &str = "COMMIT_SIGNING";
/// `createChangesetSourceForGHApp`'s strategy guard.
pub const COMMIT_SIGNING_ONLY_GITHUB_APPS: &str =
    "commit signing is only supported for GitHub apps";
/// `runAfterCommit`'s message when commit signing is required but absent.
pub const NO_APP_TO_SIGN_COMMIT: &str =
    "no GitHub App configured to sign commit, rejecting unverified commit";
/// `runAfterCommit`'s wrapper for non-`ErrNoGitHubAppConfigured` failures.
pub const GET_COMMIT_SIGNING_APP_FAILED: &str = "failed to get GitHub App for commit verification";
/// `changesetSpecForkNamespaceUser`: the sentinel meaning "fork into the
/// current user's namespace", which `GetForkNamespace` maps back to `nil`.
pub const FORK_NAMESPACE_USER: &str = "<user>";

/// The inputs of a runtime `Sourcer.ForChangeset` call (`SourcerOpts`).
#[derive(Debug, Clone)]
pub struct RuntimeSourceRequest<'a> {
    pub external_service_kind: &'a str,
    pub config: &'a Value,
    pub strategy: &'a str,
    pub github_app_kind: Option<&'a str>,
    pub account: &'a str,
    /// `SourcerOpts.AsNonCredential`: build the source without a user/site
    /// credential and authenticate through the GitHub App.
    pub as_non_credential: bool,
}

/// Runtime source selection shared by the batches syncer and reconciler.
///
/// * `as_non_credential` forces the GitHub App path and rejects any other
///   strategy with `"commit signing is only supported for GitHub apps"`.
/// * Everything else delegates to [`resolve_changeset_source_connection`]
///   (M9.32b): `GITHUB_APP` takes priority, `USER_CREDENTIAL` keeps the
///   config-derived site/PAT connection.
pub async fn resolve_runtime_changeset_source_connection(
    request: RuntimeSourceRequest<'_>,
    store: Option<&dyn BatchesGitHubAppStore>,
    seams: GitHubAppTokenSeams,
) -> Result<SourceConnection, SourceError> {
    if request.as_non_credential && request.strategy != AUTHENTICATION_STRATEGY_GITHUB_APP {
        return Err(SourceError::other(COMMIT_SIGNING_ONLY_GITHUB_APPS));
    }
    resolve_changeset_source_connection(
        request.external_service_kind,
        request.config,
        request.strategy,
        request.github_app_kind,
        request.account,
        store,
        seams,
    )
    .await
}

/// `github.SplitRepositoryNameWithOwner`.
pub fn split_repository_name_with_owner(
    name_with_owner: &str,
) -> Result<(String, String), SourceError> {
    let mut parts = name_with_owner.splitn(2, '/');
    let owner = parts.next().unwrap_or_default();
    let name = parts.next();
    match name {
        Some(name) if !name.contains('/') && !owner.is_empty() && !name.is_empty() => {
            Ok((owner.to_owned(), name.to_owned()))
        }
        _ => Err(SourceError::other(format!(
            "invalid GitHub repository \"owner/name\" string: {name_with_owner:?}"
        ))),
    }
}

/// `sources.getOwnerFromChangeset`: the account namespace whose GitHub App
/// installation signs/creates the changeset.
///
/// * fork with an explicit namespace → that namespace (`<user>` counts as
///   implicit);
/// * fork without one → the target repo's clone-URL owner;
/// * otherwise → the target repo metadata's `NameWithOwner` / `name_with_owner`.
pub fn changeset_owner_from_changeset(
    target_repo: &Repo,
    spec: Option<&ChangesetSpec>,
) -> Result<String, SourceError> {
    let spec = spec
        .ok_or_else(|| SourceError::other("getting changeset spec: changeset spec not found"))?;

    if spec.is_fork() {
        if let Some(namespace) = spec.fork_namespace() {
            if namespace != FORK_NAMESPACE_USER {
                return Ok(namespace.clone());
            }
        }
        let clone_url = get_clone_url(target_repo)
            .map_err(|err| err.with_prefix("getting url for forked changeset"))?;
        let path = clone_url.path().trim_start_matches('/');
        split_repository_name_with_owner(path)
            .map(|(owner, _)| owner)
            .map_err(|err| err.with_prefix("getting owner from repo name"))
    } else {
        let name_with_owner = target_repo
            .metadata
            .get("NameWithOwner")
            .or_else(|| target_repo.metadata.get("name_with_owner"))
            .and_then(Value::as_str)
            .unwrap_or_default();
        split_repository_name_with_owner(name_with_owner)
            .map(|(owner, _)| owner)
            .map_err(|err| err.with_prefix("getting owner from repo name"))
    }
}

/// Result of `executor.runAfterCommit`'s GitHub App resolution step.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CommitSigningResolution {
    /// A `GITHUB_APP`-authenticated source for `COMMIT_SIGNING`.
    UseSource(SourceConnection),
    /// `ErrNoGitHubAppConfigured` and `reject_unverified` is off: warn/noop.
    SkipNoApp,
    /// Any other resolution error and `reject_unverified` is off: log/noop.
    SkipError(String),
}

/// Inputs of `executor.runAfterCommit`.
#[derive(Debug, Clone)]
pub struct CommitSigningRequest<'a> {
    pub external_service_kind: &'a str,
    pub config: &'a Value,
    /// Owner resolved by [`changeset_owner_from_changeset`].
    pub account: &'a str,
    /// The strategy used for the original push; when it was not `GITHUB_APP`
    /// the signing source is built without a credential (`AsNonCredential`).
    pub original_strategy: &'a str,
    pub reject_unverified: bool,
}

/// `runAfterCommit`'s source-resolution semantics, without the gitserver push
/// or the REST duplicate (both remain seams).
///
/// `ErrNoGitHubAppConfigured` is a no-op unless `reject_unverified` is set;
/// any other error is likewise only fatal when rejecting. On success the
/// caller is expected to duplicate the commit through the signed source.
pub async fn resolve_commit_signing_source(
    request: CommitSigningRequest<'_>,
    store: Option<&dyn BatchesGitHubAppStore>,
    seams: GitHubAppTokenSeams,
) -> Result<CommitSigningResolution, SourceError> {
    let resolution = resolve_runtime_changeset_source_connection(
        RuntimeSourceRequest {
            external_service_kind: request.external_service_kind,
            config: request.config,
            strategy: AUTHENTICATION_STRATEGY_GITHUB_APP,
            github_app_kind: Some(COMMIT_SIGNING_GITHUB_APP_KIND),
            account: request.account,
            as_non_credential: request.original_strategy != AUTHENTICATION_STRATEGY_GITHUB_APP,
        },
        store,
        seams,
    )
    .await;

    match resolution {
        Ok(connection) => Ok(CommitSigningResolution::UseSource(connection)),
        Err(err) if err.message == NO_GITHUB_APP_CONFIGURED => {
            if request.reject_unverified {
                Err(SourceError::other(NO_APP_TO_SIGN_COMMIT))
            } else {
                Ok(CommitSigningResolution::SkipNoApp)
            }
        }
        Err(err) => {
            if request.reject_unverified {
                Err(err.with_prefix(GET_COMMIT_SIGNING_APP_FAILED))
            } else {
                Ok(CommitSigningResolution::SkipError(err.message))
            }
        }
    }
}

/// `sources.GitserverPushConfig`'s authenticator dispatch: the push credential
/// derived from a resolved source connection.
///
/// GitHub App installation tokens are read from the currently-held token; the
/// baseline refreshes inside `GitserverPushConfig`, which needs a mutable
/// authenticator and is therefore left to the push seam.
pub fn push_authenticator_for_source_auth(auth: &SourceAuth) -> Option<PushAuthenticator> {
    match auth {
        SourceAuth::None => None,
        SourceAuth::OAuthToken(token) | SourceAuth::GitLabPrivateToken(token) => {
            Some(PushAuthenticator::OAuthBearerToken {
                token: token.clone(),
            })
        }
        SourceAuth::BearerToken(token) => Some(PushAuthenticator::OAuthBearerToken {
            token: token.clone(),
        }),
        SourceAuth::Basic {
            username,
            password,
        } => Some(PushAuthenticator::BasicAuth {
            username: username.clone(),
            password: password.clone(),
        }),
        SourceAuth::GitHubAppInstallation(auth) => {
            Some(PushAuthenticator::GitHubAppInstallationToken {
                token: auth.get_token().token,
            })
        }
    }
}

/// Exposed for the differential harness: merge-request probe used by
/// `loadChangeset` returning the normalized metadata for a fixture.
pub async fn probe_load(
    source: &dyn ChangesetSource,
    cs: &mut SourceChangeset,
) -> Result<Value, SourceError> {
    source.load_changeset(cs).await?;
    Ok(cs.metadata())
}

// Keep `GitLabAuth` and header helpers linked for the module tests.
#[allow(dead_code)]
fn _auth_documentation(_auth: &GitLabAuth) {}

#[allow(dead_code)]
fn _header_documentation(headers: &[(String, String)]) -> Option<&str> {
    header_get(headers, "Content-Type")
}

// ---------------------------------------------------------------------------
// M9.34b: gitserver `CreateCommitFromPatch` and GitHub REST duplicate commit
// (`internal/gitserver`, `sources/github.go:138-173`,
// `reconciler/executor.go:616-648`)
// ---------------------------------------------------------------------------

/// `protocol.CreateCommitFromPatchError`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CreateCommitFromPatchError {
    pub repository_name: String,
    pub internal_error: String,
    pub command: String,
    pub combined_output: String,
}

impl CreateCommitFromPatchError {
    /// `CreateCommitFromPatchError.Error()`: the baseline returns only the
    /// internal error text.
    pub fn message(&self) -> &str {
        &self.internal_error
    }

    /// `executor.pushCommitError.Error()`'s display form. The baseline uses
    /// `%q` for the repo name and `strings.TrimSpace` for the combined output.
    pub fn push_commit_message(&self) -> String {
        format!(
            "creating commit from patch for repository {:?}: {}\n```\n$ {}\n{}\n```",
            self.repository_name,
            self.internal_error,
            self.command,
            self.combined_output.trim()
        )
    }
}

/// `protocol.CreateCommitFromPatchResponse`, reduced to the fields the
/// reconciler reads (`Rev`; `Error`/`ChangelistId` are carried for fidelity).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CreateCommitFromPatchResponse {
    pub rev: String,
    pub error: Option<CreateCommitFromPatchError>,
    pub changelist_id: String,
}

/// The two shapes of a failed `CreateCommitFromPatch`: the typed protocol
/// error (surfaced through `errors.As` in `pushCommit`) or any other
/// transport/RPC error.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GitserverPushError {
    Patch(CreateCommitFromPatchError),
    Other(String),
}

/// The gitserver RPC seam. Production is a gRPC `GitserverServiceClient`
/// (`crates/services/sg-service-repo-updater/src/gitserver.rs`); tests and the
/// differential inject a fake.
#[async_trait]
pub trait GitserverCommitClient: Send + Sync {
    /// `gitserver.Client.CreateCommitFromPatch`.
    async fn create_commit_from_patch(
        &self,
        request: CreateCommitFromPatchRequest,
    ) -> Result<CreateCommitFromPatchResponse, GitserverPushError>;
}

/// `executor.pushCommit`: call the RPC and map the two error shapes. A typed
/// protocol error whose combined output contains `patch does not apply` is
/// non-retryable (`errcode.MakeNonRetryable`); every other failure stays
/// retryable.
pub async fn push_commit(
    client: &dyn GitserverCommitClient,
    request: CreateCommitFromPatchRequest,
) -> Result<CreateCommitFromPatchResponse, SourceError> {
    client
        .create_commit_from_patch(request)
        .await
        .map_err(push_commit_error)
}

/// `pushCommit`'s error mapping, split out for the differential harness.
pub fn push_commit_error(error: GitserverPushError) -> SourceError {
    match error {
        GitserverPushError::Patch(error) => {
            let message = error.push_commit_message();
            if error.combined_output.contains("patch does not apply") {
                SourceError::non_retryable(message)
            } else {
                SourceError::other(message)
            }
        }
        GitserverPushError::Other(message) => SourceError::other(message),
    }
}

/// Bridges a [`SourceAuth`] onto the [`crate::auth::Authenticator`] trait the
/// GitHub REST client expects. The authenticator only needs to set the same
/// headers [`SourceAuth::apply`] does.
struct SourceAuthAuthenticator(SourceAuth);

impl crate::auth::Authenticator for SourceAuthAuthenticator {
    fn authenticate(
        &self,
        _method: &str,
        _url: &str,
        headers: &mut Vec<(String, String)>,
    ) -> Result<(), String> {
        self.0.apply(headers);
        Ok(())
    }

    fn hash(&self) -> String {
        format!("{:?}", self.0)
    }
}

/// `GitHubSource.DuplicateCommit` (`sources/github.go:138`): re-create the
/// pushed commit through the REST API so a GitHub App installation can sign
/// it, then move the branch ref onto the signed commit.
///
/// Error prefixes are verbatim: `getting owner and repo name to duplicate
/// commit`, `getting commit to duplicate`, `creating new commit`, `updating
/// ref to point to new commit`.
pub async fn duplicate_commit(
    api_url: &Url,
    auth: &SourceAuth,
    http: Arc<dyn HttpDoer>,
    request: &CreateCommitFromPatchRequest,
    repo: &Repo,
    rev: &str,
) -> Result<github::RestCommit, SourceError> {
    let message = request.commit_info.messages.join("\n");
    let name_with_owner = repo
        .metadata
        .get("NameWithOwner")
        .or_else(|| repo.metadata.get("name_with_owner"))
        .and_then(Value::as_str)
        .unwrap_or_default();
    let (owner, repo_name) = split_repository_name_with_owner(name_with_owner)
        .map_err(|err| err.with_prefix("getting owner and repo name to duplicate commit"))?;

    let authenticator: Arc<dyn crate::auth::Authenticator> =
        Arc::new(SourceAuthAuthenticator(auth.clone()));
    let client = github::V3Client::new(api_url.clone(), Some(authenticator), http);

    // Get the original, unsigned commit.
    let commit = client
        .get_ref(&owner, &repo_name, rev)
        .await
        .map_err(|err| SourceError::other(err.to_string()).with_prefix("getting commit to duplicate"))?;

    // Our new signed commit should have the same parents as the original.
    let parents: Vec<String> = commit.parents.iter().map(|parent| parent.sha.clone()).collect();
    // Create the new commit using the original tree SHA and parents; author
    // and committer are omitted because the App installation is the author.
    let new_commit = client
        .create_commit(
            &owner,
            &repo_name,
            &message,
            &commit.commit.tree.sha,
            &parents,
            None,
            None,
        )
        .await
        .map_err(|err| SourceError::other(err.to_string()).with_prefix("creating new commit"))?;

    // Update the branch ref to point to the new commit, orphaning the original.
    client
        .update_ref(&owner, &repo_name, rev, &new_commit.sha)
        .await
        .map_err(|err| {
            SourceError::other(err.to_string()).with_prefix("updating ref to point to new commit")
        })?;

    Ok(new_commit)
}

// ---------------------------------------------------------------------------
// M9.34b: user-credential-first / site-credential-fallback runtime selection
// (`sources.withAuthenticatorForUser` / `withSiteAuthenticator`,
// `sources.go:472-506`)
// ---------------------------------------------------------------------------

/// `database.UserCredentialScope` / `store.GetSiteCredentialOpts` lookup seam.
///
/// Production adapts the existing `user_credentials` (batches domain) and
/// `batch_changes_site_credentials` stores; no new tables or keys. `Ok(None)`
/// means "not found" (Go: `errcode.IsNotFound` / `store.ErrNoResults`); `Err`
/// carries any other lookup failure.
#[async_trait]
pub trait BatchesCredentialStore: Send + Sync {
    /// `loadUserCredential`: scope `(Domain: batches, UserID,
    /// ExternalServiceType, ExternalServiceID)`.
    async fn user_credential(
        &self,
        user_id: i32,
        repo: &Repo,
    ) -> Result<Option<SourceAuth>, String>;

    /// `loadSiteCredential`: scope `(ExternalServiceType,
    /// ExternalServiceID)`.
    async fn site_credential(&self, repo: &Repo) -> Result<Option<SourceAuth>, String>;
}

/// `sources.withAuthenticatorForUser`: prefer the user credential, fall back
/// to the site credential, and surface lookup errors with the baseline's
/// prefixes.
pub async fn with_authenticator_for_user(
    store: &dyn BatchesCredentialStore,
    user_id: i32,
    repo: &Repo,
) -> Result<SourceAuth, SourceError> {
    match store.user_credential(user_id, repo).await {
        Ok(Some(auth)) => Ok(auth),
        Ok(None) => with_site_authenticator(store, repo).await,
        Err(err) => Err(SourceError::other(err).with_prefix("loading user credential")),
    }
}

/// `sources.withSiteAuthenticator`: use the site credential or fail with
/// `ErrMissingCredentials`.
pub async fn with_site_authenticator(
    store: &dyn BatchesCredentialStore,
    repo: &Repo,
) -> Result<SourceAuth, SourceError> {
    match store.site_credential(repo).await {
        Ok(Some(auth)) => Ok(auth),
        Ok(None) => Err(SourceError::other(MISSING_CREDENTIALS)),
        Err(err) => Err(SourceError::other(err).with_prefix("loading site credential")),
    }
}

/// The runtime push-credential selection: resolve the user-preferred/site
/// credential `SourceAuth` and hand it to
/// [`push_authenticator_for_source_auth`]. A credential kind without a push
/// mapping is `ErrNoPushCredentials`.
pub async fn push_authenticator_for_user(
    store: &dyn BatchesCredentialStore,
    user_id: i32,
    repo: &Repo,
) -> Result<PushAuthenticator, SourceError> {
    let auth = with_authenticator_for_user(store, user_id, repo).await?;
    push_authenticator_for_source_auth(&auth)
        .ok_or_else(|| SourceError::non_retryable(NO_PUSH_CREDENTIALS))
}

/// Site-credential variant of [`push_authenticator_for_user`].
pub async fn push_authenticator_for_site(
    store: &dyn BatchesCredentialStore,
    repo: &Repo,
) -> Result<PushAuthenticator, SourceError> {
    let auth = with_site_authenticator(store, repo).await?;
    push_authenticator_for_source_auth(&auth)
        .ok_or_else(|| SourceError::non_retryable(NO_PUSH_CREDENTIALS))
}
