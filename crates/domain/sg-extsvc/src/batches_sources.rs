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
//!
//! Explicit not-ported (enterprise / credential-store boundaries): GitHub App
//! authenticators and commit signing, AzureDevOps / Gerrit / Perforce /
//! Bitbucket Cloud sources, outbound webhook GraphQL payloads, and
//! `state.SetDerivedState` (gitserver).

use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use url::Url;

use crate::auth::Authenticator as _;
use crate::github;
use crate::gitlab::GitLabAuth;
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
#[derive(Debug, Clone, PartialEq, Eq)]
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
}

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
// Factory (`buildChangesetSource`)
// ---------------------------------------------------------------------------

/// The external-service kinds this port can build sources for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SourceKind {
    GitHub,
    GitLab,
    BitbucketServer,
}

impl SourceKind {
    pub fn from_external_service_kind(kind: &str) -> Option<Self> {
        match kind {
            "GITHUB" => Some(Self::GitHub),
            "GITLAB" => Some(Self::GitLab),
            "BITBUCKETSERVER" => Some(Self::BitbucketServer),
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
        None => {
            if is_batches_source_kind(kind) {
                // AzureDevOps / Gerrit / Perforce / Bitbucket Cloud are OSS
                // kinds whose source constructors are still not ported.
                Err(SourceError::non_retryable(format!(
                    "building changeset source: code-host source for kind {kind} is not ported"
                )))
            } else {
                Err(unsupported_source_kind(kind))
            }
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
