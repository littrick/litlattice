#![forbid(unsafe_code)]

//! M9.18a frontend GraphQL business surface batch C.
//!
//! Ported here (main SDL, `cmd/frontend/graphqlbackend`):
//!
//! * `Query.externalServiceNamespaces` / `Query.externalServiceRepositories`
//!   (`external_services.go:590-660`, `external_service.go:535-660`) through
//!   an injectable `ExternalServiceDiscovery` seam (the default reproduces
//!   `repos.UnimplementedDiscoverySource`);
//! * `Query.renderMarkdown` / `Query.highlightCode` (`file.go:48-80`) through
//!   the `MarkdownRenderer` / `CodeHighlighter` seams;
//! * `Query.webhooks` and the `create`/`update`/`delete` webhook mutations
//!   (`cmd/frontend/internal/repos/webhooks/resolvers/resolver.go`,
//!   `internal/database/webhooks.go`, `cmd/frontend/internal/backend/webhooks.go`);
//! * `changeCodyPlan`, `checkMirrorRepositoryConnection`,
//!   `updateMirrorRepository`, `randomizeUserPassword`, `setTosAccepted`,
//!   `addPhabricatorRepo`, `resolvePhabricatorDiff`, `setUserIsSiteAdmin`,
//!   `reloadSite`, `triggerObservabilityTestAlert`, `sendTestEmail`,
//!   `syncExternalService`, `cancelExternalServiceSync` and
//!   `add`/`update`/`deletePackageRepoFilter`;
//! * the `UserEmail`, `AccessToken`, `Session`, `Webhook`,
//!   `ExternalServiceNamespace(Connection)`,
//!   `ExternalServiceRepository(Connection)`,
//!   `CheckMirrorRepositoryConnectionResult` and
//!   `RandomizeUserPasswordResult` object types.
//!
//! Every external side effect goes through an injectable trait; the production
//! defaults either reproduce the Go contract (SQL/errors) or fail honestly when
//! the frontend build has no gitserver/SMTP/repoupdater client. The differential
//! harness injects deterministic fakes on both sides.

use std::collections::HashMap;
use std::sync::Arc;

use sg_graphql::exec::{
    Actor, Arguments, ExecError, FieldValue, InputValue, ObjectValue, RootField,
};
use sg_graphql::json::Json;
use sqlx::{PgPool, Row};
use time::OffsetDateTime;

use crate::admin_store::{
    MUST_BE_SITE_ADMIN, MUST_BE_SITE_ADMIN_OR_SAME_USER, check_current_user_is_site_admin,
    check_site_admin_or_same_user,
};
use crate::graphql_resolvers::{PageInfoObject, UserData, datetime_rfc3339, unmarshal_relay_id};

// ---------------------------------------------------------------------------
// Relay helpers
// ---------------------------------------------------------------------------

/// `relay.MarshalID(kind, spec)` for an already-serialized JSON spec.
pub fn marshal_relay_json(kind: &str, spec: &str) -> String {
    use base64::Engine as _;
    base64::engine::general_purpose::URL_SAFE.encode(format!("{kind}:{spec}"))
}

/// `relay.MarshalID(kind, int)`.
pub fn marshal_relay_int_id(kind: &str, id: i64) -> String {
    marshal_relay_json(kind, &id.to_string())
}

/// `json.Marshal(types.ExternalServiceNamespace{...})` (and the repository
/// twin): `encoding/json` emits the fields in declaration order.
fn namespace_spec_json(id: i64, name: &str, external_id: &str) -> String {
    format!(
        "{{\"id\":{},\"name\":{},\"external_id\":{}}}",
        id,
        serde_json::to_string(name).unwrap_or_else(|_| "\"\"".to_owned()),
        serde_json::to_string(external_id).unwrap_or_else(|_| "\"\"".to_owned()),
    )
}

fn decode_url(s: &str) -> Option<Vec<u8>> {
    use base64::Engine as _;
    use base64::engine::general_purpose::{URL_SAFE, URL_SAFE_NO_PAD};
    URL_SAFE_NO_PAD
        .decode(s)
        .or_else(|_| URL_SAFE.decode(s))
        .ok()
}

/// `relay.UnmarshalSpec` for an `int64` ID (kind is not validated unless the
/// caller checks it first).
pub fn unmarshal_relay_i64(id: &str) -> Result<i64, String> {
    let decoded = decode_url(id).ok_or_else(|| "illegal base64 data".to_owned())?;
    let text = String::from_utf8_lossy(&decoded);
    let Some((_, spec)) = text.split_once(':') else {
        return Err("invalid graphql.ID".to_owned());
    };
    serde_json::from_str(spec).map_err(|error| error.to_string())
}

fn relay_kind_of(id: &str) -> Option<String> {
    let decoded = decode_url(id)?;
    let text = String::from_utf8_lossy(&decoded);
    let (kind, _) = text.split_once(':')?;
    Some(kind.to_owned())
}

/// `UnmarshalExternalServiceID`: the relay kind must be `ExternalService`.
fn unmarshal_external_service_id(id: &str) -> Result<i64, String> {
    const KIND: &str = "ExternalService";
    let kind = relay_kind_of(id).unwrap_or_default();
    if kind != KIND {
        return Err(format!(
            "expected graphql ID to have kind {KIND:?}; got {kind:?}"
        ));
    }
    unmarshal_relay_i64(id)
}

/// `TryUnmarshalExternalServiceID`.
fn try_unmarshal_external_service_id(id: Option<&str>) -> Result<Option<i64>, String> {
    match id {
        Some(id) => unmarshal_external_service_id(id).map(Some),
        None => Ok(None),
    }
}

// ---------------------------------------------------------------------------
// Seams
// ---------------------------------------------------------------------------

/// `internal/markdown.Render` (`internal/markdown/markdown.go:22`).
pub trait MarkdownRenderer: Send + Sync {
    fn render(&self, markdown: &str) -> Result<String, String>;
}

/// `highlight.Code` (`cmd/frontend/internal/highlight/highlight.go:316`).
/// `filepath` is `"file." + SyntectLanguageMap[strings.ToLower(fuzzyLanguage)]`.
#[async_trait::async_trait]
pub trait CodeHighlighter: Send + Sync {
    async fn highlight(
        &self,
        code: &str,
        filepath: &str,
        disable_timeout: bool,
    ) -> Result<String, String>;

    /// M9.36d: the full `highlight.Code` contract used by `CodeAsLines`:
    /// `highlightLongLines` selects `line_length_limit=2000` (omitted when
    /// true) and the boolean reports whether highlighting was aborted into a
    /// plain-text table (timeout/server error). The default keeps existing
    /// implementations working by never aborting.
    async fn highlight_with_options(
        &self,
        code: &str,
        filepath: &str,
        disable_timeout: bool,
        _highlight_long_lines: bool,
    ) -> Result<(String, bool), String> {
        Ok((self.highlight(code, filepath, disable_timeout).await?, false))
    }
}

/// `backend.ExternalServicesService.ListNamespaces` / `DiscoverRepos`
/// (`cmd/frontend/internal/backend/external_services.go:37-39`).
#[async_trait::async_trait]
pub trait ExternalServiceDiscovery: Send + Sync {
    async fn list_namespaces(
        &self,
        external_service_id: Option<i64>,
        kind: &str,
        config: &str,
    ) -> Result<Vec<ExternalNamespaceData>, String>;

    #[allow(clippy::too_many_arguments)]
    async fn discover_repos(
        &self,
        external_service_id: Option<i64>,
        kind: &str,
        config: &str,
        first: i32,
        query: &str,
        exclude_repos: &[String],
    ) -> Result<Vec<ExternalRepositoryData>, String>;
}

/// `gitserverClient.IsRepoCloneable`.
#[async_trait::async_trait]
pub trait RepoMirrorChecker: Send + Sync {
    async fn is_repo_cloneable(&self, repo_name: &str) -> Result<(), String>;
}

/// `repoupdater.Client.EnqueueRepoUpdate`.
#[async_trait::async_trait]
pub trait RepoUpdateEnqueuer: Send + Sync {
    async fn enqueue_repo_update(&self, repo_name: &str) -> Result<(), String>;
}

/// `txemail.Send` for `Mutation.sendTestEmail`.
#[async_trait::async_trait]
pub trait TestEmailSender: Send + Sync {
    async fn send_test_email(&self, to: &str, test_id: &str) -> Result<(), String>;
}

/// The `observability_test_metric_{warning,critical}` gauges.
pub trait ObservabilityAlerts: Send + Sync {
    fn trigger(&self, level: &str);
}

/// `processrestart.Restart` (scheduled after 750ms in the baseline).
pub trait SiteReloader: Send + Sync {
    fn reload(&self);
}

/// `phabricator.Client` (`internal/extsvc/phabricator/client.go`).
#[async_trait::async_trait]
pub trait PhabricatorClient: Send + Sync {
    async fn get_raw_diff(&self, diff_id: i32) -> Result<String, String>;
    async fn get_diff_info(&self, diff_id: i32) -> Result<PhabricatorDiffInfo, String>;
}

/// `gitserverClient` operations used by `ResolvePhabricatorDiff`.
#[async_trait::async_trait]
pub trait PhabricatorGit: Send + Sync {
    /// `ResolveRevision`: `Ok(true)` = found, `Ok(false)` =
    /// `RevisionNotFoundError`, `Err` = any other error.
    async fn resolve_revision(&self, repo: &str, rev: &str) -> Result<bool, String>;
    async fn create_commit_from_patch(
        &self,
        request: &PhabricatorPatchRequest,
    ) -> Result<(), String>;
    /// `RepositoryResolver.Commit`: `Ok(None)` = commit not found.
    async fn commit_oid(&self, repo: &str, rev: &str) -> Result<Option<String>, String>;
}

/// `backend.MakePasswordResetURL`: the raw reset URL (before resolution
/// against `conf.ExternalURLParsed()`).
#[async_trait::async_trait]
pub trait PasswordResetUrlProvider: Send + Sync {
    async fn make_password_reset_url(&self, user_id: i32, email: &str) -> Result<String, String>;
}

/// `sendPasswordResetURLToPrimaryEmail` (`users_randomize_password.go:35-58`),
/// only reachable when `conf.CanSendEmail()` is true. The production frontend
/// has no SMTP transport in this build, so the default fails with the same
/// error `txemail` reports.
#[async_trait::async_trait]
pub trait PasswordResetEmailSender: Send + Sync {
    async fn send_reset_password_email(&self, user_id: i32, reset_url: &str) -> Result<(), String>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PhabricatorDiffInfo {
    pub author_name: String,
    pub author_email: String,
    pub message: String,
    pub date: OffsetDateTime,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PhabricatorPatchRequest {
    pub repo: String,
    pub base_commit: String,
    pub target_ref: String,
    pub patch: String,
    pub author_name: String,
    pub author_email: String,
    pub message: String,
    pub date: OffsetDateTime,
}

pub const UNIMPLEMENTED_DISCOVERY: &str =
    "External Service type does not support discovery of repositories and namespaces.";

/// The production renderer (`internal/markdown.Render`).
pub type DefaultMarkdownRenderer = crate::markdown_graphql::GoldmarkRenderer;

/// The production highlighter (`highlight.Code` over syntect-server).
pub type DefaultCodeHighlighter = crate::markdown_graphql::SyntectCodeHighlighter;

#[derive(Debug, Default, Clone, Copy)]
pub struct UnimplementedDiscovery;

#[async_trait::async_trait]
impl ExternalServiceDiscovery for UnimplementedDiscovery {
    async fn list_namespaces(
        &self,
        _external_service_id: Option<i64>,
        _kind: &str,
        _config: &str,
    ) -> Result<Vec<ExternalNamespaceData>, String> {
        Err(UNIMPLEMENTED_DISCOVERY.to_owned())
    }

    async fn discover_repos(
        &self,
        _external_service_id: Option<i64>,
        _kind: &str,
        _config: &str,
        _first: i32,
        _query: &str,
        _exclude_repos: &[String],
    ) -> Result<Vec<ExternalRepositoryData>, String> {
        Err(UNIMPLEMENTED_DISCOVERY.to_owned())
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct UnconfiguredMirrorChecker;

#[async_trait::async_trait]
impl RepoMirrorChecker for UnconfiguredMirrorChecker {
    async fn is_repo_cloneable(&self, _repo_name: &str) -> Result<(), String> {
        Err("repository mirror checks are not available in this frontend build".to_owned())
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct UnconfiguredRepoUpdater;

#[async_trait::async_trait]
impl RepoUpdateEnqueuer for UnconfiguredRepoUpdater {
    async fn enqueue_repo_update(&self, _repo_name: &str) -> Result<(), String> {
        Err("repository update enqueuing is not available in this frontend build".to_owned())
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct UnconfiguredTestEmailSender;

#[async_trait::async_trait]
impl TestEmailSender for UnconfiguredTestEmailSender {
    async fn send_test_email(&self, _to: &str, _test_id: &str) -> Result<(), String> {
        Err("email sending is not configured".to_owned())
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct NoopObservabilityAlerts;

impl ObservabilityAlerts for NoopObservabilityAlerts {
    fn trigger(&self, _level: &str) {}
}

#[derive(Debug, Default, Clone, Copy)]
pub struct NoopSiteReloader;

impl SiteReloader for NoopSiteReloader {
    fn reload(&self) {}
}

#[derive(Debug, Default, Clone, Copy)]
pub struct UnconfiguredPhabricatorGit;

#[async_trait::async_trait]
impl PhabricatorGit for UnconfiguredPhabricatorGit {
    async fn resolve_revision(&self, _repo: &str, _rev: &str) -> Result<bool, String> {
        Err("gitserver is not configured in this frontend build".to_owned())
    }

    async fn create_commit_from_patch(
        &self,
        _request: &PhabricatorPatchRequest,
    ) -> Result<(), String> {
        Err("gitserver is not configured in this frontend build".to_owned())
    }

    async fn commit_oid(&self, _repo: &str, _rev: &str) -> Result<Option<String>, String> {
        Err("gitserver is not configured in this frontend build".to_owned())
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct UnconfiguredPasswordResetUrl;

#[async_trait::async_trait]
impl PasswordResetUrlProvider for UnconfiguredPasswordResetUrl {
    async fn make_password_reset_url(&self, _user_id: i32, _email: &str) -> Result<String, String> {
        Err("password reset URLs are not available in this frontend build".to_owned())
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct UnconfiguredPasswordResetEmail;

#[async_trait::async_trait]
impl PasswordResetEmailSender for UnconfiguredPasswordResetEmail {
    async fn send_reset_password_email(
        &self,
        _user_id: i32,
        _reset_url: &str,
    ) -> Result<(), String> {
        Err("email sending is not configured".to_owned())
    }
}

// ---------------------------------------------------------------------------
// Data types
// ---------------------------------------------------------------------------

/// `types.ExternalServiceNamespace` (`internal/types/types.go:667-671`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExternalNamespaceData {
    pub id: i64,
    pub name: String,
    pub external_id: String,
}

/// `types.ExternalServiceRepository` (`internal/types/types.go:674-678`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExternalRepositoryData {
    pub id: i64,
    pub name: String,
    pub external_id: String,
}

/// `types.Webhook` with the decrypted secret and the computed webhook URL.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WebhookData {
    pub id: i32,
    pub uuid: String,
    pub url: String,
    pub name: String,
    pub code_host_kind: String,
    pub code_host_urn: String,
    /// `None` for no secret (or a secret this build cannot decrypt).
    pub secret: Option<String>,
    pub created_at: OffsetDateTime,
    pub updated_at: OffsetDateTime,
    pub created_by: UserByIdLookupOrMissing,
    pub updated_by: UserByIdLookupOrMissing,
}

/// `webhookResolver.CreatedBy` / `UpdatedBy`: user id 0 → nil; a missing live
/// user is also nil (`errcode.IsNotFound`); other lookups error.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UserByIdLookupOrMissing {
    Missing,
    Found(UserData),
    Error(String),
}

/// `webhooksConnectionResolver` (all fields fully computed at prefetch time).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WebhookConnectionData {
    pub nodes: Vec<WebhookData>,
    pub total_count: i64,
    pub has_next_page: bool,
    pub end_cursor: Option<String>,
}

/// `userEmailResolver` (`user_emails.go:90-115`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UserEmailData {
    pub email: String,
    pub is_primary: bool,
    pub verified: bool,
    pub verification_pending: bool,
}

/// `database.AccessToken` (`internal/database/access_tokens.go`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AccessTokenData {
    pub id: i64,
    pub subject: Result<UserData, String>,
    pub scopes: Vec<String>,
    pub note: String,
    pub creator: Result<UserData, String>,
    pub created_at: OffsetDateTime,
    pub last_used_at: Option<OffsetDateTime>,
    pub expires_at: Option<OffsetDateTime>,
}

/// The full, unordered (already ordered by SQL) access-token list plus the
/// unlimited `Count`; `first` is applied at replay time.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AccessTokenConnectionFull {
    pub all: Vec<AccessTokenData>,
    pub total: i64,
}

/// Per-user fields of `User` that need their own prefetch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UserAccountsData {
    pub emails: Result<Vec<UserEmailData>, String>,
    /// `UserResolver.PrimaryEmail`: the verified primary email, `None` when
    /// there is none (or when dotcom hides it), or the `ListByUser` error.
    pub primary_email: Result<Option<UserEmailData>, String>,
    pub access_tokens: Result<AccessTokenConnectionFull, String>,
    pub has_verified_email: Result<bool, String>,
    pub session_can_sign_out: Result<bool, String>,
    pub email_verification_required: bool,
}

/// `conf.SiteConfig().ExternalServiceKind` argument mapping
/// (`package_repos.go:29-36`) duplicated here because that module keeps it
/// private.
fn external_service_to_scheme(kind: &str) -> Option<&'static str> {
    Some(match kind {
        "JVMPACKAGES" => "semanticdb",
        "NPMPACKAGES" => "npm",
        "GOMODULES" | "GOPACKAGES" => "go",
        "PYTHONPACKAGES" => "python",
        "RUSTPACKAGES" => "rust-analyzer",
        "RUBYPACKAGES" => "scip-ruby",
        _ => return None,
    })
}

/// The injectable batch-C dependencies and configuration switches.
pub struct InventoryCDeps {
    pub markdown: Arc<dyn MarkdownRenderer>,
    pub highlighter: Arc<dyn CodeHighlighter>,
    pub external_service_discovery: Arc<dyn ExternalServiceDiscovery>,
    pub mirror_checker: Arc<dyn RepoMirrorChecker>,
    pub repo_updater: Arc<dyn RepoUpdateEnqueuer>,
    pub test_email: Arc<dyn TestEmailSender>,
    pub observability: Arc<dyn ObservabilityAlerts>,
    pub site_reloader: Arc<dyn SiteReloader>,
    pub phabricator_client: Option<Arc<dyn PhabricatorClient>>,
    pub phabricator_git: Arc<dyn PhabricatorGit>,
    pub password_reset_url: Arc<dyn PasswordResetUrlProvider>,
    pub password_reset_email: Arc<dyn PasswordResetEmailSender>,
    /// `userpasswd.ResetPasswordEnabled()`.
    pub reset_password_enabled: bool,
    /// `dotcom.SourcegraphDotComMode()`.
    pub dotcom: bool,
    /// `conf.CanSendEmail()`.
    pub can_send_email: bool,
    /// `canReloadSite` (`site_reload.go:18`).
    pub can_reload_site: bool,
    /// `conf.Get().ExternalURL` (raw, empty by default).
    pub external_url: String,
    /// `conf.EmailVerificationRequired()`.
    pub email_verification_required: bool,
}

impl std::fmt::Debug for InventoryCDeps {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InventoryCDeps")
            .field("reset_password_enabled", &self.reset_password_enabled)
            .field("dotcom", &self.dotcom)
            .field("can_send_email", &self.can_send_email)
            .field("can_reload_site", &self.can_reload_site)
            .field("external_url", &self.external_url)
            .field(
                "email_verification_required",
                &self.email_verification_required,
            )
            .finish_non_exhaustive()
    }
}

impl Default for InventoryCDeps {
    fn default() -> Self {
        Self {
            markdown: Arc::new(DefaultMarkdownRenderer::default()),
            highlighter: Arc::new(DefaultCodeHighlighter::from_env()),
            external_service_discovery: Arc::new(UnimplementedDiscovery),
            mirror_checker: Arc::new(UnconfiguredMirrorChecker),
            repo_updater: Arc::new(UnconfiguredRepoUpdater),
            test_email: Arc::new(UnconfiguredTestEmailSender),
            observability: Arc::new(NoopObservabilityAlerts),
            site_reloader: Arc::new(NoopSiteReloader),
            phabricator_client: None,
            phabricator_git: Arc::new(UnconfiguredPhabricatorGit),
            password_reset_url: Arc::new(UnconfiguredPasswordResetUrl),
            password_reset_email: Arc::new(UnconfiguredPasswordResetEmail),
            reset_password_enabled: false,
            dotcom: false,
            can_send_email: false,
            can_reload_site: false,
            external_url: String::new(),
            email_verification_required: false,
        }
    }
}

/// `conf.ExternalURLParsed()` (`internal/conf/computed.go:444-486`): an empty
/// configured URL is `http://example.com`.
pub fn external_url_parsed(raw: &str) -> String {
    if raw.is_empty() {
        "http://example.com".to_owned()
    } else {
        raw.to_owned()
    }
}

// ---------------------------------------------------------------------------
// Key helpers (shared by prefetch and replay)
// ---------------------------------------------------------------------------

pub fn webhooks_key(arguments: &Arguments) -> String {
    format!(
        "{:?}:{:?}:{:?}",
        arguments.i64("first"),
        arguments.string("after"),
        arguments.string("kind")
    )
}

pub fn external_namespaces_key(arguments: &Arguments) -> String {
    format!(
        "{:?}:{:?}:{:?}:{:?}",
        arguments.string("id"),
        arguments.string("kind"),
        arguments.string("url"),
        arguments.string("token")
    )
}

pub fn external_repositories_key(arguments: &Arguments) -> String {
    let exclude = match arguments.get("excludeRepos") {
        Some(InputValue::List(items)) => items
            .iter()
            .filter_map(InputValue::as_str)
            .collect::<Vec<_>>()
            .join(","),
        _ => String::new(),
    };
    format!(
        "{:?}:{:?}:{:?}:{:?}:{:?}:{:?}",
        arguments.string("id"),
        arguments.string("kind"),
        arguments.string("url"),
        arguments.string("token"),
        arguments.string("query"),
        exclude
    )
}

pub fn markdown_key(arguments: &Arguments) -> String {
    arguments.string("markdown").unwrap_or_default().to_owned()
}

pub fn highlight_key(arguments: &Arguments) -> String {
    format!(
        "{:?}:{:?}:{:?}",
        arguments.string("code"),
        arguments.string("fuzzyLanguage"),
        arguments.bool("disableTimeout")
    )
}

// ---------------------------------------------------------------------------
// Mutation outcomes
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InventoryCOutcome {
    /// `EmptyResponse`.
    Empty,
    /// `User!` (`changeCodyPlan`).
    User(UserData),
    /// `Webhook!` (`createWebhook` / `updateWebhook`).
    Webhook(WebhookData),
    /// `PackageFilter!` (`addPackageRepoFilter`).
    PackageFilter(crate::package_repo_filters::PackageRepoFilterData),
    /// `CheckMirrorRepositoryConnectionResult!`.
    CheckMirror {
        error: Option<String>,
    },
    /// `RandomizeUserPasswordResult!`.
    RandomizePassword {
        reset_url: Option<String>,
        email_sent: bool,
    },
    /// `GitCommit` returned by `resolvePhabricatorDiff`.
    PhabricatorCommit {
        oid: String,
    },
    /// Plain string (`String!` from `sendTestEmail`).
    Text(String),
    /// A resolver that succeeded while returning a nil pointer: the baseline
    /// `AddPhabricatorRepo` returns `(nil, nil)`, so the nullable GraphQL field
    /// is `null`.
    Null,
    Error(String),
}

// ---------------------------------------------------------------------------
// Object types
// ---------------------------------------------------------------------------

pub struct WebhookObject {
    pub data: WebhookData,
    pub viewer_site_admin: bool,
    pub nested: Arc<crate::graphql_resolvers::NestedPrefetch>,
}

fn webhook_user_field(
    lookup: &UserByIdLookupOrMissing,
    viewer_site_admin: bool,
    nested: &Arc<crate::graphql_resolvers::NestedPrefetch>,
) -> Result<FieldValue, ExecError> {
    match lookup {
        UserByIdLookupOrMissing::Missing => Ok(FieldValue::Null),
        UserByIdLookupOrMissing::Found(user) => Ok(FieldValue::Object(Arc::new(
            crate::graphql_resolvers::UserObject::from_data_as_viewer(
                user.clone(),
                viewer_site_admin,
            )
            .with_nested(nested.clone()),
        ))),
        UserByIdLookupOrMissing::Error(message) => Err(ExecError::new(message.clone())),
    }
}

impl ObjectValue for WebhookObject {
    fn type_name(&self) -> &'static str {
        "Webhook"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "id" => Ok(Some(FieldValue::Json(Json::Str(marshal_relay_int_id(
                "Webhook",
                i64::from(self.data.id),
            ))))),
            "uuid" => Ok(Some(FieldValue::Json(Json::Str(self.data.uuid.clone())))),
            "url" => Ok(Some(FieldValue::Json(Json::Str(self.data.url.clone())))),
            "name" => Ok(Some(FieldValue::Json(Json::Str(self.data.name.clone())))),
            "codeHostKind" => Ok(Some(FieldValue::Json(Json::Str(
                self.data.code_host_kind.clone(),
            )))),
            "codeHostURN" => Ok(Some(FieldValue::Json(Json::Str(
                self.data.code_host_urn.clone(),
            )))),
            "secret" => Ok(Some(match &self.data.secret {
                Some(secret) => FieldValue::Json(Json::Str(secret.clone())),
                None => FieldValue::Null,
            })),
            "createdAt" => Ok(Some(FieldValue::Json(Json::Str(datetime_rfc3339(
                self.data.created_at,
            ))))),
            "updatedAt" => Ok(Some(FieldValue::Json(Json::Str(datetime_rfc3339(
                self.data.updated_at,
            ))))),
            "createdBy" => Ok(Some(webhook_user_field(
                &self.data.created_by,
                self.viewer_site_admin,
                &self.nested,
            )?)),
            "updatedBy" => Ok(Some(webhook_user_field(
                &self.data.updated_by,
                self.viewer_site_admin,
                &self.nested,
            )?)),
            // `Webhook.webhookLogs` needs a per-webhook asynchronous prefetch;
            // it remains unported in this slice (resolves as null).
            _ => Ok(None),
        }
    }
}

pub struct WebhookConnectionObject {
    pub data: WebhookConnectionData,
    pub viewer_site_admin: bool,
    pub nested: Arc<crate::graphql_resolvers::NestedPrefetch>,
}

impl ObjectValue for WebhookConnectionObject {
    fn type_name(&self) -> &'static str {
        "WebhookConnection"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "nodes" => Ok(Some(FieldValue::List {
                element_type: Some("Webhook"),
                items: self
                    .data
                    .nodes
                    .iter()
                    .cloned()
                    .map(|data| {
                        FieldValue::Object(Arc::new(WebhookObject {
                            data,
                            viewer_site_admin: self.viewer_site_admin,
                            nested: self.nested.clone(),
                        }))
                    })
                    .collect(),
            })),
            "totalCount" => Ok(Some(FieldValue::Json(Json::Int(self.data.total_count)))),
            "pageInfo" => Ok(Some(FieldValue::Object(Arc::new(PageInfoObject::new(
                self.data.has_next_page,
                self.data.end_cursor.clone(),
            ))))),
            _ => Ok(None),
        }
    }
}

pub struct UserEmailObject {
    pub data: UserEmailData,
    pub user: crate::graphql_resolvers::UserObject,
    pub viewer_site_admin: bool,
}

impl ObjectValue for UserEmailObject {
    fn type_name(&self) -> &'static str {
        "UserEmail"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "email" => Ok(Some(FieldValue::Json(Json::Str(self.data.email.clone())))),
            "isPrimary" => Ok(Some(FieldValue::Json(Json::Bool(self.data.is_primary)))),
            "verified" => Ok(Some(FieldValue::Json(Json::Bool(self.data.verified)))),
            "verificationPending" => Ok(Some(FieldValue::Json(Json::Bool(
                self.data.verification_pending,
            )))),
            "user" => Ok(Some(FieldValue::Object(Arc::new(self.user.clone())))),
            "viewerCanManuallyVerify" => Ok(Some(FieldValue::Json(Json::Bool(
                check_current_user_is_site_admin(actor, self.viewer_site_admin).is_ok(),
            )))),
            _ => Ok(None),
        }
    }
}

pub struct SessionObject {
    pub can_sign_out: bool,
}

impl ObjectValue for SessionObject {
    fn type_name(&self) -> &'static str {
        "Session"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "canSignOut" => Ok(Some(FieldValue::Json(Json::Bool(self.can_sign_out)))),
            _ => Ok(None),
        }
    }
}

pub struct AccessTokenObject {
    pub data: AccessTokenData,
    pub viewer_site_admin: bool,
    pub nested: Arc<crate::graphql_resolvers::NestedPrefetch>,
}

fn token_user_field(
    lookup: &Result<UserData, String>,
    viewer_site_admin: bool,
    nested: &Arc<crate::graphql_resolvers::NestedPrefetch>,
) -> Result<FieldValue, ExecError> {
    match lookup {
        Ok(user) => Ok(FieldValue::Object(Arc::new(
            crate::graphql_resolvers::UserObject::from_data_as_viewer(
                user.clone(),
                viewer_site_admin,
            )
            .with_nested(nested.clone()),
        ))),
        Err(message) => Err(ExecError::new(message.clone())),
    }
}

impl ObjectValue for AccessTokenObject {
    fn type_name(&self) -> &'static str {
        "AccessToken"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "id" => Ok(Some(FieldValue::Json(Json::Str(marshal_relay_int_id(
                "AccessToken",
                self.data.id,
            ))))),
            "subject" => Ok(Some(token_user_field(
                &self.data.subject,
                self.viewer_site_admin,
                &self.nested,
            )?)),
            "scopes" => Ok(Some(FieldValue::List {
                element_type: Some("String"),
                items: self
                    .data
                    .scopes
                    .iter()
                    .cloned()
                    .map(|scope| FieldValue::Json(Json::Str(scope)))
                    .collect(),
            })),
            "note" => Ok(Some(FieldValue::Json(Json::Str(self.data.note.clone())))),
            "creator" => Ok(Some(token_user_field(
                &self.data.creator,
                self.viewer_site_admin,
                &self.nested,
            )?)),
            "createdAt" => Ok(Some(FieldValue::Json(Json::Str(datetime_rfc3339(
                self.data.created_at,
            ))))),
            "lastUsedAt" => Ok(Some(match self.data.last_used_at {
                Some(t) => FieldValue::Json(Json::Str(datetime_rfc3339(t))),
                None => FieldValue::Null,
            })),
            "expiresAt" => Ok(Some(match self.data.expires_at {
                Some(t) => FieldValue::Json(Json::Str(datetime_rfc3339(t))),
                None => FieldValue::Null,
            })),
            _ => Ok(None),
        }
    }
}

pub struct AccessTokenConnectionObject {
    pub full: AccessTokenConnectionFull,
    pub first: Option<i32>,
    pub viewer_site_admin: bool,
    pub nested: Arc<crate::graphql_resolvers::NestedPrefetch>,
}

impl AccessTokenConnectionObject {
    fn limited(&self) -> &[AccessTokenData] {
        match self.first {
            Some(first) if first >= 0 && self.full.all.len() > first as usize => {
                &self.full.all[..first as usize]
            }
            _ => &self.full.all,
        }
    }
}

impl ObjectValue for AccessTokenConnectionObject {
    fn type_name(&self) -> &'static str {
        "AccessTokenConnection"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "nodes" => Ok(Some(FieldValue::List {
                element_type: Some("AccessToken"),
                items: self
                    .limited()
                    .iter()
                    .cloned()
                    .map(|data| {
                        FieldValue::Object(Arc::new(AccessTokenObject {
                            data,
                            viewer_site_admin: self.viewer_site_admin,
                            nested: self.nested.clone(),
                        }))
                    })
                    .collect(),
            })),
            "totalCount" => Ok(Some(FieldValue::Json(Json::Int(self.full.total)))),
            "pageInfo" => {
                let has_next = self
                    .first
                    .map(|first| first >= 0 && self.full.all.len() > first as usize)
                    .unwrap_or(false);
                Ok(Some(FieldValue::Object(Arc::new(PageInfoObject::new(
                    has_next, None,
                )))))
            }
            _ => Ok(None),
        }
    }
}

pub struct ExternalServiceNamespaceConnectionObject {
    pub nodes: Vec<ExternalNamespaceData>,
}

impl ObjectValue for ExternalServiceNamespaceConnectionObject {
    fn type_name(&self) -> &'static str {
        "ExternalServiceNamespaceConnection"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "nodes" => Ok(Some(FieldValue::List {
                element_type: Some("ExternalServiceNamespace"),
                items: self
                    .nodes
                    .iter()
                    .cloned()
                    .map(|node| {
                        FieldValue::Object(Arc::new(ExternalServiceNamespaceObject { node }))
                    })
                    .collect(),
            })),
            "totalCount" => Ok(Some(FieldValue::Json(Json::Int(self.nodes.len() as i64)))),
            _ => Ok(None),
        }
    }
}

pub struct ExternalServiceNamespaceObject {
    pub node: ExternalNamespaceData,
}

impl ObjectValue for ExternalServiceNamespaceObject {
    fn type_name(&self) -> &'static str {
        "ExternalServiceNamespace"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "id" => {
                // `relay.MarshalID` marshals the struct with `encoding/json`,
                // which emits the fields in declaration order.
                let spec =
                    namespace_spec_json(self.node.id, &self.node.name, &self.node.external_id);
                Ok(Some(FieldValue::Json(Json::Str(marshal_relay_json(
                    "ExternalServiceNamespace",
                    &spec,
                )))))
            }
            "name" => Ok(Some(FieldValue::Json(Json::Str(self.node.name.clone())))),
            "externalID" => Ok(Some(FieldValue::Json(Json::Str(
                self.node.external_id.clone(),
            )))),
            _ => Ok(None),
        }
    }
}

pub struct ExternalServiceRepositoryConnectionObject {
    pub nodes: Vec<ExternalRepositoryData>,
}

impl ObjectValue for ExternalServiceRepositoryConnectionObject {
    fn type_name(&self) -> &'static str {
        "ExternalServiceRepositoryConnection"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "nodes" => Ok(Some(FieldValue::List {
                element_type: Some("ExternalServiceRepository"),
                items: self
                    .nodes
                    .iter()
                    .cloned()
                    .map(|node| {
                        FieldValue::Object(Arc::new(ExternalServiceRepositoryObject { node }))
                    })
                    .collect(),
            })),
            _ => Ok(None),
        }
    }
}

pub struct ExternalServiceRepositoryObject {
    pub node: ExternalRepositoryData,
}

impl ObjectValue for ExternalServiceRepositoryObject {
    fn type_name(&self) -> &'static str {
        "ExternalServiceRepository"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "id" => {
                let spec =
                    namespace_spec_json(self.node.id, &self.node.name, &self.node.external_id);
                Ok(Some(FieldValue::Json(Json::Str(marshal_relay_json(
                    "ExternalServiceRepository",
                    &spec,
                )))))
            }
            "name" => Ok(Some(FieldValue::Json(Json::Str(self.node.name.clone())))),
            "externalID" => Ok(Some(FieldValue::Json(Json::Str(
                self.node.external_id.clone(),
            )))),
            _ => Ok(None),
        }
    }
}

pub struct CheckMirrorRepositoryConnectionResultObject {
    pub error: Option<String>,
}

impl ObjectValue for CheckMirrorRepositoryConnectionResultObject {
    fn type_name(&self) -> &'static str {
        "CheckMirrorRepositoryConnectionResult"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "error" => Ok(Some(match &self.error {
                Some(error) => FieldValue::Json(Json::Str(error.clone())),
                None => FieldValue::Null,
            })),
            _ => Ok(None),
        }
    }
}

pub struct RandomizeUserPasswordResultObject {
    pub reset_url: Option<String>,
    pub email_sent: bool,
}

impl ObjectValue for RandomizeUserPasswordResultObject {
    fn type_name(&self) -> &'static str {
        "RandomizeUserPasswordResult"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "resetPasswordURL" => Ok(Some(match &self.reset_url {
                Some(url) => FieldValue::Json(Json::Str(url.clone())),
                None => FieldValue::Null,
            })),
            "emailSent" => Ok(Some(FieldValue::Json(Json::Bool(self.email_sent)))),
            _ => Ok(None),
        }
    }
}

/// Minimal `GitCommit` exposing the `oid` returned by `resolvePhabricatorDiff`.
pub struct PhabricatorCommitObject {
    pub oid: String,
}

impl ObjectValue for PhabricatorCommitObject {
    fn type_name(&self) -> &'static str {
        "GitCommit"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "oid" => Ok(Some(FieldValue::Json(Json::Str(self.oid.clone())))),
            _ => Ok(None),
        }
    }
}

// ---------------------------------------------------------------------------
// Loading helpers
// ---------------------------------------------------------------------------

/// `UserByIDInt32` for the webhook `createdBy`/`updatedBy` fields: a missing
/// live user is nil, any DB failure is an error.
async fn load_user_lookup(pool: &PgPool, id: i32) -> UserByIdLookupOrMissing {
    if id == 0 {
        return UserByIdLookupOrMissing::Missing;
    }
    match sg_db::users::get_session_user(pool, id).await {
        Ok(Some(record)) => UserByIdLookupOrMissing::Found(crate::user_store::to_user_data(record)),
        Ok(None) => UserByIdLookupOrMissing::Missing,
        Err(error) => UserByIdLookupOrMissing::Error(error.to_string()),
    }
}

/// `UserByIDInt32`: a missing live user is `user not found: [<id>]`.
async fn load_user_result(pool: &PgPool, id: i32) -> Result<UserData, String> {
    match sg_db::users::get_session_user(pool, id).await {
        Ok(Some(record)) => Ok(crate::user_store::to_user_data(record)),
        Ok(None) => Err(format!("user not found: [{id}]")),
        Err(error) => Err(error.to_string()),
    }
}

/// `webhookResolver.URL`: parse `conf.Get().ExternalURL`, then replace the path
/// with `.api/webhooks/<uuid>`.
fn webhook_url(external_url: &str, uuid: &str) -> String {
    if external_url.is_empty() {
        return format!(".api/webhooks/{uuid}");
    }
    match url::Url::parse(external_url) {
        Ok(mut parsed) => {
            parsed.set_path(&format!(".api/webhooks/{uuid}"));
            parsed.to_string()
        }
        Err(_) => format!(".api/webhooks/{uuid}"),
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
struct RelayCursor {
    c: String,
    v: String,
    d: String,
}

fn decode_webhook_cursor(after: &str) -> Result<Option<RelayCursor>, String> {
    let kind = relay_kind_of(after).unwrap_or_default();
    if kind != "WebhookCursor" {
        return Err(format!("cannot unmarshal webhook cursor type: {kind:?}"));
    }
    let decoded = decode_url(after).ok_or_else(|| "illegal base64 data".to_owned())?;
    let text = String::from_utf8_lossy(&decoded);
    let Some((_, spec)) = text.split_once(':') else {
        return Err("invalid graphql.ID".to_owned());
    };
    serde_json::from_str(spec).map_err(|error| error.to_string())
}

fn marshal_webhook_cursor(cursor: &RelayCursor) -> String {
    let spec = serde_json::to_string(cursor).expect("cursor json");
    marshal_relay_json("WebhookCursor", &spec)
}

/// `parseWebhookCursorCond` (`internal/database/webhooks.go:359-384`).
fn parse_webhook_cursor_cond(cursor: Option<&RelayCursor>) -> Result<Option<String>, String> {
    let Some(cursor) = cursor else {
        return Ok(None);
    };
    if cursor.c.is_empty() || cursor.v.is_empty() {
        return Ok(None);
    }
    let operator = match cursor.d.as_str() {
        "next" => ">=",
        "prev" => "<=",
        _ => {
            return Err(format!(
                "missing or invalid cursor direction: {:?}",
                cursor.d
            ));
        }
    };
    if cursor.c != "id" {
        return Err(format!(
            "missing or invalid cursor: {:?} {:?}",
            cursor.c, cursor.v
        ));
    }
    Ok(Some(format!("({}) {} ({})", cursor.c, operator, cursor.v)))
}

fn webhook_row_secret(row: &sqlx::postgres::PgRow) -> Option<String> {
    let raw: Option<String> = row.get("secret");
    let key_id: Option<String> = row.get("encryption_key_id");
    match (key_id.as_deref().unwrap_or(""), raw) {
        ("", Some(raw)) if !raw.is_empty() => Some(raw),
        // Encrypted secrets cannot be decrypted without the keyring; the
        // production wiring stores plaintext (encryption disabled), so this is
        // only reachable for rows written by a different reader.
        _ => None,
    }
}

fn webhook_from_row(
    row: &sqlx::postgres::PgRow,
    external_url: &str,
    created_by: UserByIdLookupOrMissing,
    updated_by: UserByIdLookupOrMissing,
) -> WebhookData {
    let uuid: String = row.get("uuid");
    // `scanWebhook` re-parses the stored URL through
    // `extsvc.NewCodeHostBaseURL` (`internal/database/webhooks.go:427-431`),
    // which lowercases the host and ensures a trailing slash.
    let stored_urn: String = row.get("code_host_urn");
    let code_host_urn = normalize_code_host_url(&stored_urn).unwrap_or(stored_urn);
    WebhookData {
        id: row.get("id"),
        uuid: uuid.clone(),
        url: webhook_url(external_url, &uuid),
        name: row
            .try_get::<Option<String>, _>("name")
            .ok()
            .flatten()
            .unwrap_or_default(),
        code_host_kind: row.get("code_host_kind"),
        code_host_urn,
        secret: webhook_row_secret(row),
        created_at: row.get("created_at"),
        updated_at: row.get("updated_at"),
        created_by,
        updated_by,
    }
}

const WEBHOOK_COLUMNS: &str = "id, uuid::text AS uuid, code_host_kind, code_host_urn, secret, \
     created_at, updated_at, encryption_key_id, created_by_user_id, updated_by_user_id, name";

pub(crate) async fn load_webhook_by_id(
    pool: &PgPool,
    id: i32,
    external_url: &str,
) -> Result<WebhookData, String> {
    let row = sqlx::query(&format!(
        "SELECT {WEBHOOK_COLUMNS} FROM webhooks WHERE id = $1"
    ))
    .bind(id)
    .fetch_optional(pool)
    .await
    .map_err(|error| error.to_string())?;
    let Some(row) = row else {
        return Err(format!("webhook with ID {id} not found"));
    };
    let created_by_id: i32 = row
        .try_get::<Option<i32>, _>("created_by_user_id")
        .ok()
        .flatten()
        .unwrap_or(0);
    let updated_by_id: i32 = row
        .try_get::<Option<i32>, _>("updated_by_user_id")
        .ok()
        .flatten()
        .unwrap_or(0);
    let created_by = load_user_lookup(pool, created_by_id).await;
    let updated_by = load_user_lookup(pool, updated_by_id).await;
    Ok(webhook_from_row(&row, external_url, created_by, updated_by))
}

async fn load_webhooks(
    pool: &PgPool,
    kind: &str,
    cursor: Option<&RelayCursor>,
    first: Option<i32>,
    external_url: &str,
) -> Result<WebhookConnectionData, String> {
    let cond = parse_webhook_cursor_cond(cursor)
        .map_err(|error| format!("parsing webhook cursor: {error}"))?;
    let mut wheres: Vec<String> = Vec::new();
    let mut query = format!("SELECT {WEBHOOK_COLUMNS} FROM webhooks");
    let mut count_query = String::from("SELECT COUNT(*) FROM webhooks");
    if !kind.is_empty() {
        wheres.push(format!("code_host_kind = '{}'", kind.replace('\'', "''")));
    }
    if let Some(cond) = &cond {
        wheres.push(cond.clone());
    }
    if !wheres.is_empty() {
        let where_clause = wheres.join(" AND ");
        query.push_str(&format!("\nWHERE {where_clause}"));
        count_query.push_str(&format!("\nWHERE {where_clause}"));
    }
    let limit = first.map(|first| first + 1).filter(|limit| *limit > 0);
    if let Some(limit) = limit {
        query.push_str(&format!("\nLIMIT {limit}"));
    }
    let rows = sqlx::query(&query)
        .fetch_all(pool)
        .await
        .map_err(|error| format!("error running query: {error}"))?;
    let total: i64 = sqlx::query_scalar(&count_query)
        .fetch_one(pool)
        .await
        .map_err(|error| error.to_string())?;

    let mut webhooks = Vec::with_capacity(rows.len());
    for row in &rows {
        let created_by_id: i32 = row
            .try_get::<Option<i32>, _>("created_by_user_id")
            .ok()
            .flatten()
            .unwrap_or(0);
        let updated_by_id: i32 = row
            .try_get::<Option<i32>, _>("updated_by_user_id")
            .ok()
            .flatten()
            .unwrap_or(0);
        let created_by = load_user_lookup(pool, created_by_id).await;
        let updated_by = load_user_lookup(pool, updated_by_id).await;
        webhooks.push(webhook_from_row(row, external_url, created_by, updated_by));
    }
    let mut has_next_page = false;
    let mut end_cursor = None;
    if let Some(limit) = limit {
        if webhooks.len() == limit as usize {
            if let Some(last) = webhooks.last() {
                let base = cursor.cloned().unwrap_or(RelayCursor {
                    c: "id".to_owned(),
                    v: String::new(),
                    d: "next".to_owned(),
                });
                end_cursor = Some(marshal_webhook_cursor(&RelayCursor {
                    c: base.c,
                    v: last.id.to_string(),
                    d: base.d,
                }));
                has_next_page = true;
                webhooks.pop();
            }
        }
    }
    Ok(WebhookConnectionData {
        nodes: webhooks,
        total_count: total,
        has_next_page,
        end_cursor,
    })
}

/// `NewSourceConfiguration` (`external_services.go:645-660`): only GitHub
/// supports source discovery.
fn new_source_configuration(kind: &str, url: &str, token: &str) -> Result<String, String> {
    if kind != "GITHUB" {
        return Err(UNIMPLEMENTED_DISCOVERY.to_owned());
    }
    // `json.Marshal(schema.GitHubConnection{Url, Token})`: lowercase keys and
    // `token` omitted when empty (`omitempty`).
    let mut config = serde_json::Map::new();
    if !token.is_empty() {
        config.insert(
            "token".to_owned(),
            serde_json::Value::String(token.to_owned()),
        );
    }
    config.insert("url".to_owned(), serde_json::Value::String(url.to_owned()));
    Ok(serde_json::Value::Object(config).to_string())
}

async fn external_service_exists(pool: &PgPool, id: i64) -> Result<(), String> {
    let found: Option<i64> =
        sqlx::query_scalar("SELECT id FROM external_services WHERE id = $1 AND deleted_at IS NULL")
            .bind(id)
            .fetch_optional(pool)
            .await
            .map_err(|error| error.to_string())?;
    match found {
        Some(_) => Ok(()),
        None => Err(format!("external service not found: {id}")),
    }
}

/// `SyntectLanguageMap[strings.ToLower(fuzzyLanguage)]`.
fn syntect_language(language: &str) -> &'static str {
    crate::markdown_graphql::syntect_language(language)
}

// ---------------------------------------------------------------------------
// Query prefetch
// ---------------------------------------------------------------------------

async fn prefetch_external_namespaces(
    pool: &PgPool,
    fields: &[RootField],
    actor: Actor,
    viewer_site_admin: bool,
    deps: &InventoryCDeps,
) -> HashMap<String, Result<Vec<ExternalNamespaceData>, String>> {
    let mut lookups = HashMap::new();
    for field in fields {
        if field.name != "externalServiceNamespaces" {
            continue;
        }
        let key = external_namespaces_key(&field.arguments);
        if lookups.contains_key(&key) {
            continue;
        }
        let gate = check_current_user_is_site_admin(actor, viewer_site_admin);
        if gate.is_err() {
            lookups.insert(key, Err(MUST_BE_SITE_ADMIN.to_owned()));
            continue;
        }
        let kind = field
            .arguments
            .string("kind")
            .unwrap_or_default()
            .to_owned();
        let url = field.arguments.string("url").unwrap_or_default().to_owned();
        let token = field
            .arguments
            .string("token")
            .unwrap_or_default()
            .to_owned();
        let result = async {
            let config = new_source_configuration(&kind, &url, &token)?;
            let external_service_id =
                try_unmarshal_external_service_id(field.arguments.string("id"))?;
            if let Some(id) = external_service_id {
                external_service_exists(pool, id).await?;
            }
            deps.external_service_discovery
                .list_namespaces(external_service_id, &kind, &config)
                .await
        }
        .await;
        lookups.insert(key, result);
    }
    lookups
}

async fn prefetch_external_repositories(
    pool: &PgPool,
    fields: &[RootField],
    actor: Actor,
    viewer_site_admin: bool,
    deps: &InventoryCDeps,
) -> HashMap<String, Result<Vec<ExternalRepositoryData>, String>> {
    let mut lookups = HashMap::new();
    for field in fields {
        if field.name != "externalServiceRepositories" {
            continue;
        }
        let key = external_repositories_key(&field.arguments);
        if lookups.contains_key(&key) {
            continue;
        }
        let gate = check_current_user_is_site_admin(actor, viewer_site_admin);
        if gate.is_err() {
            lookups.insert(key, Err(MUST_BE_SITE_ADMIN.to_owned()));
            continue;
        }
        let kind = field
            .arguments
            .string("kind")
            .unwrap_or_default()
            .to_owned();
        let url = field.arguments.string("url").unwrap_or_default().to_owned();
        let token = field
            .arguments
            .string("token")
            .unwrap_or_default()
            .to_owned();
        let query = field
            .arguments
            .string("query")
            .unwrap_or_default()
            .to_owned();
        // `externalServices.DiscoverRepos` clamps the requested page size to
        // 100 (`external_services.go:224-226`).
        let first = (field.arguments.i64("first").unwrap_or(100) as i32).min(100);
        let exclude: Vec<String> = match field.arguments.get("excludeRepos") {
            Some(InputValue::List(items)) => items
                .iter()
                .filter_map(InputValue::as_str)
                .map(str::to_owned)
                .collect(),
            _ => Vec::new(),
        };
        let result = async {
            let config = new_source_configuration(&kind, &url, &token)?;
            let external_service_id =
                try_unmarshal_external_service_id(field.arguments.string("id"))?;
            if let Some(id) = external_service_id {
                external_service_exists(pool, id).await?;
            }
            deps.external_service_discovery
                .discover_repos(external_service_id, &kind, &config, first, &query, &exclude)
                .await
        }
        .await;
        lookups.insert(key, result);
    }
    lookups
}

fn prefetch_markdown(
    fields: &[RootField],
    deps: &InventoryCDeps,
) -> HashMap<String, Result<String, String>> {
    let mut lookups = HashMap::new();
    for field in fields {
        if field.name != "renderMarkdown" {
            continue;
        }
        let markdown = match field.arguments.string("markdown") {
            Some(markdown) => markdown,
            None => continue,
        };
        let key = markdown.to_owned();
        if lookups.contains_key(&key) {
            continue;
        }
        lookups.insert(key, deps.markdown.render(markdown));
    }
    lookups
}

async fn prefetch_highlight(
    fields: &[RootField],
    deps: &InventoryCDeps,
) -> HashMap<String, Result<String, String>> {
    let mut lookups = HashMap::new();
    for field in fields {
        if field.name != "highlightCode" {
            continue;
        }
        let key = highlight_key(&field.arguments);
        if lookups.contains_key(&key) {
            continue;
        }
        let code = field.arguments.string("code").unwrap_or_default();
        let fuzzy = field
            .arguments
            .string("fuzzyLanguage")
            .unwrap_or_default()
            .to_ascii_lowercase();
        let disable_timeout = field.arguments.bool("disableTimeout").unwrap_or(false);
        let filepath = format!("file.{}", syntect_language(&fuzzy));
        lookups.insert(
            key,
            deps.highlighter
                .highlight(code, &filepath, disable_timeout)
                .await,
        );
    }
    lookups
}

async fn prefetch_webhooks(
    pool: &PgPool,
    fields: &[RootField],
    actor: Actor,
    viewer_site_admin: bool,
    deps: &InventoryCDeps,
) -> HashMap<String, Result<WebhookConnectionData, String>> {
    let mut lookups = HashMap::new();
    for field in fields {
        if field.name != "webhooks" {
            continue;
        }
        let key = webhooks_key(&field.arguments);
        if lookups.contains_key(&key) {
            continue;
        }
        // `webhooksResolver.Webhooks`: any failed site-admin check is reported
        // as `auth.ErrMustBeSiteAdmin`.
        if check_current_user_is_site_admin(actor, viewer_site_admin).is_err() {
            lookups.insert(key, Err(MUST_BE_SITE_ADMIN.to_owned()));
            continue;
        }
        let cursor = match field.arguments.string("after") {
            Some(after) => match decode_webhook_cursor(after) {
                Ok(cursor) => cursor,
                Err(message) => {
                    lookups.insert(key, Err(message));
                    continue;
                }
            },
            None => None,
        };
        let first = field.arguments.i64("first").map(|first| first as i32);
        let kind = field.arguments.string("kind").unwrap_or_default();
        let result = load_webhooks(pool, kind, cursor.as_ref(), first, &deps.external_url).await;
        lookups.insert(key, result);
    }
    lookups
}

// ---------------------------------------------------------------------------
// Per-user account prefetch (`User.emails` / `accessTokens` / `session` / …)
// ---------------------------------------------------------------------------

pub async fn prefetch_user_accounts(
    pool: &PgPool,
    uids: &[i32],
    actor: Actor,
    viewer_site_admin: bool,
    dotcom: bool,
    email_verification_required: bool,
) -> HashMap<i32, UserAccountsData> {
    let mut accounts = HashMap::new();
    for &uid in uids {
        if accounts.contains_key(&uid) {
            continue;
        }
        let can_administer = actor.is_internal() || actor.uid() == Some(uid) || viewer_site_admin;

        let emails = if dotcom && !can_administer {
            Err(MUST_BE_SITE_ADMIN_OR_SAME_USER.to_owned())
        } else {
            load_emails(pool, uid, false, email_verification_required).await
        };

        let primary_email = if dotcom && !can_administer {
            Ok(None)
        } else {
            match load_emails(pool, uid, true, email_verification_required).await {
                Ok(emails) => Ok(emails.into_iter().find(|email| email.is_primary)),
                Err(message) => Err(message),
            }
        };

        let access_tokens = if !can_administer {
            Err(MUST_BE_SITE_ADMIN_OR_SAME_USER.to_owned())
        } else {
            load_access_tokens(pool, uid).await
        };

        let has_verified_email = if !can_administer {
            Err(MUST_BE_SITE_ADMIN_OR_SAME_USER.to_owned())
        } else {
            sqlx::query_scalar::<_, bool>(
                "SELECT EXISTS(SELECT 1 FROM user_emails \
                 WHERE user_id = $1 AND verified_at IS NOT NULL)",
            )
            .bind(uid)
            .fetch_one(pool)
            .await
            .map_err(|error| error.to_string())
        };

        let session_can_sign_out = if actor.uid() != Some(uid) {
            Err(
                "unable to view session for a user other than the currently authenticated user"
                    .to_owned(),
            )
        } else {
            // `canSignOut` requires the actor to come from a session cookie and
            // at least one non-HttpHeader auth provider. The Rust actor carries
            // neither, so it is false (matching the oracle's synthetic actor).
            Ok(false)
        };

        accounts.insert(
            uid,
            UserAccountsData {
                emails,
                primary_email,
                access_tokens,
                has_verified_email,
                session_can_sign_out,
                email_verification_required,
            },
        );
    }
    accounts
}

async fn load_emails(
    pool: &PgPool,
    uid: i32,
    only_verified: bool,
    email_verification_required: bool,
) -> Result<Vec<UserEmailData>, String> {
    let sql = if only_verified {
        "SELECT email, is_primary, verified_at FROM user_emails \
         WHERE user_id = $1 AND verified_at IS NOT NULL \
         ORDER BY created_at ASC, email ASC"
    } else {
        "SELECT email, is_primary, verified_at FROM user_emails \
         WHERE user_id = $1 ORDER BY created_at ASC, email ASC"
    };
    let rows = sqlx::query(sql)
        .bind(uid)
        .fetch_all(pool)
        .await
        .map_err(|error| error.to_string())?;
    Ok(rows
        .into_iter()
        .map(|row| {
            let verified = row
                .get::<Option<OffsetDateTime>, _>("verified_at")
                .is_some();
            UserEmailData {
                email: row.get("email"),
                is_primary: row.get("is_primary"),
                verified,
                verification_pending: !verified && email_verification_required,
            }
        })
        .collect())
}

async fn load_access_tokens(
    pool: &PgPool,
    subject_user_id: i32,
) -> Result<AccessTokenConnectionFull, String> {
    const CONDITIONS: &str = "deleted_at IS NULL \
         AND (expires_at IS NULL OR expires_at > NOW()) \
         AND internal IS FALSE \
         AND subject_user_id = $1";
    let rows = sqlx::query(&format!(
        "SELECT id, subject_user_id, scopes, note, creator_user_id, created_at, \
                last_used_at, expires_at FROM access_tokens \
         WHERE ({CONDITIONS}) \
         ORDER BY now() - created_at < interval '5 minutes' DESC, \
                  last_used_at DESC NULLS FIRST, created_at DESC"
    ))
    .bind(subject_user_id)
    .fetch_all(pool)
    .await
    .map_err(|error| error.to_string())?;
    let total: i64 = sqlx::query_scalar(&format!(
        "SELECT COUNT(*) FROM access_tokens WHERE ({CONDITIONS})"
    ))
    .bind(subject_user_id)
    .fetch_one(pool)
    .await
    .map_err(|error| error.to_string())?;

    let mut users: HashMap<i32, Result<UserData, String>> = HashMap::new();
    let mut tokens = Vec::with_capacity(rows.len());
    for row in rows {
        let subject_id: i32 = row.get("subject_user_id");
        let creator_id: i32 = row.get("creator_user_id");
        for id in [subject_id, creator_id] {
            if let std::collections::hash_map::Entry::Vacant(entry) = users.entry(id) {
                entry.insert(load_user_result(pool, id).await);
            }
        }
        tokens.push(AccessTokenData {
            id: row.get("id"),
            subject: users
                .get(&subject_id)
                .cloned()
                .unwrap_or_else(|| Err(format!("user not found: [{subject_id}]"))),
            scopes: row.get("scopes"),
            note: row.get("note"),
            creator: users
                .get(&creator_id)
                .cloned()
                .unwrap_or_else(|| Err(format!("user not found: [{creator_id}]"))),
            created_at: row.get("created_at"),
            last_used_at: row.get("last_used_at"),
            expires_at: row.get("expires_at"),
        });
    }
    Ok(AccessTokenConnectionFull { all: tokens, total })
}

// ---------------------------------------------------------------------------
// Mutation prefetch
// ---------------------------------------------------------------------------

fn bool_arg(field: &RootField, name: &str) -> bool {
    field.arguments.bool(name).unwrap_or(false)
}

/// `dbutil.NullInt32Column`: a zero actor UID is stored as NULL.
fn null_int32(uid: i32) -> Option<i32> {
    if uid == 0 { None } else { Some(uid) }
}

fn required_string(field: &RootField, name: &str) -> Result<String, InventoryCOutcome> {
    field
        .arguments
        .string(name)
        .map(str::to_owned)
        .ok_or_else(|| InventoryCOutcome::Error(format!("missing {name}")))
}

async fn change_cody_plan(
    pool: &PgPool,
    field: &RootField,
    actor: Actor,
    viewer_site_admin: bool,
    deps: &InventoryCDeps,
) -> InventoryCOutcome {
    if !deps.dotcom {
        return InventoryCOutcome::Error(
            "this feature is only available on sourcegraph.com".to_owned(),
        );
    }
    let user_id = match field.arguments.string("user").map(unmarshal_relay_id) {
        Some(Ok(id)) => id,
        Some(Err(message)) => return InventoryCOutcome::Error(message),
        None => return InventoryCOutcome::Error("invalid graphql.ID".to_owned()),
    };
    if let Err(message) = check_site_admin_or_same_user(actor, viewer_site_admin, user_id) {
        return InventoryCOutcome::Error(message);
    }
    let pro = bool_arg(field, "pro");
    let result = if pro {
        sqlx::query(
            "UPDATE users SET cody_pro_enabled_at = NOW() \
             WHERE id = $1 AND cody_pro_enabled_at IS NULL AND deleted_at IS NULL",
        )
        .bind(user_id)
        .execute(pool)
        .await
    } else {
        sqlx::query(
            "UPDATE users SET cody_pro_enabled_at = NULL \
             WHERE id = $1 AND cody_pro_enabled_at IS NOT NULL AND deleted_at IS NULL",
        )
        .bind(user_id)
        .execute(pool)
        .await
    };
    match result {
        Ok(result) => {
            if result.rows_affected() == 0 {
                match load_user_result(pool, user_id).await {
                    Ok(_) => {}
                    Err(message) => return InventoryCOutcome::Error(message),
                }
            }
        }
        Err(error) => return InventoryCOutcome::Error(error.to_string()),
    }
    match load_user_result(pool, user_id).await {
        Ok(user) => InventoryCOutcome::User(user),
        Err(message) => InventoryCOutcome::Error(message),
    }
}

/// `validateCodeHostKindAndSecret` (`internal/backend/webhooks.go:96-110`).
fn validate_code_host_kind_and_secret(kind: &str, secret: Option<&str>) -> Result<(), String> {
    match kind {
        "GITHUB" | "GITLAB" | "BITBUCKETSERVER" | "BITBUCKETCLOUD" => Ok(()),
        "AZUREDEVOPS" => {
            if secret.is_some() {
                Err(format!(
                    "webhooks do not support secrets for code host kind {kind}"
                ))
            } else {
                Ok(())
            }
        }
        _ => Err(format!(
            "webhooks are not supported for code host kind {kind}"
        )),
    }
}

/// `extsvc.NewCodeHostBaseURL` (`internal/extsvc/types.go:871-878`): parse the
/// URL, lowercase the host and ensure a trailing slash on the path.
fn normalize_code_host_url(raw: &str) -> Result<String, String> {
    let mut parsed = url::Url::parse(raw).map_err(|error| error.to_string())?;
    if let Some(host) = parsed.host_str() {
        let lowered = host.to_ascii_lowercase();
        if lowered != host {
            parsed
                .set_host(Some(&lowered))
                .map_err(|error| error.to_string())?;
        }
    }
    let path = parsed.path().to_owned();
    if !path.ends_with('/') {
        parsed.set_path(&format!("{path}/"));
    }
    Ok(parsed.to_string())
}

async fn create_webhook(
    pool: &PgPool,
    field: &RootField,
    actor: Actor,
    viewer_site_admin: bool,
    deps: &InventoryCDeps,
) -> InventoryCOutcome {
    if check_current_user_is_site_admin(actor, viewer_site_admin).is_err() {
        return InventoryCOutcome::Error(MUST_BE_SITE_ADMIN.to_owned());
    }
    let name = match required_string(field, "name") {
        Ok(name) => name,
        Err(outcome) => return outcome,
    };
    let kind = match required_string(field, "codeHostKind") {
        Ok(kind) => kind,
        Err(outcome) => return outcome,
    };
    let urn = match required_string(field, "codeHostURN") {
        Ok(urn) => urn,
        Err(outcome) => return outcome,
    };
    let secret = field.arguments.string("secret").map(str::to_owned);
    if let Err(message) = validate_code_host_kind_and_secret(&kind, secret.as_deref()) {
        return InventoryCOutcome::Error(message);
    }
    let urn = match normalize_code_host_url(&urn) {
        Ok(urn) => urn,
        Err(message) => return InventoryCOutcome::Error(message),
    };
    // `webhookStore.Create` fails when a provided secret encrypts to the empty
    // string without a key (`internal/database/webhooks.go:80`); this build has
    // no keyring key, so an empty secret hits that branch.
    if matches!(secret.as_deref(), Some("")) {
        return InventoryCOutcome::Error("empty secret and key provided".to_owned());
    }
    let actor_uid = actor.uid().unwrap_or(0);
    let row = sqlx::query(
        "INSERT INTO webhooks \
             (name, code_host_kind, code_host_urn, secret, encryption_key_id, created_by_user_id) \
         VALUES ($1, $2, $3, $4, NULL, $5) RETURNING id",
    )
    .bind(&name)
    .bind(&kind)
    .bind(&urn)
    .bind(secret.as_deref())
    .bind(null_int32(actor_uid))
    .fetch_one(pool)
    .await;
    let id: i32 = match row {
        Ok(row) => row.get("id"),
        Err(error) => return InventoryCOutcome::Error(error.to_string()),
    };
    match load_webhook_by_id(pool, id, &deps.external_url).await {
        Ok(data) => InventoryCOutcome::Webhook(data),
        Err(message) => InventoryCOutcome::Error(message),
    }
}

async fn update_webhook(
    pool: &PgPool,
    field: &RootField,
    actor: Actor,
    viewer_site_admin: bool,
    deps: &InventoryCDeps,
) -> InventoryCOutcome {
    if check_current_user_is_site_admin(actor, viewer_site_admin).is_err() {
        return InventoryCOutcome::Error(MUST_BE_SITE_ADMIN.to_owned());
    }
    let id = match field.arguments.string("id").map(unmarshal_relay_id) {
        Some(Ok(id)) => id,
        Some(Err(message)) => return InventoryCOutcome::Error(message),
        None => return InventoryCOutcome::Error("invalid graphql.ID".to_owned()),
    };
    let current = sqlx::query(&format!(
        "SELECT {WEBHOOK_COLUMNS} FROM webhooks WHERE id = $1"
    ))
    .bind(id)
    .fetch_optional(pool)
    .await;
    let current = match current {
        Ok(Some(row)) => row,
        Ok(None) => {
            return InventoryCOutcome::Error(format!(
                "update webhook: webhook with ID {id} not found"
            ));
        }
        Err(error) => return InventoryCOutcome::Error(format!("update webhook: {error}")),
    };
    let mut name: String = current
        .try_get::<Option<String>, _>("name")
        .ok()
        .flatten()
        .unwrap_or_default();
    let mut kind: String = current.get("code_host_kind");
    let mut urn: String = current.get("code_host_urn");
    let current_secret: Option<String> = current.get("secret");

    if let Some(new_name) = field.arguments.string("name") {
        if !new_name.is_empty() {
            name = new_name.to_owned();
        }
    }
    if let Some(new_kind) = field.arguments.string("codeHostKind") {
        if !new_kind.is_empty() {
            if let Err(message) =
                validate_code_host_kind_and_secret(new_kind, field.arguments.string("secret"))
            {
                return InventoryCOutcome::Error(format!("update webhook: {message}"));
            }
            kind = new_kind.to_owned();
        }
    }
    if let Some(new_urn) = field.arguments.string("codeHostURN") {
        if !new_urn.is_empty() {
            match normalize_code_host_url(new_urn) {
                Ok(normalized) => urn = normalized,
                Err(message) => {
                    return InventoryCOutcome::Error(format!("update webhook: {message}"));
                }
            }
        }
    }
    let secret = match field.arguments.string("secret") {
        Some(new_secret) => {
            let validate_kind = field
                .arguments
                .string("codeHostKind")
                .filter(|kind| !kind.is_empty())
                .unwrap_or(kind.as_str());
            if let Err(message) =
                validate_code_host_kind_and_secret(validate_kind, Some(new_secret))
            {
                return InventoryCOutcome::Error(format!("update webhook: {message}"));
            }
            Some(new_secret.to_owned())
        }
        None => current_secret,
    };
    // `webhookStore.Update` rejects a secret that encrypts to the empty string
    // with no key (`internal/database/webhooks.go:264`).
    if matches!(secret.as_deref(), Some("")) {
        return InventoryCOutcome::Error(
            "update webhook: empty secret and key provided".to_owned(),
        );
    }
    let actor_uid = actor.uid().unwrap_or(0);
    if let Err(error) = sqlx::query(
        "UPDATE webhooks SET name = $1, code_host_urn = $2, code_host_kind = $3, \
             secret = $4, encryption_key_id = NULL, updated_at = NOW(), \
             updated_by_user_id = $5 \
         WHERE id = $6",
    )
    .bind(&name)
    .bind(&urn)
    .bind(&kind)
    .bind(secret.as_deref())
    .bind(null_int32(actor_uid))
    .bind(id)
    .execute(pool)
    .await
    {
        return InventoryCOutcome::Error(format!("update webhook: {error}"));
    }
    match load_webhook_by_id(pool, id, &deps.external_url).await {
        Ok(data) => InventoryCOutcome::Webhook(data),
        Err(message) => InventoryCOutcome::Error(format!("update webhook: {message}")),
    }
}

async fn delete_webhook(
    pool: &PgPool,
    field: &RootField,
    actor: Actor,
    viewer_site_admin: bool,
) -> InventoryCOutcome {
    if check_current_user_is_site_admin(actor, viewer_site_admin).is_err() {
        return InventoryCOutcome::Error(MUST_BE_SITE_ADMIN.to_owned());
    }
    let id = match field.arguments.string("id").map(unmarshal_relay_id) {
        Some(Ok(id)) => id,
        Some(Err(message)) => return InventoryCOutcome::Error(message),
        None => return InventoryCOutcome::Error("invalid graphql.ID".to_owned()),
    };
    match sqlx::query("DELETE FROM webhooks WHERE id = $1")
        .bind(id)
        .execute(pool)
        .await
    {
        Ok(result) if result.rows_affected() == 0 => {
            // `DeleteWebhook` wraps the store's `failed to delete webhook`
            // error again with `delete webhook`.
            InventoryCOutcome::Error(format!(
                "delete webhook: failed to delete webhook: webhook with ID {id} not found"
            ))
        }
        Ok(_) => InventoryCOutcome::Empty,
        Err(error) => InventoryCOutcome::Error(format!("delete webhook: {error}")),
    }
}

async fn repository_name_by_id(pool: &PgPool, id: i32) -> Result<String, String> {
    let row = sqlx::query("SELECT name FROM repo WHERE id = $1 AND deleted_at IS NULL")
        .bind(id)
        .fetch_optional(pool)
        .await
        .map_err(|error| error.to_string())?;
    match row {
        Some(row) => Ok(row.get("name")),
        None => Err(format!("repo not found: id={id}")),
    }
}

async fn check_mirror_repository_connection(
    pool: &PgPool,
    field: &RootField,
    actor: Actor,
    viewer_site_admin: bool,
    deps: &InventoryCDeps,
) -> InventoryCOutcome {
    if let Err(message) = check_current_user_is_site_admin(actor, viewer_site_admin) {
        return InventoryCOutcome::Error(message);
    }
    let repository_id = match field.arguments.string("repository").map(unmarshal_relay_id) {
        Some(Ok(id)) => id,
        Some(Err(message)) => return InventoryCOutcome::Error(message),
        None => return InventoryCOutcome::Error("invalid graphql.ID".to_owned()),
    };
    let name = match repository_name_by_id(pool, repository_id).await {
        Ok(name) => name,
        Err(message) => return InventoryCOutcome::Error(message),
    };
    match deps.mirror_checker.is_repo_cloneable(&name).await {
        Ok(()) => InventoryCOutcome::CheckMirror { error: None },
        Err(message) => InventoryCOutcome::CheckMirror {
            error: Some(message),
        },
    }
}

async fn update_mirror_repository(
    pool: &PgPool,
    field: &RootField,
    actor: Actor,
    viewer_site_admin: bool,
    deps: &InventoryCDeps,
) -> InventoryCOutcome {
    if let Err(message) = check_current_user_is_site_admin(actor, viewer_site_admin) {
        return InventoryCOutcome::Error(message);
    }
    let repository_id = match field.arguments.string("repository").map(unmarshal_relay_id) {
        Some(Ok(id)) => id,
        Some(Err(message)) => return InventoryCOutcome::Error(message),
        None => return InventoryCOutcome::Error("invalid graphql.ID".to_owned()),
    };
    let name = match repository_name_by_id(pool, repository_id).await {
        Ok(name) => name,
        Err(message) => return InventoryCOutcome::Error(message),
    };
    match deps.repo_updater.enqueue_repo_update(&name).await {
        Ok(()) => InventoryCOutcome::Empty,
        Err(message) => InventoryCOutcome::Error(message),
    }
}

async fn randomize_user_password(
    pool: &PgPool,
    field: &RootField,
    actor: Actor,
    viewer_site_admin: bool,
    deps: &InventoryCDeps,
) -> InventoryCOutcome {
    if !deps.reset_password_enabled {
        return InventoryCOutcome::Error("resetting passwords is not enabled".to_owned());
    }
    if deps.dotcom && !deps.can_send_email {
        return InventoryCOutcome::Error(
            "unable to reset password because email sending is not configured".to_owned(),
        );
    }
    if let Err(message) = check_current_user_is_site_admin(actor, viewer_site_admin) {
        return InventoryCOutcome::Error(message);
    }
    let user_id = match field.arguments.string("user").map(unmarshal_relay_id) {
        Some(Ok(id)) => id,
        Some(Err(message)) => {
            return InventoryCOutcome::Error(format!("cannot parse user ID: {message}"));
        }
        None => {
            return InventoryCOutcome::Error("cannot parse user ID: invalid graphql.ID".to_owned());
        }
    };
    let password = uuid::Uuid::new_v4().simple().to_string();
    let hash = match bcrypt::hash(password, bcrypt::DEFAULT_COST) {
        Ok(hash) => hash,
        Err(error) => return InventoryCOutcome::Error(error.to_string()),
    };
    if let Err(error) = sqlx::query(
        "UPDATE users SET passwd_reset_code = NULL, passwd_reset_time = NULL, passwd = $1 \
         WHERE id = $2",
    )
    .bind(hash)
    .bind(user_id)
    .execute(pool)
    .await
    {
        return InventoryCOutcome::Error(error.to_string());
    }
    let reset_url = match deps
        .password_reset_url
        .make_password_reset_url(user_id, "")
        .await
    {
        Ok(url) => url,
        Err(message) => return InventoryCOutcome::Error(message),
    };
    // If email is enabled, the reset URL is also sent to the user's primary
    // email (`users_randomize_password.go:99-112`). A failed send is not a hard
    // error off dotcom.
    let mut email_sent = false;
    let mut email_send_err: Option<String> = None;
    if deps.can_send_email {
        match deps
            .password_reset_email
            .send_reset_password_email(user_id, &reset_url)
            .await
        {
            Ok(()) => email_sent = true,
            Err(message) => email_send_err = Some(message),
        }
    }
    let reset_url = if deps.dotcom {
        // On dotcom the reset URL is never returned; since we cannot offer the
        // URL, an email failure must be surfaced (`users_randomize_password.go:114-123`).
        if let Some(message) = email_send_err {
            return InventoryCOutcome::Error(format!(
                "failed to send password reset URL: {message}"
            ));
        }
        None
    } else {
        Some(resolve_reference(
            &external_url_parsed(&deps.external_url),
            &reset_url,
        ))
    };
    InventoryCOutcome::RandomizePassword {
        reset_url,
        email_sent,
    }
}

fn resolve_reference(base: &str, reference: &str) -> String {
    match url::Url::parse(base) {
        Ok(base) => base
            .join(reference)
            .map(|url| url.to_string())
            .unwrap_or_else(|_| reference.to_owned()),
        Err(_) => reference.to_owned(),
    }
}

async fn set_tos_accepted(
    pool: &PgPool,
    field: &RootField,
    actor: Actor,
    viewer_site_admin: bool,
) -> InventoryCOutcome {
    let affected_user_id = match field.arguments.string("userID") {
        Some(id) => match unmarshal_relay_id(id) {
            Ok(id) => id,
            Err(message) => return InventoryCOutcome::Error(message),
        },
        None => match actor.uid() {
            Some(uid) => uid,
            None => return InventoryCOutcome::Error("no current user".to_owned()),
        },
    };
    if let Err(message) = check_site_admin_or_same_user(actor, viewer_site_admin, affected_user_id)
    {
        return InventoryCOutcome::Error(message);
    }
    match sqlx::query("UPDATE users SET updated_at = now(), tos_accepted = true WHERE id = $1")
        .bind(affected_user_id)
        .execute(pool)
        .await
    {
        Ok(result) if result.rows_affected() == 0 => {
            InventoryCOutcome::Error(format!("user not found: [{affected_user_id}]"))
        }
        Ok(_) => InventoryCOutcome::Empty,
        Err(error) => InventoryCOutcome::Error(error.to_string()),
    }
}

async fn add_phabricator_repo(
    pool: &PgPool,
    field: &RootField,
    actor: Actor,
    viewer_site_admin: bool,
) -> InventoryCOutcome {
    if let Err(message) = check_current_user_is_site_admin(actor, viewer_site_admin) {
        return InventoryCOutcome::Error(message);
    }
    let callsign = match required_string(field, "callsign") {
        Ok(callsign) => callsign,
        Err(outcome) => return outcome,
    };
    let url = match required_string(field, "url") {
        Ok(url) => url,
        Err(outcome) => return outcome,
    };
    let name = field
        .arguments
        .string("name")
        .or_else(|| field.arguments.string("uri"))
        .map(str::to_owned);
    let Some(name) = name else {
        return InventoryCOutcome::Error("missing phabricator repo name".to_owned());
    };
    let existing = sqlx::query("SELECT id FROM phabricator_repos WHERE repo_name = $1")
        .bind(&name)
        .fetch_all(pool)
        .await;
    match existing {
        // `AddPhabricatorRepo` returns `nil, nil` on success, so the nullable
        // `EmptyResponse` field resolves to null.
        Ok(rows) if rows.len() == 1 => return InventoryCOutcome::Null,
        Ok(_) => {}
        Err(error) => return InventoryCOutcome::Error(error.to_string()),
    }
    match sqlx::query(
        "INSERT INTO phabricator_repos(callsign, repo_name, url) VALUES($1, $2, $3) RETURNING id",
    )
    .bind(&callsign)
    .bind(&name)
    .bind(&url)
    .fetch_one(pool)
    .await
    {
        Ok(_) => InventoryCOutcome::Null,
        Err(error) => InventoryCOutcome::Error(error.to_string()),
    }
}

fn phabricator_diff_id(field: &RootField) -> Result<i32, String> {
    if let Some(id) = field.arguments.i64("diffID") {
        return Ok(id as i32);
    }
    match field.arguments.string("diffID") {
        Some(id) => id
            .parse::<i32>()
            .map_err(|error| format!("cannot parse diff ID: {error}")),
        None => Err("missing diffID".to_owned()),
    }
}

async fn phabricator_origin(pool: &PgPool, repo_name: &str) -> Option<String> {
    let rows = sqlx::query("SELECT url FROM phabricator_repos WHERE repo_name = $1")
        .bind(repo_name)
        .fetch_all(pool)
        .await
        .ok()?;
    if rows.len() == 1 {
        Some(rows[0].get("url"))
    } else {
        None
    }
}

async fn resolve_phabricator_diff(
    pool: &PgPool,
    field: &RootField,
    deps: &InventoryCDeps,
) -> InventoryCOutcome {
    let repo_name = match required_string(field, "repoName") {
        Ok(name) => name,
        Err(outcome) => return outcome,
    };
    let repo_exists = sqlx::query("SELECT id FROM repo WHERE name = $1 AND deleted_at IS NULL")
        .bind(&repo_name)
        .fetch_optional(pool)
        .await;
    match repo_exists {
        Ok(Some(_)) => {}
        Ok(None) => return InventoryCOutcome::Error(format!("repo not found: name={repo_name:?}")),
        Err(error) => return InventoryCOutcome::Error(error.to_string()),
    }
    let diff_id = match phabricator_diff_id(field) {
        Ok(id) => id,
        Err(message) => return InventoryCOutcome::Error(message),
    };
    let target_ref = format!("phabricator/diff/{diff_id}");
    match deps
        .phabricator_git
        .resolve_revision(&repo_name, &target_ref)
        .await
    {
        Ok(true) => {
            return match deps
                .phabricator_git
                .commit_oid(&repo_name, &target_ref)
                .await
            {
                Ok(Some(oid)) => InventoryCOutcome::PhabricatorCommit { oid },
                Ok(None) => InventoryCOutcome::Error("commit not found".to_owned()),
                Err(message) => InventoryCOutcome::Error(message),
            };
        }
        Ok(false) => {}
        Err(message) => return InventoryCOutcome::Error(message),
    }
    let Some(origin) = phabricator_origin(pool, &repo_name).await else {
        return InventoryCOutcome::Error(
            "unable to resolve the origin of the phabricator instance".to_owned(),
        );
    };
    let patch = match field.arguments.string("patch") {
        Some(patch) => patch.to_owned(),
        None => match &deps.phabricator_client {
            Some(client) => match client.get_raw_diff(diff_id).await {
                Ok(patch) => patch,
                Err(message) => return InventoryCOutcome::Error(message),
            },
            None => {
                return InventoryCOutcome::Error(format!(
                    "no phabricator was configured for: {origin}"
                ));
            }
        },
    };
    let needs_client_info = field.arguments.string("authorEmail").is_none()
        || field.arguments.string("authorName").is_none()
        || field.arguments.string("date").is_none();
    // The baseline only consults the client when one exists
    // (`repository.go:689`); without a configured instance it falls back to the
    // supplied arguments (and panics if `date` was omitted).
    let info = if deps.phabricator_client.is_some() && needs_client_info {
        let client = deps
            .phabricator_client
            .as_ref()
            .expect("checked is_some above");
        match client.get_diff_info(diff_id).await {
            Ok(info) => info,
            Err(message) => return InventoryCOutcome::Error(message),
        }
    } else {
        let date = match field.arguments.string("date") {
            Some(date) => match date.parse::<i64>() {
                Ok(seconds) => match OffsetDateTime::from_unix_timestamp(seconds) {
                    Ok(date) => date,
                    Err(error) => return InventoryCOutcome::Error(error.to_string()),
                },
                Err(error) => {
                    return InventoryCOutcome::Error(format!(
                        "phabricator: could not parse date: {error}"
                    ));
                }
            },
            None => {
                return InventoryCOutcome::Error(
                    "phabricator: could not parse date: missing date".to_owned(),
                );
            }
        };
        PhabricatorDiffInfo {
            author_name: field
                .arguments
                .string("authorName")
                .unwrap_or_default()
                .to_owned(),
            author_email: field
                .arguments
                .string("authorEmail")
                .unwrap_or_default()
                .to_owned(),
            message: field
                .arguments
                .string("description")
                .unwrap_or_default()
                .to_owned(),
            date,
        }
    };
    let base_rev = field
        .arguments
        .string("baseRev")
        .unwrap_or_default()
        .to_owned();
    let request = PhabricatorPatchRequest {
        repo: repo_name.clone(),
        base_commit: base_rev,
        target_ref: target_ref.clone(),
        patch,
        author_name: info.author_name,
        author_email: info.author_email,
        message: info.message,
        date: info.date,
    };
    if let Err(message) = deps
        .phabricator_git
        .create_commit_from_patch(&request)
        .await
    {
        return InventoryCOutcome::Error(message);
    }
    match deps
        .phabricator_git
        .commit_oid(&repo_name, &target_ref)
        .await
    {
        Ok(Some(oid)) => InventoryCOutcome::PhabricatorCommit { oid },
        Ok(None) => InventoryCOutcome::Error("commit not found".to_owned()),
        Err(message) => InventoryCOutcome::Error(message),
    }
}

async fn set_user_is_site_admin(
    pool: &PgPool,
    field: &RootField,
    actor: Actor,
    viewer_site_admin: bool,
) -> InventoryCOutcome {
    let target_id = match field.arguments.string("userID").map(unmarshal_relay_id) {
        Some(Ok(id)) => id,
        Some(Err(message)) => return InventoryCOutcome::Error(message),
        None => return InventoryCOutcome::Error("invalid graphql.ID".to_owned()),
    };
    let Some(current_uid) = actor.uid() else {
        return InventoryCOutcome::Error(crate::admin_store::NOT_AUTHENTICATED.to_owned());
    };
    if let Err(message) = check_current_user_is_site_admin(actor, viewer_site_admin) {
        return InventoryCOutcome::Error(message);
    }
    let raw_user_id = field.arguments.string("userID").unwrap_or_default();
    if raw_user_id == crate::graphql_resolvers::UserObject::relay_id(current_uid) {
        return InventoryCOutcome::Error(
            "refusing to set current user site admin status".to_owned(),
        );
    }
    let site_admin = bool_arg(field, "siteAdmin");
    if let Err(error) = sqlx::query("UPDATE users SET site_admin = $1 WHERE id = $2")
        .bind(site_admin)
        .bind(target_id)
        .execute(pool)
        .await
    {
        return InventoryCOutcome::Error(error.to_string());
    }
    let role_result = if site_admin {
        sqlx::query(
            "INSERT INTO user_roles(user_id, role_id) \
             SELECT $1, id FROM roles WHERE name = 'SITE_ADMINISTRATOR' \
             ON CONFLICT DO NOTHING",
        )
        .bind(target_id)
        .execute(pool)
        .await
        .map(|_| ())
    } else {
        sqlx::query(
            "DELETE FROM user_roles WHERE user_id = $1 \
             AND role_id = (SELECT id FROM roles WHERE name = 'SITE_ADMINISTRATOR')",
        )
        .bind(target_id)
        .execute(pool)
        .await
        .map(|_| ())
    };
    match role_result {
        Ok(()) => InventoryCOutcome::Empty,
        Err(error) => InventoryCOutcome::Error(error.to_string()),
    }
}

async fn reload_site(
    actor: Actor,
    viewer_site_admin: bool,
    deps: &InventoryCDeps,
) -> InventoryCOutcome {
    if let Err(message) = check_current_user_is_site_admin(actor, viewer_site_admin) {
        return InventoryCOutcome::Error(message);
    }
    if !deps.can_reload_site {
        return InventoryCOutcome::Error("reloading site is not supported".to_owned());
    }
    deps.site_reloader.reload();
    InventoryCOutcome::Empty
}

async fn trigger_observability_test_alert(
    field: &RootField,
    actor: Actor,
    viewer_site_admin: bool,
    deps: &InventoryCDeps,
) -> InventoryCOutcome {
    if let Err(message) = check_current_user_is_site_admin(actor, viewer_site_admin) {
        return InventoryCOutcome::Error(message);
    }
    let level = field.arguments.string("level").unwrap_or_default();
    if level != "warning" && level != "critical" {
        return InventoryCOutcome::Error(format!("invalid alert level {level:?}"));
    }
    deps.observability.trigger(level);
    InventoryCOutcome::Empty
}

async fn send_test_email(
    field: &RootField,
    actor: Actor,
    viewer_site_admin: bool,
    deps: &InventoryCDeps,
) -> InventoryCOutcome {
    if let Err(message) = check_current_user_is_site_admin(actor, viewer_site_admin) {
        return InventoryCOutcome::Error(message);
    }
    let to = match required_string(field, "to") {
        Ok(to) => to,
        Err(outcome) => return outcome,
    };
    let full_id = uuid::Uuid::new_v4().to_string();
    let test_id = full_id.chars().take(5).collect::<String>();
    match deps.test_email.send_test_email(&to, &test_id).await {
        Ok(()) => InventoryCOutcome::Text(format!(
            "Sent test email to {to:?} successfully! Please check it was received - look for test ID: {test_id}"
        )),
        Err(message) => InventoryCOutcome::Text(format!(
            "Failed to send test email: {message}, look for test ID: {test_id}"
        )),
    }
}

async fn sync_external_service(
    pool: &PgPool,
    field: &RootField,
    actor: Actor,
    viewer_site_admin: bool,
) -> InventoryCOutcome {
    if let Err(message) = check_current_user_is_site_admin(actor, viewer_site_admin) {
        return InventoryCOutcome::Error(message);
    }
    let id = match field
        .arguments
        .string("id")
        .map(unmarshal_external_service_id)
    {
        Some(Ok(id)) => id,
        Some(Err(message)) => return InventoryCOutcome::Error(message),
        None => return InventoryCOutcome::Error("invalid graphql.ID".to_owned()),
    };
    match external_service_exists(pool, id).await {
        Ok(()) => {}
        Err(message) => return InventoryCOutcome::Error(message),
    }
    match sqlx::query(
        "WITH es AS ( \
             SELECT id FROM external_services es \
             WHERE id = $1 AND deleted_at IS NULL FOR UPDATE \
         ) \
         INSERT INTO external_service_sync_jobs (external_service_id) \
         SELECT es.id FROM es \
         WHERE NOT EXISTS ( \
             SELECT 1 FROM external_service_sync_jobs j \
             WHERE es.id = j.external_service_id AND j.state IN ('queued', 'processing') \
         )",
    )
    .bind(id)
    .execute(pool)
    .await
    {
        Ok(_) => InventoryCOutcome::Empty,
        Err(error) => InventoryCOutcome::Error(error.to_string()),
    }
}

async fn cancel_external_service_sync(
    pool: &PgPool,
    field: &RootField,
    actor: Actor,
    viewer_site_admin: bool,
) -> InventoryCOutcome {
    if let Err(message) = check_current_user_is_site_admin(actor, viewer_site_admin) {
        return InventoryCOutcome::Error(message);
    }
    let id = match field.arguments.string("id").map(unmarshal_relay_i64) {
        Some(Ok(id)) => id,
        Some(Err(message)) => return InventoryCOutcome::Error(message),
        None => return InventoryCOutcome::Error("invalid graphql.ID".to_owned()),
    };
    match sqlx::query(
        "UPDATE external_service_sync_jobs SET \
             cancel = TRUE, \
             state = CASE WHEN external_service_sync_jobs.state = 'processing' \
                 THEN external_service_sync_jobs.state ELSE 'canceled' END, \
             finished_at = CASE WHEN external_service_sync_jobs.state = 'processing' \
                 THEN external_service_sync_jobs.finished_at ELSE now() END \
         WHERE id = $1 \
             AND state IN ('queued', 'processing') \
             AND cancel IS FALSE",
    )
    .bind(id)
    .execute(pool)
    .await
    {
        Ok(result) if result.rows_affected() != 1 => {
            InventoryCOutcome::Error(format!("sync job with id {id} not found"))
        }
        Ok(_) => InventoryCOutcome::Empty,
        Err(error) => InventoryCOutcome::Error(error.to_string()),
    }
}

fn input_object_get<'a>(value: &'a InputValue, name: &str) -> Option<&'a InputValue> {
    match value {
        InputValue::Object(map) => map.get(name),
        _ => None,
    }
}

fn package_filter_input(
    field: &RootField,
) -> Result<(Option<String>, Option<(String, String)>), String> {
    let Some(filter) = field.arguments.get("filter") else {
        return Ok((None, None));
    };
    let name_filter = input_object_get(filter, "nameFilter")
        .and_then(|value| input_object_get(value, "packageGlob"))
        .and_then(InputValue::as_str)
        .map(str::to_owned);
    let version_filter = match input_object_get(filter, "versionFilter") {
        Some(value) => match (
            input_object_get(value, "packageName").and_then(InputValue::as_str),
            input_object_get(value, "versionGlob").and_then(InputValue::as_str),
        ) {
            (Some(package), Some(version)) => Some((package.to_owned(), version.to_owned())),
            _ => None,
        },
        None => None,
    };
    Ok((name_filter, version_filter))
}

fn validate_package_filter(
    name_filter: &Option<String>,
    version_filter: &Option<(String, String)>,
) -> Result<(), String> {
    if name_filter.is_none() && version_filter.is_none() {
        return Err("must provide either nameFilter or versionFilter".to_owned());
    }
    if name_filter.is_some() && version_filter.is_some() {
        return Err("cannot provide both a name filter and version filter".to_owned());
    }
    Ok(())
}

fn package_matcher_json(
    name_filter: &Option<String>,
    version_filter: &Option<(String, String)>,
) -> String {
    match (name_filter, version_filter) {
        (Some(glob), _) => serde_json::json!({"PackageGlob": glob}).to_string(),
        (None, Some((package, version))) => serde_json::json!({
            "PackageName": package,
            "VersionGlob": version,
        })
        .to_string(),
        (None, None) => "{}".to_owned(),
    }
}

async fn add_package_repo_filter(
    pool: &PgPool,
    field: &RootField,
    actor: Actor,
    viewer_site_admin: bool,
) -> InventoryCOutcome {
    if let Err(message) = check_current_user_is_site_admin(actor, viewer_site_admin) {
        return InventoryCOutcome::Error(message);
    }
    let (name_filter, version_filter) = match package_filter_input(field) {
        Ok(input) => input,
        Err(message) => return InventoryCOutcome::Error(message),
    };
    if let Err(message) = validate_package_filter(&name_filter, &version_filter) {
        return InventoryCOutcome::Error(message);
    }
    let behaviour = field.arguments.string("behaviour").unwrap_or_default();
    let kind = field.arguments.string("kind").unwrap_or_default();
    let scheme = external_service_to_scheme(kind).unwrap_or("");
    let matcher = package_matcher_json(&name_filter, &version_filter);
    match sqlx::query(
        "INSERT INTO package_repo_filters (behaviour, scheme, matcher) \
         VALUES ($1, $2, $3::jsonb) \
         ON CONFLICT (scheme, matcher) DO UPDATE \
             SET deleted_at = NULL, updated_at = now(), behaviour = EXCLUDED.behaviour \
         RETURNING id",
    )
    .bind(behaviour)
    .bind(scheme)
    .bind(&matcher)
    .fetch_one(pool)
    .await
    {
        Ok(row) => {
            InventoryCOutcome::PackageFilter(crate::package_repo_filters::PackageRepoFilterData {
                id: row.get("id"),
                behaviour: behaviour.to_owned(),
                package_scheme: scheme.to_owned(),
                name_filter,
                version_filter,
            })
        }
        Err(error) => InventoryCOutcome::Error(error.to_string()),
    }
}

async fn update_package_repo_filter(
    pool: &PgPool,
    field: &RootField,
    actor: Actor,
    viewer_site_admin: bool,
) -> InventoryCOutcome {
    if let Err(message) = check_current_user_is_site_admin(actor, viewer_site_admin) {
        return InventoryCOutcome::Error(message);
    }
    let (name_filter, version_filter) = match package_filter_input(field) {
        Ok(input) => input,
        Err(message) => return InventoryCOutcome::Error(message),
    };
    if let Err(message) = validate_package_filter(&name_filter, &version_filter) {
        return InventoryCOutcome::Error(message);
    }
    let id = match field.arguments.string("id").map(unmarshal_relay_i64) {
        Some(Ok(id)) => id,
        Some(Err(message)) => return InventoryCOutcome::Error(message),
        None => return InventoryCOutcome::Error("invalid graphql.ID".to_owned()),
    };
    let behaviour = field.arguments.string("behaviour").unwrap_or_default();
    let kind = field.arguments.string("kind").unwrap_or_default();
    let scheme = external_service_to_scheme(kind).unwrap_or("");
    let matcher = package_matcher_json(&name_filter, &version_filter);
    let result = sqlx::query(
        "WITH delete_conflicting_deleted AS ( \
             DELETE FROM package_repo_filters \
             WHERE scheme = $1 AND matcher = $2::jsonb AND deleted_at IS NOT NULL \
             RETURNING $3::integer AS id \
         ), \
         always_id AS ( \
             SELECT id FROM delete_conflicting_deleted \
             UNION SELECT $4::integer AS id \
         ) \
         UPDATE package_repo_filters prv \
         SET behaviour = $5, scheme = $6, matcher = $7::jsonb \
         FROM always_id \
         WHERE prv.id = $8 AND prv.id = always_id.id",
    )
    .bind(scheme)
    .bind(&matcher)
    .bind(id)
    .bind(id)
    .bind(behaviour)
    .bind(scheme)
    .bind(&matcher)
    .bind(id)
    .execute(pool)
    .await;
    match result {
        Ok(result) if result.rows_affected() != 1 => {
            InventoryCOutcome::Error(format!("no package repo filters for ID {id}"))
        }
        Ok(_) => InventoryCOutcome::Empty,
        Err(sqlx::Error::Database(error)) if error.code().as_deref() == Some("23505") => {
            InventoryCOutcome::Error(format!(
                "conflicting package repo filter found for (scheme={scheme},matcher={matcher})"
            ))
        }
        Err(error) => InventoryCOutcome::Error(error.to_string()),
    }
}

async fn delete_package_repo_filter(
    pool: &PgPool,
    field: &RootField,
    actor: Actor,
    viewer_site_admin: bool,
) -> InventoryCOutcome {
    if let Err(message) = check_current_user_is_site_admin(actor, viewer_site_admin) {
        return InventoryCOutcome::Error(message);
    }
    let id = match field.arguments.string("id").map(unmarshal_relay_i64) {
        Some(Ok(id)) => id,
        Some(Err(message)) => return InventoryCOutcome::Error(message),
        None => return InventoryCOutcome::Error("invalid graphql.ID".to_owned()),
    };
    match sqlx::query("UPDATE package_repo_filters SET deleted_at = now() WHERE id = $1")
        .bind(id)
        .execute(pool)
        .await
    {
        Ok(result) if result.rows_affected() != 1 => {
            InventoryCOutcome::Error(format!("no package repo filters for ID {id}"))
        }
        Ok(_) => InventoryCOutcome::Empty,
        Err(error) => InventoryCOutcome::Error(error.to_string()),
    }
}

/// Prefetch the batch-C mutations in document order.
#[allow(clippy::too_many_arguments)]
async fn prefetch_mutations(
    pool: &PgPool,
    fields: &[RootField],
    actor: Actor,
    viewer_site_admin: bool,
    deps: &InventoryCDeps,
) -> Vec<(String, InventoryCOutcome)> {
    let mut outcomes = Vec::new();
    for field in fields {
        let outcome = match field.name.as_str() {
            "changeCodyPlan" => change_cody_plan(pool, field, actor, viewer_site_admin, deps).await,
            "createWebhook" => create_webhook(pool, field, actor, viewer_site_admin, deps).await,
            "updateWebhook" => update_webhook(pool, field, actor, viewer_site_admin, deps).await,
            "deleteWebhook" => delete_webhook(pool, field, actor, viewer_site_admin).await,
            "checkMirrorRepositoryConnection" => {
                check_mirror_repository_connection(pool, field, actor, viewer_site_admin, deps)
                    .await
            }
            "updateMirrorRepository" => {
                update_mirror_repository(pool, field, actor, viewer_site_admin, deps).await
            }
            "randomizeUserPassword" => {
                randomize_user_password(pool, field, actor, viewer_site_admin, deps).await
            }
            "setTosAccepted" => set_tos_accepted(pool, field, actor, viewer_site_admin).await,
            "addPhabricatorRepo" => {
                add_phabricator_repo(pool, field, actor, viewer_site_admin).await
            }
            "resolvePhabricatorDiff" => resolve_phabricator_diff(pool, field, deps).await,
            "setUserIsSiteAdmin" => {
                set_user_is_site_admin(pool, field, actor, viewer_site_admin).await
            }
            "reloadSite" => reload_site(actor, viewer_site_admin, deps).await,
            "triggerObservabilityTestAlert" => {
                trigger_observability_test_alert(field, actor, viewer_site_admin, deps).await
            }
            "sendTestEmail" => send_test_email(field, actor, viewer_site_admin, deps).await,
            "syncExternalService" => {
                sync_external_service(pool, field, actor, viewer_site_admin).await
            }
            "cancelExternalServiceSync" => {
                cancel_external_service_sync(pool, field, actor, viewer_site_admin).await
            }
            "addPackageRepoFilter" => {
                add_package_repo_filter(pool, field, actor, viewer_site_admin).await
            }
            "updatePackageRepoFilter" => {
                update_package_repo_filter(pool, field, actor, viewer_site_admin).await
            }
            "deletePackageRepoFilter" => {
                delete_package_repo_filter(pool, field, actor, viewer_site_admin).await
            }
            _ => continue,
        };
        outcomes.push((field.name.clone(), outcome));
    }
    outcomes
}

// ---------------------------------------------------------------------------
// Request-level aggregation
// ---------------------------------------------------------------------------

/// The M9.18a batch-C roots, prefetched once per GraphQL request.
#[derive(Debug, Clone, Default)]
pub struct InventoryCRoots {
    /// `Query.webhooks`, keyed by [`webhooks_key`].
    pub webhooks: HashMap<String, Result<WebhookConnectionData, String>>,
    /// `Query.externalServiceNamespaces`, keyed by [`external_namespaces_key`].
    pub external_namespaces: HashMap<String, Result<Vec<ExternalNamespaceData>, String>>,
    /// `Query.externalServiceRepositories`, keyed by [`external_repositories_key`].
    pub external_repositories: HashMap<String, Result<Vec<ExternalRepositoryData>, String>>,
    /// `Query.renderMarkdown`, keyed by [`markdown_key`].
    pub markdown: HashMap<String, Result<String, String>>,
    /// `Query.highlightCode`, keyed by [`highlight_key`].
    pub highlight: HashMap<String, Result<String, String>>,
    /// Batch-C mutations keyed by field name.
    pub mutations: HashMap<String, InventoryCOutcome>,
}

/// Prefetch every selected batch-C root.
pub async fn prefetch_inventory_c(
    pool: &PgPool,
    fields: &[RootField],
    actor: Actor,
    viewer_site_admin: bool,
    deps: &InventoryCDeps,
) -> InventoryCRoots {
    let mut mutations = HashMap::new();
    for (field, outcome) in prefetch_mutations(pool, fields, actor, viewer_site_admin, deps).await {
        mutations.insert(field, outcome);
    }
    InventoryCRoots {
        webhooks: prefetch_webhooks(pool, fields, actor, viewer_site_admin, deps).await,
        external_namespaces: prefetch_external_namespaces(
            pool,
            fields,
            actor,
            viewer_site_admin,
            deps,
        )
        .await,
        external_repositories: prefetch_external_repositories(
            pool,
            fields,
            actor,
            viewer_site_admin,
            deps,
        )
        .await,
        markdown: prefetch_markdown(fields, deps),
        highlight: prefetch_highlight(fields, deps).await,
        mutations,
    }
}

/// Whether the operation selects one of the per-user account fields, so the
/// executor knows if [`prefetch_user_accounts`] is worth running.
pub fn selects_user_accounts(fields: &[RootField]) -> bool {
    fn scan(field: &RootField) -> bool {
        matches!(
            field.name.as_str(),
            "emails" | "primaryEmail" | "accessTokens" | "session" | "hasVerifiedEmail"
        ) || field.selections.iter().any(scan)
            || field
                .type_conditions
                .iter()
                .any(|(_, selections)| selections.iter().any(scan))
    }
    fields.iter().any(scan)
}

impl InventoryCRoots {
    /// Replay a prefetched batch-C mutation outcome.
    pub fn mutation_value(
        outcome: &InventoryCOutcome,
        viewer_site_admin: bool,
        nested: &Arc<crate::graphql_resolvers::NestedPrefetch>,
    ) -> Result<Option<FieldValue>, ExecError> {
        match outcome {
            InventoryCOutcome::Empty => Ok(Some(FieldValue::Object(Arc::new(
                crate::temporary_settings::EmptyResponseObject,
            )))),
            InventoryCOutcome::User(data) => Ok(Some(FieldValue::Object(Arc::new(
                crate::graphql_resolvers::UserObject::from_data_as_viewer(
                    data.clone(),
                    viewer_site_admin,
                )
                .with_nested(nested.clone()),
            )))),
            InventoryCOutcome::Webhook(data) => {
                Ok(Some(FieldValue::Object(Arc::new(WebhookObject {
                    data: data.clone(),
                    viewer_site_admin,
                    nested: nested.clone(),
                }))))
            }
            InventoryCOutcome::PackageFilter(filter) => Ok(Some(FieldValue::Object(Arc::new(
                crate::package_repo_filters::PackageRepoFilterObject {
                    filter: filter.clone(),
                },
            )))),
            InventoryCOutcome::CheckMirror { error } => Ok(Some(FieldValue::Object(Arc::new(
                CheckMirrorRepositoryConnectionResultObject {
                    error: error.clone(),
                },
            )))),
            InventoryCOutcome::RandomizePassword {
                reset_url,
                email_sent,
            } => Ok(Some(FieldValue::Object(Arc::new(
                RandomizeUserPasswordResultObject {
                    reset_url: reset_url.clone(),
                    email_sent: *email_sent,
                },
            )))),
            InventoryCOutcome::PhabricatorCommit { oid } => Ok(Some(FieldValue::Object(Arc::new(
                PhabricatorCommitObject { oid: oid.clone() },
            )))),
            InventoryCOutcome::Text(text) => Ok(Some(FieldValue::Json(Json::Str(text.clone())))),
            InventoryCOutcome::Null => Ok(Some(FieldValue::Null)),
            InventoryCOutcome::Error(message) => Err(ExecError::new(message.clone())),
        }
    }
}
