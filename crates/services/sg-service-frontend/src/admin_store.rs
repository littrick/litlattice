#![forbid(unsafe_code)]

//! Database and side-effect seams for the M9.12b admin/account GraphQL writes.
//!
//! The GraphQL executor resolves fields synchronously while the database is
//! async, so every mutation runs in the per-domain `prefetch_*` functions
//! before execution; [`AdminStore`] is the database seam and [`AdminEffects`]
//! the e-mail/audit/perms-sync/config seam. The production implementations
//! ([`PgAdminStore`], [`PgAdminEffects`]) talk to the existing tables only
//! (`users`, `user_emails`, `access_tokens`, `orgs`, `org_members`,
//! `external_services`, `gitserver_repos`, `repo`, `security_event_logs`);
//! the differential harness substitutes in-memory fakes.
//!
//! Error strings are the baseline's (`internal/auth*`,
//! `cmd/frontend/graphqlbackend/*`, `internal/database/*`).

use std::sync::Arc;

use async_trait::async_trait;
use sqlx::types::time::OffsetDateTime;
use sqlx::{PgPool, Row};

use sg_graphql::exec::Actor;

/// `auth.ErrNotAuthenticated`.
pub const NOT_AUTHENTICATED: &str = "not authenticated";
/// `auth.ErrMustBeSiteAdmin`.
pub const MUST_BE_SITE_ADMIN: &str = "must be site admin";
/// `auth.ErrMustBeSiteAdminOrSameUser`.
pub const MUST_BE_SITE_ADMIN_OR_SAME_USER: &str =
    "must be authenticated as the authorized user or site admin";
/// `auth.ErrNotAnOrgMember`.
pub const NOT_AN_ORG_MEMBER: &str = "current user is not an org member";
/// `auth.SourcegraphOperatorProviderType`.
pub const SOURCEGRAPH_OPERATOR_PROVIDER_TYPE: &str = "sourcegraph-operator";

/// `iauth.CheckCurrentUserIsSiteAdmin` collapsed to the GraphQL-visible error.
pub fn check_current_user_is_site_admin(
    actor: Actor,
    viewer_site_admin: bool,
) -> Result<(), String> {
    if actor.is_internal() {
        return Ok(());
    }
    match actor {
        Actor::Anonymous => Err(NOT_AUTHENTICATED.to_owned()),
        Actor::Internal => Ok(()),
        Actor::User { .. } if viewer_site_admin => Ok(()),
        Actor::User { .. } => Err(MUST_BE_SITE_ADMIN.to_owned()),
    }
}

/// `auth.CheckSiteAdminOrSameUserFromActor`.
pub fn check_site_admin_or_same_user(
    actor: Actor,
    viewer_site_admin: bool,
    subject_user_id: i32,
) -> Result<(), String> {
    if actor.is_internal() {
        return Ok(());
    }
    if actor.uid() == Some(subject_user_id) {
        return Ok(());
    }
    match check_current_user_is_site_admin(actor, viewer_site_admin) {
        Ok(()) => Ok(()),
        Err(_) => Err(MUST_BE_SITE_ADMIN_OR_SAME_USER.to_owned()),
    }
}

/// `auth.CheckSameUser`.
pub fn check_same_user(actor: Actor, subject_user_id: i32) -> Result<(), String> {
    if actor.is_internal() || actor.uid() == Some(subject_user_id) {
        return Ok(());
    }
    Err(format!(
        "must be authenticated as user with id {subject_user_id}"
    ))
}

/// `auth.CheckCurrentActorIsSiteAdmin` for an already-loaded user.
pub fn check_actor_site_admin(actor: Actor, viewer_site_admin: bool) -> Result<(), String> {
    if actor.is_internal() {
        return Ok(());
    }
    if actor.uid().is_none() {
        return Err(NOT_AUTHENTICATED.to_owned());
    }
    if viewer_site_admin {
        Ok(())
    } else {
        Err(MUST_BE_SITE_ADMIN.to_owned())
    }
}

/// One `users` row as the mutations need it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UserRow {
    pub id: i32,
    pub username: String,
    pub display_name: String,
    pub avatar_url: String,
    pub site_admin: bool,
    pub tos_accepted: bool,
    pub builtin_auth: bool,
    /// `EXISTS(... service_type='scim' ...)`.
    pub scim_controlled: bool,
    pub created_at: OffsetDateTime,
    pub updated_at: OffsetDateTime,
    /// Only populated by reads that need it (`get_by_verified_email`).
    pub primary_email: String,
}

impl UserRow {
    /// The subset `UserObject` resolves from.
    pub fn user_data(&self) -> crate::graphql_resolvers::UserData {
        crate::graphql_resolvers::UserData {
            id: self.id,
            username: self.username.clone(),
            display_name: self.display_name.clone(),
            avatar_url: self.avatar_url.clone(),
            created_at: self.created_at,
            updated_at: self.updated_at,
            site_admin: self.site_admin,
            tos_accepted: self.tos_accepted,
            builtin_auth: self.builtin_auth,
            scim_controlled: self.scim_controlled,
            primary_email: self.primary_email.clone(),
        }
    }
}

/// `database.NewUser`.
#[derive(Debug, Clone, Default)]
pub struct NewUser {
    pub username: String,
    pub password: String,
    pub email: String,
    pub email_is_verified: bool,
    pub email_verification_code: Option<String>,
}

/// `database.UserUpdate`: `None` leaves the column unchanged; an empty
/// `display_name`/`avatar_url` clears it (the resolver builds nil-if-empty).
#[derive(Debug, Clone, Default)]
pub struct UserUpdate {
    pub username: Option<String>,
    pub display_name: Option<Option<String>>,
    pub avatar_url: Option<Option<String>>,
}

/// One `orgs` row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrgRow {
    pub id: i32,
    pub name: String,
    pub display_name: Option<String>,
    pub created_at: OffsetDateTime,
    pub updated_at: OffsetDateTime,
}

impl Default for UserRow {
    fn default() -> Self {
        Self {
            id: 0,
            username: String::new(),
            display_name: String::new(),
            avatar_url: String::new(),
            site_admin: false,
            tos_accepted: false,
            builtin_auth: false,
            scim_controlled: false,
            created_at: OffsetDateTime::UNIX_EPOCH,
            updated_at: OffsetDateTime::UNIX_EPOCH,
            primary_email: String::new(),
        }
    }
}

impl Default for OrgRow {
    fn default() -> Self {
        Self {
            id: 0,
            name: String::new(),
            display_name: None,
            created_at: OffsetDateTime::UNIX_EPOCH,
            updated_at: OffsetDateTime::UNIX_EPOCH,
        }
    }
}

impl Default for UserEmailRow {
    fn default() -> Self {
        Self {
            user_id: 0,
            email: String::new(),
            is_primary: false,
            verified: false,
            verification_code: None,
            last_verification_sent_at: None,
            created_at: OffsetDateTime::UNIX_EPOCH,
            updated_at: OffsetDateTime::UNIX_EPOCH,
        }
    }
}

impl Default for AccessTokenRow {
    fn default() -> Self {
        Self {
            id: 0,
            subject_user_id: 0,
            note: String::new(),
            scopes: Vec::new(),
            created_at: OffsetDateTime::UNIX_EPOCH,
            last_used_at: None,
            expires_at: None,
            value: String::new(),
        }
    }
}

impl Default for ExternalServiceRow {
    fn default() -> Self {
        Self {
            id: 0,
            kind: String::new(),
            display_name: String::new(),
            config: String::new(),
            unrestricted: false,
            has_webhooks: false,
            code_host_id: None,
            creator_id: None,
            last_updater_id: None,
            created_at: OffsetDateTime::UNIX_EPOCH,
            updated_at: OffsetDateTime::UNIX_EPOCH,
        }
    }
}

impl OrgRow {
    pub fn org_data(&self, viewer_can_administer: bool, viewer_is_member: bool) -> crate::graphql_resolvers::OrgData {
        crate::graphql_resolvers::OrgData {
            id: self.id,
            name: self.name.clone(),
            display_name: self.display_name.clone(),
            created_at: self.created_at,
            viewer_can_administer,
            viewer_is_member,
        }
    }
}

/// One `user_emails` row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UserEmailRow {
    pub user_id: i32,
    pub email: String,
    pub is_primary: bool,
    pub verified: bool,
    pub verification_code: Option<String>,
    pub last_verification_sent_at: Option<OffsetDateTime>,
    pub created_at: OffsetDateTime,
    pub updated_at: OffsetDateTime,
}

/// One `access_tokens` row (only the columns the mutations read).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AccessTokenRow {
    pub id: i64,
    pub subject_user_id: i32,
    pub note: String,
    pub scopes: Vec<String>,
    pub created_at: OffsetDateTime,
    pub last_used_at: Option<OffsetDateTime>,
    pub expires_at: Option<OffsetDateTime>,
    pub value: String,
}

/// The created token (`accesstoken.GenerateToken` + inserted id).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreatedAccessToken {
    pub id: i64,
    pub token: String,
}

/// One `user_external_accounts` row's SOAP identity.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ExternalAccountRef {
    pub service_type: String,
    pub service_id: String,
    pub account_id: String,
}

/// One `external_services` row (decrypted config is handled by the effects).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExternalServiceRow {
    pub id: i64,
    pub kind: String,
    pub display_name: String,
    pub config: String,
    pub unrestricted: bool,
    pub has_webhooks: bool,
    pub code_host_id: Option<i64>,
    pub creator_id: Option<i32>,
    pub last_updater_id: Option<i32>,
    pub created_at: OffsetDateTime,
    pub updated_at: OffsetDateTime,
}

/// `types.ExternalService` for a create.
#[derive(Debug, Clone, Default)]
pub struct NewExternalService {
    pub kind: String,
    pub display_name: String,
    pub config: String,
    pub namespace_user_id: Option<i32>,
    pub namespace_org_id: Option<i32>,
    pub creator_id: Option<i32>,
    pub last_updater_id: Option<i32>,
}

/// One `repo` row (only `id`/`name`).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct RepoRow {
    pub id: i32,
    pub name: String,
}

/// `gitserver_repos.clone_status`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CloneStatus {
    NotCloned,
    Cloning,
    Cloned,
    Failed,
}

impl CloneStatus {
    pub fn from_str(value: &str) -> Self {
        match value {
            "cloning" => CloneStatus::Cloning,
            "cloned" => CloneStatus::Cloned,
            "failed" => CloneStatus::Failed,
            _ => CloneStatus::NotCloned,
        }
    }
}

/// `accessTokensAllow` site configuration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AccessTokensAllowMode {
    All,
    Admin,
    None,
    /// The baseline's zero value (an unset `accessTokensAllow`) falls into the
    /// default branch, which is the same error as `None`.
    Other,
}

/// `accessTokenExpirationOptions` (the offline subset the mutation reads).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AccessTokenExpirationOptions {
    pub allow_no_expiration: bool,
    /// The largest allowed duration in seconds, or `Err` for the baseline's
    /// invalid-option error text.
    pub max_duration_seconds: Result<i64, String>,
}

/// The database seam for every M9.12b write.
#[async_trait]
pub trait AdminStore: Send + Sync {
    // ---- users ----
    async fn get_user_by_id(&self, id: i32) -> Result<Option<UserRow>, String>;
    async fn find_user_by_username(&self, username: &str) -> Result<Option<UserRow>, String>;
    async fn get_users_by_ids(&self, ids: &[i32]) -> Result<Vec<UserRow>, String>;
    async fn get_by_verified_email(&self, email: &str) -> Result<Option<UserRow>, String>;
    async fn create_user(&self, new: &NewUser) -> Result<UserRow, String>;
    /// [`AdminStore::create_user`] with the request actor for the
    /// `AccountCreated` security event (`users.go:264,284,481-517`).
    /// In-memory stores keep using [`AdminStore::create_user`].
    async fn create_user_as(
        &self,
        new: &NewUser,
        _creator_uid: i32,
    ) -> Result<UserRow, String> {
        self.create_user(new).await
    }
    async fn update_user(&self, id: i32, update: &UserUpdate) -> Result<UserRow, String>;
    async fn delete_users(&self, ids: &[i32], hard: bool) -> Result<(), String>;
    /// [`AdminStore::delete_users`] with the request actor for the
    /// `AccountDeleted`/`AccountNuked` argument `{"deleter": UID}`
    /// (`users.go:694,782,787-828`). In-memory stores keep using
    /// [`AdminStore::delete_users`].
    async fn delete_users_as(
        &self,
        ids: &[i32],
        hard: bool,
        _actor_uid: i32,
    ) -> Result<(), String> {
        self.delete_users(ids, hard).await
    }
    async fn revoke_user_permissions(&self, ids: &[i32]) -> Result<(), String>;
    async fn grant_pending_permissions(&self, user_id: i32) -> Result<(), String>;
    async fn external_accounts_for_user(
        &self,
        user_id: i32,
    ) -> Result<Vec<ExternalAccountRef>, String>;

    // ---- user emails ----
    async fn list_user_emails(
        &self,
        user_id: i32,
        only_verified: bool,
    ) -> Result<Vec<UserEmailRow>, String>;
    async fn get_user_email(
        &self,
        user_id: i32,
        email: &str,
    ) -> Result<Option<UserEmailRow>, String>;
    async fn add_user_email(
        &self,
        user_id: i32,
        email: &str,
        code: Option<&str>,
    ) -> Result<(), String>;
    async fn remove_user_email(&self, user_id: i32, email: &str) -> Result<(), String>;
    async fn set_primary_email(&self, user_id: i32, email: &str) -> Result<(), String>;
    async fn set_email_verified(
        &self,
        user_id: i32,
        email: &str,
        verified: bool,
    ) -> Result<(), String>;
    /// `backend.userEmails.SetVerified` (`user_emails.go:223-273`): the email
    /// write and — when `verified` — `GrantPendingPermissions` share one
    /// transaction. The grant failure is logged only and does not fail the
    /// mutation. In-memory stores keep the two calls sequential.
    async fn set_email_verified_and_grant(
        &self,
        user_id: i32,
        email: &str,
        verified: bool,
    ) -> Result<(), String> {
        self.set_email_verified(user_id, email, verified).await?;
        if verified {
            let _ = self.grant_pending_permissions(user_id).await;
        }
        Ok(())
    }

    async fn set_last_verification(
        &self,
        user_id: i32,
        email: &str,
        code: &str,
        at: OffsetDateTime,
    ) -> Result<(), String>;
    async fn latest_verification_sent_email(
        &self,
        email: &str,
    ) -> Result<Option<UserEmailRow>, String>;

    // ---- organizations ----
    async fn get_org_by_id(&self, id: i32) -> Result<Option<OrgRow>, String>;
    async fn create_org(&self, name: &str, display_name: Option<&str>) -> Result<OrgRow, String>;
    async fn update_org(
        &self,
        id: i32,
        display_name: Option<&str>,
    ) -> Result<OrgRow, String>;
    async fn delete_org(&self, id: i32) -> Result<(), String>;
    async fn is_org_member(&self, org_id: i32, user_id: i32) -> Result<bool, String>;
    async fn org_member_count(&self, org_id: i32) -> Result<i64, String>;
    async fn add_org_member(&self, org_id: i32, user_id: i32) -> Result<(), String>;
    async fn remove_org_member(&self, org_id: i32, user_id: i32) -> Result<(), String>;

    // ---- access tokens ----
    async fn create_access_token(
        &self,
        subject_user_id: i32,
        scopes: &[String],
        note: &str,
        creator_user_id: i32,
        expires_at: Option<OffsetDateTime>,
    ) -> Result<CreatedAccessToken, String>;
    async fn get_access_token_by_id(&self, id: i64) -> Result<Option<AccessTokenRow>, String>;
    async fn get_access_token_by_token(
        &self,
        token: &str,
    ) -> Result<Option<AccessTokenRow>, String>;
    async fn delete_access_token_by_id(&self, id: i64) -> Result<(), String>;
    async fn delete_access_token_by_token(&self, token: &str) -> Result<(), String>;

    // ---- external services ----
    async fn get_external_service(&self, id: i64) -> Result<Option<ExternalServiceRow>, String>;
    async fn create_external_service(&self, new: &NewExternalService) -> Result<i64, String>;
    async fn update_external_service(
        &self,
        id: i64,
        display_name: Option<&str>,
        config: Option<&str>,
        updater_id: Option<i32>,
    ) -> Result<(), String>;
    async fn delete_external_service(&self, id: i64, async_delete: bool) -> Result<(), String>;
    async fn exclude_repo_from_external_services(
        &self,
        external_service_ids: &[i64],
        repo_id: i32,
    ) -> Result<(), String>;

    // ---- repositories ----
    async fn get_repo_by_id(&self, id: i32) -> Result<Option<RepoRow>, String>;
    async fn get_gitserver_repo(&self, repo_id: i32) -> Result<Option<CloneStatus>, String>;
    async fn reclone_repository(&self, repo_id: i32, name: &str) -> Result<(), String>;
    async fn delete_repository_from_disk(&self, repo_id: i32, name: &str) -> Result<(), String>;
    async fn reindex_repository(&self, repo_id: i32, name: &str) -> Result<(), String>;
}


/// The e-mail/audit/config side-effect seam.
#[async_trait]
pub trait AdminEffects: Send + Sync {
    /// `conf.CanSendEmail()`.
    fn can_send_email(&self) -> bool;
    /// `conf.EmailVerificationRequired()`.
    fn email_verification_required(&self) -> bool;
    /// `userpasswd.ResetPasswordEnabled()`.
    fn reset_password_enabled(&self) -> bool;
    /// `conf.AccessTokensAllow()`.
    fn access_tokens_allow(&self) -> AccessTokensAllowMode;
    /// `conf.AccessTokensExpirationOptions()`.
    fn access_token_expiration_options(&self) -> AccessTokenExpirationOptions;
    /// `dotcom.SourcegraphDotComMode()`.
    fn dotcom(&self) -> bool;
    /// `externalServicesWritable()` (`EXTSVC_CONFIG_FILE` unset).
    fn external_services_writable(&self) -> bool;
    /// `conf.Get().AuthEnableUsernameChanges`.
    fn auth_enable_username_changes(&self) -> bool;
    /// `featureflag.FromContext(ctx).GetBoolOr("auditlog-expansion", false)`.
    fn auditlog_expansion(&self) -> bool;
    /// `backend.MakeEmailVerificationCode`.
    fn make_email_verification_code(&self) -> Result<String, String>;

    /// `SendUserEmailVerificationEmail` / notification e-mails; kind names the
    /// baseline flow (`verification`, `removed an email`, `changed primary
    /// email`, `access token created`, ...). Errors are logged by the baseline
    /// unless noted in the mutation.
    async fn send_email(&self, user_id: i32, email: &str, kind: &str) -> Result<(), String>;
    /// `Users().DeletePasswordResetCode`.
    async fn delete_password_reset_code(&self, user_id: i32) -> Result<(), String>;
    /// `deleteStalePerforceExternalAccounts`.
    async fn delete_stale_perforce(
        &self,
        user_id: i32,
        email: &str,
    ) -> Result<(), String>;
    /// `permssync.SchedulePermsSync`.
    async fn schedule_perms_sync(&self, user_ids: &[i32], reason: &str) -> Result<(), String>;
    /// `backend.ExternalServicesService.ValidateConnection`; `Ok(())` means
    /// the connection is functional.
    async fn validate_external_service(&self, kind: &str, config: &str) -> Result<(), String>;
    /// `repos.Store.EnqueueSingleSyncJob`; failures are logged by the
    /// baseline and do not fail the mutation.
    async fn enqueue_external_service_sync(&self, id: i64) -> Result<(), String>;
    /// `auth.ResetPasswordURL` (`None` when password resets are disabled).
    async fn reset_password_url(
        &self,
        user: &UserRow,
        email: &str,
        email_verified: bool,
    ) -> Result<Option<String>, String>;
    /// `security_event_logs` insert guarded by `auditlog-expansion`.
    async fn audit(&self, name: &str, user_id: i32, argument: serde_json::Value);
    /// `auditlog.Record` (`AccountModified`) guarded by its own feature flag.
    async fn audit_account_modified(&self, user_id: i32, actor_user_id: i32, fields: &[String]);
}

/// One `Mutation.create*`/`update*`/`delete*` result for [`AdminStore`].
///
/// The variants mirror the GraphQL return types; the domain modules produce
/// them and [`crate::admin_mutations`] attaches them to `MutationRoot`.
#[derive(Clone)]
pub enum AdminMutationOutcome {
    /// `CreateUserResult` (`user`, `resetPasswordURL`).
    CreateUser {
        user: Arc<UserRow>,
        reset_password_url: Option<String>,
    },
    /// `User` from `updateUser`.
    User(Arc<UserRow>),
    /// `Org` from `createOrganization`/`updateOrganization`; the booleans are
    /// the viewer flags (`viewerCanAdminister`, `viewerIsMember`).
    Org {
        org: Arc<OrgRow>,
        viewer_can_administer: bool,
        viewer_is_member: bool,
    },
    /// `CreateAccessTokenResult`.
    AccessToken(CreatedAccessToken),
    /// `ExternalService` returned by add/update, with the optional
    /// `ValidateConnection` warning.
    ExternalService {
        row: Arc<ExternalServiceRow>,
        warning: Option<String>,
    },
    /// A non-nil `EmptyResponse` (or `null` for `deleteUser`).
    Empty,
    /// A nil `*EmptyResponse` (GraphQL `null`), used by `deleteOrganization`.
    Null,
    /// A resolver error surfaced on the mutation field.
    Error(String),
}

/// A production [`AdminStore`] over the existing tables.
#[derive(Clone)]
pub struct PgAdminStore {
    pool: PgPool,
    /// `conf.PermissionsUserMapping().BindID`, already normalized to
    /// `email`/`username` (`userpasswd.rs:141-153`).
    permissions_bind_id: String,
    /// `log.securityEventLog.location` is `database`/`all`
    /// (`internal/database/security_event_logs.go:163-240`). The default is
    /// `auditlog` when unset, so no `security_event_logs` row is written
    /// unless the caller supplies the parsed site configuration.
    security_event_logs_in_database: bool,
    /// Live site configuration (`conf.Get()`); when set, both values above are
    /// derived per call so configuration updates take effect without a
    /// restart.
    live: Option<Arc<sg_config::LiveConfig>>,
}

impl PgAdminStore {
    pub fn new(pool: PgPool) -> Self {
        Self {
            pool,
            permissions_bind_id: "email".to_owned(),
            security_event_logs_in_database: false,
            live: None,
        }
    }

    /// Set the normalized `permissions.userMapping.bindID`.
    pub fn with_permissions_bind_id(mut self, bind_id: impl Into<String>) -> Self {
        let bind_id = bind_id.into();
        self.permissions_bind_id = match bind_id.as_str() {
            "email" | "username" => bind_id,
            _ => "email".to_owned(),
        };
        self.live = None;
        self
    }

    /// Derive the bind id and `security_event_logs` destination from site
    /// configuration (`conf.PermissionsUserMapping`, `log.securityEventLog`).
    pub fn with_site_configuration(mut self, site: &sg_config::SiteConfiguration) -> Self {
        self.permissions_bind_id = crate::userpasswd::permissions_user_mapping_bind_id(site);
        self.security_event_logs_in_database =
            sg_db::security_event_logs::security_event_logs_in_database(site.raw());
        self.live = None;
        self
    }

    /// Read the bind id and `security_event_logs` destination from the live
    /// configuration (`conf.Get()`) on every call.
    pub fn with_live_configuration(mut self, live: Arc<sg_config::LiveConfig>) -> Self {
        self.live = Some(live);
        self
    }

    /// Set whether `security_event_logs` is a destination.
    pub fn with_security_event_logs_in_database(mut self, enabled: bool) -> Self {
        self.security_event_logs_in_database = enabled;
        self.live = None;
        self
    }

    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    fn current_permissions_bind_id(&self) -> String {
        match &self.live {
            Some(live) => {
                crate::userpasswd::permissions_user_mapping_bind_id(&live.current().site)
            }
            None => self.permissions_bind_id.clone(),
        }
    }

    fn current_security_event_logs_in_database(&self) -> bool {
        match &self.live {
            Some(live) => sg_db::security_event_logs::security_event_logs_in_database(
                live.current().site.raw(),
            ),
            None => self.security_event_logs_in_database,
        }
    }

    fn account_event_context(&self, actor_uid: i32) -> sg_db::users::DeletionEventContext<'_> {
        sg_db::users::DeletionEventContext {
            actor: sg_db::users::BackendActor {
                uid: actor_uid,
                sourcegraph_operator: false,
            },
            version: sg_metrics::VERSION,
            now: OffsetDateTime::now_utc(),
            security_event_logs_in_database: self.current_security_event_logs_in_database(),
        }
    }
}

fn row_to_user(row: &sqlx::postgres::PgRow) -> Result<UserRow, String> {
    Ok(UserRow {
        id: row.try_get("id").map_err(|e| e.to_string())?,
        username: row.try_get("username").map_err(|e| e.to_string())?,
        display_name: row
            .try_get::<Option<String>, _>("display_name")
            .map_err(|e| e.to_string())?
            .unwrap_or_default(),
        avatar_url: row
            .try_get::<Option<String>, _>("avatar_url")
            .map_err(|e| e.to_string())?
            .unwrap_or_default(),
        site_admin: row.try_get("site_admin").map_err(|e| e.to_string())?,
        tos_accepted: row.try_get("tos_accepted").map_err(|e| e.to_string())?,
        builtin_auth: row.try_get("builtin_auth").map_err(|e| e.to_string())?,
        scim_controlled: row.try_get("scim_controlled").map_err(|e| e.to_string())?,
        created_at: row.try_get("created_at").map_err(|e| e.to_string())?,
        updated_at: row.try_get("updated_at").map_err(|e| e.to_string())?,
        primary_email: String::new(),
    })
}

fn row_to_email(row: &sqlx::postgres::PgRow) -> Result<UserEmailRow, String> {
    Ok(UserEmailRow {
        user_id: row.try_get("user_id").map_err(|e| e.to_string())?,
        email: row.try_get("email").map_err(|e| e.to_string())?,
        is_primary: row.try_get("is_primary").map_err(|e| e.to_string())?,
        verified: row
            .try_get::<Option<OffsetDateTime>, _>("verified_at")
            .map_err(|e| e.to_string())?
            .is_some(),
        verification_code: row.try_get("verification_code").map_err(|e| e.to_string())?,
        last_verification_sent_at: row
            .try_get("last_verification_sent_at")
            .map_err(|e| e.to_string())?,
        created_at: row.try_get("created_at").map_err(|e| e.to_string())?,
        updated_at: OffsetDateTime::now_utc(),
    })
}

const SCIM_EXPR: &str = "EXISTS (SELECT 1 FROM user_external_accounts uea      WHERE uea.service_type='scim' AND uea.user_id=users.id AND uea.deleted_at IS NULL) AS scim_controlled";
const USER_COLUMNS: &str = "id, username, display_name, avatar_url, site_admin, tos_accepted, (passwd IS NOT NULL) AS builtin_auth, EXISTS (SELECT 1 FROM user_external_accounts uea WHERE uea.service_type='scim' AND uea.user_id = users.id AND uea.deleted_at IS NULL) AS scim_controlled, \
     created_at, updated_at";
const EMAIL_COLUMNS: &str =
    "user_id, email, is_primary, verified_at, verification_code, \
     last_verification_sent_at, created_at";

#[async_trait]
impl AdminStore for PgAdminStore {
    async fn get_user_by_id(&self, id: i32) -> Result<Option<UserRow>, String> {
        let row = sqlx::query(&format!(
            "SELECT {USER_COLUMNS}, {SCIM_EXPR} FROM users WHERE id=$1 AND deleted_at IS NULL"
        ))
        .bind(id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| e.to_string())?;
        row.as_ref().map(row_to_user).transpose()
    }

    async fn find_user_by_username(&self, username: &str) -> Result<Option<UserRow>, String> {
        // `Users().GetByUsername` is case-insensitive and prefers exact case.
        let row = sqlx::query(&format!(
            "SELECT {USER_COLUMNS}, {SCIM_EXPR} FROM users \
             WHERE deleted_at IS NULL AND lower(username)=lower($1) \
             ORDER BY (username=$1) DESC, id ASC LIMIT 1"
        ))
        .bind(username)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| e.to_string())?;
        row.as_ref().map(row_to_user).transpose()
    }

    async fn get_users_by_ids(&self, ids: &[i32]) -> Result<Vec<UserRow>, String> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        let rows = sqlx::query(&format!(
            "SELECT {USER_COLUMNS}, {SCIM_EXPR} FROM users WHERE id = ANY($1) AND deleted_at IS NULL ORDER BY id"
        ))
        .bind(ids)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| e.to_string())?;
        rows.iter().map(row_to_user).collect()
    }

    async fn get_by_verified_email(&self, email: &str) -> Result<Option<UserRow>, String> {
        let row = sqlx::query(&format!(
            "SELECT u.id, u.username, u.display_name, u.avatar_url, u.site_admin, \
             u.tos_accepted, (u.passwd IS NOT NULL) AS builtin_auth, \
             EXISTS (SELECT 1 FROM user_external_accounts uea WHERE uea.service_type='scim' \
             AND uea.user_id=u.id AND uea.deleted_at IS NULL) AS scim_controlled, \
             u.created_at, u.updated_at, ue.email AS primary_email \
             FROM users u JOIN user_emails ue ON ue.user_id=u.id \
             WHERE u.deleted_at IS NULL AND ue.email=$1 AND ue.verified_at IS NOT NULL \
             ORDER BY ue.created_at ASC LIMIT 1"
        ))
        .bind(email)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| e.to_string())?;
        row.as_ref().map(row_to_user).transpose()
    }

    async fn create_user(&self, new: &NewUser) -> Result<UserRow, String> {
        self.create_user_as(new, 0).await
    }

    async fn create_user_as(
        &self,
        new: &NewUser,
        creator_uid: i32,
    ) -> Result<UserRow, String> {
        let mut tx = self.pool.begin().await.map_err(|e| e.to_string())?;
        // `CreateInTransaction` initializes the site in the same transaction
        // (`internal/database/users.go:338-345`) and makes the insert a site
        // admin only when the site was uninitialized and no user row exists
        // yet (`users.go:357-360`). GraphQL `createUser` uses `Users().Create`
        // (`cmd/frontend/graphqlbackend/users_create.go:63`), which has the
        // same semantics.
        let already_initialized = sg_db::global_state::ensure_initialized(&mut *tx)
            .await
            .map_err(|e| e.to_string())?;
        let row = sqlx::query(&format!(
            "INSERT INTO users (username, display_name, avatar_url, site_admin, tos_accepted, \
             created_at, updated_at) \
             VALUES ($1, NULL, NULL, $2 AND NOT EXISTS(SELECT 1 FROM users), FALSE, now(), now()) \
             RETURNING {USER_COLUMNS}, \
             (EXISTS (SELECT 1 FROM user_external_accounts uea WHERE uea.service_type='scim' \
             AND uea.user_id=users.id AND uea.deleted_at IS NULL)) AS scim_controlled"
        ))
        .bind(&new.username)
        .bind(!already_initialized)
        .fetch_one(&mut *tx)
        .await
        .map_err(|e| {
            // `internal/database/users.go:355-366`: username constraint
            // violations become `ErrCannotCreateUser{err_username_exists}`.
            let constraint = e
                .as_database_error()
                .and_then(|db| db.constraint())
                .unwrap_or_default();
            if matches!(
                constraint,
                "users_username"
                    | "users_username_max_length"
                    | "users_username_valid_chars"
                    | "users_display_name_max_length"
            ) {
                "cannot create user: err_username_exists".to_owned()
            } else {
                e.to_string()
            }
        })?;
        let user = row_to_user(&row)?;
        // Reserve the username in the shared users+orgs namespace
        // (`internal/database/users.go:375-378`). The row must carry
        // `user_id` at insert time: `names_check` rejects rows whose
        // `user_id`, `org_id` and `team_id` are all NULL.
        if sqlx::query("INSERT INTO names(name, user_id) VALUES($1, $2)")
            .bind(&new.username)
            .bind(user.id)
            .execute(&mut *tx)
            .await
            .is_err()
        {
            return Err("cannot create user: err_username_exists".to_owned());
        }
        if !new.email.is_empty() {
            sqlx::query(
                "INSERT INTO user_emails (user_id, email, is_primary, verified_at, \
                 verification_code, created_at) \
                 VALUES ($1, $2, TRUE, $3, $4, now())",
            )
            .bind(user.id)
            .bind(&new.email)
            .bind(new.email_is_verified.then(OffsetDateTime::now_utc))
            .bind(&new.email_verification_code)
            .execute(&mut *tx)
            .await
            .map_err(|e| e.to_string())?;
        }
        // `CreateInTransaction` assigns the system roles in the same
        // transaction as the user insert (`internal/database/users.go:432-447`).
        sg_db::users::assign_new_user_system_roles(&mut *tx, user.id, user.site_admin)
            .await
            .map_err(|e| e.to_string())?;
        tx.commit().await.map_err(|e| e.to_string())?;
        // `logAccountCreatedEvent(ctx, NewDBWith(u), newUser, "")`
        // (`internal/database/users.go:261-265`); failures are logged only.
        let ctx = self.account_event_context(creator_uid);
        let _ = sg_db::users::log_account_created_event(
            &self.pool,
            user.id,
            user.site_admin,
            "",
            &ctx,
        )
        .await;
        let mut user = user;
        user.primary_email = new.email.clone();
        Ok(user)
    }

    async fn update_user(&self, id: i32, update: &UserUpdate) -> Result<UserRow, String> {
        let mut tx = self.pool.begin().await.map_err(|e| e.to_string())?;
        if let Some(username) = &update.username {
            // The baseline only touches `names` when the username changes and
            // renames the existing row (`internal/database/users.go:560-567`);
            // inserting a NULL-owner row would violate `names_check`.
            sqlx::query("UPDATE names SET name=$1 WHERE user_id=$2")
                .bind(username)
                .bind(id)
                .execute(&mut *tx)
                .await
                .map_err(|e| {
                    let constraint = e
                        .as_database_error()
                        .and_then(|db| db.constraint())
                        .unwrap_or_default();
                    if constraint == "names_pkey" {
                        "Username is already in use.".to_owned()
                    } else {
                        e.to_string()
                    }
                })?;
        }
        let row = sqlx::query(&format!(
            "UPDATE users SET \
             username=COALESCE($2, username), \
             display_name=CASE WHEN $3 THEN $4 ELSE display_name END, \
             avatar_url=CASE WHEN $5 THEN $6 ELSE avatar_url END, \
             updated_at=now() \
             WHERE id=$1 AND deleted_at IS NULL RETURNING {USER_COLUMNS}, \
             (EXISTS (SELECT 1 FROM user_external_accounts uea WHERE uea.service_type='scim' \
             AND uea.user_id=users.id AND uea.deleted_at IS NULL)) AS scim_controlled"
        ))
        .bind(id)
        .bind(update.username.as_deref())
        .bind(update.display_name.is_some())
        .bind(update.display_name.clone().flatten())
        .bind(update.avatar_url.is_some())
        .bind(update.avatar_url.clone().flatten())
        .fetch_optional(&mut *tx)
        .await
        .map_err(|e| {
            let constraint = e
                .as_database_error()
                .and_then(|db| db.constraint())
                .unwrap_or_default();
            if constraint == "users_username" {
                "cannot create user: err_username_exists".to_owned()
            } else {
                e.to_string()
            }
        })?
        .ok_or_else(|| format!("user not found: [{id}]"))?;
        tx.commit().await.map_err(|e| e.to_string())?;
        row_to_user(&row)
    }

    async fn delete_users(&self, ids: &[i32], hard: bool) -> Result<(), String> {
        self.delete_users_as(ids, hard, 0).await
    }

    async fn delete_users_as(
        &self,
        ids: &[i32],
        hard: bool,
        actor_uid: i32,
    ) -> Result<(), String> {
        if ids.is_empty() {
            return Ok(());
        }
        let ctx = self.account_event_context(actor_uid);
        if hard {
            // Reuse the already-ported hard delete (event-log anonymisation
            // and the `AccountNuked` events included) with the request actor
            // in the event argument.
            return sg_db::users::hard_delete_list(&self.pool, ids, &ctx)
                .await
                .map_err(|e| e.message);
        }
        let mut tx = self.pool.begin().await.map_err(|e| e.to_string())?;
        let deleted = sqlx::query(
            "UPDATE users SET deleted_at=now(), updated_at=now() \
             WHERE id = ANY($1) AND deleted_at IS NULL",
        )
        .bind(ids)
        .execute(&mut *tx)
        .await
        .map_err(|e| e.to_string())?;
        if (deleted.rows_affected() as usize) < ids.len() {
            let _ = tx.rollback().await;
            return Err(format!(
                "Some users were not found. Expected to delete {} users, but deleted only {}",
                ids.len(),
                deleted.rows_affected()
            ));
        }
        sqlx::query("DELETE FROM names WHERE user_id = ANY($1)")
            .bind(ids)
            .execute(&mut *tx)
            .await
            .map_err(|e| e.to_string())?;
        sqlx::query("UPDATE access_tokens SET deleted_at=now() WHERE subject_user_id = ANY($1) AND deleted_at IS NULL")
            .bind(ids)
            .execute(&mut *tx)
            .await
            .map_err(|e| e.to_string())?;
        sqlx::query("DELETE FROM user_emails WHERE user_id = ANY($1)")
            .bind(ids)
            .execute(&mut *tx)
            .await
            .map_err(|e| e.to_string())?;
        sqlx::query("DELETE FROM user_external_accounts WHERE user_id = ANY($1)")
            .bind(ids)
            .execute(&mut *tx)
            .await
            .map_err(|e| e.to_string())?;
        sqlx::query("DELETE FROM org_invitations WHERE recipient_user_id = ANY($1) OR sender_user_id = ANY($1)")
            .bind(ids)
            .execute(&mut *tx)
            .await
            .map_err(|e| e.to_string())?;
        sqlx::query("DELETE FROM registry_extensions WHERE publisher_user_id = ANY($1)")
            .bind(ids)
            .execute(&mut *tx)
            .await
            .map_err(|e| e.to_string())?;
        tx.commit().await.map_err(|e| e.to_string())?;
        // `logUserDeletionEvents(..., SecurityEventNameAccountDeleted)`
        // (`internal/database/users.go:694,787-828`); log-only in Go.
        let _ = sg_db::users::log_user_deletion_events(
            &self.pool,
            ids,
            sg_db::users::SECURITY_EVENT_NAME_ACCOUNT_DELETED,
            &ctx,
        )
        .await;
        Ok(())
    }

    async fn revoke_user_permissions(&self, ids: &[i32]) -> Result<(), String> {
        if ids.is_empty() {
            return Ok(());
        }
        let mut tx = self.pool.begin().await.map_err(|e| e.to_string())?;
        sqlx::query("DELETE FROM user_repo_permissions WHERE user_id = ANY($1)")
            .bind(ids)
            .execute(&mut *tx)
            .await
            .map_err(|e| e.to_string())?;
        sqlx::query("DELETE FROM user_permissions WHERE user_id = ANY($1)")
            .bind(ids)
            .execute(&mut *tx)
            .await
            .map_err(|e| e.to_string())?;
        sqlx::query("DELETE FROM sub_repo_permissions WHERE user_id = ANY($1)")
            .bind(ids)
            .execute(&mut *tx)
            .await
            .map_err(|e| e.to_string())?;
        tx.commit().await.map_err(|e| e.to_string())
    }

    async fn grant_pending_permissions(&self, user_id: i32) -> Result<(), String> {
        // `db.Authz().GrantPendingPermissions(ctx, {UserID, Perm: authz.Read,
        // Type: authz.PermRepos})` (`internal/database/authz.go:98`): enumerate
        // the user's live external accounts plus the configured user-mapping
        // identity, then grant each pending set in one transaction.
        let bind_id = self.current_permissions_bind_id();
        sg_db::authz::grant_pending_permissions(
            &self.pool,
            user_id,
            &bind_id,
            OffsetDateTime::now_utc(),
        )
        .await
        .map_err(|error| error.to_string())
    }

    async fn external_accounts_for_user(
        &self,
        user_id: i32,
    ) -> Result<Vec<ExternalAccountRef>, String> {
        let rows = sqlx::query(
            "SELECT service_type, service_id, account_id FROM user_external_accounts \
             WHERE user_id=$1 AND deleted_at IS NULL ORDER BY id",
        )
        .bind(user_id)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| e.to_string())?;
        rows.iter()
            .map(|row| {
                Ok(ExternalAccountRef {
                    service_type: row.try_get("service_type").map_err(|e| e.to_string())?,
                    service_id: row.try_get("service_id").map_err(|e| e.to_string())?,
                    account_id: row.try_get("account_id").map_err(|e| e.to_string())?,
                })
            })
            .collect()
    }

    async fn list_user_emails(
        &self,
        user_id: i32,
        only_verified: bool,
    ) -> Result<Vec<UserEmailRow>, String> {
        let rows = sqlx::query(&format!(
            "SELECT {EMAIL_COLUMNS} FROM user_emails \
             WHERE user_id=$1 AND (NOT $2 OR verified_at IS NOT NULL) \
             ORDER BY created_at ASC, email ASC"
        ))
        .bind(user_id)
        .bind(only_verified)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| e.to_string())?;
        rows.iter().map(row_to_email).collect()
    }

    async fn get_user_email(
        &self,
        user_id: i32,
        email: &str,
    ) -> Result<Option<UserEmailRow>, String> {
        let row = sqlx::query(&format!(
            "SELECT {EMAIL_COLUMNS} FROM user_emails WHERE user_id=$1 AND email=$2"
        ))
        .bind(user_id)
        .bind(email)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| e.to_string())?;
        row.as_ref().map(row_to_email).transpose()
    }

    async fn add_user_email(
        &self,
        user_id: i32,
        email: &str,
        code: Option<&str>,
    ) -> Result<(), String> {
        let result = sqlx::query(
            "INSERT INTO user_emails (user_id, email, is_primary, verified_at, verification_code, \
             created_at) VALUES ($1, $2, FALSE, NULL, $3, now()) \
             ON CONFLICT ON CONSTRAINT user_emails_no_duplicates_per_user DO NOTHING",
        )
        .bind(user_id)
        .bind(email)
        .bind(code)
        .execute(&self.pool)
        .await
        .map_err(|e| e.to_string())?;
        if result.rows_affected() == 0 {
            return Err("email address already registered for the user".to_owned());
        }
        Ok(())
    }

    async fn remove_user_email(&self, user_id: i32, email: &str) -> Result<(), String> {
        let row = sqlx::query(
            "SELECT is_primary FROM user_emails WHERE user_id=$1 AND email=$2 FOR UPDATE",
        )
        .bind(user_id)
        .bind(email)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| format!("fetching email address: {e}"))?
        .ok_or_else(|| "fetching email address: sql: no rows in result set".to_owned())?;
        let is_primary: bool = row.try_get("is_primary").map_err(|e| e.to_string())?;
        if is_primary {
            return Err("can't delete primary email address".to_owned());
        }
        sqlx::query("DELETE FROM user_emails WHERE user_id=$1 AND email=$2")
            .bind(user_id)
            .bind(email)
            .execute(&self.pool)
            .await
            .map_err(|e| e.to_string())?;
        Ok(())
    }

    async fn set_primary_email(&self, user_id: i32, email: &str) -> Result<(), String> {
        let mut tx = self.pool.begin().await.map_err(|e| e.to_string())?;
        let row = sqlx::query(
            "SELECT verified_at FROM user_emails WHERE user_id=$1 AND email=$2 FOR UPDATE",
        )
        .bind(user_id)
        .bind(email)
        .fetch_optional(&mut *tx)
        .await
        .map_err(|e| e.to_string())?
        .ok_or_else(|| "sql: no rows in result set".to_owned())?;
        let verified: Option<OffsetDateTime> =
            row.try_get("verified_at").map_err(|e| e.to_string())?;
        if verified.is_none() {
            let _ = tx.rollback().await;
            return Err("primary email must be verified".to_owned());
        }
        sqlx::query("UPDATE user_emails SET is_primary=FALSE WHERE user_id=$1")
            .bind(user_id)
            .execute(&mut *tx)
            .await
            .map_err(|e| e.to_string())?;
        sqlx::query(
            "UPDATE user_emails SET is_primary=TRUE WHERE user_id=$1 AND email=$2",
        )
        .bind(user_id)
        .bind(email)
        .execute(&mut *tx)
        .await
        .map_err(|e| e.to_string())?;
        tx.commit().await.map_err(|e| e.to_string())
    }

    async fn set_email_verified(
        &self,
        user_id: i32,
        email: &str,
        verified: bool,
    ) -> Result<(), String> {
        let mut tx = self.pool.begin().await.map_err(|e| e.to_string())?;
        let result = if verified {
            sqlx::query(
                "UPDATE user_emails SET verification_code=NULL, verified_at=now(), \
                 is_primary=(NOT EXISTS (SELECT 1 FROM user_emails WHERE user_id=$1 \
                 AND email != $2 AND is_primary=TRUE)) WHERE user_id=$1 AND email=$2",
            )
            .bind(user_id)
            .bind(email)
            .execute(&mut *tx)
            .await
            .map_err(|_| "could not mark email as verified".to_owned())?
        } else {
            sqlx::query(
                "UPDATE user_emails SET verification_code=NULL, verified_at=NULL \
                 WHERE user_id=$1 AND email=$2",
            )
            .bind(user_id)
            .bind(email)
            .execute(&mut *tx)
            .await
            .map_err(|_| "could not mark email as unverified".to_owned())?
        };
        if result.rows_affected() == 0 {
            let _ = tx.rollback().await;
            return Err("user email not found".to_owned());
        }
        if verified {
            // The baseline deletes every unverified row sharing this address and
            // ignores failures.
            let _ = sqlx::query("DELETE FROM user_emails WHERE verified_at IS NULL AND email=$1")
                .bind(email)
                .execute(&mut *tx)
                .await;
        }
        tx.commit().await.map_err(|e| e.to_string())
    }

    async fn set_email_verified_and_grant(
        &self,
        user_id: i32,
        email: &str,
        verified: bool,
    ) -> Result<(), String> {
        // `db.WithTransact`: `tx.UserEmails().SetVerified`, then either the
        // Perforce cleanup or `tx.Authz().GrantPendingPermissions`
        // (`cmd/frontend/internal/backend/user_emails.go:233-260`).
        let mut tx = self.pool.begin().await.map_err(|e| e.to_string())?;
        let result = if verified {
            sqlx::query(
                "UPDATE user_emails SET verification_code=NULL, verified_at=now(), \
                 is_primary=(NOT EXISTS (SELECT 1 FROM user_emails WHERE user_id=$1 \
                 AND email != $2 AND is_primary=TRUE)) WHERE user_id=$1 AND email=$2",
            )
            .bind(user_id)
            .bind(email)
            .execute(&mut *tx)
            .await
            .map_err(|_| "could not mark email as verified".to_owned())?
        } else {
            sqlx::query(
                "UPDATE user_emails SET verification_code=NULL, verified_at=NULL \
                 WHERE user_id=$1 AND email=$2",
            )
            .bind(user_id)
            .bind(email)
            .execute(&mut *tx)
            .await
            .map_err(|_| "could not mark email as unverified".to_owned())?
        };
        if result.rows_affected() == 0 {
            let _ = tx.rollback().await;
            return Err("user email not found".to_owned());
        }

        if !verified {
            // `deleteStalePerforceExternalAccounts`
            // (`user_emails.go:473-489`): soft-delete the Perforce accounts
            // keyed by this email, then drop the user's sub-repo permissions.
            sqlx::query(
                "UPDATE user_external_accounts SET deleted_at=now() \
                 WHERE user_id=$1 AND account_id=$2 AND service_type='perforce' \
                 AND deleted_at IS NULL",
            )
            .bind(user_id)
            .bind(email)
            .execute(&mut *tx)
            .await
            .map_err(|_| {
                "removing stale perforce external account: deleting stale external account"
                    .to_owned()
            })?;
            sqlx::query("DELETE FROM sub_repo_permissions WHERE user_id=$1")
                .bind(user_id)
                .execute(&mut *tx)
                .await
                .map_err(|_| {
                    "removing stale perforce external account: deleting sub-repo permissions"
                        .to_owned()
                })?;
        } else {
            // The baseline deletes every unverified row sharing this address
            // and ignores failures.
            let _ = sqlx::query("DELETE FROM user_emails WHERE verified_at IS NULL AND email=$1")
                .bind(email)
                .execute(&mut *tx)
                .await;
            // `tx.Authz().GrantPendingPermissions`: failures are logged only,
            // so the email write still commits.
            let bind_id = self.current_permissions_bind_id();
            let _ = sg_db::authz::grant_pending_permissions_in_tx(
                &mut tx,
                user_id,
                &bind_id,
                OffsetDateTime::now_utc(),
            )
            .await;
        }
        tx.commit().await.map_err(|e| e.to_string())
    }

    async fn set_last_verification(
        &self,
        user_id: i32,
        email: &str,
        code: &str,
        at: OffsetDateTime,
    ) -> Result<(), String> {
        let result = sqlx::query(
            "UPDATE user_emails SET verification_code=$3, last_verification_sent_at=$4 \
             WHERE user_id=$1 AND email=$2",
        )
        .bind(user_id)
        .bind(email)
        .bind(code)
        .bind(at)
        .execute(&self.pool)
        .await
        .map_err(|e| e.to_string())?;
        if result.rows_affected() == 0 {
            return Err("user email not found".to_owned());
        }
        Ok(())
    }

    async fn latest_verification_sent_email(
        &self,
        email: &str,
    ) -> Result<Option<UserEmailRow>, String> {
        let row = sqlx::query(&format!(
            "SELECT {EMAIL_COLUMNS} FROM user_emails WHERE email=$1 \
             AND last_verification_sent_at IS NOT NULL \
             ORDER BY last_verification_sent_at DESC LIMIT 1"
        ))
        .bind(email)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| e.to_string())?;
        row.as_ref().map(row_to_email).transpose()
    }

    async fn get_org_by_id(&self, id: i32) -> Result<Option<OrgRow>, String> {
        let row = sqlx::query(
            "SELECT id, name, display_name, created_at, updated_at FROM orgs WHERE id=$1",
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| e.to_string())?;
        row.as_ref().map(org_row).transpose()
    }

    async fn create_org(&self, name: &str, display_name: Option<&str>) -> Result<OrgRow, String> {
        let mut tx = self.pool.begin().await.map_err(|e| e.to_string())?;
        let row = sqlx::query(
            "INSERT INTO orgs (name, display_name, created_at, updated_at) \
             VALUES ($1, $2, now(), now()) \
             RETURNING id, name, display_name, created_at, updated_at",
        )
        .bind(name)
        .bind(display_name)
        .fetch_one(&mut *tx)
        .await
        .map_err(|e| {
            // `internal/database/orgs.go:205-217`: `orgs_name` collisions map
            // to the "already exists" error.
            let constraint = e
                .as_database_error()
                .and_then(|db| db.constraint())
                .unwrap_or_default();
            if constraint == "orgs_name" {
                "organization with name \u{201c}...\u{201d} already exists".to_owned()
            } else {
                e.to_string()
            }
        })?;
        let org = org_row(&row)?;
        // Reserve the organization name in the shared users+orgs+teams
        // namespace (`internal/database/orgs.go:218-221`). The row must carry
        // `org_id` at insert time: `names_check` rejects rows whose `user_id`,
        // `org_id` and `team_id` are all NULL.
        if sqlx::query("INSERT INTO names(name, org_id) VALUES($1, $2)")
            .bind(name)
            .bind(org.id)
            .execute(&mut *tx)
            .await
            .is_err()
        {
            return Err("organization with name \u{201c}...\u{201d} already exists".to_owned());
        }
        tx.commit().await.map_err(|e| e.to_string())?;
        Ok(org)
    }

    async fn update_org(
        &self,
        id: i32,
        display_name: Option<&str>,
    ) -> Result<OrgRow, String> {
        let row = sqlx::query(
            "UPDATE orgs SET display_name=COALESCE($2, display_name), updated_at=now() \
             WHERE id=$1 RETURNING id, name, display_name, created_at, updated_at",
        )
        .bind(id)
        .bind(display_name)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| e.to_string())?
        .ok_or_else(|| format!("org not found: id {id}"))?;
        org_row(&row)
    }

    async fn delete_org(&self, id: i32) -> Result<(), String> {
        let mut tx = self.pool.begin().await.map_err(|e| e.to_string())?;
        let result = sqlx::query(
            "UPDATE orgs SET deleted_at=now(), updated_at=now() WHERE id=$1 AND deleted_at IS NULL",
        )
        .bind(id)
        .execute(&mut *tx)
        .await
        .map_err(|e| e.to_string())?;
        if result.rows_affected() == 0 {
            let _ = tx.rollback().await;
            return Err(format!("org not found: id {id}"));
        }
        sqlx::query("DELETE FROM names WHERE org_id=$1")
            .bind(id)
            .execute(&mut *tx)
            .await
            .map_err(|e| e.to_string())?;
        sqlx::query("UPDATE org_invitations SET deleted_at=now() WHERE org_id=$1 AND deleted_at IS NULL")
            .bind(id)
            .execute(&mut *tx)
            .await
            .map_err(|e| e.to_string())?;
        sqlx::query("UPDATE registry_extensions SET deleted_at=now() WHERE publisher_org_id=$1 AND deleted_at IS NULL")
            .bind(id)
            .execute(&mut *tx)
            .await
            .map_err(|e| e.to_string())?;
        tx.commit().await.map_err(|e| e.to_string())
    }

    async fn is_org_member(&self, org_id: i32, user_id: i32) -> Result<bool, String> {
        let row = sqlx::query(
            "SELECT 1 FROM org_members WHERE org_id=$1 AND user_id=$2",
        )
        .bind(org_id)
        .bind(user_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| e.to_string())?;
        Ok(row.is_some())
    }

    async fn org_member_count(&self, org_id: i32) -> Result<i64, String> {
        let row = sqlx::query(
            "SELECT COUNT(*) AS count FROM org_members om JOIN users u ON u.id=om.user_id \
             WHERE om.org_id=$1 AND u.deleted_at IS NULL",
        )
        .bind(org_id)
        .fetch_one(&self.pool)
        .await
        .map_err(|e| e.to_string())?;
        row.try_get("count").map_err(|e| e.to_string())
    }

    async fn add_org_member(&self, org_id: i32, user_id: i32) -> Result<(), String> {
        let result = sqlx::query(
            "INSERT INTO org_members (org_id, user_id, created_at, updated_at) \
             VALUES ($1, $2, now(), now()) ON CONFLICT DO NOTHING",
        )
        .bind(org_id)
        .bind(user_id)
        .execute(&self.pool)
        .await
        .map_err(|e| e.to_string())?;
        if result.rows_affected() == 0 {
            return Err("user is already a member of the organization".to_owned());
        }
        Ok(())
    }

    async fn remove_org_member(&self, org_id: i32, user_id: i32) -> Result<(), String> {
        sqlx::query("DELETE FROM org_members WHERE org_id=$1 AND user_id=$2")
            .bind(org_id)
            .bind(user_id)
            .execute(&self.pool)
            .await
            .map_err(|e| e.to_string())?;
        Ok(())
    }

    async fn create_access_token(
        &self,
        subject_user_id: i32,
        scopes: &[String],
        note: &str,
        creator_user_id: i32,
        expires_at: Option<OffsetDateTime>,
    ) -> Result<CreatedAccessToken, String> {
        // `accesstoken.GenerateToken`: 20 random bytes hex.
        let token = generate_personal_access_token()?;
        let hash = crate::actor_chain_adapters::token_sha256_hash(&token)
            .map_err(|e| e.to_string())?;
        let row = sqlx::query(
            "INSERT INTO access_tokens (subject_user_id, value_sha256, note, scopes, \
             created_at, last_used_at, expires_at, creator_user_id) \
             VALUES ($1, $2, $3, $4, now(), NULL, $5, $6) RETURNING id",
        )
        .bind(subject_user_id)
        .bind(hash.to_vec())
        .bind(note)
        .bind(scopes)
        .bind(expires_at)
        .bind(creator_user_id)
        .fetch_one(&self.pool)
        .await
        .map_err(|e| e.to_string())?;
        Ok(CreatedAccessToken {
            id: row.try_get("id").map_err(|e| e.to_string())?,
            token,
        })
    }

    async fn get_access_token_by_id(&self, id: i64) -> Result<Option<AccessTokenRow>, String> {
        let row = sqlx::query(
            "SELECT id, subject_user_id, note, scopes, created_at, last_used_at, expires_at \
             FROM access_tokens WHERE id=$1 AND deleted_at IS NULL",
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| e.to_string())?;
        row.as_ref().map(token_row).transpose()
    }

    async fn get_access_token_by_token(
        &self,
        token: &str,
    ) -> Result<Option<AccessTokenRow>, String> {
        let hash = crate::actor_chain_adapters::token_sha256_hash(token)
            .map_err(|e| e.to_string())?;
        let row = sqlx::query(
            "SELECT id, subject_user_id, note, scopes, created_at, last_used_at, expires_at \
             FROM access_tokens WHERE value_sha256=$1 AND deleted_at IS NULL",
        )
        .bind(hash.to_vec())
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| e.to_string())?;
        row.as_ref().map(token_row).transpose()
    }

    async fn delete_access_token_by_id(&self, id: i64) -> Result<(), String> {
        let result = sqlx::query(
            "UPDATE access_tokens SET deleted_at=now() WHERE id=$1 AND deleted_at IS NULL",
        )
        .bind(id)
        .execute(&self.pool)
        .await
        .map_err(|e| e.to_string())?;
        if result.rows_affected() == 0 {
            return Err("access token not found".to_owned());
        }
        Ok(())
    }

    async fn delete_access_token_by_token(&self, token: &str) -> Result<(), String> {
        let hash = crate::actor_chain_adapters::token_sha256_hash(token)
            .map_err(|e| e.to_string())?;
        let result = sqlx::query(
            "UPDATE access_tokens SET deleted_at=now() WHERE value_sha256=$1 AND deleted_at IS NULL",
        )
        .bind(hash.to_vec())
        .execute(&self.pool)
        .await
        .map_err(|e| e.to_string())?;
        if result.rows_affected() == 0 {
            return Err("access token not found".to_owned());
        }
        Ok(())
    }

    async fn get_external_service(
        &self,
        id: i64,
    ) -> Result<Option<ExternalServiceRow>, String> {
        let row = sqlx::query(
            "SELECT id, kind, display_name, config, unrestricted, has_webhooks, code_host_id, \
             creator_id, last_updater_id, created_at, updated_at \
             FROM external_services WHERE id=$1 AND deleted_at IS NULL",
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| e.to_string())?;
        row.as_ref().map(external_service_row).transpose()
    }

    async fn create_external_service(&self, new: &NewExternalService) -> Result<i64, String> {
        // `recalculateFields` (`internal/database/external_services.go:1662-1675`)
        // writes `calcUnrestricted(config)`; for `OTHER` without an
        // `authorization` block that is `true`. `has_webhooks` stays false:
        // `OTHER` has no webhook array.
        let unrestricted = calc_unrestricted(&new.config);
        let row = sqlx::query(
            "INSERT INTO external_services (kind, display_name, config, created_at, updated_at, \
             unrestricted, has_webhooks, creator_id, last_updater_id) \
             VALUES ($1, $2, $3, now(), now(), $4, FALSE, $5, $6) RETURNING id",
        )
        .bind(&new.kind)
        .bind(&new.display_name)
        .bind(&new.config)
        .bind(unrestricted)
        .bind(new.creator_id)
        .bind(new.last_updater_id)
        .fetch_one(&self.pool)
        .await
        .map_err(|e| e.to_string())?;
        row.try_get("id").map_err(|e| e.to_string())
    }

    async fn update_external_service(
        &self,
        id: i64,
        display_name: Option<&str>,
        config: Option<&str>,
        updater_id: Option<i32>,
    ) -> Result<(), String> {
        // `Update` (`internal/database/external_services.go:814-937`): a new
        // config recomputes `unrestricted`; every update sets
        // `next_sync_at = NOW()` so the scheduler picks the service up again.
        let result = match config {
            Some(config) => {
                sqlx::query(
                    "UPDATE external_services SET display_name=COALESCE($2, display_name), \
                     config=$3, unrestricted=$4, last_updater_id=COALESCE($5, last_updater_id), \
                     next_sync_at=now(), updated_at=now() WHERE id=$1 AND deleted_at IS NULL",
                )
                .bind(id)
                .bind(display_name)
                .bind(config)
                .bind(calc_unrestricted(config))
                .bind(updater_id)
                .execute(&self.pool)
                .await
            }
            None => {
                sqlx::query(
                    "UPDATE external_services SET display_name=COALESCE($2, display_name), \
                     last_updater_id=COALESCE($3, last_updater_id), next_sync_at=now(), \
                     updated_at=now() WHERE id=$1 AND deleted_at IS NULL",
                )
                .bind(id)
                .bind(display_name)
                .bind(updater_id)
                .execute(&self.pool)
                .await
            }
        }
        .map_err(|e| e.to_string())?;
        if result.rows_affected() == 0 {
            return Err(format!("external service not found: {id}"));
        }
        Ok(())
    }

    async fn delete_external_service(&self, id: i64, _async_delete: bool) -> Result<(), String> {
        // `Delete` (`internal/database/external_services.go:947-1053`): collect
        // the associated repo ids while removing the associations, soft delete
        // repos that are now orphans, then soft delete the service itself.
        let mut tx = self.pool.begin().await.map_err(|e| e.to_string())?;
        let repo_rows = sqlx::query(
            "DELETE FROM external_service_repos WHERE external_service_id=$1 RETURNING repo_id",
        )
        .bind(id)
        .fetch_all(&mut *tx)
        .await
        .map_err(|e| e.to_string())?;
        let repo_ids: Vec<i32> = repo_rows
            .iter()
            .map(|row| row.try_get("repo_id").map_err(|e| e.to_string()))
            .collect::<Result<_, _>>()?;
        if !repo_ids.is_empty() {
            // `deleteRepoIfOrphanQuery`; `soft_deleted_repository_name`
            // prefixes `DELETED-<epoch>-` once.
            sqlx::query(
                "UPDATE repo SET name=soft_deleted_repository_name(name), deleted_at=now() \
                 WHERE deleted_at IS NULL AND id = ANY($1) \
                 AND NOT EXISTS (SELECT 1 FROM external_service_repos WHERE repo_id = repo.id)",
            )
            .bind(&repo_ids)
            .execute(&mut *tx)
            .await
            .map_err(|e| e.to_string())?;
        }
        let result = sqlx::query(
            "UPDATE external_services SET deleted_at=now() WHERE id=$1 AND deleted_at IS NULL",
        )
        .bind(id)
        .execute(&mut *tx)
        .await
        .map_err(|e| e.to_string())?;
        if result.rows_affected() == 0 {
            return Err(format!("external service not found: {id}"));
        }
        tx.commit().await.map_err(|e| e.to_string())
    }

    async fn exclude_repo_from_external_services(
        &self,
        external_service_ids: &[i64],
        repo_id: i32,
    ) -> Result<(), String> {
        for id in external_service_ids {
            sqlx::query(
                "DELETE FROM external_service_repos \
                 WHERE external_service_id=$1 AND repo_id=$2",
            )
            .bind(id)
            .bind(repo_id)
            .execute(&self.pool)
            .await
            .map_err(|e| e.to_string())?;
        }
        Ok(())
    }

    async fn get_repo_by_id(&self, id: i32) -> Result<Option<RepoRow>, String> {
        let row = sqlx::query("SELECT id, name FROM repo WHERE id=$1 AND deleted_at IS NULL")
            .bind(id)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| e.to_string())?;
        row.map(|row| {
            Ok(RepoRow {
                id: row.try_get("id").map_err(|e| e.to_string())?,
                name: row.try_get("name").map_err(|e| e.to_string())?,
            })
        })
        .transpose()
    }

    async fn get_gitserver_repo(&self, repo_id: i32) -> Result<Option<CloneStatus>, String> {
        let row = sqlx::query("SELECT clone_status FROM gitserver_repos WHERE repo_id=$1")
            .bind(repo_id)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| e.to_string())?;
        row.map(|row| {
            Ok(CloneStatus::from_str(
                &row.try_get::<String, _>("clone_status")
                    .map_err(|e| e.to_string())?,
            ))
        })
        .transpose()
    }

    async fn reclone_repository(&self, repo_id: i32, _name: &str) -> Result<(), String> {
        sqlx::query(
            "UPDATE gitserver_repos SET clone_status='not_cloned', updated_at=now() WHERE repo_id=$1",
        )
        .bind(repo_id)
        .execute(&self.pool)
        .await
        .map_err(|e| e.to_string())?;
        Ok(())
    }

    async fn delete_repository_from_disk(
        &self,
        repo_id: i32,
        _name: &str,
    ) -> Result<(), String> {
        sqlx::query(
            "UPDATE gitserver_repos SET clone_status='not_cloned', updated_at=now() WHERE repo_id=$1",
        )
        .bind(repo_id)
        .execute(&self.pool)
        .await
        .map_err(|e| e.to_string())?;
        Ok(())
    }

    async fn reindex_repository(&self, _repo_id: i32, _name: &str) -> Result<(), String> {
        // `zoekt.Reindex` is a network call to the zoekt index server; the
        // frontend baseline returns its error. The production seam is explicit
        // (`DisabledZoektReindexer` in `admin_mutations.rs`).
        Err("zoekt reindex is not configured".to_owned())
    }
}

fn org_row(row: &sqlx::postgres::PgRow) -> Result<OrgRow, String> {
    Ok(OrgRow {
        id: row.try_get("id").map_err(|e| e.to_string())?,
        name: row.try_get("name").map_err(|e| e.to_string())?,
        display_name: row.try_get("display_name").map_err(|e| e.to_string())?,
        created_at: row.try_get("created_at").map_err(|e| e.to_string())?,
        updated_at: row.try_get("updated_at").map_err(|e| e.to_string())?,
    })
}

fn token_row(row: &sqlx::postgres::PgRow) -> Result<AccessTokenRow, String> {
    Ok(AccessTokenRow {
        id: row.try_get("id").map_err(|e| e.to_string())?,
        subject_user_id: row.try_get("subject_user_id").map_err(|e| e.to_string())?,
        note: row.try_get("note").map_err(|e| e.to_string())?,
        scopes: row.try_get("scopes").map_err(|e| e.to_string())?,
        created_at: row.try_get("created_at").map_err(|e| e.to_string())?,
        last_used_at: row.try_get("last_used_at").map_err(|e| e.to_string())?,
        expires_at: row.try_get("expires_at").map_err(|e| e.to_string())?,
        value: String::new(),
    })
}

/// `calcUnrestricted` (`internal/database/external_services.go:1633-1660`) with
/// `permissions.userMapping` disabled: an `enforcePermissions` flag inverts the
/// value; otherwise a config with an `authorization` block is restricted and
/// everything else (including `OTHER`) is unrestricted.
fn calc_unrestricted(config: &str) -> bool {
    let Ok(value) = serde_json::from_str::<serde_json::Value>(config) else {
        return true;
    };
    match value.get("enforcePermissions") {
        Some(enforce_permissions) => !enforce_permissions.as_bool().unwrap_or(false),
        None => value.get("authorization").is_none(),
    }
}

fn external_service_row(row: &sqlx::postgres::PgRow) -> Result<ExternalServiceRow, String> {
    Ok(ExternalServiceRow {
        id: row.try_get("id").map_err(|e| e.to_string())?,
        kind: row.try_get("kind").map_err(|e| e.to_string())?,
        display_name: row.try_get("display_name").map_err(|e| e.to_string())?,
        config: row.try_get("config").map_err(|e| e.to_string())?,
        unrestricted: row.try_get("unrestricted").map_err(|e| e.to_string())?,
        has_webhooks: row.try_get("has_webhooks").map_err(|e| e.to_string())?,
        code_host_id: row.try_get("code_host_id").map_err(|e| e.to_string())?,
        creator_id: row.try_get("creator_id").map_err(|e| e.to_string())?,
        last_updater_id: row.try_get("last_updater_id").map_err(|e| e.to_string())?,
        created_at: row.try_get("created_at").map_err(|e| e.to_string())?,
        updated_at: row.try_get("updated_at").map_err(|e| e.to_string())?,
    })
}

/// The live configuration values the side effects read. Kept as plain values
/// so the module has no dependency on the site-configuration schema; the
/// frontend wiring fills this from `LiveConfig`.
#[derive(Debug, Clone)]
pub struct AdminConfig {
    pub can_send_email: bool,
    pub email_verification_required: bool,
    pub reset_password_enabled: bool,
    pub access_tokens_allow: AccessTokensAllowMode,
    pub access_token_expiration_options: AccessTokenExpirationOptions,
    pub dotcom: bool,
    pub external_services_writable: bool,
    pub auth_enable_username_changes: bool,
    pub external_url: String,
}

impl Default for AdminConfig {
    fn default() -> Self {
        Self {
            can_send_email: false,
            email_verification_required: false,
            reset_password_enabled: false,
            access_tokens_allow: AccessTokensAllowMode::All,
            access_token_expiration_options: AccessTokenExpirationOptions {
                allow_no_expiration: true,
                max_duration_seconds: Ok(90 * 86400),
            },
            dotcom: false,
            external_services_writable: true,
            auth_enable_username_changes: true,
            external_url: "http://localhost:3080".to_owned(),
        }
    }
}

/// Production side effects (e-mail notifications, audit events, perms sync).
pub struct PgAdminEffects {
    pool: PgPool,
    config: AdminConfig,
    live: Option<Arc<sg_config::LiveConfig>>,
}

impl PgAdminEffects {
    pub fn new(pool: PgPool, config: AdminConfig) -> Self {
        Self {
            pool,
            config,
            live: None,
        }
    }

    /// Attach the live configuration used for the `auditlog-expansion`
    /// feature-flag lookup.
    pub fn with_live(mut self, live: Arc<sg_config::LiveConfig>) -> Self {
        self.live = Some(live);
        self
    }
}

#[async_trait]
impl AdminEffects for PgAdminEffects {
    fn can_send_email(&self) -> bool {
        self.config.can_send_email
    }

    fn email_verification_required(&self) -> bool {
        self.config.email_verification_required
    }

    fn reset_password_enabled(&self) -> bool {
        self.config.reset_password_enabled
    }

    fn access_tokens_allow(&self) -> AccessTokensAllowMode {
        self.config.access_tokens_allow
    }

    fn access_token_expiration_options(&self) -> AccessTokenExpirationOptions {
        self.config.access_token_expiration_options.clone()
    }

    fn dotcom(&self) -> bool {
        self.config.dotcom
    }

    fn external_services_writable(&self) -> bool {
        self.config.external_services_writable
    }

    fn auth_enable_username_changes(&self) -> bool {
        self.config.auth_enable_username_changes
    }

    fn auditlog_expansion(&self) -> bool {
        // Per-viewer feature flags are resolved by the caller's request
        // context in the baseline. The wiring uses the site-level default;
        // diffs pass the flag through the fake effects instead.
        let _ = &self.live;
        false
    }

    fn make_email_verification_code(&self) -> Result<String, String> {
        // `backend.MakeEmailVerificationCode`: 20 random bytes, standard
        // base64.
        let mut bytes = [0u8; 20];
        random_bytes(&mut bytes)?;
        Ok(base64_std(&bytes))
    }

    async fn send_email(&self, _user_id: i32, _email: &str, _kind: &str) -> Result<(), String> {
        // SMTP transport lives in the notification layer; the baseline logs
        // send failures instead of failing the mutation.
        Ok(())
    }

    async fn delete_password_reset_code(&self, user_id: i32) -> Result<(), String> {
        sqlx::query(
            "UPDATE users SET passwd_reset_code=NULL, passwd_reset_time=NULL WHERE id=$1",
        )
        .bind(user_id)
        .execute(&self.pool)
        .await
        .map_err(|e| e.to_string())?;
        Ok(())
    }

    async fn delete_stale_perforce(
        &self,
        user_id: i32,
        email: &str,
    ) -> Result<(), String> {
        sqlx::query(
            "UPDATE user_external_accounts SET deleted_at=now() \
             WHERE user_id=$1 AND service_type='perforce' AND account_id=$2 AND deleted_at IS NULL",
        )
        .bind(user_id)
        .bind(email)
        .execute(&self.pool)
        .await
        .map_err(|e| e.to_string())?;
        Ok(())
    }

    async fn schedule_perms_sync(
        &self,
        user_ids: &[i32],
        reason: &str,
    ) -> Result<(), String> {
        for user_id in user_ids {
            sqlx::query(
                "INSERT INTO permission_sync_jobs (user_id, reason) VALUES ($1, $2)",
            )
            .bind(user_id)
            .bind(reason)
            .execute(&self.pool)
            .await
            .map_err(|e| e.to_string())?;
        }
        Ok(())
    }

    async fn validate_external_service(&self, _kind: &str, _config: &str) -> Result<(), String> {
        // The code-host network probe is an explicit seam in this snapshot.
        Ok(())
    }

    async fn enqueue_external_service_sync(&self, id: i64) -> Result<(), String> {
        // `EnqueueSingleSyncJob` (`internal/repos/store.go:568-590`) only
        // inserts when no `queued`/`processing` job exists; the job scheduler
        // retries otherwise, so the mutation treats errors as non-fatal.
        sg_db::external_service_sync_jobs::SyncJobsStore::new(self.pool.clone())
            .enqueue_single_sync_job(id)
            .await
            .map(|_| ())
            .map_err(|e| e.to_string())
    }

    async fn reset_password_url(
        &self,
        user: &UserRow,
        email: &str,
        _email_verified: bool,
    ) -> Result<Option<String>, String> {
        if !self.reset_password_enabled() {
            return Ok(None);
        }
        if !email.is_empty() && self.can_send_email() {
            // `HandleSetPasswordEmail` creates a reset code, sends the URL and
            // returns it; the transport is a no-op seam here.
            let code = self.make_email_verification_code()?;
            sqlx::query(
                "UPDATE users SET passwd_reset_code=$2, passwd_reset_time=now() WHERE id=$1",
            )
            .bind(user.id)
            .bind(&code)
            .execute(&self.pool)
            .await
            .map_err(|e| e.to_string())?;
            return Ok(Some(format!(
                "{}/-/reset-password?code={}&userID={}",
                self.config.external_url.trim_end_matches('/'),
                code,
                user.id
            )));
        }
        // `backend.MakePasswordResetURL`.
        let code = self.make_email_verification_code()?;
        sqlx::query(
            "UPDATE users SET passwd_reset_code=$2, passwd_reset_time=now() WHERE id=$1",
        )
        .bind(user.id)
        .bind(&code)
        .execute(&self.pool)
        .await
        .map_err(|e| e.to_string())?;
        Ok(Some(format!(
            "{}/-/reset-password?code={}&userID={}",
            self.config.external_url.trim_end_matches('/'),
            code,
            user.id
        )))
    }

    async fn audit(&self, name: &str, user_id: i32, argument: serde_json::Value) {
        let _ = sg_db::security_event_logs::insert_security_event(
            &self.pool,
            &sg_db::security_event_logs::SecurityEvent {
                name,
                url: "",
                user_id,
                anonymous_user_id: "",
                source: "BACKEND",
                argument,
                version: "",
                timestamp: OffsetDateTime::now_utc(),
            },
        )
        .await;
    }

    async fn audit_account_modified(
        &self,
        _user_id: i32,
        _actor_user_id: i32,
        _fields: &[String],
    ) {
        // `auditlog.Record(AccountModified)` is gated by its own feature flag
        // and writes to the audit log sink; it is not part of the GraphQL
        // response.
    }
}

/// `accesstoken.GenerateToken` (OSS default): 20 random bytes hex.
fn generate_personal_access_token() -> Result<String, String> {
    let mut bytes = [0u8; 20];
    random_bytes(&mut bytes)?;
    let mut token = String::with_capacity(40);
    for byte in bytes {
        token.push_str(&format!("{byte:02x}"));
    }
    Ok(token)
}

/// `crypto/rand.Read` through the OS entropy source.
fn random_bytes(target: &mut [u8]) -> Result<(), String> {
    use std::io::Read;
    let mut file = std::fs::File::open("/dev/urandom").map_err(|e| e.to_string())?;
    file.read_exact(target).map_err(|e| e.to_string())
}

fn base64_std(bytes: &[u8]) -> String {
    use base64::Engine;
    base64::engine::general_purpose::STANDARD.encode(bytes)
}

/// `suspiciousnames.CheckNameAllowedForUserOrOrganization`.
pub fn check_name_allowed_for_user_or_organization(
    dotcom: bool,
    name: &str,
) -> Result<(), String> {
    if dotcom && SUSPICIOUS_NAMES.contains(&name.to_ascii_lowercase().as_str()) {
        return Err(format!("rejected suspicious name {name:?}"));
    }
    Ok(())
}

include!("suspicious_names.rs");

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn site_admin_gate_messages() {
        assert_eq!(
            check_current_user_is_site_admin(Actor::Anonymous, false),
            Err(NOT_AUTHENTICATED.to_owned())
        );
        assert_eq!(
            check_current_user_is_site_admin(Actor::User { uid: 1 }, false),
            Err(MUST_BE_SITE_ADMIN.to_owned())
        );
        assert_eq!(
            check_current_user_is_site_admin(Actor::Internal, false),
            Ok(())
        );
    }

    #[test]
    fn same_user_passes_without_admin() {
        assert_eq!(
            check_site_admin_or_same_user(Actor::User { uid: 2 }, false, 2),
            Ok(())
        );
        assert_eq!(
            check_site_admin_or_same_user(Actor::User { uid: 2 }, false, 3),
            Err(MUST_BE_SITE_ADMIN_OR_SAME_USER.to_owned())
        );
    }

    #[test]
    fn suspicious_names_are_case_insensitive() {
        assert!(check_name_allowed_for_user_or_organization(true, "Admin").is_err());
        assert!(check_name_allowed_for_user_or_organization(false, "admin").is_ok());
        assert!(check_name_allowed_for_user_or_organization(true, "alice").is_ok());
    }
}
