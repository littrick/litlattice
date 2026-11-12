#![forbid(unsafe_code)]

//! Shared user provisioning for external identity providers
//! (`cmd/frontend/auth/user.go` `GetAndSaveUser`), used by the GitHub, GitLab and
//! OpenID Connect OAuth resolvers.
//!
//! The baseline resolves a user in this order:
//!
//! 1. an authenticated actor (not reachable from a callback),
//! 2. `UserExternalAccounts().Update(spec)` (existing external account),
//! 3. `Users().GetByVerifiedEmail(email)` (existing verified email),
//! 4. `Users().CreateWithExternalAccount(...)` when signup is allowed, retrying
//!    once with `AddRandomSuffix` on a username conflict,
//! 5. profile update for existing users (only empty display name / avatar URL),
//! 6. `UserExternalAccounts().Upsert(...)` when the user was found by email.
//!
//! Only the existing tables/columns are used (`users`, `names`, `user_emails`,
//! `user_external_accounts`). All storage is behind [`ProvisionStore`] so the
//! differential harness can run against an in-memory store; [`PgProvisionStore`]
//! is the production adapter over `sg-db`.

use async_trait::async_trait;
use regex::Regex;
use sqlx::PgPool;

use sg_db::user_external_accounts::AuthAccountSpec;
use sg_db::users::{self as users_db, DbError, NewAuthUser};

/// One external identity to resolve/create (`GetAndSaveUserOp`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExternalIdentity<'a> {
    /// `extsvc.AccountSpec.ServiceType` (`github`, `gitlab`, `openidconnect`).
    pub service_type: &'a str,
    /// `extsvc.AccountSpec.ServiceID` (normalized code host base URL).
    pub service_id: &'a str,
    /// `extsvc.AccountSpec.ClientID`.
    pub client_id: &'a str,
    /// `extsvc.AccountSpec.AccountID` (provider user ID as a string).
    pub account_id: &'a str,
    /// `UserProps.Username`, already normalized by the provider.
    pub login: &'a str,
    /// `UserProps.Email` (always verified for the OAuth providers).
    pub email: &'a str,
    /// `UserProps.EmailIsVerified`.
    pub email_verified: bool,
    /// `UserProps.DisplayName`.
    pub display_name: &'a str,
    /// `UserProps.AvatarURL`.
    pub avatar_url: &'a str,
    /// `GetAndSaveUserOp.CreateIfNotExist`.
    pub create_if_not_exist: bool,
}

/// The result of [`get_or_create_user`] (`newUserCreated` + user ID).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProvisionOutcome {
    /// A new `users` row (and its email/external account) was created.
    Created { user_id: i32 },
    /// An existing user matched by external account or verified email.
    Existing { user_id: i32 },
}

impl ProvisionOutcome {
    pub fn user_id(self) -> i32 {
        match self {
            ProvisionOutcome::Created { user_id } | ProvisionOutcome::Existing { user_id } => {
                user_id
            }
        }
    }

    pub fn new_user_created(self) -> bool {
        matches!(self, ProvisionOutcome::Created { .. })
    }
}

/// `auth.GetAndSaveUser`'s `(safeErrMsg, err)` pair.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProvisionError {
    /// User-facing message (the HTTP 500 body).
    pub safe_message: String,
    /// Internal error text (server logs only).
    pub message: String,
}

impl ProvisionError {
    pub fn new(safe_message: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            safe_message: safe_message.into(),
            message: message.into(),
        }
    }
}

/// `userpasswd.AddRandomSuffix`; injectable so the differential harness can use
/// a fixed suffix.
pub trait UsernameSuffixSource: Send + Sync {
    fn add_random_suffix(&self, username: &str) -> Result<String, String>;
}

/// Production source: 5 random bytes from the OS, mapped to `[a-z]` like
/// `userpasswd.AddRandomSuffix`.
#[derive(Debug, Clone, Copy, Default)]
pub struct OsUsernameSuffix;

impl UsernameSuffixSource for OsUsernameSuffix {
    fn add_random_suffix(&self, username: &str) -> Result<String, String> {
        use std::io::Read as _;
        let mut bytes = [0u8; 5];
        let mut file = std::fs::File::open("/dev/urandom").map_err(|err| err.to_string())?;
        file.read_exact(&mut bytes).map_err(|err| err.to_string())?;
        let suffix: String = bytes
            .iter()
            .map(|byte| (b'a' + (byte % 26)) as char)
            .collect();
        if username.is_empty() || username.ends_with('-') {
            Ok(format!("{username}{suffix}"))
        } else {
            Ok(format!("{username}-{suffix}"))
        }
    }
}

/// One row for `users.CreateWithExternalAccount`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NewUserRow<'a> {
    pub username: &'a str,
    pub display_name: &'a str,
    pub avatar_url: &'a str,
    pub email: &'a str,
    pub email_is_verified: bool,
}

/// The `users` fields `GetAndSaveUser` reads for the profile-update decision.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UserProfileRow {
    pub id: i32,
    pub display_name: String,
    pub avatar_url: String,
}

/// `database.ErrCannotCreateUser` reduced to the two codes the auth flow
/// distinguishes, plus a generic driver error.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CreateUserStoreError {
    /// `ErrCannotCreateUser{err_username_exists}` / `names_pkey`.
    UsernameExists,
    /// `ErrCannotCreateUser{err_email_exists}`.
    EmailExists,
    /// Any other driver error.
    Other(String),
}

/// Storage operations of `GetAndSaveUser`, injectable for the differential test.
#[async_trait]
pub trait ProvisionStore: Send + Sync {
    /// `UserExternalAccounts().Update(spec)`: the existing user ID, if any.
    /// `Ok(None)` is the baseline's not-found error.
    async fn update_external_account(
        &self,
        identity: &ExternalIdentity<'_>,
    ) -> Result<Option<i32>, String>;

    /// `Users().GetByVerifiedEmail`.
    async fn find_user_by_verified_email(&self, email: &str) -> Result<Option<i32>, String>;

    /// `Users().GetByUsername` (used by providers that opt into
    /// `LookUpByUsername`).
    async fn find_user_by_username(&self, username: &str) -> Result<Option<i32>, String>;

    /// `Users().GetByID` narrowed to the profile fields.
    async fn get_user_profile(&self, id: i32) -> Result<Option<UserProfileRow>, String>;

    /// `Users().Update` with the fields that actually change.
    async fn update_user_profile(
        &self,
        id: i32,
        display_name: Option<&str>,
        avatar_url: Option<&str>,
    ) -> Result<(), String>;

    /// `Users().CreateWithExternalAccount`: one transaction that writes
    /// `users` → `names` → `user_emails` → `user_external_accounts`.
    async fn create_with_external_account(
        &self,
        new_user: &NewUserRow<'_>,
        identity: &ExternalIdentity<'_>,
    ) -> Result<i32, CreateUserStoreError>;

    /// `UserExternalAccounts().Upsert`.
    async fn upsert_external_account(
        &self,
        user_id: i32,
        identity: &ExternalIdentity<'_>,
    ) -> Result<(), String>;

    /// `db.Authz().GrantPendingPermissions(ctx, {UserID, Perm: authz.Read,
    /// Type: authz.PermRepos})` (`internal/database/authz.go:98`). Callers log
    /// and ignore failures (`cmd/frontend/auth/user.go:184,389`).
    async fn grant_pending_permissions(&self, user_id: i32) -> Result<(), String>;
}

/// Injected dependencies of [`get_or_create_user`].
pub struct ProvisionDeps<'a> {
    pub store: &'a dyn ProvisionStore,
    pub suffix: &'a dyn UsernameSuffixSource,
    /// `dotcom.SourcegraphDotComMode()`; affects `NormalizeUsername`'s
    /// email-derived suffix only.
    pub sourcegraph_dotcom: bool,
}

const SAFE_LOOKUP_EXT_ACCOUNT: &str = "Unexpected error looking up the Sourcegraph user account associated with the external account. Ask a site admin for help.";
const SAFE_LOOKUP_EMAIL: &str =
    "Unexpected error looking up the Sourcegraph user by verified email. Ask a site admin for help.";
const SAFE_GET_USER: &str =
    "Unexpected error getting the Sourcegraph user account. Ask a site admin for help.";
const SAFE_UPDATE_USER: &str = "Unexpected error updating the Sourcegraph user account with new user profile information from the external account. Ask a site admin for help.";
const SAFE_CREATE_USER: &str =
    "Unable to create a new user account due to a unexpected error. Ask a site admin for help.";
const SAFE_LINK_EXTERNAL_ACCOUNT: &str = "Unexpected error associating the external account with your Sourcegraph user. The most likely cause for this problem is that another Sourcegraph user is already linked with this external account. A site admin or the other user can unlink the account to fix this problem.";
const SAFE_FIRST_SIGN_IN: &str = "It looks like this is your first time signing in with this external identity. Sourcegraph couldn't link it to an existing user, because no verified email was provided. Ask your site admin to configure the auth provider to include the user's verified email on sign-in.";

/// The `suspiciousnames.suspiciousNames` blocklist (alphabetical, all
/// lowercase). Only consulted on Sourcegraph.com (`sourcegraph_dotcom`).
pub const SUSPICIOUS_NAMES: &[&str] = &[
    "about",
    "access",
    "account",
    "accounts",
    "add",
    "address",
    "adm",
    "admin",
    "administration",
    "adult",
    "advertising",
    "affiliate",
    "affiliates",
    "ajax",
    "analytics",
    "android",
    "anon",
    "anonymous",
    "api",
    "app",
    "apps",
    "archive",
    "atom",
    "auth",
    "authentication",
    "avatar",
    "backup",
    "banner",
    "banners",
    "billing",
    "bin",
    "blog",
    "blogs",
    "board",
    "bot",
    "bots",
    "business",
    "cache",
    "calendar",
    "campaign",
    "careers",
    "cgi",
    "chat",
    "client",
    "code",
    "commercial",
    "compare",
    "config",
    "connect",
    "contact",
    "contest",
    "cpanel",
    "create",
    "css",
    "dashboard",
    "data",
    "database",
    "delete",
    "demo",
    "design",
    "designer",
    "dev",
    "devel",
    "dir",
    "directory",
    "doc",
    "docs",
    "domain",
    "download",
    "downloads",
    "ecommerce",
    "edit",
    "editor",
    "email",
    "example",
    "explore",
    "faq",
    "favorite",
    "feed",
    "feedback",
    "file",
    "files",
    "flog",
    "follow",
    "forum",
    "forums",
    "free",
    "ftp",
    "games",
    "global",
    "group",
    "groups",
    "guest",
    "help",
    "home",
    "homepage",
    "host",
    "hosting",
    "hostname",
    "html",
    "http",
    "httpd",
    "https",
    "image",
    "images",
    "imap",
    "img",
    "index",
    "info",
    "information",
    "intranet",
    "invite",
    "ipad",
    "iphone",
    "irc",
    "issues",
    "java",
    "javascript",
    "job",
    "jobs",
    "js",
    "knowledgebase",
    "list",
    "lists",
    "log",
    "login",
    "logout",
    "logs",
    "mail",
    "mail1",
    "mail2",
    "mail3",
    "mail4",
    "mail5",
    "mailer",
    "mailing",
    "manager",
    "marketing",
    "marketplace",
    "master",
    "me",
    "media",
    "message",
    "messenger",
    "mine",
    "mobile",
    "movie",
    "movies",
    "msg",
    "msn",
    "music",
    "mx",
    "my",
    "mysql",
    "name",
    "named",
    "net",
    "network",
    "new",
    "news",
    "newsletter",
    "nick",
    "nickname",
    "notes",
    "ns",
    "ns1",
    "ns2",
    "ns3",
    "ns4",
    "old",
    "online",
    "operator",
    "order",
    "orders",
    "page",
    "pager",
    "pages",
    "panel",
    "password",
    "perl",
    "photo",
    "photos",
    "php",
    "pic",
    "pics",
    "plugin",
    "plugins",
    "pop",
    "pop3",
    "post",
    "postfix",
    "postmaster",
    "posts",
    "profile",
    "project",
    "projects",
    "promo",
    "pub",
    "public",
    "pulls",
    "python",
    "random",
    "register",
    "registration",
    "root",
    "rss",
    "ruby",
    "sale",
    "sales",
    "sample",
    "samples",
    "script",
    "scripts",
    "search",
    "secure",
    "security",
    "send",
    "service",
    "setting",
    "settings",
    "setup",
    "shop",
    "signin",
    "signup",
    "site",
    "sitemap",
    "sites",
    "smtp",
    "sql",
    "ssh",
    "staff",
    "stafftools",
    "stage",
    "staging",
    "start",
    "stat",
    "static",
    "stats",
    "status",
    "store",
    "stores",
    "subdomain",
    "subscribe",
    "suporte",
    "support",
    "system",
    "talk",
    "task",
    "tasks",
    "tech",
    "telnet",
    "test",
    "test1",
    "test2",
    "test3",
    "teste",
    "tests",
    "theme",
    "themes",
    "tmp",
    "todo",
    "tools",
    "update",
    "upload",
    "url",
    "usage",
    "user",
    "username",
    "video",
    "videos",
    "visitor",
    "web",
    "webmail",
    "webmaster",
    "website",
    "websites",
    "ww",
    "www",
    "www1",
    "www2",
    "www3",
    "www4",
    "www5",
    "www6",
    "www7",
    "wwws",
    "wwww",
    "xpg",
    "xxx",
    "you",
    "yourdomain",
    "yourname",
    "yoursite",
    "yourusername",
];

/// `suspiciousnames.isSuspicious`: case-insensitive membership.
pub fn is_suspicious_name(name: &str) -> bool {
    SUSPICIOUS_NAMES
        .binary_search(&name.to_ascii_lowercase().as_str())
        .is_ok()
}

/// `auth.NormalizeUsername` (`userpasswd.NormalizeUsername`).
///
/// Go's `\w` is ASCII, so the classes below spell out `[0-9A-Za-z_]` instead
/// of relying on Rust's Unicode `\w`. On dotcom the normalized name is checked
/// against [`SUSPICIOUS_NAMES`]
/// (`suspiciousnames.CheckNameAllowedForUserOrOrganization`).
pub fn normalize_username(
    name: &str,
    suffix: &dyn UsernameSuffixSource,
    sourcegraph_dotcom: bool,
) -> Result<String, String> {
    let orig_name = name;
    let mut name = name.to_owned();

    // If the username is an email address (exactly one `@`), extract the
    // username part; dotcom appends a random suffix.
    if let Some(index) = name.find('@') {
        if index == name.rfind('@').unwrap_or(usize::MAX) {
            name.truncate(index);
            if sourcegraph_dotcom {
                name = suffix
                    .add_random_suffix(&name)
                    .map_err(|err| format!("add random suffix: {err}"))?;
            }
        }
    }

    // Replace all non-alphanumeric characters with a dash.
    let disallowed = Regex::new(r"[^0-9A-Za-z_.-]").expect("static regex");
    name = disallowed.replace_all(&name, "-").into_owned();

    // Replace all consecutive dashes and periods with a single dash.
    let consecutive = Regex::new(r"[-.]{2,}").expect("static regex");
    name = consecutive.replace_all(&name, "-").into_owned();

    // Trim leading and trailing dashes and periods.
    if name.starts_with('-') || name.starts_with('.') {
        name.remove(0);
    }
    if name.ends_with('.') {
        name.pop();
    }

    if name.is_empty() {
        return Err(format!(
            "username {orig_name:?} could not be normalized to acceptable format"
        ));
    }

    if sourcegraph_dotcom && is_suspicious_name(&name) {
        return Err(format!("rejected suspicious name {name:?}"));
    }

    Ok(name)
}

/// `auth.GetAndSaveUser` (see the module docs for the ordering).
pub async fn get_or_create_user(
    deps: &ProvisionDeps<'_>,
    identity: &ExternalIdentity<'_>,
) -> Result<ProvisionOutcome, ProvisionError> {
    // Second, check if the user account already exists by external account.
    match deps.store.update_external_account(identity).await {
        Ok(Some(user_id)) => {
            update_profile(deps.store, user_id, identity).await?;
            return Ok(ProvisionOutcome::Existing { user_id });
        }
        Ok(None) => {}
        Err(message) => {
            return Err(ProvisionError::new(SAFE_LOOKUP_EXT_ACCOUNT, message));
        }
    }

    // Third, look up by verified email (the providers that use
    // LookUpByUsername are not part of this port).
    if identity.email_verified {
        match deps.store.find_user_by_verified_email(identity.email).await {
            Ok(Some(user_id)) => {
                update_profile(deps.store, user_id, identity).await?;
                deps.store
                    .upsert_external_account(user_id, identity)
                    .await
                    .map_err(|message| ProvisionError::new(SAFE_LINK_EXTERNAL_ACCOUNT, message))?;
                // `db.Authz().GrantPendingPermissions`, best-effort
                // (`cmd/frontend/auth/user.go:389-400`).
                let _ = deps.store.grant_pending_permissions(user_id).await;
                return Ok(ProvisionOutcome::Existing { user_id });
            }
            Ok(None) => {
                if !identity.create_if_not_exist {
                    return Err(ProvisionError::new(
                        format!(
                            "User account with verified email {:?} does not exist. Ask a site admin to create your account and then verify your email.",
                            identity.email
                        ),
                        "user not found",
                    ));
                }
            }
            Err(message) => {
                return Err(ProvisionError::new(SAFE_LOOKUP_EMAIL, message));
            }
        }
    }

    if !identity.create_if_not_exist {
        return Err(ProvisionError::new(SAFE_FIRST_SIGN_IN, ""));
    }

    // Fourth, create a new user, retrying once with a random suffix if the
    // username is taken.
    let mut username = identity.login.to_owned();
    let created = create_once(deps, identity, &username).await;
    let created = match created {
        Err(CreateUserStoreError::UsernameExists) => {
            username = deps.suffix.add_random_suffix(&username).map_err(|err| {
                ProvisionError::new(
                    SAFE_CREATE_USER,
                    format!(
                        "username: {:?}, email: {:?}: {err}",
                        username, identity.email
                    ),
                )
            })?;
            create_once(deps, identity, &username).await
        }
        other => other,
    };

    match created {
        Ok(user_id) => {
            // `db.Authz().GrantPendingPermissions`, best-effort after the
            // permission-sync scheduling in the baseline
            // (`cmd/frontend/auth/user.go:184-194`).
            let _ = deps.store.grant_pending_permissions(user_id).await;
            Ok(ProvisionOutcome::Created { user_id })
        }
        Err(CreateUserStoreError::UsernameExists) => Err(ProvisionError::new(
            format!(
                "Username {:?} already exists, but no verified email matched {:?}",
                username, identity.email
            ),
            "cannot create user: err_username_exists",
        )),
        Err(CreateUserStoreError::EmailExists) => Err(ProvisionError::new(
            SAFE_CREATE_USER,
            "cannot create user: err_email_exists",
        )),
        Err(CreateUserStoreError::Other(message)) => Err(ProvisionError::new(
            SAFE_CREATE_USER,
            format!(
                "username: {:?}, email: {:?}: {message}",
                username, identity.email
            ),
        )),
    }
}

async fn create_once(
    deps: &ProvisionDeps<'_>,
    identity: &ExternalIdentity<'_>,
    username: &str,
) -> Result<i32, CreateUserStoreError> {
    let new_user = NewUserRow {
        username,
        display_name: identity.display_name,
        avatar_url: identity.avatar_url,
        email: identity.email,
        email_is_verified: identity.email_verified,
    };
    deps.store
        .create_with_external_account(&new_user, identity)
        .await
}

async fn update_profile(
    store: &dyn ProvisionStore,
    user_id: i32,
    identity: &ExternalIdentity<'_>,
) -> Result<(), ProvisionError> {
    let Some(profile) = store
        .get_user_profile(user_id)
        .await
        .map_err(|message| ProvisionError::new(SAFE_GET_USER, message))?
    else {
        return Err(ProvisionError::new(SAFE_GET_USER, "user not found"));
    };
    let display_name = (profile.display_name.is_empty() && !identity.display_name.is_empty())
        .then_some(identity.display_name);
    let avatar_url = (profile.avatar_url.is_empty() && !identity.avatar_url.is_empty())
        .then_some(identity.avatar_url);
    if display_name.is_some() || avatar_url.is_some() {
        store
            .update_user_profile(user_id, display_name, avatar_url)
            .await
            .map_err(|message| ProvisionError::new(SAFE_UPDATE_USER, message))?;
    }
    Ok(())
}

/// Production [`ProvisionStore`] over `sg-db`.
pub struct PgProvisionStore {
    pool: PgPool,
    /// `conf.PermissionsUserMapping().BindID`, normalized
    /// (`userpasswd.rs:141-153`).
    permissions_bind_id: String,
    /// `log.securityEventLog.location` is `database`/`all`.
    security_event_logs_in_database: bool,
}

impl PgProvisionStore {
    pub fn new(pool: PgPool) -> Self {
        Self {
            pool,
            permissions_bind_id: "email".to_owned(),
            security_event_logs_in_database: false,
        }
    }

    /// Set the normalized `permissions.userMapping.bindID`.
    pub fn with_permissions_bind_id(mut self, bind_id: impl Into<String>) -> Self {
        let bind_id = bind_id.into();
        self.permissions_bind_id = match bind_id.as_str() {
            "email" | "username" => bind_id,
            _ => "email".to_owned(),
        };
        self
    }

    /// Derive the bind id and `security_event_logs` destination from site
    /// configuration (`conf.PermissionsUserMapping`, `log.securityEventLog`).
    pub fn with_site_configuration(mut self, site: &sg_config::SiteConfiguration) -> Self {
        self.permissions_bind_id = crate::userpasswd::permissions_user_mapping_bind_id(site);
        self.security_event_logs_in_database =
            sg_db::security_event_logs::security_event_logs_in_database(site.raw());
        self
    }

    /// Set whether `security_event_logs` is a destination.
    pub fn with_security_event_logs_in_database(mut self, enabled: bool) -> Self {
        self.security_event_logs_in_database = enabled;
        self
    }
}

#[async_trait]
impl ProvisionStore for PgProvisionStore {
    async fn update_external_account(
        &self,
        identity: &ExternalIdentity<'_>,
    ) -> Result<Option<i32>, String> {
        let spec = AuthAccountSpec {
            service_type: identity.service_type,
            service_id: identity.service_id,
            client_id: identity.client_id,
            account_id: identity.account_id,
        };
        sg_db::user_external_accounts::touch_for_auth(&self.pool, &spec)
            .await
            .map_err(|err| err.to_string())
    }

    async fn find_user_by_verified_email(&self, email: &str) -> Result<Option<i32>, String> {
        Ok(users_db::find_user_by_verified_email(&self.pool, email)
            .await
            .map_err(|err| err.to_string())?
            .map(|user| user.id))
    }

    async fn find_user_by_username(&self, username: &str) -> Result<Option<i32>, String> {
        Ok(users_db::find_user_by_username(&self.pool, username)
            .await
            .map_err(|err| err.to_string())?
            .map(|user| user.id))
    }

    async fn get_user_profile(&self, id: i32) -> Result<Option<UserProfileRow>, String> {
        Ok(users_db::get_profile_by_id(&self.pool, id)
            .await
            .map_err(|err| err.to_string())?
            .map(|(id, display_name, avatar_url)| UserProfileRow {
                id,
                display_name,
                avatar_url,
            }))
    }

    async fn update_user_profile(
        &self,
        id: i32,
        display_name: Option<&str>,
        avatar_url: Option<&str>,
    ) -> Result<(), String> {
        let updated = users_db::update_user_profile(&self.pool, id, display_name, avatar_url)
            .await
            .map_err(|err| err.to_string())?;
        if updated {
            Ok(())
        } else {
            Err("user not found".to_owned())
        }
    }

    async fn create_with_external_account(
        &self,
        new_user: &NewUserRow<'_>,
        identity: &ExternalIdentity<'_>,
    ) -> Result<i32, CreateUserStoreError> {
        let spec = AuthAccountSpec {
            service_type: identity.service_type,
            service_id: identity.service_id,
            client_id: identity.client_id,
            account_id: identity.account_id,
        };
        let user = NewAuthUser {
            username: new_user.username,
            display_name: new_user.display_name,
            avatar_url: new_user.avatar_url,
            email: new_user.email,
            email_is_verified: new_user.email_is_verified,
        };
        let user_id = users_db::create_with_external_account_for_auth(&self.pool, &user, &spec)
            .await
            .map_err(|err| {
                if err.username_exists {
                    CreateUserStoreError::UsernameExists
                } else if err.email_exists {
                    CreateUserStoreError::EmailExists
                } else {
                    CreateUserStoreError::Other(err.message)
                }
            })?;

        // `logAccountCreatedEvent(ctx, ..., createdUser, acct.ServiceType)`
        // (`internal/database/users.go:276-286`). The OAuth signup context
        // carries the provider actor with `UID == 0` (`auth/user.go:150-170`).
        // Go logs and ignores failures.
        let ctx = sg_db::users::DeletionEventContext {
            actor: sg_db::users::BackendActor {
                uid: 0,
                sourcegraph_operator: false,
            },
            version: sg_metrics::VERSION,
            now: sqlx::types::time::OffsetDateTime::now_utc(),
            security_event_logs_in_database: self.security_event_logs_in_database,
        };
        let _ = sg_db::users::log_account_created_event(
            &self.pool,
            user_id,
            false,
            identity.service_type,
            &ctx,
        )
        .await;

        Ok(user_id)
    }

    async fn upsert_external_account(
        &self,
        user_id: i32,
        identity: &ExternalIdentity<'_>,
    ) -> Result<(), String> {
        let spec = AuthAccountSpec {
            service_type: identity.service_type,
            service_id: identity.service_id,
            client_id: identity.client_id,
            account_id: identity.account_id,
        };
        sg_db::user_external_accounts::upsert_for_auth(&self.pool, user_id, &spec)
            .await
            .map_err(|err: DbError| err.message)
    }

    async fn grant_pending_permissions(&self, user_id: i32) -> Result<(), String> {
        sg_db::authz::grant_pending_permissions(
            &self.pool,
            user_id,
            &self.permissions_bind_id,
            sqlx::types::time::OffsetDateTime::now_utc(),
        )
        .await
        .map_err(|err| err.to_string())
    }
}
