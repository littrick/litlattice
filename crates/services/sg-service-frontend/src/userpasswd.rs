#![forbid(unsafe_code)]

//! Built-in account authentication (`cmd/frontend/internal/auth/userpasswd`)
//! and access requests (`cmd/frontend/internal/auth/accessrequest`).
//!
//! The baseline registers nine handlers on the app router:
//! `/-/sign-up`, `/-/request-access`, `/-/site-init`, `/-/sign-in`,
//! `/-/unlock-account`, `/-/unlock-user-account`, `/-/reset-password-init`,
//! `/-/reset-password-code` and `/-/check-username-taken/{username}`. This
//! module ports their form/JSON handling, password validation, account
//! lockout, site initialization and access requests with the baseline's
//! exact status codes and error texts (see
//! `docs/overviews/m9_10c_userpasswd_overview.md`).
//!
//! Everything stateful is injected through [`UserpasswdStore`],
//! [`LockoutStore`], [`PasswordHasher`], [`EmailSender`], [`EventSink`] and
//! the existing [`sg_session`] write path, so the differential harness runs
//! against in-memory fakes. The production adapters ([`PgUserpasswdStore`],
//! [`RedisLockoutStore`], [`BcryptHasher`]) use only the existing tables and
//! Redis key prefixes (`v2:account_lockout:*`, ...).
//!
//! Password hashing matches Go's `bcrypt.GenerateFromPassword(..., 10)`:
//! cost 10 and the `$2a$` version prefix. `bcrypt::verify` accepts every Go
//! hash variant.

use std::sync::{Arc, OnceLock};

use async_trait::async_trait;
use base64::Engine as _;
use bytes::Bytes;
use http::header::{self, HeaderValue};
use http::{Method, Request, Response, StatusCode};
use http_body_util::Full;
use serde::Deserialize as _;
use serde_json::{Map, Value};
use sqlx::PgPool;
use time::OffsetDateTime;
use tonic::body::BoxBody;

use sg_config::SiteConfiguration;
use sg_metrics::prometheus::{IntCounterVec, Opts};
use sg_session::write::{SessionManager, SetCookie, WriteOptions};
use sg_txemail::{Header, Message as TxEmailMessage, SendConfig, SmtpConfig, Templates};
use sg_session::{Actor, GoTime, OsSessionIdSource, SessionIdSource, SessionStore, COOKIE_NAME};
use sg_session::SIGN_OUT_COOKIE;

use crate::user_provisioning::{
    is_suspicious_name, normalize_username, UsernameSuffixSource,
};

/// `security.maxPasswordRunes`.
pub const MAX_PASSWORD_RUNES: usize = 256;
/// The RFC 3696 e-mail length limit (`CheckEmailFormat`).
pub const MAX_EMAIL_LENGTH: usize = 320;
/// `conf.AuthMinPasswordLength`'s default.
pub const DEFAULT_MIN_PASSWORD_LENGTH: i64 = 12;
/// `conf.AuthPasswordResetLinkExpiry`'s default (4 hours, in seconds).
pub const DEFAULT_PASSWORD_LINK_EXPIRY: i64 = 14_400;

/// `userpasswd.HandleSignUp`'s `AllowSignup` rejection body.
pub const SIGNUP_DISABLED: &str =
    "Signup is not enabled (builtin auth provider allowSignup site configuration option)";
/// `userpasswd.unsafeSignUp`'s default 500 body.
pub const SIGNUP_FAILED: &str = "Signup failed unexpectedly.";
/// `userpasswd.CheckEmailFormat`'s too-long prefix.
pub const EMAIL_TOO_LONG_PREFIX: &str = "maximum email length is 320, got";
/// `accessrequest.handleRequestAccess`'s default 500 body.
pub const ACCESS_REQUEST_FAILED: &str = "Request access failed unexpectedly.";

// ---------------------------------------------------------------------------
// Site configuration predicates (`conf` equivalents)
// ---------------------------------------------------------------------------

/// `schema.BuiltinAuthProvider` reduced to the fields the handlers read.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct BuiltinProvider {
    pub allow_signup: bool,
}

/// `userpasswd.GetProviderConfig`: the first `auth.providers[].builtin`, and
/// whether more than one is configured.
pub fn get_provider_config(site: &SiteConfiguration) -> (Option<BuiltinProvider>, bool) {
    let mut builtin = None;
    if let Some(providers) = site.raw().get("auth.providers").and_then(Value::as_array) {
        for provider in providers {
            // `schema.AuthProviders` serializes each entry with a top-level
            // `type` discriminant (e.g. `{"type":"builtin","allowSignup":true}`).
            if provider.get("type").and_then(Value::as_str) != Some("builtin") {
                continue;
            }
            if builtin.is_some() {
                return (builtin, true);
            }
            builtin = Some(BuiltinProvider {
                allow_signup: provider
                    .get("allowSignup")
                    .and_then(Value::as_bool)
                    .unwrap_or(false),
            });
        }
    }
    (builtin, false)
}

/// `userpasswd.handleEnabledCheck`.
pub fn enabled_check(site: &SiteConfiguration) -> Option<Response<BoxBody>> {
    let (provider, multiple) = get_provider_config(site);
    if multiple {
        return Some(http_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "Misconfigured builtin auth provider.",
        ));
    }
    if provider.is_none() {
        return Some(http_error(
            StatusCode::FORBIDDEN,
            "Builtin auth provider is not enabled.",
        ));
    }
    None
}

/// `userpasswd.ResetPasswordEnabled`.
pub fn reset_password_enabled(site: &SiteConfiguration) -> bool {
    let (provider, multiple) = get_provider_config(site);
    provider.is_some() && !multiple
}

/// `conf.CanSendEmail` / `conf.EmailVerificationRequired`: `email.smtp`
/// present.
pub fn can_send_email(site: &SiteConfiguration) -> bool {
    site.raw()
        .get("email.smtp")
        .is_some_and(|value| !value.is_null())
}

/// `conf.PermissionsUserMapping().BindID`: the flat `permissions.userMapping`
/// key. A missing or non-`email`/`username` `bindID` falls back to `email`
/// (`internal/conf/computed.go:1445-1458`). `enabled` is not consulted on the
/// pending-permissions path (`internal/database/authz.go:127-162`).
pub fn permissions_user_mapping_bind_id(site: &SiteConfiguration) -> String {
    let bind_id = site
        .raw()
        .get("permissions.userMapping")
        .and_then(|mapping| mapping.get("bindID"))
        .and_then(Value::as_str)
        .unwrap_or("");
    match bind_id {
        "email" | "username" => bind_id.to_owned(),
        _ => "email".to_owned(),
    }
}

/// `conf.IsAccessRequestEnabled`.
pub fn access_request_enabled(site: &SiteConfiguration) -> bool {
    match site.raw().get("auth.accessRequest") {
        None | Some(Value::Null) => true,
        Some(value) => value
            .get("enabled")
            .and_then(Value::as_bool)
            .unwrap_or(true),
    }
}

/// `conf.AuthMinPasswordLength`.
pub fn auth_min_password_length(site: &SiteConfiguration) -> i64 {
    let value = site
        .raw()
        .get("auth.minPasswordLength")
        .and_then(Value::as_i64)
        .unwrap_or(0);
    if value <= 0 {
        DEFAULT_MIN_PASSWORD_LENGTH
    } else {
        value
    }
}

/// `conf.AuthPasswordResetLinkExpiry`.
pub fn password_reset_link_expiry(site: &SiteConfiguration) -> i64 {
    let value = site
        .raw()
        .get("auth.passwordResetLinkExpiry")
        .and_then(Value::as_i64)
        .unwrap_or(0);
    if value <= 0 {
        DEFAULT_PASSWORD_LINK_EXPIRY
    } else {
        value
    }
}

/// `conf.AuthUnlockAccountLinkExpiry` (minutes; zero when unset).
pub fn unlock_account_link_expiry(site: &SiteConfiguration) -> i64 {
    site.raw()
        .get("auth.unlockAccountLinkExpiry")
        .and_then(Value::as_i64)
        .unwrap_or(0)
}

/// `auth.sessionExpiry`.
pub fn session_expiry(site: &SiteConfiguration) -> Option<String> {
    site.raw()
        .get("auth.sessionExpiry")
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
}

/// `auth.unlockAccountLinkSigningKey`.
pub fn unlock_signing_key(site: &SiteConfiguration) -> String {
    site.raw()
        .get("auth.unlockAccountLinkSigningKey")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_owned()
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PasswordPolicy {
    pub enabled: bool,
    pub number_of_special_characters: i64,
    pub require_at_least_one_number: bool,
    pub require_upper_and_lower_case: bool,
}

impl PasswordPolicy {
    /// `conf.AuthPasswordPolicy`: `auth.passwordPolicy`, falling back to
    /// `experimentalFeatures.passwordPolicy`.
    pub fn from_site(site: &SiteConfiguration) -> Self {
        let raw = site.raw();
        let policy = raw
            .get("auth.passwordPolicy")
            .or_else(|| {
                raw.get("experimentalFeatures")
                    .and_then(|features| features.get("passwordPolicy"))
            });
        let Some(policy) = policy.filter(|value| !value.is_null()) else {
            return Self::default();
        };
        Self {
            enabled: policy
                .get("enabled")
                .and_then(Value::as_bool)
                .unwrap_or(false),
            number_of_special_characters: policy
                .get("numberOfSpecialCharacters")
                .and_then(Value::as_i64)
                .unwrap_or(0),
            require_at_least_one_number: policy
                .get("requireAtLeastOneNumber")
                .and_then(Value::as_bool)
                .unwrap_or(false),
            require_upper_and_lower_case: policy
                .get("requireUpperandLowerCase")
                .and_then(Value::as_bool)
                .unwrap_or(false),
        }
    }
}

/// `security.ValidatePassword` / `database.CheckPassword`.
pub fn validate_password(site: &SiteConfiguration, password: &str) -> Result<(), String> {
    let policy = PasswordPolicy::from_site(site);
    if policy.enabled {
        return validate_password_using_policy(site, &policy, password);
    }
    validate_password_default(site, password)
}

fn validate_password_default(site: &SiteConfiguration, password: &str) -> Result<(), String> {
    if password.is_empty() {
        return Err("Your password may not be empty.".to_owned());
    }
    let length = password.chars().count();
    let min = auth_min_password_length(site);
    if (length as i64) < min || length > MAX_PASSWORD_RUNES {
        return Err(format!(
            "Your password may not be less than {min} or be more than {MAX_PASSWORD_RUNES} characters."
        ));
    }
    Ok(())
}

fn validate_password_using_policy(
    site: &SiteConfiguration,
    policy: &PasswordPolicy,
    password: &str,
) -> Result<(), String> {
    let mut chars = 0i64;
    let mut numbers = false;
    let mut uppercase = false;
    let mut special = 0i64;
    for character in password.chars() {
        if character.is_numeric() {
            numbers = true;
            chars += 1;
        } else if character.is_uppercase() {
            uppercase = true;
            chars += 1;
        } else if character.is_ascii_punctuation() || is_unicode_symbol(character) {
            special += 1;
            chars += 1;
        } else if character.is_alphabetic() || character == ' ' {
            chars += 1;
        }
    }
    if chars == 0 {
        return Err("password empty".to_owned());
    }
    let minimum_length = auth_min_password_length(site);
    if chars < minimum_length {
        return Err(format!(
            "Your password may not be less than {minimum_length} characters."
        ));
    }
    if chars > MAX_PASSWORD_RUNES as i64 {
        return Err(format!(
            "Your password may not be more than {MAX_PASSWORD_RUNES} characters."
        ));
    }
    if policy.require_at_least_one_number && !numbers {
        return Err("Your password must include one number.".to_owned());
    }
    if policy.require_upper_and_lower_case && !uppercase {
        return Err("Your password must include one uppercase letter.".to_owned());
    }
    if policy.number_of_special_characters > 0 && special < policy.number_of_special_characters {
        return Err(format!(
            "Your password must include at least {} special character(s).",
            policy.number_of_special_characters
        ));
    }
    Ok(())
}

/// `unicode.IsSymbol` for the ranges the baseline's policy check can see.
/// (Rust's `char::is_ascii_punctuation` covers `unicode.IsPunct` for ASCII;
/// this covers the common symbol blocks.)
fn is_unicode_symbol(character: char) -> bool {
    matches!(character as u32,
        0x00A2..=0x00A9 | 0x00AC | 0x00AE..=0x00B1 | 0x00B4 | 0x00B6 | 0x00B8
        | 0x00D7 | 0x00F7 | 0x02C2..=0x02C5 | 0x02D2..=0x02DF | 0x02E5..=0x02EB
        | 0x02ED | 0x02EF..=0x02FF | 0x0375 | 0x0384..=0x0385 | 0x03F6
        | 0x0482 | 0x058D..=0x058F | 0x0606..=0x0608 | 0x060B | 0x060E..=0x060F
        | 0x06DE | 0x06E9 | 0x06FD..=0x06FE | 0x07F6 | 0x07FE..=0x07FF
        | 0x09F2..=0x09F3 | 0x09FA..=0x09FB | 0x0AF1 | 0x0B70 | 0x0BF3..=0x0BFA
        | 0x0C7F | 0x0D4F | 0x0D79 | 0x0E3F | 0x0F01..=0x0F03 | 0x0F13
        | 0x0F15..=0x0F17 | 0x0F1A..=0x0F1F | 0x0F34 | 0x0F36 | 0x0F38
        | 0x0FBE..=0x0FC5 | 0x0FC7..=0x0FCC | 0x0FCE..=0x0FCF | 0x0FD5..=0x0FD8
        | 0x109E..=0x109F | 0x1390..=0x1399 | 0x166D | 0x17DB | 0x1940
        | 0x19DE..=0x19FF | 0x1B61..=0x1B6A | 0x1B74..=0x1B7C | 0x1FBD
        | 0x1FBF..=0x1FC1 | 0x1FCD..=0x1FCF | 0x1FDD..=0x1FDF | 0x1FED..=0x1FEF
        | 0x1FFD..=0x1FFE | 0x2044 | 0x2052 | 0x207A..=0x207C | 0x208A..=0x208C
        | 0x20A0..=0x20BF | 0x2100..=0x2101 | 0x2103..=0x2106 | 0x2108..=0x2109
        | 0x2114 | 0x2116..=0x2118 | 0x211E..=0x2123 | 0x2125 | 0x2127
        | 0x2129 | 0x212E | 0x213A..=0x213B | 0x2140..=0x2144 | 0x214A..=0x214D
        | 0x214F | 0x218A..=0x218B | 0x2190..=0x2426 | 0x2440..=0x244A
        | 0x249C..=0x24E9 | 0x2500..=0x2767 | 0x2794..=0x27C4 | 0x27C7..=0x27E5
        | 0x27F0..=0x2982 | 0x2999..=0x29D7 | 0x29DC..=0x29FB | 0x29FE..=0x2B73
        | 0x2B76..=0x2B95 | 0x2B98..=0x2BFF | 0x2CE5..=0x2CEA | 0x2E80..=0x2E99
        | 0x2E9B..=0x2EF3 | 0x2F00..=0x2FD5 | 0x2FF0..=0x2FFB | 0x3004
        | 0x3012..=0x3013 | 0x3020 | 0x3036..=0x3037 | 0x303E..=0x303F
        | 0x309B..=0x309C | 0x3190..=0x3191 | 0x3196..=0x319F | 0x31C0..=0x31E3
        | 0x3200..=0x321E | 0x322A..=0x3247 | 0x3250 | 0x3260..=0x327F
        | 0x328A..=0x32B0 | 0x32C0..=0x33FF | 0x4DC0..=0x4DFF | 0xA490..=0xA4C6
        | 0xA700..=0xA716 | 0xA720..=0xA721 | 0xA789..=0xA78A | 0xA828..=0xA82B
        | 0xA836..=0xA839 | 0xAA77..=0xAA79 | 0xAB5B | 0xFB29 | 0xFBB2..=0xFBC2
        | 0xFD40..=0xFD4F | 0xFDCF | 0xFDFC..=0xFDFF | 0xFE62 | 0xFE64..=0xFE66
        | 0xFE69 | 0xFF04..=0xFF0B | 0xFF1C..=0xFF1E | 0xFF3E | 0xFF40
        | 0xFF5C | 0xFF5E | 0xFFE0..=0xFFE6 | 0xFFE8..=0xFFEE | 0xFFFC..=0xFFFD
        | 0x10137..=0x1013F | 0x10179..=0x10189 | 0x1018C..=0x1018E
        | 0x10190..=0x1019B | 0x101A0 | 0x101D0..=0x101FC | 0x10877..=0x10878
        | 0x10AC8 | 0x1173F | 0x11FD5..=0x11FF1 | 0x16B3C..=0x16B3F
        | 0x16B45 | 0x1BC9C | 0x1D000..=0x1D164 | 0x1D16A..=0x1D16C
        | 0x1D183..=0x1D184 | 0x1D18C..=0x1D1A9 | 0x1D1AE..=0x1D1EA
        | 0x1D200..=0x1D245 | 0x1D300..=0x1D356 | 0x1D6C1 | 0x1D6DB
        | 0x1D6FB | 0x1D715 | 0x1D735 | 0x1D74F | 0x1D76F | 0x1D789
        | 0x1D7A9 | 0x1D7C3 | 0x1D800..=0x1D9FF | 0x1DA37..=0x1DA3A
        | 0x1DA6D..=0x1DA74 | 0x1DA76..=0x1DA83 | 0x1DA85..=0x1DA8B
        | 0x1EEF0..=0x1EEF1 | 0x1F000..=0x1F02B | 0x1F030..=0x1F093
        | 0x1F0A0..=0x1F0AE | 0x1F0B1..=0x1F0BF | 0x1F0C1..=0x1F0CF
        | 0x1F0D1..=0x1F0F5 | 0x1F100..=0x1F1AD | 0x1F1E6..=0x1F202
        | 0x1F210..=0x1F23B | 0x1F240..=0x1F248 | 0x1F250..=0x1F251
        | 0x1F260..=0x1F265 | 0x1F300..=0x1F6D7 | 0x1F6DC..=0x1F6EC
        | 0x1F6F0..=0x1F6FC | 0x1F700..=0x1F776 | 0x1F77B..=0x1F7D9
        | 0x1F7E0..=0x1F7EB | 0x1F7F0 | 0x1F800..=0x1F80B | 0x1F810..=0x1F847
        | 0x1F850..=0x1F859 | 0x1F860..=0x1F887 | 0x1F890..=0x1F8AD
        | 0x1F8B0..=0x1F8B1 | 0x1F900..=0x1FA53 | 0x1FA60..=0x1FA6D
        | 0x1FA70..=0x1FA7C | 0x1FA80..=0x1FA88 | 0x1FA90..=0x1FABD
        | 0x1FABF..=0x1FAC5 | 0x1FACE..=0x1FADB | 0x1FAE0..=0x1FAE8
        | 0x1FAF0..=0x1FAF8 | 0x1FB00..=0x1FB92 | 0x1FB94..=0x1FBCA)
}

/// `userpasswd.CheckEmailFormat`.
///
/// `net/mail.ParseAddress` accepts `addr` and `Name <addr>`; the error texts
/// below are the ones `net/mail` produces for the corpus' inputs
/// (`mail: no address`, `mail: missing '@' or angle-addr`,
/// `mail: no angle-addr`, `mail: expected single address, got ...`,
/// `mail: missing word in phrase: mail: invalid string`).
pub fn check_email_format(email: &str) -> Result<(), String> {
    if email.len() > MAX_EMAIL_LENGTH {
        return Err(format!("{EMAIL_TOO_LONG_PREFIX} {}", email.len()));
    }
    if email.is_empty() {
        return Err("mail: no address".to_owned());
    }
    if email.contains(['\t', '\r', '\n']) {
        return Err("mail: invalid string".to_owned());
    }
    if let Some(start) = email.find('<') {
        let Some(end) = email.rfind('>') else {
            return Err("mail: no angle-addr".to_owned());
        };
        if end < start {
            return Err("mail: no angle-addr".to_owned());
        }
        let trailing = &email[end + 1..];
        if !trailing.trim_matches(' ').is_empty() {
            return Err(format!(
                "mail: expected single address, got {}",
                go_quote(trailing)
            ));
        }
        return check_addr_spec(&email[start + 1..end], true);
    }
    check_addr_spec(email.trim_end_matches(' '), false)
}

fn check_addr_spec(addr: &str, has_angle: bool) -> Result<(), String> {
    if addr.is_empty() {
        return Err("mail: missing '@' or angle-addr".to_owned());
    }
    if let Some(comma) = addr.find(',') {
        return Err(format!(
            "mail: expected single address, got {}",
            go_quote(&addr[comma..])
        ));
    }
    let Some(at) = addr.rfind('@') else {
        return Err("mail: missing '@' or angle-addr".to_owned());
    };
    if addr[..at].contains('@') {
        // `net/mail` parses the first address and reports the remainder.
        return Err(format!(
            "mail: expected single address, got {}",
            go_quote(&addr[at..])
        ));
    }
    let local = &addr[..at];
    let domain = &addr[at + 1..];
    if local.is_empty() {
        return Err("mail: missing word in phrase: mail: invalid string".to_owned());
    }
    if domain.is_empty() {
        return Err("mail: missing '@' or angle-addr".to_owned());
    }
    let quoted_local = local.starts_with('"') && local.ends_with('"') && local.len() > 1;
    if !quoted_local && local.contains(' ') {
        if has_angle {
            return Err("mail: invalid string".to_owned());
        }
        return Err("mail: no angle-addr".to_owned());
    }
    let local_dots = !quoted_local
        && (local.starts_with('.') || local.ends_with('.') || local.contains(".."));
    let domain_dots = domain.starts_with('.') || domain.ends_with('.') || domain.contains("..");
    if local_dots || domain_dots {
        return Err("mail: missing '@' or angle-addr".to_owned());
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Storage seam
// ---------------------------------------------------------------------------

/// One user row the handlers read (`types.User` subset).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UserIdentity {
    pub id: i32,
    pub username: String,
    pub site_admin: bool,
    pub created_at: OffsetDateTime,
}

impl Default for UserIdentity {
    fn default() -> Self {
        Self {
            id: 0,
            username: String::new(),
            site_admin: false,
            created_at: OffsetDateTime::UNIX_EPOCH,
        }
    }
}

/// `database.NewUser` after validation and password hashing.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct NewUserData {
    pub email: String,
    pub username: String,
    /// The bcrypt hash, or `None` when the form password was empty.
    pub password_hash: Option<String>,
    pub fail_if_not_initial_user: bool,
    pub email_is_verified: bool,
    pub email_verification_code: Option<String>,
}

/// `types.AccessRequest`'s form fields.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct AccessRequestData {
    pub name: String,
    pub email: String,
    pub additional_info: String,
}

/// The store error taxonomy the handlers map to status codes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StoreError {
    /// `errcode.IsNotFound`.
    NotFound,
    /// `database.IsUsernameExists`.
    UsernameExists,
    /// `database.IsEmailExists`.
    EmailExists,
    /// `errcode.PresentationMessage`.
    Presentation(String),
    /// `database.IsAccessRequestUserWithEmailExists`.
    UserEmailExists,
    /// `database.IsAccessRequestWithEmailExists`.
    AccessRequestEmailExists,
    /// `database.ErrPasswordResetRateLimit`.
    RateLimit,
    /// Any other database error.
    Other(String),
}

impl StoreError {
    pub fn other(message: impl Into<String>) -> Self {
        StoreError::Other(message.into())
    }
}

/// An e-mail row's verification-cooldown state
/// (`UserEmail.NeedsVerificationCoolDown`).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct LatestVerification {
    pub needs_cool_down: bool,
}

/// All database access the handlers perform. Production:
/// [`PgUserpasswdStore`]; the differential harness supplies a recording fake.
#[async_trait]
pub trait UserpasswdStore: Send + Sync {
    /// `Users().GetByUsername`.
    async fn user_by_username(&self, username: &str) -> Result<Option<UserIdentity>, StoreError>;
    /// `Users().GetByVerifiedEmail`.
    async fn user_by_verified_email(&self, email: &str)
        -> Result<Option<UserIdentity>, StoreError>;
    /// `Users().IsPassword`; `true` when the (verified) password matches.
    async fn is_password(&self, user_id: i32, password: &str) -> Result<bool, StoreError>;
    /// `UserEmails().GetLatestVerificationSentEmail`; `Ok(None)` is
    /// not-found.
    async fn latest_verification_sent_email(
        &self,
        email: &str,
    ) -> Result<Option<LatestVerification>, StoreError>;
    /// `Users().Create`.
    async fn create_user(&self, user: &NewUserData) -> Result<UserIdentity, StoreError>;
    /// `Authz().GrantPendingPermissions(Read, PermRepos)`.
    async fn grant_pending_permissions(&self, user_id: i32) -> Result<(), StoreError>;
    /// `UserEmails().SetLastVerification`.
    async fn set_last_verification(
        &self,
        user_id: i32,
        email: &str,
        code: &str,
    ) -> Result<(), StoreError>;
    /// `UserEmails().GetPrimaryEmail`: `(email, verified)`.
    async fn primary_email(&self, user_id: i32) -> Result<(String, bool), StoreError>;
    /// `Users().GetByID`, reduced to the username the account-update e-mail
    /// needs (`SendUserEmailOnFieldUpdate`).
    async fn username_by_id(&self, user_id: i32) -> Result<Option<String>, StoreError>;
    /// `Users().SetPassword`: `Ok(false)` when the reset code is invalid or
    /// expired.
    async fn set_password(
        &self,
        user_id: i32,
        code: &str,
        password: &str,
    ) -> Result<bool, StoreError>;
    /// `UserEmails().Verify`.
    async fn verify_email(
        &self,
        user_id: i32,
        email: &str,
        code: &str,
    ) -> Result<bool, StoreError>;
    /// `Namespaces().GetByName`; `Ok(false)` is `ErrNamespaceNotFound`.
    async fn namespace_exists(&self, name: &str) -> Result<bool, StoreError>;
    /// `AccessRequests().Create`.
    async fn create_access_request(&self, request: &AccessRequestData)
        -> Result<(), StoreError>;
    /// `auth.CheckCurrentUserIsSiteAdmin`'s database lookup.
    async fn is_site_admin(&self, user_id: i32) -> Result<bool, StoreError>;
    /// `backend.MakePasswordResetURL`.
    async fn make_password_reset_url(
        &self,
        user_id: i32,
        email: &str,
    ) -> Result<String, StoreError>;
}

/// `userpasswd.LockoutStore`.
#[async_trait]
pub trait LockoutStore: Send + Sync {
    async fn is_locked_out(&self, user_id: i32) -> (String, bool);
    async fn increase_failed_attempt(&self, user_id: i32);
    async fn reset(&self, user_id: i32);
    async fn generate_unlock_account_url(&self, user_id: i32)
        -> Result<(String, String), String>;
    async fn verify_unlock_account_token_and_reset(
        &self,
        token: &str,
    ) -> Result<bool, String>;
    async fn send_unlock_account_email(&self, user_id: i32, email: &str)
        -> Result<(), String>;
    async fn unlock_email_sent(&self, user_id: i32) -> bool;
}

/// `database.hashPassword` / `validPassword`.
pub trait PasswordHasher: Send + Sync {
    fn hash(&self, password: &str) -> Result<String, String>;
    fn verify(&self, hash: &str, password: &str) -> bool;
}

/// `txemail.Send`'s ported surface: the metric source and the rendered
/// message. Callers build the template and data exactly like the Go callers
/// (`backend.SendUserEmailVerificationEmail`, `userpasswd.SendResetPasswordURLEmail`,
/// …); the implementation only adds configuration, delivery and metrics.
#[async_trait]
pub trait EmailSender: Send + Sync {
    async fn send(&self, source: &str, message: TxEmailMessage) -> Result<(), String>;
}

/// `SecurityEventLogs().LogSecurityEvent` (and the legacy event's name).
#[async_trait]
pub trait EventSink: Send + Sync {
    async fn security_event(
        &self,
        name: &str,
        url_path: &str,
        user_id: i32,
        anonymous_uid: &str,
    );
}

/// `backend.MakeEmailVerificationCode` (20 random bytes, base64).
pub trait VerificationCodeSource: Send + Sync {
    fn make_code(&self) -> Result<String, String>;
}

/// Production source (`/dev/urandom`).
#[derive(Clone, Copy, Debug, Default)]
pub struct OsVerificationCode;

impl VerificationCodeSource for OsVerificationCode {
    fn make_code(&self) -> Result<String, String> {
        use std::io::Read as _;
        let mut bytes = [0u8; 20];
        let mut file = std::fs::File::open("/dev/urandom").map_err(|err| err.to_string())?;
        file.read_exact(&mut bytes).map_err(|err| err.to_string())?;
        Ok(base64::engine::general_purpose::STANDARD.encode(bytes))
    }
}

/// Production bcrypt hasher (cost 10, `$2a$` version prefix).
#[derive(Clone, Copy, Debug, Default)]
pub struct BcryptHasher;

impl PasswordHasher for BcryptHasher {
    fn hash(&self, password: &str) -> Result<String, String> {
        let parts = bcrypt::hash_with_result(password, 10).map_err(|err| err.to_string())?;
        Ok(parts.format_for_version(bcrypt::Version::TwoA))
    }

    fn verify(&self, hash: &str, password: &str) -> bool {
        bcrypt::verify(password, hash).unwrap_or(false)
    }
}

/// The existing session write path (`sg_session::write::SessionManager`).
#[derive(Clone)]
pub struct SessionDeps {
    pub store: Arc<dyn SessionStore>,
    pub ids: Arc<dyn SessionIdSource>,
    pub cookie_key: Vec<u8>,
    /// `conf.ExternalURLParsed().Scheme == "https"`.
    pub secure: bool,
}

/// Everything the handlers need. Built per site-configuration generation by
/// [`UserpasswdFactory`].
#[derive(Clone)]
pub struct UserpasswdDeps {
    pub site: SiteConfiguration,
    /// `conf.ExternalURLParsed()`: absolute links in transactional e-mails.
    pub external_url: String,
    pub dotcom: bool,
    pub store: Arc<dyn UserpasswdStore>,
    pub lockout: Arc<dyn LockoutStore>,
    pub hasher: Arc<dyn PasswordHasher>,
    pub email: Arc<dyn EmailSender>,
    pub events: Arc<dyn EventSink>,
    pub session: SessionDeps,
    pub suffix: Arc<dyn UsernameSuffixSource>,
    pub verification_codes: Arc<dyn VerificationCodeSource>,
    /// `licensing.GetConfiguredProductLicenseInfo()`'s `IsExpired()`.
    pub license_expired: bool,
}

impl UserpasswdDeps {
    /// Production wiring: Postgres, Redis lockout, bcrypt, Redis sessions and
    /// `/dev/urandom` verification codes. `cache` is the shared Redis cache
    /// pool handle (`redispool.Cache`); `None` disables lockout persistence,
    /// like an unreachable Redis.
    pub fn production(
        pool: PgPool,
        cache: Option<sg_redis::KeyValue>,
        site: &SiteConfiguration,
        external_url: &str,
    ) -> Self {
        let dotcom = std::env::var("SOURCEGRAPHDOTCOM_MODE")
            .map(|value| matches!(value.as_str(), "1" | "t" | "T" | "true" | "TRUE" | "True"))
            .unwrap_or(false);
        let secure = url::Url::parse(external_url)
            .map(|url| url.scheme() == "https")
            .unwrap_or(false);
        let cookie_key = std::env::var("SRC_SESSION_COOKIE_KEY")
            .unwrap_or_default()
            .into_bytes();
        let email: Arc<dyn EmailSender> = Arc::new(TxEmailSender::new(site));
        let store: Arc<dyn UserpasswdStore> =
            Arc::new(PgUserpasswdStore::new(pool, permissions_user_mapping_bind_id(site))
                .with_security_event_logs_in_database(
                    sg_db::security_event_logs::security_event_logs_in_database(site.raw()),
                ));
        let lockout: Arc<dyn LockoutStore> = match cache {
            Some(cache) => Arc::new(RedisLockoutStore::from_env(
                cache,
                site,
                external_url,
                Arc::clone(&email),
            )),
            None => Arc::new(DisabledLockoutStore),
        };
        Self {
            site: site.clone(),
            external_url: external_url.to_owned(),
            dotcom,
            store,
            lockout,
            hasher: Arc::new(BcryptHasher),
            email,
            events: Arc::new(TracingEventSink),
            session: SessionDeps {
                store: Arc::new(crate::session_auth::RedisSessionStore::from_env()),
                ids: Arc::new(OsSessionIdSource),
                cookie_key,
                secure,
            },
            suffix: Arc::new(crate::user_provisioning::OsUsernameSuffix),
            verification_codes: Arc::new(OsVerificationCode),
            license_expired: false,
        }
    }
}

/// `src_email_send{success,email_source}` (`internal/txemail/txemail.go:27`).
fn email_send_metric() -> &'static IntCounterVec {
    static METRIC: OnceLock<IntCounterVec> = OnceLock::new();
    METRIC.get_or_init(|| {
        let metric = IntCounterVec::new(
            Opts::new("src_email_send", "Number of emails sent."),
            &["success", "email_source"],
        )
        .expect("valid email send metric");
        let _ = sg_metrics::registry().register(Box::new(metric.clone()));
        metric
    })
}

/// `conf.Get()`'s e-mail settings as an [`sg_txemail::SendConfig`].
///
/// Ported from `sg-service-worker`'s `code_monitor_email::send_config_from_site`
/// (the worker cannot be a dependency of the frontend). Site configuration
/// stores these under flat keys (`"email.address"`, `"email.smtp"`, …), so the
/// raw JSON object is read directly.
pub fn send_config_from_site(site: &SiteConfiguration) -> SendConfig {
    let raw = site.raw();
    SendConfig {
        email_address: config_string(raw, "email.address"),
        sender_name: config_string(raw, "email.senderName"),
        smtp: raw
            .get("email.smtp")
            .filter(|smtp| smtp.is_object())
            .map(|smtp| SmtpConfig {
                host: config_string(smtp, "host"),
                port: smtp
                    .get("port")
                    .and_then(Value::as_u64)
                    .and_then(|value| u16::try_from(value).ok())
                    .unwrap_or(0),
                username: config_string(smtp, "username"),
                password: config_string(smtp, "password"),
                authentication: config_string(smtp, "authentication"),
                domain: config_string(smtp, "domain"),
                no_verify_tls: smtp
                    .get("noVerifyTLS")
                    .and_then(Value::as_bool)
                    .unwrap_or(false),
                additional_headers: smtp
                    .get("additionalHeaders")
                    .and_then(Value::as_array)
                    .map(|headers| {
                        headers
                            .iter()
                            .filter(|header| header.is_object())
                            .map(|header| Header {
                                key: config_string(header, "key"),
                                value: config_string(header, "value"),
                            })
                            .collect()
                    })
                    .unwrap_or_default(),
            }),
    }
}

fn config_string(object: &Value, key: &str) -> String {
    object
        .get(key)
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_owned()
}

/// `conf.ExternalURLParsed().ResolveReference(reference).String()`.
pub fn resolve_external_url(external_url: &str, reference: &str) -> String {
    match url::Url::parse(external_url) {
        Ok(base) => base
            .join(reference)
            .map(|url| url.to_string())
            .unwrap_or_else(|_| reference.to_owned()),
        Err(_) => reference.to_owned(),
    }
}

/// `conf.ExternalURLParsed().Host` (includes the port, like Go's `URL.Host`).
pub fn external_host(external_url: &str) -> String {
    match url::Url::parse(external_url) {
        Ok(url) => {
            let host = url.host_str().unwrap_or_default();
            match url.port() {
                Some(port) => format!("{host}:{port}"),
                None => host.to_owned(),
            }
        }
        Err(_) => String::new(),
    }
}

/// `userpasswd.formatExpiryTime` (`lockout.go:158`).
fn format_expiry_time(ttl: i64) -> String {
    let minutes = ttl / 60;
    if minutes < 1 {
        format!("{ttl} seconds")
    } else {
        format!("{minutes} minutes")
    }
}

/// `router.Router().Get(router.VerifyEmail).URLPath()` is `/-/verify-email`;
/// the query is `url.Values{code,email}.Encode()` (`code` sorts first).
pub fn verify_email_url(external_url: &str, email: &str, code: &str) -> String {
    let query = format!(
        "code={}&email={}",
        url_query_encode(code),
        url_query_encode(email)
    );
    resolve_external_url(external_url, &format!("/-/verify-email?{query}"))
}

/// `txemail.FromSiteConfigTemplateWithDefault`: an invalid or incomplete
/// `email.templates.{name}` falls back to the default (the validation error is
/// surfaced by site-config problems, not by sending).
pub fn site_email_template(site: &SiteConfiguration, name: &str) -> Option<Templates> {
    let template = site.raw().get("email.templates")?.get(name)?;
    let subject = template.get("subject")?.as_str()?;
    let html = template.get("html")?.as_str()?;
    if subject.is_empty() || html.is_empty() {
        return None;
    }
    let templates = Templates {
        subject: subject.to_owned(),
        html: html.to_owned(),
        text: template
            .get("text")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_owned(),
    };
    sg_txemail::parse_template(&templates).ok().map(|_| templates)
}

/// `backend.verifyEmailTemplates` (`internal/backend/user_emails.go:583`).
pub fn verify_email_templates() -> Templates {
    Templates {
        subject: "Verify your email on Sourcegraph ({{.Host}})".to_owned(),
        text: r#"Hi {{.Username}},

Please verify your email address on Sourcegraph ({{.Host}}) by clicking this link:

{{.URL}}
"#
        .to_owned(),
        html: r#"<p>Hi <a>{{.Username}},</a></p>

<p>Please verify your email address on Sourcegraph ({{.Host}}) by clicking this link:</p>

<p><strong><a href="{{.URL}}">Verify email address</a></p>
"#
        .to_owned(),
    }
}

/// `userpasswd.defaultResetPasswordEmailTemplates` (`template.go:32`), with
/// `email.templates.resetPassword` overriding when valid.
pub fn reset_password_templates(site: &SiteConfiguration) -> Templates {
    site_email_template(site, "resetPassword").unwrap_or_else(default_reset_password_templates)
}

fn default_reset_password_templates() -> Templates {
    Templates {
        subject: "Reset your Sourcegraph password ({{.Host}})".to_owned(),
        text: r#"
Somebody (likely you) requested a password reset for the user {{.Username}} on Sourcegraph ({{.Host}}).

To reset the password for {{.Username}} on Sourcegraph, follow this link:

  {{.URL}}
"#
        .to_owned(),
        html: r#"
<p>
  Somebody (likely you) requested a password reset for <strong>{{.Username}}</strong>
  on Sourcegraph ({{.Host}}).
</p>

<p><strong><a href="{{.URL}}">Reset password for {{.Username}}</a></strong></p>
"#
        .to_owned(),
    }
}

/// `userpasswd.defaultSetPasswordEmailTemplate` (`template.go:14`), with
/// `email.templates.setPassword` overriding when valid. The Rust port has no
/// set-password handler yet; the template is ready for it.
pub fn set_password_templates(site: &SiteConfiguration) -> Templates {
    site_email_template(site, "setPassword").unwrap_or_else(default_set_password_template)
}

fn default_set_password_template() -> Templates {
    Templates {
        subject: "Set your Sourcegraph password ({{.Host}})".to_owned(),
        text: r#"
Your administrator created an account for you on Sourcegraph ({{.Host}}).

To set the password for {{.Username}} on Sourcegraph, follow this link:

  {{.URL}}
"#
        .to_owned(),
        html: r#"
<p>
  Your administrator created an account for you on Sourcegraph ({{.Host}}).
</p>

<p><strong><a href="{{.URL}}">Set password for {{.Username}}</a></strong></p>
"#
        .to_owned(),
    }
}

/// `backend.updateAccountEmailTemplate` (`user_emails.go:392`).
pub fn account_update_templates() -> Templates {
    Templates {
        subject: "Update to your Sourcegraph account ({{.Host}})".to_owned(),
        text: r#"
Hi there! Somebody (likely you) {{.Change}} for the user {{.Username}} on Sourcegraph ({{.Host}}).

If this was you, carry on, and thanks for using Sourcegraph! Otherwise, please change your password immediately.
"#
        .to_owned(),
        html: r#"
<p>
Hi there! Somebody (likely you) {{.Change}} for the user {{.Username}} on Sourcegraph ({{.Host}}).
</p>

<p>If this was you, carry on, and thanks for using Sourcegraph! Otherwise, please change your password immediately.</p>
"#
        .to_owned(),
    }
}

/// `userpasswd.emailTemplates` (`lockout.go:243`).
pub fn unlock_account_templates() -> Templates {
    Templates {
        subject: "Unlock your Sourcegraph Cloud account".to_owned(),
        text: r#"
You are receiving this email because your Sourcegraph account got locked after too many sign in attempts.

Please, visit this link in your browser to unlock the account and try to sign in again: {{.UnlockAccountUrl}}

This link will expire in {{.ExpiryTime}}.

To see our Terms of Service, please visit this link: https://sourcegraph.com/terms
To see our Privacy Policy, please visit this link: https://sourcegraph.com/privacy

Sourcegraph, 981 Mission St, San Francisco, CA 94103, USA
"#
        .to_owned(),
        html: r#"
<html>
<head>
  <meta name="color-scheme" content="light">
  <meta name="supported-color-schemes" content="light">
  <style>
    body { color: #343a4d; background: #fff; padding: 20px; font-size: 16px; font-family: -apple-system,BlinkMacSystemFont,Segoe UI,Roboto,Helvetica Neue,Arial,Noto Sans,sans-serif,Apple Color Emoji,Segoe UI Emoji,Segoe UI Symbol,Noto Color Emoji; }
    .logo { height: 34px; margin-bottom: 15px; }
    a { color: #0b70db; text-decoration: none; background-color: transparent; }
    a:hover { color: #0c7bf0; text-decoration: underline; }
    a.btn { display: inline-block; color: #fff; background-color: #0b70db; padding: 8px 16px; border-radius: 3px; font-weight: 600; }
    a.btn:hover { color: #fff; background-color: #0864c6; text-decoration:none; }
    .smaller { font-size: 14px; }
    small { color: #5e6e8c; font-size: 12px; }
    .mtm { margin-top: 10px; }
    .mtl { margin-top: 20px; }
    .mtxl { margin-top: 30px; }
  </style>
</head>
<body style="font-family: -apple-system,BlinkMacSystemFont,Segoe UI,Roboto,Helvetica Neue,Arial,Noto Sans,sans-serif,Apple Color Emoji,Segoe UI Emoji,Segoe UI Symbol,Noto Color Emoji;">
  <img class="logo" src="https://storage.googleapis.com/sourcegraph-assets/sourcegraph-logo-light-small.png" alt="Sourcegraph logo">
  <p>
  	You are receiving this email because your Sourcegraph account got locked after too many sign in attempts..
  </p>
  <p class="mtxl">
    Please, follow this link in your browser to unlock your account and try to sign in again: <a class="btn mtm" href="{{.UnlockAccountUrl}}">Unlock your Account</a>
  </p>
  <p class="smaller">Or visit this link in your browser: <a href="{{.UnlockAccountUrl}}">{{.UnlockAccountUrl}}</a></p>
  <small>
  <p class="mtl">
    This link will expire in {{.ExpiryTime}}.
  </p>
  <p class="mtl">
    <a href="https://sourcegraph.com/terms">Terms</a>&nbsp;&#8226;&nbsp;
    <a href="https://sourcegraph.com/privacy">Privacy</a>
  </p>
  <p>Sourcegraph, 981 Mission St, San Francisco, CA 94103, USA</p>
  </small>
</body>
</html>
"#
        .to_owned(),
    }
}

/// The production [`EmailSender`]: reads `email.address` / `email.senderName` /
/// `email.smtp` from the site configuration this generation was built with and
/// runs the blocking [`sg_txemail::send`] on `spawn_blocking`, like
/// `txemail.Send`'s `conf.Get()` read at call time.
pub struct TxEmailSender {
    site: SiteConfiguration,
}

impl TxEmailSender {
    pub fn new(site: &SiteConfiguration) -> Self {
        Self { site: site.clone() }
    }
}

#[async_trait]
impl EmailSender for TxEmailSender {
    async fn send(&self, source: &str, message: TxEmailMessage) -> Result<(), String> {
        let config = send_config_from_site(&self.site);
        // `txemail.Send` returns these two configuration errors before the
        // metric's deferred increment, so they are not counted.
        if config.email_address.is_empty() {
            return Err("no \"From\" email address configured (in email.address)".to_owned());
        }
        if config.smtp.is_none() {
            return Err("no SMTP server configured (in email.smtp)".to_owned());
        }

        let outcome = tokio::task::spawn_blocking(move || sg_txemail::send(&config, &message)).await;
        let error = match outcome {
            Ok(Ok(())) => None,
            Ok(Err(error)) => Some(error.to_string()),
            // A panic in the blocking task is not a delivery error the
            // baseline can produce; surface it through the same channel.
            Err(error) => Some(error.to_string()),
        };

        // `strconv.FormatBool(err == nil)` for the `success` label.
        let success = if error.is_none() { "true" } else { "false" };
        email_send_metric()
            .with_label_values(&[success, source])
            .inc();

        match error {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }
}

/// Production [`EventSink`]: log-only (the security-event table writer lives
/// in `sg_db::security_event_logs`; the handlers ignore failures).
pub struct TracingEventSink;

#[async_trait]
impl EventSink for TracingEventSink {
    async fn security_event(
        &self,
        name: &str,
        url_path: &str,
        user_id: i32,
        anonymous_uid: &str,
    ) {
        tracing::info!(name, url_path, user_id, anonymous_uid, "security event");
    }
}

/// Builds [`UserpasswdDeps`] from the current site configuration and the
/// external URL.
pub struct UserpasswdWiring {
    pub deps: Arc<UserpasswdDeps>,
}

pub type UserpasswdFactory =
    Arc<dyn Fn(&SiteConfiguration, &str) -> UserpasswdWiring + Send + Sync>;

// ---------------------------------------------------------------------------
// Responses (`http.Error` / `http.Redirect` / empty 200)
// ---------------------------------------------------------------------------

fn empty_response(status: StatusCode) -> Response<BoxBody> {
    let mut response = Response::new(tonic::body::boxed(Full::new(Bytes::new())));
    *response.status_mut() = status;
    response
}

/// `http.Error`: body `msg + "\n"`, `text/plain; charset=utf-8`, nosniff.
fn http_error(status: StatusCode, message: &str) -> Response<BoxBody> {
    let mut response = crate::middleware::text_response(
        status,
        "text/plain; charset=utf-8",
        format!("{message}\n"),
    );
    response.headers_mut().insert(
        header::X_CONTENT_TYPE_OPTIONS,
        HeaderValue::from_static("nosniff"),
    );
    response
}

fn with_cookies(mut response: Response<BoxBody>, cookies: &[SetCookie]) -> Response<BoxBody> {
    for cookie in cookies {
        if let Ok(value) = HeaderValue::from_str(&cookie.to_header()) {
            response.headers_mut().append(header::SET_COOKIE, value);
        }
    }
    response
}

fn request_cookie<B>(req: &Request<B>, name: &str) -> Option<String> {
    for value in req.headers().get_all(header::COOKIE) {
        let Ok(cookies) = value.to_str() else {
            continue;
        };
        for pair in cookies.split(';') {
            let pair = pair.trim();
            let Some((key, value)) = pair.split_once('=') else {
                continue;
            };
            if key.trim() == name {
                let value = value.trim();
                let value = value
                    .strip_prefix('"')
                    .and_then(|value| value.strip_suffix('"'))
                    .unwrap_or(value);
                return Some(value.to_owned());
            }
        }
    }
    None
}

fn anonymous_uid<B>(req: &Request<B>) -> String {
    request_cookie(req, "sourcegraphAnonymousUid").unwrap_or_default()
}

/// `actor.FromContext`: the chain-resolved actor attached by the middleware
/// (`ChainActor`), or the zero actor.
pub fn request_actor<B>(req: &Request<B>) -> crate::actor_middleware::Actor {
    req.extensions()
        .get::<crate::actor_chain::ChainActor>()
        .map(|chain| chain.0.clone())
        .unwrap_or_default()
}

fn go_quote(value: &str) -> String {
    let mut out = String::from("\"");
    for character in value.chars() {
        match character {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            character if (character as u32) < 0x20 => {
                out.push_str(&format!("\\x{:02x}", character as u32))
            }
            character => out.push(character),
        }
    }
    out.push('"');
    out
}

// ---------------------------------------------------------------------------
// JSON decoding (`encoding/json` semantics)
// ---------------------------------------------------------------------------

fn decode_json(body: Option<&[u8]>) -> Option<Value> {
    let body = body?;
    if body.is_empty() {
        return None;
    }
    let mut deserializer = serde_json::Deserializer::from_slice(body);
    Value::deserialize(&mut deserializer).ok()
}

/// `json.Decoder.Decode` into a struct: a JSON object yields its fields, the
/// literal `null` leaves the zero value, anything else is a decode error.
fn json_fields(value: &Value) -> Option<Option<&Map<String, Value>>> {
    match value {
        Value::Null => Some(None),
        Value::Object(map) => Some(Some(map)),
        _ => None,
    }
}

/// `encoding/json` field lookup: an exact name wins over a case-insensitive
/// one; `null` leaves the zero value.
fn find_field<'a>(
    map: &'a Map<String, Value>,
    name: &str,
) -> Option<&'a Value> {
    if let Some(value) = map.get(name) {
        return Some(value);
    }
    map.iter()
        .find(|(key, _)| key.eq_ignore_ascii_case(name))
        .map(|(_, value)| value)
}

fn string_field(map: Option<&Map<String, Value>>, name: &str) -> Result<String, ()> {
    let Some(map) = map else {
        return Ok(String::new());
    };
    match find_field(map, name) {
        None | Some(Value::Null) => Ok(String::new()),
        Some(Value::String(value)) => Ok(value.clone()),
        Some(_) => Err(()),
    }
}

fn i32_field(map: Option<&Map<String, Value>>, name: &str) -> Result<i32, ()> {
    let Some(map) = map else {
        return Ok(0);
    };
    match find_field(map, name) {
        None | Some(Value::Null) => Ok(0),
        Some(Value::Number(number)) => number
            .as_i64()
            .and_then(|value| i32::try_from(value).ok())
            .ok_or(()),
        Some(_) => Err(()),
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct Credentials {
    email: String,
    username: String,
    password: String,
    anonymous_user_id: String,
    first_source_url: String,
    last_source_url: String,
}

impl Credentials {
    fn decode(body: Option<&[u8]>) -> Option<Self> {
        let value = decode_json(body)?;
        let map = json_fields(&value)?;
        Some(Self {
            email: string_field(map, "email").ok()?,
            username: string_field(map, "username").ok()?,
            password: string_field(map, "password").ok()?,
            anonymous_user_id: string_field(map, "anonymousUserId").ok()?,
            first_source_url: string_field(map, "firstSourceUrl").ok()?,
            last_source_url: string_field(map, "lastSourceUrl").ok()?,
        })
    }
}

// ---------------------------------------------------------------------------
// Session write
// ---------------------------------------------------------------------------

async fn actor_session<B>(
    deps: &UserpasswdDeps,
    req: &Request<B>,
    user: &UserIdentity,
) -> Result<Vec<SetCookie>, String> {
    // `session.SetActorFromUser` refuses non-admins on an expired license.
    if deps.license_expired && !user.site_admin {
        return Err(
            "Sourcegraph license is expired. Only admins are allowed to sign in.".to_owned(),
        );
    }
    let now = OffsetDateTime::now_utc();
    let session_cookie = request_cookie(req, COOKIE_NAME);
    let sign_out_cookie = request_cookie(req, SIGN_OUT_COOKIE).is_some();
    let configured = session_expiry(&deps.site);
    let mut manager = SessionManager::new(
        deps.session.store.as_ref(),
        deps.session.ids.as_ref(),
        &deps.session.cookie_key,
        WriteOptions::new(deps.session.secure),
        now,
        session_cookie.as_deref(),
        sign_out_cookie,
    );
    manager
        .set_actor(
            Some(&Actor {
                uid: user.id,
                ..Actor::default()
            }),
            0,
            configured.as_deref(),
            GoTime(user.created_at),
        )
        .await?;
    Ok(manager.cookies)
}

// ---------------------------------------------------------------------------
// Sign-up / site-init
// ---------------------------------------------------------------------------

/// `HandleSignUp`.
pub async fn handle_sign_up<B>(
    deps: &UserpasswdDeps,
    req: &Request<B>,
    body: Option<&[u8]>,
) -> Response<BoxBody> {
    if let Some(response) = enabled_check(&deps.site) {
        return response;
    }
    let (provider, _) = get_provider_config(&deps.site);
    if !provider.map(|provider| provider.allow_signup).unwrap_or(false) {
        return http_error(StatusCode::NOT_FOUND, SIGNUP_DISABLED);
    }
    handle_sign_up_common(deps, req, body, false).await
}

/// `HandleSiteInit`.
pub async fn handle_site_init<B>(
    deps: &UserpasswdDeps,
    req: &Request<B>,
    body: Option<&[u8]>,
) -> Response<BoxBody> {
    handle_sign_up_common(deps, req, body, true).await
}

async fn handle_sign_up_common<B>(
    deps: &UserpasswdDeps,
    req: &Request<B>,
    body: Option<&[u8]>,
    fail_if_new_user_is_not_initial_site_admin: bool,
) -> Response<BoxBody> {
    if req.method() != Method::POST {
        return http_error(
            StatusCode::BAD_REQUEST,
            &format!("unsupported method {}", req.method()),
        );
    }
    let Some(creds) = Credentials::decode(body) else {
        return http_error(StatusCode::BAD_REQUEST, "could not decode request body");
    };

    // `suspiciousnames.CheckNameAllowedForUserOrOrganization` (dotcom only).
    if deps.dotcom && is_suspicious_name(&creds.username) {
        return http_error(
            StatusCode::BAD_REQUEST,
            &format!("rejected suspicious name {}", go_quote(&creds.username)),
        );
    }
    if let Err(error) = check_email_format(&creds.email) {
        return http_error(StatusCode::BAD_REQUEST, &error);
    }

    let email_is_verified = fail_if_new_user_is_not_initial_site_admin;
    let verification_code = if email_is_verified {
        None
    } else {
        match deps.verification_codes.make_code() {
            Ok(code) => Some(code),
            Err(_) => return http_error(StatusCode::INTERNAL_SERVER_ERROR, SIGNUP_FAILED),
        }
    };

    if can_send_email(&deps.site) && !email_is_verified {
        match deps.store.latest_verification_sent_email(&creds.email).await {
            Ok(Some(latest)) if latest.needs_cool_down => {
                return http_error(
                    StatusCode::TOO_MANY_REQUESTS,
                    "Email address is possibly being abused, please try again later or use a different email address.",
                );
            }
            Ok(_) => {}
            Err(_) => return http_error(StatusCode::INTERNAL_SERVER_ERROR, SIGNUP_FAILED),
        }
    }

    // `CreateInTransaction` runs `security.ValidatePassword` (via
    // `EnforcePasswordLength`) before hashing; a presentation error surfaces
    // as 409 with the validation text.
    if let Err(message) = validate_password(&deps.site, &creds.password) {
        return http_error(StatusCode::CONFLICT, &message);
    }
    let password_hash = match deps.hasher.hash(&creds.password) {
        Ok(hash) => Some(hash),
        Err(_) => return http_error(StatusCode::INTERNAL_SERVER_ERROR, SIGNUP_FAILED),
    };
    let new_user = NewUserData {
        email: creds.email.clone(),
        username: creds.username.clone(),
        password_hash,
        fail_if_not_initial_user: fail_if_new_user_is_not_initial_site_admin,
        email_is_verified,
        email_verification_code: verification_code.clone(),
    };
    let user = match deps.store.create_user(&new_user).await {
        Ok(user) => user,
        Err(error) => {
            let (message, status) = match error {
                StoreError::UsernameExists => (
                    "Username is already in use. Try a different username.".to_owned(),
                    StatusCode::CONFLICT,
                ),
                StoreError::EmailExists => (
                    "Email address is already in use. Try signing into that account instead, or use a different email address.".to_owned(),
                    StatusCode::CONFLICT,
                ),
                StoreError::Presentation(message) => (message, StatusCode::CONFLICT),
                _ => (SIGNUP_FAILED.to_owned(), StatusCode::INTERNAL_SERVER_ERROR),
            };
            return http_error(status, &message);
        }
    };

    if deps.store.grant_pending_permissions(user.id).await.is_err() {
        // The baseline logs and continues.
        tracing::error!(user_id = user.id, "Failed to grant user pending permissions");
    }

    if can_send_email(&deps.site) && !email_is_verified {
        if let Some(code) = verification_code.as_deref() {
            // `backend.SendUserEmailVerificationEmail`.
            let message = TxEmailMessage {
                to: vec![creds.email.clone()],
                reply_to: None,
                message_id: None,
                references: Vec::new(),
                template: verify_email_templates(),
                data: serde_json::json!({
                    "Username": creds.username,
                    "URL": verify_email_url(&deps.external_url, &creds.email, code),
                    "Host": external_host(&deps.external_url),
                }),
            };
            match deps.email.send("user_email_verification", message).await {
                Ok(()) => {
                    if deps
                        .store
                        .set_last_verification(user.id, &creds.email, code)
                        .await
                        .is_err()
                    {
                        tracing::error!(
                            user_id = user.id,
                            "failed to set email last verification sent at"
                        );
                    }
                }
                Err(error) => tracing::error!(
                    error = %error,
                    email = %creds.email,
                    "failed to send email verification (continuing, user's email will be unverified)"
                ),
            }
        }
    }

    match actor_session(deps, req, &user).await {
        Ok(cookies) => with_cookies(empty_response(StatusCode::OK), &cookies),
        Err(error) => http_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            &format!("Could not create new user session: {error}"),
        ),
    }
}

// ---------------------------------------------------------------------------
// Sign-in / lockout
// ---------------------------------------------------------------------------

async fn record_sign_in_event(
    deps: &UserpasswdDeps,
    req: &Request<impl Sized>,
    user: &UserIdentity,
    name: &str,
) {
    deps.events
        .security_event(name, req.uri().path(), user.id, &anonymous_uid(req))
        .await;
}

/// `HandleSignIn`.
pub async fn handle_sign_in<B>(
    deps: &UserpasswdDeps,
    req: &Request<B>,
    body: Option<&[u8]>,
) -> Response<BoxBody> {
    if let Some(response) = enabled_check(&deps.site) {
        return response;
    }

    let mut user = UserIdentity::default();
    // The initial `SignInAttempted` event is recorded before the method check.
    record_sign_in_event(deps, req, &user, "SignInAttempted").await;

    let mut result = "SignInFailed";
    let response;

    if req.method() != Method::POST {
        response = http_error(
            StatusCode::BAD_REQUEST,
            &format!("Unsupported method {}", req.method()),
        );
        return finish_sign_in(deps, req, &user, result, response).await;
    }
    let Some(creds) = Credentials::decode(body) else {
        response = http_error(StatusCode::BAD_REQUEST, "Could not decode request body");
        return finish_sign_in(deps, req, &user, result, response).await;
    };

    // `getByEmailOrUsername`.
    let looked_up = if creds.email.contains('@') {
        deps.store.user_by_verified_email(&creds.email).await
    } else {
        deps.store.user_by_username(&creds.email).await
    };
    match looked_up {
        Ok(Some(found)) => user = found,
        Ok(None) | Err(_) => {
            response = http_error(StatusCode::UNAUTHORIZED, "Authentication failed");
            return finish_sign_in(deps, req, &user, result, response).await;
        }
    }

    let (reason, locked) = deps.lockout.is_locked_out(user.id).await;
    if locked {
        if can_send_email(&deps.site) && !deps.lockout.unlock_email_sent(user.id).await {
            match deps.store.primary_email(user.id).await {
                Ok((recipient, _verified)) => {
                    if let Err(error) = deps
                        .lockout
                        .send_unlock_account_email(user.id, &recipient)
                        .await
                    {
                        tracing::error!(error = %error, user_id = user.id, "Error sending unlock account email");
                    }
                }
                Err(error) => {
                    tracing::error!(error = ?error, user_id = user.id, "Error getting primary email address");
                }
            }
        }
        response = http_error(
            StatusCode::UNPROCESSABLE_ENTITY,
            &format!("Account has been locked out due to {}", go_quote(&reason)),
        );
        return finish_sign_in(deps, req, &user, result, response).await;
    }

    match deps.store.is_password(user.id, &creds.password).await {
        Ok(true) => {}
        Ok(false) => {
            response = http_error(StatusCode::UNAUTHORIZED, "Authentication failed");
            return finish_sign_in(deps, req, &user, result, response).await;
        }
        Err(_) => {
            response = http_error(StatusCode::INTERNAL_SERVER_ERROR, "Error checking password");
            return finish_sign_in(deps, req, &user, result, response).await;
        }
    }

    match actor_session(deps, req, &user).await {
        Ok(cookies) => {
            response = with_cookies(empty_response(StatusCode::OK), &cookies);
            result = "SignInSucceeded";
        }
        Err(error) => {
            response = http_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                &format!("Could not create new user session: {error}"),
            );
        }
    }
    finish_sign_in(deps, req, &user, result, response).await
}

/// The deferred block: security event, then `checkAccountLockout`.
async fn finish_sign_in(
    deps: &UserpasswdDeps,
    req: &Request<impl Sized>,
    user: &UserIdentity,
    result: &str,
    response: Response<BoxBody>,
) -> Response<BoxBody> {
    record_sign_in_event(deps, req, user, result).await;
    if user.id > 0 {
        if result == "SignInSucceeded" {
            deps.lockout.reset(user.id).await;
        } else if result == "SignInFailed" {
            deps.lockout.increase_failed_attempt(user.id).await;
        }
    }
    response
}

// ---------------------------------------------------------------------------
// Unlock handlers
// ---------------------------------------------------------------------------

#[derive(Default)]
struct UnlockAccountInfo {
    token: String,
}

/// `HandleUnlockAccount`.
pub async fn handle_unlock_account<B>(
    deps: &UserpasswdDeps,
    req: &Request<B>,
    body: Option<&[u8]>,
) -> Response<BoxBody> {
    if let Some(response) = enabled_check(&deps.site) {
        return response;
    }
    if req.method() != Method::POST {
        return http_error(
            StatusCode::BAD_REQUEST,
            &format!("Unsupported method {}", req.method()),
        );
    }
    let Some(value) = decode_json(body) else {
        return http_error(StatusCode::BAD_REQUEST, "Could not decode request body");
    };
    let Some(map) = json_fields(&value) else {
        return http_error(StatusCode::BAD_REQUEST, "Could not decode request body");
    };
    let info = match string_field(map, "token") {
        Ok(token) => UnlockAccountInfo { token },
        Err(()) => return http_error(StatusCode::BAD_REQUEST, "Could not decode request body"),
    };
    if info.token.is_empty() {
        return http_error(StatusCode::BAD_REQUEST, "Bad request: missing token");
    }
    match deps
        .lockout
        .verify_unlock_account_token_and_reset(&info.token)
        .await
    {
        Ok(true) => empty_response(StatusCode::OK),
        Ok(false) => http_error(StatusCode::UNAUTHORIZED, "invalid token provided"),
        Err(error) => http_error(StatusCode::UNAUTHORIZED, &error),
    }
}

/// `HandleUnlockUserAccount`.
pub async fn handle_unlock_user_account<B>(
    deps: &UserpasswdDeps,
    req: &Request<B>,
    body: Option<&[u8]>,
) -> Response<BoxBody> {
    let actor = request_actor(req);
    let allowed = if actor.internal {
        // `actor.IsInternal()` bypasses the site-admin check.
        true
    } else if actor.uid != 0 {
        deps.store.is_site_admin(actor.uid).await.unwrap_or(false)
    } else {
        false
    };
    if !allowed {
        return http_error(
            StatusCode::UNAUTHORIZED,
            "Only site admins can unlock user accounts",
        );
    }
    if req.method() != Method::POST {
        return http_error(
            StatusCode::BAD_REQUEST,
            &format!("Unsupported method {}", req.method()),
        );
    }
    let Some(value) = decode_json(body) else {
        return http_error(StatusCode::BAD_REQUEST, "Could not decode request body");
    };
    let Some(map) = json_fields(&value) else {
        return http_error(StatusCode::BAD_REQUEST, "Could not decode request body");
    };
    let username = match string_field(map, "username") {
        Ok(username) => username,
        Err(()) => return http_error(StatusCode::BAD_REQUEST, "Could not decode request body"),
    };
    if username.is_empty() {
        return http_error(StatusCode::BAD_REQUEST, "Bad request: missing username");
    }
    let user = match deps.store.user_by_username(&username).await {
        Ok(Some(user)) => user,
        _ => {
            return http_error(
                StatusCode::NOT_FOUND,
                &format!(
                    "Not found: could not find user with username {}",
                    go_quote(&username)
                ),
            )
        }
    };
    let (_, locked) = deps.lockout.is_locked_out(user.id).await;
    if !locked {
        return http_error(
            StatusCode::BAD_REQUEST,
            &format!("User with username {} is not locked", go_quote(&username)),
        );
    }
    deps.lockout.reset(user.id).await;
    empty_response(StatusCode::OK)
}

// ---------------------------------------------------------------------------
// Reset password
// ---------------------------------------------------------------------------

fn not_authenticated_check<B>(req: &Request<B>) -> Option<Response<BoxBody>> {
    let actor = request_actor(req);
    if actor.is_authenticated() {
        return Some(http_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "Authenticated users may not perform password reset.",
        ));
    }
    None
}

/// `HandleResetPasswordInit`.
pub async fn handle_reset_password_init<B>(
    deps: &UserpasswdDeps,
    req: &Request<B>,
    body: Option<&[u8]>,
) -> Response<BoxBody> {
    if let Some(response) = enabled_check(&deps.site) {
        return response;
    }
    if let Some(response) = not_authenticated_check(req) {
        return response;
    }
    if !can_send_email(&deps.site) {
        return http_error(
            StatusCode::NOT_FOUND,
            "Unable to reset password because email sending is not configured on this site",
        );
    }
    let Some(value) = decode_json(body) else {
        return http_error(
            StatusCode::BAD_REQUEST,
            "Could not decode password reset request body",
        );
    };
    let Some(map) = json_fields(&value) else {
        return http_error(
            StatusCode::BAD_REQUEST,
            "Could not decode password reset request body",
        );
    };
    let email = match string_field(map, "email") {
        Ok(email) => email,
        Err(()) => {
            return http_error(
                StatusCode::BAD_REQUEST,
                "Could not decode password reset request body",
            )
        }
    };
    if email.is_empty() {
        return http_error(
            StatusCode::BAD_REQUEST,
            "No email specified in password reset request",
        );
    }

    let user = match deps.store.user_by_verified_email(&email).await {
        Ok(Some(user)) => user,
        Ok(None) | Err(StoreError::NotFound) => {
            // Hide whether the address exists.
            return empty_response(StatusCode::OK);
        }
        Err(_) => {
            return http_error(StatusCode::INTERNAL_SERVER_ERROR, "Failed to lookup user");
        }
    };
    let reset_url = match deps.store.make_password_reset_url(user.id, &email).await {
        Ok(url) => url,
        Err(StoreError::RateLimit) => {
            return http_error(
                StatusCode::TOO_MANY_REQUESTS,
                "Too many password reset requests. Try again in a few minutes.",
            )
        }
        Err(_) => return http_error(StatusCode::BAD_REQUEST, "Could not reset password"),
    };
    // `userpasswd.SendResetPasswordURLEmail`.
    let message = TxEmailMessage {
        to: vec![email.clone()],
        reply_to: None,
        message_id: None,
        references: Vec::new(),
        template: reset_password_templates(&deps.site),
        data: serde_json::json!({
            "Username": user.username.clone(),
            "URL": resolve_external_url(&deps.external_url, &reset_url),
            "Host": external_host(&deps.external_url),
        }),
    };
    if let Err(error) = deps.email.send("password_reset", message).await {
        tracing::error!(error = %error, "Could not send reset password email");
        return http_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "Could not send reset password email",
        );
    }
    // `database.LogPasswordEvent` passes a fixed empty anonymous ID (it does
    // not read the request cookie).
    deps.events
        .security_event("PasswordResetRequested", req.uri().path(), user.id, "")
        .await;
    empty_response(StatusCode::OK)
}

/// `HandleResetPasswordCode`.
pub async fn handle_reset_password_code<B>(
    deps: &UserpasswdDeps,
    req: &Request<B>,
    body: Option<&[u8]>,
) -> Response<BoxBody> {
    if let Some(response) = enabled_check(&deps.site) {
        return response;
    }
    if let Some(response) = not_authenticated_check(req) {
        return response;
    }
    let Some(value) = decode_json(body) else {
        return http_error(
            StatusCode::BAD_GATEWAY,
            "Password reset with code: could not decode request body",
        );
    };
    let Some(map) = json_fields(&value) else {
        return http_error(
            StatusCode::BAD_GATEWAY,
            "Password reset with code: could not decode request body",
        );
    };
    let (user_id, code, email, email_verify_code, password) = match (
        i32_field(map, "userID"),
        string_field(map, "code"),
        string_field(map, "email"),
        string_field(map, "emailVerifyCode"),
        string_field(map, "password"),
    ) {
        (Ok(user_id), Ok(code), Ok(email), Ok(email_verify_code), Ok(password)) => {
            (user_id, code, email, email_verify_code, password)
        }
        _ => {
            return http_error(
                StatusCode::BAD_GATEWAY,
                "Password reset with code: could not decode request body",
            )
        }
    };
    let verify_email = !email.is_empty() && !email_verify_code.is_empty();

    if let Err(error) = validate_password(&deps.site, &password) {
        return http_error(StatusCode::BAD_REQUEST, &error);
    }

    match deps.store.set_password(user_id, &code, &password).await {
        Ok(true) => {}
        Ok(false) => {
            return http_error(
                StatusCode::UNAUTHORIZED,
                "Password reset code was invalid or expired.",
            )
        }
        Err(_) => return http_error(StatusCode::INTERNAL_SERVER_ERROR, "Unexpected error"),
    }

    // `database.LogPasswordEvent` passes a fixed empty anonymous ID.
    deps.events
        .security_event("PasswordChanged", req.uri().path(), user_id, "")
        .await;

    if verify_email {
        match deps.store.verify_email(user_id, &email, &email_verify_code).await {
            Ok(true) => {
                deps.events
                    .security_event("EmailVerified", req.uri().path(), user_id, &anonymous_uid(req))
                    .await;
            }
            Ok(false) => tracing::warn!("got invalid email verification code"),
            Err(error) => tracing::error!(error = ?error, "failed to verify email"),
        }
    }

    if can_send_email(&deps.site) {
        // `backend.SendUserEmailOnFieldUpdate` only sends to a verified
        // primary address (`loadUserForEmail`).
        if let Ok((primary, verified)) = deps.store.primary_email(user_id).await {
            if verified {
                match deps.store.username_by_id(user_id).await {
                    Ok(Some(username)) => {
                        let message = TxEmailMessage {
                            to: vec![primary.clone()],
                            reply_to: None,
                            message_id: None,
                            references: Vec::new(),
                            template: account_update_templates(),
                            data: serde_json::json!({
                                "Email": primary,
                                "Change": "reset the password",
                                "Username": username,
                                "Host": external_host(&deps.external_url),
                            }),
                        };
                        if let Err(error) =
                            deps.email.send("user_account_update", message).await
                        {
                            tracing::warn!(error = %error, "Failed to send email to inform user of password reset");
                        }
                    }
                    Ok(None) => tracing::warn!(
                        "Failed to send email to inform user of password reset: user not found"
                    ),
                    Err(error) => tracing::warn!(
                        error = ?error,
                        "Failed to send email to inform user of password reset"
                    ),
                }
            }
        }
    }

    empty_response(StatusCode::OK)
}

// ---------------------------------------------------------------------------
// Check username / access request
// ---------------------------------------------------------------------------

/// `HandleCheckUsernameTaken`; `username` is the gorilla route variable.
pub async fn handle_check_username_taken(
    deps: &UserpasswdDeps,
    username: &str,
) -> Response<BoxBody> {
    let normalized =
        match normalize_username(username, deps.suffix.as_ref(), deps.dotcom) {
            Ok(name) => name,
            Err(_) => return empty_response(StatusCode::BAD_REQUEST),
        };
    match deps.store.namespace_exists(&normalized).await {
        Ok(false) => empty_response(StatusCode::NOT_FOUND),
        Ok(true) => empty_response(StatusCode::OK),
        Err(_) => http_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "Error checking username uniqueness",
        ),
    }
}

/// `accessrequest.HandleRequestAccess`.
pub async fn handle_request_access<B>(
    deps: &UserpasswdDeps,
    _req: &Request<B>,
    body: Option<&[u8]>,
) -> Response<BoxBody> {
    if !access_request_enabled(&deps.site) {
        return http_error(
            StatusCode::FORBIDDEN,
            "experimental feature accessRequests is disabled, but received request",
        );
    }
    let (provider, _) = get_provider_config(&deps.site);
    if provider
        .map(|provider| provider.allow_signup)
        .unwrap_or(false)
    {
        return http_error(StatusCode::CONFLICT, "Use sign up instead.");
    }

    let Some(value) = decode_json(body) else {
        return http_error(StatusCode::BAD_REQUEST, "could not decode request body");
    };
    let Some(map) = json_fields(&value) else {
        return http_error(StatusCode::BAD_REQUEST, "could not decode request body");
    };
    let (name, email, additional_info) = match (
        string_field(map, "name"),
        string_field(map, "email"),
        string_field(map, "additionalInfo"),
    ) {
        (Ok(name), Ok(email), Ok(additional_info)) => (name, email, additional_info),
        _ => return http_error(StatusCode::BAD_REQUEST, "could not decode request body"),
    };

    if let Err(error) = check_email_format(&email) {
        return http_error(StatusCode::UNPROCESSABLE_ENTITY, &error);
    }

    let request = AccessRequestData {
        name,
        email,
        additional_info,
    };
    match deps.store.create_access_request(&request).await {
        Ok(()) => empty_response(StatusCode::CREATED),
        Err(StoreError::UserEmailExists) | Err(StoreError::AccessRequestEmailExists) => {
            // Do not leak that the address is already in use.
            empty_response(StatusCode::CREATED)
        }
        Err(StoreError::Presentation(message)) => http_error(StatusCode::CONFLICT, &message),
        Err(_) => http_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            ACCESS_REQUEST_FAILED,
        ),
    }
}

// ---------------------------------------------------------------------------
// Production store
// ---------------------------------------------------------------------------

/// Production [`UserpasswdStore`] over the existing tables.
pub struct PgUserpasswdStore {
    pool: PgPool,
    /// `conf.PermissionsUserMapping().BindID`, normalized at construction.
    permissions_bind_id: String,
    /// `log.securityEventLog.location` includes the database
    /// (`database`/`all`); gates the `AccountCreated` security event.
    security_event_logs_in_database: bool,
}

impl PgUserpasswdStore {
    pub fn new(pool: PgPool, permissions_bind_id: impl Into<String>) -> Self {
        Self {
            pool,
            permissions_bind_id: permissions_bind_id.into(),
            security_event_logs_in_database: false,
        }
    }

    /// Derive the pending-permissions bind id and the
    /// `security_event_logs` destination from site configuration
    /// (`conf.PermissionsUserMapping`, `log.securityEventLog`).
    pub fn with_site_configuration(mut self, site: &SiteConfiguration) -> Self {
        self.permissions_bind_id = permissions_user_mapping_bind_id(site);
        self.security_event_logs_in_database =
            sg_db::security_event_logs::security_event_logs_in_database(site.raw());
        self
    }

    /// Set whether `log.securityEventLog.location` includes the database.
    pub fn with_security_event_logs_in_database(mut self, enabled: bool) -> Self {
        self.security_event_logs_in_database = enabled;
        self
    }
}

fn db_error(error: sqlx::Error) -> StoreError {
    StoreError::Other(error.to_string())
}

fn constraint_error(error: &sqlx::Error) -> Option<&str> {
    match error {
        sqlx::Error::Database(database) => database.constraint(),
        _ => None,
    }
}

#[async_trait]
impl UserpasswdStore for PgUserpasswdStore {
    async fn user_by_username(&self, username: &str) -> Result<Option<UserIdentity>, StoreError> {
        Ok(sg_db::users::find_user_by_username(&self.pool, username)
            .await
            .map_err(db_error)?
            .map(|user| UserIdentity {
                id: user.id,
                username: user.username,
                site_admin: user.site_admin,
                created_at: user.created_at,
            }))
    }

    async fn user_by_verified_email(
        &self,
        email: &str,
    ) -> Result<Option<UserIdentity>, StoreError> {
        Ok(sg_db::users::find_user_by_verified_email(&self.pool, email)
            .await
            .map_err(db_error)?
            .map(|user| UserIdentity {
                id: user.id,
                username: user.username,
                site_admin: user.site_admin,
                created_at: user.created_at,
            }))
    }

    async fn is_password(&self, user_id: i32, password: &str) -> Result<bool, StoreError> {
        let hash: Option<String> = sqlx::query_scalar(
            "SELECT passwd FROM users WHERE deleted_at IS NULL AND id = $1",
        )
        .bind(user_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(db_error)?
        .flatten();
        match hash {
            Some(hash) => Ok(BcryptHasher.verify(&hash, password)),
            None => Ok(false),
        }
    }

    async fn latest_verification_sent_email(
        &self,
        email: &str,
    ) -> Result<Option<LatestVerification>, StoreError> {
        let sent_at: Option<OffsetDateTime> = sqlx::query_scalar(
            "SELECT last_verification_sent_at FROM user_emails \
             WHERE email = $1 AND last_verification_sent_at IS NOT NULL \
             ORDER BY last_verification_sent_at DESC LIMIT 1",
        )
        .bind(email)
        .fetch_optional(&self.pool)
        .await
        .map_err(db_error)?;
        Ok(sent_at.map(|sent_at| LatestVerification {
            needs_cool_down: OffsetDateTime::now_utc() < sent_at + time::Duration::seconds(30),
        }))
    }

    async fn create_user(&self, user: &NewUserData) -> Result<UserIdentity, StoreError> {
        let mut tx = self.pool.begin().await.map_err(db_error)?;

        // `GlobalStateWith(tx).EnsureInitialized`: normalize/prune the row,
        // read `alreadyInitialized`, then mark the site initialized.
        sqlx::query(
            "UPDATE global_state SET initialized = \
                 (SELECT coalesce(bool_or(gs.initialized), false) FROM global_state gs)",
        )
        .execute(&mut *tx)
        .await
        .map_err(db_error)?;
        sqlx::query(
            "DELETE FROM global_state WHERE site_id NOT IN \
                 (SELECT site_id FROM global_state ORDER BY ctid LIMIT 1)",
        )
        .execute(&mut *tx)
        .await
        .map_err(db_error)?;
        sqlx::query(
            "INSERT INTO global_state (site_id, initialized) \
             SELECT gen_random_uuid(), EXISTS (SELECT 1 FROM users WHERE deleted_at IS NULL) \
             WHERE NOT EXISTS (SELECT 1 FROM global_state)",
        )
        .execute(&mut *tx)
        .await
        .map_err(db_error)?;
        let already_initialized: bool =
            sqlx::query_scalar("SELECT coalesce(bool_or(gs.initialized), false) FROM global_state gs")
                .fetch_one(&mut *tx)
                .await
                .map_err(db_error)?;
        sqlx::query("UPDATE global_state SET initialized = true")
            .execute(&mut *tx)
            .await
            .map_err(db_error)?;
        if user.fail_if_not_initial_user && already_initialized {
            return Err(StoreError::Other(
                "cannot create user: site_already_initialized".to_owned(),
            ));
        }

        let row = sqlx::query(
            "INSERT INTO users(username, display_name, avatar_url, created_at, updated_at, \
             passwd, invalidated_sessions_at, tos_accepted, site_admin) \
             VALUES($1, '', NULL, now(), now(), $2, now(), true, \
             $3 AND NOT EXISTS(SELECT 1 FROM users)) \
             RETURNING id, site_admin, created_at",
        )
        .bind(&user.username)
        .bind(user.password_hash.as_deref())
        .bind(!already_initialized)
        .fetch_one(&mut *tx)
        .await
        .map_err(|error| match constraint_error(&error) {
            Some("users_username") => StoreError::UsernameExists,
            _ => db_error(error),
        })?;
        let id: i32 = sqlx::Row::get(&row, "id");
        let site_admin: bool = sqlx::Row::get(&row, "site_admin");
        let created_at: OffsetDateTime = sqlx::Row::get(&row, "created_at");
        if user.fail_if_not_initial_user && !site_admin {
            return Err(StoreError::Other(
                "cannot create user: initial_site_admin_must_be_first_user".to_owned(),
            ));
        }

        sqlx::query("INSERT INTO names(name, user_id) VALUES($1, $2)")
            .bind(&user.username)
            .bind(id)
            .execute(&mut *tx)
            .await
            .map_err(|error| match constraint_error(&error) {
                Some("names_name_key") | Some("names_name") => StoreError::UsernameExists,
                _ => db_error(error),
            })?;

        if !user.email.is_empty() {
            let exists: bool = sqlx::query_scalar(
                "SELECT EXISTS (SELECT 1 FROM user_emails WHERE email = $1 AND verified_at IS NOT NULL)",
            )
            .bind(&user.email)
            .fetch_one(&mut *tx)
            .await
            .map_err(db_error)?;
            if exists {
                return Err(StoreError::EmailExists);
            }
            let result = if user.email_is_verified {
                sqlx::query(
                    "INSERT INTO user_emails(user_id, email, verified_at, is_primary) \
                     VALUES($1, $2, now(), true)",
                )
                .bind(id)
                .bind(&user.email)
                .execute(&mut *tx)
                .await
            } else {
                sqlx::query(
                    "INSERT INTO user_emails(user_id, email, verification_code, is_primary) \
                     VALUES($1, $2, $3, true)",
                )
                .bind(id)
                .bind(&user.email)
                .bind(user.email_verification_code.as_deref().unwrap_or_default())
                .execute(&mut *tx)
                .await
            };
            result.map_err(|error| match constraint_error(&error) {
                Some("user_emails_unique_verified_email") => StoreError::EmailExists,
                _ => db_error(error),
            })?;
        }

        // `CreateInTransaction` assigns the system roles in the same
        // transaction (`internal/database/users.go:432-447`): `USER` for
        // everyone, `SITE_ADMINISTRATOR` when the inserted user is a site
        // admin.
        sg_db::users::assign_new_user_system_roles(&mut *tx, id, site_admin)
            .await
            .map_err(db_error)?;

        tx.commit().await.map_err(db_error)?;

        // `logAccountCreatedEvent(ctx, ..., newUser, "")`
        // (`internal/database/users.go:261-265,481-517`). The sign-up actor is
        // anonymous (`UID == 0`); failures are logged only.
        let ctx = sg_db::users::DeletionEventContext {
            actor: sg_db::users::BackendActor {
                uid: 0,
                sourcegraph_operator: false,
            },
            version: sg_metrics::VERSION,
            now: OffsetDateTime::now_utc(),
            security_event_logs_in_database: self.security_event_logs_in_database,
        };
        let _ = sg_db::users::log_account_created_event(&self.pool, id, site_admin, "", &ctx).await;

        Ok(UserIdentity {
            id,
            username: user.username.clone(),
            site_admin,
            created_at,
        })
    }

    async fn grant_pending_permissions(&self, user_id: i32) -> Result<(), StoreError> {
        // `db.Authz().GrantPendingPermissions(ctx, {UserID, Perm: authz.Read,
        // Type: authz.PermRepos})` (`internal/database/authz.go:98`): enumerate
        // the user's live external accounts plus the configured user-mapping
        // identity, then grant each pending set in one transaction. The handler
        // ignores failures and continues (`handlers.go:291-297`).
        sg_db::authz::grant_pending_permissions(
            &self.pool,
            user_id,
            &self.permissions_bind_id,
            OffsetDateTime::now_utc(),
        )
        .await
        .map_err(|error| StoreError::Other(error.to_string()))
    }

    async fn set_last_verification(
        &self,
        user_id: i32,
        email: &str,
        code: &str,
    ) -> Result<(), StoreError> {
        sqlx::query(
            "UPDATE user_emails SET last_verification_sent_at = now(), verification_code = $1 \
             WHERE user_id = $2 AND email = $3",
        )
        .bind(code)
        .bind(user_id)
        .bind(email)
        .execute(&self.pool)
        .await
        .map_err(db_error)?;
        Ok(())
    }

    async fn primary_email(&self, user_id: i32) -> Result<(String, bool), StoreError> {
        let row: Option<(String, bool)> = sqlx::query_as(
            "SELECT email, verified_at IS NOT NULL FROM user_emails \
             WHERE user_id = $1 AND is_primary",
        )
        .bind(user_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(db_error)?;
        row.ok_or(StoreError::NotFound)
    }

    async fn username_by_id(&self, user_id: i32) -> Result<Option<String>, StoreError> {
        sg_db::users::get_username_by_id(&self.pool, user_id)
            .await
            .map_err(db_error)
    }

    async fn set_password(
        &self,
        user_id: i32,
        code: &str,
        password: &str,
    ) -> Result<bool, StoreError> {
        // `CheckPassword` already ran in `handle_reset_password_code` with the
        // handler's site configuration; the store only checks the code.
        let count: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM users WHERE id = $1 AND deleted_at IS NULL \
             AND passwd_reset_code = $2 AND passwd_reset_time + \
             ($3 * interval '1 second') > now()",
        )
        .bind(user_id)
        .bind(code)
        .bind(DEFAULT_PASSWORD_LINK_EXPIRY)
        .fetch_one(&self.pool)
        .await
        .map_err(db_error)?;
        if count > 1 {
            return Err(StoreError::Other(format!(
                "illegal state: found more than one user matching ID {user_id}"
            )));
        }
        if count == 0 {
            return Ok(false);
        }
        let hash = BcryptHasher.hash(password).map_err(StoreError::Other)?;
        sqlx::query(
            "UPDATE users SET passwd_reset_code = NULL, passwd_reset_time = NULL, passwd = $1 \
             WHERE id = $2",
        )
        .bind(hash)
        .bind(user_id)
        .execute(&self.pool)
        .await
        .map_err(db_error)?;
        Ok(true)
    }

    async fn verify_email(
        &self,
        user_id: i32,
        email: &str,
        code: &str,
    ) -> Result<bool, StoreError> {
        let db_code: Option<String> = sqlx::query_scalar(
            "SELECT verification_code FROM user_emails WHERE user_id = $1 AND email = $2",
        )
        .bind(user_id)
        .bind(email)
        .fetch_optional(&self.pool)
        .await
        .map_err(db_error)?
        .flatten();
        let Some(db_code) = db_code else {
            return Err(StoreError::Other("email already verified".to_owned()));
        };
        if db_code != code {
            return Ok(false);
        }
        sqlx::query(
            "UPDATE user_emails SET verification_code = NULL, verified_at = now() \
             WHERE user_id = $1 AND email = $2",
        )
        .bind(user_id)
        .bind(email)
        .execute(&self.pool)
        .await
        .map_err(db_error)?;
        Ok(true)
    }

    async fn namespace_exists(&self, name: &str) -> Result<bool, StoreError> {
        let exists: bool = sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM names WHERE name = $1)")
            .bind(name)
            .fetch_one(&self.pool)
            .await
            .map_err(db_error)?;
        Ok(exists)
    }

    async fn create_access_request(
        &self,
        request: &AccessRequestData,
    ) -> Result<(), StoreError> {
        let exists: bool = sqlx::query_scalar(
            "SELECT EXISTS (SELECT 1 FROM user_emails WHERE email = $1 AND verified_at IS NOT NULL)",
        )
        .bind(&request.email)
        .fetch_one(&self.pool)
        .await
        .map_err(db_error)?;
        if exists {
            return Err(StoreError::UserEmailExists);
        }
        let exists: bool = sqlx::query_scalar(
            "SELECT EXISTS (SELECT 1 FROM access_requests WHERE email = $1)",
        )
        .bind(&request.email)
        .fetch_one(&self.pool)
        .await
        .map_err(db_error)?;
        if exists {
            return Err(StoreError::AccessRequestEmailExists);
        }
        sqlx::query(
            "INSERT INTO access_requests(name, email, additional_info, status) \
             VALUES($1, $2, $3, 'PENDING')",
        )
        .bind(&request.name)
        .bind(&request.email)
        .bind(&request.additional_info)
        .execute(&self.pool)
        .await
        .map_err(db_error)?;
        Ok(())
    }

    async fn is_site_admin(&self, user_id: i32) -> Result<bool, StoreError> {
        Ok(sg_db::users::get_site_admin(&self.pool, user_id)
            .await
            .map_err(db_error)?
            .unwrap_or(false))
    }

    async fn make_password_reset_url(
        &self,
        user_id: i32,
        email: &str,
    ) -> Result<String, StoreError> {
        use base64::Engine as _;
        use std::io::Read as _;
        let mut bytes = [0u8; 40];
        let mut file =
            std::fs::File::open("/dev/urandom").map_err(|err| StoreError::Other(err.to_string()))?;
        file.read_exact(&mut bytes).map_err(|err| StoreError::other(err.to_string()))?;
        let code = base64::engine::general_purpose::STANDARD.encode(bytes);
        let affected = sqlx::query(
            "UPDATE users SET passwd_reset_code = $1, passwd_reset_time = now() \
             WHERE id = $2 AND (passwd_reset_time IS NULL OR \
             passwd_reset_time + interval '1 minute' < now())",
        )
        .bind(&code)
        .bind(user_id)
        .execute(&self.pool)
        .await
        .map_err(db_error)?
        .rows_affected();
        if affected == 0 {
            return Err(StoreError::RateLimit);
        }
        Ok(format!(
            "/password-reset?code={}&email={}&userID={user_id}",
            url_query_encode(&code),
            url_query_encode(email)
        ))
    }
}

fn url_query_encode(value: &str) -> String {
    let mut out = String::new();
    for byte in value.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(byte as char)
            }
            b' ' => out.push('+'),
            _ => out.push_str(&format!("%{byte:02X}")),
        }
    }
    out
}

// ---------------------------------------------------------------------------
// Production lockout store
// ---------------------------------------------------------------------------

/// Redis-backed `userpasswd.LockoutStore`.
pub struct RedisLockoutStore {
    failed_threshold: i64,
    lockout_ttl: i64,
    consecutive_ttl: i64,
    lockouts: sg_redis::KeyValue,
    failed_attempts: sg_redis::KeyValue,
    unlock_token: sg_redis::KeyValue,
    unlock_email_sent: sg_redis::KeyValue,
    signing_key: String,
    external_url: String,
    link_expiry_minutes: i64,
    email: Arc<dyn EmailSender>,
}

impl RedisLockoutStore {
    /// `userpasswd.NewLockoutStoreFromConf` over `redispool.Cache`.
    pub fn from_env(
        cache: sg_redis::KeyValue,
        site: &SiteConfiguration,
        external_url: &str,
        email: Arc<dyn EmailSender>,
    ) -> Self {
        let (threshold, lockout, consecutive) = lockout_config(site);
        Self {
            failed_threshold: threshold,
            lockout_ttl: lockout,
            consecutive_ttl: consecutive,
            lockouts: redis_cache(&cache, "account_lockout"),
            failed_attempts: redis_cache(&cache, "account_failed_attempts"),
            unlock_token: redis_cache(&cache, "account_unlock_token"),
            unlock_email_sent: redis_cache(&cache, "account_lockout_email_sent"),
            signing_key: unlock_signing_key(site),
            external_url: external_url.to_owned(),
            link_expiry_minutes: unlock_account_link_expiry(site),
            email,
        }
    }

    fn effective_ttl(&self, ttl: i64) -> i64 {
        if ttl > self.link_expiry_minutes * 60 {
            self.link_expiry_minutes * 60
        } else {
            ttl
        }
    }

    async fn key_ttl(&self, key: &str) -> Option<i64> {
        match self.lockouts.ttl(key).await {
            Ok(ttl) if ttl >= 0 => Some(ttl),
            _ => None,
        }
    }
}

fn lockout_config(site: &SiteConfiguration) -> (i64, i64, i64) {
    let raw = site.raw();
    let lockout = raw.get("auth.lockout");
    let get = |name: &str, default: i64| {
        lockout
            .and_then(|value| value.get(name))
            .and_then(Value::as_i64)
            .filter(|value| *value > 0)
            .unwrap_or(default)
    };
    (
        get("failedAttemptThreshold", 5),
        get("lockoutPeriod", 1800),
        get("consecutivePeriod", 3600),
    )
}

/// `rcache.NewWithTTL(redispool.Cache, prefix, ttl)`'s key namespace
/// (`v2:<prefix>:`). The TTL is applied per write by the callers.
fn redis_cache(cache: &sg_redis::KeyValue, prefix: &str) -> sg_redis::KeyValue {
    cache.with_prefix(&format!("v2:{prefix}"))
}

/// The lockout store used when the Redis cache pool is unreachable at
/// startup (`redispool.Cache` dials lazily, so the baseline would fail per
/// request instead; this degrades to "never locked out").
pub struct DisabledLockoutStore;

#[async_trait]
impl LockoutStore for DisabledLockoutStore {
    async fn is_locked_out(&self, _user_id: i32) -> (String, bool) {
        (String::new(), false)
    }

    async fn increase_failed_attempt(&self, _user_id: i32) {}

    async fn reset(&self, _user_id: i32) {}

    async fn generate_unlock_account_url(
        &self,
        user_id: i32,
    ) -> Result<(String, String), String> {
        Err(format!(
            "user with id {user_id} is not locked out, cannot generate unlock url"
        ))
    }

    async fn verify_unlock_account_token_and_reset(
        &self,
        _token: &str,
    ) -> Result<bool, String> {
        Ok(false)
    }

    async fn send_unlock_account_email(
        &self,
        _user_id: i32,
        _email: &str,
    ) -> Result<(), String> {
        Ok(())
    }

    async fn unlock_email_sent(&self, _user_id: i32) -> bool {
        false
    }
}

#[async_trait]
impl LockoutStore for RedisLockoutStore {
    async fn is_locked_out(&self, user_id: i32) -> (String, bool) {
        let key = user_id.to_string();
        match self.lockouts.get(&key).await {
            Ok(Some(value)) => (String::from_utf8_lossy(&value).into_owned(), true),
            _ => (String::new(), false),
        }
    }

    async fn increase_failed_attempt(&self, user_id: i32) {
        let key = user_id.to_string();
        if let Err(error) = self.failed_attempts.incr(&key).await {
            tracing::warn!(error = %error, "failed to execute redis command");
            return;
        }
        if self.consecutive_ttl > 0 {
            let _ = self.failed_attempts.expire(&key, self.consecutive_ttl).await;
        }
        let count = match self.failed_attempts.get_string(&key).await {
            Ok(Some(value)) => value.parse::<i64>().unwrap_or(0),
            _ => 0,
        };
        if count >= self.failed_threshold {
            let _ = self
                .lockouts
                .set_ex(&key, self.lockout_ttl, "too many failed attempts")
                .await;
        }
    }

    async fn reset(&self, user_id: i32) {
        let key = user_id.to_string();
        let _ = self.lockouts.del(&key).await;
        let _ = self.failed_attempts.del(&key).await;
        let _ = self.unlock_token.del(&key).await;
        let _ = self.unlock_email_sent.del(&key).await;
    }

    async fn generate_unlock_account_url(
        &self,
        user_id: i32,
    ) -> Result<(String, String), String> {
        let key = user_id.to_string();
        let ttl = self.key_ttl(&key).await.ok_or_else(|| {
            format!("user with id {user_id} is not locked out, cannot generate unlock url")
        })?;
        if self.signing_key.is_empty() {
            return Err(
                "signing key not provided, cannot validate JWT on unlock account URL. \
                 Please add \"auth.unlockAccountLinkSigningKey\" to site configuration."
                    .to_owned(),
            );
        }
        let effective_ttl = self.effective_ttl(ttl);
        let expires_at = OffsetDateTime::now_utc().unix_timestamp() + effective_ttl;
        let token = sign_unlock_token(
            &self.signing_key,
            user_id,
            &self.external_url,
            expires_at,
        )?;
        let _ = self
            .unlock_token
            .set_ex(&key, effective_ttl, token.as_bytes())
            .await;
        let url = format!(
            "{}/unlock-account/{}",
            self.external_url.trim_end_matches('/'),
            token
        );
        Ok((url, token))
    }

    async fn verify_unlock_account_token_and_reset(
        &self,
        token: &str,
    ) -> Result<bool, String> {
        if self.signing_key.is_empty() {
            return Err(
                "signing key not provided, cannot validate JWT on account reset URL. \
                 Please add AuthUnlockAccountLinkSigningKey to site configuration."
                    .to_owned(),
            );
        }
        let claims = verify_unlock_token(&self.signing_key, token)?;
        let key = claims.user_id.to_string();
        match self.unlock_token.get(&key).await {
            Ok(Some(stored)) if stored == token.as_bytes() => {
                self.reset(claims.user_id).await;
                Ok(true)
            }
            _ => Err("No previously generated token exists for the specified user".to_owned()),
        }
    }

    async fn send_unlock_account_email(
        &self,
        user_id: i32,
        recipient: &str,
    ) -> Result<(), String> {
        let key = user_id.to_string();
        let Some(ttl) = self.key_ttl(&key).await else {
            return Ok(());
        };
        if self.unlock_email_sent(user_id).await {
            return Ok(());
        }
        let effective_ttl = self.effective_ttl(ttl);
        let (unlock_url, _) = self.generate_unlock_account_url(user_id).await?;
        let message = TxEmailMessage {
            to: vec![recipient.to_owned()],
            reply_to: None,
            message_id: None,
            references: Vec::new(),
            template: unlock_account_templates(),
            data: serde_json::json!({
                "UnlockAccountUrl": unlock_url,
                "ExpiryTime": format_expiry_time(effective_ttl),
            }),
        };
        self.email.send("account_unlock", message).await?;
        let _ = self
            .unlock_email_sent
            .set_ex(&key, effective_ttl, "sent")
            .await;
        Ok(())
    }

    async fn unlock_email_sent(&self, user_id: i32) -> bool {
        matches!(
            self.unlock_email_sent.get(&user_id.to_string()).await,
            Ok(Some(_))
        )
    }
}

#[derive(Debug, Clone, Copy)]
struct UnlockClaims {
    user_id: i32,
}

fn sign_unlock_token(
    signing_key: &str,
    user_id: i32,
    issuer: &str,
    expires_at: i64,
) -> Result<String, String> {
    use base64::engine::general_purpose::URL_SAFE_NO_PAD;
    use hmac::{Hmac, Mac};
    use sha2::Sha512;

    let key = base64::engine::general_purpose::STANDARD
        .decode(signing_key)
        .map_err(|error| error.to_string())?;
    let header = URL_SAFE_NO_PAD.encode(r#"{"alg":"HS512","typ":"JWT"}"#);
    let payload = URL_SAFE_NO_PAD.encode(
        serde_json::json!({
            "user_id": user_id,
            "iss": issuer,
            "sub": user_id.to_string(),
            "exp": expires_at,
        })
        .to_string(),
    );
    let message = format!("{header}.{payload}");
    let mut mac = Hmac::<Sha512>::new_from_slice(&key).map_err(|error| error.to_string())?;
    mac.update(message.as_bytes());
    let signature = URL_SAFE_NO_PAD.encode(mac.finalize().into_bytes());
    Ok(format!("{message}.{signature}"))
}

fn verify_unlock_token(signing_key: &str, token: &str) -> Result<UnlockClaims, String> {
    use base64::engine::general_purpose::URL_SAFE_NO_PAD;
    use hmac::{Hmac, Mac};
    use sha2::Sha512;

    let key = base64::engine::general_purpose::STANDARD
        .decode(signing_key)
        .map_err(|error| error.to_string())?;
    let mut parts = token.split('.');
    let (Some(header), Some(payload), Some(signature), None) =
        (parts.next(), parts.next(), parts.next(), parts.next())
    else {
        return Err("token contains an invalid number of segments".to_owned());
    };
    let header: Value = serde_json::from_slice(
        &URL_SAFE_NO_PAD
            .decode(header)
            .map_err(|error| error.to_string())?,
    )
    .map_err(|error| error.to_string())?;
    if header.get("alg").and_then(Value::as_str) != Some("HS512") {
        return Err("signing method HS512 is invalid".to_owned());
    }
    let mut mac = Hmac::<Sha512>::new_from_slice(&key).map_err(|error| error.to_string())?;
    mac.update(format!("{}.{}", token.split('.').next().unwrap_or(""), payload).as_bytes());
    let expected = mac.finalize().into_bytes();
    let actual = URL_SAFE_NO_PAD
        .decode(signature)
        .map_err(|error| error.to_string())?;
    if expected.as_slice() != actual.as_slice() {
        return Err("signature is invalid".to_owned());
    }
    let claims: Value = serde_json::from_slice(
        &URL_SAFE_NO_PAD
            .decode(payload)
            .map_err(|error| error.to_string())?,
    )
    .map_err(|error| error.to_string())?;
    let user_id = claims
        .get("user_id")
        .and_then(Value::as_i64)
        .and_then(|value| i32::try_from(value).ok())
        .ok_or_else(|| "provided token is invalid or expired".to_owned())?;
    let exp = claims.get("exp").and_then(Value::as_i64).unwrap_or(0);
    if exp != 0 && OffsetDateTime::now_utc().unix_timestamp() > exp {
        return Err("token is expired".to_owned());
    }
    let _ = exp;
    Ok(UnlockClaims { user_id })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn site(json: &str) -> SiteConfiguration {
        SiteConfiguration::parse(json).expect("site config")
    }

    #[test]
    fn provider_config_matches_go() {
        assert_eq!(get_provider_config(&site("{}")), (None, false));
        let (provider, multiple) = get_provider_config(&site(
            r#"{"auth.providers":[{"type":"builtin","allowSignup":true}]}"#,
        ));
        assert_eq!(provider, Some(BuiltinProvider { allow_signup: true }));
        assert!(!multiple);
        let (provider, multiple) = get_provider_config(&site(
            r#"{"auth.providers":[{"type":"builtin"},{"type":"builtin","allowSignup":true}]}"#,
        ));
        assert_eq!(provider, Some(BuiltinProvider::default()));
        assert!(multiple);
        // Non-builtin providers do not count.
        assert_eq!(
            get_provider_config(&site(r#"{"auth.providers":[{"type":"github"}]}"#)),
            (None, false)
        );
    }

    #[test]
    fn enabled_check_texts() {
        assert!(enabled_check(&site("{}")).is_some());
        assert!(enabled_check(&site(
            r#"{"auth.providers":[{"type":"builtin"},{"type":"builtin"}]}"#
        ))
        .is_some());
        assert!(enabled_check(&site(r#"{"auth.providers":[{"type":"builtin"}]}"#)).is_none());
    }

    #[test]
    fn default_password_validation() {
        let config = site("{}");
        assert_eq!(
            validate_password(&config, ""),
            Err("Your password may not be empty.".to_owned())
        );
        assert_eq!(
            validate_password(&config, "short"),
            Err("Your password may not be less than 12 or be more than 256 characters.".to_owned())
        );
        assert_eq!(validate_password(&config, "twelve char!"), Ok(()));
        assert_eq!(
            validate_password(&site(r#"{"auth.minPasswordLength":4}"#), "abcd"),
            Ok(())
        );
        let too_long = "a".repeat(257);
        assert_eq!(
            validate_password(&config, &too_long),
            Err("Your password may not be less than 12 or be more than 256 characters.".to_owned())
        );
    }

    #[test]
    fn email_format_matches_the_documented_errors() {
        assert_eq!(check_email_format("foo@bar.pl"), Ok(()));
        assert_eq!(
            check_email_format("foo@"),
            Err("mail: missing '@' or angle-addr".to_owned())
        );
        assert_eq!(
            check_email_format(""),
            Err("mail: no address".to_owned())
        );
        let long = format!("{}@example.com", "a".repeat(320));
        assert_eq!(
            check_email_format(&long),
            Err(format!("maximum email length is 320, got {}", long.len()))
        );
    }

    #[test]
    fn bcrypt_matches_go_hash_format_and_verifies_go_hashes() {
        let hasher = BcryptHasher;
        let hash = hasher.hash("correct horse battery staple").expect("hash");
        assert!(hash.starts_with("$2a$10$"), "got {hash}");
        assert!(hasher.verify(&hash, "correct horse battery staple"));
        assert!(!hasher.verify(&hash, "wrong"));

        // A hash produced by Go's bcrypt.GenerateFromPassword("password", 10)
        // with golang.org/x/crypto/bcrypt (CompareHashAndPassword returns nil).
        let go_hash = "$2a$10$slYWL.EqOUW822T7afMVb.fbCwZCjje2UfQM/oYFy.x52upXa8OGe";
        assert!(hasher.verify(go_hash, "password"));
    }

    #[test]
    fn go_quote_matches_fmt_percent_q_for_ascii() {
        assert_eq!(go_quote("too many failed attempts"), "\"too many failed attempts\"");
        assert_eq!(go_quote("a\"b\\c"), "\"a\\\"b\\\\c\"");
    }

    #[test]
    fn json_decode_matches_encoding_json() {
        assert!(Credentials::decode(Some(b"")).is_none());
        assert!(Credentials::decode(Some(b"not json")).is_none());
        assert!(Credentials::decode(Some(b"[]")).is_none());
        let creds = Credentials::decode(Some(
            br#"{"Email":"a@b.c","USERNAME":"alice","password":null,"ignored":1}"#,
        ))
        .expect("decoded");
        assert_eq!(creds.email, "a@b.c");
        assert_eq!(creds.username, "alice");
        assert_eq!(creds.password, "");
        assert!(Credentials::decode(Some(br#"{"email":12}"#)).is_none());
    }

    #[test]
    fn lockout_jwt_round_trips() {
        use base64::Engine as _;
        let key = base64::engine::general_purpose::STANDARD.encode(b"m910c-signing-key");
        let token = sign_unlock_token(&key, 7, "https://example.com", 4_000_000_000).unwrap();
        let claims = verify_unlock_token(&key, &token).unwrap();
        assert_eq!(claims.user_id, 7);
        assert!(verify_unlock_token(&key, "bad.token.value").is_err());
        assert!(verify_unlock_token(&key, &token.replace(".", "..")).is_err());
    }

    #[test]
    fn permissions_user_mapping_bind_id_matches_go_normalization() {
        // Missing / unset -> email.
        assert_eq!(permissions_user_mapping_bind_id(&site("{}")), "email");
        assert_eq!(
            permissions_user_mapping_bind_id(&site(r#"{"permissions.userMapping":{}}"#)),
            "email"
        );
        // Invalid values fall back to email regardless of `enabled`.
        assert_eq!(
            permissions_user_mapping_bind_id(&site(
                r#"{"permissions.userMapping":{"enabled":true,"bindID":"login"}}"#
            )),
            "email"
        );
        assert_eq!(
            permissions_user_mapping_bind_id(&site(
                r#"{"permissions.userMapping":{"bindID":12}}"#
            )),
            "email"
        );
        // Valid values are preserved; `enabled:false` is not consulted.
        assert_eq!(
            permissions_user_mapping_bind_id(&site(
                r#"{"permissions.userMapping":{"enabled":false,"bindID":"username"}}"#
            )),
            "username"
        );
        assert_eq!(
            permissions_user_mapping_bind_id(&site(
                r#"{"permissions.userMapping":{"enabled":true,"bindID":"email"}}"#
            )),
            "email"
        );
    }

    #[test]
    fn access_request_config_defaults() {
        assert!(access_request_enabled(&site("{}")));
        assert!(access_request_enabled(&site(r#"{"auth.accessRequest":{}}"#)));
        assert!(!access_request_enabled(&site(
            r#"{"auth.accessRequest":{"enabled":false}}"#
        )));
    }
}
