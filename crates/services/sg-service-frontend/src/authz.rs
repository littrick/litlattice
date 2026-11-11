#![forbid(unsafe_code)]

//! Ports of the `internal/authz` seams and the `cmd/frontend/internal/authz`
//! license gate that the external HTTP chain reads.
//!
//! Scope (M9.8ja): the parts a frontend HTTP request touches without a live
//! code host:
//!
//! * [`parse_authorization_header`] / [`parse_bearer_header`] —
//!   `internal/authz/header.go`, the `token` and `token-sudo` Authorization
//!   schemes used by `httpapi.AccessTokenAuthMiddleware`;
//! * [`Provider`] — `internal/authz.Provider` (`FetchAccount`/
//!   `FetchUserPerms`/`FetchRepoPerms`). The worker's permissions syncer owns
//!   the implementations; this is the frontend-side seam.
//! * [`SubRepoPermissionChecker`], [`ActorPermissions`], [`SubRepoEnabled`] —
//!   `internal/authz/sub_repo_perms.go`, the per-request sub-repository
//!   permission checks;
//! * [`Perms`] — `internal/authz/perms.go`;
//! * [`post_auth_middleware`] — `authz.PostAuthMiddleware`, the license gate
//!   that runs after authentication in `newExternalHTTPHandler`.
//!
//! Everything that reaches a database, code host or the license store is a
//! trait/injected value ([`Provider`], [`SubRepoPermissionChecker`],
//! [`SiteAdminCheck`]) so the decisions are unit-testable and the differential
//! harness can script them.

use std::fmt;

use async_trait::async_trait;
use bytes::Bytes;
use http::{header, HeaderMap, Response, StatusCode};
use http_body_util::Full;
use tonic::body::BoxBody;

use crate::actor_middleware::Actor;

// ---------------------------------------------------------------------------
// Perms (internal/authz/perms.go)
// ---------------------------------------------------------------------------

/// `authz.Perms` bitset.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Perms(pub u32);

/// `authz.None`.
pub const PERMS_NONE: Perms = Perms(0);
/// `authz.Read`.
pub const PERMS_READ: Perms = Perms(1 << 1);
/// `authz.Write`.
pub const PERMS_WRITE: Perms = Perms(1 << 2);

impl Perms {
    /// `(p Perms) Include(other)`: `p & other == other`.
    pub fn include(self, other: Perms) -> bool {
        self.0 & other.0 == other.0
    }

    /// `bits` for bit-or combination: `PERMS_READ | PERMS_WRITE`.
    pub fn or(self, other: Perms) -> Perms {
        Perms(self.0 | other.0)
    }
}

impl std::ops::BitOr for Perms {
    type Output = Perms;
    fn bitor(self, rhs: Perms) -> Perms {
        Perms(self.0 | rhs.0)
    }
}

impl fmt::Display for Perms {
    /// `(p Perms) String()`.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            PERMS_READ => f.write_str("read"),
            PERMS_WRITE => f.write_str("write"),
            Perms(6) => f.write_str("read,write"),
            _ => f.write_str("none"),
        }
    }
}

/// `authz.PermType`.
pub type PermType = &'static str;
/// `authz.PermRepos`.
pub const PERM_REPOS: PermType = "repos";

/// `authz.ErrUnauthenticated` (the `error` value of
/// [`ActorPermissions`] for anonymous actors).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ErrUnauthenticated;
impl fmt::Display for ErrUnauthenticated {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("request is unauthenticated")
    }
}
impl std::error::Error for ErrUnauthenticated {}

// ---------------------------------------------------------------------------
// Provider / sub-repository seams (internal/authz/iface.go, sub_repo_perms.go)
// ---------------------------------------------------------------------------

/// `authz.SubRepoPermissions`: glob paths relative to the repository root.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SubRepoPermissions {
    pub paths: Vec<String>,
}

/// `authz.PathWithIP`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PathWithIP {
    pub path: String,
    pub ip: String,
}

/// `authz.SubRepoPermissionsWithIPs`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SubRepoPermissionsWithIps {
    pub paths: Vec<PathWithIP>,
}

/// `authz.ExternalUserPermissions`. IDs are the external code-host IDs
/// (`extsvc.RepoID` is a string in Go).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ExternalUserPermissions {
    pub exacts: Vec<String>,
    pub include_contains: Vec<String>,
    pub exclude_contains: Vec<String>,
    /// Keyed by external repository ID.
    pub sub_repo_permissions: std::collections::BTreeMap<String, SubRepoPermissionsWithIps>,
}

/// `authz.FetchPermsOptions`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct FetchPermsOptions {
    pub invalidate_caches: bool,
}

/// The identity a [`Provider`] works with (`types.User` reduced to the fields
/// providers read).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ProviderUser {
    pub id: i32,
    pub username: String,
}

/// `extsvc.Account` reduced to the identity provider calls need.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ProviderAccount {
    pub service_type: String,
    pub service_id: String,
    pub account_id: String,
}

/// `extsvc.Repository` + external repo spec for `FetchRepoPerms`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ProviderRepo {
    pub uri: String,
    pub external_repo_id: String,
    pub service_type: String,
    pub service_id: String,
}

/// `authz.Provider` (`internal/authz/iface.go:132`). Implementations live in
/// the worker's permissions syncer; the frontend only consumes the seam.
#[async_trait]
pub trait Provider: Send + Sync {
    /// `FetchAccount`: `Ok(None)` means the provider cannot determine an
    /// account for this user.
    async fn fetch_account(&self, user: &ProviderUser) -> Result<Option<ProviderAccount>, String>;

    /// `FetchUserPerms`: private repositories the account can read.
    async fn fetch_user_perms(
        &self,
        account: &ProviderAccount,
        opts: &FetchPermsOptions,
    ) -> Result<Option<ExternalUserPermissions>, String>;

    /// `FetchRepoPerms`: code-host account IDs with read access to `repo`.
    async fn fetch_repo_perms(
        &self,
        repo: &ProviderRepo,
        opts: &FetchPermsOptions,
    ) -> Result<Vec<String>, String>;
}

/// `authz.RepoContent`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RepoContent {
    pub repo: String,
    pub path: String,
}

/// `authz.FilePermissionFunc`: valid for one request.
pub type FilePermissionFunc = Box<dyn Fn(&str) -> Result<Perms, String> + Send + Sync>;

/// `authz.SubRepoPermissionChecker` (`internal/authz/sub_repo_perms.go:33`).
#[async_trait]
pub trait SubRepoPermissionChecker: Send + Sync {
    async fn permissions(&self, user_id: i32, content: &RepoContent) -> Result<Perms, String>;

    async fn file_permissions_func(
        &self,
        user_id: i32,
        repo: &str,
    ) -> Result<FilePermissionFunc, String>;

    fn enabled(&self) -> bool;

    async fn enabled_for_repo_id(&self, repo_id: i32) -> Result<bool, String>;

    async fn enabled_for_repo(&self, repo: &str) -> Result<bool, String>;
}

/// `authz.noopPermsChecker`: disabled, always `None`.
#[derive(Debug, Clone, Copy, Default)]
pub struct NoopPermsChecker;

#[async_trait]
impl SubRepoPermissionChecker for NoopPermsChecker {
    async fn permissions(&self, _user_id: i32, _content: &RepoContent) -> Result<Perms, String> {
        Ok(PERMS_NONE)
    }

    async fn file_permissions_func(
        &self,
        _user_id: i32,
        _repo: &str,
    ) -> Result<FilePermissionFunc, String> {
        Ok(Box::new(|_path| Ok(PERMS_NONE)))
    }

    fn enabled(&self) -> bool {
        false
    }

    async fn enabled_for_repo_id(&self, _repo_id: i32) -> Result<bool, String> {
        Ok(false)
    }

    async fn enabled_for_repo(&self, _repo: &str) -> Result<bool, String> {
        Ok(false)
    }
}

/// `authz.SubRepoEnabled`: `checker != nil && checker.Enabled()`.
pub fn sub_repo_enabled(checker: Option<&dyn SubRepoPermissionChecker>) -> bool {
    checker.is_some_and(SubRepoPermissionChecker::enabled)
}

/// `authz.SubRepoEnabledForRepoID`.
pub async fn sub_repo_enabled_for_repo_id(
    checker: Option<&dyn SubRepoPermissionChecker>,
    repo_id: i32,
) -> Result<bool, String> {
    match checker {
        None => Ok(false),
        Some(checker) if !checker.enabled() => Ok(false),
        Some(checker) => checker.enabled_for_repo_id(repo_id).await,
    }
}

/// `authz.SubRepoEnabledForRepo`.
pub async fn sub_repo_enabled_for_repo(
    checker: Option<&dyn SubRepoPermissionChecker>,
    repo: &str,
) -> Result<bool, String> {
    match checker {
        None => Ok(false),
        Some(checker) if !checker.enabled() => Ok(false),
        Some(checker) => checker.enabled_for_repo(repo).await,
    }
}

/// `authz.actorSubRepoEnabled` (`internal/authz/sub_repo_perms.go:112`).
pub fn actor_sub_repo_enabled(
    checker: Option<&dyn SubRepoPermissionChecker>,
    actor: &Actor,
) -> Result<bool, ErrUnauthenticated> {
    if !sub_repo_enabled(checker) {
        return Ok(false);
    }
    if actor.is_internal() {
        return Ok(false);
    }
    if !actor.is_authenticated() {
        return Err(ErrUnauthenticated);
    }
    Ok(true)
}

/// `authz.ActorPermissions` (`internal/authz/sub_repo_perms.go:92`).
///
/// The baseline returns `Read` when the checker is disabled and for internal
/// actors (sub-repository checks do not restrict internal traffic).
pub async fn actor_permissions(
    checker: Option<&dyn SubRepoPermissionChecker>,
    actor: &Actor,
    content: &RepoContent,
) -> Result<Perms, ActorPermissionsError> {
    match actor_sub_repo_enabled(checker, actor) {
        Err(error) => return Err(ActorPermissionsError::Unauthenticated(error)),
        Ok(false) => return Ok(PERMS_READ),
        Ok(true) => {}
    }
    let checker = checker.expect("enabled checker");
    checker
        .permissions(actor.uid, content)
        .await
        .map_err(ActorPermissionsError::Message)
}

/// `ActorPermissions`' two error classes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ActorPermissionsError {
    Unauthenticated(ErrUnauthenticated),
    /// `errors.Wrapf(err, "getting actor permissions for actor: %d", a.UID)`.
    Message(String),
}

impl fmt::Display for ActorPermissionsError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ActorPermissionsError::Unauthenticated(error) => write!(f, "{error}"),
            ActorPermissionsError::Message(message) => f.write_str(message),
        }
    }
}
impl std::error::Error for ActorPermissionsError {}

// ---------------------------------------------------------------------------
// Authorization header parsing (internal/authz/header.go)
// ---------------------------------------------------------------------------

pub const SCHEME_TOKEN: &str = "token";
pub const SCHEME_TOKEN_SUDO: &str = "token-sudo";
pub const SCOPE_USER_ALL: &str = "user:all";
pub const SCOPE_SITE_ADMIN_SUDO: &str = "site-admin:sudo";

/// The baseline's three parse-error classes. `UnrecognizedScheme` is
/// `authz.IsUnrecognizedScheme` (`errUnrecognizedScheme`,
/// `errHTTPAuthParamsDuplicateKey`, `errHTTPAuthParamsNoEquals`); the other
/// two are reported as an invalid header by the access-token middleware.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuthzParseError {
    UnrecognizedScheme,
    DuplicateKey,
    NoEquals,
    /// `no token value in the HTTP Authorization request header`.
    NoToken,
    /// Any other `errors.New` message, verbatim.
    Message(String),
}

impl AuthzParseError {
    /// `authz.IsUnrecognizedScheme`.
    pub fn is_unrecognized_scheme(&self) -> bool {
        matches!(
            self,
            AuthzParseError::UnrecognizedScheme
                | AuthzParseError::DuplicateKey
                | AuthzParseError::NoEquals
        )
    }

    /// The Go error text (used for the `http.Error` body where applicable).
    pub fn message(&self) -> String {
        match self {
            AuthzParseError::UnrecognizedScheme => format!(
                "unrecognized HTTP Authorization request header scheme (supported values: {SCHEME_TOKEN:?}, {SCHEME_TOKEN_SUDO:?})"
            ),
            AuthzParseError::DuplicateKey => "duplicate key in HTTP auth-params".to_owned(),
            AuthzParseError::NoEquals => {
                "invalid HTTP auth-params list (parameter has no value)".to_owned()
            }
            AuthzParseError::NoToken => {
                "no token value in the HTTP Authorization request header".to_owned()
            }
            AuthzParseError::Message(message) => message.clone(),
        }
    }
}

impl fmt::Display for AuthzParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message())
    }
}
impl std::error::Error for AuthzParseError {}

/// `authz.ParseAuthorizationHeader` (`internal/authz/header.go:38`).
///
/// `dotcom` switches on `dotcom.SourcegraphDotComMode()`.
pub fn parse_authorization_header(
    header_value: &str,
    dotcom: bool,
) -> Result<(String, String), AuthzParseError> {
    let (scheme, token68, params) = parse_http_credentials(header_value)?;

    if scheme != SCHEME_TOKEN && scheme != SCHEME_TOKEN_SUDO {
        return Err(AuthzParseError::UnrecognizedScheme);
    }

    if !token68.is_empty() {
        return match scheme.as_str() {
            SCHEME_TOKEN => Ok((token68, String::new())),
            // token-sudo with a token68 has no params form.
            _ => Err(AuthzParseError::Message(
                "HTTP Authorization request header value must be of the following form: token=\"TOKEN\",user=\"USERNAME\""
                    .to_owned(),
            )),
        };
    }

    if dotcom && scheme == SCHEME_TOKEN_SUDO {
        return Err(AuthzParseError::Message(
            "use of access tokens with sudo scope is disabled".to_owned(),
        ));
    }

    let token = params
        .iter()
        .find(|(key, _)| key == "token")
        .map(|(_, value)| value.clone())
        .unwrap_or_default();
    if token.is_empty() {
        return Err(AuthzParseError::NoToken);
    }
    let sudo_user = params
        .iter()
        .find(|(key, _)| key == "user")
        .map(|(_, value)| value.clone())
        .unwrap_or_default();
    Ok((token, sudo_user))
}

/// `authz.ParseBearerHeader`.
pub fn parse_bearer_header(auth_header: &str) -> Result<String, AuthzParseError> {
    let Some((kind, value)) = auth_header.split_once(' ') else {
        return Err(AuthzParseError::Message(
            "token type missing in Authorization header".to_owned(),
        ));
    };
    if !kind.eq_ignore_ascii_case("bearer") {
        return Err(AuthzParseError::Message(format!(
            "invalid token type {kind}"
        )));
    }
    Ok(value.to_owned())
}

/// `parseHTTPCredentials`: `(scheme, token68, params)`.
#[allow(clippy::type_complexity)] // Mirrors the Go multiple-return signature.
fn parse_http_credentials(
    credentials: &str,
) -> Result<(String, String, Vec<(String, String)>), AuthzParseError> {
    let (scheme, rest) = match credentials.split_once(' ') {
        Some((scheme, rest)) => (scheme.to_owned(), Some(rest)),
        None => (credentials.to_owned(), None),
    };
    let Some(rest) = rest else {
        return Ok((scheme, String::new(), Vec::new()));
    };

    match parse_http_auth_params(rest) {
        Err(AuthzParseError::NoEquals) => Ok((scheme, rest.to_owned(), Vec::new())),
        Err(error) => Err(error),
        Ok(params) => Ok((scheme, String::new(), params)),
    }
}

/// `parseHTTPAuthParams`: case-insensitive keys, each at most once.
fn parse_http_auth_params(value: &str) -> Result<Vec<(String, String)>, AuthzParseError> {
    let mut params: Vec<(String, String)> = Vec::new();
    for pair in parse_http_header_list(value.trim()) {
        let Some(index) = pair.find('=') else {
            return Err(AuthzParseError::NoEquals);
        };
        if pair.ends_with('=') {
            return Err(AuthzParseError::NoEquals);
        }
        let mut v = &pair[index + 1..];
        if v.starts_with('"') && v.ends_with('"') && v.len() >= 2 {
            v = &v[1..v.len() - 1];
        }
        let key = pair[..index].to_ascii_lowercase();
        if params.iter().any(|(seen, _)| *seen == key) {
            return Err(AuthzParseError::DuplicateKey);
        }
        params.push((key, v.to_owned()));
    }
    Ok(params)
}

/// `parseHTTPHeaderList`: RFC 2068 `#rule` with quote/escape handling.
fn parse_http_header_list(value: &str) -> Vec<String> {
    let mut list = Vec::new();
    let mut buffer = String::new();
    let mut escape = false;
    let mut quote = false;
    for ch in value.chars() {
        match (escape, quote) {
            (true, _) => {
                buffer.push(ch);
                escape = false;
            }
            (false, true) => {
                if ch == '\\' {
                    escape = true;
                } else {
                    if ch == '"' {
                        quote = false;
                    }
                    buffer.push(ch);
                }
            }
            (false, false) => match ch {
                ',' => {
                    list.push(buffer.trim().to_owned());
                    buffer.clear();
                }
                '"' => {
                    quote = true;
                    buffer.push(ch);
                }
                _ => buffer.push(ch),
            },
        }
    }
    let last = buffer.trim();
    if !last.is_empty() {
        list.push(last.to_owned());
    }
    list
}

// ---------------------------------------------------------------------------
// Post-auth license gate (cmd/frontend/internal/authz/middleware.go)
// ---------------------------------------------------------------------------

/// `licensing.GetConfiguredProductLicenseInfo()` reduced to the gate's needs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LicenseState {
    /// An error reading the license key.
    Error,
    /// `info == nil`.
    None,
    /// `info.IsExpired()`.
    Expired,
    Valid,
}

/// `auth.CheckCurrentUserIsSiteAdmin`'s result.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SiteAdminCheck {
    /// `nil`: the current user is a site admin (or the actor is internal).
    IsSiteAdmin,
    /// `auth.ErrMustBeSiteAdmin`.
    MustBeSiteAdmin,
    /// Any other error (`ErrNotAuthenticated`, a database error): the baseline
    /// answers 500.
    Error,
}

/// The body of `enforcement.WriteSubscriptionErrorResponse`. The baseline
/// calls `WriteHeader` first and `Header().Set` second, so the recorded
/// response has no `Content-Type`/`Cache-Control`; the raw body is returned so
/// the harness compares it exactly.
pub fn subscription_error_body(title: &str, message: &str) -> String {
    format!(
        "\n<title>{title} - Sourcegraph</title>\n<style>\n.bg {{\n\tposition: absolute;\n\tuser-select: none;\n\tpointer-events: none;\n\tz-index: -1;\n\ttop: 0;\n\tbottom: 0;\n\tleft: 0;\n\tright: 0;\n\t/* The Sourcegraph logo in SVG. */\n\tbackground-image: url('data:image/svg+xml,<svg xmlns=\"http://www.w3.org/2000/svg\" viewBox=\"0 0 124 127\"><g fill=\"none\" fill-rule=\"evenodd\"><path fill=\"%23F96316\" d=\"M35.942 16.276L63.528 117.12c1.854 6.777 8.85 10.768 15.623 8.912 6.778-1.856 10.765-8.854 8.91-15.63L60.47 9.555C58.615 2.78 51.62-1.212 44.847.645c-6.772 1.853-10.76 8.853-8.905 15.63z\"/><path fill=\"%23B200F8\" d=\"M87.024 15.894L17.944 93.9c-4.66 5.26-4.173 13.303 1.082 17.964 5.255 4.66 13.29 4.174 17.95-1.084l69.08-78.005c4.66-5.26 4.173-13.3-1.082-17.962-5.257-4.664-13.294-4.177-17.95 1.08z\"/><path fill=\"%2300B4F2\" d=\"M8.75 59.12l98.516 32.595c6.667 2.205 13.86-1.414 16.065-8.087 2.21-6.672-1.41-13.868-8.08-16.076L16.738 34.96c-6.67-2.207-13.86 1.412-16.066 8.085-2.204 6.672 1.416 13.87 8.08 16.075z\"/></g></svg>');\n\tbackground-repeat: repeat;\n\tbackground-size: 5rem;\n\topacity: 0.1;\n}}\n\n.msg {{\n\tfont-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Helvetica Neue', Arial, sans-serif;\n\tmax-width: 30rem;\n\tmargin: 20vh auto 0;\n\tborder: solid 2px rgba(255,0,0,0.8);\n\tbackground-color: rgba(255,255,255,0.8);\n\tcolor: rgb(30, 0, 0);\n\tpadding: 1rem 2rem;\n}}\n\nh1 {{\n\tfont-size: 1.5rem;\n}}\n</style>\n<meta name=\"robots\" content=\"noindex\">\n<body>\n<div class=bg></div>\n<div class=msg><h1>{title}</h1><p>{message}</p><p>See <a href=\"https://sourcegraph.com/pricing\">sourcegraph.com</a> for more information.</p></div>\n",
        title = html_escape(title),
        message = html_escape(message),
    )
}

/// `html.EscapeString` (Go escapes `&`, `'`, `<`, `>`, `"`; note `'` becomes
/// `&#39;` and `"` becomes `&#34;`).
pub fn html_escape(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for ch in value.chars() {
        match ch {
            '&' => out.push_str("&amp;"),
            '\'' => out.push_str("&#39;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&#34;"),
            _ => out.push(ch),
        }
    }
    out
}

/// The gate's observable outcome. A subscription-page `Reject` loses the
/// `Content-Type`/`Cache-Control` headers because the baseline calls
/// `WriteHeader` first; the site-admin-check failure uses `http.Error` and
/// therefore keeps `Content-Type`/`X-Content-Type-Options` (`plain`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PostAuthDecision {
    Proceed,
    Reject {
        status: u16,
        body: String,
        plain: bool,
    },
}

/// `authz.PostAuthMiddleware` without the HTTP plumbing.
pub fn post_auth_middleware(
    actor: &Actor,
    license: LicenseState,
    site_admin: SiteAdminCheck,
) -> PostAuthDecision {
    // Not authenticated: let the request through (site admins must be able to
    // sign in and set a license).
    if !actor.is_authenticated() {
        return PostAuthDecision::Proceed;
    }

    // `siteadminOrHandler` runs only when the license is unreadable/expired.
    let siteadmin_or_reject = |status: u16, title: &str, message: &str| {
        match site_admin {
            SiteAdminCheck::IsSiteAdmin => PostAuthDecision::Proceed,
            SiteAdminCheck::MustBeSiteAdmin => PostAuthDecision::Reject {
                status,
                body: subscription_error_body(title, message),
                plain: false,
            },
            SiteAdminCheck::Error => PostAuthDecision::Reject {
                status: StatusCode::INTERNAL_SERVER_ERROR.as_u16(),
                body: "Error checking current user is site admin. Site admins may check the logs for more information.\n"
                    .to_owned(),
                plain: true,
            },
        }
    };

    match license {
        LicenseState::Error => siteadmin_or_reject(
            StatusCode::INTERNAL_SERVER_ERROR.as_u16(),
            "Error reading Sourcegraph license key",
            "Site admins may check the logs for more information. Update the license key in the [**site configuration**](/site-admin/configuration).",
        ),
        LicenseState::Expired => siteadmin_or_reject(
            StatusCode::FORBIDDEN.as_u16(),
            "Sourcegraph license expired",
            "To continue using Sourcegraph, a site admin must renew the Sourcegraph license (or downgrade to only using Sourcegraph Free features). Update the license key in the [**site configuration**](/site-admin/configuration).",
        ),
        LicenseState::None | LicenseState::Valid => PostAuthDecision::Proceed,
    }
}

/// Renders a [`PostAuthDecision`] as an HTTP response.
pub fn post_auth_response(decision: PostAuthDecision) -> Option<Response<BoxBody>> {
    match decision {
        PostAuthDecision::Proceed => None,
        PostAuthDecision::Reject {
            status,
            body,
            plain,
        } => {
            let mut response = Response::new(tonic::body::boxed(Full::new(Bytes::from(body))));
            *response.status_mut() =
                StatusCode::from_u16(status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
            if plain {
                response.headers_mut().insert(
                    header::CONTENT_TYPE,
                    header::HeaderValue::from_static("text/plain; charset=utf-8"),
                );
                response.headers_mut().insert(
                    header::X_CONTENT_TYPE_OPTIONS,
                    header::HeaderValue::from_static("nosniff"),
                );
            }
            Some(response)
        }
    }
}

/// `http.Error(w, msg, status)` as a response (headers included).
pub fn http_error(status: StatusCode, message: &str) -> Response<BoxBody> {
    let mut response = Response::new(tonic::body::boxed(Full::new(Bytes::from(format!(
        "{message}\n"
    )))));
    *response.status_mut() = status;
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        header::HeaderValue::from_static("text/plain; charset=utf-8"),
    );
    response.headers_mut().insert(
        header::X_CONTENT_TYPE_OPTIONS,
        header::HeaderValue::from_static("nosniff"),
    );
    response
}

/// The `Authorization` header value with the `/.api/llm/chat/completions`
/// `Bearer` compatibility rewrite (`httpapi.parseAuthorizationHeaderValue`).
pub fn authorization_header_value(headers: &HeaderMap, path: &str) -> Option<String> {
    let value = headers
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())?
        .to_owned();
    if value.starts_with("Bearer ") && path == "/.api/llm/chat/completions" {
        return Some(format!("token {}", &value["Bearer ".len()..]));
    }
    Some(value)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn perms_string_and_include() {
        assert_eq!(PERMS_READ.to_string(), "read");
        assert_eq!(PERMS_WRITE.to_string(), "write");
        assert_eq!((PERMS_READ | PERMS_WRITE).to_string(), "read,write");
        assert_eq!(PERMS_NONE.to_string(), "none");
        assert!((PERMS_READ | PERMS_WRITE).include(PERMS_READ));
        assert!(!PERMS_READ.include(PERMS_WRITE));
    }

    #[test]
    fn token_and_token68_forms() {
        assert_eq!(
            parse_authorization_header("token abcdef", false),
            Ok(("abcdef".to_owned(), String::new()))
        );
        assert_eq!(
            parse_authorization_header("token token=\"abcdef\"", false),
            Ok(("abcdef".to_owned(), String::new()))
        );
        assert_eq!(
            parse_authorization_header("token-sudo token=\"abcdef\",user=\"alice\"", false),
            Ok(("abcdef".to_owned(), "alice".to_owned()))
        );
    }

    #[test]
    fn token68_with_sudo_is_rejected() {
        let error = parse_authorization_header("token-sudo abc", false).unwrap_err();
        assert!(error.message().contains("token=\"TOKEN\""));
    }

    #[test]
    fn unrecognized_schemes_and_param_errors() {
        assert_eq!(
            parse_authorization_header("Basic abcd", false),
            Err(AuthzParseError::UnrecognizedScheme)
        );
        // `parseHTTPCredentials` retries a no-equals param list as token68, so
        // `token x y` is accepted as the token68 "x y".
        assert_eq!(
            parse_authorization_header("token x y", false),
            Ok(("x y".to_owned(), String::new()))
        );
        assert!(parse_authorization_header("token", false).is_err());
    }

    #[test]
    fn dotcom_sudo_and_missing_token() {
        assert!(
            parse_authorization_header("token-sudo token=\"a\",user=\"b\"", true)
                .unwrap_err()
                .message()
                .contains("disabled")
        );
        assert_eq!(
            parse_authorization_header("token token=\"\"", false),
            Err(AuthzParseError::NoToken)
        );
    }

    #[test]
    fn bearer_parsing() {
        assert_eq!(parse_bearer_header("Bearer abc").unwrap(), "abc");
        assert_eq!(parse_bearer_header("bearer abc").unwrap(), "abc");
        assert!(parse_bearer_header("Basic abc").is_err());
        assert!(parse_bearer_header("Bearer").is_err());
    }

    #[test]
    fn sub_repo_decisions() {
        let actor = Actor {
            uid: 7,
            ..Default::default()
        };
        let checker = NoopPermsChecker;
        assert!(!sub_repo_enabled(Some(&checker)));
        assert_eq!(
            actor_sub_repo_enabled(None, &actor),
            Ok(false),
            "a disabled checker never checks"
        );

        // A disabled checker grants Read via `actor_permissions`.
        let runtime = tokio::runtime::Runtime::new().unwrap();
        assert_eq!(
            runtime.block_on(actor_permissions(
                Some(&checker),
                &actor,
                &RepoContent::default()
            )),
            Ok(PERMS_READ)
        );
    }

    #[test]
    fn subscription_body_starts_with_a_newline_and_escapes() {
        let body = subscription_error_body("a<b>", "x&y");
        assert!(body.starts_with("\n<title>a&lt;b&gt; - Sourcegraph</title>"));
        assert!(body.contains("<h1>a&lt;b&gt;</h1><p>x&amp;y</p>"));
        assert!(body.ends_with("</div>\n"));
    }

    #[test]
    fn post_auth_gate_matrix() {
        let anon = Actor::default();
        assert_eq!(
            post_auth_middleware(&anon, LicenseState::Expired, SiteAdminCheck::Error),
            PostAuthDecision::Proceed
        );

        let user = Actor {
            uid: 1,
            ..Default::default()
        };
        assert_eq!(
            post_auth_middleware(&user, LicenseState::Valid, SiteAdminCheck::Error),
            PostAuthDecision::Proceed
        );
        assert_eq!(
            post_auth_middleware(&user, LicenseState::Expired, SiteAdminCheck::IsSiteAdmin),
            PostAuthDecision::Proceed
        );
        assert!(matches!(
            post_auth_middleware(
                &user,
                LicenseState::Expired,
                SiteAdminCheck::MustBeSiteAdmin
            ),
            PostAuthDecision::Reject { status: 403, .. }
        ));
        assert!(matches!(
            post_auth_middleware(&user, LicenseState::None, SiteAdminCheck::Error),
            PostAuthDecision::Proceed
        ));
        assert!(matches!(
            post_auth_middleware(&user, LicenseState::Error, SiteAdminCheck::Error),
            PostAuthDecision::Reject { status: 500, .. }
        ));
        assert!(matches!(
            post_auth_middleware(&user, LicenseState::Error, SiteAdminCheck::MustBeSiteAdmin),
            PostAuthDecision::Reject { status: 500, .. }
        ));
    }

    #[test]
    fn html_escape_matches_go() {
        assert_eq!(html_escape("a&b<c>d'e\"f"), "a&amp;b&lt;c&gt;d&#39;e&#34;f");
    }
}
