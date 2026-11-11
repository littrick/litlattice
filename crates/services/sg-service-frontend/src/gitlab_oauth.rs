#![forbid(unsafe_code)]

//! GitLab OAuth provider specialization
//! (`cmd/frontend/internal/auth/gitlaboauth` + `internal/extsvc/gitlab`).
//!
//! This slice ports three pieces:
//!
//! * `parseConfig`/`parseProvider` (`config.go`, `provider.go`): the
//!   `auth.providers` GitLab entries, the `externalURL`-derived callback URL,
//!   the `/oauth/authorize` + `/oauth/token` endpoints, the
//!   `RequestedOAuthScopes` list and the exact site-problem texts.
//! * the login specialization (`login.go`): the plain redirect to
//!   `AuthCodeURL` and `SSOLoginHandler`'s rewrite to the configured
//!   `ssoURL` with the original authorize URL in its `redirect` parameter.
//! * the user resolver (`session.go` + `login.go`'s `gitlabHandler`):
//!   `GET /api/v4/user` with an OAuth Bearer token, `validateResponse`,
//!   `NormalizeUsername`, `allowSignup`, allowed groups and the
//!   `GetAndSaveUser` call.
//!
//! Provisioning itself (the `users`/`user_emails`/`user_external_accounts`
//! writes) is **not** implemented here: it is injected as [`Provisioner`]
//! and adapted from M9.8jp's `user_provisioning::get_or_create_user` by the
//! parent wiring. All HTTP is injected through [`GitlabApiClient`], so tests
//! never touch the network.

use std::sync::Arc;

use async_trait::async_trait;
use serde::Deserialize;
use serde_json::Value;

use crate::oauth_login::{
    encode_query_sorted, go_parse_url, go_url_string, handle_login, html_escape, parse_query,
    GoUrl, OAuthFlowDeps, OAuthFlowResponse, OAuthLoginRequest, OAuthProviderConfig, OAuthToken,
    OAuthUserResolver, UserResolution,
};

pub const GITLAB_SERVICE_TYPE: &str = "gitlab";
pub const GITLAB_DEFAULT_URL: &str = "https://gitlab.com/";
pub const GITLAB_SUCCESS_EVENT: &str = "GitLabAuthSucceeded";
pub const GITLAB_FAILURE_EVENT: &str = "GitLabAuthFailed";

const CONTENT_TYPE_HTML: &str = "text/html; charset=utf-8";

// ---------------------------------------------------------------------------
// Config parsing (`gitlaboauth/config.go`, `gitlaboauth/provider.go`).
// ---------------------------------------------------------------------------

/// One `auth.providers` GitLab entry (flat JSON shape, see
/// `schema.AuthProviders`'s tagged-union marshal).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct GitlabProviderSpec {
    pub url: String,
    pub client_id: String,
    pub client_secret: String,
    pub api_scope: String,
    pub allow_signup: Option<bool>,
    pub allow_groups: Vec<String>,
    pub sso_url: String,
}

/// A parsed GitLab OAuth provider.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GitlabOAuthProvider {
    pub service_type: String,
    pub service_id: String,
    pub client_id: String,
    pub client_secret: String,
    pub auth_url: String,
    pub token_url: String,
    pub scopes: Vec<String>,
    pub redirect_url: String,
    pub allow_signup: Option<bool>,
    pub allow_groups: Vec<String>,
    pub sso_url: String,
}

impl GitlabOAuthProvider {
    /// `oauth.Provider.ConfigID().ID` = `ServiceID::ClientID`.
    pub fn config_id(&self) -> String {
        format!("{}::{}", self.service_id, self.client_id)
    }

    /// `gitlabClientFromAuthURL`'s base URL: the authorize URL minus path,
    /// query and fragment.
    pub fn base_url(&self) -> String {
        gitlab_base_url(&self.auth_url)
    }

    pub fn to_oauth_provider_config(&self) -> OAuthProviderConfig {
        OAuthProviderConfig {
            service_type: self.service_type.clone(),
            service_id: self.service_id.clone(),
            client_id: self.client_id.clone(),
            client_secret: self.client_secret.clone(),
            auth_url: self.auth_url.clone(),
            token_url: self.token_url.clone(),
            scopes: self.scopes.clone(),
            redirect_url: self.redirect_url.clone(),
            config_id: self.config_id(),
            sso_url: self.sso_url.clone(),
        }
    }
}

/// Extracts GitLab specs from a site config's `auth.providers`.
pub fn specs_from_site(site: &sg_config::SiteConfiguration) -> Vec<GitlabProviderSpec> {
    let raw = site.raw();
    let entries = raw
        .get("auth.providers")
        .and_then(Value::as_array)
        .map(Vec::as_slice)
        .unwrap_or_default();
    let mut specs = Vec::new();
    for entry in entries {
        let Some(obj) = entry.as_object() else {
            continue;
        };
        if obj.get("type").and_then(Value::as_str) != Some(GITLAB_SERVICE_TYPE) {
            continue;
        }
        specs.push(GitlabProviderSpec {
            url: string_field(obj, "url"),
            client_id: string_field(obj, "clientID"),
            client_secret: string_field(obj, "clientSecret"),
            api_scope: string_field(obj, "apiScope"),
            allow_signup: obj.get("allowSignup").and_then(Value::as_bool),
            allow_groups: obj
                .get("allowGroups")
                .and_then(Value::as_array)
                .map(|groups| {
                    groups
                        .iter()
                        .filter_map(Value::as_str)
                        .map(str::to_owned)
                        .collect()
                })
                .unwrap_or_default(),
            sso_url: string_field(obj, "ssoURL"),
        });
    }
    specs
}

fn string_field(obj: &serde_json::Map<String, Value>, key: &str) -> String {
    obj.get(key)
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_owned()
}

/// `gitlaboauth.parseConfig`: returns the providers plus the site problems in
/// baseline order.
pub fn parse_gitlab_providers(
    external_url: &str,
    specs: &[GitlabProviderSpec],
) -> (Vec<GitlabOAuthProvider>, Vec<String>) {
    let mut providers = Vec::new();
    let mut problems = Vec::new();
    let mut seen: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();

    for spec in specs {
        if external_url.is_empty() {
            problems.push(
                "`externalURL` was empty and it is needed to determine the OAuth callback URL."
                    .to_owned(),
            );
            continue;
        }
        let Some(external) = go_parse_url(external_url) else {
            problems.push(
                "Could not parse `externalURL`, which is needed to determine the OAuth callback URL."
                    .to_owned(),
            );
            continue;
        };
        let callback_url = go_url_string(&GoUrl {
            path: "/.auth/gitlab/callback".to_owned(),
            raw_query: String::new(),
            ..external
        });

        let raw_url = if spec.url.is_empty() {
            GITLAB_DEFAULT_URL
        } else {
            spec.url.as_str()
        };
        let Some(parsed) = go_parse_url(raw_url) else {
            problems.push(format!(
                "Could not parse GitLab URL {}. You will not be able to login via this GitLab instance.",
                go_quote(raw_url)
            ));
            continue;
        };

        let service_id = normalize_base_url(&parsed);
        // `NewCodeHost` lowercases the host in place before the endpoints are
        // resolved, so the authorize/token URLs carry a lowercase host too.
        let base = GoUrl {
            scheme: parsed.scheme.clone(),
            host: parsed.host.to_ascii_lowercase(),
            path: String::new(),
            raw_query: String::new(),
        };
        let auth_url = go_url_string(&GoUrl {
            path: "/oauth/authorize".to_owned(),
            ..base.clone()
        });
        let token_url = go_url_string(&GoUrl {
            path: "/oauth/token".to_owned(),
            ..base
        });
        let mut scopes = vec!["read_user".to_owned()];
        scopes.push(if spec.api_scope.is_empty() {
            "api".to_owned()
        } else {
            spec.api_scope.clone()
        });
        let provider = GitlabOAuthProvider {
            service_type: GITLAB_SERVICE_TYPE.to_owned(),
            service_id: service_id.clone(),
            client_id: spec.client_id.clone(),
            client_secret: spec.client_secret.clone(),
            auth_url,
            token_url,
            scopes,
            redirect_url: callback_url,
            allow_signup: spec.allow_signup,
            allow_groups: spec.allow_groups.clone(),
            sso_url: spec.sso_url.clone(),
        };

        // `parseConfig`'s duplicate check uses `Info.UniqueID()` =
        // `ServiceID + ":" + ClientID` (single colon, unlike `ConfigID`, which
        // uses `"::"`).
        if !seen.insert(format!("{}:{}", service_id, spec.client_id)) {
            problems.push(format!(
                "Cannot have more than one GitLab auth provider with url {} and client ID {}, only the first one will be used",
                go_quote(&service_id),
                go_quote(&spec.client_id)
            ));
            continue;
        }
        providers.push(provider);
    }
    (providers, problems)
}

fn normalize_base_url(parsed: &GoUrl) -> String {
    let mut path = parsed.path.clone();
    if !path.ends_with('/') {
        path.push('/');
    }
    go_url_string(&GoUrl {
        scheme: parsed.scheme.clone(),
        host: parsed.host.to_ascii_lowercase(),
        path,
        raw_query: String::new(),
    })
}

fn gitlab_base_url(auth_url: &str) -> String {
    match go_parse_url(auth_url) {
        Some(parsed) => format!("{}://{}", parsed.scheme, parsed.host),
        None => String::new(),
    }
}

// ---------------------------------------------------------------------------
// Login specialization (`gitlaboauth/login.go`).
// ---------------------------------------------------------------------------

/// Runs the generic `/login` flow and, when `sso_url` is configured, rewrites
/// the authorize redirect exactly like `SSOLoginHandler`:
/// `ssoURL?redirect=<authURL.Path?authURL.RawQuery>` with sorted query keys.
pub async fn handle_gitlab_login(
    deps: &OAuthFlowDeps<'_>,
    request: &OAuthLoginRequest<'_>,
    sso_url: &str,
) -> OAuthFlowResponse {
    let response = handle_login(deps, request).await;
    if sso_url.is_empty() || response.status != 302 {
        return response;
    }
    let Some(auth_url) = go_parse_url(&response.location) else {
        return response;
    };
    // No provider matched: the baseline redirects to the sign-in page, not to
    // the provider, so the SSO rewrite does not apply.
    if auth_url.path == "/sign-in" {
        return response;
    }
    let redirect = format!("{}?{}", auth_url.path, auth_url.raw_query);
    let Some(mut sso) = go_parse_url(sso_url) else {
        return response;
    };
    let mut pairs = parse_query(&sso.raw_query);
    pairs.push(("redirect".to_owned(), redirect));
    sso.raw_query = encode_query_sorted(&pairs);
    let location = go_url_string(&sso);
    OAuthFlowResponse {
        status: 302,
        location: location.clone(),
        content_type: CONTENT_TYPE_HTML.to_owned(),
        body: format!("<a href=\"{}\">Found</a>.\n\n", html_escape(&location)).into_bytes(),
        // `http.Redirect` does not touch previously written Set-Cookie headers.
        set_cookies: response.set_cookies,
    }
}

// ---------------------------------------------------------------------------
// GitLab API seam (`internal/extsvc/gitlab/users.go` `GetUser`).
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GitlabUserRequest {
    pub url: String,
    pub access_token: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GitlabHttpResponse {
    pub status: u16,
    pub content_type: String,
    pub body: Vec<u8>,
}

/// Injected GitLab REST transport. Production sends a Bearer-authenticated
/// `GET`; tests record the request and return a canned response.
#[async_trait]
pub trait GitlabApiClient: Send + Sync {
    async fn get_user(&self, request: &GitlabUserRequest) -> Result<GitlabHttpResponse, String>;
    async fn list_groups(&self, request: &GitlabUserRequest) -> Result<GitlabHttpResponse, String>;
}

/// `gitlab.AuthUser` (only the fields the auth flow reads).
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
pub struct GitlabAuthUser {
    #[serde(default)]
    pub id: i32,
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub username: String,
    #[serde(default)]
    pub email: String,
    #[serde(default)]
    pub state: String,
    #[serde(default, rename = "avatar_url")]
    pub avatar_url: String,
    #[serde(default, rename = "web_url")]
    pub web_url: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
pub struct GitlabGroup {
    #[serde(default)]
    pub id: i32,
    #[serde(default, rename = "full_path")]
    pub full_path: String,
}

// ---------------------------------------------------------------------------
// Provisioning seam (M9.8jp owns the SQL).
// ---------------------------------------------------------------------------

/// Input shape shared with tasks/39's M9.8jp `ExternalIdentity`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExternalIdentity {
    pub service_type: String,
    pub service_id: String,
    /// `extsvc.AccountSpec.ClientID` (`sessionIssuerHelper.clientID`).
    pub client_id: String,
    pub account_id: String,
    pub login: String,
    pub email: String,
    pub email_verified: bool,
    pub display_name: Option<String>,
    /// `UserProps.AvatarURL` (`gUser.AvatarURL`).
    pub avatar_url: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProvisionOutcome {
    Created { user_id: i32 },
    Existing { user_id: i32 },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProvisionError {
    pub safe_message: String,
}

/// Injected `auth.GetAndSaveUser` equivalent. The parent adapter wraps
/// M9.8jp's `user_provisioning::get_or_create_user`.
#[async_trait]
pub trait Provisioner: Send + Sync {
    async fn get_or_create_user(
        &self,
        identity: &ExternalIdentity,
        create_if_not_exist: bool,
    ) -> Result<ProvisionOutcome, ProvisionError>;
}

// ---------------------------------------------------------------------------
// Resolver (`gitlaboauth/session.go` `GetOrCreateUser`).
// ---------------------------------------------------------------------------

pub struct GitlabUserResolver {
    service_id: String,
    /// `sessionIssuerHelper.clientID` (`p.ClientID`).
    client_id: String,
    allow_signup: Option<bool>,
    allow_groups: Vec<String>,
    /// Host root, from `gitlabClientFromAuthURL` (path cleared).
    base_url: String,
    /// `CodeHost.BaseURL` keep its path, so group listings of a subpath
    /// GitLab instance go to `<host><path>/api/v4/...` (`session.go:84`).
    groups_base_url: String,
    api: Arc<dyn GitlabApiClient>,
    provisioner: Arc<dyn Provisioner>,
    sourcegraph_dotcom: bool,
}

impl GitlabUserResolver {
    pub fn new(
        provider: &GitlabOAuthProvider,
        api: Arc<dyn GitlabApiClient>,
        provisioner: Arc<dyn Provisioner>,
        sourcegraph_dotcom: bool,
    ) -> Self {
        Self {
            service_id: provider.service_id.clone(),
            client_id: provider.client_id.clone(),
            allow_signup: provider.allow_signup,
            allow_groups: provider.allow_groups.clone(),
            base_url: provider.base_url(),
            groups_base_url: provider.service_id.trim_end_matches('/').to_owned(),
            api,
            provisioner,
            sourcegraph_dotcom,
        }
    }

    fn api_url(&self, base: &str, path: &str) -> String {
        format!("{base}/api/v4/{path}")
    }

    async fn fetch_user(&self, access_token: &str) -> Result<GitlabAuthUser, UserResolution> {
        let url = self.api_url(self.base_url.trim_end_matches('/'), "user");
        let response = self
            .api
            .get_user(&GitlabUserRequest {
                url: url.clone(),
                access_token: access_token.to_owned(),
            })
            .await
            .map_err(|error| UserResolution::FailedWithStatus {
                status: 400,
                safe_message: format!("unable to get GitLab user: request failed: {error}"),
            })?;
        match parse_user_response(&url, &response) {
            Ok(user) if user.id == 0 => Err(UserResolution::FailedWithStatus {
                status: 400,
                safe_message: format!(
                    "unable to get GitLab user: bad user info {}",
                    go_struct_debug(&user)
                ),
            }),
            Ok(user) => Ok(user),
            Err(message) => Err(UserResolution::FailedWithStatus {
                status: 400,
                safe_message: format!("unable to get GitLab user: {message}"),
            }),
        }
    }

    async fn verify_user_groups(&self, access_token: &str) -> Result<bool, ()> {
        if self.allow_groups.is_empty() {
            return Ok(true);
        }
        let allowed: std::collections::BTreeSet<&str> =
            self.allow_groups.iter().map(String::as_str).collect();
        let mut page = 1;
        loop {
            let url = self.api_url(
                &self.groups_base_url,
                &format!("groups?per_page=100&page={page}&min_access_level=10"),
            );
            let response = self
                .api
                .list_groups(&GitlabUserRequest {
                    url,
                    access_token: access_token.to_owned(),
                })
                .await
                .map_err(|_| ())?;
            // `c.do` wraps a non-2xx/3xx status in an error before decoding.
            if !(200..400).contains(&response.status) {
                return Err(());
            }
            let groups: Vec<GitlabGroup> =
                serde_json::from_slice(&response.body).map_err(|_| ())?;
            let has_next_page = !groups.is_empty();
            for group in &groups {
                if allowed.contains(group.full_path.as_str()) {
                    return Ok(true);
                }
            }
            if !has_next_page {
                return Ok(false);
            }
            page += 1;
        }
    }
}

fn parse_user_response(url: &str, response: &GitlabHttpResponse) -> Result<GitlabAuthUser, String> {
    if !(200..400).contains(&response.status) {
        return Err(format!(
            "unexpected response from GitLab API ({url}): HTTP error status {}",
            response.status
        ));
    }
    serde_json::from_slice(&response.body)
        .map_err(|error| format!("unexpected response from GitLab API ({url}): {error}"))
}

#[async_trait]
impl OAuthUserResolver for GitlabUserResolver {
    fn service_id(&self) -> &str {
        &self.service_id
    }

    fn success_event_name(&self) -> &str {
        GITLAB_SUCCESS_EVENT
    }

    fn failure_event_name(&self) -> &str {
        GITLAB_FAILURE_EVENT
    }

    async fn get_or_create_user(&self, token: &OAuthToken) -> UserResolution {
        let user = match self.fetch_user(&token.access_token).await {
            Ok(user) => user,
            Err(resolution) => return resolution,
        };

        let login = match normalize_username(&user.username, self.sourcegraph_dotcom) {
            Ok(login) => login,
            Err(_) => {
                // The baseline formats the *returned* login (empty on error):
                // `Error normalizing the username %q. ...`.
                return UserResolution::Failed {
                    safe_message: format!(
                        "Error normalizing the username {}. See https://sourcegraph.com/docs/admin/auth/#username-normalization.",
                        go_quote("")
                    ),
                };
            }
        };

        match self.verify_user_groups(&token.access_token).await {
            Ok(true) => {}
            Ok(false) => {
                return UserResolution::Failed {
                    safe_message: "User does not belong to allowed GitLab groups or subgroups."
                        .to_owned(),
                }
            }
            Err(()) => {
                return UserResolution::Failed {
                    safe_message: "Error verifying user groups.".to_owned(),
                }
            }
        }

        let identity = ExternalIdentity {
            service_type: GITLAB_SERVICE_TYPE.to_owned(),
            service_id: self.service_id.clone(),
            client_id: self.client_id.clone(),
            account_id: user.id.to_string(),
            login,
            email: user.email.clone(),
            email_verified: !user.email.is_empty(),
            display_name: Some(user.name.clone()),
            avatar_url: user.avatar_url.clone(),
        };
        let create_if_not_exist = self.allow_signup.unwrap_or(true);
        match self
            .provisioner
            .get_or_create_user(&identity, create_if_not_exist)
            .await
        {
            Ok(ProvisionOutcome::Created { user_id }) => UserResolution::Created {
                user_id,
                new_user: true,
            },
            Ok(ProvisionOutcome::Existing { user_id }) => UserResolution::Created {
                user_id,
                new_user: false,
            },
            Err(error) => UserResolution::Failed {
                safe_message: error.safe_message,
            },
        }
    }
}

// ---------------------------------------------------------------------------
// `auth.NormalizeUsername` (`cmd/frontend/auth/auth.go` +
// `userpasswd.NormalizeUsername`).
// ---------------------------------------------------------------------------

/// Ports `NormalizeUsername`. On Sourcegraph.com the normalized name is also
/// checked against `suspiciousnames` (`user_provisioning::is_suspicious_name`).
pub fn normalize_username(name: &str, sourcegraph_dotcom: bool) -> Result<String, String> {
    let orig = name;
    let mut name = name.to_owned();
    if let Some(index) = name.find('@') {
        if index == name.rfind('@').unwrap_or(index) {
            name.truncate(index);
            if sourcegraph_dotcom {
                name = add_random_suffix(&name)?;
            }
        }
    }

    let mut replaced = String::with_capacity(name.len());
    for ch in name.chars() {
        if ch.is_ascii_alphanumeric() || matches!(ch, '_' | '-' | '.') {
            replaced.push(ch);
        } else {
            replaced.push('-');
        }
    }

    let mut collapsed = String::with_capacity(replaced.len());
    let mut chars = replaced.chars().peekable();
    while let Some(ch) = chars.next() {
        if ch == '-' || ch == '.' {
            let mut run = 1usize;
            while let Some(next) = chars.peek() {
                if *next == '-' || *next == '.' {
                    chars.next();
                    run += 1;
                } else {
                    break;
                }
            }
            collapsed.push(if run >= 2 { '-' } else { ch });
        } else {
            collapsed.push(ch);
        }
    }

    let mut trimmed = collapsed;
    if trimmed.starts_with('-') || trimmed.starts_with('.') {
        trimmed.remove(0);
    }
    if trimmed.ends_with('.') {
        trimmed.pop();
    }

    if trimmed.is_empty() {
        return Err(format!(
            "username {} could not be normalized to acceptable format",
            go_quote(orig)
        ));
    }
    if sourcegraph_dotcom && crate::user_provisioning::is_suspicious_name(&trimmed) {
        return Err(format!("rejected suspicious name {}", go_quote(&trimmed)));
    }
    Ok(trimmed)
}

fn add_random_suffix(username: &str) -> Result<String, String> {
    use std::io::Read as _;
    let mut bytes = [0u8; 5];
    let mut file = std::fs::File::open("/dev/urandom").map_err(|error| error.to_string())?;
    file.read_exact(&mut bytes)
        .map_err(|error| error.to_string())?;
    let suffix: String = bytes
        .iter()
        .map(|byte| char::from(b'a' + (byte % 26)))
        .collect();
    if username.is_empty() || username.ends_with('-') {
        Ok(format!("{username}{suffix}"))
    } else {
        Ok(format!("{username}-{suffix}"))
    }
}

// ---------------------------------------------------------------------------
// Go-compatible formatting helpers.
// ---------------------------------------------------------------------------

/// `fmt.Sprintf("%q", value)` for the ASCII shapes this slice formats.
pub fn go_quote(value: &str) -> String {
    let mut out = String::with_capacity(value.len() + 2);
    out.push('"');
    for ch in value.chars() {
        match ch {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => out.push_str(&format!("\\x{:02x}", c as u32)),
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

/// `fmt.Sprintf("%#+v", user)` for `*gitlab.AuthUser` (`%#v` wins: Go-syntax
/// representation). Our parser does not retain `identities`/`created_at`, so
/// those print their Go zero values.
fn go_struct_debug(user: &GitlabAuthUser) -> String {
    format!(
        "&gitlab.AuthUser{{ID:{}, Name:{}, Username:{}, Email:{}, State:{}, AvatarURL:{}, WebURL:{}, Identities:[]gitlab.Identity(nil), CreatedAt:time.Date(1, time.January, 1, 0, 0, 0, 0, time.UTC)}}",
        user.id,
        go_quote(&user.name),
        go_quote(&user.username),
        go_quote(&user.email),
        go_quote(&user.state),
        go_quote(&user.avatar_url),
        go_quote(&user.web_url)
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalizes_like_go() {
        assert_eq!(normalize_username("bob", false).unwrap(), "bob");
        assert_eq!(normalize_username("Bob.Smith", false).unwrap(), "Bob.Smith");
        assert_eq!(normalize_username("bob@example.com", false).unwrap(), "bob");
        assert_eq!(normalize_username("a..b", false).unwrap(), "a-b");
        assert_eq!(normalize_username("a.-b", false).unwrap(), "a-b");
        assert_eq!(normalize_username("..a", false).unwrap(), "a");
        assert_eq!(normalize_username("a.", false).unwrap(), "a");
        assert_eq!(normalize_username("a-", false).unwrap(), "a-");
        assert!(normalize_username("!!!", false).is_err());
        assert!(normalize_username("...", false).is_err());
        assert!(normalize_username("", false).is_err());
    }

    #[test]
    fn parses_config_problems() {
        let spec = GitlabProviderSpec {
            url: "https://gitlab.example.com/".to_owned(),
            client_id: "cid".to_owned(),
            client_secret: "sec".to_owned(),
            ..Default::default()
        };
        let (providers, problems) =
            parse_gitlab_providers("https://sourcegraph.example", std::slice::from_ref(&spec));
        assert!(problems.is_empty());
        assert_eq!(providers.len(), 1);
        assert_eq!(providers[0].service_id, "https://gitlab.example.com/");
        assert_eq!(
            providers[0].auth_url,
            "https://gitlab.example.com/oauth/authorize"
        );
        assert_eq!(providers[0].scopes, vec!["read_user", "api"]);

        let (providers, problems) = parse_gitlab_providers("", std::slice::from_ref(&spec));
        assert!(providers.is_empty());
        assert_eq!(problems.len(), 1);

        let (providers, problems) =
            parse_gitlab_providers("https://sourcegraph.example", &[spec.clone(), spec.clone()]);
        assert_eq!(providers.len(), 1);
        assert_eq!(problems.len(), 1);
        assert!(problems[0].starts_with("Cannot have more than one GitLab auth provider"));
    }
}
