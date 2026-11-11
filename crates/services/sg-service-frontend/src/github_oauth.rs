#![forbid(unsafe_code)]

//! GitHub OAuth user resolution (`cmd/frontend/internal/auth/githuboauth/session.go`).
//!
//! Implements [`oauth_login::OAuthUserResolver`]:
//!
//! * `service_id()` / `success_event_name()` / `failure_event_name()` are the
//!   `SessionIssuerHelper` accessors (`GetServiceID`, `GitHubAuthSucceeded`,
//!   `GitHubAuthFailed`);
//! * `get_or_create_user(&OAuthToken)` fetches `GET <api-root>/user` and
//!   `GET <api-root>/user/emails?per_page=100` with the OAuth bearer token
//!   (the same `githubsvc` request shape: two `Accept` preview headers), keeps
//!   only verified emails with the primary one first, normalizes the username,
//!   and delegates to [`crate::user_provisioning::get_or_create_user`] exactly
//!   like `sessionIssuerHelper.GetOrCreateUser`.
//!
//! HTTP and storage are injected ([`GithubHttpTransport`],
//! [`crate::user_provisioning::ProvisionStore`]); [`ReqwestGithubHttpClient`]
//! is the production transport. No network is used by the differential
//! harness.

use async_trait::async_trait;
use serde::Deserialize;

use crate::oauth_login::{OAuthToken, OAuthUserResolver, UserResolution};
use crate::user_provisioning::{
    get_or_create_user, normalize_username, ExternalIdentity, ProvisionDeps, ProvisionStore,
    UsernameSuffixSource,
};

/// `extsvc.TypeGitHub`.
pub const GITHUB_SERVICE_TYPE: &str = "github";
/// `database.SecurityEventGitHubAuthSucceeded`.
pub const GITHUB_AUTH_SUCCEEDED_EVENT: &str = "GitHubAuthSucceeded";
/// `database.SecurityEventGitHubAuthFailed`.
pub const GITHUB_AUTH_FAILED_EVENT: &str = "GitHubAuthFailed";

/// The two `Accept` preview headers `githubsvc.V3Client.request` adds.
const ACCEPT_PREVIEWS: [&str; 2] = [
    "application/vnd.github.jean-grey-preview+json,application/vnd.github.mercy-preview+json",
    "application/vnd.github.machine-man-preview+json",
];

const USER_FETCH_FAILED: &str = "Could not read GitHub user from callback request.";
const NO_VERIFIED_EMAIL: &str = "Could not get verified email for GitHub user. Check that your GitHub account has a verified email that matches one of your Sourcegraph verified emails.";

/// One HTTP request the resolver issues. Header ordering is preserved; the
/// differential harness records exactly these fields.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GithubHttpRequest {
    pub method: String,
    pub url: String,
    /// `Authorization: Bearer <token>`.
    pub authorization: String,
    /// `Accept` values, in emission order.
    pub accept: Vec<String>,
}

/// One HTTP response (status + body text).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GithubHttpResponse {
    pub status: u16,
    pub body: String,
}

/// Injectable GitHub REST transport (no network in tests).
#[async_trait]
pub trait GithubHttpTransport: Send + Sync {
    async fn execute(&self, request: &GithubHttpRequest) -> Result<GithubHttpResponse, String>;
}

/// Production transport over `reqwest`.
#[derive(Debug, Clone)]
pub struct ReqwestGithubHttpClient {
    client: reqwest::Client,
}

impl ReqwestGithubHttpClient {
    pub fn new(client: reqwest::Client) -> Self {
        Self { client }
    }

    pub fn from_default_client() -> Self {
        Self::new(reqwest::Client::new())
    }
}

#[async_trait]
impl GithubHttpTransport for ReqwestGithubHttpClient {
    async fn execute(&self, request: &GithubHttpRequest) -> Result<GithubHttpResponse, String> {
        let mut builder = self.client.request(
            reqwest::Method::from_bytes(request.method.as_bytes())
                .map_err(|err| err.to_string())?,
            &request.url,
        );
        for accept in &request.accept {
            builder = builder.header(reqwest::header::ACCEPT, accept);
        }
        builder = builder.header(reqwest::header::AUTHORIZATION, &request.authorization);
        let response = builder.send().await.map_err(|err| err.to_string())?;
        let status = response.status().as_u16();
        let body = response.text().await.map_err(|err| err.to_string())?;
        Ok(GithubHttpResponse { status, body })
    }
}

/// `githubsvc.APIRoot` (`internal/extsvc/github/common.go:1643-1655`).
///
/// * `github.com` / `www.github.com` → `https://api.github.com/`
/// * empty or `/` path → `<base>/api/v3`
/// * otherwise the relative `api` resolved against the base path.
pub fn github_api_root(base_url: &str) -> String {
    let Ok(mut url) = url::Url::parse(base_url) else {
        return base_url.to_owned();
    };
    let host = url.host_str().unwrap_or_default().to_ascii_lowercase();
    if host == "github.com" || host == "www.github.com" {
        return "https://api.github.com/".to_owned();
    }
    let path = url.path();
    if path.is_empty() || path == "/" {
        url.set_path("/api/v3");
        return url.to_string();
    }
    // `ResolveReference(&url.URL{Path: "api"})`: relative to the base
    // directory. `Url::join` has the same RFC 3986 behaviour.
    match url.join("api") {
        Ok(joined) => joined.to_string(),
        Err(_) => base_url.to_owned(),
    }
}

#[derive(Debug, Deserialize)]
struct GithubUser {
    #[serde(default)]
    login: String,
    #[serde(default)]
    id: i64,
    #[serde(default)]
    name: String,
    #[serde(default)]
    avatar_url: String,
}

#[derive(Debug, Deserialize)]
struct GithubEmail {
    #[serde(default)]
    email: String,
    #[serde(default)]
    primary: bool,
    #[serde(default)]
    verified: bool,
}

/// The GitHub OAuth `SessionIssuerHelper`.
pub struct GithubOAuthResolver<'a> {
    service_id: String,
    client_id: String,
    allow_signup: bool,
    api_root: String,
    http: &'a dyn GithubHttpTransport,
    store: &'a dyn ProvisionStore,
    suffix: &'a dyn UsernameSuffixSource,
    sourcegraph_dotcom: bool,
}

impl<'a> GithubOAuthResolver<'a> {
    /// `base_url` is the configured code host URL (the helper's `CodeHost.BaseURL`).
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        service_id: impl Into<String>,
        client_id: impl Into<String>,
        allow_signup: bool,
        base_url: &str,
        http: &'a dyn GithubHttpTransport,
        store: &'a dyn ProvisionStore,
        suffix: &'a dyn UsernameSuffixSource,
        sourcegraph_dotcom: bool,
    ) -> Self {
        Self {
            service_id: service_id.into(),
            client_id: client_id.into(),
            allow_signup,
            api_root: github_api_root(base_url),
            http,
            store,
            suffix,
            sourcegraph_dotcom,
        }
    }

    fn endpoint(&self, path: &str) -> String {
        format!("{}{}", self.api_root.trim_end_matches('/'), path)
    }

    async fn fetch_user(&self, access_token: &str) -> Option<GithubUser> {
        let request = GithubHttpRequest {
            method: "GET".to_owned(),
            url: self.endpoint("/user"),
            authorization: format!("Bearer {access_token}"),
            accept: ACCEPT_PREVIEWS.iter().map(|v| (*v).to_owned()).collect(),
        };
        let response = self.http.execute(&request).await.ok()?;
        if response.status != 200 {
            return None;
        }
        serde_json::from_str(&response.body).ok()
    }

    async fn verified_emails(&self, access_token: &str) -> Vec<String> {
        let request = GithubHttpRequest {
            method: "GET".to_owned(),
            url: self.endpoint("/user/emails?per_page=100"),
            authorization: format!("Bearer {access_token}"),
            accept: ACCEPT_PREVIEWS.iter().map(|v| (*v).to_owned()).collect(),
        };
        let Ok(response) = self.http.execute(&request).await else {
            return Vec::new();
        };
        if !(200..400).contains(&response.status) {
            return Vec::new();
        }
        let Ok(emails) = serde_json::from_str::<Vec<GithubEmail>>(&response.body) else {
            return Vec::new();
        };
        let mut verified = Vec::new();
        for email in emails {
            if !email.verified {
                continue;
            }
            if email.primary {
                verified.insert(0, email.email);
            } else {
                verified.push(email.email);
            }
        }
        verified
    }

    /// `sessionIssuerHelper.GetOrCreateUser`. Public so the differential
    /// harness can call it with a bare access token.
    pub async fn resolve(&self, access_token: &str) -> UserResolution {
        let Some(user) = self.fetch_user(access_token).await else {
            return UserResolution::Failed {
                safe_message: USER_FETCH_FAILED.to_owned(),
            };
        };

        let login = match normalize_username(&user.login, self.suffix, self.sourcegraph_dotcom) {
            Ok(login) => login,
            Err(_) => {
                // Go's `login` is the zero value when NormalizeUsername fails.
                return UserResolution::Failed {
                    safe_message: "Error normalizing the username \"\". See https://sourcegraph.com/docs/admin/auth/#username-normalization."
                        .to_owned(),
                };
            }
        };

        let verified_emails = self.verified_emails(access_token).await;
        if verified_emails.is_empty() {
            return UserResolution::Failed {
                safe_message: NO_VERIFIED_EMAIL.to_owned(),
            };
        }

        let account_id = user.id.to_string();
        let mut attempts: Vec<(String, bool)> = verified_emails
            .iter()
            .map(|email| (email.clone(), false))
            .collect();
        let mut signup_error_message = "";
        if self.allow_signup {
            attempts.push((verified_emails[0].clone(), true));
            signup_error_message = "\n\nOr failed on creating a user account";
        }

        let deps = ProvisionDeps {
            store: self.store,
            suffix: self.suffix,
            sourcegraph_dotcom: self.sourcegraph_dotcom,
        };
        let mut last_safe_message = String::new();
        for (email, create_if_not_exist) in attempts {
            let identity = ExternalIdentity {
                service_type: GITHUB_SERVICE_TYPE,
                service_id: &self.service_id,
                client_id: &self.client_id,
                account_id: &account_id,
                login: &login,
                email: &email,
                email_verified: true,
                display_name: &user.name,
                avatar_url: &user.avatar_url,
                create_if_not_exist,
            };
            match get_or_create_user(&deps, &identity).await {
                Ok(outcome) => {
                    return UserResolution::Created {
                        user_id: outcome.user_id(),
                        new_user: outcome.new_user_created(),
                    };
                }
                Err(err) => last_safe_message = err.safe_message,
            }
        }

        UserResolution::Failed {
            safe_message: format!(
                "Could not find existing user matching any of the verified emails: {} {} \n\nLast error was: {}",
                verified_emails.join(", "),
                signup_error_message,
                last_safe_message
            ),
        }
    }
}

#[async_trait]
impl OAuthUserResolver for GithubOAuthResolver<'_> {
    fn service_id(&self) -> &str {
        &self.service_id
    }

    fn success_event_name(&self) -> &str {
        GITHUB_AUTH_SUCCEEDED_EVENT
    }

    fn failure_event_name(&self) -> &str {
        GITHUB_AUTH_FAILED_EVENT
    }

    async fn get_or_create_user(&self, token: &OAuthToken) -> UserResolution {
        self.resolve(&token.access_token).await
    }
}
