#![forbid(unsafe_code)]

//! Production wiring of the OAuth `GetOrCreateUser` resolvers (M9.8jo's
//! `OAuthUserResolverRegistry` seam over the M9.8jp/jq/jr implementations).
//!
//! Baseline shape:
//!
//! * `oauth.NewMiddleware`'s `/.auth/<type>/callback` decodes the state
//!   parameter, resolves `state.ProviderID` and calls
//!   `GetProvider(serviceType, id).Callback(...)` (`auth/oauth/middleware.go:117-136`).
//! * GitHub/GitLab `SessionIssuerHelper.GetOrCreateUser` call `auth.GetAndSaveUser`
//!   (`githuboauth/session.go`, `gitlaboauth/session.go`).
//! * OpenID Connect's `AuthCallback` verifies the id_token (nonce == state) and
//!   then calls `getOrCreateUser` (`oidc/middleware.go:249-400`).
//!
//! This module supplies:
//!
//! * the HTTP transports the providers need (`reqwest` for GitHub REST,
//!   GitLab `/api/v4`, and OIDC discovery/JWKS/userinfo),
//! * a provisioner adapter over [`crate::user_provisioning`] for the GitLab and
//!   OpenID Connect seams (GitHub's resolver already uses it directly),
//! * per-config-generation registration into [`OAuthUserResolverRegistry`],
//!   selecting the provider whose `ConfigID` the state names (falling back to
//!   the first provider of that service type, which is what the baseline does
//!   when the state cannot be decoded).
//!
//! Known gaps (see the M9.8jo–jr report errata):
//!
//! * OIDC login (discovery + `nonce` on the authorization request) stays in the
//!   provider-specific middleware in the baseline; the generic flow only covers
//!   the callback resolution here.
//! * GitLab/OIDC seams do not carry `avatar_url` (the baseline stores
//!   `gUser.AvatarURL` / `claims.Picture` on user creation).

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine as _;
use sqlx::PgPool;

use sg_config::SiteConfiguration;

use crate::github_oauth::{
    GithubHttpTransport, GithubOAuthResolver, ReqwestGithubHttpClient, GITHUB_SERVICE_TYPE,
};
use crate::gitlab_oauth::{
    self, GitlabApiClient, GitlabHttpResponse, GitlabOAuthProvider, GitlabUserRequest,
    GitlabUserResolver, ProvisionError as GitlabProvisionError,
    ProvisionOutcome as GitlabProvisionOutcome, Provisioner as GitlabProvisioner,
};
use crate::oauth_login::{LoginState, OAuthToken, OAuthUserResolver, UserResolution};
use crate::oauth_routes::OAuthUserResolverRegistry;
use crate::oidc_login::{
    self, OidcHttpClient, OidcHttpResponse, OidcLoginResolver, OpenIdConnectAuthProvider,
    ProvisionError as OidcProvisionError, ProvisionOutcome as OidcProvisionOutcome,
    Provisioner as OidcProvisioner,
};
use crate::user_provisioning::{
    self, ExternalIdentity, OsUsernameSuffix, PgProvisionStore, ProvisionDeps, ProvisionError,
    ProvisionOutcome,
};

/// `schema.DefaultGitHubURL`.
const DEFAULT_GITHUB_URL: &str = "https://github.com/";

// ---------------------------------------------------------------------------
// Production HTTP transports
// ---------------------------------------------------------------------------

/// Production GitLab REST transport (`internal/extsvc/gitlab`): a Bearer-
/// authenticated `GET` returning status, `Content-Type` and the raw body.
#[derive(Debug, Clone)]
pub struct ReqwestGitlabApiClient {
    client: reqwest::Client,
}

impl ReqwestGitlabApiClient {
    pub fn new(client: reqwest::Client) -> Self {
        Self { client }
    }

    pub fn from_default_client() -> Self {
        Self::new(reqwest::Client::new())
    }

    async fn get(&self, request: &GitlabUserRequest) -> Result<GitlabHttpResponse, String> {
        let response = self
            .client
            .get(&request.url)
            .header(
                reqwest::header::AUTHORIZATION,
                format!("Bearer {}", request.access_token),
            )
            .send()
            .await
            .map_err(|err| err.to_string())?;
        let status = response.status().as_u16();
        let content_type = response
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .unwrap_or_default()
            .to_owned();
        let body = response
            .bytes()
            .await
            .map_err(|err| err.to_string())?
            .to_vec();
        Ok(GitlabHttpResponse {
            status,
            content_type,
            body,
        })
    }
}

impl Default for ReqwestGitlabApiClient {
    fn default() -> Self {
        Self::from_default_client()
    }
}

#[async_trait]
impl GitlabApiClient for ReqwestGitlabApiClient {
    async fn get_user(&self, request: &GitlabUserRequest) -> Result<GitlabHttpResponse, String> {
        self.get(request).await
    }

    async fn list_groups(&self, request: &GitlabUserRequest) -> Result<GitlabHttpResponse, String> {
        self.get(request).await
    }
}

/// Production OIDC transport: plain `GET`, optionally bearer-authenticated
/// (userinfo). The response keeps status and raw body so jr's error texts match
/// go-oidc's.
#[derive(Debug, Clone)]
pub struct ReqwestOidcHttpClient {
    client: reqwest::Client,
}

impl ReqwestOidcHttpClient {
    pub fn new(client: reqwest::Client) -> Self {
        Self { client }
    }

    pub fn from_default_client() -> Self {
        Self::new(reqwest::Client::new())
    }
}

impl Default for ReqwestOidcHttpClient {
    fn default() -> Self {
        Self::from_default_client()
    }
}

#[async_trait]
impl OidcHttpClient for ReqwestOidcHttpClient {
    async fn get(&self, url: &str, bearer: Option<&str>) -> Result<OidcHttpResponse, String> {
        let mut builder = self.client.get(url);
        if let Some(bearer) = bearer {
            builder = builder.header(reqwest::header::AUTHORIZATION, format!("Bearer {bearer}"));
        }
        let response = builder.send().await.map_err(|err| err.to_string())?;
        let status = response.status().as_u16();
        let body = response
            .bytes()
            .await
            .map_err(|err| err.to_string())?
            .to_vec();
        Ok(OidcHttpResponse { status, body })
    }
}

// ---------------------------------------------------------------------------
// Provisioner adapters over `user_provisioning`
// ---------------------------------------------------------------------------

/// Adapts the shared M9.8jp `GetAndSaveUser` port to the GitLab seam.
struct GitlabProvisionerAdapter {
    pool: PgPool,
    dotcom: bool,
}

impl GitlabProvisionerAdapter {
    async fn provision(
        &self,
        identity: &crate::gitlab_oauth::ExternalIdentity,
        create_if_not_exist: bool,
    ) -> Result<GitlabProvisionOutcome, GitlabProvisionError> {
        let store = PgProvisionStore::new(self.pool.clone());
        let suffix = OsUsernameSuffix;
        let deps = ProvisionDeps {
            store: &store,
            suffix: &suffix,
            sourcegraph_dotcom: self.dotcom,
        };
        let identity = ExternalIdentity {
            service_type: &identity.service_type,
            service_id: &identity.service_id,
            client_id: &identity.client_id,
            account_id: &identity.account_id,
            login: &identity.login,
            email: &identity.email,
            email_verified: identity.email_verified,
            display_name: identity.display_name.as_deref().unwrap_or_default(),
            avatar_url: &identity.avatar_url,
            create_if_not_exist,
        };
        match user_provisioning::get_or_create_user(&deps, &identity).await {
            Ok(ProvisionOutcome::Created { user_id }) => {
                Ok(GitlabProvisionOutcome::Created { user_id })
            }
            Ok(ProvisionOutcome::Existing { user_id }) => {
                Ok(GitlabProvisionOutcome::Existing { user_id })
            }
            Err(error) => Err(GitlabProvisionError {
                safe_message: error.safe_message,
            }),
        }
    }
}

#[async_trait]
impl GitlabProvisioner for GitlabProvisionerAdapter {
    async fn get_or_create_user(
        &self,
        identity: &crate::gitlab_oauth::ExternalIdentity,
        create_if_not_exist: bool,
    ) -> Result<GitlabProvisionOutcome, GitlabProvisionError> {
        self.provision(identity, create_if_not_exist).await
    }
}

/// Adapts the shared M9.8jp `GetAndSaveUser` port to the OpenID Connect seam.
///
/// The OIDC seam has no `CreateIfNotExist` parameter: the baseline takes it
/// from the provider config (`p.AllowSignup == nil || *p.AllowSignup`,
/// `openidconnect/user.go:92`), so the adapter carries it.
struct OidcProvisionerAdapter {
    pool: PgPool,
    dotcom: bool,
    create_if_not_exist: bool,
}

#[async_trait]
impl OidcProvisioner for OidcProvisionerAdapter {
    async fn get_or_create_user(
        &self,
        identity: &crate::oidc_login::ExternalIdentity<'_>,
    ) -> Result<OidcProvisionOutcome, OidcProvisionError> {
        let store = PgProvisionStore::new(self.pool.clone());
        let suffix = OsUsernameSuffix;
        let deps = ProvisionDeps {
            store: &store,
            suffix: &suffix,
            sourcegraph_dotcom: self.dotcom,
        };
        let identity = ExternalIdentity {
            service_type: identity.service_type,
            service_id: identity.service_id,
            client_id: identity.client_id,
            account_id: identity.account_id,
            login: identity.login,
            email: identity.email,
            email_verified: identity.email_verified,
            display_name: identity.display_name.unwrap_or_default(),
            avatar_url: identity.avatar_url,
            create_if_not_exist: self.create_if_not_exist,
        };
        match user_provisioning::get_or_create_user(&deps, &identity).await {
            Ok(ProvisionOutcome::Created { user_id }) => {
                Ok(OidcProvisionOutcome::Created { user_id })
            }
            Ok(ProvisionOutcome::Existing { user_id }) => {
                Ok(OidcProvisionOutcome::Existing { user_id })
            }
            Err(ProvisionError {
                safe_message,
                message,
            }) => Err(OidcProvisionError {
                safe_message,
                message,
            }),
        }
    }
}

// ---------------------------------------------------------------------------
// Config extraction
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
struct GithubConfig {
    service_id: String,
    client_id: String,
    allow_signup: bool,
    base_url: String,
    /// `auth.providers[].allowOrgs`.
    allow_orgs: Vec<String>,
    /// `auth.providers[].allowOrgsMap` (org login → allowed team names).
    allow_orgs_map: BTreeMap<String, Vec<String>>,
}

impl GithubConfig {
    fn config_id(&self) -> String {
        crate::oauth_login::provider_config_id(&self.service_id, &self.client_id)
    }
}

fn string_field(obj: &serde_json::Map<String, serde_json::Value>, key: &str) -> String {
    obj.get(key)
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default()
        .to_owned()
}

fn string_list(value: Option<&serde_json::Value>) -> Vec<String> {
    value
        .and_then(serde_json::Value::as_array)
        .map(|values| {
            values
                .iter()
                .filter_map(serde_json::Value::as_str)
                .map(str::to_owned)
                .collect()
        })
        .unwrap_or_default()
}

fn allow_orgs_map(value: Option<&serde_json::Value>) -> BTreeMap<String, Vec<String>> {
    let Some(map) = value.and_then(serde_json::Value::as_object) else {
        return BTreeMap::new();
    };
    map.iter()
        .map(|(org, teams)| (org.clone(), string_list(Some(teams))))
        .collect()
}

/// `extsvc.NormalizeBaseURL`: lowercase the host and keep a trailing `/` on the
/// path (`internal/extsvc/codehost.go:86-92`).
fn normalize_base_url(raw: &str) -> Option<String> {
    let mut url = url::Url::parse(raw).ok()?;
    let host = url.host_str()?.to_ascii_lowercase();
    url.set_host(Some(&host)).ok()?;
    if !url.path().ends_with('/') {
        let path = format!("{}/", url.path());
        url.set_path(&path);
    }
    Some(url.to_string())
}

fn github_configs(site: &SiteConfiguration) -> Vec<GithubConfig> {
    let entries = site
        .raw()
        .get("auth.providers")
        .and_then(serde_json::Value::as_array)
        .map(Vec::as_slice)
        .unwrap_or_default();
    let mut configs = Vec::new();
    for entry in entries {
        let Some(obj) = entry.as_object() else {
            continue;
        };
        if obj.get("type").and_then(serde_json::Value::as_str) != Some(GITHUB_SERVICE_TYPE) {
            continue;
        }
        let raw_url = match string_field(obj, "url") {
            url if url.is_empty() => DEFAULT_GITHUB_URL.to_owned(),
            url => url,
        };
        // `parseProvider` drops providers whose URL does not parse.
        let Some(service_id) = normalize_base_url(&raw_url) else {
            continue;
        };
        configs.push(GithubConfig {
            service_id,
            client_id: string_field(obj, "clientID"),
            allow_signup: obj
                .get("allowSignup")
                .and_then(serde_json::Value::as_bool)
                .unwrap_or(false),
            base_url: raw_url,
            allow_orgs: string_list(obj.get("allowOrgs")),
            allow_orgs_map: allow_orgs_map(obj.get("allowOrgsMap")),
        });
    }
    configs
}

fn gitlab_providers(site: &SiteConfiguration) -> Vec<GitlabOAuthProvider> {
    let external_url = site
        .raw()
        .get("externalURL")
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default();
    let (providers, _problems) =
        gitlab_oauth::parse_gitlab_providers(external_url, &gitlab_oauth::specs_from_site(site));
    providers
}

/// The M9.8js `OidcProvisionerFactory` implementation over
/// `user_provisioning`: `CreateIfNotExist = allowSignup == nil || *allowSignup`
/// (`openidconnect/user.go:92`).
struct WiringOidcProvisionerFactory {
    pool: PgPool,
    dotcom: bool,
}

impl crate::oidc_middleware::OidcProvisionerFactory for WiringOidcProvisionerFactory {
    fn for_config(&self, config: &OpenIdConnectAuthProvider) -> Arc<dyn OidcProvisioner> {
        Arc::new(OidcProvisionerAdapter {
            pool: self.pool.clone(),
            dotcom: self.dotcom,
            create_if_not_exist: config.allow_signup.unwrap_or(true),
        })
    }
}

/// `state.ProviderID` from a generic OAuth state (`oauth.LoginState`).
fn oauth_state_provider_id(state: Option<&str>) -> Option<String> {
    state
        .and_then(LoginState::decode)
        .map(|state| state.provider_id)
}

/// `AuthnState.ProviderID` from an OIDC state (`{"csrfToken", "redirect", "p"}`,
/// `openidconnect/middleware.go:461-467`).
fn oidc_state_provider_id(state: Option<&str>) -> Option<String> {
    let bytes = URL_SAFE_NO_PAD.decode(state?).ok()?;
    let value: serde_json::Value = serde_json::from_slice(&bytes).ok()?;
    value
        .get("p")
        .and_then(serde_json::Value::as_str)
        .map(str::to_owned)
        .filter(|id| !id.is_empty())
}

fn now_unix() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs() as i64)
        .unwrap_or_default()
}

// ---------------------------------------------------------------------------
// GitHub adapter
// ---------------------------------------------------------------------------

/// Owned `OAuthUserResolver` wrapper: the registry needs `'static` resolvers
/// while `GithubOAuthResolver` borrows its transport and provisioner.
struct GithubResolverAdapter {
    service_id: String,
    client_id: String,
    allow_signup: bool,
    base_url: String,
    dotcom: bool,
    allow_orgs: Vec<String>,
    allow_orgs_map: BTreeMap<String, Vec<String>>,
    http: Arc<dyn GithubHttpTransport>,
    pool: PgPool,
}

#[async_trait]
impl OAuthUserResolver for GithubResolverAdapter {
    fn service_id(&self) -> &str {
        &self.service_id
    }

    fn success_event_name(&self) -> &str {
        crate::github_oauth::GITHUB_AUTH_SUCCEEDED_EVENT
    }

    fn failure_event_name(&self) -> &str {
        crate::github_oauth::GITHUB_AUTH_FAILED_EVENT
    }

    async fn get_or_create_user(&self, token: &OAuthToken) -> UserResolution {
        let store = PgProvisionStore::new(self.pool.clone());
        let suffix = OsUsernameSuffix;
        let resolver = GithubOAuthResolver::new(
            &self.service_id,
            &self.client_id,
            self.allow_signup,
            &self.base_url,
            self.http.as_ref(),
            &store,
            &suffix,
            self.dotcom,
        )
        .with_org_restrictions(self.allow_orgs.clone(), self.allow_orgs_map.clone());
        resolver.get_or_create_user(token).await
    }
}

// ---------------------------------------------------------------------------
// Wiring entry point
// ---------------------------------------------------------------------------

/// Production dependencies of the resolver registry. Reused across site
/// configuration generations; [`Self::register`] builds a fresh registry's
/// contents from the live snapshot.
pub struct OAuthResolverWiring {
    pool: PgPool,
    dotcom: bool,
    github_http: Arc<dyn GithubHttpTransport>,
    gitlab_http: Arc<dyn GitlabApiClient>,
    oidc_http: Arc<dyn OidcHttpClient>,
}

impl OAuthResolverWiring {
    /// Production construction (`Reqwest*` transports, dotcom mode from the
    /// process environment).
    pub fn new(pool: PgPool) -> Self {
        let getenv = |key: &str| std::env::var(key).ok();
        Self::with_clients(
            pool,
            crate::service_connections::sourcegraph_dotcom_mode(&getenv),
            Arc::new(ReqwestGithubHttpClient::from_default_client()),
            Arc::new(ReqwestGitlabApiClient::from_default_client()),
            Arc::new(ReqwestOidcHttpClient::from_default_client()),
        )
    }

    /// Injectable construction (differential harnesses / tests).
    pub fn with_clients(
        pool: PgPool,
        dotcom: bool,
        github_http: Arc<dyn GithubHttpTransport>,
        gitlab_http: Arc<dyn GitlabApiClient>,
        oidc_http: Arc<dyn OidcHttpClient>,
    ) -> Self {
        Self {
            pool,
            dotcom,
            github_http,
            gitlab_http,
            oidc_http,
        }
    }

    /// The discovery/JWKS/userinfo transport, shared with the OIDC middleware
    /// (M9.8js).
    pub fn oidc_http(&self) -> Arc<dyn OidcHttpClient> {
        Arc::clone(&self.oidc_http)
    }

    /// The M9.8jp-backed provisioner factory the OIDC middleware uses for its
    /// `getOrCreateUser` call.
    pub fn oidc_provisioner_factory(
        &self,
    ) -> Arc<dyn crate::oidc_middleware::OidcProvisionerFactory> {
        Arc::new(WiringOidcProvisionerFactory {
            pool: self.pool.clone(),
            dotcom: self.dotcom,
        })
    }

    /// `dotcom.SourcegraphDotComMode()` captured at construction.
    pub fn dotcom(&self) -> bool {
        self.dotcom
    }

    /// Registers the GitHub/GitLab/OIDC resolvers for one site configuration
    /// snapshot. Each registration is a factory keyed on the callback's `state`
    /// so multi-provider setups pick the provider the state names, and so OIDC
    /// can use the state as its id_token nonce.
    pub fn register(&self, registry: &OAuthUserResolverRegistry, site: &SiteConfiguration) {
        self.register_github(registry, site);
        self.register_gitlab(registry, site);
        self.register_oidc(registry, site);
    }

    fn register_github(&self, registry: &OAuthUserResolverRegistry, site: &SiteConfiguration) {
        let configs = github_configs(site);
        if configs.is_empty() {
            return;
        }
        let http = Arc::clone(&self.github_http);
        let pool = self.pool.clone();
        let dotcom = self.dotcom;
        registry.register_factory(
            GITHUB_SERVICE_TYPE,
            Arc::new(move |state: Option<&str>| {
                let provider_id = oauth_state_provider_id(state);
                let config = provider_id
                    .as_deref()
                    .and_then(|id| configs.iter().find(|config| config.config_id() == id))
                    .unwrap_or(&configs[0]);
                Arc::new(GithubResolverAdapter {
                    service_id: config.service_id.clone(),
                    client_id: config.client_id.clone(),
                    allow_signup: config.allow_signup,
                    base_url: config.base_url.clone(),
                    dotcom,
                    allow_orgs: config.allow_orgs.clone(),
                    allow_orgs_map: config.allow_orgs_map.clone(),
                    http: Arc::clone(&http),
                    pool: pool.clone(),
                }) as Arc<dyn OAuthUserResolver>
            }),
        );
    }

    fn register_gitlab(&self, registry: &OAuthUserResolverRegistry, site: &SiteConfiguration) {
        let providers = gitlab_providers(site);
        if providers.is_empty() {
            return;
        }
        let api = Arc::clone(&self.gitlab_http);
        let pool = self.pool.clone();
        let dotcom = self.dotcom;
        registry.register_factory(
            gitlab_oauth::GITLAB_SERVICE_TYPE,
            Arc::new(move |state: Option<&str>| {
                let provider_id = oauth_state_provider_id(state);
                let provider = provider_id
                    .as_deref()
                    .and_then(|id| providers.iter().find(|provider| provider.config_id() == id))
                    .unwrap_or(&providers[0]);
                let provisioner: Arc<dyn GitlabProvisioner> = Arc::new(GitlabProvisionerAdapter {
                    pool: pool.clone(),
                    dotcom,
                });
                Arc::new(GitlabUserResolver::new(
                    provider,
                    Arc::clone(&api),
                    provisioner,
                    dotcom,
                )) as Arc<dyn OAuthUserResolver>
            }),
        );
    }

    fn register_oidc(&self, registry: &OAuthUserResolverRegistry, site: &SiteConfiguration) {
        let configs = crate::oidc_middleware::oidc_configs(site);
        if configs.is_empty() {
            return;
        }
        let http = Arc::clone(&self.oidc_http);
        let pool = self.pool.clone();
        let dotcom = self.dotcom;
        registry.register_factory(
            "openidconnect",
            Arc::new(move |state: Option<&str>| {
                let provider_id = oidc_state_provider_id(state);
                let config = provider_id
                    .as_deref()
                    .and_then(|id| {
                        configs
                            .iter()
                            .find(|config| oidc_login::provider_config_id(config) == id)
                    })
                    .unwrap_or(&configs[0]);
                // `CreateIfNotExist = AllowSignup == nil || *AllowSignup`
                // (`openidconnect/user.go:92`).
                let create_if_not_exist = config.allow_signup.unwrap_or(true);
                let provisioner: Arc<dyn OidcProvisioner> = Arc::new(OidcProvisionerAdapter {
                    pool: pool.clone(),
                    dotcom,
                    create_if_not_exist,
                });
                let resolver = OidcLoginResolver::new(
                    config.clone(),
                    Arc::clone(&http),
                    provisioner,
                    now_unix(),
                )
                // The baseline always compares the id_token nonce with the
                // state parameter, including the empty case.
                .with_expected_nonce(state.unwrap_or_default());
                Arc::new(resolver) as Arc<dyn OAuthUserResolver>
            }),
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalize_base_url_lowercases_host_and_adds_trailing_slash() {
        assert_eq!(
            normalize_base_url("https://GitHub.Example/Path").as_deref(),
            Some("https://github.example/Path/")
        );
        assert_eq!(
            normalize_base_url("https://github.com/").as_deref(),
            Some("https://github.com/")
        );
        assert_eq!(normalize_base_url("not a url"), None);
    }

    #[test]
    fn oidc_state_provider_id_reads_p_field() {
        // {"csrfToken":"x","redirect":"/search","p":"https://idp/::cid"}
        let json = br#"{"csrfToken":"x","redirect":"/search","p":"https://idp/::cid"}"#;
        let encoded = URL_SAFE_NO_PAD.encode(json);
        assert_eq!(
            oidc_state_provider_id(Some(&encoded)).as_deref(),
            Some("https://idp/::cid")
        );
        assert_eq!(oidc_state_provider_id(Some("!!!")), None);
        assert_eq!(oidc_state_provider_id(None), None);
    }

    #[test]
    fn oauth_state_provider_id_reads_login_state() {
        let state = LoginState {
            redirect: "/search".to_owned(),
            provider_id: "https://gitlab.com/::client".to_owned(),
            csrf: "csrf".to_owned(),
            op: "create".to_owned(),
        };
        let encoded = state.encode();
        assert_eq!(
            oauth_state_provider_id(Some(&encoded)).as_deref(),
            Some("https://gitlab.com/::client")
        );
    }

    fn lazy_pool() -> PgPool {
        sqlx::postgres::PgPoolOptions::new()
            .connect_lazy("postgres://sourcegraph:sourcegraph@localhost/sourcegraph")
            .expect("lazy pool")
    }

    fn wiring() -> OAuthResolverWiring {
        OAuthResolverWiring::with_clients(
            lazy_pool(),
            false,
            Arc::new(crate::github_oauth::ReqwestGithubHttpClient::from_default_client()),
            Arc::new(ReqwestGitlabApiClient::from_default_client()),
            Arc::new(ReqwestOidcHttpClient::from_default_client()),
        )
    }

    fn site() -> SiteConfiguration {
        SiteConfiguration::parse(
            r#"{
                "externalURL": "https://sourcegraph.example.com",
                "auth.providers": [
                    {"type": "github", "url": "https://GitHub.Example", "clientID": "gh"},
                    {"type": "gitlab", "url": "https://gitlab.example", "clientID": "one"},
                    {"type": "gitlab", "url": "https://gitlab.example", "clientID": "two"},
                    {"type": "openidconnect", "issuer": "https://idp.example",
                     "clientID": "oidc", "allowSignup": false}
                ]
            }"#,
        )
        .expect("site config")
    }

    #[tokio::test]
    async fn register_creates_one_resolver_per_oauth_service_type() {
        let registry = OAuthUserResolverRegistry::new();
        wiring().register(&registry, &site());
        assert_eq!(
            registry
                .get_for("github", None)
                .map(|r| r.service_id().to_owned()),
            Some("https://github.example/".to_owned())
        );
        assert_eq!(
            registry
                .get_for("gitlab", None)
                .map(|r| r.service_id().to_owned()),
            Some("https://gitlab.example/".to_owned())
        );
        assert_eq!(
            registry
                .get_for("openidconnect", Some(""))
                .map(|r| r.service_id().to_owned()),
            Some("https://idp.example".to_owned())
        );
        assert!(registry.get_for("azure", None).is_none());
    }

    #[tokio::test]
    async fn register_selects_the_provider_named_by_the_state() {
        let registry = OAuthUserResolverRegistry::new();
        wiring().register(&registry, &site());
        let state = LoginState {
            redirect: "/search".to_owned(),
            provider_id: "https://gitlab.example/::two".to_owned(),
            csrf: "csrf".to_owned(),
            op: "create".to_owned(),
        }
        .encode();
        let resolver = registry
            .get_for("gitlab", Some(&state))
            .expect("gitlab resolver");
        assert_eq!(resolver.service_id(), "https://gitlab.example/");
        // The unknown provider falls back to the first configured provider.
        assert!(registry.get_for("gitlab", Some("garbage")).is_some());
    }
}
