#![forbid(unsafe_code)]

//! `internal/authz/providers/providers.go`: `ProvidersFromConfig`, producing
//! the constructed providers plus the validation problems, warnings and
//! invalid connections the baseline returns.
//!
//! Ported kind: GitHub (`internal/authz/providers/github/authz.go`). The
//! discovery loop lists the same seven external service kinds, parses and
//! decrypts each configuration and constructs a GitHub authz provider exactly
//! like `github.NewAuthzProviders` (authorization gate, ACLs license, URL
//! parsing, `groupsCacheTTL` defaults, the `allowGroupsPermissionsSync`
//! warning and the cache it forcibly disables). Each provider is wired with a
//! [`GithubRestClient`] for the code host and, when `authorization.groupsCacheTTL`
//! is positive, a [`RedisGroupsCache`] over the caller's Redis pool — the same
//! `gh_groups_perms:<serviceID>:<urn>` key space as the baseline.
//!
//! The user-centric half (`FetchUserPerms`) is constructed here too, wired to
//! the same code host client, groups cache and `authorization` flags
//! (`syncInternalRepoPermissions`, `groupsCacheTTL`), plus the user external
//! account keyring and Postgres token store the OAuth refresh path needs (see
//! [`super::authz_github_user`]).
//!
//! Ported kind: GitLab (`internal/authz/providers/gitlab/authz.go`), for both
//! identity providers. It decrypts the config, applies the authorization
//! gate, the ACLs license check and the URL parse error. The `username`
//! branch constructs a [`SudoProvider`] wired to a [`GitlabRestClientProvider`]
//! (`!markInternalReposAsPublic` becomes `syncInternalRepoPermissions`, like
//! the baseline), the user external account keyring (to decrypt
//! `account_data` in `FetchUserPerms`) and the database-backed
//! `gitLabProjectVisibilityExperimental` lookup. The `oauth` branch matches
//! the code host against the `auth.providers` GitLab entries (hostname
//! comparison, empty URL defaults to gitlab.com, parse errors skipped) and
//! constructs an [`OAuthProvider`] wired to the same client provider, the
//! Postgres token store, the keyring and the site's GitLab auth providers for
//! the runtime `GetOAuthContext` lookup (docs §70).
//!
//! Not ported yet: GitHub App authentication (`gitHubAppDetails` /
//! `githubAppInstallationID`) is reported as a serious problem because the
//! installation-token auther is not available; connections of the other kinds
//! are discovered but produce no provider (docs §60.4).

use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use serde_json::Value;
use sg_config::{LiveConfig, SiteConfiguration};
use sg_db::external_services::{ExternalService, ExternalServiceStore};
use sg_keyring::Keyring;
use sqlx::postgres::PgPool;
use url::Url;

use super::authz_github::{
    GithubRepoPermsProvider, GroupsCache, MemoryGroupsCache, RedisGroupsCache,
};
use super::authz_github_client::GithubRestClient;
use super::authz_github_user::{
    github_oauth_providers, GithubOAuthProvider, GithubUserClientFactory, GithubUserPerms,
    GithubUserPermsProvider, PgOAuthTokenStore,
};
use super::authz_gitlab::{
    gitlab_oauth_providers, GitlabFeatureFlags, GitlabOAuthProvider, OAuthProvider, SudoProvider,
    GITLAB_PROJECT_VISIBILITY_EXPERIMENTAL, TYPE_GITLAB,
};
use super::authz_gitlab_client::GitlabRestClientProvider;
use super::perms_syncer::{AuthzProvider, ProviderRegistry};

/// `providers.ProvidersFromConfig`'s external service kinds, in its order.
pub const AUTHZ_SERVICE_KINDS: &[&str] = &[
    "AZUREDEVOPS",
    "BITBUCKETCLOUD",
    "BITBUCKETSERVER",
    "GERRIT",
    "GITHUB",
    "GITLAB",
    "PERFORCE",
];

/// `ExternalServicesListOptions.LimitOffset.Limit` ("The number is randomly
/// chosen" in the baseline).
pub const SERVICE_PAGE_LIMIT: i64 = 500;

/// `extsvc.TypeGitHub`.
pub const TYPE_GITHUB: &str = "github";
/// `schema.DefaultGitHubURL`.
const DEFAULT_GITHUB_URL: &str = "https://github.com/";

/// `authz/types.ProviderInitResult`.
#[derive(Default)]
pub struct ProviderInitResult {
    /// `authz.Provider` instances, in construction order.
    pub providers: Vec<Arc<dyn AuthzProvider>>,
    /// The baseline calls these "serious problems": they make Sourcegraph
    /// set `authz.allowAccessByDefault` to false.
    pub serious_problems: Vec<String>,
    pub warnings: Vec<String>,
    /// `extsvc.TypeGitHub`-style lowercase service types.
    pub invalid_connections: Vec<String>,
}

/// Inputs `ProvidersFromConfig` reads from global state in the baseline.
#[derive(Clone, Default)]
pub struct ProvidersOptions {
    /// `licensing.Check(licensing.FeatureACLs)`: `None` when the current
    /// license activates the feature, otherwise the error text to report as a
    /// serious problem.
    pub acls_license_error: Option<String>,
    /// `ExperimentalFeatures.EnableGithubInternalRepoVisibility`.
    pub enable_github_internal_repo_visibility: bool,
    /// `redispool.Cache`, the Redis pool the groups cache uses. `None` falls
    /// back to an in-memory cache for callers without a Redis pool; production
    /// passes the shared pool so the cache lives in Redis like the baseline.
    pub cache_kv: Option<sg_redis::KeyValue>,
}

/// One parsed GitHub external service.
struct GithubConnection {
    service: ExternalService,
    config: Value,
}

/// One parsed GitLab external service.
struct GitlabConnection {
    service: ExternalService,
    config: Value,
}

/// `featureflag.FromContext(ctx)` as attached by the perms syncer's
/// `WithFlags(ctx, db.FeatureFlags())`: the global (non-rollout) value of
/// `gitLabProjectVisibilityExperimental`. Any storage error or a rollout flag
/// means the flag is absent, so `GetBoolOr`'s default (`false`) applies.
struct PgGitlabFeatureFlags {
    pool: PgPool,
}

#[async_trait]
impl GitlabFeatureFlags for PgGitlabFeatureFlags {
    async fn project_visibility_experimental(&self) -> bool {
        match sg_db::feature_flags::get_feature_flag(
            &self.pool,
            GITLAB_PROJECT_VISIBILITY_EXPERIMENTAL,
        )
        .await
        {
            Ok(flag) => flag.evaluate_global().unwrap_or(false),
            Err(_) => false,
        }
    }
}

/// One constructed GitHub provider plus the warning the
/// `allowGroupsPermissionsSync` check emitted (if any).
struct GithubProviderBuild {
    provider: Arc<GithubRepoPermsProvider>,
    warning: Option<String>,
}

/// `ProvidersFromConfig`: list every authz-capable external service, then
/// build providers per kind. Returns the identities of the providers, any
/// serious problems that must disable default access, warnings and invalid
/// connections, in the baseline's order.
pub async fn providers_from_config(
    pool: &PgPool,
    site: &SiteConfiguration,
    keyring: &Keyring,
    opts: &ProvidersOptions,
) -> ProviderInitResult {
    let store = ExternalServiceStore::new(pool.clone());

    let mut github_conns = Vec::new();
    let mut gitlab_conns = Vec::new();
    let mut serious_problems = Vec::new();
    let mut after_id = 0_i64;

    loop {
        let services = match store
            .list_by_kinds_after(AUTHZ_SERVICE_KINDS, after_id, SERVICE_PAGE_LIMIT)
            .await
        {
            Ok(services) => services,
            Err(err) => {
                serious_problems.push(format!("Could not list external services: {err}"));
                break;
            }
        };
        if services.is_empty() {
            break;
        }
        let page_len = services.len();
        after_id = services
            .last()
            .map(|service| service.id)
            .unwrap_or(after_id);

        for service in services {
            let config = match parse_service_config(keyring, &service).await {
                Ok(config) => config,
                Err(err) => {
                    serious_problems.push(format!(
                        "Could not parse config of external service {}: {err}",
                        service.id
                    ));
                    continue;
                }
            };

            if service.kind == "GITHUB" {
                github_conns.push(GithubConnection { service, config });
            } else if service.kind == "GITLAB" {
                gitlab_conns.push(GitlabConnection { service, config });
            }
            // Other kinds are not ported yet; see the module docs.
        }

        if page_len < SERVICE_PAGE_LIMIT as usize {
            break;
        }
    }

    // `authz.go`: GitHub providers first, then GitLab. The OAuth branch needs
    // `auth.providers`, which `newAuthzProvider` receives as its `ps` argument.
    let gitlab_authn = gitlab_oauth_providers(site);
    let github = github_new_authz_providers(&github_conns, site, keyring, pool, opts);
    let gitlab = gitlab_new_authz_providers(&gitlab_conns, &gitlab_authn, keyring, pool, opts);

    let mut providers = github.providers;
    providers.extend(gitlab.providers);
    let mut warnings = github.warnings;
    warnings.extend(gitlab.warnings);
    let mut invalid_connections = github.invalid_connections;
    invalid_connections.extend(gitlab.invalid_connections);

    ProviderInitResult {
        providers,
        serious_problems: {
            serious_problems.extend(github.serious_problems);
            serious_problems.extend(gitlab.serious_problems);
            serious_problems
        },
        warnings,
        invalid_connections,
    }
}

/// `extsvc.ParseEncryptableConfig`: decrypt with the external service keyring
/// and parse the JSONC configuration.
async fn parse_service_config(
    keyring: &Keyring,
    service: &ExternalService,
) -> Result<Value, String> {
    let plain = sg_encryption::maybe_decrypt(
        keyring.external_service_key().as_deref(),
        &service.config,
        &service.encryption_key_id,
    )
    .await
    .map_err(|err| err.to_string())?;
    serde_json::from_str(&sg_config::strip_jsonc(&plain)).map_err(|err| err.to_string())
}

/// `github.NewAuthzProviders`.
fn github_new_authz_providers(
    conns: &[GithubConnection],
    site: &SiteConfiguration,
    keyring: &Keyring,
    pool: &PgPool,
    opts: &ProvidersOptions,
) -> ProviderInitResult {
    let mut result = ProviderInitResult::default();
    let auth_providers = github_auth_providers(site);
    // `auth.providers`, in configuration order: `GetOAuthContext` returns the
    // first GitHub entry whose raw `url` prefixes the service ID.
    let oauth_providers = github_oauth_providers(site);

    for conn in conns {
        let build = match new_github_authz_provider(
            conn,
            opts,
            &auth_providers,
            &oauth_providers,
            keyring,
            pool,
        ) {
            Ok(Some(build)) => build,
            Ok(None) => continue,
            Err(err) => {
                result.invalid_connections.push(TYPE_GITHUB.to_owned());
                result.serious_problems.push(err);
                continue;
            }
        };

        if let Some(warning) = build.warning {
            result.warnings.push(warning);
        }
        let provider: Arc<dyn AuthzProvider> = build.provider;
        result.providers.push(provider);
    }

    result
}

/// `gitlab.NewAuthzProviders`: build one provider per connection, reporting
/// construction failures as serious problems for `extsvc.TypeGitLab`.
fn gitlab_new_authz_providers(
    conns: &[GitlabConnection],
    authn_providers: &[GitlabOAuthProvider],
    keyring: &Keyring,
    pool: &PgPool,
    opts: &ProvidersOptions,
) -> ProviderInitResult {
    let mut result = ProviderInitResult::default();
    for conn in conns {
        match new_gitlab_authz_provider(conn, authn_providers, opts, keyring, pool) {
            Ok(Some(provider)) => result.providers.push(provider),
            Ok(None) => {}
            Err(err) => {
                result.invalid_connections.push(TYPE_GITLAB.to_owned());
                result.serious_problems.push(err);
            }
        }
    }
    result
}

/// `gitlab.newAuthzProvider`: `Ok(None)` when authorization is disabled, `Err`
/// for the baseline's "serious problems".
fn new_gitlab_authz_provider(
    conn: &GitlabConnection,
    authn_providers: &[GitlabOAuthProvider],
    opts: &ProvidersOptions,
    keyring: &Keyring,
    pool: &PgPool,
) -> Result<Option<Arc<dyn AuthzProvider>>, String> {
    let Some(authorization) = conn
        .config
        .get("authorization")
        .filter(|value| !value.is_null())
    else {
        return Ok(None);
    };

    if let Some(message) = &opts.acls_license_error {
        return Err(message.clone());
    }

    let raw_url = conn
        .config
        .get("url")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let base_url = Url::parse(raw_url)
        .map_err(|err| format!("Could not parse URL for GitLab instance {raw_url:?}: {err}"))?;

    let identity_provider = authorization
        .get("identityProvider")
        .and_then(|idp| idp.get("type"))
        .and_then(Value::as_str);
    match identity_provider {
        Some("username") => {
            // `SyncInternalRepoPermissions: !c.MarkInternalReposAsPublic`.
            let mark_internal_repos_as_public = conn
                .config
                .get("markInternalReposAsPublic")
                .and_then(Value::as_bool)
                .unwrap_or(false);
            let token = conn
                .config
                .get("token")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_owned();
            let service_id = normalize_base_url(&base_url);
            let urn = sg_extsvc::urn(&conn.service.kind, conn.service.id);
            let client_provider = Arc::new(GitlabRestClientProvider::new(&base_url, Some(token)));
            let provider: Arc<dyn AuthzProvider> = Arc::new(
                SudoProvider::new(
                    urn,
                    service_id,
                    client_provider,
                    !mark_internal_repos_as_public,
                )
                .with_user_external_account_key(keyring.user_external_account_key())
                .with_feature_flags(Arc::new(PgGitlabFeatureFlags { pool: pool.clone() })),
            );
            Ok(Some(provider))
        }
        Some("oauth") => {
            // Check that there is a GitLab authn provider corresponding to this
            // GitLab instance. The default URL is gitlab.com; a parse failure is
            // ignored because the authn provider is responsible for its own
            // validation.
            let mut found_auth_provider = false;
            let mut sync_internal_repo_permissions = true;
            for authn_provider in authn_providers {
                let authn_url = if authn_provider.url.is_empty() {
                    "https://gitlab.com"
                } else {
                    &authn_provider.url
                };
                let Ok(auth_provider_url) = Url::parse(authn_url) else {
                    continue;
                };
                if auth_provider_url.host_str() == base_url.host_str() {
                    found_auth_provider = true;
                    sync_internal_repo_permissions = authn_provider
                        .sync_internal_repo_permissions
                        .unwrap_or(true);
                    break;
                }
            }
            if !found_auth_provider {
                return Err(format!(
                    "Did not find authentication provider matching {raw_url:?}. Check the [**site configuration**](/site-admin/configuration) to verify an entry in [`auth.providers`](https://sourcegraph.com/docs/admin/auth) exists for {raw_url}."
                ));
            }

            let service_id = normalize_base_url(&base_url);
            let urn = sg_extsvc::urn(&conn.service.kind, conn.service.id);
            let token = conn
                .config
                .get("token")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_owned();
            let client = GitlabRestClientProvider::new(&base_url, Some(token));
            // The refresher uses the same HTTP client as the API requests,
            // like `oauthutil.DoRequest` passing the client's own doer.
            let http = client.http_client();
            let provider: Arc<dyn AuthzProvider> = Arc::new(
                OAuthProvider::new(
                    urn,
                    service_id,
                    Arc::new(client),
                    sync_internal_repo_permissions,
                    Arc::new(PgOAuthTokenStore::new(
                        pool.clone(),
                        keyring.user_external_account_key(),
                    )),
                    http,
                    authn_providers.to_vec(),
                )
                .with_user_external_account_key(keyring.user_external_account_key())
                .with_feature_flags(Arc::new(PgGitlabFeatureFlags { pool: pool.clone() })),
            );
            Ok(Some(provider))
        }
        // `authorization` without an `identityProvider` leaves both union
        // members nil in the baseline.
        _ => Err("No identityProvider was specified".to_owned()),
    }
}

/// One `schema.GitHubAuthProvider` entry the warning path reads.
struct GithubAuthProviderRef {
    allow_groups_permissions_sync: bool,
}

/// `NewAuthzProviders`'s login-provider map: `auth.providers` entries of type
/// `github`, keyed by normalized code host service id.
fn github_auth_providers(site: &SiteConfiguration) -> BTreeMap<String, GithubAuthProviderRef> {
    let mut providers = BTreeMap::new();
    let Some(entries) = site.raw().get("auth.providers").and_then(Value::as_array) else {
        return providers;
    };

    for entry in entries {
        if entry.get("type").and_then(Value::as_str) != Some(TYPE_GITHUB) {
            continue;
        }
        let url = entry
            .get("url")
            .and_then(Value::as_str)
            .filter(|url| !url.is_empty())
            .unwrap_or(DEFAULT_GITHUB_URL);
        let id = match Url::parse(url) {
            Ok(parsed) => normalize_base_url(&parsed),
            // Error reporting for this should happen elsewhere, for now just
            // use what is given (the baseline).
            Err(_) => url.to_owned(),
        };
        providers.insert(
            id,
            GithubAuthProviderRef {
                allow_groups_permissions_sync: entry
                    .get("allowGroupsPermissionsSync")
                    .and_then(Value::as_bool)
                    .unwrap_or(false),
            },
        );
    }

    providers
}

/// `github.newAuthzProvider`: `Ok(None)` when authorization is disabled, `Err`
/// for the baseline's "serious problems".
fn new_github_authz_provider(
    conn: &GithubConnection,
    opts: &ProvidersOptions,
    auth_providers: &BTreeMap<String, GithubAuthProviderRef>,
    oauth_providers: &[GithubOAuthProvider],
    keyring: &Keyring,
    pool: &PgPool,
) -> Result<Option<GithubProviderBuild>, String> {
    let Some(authorization) = conn
        .config
        .get("authorization")
        .filter(|value| !value.is_null())
    else {
        return Ok(None);
    };

    if let Some(message) = &opts.acls_license_error {
        return Err(message.clone());
    }

    let raw_url = conn
        .config
        .get("url")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let base_url = Url::parse(raw_url)
        .map_err(|err| format!("could not parse URL for GitHub instance {raw_url:?}: {err}"))?;

    // The schema type is `float64`; the baseline casts the hours to
    // `time.Duration` (truncating to whole nanoseconds) and then to seconds.
    // "Disable by default for now": a non-positive TTL disables the groups
    // cache; a positive TTL enables it.
    let groups_cache_ttl_hours = authorization
        .get("groupsCacheTTL")
        .and_then(Value::as_f64)
        .unwrap_or(0.0);

    let service_id = normalize_base_url(&base_url);
    let urn = sg_extsvc::urn(&conn.service.kind, conn.service.id);

    // `NewAuthzProviders`: the groups cache requires the matching login
    // provider to request the right scopes; otherwise warn and forcibly
    // disable the cache.
    let mut groups_cache_enabled = groups_cache_ttl_hours > 0.0;
    let warning = match auth_providers.get(&service_id) {
        Some(auth_provider)
            if groups_cache_enabled && !auth_provider.allow_groups_permissions_sync =>
        {
            groups_cache_enabled = false;
            Some(format!(
                "GitHub config for {service_id} has `authorization.groupsCacheTTL` enabled, but \
                 the authentication provider matching {service_id:?} does not have \
                 `allowGroupsPermissionsSync` enabled. Update the [**site \
                 configuration**](/site-admin/configuration) in the appropriate entry in \
                 [`auth.providers`](https://sourcegraph.com/docs/admin/auth) to enable this."
            ))
        }
        _ => None,
    };

    // `auth.FromConnection` (GitHub App installation tokens) is not ported;
    // report it the way the baseline reports a failure to build the auther.
    if has_github_app_details(&conn.config) {
        return Err(
            "GitHub App authentication is not supported by the Rust authz provider yet".to_owned(),
        );
    }

    let token = conn
        .config
        .get("token")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_owned();
    let client = Arc::new(GithubRestClient::new(
        &base_url,
        Some(token),
        opts.enable_github_internal_repo_visibility,
    ));

    let groups_cache: Option<Arc<dyn GroupsCache>> = if groups_cache_enabled {
        let ttl_seconds = (groups_cache_ttl_hours * 3600.0).trunc() as i64;
        let cache: Arc<dyn GroupsCache> = match &opts.cache_kv {
            Some(kv) => Arc::new(RedisGroupsCache::with_ttl(
                kv.clone(),
                &service_id,
                &urn,
                ttl_seconds,
            )),
            None => {
                tracing::warn!(
                    service_id = %service_id,
                    "no Redis pool configured for the GitHub groups cache; using an in-memory cache"
                );
                Arc::new(MemoryGroupsCache::default())
            }
        };
        Some(cache)
    } else {
        None
    };

    // `NewProvider(..., SyncInternalRepoPermissions: c.Authorization != nil &&
    // c.Authorization.SyncInternalRepoPermissions)`.
    let sync_internal_repo_permissions = authorization
        .get("syncInternalRepoPermissions")
        .and_then(Value::as_bool)
        .unwrap_or(false);

    let user_external_account_key = keyring.user_external_account_key();
    let user_client_factory: Arc<dyn GithubUserClientFactory> = client.clone();
    let user_perms: Arc<dyn GithubUserPermsProvider> = Arc::new(GithubUserPerms::new(
        service_id.clone(),
        TYPE_GITHUB,
        user_client_factory,
        groups_cache.clone(),
        sync_internal_repo_permissions,
        oauth_providers.to_vec(),
        Arc::new(PgOAuthTokenStore::new(
            pool.clone(),
            user_external_account_key.clone(),
        )),
        user_external_account_key,
        client.http_client(),
    ));

    let provider = Arc::new(
        GithubRepoPermsProvider::new(
            urn,
            service_id,
            TYPE_GITHUB,
            base_url.host_str().unwrap_or_default().to_owned(),
            client,
            groups_cache,
            opts.enable_github_internal_repo_visibility,
        )
        .with_user_perms(user_perms),
    );

    Ok(Some(GithubProviderBuild { provider, warning }))
}

/// `GitHubConnection.GitHubAppDetails != nil`: the current `gitHubAppDetails`
/// object or the deprecated `githubAppInstallationID`.
fn has_github_app_details(config: &Value) -> bool {
    let has_details = config
        .get("gitHubAppDetails")
        .is_some_and(|value| !value.is_null());
    let has_installation_id = config
        .get("githubAppInstallationID")
        .and_then(Value::as_str)
        .is_some_and(|id| !id.is_empty());
    has_details || has_installation_id
}

/// `extsvc.NormalizeBaseURL` + `url.URL.String()`: the host is lowercased and
/// the path always ends in `/`.
fn normalize_base_url(url: &Url) -> String {
    let mut normalized = url.clone();
    if let Some(host) = normalized.host_str() {
        let host = host.to_lowercase();
        // `set_host` cannot express IPv6 literals with their brackets; fall
        // back to the parser-normalized form in that case.
        let _ = normalized.set_host(Some(&host));
    }
    if !normalized.path().ends_with('/') {
        let path = format!("{}/", normalized.path());
        normalized.set_path(&path);
    }
    normalized.to_string()
}

/// `permsSyncerImpl.providerFactory`: reconstruct the providers from the live
/// configuration on every sync, discarding problems and warnings exactly like
/// the baseline (`ps, _, _, _ := providers.ProvidersFromConfig(...)`).
pub struct ConfigAuthzProviders {
    pool: PgPool,
    live_config: Arc<LiveConfig>,
    keyring: Arc<Keyring>,
    cache_kv: Option<sg_redis::KeyValue>,
    acls_error: AclsErrorCheck,
}

/// `licensing.Check(licensing.FeatureACLs)` as an injectable predicate
/// returning the error text; the default reads the site license key.
pub type AclsErrorCheck = Arc<dyn Fn(&str) -> Option<String> + Send + Sync>;

impl ConfigAuthzProviders {
    pub fn new(
        pool: PgPool,
        live_config: Arc<LiveConfig>,
        keyring: Arc<Keyring>,
        cache_kv: Option<sg_redis::KeyValue>,
    ) -> Self {
        Self {
            pool,
            live_config,
            keyring,
            cache_kv,
            acls_error: Arc::new(sg_licensing::check_acls_error),
        }
    }

    /// Test seam replacing the license check (`licensing.MockCheckFeature`).
    pub fn with_acls_error_check(mut self, check: AclsErrorCheck) -> Self {
        self.acls_error = check;
        self
    }
}

#[async_trait]
impl ProviderRegistry for ConfigAuthzProviders {
    async fn providers(&self) -> Vec<Arc<dyn AuthzProvider>> {
        let unified = self.live_config.current();
        let site = &unified.site;
        let opts = ProvidersOptions {
            acls_license_error: (self.acls_error)(&site.license_key()),
            enable_github_internal_repo_visibility: enable_github_internal_repo_visibility(site),
            cache_kv: self.cache_kv.clone(),
        };
        providers_from_config(&self.pool, site, &self.keyring, &opts)
            .await
            .providers
    }
}

/// `ExperimentalFeatures.EnableGithubInternalRepoVisibility` from the raw site
/// configuration (the zero config has no `experimentalFeatures` block).
pub fn enable_github_internal_repo_visibility(site: &SiteConfiguration) -> bool {
    site.raw()
        .get("experimentalFeatures")
        .and_then(|features| features.get("enableGithubInternalRepoVisibility"))
        .and_then(Value::as_bool)
        .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn github_conn(config: Value) -> GithubConnection {
        GithubConnection {
            service: ExternalService {
                id: 1,
                kind: "GITHUB".to_owned(),
                display_name: String::new(),
                config: String::new(),
                encryption_key_id: String::new(),
                last_sync_at: None,
                next_sync_at: None,
                unrestricted: false,
                has_webhooks: None,
                code_host_id: None,
            },
            config,
        }
    }

    fn options() -> ProvidersOptions {
        ProvidersOptions::default()
    }

    fn expect_err<T>(result: Result<T, String>) -> String {
        match result {
            Ok(_) => panic!("expected a serious problem"),
            Err(err) => err,
        }
    }

    /// `new_github_authz_provider` with the production dependencies stubbed:
    /// an unconnected pool and the empty keyring (no OAuth providers in site
    /// config for these cases).
    fn build_provider(
        conn: &GithubConnection,
        opts: &ProvidersOptions,
        auth_providers: &BTreeMap<String, GithubAuthProviderRef>,
    ) -> Result<Option<GithubProviderBuild>, String> {
        let keyring = Keyring::default();
        let pool = sqlx::postgres::PgPoolOptions::new()
            .connect_lazy("postgres://unused@localhost/unused")
            .expect("lazy pool");
        new_github_authz_provider(conn, opts, auth_providers, &[], &keyring, &pool)
    }

    #[tokio::test]
    async fn no_authorization_builds_no_provider() {
        let conn = github_conn(json!({"url": "https://github.example.com", "token": "t"}));
        let build = build_provider(&conn, &options(), &BTreeMap::new()).unwrap();
        assert!(build.is_none());
    }

    #[tokio::test]
    async fn positive_ttl_attaches_groups_cache() {
        let conn = github_conn(json!({
            "url": "https://GITHUB.example.com:8443/",
            "token": "t",
            "authorization": {"groupsCacheTTL": 24}
        }));
        let build = build_provider(&conn, &options(), &BTreeMap::new())
            .unwrap()
            .expect("provider");
        assert!(build.warning.is_none());
        assert_eq!(build.provider.urn(), "extsvc:github:1");
        assert_eq!(build.provider.service_type(), TYPE_GITHUB);
        assert_eq!(
            build.provider.service_id(),
            "https://github.example.com:8443/"
        );
        assert!(build.provider.groups_cache_enabled());
    }

    #[tokio::test]
    async fn non_positive_ttl_disables_groups_cache() {
        for ttl in [json!(-1), json!(0), json!(null)] {
            let conn = github_conn(json!({
                "url": "https://github.example.com",
                "token": "t",
                "authorization": {"groupsCacheTTL": ttl}
            }));
            let build = build_provider(&conn, &options(), &BTreeMap::new())
                .unwrap()
                .expect("provider");
            assert!(!build.provider.groups_cache_enabled());
        }
    }

    #[tokio::test]
    async fn login_provider_without_groups_sync_warns_and_disables_cache() {
        let conn = github_conn(json!({
            "url": "https://github.example.com",
            "token": "t",
            "authorization": {"groupsCacheTTL": 24}
        }));
        let mut auth_providers = BTreeMap::new();
        auth_providers.insert(
            "https://github.example.com/".to_owned(),
            GithubAuthProviderRef {
                allow_groups_permissions_sync: false,
            },
        );

        let build = build_provider(&conn, &options(), &auth_providers)
            .unwrap()
            .expect("provider");
        let warning = build.warning.expect("warning");
        assert!(warning.contains("does not have `allowGroupsPermissionsSync` enabled"));
        assert!(!build.provider.groups_cache_enabled());
    }

    #[tokio::test]
    async fn login_provider_with_groups_sync_keeps_cache() {
        let conn = github_conn(json!({
            "url": "https://github.example.com",
            "token": "t",
            "authorization": {"groupsCacheTTL": 24}
        }));
        let mut auth_providers = BTreeMap::new();
        auth_providers.insert(
            "https://github.example.com/".to_owned(),
            GithubAuthProviderRef {
                allow_groups_permissions_sync: true,
            },
        );

        let build = build_provider(&conn, &options(), &auth_providers)
            .unwrap()
            .expect("provider");
        assert!(build.warning.is_none());
        assert!(build.provider.groups_cache_enabled());
    }

    #[tokio::test]
    async fn acls_license_error_is_a_serious_problem() {
        let conn = github_conn(json!({
            "url": "https://github.example.com",
            "token": "t",
            "authorization": {}
        }));
        let opts = ProvidersOptions {
            acls_license_error: Some("acls unavailable".to_owned()),
            ..ProvidersOptions::default()
        };
        let err = expect_err(build_provider(&conn, &opts, &BTreeMap::new()));
        assert_eq!(err, "acls unavailable");
    }

    #[tokio::test]
    async fn github_app_details_are_not_supported() {
        for config in [
            json!({
                "url": "https://github.example.com",
                "gitHubAppDetails": {"appID": 1, "installationID": 2},
                "authorization": {}
            }),
            json!({
                "url": "https://github.example.com",
                "githubAppInstallationID": "2",
                "authorization": {}
            }),
        ] {
            let conn = github_conn(config);
            let err = expect_err(build_provider(&conn, &options(), &BTreeMap::new()));
            assert!(err.contains("GitHub App authentication is not supported"));
        }
    }

    /// `new_gitlab_authz_provider` with the production dependencies stubbed:
    /// an unconnected pool and the empty keyring.
    fn build_gitlab_provider(
        conn: &GitlabConnection,
        opts: &ProvidersOptions,
    ) -> Result<Option<Arc<dyn AuthzProvider>>, String> {
        let keyring = Keyring::default();
        let pool = sqlx::postgres::PgPoolOptions::new()
            .connect_lazy("postgres://unused@localhost/unused")
            .expect("lazy pool");
        new_gitlab_authz_provider(conn, &[], opts, &keyring, &pool)
    }

    /// Build with an explicit `auth.providers` list (the OAuth branch).
    fn build_gitlab_provider_with_authn(
        conn: &GitlabConnection,
        authn: &[GitlabOAuthProvider],
        opts: &ProvidersOptions,
    ) -> Result<Option<Arc<dyn AuthzProvider>>, String> {
        let keyring = Keyring::default();
        let pool = sqlx::postgres::PgPoolOptions::new()
            .connect_lazy("postgres://unused@localhost/unused")
            .expect("lazy pool");
        new_gitlab_authz_provider(conn, authn, opts, &keyring, &pool)
    }

    fn gitlab_conn(config: Value) -> GitlabConnection {
        GitlabConnection {
            service: ExternalService {
                id: 2,
                kind: "GITLAB".to_owned(),
                display_name: String::new(),
                config: String::new(),
                encryption_key_id: String::new(),
                last_sync_at: None,
                next_sync_at: None,
                unrestricted: false,
                has_webhooks: None,
                code_host_id: None,
            },
            config,
        }
    }

    fn expect_gitlab_provider(result: Option<Arc<dyn AuthzProvider>>) -> Arc<dyn AuthzProvider> {
        result.expect("expected a provider")
    }

    #[tokio::test]
    async fn gitlab_without_authorization_builds_no_provider() {
        let conn = gitlab_conn(json!({"url": "https://gitlab.example.com", "token": "t"}));
        assert!(build_gitlab_provider(&conn, &options()).unwrap().is_none());
    }

    #[tokio::test]
    async fn gitlab_username_provider_identity() {
        let conn = gitlab_conn(json!({
            "url": "https://GITLAB.example.com:8443",
            "token": "t",
            "authorization": {"identityProvider": {"type": "username"}}
        }));
        let provider = expect_gitlab_provider(build_gitlab_provider(&conn, &options()).unwrap());
        assert_eq!(provider.urn(), "extsvc:gitlab:2");
        assert_eq!(provider.service_type(), TYPE_GITLAB);
        assert_eq!(provider.service_id(), "https://gitlab.example.com:8443/");
    }

    #[tokio::test]
    async fn gitlab_missing_identity_provider_is_a_serious_problem() {
        let conn = gitlab_conn(json!({
            "url": "https://gitlab.example.com",
            "token": "t",
            "authorization": {}
        }));
        let err = expect_err(build_gitlab_provider(&conn, &options()));
        assert_eq!(err, "No identityProvider was specified");
    }

    #[tokio::test]
    async fn gitlab_oauth_without_matching_authn_provider_is_a_serious_problem() {
        let conn = gitlab_conn(json!({
            "url": "https://gitlab.example.com",
            "token": "t",
            "authorization": {"identityProvider": {"type": "oauth"}}
        }));
        let err = expect_err(build_gitlab_provider(&conn, &options()));
        assert_eq!(
            err,
            "Did not find authentication provider matching \"https://gitlab.example.com\". Check the [**site configuration**](/site-admin/configuration) to verify an entry in [`auth.providers`](https://sourcegraph.com/docs/admin/auth) exists for https://gitlab.example.com."
        );
    }

    #[tokio::test]
    async fn gitlab_oauth_with_matching_authn_provider_builds_a_provider() {
        let conn = gitlab_conn(json!({
            "url": "https://GITLAB.example.com:8443",
            "token": "t",
            "authorization": {"identityProvider": {"type": "oauth"}}
        }));
        let authn = [GitlabOAuthProvider {
            url: "https://gitlab.example.com:8443".to_owned(),
            client_id: "id".to_owned(),
            client_secret: "secret".to_owned(),
            sync_internal_repo_permissions: None,
        }];
        let provider = expect_gitlab_provider(
            build_gitlab_provider_with_authn(&conn, &authn, &options()).unwrap(),
        );
        assert_eq!(provider.urn(), "extsvc:gitlab:2");
        assert_eq!(provider.service_type(), TYPE_GITLAB);
        assert_eq!(provider.service_id(), "https://gitlab.example.com:8443/");
    }

    #[tokio::test]
    async fn gitlab_oauth_defaults_to_the_gitlab_com_authn_url() {
        let conn = gitlab_conn(json!({
            "url": "https://gitlab.com",
            "token": "t",
            "authorization": {"identityProvider": {"type": "oauth"}}
        }));
        let authn = [GitlabOAuthProvider {
            url: String::new(),
            client_id: "id".to_owned(),
            client_secret: "secret".to_owned(),
            sync_internal_repo_permissions: Some(false),
        }];
        let provider = expect_gitlab_provider(
            build_gitlab_provider_with_authn(&conn, &authn, &options()).unwrap(),
        );
        assert_eq!(provider.service_id(), "https://gitlab.com/");
    }

    #[tokio::test]
    async fn gitlab_acls_license_error_is_a_serious_problem() {
        let conn = gitlab_conn(json!({
            "url": "https://gitlab.example.com",
            "token": "t",
            "authorization": {"identityProvider": {"type": "username"}}
        }));
        let opts = ProvidersOptions {
            acls_license_error: Some("acls unavailable".to_owned()),
            ..ProvidersOptions::default()
        };
        let err = expect_err(build_gitlab_provider(&conn, &opts));
        assert_eq!(err, "acls unavailable");
    }

    #[tokio::test]
    async fn gitlab_invalid_url_is_a_serious_problem() {
        let conn = gitlab_conn(json!({
            "url": "https://exa mple.com",
            "token": "t",
            "authorization": {"identityProvider": {"type": "username"}}
        }));
        let err = expect_err(build_gitlab_provider(&conn, &options()));
        assert!(
            err.starts_with("Could not parse URL for GitLab instance \"https://exa mple.com\": ")
        );
    }
}
