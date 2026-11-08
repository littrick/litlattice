#![forbid(unsafe_code)]

//! `internal/authz/providers/providers.go`: `ProvidersFromConfig`, reduced to
//! the provider identities (`URN`, `ServiceType`, `ServiceID`) plus the
//! validation problems, warnings and invalid connections the baseline returns.
//!
//! Ported kind: GitHub (`internal/authz/providers/github/authz.go`). The
//! discovery loop lists the same seven external service kinds, parses and
//! decrypts each configuration and constructs a GitHub authz provider exactly
//! like `github.NewAuthzProviders` (authorization gate, ACLs license, URL
//! parsing, `groupsCacheTTL` defaults, the `allowGroupsPermissionsSync`
//! warning). Connections of the other kinds are discovered but produce no
//! provider yet (docs §60.4); the fixture therefore only contains GitHub and
//! unauthorized non-GitHub services.
//!
//! The GitHub provider's fetch methods are not ported yet, so this module is
//! not wired into the permission syncer. It exists so the scheduler and the
//! syncer can share one implementation once the GitHub client lands.

use std::collections::BTreeMap;

use serde_json::Value;
use sg_config::SiteConfiguration;
use sg_db::external_services::{ExternalService, ExternalServiceStore};
use sg_keyring::Keyring;
use sqlx::postgres::PgPool;
use url::Url;

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

/// The identity of one constructed `authz.Provider`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProviderIdentity {
    pub urn: String,
    pub service_type: String,
    pub service_id: String,
}

/// `authz/types.ProviderInitResult`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ProviderInitResult {
    pub providers: Vec<ProviderIdentity>,
    /// The baseline calls these "serious problems": they make Sourcegraph
    /// set `authz.allowAccessByDefault` to false.
    pub serious_problems: Vec<String>,
    pub warnings: Vec<String>,
    /// `extsvc.TypeGitHub`-style lowercase service types.
    pub invalid_connections: Vec<String>,
}

/// Inputs `ProvidersFromConfig` reads from global state in the baseline.
#[derive(Debug, Clone, Default)]
pub struct ProvidersOptions {
    /// `licensing.Check(licensing.FeatureACLs)`: `None` when the current
    /// license activates the feature, otherwise the error text to report as a
    /// serious problem.
    pub acls_license_error: Option<String>,
    /// `ExperimentalFeatures.EnableGithubInternalRepoVisibility`.
    pub enable_github_internal_repo_visibility: bool,
}

/// One parsed GitHub external service.
struct GithubConnection {
    service: ExternalService,
    config: Value,
}

/// The provider state `ProviderInitResult` cannot express but the
/// `allowGroupsPermissionsSync` warning needs.
struct GithubProviderState {
    identity: ProviderIdentity,
    groups_cache: bool,
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
            }
            // Other kinds are not ported yet; see the module docs.
        }

        if page_len < SERVICE_PAGE_LIMIT as usize {
            break;
        }
    }

    let github = github_new_authz_providers(&github_conns, site, opts);

    ProviderInitResult {
        providers: github.providers,
        serious_problems: {
            serious_problems.extend(github.serious_problems);
            serious_problems
        },
        warnings: github.warnings,
        invalid_connections: github.invalid_connections,
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
    opts: &ProvidersOptions,
) -> ProviderInitResult {
    let mut result = ProviderInitResult::default();
    let auth_providers = github_auth_providers(site);

    for conn in conns {
        let provider = match new_github_authz_provider(conn, opts) {
            Ok(Some(provider)) => provider,
            Ok(None) => continue,
            Err(err) => {
                result.invalid_connections.push(TYPE_GITHUB.to_owned());
                result.serious_problems.push(err);
                continue;
            }
        };

        if let Some(auth_provider) = auth_providers.get(&provider.identity.service_id) {
            if provider.groups_cache && !auth_provider.allow_groups_permissions_sync {
                // Groups permissions require the auth provider to request the
                // correct scopes.
                let service_id = &provider.identity.service_id;
                result.warnings.push(format!(
                    "GitHub config for {service_id} has `authorization.groupsCacheTTL` enabled, but \
                     the authentication provider matching {service_id:?} does not have \
                     `allowGroupsPermissionsSync` enabled. Update the [**site \
                     configuration**](/site-admin/configuration) in the appropriate entry in \
                     [`auth.providers`](https://sourcegraph.com/docs/admin/auth) to enable this."
                ));
            }
        }

        result.providers.push(provider.identity);
    }

    result
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

/// `github.newAuthzProvider`, reduced to the provider state the init result
/// exposes.
fn new_github_authz_provider(
    conn: &GithubConnection,
    opts: &ProvidersOptions,
) -> Result<Option<GithubProviderState>, String> {
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

    // "Disable by default for now": a non-positive TTL disables the groups
    // cache; a positive TTL enables it.
    let groups_cache_ttl = authorization
        .get("groupsCacheTTL")
        .and_then(Value::as_i64)
        .unwrap_or(0);

    // `GroupsCacheTTL >= 0` after the baseline's `<= 0` coercion.
    let groups_cache = groups_cache_ttl > 0;

    Ok(Some(GithubProviderState {
        identity: ProviderIdentity {
            urn: sg_extsvc::urn(&conn.service.kind, conn.service.id),
            service_type: TYPE_GITHUB.to_owned(),
            service_id: normalize_base_url(&base_url),
        },
        groups_cache,
    }))
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
