#![forbid(unsafe_code)]

//! `cmd/frontend/internal/cloneurls` (`clone_urls.go:26-157`) plus the
//! `internal/conf/reposource` name derivation for the code-host kinds.
//!
//! `RepoSourceCloneURLToRepoName` tries, in order: the site configuration's
//! `git.cloneURLToRepositoryName` custom mappings, the
//! `external_service_repos.clone_url` fast path, every configured external
//! service (decrypted and parsed as JSONC), and finally a synthetic
//! `https://github.com` mapping. An empty name means "no matching code host";
//! the caller then resolves it as a normal repository lookup.

use std::sync::Arc;

use async_trait::async_trait;
use serde_json::Value;
use sg_config::{parse_jsonc, LiveConfig, SiteConfiguration};
use sg_encryption::Key;
use sg_extsvc::reposource::{
    replace_pattern, AwsCodeCommitRepoSource, BitbucketCloudRepoSource, BitbucketServerRepoSource,
    GerritRepoSource, GitHubRepoSource, GitLabRepoSource, GitoliteRepoSource, OtherRepoSource,
};

use crate::graphql_resolvers::CloneUrlResolver;

/// `clone_urls.go:44-53`: the kinds consulted for clone-URL mapping.
pub const CLONE_URL_KINDS: [&str; 8] = [
    "GITHUB",
    "GITLAB",
    "BITBUCKETSERVER",
    "BITBUCKETCLOUD",
    "AWSCODECOMMIT",
    "GITOLITE",
    "PHABRICATOR",
    "OTHER",
];

/// `database.LimitOffset{Limit: 50}` in `RepoSourceCloneURLToRepoName`.
const PAGE_SIZE: i64 = 50;

/// One external service row, as needed by the clone-URL mapping.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CloneUrlService {
    pub id: i64,
    pub kind: String,
    /// Still encrypted (`external_services.config`).
    pub config: String,
    pub encryption_key_id: String,
}

/// `db.ExternalServices().List` with kinds + `AfterID` paging.
#[async_trait]
pub trait CloneUrlServiceSource: Send + Sync {
    async fn list_after(
        &self,
        kinds: &[&str],
        after_id: i64,
        limit: i64,
    ) -> Result<Vec<CloneUrlService>, String>;
}

/// `repoStore.GetFirstRepoNameByCloneURL`.
#[async_trait]
pub trait FirstRepoNameByCloneUrl: Send + Sync {
    async fn first_repo_name_by_clone_url(&self, clone_url: &str)
        -> Result<Option<String>, String>;
}

/// Production source: the `external_services` table.
pub struct PgCloneUrlServiceSource {
    store: sg_db::external_services::ExternalServiceStore,
}

impl PgCloneUrlServiceSource {
    pub fn new(pool: sqlx::PgPool) -> Self {
        Self {
            store: sg_db::external_services::ExternalServiceStore::new(pool),
        }
    }
}

#[async_trait]
impl CloneUrlServiceSource for PgCloneUrlServiceSource {
    async fn list_after(
        &self,
        kinds: &[&str],
        after_id: i64,
        limit: i64,
    ) -> Result<Vec<CloneUrlService>, String> {
        self.store
            .list_by_kinds_after(kinds, after_id, limit)
            .await
            .map(|services| {
                services
                    .into_iter()
                    .map(|service| CloneUrlService {
                        id: service.id,
                        kind: service.kind,
                        config: service.config,
                        encryption_key_id: service.encryption_key_id,
                    })
                    .collect()
            })
            .map_err(|error| error.to_string())
    }
}

/// Production source: the `repo`/`external_service_repos` join.
pub struct PgFirstRepoNameByCloneUrl {
    pool: sqlx::PgPool,
}

impl PgFirstRepoNameByCloneUrl {
    pub fn new(pool: sqlx::PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl FirstRepoNameByCloneUrl for PgFirstRepoNameByCloneUrl {
    async fn first_repo_name_by_clone_url(
        &self,
        clone_url: &str,
    ) -> Result<Option<String>, String> {
        sg_db::external_service_repos::first_repo_name_by_clone_url(&self.pool, clone_url)
            .await
            .map_err(|error| error.to_string())
    }
}

/// `siteConfig.git.cloneURLToRepositoryName` (`cloneURLResolver` list).
fn custom_resolvers(site: &SiteConfiguration) -> Vec<(regex::Regex, String)> {
    site.raw()
        .get("git.cloneURLToRepositoryName")
        .and_then(Value::as_array)
        .map(|entries| {
            entries
                .iter()
                .filter_map(|entry| {
                    let from = entry.get("from")?.as_str()?;
                    let to = entry.get("to")?.as_str()?;
                    // Compile failures are logged and skipped by the baseline;
                    // the site configuration validator reports them separately.
                    let regex = regex::Regex::new(from).ok()?;
                    Some((regex, to.to_owned()))
                })
                .collect()
        })
        .unwrap_or_default()
}

/// `reposource.CustomCloneURLToRepoName`: named capture groups are substituted
/// into the `to` template's `{name}` placeholders.
pub fn custom_clone_url_to_repo_name(site: &SiteConfiguration, clone_url: &str) -> String {
    for (regex, to) in custom_resolvers(site) {
        let Some(captures) = regex.captures(clone_url) else {
            continue;
        };
        let mut pairs = Vec::new();
        for index in 1..captures.len() {
            let name = regex
                .capture_names()
                .nth(index)
                .flatten()
                .unwrap_or_default();
            let value = captures.get(index).map(|cap| cap.as_str()).unwrap_or("");
            pairs.push((format!("{{{name}}}"), value.to_owned()));
        }
        let borrowed: Vec<(&str, &str)> = pairs
            .iter()
            .map(|(from, to)| (from.as_str(), to.as_str()))
            .collect();
        return replace_pattern(&to, &borrowed);
    }
    String::new()
}

/// `extsvc.ParseEncryptableConfig`: decrypt, then JSONC-parse.
async fn parse_config(service: &CloneUrlService, key: Option<&dyn Key>) -> Result<Value, String> {
    let raw = sg_encryption::maybe_decrypt(key, &service.config, &service.encryption_key_id)
        .await
        .map_err(|error| error.to_string())?;
    parse_jsonc(&raw).map_err(|error| error.to_string())
}

/// `schema.*Connection` field readers. `encoding/json` leaves the zero value
/// for absent/`null` fields and errors on a wrong type.
fn config_string(config: &Value, field: &str, kind: &str) -> Result<String, String> {
    match config.get(field) {
        None | Some(Value::Null) => Ok(String::new()),
        Some(Value::String(value)) => Ok(value.clone()),
        Some(other) => Err(format!(
            "json: cannot unmarshal {} into Go struct field {kind}.{field} of type string",
            json_type_name(other)
        )),
    }
}

fn json_type_name(value: &Value) -> &'static str {
    match value {
        Value::Null => "null",
        Value::Bool(_) => "bool",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}

/// GitLab `nameTransformations` (`[]*schema.GitLabNameTransformation`).
fn name_transformations(config: &Value) -> Result<Vec<(String, String)>, String> {
    let Some(entries) = config.get("nameTransformations") else {
        return Ok(Vec::new());
    };
    let Value::Array(entries) = entries else {
        return Ok(Vec::new());
    };
    let mut transformations = Vec::with_capacity(entries.len());
    for entry in entries {
        let regex = config_string(entry, "regex", "GitLabNameTransformation")?;
        let replacement = config_string(entry, "replacement", "GitLabNameTransformation")?;
        transformations.push((regex, replacement));
    }
    Ok(transformations)
}

/// `getRepoNameFromService`: decrypt + parse the service configuration and map
/// the clone URL. `Ok(None)` means this service does not match (Phabricator
/// always does).
async fn repo_name_from_service(
    service: &CloneUrlService,
    clone_url: &str,
    key: Option<&dyn Key>,
) -> Result<Option<String>, String> {
    let config = parse_config(service, key)
        .await
        .map_err(|error| format!("parse config: {error}"))?;
    // `ParseEncryptableConfig` unmarshals into `any`: a JSON `null` sets the
    // interface to nil, which the baseline's type switch rejects.
    if config.is_null() {
        return Err("parse config: unexpected connection type: <nil>".to_owned());
    }
    if !config.is_object() {
        return Err(
            "parse config: json: cannot unmarshal value into Go value of type struct".to_owned(),
        );
    }

    let kind = service.kind.to_ascii_uppercase();
    let url_field = match kind.as_str() {
        "GITHUB" | "GITLAB" | "BITBUCKETSERVER" | "BITBUCKETCLOUD" | "GERRIT" | "OTHER" => {
            config_string(&config, "url", &kind)?
        }
        // AWS and Gitolite have no `url` field; Phabricator never maps.
        _ => String::new(),
    };
    let pattern = config_string(&config, "repositoryPathPattern", &kind)?;

    // Relative `.gitmodules` URLs are resolved against the code host.
    let mut clone_url = clone_url.to_owned();
    if clone_url.starts_with("../") && !url_field.is_empty() {
        clone_url = resolve_relative(&clone_url, &url_field)?;
    }

    let name = match kind.as_str() {
        "GITHUB" => GitHubRepoSource {
            url: url_field,
            repository_path_pattern: pattern,
        }
        .clone_url_to_repo_name(&clone_url)?,
        "GITLAB" => GitLabRepoSource {
            url: url_field,
            repository_path_pattern: pattern,
            name_transformations: name_transformations(&config)?,
        }
        .clone_url_to_repo_name(&clone_url)?,
        "BITBUCKETCLOUD" => BitbucketCloudRepoSource {
            url: url_field,
            repository_path_pattern: pattern,
        }
        .clone_url_to_repo_name(&clone_url)?,
        "BITBUCKETSERVER" => BitbucketServerRepoSource {
            url: url_field,
            repository_path_pattern: pattern,
        }
        .clone_url_to_repo_name(&clone_url)?,
        "GERRIT" => GerritRepoSource {
            url: url_field,
            repository_path_pattern: pattern,
        }
        .clone_url_to_repo_name(&clone_url)?,
        "AWSCODECOMMIT" => AwsCodeCommitRepoSource {
            repository_path_pattern: pattern,
        }
        .clone_url_to_repo_name(&clone_url)?,
        "GITOLITE" => GitoliteRepoSource {
            host: config_string(&config, "host", "GitoliteConnection")?,
            prefix: config_string(&config, "prefix", "GitoliteConnection")?,
        }
        .clone_url_to_repo_name(&clone_url)?,
        "OTHER" => OtherRepoSource {
            url: url_field,
            repository_path_pattern: pattern,
        }
        .clone_url_to_repo_name(&clone_url)?,
        // Phabricator repositories are mirrored by another code host.
        "PHABRICATOR" => return Ok(None),
        _ => return Ok(None),
    };
    if name.is_empty() {
        return Ok(None);
    }
    Ok(Some(name))
}

/// `base.ResolveReference(relative)` for `../` clone URLs.
fn resolve_relative(clone_url: &str, host: &str) -> Result<String, String> {
    let base = url::Url::parse(host).map_err(|error| error.to_string())?;
    base.join(clone_url)
        .map(|url| url.to_string())
        .map_err(|error| error.to_string())
}

/// `RepoSourceCloneURLToRepoName` (`clone_urls.go:26-91`): returns the mapped
/// repository name, or `None` when no code host matched.
pub async fn repo_source_clone_url_to_repo_name(
    first: &dyn FirstRepoNameByCloneUrl,
    services: &dyn CloneUrlServiceSource,
    site: &SiteConfiguration,
    key: Option<&dyn Key>,
    clone_url: &str,
) -> Result<Option<String>, String> {
    let custom = custom_clone_url_to_repo_name(site, clone_url);
    if !custom.is_empty() {
        return Ok(Some(custom));
    }

    if let Some(name) = first.first_repo_name_by_clone_url(clone_url).await? {
        if !name.is_empty() {
            return Ok(Some(name));
        }
    }

    let mut after_id = 0;
    loop {
        let services = services
            .list_after(&CLONE_URL_KINDS, after_id, PAGE_SIZE)
            .await?;
        if services.is_empty() {
            break;
        }
        after_id = services[services.len() - 1].id;
        for service in &services {
            if let Some(name) = repo_name_from_service(service, clone_url, key).await? {
                return Ok(Some(name));
            }
        }
        if (services.len() as i64) < PAGE_SIZE {
            break;
        }
    }

    let fallback = GitHubRepoSource {
        url: "https://github.com".to_owned(),
        repository_path_pattern: String::new(),
    }
    .clone_url_to_repo_name(clone_url)?;
    Ok(if fallback.is_empty() {
        None
    } else {
        Some(fallback)
    })
}

/// Production [`CloneUrlResolver`]: the live site configuration plus the
/// `external_services` / `repo` seams and the keyring.
pub struct ConfiguredCloneUrlResolver {
    pub first: Arc<dyn FirstRepoNameByCloneUrl>,
    pub services: Arc<dyn CloneUrlServiceSource>,
    pub live: Arc<LiveConfig>,
    pub key: Option<Arc<dyn Key>>,
}

#[async_trait]
impl CloneUrlResolver for ConfiguredCloneUrlResolver {
    async fn repo_name_by_clone_url(&self, clone_url: &str) -> Result<Option<String>, String> {
        repo_source_clone_url_to_repo_name(
            self.first.as_ref(),
            self.services.as_ref(),
            &self.live.current().site,
            self.key.as_deref(),
            clone_url,
        )
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn site(config: &str) -> SiteConfiguration {
        SiteConfiguration::parse(config).expect("site config")
    }

    #[test]
    fn custom_mapping_substitutes_named_groups() {
        let site = site(
            r#"{"git.cloneURLToRepositoryName":[{"from":"^https://custom\\.example\\.com/(?P<name>.+)$","to":"custom.example.com/{name}"}]}"#,
        );
        assert_eq!(
            custom_clone_url_to_repo_name(&site, "https://custom.example.com/o/r"),
            "custom.example.com/o/r"
        );
        assert_eq!(
            custom_clone_url_to_repo_name(&site, "https://other.example.com/o/r"),
            ""
        );
    }

    #[test]
    fn github_fallback_maps_any_github_url() {
        let source = GitHubRepoSource {
            url: "https://github.com".to_owned(),
            repository_path_pattern: String::new(),
        };
        assert_eq!(
            source
                .clone_url_to_repo_name("https://github.com/sourcegraph/sourcegraph")
                .expect("github"),
            "github.com/sourcegraph/sourcegraph"
        );
    }

    #[test]
    fn relative_clone_urls_resolve_against_the_host() {
        assert_eq!(
            resolve_relative("../other/repo.git", "https://code.example.com/org/repo")
                .expect("resolve"),
            "https://code.example.com/other/repo.git"
        );
    }
}
