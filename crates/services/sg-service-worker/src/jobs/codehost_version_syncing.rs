#![forbid(unsafe_code)]

//! `internal/extsvc/versions` + the `codehost-version-syncing` worker job.
//!
//! Every 24 hours the job lists every live external service, groups them by
//! the code-host instance they point at (`UniqueEncryptableCodeHostIdentifier`)
//! and asks each unique source for the code host's version. Only GitHub,
//! GitLab and Bitbucket Server sources implement `repos.VersionSource`; the
//! other kinds are skipped. The collected versions are stored in Redis under
//! the existing `extsvcversions` key as a JSON array
//! (`[{"external_service_kind":…,"version":…}]`), which `updatecheck` reads
//! through `versions.GetVersions`.
//!
//! Semantics reproduced from `loadVersions` / `storeVersions`:
//!
//! * one representative per identifier (first service in `List` order wins);
//!   `List` orders by `id DESC`;
//! * identifier resolution decrypts and parses the config *before* the
//!   source is constructed, so a bad config aborts the whole tick;
//! * a source-construction error aborts the tick, a version-fetch error only
//!   skips that source (the baseline logs a warning);
//! * an empty result is stored as `null` (Go marshals a nil slice);
//! * the JSON string escaping matches Go's `encoding/json` (HTML characters
//!   and U+2028/U+2029 are escaped).
//!
//! Not ported: logs/metrics, GitHub App authentication, and OAuth1-signed
//! Bitbucket Server configs (see docs/modules/06_worker.md §49.5).

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use serde_json::Value;
use sg_db::external_services::{ExternalService, ExternalServiceStore};
use sg_encryption::maybe_decrypt;
use sg_keyring::Keyring;
use sg_redis::KeyValue;
use tokio::sync::OnceCell;
use tokio_util::sync::CancellationToken;
use url::Url;

use super::{Job, JobDeps};
use crate::framework::periodic::{Handler, HandlerError, PeriodicRoutine};
use crate::framework::BackgroundRoutine;

/// `syncingJob.Description` (the baseline returns the empty string).
pub const JOB_DESCRIPTION: &str = "";
/// `repomgmt.version-syncer`.
pub const ROUTINE_NAME: &str = "repomgmt.version-syncer";
pub const ROUTINE_DESCRIPTION: &str = "sync versions of external services";
pub const SYNC_INTERVAL: Duration = Duration::from_secs(24 * 60 * 60);
/// `versionsKey`.
pub const VERSIONS_KEY: &str = "extsvcversions";

/// `versions.Version`. `key` is the representative service's URN; the
/// baseline tags it `json:"-"`, so it is never serialized.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Version {
    pub external_service_kind: String,
    pub version: String,
    pub key: String,
}

/// `database.ExternalServiceStore.List` as much as `loadVersions` uses it.
#[async_trait]
pub trait ExternalServiceLister: Send + Sync {
    async fn list(&self) -> Result<Vec<ExternalService>, String>;
}

#[async_trait]
impl ExternalServiceLister for ExternalServiceStore {
    async fn list(&self) -> Result<Vec<ExternalService>, String> {
        ExternalServiceStore::list(self)
            .await
            .map_err(|err| err.to_string())
    }
}

/// `repos.VersionSource`.
#[async_trait]
pub trait VersionSource: Send + Sync {
    async fn version(&self) -> Result<String, String>;
}

/// The part of `repos.Source` this job observes. The baseline type-asserts to
/// `repos.VersionSource`; everything else is skipped.
pub enum Source {
    Version(Arc<dyn VersionSource>),
    Other,
}

/// `repos.Sourcer`.
#[async_trait]
pub trait Sourcer: Send + Sync {
    async fn source(&self, svc: &ExternalService) -> Result<Source, String>;
}

/// The identifiers `extsvc.UniqueCodeHostIdentifier` recognizes. The variant
/// set and the `AsKind`/`AsType` spellings mirror `variantValuesMap`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Variant {
    AwsCodeCommit,
    AzureDevOps,
    BitbucketCloud,
    BitbucketServer,
    Gerrit,
    GitHub,
    GitLab,
    Gitolite,
    GoModules,
    JvmPackages,
    NpmPackages,
    Other,
    Pagure,
    Perforce,
    Phabricator,
    PythonPackages,
    RubyPackages,
    RustPackages,
}

impl Variant {
    pub fn as_kind(self) -> &'static str {
        match self {
            Self::AwsCodeCommit => "AWSCODECOMMIT",
            Self::AzureDevOps => "AZUREDEVOPS",
            Self::BitbucketCloud => "BITBUCKETCLOUD",
            Self::BitbucketServer => "BITBUCKETSERVER",
            Self::Gerrit => "GERRIT",
            Self::GitHub => "GITHUB",
            Self::GitLab => "GITLAB",
            Self::Gitolite => "GITOLITE",
            Self::GoModules => "GOMODULES",
            Self::JvmPackages => "JVMPACKAGES",
            Self::NpmPackages => "NPMPACKAGES",
            Self::Other => "OTHER",
            Self::Pagure => "PAGURE",
            Self::Perforce => "PERFORCE",
            Self::Phabricator => "PHABRICATOR",
            Self::PythonPackages => "PYTHONPACKAGES",
            Self::RubyPackages => "RUBYPACKAGES",
            Self::RustPackages => "RUSTPACKAGES",
        }
    }

    pub fn as_type(self) -> &'static str {
        match self {
            Self::AwsCodeCommit => "awscodecommit",
            Self::AzureDevOps => "azuredevops",
            Self::BitbucketCloud => "bitbucketCloud",
            Self::BitbucketServer => "bitbucketServer",
            Self::Gerrit => "gerrit",
            Self::GitHub => "github",
            Self::GitLab => "gitlab",
            Self::Gitolite => "gitolite",
            Self::GoModules => "goModules",
            Self::JvmPackages => "jvmPackages",
            Self::NpmPackages => "npmPackages",
            Self::Other => "other",
            Self::Pagure => "pagure",
            Self::Perforce => "perforce",
            Self::Phabricator => "phabricator",
            Self::PythonPackages => "pythonPackages",
            Self::RubyPackages => "rubyPackages",
            Self::RustPackages => "rustPackages",
        }
    }

    /// `extsvc.VariantValueOf`: case-insensitive match against either spelling.
    pub fn from_kind(input: &str) -> Option<Self> {
        ALL_VARIANTS.iter().copied().find(|v| {
            v.as_kind().eq_ignore_ascii_case(input) || v.as_type().eq_ignore_ascii_case(input)
        })
    }
}

const ALL_VARIANTS: [Variant; 18] = [
    Variant::AwsCodeCommit,
    Variant::AzureDevOps,
    Variant::BitbucketCloud,
    Variant::BitbucketServer,
    Variant::Gerrit,
    Variant::GitHub,
    Variant::GitLab,
    Variant::Gitolite,
    Variant::GoModules,
    Variant::JvmPackages,
    Variant::NpmPackages,
    Variant::Other,
    Variant::Pagure,
    Variant::Perforce,
    Variant::Phabricator,
    Variant::PythonPackages,
    Variant::RubyPackages,
    Variant::RustPackages,
];

/// `extsvc.UniqueEncryptableCodeHostIdentifier`.
pub async fn unique_encryptable_code_host_identifier(
    keyring: &Keyring,
    kind: &str,
    config: &str,
    encryption_key_id: &str,
) -> Result<String, String> {
    // `getConfigPrototype` runs before decryption/parsing.
    let variant = Variant::from_kind(kind)
        .ok_or_else(|| format!("unknown external service kind {kind:?}"))?;

    let raw = maybe_decrypt(
        keyring.external_service_key().as_deref(),
        config,
        encryption_key_id,
    )
    .await
    .map_err(|err| err.to_string())?;
    let stripped = sg_config::strip_jsonc(&raw);
    let value: Value = serde_json::from_str(&stripped).map_err(|err| err.to_string())?;

    unique_code_host_identifier(variant, &value)
}

/// `uniqueCodeHostIdentifier` for the parsed config.
fn unique_code_host_identifier(variant: Variant, cfg: &Value) -> Result<String, String> {
    let raw_url = match variant {
        // Returned before the URL parse.
        Variant::AwsCodeCommit => {
            return Ok(format!(
                "{}:{}",
                config_string(cfg, "region"),
                config_string(cfg, "accessKeyID")
            ))
        }
        Variant::Perforce => return Ok(config_string(cfg, "p4Port")),
        Variant::GoModules => return Ok(Variant::GoModules.as_kind().to_owned()),
        Variant::JvmPackages => return Ok(Variant::JvmPackages.as_kind().to_owned()),
        Variant::NpmPackages => return Ok(Variant::NpmPackages.as_kind().to_owned()),
        Variant::PythonPackages => return Ok(Variant::PythonPackages.as_kind().to_owned()),
        Variant::RustPackages => return Ok(Variant::RustPackages.as_kind().to_owned()),
        Variant::RubyPackages => return Ok(Variant::RubyPackages.as_kind().to_owned()),
        Variant::Gitolite => config_string(cfg, "host"),
        Variant::AzureDevOps
        | Variant::BitbucketCloud
        | Variant::BitbucketServer
        | Variant::Gerrit
        | Variant::GitHub
        | Variant::GitLab
        | Variant::Other
        | Variant::Pagure
        | Variant::Phabricator => config_string(cfg, "url"),
    };

    normalize_base_url(&raw_url)
}

/// Go `json.Unmarshal` into the schema struct: absent or `null` fields are
/// zero values. (A non-string value is a Go type error; see the errata.)
fn config_string(cfg: &Value, key: &str) -> String {
    cfg.get(key)
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_owned()
}

/// `url.Parse` + `extsvc.NormalizeBaseURL` + `URL.String`: lowercase the host
/// and ensure the path ends in `/`. Scheme-less references (which Go accepts
/// and Rust's `Url` rejects) are treated as paths.
pub fn normalize_base_url(raw: &str) -> Result<String, String> {
    let mut url = match Url::parse(raw) {
        Ok(url) => url,
        Err(_) if !raw.contains("://") => {
            return Ok(if raw.ends_with('/') {
                raw.to_owned()
            } else {
                format!("{raw}/")
            });
        }
        Err(err) => return Err(err.to_string()),
    };

    if let Some(host) = url.host_str().map(str::to_lowercase) {
        url.set_host(Some(&host)).map_err(|err| err.to_string())?;
    }
    if !url.path().ends_with('/') {
        let path = format!("{}/", url.path());
        url.set_path(&path);
    }
    Ok(url.to_string())
}

/// `loadVersions`: list, group by unique code host, fetch each version.
///
/// A configuration or source-construction error aborts and is returned; a
/// version-fetch error only drops that service (the baseline logs it).
pub async fn load_versions(
    lister: &dyn ExternalServiceLister,
    keyring: &Keyring,
    sourcer: &dyn Sourcer,
) -> Result<Vec<Version>, String> {
    let services = lister.list().await?;

    let mut unique: BTreeMap<String, &ExternalService> = BTreeMap::new();
    for svc in &services {
        let identifier = unique_encryptable_code_host_identifier(
            keyring,
            &svc.kind,
            &svc.config,
            &svc.encryption_key_id,
        )
        .await?;
        // First service in List order wins, like writing to a Go map once.
        unique.entry(identifier).or_insert(svc);
    }

    let mut versions = Vec::new();
    for svc in unique.into_values() {
        let source = sourcer.source(svc).await?;
        let Source::Version(version_source) = source else {
            continue;
        };
        match version_source.version().await {
            Ok(version) => versions.push(Version {
                external_service_kind: svc.kind.clone(),
                version,
                key: sg_extsvc::urn(&svc.kind, svc.id),
            }),
            // `logger.Warn("failed to fetch version of code host")` + continue.
            Err(_) => continue,
        }
    }

    Ok(versions)
}

/// `storeVersions`: `SET extsvcversions <json>` on the store pool.
pub async fn store_versions(kv: &KeyValue, versions: &[Version]) -> Result<(), String> {
    kv.set(VERSIONS_KEY, encode_versions(versions))
        .await
        .map_err(|err| err.to_string())
}

/// `json.Marshal([]*Version)` for the subset the job stores. A nil/empty slice
/// marshals to `null`; field order follows the struct.
pub fn encode_versions(versions: &[Version]) -> String {
    if versions.is_empty() {
        return "null".to_owned();
    }
    let mut payload = String::from("[");
    for (index, version) in versions.iter().enumerate() {
        if index > 0 {
            payload.push(',');
        }
        payload.push_str("{\"external_service_kind\":");
        payload.push_str(&go_json_string(&version.external_service_kind));
        payload.push_str(",\"version\":");
        payload.push_str(&go_json_string(&version.version));
        payload.push('}');
    }
    payload.push(']');
    payload
}

/// Go `encoding/json` string escaping: `"`/`\`/control characters, the HTML
/// characters `<`, `>`, `&`, and U+2028/U+2029; everything else is literal.
fn go_json_string(value: &str) -> String {
    let mut out = String::with_capacity(value.len() + 2);
    out.push('"');
    for c in value.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            '<' => out.push_str("\\u003c"),
            '>' => out.push_str("\\u003e"),
            '&' => out.push_str("\\u0026"),
            '\u{2028}' => out.push_str("\\u2028"),
            '\u{2029}' => out.push_str("\\u2029"),
            c if (c as u32) < 0x20 => {
                out.push_str(&format!("\\u{:04x}", c as u32));
            }
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

/// `github.APIRoot`: GitHub.com's API lives on api.github.com; a GHE service
/// gets `/api/v3` at its root or `api` under a base path.
pub fn github_api_root(base: &Url) -> (Url, bool) {
    let host = base.host_str().unwrap_or_default().to_lowercase();
    if host == "github.com" || host == "www.github.com" {
        return (
            Url::parse("https://api.github.com/").expect("static API URL"),
            true,
        );
    }

    let mut api = base.clone();
    let path = base.path();
    if path.is_empty() || path == "/" {
        api.set_path("/api/v3");
    } else {
        api.set_path(&format!("{path}api"));
    }
    (api, false)
}

/// The real `repos.Sourcer` for version fetching. Only the three kinds that
/// implement `repos.VersionSource` produce a [`Source::Version`]; every other
/// known kind is [`Source::Other`].
pub struct RealSourcer {
    http: reqwest::Client,
    keyring: Arc<Keyring>,
}

impl RealSourcer {
    pub fn new(keyring: Arc<Keyring>) -> Self {
        Self {
            http: reqwest::Client::new(),
            keyring,
        }
    }
}

#[async_trait]
impl Sourcer for RealSourcer {
    async fn source(&self, svc: &ExternalService) -> Result<Source, String> {
        let raw = maybe_decrypt(
            self.keyring.external_service_key().as_deref(),
            &svc.config,
            &svc.encryption_key_id,
        )
        .await
        .map_err(|err| format!("external service id={} config error: {err}", svc.id))?;
        let stripped = sg_config::strip_jsonc(&raw);
        let cfg: Value = serde_json::from_str(&stripped)
            .map_err(|err| format!("external service id={} config error: {err}", svc.id))?;

        let Some(variant) = Variant::from_kind(&svc.kind) else {
            return Ok(Source::Other);
        };

        match variant {
            Variant::GitHub => {
                // GitHub App authentication needs the installation-token
                // exchange; not ported (errata §49.5).
                if cfg.get("githubAppDetails").is_some_and(|v| !v.is_null()) {
                    return Ok(Source::Other);
                }
                let token = config_string(&cfg, "token");
                let base = Url::parse(&normalize_base_url(&config_string(&cfg, "url"))?)
                    .map_err(|err| err.to_string())?;
                let (api, dotcom) = github_api_root(&base);
                Ok(Source::Version(Arc::new(GitHubVersionSource {
                    http: self.http.clone(),
                    api_url: api.to_string(),
                    dotcom,
                    token,
                })))
            }
            Variant::GitLab => {
                let token = config_string(&cfg, "token");
                let token_type = config_string(&cfg, "tokenType");
                let mut base = Url::parse(&normalize_base_url(&config_string(&cfg, "url"))?)
                    .map_err(|err| err.to_string())?;
                base.set_path(&format!("{}api/v4/", base.path()));
                Ok(Source::Version(Arc::new(GitLabVersionSource {
                    http: self.http.clone(),
                    url: base
                        .join("version")
                        .map_err(|err| err.to_string())?
                        .to_string(),
                    bearer: token_type == "oauth",
                    token,
                })))
            }
            Variant::BitbucketServer => {
                let authorization = cfg.get("authorization");
                let oauth1 = authorization
                    .and_then(|a| a.get("oauth"))
                    .is_some_and(|v| !v.is_null());
                if oauth1 {
                    // OAuth1 request signing is not ported.
                    return Ok(Source::Other);
                }
                let token = config_string(&cfg, "token");
                let base = Url::parse(&normalize_base_url(&config_string(&cfg, "url"))?)
                    .map_err(|err| err.to_string())?;
                let url = base
                    .join("rest/api/1.0/application-properties")
                    .map_err(|err| err.to_string())?
                    .to_string();
                let auth = if !token.is_empty() {
                    BitbucketAuth::Bearer(token)
                } else {
                    BitbucketAuth::Basic {
                        username: config_string(&cfg, "username"),
                        password: config_string(&cfg, "password"),
                    }
                };
                Ok(Source::Version(Arc::new(BitbucketVersionSource {
                    http: self.http.clone(),
                    url,
                    auth,
                })))
            }
            _ => Ok(Source::Other),
        }
    }
}

struct GitHubVersionSource {
    http: reqwest::Client,
    api_url: String,
    dotcom: bool,
    token: String,
}

#[async_trait]
impl VersionSource for GitHubVersionSource {
    async fn version(&self) -> Result<String, String> {
        // `V3Client.GetVersion`: GitHub.com short-circuits.
        if self.dotcom {
            return Ok("unknown".to_owned());
        }
        let mut request = self.http.get(&self.api_url);
        if !self.token.is_empty() {
            request = request.bearer_auth(&self.token);
        }
        let response = request.send().await.map_err(|err| err.to_string())?;
        if !(200..400).contains(&response.status().as_u16()) {
            return Err(format!(
                "request to {} returned status {}",
                self.api_url,
                response.status()
            ));
        }
        Ok(response
            .headers()
            .get("X-GitHub-Enterprise-Version")
            .and_then(|value| value.to_str().ok())
            .unwrap_or_default()
            .to_owned())
    }
}

struct GitLabVersionSource {
    http: reqwest::Client,
    url: String,
    bearer: bool,
    token: String,
}

#[async_trait]
impl VersionSource for GitLabVersionSource {
    async fn version(&self) -> Result<String, String> {
        let mut request = self.http.get(&self.url);
        if !self.token.is_empty() {
            request = if self.bearer {
                request.bearer_auth(&self.token)
            } else {
                request.header("Private-Token", &self.token)
            };
        }
        let response = request.send().await.map_err(|err| err.to_string())?;
        if !(200..400).contains(&response.status().as_u16()) {
            return Err(format!(
                "request to {} returned status {}",
                self.url,
                response.status()
            ));
        }
        let body: Value = response.json().await.map_err(|err| err.to_string())?;
        Ok(body
            .get("version")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_owned())
    }
}

enum BitbucketAuth {
    Bearer(String),
    Basic { username: String, password: String },
}

struct BitbucketVersionSource {
    http: reqwest::Client,
    url: String,
    auth: BitbucketAuth,
}

#[async_trait]
impl VersionSource for BitbucketVersionSource {
    async fn version(&self) -> Result<String, String> {
        let mut request = self.http.get(&self.url);
        request = match &self.auth {
            BitbucketAuth::Bearer(token) => request.bearer_auth(token),
            BitbucketAuth::Basic { username, password } => {
                request.basic_auth(username, Some(password))
            }
        };
        let response = request.send().await.map_err(|err| err.to_string())?;
        if !(200..400).contains(&response.status().as_u16()) {
            return Err(format!(
                "request to {} returned status {}",
                self.url,
                response.status()
            ));
        }
        let body: Value = response.json().await.map_err(|err| err.to_string())?;
        // Go decodes into a struct without tags, matching keys
        // case-insensitively; the API emits `Version`.
        Ok(body
            .get("Version")
            .or_else(|| body.get("version"))
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_owned())
    }
}

/// A `KeyValue` that dials on first use, like `redispool.Store`'s lazy pool.
struct LazyStore {
    endpoint: String,
    cell: OnceCell<KeyValue>,
}

impl LazyStore {
    async fn get(&self) -> Result<&KeyValue, String> {
        self.cell
            .get_or_try_init(|| async {
                KeyValue::connect(&self.endpoint)
                    .await
                    .map_err(|err| err.to_string())
            })
            .await
    }
}

/// The routine: `loadVersions` then `storeVersions`.
pub struct VersionSyncHandler {
    lister: ExternalServiceStore,
    keyring: Arc<Keyring>,
    sourcer: Arc<dyn Sourcer>,
    store: Arc<LazyStore>,
}

impl VersionSyncHandler {
    pub fn new(
        lister: ExternalServiceStore,
        keyring: Arc<Keyring>,
        sourcer: Arc<dyn Sourcer>,
        endpoint: String,
    ) -> Self {
        Self {
            lister,
            keyring,
            sourcer,
            store: Arc::new(LazyStore {
                endpoint,
                cell: OnceCell::new(),
            }),
        }
    }

    pub async fn handle_once(&self) -> Result<(), String> {
        let versions = load_versions(&self.lister, &self.keyring, self.sourcer.as_ref()).await?;
        let kv = self.store.get().await?;
        store_versions(kv, &versions).await
    }
}

#[async_trait]
impl Handler for VersionSyncHandler {
    async fn handle(&self, _ctx: CancellationToken) -> Result<(), HandlerError> {
        self.handle_once().await.map_err(HandlerError::new)
    }

    fn handle_error(&self, error: &HandlerError) {
        tracing::error!(error = %error, "failed to sync code host versions");
    }
}

/// `syncingJob`.
pub struct CodehostVersionSyncing;

impl CodehostVersionSyncing {
    pub fn new() -> Self {
        Self
    }
}

impl Default for CodehostVersionSyncing {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Job for CodehostVersionSyncing {
    fn description(&self) -> String {
        JOB_DESCRIPTION.to_owned()
    }

    async fn routines(
        &self,
        deps: &JobDeps,
        _setup_ctx: CancellationToken,
    ) -> Result<Vec<BackgroundRoutine>, String> {
        // `dotcom.SourcegraphDotComMode()`: not relevant on sourcegraph.com.
        if dotcom_mode() {
            return Ok(Vec::new());
        }

        let handler = VersionSyncHandler::new(
            ExternalServiceStore::new(deps.frontend.clone()),
            Arc::clone(&deps.keyring),
            Arc::new(RealSourcer::new(Arc::clone(&deps.keyring))),
            sg_redis::store_endpoint(),
        );
        Ok(vec![PeriodicRoutine::new(Arc::new(handler))
            .with_name(ROUTINE_NAME)
            .with_description(ROUTINE_DESCRIPTION)
            .with_interval(SYNC_INTERVAL)
            .into()])
    }
}

/// `dotcom.SourcegraphDotComMode()` (`SOURCEGRAPHDOTCOM_MODE`, a Go bool).
fn dotcom_mode() -> bool {
    parse_go_bool(std::env::var("SOURCEGRAPHDOTCOM_MODE").ok().as_deref())
}

/// `strconv.ParseBool`: only these spellings are true; everything else
/// (including unset) is false.
fn parse_go_bool(raw: Option<&str>) -> bool {
    matches!(raw, Some("1" | "t" | "T" | "TRUE" | "true" | "True"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn svc(id: i64, kind: &str, config: &str) -> ExternalService {
        ExternalService {
            id,
            kind: kind.to_owned(),
            display_name: format!("svc-{id}"),
            config: config.to_owned(),
            encryption_key_id: String::new(),
            last_sync_at: None,
            next_sync_at: None,
            unrestricted: false,
            has_webhooks: None,
            code_host_id: None,
        }
    }

    struct FakeLister(Vec<ExternalService>);

    #[async_trait]
    impl ExternalServiceLister for FakeLister {
        async fn list(&self) -> Result<Vec<ExternalService>, String> {
            Ok(self.0.clone())
        }
    }

    struct FakeSourcer {
        error_for: Option<String>,
        other_for: Option<String>,
        version_error_for: Option<String>,
    }

    struct FixedVersion(&'static str);

    #[async_trait]
    impl VersionSource for FixedVersion {
        async fn version(&self) -> Result<String, String> {
            Ok(self.0.to_owned())
        }
    }

    struct FailingVersion;

    #[async_trait]
    impl VersionSource for FailingVersion {
        async fn version(&self) -> Result<String, String> {
            Err("fetch failed".to_owned())
        }
    }

    #[async_trait]
    impl Sourcer for FakeSourcer {
        async fn source(&self, svc: &ExternalService) -> Result<Source, String> {
            let urn = sg_extsvc::urn(&svc.kind, svc.id);
            if self.error_for.as_deref() == Some(urn.as_str()) {
                return Err("create source: boom".to_owned());
            }
            if self.other_for.as_deref() == Some(urn.as_str()) {
                return Ok(Source::Other);
            }
            if self.version_error_for.as_deref() == Some(urn.as_str()) {
                return Ok(Source::Version(Arc::new(FailingVersion)));
            }
            Ok(Source::Version(Arc::new(FixedVersion("3.4.5"))))
        }
    }

    #[test]
    fn metadata_matches_the_baseline() {
        let job = CodehostVersionSyncing::new();
        assert_eq!(job.description(), "");
        assert!(job.configs().is_empty());
        assert_eq!(ROUTINE_NAME, "repomgmt.version-syncer");
        assert_eq!(ROUTINE_DESCRIPTION, "sync versions of external services");
        assert_eq!(SYNC_INTERVAL, Duration::from_secs(86_400));
        assert_eq!(VERSIONS_KEY, "extsvcversions");
    }

    #[test]
    fn dotcom_mode_parses_go_bools() {
        for raw in ["1", "t", "T", "TRUE", "true", "True"] {
            assert!(parse_go_bool(Some(raw)), "{raw}");
        }
        for raw in [None, Some("0"), Some("f"), Some("FALSE"), Some("yes")] {
            assert!(!parse_go_bool(raw), "{raw:?}");
        }
    }

    #[test]
    fn identifiers_match_the_baseline() {
        assert_eq!(
            unique_code_host_identifier(
                Variant::GitHub,
                &serde_json::json!({"url": "https://GitHub.Example.com"})
            )
            .unwrap(),
            "https://github.example.com/"
        );
        assert_eq!(
            unique_code_host_identifier(Variant::GitHub, &serde_json::json!({"url": ""})).unwrap(),
            "/"
        );
        assert_eq!(
            unique_code_host_identifier(
                Variant::Gitolite,
                &serde_json::json!({"host": "git.example.com"})
            )
            .unwrap(),
            "git.example.com/"
        );
        assert_eq!(
            unique_code_host_identifier(
                Variant::AwsCodeCommit,
                &serde_json::json!({"region": "us-west-1", "accessKeyID": "AKIA"})
            )
            .unwrap(),
            "us-west-1:AKIA"
        );
        assert_eq!(
            unique_code_host_identifier(
                Variant::Perforce,
                &serde_json::json!({"p4Port": "ssl:p4:1666"})
            )
            .unwrap(),
            "ssl:p4:1666"
        );
        assert_eq!(
            unique_code_host_identifier(Variant::GoModules, &serde_json::json!({})).unwrap(),
            "GOMODULES"
        );
    }

    #[tokio::test]
    async fn unknown_kind_is_reported_before_parsing() {
        let keyring = Keyring::default();
        let err = unique_encryptable_code_host_identifier(&keyring, "WEIRD", "not json", "")
            .await
            .unwrap_err();
        assert_eq!(err, "unknown external service kind \"WEIRD\"");
    }

    #[test]
    fn variant_kind_and_type_spellings() {
        assert_eq!(Variant::from_kind("github"), Some(Variant::GitHub));
        assert_eq!(Variant::from_kind("GITHUB"), Some(Variant::GitHub));
        assert_eq!(
            Variant::from_kind("bitbucketServer"),
            Some(Variant::BitbucketServer)
        );
        assert_eq!(
            Variant::from_kind("BITBUCKETSERVER"),
            Some(Variant::BitbucketServer)
        );
        assert_eq!(Variant::from_kind("nope"), None);
    }

    #[test]
    fn version_payload_uses_go_json_escaping() {
        assert_eq!(encode_versions(&[]), "null");
        let versions = vec![Version {
            external_service_kind: "GITHUB".to_owned(),
            version: "3.4.5".to_owned(),
            key: "extsvc:github:1".to_owned(),
        }];
        assert_eq!(
            encode_versions(&versions),
            "[{\"external_service_kind\":\"GITHUB\",\"version\":\"3.4.5\"}]"
        );

        let escaped = vec![Version {
            external_service_kind: "GITLAB".to_owned(),
            version: "a<b>&c\"\\\n\t\u{2028}\u{1}".to_owned(),
            key: String::new(),
        }];
        assert_eq!(
            encode_versions(&escaped),
            "[{\"external_service_kind\":\"GITLAB\",\"version\":\"a\\u003cb\\u003e\\u0026c\\\"\\\\\\n\\t\\u2028\\u0001\"}]"
        );
    }

    #[tokio::test]
    async fn load_versions_groups_skips_and_propagates() {
        let lister = FakeLister(vec![
            svc(2, "GITHUB", r#"{"url": "https://github.example.com"}"#),
            svc(1, "GITHUB", r#"{"url": "https://github.example.com/"}"#),
            svc(3, "GERRIT", r#"{"url": "https://gerrit.example.com"}"#),
            svc(4, "GITHUB", r#"{"url": "https://fetch-fail.example.com"}"#),
            svc(5, "GITLAB", r#"{"url": "https://gitlab.example.com"}"#),
        ]);
        let fake = FakeSourcer {
            error_for: None,
            other_for: Some(sg_extsvc::urn("GERRIT", 3)),
            version_error_for: Some(sg_extsvc::urn("GITHUB", 4)),
        };
        // Fake sourcer returns a default client for everything else; the
        // identifier grouping keeps the first github service (id 2).
        let versions = load_versions(&lister, &Keyring::default(), &fake)
            .await
            .unwrap();
        assert_eq!(
            versions,
            vec![
                Version {
                    external_service_kind: "GITHUB".to_owned(),
                    version: "3.4.5".to_owned(),
                    key: "extsvc:github:2".to_owned(),
                },
                Version {
                    external_service_kind: "GITLAB".to_owned(),
                    version: "3.4.5".to_owned(),
                    key: "extsvc:gitlab:5".to_owned(),
                },
            ]
        );

        let failing = FakeSourcer {
            error_for: Some(sg_extsvc::urn("GITLAB", 5)),
            other_for: None,
            version_error_for: None,
        };
        assert_eq!(
            load_versions(&lister, &Keyring::default(), &failing)
                .await
                .unwrap_err(),
            "create source: boom"
        );
    }

    #[test]
    fn github_api_root_matches_the_baseline() {
        let root = |raw: &str| {
            let (api, dotcom) = github_api_root(&Url::parse(raw).unwrap());
            (api.to_string(), dotcom)
        };
        assert_eq!(
            root("https://github.com"),
            ("https://api.github.com/".to_owned(), true)
        );
        assert_eq!(
            root("https://www.github.com/"),
            ("https://api.github.com/".to_owned(), true)
        );
        assert_eq!(
            root("https://ghe.example.com/"),
            ("https://ghe.example.com/api/v3".to_owned(), false)
        );
        assert_eq!(
            root("https://ghe.example.com/sub/"),
            ("https://ghe.example.com/sub/api".to_owned(), false)
        );
    }
}
