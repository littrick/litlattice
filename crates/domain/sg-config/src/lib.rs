#![forbid(unsafe_code)]
// `ConfigError` carries `tonic::Status`/transport errors verbatim, like the
// baseline's wrapped errors; boxing them would hide the status codes callers
// inspect.
#![allow(clippy::result_large_err)]

//! Port of the parts of `internal/conf` and `internal/api/internalapi` that
//! other services read:
//!
//! - [`ConfigClient`] calls the frontend's `api.internalapi.v1.ConfigService`
//!   (`GetConfig`) exactly like `internalapi.Client.Configuration`, and keeps
//!   the last parsed configuration for `conf.Get`-style access.
//! - [`SiteConfiguration`] is the JSONC site config with typed accessors for
//!   the fields the Rust services use (git clone timeout, auto updates,
//!   clone progress logs, custom git fetch, external TLS).
//! - [`ServiceConnections`] carries the deployment wiring the frontend owns
//!   (notably `postgresDSN` and the gitserver addresses).
//!
//! [`ConfigWatcher`] is the baseline's `continuouslyUpdate` goroutine: it
//! polls `GetConfig` every 0–5 seconds and, when the raw configuration
//! changes, replaces the [`LiveConfig`] snapshot and wakes every
//! `conf.Watch`-style consumer.

use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use sg_proto::internalapi as proto;
use tokio_util::sync::CancellationToken;
use tonic::transport::{Channel, Endpoint};

/// `SRC_FRONTEND_INTERNAL` default (`internalapi.frontendInternal`).
pub const DEFAULT_FRONTEND_INTERNAL: &str = "sourcegraph-frontend-internal";

/// `conf.defaultGitLongCommandTimeout`.
pub const DEFAULT_GIT_LONG_COMMAND_TIMEOUT: Duration = Duration::from_secs(2 * 60 * 60);

/// `conf.GitMaxConcurrentClones` default when the field is unset or
/// non-positive.
pub const DEFAULT_GIT_MAX_CONCURRENT_CLONES: usize = 5;

#[derive(Debug)]
pub enum ConfigError {
    /// The frontend could not be reached or answered with an error.
    Client(tonic::Status),
    /// The transport could not be created.
    Transport(tonic::transport::Error),
    /// The site config is not valid JSONC.
    Parse(serde_json::Error),
}

impl std::fmt::Display for ConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConfigError::Client(status) => write!(f, "getting configuration: {status}"),
            ConfigError::Transport(err) => write!(f, "connecting to configuration service: {err}"),
            ConfigError::Parse(err) => write!(f, "parsing site configuration: {err}"),
        }
    }
}

impl std::error::Error for ConfigError {}

impl ConfigError {
    /// `isFrontendUnreachableError`: gRPC `Unavailable`, or a failed
    /// connection. The watcher suppresses these during the frontend's
    /// startup window.
    pub fn is_unreachable(&self) -> bool {
        match self {
            ConfigError::Client(status) => status.code() == tonic::Code::Unavailable,
            ConfigError::Transport(_) => true,
            ConfigError::Parse(_) => false,
        }
    }
}

impl From<tonic::Status> for ConfigError {
    fn from(status: tonic::Status) -> Self {
        ConfigError::Client(status)
    }
}

impl From<tonic::transport::Error> for ConfigError {
    fn from(err: tonic::transport::Error) -> Self {
        ConfigError::Transport(err)
    }
}

impl From<serde_json::Error> for ConfigError {
    fn from(err: serde_json::Error) -> Self {
        ConfigError::Parse(err)
    }
}

/// Port of `conftypes.ServiceConnections`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ServiceConnections {
    pub git_servers: Vec<String>,
    pub postgres_dsn: String,
    pub code_intel_postgres_dsn: String,
    pub code_insights_dsn: String,
    pub searchers: Vec<String>,
    pub symbols: Vec<String>,
    pub embeddings: Vec<String>,
    pub zoekts: Vec<String>,
    pub zoekt_list_ttl: Duration,
}

impl ServiceConnections {
    /// `ServiceConnections.FromProto` (`GetZoektListTtl().AsDuration()`).
    pub fn from_proto(connections: Option<&proto::ServiceConnections>) -> Self {
        let Some(connections) = connections else {
            return Self::default();
        };
        Self {
            git_servers: connections.git_servers.clone(),
            postgres_dsn: connections.postgres_dsn.clone(),
            code_intel_postgres_dsn: connections.code_intel_postgres_dsn.clone(),
            code_insights_dsn: connections.code_insights_dsn.clone(),
            searchers: connections.searchers.clone(),
            symbols: connections.symbols.clone(),
            embeddings: connections.embeddings.clone(),
            zoekts: connections.zoekts.clone(),
            zoekt_list_ttl: connections
                .zoekt_list_ttl
                .as_ref()
                .map(|duration| {
                    Duration::new(duration.seconds.max(0) as u64, duration.nanos.max(0) as u32)
                })
                .unwrap_or_default(),
        }
    }
}

/// Port of `conftypes.RawUnified`: the unparsed configuration.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RawUnified {
    pub id: i32,
    pub site: String,
    pub service_connections: ServiceConnections,
}

impl RawUnified {
    /// `RawUnified.FromProto`.
    pub fn from_proto(raw: Option<&proto::RawUnified>) -> Self {
        let Some(raw) = raw else {
            return Self::default();
        };
        Self {
            id: raw.id,
            site: raw.site.clone(),
            service_connections: ServiceConnections::from_proto(raw.service_connections.as_ref()),
        }
    }

    /// `RawUnified.Equal`: the ID is ignored.
    pub fn equal(&self, other: &RawUnified) -> bool {
        self.site == other.site && self.service_connections == other.service_connections
    }
}

/// One entry of `experimentalFeatures.customGitFetch`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CustomFetchMapping {
    pub domain_path: String,
    /// The command split on whitespace (`strings.Fields`).
    pub fetch: Vec<String>,
}

/// `experimentalFeatures.tlsExternal` (`schema.TlsExternal`).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TlsExternal {
    pub insecure_skip_verify: bool,
    pub certificates: Vec<String>,
}

/// `schema.MountedEncryptionKey`: a key read from an environment variable or a
/// file. Exactly one of `env_var_name`/`filepath` must be set.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MountedEncryptionKeyConfig {
    pub env_var_name: String,
    pub filepath: String,
    pub keyname: String,
    pub version: String,
}

/// `schema.AWSKMSEncryptionKey`: a key managed by AWS KMS.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AwsKmsEncryptionKeyConfig {
    pub key_id: String,
    pub region: String,
    pub credentials_file: String,
}

/// `schema.CloudKMSEncryptionKey`: a key managed by Google Cloud KMS.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CloudKmsEncryptionKeyConfig {
    pub keyname: String,
    pub credentials_file: String,
}

/// `schema.EncryptionKey`, a tagged union discriminated by `type`.
#[derive(Debug, Clone, PartialEq)]
pub enum EncryptionKeyConfig {
    Cloudkms(CloudKmsEncryptionKeyConfig),
    Awskms(AwsKmsEncryptionKeyConfig),
    Mounted(MountedEncryptionKeyConfig),
    Noop,
    /// An entry whose `type` is not one of the four known types. Go fails to
    /// unmarshal such a site config; the Rust parser keeps the value so keyring
    /// construction can report the baseline's `couldn't configure key` error.
    Unknown(serde_json::Value),
}

/// `schema.EncryptionKeys` (`siteConfig.encryption.keys`).
#[derive(Debug, Clone, Default, PartialEq)]
pub struct EncryptionKeysConfig {
    pub batch_changes_credential_key: Option<EncryptionKeyConfig>,
    pub cache_size: i64,
    pub enable_cache: bool,
    pub executor_secret_key: Option<EncryptionKeyConfig>,
    pub external_service_key: Option<EncryptionKeyConfig>,
    pub github_app_key: Option<EncryptionKeyConfig>,
    pub outbound_webhook_key: Option<EncryptionKeyConfig>,
    pub user_external_account_key: Option<EncryptionKeyConfig>,
    pub webhook_key: Option<EncryptionKeyConfig>,
    pub webhook_log_key: Option<EncryptionKeyConfig>,
}

/// The parsed site configuration (`conf.Unified.SiteConfiguration`), kept as
/// JSON plus typed accessors for the fields the Rust services read.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct SiteConfiguration {
    raw: serde_json::Value,
}

impl SiteConfiguration {
    /// `parseConfigData`: empty means the zero config; otherwise JSONC.
    pub fn parse(data: &str) -> Result<Self, ConfigError> {
        if data.is_empty() {
            return Ok(Self::default());
        }
        Ok(Self {
            raw: serde_json::from_str(&strip_jsonc(data))?,
        })
    }

    /// The raw JSON object.
    pub fn raw(&self) -> &serde_json::Value {
        &self.raw
    }

    fn bool_field(&self, key: &str) -> bool {
        self.raw
            .get(key)
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false)
    }

    /// `siteConfig.cloneProgressLog`.
    pub fn clone_progress_log(&self) -> bool {
        self.bool_field("cloneProgressLog")
    }

    /// `siteConfig.disableAutoGitUpdates`.
    pub fn disable_auto_git_updates(&self) -> bool {
        self.bool_field("disableAutoGitUpdates")
    }

    /// `conf.GitLongCommandTimeout`: `siteConfig.gitLongCommandTimeout`
    /// seconds, defaulting to 2h. Non-positive values keep the default.
    pub fn git_long_command_timeout(&self) -> Duration {
        match self
            .raw
            .get("gitLongCommandTimeout")
            .and_then(serde_json::Value::as_i64)
        {
            Some(seconds) if seconds > 0 => Duration::from_secs(seconds as u64),
            _ => DEFAULT_GIT_LONG_COMMAND_TIMEOUT,
        }
    }

    /// `siteConfig.gitMaxCodehostRequestsPerSecond` (`*int`): the maximum
    /// number of remote code host git operations per second per gitserver.
    /// `None` is the baseline's nil pointer ("unlimited", `-1`).
    pub fn git_max_codehost_requests_per_second(&self) -> Option<i64> {
        self.raw
            .get("gitMaxCodehostRequestsPerSecond")
            .and_then(serde_json::Value::as_i64)
    }

    /// `conf.CodeIntelAutoIndexingPolicyRepositoryMatchLimit`: the
    /// `codeIntelAutoIndexing.policyRepositoryMatchLimit` site setting. The
    /// baseline field is a `*int` with a dotted JSON key; a missing value or
    /// anything below `-1` means `-1` (unlimited). Note the baseline would
    /// reject a non-integer value while parsing the whole site configuration;
    /// this accessor simply treats it as unset (see `06_worker.md` §33).
    pub fn codeintel_autoindexing_policy_repository_match_limit(&self) -> i64 {
        match self
            .raw
            .get("codeIntelAutoIndexing.policyRepositoryMatchLimit")
            .and_then(serde_json::Value::as_i64)
        {
            Some(value) if value >= -1 => value,
            _ => -1,
        }
    }

    /// `conf.GitMaxConcurrentClones`: `siteConfig.gitMaxConcurrentClones`,
    /// defaulting to 5 when unset or non-positive.
    pub fn git_max_concurrent_clones(&self) -> usize {
        match self
            .raw
            .get("gitMaxConcurrentClones")
            .and_then(serde_json::Value::as_i64)
        {
            Some(value) if value > 0 => value as usize,
            _ => DEFAULT_GIT_MAX_CONCURRENT_CLONES,
        }
    }

    /// `siteConfig.disableAutoCodeHostSyncs`.
    pub fn disable_auto_code_host_syncs(&self) -> bool {
        self.bool_field("disableAutoCodeHostSyncs")
    }

    /// `experimentalFeatures.batchChanges.enablePerforce`: whether Perforce is
    /// a supported Batch Changes code host
    /// (`batches/types.GetSupportedExternalServices`).
    pub fn batch_changes_enable_perforce(&self) -> bool {
        self.experimental_features()
            .and_then(|features| features.get("batchChanges"))
            .and_then(|batch_changes| batch_changes.get("enablePerforce"))
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false)
    }

    /// `experimentalFeatures.insightsDataRetention`: a tri-state
    /// `*bool`. `None` (unset/null) keeps data retention enabled; only an
    /// explicit `false` disables the archive step.
    pub fn insights_data_retention(&self) -> Option<bool> {
        self.experimental_features()
            .and_then(|features| features.get("insightsDataRetention"))
            .and_then(serde_json::Value::as_bool)
    }

    /// `conf.Get().InsightsMaximumSampleSize`: the top-level
    /// `insights.maximumSampleSize` site configuration field (note the dotted
    /// JSON key). The code insights retention job interprets `<= 0` as
    /// "unset".
    pub fn insights_maximum_sample_size(&self) -> i64 {
        self.raw
            .get("insights.maximumSampleSize")
            .and_then(serde_json::Value::as_i64)
            .unwrap_or(0)
    }

    /// `conf.Get().LicenseKey`: the product license key from the site
    /// configuration. Empty when the instance runs without a license.
    pub fn license_key(&self) -> String {
        self.raw
            .get("licenseKey")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default()
            .to_owned()
    }

    /// `conf.RepoListUpdateInterval`: `siteConfig.repoListUpdateInterval`
    /// minutes, defaulting to 1 minute. A negative value has no valid
    /// behaviour in the baseline (the periodic goroutine would receive a
    /// non-positive ticker interval), so it also falls back to the default.
    pub fn repo_list_update_interval(&self) -> Duration {
        match self
            .raw
            .get("repoListUpdateInterval")
            .and_then(serde_json::Value::as_i64)
        {
            Some(minutes) if minutes > 0 => Duration::from_secs(minutes as u64 * 60),
            _ => Duration::from_secs(60),
        }
    }

    /// `conf.RepoConcurrentExternalServiceSyncers`:
    /// `siteConfig.repoConcurrentExternalServiceSyncers`, defaulting to 3
    /// when unset or non-positive.
    pub fn repo_concurrent_external_service_syncers(&self) -> usize {
        match self
            .raw
            .get("repoConcurrentExternalServiceSyncers")
            .and_then(serde_json::Value::as_i64)
        {
            Some(value) if value > 0 => value as usize,
            _ => 3,
        }
    }

    /// `siteConfig.repoPurgeWorker`, defaulting to 15/60 minutes when the
    /// section is absent (a present section with missing fields means 0,
    /// which disables the worker like the baseline).
    pub fn repo_purge_worker(&self) -> RepoPurgeWorkerConfig {
        let Some(section) = self
            .raw
            .get("repoPurgeWorker")
            .and_then(serde_json::Value::as_object)
        else {
            return RepoPurgeWorkerConfig {
                interval_minutes: 15,
                deleted_ttl_minutes: 60,
            };
        };
        RepoPurgeWorkerConfig {
            interval_minutes: section
                .get("intervalMinutes")
                .and_then(serde_json::Value::as_i64)
                .unwrap_or(0),
            deleted_ttl_minutes: section
                .get("deletedTTLMinutes")
                .and_then(serde_json::Value::as_i64)
                .unwrap_or(0),
        }
    }

    /// `conf.Unified.GitUpdateInterval`: `siteConfig.gitUpdateInterval` rules.
    /// Each rule is a repo-name pattern (Go `regexp`) and an interval in
    /// minutes; the scheduler uses the first match.
    pub fn git_update_interval(&self) -> Vec<GitUpdateIntervalRule> {
        self.raw
            .get("gitUpdateInterval")
            .and_then(serde_json::Value::as_array)
            .map(|rules| {
                rules
                    .iter()
                    .filter_map(|rule| {
                        Some(GitUpdateIntervalRule {
                            pattern: rule.get("pattern")?.as_str()?.to_owned(),
                            interval_minutes: rule.get("interval")?.as_i64()?,
                        })
                    })
                    .collect()
            })
            .unwrap_or_default()
    }

    /// `experimentalFeatures`.
    pub fn experimental_features(&self) -> Option<&serde_json::Value> {
        self.raw.get("experimentalFeatures")
    }

    /// `experimentalFeatures.gitServerPinnedRepos`: repositories that must
    /// stay on a specific gitserver, keyed by repo name.
    pub fn git_server_pinned_repos(&self) -> std::collections::BTreeMap<String, String> {
        self.experimental_features()
            .and_then(|features| features.get("gitServerPinnedRepos"))
            .and_then(serde_json::Value::as_object)
            .map(|pins| {
                pins.iter()
                    .filter_map(|(repo, addr)| Some((repo.clone(), addr.as_str()?.to_owned())))
                    .collect()
            })
            .unwrap_or_default()
    }

    /// `experimentalFeatures.customGitFetch`.
    pub fn custom_git_fetch(&self) -> Vec<CustomFetchMapping> {
        self.experimental_features()
            .and_then(|features| features.get("customGitFetch"))
            .and_then(serde_json::Value::as_array)
            .map(|mappings| {
                mappings
                    .iter()
                    .filter_map(|mapping| {
                        let domain_path = mapping.get("domainPath")?.as_str()?.to_owned();
                        let fetch = mapping
                            .get("fetch")?
                            .as_str()?
                            .split_whitespace()
                            .map(str::to_owned)
                            .collect();
                        Some(CustomFetchMapping { domain_path, fetch })
                    })
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Whether `experimentalFeatures.customGitFetch` is present and non-null,
    /// i.e. Go's `CustomGitFetch != nil`. `[]` counts as configured (the
    /// baseline warns when it is configured but `ENABLE_CUSTOM_GIT_FETCH` is
    /// unset).
    pub fn custom_git_fetch_configured(&self) -> bool {
        self.experimental_features()
            .and_then(|features| features.get("customGitFetch"))
            .is_some_and(|value| !value.is_null())
    }

    /// `conf.Get().EncryptionKeys`: the top-level `encryption.keys` object.
    /// `None` when the site config does not configure encryption.
    pub fn encryption_keys(&self) -> Option<EncryptionKeysConfig> {
        let keys = self.raw.get("encryption.keys")?;
        let key = |name: &str| keys.get(name).and_then(parse_encryption_key);
        Some(EncryptionKeysConfig {
            batch_changes_credential_key: key("batchChangesCredentialKey"),
            cache_size: keys
                .get("cacheSize")
                .and_then(serde_json::Value::as_i64)
                .unwrap_or(0),
            enable_cache: keys
                .get("enableCache")
                .and_then(serde_json::Value::as_bool)
                .unwrap_or(false),
            executor_secret_key: key("executorSecretKey"),
            external_service_key: key("externalServiceKey"),
            github_app_key: key("gitHubAppKey"),
            outbound_webhook_key: key("outboundWebhookKey"),
            user_external_account_key: key("userExternalAccountKey"),
            webhook_key: key("webhookKey"),
            webhook_log_key: key("webhookLogKey"),
        })
    }

    /// `experimentalFeatures.tlsExternal`.
    pub fn tls_external(&self) -> TlsExternal {
        let Some(tls) = self
            .experimental_features()
            .and_then(|features| features.get("tlsExternal"))
        else {
            return TlsExternal::default();
        };
        TlsExternal {
            insecure_skip_verify: tls
                .get("insecureSkipVerify")
                .and_then(serde_json::Value::as_bool)
                .unwrap_or(false),
            certificates: tls
                .get("certificates")
                .and_then(serde_json::Value::as_array)
                .map(|certs| {
                    certs
                        .iter()
                        .filter_map(serde_json::Value::as_str)
                        .map(str::to_owned)
                        .collect()
                })
                .unwrap_or_default(),
        }
    }
}

/// `schema.EncryptionKey.UnmarshalJSON`: the `type` property selects the
/// variant. The four known types become typed values; anything else keeps the
/// raw value for the keyring to reject.
fn parse_encryption_key(value: &serde_json::Value) -> Option<EncryptionKeyConfig> {
    if !value.is_object() {
        return None;
    }
    let key_type = value.get("type").and_then(serde_json::Value::as_str)?;
    let string = |key: &str| {
        value
            .get(key)
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default()
            .to_owned()
    };
    Some(match key_type {
        "awskms" => EncryptionKeyConfig::Awskms(AwsKmsEncryptionKeyConfig {
            key_id: string("keyId"),
            region: string("region"),
            credentials_file: string("credentialsFile"),
        }),
        "cloudkms" => EncryptionKeyConfig::Cloudkms(CloudKmsEncryptionKeyConfig {
            keyname: string("keyname"),
            credentials_file: string("credentialsFile"),
        }),
        "mounted" => EncryptionKeyConfig::Mounted(MountedEncryptionKeyConfig {
            env_var_name: string("envVarName"),
            filepath: string("filepath"),
            keyname: string("keyname"),
            version: string("version"),
        }),
        "noop" => EncryptionKeyConfig::Noop,
        _ => EncryptionKeyConfig::Unknown(value.clone()),
    })
}

/// One `siteConfig.gitUpdateInterval` entry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GitUpdateIntervalRule {
    pub pattern: String,
    pub interval_minutes: i64,
}

/// `schema.RepoPurgeWorker`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RepoPurgeWorkerConfig {
    pub interval_minutes: i64,
    pub deleted_ttl_minutes: i64,
}

/// Port of `conf.Unified`.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Unified {
    pub site: SiteConfiguration,
    pub service_connections: ServiceConnections,
}

impl Unified {
    /// `conf.ParseConfig`.
    pub fn parse(raw: &RawUnified) -> Result<Self, ConfigError> {
        Ok(Self {
            site: SiteConfiguration::parse(&raw.site)?,
            service_connections: raw.service_connections.clone(),
        })
    }
}

/// Client for the frontend's `ConfigService` (`internalapi.Client`).
pub struct ConfigClient {
    addr: String,
    channel: tokio::sync::OnceCell<Channel>,
    current: RwLock<Option<Arc<Unified>>>,
    /// The raw configuration `current` was parsed from. `conf`'s store keeps
    /// it to compare fresh fetches without reparsing (`RawUnified.Equal`).
    raw: RwLock<Option<RawUnified>>,
}

impl ConfigClient {
    /// `SRC_FRONTEND_INTERNAL` with the baseline's default.
    pub fn from_env() -> Self {
        Self::new(
            std::env::var("SRC_FRONTEND_INTERNAL")
                .unwrap_or_else(|_| DEFAULT_FRONTEND_INTERNAL.to_owned()),
        )
    }

    /// `new` takes the same address syntax `internalapi` accepts: a bare
    /// `host[:port]`, an HTTP URL, or an HTTPS URL.
    pub fn new(addr: impl Into<String>) -> Self {
        Self {
            addr: normalize_address(&addr.into()),
            channel: tokio::sync::OnceCell::new(),
            current: RwLock::new(None),
            raw: RwLock::new(None),
        }
    }

    pub fn addr(&self) -> &str {
        &self.addr
    }

    async fn channel(&self) -> Result<Channel, ConfigError> {
        self.channel
            .get_or_try_init(|| async {
                let endpoint = Endpoint::from_shared(self.addr.clone())?;
                Ok(endpoint.connect().await?)
            })
            .await
            .cloned()
    }

    /// `internalapi.Client.Configuration`: `GetConfig` and no parsing.
    pub async fn fetch_raw(&self) -> Result<RawUnified, ConfigError> {
        let channel = self.channel().await?;
        let mut client = proto::config_service_client::ConfigServiceClient::new(channel);
        let response = client.get_config(proto::GetConfigRequest {}).await?;
        Ok(RawUnified::from_proto(
            response.get_ref().raw_unified.as_ref(),
        ))
    }

    /// Fetch and parse the configuration, caching it for [`Self::get`].
    pub async fn refresh(&self) -> Result<Arc<Unified>, ConfigError> {
        let raw = self.fetch_raw().await?;
        let unified = Arc::new(Unified::parse(&raw)?);
        *self.raw.write().expect("config lock poisoned") = Some(raw);
        *self.current.write().expect("config lock poisoned") = Some(unified.clone());
        Ok(unified)
    }

    /// `conf.Get`: the last successfully parsed configuration, if any.
    pub fn get(&self) -> Option<Arc<Unified>> {
        self.current.read().expect("config lock poisoned").clone()
    }

    /// The raw configuration behind [`Self::get`].
    pub fn raw(&self) -> Option<RawUnified> {
        self.raw.read().expect("config lock poisoned").clone()
    }

    /// Seed the cache for tests (`conf.Mock`).
    pub fn set(&self, unified: Unified) {
        *self.current.write().expect("config lock poisoned") = Some(Arc::new(unified));
    }
}

/// `conf.Watch`'s shared state: the most recently parsed configuration plus a
/// generation counter that advances whenever it is replaced.
///
/// Consumers either read [`Self::current`] at the point of use (the baseline's
/// `conf.Get()`) or subscribe to [`Self::subscribe`] (the baseline's
/// `conf.Watch(f)`, which also invokes `f` immediately).
pub struct LiveConfig {
    current: RwLock<Arc<Unified>>,
    generation: tokio::sync::watch::Sender<u64>,
}

impl LiveConfig {
    /// Start from the configuration the service booted with (`conf.Get`
    /// returns the initial config before the first background update).
    pub fn new(initial: Unified) -> Self {
        Self {
            current: RwLock::new(Arc::new(initial)),
            generation: tokio::sync::watch::channel(0).0,
        }
    }

    /// `conf.Get`.
    pub fn current(&self) -> Arc<Unified> {
        self.current.read().expect("config lock poisoned").clone()
    }

    /// A counter that changes exactly when the configuration is replaced.
    /// Caches keyed on it (`conf.Cached`) recompute only after a change.
    pub fn generation(&self) -> u64 {
        *self.generation.borrow()
    }

    /// `conf.Watch`: the receiver's `changed()` resolves on every replacement.
    pub fn subscribe(&self) -> tokio::sync::watch::Receiver<u64> {
        self.generation.subscribe()
    }

    /// Replace the snapshot and notify watchers. Also the test/mock hook
    /// (`conf.Mock`).
    pub fn set(&self, unified: Unified) {
        *self.current.write().expect("config lock poisoned") = Arc::new(unified);
        self.generation.send_modify(|generation| {
            *generation = generation.wrapping_add(1);
        });
    }
}

/// `conf.client.continuouslyUpdate`: the background goroutine that keeps a
/// [`LiveConfig`] in sync with the frontend.
pub struct ConfigWatcher {
    client: Arc<ConfigClient>,
    live: Arc<LiveConfig>,
    /// `opts.sleepBetweenUpdates`: only tests shrink this.
    max_poll_jitter: Duration,
}

/// `delayBeforeUnreachableLog`: how long the frontend may be unreachable
/// before the watcher logs it (services often start before the frontend).
const DELAY_BEFORE_UNREACHABLE_LOG: Duration = Duration::from_secs(15);

/// `rand.Int63n(5 * time.Second)`.
const MAX_POLL_JITTER: Duration = Duration::from_secs(5);

impl ConfigWatcher {
    pub fn new(client: Arc<ConfigClient>, live: Arc<LiveConfig>) -> Self {
        Self {
            client,
            live,
            max_poll_jitter: MAX_POLL_JITTER,
        }
    }

    /// Test hook for `continuousUpdateOptions.sleepBetweenUpdates`.
    pub fn with_poll_jitter(mut self, max_poll_jitter: Duration) -> Self {
        self.max_poll_jitter = max_poll_jitter;
        self
    }

    /// Poll `GetConfig` until shutdown, replacing `live` when the raw
    /// configuration changes (`RawUnified.Equal`, so `id` alone does not
    /// count).
    ///
    /// Erratum: the baseline's loop starts with an unconditional
    /// `fetchAndUpdate` before its first sleep. Here the caller has already
    /// fetched the initial configuration (`wait_for_initial_config`), so the
    /// first comparison is seeded from [`ConfigClient::raw`] and the loop
    /// starts with the sleep, avoiding a duplicate update at startup.
    pub async fn run(&self, shutdown: CancellationToken) {
        let mut previous = self.client.raw();
        let mut last_update = Instant::now();

        loop {
            tokio::select! {
                _ = shutdown.cancelled() => return,
                _ = tokio::time::sleep(poll_jitter(self.max_poll_jitter)) => {}
            }

            match self.client.fetch_raw().await {
                Ok(raw) => {
                    last_update = Instant::now();
                    let changed = previous
                        .as_ref()
                        .map(|previous| !previous.equal(&raw))
                        .unwrap_or(true);
                    previous = Some(raw.clone());
                    if !changed {
                        continue;
                    }
                    match Unified::parse(&raw) {
                        Ok(unified) => {
                            tracing::info!("config changed, notifying watchers");
                            self.live.set(unified);
                        }
                        // `store.MaybeUpdate` keeps the previous config when
                        // the new one does not parse.
                        Err(err) => tracing::error!(
                            error = %err,
                            "received error during background config update"
                        ),
                    }
                }
                Err(err) => {
                    if last_update.elapsed() > DELAY_BEFORE_UNREACHABLE_LOG || !err.is_unreachable()
                    {
                        tracing::error!(
                            error = %err,
                            "received error during background config update"
                        );
                    }
                }
            }
        }
    }
}

/// A cheap stand-in for Go's `rand.Int63n(5s)`: the low bits of the wall
/// clock, modulo the jitter window.
fn poll_jitter(max: Duration) -> Duration {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.as_nanos() as u64)
        .unwrap_or(0);
    Duration::from_nanos(nanos % max.as_nanos().max(1) as u64)
}

/// `mustParseSourcegraphInternalURL`: bare addresses default to `http` and
/// port 80. The tonic endpoint needs a scheme, so the host/port form becomes
/// `http://host:80`.
pub fn normalize_address(raw: &str) -> String {
    let (scheme, rest) = match raw.find("://") {
        Some(index) => (&raw[..index], &raw[index + 3..]),
        None => ("http", raw),
    };
    let authority_end = rest.find('/').unwrap_or(rest.len());
    let authority = &rest[..authority_end];
    let remainder = &rest[authority_end..];
    if authority.rsplit(':').next().map(str::len) == Some(authority.len()) {
        // No `:port` after the host.
        format!("{scheme}://{authority}:80{remainder}")
    } else {
        format!("{scheme}://{rest}")
    }
}

/// Parse the baseline's JSONC dialect (`internal/jsonc`): JSON with comments
/// and trailing commas. Used for site config and external service configs.
pub fn parse_jsonc(input: &str) -> Result<serde_json::Value, serde_json::Error> {
    serde_json::from_str(&strip_jsonc(input))
}

/// Remove JSONC comments and trailing commas before handing the text to
/// `serde_json` (the baseline uses `internal/jsonc`, a JSON5-style
/// preprocessor). Comment markers inside strings are preserved.
pub fn strip_jsonc(input: &str) -> String {
    let bytes = input.as_bytes();
    let mut out: Vec<u8> = Vec::with_capacity(bytes.len());
    let mut index = 0;
    let mut in_string = false;
    let mut escaped = false;
    while index < bytes.len() {
        let byte = bytes[index];
        if in_string {
            out.push(byte);
            if escaped {
                escaped = false;
            } else if byte == b'\\' {
                escaped = true;
            } else if byte == b'"' {
                in_string = false;
            }
            index += 1;
            continue;
        }
        match byte {
            b'"' => {
                in_string = true;
                out.push(b'"');
                index += 1;
            }
            b'/' if bytes.get(index + 1) == Some(&b'/') => {
                while index < bytes.len() && bytes[index] != b'\n' {
                    index += 1;
                }
            }
            b'/' if bytes.get(index + 1) == Some(&b'*') => {
                index += 2;
                while index + 1 < bytes.len() && !(bytes[index] == b'*' && bytes[index + 1] == b'/')
                {
                    index += 1;
                }
                index = (index + 2).min(bytes.len());
            }
            _ => {
                out.push(byte);
                index += 1;
            }
        }
    }
    remove_trailing_commas(&String::from_utf8_lossy(&out))
}

/// Remove `,` that is followed only by whitespace and `}` or `]`. The input
/// has no comments at this point, so a byte scan outside strings is enough.
fn remove_trailing_commas(input: &str) -> String {
    let bytes = input.as_bytes();
    let mut out: Vec<u8> = Vec::with_capacity(bytes.len());
    let mut index = 0;
    let mut in_string = false;
    let mut escaped = false;
    while index < bytes.len() {
        let byte = bytes[index];
        if in_string {
            out.push(byte);
            if escaped {
                escaped = false;
            } else if byte == b'\\' {
                escaped = true;
            } else if byte == b'"' {
                in_string = false;
            }
            index += 1;
            continue;
        }
        if byte == b'"' {
            in_string = true;
            out.push(b'"');
            index += 1;
            continue;
        }
        if byte == b',' {
            let mut lookahead = index + 1;
            while lookahead < bytes.len() && bytes[lookahead].is_ascii_whitespace() {
                lookahead += 1;
            }
            if matches!(bytes.get(lookahead), Some(b'}') | Some(b']')) {
                index += 1;
                continue;
            }
        }
        out.push(byte);
        index += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalizes_frontend_addresses() {
        assert_eq!(normalize_address("127.0.0.1:3090"), "http://127.0.0.1:3090");
        assert_eq!(
            normalize_address("http://127.0.0.1:3090"),
            "http://127.0.0.1:3090"
        );
        assert_eq!(
            normalize_address("sourcegraph-frontend-internal"),
            "http://sourcegraph-frontend-internal:80"
        );
        assert_eq!(
            normalize_address("https://frontend.internal/grpc"),
            "https://frontend.internal:80/grpc"
        );
    }

    #[test]
    fn parses_site_config_with_comments_and_trailing_commas() {
        let raw = RawUnified {
            id: 7,
            site: r#"{
                // The clone progress log is opt-in.
                "cloneProgressLog": true,
                "disableAutoGitUpdates": false,
                "gitLongCommandTimeout": 30,
                "experimentalFeatures": {
                    "customGitFetch": [
                        {"domainPath": "github.com/foo/bar", "fetch": "git fetch --depth=1"},
                    ],
                    "tlsExternal": {
                        "insecureSkipVerify": true,
                        "certificates": ["-----BEGIN CERTIFICATE-----"],
                    },
                    "gitServerPinnedRepos": {"github.com/foo/bar": "gitserver-1:3178"},
                },
            }"#
            .to_owned(),
            service_connections: ServiceConnections {
                postgres_dsn: "postgres://sg@db/sourcegraph".to_owned(),
                git_servers: vec!["gitserver-0:3178".to_owned()],
                ..Default::default()
            },
        };

        let unified = Unified::parse(&raw).expect("parse");
        assert!(unified.site.clone_progress_log());
        assert!(!unified.site.disable_auto_git_updates());
        assert_eq!(
            unified.site.git_long_command_timeout(),
            Duration::from_secs(30)
        );
        assert_eq!(
            unified.site.custom_git_fetch(),
            vec![CustomFetchMapping {
                domain_path: "github.com/foo/bar".to_owned(),
                fetch: vec!["git".to_owned(), "fetch".to_owned(), "--depth=1".to_owned()],
            }]
        );
        assert!(unified.site.custom_git_fetch_configured());
        assert_eq!(
            unified.site.tls_external(),
            TlsExternal {
                insecure_skip_verify: true,
                certificates: vec!["-----BEGIN CERTIFICATE-----".to_owned()],
            }
        );
        assert_eq!(
            unified.site.git_server_pinned_repos(),
            [(
                "github.com/foo/bar".to_owned(),
                "gitserver-1:3178".to_owned()
            )]
            .into_iter()
            .collect()
        );
        assert_eq!(unified.service_connections, raw.service_connections);
    }

    #[test]
    fn reads_the_license_key() {
        let site = SiteConfiguration::parse(r#"{"licenseKey": "abc def"}"#).expect("parse");
        assert_eq!(site.license_key(), "abc def");
        assert_eq!(SiteConfiguration::default().license_key(), "");
        // Non-string values are ignored, like the JSON accessor helpers.
        let site = SiteConfiguration::parse(r#"{"licenseKey": 42}"#).expect("parse");
        assert_eq!(site.license_key(), "");
    }

    #[test]
    fn empty_site_config_and_defaults() {
        let unified = Unified::parse(&RawUnified::default()).expect("parse");
        assert!(!unified.site.clone_progress_log());
        assert!(!unified.site.disable_auto_git_updates());
        assert_eq!(
            unified.site.git_long_command_timeout(),
            DEFAULT_GIT_LONG_COMMAND_TIMEOUT
        );
        assert!(unified.site.custom_git_fetch().is_empty());
        assert!(!unified.site.custom_git_fetch_configured());
        assert!(unified.site.git_server_pinned_repos().is_empty());
        assert_eq!(unified.site.tls_external(), TlsExternal::default());
        assert_eq!(
            unified.site.git_max_concurrent_clones(),
            DEFAULT_GIT_MAX_CONCURRENT_CLONES
        );
    }

    #[test]
    fn git_max_concurrent_clones_defaults_when_non_positive() {
        for (data, want) in [
            (r#"{"gitMaxConcurrentClones": 100}"#, 100),
            (r#"{"gitMaxConcurrentClones": 0}"#, 5),
            (r#"{"gitMaxConcurrentClones": -100}"#, 5),
            (r#"{}"#, 5),
        ] {
            let site = SiteConfiguration::parse(data).expect("parse");
            assert_eq!(site.git_max_concurrent_clones(), want, "{data}");
        }
    }

    #[test]
    fn parses_encryption_keys_from_the_dotted_site_config_key() {
        let site = SiteConfiguration::parse(
            r#"{
                "encryption.keys": {
                    "enableCache": true,
                    "cacheSize": 1024,
                    "externalServiceKey": {
                        "type": "mounted",
                        "keyname": "test-key",
                        "version": "v1",
                        "envVarName": "SG_KEY",
                    },
                    "webhookKey": {"type": "noop"},
                    "gitHubAppKey": {"type": "awskms", "keyId": "arn"},
                    "executorSecretKey": {
                        "type": "cloudkms",
                        "keyname": "projects/p/locations/l/keyRings/r/cryptoKeys/k",
                        "credentialsFile": "/tmp/creds.json",
                    },
                    "webhookLogKey": {"type": "mystery"},
                },
            }"#,
        )
        .expect("parse");
        let keys = site.encryption_keys().expect("keys");
        assert!(keys.enable_cache);
        assert_eq!(keys.cache_size, 1024);
        assert_eq!(
            keys.external_service_key,
            Some(EncryptionKeyConfig::Mounted(MountedEncryptionKeyConfig {
                env_var_name: "SG_KEY".to_owned(),
                filepath: String::new(),
                keyname: "test-key".to_owned(),
                version: "v1".to_owned(),
            }))
        );
        assert_eq!(keys.webhook_key, Some(EncryptionKeyConfig::Noop));
        assert_eq!(
            keys.github_app_key,
            Some(EncryptionKeyConfig::Awskms(AwsKmsEncryptionKeyConfig {
                key_id: "arn".to_owned(),
                region: String::new(),
                credentials_file: String::new(),
            }))
        );
        assert_eq!(
            keys.executor_secret_key,
            Some(EncryptionKeyConfig::Cloudkms(CloudKmsEncryptionKeyConfig {
                keyname: "projects/p/locations/l/keyRings/r/cryptoKeys/k".to_owned(),
                credentials_file: "/tmp/creds.json".to_owned(),
            }))
        );
        assert!(matches!(
            keys.webhook_log_key,
            Some(EncryptionKeyConfig::Unknown(_))
        ));
        assert_eq!(keys.batch_changes_credential_key, None);

        assert_eq!(SiteConfiguration::default().encryption_keys(), None);
    }

    #[test]
    fn keeps_comment_markers_inside_strings() {
        let site =
            SiteConfiguration::parse(r#"{"a": "http://example.com", "b": 1,}"#).expect("parse");
        assert_eq!(site.raw()["a"], "http://example.com");
    }

    #[tokio::test]
    async fn fetches_and_caches_config_from_the_frontend() {
        use proto::config_service_server::{ConfigService, ConfigServiceServer};

        struct FakeConfig(proto::RawUnified);

        #[tonic::async_trait]
        impl ConfigService for FakeConfig {
            async fn get_config(
                &self,
                _request: tonic::Request<proto::GetConfigRequest>,
            ) -> Result<tonic::Response<proto::GetConfigResponse>, tonic::Status> {
                Ok(tonic::Response::new(proto::GetConfigResponse {
                    raw_unified: Some(self.0.clone()),
                }))
            }
        }

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let addr = listener.local_addr().expect("addr");
        let incoming = tonic::transport::server::TcpIncoming::from_listener(
            listener,
            true,
            Some(Duration::from_secs(5)),
        )
        .expect("incoming");
        let raw = proto::RawUnified {
            id: 11,
            site: r#"{"cloneProgressLog": true}"#.to_owned(),
            service_connections: Some(proto::ServiceConnections {
                postgres_dsn: "postgres://sourcegraph@db/sourcegraph".to_owned(),
                ..Default::default()
            }),
        };
        let server = tonic::transport::Server::builder()
            .add_service(ConfigServiceServer::new(FakeConfig(raw)));
        let handle = tokio::spawn(async move { server.serve_with_incoming(incoming).await });

        let client = ConfigClient::new(format!("127.0.0.1:{}", addr.port()));
        assert_eq!(client.addr(), format!("http://127.0.0.1:{}", addr.port()));

        let raw = client.fetch_raw().await.expect("fetch");
        assert_eq!(raw.id, 11);
        assert_eq!(
            raw.service_connections.postgres_dsn,
            "postgres://sourcegraph@db/sourcegraph"
        );

        let unified = client.refresh().await.expect("refresh");
        assert!(unified.site.clone_progress_log());
        let cached = client.get().expect("cached");
        assert!(Arc::ptr_eq(&unified, &cached));

        handle.abort();
    }

    #[tokio::test]
    async fn config_watcher_follows_the_frontend() {
        use proto::config_service_server::{ConfigService, ConfigServiceServer};
        use std::sync::Mutex;

        struct FakeConfig(Arc<Mutex<proto::RawUnified>>);

        #[tonic::async_trait]
        impl ConfigService for FakeConfig {
            async fn get_config(
                &self,
                _request: tonic::Request<proto::GetConfigRequest>,
            ) -> Result<tonic::Response<proto::GetConfigResponse>, tonic::Status> {
                let raw = self.0.lock().expect("lock").clone();
                Ok(tonic::Response::new(proto::GetConfigResponse {
                    raw_unified: Some(raw),
                }))
            }
        }

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let addr = listener.local_addr().expect("addr");
        let incoming = tonic::transport::server::TcpIncoming::from_listener(
            listener,
            true,
            Some(Duration::from_secs(5)),
        )
        .expect("incoming");
        let raw = Arc::new(Mutex::new(proto::RawUnified {
            id: 1,
            site: r#"{"cloneProgressLog": true}"#.to_owned(),
            ..Default::default()
        }));
        let server = tonic::transport::Server::builder()
            .add_service(ConfigServiceServer::new(FakeConfig(Arc::clone(&raw))));
        let handle = tokio::spawn(async move { server.serve_with_incoming(incoming).await });

        let client = Arc::new(ConfigClient::new(format!("127.0.0.1:{}", addr.port())));
        let initial = client.refresh().await.expect("initial config");
        assert!(initial.site.clone_progress_log());
        let live = Arc::new(LiveConfig::new((*initial).clone()));
        let mut updates = live.subscribe();

        let watcher = ConfigWatcher::new(Arc::clone(&client), Arc::clone(&live))
            .with_poll_jitter(Duration::from_millis(10));
        let shutdown = CancellationToken::new();
        let task = tokio::spawn({
            let shutdown = shutdown.clone();
            async move { watcher.run(shutdown).await }
        });

        // The same raw config is not a change.
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(live.generation(), 0);

        // A site config change reaches the watchers within the poll window.
        raw.lock().expect("lock").site = r#"{"cloneProgressLog": false}"#.to_owned();
        tokio::time::timeout(Duration::from_secs(5), updates.changed())
            .await
            .expect("config change")
            .expect("watcher alive");
        assert_eq!(live.generation(), 1);
        assert!(!live.current().site.clone_progress_log());

        shutdown.cancel();
        task.await.expect("watcher exits");
        handle.abort();
    }

    /// Manual parity check against a running Go frontend:
    /// `SG_TEST_FRONTEND_ADDR=127.0.0.1:3090 cargo test -p sg-config`.
    #[tokio::test]
    async fn fetches_config_from_a_running_frontend() {
        let Ok(addr) = std::env::var("SG_TEST_FRONTEND_ADDR") else {
            return;
        };
        let client = ConfigClient::new(addr);
        let raw = client.fetch_raw().await.expect("fetch from frontend");
        assert!(!raw.site.is_empty(), "site config is empty");
        let unified = Unified::parse(&raw).expect("parse");
        assert!(
            !unified.service_connections.postgres_dsn.is_empty(),
            "postgres DSN is empty"
        );
    }

    #[tokio::test]
    async fn live_config_notifies_watchers_on_change() {
        let live = LiveConfig::new(Unified::parse(&RawUnified::default()).expect("parse"));
        assert!(!live.current().site.clone_progress_log());

        // `conf.Watch(f)` runs f immediately; here that is the initial read.
        let mut updates = live.subscribe();
        assert_eq!(live.generation(), 0);

        live.set(
            Unified::parse(&RawUnified {
                site: r#"{"cloneProgressLog": true}"#.to_owned(),
                ..Default::default()
            })
            .expect("parse"),
        );
        updates.changed().await.expect("change");
        assert_eq!(live.generation(), 1);
        assert!(live.current().site.clone_progress_log());

        // The poll jitter is always below the 5s window (`rand.Int63n`).
        assert!(poll_jitter(MAX_POLL_JITTER) < MAX_POLL_JITTER);
    }

    #[test]
    fn unreachable_errors_are_flagged() {
        assert!(ConfigError::Client(tonic::Status::unavailable("down")).is_unreachable());
        assert!(!ConfigError::Client(tonic::Status::internal("boom")).is_unreachable());
    }

    #[test]
    fn raw_unified_from_proto_matches_the_baseline() {
        let proto = proto::RawUnified {
            id: 3,
            site: "{}".to_owned(),
            service_connections: Some(proto::ServiceConnections {
                postgres_dsn: "postgres://x".to_owned(),
                zoekts: vec!["zoekt-0".to_owned()],
                zoekt_list_ttl: Some(prost_types::Duration {
                    seconds: 60,
                    nanos: 0,
                }),
                ..Default::default()
            }),
        };
        let raw = RawUnified::from_proto(Some(&proto));
        assert_eq!(raw.id, 3);
        assert_eq!(raw.service_connections.postgres_dsn, "postgres://x");
        assert_eq!(raw.service_connections.zoekts, vec!["zoekt-0".to_owned()]);
        assert_eq!(
            raw.service_connections.zoekt_list_ttl,
            Duration::from_secs(60)
        );
    }
}
