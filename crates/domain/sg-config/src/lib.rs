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
//! The client does not yet implement the baseline's watcher goroutine
//! (`conf.Watch`); callers refresh explicitly and read the cached value.

use std::sync::{Arc, RwLock};
use std::time::Duration;

use sg_proto::internalapi as proto;
use tonic::transport::{Channel, Endpoint};

/// `SRC_FRONTEND_INTERNAL` default (`internalapi.frontendInternal`).
pub const DEFAULT_FRONTEND_INTERNAL: &str = "sourcegraph-frontend-internal";

/// `conf.defaultGitLongCommandTimeout`.
pub const DEFAULT_GIT_LONG_COMMAND_TIMEOUT: Duration = Duration::from_secs(2 * 60 * 60);

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

    /// `experimentalFeatures`.
    pub fn experimental_features(&self) -> Option<&serde_json::Value> {
        self.raw.get("experimentalFeatures")
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
        *self.current.write().expect("config lock poisoned") = Some(unified.clone());
        Ok(unified)
    }

    /// `conf.Get`: the last successfully parsed configuration, if any.
    pub fn get(&self) -> Option<Arc<Unified>> {
        self.current.read().expect("config lock poisoned").clone()
    }

    /// Seed the cache for tests (`conf.Mock`).
    pub fn set(&self, unified: Unified) {
        *self.current.write().expect("config lock poisoned") = Some(Arc::new(unified));
    }
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

/// Remove JSONC comments and trailing commas before handing the text to
/// `serde_json` (the baseline uses `internal/jsonc`, a JSON5-style
/// preprocessor). Comment markers inside strings are preserved.
fn strip_jsonc(input: &str) -> String {
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
        assert_eq!(
            unified.site.tls_external(),
            TlsExternal {
                insecure_skip_verify: true,
                certificates: vec!["-----BEGIN CERTIFICATE-----".to_owned()],
            }
        );
        assert_eq!(unified.service_connections, raw.service_connections);
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
        assert_eq!(unified.site.tls_external(), TlsExternal::default());
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
