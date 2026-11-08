#![forbid(unsafe_code)]

//! `cmd/worker/internal/executors/metricsserver_{job,config}.go`: the
//! `executors-metricsserver` job.
//!
//! One HTTP server routine (`httpserver.NewFromAddr`) serving
//! `promhttp.HandlerFor(store.Gather, HandlerOpts{})` over the executor
//! metrics blobs that executor instances write to the cache Redis under
//! `executors:<instance>`.
//!
//! The address is `net.JoinHostPort(host, port)` where `host` is `127.0.0.1`
//! in insecure dev and empty otherwise; the default port is `6996`. The store
//! and the handler live in [`crate::framework::metrics_server`].

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use sg_core::{ConfigError, EnvReader};
use tokio_util::sync::CancellationToken;

use super::{Job, JobConfig, JobDeps};
use crate::config::combine_errors;
use crate::env;
use crate::framework::metrics_server::{self, DistributedStore, MetricsServerRoutine};
use crate::framework::{BackgroundRoutine, ServerRoutine};

/// `executors.metricsServerConfig`: one `env.BaseConfig` value.
#[derive(Debug, Default)]
pub struct MetricsServerConfig {
    values: Mutex<MetricsServerValues>,
    errs: Mutex<Vec<String>>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct MetricsServerValues {
    pub metrics_server_port: i64,
}

impl MetricsServerConfig {
    pub fn snapshot(&self) -> MetricsServerValues {
        *self
            .values
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

impl JobConfig for MetricsServerConfig {
    /// `metricsServerConfig.Load`.
    fn load(&self, env: &mut EnvReader) {
        let mut errs = Vec::new();
        let values = MetricsServerValues {
            metrics_server_port: get_int(
                env,
                "EXECUTORS_METRICS_SERVER_PORT",
                "6996",
                "The port to listen on to serve the metrics from executors.",
                &mut errs,
            ),
        };

        *self
            .values
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = values;
        *self
            .errs
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = errs;
    }

    /// `env.BaseConfig.Validate`.
    fn validate(&self) -> Result<(), ConfigError> {
        let errs = self
            .errs
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        match combine_errors(&errs) {
            Some(message) => Err(ConfigError::new(message)),
            None => Ok(()),
        }
    }
}

/// `BaseConfig.GetInt`: `strconv.ParseInt(raw, 10, 64)`, record
/// `invalid int %q for %s: %s` and return 0 on failure.
fn get_int(
    env: &mut EnvReader,
    name: &str,
    default: &str,
    description: &str,
    errors: &mut Vec<String>,
) -> i64 {
    let raw = env.get(name, default, description);
    match parse_go_int(&raw) {
        Ok(value) => value,
        Err(kind) => {
            errors.push(format!("invalid int {raw:?} for {name}: {kind}"));
            0
        }
    }
}

/// `strconv.ParseInt(raw, 10, 64)`; the error text is the `NumError`'s
/// `Error()` (`strconv.ParseInt: parsing %q: <err>`).
fn parse_go_int(raw: &str) -> Result<i64, String> {
    match raw.parse::<i64>() {
        Ok(value) => Ok(value),
        Err(err) => {
            // Rust's `FromStr` accepts the same optional sign as `ParseInt`
            // and rejects base-10 underscores, so only the wording differs.
            let reason = match err.kind() {
                std::num::IntErrorKind::PosOverflow | std::num::IntErrorKind::NegOverflow => {
                    "value out of range"
                }
                _ => "invalid syntax",
            };
            Err(format!("strconv.ParseInt: parsing {raw:?}: {reason}"))
        }
    }
}

/// `executors.metricsServerJob`.
pub struct ExecutorsMetricsServer {
    config: Arc<MetricsServerConfig>,
}

impl ExecutorsMetricsServer {
    pub fn new() -> Self {
        Self {
            config: Arc::new(MetricsServerConfig::default()),
        }
    }

    /// The job's `metricsServerConfigInst`, for tests and harnesses.
    pub fn config(&self) -> &MetricsServerConfig {
        &self.config
    }
}

impl Default for ExecutorsMetricsServer {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Job for ExecutorsMetricsServer {
    fn description(&self) -> String {
        "HTTP server exposing the metrics collected from executors to Prometheus".to_owned()
    }

    fn configs(&self) -> Vec<Arc<dyn JobConfig>> {
        vec![Arc::clone(&self.config) as Arc<dyn JobConfig>]
    }

    async fn routines(
        &self,
        _deps: &JobDeps,
        _setup_ctx: CancellationToken,
    ) -> Result<Vec<BackgroundRoutine>, String> {
        let host = if env::insecure_dev() { "127.0.0.1" } else { "" };
        let addr = join_host_port(host, self.config.snapshot().metrics_server_port);

        let store = Arc::new(DistributedStore::new("executors:"));
        let handler = metrics_server::handler(store);

        let routine: Arc<dyn ServerRoutine> = Arc::new(MetricsServerRoutine::new(addr, handler));
        Ok(vec![BackgroundRoutine::Server(routine)])
    }
}

/// `net.JoinHostPort`: bracket a host that already contains a colon (an IPv6
/// literal); `""` yields `":<port>"`.
fn join_host_port(host: &str, port: i64) -> String {
    if host.contains(':') {
        format!("[{host}]:{port}")
    } else {
        format!("{host}:{port}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn loaded_config() -> MetricsServerConfig {
        let config = MetricsServerConfig::default();
        config.load(&mut EnvReader::new());
        config
    }

    #[test]
    fn default_port_is_6996() {
        let config = loaded_config();
        assert_eq!(config.snapshot().metrics_server_port, 6996);
        assert!(config.validate().is_ok());
    }

    #[test]
    fn invalid_ints_use_the_go_error_text() {
        assert_eq!(parse_go_int("6996"), Ok(6996));
        assert_eq!(parse_go_int("+42"), Ok(42));
        assert_eq!(parse_go_int("-7"), Ok(-7));
        assert_eq!(
            parse_go_int("abc").expect_err("invalid syntax"),
            "strconv.ParseInt: parsing \"abc\": invalid syntax"
        );
        assert_eq!(
            parse_go_int("1.5").expect_err("invalid syntax"),
            "strconv.ParseInt: parsing \"1.5\": invalid syntax"
        );
        assert_eq!(
            parse_go_int("99999999999999999999").expect_err("out of range"),
            "strconv.ParseInt: parsing \"99999999999999999999\": value out of range"
        );
    }

    #[test]
    fn join_host_port_matches_net_package() {
        assert_eq!(join_host_port("", 6996), ":6996");
        assert_eq!(join_host_port("127.0.0.1", 6996), "127.0.0.1:6996");
        assert_eq!(join_host_port("::1", 6996), "[::1]:6996");
    }

    #[tokio::test]
    async fn job_builds_the_server_routine() {
        let deps = JobDeps {
            frontend: sqlx::postgres::PgPoolOptions::new()
                .connect_lazy("postgres://unused@localhost/unused")
                .expect("lazy pool"),
            live_config: Arc::new(sg_config::LiveConfig::new(sg_config::Unified::default())),
            keyring: Arc::new(sg_keyring::Keyring::default()),
        };
        let routines = ExecutorsMetricsServer::new()
            .routines(&deps, CancellationToken::new())
            .await
            .expect("routines");

        assert_eq!(routines.len(), 1);
        assert_eq!(routines[0].name(), "HTTP server");
        assert_eq!(routines[0].description(), "");
        // The returned value is a `Vec<BackgroundRoutine>`; the recorder
        // registers `Arc`-wrapped routines (see `create_background_routines`).
        let routine = Arc::new(routines[0].clone());
        assert!(routine.recordable().is_none());
    }
}
