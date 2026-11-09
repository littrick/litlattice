#![forbid(unsafe_code)]

//! `cmd/frontend/internal/cli/serve_cmd.go` listener configuration and
//! `cmd/frontend/envvar`'s internal address.
//!
//! The defaults depend on `INSECURE_DEV` (dev binds loopback, production binds
//! every interface). Empty addresses keep their baseline meaning: an empty
//! internal address disables the internal server, and an empty prof address
//! disables the debug server. An empty external address is passed to
//! `net.Listen("tcp", "")`, which picks a random port; [`FrontendConfig::external_bind_address`]
//! maps that to `0.0.0.0:0`.

use std::time::Duration;

use sg_core::{ConfigError, EnvReader};

/// `http.Server.ReadTimeout` for the external listener.
pub const EXTERNAL_READ_TIMEOUT: Duration = Duration::from_secs(75);
/// `http.Server.WriteTimeout` for the external listener.
pub const EXTERNAL_WRITE_TIMEOUT: Duration = Duration::from_secs(600);
/// `http.Server.ReadTimeout` for the internal listener.
pub const INTERNAL_READ_TIMEOUT: Duration = Duration::from_secs(75);
/// `http.Server.WriteTimeout` for the internal listener (git archive sized).
pub const INTERNAL_WRITE_TIMEOUT: Duration = Duration::from_secs(3600);

/// `debugserver`'s default `SRC_PROF_HTTP`.
pub const DEFAULT_PROF_HTTP_ADDR: &str = ":6060";

const INSECURE_DEV_VAR: &str = "INSECURE_DEV";
const HTTP_ADDR_VAR: &str = "SRC_HTTP_ADDR";
const HTTP_ADDR_INTERNAL_VAR: &str = "SRC_HTTP_ADDR_INTERNAL";
const PROF_HTTP_VAR: &str = "SRC_PROF_HTTP";

/// The listener configuration read once at startup.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FrontendConfig {
    pub insecure_dev: bool,
    /// `SRC_HTTP_ADDR`; may be empty (random port, like Go).
    pub http_addr: String,
    /// `SRC_HTTP_ADDR_INTERNAL`; empty disables the internal server.
    pub http_addr_internal: String,
    /// `SRC_PROF_HTTP`; `None` when empty (debug server disabled).
    pub prof_http_addr: Option<String>,
}

impl FrontendConfig {
    /// Register and read every variable; the baseline reads them at package
    /// init, so this is the only place they are consumed.
    pub fn load(env: &mut EnvReader) -> Result<Self, ConfigError> {
        let insecure_dev = env.get_bool(
            INSECURE_DEV_VAR,
            false,
            "Running in insecure dev (local laptop) mode",
        );

        let http_default = if insecure_dev {
            "127.0.0.1:3080"
        } else {
            ":3080"
        };
        let http_addr = env.get(
            HTTP_ADDR_VAR,
            http_default,
            "HTTP listen address for app and HTTP API",
        );

        let internal_default = if insecure_dev {
            "127.0.0.1:3090"
        } else {
            "0.0.0.0:3090"
        };
        let http_addr_internal = env.get(
            HTTP_ADDR_INTERNAL_VAR,
            internal_default,
            "HTTP listen address for internal HTTP API. This should never be exposed externally, as it lacks certain authz checks.",
        );

        let prof_http_addr = Some(env.get(
            PROF_HTTP_VAR,
            DEFAULT_PROF_HTTP_ADDR,
            "net/http/pprof http bind address.",
        ))
        .filter(|addr| !addr.is_empty());

        Ok(Self {
            insecure_dev,
            http_addr,
            http_addr_internal,
            prof_http_addr,
        })
    }

    /// The address handed to the listener. Go's `net.Listen("tcp", "")` binds
    /// a random port on every interface.
    pub fn external_bind_address(&self) -> &str {
        if self.http_addr.is_empty() {
            "0.0.0.0:0"
        } else {
            &self.http_addr
        }
    }

    /// `makeInternalAPI`: an empty address returns no server at all.
    pub fn internal_bind_address(&self) -> Option<&str> {
        Some(self.http_addr_internal.as_str()).filter(|addr| !addr.is_empty())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn load_with(vars: &[(&str, &str)]) -> FrontendConfig {
        // The reader resolves from the process environment; serialize and set
        // the variables for the duration of the call.
        static LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());
        let _guard = LOCK.lock().unwrap_or_else(|err| err.into_inner());
        for name in [
            INSECURE_DEV_VAR,
            HTTP_ADDR_VAR,
            HTTP_ADDR_INTERNAL_VAR,
            PROF_HTTP_VAR,
        ] {
            std::env::remove_var(name);
        }
        for (name, value) in vars {
            std::env::set_var(name, value);
        }
        let mut env = EnvReader::new();
        let config = FrontendConfig::load(&mut env).expect("config");
        for name in [
            INSECURE_DEV_VAR,
            HTTP_ADDR_VAR,
            HTTP_ADDR_INTERNAL_VAR,
            PROF_HTTP_VAR,
        ] {
            std::env::remove_var(name);
        }
        config
    }

    #[test]
    fn production_defaults_bind_every_interface() {
        let config = load_with(&[]);
        assert!(!config.insecure_dev);
        assert_eq!(config.http_addr, ":3080");
        assert_eq!(config.http_addr_internal, "0.0.0.0:3090");
        assert_eq!(config.prof_http_addr.as_deref(), Some(":6060"));
    }

    #[test]
    fn insecure_dev_defaults_bind_loopback() {
        let config = load_with(&[("INSECURE_DEV", "true")]);
        assert!(config.insecure_dev);
        assert_eq!(config.http_addr, "127.0.0.1:3080");
        assert_eq!(config.http_addr_internal, "127.0.0.1:3090");
    }

    #[test]
    fn explicit_addresses_win() {
        let config = load_with(&[
            ("INSECURE_DEV", "true"),
            ("SRC_HTTP_ADDR", ":4080"),
            ("SRC_HTTP_ADDR_INTERNAL", "127.0.0.1:4090"),
            ("SRC_PROF_HTTP", ":4060"),
        ]);
        assert_eq!(config.external_bind_address(), ":4080");
        assert_eq!(config.internal_bind_address(), Some("127.0.0.1:4090"));
        assert_eq!(config.prof_http_addr.as_deref(), Some(":4060"));
    }

    #[test]
    fn empty_internal_and_prof_addresses_disable_the_servers() {
        let config = load_with(&[("SRC_HTTP_ADDR_INTERNAL", ""), ("SRC_PROF_HTTP", "")]);
        assert_eq!(config.internal_bind_address(), None);
        assert_eq!(config.prof_http_addr, None);
    }

    #[test]
    fn empty_external_address_maps_to_a_random_port() {
        let config = load_with(&[("SRC_HTTP_ADDR", "")]);
        assert_eq!(config.external_bind_address(), "0.0.0.0:0");
    }

    #[test]
    fn timeouts_match_the_baseline() {
        assert_eq!(EXTERNAL_READ_TIMEOUT, Duration::from_secs(75));
        assert_eq!(EXTERNAL_WRITE_TIMEOUT, Duration::from_secs(600));
        assert_eq!(INTERNAL_READ_TIMEOUT, Duration::from_secs(75));
        assert_eq!(INTERNAL_WRITE_TIMEOUT, Duration::from_secs(3600));
    }
}
