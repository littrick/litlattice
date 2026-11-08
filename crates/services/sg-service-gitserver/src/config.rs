//! Environment registry entries owned by `cmd/gitserver/shared`.

use std::path::PathBuf;

use sg_core::{ConfigError, EnvReader};

#[derive(Clone)]
pub struct GitserverConfig {
    pub repos_dir: PathBuf,
    pub listen_addr: String,
    /// `GITSERVER_EXTERNAL_ADDR`: the `gitserver_repos.shard_id` and the name
    /// this instance is known by in `SRC_GIT_SERVERS`.
    pub external_address: String,
}

/// Port of `internal/hostname.Get`: `HOSTNAME` when set (the env registry
/// entry is registered by the caller), otherwise the OS hostname.
fn os_hostname() -> String {
    std::fs::read_to_string("/proc/sys/kernel/hostname")
        .map(|value| value.trim_end_matches('\n').to_owned())
        .unwrap_or_default()
}

impl GitserverConfig {
    pub fn load(env: &mut EnvReader, override_addr: Option<String>) -> Result<Self, ConfigError> {
        let repos_dir = env.get("SRC_REPOS_DIR", "/data/repos", "Root dir containing repos.");
        if repos_dir.is_empty() {
            return Err(ConfigError::new("SRC_REPOS_DIR is required"));
        }

        let insecure_dev = env
            .get(
                "INSECURE_DEV",
                "false",
                "Running in insecure dev (local laptop) mode",
            )
            .eq_ignore_ascii_case("true");

        // `env.GetOptional`: empty means "not set", then fall back to
        // loopback in insecure dev and all interfaces otherwise.
        let listen_addr = env.get(
            "GITSERVER_ADDR",
            "",
            "The address under which the gitserver API listens. Can include a port.",
        );
        let listen_addr = if listen_addr.is_empty() {
            if insecure_dev {
                "127.0.0.1:3178".to_owned()
            } else {
                ":3178".to_owned()
            }
        } else {
            listen_addr
        };

        let hostname_override = env.get("HOSTNAME", "", "Hostname override");
        let hostname = if hostname_override.is_empty() {
            os_hostname()
        } else {
            hostname_override
        };
        let external_address = env.get(
            "GITSERVER_EXTERNAL_ADDR",
            &hostname,
            "The name of this gitserver as it would appear in SRC_GIT_SERVERS.",
        );

        Ok(Self {
            repos_dir: PathBuf::from(repos_dir),
            listen_addr: override_addr.unwrap_or(listen_addr),
            external_address,
        })
    }

    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.repos_dir.as_os_str().is_empty() {
            return Err(ConfigError::new("SRC_REPOS_DIR is required"));
        }
        Ok(())
    }
}
