//! Environment registry entries owned by `cmd/gitserver/shared`.

use std::path::PathBuf;

use sg_core::{ConfigError, EnvReader};

#[derive(Clone)]
pub struct GitserverConfig {
    pub repos_dir: PathBuf,
    pub listen_addr: String,
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

        Ok(Self {
            repos_dir: PathBuf::from(repos_dir),
            listen_addr: override_addr.unwrap_or(listen_addr),
        })
    }

    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.repos_dir.as_os_str().is_empty() {
            return Err(ConfigError::new("SRC_REPOS_DIR is required"));
        }
        Ok(())
    }
}
