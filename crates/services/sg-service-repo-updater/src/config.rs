#![forbid(unsafe_code)]

//! Environment registry entries owned by `cmd/repo-updater/shared`.

use sg_core::{ConfigError, EnvReader};

/// `cmd/repo-updater/shared`: `const port = "3182"`.
pub const DEFAULT_PORT: &str = "3182";

#[derive(Debug, Clone)]
pub struct RepoUpdaterConfig {
    /// `net.JoinHostPort(host, port)` with `host = 127.0.0.1` in insecure dev.
    pub listen_addr: String,
    /// `SRC_PROF_HTTP`; `None` disables the debug server.
    pub prof_addr: Option<String>,
    /// `DISABLE_REPO_PURGE`; the baseline only treats a parseable `true` as
    /// disabling the purger.
    pub disable_repo_purge: bool,
    /// `DISABLE_BATCH_CHANGES` (`batches.IsEnabled()`): Batch Changes is
    /// disabled only when `strconv.ParseBool` accepts the value as true. Read
    /// with `os.Getenv` semantics, so `<NAME>_<process>` overrides do not
    /// apply.
    pub disable_batch_changes: bool,
}

impl RepoUpdaterConfig {
    pub fn load(env: &mut EnvReader, override_addr: Option<String>) -> Result<Self, ConfigError> {
        let insecure_dev = env
            .get(
                "INSECURE_DEV",
                "false",
                "Running in insecure dev (local laptop) mode",
            )
            .eq_ignore_ascii_case("true");

        let listen_addr = match override_addr {
            Some(addr) if !addr.is_empty() => addr,
            _ => {
                if insecure_dev {
                    format!("127.0.0.1:{DEFAULT_PORT}")
                } else {
                    format!(":{DEFAULT_PORT}")
                }
            }
        };

        let prof_addr = env.get(
            "SRC_PROF_HTTP",
            crate::debug::DEFAULT_ADDR,
            "net/http/pprof http bind address.",
        );
        let prof_addr = Some(prof_addr).filter(|addr| !addr.is_empty());

        // The baseline `strconv.ParseBool(os.Getenv("DISABLE_REPO_PURGE"))`
        // ignores parse errors (the purger stays enabled).
        let disable_repo_purge = env
            .get(
                "DISABLE_REPO_PURGE",
                "false",
                "Disable the repository purger (temporary escape hatch).",
            )
            .eq_ignore_ascii_case("true");

        // `batches.IsEnabled()`: `os.Getenv` + `strconv.ParseBool`, ignoring
        // parse errors.
        let disable_batch_changes = std::env::var("DISABLE_BATCH_CHANGES")
            .ok()
            .and_then(|raw| parse_strconv_bool(&raw))
            .unwrap_or(false);

        Ok(Self {
            listen_addr,
            prof_addr,
            disable_repo_purge,
            disable_batch_changes,
        })
    }

    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.listen_addr.is_empty() {
            return Err(ConfigError::new(
                "repo-updater listen address must not be empty",
            ));
        }
        Ok(())
    }
}

/// `strconv.ParseBool` accepted spellings, ignoring parse errors like
/// `batches.IsEnabled`.
fn parse_strconv_bool(value: &str) -> Option<bool> {
    match value {
        "1" | "t" | "T" | "TRUE" | "true" | "True" => Some(true),
        "0" | "f" | "F" | "FALSE" | "false" | "False" => Some(false),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_to_all_interfaces() {
        let _guard = ENV_LOCK.lock().expect("env lock");
        // The process running the tests may have INSECURE_DEV set.
        std::env::remove_var("INSECURE_DEV");
        let mut env = EnvReader::new();
        let config = RepoUpdaterConfig::load(&mut env, None).expect("load");
        assert_eq!(config.listen_addr, ":3182");
        assert!(!config.disable_repo_purge);
    }

    #[test]
    fn insecure_dev_binds_loopback() {
        let _guard = ENV_LOCK.lock().expect("env lock");
        std::env::set_var("INSECURE_DEV", "true");
        let mut env = EnvReader::new();
        let config = RepoUpdaterConfig::load(&mut env, None).expect("load");
        std::env::remove_var("INSECURE_DEV");
        assert_eq!(config.listen_addr, "127.0.0.1:3182");
    }

    #[test]
    fn rollout_override_wins() {
        let _guard = ENV_LOCK.lock().expect("env lock");
        let mut env = EnvReader::new();
        let config =
            RepoUpdaterConfig::load(&mut env, Some("127.0.0.1:13182".to_owned())).expect("load");
        assert_eq!(config.listen_addr, "127.0.0.1:13182");
    }

    static ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());
}
