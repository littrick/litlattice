//! Environment registry entries owned by `cmd/gitserver/shared`.

use std::path::PathBuf;
use std::time::Duration;

use sg_core::{ConfigError, EnvReader};

/// `cmd/gitserver/shared/config.go` defaults.
pub const DEFAULT_SYNC_REPO_STATE_INTERVAL: Duration = Duration::from_secs(10 * 60);
pub const DEFAULT_SYNC_REPO_STATE_BATCH_SIZE: i64 = 500;
pub const DEFAULT_SYNC_REPO_STATE_UPSERT_PER_SEC: i64 = 500;
pub const DEFAULT_JANITOR_INTERVAL: Duration = Duration::from_secs(60);

#[derive(Clone)]
pub struct GitserverConfig {
    pub repos_dir: PathBuf,
    pub listen_addr: String,
    /// `GITSERVER_EXTERNAL_ADDR`: the `gitserver_repos.shard_id` and the name
    /// this instance is known by in `SRC_GIT_SERVERS`.
    pub external_address: String,
    /// `SRC_REPOS_SYNC_STATE_INTERVAL`.
    pub sync_repo_state_interval: Duration,
    /// `SRC_REPOS_SYNC_STATE_BATCH_SIZE`.
    pub sync_repo_state_batch_size: i64,
    /// `SRC_REPOS_SYNC_STATE_UPSERT_PER_SEC`.
    pub sync_repo_state_update_per_second: i64,
    /// `ENABLE_CUSTOM_GIT_FETCH`: the baseline only treats the literal
    /// `"false"` as disabled.
    pub enable_custom_git_fetch: bool,
    /// `SRC_REPOS_JANITOR_INTERVAL`: the interval between cleanup runs.
    pub janitor_interval: Duration,
    /// `SRC_REPOS_JANITOR_DISABLE_DELETE_REPOS_ON_WRONG_SHARD`.
    pub janitor_disable_delete_repos_on_wrong_shard: bool,
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

        // `env.GetInterval` / `env.GetInt`: malformed values fall back to
        // the registered defaults.
        let sync_repo_state_interval = crate::memobs::parse_go_duration(&env.get(
            "SRC_REPOS_SYNC_STATE_INTERVAL",
            "10m",
            "Interval between state syncs",
        ))
        .unwrap_or(DEFAULT_SYNC_REPO_STATE_INTERVAL);
        let sync_repo_state_batch_size = env
            .get(
                "SRC_REPOS_SYNC_STATE_BATCH_SIZE",
                "500",
                "Number of updates to perform per batch",
            )
            .parse()
            .unwrap_or(DEFAULT_SYNC_REPO_STATE_BATCH_SIZE);
        let sync_repo_state_update_per_second = env
            .get(
                "SRC_REPOS_SYNC_STATE_UPSERT_PER_SEC",
                "500",
                "The number of updated rows allowed per second across all gitserver instances",
            )
            .parse()
            .unwrap_or(DEFAULT_SYNC_REPO_STATE_UPSERT_PER_SEC);

        // `vcssyncer.enableCustomGitFetch`: `env.Get(..., "false", ...)` and
        // any value other than the literal `"false"` enables custom fetch.
        let enable_custom_git_fetch = env.get(
            "ENABLE_CUSTOM_GIT_FETCH",
            "false",
            "Enable custom git fetch",
        ) != "false";

        // `shared.Config.JanitorInterval` / `JanitorDisableDeleteReposOnWrongShard`.
        let janitor_interval = crate::memobs::parse_go_duration(&env.get(
            "SRC_REPOS_JANITOR_INTERVAL",
            "1m",
            "Interval between cleanup runs",
        ))
        .unwrap_or(DEFAULT_JANITOR_INTERVAL);
        let janitor_disable_delete_repos_on_wrong_shard = crate::memobs::parse_go_bool(&env.get(
            "SRC_REPOS_JANITOR_DISABLE_DELETE_REPOS_ON_WRONG_SHARD",
            "false",
            "Disable deleting repos on wrong shard",
        ))
        .unwrap_or(false);

        Ok(Self {
            repos_dir: PathBuf::from(repos_dir),
            listen_addr: override_addr.unwrap_or(listen_addr),
            external_address,
            sync_repo_state_interval,
            sync_repo_state_batch_size,
            sync_repo_state_update_per_second,
            enable_custom_git_fetch,
            janitor_interval,
            janitor_disable_delete_repos_on_wrong_shard,
        })
    }

    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.repos_dir.as_os_str().is_empty() {
            return Err(ConfigError::new("SRC_REPOS_DIR is required"));
        }
        Ok(())
    }
}
