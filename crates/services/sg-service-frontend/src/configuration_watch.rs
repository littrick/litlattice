#![forbid(unsafe_code)]

//! `overrideGlobalSettings` (`cmd/frontend/internal/cli/config.go:164-207`) and
//! the file watchers `watchUpdate`/`watchPaths` (`config.go:366-441`).
//!
//! The baseline uses `fsnotify`; the workspace has no `notify` crate, so the
//! watcher is a structured polling loop (parent-approved for M9.8ge):
//!
//! * [`ConfigOverrideWatcher::new`] snapshots `(mtime, len)` for every watched
//!   path, exactly once, before the first tick (equivalent to `watcher.Add`);
//! * [`ConfigOverrideWatcher::run`] re-stats every [`DEFAULT_WATCH_INTERVAL`]
//!   and re-runs the matching update function when a stamp changes. A deleted
//!   file is a change (the update then fails reading it, like the baseline's
//!   remove event); an unchanged stamp is skipped without calling the update.
//!
//! The update functions themselves are the baseline's `updateFunc`:
//! [`crate::configuration_startup::override_site_config`] for
//! `SITE_CONFIG_FILE`, [`override_global_settings`] for `GLOBAL_SETTINGS_FILE`.
//! Log messages and the `src_frontend_config_file_watcher_updates{status}`
//! counter match `config.go`.
//!
//! See `docs/overviews/m9_8ge_config_override_watch_overview.md`.

use std::collections::HashMap;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::OnceLock;
use std::time::{Duration, SystemTime};

use sg_db::settings::{create_if_up_to_date, latest_for_site, SettingsSubjectKey};
use sg_metrics::prometheus::{IntCounterVec, Opts};
use sg_settings_schema::validate_settings;
use sqlx::PgPool;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

use crate::configuration_source::DbConfigurationSource;
use crate::configuration_startup::override_site_config;
use crate::site_config_file::{go_read_error, split_paths, SITE_CONFIG_FILE_VAR};

/// `GLOBAL_SETTINGS_FILE` (not registered with `env.Get` in the baseline).
pub const GLOBAL_SETTINGS_FILE_VAR: &str = "GLOBAL_SETTINGS_FILE";

/// Stand-in for fsnotify's kernel events. The task brief allows 250ms-1s; a
/// shorter interval means quicker propagation at a small stat cost.
pub const DEFAULT_WATCH_INTERVAL: Duration = Duration::from_millis(500);

/// The paths the startup overrides use, resolved from the process environment.
#[derive(Debug, Clone, Default)]
pub struct ConfigWatchPaths {
    /// `SITE_CONFIG_FILE`: `filepath.SplitList` semantics.
    pub site_config_files: Vec<PathBuf>,
    /// `GLOBAL_SETTINGS_FILE`: a single path (the baseline does not split it).
    pub global_settings_file: Option<PathBuf>,
}

impl ConfigWatchPaths {
    pub fn from_env() -> Self {
        Self {
            site_config_files: site_config_paths_from_env(),
            global_settings_file: global_settings_path_from_env(),
        }
    }

    /// Nothing to watch: both overrides are disabled.
    pub fn is_empty(&self) -> bool {
        self.site_config_files.is_empty() && self.global_settings_file.is_none()
    }
}

/// `filepath.SplitList(os.Getenv("SITE_CONFIG_FILE"))`.
pub fn site_config_paths_from_env() -> Vec<PathBuf> {
    split_paths(&std::env::var(SITE_CONFIG_FILE_VAR).unwrap_or_default())
}

/// `os.Getenv("GLOBAL_SETTINGS_FILE")`; empty means disabled.
pub fn global_settings_path_from_env() -> Option<PathBuf> {
    let value = std::env::var(GLOBAL_SETTINGS_FILE_VAR).unwrap_or_default();
    if value.is_empty() {
        None
    } else {
        Some(PathBuf::from(value))
    }
}

/// `overrideGlobalSettings`'s `update` function.
///
/// The baseline's condition is `currentSettings == nil ||
/// currentSettings.AuthorUserID != nil || currentSettings.Contents !=
/// globalSettings`: a human-authored row is always superseded by an anonymous
/// override row, an up-to-date anonymous row is left alone.
pub async fn override_global_settings(pool: &PgPool, path: &Path) -> Result<bool, String> {
    let bytes = fs::read(path).map_err(|err| {
        format!(
            "reading GLOBAL_SETTINGS_FILE: {}",
            go_read_error(path, &err)
        )
    })?;
    let contents = String::from_utf8_lossy(&bytes).into_owned();

    let current = latest_for_site(pool)
        .await
        .map_err(|err| format!("could not fetch current settings: {err}"))?;
    let needs_write = match &current {
        None => true,
        Some(row) => row.author_user_id.is_some() || row.contents != contents,
    };
    if !needs_write {
        return Ok(false);
    }
    let last_id = current.as_ref().map(|row| row.id);

    // `settingsStore.CreateIfUpToDate` validates before opening its
    // transaction (`internal/database/settings.go:48-52`).
    let problems = validate_settings(&contents);
    if !problems.is_empty() {
        return Err(format!(
            "writing global setting override to database: invalid settings: {}",
            problems.join(",")
        ));
    }

    create_if_up_to_date(pool, SettingsSubjectKey::Site, last_id, None, &contents)
        .await
        .map_err(|err| format!("writing global setting override to database: {err}"))?;
    Ok(true)
}

/// [`override_global_settings`] with `GLOBAL_SETTINGS_FILE` from the process
/// environment. An empty variable is a no-op.
pub async fn override_global_settings_from_env(pool: &PgPool) -> Result<bool, String> {
    match global_settings_path_from_env() {
        Some(path) => override_global_settings(pool, &path).await,
        None => Ok(false),
    }
}

/// `src_frontend_config_file_watcher_updates` (`config.go:58-61`).
fn config_override_updates() -> &'static IntCounterVec {
    static METRIC: OnceLock<IntCounterVec> = OnceLock::new();
    METRIC.get_or_init(|| {
        let metric = IntCounterVec::new(
            Opts::new(
                "src_frontend_config_file_watcher_updates",
                "Incremented each time the config file is updated.",
            ),
            &["status"],
        )
        .expect("valid config override watcher metric");
        let _ = sg_metrics::registry().register(Box::new(metric.clone()));
        metric
    })
}

/// What the watcher remembers about one path. `None` means "did not exist".
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct FileStamp {
    modified: Option<SystemTime>,
    len: u64,
}

pub(crate) fn file_stamp(path: &Path) -> io::Result<Option<FileStamp>> {
    match fs::metadata(path) {
        Ok(meta) => Ok(Some(FileStamp {
            modified: meta.modified().ok(),
            len: meta.len(),
        })),
        // A vanished file is an event (the baseline's remove event): the next
        // update reports the read error.
        Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(err) => Err(err),
    }
}

/// The polling replacement for `watchUpdate` + `watchPaths`.
pub struct ConfigOverrideWatcher {
    source: DbConfigurationSource,
    pool: PgPool,
    site_config_files: Vec<PathBuf>,
    global_settings_file: Option<PathBuf>,
    interval: Duration,
    stamps: HashMap<PathBuf, Option<FileStamp>>,
}

impl ConfigOverrideWatcher {
    /// Snapshot the watched files. Fails like `watcher.Add` when a path cannot
    /// be stat'ed; the caller logs `failed to watch config override files`.
    ///
    /// Empty paths are ignored, as in `watchPaths` (the baseline's
    /// `readSiteConfigFile` would have failed on them before this point).
    pub fn new(
        source: DbConfigurationSource,
        pool: PgPool,
        paths: ConfigWatchPaths,
        interval: Duration,
    ) -> Result<Self, String> {
        let mut stamps = HashMap::new();
        for path in paths
            .site_config_files
            .iter()
            .chain(paths.global_settings_file.iter())
        {
            if path.as_os_str().is_empty() {
                continue;
            }
            let stamp = file_stamp(path)
                .map_err(|err| format!("failed to add {} to watcher: {err}", path.display()))?;
            stamps.insert(path.clone(), stamp);
        }

        Ok(Self {
            source,
            pool,
            site_config_files: paths.site_config_files,
            global_settings_file: paths.global_settings_file,
            interval,
            stamps,
        })
    }

    /// `watchUpdate`'s event loop. Returns when `shutdown` is cancelled.
    pub async fn run(mut self, shutdown: CancellationToken) {
        let mut ticker = tokio::time::interval(self.interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        // `interval`'s first tick is immediate; `new` already took the initial
        // snapshot, so consume it before polling.
        ticker.tick().await;

        loop {
            tokio::select! {
                _ = shutdown.cancelled() => return,
                _ = ticker.tick() => {}
            }

            let mut site_changed = false;
            // Copy the path list so `self.poll` can borrow `self` mutably.
            let site_paths = self.site_config_files.clone();
            for path in &site_paths {
                if path.as_os_str().is_empty() {
                    continue;
                }
                match self.poll(path) {
                    Ok(true) => site_changed = true,
                    Ok(false) => {}
                    Err(err) => record_watch_error(path, &err),
                }
            }
            if site_changed {
                let result = override_site_config(&self.source, &self.site_config_files).await;
                record_update_result(&self.site_config_files, result);
            }

            if let Some(path) = self.global_settings_file.clone() {
                if path.as_os_str().is_empty() {
                    continue;
                }
                match self.poll(&path) {
                    Ok(true) => {
                        let result = override_global_settings(&self.pool, &path).await;
                        record_update_result(std::slice::from_ref(&path), result);
                    }
                    Ok(false) => {}
                    Err(err) => record_watch_error(&path, &err),
                }
            }
        }
    }

    /// Re-stat one path and report whether `(mtime, len)` changed.
    fn poll(&mut self, path: &Path) -> io::Result<bool> {
        let current = file_stamp(path)?;
        let changed = self.stamps.get(path).copied() != Some(current);
        self.stamps.insert(path.to_path_buf(), current);
        Ok(changed)
    }
}

/// Spawn the watcher on the service's `JoinSet` (no bare `tokio::spawn`).
pub fn spawn_config_watchers(
    tasks: &mut JoinSet<()>,
    source: DbConfigurationSource,
    pool: PgPool,
    paths: ConfigWatchPaths,
    interval: Duration,
    shutdown: CancellationToken,
) {
    // The baseline starts one `go watchUpdate` per override, so a failure to
    // watch one file set does not stop the other.
    if !paths.site_config_files.is_empty() {
        spawn_one(
            tasks,
            source.clone(),
            pool.clone(),
            ConfigWatchPaths {
                site_config_files: paths.site_config_files,
                global_settings_file: None,
            },
            interval,
            shutdown.clone(),
        );
    }
    if paths.global_settings_file.is_some() {
        spawn_one(
            tasks,
            source,
            pool,
            ConfigWatchPaths {
                site_config_files: Vec::new(),
                global_settings_file: paths.global_settings_file,
            },
            interval,
            shutdown,
        );
    }
}

fn spawn_one(
    tasks: &mut JoinSet<()>,
    source: DbConfigurationSource,
    pool: PgPool,
    paths: ConfigWatchPaths,
    interval: Duration,
    shutdown: CancellationToken,
) {
    match ConfigOverrideWatcher::new(source, pool, paths, interval) {
        Ok(watcher) => {
            tasks.spawn(watcher.run(shutdown));
        }
        // `watchUpdate`: log and give up; startup is not failed.
        Err(err) => tracing::error!(error = %err, "failed to watch config override files"),
    }
}

fn record_watch_error(path: &Path, err: &io::Error) {
    tracing::warn!(
        file = %path.display(),
        error = %err,
        "error while watching config override files"
    );
    config_override_updates()
        .with_label_values(&["watch_failed"])
        .inc();
}

pub(crate) fn record_update_result(files: &[PathBuf], result: Result<bool, String>) {
    match result {
        Ok(true) => {
            tracing::info!(
                files = ?files,
                "updated configuration from modified config override files"
            );
            config_override_updates()
                .with_label_values(&["success"])
                .inc();
        }
        Ok(false) => {
            tracing::info!(
                files = ?files,
                "skipped updating configuration as it is already up to date"
            );
            config_override_updates()
                .with_label_values(&["skipped"])
                .inc();
        }
        Err(err) => {
            tracing::error!(
                files = ?files,
                error = %err,
                "failed to update configuration from modified config override file"
            );
            config_override_updates()
                .with_label_values(&["update_failed"])
                .inc();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_dir(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("sg_m98ge_{name}_{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).expect("create temp dir");
        dir
    }

    #[test]
    fn missing_file_stamp_is_none_and_present_is_some() {
        let dir = temp_dir("stamp");
        let path = dir.join("missing.json");
        assert_eq!(file_stamp(&path).expect("stat missing"), None);

        fs::write(&path, "{}").expect("write");
        assert!(file_stamp(&path).expect("stat present").is_some());
    }

    #[test]
    fn rewrite_changes_the_stamp() {
        let dir = temp_dir("rewrite");
        let path = dir.join("site.json");
        fs::write(&path, "{}").expect("write");
        let first = file_stamp(&path).expect("stat").expect("present");
        // Different length guarantees a different stamp even on coarse clocks.
        fs::write(&path, "{\"externalURL\":\"https://example.com\"}").expect("rewrite");
        let second = file_stamp(&path).expect("stat").expect("present");
        assert_ne!(first, second);
    }

    #[test]
    fn config_watch_paths_are_empty_without_files() {
        assert!(ConfigWatchPaths::default().is_empty());
        assert!(!ConfigWatchPaths {
            site_config_files: vec![PathBuf::from("/tmp/a.json")],
            global_settings_file: None,
        }
        .is_empty());
        assert!(!ConfigWatchPaths {
            site_config_files: Vec::new(),
            global_settings_file: Some(PathBuf::from("/tmp/settings.json")),
        }
        .is_empty());
    }

    #[test]
    fn metric_matches_the_baseline_name() {
        config_override_updates()
            .with_label_values(&["success"])
            .inc();
        let text = sg_metrics::render_default();
        assert!(
            text.contains("src_frontend_config_file_watcher_updates"),
            "{text}"
        );
        assert!(text.contains("status=\"success\""), "{text}");
    }
}
