#![forbid(unsafe_code)]

//! Port of `overrideSiteConfig` (`cmd/frontend/internal/cli/config.go:111-162`).
//!
//! Before the configuration server starts, the frontend applies the merged
//! `SITE_CONFIG_FILE` document to `critical_and_site_config` so the database is
//! the source of truth from the first read on (`serve_cmd.go:132-139` calls
//! this before `conf.InitConfigurationServerFrontendOnly`).
//!
//! The baseline also starts an fsnotify watcher (`watchUpdate`,
//! `config.go:366-441`) that re-runs the override whenever one of the files
//! changes. The workspace has no `notify` crate, so the watcher lives in
//! [`crate::configuration_watch`] as a structured polling loop (documented
//! deviation) that the frontend service spawns on its `JoinSet`.
//!
//! See `docs/overviews/m9_8ge_config_override_watch_overview.md`.

use std::path::PathBuf;

use sg_config::server::ConfigurationSource;

use crate::configuration_source::DbConfigurationSource;
use crate::site_config_file::{self, SiteConfigFileError};

/// `overrideSiteConfig`'s `updateFunc`: read the current DB configuration,
/// merge the files, and write the result with `isOverride = true` when it
/// differs. Returns whether a row was written.
pub async fn override_site_config(
    source: &DbConfigurationSource,
    paths: &[PathBuf],
) -> Result<bool, String> {
    if paths.is_empty() {
        return Ok(false);
    }

    // `updateFunc` reads first (seeding the deployment default), then merges.
    let mut raw = source.read().await?;

    let merged = site_config_file::read_site_config_file(paths)
        .map_err(|err| format!("reading SITE_CONFIG_FILE: {}", format_read_error(&err)))?;
    let new_site = String::from_utf8_lossy(&merged).into_owned();
    if raw.site == new_site {
        return Ok(false);
    }

    let last_id = raw.id;
    raw.site = new_site;
    // `actor.FromContext(ctx).UID` is 0 on the startup path, so the row's
    // author is stored as NULL (the seeding insert behaves the same way).
    source
        .write_with_override(raw, last_id, 0, true)
        .await
        .map_err(|err| format!("writing site config overrides to database: {err}"))?;
    Ok(true)
}

/// [`override_site_config`] with `SITE_CONFIG_FILE` read from the process
/// environment. An empty variable is a no-op, like `filepath.SplitList("")`.
pub async fn override_from_env(source: &DbConfigurationSource) -> Result<bool, String> {
    let value = std::env::var(site_config_file::SITE_CONFIG_FILE_VAR).unwrap_or_default();
    let paths = site_config_file::split_paths(&value);
    override_site_config(source, &paths).await
}

/// Format a read error like Go's `os.ReadFile`: `open <path>: <reason>`.
fn format_read_error(err: &SiteConfigFileError) -> String {
    match err {
        SiteConfigFileError::Read { path, source } => site_config_file::go_read_error(path, source),
        other => other.to_string(),
    }
}
