#![forbid(unsafe_code)]

//! `cmd/frontend/internal/cli/serve_cmd.go` process skeleton: the external and
//! internal listeners plus the debug server.
//!
//! The configuration path is ported (M9.8el): `overrideSiteConfig`, the DB
//! configuration service and a live, generation-cached middleware config.
//! M9.16c adds the `serve_cmd.go` startup gate: the `versions`-table service
//! version upsert, the 5s Redis readiness wait, and the out-of-band migration
//! metadata sync + validation (`SRC_DISABLE_OOBMIGRATION_VALIDATION` skips
//! it). `tryAutoUpgrade`'s no-op gate is ported (`try_auto_upgrade`); the
//! multiversion upgrade machinery behind it is not. Enterprise hooks remain
//! out of scope.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use sg_core::{ConfigError, EnvReader, Service, ServiceContext, ServiceError};
use tokio::task::JoinSet;

use sg_config::SiteConfiguration;

use crate::config::FrontendConfig;
use crate::configuration_server;
use crate::graphql_api::{GraphqlDeps, SchemaGraphqlDeps};
use crate::ipallowlist::AllowlistEnv;
use crate::version::Version;
use crate::{debug, middleware};

/// `cmd/frontend` service name.
pub const SERVICE_NAME: &str = "frontend";

pub fn service_name() -> &'static str {
    SERVICE_NAME
}

/// `searchcontexts.IsEnabled` (`internal/search/searchcontexts/conf.go`):
/// `DISABLE_SEARCH_CONTEXTS` is parsed with Go's `strconv.ParseBool`; an
/// unparsable value leaves the feature enabled.
fn search_contexts_enabled() -> bool {
    match std::env::var("DISABLE_SEARCH_CONTEXTS") {
        Ok(value) => !matches!(value.as_str(), "1" | "t" | "T" | "TRUE" | "true" | "True"),
        Err(_) => true,
    }
}

/// The configuration-level half of `cody.IsCodyEnabled`
/// (`conf.CodyEnabled()`): the per-actor authentication and verified-email
/// checks run in the prefetcher, and the license/RBAC/feature-flag branches
/// stay explicit seams.
fn cody_config_disabled_reason(live: &sg_config::LiveConfig) -> Option<String> {
    let current = live.current();
    let site = current.site.raw();
    let enabled = site
        .get("cody")
        .and_then(|cody| cody.get("enabled"))
        .and_then(serde_json::Value::as_bool)
        .or_else(|| {
            site.get("completions")
                .and_then(|completions| completions.get("enabled"))
                .and_then(serde_json::Value::as_bool)
        })
        .unwrap_or(false);
    if enabled {
        None
    } else {
        Some("cody is disabled".to_owned())
    }
}

// ---------------------------------------------------------------------------
// Startup sequence (`cmd/frontend/internal/cli/serve_cmd.go:74-131`).
//
// `tryAutoUpgrade` (serve_cmd.go:99) runs first; its no-op gate is ported and
// a pending upgrade is reported as not ported (the upgrade binary and
// `upgrade_logs` machinery live outside this repository's frontend process).
// Everything after it is: `InitDB`'s service-version upsert, `waitForRedis`,
// and the out-of-band migration metadata sync + validation.
// ---------------------------------------------------------------------------

/// `version.devVersion` (`internal/version/version.go:13`).
pub const DEV_VERSION: &str = "0.0.0+dev";

/// `oobmigration.Version`'s comparable half (`internal/oobmigration/version.go`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct OobVersion {
    pub major: i32,
    pub minor: i32,
}

/// `oobmigration.VersionOrder`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum VersionOrder {
    Before,
    Equal,
    After,
}

/// `oobmigration.NewVersionFromString`
/// (`internal/oobmigration/version.go:44-96`): the same `^v?(\d+)\.(\d+)
/// (?:\.(\d+))?(?:-\w*)?(?:\+[\w.]*)?$` shape, including the `+dev` suffix.
pub fn new_version_from_string(value: &str) -> Option<OobVersion> {
    new_version_and_patch_from_string(value).map(|(version, _)| version)
}

/// `oobmigration.NewVersionAndPatchFromString` (`version.go:56-79`): the
/// major/minor plus the optional patch (`0` when the string omits it).
pub fn new_version_and_patch_from_string(value: &str) -> Option<(OobVersion, i64)> {
    let mut input = value;
    if let Some(stripped) = input.strip_suffix("+dev") {
        input = stripped;
    }
    let bytes = input.as_bytes();
    let mut index = 0;
    if bytes.get(index) == Some(&b'v') {
        index += 1;
    }
    let major = take_digits(bytes, &mut index)?;
    if bytes.get(index) != Some(&b'.') {
        return None;
    }
    index += 1;
    let minor = take_digits(bytes, &mut index)?;
    let mut patch = 0;
    if bytes.get(index) == Some(&b'.') {
        index += 1;
        patch = take_digits(bytes, &mut index)?.parse().ok()?;
    }
    if bytes.get(index) == Some(&b'-') {
        index += 1;
        while index < bytes.len() && (bytes[index].is_ascii_alphanumeric() || bytes[index] == b'_')
        {
            index += 1;
        }
    }
    if bytes.get(index) == Some(&b'+') {
        index += 1;
        while index < bytes.len()
            && (bytes[index].is_ascii_alphanumeric()
                || bytes[index] == b'_'
                || bytes[index] == b'.')
        {
            index += 1;
        }
    }
    if index != bytes.len() {
        return None;
    }
    Some((
        OobVersion {
            major: major.parse().ok()?,
            minor: minor.parse().ok()?,
        },
        patch,
    ))
}

fn take_digits<'a>(bytes: &'a [u8], index: &mut usize) -> Option<&'a str> {
    let start = *index;
    while *index < bytes.len() && bytes[*index].is_ascii_digit() {
        *index += 1;
    }
    if *index == start {
        None
    } else {
        Some(std::str::from_utf8(&bytes[start..*index]).ok()?)
    }
}

/// `oobmigration.CompareVersions` (`version.go:134-148`).
pub fn compare_versions(a: OobVersion, b: OobVersion) -> VersionOrder {
    for (left, right) in [(a.major, b.major), (a.minor, b.minor)] {
        if left < right {
            return VersionOrder::Before;
        }
        if left > right {
            return VersionOrder::After;
        }
    }
    VersionOrder::Equal
}

/// `version.IsDev` (`internal/version/version.go:48-50`).
pub fn is_dev_version(raw: &str) -> bool {
    raw == DEV_VERSION
}

/// `upgradestore.IsValidUpgrade` (`internal/version/upgradestore/upgrade.go:11-25`).
/// A version string that does not parse is the baseline's `nil` semver, which
/// always permits the upgrade.
pub fn is_valid_upgrade(previous: Option<&str>, latest: &str) -> bool {
    let (Some((prev_major, prev_minor)), Some((latest_major, latest_minor))) = (
        previous.and_then(semver_major_minor),
        semver_major_minor(latest),
    ) else {
        return true;
    };
    match prev_major.cmp(&latest_major) {
        std::cmp::Ordering::Greater => true,
        std::cmp::Ordering::Equal => {
            prev_minor >= latest_minor || prev_minor + 1 == latest_minor
        }
        std::cmp::Ordering::Less if prev_major + 1 == latest_major => latest_minor == 0,
        std::cmp::Ordering::Less => false,
    }
}

/// `semver.NewVersion`'s major/minor, `None` when the string is not
/// semver-shaped (`IsValidUpgrade` then treats it as the nil version).
fn semver_major_minor(raw: &str) -> Option<(u64, u64)> {
    let trimmed = raw.trim();
    let trimmed = trimmed.strip_prefix('v').unwrap_or(trimmed);
    let mut parts = trimmed.split(['.', '-', '+']);
    Some((parts.next()?.parse().ok()?, parts.next()?.parse().ok()?))
}

fn is_missing_relation(err: &sqlx::Error) -> bool {
    matches!(err, sqlx::Error::Database(db) if db.code().as_deref() == Some("42P01"))
}

fn postgres_error_code(err: &sqlx::Error) -> Option<String> {
    match err {
        sqlx::Error::Database(db) => db.code().map(|code| code.into_owned()),
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// `tryAutoUpgrade` (`cmd/frontend/internal/cli/autoupgrade.go:38-151`).
//
// The no-op gate (fresh instance, `auto_upgrade=false` and no
// `SRC_AUTOUPGRADE`, or already current) is ported; the multiversion upgrade
// machinery behind the gate is not (the upgrade binary and `upgrade_logs`
// bookkeeping live outside this repository's frontend process). Reaching the
// gate with an actual upgrade pending is reported as an explicit startup error
// instead of silently skipping.
// ---------------------------------------------------------------------------

/// `appName` in `autoupgrade.go:34`.
pub const AUTO_UPGRADER_APP_NAME: &str = "frontend-autoupgrader";
/// `multiversion.EnvShouldAutoUpgrade` (`autoupgrade.go:5-7`).
pub const AUTO_UPGRADE_ENV: &str = "SRC_AUTOUPGRADE";

/// Why [`auto_upgrade_plan`] is a no-op.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AutoUpgradeSkip {
    /// `sql.ErrNoRows` / `UndefinedTable`: a fresh instance.
    FreshInstance,
    /// `!dbShouldAutoUpgrade && !EnvShouldAutoUpgrade`.
    Disabled,
    /// Same major/minor and `currentPatch >= toPatch`.
    SameVersionOrNewer,
    /// `version.Version()` does not parse: the baseline logs a warning and
    /// returns nil.
    UnexpectedTargetVersion,
}

/// `tryAutoUpgrade`'s decision after `GetAutoUpgrade`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AutoUpgradePlan {
    Skip(AutoUpgradeSkip),
    /// The gate says an upgrade is required; executing it is not ported.
    Required {
        current: String,
        current_version: OobVersion,
        current_patch: i64,
        target: String,
        target_version: OobVersion,
        target_patch: i64,
    },
}

/// `upgradestore.GetAutoUpgrade` (`store.go:155-175`).
///
/// Returns `Ok(None)` for the fresh-instance cases (`sql.ErrNoRows` and
/// `UndefinedTable`). `UndefinedColumn` (a pre-5.1 `versions` table without
/// `auto_upgrade`) falls back to the version-only query and reports
/// `auto_upgrade=false`, like the zero value in the baseline.
pub async fn fetch_auto_upgrade(
    pool: &sqlx::PgPool,
) -> Result<Option<(String, bool)>, String> {
    let primary = sqlx::query_as::<_, (String, bool)>(
        "SELECT version, auto_upgrade FROM versions WHERE service = 'frontend'",
    )
    .fetch_optional(pool)
    .await;

    match primary {
        Ok(row) => Ok(row),
        Err(err) if postgres_error_code(&err).as_deref() == Some("42P01") => Ok(None),
        Err(err) if postgres_error_code(&err).as_deref() == Some("42703") => {
            let version = sqlx::query_scalar::<_, String>(
                "SELECT version FROM versions WHERE service = 'frontend'",
            )
            .fetch_optional(pool)
            .await
            .map_err(|err| format!("failed to get frontend version from fallback: {err}"))?;
            Ok(version.map(|version| (version, false)))
        }
        Err(err) => Err(format!(
            "failed to get frontend version and auto_upgrade state: {err}"
        )),
    }
}

/// The pure `tryAutoUpgrade` gate (`autoupgrade.go:55-75`).
///
/// `state` is `GetAutoUpgrade`'s result; `Err` is the baseline's error for an
/// unparseable *current* version string. An unparseable target version is the
/// baseline's warn-and-skip path.
pub fn auto_upgrade_plan(
    state: Option<(String, bool)>,
    target_version: &str,
    env_should_auto_upgrade: bool,
) -> Result<AutoUpgradePlan, String> {
    let Some((current, db_should_auto_upgrade)) = state else {
        return Ok(AutoUpgradePlan::Skip(AutoUpgradeSkip::FreshInstance));
    };
    if !db_should_auto_upgrade && !env_should_auto_upgrade {
        return Ok(AutoUpgradePlan::Skip(AutoUpgradeSkip::Disabled));
    }

    let Some((current_version, current_patch)) =
        new_version_and_patch_from_string(&current)
    else {
        return Err(format!(
            "unexpected string for desired instance schema version, skipping auto-upgrade ({current})"
        ));
    };
    let Some((target_oob, target_patch)) =
        new_version_and_patch_from_string(target_version)
    else {
        return Ok(AutoUpgradePlan::Skip(
            AutoUpgradeSkip::UnexpectedTargetVersion,
        ));
    };

    if compare_versions(current_version, target_oob) == VersionOrder::Equal
        && current_patch >= target_patch
    {
        return Ok(AutoUpgradePlan::Skip(AutoUpgradeSkip::SameVersionOrNewer));
    }

    Ok(AutoUpgradePlan::Required {
        current,
        current_version,
        current_patch,
        target: target_version.to_owned(),
        target_version: target_oob,
        target_patch,
    })
}

/// `tryAutoUpgrade` (`autoupgrade.go:38-151`) up to the gate. Returns the
/// baseline error strings; the caller prefixes `frontend.tryAutoUpgrade`.
pub async fn try_auto_upgrade(
    pool: &sqlx::PgPool,
    target_version: &str,
) -> Result<AutoUpgradePlan, String> {
    let state = fetch_auto_upgrade(pool).await?;
    let env_should_auto_upgrade = {
        let mut env = EnvReader::new();
        env.get_bool(
            AUTO_UPGRADE_ENV,
            false,
            "If you forgot to set intent to autoupgrade before shutting down the instance, or you're upgrading from pre-5.1, set this env var.",
        )
    };
    auto_upgrade_plan(state, target_version, env_should_auto_upgrade)
}

/// `upgradestore.GetFirstServiceVersion` (`store.go:44-49`).
pub async fn get_first_service_version(pool: &sqlx::PgPool) -> Result<Option<String>, String> {
    let result =
        sqlx::query_scalar::<_, String>("SELECT first_version FROM versions WHERE service = $1")
            .bind("frontend")
            .fetch_optional(pool)
            .await;
    match result {
        Ok(value) => Ok(value),
        Err(err) if is_missing_relation(&err) => Ok(None),
        Err(err) => Err(err.to_string()),
    }
}

/// `upgradestore.UpdateServiceVersion` (`store.go:86-118`): the upgrade-policy
/// gate plus the upsert that stamps `updated_at` on success. The `ON CONFLICT`
/// clause keeps the previous-version compare-and-swap of the baseline.
pub async fn update_service_version(pool: &sqlx::PgPool, version: &str) -> Result<(), String> {
    let previous =
        sqlx::query_scalar::<_, String>("SELECT version FROM versions WHERE service = $1")
            .bind("frontend")
            .fetch_optional(pool)
            .await
            .map_err(|err| err.to_string())?;

    if !is_valid_upgrade(previous.as_deref(), version) {
        return Err(format!(
            "upgrading \"frontend\" from {:?} to {:?} is not allowed",
            previous.as_deref().unwrap_or(""),
            version
        ));
    }

    sqlx::query(
        "INSERT INTO versions (service, version, updated_at) VALUES ($1, $2, $3) \
         ON CONFLICT (service) DO UPDATE SET (version, updated_at) = \
         (excluded.version, excluded.updated_at) WHERE versions.version = $4",
    )
    .bind("frontend")
    .bind(version)
    .bind(time::OffsetDateTime::now_utc())
    .bind(previous)
    .execute(pool)
    .await
    .map_err(|err| err.to_string())?;

    Ok(())
}

/// `Runner.SynchronizeMetadata` (`runner.go:75-77`).
pub async fn synchronize_out_of_band_migrations(pool: &sqlx::PgPool) -> Result<(), String> {
    sg_db::out_of_band_migrations::synchronize_metadata(pool).await
}

/// `migrationStatusError.Error` (`runner.go:113-115`).
fn migration_status_error(id: i32, expected: f64, actual: f64) -> String {
    format!(
        "migration {id} expected to be at {:.2}% (at {:.2}%)",
        expected * 100.0,
        actual * 100.0
    )
}

fn wrap_migration_errors(errors: &[String]) -> Result<(), String> {
    if errors.is_empty() {
        return Ok(());
    }
    let mut descriptions: Vec<String> = errors
        .iter()
        .map(|error| format!("  - {error}\n"))
        .collect();
    descriptions.sort();
    Err(format!(
        "Unfinished migrations. Please revert Sourcegraph to the previous version and wait for the following migrations to complete.\n\n{}\n",
        descriptions.join("\n")
    ))
}

/// One `oobmigration.Migration`'s validation-relevant fields.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct OobMigration {
    pub id: i32,
    pub introduced: OobVersion,
    pub deprecated: Option<OobVersion>,
    pub progress: f64,
}

/// `Runner.Validate` (`runner.go:128-160`), independent of the store.
pub fn validate_migrations(
    current: OobVersion,
    first: OobVersion,
    migrations: &[OobMigration],
) -> Result<(), String> {
    let mut errors = Vec::new();
    for migration in migrations {
        if compare_versions(current, migration.introduced) == VersionOrder::Before
            && migration.progress != 0.0
        {
            errors.push(migration_status_error(migration.id, 0.0, migration.progress));
        }
        let Some(deprecated) = migration.deprecated else {
            continue;
        };
        if compare_versions(first, deprecated) != VersionOrder::Before {
            continue;
        }
        if compare_versions(current, deprecated) != VersionOrder::Before && migration.progress != 1.0
        {
            errors.push(migration_status_error(migration.id, 1.0, migration.progress));
        }
    }
    wrap_migration_errors(&errors)
}

/// `Runner.Validate` (`runner.go:128-180`) over the database store.
pub async fn validate_out_of_band_migrations_against(
    pool: &sqlx::PgPool,
    current: OobVersion,
    first: OobVersion,
) -> Result<(), String> {
    let migrations: Vec<OobMigration> = sg_db::out_of_band_migrations::list(pool)
        .await?
        .into_iter()
        .map(|migration| OobMigration {
            id: migration.id,
            introduced: OobVersion {
                major: migration.introduced_major,
                minor: migration.introduced_minor,
            },
            deprecated: match (migration.deprecated_major, migration.deprecated_minor) {
                (Some(major), Some(minor)) => Some(OobVersion { major, minor }),
                _ => None,
            },
            progress: migration.progress,
        })
        .collect();
    validate_migrations(current, first, &migrations)
}

/// `ValidateOutOfBandMigrationRunner` (`validate.go:18-64`).
pub async fn validate_out_of_band_migrations(
    pool: &sqlx::PgPool,
    current_raw: &str,
) -> Result<(), String> {
    if is_dev_version(current_raw) {
        return Ok(());
    }
    let Some(current) = new_version_from_string(current_raw) else {
        // `runner.logger.Warn("Skipping out-of-band migrations check", ...)`.
        return Ok(());
    };
    let Some(first_raw) = get_first_service_version(pool)
        .await
        .map_err(|err| format!("failed to retrieve first instance version: {err}"))?
    else {
        return Ok(());
    };
    let Some(first) = new_version_from_string(&first_raw) else {
        return Ok(());
    };
    validate_out_of_band_migrations_against(pool, current, first).await
}

/// The `serve_cmd.go:103-131` startup block, in order: the service-version
/// upsert (`InitDB`), `waitForRedis`, then the env skip / metadata sync /
/// validation branch.
pub async fn run_startup_migrations(
    pool: &sqlx::PgPool,
    version: &str,
    disable_oob_validation: bool,
    redis: Option<&sg_redis::KeyValue>,
) -> Result<(), ServiceError> {
    update_service_version(pool, version)
        .await
        .map_err(|err| ServiceError::new(format!("failed to update service version: {err}")))?;

    if let Some(kv) = redis {
        wait_for_redis(kv).await;
    }

    if disable_oob_validation {
        tracing::warn!("Skipping out-of-band migrations check");
        return Ok(());
    }

    synchronize_out_of_band_migrations(pool)
        .await
        .map_err(|err| {
            ServiceError::new(format!(
                "failed to synchronize out of band migration metadata: {err}"
            ))
        })?;
    validate_out_of_band_migrations(pool, version)
        .await
        .map_err(|err| {
            ServiceError::new(format!("failed to validate out of band migrations: {err}"))
        })?;
    Ok(())
}

/// `waitForRedis` (`serve_cmd.go:453-470`): ping every 150ms for up to 5s,
/// then continue anyway with a warning. Returns whether Redis came up.
pub async fn wait_for_redis(kv: &sg_redis::KeyValue) -> bool {
    wait_for_redis_with(Duration::from_secs(5), Duration::from_millis(150), || async {
        kv.ping().await.map_err(|err| err.to_string())
    })
    .await
}

/// The injectable body of [`wait_for_redis`] (used by the unit test with a
/// paused `tokio` clock).
pub async fn wait_for_redis_with<F, Fut>(
    timeout: Duration,
    interval: Duration,
    mut ping: F,
) -> bool
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<(), String>>,
{
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        tokio::time::sleep(interval).await;
        if ping().await.is_ok() {
            return true;
        }
        if tokio::time::Instant::now() > deadline {
            tracing::warn!(
                timeout_ms = timeout.as_millis() as u64,
                "Redis (used for session store) failed to become reachable. Will continue trying to establish connection in background."
            );
            return false;
        }
    }
}

/// The ranking GraphQL surface is only useful when its store tables exist
/// (they are created by the code intel migrations). Probing avoids a startup
/// failure on a frontend that has never run the code intel workers; when the
/// relation is absent the base schema stays untouched.
async fn ranking_tables_available(pool: &sqlx::PgPool) -> bool {
    sqlx::query_scalar::<_, Option<String>>(
        "SELECT to_regclass('codeintel_ranking_graph_keys')::text",
    )
    .fetch_one(pool)
    .await
    .map(|relation| relation.is_some())
    .unwrap_or(false)
}

/// M9.15b: assemble the production adapters behind the read-only diagnostic
/// seams.
///
/// The baseline's `recorder.GetCache()` wraps the shared `redispool.Cache`;
/// here a failed connect disables only the background-jobs reader. An absent
/// zoekt endpoint list yields the baseline's zero `ListAllIndexed` result, and
/// an endpoint list that cannot be parsed fails `List` per request like the
/// baseline's lazy dialer.
async fn diagnostics_deps(
    pool: sqlx::PgPool,
    live: Arc<sg_config::LiveConfig>,
) -> crate::diagnostics_graphql::DiagnosticsDeps {
    let recorder: Arc<dyn crate::diagnostics_adapters::RecorderSource> =
        match sg_redis::KeyValue::connect(&sg_redis::cache_endpoint()).await {
            Ok(kv) => {
                Arc::new(crate::diagnostics_adapters::RedisRecorderSource::from_key_value(kv))
            }
            Err(error) => {
                tracing::warn!(
                    error = %error,
                    "background-jobs reader disabled: Redis is unreachable"
                );
                Arc::new(crate::diagnostics_adapters::UnavailableRecorderSource::new(
                    format!("Redis cache unavailable: {error}"),
                ))
            }
        };

    let indexed: Arc<dyn crate::diagnostics_adapters::IndexedStatsSource> =
        match crate::service_connections::service_connections() {
            Ok(connections) if connections.zoekts.is_empty() => {
                Arc::new(crate::diagnostics_adapters::EmptyIndexedStatsSource)
            }
            Ok(connections) => {
                match crate::search_env::IndexedJobStreamer::from_connections(&connections) {
                    Ok(streamer) => {
                        Arc::new(crate::diagnostics_adapters::ZoektIndexedStatsSource::new(
                            Arc::new(streamer),
                        ))
                    }
                    Err(error) => Arc::new(
                        crate::diagnostics_adapters::ErrorIndexedStatsSource::new(
                            error.to_string(),
                        ),
                    ),
                }
            }
            Err(error) => {
                Arc::new(crate::diagnostics_adapters::ErrorIndexedStatsSource::new(error))
            }
        };

    let store = Arc::new(crate::diagnostics_adapters::ProductionDiagnosticsStore::new(
        pool,
        recorder,
        Arc::new(crate::diagnostics_adapters::LiveGitserverDiskInfoSource::new(
            Arc::clone(&live),
        )),
        Some(indexed),
    ));

    crate::diagnostics_graphql::DiagnosticsDeps {
        store,
        dotcom: crate::service_connections::sourcegraph_dotcom_mode(&|key: &str| {
            std::env::var(key).ok()
        }),
        // Overridden per request by `live`; kept for the seam's tests.
        disable_auto_git_updates: false,
        disk_usage_threshold: None,
        live: Some(live),
    }
}

/// The frontend service. Only the transport skeleton is implemented; the
/// GraphQL API, UI and enterprise setup land in later waves.
pub struct FrontendService {
    config: Option<FrontendConfig>,
    version: Version,
    /// The `conf.Get()` snapshot: the merged `SITE_CONFIG_FILE` at configure
    /// time, empty until the configuration service is ported.
    site_config: SiteConfiguration,
    allowlist_env: AllowlistEnv,
}

impl FrontendService {
    pub fn new() -> Self {
        Self {
            config: None,
            version: Version::load(),
            site_config: SiteConfiguration::default(),
            allowlist_env: AllowlistEnv::default(),
        }
    }

    /// Test hook: pin the version string instead of reading `/version.txt`.
    pub fn with_version(version: Version) -> Self {
        Self {
            version,
            ..Self::new()
        }
    }

    /// Test hook: the site configuration snapshot the middleware chain reads.
    pub fn with_site_config(mut self, site_config: SiteConfiguration) -> Self {
        self.site_config = site_config;
        self
    }

    /// Test hook: pin the `AUTH_ALLOWED_IP_ADDRESS_*` overrides.
    pub fn with_ip_allowlist_env(mut self, env: AllowlistEnv) -> Self {
        self.allowlist_env = env;
        self
    }

    pub fn config(&self) -> Option<&FrontendConfig> {
        self.config.as_ref()
    }
}

impl Default for FrontendService {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Service for FrontendService {
    fn name(&self) -> &'static str {
        service_name()
    }

    fn configure(&mut self, env: &mut EnvReader) -> Result<(), ConfigError> {
        self.config = Some(FrontendConfig::load(env)?);
        self.allowlist_env = AllowlistEnv::from_env();
        // Baseline `serve_cmd.go:135`: `overrideSiteConfig` fails startup when
        // the file cannot be read or merged. Without the variable the baseline
        // database seeds `confdefaults.Default`, so the port falls back to the
        // deployment default document (which includes the `builtin` auth
        // provider); `SITE_CONFIG_FILE` replaces it wholesale
        // (`WriteWithOverride`).
        self.site_config = match crate::site_config_file::load_from_env().map_err(|err| {
            ConfigError::new(format!("failed to apply site config overrides: {err}"))
        })? {
            Some(site_config) => site_config,
            None => crate::deploy_defaults::default_site_config(
                &std::env::var(crate::deploy_defaults::DEPLOY_TYPE_VAR).unwrap_or_default(),
            )
            .map_err(ConfigError::new)?,
        };
        Ok(())
    }

    fn validate(&self) -> Result<(), ConfigError> {
        if self.config.is_some() {
            Ok(())
        } else {
            Err(ConfigError::new("frontend configuration was not loaded"))
        }
    }

    async fn start(self: Box<Self>, ctx: ServiceContext) -> Result<(), ServiceError> {
        // `promauto` registers every GraphQL metric family at package init, so
        // they are scrapeable (with no samples) before the first request.
        crate::graphql_metrics::register();
        // The same applies to the log-event search metrics
        // (`user_usage_stats.go:288-352`).
        crate::log_events_metrics::register();
        let config = self
            .config
            .clone()
            .ok_or_else(|| ServiceError::new("frontend configuration was not loaded"))?;

        // `internal/tracer.Init` (`serve_cmd.go`): apply the initial trace
        // policy and install the tracer runtime. Exporter construction mirrors
        // `newOtelSpanProcessor`: the environment selects OTLP/Jaeger, but the
        // transport is the no-op adapter, so no spans leave the process. A
        // construction error only warns (the baseline's `newConfWatcher`
        // continues without a processor). The baseline's `conf.Watch` update is
        // the per-generation `trace_policy::apply_tracing_config` hook in
        // `middleware`.
        let trace_startup = crate::trace_policy::plan_otel_startup(
            crate::trace_policy::tracing_config_from_site(&self.site_config).as_ref(),
            crate::trace_policy::get_trace_policy(),
        );
        let _tracer = match crate::trace_policy::init_tracer_from_env(
            &trace_startup,
            Arc::new(crate::trace_policy::NoopSpanTransport),
            |key| std::env::var(key).ok(),
        ) {
            Ok(runtime) => runtime,
            Err(error) => {
                tracing::warn!(%error, "failed to configure trace exporter");
                crate::trace_policy::init_tracer(&trace_startup, None)
            }
        };

        // Baseline `serve_cmd.go:134-139`: apply the `SITE_CONFIG_FILE`
        // override first, then start the configuration service. Like the
        // baseline (whose `InitDB` fails before this point), the process needs
        // a database.
        let pool = configuration_server::FrontendConfigurationServer::connect_frontend_pool()
            .await
            .map_err(ServiceError::new)?;

        // `cmd/frontend/main.go:11` + `shared/service.go:24-26`: install the
        // global assets provider before any handler observes it.
        sg_ui::init_from_env();

        let startup_version = String::from_utf8_lossy(self.version.as_bytes()).into_owned();

        // `serve_cmd.go:99-101`: `tryAutoUpgrade` runs before `InitDB`'s
        // service-version upsert. The no-op gate is ported; a pending upgrade
        // is an explicit not-ported startup failure.
        let auto_upgrade = try_auto_upgrade(&pool, &startup_version)
            .await
            .map_err(|error| ServiceError::new(format!("frontend.tryAutoUpgrade: {error}")))?;
        if let AutoUpgradePlan::Required {
            current, target, ..
        } = &auto_upgrade
        {
            return Err(ServiceError::new(format!(
                "frontend.tryAutoUpgrade: auto-upgrade from {current} to {target} is required, but the multiversion upgrade machinery is not ported"
            )));
        }

        // `serve_cmd.go:103-131`: service version, Redis readiness, and the
        // out-of-band migration gate. `SRC_DISABLE_OOBMIGRATION_VALIDATION`
        // (non-empty) skips the metadata sync/validation.
        let startup_redis = sg_redis::KeyValue::connect(&sg_redis::cache_endpoint())
            .await
            .ok();
        run_startup_migrations(
            &pool,
            &startup_version,
            std::env::var("SRC_DISABLE_OOBMIGRATION_VALIDATION")
                .map(|value| !value.is_empty())
                .unwrap_or(false),
            startup_redis.as_ref(),
        )
        .await?;

        let blame_pool = pool.clone();
        let search_pool = blame_pool.clone();
        let session_pool = pool.clone();
        let settings_pool = pool.clone();
        let sign_out_pool = pool.clone();
        // The external actor chain's Postgres adapters (tokens, users,
        // site-admin flags, security events).
        let chain_pool = pool.clone();
        // `app.NewHandler`'s `/-/sign-out` reads `conf.Get()` per request; the
        // wiring is rebuilt for each configuration generation in
        // `LiveMiddleware`, so the factory closes over the pool only.
        let sign_out_factory: crate::app_handlers::SignOutFactory = {
            let pool = sign_out_pool;
            Arc::new(move |site: &SiteConfiguration, external_url: &str| {
                crate::app_handlers::SignOutWiring {
                    deps: Arc::new(crate::sign_out::SignOutDeps::production(
                        pool.clone(),
                        site,
                        external_url,
                    )),
                    users: Some(Arc::new(crate::session_auth::PgSessionUsers::new(
                        pool.clone(),
                    ))),
                }
            })
        };
        // Built-in auth (`/-/sign-in`, `/-/sign-up`, `/-/site-init`, ...):
        // rebuilt per configuration generation. The Redis cache pool backs the
        // account-lockout keys; an unreachable Redis degrades to the
        // disabled lockout store.
        let userpasswd_factory: crate::userpasswd::UserpasswdFactory = {
            let pool = pool.clone();
            let cache = sg_redis::KeyValue::connect(&sg_redis::cache_endpoint())
                .await
                .ok();
            Arc::new(move |site: &SiteConfiguration, external_url: &str| {
                crate::userpasswd::UserpasswdWiring {
                    deps: Arc::new(crate::userpasswd::UserpasswdDeps::production(
                        pool.clone(),
                        cache.clone(),
                        site,
                        external_url,
                    )),
                }
            })
        };
        let source = crate::configuration_source::DbConfigurationSource::new(pool)
            .map_err(ServiceError::new)?;
        crate::configuration_startup::override_from_env(&source)
            .await
            .map_err(|err| {
                ServiceError::new(format!("failed to apply site config overrides: {err}"))
            })?;
        let watch_source = source.clone();
        let extsvc_source = source.clone();
        let wiring = configuration_server::FrontendConfigurationServer::start_with_source(
            source,
            ctx.shutdown.clone(),
        )
        .await
        .map_err(|err| {
            ServiceError::new(format!("failed to initialize configuration service: {err}"))
        })?;
        // Baseline `serve_cmd.go:140`: `conf.MustValidateDefaults()` runs right
        // after `InitConfigurationServerFrontendOnly` and panics on a bad
        // default deployment document. The port surfaces the same text as a
        // startup error.
        crate::configuration_startup::validate_default_site_configs().map_err(ServiceError::new)?;
        // Baseline `serve_cmd.go:147-149`: `overrideGlobalSettings` writes the
        // `GLOBAL_SETTINGS_FILE` document to the `settings` table and fails
        // startup when it cannot (outside `overrideExtSvcConfig`'s scope).
        crate::configuration_watch::override_global_settings_from_env(&settings_pool)
            .await
            .map_err(|err| {
                ServiceError::new(format!("failed to override global settings: {err}"))
            })?;

        // Baseline `serve_cmd.go:213-219`: `graphqlbackend.NewSchema` assembles
        // the SDL and returns an error that aborts startup. The OSS schema is
        // `schema.graphql` + `outbound_webhooks.graphql` + `viewer.graphql`
        // (enterprise `OptionalResolver`s are nil). Resolvers and execution are
        // not ported yet, but assembling here keeps the fail-fast contract.
        let graphql_schema =
            Arc::new(sg_graphql::schema::assemble_oss().map_err(|err| {
                ServiceError::new(format!("failed to parse GraphQL schema: {err}"))
            })?);
        tracing::info!(
            types = graphql_schema.type_count(),
            query = graphql_schema.query_type.as_deref().unwrap_or("-"),
            mutation = graphql_schema.mutation_type.as_deref().unwrap_or("-"),
            "assembled GraphQL schema"
        );

        let external_addr = config.external_bind_address().to_owned();
        let external = sg_http::listen(&external_addr).await.map_err(|err| {
            ServiceError::with_source(format!("failed to listen on {external_addr}"), err)
        })?;

        let internal = match config.internal_bind_address() {
            Some(addr) => {
                let addr = addr.to_owned();
                let listener = sg_http::listen(&addr).await.map_err(|err| {
                    ServiceError::with_source(format!("failed to listen on {addr}"), err)
                })?;
                Some((addr, listener))
            }
            None => None,
        };

        let ready = Arc::new(AtomicBool::new(false));
        let mut background = JoinSet::new();

        // `overrideSiteConfig`/`overrideGlobalSettings` start one watcher each
        // (`go watchUpdate`). The polling replacement runs on the service's
        // JoinSet and observes the same shutdown token as the HTTP servers;
        // `serve_cmd.go`'s watcher failures only log and never fail startup.
        let extsvc_pool = settings_pool.clone();
        crate::configuration_watch::spawn_config_watchers(
            &mut background,
            watch_source,
            settings_pool,
            crate::configuration_watch::ConfigWatchPaths::from_env(),
            crate::configuration_watch::DEFAULT_WATCH_INTERVAL,
            ctx.shutdown.clone(),
        );

        let blame_deps: Arc<dyn crate::blame::BlameDeps> = Arc::new(
            crate::blame::BackendDeps::new(blame_pool, Arc::clone(wiring.live())),
        );
        // M9.8ii: `Query.search` and the internal `/.api/search/stream` handler
        // must share one `SearchDeps` instance; the search contexts and ranking
        // GraphQL stores read the same primary pool.
        let search_contexts_pool = search_pool.clone();
        let ranking_pool = search_pool.clone();
        // `serve_cmd.go:143`: `keyring.Init` builds the encryption keys from
        // the site configuration and fails startup when one cannot be
        // constructed. The `conf.Watch` equivalent below rebuilds the ring in
        // place; a rebuild failure terminates the process like the baseline's
        // `panic`.
        //
        // M9.39c: initialized before the search runtime because the owners
        // production seam decrypts `user_external_accounts.account_data`
        // with `UserExternalAccountKey`.
        let keyring = sg_keyring::init(&wiring.live().current().site)
            .await
            .map_err(|err| {
                ServiceError::with_source("initializing keyring", std::io::Error::other(err))
            })?;
        {
            let built_generation = wiring.live().generation();
            let keyring = Arc::clone(&keyring);
            let live = Arc::clone(wiring.live());
            let shutdown = ctx.shutdown.clone();
            background.spawn(async move {
                if let Err(err) = sg_keyring::watch(keyring, live, built_generation, shutdown).await
                {
                    tracing::error!(error = %err, "creating encryption keyring");
                    std::process::exit(2);
                }
            });
        }
        // M9.8im: assemble the search runtime from the same static endpoints
        // `serve_cmd` uses (the live service connections). Missing zoekt
        // endpoints or an invalid endpoint list keeps the pre-existing
        // `TreeJobRunner::unconfigured()` behavior byte-for-byte.
        let search_site = wiring.live().current().site.clone();
        // M9.40a: `authz.DefaultSubRepoPermsChecker =
        // subrepoperms.NewSubRepoPermsClient(db.SubRepoPerms())`
        // (`cmd/frontend/internal/authz/init.go:46`). The checker is gated by
        // `experimentalFeatures.subRepoPermissions.enabled`; the getter reads
        // `authz.enforceForSiteAdmins` per query. Request actors are applied by
        // the job tree (`inputs.request_context.sub_repo_actor()`), so the
        // installed runtime keeps the anonymous actor.
        let sub_repo_pool = search_pool.clone();
        let sub_repo_enforce_live = std::sync::Arc::clone(wiring.live());
        let sub_repo_getter: std::sync::Arc<
            dyn crate::sub_repo_perms_checker::SubRepoPermissionsGetter,
        > = std::sync::Arc::new(
            crate::sub_repo_perms_checker::PgSubRepoPermissionsGetter::new(
                sub_repo_pool,
                move || {
                    sub_repo_enforce_live
                        .current()
                        .site
                        .authz_enforce_for_site_admins()
                },
            ),
        );
        let sub_repo_enabled = crate::sub_repo_perms_checker::configure_from_site(
            &search_site,
            Some(std::sync::Arc::clone(&sub_repo_getter)),
            crate::search_jobutil_misc_jobs::Actor::default(),
        );
        tracing::info!(
            enabled = sub_repo_enabled,
            "configured sub-repository permission checker"
        );
        // M9.41d: `conf.Watch` in `NewSubRepoPermsClient`
        // (`subrepoperms/sub_repo_perms.go:119`). Re-read
        // `experimentalFeatures.subRepoPermissions` whenever the live site
        // config changes; the runtime is rebuilt only on an actual generation
        // change.
        crate::sub_repo_perms_checker::spawn_sub_repo_config_watch(
            &mut background,
            std::sync::Arc::clone(wiring.live()),
            sub_repo_getter,
            ctx.shutdown.clone(),
        );
        // The same runtime bundle drives both the search stream and the
        // code-monitor `Snapshot` (`codemonitors.Snapshot`, M9.45d).
        let (search_deps, snapshot_runtime): (
            Arc<dyn crate::search_stream::SearchDeps>,
            Option<crate::search_stream::TreeRuntimeClients>,
        ) = match crate::service_connections::service_connections() {
            Ok(connections) => {
                match crate::search_runtime::assemble_with_encryption_key(
                    &search_pool,
                    &connections,
                    &search_site,
                    keyring.user_external_account_key(),
                ) {
                    Ok(runtime) => (
                        Arc::new(crate::search_stream::BackendDeps::with_runtime(
                            search_pool.clone(),
                            runtime.clone(),
                        )),
                        Some(runtime),
                    ),
                    Err(error) => {
                        tracing::warn!(
                            error = %error,
                            "search runtime is not configured; search execution is not ported"
                        );
                        (
                            Arc::new(crate::search_stream::BackendDeps::new(search_pool.clone())),
                            None,
                        )
                    }
                }
            }
            Err(error) => {
                tracing::warn!(
                    error = %error,
                    "service connections unavailable; search execution is not ported"
                );
                (
                    Arc::new(crate::search_stream::BackendDeps::new(search_pool.clone())),
                    None,
                )
            }
        };
        // `serve_cmd.go:153`: `overrideExtSvcConfig` runs after the keyring is
        // initialized so it can decrypt existing configs and encrypt new ones.
        // It fails startup when the file cannot be applied; the watcher below
        // only logs, like `watchUpdate`.
        crate::extsvc_config_file::override_ext_svc_config_from_env(
            &extsvc_source,
            &extsvc_pool,
            keyring.external_service_key(),
        )
        .await
        .map_err(|err| {
            ServiceError::new(format!("failed to override external service config: {err}"))
        })?;
        crate::extsvc_config_file::spawn_extsvc_config_watcher(
            &mut background,
            extsvc_source,
            extsvc_pool,
            keyring.external_service_key(),
            crate::configuration_watch::DEFAULT_WATCH_INTERVAL,
            ctx.shutdown.clone(),
        );
        // `Mutation.logEvent(s)` writes the `user_activity:*` all-time flags
        // through `redispool.Store`. Like the rate limiter below, a redigo
        // pool does not dial until the first command; an unreachable Redis at
        // startup only turns the flag writes into errors for the affected
        // events (the baseline behaves the same once the pool is broken).
        let log_events_store = match sg_redis::KeyValue::connect(&sg_redis::store_endpoint()).await
        {
            Ok(kv) => Some(Arc::new(kv)),
            Err(error) => {
                tracing::warn!(
                    error = %error,
                    "log-event Redis store unavailable: user_activity flags will fail"
                );
                None
            }
        };
        // The assembled OSS schema is executed by `sg-graphql` (M9.8fb); the
        // session cookie authenticates external requests and the current-user
        // row backs `Query.currentUser` (M9.8fe). `Query.clientConfiguration`
        // reads `external_services` with that same pool.
        let clone_url_resolver = Arc::new(crate::clone_urls::ConfiguredCloneUrlResolver {
            first: Arc::new(crate::clone_urls::PgFirstRepoNameByCloneUrl::new(
                session_pool.clone(),
            )),
            services: Arc::new(crate::clone_urls::PgCloneUrlServiceSource::new(
                session_pool.clone(),
            )),
            live: Arc::clone(wiring.live()),
            key: keyring.external_service_key(),
        });
        let graphql_deps =
            SchemaGraphqlDeps::with_session(Arc::clone(&graphql_schema), session_pool.clone())
                // M9.23a: the admin store reads
                // `permissions.userMapping.bindID` and
                // `log.securityEventLog.location` from the live configuration
                // (`conf.Get()`) on every call.
                .with_live_site_configuration(
                    session_pool.clone(),
                    Arc::clone(wiring.live()),
                )
                // M9.21c: `internal/featureflag`'s evaluated-flag cache uses
                // `redispool.Store` (lazily dialed, so startup does not depend
                // on Redis).
                .with_feature_flag_cache(Arc::new(
                    crate::feature_flag_cache::RedisEvaluatedFlagCache::from_env(),
                ))
                // M9.8jh: the read-only `Query.site.configuration` /
                // `settingsSubject(site).configuration` surface shares the same
                // `ConfigStore` the update loop writes, so a write is visible to
                // the next query.
                .with_config_graphql(Arc::clone(wiring.store()), session_pool.clone())
                .with_client_configuration(
                    Arc::new(
                        crate::client_configuration::PgExternalServiceConfigSource::new(
                            session_pool.clone(),
                        ),
                    ),
                    Arc::clone(&keyring),
                    Arc::clone(wiring.live()),
                )
                .with_external_services(
                    Arc::new(
                        crate::external_services::PgExternalServiceConnectionStore::new(
                            session_pool.clone(),
                        ),
                    ),
                    Arc::clone(&keyring),
                    Arc::clone(wiring.live()),
                )
                .with_webhook_logs(
                    Arc::new(crate::webhook_logs::PgWebhookLogConnectionStore::new(
                        session_pool.clone(),
                    )),
                    Arc::new(
                        crate::external_services::PgExternalServiceConnectionStore::new(
                            session_pool.clone(),
                        ),
                    ),
                    Arc::clone(&keyring),
                    Arc::clone(wiring.live()),
                )
                .with_clone_urls(clone_url_resolver)
                .with_log_events(log_events_store.clone(), Arc::clone(wiring.live()))
                .with_site_configuration(Arc::clone(wiring.store()), Arc::clone(wiring.server()))
                // M9.8jg: `Site.authProviders` reads the live `auth.providers`
                // configuration snapshot on every request.
                .with_auth_providers(Arc::clone(wiring.live()))
                // M9.8ka: `User.externalAccounts` and the `ExternalAccount`
                // node read `user_external_accounts`; the provider registry is
                // rebuilt from `auth.providers` per request.
                .with_external_accounts(
                    Arc::new(
                        crate::external_accounts_graphql::PgExternalAccountsStore::new(
                            session_pool.clone(),
                        ),
                    ),
                    keyring.user_external_account_key(),
                    Arc::clone(wiring.live()),
                    crate::service_connections::sourcegraph_dotcom_mode(&|key: &str| {
                        std::env::var(key).ok()
                    }),
                )
                // M9.9b: `AddExternalAccount`/`DeleteExternalAccount` side
                // effects. Gerrit's network verification stays an explicit
                // seam (overview §4).
                .with_external_account_services(
                    Some(Arc::new(
                        crate::external_accounts_graphql::PgPermsSyncScheduler::new(
                            session_pool.clone(),
                        ),
                    )),
                    Some(Arc::new(
                        crate::external_accounts_graphql::PgExternalAccountFeatureFlags::new(
                            session_pool.clone(),
                        ),
                    )),
                    None,
                )
                // M9.13b: executor secrets reuse the existing
                // `executor_secrets` tables and the keyring's
                // `ExecutorSecretKey`; RBAC reads the existing role tables.
                .with_executor_secrets(Arc::new(
                    crate::executor_secrets_graphql::PgExecutorSecretsStore::new(
                        session_pool.clone(),
                        keyring.executor_secret_key(),
                    ),
                ))
                .with_rbac_store(Arc::new(crate::rbac_graphql::PgRbacStore::new(
                    session_pool.clone(),
                )))
                // M9.15b: production adapters behind the M9.14c diagnostic
                // seams. Background jobs read the existing
                // `background-job-logger` Redis keys (read-only), gitserver
                // disk info reuses the live shard list, and `indexed` lists
                // zoekt through the same endpoint stack as the search runtime.
                .with_diagnostics(diagnostics_deps(
                    session_pool.clone(),
                    Arc::clone(wiring.live()),
                )
                .await)
                // M9.21b: `Query.outboundRequests` reads the same Redis FIFO
                // list the Go `redisLoggerMiddleware` writes
                // (`v2:outbound-requests`); the limit comes from the live site
                // config per request (0 disables the log).
                .with_outbound_request_log(Arc::new(
                    crate::outbound_requests_graphql::RedisOutboundRequestLogStore::from_env(
                        Arc::clone(wiring.live()),
                    ),
                ));
        // M9.48d: embeddings GraphQL surface. The jobs/stats/cancel paths read
        // the existing `repo_embedding_jobs` / `repo_embedding_job_stats`
        // tables (no schema change); the external embeddings service, gitserver
        // file reader and scheduler stay explicit seams (`None` → the Go error
        // texts, never fabricated results).
        let force_allow_embeddings = std::env::var("SRC_INTERNAL_EMBEDDINGS_ALLOWED")
            .map(|value| {
                matches!(
                    value.trim().to_ascii_lowercase().as_str(),
                    "1" | "t" | "true"
                )
            })
            .unwrap_or(false);
        let embeddings_enabled = crate::embeddings_graphql::embeddings_enabled(
            wiring.live().current().site.raw(),
            force_allow_embeddings,
        );
        let repo_lookup: Arc<dyn crate::graphql_resolvers::RepositoryLookup> =
            Arc::new(crate::repository_store::PgRepositoryStore::new(
                session_pool.clone(),
            ));
        let embeddings_deps = crate::embeddings_graphql_surface::EmbeddingsDeps {
            enabled: embeddings_enabled,
            repository: Arc::new(
                crate::embeddings_graphql::PgRepositoryEmbeddingsStore::new(
                    session_pool.clone(),
                ),
            ),
            jobs: Arc::new(
                crate::embeddings_graphql_surface::PgEmbeddingsJobsStore::new(
                    session_pool.clone(),
                ),
            ),
            repos: Some(Arc::new(
                crate::embeddings_graphql_surface::RepositoryLookupAdapter {
                    store: Arc::clone(&repo_lookup),
                },
            )),
            search: None,
            files: None,
            scheduler: None,
            // `cody.IsCodyEnabled`'s unauthenticated check runs per request in
            // the prefetcher; the config-level reason is injected here.
            cody_gate: crate::embeddings_graphql_surface::CodyGate {
                cody_disabled_reason: cody_config_disabled_reason(wiring.live()),
                // Per-user verified-email lookup is not wired at this layer
                // (SEAM_GAP); the gate is still enforced when injected.
                verified_email_error: None,
            },
        };
        let graphql_deps = graphql_deps
            .with_embeddings(embeddings_deps)
            .map_err(|err| {
                ServiceError::new(format!(
                    "failed to register the embeddings GraphQL surface: {err}"
                ))
            })?;
        // M9.48d: cody context surface. The retrieval client/chunk source/
        // intent backends/URL fetcher are explicit runtime seams; the SDL and
        // the pure resolver logic (identity rerank, gates, error texts) are
        // mounted so unconnected deployments fail like the baseline instead of
        // returning fabricated context.
        let dotcom = crate::service_connections::sourcegraph_dotcom_mode(&|key: &str| {
            std::env::var(key).ok()
        });
        let cody_context_deps = crate::cody_context_graphql::CodyContextDeps {
            dotcom,
            gate: crate::embeddings_graphql_surface::CodyGate {
                cody_disabled_reason: cody_config_disabled_reason(wiring.live()),
                verified_email_error: None,
            },
            context: None,
            repos: Some(Arc::new(
                crate::embeddings_graphql_surface::RepositoryLookupAdapter {
                    store: repo_lookup,
                },
            )),
            chunks: None,
            intent: None,
            intent_http: None,
            reranker: crate::cody_context_graphql::CodyReranker::Identity,
            url_fetcher: None,
        };
        let graphql_deps = graphql_deps
            .with_cody_context(cody_context_deps)
            .map_err(|err| {
                ServiceError::new(format!(
                    "failed to register the cody context GraphQL surface: {err}"
                ))
            })?;
        // M9.48a: the authz permissions surface reuses
        // `permission_sync_jobs`, `user_repo_permissions`,
        // `repo_permissions`, `user_pending_permissions` and the Bitbucket
        // project job table (no schema change).
        let graphql_deps = graphql_deps
            .with_authz(crate::authz_graphql::AuthzHandle::new(Arc::new(
                crate::authz_graphql_store::PgAuthzStore::new(
                    session_pool.clone(),
                    Arc::clone(wiring.live()),
                ),
            )))
            .map_err(|err| {
                ServiceError::new(format!(
                    "failed to register the authz GraphQL surface: {err}"
                ))
            })?;
        // M9.35a: `Query.searchQueryAggregate` executes through the same
        // `SearchDeps` as `Query.search` and `/.api/search/stream`. The seam
        // applies the per-match tabulator and the 2s/55s deadline; buffer and
        // proactive limits fall back to the baseline defaults (500 / 50000).
        let insights_aggregations: crate::insights_aggregations_graphql::AggregationsHandle =
            Arc::new(
                crate::insights_aggregations_graphql::ProductionAggregationSeam::new(Arc::new(
                    crate::insights_aggregations::ProductionAggregationSearchSeam::from_search_deps(
                        Arc::clone(&search_deps),
                    ),
                )),
            );
        let graphql_deps = graphql_deps
            .with_insights_aggregations(insights_aggregations)
            .map_err(|err| {
                ServiceError::new(format!(
                    "failed to register the insights aggregation GraphQL surface: {err}"
                ))
            })?;
        // M9.8jf/M9.8jj: `Query.slowRequests` reads the FIFO list whose
        // capacity is `observability.captureSlowGraphQLRequestsLimit`
        // (`conf.Get()` per request in the baseline; here from the current
        // configuration snapshot). `SchemaGraphqlDeps::capture_slow_request`
        // writes to this store from the request tracer; the in-memory FIFO
        // reproduces the baseline's `slow-graphql-requests-list` semantics.
        let slow_requests_limit =
            crate::trace_policy::capture_slow_graphql_requests_limit(&wiring.live().current().site);
        let slow_requests_store: Arc<dyn crate::slow_requests_tracer::SlowRequestStore> = Arc::new(
            crate::slow_requests_tracer::InMemorySlowRequestStore::new(slow_requests_limit),
        );
        let graphql_deps = graphql_deps.with_slow_requests(Arc::new(
            crate::slow_requests_tracer::SlowRequestsSurface::new(
                slow_requests_store,
                slow_requests_limit,
            ),
        ));
        // M9.8ii: `internal/searchcontexts/init.go` installs the resolver only
        // when search contexts are enabled; the Pg store is registered through
        // the same optional seam.
        let search_contexts: crate::graphql_resolvers::SearchContextsDatabaseHandle = Arc::new(
            crate::search_contexts_store::PgSearchContextsStore::new(search_contexts_pool),
        );
        // The ranking surface is installed by the enterprise code intel hook;
        // probe for its tables so a frontend without migrations keeps the base
        // schema (and therefore byte-identical responses).
        let ranking = if ranking_tables_available(&ranking_pool).await {
            let live = Arc::clone(wiring.live());
            let raw_graph_key_root =
                sg_codeintel_ranking::document_reference_counts_graph_key(&live.current().site);
            Some((
                Arc::new(crate::ranking_store::PgRankingService::new(
                    ranking_pool,
                    live,
                )) as Arc<dyn crate::ranking_graphql::RankingService>,
                raw_graph_key_root,
            ))
        } else {
            None
        };
        // `serve_cmd.go:213`: `Query.search` plans and executes through the
        // same client deps as `/.api/search/stream`.
        let search_executor: Arc<dyn crate::search_graphql::SearchExecutor> = Arc::new(
            crate::search_graphql::BackendSearchExecutor::new(Arc::clone(&search_deps)),
        );
        // M9.8ix/M9.9d: `Repository.commit`, `GitObject`, `GitRef.target`,
        // tree entry `content`/`isSingleChild` and `GitCommit.path`/`symbols`
        // read through gitserver via the shared `sg-git` client. The
        // constructor is shared with the M9.9d wiring differential so the
        // production assembly and its live address resolution stay testable.
        let graphql_deps = graphql_deps.with_git_objects(
            crate::git_graphql::production_git_source(Arc::clone(wiring.live())),
        );
        // M9.8iy: `TreeEntry.symbols`/`TreeEntry.symbol`/`GitCommit.symbols`
        // use the same symbols endpoints as the search runtime (M9.8im); no
        // endpoints leaves the fields reporting the symbols error.
        let graphql_deps = match crate::service_connections::service_connections() {
            Ok(connections) => {
                match crate::symbols_client::default_symbols_client(connections.symbols) {
                    Some(client) => graphql_deps.with_symbols(Arc::from(client)),
                    None => graphql_deps,
                }
            }
            Err(_) => graphql_deps,
        };
        // M9.13a: the saved-search and code-monitor optional resolvers.
        // `NewSchema` appends `code_monitors.graphql` before
        // `saved_searches.graphql`, so the builders are called in that order.
        // `codemonitors.IsEnabled()` parses `DISABLE_CODE_MONITORS` with Go's
        // permissive bool syntax (`strconv.ParseBool`); parse errors count as
        // "not disabled".
        let saved_searches: crate::saved_searches_graphql::SavedSearchesHandle = Arc::new(
            crate::saved_searches_store::PgSavedSearchesGraphqlStore::new(session_pool.clone()),
        );
        let code_monitors_enabled = !matches!(
            std::env::var("DISABLE_CODE_MONITORS").as_deref(),
            Ok("1" | "t" | "T" | "TRUE" | "true" | "True")
        );
        let code_monitors = Arc::new(crate::code_monitors_graphql::CodeMonitorsResolver::new(
            Arc::new(crate::code_monitors_graphql_store::PgCodeMonitorsGraphqlStore::new(
                session_pool.clone(),
            )),
            Arc::new(match snapshot_runtime {
                Some(runtime) => crate::code_monitors_graphql::ProductionSnapshot::new(runtime),
                None => crate::code_monitors_graphql::ProductionSnapshot::unavailable(),
            }),
            Arc::new(crate::code_monitors_graphql::UnavailableTestSender),
            code_monitors_enabled,
            crate::service_connections::sourcegraph_dotcom_mode(&|key: &str| {
                std::env::var(key).ok()
            }),
        ));
        let graphql_deps = graphql_deps
            .with_code_monitors(code_monitors)
            .and_then(|deps| deps.with_saved_searches(saved_searches))
            .map_err(|err| {
                ServiceError::new(format!(
                    "failed to register saved-search/code-monitor GraphQL surfaces: {err}"
                ))
            })?;
        // M9.27b: `Query.outboundWebhooks`/`outboundWebhookEventTypes`, the
        // outbound webhook mutations and `node(OutboundWebhook)` read/write the
        // existing `outbound_webhooks*` tables. The SDL is already part of the
        // assembled OSS schema, so this only injects the production adapter.
        let graphql_deps = graphql_deps.with_outbound_webhooks(Arc::new(
            crate::outbound_webhooks_store::OutboundWebhooksPgDatabase::new(
                session_pool.clone(),
                Arc::clone(&keyring),
            ),
        ));
        // M9.41c: production read-only Batch Changes and Code Intel GraphQL
        // surfaces. `graphqlbackend.NewSchema` appends `batches.graphql` +
        // `githubapps.graphql` for a non-nil `BatchChangesResolver`, and every
        // vendored `codeintel*.graphql` document for a non-nil
        // `CodeIntelResolver`; the builders below reproduce that SDL set. The
        // stores read the same frontend tables as the Go baseline and the
        // gitserver seam backs `projectRoot`/`tags`/retention matching.
        let codeintel_git_objects =
            crate::git_graphql::production_git_source(Arc::clone(wiring.live()));
        let codeintel_git_source: Arc<dyn crate::git_graphql::GitObjectSource> =
            codeintel_git_objects.clone();
        // M9.48b: the codenav store needs gitserver `Diff` for cross-commit
        // range translations; the same concrete object also implements
        // `GitCommitExtraSource`.
        let codeintel_git_extra: Arc<dyn crate::git_commit_extra::GitCommitExtraSource> =
            codeintel_git_objects;
        let codeintel_uploads_handle: crate::codeintel_uploads_graphql::CodeIntelUploadsHandle =
            Arc::new(
                crate::codeintel_uploads_graphql_store::PgCodeIntelUploadsStore::new(
                    session_pool.clone(),
                )
                .with_git(Arc::clone(&codeintel_git_source)),
            );
        let graphql_deps = graphql_deps
            .with_batches(Arc::new(
                crate::batches_graphql_store::PgBatchesGraphqlStore::new(session_pool.clone())
                    .with_gitserver_changeset_diff_source(Arc::clone(&codeintel_git_source))
                    // M9.43d: `conf.BatchChangesEnabled()` /
                    // `conf.BatchChangesRestrictedToAdmins()` read the live site
                    // configuration (`batchChanges.enabled` /
                    // `batchChanges.restrictToAdmins`); Batch Changes is always
                    // disabled on dotcom.
                    .with_live_batch_changes_configuration(
                        Arc::clone(wiring.live()),
                        crate::service_connections::sourcegraph_dotcom_mode(&|key: &str| {
                            std::env::var(key).ok()
                        }),
                    ),
            ))
            .and_then(|deps| {
                deps.with_codeintel_uploads(Arc::clone(&codeintel_uploads_handle))
            })
            .and_then(|deps| {
                deps.with_codeintel_policies(Arc::new(
                    crate::codeintel_policies_graphql_store::PgCodeIntelPoliciesStore::new(
                        session_pool.clone(),
                        Arc::clone(&codeintel_git_source),
                    ),
                ))
            })
            .and_then(|deps| {
                // M9.48b: the code-navigation document plus its read-only
                // SCIP/lsif seam.
                deps.with_codeintel_codenav(Arc::new(
                    crate::codeintel_codenav_store::PgCodeIntelCodenavStore::new(
                        session_pool.clone(),
                    )
                    .with_git(Arc::clone(&codeintel_git_source))
                    .with_diff_source(Arc::clone(&codeintel_git_extra))
                    .with_uploads(Arc::clone(&codeintel_uploads_handle)),
                ))
            })
            .map_err(|err| {
                ServiceError::new(format!(
                    "failed to register batch changes/code intel GraphQL surfaces: {err}"
                ))
            })?;
        // M9.48c: production Own ownership GraphQL surface. `NewSchema` appends
        // `own.graphql` for a non-nil `OwnResolver`; the store reads the
        // existing `codeowners`/`assigned_*`/`own_*`/`ownership_path_stats`
        // tables and the gitserver seam backs the CODEOWNERS file lookup. No
        // new tables, columns or Redis keys.
        let graphql_deps = graphql_deps
            .with_own(Arc::new(crate::own_store::PgOwnGraphqlStore::new(
                session_pool.clone(),
                Arc::clone(&codeintel_git_source),
                crate::service_connections::sourcegraph_dotcom_mode(&|key: &str| {
                    std::env::var(key).ok()
                }),
            )))
            .map_err(|err| {
                ServiceError::new(format!(
                    "failed to register own ownership GraphQL surface: {err}"
                ))
            })?;
        // M9.42a: production Notebooks / Code Insights / GitHub Apps / Code Intel
        // autoindexing surfaces. `graphqlbackend.NewSchema` appends
        // `notebooks.graphql`, `insights.graphql` and `githubapps.graphql` for a
        // non-nil `NotebooksResolver` / `InsightsResolver` / `GitHubAppsResolver`,
        // and the `codeintel*.graphql` document set for a non-nil
        // `CodeIntelResolver` (the autoindexing document is already appended by
        // `with_codeintel_uploads` above). The stores read the same frontend /
        // code-insights tables as the Go baseline; no new tables, columns or
        // Redis keys. `with_batches` already registered `githubapps.graphql`, so
        // `with_github_apps` only attaches its handle.
        //
        // The insights store reads the `sourcegraph-codeinsights` database
        // (`database.NewInsightsDB`); connect lazily so a frontend without that
        // database still starts and only the queries surface the connection
        // error.
        let insights_dsn = crate::service_connections::service_connections()
            .map(|connections| connections.code_insights_dsn)
            .unwrap_or_default();
        let insights_pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(5)
            .connect_lazy(&insights_dsn)
            .map_err(|err| {
                ServiceError::new(format!("configuring the code insights database: {err}"))
            })?;
        let external_url = wiring
            .live()
            .current()
            .site
            .raw()
            .get("externalURL")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default()
            .to_owned();
        // `conf.CodeIntelAutoIndexingEnabled` is read per request in the
        // baseline; the store keeps a snapshot from the same live site config.
        let autoindexing_site = wiring.live().current().site.clone();
        let graphql_deps = graphql_deps
            .with_notebooks(Arc::new(
                crate::notebooks_store::PgNotebooksStore::new(session_pool.clone()),
            ))
            .and_then(|deps| {
                deps.with_insights(Arc::new(
                    crate::insights_store::PgInsightsStore::new(insights_pool.clone())
                        .with_frontend_pool(session_pool.clone())
                        // M9.47c: `licensing.Check(FeatureCodeInsights)` from
                        // the live site config `licenseKey` + the Redis
                        // validity flag (the same check the worker's
                        // `insights.license_check` routine runs).
                        .with_live_license(
                            Arc::clone(wiring.live()),
                            log_events_store.clone(),
                        ),
                ))
            })
            .and_then(|deps| {
                deps.with_github_apps(Arc::new(
                    crate::github_apps_graphql_store::GitHubAppsPgDatabase::new(
                        session_pool.clone(),
                        Arc::clone(&keyring),
                    )
                    .with_external_url(external_url),
                ))
            })
            .map_err(|err| {
                ServiceError::new(format!(
                    "failed to register notebooks/insights/github apps GraphQL surfaces: {err}"
                ))
            })?;
        // M9.44c: inject the production inference/enqueue runtime. The chain
        // now lives in this crate (`autoindexing_runtime`), so the frontend
        // process can construct it directly from the same gitserver endpoints,
        // live site configuration and frontend pool used by the rest of the
        // production assembly. `inferAutoIndexJobsForRepo` /
        // `queueAutoIndexJobsForRepo` therefore execute the real Lua inference
        // and write the existing `lsif_indexes` table instead of reporting
        // "runtime is not configured".
        let autoindexing_connections = crate::service_connections::service_connections();
        let autoindexing_git_addresses = autoindexing_connections
            .as_ref()
            .map(|connections| connections.git_servers.clone())
            .unwrap_or_default();
        let autoindexing_jobs: Option<
            Arc<dyn crate::codeintel_autoindexing_graphql::CodeIntelAutoindexingJobsRuntime>,
        > = match crate::autoindexing_runtime::ProductionAutoindexingJobsRuntime::from_site_configuration(
            session_pool.clone(),
            Arc::new(sg_git::GitserverClient::new(autoindexing_git_addresses)),
            &autoindexing_site,
        ) {
            Ok(runtime) => Some(Arc::new(runtime)),
            Err(err) => {
                // `env.MustGetInt` panics in the baseline; here the configuration
                // error is logged and the mutations keep reporting the explicit
                // "runtime is not configured" error instead of faking jobs.
                tracing::warn!(error = %err, "autoindexing jobs runtime unavailable");
                None
            }
        };
        let graphql_deps = graphql_deps.with_codeintel_autoindexing(Arc::new(
            crate::codeintel_autoindexing_graphql_store::PgCodeIntelAutoindexingStore::with_site_configuration(
                session_pool.clone(),
                autoindexing_site,
                autoindexing_jobs,
            ),
        ));
        let graphql_deps = graphql_deps
            .with_production_surfaces(
                Some(search_contexts),
                search_contexts_enabled(),
                ranking,
                Some(search_executor),
            )
            .map_err(|err| {
                ServiceError::new(format!(
                    "failed to register production GraphQL surfaces: {err}"
                ))
            })?;
        // `serve_cmd.go:436-448`: the anonymous GraphQL limiter stores GCRA
        // state in the Redis cache pool (`gql:rl:` prefix). A redigo pool does
        // not dial at construction (failures surface per request); here an
        // unreachable Redis at startup only disables the limiter.
        let graphql_deps: Arc<dyn GraphqlDeps> =
            match sg_redis::KeyValue::connect(&sg_redis::cache_endpoint()).await {
                Ok(kv) => {
                    let watcher = crate::rate_limit::BasicLimitWatcher::new(Arc::new(
                        crate::rate_limit::RedisGcraStore::new(kv),
                    ));
                    Arc::new(graphql_deps.with_rate_limit(watcher, Arc::clone(wiring.live())))
                }
                Err(error) => {
                    tracing::warn!(
                        error = %error,
                        "GraphQL rate limiter disabled: Redis is unreachable"
                    );
                    Arc::new(graphql_deps)
                }
            };

        if let Some((addr, listener)) = internal {
            let shutdown = ctx.shutdown.clone();
            let store = Arc::clone(wiring.store());
            let live = Arc::clone(wiring.live());
            let graphql_deps = Arc::clone(&graphql_deps);
            let search_deps = Arc::clone(&search_deps);
            background.spawn(async move {
                // `httpapi.go:397` registers the configuration service on the
                // internal gRPC server; `serve_cmd.go:397` multiplexes gRPC
                // and the internal HTTP handler on the same listener
                // (`internalgrpc.MultiplexHandlers`).
                let routes = tonic::service::Routes::new(
                    sg_proto::internalapi::config_service_server::ConfigServiceServer::new(
                        crate::configuration_api::ConfigApi::new(store),
                    ),
                );
                let handler =
                    middleware::internal_handler(live, Arc::clone(&graphql_deps), search_deps);
                if let Err(err) = sg_http::serve(listener, routes, handler, shutdown).await {
                    tracing::error!(addr, error = %err, "frontend internal server failed");
                }
            });
        }

        if let Some(addr) = config.prof_http_addr.clone() {
            let shutdown = ctx.shutdown.clone();
            let ready = Arc::clone(&ready);
            background.spawn(async move { debug::serve(&addr, ready, shutdown).await });
        }

        // `newExternalHTTPHandler`'s `AccessTokenAuth → Cookie → Auth →
        // AnonymousUID → featureflag → PostAuth` chain's production seams.
        let chain_deps = Arc::new(crate::actor_chain::ChainDeps::new(
            Arc::new(crate::actor_chain_adapters::PgAccessTokenAuth::new(
                chain_pool.clone(),
                Arc::clone(wiring.live()),
            )),
            Arc::new(crate::session_auth::RedisSessionStore::from_env()),
            Arc::new(crate::session_auth::PgSessionUsers::new(chain_pool.clone())),
            std::env::var("SRC_SESSION_COOKIE_KEY")
                .unwrap_or_default()
                .into_bytes(),
            Arc::new(crate::actor_chain_adapters::LiveLicenseReader::new(
                Arc::clone(wiring.live()),
            )),
            Arc::new(crate::actor_chain_adapters::PgSiteAdminChecker::new(
                chain_pool.clone(),
            )),
        ));

        // M9.8jo: `/.auth/<provider>/{login,callback}` production seams. The
        // resolver registry is rebuilt per configuration generation so provider
        // edits take effect without a restart, and M9.8jp/jq/jr register the
        // GitHub/GitLab/OIDC `GetOrCreateUser` implementations into it. A
        // callback whose provider cannot be resolved keeps the baseline 500
        // rather than faking success. The session store and license reader are
        // the chain's, so cookies and expiry behave identically.
        let oauth_http: Arc<dyn crate::oauth_login::OAuthHttpClient> =
            Arc::new(crate::oauth_routes::ReqwestOAuthHttpClient::from_env());
        let oauth_store = Arc::clone(&chain_deps.session_store);
        let oauth_license = Arc::clone(&chain_deps.license);
        let oauth_cookie_key = chain_deps.session_cookie_key.clone();
        let oauth_pool = chain_pool.clone();
        let oauth_wiring = Arc::new(
            crate::oauth_wiring::OAuthResolverWiring::new(chain_pool.clone())
                .with_live_configuration(Arc::clone(wiring.live())),
        );
        // The OAuth factory's `move` closure takes the store/license/cookie
        // key; keep copies for the SAML factory built alongside.
        let saml_store = Arc::clone(&oauth_store);
        let saml_license = Arc::clone(&oauth_license);
        let saml_cookie_key = oauth_cookie_key.clone();
        let oauth_factory: crate::oauth_routes::OAuthRouteFactory = {
            let http = Arc::clone(&oauth_http);
            let resolver_wiring = Arc::clone(&oauth_wiring);
            Arc::new(move |site: &sg_config::SiteConfiguration| {
                let resolvers = Arc::new(crate::oauth_routes::OAuthUserResolverRegistry::new());
                resolver_wiring.register(&resolvers, site);
                let backend = Arc::new(crate::oauth_login::PgOAuthLoginBackend::new(
                    oauth_pool.clone(),
                    sg_metrics::VERSION,
                    sg_db::security_event_logs::security_event_logs_in_database(site.raw()),
                ));
                let users: Arc<dyn crate::oauth_login::OAuthUserLookup> = backend.clone();
                let events: Arc<dyn crate::oauth_login::OAuthSecurityEventSink> = backend;
                // M9.8js: the OpenID Connect middleware (including the
                // `/.auth/callback` alias) has its own handler; it reuses the
                // same session store, transports, provisioning factory and
                // user/event/license seams as the generic flow.
                let oidc = Arc::new(crate::oidc_middleware::OidcMiddlewareRuntime {
                    discovery_http: resolver_wiring.oidc_http(),
                    token_http: Arc::clone(&http),
                    store: Arc::clone(&oauth_store),
                    ids: Arc::new(sg_session::OsSessionIdSource),
                    cookie_key: oauth_cookie_key.clone(),
                    state_source: Arc::new(crate::oidc_middleware::OsAuthnStateSource),
                    provisioners: resolver_wiring.oidc_provisioner_factory(),
                    users: Arc::clone(&users),
                    events: Arc::clone(&events),
                    license: Arc::clone(&oauth_license),
                });
                Arc::new(crate::oauth_routes::OAuthRouteDeps {
                    store: Arc::clone(&oauth_store),
                    ids: Arc::new(sg_session::OsSessionIdSource),
                    state_source: Arc::new(crate::oauth_login::OsStateSource),
                    cookie_key: oauth_cookie_key.clone(),
                    license: Arc::clone(&oauth_license),
                    http: Arc::clone(&http),
                    users,
                    events,
                    resolvers,
                    oidc: Some(oidc),
                })
            })
        };

        // M9.8jt: SAML endpoints and the single-SAML-provider sign-in
        // redirect. Provider configuration, IdP metadata and the sign-in
        // provider lists are rebuilt per configuration generation; the
        // session store, license reader and provisioning seams are the
        // actor chain's, so cookies and expiry behave identically.
        let saml_http: Arc<dyn crate::saml_auth::SamlHttpClient> =
            Arc::new(crate::saml_auth::ReqwestSamlHttpClient::new());
        let saml_factory: crate::saml_wiring::SamlRouteFactory = {
            let pool = chain_pool.clone();
            let store = Arc::clone(&saml_store);
            let license = Arc::clone(&saml_license);
            let cookie_key = saml_cookie_key.clone();
            let dotcom = {
                let getenv = |key: &str| std::env::var(key).ok();
                crate::service_connections::sourcegraph_dotcom_mode(&getenv)
            };
            let http = Arc::clone(&saml_http);
            // `conf.Get()` values for the provisioned user
            // (`permissions.userMapping.bindID`, `log.securityEventLog`).
            let provisioner_live = Arc::clone(wiring.live());
            Arc::new(move |site: &sg_config::SiteConfiguration| {
                let raw = site.raw();
                let external_url = raw
                    .get("externalURL")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or_default()
                    .to_owned();
                let secure = url::Url::parse(&external_url)
                    .map(|url| url.scheme() == "https")
                    .unwrap_or(false);
                let configured_expiry = raw
                    .get("auth.sessionExpiry")
                    .and_then(serde_json::Value::as_str)
                    .filter(|value| !value.is_empty())
                    .map(str::to_owned);
                Arc::new(crate::saml_wiring::SamlRouteDeps {
                    providers: crate::saml_auth::SamlProviderSet::from_site(
                        site,
                        Arc::clone(&http),
                    ),
                    sign_in_without_soap: crate::saml_auth::sign_in_providers_from_site(site, true),
                    sign_in_with_soap: crate::saml_auth::sign_in_providers_from_site(site, false),
                    auth_public: dotcom,
                    external_url,
                    store: Arc::clone(&store),
                    ids: Arc::new(sg_session::OsSessionIdSource),
                    cookie_key: cookie_key.clone(),
                    secure,
                    configured_expiry,
                    license: Arc::clone(&license),
                    provisioner: Arc::new(
                        crate::saml_auth::PgSamlProvisioner::new(pool.clone(), dotcom)
                            .with_live_configuration(Arc::clone(&provisioner_live)),
                    ),
                    users: Arc::new(crate::saml_auth::PgSamlUsers::new(pool.clone())),
                    suffix: Arc::new(crate::user_provisioning::OsUsernameSuffix),
                    sourcegraph_dotcom: dotcom,
                })
            })
        };

        sg_metrics::register_service_metadata(sg_metrics::VERSION);
        ctx.ready.mark_ready();
        ready.store(true, Ordering::SeqCst);

        // M9.32c: `SetupGitHubAppRoutes` production dependencies. The baseline
        // creates the `rcache` (`redispool.Cache`, `github_app_state`) and uses
        // `database.DB` plus `httpcli.UncachedExternalClient`; here the Redis
        // pool is dialed alongside the other startup connections and the
        // PostgreSQL pool/keyring come from the assembled process.
        let github_app = match sg_redis::KeyValue::connect(&sg_redis::cache_endpoint()).await {
            Ok(kv) => Some(Arc::new(
                crate::githubapp_http_api::GitHubAppServer::production(
                    session_pool.clone(),
                    Arc::clone(&keyring),
                    kv,
                ),
            )),
            Err(error) => {
                tracing::warn!(
                    error = %error,
                    "GitHub App state Redis unavailable: /githubapp keeps the offline wiring"
                );
                None
            }
        };

        tracing::info!(addr = %external_addr, "frontend listening");
        let served = sg_http::serve_http(
            external,
            middleware::external_handler_with_live(
                Arc::clone(wiring.live()),
                self.allowlist_env.clone(),
                Some(sign_out_factory),
                Some(userpasswd_factory),
                Some(oauth_factory),
                Some(saml_factory),
                Some(chain_pool.clone()),
                github_app,
                self.version.bytes(),
                blame_deps,
                search_deps,
                graphql_deps,
                chain_deps,
            ),
            ctx.shutdown.clone(),
        )
        .await;

        // A failure of the external listener must stop the sibling servers.
        ctx.shutdown.cancel();
        drain(&mut background).await;
        wiring.shutdown().await;
        served.map_err(|err| ServiceError::with_source("frontend http server failed", err))
    }
}

/// Wait for every background server to observe the shutdown token.
async fn drain(background: &mut JoinSet<()>) {
    while background.join_next().await.is_some() {}
}

#[cfg(test)]
mod tests {
    use super::*;

    fn service_with_config(config: FrontendConfig) -> FrontendService {
        FrontendService {
            config: Some(config),
            ..FrontendService::with_version(Version::dev())
        }
    }

    #[test]
    fn service_name_is_frontend() {
        assert_eq!(service_name(), "frontend");
        assert_eq!(FrontendService::new().name(), "frontend");
    }

    #[test]
    fn configure_requires_env_registration() {
        let mut service = FrontendService::new();
        let mut env = EnvReader::new();
        service.configure(&mut env).expect("configure");
        assert!(service.config().is_some());
        assert!(service.validate().is_ok());
    }

    #[test]
    fn configure_loads_the_deployment_default_site_config() {
        if std::env::var(crate::site_config_file::SITE_CONFIG_FILE_VAR).is_ok() {
            // The ambient environment supplies its own configuration.
            return;
        }
        let mut service = FrontendService::new();
        let mut env = EnvReader::new();
        service.configure(&mut env).expect("configure");
        // The deployment default carries a builtin provider, so ForbidAll must
        // not reject the instance like an empty configuration would.
        assert!(!crate::auth::forbidden(&service.site_config));
    }

    #[test]
    fn validate_requires_configuration() {
        assert!(FrontendService::new().validate().is_err());
    }

    #[test]
    fn configured_service_validates() {
        let service = service_with_config(FrontendConfig {
            insecure_dev: true,
            http_addr: "127.0.0.1:0".to_owned(),
            http_addr_internal: String::new(),
            prof_http_addr: None,
        });
        assert!(service.validate().is_ok());
        let config = service.config().expect("config");
        assert_eq!(config.external_bind_address(), "127.0.0.1:0");
        assert_eq!(config.internal_bind_address(), None);
    }

    fn version(major: i32, minor: i32) -> OobVersion {
        OobVersion { major, minor }
    }

    #[test]
    fn new_version_from_string_matches_the_baseline_regex() {
        // `^v?(\d+)\.(\d+)(?:\.(\d+))?(?:-\w*)?(?:\+[\w.]*)?$`.
        assert_eq!(new_version_from_string("3.42"), Some(version(3, 42)));
        assert_eq!(new_version_from_string("v3.42.0"), Some(version(3, 42)));
        assert_eq!(new_version_from_string("3.42.9"), Some(version(3, 42)));
        assert_eq!(new_version_from_string("3.42.0-rc1"), Some(version(3, 42)));
        assert_eq!(new_version_from_string("3.42.0-rc_1"), Some(version(3, 42)));
        assert_eq!(new_version_from_string("3.42.0+dev"), Some(version(3, 42)));
        assert_eq!(new_version_from_string("3.42+dev"), Some(version(3, 42)));
        assert_eq!(new_version_from_string("3.42.0+meta.1"), Some(version(3, 42)));

        assert_eq!(new_version_from_string(""), None);
        assert_eq!(new_version_from_string("3"), None);
        assert_eq!(new_version_from_string("3."), None);
        assert_eq!(new_version_from_string(".42"), None);
        assert_eq!(new_version_from_string("dev"), None);
        assert_eq!(new_version_from_string("3.42.0.1"), None);
        // `-\w*` permits an empty pre-release, exactly like the baseline regex.
        assert_eq!(new_version_from_string("3.42.0-"), Some(version(3, 42)));
        assert_eq!(new_version_from_string("v"), None);
    }

    #[test]
    fn compare_versions_ignores_patch_and_orders_by_major_then_minor() {
        assert_eq!(compare_versions(version(3, 42), version(3, 42)), VersionOrder::Equal);
        assert_eq!(compare_versions(version(3, 42), version(3, 43)), VersionOrder::Before);
        assert_eq!(compare_versions(version(3, 43), version(3, 42)), VersionOrder::After);
        assert_eq!(compare_versions(version(3, 99), version(4, 0)), VersionOrder::Before);
        assert_eq!(compare_versions(version(4, 0), version(3, 99)), VersionOrder::After);
    }

    #[test]
    fn is_valid_upgrade_follows_the_baseline_rules() {
        // No previous version (or an unparseable one) permits any upgrade.
        assert!(is_valid_upgrade(None, "3.42.0"));
        assert!(is_valid_upgrade(Some("not-semver"), "3.42.0"));
        assert!(is_valid_upgrade(Some("3.42.0"), "not-semver"));
        // Same major: forward by at most one minor, or any downgrade.
        assert!(is_valid_upgrade(Some("3.42.0"), "3.42.0"));
        assert!(is_valid_upgrade(Some("3.42.0"), "3.43.0"));
        assert!(!is_valid_upgrade(Some("3.42.0"), "3.44.0"));
        assert!(is_valid_upgrade(Some("3.42.0"), "3.41.0"));
        // Next major: only x.0 is a valid jump.
        assert!(is_valid_upgrade(Some("3.42.0"), "4.0.0"));
        assert!(!is_valid_upgrade(Some("3.42.0"), "4.1.0"));
        // A major-version downgrade is always a valid "upgrade".
        assert!(is_valid_upgrade(Some("4.0.0"), "3.42.0"));
    }

    #[test]
    fn validate_migrations_reports_unfinished_migrations_in_sorted_order() {
        assert_eq!(validate_migrations(version(3, 42), version(3, 0), &[]), Ok(()));

        let migrations = [
            // Introduced after the running version but already partially
            // rolled back: it must be at 0%.
            OobMigration {
                id: 12,
                introduced: version(4, 0),
                deprecated: None,
                progress: 0.5,
            },
            // Deprecated before the first instance version: ignore entirely.
            OobMigration {
                id: 3,
                introduced: version(3, 0),
                deprecated: Some(version(2, 0)),
                progress: 0.5,
            },
            // Deprecated in this version: it must be complete.
            OobMigration {
                id: 8,
                introduced: version(3, 0),
                deprecated: Some(version(3, 42)),
                progress: 0.25,
            },
            // Complete deprecation is fine.
            OobMigration {
                id: 9,
                introduced: version(3, 0),
                deprecated: Some(version(3, 42)),
                progress: 1.0,
            },
        ];
        let error = validate_migrations(version(3, 42), version(3, 0), &migrations)
            .expect_err("unfinished migrations must be rejected");
        assert_eq!(
            error,
            "Unfinished migrations. Please revert Sourcegraph to the previous version and wait for the following migrations to complete.\n\n  - migration 12 expected to be at 0.00% (at 50.00%)\n\n  - migration 8 expected to be at 100.00% (at 25.00%)\n\n"
        );
    }

    #[tokio::test]
    async fn wait_for_redis_with_returns_once_the_ping_succeeds() {
        let mut attempts = 0;
        let up = wait_for_redis_with(Duration::from_secs(5), Duration::from_millis(1), || {
            attempts += 1;
            let attempt = attempts;
            async move {
                if attempt < 3 {
                    Err("connection refused".to_owned())
                } else {
                    Ok(())
                }
            }
        })
        .await;
        assert!(up);
        assert_eq!(attempts, 3);
    }

    #[test]
    fn version_and_patch_matches_the_baseline() {
        assert_eq!(
            new_version_and_patch_from_string("5.1.3"),
            Some((version(5, 1), 3))
        );
        assert_eq!(
            new_version_and_patch_from_string("v4.5"),
            Some((version(4, 5), 0))
        );
        assert_eq!(
            new_version_and_patch_from_string("5.2.0+dev"),
            Some((version(5, 2), 0))
        );
        assert_eq!(
            new_version_and_patch_from_string("5.2.1-rc1"),
            Some((version(5, 2), 1))
        );
        assert_eq!(new_version_and_patch_from_string("dev"), None);
    }

    fn auto_upgrade_required(plan: AutoUpgradePlan) -> bool {
        matches!(plan, AutoUpgradePlan::Required { .. })
    }

    #[test]
    fn autoupgrade_gate_matches_the_baseline_noop_paths() {
        // Fresh instance: GetAutoUpgrade returns no row.
        assert_eq!(
            auto_upgrade_plan(None, "5.2.0", false),
            Ok(AutoUpgradePlan::Skip(AutoUpgradeSkip::FreshInstance))
        );
        // auto_upgrade=false and no SRC_AUTOUPGRADE.
        assert_eq!(
            auto_upgrade_plan(Some(("5.1.0".to_owned(), false)), "5.2.0", false),
            Ok(AutoUpgradePlan::Skip(AutoUpgradeSkip::Disabled))
        );
        // Same major/minor and an equal-or-newer patch.
        assert_eq!(
            auto_upgrade_plan(Some(("5.2.3".to_owned(), true)), "5.2.3", false),
            Ok(AutoUpgradePlan::Skip(AutoUpgradeSkip::SameVersionOrNewer))
        );
        // Unparseable target: warn and skip.
        assert_eq!(
            auto_upgrade_plan(Some(("5.1.0".to_owned(), true)), "dev", false),
            Ok(AutoUpgradePlan::Skip(AutoUpgradeSkip::UnexpectedTargetVersion))
        );
        // Unparseable current version: baseline error string.
        assert_eq!(
            auto_upgrade_plan(Some(("vNext".to_owned(), true)), "5.2.0", false),
            Err(
                "unexpected string for desired instance schema version, skipping auto-upgrade (vNext)"
                    .to_owned()
            )
        );
    }

    #[test]
    fn autoupgrade_gate_selects_the_upgrade_path_when_required() {
        // DB intent, older minor version.
        assert!(auto_upgrade_required(
            auto_upgrade_plan(Some(("5.1.0".to_owned(), true)), "5.2.0", false).unwrap()
        ));
        // Environment intent.
        assert!(auto_upgrade_required(
            auto_upgrade_plan(Some(("5.1.0".to_owned(), false)), "5.2.0", true).unwrap()
        ));
        // Same minor, older patch.
        assert!(auto_upgrade_required(
            auto_upgrade_plan(Some(("5.2.1".to_owned(), true)), "5.2.3", false).unwrap()
        ));
    }

    #[tokio::test]
    async fn wait_for_redis_with_gives_up_at_the_timeout_without_failing_startup() {
        let up = wait_for_redis_with(
            Duration::from_millis(20),
            Duration::from_millis(1),
            || async { Err("connection refused".to_owned()) },
        )
        .await;
        assert!(!up);
    }
}
