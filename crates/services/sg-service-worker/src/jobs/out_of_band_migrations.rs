#![forbid(unsafe_code)]

//! `cmd/worker/internal/migrations` + `internal/oobmigration`: the
//! `out-of-band-migrations` worker job.
//!
//! The job mirrors the baseline's setup sequence: synchronize the embedded
//! migration metadata, register the OSS migrators, validate the runner (unless
//! `SRC_DISABLE_OOBMIGRATION_VALIDATION` is set), determine the current and
//! first instance versions, and return one long-running routine that executes
//! each active migration in the forward or reverse direction.
//!
//! Ported OSS migrators:
//!
//! * id 13 `externalServiceWebhookMigrator` (batch 50, every 3s): fills
//!   `external_services.has_webhooks` from the (decrypted, JSONC) config.
//! * id 19 `userRoleAssignmentMigrator` (batch 250, every 10s): inserts the
//!   `USER` / `SITE_ADMINISTRATOR` system roles into `user_roles`.
//!
//! Enterpise migrators (codeintel, insights, IAM, remaining batch-changes
//! migrators) are not ported; `RegisterEnterpriseMigrators` is therefore a
//! documented gap (docs/modules/06_worker.md §78).
//!
//! Deviations, all documented in §78.3:
//!
//! * The runner processes due migrations sequentially instead of one
//!   goroutine per migration. Each migration still runs on its own interval
//!   and the 30s refresh is preserved.
//! * `Version.Patch` is only meaningful for [`new_version_and_patch_from_string`];
//!   the baseline's `len(matches) == 3` branch is unreachable because
//!   `FindStringSubmatch` always returns 1+3 captures. The patch value still
//!   ends up parsed (or zero when absent), because `strconv.Atoi("")` errors
//!   and is ignored.
//! * The pre-3.29 `introduced`/`deprecated` fallback columns are reproduced
//!   but cannot be exercised against the current schema.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use sg_db::out_of_band_migrations::{self as store, Migration, YamlMigration, YAML_MIGRATIONS};
use sg_encryption::Key;
use sqlx::PgPool;
use tokio::time::sleep_until;
use tokio_util::sync::CancellationToken;

use super::{Job, JobDeps};
use crate::framework::BackgroundRoutine;

/// `oobmigration.RefreshInterval`.
pub const REFRESH_INTERVAL: Duration = Duration::from_secs(30);

/// `version.devVersion`.
pub const DEV_VERSION: &str = "0.0.0+dev";

/// `version.LastMinorVersionInMajorRelease`.
pub const LAST_MINOR_VERSION_IN_MAJOR_RELEASE: &[(i32, i32)] = &[(3, 43), (4, 5)];

/// `types.UserSystemRole`.
pub const USER_SYSTEM_ROLE: &str = "USER";
/// `types.SiteAdministratorSystemRole`.
pub const SITE_ADMINISTRATOR_SYSTEM_ROLE: &str = "SITE_ADMINISTRATOR";

/// `oobmigration.Version`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Version {
    pub major: i32,
    pub minor: i32,
    /// `Version.Dev`: whether the version string carried the `+dev` flag.
    pub dev: bool,
}

impl Version {
    /// `NewVersion`.
    pub fn new(major: i32, minor: i32) -> Self {
        Self {
            major,
            minor,
            dev: false,
        }
    }

    /// `newDevVersion`.
    pub fn new_dev(major: i32, minor: i32) -> Self {
        Self {
            major,
            minor,
            dev: true,
        }
    }

    /// `Version.String`: `"%d.%d"`.
    pub fn to_version_string(&self) -> String {
        format!("{}.{}", self.major, self.minor)
    }

    /// `Version.GitTag`.
    pub fn git_tag(&self) -> String {
        self.git_tag_with_patch(0)
    }

    /// `Version.GitTagWithPatch`.
    pub fn git_tag_with_patch(&self, patch: i32) -> String {
        format!("v{}.{}.{}", self.major, self.minor, patch)
    }

    /// `Version.Next`: bump the minor, rolling to the next major at the last
    /// minor release of a major.
    pub fn next(&self) -> Version {
        if LAST_MINOR_VERSION_IN_MAJOR_RELEASE
            .iter()
            .any(|(major, minor)| *major == self.major && *minor == self.minor)
        {
            return Version::new(self.major + 1, 0);
        }

        Version::new(self.major, self.minor + 1)
    }

    /// `Version.Previous`: the preceding minor, or the last minor of the
    /// previous major. The flag is false when that major has no known
    /// terminal minor; like the baseline the returned version is still
    /// `NewVersion(major-1, 0)` in that case.
    pub fn previous(&self) -> (Version, bool) {
        if self.minor == 0 {
            let found = LAST_MINOR_VERSION_IN_MAJOR_RELEASE
                .iter()
                .find(|(major, _)| *major == self.major - 1)
                .copied();
            return match found {
                Some((major, minor)) => (Version::new(major, minor), true),
                None => (Version::new(self.major - 1, 0), false),
            };
        }

        (Version::new(self.major, self.minor - 1), true)
    }
}

impl PartialOrd for Version {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Version {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        (self.major, self.minor, self.dev).cmp(&(other.major, other.minor, other.dev))
    }
}

fn take_digits(bytes: &[u8], index: &mut usize) -> Option<String> {
    let start = *index;
    while *index < bytes.len() && bytes[*index].is_ascii_digit() {
        *index += 1;
    }
    if *index == start {
        return None;
    }
    Some(String::from_utf8_lossy(&bytes[start..*index]).into_owned())
}

fn is_word(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || byte == b'_'
}

/// `NewVersionAndPatchFromString`.
///
/// Returns the zero version and a `false` flag when the input does not match
/// `^v?(\d+)\.(\d+)(?:\.(\d+))?(?:-\w*)?(?:\+[\w.]*)?$` (after stripping a
/// `+dev` suffix, which sets the `Dev` flag). The patch is zero when absent,
/// matching `strconv.Atoi("")`'s ignored error.
pub fn new_version_and_patch_from_string(value: &str) -> (Version, i32, bool) {
    let mut input = value;
    let mut constructor: fn(i32, i32) -> Version = Version::new;
    if let Some(stripped) = input.strip_suffix("+dev") {
        input = stripped;
        constructor = Version::new_dev;
    }

    let bytes = input.as_bytes();
    let mut index = 0;

    if bytes.get(index) == Some(&b'v') {
        index += 1;
    }
    let Some(major) = take_digits(bytes, &mut index) else {
        return (Version::new(0, 0), 0, false);
    };
    if bytes.get(index) != Some(&b'.') {
        return (Version::new(0, 0), 0, false);
    }
    index += 1;
    let Some(minor) = take_digits(bytes, &mut index) else {
        return (Version::new(0, 0), 0, false);
    };

    let mut patch = 0;
    if bytes.get(index) == Some(&b'.') {
        index += 1;
        let Some(digits) = take_digits(bytes, &mut index) else {
            return (Version::new(0, 0), 0, false);
        };
        patch = digits.parse::<i32>().unwrap_or(0);
    }

    if bytes.get(index) == Some(&b'-') {
        index += 1;
        while index < bytes.len() && is_word(bytes[index]) {
            index += 1;
        }
    }

    if bytes.get(index) == Some(&b'+') {
        index += 1;
        while index < bytes.len() && (is_word(bytes[index]) || bytes[index] == b'.') {
            index += 1;
        }
    }

    if index != bytes.len() {
        return (Version::new(0, 0), 0, false);
    }

    let major = major.parse::<i32>().unwrap_or(0);
    let minor = minor.parse::<i32>().unwrap_or(0);
    (constructor(major, minor), patch, true)
}

/// `NewVersionFromString`.
pub fn new_version_from_string(value: &str) -> Option<Version> {
    let (version, _, ok) = new_version_and_patch_from_string(value);
    ok.then_some(version)
}

/// `oobmigration.UpgradeRange`.
pub fn upgrade_range(from: Version, to: Version) -> Result<Vec<Version>, String> {
    if compare_versions(from, to) != VersionOrder::Before {
        return Err(format!(
            "invalid range (from={} >= to={})",
            from.to_version_string(),
            to.to_version_string()
        ));
    }

    let mut versions = Vec::new();
    let mut version = from;
    while compare_versions(version, to) != VersionOrder::After {
        versions.push(version);
        version = version.next();
    }

    Ok(versions)
}

/// `oobmigration.VersionOrder` (the iota values).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VersionOrder {
    Before,
    Equal,
    After,
}

/// `CompareVersions`: compares major then minor, ignoring the `Dev` flag.
pub fn compare_versions(a: Version, b: Version) -> VersionOrder {
    if a.major < b.major {
        return VersionOrder::Before;
    }
    if a.major > b.major {
        return VersionOrder::After;
    }
    if a.minor < b.minor {
        return VersionOrder::Before;
    }
    if a.minor > b.minor {
        return VersionOrder::After;
    }
    VersionOrder::Equal
}

/// `SortVersions`.
pub fn sort_versions(versions: &mut [Version]) {
    versions.sort_by(|a, b| match compare_versions(*a, *b) {
        VersionOrder::Before => std::cmp::Ordering::Less,
        VersionOrder::Equal => std::cmp::Ordering::Equal,
        VersionOrder::After => std::cmp::Ordering::Greater,
    });
}

/// `pointIntersectsInterval`.
fn point_intersects_interval(lower: Version, upper: Version, point: Version) -> bool {
    compare_versions(point, lower) != VersionOrder::Before
        && compare_versions(upper, point) != VersionOrder::Before
}

/// `oobmigration.MigrationInterrupt`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MigrationInterrupt {
    pub version: Version,
    pub migration_ids: Vec<i32>,
}

/// `ScheduleMigrationInterrupts` over the embedded metadata.
pub fn schedule_migration_interrupts(
    from: Version,
    to: Version,
) -> Result<Vec<MigrationInterrupt>, String> {
    schedule_migration_interrupts_with(from, to, YAML_MIGRATIONS)
}

fn schedule_migration_interrupts_with(
    from: Version,
    to: Version,
    migrations: &[YamlMigration],
) -> Result<Vec<MigrationInterrupt>, String> {
    match compare_versions(from, to) {
        VersionOrder::Before => schedule_upgrade(from, to, migrations),
        VersionOrder::After => schedule_downgrade(from, to, migrations),
        VersionOrder::Equal => Ok(Vec::new()),
    }
}

#[derive(Debug, Clone, Copy)]
struct MigrationInterval {
    id: i32,
    introduced: Version,
    deprecated: Version,
}

/// `scheduleUpgrade`.
fn schedule_upgrade(
    from: Version,
    to: Version,
    migrations: &[YamlMigration],
) -> Result<Vec<MigrationInterrupt>, String> {
    let mut intervals = Vec::new();
    for migration in migrations {
        let (Some(deprecated_major), Some(deprecated_minor)) = (
            migration.deprecated_version_major,
            migration.deprecated_version_minor,
        ) else {
            continue;
        };

        let interval = MigrationInterval {
            id: migration.id,
            introduced: Version::new(
                migration.introduced_version_major,
                migration.introduced_version_minor,
            ),
            deprecated: Version::new(deprecated_major, deprecated_minor),
        };

        if compare_versions(from, interval.deprecated) == VersionOrder::Before
            && compare_versions(interval.deprecated, to) != VersionOrder::After
        {
            intervals.push(interval);
        }
    }

    intervals.sort_by(|a, b| match compare_versions(a.deprecated, b.deprecated) {
        VersionOrder::Before => std::cmp::Ordering::Less,
        VersionOrder::Equal => std::cmp::Ordering::Equal,
        VersionOrder::After => std::cmp::Ordering::Greater,
    });

    let mut points: Vec<Version> = Vec::with_capacity(intervals.len());
    for interval in &intervals {
        let needs_point = points.is_empty()
            || compare_versions(*points.last().expect("non-empty"), interval.introduced)
                == VersionOrder::Before;
        if needs_point {
            let (previous, ok) = interval.deprecated.previous();
            if !ok {
                return Err(format!(
                    "cannot determine version prior to {}",
                    interval.deprecated.to_version_string()
                ));
            }
            points.push(previous);
        }
    }

    let mut interrupts = make_covering_set(&intervals, &points);
    interrupts.sort_by(|a, b| match compare_versions(a.version, b.version) {
        VersionOrder::Before => std::cmp::Ordering::Less,
        VersionOrder::Equal => std::cmp::Ordering::Equal,
        VersionOrder::After => std::cmp::Ordering::Greater,
    });
    Ok(interrupts)
}

/// `scheduleDowngrade`.
fn schedule_downgrade(
    from: Version,
    to: Version,
    migrations: &[YamlMigration],
) -> Result<Vec<MigrationInterrupt>, String> {
    let mut intervals = Vec::new();
    for migration in migrations {
        let deprecated = match (
            migration.deprecated_version_major,
            migration.deprecated_version_minor,
        ) {
            (Some(major), Some(minor)) => Version::new(major, minor),
            // "Just assume it's deprecated after the current version prior to
            // a downgrade."
            _ => to.next(),
        };

        let interval = MigrationInterval {
            id: migration.id,
            introduced: Version::new(
                migration.introduced_version_major,
                migration.introduced_version_minor,
            ),
            deprecated,
        };

        if compare_versions(to, interval.introduced) != VersionOrder::After
            && compare_versions(interval.introduced, from) == VersionOrder::Before
        {
            intervals.push(interval);
        }
    }

    intervals.sort_by(|a, b| match compare_versions(b.introduced, a.introduced) {
        VersionOrder::Before => std::cmp::Ordering::Less,
        VersionOrder::Equal => std::cmp::Ordering::Equal,
        VersionOrder::After => std::cmp::Ordering::Greater,
    });

    let mut points: Vec<Version> = Vec::with_capacity(intervals.len());
    for interval in &intervals {
        let needs_point = points.is_empty()
            || compare_versions(interval.deprecated, *points.last().expect("non-empty"))
                != VersionOrder::After;
        if needs_point {
            points.push(interval.introduced);
        }
    }

    let mut interrupts = make_covering_set(&intervals, &points);
    interrupts.sort_by(|a, b| match compare_versions(b.version, a.version) {
        VersionOrder::Before => std::cmp::Ordering::Less,
        VersionOrder::Equal => std::cmp::Ordering::Equal,
        VersionOrder::After => std::cmp::Ordering::Greater,
    });
    Ok(interrupts)
}

/// `makeCoveringSet`: pair each chosen point with the intervals it covers.
fn make_covering_set(
    intervals: &[MigrationInterval],
    points: &[Version],
) -> Vec<MigrationInterrupt> {
    let mut covering_set: BTreeMap<Version, Vec<i32>> = BTreeMap::new();
    let mut points: Vec<Version> = points.to_vec();
    points.reverse();

    'outer: for interval in intervals {
        for point in &points {
            if point_intersects_interval(interval.introduced, interval.deprecated, *point) {
                covering_set.entry(*point).or_default().push(interval.id);
                continue 'outer;
            }
        }
        panic!("unreachable: input interval not covered in output");
    }

    covering_set
        .into_iter()
        .map(|(version, mut migration_ids)| {
            migration_ids.sort_unstable();
            MigrationInterrupt {
                version,
                migration_ids,
            }
        })
        .collect()
}

/// `migrationStatusError`.
#[derive(Debug, Clone, PartialEq)]
pub struct MigrationStatusError {
    pub id: i32,
    pub expected_progress: f64,
    pub actual_progress: f64,
}

impl std::fmt::Display for MigrationStatusError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "migration {} expected to be at {:.2}% (at {:.2}%)",
            self.id,
            self.expected_progress * 100.0,
            self.actual_progress * 100.0
        )
    }
}

/// `wrapMigrationErrors`.
fn wrap_migration_errors(errs: &[String]) -> Option<String> {
    if errs.is_empty() {
        return None;
    }

    let mut descriptions: Vec<String> = errs.iter().map(|err| format!("  - {err}\n")).collect();
    descriptions.sort();

    Some(format!(
        "Unfinished migrations. Please revert Sourcegraph to the previous version and wait for the following migrations to complete.\n\n{}\n",
        descriptions.join("\n")
    ))
}

/// `Runner.Validate` over an already-listed set of migrations.
pub fn validate_migrations(
    migrations: &[Migration],
    current_version: Version,
    first_version: Version,
) -> Result<(), String> {
    let mut errors = Vec::new();

    for migration in migrations {
        if compare_versions(
            current_version,
            Version::new(migration.introduced_major, migration.introduced_minor),
        ) == VersionOrder::Before
            && migration.progress != 0.0
        {
            errors.push(
                MigrationStatusError {
                    id: migration.id,
                    expected_progress: 0.0,
                    actual_progress: migration.progress,
                }
                .to_string(),
            );
        }

        let (Some(deprecated_major), Some(deprecated_minor)) =
            (migration.deprecated_major, migration.deprecated_minor)
        else {
            continue;
        };
        let deprecated = Version::new(deprecated_major, deprecated_minor);

        if compare_versions(first_version, deprecated) != VersionOrder::Before {
            continue;
        }

        if compare_versions(current_version, deprecated) != VersionOrder::Before
            && migration.progress != 1.0
        {
            errors.push(
                MigrationStatusError {
                    id: migration.id,
                    expected_progress: 1.0,
                    actual_progress: migration.progress,
                }
                .to_string(),
            );
        }
    }

    match wrap_migration_errors(&errors) {
        Some(message) => Err(message),
        None => Ok(()),
    }
}

/// `oobmigration.Migrator`.
#[async_trait]
pub trait Migrator: Send + Sync {
    /// `Progress`: percentage `[0, 1]` of records still to migrate in the
    /// requested direction.
    async fn progress(&self, apply_reverse: bool) -> Result<f64, String>;
    /// `Up`: run one forward batch.
    async fn up(&self) -> Result<(), String>;
    /// `Down`: run one reverse batch.
    async fn down(&self) -> Result<(), String>;
}

struct RegisteredMigrator {
    migrator: Arc<dyn Migrator>,
    interval: Duration,
}

/// `oobmigration.Runner`.
pub struct Runner {
    pool: PgPool,
    migrators: Mutex<HashMap<i32, RegisteredMigrator>>,
}

impl Runner {
    pub fn new(pool: PgPool) -> Self {
        Self {
            pool,
            migrators: Mutex::new(HashMap::new()),
        }
    }

    /// `Runner.SynchronizeMetadata`.
    pub async fn synchronize_metadata(&self) -> Result<(), String> {
        store::synchronize_metadata(&self.pool).await
    }

    /// `Runner.Register`; an interval of zero defaults to one second.
    pub fn register(
        &self,
        id: i32,
        migrator: Arc<dyn Migrator>,
        interval: Duration,
    ) -> Result<(), String> {
        let mut migrators = self.migrators.lock().expect("migrators lock poisoned");
        if migrators.contains_key(&id) {
            return Err(format!("migrator {id} already registered"));
        }

        let interval = if interval.is_zero() {
            Duration::from_secs(1)
        } else {
            interval
        };
        migrators.insert(id, RegisteredMigrator { migrator, interval });
        Ok(())
    }

    /// `Runner.Validate`.
    pub async fn validate(
        &self,
        current_version: Version,
        first_version: Version,
    ) -> Result<(), String> {
        let migrations = store::list(&self.pool).await?;
        validate_migrations(&migrations, current_version, first_version)
    }

    /// `Runner.UpdateDirection`, in one transaction.
    pub async fn update_direction(&self, ids: &[i32], apply_reverse: bool) -> Result<(), String> {
        let mut tx = self.pool.begin().await.map_err(|e| e.to_string())?;
        for id in ids {
            sqlx::query("UPDATE out_of_band_migrations SET apply_reverse = $1 WHERE id = $2")
                .bind(apply_reverse)
                .bind(id)
                .execute(&mut *tx)
                .await
                .map_err(|e| e.to_string())?;
        }
        tx.commit().await.map_err(|e| e.to_string())
    }

    /// `Runner.Name`.
    pub fn name(&self) -> &'static str {
        "oobmigrationRunner"
    }

    /// `Runner.Start`: run each active migration on its own interval,
    /// refreshing the database view every [`REFRESH_INTERVAL`].
    pub async fn start(
        &self,
        current_version: Version,
        first_version: Version,
        shutdown: CancellationToken,
    ) {
        struct State {
            migration: Migration,
            interval: Duration,
            next_tick: Instant,
        }

        let mut states: HashMap<i32, State> = HashMap::new();
        let mut next_refresh = Instant::now();

        loop {
            let now = Instant::now();
            if now >= next_refresh {
                let migrations = match store::list(&self.pool).await {
                    Ok(migrations) => migrations,
                    Err(err) => {
                        tracing::error!(error = %err, "Failed to list out-of-band migrations");
                        Vec::new()
                    }
                };

                // Snapshot the registered migrators so the lock is not held
                // across database awaits.
                let registered: Vec<(i32, Arc<dyn Migrator>, Duration)> = {
                    let migrators = self.migrators.lock().expect("migrators lock poisoned");
                    migrations
                        .iter()
                        .filter_map(|migration| {
                            migrators.get(&migration.id).map(|registered| {
                                (
                                    migration.id,
                                    Arc::clone(&registered.migrator),
                                    registered.interval,
                                )
                            })
                        })
                        .collect()
                };

                for (id, migrator, interval) in registered {
                    let Some(migration) = migrations.iter().find(|m| m.id == id).cloned() else {
                        continue;
                    };
                    if !should_run(current_version, first_version, &migration) {
                        continue;
                    }

                    if let Some(state) = states.get_mut(&migration.id) {
                        // Refresh the database view; keep our next tick.
                        state.migration = migration;
                        if let Err(err) =
                            update_progress(&self.pool, &mut state.migration, migrator.as_ref())
                                .await
                        {
                            tracing::error!(
                                error = %err,
                                migration_id = state.migration.id,
                                "Failed to determine migration progress"
                            );
                        }
                    } else {
                        let mut migration = migration;
                        if let Err(err) =
                            update_progress(&self.pool, &mut migration, migrator.as_ref()).await
                        {
                            tracing::error!(
                                error = %err,
                                migration_id = migration.id,
                                "Failed to determine migration progress"
                            );
                        }
                        states.insert(
                            migration.id,
                            State {
                                migration,
                                interval,
                                next_tick: Instant::now() + interval,
                            },
                        );
                    }
                }
                next_refresh = Instant::now() + REFRESH_INTERVAL;
            }

            // Run every migration whose interval has elapsed. The baseline
            // runs each in its own goroutine; here they are sequential, but
            // each still answers to its own schedule.
            let now = Instant::now();
            let mut due: Vec<i32> = states
                .iter()
                .filter(|(_, state)| now >= state.next_tick)
                .map(|(id, _)| *id)
                .collect();
            due.sort_unstable();
            for id in due {
                let migrator = {
                    let migrators = self.migrators.lock().expect("migrators lock poisoned");
                    migrators.get(&id).map(|r| Arc::clone(&r.migrator))
                };
                let (Some(migrator), Some(state)) = (migrator, states.get_mut(&id)) else {
                    continue;
                };

                if !state.migration.complete() {
                    if let Err(err) =
                        run_migration_function(&self.pool, &mut state.migration, migrator.as_ref())
                            .await
                    {
                        tracing::error!(
                            error = %err,
                            migration_id = state.migration.id,
                            "Failed migration action"
                        );
                    }
                }
                state.next_tick = Instant::now() + state.interval;
            }

            let wake_at = states
                .values()
                .map(|state| state.next_tick)
                .chain(std::iter::once(next_refresh))
                .min()
                .expect("next_refresh is always present");

            tokio::select! {
                _ = sleep_until(tokio::time::Instant::from_std(wake_at)) => {}
                _ = shutdown.cancelled() => return,
            }
        }
    }
}

/// `Runner.Start`'s `shouldRunMigration` predicate.
pub fn should_run(current_version: Version, first_version: Version, migration: &Migration) -> bool {
    let introduced = Version::new(migration.introduced_major, migration.introduced_minor);
    if compare_versions(current_version, introduced) == VersionOrder::Before {
        return false;
    }

    let Some(deprecated_major) = migration.deprecated_major else {
        return true;
    };
    let deprecated = Version::new(
        deprecated_major,
        migration.deprecated_minor.unwrap_or_default(),
    );

    if compare_versions(first_version, deprecated) == VersionOrder::After {
        return false;
    }

    compare_versions(current_version, deprecated) == VersionOrder::Before
}

/// `runMigrationFunction`: run one batch, record the error, then refresh the
/// stored progress regardless of the batch outcome.
async fn run_migration_function(
    pool: &PgPool,
    migration: &mut Migration,
    migrator: &dyn Migrator,
) -> Result<(), String> {
    let result = if migration.apply_reverse {
        migrator.down().await
    } else {
        migrator.up().await
    };

    if let Err(err) = result {
        store::add_error(pool, migration.id, &err).await?;
    }

    update_progress(pool, migration, migrator).await
}

/// `updateProgress`.
async fn update_progress(
    pool: &PgPool,
    migration: &mut Migration,
    migrator: &dyn Migrator,
) -> Result<(), String> {
    let progress = migrator.progress(migration.apply_reverse).await?;
    store::update_progress(pool, migration.id, progress).await?;
    migration.progress = progress;
    Ok(())
}

/// `migrations.currentVersion`.
pub fn current_version() -> Result<Version, String> {
    let raw_version = crate::telemetry::VERSION;
    if !is_dev(raw_version) {
        return parse_version(raw_version)
            .ok_or_else(|| format!("failed to parse current version: {raw_version:?}"));
    }

    if let Ok(raw_version) = std::env::var("SRC_OOBMIGRATION_CURRENT_VERSION") {
        if !raw_version.is_empty() {
            return new_version_from_string(&raw_version)
                .ok_or_else(|| format!("failed to parse force-supplied version: {raw_version:?}"));
        }
    }

    if is_dev(crate::telemetry::VERSION)
        && std::env::var("BAZEL_SKIP_OOB_INFER_VERSION")
            .map(|value| !value.is_empty())
            .unwrap_or(false)
    {
        return Ok(Version::new(5, 99));
    }

    let version = infer_next_release_version()?;
    tracing::info!(version = %version.to_version_string(), "Using latest tag as current version");
    Ok(version)
}

/// `version.IsDev`.
pub fn is_dev(version: &str) -> bool {
    version == DEV_VERSION
}

/// `currentVersion.parseVersion`: the tagged/app release formats.
pub fn parse_version(raw_version: &str) -> Option<Version> {
    if let Some(version) = new_version_from_string(raw_version) {
        return Some(version);
    }

    let mut parts: Vec<&str> = raw_version.split('_').collect();
    if parts.last() == Some(&"patch") {
        parts.pop();
    }
    let last = parts.last()?;
    let candidate = last.split('-').next().unwrap_or("");
    new_version_from_string(candidate)
}

/// `inferNextReleaseVersion`: the minor after the highest `v*` tag in the
/// current working directory.
pub fn infer_next_release_version() -> Result<Version, String> {
    let working_directory = std::env::current_dir().map_err(|err| err.to_string())?;
    let output = std::process::Command::new("git")
        .args(["tag", "--list", "v*"])
        .current_dir(&working_directory)
        .output()
        .map_err(|err| err.to_string())?;
    if !output.status.success() {
        return Err(String::from_utf8_lossy(&output.stderr).into_owned());
    }

    let stdout = String::from_utf8_lossy(&output.stdout);
    let mut tag_map: std::collections::HashSet<String> = std::collections::HashSet::new();
    for tag in stdout.split('\n') {
        // Strip off rc suffix if it exists.
        let tag = tag.split('-').next().unwrap_or("");
        if let Some(version) = new_version_from_string(tag) {
            tag_map.insert(version.to_version_string());
        }
    }

    let mut versions: Vec<Version> = tag_map
        .iter()
        .filter_map(|tag| new_version_from_string(tag))
        .collect();
    sort_versions(&mut versions);

    let Some(last) = versions.last() else {
        return Err("failed to find tagged version".to_owned());
    };

    Ok(last.next())
}

/// `upgradestore.GetFirstServiceVersion`.
pub async fn first_service_version(pool: &PgPool) -> Result<Option<String>, String> {
    let result =
        sqlx::query_scalar::<_, String>("SELECT first_version FROM versions WHERE service = $1")
            .bind("frontend")
            .fetch_optional(pool)
            .await;

    match result {
        Ok(value) => Ok(value),
        Err(sqlx::Error::Database(db)) if db.code().as_deref() == Some("42P01") => Ok(None),
        Err(err) => Err(err.to_string()),
    }
}

/// `ValidateOutOfBandMigrationRunner`.
pub async fn validate_out_of_band_migration_runner(
    pool: &PgPool,
    runner: &Runner,
) -> Result<(), String> {
    let current_raw = crate::telemetry::VERSION;
    if is_dev(current_raw) {
        return Ok(());
    }

    let Some(current_version) = semver_major_minor(current_raw) else {
        tracing::warn!(
            version = current_raw,
            "Skipping out-of-band migrations check"
        );
        return Ok(());
    };

    let first_raw = first_service_version(pool)
        .await
        .map_err(|err| format!("failed to retrieve first instance version: {err}"))?;
    let Some(first_raw) = first_raw else {
        return Ok(());
    };

    let Some(first_version) = semver_major_minor(&first_raw) else {
        tracing::warn!(
            version = current_raw,
            "Skipping out-of-band migrations check"
        );
        return Ok(());
    };

    runner.validate(current_version, first_version).await
}

/// The `(major, minor)` of a semver-shaped instance version; `None` when the
/// string cannot be parsed (the baseline logs and skips the check).
fn semver_major_minor(version: &str) -> Option<Version> {
    new_version_from_string(version.trim())
}

fn strip_jsonc_and_parse<T: serde::de::DeserializeOwned>(raw: &str) -> Result<T, String> {
    let stripped = sg_config::strip_jsonc(raw);
    serde_json::from_str(&stripped).map_err(|err| err.to_string())
}

#[derive(serde::Deserialize)]
struct JsonWebhook {
    #[serde(default)]
    secret: String,
}

#[derive(serde::Deserialize)]
struct JsonGitHubGitLabConfig {
    #[serde(default)]
    webhooks: Option<Vec<Option<JsonWebhook>>>,
}

#[derive(serde::Deserialize)]
struct JsonPlugin {
    #[serde(default)]
    webhooks: Option<JsonWebhook>,
}

#[derive(serde::Deserialize)]
struct JsonBitBucketConfig {
    #[serde(default)]
    webhooks: Option<JsonWebhook>,
    #[serde(default)]
    plugin: Option<JsonPlugin>,
}

/// `externalServiceWebhookMigrator`'s `hasWebhooks`.
pub fn config_has_webhooks(kind: &str, raw_config: &str) -> Result<bool, String> {
    match kind.to_uppercase().as_str() {
        "GITHUB" | "GITLAB" => {
            let config: JsonGitHubGitLabConfig = strip_jsonc_and_parse(raw_config)?;
            Ok(config
                .webhooks
                .map(|webhooks| !webhooks.is_empty())
                .unwrap_or(false))
        }
        "BITBUCKETSERVER" => {
            let config: JsonBitBucketConfig = strip_jsonc_and_parse(raw_config)?;
            let has_webhooks = config
                .webhooks
                .as_ref()
                .map(|webhook| !webhook.secret.is_empty())
                .unwrap_or(false);
            let has_plugin_webhooks = config
                .plugin
                .as_ref()
                .and_then(|plugin| plugin.webhooks.as_ref())
                .map(|webhook| !webhook.secret.is_empty())
                .unwrap_or(false);
            Ok(has_webhooks || has_plugin_webhooks)
        }
        _ => Ok(false),
    }
}

/// `batches.externalServiceWebhookMigrator` (id 13).
pub struct ExternalServiceWebhookMigrator {
    pool: PgPool,
    key: Option<Arc<dyn Key>>,
    batch_size: i64,
}

impl ExternalServiceWebhookMigrator {
    pub fn new(pool: PgPool, key: Option<Arc<dyn Key>>, batch_size: i64) -> Self {
        Self {
            pool,
            key,
            batch_size,
        }
    }

    /// `ID`.
    pub fn id(&self) -> i32 {
        13
    }

    /// `Interval`.
    pub fn interval(&self) -> Duration {
        Duration::from_secs(3)
    }
}

#[async_trait]
impl Migrator for ExternalServiceWebhookMigrator {
    async fn progress(&self, _apply_reverse: bool) -> Result<f64, String> {
        sqlx::query_scalar::<_, f64>(
            r#"
SELECT
	CASE c2.count WHEN 0 THEN 1 ELSE
		CAST(c1.count AS float) / CAST(c2.count AS float)
	END
FROM
	(SELECT COUNT(*) AS count FROM external_services WHERE deleted_at IS NULL AND has_webhooks IS NOT NULL) c1,
	(SELECT COUNT(*) AS count FROM external_services WHERE deleted_at IS NULL) c2
"#,
        )
        .fetch_one(&self.pool)
        .await
        .map_err(|err| err.to_string())
    }

    async fn up(&self) -> Result<(), String> {
        let mut tx = self.pool.begin().await.map_err(|err| err.to_string())?;

        let rows = sqlx::query(
            "SELECT id, kind, config, encryption_key_id FROM external_services WHERE deleted_at IS NULL AND has_webhooks IS NULL ORDER BY id LIMIT $1 FOR UPDATE",
        )
        .bind(self.batch_size)
        .fetch_all(&mut *tx)
        .await
        .map_err(|err| err.to_string())?;

        let mut parse_errors: Vec<String> = Vec::new();
        for row in rows {
            use sqlx::Row;
            let id: i64 = row.try_get("id").map_err(|err| err.to_string())?;
            let kind: String = row.try_get("kind").map_err(|err| err.to_string())?;
            let config: String = row.try_get("config").map_err(|err| err.to_string())?;
            let key_id: String = row
                .try_get("encryption_key_id")
                .map_err(|err| err.to_string())?;

            let config = sg_encryption::maybe_decrypt(self.key.as_deref(), &config, &key_id)
                .await
                .map_err(|err| err.to_string())?;

            match config_has_webhooks(&kind, &config) {
                Ok(has_webhooks) => {
                    sqlx::query("UPDATE external_services SET has_webhooks = $1 WHERE id = $2")
                        .bind(has_webhooks)
                        .bind(id)
                        .execute(&mut *tx)
                        .await
                        .map_err(|err| err.to_string())?;
                }
                // Do not fail-fast on parse errors; make progress on the batch.
                Err(err) => parse_errors.push(err),
            }
        }

        let commit_result = tx.commit().await.map_err(|err| err.to_string());

        match (commit_result, parse_errors.is_empty()) {
            (Err(err), true) => Err(err),
            (Err(err), false) => Err(combine_errors(Some(err), parse_errors)),
            (Ok(()), true) => Ok(()),
            (Ok(()), false) => Err(combine_errors(None, parse_errors)),
        }
    }

    async fn down(&self) -> Result<(), String> {
        // non-destructive
        Ok(())
    }
}

/// `errors.CombineErrors`'s message: a single error is printed bare, several
/// are prefixed with the count.
fn combine_errors(first: Option<String>, mut rest: Vec<String>) -> String {
    let mut errors = Vec::new();
    if let Some(first) = first {
        errors.push(first);
    }
    errors.append(&mut rest);

    if errors.len() == 1 {
        return errors.pop().expect("one error");
    }

    format!(
        "{} errors occurred:\n\t* {}",
        errors.len(),
        errors.join("\n\t* ")
    )
}

/// `batches.userRoleAssignmentMigrator` (id 19).
pub struct UserRoleAssignmentMigrator {
    pool: PgPool,
    batch_size: i64,
}

impl UserRoleAssignmentMigrator {
    pub fn new(pool: PgPool, batch_size: i64) -> Self {
        Self { pool, batch_size }
    }

    /// `ID`.
    pub fn id(&self) -> i32 {
        19
    }

    /// `Interval`.
    pub fn interval(&self) -> Duration {
        Duration::from_secs(10)
    }
}

#[async_trait]
impl Migrator for UserRoleAssignmentMigrator {
    async fn progress(&self, _apply_reverse: bool) -> Result<f64, String> {
        sqlx::query_scalar::<_, f64>(
            r#"
WITH system_roles AS MATERIALIZED (
	SELECT id FROM roles WHERE system
)
SELECT
	CASE u1.regular_count WHEN 0 THEN 1 ELSE
		CAST(ur1.count AS FLOAT) / CAST((u1.regular_count + u1.siteadmin_count) AS FLOAT)
	END
FROM
	(SELECT COUNT(1) AS regular_count, COUNT(1) FILTER (WHERE site_admin) AS siteadmin_count from users u) u1,
	(SELECT COUNT(1) AS count FROM user_roles WHERE role_id IN (SELECT id FROM system_roles)) ur1
"#,
        )
        .fetch_one(&self.pool)
        .await
        .map_err(|err| err.to_string())
    }

    async fn up(&self) -> Result<(), String> {
        sqlx::query(
            r#"
WITH user_system_role AS MATERIALIZED (
    SELECT id FROM roles WHERE name = $1
),
site_admin_system_role AS MATERIALIZED (
    SELECT id FROM roles WHERE name = $2
),
-- this query selects all users without the USER role
users_without_user_role AS MATERIALIZED (
	SELECT
		u.id as user_id, role.id AS role_id
	FROM users u,
	(SELECT id FROM user_system_role) AS role
	WHERE NOT EXISTS (SELECT user_id from user_roles ur WHERE ur.user_id = u.id AND ur.role_id = role.id)
	LIMIT $3
	FOR UPDATE SKIP LOCKED
),
-- this query selects all site administrators without the SITE_ADMINISTRATOR role
admins_without_admin_role AS MATERIALIZED (
	SELECT
		u.id as user_id, role.id AS role_id
	FROM users u,
	(SELECT id FROM site_admin_system_role) AS role
	WHERE u.site_admin AND NOT EXISTS (SELECT user_id from user_roles ur WHERE ur.user_id = u.id AND ur.role_id = role.id)
	LIMIT $4
	FOR UPDATE SKIP LOCKED
)
INSERT INTO user_roles (user_id, role_id)
	SELECT user_id, role_id FROM users_without_user_role
		UNION ALL
	SELECT user_id, role_id FROM admins_without_admin_role
ON CONFLICT DO NOTHING
"#,
        )
        .bind(USER_SYSTEM_ROLE)
        .bind(SITE_ADMINISTRATOR_SYSTEM_ROLE)
        .bind(self.batch_size)
        .bind(self.batch_size)
        .execute(&self.pool)
        .await
        .map(|_| ())
        .map_err(|err| err.to_string())
    }

    async fn down(&self) -> Result<(), String> {
        // non-destructive
        Ok(())
    }
}

/// Register the OSS migrators (`register.RegisterOSSMigrators`).
pub fn register_oss_migrators(runner: &Runner, deps: &JobDeps) -> Result<(), String> {
    let webhook_migrator = ExternalServiceWebhookMigrator::new(
        deps.frontend.clone(),
        deps.keyring.external_service_key(),
        50,
    );
    runner.register(
        webhook_migrator.id(),
        Arc::new(webhook_migrator),
        Duration::from_secs(3),
    )?;

    let role_migrator = UserRoleAssignmentMigrator::new(deps.frontend.clone(), 250);
    runner.register(
        role_migrator.id(),
        Arc::new(role_migrator),
        Duration::from_secs(10),
    )
}

/// `cmd/worker`'s `out-of-band-migrations` job.
#[derive(Default)]
pub struct OutOfBandMigrationsJob;

impl OutOfBandMigrationsJob {
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl Job for OutOfBandMigrationsJob {
    fn description(&self) -> String {
        // `func (m *migrator) Description() string { return "" }`
        String::new()
    }

    async fn routines(
        &self,
        deps: &JobDeps,
        _setup_ctx: CancellationToken,
    ) -> Result<Vec<BackgroundRoutine>, String> {
        let runner = Arc::new(Runner::new(deps.frontend.clone()));

        runner.synchronize_metadata().await.map_err(|err| {
            format!("failed to synchronize out of band migration metadata: {err}")
        })?;

        register_oss_migrators(&runner, deps)?;

        if std::env::var("SRC_DISABLE_OOBMIGRATION_VALIDATION")
            .map(|value| !value.is_empty())
            .unwrap_or(false)
        {
            tracing::warn!("Skipping out-of-band migrations check");
        } else {
            validate_out_of_band_migration_runner(&deps.frontend, &runner).await?;
        }

        let current_version = current_version()?;

        let first_version_string = first_service_version(&deps.frontend).await?;
        let first_version = match first_version_string
            .as_deref()
            .and_then(new_version_from_string)
        {
            Some(version) => version,
            // The baseline returns `nil, err` where `err` is nil at this
            // point, i.e. no routines and no error (see §78.3).
            None => return Ok(Vec::new()),
        };

        Ok(vec![BackgroundRoutine::Custom(Arc::new(
            OutOfBandMigrationRunnerRoutine {
                runner,
                current_version,
                first_version,
            },
        ))])
    }
}

/// `outOfBandMigrationRunnerWrapper`: `Start` blocks until cancellation.
pub struct OutOfBandMigrationRunnerRoutine {
    runner: Arc<Runner>,
    current_version: Version,
    first_version: Version,
}

#[async_trait]
impl crate::framework::BlockingRoutine for OutOfBandMigrationRunnerRoutine {
    fn name(&self) -> &'static str {
        "oobmigrationRunner"
    }

    async fn start(&self, shutdown: CancellationToken) {
        self.runner
            .start(self.current_version, self.first_version, shutdown)
            .await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn v(major: i32, minor: i32) -> Version {
        Version::new(major, minor)
    }

    #[test]
    fn parses_version_strings() {
        assert_eq!(new_version_from_string("v1.2.3"), Some(v(1, 2)));
        assert_eq!(new_version_from_string("5.2"), Some(v(5, 2)));
        let (version, patch, ok) = new_version_and_patch_from_string("1.2.3");
        assert!(ok);
        assert_eq!(version, v(1, 2));
        assert_eq!(patch, 3);
        assert_eq!(
            new_version_from_string("5.2+dev"),
            Some(Version::new_dev(5, 2))
        );
        assert_eq!(new_version_from_string("1.2.3-rc.1"), None);
        assert_eq!(
            new_version_from_string("2023.03.23+204874.db2922"),
            Some(v(2023, 3))
        );
    }

    #[test]
    fn next_and_previous_roll_majors() {
        assert_eq!(v(3, 43).next(), v(4, 0));
        assert_eq!(v(4, 5).next(), v(5, 0));
        assert_eq!(v(4, 1).next(), v(4, 2));
        assert_eq!(v(4, 0).previous(), (v(3, 43), true));
        assert_eq!(v(5, 0).previous(), (v(4, 5), true));
        assert_eq!(v(4, 2).previous(), (v(4, 1), true));
        assert_eq!(v(2, 0).previous(), (v(1, 0), false));
    }

    #[test]
    fn upgrade_range_is_inclusive() {
        assert_eq!(
            upgrade_range(v(4, 3), v(4, 5)).expect("range"),
            vec![v(4, 3), v(4, 4), v(4, 5)]
        );
        assert!(upgrade_range(v(4, 6), v(4, 6)).is_err());
    }

    #[test]
    fn schedule_upgrade_covers_deprecations() {
        // Every migration deprecated at 4.0 is covered by the last 3.x
        // version, so a single interrupt suffices (the greedy algorithm does
        // not add a second point at 4.0).
        let interrupts = schedule_migration_interrupts(v(3, 30), v(4, 1)).expect("interrupts");
        assert_eq!(
            interrupts,
            vec![MigrationInterrupt {
                version: v(3, 43),
                migration_ids: vec![1, 2, 4, 5, 7, 13, 14, 15, 16],
            }]
        );
    }

    #[test]
    fn schedule_downgrade_covers_introductions() {
        assert_eq!(
            schedule_migration_interrupts(v(4, 1), v(3, 30)).expect("interrupts"),
            vec![MigrationInterrupt {
                version: v(3, 43),
                migration_ids: vec![13, 14, 15, 16],
            }]
        );

        // Baseline behaviour: a not-yet-deprecated migration introduced inside
        // the downgrade range has an empty synthetic interval
        // (`deprecated = to.Next()`), which no chosen point can cover, so
        // `makeCoveringSet` panics. Reproduced verbatim.
        let panic = std::panic::catch_unwind(|| {
            let _ = schedule_migration_interrupts(v(4, 6), v(4, 0));
        });
        assert!(panic.is_err());
    }

    #[test]
    fn current_version_uses_force_supplied_value() {
        let _guard = ENV_LOCK.lock().expect("env lock");
        std::env::set_var("SRC_OOBMIGRATION_CURRENT_VERSION", "4.5");
        std::env::remove_var("BAZEL_SKIP_OOB_INFER_VERSION");
        assert_eq!(current_version().expect("version"), v(4, 5));
        std::env::set_var("SRC_OOBMIGRATION_CURRENT_VERSION", "nope");
        assert_eq!(
            current_version().expect_err("parse error"),
            "failed to parse force-supplied version: \"nope\""
        );
        std::env::remove_var("SRC_OOBMIGRATION_CURRENT_VERSION");
    }

    #[test]
    fn parse_version_handles_tagged_formats() {
        assert_eq!(parse_version("v4.5.0"), Some(v(4, 5)));
        assert_eq!(parse_version("4.5.1"), Some(v(4, 5)));
    }

    #[test]
    fn config_webhooks_match_baseline() {
        assert!(config_has_webhooks("github", r#"{"webhooks":[{"secret":"s"}]}"#).expect("github"));
        assert!(!config_has_webhooks("github", r#"{"webhooks":null}"#).expect("github"));
        assert!(config_has_webhooks("gitlab", "{\"webhooks\": [").is_err());
        assert!(
            config_has_webhooks("bitbucketserver", r#"{"webhooks":{"secret":"s"}}"#)
                .expect("bitbucket")
        );
        assert!(config_has_webhooks(
            "bitbucketserver",
            r#"{"plugin":{"webhooks":{"secret":"s"}}}"#
        )
        .expect("bitbucket plugin"));
        assert!(
            !config_has_webhooks("bitbucketserver", r#"{"webhooks":{"secret":""}}"#)
                .expect("bitbucket")
        );
        assert!(!config_has_webhooks("other", "not json").expect("unknown kind"));
    }

    #[test]
    fn validate_reports_unfinished_migrations() {
        let now = sqlx::types::time::OffsetDateTime::now_utc();
        let migration =
            |id: i32, introduced: (i32, i32), deprecated: Option<(i32, i32)>, progress| Migration {
                id,
                team: "t".to_owned(),
                component: "c".to_owned(),
                description: "d".to_owned(),
                introduced_major: introduced.0,
                introduced_minor: introduced.1,
                deprecated_major: deprecated.map(|d| d.0),
                deprecated_minor: deprecated.map(|d| d.1),
                progress,
                created: now,
                last_updated: None,
                non_destructive: true,
                is_enterprise: false,
                apply_reverse: false,
                metadata: serde_json::Value::Null,
                errors: Vec::new(),
            };

        let message = validate_migrations(
            &[migration(19, (4, 5), Some((4, 6)), 0.5)],
            v(4, 6),
            v(4, 0),
        )
        .expect_err("unfinished");
        assert_eq!(
            message,
            "Unfinished migrations. Please revert Sourcegraph to the previous version and wait for the following migrations to complete.\n\n  - migration 19 expected to be at 100.00% (at 50.00%)\n\n"
        );

        validate_migrations(
            &[migration(19, (4, 5), Some((4, 6)), 1.0)],
            v(4, 6),
            v(4, 0),
        )
        .expect("finished");

        // Booting on or after the deprecation version skips the check.
        validate_migrations(
            &[migration(19, (4, 5), Some((4, 6)), 0.5)],
            v(4, 6),
            v(4, 6),
        )
        .expect("fresh instance");
    }

    #[test]
    fn should_run_matches_runner_predicate() {
        let now = sqlx::types::time::OffsetDateTime::now_utc();
        let migration = Migration {
            id: 19,
            team: "t".to_owned(),
            component: "c".to_owned(),
            description: "d".to_owned(),
            introduced_major: 4,
            introduced_minor: 5,
            deprecated_major: Some(4),
            deprecated_minor: Some(6),
            progress: 0.0,
            created: now,
            last_updated: None,
            non_destructive: true,
            is_enterprise: false,
            apply_reverse: false,
            metadata: serde_json::Value::Null,
            errors: Vec::new(),
        };
        assert!(!should_run(v(4, 4), v(4, 0), &migration));
        assert!(should_run(v(4, 5), v(4, 0), &migration));
        assert!(!should_run(v(4, 6), v(4, 0), &migration));
        // `first == deprecated` is not "after deprecation", so it still runs.
        assert!(should_run(v(4, 5), v(4, 6), &migration));
        assert!(!should_run(v(4, 5), v(4, 7), &migration));
    }

    #[test]
    fn combined_errors_match_go_multi_error() {
        assert_eq!(combine_errors(None, vec!["one".to_owned()]), "one");
        assert_eq!(
            combine_errors(
                Some("commit".to_owned()),
                vec!["a".to_owned(), "b".to_owned()]
            ),
            "3 errors occurred:\n\t* commit\n\t* a\n\t* b"
        );
    }

    static ENV_LOCK: Mutex<()> = Mutex::new(());
}
