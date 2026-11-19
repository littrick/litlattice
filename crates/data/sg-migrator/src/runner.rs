#![forbid(unsafe_code)]

//! `internal/database/migration/runner`: apply/revert migrations and validate
//! schema state.
//!
//! The up path mirrors Go's `runSchema`/`applyMigrations` loop: each pass holds
//! the schema advisory lock, re-reads the migration logs, and applies as much
//! as it can. `CREATE INDEX CONCURRENTLY` migrations inspect `pg_index` /
//! `pg_stat_progress_create_index`, skip creation when a valid index already
//! exists, repair an invalid idle index, and release the advisory lock before
//! creating the index (then re-acquire it for the remaining definitions).

use std::collections::BTreeSet;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use base64::Engine as _;
use sha1::{Digest, Sha1};
use sqlx::Row as _;

use crate::error::MigratorError;
use crate::store::{LockGuard, MigrationVersions, Store};
use crate::types::{Definition, Schema};

/// Go's `indexPollInterval` (`run.go`): wait between system-catalog polls while
/// an index creation is in progress.
const INDEX_POLL_INTERVAL: Duration = Duration::from_secs(5);

/// `store.indexStatusQuery` (`store.go`), with stable column aliases.
const INDEX_STATUS_QUERY: &str = r#"
SELECT
	pi.indisvalid AS is_valid,
	pi.indisready AS is_ready,
	pi.indislive AS is_live,
	p.phase AS phase,
	p.lockers_total AS lockers_total,
	p.lockers_done AS lockers_done,
	p.blocks_total AS blocks_total,
	p.blocks_done AS blocks_done,
	p.tuples_total AS tuples_total,
	p.tuples_done AS tuples_done
FROM pg_catalog.pg_stat_all_indexes ai
JOIN pg_catalog.pg_index pi ON pi.indexrelid = ai.indexrelid
LEFT JOIN pg_catalog.pg_stat_progress_create_index p ON p.relid = ai.relid AND p.index_relid = ai.indexrelid
WHERE
	ai.relname = $1 AND
	ai.indexrelname = $2
"#;

/// `shared.IndexStatus` (`shared/types.go`): validity flags plus the progress
/// of an in-flight `CREATE INDEX CONCURRENTLY`, if any.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct IndexStatus {
    is_valid: bool,
    is_ready: bool,
    is_live: bool,
    phase: Option<String>,
    lockers_done: Option<i64>,
    lockers_total: Option<i64>,
    blocks_done: Option<i64>,
    blocks_total: Option<i64>,
    tuples_done: Option<i64>,
    tuples_total: Option<i64>,
}

/// What [`Runner::create_index_concurrently`] should do for the observed
/// index state. Mirrors the branches in Go's `createIndexConcurrently`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum IndexPlan {
    /// Index exists and is valid: record a successful log, run no SQL.
    AlreadyValid,
    /// Index exists but is invalid and no creation is in progress: drop it via
    /// the down query, then re-create.
    RepairThenCreate,
    /// Index exists and another session is creating it: wait and re-poll.
    WaitForCreation,
    /// Index is missing: create it.
    Create,
}

fn classify_index_status(exists: bool, status: &IndexStatus) -> IndexPlan {
    if exists && status.is_valid {
        IndexPlan::AlreadyValid
    } else if exists && status.phase.is_none() {
        IndexPlan::RepairThenCreate
    } else if exists {
        IndexPlan::WaitForCreation
    } else {
        IndexPlan::Create
    }
}

/// `42P07` (`duplicate_table`) means another migrator created the index first.
fn is_duplicate_object_error(error: &sqlx::Error) -> bool {
    matches!(
        error,
        sqlx::Error::Database(database_error)
            if database_error.code().as_deref() == Some("42P07")
    )
}

fn decode_index_status_error(error: sqlx::Error) -> MigratorError {
    MigratorError::new(format!("failed to decode index status: {error}"))
}

/// Best-effort port of Go's `logIndexStatus`; progress is only rendered while a
/// creation operation is visible in `pg_stat_progress_create_index`.
fn log_index_status(
    schema: &str,
    table_name: &str,
    index_name: &str,
    status: &IndexStatus,
    exists: bool,
) {
    match &status.phase {
        Some(phase) => tracing::info!(
            schema = %schema,
            table_name = %table_name,
            index_name = %index_name,
            exists,
            is_valid = status.is_valid,
            is_ready = status.is_ready,
            is_live = status.is_live,
            phase = %phase,
            lockers = ?(status.lockers_done, status.lockers_total),
            blocks = ?(status.blocks_done, status.blocks_total),
            tuples = ?(status.tuples_done, status.tuples_total),
            "Checked progress of index creation"
        ),
        None => tracing::info!(
            schema = %schema,
            table_name = %table_name,
            index_name = %index_name,
            exists,
            is_valid = status.is_valid,
            is_ready = status.is_ready,
            is_live = status.is_live,
            "Checked index status"
        ),
    }
}

/// `filterAppliedDefinitions`: keep the definitions that still need work in the
/// requested direction, preserving order.
fn filter_applied_definitions(
    definitions: &[Definition],
    applied: &[i32],
    up: bool,
) -> Vec<Definition> {
    let applied: BTreeSet<i32> = applied.iter().copied().collect();
    definitions
        .iter()
        .filter(|definition| applied.contains(&definition.id) != up)
        .cloned()
        .collect()
}

/// `runner.PrivilegedMode`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum PrivilegedMode {
    /// Apply privileged migrations like any other (default).
    #[default]
    Apply,
    /// Skip privileged migrations but record a successful log entry.
    Noop,
    /// Fail-fast when a privileged migration would be applied.
    Refuse,
}

/// Options shared by `up`/`upto`/`downto`.
#[derive(Debug, Clone, Default)]
pub struct RunOptions {
    pub privileged_mode: PrivilegedMode,
    /// `-privileged-hash` (only meaningful with [`PrivilegedMode::Noop`]).
    pub privileged_hash: Option<String>,
    pub ignore_single_dirty_log: bool,
    pub ignore_single_pending_log: bool,
}

/// One schema plus its database.
pub struct SchemaStore {
    pub schema: Schema,
    pub store: Store,
}

/// `runner.Runner` over a fixed set of schemas.
pub struct Runner {
    pub schemas: Vec<SchemaStore>,
}

impl Runner {
    pub fn new(schemas: Vec<SchemaStore>) -> Self {
        Self { schemas }
    }

    fn selected(&self, names: &[String]) -> Vec<&SchemaStore> {
        if names.is_empty() || names.iter().any(|name| name == "all") {
            return self.schemas.iter().collect();
        }
        self.schemas
            .iter()
            .filter(|entry| names.iter().any(|name| name == &entry.schema.name))
            .collect()
    }

    /// `up` (no target) / `upto` (with a target version).
    pub async fn up(
        &self,
        schema_names: &[String],
        target: Option<i32>,
        options: &RunOptions,
    ) -> Result<(), MigratorError> {
        for entry in self.selected(schema_names) {
            self.run_schema(entry, Direction::Up, target, options).await?;
        }
        Ok(())
    }

    /// `downto` — target is required.
    pub async fn down_to(
        &self,
        schema_names: &[String],
        target: i32,
        options: &RunOptions,
    ) -> Result<(), MigratorError> {
        for entry in self.selected(schema_names) {
            self.run_schema(entry, Direction::Down, Some(target), options).await?;
        }
        Ok(())
    }

    /// `add-log` (`cliutil/addlog.go`): append a completed migration log row
    /// to every selected schema, running no migration SQL.
    ///
    /// Go's `setupStore` resolves the store through the runner's factory,
    /// which runs `connections.initStore` (`EnsureSchemaTable` followed by
    /// `BackfillSchemaVersions`) before the command calls `WithMigrationLog`
    /// with a no-op callback. This mirrors that path per selected schema via
    /// [`prepare_store`] and [`Store::add_log`].
    ///
    /// Schemas are chosen with [`Runner::selected`], like the other commands;
    /// the first error aborts the loop. As in Go, no advisory lock is taken
    /// and duplicate versions are not detected.
    pub async fn add_log(
        &self,
        schema_names: &[String],
        version: i32,
        up: bool,
    ) -> Result<(), MigratorError> {
        for entry in self.selected(schema_names) {
            prepare_store(entry).await?;
            entry.store.add_log(version, up).await?;
        }
        Ok(())
    }

    /// `validate`: every leaf migration must be applied, with no dirty state.
    pub async fn validate(&self, schema_names: &[String]) -> Result<(), MigratorError> {
        let options = RunOptions {
            privileged_mode: PrivilegedMode::Refuse,
            ..RunOptions::default()
        };
        for entry in self.selected(schema_names) {
            prepare_store(entry).await?;
            let version = entry.store.versions().await?;
            check_dirty(&entry.schema, &version, &options)?;
            let leaves: Vec<i32> = entry
                .schema
                .definitions
                .leaves()
                .into_iter()
                .map(|definition| definition.id)
                .collect();
            let missing = entry.schema.definitions.up(&version.applied, &leaves)?;
            if !missing.is_empty() {
                let ids: Vec<String> = missing.iter().map(|definition| definition.id.to_string()).collect();
                return Err(MigratorError::new(instructional(
                    "schema out of date",
                    &format!(
                        "schema {:?} requires the following migrations to be applied: {}\n",
                        entry.schema.name,
                        ids.join(", ")
                    ),
                    &[
                        "This software expects a migrator instance to have run on this schema prior to the deployment of this process.",
                        "If this error is occurring directly after an upgrade, roll back your instance to the previous version and ensure the migrator instance runs successfully prior attempting to re-upgrade.",
                    ],
                )));
            }
        }
        Ok(())
    }

    async fn run_schema(
        &self,
        entry: &SchemaStore,
        direction: Direction,
        target: Option<i32>,
        options: &RunOptions,
    ) -> Result<(), MigratorError> {
        prepare_store(entry).await?;

        // Compute the delta once from the current view of the schema. Each pass
        // of `apply_migrations` re-reads versions and re-filters this set, so a
        // concurrent migrator's progress is observed.
        let versions = entry.store.versions().await?;
        let (definitions, up) = match direction {
            Direction::Up => {
                let targets = match target {
                    Some(id) => vec![id],
                    None => entry
                        .schema
                        .definitions
                        .leaves()
                        .into_iter()
                        .map(|definition| definition.id)
                        .collect(),
                };
                (entry.schema.definitions.up(&versions.applied, &targets)?, true)
            }
            Direction::Down => {
                let target = target.ok_or_else(|| {
                    MigratorError::new("downto requires a target migration version")
                })?;
                (
                    entry.schema.definitions.down(&versions.applied, &[target])?,
                    false,
                )
            }
        };

        // Mirror Go's `runSchema` retry loop: a pass ends early when it must
        // release the advisory lock to create an index concurrently (or while
        // waiting for another migrator's index creation). Re-acquire and apply
        // the remainder.
        while self.apply_migrations(entry, &definitions, up, options).await? {}
        Ok(())
    }

    /// A single iteration of Go's `applyMigrations`.
    ///
    /// Returns `true` when the caller should run another pass: either because
    /// this pass released the advisory lock to create an index concurrently, or
    /// because another migrator is actively creating an index (in which case
    /// the pass also sleeps before returning).
    async fn apply_migrations(
        &self,
        entry: &SchemaStore,
        definitions: &[Definition],
        up: bool,
        options: &RunOptions,
    ) -> Result<bool, MigratorError> {
        let mut guard = Some(entry.store.lock().await?);
        let mut retry = false;
        let mut dropped_lock = false;

        let result: Result<(), MigratorError> = async {
            // Re-fetch the schema state now that we hold the lock; another
            // migrator may have made progress since our initial read.
            let versions = entry.store.versions().await?;

            // Detect dirty state, but tolerate pending logs that correspond to
            // an index creation in progress (another migrator will finish it).
            if self
                .schema_state_requires_retry(entry, &versions, options)
                .await?
            {
                retry = true;
                return Ok(());
            }

            let remaining = filter_applied_definitions(definitions, &versions.applied, up);
            if remaining.is_empty() {
                return Ok(());
            }

            self.check_privileged(entry, &remaining, up, options)?;

            for definition in &remaining {
                if up && definition.is_create_index_concurrently {
                    // Handle `CREATE INDEX CONCURRENTLY` specially. When this
                    // returns `true` the lock has been released; stop applying
                    // and let the next pass re-acquire it.
                    if self
                        .create_index_concurrently(entry, definition, &mut guard)
                        .await?
                    {
                        dropped_lock = true;
                        return Ok(());
                    }
                } else {
                    // All other migrations apply uniformly under the lock.
                    self.apply(entry, definition, up, options).await?;
                }
            }
            Ok(())
        }
        .await;

        // Release the lock if the concurrent-index path did not already do so.
        let unlock = match guard.take() {
            Some(guard) => guard.unlock().await,
            None => Ok(()),
        };
        combine_run(result, unlock)?;

        if retry {
            // An index creation is in progress elsewhere. Wait before
            // re-querying so we do not flood the system catalog.
            tokio::time::sleep(INDEX_POLL_INTERVAL).await;
            return Ok(true);
        }

        Ok(dropped_lock)
    }

    /// Go's `validateSchemaState` (the retry-relevant portions): returns `true`
    /// when the only pending migrations are concurrent-index definitions whose
    /// creation is visibly in progress, meaning the caller should wait and
    /// re-invoke. Failed logs (and dead pending logs) surface as dirty errors.
    async fn schema_state_requires_retry(
        &self,
        entry: &SchemaStore,
        versions: &MigrationVersions,
        options: &RunOptions,
    ) -> Result<bool, MigratorError> {
        // Failed logs require administrator intervention; reuse the existing
        // error text (honoring `ignore_single_dirty_log`).
        let failed_only = MigrationVersions {
            applied: versions.applied.clone(),
            pending: Vec::new(),
            failed: versions.failed.clone(),
        };
        check_dirty(&entry.schema, &failed_only, options)?;

        if versions.pending.is_empty() || options.ignore_single_pending_log {
            return Ok(false);
        }

        let mut active = false;
        let mut dead: Vec<i32> = Vec::new();
        for version in &versions.pending {
            let definition = entry.schema.definitions.get_by_id(*version);
            let mut active_creation = false;
            if let Some(definition) = definition {
                if definition.is_create_index_concurrently {
                    if let Some((table_name, index_name)) =
                        crate::definition::index_metadata(&definition.up_sql)
                    {
                        let (status, exists) =
                            self.index_status(entry, &table_name, &index_name).await?;
                        log_index_status(
                            &entry.schema.name,
                            &table_name,
                            &index_name,
                            &status,
                            exists,
                        );
                        active_creation = exists && status.phase.is_some();
                    }
                }
            }

            if active_creation {
                active = true;
            } else {
                dead.push(*version);
            }
        }

        if !dead.is_empty() {
            let pending_only = MigrationVersions {
                applied: Vec::new(),
                pending: dead,
                failed: Vec::new(),
            };
            check_dirty(&entry.schema, &pending_only, options)?;
        }

        Ok(active)
    }

    /// Go's `createIndexConcurrently`. Queries the target index's status,
    /// records a log without executing SQL when it already exists and is valid,
    /// repairs an invalid idle index, and otherwise releases the advisory lock
    /// and (re)creates the index outside a transaction.
    ///
    /// Returns `true` when the advisory lock was released; the caller must then
    /// re-acquire it on the next pass before applying more migrations.
    async fn create_index_concurrently(
        &self,
        entry: &SchemaStore,
        definition: &Definition,
        guard: &mut Option<LockGuard>,
    ) -> Result<bool, MigratorError> {
        let (table_name, index_name) =
            crate::definition::index_metadata(&definition.up_sql).ok_or_else(|| {
                MigratorError::new(format!(
                    "failed to determine index metadata for concurrent migration {}",
                    definition.id
                ))
            })?;

        let mut unlocked = false;
        loop {
            let (status, exists) = self.index_status(entry, &table_name, &index_name).await?;
            log_index_status(&entry.schema.name, &table_name, &index_name, &status, exists);

            let plan = classify_index_status(exists, &status);
            match plan {
                IndexPlan::AlreadyValid => {
                    // Index exists and is valid. It may have been created out
                    // of band, so still record a successful migration log.
                    let version = definition.id;
                    entry
                        .store
                        .with_migration_log(version, true, |_pool| async { Ok(()) })
                        .await
                        .map_err(|error| {
                            MigratorError::new(format!(
                                "failed to create migration log {version}: {error}"
                            ))
                        })?;
                    return Ok(unlocked);
                }
                IndexPlan::RepairThenCreate => {
                    // Invalid index with no creation in progress: drop it via
                    // the down query (while still holding the lock), then
                    // re-create below.
                    self.drop_invalid_index(entry, definition).await?;
                }
                IndexPlan::WaitForCreation | IndexPlan::Create => {}
            }

            // Concurrent index creation cannot proceed while this session
            // holds an advisory lock that other transactions may be waiting
            // on. Release it (idempotent: only the first release acts).
            if let Some(held) = guard.take() {
                held.unlock().await?;
            }
            unlocked = true;

            if plan == IndexPlan::WaitForCreation {
                // Another session is creating the index; wait and re-poll.
                tokio::time::sleep(INDEX_POLL_INTERVAL).await;
                continue;
            }

            // Create the index. If another migrator wins the race (42P07), do
            // not move on: re-poll until the index is usable.
            if self
                .create_index(entry, definition, &table_name, &index_name)
                .await?
            {
                continue;
            }

            return Ok(true);
        }
    }

    /// Drop an invalid index using the migration's down query, recording a
    /// (successful) down log as Go's repair path does.
    async fn drop_invalid_index(
        &self,
        entry: &SchemaStore,
        definition: &Definition,
    ) -> Result<(), MigratorError> {
        let sql = definition.down_sql.clone();
        let version = definition.id;
        entry
            .store
            .with_migration_log(version, false, move |pool| async move {
                let mut tx = pool.begin().await.map_err(|error| {
                    MigratorError::new(format!(
                        "failed to open transaction to drop invalid index for migration {version}: {error}"
                    ))
                })?;
                sqlx::raw_sql(&sql).execute(&mut *tx).await.map_err(|error| {
                    MigratorError::new(format!(
                        "failed to drop invalid index for migration {version}:\n```\n{sql}\n```\n{error}"
                    ))
                })?;
                tx.commit().await.map_err(|error| {
                    MigratorError::new(format!(
                        "failed to commit index drop for migration {version}: {error}"
                    ))
                })
            })
            .await
    }

    /// Execute the `CREATE INDEX CONCURRENTLY` statement outside a transaction
    /// and record the migration log. Returns `true` when the statement raced
    /// with another migrator (`42P07`), meaning the caller should re-poll.
    async fn create_index(
        &self,
        entry: &SchemaStore,
        definition: &Definition,
        table_name: &str,
        index_name: &str,
    ) -> Result<bool, MigratorError> {
        let sql = definition.up_sql.clone();
        let version = definition.id;
        let raced = Arc::new(AtomicBool::new(false));
        let raced_in_closure = Arc::clone(&raced);

        tracing::info!(
            schema = %entry.schema.name,
            migration_id = version,
            table_name = %table_name,
            index_name = %index_name,
            "Creating index concurrently"
        );

        entry
            .store
            .with_migration_log(version, true, move |pool| async move {
                match sqlx::raw_sql(&sql).execute(&pool).await {
                    Ok(_) => Ok(()),
                    // Another migrator created the index first. Swallow the
                    // duplicate-object error so we do not write a failed log,
                    // and let the caller re-poll the catalog.
                    Err(error) if is_duplicate_object_error(&error) => {
                        raced_in_closure.store(true, Ordering::SeqCst);
                        Ok(())
                    }
                    Err(error) => Err(MigratorError::new(format!(
                        "failed to apply migration {version}:\n```\n{sql}\n```\n{error}"
                    ))),
                }
            })
            .await?;

        Ok(raced.load(Ordering::SeqCst))
    }

    /// `Store.IndexStatus` (`store.go:420-447`): query the validity flags and,
    /// if a creation is in flight, its progress. The boolean is Go's `exists`
    /// flag (whether a matching index row was found).
    async fn index_status(
        &self,
        entry: &SchemaStore,
        table_name: &str,
        index_name: &str,
    ) -> Result<(IndexStatus, bool), MigratorError> {
        let row = sqlx::query(INDEX_STATUS_QUERY)
            .bind(table_name)
            .bind(index_name)
            .fetch_optional(&entry.store.pool)
            .await
            .map_err(|error| {
                MigratorError::new(format!(
                    "failed to query state of index {table_name:?}.{index_name:?}: {error}"
                ))
            })?;

        let Some(row) = row else {
            return Ok((IndexStatus::default(), false));
        };

        let status = IndexStatus {
            is_valid: row.try_get("is_valid").map_err(decode_index_status_error)?,
            is_ready: row.try_get("is_ready").map_err(decode_index_status_error)?,
            is_live: row.try_get("is_live").map_err(decode_index_status_error)?,
            phase: row.try_get("phase").map_err(decode_index_status_error)?,
            lockers_done: row
                .try_get("lockers_done")
                .map_err(decode_index_status_error)?,
            lockers_total: row
                .try_get("lockers_total")
                .map_err(decode_index_status_error)?,
            blocks_done: row
                .try_get("blocks_done")
                .map_err(decode_index_status_error)?,
            blocks_total: row
                .try_get("blocks_total")
                .map_err(decode_index_status_error)?,
            tuples_done: row
                .try_get("tuples_done")
                .map_err(decode_index_status_error)?,
            tuples_total: row
                .try_get("tuples_total")
                .map_err(decode_index_status_error)?,
        };

        Ok((status, true))
    }

    fn check_privileged(
        &self,
        entry: &SchemaStore,
        definitions: &[Definition],
        up: bool,
        options: &RunOptions,
    ) -> Result<(), MigratorError> {
        if options.privileged_mode == PrivilegedMode::Apply
            || (options.privileged_mode == PrivilegedMode::Refuse && !up)
        {
            return Ok(());
        }
        let privileged: Vec<&Definition> =
            definitions.iter().filter(|definition| definition.privileged).collect();
        if privileged.is_empty() {
            return Ok(());
        }
        let ids: Vec<String> = privileged.iter().map(|definition| definition.id.to_string()).collect();

        if options.privileged_mode == PrivilegedMode::Refuse {
            return Err(MigratorError::new(instructional(
                "refusing to apply a privileged migration",
                &format!(
                    "schema {:?} requires database {} to be applied by a database user with elevated permissions\n",
                    entry.schema.name,
                    humanize_migrations(&ids)
                ),
                &[
                    "The migration runner is currently being run with -unprivileged-only.",
                    "The indicated migration is marked as privileged and cannot be applied by this invocation of the migration runner.",
                    "Before re-invoking the migration runner, follow the instructions on https://sourcegraph.com/docs/admin/how-to/privileged_migrations.",
                    "Please contact support@sourcegraph.com for further assistance.",
                ],
            )));
        }

        let hash = hash_definition_ids(&privileged);
        if up {
            let supplied = options.privileged_hash.as_deref().unwrap_or("");
            if supplied != hash {
                let sql: String = privileged
                    .iter()
                    .map(|definition| definition.up_sql.clone())
                    .collect::<Vec<_>>()
                    .join("\n");
                return Err(MigratorError::new(format!(
                    "refusing to apply a privileged migration: apply the following SQL and re-run with the added flag `-privileged-hash={hash}` to continue.\n\n```\n{sql}\n```\n"
                )));
            }
        }
        Ok(())
    }

    async fn apply(
        &self,
        entry: &SchemaStore,
        definition: &Definition,
        up: bool,
        options: &RunOptions,
    ) -> Result<(), MigratorError> {
        let noop = options.privileged_mode == PrivilegedMode::Noop && definition.privileged;
        if noop {
            return entry
                .store
                .with_migration_log(definition.id, up, |_pool| async { Ok(()) })
                .await
                .map_err(|error| {
                    MigratorError::new(format!(
                        "failed to create migration log {}: {error}",
                        definition.id
                    ))
                });
        }

        let sql = if up {
            definition.up_sql.clone()
        } else {
            definition.down_sql.clone()
        };
        let concurrent = definition.is_create_index_concurrently;
        let version = definition.id;
        entry
            .store
            .with_migration_log(version, up, move |pool| async move {
                if concurrent {
                    sqlx::raw_sql(&sql)
                        .execute(&pool)
                        .await
                        .map(|_| ())
                        .map_err(|error| {
                            MigratorError::new(format!(
                                "failed to apply migration {version}:\n```\n{sql}\n```\n{error}"
                            ))
                        })
                } else {
                    let mut tx = pool.begin().await.map_err(|error| {
                        MigratorError::new(format!(
                            "failed to open transaction for migration {version}: {error}"
                        ))
                    })?;
                    sqlx::raw_sql(&sql).execute(&mut *tx).await.map_err(|error| {
                        MigratorError::new(format!(
                            "failed to apply migration {version}:\n```\n{sql}\n```\n{error}"
                        ))
                    })?;
                    tx.commit().await.map_err(|error| {
                        MigratorError::new(format!(
                            "failed to commit migration {version}: {error}"
                        ))
                    })
                }
            })
            .await
    }
}

/// `connections.initStore`: ensure `migration_logs` exists and backfill the
/// historical (squashed) versions from the stitched graph.
async fn prepare_store(entry: &SchemaStore) -> Result<(), MigratorError> {
    entry.store.ensure_migration_logs_table().await?;
    if let Some(stitched) = crate::stitched::for_schema(&entry.schema.name)? {
        let bounds = stitched.bounds();
        entry
            .store
            .backfill_schema_versions(crate::store::StitchedGraph {
                definitions: &stitched.definitions,
                bounds_by_rev: &bounds,
            })
            .await?;
    }
    Ok(())
}

/// Migration direction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Direction {
    Up,
    Down,
}

/// Combine a pass's work result with the best-effort lock release, preserving
/// both errors as Go's deferred `unlock(err)` does.
fn combine_run(
    result: Result<(), MigratorError>,
    unlock: Result<(), MigratorError>,
) -> Result<(), MigratorError> {
    match (result, unlock) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(error), Ok(())) => Err(error),
        (Ok(()), Err(error)) => Err(error),
        (Err(error), Err(unlock_error)) => Err(MigratorError::new(format!(
            "{error}; additionally failed to release advisory lock: {unlock_error}"
        ))),
    }
}

fn check_dirty(
    schema: &Schema,
    versions: &MigrationVersions,
    options: &RunOptions,
) -> Result<(), MigratorError> {
    if !versions.failed.is_empty() {
        if options.ignore_single_dirty_log && versions.failed.len() == 1 {
            return Ok(());
        }
        let ids: Vec<String> = versions.failed.iter().map(i32::to_string).collect();
        return Err(MigratorError::new(instructional(
            "dirty database",
            &format!(
                "schema {:?} marked the following migrations as failed: {}\n",
                schema.name,
                ids.join(", ")
            ),
            &[
                "The target schema is marked as dirty and no other migration operation is seen running on this schema.",
                "The last migration operation over this schema has failed (or, at least, the migrator instance issuing that migration has died).",
                "Please contact support@sourcegraph.com for further assistance.",
            ],
        )));
    }
    if !versions.pending.is_empty() && !options.ignore_single_pending_log {
        let ids: Vec<String> = versions.pending.iter().map(i32::to_string).collect();
        return Err(MigratorError::new(format!(
            "schema {:?} has pending migrations: {}",
            schema.name,
            ids.join(", ")
        )));
    }
    Ok(())
}

/// `instructionalError.Error`.
fn instructional(class: &str, description: &str, instructions: &[&str]) -> String {
    format!(
        "{}: {}\n\n{}\n",
        class,
        description,
        instructions.join(" ")
    )
}

/// `hashDefinitionIDs`: base64(SHA1("id1,id2,...")).
pub fn hash_definition_ids(definitions: &[&Definition]) -> String {
    let joined: Vec<String> = definitions.iter().map(|definition| definition.id.to_string()).collect();
    let mut hasher = Sha1::new();
    hasher.update(joined.join(",").as_bytes());
    base64::engine::general_purpose::STANDARD.encode(hasher.finalize())
}

/// `humanizeList("migration", "migrations", ids)`.
fn humanize_migrations(values: &[String]) -> String {
    match values.len() {
        0 => String::new(),
        1 => format!("migration {}", values[0]),
        2 => format!("migrations {} and {}", values[0], values[1]),
        _ => {
            let mut owned: Vec<String> = values.to_vec();
            let last = owned.len() - 1;
            owned[last] = format!("and {}", owned[last]);
            format!("migrations {}", owned.join(", "))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn definition(id: i32, parents: Vec<i32>, privileged: bool) -> Definition {
        Definition {
            id,
            name: format!("m{id}"),
            up_sql: format!("SELECT {id}"),
            down_sql: format!("SELECT -{id}"),
            privileged,
            parents,
            ..Definition::default()
        }
    }

    fn definitions() -> crate::types::Definitions {
        crate::types::Definitions::new(vec![
            definition(1, vec![], false),
            definition(2, vec![1], false),
            definition(3, vec![2], true),
            definition(4, vec![2], false),
        ])
        .expect("definitions")
    }

    #[test]
    fn up_includes_ancestors_only() {
        let definitions = definitions();
        let ids: Vec<i32> = definitions
            .up(&[1], &[3])
            .expect("up")
            .into_iter()
            .map(|definition| definition.id)
            .collect();
        assert_eq!(ids, vec![2, 3]);

        let all: Vec<i32> = definitions
            .up(&[1], &[3, 4])
            .expect("up")
            .into_iter()
            .map(|definition| definition.id)
            .collect();
        assert_eq!(all, vec![2, 3, 4]);
    }

    #[test]
    fn down_reverts_descendants_in_reverse_order() {
        let definitions = definitions();
        let ids: Vec<i32> = definitions
            .down(&[1, 2, 3, 4], &[2])
            .expect("down")
            .into_iter()
            .map(|definition| definition.id)
            .collect();
        assert_eq!(ids, vec![4, 3]);
    }

    #[test]
    fn unknown_target_is_an_error() {
        let definitions = definitions();
        assert_eq!(
            definitions.up(&[], &[9]).expect_err("unknown").to_string(),
            "unknown migration 9"
        );
        assert_eq!(
            definitions.down(&[], &[9]).expect_err("unknown").to_string(),
            "unknown migration 9"
        );
    }

    #[test]
    fn privileged_hash_matches_go() {
        let one = definition(3, vec![], true);
        let two = definition(4, vec![], true);
        assert_eq!(
            hash_definition_ids(&[&one, &two]),
            "8Q6V8PuvcW85JkrKkaEE+gBUQy4="
        );
    }

    #[test]
    fn humanize_matches_go() {
        assert_eq!(humanize_migrations(&[]), "");
        assert_eq!(humanize_migrations(&["1".into()]), "migration 1");
        assert_eq!(
            humanize_migrations(&["1".into(), "2".into()]),
            "migrations 1 and 2"
        );
        assert_eq!(
            humanize_migrations(&["1".into(), "2".into(), "3".into()]),
            "migrations 1, 2, and 3"
        );
    }

    #[test]
    fn classify_index_status_matches_go() {
        // Existing and valid: skip creation, even if a progress row lingers.
        let valid = IndexStatus {
            is_valid: true,
            ..IndexStatus::default()
        };
        assert_eq!(classify_index_status(true, &valid), IndexPlan::AlreadyValid);
        let valid_with_phase = IndexStatus {
            is_valid: true,
            phase: Some("building index".to_string()),
            ..IndexStatus::default()
        };
        assert_eq!(
            classify_index_status(true, &valid_with_phase),
            IndexPlan::AlreadyValid
        );

        // Invalid with no in-flight creation: drop and re-create.
        let invalid_idle = IndexStatus {
            is_valid: false,
            phase: None,
            ..IndexStatus::default()
        };
        assert_eq!(
            classify_index_status(true, &invalid_idle),
            IndexPlan::RepairThenCreate
        );

        // Invalid with an in-flight creation: wait for the other session.
        let invalid_in_progress = IndexStatus {
            is_valid: false,
            phase: Some("index validation: scanning index".to_string()),
            ..IndexStatus::default()
        };
        assert_eq!(
            classify_index_status(true, &invalid_in_progress),
            IndexPlan::WaitForCreation
        );

        // Missing index: create it.
        assert_eq!(
            classify_index_status(false, &IndexStatus::default()),
            IndexPlan::Create
        );
    }

    #[test]
    fn filter_applied_definitions_respects_direction() {
        let definitions = definitions();
        let all = definitions.all();
        let applied = vec![1, 2];

        let up_ids: Vec<i32> = filter_applied_definitions(all, &applied, true)
            .iter()
            .map(|definition| definition.id)
            .collect();
        assert_eq!(up_ids, vec![3, 4]);

        let down_ids: Vec<i32> = filter_applied_definitions(all, &applied, false)
            .iter()
            .map(|definition| definition.id)
            .collect();
        assert_eq!(down_ids, vec![1, 2]);
    }

    fn schema_store(name: &str) -> SchemaStore {
        let pool = sqlx::postgres::PgPoolOptions::new()
            .connect_lazy("postgres://localhost/unused")
            .expect("lazy pool");
        SchemaStore {
            schema: Schema::new(
                name,
                format!("{name}_schema_migrations"),
                crate::types::Definitions::default(),
            ),
            store: Store::new(pool, name, format!("{name}_schema_migrations")),
        }
    }

    #[tokio::test]
    async fn selected_names_follow_the_runner_convention() {
        let runner = Runner::new(vec![
            schema_store("frontend"),
            schema_store("codeintel"),
            schema_store("codeinsights"),
        ]);

        let names = |selected: Vec<&SchemaStore>| -> Vec<String> {
            selected
                .into_iter()
                .map(|entry| entry.schema.name.clone())
                .collect()
        };

        assert_eq!(
            names(runner.selected(&[])),
            vec!["frontend", "codeintel", "codeinsights"]
        );
        assert_eq!(
            names(runner.selected(&["all".to_string()])),
            vec!["frontend", "codeintel", "codeinsights"]
        );
        assert_eq!(
            names(runner.selected(&["codeintel".to_string()])),
            vec!["codeintel"]
        );
        // Unknown names are ignored, matching the other runner methods.
        assert!(runner.selected(&["missing".to_string()]).is_empty());
    }

    #[tokio::test]
    async fn add_log_is_a_noop_when_no_schema_is_selected() {
        let runner = Runner::new(vec![schema_store("frontend")]);

        // Selecting an unknown schema selects nothing, so no database
        // connection is attempted (the lazy pool would fail if used).
        runner
            .add_log(&["missing".to_string()], 42, true)
            .await
            .expect("no-op add-log");
    }
}
