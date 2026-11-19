#![forbid(unsafe_code)]

//! `internal/database/migration/store`: `migration_logs`, advisory locks and
//! version queries.
//!
//! # Schema/migration-table naming
//!
//! Go's `store.Store` is constructed with `migrationsTable` and stores it in a
//! field misleadingly named `schemaName`. Every query writes that value into
//! the `migration_logs.schema` column and hashes it to derive the advisory
//! lock key. The Rust port keeps both fields: [`Store::schema`] is the human
//! schema name (`frontend`, `codeintel`, ...) and
//! [`Store::migrations_table_name`] is Go's `schemaName`
//! (`schema_migrations`, `<name>_schema_migrations`, or an arbitrary test
//! name). Database queries therefore key on `migrations_table_name`, matching
//! the baseline.
//!
//! # Deferred pieces
//!
//! * `Up`/`Down`/`RunDDLStatements`/`IndexStatus` belong to the runner port
//!   and are not implemented here.
//! * Go's `BackfillSchemaVersions` reads the global
//!   `shared.StitchedMigationsBySchemaName` table, which is not vendored in
//!   this crate. [`Store::backfill_schema_versions`] implements the same
//!   algorithm but takes the stitched graph explicitly via
//!   [`StitchedGraph`]; resolving the human schema name to that graph (Go's
//!   `humanizeSchemaName` lookup) is the caller's responsibility.
//!
//! # "Dirty" logs
//!
//! Following Go's `Versions`, a log row with `success IS NULL` is *pending*
//! (an in-progress or interrupted attempt) and a row with `success = false`
//! is *failed*. Either state makes the schema dirty; [`Store::version`] only
//! reports the latest successfully-applied version and does not fail on dirty
//! state, so later runner checks should call [`Store::versions`] and inspect
//! [`MigrationVersions::is_dirty`].

use std::collections::BTreeSet;
use std::future::Future;

use sqlx::pool::PoolConnection;
use sqlx::{PgPool, Postgres, Row};
use time::OffsetDateTime;

use crate::error::MigratorError;
use crate::types::{Definitions, MigrationLog};

/// `currentMigrationLogSchemaVersion` (`store.go:59`).
pub const CURRENT_MIGRATION_LOG_SCHEMA_VERSION: i32 = 2;

/// The ten idempotent statements from Go's `EnsureSchemaTable`
/// (`store.go:68-79`), in order.
pub const ENSURE_MIGRATION_LOGS_QUERIES: [&str; 10] = [
    "CREATE TABLE IF NOT EXISTS migration_logs(id SERIAL PRIMARY KEY)",
    "ALTER TABLE migration_logs ADD COLUMN IF NOT EXISTS migration_logs_schema_version integer NOT NULL",
    "ALTER TABLE migration_logs ADD COLUMN IF NOT EXISTS schema text NOT NULL",
    "ALTER TABLE migration_logs ADD COLUMN IF NOT EXISTS version integer NOT NULL",
    "ALTER TABLE migration_logs ADD COLUMN IF NOT EXISTS up bool NOT NULL",
    "ALTER TABLE migration_logs ADD COLUMN IF NOT EXISTS started_at timestamptz NOT NULL",
    "ALTER TABLE migration_logs ADD COLUMN IF NOT EXISTS finished_at timestamptz",
    "ALTER TABLE migration_logs ADD COLUMN IF NOT EXISTS success boolean",
    "ALTER TABLE migration_logs ADD COLUMN IF NOT EXISTS error_message text",
    "ALTER TABLE migration_logs ADD COLUMN IF NOT EXISTS backfilled boolean NOT NULL DEFAULT FALSE",
];

/// Go's `versionsQuery` (`store.go:279-304`), parameterized by the schema
/// identifier.
pub const VERSIONS_QUERY: &str = r#"
WITH ranked_migration_logs AS (
	SELECT
		migration_logs.*,
		ROW_NUMBER() OVER (PARTITION BY version ORDER BY backfilled, started_at DESC) AS row_number
	FROM migration_logs
	WHERE
		schema = $1 AND
		-- Filter out failed reverts, which should have no visible effect but are
		-- a common occurrence in development. We don't allow CIC in downgrades
		-- therefore all reverts are applied in a txn.
		NOT (
			NOT up AND
			NOT success AND
			finished_at IS NOT NULL
		)
)
SELECT
	schema,
	version,
	up,
	success
FROM ranked_migration_logs
WHERE row_number = 1
ORDER BY version
"#;

/// Every column of `migration_logs`, oldest first.
pub const LOGS_QUERY: &str = r#"
SELECT
	id,
	migration_logs_schema_version,
	schema,
	version,
	up,
	started_at,
	finished_at,
	success,
	error_message,
	backfilled
FROM migration_logs
WHERE schema = $1
ORDER BY started_at, id
"#;

/// Go's `backfillSchemaVersionsQuery` (`store.go:202-230`). Parameters are
/// `(migration_logs_schema_version, schema, versions int[] )`.
pub const BACKFILL_SCHEMA_VERSIONS_QUERY: &str = r#"
WITH candidates AS (
	SELECT
		$1::integer AS migration_logs_schema_version,
		$2 AS schema,
		version AS version,
		true AS up,
		NOW() AS started_at,
		NOW() AS finished_at,
		true AS success,
		true AS backfilled
	FROM (SELECT unnest($3::integer[])) AS vs(version)
)
INSERT INTO migration_logs (
	migration_logs_schema_version,
	schema,
	version,
	up,
	started_at,
	finished_at,
	success,
	backfilled
)
SELECT c.* FROM candidates c
WHERE NOT EXISTS (
	SELECT 1 FROM migration_logs ml
	WHERE ml.schema = c.schema AND ml.version = c.version
)
"#;

/// Migration IDs that are always backfilled after the stitched graph has been
/// constructed (`store.go:98-100`).
pub const BACKFILL_OVERRIDES: [i32; 1] = [
    1648115472, // See https://github.com/sourcegraph/sourcegraph/pull/46969 and https://github.com/sourcegraph/sourcegraph/pull/55650
];

const CREATE_MIGRATION_LOG_QUERY: &str = r#"
INSERT INTO migration_logs (
	migration_logs_schema_version,
	schema,
	version,
	up,
	started_at
) VALUES ($1, $2, $3, $4, NOW())
RETURNING id
"#;

const UPDATE_MIGRATION_LOG_QUERY: &str = r#"
UPDATE migration_logs
SET finished_at = NOW(), success = $1, error_message = $2
WHERE id = $3
"#;

const ADVISORY_LOCK_QUERY: &str = "SELECT pg_advisory_lock($1, $2)";
const TRY_ADVISORY_LOCK_QUERY: &str = "SELECT pg_try_advisory_lock($1, $2)";
const ADVISORY_UNLOCK_QUERY: &str = "SELECT pg_advisory_unlock($1, $2)";

/// `MigrationBounds` (`shared/types.go`): the root and leaves of one stitched
/// revision.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MigrationBounds {
    pub root_id: i32,
    pub leaf_ids: Vec<i32>,
}

/// The parts of `shared.StitchedMigration` that backfilling needs.
#[derive(Debug, Copy, Clone)]
pub struct StitchedGraph<'a> {
    pub definitions: &'a Definitions,
    pub bounds_by_rev: &'a [MigrationBounds],
}

/// Result of Go's `Versions`: three disjoint sets of migration versions.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MigrationVersions {
    /// Successfully applied `up` migrations.
    pub applied: Vec<i32>,
    /// Attempts with `success IS NULL` (in-progress or interrupted).
    pub pending: Vec<i32>,
    /// Attempts with `success = false` (require administrator attention).
    pub failed: Vec<i32>,
}

impl MigrationVersions {
    /// Go's runner treats any pending or failed log as a dirty schema.
    pub fn is_dirty(&self) -> bool {
        !self.pending.is_empty() || !self.failed.is_empty()
    }
}

/// How `(up, success)` in a `versionsQuery` row is classified.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LogState {
    Applied,
    Pending,
    Failed,
    /// A successful revert: no visible effect on the applied set.
    Reverted,
}

fn classify_log_state(up: bool, success: Option<bool>) -> LogState {
    match success {
        None => LogState::Pending,
        Some(false) => LogState::Failed,
        Some(true) if up => LogState::Applied,
        Some(true) => LogState::Reverted,
    }
}

/// A held session-level advisory lock. The lock lives on a dedicated pooled
/// connection so it can span transactions, matching Go's `TryLock`.
pub struct LockGuard {
    conn: Option<PoolConnection<Postgres>>,
    key: i32,
}

impl LockGuard {
    /// The advisory lock key (`pg_advisory_lock(key, 0)`).
    pub fn key(&self) -> i32 {
        self.key
    }

    /// Release the lock. Idempotent from the caller's perspective; consuming
    /// `self` means it cannot be called twice on the same guard.
    pub async fn unlock(mut self) -> Result<(), MigratorError> {
        self.release().await
    }

    async fn release(&mut self) -> Result<(), MigratorError> {
        let Some(mut conn) = self.conn.take() else {
            return Ok(());
        };

        let result = sqlx::query(ADVISORY_UNLOCK_QUERY)
            .bind(self.key)
            .bind(0_i32)
            .execute(&mut *conn)
            .await;

        result
            .map(|_| ())
            .map_err(|error| db_error("failed to release advisory lock", error))
    }
}

impl Drop for LockGuard {
    fn drop(&mut self) {
        let Some(mut conn) = self.conn.take() else {
            return;
        };

        // Session-level advisory locks outlive a connection being returned to
        // the pool, so a guard dropped without `unlock()` must release the
        // lock before handing the connection back. `Drop` cannot await, so do
        // the release on the current runtime. Without a runtime (for example,
        // during shutdown) the connection is dropped and the server releases
        // the lock when the session ends.
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            let key = self.key;
            handle.spawn(async move {
                let _ = sqlx::query(ADVISORY_UNLOCK_QUERY)
                    .bind(key)
                    .bind(0_i32)
                    .execute(&mut *conn)
                    .await;
            });
        }
    }
}

/// `store.Store` bound to one schema's database.
#[derive(Clone)]
pub struct Store {
    pub(crate) pool: PgPool,
    pub(crate) schema: String,
    pub(crate) migrations_table_name: String,
}

impl Store {
    pub fn new(pool: PgPool, schema: impl Into<String>, migrations_table_name: impl Into<String>) -> Self {
        Self {
            pool,
            schema: schema.into(),
            migrations_table_name: migrations_table_name.into(),
        }
    }

    /// The value written to `migration_logs.schema`. Go uses the store's
    /// `schemaName`, which is the migrations table name; an empty table name
    /// falls back to the human schema name.
    fn migration_log_schema(&self) -> &str {
        if self.migrations_table_name.is_empty() {
            &self.schema
        } else {
            &self.migrations_table_name
        }
    }

    /// `EnsureSchemaTable` (`store.go:64-94`): create or upgrade
    /// `migration_logs` in one transaction. Every statement is idempotent, so
    /// this may be called repeatedly.
    pub async fn ensure_migration_logs_table(&self) -> Result<(), MigratorError> {
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|error| db_error("failed to begin migration_logs setup transaction", error))?;

        for statement in ENSURE_MIGRATION_LOGS_QUERIES {
            sqlx::query(statement)
                .execute(&mut *tx)
                .await
                .map_err(|error| db_error("failed to ensure migration_logs table", error))?;
        }

        tx.commit()
            .await
            .map_err(|error| db_error("failed to commit migration_logs setup", error))
    }

    /// Latest successfully-applied migration version for this schema.
    ///
    /// Equivalent to taking the last element of [`Store::versions`]'s applied
    /// set, i.e. the query
    ///
    /// ```sql
    /// SELECT MAX(version) FROM (...) WHERE row_number = 1 AND up AND success
    /// ```
    ///
    /// `success IS NULL` (pending/dirty) and `success = false` (failed) rows
    /// are ignored here; use [`Store::versions`] to observe them.
    pub async fn version(&self) -> Result<Option<i32>, MigratorError> {
        Ok(self.versions().await?.applied.last().copied())
    }

    /// `Versions` (`store.go:253-277`): classify the newest log for each
    /// version into applied/pending/failed.
    pub async fn versions(&self) -> Result<MigrationVersions, MigratorError> {
        let rows = sqlx::query(VERSIONS_QUERY)
            .bind(self.migration_log_schema())
            .fetch_all(&self.pool)
            .await
            .map_err(|error| db_error("failed to query migration logs", error))?;

        let mut versions = MigrationVersions::default();
        for row in rows {
            let version = row
                .try_get::<i32, _>("version")
                .map_err(|error| db_error("failed to decode migration log version", error))?;
            let up = row
                .try_get::<bool, _>("up")
                .map_err(|error| db_error("failed to decode migration log up flag", error))?;
            let success = row
                .try_get::<Option<bool>, _>("success")
                .map_err(|error| db_error("failed to decode migration log success flag", error))?;

            match classify_log_state(up, success) {
                LogState::Applied => versions.applied.push(version),
                LogState::Pending => versions.pending.push(version),
                LogState::Failed => versions.failed.push(version),
                LogState::Reverted => {}
            }
        }

        Ok(versions)
    }

    /// Every `migration_logs` row for this schema, oldest first, including
    /// the bookkeeping columns (`migration_logs_schema_version`, `success`,
    /// `error_message`, `backfilled`).
    pub async fn logs(&self) -> Result<Vec<MigrationLog>, MigratorError> {
        let rows = sqlx::query(LOGS_QUERY)
            .bind(self.migration_log_schema())
            .fetch_all(&self.pool)
            .await
            .map_err(|error| db_error("failed to query migration logs", error))?;

        let mut logs = Vec::with_capacity(rows.len());
        for row in rows {
            logs.push(MigrationLog {
                id: row
                    .try_get("id")
                    .map_err(|error| db_error("failed to decode migration log id", error))?,
                migration_logs_schema_version: row
                    .try_get("migration_logs_schema_version")
                    .map_err(|error| {
                        db_error("failed to decode migration log schema version", error)
                    })?,
                schema: row
                    .try_get("schema")
                    .map_err(|error| db_error("failed to decode migration log schema", error))?,
                version: row
                    .try_get("version")
                    .map_err(|error| db_error("failed to decode migration log version", error))?,
                up: row
                    .try_get("up")
                    .map_err(|error| db_error("failed to decode migration log up flag", error))?,
                started_at: row
                    .try_get::<OffsetDateTime, _>("started_at")
                    .map_err(|error| {
                        db_error("failed to decode migration log started_at", error)
                    })?,
                finished_at: row
                    .try_get::<Option<OffsetDateTime>, _>("finished_at")
                    .map_err(|error| {
                        db_error("failed to decode migration log finished_at", error)
                    })?,
                success: row
                    .try_get("success")
                    .map_err(|error| {
                        db_error("failed to decode migration log success flag", error)
                    })?,
                error_message: row
                    .try_get("error_message")
                    .map_err(|error| {
                        db_error("failed to decode migration log error_message", error)
                    })?,
                backfilled: row
                    .try_get("backfilled")
                    .map_err(|error| {
                        db_error("failed to decode migration log backfilled flag", error)
                    })?,
            });
        }

        Ok(logs)
    }

    /// `WithMigrationLog` (`store.go:449-477`): insert a running log row,
    /// invoke `f`, then record `finished_at`, `success` and `error_message`.
    ///
    /// The callback receives an owned [`PgPool`] clone so it can run the
    /// migration's SQL asynchronously without borrowing `self`. Its error is
    /// returned unchanged after it has been recorded, matching Go.
    ///
    /// Note that, as in Go, this method does *not* take an advisory lock;
    /// locking is orthogonal (see [`Store::try_lock`] and
    /// [`Store::with_migration_log_locked`]).
    pub async fn with_migration_log<F, Fut>(
        &self,
        version: i32,
        up: bool,
        f: F,
    ) -> Result<(), MigratorError>
    where
        F: FnOnce(PgPool) -> Fut,
        Fut: Future<Output = Result<(), MigratorError>>,
    {
        let log_id = self.create_migration_log(version, up).await?;

        let result = f(self.pool.clone()).await;

        let (success, error_message) = match &result {
            Ok(()) => (true, None),
            Err(error) => (false, Some(error.to_string())),
        };

        let update = sqlx::query(UPDATE_MIGRATION_LOG_QUERY)
            .bind(success)
            .bind(error_message)
            .bind(log_id)
            .execute(&self.pool)
            .await
            .map_err(|error| db_error("failed to update migration log", error));

        match (result, update) {
            (Ok(()), Ok(_)) => Ok(()),
            (Err(error), Ok(_)) => Err(error),
            (Ok(()), Err(update_error)) => Err(update_error),
            (Err(error), Err(update_error)) => {
                Err(MigratorError::Message(format!("{error}; additionally {update_error}")))
            }
        }
    }

    /// Acquire the schema's advisory lock and then run
    /// [`Store::with_migration_log`], releasing the lock afterwards.
    ///
    /// Go's runner takes the lock separately (`tryLock`) and re-reads versions
    /// before invoking `WithMigrationLog`; this convenience method covers the
    /// common "one migration under the lock" shape while the split primitives
    /// remain available.
    pub async fn with_migration_log_locked<F, Fut>(
        &self,
        version: i32,
        up: bool,
        f: F,
    ) -> Result<(), MigratorError>
    where
        F: FnOnce(PgPool) -> Fut,
        Fut: Future<Output = Result<(), MigratorError>>,
    {
        let guard = self.lock().await?;
        let result = self.with_migration_log(version, up, f).await;
        let unlock = guard.unlock().await;

        match (result, unlock) {
            (Ok(()), Ok(())) => Ok(()),
            (Err(error), Ok(())) => Err(error),
            (Ok(()), Err(unlock_error)) => Err(unlock_error),
            (Err(error), Err(unlock_error)) => {
                Err(MigratorError::Message(format!("{error}; additionally {unlock_error}")))
            }
        }
    }

    async fn create_migration_log(&self, version: i32, up: bool) -> Result<i32, MigratorError> {
        sqlx::query_scalar::<_, i32>(CREATE_MIGRATION_LOG_QUERY)
            .bind(CURRENT_MIGRATION_LOG_SCHEMA_VERSION)
            .bind(self.migration_log_schema())
            .bind(version)
            .bind(up)
            .fetch_one(&self.pool)
            .await
            .map_err(|error| db_error("failed to create migration log", error))
    }

    /// `BackfillSchemaVersions` (`store.go:102-200`), parameterized over the
    /// stitched migration graph (not vendored in this crate).
    ///
    /// Inserts successful, `backfilled = true` rows for ancestors of every
    /// fully-applied stitched revision. If the schema is dirty (pending or
    /// failed logs) or nothing has been applied yet, this is a no-op, exactly
    /// as in Go.
    pub async fn backfill_schema_versions(
        &self,
        graph: StitchedGraph<'_>,
    ) -> Result<(), MigratorError> {
        let versions = self.versions().await?;
        if versions.pending.len() != 0 || versions.failed.len() != 0 {
            // Don't mask drift by overwriting in-progress/failed records with
            // fake successful ones.
            return Ok(());
        }
        if versions.applied.is_empty() {
            // Haven't applied anything yet to be able to backfill from.
            return Ok(());
        }

        let applied: BTreeSet<i32> = versions.applied.iter().copied().collect();
        let roots = backfill_root_ids(graph.definitions, graph.bounds_by_rev, &applied);
        let ancestor_ids = backfill_ancestor_ids(graph.definitions, &roots)?;

        // Go returns early when no ancestors were found, even though the fixed
        // overrides are otherwise always included.
        if ancestor_ids.is_empty() {
            return Ok(());
        }

        let mut ids_to_backfill: Vec<i32> = BACKFILL_OVERRIDES.to_vec();
        ids_to_backfill.extend(ancestor_ids);

        sqlx::query(BACKFILL_SCHEMA_VERSIONS_QUERY)
            .bind(CURRENT_MIGRATION_LOG_SCHEMA_VERSION)
            .bind(self.migration_log_schema())
            .bind(ids_to_backfill)
            .execute(&self.pool)
            .await
            .map_err(|error| db_error("failed to backfill schema versions", error))?;

        Ok(())
    }

    /// The advisory lock key derived from the migrations table name
    /// (`store.go:362-364`): `StringKey("<schemaName>:migrations")`.
    pub fn lock_key(&self) -> i32 {
        lock_key_for(self.migration_log_schema())
    }

    /// Block until this schema's advisory lock is held. The returned guard
    /// owns a dedicated connection; prefer [`LockGuard::unlock`] for a
    /// deterministic release.
    pub async fn lock(&self) -> Result<LockGuard, MigratorError> {
        match self.acquire_lock(true).await? {
            Some(guard) => Ok(guard),
            None => Err(MigratorError::Message(
                "failed to acquire advisory lock".to_string(),
            )),
        }
    }

    /// Non-blocking `pg_try_advisory_lock`. Returns `None` when another
    /// session holds the lock, mirroring Go's `TryLock` `(false, no-op, nil)`.
    pub async fn try_lock(&self) -> Result<Option<LockGuard>, MigratorError> {
        self.acquire_lock(false).await
    }

    async fn acquire_lock(&self, blocking: bool) -> Result<Option<LockGuard>, MigratorError> {
        let key = self.lock_key();
        let mut conn = self.pool.acquire().await.map_err(|error| {
            db_error("failed to acquire connection for advisory lock", error)
        })?;

        let locked = if blocking {
            sqlx::query(ADVISORY_LOCK_QUERY)
                .bind(key)
                .bind(0_i32)
                .execute(&mut *conn)
                .await
                .map_err(|error| db_error("failed to acquire advisory lock", error))?;
            true
        } else {
            sqlx::query_scalar::<_, bool>(TRY_ADVISORY_LOCK_QUERY)
                .bind(key)
                .bind(0_i32)
                .fetch_one(&mut *conn)
                .await
                .map_err(|error| db_error("failed to try advisory lock", error))?
        };

        if locked {
            Ok(Some(LockGuard {
                conn: Some(conn),
                key,
            }))
        } else {
            // No lock held; returning the connection to the pool is safe.
            Ok(None)
        }
    }
}

/// `humanizeSchemaName` (`store.go:580-589`): map a golang-migrate table name
/// back to the schema name used by the migration definitions.
pub fn humanize_schema_name(schema_name: &str) -> String {
    if schema_name == "schema_migrations" {
        return "frontend".to_string();
    }

    schema_name
        .strip_suffix("_schema_migrations")
        .unwrap_or(schema_name)
        .to_string()
}

/// `locker.StringKey` (`internal/database/locker/locker.go:14-17`):
/// `int32(FNV-1(s) % math.MaxInt32)`.
pub fn string_key(s: &str) -> i32 {
    let max_int32 = i32::MAX as u32;
    (fnv1_32(s) % max_int32) as i32
}

/// The lock key for a schema identifier:
/// `StringKey(fmt.Sprintf("%s:migrations", schema_identifier))`.
pub fn lock_key_for(schema_identifier: &str) -> i32 {
    string_key(&format!("{schema_identifier}:migrations"))
}

/// 32-bit FNV-1 (multiply, then xor) — **not** FNV-1a. Matches Go's
/// `segmentio/fasthash/fnv1.HashString32` byte for byte.
fn fnv1_32(s: &str) -> u32 {
    let mut hash: u32 = 0x811c_9dc5;
    for byte in s.as_bytes() {
        hash = hash.wrapping_mul(0x0100_0193);
        hash ^= u32::from(*byte);
    }
    hash
}

/// Select the roots of stitched revisions whose leaf migrations have all been
/// applied (`store.go:129-171`). Only roots present in `definitions` are
/// returned; negative roots are normalized with `abs`.
fn backfill_root_ids(
    definitions: &Definitions,
    bounds_by_rev: &[MigrationBounds],
    applied: &BTreeSet<i32>,
) -> Vec<i32> {
    let mut roots = BTreeSet::new();

    for bounds in bounds_by_rev {
        if bounds.leaf_ids.iter().any(|id| !applied.contains(id)) {
            // This revision is not completely applied; skip it.
            continue;
        }

        // Go: `if root < 0 { root = -root }`.
        let root = bounds.root_id.unsigned_abs() as i32;
        if definitions.get_by_id(root).is_some() {
            roots.insert(root);
        }
    }

    roots.into_iter().collect()
}

/// `ancestors` (`store.go:232-245`): all definitions needed to reach `roots`
/// (roots included), sorted ascending.
fn backfill_ancestor_ids(
    definitions: &Definitions,
    roots: &[i32],
) -> Result<Vec<i32>, MigratorError> {
    let mut ids = BTreeSet::new();
    for root in roots {
        for definition in definitions.including_ancestors(*root)? {
            ids.insert(definition.id);
        }
    }

    Ok(ids.into_iter().collect())
}

fn db_error(context: &str, error: sqlx::Error) -> MigratorError {
    MigratorError::Message(format!("{context}: {error}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::Definition;

    fn definition(id: i32, parents: &[i32]) -> Definition {
        Definition {
            id,
            name: format!("migration-{id}"),
            up_sql: String::new(),
            down_sql: String::new(),
            privileged: false,
            non_idempotent: false,
            parents: parents.to_vec(),
            is_create_index_concurrently: false,
        }
    }

    #[test]
    fn migration_log_schema_version_matches_go() {
        assert_eq!(CURRENT_MIGRATION_LOG_SCHEMA_VERSION, 2);
    }

    #[test]
    fn ensure_queries_match_the_go_ddl() {
        assert_eq!(ENSURE_MIGRATION_LOGS_QUERIES.len(), 10);

        let joined = ENSURE_MIGRATION_LOGS_QUERIES.join("\n");
        assert!(joined.contains("CREATE TABLE IF NOT EXISTS migration_logs(id SERIAL PRIMARY KEY)"));

        for column in [
            "migration_logs_schema_version integer NOT NULL",
            "schema text NOT NULL",
            "version integer NOT NULL",
            "up bool NOT NULL",
            "started_at timestamptz NOT NULL",
            "finished_at timestamptz",
            "success boolean",
            "error_message text",
            "backfilled boolean NOT NULL DEFAULT FALSE",
        ] {
            let clause = format!("ADD COLUMN IF NOT EXISTS {column}");
            assert!(joined.contains(clause.as_str()), "missing column: {column}");
        }
    }

    #[test]
    fn versions_query_ranks_latest_and_filters_failed_reverts() {
        let query = VERSIONS_QUERY.to_ascii_lowercase();
        assert!(query.contains("row_number() over (partition by version order by backfilled, started_at desc)"));
        assert!(query.contains("not up"));
        assert!(query.contains("not success"));
        assert!(query.contains("finished_at is not null"));
        assert!(query.contains("where row_number = 1"));
        assert!(query.contains("order by version"));
        assert!(query.contains("schema = $1"));
    }

    #[test]
    fn logs_query_selects_all_columns_oldest_first() {
        let query = LOGS_QUERY.to_ascii_lowercase();
        for column in [
            "id",
            "migration_logs_schema_version",
            "schema",
            "version",
            "up",
            "started_at",
            "finished_at",
            "success",
            "error_message",
            "backfilled",
        ] {
            assert!(query.contains(column), "missing column: {column}");
        }
        assert!(query.contains("order by started_at, id"));
        assert!(query.contains("where schema = $1"));
    }

    #[test]
    fn backfill_query_matches_go() {
        let query = BACKFILL_SCHEMA_VERSIONS_QUERY.to_ascii_lowercase();
        assert!(query.contains("unnest($3::integer[])"));
        assert!(query.contains("true as backfilled"));
        assert!(query.contains("true as success"));
        assert!(query.contains("where not exists"));
        assert!(query.contains("ml.schema = c.schema and ml.version = c.version"));
        assert_eq!(BACKFILL_OVERRIDES, [1_648_115_472]);
    }

    #[test]
    fn humanize_schema_name_matches_go() {
        assert_eq!(humanize_schema_name("schema_migrations"), "frontend");
        assert_eq!(
            humanize_schema_name("codeintel_schema_migrations"),
            "codeintel"
        );
        assert_eq!(
            humanize_schema_name("codeinsights_schema_migrations"),
            "codeinsights"
        );
        assert_eq!(humanize_schema_name("test_schema_migrations"), "test");
        assert_eq!(humanize_schema_name("plain"), "plain");
    }

    #[test]
    fn advisory_lock_keys_match_the_go_locker() {
        // Values produced by Go's `locker.StringKey` over
        // "<migrations table name>:migrations".
        assert_eq!(string_key(""), 18_652_614);
        assert_eq!(
            string_key("schema_migrations:migrations"),
            1_308_612_253
        );
        assert_eq!(lock_key_for("schema_migrations"), 1_308_612_253);
        assert_eq!(
            lock_key_for("codeintel_schema_migrations"),
            1_731_304_153
        );
        assert_eq!(
            lock_key_for("codeinsights_schema_migrations"),
            1_054_047_648
        );
        // FNV-1 output exceeding MathInt32 exercises the modulo branch.
        assert_eq!(lock_key_for("test_migrations_table"), 1_758_221_596);
        assert_eq!(lock_key_for("frontend"), 1_026_352_991);
    }

    #[test]
    fn version_logs_are_classified_like_go() {
        assert_eq!(classify_log_state(true, None), LogState::Pending);
        assert_eq!(classify_log_state(false, None), LogState::Pending);
        assert_eq!(classify_log_state(true, Some(false)), LogState::Failed);
        assert_eq!(classify_log_state(false, Some(false)), LogState::Failed);
        assert_eq!(classify_log_state(true, Some(true)), LogState::Applied);
        assert_eq!(classify_log_state(false, Some(true)), LogState::Reverted);
    }

    #[test]
    fn dirty_versions_only_count_pending_and_failed() {
        let clean = MigrationVersions {
            applied: vec![1, 2, 3],
            pending: Vec::new(),
            failed: Vec::new(),
        };
        assert!(!clean.is_dirty());

        let pending = MigrationVersions {
            pending: vec![4],
            ..clean.clone()
        };
        assert!(pending.is_dirty());

        let failed = MigrationVersions {
            failed: vec![4],
            ..clean
        };
        assert!(failed.is_dirty());
    }

    #[test]
    fn backfill_selects_roots_of_fully_applied_revisions() {
        // A single-root graph (as `Definitions` requires): 1 -> 2 -> 3 and a
        // sibling branch 1 -> 10.
        let definitions = Definitions::new(vec![
            definition(1, &[]),
            definition(2, &[1]),
            definition(3, &[2]),
            definition(10, &[1]),
        ])
        .unwrap();

        let bounds = vec![
            MigrationBounds {
                root_id: 1,
                leaf_ids: vec![3],
            },
            MigrationBounds {
                root_id: 1,
                leaf_ids: vec![10],
            },
        ];

        // Only the 1 -> 2 -> 3 revision is completely applied.
        let partially_applied = BTreeSet::from([1, 2, 3]);
        assert_eq!(
            backfill_root_ids(&definitions, &bounds, &partially_applied),
            vec![1]
        );

        // Neither revision is fully applied.
        let none_applied = BTreeSet::from([1]);
        assert_eq!(
            backfill_root_ids(&definitions, &bounds, &none_applied),
            Vec::<i32>::new()
        );
    }

    #[test]
    fn backfill_normalizes_negative_root_ids() {
        let definitions =
            Definitions::new(vec![definition(1, &[]), definition(2, &[1])]).unwrap();
        let bounds = vec![MigrationBounds {
            root_id: -1,
            leaf_ids: vec![2],
        }];

        assert_eq!(
            backfill_root_ids(&definitions, &bounds, &BTreeSet::from([1, 2])),
            vec![1]
        );
    }

    #[test]
    fn backfill_ancestors_include_roots_sorted_ascending() {
        let definitions = Definitions::new(vec![
            definition(1, &[]),
            definition(2, &[1]),
            definition(3, &[2]),
            definition(10, &[1]),
        ])
        .unwrap();

        // Go's `ancestors` sorts IDs ascending after collecting them.
        assert_eq!(
            backfill_ancestor_ids(&definitions, &[3, 10]).unwrap(),
            vec![1, 2, 3, 10]
        );
    }

    #[tokio::test]
    async fn store_keys_logs_by_migrations_table_name() {
        let pool = sqlx::postgres::PgPoolOptions::new()
            .connect_lazy("postgres://localhost/unused")
            .unwrap();
        let store = Store::new(pool, "frontend", "schema_migrations");

        assert_eq!(store.migration_log_schema(), "schema_migrations");
        assert_eq!(store.lock_key(), 1_308_612_253);
    }
}
