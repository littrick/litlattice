#![forbid(unsafe_code)]

//! `internal/database/migration/runner`: apply/revert migrations and validate
//! schema state.
//!
//! Milestone scope: targeted up/down and validation for clean schemas.
//! Concurrent-index polling (`createIndexConcurrently`) and the stitched
//! multi-version upgrade/downgrade commands are not implemented here; they are
//! delegated to `migrator-go` by the binary.

use base64::Engine as _;
use sha1::{Digest, Sha1};

use crate::error::MigratorError;
use crate::store::{MigrationVersions, Store};
use crate::types::{Definition, Schema};

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
        let guard = entry.store.lock().await?;

        let result = async {
            let versions = entry.store.versions().await?;
            check_dirty(&entry.schema, &versions, options)?;

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

            self.check_privileged(entry, &definitions, up, options)?;

            for definition in &definitions {
                self.apply(entry, definition, up, options).await?;
            }
            Ok(())
        }
        .await;

        let unlock = guard.unlock().await;
        match (result, unlock) {
            (Ok(()), Ok(())) => Ok(()),
            (Err(error), Ok(())) => Err(error),
            (Ok(()), Err(error)) => Err(error),
            (Err(error), Err(unlock_error)) => Err(MigratorError::new(format!(
                "{error}; additionally failed to release advisory lock: {unlock_error}"
            ))),
        }
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
}
