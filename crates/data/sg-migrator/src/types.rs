#![forbid(unsafe_code)]

//! Shared migration model (`internal/database/migration/definition` and
//! `schemas`), independent of storage.

use std::collections::BTreeMap;

use crate::error::MigratorError;

/// One `definition.Definition` (`definition/definition.go:9-20`).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Definition {
    /// Unix timestamp directory prefix.
    pub id: i32,
    /// `metadata.yaml` `name`.
    pub name: String,
    pub up_sql: String,
    pub down_sql: String,
    pub privileged: bool,
    pub non_idempotent: bool,
    pub parents: Vec<i32>,
    pub is_create_index_concurrently: bool,
}

/// `definition.Definitions`: ordered definitions with parent/child indexes.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Definitions {
    definitions: Vec<Definition>,
    by_id: BTreeMap<i32, Definition>,
    children: BTreeMap<i32, Vec<i32>>,
    position: BTreeMap<i32, usize>,
}

impl Definitions {
    /// Crate-internal constructor used by [`crate::definition::validate_and_order`]
    /// after it has validated the graph and computed the ordered index maps.
    /// The public API is unchanged by this helper.
    pub(crate) fn from_parts(
        definitions: Vec<Definition>,
        by_id: BTreeMap<i32, Definition>,
        children: BTreeMap<i32, Vec<i32>>,
        position: BTreeMap<i32, usize>,
    ) -> Self {
        Self {
            definitions,
            by_id,
            children,
            position,
        }
    }

    /// `NewDefinitions`: validates the parent graph and stores the topological
    /// order produced by `reorderDefinitions`.
    pub fn new(definitions: Vec<Definition>) -> Result<Self, MigratorError> {
        crate::definition::validate_and_order(definitions)
    }

    /// `All`: definitions in dependency order.
    pub fn all(&self) -> &[Definition] {
        &self.definitions
    }

    /// `GetByID`.
    pub fn get_by_id(&self, id: i32) -> Option<&Definition> {
        self.by_id.get(&id)
    }

    /// `Root`: the unique definition with no parents.
    pub fn root(&self) -> Option<&Definition> {
        self.definitions
            .iter()
            .find(|definition| definition.parents.is_empty())
    }

    /// `Leaves`: definitions with no children.
    pub fn leaves(&self) -> Vec<&Definition> {
        self.definitions
            .iter()
            .filter(|definition| {
                self.children
                    .get(&definition.id)
                    .map(|children| children.is_empty())
                    .unwrap_or(true)
            })
            .collect()
    }

    /// Child IDs of `id` (dependency order is not significant here).
    pub fn children_of(&self, id: i32) -> &[i32] {
        self.children.get(&id).map(Vec::as_slice).unwrap_or(&[])
    }

    /// Index of `id` in [`Self::all`].
    pub fn position_of(&self, id: i32) -> Option<usize> {
        self.position.get(&id).copied()
    }

    /// `Definitions.Up`: ancestors of `targets` (in dependency order) that
    /// are not already applied.
    pub fn up(
        &self,
        applied: &[i32],
        targets: &[i32],
    ) -> Result<Vec<Definition>, MigratorError> {
        let mut visited: BTreeMap<i32, ()> = BTreeMap::new();
        let mut frontier: Vec<i32> = targets.to_vec();
        while let Some(id) = frontier.pop() {
            if visited.insert(id, ()).is_some() {
                continue;
            }
            let definition = self
                .get_by_id(id)
                .ok_or_else(|| MigratorError::new(format!("unknown migration {id}")))?;
            frontier.extend(definition.parents.iter().copied());
        }
        let applied: BTreeMap<i32, ()> = applied.iter().map(|id| (*id, ())).collect();
        Ok(self
            .definitions
            .iter()
            .filter(|definition| visited.contains_key(&definition.id))
            .filter(|definition| !applied.contains_key(&definition.id))
            .cloned()
            .collect())
    }

    /// `Definitions.Down`: descendants of `targets` that are applied, in the
    /// reverse of their dependency order (targets themselves excluded).
    pub fn down(
        &self,
        applied: &[i32],
        targets: &[i32],
    ) -> Result<Vec<Definition>, MigratorError> {
        let mut visited: BTreeMap<i32, ()> = BTreeMap::new();
        let mut frontier: Vec<i32> = targets.to_vec();
        while let Some(id) = frontier.pop() {
            if visited.insert(id, ()).is_some() {
                continue;
            }
            if self.get_by_id(id).is_none() {
                return Err(MigratorError::new(format!("unknown migration {id}")));
            }
            frontier.extend(self.children_of(id).iter().copied());
        }
        let targets: BTreeMap<i32, ()> = targets.iter().map(|id| (*id, ())).collect();
        let applied: BTreeMap<i32, ()> = applied.iter().map(|id| (*id, ())).collect();
        let mut definitions: Vec<Definition> = self
            .definitions
            .iter()
            .filter(|definition| visited.contains_key(&definition.id))
            .filter(|definition| !targets.contains_key(&definition.id))
            .filter(|definition| applied.contains_key(&definition.id))
            .cloned()
            .collect();
        definitions.reverse();
        Ok(definitions)
    }

    /// Every definition that must be applied before `id` (transitive parents,
    /// parents first), including `id` itself.
    pub fn including_ancestors(&self, id: i32) -> Result<Vec<&Definition>, MigratorError> {
        let mut seen = BTreeMap::new();
        self.collect_ancestors(id, &mut seen)?;
        Ok(seen.into_values().collect())
    }

    fn collect_ancestors<'a>(
        &'a self,
        id: i32,
        seen: &mut BTreeMap<usize, &'a Definition>,
    ) -> Result<(), MigratorError> {
        let definition = self
            .get_by_id(id)
            .ok_or_else(|| MigratorError::new(format!("unknown migration {id}")))?;
        let position = self
            .position_of(id)
            .ok_or_else(|| MigratorError::new(format!("unknown migration {id}")))?;
        if seen.contains_key(&position) {
            return Ok(());
        }
        for parent in &definition.parents {
            self.collect_ancestors(*parent, seen)?;
        }
        seen.insert(position, definition);
        Ok(())
    }
}

/// `schemas.Schema`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Schema {
    pub name: String,
    pub migrations_table_name: String,
    pub definitions: Definitions,
}

impl Schema {
    pub fn new(
        name: impl Into<String>,
        migrations_table_name: impl Into<String>,
        definitions: Definitions,
    ) -> Self {
        Self {
            name: name.into(),
            migrations_table_name: migrations_table_name.into(),
            definitions,
        }
    }
}

/// `store.MigrationLog` (`migration_logs` row).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MigrationLog {
    pub id: i32,
    pub migration_logs_schema_version: i32,
    pub schema: String,
    pub version: i32,
    pub up: bool,
    pub started_at: time::OffsetDateTime,
    pub finished_at: Option<time::OffsetDateTime>,
    pub success: Option<bool>,
    pub error_message: Option<String>,
    pub backfilled: bool,
}
