#![forbid(unsafe_code)]

//! `internal/database/migration/shared`: the stitched migration graph used by
//! `BackfillSchemaVersions`.
//!
//! The graph is the same generated JSON the Go migrator embeds
//! (`shared/data/stitched-migration-graph.json`); it is vendored under
//! `assets/` and parsed once per process.

use std::collections::BTreeMap;
use std::sync::OnceLock;

use serde::Deserialize;

use crate::error::MigratorError;
use crate::store::MigrationBounds;
use crate::types::{Definition, Definitions};

/// `shared.StitchedMigration`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StitchedMigration {
    pub definitions: Definitions,
    pub bounds_by_rev: BTreeMap<String, MigrationBounds>,
}

impl StitchedMigration {
    /// All bounds, in revision order, as the store's backfill API expects.
    pub fn bounds(&self) -> Vec<MigrationBounds> {
        self.bounds_by_rev.values().cloned().collect()
    }
}

#[derive(Debug, Deserialize)]
struct RawDefinition {
    #[serde(rename = "ID")]
    id: i32,
    #[serde(rename = "Name", default)]
    name: String,
    #[serde(rename = "UpQuery", default)]
    up_query: String,
    #[serde(rename = "DownQuery", default)]
    down_query: String,
    #[serde(rename = "Privileged", default)]
    privileged: bool,
    #[serde(rename = "NonIdempotent", default)]
    non_idempotent: bool,
    #[serde(rename = "Parents", default)]
    parents: Option<Vec<i32>>,
    #[serde(rename = "IsCreateIndexConcurrently", default)]
    is_create_index_concurrently: bool,
}

#[derive(Debug, Deserialize)]
struct RawBounds {
    #[serde(rename = "RootID")]
    root_id: i32,
    #[serde(rename = "LeafIDs", default)]
    leaf_ids: Option<Vec<i32>>,
}

#[derive(Debug, Deserialize)]
struct RawStitched {
    #[serde(rename = "Definitions")]
    definitions: Vec<RawDefinition>,
    #[serde(rename = "BoundsByRev")]
    bounds_by_rev: BTreeMap<String, RawBounds>,
}

#[derive(Debug, Deserialize)]
struct RawGraph(BTreeMap<String, RawStitched>);

fn graph() -> &'static BTreeMap<String, Result<StitchedMigration, String>> {
    static GRAPH: OnceLock<BTreeMap<String, Result<StitchedMigration, String>>> = OnceLock::new();
    GRAPH.get_or_init(|| {
        let raw: RawGraph = serde_json::from_str(include_str!(
            "../assets/stitched-migration-graph.json"
        ))
        .expect("vendored stitched migration graph is valid JSON");
        raw.0
            .into_iter()
            .map(|(name, value)| {
                let definitions = value
                    .definitions
                    .into_iter()
                    .map(|definition| Definition {
                        id: definition.id,
                        name: definition.name,
                        up_sql: definition.up_query,
                        down_sql: definition.down_query,
                        privileged: definition.privileged,
                        non_idempotent: definition.non_idempotent,
                        parents: definition.parents.unwrap_or_default(),
                        is_create_index_concurrently: definition.is_create_index_concurrently,
                    })
                    .collect();
                let result = Definitions::new(definitions).map(|definitions| StitchedMigration {
                    definitions,
                    bounds_by_rev: value
                        .bounds_by_rev
                        .into_iter()
                        .map(|(rev, bounds)| {
                            (
                                rev,
                                MigrationBounds {
                                    root_id: bounds.root_id,
                                    leaf_ids: bounds.leaf_ids.unwrap_or_default(),
                                },
                            )
                        })
                        .collect(),
                });
                (name, result.map_err(|error| error.to_string()))
            })
            .collect()
    })
}

/// The stitched graph for `schema`, or `None` when the schema has no generated
/// graph (backfill then behaves like Go's empty map lookup → no-op).
pub fn for_schema(schema: &str) -> Result<Option<&'static StitchedMigration>, MigratorError> {
    match graph().get(schema) {
        Some(Ok(migration)) => Ok(Some(migration)),
        Some(Err(detail)) => Err(MigratorError::new(format!(
            "failed to load stitched migration graph for {schema:?}: {detail}"
        ))),
        None => Ok(None),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn vendored_graph_parses_for_all_schemas() {
        let frontend = for_schema("frontend").expect("frontend").expect("present");
        assert_eq!(frontend.definitions.all().len(), 688);
        assert!(!frontend.bounds_by_rev.is_empty());
        assert_eq!(
            frontend.definitions.root().map(|definition| definition.id),
            Some(-1528395684)
        );
        for name in ["codeintel", "codeinsights"] {
            let migration = for_schema(name).expect(name).expect("present");
            assert!(!migration.definitions.all().is_empty(), "{name}");
        }
        assert!(for_schema("other").expect("other").is_none());
    }
}
