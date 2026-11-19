#![forbid(unsafe_code)]

//! `internal/database/migration/multiversion`: multi-version upgrade and
//! downgrade planning over stitched schema definitions and out-of-band
//! migration interrupts.
//!
//! The SQL corpus is not shipped here. Planning only needs the pre-compiled
//! stitched migration graph (vendored in [`crate::stitched`]), whose
//! `BoundsByRev` map supplies the leaf migration IDs for every release tag in
//! the requested range. [`load_frozen_definitions`] additionally exposes the
//! revision-frozen definition files used by the Rust CLI when it needs the
//! graph as it existed at a specific release (`<root>/frozen/<rev>.json`).

use std::collections::BTreeMap;
use std::io;
use std::path::Path;

use serde::Deserialize;

use crate::error::MigratorError;
use crate::oobmigration::interrupts::{schedule_migration_interrupts, MigrationInterrupt};
use crate::oobmigration::version::{compare_versions, upgrade_range, Version, VersionOrder};
use crate::schemas::schema_names;
use crate::stitched;
use crate::store::MigrationBounds;
use crate::types::{Definition, Definitions};

/// `multiversion.MigrationStep`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MigrationStep {
    pub instance_version: Version,
    pub schema_migration_leaf_ids_by_schema_name: BTreeMap<String, Vec<i32>>,
    pub out_of_band_migration_ids: Vec<i32>,
}

/// `multiversion.MigrationPlan`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MigrationPlan {
    pub from: Version,
    pub to: Version,
    /// Stitched definitions over the whole version range, by schema name.
    /// Kept on the plan because `SerializeUpgradePlan` needs to expand the
    /// final leaf IDs into an ordered list of migration IDs and names.
    pub stitched_definitions_by_schema_name: BTreeMap<String, Definitions>,
    pub steps: Vec<MigrationStep>,
}

/// `upgradestore.UpgradePlan`: the persisted description of an upgrade shown
/// while a multi-version upgrade is in progress.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct UpgradePlan {
    pub out_of_band_migration_ids: Vec<i32>,
    pub migrations: BTreeMap<String, Vec<i32>>,
    pub migration_names: BTreeMap<String, BTreeMap<i32, String>>,
}

/// A stitched migration filtered to a set of release tags
/// (`shared.StitchedMigration` after `filterStitchedMigrationsForTags`).
#[derive(Debug, Clone)]
struct FilteredStitchedMigration {
    definitions: Definitions,
    bounds_by_rev: BTreeMap<String, MigrationBounds>,
}

/// `multiversion.PlanMigration`.
///
/// In Go this function receives an explicit version range and interrupt set
/// computed by its CLI callers. The Rust entry point derives both from
/// `from`/`to`: `upgrade_range` covers the inclusive range in either
/// direction, and [`schedule_migration_interrupts`] produces the set of
/// out-of-band completion points.
///
/// `root` is the migration corpus root; it is retained in the signature for
/// callers that also need [`load_frozen_definitions`]. Planning itself uses the
/// vendored stitched graph and does not read `root`.
pub fn plan_migration(
    from: Version,
    to: Version,
    _root: &Path,
) -> Result<MigrationPlan, MigratorError> {
    let version_range = match compare_versions(from, to) {
        VersionOrder::Before => upgrade_range(from, to)?,
        VersionOrder::After => upgrade_range(to, from)?,
        VersionOrder::Equal => {
            return Err(MigratorError::new(format!(
                "invalid range (from={} >= to={})",
                from, to
            )))
        }
    };

    let interrupts = schedule_migration_interrupts(from, to)?;
    plan_migration_with(from, to, &version_range, &interrupts)
}

/// `PlanMigration` with an explicit version range and interrupt list. This is
/// the exact Go contract; [`plan_migration`] computes the two from the
/// instance versions.
pub fn plan_migration_with(
    from: Version,
    to: Version,
    version_range: &[Version],
    interrupts: &[MigrationInterrupt],
) -> Result<MigrationPlan, MigratorError> {
    let version_tags: Vec<String> = version_range
        .iter()
        .map(Version::git_tag)
        .collect();

    // Retrieve relevant stitched migrations for this version range.
    let stitched_migrations = filter_stitched_migrations_for_tags(&version_tags)?;

    // Extract/rotate stitched definitions so we can query them by schema name.
    let stitched_definitions_by_schema_name: BTreeMap<String, Definitions> = stitched_migrations
        .iter()
        .map(|(schema, migration)| (schema.clone(), migration.definitions.clone()))
        .collect();

    // Extract/rotate leaf identifiers so we can query them by version tag
    // first, then by schema name.
    let mut leaf_ids_by_schema_name_by_tag: BTreeMap<String, BTreeMap<String, Vec<i32>>> =
        BTreeMap::new();
    for (schema_name, migration) in &stitched_migrations {
        for (tag, bounds) in &migration.bounds_by_rev {
            leaf_ids_by_schema_name_by_tag
                .entry(tag.clone())
                .or_default()
                .insert(schema_name.clone(), bounds.leaf_ids.clone());
        }
    }

    // Interleave out-of-band migration interrupts and schema migrations.
    let mut steps = Vec::with_capacity(interrupts.len() + 1);
    for interrupt in interrupts {
        steps.push(MigrationStep {
            instance_version: interrupt.version,
            schema_migration_leaf_ids_by_schema_name: leaf_ids_by_schema_name_by_tag
                .get(&interrupt.version.git_tag())
                .cloned()
                .unwrap_or_default(),
            out_of_band_migration_ids: interrupt.migration_ids.clone(),
        });
    }
    steps.push(MigrationStep {
        instance_version: to,
        schema_migration_leaf_ids_by_schema_name: leaf_ids_by_schema_name_by_tag
            .get(&to.git_tag())
            .cloned()
            .unwrap_or_default(),
        // All required out of band migrations have already completed.
        out_of_band_migration_ids: Vec::new(),
    });

    Ok(MigrationPlan {
        from,
        to,
        stitched_definitions_by_schema_name,
        steps,
    })
}

/// `filterStitchedMigrationsForTags`: a copy of the pre-compiled stitched map
/// with references to tags outside the given set removed. This lets a migrator
/// that knows the path from X -> Y also know the path from any partial
/// migration X <= W -> Z <= Y.
fn filter_stitched_migrations_for_tags(
    tags: &[String],
) -> Result<BTreeMap<String, FilteredStitchedMigration>, MigratorError> {
    let mut filtered = BTreeMap::new();

    for schema_name in schema_names() {
        let stitched_migration = stitched::for_schema(schema_name)?.ok_or_else(|| {
            MigratorError::new(format!("unknown schema {schema_name:?} in stitched graph"))
        })?;

        let mut bounds_by_rev = BTreeMap::new();
        for tag in tags {
            let bounds = stitched_migration
                .bounds_by_rev
                .get(tag)
                .ok_or_else(|| MigratorError::new(format!("unknown tag {tag:?}")))?;
            bounds_by_rev.insert(tag.clone(), bounds.clone());
        }

        filtered.insert(
            (*schema_name).to_string(),
            FilteredStitchedMigration {
                definitions: stitched_migration.definitions.clone(),
                bounds_by_rev,
            },
        );
    }

    Ok(filtered)
}

/// `multiversion.SerializeUpgradePlan`: expand the final step's leaf IDs into
/// the ordered list of migrations (and names) applied over the whole range.
pub fn serialize_upgrade_plan(plan: &MigrationPlan) -> UpgradePlan {
    if plan.steps.is_empty() {
        return UpgradePlan::default();
    }

    let out_of_band_migration_ids: Vec<i32> = plan
        .steps
        .iter()
        .flat_map(|step| step.out_of_band_migration_ids.iter().copied())
        .collect();

    let last_step = plan.steps.last().expect("checked non-empty");
    let mut migrations = BTreeMap::new();
    let mut migration_names = BTreeMap::new();

    for (schema, leaf_ids) in &last_step.schema_migration_leaf_ids_by_schema_name {
        let mut names_for_schema = BTreeMap::new();

        if let Some(definitions) = plan.stitched_definitions_by_schema_name.get(schema) {
            if let Ok(up) = definitions.up(&[], leaf_ids) {
                migrations.insert(
                    schema.clone(),
                    up.iter().map(|definition| definition.id).collect::<Vec<_>>(),
                );
                for definition in up {
                    names_for_schema.insert(definition.id, definition.name);
                }
            }
        }

        // Go installs an (possibly empty) name map for every schema in the
        // final step, even when the definition lookup fails.
        migration_names.insert(schema.clone(), names_for_schema);
    }

    UpgradePlan {
        out_of_band_migration_ids,
        migrations,
        migration_names,
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

impl From<RawDefinition> for Definition {
    fn from(definition: RawDefinition) -> Self {
        Self {
            id: definition.id,
            name: definition.name,
            up_sql: definition.up_query,
            down_sql: definition.down_query,
            privileged: definition.privileged,
            non_idempotent: definition.non_idempotent,
            parents: definition.parents.unwrap_or_default(),
            is_create_index_concurrently: definition.is_create_index_concurrently,
        }
    }
}

#[derive(Debug, Deserialize)]
struct RawFrozenSchema {
    #[serde(rename = "Definitions", default)]
    definitions: Option<Vec<RawDefinition>>,
}

/// `shared.GetFrozenDefinitions`: load the schema definitions frozen at a
/// revision from `<root>/frozen/<rev>.json`.
///
/// The file is shaped `{ "<schema>": { "Definitions": [...] } }`; extra keys
/// (notably `BoundsByRev`) are ignored. A missing file produces Go's
/// `failed to load schema at revision "..."` message.
pub fn load_frozen_definitions(
    root: &Path,
    rev: &str,
) -> Result<BTreeMap<String, Definitions>, MigratorError> {
    let path = root.join("frozen").join(format!("{rev}.json"));
    let content = match std::fs::read_to_string(&path) {
        Ok(content) => content,
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            return Err(MigratorError::new(format!(
                "failed to load schema at revision {rev:?} (check the versions listed in cmd/generator/consts.go)"
            )))
        }
        Err(error) => {
            return Err(MigratorError::new(format!(
                "failed to read frozen definitions at revision {rev:?}: {error}"
            )))
        }
    };

    let raw: BTreeMap<String, RawFrozenSchema> =
        serde_json::from_str(&content).map_err(|error| {
            MigratorError::new(format!(
                "failed to parse frozen definitions at revision {rev:?}: {error}"
            ))
        })?;

    let mut definitions_by_schema = BTreeMap::new();
    for (schema_name, frozen) in raw {
        let definitions: Vec<Definition> = frozen
            .definitions
            .unwrap_or_default()
            .into_iter()
            .map(Definition::from)
            .collect();
        let definitions = Definitions::new(definitions).map_err(|error| {
            MigratorError::new(format!(
                "failed to load schema {schema_name:?} at revision {rev:?}: {error}"
            ))
        })?;
        definitions_by_schema.insert(schema_name, definitions);
    }

    Ok(definitions_by_schema)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    const ORIGIN_FROZEN_ROOT: &str =
        "/home/developer/ws/dev/origin/repo/internal/database/migration/shared/data";

    /// Removes its directory when dropped so fixtures do not accumulate.
    struct TempDir(PathBuf);

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    fn unique_temp_dir() -> TempDir {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock before Unix epoch")
            .as_nanos();
        TempDir(std::env::temp_dir().join(format!(
            "sg-migrator-multiversion-{}-{nanos}",
            std::process::id()
        )))
    }

    fn root_with_frozen(rev: &str, contents: &str) -> TempDir {
        let root = unique_temp_dir();
        let frozen_dir = root.0.join("frozen");
        fs::create_dir_all(&frozen_dir).expect("create frozen directory");
        fs::write(frozen_dir.join(format!("{rev}.json")), contents).expect("write frozen file");
        root
    }

    fn leaves(
        step: &MigrationStep,
    ) -> &BTreeMap<String, Vec<i32>> {
        &step.schema_migration_leaf_ids_by_schema_name
    }

    #[test]
    fn loads_frozen_definitions_from_fixture() {
        let root = root_with_frozen(
            "4.5.0",
            r#"{
                "frontend": {
                    "Definitions": [
                        {"ID": 2, "Name": "second", "UpQuery": "UP2", "DownQuery": "DOWN2", "Parents": [1]},
                        {"ID": 1, "Name": "first", "UpQuery": "UP1", "DownQuery": "DOWN1", "Privileged": true, "NonIdempotent": true, "Parents": []}
                    ],
                    "BoundsByRev": {"v4.5.0": {"RootID": 1, "LeafIDs": [2], "PreCreation": false}}
                },
                "codeintel": {"Definitions": []}
            }"#,
        );

        let loaded = load_frozen_definitions(&root.0, "4.5.0").expect("load frozen");
        assert_eq!(loaded.len(), 2);

        let frontend = loaded.get("frontend").expect("frontend");
        // Definitions::new orders parents before children.
        let ids: Vec<i32> = frontend.all().iter().map(|definition| definition.id).collect();
        assert_eq!(ids, vec![1, 2]);

        let first = frontend.get_by_id(1).expect("first");
        assert_eq!(first.name, "first");
        assert_eq!(first.up_sql, "UP1");
        assert_eq!(first.down_sql, "DOWN1");
        assert!(first.privileged);
        assert!(first.non_idempotent);
        assert_eq!(first.parents, Vec::<i32>::new());

        let second = frontend.get_by_id(2).expect("second");
        assert_eq!(second.parents, vec![1]);
        assert!(loaded.get("codeintel").expect("codeintel").all().is_empty());
    }

    #[test]
    fn missing_frozen_file_matches_go_error() {
        let error = load_frozen_definitions(Path::new("/nonexistent/sg-migrator"), "9.9.9")
            .expect_err("missing file");
        assert!(
            error
                .to_string()
                .contains("failed to load schema at revision \"9.9.9\""),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn loads_real_frozen_4_5_0_when_available() {
        let root = Path::new(ORIGIN_FROZEN_ROOT);
        if !root.join("frozen/4.5.0.json").exists() {
            return;
        }

        let loaded = load_frozen_definitions(root, "4.5.0").expect("load real frozen data");
        assert_eq!(loaded.len(), 3);
        assert_eq!(loaded["frontend"].all().len(), 188);
        assert_eq!(loaded["codeintel"].all().len(), 15);
        assert_eq!(loaded["codeinsights"].all().len(), 24);
        assert_eq!(
            loaded["frontend"].root().map(|definition| definition.id),
            Some(1648051770)
        );
    }

    #[test]
    fn plans_upgrade_from_4_5_to_current() {
        let plan = plan_migration(Version::new(4, 5), Version::new(5, 6), Path::new("/corpus"))
            .expect("plan upgrade");

        assert_eq!(plan.from, Version::new(4, 5));
        assert_eq!(plan.to, Version::new(5, 6));
        assert_eq!(plan.steps.len(), 2);

        // Interrupt at 4.5: migrations 19 and 20 deprecate in 4.6.
        let first = &plan.steps[0];
        assert_eq!(first.instance_version, Version::new(4, 5));
        assert_eq!(first.out_of_band_migration_ids, vec![19, 20]);
        assert_eq!(
            leaves(first)["frontend"],
            vec![1675296942, 1676996650, 1675864432]
        );
        assert_eq!(leaves(first)["codeintel"], vec![1671059396]);
        assert_eq!(leaves(first)["codeinsights"], vec![1675347548]);

        let final_step = &plan.steps[1];
        assert_eq!(final_step.instance_version, Version::new(5, 6));
        assert!(final_step.out_of_band_migration_ids.is_empty());
        assert_eq!(leaves(final_step)["frontend"], vec![1722348497]);
        assert_eq!(leaves(final_step)["codeintel"], vec![1686315964]);
        assert_eq!(leaves(final_step)["codeinsights"], vec![1719914228]);

        // All schemas' stitched definitions are retained on the plan.
        assert_eq!(plan.stitched_definitions_by_schema_name.len(), 3);
        assert_eq!(
            plan.stitched_definitions_by_schema_name["frontend"]
                .all()
                .len(),
            688
        );
    }

    #[test]
    fn plans_downgrade_from_current_to_4_5() {
        let plan = plan_migration(Version::new(5, 6), Version::new(4, 5), Path::new("/corpus"))
            .expect("plan downgrade");

        assert_eq!(plan.from, Version::new(5, 6));
        assert_eq!(plan.to, Version::new(4, 5));
        assert_eq!(plan.steps.len(), 3);

        assert_eq!(plan.steps[0].instance_version, Version::new(5, 0));
        assert_eq!(plan.steps[0].out_of_band_migration_ids, vec![22, 23]);
        assert_eq!(
            leaves(&plan.steps[0])["frontend"],
            vec![1678899992, 1678994673]
        );
        assert_eq!(leaves(&plan.steps[0])["codeintel"], vec![1679010276]);
        assert_eq!(leaves(&plan.steps[0])["codeinsights"], vec![1675347548]);

        assert_eq!(plan.steps[1].instance_version, Version::new(4, 5));
        assert_eq!(plan.steps[1].out_of_band_migration_ids, vec![19, 20, 21]);
        assert_eq!(
            leaves(&plan.steps[1])["frontend"],
            vec![1675296942, 1676996650, 1675864432]
        );

        // Go always appends a final step at `to` with no out-of-band work.
        assert_eq!(plan.steps[2].instance_version, Version::new(4, 5));
        assert!(plan.steps[2].out_of_band_migration_ids.is_empty());
        assert_eq!(leaves(&plan.steps[2]), leaves(&plan.steps[1]));
    }

    #[test]
    fn serializes_upgrade_plan() {
        let plan = plan_migration(Version::new(4, 5), Version::new(5, 6), Path::new("/corpus"))
            .expect("plan upgrade");
        let serialized = serialize_upgrade_plan(&plan);

        assert_eq!(serialized.out_of_band_migration_ids, vec![19, 20]);
        // The final leaves reach every definition in each stitched graph.
        for (schema, definitions) in &plan.stitched_definitions_by_schema_name {
            let ids = serialized
                .migrations
                .get(schema)
                .unwrap_or_else(|| panic!("missing migrations for {schema}"));
            assert_eq!(ids.len(), definitions.all().len(), "{schema}");
            assert_eq!(ids[0], definitions.root().expect("root").id, "{schema}");

            let names = &serialized.migration_names[schema];
            assert_eq!(names.len(), ids.len(), "{schema}");
            for id in ids {
                assert!(names.contains_key(id), "missing name for {schema}/{id}");
            }
        }
        assert_eq!(
            serialized.migration_names["frontend"].get(&1722348497),
            Some(&"exhaustive search jobs add column".to_string())
        );
    }

    #[test]
    fn serialize_empty_plan_is_zero_value() {
        let plan = MigrationPlan {
            from: Version::new(4, 5),
            to: Version::new(4, 5),
            stitched_definitions_by_schema_name: BTreeMap::new(),
            steps: Vec::new(),
        };
        assert_eq!(serialize_upgrade_plan(&plan), UpgradePlan::default());
    }

    #[test]
    fn rejects_empty_range() {
        let error = plan_migration(Version::new(4, 5), Version::new(4, 5), Path::new("/corpus"))
            .expect_err("equal versions are not a range");
        assert!(
            error.to_string().contains("invalid range (from=4.5 >= to=4.5)"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn rejects_unknown_version_tag() {
        let error = plan_migration(Version::new(6, 0), Version::new(6, 1), Path::new("/corpus"))
            .expect_err("unknown tags");
        assert!(
            error.to_string().contains("unknown tag \"v6.0.0\""),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn filter_rejects_unknown_tag_directly() {
        let error = filter_stitched_migrations_for_tags(&["v1.0.0".to_string()])
            .expect_err("unknown tag");
        assert_eq!(error.to_string(), "unknown tag \"v1.0.0\"");
    }

    #[test]
    fn plan_with_explicit_range_mirrors_go() {
        let version_range = upgrade_range(Version::new(4, 5), Version::new(5, 0)).expect("range");
        let plan = plan_migration_with(
            Version::new(4, 5),
            Version::new(5, 0),
            &version_range,
            &[MigrationInterrupt {
                version: Version::new(4, 5),
                migration_ids: vec![19, 20],
            }],
        )
        .expect("plan");

        assert_eq!(plan.steps.len(), 2);
        assert_eq!(plan.steps[0].out_of_band_migration_ids, vec![19, 20]);
        assert_eq!(plan.steps[1].instance_version, Version::new(5, 0));
        assert_eq!(leaves(&plan.steps[1])["frontend"], vec![1678899992, 1678994673]);
    }
}

/// Options for [`run_migration`] (Go `multiversion.RunMigration`).
#[derive(Debug, Clone)]
pub struct RunMigrationOptions {
    pub privileged_mode: crate::runner::PrivilegedMode,
    pub privileged_hashes: Vec<String>,
    pub skip_version_check: bool,
    pub skip_drift_check: bool,
    pub dry_run: bool,
    pub up: bool,
    pub disable_animation: bool,
    /// `SOURCEGRAPH_VERSION`-style target used by the auto-upgrade path.
    pub root: std::path::PathBuf,
}

/// `multiversion.RunMigration`: apply each plan step's schema migrations and
/// out-of-band migrations, then record the target service version.
pub async fn run_migration(
    runner: &crate::runner::Runner,
    frontend_pool: &sqlx::PgPool,
    plan: &MigrationPlan,
    options: &RunMigrationOptions,
) -> Result<(), MigratorError> {
    let current = crate::service_version::get_service_version(frontend_pool).await?;
    if !options.skip_version_check {
        let Some(current_version) = current.as_deref() else {
            return Err(MigratorError::new(format!(
                "version assertion failed: unknown version != {}. Re-invoke with --skip-version-check to ignore this check",
                plan.from
            )));
        };
        match Version::from_string(current_version) {
            Some(version) if version == plan.from => {}
            _ => {
                return Err(MigratorError::new(format!(
                    "version assertion failed: {current_version:?} != {}. Re-invoke with --skip-version-check to ignore this check",
                    plan.from
                )))
            }
        }
    }

    if !options.skip_drift_check {
        check_drift(runner, frontend_pool, plan, &options.root).await?;
    }

    for step in &plan.steps {
        if !options.dry_run {
            let operations: Vec<crate::runner::MigrationOperation> = step
                .schema_migration_leaf_ids_by_schema_name
                .iter()
                .map(|(schema_name, targets)| crate::runner::MigrationOperation {
                    schema_name: schema_name.clone(),
                    up: options.up,
                    targets: targets.clone(),
                })
                .collect();
            runner
                .run_operations(
                    &operations,
                    &crate::runner::RunOptions {
                        privileged_mode: options.privileged_mode,
                        privileged_hash: None,
                        privileged_hashes: options.privileged_hashes.clone(),
                        ignore_single_dirty_log: true,
                        ignore_single_pending_log: true,
                    },
                )
                .await?;
        }

        if !step.out_of_band_migration_ids.is_empty() {
            let codeintel_pool = runner
                .schemas
                .iter()
                .find(|entry| entry.schema.name == "codeintel")
                .map(|entry| &entry.store.pool);
            let insights_pool = runner
                .schemas
                .iter()
                .find(|entry| entry.schema.name == "codeinsights")
                .map(|entry| &entry.store.pool);
            crate::oobmigration::run_out_of_band_migrations_with_pools(
                frontend_pool,
                codeintel_pool,
                insights_pool,
                &crate::oobmigration::RunOutOfBandOptions {
                    ids: step.out_of_band_migration_ids.clone(),
                    apply_reverse: !options.up,
                    dry_run: options.dry_run,
                    disable_animation: options.disable_animation,
                },
            )
            .await?;
        }
    }

    if !options.dry_run {
        crate::service_version::set_service_version(
            frontend_pool,
            &format!("{}.{}.0", plan.to.major, plan.to.minor),
        )
        .await?;
    }
    Ok(())
}

/// `multiversion.CheckDrift` over every schema in `runner`.
async fn check_drift(
    runner: &crate::runner::Runner,
    _frontend_pool: &sqlx::PgPool,
    plan: &MigrationPlan,
    root: &std::path::Path,
) -> Result<(), MigratorError> {
    for entry in &runner.schemas {
        let mut expected = crate::drift::fetch_expected_schema(
            &entry.schema.name,
            &plan.from.git_tag(),
            None,
            root,
        )
        .await?;
        let mut actual = crate::describe::describe(&entry.store).await?;
        crate::drift::canonicalize(&mut actual);
        crate::drift::canonicalize(&mut expected);
        let summaries = crate::drift::compare_schema_descriptions(
            &entry.schema.name,
            &plan.from.git_tag(),
            &actual,
            &expected,
        );
        if !summaries.is_empty() {
            return Err(MigratorError::new(format!(
                "drift detected in schema {:?}\n{}",
                entry.schema.name,
                crate::drift::display_schema_summaries(&summaries)
            )));
        }
    }
    Ok(())
}
