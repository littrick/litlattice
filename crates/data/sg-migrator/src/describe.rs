#![forbid(unsafe_code)]

//! `internal/database/migration/{store/describe.go,schemas/description.go}`:
//! introspect a live schema and render the canonical description used by
//! `describe`, `drift` and `upgrade`/`downgrade`.
//!
//! # Shape and JSON encoding
//!
//! The Go descriptions have no struct tags, so `json.MarshalIndent` emits the
//! exported field names verbatim (`Name`, `TypeName`, `IsPrimaryKey`, ...) in
//! declaration order. The Rust structs mirror that declaration order and use
//! `#[serde(rename_all = "PascalCase")]`, so `format_json` reproduces the same
//! key names and key order. Serde renders the same two-space indentation that
//! `json.MarshalIndent(_, "", "  ")` does.
//!
//! # Deliberate deviations from Go
//!
//! * Go's `json.Marshal` escapes HTML (`&`, `<`, `>`, `"`, `'`); serde_json
//!   does not. Schema descriptions are compared as structured values rather
//!   than by byte equality, so this is not reproduced.
//! * Go keeps `TableDescription.Constraints` as a nil slice when a table has
//!   no constraints and serializes it as `null`; all Rust collections are
//!   `Vec`s and serialize as `[]`. (The other table collections are always
//!   initialized by Go and also render as `[]`.)
//! * Like `store.Describe`, [`describe`] includes the migrator's own tables
//!   (`migration_logs` and `<schema>_schema_migrations`); `drift`/`upgrade`
//!   filtering, if any, lives downstream in canonicalization.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use sqlx::Row;

use crate::error::MigratorError;
use crate::store::Store;

/// `schemas.SchemaDescription`.
///
/// Field order is load-bearing for [`format_json`]: it matches the Go struct
/// (`Extensions`, `Enums`, `Functions`, `Sequences`, `Tables`, `Views`).
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(default, rename_all = "PascalCase")]
pub struct SchemaDescription {
    pub extensions: Vec<String>,
    pub enums: Vec<EnumDescription>,
    pub functions: Vec<FunctionDescription>,
    pub sequences: Vec<SequenceDescription>,
    pub tables: Vec<TableDescription>,
    pub views: Vec<ViewDescription>,
}

impl SchemaDescription {
    /// `SchemaDescription.WrappedExtensions`.
    pub fn wrapped_extensions(&self) -> Vec<ExtensionDescription> {
        self.extensions
            .iter()
            .map(|name| ExtensionDescription { name: name.clone() })
            .collect()
    }
}

/// `schemas.ExtensionDescription`.
///
/// Note that [`SchemaDescription::extensions`] stores plain extension names
/// (`[]string` in Go); this wrapper exists for parity with Go's type and its
/// `CreateStatement` helper.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, rename_all = "PascalCase")]
pub struct ExtensionDescription {
    pub name: String,
}

impl ExtensionDescription {
    /// `ExtensionDescription.CreateStatement`.
    pub fn create_statement(&self) -> String {
        format!("CREATE EXTENSION {};", self.name)
    }
}

/// `schemas.EnumDescription`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, rename_all = "PascalCase")]
pub struct EnumDescription {
    pub name: String,
    pub labels: Vec<String>,
}

/// `schemas.FunctionDescription`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, rename_all = "PascalCase")]
pub struct FunctionDescription {
    pub name: String,
    pub definition: String,
}

/// `schemas.SequenceDescription`.
///
/// Go uses `int` (64-bit on supported platforms); the catalog values are
/// parsed as `bigint`, so `i64` is used here.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, rename_all = "PascalCase")]
pub struct SequenceDescription {
    pub name: String,
    pub type_name: String,
    pub start_value: i64,
    pub minimum_value: i64,
    pub maximum_value: i64,
    pub increment: i64,
    pub cycle_option: String,
}

/// `schemas.TableDescription`.
///
/// Field order matches Go (`Name`, `Comment`, `Columns`, `Indexes`,
/// `Constraints`, `Triggers`).
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(default, rename_all = "PascalCase")]
pub struct TableDescription {
    pub name: String,
    pub comment: String,
    pub columns: Vec<ColumnDescription>,
    pub indexes: Vec<IndexDescription>,
    pub constraints: Vec<ConstraintDescription>,
    pub triggers: Vec<TriggerDescription>,
}

/// `schemas.ColumnDescription`.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(default, rename_all = "PascalCase")]
pub struct ColumnDescription {
    pub name: String,
    pub index: i32,
    pub type_name: String,
    pub is_nullable: bool,
    pub default: String,
    pub character_maximum_length: i32,
    pub is_identity: bool,
    pub identity_generation: String,
    pub is_generated: String,
    pub generation_expression: String,
    pub comment: String,
}

/// `schemas.IndexDescription`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, rename_all = "PascalCase")]
pub struct IndexDescription {
    pub name: String,
    pub is_primary_key: bool,
    pub is_unique: bool,
    pub is_exclusion: bool,
    pub is_deferrable: bool,
    pub index_definition: String,
    pub constraint_type: String,
    pub constraint_definition: String,
}

/// `schemas.ConstraintDescription`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, rename_all = "PascalCase")]
pub struct ConstraintDescription {
    pub name: String,
    pub constraint_type: String,
    pub ref_table_name: String,
    pub is_deferrable: bool,
    pub constraint_definition: String,
}

/// `schemas.TriggerDescription`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, rename_all = "PascalCase")]
pub struct TriggerDescription {
    pub name: String,
    pub definition: String,
}

/// `schemas.ViewDescription`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, rename_all = "PascalCase")]
pub struct ViewDescription {
    pub name: String,
    pub definition: String,
}

// ---------------------------------------------------------------------------
// store.Describe
// ---------------------------------------------------------------------------

/// `store.Describe`: read the live schema's description.
///
/// Go returns a map keyed by schema name and its CLI reads `["public"]`; this
/// port performs the same grouping and returns the `public` entry (an empty
/// description when the database has no public objects).
pub async fn describe(store: &Store) -> Result<SchemaDescription, MigratorError> {
    let mut descriptions: BTreeMap<String, SchemaDescription> = BTreeMap::new();

    for extension in list_extensions(store).await? {
        descriptions
            .entry(extension.schema_name)
            .or_default()
            .extensions
            .push(extension.extension_name);
    }

    for enum_row in list_enums(store).await? {
        let description = descriptions.entry(enum_row.schema_name).or_default();
        if let Some(existing) = description
            .enums
            .iter_mut()
            .find(|entry| entry.name == enum_row.type_name)
        {
            existing.labels.push(enum_row.label);
        } else {
            description.enums.push(EnumDescription {
                name: enum_row.type_name,
                labels: vec![enum_row.label],
            });
        }
    }

    for function in list_functions(store).await? {
        descriptions
            .entry(function.schema_name)
            .or_default()
            .functions
            .push(FunctionDescription {
                name: function.function_name,
                definition: function.definition,
            });
    }

    for sequence in list_sequences(store).await? {
        descriptions
            .entry(sequence.schema_name)
            .or_default()
            .sequences
            .push(SequenceDescription {
                name: sequence.sequence_name,
                type_name: sequence.data_type,
                start_value: sequence.start_value,
                minimum_value: sequence.minimum_value,
                maximum_value: sequence.maximum_value,
                increment: sequence.increment,
                cycle_option: sequence.cycle_option,
            });
    }

    // Ordered map keyed by schema and then table, so Go's `sort.Strings` over
    // table names is implicit and deterministic.
    let mut table_map: BTreeMap<String, BTreeMap<String, TableDescription>> = BTreeMap::new();

    for TableRow {
        schema_name,
        table_name,
        comment,
    } in list_tables(store).await?
    {
        let name = table_name.clone();
        update_table_map(&mut table_map, &schema_name, &table_name, move |entry| {
            entry.name = name;
            entry.comment = comment.unwrap_or_default();
        });
    }

    for ColumnRow {
        schema_name,
        table_name,
        column_name,
        index,
        data_type,
        is_nullable,
        default,
        character_maximum_length,
        is_identity,
        identity_generation,
        is_generated,
        generation_expression,
        comment,
    } in list_columns(store).await?
    {
        update_table_map(&mut table_map, &schema_name, &table_name, move |table| {
            table.columns.push(ColumnDescription {
                name: column_name,
                index,
                type_name: data_type,
                is_nullable,
                default,
                character_maximum_length,
                is_identity,
                identity_generation,
                is_generated,
                generation_expression,
                comment,
            });
        });
    }

    for IndexRow {
        schema_name,
        table_name,
        index_name,
        is_primary_key,
        is_unique,
        is_exclusion,
        is_deferrable,
        index_definition,
        constraint_type,
        constraint_definition,
    } in list_indexes(store).await?
    {
        update_table_map(&mut table_map, &schema_name, &table_name, move |table| {
            table.indexes.push(IndexDescription {
                name: index_name,
                is_primary_key,
                is_unique,
                is_exclusion,
                is_deferrable,
                index_definition,
                constraint_type,
                constraint_definition,
            });
        });
    }

    for ConstraintRow {
        schema_name,
        table_name,
        constraint_name,
        constraint_type,
        is_deferrable,
        ref_table_name,
        constraint_definition,
    } in list_constraints(store).await?
    {
        update_table_map(&mut table_map, &schema_name, &table_name, move |table| {
            table.constraints.push(ConstraintDescription {
                name: constraint_name,
                constraint_type,
                ref_table_name,
                is_deferrable,
                constraint_definition,
            });
        });
    }

    for TriggerRow {
        schema_name,
        table_name,
        trigger_name,
        trigger_definition,
    } in list_triggers(store).await?
    {
        update_table_map(&mut table_map, &schema_name, &table_name, move |table| {
            table.triggers.push(TriggerDescription {
                name: trigger_name,
                definition: trigger_definition,
            });
        });
    }

    for (schema_name, tables) in table_map {
        let description = descriptions.entry(schema_name).or_default();
        for (_, table) in tables {
            description.tables.push(table);
        }
    }

    for view in list_views(store).await? {
        descriptions
            .entry(view.schema_name)
            .or_default()
            .views
            .push(ViewDescription {
                name: view.view_name,
                definition: view.definition,
            });
    }

    Ok(descriptions.remove("public").unwrap_or_default())
}

/// Go's `updateTableMap`: lazily create the schema/table entry and apply `f`.
fn update_table_map<F>(
    table_map: &mut BTreeMap<String, BTreeMap<String, TableDescription>>,
    schema_name: &str,
    table_name: &str,
    f: F,
) where
    F: FnOnce(&mut TableDescription),
{
    let tables = table_map.entry(schema_name.to_string()).or_default();
    let table = tables.entry(table_name.to_string()).or_default();
    f(table);
}

fn is_truthy(catalog_value: &str) -> bool {
    let lower = catalog_value.to_ascii_lowercase();
    lower == "yes" || lower == "true"
}

fn describe_error(context: &str, error: sqlx::Error) -> MigratorError {
    MigratorError::new(format!("{context}: {error}"))
}

// --- catalog rows ---------------------------------------------------------

struct ExtensionRow {
    schema_name: String,
    extension_name: String,
}

struct EnumRow {
    schema_name: String,
    type_name: String,
    label: String,
}

struct FunctionRow {
    schema_name: String,
    function_name: String,
    definition: String,
}

struct SequenceRow {
    schema_name: String,
    sequence_name: String,
    data_type: String,
    start_value: i64,
    minimum_value: i64,
    maximum_value: i64,
    increment: i64,
    cycle_option: String,
}

struct TableRow {
    schema_name: String,
    table_name: String,
    comment: Option<String>,
}

struct ColumnRow {
    schema_name: String,
    table_name: String,
    column_name: String,
    index: i32,
    data_type: String,
    is_nullable: bool,
    default: String,
    character_maximum_length: i32,
    is_identity: bool,
    identity_generation: String,
    is_generated: String,
    generation_expression: String,
    comment: String,
}

struct IndexRow {
    schema_name: String,
    table_name: String,
    index_name: String,
    is_primary_key: bool,
    is_unique: bool,
    is_exclusion: bool,
    is_deferrable: bool,
    index_definition: String,
    constraint_type: String,
    constraint_definition: String,
}

struct ConstraintRow {
    schema_name: String,
    table_name: String,
    constraint_name: String,
    constraint_type: String,
    is_deferrable: bool,
    ref_table_name: String,
    constraint_definition: String,
}

struct TriggerRow {
    schema_name: String,
    table_name: String,
    trigger_name: String,
    trigger_definition: String,
}

struct ViewRow {
    schema_name: String,
    view_name: String,
    definition: String,
}

// --- queries (ported from store/describe.go) ------------------------------

const LIST_EXTENSIONS_QUERY: &str = r#"
SELECT
	n.nspname AS schemaName,
	e.extname AS extensionName
FROM pg_catalog.pg_extension e
JOIN pg_catalog.pg_namespace n ON n.oid = e.extnamespace
WHERE
	n.nspname NOT LIKE 'pg_%' AND
	n.nspname NOT LIKE '_timescaledb_%' AND
	n.nspname != 'information_schema'
ORDER BY
	n.nspname,
	e.extname
"#;

const LIST_ENUMS_QUERY: &str = r#"
SELECT
	n.nspname AS schemaName,
	t.typname AS typeName,
	e.enumlabel AS label
FROM pg_catalog.pg_enum e
JOIN pg_catalog.pg_type t ON t.oid = e.enumtypid
JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace
WHERE
	n.nspname NOT LIKE 'pg_%' AND
	n.nspname NOT LIKE '_timescaledb_%' AND
	n.nspname != 'information_schema'
ORDER BY
	n.nspname,
	t.typname,
	e.enumsortorder
"#;

const LIST_FUNCTIONS_QUERY: &str = r#"
SELECT
	n.nspname AS schemaName,
	p.proname AS functionName,
	p.oid::regprocedure::text AS fancy,
	t.typname::text AS returnType,
	pg_get_functiondef(p.oid) AS definition
FROM pg_catalog.pg_proc p
JOIN pg_catalog.pg_type t ON t.oid = p.prorettype
JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
JOIN pg_language l ON l.oid = p.prolang AND l.lanname IN ('sql', 'plpgsql', 'c')
LEFT JOIN pg_depend d ON d.objid = p.oid AND d.deptype = 'e'
WHERE
	n.nspname NOT LIKE 'pg_%' AND
	n.nspname NOT LIKE '_timescaledb_%' AND
	n.nspname != 'information_schema' AND
	-- function is not defined in an extension
	d.objid IS NULL
ORDER BY
	n.nspname,
	p.proname
"#;

const LIST_SEQUENCES_QUERY: &str = r#"
SELECT
	s.sequence_schema::text AS schemaName,
	s.sequence_name::text AS sequenceName,
	s.data_type::text AS dataType,
	s.start_value::bigint AS startValue,
	s.minimum_value::bigint AS minimumValue,
	s.maximum_value::bigint AS maximumValue,
	s.increment::bigint AS increment,
	s.cycle_option::text AS cycleOption
FROM information_schema.sequences s
WHERE
	s.sequence_schema NOT LIKE 'pg_%' AND
	s.sequence_schema NOT LIKE '_timescaledb_%' AND
	s.sequence_schema != 'information_schema'
ORDER BY
	s.sequence_schema,
	s.sequence_name
"#;

const LIST_TABLES_QUERY: &str = r#"
SELECT
	t.table_schema::text AS schemaName,
	t.table_name::text AS tableName,
	obj_description(t.table_name::regclass) AS comment
FROM information_schema.tables t
WHERE
	t.table_type = 'BASE TABLE' AND
	t.table_schema NOT LIKE 'pg_%' AND
	t.table_schema NOT LIKE '_timescaledb_%' AND
	t.table_schema != 'information_schema'
ORDER BY
	t.table_schema,
	t.table_name
"#;

const LIST_COLUMNS_QUERY: &str = r#"
WITH
tables AS MATERIALIZED (
	SELECT
		t.table_schema,
		t.table_name
	FROM information_schema.tables t
	WHERE
		t.table_type = 'BASE TABLE' AND
		t.table_schema NOT LIKE 'pg_%' AND
		t.table_schema NOT LIKE '_timescaledb_%' AND
		t.table_schema != 'information_schema'
)
SELECT
	c.table_schema::text AS schemaName,
	c.table_name::text AS tableName,
	c.column_name::text AS columnName,
	c.ordinal_position::integer AS index,
	(CASE
		WHEN c.data_type = 'ARRAY' THEN COALESCE((
			SELECT e.data_type
			FROM information_schema.element_types e
			WHERE
				e.object_type = 'TABLE' AND
				e.object_catalog = c.table_catalog AND
				e.object_schema = c.table_schema AND
				e.object_name = c.table_name AND
				e.collection_type_identifier = c.dtd_identifier
		)) || '[]'
		WHEN c.data_type = 'USER-DEFINED'    THEN c.udt_name
		WHEN c.character_maximum_length != 0 THEN c.data_type || '(' || c.character_maximum_length::text || ')'
		ELSE c.data_type
	END)::text as dataType,
	c.is_nullable::text AS isNullable,
	c.column_default::text AS columnDefault,
	c.character_maximum_length::integer AS characterMaximumLength,
	c.is_identity::text AS isIdentity,
	c.identity_generation::text AS identityGeneration,
	c.is_generated::text AS isGenerated,
	c.generation_expression::text AS generationExpression,
	pg_catalog.col_description(c.table_name::regclass::oid, c.ordinal_position::int) AS comment
FROM information_schema.columns c
JOIN tables t ON
	t.table_schema = c.table_schema AND
	t.table_name = c.table_name
ORDER BY
	c.table_schema,
	c.table_name,
	c.column_name
"#;

const LIST_INDEXES_QUERY: &str = r#"
SELECT
	n.nspname AS schemaName,
	table_class.relname AS tableName,
	index_class.relname AS indexName,
	i.indisprimary AS isPrimaryKey,
	i.indisunique AS isUnique,
	i.indisexclusion AS isExclusion,
	con.condeferrable AS isDeferrable,
	pg_catalog.pg_get_indexdef(i.indexrelid, 0, true) AS indexDefinition,
	con.contype::text AS constraintType,
	pg_catalog.pg_get_constraintdef(con.oid, true) AS constraintDefinition
FROM pg_catalog.pg_index i
JOIN pg_catalog.pg_class table_class ON table_class.oid = i.indrelid
JOIN pg_catalog.pg_class index_class ON index_class.oid = i.indexrelid
JOIN pg_catalog.pg_namespace n ON n.oid = table_class.relnamespace
LEFT OUTER JOIN pg_catalog.pg_constraint con ON (
	con.conrelid = i.indrelid AND
	con.conindid = i.indexrelid AND
	con.contype IN ('p', 'u', 'x')
)
WHERE
	n.nspname NOT LIKE 'pg_%' AND
	n.nspname NOT LIKE '_timescaledb_%' AND
	n.nspname != 'information_schema'
ORDER BY
	n.nspname,
	table_class.relname,
	index_class.relname
"#;

const LIST_CONSTRAINTS_QUERY: &str = r#"
SELECT
	n.nspname AS schemaName,
	table_class.relname AS tableName,
	con.conname AS constraintName,
	con.contype::text AS constraintType,
	con.condeferrable AS isDeferrable,
	reftable_class.relname AS refTableName,
	pg_catalog.pg_get_constraintdef(con.oid, true) AS constraintDefinition
FROM pg_catalog.pg_constraint con
JOIN pg_catalog.pg_class table_class ON table_class.oid = con.conrelid
JOIN pg_catalog.pg_namespace n ON n.oid = table_class.relnamespace
LEFT OUTER JOIN pg_catalog.pg_class reftable_class ON reftable_class.oid = con.confrelid
WHERE
	n.nspname NOT LIKE 'pg_%' AND
	n.nspname NOT LIKE '_timescaledb_%' AND
	n.nspname != 'information_schema' AND
	con.contype IN ('c', 'f', 't')
ORDER BY
	n.nspname,
	table_class.relname,
	con.conname
"#;

const LIST_TRIGGERS_QUERY: &str = r#"
SELECT
	n.nspname AS schemaName,
	c.relname AS tableName,
	t.tgname AS triggerName,
	pg_catalog.pg_get_triggerdef(t.oid, true) AS triggerDefinition
FROM pg_catalog.pg_trigger t
JOIN pg_catalog.pg_class c ON c.oid = t.tgrelid
JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
WHERE
	n.nspname NOT LIKE 'pg_%' AND
	n.nspname NOT LIKE '_timescaledb_%' AND
	n.nspname != 'information_schema' AND
	NOT t.tgisinternal
ORDER BY
	n.nspname,
	c.relname,
	t.tgname
"#;

const LIST_VIEWS_QUERY: &str = r#"
SELECT
	v.schemaname AS schemaName,
	v.viewname AS viewName,
	v.definition AS definition
FROM pg_catalog.pg_views v
WHERE
	v.schemaname NOT LIKE 'pg_%' AND
	v.schemaname NOT LIKE '_timescaledb_%' AND
	v.schemaname != 'information_schema' AND
	v.viewname NOT LIKE 'pg_stat_%'
ORDER BY
	v.schemaname,
	v.viewname
"#;

// --- row loaders ----------------------------------------------------------

async fn list_extensions(store: &Store) -> Result<Vec<ExtensionRow>, MigratorError> {
    let rows = sqlx::query(LIST_EXTENSIONS_QUERY)
        .fetch_all(&store.pool)
        .await
        .map_err(|error| describe_error("store.listExtensions", error))?;

    rows.into_iter()
        .map(|row| {
            Ok(ExtensionRow {
                schema_name: row
                    .try_get(0)
                    .map_err(|error| describe_error("store.listExtensions", error))?,
                extension_name: row
                    .try_get(1)
                    .map_err(|error| describe_error("store.listExtensions", error))?,
            })
        })
        .collect()
}

async fn list_enums(store: &Store) -> Result<Vec<EnumRow>, MigratorError> {
    let rows = sqlx::query(LIST_ENUMS_QUERY)
        .fetch_all(&store.pool)
        .await
        .map_err(|error| describe_error("store.listEnums", error))?;

    rows.into_iter()
        .map(|row| {
            Ok(EnumRow {
                schema_name: row
                    .try_get(0)
                    .map_err(|error| describe_error("store.listEnums", error))?,
                type_name: row
                    .try_get(1)
                    .map_err(|error| describe_error("store.listEnums", error))?,
                label: row
                    .try_get(2)
                    .map_err(|error| describe_error("store.listEnums", error))?,
            })
        })
        .collect()
}

async fn list_functions(store: &Store) -> Result<Vec<FunctionRow>, MigratorError> {
    let rows = sqlx::query(LIST_FUNCTIONS_QUERY)
        .fetch_all(&store.pool)
        .await
        .map_err(|error| describe_error("store.listFunctions", error))?;

    rows.into_iter()
        .map(|row| {
            // Columns 2 and 3 (`fancy`, `returnType`) are selected for parity
            // with Go but are not part of the description.
            let _fancy: String = row
                .try_get(2)
                .map_err(|error| describe_error("store.listFunctions", error))?;
            let _return_type: String = row
                .try_get(3)
                .map_err(|error| describe_error("store.listFunctions", error))?;

            Ok(FunctionRow {
                schema_name: row
                    .try_get(0)
                    .map_err(|error| describe_error("store.listFunctions", error))?,
                function_name: row
                    .try_get(1)
                    .map_err(|error| describe_error("store.listFunctions", error))?,
                definition: row
                    .try_get(4)
                    .map_err(|error| describe_error("store.listFunctions", error))?,
            })
        })
        .collect()
}

async fn list_sequences(store: &Store) -> Result<Vec<SequenceRow>, MigratorError> {
    let rows = sqlx::query(LIST_SEQUENCES_QUERY)
        .fetch_all(&store.pool)
        .await
        .map_err(|error| describe_error("store.listSequences", error))?;

    rows.into_iter()
        .map(|row| {
            Ok(SequenceRow {
                schema_name: row
                    .try_get(0)
                    .map_err(|error| describe_error("store.listSequences", error))?,
                sequence_name: row
                    .try_get(1)
                    .map_err(|error| describe_error("store.listSequences", error))?,
                data_type: row
                    .try_get(2)
                    .map_err(|error| describe_error("store.listSequences", error))?,
                start_value: row
                    .try_get(3)
                    .map_err(|error| describe_error("store.listSequences", error))?,
                minimum_value: row
                    .try_get(4)
                    .map_err(|error| describe_error("store.listSequences", error))?,
                maximum_value: row
                    .try_get(5)
                    .map_err(|error| describe_error("store.listSequences", error))?,
                increment: row
                    .try_get(6)
                    .map_err(|error| describe_error("store.listSequences", error))?,
                cycle_option: row
                    .try_get(7)
                    .map_err(|error| describe_error("store.listSequences", error))?,
            })
        })
        .collect()
}

async fn list_tables(store: &Store) -> Result<Vec<TableRow>, MigratorError> {
    let rows = sqlx::query(LIST_TABLES_QUERY)
        .fetch_all(&store.pool)
        .await
        .map_err(|error| describe_error("store.listTables", error))?;

    rows.into_iter()
        .map(|row| {
            Ok(TableRow {
                schema_name: row
                    .try_get(0)
                    .map_err(|error| describe_error("store.listTables", error))?,
                table_name: row
                    .try_get(1)
                    .map_err(|error| describe_error("store.listTables", error))?,
                comment: row
                    .try_get(2)
                    .map_err(|error| describe_error("store.listTables", error))?,
            })
        })
        .collect()
}

async fn list_columns(store: &Store) -> Result<Vec<ColumnRow>, MigratorError> {
    let rows = sqlx::query(LIST_COLUMNS_QUERY)
        .fetch_all(&store.pool)
        .await
        .map_err(|error| describe_error("store.listColumns", error))?;

    rows.into_iter()
        .map(|row| {
            let is_nullable: String = row
                .try_get(5)
                .map_err(|error| describe_error("store.listColumns", error))?;
            let is_identity: String = row
                .try_get(8)
                .map_err(|error| describe_error("store.listColumns", error))?;

            Ok(ColumnRow {
                schema_name: row
                    .try_get(0)
                    .map_err(|error| describe_error("store.listColumns", error))?,
                table_name: row
                    .try_get(1)
                    .map_err(|error| describe_error("store.listColumns", error))?,
                column_name: row
                    .try_get(2)
                    .map_err(|error| describe_error("store.listColumns", error))?,
                index: row
                    .try_get(3)
                    .map_err(|error| describe_error("store.listColumns", error))?,
                data_type: row
                    .try_get(4)
                    .map_err(|error| describe_error("store.listColumns", error))?,
                is_nullable: is_truthy(&is_nullable),
                default: row
                    .try_get::<Option<String>, _>(6)
                    .map_err(|error| describe_error("store.listColumns", error))?
                    .unwrap_or_default(),
                character_maximum_length: row
                    .try_get::<Option<i32>, _>(7)
                    .map_err(|error| describe_error("store.listColumns", error))?
                    .unwrap_or_default(),
                is_identity: is_truthy(&is_identity),
                identity_generation: row
                    .try_get::<Option<String>, _>(9)
                    .map_err(|error| describe_error("store.listColumns", error))?
                    .unwrap_or_default(),
                is_generated: row
                    .try_get(10)
                    .map_err(|error| describe_error("store.listColumns", error))?,
                generation_expression: row
                    .try_get::<Option<String>, _>(11)
                    .map_err(|error| describe_error("store.listColumns", error))?
                    .unwrap_or_default(),
                comment: row
                    .try_get::<Option<String>, _>(12)
                    .map_err(|error| describe_error("store.listColumns", error))?
                    .unwrap_or_default(),
            })
        })
        .collect()
}

async fn list_indexes(store: &Store) -> Result<Vec<IndexRow>, MigratorError> {
    let rows = sqlx::query(LIST_INDEXES_QUERY)
        .fetch_all(&store.pool)
        .await
        .map_err(|error| describe_error("store.listIndexes", error))?;

    rows.into_iter()
        .map(|row| {
            Ok(IndexRow {
                schema_name: row
                    .try_get(0)
                    .map_err(|error| describe_error("store.listIndexes", error))?,
                table_name: row
                    .try_get(1)
                    .map_err(|error| describe_error("store.listIndexes", error))?,
                index_name: row
                    .try_get(2)
                    .map_err(|error| describe_error("store.listIndexes", error))?,
                is_primary_key: row
                    .try_get(3)
                    .map_err(|error| describe_error("store.listIndexes", error))?,
                is_unique: row
                    .try_get(4)
                    .map_err(|error| describe_error("store.listIndexes", error))?,
                is_exclusion: row
                    .try_get::<Option<bool>, _>(5)
                    .map_err(|error| describe_error("store.listIndexes", error))?
                    .unwrap_or(false),
                is_deferrable: row
                    .try_get::<Option<bool>, _>(6)
                    .map_err(|error| describe_error("store.listIndexes", error))?
                    .unwrap_or(false),
                index_definition: row
                    .try_get(7)
                    .map_err(|error| describe_error("store.listIndexes", error))?,
                constraint_type: row
                    .try_get::<Option<String>, _>(8)
                    .map_err(|error| describe_error("store.listIndexes", error))?
                    .unwrap_or_default(),
                constraint_definition: row
                    .try_get::<Option<String>, _>(9)
                    .map_err(|error| describe_error("store.listIndexes", error))?
                    .unwrap_or_default(),
            })
        })
        .collect()
}

async fn list_constraints(store: &Store) -> Result<Vec<ConstraintRow>, MigratorError> {
    let rows = sqlx::query(LIST_CONSTRAINTS_QUERY)
        .fetch_all(&store.pool)
        .await
        .map_err(|error| describe_error("store.listConstraints", error))?;

    rows.into_iter()
        .map(|row| {
            Ok(ConstraintRow {
                schema_name: row
                    .try_get(0)
                    .map_err(|error| describe_error("store.listConstraints", error))?,
                table_name: row
                    .try_get(1)
                    .map_err(|error| describe_error("store.listConstraints", error))?,
                constraint_name: row
                    .try_get(2)
                    .map_err(|error| describe_error("store.listConstraints", error))?,
                constraint_type: row
                    .try_get(3)
                    .map_err(|error| describe_error("store.listConstraints", error))?,
                is_deferrable: row
                    .try_get::<Option<bool>, _>(4)
                    .map_err(|error| describe_error("store.listConstraints", error))?
                    .unwrap_or(false),
                ref_table_name: row
                    .try_get::<Option<String>, _>(5)
                    .map_err(|error| describe_error("store.listConstraints", error))?
                    .unwrap_or_default(),
                constraint_definition: row
                    .try_get(6)
                    .map_err(|error| describe_error("store.listConstraints", error))?,
            })
        })
        .collect()
}

async fn list_triggers(store: &Store) -> Result<Vec<TriggerRow>, MigratorError> {
    let rows = sqlx::query(LIST_TRIGGERS_QUERY)
        .fetch_all(&store.pool)
        .await
        .map_err(|error| describe_error("store.listTriggers", error))?;

    rows.into_iter()
        .map(|row| {
            Ok(TriggerRow {
                schema_name: row
                    .try_get(0)
                    .map_err(|error| describe_error("store.listTriggers", error))?,
                table_name: row
                    .try_get(1)
                    .map_err(|error| describe_error("store.listTriggers", error))?,
                trigger_name: row
                    .try_get(2)
                    .map_err(|error| describe_error("store.listTriggers", error))?,
                trigger_definition: row
                    .try_get(3)
                    .map_err(|error| describe_error("store.listTriggers", error))?,
            })
        })
        .collect()
}

async fn list_views(store: &Store) -> Result<Vec<ViewRow>, MigratorError> {
    let rows = sqlx::query(LIST_VIEWS_QUERY)
        .fetch_all(&store.pool)
        .await
        .map_err(|error| describe_error("store.listViews", error))?;

    rows.into_iter()
        .map(|row| {
            Ok(ViewRow {
                schema_name: row
                    .try_get(0)
                    .map_err(|error| describe_error("store.listViews", error))?,
                view_name: row
                    .try_get(1)
                    .map_err(|error| describe_error("store.listViews", error))?,
                definition: row
                    .try_get(2)
                    .map_err(|error| describe_error("store.listViews", error))?,
            })
        })
        .collect()
}

// ---------------------------------------------------------------------------
// schemas.Canonicalize
// ---------------------------------------------------------------------------

/// `schemas.Canonicalize`: sort every collection so two equivalent
/// descriptions compare equal and render deterministically.
pub fn canonicalize(description: &mut SchemaDescription) {
    for table in &mut description.tables {
        sort_columns_by_name(&mut table.columns);
        sort_indexes(&mut table.indexes);
        sort_constraints(&mut table.constraints);
        sort_triggers(&mut table.triggers);
    }

    sort_enums(&mut description.enums);
    sort_functions(&mut description.functions);
    sort_sequences(&mut description.sequences);
    sort_tables(&mut description.tables);
    sort_views(&mut description.views);
}

fn sort_tables(tables: &mut [TableDescription]) {
    tables.sort_by(|a, b| a.name.cmp(&b.name));
}

fn sort_views(views: &mut [ViewDescription]) {
    views.sort_by(|a, b| a.name.cmp(&b.name));
}

fn sort_columns(columns: &mut [ColumnDescription]) {
    columns.sort_by(|a, b| a.index.cmp(&b.index));
}

fn sort_columns_by_name(columns: &mut [ColumnDescription]) {
    columns.sort_by(|a, b| a.name.cmp(&b.name));
}

fn sort_indexes(indexes: &mut [IndexDescription]) {
    indexes.sort_by(|a, b| match (a.is_unique, b.is_unique) {
        (true, false) => std::cmp::Ordering::Less,
        (false, true) => std::cmp::Ordering::Greater,
        _ => a.name.cmp(&b.name),
    });
}

fn sort_constraints(constraints: &mut [ConstraintDescription]) {
    constraints.sort_by(|a, b| a.name.cmp(&b.name));
}

fn sort_triggers(triggers: &mut [TriggerDescription]) {
    triggers.sort_by(|a, b| a.name.cmp(&b.name));
}

fn sort_enums(enums: &mut [EnumDescription]) {
    enums.sort_by(|a, b| a.name.cmp(&b.name));
}

fn sort_functions(functions: &mut [FunctionDescription]) {
    functions.sort_by(|a, b| {
        a.name
            .cmp(&b.name)
            .then_with(|| a.definition.cmp(&b.definition))
    });
}

fn sort_sequences(sequences: &mut [SequenceDescription]) {
    sequences.sort_by(|a, b| a.name.cmp(&b.name));
}

// ---------------------------------------------------------------------------
// formatters
// ---------------------------------------------------------------------------

/// `schemas.NewPSQLFormatter().Format`.
///
/// The Go formatter renders a terminal/psql-style description: one section per
/// table (columns table, indexes, constraints, referenced-by, triggers),
/// followed by views and enum types. Extensions, functions and sequences are
/// part of the description but are intentionally not rendered by this
/// formatter, matching the baseline.
pub fn format_psql(description: &SchemaDescription) -> String {
    let mut tables = description.tables.clone();
    sort_tables(&mut tables);

    let mut views = description.views.clone();
    sort_views(&mut views);

    let mut docs: Vec<String> = Vec::new();
    for table in &tables {
        docs.extend(format_table(&tables, table));
    }
    for view in &views {
        docs.extend(format_view(view));
    }

    let mut types: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for enumeration in &description.enums {
        types.insert(enumeration.name.clone(), enumeration.labels.clone());
    }
    if !types.is_empty() {
        docs.extend(format_types(&types));
    }

    docs.join("\n")
}

fn format_table(all_tables: &[TableDescription], table: &TableDescription) -> Vec<String> {
    fn format_columns(headers: &[&str], rows: &[Vec<String>]) -> Vec<String> {
        let mut sizes: Vec<usize> = headers.iter().map(|header| header.len()).collect();
        for row in rows {
            for (index, value) in row.iter().enumerate() {
                if value.len() > sizes[index] {
                    sizes[index] = value.len();
                }
            }
        }

        let header_values: Vec<String> = headers
            .iter()
            .enumerate()
            .map(|(index, header)| center_string(header, sizes[index] + 2))
            .collect();
        let separator_values: Vec<String> = sizes.iter().map(|size| "-".repeat(size + 2)).collect();

        let mut docs = vec![header_values.join("|"), separator_values.join("+")];
        for row in rows {
            let mut values = Vec::with_capacity(headers.len());
            for index in 0..headers.len() {
                if index == headers.len() - 1 {
                    values.push(row[index].clone());
                } else {
                    values.push(format!("{:<width$}", row[index], width = sizes[index]));
                }
            }
            docs.push(format!(" {}", values.join(" | ")));
        }

        docs
    }

    let mut docs: Vec<String> = Vec::new();
    docs.push(format!("# Table \"public.{}\"", table.name));
    docs.push("```".to_string());

    let headers = ["Column", "Type", "Collation", "Nullable", "Default"];
    let mut columns = table.columns.clone();
    sort_columns(&mut columns);

    let mut rows: Vec<Vec<String>> = Vec::with_capacity(columns.len());
    for column in &columns {
        let null_constraint = if column.is_nullable { "" } else { "not null" };
        let default_value = if column.is_generated == "ALWAYS" {
            format!(
                "generated always as ({}) stored",
                column.generation_expression
            )
        } else {
            column.default.clone()
        };

        rows.push(vec![
            column.name.clone(),
            column.type_name.clone(),
            String::new(),
            null_constraint.to_string(),
            default_value,
        ]);
    }
    docs.extend(format_columns(&headers, &rows));

    if !table.indexes.is_empty() {
        docs.push("Indexes:".to_string());
        let mut indexes = table.indexes.clone();
        sort_indexes(&mut indexes);

        for index in &indexes {
            if index.is_primary_key {
                let definition = using_tail(&index.index_definition);
                docs.push(format!(
                    "    {} PRIMARY KEY, {}",
                    go_quote(&index.name),
                    definition
                ));
            }
        }
        for index in &indexes {
            if !index.is_primary_key {
                let unique = if index.is_unique {
                    let constraint = if index.constraint_type == "u" {
                        " CONSTRAINT"
                    } else {
                        ""
                    };
                    format!(" UNIQUE{},", constraint)
                } else {
                    String::new()
                };
                let deferrable = if index.is_deferrable {
                    " DEFERRABLE"
                } else {
                    ""
                };
                let mut definition = using_tail(&index.index_definition).to_string();
                if index.is_exclusion {
                    definition = format!("EXCLUDE USING {}", definition);
                }
                docs.push(format!(
                    "    {}{} {}{}",
                    go_quote(&index.name),
                    unique,
                    definition,
                    deferrable
                ));
            }
        }
    }

    let num_check_constraints = table
        .constraints
        .iter()
        .filter(|constraint| constraint.constraint_type == "c")
        .count();
    let num_foreign_key_constraints = table
        .constraints
        .iter()
        .filter(|constraint| constraint.constraint_type == "f")
        .count();

    if num_check_constraints > 0 {
        docs.push("Check constraints:".to_string());
        for constraint in &table.constraints {
            if constraint.constraint_type == "c" {
                docs.push(format!(
                    "    {} {}",
                    go_quote(&constraint.name),
                    constraint.constraint_definition
                ));
            }
        }
    }
    if num_foreign_key_constraints > 0 {
        docs.push("Foreign-key constraints:".to_string());
        for constraint in &table.constraints {
            if constraint.constraint_type == "f" {
                docs.push(format!(
                    "    {} {}",
                    go_quote(&constraint.name),
                    constraint.constraint_definition
                ));
            }
        }
    }

    let mut referenced: Vec<(&TableDescription, &ConstraintDescription)> = Vec::new();
    for other_table in all_tables {
        for constraint in &other_table.constraints {
            if constraint.ref_table_name == table.name {
                referenced.push((other_table, constraint));
            }
        }
    }
    referenced.sort_by(|a, b| a.1.name.cmp(&b.1.name));
    if !referenced.is_empty() {
        docs.push("Referenced by:".to_string());
        for (other_table, constraint) in referenced {
            docs.push(format!(
                "    TABLE {} CONSTRAINT {} {}",
                go_quote(&other_table.name),
                go_quote(&constraint.name),
                constraint.constraint_definition
            ));
        }
    }

    if !table.triggers.is_empty() {
        docs.push("Triggers:".to_string());
        for trigger in &table.triggers {
            let definition = trigger
                .definition
                .splitn(2, trigger.name.as_str())
                .nth(1)
                .unwrap_or("")
                .trim();
            docs.push(format!("    {} {}", trigger.name, definition));
        }
    }

    docs.push("\n```\n".to_string());

    if !table.comment.is_empty() {
        docs.push(format!("{}\n", html_escape(&table.comment)));
    }

    let mut columns_by_name = columns.clone();
    sort_columns_by_name(&mut columns_by_name);
    for column in &columns_by_name {
        if !column.comment.is_empty() {
            docs.push(format!(
                "**{}**: {}\n",
                column.name,
                html_escape(&column.comment)
            ));
        }
    }

    docs
}

fn format_view(view: &ViewDescription) -> Vec<String> {
    vec![
        format!("# View \"public.{}\"\n", view.name),
        format!("## View query:\n\n```sql\n{}\n```\n", view.definition),
    ]
}

fn format_types(types: &BTreeMap<String, Vec<String>>) -> Vec<String> {
    let mut docs = Vec::with_capacity(types.len() * 4);
    for (name, labels) in types {
        docs.push(format!("# Type {}", name));
        docs.push(String::new());
        docs.push(format!("- {}", labels.join("\n- ")));
        docs.push(String::new());
    }
    docs
}

fn center_string(value: &str, width: usize) -> String {
    let delta = width as f64 - value.len() as f64;
    let mut left = (delta / 2.0).floor() as i64;
    if left <= 0 {
        left = 1;
    }
    let mut right = (delta / 2.0).ceil() as i64;
    if right <= 0 {
        right = 1;
    }

    format!(
        "{}{}{}",
        " ".repeat(left as usize),
        value,
        " ".repeat(right as usize)
    )
}

/// Go's `strings.TrimSpace(strings.Split(definition, "USING")[1])`.
///
/// The fallback avoids the panic Go would hit on a definition without
/// `USING`.
fn using_tail(definition: &str) -> &str {
    definition
        .split("USING")
        .nth(1)
        .map(str::trim)
        .unwrap_or_else(|| definition.trim())
}

/// Go's `%q` for the identifiers this formatter prints.
fn go_quote(value: &str) -> String {
    let mut quoted = String::with_capacity(value.len() + 2);
    quoted.push('"');
    for character in value.chars() {
        match character {
            '"' => quoted.push_str("\\\""),
            '\\' => quoted.push_str("\\\\"),
            '\n' => quoted.push_str("\\n"),
            '\r' => quoted.push_str("\\r"),
            '\t' => quoted.push_str("\\t"),
            character if (character as u32) < 0x20 || (character as u32) == 0x7f => {
                quoted.push_str(&format!("\\x{:02x}", character as u32));
            }
            character => quoted.push(character),
        }
    }
    quoted.push('"');
    quoted
}

/// Go's `html.EscapeString`.
fn html_escape(value: &str) -> String {
    let mut escaped = String::with_capacity(value.len());
    for character in value.chars() {
        match character {
            '&' => escaped.push_str("&amp;"),
            '\'' => escaped.push_str("&#39;"),
            '<' => escaped.push_str("&lt;"),
            '>' => escaped.push_str("&gt;"),
            '"' => escaped.push_str("&#34;"),
            character => escaped.push(character),
        }
    }
    escaped
}

/// `schemas.NewJSONFormatter().Format`.
///
/// The Go formatter canonicalizes a copy and then uses
/// `json.MarshalIndent(_, "", "  ")`. Field order follows the struct
/// declaration order (see the struct definitions).
pub fn format_json(description: &SchemaDescription) -> Result<String, MigratorError> {
    let mut description = description.clone();
    canonicalize(&mut description);
    let mut out = String::new();
    write_json(&schema_json(&description), &mut out, 0);
    Ok(out)
}

/// A minimal JSON tree that preserves Go's struct field order; serde_json's
/// `Value` sorts object keys, which would break byte-parity with
/// `json.MarshalIndent`.
enum J {
    Null,
    Bool(bool),
    Num(i64),
    Str(String),
    Arr(Vec<J>),
    Obj(Vec<(&'static str, J)>),
}

fn schema_json(description: &SchemaDescription) -> J {
    J::Obj(vec![
        ("Extensions", nil_if_empty(description.extensions.iter().map(|value| J::Str(value.clone())).collect())),
        ("Enums", nil_if_empty(description.enums.iter().map(|value| J::Obj(vec![
            ("Name", J::Str(value.name.clone())),
            ("Labels", J::Arr(value.labels.iter().cloned().map(J::Str).collect())),
        ])).collect())),
        ("Functions", nil_if_empty(description.functions.iter().map(|value| J::Obj(vec![
            ("Name", J::Str(value.name.clone())),
            ("Definition", J::Str(value.definition.clone())),
        ])).collect())),
        ("Sequences", nil_if_empty(description.sequences.iter().map(|value| J::Obj(vec![
            ("Name", J::Str(value.name.clone())),
            ("TypeName", J::Str(value.type_name.clone())),
            ("StartValue", J::Num(value.start_value)),
            ("MinimumValue", J::Num(value.minimum_value)),
            ("MaximumValue", J::Num(value.maximum_value)),
            ("Increment", J::Num(value.increment)),
            ("CycleOption", J::Str(value.cycle_option.clone())),
        ])).collect())),
        ("Tables", nil_if_empty(description.tables.iter().map(table_json).collect())),
        ("Views", nil_if_empty(description.views.iter().map(|value| J::Obj(vec![
            ("Name", J::Str(value.name.clone())),
            ("Definition", J::Str(value.definition.clone())),
        ])).collect())),
    ])
}

fn table_json(table: &TableDescription) -> J {
    J::Obj(vec![
        ("Name", J::Str(table.name.clone())),
        ("Comment", J::Str(table.comment.clone())),
        ("Columns", J::Arr(table.columns.iter().map(|column| J::Obj(vec![
            ("Name", J::Str(column.name.clone())),
            ("Index", J::Num(i64::from(column.index))),
            ("TypeName", J::Str(column.type_name.clone())),
            ("IsNullable", J::Bool(column.is_nullable)),
            ("Default", J::Str(column.default.clone())),
            ("CharacterMaximumLength", J::Num(i64::from(column.character_maximum_length))),
            ("IsIdentity", J::Bool(column.is_identity)),
            ("IdentityGeneration", J::Str(column.identity_generation.clone())),
            ("IsGenerated", J::Str(column.is_generated.clone())),
            ("GenerationExpression", J::Str(column.generation_expression.clone())),
            ("Comment", J::Str(column.comment.clone())),
        ])).collect())),
        ("Indexes", J::Arr(table.indexes.iter().map(|index| J::Obj(vec![
            ("Name", J::Str(index.name.clone())),
            ("IsPrimaryKey", J::Bool(index.is_primary_key)),
            ("IsUnique", J::Bool(index.is_unique)),
            ("IsExclusion", J::Bool(index.is_exclusion)),
            ("IsDeferrable", J::Bool(index.is_deferrable)),
            ("IndexDefinition", J::Str(index.index_definition.clone())),
            ("ConstraintType", J::Str(index.constraint_type.clone())),
            ("ConstraintDefinition", J::Str(index.constraint_definition.clone())),
        ])).collect())),
        // Go leaves Constraints nil unless a constraint row was appended.
        ("Constraints", if table.constraints.is_empty() { J::Null } else { J::Arr(table.constraints.iter().map(|constraint| J::Obj(vec![
            ("Name", J::Str(constraint.name.clone())),
            ("ConstraintType", J::Str(constraint.constraint_type.clone())),
            ("RefTableName", J::Str(constraint.ref_table_name.clone())),
            ("IsDeferrable", J::Bool(constraint.is_deferrable)),
            ("ConstraintDefinition", J::Str(constraint.constraint_definition.clone())),
        ])).collect()) }),
        ("Triggers", J::Arr(table.triggers.iter().map(|trigger| J::Obj(vec![
            ("Name", J::Str(trigger.name.clone())),
            ("Definition", J::Str(trigger.definition.clone())),
        ])).collect())),
    ])
}

fn nil_if_empty(values: Vec<J>) -> J {
    if values.is_empty() {
        J::Null
    } else {
        J::Arr(values)
    }
}

fn write_json(value: &J, out: &mut String, depth: usize) {
    match value {
        J::Null => out.push_str("null"),
        J::Bool(value) => out.push_str(if *value { "true" } else { "false" }),
        J::Num(value) => out.push_str(&value.to_string()),
        J::Str(value) => write_go_string(value, out),
        J::Arr(values) => {
            if values.is_empty() {
                out.push_str("[]");
                return;
            }
            out.push('[');
            out.push('\n');
            for (index, value) in values.iter().enumerate() {
                push_json_indent(out, depth + 1);
                write_json(value, out, depth + 1);
                if index + 1 != values.len() {
                    out.push(',');
                }
                out.push('\n');
            }
            push_json_indent(out, depth);
            out.push(']');
        }
        J::Obj(entries) => {
            out.push('{');
            out.push('\n');
            for (index, (key, value)) in entries.iter().enumerate() {
                push_json_indent(out, depth + 1);
                write_go_string(key, out);
                out.push_str(": ");
                write_json(value, out, depth + 1);
                if index + 1 != entries.len() {
                    out.push(',');
                }
                out.push('\n');
            }
            push_json_indent(out, depth);
            out.push('}');
        }
    }
}

fn push_json_indent(out: &mut String, depth: usize) {
    for _ in 0..depth {
        out.push_str("  ");
    }
}

/// Go `json.Marshal` string escaping with HTML escaping enabled.
fn write_go_string(value: &str, out: &mut String) {
    out.push('"');
    for character in value.chars() {
        match character {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            '&' => out.push_str("\\u0026"),
            '<' => out.push_str("\\u003c"),
            '>' => out.push_str("\\u003e"),
            '\u{2028}' => out.push_str("\\u2028"),
            '\u{2029}' => out.push_str("\\u2029"),
            character if (character as u32) < 0x20 => {
                out.push_str(&format!("\\u{:04x}", character as u32))
            }
            character => out.push(character),
        }
    }
    out.push('"');
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture() -> SchemaDescription {
        SchemaDescription {
            extensions: vec!["pgcrypto".to_string()],
            enums: vec![EnumDescription {
                name: "mpaa_rating".to_string(),
                labels: vec!["G".to_string(), "PG".to_string()],
            }],
            functions: vec![FunctionDescription {
                name: "f".to_string(),
                definition:
                    "CREATE FUNCTION f() RETURNS integer LANGUAGE sql AS $function$ SELECT 1; $function$"
                        .to_string(),
            }],
            sequences: vec![SequenceDescription {
                name: "t_id_seq".to_string(),
                type_name: "integer".to_string(),
                start_value: 1,
                minimum_value: 1,
                maximum_value: 2_147_483_647,
                increment: 1,
                cycle_option: "NO".to_string(),
            }],
            tables: vec![TableDescription {
                name: "t".to_string(),
                comment: String::new(),
                columns: vec![
                    ColumnDescription {
                        name: "id".to_string(),
                        index: 1,
                        type_name: "integer".to_string(),
                        is_nullable: false,
                        default: "nextval('t_id_seq'::regclass)".to_string(),
                        character_maximum_length: 0,
                        is_identity: false,
                        identity_generation: String::new(),
                        is_generated: "NEVER".to_string(),
                        generation_expression: String::new(),
                        comment: String::new(),
                    },
                    ColumnDescription {
                        name: "name".to_string(),
                        index: 2,
                        type_name: "text".to_string(),
                        is_nullable: true,
                        default: String::new(),
                        character_maximum_length: 0,
                        is_identity: false,
                        identity_generation: String::new(),
                        is_generated: "NEVER".to_string(),
                        generation_expression: String::new(),
                        comment: String::new(),
                    },
                ],
                indexes: vec![IndexDescription {
                    name: "t_pkey".to_string(),
                    is_primary_key: true,
                    is_unique: true,
                    is_exclusion: false,
                    is_deferrable: false,
                    index_definition:
                        "CREATE UNIQUE INDEX t_pkey ON public.t USING btree (id)".to_string(),
                    constraint_type: "p".to_string(),
                    constraint_definition: "PRIMARY KEY (id)".to_string(),
                }],
                constraints: vec![ConstraintDescription {
                    name: "t_name_check".to_string(),
                    constraint_type: "c".to_string(),
                    ref_table_name: String::new(),
                    is_deferrable: false,
                    constraint_definition: "CHECK ((name <> ''::text))".to_string(),
                }],
                triggers: Vec::new(),
            }],
            views: vec![ViewDescription {
                name: "v_t".to_string(),
                definition: " SELECT t.id,\n    t.name\n   FROM public.t;".to_string(),
            }],
        }
    }

    #[test]
    fn is_truthy_matches_sql_spec_values() {
        assert!(is_truthy("YES"));
        assert!(is_truthy("true"));
        assert!(!is_truthy("NO"));
        assert!(!is_truthy("false"));
        assert!(!is_truthy(""));
    }

    #[test]
    fn psql_renders_the_fixture_deterministically() {
        // Go's column formatter pads every cell, so the header line and the
        // row whose final column is empty both end in spaces.
        let expected = concat!(
            "# Table \"public.t\"\n",
            "```\n",
            " Column |  Type   | Collation | Nullable |            Default            \n",
            "--------+---------+-----------+----------+-------------------------------\n",
            " id     | integer |           | not null | nextval('t_id_seq'::regclass)\n",
            " name   | text    |           |          | \n",
            "Indexes:\n",
            "    \"t_pkey\" PRIMARY KEY, btree (id)\n",
            "Check constraints:\n",
            "    \"t_name_check\" CHECK ((name <> ''::text))\n",
            "\n",
            "```\n",
            "\n",
            "# View \"public.v_t\"\n",
            "\n",
            "## View query:\n",
            "\n",
            "```sql\n",
            " SELECT t.id,\n",
            "    t.name\n",
            "   FROM public.t;\n",
            "```\n",
            "\n",
            "# Type mpaa_rating\n",
            "\n",
            "- G\n",
            "- PG\n",
        );

        assert_eq!(format_psql(&fixture()), expected);
    }

    #[test]
    fn json_matches_go_field_names_and_order() {
        let expected = r#"{
  "Extensions": [
    "pgcrypto"
  ],
  "Enums": [
    {
      "Name": "mpaa_rating",
      "Labels": [
        "G",
        "PG"
      ]
    }
  ],
  "Functions": [
    {
      "Name": "f",
      "Definition": "CREATE FUNCTION f() RETURNS integer LANGUAGE sql AS $function$ SELECT 1; $function$"
    }
  ],
  "Sequences": [
    {
      "Name": "t_id_seq",
      "TypeName": "integer",
      "StartValue": 1,
      "MinimumValue": 1,
      "MaximumValue": 2147483647,
      "Increment": 1,
      "CycleOption": "NO"
    }
  ],
  "Tables": [
    {
      "Name": "t",
      "Comment": "",
      "Columns": [
        {
          "Name": "id",
          "Index": 1,
          "TypeName": "integer",
          "IsNullable": false,
          "Default": "nextval('t_id_seq'::regclass)",
          "CharacterMaximumLength": 0,
          "IsIdentity": false,
          "IdentityGeneration": "",
          "IsGenerated": "NEVER",
          "GenerationExpression": "",
          "Comment": ""
        },
        {
          "Name": "name",
          "Index": 2,
          "TypeName": "text",
          "IsNullable": true,
          "Default": "",
          "CharacterMaximumLength": 0,
          "IsIdentity": false,
          "IdentityGeneration": "",
          "IsGenerated": "NEVER",
          "GenerationExpression": "",
          "Comment": ""
        }
      ],
      "Indexes": [
        {
          "Name": "t_pkey",
          "IsPrimaryKey": true,
          "IsUnique": true,
          "IsExclusion": false,
          "IsDeferrable": false,
          "IndexDefinition": "CREATE UNIQUE INDEX t_pkey ON public.t USING btree (id)",
          "ConstraintType": "p",
          "ConstraintDefinition": "PRIMARY KEY (id)"
        }
      ],
      "Constraints": [
        {
          "Name": "t_name_check",
          "ConstraintType": "c",
          "RefTableName": "",
          "IsDeferrable": false,
          "ConstraintDefinition": "CHECK ((name \u003c\u003e ''::text))"
        }
      ],
      "Triggers": []
    }
  ],
  "Views": [
    {
      "Name": "v_t",
      "Definition": " SELECT t.id,\n    t.name\n   FROM public.t;"
    }
  ]
}"#;

        assert_eq!(format_json(&fixture()).unwrap(), expected);
    }

    #[test]
    fn canonicalize_sorts_every_collection() {
        let mut description = SchemaDescription {
            enums: vec![
                EnumDescription {
                    name: "b".to_string(),
                    labels: vec![],
                },
                EnumDescription {
                    name: "a".to_string(),
                    labels: vec![],
                },
            ],
            functions: vec![
                FunctionDescription {
                    name: "b".to_string(),
                    definition: String::new(),
                },
                FunctionDescription {
                    name: "a".to_string(),
                    definition: String::new(),
                },
            ],
            sequences: vec![
                SequenceDescription {
                    name: "b".to_string(),
                    ..SequenceDescription::default()
                },
                SequenceDescription {
                    name: "a".to_string(),
                    ..SequenceDescription::default()
                },
            ],
            tables: vec![
                TableDescription {
                    name: "b".to_string(),
                    ..TableDescription::default()
                },
                TableDescription {
                    name: "a".to_string(),
                    ..TableDescription::default()
                },
            ],
            views: vec![
                ViewDescription {
                    name: "b".to_string(),
                    definition: String::new(),
                },
                ViewDescription {
                    name: "a".to_string(),
                    definition: String::new(),
                },
            ],
            ..SchemaDescription::default()
        };

        canonicalize(&mut description);

        assert_eq!(description.enums[0].name, "a");
        assert_eq!(description.functions[0].name, "a");
        assert_eq!(description.sequences[0].name, "a");
        assert_eq!(description.tables[0].name, "a");
        assert_eq!(description.views[0].name, "a");
    }
}
