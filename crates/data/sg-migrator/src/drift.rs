#![forbid(unsafe_code)]

//! `internal/database/migration/{multiversion/drift.go,drift}` and
//! `schemas/drift_schema.go`: compare a live schema to the expected schema.
//!
//! # Expected-schema sources
//!
//! [`fetch_expected_schema`] mirrors `schemas.DefaultSchemaFactories`:
//!
//! 1. **Local file** (`-file`): parse exactly the supplied path.
//! 2. **Local/bundled**: `<root>/schema.json`, `<root>/schema.codeintel.json`
//!    or `<root>/schema.codeinsights.json` (the packaged image layout under
//!    `/migrations`), and the Go repository path under `root`
//!    (e.g. `internal/database/schema.json`) for development checkouts.
//! 3. **GitHub**: `raw.githubusercontent.com/sourcegraph/sourcegraph/<rev>/<path>`
//!    for version branches, tags and commit revisions.
//! 4. **GCS**: `storage.googleapis.com/sourcegraph-assets/migrations/drift/...`
//!    for tagged releases (the historical backfill bucket).
//!
//! Unlike Go, the local source is tried for every version (the image bundles
//! the description for exactly the version it ships). `reqwest::Client::new`
//! honours the standard proxy environment variables.
//!
//! # Deliberate deviations from Go
//!
//! * `go-cmp`'s `cmp.Diff` is replaced by a deterministic line diff over the
//!   pretty-printed JSON of the two values; the surrounding summary wording is
//!   unchanged.
//! * `output.Output`'s emoji/colour formatting is replaced with plain text;
//!   code blocks keep their Markdown fences, matching `WriteCode` with colour
//!   disabled.
//! * The `-version`/`-file` conflict and empty-version checks live here (Go
//!   performs them in `cliutil`), because this port combines the fetch entry
//!   point with those preconditions.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use serde::Serialize;
use serde_json::Value;

use crate::describe::{
    ColumnDescription, ConstraintDescription, EnumDescription, FunctionDescription,
    IndexDescription, SchemaDescription, SequenceDescription, TableDescription, TriggerDescription,
    ViewDescription,
};
use crate::error::MigratorError;

/// `multiversion.Canonicalize`: normalize a description before comparison
/// (sorted collections, migrator bookkeeping tables removed).
pub fn canonicalize(description: &mut SchemaDescription) {
    crate::describe::canonicalize(description);
    description
        .tables
        .retain(|table| table.name != "migration_logs");
}

/// `schemas.GetSchemaJSONFilename`.
fn schema_json_filename(schema: &str) -> Result<&'static str, MigratorError> {
    match schema {
        "frontend" => Ok("internal/database/schema.json"),
        "codeintel" => Ok("internal/database/schema.codeintel.json"),
        "codeinsights" => Ok("internal/database/schema.codeinsights.json"),
        _ => Err(MigratorError::new(format!(
            "unknown schema name {schema:?}"
        ))),
    }
}

/// `multiversion.FetchExpectedSchema`: load the expected description for
/// `version` or from an explicit `file`.
pub async fn fetch_expected_schema(
    schema: &str,
    version: &str,
    file: Option<&str>,
    root: &Path,
) -> Result<SchemaDescription, MigratorError> {
    let go_path = schema_json_filename(schema)?;

    if let Some(file) = file {
        if !version.is_empty() {
            return Err(MigratorError::new(
                "the flags -version or -file are mutually exclusive",
            ));
        }
        return read_schema_from_file(Path::new(file));
    }

    if version.is_empty() {
        return Err(MigratorError::new("must supply a -version or -file"));
    }

    let version = &normalize_version(version);

    // Local/bundled description first: the packaged image layout, then the Go
    // repository path for development checkouts.
    let mut local_candidates: Vec<std::path::PathBuf> = Vec::new();
    if let Some(file_name) = Path::new(go_path).file_name() {
        local_candidates.push(root.join(file_name));
    }
    local_candidates.push(root.join(go_path));
    local_candidates.dedup();

    for candidate in local_candidates {
        if candidate.is_file() {
            if let Ok(description) = read_schema_from_file(&candidate) {
                return Ok(description);
            }
        }
    }

    if is_github_version(version) {
        let url = github_expected_schema_path(go_path, version);
        if let Ok(description) = fetch_schema(&url).await {
            return Ok(description);
        }
    }

    if is_tag(version) {
        let url = gcs_expected_schema_path(go_path, version);
        if let Ok(description) = fetch_schema(&url).await {
            return Ok(description);
        }
    }

    Err(MigratorError::new(
        "failed to locate target schema description",
    ))
}

/// `schemas.GithubExpectedSchemaPath`.
fn github_expected_schema_path(filename: &str, version: &str) -> String {
    format!("https://raw.githubusercontent.com/sourcegraph/sourcegraph/{version}/{filename}")
}

/// `schemas.GcsExpectedSchemaPath`.
fn gcs_expected_schema_path(filename: &str, version: &str) -> String {
    format!(
        "https://storage.googleapis.com/sourcegraph-assets/migrations/drift/{version}-{}",
        filename.replace('/', "_")
    )
}

/// `schemas.fetchSchema`.
async fn fetch_schema(url: &str) -> Result<SchemaDescription, MigratorError> {
    let response = reqwest::Client::new()
        .get(url)
        .send()
        .await
        .map_err(|error| MigratorError::new(error.to_string()))?;

    if response.status() != reqwest::StatusCode::OK {
        return Err(MigratorError::new(format!(
            "HTTP {}: {url}",
            response.status().as_u16()
        )));
    }

    let body = response
        .bytes()
        .await
        .map_err(|error| MigratorError::new(error.to_string()))?;

    parse_schema_description(&body).map_err(MigratorError::new)
}

/// `schemas.ReadSchemaFromFile`.
fn read_schema_from_file(path: &Path) -> Result<SchemaDescription, MigratorError> {
    let bytes = std::fs::read(path).map_err(|error| {
        MigratorError::new(format!(
            "open {}: {}",
            path.display(),
            go_io_error_message(&error)
        ))
    })?;

    parse_schema_description(&bytes).map_err(MigratorError::new)
}

/// Render the common `os` errors the way Go does (`open x: no such file or
/// directory`) rather than with Rust's `(os error N)` suffix.
fn go_io_error_message(error: &std::io::Error) -> String {
    match error.kind() {
        std::io::ErrorKind::NotFound => "no such file or directory".to_string(),
        std::io::ErrorKind::PermissionDenied => "permission denied".to_string(),
        _ => error.to_string(),
    }
}

/// Decode a Go-serialised schema description.
///
/// Go unmarshals a JSON `null` into a nil slice; serde's `Vec` does not accept
/// `null`. The checked-in descriptions use `null` for empty collections
/// (`Enums`, `Functions`, `Views`, `Constraints`), so those are normalised to
/// empty arrays first.
fn parse_schema_description(bytes: &[u8]) -> Result<SchemaDescription, String> {
    let mut value: Value = serde_json::from_slice(bytes).map_err(|error| error.to_string())?;
    normalize_null_arrays(&mut value);
    serde_json::from_value(value).map_err(|error| error.to_string())
}

fn normalize_null_arrays(value: &mut Value) {
    match value {
        Value::Object(map) => {
            for (key, child) in map.iter_mut() {
                if child.is_null() && is_array_field(key) {
                    *child = Value::Array(Vec::new());
                } else {
                    normalize_null_arrays(child);
                }
            }
        }
        Value::Array(items) => {
            for item in items {
                normalize_null_arrays(item);
            }
        }
        _ => {}
    }
}

fn is_array_field(key: &str) -> bool {
    matches!(
        key,
        "Extensions"
            | "Enums"
            | "Functions"
            | "Sequences"
            | "Tables"
            | "Views"
            | "Columns"
            | "Indexes"
            | "Constraints"
            | "Triggers"
            | "Labels"
    )
}

// ---------------------------------------------------------------------------
// version handling (cliutil/drift.go, oobmigration/version.go)
// ---------------------------------------------------------------------------

/// Apply `cliutil`'s version normalisation: a parseable version is rewritten
/// as `v<major>.<minor>.<patch>`; anything else (branches, `HEAD`, revhashes)
/// is left untouched.
fn normalize_version(version: &str) -> String {
    match parse_version(version) {
        Some((major, minor, patch)) => {
            let normalized = format!("v{major}.{minor}.{patch}");
            if normalized == version {
                version.to_string()
            } else {
                normalized
            }
        }
        None => version.to_string(),
    }
}

/// Matches `^v?(\d+)\.(\d+)(?:\.(\d+))?(?:-\w*)?(?:\+[\w.]*)?$` and returns
/// `(major, minor, patch)`.
fn parse_version(version: &str) -> Option<(u64, u64, u64)> {
    let rest = version.strip_prefix('v').unwrap_or(version);
    let rest = match rest.find('+') {
        Some(index) => {
            if !rest[index + 1..]
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '.')
            {
                return None;
            }
            &rest[..index]
        }
        None => rest,
    };
    let rest = match rest.find('-') {
        Some(index) => {
            if !rest[index + 1..]
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '_')
            {
                return None;
            }
            &rest[..index]
        }
        None => rest,
    };

    let mut parts = rest.split('.');
    let major = parse_number(parts.next()?)?;
    let minor = parse_number(parts.next()?)?;
    let patch = match parts.next() {
        Some(value) => parse_number(value)?,
        None => 0,
    };
    if parts.next().is_some() {
        return None;
    }
    Some((major, minor, patch))
}

fn parse_number(value: &str) -> Option<u64> {
    if value.is_empty() || !value.chars().all(|c| c.is_ascii_digit()) {
        return None;
    }
    // Go's strconv.Atoi turns overflow into 0.
    Some(value.parse().unwrap_or(0))
}

/// `schemas.GitHubExpectedSchemaFactory`'s version patterns.
fn is_github_version(version: &str) -> bool {
    is_version_branch(version) || is_tag(version) || is_commit(version)
}

/// `^\d+\.\d+$`
fn is_version_branch(version: &str) -> bool {
    match version.split_once('.') {
        Some((major, minor)) => {
            !major.is_empty()
                && !minor.is_empty()
                && major.chars().all(|c| c.is_ascii_digit())
                && minor.chars().all(|c| c.is_ascii_digit())
        }
        None => false,
    }
}

/// `^v\d+\.\d+\.\d+$`
fn is_tag(version: &str) -> bool {
    let Some(rest) = version.strip_prefix('v') else {
        return false;
    };
    let parts: Vec<&str> = rest.split('.').collect();
    parts.len() == 3
        && parts
            .iter()
            .all(|part| !part.is_empty() && part.chars().all(|c| c.is_ascii_digit()))
}

/// `^[0-9A-Fa-f]{12}$` (abbreviated) and `^[0-9A-Fa-f]{40}$` (full).
fn is_commit(version: &str) -> bool {
    (version.len() == 12 || version.len() == 40) && version.chars().all(|c| c.is_ascii_hexdigit())
}

// ---------------------------------------------------------------------------
// drift.CompareSchemaDescriptions
// ---------------------------------------------------------------------------

/// A drift finding (`drift.Summary`).
#[derive(Debug, Clone, PartialEq)]
pub struct Summary {
    pub name: String,
    pub problem: String,
    pub solution: String,
    pub diff: Option<(Value, Value)>,
    pub statements: Option<Vec<String>>,
    pub url_hint: Option<String>,
}

impl Summary {
    fn new(
        name: impl Into<String>,
        problem: impl Into<String>,
        solution: impl Into<String>,
    ) -> Self {
        Self {
            name: name.into(),
            problem: problem.into(),
            solution: solution.into(),
            diff: None,
            statements: None,
            url_hint: None,
        }
    }

    fn with_diff(mut self, expected: Value, actual: Value) -> Self {
        self.diff = Some((expected, actual));
        self
    }

    fn with_statements(mut self, statements: Vec<String>) -> Self {
        self.statements = Some(statements);
        self
    }

    fn with_url_hint(mut self, url: impl Into<String>) -> Self {
        self.url_hint = Some(url.into());
        self
    }
}

/// `drift.CompareSchemaDescriptions`.
pub fn compare_schema_descriptions(
    schema_name: &str,
    version: &str,
    actual: &SchemaDescription,
    expected: &SchemaDescription,
) -> Vec<Summary> {
    let mut summaries = Vec::new();
    summaries.extend(compare_extensions(actual, expected));
    summaries.extend(compare_enums(actual, expected));
    summaries.extend(compare_functions(actual, expected));
    summaries.extend(compare_sequences(schema_name, version, actual, expected));
    summaries.extend(compare_tables(schema_name, version, actual, expected));
    summaries.extend(compare_views(actual, expected));
    summaries
}

/// The shared `compareNamedLists*` machinery. `actual` and `expected` are
/// grouped by name (last duplicate wins), keys are visited in sorted order,
/// values are normalised before comparison, and names that exist only in
/// `actual` are handed to `additional`.
#[allow(clippy::too_many_arguments)]
fn compare_named_lists<T, N, E, Q, F, A>(
    actual: &[T],
    expected: &[T],
    name: N,
    normalize: E,
    equivalent: Q,
    primary: F,
    additional: A,
) -> Vec<Summary>
where
    T: Clone,
    N: Fn(&T) -> String,
    E: Fn(&T) -> T,
    Q: Fn(&T, &T) -> bool,
    F: Fn(Option<&T>, &T) -> Vec<Summary>,
    A: Fn(&[T]) -> Vec<Summary>,
{
    let group = |items: &[T]| -> BTreeMap<String, T> {
        items
            .iter()
            .map(|item| (name(item), item.clone()))
            .collect()
    };
    let actual_by_name = group(actual);
    let expected_by_name = group(expected);

    let mut additional_items: Vec<T> = Vec::new();
    let mut summaries: Vec<Summary> = Vec::new();

    for (key, actual_item) in &actual_by_name {
        let actual = normalize(actual_item);
        match expected_by_name.get(key) {
            Some(expected_item) => {
                let expected = normalize(expected_item);
                if !equivalent(&actual, &expected) {
                    summaries.extend(primary(Some(&actual), &expected));
                }
            }
            None => additional_items.push(actual),
        }
    }

    for (key, expected_item) in &expected_by_name {
        if !actual_by_name.contains_key(key) {
            let expected = normalize(expected_item);
            summaries.extend(primary(None, &expected));
        }
    }

    if !additional_items.is_empty() {
        summaries.extend(additional(&additional_items));
    }

    summaries
}

fn identity<T: Clone>(value: &T) -> T {
    value.clone()
}

fn single(summary: Option<Summary>) -> Vec<Summary> {
    summary.into_iter().collect()
}

fn json_value<T: Serialize>(value: &T) -> Value {
    serde_json::to_value(value).unwrap_or(Value::Null)
}

fn compare_extensions(actual: &SchemaDescription, expected: &SchemaDescription) -> Vec<Summary> {
    let actual = actual.wrapped_extensions();
    let expected = expected.wrapped_extensions();

    compare_named_lists(
        &actual,
        &expected,
        |extension| extension.name.clone(),
        identity,
        |a, b| a == b,
        |actual, expected| {
            if actual.is_some() {
                return Vec::new();
            }
            vec![Summary::new(
                expected.name.clone(),
                format!("Missing extension {}", go_quote(&expected.name)),
                "define the extension",
            )
            .with_statements(vec![expected.create_statement()])]
        },
        |_| Vec::new(),
    )
}

fn compare_enums(actual: &SchemaDescription, expected: &SchemaDescription) -> Vec<Summary> {
    compare_named_lists(
        &actual.enums,
        &expected.enums,
        |enumeration| enumeration.name.clone(),
        identity,
        |a, b| a == b,
        |actual, expected| single(compare_enum_callback(actual, expected)),
        |_| Vec::new(),
    )
}

fn compare_enum_callback(
    actual: Option<&EnumDescription>,
    expected: &EnumDescription,
) -> Option<Summary> {
    let Some(actual) = actual else {
        return Some(
            Summary::new(
                expected.name.clone(),
                format!("Missing enum {}", go_quote(&expected.name)),
                "define the type",
            )
            .with_statements(vec![enum_create_statement(expected)]),
        );
    };

    if let Some(statements) = enum_alter_to_target(actual, expected) {
        return Some(
            Summary::new(
                expected.name.clone(),
                format!("Unexpected properties of enum {}", go_quote(&expected.name)),
                "alter the type",
            )
            .with_statements(statements),
        );
    }

    Some(
        Summary::new(
            expected.name.clone(),
            format!("Unexpected properties of enum {}", go_quote(&expected.name)),
            "redefine the type",
        )
        .with_diff(json_value(&expected.labels), json_value(&actual.labels))
        .with_statements(vec![
            enum_drop_statement(expected),
            enum_create_statement(expected),
        ]),
    )
}

fn compare_functions(actual: &SchemaDescription, expected: &SchemaDescription) -> Vec<Summary> {
    compare_named_lists(
        &actual.functions,
        &expected.functions,
        |function| function.name.clone(),
        identity,
        |a, b| normalize_function(&a.definition) == normalize_function(&b.definition),
        |actual, expected| single(compare_function_callback(actual, expected)),
        |_| Vec::new(),
    )
}

fn compare_function_callback(
    actual: Option<&FunctionDescription>,
    expected: &FunctionDescription,
) -> Option<Summary> {
    match actual {
        None => Some(
            Summary::new(
                expected.name.clone(),
                format!("Missing function {}", go_quote(&expected.name)),
                "define the function",
            )
            .with_statements(vec![function_create_or_replace_statement(expected)]),
        ),
        Some(actual) => Some(
            Summary::new(
                expected.name.clone(),
                format!(
                    "Unexpected definition of function {}",
                    go_quote(&expected.name)
                ),
                "redefine the function",
            )
            .with_diff(
                json_value(&expected.definition),
                json_value(&actual.definition),
            )
            .with_statements(vec![function_create_or_replace_statement(expected)]),
        ),
    }
}

fn compare_sequences(
    schema_name: &str,
    version: &str,
    actual: &SchemaDescription,
    expected: &SchemaDescription,
) -> Vec<Summary> {
    compare_named_lists(
        &actual.sequences,
        &expected.sequences,
        |sequence| sequence.name.clone(),
        identity,
        |a, b| a == b,
        |actual, expected| {
            single(compare_sequence_callback(
                schema_name,
                version,
                actual,
                expected,
            ))
        },
        |_| Vec::new(),
    )
}

fn compare_sequence_callback(
    schema_name: &str,
    version: &str,
    actual: Option<&SequenceDescription>,
    expected: &SequenceDescription,
) -> Option<Summary> {
    let Some(actual) = actual else {
        return Some(
            Summary::new(
                expected.name.clone(),
                format!("Missing sequence {}", go_quote(&expected.name)),
                "define the sequence",
            )
            .with_statements(vec![sequence_create_statement(expected)]),
        );
    };

    let (statements, alterable) = sequence_alter_to_target(actual, expected);
    if alterable {
        return Some(
            Summary::new(
                expected.name.clone(),
                format!(
                    "Unexpected properties of sequence {}",
                    go_quote(&expected.name)
                ),
                "alter the sequence",
            )
            .with_statements(statements),
        );
    }

    Some(
        Summary::new(
            expected.name.clone(),
            format!(
                "Unexpected properties of sequence {}",
                go_quote(&expected.name)
            ),
            "redefine the sequence",
        )
        .with_diff(json_value(expected), json_value(actual))
        .with_url_hint(make_search_url(
            schema_name,
            version,
            &[
                format!("CREATE SEQUENCE {}", expected.name),
                format!("nextval('{}'::regclass);", expected.name),
            ],
        )),
    )
}

fn compare_tables(
    schema_name: &str,
    version: &str,
    actual: &SchemaDescription,
    expected: &SchemaDescription,
) -> Vec<Summary> {
    compare_named_lists(
        &actual.tables,
        &expected.tables,
        |table| table.name.clone(),
        |table: &TableDescription| {
            let mut table = table.clone();
            table.comment.clear();
            table
        },
        |a, b| a == b,
        |actual, expected| compare_table_callback(schema_name, version, actual, expected),
        |_| Vec::new(),
    )
}

fn compare_table_callback(
    schema_name: &str,
    version: &str,
    actual: Option<&TableDescription>,
    expected: &TableDescription,
) -> Vec<Summary> {
    let Some(actual) = actual else {
        return vec![Summary::new(
            expected.name.clone(),
            format!("Missing table {}", go_quote(&expected.name)),
            "define the table",
        )
        .with_url_hint(make_search_url(
            schema_name,
            version,
            &[
                format!("CREATE TABLE {}", expected.name),
                format!("ALTER TABLE ONLY {}", expected.name),
                format!("CREATE .*(INDEX|TRIGGER).* ON {}", expected.name),
            ],
        ))];
    };

    let mut summaries = Vec::new();
    summaries.extend(compare_columns(schema_name, version, actual, expected));
    summaries.extend(compare_constraints(actual, expected));
    summaries.extend(compare_indexes(actual, expected));
    summaries.extend(compare_triggers(actual, expected));
    summaries
}

fn compare_columns(
    schema_name: &str,
    version: &str,
    actual_table: &TableDescription,
    expected_table: &TableDescription,
) -> Vec<Summary> {
    compare_named_lists(
        &actual_table.columns,
        &expected_table.columns,
        |column| column.name.clone(),
        |column: &ColumnDescription| {
            let mut column = column.clone();
            column.index = -1;
            column.comment.clear();
            column
        },
        |a, b| a == b,
        |actual, expected| {
            single(compare_column_callback(
                schema_name,
                version,
                expected_table,
                actual,
                expected,
            ))
        },
        |additional| {
            additional
                .iter()
                .map(|column| {
                    Summary::new(
                        format!(
                            "{}.{}",
                            go_quote(&expected_table.name),
                            go_quote(&column.name)
                        ),
                        format!(
                            "Unexpected column {}.{}",
                            go_quote(&expected_table.name),
                            go_quote(&column.name)
                        ),
                        "drop the column",
                    )
                    .with_statements(vec![column_drop_statement(expected_table, column)])
                })
                .collect()
        },
    )
}

fn compare_column_callback(
    schema_name: &str,
    version: &str,
    table: &TableDescription,
    actual: Option<&ColumnDescription>,
    expected: &ColumnDescription,
) -> Option<Summary> {
    let Some(actual) = actual else {
        return Some(
            Summary::new(
                format!("{}.{}", go_quote(&table.name), go_quote(&expected.name)),
                format!(
                    "Missing column {}.{}",
                    go_quote(&table.name),
                    go_quote(&expected.name)
                ),
                "define the column",
            )
            .with_statements(vec![column_create_statement(table, expected)])
            .with_url_hint(make_search_url(
                schema_name,
                version,
                &[
                    format!("CREATE TABLE {}", table.name),
                    format!("ALTER TABLE ONLY {}", table.name),
                ],
            )),
        );
    };

    let (statements, alterable) = column_alter_to_target(table, actual, expected);
    if alterable {
        return Some(
            Summary::new(
                expected.name.clone(),
                format!(
                    "Unexpected properties of column {}.{}",
                    table.name,
                    go_quote(&expected.name)
                ),
                "alter the column",
            )
            .with_statements(statements),
        );
    }

    Some(
        Summary::new(
            format!("{}.{}", go_quote(&table.name), go_quote(&expected.name)),
            format!(
                "Unexpected properties of column {}.{}",
                go_quote(&table.name),
                go_quote(&expected.name)
            ),
            "redefine the column",
        )
        .with_diff(json_value(expected), json_value(actual))
        .with_url_hint(make_search_url(
            schema_name,
            version,
            &[
                format!("CREATE TABLE {}", table.name),
                format!("ALTER TABLE ONLY {}", table.name),
            ],
        )),
    )
}

fn compare_indexes(
    actual_table: &TableDescription,
    expected_table: &TableDescription,
) -> Vec<Summary> {
    compare_named_lists(
        &actual_table.indexes,
        &expected_table.indexes,
        |index| index.name.clone(),
        identity,
        |a, b| a == b,
        |actual, expected| single(compare_index_callback(expected_table, actual, expected)),
        |additional| {
            additional
                .iter()
                .map(|index| {
                    Summary::new(
                        format!(
                            "{}.{}",
                            go_quote(&expected_table.name),
                            go_quote(&index.name)
                        ),
                        format!(
                            "Unexpected index {}.{}",
                            go_quote(&expected_table.name),
                            go_quote(&index.name)
                        ),
                        "drop the index",
                    )
                    .with_statements(vec![index_drop_statement(expected_table, index)])
                })
                .collect()
        },
    )
}

fn compare_index_callback(
    table: &TableDescription,
    actual: Option<&IndexDescription>,
    expected: &IndexDescription,
) -> Option<Summary> {
    let name = format!("{}.{}", go_quote(&table.name), go_quote(&expected.name));

    let Some(actual) = actual else {
        return Some(
            Summary::new(
                name,
                format!(
                    "Missing index {}.{}",
                    go_quote(&table.name),
                    go_quote(&expected.name)
                ),
                "define the index",
            )
            .with_statements(vec![index_create_statement(table, expected)]),
        );
    };

    Some(
        Summary::new(
            name,
            format!(
                "Unexpected properties of index {}.{}",
                go_quote(&table.name),
                go_quote(&expected.name)
            ),
            "redefine the index",
        )
        .with_diff(json_value(expected), json_value(actual))
        .with_statements(vec![
            index_drop_statement(table, expected),
            index_create_statement(table, expected),
        ]),
    )
}

fn compare_constraints(
    actual_table: &TableDescription,
    expected_table: &TableDescription,
) -> Vec<Summary> {
    compare_named_lists(
        &actual_table.constraints,
        &expected_table.constraints,
        |constraint| constraint.name.clone(),
        identity,
        |a, b| a == b,
        |actual, expected| {
            single(compare_constraint_callback(
                expected_table,
                actual,
                expected,
            ))
        },
        |additional| {
            additional
                .iter()
                .map(|constraint| {
                    Summary::new(
                        format!(
                            "{}.{}",
                            go_quote(&expected_table.name),
                            go_quote(&constraint.name)
                        ),
                        format!(
                            "Unexpected constraint {}.{}",
                            go_quote(&expected_table.name),
                            go_quote(&constraint.name)
                        ),
                        "drop the constraint",
                    )
                    .with_statements(vec![constraint_drop_statement(expected_table, constraint)])
                })
                .collect()
        },
    )
}

fn compare_constraint_callback(
    table: &TableDescription,
    actual: Option<&ConstraintDescription>,
    expected: &ConstraintDescription,
) -> Option<Summary> {
    let name = format!("{}.{}", go_quote(&table.name), go_quote(&expected.name));

    let Some(actual) = actual else {
        return Some(
            Summary::new(
                name,
                format!(
                    "Missing constraint {}.{}",
                    go_quote(&table.name),
                    go_quote(&expected.name)
                ),
                "define the constraint",
            )
            .with_statements(vec![constraint_create_statement(table, expected)]),
        );
    };

    Some(
        Summary::new(
            name,
            format!(
                "Unexpected properties of constraint {}.{}",
                go_quote(&table.name),
                go_quote(&expected.name)
            ),
            "redefine the constraint",
        )
        .with_diff(json_value(expected), json_value(actual))
        .with_statements(vec![
            constraint_drop_statement(table, expected),
            constraint_create_statement(table, expected),
        ]),
    )
}

fn compare_triggers(
    actual_table: &TableDescription,
    expected_table: &TableDescription,
) -> Vec<Summary> {
    compare_named_lists(
        &actual_table.triggers,
        &expected_table.triggers,
        |trigger| trigger.name.clone(),
        identity,
        |a, b| a == b,
        |actual, expected| single(compare_trigger_callback(expected_table, actual, expected)),
        |additional| {
            additional
                .iter()
                .map(|trigger| {
                    Summary::new(
                        format!(
                            "{}.{}",
                            go_quote(&expected_table.name),
                            go_quote(&trigger.name)
                        ),
                        format!(
                            "Unexpected trigger {}.{}",
                            go_quote(&expected_table.name),
                            go_quote(&trigger.name)
                        ),
                        "drop the trigger",
                    )
                    .with_statements(vec![trigger_drop_statement(expected_table, trigger)])
                })
                .collect()
        },
    )
}

fn compare_trigger_callback(
    table: &TableDescription,
    actual: Option<&TriggerDescription>,
    expected: &TriggerDescription,
) -> Option<Summary> {
    let name = format!("{}.{}", go_quote(&table.name), go_quote(&expected.name));

    let Some(actual) = actual else {
        return Some(
            Summary::new(
                name,
                format!(
                    "Missing trigger {}.{}",
                    go_quote(&table.name),
                    go_quote(&expected.name)
                ),
                "define the trigger",
            )
            .with_statements(vec![trigger_create_statement(expected)]),
        );
    };

    Some(
        Summary::new(
            name,
            format!(
                "Unexpected properties of trigger {}.{}",
                go_quote(&table.name),
                go_quote(&expected.name)
            ),
            "redefine the trigger",
        )
        .with_diff(json_value(expected), json_value(actual))
        .with_statements(vec![
            trigger_drop_statement(table, expected),
            trigger_create_statement(expected),
        ]),
    )
}

fn compare_views(actual: &SchemaDescription, expected: &SchemaDescription) -> Vec<Summary> {
    compare_named_lists(
        &actual.views,
        &expected.views,
        |view| view.name.clone(),
        identity,
        |a, b| a == b,
        |actual, expected| single(compare_view_callback(actual, expected)),
        |_| Vec::new(),
    )
}

fn compare_view_callback(
    actual: Option<&ViewDescription>,
    expected: &ViewDescription,
) -> Option<Summary> {
    match actual {
        None => Some(
            Summary::new(
                expected.name.clone(),
                format!("Missing view {}", go_quote(&expected.name)),
                "define the view",
            )
            .with_statements(vec![view_create_statement(expected)]),
        ),
        Some(actual) => Some(
            Summary::new(
                expected.name.clone(),
                format!("Unexpected definition of view {}", go_quote(&expected.name)),
                "redefine the view",
            )
            .with_diff(
                json_value(&expected.definition),
                json_value(&actual.definition),
            )
            .with_statements(vec![
                view_drop_statement(expected),
                view_create_statement(expected),
            ]),
        ),
    }
}

// ---------------------------------------------------------------------------
// schemas helper statements
// ---------------------------------------------------------------------------

fn enum_create_statement(enumeration: &EnumDescription) -> String {
    let labels: Vec<String> = enumeration
        .labels
        .iter()
        .map(|label| format!("'{label}'"))
        .collect();
    format!(
        "CREATE TYPE {} AS ENUM ({});",
        enumeration.name,
        labels.join(", ")
    )
}

fn enum_drop_statement(enumeration: &EnumDescription) -> String {
    format!("DROP TYPE IF EXISTS {};", enumeration.name)
}

fn function_create_or_replace_statement(function: &FunctionDescription) -> String {
    format!("{};", function.definition)
}

fn sequence_create_statement(sequence: &SequenceDescription) -> String {
    let min_value = if sequence.minimum_value != 0 {
        format!("MINVALUE {}", sequence.minimum_value)
    } else {
        "NO MINVALUE".to_string()
    };
    let max_value = if sequence.maximum_value != 0 {
        format!("MAXVALUE {}", sequence.maximum_value)
    } else {
        "NO MAXVALUE".to_string()
    };

    format!(
        "CREATE SEQUENCE {} AS {} INCREMENT BY {} {} {} START WITH {} {} CYCLE;",
        sequence.name,
        sequence.type_name,
        sequence.increment,
        min_value,
        max_value,
        sequence.start_value,
        sequence.cycle_option,
    )
}

fn column_create_statement(table: &TableDescription, column: &ColumnDescription) -> String {
    let nullable_expr = if column.is_nullable { "" } else { " NOT NULL" };
    let default_expr = if column.default.is_empty() {
        String::new()
    } else {
        format!(" DEFAULT {}", column.default)
    };

    format!(
        "ALTER TABLE {} ADD COLUMN {} {}{}{};",
        table.name, column.name, column.type_name, nullable_expr, default_expr,
    )
}

fn column_drop_statement(table: &TableDescription, column: &ColumnDescription) -> String {
    format!(
        "ALTER TABLE {} DROP COLUMN IF EXISTS {};",
        table.name, column.name
    )
}

fn index_create_statement(table: &TableDescription, index: &IndexDescription) -> String {
    if index.constraint_type == "u" || index.constraint_type == "p" {
        format!(
            "ALTER TABLE {} ADD CONSTRAINT {} {};",
            table.name, index.name, index.constraint_definition
        )
    } else {
        format!("{};", index.index_definition)
    }
}

fn index_drop_statement(table: &TableDescription, index: &IndexDescription) -> String {
    if index.constraint_type == "u" || index.constraint_type == "p" {
        format!("ALTER TABLE {} DROP CONSTRAINT {};", table.name, index.name)
    } else {
        format!("DROP INDEX IF EXISTS {};", index.name)
    }
}

fn constraint_create_statement(
    table: &TableDescription,
    constraint: &ConstraintDescription,
) -> String {
    format!(
        "ALTER TABLE {} ADD CONSTRAINT {} {};",
        table.name, constraint.name, constraint.constraint_definition
    )
}

fn constraint_drop_statement(
    table: &TableDescription,
    constraint: &ConstraintDescription,
) -> String {
    format!(
        "ALTER TABLE {} DROP CONSTRAINT {};",
        table.name, constraint.name
    )
}

fn trigger_create_statement(trigger: &TriggerDescription) -> String {
    format!("{};", trigger.definition)
}

fn trigger_drop_statement(table: &TableDescription, trigger: &TriggerDescription) -> String {
    format!("DROP TRIGGER IF EXISTS {} ON {};", trigger.name, table.name)
}

fn view_create_statement(view: &ViewDescription) -> String {
    format!(
        "CREATE VIEW {} AS {}",
        view.name,
        strip_indent(&format!(" {}", view.definition)).trim()
    )
}

fn view_drop_statement(view: &ViewDescription) -> String {
    format!("DROP VIEW IF EXISTS {};", view.name)
}

/// `schemas.stripIndent`.
fn strip_indent(text: &str) -> String {
    let trimmed = text.trim_end_matches('\n');
    let mut lines: Vec<&str> = trimmed.split('\n').collect();
    let mut min_indent = leading_spaces(lines[0]);
    for line in &lines {
        min_indent = min_indent.min(leading_spaces(line));
    }
    lines = lines
        .into_iter()
        .map(|line| &line[min_indent.min(line.len())..])
        .collect();
    lines.join("\n")
}

fn leading_spaces(line: &str) -> usize {
    line.bytes().take_while(|byte| *byte == b' ').count()
}

// ---------------------------------------------------------------------------
// alteration checks
// ---------------------------------------------------------------------------

/// `EnumDescription.AlterToTarget`: `None` means values must be removed (the
/// type has to be recreated); `Some` carries the additive `ADD VALUE`
/// statements.
fn enum_alter_to_target(actual: &EnumDescription, target: &EnumDescription) -> Option<Vec<String>> {
    let labels: BTreeSet<&String> = actual.labels.iter().collect();
    let expected_labels: BTreeSet<&String> = target.labels.iter().collect();

    if labels.iter().any(|label| !expected_labels.contains(label)) {
        return None;
    }

    struct MissingLabel {
        label: String,
        neighbor: String,
        before: bool,
    }

    let mut missing = Vec::new();

    let mut after = String::new();
    for label in &target.labels {
        if !labels.contains(label) && !after.is_empty() {
            missing.push(MissingLabel {
                label: label.clone(),
                neighbor: after.clone(),
                before: false,
            });
        }
        after = label.clone();
    }

    let mut before = String::new();
    for label in target.labels.iter().rev() {
        if !labels.contains(label) && !before.is_empty() {
            missing.push(MissingLabel {
                label: label.clone(),
                neighbor: before.clone(),
                before: true,
            });
        }
        before = label.clone();
    }

    let mut ordered = Vec::new();
    let mut reachable: BTreeSet<String> = actual.labels.iter().cloned().collect();

    'outer: while !missing.is_empty() {
        for label in &missing {
            if !reachable.contains(&label.neighbor) {
                continue;
            }

            let relation = if label.before { "BEFORE" } else { "AFTER" };
            let label_name = label.label.clone();
            let neighbor = label.neighbor.clone();
            missing.retain(|candidate| candidate.label != label_name);
            reachable.insert(label_name.clone());
            ordered.push(format!(
                "ALTER TYPE {} ADD VALUE '{}' {} '{}';",
                target.name, label_name, relation, neighbor
            ));
            continue 'outer;
        }

        // Go panics here; an unreachable graph should not happen for
        // well-formed descriptions, so stop rather than loop forever.
        break;
    }

    Some(ordered)
}

/// `SequenceDescription.AlterToTarget`.
fn sequence_alter_to_target(
    actual: &SequenceDescription,
    target: &SequenceDescription,
) -> (Vec<String>, bool) {
    let mut statements = Vec::new();
    let mut adjusted = actual.clone();

    if adjusted.type_name != target.type_name {
        statements.push(format!(
            "ALTER SEQUENCE {} AS {} MAXVALUE {};",
            adjusted.name, target.type_name, target.maximum_value
        ));
        adjusted.type_name = target.type_name.clone();
        adjusted.maximum_value = target.maximum_value;
    }

    let alterable = adjusted == *target;
    (statements, alterable)
}

/// `ColumnDescription.AlterToTarget`.
fn column_alter_to_target(
    table: &TableDescription,
    actual: &ColumnDescription,
    target: &ColumnDescription,
) -> (Vec<String>, bool) {
    let mut statements = Vec::new();
    let mut adjusted = actual.clone();

    if adjusted.type_name != target.type_name {
        statements.push(format!(
            "ALTER TABLE {} ALTER COLUMN {} SET DATA TYPE {};",
            table.name, target.name, target.type_name
        ));
        adjusted.type_name = target.type_name.clone();
    }
    if adjusted.is_nullable != target.is_nullable {
        let verb = if target.is_nullable { "DROP" } else { "SET" };
        statements.push(format!(
            "ALTER TABLE {} ALTER COLUMN {} {} NOT NULL;",
            table.name, target.name, verb
        ));
        adjusted.is_nullable = target.is_nullable;
    }
    if adjusted.default != target.default {
        if target.default.is_empty() {
            statements.push(format!(
                "ALTER TABLE {} ALTER COLUMN {} DROP DEFAULT;",
                table.name, target.name
            ));
        } else {
            statements.push(format!(
                "ALTER TABLE {} ALTER COLUMN {} SET DEFAULT {};",
                table.name, target.name, target.default
            ));
        }
        adjusted.default = target.default.clone();
    }

    let alterable = adjusted == *target;
    (statements, alterable)
}

// ---------------------------------------------------------------------------
// search hints (drift/util_search.go)
// ---------------------------------------------------------------------------

/// `drift.makeSearchURL`.
fn make_search_url(schema_name: &str, version: &str, search_terms: &[String]) -> String {
    let terms: Vec<String> = search_terms.iter().map(|term| quote_term(term)).collect();

    let query_parts = [
        format!(r"repo:^github\.com/sourcegraph/sourcegraph$@{version}"),
        format!(r"file:^migrations/{schema_name}/squashed\.sql$"),
        terms.join(" OR "),
    ];

    let mut serializer = url::form_urlencoded::Serializer::new(String::new());
    // Match `url.Values.Encode`, which sorts parameters by key.
    serializer.append_pair("patternType", "regexp");
    serializer.append_pair("q", &query_parts.join(" "));
    format!("https://sourcegraph.com/search?{}", serializer.finish())
}

/// `drift.quoteTerm`.
fn quote_term(search_term: &str) -> String {
    let terms: Vec<String> = search_term.split(' ').map(regexp_quote_meta).collect();
    format!(r"(^|\b){}{}", terms.join(r"\s"), r"($|\b)")
}

/// `regexp.QuoteMeta`: escape regex metacharacters.
fn regexp_quote_meta(value: &str) -> String {
    let mut quoted = String::with_capacity(value.len());
    for character in value.chars() {
        if matches!(
            character,
            '\\' | '.' | '+' | '*' | '?' | '(' | ')' | '|' | '[' | ']' | '{' | '}' | '^' | '$'
        ) {
            quoted.push('\\');
        }
        quoted.push(character);
    }
    quoted
}

// ---------------------------------------------------------------------------
// Normalization (schemas/description.go)
// ---------------------------------------------------------------------------

/// `schemas.normalizeFunction`: drop `--` comments, collapse whitespace.
fn normalize_function(definition: &str) -> String {
    let joined: Vec<&str> = definition
        .split('\n')
        .map(|line| line.split("--").next().unwrap_or(""))
        .collect();
    let joined = joined.join("\n");

    let mut collapsed = String::with_capacity(joined.len());
    let mut in_whitespace = false;
    for character in joined.chars() {
        if character.is_ascii_whitespace() {
            if !in_whitespace {
                collapsed.push(' ');
                in_whitespace = true;
            }
        } else {
            collapsed.push(character);
            in_whitespace = false;
        }
    }

    collapsed.trim().to_string()
}

// ---------------------------------------------------------------------------
// drift.DisplaySchemaSummaries
// ---------------------------------------------------------------------------

/// `drift.DisplaySchemaSummaries`: render summaries for humans as plain text.
pub fn display_schema_summaries(summaries: &[Summary]) -> String {
    if summaries.is_empty() {
        return "No drift detected\n".to_string();
    }

    let mut output = String::new();
    push_line(&mut output, "Drift detected!");
    push_line(&mut output, "");

    for summary in summaries {
        push_line(&mut output, &summary.problem);

        if let Some((expected, actual)) = &summary.diff {
            push_code(&mut output, "diff", &render_diff(expected, actual));
        }

        push_line(
            &mut output,
            &format!("Suggested action: {}.", summary.solution),
        );

        if let Some(statements) = &summary.statements {
            push_code(&mut output, "sql", &statements.join("\n"));
        }

        if let Some(url_hint) = &summary.url_hint {
            push_line(
                &mut output,
                &format!(
                    "Hint: Reproduce {} as defined at the following URL:",
                    summary.name
                ),
            );
            push_line(&mut output, "");
            push_line(&mut output, url_hint);
            push_line(&mut output, "");
        }
    }

    output
}

fn push_line(output: &mut String, line: &str) {
    output.push_str(line);
    output.push('\n');
}

fn push_code(output: &mut String, language: &str, body: &str) {
    output.push_str(&format!("```{language}\n{body}\n```\n"));
}

/// Deterministic line diff used in place of `go-cmp`'s `cmp.Diff`.
fn render_diff(expected: &Value, actual: &Value) -> String {
    let expected_lines = pretty_lines(expected);
    let actual_lines = pretty_lines(actual);

    // Longest common subsequence table.
    let mut lengths = vec![vec![0usize; actual_lines.len() + 1]; expected_lines.len() + 1];
    for i in (0..expected_lines.len()).rev() {
        for j in (0..actual_lines.len()).rev() {
            lengths[i][j] = if expected_lines[i] == actual_lines[j] {
                lengths[i + 1][j + 1] + 1
            } else {
                lengths[i + 1][j].max(lengths[i][j + 1])
            };
        }
    }

    let mut rendered = Vec::new();
    let (mut i, mut j) = (0, 0);
    while i < expected_lines.len() && j < actual_lines.len() {
        if expected_lines[i] == actual_lines[j] {
            rendered.push(format!("  {}", expected_lines[i]));
            i += 1;
            j += 1;
        } else if lengths[i + 1][j] >= lengths[i][j + 1] {
            rendered.push(format!("- {}", expected_lines[i]));
            i += 1;
        } else {
            rendered.push(format!("+ {}", actual_lines[j]));
            j += 1;
        }
    }
    while i < expected_lines.len() {
        rendered.push(format!("- {}", expected_lines[i]));
        i += 1;
    }
    while j < actual_lines.len() {
        rendered.push(format!("+ {}", actual_lines[j]));
        j += 1;
    }

    rendered.join("\n")
}

fn pretty_lines(value: &Value) -> Vec<String> {
    serde_json::to_string_pretty(value)
        .unwrap_or_default()
        .lines()
        .map(str::to_string)
        .collect()
}

/// Go's `%q` for the identifiers printed in summary messages.
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

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
        TempDir(
            std::env::temp_dir().join(format!("sg-migrator-drift-{}-{nanos}", std::process::id())),
        )
    }

    fn column(name: &str, index: i32, type_name: &str) -> ColumnDescription {
        ColumnDescription {
            name: name.to_string(),
            index,
            type_name: type_name.to_string(),
            ..ColumnDescription::default()
        }
    }

    fn table(name: &str) -> TableDescription {
        TableDescription {
            name: name.to_string(),
            ..TableDescription::default()
        }
    }

    fn index(name: &str) -> IndexDescription {
        IndexDescription {
            name: name.to_string(),
            ..IndexDescription::default()
        }
    }

    fn constraint(name: &str, constraint_type: &str) -> ConstraintDescription {
        ConstraintDescription {
            name: name.to_string(),
            constraint_type: constraint_type.to_string(),
            ..ConstraintDescription::default()
        }
    }

    fn enum_description(name: &str, labels: &[&str]) -> EnumDescription {
        EnumDescription {
            name: name.to_string(),
            labels: labels.iter().map(|label| label.to_string()).collect(),
        }
    }

    fn view(name: &str, definition: &str) -> ViewDescription {
        ViewDescription {
            name: name.to_string(),
            definition: definition.to_string(),
        }
    }

    fn description() -> SchemaDescription {
        SchemaDescription::default()
    }

    fn problems(summaries: &[Summary]) -> Vec<&str> {
        summaries
            .iter()
            .map(|summary| summary.problem.as_str())
            .collect()
    }

    #[test]
    fn canonicalize_sorts_and_drops_migrator_tables() {
        let mut description = SchemaDescription {
            enums: vec![enum_description("b", &["x"]), enum_description("a", &["x"])],
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
                    name: "zz".to_string(),
                    columns: vec![column("b", 2, "text"), column("a", 1, "integer")],
                    indexes: vec![index("t_b"), index("t_a")],
                    constraints: vec![constraint("z", "c"), constraint("a", "c")],
                    triggers: vec![
                        TriggerDescription {
                            name: "z".to_string(),
                            definition: String::new(),
                        },
                        TriggerDescription {
                            name: "a".to_string(),
                            definition: String::new(),
                        },
                    ],
                    ..TableDescription::default()
                },
                TableDescription {
                    name: "migration_logs".to_string(),
                    ..TableDescription::default()
                },
                TableDescription {
                    name: "aa".to_string(),
                    ..TableDescription::default()
                },
            ],
            views: vec![view("b", ""), view("a", "")],
            extensions: vec!["pgcrypto".to_string()],
        };

        canonicalize(&mut description);

        let names: Vec<&str> = description
            .tables
            .iter()
            .map(|table| table.name.as_str())
            .collect();
        assert_eq!(names, vec!["aa", "zz"]);
        assert_eq!(description.enums[0].name, "a");
        assert_eq!(description.functions[0].name, "a");
        assert_eq!(description.sequences[0].name, "a");
        assert_eq!(description.views[0].name, "a");

        let table = &description.tables[1];
        assert_eq!(table.columns[0].name, "a");
        assert_eq!(table.columns[1].name, "b");
        assert_eq!(table.indexes[0].name, "t_a");
        assert_eq!(table.constraints[0].name, "a");
        assert_eq!(table.triggers[0].name, "a");
        assert_eq!(description.extensions, vec!["pgcrypto"]);

        // Idempotent.
        let once = description.clone();
        canonicalize(&mut description);
        assert_eq!(description, once);
    }

    #[test]
    fn compare_reports_missing_table_and_not_extra_tables() {
        let mut expected = description();
        expected.tables = vec![table("new_table")];

        let summaries =
            compare_schema_descriptions("frontend", "v4.1.1", &description(), &expected);
        assert_eq!(problems(&summaries), vec![r#"Missing table "new_table""#]);
        assert!(summaries[0].url_hint.is_some());
        assert!(summaries[0].statements.is_none());

        // A table present only in the live schema is not reported.
        let summaries =
            compare_schema_descriptions("frontend", "v4.1.1", &expected, &description());
        assert!(summaries.is_empty());
    }

    #[test]
    fn compare_reports_missing_and_unexpected_columns() {
        let mut actual = description();
        actual.tables = vec![TableDescription {
            name: "t".to_string(),
            columns: vec![column("old", 1, "text")],
            ..TableDescription::default()
        }];
        let mut expected = description();
        expected.tables = vec![TableDescription {
            name: "t".to_string(),
            columns: vec![column("new", 1, "integer")],
            ..TableDescription::default()
        }];

        let summaries = compare_schema_descriptions("frontend", "v4.1.1", &actual, &expected);
        assert_eq!(
            problems(&summaries),
            vec![
                r#"Missing column "t"."new""#,
                r#"Unexpected column "t"."old""#,
            ]
        );
        assert_eq!(
            summaries[0].statements.as_deref(),
            Some(&["ALTER TABLE t ADD COLUMN new integer NOT NULL;".to_string()][..])
        );
        assert!(summaries[0].url_hint.is_some());
        assert_eq!(
            summaries[1].statements.as_deref(),
            Some(&["ALTER TABLE t DROP COLUMN IF EXISTS old;".to_string()][..])
        );
        assert_eq!(summaries[1].solution, "drop the column");
    }

    #[test]
    fn compare_ignores_column_index_and_comment_differences() {
        let mut actual = description();
        actual.tables = vec![TableDescription {
            name: "t".to_string(),
            columns: vec![ColumnDescription {
                index: 7,
                comment: "live".to_string(),
                ..column("c", 1, "integer")
            }],
            ..TableDescription::default()
        }];
        let expected = {
            let mut expected = description();
            expected.tables = vec![TableDescription {
                name: "t".to_string(),
                columns: vec![ColumnDescription {
                    index: 1,
                    comment: "expected".to_string(),
                    ..column("c", 2, "integer")
                }],
                ..TableDescription::default()
            }];
            expected
        };

        assert!(compare_schema_descriptions("frontend", "v4.1.1", &actual, &expected).is_empty());
    }

    #[test]
    fn compare_reports_index_and_constraint_differences() {
        let mut actual = description();
        actual.tables = vec![TableDescription {
            name: "t".to_string(),
            indexes: vec![index("stale")],
            constraints: vec![constraint("stale_check", "c")],
            ..TableDescription::default()
        }];
        let mut expected = description();
        expected.tables = vec![TableDescription {
            name: "t".to_string(),
            indexes: vec![index("fresh")],
            constraints: vec![constraint("fresh_check", "c")],
            ..TableDescription::default()
        }];

        let summaries = compare_schema_descriptions("frontend", "v4.1.1", &actual, &expected);
        assert_eq!(
            problems(&summaries),
            vec![
                r#"Missing constraint "t"."fresh_check""#,
                r#"Unexpected constraint "t"."stale_check""#,
                r#"Missing index "t"."fresh""#,
                r#"Unexpected index "t"."stale""#,
            ]
        );

        let missing_index = &summaries[2];
        assert_eq!(missing_index.solution, "define the index");
        assert_eq!(
            missing_index.statements.as_deref(),
            Some(&[";".to_string()][..])
        );
        let unexpected_index = &summaries[3];
        assert_eq!(
            unexpected_index.statements.as_deref(),
            Some(&["DROP INDEX IF EXISTS stale;".to_string()][..])
        );
    }

    #[test]
    fn compare_reports_trigger_and_view_changes() {
        let mut actual = description();
        actual.tables = vec![TableDescription {
            name: "t".to_string(),
            triggers: vec![TriggerDescription {
                name: "trg".to_string(),
                definition: "old".to_string(),
            }],
            ..TableDescription::default()
        }];
        actual.views = vec![view("v", " SELECT 1;")];
        let mut expected = description();
        expected.tables = vec![TableDescription {
            name: "t".to_string(),
            triggers: vec![TriggerDescription {
                name: "trg".to_string(),
                definition: "new".to_string(),
            }],
            ..TableDescription::default()
        }];
        expected.views = vec![view("v", " SELECT 2;")];

        let summaries = compare_schema_descriptions("frontend", "v4.1.1", &actual, &expected);
        assert_eq!(
            problems(&summaries),
            vec![
                r#"Unexpected properties of trigger "t"."trg""#,
                r#"Unexpected definition of view "v""#,
            ]
        );
        assert_eq!(
            summaries[0].statements.as_deref(),
            Some(
                &[
                    "DROP TRIGGER IF EXISTS trg ON t;".to_string(),
                    "new;".to_string()
                ][..]
            )
        );
        assert_eq!(
            summaries[1].statements.as_deref(),
            Some(
                &[
                    "DROP VIEW IF EXISTS v;".to_string(),
                    "CREATE VIEW v AS SELECT 2;".to_string()
                ][..]
            )
        );
    }

    #[test]
    fn compare_enums_alerts_on_additive_and_destructive_changes() {
        let mut actual = description();
        actual.enums = vec![enum_description("mood", &["sad"])];
        let mut expected = description();
        expected.enums = vec![enum_description("mood", &["sad", "happy"])];

        let summaries = compare_schema_descriptions("frontend", "v4.1.1", &actual, &expected);
        assert_eq!(
            problems(&summaries),
            vec![r#"Unexpected properties of enum "mood""#]
        );
        assert_eq!(summaries[0].solution, "alter the type");
        assert_eq!(
            summaries[0].statements.as_deref(),
            Some(&["ALTER TYPE mood ADD VALUE 'happy' AFTER 'sad';".to_string()][..])
        );

        // Removing a label forces a redefine.
        let summaries = compare_schema_descriptions("frontend", "v4.1.1", &expected, &actual);
        assert_eq!(
            problems(&summaries),
            vec![r#"Unexpected properties of enum "mood""#]
        );
        assert_eq!(summaries[0].solution, "redefine the type");
        assert!(summaries[0].diff.is_some());
        assert_eq!(
            summaries[0].statements.as_deref(),
            Some(
                &[
                    "DROP TYPE IF EXISTS mood;".to_string(),
                    "CREATE TYPE mood AS ENUM ('sad');".to_string()
                ][..]
            )
        );
    }

    #[test]
    fn compare_functions_ignores_comment_and_whitespace_only_changes() {
        let mut actual = description();
        actual.functions = vec![FunctionDescription {
            name: "f".to_string(),
            definition:
                "CREATE FUNCTION f() RETURNS integer LANGUAGE sql AS $function$\n  -- note\n  SELECT 1;\n$function$"
                    .to_string(),
        }];
        let mut expected = description();
        expected.functions = vec![FunctionDescription {
            name: "f".to_string(),
            definition:
                "CREATE FUNCTION f() RETURNS integer LANGUAGE sql AS $function$ SELECT 1; $function$"
                    .to_string(),
        }];

        assert!(compare_schema_descriptions("frontend", "v4.1.1", &actual, &expected).is_empty());

        expected.functions[0].definition =
            "CREATE FUNCTION f() RETURNS integer LANGUAGE sql AS $function$ SELECT 2; $function$"
                .to_string();
        let summaries = compare_schema_descriptions("frontend", "v4.1.1", &actual, &expected);
        assert_eq!(
            problems(&summaries),
            vec![r#"Unexpected definition of function "f""#]
        );
        assert_eq!(
            summaries[0].statements.as_deref(),
            Some(
                &[
                    "CREATE FUNCTION f() RETURNS integer LANGUAGE sql AS $function$ SELECT 2; $function$;"
                        .to_string()
                ][..]
            )
        );
    }

    #[test]
    fn canonicalize_makes_comparison_order_independent() {
        let mut actual = description();
        actual.tables = vec![TableDescription {
            name: "t".to_string(),
            columns: vec![column("b", 2, "text"), column("a", 1, "integer")],
            indexes: vec![index("t_b"), index("t_a")],
            ..TableDescription::default()
        }];
        let expected = {
            let mut expected = description();
            expected.tables = vec![TableDescription {
                name: "t".to_string(),
                columns: vec![column("a", 1, "integer"), column("b", 2, "text")],
                indexes: vec![index("t_a"), index("t_b")],
                ..TableDescription::default()
            }];
            expected
        };

        canonicalize(&mut actual);
        let mut expected = expected;
        canonicalize(&mut expected);
        assert!(compare_schema_descriptions("frontend", "v4.1.1", &actual, &expected).is_empty());
    }

    #[tokio::test]
    async fn fetch_expected_schema_loads_local_bundled_file() {
        let root = unique_temp_dir();
        fs::create_dir_all(&root.0).expect("create temp root");
        fs::write(
            root.0.join("schema.json"),
            r#"{
              "Extensions": ["pgcrypto"],
              "Enums": null,
              "Functions": null,
              "Sequences": null,
              "Tables": [
                {"Name": "t", "Comment": "", "Columns": null,
                 "Indexes": [], "Constraints": null, "Triggers": []}
              ],
              "Views": null
            }"#,
        )
        .expect("write schema.json");

        let description = fetch_expected_schema("frontend", "v4.1.1", None, &root.0)
            .await
            .expect("load bundled description");
        assert_eq!(description.extensions, vec!["pgcrypto"]);
        assert!(description.enums.is_empty());
        assert!(description.functions.is_empty());
        assert!(description.views.is_empty());
        assert_eq!(description.tables.len(), 1);
        assert!(description.tables[0].columns.is_empty());
        assert!(description.tables[0].constraints.is_empty());
    }

    #[tokio::test]
    async fn fetch_expected_schema_loads_go_repository_path() {
        let root = unique_temp_dir();
        let nested = root.0.join("internal/database");
        fs::create_dir_all(&nested).expect("create nested dir");
        fs::write(
            nested.join("schema.codeintel.json"),
            r#"{"Extensions": [], "Enums": [], "Functions": [],
                "Sequences": [], "Tables": [], "Views": []}"#,
        )
        .expect("write schema.codeintel.json");

        let description = fetch_expected_schema("codeintel", "v4.1.1", None, &root.0)
            .await
            .expect("load repository-path description");
        assert_eq!(description, SchemaDescription::default());
    }

    #[tokio::test]
    async fn fetch_expected_schema_loads_explicit_file() {
        let root = unique_temp_dir();
        fs::create_dir_all(&root.0).expect("create temp root");
        let path = root.0.join("explicit.json");
        fs::write(
            &path,
            r#"{"Extensions": ["hstore"], "Enums": [], "Functions": [],
                "Sequences": [], "Tables": [], "Views": []}"#,
        )
        .expect("write explicit description");

        let path = path.to_str().expect("utf-8 path");
        let description = fetch_expected_schema("frontend", "", Some(path), &root.0)
            .await
            .expect("load explicit description");
        assert_eq!(description.extensions, vec!["hstore"]);
    }

    #[tokio::test]
    async fn fetch_expected_schema_enforces_version_file_preconditions() {
        let root = unique_temp_dir();

        let error = fetch_expected_schema("frontend", "", None, &root.0)
            .await
            .expect_err("empty version without file");
        assert_eq!(error.to_string(), "must supply a -version or -file");

        let error = fetch_expected_schema("frontend", "v4.1.1", Some("/tmp/schema.json"), &root.0)
            .await
            .expect_err("version and file are exclusive");
        assert_eq!(
            error.to_string(),
            "the flags -version or -file are mutually exclusive"
        );

        let error = fetch_expected_schema("bogus", "v4.1.1", None, &root.0)
            .await
            .expect_err("unknown schema");
        assert_eq!(error.to_string(), r#"unknown schema name "bogus""#);

        let missing = root.0.join("does-not-exist.json");
        let error = fetch_expected_schema("frontend", "", Some(missing.to_str().unwrap()), &root.0)
            .await
            .expect_err("missing explicit file");
        assert_eq!(
            error.to_string(),
            format!("open {}: no such file or directory", missing.display())
        );
    }

    #[test]
    fn display_schema_summaries_no_drift() {
        assert_eq!(display_schema_summaries(&[]), "No drift detected\n");
    }

    #[test]
    fn display_schema_summaries_snapshot() {
        let summaries = vec![
            Summary::new(
                r#""t"."new""#,
                r#"Missing column "t"."new""#,
                "define the column",
            )
            .with_statements(vec![
                "ALTER TABLE t ADD COLUMN new integer NOT NULL;".to_string()
            ]),
            Summary::new(
                r#""t"."old""#,
                r#"Unexpected column "t"."old""#,
                "drop the column",
            )
            .with_statements(vec!["ALTER TABLE t DROP COLUMN IF EXISTS old;".to_string()])
            .with_diff(
                Value::String("expected".to_string()),
                Value::String("actual".to_string()),
            ),
        ];

        let expected = concat!(
            "Drift detected!\n",
            "\n",
            "Missing column \"t\".\"new\"\n",
            "Suggested action: define the column.\n",
            "```sql\n",
            "ALTER TABLE t ADD COLUMN new integer NOT NULL;\n",
            "```\n",
            "Unexpected column \"t\".\"old\"\n",
            "```diff\n",
            "- \"expected\"\n",
            "+ \"actual\"\n",
            "```\n",
            "Suggested action: drop the column.\n",
            "```sql\n",
            "ALTER TABLE t DROP COLUMN IF EXISTS old;\n",
            "```\n",
        );

        assert_eq!(display_schema_summaries(&summaries), expected);
    }

    #[test]
    fn search_url_contains_repo_file_and_encoded_terms() {
        let url = make_search_url(
            "frontend",
            "v4.1.1",
            &[
                "CREATE TABLE t".to_string(),
                "ALTER TABLE ONLY t".to_string(),
            ],
        );

        let parsed = url::Url::parse(&url).expect("valid URL");
        assert_eq!(parsed.path(), "/search");

        let query: BTreeMap<String, String> =
            url::form_urlencoded::parse(parsed.query().unwrap_or_default().as_bytes())
                .into_owned()
                .collect();
        assert_eq!(query.get("patternType").map(String::as_str), Some("regexp"));

        let pattern = query.get("q").expect("q parameter");
        assert!(pattern.contains(r"repo:^github\.com/sourcegraph/sourcegraph$@v4.1.1"));
        assert!(pattern.contains(r"file:^migrations/frontend/squashed\.sql$"));
        assert!(pattern.contains(r"(^|\b)CREATE\sTABLE\st($|\b)"));
        assert!(pattern.contains(" OR "));
        assert!(pattern.contains(r"(^|\b)ALTER\sTABLE\sONLY\st($|\b)"));
    }

    #[test]
    fn version_normalization_matches_cliutil() {
        assert_eq!(normalize_version("5.0.2"), "v5.0.2");
        assert_eq!(normalize_version("v5.0.2"), "v5.0.2");
        assert_eq!(normalize_version("5.0"), "v5.0.0");
        assert_eq!(normalize_version("5.2+dev"), "v5.2.0");
        assert_eq!(normalize_version("HEAD"), "HEAD");
        assert_eq!(
            normalize_version("57b1f56787619464dc62f469127d64721b428b76"),
            "57b1f56787619464dc62f469127d64721b428b76"
        );
        assert!(is_tag("v4.1.1"));
        assert!(!is_tag("HEAD"));
        assert!(is_commit("57b1f5678761"));
        assert!(is_commit("57b1f56787619464dc62f469127d64721b428b76"));
        assert!(!is_commit("57b1f56787"));
    }

    #[test]
    fn strip_indent_matches_go() {
        assert_eq!(
            strip_indent(" SELECT 1;\n   FROM t;"),
            "SELECT 1;\n  FROM t;"
        );
        assert_eq!(strip_indent("   x\n y"), "  x\ny");
    }
}
