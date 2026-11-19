#![forbid(unsafe_code)]

//! `internal/database/migration/definition`: parse `<version>_<slug>/`
//! directories (`up.sql`, `down.sql`, `metadata.yaml`) and build the
//! dependency graph.

use std::collections::BTreeMap;
use std::path::Path;

use crate::error::MigratorError;
use crate::types::{Definition, Definitions};

/// `ReadDefinitions(fs, schemaBasePath)`.
pub fn read_definitions(root: &Path, schema_base_path: &str) -> Result<Definitions, MigratorError> {
    let definitions = read_definitions_inner(root, schema_base_path)
        .map_err(|error| MigratorError::new(format!("readDefinitions: {error}")))?;

    validate_and_order(definitions)
}

fn read_definitions_inner(
    root: &Path,
    schema_base_path: &str,
) -> Result<Vec<Definition>, MigratorError> {
    let directory = root.join(schema_base_path);
    let entries = std::fs::read_dir(&directory).map_err(|error| {
        MigratorError::new(format!("open {}: {error}", directory.display()))
    })?;

    let mut definitions = Vec::new();
    for entry in entries {
        let entry = match entry {
            Ok(entry) => entry,
            Err(error) => {
                return Err(MigratorError::new(format!(
                    "read {}: {error}",
                    directory.display()
                )))
            }
        };

        let name = match entry.file_name().to_str() {
            Some(name) => name.to_string(),
            // Non-UTF-8 entry names can never be a versioned migration.
            None => continue,
        };

        // Not a versioned migration directory (e.g. `BUILD.bazel`,
        // `squashed.sql`): Go's ParseRawVersion fails and the entry is ignored.
        let version = match parse_raw_version(&name) {
            Some(version) => version,
            None => continue,
        };

        let definition = read_definition(&directory, schema_base_path, version, &name).map_err(
            |error| {
                MigratorError::new(format!(
                    "malformed migration definition at '{}': {error}",
                    join_path(schema_base_path, &name)
                ))
            },
        )?;
        definitions.push(definition);
    }

    definitions.sort_by_key(|definition| definition.id);
    Ok(definitions)
}

/// `ParseRawVersion`: the leading decimal digits of the entry name (up to the
/// first non-digit). Returns `None` when the name does not start with a digit.
fn parse_raw_version(raw_version: &str) -> Option<i32> {
    let digits: String = raw_version
        .chars()
        .take_while(|character| character.is_ascii_digit())
        .collect();
    if digits.is_empty() {
        return None;
    }

    digits.parse::<i32>().ok()
}

fn read_definition(
    schema_directory: &Path,
    schema_base_path: &str,
    version: i32,
    dir_name: &str,
) -> Result<Definition, MigratorError> {
    let definition_directory = schema_directory.join(dir_name);

    let up_sql = read_text(&definition_directory.join("up.sql"))?;
    let down_sql = read_text(&definition_directory.join("down.sql"))?;
    let metadata = read_text(&definition_directory.join("metadata.yaml"))?;

    hydrate_metadata_from_text(
        schema_base_path,
        dir_name,
        version,
        up_sql,
        down_sql,
        &metadata,
    )
}

fn read_text(path: &Path) -> Result<String, MigratorError> {
    let bytes = std::fs::read(path)
        .map_err(|error| MigratorError::new(format!("open {}: {error}", path.display())))?;
    String::from_utf8(bytes)
        .map_err(|error| MigratorError::new(format!("{}: {error}", path.display())))
}

/// `hydrateMetadataFromFile`: parse `metadata.yaml` and cross-check it against
/// the up/down query text.
fn hydrate_metadata_from_text(
    schema_base_path: &str,
    dir_name: &str,
    version: i32,
    up_sql: String,
    down_sql: String,
    metadata: &str,
) -> Result<Definition, MigratorError> {
    let payload = parse_metadata(metadata)
        .map_err(|error| MigratorError::new(format!("invalid metadata.yaml: {error}")))?;

    let mut parents = payload.parents;
    if payload.parent != 0 {
        parents.push(payload.parent);
    }
    parents.sort_unstable();

    let schema_path = join_path(schema_base_path, &version.to_string());
    let up_path = join_path(
        &join_path(schema_base_path, dir_name),
        "up.sql",
    );
    let metadata_path = join_path(
        &join_path(schema_base_path, dir_name),
        "metadata.yaml",
    );

    // `queryFromString` canonicalizes the SQL before it is inspected.
    let down_query_text = canonicalize_query(&down_sql);
    let up_query_text = canonicalize_query(&up_sql);

    if parse_index_metadata(&down_query_text).is_some() {
        return Err(instructional_error(
            "malformed concurrent index creation",
            format!(
                "did not expect down query of migration at '{schema_path}' to contain concurrent creation of an index"
            ),
            concat!(
                "Remove `CONCURRENTLY` when re-creating an old index in down migrations ",
                "(if you're seeing this in a local dev environment, try running `sg update` ",
                "to see if it fixes the issue first). Downgrades indicate an instance ",
                "stability error which generally requires a maintenance window."
            )
            .to_string(),
        ));
    }

    let mut is_create_index_concurrently = false;
    if parse_index_metadata(&up_query_text).is_some() {
        if !payload.create_index_concurrently {
            return Err(instructional_error(
                "malformed concurrent index creation",
                format!(
                    "did not expect up query of migration at '{schema_path}' to contain concurrent creation of an index"
                ),
                format!(
                    "Add `createIndexConcurrently: true` to the metadata file '{metadata_path}'."
                ),
            ));
        } else if !remove_concurrent_index_creation(&up_query_text).is_empty() {
            return Err(instructional_error(
                "malformed concurrent index creation",
                format!(
                    "did not expect up query of migration at '{schema_path}' to contain additional statements"
                ),
                format!("Split the index creation from '{up_path}' into a new migration file."),
            ));
        }

        is_create_index_concurrently = true;
    } else if payload.create_index_concurrently {
        return Err(instructional_error(
            "malformed concurrent index creation",
            format!(
                "expected up query of migration at '{schema_path}' to contain concurrent creation of an index"
            ),
            format!(
                "Remove `createIndexConcurrently: true` from the metadata file '{metadata_path}'."
            ),
        ));
    }

    if is_privileged(&up_query_text) || is_privileged(&down_query_text) {
        if !payload.privileged {
            return Err(instructional_error(
                "malformed Postgres extension modification",
                format!(
                    "did not expect queries of migration at '{schema_path}' to require elevated permissions"
                ),
                format!("Add `privileged: true` to the metadata file '{metadata_path}'."),
            ));
        }
    }

    Ok(Definition {
        id: version,
        name: payload.name,
        up_sql,
        down_sql,
        privileged: payload.privileged,
        non_idempotent: payload.non_idempotent,
        parents,
        is_create_index_concurrently,
    })
}

fn instructional_error(
    class: &str,
    description: String,
    instructions: String,
) -> MigratorError {
    MigratorError::new(format!("{class}: {description}\n\n{instructions}\n"))
}

/// Join two path-like components with `/`, matching Go's `filepath.Join` on
/// the supported (Unix) platform without normalizing away names.
fn join_path(base: &str, name: &str) -> String {
    if base.is_empty() {
        name.to_string()
    } else {
        format!("{}/{}", base.trim_end_matches('/'), name)
    }
}

// ---------------------------------------------------------------------------
// metadata.yaml parsing (minimal YAML subset used by the migration corpus)
// ---------------------------------------------------------------------------

#[derive(Debug, Default)]
struct MetadataPayload {
    name: String,
    parent: i32,
    parents: Vec<i32>,
    create_index_concurrently: bool,
    privileged: bool,
    non_idempotent: bool,
}

fn parse_metadata(contents: &str) -> Result<MetadataPayload, String> {
    let mut payload = MetadataPayload::default();
    // Key that may be followed by a block sequence (`- <int>` lines).
    let mut block_key: Option<String> = None;

    for raw_line in contents.split('\n') {
        let line = strip_comment(raw_line).trim();
        if line.is_empty() {
            continue;
        }

        // Block sequence entry (`- 10003`), only valid under `parents`.
        if line == "-" || line.starts_with("- ") {
            let item = line[1..].trim();
            match block_key.as_deref() {
                Some("parents") => payload.parents.push(parse_int(item)?),
                // Unknown keys are ignored entirely, including their nested
                // content (mirrors yaml.Unmarshal into a struct).
                _ => {}
            }
            continue;
        }

        let (key, value) = match line.split_once(':') {
            Some((key, value)) => (key.trim(), value.trim()),
            None => return Err(format!("could not parse metadata line {line:?}")),
        };

        block_key = None;
        match key {
            "name" => payload.name = parse_scalar(value),
            "parent" => payload.parent = parse_int(value)?,
            "parents" => {
                if value.is_empty() {
                    block_key = Some("parents".to_string());
                } else {
                    payload.parents = parse_int_list(value)?;
                }
            }
            "createIndexConcurrently" => {
                payload.create_index_concurrently = parse_bool(value)?
            }
            "privileged" => payload.privileged = parse_bool(value)?,
            "nonIdempotent" => payload.non_idempotent = parse_bool(value)?,
            _ => {}
        }
    }

    Ok(payload)
}

fn strip_comment(line: &str) -> &str {
    let mut in_single_quote = false;
    let mut in_double_quote = false;
    let mut previous_was_space = true;

    for (index, character) in line.char_indices() {
        match character {
            '\'' if !in_double_quote => in_single_quote = !in_single_quote,
            '"' if !in_single_quote => in_double_quote = !in_double_quote,
            '#' if !in_single_quote && !in_double_quote && previous_was_space => {
                return &line[..index];
            }
            _ => {}
        }
        previous_was_space = character.is_whitespace();
    }

    line
}

fn parse_scalar(value: &str) -> String {
    let value = value.trim();
    if value.len() >= 2 {
        if value.starts_with('\'') && value.ends_with('\'') {
            return value[1..value.len() - 1].replace("''", "'");
        }
        if value.starts_with('"') && value.ends_with('"') {
            return value[1..value.len() - 1].to_string();
        }
    }

    value.to_string()
}

fn parse_int(value: &str) -> Result<i32, String> {
    value
        .trim()
        .parse::<i32>()
        .map_err(|_| format!("invalid integer {value:?}"))
}

fn parse_bool(value: &str) -> Result<bool, String> {
    match value.trim() {
        "true" => Ok(true),
        "false" => Ok(false),
        other => Err(format!("invalid boolean {other:?}")),
    }
}

fn parse_int_list(value: &str) -> Result<Vec<i32>, String> {
    let value = value.trim();
    if !(value.starts_with('[') && value.ends_with(']')) {
        return Err(format!("invalid integer list {value:?}"));
    }

    let inner = &value[1..value.len() - 1];
    if inner.trim().is_empty() {
        return Ok(Vec::new());
    }

    inner
        .split(',')
        .map(|item| parse_int(item.trim()))
        .collect()
}

// ---------------------------------------------------------------------------
// SQL canonicalization and concurrent index parsing (read.go)
// ---------------------------------------------------------------------------

/// `CanonicalizeQuery`: strip historic YAML frontmatter and outermost
/// transaction blocks.
fn canonicalize_query(query: &str) -> String {
    const FRONTMATTER: &str = "-- +++\n";

    let mut query = query;
    if let Some(first) = query.find(FRONTMATTER) {
        let after_first = &query[first + FRONTMATTER.len()..];
        if let Some(second) = after_first.find(FRONTMATTER) {
            query = &after_first[second + FRONTMATTER.len()..];
        }
    }

    let mut canonical = query.trim().to_string();
    if let Some(rest) = canonical.strip_prefix("BEGIN;") {
        canonical = rest.to_string();
    }
    if let Some(rest) = canonical.strip_suffix("COMMIT;") {
        canonical = rest.to_string();
    }

    canonical.trim().to_string()
}

const CREATE_INDEX_CONCURRENTLY: &[u8] = b"CREATE";
const UNIQUE: &[u8] = b"UNIQUE";
const INDEX: &[u8] = b"INDEX";
const CONCURRENTLY: &[u8] = b"CONCURRENTLY";
const IF: &[u8] = b"IF";
const NOT: &[u8] = b"NOT";
const EXISTS: &[u8] = b"EXISTS";
const ON: &[u8] = b"ON";

struct Cursor<'a> {
    bytes: &'a [u8],
    position: usize,
}

impl<'a> Cursor<'a> {
    fn new(bytes: &'a [u8], position: usize) -> Self {
        Self { bytes, position }
    }

    fn eat(&mut self, literal: &[u8]) -> bool {
        if self.bytes[self.position..].starts_with(literal) {
            self.position += literal.len();
            true
        } else {
            false
        }
    }

    /// One or more `\s` (Go's `\s` is `[ \t\n\f\r]`).
    fn whitespace(&mut self) -> bool {
        let start = self.position;
        while self.position < self.bytes.len()
            && self.bytes[self.position].is_ascii_whitespace()
        {
            self.position += 1;
        }
        self.position > start
    }

    /// `[A-Za-z0-9_]+`.
    fn identifier(&mut self) -> Option<String> {
        let start = self.position;
        while self.position < self.bytes.len() {
            let byte = self.bytes[self.position];
            if byte.is_ascii_alphanumeric() || byte == b'_' {
                self.position += 1;
            } else {
                break;
            }
        }

        if self.position == start {
            None
        } else {
            // Only ASCII identifier bytes were consumed, so this is valid UTF-8.
            Some(String::from_utf8(self.bytes[start..self.position].to_vec()).unwrap())
        }
    }
}

/// Match `CREATE\s+(?:UNIQUE\s+)?INDEX\s+CONCURRENTLY\s+(?:IF\s+NOT\s+EXISTS\s+)?([A-Za-z0-9_]+)\s+ON\s+([A-Za-z0-9_]+)`
/// at `start`. Returns `(end, index_name, table_name)`.
fn match_create_index_at(bytes: &[u8], start: usize) -> Option<(usize, String, String)> {
    let mut cursor = Cursor::new(bytes, start);

    if !cursor.eat(CREATE_INDEX_CONCURRENTLY) {
        return None;
    }
    if !cursor.whitespace() {
        return None;
    }

    let before_unique = cursor.position;
    if cursor.eat(UNIQUE) {
        if !cursor.whitespace() {
            return None;
        }
    } else {
        cursor.position = before_unique;
    }

    if !cursor.eat(INDEX) {
        return None;
    }
    if !cursor.whitespace() {
        return None;
    }
    if !cursor.eat(CONCURRENTLY) {
        return None;
    }
    if !cursor.whitespace() {
        return None;
    }

    // Optional `IF NOT EXISTS`; the regex engine may skip the group, in which
    // case `IF` itself is a valid index name.
    let before_if = cursor.position;
    if cursor.eat(IF) {
        let matched = cursor.whitespace()
            && cursor.eat(NOT)
            && cursor.whitespace()
            && cursor.eat(EXISTS)
            && cursor.whitespace();
        if !matched {
            cursor.position = before_if;
        }
    }

    let index_name = cursor.identifier()?;
    if !cursor.whitespace() {
        return None;
    }
    if !cursor.eat(ON) {
        return None;
    }
    if !cursor.whitespace() {
        return None;
    }
    let table_name = cursor.identifier()?;

    Some((cursor.position, index_name, table_name))
}

/// Leftmost match of the concurrent-index pattern starting at or after
/// `start`. Returns `(match_start, match_end, index_name, table_name)`.
fn find_create_index_from(
    bytes: &[u8],
    start: usize,
) -> Option<(usize, usize, String, String)> {
    for position in start..bytes.len() {
        if bytes[position] != b'C' {
            continue;
        }
        if let Some((end, index_name, table_name)) = match_create_index_at(bytes, position) {
            return Some((position, end, index_name, table_name));
        }
    }

    None
}

/// `parseIndexMetadata`.
fn parse_index_metadata(query_text: &str) -> Option<(String, String)> {
    find_create_index_from(query_text.as_bytes(), 0)
        .map(|(_, _, index_name, table_name)| (index_name, table_name))
}

/// The `(table_name, index_name)` pair implied by a concurrent-index migration.
///
/// Go's `queryFromString` canonicalizes (and stores) the up query before
/// `parseIndexMetadata` runs, so canonicalize here as well: a [`Definition`]
/// keeps the raw file contents. Returns `None` when the query does not contain
/// the canonical `CREATE INDEX CONCURRENTLY` statement.
pub(crate) fn index_metadata(query_text: &str) -> Option<(String, String)> {
    let canonical = canonicalize_query(query_text);
    parse_index_metadata(&canonical).map(|(index_name, table_name)| (table_name, index_name))
}

/// Greedily match `[^;]+;` at `start`, returning the position just past `;`.
fn match_statement_tail(bytes: &[u8], start: usize) -> Option<usize> {
    let mut position = start;
    while position < bytes.len() && bytes[position] != b';' {
        position += 1;
    }

    if position > start && position < bytes.len() && bytes[position] == b';' {
        Some(position + 1)
    } else {
        None
    }
}

/// `removeConcurrentIndexCreation`.
fn remove_concurrent_index_creation(query: &str) -> String {
    let bytes = query.as_bytes();
    let mut replacement: Option<(usize, usize)> = None;
    let mut search_from = 0;

    while search_from < bytes.len() {
        let (start, end, _, _) = match find_create_index_from(bytes, search_from) {
            Some(found) => found,
            None => break,
        };

        if let Some(statement_end) = match_statement_tail(bytes, end) {
            replacement = Some((start, statement_end));
            break;
        }

        search_from = start + 1;
    }

    let mut query = query.to_string();
    if let Some((start, end)) = replacement {
        query.replace_range(start..end, "");
    }

    remove_comments(&query)
}

/// `removeComments`.
fn remove_comments(query: &str) -> String {
    let filtered: Vec<&str> = query
        .split('\n')
        .map(|line| line.split("--").next().unwrap_or("").trim())
        .filter(|line| !line.is_empty())
        .collect();

    filtered.join("\n").trim().to_string()
}

/// `isPrivileged`: `(CREATE|COMMENT ON|DROP)\s+EXTENSION`.
fn is_privileged(query_text: &str) -> bool {
    let bytes = query_text.as_bytes();
    let alternatives: [&[u8]; 3] = [b"CREATE", b"COMMENT ON", b"DROP"];

    for position in 0..bytes.len() {
        for alternative in alternatives {
            if !bytes[position..].starts_with(alternative) {
                continue;
            }

            let mut end = position + alternative.len();
            let whitespace_start = end;
            while end < bytes.len() && bytes[end].is_ascii_whitespace() {
                end += 1;
            }

            if end > whitespace_start && bytes[end..].starts_with(b"EXTENSION") {
                return true;
            }
        }
    }

    false
}

// ---------------------------------------------------------------------------
// Graph validation and ordering (reorderDefinitions)
// ---------------------------------------------------------------------------

/// `reorderDefinitions` + `NewDefinitions`: validate the dependency graph and
/// return definitions in topological order.
pub fn validate_and_order(
    definitions: Vec<Definition>,
) -> Result<Definitions, MigratorError> {
    let ordered = reorder_definitions(definitions)
        .map_err(|error| MigratorError::new(format!("reorderDefinitions: {error}")))?;

    Ok(build_definitions(ordered))
}

fn build_definitions(ordered: Vec<Definition>) -> Definitions {
    let mut by_id = BTreeMap::new();
    let mut children: BTreeMap<i32, Vec<i32>> = BTreeMap::new();
    let mut position = BTreeMap::new();

    for (index, definition) in ordered.iter().enumerate() {
        by_id.insert(definition.id, definition.clone());
        position.insert(definition.id, index);
        for parent in &definition.parents {
            children.entry(*parent).or_default().push(definition.id);
        }
    }

    Definitions::from_parts(ordered, by_id, children, position)
}

/// Port of Go's `reorderDefinitions`. Returns definitions ordered such that
/// every migration occurs after all of its parents.
fn reorder_definitions(mut definitions: Vec<Definition>) -> Result<Vec<Definition>, MigratorError> {
    if definitions.is_empty() {
        return Ok(definitions);
    }

    // `readDefinitions` sorts by ID before reordering; do the same so direct
    // callers of `NewDefinitions` observe the same deterministic order.
    definitions.sort_by_key(|definition| definition.id);

    for pair in definitions.windows(2) {
        if pair[0].id == pair[1].id {
            return Err(MigratorError::new(format!(
                "duplicate migration {}",
                pair[0].id
            )));
        }
    }

    let mut by_id: BTreeMap<i32, Definition> = BTreeMap::new();
    for definition in &definitions {
        by_id.insert(definition.id, definition.clone());
    }

    for definition in &definitions {
        for parent in &definition.parents {
            if !by_id.contains_key(parent) {
                return Err(unknown_migration_error(*parent, Some(definition.id)));
            }
        }
    }

    let root = find_root(&definitions)?;
    let mut children = build_children_map(&definitions);
    for child_ids in children.values_mut() {
        // Reverse-order each child slice so unrelated migrations end up ordered
        // by ascending version number after the final reversal.
        child_ids.sort_unstable_by(|a, b| b.cmp(a));
    }

    let mut marks: BTreeMap<i32, Mark> = BTreeMap::new();
    let mut order = Vec::with_capacity(definitions.len());

    dfs(root, &[], &children, &mut marks, &mut order)?;

    if order.len() < definitions.len() {
        // We have a single root but did not visit every node: there is a
        // disconnected cycle. Continue the traversal from each unvisited node.
        for definition in &definitions {
            if !marks.contains_key(&definition.id) {
                dfs(definition.id, &[], &children, &mut marks, &mut order)?;
            }
        }

        if order.len() < definitions.len() {
            return Err(MigratorError::new(
                "migration dependency cycle: unable to order migration definitions",
            ));
        }
    }

    let mut ordered = definitions.clone();
    let count = order.len();
    for (index, id) in order.into_iter().enumerate() {
        // `order` is post-order (children before parents), so fill backwards.
        ordered[count - 1 - index] = by_id
            .remove(&id)
            .ok_or_else(|| MigratorError::new(format!("unknown migration {id}")))?;
    }

    Ok(ordered)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Mark {
    Visiting,
    Visited,
}

fn dfs(
    id: i32,
    parents: &[i32],
    children: &BTreeMap<i32, Vec<i32>>,
    marks: &mut BTreeMap<i32, Mark>,
    order: &mut Vec<i32>,
) -> Result<(), MigratorError> {
    match marks.get(&id) {
        Some(Mark::Visiting) => {
            // Peel off the head of the parent list until we reach this node.
            let mut offset = 0;
            while offset < parents.len() && parents[offset] != id {
                offset += 1;
            }
            if offset == parents.len() {
                return Err(MigratorError::new(format!(
                    "migration dependency cycle at migration {id}"
                )));
            }

            let peeled = &parents[offset..];
            let previous = *peeled.last().unwrap_or(&id);
            let mut cycle = peeled.to_vec();
            cycle.push(id);

            let cycle_text = cycle
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
                .join(" -> ");

            return Err(instructional_error(
                "migration dependency cycle",
                format!(
                    "migrations {previous} and {id} declare each other as dependencies"
                ),
                format!("Break one of the links in the following cycle:\n{cycle_text}"),
            ));
        }
        Some(Mark::Visited) => return Ok(()),
        None => {}
    }

    marks.insert(id, Mark::Visiting);

    if let Some(child_ids) = children.get(&id) {
        for child in child_ids {
            let mut path = parents.to_vec();
            path.push(id);
            dfs(*child, &path, children, marks, order)?;
        }
    }

    marks.insert(id, Mark::Visited);
    order.push(id);
    Ok(())
}

fn build_children_map(definitions: &[Definition]) -> BTreeMap<i32, Vec<i32>> {
    let mut children: BTreeMap<i32, Vec<i32>> = BTreeMap::new();
    for definition in definitions {
        for parent in &definition.parents {
            children.entry(*parent).or_default().push(definition.id);
        }
    }
    children
}

/// `root`: the unique migration definition with no parent.
fn find_root(definitions: &[Definition]) -> Result<i32, MigratorError> {
    let roots: Vec<i32> = definitions
        .iter()
        .filter(|definition| definition.parents.is_empty())
        .map(|definition| definition.id)
        .collect();

    if roots.is_empty() {
        return Err(instructional_error(
            "no roots",
            "every migration declares a parent".to_string(),
            concat!(
                "There is no migration defined in this schema that does not declare a parent. ",
                "This indicates either a migration dependency cycle or a reference to a ",
                "parent migration that no longer exists."
            )
            .to_string(),
        ));
    }

    if roots.len() > 1 {
        let rendered = roots
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join(" ");

        return Err(instructional_error(
            "multiple roots",
            format!(
                "expected exactly one migration to have no parent but found {} ([{rendered}])",
                roots.len()
            ),
            concat!(
                "There are multiple migrations defined in this schema that do not declare ",
                "a parent. This indicates a new migration that did not correctly attach ",
                "itself to an existing migration. This may also indicate the presence of ",
                "a duplicate squashed migration."
            )
            .to_string(),
        ));
    }

    Ok(roots[0])
}

fn unknown_migration_error(id: i32, source: Option<i32>) -> MigratorError {
    match source {
        Some(source) => MigratorError::new(format!(
            "unknown migration {id} referenced from migration {source}"
        )),
        None => MigratorError::new(format!("unknown migration {id}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};

    const SCHEMA: &str = "migrations/frontend";

    struct TempDir(PathBuf);

    impl TempDir {
        fn new() -> Self {
            static COUNTER: AtomicU64 = AtomicU64::new(0);
            let unique = COUNTER.fetch_add(1, Ordering::SeqCst);
            let nanos = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos();
            let path = std::env::temp_dir().join(format!(
                "sg-migrator-definition-{}-{nanos}-{unique}",
                std::process::id()
            ));
            fs::create_dir_all(&path).unwrap();
            Self(path)
        }

        fn path(&self) -> &Path {
            &self.0
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    fn write_file(root: &Path, schema: &str, relative: &str, contents: &str) {
        let path = root.join(schema).join(relative);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).unwrap();
        }
        fs::write(path, contents).unwrap();
    }

    fn write_migration(
        root: &Path,
        dir: &str,
        up: &str,
        down: &str,
        metadata: &str,
    ) {
        write_file(root, SCHEMA, &format!("{dir}/up.sql"), up);
        write_file(root, SCHEMA, &format!("{dir}/down.sql"), down);
        write_file(root, SCHEMA, &format!("{dir}/metadata.yaml"), metadata);
    }

    fn read(root: &Path) -> Result<Definitions, MigratorError> {
        read_definitions(root, SCHEMA)
    }

    fn error_message(result: Result<Definitions, MigratorError>) -> String {
        result.expect_err("expected error").to_string()
    }

    #[test]
    fn reads_valid_chain_with_parents() {
        let tree = TempDir::new();
        write_migration(
            tree.path(),
            "10001",
            "10001 UP",
            "10001 DOWN",
            "name: first\n",
        );
        write_migration(
            tree.path(),
            "10002",
            "10002 UP",
            "10002 DOWN",
            "name: 'second'\nparent: 10001\n",
        );
        write_migration(
            tree.path(),
            "10003_do_the_thing",
            "10003 UP",
            "10003 DOWN",
            "name: third\nparents: [10001, 10002]\n",
        );

        let definitions = read(tree.path()).unwrap();
        let all = definitions.all();
        let ids: Vec<i32> = all.iter().map(|definition| definition.id).collect();
        assert_eq!(ids, vec![10001, 10002, 10003]);

        assert_eq!(all[0].name, "first");
        assert!(all[0].parents.is_empty());
        assert_eq!(all[0].up_sql, "10001 UP");
        assert_eq!(all[0].down_sql, "10001 DOWN");

        assert_eq!(all[1].name, "second");
        assert_eq!(all[1].parents, vec![10001]);

        assert_eq!(all[2].name, "third");
        assert_eq!(all[2].parents, vec![10001, 10002]);

        assert!(definitions.get_by_id(10002).is_some());
        assert!(definitions.get_by_id(99999).is_none());
    }

    #[test]
    fn merges_and_sorts_parent_and_parents() {
        let tree = TempDir::new();
        write_migration(tree.path(), "10001", "up", "down", "name: root\n");
        write_migration(
            tree.path(),
            "10002",
            "up",
            "down",
            "name: child\nparent: 10001\n",
        );
        write_migration(
            tree.path(),
            "10003",
            "up",
            "down",
            "name: merged\nparents:\n  - 10002\nparent: 10001\n",
        );

        let definitions = read(tree.path()).unwrap();
        let merged = definitions.get_by_id(10003).unwrap();
        assert_eq!(merged.parents, vec![10001, 10002]);
    }

    #[test]
    fn ignores_non_versioned_entries() {
        let tree = TempDir::new();
        write_file(tree.path(), SCHEMA, "BUILD.bazel", "not a migration");
        write_file(tree.path(), SCHEMA, "squashed.sql", "select 1;");
        write_file(tree.path(), SCHEMA, "README.md", "docs");
        write_migration(tree.path(), "10001", "up", "down", "name: only\n");

        let definitions = read(tree.path()).unwrap();
        assert_eq!(definitions.all().len(), 1);
        assert_eq!(definitions.all()[0].id, 10001);
    }

    #[test]
    fn rejects_duplicate_ids() {
        let tree = TempDir::new();
        write_migration(tree.path(), "10001_a", "up", "down", "name: a\n");
        write_migration(tree.path(), "10001_b", "up", "down", "name: b\n");

        let message = error_message(read(tree.path()));
        assert!(
            message.contains("duplicate migration 10001"),
            "unexpected error: {message}"
        );
    }

    #[test]
    fn rejects_missing_parent() {
        let tree = TempDir::new();
        write_migration(tree.path(), "10001", "up", "down", "name: root\n");
        write_migration(
            tree.path(),
            "10002",
            "up",
            "down",
            "name: orphan\nparent: 99999\n",
        );

        let message = error_message(read(tree.path()));
        assert!(
            message.contains("unknown migration 99999 referenced from migration 10002"),
            "unexpected error: {message}"
        );
    }

    #[test]
    fn rejects_cycles() {
        let tree = TempDir::new();
        write_migration(tree.path(), "10001", "up", "down", "name: root\n");
        write_migration(
            tree.path(),
            "10002",
            "up",
            "down",
            "name: child\nparent: 10001\n",
        );
        write_migration(
            tree.path(),
            "10003",
            "up",
            "down",
            "name: cycle a\nparent: 10004\n",
        );
        write_migration(
            tree.path(),
            "10004",
            "up",
            "down",
            "name: cycle b\nparent: 10003\n",
        );

        let message = error_message(read(tree.path()));
        assert!(
            message.contains("migration dependency cycle"),
            "unexpected error: {message}"
        );
    }

    #[test]
    fn parses_metadata_booleans() {
        let tree = TempDir::new();
        write_migration(
            tree.path(),
            "10001",
            "CREATE EXTENSION IF NOT EXISTS citext;",
            "DROP EXTENSION IF EXISTS citext;",
            "name: privileged\nprivileged: true\nnonIdempotent: true\n",
        );
        write_migration(
            tree.path(),
            "10002",
            "-- docs\nCREATE INDEX CONCURRENTLY IF NOT EXISTS idx ON tbl(col1);\n",
            "DROP INDEX IF EXISTS idx;",
            "name: concurrent\nparent: 10001\ncreateIndexConcurrently: true\n",
        );

        let definitions = read(tree.path()).unwrap();

        let privileged = definitions.get_by_id(10001).unwrap();
        assert!(privileged.privileged);
        assert!(privileged.non_idempotent);
        assert!(!privileged.is_create_index_concurrently);

        let concurrent = definitions.get_by_id(10002).unwrap();
        assert!(concurrent.is_create_index_concurrently);
        assert!(!concurrent.privileged);
        assert!(!concurrent.non_idempotent);
    }

    #[test]
    fn rejects_malformed_metadata() {
        let tree = TempDir::new();
        write_migration(tree.path(), "10001", "up", "down", "name: bad\nparent: nope\n");

        let message = error_message(read(tree.path()));
        assert!(
            message.contains("malformed migration definition"),
            "unexpected error: {message}"
        );
        assert!(
            message.contains("invalid integer"),
            "unexpected error: {message}"
        );
    }

    #[test]
    fn rejects_unexpected_concurrent_index() {
        let tree = TempDir::new();
        write_migration(tree.path(), "10001", "up", "down", "name: root\n");
        write_migration(
            tree.path(),
            "10002",
            "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx ON tbl(a);",
            "DROP INDEX IF EXISTS idx;",
            "name: unexpected\nparent: 10001\n",
        );

        let message = error_message(read(tree.path()));
        assert!(
            message.contains("did not expect up query of migration at 'migrations/frontend/10002'"),
            "unexpected error: {message}"
        );
    }

    #[test]
    fn rejects_expected_but_missing_concurrent_index() {
        let tree = TempDir::new();
        write_migration(tree.path(), "10001", "up", "down", "name: root\n");
        write_migration(
            tree.path(),
            "10002",
            "10002 UP",
            "10002 DOWN",
            "name: expected\nparent: 10001\ncreateIndexConcurrently: true\n",
        );

        let message = error_message(read(tree.path()));
        assert!(
            message.contains("expected up query of migration at 'migrations/frontend/10002'"),
            "unexpected error: {message}"
        );
    }

    #[test]
    fn rejects_non_isolated_concurrent_index() {
        let tree = TempDir::new();
        write_migration(tree.path(), "10001", "up", "down", "name: root\n");
        write_migration(
            tree.path(),
            "10002",
            "ALTER TABLE tbl ADD COLUMN c text;\n\nCREATE INDEX CONCURRENTLY IF NOT EXISTS idx ON tbl(a);",
            "DROP INDEX IF EXISTS idx;",
            "name: extra\nparent: 10001\ncreateIndexConcurrently: true\n",
        );

        let message = error_message(read(tree.path()));
        assert!(
            message.contains("contain additional statements"),
            "unexpected error: {message}"
        );
    }

    #[test]
    fn rejects_concurrent_index_in_down_query() {
        let tree = TempDir::new();
        write_migration(tree.path(), "10001", "up", "down", "name: root\n");
        write_migration(
            tree.path(),
            "10002",
            "10002 UP",
            "-- docs\nCREATE INDEX CONCURRENTLY IF NOT EXISTS idx ON tbl(a);",
            "name: down\nparent: 10001\n",
        );

        let message = error_message(read(tree.path()));
        assert!(
            message.contains("did not expect down query of migration at 'migrations/frontend/10002'"),
            "unexpected error: {message}"
        );
    }

    #[test]
    fn rejects_unmarked_privileged_queries() {
        let tree = TempDir::new();
        write_migration(
            tree.path(),
            "10001",
            "CREATE EXTENSION IF NOT EXISTS citext;",
            "DROP EXTENSION IF EXISTS citext;",
            "name: unmarked\n",
        );

        let message = error_message(read(tree.path()));
        assert!(
            message.contains("require elevated permissions"),
            "unexpected error: {message}"
        );
    }

    #[test]
    fn extracts_index_metadata_from_concurrent_up_queries() {
        assert_eq!(
            index_metadata("CREATE INDEX CONCURRENTLY IF NOT EXISTS idx ON tbl(col);"),
            Some(("tbl".to_string(), "idx".to_string()))
        );
        assert_eq!(
            index_metadata("CREATE UNIQUE INDEX CONCURRENTLY idx ON tbl (col);"),
            Some(("tbl".to_string(), "idx".to_string()))
        );
        // The helper canonicalizes historic transaction wrappers first.
        assert_eq!(
            index_metadata("BEGIN;\nCREATE INDEX CONCURRENTLY idx ON tbl(col);\nCOMMIT;"),
            Some(("tbl".to_string(), "idx".to_string()))
        );
        // Non-concurrent statements (and unrelated DDL) yield no metadata.
        assert_eq!(index_metadata("CREATE INDEX idx ON tbl(col);"), None);
        assert_eq!(index_metadata("ALTER TABLE tbl ADD COLUMN c text;"), None);
    }

    #[test]
    fn parses_inline_and_block_parent_lists() {
        let tree = TempDir::new();
        write_migration(tree.path(), "10001", "up", "down", "name: root\n");
        write_migration(
            tree.path(),
            "10002",
            "up",
            "down",
            "name: inline\nparent: 10001\nparents: [10001]\n",
        );

        // Duplicate parent entries are preserved, matching Go's append+sort.
        let definitions = read(tree.path()).unwrap();
        assert_eq!(
            definitions.get_by_id(10002).unwrap().parents,
            vec![10001, 10001]
        );
    }
}
