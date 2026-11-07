#![forbid(unsafe_code)]

//! SQLite symbol store, ported from `cmd/symbols/internal/database/store`.
//!
//! Schema, index names and query shape match the baseline so that a database
//! produced by either implementation can be read by the other.

use std::collections::HashMap;
use std::path::Path;
use std::sync::{Mutex, OnceLock};

use regex::Regex;
use regex_syntax::hir::{HirKind, Look};
use rusqlite::functions::FunctionFlags;
use rusqlite::types::Value;
use rusqlite::{params_from_iter, Connection, OpenFlags};
use tokio::sync::mpsc;

/// `store.MaxSymbolLimit`.
pub const MAX_SYMBOL_LIMIT: i64 = 50_000;

const MAX_SQLITE_PARAMETERS: usize = 999;
const SYMBOL_COLUMNS: usize = 12;
const BATCH_ROWS: usize = MAX_SQLITE_PARAMETERS / SYMBOL_COLUMNS;
const DELETE_CHUNK: usize = 1000;
const REGEX_CACHE_SIZE: usize = 1000;

const CREATE_META_TABLE: &str = "
CREATE TABLE IF NOT EXISTS meta (
    id INTEGER PRIMARY KEY CHECK (id = 0),
    revision TEXT NOT NULL
)";

const CREATE_SYMBOLS_TABLE: &str = "
CREATE TABLE IF NOT EXISTS symbols (
    name VARCHAR(256) NOT NULL,
    namelowercase VARCHAR(256) NOT NULL,
    path VARCHAR(4096) NOT NULL,
    pathlowercase VARCHAR(4096) NOT NULL,
    line INT NOT NULL,
    character INT NOT NULL,
    kind VARCHAR(255) NOT NULL,
    language VARCHAR(255) NOT NULL,
    parent VARCHAR(255) NOT NULL,
    parentkind VARCHAR(255) NOT NULL,
    signature VARCHAR(255) NOT NULL,
    filelimited BOOLEAN NOT NULL
)";

const CREATE_INDEXES: [&str; 4] = [
    "CREATE INDEX idx_name ON symbols(name)",
    "CREATE INDEX idx_path ON symbols(path)",
    "CREATE INDEX idx_namelowercase ON symbols(namelowercase)",
    "CREATE INDEX idx_pathlowercase ON symbols(pathlowercase)",
];

const INSERT_ROW: &str = "(?,?,?,?,?,?,?,?,?,?,?,?)";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Symbol {
    pub name: String,
    pub path: String,
    pub line: i32,
    pub character: i32,
    pub kind: String,
    pub language: String,
    pub parent: String,
    pub parent_kind: String,
    pub signature: String,
    pub file_limited: bool,
}

/// Search arguments after proto conversion (`IsRegExp` is ignored by the
/// baseline store, so it is absent here too).
#[derive(Debug, Clone, Default)]
pub struct SearchParams {
    pub query: String,
    pub is_case_sensitive: bool,
    pub include_patterns: Vec<String>,
    pub exclude_pattern: String,
    pub include_langs: Vec<String>,
    pub exclude_langs: Vec<String>,
    pub first: i64,
}

#[derive(Debug, Default, PartialEq, Eq)]
pub struct SearchResult {
    pub symbols: Vec<Symbol>,
    pub limit_hit: bool,
}

/// Consume the parser output and return any fatal error.
pub type SymbolReceiver = mpsc::Receiver<Result<Symbol, String>>;

/// Create a database from scratch. The receiver is drained until all senders
/// are dropped.
pub fn write_full(path: &Path, commit: &str, symbols: &mut SymbolReceiver) -> Result<(), String> {
    let mut connection = open(path)?;
    let transaction = connection.transaction().map_err(stringify)?;
    transaction
        .execute_batch(CREATE_META_TABLE)
        .map_err(stringify)?;
    transaction
        .execute_batch(CREATE_SYMBOLS_TABLE)
        .map_err(stringify)?;
    transaction
        .execute("INSERT INTO meta (id, revision) VALUES (0, ?1)", [commit])
        .map_err(stringify)?;
    insert_symbols(&transaction, symbols)?;
    for index in CREATE_INDEXES {
        transaction.execute_batch(index).map_err(stringify)?;
    }
    transaction.commit().map_err(stringify)
}

/// Copy-on-write update of an existing database: bump the revision, delete the
/// paths that changed and re-insert their symbols.
pub fn write_incremental(
    path: &Path,
    commit: &str,
    deleted_paths: &[String],
    symbols: &mut SymbolReceiver,
) -> Result<(), String> {
    let mut connection = open(path)?;
    let transaction = connection.transaction().map_err(stringify)?;
    transaction
        .execute("UPDATE meta SET revision = ?1", [commit])
        .map_err(stringify)?;
    for chunk in deleted_paths.chunks(DELETE_CHUNK) {
        let placeholders = std::iter::repeat_n("?", chunk.len())
            .collect::<Vec<_>>()
            .join(",");
        let sql = format!("DELETE FROM symbols WHERE path IN ({placeholders})");
        transaction
            .execute(&sql, params_from_iter(chunk.iter()))
            .map_err(stringify)?;
    }
    insert_symbols(&transaction, symbols)?;
    transaction.commit().map_err(stringify)
}

/// Read the repository revision recorded in `meta` (used to pick an
/// incremental base). A missing table/revision means "not usable as a base".
pub fn read_commit(path: &Path) -> Option<String> {
    let connection = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY).ok()?;
    let commit = connection
        .query_row("SELECT revision FROM meta", [], |row| {
            row.get::<_, String>(0)
        })
        .ok()?;
    Some(commit)
}

fn open(path: &Path) -> Result<Connection, String> {
    Connection::open(path).map_err(|err| format!("open {}: {err}", path.display()))
}

fn insert_symbols(connection: &Connection, symbols: &mut SymbolReceiver) -> Result<(), String> {
    let mut rows: Vec<Value> = Vec::with_capacity(BATCH_ROWS * SYMBOL_COLUMNS);
    loop {
        match symbols.blocking_recv() {
            None => break,
            Some(Err(err)) => return Err(err),
            Some(Ok(symbol)) => {
                push_row(&mut rows, symbol);
                if rows.len() == BATCH_ROWS * SYMBOL_COLUMNS {
                    insert_batch(connection, &rows)?;
                    rows.clear();
                }
            }
        }
    }
    if !rows.is_empty() {
        insert_batch(connection, &rows)?;
    }
    Ok(())
}

fn insert_batch(connection: &Connection, rows: &[Value]) -> Result<(), String> {
    let count = rows.len() / SYMBOL_COLUMNS;
    let placeholders = std::iter::repeat_n(INSERT_ROW, count)
        .collect::<Vec<_>>()
        .join(",");
    let sql = format!(
        "INSERT INTO symbols (name, namelowercase, path, pathlowercase, line, character, \
         kind, language, parent, parentkind, signature, filelimited) VALUES {placeholders}"
    );
    connection
        .execute(&sql, params_from_iter(rows.iter()))
        .map_err(stringify)?;
    Ok(())
}

fn push_row(rows: &mut Vec<Value>, symbol: Symbol) {
    // Column order from INSERT_ROW: name, namelowercase, path, pathlowercase,
    // line, character, kind, language, parent, parentkind, signature,
    // filelimited.
    rows.push(Value::Text(symbol.name.clone()));
    rows.push(Value::Text(symbol.name.to_lowercase()));
    rows.push(Value::Text(symbol.path.clone()));
    rows.push(Value::Text(symbol.path.to_lowercase()));
    rows.push(Value::Integer(symbol.line as i64));
    rows.push(Value::Integer(symbol.character as i64));
    rows.push(Value::Text(symbol.kind));
    rows.push(Value::Text(symbol.language));
    rows.push(Value::Text(symbol.parent));
    rows.push(Value::Text(symbol.parent_kind));
    rows.push(Value::Text(symbol.signature));
    rows.push(Value::Integer(if symbol.file_limited { 1 } else { 0 }));
}

fn stringify(err: rusqlite::Error) -> String {
    err.to_string()
}

/// Execute a symbol search against an existing database.
pub fn search(path: &Path, params: &SearchParams) -> Result<SearchResult, String> {
    if params.first < 0 {
        return Err("limit must be greater than or equal to 0".to_owned());
    }

    let mut limit = params.first;
    let out_of_bounds = limit > MAX_SYMBOL_LIMIT + 1;
    if out_of_bounds {
        limit = MAX_SYMBOL_LIMIT;
    }

    let connection = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY)
        .map_err(|err| format!("open {}: {err}", path.display()))?;
    register_regexp(&connection)?;

    let (where_sql, mut values) = build_conditions(params);
    let sql = format!(
        "SELECT name, path, line, character, kind, language, parent, parentkind, signature, \
         filelimited FROM symbols WHERE {where_sql} LIMIT ?"
    );
    values.push(Value::Integer(limit));

    let mut statement = connection.prepare(&sql).map_err(stringify)?;
    let mut rows = statement
        .query(params_from_iter(values.iter()))
        .map_err(stringify)?;

    let mut symbols = Vec::new();
    while let Some(row) = rows.next().map_err(stringify)? {
        symbols.push(Symbol {
            name: row.get(0).map_err(stringify)?,
            path: row.get(1).map_err(stringify)?,
            line: row.get(2).map_err(stringify)?,
            character: row.get(3).map_err(stringify)?,
            kind: row.get(4).map_err(stringify)?,
            language: row.get(5).map_err(stringify)?,
            parent: row.get(6).map_err(stringify)?,
            parent_kind: row.get(7).map_err(stringify)?,
            signature: row.get(8).map_err(stringify)?,
            file_limited: row.get::<_, i64>(9).map_err(stringify)? != 0,
        });
    }

    let limit_hit = out_of_bounds && symbols.len() as i64 == limit;
    Ok(SearchResult { symbols, limit_hit })
}

fn build_conditions(params: &SearchParams) -> (String, Vec<Value>) {
    let mut conditions: Vec<(String, Vec<Value>)> = Vec::new();

    if let Some(condition) = search_condition("name", &params.query, params.is_case_sensitive) {
        conditions.push(condition);
    }
    if let Some(condition) =
        search_condition("path", &params.exclude_pattern, params.is_case_sensitive)
    {
        conditions.push(negate(condition));
    }
    for include in &params.include_patterns {
        if let Some(condition) = search_condition("path", include, params.is_case_sensitive) {
            conditions.push(condition);
        }
    }
    for language in &params.include_langs {
        conditions.push(lang_condition(language));
    }
    for language in &params.exclude_langs {
        conditions.push(negate(lang_condition(language)));
    }

    if conditions.is_empty() {
        return ("TRUE".to_owned(), Vec::new());
    }

    let mut sql = String::new();
    let mut values = Vec::new();
    for (index, (condition, condition_values)) in conditions.into_iter().enumerate() {
        if index > 0 {
            sql.push_str(" AND ");
        }
        sql.push_str(&condition);
        values.extend(condition_values);
    }
    (sql, values)
}

fn negate(condition: (String, Vec<Value>)) -> (String, Vec<Value>) {
    (format!("NOT ({})", condition.0), condition.1)
}

fn search_condition(
    column: &str,
    regex: &str,
    case_sensitive: bool,
) -> Option<(String, Vec<Value>)> {
    if regex.is_empty() {
        return None;
    }

    if let Some(literal) = literal_match(regex, true) {
        return Some(if case_sensitive {
            (format!("{column} = ?"), vec![Value::Text(literal)])
        } else {
            (
                format!("{}lowercase = ?", column),
                vec![Value::Text(literal.to_lowercase())],
            )
        });
    }

    if let Some(literal) = literal_match(regex, false) {
        let pattern = format!("{}*", glob_escape(&literal));
        return Some(if case_sensitive {
            (format!("{column} GLOB ?"), vec![Value::Text(pattern)])
        } else {
            (
                format!("{}lowercase GLOB ?", column),
                vec![Value::Text(pattern.to_lowercase())],
            )
        });
    }

    let pattern = if case_sensitive {
        regex.to_owned()
    } else {
        format!("(?i:{regex})")
    };
    Some((format!("{column} REGEXP ?"), vec![Value::Text(pattern)]))
}

fn lang_condition(language: &str) -> (String, Vec<Value>) {
    let languages = crate::language::enry_to_ctags_langs(language);
    let placeholders = std::iter::repeat_n("?", languages.len())
        .collect::<Vec<_>>()
        .join(",");
    (
        format!("lower(language) IN ({placeholders})"),
        languages.into_iter().map(Value::Text).collect(),
    )
}

/// `store.isLiteralEquality` / `isLiteralPrefix`, implemented over the HIR.
fn literal_match(expression: &str, equality: bool) -> Option<String> {
    let hir = regex_syntax::Parser::new().parse(expression).ok()?;
    let HirKind::Concat(sub) = hir.kind() else {
        return None;
    };
    let expected = if equality { 3 } else { 2 };
    if sub.len() != expected || !matches!(sub[0].kind(), HirKind::Look(Look::Start)) {
        return None;
    }
    if equality && !matches!(sub[2].kind(), HirKind::Look(Look::End)) {
        return None;
    }
    match sub[1].kind() {
        HirKind::Literal(literal) => String::from_utf8(literal.0.to_vec()).ok(),
        _ => None,
    }
}

/// `store.globEscape`.
fn glob_escape(input: &str) -> String {
    let mut escaped = String::with_capacity(input.len());
    for character in input.chars() {
        if matches!(character, '[' | ']' | '*' | '?') {
            escaped.push('[');
            escaped.push(character);
            escaped.push(']');
        } else {
            escaped.push(character);
        }
    }
    escaped
}

fn register_regexp(connection: &Connection) -> Result<(), String> {
    connection
        .create_scalar_function(
            "REGEXP",
            2,
            FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
            |context| {
                let pattern: String = context.get(0)?;
                let value: String = context.get(1)?;
                let regex = compile_regexp(&pattern).map_err(|err| {
                    rusqlite::Error::UserFunctionError(
                        format!("invalid regexp {pattern:?}: {err}").into(),
                    )
                })?;
                Ok(regex.is_match(&value))
            },
        )
        .map_err(stringify)
}

fn compile_regexp(pattern: &str) -> Result<Regex, String> {
    static CACHE: OnceLock<Mutex<HashMap<String, Regex>>> = OnceLock::new();
    let cache = CACHE.get_or_init(|| Mutex::new(HashMap::new()));
    if let Some(regex) = cache.lock().expect("regex cache").get(pattern) {
        return Ok(regex.clone());
    }

    let regex = Regex::new(pattern).map_err(|err| err.to_string())?;
    let mut cache = cache.lock().expect("regex cache");
    if cache.len() >= REGEX_CACHE_SIZE {
        cache.clear();
    }
    cache.insert(pattern.to_owned(), regex.clone());
    Ok(regex)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn database(symbols: Vec<Symbol>) -> (tempfile::TempDir, std::path::PathBuf) {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("index.zip");
        let (tx, mut rx) = mpsc::channel(64);
        for symbol in symbols {
            tx.blocking_send(Ok(symbol)).expect("send symbol");
        }
        drop(tx);
        write_full(&path, "abc", &mut rx).expect("write database");
        (dir, path)
    }

    fn symbol(name: &str, path: &str, language: &str) -> Symbol {
        Symbol {
            name: name.to_owned(),
            path: path.to_owned(),
            line: 0,
            character: 1,
            kind: "function".to_owned(),
            language: language.to_owned(),
            parent: String::new(),
            parent_kind: String::new(),
            signature: String::new(),
            file_limited: false,
        }
    }

    fn params(query: &str) -> SearchParams {
        SearchParams {
            query: query.to_owned(),
            first: 10,
            ..SearchParams::default()
        }
    }

    #[test]
    fn matches_exact_prefix_and_regex() {
        let (_dir, path) = database(vec![
            symbol("Foo", "a.go", "Go"),
            symbol("Foobar", "b.go", "Go"),
            symbol("bar", "c.go", "Go"),
        ]);

        let exact = search(
            &path,
            &SearchParams {
                is_case_sensitive: true,
                ..params("^Foo$")
            },
        )
        .expect("exact");
        assert_eq!(exact.symbols, vec![symbol("Foo", "a.go", "Go")]);

        let prefix = search(&path, &params("^Foo")).expect("prefix");
        assert_eq!(prefix.symbols.len(), 2);

        let case_insensitive = search(&path, &params("^foo$")).expect("case insensitive");
        assert_eq!(case_insensitive.symbols, vec![symbol("Foo", "a.go", "Go")]);

        let case_sensitive = search(
            &path,
            &SearchParams {
                is_case_sensitive: true,
                ..params("^foo$")
            },
        )
        .expect("case sensitive");
        assert!(case_sensitive.symbols.is_empty());

        let regex = search(&path, &params("^Fo+")).expect("regex");
        assert_eq!(regex.symbols.len(), 2);
    }

    #[test]
    fn filters_by_path_and_language() {
        let (_dir, path) = database(vec![
            symbol("x", "a.pl", "Perl"),
            symbol("y", "a.pl", "Perl"),
            symbol("z", ".zshrc", "Zsh"),
        ]);

        let by_path = search(
            &path,
            &SearchParams {
                include_patterns: vec!["^a.pl$".to_owned()],
                ..params("")
            },
        )
        .expect("path");
        assert_eq!(by_path.symbols.len(), 2);

        let excluded = search(
            &path,
            &SearchParams {
                exclude_pattern: "a.pl".to_owned(),
                is_case_sensitive: true,
                ..params("")
            },
        )
        .expect("exclude");
        assert_eq!(excluded.symbols, vec![symbol("z", ".zshrc", "Zsh")]);

        let shell = search(
            &path,
            &SearchParams {
                include_langs: vec!["Shell".to_owned()],
                is_case_sensitive: true,
                ..params(".*")
            },
        )
        .expect("shell");
        assert_eq!(shell.symbols, vec![symbol("z", ".zshrc", "Zsh")]);
    }

    #[test]
    fn applies_the_limit_semantics() {
        let (_dir, path) = database(
            (0..3)
                .map(|i| symbol(&format!("s{i}"), "a", "Go"))
                .collect(),
        );

        let limited = search(
            &path,
            &SearchParams {
                first: 2,
                ..params("")
            },
        )
        .expect("limit");
        assert_eq!(limited.symbols.len(), 2);
        assert!(!limited.limit_hit);

        let zero = search(
            &path,
            &SearchParams {
                first: 0,
                ..params("")
            },
        )
        .expect("zero");
        assert!(zero.symbols.is_empty());

        let negative = search(
            &path,
            &SearchParams {
                first: -1,
                ..params("")
            },
        );
        assert!(negative.is_err());
    }

    #[test]
    fn rewrites_changed_paths_incrementally() {
        let (_dir, path) = database(vec![
            symbol("old", "a.go", "Go"),
            symbol("keep", "b.go", "Go"),
        ]);
        let (tx, mut rx) = mpsc::channel(8);
        tx.blocking_send(Ok(symbol("new", "a.go", "Go")))
            .expect("send");
        drop(tx);
        write_incremental(&path, "def", &["a.go".to_owned()], &mut rx).expect("incremental");

        let result = search(&path, &params("")).expect("search");
        assert_eq!(result.symbols.len(), 2);
        assert!(result.symbols.iter().any(|symbol| symbol.name == "new"));
    }
}
