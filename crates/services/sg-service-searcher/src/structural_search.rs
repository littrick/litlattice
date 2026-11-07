#![forbid(unsafe_code)]

//! Structural search through a `comby` subprocess (`search_structural.go`).
//!
//! Filtered structural search first runs an approximate regexp search over the
//! prepared archive to find candidate files, then asks comby to match the
//! pattern inside that zip and streams the results back. Only the zip input is
//! wired here; the tar input exists in the baseline for the zoekt short-circuit
//! and follows with the indexed path.

use std::io::{BufRead, BufReader, Read};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use regex::Regex;
use serde::Deserialize;
use tokio_util::sync::CancellationToken;

use crate::chunk::{chunk_ranges, chunks_to_matches};
use crate::langmatch::to_lang_matcher;
use crate::matchtree::to_match_tree;
use crate::pathmatch::to_path_matcher;
use crate::protocol::{FileMatch, Location, PatternInfo, PatternNode, QueryNode, Range};
use crate::regex_search::{RegexSearch, SearchError};
use crate::sender::LimitedStream;
use crate::zipcache::ZipFile;

/// Baseline `numWorkers` for structural search: forked processes are capped to
/// bound the amount of zip data mapped to memory.
pub const NUM_WORKERS: usize = 4;

/// Failure while running a structural search.
#[derive(Debug)]
pub enum StructuralError {
    /// The query is not a single leaf pattern (`extractQueryAtom`).
    BadQuery(String),
    /// Reading the archive failed.
    Search(SearchError),
    /// I/O while talking to comby or reading the archive.
    Io(std::io::Error),
    /// comby rejected the pattern or exited unsuccessfully.
    Comby(String),
}

impl std::fmt::Display for StructuralError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::BadQuery(message) | Self::Comby(message) => f.write_str(message),
            Self::Search(err) => err.fmt(f),
            Self::Io(err) => write!(f, "{err}"),
        }
    }
}

impl std::error::Error for StructuralError {}

impl From<std::io::Error> for StructuralError {
    fn from(err: std::io::Error) -> Self {
        Self::Io(err)
    }
}

impl From<SearchError> for StructuralError {
    fn from(err: SearchError) -> Self {
        Self::Search(err)
    }
}

// ---------------------------------------------------------------------------
// Comby template translation (`internal/comby/translate.go`).
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
enum Term {
    Literal(String),
    Hole(String),
}

fn match_regexp_pattern() -> &'static Regex {
    static PATTERN: OnceLock<Regex> = OnceLock::new();
    PATTERN.get_or_init(|| Regex::new(r":\[(\w+)?~(.*)\]").expect("valid hole regexp"))
}

fn on_match_whitespace() -> &'static Regex {
    static PATTERN: OnceLock<Regex> = OnceLock::new();
    PATTERN.get_or_init(|| Regex::new(r"[\s]+").expect("valid whitespace regexp"))
}

/// Baseline `parseTemplate`: split a comby pattern into literals and holes.
fn parse_template(input: &str) -> Vec<Term> {
    let chars: Vec<char> = input.chars().collect();
    let mut index = 0usize;
    // Greater than zero while inside an opening hole, e.g. after `:[`.
    let mut open = 0usize;
    // Greater than zero inside a regexp character set inside a hole.
    let mut inside = 0usize;
    let mut token = String::new();
    let mut result = Vec::new();

    while index < chars.len() {
        let current = chars[index];
        index += 1;
        match current {
            ':' => {
                if open > 0 {
                    token.push(':');
                    continue;
                }
                if index < chars.len() {
                    if chars[index] == '[' {
                        index += 1;
                        open += 1;
                        if !token.is_empty() {
                            result.push(Term::Literal(std::mem::take(&mut token)));
                        }
                        token.push(':');
                        token.push('[');
                        continue;
                    }
                    token.push(':');
                    continue;
                }
                token.push(':');
            }
            '\\' => {
                if index < chars.len() && open > 0 {
                    let escaped = chars[index];
                    index += 1;
                    token.push('\\');
                    token.push(escaped);
                    continue;
                }
                token.push('\\');
            }
            '[' => {
                if open > 0 {
                    inside += 1;
                }
                token.push('[');
            }
            ']' => {
                if open > 0 && inside > 0 {
                    inside -= 1;
                    token.push(']');
                    continue;
                }
                if open > 0 {
                    open -= 1;
                    token.push(']');
                    result.push(Term::Hole(std::mem::take(&mut token)));
                    continue;
                }
                token.push(']');
            }
            other => token.push(other),
        }
    }
    if !token.is_empty() {
        result.push(Term::Literal(token));
    }
    result
}

/// Baseline `StructuralPatToRegexpQuery`: approximate the comby pattern with a
/// regexp that the unindexed candidate search (or zoekt) can evaluate.
pub fn structural_pat_to_regexp_query(pattern: &str, shortcircuit: bool) -> String {
    let mut pieces = Vec::new();
    for term in parse_template(pattern) {
        match term {
            Term::Literal(literal) => {
                let piece = regex::escape(&literal);
                pieces.push(
                    on_match_whitespace()
                        .replace_all(&piece, r"[\s]+")
                        .into_owned(),
                );
            }
            Term::Hole(hole) => {
                if let Some(captures) = match_regexp_pattern().captures(&hole) {
                    pieces.push(
                        captures
                            .get(2)
                            .map(|matched| matched.as_str().to_owned())
                            .unwrap_or_default(),
                    );
                }
            }
        }
    }

    if pieces.is_empty() {
        // Match anything.
        return r"(?:.|\s)*?".to_owned();
    }
    if shortcircuit {
        // Do not match across newlines of structural search pieces.
        format!("(?:{})", pieces.join(").*?(?:"))
    } else {
        format!("(?:{})", pieces.join(r")(?:.|\s)*?(?:"))
    }
}

// ---------------------------------------------------------------------------
// Matcher selection.
// ---------------------------------------------------------------------------

fn is_valid_matcher(extension: &str) -> bool {
    static PATTERN: OnceLock<Regex> = OnceLock::new();
    PATTERN
        .get_or_init(|| {
            Regex::new(concat!(
                r"\.(s|sh|bib|c|cs|css|dart|clj|elm|erl|ex|f|fsx|go|html|hs|java|js|json|jl|kt|",
                r"tex|lisp|nim|md|ml|org|pas|php|py|re|rb|rs|rst|scala|sql|swift|tex|txt|ts)$"
            ))
            .expect("valid matcher regexp")
        })
        .is_match(extension)
}

fn extension_to_matcher(extension: &str) -> String {
    if is_valid_matcher(extension) {
        extension.to_owned()
    } else {
        ".generic".to_owned()
    }
}

/// Baseline `lookupMatcher`. The comparison mirrors the Go switch exactly:
/// `strings.ToLower(language)` is compared against the case-sensitive literals,
/// so the `"laTeX"` and `"c#, csharp"` arms never match (`c#` falls back to
/// `.generic`, as in the baseline).
fn lookup_matcher(language: &str) -> String {
    match language.to_lowercase().as_str() {
        "assembly" | "asm" => ".s",
        "bash" => ".sh",
        "c" => ".c",
        "css" => ".css",
        "dart" => ".dart",
        "clojure" => ".clj",
        "elm" => ".elm",
        "erlang" => ".erl",
        "elixir" => ".ex",
        "fortran" => ".f",
        "f#" | "fsharp" => ".fsx",
        "go" => ".go",
        "html" => ".html",
        "haskell" => ".hs",
        "java" => ".java",
        "javascript" => ".js",
        "json" => ".json",
        "julia" => ".jl",
        "kotlin" => ".kt",
        "lisp" => ".lisp",
        "nim" => ".nim",
        "ocaml" => ".ml",
        "pascal" => ".pas",
        "php" => ".php",
        "python" => ".py",
        "reason" => ".re",
        "ruby" => ".rb",
        "rust" => ".rs",
        "scala" => ".scala",
        "sql" => ".sql",
        "swift" => ".swift",
        "text" => ".txt",
        "typescript" | "ts" => ".ts",
        "xml" => ".xml",
        _ => ".generic",
    }
    .to_owned()
}

/// Baseline `toMatcher`: an explicit language wins, otherwise infer from the
/// first candidate path's extension.
pub fn to_matcher(languages: &[String], extension_hint: &str) -> String {
    if let Some(language) = languages.first() {
        return lookup_matcher(language);
    }
    if !extension_hint.is_empty() {
        return extension_to_matcher(extension_hint);
    }
    ".generic".to_owned()
}

// ---------------------------------------------------------------------------
// Comby output.
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
struct CombyLocation {
    offset: i64,
    line: i64,
    column: i64,
}

#[derive(Debug, Deserialize)]
struct CombyRange {
    start: CombyLocation,
    end: CombyLocation,
}

#[derive(Debug, Deserialize)]
struct CombyMatch {
    range: CombyRange,
}

#[derive(Debug, Deserialize)]
struct CombyFileMatch {
    uri: String,
    matches: Vec<CombyMatch>,
}

fn convert_location(location: &CombyLocation) -> Location {
    // Comby returns 1-based line numbers and columns.
    Location {
        offset: i32::try_from(location.offset).unwrap_or(i32::MAX),
        line: i32::try_from(location.line - 1).unwrap_or(0),
        column: i32::try_from(location.column - 1).unwrap_or(0),
    }
}

/// Baseline `toFileMatch`: convert one comby result into chunk matches.
fn to_file_match(
    zip: &ZipFile,
    comby_match: &CombyFileMatch,
    context_lines: i32,
) -> Result<FileMatch, StructuralError> {
    let file_buf = zip
        .data_by_name(&comby_match.uri)?
        .ok_or_else(|| StructuralError::Comby("comby returned a path not in the archive".into()))?;

    let mut ranges = Vec::with_capacity(comby_match.matches.len());
    for found in &comby_match.matches {
        // Trust, but verify. A negative offset would also panic the chunk
        // bookkeeping, so reject it here.
        let (start, end) = (found.range.start.offset, found.range.end.offset);
        if start < 0 || end < 0 || start > file_buf.len() as i64 || end > file_buf.len() as i64 {
            return Err(StructuralError::Comby(
                "comby match range does not fit in file".to_owned(),
            ));
        }
        ranges.push(Range {
            start: convert_location(&found.range.start),
            end: convert_location(&found.range.end),
        });
    }

    let chunks = chunk_ranges(ranges, context_lines * 2);
    Ok(FileMatch {
        path: comby_match.uri.clone(),
        chunk_matches: chunks_to_matches(&file_buf, &chunks, context_lines),
        ..FileMatch::default()
    })
}

// ---------------------------------------------------------------------------
// Candidate filtering.
// ---------------------------------------------------------------------------

fn extract_query_atom(pattern_info: &PatternInfo) -> Result<PatternNode, StructuralError> {
    match &pattern_info.query {
        Some(QueryNode::Pattern(atom)) => Ok(atom.clone()),
        _ => Err(StructuralError::BadQuery(
            "structural search only supports leaf regex patterns".to_owned(),
        )),
    }
}

/// Baseline `regexSearchBatch`: collect the files whose content may contain the
/// structural pattern.
fn regex_search_batch(
    pattern_info: &PatternInfo,
    zip: &ZipFile,
    context_lines: i32,
    cancel: &CancellationToken,
) -> Result<Vec<FileMatch>, StructuralError> {
    let query = pattern_info
        .query
        .as_ref()
        .ok_or_else(|| StructuralError::BadQuery("unknown query node type <nil>".to_owned()))?;
    let match_tree = to_match_tree(query, pattern_info.is_case_sensitive)
        .map_err(|err| StructuralError::BadQuery(err.to_string()))?;
    let lang_matcher = to_lang_matcher(pattern_info);
    let path_matcher =
        to_path_matcher(pattern_info).map_err(|err| StructuralError::BadQuery(err.to_string()))?;

    let collected = Arc::new(Mutex::new(Vec::new()));
    let sink = Arc::clone(&collected);
    // A child token so the collector's own limit does not cancel the request.
    let collector_cancel = cancel.child_token();
    let limit = if pattern_info.limit <= 0 {
        i32::MAX
    } else {
        pattern_info.limit
    };
    let sender = LimitedStream::new(
        limit,
        Box::new(move |file_match| {
            sink.lock().expect("collector").push(file_match);
        }),
        collector_cancel.clone(),
    );

    let search = RegexSearch {
        match_tree: &match_tree,
        path_matcher: &path_matcher,
        lang_matcher: &lang_matcher,
        zip,
        pattern_matches_content: true,
        pattern_matches_path: false,
        context_lines,
    };
    search.run(&collector_cancel, &sender)?;

    let matches = collected.lock().expect("collector").clone();
    Ok(matches)
}

// ---------------------------------------------------------------------------
// Filtered structural search.
// ---------------------------------------------------------------------------

/// Baseline `filteredStructuralSearch`: narrow the archive with a regexp pass,
/// then run comby against the zip for the surviving paths.
pub fn filtered_structural_search(
    zip_path: &Path,
    zip: &ZipFile,
    pattern_info: &PatternInfo,
    comby_path: &Path,
    context_lines: i32,
    sender: &LimitedStream,
) -> Result<(), StructuralError> {
    let atom = extract_query_atom(pattern_info)?;

    // Make a copy of the pattern info to modify it to work for a regex search.
    let mut regexp_info = pattern_info.clone();
    regexp_info.is_structural = false;
    regexp_info.pattern_matches_content = true;
    regexp_info.pattern_matches_path = false;
    regexp_info.query = Some(QueryNode::Pattern(PatternNode {
        value: structural_pat_to_regexp_query(&atom.value, false),
        is_negated: atom.is_negated,
        is_regexp: true,
        boost: atom.boost,
    }));

    let cancel = sender.cancel_token();
    let file_matches = regex_search_batch(&regexp_info, zip, context_lines, &cancel)?;
    if file_matches.is_empty() {
        return Ok(());
    }

    let matched_paths: Vec<String> = file_matches.into_iter().map(|m| m.path).collect();
    let extension_hint = matched_paths
        .first()
        .and_then(|path| Path::new(path).extension())
        .map(|extension| format!(".{}", extension.to_string_lossy()))
        .unwrap_or_default();
    let matcher = to_matcher(&pattern_info.languages, &extension_hint);

    run_comby_against_zip(
        comby_path,
        &matcher,
        &atom.value,
        &pattern_info.comby_rule,
        &matched_paths,
        zip_path,
        zip,
        context_lines,
        sender,
    )
}

/// Baseline `comby.rawArgs` for `MatchOnly` with a zip input.
fn comby_args(
    matcher: &str,
    pattern: &str,
    rule: &str,
    file_patterns: &[String],
    zip_path: &Path,
) -> Vec<std::ffi::OsString> {
    let mut args: Vec<std::ffi::OsString> = Vec::new();
    args.push(pattern.into());
    // Rewrite template, empty for match-only.
    args.push("".into());
    if !rule.is_empty() {
        args.push("-rule".into());
        args.push(rule.into());
    }
    if !file_patterns.is_empty() {
        args.push("-f".into());
        args.push(file_patterns.join(",").into());
    }
    args.push("-json-lines".into());
    args.push("-match-newline-at-toplevel".into());
    args.push("-match-only".into());
    args.push("-jobs".into());
    args.push(NUM_WORKERS.to_string().into());
    if !matcher.is_empty() {
        args.push("-matcher".into());
        args.push(matcher.into());
    }
    args.push("-zip".into());
    args.push(zip_path.as_os_str().to_owned());
    args
}

fn trim_line_end(line: &[u8]) -> &[u8] {
    let mut end = line.len();
    while end > 0 && (line[end - 1] == b'\n' || line[end - 1] == b'\r') {
        end -= 1;
    }
    &line[..end]
}

/// Read comby's JSON lines and forward each match to `sender`.
fn scan_comby_output(
    reader: &mut impl BufRead,
    zip: &ZipFile,
    context_lines: i32,
    sender: &LimitedStream,
) -> Result<(), StructuralError> {
    let mut line = Vec::new();
    loop {
        line.clear();
        // `read_until` returns 0 at EOF.
        if reader.read_until(b'\n', &mut line)? == 0 {
            return Ok(());
        }
        let trimmed = trim_line_end(&line);
        if trimmed.is_empty() {
            continue;
        }
        let comby_match: CombyFileMatch = serde_json::from_slice(trimmed)
            .map_err(|err| StructuralError::Comby(format!("decode comby output: {err}")))?;
        sender.send(to_file_match(zip, &comby_match, context_lines)?);
    }
}

/// Baseline `runCombyAgainstZip`: stream comby's JSON lines, convert each to a
/// [`FileMatch`] and send it. The process is killed when the request is
/// cancelled (limit hit, client disconnect, shutdown).
#[allow(clippy::too_many_arguments)]
fn run_comby_against_zip(
    comby_path: &Path,
    matcher: &str,
    pattern: &str,
    rule: &str,
    file_patterns: &[String],
    zip_path: &Path,
    zip: &ZipFile,
    context_lines: i32,
    sender: &LimitedStream,
) -> Result<(), StructuralError> {
    let args = comby_args(matcher, pattern, rule, file_patterns, zip_path);
    let mut child = Command::new(comby_path)
        .args(&args)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|err| {
            if err.kind() == std::io::ErrorKind::NotFound {
                StructuralError::Comby("comby is not installed".to_owned())
            } else {
                StructuralError::Comby(format!("failed to start comby: {err}"))
            }
        })?;

    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| StructuralError::Comby("comby stdout is not piped".to_owned()))?;
    let mut stderr = child
        .stderr
        .take()
        .ok_or_else(|| StructuralError::Comby("comby stderr is not piped".to_owned()))?;

    let child = Arc::new(Mutex::new(child));
    let done = Arc::new(AtomicBool::new(false));
    let cancel = sender.cancel_token();

    let (scan_result, status, stderr_output) = std::thread::scope(|scope| {
        let stderr_thread = scope.spawn(move || {
            let mut buffer = Vec::new();
            let _ = stderr.read_to_end(&mut buffer);
            buffer
        });

        {
            let done = Arc::clone(&done);
            let child = Arc::clone(&child);
            let cancel = cancel.clone();
            scope.spawn(move || {
                while !done.load(Ordering::SeqCst) {
                    if cancel.is_cancelled() {
                        let _ = child.lock().expect("comby child").kill();
                        return;
                    }
                    std::thread::sleep(Duration::from_millis(50));
                }
            });
        }

        let mut reader = BufReader::new(stdout);
        let scan_result = scan_comby_output(&mut reader, zip, context_lines, sender);

        done.store(true, Ordering::SeqCst);
        if scan_result.is_err() {
            let _ = child.lock().expect("comby child").kill();
        }
        let status = child.lock().expect("comby child").wait();
        let stderr_output = stderr_thread.join().unwrap_or_default();
        (scan_result, status, stderr_output)
    });

    scan_result?;
    match status {
        Ok(status) if status.success() => Ok(()),
        Ok(status) => {
            let stderr = String::from_utf8_lossy(&stderr_output);
            if stderr.is_empty() {
                Err(StructuralError::Comby(format!(
                    "failed to wait for executing comby command: {status}"
                )))
            } else {
                Err(StructuralError::Comby(format!(
                    "failed to execute comby command: stderr: {stderr:?}"
                )))
            }
        }
        Err(err) => Err(StructuralError::Comby(format!(
            "failed to wait for executing comby command: {err}"
        ))),
    }
}

/// A comby executable path, defaulting to the baseline's `comby` on `PATH`.
pub fn default_comby_path() -> PathBuf {
    PathBuf::from("comby")
}

#[cfg(test)]
mod tests {
    use std::io::Write;
    use std::os::unix::fs::PermissionsExt;

    use zip::CompressionMethod;

    use super::*;
    use crate::zipcache::ZipCache;

    #[test]
    fn parses_comby_templates() {
        assert_eq!(
            parse_template("foo(:[x])"),
            vec![
                Term::Literal("foo(".to_owned()),
                Term::Hole(":[x]".to_owned()),
                Term::Literal(")".to_owned()),
            ]
        );
        assert_eq!(
            parse_template(":[x~[a-z]+]"),
            vec![Term::Hole(":[x~[a-z]+]".to_owned())]
        );
        assert_eq!(
            parse_template(r"a\b"),
            vec![Term::Literal(r"a\b".to_owned())]
        );
    }

    #[test]
    fn translates_structural_patterns_to_regexps() {
        // Port of the baseline `translate_test.go` cases.
        let cases: &[(&str, bool, &str)] = &[
            ("", false, r"(?:.|\s)*?"),
            ("foo", false, "(?:foo)"),
            ("foo bar", false, r"(?:foo[\s]+bar)"),
            ("foo(:[x])", false, r"(?:foo\()(?:.|\s)*?(?:\))"),
            ("foo(:[x])", true, r"(?:foo\().*?(?:\))"),
            (
                "ParseInt(:[args]) if err != nil",
                false,
                r"(?:ParseInt\()(?:.|\s)*?(?:\)[\s]+if[\s]+err[\s]+!=[\s]+nil)",
            ),
            (":[x~[a-z]+]", false, "(?:[a-z]+)"),
        ];
        for (pattern, shortcircuit, expected) in cases {
            assert_eq!(
                structural_pat_to_regexp_query(pattern, *shortcircuit),
                *expected,
                "pattern {pattern:?}"
            );
        }
    }

    #[test]
    fn selects_comby_matchers() {
        assert_eq!(to_matcher(&["Go".to_owned()], ""), ".go");
        assert_eq!(to_matcher(&["c#".to_owned()], ""), ".generic");
        assert_eq!(to_matcher(&[], ".rs"), ".rs");
        assert_eq!(to_matcher(&[], ".unknown"), ".generic");
        assert_eq!(to_matcher(&[".".to_owned()], ""), ".generic");
    }

    fn write_zip(path: &Path, files: &[(&str, &[u8])]) {
        let file = std::fs::File::create(path).expect("create");
        let mut writer = zip::ZipWriter::new(file);
        for (name, data) in files {
            writer
                .start_file(
                    *name,
                    zip::write::SimpleFileOptions::default()
                        .compression_method(CompressionMethod::Stored),
                )
                .expect("start file");
            writer.write_all(data).expect("write");
        }
        writer.finish().expect("finish");
    }

    fn comby_stub(dir: &Path, output: &str) -> PathBuf {
        let path = dir.join("comby");
        let mut file = std::fs::File::create(&path).expect("stub");
        writeln!(file, "#!/bin/sh").expect("write");
        writeln!(file, "cat <<'EOF'").expect("write");
        write!(file, "{output}").expect("write");
        writeln!(file).expect("write");
        writeln!(file, "EOF").expect("write");
        file.set_permissions(std::fs::Permissions::from_mode(0o755))
            .expect("chmod");
        path
    }

    #[test]
    fn filters_candidates_and_emits_comby_matches() {
        let dir = tempfile::tempdir().expect("tempdir");
        let zip_path = dir.path().join("archive.zip");
        let content = b"fn main() { foo(bar); }\n";
        write_zip(
            &zip_path,
            &[("a.go", content.as_slice()), ("b.go", b"nothing\n")],
        );
        let zip = ZipCache::new().get(&zip_path).expect("zip");

        let comby = comby_stub(
            dir.path(),
            r#"{"uri":"a.go","matches":[{"range":{"start":{"offset":12,"line":1,"column":13},"end":{"offset":20,"line":1,"column":21}},"matched":"foo(bar)"}]}"#,
        );

        let pattern_info = PatternInfo {
            query: Some(QueryNode::Pattern(PatternNode {
                value: "foo(:[x])".to_owned(),
                is_negated: false,
                is_regexp: false,
                boost: false,
            })),
            pattern_matches_content: true,
            limit: 100,
            include_langs: vec!["Go".to_owned()],
            ..PatternInfo::default()
        };

        let collected = Arc::new(Mutex::new(Vec::new()));
        let sink = Arc::clone(&collected);
        let cancel = CancellationToken::new();
        let sender = LimitedStream::new(
            100,
            Box::new(move |file_match| {
                sink.lock().expect("collector").push(file_match);
            }),
            cancel.clone(),
        );

        filtered_structural_search(&zip_path, &zip, &pattern_info, &comby, 0, &sender)
            .expect("structural search");

        let matches = collected.lock().expect("collector");
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].path, "a.go");
        assert_eq!(matches[0].match_count(), 1);
        let chunk = &matches[0].chunk_matches[0];
        assert_eq!(chunk.content, "fn main() { foo(bar); }\n");
        assert_eq!(chunk.ranges[0].start.offset, 12);
        assert_eq!(chunk.ranges[0].start.column, 12);
        assert_eq!(chunk.ranges[0].end.offset, 20);
    }

    #[test]
    fn rejects_missing_comby_binary() {
        let dir = tempfile::tempdir().expect("tempdir");
        let zip_path = dir.path().join("archive.zip");
        write_zip(&zip_path, &[("a.go", b"foo(bar)\n")]);
        let zip = ZipCache::new().get(&zip_path).expect("zip");

        let pattern_info = PatternInfo {
            query: Some(QueryNode::Pattern(PatternNode {
                value: "foo(:[x])".to_owned(),
                is_negated: false,
                is_regexp: false,
                boost: false,
            })),
            pattern_matches_content: true,
            limit: 100,
            ..PatternInfo::default()
        };
        let cancel = CancellationToken::new();
        let sender = LimitedStream::new(100, Box::new(|_| {}), cancel);

        let error = filtered_structural_search(
            &zip_path,
            &zip,
            &pattern_info,
            &dir.path().join("missing-comby"),
            0,
            &sender,
        )
        .expect_err("missing comby");
        assert_eq!(error.to_string(), "comby is not installed");
    }
}
