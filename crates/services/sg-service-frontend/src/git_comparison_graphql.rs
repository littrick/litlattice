#![forbid(unsafe_code)]

//! `Repository.comparison`, the remaining `FileDiff` fields and the preview
//! comparison (M9.8jd).
//!
//! Baseline: `cmd/frontend/graphqlbackend/{repository_comparison.go,
//! preview_repository_comparison.go,git_revision.go}` and the offline subset of
//! `cmd/frontend/internal/highlight` (`generatePlainTable` /
//! `SplitHighlightedLines`). The syntect-backed `HTML_HIGHLIGHT` format is a
//! seam (there is no gosyntect client in Rust); `HTML_PLAINTEXT` is reproduced
//! exactly.
//!
//! Like M9.8iz, every gitserver read is prefetched into [`GitPrefetch`] because
//! `ObjectValue::field` is synchronous. The shared `ComparisonData` and
//! file-diff resolvers stay in `git_commit_extra.rs`; this module adds the
//! repository-level comparison, file content/highlight prefetch and the preview
//! (patch-based) comparison.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use async_trait::async_trait;

use sg_git::{FileStat, DEV_NULL_SHA};
use sg_graphql::exec::{
    Actor, Arguments, ExecError, FieldValue, InputValue, ObjectValue, RootField,
};
use sg_graphql::json::Json;
use sg_service_gitserver::diff::{parse_multi_file_diff, FileDiff, Hunk};

use crate::git_commit_extra::{
    compute_comparison_commits, compute_file_diffs, ComparisonData, GitCommitExtraSource,
};
use crate::git_graphql::{load_revision, GitObjectSource, GitPrefetch, RevisionLookup};
use crate::git_tree_entry_graphql::GitTreeEntryObject;
use crate::search_stream::convert::MinimalRepo;

// ---------------------------------------------------------------------------
// Prefetch data
// ---------------------------------------------------------------------------

/// `(repo id, base revspec, head revspec)`.
type RepositoryComparisonKey = (i32, String, String);

/// The arguments that identify one highlighted file snapshot.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct HighlightArgsKey {
    pub start_line: Option<i32>,
    pub end_line: Option<i32>,
    pub disable_timeout: bool,
    pub highlight_long_lines: bool,
    pub format: String,
}

/// `(repo id, commit oid, path, args)` → highlighted lines.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct HighlightKey {
    pub repo_id: i32,
    pub oid: String,
    pub path: String,
    pub args: HighlightArgsKey,
}

/// Portable subset of `scip.Occurrence` (`sourcegraph/scip`'s
/// `bindings/go/scip/scip.pb.go`). Only the fields the highlighter consumes
/// are modeled. `syntax_kind` holds the numeric `scip.SyntaxKind` enum value.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LsifOccurrence {
    /// Half-open `[startLine, startChar, endLine, endChar]` or
    /// `[startLine, startChar, endChar]`.
    pub range: Vec<i32>,
    pub symbol: Option<String>,
    pub symbol_roles: i32,
    pub override_documentation: Vec<String>,
    pub syntax_kind: i32,
}

/// Portable subset of `scip.Document`. `internal/highlight` consumes only
/// `Occurrences` for HTML/line-range rendering; `Language`/`RelativePath` are
/// carried for `HighlightedFile.lsif` (protobuf JSON) fidelity.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LsifDocument {
    pub language: Option<String>,
    pub relative_path: Option<String>,
    pub occurrences: Vec<LsifOccurrence>,
}

/// `highlight.CodeAsLines`' result: the per-line HTML and the aborted flag.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct HighlightedFile {
    pub lines: Vec<String>,
    pub aborted: bool,
    /// M9.37c: the full HTML table (`HighlightedCode.HTML()`), needed by
    /// `GitBlob.highlight`'s `html`/`lineRanges` fields. `None` when the
    /// producing path only has per-line HTML.
    pub html: Option<String>,
    /// M9.47d: `HighlightedCode.code`, the source text with at most one final
    /// newline trimmed. Required to render a SCIP document (`DocumentToHTML` /
    /// `LinesForRanges`) when `lsif` is present.
    pub code: Option<String>,
    /// M9.47d: `HighlightedCode.document`, the `scip.Document` produced by a
    /// tree-sitter/`JSON_SCIP` request. `None` for every syntect/HTML/plain
    /// response, which is exactly when Go's `HighlightedFileResolver.LineRanges`
    /// takes the HTML branch and `lsif` marshals to `"{}"`.
    pub lsif: Option<LsifDocument>,
}

/// Comparison-specific prefetch state, attached to [`GitPrefetch`].
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ComparisonPrefetch {
    pub repository_comparisons: HashMap<RepositoryComparisonKey, Result<ComparisonData, String>>,
    pub highlights: HashMap<HighlightKey, Result<HighlightedFile, String>>,
}

// ---------------------------------------------------------------------------
// Highlight seam + offline (`HTML_PLAINTEXT`) subset
// ---------------------------------------------------------------------------

/// One `highlight.CodeAsLines` request (`highlight.Params`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HighlightRequest {
    pub content: Vec<u8>,
    pub filepath: String,
    pub disable_timeout: bool,
    pub highlight_long_lines: bool,
    pub format: String,
    pub start_line: Option<i32>,
    pub end_line: Option<i32>,
    pub keep_final_newline: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HighlightError {
    /// `highlight.ErrBinary`; the file is rendered without highlighted lines.
    Binary,
    Message(String),
}

/// Syntax-highlighting seam. The production build has no gosyntect client, so
/// [`GitObjectSource::highlighter`] stays `None` and [`offline_highlight`] is
/// used. Tests or a future transport can inject another implementation.
#[async_trait]
pub trait HighlightSource: Send + Sync {
    async fn highlight(
        &self,
        request: &HighlightRequest,
    ) -> Result<HighlightedFile, HighlightError>;
}

/// `internal/binary.IsBinary`, approximated: the baseline also consults
/// `http.DetectContentType` for non-UTF-8 text encodings. Corpus content is
/// UTF-8; the difference is recorded as an erratum.
pub fn is_binary(content: &[u8]) -> bool {
    std::str::from_utf8(content).is_err()
}

/// Go `html.Render`'s text-node escaping (`escapedChars = "&'<>\"\r"`).
fn go_html_escape(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    for ch in text.chars() {
        match ch {
            '&' => out.push_str("&amp;"),
            '\'' => out.push_str("&#39;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&#34;"),
            '\r' => out.push_str("&#13;"),
            ch => out.push(ch),
        }
    }
    out
}

/// The offline subset of `highlight.CodeAsLines`:
/// `generatePlainTable` followed by `SplitHighlightedLines(false)`, which keeps
/// one `<span>TEXT</span>` per line. `HTML_PLAINTEXT` is the only format that
/// does not call the syntect service; it always reports `aborted=true`.
///
/// M9.35d: `start_line`/`end_line` are applied with `GitTreeEntry.Content`'s
/// `pageContent` *before* highlighting, matching `fileDiffHighlighter`
/// (`repository_comparison.go:491-515`). The binary check also runs on the
/// sliced bytes, exactly like `highlight.Code` receiving the paged content.
pub fn offline_highlight(request: &HighlightRequest) -> Result<HighlightedFile, HighlightError> {
    let content = crate::git_tree_entry_graphql::page_content(
        &request.content,
        request.start_line,
        request.end_line,
    );
    if is_binary(content) {
        return Err(HighlightError::Binary);
    }
    if request.format != "HTML_PLAINTEXT" {
        return Err(HighlightError::Message(format!(
            "syntax highlighting for format {} is not available offline",
            request.format
        )));
    }
    let mut code = String::from_utf8_lossy(content).into_owned();
    if !request.keep_final_newline {
        if let Some(stripped) = code.strip_suffix('\n') {
            code = stripped.to_owned();
        }
    }
    let mut lines = Vec::new();
    for line in code.split('\n') {
        let line = line.strip_suffix('\r').unwrap_or(line);
        let line = if line.is_empty() { "\n" } else { line };
        lines.push(format!("<span>{}</span>", go_html_escape(line)));
    }
    // `generatePlainTable`: `<table>` + one `<tr>` per line with the `<span>`
    // directly under the code cell (no `<div>`).
    let mut html = String::from("<table>");
    for (row, line) in lines.iter().enumerate() {
        html.push_str("<tr><td class=\"line\" data-line=\"");
        html.push_str(&(row + 1).to_string());
        html.push_str("\"></td><td class=\"code\">");
        html.push_str(line);
        html.push_str("</td></tr>");
    }
    html.push_str("</table>");
    Ok(HighlightedFile {
        lines,
        aborted: true,
        html: Some(html),
        code: Some(code),
        lsif: None,
    })
}

pub(crate) async fn compute_highlight(
    source: &dyn GitObjectSource,
    request: &HighlightRequest,
) -> Result<HighlightedFile, HighlightError> {
    match source.highlighter() {
        Some(highlighter) => highlighter.highlight(request).await,
        None => offline_highlight(request),
    }
}

// ---------------------------------------------------------------------------
// Production highlighter adapter (M9.36d)
// ---------------------------------------------------------------------------

/// The production [`HighlightSource`]: `highlight.CodeAsLines` backed by the
/// M9.23c syntect `CodeHighlighter` (`highlight.Code`). `fileDiffHighlighter`
/// and `GitBlob.highlight` both go through `CodeAsLines`, so this adapter is
/// their shared implementation.
///
/// Order matches the baseline: `Content(startLine,endLine)` is applied first
/// ([`crate::git_tree_entry_graphql::page_content`]), then the sliced bytes are
/// highlighted. Binary detection therefore runs on the sliced content, exactly
/// like `fileDiffHighlighter` calling `highlight.Code` with the paged bytes.
pub struct CodeHighlighterSource {
    highlighter: Arc<dyn crate::graphql_inventory_c::CodeHighlighter>,
}

impl CodeHighlighterSource {
    pub fn new(highlighter: Arc<dyn crate::graphql_inventory_c::CodeHighlighter>) -> Self {
        Self { highlighter }
    }

    /// Production assembly: `SRC_SYNTECT_SERVER` (the same client the
    /// `highlightCode` resolver uses).
    pub fn production() -> Self {
        Self::new(Arc::new(
            crate::markdown_graphql::SyntectCodeHighlighter::from_env(),
        ))
    }
}

#[async_trait]
impl HighlightSource for CodeHighlighterSource {
    async fn highlight(
        &self,
        request: &HighlightRequest,
    ) -> Result<HighlightedFile, HighlightError> {
        let content = crate::git_tree_entry_graphql::page_content(
            &request.content,
            request.start_line,
            request.end_line,
        );
        if is_binary(content) {
            return Err(HighlightError::Binary);
        }

        // `HTML_PLAINTEXT` never reaches syntect in Go (`unhighlightedCode`).
        if request.format == "HTML_PLAINTEXT" {
            return offline_highlight(request);
        }
        if request.format != "HTML_HIGHLIGHT" {
            return Err(HighlightError::Message(format!(
                "syntax highlighting for format {} is not available",
                request.format
            )));
        }

        let code = String::from_utf8_lossy(content).into_owned();
        let normalized = crate::markdown_graphql::normalize_filepath(&request.filepath);
        if crate::markdown_graphql::detect_language(&normalized, &code).is_empty() {
            // Unknown language: the syntect server renders a plain-text table.
            // Reproduce that offline (`HTML_PLAINTEXT`) instead of sending an
            // empty `filetype`.
            let mut plain = request.clone();
            plain.format = "HTML_PLAINTEXT".to_owned();
            return offline_highlight(&plain);
        }

        // `SyntectCodeHighlighter` (like `highlight.Code`) strips one final
        // newline unless told otherwise; re-add one so `KeepFinalNewline`
        // sends the exact bytes to the server.
        let input = if request.keep_final_newline {
            format!("{code}\n")
        } else {
            code.clone()
        };
        let (html, aborted) = self
            .highlighter
            .highlight_with_options(
                &input,
                &request.filepath,
                request.disable_timeout,
                request.highlight_long_lines,
            )
            .await
            .map_err(HighlightError::Message)?;
        Ok(HighlightedFile {
            lines: split_highlighted_lines(&html).map_err(HighlightError::Message)?,
            aborted,
            html: Some(html),
            code: Some(code),
            lsif: None,
        })
    }
}

/// `HighlightedCode.SplitHighlightedLines(false)`: one entry per table row,
/// the rendered first child of the row's code cell (`tr > td.code > div`).
///
/// The ported stand-ins emit canonical HTML (Go `html.Render` round-trips to
/// itself), so extracting the raw `<div>…</div>` / `<span>…</span>` substring
/// matches the baseline's parse-and-render output.
pub(crate) fn split_highlighted_lines(html: &str) -> Result<Vec<String>, String> {
    let mut lines = Vec::new();
    let mut position = 0;
    while let Some(row_start) = find_from(html, "<tr", position) {
        let Some(row_end) = find_from(html, "</tr>", row_start) else {
            break;
        };
        let row = &html[row_start..row_end];
        let td_start = rfind_td(row).ok_or("expected <td> in highlighted table row")?;
        let cell_start = find_from(row, ">", td_start).ok_or("unterminated <td>")? + 1;
        let cell_end = find_from(row, "</td>", cell_start).ok_or("unterminated <td>")?;
        lines.push(first_child(&row[cell_start..cell_end])?);
        position = row_end + "</tr>".len();
    }
    Ok(lines)
}

/// `HighlightedCode.SplitHighlightedLines(true)`: one rendered `<tr>…</tr>`
/// per table row (`SplitLineRanges` uses these).
pub(crate) fn split_highlighted_rows(html: &str) -> Vec<String> {
    let mut rows = Vec::new();
    let mut position = 0;
    while let Some(row_start) = find_from(html, "<tr", position) {
        let Some(row_end) = find_from(html, "</tr>", row_start) else {
            break;
        };
        let end = row_end + "</tr>".len();
        rows.push(html[row_start..end].to_owned());
        position = end;
    }
    rows
}

fn find_from(haystack: &str, needle: &str, from: usize) -> Option<usize> {
    haystack.get(from..)?.find(needle).map(|offset| from + offset)
}

fn rfind_td(row: &str) -> Option<usize> {
    let mut found = None;
    let mut position = 0;
    while let Some(start) = find_from(row, "<td", position) {
        found = Some(start);
        position = start + 3;
    }
    found
}

/// The first child node of a code cell, re-serialized as the baseline's
/// `html.Render` would (canonical input is preserved verbatim).
fn first_child(inner: &str) -> Result<String, String> {
    let inner = inner.trim_start();
    if inner.is_empty() {
        return Ok(String::new());
    }
    if !inner.starts_with('<') {
        let end = inner.find('<').unwrap_or(inner.len());
        return Ok(inner[..end].to_string());
    }
    let name_end = inner[1..]
        .find(|ch: char| ch.is_whitespace() || ch == '>' || ch == '/')
        .map(|offset| offset + 1)
        .ok_or("unterminated element")?;
    let name = &inner[1..name_end];
    let open_end = find_from(inner, ">", name_end).ok_or("unterminated element")?;
    let close = format!("</{name}>");
    let close_at =
        find_from(inner, &close, open_end).ok_or_else(|| format!("missing {close}"))?;
    Ok(inner[..close_at + close.len()].to_string())
}

// ---------------------------------------------------------------------------
// SCIP document consumption (M9.47d)
// ---------------------------------------------------------------------------
//
// Portable port of `cmd/frontend/internal/highlight/html.go`'s
// `DocumentToHTML`, `DocumentToSplitHTML`'s row builder and
// `HighlightedCode.LinesForRanges` (`highlight.go:202`). The producer
// (tree-sitter queries in syntect_server) is not portable; a SCIP document is
// injected through `HighlightSource` (tests / a future transport).

/// `scip.SyntaxKind_name` (`bindings/go/scip/scip.pb.go:367-407`). Returns
/// `None` for `UnspecifiedSyntaxKind` (no class) and `Some("")` for unknown
/// enum values, matching Go's map lookup in `appendTextToNode`.
fn syntax_kind_class(kind: i32) -> Option<&'static str> {
    if kind == 0 {
        return None;
    }
    Some(match kind {
        1 => "Comment",
        2 => "PunctuationDelimiter",
        3 => "PunctuationBracket",
        4 => "Keyword",
        5 => "IdentifierOperator",
        6 => "Identifier",
        7 => "IdentifierBuiltin",
        8 => "IdentifierNull",
        9 => "IdentifierConstant",
        10 => "IdentifierMutableGlobal",
        11 => "IdentifierParameter",
        12 => "IdentifierLocal",
        13 => "IdentifierShadowed",
        14 => "IdentifierNamespace",
        15 => "IdentifierFunction",
        16 => "IdentifierFunctionDefinition",
        17 => "IdentifierMacro",
        18 => "IdentifierMacroDefinition",
        19 => "IdentifierType",
        20 => "IdentifierBuiltinType",
        21 => "IdentifierAttribute",
        22 => "RegexEscape",
        23 => "RegexRepeated",
        24 => "RegexWildcard",
        25 => "RegexDelimiter",
        26 => "RegexJoin",
        27 => "StringLiteral",
        28 => "StringLiteralEscape",
        29 => "StringLiteralSpecial",
        30 => "StringLiteralKey",
        31 => "CharacterLiteral",
        32 => "NumericLiteral",
        33 => "BooleanLiteral",
        34 => "Tag",
        35 => "TagAttribute",
        36 => "TagDelimiter",
        _ => "",
    })
}

/// `normalizeSCIPRange` (`html.go:320`): a three-element range is single-line.
fn normalize_scip_range(range: &[i32]) -> (i32, i32, i32, i32) {
    match range.len() {
        3 => (range[0], range[1], range[0], range[2]),
        n if n >= 4 => (range[0], range[1], range[2], range[3]),
        _ => (0, 0, 0, 0),
    }
}

/// `safeSlice` (`html.go:83`): rune-indexed, truncating at the line end.
fn safe_slice(text: &[char], start: i32, finish: i32) -> String {
    if start > finish {
        return String::new();
    }
    let start = start.max(0) as usize;
    let finish = finish.max(0) as usize;
    if start > text.len() {
        return String::new();
    }
    if finish > text.len() {
        return text[start..].iter().collect();
    }
    text[start..finish].iter().collect()
}

/// `htmlManager` (`html.go:112`). Rows are accumulated as
/// `(line number, [(syntax kind, text)])`; rendering is deferred so a row can
/// be added mid-occurrence exactly like `newHtmlRow`/`addText`.
struct ScipHtmlManager<'a> {
    lines: Vec<Vec<char>>,
    occurrences: &'a [LsifOccurrence],
    row: i32,
    occ_idx: usize,
    valid_lines: Option<HashSet<i32>>,
    rows: Vec<(i32, Vec<(i32, String)>)>,
}

impl<'a> ScipHtmlManager<'a> {
    fn new(
        code: &str,
        document: &'a LsifDocument,
        valid_lines: Option<HashSet<i32>>,
    ) -> Self {
        let lines = code
            .split('\n')
            .map(|line| {
                let line = line.strip_suffix('\r').unwrap_or(line);
                // Important for e.g. selecting whitespace in the produced table.
                let line = if line.is_empty() { "\n" } else { line };
                line.chars().collect()
            })
            .collect();
        Self {
            lines,
            occurrences: &document.occurrences,
            row: 0,
            occ_idx: 0,
            valid_lines,
            rows: Vec::new(),
        }
    }

    fn process(&mut self) {
        let row_count = self.lines.len() as i32;
        while self.row < row_count {
            self.process_row();
            self.next_row();
        }
    }

    fn valid_row(&self, row: i32) -> bool {
        self.valid_lines
            .as_ref()
            .map_or(true, |valid| valid.contains(&row))
    }

    fn next_row(&mut self) {
        self.row += 1;
    }

    fn current_line(&self) -> Vec<char> {
        self.lines
            .get(self.row as usize)
            .cloned()
            .unwrap_or_default()
    }

    fn add_row(&mut self, row: i32) {
        self.rows.push((row, Vec::new()));
    }

    fn add_text(&mut self, kind: i32, text: String) {
        if text.is_empty() {
            return;
        }
        if let Some(last) = self.rows.last_mut() {
            last.1.push((kind, text));
        }
    }

    fn process_row(&mut self) {
        let row = self.row;
        if !self.valid_row(row) {
            return;
        }
        self.add_row(row);

        let mut line_character: i32 = 0;
        while self.occ_idx < self.occurrences.len()
            && self.occurrences[self.occ_idx]
                .range
                .first()
                .copied()
                .unwrap_or(i32::MAX)
                < row + 1
        {
            let occ = self.occurrences[self.occ_idx].clone();
            self.occ_idx += 1;
            line_character = self.process_one_occ(&occ, line_character);
        }

        let line = self.current_line();
        let len = line.len() as i32;
        if line_character != len {
            self.add_text(0, safe_slice(&line, line_character, len));
        }
    }

    fn process_one_occ(&mut self, occ: &LsifOccurrence, line_character: i32) -> i32 {
        let (start_row, start_character, end_row, end_character) =
            normalize_scip_range(&occ.range);

        // Skip occurrences that end before the current row.
        if end_row < self.row {
            return 0;
        }

        if start_row == self.row && line_character != start_character {
            let current_line = self.current_line();
            self.add_text(
                0,
                safe_slice(&current_line, line_character, start_character),
            );
        }

        if start_row == end_row {
            self.process_single_line_occ(occ, start_row, start_character, end_character);
        } else {
            self.process_multi_line_occ(occ, start_row, start_character, end_row, end_character);
        }

        end_character
    }

    fn process_single_line_occ(
        &mut self,
        occ: &LsifOccurrence,
        start_row: i32,
        start_character: i32,
        end_character: i32,
    ) {
        if start_row != self.row {
            return;
        }
        let line = self.lines[start_row as usize].clone();
        self.add_text(
            occ.syntax_kind,
            safe_slice(&line, start_character, end_character),
        );
    }

    fn process_multi_line_occ(
        &mut self,
        occ: &LsifOccurrence,
        start_row: i32,
        start_character: i32,
        end_row: i32,
        end_character: i32,
    ) {
        let max_row = self.lines.len() as i32;

        if self.row == start_row {
            let line = self.current_line();
            let len = line.len() as i32;
            self.add_text(occ.syntax_kind, safe_slice(&line, start_character, len));
            self.next_row();
            self.add_row(self.row);
        }

        while self.row < end_row && self.row < max_row {
            let line = self.current_line();
            let text: String = line.iter().collect();
            self.add_text(occ.syntax_kind, text);
            self.next_row();
            if self.row >= max_row {
                break;
            }
            self.add_row(self.row);
        }

        if self.row == end_row && self.row < max_row {
            let line = self.current_line();
            self.add_text(occ.syntax_kind, safe_slice(&line, 0, end_character));
        }
    }
}

/// Render one `<tr>` the way Go's `html.Render(newHtmlRow(...))` does.
fn render_scip_row(row: i32, segments: &[(i32, String)]) -> String {
    let mut out = String::from("<tr>");
    out.push_str("<td class=\"line\" data-line=\"");
    out.push_str(&(row + 1).to_string());
    out.push_str("\"></td><td class=\"code\">");
    for (kind, text) in segments {
        match syntax_kind_class(*kind) {
            Some(name) => {
                out.push_str("<span class=\"hl-typed-");
                out.push_str(name);
                out.push_str("\">");
            }
            None => out.push_str("<span>"),
        }
        out.push_str(&go_html_escape(text));
        out.push_str("</span>");
    }
    out.push_str("</td></tr>");
    out
}

/// `highlight.DocumentToHTML` (`html.go:54`): the full table, line numbers on.
pub fn document_to_html(code: &str, document: &LsifDocument) -> String {
    let mut manager = ScipHtmlManager::new(code, document, None);
    manager.process();
    let mut out = String::from("<table>");
    for (row, segments) in &manager.rows {
        out.push_str(&render_scip_row(*row, segments));
    }
    out.push_str("</table>");
    out
}

/// `HighlightedCode.LinesForRanges` (`highlight.go:202`): render only the
/// requested rows, then slice each range (clamp order matches Go).
pub fn lines_for_ranges(
    code: &str,
    document: &LsifDocument,
    ranges: &[(i32, i32)],
) -> Result<Vec<Vec<String>>, String> {
    let max_lines = code.split('\n').count() as i32;

    let mut valid_lines = HashSet::new();
    for &(start_line, end_line) in ranges {
        let (start, end) = clamp_range(start_line, end_line, max_lines);
        for row in start..end {
            valid_lines.insert(row);
        }
    }

    let mut manager = ScipHtmlManager::new(code, document, Some(valid_lines));
    manager.process();
    let string_rows: HashMap<i32, String> = manager
        .rows
        .iter()
        .map(|(row, segments)| (*row, render_scip_row(*row, segments)))
        .collect();

    let mut line_ranges = Vec::with_capacity(ranges.len());
    for &(start_line, end_line) in ranges {
        let (start, end) = clamp_range(start_line, end_line, max_lines);
        let mut current = Vec::new();
        for row in start..end {
            match string_rows.get(&row) {
                Some(text) => current.push(text.clone()),
                None => return Err("Missing row for some reason".to_owned()),
            }
        }
        line_ranges.push(current);
    }
    Ok(line_ranges)
}

/// Go `LinesForRanges`/`SplitLineRanges` clamp order:
/// `start<0→0`, `start>end→(0,0)`, `end>max→max`.
fn clamp_range(start_line: i32, end_line: i32, max_lines: i32) -> (i32, i32) {
    let mut start = start_line;
    let mut end = end_line;
    if start < 0 {
        start = 0;
    }
    if start > end {
        start = 0;
        end = 0;
    }
    if end > max_lines {
        end = max_lines;
    }
    (start, end)
}

/// Minimal JSON string quoting matching gogo `jsonpb` (no HTML escaping).
fn jsonpb_string(text: &str) -> String {
    let mut out = String::with_capacity(text.len() + 2);
    out.push('"');
    for ch in text.chars() {
        match ch {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            '\u{08}' => out.push_str("\\b"),
            '\u{0c}' => out.push_str("\\f"),
            c if (c as u32) < 0x20 => {
                out.push_str(&format!("\\u{:04x}", c as u32));
            }
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

fn lsif_occurrence_json(occurrence: &LsifOccurrence) -> String {
    // Field order mirrors gogo `jsonpb`'s struct-field traversal so the
    // serialized string is byte-identical to Go (the resolver returns it as a
    // string). `EmitDefaults:false` omits zero/empty values.
    let mut parts: Vec<String> = Vec::new();
    if !occurrence.range.is_empty() {
        let range: Vec<String> = occurrence.range.iter().map(i32::to_string).collect();
        parts.push(format!("\"range\":[{}]", range.join(",")));
    }
    if let Some(symbol) = &occurrence.symbol {
        if !symbol.is_empty() {
            parts.push(format!("\"symbol\":{}", jsonpb_string(symbol)));
        }
    }
    if occurrence.symbol_roles != 0 {
        parts.push(format!("\"symbolRoles\":{}", occurrence.symbol_roles));
    }
    if !occurrence.override_documentation.is_empty() {
        let docs: Vec<String> = occurrence
            .override_documentation
            .iter()
            .map(|doc| jsonpb_string(doc))
            .collect();
        parts.push(format!("\"overrideDocumentation\":[{}]", docs.join(",")));
    }
    if occurrence.syntax_kind != 0 {
        parts.push(format!("\"syntaxKind\":{}", occurrence.syntax_kind));
    }
    format!("{{{}}}", parts.join(","))
}

/// `HighlightedFileResolver.LSIF` (`graphqlbackend/highlight.go:58`):
/// `jsonpb{EnumsAsInts:true,EmitDefaults:false}`. An empty/nil document
/// marshals to `"{}"`.
pub fn lsif_document_json(document: &LsifDocument) -> String {
    let mut parts: Vec<String> = Vec::new();
    if let Some(language) = &document.language {
        if !language.is_empty() {
            parts.push(format!("\"language\":{}", jsonpb_string(language)));
        }
    }
    if let Some(path) = &document.relative_path {
        if !path.is_empty() {
            parts.push(format!("\"relativePath\":{}", jsonpb_string(path)));
        }
    }
    if !document.occurrences.is_empty() {
        let occurrences: Vec<String> = document
            .occurrences
            .iter()
            .map(lsif_occurrence_json)
            .collect();
        parts.push(format!("\"occurrences\":[{}]", occurrences.join(",")));
    }
    format!("{{{}}}", parts.join(","))
}

// ---------------------------------------------------------------------------
// Selection helpers
// ---------------------------------------------------------------------------

fn find_field<'a>(selections: &'a [RootField], name: &str) -> Option<&'a RootField> {
    selections.iter().find(|field| field.name == name)
}

/// All fields under `selections`, including inline-fragment conditions,
/// recursively.
fn descendant_fields<'a>(selections: &'a [RootField], out: &mut Vec<&'a RootField>) {
    for field in selections {
        out.push(field);
        descendant_fields(&field.selections, out);
        for (_, nested) in &field.type_conditions {
            descendant_fields(nested, out);
        }
    }
}

/// Whether `field`'s subtree selects any of `names`.
fn subtree_mentions(field: &RootField, names: &[&str]) -> bool {
    let mut fields = Vec::new();
    descendant_fields(&field.selections, &mut fields);
    for (_, nested) in &field.type_conditions {
        descendant_fields(nested, &mut fields);
    }
    fields
        .iter()
        .any(|child| names.contains(&child.name.as_str()))
}

fn string_list(arguments: &Arguments, name: &str) -> Vec<String> {
    match arguments.get(name) {
        Some(InputValue::List(items)) => items
            .iter()
            .filter_map(|item| item.as_str().map(str::to_owned))
            .collect(),
        _ => Vec::new(),
    }
}

fn int_arg(arguments: &Arguments, name: &str) -> Option<i32> {
    arguments.i64(name).map(|value| value as i32)
}

/// `diffPathOrNull`.
pub(crate) fn diff_path(path: &[u8]) -> Option<String> {
    if path.is_empty() || path == b"/dev/null" {
        None
    } else {
        Some(String::from_utf8_lossy(path).into_owned())
    }
}

// ---------------------------------------------------------------------------
// Repository.comparison prefetch
// ---------------------------------------------------------------------------

/// Prefetch `Repository.comparison` and its lazy `fileDiffs`/`commits`.
pub(crate) async fn prefetch_repository_comparison(
    source: &dyn GitObjectSource,
    prefetch: &mut GitPrefetch,
    repo: &MinimalRepo,
    selections: &[RootField],
) {
    let Some(field) = find_field(selections, "comparison") else {
        return;
    };
    let Some(extra) = source.commit_extra() else {
        return;
    };
    let base = field.arguments.string("base").unwrap_or("HEAD").to_owned();
    let head = field.arguments.string("head").unwrap_or("HEAD").to_owned();
    let key = (repo.id, base.clone(), head.clone());
    if !prefetch
        .comparison
        .repository_comparisons
        .contains_key(&key)
    {
        let result =
            compute_repository_comparison(extra, source, prefetch, repo, &base, &head).await;
        prefetch
            .comparison
            .repository_comparisons
            .insert(key.clone(), result);
    }
    let Ok(mut data) = prefetch.comparison.repository_comparisons[&key].clone() else {
        return;
    };

    if let Some(file_diffs_field) = find_field(&field.selections, "fileDiffs") {
        let paths = string_list(&file_diffs_field.arguments, "paths");
        let result = compute_file_diffs(extra, repo, &data, paths).await;
        if let Ok(files) = &result {
            prefetch_file_diff_extras(
                source,
                prefetch,
                repo,
                &data,
                files,
                &file_diffs_field.selections,
            )
            .await;
        }
        data.file_diffs = Some(result);
    }
    if let Some(commits_field) = find_field(&field.selections, "commits") {
        let first = int_arg(&commits_field.arguments, "first");
        let path = commits_field
            .arguments
            .string("path")
            .map(str::to_owned)
            .unwrap_or_default();
        data.commits = Some(
            compute_comparison_commits(
                extra,
                source,
                prefetch,
                repo,
                &data,
                first,
                &path,
                &commits_field.selections,
                0,
            )
            .await,
        );
    }
    if let Some(stored) = prefetch
        .comparison
        .repository_comparisons
        .get_mut(&key)
        .and_then(|result| result.as_mut().ok())
    {
        stored.file_diffs = data.file_diffs;
        stored.commits = data.commits;
    }
}

async fn compute_repository_comparison(
    extra: &dyn GitCommitExtraSource,
    source: &dyn GitObjectSource,
    prefetch: &mut GitPrefetch,
    repo: &MinimalRepo,
    base_revspec: &str,
    head_revspec: &str,
) -> Result<ComparisonData, String> {
    // `getCommit(headRevspec)`: `DevNullSHA` yields a nil resolver; keep the
    // literal so the range strings still match the baseline.
    let head_oid = if head_revspec == DEV_NULL_SHA {
        DEV_NULL_SHA.to_owned()
    } else {
        match load_revision(source, prefetch, repo, head_revspec).await {
            RevisionLookup::Found(oid) => oid,
            RevisionLookup::NotFound => {
                return Err(format!("revision not found: {}@{head_revspec}", repo.name))
            }
            RevisionLookup::Error(message) => return Err(message),
        }
    };

    let mut range_type = "..".to_owned();
    let mut base_oid = None;
    if base_revspec != DEV_NULL_SHA {
        range_type = "...".to_owned();
        let merge_base = extra
            .merge_base(repo.id, &repo.name, base_revspec, head_revspec)
            .await
            .map_err(|error| error.message)?;
        let base_commit = if merge_base.is_empty() {
            range_type = "..".to_owned();
            base_revspec.to_owned()
        } else {
            merge_base
        };
        if base_commit != DEV_NULL_SHA {
            match load_revision(source, prefetch, repo, &base_commit).await {
                RevisionLookup::Found(oid) => base_oid = Some(oid),
                RevisionLookup::NotFound => {
                    return Err(format!("revision not found: {}@{base_commit}", repo.name))
                }
                RevisionLookup::Error(message) => return Err(message),
            }
        }
        // `GitRevisionRange.baseRevSpec.object` resolves the original revspec.
        load_revision(source, prefetch, repo, base_revspec).await;
    }

    Ok(ComparisonData {
        base_revspec: base_revspec.to_owned(),
        head_revspec: head_revspec.to_owned(),
        range_type,
        base_oid,
        head_oid,
        file_diffs: None,
        commits: None,
    })
}

// ---------------------------------------------------------------------------
// FileDiff content / highlight prefetch (shared with `GitCommit.diff`)
// ---------------------------------------------------------------------------

pub(crate) async fn ensure_content(
    source: &dyn GitObjectSource,
    prefetch: &mut GitPrefetch,
    repo: &MinimalRepo,
    oid: &str,
    path: &str,
) {
    let key = (repo.id, oid.to_owned(), path.to_owned());
    if prefetch.contents.contains_key(&key) {
        return;
    }
    let lookup = match source.read_file(repo.id, &repo.name, oid, path).await {
        Ok(bytes) => crate::git_graphql::ContentLookup::Found(bytes),
        Err(error) => crate::git_graphql::ContentLookup::Error(error.message),
    };
    prefetch.contents.insert(key, lookup);
}

fn content_bytes(
    prefetch: &GitPrefetch,
    repo_id: i32,
    oid: &str,
    path: &str,
) -> Result<Vec<u8>, String> {
    match prefetch.content(repo_id, oid, path) {
        Some(crate::git_graphql::ContentLookup::Found(bytes)) => Ok(bytes.clone()),
        Some(crate::git_graphql::ContentLookup::Error(message)) => Err(message.clone()),
        None => Err("content is not prefetched".to_owned()),
    }
}

fn highlight_args(field: &RootField) -> HighlightArgsKey {
    HighlightArgsKey {
        start_line: int_arg(&field.arguments, "startLine"),
        end_line: int_arg(&field.arguments, "endLine"),
        disable_timeout: field.arguments.bool("disableTimeout").unwrap_or(false),
        highlight_long_lines: field.arguments.bool("highlightLongLines").unwrap_or(false),
        format: field
            .arguments
            .string("format")
            .unwrap_or("HTML_HIGHLIGHT")
            .to_owned(),
    }
}

pub(crate) fn parse_highlight_args(arguments: &Arguments) -> HighlightArgsKey {
    HighlightArgsKey {
        start_line: int_arg(arguments, "startLine"),
        end_line: int_arg(arguments, "endLine"),
        disable_timeout: arguments.bool("disableTimeout").unwrap_or(false),
        highlight_long_lines: arguments.bool("highlightLongLines").unwrap_or(false),
        format: arguments
            .string("format")
            .unwrap_or("HTML_HIGHLIGHT")
            .to_owned(),
    }
}

/// Prefetch the `oldFile`/`newFile`/`mostRelevantFile` content reads and the
/// hunk `highlight` results that the `fileDiffs` selection set can observe.
pub(crate) async fn prefetch_file_diff_extras(
    source: &dyn GitObjectSource,
    prefetch: &mut GitPrefetch,
    repo: &MinimalRepo,
    data: &ComparisonData,
    files: &[FileDiff],
    file_diffs_selections: &[RootField],
) {
    let Some(nodes) = find_field(file_diffs_selections, "nodes") else {
        return;
    };

    let old_file = find_field(&nodes.selections, "oldFile");
    let new_file = find_field(&nodes.selections, "newFile");
    let most_relevant = find_field(&nodes.selections, "mostRelevantFile");
    let hunks = find_field(&nodes.selections, "hunks");

    const CONTENT_FIELDS: [&str; 4] = ["content", "byteSize", "totalLines", "binary"];
    let old_content = old_file.is_some_and(|field| subtree_mentions(field, &CONTENT_FIELDS))
        || most_relevant.is_some_and(|field| subtree_mentions(field, &CONTENT_FIELDS));
    let new_content = new_file.is_some_and(|field| subtree_mentions(field, &CONTENT_FIELDS))
        || most_relevant.is_some_and(|field| subtree_mentions(field, &CONTENT_FIELDS));

    let mut highlight_fields: Vec<&RootField> = Vec::new();
    if let Some(hunks) = hunks {
        let mut fields = Vec::new();
        descendant_fields(&hunks.selections, &mut fields);
        for (_, nested) in &hunks.type_conditions {
            descendant_fields(nested, &mut fields);
        }
        for field in fields {
            if field.name == "highlight" {
                highlight_fields.push(field);
            }
        }
    }

    for file in files {
        let old_path = diff_path(&file.orig_name);
        let new_path = diff_path(&file.new_name);

        if old_content {
            if let (Some(path), Some(oid)) = (&old_path, &data.base_oid) {
                ensure_content(source, prefetch, repo, oid, path).await;
            }
        }
        if new_content {
            if let Some(path) = &new_path {
                ensure_content(source, prefetch, repo, &data.head_oid, path).await;
            }
        }

        for field in &highlight_fields {
            let args = highlight_args(field);
            prefetch_side_highlight(
                source,
                prefetch,
                repo,
                old_path.as_deref(),
                data.base_oid.as_deref(),
                &args,
            )
            .await;
            prefetch_side_highlight(
                source,
                prefetch,
                repo,
                new_path.as_deref(),
                Some(&data.head_oid),
                &args,
            )
            .await;
        }
    }
}

async fn prefetch_side_highlight(
    source: &dyn GitObjectSource,
    prefetch: &mut GitPrefetch,
    repo: &MinimalRepo,
    path: Option<&str>,
    oid: Option<&str>,
    args: &HighlightArgsKey,
) {
    let (Some(path), Some(oid)) = (path, oid) else {
        return;
    };
    let key = HighlightKey {
        repo_id: repo.id,
        oid: oid.to_owned(),
        path: path.to_owned(),
        args: args.clone(),
    };
    if prefetch.comparison.highlights.contains_key(&key) {
        return;
    }
    ensure_content(source, prefetch, repo, oid, path).await;
    let result = match content_bytes(prefetch, repo.id, oid, path) {
        Err(message) => Err(message),
        Ok(content) => {
            let request = HighlightRequest {
                content,
                filepath: path.to_owned(),
                disable_timeout: args.disable_timeout,
                highlight_long_lines: args.highlight_long_lines,
                format: args.format.clone(),
                start_line: args.start_line,
                end_line: args.end_line,
                // `fileDiffHighlighter` keeps the final newline.
                keep_final_newline: true,
            };
            match compute_highlight(source, &request).await {
                Ok(highlighted) => Ok(highlighted),
                Err(HighlightError::Binary) => Ok(HighlightedFile::default()),
                Err(HighlightError::Message(message)) => Err(message),
            }
        }
    };
    prefetch.comparison.highlights.insert(key, result);
}

fn highlighted_side(
    prefetch: Option<&GitPrefetch>,
    repo_id: i32,
    oid: Option<&str>,
    path: Option<&str>,
    args: &HighlightArgsKey,
) -> Result<HighlightedFile, String> {
    let (Some(git), Some(oid), Some(path)) = (prefetch, oid, path) else {
        return Ok(HighlightedFile::default());
    };
    let key = HighlightKey {
        repo_id,
        oid: oid.to_owned(),
        path: path.to_owned(),
        args: args.clone(),
    };
    match git.comparison.highlights.get(&key) {
        Some(Ok(highlighted)) => Ok(highlighted.clone()),
        Some(Err(message)) => Err(message.clone()),
        // M9.34d: `Changeset.diff` is resolved from the synchronous GraphQL
        // field hook (`batches_changeset_read.rs`), which has no selection set
        // to drive `prefetch_file_diff_extras`. Both of its paths still
        // prefetch the `oldFile`/`newFile` content (`ensure_content`), so
        // replay `highlight.CodeAsLines` from that content with the same
        // offline highlighter. This mirrors `prefetch_side_highlight` (no new
        // highlighter is introduced).
        None => match content_bytes(git, repo_id, oid, path) {
            Err(message) => Err(message),
            Ok(content) => {
                let request = HighlightRequest {
                    content,
                    filepath: path.to_owned(),
                    disable_timeout: args.disable_timeout,
                    highlight_long_lines: args.highlight_long_lines,
                    format: args.format.clone(),
                    start_line: args.start_line,
                    end_line: args.end_line,
                    // `fileDiffHighlighter` keeps the final newline.
                    keep_final_newline: true,
                };
                match offline_highlight(&request) {
                    Ok(highlighted) => Ok(highlighted),
                    Err(HighlightError::Binary) => Ok(HighlightedFile::default()),
                    Err(HighlightError::Message(message)) => Err(message),
                }
            }
        },
    }
}

// ---------------------------------------------------------------------------
// FileDiff File2 / hunk highlight helpers (replayed by `git_commit_extra.rs`)
// ---------------------------------------------------------------------------

/// Where a `FileDiff.newFile` comes from: the head commit (regular comparison)
/// or [`PreviewData`] (patch preview).
#[derive(Clone)]
pub enum NewFileSource {
    Tree,
    Preview(Arc<PreviewData>),
}

impl std::fmt::Debug for NewFileSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Tree => f.write_str("Tree"),
            Self::Preview(_) => f.write_str("Preview"),
        }
    }
}

/// `FileDiff.oldFile`'s `GitTreeEntryResolver`.
pub(crate) fn tree_file_object(
    repo: &MinimalRepo,
    oid: Option<&str>,
    path: &str,
    git: Option<Arc<GitPrefetch>>,
) -> GitTreeEntryObject {
    GitTreeEntryObject {
        repo: repo.clone(),
        oid: oid.unwrap_or_default().to_owned(),
        input_rev: None,
        stat: FileStat::synthetic(path, false),
        git,
    }
}

/// `FileDiff.newFile`'s `File2`.
pub(crate) fn new_file_value(
    repo: &MinimalRepo,
    source: &NewFileSource,
    path: &str,
    head_oid: &str,
    git: Option<Arc<GitPrefetch>>,
) -> FieldValue {
    match source {
        NewFileSource::Tree => {
            FieldValue::Object(Arc::new(tree_file_object(repo, Some(head_oid), path, git)))
        }
        NewFileSource::Preview(preview) => FieldValue::Object(Arc::new(VirtualFileObject {
            path: path.to_owned(),
            content: preview.content(path),
        })),
    }
}

/// `fileDiffVirtualFileContent`: the preview's patched new content.
#[derive(Clone, Default)]
pub struct PreviewData {
    new_contents: HashMap<String, Result<String, String>>,
}

impl PreviewData {
    pub fn content(&self, path: &str) -> Result<String, String> {
        match self.new_contents.get(path) {
            Some(Ok(content)) => Ok(content.clone()),
            Some(Err(message)) => Err(message.clone()),
            None => Ok(String::new()),
        }
    }

    /// Compute the highlighted lines of the patched content synchronously with
    /// the offline implementation (the preview path has no prefetch step).
    fn highlight(&self, path: &str, args: &HighlightArgsKey) -> Result<HighlightedFile, String> {
        let content = self.content(path)?;
        let request = HighlightRequest {
            content: content.into_bytes(),
            filepath: path.to_owned(),
            disable_timeout: args.disable_timeout,
            highlight_long_lines: args.highlight_long_lines,
            format: args.format.clone(),
            start_line: args.start_line,
            end_line: args.end_line,
            keep_final_newline: true,
        };
        match offline_highlight(&request) {
            Ok(highlighted) => Ok(highlighted),
            Err(HighlightError::Binary) => Ok(HighlightedFile::default()),
            Err(HighlightError::Message(message)) => Err(message),
        }
    }
}

/// `safeIndex`: out-of-bounds lines render `<div></div>`.
fn safe_index(lines: &[String], target: i32) -> String {
    if target >= 0 && lines.len() > target as usize {
        lines[target as usize].clone()
    } else {
        "<div></div>".to_owned()
    }
}

/// `DiffHunk.Highlight`'s line assembly.
fn hunk_lines(
    body: &[u8],
    orig_start_line: i32,
    new_start_line: i32,
    highlighted_base: &[String],
    highlighted_head: &[String],
) -> Result<Vec<(String, &'static str)>, String> {
    let mut lines: Vec<(String, &'static str)> = Vec::new();
    let trimmed = body.strip_suffix(b"\n").unwrap_or(body);
    let mut base_line = orig_start_line - 1;
    let mut head_line = new_start_line - 1;
    for hunk_line in trimmed.split(|&byte| byte == b'\n') {
        let first = *hunk_line.first().ok_or_else(|| {
            "expected patch lines to start with ' ', '-', '+', but found ''".to_owned()
        })?;
        match first {
            b' ' => {
                lines.push((safe_index(highlighted_base, base_line), "UNCHANGED"));
                base_line += 1;
                head_line += 1;
            }
            b'+' => {
                lines.push((safe_index(highlighted_head, head_line), "ADDED"));
                head_line += 1;
            }
            b'-' => {
                lines.push((safe_index(highlighted_base, base_line), "DELETED"));
                base_line += 1;
            }
            other => {
                return Err(format!(
                    "expected patch lines to start with ' ', '-', '+', but found {}",
                    go_quoted_byte(other)
                ))
            }
        }
    }
    Ok(lines)
}

/// Go `%q` for one byte in an error message.
fn go_quoted_byte(byte: u8) -> String {
    match byte {
        b'\'' => "'\\''".to_owned(),
        b'\\' => "'\\\\'".to_owned(),
        b'\n' => "'\\n'".to_owned(),
        b'\r' => "'\\r'".to_owned(),
        b'\t' => "'\\t'".to_owned(),
        0x20..=0x7e => format!("'{}'", byte as char),
        _ => format!("'\\x{byte:02x}'"),
    }
}

/// Build `HighlightedDiffHunkBody` for one hunk (`DiffHunk.Highlight`).
#[allow(clippy::too_many_arguments)]
pub(crate) fn hunk_highlight_body(
    repo: &MinimalRepo,
    hunk: &Hunk,
    old_path: Option<&str>,
    new_path: Option<&str>,
    base_oid: Option<&str>,
    head_oid: Option<&str>,
    preview: Option<&PreviewData>,
    git: Option<&Arc<GitPrefetch>>,
    arguments: &Arguments,
) -> Result<HighlightedDiffHunkBodyObject, ExecError> {
    let args = parse_highlight_args(arguments);
    let base = highlighted_side(git.map(Arc::as_ref), repo.id, base_oid, old_path, &args)
        .map_err(ExecError::new)?;
    let head = match (preview, new_path) {
        (Some(preview), Some(path)) => preview.highlight(path, &args).map_err(ExecError::new)?,
        _ => highlighted_side(git.map(Arc::as_ref), repo.id, head_oid, new_path, &args)
            .map_err(ExecError::new)?,
    };
    let lines = hunk_lines(
        &hunk.body,
        hunk.orig_start_line,
        hunk.new_start_line,
        &base.lines,
        &head.lines,
    )
    .map_err(ExecError::new)?;
    Ok(HighlightedDiffHunkBodyObject {
        lines,
        aborted: base.aborted || head.aborted,
    })
}

/// `HighlightedDiffHunkBody` / `HighlightedDiffHunkLine`.
pub struct HighlightedDiffHunkBodyObject {
    pub lines: Vec<(String, &'static str)>,
    pub aborted: bool,
}

impl ObjectValue for HighlightedDiffHunkBodyObject {
    fn type_name(&self) -> &'static str {
        "HighlightedDiffHunkBody"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "aborted" => Ok(Some(FieldValue::Json(Json::Bool(self.aborted)))),
            "lines" => Ok(Some(FieldValue::List {
                items: self
                    .lines
                    .iter()
                    .map(|(html, kind)| {
                        FieldValue::Object(Arc::new(HighlightedDiffHunkLineObject {
                            html: html.clone(),
                            kind,
                        }))
                    })
                    .collect(),
                element_type: Some("HighlightedDiffHunkLine"),
            })),
            _ => Ok(None),
        }
    }
}

struct HighlightedDiffHunkLineObject {
    html: String,
    kind: &'static str,
}

impl ObjectValue for HighlightedDiffHunkLineObject {
    fn type_name(&self) -> &'static str {
        "HighlightedDiffHunkLine"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "html" => Ok(Some(FieldValue::Json(Json::Str(self.html.clone())))),
            "kind" => Ok(Some(FieldValue::Json(Json::Str(self.kind.to_owned())))),
            _ => Ok(None),
        }
    }
}

// ---------------------------------------------------------------------------
// Preview comparison
// ---------------------------------------------------------------------------

pub struct PreviewRepositoryComparisonObject {
    pub repo: MinimalRepo,
    pub commit_oid: String,
    pub file_diffs: Vec<FileDiff>,
    pub preview: Arc<PreviewData>,
    pub git: Option<Arc<GitPrefetch>>,
}

impl PreviewRepositoryComparisonObject {
    pub fn with_prefetch(mut self, git: Option<Arc<GitPrefetch>>) -> Self {
        self.git = git;
        self
    }
}

impl ObjectValue for PreviewRepositoryComparisonObject {
    fn type_name(&self) -> &'static str {
        "PreviewRepositoryComparison"
    }

    fn field(
        &self,
        field: &str,
        arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "baseRepository" => Ok(Some(FieldValue::Object(Arc::new(
                crate::graphql_resolvers::RepositoryObject::from_data(
                    crate::git_graphql::repository_data(&self.repo),
                )
                .with_git(self.git.clone()),
            )))),
            "fileDiffs" => Ok(Some(FieldValue::Object(Arc::new(
                crate::git_commit_extra::FileDiffConnectionObject {
                    repo: self.repo.clone(),
                    file_diffs: Ok(self.file_diffs.clone()),
                    first: int_arg(arguments, "first"),
                    after: arguments.string("after").map(str::to_owned),
                    base_oid: Some(self.commit_oid.clone()),
                    head_oid: self.commit_oid.clone(),
                    new_file: NewFileSource::Preview(self.preview.clone()),
                    git: self.git.clone(),
                },
            )))),
            _ => Ok(None),
        }
    }
}

/// `NewPreviewRepositoryComparisonResolver`: resolve `base_rev`, parse the
/// patch and derive each new file's content with [`apply_patch`].
pub async fn new_preview_repository_comparison(
    source: &dyn GitObjectSource,
    prefetch: &mut GitPrefetch,
    repo: &MinimalRepo,
    base_rev: &str,
    patch: &[u8],
) -> Result<PreviewRepositoryComparisonObject, String> {
    let commit_oid = match load_revision(source, prefetch, repo, base_rev).await {
        RevisionLookup::Found(oid) => oid,
        RevisionLookup::NotFound => {
            return Err(format!("revision not found: {}@{}", repo.name, base_rev))
        }
        RevisionLookup::Error(message) => return Err(message),
    };
    let file_diffs = parse_multi_file_diff(patch).map_err(|error| error.to_string())?;

    let mut new_contents = HashMap::new();
    for file in &file_diffs {
        let Some(new_path) = diff_path(&file.new_name) else {
            continue;
        };
        let old_content = match diff_path(&file.orig_name) {
            Some(old_path) => {
                ensure_content(source, prefetch, repo, &commit_oid, &old_path).await;
                match content_bytes(prefetch, repo.id, &commit_oid, &old_path) {
                    Ok(bytes) => String::from_utf8_lossy(&bytes).into_owned(),
                    Err(message) => {
                        new_contents.insert(new_path, Err(message));
                        continue;
                    }
                }
            }
            None => String::new(),
        };
        new_contents.insert(new_path, apply_patch(&old_content, file));
    }

    Ok(PreviewRepositoryComparisonObject {
        repo: repo.clone(),
        commit_oid,
        file_diffs,
        preview: Arc::new(PreviewData { new_contents }),
        git: None,
    })
}

/// `previewNewFile`'s `VirtualFileResolver`.
pub struct VirtualFileObject {
    pub path: String,
    pub content: Result<String, String>,
}

impl ObjectValue for VirtualFileObject {
    fn type_name(&self) -> &'static str {
        "VirtualFile"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "path" => Ok(Some(FieldValue::Json(Json::Str(self.path.clone())))),
            "name" => Ok(Some(FieldValue::Json(Json::Str(go_path_base(&self.path))))),
            "isDirectory" => Ok(Some(FieldValue::Json(Json::Bool(false)))),
            "url" | "canonicalURL" => Ok(Some(FieldValue::Json(Json::Str(String::new())))),
            "content" => Ok(Some(FieldValue::Json(Json::Str(
                self.content
                    .as_ref()
                    .map_err(|message| ExecError::new(message.clone()))?
                    .clone(),
            )))),
            "byteSize" => Ok(Some(FieldValue::Json(Json::Int(i64::from(
                self.content
                    .as_ref()
                    .map_err(|message| ExecError::new(message.clone()))?
                    .len() as i32,
            ))))),
            "totalLines" => {
                let content = self
                    .content
                    .as_ref()
                    .map_err(|message| ExecError::new(message.clone()))?;
                let lines = if is_binary(content.as_bytes()) {
                    0
                } else {
                    content.split('\n').count()
                };
                Ok(Some(FieldValue::Json(Json::Int(lines as i64))))
            }
            "binary" => Ok(Some(FieldValue::Json(Json::Bool(is_binary(
                self.content
                    .as_ref()
                    .map_err(|message| ExecError::new(message.clone()))?
                    .as_bytes(),
            ))))),
            _ => Ok(None),
        }
    }
}

fn go_path_base(path: &str) -> String {
    match path.rsplit_once('/') {
        Some((_, base)) => base.to_owned(),
        None => path.to_owned(),
    }
}

/// `applyPatch`: apply one go-diff `FileDiff` to a file's content.
pub fn apply_patch(file_content: &str, file_diff: &FileDiff) -> Result<String, String> {
    if diff_path(&file_diff.new_name).is_none() {
        // The file was deleted; no need to do costly computation.
        return Ok(String::new());
    }

    let orig_has_final_newline = file_content.ends_with('\n');

    let content_lines: Vec<&str> = if file_content.is_empty() {
        Vec::new()
    } else {
        file_content
            .strip_suffix('\n')
            .unwrap_or(file_content)
            .split('\n')
            .collect()
    };

    let mut new_content_lines: Vec<String> = Vec::new();
    let mut last_hunk_had_newline_in_last_addition = false;
    let mut current_line: i32 = 1;

    for (index, hunk) in file_diff.hunks.iter().enumerate() {
        let is_last_hunk = index + 1 == file_diff.hunks.len();
        if hunk.orig_start_line != 0 && hunk.orig_start_line != current_line {
            let start = (current_line - 1).max(0) as usize;
            let end = (hunk.orig_start_line - 1).max(0) as usize;
            if start > end || end > content_lines.len() {
                return Err("malformed patch: hunk range out of bounds".to_owned());
            }
            new_content_lines.extend(
                content_lines[start..end]
                    .iter()
                    .map(|line| (*line).to_owned()),
            );
            current_line += (end - start) as i32;
        }

        let body = String::from_utf8_lossy(&hunk.body);
        let trimmed = body.strip_suffix('\n').unwrap_or(&body);
        let hunk_lines: Vec<&str> = trimmed.split('\n').collect();
        let hunk_has_final_newline = hunk.body.ends_with(b"\n");
        if is_last_hunk && hunk_has_final_newline {
            last_hunk_had_newline_in_last_addition = true;
        }

        for line in hunk_lines {
            if line.starts_with(' ') {
                let index = (current_line - 1).max(0) as usize;
                let original = content_lines
                    .get(index)
                    .ok_or_else(|| "malformed patch: hunk range out of bounds".to_owned())?;
                new_content_lines.push((*original).to_owned());
                current_line += 1;
            } else if line.starts_with('-') {
                current_line += 1;
            } else if let Some(rest) = line.strip_prefix('+') {
                new_content_lines.push(rest.to_owned());
            } else {
                return Err(format!(
                    "malformed patch, expected hunk lines to start with ' ', '-', or '+' but got {}",
                    go_quote_str(line)
                ));
            }
        }
    }

    let orig_lines = content_lines.len() as i32;
    if orig_lines > 0 && orig_lines != current_line - 1 {
        let start = (current_line - 1).max(0) as usize;
        if start > content_lines.len() {
            return Err("malformed patch: hunk range out of bounds".to_owned());
        }
        new_content_lines.extend(content_lines[start..].iter().map(|line| (*line).to_owned()));
        let mut content = new_content_lines.join("\n");
        if orig_has_final_newline {
            content.push('\n');
        }
        return Ok(content);
    }

    let mut content = new_content_lines.join("\n");
    if last_hunk_had_newline_in_last_addition {
        content.push('\n');
    }
    Ok(content)
}

/// Go `%q` for a string in the malformed-patch error.
fn go_quote_str(text: &str) -> String {
    let mut out = String::from("\"");
    for ch in text.chars() {
        match ch {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            ch => out.push(ch),
        }
    }
    out.push('"');
    out
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use sg_service_gitserver::diff::Hunk;

    fn request(content: &str, format: &str) -> HighlightRequest {
        HighlightRequest {
            content: content.as_bytes().to_vec(),
            filepath: "a.txt".to_owned(),
            disable_timeout: false,
            highlight_long_lines: false,
            format: format.to_owned(),
            start_line: None,
            end_line: None,
            keep_final_newline: true,
        }
    }

    #[test]
    fn plaintext_lines_match_generate_plain_table() {
        let highlighted = offline_highlight(&request("a\nb\n", "HTML_PLAINTEXT")).unwrap();
        assert!(highlighted.aborted);
        assert_eq!(
            highlighted.lines,
            vec![
                "<span>a</span>".to_owned(),
                "<span>b</span>".to_owned(),
                "<span>\n</span>".to_owned(),
            ]
        );
    }

    #[test]
    fn plaintext_escapes_go_html() {
        let highlighted = offline_highlight(&request("a<b&c>\"d'", "HTML_PLAINTEXT")).unwrap();
        assert_eq!(
            highlighted.lines,
            vec!["<span>a&lt;b&amp;c&gt;&#34;d&#39;</span>".to_owned()]
        );
    }

    #[test]
    fn non_plaintext_is_a_seam() {
        assert!(matches!(
            offline_highlight(&request("a\n", "HTML_HIGHLIGHT")),
            Err(HighlightError::Message(_))
        ));
    }

    #[test]
    fn plaintext_applies_start_end_lines_before_highlighting() {
        // M9.35d: `fileDiffHighlighter` slices the file content through
        // `GitTreeEntry.Content` before `highlight.CodeAsLines`.
        let mut req = request("line one\nold line\nline three\ncontext\n", "HTML_PLAINTEXT");
        req.start_line = Some(2);
        req.end_line = Some(2);
        let highlighted = offline_highlight(&req).unwrap();
        assert!(highlighted.aborted);
        assert_eq!(
            highlighted.lines,
            vec!["<span>old line</span>".to_owned(), "<span>\n</span>".to_owned()]
        );
    }

    #[test]
    fn plaintext_out_of_range_slices_empty_content() {
        let mut req = request("a\nb\n", "HTML_PLAINTEXT");
        req.start_line = Some(9);
        let highlighted = offline_highlight(&req).unwrap();
        assert_eq!(highlighted.lines, vec!["<span>\n</span>".to_owned()]);
    }

    #[test]
    fn hunk_lines_map_context_added_deleted() {
        let base = vec!["<span>o1</span>".to_owned(), "<span>o2</span>".to_owned()];
        let head = vec!["<span>n1</span>".to_owned(), "<span>n2</span>".to_owned()];
        let lines = hunk_lines(b" o1\n-nope\n+n1\n", 1, 1, &base, &head).unwrap();
        assert_eq!(
            lines,
            vec![
                ("<span>o1</span>".to_owned(), "UNCHANGED"),
                ("<span>o2</span>".to_owned(), "DELETED"),
                // The context line consumed new line 1, so the added line reads
                // the next highlighted-head index.
                ("<span>n2</span>".to_owned(), "ADDED"),
            ]
        );
    }

    fn file_diff(orig: &str, new: &str, hunks: Vec<Hunk>) -> FileDiff {
        FileDiff {
            orig_name: orig.as_bytes().to_vec(),
            new_name: new.as_bytes().to_vec(),
            extended: Vec::new(),
            hunks,
        }
    }

    fn hunk(orig_start: i32, new_start: i32, body: &str) -> Hunk {
        Hunk {
            orig_start_line: orig_start,
            orig_lines: 0,
            new_start_line: new_start,
            new_lines: 0,
            section: Vec::new(),
            body: body.as_bytes().to_vec(),
            orig_no_newline_at: 0,
        }
    }

    #[test]
    fn apply_patch_replaces_and_appends() {
        let diff = file_diff("a.txt", "a.txt", vec![hunk(1, 1, " one\n-two\n+TWO\n")]);
        assert_eq!(
            apply_patch("one\ntwo\nthree\n", &diff).unwrap(),
            "one\nTWO\nthree\n"
        );
    }

    #[test]
    fn apply_patch_deleted_file() {
        let diff = file_diff("a.txt", "/dev/null", vec![]);
        assert_eq!(apply_patch("gone\n", &diff).unwrap(), "");
    }

    #[test]
    fn apply_patch_handles_no_final_newline() {
        let diff = file_diff("a.txt", "a.txt", vec![hunk(1, 1, "-a\n+b")]);
        assert_eq!(apply_patch("a", &diff).unwrap(), "b");
    }

    #[test]
    fn apply_patch_matches_go_preview_fixture() {
        let old = "First\nSecond\nLine 1\nLine 2\nLine 3\nLine 4\nLine 5\nLine 6\nLine 7\nLine 8\nLine 9\nLine 10\n";
        let patch = "--- INSTALL.md\n+++ INSTALL.md\n@@ -3,10 +3,10 @@\n Line 1\n Line 2\n Line 3\n-Line 4\n+This is cool: Line 4\n Line 5\n Line 6\n-Line 7\n-Line 8\n+Another Line 7\n+Foobar Line 8\n Line 9\n Line 10\n";
        let files = parse_multi_file_diff(patch.as_bytes()).unwrap();
        assert_eq!(
            apply_patch(old, &files[0]).unwrap(),
            "First\nSecond\nLine 1\nLine 2\nLine 3\nThis is cool: Line 4\nLine 5\nLine 6\nAnother Line 7\nFoobar Line 8\nLine 9\nLine 10\n"
        );
    }
}
