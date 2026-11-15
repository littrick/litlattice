#![forbid(unsafe_code)]

//! Code Intel code-navigation GraphQL surface (M9.48b).
//!
//! Ports the Go `codeintel.codenav.graphql` resolver layer and its pure data
//! conversions:
//!
//! - `internal/codeintel/codenav/transport/graphql/{root_resolver,
//!   root_resolver_definitions,root_resolver_references,
//!   root_resolver_implementations,root_resolver_hover,root_resolver_ranges,
//!   root_resolver_stencil,root_resolver_diagnostics,root_resolver_code_graph,
//!   root_resolver_raw_scip,root_resolver_usages,util_cursor,util_locations}.go`;
//! - `internal/codeintel/codenav/internal/lsifstore/{document_metadata,
//!   locations_by_position,metadata_by_position}.go`;
//! - `internal/codeintel/codenav/{service,service_new,gittree_translator,
//!   utils}.go` (the subset reachable from the fields above).
//!
//! The database is injected through [`CodeIntelCodenavDatabase`], a synchronous
//! seam in the same style as the uploads resolver. The production adapter
//! ([`crate::codeintel_codenav_store`]) only reads the existing
//! `lsif_uploads*`/`lsif_nearest_uploads*` and `codeintel_scip_*` tables; no
//! table, column or Redis key is added.
//!
//! Deviations (also recorded in
//! `dev/rs/docs/overviews/m9_48b_codeintel_codenav_overview.md`):
//! - `GitBlob.localCodeIntel` / `GitBlob.symbolInfo` need the symbols gRPC
//!   `LocalCodeIntel`/`SymbolInfo` methods (the Rust client only exposes
//!   `search`), so they stay unassembled;
//! - `Query.usagesForSymbol` implements local precise usages; syntactic and
//!   search-based results and the cross-index symbol trie remain out of scope;
//! - the `experimentalFeatures.scipBasedAPIs` site-config gate is not wired.

use std::collections::BTreeMap;
use std::sync::Arc;

use base64::Engine as _;
use serde::{Deserialize, Serialize};
use sg_graphql::exec::{Actor, Arguments, ExecError, FieldValue, InputValue, ObjectValue};
use sg_graphql::json::Json;
use sg_scip::scip;

use crate::git_tree_entry_graphql::{
    range_url_fragment, CodeIntelGitBlobObject, GitTreeEntryObject, LocationObject, RangeObject,
    SymbolRange,
};
use crate::graphql_resolvers::PageInfoObject;
use crate::search_stream::convert::MinimalRepo;

/// `shared.SyntacticIndexer` (`internal/codeintel/uploads/shared/indexers2.go:15`).
pub const SYNTACTIC_INDEXER: &str = "scip-syntax";

/// `DefaultDefinitionsPageSize`.
pub const DEFAULT_DEFINITIONS_PAGE_SIZE: i64 = 100;
/// `DefaultReferencesPageSize`.
pub const DEFAULT_REFERENCES_PAGE_SIZE: i64 = 100;
/// `DefaultImplementationsPageSize`.
pub const DEFAULT_IMPLEMENTATIONS_PAGE_SIZE: i64 = 100;
/// `DefaultDiagnosticsPageSize`.
pub const DEFAULT_DIAGNOSTICS_PAGE_SIZE: i64 = 100;
/// `maxPageSize` in `codeGraphDataResolver.Occurrences`.
pub const MAX_OCCURRENCES_PAGE_SIZE: i64 = 100_000;

/// `ErrIllegalLimit` / `ErrIllegalBounds` message text.
pub const ERR_ILLEGAL_LIMIT: &str = "illegal limit";
pub const ERR_ILLEGAL_BOUNDS: &str = "illegal bounds";

/// `scip.SymbolRole_Definition`.
pub const SYMBOL_ROLE_DEFINITION: i32 = 1;
/// `scip.SymbolRole_ForwardDefinition`.
pub const SYMBOL_ROLE_FORWARD_DEFINITION: i32 = 64;

// ---------------------------------------------------------------------------
// Seam data
// ---------------------------------------------------------------------------

/// A completed upload, reduced to the columns the codenav surface reads
/// (`shared.CompletedUpload`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UploadRecord {
    pub id: i64,
    pub repository_id: i32,
    pub repository_name: String,
    pub commit: String,
    pub root: String,
    pub indexer: String,
    pub indexer_version: String,
}

impl UploadRecord {
    /// `core.NewUploadRelPath`: `strings.TrimPrefix(repoPath, upload.Root)`.
    pub fn upload_rel_path(&self, repo_path: &str) -> String {
        repo_path
            .strip_prefix(self.root.as_str())
            .unwrap_or(repo_path)
            .to_owned()
    }
}

/// One SCIP diagnostic attached to an occurrence, already scoped to a document
/// (`shared.Diagnostic[core.UploadRelPath]`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DiagnosticRecord {
    pub path: String,
    pub range: SymbolRange,
    pub severity: i32,
    pub code: String,
    pub message: String,
    pub source: String,
}

/// The output of `extractOccurrenceData`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ExtractedOccurrenceData {
    pub definitions: Vec<UsageBuilder>,
    pub references: Vec<UsageBuilder>,
    pub implementations: Vec<UsageBuilder>,
    pub prototypes: Vec<UsageBuilder>,
    pub hover_text: Vec<String>,
}

/// `shared.UsageBuilder`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UsageBuilder {
    pub range: SymbolRange,
    pub symbol: String,
    pub symbol_roles: i32,
}

/// `shared.UsageKind`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UsageKind {
    Definition,
    Reference,
    Implementation,
    Super,
}

impl UsageKind {
    pub fn graphql_name(self) -> &'static str {
        match self {
            UsageKind::Definition => "DEFINITION",
            UsageKind::Reference => "REFERENCE",
            UsageKind::Implementation => "IMPLEMENTATION",
            UsageKind::Super => "SUPER",
        }
    }
}

/// A usage scoped to an upload (the local precise subset of
/// `shared.UploadUsage`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UploadUsage {
    pub upload: UploadRecord,
    pub path: String,
    pub target_commit: String,
    pub range: SymbolRange,
    pub symbol: String,
    pub kind: UsageKind,
}

/// `shared.UploadLocation`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UploadLocation {
    pub upload: UploadRecord,
    pub path: String,
    pub target_commit: String,
    pub range: SymbolRange,
}

/// `shared.SnapshotData`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotData {
    pub document_offset: i32,
    pub symbol: String,
    pub additional_data: Vec<String>,
}

/// `codenav.PreciseCursor`, reduced to the local-phase offsets this slice
/// advances. Unknown JSON fields from a Go-issued cursor are ignored.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PreciseCursor {
    #[serde(rename = "p", default)]
    pub phase: String,
    #[serde(rename = "l_uo", default)]
    pub local_upload_offset: i32,
    #[serde(rename = "l_lo", default)]
    pub local_location_offset: i32,
}

impl PreciseCursor {
    /// `PreciseCursor.Encode`: `base64.RawURLEncoding(json.Marshal(c))`.
    pub fn encode(&self) -> String {
        let json = serde_json::to_vec(self).expect("serialize cursor");
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(json)
    }

    /// `codenav.DecodeCursor`.
    pub fn decode(raw: &str) -> Result<Self, String> {
        if raw.is_empty() {
            return Ok(Self::default());
        }
        let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .decode(raw)
            .map_err(|error| error.to_string())?;
        serde_json::from_slice(&bytes).map_err(|error| error.to_string())
    }
}

/// `transport/graphql.decodeCursor`: standard base64 of the raw cursor.
pub fn decode_cursor(value: Option<&str>) -> Result<String, String> {
    match value {
        None => Ok(String::new()),
        Some(value) => base64::engine::general_purpose::STANDARD
            .decode(value)
            .map(|bytes| String::from_utf8_lossy(&bytes).into_owned())
            .map_err(|error| error.to_string()),
    }
}

/// `transport/graphql.encodeCursor`.
pub fn encode_cursor(value: Option<&str>) -> String {
    value
        .map(|value| base64::engine::general_purpose::STANDARD.encode(value))
        .unwrap_or_default()
}

/// `transport/graphql.marshalCursor`: base64 of `{"Index":N}`.
pub fn marshal_occurrence_cursor(index: usize) -> String {
    let json = serde_json::json!({ "Index": index }).to_string();
    base64::engine::general_purpose::STANDARD.encode(json)
}

/// `transport/graphql.UnmarshalCursor` for occurrence connections.
pub fn unmarshal_occurrence_cursor(value: &str) -> Result<usize, String> {
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(value)
        .map_err(|error| error.to_string())?;
    #[derive(Deserialize)]
    struct Cursor {
        #[serde(rename = "Index", default)]
        index: i64,
    }
    let cursor: Cursor = serde_json::from_slice(&bytes).map_err(|error| error.to_string())?;
    Ok(cursor.index.max(0) as usize)
}

// ---------------------------------------------------------------------------
// Range / position helpers (scip position.go)
// ---------------------------------------------------------------------------

/// `scip.NewRangeUnchecked`.
pub fn symbol_range_from_scip(range: &[i32]) -> SymbolRange {
    let end_character = if range.len() == 4 { range[3] } else { range[2] };
    let end_line = if range.len() == 4 { range[2] } else { range[0] };
    SymbolRange {
        start: crate::git_tree_entry_graphql::Position {
            line: range[0],
            character: range[1],
        },
        end: crate::git_tree_entry_graphql::Position {
            line: end_line,
            character: end_character,
        },
    }
}

/// `Range.SCIPRange`.
pub fn scip_range_from_symbol(range: SymbolRange) -> Vec<i32> {
    if range.start.line == range.end.line {
        vec![
            range.start.line,
            range.start.character,
            range.end.character,
        ]
    } else {
        vec![
            range.start.line,
            range.start.character,
            range.end.line,
            range.end.character,
        ]
    }
}

fn position_less(a: crate::git_tree_entry_graphql::Position, b: crate::git_tree_entry_graphql::Position) -> bool {
    a.line < b.line || (a.line == b.line && a.character < b.character)
}

fn position_compare(
    a: crate::git_tree_entry_graphql::Position,
    b: crate::git_tree_entry_graphql::Position,
) -> i32 {
    use std::cmp::Ordering;
    match a.line.cmp(&b.line) {
        Ordering::Less => -1,
        Ordering::Greater => 1,
        Ordering::Equal => match a.character.cmp(&b.character) {
            Ordering::Less => -1,
            Ordering::Greater => 1,
            Ordering::Equal => 0,
        },
    }
}

/// `Range.Contains`.
pub fn range_contains(
    range: SymbolRange,
    position: crate::git_tree_entry_graphql::Position,
) -> bool {
    !position_less(position, range.start) && position_less(position, range.end)
}

/// `Range.CompareStrict`.
pub fn range_compare_strict(a: SymbolRange, b: SymbolRange) -> i32 {
    let start = position_compare(a.start, b.start);
    if start != 0 {
        return start;
    }
    position_compare(a.end, b.end)
}

// ---------------------------------------------------------------------------
// SCIP document consumption (lsifstore)
// ---------------------------------------------------------------------------

/// `scip.FindSymbolBinarySearch`.
pub fn find_symbol_binary_search<'a>(
    symbols: &'a [scip::SymbolInformation],
    symbol_name: &str,
) -> Option<&'a scip::SymbolInformation> {
    symbols
        .binary_search_by(|symbol| symbol.symbol.as_str().cmp(symbol_name))
        .ok()
        .map(|index| &symbols[index])
}

/// `scip.FindOccurrences`: inside-out (largest range first) ordering.
pub fn find_occurrences<'a>(
    occurrences: &'a [scip::Occurrence],
    target_line: i32,
    target_character: i32,
) -> Vec<&'a scip::Occurrence> {
    let position = crate::git_tree_entry_graphql::Position {
        line: target_line,
        character: target_character,
    };
    let mut filtered: Vec<&scip::Occurrence> = occurrences
        .iter()
        .filter(|occurrence| range_contains(symbol_range_from_scip(&occurrence.range), position))
        .collect();
    // Ordered so that the least precise (largest) range comes last.
    filtered.sort_by(|a, b| {
        range_compare_strict(
            symbol_range_from_scip(&b.range),
            symbol_range_from_scip(&a.range),
        )
        .cmp(&0)
    });
    filtered
}

/// `scip.IsLocalSymbol`.
fn is_local_symbol(symbol: &str) -> bool {
    symbol.starts_with("local ")
}

/// `symbolHoverText`.
pub fn symbol_hover_text(symbol: &scip::SymbolInformation) -> Vec<String> {
    if let Some(signature) = &symbol.signature_documentation {
        if !signature.text.is_empty() && !signature.language.is_empty() {
            let mut out = vec![format!(
                "```{}\n{}\n```",
                signature.language, signature.text
            )];
            out.extend(symbol.documentation.iter().cloned());
            return out;
        }
    }
    symbol.documentation.clone()
}

/// `shared.NewUsageBuilder`.
pub fn new_usage_builder(occurrence: &scip::Occurrence) -> UsageBuilder {
    UsageBuilder {
        range: symbol_range_from_scip(&occurrence.range),
        symbol: occurrence.symbol.clone(),
        symbol_roles: occurrence.symbol_roles,
    }
}

/// `symbolRole.Matches`: `roles & bit > 0`.
pub fn occurrence_has_role(occurrence: &scip::Occurrence, role: i32) -> bool {
    occurrence.symbol_roles & role > 0
}

/// `extractOccurrenceData`.
pub fn extract_occurrence_data(
    document: &scip::Document,
    lookup_occurrence: &scip::Occurrence,
) -> ExtractedOccurrenceData {
    if lookup_occurrence.symbol.is_empty() {
        return ExtractedOccurrenceData {
            hover_text: lookup_occurrence.override_documentation.clone(),
            ..ExtractedOccurrenceData::default()
        };
    }

    let mut hover_text = Vec::new();
    let mut definition_symbol = lookup_occurrence.symbol.clone();
    let mut references_by_symbol: Vec<String> = Vec::new();
    let mut implementations_by_symbol: Vec<String> = Vec::new();
    let mut prototype_by_symbol: Vec<String> = Vec::new();
    let push_unique = |list: &mut Vec<String>, value: &str| {
        if !list.iter().any(|existing| existing == value) {
            list.push(value.to_owned());
        }
    };

    if let Some(lookup_symbol_info) =
        find_symbol_binary_search(&document.symbols, &lookup_occurrence.symbol)
    {
        hover_text = symbol_hover_text(lookup_symbol_info);
        for relationship in &lookup_symbol_info.relationships {
            if relationship.is_definition {
                definition_symbol = relationship.symbol.clone();
            }
            if relationship.is_reference {
                push_unique(&mut references_by_symbol, &relationship.symbol);
            }
            if relationship.is_implementation {
                push_unique(&mut prototype_by_symbol, &relationship.symbol);
            }
        }
    }

    for symbol in &document.symbols {
        for relationship in &symbol.relationships {
            if relationship.is_implementation && relationship.symbol == lookup_occurrence.symbol {
                push_unique(&mut implementations_by_symbol, &symbol.symbol);
            }
        }
    }

    let mut definitions = Vec::new();
    let mut references = Vec::new();
    let mut implementations = Vec::new();
    let mut prototypes = Vec::new();
    push_unique(&mut references_by_symbol, &lookup_occurrence.symbol);

    for occurrence in &document.occurrences {
        let is_definition = occurrence_has_role(occurrence, SYMBOL_ROLE_DEFINITION);

        if definition_symbol == occurrence.symbol && is_definition {
            definitions.push(new_usage_builder(occurrence));
        }
        if !is_definition && references_by_symbol.iter().any(|s| s == &occurrence.symbol) {
            references.push(new_usage_builder(occurrence));
        }
        if is_definition
            && implementations_by_symbol
                .iter()
                .any(|s| s == &occurrence.symbol)
            && definition_symbol != occurrence.symbol
        {
            implementations.push(new_usage_builder(occurrence));
        }
        if is_definition
            && prototype_by_symbol
                .iter()
                .any(|s| s == &occurrence.symbol)
        {
            prototypes.push(new_usage_builder(occurrence));
        }
    }

    if !lookup_occurrence.override_documentation.is_empty() {
        hover_text = lookup_occurrence.override_documentation.clone();
    }

    ExtractedOccurrenceData {
        definitions,
        references,
        implementations,
        prototypes,
        hover_text,
    }
}

/// `extractHoverData`.
pub fn extract_hover_data(
    document: &scip::Document,
    lookup_occurrence: &scip::Occurrence,
) -> Vec<String> {
    extract_occurrence_data(document, lookup_occurrence).hover_text
}

/// `store.GetStencil`: every occurrence range in the document.
pub fn get_stencil(document: &scip::Document) -> Vec<SymbolRange> {
    document
        .occurrences
        .iter()
        .map(|occurrence| symbol_range_from_scip(&occurrence.range))
        .collect()
}

/// One `shared.CodeIntelligenceRange`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CodeIntelligenceRange {
    pub range: SymbolRange,
    pub definitions: Vec<UsageBuilder>,
    pub references: Vec<UsageBuilder>,
    pub implementations: Vec<UsageBuilder>,
    pub hover_text: String,
}

/// `store.GetRanges`.
pub fn get_ranges(
    document: &scip::Document,
    start_line: i32,
    end_line: i32,
) -> Vec<CodeIntelligenceRange> {
    let mut ranges = Vec::new();
    for lookup_occurrence in &document.occurrences {
        let range = symbol_range_from_scip(&lookup_occurrence.range);
        if (start_line <= range.start.line && range.start.line < end_line)
            || (start_line <= range.end.line && range.end.line < end_line)
        {
            let data = extract_occurrence_data(document, lookup_occurrence);
            ranges.push(CodeIntelligenceRange {
                range,
                definitions: data.definitions,
                references: data.references,
                implementations: data.implementations,
                hover_text: data.hover_text.join("\n"),
            });
        }
    }
    ranges
}

/// `store.GetHover` (in-document symbol information path plus the same-index
/// fallback; cross-document fallback is handled by the caller).
pub fn get_hover(
    document: &scip::Document,
    line: i32,
    character: i32,
) -> Option<(String, SymbolRange)> {
    let occurrences = find_occurrences(&document.occurrences, line, character);
    for occurrence in &occurrences {
        let hover = extract_hover_data(document, occurrence);
        if !hover.is_empty() {
            return Some((
                hover.join("\n"),
                symbol_range_from_scip(&occurrence.range),
            ));
        }
    }

    // Slow path: match a symbol's own documentation in the same document.
    for occurrence in &occurrences {
        if occurrence.symbol.is_empty() || is_local_symbol(&occurrence.symbol) {
            continue;
        }
        if let Some(symbol) = find_symbol_binary_search(&document.symbols, &occurrence.symbol) {
            let text = symbol_hover_text(symbol);
            if !text.is_empty() {
                return Some((
                    text.join("\n"),
                    symbol_range_from_scip(&occurrence.range),
                ));
            }
        }
    }
    None
}

/// `store.GetDiagnostics` for a single document, mirroring the offset/limit
/// loop (the total count accumulates across all documents).
pub fn collect_diagnostics(
    document: &scip::Document,
    path: &str,
    offset: &mut i64,
    limit: i64,
    diagnostics: &mut Vec<DiagnosticRecord>,
) -> i64 {
    let mut total_count = 0i64;
    for occurrence in &document.occurrences {
        total_count += occurrence.diagnostics.len() as i64;
    }
    'occurrence_loop: for occurrence in &document.occurrences {
        if occurrence.diagnostics.is_empty() {
            continue;
        }
        let range = symbol_range_from_scip(&occurrence.range);
        for diagnostic in &occurrence.diagnostics {
            *offset -= 1;
            if *offset < 0 && (diagnostics.len() as i64) < limit {
                diagnostics.push(DiagnosticRecord {
                    path: path.to_owned(),
                    range,
                    severity: diagnostic.severity,
                    code: diagnostic.code.clone(),
                    message: diagnostic.message.clone(),
                    source: diagnostic.source.clone(),
                });
            } else {
                break 'occurrence_loop;
            }
        }
    }
    total_count
}

/// `toSeverity` (`root_resolver_diagnostics.go:60-74`).
pub fn to_severity(value: i32) -> Option<&'static str> {
    match value {
        1 => Some("ERROR"),
        2 => Some("WARNING"),
        3 => Some("INFORMATION"),
        4 => Some("HINT"),
        _ => None,
    }
}

/// `scipOccurrence.Roles` (`root_resolver_code_graph.go:120-135`).
pub fn occurrence_roles(symbol_roles: i32) -> Vec<&'static str> {
    let mut out = Vec::new();
    if symbol_roles & SYMBOL_ROLE_DEFINITION != 0 {
        out.push("DEFINITION");
    } else {
        out.push("REFERENCE");
    }
    if symbol_roles & SYMBOL_ROLE_FORWARD_DEFINITION != 0 {
        out.push("FORWARD_DEFINITION");
    }
    out
}

// ---------------------------------------------------------------------------
// Location sorting / range sorting (shared/types.go, codenav/utils.go)
// ---------------------------------------------------------------------------

/// `shared.SortAndDedupLocations`.
pub fn sort_and_dedup_locations(locations: Vec<UploadLocation>) -> Vec<UploadLocation> {
    let mut locations = locations;
    locations.sort_by(|a, b| {
        a.upload
            .repository_name
            .cmp(&b.upload.repository_name)
            .then_with(|| a.path.cmp(&b.path))
            // Newer uploads first.
            .then_with(|| b.upload.id.cmp(&a.upload.id))
            .then_with(|| {
                range_compare_strict(a.range, b.range).cmp(&0)
            })
    });
    let mut out: Vec<UploadLocation> = Vec::new();
    for location in locations {
        if let Some(previous) = out.last() {
            if previous.upload.repository_name == location.upload.repository_name
                && previous.path == location.path
            {
                continue;
            }
        }
        out.push(location);
    }
    out
}

/// `sortRanges`.
pub fn sort_ranges(ranges: Vec<SymbolRange>) -> Vec<SymbolRange> {
    let mut ranges = ranges;
    ranges.sort_by(|i, j| {
        let i_start = i.start;
        let j_start = j.start;
        if i_start.line < j_start.line {
            return std::cmp::Ordering::Less;
        } else if i_start.line > j_start.line {
            return std::cmp::Ordering::Greater;
        }
        if i_start.character < j_start.character {
            return std::cmp::Ordering::Less;
        } else if i_start.character > j_start.character {
            return std::cmp::Ordering::Greater;
        }
        let i_end = i.end;
        let j_end = j.end;
        if j_end.line < i_end.line {
            return std::cmp::Ordering::Greater;
        } else if j_start.line < j_end.line {
            return std::cmp::Ordering::Less;
        }
        if j_start.character < j_end.character {
            return std::cmp::Ordering::Less;
        }
        std::cmp::Ordering::Equal
    });
    ranges
}

/// `dedupeRanges`.
pub fn dedupe_ranges(ranges: Vec<SymbolRange>) -> Vec<SymbolRange> {
    let mut out: Vec<SymbolRange> = Vec::new();
    for range in ranges {
        if out.last() != Some(&range) {
            out.push(range);
        }
    }
    out
}

// ---------------------------------------------------------------------------
// GitTreeTranslator (pure part)
// ---------------------------------------------------------------------------

/// `compactHunk`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CompactHunk {
    pub orig_start_line: i32,
    pub orig_lines: i32,
    pub new_start_line: i32,
    pub new_lines: i32,
}

/// `newCompactHunk`.
pub fn new_compact_hunk(hunk: &sg_service_gitserver::diff::Hunk) -> CompactHunk {
    let mut orig_start_line = hunk.orig_start_line;
    if hunk.orig_lines == 0 {
        orig_start_line += 1;
    }
    let mut new_start_line = hunk.new_start_line;
    if hunk.new_lines == 0 {
        new_start_line += 1;
    }
    CompactHunk {
        orig_start_line,
        orig_lines: hunk.orig_lines,
        new_start_line,
        new_lines: hunk.new_lines,
    }
}

impl CompactHunk {
    /// `overlapsLine`.
    fn overlaps_line(&self, line: i32) -> bool {
        self.orig_start_line <= line + 1 && line + 1 < self.orig_start_line + self.orig_lines
    }

    /// `shiftLine`.
    fn shift_line(&self, line: i32) -> Option<i32> {
        if self.overlaps_line(line) {
            return None;
        }
        let original_span = self.orig_start_line + self.orig_lines;
        let new_span = self.new_start_line + self.new_lines;
        let new_line = line + new_span - original_span;
        (new_line >= 0).then_some(new_line)
    }

    /// `shiftPosition`.
    fn shift_position(
        &self,
        position: crate::git_tree_entry_graphql::Position,
    ) -> Option<crate::git_tree_entry_graphql::Position> {
        self.shift_line(position.line).map(|line| {
            crate::git_tree_entry_graphql::Position {
                line,
                character: position.character,
            }
        })
    }
}

/// `precedingHunk`.
pub fn preceding_hunk(hunks: &[CompactHunk], line: i32) -> Option<CompactHunk> {
    let line = line + 1; // diff hunks are 1-based
    // `slices.BinarySearchFunc` insertion point: first hunk with
    // `origStartLine >= line`.
    let index = hunks.partition_point(|hunk| hunk.orig_start_line < line);
    let found = hunks
        .get(index)
        .is_some_and(|hunk| hunk.orig_start_line == line);
    if index == 0 && !found {
        return None;
    }
    let index = if found { index } else { index - 1 };
    hunks.get(index).copied()
}

/// `translateLine`.
pub fn translate_line(hunks: &[CompactHunk], line: i32) -> Option<i32> {
    match preceding_hunk(hunks, line) {
        None => Some(line),
        Some(hunk) => hunk.shift_line(line),
    }
}

/// `translatePosition`.
pub fn translate_position(
    hunks: &[CompactHunk],
    position: crate::git_tree_entry_graphql::Position,
) -> Option<crate::git_tree_entry_graphql::Position> {
    match preceding_hunk(hunks, position.line) {
        None => Some(position),
        Some(hunk) => hunk.shift_position(position),
    }
}

/// `translateRange`.
pub fn translate_range(hunks: &[CompactHunk], range: SymbolRange) -> Option<SymbolRange> {
    if range.start.line == range.end.line {
        let new_line = translate_line(hunks, range.start.line)?;
        return Some(SymbolRange {
            start: crate::git_tree_entry_graphql::Position {
                line: new_line,
                character: range.start.character,
            },
            end: crate::git_tree_entry_graphql::Position {
                line: new_line,
                character: range.end.character,
            },
        });
    }
    let start = translate_position(hunks, range.start)?;
    let end = translate_position(hunks, range.end)?;
    Some(SymbolRange { start, end })
}

// ---------------------------------------------------------------------------
// Snapshot (codenav/service.go SnapshotForDocument)
// ---------------------------------------------------------------------------

/// `newLinemap`.
pub fn new_linemap(source: &str) -> Vec<usize> {
    let mut positions = vec![0usize];
    for (index, ch) in source.char_indices() {
        if ch == '\n' {
            positions.push(index + 1);
        }
    }
    let last_newline = *positions.last().expect("linemap starts at 0");
    let len_to_end = source.len() - last_newline;
    positions.push(last_newline + len_to_end + 1);
    positions
}

/// `SnapshotForDocument`'s per-occurrence loop. `translate` maps a position in
/// the requested commit to the upload's commit.
pub fn snapshot_for_document(
    document: &scip::Document,
    file: &str,
    translate: &dyn Fn(crate::git_tree_entry_graphql::Position) -> Option<crate::git_tree_entry_graphql::Position>,
) -> Vec<SnapshotData> {
    use sg_scip::symbol_formatter::SymbolFormatter;

    let linemap = new_linemap(file);
    let formatter = SymbolFormatter::lenient_verbose();
    let bytes = file.as_bytes();
    let mut symbols: BTreeMap<&str, &scip::SymbolInformation> = BTreeMap::new();
    for symbol in &document.symbols {
        // Go's `map[string]*SymbolInformation` overwrites duplicates.
        symbols.insert(symbol.symbol.as_str(), symbol);
    }

    let mut data = Vec::new();
    for occurrence in &document.occurrences {
        let formatted = formatter
            .format(&occurrence.symbol)
            .unwrap_or_else(|_| format!("error formatting {:?}", occurrence.symbol));
        let original_range = symbol_range_from_scip(&occurrence.range);
        let start_line = original_range.start.line;
        if start_line < 0 || start_line as usize >= linemap.len() {
            continue;
        }
        let line_offset = linemap[start_line as usize];
        let start_character = original_range.start.character.max(0) as usize;
        let line_end = line_offset.saturating_add(start_character);
        if line_end > bytes.len() {
            continue;
        }
        let line = &bytes[line_offset..line_end];
        let tab_count = line.iter().filter(|byte| **byte == b'\t').count() as i32;

        let padding = (original_range.start.character - tab_count) + tab_count * 4;
        let carets = (original_range.end.character - original_range.start.character).max(0);
        let mut snapshot = String::new();
        snapshot.push_str(&" ".repeat(padding.max(0) as usize));
        snapshot.push_str(&"^".repeat(carets as usize));
        snapshot.push(' ');
        if occurrence_has_role(occurrence, SYMBOL_ROLE_DEFINITION) {
            snapshot.push_str("definition");
        } else {
            snapshot.push_str("reference");
        }
        snapshot.push(' ');
        snapshot.push_str(&formatted);

        let mut additional_data = Vec::new();
        let is_definition = occurrence_has_role(occurrence, SYMBOL_ROLE_DEFINITION);
        if let Some(info) = symbols.get(occurrence.symbol.as_str()) {
            if is_definition {
                let mut relationships = info.relationships.clone();
                relationships.sort_by(|a, b| a.symbol.cmp(&b.symbol));
                for relationship in &relationships {
                    let formatted = formatter
                        .format(&relationship.symbol)
                        .unwrap_or_else(|_| format!("error formatting {:?}", occurrence.symbol));
                    let mut line = String::new();
                    line.push_str(&" ".repeat(padding.max(0) as usize));
                    line.push_str(&"^".repeat(carets as usize));
                    line.push_str(" relationship ");
                    line.push_str(&formatted);
                    if relationship.is_implementation {
                        line.push_str(" implementation");
                    }
                    if relationship.is_reference {
                        line.push_str(" reference");
                    }
                    if relationship.is_type_definition {
                        line.push_str(" type_definition");
                    }
                    additional_data.push(line);
                }
            }
        }

        let Some(new_position) = translate(original_range.start) else {
            continue;
        };
        let offset_index = (new_position.line + 1).max(0) as usize;
        let Some(offset) = linemap.get(offset_index) else {
            continue;
        };

        data.push(SnapshotData {
            document_offset: *offset as i32,
            symbol: snapshot,
            additional_data,
        });
    }
    data
}

// ---------------------------------------------------------------------------
// Database seam
// ---------------------------------------------------------------------------

/// Synchronous read seam for the codenav resolvers.
pub trait CodeIntelCodenavDatabase: Send + Sync {
    /// `InferClosestUploads` + path filtering.
    fn closest_uploads(
        &self,
        repository_id: i32,
        commit: &str,
        path: &str,
        exact_path: bool,
        tool_name: &str,
    ) -> Result<Vec<UploadRecord>, String>;

    /// `SCIPDocument` (gzip + protobuf) with cross-commit range translation.
    fn scip_document(
        &self,
        upload: &UploadRecord,
        target_commit: &str,
        repo_path: &str,
    ) -> Result<Option<scip::Document>, String>;

    /// `GetDiagnostics`'s document read (`path LIKE prefix || '%'`, LIMIT 1).
    fn documents_with_prefix(
        &self,
        upload: &UploadRecord,
        path_prefix: &str,
    ) -> Result<Vec<(String, scip::Document)>, String>;

    /// `gitserver.NewFileReader` for `SnapshotForDocument`.
    fn file_content(&self, repo_name: &str, commit: &str, path: &str) -> Result<Option<String>, String>;

    /// `GitTreeTranslator.TranslateRange(from, to, path, range)`.
    fn translate_range(
        &self,
        from: &str,
        to: &str,
        repository_id: i32,
        repository_name: &str,
        path: &str,
        range: SymbolRange,
    ) -> Result<Option<SymbolRange>, String>;

    /// `GitTreeTranslator.TranslatePosition(from, to, path, position)`.
    fn translate_position(
        &self,
        from: &str,
        to: &str,
        repository_id: i32,
        repository_name: &str,
        path: &str,
        position: crate::git_tree_entry_graphql::Position,
    ) -> Result<Option<crate::git_tree_entry_graphql::Position>, String>;

    /// `VisibleIndexes`: the `PreciseIndex` resolver for an upload.
    fn visible_index_field(
        &self,
        upload: &UploadRecord,
        actor: Actor,
    ) -> Result<Option<FieldValue>, String>;

    /// `Query.usagesForSymbol`.
    fn usages_for_symbol(
        &self,
        arguments: &Arguments,
        actor: Actor,
    ) -> Result<FieldValue, String>;

    /// Resolve a repository name to its id (for `RangeInput.repository`).
    fn repository_id_by_name(&self, name: &str) -> Result<Option<i32>, String>;
}

pub type CodeIntelCodenavHandle = Arc<dyn CodeIntelCodenavDatabase + Send + Sync>;

impl std::fmt::Debug for dyn CodeIntelCodenavDatabase + Send + Sync {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("CodeIntelCodenavDatabase")
    }
}

// ---------------------------------------------------------------------------
// Resolvers
// ---------------------------------------------------------------------------

fn string_field(value: String) -> FieldValue {
    FieldValue::Json(Json::Str(value))
}

fn int_field(value: i64) -> FieldValue {
    FieldValue::Json(Json::Int(value))
}

/// Build a `Location` node (`resolveLocation`).
fn location_object(
    repository_id: i32,
    repository_name: &str,
    commit: &str,
    path: &str,
    range: SymbolRange,
) -> LocationObject {
    LocationObject {
        resource: GitTreeEntryObject {
            repo: MinimalRepo {
                id: repository_id,
                name: repository_name.to_owned(),
            },
            oid: commit.to_owned(),
            input_rev: None,
            stat: sg_git::FileStat::synthetic(path, false),
            git: None,
        },
        range,
    }
}

/// A lazy `LocationConnection` (nodes + pageInfo).
pub struct LocationConnectionObject {
    pub nodes: Vec<LocationObject>,
    pub end_cursor: Option<String>,
}

impl LocationConnectionObject {
    fn new(nodes: Vec<LocationObject>, end_cursor: Option<String>) -> Self {
        Self { nodes, end_cursor }
    }
}

impl ObjectValue for LocationConnectionObject {
    fn type_name(&self) -> &'static str {
        "LocationConnection"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "nodes" => Ok(Some(FieldValue::List {
                items: self
                    .nodes
                    .iter()
                    .map(|node| {
                        FieldValue::Object(Arc::new(LocationObject {
                            resource: node.resource.clone(),
                            range: node.range,
                        }))
                    })
                    .collect(),
                element_type: Some("Location"),
            })),
            "pageInfo" => Ok(Some(FieldValue::Object(Arc::new(PageInfoObject::new(
                self.end_cursor.is_some(),
                self.end_cursor.clone(),
            ))))),
            _ => Ok(None),
        }
    }
}

/// `Hover`.
#[derive(Clone)]
pub struct HoverObject {
    pub markdown: String,
    pub range: SymbolRange,
}

impl ObjectValue for HoverObject {
    fn type_name(&self) -> &'static str {
        "Hover"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "markdown" => Ok(Some(string_field(self.markdown.clone()))),
            "range" => Ok(Some(FieldValue::Object(Arc::new(RangeObject {
                range: self.range,
            })))),
            _ => Ok(None),
        }
    }
}

/// `CodeIntelligenceRangeConnection`.
#[derive(Clone)]
pub struct CodeIntelligenceRangeConnectionObject {
    pub nodes: Vec<CodeIntelligenceRangeObject>,
}

impl ObjectValue for CodeIntelligenceRangeConnectionObject {
    fn type_name(&self) -> &'static str {
        "CodeIntelligenceRangeConnection"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "nodes" => Ok(Some(FieldValue::List {
                items: self
                    .nodes
                    .iter()
                    .cloned()
                    .map(|node| FieldValue::Object(Arc::new(node)))
                    .collect(),
                element_type: Some("CodeIntelligenceRange"),
            })),
            _ => Ok(None),
        }
    }
}

/// One adjusted `CodeIntelligenceRange`.
#[derive(Clone)]
pub struct CodeIntelligenceRangeObject {
    pub range: SymbolRange,
    pub definitions: Vec<LocationObject>,
    pub references: Vec<LocationObject>,
    pub implementations: Vec<LocationObject>,
    pub hover_text: String,
}

impl ObjectValue for CodeIntelligenceRangeObject {
    fn type_name(&self) -> &'static str {
        "CodeIntelligenceRange"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        let connection = |nodes: &[LocationObject]| {
            FieldValue::Object(Arc::new(LocationConnectionObject::new(
                nodes
                    .iter()
                    .map(|node| LocationObject {
                        resource: node.resource.clone(),
                        range: node.range,
                    })
                    .collect(),
                None,
            )))
        };
        match field {
            "range" => Ok(Some(FieldValue::Object(Arc::new(RangeObject {
                range: self.range,
            })))),
            "definitions" => Ok(Some(connection(&self.definitions))),
            "references" => Ok(Some(connection(&self.references))),
            "implementations" => Ok(Some(connection(&self.implementations))),
            "hover" => Ok(Some(FieldValue::Object(Arc::new(HoverObject {
                markdown: self.hover_text.clone(),
                range: self.range,
            })))),
            _ => Ok(None),
        }
    }
}

/// `Diagnostic`.
#[derive(Clone)]
pub struct DiagnosticObject {
    pub record: DiagnosticRecord,
    pub upload: UploadRecord,
    pub adjusted_commit: String,
    pub adjusted_range: SymbolRange,
}

impl ObjectValue for DiagnosticObject {
    fn type_name(&self) -> &'static str {
        "Diagnostic"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "location" => Ok(Some(FieldValue::Object(Arc::new(location_object(
                self.upload.repository_id,
                &self.upload.repository_name,
                &self.adjusted_commit,
                &self.record.path,
                self.adjusted_range,
            ))))),
            "severity" => Ok(Some(match to_severity(self.record.severity) {
                Some(severity) => string_field(severity.to_owned()),
                None => {
                    return Err(ExecError::new(format!(
                        "unknown diagnostic severity {}",
                        self.record.severity
                    )))
                }
            })),
            "code" => Ok(Some(non_zero_string(&self.record.code))),
            "source" => Ok(Some(non_zero_string(&self.record.source))),
            "message" => Ok(Some(non_zero_string(&self.record.message))),
            _ => Ok(None),
        }
    }
}

fn non_zero_string(value: &str) -> FieldValue {
    if value.is_empty() {
        FieldValue::Null
    } else {
        string_field(value.to_owned())
    }
}

/// `DiagnosticConnection`.
pub struct DiagnosticConnectionObject {
    pub nodes: Vec<DiagnosticObject>,
    pub total_count: i64,
}

impl ObjectValue for DiagnosticConnectionObject {
    fn type_name(&self) -> &'static str {
        "DiagnosticConnection"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "nodes" => Ok(Some(FieldValue::List {
                items: self
                    .nodes
                    .iter()
                    .cloned()
                    .map(|node| FieldValue::Object(Arc::new(node)))
                    .collect(),
                element_type: Some("Diagnostic"),
            })),
            "totalCount" => Ok(Some(int_field(self.total_count))),
            "pageInfo" => Ok(Some(FieldValue::Object(Arc::new(PageInfoObject::new(
                false, None,
            ))))),
            _ => Ok(None),
        }
    }
}

/// `SCIPOccurrence`.
#[derive(Clone)]
pub struct SCIPOccurrenceObject {
    pub occurrence: scip::Occurrence,
}

impl ObjectValue for SCIPOccurrenceObject {
    fn type_name(&self) -> &'static str {
        "SCIPOccurrence"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "symbol" => Ok(Some(string_field(self.occurrence.symbol.clone()))),
            "range" => Ok(Some(FieldValue::Object(Arc::new(RangeObject {
                range: symbol_range_from_scip(&self.occurrence.range),
            })))),
            "roles" => Ok(Some(FieldValue::List {
                items: occurrence_roles(self.occurrence.symbol_roles)
                    .into_iter()
                    .map(|role| string_field(role.to_owned()))
                    .collect(),
                element_type: None,
            })),
            _ => Ok(None),
        }
    }
}

/// `SCIPOccurrenceConnection`.
pub struct SCIPOccurrenceConnectionObject {
    pub nodes: Vec<SCIPOccurrenceObject>,
    pub has_next_page: bool,
    pub end_cursor: Option<String>,
}

impl ObjectValue for SCIPOccurrenceConnectionObject {
    fn type_name(&self) -> &'static str {
        "SCIPOccurrenceConnection"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "nodes" => Ok(Some(FieldValue::List {
                items: self
                    .nodes
                    .iter()
                    .cloned()
                    .map(|node| FieldValue::Object(Arc::new(node)))
                    .collect(),
                element_type: Some("SCIPOccurrence"),
            })),
            "pageInfo" => Ok(Some(FieldValue::Object(Arc::new(PageInfoObject::new(
                self.has_next_page,
                self.end_cursor.clone(),
            ))))),
            _ => Ok(None),
        }
    }
}

/// `CodeGraphToolInfo`.
#[derive(Clone)]
pub struct CodeGraphToolInfoObject {
    pub name: String,
    pub version: String,
}

impl ObjectValue for CodeGraphToolInfoObject {
    fn type_name(&self) -> &'static str {
        "CodeGraphToolInfo"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "name" => Ok(Some(string_field(self.name.clone()))),
            "version" => Ok(Some(string_field(self.version.clone()))),
            _ => Ok(None),
        }
    }
}

/// `CodeGraphData` (`codeGraphDataResolver`).
#[derive(Clone)]
pub struct CodeGraphDataObject {
    pub upload: UploadRecord,
    pub provenance: &'static str,
    pub document: Option<scip::Document>,
    pub document_error: Option<String>,
}

impl CodeGraphDataObject {
    fn occurrences(&self, arguments: &Arguments) -> Result<FieldValue, ExecError> {
        if let Some(error) = &self.document_error {
            return Err(ExecError::new(error.clone()));
        }
        let document = self
            .document
            .as_ref()
            .ok_or_else(|| ExecError::new("document not found"))?;
        let first = arguments
            .i64("first")
            .map(|first| first.clamp(0, MAX_OCCURRENCES_PAGE_SIZE))
            .unwrap_or(MAX_OCCURRENCES_PAGE_SIZE);
        let offset = match arguments.string("after") {
            None => 0,
            Some(value) => unmarshal_occurrence_cursor(value).map_err(ExecError::new)?,
        };
        let total = document.occurrences.len();
        let start = offset.min(total);
        let end = start.saturating_add(first as usize).min(total);
        let nodes: Vec<SCIPOccurrenceObject> = document.occurrences[start..end]
            .iter()
            .cloned()
            .map(|occurrence| SCIPOccurrenceObject { occurrence })
            .collect();
        let has_next_page = end < total;
        let end_cursor = if has_next_page && !nodes.is_empty() {
            Some(marshal_occurrence_cursor(end - 1))
        } else {
            None
        };
        Ok(FieldValue::Object(Arc::new(
            SCIPOccurrenceConnectionObject {
                nodes,
                has_next_page,
                end_cursor,
            },
        )))
    }
}

impl ObjectValue for CodeGraphDataObject {
    fn type_name(&self) -> &'static str {
        "CodeGraphData"
    }

    fn field(
        &self,
        field: &str,
        arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "id" => {
                let json = serde_json::json!({
                    "UploadID": self.upload.id,
                    "Commit": self.upload.commit,
                    "Root": self.upload.root,
                    "Indexer": self.upload.indexer,
                    "IndexerVersion": self.upload.indexer_version,
                    "RepoID": self.upload.repository_id,
                    "Path": self.upload.root,
                    "CodeGraphDataProvenance": self.provenance,
                })
                .to_string();
                Ok(Some(string_field(format!(
                    "CodeGraphData:{}",
                    base64::engine::general_purpose::URL_SAFE.encode(json)
                ))))
            }
            "provenance" => Ok(Some(string_field(self.provenance.to_owned()))),
            "commit" => Ok(Some(string_field(self.upload.commit.clone()))),
            "toolInfo" => Ok(Some(FieldValue::Object(Arc::new(
                CodeGraphToolInfoObject {
                    name: self.upload.indexer.clone(),
                    version: self.upload.indexer_version.clone(),
                },
            )))),
            "occurrences" => Ok(Some(self.occurrences(arguments)?)),
            _ => Ok(None),
        }
    }
}

/// `SnapshotData`.
#[derive(Clone)]
pub struct SnapshotDataObject {
    pub data: SnapshotData,
}

impl ObjectValue for SnapshotDataObject {
    fn type_name(&self) -> &'static str {
        "SnapshotData"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "offset" => Ok(Some(int_field(i64::from(self.data.document_offset)))),
            "data" => Ok(Some(string_field(self.data.symbol.clone()))),
            // Go returns a pointer to a nil slice when there are no
            // relationships, which graph-gophers marshals as `null`.
            "additional" => Ok(Some(if self.data.additional_data.is_empty() {
                FieldValue::Null
            } else {
                FieldValue::List {
                    items: self
                        .data
                        .additional_data
                        .iter()
                        .cloned()
                        .map(string_field)
                        .collect(),
                    element_type: Some("String"),
                }
            })),
            _ => Ok(None),
        }
    }
}

/// `LineRange`.
#[derive(Clone)]
pub struct LineRangeObject {
    pub range: SymbolRange,
}

impl ObjectValue for LineRangeObject {
    fn type_name(&self) -> &'static str {
        "LineRange"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "line" => Ok(Some(int_field(i64::from(self.range.start.line)))),
            "character" => Ok(Some(int_field(i64::from(self.range.start.character)))),
            "length" => Ok(Some(int_field(i64::from(
                self.range.end.character - self.range.start.character,
            )))),
            _ => Ok(None),
        }
    }
}

/// `SymbolLocation`.
#[derive(Clone)]
pub struct SymbolLocationObject {
    pub repo: String,
    pub commit: String,
    pub path: String,
    pub range: Option<SymbolRange>,
}

impl ObjectValue for SymbolLocationObject {
    fn type_name(&self) -> &'static str {
        "SymbolLocation"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        let line = self.range.map(|range| range.start.line).unwrap_or(0);
        let character = self.range.map(|range| range.start.character).unwrap_or(0);
        let length = self
            .range
            .map(|range| range.end.character - range.start.character)
            .unwrap_or(0);
        match field {
            "repo" => Ok(Some(string_field(self.repo.clone()))),
            "commit" => Ok(Some(string_field(self.commit.clone()))),
            "path" => Ok(Some(string_field(self.path.clone()))),
            "range" => Ok(Some(match self.range {
                Some(range) => FieldValue::Object(Arc::new(LineRangeObject { range })),
                None => FieldValue::Null,
            })),
            "line" => Ok(Some(int_field(i64::from(line)))),
            "character" => Ok(Some(int_field(i64::from(character)))),
            "length" => Ok(Some(int_field(i64::from(length)))),
            _ => Ok(None),
        }
    }
}

/// `SymbolInfo`.
#[derive(Clone)]
pub struct SymbolInfoObject {
    pub definition: Option<SymbolLocationObject>,
    pub hover: Option<String>,
}

impl ObjectValue for SymbolInfoObject {
    fn type_name(&self) -> &'static str {
        "SymbolInfo"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "definition" => Ok(Some(match &self.definition {
                Some(definition) => FieldValue::Object(Arc::new(definition.clone())),
                None => FieldValue::Null,
            })),
            "hover" => Ok(Some(match &self.hover {
                Some(hover) => string_field(hover.clone()),
                None => FieldValue::Null,
            })),
            _ => Ok(None),
        }
    }
}

/// `SymbolInformation`.
#[derive(Clone)]
pub struct SymbolInformationObject {
    pub name: String,
}

impl ObjectValue for SymbolInformationObject {
    fn type_name(&self) -> &'static str {
        "SymbolInformation"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "name" => Ok(Some(string_field(self.name.clone()))),
            // Go's `symbolInformationResolver.Documentation` panics
            // ("implement me"); there is no storage/service path.
            "documentation" => Ok(None),
            _ => Ok(None),
        }
    }
}

/// `UsageRange`.
#[derive(Clone)]
pub struct UsageRangeObject {
    pub repository: String,
    pub revision: String,
    pub path: String,
    pub range: SymbolRange,
}

impl ObjectValue for UsageRangeObject {
    fn type_name(&self) -> &'static str {
        "UsageRange"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "repository" => Ok(Some(string_field(self.repository.clone()))),
            "revision" => Ok(Some(string_field(self.revision.clone()))),
            "path" => Ok(Some(string_field(self.path.clone()))),
            "range" => Ok(Some(FieldValue::Object(Arc::new(RangeObject {
                range: self.range,
            })))),
            _ => Ok(None),
        }
    }
}

/// `Usage`.
#[derive(Clone)]
pub struct UsageObject {
    pub symbol: Option<String>,
    pub provenance: &'static str,
    pub surrounding_content: String,
    pub usage_range: UsageRangeObject,
    pub kind: UsageKind,
}

impl ObjectValue for UsageObject {
    fn type_name(&self) -> &'static str {
        "Usage"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "symbol" => Ok(Some(match &self.symbol {
                Some(name) => FieldValue::Object(Arc::new(SymbolInformationObject {
                    name: name.clone(),
                })),
                None => FieldValue::Null,
            })),
            "provenance" => Ok(Some(string_field(self.provenance.to_owned()))),
            // Go panics ("implement me"); leave unported.
            "dataSource" => Ok(None),
            "usageRange" => Ok(Some(FieldValue::Object(Arc::new(
                self.usage_range.clone(),
            )))),
            "surroundingContent" => Ok(Some(string_field(self.surrounding_content.clone()))),
            "usageKind" => Ok(Some(string_field(self.kind.graphql_name().to_owned()))),
            _ => Ok(None),
        }
    }
}

/// `UsageConnection`.
pub struct UsageConnectionObject {
    pub nodes: Vec<UsageObject>,
    pub has_next_page: bool,
    pub end_cursor: Option<String>,
}

impl ObjectValue for UsageConnectionObject {
    fn type_name(&self) -> &'static str {
        "UsageConnection"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "nodes" => Ok(Some(FieldValue::List {
                items: self
                    .nodes
                    .iter()
                    .cloned()
                    .map(|node| FieldValue::Object(Arc::new(node)))
                    .collect(),
                element_type: Some("Usage"),
            })),
            "pageInfo" => Ok(Some(FieldValue::Object(Arc::new(PageInfoObject::new(
                self.has_next_page,
                self.end_cursor.clone(),
            ))))),
            _ => Ok(None),
        }
    }
}

/// Build a `Location` from an upload-relative usage/location.
fn location_from_upload_location(location: &UploadLocation) -> LocationObject {
    location_object(
        location.upload.repository_id,
        &location.upload.repository_name,
        &location.target_commit,
        &location.path,
        location.range,
    )
}

// ---------------------------------------------------------------------------
// GitBlob / GitTree field entry points
// ---------------------------------------------------------------------------

/// `GitBlobLSIFData` (`gitBlobLSIFDataResolver`).
pub struct GitBlobLSIFDataObject {
    pub db: CodeIntelCodenavHandle,
    pub repo: MinimalRepo,
    pub commit: String,
    pub path: String,
    pub exact_path: bool,
    pub uploads: Vec<UploadRecord>,
}

impl GitBlobLSIFDataObject {
    fn document(
        &self,
        upload: &UploadRecord,
    ) -> Result<Option<scip::Document>, String> {
        self.db.scip_document(upload, &self.commit, &self.path)
    }

    fn adjust_range(
        &self,
        upload: &UploadRecord,
        range: SymbolRange,
    ) -> Result<Option<SymbolRange>, String> {
        self.db.translate_range(
            &upload.commit,
            &self.commit,
            upload.repository_id,
            &upload.repository_name,
            &self.path,
            range,
        )
    }

    fn upload_usage_locations(
        &self,
        usages: Vec<UsageBuilder>,
        upload: &UploadRecord,
        kind: UsageKind,
    ) -> Result<Vec<UploadLocation>, String> {
        let mut locations = Vec::new();
        for usage in usages {
            let Some(adjusted) = self.adjust_range(upload, usage.range)? else {
                continue;
            };
            locations.push(UploadLocation {
                upload: upload.clone(),
                path: self.path.clone(),
                target_commit: self.commit.clone(),
                range: adjusted,
            });
        }
        let _ = kind;
        Ok(sort_and_dedup_locations(locations))
    }

}

impl ObjectValue for GitBlobLSIFDataObject {
    fn type_name(&self) -> &'static str {
        "GitBlobLSIFData"
    }

    fn field(
        &self,
        field: &str,
        arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "stencil" => {
                let mut ranges = Vec::new();
                for upload in &self.uploads {
                    if let Some(document) = self.document(upload).map_err(ExecError::new)? {
                        for range in get_stencil(&document) {
                            if let Some(adjusted) =
                                self.adjust_range(upload, range).map_err(ExecError::new)?
                            {
                                ranges.push(adjusted);
                            }
                        }
                    }
                }
                Ok(Some(FieldValue::List {
                    items: sort_ranges(ranges)
                        .into_iter()
                        .map(|range| FieldValue::Object(Arc::new(RangeObject { range })))
                        .collect(),
                    element_type: Some("Range"),
                }))
            }
            "ranges" => {
                let start_line = arguments.i64("startLine").unwrap_or(0) as i32;
                let end_line = arguments.i64("endLine").unwrap_or(0) as i32;
                if start_line < 0 || end_line < start_line {
                    return Err(ExecError::new(ERR_ILLEGAL_BOUNDS));
                }
                let mut nodes = Vec::new();
                for upload in &self.uploads {
                    let Some(document) = self.document(upload).map_err(ExecError::new)? else {
                        continue;
                    };
                    for range in get_ranges(&document, start_line, end_line) {
                        let Some(adjusted) =
                            self.adjust_range(upload, range.range).map_err(ExecError::new)?
                        else {
                            continue;
                        };
                        let definitions = self
                            .upload_usage_locations(
                                range.definitions,
                                upload,
                                UsageKind::Definition,
                            )
                            .map_err(ExecError::new)?;
                        let references = self
                            .upload_usage_locations(
                                range.references,
                                upload,
                                UsageKind::Reference,
                            )
                            .map_err(ExecError::new)?;
                        let implementations = self
                            .upload_usage_locations(
                                range.implementations,
                                upload,
                                UsageKind::Implementation,
                            )
                            .map_err(ExecError::new)?;
                        nodes.push(CodeIntelligenceRangeObject {
                            range: adjusted,
                            definitions: definitions
                                .iter()
                                .map(location_from_upload_location)
                                .collect(),
                            references: references
                                .iter()
                                .map(location_from_upload_location)
                                .collect(),
                            implementations: implementations
                                .iter()
                                .map(location_from_upload_location)
                                .collect(),
                            hover_text: range.hover_text,
                        });
                    }
                }
                Ok(Some(FieldValue::Object(Arc::new(
                    CodeIntelligenceRangeConnectionObject { nodes },
                ))))
            }
            "definitions" => {
                let line = arguments.i64("line").unwrap_or(0) as i32;
                let character = arguments.i64("character").unwrap_or(0) as i32;
                let filter = arguments.string("filter");
                let mut all = Vec::new();
                for upload in &self.uploads {
                    let Some(document) = self.document(upload).map_err(ExecError::new)? else {
                        continue;
                    };
                    for occurrence in find_occurrences(&document.occurrences, line, character) {
                        let data = extract_occurrence_data(&document, occurrence);
                        for builder in data.definitions {
                            if let Some(filter) = filter {
                                if !filter.is_empty() && !self.path.contains(filter) {
                                    continue;
                                }
                            }
                            let Some(adjusted) = self
                                .adjust_range(upload, builder.range)
                                .map_err(ExecError::new)?
                            else {
                                continue;
                            };
                            all.push(UploadLocation {
                                upload: upload.clone(),
                                path: self.path.clone(),
                                target_commit: self.commit.clone(),
                                range: adjusted,
                            });
                        }
                        // Only the first matching occurrence contributes.
                        break;
                    }
                }
                let locations = sort_and_dedup_locations(all);
                Ok(Some(FieldValue::Object(Arc::new(LocationConnectionObject::new(
                    locations.iter().map(location_from_upload_location).collect(),
                    None,
                )))))
            }
            "references" | "implementations" | "prototypes" => {
                self.paged_locations(field, arguments)
            }
            "hover" => {
                let line = arguments.i64("line").unwrap_or(0) as i32;
                let character = arguments.i64("character").unwrap_or(0) as i32;
                for upload in &self.uploads {
                    let Some(document) = self.document(upload).map_err(ExecError::new)? else {
                        continue;
                    };
                    if let Some((text, range)) = get_hover(&document, line, character) {
                        let Some(adjusted) =
                            self.adjust_range(upload, range).map_err(ExecError::new)?
                        else {
                            continue;
                        };
                        return Ok(Some(FieldValue::Object(Arc::new(HoverObject {
                            markdown: text,
                            range: adjusted,
                        }))));
                    }
                }
                Ok(Some(FieldValue::Null))
            }
            "diagnostics" => self.diagnostics_field(arguments),
            "visibleIndexes" => {
                let mut items = Vec::new();
                for upload in &self.uploads {
                    if let Some(value) = self
                        .db
                        .visible_index_field(upload, _actor)
                        .map_err(ExecError::new)?
                    {
                        items.push(value);
                    }
                }
                Ok(Some(FieldValue::List {
                    items,
                    element_type: Some("PreciseIndex"),
                }))
            }
            "snapshot" => self.snapshot_field(arguments),
            _ => Ok(None),
        }
    }
}

impl GitBlobLSIFDataObject {
    fn paged_locations(
        &self,
        field: &str,
        arguments: &Arguments,
    ) -> Result<Option<FieldValue>, ExecError> {
        let limit = arguments
            .i64("first")
            .unwrap_or(match field {
                "references" => DEFAULT_REFERENCES_PAGE_SIZE,
                _ => DEFAULT_IMPLEMENTATIONS_PAGE_SIZE,
            });
        if limit <= 0 {
            return Err(ExecError::new(ERR_ILLEGAL_LIMIT));
        }
        let raw_cursor = decode_cursor(arguments.string("after")).map_err(ExecError::new)?;
        let cursor = PreciseCursor::decode(&raw_cursor).map_err(ExecError::new)?;
        let line = arguments.i64("line").unwrap_or(0) as i32;
        let character = arguments.i64("character").unwrap_or(0) as i32;
        let filter = arguments.string("filter");

        let mut builders: Vec<(UploadRecord, Vec<UsageBuilder>)> = Vec::new();
        for upload in &self.uploads {
            let Some(document) = self.document(upload).map_err(ExecError::new)? else {
                continue;
            };
            let Some(lookup) = find_occurrences(&document.occurrences, line, character)
                .into_iter()
                .next()
            else {
                continue;
            };
            let data = extract_occurrence_data(&document, lookup);
            let selected = match field {
                "references" => data.references,
                "implementations" => data.implementations,
                _ => data.prototypes,
            };
            let selected: Vec<UsageBuilder> = selected
                .into_iter()
                .filter(|_builder| match filter {
                    Some(filter) if !filter.is_empty() => self.path.contains(filter),
                    _ => true,
                })
                .collect();
            builders.push((upload.clone(), selected));
        }

        let offset = (cursor.local_upload_offset.max(0) as usize)
            .saturating_add(cursor.local_location_offset.max(0) as usize);
        let mut all_locations = Vec::new();
        for (upload, selected) in builders {
            let adjusted = self
                .upload_usage_locations(selected, &upload, UsageKind::Reference)
                .map_err(ExecError::new)?;
            all_locations.extend(adjusted);
        }
        let total = all_locations.len();
        let start = offset.min(total);
        let end = start.saturating_add(limit as usize).min(total);
        let page = &all_locations[start..end];

        let mut next_cursor = None;
        if end < total {
            let next = PreciseCursor {
                phase: "local".to_owned(),
                local_upload_offset: end as i32,
                local_location_offset: 0,
            };
            next_cursor = Some(encode_cursor(Some(&next.encode())));
        }
        Ok(Some(FieldValue::Object(Arc::new(
            LocationConnectionObject::new(
                page.iter().map(location_from_upload_location).collect(),
                next_cursor,
            ),
        ))))
    }

    fn diagnostics_field(
        &self,
        arguments: &Arguments,
    ) -> Result<Option<FieldValue>, ExecError> {
        let limit = arguments
            .i64("first")
            .unwrap_or(DEFAULT_DIAGNOSTICS_PAGE_SIZE);
        if limit <= 0 {
            return Err(ExecError::new(ERR_ILLEGAL_LIMIT));
        }
        let mut offset = 0i64;
        let mut records = Vec::new();
        let mut total_count = 0i64;
        let mut nodes = Vec::new();
        for upload in &self.uploads {
            let documents = self
                .db
                .documents_with_prefix(upload, &self.path)
                .map_err(ExecError::new)?;
            for (path, document) in documents {
                total_count += collect_diagnostics(&document, &path, &mut offset, limit, &mut records);
            }
        }
        if (records.len() as i64) > limit {
            records.truncate(limit as usize);
        }
        for record in records {
            let upload = self
                .uploads
                .iter()
                .find(|upload| record.path.starts_with(&upload.root))
                .cloned()
                .unwrap_or_else(|| self.uploads[0].clone());
            let adjusted = self
                .adjust_range(&upload, record.range)
                .map_err(ExecError::new)?
                .unwrap_or(record.range);
            nodes.push(DiagnosticObject {
                record,
                upload,
                adjusted_commit: self.commit.clone(),
                adjusted_range: adjusted,
            });
        }
        Ok(Some(FieldValue::Object(Arc::new(
            DiagnosticConnectionObject {
                nodes,
                total_count,
            },
        ))))
    }

    fn snapshot_field(&self, arguments: &Arguments) -> Result<Option<FieldValue>, ExecError> {
        let Some(index_id) = arguments.string("indexID") else {
            return Err(ExecError::new("indexID is required"));
        };
        let upload_id = decode_precise_index_upload_id(index_id)
            .ok_or_else(|| ExecError::new("invalid identifier"))?;
        let Some(upload) = self
            .uploads
            .iter()
            .find(|upload| upload.id == upload_id)
        else {
            return Ok(Some(FieldValue::List {
                items: Vec::new(),
                element_type: Some("SnapshotData"),
            }));
        };
        let Some(document) = self.document(upload).map_err(ExecError::new)? else {
            return Err(ExecError::new("no document found"));
        };
        let Some(file) = self
            .db
            .file_content(&upload.repository_name, &upload.commit, &self.path)
            .map_err(ExecError::new)?
        else {
            return Err(ExecError::new("no document found"));
        };
        let file = file.replace("\r\n", "\n");
        let db = self.db.clone();
        let upload = upload.clone();
        let commit = self.commit.clone();
        let path = self.path.clone();
        let translate = move |position: crate::git_tree_entry_graphql::Position| {
            db.translate_position(
                &commit,
                &upload.commit,
                upload.repository_id,
                &upload.repository_name,
                &path,
                position,
            )
            .ok()
            .flatten()
        };
        let data = snapshot_for_document(&document, &file, &translate);
        Ok(Some(FieldValue::List {
            items: data
                .into_iter()
                .map(|data| FieldValue::Object(Arc::new(SnapshotDataObject { data })))
                .collect(),
            element_type: Some("SnapshotData"),
        }))
    }
}

/// `uploadgraphql.UnmarshalPreciseIndexGQLID`'s upload component (`U<id>` or
/// the relay JSON form).
pub fn decode_precise_index_upload_id(value: &str) -> Option<i64> {
    if let Some(raw) = value.strip_prefix('U') {
        return raw.trim_start_matches(':').parse().ok();
    }
    let decoded = base64::engine::general_purpose::URL_SAFE
        .decode(value)
        .ok()?;
    let decoded = String::from_utf8(decoded).ok()?;
    let (_kind, payload) = decoded.split_once(':')?;
    // `relay.UnmarshalSpec` JSON-decodes the payload into the `U:<id>:I:<id>`
    // spec string (`codeintel_uploads_graphql::decode_precise_index_id`).
    let spec: String = serde_json::from_str(payload).ok()?;
    let parts: Vec<&str> = spec.split(':').collect();
    let mut index = 0;
    while index + 1 < parts.len() {
        if parts[index] == "U" {
            return parts[index + 1].parse().ok();
        }
        index += 2;
    }
    None
}

/// `GitTreeLSIFData` (`TreeEntryLSIFData` for directories).
pub struct GitTreeLSIFDataObject {
    pub inner: GitBlobLSIFDataObject,
}

impl ObjectValue for GitTreeLSIFDataObject {
    fn type_name(&self) -> &'static str {
        "GitTreeLSIFData"
    }

    fn field(
        &self,
        field: &str,
        arguments: &Arguments,
        actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "diagnostics" => self.inner.field("diagnostics", arguments, actor),
            _ => Ok(None),
        }
    }
}

/// `GitBlob.lsif` / `GitTree.lsif` / `TreeEntry.lsif`.
pub fn lsif_field(
    handle: Option<&CodeIntelCodenavHandle>,
    repo: MinimalRepo,
    commit: &str,
    path: &str,
    is_dir: bool,
    arguments: &Arguments,
) -> Result<Option<FieldValue>, ExecError> {
    let Some(db) = handle else {
        return Ok(Some(FieldValue::Null));
    };
    let tool_name = arguments.string("toolName").unwrap_or("").to_owned();
    let uploads = db
        .closest_uploads(repo.id, commit, path, !is_dir, &tool_name)
        .map_err(ExecError::new)?;
    if uploads.is_empty() {
        return Ok(Some(FieldValue::Null));
    }
    let object = GitBlobLSIFDataObject {
        db: db.clone(),
        repo,
        commit: commit.to_owned(),
        path: path.to_owned(),
        exact_path: !is_dir,
        uploads,
    };
    if is_dir {
        Ok(Some(FieldValue::Object(Arc::new(GitTreeLSIFDataObject {
            inner: object,
        }))))
    } else {
        Ok(Some(FieldValue::Object(Arc::new(object))))
    }
}

/// `GitBlob.codeGraphData(filter:)`.
pub fn code_graph_data_field(
    handle: Option<&CodeIntelCodenavHandle>,
    repo: MinimalRepo,
    commit: &str,
    path: &str,
    arguments: &Arguments,
) -> Result<Option<FieldValue>, ExecError> {
    let Some(db) = handle else {
        return Ok(Some(FieldValue::List {
            items: Vec::new(),
            element_type: Some("CodeGraphData"),
        }));
    };
    let requested = requested_provenances(arguments);
    let mut out = Vec::new();
    for provenance in requested {
        let tool_name = if provenance == "SYNTACTIC" {
            SYNTACTIC_INDEXER
        } else {
            ""
        };
        let uploads = db
            .closest_uploads(repo.id, commit, path, true, tool_name)
            .map_err(ExecError::new)?;
        if uploads.is_empty() {
            continue;
        }
        let filtered = prefer_uploads_with_longest_roots(uploads);
        for upload in filtered {
            let (document, document_error) =
                match db.scip_document(&upload, commit, path) {
                    Ok(document) => (document, None),
                    Err(error) => (None, Some(error)),
                };
            out.push(FieldValue::Object(Arc::new(CodeGraphDataObject {
                upload,
                provenance,
                document,
                document_error,
            })));
        }
        if !out.is_empty() {
            break;
        }
    }
    let _ = path;
    Ok(Some(FieldValue::List {
        items: out,
        element_type: Some("CodeGraphData"),
    }))
}

fn requested_provenances(arguments: &Arguments) -> Vec<&'static str> {
    let filter = arguments.get("filter");
    let equals = filter
        .and_then(|filter| match filter {
            InputValue::Object(map) => map.get("provenance"),
            _ => None,
        })
        .and_then(|provenance| match provenance {
            InputValue::Object(map) => map.get("equals"),
            _ => None,
        })
        .and_then(InputValue::as_str);
    match equals {
        Some("PRECISE") => vec!["PRECISE"],
        Some("SYNTACTIC") => vec!["SYNTACTIC"],
        Some("SEARCH_BASED") => Vec::new(),
        _ => vec!["PRECISE", "SYNTACTIC"],
    }
}

/// `preferUploadsWithLongestRoots`.
pub fn prefer_uploads_with_longest_roots(uploads: Vec<UploadRecord>) -> Vec<UploadRecord> {
    let mut order: Vec<(String, usize)> = Vec::new();
    let mut chosen: Vec<Option<UploadRecord>> = Vec::new();
    for upload in uploads {
        let key = format!("{}:{}", upload.indexer, upload.commit);
        if let Some(position) = order.iter().position(|(existing, _)| existing == &key) {
            let index = order[position].1;
            if let Some(current) = &chosen[index] {
                if current.root.len() < upload.root.len() {
                    chosen[index] = Some(upload);
                }
            }
        } else {
            let index = chosen.len();
            order.push((key, index));
            chosen.push(Some(upload));
        }
    }
    chosen.into_iter().flatten().collect()
}

/// `GitBlob.symbolInfo` / `GitBlob.localCodeIntel` are service gaps; this
/// returns the unassembled (`null`) shape for callers that still expose them.
pub fn symbol_info_gap() -> FieldValue {
    FieldValue::Null
}

/// Render the `CodeIntelGitBlob` resource for a location (used by tests).
pub fn code_intel_resource(entry: GitTreeEntryObject) -> CodeIntelGitBlobObject {
    CodeIntelGitBlobObject(entry)
}

/// `rangeResolver.urlFragment` (shared with `git_tree_entry_graphql`).
pub fn range_url(range: SymbolRange) -> String {
    range_url_fragment(range)
}


#[cfg(test)]
mod tests {
    use super::*;

    /// The opt-in document set assembles exactly like `NewSchema`'s
    /// `codeintel*.graphql` batch.
    #[test]
    fn codenav_document_assembles() {
        let schema = sg_graphql::schema::assemble(&[
            sg_graphql::schema::MAIN_SCHEMA,
            sg_graphql::schema::CODEINTEL_AUTOINDEXING_SCHEMA,
            sg_graphql::schema::CODEINTEL_POLICIES_SCHEMA,
            sg_graphql::schema::CODEINTEL_CODENAV_SCHEMA,
        ])
        .expect("codenav SDL must assemble");
        assert!(schema.types.contains_key("GitBlobLSIFData"));
        assert!(schema.types.contains_key("GitTreeLSIFData"));
        assert!(schema.types.contains_key("CodeGraphData"));
        assert!(schema.types.contains_key("TreeEntryLSIFData"));
        assert!(schema.types["GitBlob"].fields.iter().any(|f| f.name == "lsif"));
        assert!(schema.types["Query"]
            .fields
            .iter()
            .any(|f| f.name == "usagesForSymbol"));
    }

    #[test]
    fn translate_matches_compact_hunk_semantics() {
        let hunks = vec![CompactHunk {
            orig_start_line: 3,
            orig_lines: 0,
            new_start_line: 3,
            new_lines: 2,
        }];
        // Line 2 (0-based) is after the insertion point.
        assert_eq!(
            translate_line(&hunks, 2),
            Some(4)
        );
        // A modified line is dropped.
        let hunks = vec![CompactHunk {
            orig_start_line: 2,
            orig_lines: 1,
            new_start_line: 2,
            new_lines: 1,
        }];
        assert_eq!(translate_line(&hunks, 1), None);
    }

    #[test]
    fn occurrence_roles_follow_go() {
        assert_eq!(occurrence_roles(0), vec!["REFERENCE"]);
        assert_eq!(occurrence_roles(1), vec!["DEFINITION"]);
        assert_eq!(
            occurrence_roles(65),
            vec!["DEFINITION", "FORWARD_DEFINITION"]
        );
    }
}
