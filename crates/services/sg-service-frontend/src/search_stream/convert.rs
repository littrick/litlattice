//! `internal/search/result` + `internal/search/type_converters.go`: the
//! result model the handler converts into streaming events.
//!
//! Only the fields the stream uses are modelled. `FromMatch` is a pure
//! function over the model, so the search backend can stay unported while the
//! event conversion is compared against the Go baseline.

use std::collections::BTreeMap;
use std::collections::HashMap;

use time::{Duration, OffsetDateTime};

use super::events::{
    ChunkMatchEvent, CommitMatchEvent, ContentMatchEvent, EventMatch, LineMatchEvent, Location,
    PathMatchEvent, PersonMatchEvent, Range, RepoMatchEvent, SymbolEvent, SymbolMatchEvent,
    TeamMatchEvent, UserMetadata,
};
use sg_service_searcher::langmatch;

/// `types.MinimalRepo`.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct MinimalRepo {
    pub id: i32,
    pub name: String,
}

/// `types.SearchedRepo` as consumed by `FromMatch`.
#[derive(Debug, Clone, PartialEq)]
pub struct RepoMetadata {
    pub id: i32,
    pub name: String,
    pub description: String,
    pub fork: bool,
    pub archived: bool,
    pub private: bool,
    pub stars: i32,
    pub last_fetched: Option<OffsetDateTime>,
    pub key_value_pairs: Option<BTreeMap<String, Option<String>>>,
    pub topics: Option<Vec<String>>,
}

/// `result.ChunkMatch`.
#[derive(Debug, Clone, PartialEq)]
pub struct ChunkMatch {
    pub content: String,
    pub content_start: Location,
    pub ranges: Vec<Range>,
}

impl ChunkMatch {
    /// `ChunkMatch.AsLineMatches`.
    fn as_line_matches(&self) -> Vec<LineMatchEvent> {
        let content = self.content.strip_suffix('\n').unwrap_or(&self.content);
        let lines: Vec<&str> = content.split('\n').collect();
        let mut out = Vec::new();
        for (i, line) in lines.iter().enumerate() {
            let line_number = self.content_start.line + i as i64;
            let mut offset_and_lengths: Vec<[i32; 2]> = Vec::new();
            for range in &self.ranges {
                let mut range_line = range.start.line;
                while range_line <= range.end.line {
                    if range_line == line_number {
                        let mut start = 0i64;
                        if range_line == range.start.line {
                            start = range.start.column;
                        }
                        let mut end = line.chars().count() as i64;
                        if range_line == range.end.line {
                            end = range.end.column;
                        }
                        if start != end {
                            offset_and_lengths.push([start as i32, (end - start) as i32]);
                        }
                    }
                    range_line += 1;
                }
            }
            if !offset_and_lengths.is_empty() {
                out.push(LineMatchEvent {
                    line: (*line).to_owned(),
                    line_number: line_number as i32,
                    offset_and_lengths,
                });
            }
        }
        out
    }
}

/// `ChunkMatches.MatchCount`.
fn chunk_match_count(chunks: &[ChunkMatch]) -> i64 {
    chunks.iter().map(|c| c.ranges.len() as i64).sum()
}

/// `ChunkMatches.Limit`.
fn chunk_limit(chunks: &mut Vec<ChunkMatch>, limit: i64) {
    let mut remaining = limit;
    for i in 0..chunks.len() {
        let count = chunks[i].ranges.len() as i64;
        if count >= remaining {
            chunks[i].ranges.truncate(remaining as usize);
            chunks.truncate(i + 1);
            return;
        }
        remaining -= count;
    }
}

/// `result.Symbol`.
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
}

impl Symbol {
    fn lsp_kind(&self) -> Option<u16> {
        lsp_kind(&self.kind)
    }

    /// `Symbol.SelectKind`.
    fn select_kind(&self) -> Option<String> {
        let kind = self.lsp_kind()?;
        let name = lsp_kind_name(kind).to_ascii_lowercase();
        Some(match name.as_str() {
            "enummember" => "enum-member".to_owned(),
            "typeparameter" => "type-parameter".to_owned(),
            _ => name,
        })
    }
}

/// The `result.File` part of a file match.
#[derive(Debug, Clone, PartialEq)]
pub struct FileRef {
    pub repo: MinimalRepo,
    pub commit_id: String,
    pub path: String,
    pub input_rev: Option<String>,
    pub precise_language: String,
}

impl FileRef {
    /// `File.MostLikelyLanguage`.
    fn most_likely_language(&self) -> String {
        if !self.precise_language.is_empty() {
            return self.precise_language.clone();
        }
        let candidates =
            langmatch::get_languages::<fn() -> std::io::Result<Vec<u8>>>(&self.path, None);
        match candidates {
            Ok(candidates) => candidates.into_iter().next().unwrap_or_default(),
            Err(_) => String::new(),
        }
    }

    /// `File.URL().String()`.
    fn url(&self) -> String {
        let mut path = String::new();
        path.push('/');
        path.push_str(&self.repo.name);
        if let Some(rev) = &self.input_rev {
            if !rev.is_empty() {
                path.push('@');
                path.push_str(rev);
            }
        }
        path.push_str("/-/blob/");
        path.push_str(&self.path);
        go_url_path(&path)
    }
}

/// `result.SymbolMatch`.
#[derive(Debug, Clone, PartialEq)]
pub struct SymbolMatch {
    pub symbol: Symbol,
    pub file: FileRef,
}

impl SymbolMatch {
    /// `SymbolMatch.URL().String()`.
    fn url(&self) -> String {
        let mut url = self.file.url();
        url.push('?');
        url.push_str(&url_fragment_from_range(&self.symbol));
        url
    }
}

/// `result.compareSymbolMatches`: equal when the line and symbol identity
/// fields match (character/path/language are deliberately ignored).
fn same_symbol(a: &SymbolMatch, b: &SymbolMatch) -> bool {
    a.symbol.line == b.symbol.line
        && a.symbol.name == b.symbol.name
        && a.symbol.kind == b.symbol.kind
        && a.symbol.parent == b.symbol.parent
        && a.symbol.parent_kind == b.symbol.parent_kind
}

/// `result.DedupSymbols`: drop duplicates among **adjacent** entries.
///
/// The list is not sorted; only consecutive matches with the same line and
/// symbol identity are removed.
pub fn dedup_symbols(symbols: Vec<SymbolMatch>) -> Vec<SymbolMatch> {
    let mut dedup: Vec<SymbolMatch> = Vec::with_capacity(symbols.len());
    for symbol in symbols {
        let duplicate = dedup.last().is_some_and(|last| same_symbol(&symbol, last));
        if !duplicate {
            dedup.push(symbol);
        }
    }
    dedup
}

/// `result.FileMatch`.
#[derive(Debug, Clone, PartialEq)]
pub struct FileMatch {
    pub file: FileRef,
    pub chunk_matches: Vec<ChunkMatch>,
    pub symbols: Vec<SymbolMatch>,
    pub path_matches: Vec<Range>,
    pub limit_hit: bool,
    pub debug: Option<String>,
}

impl FileMatch {
    pub fn result_count(&self) -> i64 {
        let count = self.symbols.len() as i64 + chunk_match_count(&self.chunk_matches);
        if count == 0 {
            1
        } else {
            count
        }
    }

    fn limit(&mut self, limit: i64) -> i64 {
        let mut limit = limit;
        let match_count = chunk_match_count(&self.chunk_matches);
        let symbol_count = self.symbols.len() as i64;

        if match_count == 0 && symbol_count == 0 {
            return limit - 1;
        }

        if limit < match_count {
            chunk_limit(&mut self.chunk_matches, limit);
            limit = 0;
            self.limit_hit = true;
        } else {
            limit -= match_count;
        }

        if limit < symbol_count {
            self.symbols.truncate(limit as usize);
            limit = 0;
            self.limit_hit = true;
        } else {
            limit -= symbol_count;
        }
        limit
    }
}

/// `result.RepoMatch`.
#[derive(Debug, Clone, PartialEq)]
pub struct RepoMatch {
    pub name: String,
    pub id: i32,
    pub rev: String,
    pub description_matches: Vec<Range>,
    pub repo_name_matches: Vec<Range>,
}

impl RepoMatch {
    fn url(&self) -> String {
        let mut path = String::new();
        path.push('/');
        path.push_str(&self.name);
        if !self.rev.is_empty() {
            path.push('@');
            path.push_str(&self.rev);
        }
        go_url_path(&path)
    }
}

/// `gitdomain.Signature`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Signature {
    pub name: String,
    pub email: String,
    pub date: OffsetDateTime,
}

/// `gitdomain.Commit`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GitCommit {
    pub id: String,
    pub message: String,
    pub author: Signature,
    pub committer: Signature,
}

/// `result.MatchedString`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MatchedString {
    pub content: String,
    pub matched_ranges: Vec<Range>,
}

/// `result.CommitMatch`.
#[derive(Debug, Clone, PartialEq)]
pub struct CommitMatch {
    pub commit: GitCommit,
    pub repo: MinimalRepo,
    pub message_preview: Option<MatchedString>,
    pub diff_preview: Option<MatchedString>,
}

impl CommitMatch {
    /// `CommitMatch.ResultCount`.
    pub fn result_count(&self) -> i64 {
        let count = if let Some(diff) = &self.diff_preview {
            diff.matched_ranges.len()
        } else if let Some(message) = &self.message_preview {
            message.matched_ranges.len()
        } else {
            0
        };
        if count > 0 {
            count as i64
        } else {
            1
        }
    }

    fn body(&self) -> MatchedString {
        if let Some(diff) = &self.diff_preview {
            return MatchedString {
                content: format!("```diff\n{}\n```", diff.content),
                matched_ranges: add_shift(&diff.matched_ranges, "```diff\n"),
            };
        }
        let message = self
            .message_preview
            .as_ref()
            .cloned()
            .unwrap_or_else(|| MatchedString {
                content: String::new(),
                matched_ranges: Vec::new(),
            });
        MatchedString {
            content: format!("```COMMIT_EDITMSG\n{}\n```", message.content),
            matched_ranges: add_shift(&message.matched_ranges, "```COMMIT_EDITMSG\n"),
        }
    }

    fn limit(&mut self, limit: i64) -> i64 {
        let preview = if let Some(diff) = self.diff_preview.as_mut() {
            diff
        } else if let Some(message) = self.message_preview.as_mut() {
            message
        } else {
            return limit - 1;
        };
        if preview.matched_ranges.is_empty() {
            return limit - 1;
        }
        if preview.matched_ranges.len() as i64 > limit {
            preview.matched_ranges.truncate(limit as usize);
            return 0;
        }
        limit - preview.matched_ranges.len() as i64
    }

    /// `CommitMatch.Label`.
    fn label(&self) -> String {
        let message = commit_subject(&self.commit.message);
        let author = &self.commit.author.name;
        let repo_name = display_repo_name(&self.repo.name);
        let repo_url = RepoMatch {
            name: self.repo.name.clone(),
            id: self.repo.id,
            rev: String::new(),
            description_matches: Vec::new(),
            repo_name_matches: Vec::new(),
        }
        .url();
        let commit_url = self.url();
        format!("[{repo_name}]({repo_url}) › [{author}]({commit_url}): [{message}]({commit_url})")
    }

    /// `CommitMatch.Detail`. The suffix of `timeago.NoMax(timeago.English)`
    /// uses `time.Now()`; callers pass the handler's start-adjacent `now`.
    fn detail(&self, now: OffsetDateTime) -> String {
        let commit_hash = short_commit_id(&self.commit.id);
        format!(
            "[`{commit_hash}` {}]({})",
            timeago_english_no_max(self.commit.author.date, now),
            self.url()
        )
    }

    /// `CommitMatch.URL().String()`.
    fn url(&self) -> String {
        let mut path = String::new();
        path.push('/');
        path.push_str(&self.repo.name);
        path.push_str("/-/commit/");
        path.push_str(&self.commit.id);
        go_url_path(&path)
    }
}

/// `result.OwnerMatch.ResolvedOwner`.
#[derive(Debug, Clone, PartialEq)]
pub enum ResolvedOwner {
    Person {
        handle: String,
        email: String,
        user: Option<UserMetadata>,
    },
    Team {
        handle: String,
        email: String,
        name: String,
        display_name: String,
    },
}

/// `result.OwnerMatch`.
#[derive(Debug, Clone, PartialEq)]
pub struct OwnerMatch {
    pub owner: ResolvedOwner,
    pub repo: MinimalRepo,
}

/// The `result.Match` union.
#[derive(Debug, Clone, PartialEq)]
pub enum Match {
    File(FileMatch),
    Repo(RepoMatch),
    Commit(CommitMatch),
    Owner(OwnerMatch),
}

impl Match {
    /// `Match.RepoName`.
    pub fn repo_name(&self) -> MinimalRepo {
        match self {
            Match::File(m) => m.file.repo.clone(),
            Match::Repo(m) => MinimalRepo {
                id: m.id,
                name: m.name.clone(),
            },
            Match::Commit(m) => m.repo.clone(),
            Match::Owner(m) => m.repo.clone(),
        }
    }

    /// `Match.ResultCount`.
    pub fn result_count(&self) -> i64 {
        match self {
            Match::File(m) => m.result_count(),
            Match::Repo(_) => 1,
            Match::Commit(m) => m.result_count(),
            Match::Owner(_) => 1,
        }
    }

    /// `Match.Limit`.
    pub fn limit(&mut self, limit: i64) -> i64 {
        match self {
            Match::File(m) => m.limit(limit),
            Match::Repo(_) => limit - 1,
            Match::Commit(m) => m.limit(limit),
            Match::Owner(_) => limit - 1,
        }
    }
}

/// `search.FromMatchOptions`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FromMatchOptions {
    pub chunk_matches: bool,
    pub max_content_line_length: i64,
    /// Stands in for `time.Now()` in `CommitMatch.Detail`.
    pub now: OffsetDateTime,
}

/// `search.FromMatch`.
pub fn from_match(
    match_: &Match,
    repo_cache: &HashMap<i32, RepoMetadata>,
    opts: FromMatchOptions,
) -> EventMatch {
    match match_ {
        Match::File(m) => {
            if !m.symbols.is_empty() {
                EventMatch::Symbol(from_symbol_match(m, repo_cache))
            } else if chunk_match_count(&m.chunk_matches) > 0 {
                EventMatch::Content(from_content_match(m, repo_cache, opts))
            } else {
                EventMatch::Path(from_path_match(m, repo_cache))
            }
        }
        Match::Repo(m) => EventMatch::Repo(from_repository(m, repo_cache)),
        Match::Commit(m) => EventMatch::Commit(from_commit(m, repo_cache, opts.now)),
        Match::Owner(m) => from_owner(m),
    }
}

fn from_path_match(fm: &FileMatch, repo_cache: &HashMap<i32, RepoMetadata>) -> PathMatchEvent {
    let mut event = PathMatchEvent {
        path: fm.file.path.clone(),
        path_matches: fm.path_matches.clone(),
        repository_id: fm.file.repo.id,
        repository: fm.file.repo.name.clone(),
        repo_stars: 0,
        repo_last_fetched: None,
        branches: fm.file.input_rev.as_ref().map(|rev| vec![rev.clone()]),
        commit: fm.file.commit_id.clone(),
        language: fm.file.most_likely_language(),
        debug: fm.debug.clone().unwrap_or_default(),
    };
    if let Some(metadata) = repo_cache.get(&fm.file.repo.id) {
        event.repo_stars = metadata.stars;
        event.repo_last_fetched = metadata.last_fetched;
    }
    event
}

fn from_chunk_matches(chunks: &[ChunkMatch], opts: FromMatchOptions) -> Vec<ChunkMatchEvent> {
    chunks
        .iter()
        .map(|cm| {
            let (content, truncated) = truncate_lines(&cm.content, opts.max_content_line_length);
            ChunkMatchEvent {
                content,
                content_start: cm.content_start,
                ranges: cm.ranges.clone(),
                content_truncated: truncated,
            }
        })
        .collect()
}

fn from_content_match(
    fm: &FileMatch,
    repo_cache: &HashMap<i32, RepoMetadata>,
    opts: FromMatchOptions,
) -> ContentMatchEvent {
    let (line_matches, chunk_matches) = if opts.chunk_matches {
        (None, Some(from_chunk_matches(&fm.chunk_matches, opts)))
    } else {
        let mut lines = Vec::new();
        for cm in &fm.chunk_matches {
            lines.extend(cm.as_line_matches());
        }
        (Some(lines), None)
    };

    let mut event = ContentMatchEvent {
        path: fm.file.path.clone(),
        path_matches: fm.path_matches.clone(),
        repository_id: fm.file.repo.id,
        repository: fm.file.repo.name.clone(),
        repo_stars: 0,
        repo_last_fetched: None,
        branches: fm.file.input_rev.as_ref().map(|rev| vec![rev.clone()]),
        commit: fm.file.commit_id.clone(),
        line_matches,
        chunk_matches,
        language: fm.file.most_likely_language(),
        debug: fm.debug.clone().unwrap_or_default(),
    };
    if let Some(metadata) = repo_cache.get(&fm.file.repo.id) {
        event.repo_stars = metadata.stars;
        event.repo_last_fetched = metadata.last_fetched;
    }
    event
}

fn from_symbol_match(fm: &FileMatch, repo_cache: &HashMap<i32, RepoMetadata>) -> SymbolMatchEvent {
    let symbols = fm
        .symbols
        .iter()
        .map(|sym| {
            let kind = match sym.symbol.lsp_kind() {
                Some(kind) => lsp_kind_name(kind).to_ascii_uppercase(),
                None => "UNKNOWN".to_owned(),
            };
            SymbolEvent {
                url: sym.url(),
                name: sym.symbol.name.clone(),
                container_name: sym.symbol.parent.clone(),
                kind,
                line: sym.symbol.line,
            }
        })
        .collect();

    let mut event = SymbolMatchEvent {
        path: fm.file.path.clone(),
        repository_id: fm.file.repo.id,
        repository: fm.file.repo.name.clone(),
        repo_stars: 0,
        repo_last_fetched: None,
        branches: fm.file.input_rev.as_ref().map(|rev| vec![rev.clone()]),
        commit: fm.file.commit_id.clone(),
        language: fm.file.most_likely_language(),
        symbols,
    };
    if let Some(metadata) = repo_cache.get(&fm.file.repo.id) {
        event.repo_stars = metadata.stars;
        event.repo_last_fetched = metadata.last_fetched;
    }
    event
}

fn from_repository(rm: &RepoMatch, repo_cache: &HashMap<i32, RepoMetadata>) -> RepoMatchEvent {
    let mut event = RepoMatchEvent {
        repository_id: rm.id,
        repository: rm.name.clone(),
        repository_matches: rm.repo_name_matches.clone(),
        branches: if rm.rev.is_empty() {
            None
        } else {
            Some(vec![rm.rev.clone()])
        },
        repo_stars: 0,
        repo_last_fetched: None,
        description: String::new(),
        description_matches: rm.description_matches.clone(),
        fork: false,
        archived: false,
        private: false,
        metadata: None,
        topics: None,
    };
    if let Some(metadata) = repo_cache.get(&rm.id) {
        event.repo_stars = metadata.stars;
        event.repo_last_fetched = metadata.last_fetched;
        event.description = metadata.description.clone();
        event.fork = metadata.fork;
        event.archived = metadata.archived;
        event.private = metadata.private;
        event.metadata = metadata.key_value_pairs.clone();
        event.topics = metadata.topics.clone();
    }
    event
}

fn from_commit(
    commit: &CommitMatch,
    repo_cache: &HashMap<i32, RepoMetadata>,
    now: OffsetDateTime,
) -> CommitMatchEvent {
    let body = commit.body();
    let mut ranges = Vec::new();
    for range in &body.matched_ranges {
        ranges.extend(range_to_highlights(&body.content, range));
    }

    let mut event = CommitMatchEvent {
        label: commit.label(),
        url: commit.url(),
        detail: commit.detail(now),
        repository_id: commit.repo.id,
        repository: commit.repo.name.clone(),
        oid: commit.commit.id.clone(),
        message: commit.commit.message.clone(),
        author_name: commit.commit.author.name.clone(),
        author_date: commit.commit.author.date,
        committer_name: commit.commit.committer.name.clone(),
        committer_date: commit.commit.committer.date,
        repo_stars: 0,
        repo_last_fetched: None,
        content: body.content.clone(),
        ranges,
    };
    if let Some(metadata) = repo_cache.get(&commit.repo.id) {
        event.repo_stars = metadata.stars;
        event.repo_last_fetched = metadata.last_fetched;
    }
    event
}

fn from_owner(owner: &OwnerMatch) -> EventMatch {
    match &owner.owner {
        ResolvedOwner::Person {
            handle,
            email,
            user,
        } => EventMatch::Person(PersonMatchEvent {
            handle: handle.clone(),
            email: email.clone(),
            user: user.clone(),
        }),
        ResolvedOwner::Team {
            handle,
            email,
            name,
            display_name,
        } => EventMatch::Team(TeamMatchEvent {
            handle: handle.clone(),
            email: email.clone(),
            name: name.clone(),
            display_name: display_name.clone(),
        }),
    }
}

fn add_shift(ranges: &[Range], prefix: &str) -> Vec<Range> {
    let shift = Location {
        offset: prefix.len() as i64,
        line: 1,
        column: 0,
    };
    ranges
        .iter()
        .map(|range| Range {
            start: Location {
                offset: range.start.offset + shift.offset,
                line: range.start.line + shift.line,
                column: range.start.column + shift.column,
            },
            end: Location {
                offset: range.end.offset + shift.offset,
                line: range.end.line + shift.line,
                column: range.end.column + shift.column,
            },
        })
        .collect()
}

/// `rangeToHighlights`: split a possibly multi-line range into per-line
/// `[line, character, length]` triples. Byte offsets, rune-free lengths, and
/// `bufio.Scanner` line semantics are reproduced.
fn range_to_highlights(content: &str, range: &Range) -> Vec<[i32; 3]> {
    let bytes = content.as_bytes();
    let start = range.start.offset.max(0) as usize;
    let end = range.end.offset.max(0) as usize;
    if start > bytes.len() || end > bytes.len() || start > end {
        return Vec::new();
    }
    let mut out = Vec::new();
    for (line_num, line) in (range.start.line..).zip(scan_lines(&bytes[start..end])) {
        let mut character = 0i64;
        if line_num == range.start.line {
            character = range.start.column;
        }
        let mut length = line.len() as i64;
        if line_num == range.end.line {
            length = range.end.column - character;
        }
        if length > 0 {
            out.push([line_num as i32, character as i32, length as i32]);
        }
    }
    out
}

/// `bufio.ScanLines` over bytes: drop a trailing `\r` per line, and no
/// trailing empty token when the data ends with a newline.
fn scan_lines(data: &[u8]) -> Vec<&[u8]> {
    if data.is_empty() {
        return Vec::new();
    }
    let mut lines: Vec<&[u8]> = data.split(|byte| *byte == b'\n').collect();
    if data.ends_with(b"\n") {
        lines.pop();
    }
    lines
        .into_iter()
        .map(|line| line.strip_suffix(b"\r").unwrap_or(line))
        .collect()
}

/// `search.truncateLines`.
fn truncate_lines(content: &str, max_line_len: i64) -> (String, bool) {
    if max_line_len <= 0 || (content.len() as i64) < max_line_len {
        return (content.to_owned(), false);
    }
    if all_line_len_less_than_equal(content, max_line_len) {
        return (content.to_owned(), false);
    }
    let mut suffix = "...truncated";
    if suffix.len() as i64 > max_line_len {
        suffix = "";
    }
    let lines: Vec<&str> = content.split('\n').collect();
    let mut out: Vec<String> = Vec::with_capacity(lines.len());
    for line in lines {
        if line.len() as i64 <= max_line_len {
            out.push(line.to_owned());
            continue;
        }
        let keep = (max_line_len - suffix.len() as i64) as usize;
        let truncated = truncate_bytes(line, keep);
        out.push(format!("{truncated}{suffix}"));
    }
    (out.join("\n"), true)
}

fn all_line_len_less_than_equal(content: &str, max_line_len: i64) -> bool {
    let mut rest = content;
    while !rest.is_empty() {
        match rest.find('\n') {
            None => return rest.len() as i64 <= max_line_len,
            Some(idx) => {
                if idx as i64 > max_line_len {
                    return false;
                }
                rest = &rest[idx + 1..];
            }
        }
    }
    true
}

/// Go string slicing operates on bytes and may split a rune; `encoding/json`
/// replaces the resulting invalid UTF-8 with U+FFFD, which is what Rust's
/// lossy conversion produces for the same prefix.
fn truncate_bytes(value: &str, keep: usize) -> String {
    let bytes = value.as_bytes();
    let keep = keep.min(bytes.len());
    String::from_utf8_lossy(&bytes[..keep]).into_owned()
}

/// `displayRepoName`: drop the hostname component when the repo path has one.
fn display_repo_name(repo_path: &str) -> String {
    let mut parts: Vec<&str> = repo_path.split('/').collect();
    if parts.len() >= 3 && parts[0].contains('.') {
        parts.remove(0);
    }
    parts.join("/")
}

/// `Message.Subject`: first line, trimmed.
fn commit_subject(message: &str) -> String {
    match message.find('\n') {
        None => message.trim().to_owned(),
        Some(idx) => message[..idx].trim().to_owned(),
    }
}

/// `CommitID.Short`.
fn short_commit_id(id: &str) -> String {
    id.chars().take(7).collect()
}

/// `timeago.NoMax(timeago.English).Format`; `Max` is `math.MaxInt64`
/// nanoseconds, so normal dates always take the fuzzy path.
pub fn timeago_english_no_max(t: OffsetDateTime, now: OffsetDateTime) -> String {
    let delta = (now - t).whole_nanoseconds();
    if delta.abs() >= i64::MAX as i128 {
        return format_rfc3339(t);
    }
    let is_past = delta >= 0;
    let text = get_time_text(delta.unsigned_abs() as i64, true);
    if is_past {
        format!("{text} ago")
    } else {
        format!("in {text}")
    }
}

const PERIODS: [(i64, &str, &str); 6] = [
    (1_000_000_000, "about a second", "%d seconds"),
    (60 * 1_000_000_000, "about a minute", "%d minutes"),
    (60 * 60 * 1_000_000_000, "about an hour", "%d hours"),
    (24 * 60 * 60 * 1_000_000_000, "one day", "%d days"),
    (30 * 24 * 60 * 60 * 1_000_000_000, "one month", "%d months"),
    (365 * 24 * 60 * 60 * 1_000_000_000, "one year", "%d years"),
];

fn get_time_text(d: i64, round_closer: bool) -> String {
    if d < PERIODS[0].0 {
        return "about a second".to_owned();
    }
    for i in 0..PERIODS.len() {
        let (period, one, many) = PERIODS[i];
        let next = if i + 1 < PERIODS.len() {
            PERIODS[i + 1].0
        } else {
            period
        };
        if i + 1 == PERIODS.len() || d < next {
            let r = round_duration(d, period, round_closer);
            if next != period && r == round_duration(next, period, round_closer) {
                continue;
            }
            if r == 0 {
                return String::new();
            }
            let layout = if r == 1 { one } else { many };
            if layout.contains("%d") {
                return layout.replace("%d", &r.to_string());
            }
            return layout.to_owned();
        }
    }
    String::new()
}

/// `round`: `time.Duration(float64(d)/float64(step) + 0.5)` — a nanosecond
/// duration whose value is the rounded ratio.
fn round_duration(d: i64, step: i64, round_closer: bool) -> i64 {
    let ratio = d as f64 / step as f64;
    if round_closer {
        (ratio + 0.5) as i64
    } else {
        ratio as i64
    }
}

fn format_rfc3339(value: OffsetDateTime) -> String {
    format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}Z",
        value.year(),
        u8::from(value.month()),
        value.day(),
        value.hour(),
        value.minute(),
        value.second()
    )
}

/// `lib/codeintel/languages`'s `most likely` language is also consumed by the
/// filters (`SearchFilters`), which needs the same helper.
pub fn most_likely_language(file: &FileRef) -> String {
    file.most_likely_language()
}

/// The symbol range URL fragment (`urlFragmentFromRange`).
fn url_fragment_from_range(symbol: &Symbol) -> String {
    let start_line = symbol.line - 1;
    let start_char = symbol.character;
    let end_line = start_line;
    let end_char = symbol.character + symbol.name.len() as i32;
    if start_line == end_line && start_char == end_char {
        return format!("L{}", start_line + 1);
    }
    let has_character = start_char != 0 || end_char != 0;
    format!(
        "L{}-{}",
        line_spec_from_position(start_line, start_char, has_character),
        line_spec_from_position(end_line, end_char, has_character)
    )
}

fn line_spec_from_position(line: i32, character: i32, force_include_character: bool) -> String {
    if !force_include_character && character == 0 {
        return (line + 1).to_string();
    }
    format!("{}:{}", line + 1, character + 1)
}

/// Percent-encode a path exactly like `net/url`'s `encodePath`
/// (`url.URL{Path: p}.String()`). Only `?` of the reserved characters is
/// escaped; `$&+,/:;=@` and the unreserved set stay literal.
pub fn go_url_path(path: &str) -> String {
    let mut out = String::with_capacity(path.len());
    for byte in path.bytes() {
        let allowed = byte.is_ascii_alphanumeric()
            || matches!(
                byte,
                b'-' | b'_'
                    | b'.'
                    | b'~'
                    | b'$'
                    | b'&'
                    | b'+'
                    | b','
                    | b'/'
                    | b':'
                    | b';'
                    | b'='
                    | b'@'
            );
        if allowed {
            out.push(byte as char);
        } else {
            out.push_str(&format!("%{byte:02X}"));
        }
    }
    out
}

/// `regexp.QuoteMeta`.
pub fn quote_meta(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for ch in value.chars() {
        if matches!(
            ch,
            '\\' | '.' | '+' | '*' | '?' | '(' | ')' | '|' | '[' | ']' | '{' | '}' | '^' | '$'
        ) {
            out.push('\\');
        }
        out.push(ch);
    }
    out
}

/// Go `strconv.Quote` for the language filter values (`"c sharp"`).
pub fn go_strconv_quote(value: &str) -> String {
    let mut out = String::from("\"");
    for ch in value.chars() {
        match ch {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\u{7}' => out.push_str("\\a"),
            '\u{8}' => out.push_str("\\b"),
            '\u{c}' => out.push_str("\\f"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            '\u{b}' => out.push_str("\\v"),
            c if (c as u32) < 0x20 || c as u32 == 0x7f => {
                out.push_str(&format!("\\x{:02x}", c as u32));
            }
            c if is_go_print(c) => out.push(c),
            c if (c as u32) <= 0xffff => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push_str(&format!("\\U{:08x}", c as u32)),
        }
    }
    out.push('"');
    out
}

/// Approximation of `unicode.IsPrint` for the characters that can appear in
/// language names: printable ASCII plus all non-control Unicode scalars.
fn is_go_print(ch: char) -> bool {
    !ch.is_control() && ch != '\u{2028}' && ch != '\u{2029}'
}

fn lsp_kind(kind: &str) -> Option<u16> {
    Some(match kind {
        "file" => 1,
        "module" => 2,
        "namespace" => 3,
        "package" | "packagename" | "subprogspec" => 4,
        "class" | "classes" | "type" | "service" | "typedef" | "union" | "section" | "subtype"
        | "component" => 5,
        "accessor" | "getter" | "method" | "methodalias" | "methodspec" | "setter" => 6,
        "property" => 7,
        "field" | "member" | "anonmember" | "recordfield" => 8,
        "constructor" => 9,
        "enum" | "enumerator" => 10,
        "interface" => 11,
        "function" | "func" | "subroutine" | "macro" | "subprogram" | "procedure" | "command"
        | "singletonmethod" => 12,
        "variable" | "var" | "functionvar" | "define" | "alias" | "val" => 13,
        "constant" | "const" => 14,
        "string" | "message" | "heredoc" => 15,
        "number" => 16,
        "bool" | "boolean" => 17,
        "array" => 18,
        "object" | "literal" | "map" => 19,
        "key" | "label" | "target" | "selector" | "id" | "tag" => 20,
        "null" => 21,
        "enum member" | "enumconstant" | "enummember" => 22,
        "struct" => 23,
        "event" => 24,
        "operator" => 25,
        "type parameter" | "annotation" => 26,
        _ => return None,
    })
}

fn lsp_kind_name(kind: u16) -> &'static str {
    match kind {
        1 => "File",
        2 => "Module",
        3 => "Namespace",
        4 => "Package",
        5 => "Class",
        6 => "Method",
        7 => "Property",
        8 => "Field",
        9 => "Constructor",
        10 => "Enum",
        11 => "Interface",
        12 => "Function",
        13 => "Variable",
        14 => "Constant",
        15 => "String",
        16 => "Number",
        17 => "Boolean",
        18 => "Array",
        19 => "Object",
        20 => "Key",
        21 => "Null",
        22 => "EnumMember",
        23 => "Struct",
        24 => "Event",
        25 => "Operator",
        26 => "TypeParameter",
        _ => "UNKNOWN",
    }
}

/// `Symbol.SelectKind` for the filter code.
pub fn select_kind(symbol: &Symbol) -> Option<String> {
    symbol.select_kind()
}

/// `determineTimeframe` building blocks (`SearchFilters`).
pub fn timeframe_for(
    date: OffsetDateTime,
    now: OffsetDateTime,
) -> (&'static str, &'static str, &'static str) {
    if date > now - Duration::hours(25) {
        ("after", "yesterday", "Last 24 hours")
    } else if date > now - Duration::days(8) {
        ("before", "\"1 week ago\"", "Last week")
    } else if date > now - Duration::days(31) {
        ("before", "\"1 month ago\"", "Last month")
    } else {
        ("before", "\"2 months ago\"", "Older than 2 months")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn t(seconds: i64) -> OffsetDateTime {
        OffsetDateTime::from_unix_timestamp(seconds).expect("timestamp")
    }

    #[test]
    fn truncate_lines_matches_go() {
        assert_eq!(
            truncate_lines("line one match\nsecond line\n", 6),
            ("line o\nsecond\n".to_owned(), true)
        );
        // Short lines are returned untouched.
        assert_eq!(truncate_lines("a\nbb\n", 6), ("a\nbb\n".to_owned(), false));
        // The suffix is kept when it fits.
        assert_eq!(
            truncate_lines("012345678901234567890123456789\n", 20),
            ("01234567...truncated\n".to_owned(), true)
        );
        // Disabled.
        assert_eq!(
            truncate_lines("0123456789\n", 0),
            ("0123456789\n".to_owned(), false)
        );
    }

    #[test]
    fn timeago_matches_go() {
        let now = t(2_500_000_000);
        assert_eq!(timeago_english_no_max(now, now), "about a second ago");
        assert_eq!(
            timeago_english_no_max(t(now.unix_timestamp() - 1), now),
            "about a second ago"
        );
        assert_eq!(
            timeago_english_no_max(t(now.unix_timestamp() - 90), now),
            "2 minutes ago"
        );
        assert_eq!(
            timeago_english_no_max(t(now.unix_timestamp() - 3600), now),
            "about an hour ago"
        );
        assert_eq!(
            timeago_english_no_max(t(now.unix_timestamp() - 36 * 3600), now),
            "2 days ago"
        );
        assert_eq!(
            timeago_english_no_max(t(now.unix_timestamp() - 400 * 86400), now),
            "one year ago"
        );
        assert_eq!(
            timeago_english_no_max(t(now.unix_timestamp() - 2500 * 86400), now),
            "7 years ago"
        );
        assert_eq!(
            timeago_english_no_max(t(now.unix_timestamp() + 10), now),
            "in 10 seconds"
        );
    }

    #[test]
    fn url_path_escapes_like_go() {
        assert_eq!(
            go_url_path("/github.com/foo/bar/-/blob/src/main.go"),
            "/github.com/foo/bar/-/blob/src/main.go"
        );
        assert_eq!(go_url_path("/x/foo bar?x"), "/x/foo%20bar%3Fx");
        assert_eq!(go_url_path("/caf\u{e9}"), "/caf%C3%A9");
    }

    #[test]
    fn display_repo_name_strips_hostname() {
        assert_eq!(display_repo_name("github.com/foo/bar"), "foo/bar");
        assert_eq!(display_repo_name("foo/bar"), "foo/bar");
        assert_eq!(display_repo_name("example.com/x/y/z"), "x/y/z");
    }

    #[test]
    fn timeframe_matches_go() {
        let now = t(2_500_000_000);
        assert_eq!(
            timeframe_for(now - Duration::hours(1), now),
            ("after", "yesterday", "Last 24 hours")
        );
        assert_eq!(
            timeframe_for(now - Duration::days(7), now),
            ("before", "\"1 week ago\"", "Last week")
        );
        assert_eq!(
            timeframe_for(now - Duration::days(30), now),
            ("before", "\"1 month ago\"", "Last month")
        );
        assert_eq!(
            timeframe_for(now - Duration::days(365), now),
            ("before", "\"2 months ago\"", "Older than 2 months")
        );
    }
}
