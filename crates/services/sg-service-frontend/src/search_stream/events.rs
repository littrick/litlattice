//! Event payloads (`internal/search/streaming/http/events.go`) and their
//! `encoding/json`-compatible encodings.

use time::OffsetDateTime;

use super::convert::{CommitMatch, Signature};
use super::json;

/// `result.Location`; JSON is `{offset,line,column}` in the event API.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Location {
    pub offset: i64,
    pub line: i64,
    pub column: i64,
}

/// `result.Range`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Range {
    pub start: Location,
    pub end: Location,
}

/// `http.ChunkMatch`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChunkMatchEvent {
    pub content: String,
    pub content_start: Location,
    pub ranges: Vec<Range>,
    pub content_truncated: bool,
}

/// `http.EventLineMatch`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LineMatchEvent {
    pub line: String,
    pub line_number: i32,
    pub offset_and_lengths: Vec<[i32; 2]>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ContentMatchEvent {
    pub path: String,
    pub path_matches: Vec<Range>,
    pub repository_id: i32,
    pub repository: String,
    pub repo_stars: i32,
    pub repo_last_fetched: Option<OffsetDateTime>,
    pub branches: Option<Vec<String>>,
    pub commit: String,
    pub line_matches: Option<Vec<LineMatchEvent>>,
    pub chunk_matches: Option<Vec<ChunkMatchEvent>>,
    pub language: String,
    pub debug: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PathMatchEvent {
    pub path: String,
    pub path_matches: Vec<Range>,
    pub repository_id: i32,
    pub repository: String,
    pub repo_stars: i32,
    pub repo_last_fetched: Option<OffsetDateTime>,
    pub branches: Option<Vec<String>>,
    pub commit: String,
    pub language: String,
    pub debug: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SymbolEvent {
    pub url: String,
    pub name: String,
    pub container_name: String,
    pub kind: String,
    pub line: i32,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SymbolMatchEvent {
    pub path: String,
    pub repository_id: i32,
    pub repository: String,
    pub repo_stars: i32,
    pub repo_last_fetched: Option<OffsetDateTime>,
    pub branches: Option<Vec<String>>,
    pub commit: String,
    pub language: String,
    pub symbols: Vec<SymbolEvent>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct RepoMatchEvent {
    pub repository_id: i32,
    pub repository: String,
    pub repository_matches: Vec<Range>,
    pub branches: Option<Vec<String>>,
    pub repo_stars: i32,
    pub repo_last_fetched: Option<OffsetDateTime>,
    pub description: String,
    pub description_matches: Vec<Range>,
    pub fork: bool,
    pub archived: bool,
    pub private: bool,
    pub metadata: Option<std::collections::BTreeMap<String, Option<String>>>,
    pub topics: Option<Vec<String>>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CommitMatchEvent {
    pub label: String,
    pub url: String,
    pub detail: String,
    pub repository_id: i32,
    pub repository: String,
    pub oid: String,
    pub message: String,
    pub author_name: String,
    pub author_date: OffsetDateTime,
    pub committer_name: String,
    pub committer_date: OffsetDateTime,
    pub repo_stars: i32,
    pub repo_last_fetched: Option<OffsetDateTime>,
    pub content: String,
    /// `[line, character, length]`.
    pub ranges: Vec<[i32; 3]>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PersonMatchEvent {
    pub handle: String,
    pub email: String,
    pub user: Option<UserMetadata>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct UserMetadata {
    pub username: String,
    pub display_name: String,
    pub avatar_url: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct TeamMatchEvent {
    pub handle: String,
    pub email: String,
    pub name: String,
    pub display_name: String,
}

#[derive(Debug, Clone, PartialEq)]
pub enum EventMatch {
    Content(ContentMatchEvent),
    Path(PathMatchEvent),
    Symbol(SymbolMatchEvent),
    Repo(RepoMatchEvent),
    Commit(CommitMatchEvent),
    Person(PersonMatchEvent),
    Team(TeamMatchEvent),
}

impl EventMatch {
    pub fn encode(&self) -> String {
        let mut out = String::from("{");
        match self {
            EventMatch::Content(e) => encode_content_match(&mut out, e),
            EventMatch::Path(e) => encode_path_match(&mut out, e),
            EventMatch::Symbol(e) => encode_symbol_match(&mut out, e),
            EventMatch::Repo(e) => encode_repo_match(&mut out, e),
            EventMatch::Commit(e) => encode_commit_match(&mut out, e),
            EventMatch::Person(e) => encode_person_match(&mut out, e),
            EventMatch::Team(e) => encode_team_match(&mut out, e),
        }
        out.push('}');
        out
    }
}

fn encode_content_match(out: &mut String, e: &ContentMatchEvent) {
    json::field_str(out, "type", "content");
    json::field_str(out, "path", &e.path);
    if !e.path_matches.is_empty() {
        json::separator_public(out);
        json::push_key(out, "pathMatches");
        encode_ranges(out, &e.path_matches);
    }
    json::field_int(out, "repositoryID", i64::from(e.repository_id));
    json::field_str(out, "repository", &e.repository);
    json::opt_int(out, "repoStars", i64::from(e.repo_stars));
    json::opt_time(out, "repoLastFetched", e.repo_last_fetched);
    json::opt_strings(out, "branches", e.branches.as_deref());
    json::opt_str(out, "commit", &e.commit);
    // `hunks` has no `omitempty` and is never set by the baseline.
    json::separator_public(out);
    json::push_key(out, "hunks");
    out.push_str("null");
    if let Some(line_matches) = &e.line_matches {
        if !line_matches.is_empty() {
            json::separator_public(out);
            json::push_key(out, "lineMatches");
            out.push('[');
            for (i, lm) in line_matches.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                encode_line_match(out, lm);
            }
            out.push(']');
        }
    }
    if let Some(chunk_matches) = &e.chunk_matches {
        if !chunk_matches.is_empty() {
            json::separator_public(out);
            json::push_key(out, "chunkMatches");
            out.push('[');
            for (i, cm) in chunk_matches.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                encode_chunk_match(out, cm);
            }
            out.push(']');
        }
    }
    json::opt_str(out, "language", &e.language);
    json::opt_str(out, "debug", &e.debug);
}

fn encode_path_match(out: &mut String, e: &PathMatchEvent) {
    json::field_str(out, "type", "path");
    json::field_str(out, "path", &e.path);
    if !e.path_matches.is_empty() {
        json::separator_public(out);
        json::push_key(out, "pathMatches");
        encode_ranges(out, &e.path_matches);
    }
    json::field_int(out, "repositoryID", i64::from(e.repository_id));
    json::field_str(out, "repository", &e.repository);
    json::opt_int(out, "repoStars", i64::from(e.repo_stars));
    json::opt_time(out, "repoLastFetched", e.repo_last_fetched);
    json::opt_strings(out, "branches", e.branches.as_deref());
    json::opt_str(out, "commit", &e.commit);
    json::opt_str(out, "language", &e.language);
    json::opt_str(out, "debug", &e.debug);
}

fn encode_symbol_match(out: &mut String, e: &SymbolMatchEvent) {
    json::field_str(out, "type", "symbol");
    json::field_str(out, "path", &e.path);
    json::field_int(out, "repositoryID", i64::from(e.repository_id));
    json::field_str(out, "repository", &e.repository);
    json::opt_int(out, "repoStars", i64::from(e.repo_stars));
    json::opt_time(out, "repoLastFetched", e.repo_last_fetched);
    json::opt_strings(out, "branches", e.branches.as_deref());
    json::opt_str(out, "commit", &e.commit);
    json::opt_str(out, "language", &e.language);
    json::separator_public(out);
    json::push_key(out, "symbols");
    out.push('[');
    for (i, symbol) in e.symbols.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        out.push('{');
        json::field_str(out, "url", &symbol.url);
        json::field_str(out, "name", &symbol.name);
        json::field_str(out, "containerName", &symbol.container_name);
        json::field_str(out, "kind", &symbol.kind);
        json::field_int(out, "line", i64::from(symbol.line));
        out.push('}');
    }
    out.push(']');
}

fn encode_repo_match(out: &mut String, e: &RepoMatchEvent) {
    json::field_str(out, "type", "repo");
    json::field_int(out, "repositoryID", i64::from(e.repository_id));
    json::field_str(out, "repository", &e.repository);
    if !e.repository_matches.is_empty() {
        json::separator_public(out);
        json::push_key(out, "repositoryMatches");
        encode_ranges(out, &e.repository_matches);
    }
    json::opt_strings(out, "branches", e.branches.as_deref());
    json::opt_int(out, "repoStars", i64::from(e.repo_stars));
    json::opt_time(out, "repoLastFetched", e.repo_last_fetched);
    json::opt_str(out, "description", &e.description);
    if !e.description_matches.is_empty() {
        json::separator_public(out);
        json::push_key(out, "descriptionMatches");
        encode_ranges(out, &e.description_matches);
    }
    json::opt_bool(out, "fork", e.fork);
    json::opt_bool(out, "archived", e.archived);
    json::opt_bool(out, "private", e.private);
    json::opt_string_map(out, "metadata", e.metadata.as_ref());
    json::opt_strings(out, "topics", e.topics.as_deref());
}

fn encode_commit_match(out: &mut String, e: &CommitMatchEvent) {
    json::field_str(out, "type", "commit");
    json::field_str(out, "label", &e.label);
    json::field_str(out, "url", &e.url);
    json::field_str(out, "detail", &e.detail);
    json::field_int(out, "repositoryID", i64::from(e.repository_id));
    json::field_str(out, "repository", &e.repository);
    json::field_str(out, "oid", &e.oid);
    json::field_str(out, "message", &e.message);
    json::field_str(out, "authorName", &e.author_name);
    json::separator_public(out);
    json::push_key(out, "authorDate");
    json::push_time(out, e.author_date);
    json::field_str(out, "committerName", &e.committer_name);
    json::separator_public(out);
    json::push_key(out, "committerDate");
    json::push_time(out, e.committer_date);
    json::opt_int(out, "repoStars", i64::from(e.repo_stars));
    json::opt_time(out, "repoLastFetched", e.repo_last_fetched);
    json::field_str(out, "content", &e.content);
    json::separator_public(out);
    json::push_key(out, "ranges");
    out.push('[');
    for (i, range) in e.ranges.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        out.push_str(&format!("[{},{},{}]", range[0], range[1], range[2]));
    }
    out.push(']');
}

fn encode_person_match(out: &mut String, e: &PersonMatchEvent) {
    json::field_str(out, "type", "person");
    json::field_str(out, "handle", &e.handle);
    json::field_str(out, "email", &e.email);
    if let Some(user) = &e.user {
        json::separator_public(out);
        json::push_key(out, "user");
        out.push('{');
        json::field_str(out, "username", &user.username);
        json::field_str(out, "displayName", &user.display_name);
        json::field_str(out, "avatarURL", &user.avatar_url);
        out.push('}');
    }
}

fn encode_team_match(out: &mut String, e: &TeamMatchEvent) {
    json::field_str(out, "type", "team");
    json::field_str(out, "handle", &e.handle);
    json::field_str(out, "email", &e.email);
    json::field_str(out, "name", &e.name);
    json::field_str(out, "displayName", &e.display_name);
}

fn encode_location(out: &mut String, location: &Location) {
    out.push('{');
    json::field_int(out, "offset", location.offset);
    json::field_int(out, "line", location.line);
    json::field_int(out, "column", location.column);
    out.push('}');
}

fn encode_ranges(out: &mut String, ranges: &[Range]) {
    out.push('[');
    for (i, range) in ranges.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        out.push('{');
        json::separator_public(out);
        json::push_key(out, "start");
        encode_location(out, &range.start);
        json::separator_public(out);
        json::push_key(out, "end");
        encode_location(out, &range.end);
        out.push('}');
    }
    out.push(']');
}

fn encode_line_match(out: &mut String, lm: &LineMatchEvent) {
    out.push('{');
    json::field_str(out, "line", &lm.line);
    json::field_int(out, "lineNumber", i64::from(lm.line_number));
    json::separator_public(out);
    json::push_key(out, "offsetAndLengths");
    out.push('[');
    for (i, pair) in lm.offset_and_lengths.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        out.push_str(&format!("[{},{}]", pair[0], pair[1]));
    }
    out.push(']');
    out.push('}');
}

fn encode_chunk_match(out: &mut String, cm: &ChunkMatchEvent) {
    out.push('{');
    json::field_str(out, "content", &cm.content);
    json::separator_public(out);
    json::push_key(out, "contentStart");
    encode_location(out, &cm.content_start);
    json::separator_public(out);
    json::push_key(out, "ranges");
    encode_ranges(out, &cm.ranges);
    json::opt_bool(out, "contentTruncated", cm.content_truncated);
    out.push('}');
}

/// `http.EventFilter`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventFilter {
    pub value: String,
    pub label: String,
    pub count: i64,
    pub exhaustive: bool,
    pub kind: String,
}

pub fn encode_filters_json(filters: &[EventFilter]) -> String {
    let mut data = String::from("[");
    for (i, f) in filters.iter().enumerate() {
        if i > 0 {
            data.push(',');
        }
        data.push('{');
        json::field_str(&mut data, "value", &f.value);
        json::field_str(&mut data, "label", &f.label);
        json::field_int(&mut data, "count", f.count);
        json::field_bool(&mut data, "exhaustive", f.exhaustive);
        json::field_str(&mut data, "kind", &f.kind);
        data.push('}');
    }
    data.push(']');
    data
}

/// `http.EventError`.
pub fn encode_error(message: &str) -> String {
    let mut out = String::from("{");
    json::field_str(&mut out, "message", message);
    out.push('}');
    out
}

/// `search.Alert` after `eventWriter.Alert` conversion.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventAlert {
    pub title: String,
    pub description: String,
    pub kind: String,
    pub proposed_queries: Option<Vec<QueryDescription>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryDescription {
    pub description: String,
    pub query: String,
    pub annotations: Vec<Annotation>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Annotation {
    pub name: String,
    pub value: String,
}

/// `search.AlertForQuery` (`internal/search/alert.go:84-97`).
pub fn alert_for_query(message: &str, expected_operand: bool) -> EventAlert {
    if expected_operand {
        return EventAlert {
            title: "Unable To Process Query".to_owned(),
            description: "I'm having trouble understanding that query. Putting parentheses around the search pattern may help.".to_owned(),
            kind: String::new(),
            proposed_queries: None,
        };
    }
    EventAlert {
        title: "Unable To Process Query".to_owned(),
        description: cap_first(message),
        kind: String::new(),
        proposed_queries: None,
    }
}

/// `capFirst`: title-case the first rune, leave the rest untouched.
fn cap_first(value: &str) -> String {
    let mut chars = value.chars();
    match chars.next() {
        None => String::new(),
        Some(first) => first.to_uppercase().collect::<String>() + chars.as_str(),
    }
}

pub fn encode_alert(alert: &EventAlert) -> String {
    let mut out = String::from("{");
    json::field_str(&mut out, "title", &alert.title);
    json::opt_str(&mut out, "description", &alert.description);
    json::opt_str(&mut out, "kind", &alert.kind);
    // `proposedQueries` has no `omitempty`: nil marshals as `null`.
    json::separator_public(&mut out);
    json::push_key(&mut out, "proposedQueries");
    match &alert.proposed_queries {
        None => out.push_str("null"),
        Some(queries) => {
            out.push('[');
            for (i, pq) in queries.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                out.push('{');
                json::opt_str(&mut out, "description", &pq.description);
                json::field_str(&mut out, "query", &pq.query);
                if !pq.annotations.is_empty() {
                    json::separator_public(&mut out);
                    json::push_key(&mut out, "annotations");
                    out.push('[');
                    for (j, annotation) in pq.annotations.iter().enumerate() {
                        if j > 0 {
                            out.push(',');
                        }
                        out.push('{');
                        json::field_str(&mut out, "name", &annotation.name);
                        json::field_str(&mut out, "value", &annotation.value);
                        out.push('}');
                    }
                    out.push(']');
                }
                out.push('}');
            }
            out.push(']');
        }
    }
    out.push('}');
    out
}

// ---------------------------------------------------------------------------
// `protocol.CommitMatch` JSON
// ---------------------------------------------------------------------------

/// `encoding/json` of `protocol.CommitMatch`
/// (`internal/gitserver/protocol/gitserver.go:93-103`).
///
/// This is the wire form of the richer commit result model
/// ([`CommitMatch`](super::convert::CommitMatch)); it is deliberately separate
/// from [`encode_commit_match`], which encodes the streaming `http.EventCommitMatch`
/// shape (`events.go:151-171`) and must stay byte-identical to the baseline.
/// Field names, declaration order, `omitempty` behaviour and string escaping
/// match Go's `encoding/json`.
pub fn encode_protocol_commit_match(commit: &CommitMatch) -> String {
    let mut out = String::from("{");
    protocol_field_str(&mut out, "Oid", &commit.commit.id);
    json::separator_public(&mut out);
    json::push_key(&mut out, "Author");
    encode_protocol_signature(&mut out, &commit.commit.author);
    json::separator_public(&mut out);
    json::push_key(&mut out, "Committer");
    encode_protocol_signature(&mut out, &commit.commit.committer);
    protocol_opt_strings(&mut out, "Parents", &commit.commit.parents);
    protocol_opt_strings(&mut out, "Refs", &commit.refs);
    protocol_opt_strings(&mut out, "SourceRefs", &commit.source_refs);
    json::separator_public(&mut out);
    json::push_key(&mut out, "Message");
    let message_ranges = commit
        .message_preview
        .as_ref()
        .map(|preview| preview.matched_ranges.as_slice())
        .unwrap_or(&[]);
    encode_protocol_matched_string(&mut out, &commit.commit.message, message_ranges);
    json::separator_public(&mut out);
    json::push_key(&mut out, "Diff");
    let (diff_content, diff_ranges) = match &commit.diff_preview {
        Some(preview) => (preview.content.as_str(), preview.matched_ranges.as_slice()),
        None => ("", &[][..]),
    };
    encode_protocol_matched_string(&mut out, diff_content, diff_ranges);
    protocol_opt_strings(&mut out, "ModifiedFiles", &commit.modified_files);
    out.push('}');
    out
}

/// `encoding/json` string escaping. It differs from [`json::push_quoted`] only
/// for backspace/form feed, which Go 1.22 emits as the short escapes `\b`/`\f`
/// (`encoding/json/encode.go:appendString`).
fn push_protocol_quoted(out: &mut String, value: &str) {
    out.push('"');
    for ch in value.chars() {
        match ch {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\u{8}' => out.push_str("\\b"),
            '\u{c}' => out.push_str("\\f"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            '<' => out.push_str("\\u003c"),
            '>' => out.push_str("\\u003e"),
            '&' => out.push_str("\\u0026"),
            '\u{2028}' => out.push_str("\\u2028"),
            '\u{2029}' => out.push_str("\\u2029"),
            c if (c as u32) < 0x20 => {
                out.push_str(&format!("\\u{:04x}", c as u32));
            }
            c => out.push(c),
        }
    }
    out.push('"');
}

fn protocol_field_str(out: &mut String, key: &str, value: &str) {
    if !out.ends_with('{') {
        out.push(',');
    }
    json::push_key(out, key);
    push_protocol_quoted(out, value);
}

fn protocol_opt_str(out: &mut String, key: &str, value: &str) {
    if !value.is_empty() {
        protocol_field_str(out, key, value);
    }
}

fn protocol_opt_strings(out: &mut String, key: &str, values: &[String]) {
    if values.is_empty() {
        return;
    }
    if !out.ends_with('{') {
        out.push(',');
    }
    json::push_key(out, key);
    out.push('[');
    for (i, value) in values.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        push_protocol_quoted(out, value);
    }
    out.push(']');
}

fn encode_protocol_signature(out: &mut String, signature: &Signature) {
    out.push('{');
    protocol_opt_str(out, "Name", &signature.name);
    protocol_opt_str(out, "Email", &signature.email);
    json::separator_public(out);
    json::push_key(out, "Date");
    json::push_time(out, signature.date);
    out.push('}');
}

fn encode_protocol_matched_string(out: &mut String, content: &str, ranges: &[Range]) {
    out.push('{');
    protocol_field_str(out, "content", content);
    json::separator_public(out);
    json::push_key(out, "matchedRanges");
    out.push('[');
    for (i, range) in ranges.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        out.push('{');
        json::separator_public(out);
        json::push_key(out, "start");
        encode_protocol_location(out, &range.start);
        json::separator_public(out);
        json::push_key(out, "end");
        encode_protocol_location(out, &range.end);
        out.push('}');
    }
    out.push(']');
    out.push('}');
}

/// `result.Location.MarshalJSON`: `[offset,line,column]`.
fn encode_protocol_location(out: &mut String, location: &Location) {
    out.push('[');
    out.push_str(&location.offset.to_string());
    out.push(',');
    out.push_str(&location.line.to_string());
    out.push(',');
    out.push_str(&location.column.to_string());
    out.push(']');
}
