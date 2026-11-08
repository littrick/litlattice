//! Commit search over the `git log` of a repository.
//!
//! Baseline: `cmd/gitserver/internal/search` and the `Search` RPC in
//! `cmd/gitserver/internal/server_grpc.go`. The Go implementation streams
//! `git log --decorate=full -z --format=format:%x1E…` and splits the output on
//! the ASCII record separator (0x1E); every commit starts with that byte, so
//! the scan is done with a small streaming parser instead of line splitting.
//!
//! Implemented: the full query node set (`author`, `committer`, `before`,
//! `after`, `message`, `diff`, `file`, booleans and operators), revision
//! expansion, the `limit`/`limit_hit` semantics, `include_modified_files`
//! (including the baseline's implicit `--name-status` when the query contains
//! a `DiffModifiesFile` node) and `include_diff`.
//!
//! Diff queries run against a persistent `git diff-tree --stdin` subprocess
//! (`search.DiffFetcher`): hashes are written one at a time, followed by the
//! bogus `ENDOFPATCH` revision whose echoed name delimits the patch. Patches
//! are parsed by [`crate::diff`], a port of go-diff, and the formatted output
//! (`FormatDiff` with its 1 context line / 5 lines per hunk / 3 hunks per file
//! / 5 files limits) mirrors the baseline byte for byte.
//!
//! Known divergence: the baseline compiles case-insensitive patterns by
//! lower-casing the pattern and the haystack with an ASCII-only table
//! (`internal/search/casetransform`). The Rust implementation switches on the
//! regex crate's `case_insensitive` flag instead, which also folds non-ASCII
//! characters. Fixtures use ASCII patterns, where both are identical.

use std::collections::{BTreeMap, BTreeSet};
use std::ffi::OsString;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, ChildStdin, ChildStdout};
use tokio_util::task::TaskTracker;

use sg_proto::gitserver as gpb;

use crate::diff::{self, FileDiff, Hunk};
use crate::git::GitRepo;

/// The baseline `commitSeparator`: ASCII record separator.
const COMMIT_SEPARATOR: u8 = 0x1e;

/// `bufio.Scanner.Buffer(make([]byte, 1024), 1<<22)`: a token may not exceed
/// 4 MiB. The baseline swallows the resulting scanner error.
const MAX_TOKEN_SIZE: usize = 1 << 22;

/// The baseline `commitFields` in wire order (11 NUL-separated fields).
const COMMIT_FIELDS: usize = 11;

/// Baseline `search.logArgs`, with the record separator in front of each
/// commit and NUL between fields.
const LOG_FORMAT: &str = "--format=format:%x1E%H%x00%D%x00%S%x00%aN%x00%aE%x00%at%x00%cN%x00\
     %cE%x00%ct%x00%B%x00%P%x00";

/// Baseline `diff_format.go` constants.
const MATCH_CONTEXT_LINES: usize = 1;
const MAX_LINES_PER_HUNK: usize = 5;
const MAX_HUNKS_PER_FILE: usize = 3;
const MAX_FILES: usize = 5;

/// The marker the diff fetcher appends to every request so the subprocess
/// echoes it after the patch (`search.DiffFetcher`).
const END_OF_PATCH: &[u8] = b"ENDOFPATCH\n";

/// Errors surfaced by [`search`].
#[derive(Debug, PartialEq, Eq)]
pub enum SearchError {
    /// `NodeFromProto` failed: the baseline answers `INVALID_ARGUMENT`.
    InvalidArgument(String),
    /// A feature the Rust gitserver does not implement yet.
    Unsupported(String),
    /// Any other fault; the baseline answers `UNKNOWN`.
    Message(String),
}

impl std::fmt::Display for SearchError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidArgument(message)
            | Self::Unsupported(message)
            | Self::Message(message) => formatter.write_str(message),
        }
    }
}

// ---------------------------------------------------------------------------
// Request conversion (`protocol.NodeFromProto`)
// ---------------------------------------------------------------------------

/// Baseline `protocol.OperatorKind`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OperatorKind {
    And,
    Or,
    Not,
}

/// Baseline `protocol.Node`: the wire query tree before regex compilation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum QueryNode {
    AuthorMatches {
        expr: String,
        ignore_case: bool,
    },
    CommitterMatches {
        expr: String,
        ignore_case: bool,
    },
    CommitBefore {
        seconds: i64,
        nanos: i32,
    },
    CommitAfter {
        seconds: i64,
        nanos: i32,
    },
    MessageMatches {
        expr: String,
        ignore_case: bool,
    },
    DiffMatches {
        expr: String,
        ignore_case: bool,
    },
    DiffModifiesFile {
        expr: String,
        ignore_case: bool,
    },
    Boolean(bool),
    Operator {
        kind: OperatorKind,
        operands: Vec<QueryNode>,
    },
}

/// Baseline `protocol.SearchRequest`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SearchRequest {
    pub revisions: Vec<String>,
    pub limit: i64,
    pub include_diff: bool,
    pub include_modified_files: bool,
    pub query: QueryNode,
}

impl SearchRequest {
    /// Baseline `SearchRequestFromProto`. GraphQL-level validation (the
    /// required `repo`) happens in the gRPC handler, after conversion.
    pub fn from_proto(request: &gpb::SearchRequest) -> Result<Self, SearchError> {
        Ok(Self {
            revisions: request
                .revisions
                .iter()
                .map(|revision| revision.rev_spec.clone())
                .collect(),
            limit: request.limit,
            include_diff: request.include_diff,
            include_modified_files: request.include_modified_files,
            query: node_from_proto(request.query.as_ref())?,
        })
    }
}

fn invalid_argument(message: impl Into<String>) -> SearchError {
    SearchError::InvalidArgument(message.into())
}

fn node_from_proto(node: Option<&gpb::QueryNode>) -> Result<QueryNode, SearchError> {
    use gpb::query_node;

    let value = node
        .and_then(|node| node.value.as_ref())
        .ok_or_else(|| invalid_argument("unknown query node type <nil>"))?;
    Ok(match value {
        query_node::Value::AuthorMatches(node) => QueryNode::AuthorMatches {
            expr: node.expr.clone(),
            ignore_case: node.ignore_case,
        },
        query_node::Value::CommitterMatches(node) => QueryNode::CommitterMatches {
            expr: node.expr.clone(),
            ignore_case: node.ignore_case,
        },
        // `GetTimestamp().AsTime()` on a missing timestamp is the Unix epoch.
        query_node::Value::CommitBefore(node) => QueryNode::CommitBefore {
            seconds: node.timestamp.as_ref().map_or(0, |time| time.seconds),
            nanos: node.timestamp.as_ref().map_or(0, |time| time.nanos),
        },
        query_node::Value::CommitAfter(node) => QueryNode::CommitAfter {
            seconds: node.timestamp.as_ref().map_or(0, |time| time.seconds),
            nanos: node.timestamp.as_ref().map_or(0, |time| time.nanos),
        },
        query_node::Value::MessageMatches(node) => QueryNode::MessageMatches {
            expr: node.expr.clone(),
            ignore_case: node.ignore_case,
        },
        query_node::Value::DiffMatches(node) => QueryNode::DiffMatches {
            expr: node.expr.clone(),
            ignore_case: node.ignore_case,
        },
        query_node::Value::DiffModifiesFile(node) => QueryNode::DiffModifiesFile {
            expr: node.expr.clone(),
            ignore_case: node.ignore_case,
        },
        query_node::Value::Boolean(node) => QueryNode::Boolean(node.value),
        query_node::Value::Operator(node) => {
            let kind = match node.kind {
                1 => OperatorKind::And,
                2 => OperatorKind::Or,
                3 => OperatorKind::Not,
                other => {
                    return Err(invalid_argument(format!(
                        "unknown operator kind {}",
                        operator_kind_name(other)
                    )))
                }
            };
            let mut operands = Vec::with_capacity(node.operands.len());
            for operand in &node.operands {
                operands.push(node_from_proto(Some(operand))?);
            }
            QueryNode::Operator { kind, operands }
        }
    })
}

/// Proto enum `String()`: the declared name for known values, the decimal
/// number for unrecognized ones.
fn operator_kind_name(value: i32) -> String {
    match gpb::OperatorKind::try_from(value) {
        Ok(kind) => kind.as_str_name().to_owned(),
        Err(_) => value.to_string(),
    }
}

// ---------------------------------------------------------------------------
// Match tree (`ToMatchTree`)
// ---------------------------------------------------------------------------

/// A compiled pattern. `ignore_case` mirrors `casetransform.CompileRegexp`,
/// with the case-folding divergence documented at the module level.
struct Regexp {
    re: regex::bytes::Regex,
}

impl Regexp {
    fn compile(expr: &str, ignore_case: bool) -> Result<Self, SearchError> {
        let re = regex::bytes::RegexBuilder::new(expr)
            .case_insensitive(ignore_case)
            .build()
            .map_err(|err| SearchError::Message(format!("error parsing regexp: {err}")))?;
        Ok(Self { re })
    }

    fn is_match(&self, haystack: &[u8]) -> bool {
        self.re.is_match(haystack)
    }

    /// `regexp.FindAllIndex(b, -1)`.
    fn find_all(&self, haystack: &[u8]) -> Vec<(usize, usize)> {
        self.re
            .find_iter(haystack)
            .map(|found| (found.start(), found.end()))
            .collect()
    }
}

/// A timestamp from a `CommitBefore`/`CommitAfter` node.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct TimestampValue {
    seconds: i64,
    nanos: i32,
}

type MatchFuture<'a> = Pin<
    Box<dyn Future<Output = Result<(CommitFilterResult, MatchedCommit), SearchError>> + Send + 'a>,
>;

/// Baseline `search.MatchTree`.
enum MatchTree {
    AuthorMatches(Regexp),
    CommitterMatches(Regexp),
    CommitBefore(TimestampValue),
    CommitAfter(TimestampValue),
    MessageMatches(Regexp),
    DiffMatches(Regexp),
    DiffModifiesFile(Regexp),
    Boolean(bool),
    Operator {
        kind: OperatorKind,
        operands: Vec<MatchTree>,
    },
}

impl MatchTree {
    /// Baseline `ToMatchTree`.
    fn compile(node: &QueryNode) -> Result<Self, SearchError> {
        Ok(match node {
            QueryNode::AuthorMatches { expr, ignore_case } => {
                Self::AuthorMatches(Regexp::compile(expr, *ignore_case)?)
            }
            QueryNode::CommitterMatches { expr, ignore_case } => {
                Self::CommitterMatches(Regexp::compile(expr, *ignore_case)?)
            }
            QueryNode::CommitBefore { seconds, nanos } => Self::CommitBefore(TimestampValue {
                seconds: *seconds,
                nanos: *nanos,
            }),
            QueryNode::CommitAfter { seconds, nanos } => Self::CommitAfter(TimestampValue {
                seconds: *seconds,
                nanos: *nanos,
            }),
            QueryNode::MessageMatches { expr, ignore_case } => {
                Self::MessageMatches(Regexp::compile(expr, *ignore_case)?)
            }
            QueryNode::DiffMatches { expr, ignore_case } => {
                Self::DiffMatches(Regexp::compile(expr, *ignore_case)?)
            }
            QueryNode::DiffModifiesFile { expr, ignore_case } => {
                Self::DiffModifiesFile(Regexp::compile(expr, *ignore_case)?)
            }
            QueryNode::Boolean(value) => Self::Boolean(*value),
            QueryNode::Operator { kind, operands } => {
                let mut compiled = Vec::with_capacity(operands.len());
                for operand in operands {
                    compiled.push(Self::compile(operand)?);
                }
                Self::Operator {
                    kind: *kind,
                    operands: compiled,
                }
            }
        })
    }

    /// Baseline `MatchTree.Match`. Async because diff nodes may have to fetch
    /// and parse a patch; the recursion is boxed for that reason.
    fn matches<'a, 'b>(&'a self, commit: &'a mut LazyCommit<'b>) -> MatchFuture<'a> {
        Box::pin(async move {
            match self {
                Self::AuthorMatches(regexp) => {
                    let matched = regexp.is_match(&commit.raw.author_name)
                        || regexp.is_match(&commit.raw.author_email);
                    Ok((
                        CommitFilterResult::matched(matched),
                        MatchedCommit::default(),
                    ))
                }
                Self::CommitterMatches(regexp) => {
                    let matched = regexp.is_match(&commit.raw.committer_name)
                        || regexp.is_match(&commit.raw.committer_email);
                    Ok((
                        CommitFilterResult::matched(matched),
                        MatchedCommit::default(),
                    ))
                }
                Self::CommitBefore(time) => {
                    let date = commit.committer_date()?;
                    Ok((
                        CommitFilterResult::matched(compare_timestamps(date, *time).is_lt()),
                        MatchedCommit::default(),
                    ))
                }
                Self::CommitAfter(time) => {
                    let date = commit.committer_date()?;
                    Ok((
                        CommitFilterResult::matched(compare_timestamps(date, *time).is_gt()),
                        MatchedCommit::default(),
                    ))
                }
                Self::MessageMatches(regexp) => {
                    let found = regexp.find_all(&commit.raw.message);
                    if found.is_empty() {
                        return Ok((CommitFilterResult::matched(false), MatchedCommit::default()));
                    }
                    Ok((
                        CommitFilterResult::matched(true),
                        MatchedCommit {
                            message: matches_to_ranges(&commit.raw.message, &found),
                            diff: BTreeMap::new(),
                        },
                    ))
                }
                Self::DiffMatches(regexp) => {
                    let diff = commit.diff().await?;
                    let mut file_diff_highlights: BTreeMap<usize, MatchedFileDiff> =
                        BTreeMap::new();
                    let mut matched_file_diffs: BTreeSet<usize> = BTreeSet::new();
                    for (file_index, file_diff) in diff.iter().enumerate() {
                        let mut hunk_highlights: BTreeMap<usize, MatchedHunk> = BTreeMap::new();
                        for (hunk_index, hunk) in file_diff.hunks.iter().enumerate() {
                            let mut line_highlights: BTreeMap<usize, Vec<MatchRange>> =
                                BTreeMap::new();
                            for (line_index, line) in ByteLines::new(&hunk.body).enumerate() {
                                if line.is_empty() {
                                    continue;
                                }
                                if !matches!(line[0], b'+' | b'-') {
                                    continue;
                                }
                                let content = &line[1..];
                                let found = regexp.find_all(content);
                                if !found.is_empty() {
                                    line_highlights
                                        .insert(line_index, matches_to_ranges(content, &found));
                                }
                            }
                            if !line_highlights.is_empty() {
                                hunk_highlights.insert(
                                    hunk_index,
                                    MatchedHunk {
                                        matched_lines: line_highlights,
                                    },
                                );
                            }
                        }
                        if !hunk_highlights.is_empty() {
                            file_diff_highlights.insert(
                                file_index,
                                MatchedFileDiff {
                                    matched_hunks: hunk_highlights,
                                    ..MatchedFileDiff::default()
                                },
                            );
                            matched_file_diffs.insert(file_index);
                        }
                    }
                    Ok((
                        CommitFilterResult {
                            commit_matched: false,
                            matched_file_diffs: Some(matched_file_diffs),
                        },
                        MatchedCommit {
                            message: Vec::new(),
                            diff: file_diff_highlights,
                        },
                    ))
                }
                Self::DiffModifiesFile(regexp) => {
                    // Pre-filter with the cheaper `--name-status` output.
                    let found_match = commit
                        .modified_files()
                        .iter()
                        .any(|file| regexp.is_match(file.as_bytes()));
                    if !found_match {
                        return Ok((CommitFilterResult::matched(false), MatchedCommit::default()));
                    }

                    let diff = commit.diff().await?;
                    let mut file_diff_highlights: BTreeMap<usize, MatchedFileDiff> =
                        BTreeMap::new();
                    let mut matched_file_diffs: BTreeSet<usize> = BTreeSet::new();
                    for (file_index, file_diff) in diff.iter().enumerate() {
                        let old_matches = regexp.find_all(&file_diff.orig_name);
                        let new_matches = regexp.find_all(&file_diff.new_name);
                        if !old_matches.is_empty() || !new_matches.is_empty() {
                            file_diff_highlights.insert(
                                file_index,
                                MatchedFileDiff {
                                    old_file: matches_to_ranges(&file_diff.orig_name, &old_matches),
                                    new_file: matches_to_ranges(&file_diff.new_name, &new_matches),
                                    matched_hunks: BTreeMap::new(),
                                },
                            );
                            matched_file_diffs.insert(file_index);
                        }
                    }
                    Ok((
                        CommitFilterResult {
                            commit_matched: false,
                            matched_file_diffs: Some(matched_file_diffs),
                        },
                        MatchedCommit {
                            message: Vec::new(),
                            diff: file_diff_highlights,
                        },
                    ))
                }
                Self::Boolean(value) => Ok((
                    CommitFilterResult::matched(*value),
                    MatchedCommit::default(),
                )),
                Self::Operator { kind, operands } => match kind {
                    OperatorKind::Not => {
                        let Some(operand) = operands.first() else {
                            return Err(SearchError::Message(
                                "operator NOT has no operands".to_owned(),
                            ));
                        };
                        let (mut result, _) = operand.matches(&mut *commit).await?;
                        invert(&mut result, &mut *commit).await?;
                        Ok((result, MatchedCommit::default()))
                    }
                    OperatorKind::And => {
                        let mut result_matches = MatchedCommit::default();
                        // Start with everything matching, then intersect.
                        let mut merged = CommitFilterResult {
                            commit_matched: true,
                            matched_file_diffs: None,
                        };
                        for operand in operands {
                            let (result, matches) = operand.matches(&mut *commit).await?;
                            merged.intersect(&result);
                            if !merged.satisfies() {
                                return Ok((
                                    CommitFilterResult::matched(false),
                                    MatchedCommit::default(),
                                ));
                            }
                            result_matches = result_matches.merge(matches);
                        }
                        result_matches.constrain_to_matched(&merged.matched_file_diffs);
                        Ok((merged, result_matches))
                    }
                    OperatorKind::Or => {
                        let mut result_matches = MatchedCommit::default();
                        // Start with no matches, then union.
                        let mut merged = CommitFilterResult {
                            commit_matched: false,
                            matched_file_diffs: Some(BTreeSet::new()),
                        };
                        for operand in operands {
                            let (result, matches) = operand.matches(&mut *commit).await?;
                            merged.union(&result);
                            if merged.satisfies() {
                                result_matches = result_matches.merge(matches);
                            }
                        }
                        result_matches.constrain_to_matched(&merged.matched_file_diffs);
                        Ok((merged, result_matches))
                    }
                },
            }
        })
    }

    /// Baseline `Visit` + `doSearch`'s `hasDiffModifiesFile`: any
    /// `DiffModifiesFile` node forces `--name-status` on `git log`.
    fn has_diff_modifies_file(&self) -> bool {
        match self {
            Self::DiffModifiesFile(_) => true,
            Self::Operator { operands, .. } => {
                operands.iter().any(MatchTree::has_diff_modifies_file)
            }
            _ => false,
        }
    }
}

/// Baseline `CommitFilterResult.Invert`, which needs the number of file diffs
/// in the commit.
async fn invert<'b>(
    result: &mut CommitFilterResult,
    commit: &mut LazyCommit<'b>,
) -> Result<(), SearchError> {
    result.commit_matched = !result.commit_matched;
    match result.matched_file_diffs.take() {
        None => result.matched_file_diffs = Some(BTreeSet::new()),
        Some(diffs) if diffs.is_empty() => result.matched_file_diffs = None,
        Some(diffs) => {
            let diff = commit.diff().await?;
            let mut inverted = BTreeSet::new();
            for index in 0..diff.len() {
                if !diffs.contains(&index) {
                    inverted.insert(index);
                }
            }
            result.matched_file_diffs = Some(inverted);
        }
    }
    Ok(())
}

fn compare_timestamps(left: TimestampValue, right: TimestampValue) -> std::cmp::Ordering {
    (left.seconds, left.nanos).cmp(&(right.seconds, right.nanos))
}

// ---------------------------------------------------------------------------
// Highlights (`search.MatchedCommit`)
// ---------------------------------------------------------------------------

/// Baseline `result.Location`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Location {
    pub offset: u32,
    pub line: u32,
    pub column: u32,
}

/// Baseline `result.Range`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct MatchRange {
    pub start: Location,
    pub end: Location,
}

/// Baseline `search.Mat(c)hedCommit`: the portions of a commit that matched.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct MatchedCommit {
    pub message: Vec<MatchRange>,
    /// Keyed by the file diff index; empty while diff search is unimplemented.
    pub diff: BTreeMap<usize, MatchedFileDiff>,
}

impl MatchedCommit {
    fn merge(mut self, other: Self) -> Self {
        self.message.extend(other.message);
        self.message.sort_by_key(|range| range.start.offset);
        if self.diff.is_empty() {
            self.diff = other.diff;
        } else {
            for (index, file) in other.diff {
                match self.diff.remove(&index) {
                    Some(existing) => {
                        self.diff.insert(index, existing.merge(file));
                    }
                    None => {
                        self.diff.insert(index, file);
                    }
                }
            }
        }
        self
    }

    fn constrain_to_matched(&mut self, matched_file_diffs: &Option<BTreeSet<usize>>) {
        match matched_file_diffs {
            None => self.diff.clear(),
            Some(matched) => self.diff.retain(|index, _| matched.contains(index)),
        }
    }
}

/// Baseline `search.MatchedFileDiff`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct MatchedFileDiff {
    pub old_file: Vec<MatchRange>,
    pub new_file: Vec<MatchRange>,
    pub matched_hunks: BTreeMap<usize, MatchedHunk>,
}

impl MatchedFileDiff {
    fn merge(mut self, other: Self) -> Self {
        self.old_file.extend(other.old_file);
        self.old_file.sort_by_key(|range| range.start.offset);
        self.new_file.extend(other.new_file);
        self.new_file.sort_by_key(|range| range.start.offset);
        if self.matched_hunks.is_empty() {
            self.matched_hunks = other.matched_hunks;
        } else {
            for (index, hunk) in other.matched_hunks {
                match self.matched_hunks.remove(&index) {
                    Some(existing) => {
                        self.matched_hunks.insert(index, existing.merge(hunk));
                    }
                    None => {
                        self.matched_hunks.insert(index, hunk);
                    }
                }
            }
        }
        self
    }
}

/// Baseline `search.MatchedHunk`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct MatchedHunk {
    pub matched_lines: BTreeMap<usize, Vec<MatchRange>>,
}

impl MatchedHunk {
    fn merge(mut self, other: Self) -> Self {
        if self.matched_lines.is_empty() {
            self.matched_lines = other.matched_lines;
        } else {
            for (index, lines) in other.matched_lines {
                match self.matched_lines.remove(&index) {
                    Some(mut existing) => {
                        existing.extend(lines);
                        existing.sort_by_key(|range| range.start.offset);
                        self.matched_lines.insert(index, existing);
                    }
                    None => {
                        self.matched_lines.insert(index, lines);
                    }
                }
            }
        }
        self
    }
}

/// Baseline `search.CommitFilterResult`. `matched_file_diffs == None` means
/// "unevaluated", which merges as "everything matches".
#[derive(Clone, Debug, PartialEq, Eq)]
struct CommitFilterResult {
    commit_matched: bool,
    matched_file_diffs: Option<BTreeSet<usize>>,
}

impl CommitFilterResult {
    /// Baseline `filterResult`.
    fn matched(value: bool) -> Self {
        Self {
            commit_matched: value,
            matched_file_diffs: if value { None } else { Some(BTreeSet::new()) },
        }
    }

    fn satisfies(&self) -> bool {
        if self.commit_matched {
            return true;
        }
        self.matched_file_diffs
            .as_ref()
            .is_some_and(|diffs| !diffs.is_empty())
    }

    fn union(&mut self, other: &Self) {
        self.commit_matched |= other.commit_matched;
        match (&mut self.matched_file_diffs, &other.matched_file_diffs) {
            (Some(diffs), Some(other_diffs)) => diffs.extend(other_diffs),
            _ => self.matched_file_diffs = None,
        }
    }

    fn intersect(&mut self, other: &Self) {
        self.commit_matched &= other.commit_matched;
        match (&mut self.matched_file_diffs, &other.matched_file_diffs) {
            (None, other_diffs) => self.matched_file_diffs = other_diffs.clone(),
            (Some(_), None) => {}
            (Some(diffs), Some(other_diffs)) => diffs.retain(|index| other_diffs.contains(index)),
        }
    }
}

// ---------------------------------------------------------------------------
// Ranges (the baseline `matchesToRanges`)
// ---------------------------------------------------------------------------

/// `regexp.FindAllStringIndex` results → `result.Ranges`. Matches are ordered
/// and non-overlapping, so one forward pass over the content is enough.
fn matches_to_ranges(content: &[u8], matches: &[(usize, usize)]) -> Vec<MatchRange> {
    let mut counter = RangeCounter::new();
    let mut ranges = Vec::with_capacity(matches.len());
    for &(start, end) in matches {
        let (start_line, start_column) = counter.line_column(content, start);
        let (end_line, end_column) = counter.line_column(content, end);
        ranges.push(MatchRange {
            start: Location {
                offset: start as u32,
                line: start_line,
                column: start_column,
            },
            end: Location {
                offset: end as u32,
                line: end_line,
                column: end_column,
            },
        });
    }
    ranges
}

/// The stateful half of `matchesToRanges`: line/column derived from the byte
/// offsets scanned since the previous match.
struct RangeCounter {
    unscanned_offset: usize,
    scanned_newlines: u32,
    last_scanned_newline_offset: i64,
}

impl RangeCounter {
    fn new() -> Self {
        Self {
            unscanned_offset: 0,
            scanned_newlines: 0,
            last_scanned_newline_offset: -1,
        }
    }

    fn line_column(&mut self, content: &[u8], byte_offset: usize) -> (u32, u32) {
        let unscanned = &content[self.unscanned_offset..byte_offset];
        if let Some(index) = unscanned.iter().rposition(|&byte| byte == b'\n') {
            self.last_scanned_newline_offset = (self.unscanned_offset + index) as i64;
            self.scanned_newlines += unscanned.iter().filter(|&&byte| byte == b'\n').count() as u32;
        }
        let start = (self.last_scanned_newline_offset + 1) as usize;
        let column = go_rune_count(&content[start..byte_offset]) as u32;
        self.unscanned_offset = byte_offset;
        (self.scanned_newlines, column)
    }
}

/// `utf8.RuneCount`: every invalid byte counts as one rune.
fn go_rune_count(bytes: &[u8]) -> usize {
    let mut count = 0;
    let mut index = 0;
    while index < bytes.len() {
        index += utf8_sequence_len(&bytes[index..]);
        count += 1;
    }
    count
}

/// `utf8.DecodeRune`'s width, or 1 for an invalid sequence.
fn utf8_sequence_len(rest: &[u8]) -> usize {
    let width = match rest[0] {
        0x00..=0x7f => 1,
        0xc2..=0xdf => 2,
        0xe0..=0xef => 3,
        0xf0..=0xf4 => 4,
        _ => 1,
    };
    if width == 1 {
        return 1;
    }
    match rest.get(..width) {
        Some(sequence) if std::str::from_utf8(sequence).is_ok() => width,
        _ => 1,
    }
}

/// `bytes.ToValidUTF8(b, "�")`: runs of invalid bytes collapse into a single
/// replacement character.
fn to_valid_utf8(bytes: &[u8]) -> String {
    let mut out = Vec::with_capacity(bytes.len());
    let mut invalid = false;
    let mut index = 0;
    while index < bytes.len() {
        let byte = bytes[index];
        if byte < 0x80 {
            out.push(byte);
            invalid = false;
            index += 1;
            continue;
        }
        let width = utf8_sequence_len(&bytes[index..]);
        if width > 1 {
            out.extend_from_slice(&bytes[index..index + width]);
            invalid = false;
            index += width;
        } else {
            if !invalid {
                out.extend_from_slice("\u{FFFD}".as_bytes());
                invalid = true;
            }
            index += 1;
        }
    }
    String::from_utf8(out).expect("replacement characters make the output valid")
}

/// `bytes.TrimSpace`, approximated with ASCII whitespace (git output only
/// needs it to drop the newlines `--name-status` emits).
fn trim_space(bytes: &[u8]) -> &[u8] {
    let mut start = 0;
    let mut end = bytes.len();
    while start < end && bytes[start].is_ascii_whitespace() {
        start += 1;
    }
    while end > start && bytes[end - 1].is_ascii_whitespace() {
        end -= 1;
    }
    &bytes[start..end]
}

// ---------------------------------------------------------------------------
// Commits (`search.RawCommit`, `search.LazyCommit`)
// ---------------------------------------------------------------------------

/// Baseline `search.RawCommit`: a shallow parse of one `git log` record.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct RawCommit {
    hash: Vec<u8>,
    ref_names: Vec<u8>,
    source_refs: Vec<u8>,
    author_name: Vec<u8>,
    author_email: Vec<u8>,
    author_date: Vec<u8>,
    committer_name: Vec<u8>,
    committer_email: Vec<u8>,
    committer_date: Vec<u8>,
    message: Vec<u8>,
    parent_hashes: Vec<u8>,
    modified_files: Vec<Vec<u8>>,
}

impl RawCommit {
    /// Baseline `CommitScanner.Scan`'s record parsing.
    fn parse(token: &[u8]) -> Result<Self, SearchError> {
        let fields = token.split(|&byte| byte == 0).collect::<Vec<_>>();
        if fields.len() < COMMIT_FIELDS {
            return Err(SearchError::Message(format!(
                "invalid commit log entry: {}",
                quote_fields(&fields)
            )));
        }
        // `--name-status` leaves empty parts behind; drop them and trim the
        // newlines around each entry.
        let mut modified_files = Vec::new();
        for field in &fields[COMMIT_FIELDS..] {
            if !field.is_empty() {
                modified_files.push(trim_space(field).to_vec());
            }
        }
        Ok(Self {
            hash: fields[0].to_vec(),
            ref_names: fields[1].to_vec(),
            source_refs: fields[2].to_vec(),
            author_name: fields[3].to_vec(),
            author_email: fields[4].to_vec(),
            author_date: fields[5].to_vec(),
            committer_name: fields[6].to_vec(),
            committer_email: fields[7].to_vec(),
            committer_date: fields[8].to_vec(),
            message: trim_space(fields[9]).to_vec(),
            parent_hashes: fields[10].to_vec(),
            modified_files,
        })
    }
}

/// A best-effort `%q` of `[][]byte` for the invalid-record error.
fn quote_fields(fields: &[&[u8]]) -> String {
    let mut out = String::from("[");
    for (index, field) in fields.iter().enumerate() {
        if index > 0 {
            out.push(' ');
        }
        out.push_str(&format!("{:?}", String::from_utf8_lossy(field)));
    }
    out.push(']');
    out
}

/// Baseline `search.LazyCommit`: the fields that need parsing are parsed on
/// demand and the parsed diff is cached after the first fetch.
struct LazyCommit<'a> {
    raw: RawCommit,
    diff: Option<Arc<Vec<FileDiff>>>,
    /// The persistent `git diff-tree` process, started on the first diff
    /// request and shared by every commit of this search.
    fetcher: &'a mut Option<DiffFetcher>,
    repo: &'a GitRepo,
}

impl<'a> LazyCommit<'a> {
    fn new(raw: RawCommit, fetcher: &'a mut Option<DiffFetcher>, repo: &'a GitRepo) -> Self {
        Self {
            raw,
            diff: None,
            fetcher,
            repo,
        }
    }

    /// Baseline `LazyCommit.Diff`: fetch the raw patch, then parse it with
    /// go-diff semantics (ported in [`crate::diff`]).
    async fn diff(&mut self) -> Result<Arc<Vec<FileDiff>>, SearchError> {
        if let Some(diff) = &self.diff {
            return Ok(Arc::clone(diff));
        }
        if self.fetcher.is_none() {
            *self.fetcher = Some(DiffFetcher::start(self.repo)?);
        }
        let fetcher = self
            .fetcher
            .as_mut()
            .expect("diff fetcher was just started");
        let raw = fetcher.fetch(&self.raw.hash).await?;
        let parsed = Arc::new(
            diff::parse_multi_file_diff(&raw)
                .map_err(|error| SearchError::Message(error.to_string()))?,
        );
        self.diff = Some(Arc::clone(&parsed));
        Ok(parsed)
    }

    fn author_date(&self) -> Result<i64, SearchError> {
        parse_unix_seconds(&self.raw.author_date)
    }

    fn committer_date(&self) -> Result<TimestampValue, SearchError> {
        Ok(TimestampValue {
            seconds: parse_unix_seconds(&self.raw.committer_date)?,
            nanos: 0,
        })
    }

    fn parent_ids(&self) -> Result<Vec<String>, SearchError> {
        if self.raw.parent_hashes.is_empty() {
            return Ok(Vec::new());
        }
        let mut parents = Vec::new();
        for parent in self.raw.parent_hashes.split(|&byte| byte == b' ') {
            parents.push(validated_commit_id(parent)?);
        }
        Ok(parents)
    }

    /// `strings.Split(utf8String(refNames), ", ")`; an empty field yields `[""]`.
    fn ref_names(&self) -> Vec<String> {
        split_refs(&self.raw.ref_names)
    }

    fn source_refs(&self) -> Vec<String> {
        split_refs(&self.raw.source_refs)
    }

    fn modified_files(&self) -> Vec<String> {
        let files = &self.raw.modified_files;
        let mut result = Vec::with_capacity(files.len() / 2);
        let mut index = 0;
        while index < files.len() {
            if files[index].is_empty() {
                return result;
            }
            match files[index][0] {
                b'R' => {
                    // A rename entry is followed by two file names.
                    if index + 2 >= files.len() {
                        return result;
                    }
                    result.push(to_valid_utf8(&files[index + 1]));
                    result.push(to_valid_utf8(&files[index + 2]));
                    index += 3;
                }
                _ => {
                    if index + 1 >= files.len() {
                        return result;
                    }
                    result.push(to_valid_utf8(&files[index + 1]));
                    index += 2;
                }
            }
        }
        result
    }
}

// ---------------------------------------------------------------------------
// Diff fetcher (`search.DiffFetcher`)
// ---------------------------------------------------------------------------

/// A handle to a persistent `git diff-tree --stdin` subprocess. Hashes are
/// written one at a time; the echoed `ENDOFPATCH` marker delimits each patch.
struct DiffFetcher {
    _child: Child,
    stdin: ChildStdin,
    stdout: BufReader<ChildStdout>,
    stderr: Option<tokio::process::ChildStderr>,
    buffer: Vec<u8>,
}

impl DiffFetcher {
    fn start(repo: &GitRepo) -> Result<Self, SearchError> {
        let mut command = tokio::process::Command::new("git");
        command
            .args([
                "diff-tree",
                "--stdin",
                "--no-prefix",
                "-p",
                "--format=format:",
                "--root",
            ])
            .current_dir(&repo.git_dir)
            .env("GIT_DIR", &repo.git_dir)
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .kill_on_drop(true);
        let mut child = command
            .spawn()
            .map_err(|err| SearchError::Message(format!("failed to run git diff-tree: {err}")))?;
        let stdin = child.stdin.take().expect("git diff-tree stdin is piped");
        let stdout = child.stdout.take().expect("git diff-tree stdout is piped");
        let stderr = child.stderr.take();
        Ok(Self {
            _child: child,
            stdin,
            stdout: BufReader::new(stdout),
            stderr,
            buffer: Vec::new(),
        })
    }

    /// Baseline `DiffFetcher.Fetch`: write the hash plus the marker revision,
    /// then read until the marker comes back.
    async fn fetch(&mut self, hash: &[u8]) -> Result<Vec<u8>, SearchError> {
        // The baseline ignores write errors; a dead subprocess surfaces when
        // the read below hits EOF instead.
        let _ = self.stdin.write_all(hash).await;
        let _ = self.stdin.write_all(b"\nENDOFPATCH\n").await;
        let _ = self.stdin.flush().await;

        loop {
            if let Some(patch) = take_patch(&mut self.buffer) {
                return Ok(patch);
            }
            let mut chunk = [0u8; 32 * 1024];
            let read = self
                .stdout
                .read(&mut chunk)
                .await
                .map_err(|err| SearchError::Message(err.to_string()))?;
            if read == 0 {
                break;
            }
            self.buffer.extend_from_slice(&chunk[..read]);
        }

        // The scanner stopped without a token; report the subprocess stderr
        // exactly like the baseline (`scanner.Err()` is always nil here).
        let mut stderr = Vec::new();
        if let Some(mut handle) = self.stderr.take() {
            let _ = handle.read_to_end(&mut stderr).await;
        }
        if !stderr.is_empty() {
            return Err(SearchError::Message(format!(
                "git subprocess stderr: {}",
                String::from_utf8_lossy(&stderr)
            )));
        }
        Err(SearchError::Message("expected scan to succeed".to_owned()))
    }
}

/// If the buffered output ends with the marker, return the patch before it.
fn take_patch(buffer: &mut Vec<u8>) -> Option<Vec<u8>> {
    if !buffer.ends_with(END_OF_PATCH) {
        return None;
    }
    let patch_length = buffer.len() - END_OF_PATCH.len();
    let patch = buffer[..patch_length].to_vec();
    buffer.clear();
    Some(patch)
}

fn split_refs(bytes: &[u8]) -> Vec<String> {
    to_valid_utf8(bytes)
        .split(", ")
        .map(str::to_owned)
        .collect()
}

fn parse_unix_seconds(bytes: &[u8]) -> Result<i64, SearchError> {
    let text = String::from_utf8_lossy(bytes);
    text.parse::<i64>().map_err(|_| {
        SearchError::Message(format!(
            "strconv.Atoi: parsing {:?}: invalid syntax",
            text.as_ref()
        ))
    })
}

fn validated_commit_id(bytes: &[u8]) -> Result<String, SearchError> {
    let valid = bytes.len() == 40 && bytes.iter().all(u8::is_ascii_hexdigit);
    if !valid {
        return Err(SearchError::Message(format!(
            "invalid CommitID {:?}",
            String::from_utf8_lossy(bytes)
        )));
    }
    Ok(String::from_utf8(bytes.to_vec()).expect("hex digits are ASCII"))
}

// ---------------------------------------------------------------------------
// Commit match (`search.CreateCommitMatch`)
// ---------------------------------------------------------------------------

fn signature(name: &[u8], email: &[u8], date_seconds: i64) -> gpb::commit_match::Signature {
    gpb::commit_match::Signature {
        name: to_valid_utf8(name),
        email: to_valid_utf8(email),
        date: Some(prost_types::Timestamp {
            seconds: date_seconds,
            nanos: 0,
        }),
    }
}

fn location_proto(location: Location) -> gpb::commit_match::Location {
    gpb::commit_match::Location {
        offset: location.offset,
        line: location.line,
        column: location.column,
    }
}

fn matched_string_proto(
    content: String,
    ranges: &[MatchRange],
) -> gpb::commit_match::MatchedString {
    gpb::commit_match::MatchedString {
        content,
        ranges: ranges
            .iter()
            .map(|range| gpb::commit_match::Range {
                start: Some(location_proto(range.start)),
                end: Some(location_proto(range.end)),
            })
            .collect(),
    }
}

async fn create_commit_match(
    commit: &mut LazyCommit<'_>,
    highlights: &MatchedCommit,
    include_diff: bool,
) -> Result<gpb::CommitMatch, SearchError> {
    let author_date = commit.author_date()?;
    let committer_date = commit.committer_date()?;

    // Baseline order: dates, then the diff (which may fetch), then the commit
    // ID and parents.
    let diff = if include_diff {
        let raw_diff = commit.diff().await?;
        // `filterRawDiff` only applies with sub-repo permissions, which the
        // Rust gitserver does not implement.
        let (content, ranges) = format_diff(&raw_diff, &highlights.diff);
        matched_string_proto(content, &ranges)
    } else {
        gpb::commit_match::MatchedString {
            content: String::new(),
            ranges: Vec::new(),
        }
    };

    let oid = validated_commit_id(&commit.raw.hash)?;
    let parents = commit.parent_ids()?;
    Ok(gpb::CommitMatch {
        oid,
        author: Some(signature(
            &commit.raw.author_name,
            &commit.raw.author_email,
            author_date,
        )),
        committer: Some(signature(
            &commit.raw.committer_name,
            &commit.raw.committer_email,
            committer_date.seconds,
        )),
        parents,
        refs: commit.ref_names(),
        source_refs: commit.source_refs(),
        message: Some(matched_string_proto(
            to_valid_utf8(&commit.raw.message),
            &highlights.message,
        )),
        diff: Some(diff),
        modified_files: commit.modified_files(),
    })
}

// ---------------------------------------------------------------------------
// Diff formatting (`search.FormatDiff`)
// ---------------------------------------------------------------------------

/// Baseline `FormatDiff`: render up to [`MAX_FILES`] matching files, up to
/// [`MAX_HUNKS_PER_FILE`] hunks per file, filtered by
/// [`split_hunk_matches`], and translate the match ranges into the formatted
/// text's coordinates.
fn format_diff(
    raw_diff: &[FileDiff],
    highlights: &BTreeMap<usize, MatchedFileDiff>,
) -> (String, Vec<MatchRange>) {
    let mut buffer = String::new();
    let mut location = Location::default();
    let mut ranges = Vec::new();

    let no_highlights = MatchedFileDiff::default();
    let mut file_count = 0;
    for (file_index, file_diff) in raw_diff.iter().enumerate() {
        let file_highlight = match highlights.get(&file_index) {
            Some(highlight) => highlight,
            None if highlights.is_empty() => &no_highlights,
            None => continue,
        };
        if file_count >= MAX_FILES {
            break;
        }
        file_count += 1;

        ranges.extend(add_ranges(&file_highlight.old_file, location));
        let escaped = escape_utf8_and_spaces(&file_diff.orig_name);
        buffer.push_str(&escaped);
        buffer.push(' ');
        location.offset = buffer.len() as u32;
        location.column = escaped.len() as u32 + 1;

        ranges.extend(add_ranges(&file_highlight.new_file, location));
        buffer.push_str(&escape_utf8_and_spaces(&file_diff.new_name));
        buffer.push('\n');
        location.offset = buffer.len() as u32;
        location.line += 1;
        location.column = 0;

        let (filtered_hunks, filtered_highlights) = split_hunk_matches(
            &file_diff.hunks,
            &file_highlight.matched_hunks,
            MATCH_CONTEXT_LINES,
            MAX_LINES_PER_HUNK,
        );

        let mut hunk_count = 0;
        let no_hunk = MatchedHunk::default();
        for (hunk_index, hunk) in filtered_hunks.iter().enumerate() {
            let hunk_highlight = match filtered_highlights.get(&hunk_index) {
                Some(highlight) => highlight,
                None if filtered_highlights.is_empty() => &no_hunk,
                None => continue,
            };
            if hunk_count >= MAX_HUNKS_PER_FILE {
                break;
            }
            hunk_count += 1;

            buffer.push_str(&format!(
                "@@ -{},{} +{},{} @@ {}\n",
                hunk.orig_start_line,
                hunk.orig_lines,
                hunk.new_start_line,
                hunk.new_lines,
                to_valid_utf8(&hunk.section),
            ));
            location.offset = buffer.len() as u32;
            location.line += 1;
            location.column = 0;

            for (line_index, line) in ByteLines::new(&hunk.body).enumerate() {
                if line.is_empty() {
                    continue;
                }
                let prefix = line[0];
                let without_prefix = &line[1..];
                buffer.push(prefix as char);
                location.offset = buffer.len() as u32;
                location.column = 1;

                if let Some(line_highlights) = hunk_highlight.matched_lines.get(&line_index) {
                    ranges.extend(add_ranges(line_highlights, location));
                }

                buffer.push_str(&to_valid_utf8(without_prefix));
                buffer.push('\n');
                location.offset = buffer.len() as u32;
                location.line += 1;
                location.column = 0;
            }
        }
    }

    (buffer, ranges)
}

/// Baseline `escapeUTF8AndSpaces`: make the name valid UTF-8, then escape
/// every space as `\ `.
fn escape_utf8_and_spaces(name: &[u8]) -> String {
    to_valid_utf8(name).replace(' ', "\\ ")
}

/// `Ranges.Add`: shift every range by `location`.
fn add_ranges(ranges: &[MatchRange], location: Location) -> Vec<MatchRange> {
    ranges
        .iter()
        .map(|range| MatchRange {
            start: add_location(range.start, location),
            end: add_location(range.end, location),
        })
        .collect()
}

fn add_location(left: Location, right: Location) -> Location {
    Location {
        offset: left.offset + right.offset,
        line: left.line + right.line,
        column: left.column + right.column,
    }
}

/// Baseline `splitHunkMatches`: keep only matching changed lines plus
/// `match_context_lines` of context, cap the hunk at
/// `2 * context + max_lines_per_hunk` lines, split where gaps remain, and
/// renumber the hunk headers. Highlights are re-keyed to the new hunk/line
/// indices.
fn split_hunk_matches(
    hunks: &[Hunk],
    hunk_highlights: &BTreeMap<usize, MatchedHunk>,
    match_context_lines: usize,
    max_lines_per_hunk: usize,
) -> (Vec<Hunk>, BTreeMap<usize, MatchedHunk>) {
    let mut results: Vec<Hunk> = Vec::new();
    let mut new_highlights: BTreeMap<usize, MatchedHunk> = BTreeMap::new();

    for (hunk_index, hunk) in hunks.iter().enumerate() {
        let orig_highlights = hunk_highlights
            .get(&hunk_index)
            .map(|highlight| &highlight.matched_lines);
        let lines: Vec<&[u8]> = split_after(&hunk.body, b'\n');
        let line_info = compute_diff_hunk_info(&lines, orig_highlights, match_context_lines);

        let mut current: Option<Hunk> = None;
        let mut current_lines: Vec<u8> = Vec::new();
        // `curLines` is a slice of lines in the baseline, so the cap below and
        // the highlight keys count lines, not bytes.
        let mut current_line_count: usize = 0;
        let mut current_highlights: BTreeMap<usize, Vec<MatchRange>> = BTreeMap::new();
        let mut extra_hunk_matches = 0usize;
        let mut orig_line_offset: i32 = 0;
        let mut new_line_offset: i32 = 0;

        for (line_index, line) in lines.iter().enumerate() {
            if line.is_empty() {
                break;
            }
            let info = &line_info[line_index];
            if info.matching || info.context {
                if max_lines_per_hunk > 0
                    && current_line_count == match_context_lines * 2 + max_lines_per_hunk
                {
                    if info.matching {
                        extra_hunk_matches += 1;
                    }
                    // The baseline `continue`s here, skipping the offset
                    // updates below as well.
                    continue;
                }

                let current_hunk = current.get_or_insert_with(|| Hunk {
                    orig_start_line: hunk.orig_start_line + orig_line_offset,
                    new_start_line: hunk.new_start_line + new_line_offset,
                    section: hunk.section.clone(),
                    ..Hunk::default()
                });
                if !info.added {
                    current_hunk.orig_lines += 1;
                }
                if !info.removed {
                    current_hunk.new_lines += 1;
                }
                if let Some(highlights) = orig_highlights.and_then(|map| map.get(&line_index)) {
                    if !highlights.is_empty() {
                        current_highlights.insert(current_line_count, highlights.clone());
                    }
                }
                current_lines.extend_from_slice(line);
                current_line_count += 1;
            } else if current.is_some() {
                let mut done = current.take().expect("checked above");
                add_extra_hunk_matches_section(&mut done, extra_hunk_matches);
                done.body = std::mem::take(&mut current_lines);
                current_line_count = 0;
                if !current_highlights.is_empty() {
                    new_highlights.insert(
                        results.len(),
                        MatchedHunk {
                            matched_lines: std::mem::take(&mut current_highlights),
                        },
                    );
                }
                results.push(done);
                extra_hunk_matches = 0;
            }

            if !info.added {
                orig_line_offset += 1;
            }
            if !info.removed {
                new_line_offset += 1;
            }
        }

        if let Some(mut done) = current {
            add_extra_hunk_matches_section(&mut done, extra_hunk_matches);
            done.body = current_lines;
            if !current_highlights.is_empty() {
                new_highlights.insert(
                    results.len(),
                    MatchedHunk {
                        matched_lines: current_highlights,
                    },
                );
            }
            results.push(done);
        }
    }

    (results, new_highlights)
}

/// Baseline `addExtraHunkMatchesSection`.
fn add_extra_hunk_matches_section(hunk: &mut Hunk, extra_hunk_matches: usize) {
    if extra_hunk_matches > 0 {
        if !hunk.section.is_empty() {
            hunk.section.push(b' ');
        }
        hunk.section
            .extend_from_slice(format!("... +{extra_hunk_matches}").as_bytes());
    }
}

#[derive(Clone, Copy, Debug, Default)]
struct DiffHunkLineInfo {
    added: bool,
    removed: bool,
    matching: bool,
    context: bool,
}

impl DiffHunkLineInfo {
    fn changed(&self) -> bool {
        self.added || self.removed
    }
}

/// Baseline `computeDiffHunkInfo`; `None` means the hunk has no highlights
/// (every changed line matches).
fn compute_diff_hunk_info(
    lines: &[&[u8]],
    line_highlights: Option<&BTreeMap<usize, Vec<MatchRange>>>,
    match_context_lines: usize,
) -> Vec<DiffHunkLineInfo> {
    let mut info = vec![DiffHunkLineInfo::default(); lines.len()];
    let context_lines = |line: usize| {
        let start = line.saturating_sub(match_context_lines);
        let end = (line + match_context_lines).min(lines.len().saturating_sub(1));
        (start, end)
    };

    for (index, line) in lines.iter().enumerate() {
        let (added, removed) = diff_hunk_line_status(line);
        info[index].added = added;
        info[index].removed = removed;
        if info[index].changed() {
            let matching = match line_highlights {
                None => true,
                Some(map) => map.contains_key(&index) || map.is_empty(),
            };
            info[index].matching = matching;
            if matching {
                let (start, end) = context_lines(index);
                for (position, slot) in info.iter_mut().enumerate().take(end + 1).skip(start) {
                    if position != index {
                        slot.context = true;
                    }
                }
            }
        }
    }
    info
}

/// Baseline `diffHunkLineStatus`.
fn diff_hunk_line_status(line: &[u8]) -> (bool, bool) {
    match line.first() {
        Some(b'+') => (true, false),
        Some(b'-') => (false, true),
        _ => (false, false),
    }
}

/// `bytes.SplitAfter(data, sep)`: every line keeps its trailing separator,
/// and a trailing separator yields a final empty element.
fn split_after(data: &[u8], separator: u8) -> Vec<&[u8]> {
    let mut parts = Vec::new();
    let mut start = 0;
    let mut index = 0;
    while index < data.len() {
        if data[index] == separator {
            parts.push(&data[start..=index]);
            start = index + 1;
        }
        index += 1;
    }
    parts.push(&data[start..]);
    parts
}

/// `byteutils.NewLineReader`: iterate over `\n`-terminated lines without a
/// final empty line (and no line at all for empty input).
struct ByteLines<'a> {
    data: &'a [u8],
    index: usize,
}

impl<'a> ByteLines<'a> {
    fn new(data: &'a [u8]) -> Self {
        Self { data, index: 0 }
    }
}

impl<'a> Iterator for ByteLines<'a> {
    type Item = &'a [u8];

    fn next(&mut self) -> Option<Self::Item> {
        if self.index >= self.data.len() {
            return None;
        }
        let start = self.index;
        match self.data[start..].iter().position(|&byte| byte == b'\n') {
            Some(index) => {
                self.index = start + index + 1;
                Some(&self.data[start..start + index])
            }
            None => {
                self.index = self.data.len();
                Some(&self.data[start..])
            }
        }
    }
}

/// Baseline `matchCount`: diff ranges, then message ranges, then one.
fn match_count(commit_match: &gpb::CommitMatch) -> i64 {
    if let Some(diff) = &commit_match.diff {
        if !diff.ranges.is_empty() {
            return diff.ranges.len() as i64;
        }
    }
    if let Some(message) = &commit_match.message {
        if !message.ranges.is_empty() {
            return message.ranges.len() as i64;
        }
    }
    1
}

// ---------------------------------------------------------------------------
// Search driver (`search.CommitSearcher`)
// ---------------------------------------------------------------------------

/// Baseline `revsToGitArgs`: an empty revision means `HEAD`; an empty list
/// lets git default to `HEAD`.
fn revision_args(revisions: &[String]) -> Vec<OsString> {
    revisions
        .iter()
        .map(|revision| {
            if revision.is_empty() {
                OsString::from("HEAD")
            } else {
                OsString::from(revision)
            }
        })
        .collect()
}

/// Run a commit search, calling `on_match` for every result in git's output
/// order. The caller sends the terminating `limit_hit` message after this
/// returns; see the note at the end about why its value is always `false`.
/// `on_match` returns `false` when the stream's receiver is gone, which stops
/// the search.
pub async fn search<F, Fut>(
    repo: &GitRepo,
    request: &SearchRequest,
    tasks: &TaskTracker,
    mut on_match: F,
) -> Result<bool, SearchError>
where
    F: FnMut(gpb::CommitMatch) -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    // Baseline `doSearch`: `Limit == 0` means no limit.
    let limit = if request.limit == 0 {
        i64::from(i32::MAX)
    } else {
        request.limit
    };
    let match_tree = MatchTree::compile(&request.query)?;
    // `doSearch`: a DiffModifiesFile node needs `--name-status` for its
    // pre-filter, so it forces the field on even when the request did not ask.
    let include_modified_files =
        request.include_modified_files || match_tree.has_diff_modifies_file();

    let mut child = repo
        .command(&search_log_args(&request.revisions, include_modified_files))
        .spawn()
        .map_err(|err| SearchError::Message(format!("failed to run git log: {err}")))?;

    // Drain stderr concurrently so a chatty `git log` cannot fill the pipe.
    let mut stderr = child
        .stderr
        .take()
        .expect("git log stderr is piped by GitRepo::command");
    let stderr_task = tasks.spawn(async move {
        let mut buffer = Vec::new();
        stderr.read_to_end(&mut buffer).await.map(|_| buffer)
    });

    let stdout = child
        .stdout
        .take()
        .expect("git log stdout is piped by GitRepo::command");
    let mut scanner = CommitScanner::new(stdout);

    let mut sent = 0i64;
    let mut keep_going = true;
    // Started lazily on the first diff request, like each baseline worker's
    // `NewDiffFetcher`.
    let mut diff_fetcher: Option<DiffFetcher> = None;

    // The baseline scanner silently stops when the stream does not start with
    // the record separator, so an empty or malformed output yields no matches.
    match scanner.next_token().await? {
        None => {}
        Some(prefix) if prefix.is_empty_or_separator() => {
            while keep_going {
                let Some(segment) = scanner.next_token().await? else {
                    break;
                };
                let at_eof = !segment.terminated;
                let token = segment.token().to_vec();
                if token.len() > MAX_TOKEN_SIZE {
                    break;
                }
                let raw = RawCommit::parse(&token)?;
                let mut commit = LazyCommit::new(raw, &mut diff_fetcher, repo);
                let (filter, highlights) = match_tree.matches(&mut commit).await?;
                if filter.satisfies() {
                    let commit_match =
                        create_commit_match(&mut commit, &highlights, request.include_diff).await?;
                    // Once the limit is reached the baseline still evaluates
                    // every remaining commit, but drops the matches.
                    if sent < limit {
                        sent += match_count(&commit_match);
                        keep_going = on_match(commit_match).await;
                    }
                }
                if at_eof {
                    break;
                }
            }
        }
        Some(_) => {
            // `expected commit separator`: swallowed by `CommitScanner.Err`.
        }
    }

    child
        .wait()
        .await
        .map_err(|err| SearchError::Message(format!("failed to wait for git log: {err}")))?;
    // Baseline quirk: the deferred `cmd.Wait` check passes its *feed* error
    // (still nil at that point) instead of the wait error to
    // `tryInterpretErrorWithStderr`, so `errors.Append(nil, nil)` discards it.
    // A failing `git log` — bad revision, broken repository, empty repository —
    // therefore ends as a successful, empty search. Keep the stderr reader
    // joined so the pipe is drained, but its contents cannot change the result.
    let _ = stderr_task.await;

    // Baseline quirk: `doSearch` is
    // `return hitLimit.Load(), searcher.Search(ctx, limitedOnMatch)`.
    // Go evaluates return operands left to right, so the recorded value is
    // read *before* the search runs and the RPC always terminates with
    // `limit_hit=false` — even when the limit was reached and matches were
    // dropped. The fixtures freeze that behavior.
    Ok(false)
}

fn search_log_args(revisions: &[String], include_modified_files: bool) -> Vec<OsString> {
    let mut args: Vec<OsString> = vec!["log".into(), "--decorate=full".into(), "-z".into()];
    args.push(LOG_FORMAT.into());
    args.extend(revision_args(revisions));
    if include_modified_files {
        args.push("--name-status".into());
    }
    args
}

/// A record returned by [`CommitScanner`]: the bytes after one separator.
struct Segment {
    buffer: Vec<u8>,
    terminated: bool,
}

impl Segment {
    /// `true` for the empty prefix before the first commit's separator.
    fn is_empty_or_separator(&self) -> bool {
        self.buffer == [COMMIT_SEPARATOR]
    }

    /// The token between separators (the trailing separator is not part of it).
    fn token(&self) -> &[u8] {
        if self.terminated {
            &self.buffer[..self.buffer.len() - 1]
        } else {
            &self.buffer
        }
    }
}

/// The baseline `CommitScanner`: split `git log` on the record separator.
struct CommitScanner {
    reader: BufReader<tokio::process::ChildStdout>,
    buffer: Vec<u8>,
}

impl CommitScanner {
    fn new(stdout: tokio::process::ChildStdout) -> Self {
        Self {
            reader: BufReader::new(stdout),
            buffer: Vec::new(),
        }
    }

    /// Read up to (and including) the next separator; `None` at EOF.
    async fn next_token(&mut self) -> Result<Option<Segment>, SearchError> {
        self.buffer.clear();
        let read = self
            .reader
            .read_until(COMMIT_SEPARATOR, &mut self.buffer)
            .await
            .map_err(|err| SearchError::Message(format!("read git log: {err}")))?;
        if read == 0 {
            return Ok(None);
        }
        let terminated = self.buffer.last() == Some(&COMMIT_SEPARATOR);
        Ok(Some(Segment {
            buffer: std::mem::take(&mut self.buffer),
            terminated,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counts_runes_like_go() {
        assert_eq!(go_rune_count(b"abc"), 3);
        assert_eq!(go_rune_count("héllo".as_bytes()), 5);
        // Each invalid byte is one rune (RuneError).
        assert_eq!(go_rune_count(&[0xff, 0xff]), 2);
        assert_eq!(go_rune_count(&[0xe2, 0x82]), 2);
    }

    #[test]
    fn replaces_invalid_utf8_like_go() {
        assert_eq!(to_valid_utf8(b"ok"), "ok");
        assert_eq!(to_valid_utf8(b"a\xff\xffb"), "a\u{FFFD}b");
        assert_eq!(to_valid_utf8("é".as_bytes()), "é");
    }

    #[test]
    fn builds_ranges_with_lines_and_columns() {
        let content = b"first line\nsecond line\nthird";
        let matches = [(0usize, 5usize), (11, 17), (23, 28)];
        let ranges = matches_to_ranges(content, &matches);
        assert_eq!(ranges.len(), 3);
        assert_eq!(
            ranges[0].start,
            Location {
                offset: 0,
                line: 0,
                column: 0
            }
        );
        assert_eq!(
            ranges[0].end,
            Location {
                offset: 5,
                line: 0,
                column: 5
            }
        );
        assert_eq!(
            ranges[1].start,
            Location {
                offset: 11,
                line: 1,
                column: 0
            }
        );
        assert_eq!(
            ranges[1].end,
            Location {
                offset: 17,
                line: 1,
                column: 6
            }
        );
        assert_eq!(
            ranges[2].start,
            Location {
                offset: 23,
                line: 2,
                column: 0
            }
        );
        assert_eq!(ranges[2].end.column, 5);
    }

    #[test]
    fn parses_a_commit_record() {
        let token = b"abc\x00HEAD -> refs/heads/main\x00HEAD\x00A\x00a@example.com\x00113\
            \x00B\x00b@example.com\x00114\x00subject\n\nbody\n\x00parent1 parent2\x00\x00M\x00file.go\x00";
        let commit = RawCommit::parse(token).expect("record");
        assert_eq!(commit.hash, b"abc");
        assert_eq!(commit.ref_names, b"HEAD -> refs/heads/main");
        assert_eq!(commit.message, b"subject\n\nbody");
        assert_eq!(
            commit.modified_files,
            vec![b"M".to_vec(), b"file.go".to_vec()]
        );
    }

    /// With no diff highlights every changed line matches, so the whole hunk
    /// (plus one line of context) is kept and renumbered.
    #[test]
    fn formats_diff_without_highlights() {
        let raw = include_bytes!("../testdata/head.diff");
        let files = crate::diff::parse_multi_file_diff(raw).expect("parses");
        let (content, ranges) = format_diff(&files, &BTreeMap::new());
        assert_eq!(
            content,
            "file.txt file.txt\n@@ -1,3 +1,3 @@ \n ALPHA\n-beta\n+ beta\n gamma\n"
        );
        assert!(ranges.is_empty());
    }

    #[test]
    fn expands_rename_entries_in_modified_files() {
        let raw = RawCommit {
            modified_files: vec![
                b"R100".to_vec(),
                b"old.go".to_vec(),
                b"new.go".to_vec(),
                b"A".to_vec(),
                b"added.go".to_vec(),
            ],
            ..RawCommit::default()
        };
        let mut fetcher = None;
        let repo = GitRepo {
            git_dir: std::path::PathBuf::new(),
        };
        let commit = LazyCommit::new(raw, &mut fetcher, &repo);
        assert_eq!(
            commit.modified_files(),
            vec![
                "old.go".to_owned(),
                "new.go".to_owned(),
                "added.go".to_owned()
            ]
        );
    }
}
