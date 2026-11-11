//! `internal/search/job/jobutil/{filter_file_contains.go,filter_file_contributor.go,select.go}`.
//!
//! Three post-filter jobs over the already-ported result model:
//!
//! * [`FilterFileContainsJob`] (`file:contains.content()`): keeps only the
//!   chunk ranges that do not match an include pattern (unless they also
//!   match an original pattern), and, for diff commit matches, runs one
//!   injected searcher per include pattern at the result's commit and drops
//!   file diffs that did not match every include pattern.
//! * [`FilterFileHasContributorsJob`] (`file:has.contributor()`): keeps file
//!   matches whose commit contributors satisfy all injected include regexps
//!   and no exclude regexp.
//! * [`SelectJob`]: transforms matches via a `select:` path and deduplicates
//!   them with the baseline `result.Key` rules.
//!
//! Dependencies that need real gRPC/DB in the baseline are injected:
//! [`CommitContentSearcher`] for the contains diff search and
//! [`ContributorFetcher`] for `gitserver.ContributorCount`.
//!
//! `convert::CommitMatch` does not model `result.CommitMatch.Diff`. The
//! baseline populates that field with `ParseDiffString(DiffPreview.Content)`
//! (`internal/search/commit/commit.go:327`), so this module re-derives it from
//! [`CommitMatch::diff_preview`] with the ported [`parse_diff_string`]. This
//! keeps `search_job.rs` and `search_stream::convert` untouched.
//!
//! See `docs/overviews/m9_8hi_jobutil_filters_overview.md`.

use std::collections::{HashMap, HashSet};
use std::time::Instant;

use regex::Regex;

use crate::search_job::{JobContext, JobError, RuntimeClients, SearchJob, Verbosity};
use crate::search_jobutil_combinators as comb;
use crate::search_stream::convert::{
    quote_meta, select_kind, ChunkMatch, CommitMatch, FileMatch, Match, MatchedString, RepoMatch,
};
use crate::search_stream::events::{Location, Range};
use crate::search_stream::progress::SearchEvent;

// ---------------------------------------------------------------------------
// `file:contains.content()`
// ---------------------------------------------------------------------------

/// `query.Node` subset consumed by `patternsInTree`
/// (`filter_file_contains.go:326-341`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PatternNode {
    /// `query.Pattern`.
    Pattern(String),
    /// `query.Operator`.
    Operator(Vec<PatternNode>),
}

/// `patternsInTree`: flatten an operator tree into its pattern values.
pub fn patterns_in_tree(node: &PatternNode) -> Vec<String> {
    match node {
        PatternNode::Pattern(value) => vec![value.clone()],
        PatternNode::Operator(operands) => operands.iter().flat_map(patterns_in_tree).collect(),
    }
}

/// Which constructor regex list failed to compile.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ContainsPatternStage {
    Include,
    Original,
}

/// `NewFileContainsFilterJob` regexp compilation error. The baseline returns a
/// wrapped error; this keeps the failing stage and pattern for differential
/// normalization.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContainsPatternError {
    pub stage: ContainsPatternStage,
    pub pattern: String,
    pub message: String,
}

impl std::fmt::Display for ContainsPatternError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let list = match self.stage {
            ContainsPatternStage::Include => "include",
            ContainsPatternStage::Original => "original",
        };
        write!(
            f,
            "failed to regexp.Compile({:?}) for file:contains.content() {list} patterns: {}",
            self.pattern, self.message
        )
    }
}

impl std::error::Error for ContainsPatternError {}

/// `search.TextPatternInfo` fields the ported search needs
/// (`filter_file_contains.go:182-192`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IncludeSearchRequest {
    pub repo_name: String,
    pub repo_id: i32,
    pub commit_id: String,
    /// The raw `file:contains.content()` pattern (not `(?i:)`-wrapped).
    pub pattern: String,
    pub is_case_sensitive: bool,
    /// `query.UnionRegExps(quote_meta(Diff[].NewName))`.
    pub include_paths: String,
    /// `FileMatchLimit`.
    pub file_match_limit: i64,
}

/// `searcher.Search` dependency used by `filterCommitMatch`. Returning `Err`
/// is the baseline's "ignore any files where the search errors" path.
pub trait CommitContentSearcher {
    fn search_include(&self, request: &IncludeSearchRequest) -> Result<Vec<String>, JobError>;
}

/// One parsed diff file (`result.DiffFile`).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DiffFile {
    pub orig_name: String,
    pub new_name: String,
    pub hunks: Vec<Hunk>,
}

/// One diff hunk (`result.Hunk`).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Hunk {
    pub old_start: i64,
    pub old_count: i64,
    pub new_start: i64,
    pub new_count: i64,
    pub header: String,
    pub lines: Vec<String>,
}

/// `filter_file_contains.go:39-97`.
pub struct FilterFileContainsJob {
    case_sensitive: bool,
    include_patterns: Vec<String>,
    include_matchers: Vec<Regex>,
    original_pattern_matchers: Vec<Regex>,
    searcher: Option<Box<dyn CommitContentSearcher>>,
    child: Box<dyn SearchJob>,
}

impl FilterFileContainsJob {
    /// `NewFileContainsFilterJob`. Compilation order is include patterns first,
    /// then original patterns, matching the baseline.
    pub fn new(
        include_patterns: Vec<String>,
        original_pattern: Option<&PatternNode>,
        case_sensitive: bool,
        child: Box<dyn SearchJob>,
    ) -> Result<Self, ContainsPatternError> {
        let include_matchers = compile_patterns(
            &include_patterns,
            case_sensitive,
            ContainsPatternStage::Include,
        )?;
        let original_patterns = original_pattern.map(patterns_in_tree).unwrap_or_default();
        let original_pattern_matchers = compile_patterns(
            &original_patterns,
            case_sensitive,
            ContainsPatternStage::Original,
        )?;
        Ok(Self {
            case_sensitive,
            include_patterns,
            include_matchers,
            original_pattern_matchers,
            searcher: None,
            child,
        })
    }

    /// Inject the diff searcher (the baseline reads it from `job.RuntimeClients`).
    pub fn with_searcher(mut self, searcher: Box<dyn CommitContentSearcher>) -> Self {
        self.searcher = Some(searcher);
        self
    }

    /// `removeUnmatchedFileDiffs` (`filter_file_contains.go:222-290`), reduced to
    /// the mutation of `DiffPreview`; returns the kept diffs (the baseline also
    /// stores them in `CommitMatch.Diff`).
    ///
    /// The caller is responsible for `include_pattern_count` matching the
    /// number of compiled include matchers.
    pub fn filter_unmatched_file_diffs(
        &self,
        cm: &mut CommitMatch,
        matched_file_counts: &HashMap<String, usize>,
    ) -> Option<Vec<DiffFile>> {
        filter_unmatched_file_diffs(cm, matched_file_counts, self.include_matchers.len())
    }
}

impl SearchJob for FilterFileContainsJob {
    fn run(
        &mut self,
        ctx: &JobContext,
        clients: &RuntimeClients<'_>,
        stream: &mut dyn FnMut(SearchEvent),
    ) -> Result<(), JobError> {
        let Self {
            include_matchers,
            original_pattern_matchers,
            include_patterns,
            case_sensitive,
            searcher,
            child,
        } = self;
        let mut filtered = |event: SearchEvent| {
            let event = filter_event(
                event,
                include_matchers,
                original_pattern_matchers,
                include_patterns,
                *case_sensitive,
                searcher.as_deref(),
            );
            stream(event);
        };
        child.run(ctx, clients, &mut filtered)
    }

    fn name(&self) -> &'static str {
        "FileContainsFilterJob"
    }

    fn children(&self) -> Vec<&dyn SearchJob> {
        vec![self.child.as_ref()]
    }

    fn attributes(&self, verbosity: Verbosity) -> Vec<(String, String)> {
        let mut res = Vec::new();
        if matches!(verbosity, Verbosity::Basic | Verbosity::Max) {
            res.push((
                "originalPatterns".to_owned(),
                string_slice(self.original_pattern_matchers.iter().map(Regex::as_str)),
            ));
            res.push((
                "filterPatterns".to_owned(),
                string_slice(self.include_matchers.iter().map(Regex::as_str)),
            ));
        }
        res
    }
}

fn compile_patterns(
    patterns: &[String],
    case_sensitive: bool,
    stage: ContainsPatternStage,
) -> Result<Vec<Regex>, ContainsPatternError> {
    let mut matchers = Vec::with_capacity(patterns.len());
    for pattern in patterns {
        let source = if case_sensitive {
            pattern.clone()
        } else {
            format!("(?i:{pattern})")
        };
        match Regex::new(&source) {
            Ok(re) => matchers.push(re),
            Err(err) => {
                return Err(ContainsPatternError {
                    stage,
                    pattern: pattern.clone(),
                    message: err.to_string(),
                })
            }
        }
    }
    Ok(matchers)
}

/// `filterEvent` (`filter_file_contains.go:104-124`): keep only file and
/// commit matches, and route each through the matching filter.
fn filter_event(
    event: SearchEvent,
    include_matchers: &[Regex],
    original_pattern_matchers: &[Regex],
    include_patterns: &[String],
    case_sensitive: bool,
    searcher: Option<&dyn CommitContentSearcher>,
) -> SearchEvent {
    let mut event = event;
    let results = std::mem::take(&mut event.results);
    let mut filtered = Vec::with_capacity(results.len());
    for result in results {
        match result {
            Match::File(fm) => {
                filtered.push(Match::File(filter_file_match(
                    fm,
                    include_matchers,
                    original_pattern_matchers,
                )));
            }
            Match::Commit(cm) => {
                let filtered_cm = filter_commit_match_with(
                    cm,
                    include_matchers,
                    include_patterns,
                    case_sensitive,
                    searcher,
                );
                if let Some(cm) = filtered_cm {
                    filtered.push(Match::Commit(cm));
                }
            }
            Match::Repo(_) | Match::Owner(_) => {}
        }
    }
    event.results = filtered;
    event
}

/// Free-function form of `filterCommitMatch` used by [`filter_event`].
fn filter_commit_match_with(
    mut cm: CommitMatch,
    include_matchers: &[Regex],
    include_patterns: &[String],
    case_sensitive: bool,
    searcher: Option<&dyn CommitContentSearcher>,
) -> Option<CommitMatch> {
    let content = cm.diff_preview.as_ref()?.content.clone();
    let diffs = parse_diff_string(&content).unwrap_or_default();
    let file_names: Vec<String> = diffs
        .iter()
        .map(|diff| quote_meta(&diff.new_name))
        .collect();
    let include_paths = union_reg_exps(&file_names);

    let mut matched_file_counts: HashMap<String, usize> = HashMap::new();
    if !include_patterns.is_empty() {
        let searcher = searcher?;
        for include_pattern in include_patterns {
            let request = IncludeSearchRequest {
                repo_name: cm.repo.name.clone(),
                repo_id: cm.repo.id,
                commit_id: cm.commit.id.clone(),
                pattern: include_pattern.clone(),
                is_case_sensitive: case_sensitive,
                include_paths: include_paths.clone(),
                file_match_limit: 99_999_999,
            };
            let paths = searcher.search_include(&request).ok()?;
            for path in paths {
                *matched_file_counts.entry(path).or_insert(0) += 1;
            }
        }
    }

    filter_unmatched_file_diffs(&mut cm, &matched_file_counts, include_matchers.len())?;
    Some(cm)
}

/// `filterFileMatch` (`filter_file_contains.go:126-140`): a file match whose
/// chunks are all filtered away is still valid and becomes a path match.
fn filter_file_match(
    mut fm: FileMatch,
    include_matchers: &[Regex],
    original_pattern_matchers: &[Regex],
) -> FileMatch {
    let chunks = std::mem::take(&mut fm.chunk_matches);
    fm.chunk_matches = chunks
        .into_iter()
        .filter_map(|chunk| {
            let chunk = filter_chunk(chunk, include_matchers, original_pattern_matchers);
            if chunk.ranges.is_empty() {
                None
            } else {
                Some(chunk)
            }
        })
        .collect();
    fm
}

/// `filterChunk` (`filter_file_contains.go:142-152`).
fn filter_chunk(
    mut chunk: ChunkMatch,
    include_matchers: &[Regex],
    original_pattern_matchers: &[Regex],
) -> ChunkMatch {
    let matched = matched_content(&chunk);
    let mut filtered = Vec::with_capacity(chunk.ranges.len());
    for (i, value) in matched.iter().enumerate() {
        if matches_any(value, include_matchers) && !matches_any(value, original_pattern_matchers) {
            continue;
        }
        if let Some(range) = chunk.ranges.get(i) {
            filtered.push(*range);
        }
    }
    chunk.ranges = filtered;
    chunk
}

/// `ChunkMatch.MatchedContent` (`internal/search/result/file.go:257-275`).
fn matched_content(chunk: &ChunkMatch) -> Vec<String> {
    let content_start = chunk.content_start;
    chunk
        .ranges
        .iter()
        .map(|range| {
            let start = range.start.offset - content_start.offset;
            let end = range.end.offset - content_start.offset;
            let len = chunk.content.len() as i64;
            let start = start.clamp(0, len) as usize;
            let end = end.clamp(0, len) as usize;
            if start <= end {
                chunk.content.get(start..end).unwrap_or_default().to_owned()
            } else {
                String::new()
            }
        })
        .collect()
}

fn matches_any(value: &str, matchers: &[Regex]) -> bool {
    matchers.iter().any(|re| re.is_match(value))
}

/// `removeUnmatchedFileDiffs` core. Returns the kept diffs, or `None` when the
/// commit match should be dropped.
pub fn filter_unmatched_file_diffs(
    cm: &mut CommitMatch,
    matched_file_counts: &HashMap<String, usize>,
    include_pattern_count: usize,
) -> Option<Vec<DiffFile>> {
    let preview = cm.diff_preview.as_mut()?;

    // Ensure the matched ranges are sorted by start offset.
    preview
        .matched_ranges
        .sort_by_key(|range| range.start.offset);

    let diffs = parse_diff_string(&preview.content).unwrap_or_default();
    let diff_strings: Vec<String> = diffs
        .iter()
        .map(|diff| format_diff_files(std::slice::from_ref(diff)))
        .collect();

    // grouped_ranges[i] is the set of ranges contained by diff_strings[i].
    let ranges = preview.matched_ranges.clone();
    let mut grouped_ranges: Vec<Vec<Range>> = vec![Vec::new(); diffs.len()];
    {
        let mut range_num_start = 0usize;
        let mut current_diff_end = 0i64;
        'outer: for (i, diff_string) in diff_strings.iter().enumerate() {
            current_diff_end += diff_string.len() as i64;
            let mut range_num = range_num_start;
            while range_num < ranges.len() {
                if ranges[range_num].start.offset > current_diff_end {
                    grouped_ranges[i] = ranges[range_num_start..range_num].to_vec();
                    range_num_start = range_num;
                    continue 'outer;
                }
                range_num += 1;
            }
            grouped_ranges[i] = ranges[range_num_start..].to_vec();
        }
    }

    let mut filtered_diffs: Vec<DiffFile> = Vec::new();
    let mut filtered_diff_strings: Vec<String> = Vec::new();
    let mut filtered_ranges: Vec<Vec<Range>> = Vec::new();
    let mut removed_amount = Location::default();
    for (i, diff) in diffs.iter().enumerate() {
        let count = matched_file_counts
            .get(&diff.new_name)
            .copied()
            .unwrap_or(0);
        if count == include_pattern_count {
            filtered_diffs.push(diff.clone());
            filtered_diff_strings.push(diff_strings[i].clone());
            filtered_ranges.push(
                grouped_ranges[i]
                    .iter()
                    .map(|range| range_sub(*range, removed_amount))
                    .collect(),
            );
        } else {
            // Not all `file:contains.content()` patterns matched this file.
            // Drop it and shift subsequent ranges up.
            removed_amount = location_add(
                removed_amount,
                Location {
                    offset: diff_strings[i].len() as i64,
                    line: diff_strings[i].matches('\n').count() as i64,
                    column: 0,
                },
            );
        }
    }

    let ungrouped_ranges: Vec<Range> = filtered_ranges.into_iter().flatten().collect();
    preview.matched_ranges = ungrouped_ranges;
    preview.content = filtered_diff_strings.concat();
    if filtered_diffs.is_empty() {
        None
    } else {
        Some(filtered_diffs)
    }
}

fn location_add(a: Location, b: Location) -> Location {
    Location {
        offset: a.offset + b.offset,
        line: a.line + b.line,
        column: a.column + b.column,
    }
}

fn location_sub(a: Location, b: Location) -> Location {
    Location {
        offset: a.offset - b.offset,
        line: a.line - b.line,
        column: a.column - b.column,
    }
}

fn range_sub(range: Range, amount: Location) -> Range {
    Range {
        start: location_sub(range.start, amount),
        end: location_sub(range.end, amount),
    }
}

/// `query.UnionRegExps` (`internal/search/query/helpers.go:15-30`).
fn union_reg_exps(values: &[String]) -> String {
    match values.len() {
        0 => String::new(),
        1 => values[0].clone(),
        _ => format!("(?:{})", values.join(")|(?:")),
    }
}

/// `FormatDiffFiles` (`internal/search/result/commit_diff.go:118-139`).
fn format_diff_files(diffs: &[DiffFile]) -> String {
    use std::fmt::Write as _;

    let mut buf = String::new();
    for diff in diffs {
        buf.push_str(&escape_diff_name(&diff.orig_name));
        buf.push(' ');
        buf.push_str(&escape_diff_name(&diff.new_name));
        buf.push('\n');
        for hunk in &diff.hunks {
            let _ = write!(
                buf,
                "@@ -{},{} +{},{} @@",
                hunk.old_start, hunk.old_count, hunk.new_start, hunk.new_count
            );
            if !hunk.header.is_empty() {
                buf.push(' ');
                buf.push_str(&hunk.header);
            }
            buf.push('\n');
            for line in &hunk.lines {
                buf.push_str(line);
                buf.push('\n');
            }
        }
    }
    buf
}

/// `escaper`/`unescaper` (`commit_diff.go:141-142`).
fn escape_diff_name(name: &str) -> String {
    name.replace(' ', "\\ ")
}

fn unescape_diff_name(name: &str) -> String {
    name.replace("\\ ", " ")
}

/// Malformed diff input. `ParseDiffString` returns an error here and callers
/// ignore it; the payload is intentionally empty.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DiffParseError;

impl std::fmt::Display for DiffParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("failed to parse diff")
    }
}

impl std::error::Error for DiffParseError {}

/// `ParseDiffString` (`internal/search/result/commit_diff.go:144-198`). `Err`
/// mirrors the baseline's `nil, err` return, which callers ignore.
pub fn parse_diff_string(diff: &str) -> Result<Vec<DiffFile>, DiffParseError> {
    let split_re = Regex::new(r"(.*[^\\]) (.*)").map_err(|_| DiffParseError)?;
    let header_re = Regex::new(r"@@ -([0-9]+),([0-9]+) \+([0-9]+),([0-9]+) @@ ?(.*)")
        .map_err(|_| DiffParseError)?;

    #[derive(Clone, Copy, PartialEq, Eq)]
    enum State {
        Init,
        InDiff,
        InHunk,
    }

    let mut res = Vec::new();
    let mut current_diff = DiffFile::default();
    let mut current_hunk = Hunk::default();
    let mut state = State::Init;

    for line in diff.split('\n') {
        if line.is_empty() {
            continue;
        }
        match state {
            State::Init => {
                let (orig, new) = split_diff_files(line, &split_re).ok_or(DiffParseError)?;
                current_diff.orig_name = orig;
                current_diff.new_name = new;
                state = State::InDiff;
            }
            State::InDiff => {
                current_hunk = parse_hunk_header(line, &header_re).ok_or(DiffParseError)?;
                state = State::InHunk;
            }
            State::InHunk => match line.as_bytes()[0] {
                b'-' | b'+' | b' ' => current_hunk.lines.push(line.to_owned()),
                b'@' => {
                    current_diff.hunks.push(std::mem::take(&mut current_hunk));
                    current_hunk = parse_hunk_header(line, &header_re).ok_or(DiffParseError)?;
                    state = State::InHunk;
                }
                _ => {
                    current_diff.hunks.push(std::mem::take(&mut current_hunk));
                    res.push(std::mem::take(&mut current_diff));
                    let (orig, new) = split_diff_files(line, &split_re).ok_or(DiffParseError)?;
                    current_diff.orig_name = orig;
                    current_diff.new_name = new;
                    state = State::InDiff;
                }
            },
        }
    }
    current_diff.hunks.push(current_hunk);
    res.push(current_diff);

    Ok(res)
}

fn split_diff_files(line: &str, re: &Regex) -> Option<(String, String)> {
    let caps = re.captures(line)?;
    let orig = caps.get(1)?.as_str();
    let new = caps.get(2)?.as_str();
    Some((unescape_diff_name(orig), unescape_diff_name(new)))
}

fn parse_hunk_header(line: &str, re: &Regex) -> Option<Hunk> {
    let caps = re.captures(line)?;
    Some(Hunk {
        old_start: caps.get(1)?.as_str().parse().ok()?,
        old_count: caps.get(2)?.as_str().parse().ok()?,
        new_start: caps.get(3)?.as_str().parse().ok()?,
        new_count: caps.get(4)?.as_str().parse().ok()?,
        header: caps
            .get(5)
            .map(|group| group.as_str().to_owned())
            .unwrap_or_default(),
        lines: Vec::new(),
    })
}

// ---------------------------------------------------------------------------
// `file:has.contributor()`
// ---------------------------------------------------------------------------

/// `gitdomain.ContributorCount`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContributorCount {
    pub name: String,
    pub email: String,
    pub count: i32,
}

/// `gitserver.ContributorOptions` subset (`Range`, `Path`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContributorOptions {
    pub range: String,
    pub path: String,
}

/// `gitserver.Client.ContributorCount`, injected because the real client talks
/// to gitserver over gRPC.
pub trait ContributorFetcher {
    fn contributor_count(
        &self,
        repo_name: &str,
        options: &ContributorOptions,
    ) -> Result<Vec<ContributorCount>, JobError>;
}

/// `filter_file_contributor.go:33-38`.
pub struct FilterFileHasContributorsJob {
    include: Vec<Regex>,
    exclude: Vec<Regex>,
    fetcher: Box<dyn ContributorFetcher>,
    child: Box<dyn SearchJob>,
}

impl FilterFileHasContributorsJob {
    /// `NewFileHasContributorsJob`. The baseline takes already-compiled
    /// regexps; callers compile them (and report their own errors).
    pub fn new(
        child: Box<dyn SearchJob>,
        include: Vec<Regex>,
        exclude: Vec<Regex>,
        fetcher: Box<dyn ContributorFetcher>,
    ) -> Self {
        Self {
            include,
            exclude,
            fetcher,
            child,
        }
    }

    /// `Filtered` (`filter_file_contains.go:152-166` exposed for differential
    /// testing): all filters are AND'ed, and `exclude_contributors` negates
    /// each one.
    pub fn filtered(&self, contributors: &[ContributorCount], exclude_contributors: bool) -> bool {
        if exclude_contributors {
            !any_filter_matches(contributors, &self.exclude)
        } else {
            all_filters_match(contributors, &self.include)
        }
    }
}

impl SearchJob for FilterFileHasContributorsJob {
    fn run(
        &mut self,
        ctx: &JobContext,
        clients: &RuntimeClients<'_>,
        stream: &mut dyn FnMut(SearchEvent),
    ) -> Result<(), JobError> {
        let Self {
            include,
            exclude,
            fetcher,
            child,
        } = self;
        let deadline = ctx.deadline;
        let mut errs: Vec<JobError> = Vec::new();
        let result = {
            let mut filtered = |event: SearchEvent| {
                let mut event = event;
                let results = std::mem::take(&mut event.results);
                let mut kept = Vec::with_capacity(results.len());
                for result in results {
                    // Filter out any result that is not a file.
                    let Match::File(fm) = result else {
                        continue;
                    };
                    if deadline.is_some_and(|deadline| Instant::now() >= deadline) {
                        errs.push(JobError("context deadline exceeded".to_owned()));
                        break;
                    }
                    let options = ContributorOptions {
                        range: fm.file.commit_id.clone(),
                        path: fm.file.path.clone(),
                    };
                    let contributors = match fetcher.contributor_count(&fm.file.repo.name, &options)
                    {
                        Ok(contributors) => contributors,
                        Err(err) => {
                            errs.push(err);
                            continue;
                        }
                    };
                    if any_filter_matches(&contributors, exclude) {
                        continue;
                    }
                    if !all_filters_match(&contributors, include) {
                        continue;
                    }
                    kept.push(Match::File(fm));
                }
                event.results = kept;
                stream(event);
            };
            child.run(ctx, clients, &mut filtered)
        };
        if let Err(err) = &result {
            errs.push(JobError(err.to_string()));
        }
        if errs.is_empty() {
            result
        } else {
            Err(JobError(
                errs.iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
                    .join("; "),
            ))
        }
    }

    fn name(&self) -> &'static str {
        "FileHasContributorsFilterJob"
    }

    fn children(&self) -> Vec<&dyn SearchJob> {
        vec![self.child.as_ref()]
    }

    fn attributes(&self, verbosity: Verbosity) -> Vec<(String, String)> {
        let mut res = Vec::new();
        if matches!(verbosity, Verbosity::Basic | Verbosity::Max) {
            res.push((
                "includeContributors".to_owned(),
                string_slice(self.include.iter().map(Regex::as_str)),
            ));
            res.push((
                "excludeContributors".to_owned(),
                string_slice(self.exclude.iter().map(Regex::as_str)),
            ));
        }
        res
    }
}

fn contributor_matches(contributors: &[ContributorCount], re: &Regex) -> bool {
    contributors
        .iter()
        .any(|contributor| re.is_match(&contributor.name) || re.is_match(&contributor.email))
}

fn any_filter_matches(contributors: &[ContributorCount], filters: &[Regex]) -> bool {
    filters
        .iter()
        .any(|filter| contributor_matches(contributors, filter))
}

fn all_filters_match(contributors: &[ContributorCount], filters: &[Regex]) -> bool {
    filters
        .iter()
        .all(|filter| contributor_matches(contributors, filter))
}

// ---------------------------------------------------------------------------
// `select:`
// ---------------------------------------------------------------------------

/// `result.Key` (`internal/search/result/match.go:57-86`), restricted to the
/// fields these match types set. Author dates are encoded as
/// `(unix nanos, utc offset seconds)` so the key is `Hash + Eq`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct MatchKey {
    repo: String,
    rev: String,
    author_nanos: i128,
    author_offset: i32,
    commit: String,
    path: String,
    type_rank: i32,
}

/// Baseline match ranks (`match.go:44-50`).
mod rank {
    pub const FILE: i32 = 0;
    pub const COMMIT: i32 = 1;
    pub const DIFF: i32 = 2;
    pub const REPO: i32 = 3;
    pub const OWNER: i32 = 4;
}

/// `select.go:22-33`.
pub struct SelectJob {
    path: Vec<String>,
    child: Box<dyn SearchJob>,
}

impl SelectJob {
    /// `NewSelectJob`.
    pub fn new(path: Vec<String>, child: Box<dyn SearchJob>) -> Self {
        Self { path, child }
    }
}

/// `newSelectingStream`'s per-`Run` state: the `select:` transform plus the
/// dedup set. One instance is created per run, like the baseline's closure.
#[derive(Debug, Default)]
pub struct SelectDeduper {
    path: Vec<String>,
    dedup: HashSet<MatchKey>,
}

impl SelectDeduper {
    pub fn new(path: Vec<String>) -> Self {
        Self {
            path,
            dedup: HashSet::new(),
        }
    }

    /// Applies `Match.Select` and the dedup rules to one event.
    pub fn filter(&mut self, mut event: SearchEvent) -> SearchEvent {
        let results = std::mem::take(&mut event.results);
        let mut selected = Vec::with_capacity(results.len());
        for result in results {
            let Some(current) = select_match(&result, &self.path) else {
                continue;
            };
            let seen = self.dedup.contains(&match_key(&current));
            let is_file_match = matches!(current, Match::File(_));
            if seen && !is_file_match {
                continue;
            }
            if seen && is_file_match {
                if let Match::File(fm) = &current {
                    if is_path_match(fm) {
                        continue;
                    }
                }
            }
            self.dedup.insert(match_key(&current));
            selected.push(current);
        }
        event.results = selected;
        event
    }
}

impl SearchJob for SelectJob {
    fn run(
        &mut self,
        ctx: &JobContext,
        clients: &RuntimeClients<'_>,
        stream: &mut dyn FnMut(SearchEvent),
    ) -> Result<(), JobError> {
        let mut dedup = SelectDeduper::new(self.path.clone());
        let mut selecting = |event: SearchEvent| stream(dedup.filter(event));
        self.child.run(ctx, clients, &mut selecting)
    }

    fn name(&self) -> &'static str {
        "SelectJob"
    }

    fn children(&self) -> Vec<&dyn SearchJob> {
        vec![self.child.as_ref()]
    }

    fn attributes(&self, verbosity: Verbosity) -> Vec<(String, String)> {
        let mut res = Vec::new();
        if matches!(verbosity, Verbosity::Basic | Verbosity::Max) {
            res.push((
                "select".to_owned(),
                string_slice(self.path.iter().map(String::as_str)),
            ));
        }
        res
    }
}

/// `selectJob.Run` over a [`comb::CombinatorJob`] child.
///
/// Behaves exactly like [`SelectJob`] (same transform and dedup) but keeps the
/// child's `(alert, error)` pair, including error classification, so a select
/// node nested in the job tree does not change what an outer `AlertJob`
/// observes.
pub struct SelectCombinatorJob {
    path: Vec<String>,
    child: Box<dyn comb::CombinatorJob>,
}

impl SelectCombinatorJob {
    pub fn new(path: Vec<String>, child: Box<dyn comb::CombinatorJob>) -> Self {
        Self { path, child }
    }
}

impl comb::CombinatorJob for SelectCombinatorJob {
    fn run(
        &mut self,
        ctx: &comb::RunContext,
        clients: &RuntimeClients<'_>,
        stream: comb::Sender<'_>,
    ) -> (Option<comb::Alert>, Option<comb::JobFailure>) {
        let mut dedup = SelectDeduper::new(self.path.clone());
        let mut selecting = |event: SearchEvent| stream(dedup.filter(event));
        self.child.run(ctx, clients, &mut selecting)
    }

    fn name(&self) -> &'static str {
        "SelectJob"
    }

    fn children(&self) -> Vec<&dyn comb::CombinatorJob> {
        vec![self.child.as_ref()]
    }

    fn attributes(&self, verbosity: Verbosity) -> Vec<(String, String)> {
        let mut res = Vec::new();
        if matches!(verbosity, Verbosity::Basic | Verbosity::Max) {
            res.push((
                "select".to_owned(),
                string_slice(self.path.iter().map(String::as_str)),
            ));
        }
        res
    }
}

/// `Match.Select` for the four match types. `OwnerMatch.Select` always returns
/// nil (`owner.go:68-71`).
pub fn select_match(match_: &Match, path: &[String]) -> Option<Match> {
    let root = path.first().map(String::as_str).unwrap_or("");
    match match_ {
        Match::File(fm) => select_file_match(fm, root, path),
        Match::Repo(repo) => {
            if root == "repo" {
                Some(Match::Repo(repo.clone()))
            } else {
                None
            }
        }
        Match::Commit(cm) => select_commit_match(cm, root, path),
        Match::Owner(_) => None,
    }
}

fn select_file_match(fm: &FileMatch, root: &str, path: &[String]) -> Option<Match> {
    match root {
        "repo" => Some(Match::Repo(RepoMatch {
            name: fm.file.repo.name.clone(),
            id: fm.file.repo.id,
            rev: String::new(),
            description_matches: Vec::new(),
            repo_name_matches: Vec::new(),
        })),
        "file" => {
            let mut selected = fm.clone();
            selected.chunk_matches.clear();
            selected.symbols.clear();
            if path.len() > 1 && path[1] == "directory" {
                // `path.Clean(path.Dir(fm.Path)) + "/"`.
                selected.file.path =
                    format!("{}/", go_path_clean(&go_path_dir(&selected.file.path)));
            }
            Some(Match::File(selected))
        }
        "symbol" => {
            if fm.symbols.is_empty() {
                return None;
            }
            let mut selected = fm.clone();
            selected.chunk_matches.clear();
            if path.len() > 1 {
                let kind = &path[1];
                selected.symbols.retain(|symbol_match| {
                    select_kind(&symbol_match.symbol).as_deref() == Some(kind.as_str())
                });
                if selected.symbols.is_empty() {
                    return None;
                }
            }
            Some(Match::File(selected))
        }
        "content" => {
            if fm.chunk_matches.is_empty() {
                return None;
            }
            let mut selected = fm.clone();
            selected.symbols.clear();
            selected.path_matches.clear();
            Some(Match::File(selected))
        }
        "commit" => None,
        _ => None,
    }
}

fn select_commit_match(cm: &CommitMatch, root: &str, path: &[String]) -> Option<Match> {
    match root {
        "repo" => Some(Match::Repo(RepoMatch {
            name: cm.repo.name.clone(),
            id: cm.repo.id,
            rev: String::new(),
            description_matches: Vec::new(),
            repo_name_matches: Vec::new(),
        })),
        "commit" => {
            let fields = &path[1..];
            if !fields.is_empty() && fields[0] == "diff" {
                let preview = cm.diff_preview.as_ref()?;
                if fields.len() == 1 {
                    return Some(Match::Commit(cm.clone()));
                }
                if fields.len() == 2 {
                    let mut selected = cm.clone();
                    let filtered = select_commit_diff_kind(preview, &fields[1])?;
                    selected.diff_preview = Some(filtered);
                    return Some(Match::Commit(selected));
                }
                return None;
            }
            Some(Match::Commit(cm.clone()))
        }
        _ => None,
    }
}

/// `selectCommitDiffKind` (`internal/search/result/commit.go:229-252`).
fn select_commit_diff_kind(preview: &MatchedString, field: &str) -> Option<MatchedString> {
    let prefix = match field {
        "added" => "+",
        "removed" => "-",
        _ => "",
    };
    if preview.matched_ranges.is_empty() {
        // No highlights: keep based on the existence of matching lines.
        return if modified_lines_exist(&preview.content, prefix) {
            Some(preview.clone())
        } else {
            None
        };
    }
    let highlights = select_modified_lines(&preview.content, &preview.matched_ranges, prefix);
    if highlights.is_empty() {
        None
    } else {
        let mut selected = preview.clone();
        selected.matched_ranges = highlights;
        Some(selected)
    }
}

/// `selectModifiedLines` (`internal/search/result/commit.go:199-220`).
fn select_modified_lines(content: &str, highlights: &[Range], prefix: &str) -> Vec<Range> {
    let lines: Vec<&str> = content.split('\n').collect();
    if lines.is_empty() {
        return highlights.to_vec();
    }
    highlights
        .iter()
        .filter(|highlight| highlight.start.line >= 0)
        .filter(|highlight| {
            lines
                .get(highlight.start.line as usize)
                .is_some_and(|line| line.starts_with(prefix))
        })
        .copied()
        .collect()
}

/// `modifiedLinesExist` (`internal/search/result/commit.go:223-226`).
fn modified_lines_exist(content: &str, prefix: &str) -> bool {
    content.split('\n').any(|line| line.starts_with(prefix))
}

fn match_key(match_: &Match) -> MatchKey {
    match match_ {
        Match::File(fm) => MatchKey {
            repo: fm.file.repo.name.clone(),
            rev: fm.file.input_rev.clone().unwrap_or_default(),
            author_nanos: 0,
            author_offset: 0,
            commit: fm.file.commit_id.clone(),
            path: fm.file.path.clone(),
            type_rank: rank::FILE,
        },
        Match::Repo(repo) => MatchKey {
            repo: repo.name.clone(),
            rev: repo.rev.clone(),
            author_nanos: 0,
            author_offset: 0,
            commit: String::new(),
            path: String::new(),
            type_rank: rank::REPO,
        },
        Match::Commit(cm) => MatchKey {
            repo: cm.repo.name.clone(),
            rev: String::new(),
            author_nanos: cm.commit.author.date.unix_timestamp_nanos(),
            author_offset: cm.commit.author.date.offset().whole_seconds(),
            commit: cm.commit.id.clone(),
            path: String::new(),
            type_rank: if cm.diff_preview.is_some() {
                rank::DIFF
            } else {
                rank::COMMIT
            },
        },
        Match::Owner(owner) => MatchKey {
            repo: owner.repo.name.clone(),
            rev: String::new(),
            author_nanos: 0,
            author_offset: 0,
            commit: String::new(),
            path: String::new(),
            type_rank: rank::OWNER,
        },
    }
}

/// `FileMatch.IsPathMatch` (`internal/search/result/file.go:124-126`).
fn is_path_match(fm: &FileMatch) -> bool {
    fm.chunk_matches.is_empty() && fm.symbols.is_empty()
}

/// Go `path.Dir` for slash-separated paths (`internal/search/result/file.go:139`).
fn go_path_dir(path: &str) -> String {
    match path.rfind('/') {
        None => ".".to_owned(),
        Some(index) => go_path_clean(&path[..=index]),
    }
}

/// Go `path.Clean` for slash-separated paths.
fn go_path_clean(path: &str) -> String {
    if path.is_empty() {
        return ".".to_owned();
    }
    let rooted = path.starts_with('/');
    let mut parts: Vec<&str> = Vec::new();
    for segment in path.split('/') {
        match segment {
            "" | "." => {}
            ".." => {
                if let Some(last) = parts.last() {
                    if *last != ".." {
                        parts.pop();
                    } else if !rooted {
                        parts.push("..");
                    }
                } else if !rooted {
                    parts.push("..");
                }
            }
            other => parts.push(other),
        }
    }
    let joined = parts.join("/");
    if rooted {
        format!("/{joined}")
    } else if joined.is_empty() {
        ".".to_owned()
    } else {
        joined
    }
}

/// Normalize a Go `attribute.StringSlice` like `fmt.Sprintf("%v", []string)`.
fn string_slice<I, S>(values: I) -> String
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    let joined = values
        .into_iter()
        .map(|value| value.as_ref().to_owned())
        .collect::<Vec<_>>()
        .join(" ");
    format!("[{joined}]")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::search_job::{JobStreamer, PrivateRepoLister, ReposListOptions};
    use crate::search_stream::convert::{FileRef, GitCommit, MinimalRepo, Signature};
    use sg_zoekt::indexed_repo_revs::MinimalRepo as ZoektMinimalRepo;
    use sg_zoekt::query::Query;
    use sg_zoekt::types::{SearchOptions, SearchResult};
    use time::OffsetDateTime;

    struct NoopStreamer;

    impl JobStreamer for NoopStreamer {
        fn stream_search(
            &self,
            _query: &Query,
            _options: &SearchOptions,
            _sender: &mut dyn FnMut(SearchResult),
        ) -> Result<(), JobError> {
            panic!("stream_search must not be called");
        }
    }

    struct NoopLister;

    impl PrivateRepoLister for NoopLister {
        fn list_minimal_repos(
            &self,
            _options: &ReposListOptions,
        ) -> Result<Vec<ZoektMinimalRepo>, JobError> {
            Ok(Vec::new())
        }
    }

    struct StaticJob {
        events: Vec<SearchEvent>,
    }

    impl SearchJob for StaticJob {
        fn run(
            &mut self,
            _ctx: &JobContext,
            _clients: &RuntimeClients<'_>,
            stream: &mut dyn FnMut(SearchEvent),
        ) -> Result<(), JobError> {
            for event in &self.events {
                stream(event.clone());
            }
            Ok(())
        }

        fn name(&self) -> &'static str {
            "StaticJob"
        }

        fn attributes(&self, _verbosity: Verbosity) -> Vec<(String, String)> {
            Vec::new()
        }
    }

    fn clients<'a>(db: &'a NoopLister, zoekt: &'a NoopStreamer) -> RuntimeClients<'a> {
        RuntimeClients {
            db,
            zoekt,
            max_repos: None,
            searcher: None,
            commit: None,
        }
    }

    fn file(path: &str, chunks: Vec<(&str, Vec<(i64, i64)>)>) -> Match {
        let chunk_matches = chunks
            .into_iter()
            .map(|(content, ranges)| ChunkMatch {
                content: content.to_owned(),
                content_start: Location::default(),
                ranges: ranges
                    .into_iter()
                    .map(|(start, end)| Range {
                        start: Location {
                            offset: start,
                            line: 0,
                            column: start,
                        },
                        end: Location {
                            offset: end,
                            line: 0,
                            column: end,
                        },
                    })
                    .collect::<Vec<_>>(),
            })
            .collect::<Vec<_>>();
        Match::File(FileMatch {
            file: FileRef {
                repo: MinimalRepo {
                    id: 1,
                    name: "repo".to_owned(),
                },
                commit_id: "abc".to_owned(),
                path: path.to_owned(),
                input_rev: None,
                precise_language: String::new(),
            },
            chunk_matches,
            symbols: Vec::new(),
            path_matches: Vec::new(),
            limit_hit: false,
            debug: None,
        })
    }

    fn run_job(job: &mut dyn SearchJob) -> Vec<SearchEvent> {
        let streamer = NoopStreamer;
        let lister = NoopLister;
        let clients = clients(&lister, &streamer);
        let ctx = JobContext::default();
        let mut events = Vec::new();
        job.run(&ctx, &clients, &mut |event| events.push(event))
            .unwrap();
        events
    }

    #[test]
    fn contains_filters_ranges_and_keeps_path_matches() {
        // "foo bar" with ranges over "foo" and "bar".
        let input = file("a.txt", vec![("foo bar", vec![(0, 3), (4, 7)])]);
        let child = Box::new(StaticJob {
            events: vec![SearchEvent {
                results: vec![input],
                stats: Default::default(),
            }],
        });
        let mut job =
            FilterFileContainsJob::new(vec!["foo".to_owned()], None, true, child).unwrap();
        let events = run_job(&mut job);
        let Match::File(fm) = &events[0].results[0] else {
            panic!("expected file match");
        };
        assert_eq!(fm.chunk_matches.len(), 1);
        assert_eq!(fm.chunk_matches[0].ranges.len(), 1);
        assert_eq!(fm.chunk_matches[0].ranges[0].start.offset, 4);

        // All ranges filtered: the file is still returned as a path match.
        let input = file("b.txt", vec![("foo", vec![(0, 3)])]);
        let child = Box::new(StaticJob {
            events: vec![SearchEvent {
                results: vec![input],
                stats: Default::default(),
            }],
        });
        let mut job =
            FilterFileContainsJob::new(vec!["foo".to_owned()], None, true, child).unwrap();
        let events = run_job(&mut job);
        let Match::File(fm) = &events[0].results[0] else {
            panic!("expected file match");
        };
        assert!(fm.chunk_matches.is_empty());
        assert!(is_path_match(fm));
    }

    #[test]
    fn contains_case_sensitivity_and_original_pattern() {
        let make_input = || file("a.txt", vec![("FOO", vec![(0, 3)])]);

        let child = Box::new(StaticJob {
            events: vec![SearchEvent {
                results: vec![make_input()],
                stats: Default::default(),
            }],
        });
        let mut insensitive =
            FilterFileContainsJob::new(vec!["foo".to_owned()], None, false, child).unwrap();
        let events = run_job(&mut insensitive);
        let Match::File(fm) = &events[0].results[0] else {
            panic!("expected file match");
        };
        assert!(fm.chunk_matches.is_empty());

        let child = Box::new(StaticJob {
            events: vec![SearchEvent {
                results: vec![make_input()],
                stats: Default::default(),
            }],
        });
        let mut sensitive =
            FilterFileContainsJob::new(vec!["foo".to_owned()], None, true, child).unwrap();
        let events = run_job(&mut sensitive);
        let Match::File(fm) = &events[0].results[0] else {
            panic!("expected file match");
        };
        assert_eq!(fm.chunk_matches[0].ranges.len(), 1);

        // An original pattern matching the same text keeps the range.
        let child = Box::new(StaticJob {
            events: vec![SearchEvent {
                results: vec![make_input()],
                stats: Default::default(),
            }],
        });
        let original = PatternNode::Pattern("FOO".to_owned());
        let mut job =
            FilterFileContainsJob::new(vec!["foo".to_owned()], Some(&original), false, child)
                .unwrap();
        let events = run_job(&mut job);
        let Match::File(fm) = &events[0].results[0] else {
            panic!("expected file match");
        };
        assert_eq!(fm.chunk_matches[0].ranges.len(), 1);
    }

    #[test]
    fn contains_constructor_reports_stage() {
        let child = || Box::new(StaticJob { events: Vec::new() }) as Box<dyn SearchJob>;
        let err = FilterFileContainsJob::new(vec!["(".to_owned()], None, true, child())
            .err()
            .expect("invalid include pattern");
        assert_eq!(err.stage, ContainsPatternStage::Include);
        let original = PatternNode::Operator(vec![PatternNode::Pattern("[".to_owned())]);
        let err = FilterFileContainsJob::new(vec![], Some(&original), true, child())
            .err()
            .expect("invalid original pattern");
        assert_eq!(err.stage, ContainsPatternStage::Original);
    }

    struct FixedSearcher {
        matches: HashMap<String, Vec<String>>,
    }

    impl CommitContentSearcher for FixedSearcher {
        fn search_include(&self, request: &IncludeSearchRequest) -> Result<Vec<String>, JobError> {
            Ok(self
                .matches
                .get(&request.pattern)
                .cloned()
                .unwrap_or_default())
        }
    }

    fn commit_with_diff(content: &str, ranges: Vec<Range>) -> Match {
        Match::Commit(CommitMatch {
            commit: GitCommit {
                id: "c1".to_owned(),
                message: "msg".to_owned(),
                author: Signature {
                    name: "a".to_owned(),
                    email: "a@example.com".to_owned(),
                    date: OffsetDateTime::UNIX_EPOCH,
                },
                committer: Signature {
                    name: "a".to_owned(),
                    email: "a@example.com".to_owned(),
                    date: OffsetDateTime::UNIX_EPOCH,
                },
                parents: Vec::new(),
            },
            repo: MinimalRepo {
                id: 1,
                name: "repo".to_owned(),
            },
            message_preview: None,
            diff_preview: Some(MatchedString {
                content: content.to_owned(),
                matched_ranges: ranges,
            }),
            refs: Vec::new(),
            source_refs: Vec::new(),
            diff: Vec::new(),
            modified_files: Vec::new(),
        })
    }

    #[test]
    fn contains_commit_uses_injected_searcher_and_filters_diffs() {
        let content = "a.txt b.txt\n@@ -1,1 +1,1 @@\n-old\n+new\n";
        let input = commit_with_diff(content, Vec::new());
        let child = Box::new(StaticJob {
            events: vec![SearchEvent {
                results: vec![input],
                stats: Default::default(),
            }],
        });
        let mut searcher_matches = HashMap::new();
        searcher_matches.insert("foo".to_owned(), vec!["b.txt".to_owned()]);
        let mut job = FilterFileContainsJob::new(vec!["foo".to_owned()], None, true, child)
            .unwrap()
            .with_searcher(Box::new(FixedSearcher {
                matches: searcher_matches,
            }));
        let events = run_job(&mut job);
        let Match::Commit(cm) = &events[0].results[0] else {
            panic!("expected commit match");
        };
        assert!(cm.diff_preview.is_some());

        // A searcher error drops the whole commit match.
        let input = commit_with_diff(content, Vec::new());
        let child = Box::new(StaticJob {
            events: vec![SearchEvent {
                results: vec![input],
                stats: Default::default(),
            }],
        });
        struct FailingSearcher;
        impl CommitContentSearcher for FailingSearcher {
            fn search_include(
                &self,
                _request: &IncludeSearchRequest,
            ) -> Result<Vec<String>, JobError> {
                Err(JobError("boom".to_owned()))
            }
        }
        let mut job = FilterFileContainsJob::new(vec!["foo".to_owned()], None, true, child)
            .unwrap()
            .with_searcher(Box::new(FailingSearcher));
        let events = run_job(&mut job);
        assert!(events[0].results.is_empty());
    }

    struct FixedFetcher {
        contributors: HashMap<String, Vec<ContributorCount>>,
    }

    impl ContributorFetcher for FixedFetcher {
        fn contributor_count(
            &self,
            _repo_name: &str,
            options: &ContributorOptions,
        ) -> Result<Vec<ContributorCount>, JobError> {
            Ok(self
                .contributors
                .get(&options.path)
                .cloned()
                .unwrap_or_default())
        }
    }

    fn file_match(path: &str) -> Match {
        file(path, Vec::new())
    }

    #[test]
    fn contributor_filtered_semantics() {
        let job = FilterFileHasContributorsJob::new(
            Box::new(StaticJob { events: Vec::new() }),
            vec![Regex::new("alice").unwrap()],
            vec![Regex::new("mallory@").unwrap()],
            Box::new(FixedFetcher {
                contributors: HashMap::new(),
            }),
        );
        let alice = vec![ContributorCount {
            name: "Alice".to_owned(),
            email: "alice@example.com".to_owned(),
            count: 1,
        }];
        let mallory = vec![ContributorCount {
            name: "Mallory".to_owned(),
            email: "mallory@example.com".to_owned(),
            count: 1,
        }];
        assert!(job.filtered(&alice, false));
        assert!(job.filtered(&alice, true));
        assert!(!job.filtered(&mallory, false));
        assert!(!job.filtered(&mallory, true));
    }

    #[test]
    fn contributor_run_filters_and_accumulates_errors() {
        let mut contributors = HashMap::new();
        contributors.insert(
            "a.txt".to_owned(),
            vec![ContributorCount {
                name: "Alice".to_owned(),
                email: "alice@example.com".to_owned(),
                count: 1,
            }],
        );
        contributors.insert("b.txt".to_owned(), Vec::new());
        let child = Box::new(StaticJob {
            events: vec![SearchEvent {
                results: vec![file_match("a.txt"), file_match("b.txt")],
                stats: Default::default(),
            }],
        });
        let mut job = FilterFileHasContributorsJob::new(
            child,
            vec![Regex::new("alice").unwrap()],
            Vec::new(),
            Box::new(FixedFetcher { contributors }),
        );
        let events = run_job(&mut job);
        assert_eq!(events[0].results.len(), 1);
    }

    #[test]
    fn select_file_and_repo_dedup() {
        // select:file clears ranges/symbols and turns matches into path matches.
        let child = Box::new(StaticJob {
            events: vec![SearchEvent {
                results: vec![file("dir/a.txt", Vec::new()), file("dir/a.txt", Vec::new())],
                stats: Default::default(),
            }],
        });
        let mut job = SelectJob::new(vec!["file".to_owned()], child);
        let events = run_job(&mut job);
        assert_eq!(events[0].results.len(), 1);

        // select:repo collapses file + repo + commit into a single RepoMatch.
        let commit = commit_with_diff("", Vec::new());
        let repo = Match::Repo(RepoMatch {
            name: "repo".to_owned(),
            id: 1,
            rev: String::new(),
            description_matches: Vec::new(),
            repo_name_matches: Vec::new(),
        });
        let child = Box::new(StaticJob {
            events: vec![SearchEvent {
                results: vec![file_match("a.txt"), repo, commit],
                stats: Default::default(),
            }],
        });
        let mut job = SelectJob::new(vec!["repo".to_owned()], child);
        let events = run_job(&mut job);
        assert_eq!(events[0].results.len(), 1);
        assert!(matches!(events[0].results[0], Match::Repo(_)));
    }

    #[test]
    fn select_commit_diff_added_filters_highlights() {
        let content = "@@ -1,2 +1,2 @@\n-old\n+new\n context";
        // A range on "+new" starts at line 2, column 0.
        let preview = MatchedString {
            content: content.to_owned(),
            matched_ranges: vec![Range {
                start: Location {
                    offset: 22,
                    line: 2,
                    column: 0,
                },
                end: Location {
                    offset: 26,
                    line: 2,
                    column: 4,
                },
            }],
        };
        let selected = select_commit_diff_kind(&preview, "added").expect("added line");
        assert_eq!(selected.matched_ranges.len(), 1);
        assert!(select_commit_diff_kind(&preview, "removed").is_none());

        // Without highlights the existence of `+`/`-` lines decides.
        let plain = MatchedString {
            content: content.to_owned(),
            matched_ranges: Vec::new(),
        };
        assert!(select_commit_diff_kind(&plain, "added").is_some());
        assert!(select_commit_diff_kind(&plain, "removed").is_some());
    }

    #[test]
    fn parse_and_format_diff_round_trip() {
        let content = "a.txt b.txt\n@@ -1,1 +1,1 @@\n-old\n+new\n";
        let diffs = parse_diff_string(content).unwrap();
        assert_eq!(diffs.len(), 1);
        assert_eq!(diffs[0].orig_name, "a.txt");
        assert_eq!(diffs[0].new_name, "b.txt");
        assert_eq!(format_diff_files(&diffs), content);
    }

    #[test]
    fn path_helpers_match_go() {
        assert_eq!(go_path_dir("a/b/c.txt"), "a/b");
        assert_eq!(go_path_dir("c.txt"), ".");
        assert_eq!(go_path_dir("/a"), "/");
        assert_eq!(go_path_dir("a//b"), "a");
        assert_eq!(go_path_clean("/a/b/"), "/a/b");
        assert_eq!(go_path_clean("a/../b"), "b");
        assert_eq!(go_path_clean("../a"), "../a");
        assert_eq!(go_path_clean("/../a"), "/a");
        assert_eq!(go_path_clean(""), ".");
    }
}
