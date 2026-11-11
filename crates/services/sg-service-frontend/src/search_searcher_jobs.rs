//! Searcher (`index:no`) jobs: `internal/search/searcher/search.go:57-280`
//! (`TextSearchJob`) and `internal/search/searcher/symbol_search_job.go:29-215`
//! (`SymbolSearchJob`).
//!
//! Unlike the zoekt jobs in [`crate::search_job`], these wrap the just-in-time
//! searcher service and the symbols service. The execution functions are
//! injected through [`crate::search_searcher_client::SearcherClients`]:
//! revision resolution, archive-fetch timeout/routing, the mutable text-search
//! limiter, and the symbols search all live behind traits so the differential
//! harness can supply fakes.
//!
//! Not ported: real gRPC/TLS dialing, concurrency (the baseline launches one
//! goroutine per rev/repo; this port runs them sequentially), tracing spans and
//! log fields. Events, stats, request construction and error classification
//! follow the baseline exactly.
//!
//! See `docs/overviews/m9_8hx_searcher_jobs_overview.md`.

use std::fmt;
use std::time::{Duration, Instant};

use regex::Regex;

use crate::search_job::{JobContext, JobError, RuntimeClients, SearchJob, Verbosity};
use crate::search_searcher_client::{
    protocol, SearcherClients, SearcherError, SearcherErrorKind, SymbolsParameters,
    NOT_PORTED_MESSAGE,
};
use crate::search_stream::convert::{self, Match};
use crate::search_stream::events::{Location, Range};
use crate::search_stream::progress::{repo_status, RepoStatusMap, SearchEvent, Stats};

/// `search.RepositoryRevisions` (`internal/search/repo_revs.go:14-17`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RepositoryRevisions {
    pub repo: convert::MinimalRepo,
    pub revs: Vec<String>,
}

/// `search.TextPatternInfo` (`internal/search/types.go:265-291`), reduced to
/// the fields the searcher job reads.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct TextPatternInfo {
    /// `Query protocol.QueryNode`; `None` models a nil node.
    pub query: Option<protocol::QueryNode>,
    pub is_structural_pat: bool,
    pub comby_rule: String,
    pub is_case_sensitive: bool,
    pub file_match_limit: i32,
    /// `Select filter.SelectPath`; `Search` uses `SelectPath.Root()` (first).
    pub select: Vec<String>,
    pub include_paths: Vec<String>,
    pub exclude_paths: String,
    pub include_langs: Vec<String>,
    pub exclude_langs: Vec<String>,
    pub path_patterns_are_case_sensitive: bool,
    pub pattern_matches_content: bool,
    pub pattern_matches_path: bool,
    pub languages: Vec<String>,
}

impl TextPatternInfo {
    /// `searcher.Search`'s `protocol.PatternInfo` construction
    /// (`client.go:55-70`).
    pub fn to_protocol(&self) -> protocol::PatternInfo {
        protocol::PatternInfo {
            query: self.query.clone(),
            is_structural: self.is_structural_pat,
            is_case_sensitive: self.is_case_sensitive,
            exclude_paths: self.exclude_paths.clone(),
            include_paths: self.include_paths.clone(),
            include_langs: self.include_langs.clone(),
            exclude_langs: self.exclude_langs.clone(),
            path_patterns_are_case_sensitive: self.path_patterns_are_case_sensitive,
            limit: self.file_match_limit,
            pattern_matches_content: self.pattern_matches_content,
            pattern_matches_path: self.pattern_matches_path,
            comby_rule: self.comby_rule.clone(),
            // `p.Select.Root()`.
            select: self.select.first().cloned().unwrap_or_default(),
            languages: self.languages.clone(),
        }
    }
}

impl fmt::Display for TextPatternInfo {
    /// `TextPatternInfo.String` (`internal/search/types.go:336-368`).
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut args: Vec<String> = vec![self
            .query
            .as_ref()
            .map(ToString::to_string)
            .unwrap_or_default()];

        if self.is_structural_pat {
            if self.comby_rule.is_empty() {
                args.push("comby".to_owned());
            } else {
                args.push(format!("comby:{}", self.comby_rule));
            }
        }
        if self.is_case_sensitive {
            args.push("case".to_owned());
        }
        if !self.pattern_matches_content {
            args.push("nocontent".to_owned());
        }
        if !self.pattern_matches_path {
            args.push("nopath".to_owned());
        }
        if self.file_match_limit > 0 {
            args.push(format!("filematchlimit:{}", self.file_match_limit));
        }
        for lang in &self.languages {
            args.push(format!("lang:{lang}"));
        }

        let path = if self.path_patterns_are_case_sensitive {
            "F"
        } else {
            "f"
        };
        if !self.exclude_paths.is_empty() {
            args.push(format!("-{path}:{:?}", self.exclude_paths));
        }
        for include in &self.include_paths {
            args.push(format!("{path}:{include:?}"));
        }

        write!(f, "TextPatternInfo{{{}}}", args.join(","))
    }
}

/// `TextSearchJob` (`internal/search/searcher/search.go:33-55`).
///
/// `Features` is omitted because `searcher.Search` never reads it.
pub struct TextSearchJob {
    pub pattern_info: TextPatternInfo,
    /// `Repos []*search.RepositoryRevisions` (possibly empty).
    pub repos: Vec<RepositoryRevisions>,
    pub path_regexps: Vec<Regex>,
    pub indexed: bool,
    pub num_context_lines: i32,
    pub use_full_deadline: bool,
}

impl SearchJob for TextSearchJob {
    fn run(
        &mut self,
        ctx: &JobContext,
        clients: &RuntimeClients<'_>,
        stream: &mut dyn FnMut(SearchEvent),
    ) -> Result<(), JobError> {
        let Some(searcher) = clients.searcher else {
            return Err(JobError(NOT_PORTED_MESSAGE.to_owned()));
        };

        // The baseline computes the fetch timeout before the empty-repo early
        // return, but only the request path can observe it.
        let fetch_timeout = fetch_timeout(self.repos.len(), self.use_full_deadline, ctx.deadline);

        if self.repos.is_empty() {
            return Ok(());
        }

        let endpoints = searcher.client.endpoints().map_err(job_error)?;
        searcher.limiter.set_limit(endpoints.len() * 32);

        for repo in &self.repos {
            if repo.revs.is_empty() {
                continue;
            }
            for rev in &repo.revs {
                searcher.limiter.acquire().map_err(job_error)?;

                let result = self.search_files_in_repo(searcher, repo, rev, fetch_timeout, stream);
                let (repo_limit_hit, error) = match result {
                    Ok(limit_hit) => (limit_hit, None),
                    Err(error) => (false, Some(error)),
                };

                let (status, fatal) = handle_repo_search_result(
                    repo.repo.id,
                    std::slice::from_ref(rev),
                    repo_limit_hit,
                    false,
                    error.as_ref(),
                );
                let stats = Stats {
                    status,
                    is_limit_hit: repo_limit_hit,
                    ..Stats::default()
                };
                stream(SearchEvent {
                    results: Vec::new(),
                    stats,
                });

                if let Some(fatal) = fatal {
                    return Err(job_error(fatal));
                }
            }
        }
        Ok(())
    }

    fn name(&self) -> &'static str {
        "SearcherTextSearchJob"
    }

    fn attributes(&self, verbosity: Verbosity) -> Vec<(String, String)> {
        let mut res = Vec::new();
        if verbosity == Verbosity::Max {
            res.push((
                "useFullDeadline".to_owned(),
                self.use_full_deadline.to_string(),
            ));
            res.push(("patternInfo".to_owned(), self.pattern_info.to_string()));
            res.push(("numRepos".to_owned(), self.repos.len().to_string()));
            res.push((
                "pathRegexps".to_owned(),
                string_slice(self.path_regexps.iter().map(Regex::as_str)),
            ));
        }
        if matches!(verbosity, Verbosity::Max | Verbosity::Basic) {
            res.push(("indexed".to_owned(), self.indexed.to_string()));
        }
        res
    }
}

impl TextSearchJob {
    /// `TextSearchJob.searchFilesInRepo` (`search.go:182-210`).
    fn search_files_in_repo(
        &self,
        searcher: &SearcherClients<'_>,
        repo: &RepositoryRevisions,
        rev: &str,
        fetch_timeout: Duration,
        stream: &mut dyn FnMut(SearchEvent),
    ) -> Result<bool, SearcherError> {
        // No repo-updater lookup: `EnsureRevision: false`.
        let commit = searcher.gitserver.resolve_revision(&repo.repo.name, rev)?;

        let request = protocol::Request {
            repo: repo.repo.name.clone(),
            repo_id: repo.repo.id as u32,
            commit: commit.clone(),
            // `Search`'s `branch` parameter is the input rev.
            branch: rev.to_owned(),
            pattern_info: self.pattern_info.to_protocol(),
            fetch_timeout,
            indexed: self.indexed,
            num_context_lines: self.num_context_lines,
        };

        let repo = repo.repo.clone();
        let input_rev = rev.to_owned();
        searcher.client.search(&request, &mut |file_match| {
            let results = convert_matches(
                &repo,
                &commit,
                Some(&input_rev),
                std::slice::from_ref(&file_match),
                &self.path_regexps,
            );
            stream(SearchEvent {
                results,
                stats: Stats::default(),
            });
        })
    }
}

/// `SymbolSearchRequest` (`internal/search/searcher/symbol_search_job.go:166-215`).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct SymbolSearchRequest {
    pub regexp_pattern: String,
    pub is_case_sensitive: bool,
    pub include_patterns: Vec<String>,
    pub exclude_pattern: String,
    pub include_langs: Vec<String>,
    pub exclude_langs: Vec<String>,
}

impl SymbolSearchRequest {
    /// `SymbolSearchRequest.Fields`. Keys are scoped with `request.` by
    /// `Attributes`.
    pub fn fields(&self) -> Vec<(&'static str, String)> {
        let mut res = vec![("pattern", self.regexp_pattern.clone())];
        if self.is_case_sensitive {
            res.push(("isCaseSensitive", "true".to_owned()));
        }
        if !self.include_patterns.is_empty() {
            res.push((
                "includePatterns",
                string_slice(self.include_patterns.iter().map(String::as_str)),
            ));
        }
        if !self.exclude_pattern.is_empty() {
            res.push(("excludePattern", self.exclude_pattern.clone()));
        }
        if !self.include_langs.is_empty() {
            res.push((
                "includeLangs",
                string_slice(self.include_langs.iter().map(String::as_str)),
            ));
        }
        if !self.exclude_langs.is_empty() {
            res.push((
                "excludeLangs",
                string_slice(self.exclude_langs.iter().map(String::as_str)),
            ));
        }
        res
    }
}

/// `SymbolSearchJob` (`symbol_search_job.go:21-25`).
pub struct SymbolSearchJob {
    pub request: SymbolSearchRequest,
    pub repos: Vec<RepositoryRevisions>,
    pub limit: i32,
}

impl SearchJob for SymbolSearchJob {
    fn run(
        &mut self,
        ctx: &JobContext,
        clients: &RuntimeClients<'_>,
        stream: &mut dyn FnMut(SearchEvent),
    ) -> Result<(), JobError> {
        let Some(searcher) = clients.searcher else {
            return Err(JobError(NOT_PORTED_MESSAGE.to_owned()));
        };

        for repo in &self.repos {
            // `WithContext`: stop launching once the context is done.
            if context_done(ctx) {
                break;
            }
            if repo.revs.is_empty() {
                continue;
            }

            let result = self.search_in_repo(searcher, repo);
            let (matches, limit_hit, error) = match result {
                Ok((matches, limit_hit)) => (matches, limit_hit, None),
                Err(error) => (Vec::new(), false, Some(error)),
            };
            // The baseline compares the *number of file matches* to the limit.
            let is_limit_hit = (matches.len() as i64) > i64::from(self.limit) || limit_hit;

            let (status, fatal) = handle_repo_search_result(
                repo.repo.id,
                &repo.revs,
                is_limit_hit,
                false,
                error.as_ref(),
            );
            let stats = Stats {
                status,
                is_limit_hit,
                ..Stats::default()
            };
            stream(SearchEvent {
                results: matches,
                stats,
            });

            if let Some(fatal) = fatal {
                return Err(job_error(fatal));
            }
        }
        Ok(())
    }

    fn name(&self) -> &'static str {
        "SearcherSymbolSearchJob"
    }

    fn attributes(&self, verbosity: Verbosity) -> Vec<(String, String)> {
        let mut res = Vec::new();
        if matches!(verbosity, Verbosity::Max | Verbosity::Basic) {
            res.extend(
                self.request
                    .fields()
                    .into_iter()
                    .map(|(key, value)| (format!("request.{key}"), value)),
            );
            res.push(("numRepos".to_owned(), self.repos.len().to_string()));
            res.push(("limit".to_owned(), self.limit.to_string()));
        }
        res
    }
}

impl SymbolSearchJob {
    /// `searchInRepo` (`symbol_search_job.go:95-135`).
    fn search_in_repo(
        &self,
        searcher: &SearcherClients<'_>,
        repo: &RepositoryRevisions,
    ) -> Result<(Vec<Match>, bool), SearcherError> {
        let input_rev = repo.revs.first().expect("caller skips repos without revs");
        let commit = searcher
            .gitserver
            .resolve_revision(&repo.repo.name, input_rev)?;

        let params = SymbolsParameters {
            repo: repo.repo.name.clone(),
            commit_id: commit.clone(),
            query: self.request.regexp_pattern.clone(),
            is_regexp: true,
            is_case_sensitive: self.request.is_case_sensitive,
            include_patterns: self.request.include_patterns.clone(),
            exclude_pattern: self.request.exclude_pattern.clone(),
            include_langs: self.request.include_langs.clone(),
            exclude_langs: self.request.exclude_langs.clone(),
            // Ask for limit + 1 so the service can report limit hits.
            first: self.limit.saturating_add(1),
            // Go leaves `Timeout` unset on the searcher symbol job.
            timeout: Duration::ZERO,
            deadline: None,
        };

        let response = searcher.symbols.search(&params)?;
        let mut symbols = response.symbols;
        for symbol in &mut symbols {
            // Callers expect 1-indexed lines.
            symbol.line += 1;
        }

        Ok((
            symbols_to_matches(symbols, &repo.repo, &commit, input_rev),
            response.limit_hit,
        ))
    }
}

/// `convertMatches` (`search.go:213-269`). The caller passes a single
/// `FileMatch`, matching `searchFilesInRepo`'s `onMatch`.
fn convert_matches(
    repo: &convert::MinimalRepo,
    commit: &str,
    rev: Option<&str>,
    searcher_matches: &[protocol::FileMatch],
    path_regexps: &[Regex],
) -> Vec<Match> {
    let mut matches = Vec::with_capacity(searcher_matches.len());
    for file_match in searcher_matches {
        let chunk_matches = file_match
            .chunk_matches
            .iter()
            .map(|chunk| convert::ChunkMatch {
                content: chunk.content.clone(),
                content_start: Location {
                    offset: i64::from(chunk.content_start.offset),
                    line: i64::from(chunk.content_start.line),
                    // The baseline forces the content-start column to 0.
                    column: 0,
                },
                ranges: chunk
                    .ranges
                    .iter()
                    .map(|range| Range {
                        start: location(range.start),
                        end: location(range.end),
                    })
                    .collect(),
            })
            .collect();

        let mut path_matches = Vec::new();
        for regexp in path_regexps {
            for found in regexp.find_iter(&file_match.path) {
                path_matches.push(Range {
                    start: Location {
                        offset: found.start() as i64,
                        line: 0,
                        column: rune_count(&file_match.path[..found.start()]),
                    },
                    end: Location {
                        offset: found.end() as i64,
                        line: 0,
                        column: rune_count(&file_match.path[..found.end()]),
                    },
                });
            }
        }

        matches.push(Match::File(convert::FileMatch {
            file: convert::FileRef {
                repo: repo.clone(),
                commit_id: commit.to_owned(),
                path: file_match.path.clone(),
                input_rev: rev.map(str::to_owned),
                // Pass on the detected language; it may be empty.
                precise_language: file_match.language.clone(),
            },
            chunk_matches,
            symbols: Vec::new(),
            path_matches,
            limit_hit: file_match.limit_hit,
            debug: None,
        }));
    }
    matches
}

fn location(location: protocol::Location) -> Location {
    Location {
        offset: i64::from(location.offset),
        line: i64::from(location.line),
        column: i64::from(location.column),
    }
}

fn rune_count(prefix: &str) -> i64 {
    prefix.chars().count() as i64
}

/// `symbolsToMatches` (`symbol_search_job.go:138-171`): partition by
/// `(path, language)`, one file match per group, sorted by `result.Key`
/// (repo name, rev, commit, path).
fn symbols_to_matches(
    symbols: Vec<convert::Symbol>,
    repo: &convert::MinimalRepo,
    commit: &str,
    input_rev: &str,
) -> Vec<Match> {
    let mut by_path: std::collections::BTreeMap<(String, String), Vec<convert::Symbol>> =
        std::collections::BTreeMap::new();
    for symbol in symbols {
        by_path
            .entry((symbol.path.clone(), symbol.language.clone()))
            .or_default()
            .push(symbol);
    }

    let mut matches: Vec<Match> = Vec::with_capacity(by_path.len());
    for ((path, language), symbols) in by_path {
        let file = convert::FileRef {
            repo: repo.clone(),
            commit_id: commit.to_owned(),
            path,
            input_rev: Some(input_rev.to_owned()),
            precise_language: language,
        };
        let symbol_matches = symbols
            .into_iter()
            .map(|symbol| convert::SymbolMatch {
                symbol,
                file: file.clone(),
            })
            .collect();
        matches.push(Match::File(convert::FileMatch {
            file,
            chunk_matches: Vec::new(),
            symbols: symbol_matches,
            path_matches: Vec::new(),
            limit_hit: false,
            debug: None,
        }));
    }

    matches.sort_by_key(match_key);
    matches
}

/// `result.FileMatch.Key()` comparison fields (`match.go:57-117`).
fn match_key(match_: &Match) -> (String, String, String, String) {
    match match_ {
        Match::File(file_match) => (
            file_match.file.repo.name.clone(),
            file_match.file.input_rev.clone().unwrap_or_default(),
            file_match.file.commit_id.clone(),
            file_match.file.path.clone(),
        ),
        _ => (String::new(), String::new(), String::new(), String::new()),
    }
}

/// `search.HandleRepoSearchResult` (`internal/search/repo_status.go:161-188`).
///
/// Returns the per-repo status map and, for fatal errors, the error to promote.
pub fn handle_repo_search_result(
    repo_id: i32,
    rev_specs: &[String],
    limit_hit: bool,
    timed_out: bool,
    search_error: Option<&SearcherError>,
) -> (RepoStatusMap, Option<SearcherError>) {
    let mut status = 0u8;
    let mut fatal = None;

    if limit_hit {
        status |= repo_status::LIMIT_HIT;
    }

    match search_error {
        None => {}
        Some(error) => match error.kind {
            SearcherErrorKind::RepoNotFound { clone_in_progress } => {
                status |= if clone_in_progress {
                    repo_status::CLONING
                } else {
                    repo_status::MISSING
                };
            }
            SearcherErrorKind::RevisionNotFound => {
                // No input revision means the repository is empty; ignore it.
                let empty_rev =
                    rev_specs.is_empty() || (rev_specs.len() == 1 && rev_specs[0].is_empty());
                if !empty_rev {
                    fatal = Some(error.clone());
                }
            }
            SearcherErrorKind::NotFound => status |= repo_status::MISSING,
            SearcherErrorKind::Timeout | SearcherErrorKind::Temporary => {
                status |= repo_status::TIMED_OUT
            }
            SearcherErrorKind::Other => fatal = Some(error.clone()),
        },
    }

    if timed_out {
        status |= repo_status::TIMED_OUT;
    }

    let mut map = RepoStatusMap::default();
    // `repoStatusSingleton`: status 0 produces an empty map.
    if status != 0 {
        map.update(repo_id, status);
    }
    (map, fatal)
}

/// `search.go:60-79`: give a single repo (or `UseFullDeadline`) the remaining
/// deadline, otherwise 500ms so one slow repo cannot delay the rest.
fn fetch_timeout(
    repo_count: usize,
    use_full_deadline: bool,
    deadline: Option<Instant>,
) -> Duration {
    if repo_count == 1 || use_full_deadline {
        match deadline {
            Some(deadline) => deadline.saturating_duration_since(Instant::now()),
            // In practice a deadline should always be set.
            None => Duration::from_secs(60),
        }
    } else {
        Duration::from_millis(500)
    }
}

fn context_done(ctx: &JobContext) -> bool {
    ctx.deadline
        .is_some_and(|deadline| Instant::now() >= deadline)
}

fn job_error(error: SearcherError) -> JobError {
    JobError(error.message)
}

/// Normalize a Go `attribute.StringSlice` the way `fmt.Sprintf("%v", []string)`
/// does: `"[a b]"` (and `"[]"` when empty).
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

    #[test]
    fn names_and_attributes_match_the_baseline() {
        let text = TextSearchJob {
            pattern_info: TextPatternInfo::default(),
            repos: Vec::new(),
            path_regexps: Vec::new(),
            indexed: false,
            num_context_lines: 0,
            use_full_deadline: false,
        };
        assert_eq!(text.name(), "SearcherTextSearchJob");
        assert!(text.children().is_empty());
        assert_eq!(
            text.attributes(Verbosity::Basic),
            vec![("indexed".to_owned(), "false".to_owned())]
        );

        let symbol = SymbolSearchJob {
            request: SymbolSearchRequest::default(),
            repos: Vec::new(),
            limit: 10,
        };
        assert_eq!(symbol.name(), "SearcherSymbolSearchJob");
    }

    #[test]
    fn text_pattern_info_display_matches_go() {
        let info = TextPatternInfo {
            query: Some(protocol::QueryNode::Pattern(protocol::PatternNode {
                value: "route".to_owned(),
                is_negated: false,
                is_regexp: false,
                boost: false,
            })),
            file_match_limit: 10,
            pattern_matches_content: true,
            pattern_matches_path: true,
            ..TextPatternInfo::default()
        };
        assert_eq!(
            info.to_string(),
            "TextPatternInfo{\"route\",filematchlimit:10}"
        );
    }

    #[test]
    fn handle_repo_search_result_classifies_errors() {
        let revision =
            SearcherError::new(SearcherErrorKind::RevisionNotFound, "revision not found");
        let (status, fatal) =
            handle_repo_search_result(7, &[String::new()], false, false, Some(&revision));
        assert!(status.is_empty());
        assert!(fatal.is_none());

        let (status, fatal) =
            handle_repo_search_result(7, &["main".to_owned()], false, false, Some(&revision));
        assert!(status.is_empty());
        assert_eq!(fatal, Some(revision));

        let temporary = SearcherError::new(SearcherErrorKind::Temporary, "temporary");
        let (status, fatal) =
            handle_repo_search_result(7, &["main".to_owned()], true, false, Some(&temporary));
        assert_eq!(fatal, None);
        assert!(status.any(repo_status::TIMED_OUT));
        assert!(status.any(repo_status::LIMIT_HIT));
    }

    #[test]
    fn run_without_a_seam_returns_not_ported() {
        struct NoopLister;
        impl crate::search_job::PrivateRepoLister for NoopLister {
            fn list_minimal_repos(
                &self,
                _options: &crate::search_job::ReposListOptions,
            ) -> Result<Vec<sg_zoekt::indexed_repo_revs::MinimalRepo>, JobError> {
                Ok(Vec::new())
            }
        }
        struct NoopStreamer;
        impl crate::search_job::JobStreamer for NoopStreamer {
            fn stream_search(
                &self,
                _query: &sg_zoekt::query::Query,
                _options: &sg_zoekt::types::SearchOptions,
                _sender: &mut dyn FnMut(sg_zoekt::types::SearchResult),
            ) -> Result<(), JobError> {
                Ok(())
            }
        }
        let lister = NoopLister;
        let streamer = NoopStreamer;
        let clients = RuntimeClients {
            db: &lister,
            zoekt: &streamer,
            max_repos: None,
            searcher: None,
            commit: None,
            repo_pager: None,
        };
        let mut text = TextSearchJob {
            pattern_info: TextPatternInfo::default(),
            repos: Vec::new(),
            path_regexps: Vec::new(),
            indexed: false,
            num_context_lines: 0,
            use_full_deadline: false,
        };
        let mut events = Vec::new();
        assert_eq!(
            text.run(&JobContext::default(), &clients, &mut |event| events
                .push(event))
                .unwrap_err()
                .to_string(),
            NOT_PORTED_MESSAGE
        );
        assert!(events.is_empty());
    }

    #[test]
    fn fetch_timeout_uses_the_baseline_buckets() {
        assert_eq!(fetch_timeout(2, false, None), Duration::from_millis(500));
        assert_eq!(fetch_timeout(1, false, None), Duration::from_secs(60));
        assert_eq!(fetch_timeout(3, true, None), Duration::from_secs(60));
    }
}
