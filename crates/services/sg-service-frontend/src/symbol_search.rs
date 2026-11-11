//! `internal/search/symbol.ZoektSymbolsClient`: the indexed-symbols path used
//! by the GraphQL tree-entry symbol resolvers.
//!
//! `Compute` first asks zoekt (through [`ZoektSymbolSearcher`]) whether the
//! repository is indexed with symbols at the requested commit
//! (`indexedSymbolsBranch`). If so it runs a zoekt symbol query
//! (`searchZoekt`: limit + 1 budgets, three-second wall time) and filters the
//! results through the sub-repo permission seam. Otherwise it falls back to the
//! symbols service with the baseline 5s server / 10s client timeouts and the
//! exact unresponsive error text.
//!
//! The GraphQL wiring for these resolvers is out of this slice; the inputs are
//! modelled directly so the differential harness can drive the same code the
//! eventual resolver will call. See
//! `docs/overviews/m9_8iq_symbols_client_overview.md`.

use std::sync::Arc;
use std::time::{Duration, Instant};

use sg_zoekt::go_regexp::{self, Literal};
use sg_zoekt::query::Query;
use sg_zoekt::streamer::{BlockingStreamer, Streamer};
use sg_zoekt::types::{ListOptions, ListResponse, RepoListField, SearchOptions, SearchResult};

use crate::search_searcher_client::{run_to_completion, SymbolsClient, SymbolsParameters};
use crate::search_stream::convert::{self, FileRef, Symbol, SymbolMatch};
use crate::symbols_client::SubRepoPermsChecker;

/// `DefaultSymbolLimit`.
pub const DEFAULT_SYMBOL_LIMIT: i32 = 100;

/// `serverTimeout` in the symbols-service fallback.
pub const SYMBOLS_SERVER_TIMEOUT: Duration = Duration::from_secs(5);

/// `clientTimeout` (`2 * serverTimeout`) around the fallback call.
pub const SYMBOLS_CLIENT_TIMEOUT: Duration = Duration::from_secs(10);

/// The exact baseline error when the symbols service does not answer in time
/// (`symbol.go:66`).
pub const SYMBOLS_UNRESPONSIVE_MESSAGE: &str =
    "The symbols service appears unresponsive, check the logs for errors.";

/// `search.SymbolsParameters` inputs of
/// `ZoektSymbolsClient.Compute` (`symbol.go:45`).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SymbolSearchParams {
    pub repo: convert::MinimalRepo,
    pub commit_id: String,
    /// `inputRev`: `None` models the Go nil pointer.
    pub input_rev: Option<String>,
    /// `query`: `None` models the Go nil pointer (the client uses `""`).
    pub query: Option<String>,
    /// `first`: `None` models the Go nil pointer (default 100).
    pub first: Option<i32>,
    /// `includePatterns`: `None` models the Go nil pointer.
    pub include_patterns: Option<Vec<String>>,
    /// Parent context deadline. `None` means no parent deadline; the service
    /// fallback still applies its own 10s client timeout.
    pub deadline: Option<Instant>,
}

/// `zoekt.Searcher` calls the symbol path makes.
pub trait ZoektSymbolSearcher: Send + Sync {
    /// `search.ListAllIndexed`: `List(Const(true), ReposMap)`.
    fn list_all_indexed(&self) -> Result<ListResponse, String>;

    /// `zoekt.Searcher.Search` (the searcher transport is streaming; the
    /// adapter aggregates chunks into one result).
    fn search(&self, query: &Query, options: &SearchOptions) -> Result<SearchResult, String>;
}

/// [`ZoektSymbolSearcher`] over the async [`Streamer`] transport.
pub struct BlockingZoektSymbolSearcher<S> {
    inner: BlockingStreamer<S>,
}

impl<S> BlockingZoektSymbolSearcher<S> {
    pub fn new(inner: S) -> Self {
        Self {
            inner: BlockingStreamer::new(inner),
        }
    }

    pub fn inner(&self) -> &S {
        self.inner.inner()
    }
}

impl<S: Streamer> ZoektSymbolSearcher for BlockingZoektSymbolSearcher<S> {
    fn list_all_indexed(&self) -> Result<ListResponse, String> {
        run_to_completion(self.inner.inner().list(
            &Query::Const(true),
            Some(ListOptions {
                field: RepoListField::ReposMap,
            }),
        ))
        .map_err(|error| error.to_string())
    }

    fn search(&self, query: &Query, options: &SearchOptions) -> Result<SearchResult, String> {
        let mut result = SearchResult::default();
        self.inner
            .stream_search_blocking(query, options, &mut |chunk| {
                result.files.extend(chunk.files);
                result.stats.add(&chunk.stats);
                result.progress = chunk.progress;
            })
            .map_err(|error| error.to_string())?;
        Ok(result)
    }
}

/// `symbol.ZoektSymbolsClient` (`symbol.go:39-114`).
pub struct ZoektSymbolsClient {
    /// `zoektStreamer`; `None` models an unconfigured zoekt.
    pub zoekt: Option<Arc<dyn ZoektSymbolSearcher>>,
    /// `symbols.DefaultClient`.
    pub symbols: Arc<dyn SymbolsClient + Send + Sync>,
    /// `subRepoPermsChecker`.
    pub sub_repo_perms: Arc<dyn SubRepoPermsChecker>,
}

impl ZoektSymbolsClient {
    /// `Compute`.
    pub fn compute(&self, params: &SymbolSearchParams) -> Result<Vec<SymbolMatch>, String> {
        if let Some(branch) = self.indexed_symbols_branch(params) {
            let mut results = self
                .search_zoekt(params, &branch)
                .map_err(|error| format!("zoekt symbol search: {error}"))?;
            results = self
                .filter_zoekt_results(&results)
                .map_err(|error| format!("checking permissions: {error}"))?;
            return Ok(results);
        }

        let client_deadline = Instant::now() + SYMBOLS_CLIENT_TIMEOUT;
        let effective_deadline = match params.deadline {
            Some(deadline) => deadline.min(client_deadline),
            None => client_deadline,
        };

        let include_patterns = params.include_patterns.clone().unwrap_or_default();
        let symbols_params = SymbolsParameters {
            repo: params.repo.name.clone(),
            commit_id: params.commit_id.clone(),
            query: params.query.clone().unwrap_or_default(),
            is_regexp: false,
            is_case_sensitive: false,
            include_patterns,
            exclude_pattern: String::new(),
            include_langs: Vec::new(),
            exclude_langs: Vec::new(),
            // Add 1 so callers can determine `PageInfo.hasNextPage`.
            first: limit_or_default(params.first) + 1,
            timeout: SYMBOLS_SERVER_TIMEOUT,
            deadline: Some(effective_deadline),
        };

        // Go's deferred function replaces the error when the context expired
        // and no results came back. A response error or a local timeout is
        // observed as an empty result set (the trait returns no partials).
        let response = match self.symbols.search(&symbols_params) {
            Ok(response) => response,
            Err(error) => {
                if Instant::now() >= effective_deadline {
                    return Err(SYMBOLS_UNRESPONSIVE_MESSAGE.to_owned());
                }
                return Err(error.message);
            }
        };

        let mut matches = Vec::with_capacity(response.symbols.len());
        for mut symbol in response.symbols {
            // Callers expect 1-indexed lines.
            symbol.line += 1;
            let file = FileRef {
                repo: params.repo.clone(),
                commit_id: params.commit_id.clone(),
                path: symbol.path.clone(),
                input_rev: params.input_rev.clone(),
                precise_language: symbol.language.clone(),
            };
            matches.push(SymbolMatch { symbol, file });
        }

        if matches.is_empty() && Instant::now() >= effective_deadline {
            return Err(SYMBOLS_UNRESPONSIVE_MESSAGE.to_owned());
        }
        // `LimitHit` is ignored by the baseline for this path.
        Ok(matches)
    }

    /// `GetMatchAtLineCharacter` (`symbol.go:116-137`).
    pub fn get_match_at_line_character(
        &self,
        params: &SymbolSearchParams,
        file_path: &str,
        line: i32,
        character: i32,
    ) -> Result<Option<SymbolMatch>, String> {
        let first = Some(999_999);
        let query = Some(String::new());
        let include_patterns = Some(vec![convert::quote_meta(file_path)]);
        let symbol_params = SymbolSearchParams {
            first,
            query,
            include_patterns,
            ..params.clone()
        };
        let matches = self.compute(&symbol_params)?;

        let mut best: Option<SymbolMatch> = None;
        for symbol_match in matches {
            let symbol_range = symbol_range(&symbol_match.symbol);
            let within = line >= symbol_range.0
                && character >= symbol_range.1
                && line <= symbol_range.2
                && character <= symbol_range.3;
            if within
                && best.as_ref().is_none_or(|current| {
                    symbol_match.symbol.name.len() < current.symbol.name.len()
                })
            {
                best = Some(symbol_match);
            }
        }
        Ok(best)
    }

    /// `indexedSymbolsBranch`: branch name when zoekt indexed symbols for the
    /// commit, otherwise `None` (the Go empty string).
    fn indexed_symbols_branch(&self, params: &SymbolSearchParams) -> Option<String> {
        // The baseline caps this at 1s. The synchronous seam has no context to
        // cancel; the erratum records the missing timer.
        let zoekt = self.zoekt.as_ref()?;
        let list = zoekt.list_all_indexed().ok()?;
        let entry = list.repos_map.get(&(params.repo.id as u32))?;
        if !entry.has_symbols {
            return None;
        }
        entry
            .branches
            .iter()
            .find(|branch| branch.version == params.commit_id)
            .map(|branch| branch.name.clone())
    }

    /// `searchZoekt` (`symbol.go:184-288`).
    fn search_zoekt(
        &self,
        params: &SymbolSearchParams,
        branch: &str,
    ) -> Result<Vec<SymbolMatch>, String> {
        let zoekt = self
            .zoekt
            .as_ref()
            .ok_or_else(|| "zoekt is not configured".to_owned())?;

        let raw = match &params.query {
            Some(query) if !query.is_empty() => query.clone(),
            _ => ".*".to_owned(),
        };

        let expression = match go_regexp::parse(&raw) {
            Ok(Literal::Empty) => Query::Regexp {
                regexp: String::new(),
                file_name: false,
                content: true,
                case_sensitive: false,
            },
            Ok(Literal::Literal { text, .. }) => Query::Substring {
                pattern: text,
                case_sensitive: false,
                file_name: false,
                content: true,
            },
            Ok(Literal::NonLiteral) => Query::Regexp {
                regexp: raw.clone(),
                file_name: false,
                content: true,
                case_sensitive: false,
            },
            Err(_) => return Err(invalid_regexp_message(&raw)),
        };

        let mut ands = vec![
            Query::single_branches_repos(branch, &[params.repo.id as u32]),
            Query::Symbol(Box::new(expression)),
        ];
        if let Some(include_patterns) = &params.include_patterns {
            for pattern in include_patterns {
                ands.push(file_re(pattern)?);
            }
        }
        let final_query = Query::and(ands).simplify();

        let match_count = i64::from(limit_or_default(params.first)) + 1;
        let options = SearchOptions {
            estimate_doc_count: false,
            whole: false,
            shard_max_match_count: match_count * 25,
            total_max_match_count: match_count * 25,
            shard_repo_max_match_count: 0,
            max_wall_time: Duration::from_secs(3),
            flush_wall_time: Duration::ZERO,
            max_doc_display_count: match_count,
            max_match_display_count: 0,
            num_context_lines: 0,
            chunk_matches: true,
            use_document_ranks: false,
            document_ranks_weight: 0.0,
            // `policy.ShouldTrace(ctx)` is false for every harness case; the
            // port has no trace policy context yet (see the erratum).
            trace: false,
            debug_score: false,
            use_bm25_scoring: false,
        };

        let response = zoekt.search(&final_query, &options)?;
        let mut results = Vec::new();
        for file in response.files {
            let path = String::from_utf8_lossy(&file.file_name).into_owned();
            let file_ref = FileRef {
                repo: params.repo.clone(),
                commit_id: params.commit_id.clone(),
                path: path.clone(),
                input_rev: params.input_rev.clone(),
                precise_language: file.language.clone(),
            };
            for chunk in &file.chunk_matches {
                if chunk.file_name || chunk.symbol_info.is_empty() {
                    continue;
                }
                for (index, range) in chunk.ranges.iter().enumerate() {
                    let Some(symbol_info) = chunk.symbol_info.get(index).and_then(Option::as_ref)
                    else {
                        continue;
                    };
                    results.push(SymbolMatch {
                        symbol: Symbol {
                            name: symbol_info.sym.clone(),
                            path: path.clone(),
                            line: range.start.line_number as i32,
                            character: range.start.column as i32,
                            kind: symbol_info.kind.clone(),
                            language: file.language.clone(),
                            parent: symbol_info.parent.clone(),
                            parent_kind: symbol_info.parent_kind.clone(),
                        },
                        file: file_ref.clone(),
                    });
                }
            }
        }
        Ok(results)
    }

    /// `filterZoektResults`.
    fn filter_zoekt_results(&self, results: &[SymbolMatch]) -> Result<Vec<SymbolMatch>, String> {
        if !self.sub_repo_perms.enabled() {
            return Ok(results.to_vec());
        }
        let mut filtered = Vec::with_capacity(results.len());
        for result in results {
            if self
                .sub_repo_perms
                .include(&result.file.repo.name, &result.file.path)?
            {
                filtered.push(result.clone());
            }
        }
        Ok(filtered)
    }
}

/// `limitOrDefault`.
pub fn limit_or_default(first: Option<i32>) -> i32 {
    first.unwrap_or(DEFAULT_SYMBOL_LIMIT)
}

/// `zoektquery.FileRe(pattern, true)`: `ParseRe(pattern, true, false, true)`.
fn file_re(pattern: &str) -> Result<Query, String> {
    match go_regexp::parse(pattern) {
        Ok(Literal::Empty) => Ok(Query::Regexp {
            regexp: String::new(),
            file_name: true,
            content: false,
            case_sensitive: true,
        }),
        Ok(Literal::Literal { text, .. }) => Ok(Query::Substring {
            pattern: text,
            case_sensitive: true,
            file_name: true,
            content: false,
        }),
        Ok(Literal::NonLiteral) => {
            go_regexp::validate(pattern)?;
            Ok(Query::Regexp {
                regexp: pattern.to_owned(),
                file_name: true,
                content: false,
                case_sensitive: true,
            })
        }
        Err(_) => Err(invalid_regexp_message(pattern)),
    }
}

fn invalid_regexp_message(pattern: &str) -> String {
    let detail = go_regexp::validate(pattern)
        .err()
        .unwrap_or_else(|| "invalid regexp syntax".to_owned());
    format!("error parsing regexp: {detail}")
}

/// `Symbol.Range()`: `[start_line, start_character, end_line, end_character]`
/// in 0-indexed lines, with `len(name)` counted in bytes.
fn symbol_range(symbol: &Symbol) -> (i32, i32, i32, i32) {
    let start_line = symbol.line - 1;
    (
        start_line,
        symbol.character,
        start_line,
        symbol.character + symbol.name.len() as i32,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::search_searcher_client::{SearcherError, SymbolsResponse};
    use sg_zoekt::types::{
        ChunkMatch, FileMatch, Location, MinimalRepoListEntry, Range, RepositoryBranch, SymbolInfo,
    };
    use std::collections::BTreeMap;
    use std::sync::Mutex;

    struct FakeZoekt {
        list: Result<ListResponse, String>,
        search: Result<SearchResult, String>,
        seen: Mutex<Option<(Query, SearchOptions)>>,
    }

    impl ZoektSymbolSearcher for FakeZoekt {
        fn list_all_indexed(&self) -> Result<ListResponse, String> {
            self.list.clone()
        }

        fn search(&self, query: &Query, options: &SearchOptions) -> Result<SearchResult, String> {
            *self.seen.lock().expect("lock") = Some((query.clone(), options.clone()));
            self.search.clone()
        }
    }

    struct FakeSymbols {
        response: Result<SymbolsResponse, SearcherError>,
        seen: Mutex<Option<SymbolsParameters>>,
    }

    impl SymbolsClient for FakeSymbols {
        fn search(&self, params: &SymbolsParameters) -> Result<SymbolsResponse, SearcherError> {
            *self.seen.lock().expect("lock") = Some(params.clone());
            self.response.clone()
        }
    }

    fn repo_list(has_symbols: bool, version: &str) -> ListResponse {
        let mut repos_map = BTreeMap::new();
        repos_map.insert(
            7,
            MinimalRepoListEntry {
                has_symbols,
                branches: vec![RepositoryBranch {
                    name: "main".to_owned(),
                    version: version.to_owned(),
                }],
                index_time_unix: 0,
            },
        );
        ListResponse {
            repos_map,
            ..ListResponse::default()
        }
    }

    fn zoekt_result() -> SearchResult {
        let chunk = ChunkMatch {
            content: b"func Foo".to_vec(),
            content_start: Location::default(),
            file_name: false,
            ranges: vec![Range {
                start: Location {
                    byte_offset: 0,
                    line_number: 1,
                    column: 5,
                },
                end: Location::default(),
            }],
            symbol_info: vec![Some(SymbolInfo {
                sym: "Foo".to_owned(),
                kind: "function".to_owned(),
                parent: "".to_owned(),
                parent_kind: "".to_owned(),
            })],
            score: 0.0,
        };
        SearchResult {
            files: vec![FileMatch {
                file_name: b"src/main.go".to_vec(),
                language: "Go".to_owned(),
                chunk_matches: vec![chunk],
                ..FileMatch::default()
            }],
            ..SearchResult::default()
        }
    }

    fn params() -> SymbolSearchParams {
        SymbolSearchParams {
            repo: convert::MinimalRepo {
                id: 7,
                name: "github.com/o/r".to_owned(),
            },
            commit_id: "abc".to_owned(),
            input_rev: Some("main".to_owned()),
            query: Some("Foo".to_owned()),
            first: Some(2),
            include_patterns: Some(vec!["src/".to_owned()]),
            deadline: None,
        }
    }

    #[test]
    fn indexed_branch_runs_the_zoekt_query_with_limit_plus_one() {
        let zoekt = Arc::new(FakeZoekt {
            list: Ok(repo_list(true, "abc")),
            search: Ok(zoekt_result()),
            seen: Mutex::new(None),
        });
        let symbols = Arc::new(FakeSymbols {
            response: Ok(SymbolsResponse::default()),
            seen: Mutex::new(None),
        });
        let client = ZoektSymbolsClient {
            zoekt: Some(zoekt.clone()),
            symbols,
            sub_repo_perms: Arc::new(crate::symbols_client::NoSubRepoPerms),
        };
        let matches = client.compute(&params()).expect("zoekt branch");
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].symbol.name, "Foo");
        assert_eq!(matches[0].symbol.line, 1);
        assert_eq!(matches[0].file.path, "src/main.go");

        let (query, options) = zoekt.seen.lock().expect("lock").clone().expect("search");
        assert!(matches!(query, Query::And(_)));
        assert_eq!(options.max_doc_display_count, 3, "limit + 1");
        assert_eq!(options.shard_max_match_count, 75);
        assert_eq!(options.total_max_match_count, 75);
        assert_eq!(options.max_wall_time, Duration::from_secs(3));
        assert!(options.chunk_matches);
        assert_eq!(options.num_context_lines, 0);
    }

    #[test]
    fn unindexed_symbols_fall_back_to_the_service_with_limit_plus_one() {
        let zoekt = Arc::new(FakeZoekt {
            list: Ok(repo_list(false, "abc")),
            search: Err("must not be called".to_owned()),
            seen: Mutex::new(None),
        });
        let symbols = Arc::new(FakeSymbols {
            response: Ok(SymbolsResponse {
                symbols: vec![Symbol {
                    name: "Bar".to_owned(),
                    path: "a.go".to_owned(),
                    line: 0,
                    character: 2,
                    kind: "function".to_owned(),
                    language: "Go".to_owned(),
                    parent: String::new(),
                    parent_kind: String::new(),
                }],
                limit_hit: true,
                file_limited: vec![false],
            }),
            seen: Mutex::new(None),
        });
        let client = ZoektSymbolsClient {
            zoekt: Some(zoekt),
            symbols: symbols.clone(),
            sub_repo_perms: Arc::new(crate::symbols_client::NoSubRepoPerms),
        };
        let matches = client.compute(&params()).expect("service branch");
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].symbol.line, 1, "1-indexed");
        assert_eq!(matches[0].file.repo.name, "github.com/o/r");

        let params = symbols.seen.lock().expect("lock").clone().expect("call");
        assert_eq!(params.first, 3);
        assert_eq!(params.timeout, SYMBOLS_SERVER_TIMEOUT);
        assert_eq!(params.query, "Foo");
        assert!(!params.is_regexp);
    }

    #[test]
    fn expired_deadline_reports_the_unresponsive_message() {
        struct Failing;
        impl SymbolsClient for Failing {
            fn search(
                &self,
                _params: &SymbolsParameters,
            ) -> Result<SymbolsResponse, SearcherError> {
                Err(SearcherError::new(
                    crate::search_searcher_client::SearcherErrorKind::Timeout,
                    "executing symbols search request: context deadline exceeded",
                ))
            }
        }
        let client = ZoektSymbolsClient {
            zoekt: Some(Arc::new(FakeZoekt {
                list: Ok(ListResponse::default()),
                search: Err("unused".to_owned()),
                seen: Mutex::new(None),
            })),
            symbols: Arc::new(Failing),
            sub_repo_perms: Arc::new(crate::symbols_client::NoSubRepoPerms),
        };
        let mut params = params();
        params.deadline = Some(Instant::now() - Duration::from_secs(1));
        assert_eq!(
            client.compute(&params),
            Err(SYMBOLS_UNRESPONSIVE_MESSAGE.to_owned())
        );
    }

    #[test]
    fn limit_defaults_to_one_hundred() {
        assert_eq!(limit_or_default(None), 100);
        assert_eq!(limit_or_default(Some(1)), 1);
    }

    #[test]
    fn symbol_range_is_zero_indexed_with_byte_name_length() {
        let symbol = Symbol {
            name: "abc".to_owned(),
            path: "p".to_owned(),
            line: 3,
            character: 4,
            kind: String::new(),
            language: String::new(),
            parent: String::new(),
            parent_kind: String::new(),
        };
        assert_eq!(symbol_range(&symbol), (2, 4, 2, 7));
    }
}
