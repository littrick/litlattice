#![forbid(unsafe_code)]

//! Hybrid search (`hybrid.go`): offload the unchanged part of a repository to
//! zoekt and search only the changed files from the archive.
//!
//! The retry structure is the baseline's: ask zoekt which commit it indexed,
//! diff that commit against the requested one, search the indexed files while
//! ignoring changed/deleted paths, and verify that the commit searched matches
//! the one listed. Up to five attempts are made because the index may move
//! between the List and the Search.

use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use tokio_util::sync::CancellationToken;

use sg_git::{ChangeStatus, ChangedFile};
use sg_zoekt::client::{Client as ZoektClient, Error as ZoektError};
use sg_zoekt::query::Query;
use sg_zoekt::types::{ListOptions, ListResponse, RepoListField, SearchChunk, SearchOptions};

use crate::matchtree::to_match_tree;
use crate::metrics::Metrics;
use crate::protocol::{self, PatternInfo, Request};
use crate::sender::LimitedStream;

/// Baseline `for try := range 5`.
pub const MAX_TRIES: usize = 5;

/// The operations hybrid search needs from a zoekt endpoint
/// (`zoekt.Streamer`).
#[async_trait]
pub trait IndexedSearcher: Send + Sync {
    async fn list(&self, query: Query, options: ListOptions) -> Result<ListResponse, ZoektError>;

    /// Stream one search, invoking `on_chunk` for every response chunk.
    async fn stream_search(
        &self,
        query: Query,
        options: SearchOptions,
        cancel: CancellationToken,
        on_chunk: &mut (dyn FnMut(SearchChunk) + Send),
    ) -> Result<(), ZoektError>;
}

#[async_trait]
impl IndexedSearcher for ZoektClient {
    async fn list(&self, query: Query, options: ListOptions) -> Result<ListResponse, ZoektError> {
        ZoektClient::list(self, &query, options).await
    }

    async fn stream_search(
        &self,
        query: Query,
        options: SearchOptions,
        cancel: CancellationToken,
        on_chunk: &mut (dyn FnMut(SearchChunk) + Send),
    ) -> Result<(), ZoektError> {
        ZoektClient::stream_search(self, &query, &options, &cancel, on_chunk).await
    }
}

/// A set of zoekt replicas.
///
/// The baseline's `HorizontalSearcher` queries every replica concurrently.
/// Replicas are queried sequentially here, which returns the same matches
/// (only latency differs); see `docs/modules/04_searcher.md` for the
/// divergence.
pub struct ZoektPool {
    clients: Vec<ZoektClient>,
}

impl ZoektPool {
    pub fn new(addrs: &[String]) -> Result<Self, ZoektError> {
        let clients = addrs
            .iter()
            .map(|addr| ZoektClient::connect_lazy(addr))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self { clients })
    }

    pub fn is_empty(&self) -> bool {
        self.clients.is_empty()
    }
}

#[async_trait]
impl IndexedSearcher for ZoektPool {
    async fn list(&self, query: Query, options: ListOptions) -> Result<ListResponse, ZoektError> {
        let mut merged = ListResponse::default();
        for client in &self.clients {
            let response = client.list(&query, options).await?;
            merged.crashes += response.crashes;
            merged.repos.extend(response.repos);
            for (repo_id, entry) in response.repos_map {
                merged.repos_map.entry(repo_id).or_insert(entry);
            }
        }
        Ok(merged)
    }

    async fn stream_search(
        &self,
        query: Query,
        options: SearchOptions,
        cancel: CancellationToken,
        on_chunk: &mut (dyn FnMut(SearchChunk) + Send),
    ) -> Result<(), ZoektError> {
        for client in &self.clients {
            client
                .stream_search(&query, &options, &cancel, &mut *on_chunk)
                .await?;
        }
        Ok(())
    }
}

/// Source of `git diff indexed..commit` (`Service.GitChangedFiles`).
#[async_trait]
pub trait ChangedFilesProvider: Send + Sync {
    async fn changed_files(
        &self,
        repo: &str,
        base: &str,
        head: &str,
    ) -> Result<Vec<ChangedFile>, sg_git::Error>;
}

#[async_trait]
impl ChangedFilesProvider for sg_git::GitserverClient {
    async fn changed_files(
        &self,
        repo: &str,
        base: &str,
        head: &str,
    ) -> Result<Vec<ChangedFile>, sg_git::Error> {
        sg_git::GitserverClient::changed_files(self, repo, base, head).await
    }
}

/// Failure inside hybrid search. Mirrors the wrapped errors the baseline
/// returns; the caller treats any error as "fall back is not possible" and
/// fails the request (cancellation and deadlines are handled inside
/// [`hybrid`], like the baseline's deferred function).
#[derive(Debug)]
pub enum HybridError {
    Zoekt(ZoektError),
    Git(sg_git::Error),
    Compile(String),
    Internal(String),
}

impl HybridError {
    fn is_deadline_exceeded(&self) -> bool {
        matches!(self, Self::Zoekt(err) if err.is_deadline_exceeded())
    }
}

impl fmt::Display for HybridError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Zoekt(err) => write!(f, "zoekt: {err}"),
            Self::Git(err) => write!(f, "gitserver: {err}"),
            Self::Compile(message) | Self::Internal(message) => f.write_str(message),
        }
    }
}

impl std::error::Error for HybridError {}

impl From<ZoektError> for HybridError {
    fn from(err: ZoektError) -> Self {
        Self::Zoekt(err)
    }
}

impl From<sg_git::Error> for HybridError {
    fn from(err: sg_git::Error) -> Self {
        Self::Git(err)
    }
}

/// `(unsearched, ok)` from the baseline.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct HybridOutcome {
    /// Paths the caller must search in the archive (`unindexedSearch`).
    pub unsearched: Vec<String>,
    /// `false` means the caller must fall back to searching the whole
    /// archive.
    pub ok: bool,
}

/// Baseline `Service.hybrid`.
///
/// Cancellation is handled like the baseline's deferred function: an error
/// while the caller's token is cancelled is swallowed and reported as a
/// successful, empty hybrid search (results already streamed are final). A
/// deadline is reported after recording `search-timeout`.
pub async fn hybrid(
    searcher: &dyn IndexedSearcher,
    changed_files: &dyn ChangedFilesProvider,
    request: &Request,
    max_total_paths_length: usize,
    sender: &Arc<LimitedStream>,
    metrics: &Metrics,
    cancel: &CancellationToken,
) -> Result<HybridOutcome, HybridError> {
    let (mut state, mut result) = run(
        searcher,
        changed_files,
        request,
        max_total_paths_length,
        sender,
        cancel,
    )
    .await;

    // Baseline deferred function. `recordHybridFinalState` only remembers the
    // last state; the metric is incremented once, at the end.
    if let Err(err) = &result {
        if cancel.is_cancelled() {
            // We only cancel requests once limits are hit or the caller went
            // away; whatever was streamed is the final result.
            state = "search-canceled".to_owned();
            result = Ok(HybridOutcome {
                unsearched: Vec::new(),
                ok: true,
            });
        } else if err.is_deadline_exceeded() {
            state = "search-timeout".to_owned();
            tracing::warn!(state, error = %err, "hybrid search failed");
        } else {
            tracing::error!(state, error = %err, "hybrid search failed");
        }
    }
    metrics.hybrid_final_state(&state);
    result
}

async fn run(
    searcher: &dyn IndexedSearcher,
    changed_files: &dyn ChangedFilesProvider,
    request: &Request,
    max_total_paths_length: usize,
    sender: &Arc<LimitedStream>,
    cancel: &CancellationToken,
) -> (String, Result<HybridOutcome, HybridError>) {
    for _try in 0..MAX_TRIES {
        let indexed = match zoekt_indexed_commit(searcher, &request.repo).await {
            Ok(Some(indexed)) => indexed,
            Ok(None) => {
                return (
                    "zoekt-list-missing".to_owned(),
                    Ok(HybridOutcome::default()),
                )
            }
            Err(err) => return ("zoekt-list-error".to_owned(), Err(HybridError::Zoekt(err))),
        };

        let (indexed_ignore, unindexed_search) =
            match diff_indexed_files(changed_files, request, &indexed).await {
                Ok(paths) => paths,
                Err(err) => {
                    // Baseline quirk: the not-found state is recorded first
                    // but unconditionally overwritten by `git-diff-error`
                    // before the deferred metric runs, so the observable
                    // final state is always `git-diff-error`.
                    return ("git-diff-error".to_owned(), Err(err));
                }
            };

        let total_len_indexed_ignore = total_strings_len(&indexed_ignore);
        let total_len_unindexed_search = total_strings_len(&unindexed_search);
        if total_len_indexed_ignore > max_total_paths_length
            || total_len_unindexed_search > max_total_paths_length
        {
            return ("diff-too-large".to_owned(), Ok(HybridOutcome::default()));
        }

        match zoekt_search_ignore_paths(
            searcher,
            request,
            sender,
            &indexed,
            &indexed_ignore,
            cancel,
        )
        .await
        {
            Err(err) => return ("zoekt-search-error".to_owned(), Err(err)),
            Ok(Some(retry_reason)) => {
                metrics_retry(&retry_reason);
                continue;
            }
            Ok(None) => {
                return (
                    "success".to_owned(),
                    Ok(HybridOutcome {
                        unsearched: unindexed_search,
                        ok: true,
                    }),
                )
            }
        }
    }

    ("max-retrys".to_owned(), Ok(HybridOutcome::default()))
}

fn metrics_retry(reason: &str) {
    Metrics::global().hybrid_retry(reason);
}

/// Diff `indexed..commit` and classify paths exactly like the baseline.
async fn diff_indexed_files(
    changed_files: &dyn ChangedFilesProvider,
    request: &Request,
    indexed: &str,
) -> Result<(Vec<String>, Vec<String>), HybridError> {
    let changed = changed_files
        .changed_files(&request.repo, indexed, &request.commit)
        .await?;

    let mut indexed_ignore = Vec::new();
    let mut unindexed_search = Vec::new();
    for file in changed {
        match file.status {
            // No longer appears in `commit`.
            ChangeStatus::Deleted => indexed_ignore.push(file.path),
            // Changed in both `indexed` and `commit`.
            ChangeStatus::Modified => {
                indexed_ignore.push(file.path.clone());
                unindexed_search.push(file.path);
            }
            // Does not exist in `indexed`.
            ChangeStatus::Added => unindexed_search.push(file.path),
            // A type change does not change the contents.
            ChangeStatus::TypeChanged => {}
        }
    }
    indexed_ignore.sort();
    unindexed_search.sort();
    Ok((indexed_ignore, unindexed_search))
}

/// `zoektIndexedCommit`: the version of the first indexed branch of `repo`.
async fn zoekt_indexed_commit(
    searcher: &dyn IndexedSearcher,
    repo: &str,
) -> Result<Option<String>, ZoektError> {
    let query = Query::repo_set([repo.to_owned()]);
    let response = searcher
        .list(
            query,
            ListOptions {
                field: RepoListField::ReposMap,
            },
        )
        .await?;
    Ok(response
        .repos_map
        .values()
        .next()
        .and_then(|entry| entry.branches.first())
        .map(|branch| branch.version.clone()))
}

/// `zoektSearchIgnorePaths`.
///
/// `Ok(Some(reason))` means the index changed while searching and the caller
/// should retry. Matches are streamed into `sender` before returning.
async fn zoekt_search_ignore_paths(
    searcher: &dyn IndexedSearcher,
    request: &Request,
    sender: &Arc<LimitedStream>,
    indexed: &str,
    ignored_paths: &[String],
    cancel: &CancellationToken,
) -> Result<Option<String>, HybridError> {
    let query = Query::and(vec![
        Query::single_branches_repos("HEAD", &[request.repo_id]),
        zoekt_compile(&request.pattern_info)?,
        Query::negate(Query::file_name_set(ignored_paths.iter().cloned())),
    ])
    .simplify();

    let options = SearchOptions::hybrid(request.pattern_info.limit, request.num_context_lines);

    // The baseline derives a child context so a wrong commit cancels only
    // this attempt, not the request.
    let stream_cancel = cancel.child_token();

    let mut wrong_commit = false;
    let mut crashes = 0i64;
    let mut chunks = Vec::new();
    let result = searcher
        .stream_search(query, options, stream_cancel.clone(), &mut |chunk| {
            crashes += chunk.crashes;
            if chunk.files.iter().any(|file| file.version != indexed) {
                // Unexpected commit searched: discard this attempt and retry.
                wrong_commit = true;
                stream_cancel.cancel();
                return;
            }
            chunks.push(chunk);
        })
        .await;

    // Baseline checks `wrongCommit` first: it overrides the (likely
    // cancelled) error.
    if wrong_commit {
        return Ok(Some("index-search-changed".to_owned()));
    }
    if let Err(err) = result {
        return Err(HybridError::Zoekt(err));
    }

    let files: Vec<protocol::FileMatch> = chunks
        .into_iter()
        .flat_map(|chunk| chunk.files)
        .map(protocol_file_match)
        .collect();
    let found_results = !files.is_empty();
    if found_results {
        // `LimitedStream::send` blocks on the gRPC backpressure channel; run
        // it off the runtime so the streaming loop never blocks a worker.
        let sender = Arc::clone(sender);
        let join = tokio::task::spawn_blocking(move || {
            for file in files {
                sender.send(file);
            }
        });
        join.await
            .map_err(|err| HybridError::Internal(format!("sending zoekt matches failed: {err}")))?;
        return Ok(None);
    }

    // The zoekt containing the repo may have been unreachable.
    if crashes > 0 {
        return Ok(Some("index-search-missing".to_owned()));
    }

    // No matches, so we do not know whether the correct commit was searched.
    let new_indexed = match zoekt_indexed_commit(searcher, &request.repo).await {
        Ok(indexed) => indexed,
        Err(err) => {
            return Err(HybridError::Internal(format!(
                "failed to double check indexed commit: {err}"
            )))
        }
    };
    Ok(match new_indexed {
        // Let the retry logic call `zoektIndexedCommit` again.
        None => Some("index-list-missing".to_owned()),
        Some(new_indexed) if new_indexed != indexed => Some("index-list-changed".to_owned()),
        Some(_) => None,
    })
}

/// `zoektCompile`: the match tree as a zoekt query, ANDed with the
/// path/language filters.
fn zoekt_compile(pattern_info: &PatternInfo) -> Result<Query, HybridError> {
    let Some(node) = pattern_info.query.as_ref() else {
        return Err(HybridError::Compile(
            "unknown query node type <nil>".to_owned(),
        ));
    };
    let tree = to_match_tree(node, pattern_info.is_case_sensitive)
        .map_err(|err| HybridError::Compile(err.to_string()))?;

    let mut parts = vec![tree.to_zoekt_query(
        pattern_info.pattern_matches_content,
        pattern_info.pattern_matches_path,
    )];

    for pattern in &pattern_info.include_paths {
        parts.push(Query::Regexp {
            regexp: pattern.clone(),
            file_name: true,
            content: false,
            case_sensitive: pattern_info.path_patterns_are_case_sensitive,
        });
    }
    if !pattern_info.exclude_paths.is_empty() {
        parts.push(Query::negate(Query::Regexp {
            regexp: pattern_info.exclude_paths.clone(),
            file_name: true,
            content: false,
            case_sensitive: pattern_info.path_patterns_are_case_sensitive,
        }));
    }
    for language in &pattern_info.include_langs {
        parts.push(Query::Language(language.clone()));
    }
    for language in &pattern_info.exclude_langs {
        parts.push(Query::negate(Query::Language(language.clone())));
    }

    Ok(Query::and(parts).simplify())
}

/// `zoektChunkMatches` plus the enclosing `protocol.FileMatch`.
fn protocol_file_match(file: sg_zoekt::types::FileMatch) -> protocol::FileMatch {
    protocol::FileMatch {
        path: String::from_utf8_lossy(&file.file_name).into_owned(),
        language: file.language,
        chunk_matches: file
            .chunk_matches
            .into_iter()
            .filter(|chunk| !chunk.file_name)
            .map(|chunk| protocol::ChunkMatch {
                content: String::from_utf8_lossy(&chunk.content).into_owned(),
                content_start: protocol_location(chunk.content_start),
                ranges: chunk
                    .ranges
                    .into_iter()
                    .map(|range| protocol::Range {
                        start: protocol_location(range.start),
                        end: protocol_location(range.end),
                    })
                    .collect(),
            })
            .collect(),
        limit_hit: false,
    }
}

/// zoekt's locations are 1-based for lines and columns; the searcher protocol
/// is 0-based.
fn protocol_location(location: sg_zoekt::types::Location) -> protocol::Location {
    protocol::Location {
        offset: location.byte_offset as i32,
        line: location.line_number as i32 - 1,
        column: location.column as i32 - 1,
    }
}

/// `totalStringsLen`: sum of the byte lengths.
fn total_strings_len(strings: &[String]) -> usize {
    strings.iter().map(String::len).sum()
}

/// Result of a search kicked off through the pool, used by tests.
pub type SharedIndexedSearcher = Arc<dyn IndexedSearcher>;
/// Shared changed-files provider, used by tests.
pub type SharedChangedFiles = Arc<dyn ChangedFilesProvider>;

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use super::*;
    use sg_zoekt::types::{ChunkMatch, FileMatch, Location, Range};

    /// Fake index. `List` pops an override per call and falls back to the
    /// default commit, so retries are deterministic.
    #[derive(Default)]
    struct FakeIndexed {
        default_commit: Option<String>,
        list_overrides: Mutex<std::collections::VecDeque<Option<String>>>,
        list_calls: Mutex<usize>,
        chunks: Mutex<Vec<SearchChunk>>,
        stream_error: Mutex<Option<ZoektError>>,
        seen_queries: Mutex<Vec<Query>>,
    }

    impl FakeIndexed {
        fn with_commit(commit: &str) -> Self {
            Self {
                default_commit: Some(commit.to_owned()),
                ..Self::default()
            }
        }

        fn push_commit(&self, commit: Option<&str>) {
            self.list_overrides
                .lock()
                .expect("overrides")
                .push_back(commit.map(str::to_owned));
        }
    }

    #[async_trait]
    impl IndexedSearcher for FakeIndexed {
        async fn list(
            &self,
            _query: Query,
            _options: ListOptions,
        ) -> Result<ListResponse, ZoektError> {
            let mut calls = self.list_calls.lock().expect("calls");
            let commit = self
                .list_overrides
                .lock()
                .expect("overrides")
                .pop_front()
                .unwrap_or_else(|| self.default_commit.clone());
            *calls += 1;
            let mut response = ListResponse::default();
            if let Some(commit) = commit {
                response.repos_map.insert(
                    42,
                    sg_zoekt::types::MinimalRepoListEntry {
                        has_symbols: false,
                        branches: vec![sg_zoekt::types::RepositoryBranch {
                            name: "HEAD".to_owned(),
                            version: commit,
                        }],
                        index_time_unix: 0,
                    },
                );
            }
            Ok(response)
        }

        async fn stream_search(
            &self,
            query: Query,
            _options: SearchOptions,
            _cancel: CancellationToken,
            on_chunk: &mut (dyn FnMut(SearchChunk) + Send),
        ) -> Result<(), ZoektError> {
            self.seen_queries.lock().expect("queries").push(query);
            let chunks = self.chunks.lock().expect("chunks").clone();
            for chunk in chunks {
                on_chunk(chunk);
            }
            match self.stream_error.lock().expect("error").take() {
                Some(err) => Err(err),
                None => Ok(()),
            }
        }
    }

    #[derive(Default)]
    struct FakeChangedFiles {
        files: Mutex<Vec<ChangedFile>>,
        error: Mutex<Option<sg_git::Error>>,
    }

    #[async_trait]
    impl ChangedFilesProvider for FakeChangedFiles {
        async fn changed_files(
            &self,
            _repo: &str,
            _base: &str,
            _head: &str,
        ) -> Result<Vec<ChangedFile>, sg_git::Error> {
            if let Some(err) = self.error.lock().expect("error").take() {
                return Err(err);
            }
            Ok(self.files.lock().expect("files").clone())
        }
    }

    fn request() -> Request {
        use crate::protocol::{PatternNode, QueryNode};

        Request {
            repo: "test/repo".to_owned(),
            repo_id: 42,
            commit: "0123456789012345678901234567890123456789".to_owned(),
            branch: "HEAD".to_owned(),
            pattern_info: PatternInfo {
                query: Some(QueryNode::Pattern(PatternNode {
                    value: "needle".to_owned(),
                    is_negated: false,
                    is_regexp: false,
                    boost: false,
                })),
                is_case_sensitive: true,
                limit: 100,
                pattern_matches_content: true,
                pattern_matches_path: false,
                ..PatternInfo::default()
            },
            fetch_timeout: std::time::Duration::ZERO,
            indexed: false,
            num_context_lines: 0,
        }
    }

    fn sender() -> Arc<LimitedStream> {
        Arc::new(LimitedStream::new(
            100,
            Box::new(|_file_match| {}),
            CancellationToken::new(),
        ))
    }

    fn chunk(version: &str, path: &str) -> SearchChunk {
        SearchChunk {
            files: vec![FileMatch {
                file_name: path.as_bytes().to_vec(),
                language: "Go".to_owned(),
                version: version.to_owned(),
                chunk_matches: vec![ChunkMatch {
                    content: b"needle\n".to_vec(),
                    content_start: Location {
                        byte_offset: 0,
                        line_number: 1,
                        column: 1,
                    },
                    file_name: false,
                    ranges: vec![Range {
                        start: Location {
                            byte_offset: 0,
                            line_number: 1,
                            column: 1,
                        },
                        end: Location {
                            byte_offset: 6,
                            line_number: 1,
                            column: 7,
                        },
                    }],
                    score: 0.0,
                }],
                ..FileMatch::default()
            }],
            crashes: 0,
        }
    }

    async fn run_hybrid(
        indexed: &dyn IndexedSearcher,
        changed: &dyn ChangedFilesProvider,
    ) -> Result<HybridOutcome, HybridError> {
        let request = request();
        let sender = sender();
        let cancel = CancellationToken::new();
        hybrid(
            indexed,
            changed,
            &request,
            100_000,
            &sender,
            Metrics::global(),
            &cancel,
        )
        .await
    }

    #[tokio::test]
    async fn streams_indexed_matches_and_returns_unsearched_paths() {
        let indexed = FakeIndexed::with_commit("aaaa");
        *indexed.chunks.lock().expect("chunks") = vec![chunk("aaaa", "a.go")];
        let changed = FakeChangedFiles::default();
        *changed.files.lock().expect("files") = vec![
            ChangedFile {
                path: "b.go".to_owned(),
                status: ChangeStatus::Added,
            },
            ChangedFile {
                path: "c.go".to_owned(),
                status: ChangeStatus::Modified,
            },
            ChangedFile {
                path: "d.go".to_owned(),
                status: ChangeStatus::Deleted,
            },
            ChangedFile {
                path: "e.go".to_owned(),
                status: ChangeStatus::TypeChanged,
            },
        ];

        let outcome = run_hybrid(&indexed, &changed).await.expect("hybrid");
        assert!(outcome.ok);
        assert_eq!(outcome.unsearched, vec!["b.go", "c.go"]);

        // The query excludes changed/deleted paths and pins the repo/branch.
        let queries = indexed.seen_queries.lock().expect("queries");
        let Query::And(parts) = &queries[0] else {
            panic!("expected AND, got {:?}", queries[0]);
        };
        assert!(parts
            .iter()
            .any(|part| matches!(part, Query::BranchesRepos(_))));
        let Some(Query::Not(ignored)) = parts.iter().find(|part| matches!(part, Query::Not(_)))
        else {
            panic!("expected NOT of ignored paths");
        };
        let Query::FileNameSet(names) = ignored.as_ref() else {
            panic!("expected file name set");
        };
        assert_eq!(
            names.iter().cloned().collect::<Vec<_>>(),
            vec!["c.go".to_owned(), "d.go".to_owned()]
        );
    }

    #[tokio::test]
    async fn retries_when_the_searched_commit_moved() {
        let indexed = FakeIndexed::with_commit("aaaa");
        *indexed.chunks.lock().expect("chunks") = vec![chunk("bbbb", "a.go")];
        let changed = FakeChangedFiles::default();

        let outcome = run_hybrid(&indexed, &changed).await.expect("hybrid");
        // Every attempt sees the wrong commit; the loop exhausts.
        assert!(!outcome.ok);
        assert_eq!(*indexed.list_calls.lock().expect("calls"), 5);
    }

    #[tokio::test]
    async fn retries_when_the_list_changed_and_no_results_were_found() {
        let indexed = FakeIndexed::with_commit("aaaa");
        indexed.push_commit(Some("aaaa"));
        indexed.push_commit(Some("bbbb"));
        *indexed.chunks.lock().expect("chunks") = Vec::new();
        let changed = FakeChangedFiles::default();

        let outcome = run_hybrid(&indexed, &changed).await.expect("hybrid");
        // First attempt: listed aaaa, double check found bbbb; second attempt
        // finds aaaa again and succeeds with no results.
        assert!(outcome.ok);
        assert_eq!(*indexed.list_calls.lock().expect("calls"), 4);
    }

    #[tokio::test]
    async fn falls_back_when_no_commit_is_indexed() {
        let indexed = FakeIndexed::default();
        let changed = FakeChangedFiles::default();
        let outcome = run_hybrid(&indexed, &changed).await.expect("hybrid");
        assert!(!outcome.ok);
        assert_eq!(*indexed.list_calls.lock().expect("calls"), 1);
    }

    #[tokio::test]
    async fn falls_back_when_the_diff_is_too_large() {
        let indexed = FakeIndexed::with_commit("aaaa");
        let changed = FakeChangedFiles::default();
        *changed.files.lock().expect("files") = vec![ChangedFile {
            path: "x".repeat(100_001),
            status: ChangeStatus::Added,
        }];

        let request = request();
        let sender = sender();
        let cancel = CancellationToken::new();
        let outcome = hybrid(
            &indexed,
            &changed,
            &request,
            100_000,
            &sender,
            Metrics::global(),
            &cancel,
        )
        .await
        .expect("hybrid");
        assert!(!outcome.ok);
        assert!(indexed.seen_queries.lock().expect("queries").is_empty());
    }

    #[tokio::test]
    async fn falls_back_when_crashes_were_reported() {
        let indexed = FakeIndexed::with_commit("aaaa");
        let mut crashed = chunk("aaaa", "a.go");
        crashed.files.clear();
        crashed.crashes = 1;
        *indexed.chunks.lock().expect("chunks") = vec![crashed];
        let changed = FakeChangedFiles::default();

        let outcome = run_hybrid(&indexed, &changed).await.expect("hybrid");
        assert!(!outcome.ok);
        assert_eq!(*indexed.list_calls.lock().expect("calls"), 5);
    }

    #[tokio::test]
    async fn swallows_errors_when_the_request_was_cancelled() {
        let indexed = FakeIndexed::with_commit("aaaa");
        *indexed.stream_error.lock().expect("error") = Some(ZoektError::Canceled);
        let changed = FakeChangedFiles::default();

        let request = request();
        let sender = sender();
        let cancel = CancellationToken::new();
        cancel.cancel();
        let outcome = hybrid(
            &indexed,
            &changed,
            &request,
            100_000,
            &sender,
            Metrics::global(),
            &cancel,
        )
        .await
        .expect("cancellation is swallowed");
        assert!(outcome.ok);
        assert!(outcome.unsearched.is_empty());
    }

    #[test]
    fn compiles_path_and_language_filters() {
        let mut info = request().pattern_info;
        info.include_paths = vec!["\\.go$".to_owned()];
        info.exclude_paths = "vendor/".to_owned();
        info.include_langs = vec!["Go".to_owned()];
        info.exclude_langs = vec!["Markdown".to_owned()];
        info.path_patterns_are_case_sensitive = true;

        let Query::And(parts) = zoekt_compile(&info).expect("compile") else {
            panic!("expected AND");
        };
        assert!(parts.iter().any(|part| matches!(
            part,
            Query::Regexp {
                file_name: true,
                content: false,
                case_sensitive: true,
                ..
            }
        )));
        assert!(parts
            .iter()
            .any(|part| matches!(part, Query::Language(lang) if lang == "Go")));
        assert!(parts.iter().any(|part| matches!(
            part,
            Query::Not(child)
                if matches!(child.as_ref(), Query::Language(lang) if lang == "Markdown")
        )));
        assert_eq!(parts.len(), 5);
    }

    #[test]
    fn converts_zoekt_chunk_matches() {
        let file = FileMatch {
            file_name: b"dir/file.go".to_vec(),
            language: "Go".to_owned(),
            chunk_matches: vec![
                ChunkMatch {
                    content: b"needle".to_vec(),
                    content_start: Location {
                        byte_offset: 10,
                        line_number: 2,
                        column: 1,
                    },
                    file_name: false,
                    ranges: vec![Range {
                        start: Location {
                            byte_offset: 10,
                            line_number: 2,
                            column: 1,
                        },
                        end: Location {
                            byte_offset: 16,
                            line_number: 2,
                            column: 7,
                        },
                    }],
                    score: 0.0,
                },
                ChunkMatch {
                    content: b"dir/file.go".to_vec(),
                    file_name: true,
                    ..ChunkMatch::default()
                },
            ],
            ..FileMatch::default()
        };

        let converted = protocol_file_match(file);
        assert_eq!(converted.path, "dir/file.go");
        assert_eq!(converted.match_count(), 1);
        assert_eq!(converted.chunk_matches.len(), 1);
        assert_eq!(converted.chunk_matches[0].content, "needle");
        assert_eq!(converted.chunk_matches[0].content_start.line, 1);
        assert_eq!(converted.chunk_matches[0].ranges[0].end.column, 6);
    }

    #[test]
    fn boost_wraps_the_query() {
        use crate::protocol::{PatternNode, QueryNode};
        let mut info = request().pattern_info;
        info.query = Some(QueryNode::Pattern(PatternNode {
            value: "needle".to_owned(),
            is_negated: false,
            is_regexp: false,
            boost: true,
        }));
        let query = zoekt_compile(&info).expect("compile");
        let Query::Boost { boost, .. } = query else {
            panic!("expected a boosted query, got {query:?}");
        };
        assert_eq!(boost, sg_zoekt::query::ZOEKT_SCORE_BOOST);
    }
}
