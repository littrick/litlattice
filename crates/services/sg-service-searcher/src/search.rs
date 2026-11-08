#![forbid(unsafe_code)]

//! Search orchestration (`Service.search` in `search.go`).
//!
//! Unindexed regexp search compiles the query, prepares the archive through
//! [`Store`], and runs the worker-pool scan in [`RegexSearch`]. Structural
//! requests narrow the archive with an approximate regexp pass and stream
//! comby results. When a zoekt endpoint and a gitserver diff source are
//! configured, hybrid search ([`crate::hybrid`]) first streams the unchanged
//! part of the repository from zoekt and the archive is narrowed to the
//! changed paths; otherwise the whole archive is searched, which is also the
//! baseline fallback when indexed search is unavailable.

use std::io;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use crate::hybrid::{hybrid, ChangedFilesProvider, HybridOutcome, IndexedSearcher};
use crate::langmatch::to_lang_matcher;
use crate::matchtree::to_match_tree;
use crate::metrics::Metrics;
use crate::pathmatch::to_path_matcher;
use crate::protocol::Request;
use crate::regex_search::{RegexSearch, SearchError};
use crate::sender::LimitedStream;
use crate::store::{Store, StoreError};
use crate::structural_search::{filtered_structural_search, StructuralError};

/// Baseline `Service.search` default when the request carries no timeout.
pub const DEFAULT_FETCH_TIMEOUT: Duration = Duration::from_millis(500);

/// Failure of one search request.
#[derive(Debug)]
pub enum ServiceError {
    /// Baseline `badRequestError`; the gRPC layer maps it to `Unknown`.
    BadRequest(String),
    /// Preparing the archive failed.
    Archive(StoreError),
    /// The archive scan failed.
    Search(SearchError),
    /// Structural search failed (comby or candidate scan).
    Structural(StructuralError),
    /// Hybrid search failed (zoekt or gitserver); the baseline returns the
    /// wrapped error and does not fall back.
    Hybrid(String),
}

impl std::fmt::Display for ServiceError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::BadRequest(message)
            | Self::Archive(StoreError::Message(message))
            | Self::Archive(StoreError::Timeout(message))
            | Self::Archive(StoreError::Canceled(message))
            | Self::Archive(StoreError::Temporary(message)) => f.write_str(message),
            Self::Search(err) => err.fmt(f),
            Self::Structural(err) => err.fmt(f),
            Self::Hybrid(message) => f.write_str(message),
        }
    }
}

impl std::error::Error for ServiceError {}

impl From<StoreError> for ServiceError {
    fn from(err: StoreError) -> Self {
        Self::Archive(err)
    }
}

impl From<SearchError> for ServiceError {
    fn from(err: SearchError) -> Self {
        Self::Search(err)
    }
}

impl From<StructuralError> for ServiceError {
    fn from(err: StructuralError) -> Self {
        Self::Structural(err)
    }
}

/// Baseline `Service`: everything the unindexed search path needs.
pub struct SearchService {
    store: Arc<Store>,
    comby_path: PathBuf,
    indexed: Option<Arc<dyn IndexedSearcher>>,
    changed_files: Option<Arc<dyn ChangedFilesProvider>>,
    max_total_paths_length: usize,
    disable_hybrid_search: bool,
}

impl SearchService {
    pub fn new(store: Arc<Store>) -> Self {
        Self {
            store,
            comby_path: crate::structural_search::default_comby_path(),
            indexed: None,
            changed_files: None,
            max_total_paths_length: 100_000,
            disable_hybrid_search: false,
        }
    }

    /// Test hook: run a different comby executable.
    pub fn with_comby_path(mut self, path: impl Into<PathBuf>) -> Self {
        self.comby_path = path.into();
        self
    }

    /// The zoekt endpoint(s) hybrid search offloads to.
    pub fn with_indexed(mut self, indexed: Arc<dyn IndexedSearcher>) -> Self {
        self.indexed = Some(indexed);
        self
    }

    /// The gitserver diff source hybrid search classifies changed files with.
    pub fn with_changed_files(mut self, changed_files: Arc<dyn ChangedFilesProvider>) -> Self {
        self.changed_files = Some(changed_files);
        self
    }

    /// `MAX_TOTAL_PATHS_LENGTH` (`Config.MaxTotalGitArchivePathsLength`).
    pub fn with_max_total_paths_length(mut self, max_total_paths_length: usize) -> Self {
        self.max_total_paths_length = max_total_paths_length;
        self
    }

    /// `DISABLE_HYBRID_SEARCH`.
    pub fn with_disable_hybrid_search(mut self, disable: bool) -> Self {
        self.disable_hybrid_search = disable;
        self
    }

    pub fn store(&self) -> &Arc<Store> {
        &self.store
    }

    /// Baseline `Service.search`.
    ///
    /// Compiles the query before touching the store so invalid requests never
    /// trigger a fetch, then runs the blocking scan on a dedicated thread.
    pub async fn search(
        &self,
        request: &Request,
        sender: Arc<LimitedStream>,
    ) -> Result<(), ServiceError> {
        let fetch_timeout = if request.fetch_timeout.is_zero() {
            DEFAULT_FETCH_TIMEOUT
        } else {
            request.fetch_timeout
        };

        if request.pattern_info.is_structural {
            // The baseline fetches the archive before validating the query.
            let (path, zip) = self
                .store
                .get_zip_file(&request.repo, &request.commit, &[], fetch_timeout)
                .await?;

            let pattern_info = request.pattern_info.clone();
            let comby_path = self.comby_path.clone();
            let context_lines = request.num_context_lines;
            tokio::task::spawn_blocking(move || {
                filtered_structural_search(
                    &path,
                    &zip,
                    &pattern_info,
                    &comby_path,
                    context_lines,
                    &sender,
                )
            })
            .await
            .map_err(|err| {
                ServiceError::Structural(StructuralError::Comby(format!(
                    "structural search worker panicked: {err}"
                )))
            })??;

            return Ok(());
        }

        let query =
            request.pattern_info.query.as_ref().ok_or_else(|| {
                ServiceError::BadRequest("unknown query node type <nil>".to_owned())
            })?;
        let match_tree = to_match_tree(query, request.pattern_info.is_case_sensitive)
            .map_err(|err| ServiceError::BadRequest(err.to_string()))?;
        let lang_matcher = to_lang_matcher(&request.pattern_info);
        let path_matcher = to_path_matcher(&request.pattern_info)
            .map_err(|err| ServiceError::BadRequest(err.to_string()))?;

        // Hybrid search: stream the unchanged paths from zoekt and search the
        // remaining archive. A failed hybrid search fails the request; an
        // unavailable index falls back to the whole archive.
        let mut paths: Vec<String> = Vec::new();
        if !self.disable_hybrid_search {
            if let (Some(indexed), Some(changed_files)) = (&self.indexed, &self.changed_files) {
                let cancel_token = sender.cancel_token();
                match hybrid(
                    indexed.as_ref(),
                    changed_files.as_ref(),
                    request,
                    self.max_total_paths_length,
                    &sender,
                    Metrics::global(),
                    &cancel_token,
                )
                .await
                {
                    Err(err) => return Err(ServiceError::Hybrid(err.to_string())),
                    Ok(HybridOutcome { ok: false, .. }) => {
                        // Fall back to searching the whole archive.
                    }
                    Ok(HybridOutcome {
                        unsearched,
                        ok: true,
                    }) => {
                        if unsearched.is_empty() {
                            // Indexed search did it all.
                            return Ok(());
                        }
                        paths = unsearched;
                    }
                }
            }
        }

        let (_path, zip) = self
            .store
            .get_zip_file(&request.repo, &request.commit, &paths, fetch_timeout)
            .await?;

        let pattern_matches_content = request.pattern_info.pattern_matches_content;
        let pattern_matches_path = request.pattern_info.pattern_matches_path;
        let context_lines = request.num_context_lines;
        let cancel = sender.cancel_token();

        tokio::task::spawn_blocking(move || {
            let search = RegexSearch {
                match_tree: &match_tree,
                path_matcher: &path_matcher,
                lang_matcher: &lang_matcher,
                zip: &zip,
                pattern_matches_content,
                pattern_matches_path,
                context_lines,
            };
            search.run(&cancel, &sender)
        })
        .await
        .map_err(|err| {
            ServiceError::Search(SearchError::Io(io::Error::other(format!(
                "search worker panicked: {err}"
            ))))
        })??;

        Ok(())
    }
}
