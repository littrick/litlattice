//! Searcher (`index:no`) backend seam (`internal/search/searcher/client.go`)
//! and its collaborators.
//!
//! The request/response types are **not** duplicated here: this module reuses
//! [`sg_service_searcher::protocol`] verbatim. The baseline `searcher.Search`
//! couples three concerns that this seam keeps separate so tests can inject
//! fakes:
//!
//! * [`SearcherClient`] — `searcherURLs.Endpoints()` plus `Search`'s archive
//!   fetch / consistent-hash endpoint routing / gRPC streaming;
//! * [`SearcherLimiter`] — the global mutable text-search limiter
//!   (`textSearchLimiter`);
//! * [`SearcherGitserver`] — `gitserver.Client.ResolveRevision` (and
//!   `GetDefaultBranch`) used to turn a revspec into a commit id;
//! * [`SymbolsClient`] — `symbols.Client.Search` used by the symbol job.
//!
//! A default [`NotPortedSearcher`] implementation returns an explicit
//! not-ported error; the real gRPC transport is not wired in this slice.
//!
//! See `docs/overviews/m9_8hx_searcher_jobs_overview.md`.

use std::fmt;

pub use sg_service_searcher::protocol;

use crate::search_stream::convert;

/// Message used when the searcher backend has not been wired yet.
pub const NOT_PORTED_MESSAGE: &str = "searcher backend is not ported yet";

/// Classification of an error crossing the searcher seam. Mirrors the checks
/// `search.HandleRepoSearchResult` performs (`internal/search/repo_status.go`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SearcherErrorKind {
    /// Any other error (fatal for the repo search).
    #[default]
    Other,
    /// `gitdomain.RepoNotExistError`.
    RepoNotFound {
        /// `RepoNotExistError.CloneInProgress`.
        clone_in_progress: bool,
    },
    /// `gitdomain.RevisionNotFoundError`.
    RevisionNotFound,
    /// `errcode.IsNotFound` (but not a missing repo).
    NotFound,
    /// `errcode.IsTimeout`.
    Timeout,
    /// `errcode.IsTemporary`.
    Temporary,
}

/// Error returned by the searcher seam. `message` is the baseline error text;
/// `kind` carries the classification `HandleRepoSearchResult` needs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SearcherError {
    pub kind: SearcherErrorKind,
    pub message: String,
}

impl SearcherError {
    pub fn new(kind: SearcherErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
        }
    }

    /// A plain error (fatal for the repo search).
    pub fn other(message: impl Into<String>) -> Self {
        Self::new(SearcherErrorKind::Other, message)
    }

    /// The searcher backend is not wired yet.
    pub fn not_ported() -> Self {
        Self::other(NOT_PORTED_MESSAGE)
    }
}

impl fmt::Display for SearcherError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for SearcherError {}

/// `searcherURLs` + `searcher.Search`
/// (`internal/search/searcher/client.go:29-141`).
pub trait SearcherClient {
    /// `searcherURLs.Endpoints()`. `TextSearchJob.Run` uses the length to set
    /// the limiter and propagates the error unchanged.
    fn endpoints(&self) -> Result<Vec<String>, SearcherError>;

    /// `searcher.Search`: send `request` to a searcher endpoint, invoke
    /// `on_match` for every streamed `FileMatch`, and return `Done.LimitHit`.
    fn search(
        &self,
        request: &protocol::Request,
        on_match: &mut dyn FnMut(protocol::FileMatch),
    ) -> Result<bool, SearcherError>;
}

/// The global `textSearchLimiter` (`internal/limiter.MutableLimiter`).
pub trait SearcherLimiter {
    /// `MutableLimiter.SetLimit`: `len(endpoints) * 32` in the baseline.
    fn set_limit(&self, limit: usize);

    /// `MutableLimiter.Acquire`: acquire a search slot.
    fn acquire(&self) -> Result<(), SearcherError>;
}

/// The `gitserver.Client` methods the searcher jobs call.
///
/// This maps onto [`sg_git::GitserverClient`]: `resolve_revision` is
/// `ResolveRevision(..., EnsureRevision: false)` and `default_branch` is
/// `GetDefaultBranch(repo, false)` (full ref name). As in gitserver, an empty
/// revspec resolves to `HEAD`, i.e. the repository's default branch.
pub trait SearcherGitserver {
    /// `gitserver.Client.ResolveRevision`.
    fn resolve_revision(&self, repo: &str, revspec: &str) -> Result<String, SearcherError>;

    /// `gitserver.Client.GetDefaultBranch(repo, false)` (full ref name).
    fn default_branch(&self, repo: &str) -> Result<String, SearcherError>;
}

/// `search.SymbolsParameters` (`internal/search/types.go:101-141`).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct SymbolsParameters {
    pub repo: String,
    pub commit_id: String,
    pub query: String,
    pub is_regexp: bool,
    pub is_case_sensitive: bool,
    pub include_patterns: Vec<String>,
    pub exclude_pattern: String,
    pub include_langs: Vec<String>,
    pub exclude_langs: Vec<String>,
    /// `First`: the symbol job passes `limit + 1`.
    pub first: i32,
}

/// `search.SymbolsResponse` reduced to the fields the job consumes.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct SymbolsResponse {
    pub symbols: Vec<convert::Symbol>,
    pub limit_hit: bool,
}

/// `symbols.Client.Search` (`internal/symbols/client.go:58-104`).
pub trait SymbolsClient {
    fn search(&self, params: &SymbolsParameters) -> Result<SymbolsResponse, SearcherError>;
}

/// The injected searcher collaborators. `SearchJob` runners reach this through
/// [`crate::search_job::RuntimeClients::searcher`].
pub struct SearcherClients<'a> {
    pub client: &'a dyn SearcherClient,
    pub limiter: &'a dyn SearcherLimiter,
    pub gitserver: &'a dyn SearcherGitserver,
    pub symbols: &'a dyn SymbolsClient,
}

/// Default implementations used when no real client is wired: every method
/// reports [`NOT_PORTED_MESSAGE`].
#[derive(Debug, Clone, Copy, Default)]
pub struct NotPortedSearcher;

impl SearcherClient for NotPortedSearcher {
    fn endpoints(&self) -> Result<Vec<String>, SearcherError> {
        Err(SearcherError::not_ported())
    }

    fn search(
        &self,
        _request: &protocol::Request,
        _on_match: &mut dyn FnMut(protocol::FileMatch),
    ) -> Result<bool, SearcherError> {
        Err(SearcherError::not_ported())
    }
}

impl SearcherLimiter for NotPortedSearcher {
    fn set_limit(&self, _limit: usize) {}

    fn acquire(&self) -> Result<(), SearcherError> {
        Err(SearcherError::not_ported())
    }
}

impl SearcherGitserver for NotPortedSearcher {
    fn resolve_revision(&self, _repo: &str, _revspec: &str) -> Result<String, SearcherError> {
        Err(SearcherError::not_ported())
    }

    fn default_branch(&self, _repo: &str) -> Result<String, SearcherError> {
        Err(SearcherError::not_ported())
    }
}

impl SymbolsClient for NotPortedSearcher {
    fn search(&self, _params: &SymbolsParameters) -> Result<SymbolsResponse, SearcherError> {
        Err(SearcherError::not_ported())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn not_ported_reports_the_explicit_error() {
        let searcher = NotPortedSearcher;
        assert_eq!(searcher.endpoints(), Err(SearcherError::not_ported()));
        assert_eq!(
            searcher.resolve_revision("repo", "HEAD"),
            Err(SearcherError::not_ported())
        );
        assert_eq!(
            searcher.default_branch("repo"),
            Err(SearcherError::not_ported())
        );
        let request = protocol::Request {
            repo: "repo".to_owned(),
            repo_id: 7,
            commit: "abc".to_owned(),
            branch: "HEAD".to_owned(),
            pattern_info: protocol::PatternInfo::default(),
            fetch_timeout: std::time::Duration::from_secs(1),
            indexed: false,
            num_context_lines: 0,
        };
        assert_eq!(
            SearcherClient::search(&searcher, &request, &mut |_| {}),
            Err(SearcherError::not_ported())
        );
        assert_eq!(searcher.acquire(), Err(SearcherError::not_ported()));
        assert_eq!(
            SymbolsClient::search(&searcher, &SymbolsParameters::default()),
            Err(SearcherError::not_ported())
        );
        assert_eq!(SearcherError::not_ported().to_string(), NOT_PORTED_MESSAGE);
    }
}
