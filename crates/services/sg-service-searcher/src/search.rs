#![forbid(unsafe_code)]

//! Search orchestration (`Service.search` in `search.go`).
//!
//! M8.6 implements the unindexed path: query compilation, archive
//! preparation through [`Store`], and the worker-pool scan in
//! [`RegexSearch`]. Hybrid/zoekt offload and structural search land in M8.7;
//! until then structural requests fail loudly instead of silently running a
//! regexp search.

use std::io;
use std::sync::Arc;
use std::time::Duration;

use crate::langmatch::to_lang_matcher;
use crate::matchtree::to_match_tree;
use crate::pathmatch::to_path_matcher;
use crate::protocol::Request;
use crate::regex_search::{RegexSearch, SearchError};
use crate::sender::LimitedStream;
use crate::store::{Store, StoreError};

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
    /// The request needs a code path scheduled for a later milestone.
    Unsupported(&'static str),
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
            Self::Unsupported(message) => f.write_str(message),
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

/// Baseline `Service`: everything the unindexed search path needs.
pub struct SearchService {
    store: Arc<Store>,
}

impl SearchService {
    pub fn new(store: Arc<Store>) -> Self {
        Self { store }
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
        if request.pattern_info.is_structural {
            return Err(ServiceError::Unsupported(
                "structural search is not implemented yet",
            ));
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

        let fetch_timeout = if request.fetch_timeout.is_zero() {
            DEFAULT_FETCH_TIMEOUT
        } else {
            request.fetch_timeout
        };

        // No hybrid search yet: the whole archive is unsearched.
        let (_path, zip) = self
            .store
            .get_zip_file(&request.repo, &request.commit, &[], fetch_timeout)
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
