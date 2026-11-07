#![forbid(unsafe_code)]

//! `Search` orchestration: 60s deadline, cache lookup/build, SQLite query.
//!
//! Mirrors `api.MakeSqliteSearchFunc`. Indexing runs in the background (up to
//! `PROCESSING_TIMEOUT`), so a request timeout never corrupts a half-built
//! database.

use std::sync::Arc;
use std::time::Duration;

use tokio::time::Instant;

use crate::cache::{Cache, ParseAmount};
use crate::store::{self, SearchParams, Symbol};

/// `api.searchTimeout`.
pub const SEARCH_TIMEOUT: Duration = Duration::from_secs(60);

#[derive(Debug, Clone, Default)]
pub struct Params {
    pub repo: String,
    pub commit: String,
    pub query: String,
    pub is_case_sensitive: bool,
    pub include_patterns: Vec<String>,
    pub exclude_pattern: String,
    pub first: i64,
    pub timeout: Option<Duration>,
    pub include_langs: Vec<String>,
    pub exclude_langs: Vec<String>,
}

#[derive(Debug, Default, PartialEq, Eq)]
pub struct Outcome {
    pub symbols: Vec<Symbol>,
    pub limit_hit: bool,
    /// How the index was obtained, for the `parseAmount` metric label.
    pub parse_amount: ParseAmount,
}

pub struct SearchEngine {
    cache: Arc<Cache>,
}

impl SearchEngine {
    pub fn new(cache: Arc<Cache>) -> Self {
        Self { cache }
    }

    pub async fn search(&self, params: &Params) -> Result<Outcome, String> {
        let timeout = match params.timeout {
            Some(timeout) if !timeout.is_zero() && timeout < SEARCH_TIMEOUT => timeout,
            _ => SEARCH_TIMEOUT,
        };

        let (db_file, parse_amount) = match tokio::time::timeout_at(
            Instant::now() + timeout,
            self.cache.get_or_create(&params.repo, &params.commit),
        )
        .await
        {
            Ok(result) => result?,
            Err(_) => return Err(timeout_message()),
        };

        let store_params = SearchParams {
            query: params.query.clone(),
            is_case_sensitive: params.is_case_sensitive,
            include_patterns: params.include_patterns.clone(),
            exclude_pattern: params.exclude_pattern.clone(),
            include_langs: params.include_langs.clone(),
            exclude_langs: params.exclude_langs.clone(),
            first: params.first,
        };

        let result = tokio::task::spawn_blocking(move || store::search(&db_file, &store_params))
            .await
            .map_err(|err| format!("symbol search task failed: {err}"))??;

        Ok(Outcome {
            symbols: result.symbols,
            limit_hit: result.limit_hit,
            parse_amount,
        })
    }
}

/// The baseline's diagnostic when the frontend database is unavailable: it
/// cannot look up the repository size, so it suggests Rockskip unconditionally.
fn timeout_message() -> String {
    "Processing symbols using the SQLite backend is taking a while. If this repository is ~1GB+, \
     enable [Rockskip](https://sourcegraph.com/docs/code_navigation/explanations/rockskip)."
        .to_owned()
}
