#![forbid(unsafe_code)]

//! The `zoekt.Streamer` abstraction the searcher wrappers build on.
//!
//! Mirrors `github.com/sourcegraph/zoekt.Streamer` (`api.go`): a searcher that
//! can both `StreamSearch` and `List`. The futures are required to be `Send`
//! so [`crate::cached::CachedStreamer`] can refresh entries from a spawned
//! task.

use std::future::Future;

use crate::query::Query;
use crate::types::{ListOptions, ListResponse, SearchChunk, SearchOptions};

/// A zoekt searcher transport (`zoekt.Streamer`).
pub trait Streamer: Send + Sync + 'static {
    /// The transport error type. It must be cloneable because the cached
    /// wrapper hands the same cached error to every caller.
    type Error: std::error::Error + Clone + Send + Sync + 'static;

    /// `zoekt.Streamer.List`.
    fn list(
        &self,
        query: &Query,
        opts: Option<ListOptions>,
    ) -> impl Future<Output = Result<ListResponse, Self::Error>> + Send;

    /// `zoekt.Streamer.StreamSearch`.
    fn stream_search(
        &self,
        query: &Query,
        options: &SearchOptions,
        sender: &mut (dyn FnMut(SearchChunk) + Send),
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;
}
