#![forbid(unsafe_code)]

//! `sg-insights`: Code Insights domain helpers.
//!
//! This crate hosts the parts of `internal/insights` that are independent of
//! the search execution layer:
//!
//! - [`priority`] (`internal/insights/priority`): the priority/scheduling cost
//!   heuristics used by the insight query runner, ported on top of
//!   [`sg_search_query`]'s plan and [`sg_insights_query`]'s query builder.
//! - [`timeseries`] (`internal/insights/timeseries`): sample-interval stepping
//!   (including Go's `AddDate` normalization) and sample-point construction.
//! - [`scheduler`] (`internal/insights/scheduler`): the pure backfill decision
//!   helpers (state enum, error threshold, series query parsing, incomplete
//!   reasons).
//! - [`iterator`] (`internal/insights/scheduler/iterator`): the pure
//!   repository-iterator state machine (peeking, error accounting, retries).
//! - [`types`]: the subset of `internal/insights/types` those modules need.
//! - [`discovery`] (`internal/insights/discovery`): the repository iterators
//!   used by the backfill workers (all repositories with a page cache, query
//!   scope, configured list, and the per-series dispatch).
//!
//! Not ported here: `aggregation`/`compression` (they need search results and
//! gitserver commit indexes), the storage-bound scheduler/iterator methods
//! (`BackfillStore`, `PersistentRepoIterator` persistence) and anything else
//! that touches insight storage; see `docs/modules/06_worker.md`.

pub mod discovery;
pub mod iterator;
pub mod priority;
pub mod scheduler;
pub mod timeseries;
pub mod types;
