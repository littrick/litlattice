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
//! - [`types`]: the subset of `internal/insights/types` those modules need.
//!
//! Not ported here: `aggregation`/`compression` (they need search results and
//! gitserver commit indexes) and anything that touches insight storage; see
//! `docs/modules/06_worker.md`.

pub mod priority;
pub mod timeseries;
pub mod types;
