#![forbid(unsafe_code)]

//! `sg-insights`: Code Insights domain helpers.
//!
//! This crate hosts the parts of `internal/insights` that are independent of
//! the search execution layer. The first module is [`priority`]
//! (`internal/insights/priority`): the priority/scheduling cost heuristics
//! used by the insight query runner, ported on top of [`sg_search_query`]'s
//! plan and [`sg_insights_query`]'s query builder.
//!
//! Not ported here: `aggregation`/`compression`/`timeseries` (they need search
//! results, gitserver commit indexes or scheduler types) and anything that
//! touches insight storage; see `docs/modules/06_worker.md`.

pub mod priority;
