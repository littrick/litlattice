#![forbid(unsafe_code)]

//! `sg-graphql`: schema assembly/validation/execution and query cost
//! estimation. Only cost estimation is implemented so far.

pub const CRATE_NAME: &str = "sg-graphql";

pub mod cost;

pub use cost::{estimate_query_cost, CostError, QueryCost, COST_ESTIMATE_VERSION};
