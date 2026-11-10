#![forbid(unsafe_code)]

//! `sg-graphql`: schema assembly/validation/execution and query cost
//! estimation. Schema assembly and cost estimation are implemented so far;
//! execution/introspection is not ported yet.

pub const CRATE_NAME: &str = "sg-graphql";

pub mod cost;
pub mod schema;

pub use cost::{estimate_query_cost, CostError, QueryCost, COST_ESTIMATE_VERSION};
pub use schema::{assemble, assemble_oss, Schema, SchemaError};
