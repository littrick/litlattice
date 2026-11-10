#![forbid(unsafe_code)]

//! `sg-graphql`: schema assembly/validation/introspection and query cost
//! estimation. Schema assembly, validation, Relay introspection JSON and cost
//! estimation are implemented so far; query execution and resolvers are not
//! ported yet.

pub const CRATE_NAME: &str = "sg-graphql";

pub mod cost;
pub mod introspection;
pub mod schema;

pub use cost::{estimate_query_cost, CostError, QueryCost, COST_ESTIMATE_VERSION};
pub use schema::{assemble, assemble_oss, Schema, SchemaError};
