#![forbid(unsafe_code)]

//! `sg-graphql`: schema assembly/validation/introspection, query cost
//! estimation and the introspection query executor. Business resolvers are
//! not ported yet.

pub const CRATE_NAME: &str = "sg-graphql";

pub mod cost;
pub mod exec;
pub mod introspection;
mod json;
pub mod schema;

pub use cost::{estimate_query_cost, CostError, QueryCost, COST_ESTIMATE_VERSION};
pub use exec::{ExecError, ExecRequest, ExecResponse};
pub use schema::{assemble, assemble_oss, Schema, SchemaError};
