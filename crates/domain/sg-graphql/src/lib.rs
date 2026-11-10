#![forbid(unsafe_code)]

//! `sg-graphql`: schema assembly/validation/introspection, query cost
//! estimation, the introspection query executor and Go-compatible JSON
//! serialization. Business resolvers are not ported yet.

pub const CRATE_NAME: &str = "sg-graphql";

pub mod cost;
pub mod exec;
pub mod introspection;
pub mod json;
pub mod schema;

pub use cost::{estimate_query_cost, CostError, QueryCost, COST_ESTIMATE_VERSION};
pub use exec::{ExecError, ExecRequest, ExecResponse};
pub use json::Json;
pub use schema::{assemble, assemble_oss, Schema, SchemaError};
