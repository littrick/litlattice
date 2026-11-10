#![forbid(unsafe_code)]

//! `sg-service-frontend`: the Rust `frontend` process.
//!
//! Wave 1 (process skeleton) so far: the external/internal/debug listeners,
//! `INSECURE_DEV`-dependent address defaults, `/healthz` and `/__version`, the
//! `BlackHole` middleware, and the debug `/healthz` + `/ready` endpoints.
//! GraphQL, sessions, UI, the configuration service and enterprise setup are
//! not ported yet; `docs/overviews/m9_8dx_frontend_skeleton_overview.md`
//! records what each missing layer is supposed to do.

pub mod auth;
pub mod blame;
pub mod config;
pub mod configuration_api;
pub mod configuration_server;
pub mod configuration_source;
pub mod configuration_startup;
pub mod debug;
pub mod deploy_defaults;
pub mod external_api;
pub mod goimportpath;
pub mod graphql_api;
pub mod graphql_resolvers;
pub mod health;
pub mod internal_api;
pub mod ipallowlist;
pub mod middleware;
pub mod mux;
pub mod search_stream;
pub mod service;
pub mod service_connections;
pub mod site_config_file;
pub mod tenant;
pub mod trace_headers;
pub mod version;

pub use service::{service_name, FrontendService, SERVICE_NAME};
pub use version::Version;
