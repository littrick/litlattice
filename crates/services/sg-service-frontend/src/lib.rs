#![forbid(unsafe_code)]

//! `sg-service-frontend`: the Rust `frontend` process.
//!
//! Wave 1 (process skeleton) so far: the external/internal/debug listeners,
//! `INSECURE_DEV`-dependent address defaults, `/healthz` and `/__version`, the
//! `BlackHole` middleware, and the debug `/healthz` + `/ready` endpoints.
//! GraphQL, sessions, UI, the configuration service and enterprise setup are
//! not ported yet; `docs/overviews/m9_8dx_frontend_skeleton_overview.md`
//! records what each missing layer is supposed to do.

pub mod audit;
pub mod auth;
pub mod blame;
pub mod client_configuration;
pub mod clone_urls;
pub mod config;
pub mod configuration_api;
pub mod configuration_server;
pub mod configuration_source;
pub mod configuration_startup;
pub mod configuration_watch;
pub mod debug;
pub mod deploy_defaults;
pub mod external_api;
pub mod external_services;
pub mod extsvc_config_file;
pub mod feature_flag_mutations;
pub mod feature_flag_store;
pub mod goimportpath;
pub mod graphql_api;
pub mod graphql_metrics;
pub mod graphql_resolvers;
pub mod health;
pub mod internal_api;
pub mod ipallowlist;
pub mod log_events;
pub mod log_events_metrics;
pub mod middleware;
pub mod mux;
pub mod org_store;
pub mod parse_search_query;
pub mod ranking_graphql;
pub mod rate_limit;
pub mod repositories;
pub mod repository_store;
pub mod search_client;
pub mod search_contexts;
pub mod search_contexts_graphql;
pub mod search_contexts_schema;
pub mod search_job;
pub mod search_job_printer;
pub mod search_jobutil_combinators;
pub mod search_jobutil_exhaustive;
pub mod search_jobutil_expression;
pub mod search_jobutil_filters;
pub mod search_jobutil_job_builders;
pub mod search_jobutil_misc_jobs;
pub mod search_jobutil_repos;
pub mod search_jobutil_tree;
pub mod search_result_merger;
pub mod search_source;
pub mod search_stream;
pub mod service;
pub mod service_connections;
pub mod session_auth;
pub mod settings_merge;
pub mod settings_mutation;
pub mod site_config_change;
pub mod site_config_file;
pub mod site_configuration_mutation;
pub mod site_configuration_query;
pub mod site_configuration_secrets;
pub mod temporary_settings;
pub mod tenant;
pub mod trace_headers;
pub mod user_store;
pub mod version;
pub mod webhook_logs;

pub use service::{service_name, FrontendService, SERVICE_NAME};
pub use version::Version;
