#![forbid(unsafe_code)]

//! `sg-core`: service lifecycle shared by every Rust Sourcegraph service.
//!
//! Mirrors the baseline `internal/service/svcmain` contract:
//!
//! 1. call `configure` on every service (environment is still mutable);
//! 2. call `validate` on every configuration (fatal `invalid configuration`);
//! 3. lock the environment;
//! 4. handle `help`/`-h`/`--help` (print the registry, exit 0);
//! 5. start every service concurrently, aggregate readiness;
//! 6. wait for all services; a start error is fatal (exit 1).

pub mod error;
pub mod locked_env;
pub mod prof;
pub mod ready;
pub mod run;
pub mod sanity;
pub mod service;

pub use error::{ConfigError, ServiceError};
pub use locked_env::LockedEnv;
pub use prof::prof_http_addr;
pub use ready::ReadyHandle;
pub use run::{run, run_with_options, RunOptions};
pub use sanity::sanity_check;
pub use service::{Service, ServiceContext};

pub use sg_env::{parse_go_duration, EnvReader};
