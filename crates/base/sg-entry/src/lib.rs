#![forbid(unsafe_code)]

//! `sg-entry`: assembly logic for the two combined Sourcegraph entry points.
//!
//! * [`combined`] — the `cmd/sourcegraph` single-program service order.
//! * [`server`] — the `cmd/server` single-container environment defaults and
//!   Procfile plan.
//! * [`launcher`] — Procfile parsing plus the goreman supervisor semantics.
//! * [`migrator`] — the Go `migrator up -db <schema>` convention.
//!
//! The modules deliberately keep the planning part (environment + Procfile)
//! separate from the execution part so the differential harness can exercise
//! the plan without spawning processes or listening on ports.

pub mod combined;
pub mod env;
pub mod launcher;
pub mod migrator;
pub mod server;

pub use env::EnvMap;
