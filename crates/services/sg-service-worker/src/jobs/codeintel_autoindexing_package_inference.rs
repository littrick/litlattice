#![forbid(unsafe_code)]

//! `internal/codeintel/autoindexing/internal/inference/infer.go` — package to
//! repository/revision inference.
//!
//! M9.45c moved the implementation to
//! [`sg_service_frontend::autoindexing_runtime::package_inference`] so the
//! frontend runtime and the worker schedulers share one recognizer table. This
//! module stays as the worker's historical import path; `jobs/mod.rs` still
//! declares it, so the `super::codeintel_autoindexing_package_inference::…`
//! call sites in the scheduler jobs keep working unchanged.

pub use sg_service_frontend::autoindexing_runtime::package_inference::*;
