#![forbid(unsafe_code)]

//! `sg-log`: process log initialization.
//!
//! Baseline services emit structured logs through `sourcegraph/log`. The
//! textual layout is a compatibility grade O concern, so this crate installs a
//! tracing subscriber whose filter can be tuned with `RUST_LOG` (default
//! `info`). `SERVICE_NAME` is attached to every record.

use tracing_subscriber::EnvFilter;

/// Install the global subscriber. Safe to call once per process; later calls
/// are ignored.
pub fn init(service: &'static str) {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let _ = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(false)
        .try_init();
    tracing::debug!(service, "logging initialized");
}
