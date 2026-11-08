//! Startup wiring for the frontend configuration service.
//!
//! The Go baseline calls `conf.Get()` while starting (`shared.Main` →
//! `getDB`), which blocks until the frontend has sent the initial
//! configuration: a gitserver that cannot reach the frontend never finishes
//! starting. This module reproduces that behaviour against
//! [`sg_config::ConfigClient`], with the Rust service's shutdown token as the
//! only escape hatch.

use std::sync::Arc;
use std::time::Duration;

use sg_config::{ConfigClient, Unified};
use tokio_util::sync::CancellationToken;

/// How long to wait between failed `GetConfig` attempts. The Go client uses an
/// exponential backoff capped at a minute (`frontendclient`); a fixed second
/// keeps startup fast in development and is never hit in a healthy cluster.
const RETRY_INTERVAL: Duration = Duration::from_secs(1);

/// Block until the first configuration arrives, or the process is shutting
/// down.
///
/// `None` means shutdown was requested while waiting; the caller should treat
/// that as an ordinary termination rather than a startup failure (the baseline
/// would still be blocked in `conf.Get`).
pub async fn wait_for_initial_config(
    client: &ConfigClient,
    shutdown: &CancellationToken,
) -> Option<Arc<Unified>> {
    loop {
        if shutdown.is_cancelled() {
            return None;
        }
        match client.refresh().await {
            Ok(unified) => return Some(unified),
            Err(err) => {
                tracing::warn!(
                    addr = client.addr(),
                    error = %err,
                    "waiting for the initial frontend configuration"
                );
            }
        }
        tokio::select! {
            _ = shutdown.cancelled() => return None,
            _ = tokio::time::sleep(RETRY_INTERVAL) => {}
        }
    }
}
