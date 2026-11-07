#![forbid(unsafe_code)]

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use tokio::sync::Notify;

/// Idempotent readiness signal. Mirrors `sync.OnceFunc` in the baseline:
/// marking ready more than once is harmless, and the framework marks a service
/// ready when its `start` future returns even if the service never did.
#[derive(Debug, Clone, Default)]
pub struct ReadyHandle {
    inner: Arc<ReadyInner>,
}

#[derive(Debug, Default)]
struct ReadyInner {
    ready: AtomicBool,
    notify: Notify,
}

impl ReadyHandle {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn mark_ready(&self) {
        if !self.inner.ready.swap(true, Ordering::Release) {
            self.inner.notify.notify_waiters();
        }
    }

    pub fn is_ready(&self) -> bool {
        self.inner.ready.load(Ordering::Acquire)
    }

    /// Resolves as soon as the handle is (or becomes) ready.
    pub async fn wait_ready(&self) {
        loop {
            if self.is_ready() {
                return;
            }
            // Register before re-checking to avoid a lost notification.
            let notified = self.inner.notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.is_ready() {
                return;
            }
            notified.await;
        }
    }
}
