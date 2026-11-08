//! Port of `internal/limiter.MutableLimiter` (the clone/repo-update
//! semaphore, `GITSERVER_MAX_CONCURRENT_CLONES` / site config
//! `gitMaxConcurrentClones`).
//!
//! Semantics kept from the baseline:
//! - `acquire` waits until the number of active permits is below the limit and
//!   gives up when the caller's token is cancelled.
//! - `set_limit` can change the capacity at any time; shrinking cancels the
//!   *oldest* active permits. The baseline hands the holder a cancelable child
//!   context, which the update job does not use; the cancel only rebalances
//!   the semaphore. Here the permit simply stops counting against the limit.
//! - Dropping a permit releases it (idempotent; a permit dropped after a
//!   shrink was already excluded).

use std::collections::VecDeque;
use std::sync::{Arc, Mutex, MutexGuard};

use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

pub struct MutableLimiter {
    state: Mutex<State>,
    notify: Notify,
}

struct State {
    limit: usize,
    next_id: u64,
    /// Active permits, oldest first, with the token cancelled on shrink.
    active: VecDeque<(u64, CancellationToken)>,
}

impl MutableLimiter {
    pub fn new(limit: usize) -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(State {
                limit,
                next_id: 0,
                active: VecDeque::new(),
            }),
            notify: Notify::new(),
        })
    }

    fn state(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(|err| err.into_inner())
    }

    /// Acquire a permit, waiting until capacity is available or the caller is
    /// cancelled. Equivalent to `MutableLimiter.Acquire`; the returned token
    /// corresponds to the baseline's child context (cancelled when the limit
    /// shrinks past this permit).
    pub async fn acquire(
        self: &Arc<Self>,
        cancelled: &CancellationToken,
    ) -> Result<Permit, AcquireError> {
        loop {
            {
                let mut state = self.state();
                if state.active.len() < state.limit {
                    let id = state.next_id;
                    state.next_id += 1;
                    let token = CancellationToken::new();
                    state.active.push_back((id, token.clone()));
                    return Ok(Permit {
                        id,
                        token,
                        limiter: Arc::clone(self),
                    });
                }
            }
            tokio::select! {
                _ = self.notify.notified() => {}
                _ = cancelled.cancelled() => return Err(AcquireError::Cancelled),
            }
        }
    }

    /// Adjust the capacity, cancelling the oldest permits when shrinking.
    pub fn set_limit(&self, limit: usize) {
        let mut state = self.state();
        state.limit = limit;
        while state.active.len() > limit {
            if let Some((_id, token)) = state.active.pop_front() {
                token.cancel();
            }
        }
        drop(state);
        self.notify.notify_waiters();
    }

    /// `(capacity, in use)`, like `MutableLimiter.GetLimit`.
    pub fn get_limit(&self) -> (usize, usize) {
        let state = self.state();
        (state.limit, state.active.len())
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum AcquireError {
    Cancelled,
}

pub struct Permit {
    id: u64,
    token: CancellationToken,
    limiter: Arc<MutableLimiter>,
}

impl Permit {
    /// The baseline's child context: cancelled when the limit shrinks past
    /// this permit.
    pub fn cancelled(&self) -> &CancellationToken {
        &self.token
    }
}

impl Drop for Permit {
    fn drop(&mut self) {
        {
            let mut state = self.limiter.state();
            state.active.retain(|(id, _)| *id != self.id);
        }
        self.limiter.notify.notify_waiters();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn limits_and_queues_acquisitions() {
        let limiter = MutableLimiter::new(1);
        let caller = CancellationToken::new();
        let first = limiter.acquire(&caller).await.expect("first");
        assert_eq!(limiter.get_limit(), (1, 1));

        // A second acquisition waits until the first permit is dropped.
        let second = {
            let limiter = Arc::clone(&limiter);
            let caller = CancellationToken::new();
            tokio::spawn(async move { limiter.acquire(&caller).await })
        };
        tokio::task::yield_now().await;
        assert!(!second.is_finished());

        drop(first);
        let second = second.await.expect("join").expect("second");
        assert_eq!(limiter.get_limit(), (1, 1));
        drop(second);
        assert_eq!(limiter.get_limit(), (1, 0));
    }

    #[tokio::test]
    async fn shrinking_cancels_the_oldest_permits() {
        let limiter = MutableLimiter::new(2);
        let caller = CancellationToken::new();
        let oldest = limiter.acquire(&caller).await.expect("oldest");
        let newest = limiter.acquire(&caller).await.expect("newest");
        assert_eq!(limiter.get_limit(), (2, 2));

        limiter.set_limit(1);
        assert!(oldest.cancelled().is_cancelled());
        assert!(!newest.cancelled().is_cancelled());
        // The cancelled permit no longer counts, and dropping it is harmless.
        assert_eq!(limiter.get_limit(), (1, 1));
        drop(newest);
        assert_eq!(limiter.get_limit(), (1, 0));
    }

    #[tokio::test]
    async fn gives_up_when_the_caller_is_cancelled() {
        let limiter = MutableLimiter::new(1);
        let caller = CancellationToken::new();
        let held = limiter.acquire(&caller).await.expect("held");

        let waiting_caller = CancellationToken::new();
        let waiting = {
            let limiter = Arc::clone(&limiter);
            let token = waiting_caller.clone();
            tokio::spawn(async move { limiter.acquire(&token).await })
        };
        waiting_caller.cancel();
        assert_eq!(
            waiting.await.expect("join").err().expect("cancelled"),
            AcquireError::Cancelled
        );
        // A released slot is usable again.
        drop(held);
        assert_eq!(limiter.get_limit(), (1, 0));
    }
}
