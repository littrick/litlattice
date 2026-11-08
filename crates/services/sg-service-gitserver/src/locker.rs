//! Port of `cmd/gitserver/internal/lock.go`: per-repository locks that also
//! carry a human-readable status.
//!
//! The status is what `RepoCloneProgress` reports while a clone or fetch is in
//! flight; a repository is locked exactly while it has an entry in the map.
//! Unlike the baseline, dropping a [`RepositoryLock`] releases it (the Go
//! implementation requires an explicit `Release`), which only prevents leaked
//! locks and cannot extend a lock's lifetime.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard};

pub struct RepositoryLocker {
    /// Locked repositories and their status. Guarded for the whole duration
    /// of every operation, like `repositoryLocker.mu`.
    status: Mutex<HashMap<String, String>>,
}

impl RepositoryLocker {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            status: Mutex::new(HashMap::new()),
        })
    }

    fn status_map(&self) -> MutexGuard<'_, HashMap<String, String>> {
        // A poisoned map only means a panic while holding the lock; the map
        // itself stays consistent because every mutation is a single insert or
        // remove, so keep serving.
        self.status.lock().unwrap_or_else(|err| err.into_inner())
    }

    /// Acquire the lock for `repo`. Returns `None` when it is already held.
    pub fn try_acquire(
        self: &Arc<Self>,
        repo: &str,
        initial_status: &str,
    ) -> Option<RepositoryLock> {
        {
            let mut status = self.status_map();
            if status.contains_key(repo) {
                return None;
            }
            status.insert(repo.to_owned(), initial_status.to_owned());
        }
        Some(RepositoryLock {
            locker: Arc::clone(self),
            repo: repo.to_owned(),
            done: Mutex::new(false),
        })
    }

    /// The current status, or `None` when the repository is not locked.
    pub fn status(&self, repo: &str) -> Option<String> {
        self.status_map().get(repo).cloned()
    }

    /// Snapshot of every locked repository's status.
    pub fn all_statuses(&self) -> HashMap<String, String> {
        self.status_map().clone()
    }
}

pub struct RepositoryLock {
    locker: Arc<RepositoryLocker>,
    repo: String,
    /// Set under the per-lock mutex so that a `set_status` racing with
    /// `release` observes the same ordering as the baseline's `l.mu`.
    done: Mutex<bool>,
}

impl RepositoryLock {
    fn done(&self) -> MutexGuard<'_, bool> {
        self.done.lock().unwrap_or_else(|err| err.into_inner())
    }

    /// Update the status shown by `RepoCloneProgress`. A no-op once the lock
    /// has been released, like the baseline's `done` flag.
    pub fn set_status(&self, status: &str) {
        let done = self.done();
        if !*done {
            self.locker
                .status_map()
                .insert(self.repo.clone(), status.to_owned());
        }
    }

    /// Release the lock; further `set_status` calls are ignored and a second
    /// release is a no-op.
    pub fn release(&self) {
        let mut done = self.done();
        if !*done {
            self.locker.status_map().remove(&self.repo);
            *done = true;
        }
    }
}

impl Drop for RepositoryLock {
    fn drop(&mut self) {
        self.release();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn locks_repositories_exclusively() {
        let locker = RepositoryLocker::new();
        let lock = locker
            .try_acquire("github.com/owner/repo", "cloning")
            .expect("acquire");
        assert_eq!(
            locker.status("github.com/owner/repo").as_deref(),
            Some("cloning")
        );
        // A second acquisition fails and keeps the original status.
        assert!(locker
            .try_acquire("github.com/owner/repo", "fetching")
            .is_none());
        assert_eq!(
            locker.status("github.com/owner/repo").as_deref(),
            Some("cloning")
        );
        // Other repositories are independent.
        let other = locker
            .try_acquire("github.com/owner/other", "fetching")
            .expect("acquire other");
        assert_eq!(locker.all_statuses().len(), 2);

        lock.set_status("still cloning");
        assert_eq!(
            locker.status("github.com/owner/repo").as_deref(),
            Some("still cloning")
        );

        lock.release();
        assert_eq!(locker.status("github.com/owner/repo"), None);
        // Releasing twice and setting status after release are no-ops.
        lock.release();
        lock.set_status("ignored");
        assert_eq!(locker.status("github.com/owner/repo"), None);
        assert_eq!(
            locker.status("github.com/owner/other").as_deref(),
            Some("fetching")
        );

        // The released repository can be acquired again.
        let again = locker
            .try_acquire("github.com/owner/repo", "fetching")
            .expect("re-acquire");
        assert_eq!(
            locker.status("github.com/owner/repo").as_deref(),
            Some("fetching")
        );
        drop(again);
        assert_eq!(locker.status("github.com/owner/repo"), None);
        drop(other);
        assert!(locker.all_statuses().is_empty());
    }
}
