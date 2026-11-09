#![forbid(unsafe_code)]

//! `internal/insights/scheduler/iterator`: the pure state of the persistent
//! repository iterator.
//!
//! [`RepoIterator`] mirrors the in-memory fields and pure methods of Go's
//! `PersistentRepoIterator`: peeking at the next page, error accounting, the
//! terminal-error transition and the retry walk. The storage-bound methods
//! (`New`, `Load`, `NextWithFinish`, `doFinishN`, `MarkComplete`, `Restart`)
//! commit to `repo_iterator*` tables and stay in the database layer
//! (`sg-db`) for the storage-wiring slice.
//!
//! Two baseline quirks are preserved deliberately:
//!
//! - `HasTerminalErrors` returns whether `errors` is non-empty (Go checks
//!   `len(p.errors) > 0`, not `terminalErrors`);
//! - `peek_n` returns an empty slice once the cursor is past the end, while
//!   an in-range peek returns the remaining tail if `num` exceeds it.

use std::collections::BTreeMap;

/// `iterator.IterationConfig` (the `OnTerminal` callback is only used by the
/// storage path, so it is not part of the pure state).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IterationConfig {
    pub max_failures: i64,
}

/// `iterator.IterationError`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IterationError {
    pub repo_id: i32,
    pub failure_count: i64,
    pub error_messages: Vec<String>,
}

/// `iterator.PersistentRepoIterator`, pure subset.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct RepoIterator {
    pub repos: Vec<i32>,
    pub cursor: usize,
    pub total_count: i64,
    pub success_count: i64,
    pub percent_complete: f64,
    /// `completed_at`; `None` mirrors Go's zero `time.Time`.
    pub completed_at: Option<time::OffsetDateTime>,
    /// `runtime_duration` in nanoseconds (Go `time.Duration`).
    pub runtime_duration_nanoseconds: i64,
    pub errors: BTreeMap<i32, IterationError>,
    pub terminal_errors: BTreeMap<i32, IterationError>,
    pub retry_repos: Vec<i32>,
    pub retry_cursor: usize,
}

impl RepoIterator {
    /// `HasMore`.
    pub fn has_more(&self) -> bool {
        self.peek(self.cursor).is_some()
    }

    /// `peek`: the repo at `offset`, if any.
    pub fn peek(&self, offset: usize) -> Option<i32> {
        self.repos.get(offset).copied()
    }

    /// `peekN`: the repos in `[offset, offset+num)`, or an empty slice when
    /// the cursor is past the end.
    pub fn peek_n(&self, offset: usize, num: usize) -> Vec<i32> {
        if offset >= self.repos.len() {
            return Vec::new();
        }
        let end = offset.saturating_add(num).min(self.repos.len());
        self.repos[offset..end].to_vec()
    }

    /// `HasErrors`.
    pub fn has_errors(&self) -> bool {
        !self.errors.is_empty()
    }

    /// `HasTerminalErrors` (baseline returns whether `errors` is non-empty).
    pub fn has_terminal_errors(&self) -> bool {
        !self.errors.is_empty()
    }

    /// `ErroredRepos`.
    pub fn errored_repos(&self) -> usize {
        self.errors.len()
    }

    /// `TotalErrors`.
    pub fn total_errors(&self) -> i64 {
        self.errors
            .values()
            .chain(self.terminal_errors.values())
            .map(|err| err.failure_count)
            .sum()
    }

    /// `Errors`, sorted by repo id (the Go map order is unspecified; the
    /// differential harness sorts both sides).
    pub fn errors_sorted(&self) -> Vec<IterationError> {
        self.errors.values().cloned().collect()
    }

    /// `errorMap.FailureCount`.
    pub fn failure_count(&self, repo: i32) -> i64 {
        self.errors
            .get(&repo)
            .map(|err| err.failure_count)
            .unwrap_or(0)
    }

    /// `advanceRetry`.
    pub fn advance_retry(&mut self) {
        self.retry_cursor += 1;
    }

    /// `setRepoTerminal`: move a repo's error from `errors` to
    /// `terminal_errors`. The baseline panics if the repo has no error; the
    /// caller guarantees it does.
    pub fn set_repo_terminal(&mut self, repo: i32) {
        let err = self
            .errors
            .remove(&repo)
            .expect("set_repo_terminal requires a recorded error");
        self.terminal_errors.insert(repo, err);
    }

    /// `resetRetry`: rebuild the retry list, moving repos that reached
    /// `max_failures` to the terminal set.
    pub fn reset_retry(&mut self, config: IterationConfig) {
        self.retry_cursor = 0;
        let mut retry = Vec::new();
        let mut terminal = BTreeMap::new();
        let mut remaining = BTreeMap::new();
        for (repo, err) in std::mem::take(&mut self.errors) {
            if config.max_failures > 0 && err.failure_count >= config.max_failures {
                terminal.insert(repo, err);
            } else {
                retry.push(repo);
                remaining.insert(repo, err);
            }
        }
        // Repos over the threshold leave `errors` for `terminal_errors`,
        // matching Go's `delete(p.errors, repo)`; the rest stay recorded.
        self.errors = remaining;
        self.terminal_errors = terminal;
        self.retry_repos = retry;
    }

    /// The decision half of `NextRetryWithFinish`: the next repo to retry,
    /// skipping (and terminating) repos already over the failure threshold.
    /// Returns `None` when the iterator is complete or the retry list is
    /// exhausted.
    pub fn next_retry(&mut self, config: IterationConfig) -> Option<i32> {
        if self.retry_repos.is_empty() {
            self.reset_retry(config);
        }
        loop {
            let current = self.peek_retry()?;
            if config.max_failures > 0 && self.failure_count(current) >= config.max_failures {
                self.advance_retry();
                self.set_repo_terminal(current);
                continue;
            }
            return Some(current);
        }
    }

    fn peek_retry(&self) -> Option<i32> {
        if self.completed_at.is_some() {
            return None;
        }
        self.retry_repos.get(self.retry_cursor).copied()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn iterator(repos: &[i32], errors: &[(i32, i64)]) -> RepoIterator {
        RepoIterator {
            repos: repos.to_vec(),
            total_count: repos.len() as i64,
            errors: errors
                .iter()
                .map(|&(repo, failure_count)| {
                    (
                        repo,
                        IterationError {
                            repo_id: repo,
                            failure_count,
                            error_messages: vec!["boom".to_owned()],
                        },
                    )
                })
                .collect(),
            ..RepoIterator::default()
        }
    }

    #[test]
    fn peek_matches_baseline() {
        let it = iterator(&[10, 20, 30], &[]);
        assert!(it.has_more());
        assert_eq!(it.peek(0), Some(10));
        assert_eq!(it.peek(2), Some(30));
        assert_eq!(it.peek(3), None);
        assert_eq!(it.peek_n(0, 2), vec![10, 20]);
        assert_eq!(it.peek_n(1, 5), vec![20, 30]);
        assert_eq!(it.peek_n(3, 2), Vec::<i32>::new());
    }

    #[test]
    fn error_accounting_matches_baseline() {
        let it = iterator(&[1, 2, 3], &[(1, 2), (2, 1)]);
        assert!(it.has_errors());
        assert!(it.has_terminal_errors()); // baseline checks `errors`
        assert_eq!(it.errored_repos(), 2);
        assert_eq!(it.total_errors(), 3);
        assert_eq!(it.failure_count(1), 2);
        assert_eq!(it.failure_count(9), 0);
    }

    #[test]
    fn reset_retry_moves_terminal_errors() {
        let mut it = iterator(&[1, 2, 3], &[(1, 3), (2, 1)]);
        it.reset_retry(IterationConfig { max_failures: 3 });
        assert_eq!(it.retry_repos, vec![2]);
        assert_eq!(
            it.terminal_errors.keys().copied().collect::<Vec<_>>(),
            vec![1]
        );
        // Repos still under the threshold stay recorded.
        assert_eq!(it.errors.keys().copied().collect::<Vec<_>>(), vec![2]);
        assert_eq!(it.total_errors(), 4);
    }

    #[test]
    fn next_retry_skips_and_terminates() {
        let mut it = iterator(&[1, 2], &[]);
        it.retry_repos = vec![1, 2];
        it.errors.insert(
            1,
            IterationError {
                repo_id: 1,
                failure_count: 3,
                error_messages: vec!["boom".to_owned()],
            },
        );
        let next = it.next_retry(IterationConfig { max_failures: 3 });
        assert_eq!(next, Some(2));
        assert_eq!(it.retry_cursor, 1);
        assert_eq!(
            it.terminal_errors.keys().copied().collect::<Vec<_>>(),
            vec![1]
        );
    }
}
