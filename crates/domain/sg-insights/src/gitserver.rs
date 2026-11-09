#![forbid(unsafe_code)]

//! `internal/insights/gitserver`: the commit client the insights backfiller
//! and compression use.
//!
//! [`GitCommitClient::recent_commits`] asks for the single commit nearest to
//! (and before) a sample time, restricted to `revision` or `HEAD`, ordered by
//! commit date. [`GitCommitClient::first_commit`] resolves the first commit
//! ever made to the repository and caches it in memory for the lifetime of
//! the client (entries are never evicted, matching
//! `CachedGitFirstEverCommit`); errors are not cached, so the next call
//! retries.
//!
//! The baseline constructor takes `gitserver.Client`; this crate only needs
//! `Commits` and `FirstEverCommit`, so the transport is injected through
//! [`GitserverCommitSource`] (implemented for [`sg_git::GitserverClient`]).
//! [`CommitError`] models the baseline's error chain because
//! `isFirstCommitEmptyRepoError` walks `errors.Unwrap` looking for
//! `gitdomain.RevisionNotFoundError`.
//!
//! The baseline also exposes `EmptyRepoErr` ("empty repository"), but nothing
//! in the baseline constructs it — the callers' `errors.Is` checks never
//! match. It is documented here as dead code rather than reproduced as a
//! constructible value.

use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use sg_git::{Commit, CommitsOptions, CommitsOrder};
use time::OffsetDateTime;
use tokio::sync::Mutex;

use crate::compression;

/// `gitdomain.RevisionNotFoundError`, possibly wrapped by the caller's error
/// handling (`errors.Wrapf`), preserved so [`is_empty_repo_error`] can walk
/// the chain like `errors.Unwrap`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CommitError {
    RevisionNotFound {
        repo: String,
        spec: String,
    },
    Wrapped {
        message: String,
        source: Box<CommitError>,
    },
    Other(String),
}

impl CommitError {
    /// `errors.Wrapf(err, "%s", message)`.
    pub fn wrapped(message: impl Into<String>, source: CommitError) -> Self {
        Self::Wrapped {
            message: message.into(),
            source: Box::new(source),
        }
    }

    /// `gitdomain.IsRevisionNotFoundError` after unwrapping.
    pub fn is_revision_not_found(&self) -> bool {
        match self {
            Self::RevisionNotFound { .. } => true,
            Self::Wrapped { source, .. } => source.is_revision_not_found(),
            Self::Other(_) => false,
        }
    }
}

impl fmt::Display for CommitError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::RevisionNotFound { repo, spec } => {
                write!(f, "revision not found: {repo}@{spec}")
            }
            Self::Wrapped { message, source } => write!(f, "{message}: {source}"),
            Self::Other(message) => f.write_str(message),
        }
    }
}

impl std::error::Error for CommitError {}

impl From<sg_git::Error> for CommitError {
    fn from(err: sg_git::Error) -> Self {
        match err {
            sg_git::Error::RevisionNotFound { repo, spec } => Self::RevisionNotFound { repo, spec },
            other => Self::Other(other.to_string()),
        }
    }
}

/// `isFirstCommitEmptyRepoError`: true for a `RevisionNotFoundError` at any
/// depth of the wrapper chain.
pub fn is_empty_repo_error(error: &CommitError) -> bool {
    error.is_revision_not_found()
}

/// The baseline's `gitserver.Client` surface used by this package.
#[async_trait]
pub trait GitserverCommitSource: Send + Sync {
    async fn commits(
        &self,
        repo: &str,
        options: &CommitsOptions,
    ) -> Result<Vec<Commit>, CommitError>;

    async fn first_ever_commit(&self, repo: &str) -> Result<Commit, CommitError>;
}

#[async_trait]
impl GitserverCommitSource for sg_git::GitserverClient {
    async fn commits(
        &self,
        repo: &str,
        options: &CommitsOptions,
    ) -> Result<Vec<Commit>, CommitError> {
        sg_git::GitserverClient::commits(self, repo, options)
            .await
            .map_err(CommitError::from)
    }

    async fn first_ever_commit(&self, repo: &str) -> Result<Commit, CommitError> {
        sg_git::GitserverClient::first_ever_commit(self, repo)
            .await
            .map_err(CommitError::from)
    }
}

/// `insights/gitserver.GitCommitClient`.
pub struct GitCommitClient {
    source: Arc<dyn GitserverCommitSource>,
    /// `CachedGitFirstEverCommit.cache`; the baseline holds a plain mutex
    /// across the RPC, so first-commit lookups are serialized per client.
    first_commit_cache: Mutex<HashMap<String, Commit>>,
}

impl GitCommitClient {
    pub fn new(source: Arc<dyn GitserverCommitSource>) -> Self {
        Self {
            source,
            first_commit_cache: Mutex::new(HashMap::new()),
        }
    }

    /// `GitCommitClient.FirstCommit`.
    pub async fn first_commit(&self, repo: &str) -> Result<Commit, CommitError> {
        let mut cache = self.first_commit_cache.lock().await;
        if let Some(commit) = cache.get(repo) {
            return Ok(commit.clone());
        }
        let commit = self.source.first_ever_commit(repo).await?;
        cache.insert(repo.to_owned(), commit.clone());
        Ok(commit)
    }

    /// `GitCommitClient.RecentCommits`: one commit strictly before `target`,
    /// ordered by commit date, on `revision` (or `HEAD` when empty).
    pub async fn recent_commits(
        &self,
        repo: &str,
        target: OffsetDateTime,
        revision: &str,
    ) -> Result<Vec<Commit>, CommitError> {
        let options = CommitsOptions {
            max_commits: 1,
            before: Some(timestamp(target)),
            order: Some(CommitsOrder::CommitDate),
            ranges: vec![if revision.is_empty() {
                "HEAD".to_owned()
            } else {
                revision.to_owned()
            }],
            ..CommitsOptions::default()
        };
        self.source.commits(repo, &options).await
    }
}

/// `time.Time` → `timestamppb.Timestamp` (`gitserver.CommitsOptions.ToProto`
/// stores `Before` as a timestamp).
fn timestamp(value: OffsetDateTime) -> prost_types::Timestamp {
    prost_types::Timestamp {
        seconds: value.unix_timestamp(),
        nanos: value.nanosecond() as i32,
    }
}

#[async_trait]
impl compression::CommitFetcher for GitCommitClient {
    async fn recent_commits(
        &self,
        repo_name: &str,
        target: OffsetDateTime,
        revision: &str,
    ) -> Result<Vec<compression::Commit>, String> {
        GitCommitClient::recent_commits(self, repo_name, target, revision)
            .await
            .map(|commits| {
                commits
                    .into_iter()
                    .map(|commit| compression::Commit { id: commit.oid })
                    .collect()
            })
            .map_err(|err| err.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use time::macros::datetime;

    struct FakeSource {
        first_commit: Result<Commit, CommitError>,
        first_commit_calls: AtomicUsize,
        commits: Vec<Commit>,
        last_options: Mutex<Option<CommitsOptions>>,
    }

    impl FakeSource {
        fn new(first_commit: Result<Commit, CommitError>, commits: Vec<Commit>) -> Self {
            Self {
                first_commit,
                first_commit_calls: AtomicUsize::new(0),
                commits,
                last_options: Mutex::new(None),
            }
        }
    }

    #[async_trait]
    impl GitserverCommitSource for FakeSource {
        async fn commits(
            &self,
            _repo: &str,
            options: &CommitsOptions,
        ) -> Result<Vec<Commit>, CommitError> {
            *self.last_options.lock().await = Some(options.clone());
            Ok(self.commits.clone())
        }

        async fn first_ever_commit(&self, _repo: &str) -> Result<Commit, CommitError> {
            self.first_commit_calls.fetch_add(1, Ordering::SeqCst);
            self.first_commit.clone()
        }
    }

    fn commit(oid: &str) -> Commit {
        Commit {
            oid: oid.to_owned(),
            parents: Vec::new(),
            author_name: String::new(),
            author_email: String::new(),
            author_date: None,
            committer_date: None,
            message: String::new(),
        }
    }

    #[tokio::test]
    async fn recent_commits_default_to_head_before_target_by_commit_date() {
        let source = Arc::new(FakeSource::new(Ok(commit("first")), vec![commit("recent")]));
        let client = GitCommitClient::new(source.clone());
        let target = datetime!(2021-01-01 0:00 UTC);

        let commits = client.recent_commits("myrepo", target, "").await.unwrap();

        assert_eq!(commits, vec![commit("recent")]);
        let options = source.last_options.lock().await.clone().unwrap();
        assert_eq!(options.max_commits, 1);
        assert_eq!(options.ranges, ["HEAD"]);
        assert_eq!(options.order, Some(CommitsOrder::CommitDate));
        let before = options.before.unwrap();
        assert_eq!(before.seconds, target.unix_timestamp());
        assert_eq!(before.nanos, 0);
    }

    #[tokio::test]
    async fn recent_commits_use_the_revision_when_set() {
        let source = Arc::new(FakeSource::new(Ok(commit("first")), Vec::new()));
        let client = GitCommitClient::new(source.clone());

        let _ = client
            .recent_commits("myrepo", datetime!(2021-01-01 0:00 UTC), "abc123")
            .await;

        let options = source.last_options.lock().await.clone().unwrap();
        assert_eq!(options.ranges, ["abc123"]);
    }

    #[tokio::test]
    async fn first_commit_is_cached() {
        let source = Arc::new(FakeSource::new(Ok(commit("first")), Vec::new()));
        let client = GitCommitClient::new(source.clone());

        assert_eq!(client.first_commit("myrepo").await.unwrap().oid, "first");
        assert_eq!(client.first_commit("myrepo").await.unwrap().oid, "first");

        assert_eq!(source.first_commit_calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn first_commit_errors_are_not_cached() {
        let source = Arc::new(FakeSource::new(
            Err(CommitError::Other("boom".to_owned())),
            Vec::new(),
        ));
        let client = GitCommitClient::new(source.clone());

        assert!(client.first_commit("myrepo").await.is_err());
        assert!(client.first_commit("myrepo").await.is_err());

        assert_eq!(source.first_commit_calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn compression_fetcher_maps_commit_ids() {
        let source = Arc::new(FakeSource::new(Ok(commit("first")), vec![commit("recent")]));
        let client = GitCommitClient::new(source);

        let commits = compression::CommitFetcher::recent_commits(
            &client,
            "myrepo",
            datetime!(2021-01-01 0:00 UTC),
            "",
        )
        .await
        .unwrap();

        assert_eq!(
            commits,
            vec![compression::Commit {
                id: "recent".to_owned()
            }]
        );
    }

    #[test]
    fn empty_repo_error_follows_the_wrapper_chain() {
        let not_found = CommitError::RevisionNotFound {
            repo: "foo".to_owned(),
            spec: "HEAD".to_owned(),
        };
        assert!(!is_empty_repo_error(&CommitError::Other(
            "A different error".to_owned()
        )));
        assert!(!is_empty_repo_error(&CommitError::wrapped(
            "Another message",
            CommitError::Other("Not an empty repo".to_owned())
        )));
        assert!(is_empty_repo_error(&not_found));
        assert!(is_empty_repo_error(&CommitError::wrapped(
            "nice!", not_found
        )));
    }
}
