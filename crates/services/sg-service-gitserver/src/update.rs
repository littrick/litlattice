//! Port of `Server.repoUpdateOrClone` (`cmd/gitserver/internal/server.go`):
//! the clone-vs-fetch decision and its locking/limiting.
//!
//! The baseline wires this to the filesystem, the VCS syncer (remote URL from
//! external services), and `gitserver_repos`; those do not exist in Rust yet,
//! so the work they do is behind [`UpdateBackend`]. The orchestration itself
//! is complete: acquire the repository lock, wait for a clone slot, run the
//! job in a tracked background task (it survives the caller, matching Go's
//! goroutine), release the lock, and record the last error.

use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

use crate::git::GitError;
use crate::limiter::{AcquireError, MutableLimiter};
use crate::locker::{RepositoryLock, RepositoryLocker};
use crate::metrics;

/// The operations `repoUpdateOrClone` performs on the repository. The real
/// implementation will use `vcssyncer` (clone/fetch) and the filesystem
/// (`RepoCloned`); `set_last_error` writes `gitserver_repos.last_error`.
#[async_trait]
pub trait UpdateBackend: Send + Sync {
    /// `fs.RepoCloned`: `<gitdir>/HEAD` exists.
    async fn repo_cloned(&self, repo: &str) -> Result<bool, GitError>;
    /// Clone into the repository directory (temporary staging + rename).
    async fn clone(
        &self,
        repo: &str,
        lock: &RepositoryLock,
        cancelled: &CancellationToken,
    ) -> Result<(), GitError>;
    /// Fetch updates for an already cloned repository.
    async fn fetch(
        &self,
        repo: &str,
        lock: &RepositoryLock,
        cancelled: &CancellationToken,
    ) -> Result<(), GitError>;
    /// `db.GitserverRepos().SetLastError`; called with an empty string after a
    /// successful update. Failures are logged by the implementation.
    async fn set_last_error(&self, repo: &str, error: &str);
}

pub struct UpdateServer {
    pub locker: Arc<RepositoryLocker>,
    pub limiter: Arc<MutableLimiter>,
    pub backend: Arc<dyn UpdateBackend>,
    /// Server-scoped cancellation (`Server.ctx`): an update keeps running
    /// after the caller goes away, but stops when the server shuts down.
    pub cancelled: CancellationToken,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UpdateError {
    /// `ErrFetchInProgress`.
    FetchInProgress,
    /// The caller's context was cancelled while waiting for a slot.
    Cancelled,
    /// A clone or fetch failed; the message includes the baseline's wrapper
    /// (`failed to clone <repo>: ...` / `failed to fetch <repo>: ...`).
    Failed(String),
}

impl fmt::Display for UpdateError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::FetchInProgress => f.write_str("fetch for this repo already in progress"),
            // Go `context.Canceled.Error()`.
            Self::Cancelled => f.write_str("context canceled"),
            Self::Failed(message) => f.write_str(message),
        }
    }
}

impl std::error::Error for UpdateError {}

impl UpdateServer {
    /// `repoUpdateOrClone`. `tasks` is the server's [`TaskTracker`] so that
    /// the background update is shut down together with the service.
    ///
    /// Calling this while another update for the same repository is running
    /// fails immediately with [`UpdateError::FetchInProgress`]; cancelling
    /// the caller's token only makes *this* call return early, the update
    /// keeps running in the background.
    pub async fn repo_update_or_clone(
        self: &Arc<Self>,
        tasks: &TaskTracker,
        repo: &str,
        caller_cancelled: &CancellationToken,
    ) -> Result<(), UpdateError> {
        let Some(lock) = self.locker.try_acquire(repo, "starting fetch") else {
            return Err(UpdateError::FetchInProgress);
        };

        // `pendingClones.Inc()` / `pendingClones.Dec()` around the limiter
        // wait: the gauge counts repositories *waiting* for a slot.
        metrics::clone_queued(1.0);
        let permit = match self.limiter.acquire(caller_cancelled).await {
            Ok(permit) => permit,
            Err(AcquireError::Cancelled) => {
                metrics::clone_queued(-1.0);
                lock.release();
                return Err(UpdateError::Cancelled);
            }
        };
        metrics::clone_queued(-1.0);

        let (tx, rx) = tokio::sync::oneshot::channel();
        let server = Arc::clone(self);
        let repo = repo.to_owned();
        tasks.spawn(async move {
            let result = server.run_update(&repo, &lock).await;
            // `defer` order in the baseline: release the lock and the limiter
            // slot after the error is recorded.
            let error = match &result {
                Ok(()) => String::new(),
                Err(err) => err.to_string(),
            };
            server.backend.set_last_error(&repo, &error).await;
            lock.release();
            drop(permit);
            let _ = tx.send(result);
        });

        tokio::select! {
            result = rx => match result {
                Ok(result) => result,
                // The background task can only disappear by panicking.
                Err(_) => Err(UpdateError::Failed("repo update task failed".to_owned())),
            },
            _ = caller_cancelled.cancelled() => Err(UpdateError::Cancelled),
        }
    }

    async fn run_update(&self, repo: &str, lock: &RepositoryLock) -> Result<(), UpdateError> {
        let cloned = self.backend.repo_cloned(repo).await.map_err(|err| {
            UpdateError::Failed(format!("determining cloned status: {}", err.message()))
        })?;

        if !cloned {
            // Fully qualified: `Arc<dyn UpdateBackend>` also has `Clone::clone`.
            let result =
                UpdateBackend::clone(self.backend.as_ref(), repo, lock, &self.cancelled).await;
            if let Err(err) = result {
                metrics::clone_failed();
                return Err(UpdateError::Failed(format!(
                    "failed to clone {repo}: {}",
                    err.message()
                )));
            }
            metrics::clone_succeeded();
            tracing::info!(repo, "cloned repo");
        } else {
            self.backend
                .fetch(repo, lock, &self.cancelled)
                .await
                .map_err(|err| {
                    UpdateError::Failed(format!("failed to fetch {repo}: {}", err.message()))
                })?;
        }
        Ok(())
    }
}

/// Where a repository's remote URL comes from. In the baseline this is the
/// external-service/site-config lookup (`getVCSSyncer`); the Rust site-config
/// client is not ported yet, so callers provide it explicitly.
#[async_trait]
pub trait RemoteUrlSource: Send + Sync {
    async fn remote_url(&self, repo: &str) -> Result<String, GitError>;
}

/// The real [`UpdateBackend`]: clones and fetches repositories on disk the way
/// `gitRepoSyncer`/`cloneRepo`/`doRepoUpdate` do.
///
/// Still missing from the baseline path: `gitserver_repos` clone status and
/// last-output writes, the fast-forward/`RepoSyncState` DB update, and the
/// progress stream (the syncer writes progress into a buffer that is dropped).
pub struct LocalBackend {
    pub repos_dir: PathBuf,
    pub remote_urls: Arc<dyn RemoteUrlSource>,
}

impl LocalBackend {
    fn git_dir(&self, repo: &str) -> Result<PathBuf, GitError> {
        crate::repo::repo_dir(&self.repos_dir, repo).ok_or_else(|| {
            GitError::Message(format!("repository name {:?} escapes the repos dir", repo))
        })
    }

    async fn remote_url(&self, repo: &str) -> Result<String, GitError> {
        self.remote_urls.remote_url(repo).await.map_err(|err| {
            GitError::Message(format!(
                "failed to get remote URL source for {:?}: {}",
                repo,
                err.message()
            ))
        })
    }
}

fn io_error(context: &str, err: std::io::Error) -> GitError {
    GitError::Message(format!("{context}: {err}"))
}

#[async_trait]
impl UpdateBackend for LocalBackend {
    async fn repo_cloned(&self, repo: &str) -> Result<bool, GitError> {
        crate::fs::repo_cloned(&self.repos_dir, repo).map_err(|err| io_error("repo cloned", err))
    }

    async fn clone(
        &self,
        repo: &str,
        lock: &RepositoryLock,
        _cancelled: &CancellationToken,
    ) -> Result<(), GitError> {
        // `cloneRepo` returns success without touching the disk for the
        // always-cloning test repository.
        if crate::grpc::is_always_cloning_test(repo) {
            return Ok(());
        }

        let dst = self.git_dir(repo)?;
        if std::fs::metadata(&dst).is_ok() {
            // Baseline `&os.PathError{Op: "cloneRepo", Path, Err: os.ErrExist}`.
            return Err(GitError::Message(format!(
                "cloneRepo {}: file exists",
                dst.display()
            )));
        }

        let remote_url = self.remote_url(repo).await?;
        let tmp_dir = crate::fs::temp_dir(&self.repos_dir, "clone-")
            .map_err(|err| io_error("clone failed to create tmp dir", err))?;
        let _cleanup = crate::fs::TempDirCleanup(tmp_dir.clone());
        let tmp_path = crate::vcssyncer::clone_tmp_path(&tmp_dir);

        // `readFetchProgress`: every syncer message and git progress line
        // becomes the lock status shown by `RepoCloneProgress`.
        let mut progress = Vec::new();
        let mut on_progress = |line: &str| lock.set_status(line);
        crate::vcssyncer::clone_repo_with(&tmp_path, &remote_url, &mut progress, &mut on_progress)
            .await?;

        crate::postfetch::post_fetch_actions(&tmp_path, repo).await?;

        let parent = dst.parent().unwrap_or_else(|| Path::new("."));
        std::fs::create_dir_all(parent)
            .map_err(|err| io_error("failed to create repository directory", err))?;
        crate::postfetch::rename_and_sync(&tmp_path, &dst)
            .map_err(|err| io_error("failed to rename cloned repository", err))?;
        Ok(())
    }

    async fn fetch(
        &self,
        repo: &str,
        lock: &RepositoryLock,
        _cancelled: &CancellationToken,
    ) -> Result<(), GitError> {
        let git_dir = self.git_dir(repo)?;
        let remote_url = self.remote_url(repo).await?;

        let mut progress = Vec::new();
        let mut on_progress = |line: &str| lock.set_status(line);
        crate::vcssyncer::fetch_repo_with(&git_dir, &remote_url, &mut progress, &mut on_progress)
            .await?;

        crate::postfetch::post_fetch_actions(&git_dir, repo).await?;
        Ok(())
    }

    async fn set_last_error(&self, repo: &str, error: &str) {
        // `SetLastError` needs `gitserver_repos`; until then the update log is
        // the only place the error is visible, matching the baseline's log line.
        if error.is_empty() {
            tracing::debug!(repo, "repo update succeeded");
        } else {
            tracing::warn!(repo, error, "repo update failed");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    #[derive(Default)]
    struct FakeBackend {
        cloned: bool,
        clone_error: Option<String>,
        /// When set, `clone` announces that it started and waits for the gate
        /// before returning.
        gate: Option<Arc<tokio::sync::Notify>>,
        started: Arc<tokio::sync::Notify>,
        calls: Mutex<Vec<String>>,
        last_errors: Mutex<Vec<String>>,
    }

    impl FakeBackend {
        fn calls(&self) -> Vec<String> {
            self.calls.lock().unwrap().clone()
        }
        fn last_errors(&self) -> Vec<String> {
            self.last_errors.lock().unwrap().clone()
        }
    }

    #[async_trait]
    impl UpdateBackend for FakeBackend {
        async fn repo_cloned(&self, _repo: &str) -> Result<bool, GitError> {
            self.calls.lock().unwrap().push("cloned".to_owned());
            Ok(self.cloned)
        }

        async fn clone(
            &self,
            _repo: &str,
            _lock: &RepositoryLock,
            _cancelled: &CancellationToken,
        ) -> Result<(), GitError> {
            self.calls.lock().unwrap().push("clone".to_owned());
            if let Some(gate) = &self.gate {
                self.started.notify_waiters();
                gate.notified().await;
            }
            match &self.clone_error {
                Some(error) => Err(GitError::Message(error.clone())),
                None => Ok(()),
            }
        }

        async fn fetch(
            &self,
            _repo: &str,
            _lock: &RepositoryLock,
            _cancelled: &CancellationToken,
        ) -> Result<(), GitError> {
            self.calls.lock().unwrap().push("fetch".to_owned());
            Ok(())
        }

        async fn set_last_error(&self, _repo: &str, error: &str) {
            self.last_errors.lock().unwrap().push(error.to_owned());
        }
    }

    fn server(backend: Arc<FakeBackend>) -> (Arc<UpdateServer>, TaskTracker) {
        let server = Arc::new(UpdateServer {
            locker: RepositoryLocker::new(),
            limiter: MutableLimiter::new(5),
            backend,
            cancelled: CancellationToken::new(),
        });
        (server, TaskTracker::new())
    }

    #[tokio::test]
    async fn clones_when_the_repository_is_missing() {
        let fake = Arc::new(FakeBackend::default());
        let (server, tasks) = server(Arc::clone(&fake));
        let caller = CancellationToken::new();
        server
            .repo_update_or_clone(&tasks, "github.com/owner/repo", &caller)
            .await
            .expect("update");
        tasks.close();
        tasks.wait().await;

        assert_eq!(fake.calls(), vec!["cloned", "clone"]);
        assert_eq!(fake.last_errors(), vec![""]);
        // The lock was released.
        assert_eq!(server.locker.status("github.com/owner/repo"), None);
    }

    #[tokio::test]
    async fn fetches_when_the_repository_exists() {
        let fake = Arc::new(FakeBackend {
            cloned: true,
            ..FakeBackend::default()
        });
        let (server, tasks) = server(Arc::clone(&fake));
        let caller = CancellationToken::new();
        server
            .repo_update_or_clone(&tasks, "github.com/owner/repo", &caller)
            .await
            .expect("update");
        tasks.close();
        tasks.wait().await;

        assert_eq!(fake.calls(), vec!["cloned", "fetch"]);
        assert_eq!(fake.last_errors(), vec![""]);
    }

    #[tokio::test]
    async fn records_clone_failures() {
        let fake = Arc::new(FakeBackend {
            clone_error: Some("boom".to_owned()),
            ..FakeBackend::default()
        });
        let (server, tasks) = server(Arc::clone(&fake));
        let caller = CancellationToken::new();
        let err = server
            .repo_update_or_clone(&tasks, "github.com/owner/repo", &caller)
            .await
            .expect_err("clone failure");
        tasks.close();
        tasks.wait().await;

        assert_eq!(
            err,
            UpdateError::Failed("failed to clone github.com/owner/repo: boom".to_owned())
        );
        assert_eq!(
            fake.last_errors(),
            vec!["failed to clone github.com/owner/repo: boom"]
        );
        assert_eq!(server.locker.status("github.com/owner/repo"), None);
    }

    #[tokio::test]
    async fn rejects_concurrent_updates_for_the_same_repo() {
        let gate = Arc::new(tokio::sync::Notify::new());
        let fake = Arc::new(FakeBackend {
            gate: Some(Arc::clone(&gate)),
            ..FakeBackend::default()
        });
        let (server, tasks) = server(Arc::clone(&fake));
        let started = Arc::clone(&fake.started);

        let first = {
            let server = Arc::clone(&server);
            let tasks = tasks.clone();
            let caller = CancellationToken::new();
            tokio::spawn(async move {
                server
                    .repo_update_or_clone(&tasks, "github.com/owner/repo", &caller)
                    .await
            })
        };
        started.notified().await;
        assert_eq!(
            server
                .repo_update_or_clone(&tasks, "github.com/owner/repo", &CancellationToken::new())
                .await
                .expect_err("already in progress"),
            UpdateError::FetchInProgress
        );

        gate.notify_waiters();
        first.await.expect("join").expect("first update");
        tasks.close();
        tasks.wait().await;
        assert_eq!(server.locker.status("github.com/owner/repo"), None);
    }

    #[tokio::test]
    async fn releases_the_lock_when_the_caller_is_cancelled_while_waiting() {
        let fake = Arc::new(FakeBackend::default());
        let (server, tasks) = server(Arc::clone(&fake));
        // No clone slots: the acquisition can only end through cancellation.
        server.limiter.set_limit(0);
        let caller = CancellationToken::new();
        caller.cancel();

        assert_eq!(
            server
                .repo_update_or_clone(&tasks, "github.com/owner/repo", &caller)
                .await
                .expect_err("cancelled"),
            UpdateError::Cancelled
        );
        assert_eq!(server.locker.status("github.com/owner/repo"), None);
        assert!(fake.calls().is_empty());
    }

    // --- LocalBackend: the on-disk implementation -------------------------

    use std::collections::HashMap;

    struct StaticUrls(HashMap<String, String>);

    #[async_trait]
    impl RemoteUrlSource for StaticUrls {
        async fn remote_url(&self, repo: &str) -> Result<String, GitError> {
            self.0
                .get(repo)
                .cloned()
                .ok_or_else(|| GitError::Message(format!("no remote URL for {repo}")))
        }
    }

    fn git(dir: &Path, args: &[&str]) {
        let status = std::process::Command::new("git")
            .args(args)
            .current_dir(dir)
            .status()
            .expect("run git");
        assert!(status.success(), "git {args:?} failed");
    }

    fn git_output(dir: &Path, args: &[&str]) -> String {
        let output = std::process::Command::new("git")
            .args(args)
            .current_dir(dir)
            .output()
            .expect("run git");
        assert!(output.status.success(), "git {args:?} failed");
        String::from_utf8(output.stdout)
            .expect("utf8")
            .trim()
            .to_owned()
    }

    /// A bare origin with one commit on `trunk`, plus the work tree used to
    /// push further commits.
    fn source_repo(root: &Path) -> (PathBuf, PathBuf) {
        let origin = root.join("origin.git");
        std::fs::create_dir_all(&origin).expect("mkdir");
        git(&origin, &["init", "--bare", "-q", "--initial-branch=trunk"]);

        let work = root.join("work");
        git(
            root,
            &[
                "init",
                "-q",
                "--initial-branch=trunk",
                work.to_str().unwrap(),
            ],
        );
        std::fs::write(work.join("a.txt"), b"hello\n").expect("write");
        git(&work, &["add", "a.txt"]);
        git(
            &work,
            &[
                "-c",
                "user.name=test",
                "-c",
                "user.email=test@example.com",
                "commit",
                "-q",
                "-m",
                "first",
            ],
        );
        git(
            &work,
            &[
                "push",
                "-q",
                origin.to_str().unwrap(),
                "HEAD:refs/heads/trunk",
            ],
        );
        (origin, work)
    }

    fn local_backend(repos_dir: &Path, repo: &str, origin: &Path) -> LocalBackend {
        LocalBackend {
            repos_dir: repos_dir.to_path_buf(),
            remote_urls: Arc::new(StaticUrls(HashMap::from([(
                repo.to_owned(),
                origin.to_str().expect("utf8").to_owned(),
            )]))),
        }
    }

    #[tokio::test]
    async fn local_backend_clones_and_fetches_from_a_remote() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let (origin, work) = source_repo(tmp.path());
        let repos_dir = tmp.path().join("repos");
        std::fs::create_dir_all(&repos_dir).expect("mkdir");
        let repo = "example.com/owner/repo";
        let backend = local_backend(&repos_dir, repo, &origin);
        let locker = RepositoryLocker::new();
        let lock = locker.try_acquire(repo, "test").expect("lock");
        let token = CancellationToken::new();

        assert!(!backend.repo_cloned(repo).await.expect("repo cloned"));

        backend.clone(repo, &lock, &token).await.expect("clone");
        assert!(backend.repo_cloned(repo).await.expect("repo cloned"));
        // Progress lines from the syncer became the lock status.
        assert_eq!(
            locker.status(repo).as_deref(),
            Some("Finished setting local HEAD to remote HEAD")
        );

        let git_dir = crate::repo::repo_dir(&repos_dir, repo).expect("git dir");
        assert_eq!(
            git_output(&git_dir, &["symbolic-ref", "HEAD"]),
            "refs/heads/trunk"
        );
        // Post-fetch bookkeeping ran: ref hash stamp, managed attributes,
        // repository type, and no leftover staging directory.
        assert!(git_dir.join("sg_refhash_v2").exists());
        assert!(git_dir.join("info").join("attributes").exists());
        assert_eq!(git_output(&git_dir, &["config", "sourcegraph.type"]), "git");
        let staging = std::fs::read_dir(repos_dir.join(".tmp")).expect("read .tmp");
        assert_eq!(staging.count(), 0, "staging directory left behind");

        // A second clone must not clobber the existing repository.
        let err = backend
            .clone(repo, &lock, &token)
            .await
            .expect_err("already cloned");
        let message = err.message();
        assert!(
            message.contains("file exists"),
            "unexpected error: {message}"
        );

        // Fetch picks up a new commit from the remote.
        std::fs::write(work.join("b.txt"), b"world\n").expect("write");
        git(&work, &["add", "b.txt"]);
        git(
            &work,
            &[
                "-c",
                "user.name=test",
                "-c",
                "user.email=test@example.com",
                "commit",
                "-q",
                "-m",
                "second",
            ],
        );
        git(
            &work,
            &[
                "push",
                "-q",
                origin.to_str().unwrap(),
                "HEAD:refs/heads/trunk",
            ],
        );

        backend.fetch(repo, &lock, &token).await.expect("fetch");
        assert_eq!(
            locker.status(repo).as_deref(),
            Some("Finished setting local HEAD to remote HEAD")
        );
        assert_eq!(
            git_output(&git_dir, &["rev-parse", "refs/heads/trunk"]),
            git_output(&origin, &["rev-parse", "refs/heads/trunk"])
        );
    }

    #[tokio::test]
    async fn local_backend_skips_the_always_cloning_repo() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let repos_dir = tmp.path().join("repos");
        std::fs::create_dir_all(&repos_dir).expect("mkdir");
        // No remote URL is registered: the special case must not look it up.
        let backend = LocalBackend {
            repos_dir: repos_dir.clone(),
            remote_urls: Arc::new(StaticUrls(HashMap::new())),
        };
        let repo = "github.com/sourcegraphtest/alwayscloningtest";
        let locker = RepositoryLocker::new();
        let lock = locker.try_acquire(repo, "test").expect("lock");
        let token = CancellationToken::new();

        backend.clone(repo, &lock, &token).await.expect("noop");
        assert!(!backend.repo_cloned(repo).await.expect("repo cloned"));
    }

    #[tokio::test]
    async fn local_backend_reports_an_unknown_remote() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let repos_dir = tmp.path().join("repos");
        std::fs::create_dir_all(&repos_dir).expect("mkdir");
        let backend = LocalBackend {
            repos_dir,
            remote_urls: Arc::new(StaticUrls(HashMap::new())),
        };
        let locker = RepositoryLocker::new();
        let lock = locker.try_acquire("example.com/x", "test").expect("lock");
        let err = backend
            .clone("example.com/x", &lock, &CancellationToken::new())
            .await
            .expect_err("unknown remote");
        assert_eq!(
            err.message(),
            "failed to get remote URL source for \"example.com/x\": no remote URL for example.com/x"
        );
    }
}
