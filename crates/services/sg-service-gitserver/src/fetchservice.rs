//! `Server.FetchRepository` and `Server.EnsureRevision`
//! (`cmd/gitserver/internal/server.go`, `ensurerevision.go`).
//!
//! Both the repo-updater-facing [`crate::repositoryservice`] RPC and the read
//! path (`ResolveRevision` with `ensure_revision`) share this service, like the
//! Go `Server` does.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use sqlx::types::time::OffsetDateTime;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

use sg_config::LiveConfig;
use sg_db::gitserver_repos::GitserverReposStore;

use crate::repo::{repo_dir, repo_last_changed, repo_last_fetched};
use crate::update::UpdateServer;

pub struct FetchService {
    pub repos_dir: Arc<PathBuf>,
    pub updater: Arc<UpdateServer>,
    /// The server-scoped task tracker used for background updates.
    pub tasks: TaskTracker,
    /// `s.db.GitserverRepos()`; `None` only in unit tests.
    pub store: Option<Arc<GitserverReposStore>>,
    /// The live configuration; `EnsureRevision` reads
    /// `conf.Get().DisableAutoGitUpdates` on every call, like the baseline.
    pub live: Arc<LiveConfig>,
}

impl FetchService {
    /// `Server.FetchRepository`: update-or-clone, then report the mtimes of
    /// `FETCH_HEAD` (`last_fetched`) and `sg_refhash_v2` (`last_changed`).
    ///
    /// The error text excludes the RPC-level `failed to fetch repository`
    /// wrapper, exactly like the Go method; `repositoryservice` adds it.
    pub async fn fetch_repository(&self, repo: &str) -> Result<(SystemTime, SystemTime), String> {
        // The baseline passes the caller context here; it only controls how
        // long this call waits for a clone slot, while the update itself keeps
        // running on the server context. tonic does not expose client
        // cancellation, so wait without a caller token.
        let caller_cancelled = CancellationToken::new();
        self.updater
            .repo_update_or_clone(&self.tasks, repo, &caller_cancelled)
            .await
            .map_err(|err| err.to_string())?;

        let Some(git_dir) = repo_dir(&self.repos_dir, repo) else {
            return Err(format!(
                "failed to get last fetched time: repository name {repo:?} escapes the repos dir"
            ));
        };
        let last_fetched = repo_last_fetched(&git_dir)
            .map_err(|err| format!("failed to get last fetched time: {err}"))?;
        let last_changed = repo_last_changed(&git_dir)
            .map_err(|err| format!("failed to get last changed time: {err}"))?;
        Ok((last_fetched, last_changed))
    }

    /// `Server.EnsureRevision`: fetch before a caller retries a missing
    /// revision. Returns whether the caller should retry (`true` even when the
    /// fetch failed, matching the baseline's `TODO: Shouldn't we return false
    /// here?`).
    pub async fn ensure_revision(&self, repo: &str, rev: &str) -> bool {
        if rev.is_empty() || rev == "HEAD" {
            crate::metrics::ensure_revision("HEAD");
            return false;
        }

        if self.live.current().site.disable_auto_git_updates() {
            crate::metrics::ensure_revision("disabled");
            // A fetch here is exactly what `DisableAutoGitUpdates` forbids.
            return false;
        }

        match self.fetch_repository(repo).await {
            Ok((last_fetched, last_changed)) => {
                if let Some(store) = &self.store {
                    if let Err(err) = store
                        .set_last_fetched(
                            repo,
                            offset_datetime(last_fetched),
                            offset_datetime(last_changed),
                        )
                        .await
                    {
                        tracing::error!(
                            repo = %repo,
                            error = %err,
                            "failed to store repo update timestamps"
                        );
                    }
                }
                crate::metrics::ensure_revision("updated");
            }
            Err(err) => {
                // The baseline only counts failures when the caller is still
                // waiting; the Rust layer has no caller context to check.
                crate::metrics::ensure_revision("update_failed");
                tracing::warn!(
                    repo = %repo,
                    rev = %rev,
                    error = %err,
                    "failed to perform background repo update"
                );
            }
        }
        true
    }
}

/// `time.Time` → `OffsetDateTime` with nanosecond precision (`timestamppb`-like
/// handling of times before the Unix epoch).
fn offset_datetime(time: SystemTime) -> OffsetDateTime {
    let nanos = match time.duration_since(UNIX_EPOCH) {
        Ok(elapsed) => elapsed.as_secs() as i128 * 1_000_000_000 + elapsed.subsec_nanos() as i128,
        Err(err) => {
            let before = err.duration();
            -(before.as_secs() as i128 * 1_000_000_000 + before.subsec_nanos() as i128)
        }
    };
    OffsetDateTime::from_unix_timestamp_nanos(nanos).unwrap_or_else(|_| OffsetDateTime::now_utc())
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;

    use crate::git::GitError;
    use crate::locker::RepositoryLock;
    use crate::update::UpdateBackend;

    /// Creates `<gitdir>/HEAD` on clone, like a successful `git clone`.
    struct CloningBackend {
        repos_dir: std::path::PathBuf,
        fail: bool,
    }

    #[async_trait]
    impl UpdateBackend for CloningBackend {
        async fn repo_cloned(&self, repo: &str) -> Result<bool, GitError> {
            Ok(crate::repo::repo_dir(&self.repos_dir, repo)
                .map(|dir| dir.join("HEAD").exists())
                .unwrap_or(false))
        }

        async fn clone(
            &self,
            repo: &str,
            _lock: &RepositoryLock,
            _cancelled: &CancellationToken,
        ) -> Result<(), GitError> {
            if self.fail {
                return Err(GitError::Message("clone failed".to_owned()));
            }
            let dir = crate::repo::repo_dir(&self.repos_dir, repo).expect("repo dir");
            std::fs::create_dir_all(&dir).expect("mkdir");
            std::fs::write(dir.join("HEAD"), b"ref: refs/heads/main").expect("HEAD");
            Ok(())
        }

        async fn fetch(
            &self,
            _repo: &str,
            _lock: &RepositoryLock,
            _cancelled: &CancellationToken,
        ) -> Result<(), GitError> {
            Ok(())
        }

        async fn set_last_error(&self, _repo: &str, _error: &str) {}
    }

    fn service(repos_dir: &std::path::Path, fail: bool, disable: bool) -> FetchService {
        FetchService {
            repos_dir: Arc::new(repos_dir.to_path_buf()),
            updater: Arc::new(UpdateServer {
                locker: crate::locker::RepositoryLocker::new(),
                limiter: crate::limiter::MutableLimiter::new(5),
                backend: Arc::new(CloningBackend {
                    repos_dir: repos_dir.to_path_buf(),
                    fail,
                }),
                cancelled: CancellationToken::new(),
            }),
            tasks: TaskTracker::new(),
            store: None,
            live: live_config(disable),
        }
    }

    fn live_config(disable: bool) -> Arc<LiveConfig> {
        let raw = sg_config::RawUnified {
            site: serde_json::json!({ "disableAutoGitUpdates": disable }).to_string(),
            ..Default::default()
        };
        Arc::new(LiveConfig::new(
            sg_config::Unified::parse(&raw).expect("site config"),
        ))
    }

    async fn shutdown(service: &FetchService) {
        service.tasks.close();
        service.tasks.wait().await;
    }

    #[tokio::test]
    async fn skips_head_and_empty_revisions() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let service = service(tmp.path(), false, false);
        assert!(!service.ensure_revision("example.com/o/r", "HEAD").await);
        assert!(!service.ensure_revision("example.com/o/r", "").await);
        // Neither call fetched anything.
        assert!(!crate::repo::repo_dir(tmp.path(), "example.com/o/r")
            .expect("dir")
            .join("HEAD")
            .exists());
        shutdown(&service).await;
    }

    #[tokio::test]
    async fn does_not_fetch_when_auto_updates_are_disabled() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let service = service(tmp.path(), false, true);
        assert!(!service.ensure_revision("example.com/o/r", "abc123").await);
        assert!(!crate::repo::repo_dir(tmp.path(), "example.com/o/r")
            .expect("dir")
            .join("HEAD")
            .exists());
        shutdown(&service).await;
    }

    #[tokio::test]
    async fn asks_the_caller_to_retry_after_a_failed_fetch() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let service = service(tmp.path(), true, false);
        // The baseline returns `true` even when the fetch failed.
        assert!(service.ensure_revision("example.com/o/r", "abc123").await);
        shutdown(&service).await;
    }

    #[tokio::test]
    async fn fetches_missing_revisions() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let service = service(tmp.path(), false, false);
        assert!(service.ensure_revision("example.com/o/r", "abc123").await);
        assert!(crate::repo::repo_dir(tmp.path(), "example.com/o/r")
            .expect("dir")
            .join("HEAD")
            .exists());
        shutdown(&service).await;
    }

    #[test]
    fn converts_system_times_to_offset_datetimes() {
        let epoch = UNIX_EPOCH;
        assert_eq!(offset_datetime(epoch).unix_timestamp_nanos(), 0);

        let after = epoch + std::time::Duration::new(1_700_000_000, 123);
        let converted = offset_datetime(after);
        assert_eq!(converted.unix_timestamp_nanos(), 1_700_000_000_000_000_123);

        let before = epoch - std::time::Duration::new(1, 500_000_000);
        let converted = offset_datetime(before);
        assert_eq!(converted.unix_timestamp_nanos(), -1_500_000_000);
    }
}
