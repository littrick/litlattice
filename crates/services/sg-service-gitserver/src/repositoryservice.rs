//! `gitserver.v1.GitserverRepositoryService`: the repo-updater-facing API.
//!
//! Ported from `cmd/gitserver/internal/repositoryservice.go`.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine;
use tonic::{Request, Response, Status};

use sg_proto::gitserver as gpb;

use crate::details::repo_not_found;
use crate::fetchservice::FetchService;
use crate::fs::{canonical_path, for_each_repo, remove_repo_directory, repo_cloned};
use crate::repo::repo_dir;
use sg_db::gitserver_repos::{CloneStatus, GitserverReposStore};

pub struct RepositoryService {
    pub repos_dir: Arc<PathBuf>,
    /// `s.db.GitserverRepos()`. `None` only when the process was started
    /// without a database (the baseline always has one and `DeleteRepository`
    /// reports an internal error in that case).
    pub store: Option<Arc<GitserverReposStore>>,
    /// `s.hostname`: the `gitserver_repos.shard_id` written on deletion.
    pub hostname: String,
    /// `s.svc`: the fetch/clone orchestration (`Server.FetchRepository`).
    /// `None` only in unit tests that do not exercise fetching.
    pub fetch: Option<Arc<FetchService>>,
}

impl std::fmt::Debug for RepositoryService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RepositoryService")
            .field("repos_dir", &self.repos_dir)
            .field("hostname", &self.hostname)
            .finish_non_exhaustive()
    }
}

/// Go `timestamppb.New(time.Time)`: seconds floored at the Unix epoch and a
/// non-negative nanosecond remainder.
fn timestamp(time: SystemTime) -> prost_types::Timestamp {
    match time.duration_since(UNIX_EPOCH) {
        Ok(elapsed) => prost_types::Timestamp {
            seconds: elapsed.as_secs() as i64,
            nanos: elapsed.subsec_nanos() as i32,
        },
        Err(err) => {
            let before = err.duration();
            let nanos = before.subsec_nanos();
            if nanos == 0 {
                prost_types::Timestamp {
                    seconds: -(before.as_secs() as i64),
                    nanos: 0,
                }
            } else {
                prost_types::Timestamp {
                    seconds: -(before.as_secs() as i64) - 1,
                    nanos: (1_000_000_000 - nanos) as i32,
                }
            }
        }
    }
}

#[tonic::async_trait]
impl gpb::gitserver_repository_service_server::GitserverRepositoryService for RepositoryService {
    /// `Server.FetchRepository`: update-or-clone the repository, then report
    /// the mtimes of `FETCH_HEAD` (`last_fetched`) and `sg_refhash_v2`
    /// (`last_changed`).
    async fn fetch_repository(
        &self,
        request: Request<gpb::FetchRepositoryRequest>,
    ) -> Result<Response<gpb::FetchRepositoryResponse>, Status> {
        let repo_name = request.into_inner().repo_name;
        if repo_name.is_empty() {
            return Err(Status::invalid_argument("repo_name must be specified"));
        }

        let Some(fetch) = &self.fetch else {
            return Err(Status::internal(
                "failed to fetch repository: the update server is not configured",
            ));
        };
        let (last_fetched, last_changed) = fetch
            .fetch_repository(&repo_name)
            .await
            .map_err(|err| Status::internal(format!("failed to fetch repository: {err}")))?;

        Ok(Response::new(gpb::FetchRepositoryResponse {
            last_fetched: Some(timestamp(last_fetched)),
            last_changed: Some(timestamp(last_changed)),
        }))
    }

    async fn delete_repository(
        &self,
        request: Request<gpb::DeleteRepositoryRequest>,
    ) -> Result<Response<gpb::DeleteRepositoryResponse>, Status> {
        let repo_name = request.into_inner().repo_name;
        if repo_name.is_empty() {
            return Err(Status::invalid_argument("repo_name must be specified"));
        }

        // `s.fs.RepoCloned`: an unreadable repository reports an internal
        // error instead of "not found".
        let cloned = repo_cloned(&self.repos_dir, &repo_name)
            .map_err(|_| Status::internal("failed to determine clone status"))?;
        if !cloned {
            return Err(repo_not_found(&repo_name));
        }

        let Some(git_dir) = repo_dir(&self.repos_dir, &repo_name) else {
            return Err(repo_not_found(&repo_name));
        };
        if let Err(err) = remove_repo_directory(&self.repos_dir, &git_dir) {
            tracing::error!(
                repo = %repo_name,
                error = %err,
                "failed to delete repository"
            );
            return Err(Status::internal(format!(
                "failed to delete repository {repo_name}: removing repo directory: {err}"
            )));
        }

        let Some(store) = &self.store else {
            return Err(Status::internal(format!(
                "failed to delete repository {repo_name}: database is not configured"
            )));
        };
        if let Err(err) = store
            .set_clone_status(&repo_name, CloneStatus::NotCloned, &self.hostname)
            .await
        {
            let err = format!("setting clone status after delete: {err}");
            tracing::error!(repo = %repo_name, error = %err, "failed to delete repository");
            return Err(Status::internal(format!(
                "failed to delete repository {repo_name}: {err}"
            )));
        }

        tracing::info!(repo = %repo_name, "repository deleted");
        Ok(Response::new(gpb::DeleteRepositoryResponse {}))
    }

    async fn list_repositories(
        &self,
        request: Request<gpb::ListRepositoriesRequest>,
    ) -> Result<Response<gpb::ListRepositoriesResponse>, Status> {
        let request = request.into_inner();
        if request.page_size == 0 {
            return Err(Status::invalid_argument("page_size must be > 0"));
        }
        let token = BASE64
            .decode(&request.page_token)
            .map_err(|_| Status::invalid_argument("invalid page_token"))?;
        let token = String::from_utf8_lossy(&token).into_owned();

        let mut repos: Vec<gpb::list_repositories_response::GitRepository> =
            Vec::with_capacity(request.page_size as usize);
        let mut next_cursor = String::new();
        let mut at_start = false;
        for_each_repo(&self.repos_dir, |name, git_dir| {
            if !at_start && !token.is_empty() && git_dir.to_string_lossy() != token {
                return false;
            }
            at_start = true;

            if repos.len() > request.page_size as usize - 1 {
                next_cursor = BASE64.encode(git_dir.to_string_lossy().as_bytes());
                return true;
            }

            repos.push(gpb::list_repositories_response::GitRepository {
                path: canonical_path(&self.repos_dir, git_dir).into_bytes(),
                name: name.to_owned(),
            });
            false
        });

        Ok(Response::new(gpb::ListRepositoriesResponse {
            repositories: repos,
            next_page_token: next_cursor,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use gpb::gitserver_repository_service_server::GitserverRepositoryService as _;
    use tokio_util::sync::CancellationToken;
    use tokio_util::task::TaskTracker;

    fn service(repos_dir: &std::path::Path) -> RepositoryService {
        RepositoryService {
            repos_dir: Arc::new(repos_dir.to_path_buf()),
            store: None,
            hostname: "test-shard".to_owned(),
            fetch: None,
        }
    }

    fn make_repo(repos_dir: &std::path::Path, name: &str) {
        let git_dir = repo_dir(repos_dir, name).expect("dir");
        std::fs::create_dir_all(&git_dir).expect("mkdir");
        std::fs::write(git_dir.join("HEAD"), b"ref: refs/heads/main").expect("write");
    }

    #[tokio::test]
    async fn fetch_requires_a_repo_name() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let svc = service(tmp.path());
        let err = svc
            .fetch_repository(Request::new(gpb::FetchRepositoryRequest {
                repo_name: String::new(),
            }))
            .await
            .expect_err("empty name");
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
        assert_eq!(err.message(), "repo_name must be specified");
    }

    #[tokio::test]
    async fn fetch_without_an_update_server_is_internal() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let svc = service(tmp.path());
        let err = svc
            .fetch_repository(Request::new(gpb::FetchRepositoryRequest {
                repo_name: "example.com/owner/repo".to_owned(),
            }))
            .await
            .expect_err("no updater");
        assert_eq!(err.code(), tonic::Code::Internal);
        assert_eq!(
            err.message(),
            "failed to fetch repository: the update server is not configured"
        );
    }

    /// Clones `HEAD` into the repos dir (like `git clone`) and then serves the
    /// `repoLastFetched`/`repoLastChanged` mtimes.
    struct CloneBackend {
        repos_dir: PathBuf,
    }

    #[async_trait::async_trait]
    impl crate::update::UpdateBackend for CloneBackend {
        async fn repo_cloned(&self, repo: &str) -> Result<bool, crate::git::GitError> {
            crate::fs::repo_cloned(&self.repos_dir, repo)
                .map_err(|err| crate::git::GitError::Message(err.to_string()))
        }

        async fn clone(
            &self,
            repo: &str,
            _lock: &crate::locker::RepositoryLock,
            _cancelled: &CancellationToken,
        ) -> Result<(), crate::git::GitError> {
            let git_dir = repo_dir(&self.repos_dir, repo).expect("repo dir");
            std::fs::create_dir_all(&git_dir).expect("mkdir");
            std::fs::write(git_dir.join("HEAD"), b"ref: refs/heads/main").expect("write HEAD");
            Ok(())
        }

        async fn fetch(
            &self,
            _repo: &str,
            _lock: &crate::locker::RepositoryLock,
            _cancelled: &CancellationToken,
        ) -> Result<(), crate::git::GitError> {
            Ok(())
        }

        async fn set_last_error(&self, _repo: &str, _error: &str) {}
    }

    fn service_with_updater(repos_dir: &std::path::Path) -> RepositoryService {
        let mut svc = service(repos_dir);
        let tasks = TaskTracker::new();
        svc.fetch = Some(Arc::new(crate::fetchservice::FetchService {
            repos_dir: Arc::new(repos_dir.to_path_buf()),
            updater: Arc::new(crate::update::UpdateServer {
                locker: crate::locker::RepositoryLocker::new(),
                limiter: crate::limiter::MutableLimiter::new(5),
                backend: Arc::new(CloneBackend {
                    repos_dir: repos_dir.to_path_buf(),
                }),
                cancelled: CancellationToken::new(),
            }),
            tasks,
            store: None,
            disable_auto_git_updates: false,
        }));
        svc
    }

    #[tokio::test]
    async fn fetch_clones_and_reports_the_last_fetched_and_changed_times() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let repos_dir = tmp.path().join("repos");
        std::fs::create_dir_all(&repos_dir).expect("mkdir");
        let svc = service_with_updater(&repos_dir);

        let repo = "example.com/owner/repo";
        let response = svc
            .fetch_repository(Request::new(gpb::FetchRepositoryRequest {
                repo_name: repo.to_owned(),
            }))
            .await
            .expect("fetch")
            .into_inner();
        let last_fetched = response.last_fetched.expect("last_fetched");
        let last_changed = response.last_changed.expect("last_changed");
        assert!(last_fetched.seconds > 0);
        // Without `sg_refhash_v2`, `last_changed` falls back to
        // `repoLastFetched`.
        assert_eq!(last_fetched, last_changed);

        // A `sg_refhash_v2` file written later becomes the changed time.
        let refhash = repo_dir(&repos_dir, repo)
            .expect("dir")
            .join("sg_refhash_v2");
        std::fs::write(&refhash, b"refhash").expect("write refhash");
        let later = SystemTime::now() + std::time::Duration::from_secs(60);
        std::fs::File::options()
            .write(true)
            .open(&refhash)
            .expect("open refhash")
            .set_modified(later)
            .expect("set mtime");

        let response = svc
            .fetch_repository(Request::new(gpb::FetchRepositoryRequest {
                repo_name: repo.to_owned(),
            }))
            .await
            .expect("fetch")
            .into_inner();
        assert_eq!(response.last_fetched.expect("last_fetched"), last_fetched);
        let changed = response.last_changed.expect("last_changed");
        assert!(changed.seconds > last_fetched.seconds);

        if let Some(fetch) = &svc.fetch {
            fetch.tasks.close();
            fetch.tasks.wait().await;
        }
    }

    #[tokio::test]
    async fn lists_repositories_in_walk_order_with_pagination() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let repos_dir = tmp.path();
        make_repo(repos_dir, "example.com/b/repo");
        make_repo(repos_dir, "example.com/a/repo");
        // `.tmp` and `.p4home` are ignored, as are plain directories without
        // a `.git` child.
        std::fs::create_dir_all(repos_dir.join(".tmp/clone-1")).expect("tmp");
        std::fs::create_dir_all(repos_dir.join(".p4home/x")).expect("p4");
        std::fs::create_dir_all(repos_dir.join("example.com/not-a-repo")).expect("dir");

        let svc = service(repos_dir);
        let response = svc
            .list_repositories(Request::new(gpb::ListRepositoriesRequest {
                page_size: 10,
                page_token: String::new(),
            }))
            .await
            .expect("list")
            .into_inner();

        let names: Vec<_> = response
            .repositories
            .iter()
            .map(|repo| repo.name.as_str())
            .collect();
        assert_eq!(names, ["example.com/a/repo", "example.com/b/repo"]);
        assert_eq!(
            String::from_utf8(response.repositories[0].path.clone()).expect("utf8"),
            "/example.com/a/repo/.git"
        );
        assert!(response.next_page_token.is_empty());

        // A page of one returns the first repo and a token pointing at the
        // next `.git` directory.
        let first = svc
            .list_repositories(Request::new(gpb::ListRepositoriesRequest {
                page_size: 1,
                page_token: String::new(),
            }))
            .await
            .expect("list")
            .into_inner();
        assert_eq!(first.repositories.len(), 1);
        assert_eq!(first.repositories[0].name, "example.com/a/repo");
        let cursor = BASE64.decode(&first.next_page_token).expect("token");
        assert_eq!(
            String::from_utf8(cursor).expect("utf8"),
            repos_dir.join("example.com/b/repo/.git").to_string_lossy()
        );

        let second = svc
            .list_repositories(Request::new(gpb::ListRepositoriesRequest {
                page_size: 1,
                page_token: first.next_page_token.clone(),
            }))
            .await
            .expect("list")
            .into_inner();
        assert_eq!(second.repositories.len(), 1);
        assert_eq!(second.repositories[0].name, "example.com/b/repo");
        // The last page has no cursor.
        assert!(second.next_page_token.is_empty());
    }

    #[tokio::test]
    async fn rejects_invalid_list_requests() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let svc = service(tmp.path());

        let err = svc
            .list_repositories(Request::new(gpb::ListRepositoriesRequest {
                page_size: 0,
                page_token: String::new(),
            }))
            .await
            .expect_err("page size");
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
        assert_eq!(err.message(), "page_size must be > 0");

        let err = svc
            .list_repositories(Request::new(gpb::ListRepositoriesRequest {
                page_size: 1,
                page_token: "!!!".to_owned(),
            }))
            .await
            .expect_err("token");
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
        assert_eq!(err.message(), "invalid page_token");
    }

    #[tokio::test]
    async fn delete_requires_a_repo_name_and_an_existing_repo() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let repos_dir = tmp.path();
        let svc = service(repos_dir);

        let err = svc
            .delete_repository(Request::new(gpb::DeleteRepositoryRequest {
                repo_name: String::new(),
            }))
            .await
            .expect_err("empty name");
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
        assert_eq!(err.message(), "repo_name must be specified");

        let err = svc
            .delete_repository(Request::new(gpb::DeleteRepositoryRequest {
                repo_name: "example.com/owner/missing".to_owned(),
            }))
            .await
            .expect_err("missing");
        assert_eq!(err.code(), tonic::Code::NotFound);
    }

    #[tokio::test]
    async fn delete_removes_the_repository_and_empty_parents() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let repos_dir = tmp.path().join("repos");
        std::fs::create_dir_all(&repos_dir).expect("mkdir");
        make_repo(&repos_dir, "example.com/owner/repo");
        let svc = service(&repos_dir);

        // Without a store the filesystem part still runs; the missing
        // database is reported afterwards.
        let err = svc
            .delete_repository(Request::new(gpb::DeleteRepositoryRequest {
                repo_name: "example.com/owner/repo".to_owned(),
            }))
            .await
            .expect_err("no database");
        assert!(err.message().contains("database is not configured"));

        assert!(!repo_dir(&repos_dir, "example.com/owner/repo")
            .expect("dir")
            .exists());
        // Parent directories are removed up to (not including) the repos dir.
        assert!(!repos_dir.join("example.com/owner").exists());
        assert!(!repos_dir.join("example.com").exists());
        assert!(repos_dir.exists());
    }

    /// Exercises the gRPC surface against a running gitserver when
    /// `SG_TEST_GITSERVER_ADDR` is set (e.g. `127.0.0.1:13178`).
    #[tokio::test]
    async fn lists_repositories_from_a_running_gitserver() {
        let Ok(addr) = std::env::var("SG_TEST_GITSERVER_ADDR") else {
            eprintln!("SG_TEST_GITSERVER_ADDR unset; skipping");
            return;
        };
        let mut client =
            gpb::gitserver_repository_service_client::GitserverRepositoryServiceClient::connect(
                format!("http://{addr}"),
            )
            .await
            .expect("connect");

        let response = client
            .list_repositories(gpb::ListRepositoriesRequest {
                page_size: 100,
                page_token: String::new(),
            })
            .await
            .expect("list")
            .into_inner();
        assert!(
            response
                .repositories
                .iter()
                .any(|repo| repo.name == "test/symbols-fixture"),
            "missing fixture repo in {:?}",
            response
                .repositories
                .iter()
                .map(|repo| &repo.name)
                .collect::<Vec<_>>()
        );

        let err = client
            .list_repositories(gpb::ListRepositoriesRequest {
                page_size: 0,
                page_token: String::new(),
            })
            .await
            .expect_err("page size");
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
    }

    /// End-to-end `FetchRepository` against a running gitserver: the repo and
    /// `OTHER` external service are seeded in Postgres, and the clone URL is a
    /// `file://` URL to a fresh local origin. Set `SG_TEST_GITSERVER_ADDR`
    /// (and `SG_TEST_DATABASE_DSN`) to run it against Go (:3178) and Rust
    /// (:13178) for differential results.
    #[tokio::test]
    async fn fetches_repositories_from_a_running_gitserver() {
        let (Ok(addr), Ok(dsn)) = (
            std::env::var("SG_TEST_GITSERVER_ADDR"),
            std::env::var("SG_TEST_DATABASE_DSN"),
        ) else {
            eprintln!("SG_TEST_GITSERVER_ADDR/SG_TEST_DATABASE_DSN unset; skipping");
            return;
        };
        let mut client =
            gpb::gitserver_repository_service_client::GitserverRepositoryServiceClient::connect(
                format!("http://{addr}"),
            )
            .await
            .expect("connect");

        let tmp = tempfile::tempdir().expect("tempdir");
        let origin = tmp.path().join("origin");
        std::fs::create_dir_all(&origin).expect("mkdir origin");
        for command in [
            vec!["init", "--quiet", "--initial-branch=main"],
            vec!["config", "user.name", "Test"],
            vec!["config", "user.email", "test@example.com"],
        ] {
            let status = std::process::Command::new("git")
                .args(&command)
                .current_dir(&origin)
                .status()
                .expect("git");
            assert!(status.success(), "git {command:?}");
        }
        std::fs::write(origin.join("README.md"), b"hello\n").expect("write");
        for command in [
            vec!["add", "README.md"],
            vec!["commit", "--quiet", "-m", "initial"],
        ] {
            let status = std::process::Command::new("git")
                .args(&command)
                .current_dir(&origin)
                .status()
                .expect("git");
            assert!(status.success(), "git {command:?}");
        }

        let pool = sg_db::connect(&dsn, 2).await.expect("connect");
        let name = format!(
            "gitserver-fetch-{}.example.com/owner/repo",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock")
                .as_nanos()
        );
        let service_id: i64 = sqlx::query_scalar(
            "INSERT INTO external_services(kind, display_name, config) \
             VALUES ('OTHER', 'other', '{\"url\": \"file:///tmp\", \"repos\": []}') \
             RETURNING id",
        )
        .fetch_one(&pool)
        .await
        .expect("insert service");
        let repo_id: i32 = sqlx::query_scalar(
            "INSERT INTO repo(name, external_service_type, external_service_id, metadata) \
             VALUES ($1, 'other', $2, '{\"RelativePath\": \"\"}') RETURNING id",
        )
        .bind(&name)
        .bind(format!("file://{}", origin.display()))
        .fetch_one(&pool)
        .await
        .expect("insert repo");
        sqlx::query(
            "INSERT INTO external_service_repos(external_service_id, repo_id, clone_url) \
             VALUES ($1, $2, $3)",
        )
        .bind(service_id)
        .bind(repo_id)
        .bind(format!("file://{}", origin.display()))
        .execute(&pool)
        .await
        .expect("insert source");

        let response = client
            .fetch_repository(gpb::FetchRepositoryRequest {
                repo_name: name.clone(),
            })
            .await
            .expect("fetch")
            .into_inner();
        let last_fetched = response.last_fetched.expect("last_fetched");
        let last_changed = response.last_changed.expect("last_changed");
        // `last_changed` is the mtime of `sg_refhash_v2`, which for a freshly
        // cloned repo is the *latest commit timestamp* (set by
        // `setLastChanged`), so it is not necessarily after `last_fetched`.
        assert!(last_fetched.seconds > 0, "{last_fetched:?}");
        assert!(last_changed.seconds > 0, "{last_changed:?}");

        // A missing remote is reported as an internal error whose message
        // carries the whole `failed to fetch repository` chain.
        let missing = format!("{}-missing.example.com/repo", name);
        sqlx::query("INSERT INTO repo(name) VALUES ($1)")
            .bind(&missing)
            .execute(&pool)
            .await
            .expect("insert missing repo");
        let err = client
            .fetch_repository(gpb::FetchRepositoryRequest {
                repo_name: missing.clone(),
            })
            .await
            .expect_err("missing");
        assert_eq!(err.code(), tonic::Code::Internal);
        assert!(
            err.message().starts_with(&format!(
                "failed to fetch repository: failed to clone {missing}: clone failed. Output:"
            )),
            "{}",
            err.message()
        );
        assert!(
            err.message().contains("no sources for"),
            "{}",
            err.message()
        );

        sqlx::query("DELETE FROM external_service_repos WHERE repo_id = $1")
            .bind(repo_id)
            .execute(&pool)
            .await
            .expect("cleanup sources");
        sqlx::query("DELETE FROM repo WHERE name = ANY($1)")
            .bind(vec![name.clone(), missing.clone()])
            .execute(&pool)
            .await
            .expect("cleanup repos");
        sqlx::query("DELETE FROM external_services WHERE id = $1")
            .bind(service_id)
            .execute(&pool)
            .await
            .expect("cleanup service");
        // The server cloned into its own repos dir; remove it when the
        // harness tells us where that is.
        if let Ok(repos_dir) = std::env::var("SG_TEST_REPOS_DIR") {
            if let Some(git_dir) = repo_dir(std::path::Path::new(&repos_dir), &name) {
                let _ = std::fs::remove_dir_all(git_dir.parent().unwrap_or(&git_dir));
            }
        }
    }

    /// Exercises the `gitserver_repos` write of `DeleteRepository` against the
    /// local baseline database when `SG_TEST_DATABASE_DSN` is set.
    #[tokio::test]
    async fn delete_persists_not_cloned_when_a_database_is_configured() {
        let Ok(dsn) = std::env::var("SG_TEST_DATABASE_DSN") else {
            eprintln!("SG_TEST_DATABASE_DSN unset; skipping");
            return;
        };
        let pool = sg_db::connect(&dsn, 2).await.expect("connect");
        let store = Arc::new(GitserverReposStore::new(pool.clone()));

        let name = format!(
            "gitserver-delete-{}.example.com/repo",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock")
                .as_nanos()
        );
        sqlx::query("INSERT INTO repo(name) VALUES ($1)")
            .bind(&name)
            .execute(store.pool())
            .await
            .expect("insert repo");

        let tmp = tempfile::tempdir().expect("tempdir");
        let repos_dir = tmp.path().join("repos");
        std::fs::create_dir_all(&repos_dir).expect("mkdir");
        make_repo(&repos_dir, &name);
        // The clone status written by a previous clone on this shard.
        store
            .set_clone_status(&name, CloneStatus::Cloned, "test-shard")
            .await
            .expect("seed status");

        let svc = RepositoryService {
            repos_dir: Arc::new(repos_dir.clone()),
            store: Some(Arc::clone(&store)),
            hostname: "test-shard".to_owned(),
            fetch: None,
        };
        svc.delete_repository(Request::new(gpb::DeleteRepositoryRequest {
            repo_name: name.clone(),
        }))
        .await
        .expect("delete");

        let repo = store.get_by_name(&name).await.expect("get").expect("row");
        assert_eq!(repo.clone_status, CloneStatus::NotCloned);

        sqlx::query("DELETE FROM repo WHERE name = $1")
            .bind(&name)
            .execute(store.pool())
            .await
            .expect("cleanup");
    }
}
