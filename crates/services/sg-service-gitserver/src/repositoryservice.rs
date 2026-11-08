//! `gitserver.v1.GitserverRepositoryService`: the repo-updater-facing API.
//!
//! Implemented here: `ListRepositories` and `DeleteRepository` (M9.3). The
//! `FetchRepository` fetch/clone orchestration is not wired to RPCs yet and
//! keeps tonic's generated `UNIMPLEMENTED` default.
//!
//! Ported from `cmd/gitserver/internal/repositoryservice.go`.

use std::path::PathBuf;
use std::sync::Arc;

use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine;
use tonic::{Request, Response, Status};

use sg_proto::gitserver as gpb;

use crate::details::repo_not_found;
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
}

impl std::fmt::Debug for RepositoryService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RepositoryService")
            .field("repos_dir", &self.repos_dir)
            .field("hostname", &self.hostname)
            .finish_non_exhaustive()
    }
}

#[tonic::async_trait]
impl gpb::gitserver_repository_service_server::GitserverRepositoryService for RepositoryService {
    /// Fetch/clone orchestration is not wired to RPCs yet (M9.3 follow-up);
    /// return tonic's usual unimplemented status.
    async fn fetch_repository(
        &self,
        _request: Request<gpb::FetchRepositoryRequest>,
    ) -> Result<Response<gpb::FetchRepositoryResponse>, Status> {
        Err(Status::unimplemented(
            "FetchRepository is not implemented by the Rust gitserver yet",
        ))
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

    fn service(repos_dir: &std::path::Path) -> RepositoryService {
        RepositoryService {
            repos_dir: Arc::new(repos_dir.to_path_buf()),
            store: None,
            hostname: "test-shard".to_owned(),
        }
    }

    fn make_repo(repos_dir: &std::path::Path, name: &str) {
        let git_dir = repo_dir(repos_dir, name).expect("dir");
        std::fs::create_dir_all(&git_dir).expect("mkdir");
        std::fs::write(git_dir.join("HEAD"), b"ref: refs/heads/main").expect("write");
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
