//! Port of `shared.getRemoteURLFunc` plus the `getRemoteURLSource` closure
//! from `shared.makeServer`: look the repository up in Postgres, decrypt its
//! first external service's configuration, and compute the clone URL with
//! [`crate::cloneurl`].
//!
//! The baseline wraps failures of this function with
//! `getting remote URL for %q`; that wrapper lives here so
//! [`crate::update::LocalBackend`] stays source-agnostic.

use std::sync::Arc;

use crate::cloneurl;
use crate::git::GitError;
use crate::update::RemoteUrlSource;
use async_trait::async_trait;
use sg_db::external_services::ExternalServiceStore;
use sg_db::repos::RepoStore;
use sg_keyring::Keyring;

/// `db.Repos()` / `db.ExternalServices()` backed [`RemoteUrlSource`].
pub struct DbRemoteUrlSource {
    pub repos: Arc<RepoStore>,
    pub external_services: Arc<ExternalServiceStore>,
    /// `keyring.Default()`: `getEncryptionKey` reads the current ring for
    /// every lookup, so a rebuilt keyring applies to subsequent reads.
    pub keyring: Arc<Keyring>,
}

impl DbRemoteUrlSource {
    /// `getRemoteURLFunc`: lookup errors are returned unwrapped (the caller
    /// adds `getting remote URL for %q`), matching the baseline.
    async fn lookup(&self, repo: &str) -> Result<String, String> {
        let stored = match self.repos.get_by_name(repo).await {
            Ok(Some(stored)) => stored,
            Ok(None) => return Err(format!("repo not found: name={repo:?}")),
            Err(err) => return Err(err.to_string()),
        };

        // `GetByName` returns the repo and `repo.IsBlocked()`'s error; the
        // caller (getRemoteURLFunc) returns the error.
        if let Some(blocked) = &stored.blocked {
            let reason = blocked
                .get("reason")
                .and_then(serde_json::Value::as_str)
                .unwrap_or_default();
            return Err(format!(
                "repository {} has been blocked. reason: {}",
                stored.name, reason
            ));
        }

        let Some(source) = stored.sources.first() else {
            return Err(format!("no sources for {repo:?}"));
        };

        let service = self
            .external_services
            .get_by_id(source.external_service_id)
            .await
            .map_err(|err| err.to_string())?;

        let key = self.keyring.external_service_key();
        cloneurl::for_encryptable_config(
            &service.kind,
            &service.config,
            &service.encryption_key_id,
            key.as_deref(),
            &stored,
        )
        .await
    }
}

#[async_trait]
impl RemoteUrlSource for DbRemoteUrlSource {
    /// `GetRemoteURLFunc`: the raw lookup. `Server.getRemoteURL` adds the
    /// `GetRemoteURLFunc` wrapper itself.
    async fn remote_url_func(&self, repo: &str) -> Result<String, GitError> {
        self.lookup(repo).await.map_err(GitError::Message)
    }

    async fn remote_url(&self, repo: &str) -> Result<String, GitError> {
        self.lookup(repo)
            .await
            .map_err(|err| GitError::Message(format!("getting remote URL for {repo:?}: {err}")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn reports_missing_repos_with_the_baseline_message() {
        let Ok(dsn) = std::env::var("SG_TEST_DATABASE_DSN") else {
            eprintln!("SG_TEST_DATABASE_DSN unset; skipping");
            return;
        };
        let pool = sg_db::connect(&dsn, 2).await.expect("connect");
        let source = DbRemoteUrlSource {
            repos: Arc::new(RepoStore::new(pool.clone())),
            external_services: Arc::new(ExternalServiceStore::new(pool)),
            keyring: Arc::new(Keyring::default()),
        };
        let err = source
            .remote_url("missing.example.com/repo")
            .await
            .expect_err("missing");
        assert_eq!(
            err.message(),
            "getting remote URL for \"missing.example.com/repo\": \
             repo not found: name=\"missing.example.com/repo\""
        );
    }

    /// Mirrors `getRemoteURLFunc` for an `OTHER` external service:
    /// `otherCloneURL` concatenates the repo's `ExternalRepo.ServiceID` with
    /// the metadata's `RelativePath` (in a real sync the service ID is the
    /// service's configured URL).
    #[tokio::test]
    async fn resolves_an_other_external_service_from_the_database() {
        let Ok(dsn) = std::env::var("SG_TEST_DATABASE_DSN") else {
            eprintln!("SG_TEST_DATABASE_DSN unset; skipping");
            return;
        };
        let pool = sg_db::connect(&dsn, 2).await.expect("connect");
        let repos = Arc::new(RepoStore::new(pool.clone()));
        let external_services = Arc::new(ExternalServiceStore::new(pool.clone()));
        let source = DbRemoteUrlSource {
            repos: repos.clone(),
            external_services,
            keyring: Arc::new(Keyring::default()),
        };

        let name = format!(
            "remote-url-{}.example.com/owner/repo",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock")
                .as_nanos()
        );
        let service_id: i64 = sqlx::query_scalar(
            "INSERT INTO external_services(kind, display_name, config) \
             VALUES ('OTHER', 'other', '{\"url\": \"https://example.com\", \"repos\": []}') \
             RETURNING id",
        )
        .fetch_one(&pool)
        .await
        .expect("insert service");
        let repo_id: i32 = sqlx::query_scalar(
            "INSERT INTO repo(name, external_service_type, external_service_id, metadata) \
             VALUES ($1, 'other', $2, '{\"RelativePath\": \"owner/repo\"}') RETURNING id",
        )
        .bind(&name)
        .bind(service_id.to_string())
        .fetch_one(&pool)
        .await
        .expect("insert repo");
        sqlx::query(
            "INSERT INTO external_service_repos(external_service_id, repo_id, clone_url) \
             VALUES ($1, $2, 'https://example.com/owner/repo')",
        )
        .bind(service_id)
        .bind(repo_id)
        .execute(&pool)
        .await
        .expect("insert source");

        let want = format!("{service_id}owner/repo");
        let url = source.remote_url(&name).await.expect("remote url");
        assert_eq!(url, want);

        // `GetByName` also matches the `uri` column.
        sqlx::query("UPDATE repo SET uri = $2 WHERE id = $1")
            .bind(repo_id)
            .bind(format!("{name}-uri"))
            .execute(&pool)
            .await
            .expect("set uri");
        let url = source
            .remote_url(&format!("{name}-uri"))
            .await
            .expect("remote url by uri");
        assert_eq!(url, want);

        // A blocked repo reports the baseline's BlockedRepoError text.
        sqlx::query(r#"UPDATE repo SET blocked = '{"reason": "dmca"}' WHERE id = $1"#)
            .bind(repo_id)
            .execute(&pool)
            .await
            .expect("block");
        let err = source.remote_url(&name).await.expect_err("blocked");
        assert_eq!(
            err.message(),
            format!(
                "getting remote URL for {name:?}: repository {name} has been blocked. reason: dmca"
            )
        );

        sqlx::query("DELETE FROM external_services WHERE id = $1")
            .bind(service_id)
            .execute(&pool)
            .await
            .expect("cleanup service");
        sqlx::query("DELETE FROM repo WHERE id = $1")
            .bind(repo_id)
            .execute(&pool)
            .await
            .expect("cleanup repo");
    }

    /// A repo without external service sources reports `no sources for %q`.
    #[tokio::test]
    async fn reports_repos_without_sources() {
        let Ok(dsn) = std::env::var("SG_TEST_DATABASE_DSN") else {
            eprintln!("SG_TEST_DATABASE_DSN unset; skipping");
            return;
        };
        let pool = sg_db::connect(&dsn, 2).await.expect("connect");
        let source = DbRemoteUrlSource {
            repos: Arc::new(RepoStore::new(pool.clone())),
            external_services: Arc::new(ExternalServiceStore::new(pool.clone())),
            keyring: Arc::new(Keyring::default()),
        };
        let name = format!(
            "remote-url-nosources-{}.example.com/repo",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock")
                .as_nanos()
        );
        let repo_id: i32 = sqlx::query_scalar("INSERT INTO repo(name) VALUES ($1) RETURNING id")
            .bind(&name)
            .fetch_one(&pool)
            .await
            .expect("insert repo");

        let err = source.remote_url(&name).await.expect_err("no sources");
        assert_eq!(
            err.message(),
            format!("getting remote URL for {name:?}: no sources for {name:?}")
        );

        sqlx::query("DELETE FROM repo WHERE id = $1")
            .bind(repo_id)
            .execute(&pool)
            .await
            .expect("cleanup");
    }
}
