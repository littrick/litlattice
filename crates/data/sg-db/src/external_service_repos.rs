//! Port of `internal/repos/store.go`'s repo-side methods:
//!
//! - `DeleteExternalServiceReposNotIn` / `DeleteExternalServiceRepo`
//! - `CreateExternalServiceRepo` / `UpdateExternalServiceRepo`
//!
//! The repo (`repo`) and association (`external_service_repos`) writes are
//! issued in one transaction, like `Syncer.sync`'s `Store.Transact`.

use std::sync::Arc;
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use sqlx::postgres::PgPool;
use sqlx::types::time::OffsetDateTime;
use sqlx::Row;

/// `internal/repos/observability.go`'s `StoreMetrics` operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StoreOp {
    Transact,
    Done,
    CreateExternalServiceRepo,
    UpdateExternalServiceRepo,
    DeleteExternalServiceRepo,
    DeleteExternalServiceReposNotIn,
}

/// Observation hook for the store's RED metrics. The repo-updater service
/// implements it on top of `sg_metrics` (see `internal/repos/observability.go`
/// `NewStoreMetrics`); `sg-db` itself stays free of Prometheus so the store
/// can be used by other processes that never observe these families.
pub trait StoreObserver: Send + Sync {
    /// `metrics.REDMetrics.Observe(secs, count, err)`: `had_error` records
    /// whether the operation returned an error.
    fn observe(&self, op: StoreOp, seconds: Duration, had_error: bool);
}

/// `api.ExternalRepoSpec` as stored in the `repo` columns.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ExternalRepoSpec {
    pub id: String,
    pub service_type: String,
    pub service_id: String,
}

impl ExternalRepoSpec {
    pub fn equal(&self, other: &Self) -> bool {
        !other.id.is_empty()
            && !other.service_id.is_empty()
            && !other.service_type.is_empty()
            && self == other
    }
}

/// `types.SourceInfo` as returned by `scanRepo`'s sources subquery.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoredSource {
    #[serde(rename = "ID")]
    pub external_service_id: i64,
    #[serde(rename = "Kind")]
    pub kind: String,
    #[serde(rename = "CloneURL")]
    pub clone_url: String,
}

impl StoredSource {
    /// `extsvc.URN(kind, id)`, the key `types.Repo.Sources` uses.
    pub fn urn(&self) -> String {
        format!(
            "extsvc:{}:{}",
            self.kind.to_lowercase(),
            self.external_service_id
        )
    }
}

/// The `repo` row subset the syncer compares and writes.
#[derive(Debug, Clone)]
pub struct StoredRepo {
    pub id: i32,
    pub name: String,
    pub uri: String,
    pub description: String,
    pub external_repo: ExternalRepoSpec,
    pub archived: bool,
    pub fork: bool,
    pub stars: i32,
    pub private: bool,
    pub metadata: serde_json::Value,
    pub blocked: Option<serde_json::Value>,
    pub deleted_at: Option<OffsetDateTime>,
    pub sources: Vec<StoredSource>,
}

/// The sourced values written to `repo` (`CreateExternalServiceRepo`'s input).
#[derive(Debug, Clone)]
pub struct RepoWrite {
    pub name: String,
    pub uri: String,
    pub description: String,
    pub external_repo: ExternalRepoSpec,
    pub archived: bool,
    pub fork: bool,
    pub stars: i32,
    pub private: bool,
    pub metadata: serde_json::Value,
}

/// Result of `Syncer.sync`'s database half.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SyncRepoOutcome {
    Created { id: i32, created_at: OffsetDateTime },
    Updated { id: i32, updated_at: OffsetDateTime },
}

#[derive(Clone)]
pub struct ExternalServiceReposStore {
    pool: PgPool,
    observer: Option<Arc<dyn StoreObserver>>,
}

/// A transaction for `Syncer.sync`'s read-decide-write sequence
/// (`Store.Transact` in the baseline).
pub struct RepoTransaction<'a> {
    tx: Option<sqlx::Transaction<'a, sqlx::Postgres>>,
    observer: Option<Arc<dyn StoreObserver>>,
    /// Set by [`RepoTransaction::commit`] so the rollback path in
    /// [`Drop`] does not observe a second `Done`.
    finished: bool,
}

impl ExternalServiceReposStore {
    pub fn new(pool: PgPool) -> Self {
        Self {
            pool,
            observer: None,
        }
    }

    /// Attach the RED metrics observer (`SetMetrics` in the baseline).
    pub fn with_observer(mut self, observer: Arc<dyn StoreObserver>) -> Self {
        self.observer = Some(observer);
        self
    }

    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    fn observe_store(&self, op: StoreOp, started: Instant, had_error: bool) {
        if let Some(observer) = &self.observer {
            observer.observe(op, started.elapsed(), had_error);
        }
    }

    pub async fn begin(&self) -> Result<RepoTransaction<'_>, sqlx::Error> {
        let started = Instant::now();
        let tx = self.pool.begin().await;
        self.observe_store(StoreOp::Transact, started, tx.is_err());
        Ok(RepoTransaction {
            tx: Some(tx?),
            observer: self.observer.clone(),
            finished: false,
        })
    }

    /// Standalone `Repos.List` used by tests and callers that do not need the
    /// whole sync transaction.
    pub async fn list_for_sync(
        &self,
        name: &str,
        external_repo: &ExternalRepoSpec,
    ) -> Result<Vec<StoredRepo>, sqlx::Error> {
        let mut tx = self.begin().await?;
        let rows = tx.list_for_sync(name, external_repo).await?;
        tx.commit().await?;
        Ok(rows)
    }

    /// Standalone `CreateExternalServiceRepo`.
    pub async fn create(
        &self,
        service_id: i64,
        repo: &RepoWrite,
        clone_url: &str,
    ) -> Result<SyncRepoOutcome, sqlx::Error> {
        let mut tx = self.begin().await?;
        let outcome = tx.create(service_id, repo, clone_url).await?;
        tx.commit().await?;
        Ok(outcome)
    }

    /// Standalone `UpdateExternalServiceRepo`.
    pub async fn update(
        &self,
        service_id: i64,
        id: i32,
        repo: &RepoWrite,
        clone_url: &str,
    ) -> Result<SyncRepoOutcome, sqlx::Error> {
        let mut tx = self.begin().await?;
        let outcome = tx.update(service_id, id, repo, clone_url).await?;
        tx.commit().await?;
        Ok(outcome)
    }

    /// Standalone soft delete for the naming-conflict branch.
    pub async fn delete_conflicting(&self, id: i32) -> Result<(), sqlx::Error> {
        let mut tx = self.begin().await?;
        tx.delete_conflicting(id).await?;
        tx.commit().await?;
        Ok(())
    }

    /// `DeleteExternalServiceReposNotIn`: returns the ids that were
    /// successfully disassociated and soft-deleted (when orphaned).
    pub async fn delete_not_in(
        &self,
        service_id: i64,
        seen: &[i64],
    ) -> Result<(Vec<i32>, Option<sqlx::Error>), sqlx::Error> {
        let started = Instant::now();
        let result = self.delete_not_in_inner(service_id, seen).await;
        // The baseline observes the named `err` return after the explicit
        // `return deleted, errs`, i.e. the aggregated per-repo error (or the
        // list query error) decides whether the call counts as failed.
        let had_error = match &result {
            Ok((_, first_error)) => first_error.is_some(),
            Err(_) => true,
        };
        self.observe_store(StoreOp::DeleteExternalServiceReposNotIn, started, had_error);
        result
    }

    async fn delete_not_in_inner(
        &self,
        service_id: i64,
        seen: &[i64],
    ) -> Result<(Vec<i32>, Option<sqlx::Error>), sqlx::Error> {
        let row = sqlx::query(
            r#"
SELECT array_agg(repo_id) AS ids
FROM external_service_repos
WHERE external_service_id = $1 AND repo_id != ALL($2)
"#,
        )
        .bind(service_id)
        .bind(seen)
        .fetch_one(&self.pool)
        .await?;
        let ids: Option<Vec<i32>> = row.try_get("ids")?;
        let Some(ids) = ids else {
            return Ok((Vec::new(), None));
        };

        let mut deleted = Vec::with_capacity(ids.len());
        let mut first_error = None;
        for id in ids {
            match self.delete_one(service_id, id).await {
                Ok(()) => deleted.push(id),
                Err(err) => {
                    if first_error.is_none() {
                        first_error = Some(err);
                    }
                }
            }
        }
        Ok((deleted, first_error))
    }

    /// `DeleteExternalServiceRepo`. Outside a transaction the baseline opens
    /// a nested `s.transact`/`s.Done` pair, so this observes `Transact` and
    /// `Done` around the delete as well.
    pub async fn delete_one(&self, service_id: i64, repo_id: i32) -> Result<(), sqlx::Error> {
        let started = Instant::now();
        let result = self.delete_one_inner(service_id, repo_id).await;
        self.observe_store(StoreOp::DeleteExternalServiceRepo, started, result.is_err());
        result
    }

    async fn delete_one_inner(&self, service_id: i64, repo_id: i32) -> Result<(), sqlx::Error> {
        let mut tx = self.begin().await?;
        delete_association(tx.tx(), service_id, repo_id).await?;
        tx.commit().await
    }
}

impl<'a> RepoTransaction<'a> {
    /// The live `sqlx` transaction; present until [`RepoTransaction::commit`].
    fn tx(&mut self) -> &mut sqlx::Transaction<'a, sqlx::Postgres> {
        self.tx
            .as_mut()
            .expect("transaction is present until commit")
    }

    /// `Repos.List` with `Names` + `ExternalRepos`, `IncludeBlocked` and
    /// `IncludeDeleted` set, `UseOr: true` (`Syncer.sync`'s lookup).
    pub async fn list_for_sync(
        &mut self,
        name: &str,
        external_repo: &ExternalRepoSpec,
    ) -> Result<Vec<StoredRepo>, sqlx::Error> {
        let rows = sqlx::query(
            r#"
SELECT
	repo.id, repo.name, repo.uri, repo.description,
	repo.external_service_type, repo.external_service_id, repo.external_id,
	repo.archived, repo.fork, repo.stars, repo.private, repo.metadata,
	repo.blocked, repo.deleted_at,
	(
		SELECT json_agg(
			json_build_object(
				'CloneURL', esr.clone_url,
				'ID', esr.external_service_id,
				'Kind', LOWER(svcs.kind)
			)
		)
		FROM external_service_repos AS esr
		JOIN external_services AS svcs ON esr.external_service_id = svcs.id
		WHERE esr.repo_id = repo.id AND svcs.deleted_at IS NULL
	) AS sources
FROM repo
WHERE
	(
		lower(repo.name::text) COLLATE "C" = $1
		OR (
			repo.external_id = $2
			AND repo.external_service_type = $3
			AND repo.external_service_id = $4
		)
	)
"#,
        )
        .bind(name.to_lowercase())
        .bind(&external_repo.id)
        .bind(&external_repo.service_type)
        .bind(&external_repo.service_id)
        .fetch_all(&mut **self.tx())
        .await?;

        rows.into_iter().map(row_to_stored_repo).collect()
    }

    /// `RepoStore.Count(ReposListOptions{OnlyPrivate: true})`: the number of
    /// live private repositories, counted inside the transaction so the
    /// license hook sees writes made earlier in the same sync.
    pub async fn count_private(&mut self) -> Result<i64, sqlx::Error> {
        sqlx::query_scalar("SELECT COUNT(*) FROM repo WHERE private AND deleted_at IS NULL")
            .fetch_one(&mut **self.tx())
            .await
    }

    /// `CreateExternalServiceRepo` (and the caller-reported `Added` diff).
    pub async fn create(
        &mut self,
        service_id: i64,
        repo: &RepoWrite,
        clone_url: &str,
    ) -> Result<SyncRepoOutcome, sqlx::Error> {
        let started = Instant::now();
        let result = self.create_inner(service_id, repo, clone_url).await;
        self.observe(StoreOp::CreateExternalServiceRepo, started, result.is_err());
        result
    }

    async fn create_inner(
        &mut self,
        service_id: i64,
        repo: &RepoWrite,
        clone_url: &str,
    ) -> Result<SyncRepoOutcome, sqlx::Error> {
        let row = sqlx::query(
            r#"
INSERT INTO repo (
	name, uri, description, external_service_type, external_service_id,
	external_id, archived, fork, stars, private, metadata, created_at
)
VALUES ($1, NULLIF($2, ''), $3, $4, $5, $6, $7, $8, $9, $10, $11, now())
RETURNING id, created_at
"#,
        )
        .bind(&repo.name)
        .bind(&repo.uri)
        .bind(&repo.description)
        .bind(&repo.external_repo.service_type)
        .bind(&repo.external_repo.service_id)
        .bind(&repo.external_repo.id)
        .bind(repo.archived)
        .bind(repo.fork)
        .bind(repo.stars)
        .bind(repo.private)
        .bind(&repo.metadata)
        .fetch_one(&mut **self.tx())
        .await?;

        let id: i32 = row.try_get("id")?;
        let created_at: OffsetDateTime = row.try_get("created_at")?;
        upsert_association(self.tx(), service_id, id, clone_url).await?;
        Ok(SyncRepoOutcome::Created { id, created_at })
    }

    /// `UpdateExternalServiceRepo`: updates the repo row (clearing
    /// `deleted_at`) and the association's clone URL.
    pub async fn update(
        &mut self,
        service_id: i64,
        id: i32,
        repo: &RepoWrite,
        clone_url: &str,
    ) -> Result<SyncRepoOutcome, sqlx::Error> {
        let started = Instant::now();
        let result = self.update_inner(service_id, id, repo, clone_url).await;
        self.observe(StoreOp::UpdateExternalServiceRepo, started, result.is_err());
        result
    }

    async fn update_inner(
        &mut self,
        service_id: i64,
        id: i32,
        repo: &RepoWrite,
        clone_url: &str,
    ) -> Result<SyncRepoOutcome, sqlx::Error> {
        let row = sqlx::query(
            r#"
UPDATE repo
SET
	name                  = $1,
	uri                   = NULLIF($2, ''),
	description           = $3,
	external_service_type = $4,
	external_service_id   = $5,
	external_id           = $6,
	archived              = $7,
	fork                  = $8,
	stars                 = $9,
	private               = $10,
	metadata              = $11,
	updated_at            = now(),
	deleted_at            = NULL
WHERE id = $12
RETURNING updated_at
"#,
        )
        .bind(&repo.name)
        .bind(&repo.uri)
        .bind(&repo.description)
        .bind(&repo.external_repo.service_type)
        .bind(&repo.external_repo.service_id)
        .bind(&repo.external_repo.id)
        .bind(repo.archived)
        .bind(repo.fork)
        .bind(repo.stars)
        .bind(repo.private)
        .bind(&repo.metadata)
        .bind(id)
        .fetch_one(&mut **self.tx())
        .await?;
        let updated_at: OffsetDateTime = row.try_get("updated_at")?;
        upsert_association(self.tx(), service_id, id, clone_url).await?;
        Ok(SyncRepoOutcome::Updated { id, updated_at })
    }

    /// `Syncer.sync`'s naming-conflict branch: soft-delete the repo that owns
    /// the sourced name but has a different external repo spec.
    pub async fn delete_conflicting(&mut self, id: i32) -> Result<(), sqlx::Error> {
        soft_delete_repo(self.tx(), id).await
    }

    fn observe(&self, op: StoreOp, started: Instant, had_error: bool) {
        if let Some(observer) = &self.observer {
            observer.observe(op, started.elapsed(), had_error);
        }
    }

    /// `Store.Done`: commits and observes the close (success or commit
    /// error). The rollback path is observed by [`Drop`].
    pub async fn commit(mut self) -> Result<(), sqlx::Error> {
        let started = Instant::now();
        let result = match self.tx.take() {
            Some(tx) => tx.commit().await,
            None => Ok(()),
        };
        self.finished = true;
        self.observe(StoreOp::Done, started, result.is_err());
        result
    }
}

impl Drop for RepoTransaction<'_> {
    fn drop(&mut self) {
        if self.finished {
            return;
        }
        // Dropping the `sqlx` transaction rolls back. The baseline observes
        // `Store.Done(err)` with the caller's error before rolling back; the
        // rollback itself is too fast to measure, so the sample is zero.
        if let Some(observer) = &self.observer {
            observer.observe(StoreOp::Done, Duration::ZERO, true);
        }
    }
}

async fn delete_association(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    service_id: i64,
    repo_id: i32,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"
DELETE FROM external_service_repos
WHERE external_service_id = $1 AND repo_id = $2
"#,
    )
    .bind(service_id)
    .bind(repo_id)
    .execute(&mut **tx)
    .await?;
    sqlx::query(
        r#"
UPDATE repo
SET name = soft_deleted_repository_name(name), deleted_at = now()
WHERE id = $1 AND NOT EXISTS (
	SELECT FROM external_service_repos
	WHERE repo_id = $1 LIMIT 1
)
"#,
    )
    .bind(repo_id)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

async fn upsert_association(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    service_id: i64,
    repo_id: i32,
    clone_url: &str,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"
INSERT INTO external_service_repos (
	external_service_id,
	repo_id,
	clone_url
)
VALUES ($1, $2, $3)
ON CONFLICT (external_service_id, repo_id)
DO UPDATE SET
	clone_url = excluded.clone_url
WHERE
	external_service_repos.clone_url != excluded.clone_url
"#,
    )
    .bind(service_id)
    .bind(repo_id)
    .bind(clone_url)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

/// `Repos.Delete`'s per-id body (`deleteReposQuery`).
async fn soft_delete_repo(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    id: i32,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"
UPDATE repo
SET
  name = soft_deleted_repository_name(name),
  deleted_at = COALESCE(deleted_at, transaction_timestamp())
WHERE id = $1
"#,
    )
    .bind(id)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

fn row_to_stored_repo(row: sqlx::postgres::PgRow) -> Result<StoredRepo, sqlx::Error> {
    let sources: Option<serde_json::Value> = row.try_get("sources")?;
    let sources = match sources {
        Some(value) => {
            serde_json::from_value(value).map_err(|err| sqlx::Error::Decode(Box::new(err)))?
        }
        None => Vec::new(),
    };
    Ok(StoredRepo {
        id: row.try_get("id")?,
        name: row.try_get("name")?,
        uri: row.try_get::<Option<String>, _>("uri")?.unwrap_or_default(),
        description: row
            .try_get::<Option<String>, _>("description")?
            .unwrap_or_default(),
        external_repo: ExternalRepoSpec {
            id: row
                .try_get::<Option<String>, _>("external_id")?
                .unwrap_or_default(),
            service_type: row
                .try_get::<Option<String>, _>("external_service_type")?
                .unwrap_or_default(),
            service_id: row
                .try_get::<Option<String>, _>("external_service_id")?
                .unwrap_or_default(),
        },
        archived: row.try_get("archived")?,
        fork: row.try_get("fork")?,
        stars: row.try_get("stars")?,
        private: row.try_get("private")?,
        metadata: row
            .try_get::<Option<serde_json::Value>, _>("metadata")?
            .unwrap_or(serde_json::Value::Null),
        blocked: row.try_get("blocked")?,
        deleted_at: row.try_get("deleted_at")?,
        sources,
    })
}

/// `repoStore.GetFirstRepoNameByCloneURL`
/// (`internal/database/repos.go:1585-1608`): the first repo (by
/// `repo.updated_at DESC`) whose external-service association carries exactly
/// this clone URL. Returns `None` when no association matches. Like the
/// baseline, there is no `deleted_at`/blocked filter here; callers re-apply the
/// repository lookup semantics afterwards.
pub async fn first_repo_name_by_clone_url(
    pool: &PgPool,
    clone_url: &str,
) -> Result<Option<String>, sqlx::Error> {
    sqlx::query_scalar(
        r#"
SELECT r.name
FROM repo r
JOIN external_service_repos esr ON r.id = esr.repo_id
WHERE esr.clone_url = $1
ORDER BY r.updated_at DESC
LIMIT 1
"#,
    )
    .bind(clone_url)
    .fetch_optional(pool)
    .await
}

#[cfg(test)]
mod tests {
    use super::*;

    fn unique_suffix() -> u128 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos()
    }

    async fn insert_service(pool: &PgPool, suffix: u128, kind: &str) -> i64 {
        sqlx::query_scalar(
            "INSERT INTO external_services(kind, display_name, config) VALUES ($1, $2, '{}') RETURNING id",
        )
        .bind(kind)
        .bind(format!("svc-{suffix}"))
        .fetch_one(pool)
        .await
        .expect("insert external service")
    }

    fn repo_write(
        name: &str,
        external_id: &str,
        service_type: &str,
        service_id: &str,
    ) -> RepoWrite {
        RepoWrite {
            name: name.to_owned(),
            uri: name.to_owned(),
            description: String::new(),
            external_repo: ExternalRepoSpec {
                id: external_id.to_owned(),
                service_type: service_type.to_owned(),
                service_id: service_id.to_owned(),
            },
            archived: false,
            fork: false,
            stars: 0,
            private: true,
            metadata: serde_json::json!({"RelativePath": "/repo"}),
        }
    }

    #[tokio::test]
    async fn sync_writes_repo_and_association_like_the_baseline() {
        let Ok(dsn) = std::env::var("SG_TEST_DATABASE_DSN") else {
            eprintln!("SG_TEST_DATABASE_DSN unset; skipping");
            return;
        };
        let pool = crate::connect(&dsn, 2).await.expect("connect");
        let store = ExternalServiceReposStore::new(pool.clone());
        let suffix = unique_suffix();
        let service_id = insert_service(&pool, suffix, "OTHER").await;
        let service_id2 = insert_service(&pool, suffix + 1, "GITHUB").await;
        let name = format!("extsvc-test-{suffix}.example.com/repo");
        let write = repo_write(
            &name,
            &name,
            "OTHER",
            &format!("https://extsvc-test-{suffix}.example.com"),
        );

        let result = async {
            let outcome = store
                .create(service_id, &write, "https://clone.example.com/repo.git")
                .await
                .expect("create");
            let SyncRepoOutcome::Created { id, .. } = outcome else {
                panic!("expected create, got {outcome:?}");
            };

            let found = store
                .list_for_sync(&name.to_uppercase(), &write.external_repo)
                .await
                .expect("list by name");
            assert_eq!(found.len(), 1, "name match is case insensitive");
            assert_eq!(found[0].id, id);
            assert_eq!(found[0].name, name);
            assert_eq!(found[0].sources.len(), 1);
            assert_eq!(
                found[0].sources[0].urn(),
                format!("extsvc:other:{service_id}")
            );
            assert_eq!(
                found[0].sources[0].clone_url,
                "https://clone.example.com/repo.git"
            );
            assert!(found[0].private);
            assert!(found[0].deleted_at.is_none());

            // The external repo spec also matches, and returns the same row.
            let mut other_spec = write.external_repo.clone();
            other_spec.id = name.clone();
            let by_spec = store
                .list_for_sync("nothing-matches.example.com/other", &other_spec)
                .await
                .expect("list by spec");
            assert_eq!(by_spec.len(), 1);
            assert_eq!(by_spec[0].id, id);

            // A second source joins the same repo; the association rows are
            // independent.
            store
                .update(
                    service_id2,
                    id,
                    &write,
                    "https://clone2.example.com/repo.git",
                )
                .await
                .expect("update");
            let found = store
                .list_for_sync(&name, &write.external_repo)
                .await
                .expect("list");
            assert_eq!(found[0].sources.len(), 2);

            // Updating only the clone URL rewrites the association.
            store
                .update(
                    service_id,
                    id,
                    &write,
                    "https://clone3.example.com/repo.git",
                )
                .await
                .expect("update clone url");
            let found = store
                .list_for_sync(&name, &write.external_repo)
                .await
                .expect("list");
            let source = found[0]
                .sources
                .iter()
                .find(|source| source.external_service_id == service_id)
                .expect("source");
            assert_eq!(source.clone_url, "https://clone3.example.com/repo.git");

            // delete_not_in keeps the repo because the second association
            // still references it.
            let (deleted, err) = store.delete_not_in(service_id, &[]).await.expect("delete");
            assert_eq!(deleted, vec![id]);
            assert!(err.is_none());
            let found = store
                .list_for_sync(&name, &write.external_repo)
                .await
                .expect("list");
            assert_eq!(found.len(), 1);
            assert!(found[0].deleted_at.is_none(), "still associated");

            // Removing the last association soft-deletes the repo.
            store.delete_one(service_id2, id).await.expect("delete one");
            let found = store
                .list_for_sync(&name, &write.external_repo)
                .await
                .expect("list");
            assert_eq!(found.len(), 1);
            assert!(found[0].deleted_at.is_some());
            assert!(
                found[0].name.starts_with("DELETED-") && found[0].name.ends_with(&name),
                "{}",
                found[0].name
            );
        }
        .await;

        sqlx::query("DELETE FROM external_services WHERE id = ANY($1)")
            .bind([service_id, service_id2])
            .execute(&pool)
            .await
            .expect("cleanup services");
        sqlx::query("DELETE FROM repo WHERE name::text LIKE $1")
            .bind(format!("%extsvc-test-{suffix}%"))
            .execute(&pool)
            .await
            .expect("cleanup repos");
        result
    }

    #[tokio::test]
    async fn naming_conflicts_soft_delete_and_recreate() {
        let Ok(dsn) = std::env::var("SG_TEST_DATABASE_DSN") else {
            eprintln!("SG_TEST_DATABASE_DSN unset; skipping");
            return;
        };
        let pool = crate::connect(&dsn, 2).await.expect("connect");
        let store = ExternalServiceReposStore::new(pool.clone());
        let suffix = unique_suffix();
        let service_id = insert_service(&pool, suffix, "OTHER").await;
        let name = format!("conflict-test-{suffix}.example.com/repo");
        let old = repo_write(
            &name,
            &format!("old-id-{suffix}"),
            "OTHER",
            &format!("old-service-{suffix}"),
        );
        let new = repo_write(
            &name,
            &name,
            "OTHER",
            &format!("https://conflict-test-{suffix}.example.com"),
        );

        let result = async {
            store
                .create(service_id, &old, "https://old.example.com/repo.git")
                .await
                .expect("create old");
            let existing = store
                .list_for_sync(&name, &new.external_repo)
                .await
                .expect("list");
            let conflicting = existing
                .iter()
                .find(|repo| !repo.external_repo.equal(&new.external_repo))
                .expect("conflict found");
            store
                .delete_conflicting(conflicting.id)
                .await
                .expect("delete conflict");

            let outcome = store
                .create(service_id, &new, "https://clone.example.com/repo.git")
                .await
                .expect("recreate");
            let SyncRepoOutcome::Created { id, .. } = outcome else {
                panic!("expected create");
            };
            let found = store
                .list_for_sync(&name, &new.external_repo)
                .await
                .expect("list");
            let live = found
                .iter()
                .find(|repo| repo.deleted_at.is_none())
                .expect("live repo");
            assert_eq!(live.id, id);
            assert_eq!(live.external_repo, new.external_repo);
            let old_name: String =
                sqlx::query_scalar("SELECT name FROM repo WHERE external_id = $1")
                    .bind(format!("old-id-{suffix}"))
                    .fetch_one(&pool)
                    .await
                    .expect("old row kept");
            assert!(
                old_name.starts_with("DELETED-") && old_name.ends_with(&name),
                "{old_name}"
            );
        }
        .await;

        sqlx::query("DELETE FROM external_services WHERE id = $1")
            .bind(service_id)
            .execute(&pool)
            .await
            .expect("cleanup services");
        sqlx::query("DELETE FROM repo WHERE name::text LIKE $1 OR external_id LIKE $2")
            .bind(format!("%conflict-test-{suffix}%"))
            .bind(format!("old-id-{suffix}"))
            .execute(&pool)
            .await
            .expect("cleanup repos");
        result
    }
}
