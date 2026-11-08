//! Read-only view of `repo` and its sources, as needed by gitserver's remote
//! URL lookup (`internal/database/repos.go`: `GetByName` + `getSourcesByRepoQueryStr`).
//!
//! `sources` is aggregated exactly like the baseline subquery: every live
//! external service linked through `external_service_repos`, with the service
//! kind lowercased. The `Kind`/`ID`/`CloneURL` key casing matches the JSON the
//! Go scanner decodes (`scanRepo`).

use serde::Deserialize;
use sqlx::postgres::PgPool;
use sqlx::Row;

/// One entry of `types.Repo.Sources` (`types.SourceInfo` after `scanRepo`).
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct SourceInfo {
    #[serde(rename = "ID")]
    pub external_service_id: i64,
    #[serde(rename = "Kind")]
    pub kind: String,
    #[serde(rename = "CloneURL")]
    pub clone_url: String,
}

/// Subset of `types.Repo` used by the gitserver write path.
#[derive(Debug, Clone, PartialEq)]
pub struct Repo {
    pub id: i32,
    pub name: String,
    pub external_service_type: Option<String>,
    pub external_service_id: Option<String>,
    /// `repo.metadata`: code-host specific repository metadata.
    pub metadata: serde_json::Value,
    /// `repo.uri`: the repository's external identifier (used as a lookup
    /// fallback by `GetByName`).
    pub uri: Option<String>,
    /// `repo.blocked`: `{"reason": "..."}` when the repository is blocked.
    pub blocked: Option<serde_json::Value>,
    /// `repo.sources`; empty when no live external service references the repo.
    pub sources: Vec<SourceInfo>,
}

#[derive(Clone)]
pub struct RepoStore {
    pool: PgPool,
}

impl RepoStore {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// `Repos().GetByName`: soft-deleted repositories are not returned (the
    /// baseline's `listRepos` defaults to `deleted_at IS NULL`). The name is
    /// matched first and the `uri` column second (the baseline does two
    /// queries because `uri` is not unique); both are `citext`, so the
    /// comparison is case-insensitive. Blocked repositories are returned like
    /// the baseline (`IncludeBlocked: true`); [`Repo::blocked`] lets the
    /// caller produce the `BlockedRepoError` the baseline returns alongside
    /// the repo.
    pub async fn get_by_name(&self, name: &str) -> Result<Option<Repo>, sqlx::Error> {
        let row = sqlx::query(
            r#"
SELECT
	r.id,
	r.name,
	r.external_service_type,
	r.external_service_id,
	r.metadata,
	r.uri,
	r.blocked,
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
		WHERE esr.repo_id = r.id AND svcs.deleted_at IS NULL
	) AS sources
FROM repo r
WHERE r.deleted_at IS NULL AND (r.name = $1::citext OR r.uri = $1::citext)
ORDER BY (r.name = $1::citext) DESC, r.id
LIMIT 1
"#,
        )
        .bind(name)
        .fetch_optional(&self.pool)
        .await?;
        let Some(row) = row else {
            return Ok(None);
        };

        let sources: Option<serde_json::Value> = row.try_get("sources")?;
        let sources = match sources {
            Some(value) => {
                serde_json::from_value(value).map_err(|err| sqlx::Error::Decode(Box::new(err)))?
            }
            None => Vec::new(),
        };

        Ok(Some(Repo {
            id: row.try_get("id")?,
            name: row.try_get("name")?,
            external_service_type: row.try_get("external_service_type")?,
            external_service_id: row.try_get("external_service_id")?,
            metadata: row.try_get("metadata")?,
            uri: row.try_get("uri")?,
            blocked: row.try_get("blocked")?,
            sources,
        }))
    }
}

/// Minimal repository identity used by repo-updater's RPCs
/// (`Repos.List` with `Names`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RepoName {
    pub id: i32,
    pub name: String,
}

impl RepoStore {
    /// `Repos.List` restricted to exact names. Soft-deleted repositories are
    /// excluded like the baseline default; blocked repositories are included
    /// (the repo-updater RPCs only care about identity).
    pub async fn list_by_names(&self, names: &[String]) -> Result<Vec<RepoName>, sqlx::Error> {
        if names.is_empty() {
            return Ok(Vec::new());
        }
        let rows = sqlx::query(
            r#"
SELECT id, name
FROM repo
WHERE deleted_at IS NULL AND name = ANY($1::citext[])
ORDER BY id
"#,
        )
        .bind(names)
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter()
            .map(|row| {
                Ok(RepoName {
                    id: row.try_get("id")?,
                    name: row.try_get("name")?,
                })
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn loads_repo_sources_from_the_baseline_database() {
        let Ok(dsn) = std::env::var("SG_TEST_DATABASE_DSN") else {
            eprintln!("SG_TEST_DATABASE_DSN unset; skipping");
            return;
        };
        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(2)
            .connect(&dsn)
            .await
            .expect("connect");
        let store = RepoStore::new(pool.clone());

        let name = format!(
            "sg-db-repos-{}.example.com/repo",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock")
                .as_nanos()
        );
        let uri = format!("{name}-uri");
        let repo_id: i32 = sqlx::query_scalar(
            "INSERT INTO repo(name, uri, external_service_type, external_service_id, external_id, metadata) \
             VALUES ($1, $2, 'github', '123', 'repo-1', $3) RETURNING id",
        )
        .bind(&name)
        .bind(&uri)
        .bind(serde_json::json!({"URL": "https://example.com/o/r", "NameWithOwner": "o/r"}))
        .fetch_one(&pool)
        .await
        .expect("insert repo");
        let service_id: i64 = sqlx::query_scalar(
            "INSERT INTO external_services(kind, display_name, config) \
             VALUES ('GITHUB', 'gh', '{}') RETURNING id",
        )
        .fetch_one(&pool)
        .await
        .expect("insert service");
        sqlx::query(
            "INSERT INTO external_service_repos(external_service_id, repo_id, clone_url) \
             VALUES ($1, $2, 'https://example.com/o/r.git')",
        )
        .bind(service_id)
        .bind(repo_id)
        .execute(&pool)
        .await
        .expect("insert source");

        let repo = store
            .get_by_name(&name.to_uppercase())
            .await
            .expect("get")
            .expect("row");
        assert_eq!(repo.id, repo_id);
        assert_eq!(repo.name, name);
        assert_eq!(repo.external_service_type.as_deref(), Some("github"));
        assert_eq!(repo.metadata["NameWithOwner"], "o/r");
        assert_eq!(
            repo.sources,
            [SourceInfo {
                external_service_id: service_id,
                kind: "github".to_owned(),
                clone_url: "https://example.com/o/r.git".to_owned(),
            }]
        );

        assert_eq!(repo.uri.as_deref(), Some(uri.as_str()));
        assert_eq!(repo.blocked, None);

        // `GetByName` falls back to the `uri` column, case-insensitively.
        let by_uri = store
            .get_by_name(&uri.to_uppercase())
            .await
            .expect("get by uri")
            .expect("row");
        assert_eq!(by_uri.id, repo_id);

        // A blocked repo is still returned; the caller raises the error.
        sqlx::query(r#"UPDATE repo SET blocked = '{"reason": "dmca"}' WHERE id = $1"#)
            .bind(repo_id)
            .execute(&pool)
            .await
            .expect("block");
        let blocked = store.get_by_name(&name).await.expect("get").expect("row");
        assert_eq!(blocked.blocked.as_ref().unwrap()["reason"], "dmca");

        // Soft-deleted repos are invisible.
        sqlx::query("UPDATE repo SET deleted_at = NOW() WHERE id = $1")
            .bind(repo_id)
            .execute(&pool)
            .await
            .expect("soft delete");
        assert!(store.get_by_name(&name).await.expect("get").is_none());

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
}
