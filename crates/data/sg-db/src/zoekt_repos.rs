#![forbid(unsafe_code)]

//! `internal/database/zoekt_repos.go`'s `zoektReposStore.UpdateIndexStatuses`.
//!
//! The worker's `zoekt-repos-updater` is the only writer this module serves.
//! The baseline stages the indexed repositories in a `ON COMMIT DROP`
//! temporary table and applies one `UPDATE ... FROM temp_table` whose `WHERE`
//! clause only rewrites rows whose status, branches or `last_indexed_at`
//! actually changed, so a repeated reconcilation leaves `updated_at` alone.
//!
//! `batch.NewInserter` is replaced by one `INSERT` per row: the statement
//! text is the same, only the number of round trips differs (the inserter
//! batches to stay under Postgres' 65535 parameter limit).

use std::collections::BTreeMap;

use serde::Serialize;
use sqlx::postgres::PgPool;
use sqlx::types::time::OffsetDateTime;

/// `zoekt.RepositoryBranch`.
///
/// The zoekt struct has no JSON tags (`api.go`), so `branchesColumn`'s
/// `json.Marshal` writes the exported Go field names: `{"Name": ..., "Version": ...}`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct RepositoryBranch {
    pub name: String,
    pub version: String,
}

/// `zoekt.MinimalRepoListEntry`: the subset of a `zoekt.ReposMap` value the
/// updater writes.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct MinimalRepoListEntry {
    pub has_symbols: bool,
    pub branches: Vec<RepositoryBranch>,
    pub index_time_unix: i64,
}

/// Errors from [`update_index_statuses`].
#[derive(Debug, thiserror::Error)]
pub enum UpdateIndexStatusesError {
    #[error(transparent)]
    Sqlx(#[from] sqlx::Error),
    #[error("failed to marshal branches: {0}")]
    Json(#[from] serde_json::Error),
    #[error("invalid index time: {0}")]
    Time(#[from] time::error::ComponentRange),
}

/// `updateIndexStatusesCreateTempTableQuery`.
const CREATE_TEMP_TABLE_QUERY: &str = r#"
CREATE TEMPORARY TABLE temp_table (
	repo_id         integer NOT NULL,
	index_status    text NOT NULL,
	last_indexed_at TIMESTAMP WITH TIME ZONE,
	branches        jsonb
) ON COMMIT DROP
"#;

/// The `batch.NewInserter` target columns.
const INSERT_TEMP_TABLE_QUERY: &str = r#"
INSERT INTO temp_table (repo_id, index_status, branches, last_indexed_at)
VALUES ($1, $2, $3::jsonb, $4)
"#;

/// `updateIndexStatusesUpdateQuery`.
const UPDATE_QUERY: &str = r#"
UPDATE zoekt_repos zr
SET
	index_status    = source.index_status,
	branches        = source.branches,
	last_indexed_at = source.last_indexed_at,
	updated_at      = now()
FROM temp_table source
WHERE
	zr.repo_id = source.repo_id
AND
	(zr.index_status != source.index_status OR zr.branches != source.branches OR zr.last_indexed_at IS DISTINCT FROM source.last_indexed_at)
;
"#;

/// `branchesColumn`: `[]` for an empty slice, the JSON array otherwise.
fn branches_column(branches: &[RepositoryBranch]) -> Result<String, serde_json::Error> {
    if branches.is_empty() {
        Ok("[]".to_owned())
    } else {
        serde_json::to_string(branches)
    }
}

/// `UpdateIndexStatuses(ctx, indexed)`.
pub async fn update_index_statuses(
    pool: &PgPool,
    indexed: &BTreeMap<u32, MinimalRepoListEntry>,
) -> Result<(), UpdateIndexStatusesError> {
    let mut tx = pool.begin().await?;

    sqlx::query(CREATE_TEMP_TABLE_QUERY)
        .execute(&mut *tx)
        .await?;

    for (repo_id, entry) in indexed {
        let branches = branches_column(&entry.branches)?;
        // `time.Unix(entry.IndexTimeUnix, 0)`; zero means "no index time".
        let last_indexed_at = if entry.index_time_unix != 0 {
            Some(OffsetDateTime::from_unix_timestamp(entry.index_time_unix)?)
        } else {
            None
        };

        sqlx::query(INSERT_TEMP_TABLE_QUERY)
            .bind(*repo_id as i32)
            .bind("indexed")
            .bind(branches)
            .bind(last_indexed_at)
            .execute(&mut *tx)
            .await?;
    }

    sqlx::query(UPDATE_QUERY).execute(&mut *tx).await?;

    tx.commit().await?;
    Ok(())
}

/// `database.ZoektRepoStatistics` (`internal/database/zoekt_repos.go`).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ZoektRepoStatistics {
    pub total: i64,
    pub indexed: i64,
    pub not_indexed: i64,
}

/// `zoektReposStore.GetStatistics`: summary of `zoekt_repos` joined to live
/// (not soft-deleted, not blocked) repositories.
pub async fn get_statistics(pool: &PgPool) -> Result<ZoektRepoStatistics, sqlx::Error> {
    sqlx::query_as::<_, (i64, i64, i64)>(GET_STATISTICS_QUERY)
        .fetch_one(pool)
        .await
        .map(|(total, indexed, not_indexed)| ZoektRepoStatistics {
            total,
            indexed,
            not_indexed,
        })
}

const GET_STATISTICS_QUERY: &str = r#"
SELECT
	COUNT(*) AS total,
	COUNT(*) FILTER(WHERE index_status = 'indexed') AS indexed,
	COUNT(*) FILTER(WHERE index_status = 'not_indexed') AS not_indexed
FROM zoekt_repos zr
JOIN repo ON repo.id = zr.repo_id
WHERE
	repo.deleted_at is NULL
AND
	repo.blocked IS NULL
"#;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn branches_column_matches_the_baseline() {
        assert_eq!(branches_column(&[]).unwrap(), "[]");
        assert_eq!(
            branches_column(&[RepositoryBranch {
                name: "main".to_owned(),
                version: "abc".to_owned(),
            }])
            .unwrap(),
            r#"[{"Name":"main","Version":"abc"}]"#
        );
    }
}
