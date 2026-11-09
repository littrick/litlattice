#![forbid(unsafe_code)]

//! `internal/database/recent_contribution_signal.go`:
//! `RecentContributionSignalStore.ClearSignals` / `AddCommit` /
//! `FindRecentAuthors`, used by the Own `recent-contributors` indexer.
//!
//! `AddCommit` inserts one signal per changed file; the actual aggregation is
//! performed by the `update_own_aggregate_recent_contribution` trigger, which
//! stays owned by the Go migrations.

use sqlx::postgres::PgPool;
use sqlx::types::time::OffsetDateTime;

use crate::repo_paths::ensure_repo_paths;

/// `database.Commit`.
#[derive(Debug, Clone)]
pub struct Commit {
    pub repo_id: i32,
    pub author_name: String,
    pub author_email: String,
    pub timestamp: OffsetDateTime,
    pub commit_sha: String,
    pub files_changed: Vec<String>,
}

/// `database.RecentContributorSummary`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecentContributorSummary {
    pub author_name: String,
    pub author_email: String,
    pub contribution_count: i32,
}

/// `commitAuthorInsertFmtstr`:
///
/// ```sql
/// WITH already_exists (id) AS (
///     SELECT id FROM commit_authors WHERE name = $1 AND email = $2
/// ),
/// need_to_insert (id) AS (
///     INSERT INTO commit_authors (name, email)
///     VALUES ($3, $4)
///     ON CONFLICT (name, email) DO NOTHING
///     RETURNING id
/// )
/// SELECT id FROM already_exists
/// UNION ALL
/// SELECT id FROM need_to_insert
/// ```
async fn ensure_author(pool: &PgPool, commit: &Commit) -> Result<i32, String> {
    sqlx::query_scalar(
        "WITH already_exists (id) AS ( \
             SELECT id FROM commit_authors WHERE name = $1 AND email = $2 \
         ), need_to_insert (id) AS ( \
             INSERT INTO commit_authors (name, email) \
             VALUES ($3, $4) \
             ON CONFLICT (name, email) DO NOTHING \
             RETURNING id \
         ) \
         SELECT id FROM already_exists \
         UNION ALL \
         SELECT id FROM need_to_insert",
    )
    .bind(&commit.author_name)
    .bind(&commit.author_email)
    .bind(&commit.author_name)
    .bind(&commit.author_email)
    .fetch_one(pool)
    .await
    .map_err(|err| format!("cannot insert commit author: {err}"))
}

/// `clearSignalsFmtstr` for one of the two signal tables:
///
/// ```sql
/// WITH rps AS (SELECT id FROM repo_paths WHERE repo_id = $1)
/// DELETE FROM <table> WHERE changed_file_path_id IN (SELECT * FROM rps)
/// ```
async fn clear_signals_from(pool: &PgPool, repo_id: i32, table: &str) -> Result<(), String> {
    sqlx::query(&format!(
        "WITH rps AS (SELECT id FROM repo_paths WHERE repo_id = $1)\n\
         DELETE FROM {table}\n\
         WHERE changed_file_path_id IN (SELECT * FROM rps)"
    ))
    .bind(repo_id)
    .execute(pool)
    .await
    .map_err(|err| format!("table: {table}: {err}"))?;
    Ok(())
}

/// `RecentContributionSignalStore.ClearSignals`.
pub async fn clear_signals(pool: &PgPool, repo_id: i32) -> Result<(), String> {
    for table in [
        "own_signal_recent_contribution",
        "own_aggregate_recent_contribution",
    ] {
        clear_signals_from(pool, repo_id, table).await?;
    }
    Ok(())
}

/// `dbutil.CommitBytea.Value`: the commit SHA hex-decoded into the `bytea`
/// column, with Go's `hex.DecodeString` error surfaced.
fn commit_bytea(sha: &str) -> Result<Vec<u8>, String> {
    if sha.len() % 2 != 0 {
        return Err("encoding/hex: odd length hex string".to_owned());
    }
    let mut bytes = Vec::with_capacity(sha.len() / 2);
    let raw = sha.as_bytes();
    for pair in raw.chunks_exact(2) {
        let high = (pair[0] as char).to_digit(16);
        let low = (pair[1] as char).to_digit(16);
        match (high, low) {
            (Some(high), Some(low)) => bytes.push((high * 16 + low) as u8),
            _ => {
                return Err(format!(
                    "encoding/hex: invalid byte: {}",
                    String::from_utf8_lossy(pair)
                ))
            }
        }
    }
    Ok(bytes)
}

/// `insertRecentContributorSignalFmtstr`:
///
/// ```sql
/// INSERT INTO own_signal_recent_contribution (
///     commit_author_id, changed_file_path_id, commit_timestamp, commit_id
/// ) VALUES ($1, $2, $3, $4)
/// ```
pub async fn add_commit(pool: &PgPool, commit: &Commit) -> Result<(), String> {
    let author_id = ensure_author(pool, commit).await?;
    let path_ids = ensure_repo_paths(pool, &commit.files_changed, commit.repo_id)
        .await
        .map_err(|err| format!("cannot insert repo paths: {err}"))?;
    let commit_id = commit_bytea(&commit.commit_sha)?;

    for path_id in path_ids {
        sqlx::query(
            "INSERT INTO own_signal_recent_contribution ( \
                 commit_author_id, changed_file_path_id, commit_timestamp, commit_id \
             ) VALUES ($1, $2, $3, $4)",
        )
        .bind(author_id)
        .bind(path_id)
        .bind(commit.timestamp)
        .bind(&commit_id)
        .execute(pool)
        .await
        .map_err(|err| err.to_string())?;
    }
    Ok(())
}

/// `findRecentContributorsFmtstr`: authors of the aggregate signals for
/// `(repo_id, absolute_path)`, most contributions first.
pub async fn find_recent_authors(
    pool: &PgPool,
    repo_id: i32,
    path: &str,
) -> Result<Vec<RecentContributorSummary>, sqlx::Error> {
    let rows = sqlx::query_as::<_, (String, String, i32)>(
        "SELECT a.name, a.email, g.contributions_count \
         FROM commit_authors AS a \
         INNER JOIN own_aggregate_recent_contribution AS g \
         ON a.id = g.commit_author_id \
         INNER JOIN repo_paths AS p \
         ON p.id = g.changed_file_path_id \
         WHERE p.repo_id = $1 \
         AND p.absolute_path = $2 \
         ORDER BY 3 DESC",
    )
    .bind(repo_id)
    .bind(path)
    .fetch_all(pool)
    .await?;

    Ok(rows
        .into_iter()
        .map(
            |(author_name, author_email, contribution_count)| RecentContributorSummary {
                author_name,
                author_email,
                contribution_count,
            },
        )
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn commit_bytea_matches_hex_decoding() {
        assert_eq!(
            commit_bytea("00ff10").expect("valid hex"),
            vec![0x00, 0xff, 0x10]
        );
        assert!(commit_bytea("abc").is_err());
        assert!(commit_bytea("zz").is_err());
    }
}
