#![forbid(unsafe_code)]

//! Store operations used by the `codeintel-commitgraph-updater` worker job
//! (`internal/codeintel/uploads/internal/store/commitgraph.go` and
//! `commitdate.go`):
//!
//! * `GetDirtyRepositories`: repositories whose `dirty_token` is ahead of
//!   their `update_token`;
//! * `GetCommitAndDateForOldestUpload`: the oldest `committed_at` among the
//!   repository's completed uploads (a NULL means the commit-date backfill
//!   has not finished and is reported as an error, exactly like
//!   `backfillIncompleteError`);
//! * `UpdateUploadsVisibleToCommits`: decorate the in-memory commit graph
//!   with the repository's completed uploads, write the result into three
//!   `ON COMMIT DROP` temporary tables, transfer them into the persistent
//!   `lsif_nearest_uploads`, `lsif_nearest_uploads_links` and
//!   `lsif_uploads_visible_at_tip` tables, clear the dirty token, and mark
//!   `deleting` uploads unreachable from the graph as `deleted`.
//!
//! Every SQL string is copied from the baseline store. Migrations stay owned
//! by the Go `migrator` (the M6 storage contract); no table or column is
//! added. The baseline wraps the whole update in one transaction
//! (`store.withTransaction`), which this module reproduces with a single
//! `sqlx` transaction.
//!
//! Deliberate equivalences:
//!
//! * The baseline writes the temporary tables through parallel batch
//!   inserters fed by channels. Rust inserts row by row inside the same
//!   transaction; the persisted rows are identical.
//! * The baseline counts inserted/updated/deleted rows for its metrics slice;
//!   the Rust port executes the same three statements and does not report
//!   counts.
//! * The baseline serializes the uploads JSONB value in Go map order
//!   (non-deterministic); PostgreSQL's `jsonb` normalizes the object, and
//!   the Rust serializer sorts by upload id.

use std::collections::HashMap;

use sg_codeintel::commitgraph::{CommitGraph, Graph, UploadMeta};
use sqlx::postgres::PgPool;
use sqlx::Row;
use time::{Duration, OffsetDateTime};

/// `shared.DirtyRepository`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DirtyRepository {
    pub repository_id: i32,
    pub repository_name: String,
    pub dirty_token: i32,
}

/// `store.CommitWithDate`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommitWithDate {
    pub commit: String,
    pub committer_date: OffsetDateTime,
}

/// `store.backfillIncompleteError` / database failures from
/// [`get_commit_and_date_for_oldest_upload`].
#[derive(Debug)]
pub enum OldestUploadError {
    /// `repository %d has not yet completed its backfill of commit dates`.
    BackfillIncomplete {
        repository_id: i32,
    },
    Database(sqlx::Error),
}

impl std::fmt::Display for OldestUploadError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::BackfillIncomplete { repository_id } => write!(
                f,
                "repository {repository_id} has not yet completed its backfill of commit dates"
            ),
            Self::Database(err) => write!(f, "{err}"),
        }
    }
}

impl std::error::Error for OldestUploadError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::BackfillIncomplete { .. } => None,
            Self::Database(err) => Some(err),
        }
    }
}

impl From<sqlx::Error> for OldestUploadError {
    fn from(err: sqlx::Error) -> Self {
        Self::Database(err)
    }
}

/// `gitdomain.RefType`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RefType {
    Unknown,
    Branch,
    Tag,
}

/// The subset of `gitdomain.Ref` the commit graph update reads.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RefInfo {
    pub short_name: String,
    pub ref_type: RefType,
    /// `Ref.CreatedDate`; `None` mirrors Go's zero `time.Time`/epoch
    /// (`(*timestamppb.Timestamp)(nil).AsTime()`), which is always stale.
    pub created_at: Option<OffsetDateTime>,
    pub is_head: bool,
}

/// `GetDirtyRepositories`.
pub async fn get_dirty_repositories(pool: &PgPool) -> Result<Vec<DirtyRepository>, sqlx::Error> {
    let rows = sqlx::query(
        r#"
SELECT ldr.repository_id, repo.name, ldr.dirty_token
  FROM lsif_dirty_repositories ldr
    INNER JOIN repo ON repo.id = ldr.repository_id
  WHERE ldr.dirty_token > ldr.update_token
    AND repo.deleted_at IS NULL
    AND repo.blocked IS NULL
"#,
    )
    .fetch_all(pool)
    .await?;

    rows.into_iter()
        .map(|row| {
            Ok(DirtyRepository {
                repository_id: row.try_get("repository_id")?,
                repository_name: row.try_get("name")?,
                dirty_token: row.try_get("dirty_token")?,
            })
        })
        .collect()
}

/// `GetCommitAndDateForOldestUpload`.
///
/// `Ok(None)` means there is no completed upload for the repository. A
/// completed upload whose commit has no `codeintel_commit_dates` row is
/// reported as [`OldestUploadError::BackfillIncomplete`], mirroring the
/// baseline's `backfillIncompleteError` (the query sorts NULLs first, so the
/// error is per repository, not per upload).
pub async fn get_commit_and_date_for_oldest_upload(
    pool: &PgPool,
    repository_id: i32,
) -> Result<Option<CommitWithDate>, OldestUploadError> {
    // Note: the baseline checks against '-infinity', as the backfill uses it
    // as a sentinel for commits gitserver no longer knows about.
    let row = sqlx::query(
        r#"
SELECT
	u.commit, cd.committed_at
FROM lsif_uploads u
LEFT JOIN codeintel_commit_dates cd ON cd.repository_id = u.repository_id AND cd.commit_bytea = decode(u.commit, 'hex')
WHERE
	u.repository_id = $1 AND
	u.state = 'completed' AND
	(cd.committed_at != '-infinity' OR cd.committed_at IS NULL)
ORDER BY cd.committed_at NULLS FIRST
LIMIT 1
"#,
    )
    .bind(repository_id)
    .fetch_optional(pool)
    .await?;

    let Some(row) = row else {
        return Ok(None);
    };

    let commit: String = row.try_get("commit")?;
    let committed_at: Option<OffsetDateTime> = row.try_get("committed_at")?;
    match committed_at {
        Some(committer_date) => Ok(Some(CommitWithDate {
            commit,
            committer_date,
        })),
        None => Err(OldestUploadError::BackfillIncomplete { repository_id }),
    }
}

/// `UpdateUploadsVisibleToCommits`.
///
/// `refs` maps a commit ID to the refs pointing at it
/// (`mapRefsToCommits`). `now` replaces the baseline's
/// `transaction_timestamp()` for `lsif_dirty_repositories.updated_at`; the
/// job passes `None`.
#[allow(clippy::too_many_arguments)]
pub async fn update_uploads_visible_to_commits(
    pool: &PgPool,
    repository_id: i32,
    commit_graph: &CommitGraph,
    refs: &HashMap<String, Vec<RefInfo>>,
    max_age_for_non_stale_branches: Duration,
    max_age_for_non_stale_tags: Duration,
    dirty_token: i32,
    now: Option<OffsetDateTime>,
) -> Result<(), sqlx::Error> {
    let mut tx = pool.begin().await?;

    // Determine the retention policy for this repository.
    let (max_age_for_non_stale_branches, max_age_for_non_stale_tags) =
        refine_retention_configuration(
            &mut tx,
            repository_id,
            max_age_for_non_stale_branches,
            max_age_for_non_stale_tags,
        )
        .await?;

    // Pull all queryable upload metadata known to this repository.
    let commit_graph_view = scan_commit_graph_view(&mut tx, repository_id).await?;

    // Determine which uploads are visible to which commits.
    let graph = Graph::new(commit_graph, &commit_graph_view);

    create_temporary_tables(&mut tx).await?;

    // Write the graph into temporary tables.
    for envelope in graph.stream() {
        match envelope {
            sg_codeintel::commitgraph::Envelope::Uploads(visibility) => {
                sqlx::query(
                    "INSERT INTO t_lsif_nearest_uploads (commit_bytea, uploads) \
                     VALUES (decode($1, 'hex'), $2::jsonb)",
                )
                .bind(&visibility.commit)
                .bind(serialize_upload_meta_list(&visibility.uploads))
                .execute(&mut *tx)
                .await?;
            }
            sg_codeintel::commitgraph::Envelope::Links(link) => {
                sqlx::query(
                    "INSERT INTO t_lsif_nearest_uploads_links \
                     (commit_bytea, ancestor_commit_bytea, distance) \
                     VALUES (decode($1, 'hex'), decode($2, 'hex'), $3)",
                )
                .bind(&link.commit)
                .bind(&link.ancestor_commit)
                .bind(i32::try_from(link.distance).unwrap_or(i32::MAX))
                .execute(&mut *tx)
                .await?;
            }
        }
    }

    // `sanitizeCommitInput`'s refs half: drop refs whose type has no max age
    // or that are older than it, unless they are the head/default branch.
    let max_ages = HashMap::from([
        (RefType::Branch, max_age_for_non_stale_branches),
        (RefType::Tag, max_age_for_non_stale_tags),
    ]);
    let now_utc = OffsetDateTime::now_utc();
    for (commit, commit_refs) in refs {
        let mut is_default_branch = false;
        let mut names: Vec<&str> = Vec::with_capacity(commit_refs.len());

        for commit_ref in commit_refs {
            if commit_ref.is_head {
                is_default_branch = true;
            } else {
                let Some(max_age) = max_ages.get(&commit_ref.ref_type) else {
                    continue;
                };
                let created_at = commit_ref.created_at.unwrap_or(OffsetDateTime::UNIX_EPOCH);
                if now_utc - created_at > *max_age {
                    continue;
                }
            }
            names.push(&commit_ref.short_name);
        }
        names.sort_unstable();

        if names.is_empty() {
            continue;
        }
        let branch_or_tag_name = names.join(",");

        for upload_meta in graph.uploads_visible_at_commit(commit) {
            sqlx::query(
                "INSERT INTO t_lsif_uploads_visible_at_tip \
                 (upload_id, branch_or_tag_name, is_default_branch) VALUES ($1, $2, $3)",
            )
            .bind(upload_meta.upload_id)
            .bind(&branch_or_tag_name)
            .bind(is_default_branch)
            .execute(&mut *tx)
            .await?;
        }
    }

    // Persist data to permanent tables.
    persist_nearest_uploads(&mut tx, repository_id).await?;
    persist_nearest_uploads_links(&mut tx, repository_id).await?;
    persist_uploads_visible_at_tip(&mut tx, repository_id).await?;

    if dirty_token != 0 {
        // Only raise update_token; the most recent upload token wins.
        match now {
            Some(now) => {
                sqlx::query(
                    "UPDATE lsif_dirty_repositories SET update_token = GREATEST(update_token, $1), \
                     updated_at = $2 WHERE repository_id = $3",
                )
                .bind(dirty_token)
                .bind(now)
                .bind(repository_id)
                .execute(&mut *tx)
                .await?;
            }
            None => {
                sqlx::query(
                    "UPDATE lsif_dirty_repositories SET update_token = GREATEST(update_token, $1), \
                     updated_at = transaction_timestamp() WHERE repository_id = $2",
                )
                .bind(dirty_token)
                .bind(repository_id)
                .execute(&mut *tx)
                .await?;
            }
        }
    }

    // All completed uploads are now visible. Mark any uploads queued for
    // deletion as deleted; they are no longer reachable from the commit graph.
    sqlx::query(
        "SET LOCAL codeintel.lsif_uploads_audit.reason = 'upload not reachable within the commit graph'",
    )
    .execute(&mut *tx)
    .await?;
    sqlx::query(
        r#"
WITH
candidates AS (
	SELECT u.id
	FROM lsif_uploads u
	WHERE u.state = 'deleting' AND u.repository_id = $1

	-- Lock these rows in a deterministic order so that we don't
	-- deadlock with other processes updating the lsif_uploads table.
	ORDER BY u.id FOR UPDATE
)
UPDATE lsif_uploads
SET state = 'deleted'
WHERE id IN (SELECT id FROM candidates)
"#,
    )
    .bind(repository_id)
    .execute(&mut *tx)
    .await?;

    tx.commit().await
}

/// `refineRetentionConfiguration`: the repository's configured maximum ages,
/// or the given defaults when it has no retention configuration.
async fn refine_retention_configuration(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    repository_id: i32,
    max_age_for_non_stale_branches: Duration,
    max_age_for_non_stale_tags: Duration,
) -> Result<(Duration, Duration), sqlx::Error> {
    let row = sqlx::query(
        r#"
SELECT max_age_for_non_stale_branches_seconds, max_age_for_non_stale_tags_seconds
FROM lsif_retention_configuration
WHERE repository_id = $1
"#,
    )
    .bind(repository_id)
    .fetch_optional(&mut **tx)
    .await?;

    let Some(row) = row else {
        return Ok((max_age_for_non_stale_branches, max_age_for_non_stale_tags));
    };

    let branches: i32 = row.try_get("max_age_for_non_stale_branches_seconds")?;
    let tags: i32 = row.try_get("max_age_for_non_stale_tags_seconds")?;
    Ok((
        Duration::seconds(i64::from(branches)),
        Duration::seconds(i64::from(tags)),
    ))
}

/// `scanCommitGraphView`.
async fn scan_commit_graph_view(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    repository_id: i32,
) -> Result<sg_codeintel::commitgraph::CommitGraphView, sqlx::Error> {
    let rows = sqlx::query(
        r#"
SELECT id, commit, md5(root || ':' || indexer) as token, 0 as distance FROM lsif_uploads WHERE state = 'completed' AND repository_id = $1
"#,
    )
    .bind(repository_id)
    .fetch_all(&mut **tx)
    .await?;

    let mut view = sg_codeintel::commitgraph::CommitGraphView::new();
    for row in rows {
        let upload_id: i32 = row.try_get("id")?;
        let commit: String = row.try_get("commit")?;
        let token: String = row.try_get("token")?;
        let distance: i32 = row.try_get("distance")?;
        view.add(
            UploadMeta {
                upload_id,
                distance: u32::try_from(distance).unwrap_or(0),
            },
            &commit,
            &token,
        );
    }
    Ok(view)
}

/// `createTemporaryNearestUploadsTables`.
async fn create_temporary_tables(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<(), sqlx::Error> {
    for statement in [
        r#"
CREATE TEMPORARY TABLE t_lsif_nearest_uploads (
	commit_bytea bytea NOT NULL,
	uploads      jsonb NOT NULL
) ON COMMIT DROP
"#,
        r#"
CREATE TEMPORARY TABLE t_lsif_nearest_uploads_links (
	commit_bytea          bytea NOT NULL,
	ancestor_commit_bytea bytea NOT NULL,
	distance              integer NOT NULL
) ON COMMIT DROP
"#,
        r#"
CREATE TEMPORARY TABLE t_lsif_uploads_visible_at_tip (
	upload_id integer NOT NULL,
	branch_or_tag_name text NOT NULL,
	is_default_branch boolean NOT NULL
) ON COMMIT DROP
"#,
    ] {
        sqlx::query(statement).execute(&mut **tx).await?;
    }
    Ok(())
}

/// `persistNearestUploads`.
async fn persist_nearest_uploads(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    repository_id: i32,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"
INSERT INTO lsif_nearest_uploads
SELECT $1, source.commit_bytea, source.uploads
FROM t_lsif_nearest_uploads source
WHERE source.commit_bytea NOT IN (SELECT nu.commit_bytea FROM lsif_nearest_uploads nu WHERE nu.repository_id = $2)
"#,
    )
    .bind(repository_id)
    .bind(repository_id)
    .execute(&mut **tx)
    .await?;

    sqlx::query(
        r#"
UPDATE lsif_nearest_uploads nu
SET uploads = source.uploads
FROM t_lsif_nearest_uploads source
WHERE
	nu.repository_id = $1 AND
	nu.commit_bytea = source.commit_bytea AND
	nu.uploads != source.uploads
"#,
    )
    .bind(repository_id)
    .execute(&mut **tx)
    .await?;

    sqlx::query(
        r#"
DELETE FROM lsif_nearest_uploads nu
WHERE
	nu.repository_id = $1 AND
	nu.commit_bytea NOT IN (SELECT source.commit_bytea FROM t_lsif_nearest_uploads source)
"#,
    )
    .bind(repository_id)
    .execute(&mut **tx)
    .await?;

    Ok(())
}

/// `persistNearestUploadsLinks`.
async fn persist_nearest_uploads_links(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    repository_id: i32,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"
INSERT INTO lsif_nearest_uploads_links
SELECT $1, source.commit_bytea, source.ancestor_commit_bytea, source.distance
FROM t_lsif_nearest_uploads_links source
WHERE source.commit_bytea NOT IN (SELECT nul.commit_bytea FROM lsif_nearest_uploads_links nul WHERE nul.repository_id = $2)
"#,
    )
    .bind(repository_id)
    .bind(repository_id)
    .execute(&mut **tx)
    .await?;

    sqlx::query(
        r#"
UPDATE lsif_nearest_uploads_links nul
SET ancestor_commit_bytea = source.ancestor_commit_bytea, distance = source.distance
FROM t_lsif_nearest_uploads_links source
WHERE
	nul.repository_id = $1 AND
	nul.commit_bytea = source.commit_bytea AND
	nul.ancestor_commit_bytea != source.ancestor_commit_bytea AND
	nul.distance != source.distance
"#,
    )
    .bind(repository_id)
    .execute(&mut **tx)
    .await?;

    sqlx::query(
        r#"
DELETE FROM lsif_nearest_uploads_links nul
WHERE
	nul.repository_id = $1 AND
	nul.commit_bytea NOT IN (SELECT source.commit_bytea FROM t_lsif_nearest_uploads_links source)
"#,
    )
    .bind(repository_id)
    .execute(&mut **tx)
    .await?;

    Ok(())
}

/// `persistUploadsVisibleAtTip` (the baseline's update query is nil).
async fn persist_uploads_visible_at_tip(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    repository_id: i32,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"
INSERT INTO lsif_uploads_visible_at_tip
SELECT $1, source.upload_id, source.branch_or_tag_name, source.is_default_branch
FROM t_lsif_uploads_visible_at_tip source
WHERE NOT EXISTS (
	SELECT 1
	FROM lsif_uploads_visible_at_tip vat
	WHERE
		vat.repository_id = $2 AND
		vat.upload_id = source.upload_id AND
		vat.branch_or_tag_name = source.branch_or_tag_name AND
		vat.is_default_branch = source.is_default_branch
)
"#,
    )
    .bind(repository_id)
    .bind(repository_id)
    .execute(&mut **tx)
    .await?;

    sqlx::query(
        r#"
DELETE FROM lsif_uploads_visible_at_tip vat
WHERE
	vat.repository_id = $1 AND
	NOT EXISTS (
		SELECT 1
		FROM t_lsif_uploads_visible_at_tip source
		WHERE
			source.upload_id = vat.upload_id AND
			source.branch_or_tag_name = vat.branch_or_tag_name AND
			source.is_default_branch = vat.is_default_branch
	)
"#,
    )
    .bind(repository_id)
    .execute(&mut **tx)
    .await?;

    Ok(())
}

/// `uploadMetaListSerializer.Serialize`: `{"<upload_id>":<distance>,...}`.
fn serialize_upload_meta_list(uploads: &[UploadMeta]) -> String {
    let mut buf = String::from("{");
    for (index, upload) in uploads.iter().enumerate() {
        if index > 0 {
            buf.push(',');
        }
        buf.push('"');
        buf.push_str(&upload.upload_id.to_string());
        buf.push_str("\":");
        buf.push_str(&upload.distance.to_string());
    }
    buf.push('}');
    buf
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn upload_meta_list_serialization_matches_the_baseline() {
        assert_eq!(serialize_upload_meta_list(&[]), "{}");
        assert_eq!(
            serialize_upload_meta_list(&[
                UploadMeta {
                    upload_id: 45,
                    distance: 0
                },
                UploadMeta {
                    upload_id: 50,
                    distance: 6
                },
            ]),
            r#"{"45":0,"50":6}"#
        );
    }

    #[test]
    fn backfill_incomplete_message_matches_the_baseline() {
        let err = OldestUploadError::BackfillIncomplete { repository_id: 42 };
        assert_eq!(
            err.to_string(),
            "repository 42 has not yet completed its backfill of commit dates"
        );
    }
}
