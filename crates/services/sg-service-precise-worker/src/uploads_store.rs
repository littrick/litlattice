#![forbid(unsafe_code)]

//! The uploads-store mutations `HandleRawUpload` performs after the code graph
//! has been written (`internal/codeintel/uploads/internal/store`):
//!
//! - `DeleteOverlappingCompletedUploads` (`processing.go`)
//! - `UpdatePackages` / `UpdatePackageReferences` (`dependencies.go`)
//! - `InsertDependencySyncingJob` (`misc.go`)
//! - `SetRepositoryAsDirty` (`expiration.go`)
//!
//! All of these operate on the **frontend** database and run inside the same
//! transaction as the handler's completion step.
//!
//! Baseline detail: `UpdatePackages` and `UpdatePackageReferences` stage rows
//! in `ON COMMIT DROP` temporary tables and copy them in a nested transaction.
//! This port inserts the rows into the target tables directly; the committed
//! state is identical (see module 07 §19.2).

use sqlx::{PgConnection, QueryBuilder};

use sg_scip::Package;

use crate::scip_stream::PackageReference;

/// Rows per multi-row `INSERT` (well below PostgreSQL's 65535-parameter limit).
const INSERT_CHUNK_ROWS: usize = 1000;

/// `setRepositoryAsDirtyQuery`.
const SET_REPOSITORY_AS_DIRTY_QUERY: &str = "
INSERT INTO lsif_dirty_repositories (repository_id, dirty_token, update_token)
VALUES ($1, 1, 0)
ON CONFLICT (repository_id) DO UPDATE SET
    dirty_token = lsif_dirty_repositories.dirty_token + 1,
    set_dirty_at = CASE
        WHEN lsif_dirty_repositories.update_token = lsif_dirty_repositories.dirty_token THEN NOW()
        ELSE lsif_dirty_repositories.set_dirty_at
    END
";

/// `deleteOverlappingCompletedUploadsQuery`.
const DELETE_OVERLAPPING_COMPLETED_UPLOADS_QUERY: &str = "
WITH
candidates AS (
	SELECT u.id
	FROM lsif_uploads u
	WHERE
		u.state = 'completed' AND
		u.repository_id = $1 AND
		u.commit = $2 AND
		u.root = $3 AND
		u.indexer = $4

	-- Lock these rows in a deterministic order so that we don't
	-- deadlock with other processes updating the lsif_uploads table.
	ORDER BY u.id FOR UPDATE
),
updated AS (
	UPDATE lsif_uploads
	SET state = 'deleting'
	WHERE id IN (SELECT id FROM candidates)
	RETURNING 1
)
SELECT COUNT(*)::int FROM updated
";

/// `DeleteOverlappingCompletedUploads`: move completed uploads with the same
/// `(repository, commit, root, indexer)` to `deleting` so the unique index
/// admits this upload once it is marked complete. Returns the row count.
///
/// The audit-log reason is set for the duration of the caller's transaction
/// (`basestore.Store.SetLocal`).
pub async fn delete_overlapping_completed_uploads(
    conn: &mut PgConnection,
    repository_id: i32,
    commit: &str,
    root: &str,
    indexer: &str,
) -> Result<i32, sqlx::Error> {
    sqlx::query(
        "SELECT set_config('codeintel.lsif_uploads_audit.reason', \
         'upload overlapping with a newer upload', true)",
    )
    .execute(&mut *conn)
    .await?;

    let count: i32 = sqlx::query_scalar(DELETE_OVERLAPPING_COMPLETED_UPLOADS_QUERY)
        .bind(repository_id)
        .bind(commit)
        .bind(root)
        .bind(indexer)
        .fetch_one(&mut *conn)
        .await?;
    Ok(count)
}

/// `UpdatePackages`: attach the indexed packages to the upload.
pub async fn update_packages(
    conn: &mut PgConnection,
    upload_id: i32,
    packages: &[Package],
) -> Result<(), sqlx::Error> {
    if packages.is_empty() {
        return Ok(());
    }

    for chunk in packages.chunks(INSERT_CHUNK_ROWS) {
        let mut query = QueryBuilder::new(
            "INSERT INTO lsif_packages (dump_id, scheme, manager, name, version) ",
        );
        query.push_values(chunk, |mut row, package| {
            row.push_bind(upload_id)
                .push_bind(&package.scheme)
                .push_bind(&package.manager)
                .push_bind(&package.name)
                .push_bind(&package.version);
        });
        query.build().execute(&mut *conn).await?;
    }
    Ok(())
}

/// `UpdatePackageReferences`: attach the upload's import references (used to
/// trigger cross-repository dependency syncing).
pub async fn update_package_references(
    conn: &mut PgConnection,
    upload_id: i32,
    references: &[PackageReference],
) -> Result<(), sqlx::Error> {
    if references.is_empty() {
        return Ok(());
    }

    for chunk in references.chunks(INSERT_CHUNK_ROWS) {
        let mut query = QueryBuilder::new(
            "INSERT INTO lsif_references (dump_id, scheme, manager, name, version) ",
        );
        query.push_values(chunk, |mut row, reference| {
            let package = &reference.package;
            row.push_bind(upload_id)
                .push_bind(&package.scheme)
                .push_bind(&package.manager)
                .push_bind(&package.name)
                .push_bind(&package.version);
        });
        query.build().execute(&mut *conn).await?;
    }
    Ok(())
}

/// `InsertDependencySyncingJob`: enqueue the dependency-syncing companion job
/// and return its id.
pub async fn insert_dependency_syncing_job(
    conn: &mut PgConnection,
    upload_id: i32,
) -> Result<i32, sqlx::Error> {
    sqlx::query_scalar(
        "INSERT INTO lsif_dependency_syncing_jobs (upload_id) VALUES ($1) RETURNING id",
    )
    .bind(upload_id)
    .fetch_one(&mut *conn)
    .await
}

/// `SetRepositoryAsDirty`: bump the repository's dirty token so the commit
/// updater recomputes visible uploads.
pub async fn set_repository_as_dirty(
    conn: &mut PgConnection,
    repository_id: i32,
) -> Result<(), sqlx::Error> {
    sqlx::query(SET_REPOSITORY_AS_DIRTY_QUERY)
        .bind(repository_id)
        .execute(&mut *conn)
        .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dirty_query_matches_baseline_shape() {
        // Guards against accidentally rewriting the upsert: the dirty token
        // must increment on conflict and `set_dirty_at` must only change when
        // the previous token pair was clean.
        assert!(SET_REPOSITORY_AS_DIRTY_QUERY
            .contains("dirty_token = lsif_dirty_repositories.dirty_token + 1"));
        assert!(SET_REPOSITORY_AS_DIRTY_QUERY.contains(
            "WHEN lsif_dirty_repositories.update_token = lsif_dirty_repositories.dirty_token"
        ));
    }
}
