//! `internal/codeintel/uploads/internal/store/commitgraph.go`'s write path:
//! `UpdateUploadsVisibleToCommits` and its helpers.
//!
//! The SQL is the baseline's, including the temporary-table transfer that
//! avoids rewriting every row of the persisted tables on each update. The
//! whole operation runs in one transaction; a failure rolls everything back so
//! the dirty token is never advanced without the graph being persisted.
//!
//! This module only reads/writes the existing schema (M6 storage contract); it
//! adds no tables, columns, indexes or Redis keys.

use std::collections::BTreeMap;
use std::time::Duration;

use sg_codeintel::commitgraph::{CommitGraph, CommitGraphView, Envelope, Graph, UploadMeta};
use sqlx::postgres::{PgPool, Postgres};
use sqlx::{Row, Transaction};
use time::OffsetDateTime;

/// `gitdomain.RefType`. The max-age map in `sanitizeCommitInput` only has
/// entries for branches and tags; every other type is filtered out.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RefType {
    Unknown,
    Branch,
    Tag,
}

/// `gitdomain.Ref`'s commit-graph fields.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommitGraphRef {
    pub short_name: String,
    pub ref_type: RefType,
    /// `gitdomain.Ref.CreatedDate`; `None` is the Unix epoch (a missing
    /// protobuf timestamp decodes to the zero `time.Time`).
    pub created_at: Option<OffsetDateTime>,
    pub is_head: bool,
}

/// `map[string][]gitdomain.Ref`.
pub type RefsByCommit = BTreeMap<String, Vec<CommitGraphRef>>;

/// `UpdateUploadsVisibleToCommits`: decorate `commit_graph` with the uploads
/// stored for `repository_id`, persist the nearest-upload/link/tip-visibility
/// tables, advance the dirty token and mark unreachable `deleting` uploads as
/// `deleted`.
///
/// `now == None` is the baseline's zero `time.Time`: `updated_at` is set to
/// `transaction_timestamp()`.
#[allow(clippy::too_many_arguments)]
pub async fn update_uploads_visible_to_commits(
    pool: &PgPool,
    repository_id: i64,
    commit_graph: &CommitGraph,
    refs: &RefsByCommit,
    max_age_for_non_stale_branches: Duration,
    max_age_for_non_stale_tags: Duration,
    dirty_token: i64,
    now: Option<OffsetDateTime>,
) -> Result<(), sqlx::Error> {
    let mut tx = pool.begin().await?;

    // Determine the retention policy for this repository.
    let (max_age_branches, max_age_tags) = refine_retention_configuration(
        &mut tx,
        repository_id,
        max_age_for_non_stale_branches,
        max_age_for_non_stale_tags,
    )
    .await?;

    // Pull all queryable upload metadata known to this repository so we can
    // correlate it with the current commit graph.
    let view = load_commit_graph_view(&mut tx, repository_id).await?;

    // Determine which uploads are visible to which commits.
    let graph = Graph::new(commit_graph, &view);
    let input = sanitize_commit_input(
        &graph,
        refs,
        max_age_branches,
        max_age_tags,
        OffsetDateTime::now_utc(),
    );

    // Write the graph into temporary tables, then transfer the deltas into the
    // permanent tables.
    create_temporary_tables(&mut tx).await?;
    write_visible_uploads(&mut tx, &input).await?;
    persist_nearest_uploads(&mut tx, repository_id).await?;
    persist_nearest_uploads_links(&mut tx, repository_id).await?;
    persist_uploads_visible_at_tip(&mut tx, repository_id).await?;

    if dirty_token != 0 {
        // If the user requests us to clear a dirty token, set the updated_token
        // value to the dirty token if it wouldn't decrease the value. Dirty
        // repositories are determined by having a non-equal dirty and update
        // token, and we want the most recent upload token to win this write.
        match now {
            Some(now) => {
                sqlx::query(
                    "UPDATE lsif_dirty_repositories \
                     SET update_token = GREATEST(update_token, $1), updated_at = $2 \
                     WHERE repository_id = $3",
                )
                .bind(dirty_token as i32)
                .bind(now)
                .bind(repository_id as i32)
                .execute(&mut *tx)
                .await?;
            }
            None => {
                sqlx::query(
                    "UPDATE lsif_dirty_repositories \
                     SET update_token = GREATEST(update_token, $1), \
                         updated_at = transaction_timestamp() \
                     WHERE repository_id = $2",
                )
                .bind(dirty_token as i32)
                .bind(repository_id as i32)
                .execute(&mut *tx)
                .await?;
            }
        }
    }

    // All completed uploads are now visible. Mark any uploads queued for
    // deletion as deleted as they are no longer reachable from the commit
    // graph and cannot be used to fulfill any API requests.
    set_audit_reason(&mut tx, AUDIT_REASON_NOT_REACHABLE).await?;
    delete_uploads_queued_for_deletion(&mut tx, repository_id).await?;
    set_audit_reason(&mut tx, "").await?;

    tx.commit().await?;
    Ok(())
}

/// `codeintel.lsif_uploads_audit.reason` for unreachable uploads.
pub const AUDIT_REASON_NOT_REACHABLE: &str = "upload not reachable within the commit graph";

/// `refineRetentionConfiguration`: the configured max ages override the
/// caller's defaults when the repository has a retention row.
async fn refine_retention_configuration(
    tx: &mut Transaction<'_, Postgres>,
    repository_id: i64,
    mut max_age_for_non_stale_branches: Duration,
    mut max_age_for_non_stale_tags: Duration,
) -> Result<(Duration, Duration), sqlx::Error> {
    let row = sqlx::query(
        "SELECT max_age_for_non_stale_branches_seconds, \
                max_age_for_non_stale_tags_seconds \
         FROM lsif_retention_configuration \
         WHERE repository_id = $1",
    )
    .bind(repository_id as i32)
    .fetch_optional(&mut **tx)
    .await?;

    if let Some(row) = row {
        let branches: i32 = row.try_get(0)?;
        let tags: i32 = row.try_get(1)?;
        max_age_for_non_stale_branches = seconds_to_duration(branches);
        max_age_for_non_stale_tags = seconds_to_duration(tags);
    }

    Ok((max_age_for_non_stale_branches, max_age_for_non_stale_tags))
}

/// `time.Second * time.Duration(v)`. Negative values make every ref stale in
/// the baseline; clamping to zero keeps that behavior for any non-zero age.
fn seconds_to_duration(seconds: i32) -> Duration {
    if seconds <= 0 {
        Duration::ZERO
    } else {
        Duration::from_secs(seconds as u64)
    }
}

/// `calculateVisibleUploadsCommitGraphQuery` + `CommitGraphView.Add`. The
/// query's distance is always zero, so only the upload id/commit/token matter.
async fn load_commit_graph_view(
    tx: &mut Transaction<'_, Postgres>,
    repository_id: i64,
) -> Result<CommitGraphView, sqlx::Error> {
    let rows = sqlx::query(
        "SELECT id, commit, md5(root || ':' || indexer) as token, 0 as distance \
         FROM lsif_uploads \
         WHERE state = 'completed' AND repository_id = $1",
    )
    .bind(repository_id as i32)
    .fetch_all(&mut **tx)
    .await?;

    let mut view = CommitGraphView::default();
    for row in rows {
        let id: i32 = row.try_get("id")?;
        let commit: String = row.try_get("commit")?;
        let token: String = row.try_get("token")?;
        view.add(
            UploadMeta {
                upload_id: i64::from(id),
                distance: 0,
            },
            &commit,
            &token,
        );
    }
    Ok(view)
}

/// `sanitizedCommitInput`: the three row streams the baseline feeds into the
/// temporary tables.
#[derive(Debug, Default, PartialEq, Eq)]
struct SanitizedCommitInput {
    /// `(commit hex, {"upload_id": distance, ...} json)`
    nearest_uploads: Vec<(String, String)>,
    /// `(commit hex, ancestor commit hex, distance)`
    nearest_uploads_links: Vec<(String, String, i32)>,
    /// `(upload_id, "branch,tag", is_default_branch)`
    uploads_visible_at_tip: Vec<(i32, String, bool)>,
}

/// `sanitizeCommitInput`: walks the graph stream and filters refs by type and
/// age. The age test uses the wall clock (`time.Since` in the baseline), not
/// the caller's `now` value.
fn sanitize_commit_input(
    graph: &Graph,
    refs: &RefsByCommit,
    max_age_for_non_stale_branches: Duration,
    max_age_for_non_stale_tags: Duration,
    now: OffsetDateTime,
) -> SanitizedCommitInput {
    let mut input = SanitizedCommitInput::default();

    for envelope in graph.stream() {
        match envelope {
            Envelope::Uploads(relationship) => input
                .nearest_uploads
                .push((relationship.commit, upload_meta_json(&relationship.uploads))),
            Envelope::Links(relationship) => input.nearest_uploads_links.push((
                relationship.commit,
                relationship.ancestor_commit,
                relationship.distance as i32,
            )),
        }
    }

    for (commit, commit_refs) in refs {
        let mut is_default_branch = false;
        let mut names = Vec::with_capacity(commit_refs.len());

        for commit_ref in commit_refs {
            if commit_ref.is_head {
                is_default_branch = true;
            } else {
                let max_age = match commit_ref.ref_type {
                    RefType::Branch => Some(max_age_for_non_stale_branches),
                    RefType::Tag => Some(max_age_for_non_stale_tags),
                    // `ok == false` in the baseline: filtered out.
                    RefType::Unknown => None,
                };
                let Some(max_age) = max_age else {
                    continue;
                };
                let created_at = commit_ref.created_at.unwrap_or(UNIX_EPOCH);
                if exceeds_max_age(now, created_at, max_age) {
                    continue;
                }
            }

            names.push(commit_ref.short_name.clone());
        }
        names.sort();

        if names.is_empty() {
            continue;
        }

        let joined = names.join(",");
        for upload_meta in graph.uploads_visible_at_commit(commit) {
            input.uploads_visible_at_tip.push((
                upload_meta.upload_id as i32,
                joined.clone(),
                is_default_branch,
            ));
        }
    }

    input
}

/// `time.Since(created) > maxAge`, using a signed nanosecond difference so a
/// future `created` is not stale.
fn exceeds_max_age(now: OffsetDateTime, created: OffsetDateTime, max_age: Duration) -> bool {
    let seconds = i128::from(now.unix_timestamp() - created.unix_timestamp());
    let nanos = i128::from(now.nanosecond()) - i128::from(created.nanosecond());
    let age_nanos = seconds * 1_000_000_000 + nanos;
    age_nanos > max_age.as_nanos() as i128
}

fn upload_meta_json(uploads: &[UploadMeta]) -> String {
    let mut map = serde_json::Map::new();
    for upload in uploads {
        map.insert(
            upload.upload_id.to_string(),
            serde_json::Value::from(upload.distance),
        );
    }
    serde_json::Value::Object(map).to_string()
}

const UNIX_EPOCH: OffsetDateTime = OffsetDateTime::UNIX_EPOCH;

/// `createTemporaryNearestUploadsTables`.
async fn create_temporary_tables(tx: &mut Transaction<'_, Postgres>) -> Result<(), sqlx::Error> {
    sqlx::query(
        "CREATE TEMPORARY TABLE t_lsif_nearest_uploads ( \
             commit_bytea bytea NOT NULL, \
             uploads      jsonb NOT NULL \
         ) ON COMMIT DROP",
    )
    .execute(&mut **tx)
    .await?;
    sqlx::query(
        "CREATE TEMPORARY TABLE t_lsif_nearest_uploads_links ( \
             commit_bytea          bytea NOT NULL, \
             ancestor_commit_bytea bytea NOT NULL, \
             distance              integer NOT NULL \
         ) ON COMMIT DROP",
    )
    .execute(&mut **tx)
    .await?;
    sqlx::query(
        "CREATE TEMPORARY TABLE t_lsif_uploads_visible_at_tip ( \
             upload_id integer NOT NULL, \
             branch_or_tag_name text NOT NULL, \
             is_default_branch boolean NOT NULL \
         ) ON COMMIT DROP",
    )
    .execute(&mut **tx)
    .await?;
    Ok(())
}

/// `writeVisibleUploads`: bulk-load the sanitized rows into the temporary
/// tables. `batch.Inserter` uses the same INSERT shape; `unnest` keeps the
/// parameter count constant regardless of the row count.
async fn write_visible_uploads(
    tx: &mut Transaction<'_, Postgres>,
    input: &SanitizedCommitInput,
) -> Result<(), sqlx::Error> {
    let commits: Vec<String> = input
        .nearest_uploads
        .iter()
        .map(|(commit, _)| commit.clone())
        .collect();
    let uploads: Vec<String> = input
        .nearest_uploads
        .iter()
        .map(|(_, uploads)| uploads.clone())
        .collect();
    sqlx::query(
        "INSERT INTO t_lsif_nearest_uploads (commit_bytea, uploads) \
         SELECT decode(c, 'hex'), u::jsonb \
         FROM unnest($1::text[], $2::text[]) AS t(c, u)",
    )
    .bind(&commits)
    .bind(&uploads)
    .execute(&mut **tx)
    .await?;

    let commits: Vec<String> = input
        .nearest_uploads_links
        .iter()
        .map(|(commit, _, _)| commit.clone())
        .collect();
    let ancestors: Vec<String> = input
        .nearest_uploads_links
        .iter()
        .map(|(_, ancestor, _)| ancestor.clone())
        .collect();
    let distances: Vec<i32> = input
        .nearest_uploads_links
        .iter()
        .map(|(_, _, distance)| *distance)
        .collect();
    sqlx::query(
        "INSERT INTO t_lsif_nearest_uploads_links \
             (commit_bytea, ancestor_commit_bytea, distance) \
         SELECT decode(c, 'hex'), decode(a, 'hex'), d \
         FROM unnest($1::text[], $2::text[], $3::integer[]) AS t(c, a, d)",
    )
    .bind(&commits)
    .bind(&ancestors)
    .bind(&distances)
    .execute(&mut **tx)
    .await?;

    let upload_ids: Vec<i32> = input
        .uploads_visible_at_tip
        .iter()
        .map(|(upload_id, _, _)| *upload_id)
        .collect();
    let names: Vec<String> = input
        .uploads_visible_at_tip
        .iter()
        .map(|(_, names, _)| names.clone())
        .collect();
    let is_default_branch: Vec<bool> = input
        .uploads_visible_at_tip
        .iter()
        .map(|(_, _, is_default_branch)| *is_default_branch)
        .collect();
    sqlx::query(
        "INSERT INTO t_lsif_uploads_visible_at_tip \
             (upload_id, branch_or_tag_name, is_default_branch) \
         SELECT u, n, d \
         FROM unnest($1::integer[], $2::text[], $3::boolean[]) AS t(u, n, d)",
    )
    .bind(&upload_ids)
    .bind(&names)
    .bind(&is_default_branch)
    .execute(&mut **tx)
    .await?;

    Ok(())
}

/// `persistNearestUploads` (insert/update/delete).
async fn persist_nearest_uploads(
    tx: &mut Transaction<'_, Postgres>,
    repository_id: i64,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "INSERT INTO lsif_nearest_uploads \
         SELECT $1, source.commit_bytea, source.uploads \
         FROM t_lsif_nearest_uploads source \
         WHERE source.commit_bytea NOT IN ( \
             SELECT nu.commit_bytea FROM lsif_nearest_uploads nu \
             WHERE nu.repository_id = $2 \
         )",
    )
    .bind(repository_id as i32)
    .bind(repository_id as i32)
    .execute(&mut **tx)
    .await?;

    sqlx::query(
        "UPDATE lsif_nearest_uploads nu \
         SET uploads = source.uploads \
         FROM t_lsif_nearest_uploads source \
         WHERE \
             nu.repository_id = $1 AND \
             nu.commit_bytea = source.commit_bytea AND \
             nu.uploads != source.uploads",
    )
    .bind(repository_id as i32)
    .execute(&mut **tx)
    .await?;

    sqlx::query(
        "DELETE FROM lsif_nearest_uploads nu \
         WHERE \
             nu.repository_id = $1 AND \
             nu.commit_bytea NOT IN ( \
                 SELECT source.commit_bytea FROM t_lsif_nearest_uploads source \
             )",
    )
    .bind(repository_id as i32)
    .execute(&mut **tx)
    .await?;

    Ok(())
}

/// `persistNearestUploadsLinks` (insert/update/delete).
async fn persist_nearest_uploads_links(
    tx: &mut Transaction<'_, Postgres>,
    repository_id: i64,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "INSERT INTO lsif_nearest_uploads_links \
         SELECT $1, source.commit_bytea, source.ancestor_commit_bytea, source.distance \
         FROM t_lsif_nearest_uploads_links source \
         WHERE source.commit_bytea NOT IN ( \
             SELECT nul.commit_bytea FROM lsif_nearest_uploads_links nul \
             WHERE nul.repository_id = $2 \
         )",
    )
    .bind(repository_id as i32)
    .bind(repository_id as i32)
    .execute(&mut **tx)
    .await?;

    sqlx::query(
        "UPDATE lsif_nearest_uploads_links nul \
         SET ancestor_commit_bytea = source.ancestor_commit_bytea, \
             distance = source.distance \
         FROM t_lsif_nearest_uploads_links source \
         WHERE \
             nul.repository_id = $1 AND \
             nul.commit_bytea = source.commit_bytea AND \
             nul.ancestor_commit_bytea != source.ancestor_commit_bytea AND \
             nul.distance != source.distance",
    )
    .bind(repository_id as i32)
    .execute(&mut **tx)
    .await?;

    sqlx::query(
        "DELETE FROM lsif_nearest_uploads_links nul \
         WHERE \
             nul.repository_id = $1 AND \
             nul.commit_bytea NOT IN ( \
                 SELECT source.commit_bytea FROM t_lsif_nearest_uploads_links source \
             )",
    )
    .bind(repository_id as i32)
    .execute(&mut **tx)
    .await?;

    Ok(())
}

/// `persistUploadsVisibleAtTip`: insert missing triples and delete rows not in
/// the temporary table. There is no update query in the baseline.
async fn persist_uploads_visible_at_tip(
    tx: &mut Transaction<'_, Postgres>,
    repository_id: i64,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "INSERT INTO lsif_uploads_visible_at_tip \
         SELECT $1, source.upload_id, source.branch_or_tag_name, \
                source.is_default_branch \
         FROM t_lsif_uploads_visible_at_tip source \
         WHERE NOT EXISTS ( \
             SELECT 1 \
             FROM lsif_uploads_visible_at_tip vat \
             WHERE \
                 vat.repository_id = $2 AND \
                 vat.upload_id = source.upload_id AND \
                 vat.branch_or_tag_name = source.branch_or_tag_name AND \
                 vat.is_default_branch = source.is_default_branch \
         )",
    )
    .bind(repository_id as i32)
    .bind(repository_id as i32)
    .execute(&mut **tx)
    .await?;

    sqlx::query(
        "DELETE FROM lsif_uploads_visible_at_tip vat \
         WHERE \
             vat.repository_id = $1 AND \
             NOT EXISTS ( \
                 SELECT 1 \
                 FROM t_lsif_uploads_visible_at_tip source \
                 WHERE \
                     source.upload_id = vat.upload_id AND \
                     source.branch_or_tag_name = vat.branch_or_tag_name AND \
                     source.is_default_branch = vat.is_default_branch \
             )",
    )
    .bind(repository_id as i32)
    .execute(&mut **tx)
    .await?;

    Ok(())
}

/// `tx.db.SetLocal(ctx, "codeintel.lsif_uploads_audit.reason", reason)`.
async fn set_audit_reason(
    tx: &mut Transaction<'_, Postgres>,
    reason: &str,
) -> Result<(), sqlx::Error> {
    sqlx::query("SELECT set_config('codeintel.lsif_uploads_audit.reason', $1, true)")
        .bind(reason)
        .execute(&mut **tx)
        .await?;
    Ok(())
}

/// `calculateVisibleUploadsDeleteUploadsQueuedForDeletionQuery`: mark uploads
/// queued for deletion as deleted once they are no longer reachable.
async fn delete_uploads_queued_for_deletion(
    tx: &mut Transaction<'_, Postgres>,
    repository_id: i64,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "WITH \
         candidates AS ( \
             SELECT u.id \
             FROM lsif_uploads u \
             WHERE u.state = 'deleting' AND u.repository_id = $1 \
             ORDER BY u.id FOR UPDATE \
         ) \
         UPDATE lsif_uploads \
         SET state = 'deleted' \
         WHERE id IN (SELECT id FROM candidates)",
    )
    .bind(repository_id as i32)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use sg_codeintel::commitgraph::{parse_commit_graph, CommitNode};

    fn upload_meta(upload_id: i64, distance: u32) -> UploadMeta {
        UploadMeta {
            upload_id,
            distance,
        }
    }

    #[test]
    fn upload_meta_json_shape_matches_the_baseline() {
        let json = upload_meta_json(&[upload_meta(101, 0), upload_meta(5, 2), upload_meta(44, 10)]);
        assert_eq!(json, r#"{"101":0,"44":10,"5":2}"#);
        assert_eq!(upload_meta_json(&[]), "{}");
    }

    #[test]
    fn exceeds_max_age_is_strict_and_signed() {
        let now = OffsetDateTime::UNIX_EPOCH + Duration::from_secs(100);
        let created = OffsetDateTime::UNIX_EPOCH + Duration::from_secs(10);

        assert!(!exceeds_max_age(now, created, Duration::from_secs(90)));
        assert!(exceeds_max_age(now, created, Duration::from_secs(89)));
        // A ref created in the future is fresh.
        assert!(!exceeds_max_age(created, now, Duration::ZERO));
    }

    #[test]
    fn sanitize_orders_names_and_filters_stale_refs() {
        let graph = Graph::new(
            &parse_commit_graph(&[CommitNode {
                commit: "c".repeat(40),
                parents: vec![],
            }]),
            &CommitGraphView {
                meta: [("c".repeat(40), vec![upload_meta(7, 0)])]
                    .into_iter()
                    .collect(),
                tokens: [(7, "t".to_owned())].into_iter().collect(),
            },
        );

        let old = OffsetDateTime::now_utc() - Duration::from_secs(10_000);
        let fresh = OffsetDateTime::now_utc() - Duration::from_secs(10);
        let refs = [(
            "c".repeat(40),
            vec![
                CommitGraphRef {
                    short_name: "zzz".to_owned(),
                    ref_type: RefType::Tag,
                    created_at: Some(fresh),
                    is_head: false,
                },
                CommitGraphRef {
                    short_name: "main".to_owned(),
                    ref_type: RefType::Branch,
                    created_at: Some(old),
                    is_head: true,
                },
                CommitGraphRef {
                    short_name: "old".to_owned(),
                    ref_type: RefType::Branch,
                    created_at: Some(old),
                    is_head: false,
                },
                CommitGraphRef {
                    short_name: "unknown".to_owned(),
                    ref_type: RefType::Unknown,
                    created_at: Some(fresh),
                    is_head: false,
                },
            ],
        )]
        .into_iter()
        .collect();

        let input = sanitize_commit_input(
            &graph,
            &refs,
            Duration::from_secs(100),
            Duration::from_secs(100),
            OffsetDateTime::now_utc(),
        );

        assert_eq!(
            input.uploads_visible_at_tip,
            vec![(7, "main,zzz".to_owned(), true)]
        );
    }
}
