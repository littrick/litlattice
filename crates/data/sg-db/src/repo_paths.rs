#![forbid(unsafe_code)]

//! `internal/database/repo_paths.go`: `ensureRepoPaths` (shared by the Own
//! recent-contributor signal store and the analytics indexer),
//! `UpdateFileCounts` and `AggregateFileCount`.

use sqlx::postgres::PgPool;
use sqlx::types::time::OffsetDateTime;

/// Go `path.Dir` for slash-separated paths: `Dir("a/b") == "a"`,
/// `Dir("a") == "."`, `Dir("") == "."`, `Dir("/a") == "/"`.
pub fn go_path_dir(path: &str) -> String {
    if path.is_empty() {
        return ".".to_owned();
    }
    // `path.Clean` collapses trailing slashes and repeated separators; the
    // callers feed repository-relative paths, but mirroring Clean keeps the
    // ancestor computation identical for the edge cases.
    let cleaned = clean(path);
    if cleaned == "/" {
        return "/".to_owned();
    }
    match cleaned.rfind('/') {
        None => ".".to_owned(),
        Some(0) => "/".to_owned(),
        Some(index) => cleaned[..index].to_owned(),
    }
}

fn clean(path: &str) -> String {
    let rooted = path.starts_with('/');
    let mut parts: Vec<&str> = Vec::new();
    for part in path.split('/') {
        match part {
            "" | "." => {}
            ".." => {
                if parts.last().is_some_and(|part| *part != "..") {
                    parts.pop();
                } else if !rooted {
                    parts.push("..");
                }
            }
            part => parts.push(part),
        }
    }
    let joined = parts.join("/");
    if rooted {
        format!("/{joined}")
    } else if joined.is_empty() {
        ".".to_owned()
    } else {
        joined
    }
}

/// `pathInsertFmtstr`:
///
/// ```sql
/// WITH already_exists (id) AS (
///     SELECT id FROM repo_paths WHERE repo_id = $1 AND absolute_path = $2
/// ),
/// need_to_insert (id) AS (
///     INSERT INTO repo_paths (repo_id, absolute_path, parent_id)
///     SELECT $3, $4, $5
///     WHERE NOT EXISTS (SELECT FROM repo_paths WHERE repo_id = $6 AND absolute_path = $7)
///     ON CONFLICT (repo_id, absolute_path) DO NOTHING
///     RETURNING id
/// )
/// SELECT id FROM already_exists
/// UNION ALL
/// SELECT id FROM need_to_insert
/// ```
async fn insert_path(
    pool: &PgPool,
    repo_id: i32,
    path: &str,
    parent_id: Option<i32>,
) -> Result<i32, sqlx::Error> {
    sqlx::query_scalar(
        "WITH already_exists (id) AS ( \
             SELECT id FROM repo_paths WHERE repo_id = $1 AND absolute_path = $2 \
         ), need_to_insert (id) AS ( \
             INSERT INTO repo_paths (repo_id, absolute_path, parent_id) \
             SELECT $3, $4, $5 \
             WHERE NOT EXISTS ( \
                 SELECT FROM repo_paths WHERE repo_id = $6 AND absolute_path = $7 \
             ) \
             ON CONFLICT (repo_id, absolute_path) DO NOTHING \
             RETURNING id \
         ) \
         SELECT id FROM already_exists \
         UNION ALL \
         SELECT id FROM need_to_insert",
    )
    .bind(repo_id)
    .bind(path)
    .bind(repo_id)
    .bind(path)
    .bind(parent_id)
    .bind(repo_id)
    .bind(path)
    .fetch_one(pool)
    .await
}

/// `ensureRepoPaths`: insert every ancestor path of `files` (plus the repo
/// root, `""`) and return the IDs of `files` in order.
pub async fn ensure_repo_paths(
    pool: &PgPool,
    files: &[String],
    repo_id: i32,
) -> Result<Vec<i32>, String> {
    let mut paths: Vec<String> = Vec::new();
    for file in files {
        let mut current = file.clone();
        while current != "." {
            paths.push(current.clone());
            current = go_path_dir(&current);
        }
    }
    paths.push(String::new());
    paths.reverse();

    // Remove duplicates, keeping the first (root-most) occurrence.
    let mut seen = std::collections::HashSet::new();
    paths.retain(|path| seen.insert(path.clone()));

    let mut ids: std::collections::HashMap<String, i32> = std::collections::HashMap::new();
    for path in &paths {
        let parent = go_path_dir(path);
        let parent = if parent == "." { String::new() } else { parent };
        let parent_id = if path.is_empty() {
            None
        } else {
            match ids.get(&parent) {
                Some(id) => Some(*id),
                None => return Err(format!("cannot find parent id of {path:?}: this is a bug")),
            }
        };
        let id = insert_path(pool, repo_id, path, parent_id)
            .await
            .map_err(|err| format!("failed to insert or retrieve {path:?}: {err}"))?;
        ids.insert(path.clone(), id);
    }

    files
        .iter()
        .map(|file| {
            ids.get(file).copied().ok_or_else(|| {
                format!("cannot find id of {file:?} which should have been inserted, this is a bug")
            })
        })
        .collect()
}

/// `updateFileCountsFmtstr`:
/// `UPDATE repo_paths SET tree_files_count = $1, tree_files_counts_updated_at = $2 WHERE id = $3`.
pub async fn update_file_counts(
    pool: &PgPool,
    repo_id: i32,
    path: &str,
    total_files: i32,
    timestamp: OffsetDateTime,
) -> Result<i32, String> {
    let path_ids = ensure_repo_paths(pool, &[path.to_owned()], repo_id).await?;
    if path_ids.len() != 1 {
        return Err(format!("want exactly 1 repo path, got {}", path_ids.len()));
    }
    let result = sqlx::query(
        "UPDATE repo_paths \
         SET tree_files_count = $1, tree_files_counts_updated_at = $2 \
         WHERE id = $3",
    )
    .bind(total_files)
    .bind(timestamp)
    .bind(path_ids[0])
    .execute(pool)
    .await
    .map_err(|err| err.to_string())?;
    Ok(result.rows_affected() as i32)
}

/// `aggregateFileCountFmtstr`: the analytics-aware sum of `tree_files_count`
/// for one path, excluding repos matched by the `analytics` configuration's
/// `excluded_repo_patterns`.
pub async fn aggregate_file_count(
    pool: &PgPool,
    repo_id: Option<i32>,
    path: &str,
) -> Result<i32, String> {
    let row: (Option<i64>,) = if let Some(repo_id) = repo_id {
        sqlx::query_as(
            "WITH signal_config AS ( \
                 SELECT * FROM own_signal_configurations WHERE name = 'analytics' LIMIT 1 \
             ) \
             SELECT SUM(COALESCE(p.tree_files_count, 0)) \
             FROM repo_paths AS p \
             WHERE p.absolute_path = $1 AND p.repo_id = $2 AND p.repo_id NOT IN ( \
                 SELECT repo.id FROM repo, signal_config \
                 WHERE repo.name ~~ ANY(signal_config.excluded_repo_patterns) \
             )",
        )
        .bind(path)
        .bind(repo_id)
        .fetch_one(pool)
        .await
        .map_err(|err| err.to_string())?
    } else {
        sqlx::query_as(
            "WITH signal_config AS ( \
                 SELECT * FROM own_signal_configurations WHERE name = 'analytics' LIMIT 1 \
             ) \
             SELECT SUM(COALESCE(p.tree_files_count, 0)) \
             FROM repo_paths AS p \
             WHERE p.absolute_path = $1 AND p.repo_id NOT IN ( \
                 SELECT repo.id FROM repo, signal_config \
                 WHERE repo.name ~~ ANY(signal_config.excluded_repo_patterns) \
             )",
        )
        .bind(path)
        .fetch_one(pool)
        .await
        .map_err(|err| err.to_string())?
    };
    Ok(row.0.unwrap_or(0) as i32)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn go_path_dir_matches_the_baseline() {
        assert_eq!(go_path_dir("a/b/c"), "a/b");
        assert_eq!(go_path_dir("a/b"), "a");
        assert_eq!(go_path_dir("a"), ".");
        assert_eq!(go_path_dir(""), ".");
        assert_eq!(go_path_dir("/a"), "/");
        assert_eq!(go_path_dir("a/b/"), "a");
        assert_eq!(go_path_dir("./a"), ".");
        assert_eq!(go_path_dir("a//b"), "a");
    }
}
