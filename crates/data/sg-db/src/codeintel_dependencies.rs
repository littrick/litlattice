//! `internal/codeintel/dependencies/internal/store`.
//!
//! Ported paths:
//!
//! * `ShouldRefilterPackageRepoRefs`;
//! * `ListPackageRepoRefFilters` with empty options (live filters, ordered by
//!   id);
//! * `ListPackageRepoRefs` with `IncludeBlocked=true`, no scheme/name filter,
//!   `After`/`Limit` pagination;
//! * `UpdateAllBlockedStatuses` (temporary tables + one update statement);
//! * `InsertPackageRepoRefs` (temporary tables + batch inserts, M9.8di).
//!
//! SQL is copied from `internal/codeintel/dependencies/internal/store/store.go`
//! (baseline `c864f15`). `ListPackageRepoRefs`' second `COUNT(DISTINCT lr.id)`
//! query is not needed by this job and is skipped.

use sqlx::postgres::PgPool;
use sqlx::types::time::OffsetDateTime;
use sqlx::Row;

/// `shared.PackageRepoFilter`'s name matcher. JSON keys are the Go struct
/// field names (`{"PackageGlob": "..."}`).
#[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PackageNameFilter {
    #[serde(rename = "PackageGlob")]
    pub package_glob: String,
}

/// `shared.PackageRepoFilter`'s version matcher.
#[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PackageVersionFilter {
    #[serde(rename = "PackageName")]
    pub package_name: String,
    #[serde(rename = "VersionGlob")]
    pub version_glob: String,
}

/// `shared.PackageRepoFilter` (only the fields the applicator reads).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PackageRepoFilter {
    /// `package_repo_filters.id` is `integer` (INT4), not `bigint`.
    pub id: i32,
    pub behaviour: String,
    pub package_scheme: String,
    pub name_filter: Option<PackageNameFilter>,
    pub version_filter: Option<PackageVersionFilter>,
}

/// `shared.PackageRepoRefVersion`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PackageRepoRefVersion {
    pub id: i64,
    /// `package_repo_versions.package_id`; filled by the list path with the
    /// owning ref's id and by `insert_package_repo_refs` from `RETURNING`.
    pub package_ref_id: i64,
    pub version: String,
    pub blocked: bool,
    pub last_checked_at: Option<OffsetDateTime>,
}

/// `shared.PackageRepoReference`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PackageRepoReference {
    pub id: i64,
    pub scheme: String,
    pub name: String,
    pub blocked: bool,
    pub versions: Vec<PackageRepoRefVersion>,
    pub last_checked_at: Option<OffsetDateTime>,
}

/// `dependencies.MinimalPackageRepoRef`: the input of
/// `Store.InsertPackageRepoRefs`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MinimalPackageRepoRef {
    pub scheme: String,
    pub name: String,
    pub versions: Vec<MinimalPackageRepoRefVersion>,
    pub blocked: bool,
    pub last_checked_at: Option<OffsetDateTime>,
}

/// `dependencies.MinimalPackageRepoRefVersion`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MinimalPackageRepoRefVersion {
    pub version: String,
    pub blocked: bool,
    pub last_checked_at: Option<OffsetDateTime>,
}

/// `Store.ShouldRefilterPackageRepoRefs`.
pub async fn should_refilter_package_repo_refs(pool: &PgPool) -> Result<bool, sqlx::Error> {
    let row = sqlx::query(SHOULD_REFILTER_QUERY)
        .fetch_optional(pool)
        .await?;
    Ok(row.is_some())
}

/// `Store.ListPackageRepoRefFilters` with the applicator's empty options:
/// `deleted_at IS NULL`, ordered by id, no limit.
pub async fn list_package_repo_filters(pool: &PgPool) -> Result<Vec<PackageRepoFilter>, String> {
    let rows = sqlx::query(LIST_FILTERS_QUERY)
        .fetch_all(pool)
        .await
        .map_err(|err| err.to_string())?;

    let mut filters = Vec::with_capacity(rows.len());
    for row in rows {
        let matcher: serde_json::Value = row.try_get("matcher").map_err(|err| err.to_string())?;
        let (name_filter, version_filter) = scan_matcher(matcher)?;
        filters.push(PackageRepoFilter {
            id: row.try_get("id").map_err(|err| err.to_string())?,
            behaviour: row.try_get("behaviour").map_err(|err| err.to_string())?,
            package_scheme: row.try_get("scheme").map_err(|err| err.to_string())?,
            name_filter,
            version_filter,
        });
    }
    Ok(filters)
}

/// `scanPackageFilter`'s matcher decoding: try the name filter first, then the
/// version filter (`json.Decoder.DisallowUnknownFields`).
fn scan_matcher(
    matcher: serde_json::Value,
) -> Result<(Option<PackageNameFilter>, Option<PackageVersionFilter>), String> {
    match serde_json::from_value::<PackageNameFilter>(matcher.clone()) {
        Ok(name_filter) => Ok((Some(name_filter), None)),
        Err(_) => serde_json::from_value::<PackageVersionFilter>(matcher)
            .map(|version_filter| (None, Some(version_filter)))
            .map_err(|err| err.to_string()),
    }
}

/// `Store.ListPackageRepoRefs` for the applicator: `IncludeBlocked=true`, no
/// scheme/name condition, `ORDER BY lr.id`, fetching `limit + 1` rows and
/// truncating back to `limit` exactly like the baseline.
pub async fn list_package_repo_refs(
    pool: &PgPool,
    after: i64,
    limit: i64,
) -> Result<Vec<PackageRepoReference>, sqlx::Error> {
    // `after == 0` is the applicator's "first page" marker.
    let rows = if after > 0 {
        sqlx::query(LIST_REFS_AFTER_QUERY)
            .bind(after)
            .bind(limit + 1)
            .fetch_all(pool)
            .await?
    } else {
        sqlx::query(LIST_REFS_QUERY)
            .bind(limit + 1)
            .fetch_all(pool)
            .await?
    };

    let mut refs = Vec::with_capacity(rows.len().min(limit as usize));
    for row in rows {
        let ids: Vec<i64> = row.try_get("vid")?;
        let versions: Vec<String> = row.try_get("version")?;
        let blocked: Vec<bool> = row.try_get("vers_blocked")?;
        let checked: Vec<Option<OffsetDateTime>> = row.try_get("vers_last_checked_at")?;

        let ref_id: i64 = row.try_get("id")?;

        let mut ref_versions = Vec::with_capacity(versions.len());
        for (i, version) in versions.into_iter().enumerate() {
            ref_versions.push(PackageRepoRefVersion {
                id: ids.get(i).copied().unwrap_or_default(),
                package_ref_id: ref_id,
                version,
                blocked: blocked.get(i).copied().unwrap_or_default(),
                last_checked_at: checked.get(i).copied().flatten(),
            });
        }

        refs.push(PackageRepoReference {
            id: ref_id,
            scheme: row.try_get("scheme")?,
            name: row.try_get("name")?,
            blocked: row.try_get("blocked")?,
            versions: ref_versions,
            last_checked_at: row.try_get("last_checked_at")?,
        });
    }

    refs.truncate(limit as usize);
    Ok(refs)
}

/// `Store.InsertPackageRepoRefs`: sort/merge the input, then insert the new
/// package repo refs and versions either side of the transaction's temporary
/// tables. Returns the rows actually created (`newDeps` has an empty
/// `versions` slice, exactly like the baseline's 5-column scan).
///
/// Mirrors `store.go:213`. The baseline inserts in-place and therefore mutates
/// the caller's slice; this function works on a copy with the same stable
/// ordering (byte-wise `scheme`, then `name`).
pub async fn insert_package_repo_refs(
    pool: &PgPool,
    deps: &[MinimalPackageRepoRef],
) -> Result<(Vec<PackageRepoReference>, Vec<PackageRepoRefVersion>), String> {
    // `len(deps) == 0` short-circuits before the transaction is opened.
    if deps.is_empty() {
        return Ok((Vec::new(), Vec::new()));
    }

    // Stable sort by (scheme, name) — byte order, matching `cmp.Compare`.
    let mut sorted: Vec<MinimalPackageRepoRef> = deps.to_vec();
    sorted.sort_by(|a, b| a.scheme.cmp(&b.scheme).then_with(|| a.name.cmp(&b.name)));

    // "first reduce": append the versions of adjacent duplicates to the first.
    let mut merged: Vec<MinimalPackageRepoRef> = Vec::with_capacity(sorted.len());
    for dep in sorted {
        if let Some(last) = merged.last_mut() {
            if last.scheme == dep.scheme && last.name == dep.name {
                last.versions.extend(dep.versions);
                continue;
            }
        }
        merged.push(dep);
    }

    // "then collapse": drop entries with an empty scheme or name.
    merged.retain(|dep| !dep.name.is_empty() && !dep.scheme.is_empty());

    let mut tx = pool.begin().await.map_err(|err| err.to_string())?;
    let result = insert_package_repo_refs_tx(&mut tx, &merged).await;
    match result {
        Ok(value) => {
            tx.commit().await.map_err(|err| err.to_string())?;
            Ok(value)
        }
        Err(err) => {
            let _ = tx.rollback().await;
            Err(err)
        }
    }
}

async fn insert_package_repo_refs_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    merged: &[MinimalPackageRepoRef],
) -> Result<(Vec<PackageRepoReference>, Vec<PackageRepoRefVersion>), String> {
    for query in [
        CREATE_TEMP_INSERT_REFS_QUERY,
        CREATE_TEMP_INSERT_VERSIONS_QUERY,
    ] {
        sqlx::query(query)
            .execute(&mut **tx)
            .await
            .map_err(|err| format!("failed to create temporary tables: {err}"))?;
    }

    let schemes: Vec<String> = merged.iter().map(|dep| dep.scheme.clone()).collect();
    let names: Vec<String> = merged.iter().map(|dep| dep.name.clone()).collect();
    let blocked: Vec<bool> = merged.iter().map(|dep| dep.blocked).collect();
    let checked: Vec<Option<OffsetDateTime>> =
        merged.iter().map(|dep| dep.last_checked_at).collect();
    if !merged.is_empty() {
        sqlx::query(INSERT_TEMP_INSERT_REFS_QUERY)
            .bind(&schemes)
            .bind(&names)
            .bind(&blocked)
            .bind(&checked)
            .execute(&mut **tx)
            .await
            .map_err(|err| format!("failed to insert package repos in temporary table: {err}"))?;
    }

    let new_deps = sqlx::query(TRANSFER_INSERT_REFS_QUERY)
        .fetch_all(&mut **tx)
        .await
        .map_err(|err| format!("failed to transfer package repos from temporary table: {err}"))?
        .into_iter()
        .map(|row| {
            Ok(PackageRepoReference {
                id: row.try_get("id").map_err(|err| err.to_string())?,
                scheme: row.try_get("scheme").map_err(|err| err.to_string())?,
                name: row.try_get("name").map_err(|err| err.to_string())?,
                blocked: row.try_get("blocked").map_err(|err| err.to_string())?,
                versions: Vec::new(),
                last_checked_at: row
                    .try_get("last_checked_at")
                    .map_err(|err| err.to_string())?,
            })
        })
        .collect::<Result<Vec<_>, String>>()?;

    // Ids of every attempted `(scheme, name)` pair, in the same order as
    // `merged`. The baseline batches the lookup by
    // `batch.MaxNumPostgresParameters / 2` = 32767 entries; the chunk
    // boundaries are kept because the per-chunk `ORDER BY` is what aligns the
    // ids with `merged`.
    const MAX_BATCH_SIZE: usize = 65535 / 2;

    let mut all_ids: Vec<i64> = Vec::with_capacity(merged.len());
    for chunk in merged.chunks(MAX_BATCH_SIZE) {
        let schemes: Vec<String> = chunk.iter().map(|dep| dep.scheme.clone()).collect();
        let names: Vec<String> = chunk.iter().map(|dep| dep.name.clone()).collect();
        let rows = sqlx::query(GET_ATTEMPTED_INSERT_IDS_QUERY)
            .bind(&schemes)
            .bind(&names)
            .fetch_all(&mut **tx)
            .await
            // The baseline returns this error unwrapped.
            .map_err(|err| err.to_string())?;
        for row in rows {
            all_ids.push(row.try_get("id").map_err(|err| err.to_string())?);
        }
    }

    let mut package_ids: Vec<i64> = Vec::new();
    let mut version_strings: Vec<String> = Vec::new();
    let mut version_blocked: Vec<bool> = Vec::new();
    let mut version_checked: Vec<Option<OffsetDateTime>> = Vec::new();
    for (i, dep) in merged.iter().enumerate() {
        for version in &dep.versions {
            package_ids.push(all_ids[i]);
            version_strings.push(version.version.clone());
            version_blocked.push(version.blocked);
            version_checked.push(version.last_checked_at);
        }
    }
    if !version_strings.is_empty() {
        sqlx::query(INSERT_TEMP_INSERT_VERSIONS_QUERY)
            .bind(&package_ids)
            .bind(&version_strings)
            .bind(&version_blocked)
            .bind(&version_checked)
            .execute(&mut **tx)
            .await
            .map_err(|err| {
                format!("failed to insert package repo versions in temporary table: {err}")
            })?;
    }

    let new_versions = sqlx::query(TRANSFER_INSERT_VERSIONS_QUERY)
        .fetch_all(&mut **tx)
        .await
        .map_err(|err| format!("failed to transfer package repos from temporary table: {err}"))?
        .into_iter()
        .map(|row| {
            Ok(PackageRepoRefVersion {
                id: row.try_get("id").map_err(|err| err.to_string())?,
                package_ref_id: row.try_get("package_id").map_err(|err| err.to_string())?,
                version: row.try_get("version").map_err(|err| err.to_string())?,
                blocked: row.try_get("blocked").map_err(|err| err.to_string())?,
                last_checked_at: row
                    .try_get("last_checked_at")
                    .map_err(|err| err.to_string())?,
            })
        })
        .collect::<Result<Vec<_>, String>>()?;

    Ok((new_deps, new_versions))
}

/// `Store.UpdateAllBlockedStatuses`: insert the computed statuses into
/// temporary tables inside one transaction, then apply them and count the
/// rows whose `blocked` value actually changed.
pub async fn update_all_blocked_statuses(
    pool: &PgPool,
    pkgs: &[PackageRepoReference],
    start_time: OffsetDateTime,
) -> Result<(i64, i64), String> {
    let mut tx = pool.begin().await.map_err(|err| err.to_string())?;

    for query in [CREATE_TEMP_REFS_QUERY, CREATE_TEMP_VERSIONS_QUERY] {
        sqlx::query(query)
            .execute(&mut *tx)
            .await
            .map_err(|err| format!("failed to create temporary tables: {err}"))?;
    }

    let ref_ids: Vec<i64> = pkgs.iter().map(|pkg| pkg.id).collect();
    let ref_blocked: Vec<bool> = pkgs.iter().map(|pkg| pkg.blocked).collect();
    sqlx::query(INSERT_TEMP_REFS_QUERY)
        .bind(&ref_ids)
        .bind(&ref_blocked)
        .execute(&mut *tx)
        .await
        .map_err(|err| format!("error inserting into temporary package repos table: {err}"))?;

    let mut version_ids = Vec::new();
    let mut version_blocked = Vec::new();
    for pkg in pkgs {
        for version in &pkg.versions {
            version_ids.push(version.id);
            version_blocked.push(version.blocked);
        }
    }
    sqlx::query(INSERT_TEMP_VERSIONS_QUERY)
        .bind(&version_ids)
        .bind(&version_blocked)
        .execute(&mut *tx)
        .await
        .map_err(|err| {
            format!("error inserting into temporary package repo versions table: {err}")
        })?;

    let (pkgs_updated, versions_updated): (i64, i64) = sqlx::query_as(UPDATE_STATUSES_QUERY)
        .bind(start_time)
        .bind(start_time)
        .fetch_one(&mut *tx)
        .await
        .map_err(|err| format!("error scanning update results: {err}"))?;

    tx.commit().await.map_err(|err| err.to_string())?;
    Ok((pkgs_updated, versions_updated))
}

const SHOULD_REFILTER_QUERY: &str = r#"
WITH least_recently_checked AS (
    SELECT * FROM (
        (
            SELECT last_checked_at FROM lsif_dependency_repos
            ORDER BY last_checked_at ASC NULLS FIRST
            LIMIT 1
        )
        UNION ALL
        (
            SELECT last_checked_at FROM package_repo_versions
            ORDER BY last_checked_at ASC NULLS FIRST
            LIMIT 1
        )
    ) p
    ORDER BY last_checked_at ASC NULLS FIRST
    LIMIT 1
),
most_recently_updated_filter AS (
    SELECT COALESCE(deleted_at, updated_at)
    FROM package_repo_filters
    ORDER BY COALESCE(deleted_at, updated_at) DESC
    LIMIT 1
)
SELECT 1
WHERE
    (SELECT COUNT(*) FROM most_recently_updated_filter) <> 0 AND
    (SELECT COUNT(*) FROM least_recently_checked) <> 0 AND
    (
        (SELECT * FROM least_recently_checked) IS NULL OR
        (SELECT * FROM least_recently_checked) < (SELECT * FROM most_recently_updated_filter)
    )
"#;

const LIST_FILTERS_QUERY: &str = r#"
SELECT id, behaviour, scheme, matcher
FROM package_repo_filters
WHERE deleted_at IS NULL
ORDER BY id
"#;

const LIST_REFS_QUERY: &str = r#"
SELECT lr.id, lr.scheme, lr.name, lr.blocked, lr.last_checked_at,
       array_agg(prv.id ORDER BY prv.id) AS vid,
       array_agg(prv.version ORDER BY prv.id) AS version,
       array_agg(prv.blocked ORDER BY prv.id) AS vers_blocked,
       array_agg(prv.last_checked_at ORDER BY prv.id) AS vers_last_checked_at
FROM lsif_dependency_repos lr
JOIN LATERAL (
    SELECT id, package_id, version, blocked, last_checked_at
    FROM package_repo_versions
    WHERE package_id = lr.id
    ORDER BY id
) prv
ON lr.id = prv.package_id
WHERE TRUE
GROUP BY lr.id
ORDER BY lr.id ASC
LIMIT $1
"#;

const LIST_REFS_AFTER_QUERY: &str = r#"
SELECT lr.id, lr.scheme, lr.name, lr.blocked, lr.last_checked_at,
       array_agg(prv.id ORDER BY prv.id) AS vid,
       array_agg(prv.version ORDER BY prv.id) AS version,
       array_agg(prv.blocked ORDER BY prv.id) AS vers_blocked,
       array_agg(prv.last_checked_at ORDER BY prv.id) AS vers_last_checked_at
FROM lsif_dependency_repos lr
JOIN LATERAL (
    SELECT id, package_id, version, blocked, last_checked_at
    FROM package_repo_versions
    WHERE package_id = lr.id
    ORDER BY id
) prv
ON lr.id = prv.package_id
WHERE lr.id > $1
GROUP BY lr.id
ORDER BY lr.id ASC
LIMIT $2
"#;

const CREATE_TEMP_REFS_QUERY: &str = r#"
CREATE TEMPORARY TABLE t_lsif_dependency_repos (
	id BIGINT NOT NULL,
	blocked BOOLEAN NOT NULL
) ON COMMIT DROP
"#;

const CREATE_TEMP_VERSIONS_QUERY: &str = r#"
CREATE TEMPORARY TABLE t_package_repo_versions (
	id BIGINT NOT NULL,
	blocked BOOLEAN NOT NULL
) ON COMMIT DROP
"#;

const INSERT_TEMP_REFS_QUERY: &str = r#"
INSERT INTO t_lsif_dependency_repos (id, blocked)
SELECT * FROM UNNEST($1::bigint[], $2::boolean[])
"#;

const INSERT_TEMP_VERSIONS_QUERY: &str = r#"
INSERT INTO t_package_repo_versions (id, blocked)
SELECT * FROM UNNEST($1::bigint[], $2::boolean[])
"#;

const CREATE_TEMP_INSERT_REFS_QUERY: &str = r#"
CREATE TEMPORARY TABLE t_package_repo_refs (
	scheme TEXT NOT NULL,
	name TEXT NOT NULL,
	blocked BOOLEAN NOT NULL,
	last_checked_at TIMESTAMPTZ
) ON COMMIT DROP
"#;

const CREATE_TEMP_INSERT_VERSIONS_QUERY: &str = r#"
CREATE TEMPORARY TABLE t_package_repo_versions (
	package_id BIGINT NOT NULL,
	version TEXT NOT NULL,
	blocked BOOLEAN NOT NULL,
	last_checked_at TIMESTAMPTZ
) ON COMMIT DROP
"#;

const INSERT_TEMP_INSERT_REFS_QUERY: &str = r#"
INSERT INTO t_package_repo_refs (scheme, name, blocked, last_checked_at)
SELECT * FROM UNNEST($1::text[], $2::text[], $3::boolean[], $4::timestamptz[])
"#;

const TRANSFER_INSERT_REFS_QUERY: &str = r#"
INSERT INTO lsif_dependency_repos (scheme, name, blocked, last_checked_at)
SELECT scheme, name, blocked, last_checked_at
FROM t_package_repo_refs t
WHERE NOT EXISTS (
	SELECT scheme, name
	FROM lsif_dependency_repos
	WHERE scheme = t.scheme AND
	name = t.name
)
ORDER BY name
RETURNING id, scheme, name, blocked, last_checked_at
"#;

const GET_ATTEMPTED_INSERT_IDS_QUERY: &str = r#"
SELECT id FROM lsif_dependency_repos
WHERE (scheme, name) IN (SELECT * FROM UNNEST($1::text[], $2::text[]))
ORDER BY (scheme, name)
"#;

const INSERT_TEMP_INSERT_VERSIONS_QUERY: &str = r#"
INSERT INTO t_package_repo_versions (package_id, version, blocked, last_checked_at)
SELECT * FROM UNNEST($1::bigint[], $2::text[], $3::boolean[], $4::timestamptz[])
"#;

const TRANSFER_INSERT_VERSIONS_QUERY: &str = r#"
INSERT INTO package_repo_versions (package_id, version, blocked, last_checked_at)
-- we dont reduce package repo versions,
-- so DISTINCT here to avoid conflict
SELECT DISTINCT ON (package_id, version) package_id, version, blocked, last_checked_at
FROM t_package_repo_versions t
WHERE NOT EXISTS (
	SELECT package_id, version
	FROM package_repo_versions
	WHERE package_id = t.package_id AND
	version = t.version
)
-- unit tests rely on a certain order
ORDER BY package_id, version
RETURNING id, package_id, version, blocked, last_checked_at
"#;

const UPDATE_STATUSES_QUERY: &str = r#"
WITH updated_package_repos AS (
	UPDATE lsif_dependency_repos new
	SET
		blocked = temp.blocked,
		last_checked_at = $1
	FROM t_lsif_dependency_repos temp
	JOIN lsif_dependency_repos old
	ON temp.id = old.id
	WHERE old.id = new.id
	RETURNING old.blocked <> new.blocked AS changed
),
updated_package_repo_versions AS (
	UPDATE package_repo_versions new
	SET
		blocked = temp.blocked,
		last_checked_at = $2
	FROM t_package_repo_versions temp
	JOIN package_repo_versions old
	ON temp.id = old.id
	WHERE old.id = new.id
	RETURNING old.blocked <> new.blocked AS changed
)
SELECT (
	SELECT COUNT(*) FILTER (WHERE changed)
	FROM updated_package_repos
) AS packages_changed, (
	SELECT COUNT(*) FILTER (WHERE changed)
	FROM updated_package_repo_versions
) AS versions_changed
"#;
