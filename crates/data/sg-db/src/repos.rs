//! Read-only view of `repo` and its sources, as needed by gitserver's remote
//! URL lookup (`internal/database/repos.go`: `GetByName` + `getSourcesByRepoQueryStr`).
//!
//! `sources` is aggregated exactly like the baseline subquery: every live
//! external service linked through `external_service_repos`, with the service
//! kind lowercased. The `Kind`/`ID`/`CloneURL` key casing matches the JSON the
//! Go scanner decodes (`scanRepo`).

use serde::Deserialize;
use sqlx::postgres::PgPool;
use sqlx::types::time::OffsetDateTime;
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
    /// `repo.private`; permission syncing only fetches private repositories.
    pub private: bool,
    /// `repo.external_id`; `types.Repo.ExternalRepo.ID`.
    pub external_id: Option<String>,
    /// `repo.created_at`; used for the first-sync delay metric.
    pub created_at: OffsetDateTime,
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
	r.private,
	r.external_id,
	r.created_at,
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
            private: row.try_get("private")?,
            external_id: row.try_get("external_id")?,
            created_at: row.try_get("created_at")?,
            sources,
        }))
    }
}

impl RepoStore {
    /// `Repos().Get(id)`: `listRepos` with `IDs: [id]`, `Limit: 1`,
    /// `IncludeBlocked: true`. Soft-deleted repositories are excluded by the
    /// default conditions; blocked repositories are returned (the baseline
    /// returns them together with a `BlockedRepoError`, which the caller
    /// checks via [`Repo::blocked`]).
    pub async fn get_by_id(&self, id: i32) -> Result<Option<Repo>, sqlx::Error> {
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
	r.private,
	r.external_id,
	r.created_at,
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
WHERE r.id = $1 AND r.deleted_at IS NULL
LIMIT 1
"#,
        )
        .bind(id)
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
            private: row.try_get("private")?,
            external_id: row.try_get("external_id")?,
            created_at: row.try_get("created_at")?,
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

    /// `Repos.ListMinimalRepos(ctx, ReposListOptions{Names: names})` reduced to
    /// the names the dependency indexing scheduler reads back. The baseline
    /// compares `lower(name::text) COLLATE "C"` against the lowercased names,
    /// excludes soft-deleted and blocked repositories (the default list
    /// conditions) and orders by `name` (the handler passes
    /// `OrderBy: {Field: RepoListName}`); the returned `name` keeps the stored
    /// casing.
    pub async fn list_minimal_repos_by_names(
        &self,
        names: &[String],
    ) -> Result<Vec<String>, sqlx::Error> {
        if names.is_empty() {
            return Ok(Vec::new());
        }
        let lower_names: Vec<String> = names.iter().map(|name| name.to_lowercase()).collect();
        let rows = sqlx::query(
            r#"
SELECT name
FROM repo
WHERE deleted_at IS NULL AND blocked IS NULL
  AND lower(name::text) COLLATE "C" = ANY($1::text[])
ORDER BY name
"#,
        )
        .bind(&lower_names)
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter().map(|row| row.try_get("name")).collect()
    }

    /// `Repos.ListMinimalRepos(ctx, ReposListOptions{NoCloned: true})`: the
    /// repositories that gitserver has not finished cloning yet. The baseline
    /// joins `gitserver_repos` and keeps `clone_status IN ('not_cloned',
    /// 'cloning')`; soft-deleted and blocked repositories are excluded by the
    /// default list conditions, and the result is ordered by id.
    pub async fn list_uncloned(&self) -> Result<Vec<RepoName>, sqlx::Error> {
        let rows = sqlx::query(
            r#"
SELECT repo.id, repo.name
FROM repo
JOIN gitserver_repos gr ON gr.repo_id = repo.id
WHERE repo.deleted_at IS NULL AND repo.blocked IS NULL
  AND gr.clone_status IN ('not_cloned', 'cloning')
ORDER BY repo.id
"#,
        )
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

/// `api.ExternalRepoSpec`: the code-host identity of a repository.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct ExternalRepoSpec {
    pub id: String,
    pub service_type: String,
    pub service_id: String,
}

impl std::fmt::Display for ExternalRepoSpec {
    /// Matches Go's `%v` on `api.ExternalRepoSpec`.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{{{} {} {}}}",
            self.id, self.service_type, self.service_id
        )
    }
}

impl RepoStore {
    /// `ListMinimalRepos(ctx, ReposListOptions{ExternalRepos: specs,
    /// OnlyPrivate: true})`: exact matches of `(external_id,
    /// external_service_type, external_service_id)`, ordered by id.
    ///
    /// The syncer chunks `specs` into groups of 10000 to stay below the
    /// Postgres bind-parameter limit. Like the baseline, the same repository
    /// may appear more than once when a spec is repeated.
    pub async fn list_private_repos_by_external_specs(
        &self,
        specs: &[ExternalRepoSpec],
    ) -> Result<Vec<i32>, sqlx::Error> {
        if specs.is_empty() {
            return Ok(Vec::new());
        }
        let mut query = sqlx::QueryBuilder::new(
            "SELECT id FROM repo WHERE deleted_at IS NULL AND blocked IS NULL AND private AND (",
        );
        for (i, spec) in specs.iter().enumerate() {
            if i > 0 {
                query.push(" OR ");
            }
            query
                .push("(external_id = ")
                .push_bind(spec.id.clone())
                .push(" AND external_service_type = ")
                .push_bind(spec.service_type.clone())
                .push(" AND external_service_id = ")
                .push_bind(spec.service_id.clone())
                .push(")");
        }
        query.push(") ORDER BY id");
        let rows = query.build().fetch_all(&self.pool).await?;
        rows.iter().map(|row| row.try_get("id")).collect()
    }

    /// `ListMinimalRepos(ctx, ReposListOptions{ExternalRepoIncludeContains,
    /// ExternalRepoExcludeContains, OnlyPrivate: true})`: private repositories
    /// whose external id matches every exclusion and at least one inclusion
    /// (`SIMILAR TO`, i.e. a POSIX regex), ordered by id.
    ///
    /// The baseline only calls this when at least one inclusion exists;
    /// exclusions are relative to inclusions.
    pub async fn list_private_repos_by_external_contains(
        &self,
        includes: &[ExternalRepoSpec],
        excludes: &[ExternalRepoSpec],
    ) -> Result<Vec<i32>, sqlx::Error> {
        if includes.is_empty() {
            return Ok(Vec::new());
        }
        let mut query = sqlx::QueryBuilder::new(
            "SELECT id FROM repo WHERE deleted_at IS NULL AND blocked IS NULL AND private AND (",
        );
        for (i, spec) in includes.iter().enumerate() {
            if i > 0 {
                query.push(" OR ");
            }
            query
                .push("(external_id SIMILAR TO ")
                .push_bind(spec.id.clone())
                .push(" AND external_service_type = ")
                .push_bind(spec.service_type.clone())
                .push(" AND external_service_id = ")
                .push_bind(spec.service_id.clone())
                .push(")");
        }
        query.push(")");
        for spec in excludes {
            query
                .push(" AND (external_id NOT SIMILAR TO ")
                .push_bind(spec.id.clone())
                .push(" AND external_service_type = ")
                .push_bind(spec.service_type.clone())
                .push(" AND external_service_id = ")
                .push_bind(spec.service_id.clone())
                .push(")");
        }
        query.push(" ORDER BY id");
        let rows = query.build().fetch_all(&self.pool).await?;
        rows.iter().map(|row| row.try_get("id")).collect()
    }
}

/// One entry of `types.SearchedRepo`, the metadata used to decorate search
/// results (`internal/database/repos.go:339-405`, `Repos().Metadata`).
#[derive(Debug, Clone, PartialEq)]
pub struct SearchedRepo {
    pub id: i32,
    pub name: String,
    /// `repo.description` (NULL scans as the empty string, like the
    /// baseline's `dbutil.NullString`).
    pub description: String,
    pub fork: bool,
    pub archived: bool,
    pub private: bool,
    /// `repo.stars` (NULL scans as 0, like `dbutil.NullInt`).
    pub stars: i32,
    /// `gr.last_fetched` from `gitserver_repos`.
    pub last_fetched: Option<OffsetDateTime>,
    /// `(SELECT json_object_agg(key, value) FROM repo_kvps ...)`; `None`
    /// when the subquery returns NULL (or `{}`).
    pub key_value_pairs: Option<serde_json::Value>,
    /// `repo.topics`; `None` when the array is empty (`repoTopics.Scan`
    /// converts `{}` to a nil slice).
    pub topics: Option<Vec<String>>,
}

impl RepoStore {
    /// `Repos().Metadata(ids...)`: the limited column set the frontend
    /// search stream uses to decorate matches. Repositories missing from the
    /// result set (deleted or unknown IDs) are simply absent. Order follows
    /// `ORDER BY repo.id`; callers map by ID, so it does not matter.
    pub async fn metadata(&self, ids: &[i32]) -> Result<Vec<SearchedRepo>, sqlx::Error> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        let rows = sqlx::query(
            r#"
SELECT
	repo.id,
	repo.name,
	repo.description,
	repo.fork,
	repo.archived,
	repo.private,
	repo.stars,
	gr.last_fetched,
	(SELECT json_object_agg(key, value) FROM repo_kvps WHERE repo_kvps.repo_id = repo.id) AS key_value_pairs,
	repo.topics
FROM repo
LEFT JOIN gitserver_repos gr ON gr.repo_id = repo.id
WHERE repo.deleted_at IS NULL AND repo.id = ANY($1)
ORDER BY repo.id
"#,
        )
        .bind(ids)
        .fetch_all(&self.pool)
        .await?;

        rows.iter()
            .map(|row| {
                let topics: Option<Vec<String>> = row.try_get("topics")?;
                // The baseline scans `{}` into a nil slice; `json_object_agg`
                // over an empty table yields NULL.
                let key_value_pairs: Option<serde_json::Value> = row.try_get("key_value_pairs")?;
                Ok(SearchedRepo {
                    id: row.try_get("id")?,
                    name: row.try_get("name")?,
                    description: row.try_get::<Option<String>, _>("description")?.unwrap_or_default(),
                    fork: row.try_get("fork")?,
                    archived: row.try_get("archived")?,
                    private: row.try_get("private")?,
                    stars: row.try_get::<Option<i32>, _>("stars")?.unwrap_or(0),
                    last_fetched: row.try_get("last_fetched")?,
                    key_value_pairs: key_value_pairs.filter(|v| !is_empty_json_object(v)),
                    topics: topics.filter(|t| !t.is_empty()),
                })
            })
            .collect()
    }

    /// Best-effort ID → name lookup for the streaming progress "skipped"
    /// lists and `RepoNamer` (`Repos().StreamMinimalRepos` with `IDs` only,
    /// `ORDER BY id`). Unknown IDs are absent from the result, matching the
    /// baseline's best-effort callback (callers fall back to
    /// `UNKNOWN{ID=...}`).
    pub async fn minimal_repo_names_by_ids(
        &self,
        ids: &[i32],
    ) -> Result<Vec<(i32, String)>, sqlx::Error> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        let rows = sqlx::query(
            r#"
SELECT id, name
FROM repo
WHERE deleted_at IS NULL AND id = ANY($1)
ORDER BY id
"#,
        )
        .bind(ids)
        .fetch_all(&self.pool)
        .await?;
        rows.iter()
            .map(|row| Ok((row.try_get("id")?, row.try_get("name")?)))
            .collect()
    }
}

/// `json_object_agg` returns `{}` (not NULL) when the filtered set is empty
/// in some PostgreSQL versions; the baseline treats an empty map as nil
/// because `encoding/json` omits it either way.
fn is_empty_json_object(value: &serde_json::Value) -> bool {
    matches!(value, serde_json::Value::Object(map) if map.is_empty())
}

// ---------------------------------------------------------------------------
// `Repos().ListMinimalRepos` for the search repository resolver (M9.10a)
// ---------------------------------------------------------------------------

/// `database.RepoKVPFilter`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RepoKvpFilter {
    pub key: String,
    pub value: Option<String>,
    pub negated: bool,
    pub key_only: bool,
}

/// `database.RepoTopicFilter`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RepoTopicFilter {
    pub topic: String,
    pub negated: bool,
}

/// One `types.Cursor`. `value` is the string form the Go cursor carries;
/// `stars`/`id` values are parsed as integers when the condition is built.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RepoListCursor {
    pub column: String,
    pub value: String,
    pub direction: String,
}

/// `database.RepoListSort` (`field`, `descending`, `nulls`).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RepoListSort {
    pub field: String,
    pub descending: bool,
    pub nulls: Option<String>,
}

impl RepoListSort {
    pub fn stars_desc_nulls_last() -> Self {
        Self {
            field: "stars".to_owned(),
            descending: true,
            nulls: Some("LAST".to_owned()),
        }
    }

    pub fn id_desc() -> Self {
        Self {
            field: "id".to_owned(),
            descending: true,
            nulls: None,
        }
    }
}

/// Subset of `database.ReposListOptions` the resolver constructs
/// (`internal/search/repos/repos.go:250-285`).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RepoListOptions {
    pub include_patterns: Vec<String>,
    pub exclude_pattern: String,
    pub description_patterns: Vec<String>,
    pub case_sensitive_patterns: bool,
    pub kvp_filters: Vec<RepoKvpFilter>,
    pub topic_filters: Vec<RepoTopicFilter>,
    pub cursors: Vec<RepoListCursor>,
    pub limit: i64,
    pub no_forks: bool,
    pub only_forks: bool,
    pub no_archived: bool,
    pub only_archived: bool,
    pub no_private: bool,
    pub only_private: bool,
    pub only_cloned: bool,
    /// `0` means "no search-context join".
    pub search_context_id: i64,
    pub order_by: Vec<RepoListSort>,
}

/// `types.MinimalRepo` fields the resolver reads.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RepoListItem {
    pub id: i32,
    pub name: String,
    pub stars: i64,
    /// `types.MinimalRepo.ExternalRepo`.
    pub external_id: String,
    pub external_service_type: String,
    pub external_service_id: String,
}

/// One `database.RepoChangelistIDs`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RepoChangelistIDs {
    pub repo_id: i32,
    pub changelist_ids: Vec<i64>,
}

/// `AuthzQueryConds` for the fixed anonymous actor (`BypassAuthz=false`,
/// `AuthenticatedUserID=0`), identical to [`super::repos`]'s
/// `search_runtime::PgPrivateRepoLister` condition.
const AUTHZ_CONDITION: &str = r#"(
    EXISTS (SELECT 1 FROM user_repo_permissions WHERE repo_id = repo.id AND user_id IS NULL)
    OR NOT repo.private
    OR EXISTS (
        SELECT 1 FROM external_services es
        JOIN external_service_repos esr ON (
            esr.external_service_id = es.id AND esr.repo_id = repo.id
            AND es.unrestricted = TRUE AND es.deleted_at IS NULL
        )
    )
    OR EXISTS (
        SELECT 1 FROM user_repo_permissions WHERE repo_id = repo.id AND user_id = 0
    )
)"#;

/// `(*repoStore).ListMinimalRepos` for the repository resolver.
///
/// This is the `listSQL` condition subset M9.10a reaches. Include patterns are
/// always evaluated as regular expressions (the baseline's exact/LIKE
/// optimization is not ported; see the M9.10a overview §2.4).
pub async fn list_minimal_repos(
    pool: &PgPool,
    options: &RepoListOptions,
) -> Result<Vec<RepoListItem>, sqlx::Error> {
    let mut sql = String::from(
        "SELECT repo.id, repo.name, COALESCE(repo.stars, 0) AS stars, \
                COALESCE(repo.external_id, '') AS external_id, \
                COALESCE(repo.external_service_type, '') AS external_service_type, \
                COALESCE(repo.external_service_id, '') AS external_service_id \
         FROM repo",
    );
    let mut arguments: Vec<String> = Vec::new();

    if options.only_cloned {
        sql.push_str(" JOIN gitserver_repos gr ON gr.repo_id = repo.id");
    }
    if options.search_context_id != 0 {
        sql.push_str(
            " JOIN (SELECT DISTINCT repo_id, search_context_id FROM search_context_repos) dscr \
              ON repo.id = dscr.repo_id",
        );
    }

    sql.push_str(" WHERE repo.deleted_at IS NULL AND repo.blocked IS NULL");

    for pattern in &options.include_patterns {
        arguments.push(pattern.clone());
        if options.case_sensitive_patterns {
            sql.push_str(&format!(" AND repo.name::text ~ ${}", arguments.len()));
        } else {
            sql.push_str(&format!(" AND lower(repo.name) ~ lower(${})", arguments.len()));
        }
    }
    if !options.exclude_pattern.is_empty() {
        arguments.push(options.exclude_pattern.clone());
        if options.case_sensitive_patterns {
            sql.push_str(&format!(" AND repo.name !~* ${}", arguments.len()));
        } else {
            sql.push_str(&format!(" AND lower(repo.name) !~* ${}", arguments.len()));
        }
    }
    for pattern in &options.description_patterns {
        arguments.push(pattern.clone());
        sql.push_str(&format!(
            " AND lower(repo.description) ~* lower(${})",
            arguments.len()
        ));
    }
    for filter in &options.kvp_filters {
        let mut conds = Vec::new();
        arguments.push(filter.key.clone());
        conds.push(format!("key ~* ${}", arguments.len()));
        if filter.key_only {
            conds.push("value IS NULL".to_owned());
        } else if let Some(value) = &filter.value {
            arguments.push(value.clone());
            conds.push(format!("value ~* ${}", arguments.len()));
        } else {
            conds.push("value IS NULL".to_owned());
        }
        let cond = format!(
            "EXISTS (SELECT 1 FROM repo_kvps WHERE repo_id = repo.id AND {})",
            conds.join(" AND ")
        );
        if filter.negated {
            sql.push_str(&format!(" AND NOT ({cond})"));
        } else {
            sql.push_str(&format!(" AND ({cond})"));
        }
    }
    for filter in &options.topic_filters {
        arguments.push(filter.topic.clone());
        let cond = format!("repo.topics @> ARRAY[${}]::text[]", arguments.len());
        if filter.negated {
            sql.push_str(&format!(" AND NOT ({cond})"));
        } else {
            sql.push_str(&format!(" AND ({cond})"));
        }
    }
    if options.no_forks {
        sql.push_str(" AND NOT repo.fork");
    }
    if options.only_forks {
        sql.push_str(" AND repo.fork");
    }
    if options.no_archived {
        sql.push_str(" AND NOT repo.archived");
    }
    if options.only_archived {
        sql.push_str(" AND repo.archived");
    }
    if options.no_private {
        sql.push_str(" AND NOT repo.private");
    }
    if options.only_private {
        sql.push_str(" AND repo.private");
    }
    if options.only_cloned {
        sql.push_str(" AND gr.clone_status = 'cloned'");
    }
    if options.search_context_id != 0 {
        arguments.push(options.search_context_id.to_string());
        sql.push_str(&format!(" AND dscr.search_context_id = ${}", arguments.len()));
    }

    // `parseCursorConds`: same-direction cursors only; `next` is `>=` and
    // `prev` is `<=`. The resolver only emits `prev`.
    if !options.cursors.is_empty() {
        let mut direction = String::new();
        let mut columns = Vec::new();
        let mut values = Vec::new();
        for cursor in &options.cursors {
            if cursor.column.is_empty() || cursor.value.is_empty() {
                continue;
            }
            if direction.is_empty() {
                direction = cursor.direction.clone();
            } else if direction != cursor.direction {
                return Err(sqlx::Error::Protocol(
                    "multi-cursors must have the same direction".to_owned(),
                ));
            }
            match cursor.column.as_str() {
                "stars" | "id" => {
                    let value: i64 = cursor.value.parse().map_err(|_| {
                        sqlx::Error::Protocol(format!(
                            "missing or invalid cursor: {:?}",
                            cursor.value
                        ))
                    })?;
                    columns.push(format!("repo.{}", cursor.column));
                    arguments.push(value.to_string());
                    values.push(format!("${}::bigint", arguments.len()));
                }
                "name" => {
                    columns.push("repo.name".to_owned());
                    arguments.push(cursor.value.clone());
                    values.push(format!("${}::citext", arguments.len()));
                }
                "created_at" => {
                    columns.push("repo.created_at".to_owned());
                    arguments.push(cursor.value.clone());
                    values.push(format!("${}::timestamptz", arguments.len()));
                }
                other => {
                    return Err(sqlx::Error::Protocol(format!(
                        "missing or invalid cursor: {other:?}"
                    )))
                }
            }
        }
        let operator = match direction.as_str() {
            "next" => ">=",
            "prev" => "<=",
            other => {
                return Err(sqlx::Error::Protocol(format!(
                    "missing or invalid cursor direction: {other:?}"
                )))
            }
        };
        if !columns.is_empty() {
            sql.push_str(&format!(
                " AND ({}) {} ({})",
                columns.join(", "),
                operator,
                values.join(", ")
            ));
        }
    }

    sql.push_str(" AND ");
    sql.push_str(AUTHZ_CONDITION);

    if options.order_by.is_empty() {
        sql.push_str(" ORDER BY repo.id");
    } else {
        let mut parts = Vec::new();
        for sort in &options.order_by {
            let column = match sort.field.as_str() {
                "stars" | "id" | "name" | "created_at" | "size" => {
                    format!("repo.{}", sort.field)
                }
                other => {
                    return Err(sqlx::Error::Protocol(format!(
                        "missing or invalid repo sort field: {other:?}"
                    )))
                }
            };
            let mut part = column;
            if sort.descending {
                part.push_str(" DESC");
            }
            if let Some(nulls) = &sort.nulls {
                part.push_str(" NULLS ");
                part.push_str(nulls);
            }
            parts.push(part);
        }
        sql.push_str(" ORDER BY ");
        sql.push_str(&parts.join(", "));
    }
    if options.limit > 0 {
        sql.push_str(&format!(" LIMIT {}", options.limit));
    }

    let mut query = sqlx::query(&sql);
    for argument in &arguments {
        query = query.bind(argument);
    }
    let rows = query.fetch_all(pool).await?;
    let mut repos = Vec::with_capacity(rows.len());
    for row in rows {
        repos.push(RepoListItem {
            id: row.try_get("id")?,
            name: row.try_get("name")?,
            stars: i64::from(row.try_get::<i32, _>("stars")?),
            external_id: row.try_get("external_id")?,
            external_service_type: row.try_get("external_service_type")?,
            external_service_id: row.try_get("external_service_id")?,
        });
    }
    Ok(repos)
}

/// `Repos().ListMinimalRepos(ctx, ReposListOptions{IDs: ids})`: the
/// exhaustive searcher's `minimalRepo` lookup (`searcher.go:196-207`). The
/// default list conditions (`deleted_at IS NULL`, `blocked IS NULL`) and
/// `ORDER BY repo.id` match `list_minimal_repos`; only the `IDs` filter is
/// added (no schema change).
pub async fn minimal_repos_by_ids(
    pool: &PgPool,
    ids: &[i32],
) -> Result<Vec<RepoListItem>, sqlx::Error> {
    if ids.is_empty() {
        return Ok(Vec::new());
    }
    let rows = sqlx::query(
        r#"
SELECT repo.id, repo.name, COALESCE(repo.stars, 0) AS stars,
       COALESCE(repo.external_id, '') AS external_id,
       COALESCE(repo.external_service_type, '') AS external_service_type,
       COALESCE(repo.external_service_id, '') AS external_service_id
FROM repo
WHERE repo.deleted_at IS NULL AND repo.blocked IS NULL AND repo.id = ANY($1)
ORDER BY repo.id
"#,
    )
    .bind(ids)
    .fetch_all(pool)
    .await?;
    rows.iter()
        .map(|row| {
            Ok(RepoListItem {
                id: row.try_get("id")?,
                name: row.try_get("name")?,
                stars: i64::from(row.try_get::<i32, _>("stars")?),
                external_id: row.try_get("external_id")?,
                external_service_type: row.try_get("external_service_type")?,
                external_service_id: row.try_get("external_service_id")?,
            })
        })
        .collect()
}

/// `BatchGetRepoCommitChangelist` (`internal/database/repo_commits_changelists.go`).
///
/// Returns `repo_id -> changelist_id -> commit_sha`.
pub async fn batch_get_repo_commit_changelist(
    pool: &PgPool,
    repos: &[RepoChangelistIDs],
) -> Result<std::collections::BTreeMap<i32, std::collections::BTreeMap<i64, String>>, sqlx::Error>
{
    use std::collections::BTreeMap;
    let mut result: BTreeMap<i32, BTreeMap<i64, String>> = BTreeMap::new();
    if repos.is_empty() {
        return Ok(result);
    }
    let repo_ids: Vec<i32> = repos.iter().map(|repo| repo.repo_id).collect();
    let changelist_ids: Vec<i64> = repos
        .iter()
        .flat_map(|repo| repo.changelist_ids.iter().copied())
        .collect();
    let rows = sqlx::query(
        r#"
SELECT repo_id, perforce_changelist_id, commit_sha
FROM repo_commits_changelists
WHERE repo_id = ANY($1) AND perforce_changelist_id = ANY($2)
"#,
    )
    .bind(&repo_ids)
    .bind(&changelist_ids)
    .fetch_all(pool)
    .await?;
    for row in rows {
        let repo_id: i32 = row.try_get("repo_id")?;
        let changelist_id: i64 = row.try_get("perforce_changelist_id")?;
        let commit_sha: String = row.try_get("commit_sha")?;
        result
            .entry(repo_id)
            .or_default()
            .insert(changelist_id, commit_sha);
    }
    Ok(result)
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
