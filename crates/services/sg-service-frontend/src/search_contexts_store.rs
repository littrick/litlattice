#![forbid(unsafe_code)]

//! Production Postgres adapter for the search contexts database seams
//! (M9.8ii).
//!
//! [`PgSearchContextsStore`] implements both
//! [`crate::search_contexts::SearchContextsDatabase`] (M9.8hg) and
//! [`crate::search_contexts_graphql::SearchContextsGraphqlDatabase`] (M9.8hq)
//! by porting the SQL in `internal/database/search_contexts.go`
//! (`searchContextQueryFmtStr`, `listSearchContextsFmtStr`,
//! `countSearchContextsFmtStr`, the CRUD statements and the
//! `search_context_repos` joins).
//!
//! The M9.8hg trait methods are synchronous (the Go store is synchronous), so
//! this adapter bridges the async `sqlx` calls with
//! `tokio::task::block_in_place` (the frontend runs on the multi-thread
//! runtime). GraphQL trait methods that receive a
//! [`crate::search_contexts::RequestContext`] apply the baseline permission
//! predicate; the context-less `get_search_context` lookup used for
//! create/update validation runs with the internal bypass, matching the write
//! access checks that already happen in `search_contexts.rs`.

use std::collections::BTreeMap;
use std::future::Future;

use sqlx::postgres::PgRow;
use sqlx::{PgPool, Row};
use time::OffsetDateTime;

use crate::search_contexts::{
    Error, GetSearchContextOptions, MinimalRepo, Namespace, OrgMembership, ReposListOptions,
    RequestContext, SearchContext, SearchContextRepositoryRevisions, SearchContextsDatabase, User,
};
use crate::search_contexts_graphql::{
    ListSearchContextsOptions, ListSearchContextsPageOptions, MinimalRepoRef, NamespaceOrg,
    NamespaceUser, SearchContextRecord, SearchContextsGraphqlDatabase, SearchContextsOrderBy,
};

/// Bridge a future to the synchronous DB trait. Production executes the
/// GraphQL resolvers on the multi-thread Tokio runtime, where `block_in_place`
/// moves other tasks to a sibling worker while this thread drives the query.
fn block_on<F: Future>(future: F) -> F::Output {
    match tokio::runtime::Handle::try_current() {
        Ok(handle) => tokio::task::block_in_place(|| handle.block_on(future)),
        Err(_) => tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("search contexts blocking runtime")
            .block_on(future),
    }
}

pub struct PgSearchContextsStore {
    pool: PgPool,
}

impl PgSearchContextsStore {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    fn sql_error(error: sqlx::Error) -> Error {
        Error::other(error.to_string())
    }

    /// `searchContextQueryFmtStr`: the global context union plus the
    /// users/orgs/stars/default joins for database rows.
    fn search_context_query(&self, uid: i32) -> String {
        format!(
            r#"
SELECT
    0::bigint AS id,
    'global'::text AS context_name,
    'All repositories on Sourcegraph'::text AS description,
    true AS public,
    true AS autodefined,
    NULL::integer AS namespace_user_id,
    NULL::integer AS namespace_org_id,
    TIMESTAMP WITH TIME ZONE 'epoch' AS updated_at,
    NULL::text AS query,
    NULL::text AS namespace_name,
    NULL::text AS namespace_username,
    NULL::text AS namespace_org_name,
    (NOT EXISTS (SELECT FROM search_context_default scd WHERE scd.user_id = {uid})) AS user_default,
    false AS user_starred
UNION ALL
SELECT
    sc.id AS id,
    sc.name AS context_name,
    sc.description AS description,
    sc.public AS public,
    false AS autodefined,
    sc.namespace_user_id AS namespace_user_id,
    sc.namespace_org_id AS namespace_org_id,
    sc.updated_at AS updated_at,
    sc.query AS query,
    COALESCE(u.username, o.name) AS namespace_name,
    u.username AS namespace_username,
    o.name AS namespace_org_name,
    (scd.search_context_id IS NOT NULL) AS user_default,
    (scs.search_context_id IS NOT NULL) AS user_starred
FROM search_contexts sc
LEFT JOIN users u ON sc.namespace_user_id = u.id
LEFT JOIN orgs o ON sc.namespace_org_id = o.id
LEFT JOIN search_context_stars scs
    ON scs.user_id = {uid} AND scs.search_context_id = sc.id
LEFT JOIN search_context_default scd
    ON scd.user_id = {uid} AND scd.search_context_id = sc.id
"#
        )
    }

    /// `searchContextsPermissionsCondition`.
    fn permissions_condition(ctx: &RequestContext) -> String {
        let uid = ctx.actor_uid;
        format!(
            r#"(
    {internal}
    OR public
    OR (namespace_user_id IS NOT NULL AND namespace_user_id = {uid})
    OR (namespace_org_id IS NOT NULL AND EXISTS (SELECT FROM org_members om WHERE om.org_id = namespace_org_id AND om.user_id = {uid}))
    OR (namespace_user_id IS NULL AND namespace_org_id IS NULL AND EXISTS (SELECT FROM users u WHERE u.id = {uid} AND u.site_admin))
)"#,
            internal = ctx.internal,
            uid = uid,
        )
    }

    fn order_by_clause(order_by: SearchContextsOrderBy, descending: bool) -> &'static str {
        match (order_by, descending) {
            (SearchContextsOrderBy::Spec, false) => "namespace_name ASC, context_name ASC",
            (SearchContextsOrderBy::Spec, true) => "namespace_name DESC, context_name DESC",
            (SearchContextsOrderBy::UpdatedAt, false) => "updated_at ASC",
            (SearchContextsOrderBy::UpdatedAt, true) => "updated_at DESC",
        }
    }

    /// `getSearchContextsQueryConditions`. Returns the SQL fragment and the
    /// string bind values (the integer conditions are inlined like the
    /// baseline's `sqlf` rendering).
    fn query_conditions(opts: &ListSearchContextsOptions) -> (String, Vec<String>) {
        let mut namespace_conds: Vec<String> = Vec::new();
        if opts.no_namespace {
            namespace_conds
                .push("(namespace_user_id IS NULL AND namespace_org_id IS NULL)".to_owned());
        }
        if !opts.namespace_user_ids.is_empty() {
            namespace_conds.push(format!(
                "namespace_user_id IN ({})",
                join_ints(&opts.namespace_user_ids)
            ));
        }
        if !opts.namespace_org_ids.is_empty() {
            namespace_conds.push(format!(
                "namespace_org_id IN ({})",
                join_ints(&opts.namespace_org_ids)
            ));
        }

        let mut conds: Vec<String> = Vec::new();
        if !namespace_conds.is_empty() {
            conds.push(format!("({})", namespace_conds.join(" OR ")));
        }
        let mut binds = Vec::new();
        if !opts.name.is_empty() {
            binds.push(format!("%{}%", opts.name));
            conds.push(format!("context_name LIKE ${}", binds.len()));
        }
        if !opts.namespace_name.is_empty() {
            binds.push(format!("%{}%", opts.namespace_name));
            conds.push(format!(
                "COALESCE(namespace_username, namespace_org_name, '') ILIKE ${}",
                binds.len()
            ));
        }
        if conds.is_empty() {
            conds.push("1 = 1".to_owned());
        }
        (conds.join("\n AND "), binds)
    }

    fn fetch_records(
        &self,
        sql: &str,
        binds: &[String],
    ) -> Result<Vec<SearchContextRecord>, Error> {
        let mut query = sqlx::query(sql);
        for value in binds {
            query = query.bind(value.as_str());
        }
        let rows = block_on(query.fetch_all(&self.pool)).map_err(Self::sql_error)?;
        rows.iter()
            .map(record_from_row)
            .collect::<Result<_, _>>()
            .map_err(Self::sql_error)
    }
}

fn join_ints(values: &[i32]) -> String {
    values
        .iter()
        .map(i32::to_string)
        .collect::<Vec<_>>()
        .join(",")
}

fn record_from_row(row: &PgRow) -> Result<SearchContextRecord, sqlx::Error> {
    let namespace_user_id: Option<i32> = row.try_get("namespace_user_id")?;
    let namespace_org_id: Option<i32> = row.try_get("namespace_org_id")?;
    let query: Option<String> = row.try_get("query")?;
    let namespace_username: Option<String> = row.try_get("namespace_username")?;
    let namespace_org_name: Option<String> = row.try_get("namespace_org_name")?;
    Ok(SearchContextRecord {
        id: row.try_get("id")?,
        name: row.try_get("context_name")?,
        description: row.try_get("description")?,
        public: row.try_get("public")?,
        namespace_user_id: namespace_user_id.unwrap_or(0),
        namespace_org_id: namespace_org_id.unwrap_or(0),
        namespace_user_name: namespace_username.unwrap_or_default(),
        namespace_org_name: namespace_org_name.unwrap_or_default(),
        query: query.unwrap_or_default(),
        auto_defined: row.try_get("autodefined")?,
        updated_at: row.try_get("updated_at")?,
        default: row.try_get("user_default")?,
        starred: row.try_get("user_starred")?,
    })
}

impl PgSearchContextsStore {
    /// The list/count query body shared by the GraphQL trait methods.
    fn list_sql(
        &self,
        ctx: &RequestContext,
        conditions: &str,
        order_by: &str,
        limit: i32,
        offset: i32,
    ) -> String {
        format!(
            r#"SELECT
    id, context_name, description, public, autodefined,
    namespace_user_id, namespace_org_id, updated_at, query,
    namespace_username, namespace_org_name, user_default, user_starred
FROM (
{}
) AS t
WHERE
    {}
    AND ({})
ORDER BY
    autodefined DESC,
    user_default DESC,
    user_starred DESC,
    {}
LIMIT {}
OFFSET {}"#,
            self.search_context_query(ctx.actor_uid),
            Self::permissions_condition(ctx),
            conditions,
            order_by,
            limit,
            offset,
        )
    }
}

impl SearchContextsDatabase for PgSearchContextsStore {
    fn get_namespace_by_name(&self, name: &str) -> Result<Namespace, Error> {
        let row = block_on(
            sqlx::query(
                "SELECT COALESCE(user_id, 0) AS user_id, COALESCE(org_id, 0) AS org_id \
                 FROM names WHERE name = $1",
            )
            .bind(name)
            .fetch_optional(&self.pool),
        )
        .map_err(Self::sql_error)?;
        match row {
            Some(row) => Ok(Namespace {
                user: row.try_get("user_id").map_err(Self::sql_error)?,
                organization: row.try_get("org_id").map_err(Self::sql_error)?,
            }),
            None => Err(Error::namespace_not_found()),
        }
    }

    fn get_org_member_by_org_id_and_user_id(
        &self,
        org_id: i32,
        user_id: i32,
    ) -> Result<Option<OrgMembership>, Error> {
        let row = block_on(
            sqlx::query(
                "SELECT org_id, user_id FROM org_members WHERE org_id = $1 AND user_id = $2",
            )
            .bind(org_id)
            .bind(user_id)
            .fetch_optional(&self.pool),
        )
        .map_err(Self::sql_error)?;
        match row {
            Some(row) => Ok(Some(OrgMembership {
                org_id: row.try_get("org_id").map_err(Self::sql_error)?,
                user_id: row.try_get("user_id").map_err(Self::sql_error)?,
            })),
            None => Ok(None),
        }
    }

    fn get_current_user(&self, uid: i32) -> Result<Option<User>, Error> {
        if uid == 0 {
            return Ok(None);
        }
        let row = block_on(
            sqlx::query("SELECT id, site_admin FROM users WHERE id = $1")
                .bind(uid)
                .fetch_optional(&self.pool),
        )
        .map_err(Self::sql_error)?;
        row.map(|row| {
            Ok(User {
                id: row.try_get("id").map_err(Self::sql_error)?,
                site_admin: row.try_get("site_admin").map_err(Self::sql_error)?,
            })
        })
        .transpose()
    }

    fn get_search_context(&self, opts: &GetSearchContextOptions) -> Result<SearchContext, Error> {
        let uid = 0;
        let mut conds = Vec::new();
        if opts.namespace_user_id == 0 && opts.namespace_org_id == 0 {
            conds.push("namespace_user_id IS NULL".to_owned());
            conds.push("namespace_org_id IS NULL".to_owned());
        } else if opts.namespace_user_id != 0 {
            conds.push(format!("namespace_user_id = {}", opts.namespace_user_id));
        } else {
            conds.push(format!("namespace_org_id = {}", opts.namespace_org_id));
        }
        let sql = format!(
            r#"SELECT
    id, context_name, description, public, autodefined,
    namespace_user_id, namespace_org_id, updated_at, query,
    namespace_username, namespace_org_name, user_default, user_starred
FROM (
{}
) AS t
WHERE {} AND context_name = $1
ORDER BY id ASC
LIMIT 1"#,
            self.search_context_query(uid),
            conds.join(" AND "),
        );
        let rows = self.fetch_records(&sql, std::slice::from_ref(&opts.name))?;
        rows.into_iter()
            .next()
            .map(record_to_search_context)
            .ok_or_else(Error::search_context_not_found)
    }

    fn create_search_context_with_repository_revisions(
        &self,
        search_context: &SearchContext,
        repository_revisions: &[SearchContextRepositoryRevisions],
    ) -> Result<SearchContext, Error> {
        block_on(
            sqlx::query(
                "INSERT INTO search_contexts \
                 (name, description, public, namespace_user_id, namespace_org_id, query) \
                 VALUES ($1, $2, $3, $4, $5, $6)",
            )
            .bind(&search_context.name)
            .bind(&search_context.description)
            .bind(search_context.public)
            .bind(optional_id(search_context.namespace_user_id))
            .bind(optional_id(search_context.namespace_org_id))
            .bind(optional_string(&search_context.query))
            .execute(&self.pool),
        )
        .map_err(Self::sql_error)?;
        let created = self.get_search_context(&GetSearchContextOptions {
            name: search_context.name.clone(),
            namespace_user_id: search_context.namespace_user_id,
            namespace_org_id: search_context.namespace_org_id,
        })?;
        self.set_repository_revisions(created.id, repository_revisions)?;
        Ok(created)
    }

    fn update_search_context_with_repository_revisions(
        &self,
        search_context: &SearchContext,
        repository_revisions: &[SearchContextRepositoryRevisions],
    ) -> Result<SearchContext, Error> {
        block_on(
            sqlx::query(
                "UPDATE search_contexts SET name = $1, description = $2, public = $3, \
                 query = $4, updated_at = now() WHERE id = $5",
            )
            .bind(&search_context.name)
            .bind(&search_context.description)
            .bind(search_context.public)
            .bind(optional_string(&search_context.query))
            .bind(search_context.id)
            .execute(&self.pool),
        )
        .map_err(Self::sql_error)?;
        self.set_repository_revisions(search_context.id, repository_revisions)?;
        self.get_search_context(&GetSearchContextOptions {
            name: search_context.name.clone(),
            namespace_user_id: search_context.namespace_user_id,
            namespace_org_id: search_context.namespace_org_id,
        })
    }

    fn delete_search_context(&self, search_context_id: i64) -> Result<(), Error> {
        block_on(
            sqlx::query("DELETE FROM search_contexts WHERE id = $1")
                .bind(search_context_id)
                .execute(&self.pool),
        )
        .map_err(Self::sql_error)?;
        Ok(())
    }

    fn get_all_revisions_for_repos(
        &self,
        repo_ids: &[i32],
    ) -> Result<BTreeMap<i32, Vec<String>>, Error> {
        if repo_ids.is_empty() {
            return Ok(BTreeMap::new());
        }
        let rows = block_on(
            sqlx::query(
                "SELECT DISTINCT scr.repo_id, scr.revision FROM search_context_repos scr \
                 WHERE scr.repo_id = ANY($1) ORDER BY scr.revision",
            )
            .bind(repo_ids)
            .fetch_all(&self.pool),
        )
        .map_err(Self::sql_error)?;
        let mut revisions: BTreeMap<i32, Vec<String>> = BTreeMap::new();
        for row in rows {
            revisions
                .entry(row.try_get("repo_id").map_err(Self::sql_error)?)
                .or_default()
                .push(row.try_get("revision").map_err(Self::sql_error)?);
        }
        Ok(revisions)
    }

    fn get_all_queries(&self) -> Result<Vec<String>, Error> {
        let queries: Option<Vec<String>> = block_on(
            sqlx::query_scalar(
                "SELECT array_agg(query) FROM search_contexts WHERE query IS NOT NULL",
            )
            .fetch_one(&self.pool),
        )
        .map_err(Self::sql_error)?;
        Ok(queries.unwrap_or_default())
    }

    fn get_search_context_repository_revisions(
        &self,
        search_context_id: i64,
    ) -> Result<Vec<SearchContextRepositoryRevisions>, Error> {
        let rows = block_on(
            sqlx::query(
                "SELECT sc.repo_id, sc.revision, r.name FROM search_context_repos sc \
                 JOIN repo r ON r.id = sc.repo_id \
                 WHERE r.deleted_at IS NULL AND r.blocked IS NULL AND sc.search_context_id = $1",
            )
            .bind(search_context_id)
            .fetch_all(&self.pool),
        )
        .map_err(Self::sql_error)?;
        let mut by_repo: BTreeMap<i32, (String, Vec<String>)> = BTreeMap::new();
        for row in rows {
            let repo_id: i32 = row.try_get("repo_id").map_err(Self::sql_error)?;
            let name: String = row.try_get("name").map_err(Self::sql_error)?;
            let revision: String = row.try_get("revision").map_err(Self::sql_error)?;
            by_repo
                .entry(repo_id)
                .or_insert_with(|| (name, Vec::new()))
                .1
                .push(revision);
        }
        Ok(by_repo
            .into_iter()
            .map(|(id, (name, mut revisions))| {
                revisions.sort();
                SearchContextRepositoryRevisions {
                    repo: MinimalRepo { id, name },
                    revisions,
                }
            })
            .collect())
    }

    fn list_minimal_repos(&self, opts: &ReposListOptions) -> Result<Vec<MinimalRepo>, Error> {
        let mut conditions = vec![
            "deleted_at IS NULL".to_owned(),
            "blocked IS NULL".to_owned(),
        ];
        let mut binds: Vec<String> = Vec::new();
        for pattern in &opts.include_patterns {
            binds.push(pattern.clone());
            let operator = if opts.case_sensitive_patterns {
                "~"
            } else {
                "~*"
            };
            conditions.push(format!("name {} ${}", operator, binds.len()));
        }
        if !opts.exclude_pattern.is_empty() {
            binds.push(opts.exclude_pattern.clone());
            let operator = if opts.case_sensitive_patterns {
                "~"
            } else {
                "~*"
            };
            conditions.push(format!("NOT (name {} ${})", operator, binds.len()));
        }
        if opts.only_forks {
            conditions.push("fork".to_owned());
        }
        if opts.no_forks {
            conditions.push("NOT fork".to_owned());
        }
        if opts.only_archived {
            conditions.push("archived".to_owned());
        }
        if opts.no_archived {
            conditions.push("NOT archived".to_owned());
        }
        if opts.no_private {
            conditions.push("NOT private".to_owned());
        }
        if opts.only_private {
            conditions.push("private".to_owned());
        }
        let ids_condition = if opts.ids.is_empty() {
            String::new()
        } else {
            format!(" AND id IN ({})", join_ints(&opts.ids))
        };
        let sql = format!(
            "SELECT id, name FROM repo WHERE {}{}",
            conditions.join(" AND "),
            ids_condition
        );
        let mut query = sqlx::query(&sql);
        for value in &binds {
            query = query.bind(value.as_str());
        }
        let rows = block_on(query.fetch_all(&self.pool)).map_err(Self::sql_error)?;
        rows.iter()
            .map(|row| {
                Ok(MinimalRepo {
                    id: row.try_get("id").map_err(Self::sql_error)?,
                    name: row.try_get("name").map_err(Self::sql_error)?,
                })
            })
            .collect()
    }

    fn create_search_context_star_for_user(
        &self,
        user_id: i32,
        search_context_id: i64,
    ) -> Result<(), Error> {
        block_on(
            sqlx::query(
                "INSERT INTO search_context_stars (user_id, search_context_id) \
                 VALUES ($1, $2) ON CONFLICT DO NOTHING",
            )
            .bind(user_id)
            .bind(search_context_id)
            .execute(&self.pool),
        )
        .map_err(Self::sql_error)?;
        Ok(())
    }

    fn delete_search_context_star_for_user(
        &self,
        user_id: i32,
        search_context_id: i64,
    ) -> Result<(), Error> {
        block_on(
            sqlx::query(
                "DELETE FROM search_context_stars WHERE user_id = $1 AND search_context_id = $2",
            )
            .bind(user_id)
            .bind(search_context_id)
            .execute(&self.pool),
        )
        .map_err(Self::sql_error)?;
        Ok(())
    }

    fn set_user_default_search_context_id(
        &self,
        user_id: i32,
        search_context_id: i64,
    ) -> Result<(), Error> {
        let sql = if search_context_id == 0 {
            "DELETE FROM search_context_default WHERE user_id = $1"
        } else {
            "INSERT INTO search_context_default (user_id, search_context_id) \
             VALUES ($1, $2) ON CONFLICT (user_id) DO UPDATE \
             SET search_context_id = EXCLUDED.search_context_id"
        };
        let mut query = sqlx::query(sql).bind(user_id);
        if search_context_id != 0 {
            query = query.bind(search_context_id);
        }
        block_on(query.execute(&self.pool)).map_err(Self::sql_error)?;
        Ok(())
    }
}

impl PgSearchContextsStore {
    fn set_repository_revisions(
        &self,
        search_context_id: i64,
        repository_revisions: &[SearchContextRepositoryRevisions],
    ) -> Result<(), Error> {
        block_on(
            sqlx::query("DELETE FROM search_context_repos WHERE search_context_id = $1")
                .bind(search_context_id)
                .execute(&self.pool),
        )
        .map_err(Self::sql_error)?;
        for repo_revisions in repository_revisions {
            for revision in &repo_revisions.revisions {
                block_on(
                    sqlx::query(
                        "INSERT INTO search_context_repos (search_context_id, repo_id, revision) \
                         VALUES ($1, $2, $3)",
                    )
                    .bind(search_context_id)
                    .bind(repo_revisions.repo.id)
                    .bind(revision)
                    .execute(&self.pool),
                )
                .map_err(Self::sql_error)?;
            }
        }
        Ok(())
    }
}

impl SearchContextsGraphqlDatabase for PgSearchContextsStore {
    fn as_search_contexts_database(&self) -> &dyn SearchContextsDatabase {
        self
    }

    fn get_search_context_by_id(
        &self,
        ctx: &RequestContext,
        id: i64,
    ) -> Result<SearchContextRecord, Error> {
        let sql = self.list_sql(ctx, &format!("id = {}", id), "id ASC", 1, 0);
        self.fetch_records(&sql, &[])?
            .into_iter()
            .next()
            .ok_or_else(Error::search_context_not_found)
    }

    fn list_search_contexts(
        &self,
        ctx: &RequestContext,
        page: &ListSearchContextsPageOptions,
        opts: &ListSearchContextsOptions,
    ) -> Result<Vec<SearchContextRecord>, Error> {
        let (conditions, binds) = Self::query_conditions(opts);
        let sql = self.list_sql(
            ctx,
            &conditions,
            Self::order_by_clause(opts.order_by, opts.order_by_descending),
            page.first,
            page.after,
        );
        self.fetch_records(&sql, &binds)
    }

    fn count_search_contexts(
        &self,
        ctx: &RequestContext,
        opts: &ListSearchContextsOptions,
    ) -> Result<i32, Error> {
        let (conditions, binds) = Self::query_conditions(opts);
        let sql = format!(
            "SELECT COUNT(*) FROM ({} ) AS t WHERE {} AND ({})",
            self.search_context_query(ctx.actor_uid),
            Self::permissions_condition(ctx),
            conditions,
        );
        let mut query = sqlx::query_scalar::<_, i64>(&sql);
        for value in &binds {
            query = query.bind(value.as_str());
        }
        let count = block_on(query.fetch_one(&self.pool)).map_err(Self::sql_error)?;
        Ok(i32::try_from(count).unwrap_or(i32::MAX))
    }

    fn get_default_search_context_for_current_user(
        &self,
        ctx: &RequestContext,
    ) -> Result<SearchContextRecord, Error> {
        let sql = self.list_sql(ctx, "user_default = true", "id ASC", 1, 0);
        self.fetch_records(&sql, &[])?
            .into_iter()
            .next()
            .ok_or_else(Error::search_context_not_found)
    }

    fn get_repos_set_by_ids(&self, ids: &[i32]) -> Result<BTreeMap<i32, MinimalRepoRef>, Error> {
        if ids.is_empty() {
            return Ok(BTreeMap::new());
        }
        let rows = block_on(
            sqlx::query("SELECT id, name FROM repo WHERE id = ANY($1)")
                .bind(ids)
                .fetch_all(&self.pool),
        )
        .map_err(Self::sql_error)?;
        let mut repos = BTreeMap::new();
        for row in rows {
            let id: i32 = row.try_get("id").map_err(Self::sql_error)?;
            let name: String = row.try_get("name").map_err(Self::sql_error)?;
            repos.insert(id, MinimalRepoRef { id, name });
        }
        Ok(repos)
    }

    fn get_user_by_id(&self, id: i32) -> Result<NamespaceUser, Error> {
        let row = block_on(
            sqlx::query("SELECT id, username FROM users WHERE id = $1")
                .bind(id)
                .fetch_optional(&self.pool),
        )
        .map_err(Self::sql_error)?
        .ok_or_else(|| Error::other("user not found"))?;
        Ok(NamespaceUser {
            id: row.try_get("id").map_err(Self::sql_error)?,
            username: row.try_get("username").map_err(Self::sql_error)?,
        })
    }

    fn get_org_by_id(&self, id: i32) -> Result<NamespaceOrg, Error> {
        let row = block_on(
            sqlx::query("SELECT id, name FROM orgs WHERE id = $1")
                .bind(id)
                .fetch_optional(&self.pool),
        )
        .map_err(Self::sql_error)?
        .ok_or_else(|| Error::other("org not found"))?;
        Ok(NamespaceOrg {
            id: row.try_get("id").map_err(Self::sql_error)?,
            name: row.try_get("name").map_err(Self::sql_error)?,
        })
    }

    fn get_orgs_by_user_id(&self, user_id: i32) -> Result<Vec<NamespaceOrg>, Error> {
        let rows = block_on(
            sqlx::query(
                "SELECT o.id, o.name FROM orgs o \
                 JOIN org_members om ON om.org_id = o.id WHERE om.user_id = $1",
            )
            .bind(user_id)
            .fetch_all(&self.pool),
        )
        .map_err(Self::sql_error)?;
        rows.iter()
            .map(|row| {
                Ok(NamespaceOrg {
                    id: row.try_get("id").map_err(Self::sql_error)?,
                    name: row.try_get("name").map_err(Self::sql_error)?,
                })
            })
            .collect()
    }
}

fn optional_id(value: i32) -> Option<i32> {
    (value != 0).then_some(value)
}

fn optional_string(value: &str) -> Option<&str> {
    (!value.is_empty()).then_some(value)
}

fn record_to_search_context(record: SearchContextRecord) -> SearchContext {
    SearchContext {
        id: record.id,
        name: record.name,
        description: record.description,
        public: record.public,
        namespace_user_id: record.namespace_user_id,
        namespace_org_id: record.namespace_org_id,
        namespace_user_name: record.namespace_user_name,
        namespace_org_name: record.namespace_org_name,
        query: record.query,
        auto_defined: record.auto_defined,
    }
}

/// Keep the `OffsetDateTime` import meaningful for the row decoder's type
/// inference future-proofing (the column is decoded into this type by sqlx).
const _: fn(OffsetDateTime) = |_| {};
