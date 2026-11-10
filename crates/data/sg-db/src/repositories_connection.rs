//! `Query.repositories`'s database seam: `repoStore.List`/`Count` with the
//! baseline's repository filters, cursor pagination and permission predicate
//! (`internal/database/repos.go:425-435,839-1315`,
//! `internal/database/repos_perm.go:120-140`).
//!
//! Only the options reachable from `repositoryArgs` are modelled: the
//! `cloned`/`notCloned`, `indexed`/`notIndexed`, `embedded`/`notEmbedded`
//! triples, `cloneStatus`, `failedFetch`, `corrupted`, `externalService`,
//! `query` and `names`. The resulting SQL keeps the baseline's joins
//! (`gitserver_repos`, `zoekt_repos`, the completed-embedding subquery) and
//! its `ORDER BY <column>, id` / `LIMIT` pagination.

use sqlx::postgres::PgPool;
use sqlx::types::time::OffsetDateTime;
use sqlx::{Postgres, QueryBuilder, Row};

/// The `repo` columns the `Query.repositories` connection returns, plus the
/// cursor-only `created_at`/`repo_size_bytes` values.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RepoListRecord {
    pub id: i32,
    pub name: String,
    /// `repo.uri` (NULL → empty string).
    pub uri: String,
    /// `repo.description` (NULL → empty string).
    pub description: String,
    pub fork: bool,
    pub archived: bool,
    pub private: bool,
    pub stars: i32,
    pub created_at: OffsetDateTime,
    /// `repo.external_id` (NULL → empty string).
    pub external_id: String,
    /// `repo.external_service_type` (NULL → empty string).
    pub external_service_type: String,
    /// `repo.external_service_id` (NULL → empty string).
    pub external_service_id: String,
    /// `gitserver_repos.repo_size_bytes`; `0` when the row is not joined or
    /// the column is NULL (the baseline scans NULL into `int64` → 0).
    pub repo_size_bytes: i64,
}

/// `database.RepoListColumn`: the three order-by columns reachable from
/// `RepositoryOrderBy` (`repositories.go:283-292`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RepoOrderColumn {
    Name,
    CreatedAt,
    Size,
}

impl RepoOrderColumn {
    /// The SQL column used by `ORDER BY` and the cursor row comparison.
    pub fn sql_column(&self) -> &'static str {
        match self {
            Self::Name => "repo.name",
            Self::CreatedAt => "repo.created_at",
            Self::Size => "gr.repo_size_bytes",
        }
    }

    /// `database.RepoListColumn`'s wire value, which is what the cursor's
    /// `c` field stores.
    pub fn cursor_column(&self) -> &'static str {
        match self {
            Self::Name => "name",
            Self::CreatedAt => "created_at",
            Self::Size => "gr.repo_size_bytes",
        }
    }
}

/// One `repositoryArgs.orderBy`/`descending` pair.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RepoListOrder {
    pub column: RepoOrderColumn,
    /// `gqlutil.ConnectionResolverOptions.Ascending` (`!descending`).
    pub ascending: bool,
}

/// A decoded `RepositoryCursor` value. `Text` covers `name`/`created_at`
/// (the baseline keeps the RFC3339 string for the latter).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RepoCursorValue {
    Text(String),
    Size(i64),
}

/// The `<value>@<id>` cursor pair unmarshalled by
/// `repositoriesConnectionStore.UnmarshalCursor`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RepoCursor {
    pub value: RepoCursorValue,
    pub repo_id: i32,
}

/// `database.PaginationArgs` after `gqlutil` converted the GraphQL arguments.
/// `first`/`last` already include the `pageSize + 1` over-fetch; `first` wins
/// when both are present, exactly like `Limit`/`paginationArgs`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RepoPagination {
    pub first: Option<i64>,
    pub last: Option<i64>,
    pub after: Option<RepoCursor>,
    pub before: Option<RepoCursor>,
}

/// `database.ReposListOptions`, restricted to `repositoryArgs`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RepoListOptions {
    pub query: Option<String>,
    /// `database.maybeQueryIsID(query)` result, computed by the resolver layer.
    pub query_id: Option<i32>,
    pub names: Option<Vec<String>>,
    pub no_cloned: bool,
    pub only_cloned: bool,
    /// A non-unknown `CloneStatus` value (`not_cloned`/`cloning`/`cloned`).
    pub clone_status: Option<String>,
    pub no_indexed: bool,
    pub only_indexed: bool,
    pub no_embedded: bool,
    pub only_embedded: bool,
    pub failed_fetch: bool,
    pub only_corrupted: bool,
    pub external_service_ids: Vec<i64>,
}

impl RepoListOptions {
    /// Whether the baseline joins `gitserver_repos` for the filters alone
    /// (`repos.go:1197-1200`, excluding the order-by-size condition).
    fn needs_gitserver_join(&self) -> bool {
        self.no_cloned
            || self.only_cloned
            || self.clone_status.is_some()
            || self.failed_fetch
            || self.only_corrupted
    }
}

/// `repoStore.List` with `PaginationArgs` (`repos.go:839-853`).
pub async fn list(
    pool: &PgPool,
    authenticated_user_id: i32,
    bypass_authz: bool,
    options: &RepoListOptions,
    order: RepoListOrder,
    pagination: &RepoPagination,
) -> Result<Vec<RepoListRecord>, sqlx::Error> {
    let join_gitserver = options.needs_gitserver_join() || order.column == RepoOrderColumn::Size;
    let mut builder = QueryBuilder::<Postgres>::new("");
    push_select(&mut builder, join_gitserver);
    push_from(&mut builder, options, join_gitserver);
    push_where(
        &mut builder,
        authenticated_user_id,
        bypass_authz,
        options,
        Some(order),
        Some(pagination),
    );

    push_order_and_limit(&mut builder, order, pagination);

    let rows: Vec<sqlx::postgres::PgRow> = builder.build().fetch_all(pool).await?;
    rows.into_iter().map(record_from_row).collect()
}

/// `repoStore.Count` (`repos.go:322-336`): the same predicates, no ordering or
/// pagination.
pub async fn count(
    pool: &PgPool,
    authenticated_user_id: i32,
    bypass_authz: bool,
    options: &RepoListOptions,
) -> Result<i64, sqlx::Error> {
    let join_gitserver = options.needs_gitserver_join();
    let mut builder = QueryBuilder::<Postgres>::new("SELECT COUNT(*)");
    push_from(&mut builder, options, join_gitserver);
    push_where(
        &mut builder,
        authenticated_user_id,
        bypass_authz,
        options,
        None,
        None,
    );
    builder.build().fetch_one(pool).await?.try_get(0)
}

fn push_select(builder: &mut QueryBuilder<'_, Postgres>, join_gitserver: bool) {
    builder.push(
        "SELECT repo.id, repo.name, COALESCE(repo.uri, '') AS uri, \
         COALESCE(repo.description, '') AS description, repo.fork, repo.archived, \
         repo.private, repo.stars, repo.created_at, \
         COALESCE(repo.external_id, '') AS external_id, \
         COALESCE(repo.external_service_type, '') AS external_service_type, \
         COALESCE(repo.external_service_id, '') AS external_service_id, ",
    );
    if join_gitserver {
        builder.push("COALESCE(gr.repo_size_bytes, 0) AS repo_size_bytes");
    } else {
        builder.push("0::bigint AS repo_size_bytes");
    }
}

fn push_from(
    builder: &mut QueryBuilder<'_, Postgres>,
    options: &RepoListOptions,
    join_gitserver: bool,
) {
    builder.push(" FROM repo");
    if join_gitserver {
        builder.push(" JOIN gitserver_repos gr ON gr.repo_id = repo.id");
    }
    if options.no_indexed || options.only_indexed {
        builder.push(" JOIN zoekt_repos zr ON zr.repo_id = repo.id");
    }
    if options.no_embedded || options.only_embedded {
        builder.push(
            " LEFT JOIN (SELECT DISTINCT ON (repo_id) repo_id, true embedded \
             FROM repo_embedding_jobs WHERE state = 'completed') embedded \
             ON embedded.repo_id = repo.id",
        );
    }
}

/// The baseline appends the cursor predicate first, then the option filters,
/// and finally the permission predicate (`repos.go:938-1290`,
/// `repos_perm.go:120-140`).
fn push_where(
    builder: &mut QueryBuilder<'_, Postgres>,
    authenticated_user_id: i32,
    bypass_authz: bool,
    options: &RepoListOptions,
    order: Option<RepoListOrder>,
    pagination: Option<&RepoPagination>,
) {
    builder.push(" WHERE repo.deleted_at IS NULL AND repo.blocked IS NULL AND (");

    let mut first_condition = true;

    if let (Some(order), Some(pagination)) = (order, pagination) {
        if let Some(after) = &pagination.after {
            and_clause(builder, &mut first_condition);
            push_cursor_comparison(builder, order, after, true);
        }
        if let Some(before) = &pagination.before {
            and_clause(builder, &mut first_condition);
            push_cursor_comparison(builder, order, before, false);
        }
    }

    if let Some(query) = &options.query {
        and_clause(builder, &mut first_condition);
        builder.push("(lower(repo.name) LIKE ");
        builder.push_bind(format!("%{}%", query.to_lowercase()));
        if let Some(query_id) = options.query_id {
            builder.push(" OR repo.id = ");
            builder.push_bind(query_id);
        }
        builder.push(")");
    }

    if let Some(names) = &options.names {
        if !names.is_empty() {
            and_clause(builder, &mut first_condition);
            builder.push("lower(repo.name::text) COLLATE \"C\" = ANY(");
            builder.push_bind(
                names
                    .iter()
                    .map(|name| name.to_lowercase())
                    .collect::<Vec<String>>(),
            );
            builder.push("::text[])");
        }
    }

    if !options.external_service_ids.is_empty() {
        and_clause(builder, &mut first_condition);
        builder.push(
            "EXISTS (SELECT 1 FROM external_service_repos esr \
             WHERE repo.id = esr.repo_id AND esr.external_service_id = ANY(",
        );
        builder.push_bind(options.external_service_ids.clone());
        builder.push("))");
    }

    if options.no_cloned {
        and_clause(builder, &mut first_condition);
        builder.push("(gr.clone_status IN ('not_cloned', 'cloning'))");
    }
    if options.only_cloned {
        and_clause(builder, &mut first_condition);
        builder.push("gr.clone_status = 'cloned'");
    }
    if let Some(clone_status) = &options.clone_status {
        and_clause(builder, &mut first_condition);
        builder.push("gr.clone_status = ");
        builder.push_bind(clone_status.clone());
    }
    if options.no_indexed {
        and_clause(builder, &mut first_condition);
        builder.push("zr.index_status = 'not_indexed'");
    }
    if options.only_indexed {
        and_clause(builder, &mut first_condition);
        builder.push("zr.index_status = 'indexed'");
    }
    if options.no_embedded {
        and_clause(builder, &mut first_condition);
        builder.push("embedded IS NULL");
    }
    if options.only_embedded {
        and_clause(builder, &mut first_condition);
        builder.push("embedded IS NOT NULL");
    }
    if options.failed_fetch {
        and_clause(builder, &mut first_condition);
        builder.push("gr.last_error IS NOT NULL");
    }
    if options.only_corrupted {
        and_clause(builder, &mut first_condition);
        builder.push("gr.corrupted_at IS NOT NULL");
    }

    // `AuthzQueryConds`: bypass → TRUE, otherwise the three-way predicate.
    and_clause(builder, &mut first_condition);
    push_authz(builder, authenticated_user_id, bypass_authz);

    builder.push(")");
}

/// Append `AND` unless this is the first predicate inside the filter group.
fn and_clause(builder: &mut QueryBuilder<'_, Postgres>, first: &mut bool) {
    if *first {
        *first = false;
    } else {
        builder.push(" AND ");
    }
}

fn push_authz(
    builder: &mut QueryBuilder<'_, Postgres>,
    authenticated_user_id: i32,
    bypass_authz: bool,
) {
    if bypass_authz {
        builder.push("(TRUE)");
        return;
    }
    builder.push(
        "(EXISTS (SELECT FROM user_repo_permissions \
         WHERE repo_id = repo.id AND user_id IS NULL) \
         OR NOT repo.private \
         OR EXISTS (SELECT FROM external_services es \
         JOIN external_service_repos esr ON (esr.external_service_id = es.id \
         AND esr.repo_id = repo.id AND es.unrestricted = TRUE \
         AND es.deleted_at IS NULL)) \
         OR EXISTS (SELECT repo_id FROM user_repo_permissions \
         WHERE repo_id = repo.id AND user_id = ",
    );
    builder.push_bind(authenticated_user_id);
    builder.push("))");
}

/// `PaginationArgs.SQL`'s `(columns) <op> (values)` predicate. A cursor
/// `after` uses `>` when ascending and `<` when descending; `before` is the
/// inverse.
fn push_cursor_comparison(
    builder: &mut QueryBuilder<'_, Postgres>,
    order: RepoListOrder,
    cursor: &RepoCursor,
    after: bool,
) {
    let greater = order.ascending == after;
    builder.push("(");
    builder.push(order.column.sql_column());
    builder.push(", repo.id) ");
    builder.push(if greater { ">" } else { "<" });
    builder.push(" (");
    match &cursor.value {
        RepoCursorValue::Text(value) => {
            builder.push_bind(value.clone());
            builder.push(match order.column {
                RepoOrderColumn::Name => "::citext",
                RepoOrderColumn::CreatedAt => "::timestamptz",
                RepoOrderColumn::Size => "",
            });
        }
        RepoCursorValue::Size(value) => {
            builder.push_bind(*value);
        }
    }
    builder.push(", ");
    builder.push_bind(cursor.repo_id);
    builder.push(")");
}

/// `PaginationArgs.SQL`: `first` uses `Ascending`, `last` reverses it; either
/// way the limit is the over-fetch `pageSize + 1`.
fn push_order_and_limit(
    builder: &mut QueryBuilder<'_, Postgres>,
    order: RepoListOrder,
    pagination: &RepoPagination,
) {
    let (ascending, limit) = if let Some(first) = pagination.first {
        (order.ascending, first)
    } else if let Some(last) = pagination.last {
        (!order.ascending, last)
    } else {
        (order.ascending, 0)
    };
    let direction = if ascending { "ASC" } else { "DESC" };
    builder.push(" ORDER BY ");
    builder.push(order.column.sql_column());
    builder.push(" ");
    builder.push(direction);
    builder.push(", repo.id ");
    builder.push(direction);
    builder.push(format!(" LIMIT {limit}"));
}

fn record_from_row(row: sqlx::postgres::PgRow) -> Result<RepoListRecord, sqlx::Error> {
    Ok(RepoListRecord {
        id: row.try_get("id")?,
        name: row.try_get("name")?,
        uri: row.try_get("uri")?,
        description: row.try_get("description")?,
        fork: row.try_get("fork")?,
        archived: row.try_get("archived")?,
        private: row.try_get("private")?,
        stars: row.try_get("stars")?,
        created_at: row.try_get("created_at")?,
        external_id: row.try_get("external_id")?,
        external_service_type: row.try_get("external_service_type")?,
        external_service_id: row.try_get("external_service_id")?,
        repo_size_bytes: row.try_get("repo_size_bytes")?,
    })
}
