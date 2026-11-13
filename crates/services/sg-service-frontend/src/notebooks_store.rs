#![forbid(unsafe_code)]

//! Production Postgres adapter for
//! [`crate::notebooks_graphql::NotebooksGraphqlDatabase`] (M9.29a).
//!
//! Bridges the synchronous GraphQL seam to sqlx with
//! `tokio::task::block_in_place` and reproduces
//! `internal/notebooks/store.go`'s SQL (permission condition, text-search
//! filter, star join/group-by, ordering). No table, column, index or Redis key
//! is added.

use std::future::Future;

use serde_json::Value;
use sqlx::{PgPool, Postgres, Row};

use sg_graphql::exec::Actor;

use crate::graphql_resolvers::{OrgData, UserData};
use crate::notebooks_graphql::{
    ListNotebooksOptions, ListNotebooksPageOptions, ListNotebookStarsPageOptions, NotebookBlock,
    NotebookData, NotebookStarData, NotebooksGraphqlDatabase, NotebooksOrderBy,
};

fn block_on<F: Future>(future: F) -> F::Output {
    match tokio::runtime::Handle::try_current() {
        Ok(handle) => tokio::task::block_in_place(|| handle.block_on(future)),
        Err(_) => tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("notebooks blocking runtime")
            .block_on(future),
    }
}

fn scan_notebook(row: &sqlx::postgres::PgRow) -> Result<NotebookData, String> {
    let blocks_value: Value = row.try_get("blocks").map_err(|err| err.to_string())?;
    let blocks: Vec<NotebookBlock> =
        serde_json::from_value(blocks_value).map_err(|err| err.to_string())?;
    Ok(NotebookData {
        id: row.try_get("id").map_err(|err| err.to_string())?,
        title: row.try_get("title").map_err(|err| err.to_string())?,
        blocks,
        public: row.try_get("public").map_err(|err| err.to_string())?,
        creator_user_id: row
            .try_get::<Option<i32>, _>("creator_user_id")
            .map_err(|err| err.to_string())?
            .unwrap_or(0),
        updater_user_id: row
            .try_get::<Option<i32>, _>("updater_user_id")
            .map_err(|err| err.to_string())?
            .unwrap_or(0),
        namespace_user_id: row
            .try_get::<Option<i32>, _>("namespace_user_id")
            .map_err(|err| err.to_string())?
            .unwrap_or(0),
        namespace_org_id: row
            .try_get::<Option<i32>, _>("namespace_org_id")
            .map_err(|err| err.to_string())?
            .unwrap_or(0),
        created_at: row.try_get("created_at").map_err(|err| err.to_string())?,
        updated_at: row.try_get("updated_at").map_err(|err| err.to_string())?,
        pattern_type: row.try_get("pattern_type").map_err(|err| err.to_string())?,
    })
}

const NOTEBOOK_COLUMNS: &str = "notebooks.id, notebooks.title, notebooks.blocks, \
    notebooks.public, notebooks.creator_user_id, notebooks.updater_user_id, \
    notebooks.namespace_user_id, notebooks.namespace_org_id, notebooks.created_at, \
    notebooks.updated_at, notebooks.pattern_type::text AS pattern_type";

/// `notebooksPermissionsCondition`: internal actors bypass, public notebooks
/// are visible to everyone, private user notebooks to their creator and
/// private org notebooks to members.
fn permissions_clause(actor: Actor) -> String {
    let bypass = if actor.is_internal() { "TRUE" } else { "FALSE" };
    let uid = actor.uid().unwrap_or(0);
    format!(
        "({bypass} OR notebooks.public \
         OR (notebooks.namespace_user_id IS NOT NULL AND notebooks.namespace_user_id = {uid}) \
         OR (notebooks.namespace_org_id IS NOT NULL AND EXISTS (SELECT FROM org_members om \
             WHERE om.org_id = notebooks.namespace_org_id AND om.user_id = {uid})))"
    )
}

/// `getNotebooksOrderByClause`.
fn order_by_clause(order_by: NotebooksOrderBy, descending: bool) -> String {
    let direction = if descending { "DESC" } else { "ASC" };
    let expression = match order_by {
        NotebooksOrderBy::CreatedAt => "notebooks.created_at",
        NotebooksOrderBy::UpdatedAt => "notebooks.updated_at",
        NotebooksOrderBy::Id => "notebooks.id",
        NotebooksOrderBy::StarCount => {
            "(SELECT COUNT(*) FROM notebook_stars WHERE notebook_id = notebooks.id)"
        }
    };
    format!("{expression} {direction}")
}

/// `toPostgresTextSearchQuery`: strip TSQUERY specials, split on whitespace
/// and turn every token into a prefix term joined with `&`.
fn to_postgres_text_search_query(query: &str) -> String {
    let mut sanitized = String::with_capacity(query.len());
    for character in query.chars() {
        if matches!(character, '&' | '|' | '!' | '(' | ')' | ':') {
            sanitized.push(' ');
        } else {
            sanitized.push(character);
        }
    }
    sanitized
        .split_whitespace()
        .map(|token| format!("{token}:*"))
        .collect::<Vec<_>>()
        .join(" & ")
}

struct ListSql {
    join: String,
    group_by: String,
    conditions: Vec<String>,
    binds: Vec<String>,
}

/// `getNotebooksQueryCondition` + join/group-by.
fn list_sql(opts: &ListNotebooksOptions) -> ListSql {
    let mut join = String::new();
    let mut group_by = String::new();
    let mut conditions: Vec<String> = Vec::new();
    let mut binds: Vec<String> = Vec::new();
    if opts.starred_by_user_id != 0 {
        join = "LEFT JOIN notebook_stars ON notebooks.id = notebook_stars.notebook_id".to_owned();
        group_by = "GROUP BY notebooks.id".to_owned();
        conditions.push(format!(
            "notebook_stars.user_id = {}",
            opts.starred_by_user_id
        ));
    }
    if opts.creator_user_id != 0 {
        conditions.push(format!(
            "notebooks.creator_user_id = {}",
            opts.creator_user_id
        ));
    }
    if opts.namespace_user_id != 0 {
        conditions.push(format!(
            "notebooks.namespace_user_id = {}",
            opts.namespace_user_id
        ));
    }
    if opts.namespace_org_id != 0 {
        conditions.push(format!(
            "notebooks.namespace_org_id = {}",
            opts.namespace_org_id
        ));
    }
    if !opts.query.is_empty() {
        conditions.push(format!(
            "(notebooks.title ILIKE $1 OR notebooks.blocks_tsvector @@ to_tsquery('english', $2))"
        ));
        binds.push(format!("%{}%", opts.query));
        binds.push(to_postgres_text_search_query(&opts.query));
    }
    if conditions.is_empty() {
        conditions.push("1 = 1".to_owned());
    }
    ListSql {
        join,
        group_by,
        conditions,
        binds,
    }
}

fn bind_strings<'q>(
    mut query: sqlx::query::Query<'q, Postgres, sqlx::postgres::PgArguments>,
    binds: &'q [String],
) -> sqlx::query::Query<'q, Postgres, sqlx::postgres::PgArguments> {
    for value in binds {
        query = query.bind(value.as_str());
    }
    query
}

/// Production adapter.
#[derive(Debug)]
pub struct PgNotebooksStore {
    pool: PgPool,
}

impl PgNotebooksStore {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    fn user_columns() -> &'static str {
        "id, username, display_name, avatar_url, created_at, updated_at, site_admin, tos_accepted"
    }

    fn scan_user(row: &sqlx::postgres::PgRow) -> Result<UserData, sqlx::Error> {
        let display_name: Option<String> = row.try_get("display_name")?;
        let avatar_url: Option<String> = row.try_get("avatar_url")?;
        Ok(UserData {
            id: row.try_get("id")?,
            username: row.try_get("username")?,
            display_name: display_name.unwrap_or_default(),
            avatar_url: avatar_url.unwrap_or_default(),
            created_at: row.try_get("created_at")?,
            updated_at: row.try_get("updated_at")?,
            site_admin: row.try_get("site_admin")?,
            tos_accepted: row.try_get("tos_accepted")?,
            primary_email: String::new(),
        })
    }
}

impl NotebooksGraphqlDatabase for PgNotebooksStore {
    fn get_notebook(&self, actor: Actor, id: i64) -> Result<Option<NotebookData>, String> {
        let sql = format!(
            "SELECT {NOTEBOOK_COLUMNS} FROM notebooks WHERE ({}) AND notebooks.id = $1",
            permissions_clause(actor)
        );
        let rows = block_on(sqlx::query(&sql).bind(id).fetch_all(&self.pool))
            .map_err(|err| err.to_string())?;
        match rows.first() {
            // `ErrNotebookNotFound` is what the resolver propagates.
            None => Err("notebook not found".to_owned()),
            Some(row) => scan_notebook(row).map(Some),
        }
    }

    fn create_notebook(&self, notebook: &NotebookData) -> Result<NotebookData, String> {
        let blocks = serde_json::to_value(&notebook.blocks).map_err(|err| err.to_string())?;
        let sql = format!(
            "INSERT INTO notebooks \
             (title, blocks, public, creator_user_id, updater_user_id, namespace_user_id, \
              namespace_org_id) VALUES ($1, $2, $3, $4, $5, $6, $7) RETURNING {NOTEBOOK_COLUMNS}"
        );
        let row = block_on(
            sqlx::query(&sql)
                .bind(&notebook.title)
                .bind(blocks)
                .bind(notebook.public)
                .bind(option_id(notebook.creator_user_id))
                .bind(option_id(notebook.updater_user_id))
                .bind(option_id(notebook.namespace_user_id))
                .bind(option_id(notebook.namespace_org_id))
                .fetch_one(&self.pool),
        )
        .map_err(|err| err.to_string())?;
        scan_notebook(&row)
    }

    fn update_notebook(&self, notebook: &NotebookData) -> Result<NotebookData, String> {
        let blocks = serde_json::to_value(&notebook.blocks).map_err(|err| err.to_string())?;
        let sql = format!(
            "UPDATE notebooks SET title = $1, blocks = $2, public = $3, updater_user_id = $4, \
             namespace_user_id = $5, namespace_org_id = $6, updated_at = now() \
             WHERE id = $7 RETURNING {NOTEBOOK_COLUMNS}"
        );
        let row = block_on(
            sqlx::query(&sql)
                .bind(&notebook.title)
                .bind(blocks)
                .bind(notebook.public)
                .bind(option_id(notebook.updater_user_id))
                .bind(option_id(notebook.namespace_user_id))
                .bind(option_id(notebook.namespace_org_id))
                .bind(notebook.id)
                .fetch_one(&self.pool),
        )
        .map_err(|err| err.to_string())?;
        scan_notebook(&row)
    }

    fn delete_notebook(&self, id: i64) -> Result<(), String> {
        block_on(sqlx::query("DELETE FROM notebooks WHERE id = $1").bind(id).execute(&self.pool))
            .map(|_| ())
            .map_err(|err| err.to_string())
    }

    fn list_notebooks(
        &self,
        actor: Actor,
        page: &ListNotebooksPageOptions,
        opts: &ListNotebooksOptions,
    ) -> Result<Vec<NotebookData>, String> {
        let parts = list_sql(opts);
        let order_by = opts.order_by.unwrap_or(NotebooksOrderBy::UpdatedAt);
        let sql = format!(
            "SELECT {NOTEBOOK_COLUMNS} FROM notebooks {} WHERE ({}) AND ({}) {} \
             ORDER BY {} LIMIT {} OFFSET {}",
            parts.join,
            permissions_clause(actor),
            parts.conditions.join("\n AND "),
            parts.group_by,
            order_by_clause(order_by, opts.order_by_descending),
            page.first,
            page.after
        );
        let query = bind_strings(sqlx::query(&sql), &parts.binds);
        let rows = block_on(query.fetch_all(&self.pool)).map_err(|err| err.to_string())?;
        rows.iter().map(scan_notebook).collect()
    }

    fn count_notebooks(
        &self,
        actor: Actor,
        opts: &ListNotebooksOptions,
    ) -> Result<i64, String> {
        let parts = list_sql(opts);
        let sql = format!(
            "SELECT COUNT(DISTINCT notebooks.id) FROM notebooks {} WHERE ({}) AND ({})",
            parts.join,
            permissions_clause(actor),
            parts.conditions.join("\n AND ")
        );
        let query = bind_strings(sqlx::query(&sql), &parts.binds);
        let row = block_on(query.fetch_one(&self.pool)).map_err(|err| err.to_string())?;
        row.try_get::<i64, _>(0).map_err(|err| err.to_string())
    }

    fn get_notebook_star(
        &self,
        notebook_id: i64,
        user_id: i32,
    ) -> Result<Option<NotebookStarData>, String> {
        let rows = block_on(
            sqlx::query(
                "SELECT notebook_id, user_id, created_at FROM notebook_stars \
                 WHERE notebook_id = $1 AND user_id = $2",
            )
            .bind(notebook_id as i32)
            .bind(user_id)
            .fetch_all(&self.pool),
        )
        .map_err(|err| err.to_string())?;
        match rows.first() {
            Some(row) => Ok(Some(scan_star(row)?)),
            None => Ok(None),
        }
    }

    fn create_notebook_star(
        &self,
        notebook_id: i64,
        user_id: i32,
    ) -> Result<NotebookStarData, String> {
        let row = block_on(
            sqlx::query(
                "INSERT INTO notebook_stars (notebook_id, user_id) VALUES ($1, $2) \
                 RETURNING notebook_id, user_id, created_at",
            )
            .bind(notebook_id as i32)
            .bind(user_id)
            .fetch_one(&self.pool),
        )
        .map_err(|err| err.to_string())?;
        scan_star(&row)
    }

    fn delete_notebook_star(&self, notebook_id: i64, user_id: i32) -> Result<(), String> {
        block_on(
            sqlx::query("DELETE FROM notebook_stars WHERE notebook_id = $1 AND user_id = $2")
                .bind(notebook_id as i32)
                .bind(user_id)
                .execute(&self.pool),
        )
        .map(|_| ())
        .map_err(|err| err.to_string())
    }

    fn list_notebook_stars(
        &self,
        page: &ListNotebookStarsPageOptions,
        notebook_id: i64,
    ) -> Result<Vec<NotebookStarData>, String> {
        let rows = block_on(
            sqlx::query(
                "SELECT notebook_id, user_id, created_at FROM notebook_stars \
                 WHERE notebook_id = $1 ORDER BY created_at DESC LIMIT $2 OFFSET $3",
            )
            .bind(notebook_id as i32)
            .bind(page.first)
            .bind(page.after)
            .fetch_all(&self.pool),
        )
        .map_err(|err| err.to_string())?;
        rows.iter().map(scan_star).collect()
    }

    fn count_notebook_stars(&self, notebook_id: i64) -> Result<i64, String> {
        let row = block_on(
            sqlx::query("SELECT COUNT(*) FROM notebook_stars WHERE notebook_id = $1")
                .bind(notebook_id as i32)
                .fetch_one(&self.pool),
        )
        .map_err(|err| err.to_string())?;
        row.try_get::<i64, _>(0).map_err(|err| err.to_string())
    }

    fn user_by_id(&self, id: i32) -> Result<Option<UserData>, String> {
        let sql = format!(
            "SELECT {} FROM users WHERE id = $1 AND deleted_at IS NULL LIMIT 1",
            Self::user_columns()
        );
        let rows = block_on(sqlx::query(&sql).bind(id).fetch_all(&self.pool))
            .map_err(|err| err.to_string())?;
        match rows.first() {
            Some(row) => Self::scan_user(row)
                .map(Some)
                .map_err(|err| err.to_string()),
            None => Ok(None),
        }
    }

    fn org_by_id(
        &self,
        id: i32,
        viewer_uid: i32,
        viewer_site_admin: bool,
    ) -> Result<Option<OrgData>, String> {
        let rows = block_on(
            sqlx::query(
                "SELECT id, name, display_name, created_at FROM orgs \
                 WHERE deleted_at IS NULL AND id = $1 LIMIT 1",
            )
            .bind(id)
            .fetch_all(&self.pool),
        )
        .map_err(|err| err.to_string())?;
        let Some(row) = rows.first() else {
            return Ok(None);
        };
        let viewer_is_member = if viewer_uid != 0 {
            self.is_org_member(id, viewer_uid)?
        } else {
            false
        };
        Ok(Some(OrgData {
            id: row.try_get("id").map_err(|err| err.to_string())?,
            name: row.try_get("name").map_err(|err| err.to_string())?,
            display_name: row.try_get("display_name").map_err(|err| err.to_string())?,
            created_at: row.try_get("created_at").map_err(|err| err.to_string())?,
            viewer_can_administer: viewer_site_admin || viewer_is_member,
            viewer_is_member,
        }))
    }

    fn is_org_member(&self, org_id: i32, user_id: i32) -> Result<bool, String> {
        let rows = block_on(
            sqlx::query(
                "SELECT 1 FROM org_members om INNER JOIN users ON om.user_id = users.id \
                 WHERE om.org_id = $1 AND om.user_id = $2 AND users.deleted_at IS NULL LIMIT 1",
            )
            .bind(org_id)
            .bind(user_id)
            .fetch_all(&self.pool),
        )
        .map_err(|err| err.to_string())?;
        Ok(!rows.is_empty())
    }

    fn current_user(&self, actor: Actor) -> Result<Option<UserData>, String> {
        match actor {
            Actor::Anonymous | Actor::Internal => Ok(None),
            Actor::User { uid } => match self.user_by_id(uid)? {
                Some(user) => Ok(Some(user)),
                None => Err(format!("user not found: [userID {uid}]")),
            },
        }
    }
}

fn scan_star(row: &sqlx::postgres::PgRow) -> Result<NotebookStarData, String> {
    // `notebook_stars.notebook_id` is a 4-byte integer in the baseline schema
    // (Go scans it into an `int64` field).
    let notebook_id: i32 = row.try_get("notebook_id").map_err(|err| err.to_string())?;
    Ok(NotebookStarData {
        notebook_id: i64::from(notebook_id),
        user_id: row.try_get("user_id").map_err(|err| err.to_string())?,
        created_at: row.try_get("created_at").map_err(|err| err.to_string())?,
    })
}

fn option_id(id: i32) -> Option<i32> {
    if id == 0 {
        None
    } else {
        Some(id)
    }
}

/// `internal/notebooks.IsEnabled`: `DISABLE_NOTEBOOKS` (default enabled).
pub fn notebooks_enabled() -> bool {
    std::env::var("DISABLE_NOTEBOOKS")
        .ok()
        .and_then(|value| value.parse::<bool>().ok())
        .map(|disabled| !disabled)
        .unwrap_or(true)
}
