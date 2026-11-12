#![forbid(unsafe_code)]

//! Production Postgres adapter for [`crate::saved_searches_graphql::SavedSearchesGraphqlDatabase`]
//! (M9.13a).
//!
//! Ports `internal/database/saved_searches.go`: the `saved_searchColumns`
//! list, `SavedSearchListArgs.toSQL()`, the `PaginationArgs.SQL()` composite
//! cursor comparisons (`(updated_at) < ($1)`-style), `Create`/`Update`/
//! `UpdateOwner`/`UpdateVisibility`/`Delete`/`GetByID`/`List`/`Count`, plus
//! the `users`/`orgs`/`org_members` lookups the resolvers need.
//!
//! No table, column or index is added. The synchronous trait is bridged to
//! `sqlx` with the same `block_in_place` helper as the other store adapters.

use std::future::Future;

use sqlx::postgres::PgRow;
use sqlx::{PgPool, Row};
use time::OffsetDateTime;

use crate::graphql_resolvers::{OrgData, UserData};
use crate::saved_searches_graphql::{
    NamespaceIds, SavedSearchCursorValue, SavedSearchListArgs, SavedSearchRow,
    SavedSearchesGraphqlDatabase, SavedSearchesOrderBy,
};

fn block_on<F: Future>(future: F) -> F::Output {
    match tokio::runtime::Handle::try_current() {
        Ok(handle) => tokio::task::block_in_place(|| handle.block_on(future)),
        Err(_) => tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("saved searches blocking runtime")
            .block_on(future),
    }
}

/// `savedSearchColumns` (`saved_searches.go:61`).
const SAVED_SEARCH_COLUMNS: &str = "description, query, draft, user_id, org_id, \
     visibility_secret, created_by, created_at, updated_by, updated_at";

fn scan_saved_search(row: &PgRow) -> Result<SavedSearchRow, sqlx::Error> {
    Ok(SavedSearchRow {
        id: row.try_get("id")?,
        description: row.try_get("description")?,
        query: row.try_get("query")?,
        draft: row.try_get("draft")?,
        user_id: row.try_get("user_id")?,
        org_id: row.try_get("org_id")?,
        visibility_secret: row.try_get("visibility_secret")?,
        created_by_user: row.try_get("created_by")?,
        created_at: row.try_get("created_at")?,
        updated_by_user: row.try_get("updated_by")?,
        updated_at: row.try_get("updated_at")?,
    })
}

fn error(error: sqlx::Error) -> String {
    error.to_string()
}

enum Bind {
    I32(i32),
    I64(i64),
    Text(String),
    Timestamp(OffsetDateTime),
}

impl Bind {
    fn apply<'q>(
        &self,
        query: sqlx::query::Query<'q, sqlx::Postgres, sqlx::postgres::PgArguments>,
    ) -> sqlx::query::Query<'q, sqlx::Postgres, sqlx::postgres::PgArguments> {
        match self {
            Self::I32(value) => query.bind(*value),
            Self::I64(value) => query.bind(*value),
            Self::Text(value) => query.bind(value.clone()),
            Self::Timestamp(value) => query.bind(*value),
        }
    }

    fn apply_scalar<'q>(
        &self,
        query: sqlx::query::QueryScalar<'q, sqlx::Postgres, i64, sqlx::postgres::PgArguments>,
    ) -> sqlx::query::QueryScalar<'q, sqlx::Postgres, i64, sqlx::postgres::PgArguments> {
        match self {
            Self::I32(value) => query.bind(*value as i64),
            Self::I64(value) => query.bind(*value),
            Self::Text(value) => query.bind(value.clone()),
            Self::Timestamp(value) => query.bind(*value),
        }
    }
}

pub struct PgSavedSearchesGraphqlStore {
    pool: PgPool,
}

impl PgSavedSearchesGraphqlStore {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

/// `SavedSearchListArgs.toSQL()` rendered with positional placeholders.
struct FilterSql {
    where_sql: String,
    binds: Vec<Bind>,
    next_index: usize,
}

fn filter_sql(args: &SavedSearchListArgs) -> Result<FilterSql, String> {
    let mut conditions: Vec<String> = Vec::new();
    let mut binds: Vec<Bind> = Vec::new();
    let mut index = 0;

    if !args.query.is_empty() {
        let pattern = format!("%{}%", args.query);
        index += 1;
        let a = index;
        index += 1;
        let b = index;
        conditions.push(format!("(description ILIKE ${a} OR query ILIKE ${b})"));
        binds.push(Bind::Text(pattern.clone()));
        binds.push(Bind::Text(pattern));
    }
    if let Some(user) = args.affiliated_user {
        index += 1;
        let a = index;
        index += 1;
        let b = index;
        conditions.push(format!(
            "(user_id=${a} OR org_id IN (SELECT org_members.org_id FROM org_members \
             LEFT JOIN orgs ON orgs.id=org_members.org_id \
             WHERE orgs.deleted_at IS NULL AND org_members.user_id=${b}) \
             OR NOT visibility_secret)"
        ));
        binds.push(Bind::I32(user));
        binds.push(Bind::I32(user));
    }
    if args.public_only {
        conditions.push("NOT visibility_secret".to_owned());
    }
    if let Some(owner) = args.owner {
        if let Some(user) = owner.user.filter(|id| *id != 0) {
            index += 1;
            conditions.push(format!("user_id=${index}"));
            binds.push(Bind::I32(user));
        } else if let Some(org) = owner.org.filter(|id| *id != 0) {
            index += 1;
            conditions.push(format!("org_id=${index}"));
            binds.push(Bind::I32(org));
        } else {
            return Err("invalid owner (no user or org ID)".to_owned());
        }
    }
    if args.hide_drafts {
        conditions.push("NOT draft".to_owned());
    }
    if conditions.is_empty() {
        conditions.push("TRUE".to_owned());
    }
    Ok(FilterSql {
        where_sql: conditions.join(") AND ("),
        binds,
        next_index: index,
    })
}

/// `PaginationArgs.SQL()`: composite cursor comparisons plus order/limit.
struct ListSql {
    sql: String,
    binds: Vec<Bind>,
}

fn list_sql(
    args: &SavedSearchListArgs,
    order: SavedSearchesOrderBy,
    after: Option<&[SavedSearchCursorValue]>,
    before: Option<&[SavedSearchCursorValue]>,
    first: Option<i64>,
    last: Option<i64>,
) -> Result<ListSql, String> {
    let filter = filter_sql(args)?;
    let mut conditions = vec![filter.where_sql];
    let mut binds = filter.binds;
    let mut index = filter.next_index;

    let ascending = order.ascending();
    let column = order.column();

    let push_cursor =
        |values: &[SavedSearchCursorValue],
         binds: &mut Vec<Bind>,
         index: &mut usize,
         condition: &str|
         -> String {
            let mut placeholders = Vec::with_capacity(values.len());
            for value in values {
                *index += 1;
                placeholders.push(format!("${index}"));
                match value {
                    SavedSearchCursorValue::Id(id) => binds.push(Bind::I32(*id)),
                    SavedSearchCursorValue::Description(text) => {
                        binds.push(Bind::Text(text.clone()))
                    }
                    SavedSearchCursorValue::UpdatedAt(nanos) => {
                        binds.push(Bind::Timestamp(
                            OffsetDateTime::from_unix_timestamp_nanos(*nanos as i128)
                                .expect("valid cursor timestamp"),
                        ))
                    }
                }
            }
            format!("({column}) {condition} ({})", placeholders.join(","))
        };

    if let Some(values) = after {
        if !values.is_empty() {
            let condition = if ascending { ">" } else { "<" };
            let clause = push_cursor(values, &mut binds, &mut index, condition);
            conditions.push(clause);
        }
    }
    if let Some(values) = before {
        if !values.is_empty() {
            let condition = if ascending { "<" } else { ">" };
            let clause = push_cursor(values, &mut binds, &mut index, condition);
            conditions.push(clause);
        }
    }

    let mut sql = format!(
        "SELECT id, {SAVED_SEARCH_COLUMNS} FROM saved_searches WHERE ({})",
        conditions.join(") AND (")
    );
    let order_ascending = if first.is_some() {
        ascending
    } else {
        !ascending
    };
    sql.push_str(&format!(
        " ORDER BY {column} {}",
        if order_ascending { "ASC" } else { "DESC" }
    ));
    if let Some(limit) = first.or(last) {
        index += 1;
        sql.push_str(&format!(" LIMIT ${index}"));
        binds.push(Bind::I64(limit));
    }
    Ok(ListSql { sql, binds })
}

impl SavedSearchesGraphqlDatabase for PgSavedSearchesGraphqlStore {
    fn get_saved_search(&self, id: i32) -> Result<Option<SavedSearchRow>, String> {
        let sql = format!(
            "SELECT id, {SAVED_SEARCH_COLUMNS} FROM saved_searches WHERE id=$1"
        );
        let row = block_on(
            sqlx::query(&sql)
                .bind(id)
                .fetch_optional(&self.pool),
        )
        .map_err(error)?;
        row.as_ref()
            .map(scan_saved_search)
            .transpose()
            .map_err(error)
    }

    fn list_saved_searches(
        &self,
        args: &SavedSearchListArgs,
        order: SavedSearchesOrderBy,
        after: Option<&Vec<SavedSearchCursorValue>>,
        before: Option<&Vec<SavedSearchCursorValue>>,
        first: Option<i64>,
        last: Option<i64>,
    ) -> Result<Vec<SavedSearchRow>, String> {
        let built = list_sql(
            args,
            order,
            after.map(Vec::as_slice),
            before.map(Vec::as_slice),
            first,
            last,
        )?;
        let mut query = sqlx::query(&built.sql);
        for bind in &built.binds {
            query = bind.apply(query);
        }
        let rows = block_on(query.fetch_all(&self.pool)).map_err(error)?;
        rows.iter()
            .map(scan_saved_search)
            .collect::<Result<_, _>>()
            .map_err(error)
    }

    fn count_saved_searches(&self, args: &SavedSearchListArgs) -> Result<i32, String> {
        // `Count` ignores pagination entirely.
        let filter = filter_sql(args)?;
        let count_sql = format!(
            "SELECT COUNT(*) FROM saved_searches WHERE ({})",
            filter.where_sql
        );
        let mut query = sqlx::query_scalar::<_, i64>(&count_sql);
        for bind in &filter.binds {
            query = bind.apply_scalar(query);
        }
        let count = block_on(query.fetch_one(&self.pool)).map_err(error)?;
        Ok(count as i32)
    }

    fn create_saved_search(
        &self,
        description: &str,
        query: &str,
        draft: bool,
        owner: NamespaceIds,
        visibility_secret: bool,
        actor_uid: i32,
    ) -> Result<SavedSearchRow, String> {
        let sql = format!(
            "INSERT INTO saved_searches(description, query, draft, user_id, org_id, \
             visibility_secret, created_by, created_at, updated_by, updated_at) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, DEFAULT, $8, DEFAULT) \
             RETURNING id, {SAVED_SEARCH_COLUMNS}"
        );
        let row = block_on(
            sqlx::query(&sql)
                .bind(description)
                .bind(query)
                .bind(draft)
                .bind(owner.user)
                .bind(owner.org)
                .bind(visibility_secret)
                .bind(actor_uid)
                .bind(actor_uid)
                .fetch_one(&self.pool),
        )
        .map_err(error)?;
        scan_saved_search(&row).map_err(error)
    }

    fn update_saved_search(
        &self,
        id: i32,
        description: &str,
        query: &str,
        draft: bool,
        actor_uid: i32,
    ) -> Result<SavedSearchRow, String> {
        let sql = format!(
            "UPDATE saved_searches SET description=$1, query=$2, draft=$3, \
             updated_at=now(), updated_by=$4 WHERE id=$5 \
             RETURNING id, {SAVED_SEARCH_COLUMNS}"
        );
        let row = block_on(
            sqlx::query(&sql)
                .bind(description)
                .bind(query)
                .bind(draft)
                .bind(actor_uid)
                .bind(id)
                .fetch_optional(&self.pool),
        )
        .map_err(error)?;
        match row {
            Some(row) => scan_saved_search(&row).map_err(error),
            None => Err("saved search not found".to_owned()),
        }
    }

    fn update_saved_search_owner(
        &self,
        id: i32,
        owner: NamespaceIds,
        actor_uid: i32,
    ) -> Result<SavedSearchRow, String> {
        let sql = format!(
            "UPDATE saved_searches SET user_id=$1, org_id=$2, updated_at=now(), \
             updated_by=$3 WHERE id=$4 RETURNING id, {SAVED_SEARCH_COLUMNS}"
        );
        let row = block_on(
            sqlx::query(&sql)
                .bind(owner.user)
                .bind(owner.org)
                .bind(actor_uid)
                .bind(id)
                .fetch_optional(&self.pool),
        )
        .map_err(error)?;
        match row {
            Some(row) => scan_saved_search(&row).map_err(error),
            None => Err("saved search not found".to_owned()),
        }
    }

    fn update_saved_search_visibility(
        &self,
        id: i32,
        visibility_secret: bool,
        actor_uid: i32,
    ) -> Result<SavedSearchRow, String> {
        let sql = format!(
            "UPDATE saved_searches SET visibility_secret=$1, updated_at=now(), \
             updated_by=$2 WHERE id=$3 RETURNING id, {SAVED_SEARCH_COLUMNS}"
        );
        let row = block_on(
            sqlx::query(&sql)
                .bind(visibility_secret)
                .bind(actor_uid)
                .bind(id)
                .fetch_optional(&self.pool),
        )
        .map_err(error)?;
        match row {
            Some(row) => scan_saved_search(&row).map_err(error),
            None => Err("saved search not found".to_owned()),
        }
    }

    fn delete_saved_search(&self, id: i32) -> Result<(), String> {
        let result = block_on(
            sqlx::query("DELETE FROM saved_searches WHERE id=$1")
                .bind(id)
                .execute(&self.pool),
        )
        .map_err(error)?;
        if result.rows_affected() == 0 {
            return Err("saved search not found".to_owned());
        }
        Ok(())
    }

    fn user_by_id(&self, id: i32) -> Result<Option<UserData>, String> {
        let record = block_on(sg_db::users::get_session_user(&self.pool, id))
            .map_err(error)?;
        Ok(record.map(crate::user_store::to_user_data))
    }

    fn org_by_id(&self, id: i32) -> Result<Option<OrgData>, String> {
        let record = block_on(sg_db::orgs::find_org_by_id(&self.pool, id))
            .map_err(error)?;
        Ok(record.map(|record| OrgData {
            id: record.id,
            name: record.name,
            display_name: record.display_name,
            created_at: record.created_at,
            viewer_can_administer: false,
            viewer_is_member: false,
        }))
    }

    fn current_user(&self, uid: i32) -> Result<Option<UserData>, String> {
        self.user_by_id(uid)
    }

    fn is_site_admin(&self, uid: i32) -> Result<bool, String> {
        let record = block_on(sg_db::users::get_session_user(&self.pool, uid))
            .map_err(error)?;
        Ok(record.map(|record| record.site_admin).unwrap_or(false))
    }

    fn is_org_member(&self, org_id: i32, user_id: i32) -> Result<bool, String> {
        let orgs = block_on(sg_db::orgs::org_members_for_user(
            &self.pool,
            user_id,
            &[org_id],
        ))
        .map_err(error)?;
        Ok(orgs.contains(&org_id))
    }
}
