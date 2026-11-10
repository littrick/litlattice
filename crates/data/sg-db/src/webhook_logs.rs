#![forbid(unsafe_code)]

//! `internal/database/webhook_logs.go`: the `webhook_logs` reads the
//! `Query.webhookLogs` connection needs (`Count`/`List`), plus `GetByID` for
//! completeness.
//!
//! The stored `request`/`response` values are bytea blobs whose `encryption_key_id`
//! selects the keyring entry (`keyring.Default().WebhookLogKey`); this module
//! returns them verbatim and the GraphQL layer decrypts with `sg-encryption`
//! (`internal/encryption/json_encryptable.go`).
//!
//! Filtering and ordering mirror the baseline exactly:
//! - `external_service_id` / `webhook_id`: `None` = no predicate; `0` =
//!   `IS NULL`; non-zero = equality;
//! - `only_errors`: `status_code NOT BETWEEN 100 AND 399`;
//! - `since`/`until`: inclusive `received_at` bounds;
//! - the list cursor is `id <= cursor` (0 = no cursor);
//! - `ORDER BY id DESC`, `LIMIT limit+1` when `limit != 0`; the extra row is
//!   dropped and its id becomes the next-page cursor.

use sqlx::postgres::{PgPool, PgRow};
use sqlx::types::time::OffsetDateTime;
use sqlx::{QueryBuilder, Row};

/// `database.WebhookLogListOpts` (`internal/database/webhook_logs.go:82-113`).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct WebhookLogListOpts {
    /// The maximum number of entries to return. `0` means no limit, like the
    /// baseline's `LIMIT` omission.
    pub limit: i64,
    /// `id <= cursor` when non-zero.
    pub cursor: i64,
    pub external_service_id: Option<i64>,
    pub webhook_id: Option<i32>,
    pub only_errors: bool,
    pub since: Option<OffsetDateTime>,
    pub until: Option<OffsetDateTime>,
}

/// One `webhook_logs` row (`database.scanWebhookLog`,
/// `internal/database/webhook_logs.go:297-320`). `request`/`response` stay
/// encrypted (or plaintext with an empty key id).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WebhookLogRecord {
    pub id: i64,
    pub received_at: OffsetDateTime,
    pub external_service_id: Option<i64>,
    pub webhook_id: Option<i32>,
    pub status_code: i32,
    pub request: Vec<u8>,
    pub response: Vec<u8>,
    pub encryption_key_id: String,
}

/// `WebhookLogListOpts.predicates` (`internal/database/webhook_logs.go:115-139`).
fn push_predicates(builder: &mut QueryBuilder<'_, sqlx::Postgres>, opts: &WebhookLogListOpts) {
    builder.push("TRUE");
    if let Some(id) = opts.external_service_id {
        if id == 0 {
            builder.push(" AND external_service_id IS NULL");
        } else {
            builder.push(" AND external_service_id = ").push_bind(id);
        }
    }
    if let Some(id) = opts.webhook_id {
        if id == 0 {
            builder.push(" AND webhook_id IS NULL");
        } else {
            builder.push(" AND webhook_id = ").push_bind(id);
        }
    }
    if opts.only_errors {
        builder.push(" AND status_code NOT BETWEEN 100 AND 399");
    }
    if let Some(since) = opts.since {
        builder.push(" AND received_at >= ").push_bind(since);
    }
    if let Some(until) = opts.until {
        builder.push(" AND received_at <= ").push_bind(until);
    }
}

fn record_from_row(row: &PgRow) -> Result<WebhookLogRecord, sqlx::Error> {
    Ok(WebhookLogRecord {
        id: row.try_get(0)?,
        received_at: row.try_get(1)?,
        external_service_id: row.try_get(2)?,
        webhook_id: row.try_get(3)?,
        status_code: row.try_get(4)?,
        request: row.try_get(5)?,
        response: row.try_get(6)?,
        encryption_key_id: row.try_get(7)?,
    })
}

const COLUMNS: &str = "id, received_at, external_service_id::bigint, webhook_id, \
                       status_code, request, response, encryption_key_id";

/// `webhookLogStore.Count` (`internal/database/webhook_logs.go:141-154`): the
/// cursor and limit are ignored.
pub async fn count(pool: &PgPool, opts: &WebhookLogListOpts) -> Result<i64, sqlx::Error> {
    let mut builder: QueryBuilder<'_, sqlx::Postgres> =
        QueryBuilder::new("SELECT COUNT(id) FROM webhook_logs WHERE ");
    push_predicates(&mut builder, opts);
    builder.build().fetch_one(pool).await?.try_get(0)
}

/// `webhookLogStore.List` (`internal/database/webhook_logs.go:156-192`).
/// Returns `(rows, next)` where `next` is the id of the truncated extra row
/// when the result was over-fetched, else `0`.
pub async fn list(
    pool: &PgPool,
    opts: &WebhookLogListOpts,
) -> Result<(Vec<WebhookLogRecord>, i64), sqlx::Error> {
    let mut builder: QueryBuilder<'_, sqlx::Postgres> = QueryBuilder::new("SELECT ");
    builder.push(COLUMNS).push(" FROM webhook_logs WHERE ");
    push_predicates(&mut builder, opts);
    if opts.cursor != 0 {
        builder.push(" AND id <= ").push_bind(opts.cursor);
    }
    builder.push(" ORDER BY id DESC");
    if opts.limit != 0 {
        builder
            .push(" LIMIT ")
            .push_bind(opts.limit.saturating_add(1));
    }

    let rows = builder.build().fetch_all(pool).await?;
    let mut records = Vec::with_capacity(rows.len());
    for row in &rows {
        records.push(record_from_row(row)?);
    }

    let mut next = 0;
    if opts.limit != 0 && records.len() as i64 == opts.limit.saturating_add(1) {
        if let Some(last) = records.pop() {
            next = last.id;
        }
    }
    Ok((records, next))
}

/// `webhookLogStore.GetByID` (`internal/database/webhook_logs.go:70-84`). A
/// missing row is `sqlx::Error::RowNotFound`, matching `sql.ErrNoRows`.
pub async fn get_by_id(pool: &PgPool, id: i64) -> Result<WebhookLogRecord, sqlx::Error> {
    let mut builder: QueryBuilder<'_, sqlx::Postgres> = QueryBuilder::new("SELECT ");
    builder
        .push(COLUMNS)
        .push(" FROM webhook_logs WHERE id = ")
        .push_bind(id);
    let rows = builder.build().fetch_all(pool).await?;
    match rows.first() {
        Some(row) => record_from_row(row),
        None => Err(sqlx::Error::RowNotFound),
    }
}

/// The store facade used by the frontend.
#[derive(Clone)]
pub struct WebhookLogStore {
    pool: PgPool,
}

impl WebhookLogStore {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    pub async fn list(
        &self,
        opts: &WebhookLogListOpts,
    ) -> Result<(Vec<WebhookLogRecord>, i64), sqlx::Error> {
        list(&self.pool, opts).await
    }

    pub async fn count(&self, opts: &WebhookLogListOpts) -> Result<i64, sqlx::Error> {
        count(&self.pool, opts).await
    }

    pub async fn get_by_id(&self, id: i64) -> Result<WebhookLogRecord, sqlx::Error> {
        get_by_id(&self.pool, id).await
    }
}
