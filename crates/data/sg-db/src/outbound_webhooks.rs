//! `internal/database/outbound_webhooks.go`: the `outbound_webhooks` /
//! `outbound_webhook_event_types` store used by the frontend GraphQL
//! resolvers (M9.27b).
//!
//! Ported paths:
//!
//! * `Count` — `SELECT COUNT(*) FROM outbound_webhooks_with_event_types`
//!   with the event-type subquery filter.
//! * `List` — the same view, `ORDER BY id ASC`, `LIMIT`/`OFFSET`.
//! * `GetByID` — the same view, one row.
//! * `Create` / `Update` — the CTE inserts/deletes that keep the
//!   `event_types` json column recomputed in the mutating statement (the view
//!   only sees committed rows).
//! * `Delete` — `DELETE FROM outbound_webhooks WHERE id = $1` (a missing row
//!   is not an error).
//!
//! The SQL is the baseline's (including `event_types` as
//! `array_to_json(ARRAY(SELECT json_build_object(...)))`) so row ordering and
//! the scanned columns match byte-for-byte. Values stay raw (`url`/`secret`
//! are `bytea`); decryption is the caller's job, exactly like the Go store,
//! whose keyring `OutboundWebhookKey` is always nil.
//!
//! No table, column or index is added.

use sqlx::postgres::{PgPool, PgRow};
use sqlx::Row;
use time::OffsetDateTime;

/// `types.OutboundWebhookEventType`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutboundWebhookEventType {
    pub id: i64,
    pub outbound_webhook_id: i64,
    pub event_type: String,
    pub scope: Option<String>,
}

/// `types.OutboundWebhook` with raw `url`/`secret` bytes (the Go store hands
/// out `*encryption.Encryptable`, which decrypts lazily).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutboundWebhook {
    pub id: i64,
    pub created_by: i32,
    pub created_at: OffsetDateTime,
    pub updated_by: i32,
    pub updated_at: OffsetDateTime,
    /// Empty when the column is NULL (`dbutil.NullString` scan).
    pub encryption_key_id: String,
    pub url: Vec<u8>,
    pub secret: Vec<u8>,
    pub event_types: Vec<OutboundWebhookEventType>,
}

/// `database.FilterEventType`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FilterEventType {
    pub event_type: String,
    /// `None` matches any scope; `FilterEventTypeNoScope` matches NULL.
    pub scope: Option<String>,
}

/// `database.FilterEventTypeNoScope`.
pub const FILTER_EVENT_TYPE_NO_SCOPE: &str = "RESERVED_KEYWORD_MATCH_NULL_SCOPE";

/// `database.OutboundWebhookCountOpts`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct OutboundWebhookCountOpts {
    pub event_types: Vec<FilterEventType>,
}

/// `database.OutboundWebhookListOpts`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct OutboundWebhookListOpts {
    pub count: OutboundWebhookCountOpts,
    pub limit: i64,
    pub offset: i64,
}

/// `(event_type, scope)` row for the `VALUES` CTE.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScopedEventType {
    pub event_type: String,
    pub scope: Option<String>,
}

/// Build the `WHERE` clause used by `Count` and `List`
/// (`OutboundWebhookCountOpts.where`). Returns the SQL fragment and the
/// positional binds, whose indices start at `first_bind`.
fn where_clause(opts: &OutboundWebhookCountOpts, first_bind: usize) -> (String, Vec<Option<String>>) {
    if opts.event_types.is_empty() {
        return ("TRUE".to_owned(), Vec::new());
    }

    let mut preds = Vec::new();
    let mut binds = Vec::new();
    let mut next = first_bind;
    for filter in &opts.event_types {
        let mut pred = format!("(event_type = ${next})");
        binds.push(Some(filter.event_type.clone()));
        next += 1;
        match filter.scope.as_deref() {
            None => {}
            Some(FILTER_EVENT_TYPE_NO_SCOPE) => pred.push_str(" AND scope IS NULL"),
            Some(scope) => {
                pred.push_str(&format!(" AND scope = ${next}"));
                binds.push(Some(scope.to_owned()));
                next += 1;
            }
        }
        preds.push(pred);
    }

    (
        format!(
            "id IN (SELECT outbound_webhook_id FROM outbound_webhook_event_types WHERE {})",
            preds.join(" OR ")
        ),
        binds,
    )
}

const SELECT_COLUMNS: &str = "id, created_by, created_at, updated_by, updated_at, \
     encryption_key_id, url, secret, event_types";

/// The `RETURNING *` CTE in `Create`/`Update` has no `event_types` column (that
/// is recomputed separately), so the mutators cannot reuse
/// [`SELECT_COLUMNS`].
const MUTATION_COLUMNS: &str = "id, created_by, created_at, updated_by, updated_at, \
     encryption_key_id, url, secret";

fn scan_event_types(raw: &serde_json::Value) -> Result<Vec<OutboundWebhookEventType>, String> {
    let array = raw.as_array().ok_or_else(|| {
        format!("unmarshalling event types: expected a JSON array, got {raw}")
    })?;
    let mut event_types = Vec::with_capacity(array.len());
    for value in array {
        let get_i64 = |name: &str| -> Result<i64, String> {
            value
                .get(name)
                .and_then(|value| value.as_i64())
                .ok_or_else(|| format!("unmarshalling event types: missing {name}"))
        };
        let event_type = value
            .get("event_type")
            .and_then(|value| value.as_str())
            .ok_or_else(|| "unmarshalling event types: missing event_type".to_owned())?;
        let scope = match value.get("scope") {
            None | Some(serde_json::Value::Null) => None,
            Some(serde_json::Value::String(scope)) => Some(scope.clone()),
            Some(other) => {
                return Err(format!(
                    "unmarshalling event types: invalid scope {other}"
                ))
            }
        };
        event_types.push(OutboundWebhookEventType {
            id: get_i64("id")?,
            outbound_webhook_id: get_i64("outbound_webhook_id")?,
            event_type: event_type.to_owned(),
            scope,
        });
    }
    Ok(event_types)
}

fn scan_webhook(row: &PgRow) -> Result<OutboundWebhook, String> {
    let raw_event_types: serde_json::Value = row
        .try_get("event_types")
        .map_err(|err| format!("unmarshalling event types: {err}"))?;
    Ok(OutboundWebhook {
        id: row.try_get("id").map_err(|err| err.to_string())?,
        created_by: row
            .try_get::<Option<i32>, _>("created_by")
            .map_err(|err| err.to_string())?
            .unwrap_or(0),
        created_at: row
            .try_get::<OffsetDateTime, _>("created_at")
            .map_err(|err| err.to_string())?,
        updated_by: row
            .try_get::<Option<i32>, _>("updated_by")
            .map_err(|err| err.to_string())?
            .unwrap_or(0),
        updated_at: row
            .try_get::<OffsetDateTime, _>("updated_at")
            .map_err(|err| err.to_string())?,
        encryption_key_id: row
            .try_get::<Option<String>, _>("encryption_key_id")
            .map_err(|err| err.to_string())?
            .unwrap_or_default(),
        url: row.try_get("url").map_err(|err| err.to_string())?,
        secret: row.try_get("secret").map_err(|err| err.to_string())?,
        event_types: scan_event_types(&raw_event_types)?,
    })
}

/// `OutboundWebhookStore.Count`.
pub async fn count(
    pool: &PgPool,
    opts: &OutboundWebhookCountOpts,
) -> Result<i64, String> {
    let (where_sql, binds) = where_clause(opts, 1);
    let sql = format!(
        "SELECT COUNT(*) FROM outbound_webhooks_with_event_types WHERE {where_sql}"
    );
    let mut query = sqlx::query_scalar::<_, i64>(&sql);
    for bind in binds {
        query = query.bind(bind);
    }
    query.fetch_one(pool).await.map_err(|err| err.to_string())
}

/// `OutboundWebhookStore.List`.
pub async fn list(
    pool: &PgPool,
    opts: &OutboundWebhookListOpts,
) -> Result<Vec<OutboundWebhook>, String> {
    let (where_sql, binds) = where_clause(&opts.count, 1);
    let sql = format!(
        "SELECT {SELECT_COLUMNS} FROM outbound_webhooks_with_event_types \
         WHERE {where_sql} ORDER BY id ASC LIMIT {} OFFSET {}",
        opts.limit, opts.offset
    );
    let mut query = sqlx::query(&sql);
    for bind in binds {
        query = query.bind(bind);
    }
    let rows = query
        .fetch_all(pool)
        .await
        .map_err(|err| err.to_string())?;
    rows.iter().map(scan_webhook).collect()
}

/// `OutboundWebhookStore.GetByID`: `Ok(None)` is `OutboundWebhookNotFoundErr`.
pub async fn get_by_id(
    pool: &PgPool,
    id: i64,
) -> Result<Option<OutboundWebhook>, String> {
    let sql = format!(
        "SELECT {SELECT_COLUMNS} FROM outbound_webhooks_with_event_types WHERE id = $1"
    );
    let row = sqlx::query(&sql)
        .bind(id)
        .fetch_optional(pool)
        .await
        .map_err(|err| err.to_string())?;
    row.as_ref().map(scan_webhook).transpose()
}

/// `eventTypesToInsertableRows`: returns the placeholder list and appends the
/// event-type binds starting at `first_bind`.
fn event_type_values(
    event_types: &[ScopedEventType],
    first_bind: usize,
) -> Result<(String, Vec<Option<String>>), String> {
    if event_types.is_empty() {
        return Err("an outbound webhook must have at least one event type".to_owned());
    }
    let mut placeholders = Vec::with_capacity(event_types.len());
    let mut binds = Vec::with_capacity(event_types.len() * 2);
    let mut next = first_bind;
    for event_type in event_types {
        placeholders.push(format!("(${next}::text, ${}::text)", next + 1));
        binds.push(Some(event_type.event_type.clone()));
        binds.push(event_type.scope.clone());
        next += 2;
    }
    Ok((placeholders.join(","), binds))
}

const EVENT_TYPES_COLUMN: &str = "array_to_json(array(SELECT json_build_object(\
        'id', id, 'outbound_webhook_id', outbound_webhook_id, \
        'event_type', event_type, 'scope', scope) FROM event_types)) AS event_types";

/// `OutboundWebhookStore.Create`.
pub async fn create(
    pool: &PgPool,
    created_by: i32,
    updated_by: i32,
    encryption_key_id: Option<&str>,
    url: &[u8],
    secret: &[u8],
    event_types: &[ScopedEventType],
) -> Result<OutboundWebhook, String> {
    let (values, event_binds) = event_type_values(event_types, 6)?;
    let sql = format!(
        "WITH outbound_webhook AS (\
            INSERT INTO outbound_webhooks \
                (created_by, updated_by, encryption_key_id, url, secret) \
            VALUES ($1, $2, $3, $4, $5) RETURNING *\
         ), data (event_type, scope) AS (VALUES {values}), event_types AS (\
            INSERT INTO outbound_webhook_event_types \
                (outbound_webhook_id, event_type, scope) \
            SELECT outbound_webhook.id, data.event_type, data.scope \
            FROM outbound_webhook CROSS JOIN data RETURNING *\
         ) \
         SELECT {MUTATION_COLUMNS}, {EVENT_TYPES_COLUMN} FROM outbound_webhook"
    );
    let mut query = sqlx::query(&sql)
        .bind(created_by)
        .bind(updated_by)
        .bind(encryption_key_id)
        .bind(url)
        .bind(secret);
    for bind in event_binds {
        query = query.bind(bind);
    }
    let row = query
        .fetch_one(pool)
        .await
        .map_err(|err| format!("scanning outbound webhook: {err}"))?;
    scan_webhook(&row).map_err(|err| format!("scanning outbound webhook: {err}"))
}

/// The row `OutboundWebhookStore.Update` needs from `GetByID` (the raw secret
/// is re-written unchanged, and a non-empty key id makes `encryptFields` fail
/// because the new `url` encrypts under the nil key).
pub async fn secret_for_update(pool: &PgPool, id: i64) -> Result<Option<(Vec<u8>, String)>, String> {
    let row = sqlx::query("SELECT secret, encryption_key_id FROM outbound_webhooks WHERE id = $1")
        .bind(id)
        .fetch_optional(pool)
        .await
        .map_err(|err| err.to_string())?;
    row.map(|row| {
        Ok((
            row.try_get::<Vec<u8>, _>("secret")
                .map_err(|err| err.to_string())?,
            row.try_get::<Option<String>, _>("encryption_key_id")
                .map_err(|err| err.to_string())?
                .unwrap_or_default(),
        ))
    })
    .transpose()
}

/// `OutboundWebhookStore.Update`: caller already resolved the secret/key id
/// (see [`secret_for_update`]). `Ok(None)` if the row vanished.
pub async fn update(
    pool: &PgPool,
    id: i64,
    updated_by: i32,
    url: &[u8],
    secret: &[u8],
    encryption_key_id: Option<&str>,
    event_types: &[ScopedEventType],
) -> Result<Option<OutboundWebhook>, String> {
    let (values, event_binds) = event_type_values(event_types, 6)?;
    let sql = format!(
        "WITH outbound_webhook AS (\
            UPDATE outbound_webhooks SET \
                updated_at = NOW(), updated_by = $1, encryption_key_id = $2, \
                url = $3, secret = $4 \
            WHERE id = $5 RETURNING *\
         ), delete_all_event_types AS (\
            DELETE FROM outbound_webhook_event_types \
            WHERE outbound_webhook_id IN (SELECT id FROM outbound_webhook)\
         ), data (event_type, scope) AS (VALUES {values}), event_types AS (\
            INSERT INTO outbound_webhook_event_types \
                (outbound_webhook_id, event_type, scope) \
            SELECT outbound_webhook.id, data.event_type, data.scope \
            FROM outbound_webhook CROSS JOIN data RETURNING *\
         ) \
         SELECT {MUTATION_COLUMNS}, {EVENT_TYPES_COLUMN} FROM outbound_webhook"
    );
    let mut query = sqlx::query(&sql)
        .bind(updated_by)
        .bind(encryption_key_id)
        .bind(url)
        .bind(secret)
        .bind(id);
    for bind in event_binds {
        query = query.bind(bind);
    }
    let row = query
        .fetch_optional(pool)
        .await
        .map_err(|err| format!("scanning outbound webhook: {err}"))?;
    row.as_ref()
        .map(scan_webhook)
        .transpose()
        .map_err(|err| format!("scanning outbound webhook: {err}"))
}

/// `OutboundWebhookStore.Delete`: a missing row is not an error.
pub async fn delete(pool: &PgPool, id: i64) -> Result<(), String> {
    sqlx::query("DELETE FROM outbound_webhooks WHERE id = $1")
        .bind(id)
        .execute(pool)
        .await
        .map_err(|err| err.to_string())?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Outbound webhook logs and jobs (`internal/database/outbound_webhook_logs.go`,
// `outbound_webhook_jobs.go`)
// ---------------------------------------------------------------------------

/// `types.OutboundWebhookLog` with the encrypted columns kept raw; decryption
/// is the caller's job (`EncryptableWebhookLogMessage`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutboundWebhookLog {
    pub id: i64,
    pub job_id: i64,
    pub outbound_webhook_id: i64,
    pub sent_at: OffsetDateTime,
    pub status_code: i32,
    /// Empty when the column is NULL (`dbutil.NullString` scan).
    pub encryption_key_id: String,
    pub request: Vec<u8>,
    pub response: Vec<u8>,
    pub error: Vec<u8>,
}

/// `database.OutboundWebhookLogListOpts`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct OutboundWebhookLogListOpts {
    pub limit: i64,
    pub offset: i64,
    pub only_errors: bool,
    pub outbound_webhook_id: i64,
}

/// `types.OutboundWebhookJob` (only the fields the GraphQL resolver exposes).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutboundWebhookJob {
    pub id: i64,
    pub event_type: String,
    pub scope: Option<String>,
    /// Empty when the column is NULL (`dbutil.NullString` scan).
    pub encryption_key_id: String,
    pub payload: Vec<u8>,
}

const OUTBOUND_WEBHOOK_LOG_COLUMNS: &str = "id, job_id, outbound_webhook_id, \
     sent_at, status_code, encryption_key_id, request, response, error";

fn scan_log(row: &PgRow) -> Result<OutboundWebhookLog, String> {
    Ok(OutboundWebhookLog {
        id: row.try_get("id").map_err(|err| err.to_string())?,
        job_id: row.try_get("job_id").map_err(|err| err.to_string())?,
        outbound_webhook_id: row
            .try_get("outbound_webhook_id")
            .map_err(|err| err.to_string())?,
        sent_at: row
            .try_get("sent_at")
            .map_err(|err| err.to_string())?,
        status_code: row
            .try_get("status_code")
            .map_err(|err| err.to_string())?,
        encryption_key_id: row
            .try_get::<Option<String>, _>("encryption_key_id")
            .map_err(|err| err.to_string())?
            .unwrap_or_default(),
        request: row.try_get("request").map_err(|err| err.to_string())?,
        response: row.try_get("response").map_err(|err| err.to_string())?,
        error: row.try_get("error").map_err(|err| err.to_string())?,
    })
}

/// `OutboundWebhookLogStore.ListForOutboundWebhook`: newest `id` first,
/// `limit`/`offset` from `LimitOffset.SQL()`, and the baseline `where()`
/// predicate (`status_code NOT BETWEEN 100 AND 399` when `OnlyErrors`).
pub async fn list_logs(
    pool: &PgPool,
    opts: &OutboundWebhookLogListOpts,
) -> Result<Vec<OutboundWebhookLog>, String> {
    let mut where_sql = "outbound_webhook_id = $1".to_owned();
    if opts.only_errors {
        where_sql.push_str(" AND status_code NOT BETWEEN 100 AND 399");
    }
    let sql = format!(
        "SELECT {OUTBOUND_WEBHOOK_LOG_COLUMNS} FROM outbound_webhook_logs \
         WHERE {where_sql} ORDER BY id DESC LIMIT {} OFFSET {}",
        opts.limit, opts.offset
    );
    let rows = sqlx::query(&sql)
        .bind(opts.outbound_webhook_id)
        .fetch_all(pool)
        .await
        .map_err(|err| err.to_string())?;
    rows.iter().map(scan_log).collect()
}

/// `OutboundWebhookLogStore.CountsForOutboundWebhook`: `(total, errored)`.
pub async fn counts_for_outbound_webhook(
    pool: &PgPool,
    outbound_webhook_id: i64,
) -> Result<(i64, i64), String> {
    let row = sqlx::query(
        "SELECT \
            COUNT(*) AS total, \
            COUNT(*) FILTER (WHERE status_code NOT BETWEEN 100 AND 399) AS errored \
         FROM outbound_webhook_logs WHERE outbound_webhook_id = $1",
    )
    .bind(outbound_webhook_id)
    .fetch_one(pool)
    .await
    .map_err(|err| err.to_string())?;
    Ok((
        row.try_get("total").map_err(|err| err.to_string())?,
        row.try_get("errored").map_err(|err| err.to_string())?,
    ))
}

/// `OutboundWebhookJobStore.GetByID`: `None` is
/// `OutboundWebhookJobNotFoundErr`.
pub async fn get_job(
    pool: &PgPool,
    id: i64,
) -> Result<Option<OutboundWebhookJob>, String> {
    let row = sqlx::query(
        "SELECT id, event_type, scope, encryption_key_id, payload \
         FROM outbound_webhook_jobs WHERE id = $1",
    )
    .bind(id)
    .fetch_optional(pool)
    .await
    .map_err(|err| err.to_string())?;
    row.map(|row| {
        Ok(OutboundWebhookJob {
            id: row.try_get("id").map_err(|err| err.to_string())?,
            event_type: row
                .try_get("event_type")
                .map_err(|err| err.to_string())?,
            scope: row
                .try_get::<Option<String>, _>("scope")
                .map_err(|err| err.to_string())?,
            encryption_key_id: row
                .try_get::<Option<String>, _>("encryption_key_id")
                .map_err(|err| err.to_string())?
                .unwrap_or_default(),
            payload: row
                .try_get("payload")
                .map_err(|err| err.to_string())?,
        })
    })
    .transpose()
}
