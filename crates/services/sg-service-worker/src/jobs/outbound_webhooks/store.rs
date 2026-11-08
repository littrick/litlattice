#![forbid(unsafe_code)]

//! Database access for the `outbound-webhook-sender` job.
//!
//! Ports the parts of `internal/workerutil/dbworker/store` and
//! `internal/database/outbound_webhook*.go` the sender needs:
//!
//! * [`WebhookJobQueue`] — the `outbound_webhook_jobs` queue configured like
//!   `cmd/worker/internal/outboundwebhooks/store.go` (`OrderByExpression: id`,
//!   `MaxNumResets: 5`, `StalledMaxAge: 10s`, no `ViewName` and no
//!   `RetryAfter`), with `CountByState`, `Dequeue`, `Heartbeat`, `Requeue`,
//!   `MarkComplete`/`MarkErrored`/`MarkFailed`, `ResetStalled`, `Create` and
//!   `DeleteBefore`;
//! * [`WebhookStore`] — `OutboundWebhooks.List` against the
//!   `outbound_webhooks_with_event_types` view;
//! * [`WebhookLogStore`] — `OutboundWebhookLogs.Create`.
//!
//! The baseline ring never populates `OutboundWebhookKey` (see
//! `sg-keyring`), so the production sender holds `None` and every value stays
//! plaintext. The stores keep the key parameter so the encryption round-trip
//! is exercised when a key is supplied.

use std::sync::Arc;
use std::time::Duration;

use sg_encryption::{maybe_decrypt, maybe_encrypt, Key};
use sqlx::types::time::OffsetDateTime;
use sqlx::{PgPool, Row};

/// `workerutil/dbworker/store.RecordState` bitset.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecordState(pub u32);

impl RecordState {
    pub const QUEUED: Self = Self(1 << 0);
    pub const ERRORED: Self = Self(1 << 1);
    pub const PROCESSING: Self = Self(1 << 2);

    fn list(self) -> Vec<&'static str> {
        let mut states = Vec::new();
        if self.0 & Self::QUEUED.0 != 0 {
            states.push("queued");
        }
        if self.0 & Self::ERRORED.0 != 0 {
            states.push("errored");
        }
        if self.0 & Self::PROCESSING.0 != 0 {
            states.push("processing");
        }
        states
    }
}

impl std::ops::BitOr for RecordState {
    type Output = Self;

    fn bitor(self, rhs: Self) -> Self {
        Self(self.0 | rhs.0)
    }
}

/// `encryption.Encryptable` for a byte-valued column: either plaintext or
/// ciphertext plus the stored key identifier.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EncryptableValue {
    pub cipher: Vec<u8>,
    pub key_id: String,
}

impl EncryptableValue {
    /// `encryption.NewEncrypted(string(rawBytes), keyID, key)`.
    pub fn new(cipher: Vec<u8>, key_id: String) -> Self {
        Self { cipher, key_id }
    }

    /// `encryption.NewUnencrypted`.
    pub fn unencrypted(value: impl AsRef<[u8]>) -> Self {
        Self {
            cipher: value.as_ref().to_vec(),
            key_id: String::new(),
        }
    }

    /// `Encryptable.Decrypt` (memoisation is irrelevant for a single read).
    pub async fn decrypt(&self, key: Option<&dyn Key>) -> Result<String, String> {
        let cipher = String::from_utf8_lossy(&self.cipher);
        maybe_decrypt(key, &cipher, &self.key_id)
            .await
            .map_err(|err| err.to_string())
    }

    /// `Encryptable.Encrypt`: returns the stored ciphertext and key ID.
    pub async fn encrypt(
        plaintext: &str,
        key: Option<&dyn Key>,
    ) -> Result<(Vec<u8>, String), String> {
        let (cipher, key_id) = maybe_encrypt(key, plaintext)
            .await
            .map_err(|err| err.to_string())?;
        Ok((cipher.into_bytes(), key_id))
    }
}

/// `types.OutboundWebhookJob` (the columns the sender reads).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WebhookJob {
    pub id: i64,
    pub event_type: String,
    pub scope: Option<String>,
    pub payload: EncryptableValue,
    pub state: String,
    pub failure_message: Option<String>,
    pub queued_at: OffsetDateTime,
    pub started_at: Option<OffsetDateTime>,
    pub finished_at: Option<OffsetDateTime>,
    pub process_after: Option<OffsetDateTime>,
    pub num_resets: i32,
    pub num_failures: i32,
    pub last_heartbeat_at: Option<OffsetDateTime>,
    pub worker_hostname: String,
    pub cancel: bool,
}

impl WebhookJob {
    /// `types.OutboundWebhookJob.RecordID`.
    pub fn record_id(&self) -> i64 {
        self.id
    }
}

/// `types.OutboundWebhookEventType`.
#[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct WebhookEventType {
    pub id: i64,
    pub outbound_webhook_id: i64,
    pub event_type: String,
    pub scope: Option<String>,
}

/// `types.OutboundWebhook`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Webhook {
    pub id: i64,
    pub created_by: Option<i32>,
    pub created_at: OffsetDateTime,
    pub updated_by: Option<i32>,
    pub updated_at: OffsetDateTime,
    pub url: EncryptableValue,
    pub secret: EncryptableValue,
    pub event_types: Vec<WebhookEventType>,
}

/// `types.OutboundWebhookLog`; request/response/error start as plaintext JSON
/// and are encrypted by [`WebhookLogStore::create`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WebhookLog {
    pub id: i64,
    pub job_id: i64,
    pub outbound_webhook_id: i64,
    pub status_code: i32,
    pub request: String,
    pub response: String,
    pub error: String,
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

/// The columns selected by `database.OutboundWebhookJobColumns`, minus
/// `execution_logs` (the sender never reads it and the queued rows start with
/// NULL).
const JOB_COLUMNS: &str = "id, event_type, scope, encryption_key_id, payload, state, \
     failure_message, queued_at, started_at, finished_at, process_after, \
     num_resets, num_failures, last_heartbeat_at, worker_hostname, cancel";

/// `makeDequeueSelectExpressions`: a data-modifying CTE does not publish its
/// changes to the rest of the same statement, so `dbworker` substitutes the
/// literal values it just assigned instead of re-reading the row. Without this
/// the returned record would carry the pre-dequeue state.
const DEQUEUE_SELECT: &str = "id, event_type, scope, encryption_key_id, payload, \
     'processing'::text AS state, \
     NULL::text AS failure_message, \
     queued_at, $1::timestamptz AS started_at, \
     NULL::timestamptz AS finished_at, process_after, num_resets, num_failures, \
     $1::timestamptz AS last_heartbeat_at, $2::text AS worker_hostname, cancel";

/// `outbound_webhook_jobs` through `dbworker/store.Store`.
#[derive(Clone)]
pub struct WebhookJobQueue {
    pool: PgPool,
    key: Option<Arc<dyn Key>>,
}

impl WebhookJobQueue {
    pub fn new(pool: PgPool, key: Option<Arc<dyn Key>>) -> Self {
        Self { pool, key }
    }

    pub fn key(&self) -> Option<&dyn Key> {
        self.key.as_deref()
    }

    fn scan_job(&self, row: &sqlx::postgres::PgRow) -> Result<WebhookJob, String> {
        let key_id: Option<String> = row.try_get("encryption_key_id").map_err(stringify)?;
        Ok(WebhookJob {
            id: row.try_get("id").map_err(stringify)?,
            event_type: row.try_get("event_type").map_err(stringify)?,
            scope: row.try_get("scope").map_err(stringify)?,
            payload: EncryptableValue::new(
                row.try_get("payload").map_err(stringify)?,
                key_id.unwrap_or_default(),
            ),
            state: row.try_get("state").map_err(stringify)?,
            failure_message: row.try_get("failure_message").map_err(stringify)?,
            queued_at: row.try_get("queued_at").map_err(stringify)?,
            started_at: row.try_get("started_at").map_err(stringify)?,
            finished_at: row.try_get("finished_at").map_err(stringify)?,
            process_after: row.try_get("process_after").map_err(stringify)?,
            num_resets: row.try_get("num_resets").map_err(stringify)?,
            num_failures: row.try_get("num_failures").map_err(stringify)?,
            last_heartbeat_at: row.try_get("last_heartbeat_at").map_err(stringify)?,
            worker_hostname: row.try_get("worker_hostname").map_err(stringify)?,
            cancel: row.try_get("cancel").map_err(stringify)?,
        })
    }

    /// `Store.CountByState`: counts the records in `states`.
    pub async fn count_by_state(&self, states: RecordState) -> Result<i64, String> {
        let list = states.list();
        if list.is_empty() {
            return Err(
                "pre-condition failure: statesBitset should contain at least one state".to_owned(),
            );
        }
        let placeholders: Vec<String> = (1..=list.len()).map(|i| format!("${i}")).collect();
        let query = format!(
            "SELECT COUNT(*) FROM outbound_webhook_jobs WHERE state IN ({})",
            placeholders.join(", ")
        );
        let mut q = sqlx::query_scalar::<_, i64>(&query);
        for state in list {
            q = q.bind(state);
        }
        q.fetch_one(&self.pool).await.map_err(stringify)
    }

    /// `Store.Dequeue` with `RetryAfter: 0` (only queued rows are eligible)
    /// and no extra conditions.
    pub async fn dequeue(
        &self,
        worker_hostname: &str,
        now: OffsetDateTime,
    ) -> Result<Option<WebhookJob>, String> {
        let query = format!(
            r#"
WITH potential_candidates AS (
    SELECT
        id AS candidate_id,
        ROW_NUMBER() OVER (ORDER BY id) AS order
    FROM outbound_webhook_jobs
    WHERE
        state = 'queued' AND
        (process_after IS NULL OR process_after <= $1)
    ORDER BY id
    LIMIT 50
),
candidate AS (
    SELECT id FROM outbound_webhook_jobs
    JOIN potential_candidates pc ON pc.candidate_id = id
    WHERE state IN ('queued', 'errored')
    ORDER BY pc.order
    FOR UPDATE OF outbound_webhook_jobs SKIP LOCKED
    LIMIT 1
),
updated_record AS (
    UPDATE outbound_webhook_jobs
    SET
        state = 'processing',
        started_at = $1,
        last_heartbeat_at = $1,
        finished_at = NULL,
        failure_message = NULL,
        execution_logs = NULL,
        worker_hostname = $2
    WHERE id IN (SELECT id FROM candidate)
)
SELECT {DEQUEUE_SELECT}
FROM outbound_webhook_jobs
WHERE id IN (SELECT id FROM candidate)
"#
        );
        let rows = sqlx::query(&query)
            .bind(now)
            .bind(worker_hostname)
            .fetch_all(&self.pool)
            .await
            .map_err(stringify)?;
        match rows.len() {
            0 => Ok(None),
            1 => self.scan_job(&rows[0]).map(Some),
            count => Err(format!("more than one record dequeued: {count}")),
        }
    }

    /// `Store.Heartbeat`.
    pub async fn heartbeat(
        &self,
        ids: &[i64],
        worker_hostname: Option<&str>,
        now: OffsetDateTime,
    ) -> Result<(Vec<String>, Vec<String>), String> {
        if ids.is_empty() {
            return Ok((Vec::new(), Vec::new()));
        }

        let hostname_cond = if worker_hostname.is_some_and(|hostname| !hostname.is_empty()) {
            " AND worker_hostname = $3"
        } else {
            ""
        };
        let query = format!(
            r#"
WITH alive_candidates AS (
    SELECT id
    FROM outbound_webhook_jobs
    WHERE
        id = ANY($1) AND
        state = 'processing'{hostname_cond}
    ORDER BY id ASC
    FOR UPDATE
)
UPDATE outbound_webhook_jobs
SET last_heartbeat_at = $2
WHERE id IN (SELECT id FROM alive_candidates)
RETURNING id, cancel
"#
        );
        let mut q = sqlx::query(&query).bind(ids.to_vec()).bind(now);
        if let Some(hostname) = worker_hostname {
            if !hostname.is_empty() {
                q = q.bind(hostname);
            }
        }
        let rows = q.fetch_all(&self.pool).await.map_err(stringify)?;

        let mut known = Vec::new();
        let mut cancel = Vec::new();
        for row in rows {
            let id: i64 = row.try_get("id").map_err(stringify)?;
            let canceled: bool = row.try_get("cancel").map_err(stringify)?;
            known.push(id.to_string());
            if canceled {
                cancel.push(id.to_string());
            }
        }
        Ok((known, cancel))
    }

    /// `Store.Requeue`.
    pub async fn requeue(&self, id: i64, after: OffsetDateTime) -> Result<(), String> {
        sqlx::query(
            r#"
UPDATE outbound_webhook_jobs
SET
    state = 'queued',
    queued_at = clock_timestamp(),
    started_at = null,
    process_after = $1,
    cancel = false
WHERE id = $2
"#,
        )
        .bind(after)
        .bind(id)
        .execute(&self.pool)
        .await
        .map(|_| ())
        .map_err(stringify)
    }

    /// `Store.MarkComplete`.
    pub async fn mark_complete(
        &self,
        id: i64,
        worker_hostname: Option<&str>,
    ) -> Result<bool, String> {
        self.mark_final(
            r#"
UPDATE outbound_webhook_jobs
SET state = 'completed', finished_at = clock_timestamp()
WHERE id = $1 AND state = 'processing'{hostname_cond}
RETURNING id
"#,
            id,
            worker_hostname,
            None,
        )
        .await
    }

    /// `Store.MarkErrored` (with `MaxNumRetries = 0`, as configured by the
    /// sender's store).
    pub async fn mark_errored(
        &self,
        id: i64,
        failure_message: &str,
        worker_hostname: Option<&str>,
    ) -> Result<bool, String> {
        self.mark_final(
            r#"
UPDATE outbound_webhook_jobs
SET state = CASE WHEN cancel THEN 'canceled' WHEN num_failures + 1 >= 0 THEN 'failed' ELSE 'errored' END,
    finished_at = clock_timestamp(),
    failure_message = $2,
    num_failures = CASE WHEN cancel THEN num_failures ELSE num_failures + 1 END
WHERE id = $1 AND state = 'processing'{hostname_cond}
RETURNING id
"#,
            id,
            worker_hostname,
            Some(failure_message.to_owned()),
        )
        .await
    }

    /// `Store.MarkFailed`.
    pub async fn mark_failed(
        &self,
        id: i64,
        failure_message: &str,
        worker_hostname: Option<&str>,
    ) -> Result<bool, String> {
        self.mark_final(
            r#"
UPDATE outbound_webhook_jobs
SET state = CASE WHEN cancel THEN 'canceled' ELSE 'failed' END,
    finished_at = clock_timestamp(),
    failure_message = $2,
    num_failures = CASE WHEN cancel THEN num_failures ELSE num_failures + 1 END
WHERE id = $1 AND state = 'processing'{hostname_cond}
RETURNING id
"#,
            id,
            worker_hostname,
            Some(failure_message.to_owned()),
        )
        .await
    }

    async fn mark_final(
        &self,
        template: &str,
        id: i64,
        worker_hostname: Option<&str>,
        failure_message: Option<String>,
    ) -> Result<bool, String> {
        let has_hostname = worker_hostname.is_some_and(|hostname| !hostname.is_empty());
        let query = template.replace(
            "{hostname_cond}",
            if has_hostname {
                " AND worker_hostname = $3"
            } else {
                ""
            },
        );
        let mut q = sqlx::query(&query).bind(id);
        if let Some(message) = failure_message {
            q = q.bind(message);
        }
        if has_hostname {
            q = q.bind(worker_hostname.unwrap_or_default());
        }
        let rows = q.fetch_all(&self.pool).await.map_err(stringify)?;
        Ok(!rows.is_empty())
    }

    /// `Store.ResetStalled`: returns `(reset, failed)` as `(id, age)` pairs.
    pub async fn reset_stalled(
        &self,
        now: OffsetDateTime,
        stalled_max_age: Duration,
        max_num_resets: i32,
    ) -> Result<(Vec<(i64, Duration)>, Vec<(i64, Duration)>), String> {
        let stalled_secs = stalled_max_age.as_secs() as i32;
        let reset = sqlx::query(
            r#"
WITH stalled AS (
    SELECT id FROM outbound_webhook_jobs
    WHERE
        state = 'processing' AND
        $1 - last_heartbeat_at > ($2 * '1 second'::interval) AND
        num_resets < $3
    FOR UPDATE SKIP LOCKED
)
UPDATE outbound_webhook_jobs
SET
    state = 'queued',
    queued_at = clock_timestamp(),
    started_at = null,
    num_resets = num_resets + 1
WHERE id IN (SELECT id FROM stalled)
RETURNING id, last_heartbeat_at
"#,
        )
        .bind(now)
        .bind(stalled_secs)
        .bind(max_num_resets)
        .fetch_all(&self.pool)
        .await
        .map_err(stringify)?;

        let failed = sqlx::query(
            r#"
WITH stalled AS (
    SELECT id FROM outbound_webhook_jobs
    WHERE
        state = 'processing' AND
        $1 - last_heartbeat_at > ($2 * '1 second'::interval) AND
        num_resets >= $3
    FOR UPDATE SKIP LOCKED
)
UPDATE outbound_webhook_jobs
SET
    state = 'failed',
    finished_at = clock_timestamp(),
    failure_message = $4
WHERE id IN (SELECT id FROM stalled)
RETURNING id, last_heartbeat_at
"#,
        )
        .bind(now)
        .bind(stalled_secs)
        .bind(max_num_resets)
        .bind(DEFAULT_RESET_FAILURE_MESSAGE)
        .fetch_all(&self.pool)
        .await
        .map_err(stringify)?;

        Ok((scan_ages(reset, now)?, scan_ages(failed, now)?))
    }

    /// `OutboundWebhookJobStore.Create`.
    pub async fn create(
        &self,
        event_type: &str,
        scope: Option<&str>,
        payload: &[u8],
    ) -> Result<WebhookJob, String> {
        let (cipher, key_id) =
            EncryptableValue::encrypt(&String::from_utf8_lossy(payload), self.key())
                .await
                .map_err(|err| format!("encrypting payload: {err}"))?;
        let key_id = (!key_id.is_empty()).then_some(key_id);

        let query = format!(
            r#"
INSERT INTO outbound_webhook_jobs (event_type, scope, encryption_key_id, payload)
VALUES ($1, $2, $3, $4)
RETURNING {JOB_COLUMNS}
"#
        );
        let row = sqlx::query(&query)
            .bind(event_type)
            .bind(scope)
            .bind(key_id)
            .bind(cipher)
            .fetch_one(&self.pool)
            .await
            .map_err(stringify)?;
        self.scan_job(&row)
    }

    /// `OutboundWebhookJobStore.GetByID`.
    pub async fn get_by_id(&self, id: i64) -> Result<Option<WebhookJob>, String> {
        let query = format!("SELECT {JOB_COLUMNS} FROM outbound_webhook_jobs WHERE id = $1");
        let row = sqlx::query(&query)
            .bind(id)
            .fetch_optional(&self.pool)
            .await
            .map_err(stringify)?;
        row.as_ref().map(|row| self.scan_job(row)).transpose()
    }

    /// `OutboundWebhookJobStore.DeleteBefore`.
    pub async fn delete_before(&self, before: OffsetDateTime) -> Result<(), String> {
        sqlx::query("DELETE FROM outbound_webhook_jobs WHERE finished_at < $1")
            .bind(before)
            .execute(&self.pool)
            .await
            .map(|_| ())
            .map_err(stringify)
    }
}

/// `defaultResetFailureMessage`.
pub const DEFAULT_RESET_FAILURE_MESSAGE: &str =
    "job processor died while handling this message too many times";

fn scan_ages(
    rows: Vec<sqlx::postgres::PgRow>,
    now: OffsetDateTime,
) -> Result<Vec<(i64, Duration)>, String> {
    let mut ages = Vec::with_capacity(rows.len());
    for row in rows {
        let id: i64 = row.try_get("id").map_err(stringify)?;
        let last_heartbeat: OffsetDateTime = row.try_get("last_heartbeat_at").map_err(stringify)?;
        let age = now - last_heartbeat;
        ages.push((id, age.try_into().unwrap_or(Duration::ZERO)));
    }
    ages.sort_by_key(|(id, _)| *id);
    Ok(ages)
}

/// `database.OutboundWebhooks.List`.
///
/// The row values are returned as [`EncryptableValue`]s; decryption is the
/// caller's job (the sender holds the ring's `OutboundWebhookKey`).
#[derive(Clone)]
pub struct WebhookStore {
    pool: PgPool,
}

impl WebhookStore {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// `OutboundWebhookStore.List`.
    pub async fn list(&self, filters: &[FilterEventType]) -> Result<Vec<Webhook>, String> {
        let mut predicates = Vec::new();
        let mut binds: Vec<String> = Vec::new();
        for filter in filters {
            match &filter.scope {
                None => {
                    binds.push(filter.event_type.clone());
                    predicates.push(format!("(event_type = ${})", binds.len()));
                }
                Some(scope) if scope == FILTER_EVENT_TYPE_NO_SCOPE => {
                    binds.push(filter.event_type.clone());
                    predicates.push(format!("(event_type = ${} AND scope IS NULL)", binds.len()));
                }
                Some(scope) => {
                    binds.push(filter.event_type.clone());
                    binds.push(scope.clone());
                    predicates.push(format!(
                        "(event_type = ${} AND scope = ${})",
                        binds.len() - 1,
                        binds.len()
                    ));
                }
            }
        }

        let where_clause = if predicates.is_empty() {
            "TRUE".to_owned()
        } else {
            format!(
                "id IN (SELECT outbound_webhook_id FROM outbound_webhook_event_types WHERE {})",
                predicates.join(" OR ")
            )
        };
        let query = format!(
            r#"
SELECT
    id, created_by, created_at, updated_by, updated_at,
    encryption_key_id, url, secret, event_types::text AS event_types
FROM outbound_webhooks_with_event_types
WHERE {where_clause}
ORDER BY id ASC
"#
        );
        let mut q = sqlx::query(&query);
        for bind in binds {
            q = q.bind(bind);
        }
        let rows = q.fetch_all(&self.pool).await.map_err(stringify)?;

        let mut webhooks = Vec::with_capacity(rows.len());
        for row in rows {
            let key_id: Option<String> = row.try_get("encryption_key_id").map_err(stringify)?;
            let key_id = key_id.unwrap_or_default();
            let raw_event_types: String = row.try_get("event_types").map_err(stringify)?;
            let event_types: Vec<WebhookEventType> = serde_json::from_str(&raw_event_types)
                .map_err(|err| format!("unmarshalling event types: {err}"))?;
            webhooks.push(Webhook {
                id: row.try_get("id").map_err(stringify)?,
                created_by: row.try_get("created_by").map_err(stringify)?,
                created_at: row.try_get("created_at").map_err(stringify)?,
                updated_by: row.try_get("updated_by").map_err(stringify)?,
                updated_at: row.try_get("updated_at").map_err(stringify)?,
                url: EncryptableValue::new(row.try_get("url").map_err(stringify)?, key_id.clone()),
                secret: EncryptableValue::new(row.try_get("secret").map_err(stringify)?, key_id),
                event_types,
            });
        }
        Ok(webhooks)
    }
}

/// `database.OutboundWebhookLogs.Create`.
#[derive(Clone)]
pub struct WebhookLogStore {
    pool: PgPool,
    key: Option<Arc<dyn Key>>,
}

impl WebhookLogStore {
    pub fn new(pool: PgPool, key: Option<Arc<dyn Key>>) -> Self {
        Self { pool, key }
    }

    /// `OutboundWebhookLogStore.Create`: the key identifier of the `error`
    /// column is written for all three encrypted columns, as in the baseline.
    pub async fn create(&self, log: &WebhookLog) -> Result<WebhookLog, String> {
        let (raw_request, _) = EncryptableValue::encrypt(&log.request, self.key.as_deref())
            .await
            .map_err(|err| format!("encrypting request: {err}"))?;
        let (raw_response, _) = EncryptableValue::encrypt(&log.response, self.key.as_deref())
            .await
            .map_err(|err| format!("encrypting response: {err}"))?;
        let (raw_error, key_id) = EncryptableValue::encrypt(&log.error, self.key.as_deref())
            .await
            .map_err(|err| format!("encrypting error: {err}"))?;
        let key_id = (!key_id.is_empty()).then_some(key_id);

        let row = sqlx::query(
            r#"
INSERT INTO outbound_webhook_logs (
    job_id, outbound_webhook_id, status_code, encryption_key_id, request, response, error
)
VALUES ($1, $2, $3, $4, $5, $6, $7)
RETURNING id, job_id, outbound_webhook_id, status_code, encryption_key_id, request, response, error
"#,
        )
        .bind(log.job_id)
        .bind(log.outbound_webhook_id)
        .bind(log.status_code)
        .bind(key_id)
        .bind(raw_request)
        .bind(raw_response)
        .bind(raw_error)
        .fetch_one(&self.pool)
        .await
        .map_err(stringify)?;

        let key_id: Option<String> = row.try_get("encryption_key_id").map_err(stringify)?;
        let key_id = key_id.unwrap_or_default();
        let request: Vec<u8> = row.try_get("request").map_err(stringify)?;
        let response: Vec<u8> = row.try_get("response").map_err(stringify)?;
        let error: Vec<u8> = row.try_get("error").map_err(stringify)?;
        Ok(WebhookLog {
            id: row.try_get("id").map_err(stringify)?,
            job_id: row.try_get("job_id").map_err(stringify)?,
            outbound_webhook_id: row.try_get("outbound_webhook_id").map_err(stringify)?,
            status_code: row.try_get("status_code").map_err(stringify)?,
            request: EncryptableValue::new(request, key_id.clone())
                .decrypt(self.key.as_deref())
                .await?,
            response: EncryptableValue::new(response, key_id.clone())
                .decrypt(self.key.as_deref())
                .await?,
            error: EncryptableValue::new(error, key_id)
                .decrypt(self.key.as_deref())
                .await?,
        })
    }
}

fn stringify(err: sqlx::Error) -> String {
    err.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn record_state_expands_in_baseline_order() {
        assert_eq!(RecordState::QUEUED.list(), vec!["queued"]);
        assert_eq!(
            (RecordState::QUEUED | RecordState::ERRORED | RecordState::PROCESSING).list(),
            vec!["queued", "errored", "processing"]
        );
        assert!(RecordState(0).list().is_empty());
    }

    #[tokio::test]
    async fn encryptable_round_trips_without_a_key() {
        let value = EncryptableValue::unencrypted("plain");
        assert_eq!(value.decrypt(None).await.expect("decrypt"), "plain");
        let (cipher, key_id) = EncryptableValue::encrypt("plain", None)
            .await
            .expect("encrypt");
        assert_eq!(cipher, b"plain");
        assert_eq!(key_id, "");
    }
}
