#![forbid(unsafe_code)]

//! `cmd/worker/internal/encryption`: the `record-encrypter` job.
//!
//! Two routines share one store:
//!
//! * `encryption.record-encrypter` walks the seven at-rest-encryption tables
//!   and, per table, encrypts (or decrypts, with `ALLOW_DECRYPTION=true`) one
//!   batch of `Limit` rows inside a transaction using
//!   `ORDER BY id ASC ... FOR UPDATE SKIP LOCKED`;
//! * `encryption.operation-metrics` refreshes the per-table
//!   encrypted/unencrypted gauges.
//!
//! Wire compatibility: values are written through
//! [`sg_encryption::maybe_encrypt`] / [`sg_encryption::maybe_decrypt`], which
//! produce the same base64 envelope the Go baseline stores. The update
//! statement binds one parameter per column and orders the `SET` pairs by
//! column name, and `UpdateAsBytes`/`TreatEmptyAsNull` follow the baseline's
//! per-table configuration.
//!
//! Known baseline behavior reproduced deliberately: `keyring.NewRing` never
//! assigns `OutboundWebhookKey`, so `outbound_webhooks` rows are never
//! encrypted even when `encryption.keys.outboundWebhookKey` is configured
//! (docs/modules/06_worker.md §24).

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use async_trait::async_trait;
use sg_core::{ConfigError, EnvReader};
use sg_encryption::{maybe_decrypt, maybe_encrypt, Key, UNMIGRATED_ENCRYPTION_KEY_ID};
use sg_keyring::Keyring;
use sg_metrics::prometheus::{IntCounter, IntCounterVec, IntGaugeVec, Opts};
use sqlx::postgres::PgRow;
use sqlx::{PgPool, Row};
use tokio_util::sync::CancellationToken;

use super::{Job, JobConfig, JobDeps};
use crate::config::combine_errors;
use crate::framework::periodic::{Handler, HandlerError, PeriodicRoutine};
use crate::framework::BackgroundRoutine;

/// `ConfigInst.EncryptionInterval` default.
pub const DEFAULT_ENCRYPTION_INTERVAL: Duration = Duration::from_secs(10);
/// `ConfigInst.MetricsInterval` default.
pub const DEFAULT_METRICS_INTERVAL: Duration = Duration::from_secs(30);

pub const ROUTINE_ENCRYPTER: &str = "encryption.record-encrypter";
pub const ENCRYPTER_DESCRIPTION: &str =
    "encrypts/decrypts existing data when a key is provided/removed";
pub const ROUTINE_OPERATION_METRICS: &str = "encryption.operation-metrics";
pub const OPERATION_METRICS_DESCRIPTION: &str = "tracks number of encrypted vs unencrypted records";

pub const METRIC_RECORDS_ENCRYPTED_AT_REST: &str = "src_records_encrypted_at_rest_total";
pub const METRIC_RECORDS_UNENCRYPTED_AT_REST: &str = "src_records_unencrypted_at_rest_total";
pub const METRIC_RECORDS_ENCRYPTED: &str = "src_records_encrypted_total";
pub const METRIC_RECORDS_DECRYPTED: &str = "src_records_decrypted_total";
pub const METRIC_ENCRYPTION_ERRORS: &str = "src_record_encryption_errors_total";

/// `config` (`cmd/worker/internal/encryption/config.go`).
#[derive(Debug, Default)]
pub struct EncrypterConfig {
    values: Mutex<EncrypterValues>,
    errs: Mutex<Vec<String>>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct EncrypterValues {
    pub encryption_interval: Duration,
    pub metrics_interval: Duration,
    pub decrypt: bool,
}

impl EncrypterConfig {
    pub fn snapshot(&self) -> EncrypterValues {
        *self
            .values
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

impl JobConfig for EncrypterConfig {
    /// `config.Load`.
    fn load(&self, env: &mut EnvReader) {
        let mut errs = Vec::new();
        let values = EncrypterValues {
            encryption_interval: super::get_interval(
                env,
                "RECORD_ENCRYPTER_INTERVAL",
                "10s",
                "How frequently to encrypt/decrypt a batch of records in the database.",
                &mut errs,
            ),
            metrics_interval: super::get_interval(
                env,
                "RECORD_ENCRYPTER_METRICS_INTERVAL",
                "30s",
                "How frequently to update progress metrics related to encryption/decryption.",
                &mut errs,
            ),
            decrypt: env.get_bool(
                "ALLOW_DECRYPTION",
                false,
                "If true, encrypted records will be decrypted and stored in plaintext.",
            ),
        };

        *self
            .values
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = values;
        *self
            .errs
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = errs;
    }

    fn validate(&self) -> Result<(), ConfigError> {
        let errs = self
            .errs
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        match combine_errors(&errs) {
            Some(message) => Err(ConfigError::new(message)),
            None => Ok(()),
        }
    }
}

/// How a table's encrypted columns are read.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScanKind {
    /// `scanEncryptedString`: one `text` column.
    EncryptedString,
    /// `scanNullableEncryptedStringPair`: two nullable `text` columns.
    NullableEncryptedStringPair,
    /// `scanEncryptedStringPair`: two `text`/`bytea` columns.
    EncryptedStringPair,
    /// `scanEncryptedBytea`: one `bytea` column.
    EncryptedBytea,
}

/// `encryptionConfig`.
pub struct EncryptionConfig {
    pub table_name: &'static str,
    pub id_field_name: &'static str,
    pub key_id_field_name: &'static str,
    pub encrypted_field_names: &'static [&'static str],
    pub update_as_bytes: bool,
    pub treat_empty_as_null: bool,
    /// Encrypted columns whose Postgres type is `bytea`. The baseline binds a
    /// Go `string`/`[]byte` (unspecified parameter type), which Postgres
    /// parses according to the target column; `sqlx` sends a typed parameter,
    /// so these columns must be bound as bytes instead (docs §24).
    pub binary_columns: &'static [&'static str],
    pub scan: ScanKind,
    pub limit: i64,
    /// `Key func() encryption.Key`: reads the current ring entry.
    pub key: fn(&Keyring) -> Option<Arc<dyn Key>>,
}

/// `encryptionConfigs`, in the baseline's order.
pub fn encryption_configs() -> &'static [EncryptionConfig] {
    &[
        EncryptionConfig {
            table_name: "external_services",
            id_field_name: "id",
            key_id_field_name: "encryption_key_id",
            encrypted_field_names: &["config"],
            update_as_bytes: false,
            treat_empty_as_null: false,
            binary_columns: &[],
            scan: ScanKind::EncryptedString,
            limit: 100,
            key: Keyring::external_service_key,
        },
        EncryptionConfig {
            table_name: "user_external_accounts",
            id_field_name: "id",
            key_id_field_name: "encryption_key_id",
            encrypted_field_names: &["auth_data", "account_data"],
            update_as_bytes: false,
            treat_empty_as_null: true,
            binary_columns: &[],
            scan: ScanKind::NullableEncryptedStringPair,
            limit: 100,
            key: Keyring::user_external_account_key,
        },
        EncryptionConfig {
            table_name: "user_credentials",
            id_field_name: "id",
            key_id_field_name: "encryption_key_id",
            encrypted_field_names: &["credential"],
            update_as_bytes: true,
            treat_empty_as_null: false,
            binary_columns: &["credential"],
            scan: ScanKind::EncryptedBytea,
            limit: 5,
            key: Keyring::batch_changes_credential_key,
        },
        EncryptionConfig {
            table_name: "batch_changes_site_credentials",
            id_field_name: "id",
            key_id_field_name: "encryption_key_id",
            encrypted_field_names: &["credential"],
            update_as_bytes: true,
            treat_empty_as_null: false,
            binary_columns: &["credential"],
            scan: ScanKind::EncryptedBytea,
            limit: 5,
            key: Keyring::batch_changes_credential_key,
        },
        EncryptionConfig {
            table_name: "webhook_logs",
            id_field_name: "id",
            key_id_field_name: "encryption_key_id",
            encrypted_field_names: &["request", "response"],
            update_as_bytes: false,
            treat_empty_as_null: false,
            binary_columns: &["request", "response"],
            scan: ScanKind::EncryptedStringPair,
            limit: 5,
            key: Keyring::webhook_log_key,
        },
        EncryptionConfig {
            table_name: "executor_secrets",
            id_field_name: "id",
            key_id_field_name: "encryption_key_id",
            encrypted_field_names: &["value"],
            update_as_bytes: true,
            treat_empty_as_null: false,
            binary_columns: &["value"],
            scan: ScanKind::EncryptedBytea,
            limit: 5,
            key: Keyring::executor_secret_key,
        },
        EncryptionConfig {
            table_name: "outbound_webhooks",
            id_field_name: "id",
            key_id_field_name: "encryption_key_id",
            encrypted_field_names: &["url", "secret"],
            update_as_bytes: false,
            treat_empty_as_null: false,
            binary_columns: &["url", "secret"],
            scan: ScanKind::EncryptedStringPair,
            limit: 5,
            key: Keyring::outbound_webhook_key,
        },
    ]
}

/// `encryption.Encrypted`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Encrypted {
    pub values: Vec<String>,
    /// The value stored in the key-id column.
    pub key_id: String,
}

/// One column value in an `UPDATE ... SET` clause.
#[derive(Debug, Clone, PartialEq, Eq)]
enum BindValue {
    Null,
    Text(String),
    Bytes(Vec<u8>),
}

/// `recordEncrypter`.
pub struct RecordEncrypterStore {
    pool: PgPool,
    keyring: Arc<Keyring>,
}

impl RecordEncrypterStore {
    pub fn new(pool: PgPool, keyring: Arc<Keyring>) -> Self {
        Self { pool, keyring }
    }

    /// `recordEncrypter.Count`.
    pub async fn count(&self, config: &EncryptionConfig) -> Result<(i64, i64), String> {
        let query = format!(
            "SELECT\n\t\t\t(SELECT COUNT(*) FROM {table} WHERE {key_id} NOT IN ('', $1)) AS   encrypted,\n\t\t\t(SELECT COUNT(*) FROM {table} WHERE {key_id}     IN ('', $1)) AS unencrypted\n\t\t",
            table = config.table_name,
            key_id = config.key_id_field_name,
        );
        let row = sqlx::query(&query)
            .bind(UNMIGRATED_ENCRYPTION_KEY_ID)
            .fetch_one(&self.pool)
            .await
            .map_err(|err| err.to_string())?;
        let encrypted: i64 = row.try_get(0).map_err(|err| err.to_string())?;
        let unencrypted: i64 = row.try_get(1).map_err(|err| err.to_string())?;
        Ok((encrypted, unencrypted))
    }

    /// `recordEncrypter.EncryptBatch`.
    pub async fn encrypt_batch(&self, config: &EncryptionConfig) -> Result<usize, String> {
        let Some(key) = (config.key)(&self.keyring) else {
            return Ok(0);
        };

        let mut tx = self.pool.begin().await.map_err(|err| err.to_string())?;
        let query = format!(
            "SELECT {fields} FROM {table} WHERE {key_id} IN ('', $1) ORDER BY {id} ASC LIMIT $2 FOR UPDATE SKIP LOCKED",
            fields = select_fields(config),
            table = config.table_name,
            key_id = config.key_id_field_name,
            id = config.id_field_name,
        );
        let rows = match sqlx::query(&query)
            .bind(UNMIGRATED_ENCRYPTION_KEY_ID)
            .bind(config.limit)
            .fetch_all(&mut *tx)
            .await
        {
            Ok(rows) => rows,
            Err(err) => {
                let _ = tx.rollback().await;
                return Err(err.to_string());
            }
        };

        let mut encrypted = Vec::with_capacity(rows.len());
        for row in &rows {
            let (id, value) = match scan_row(config.scan, row) {
                Ok(scan) => scan,
                Err(err) => {
                    let _ = tx.rollback().await;
                    return Err(err);
                }
            };
            let mut key_id = String::new();
            let mut values = Vec::with_capacity(value.values.len());
            for raw in &value.values {
                match maybe_encrypt(Some(key.as_ref()), raw).await {
                    Ok((encrypted_value, encrypted_key_id)) => {
                        key_id = encrypted_key_id;
                        values.push(encrypted_value);
                    }
                    Err(err) => {
                        let _ = tx.rollback().await;
                        return Err(err.to_string());
                    }
                }
            }
            encrypted.push((id, Encrypted { values, key_id }));
        }

        for (id, value) in &encrypted {
            if let Err(err) = update_row(&mut tx, config, *id, value).await {
                let _ = tx.rollback().await;
                return Err(err);
            }
        }

        if let Err(err) = tx.commit().await {
            return Err(err.to_string());
        }
        Ok(encrypted.len())
    }

    /// `recordEncrypter.DecryptBatch`.
    pub async fn decrypt_batch(&self, config: &EncryptionConfig) -> Result<usize, String> {
        let key = (config.key)(&self.keyring);
        let mut tx = self.pool.begin().await.map_err(|err| err.to_string())?;
        let query = format!(
            "SELECT {fields} FROM {table} WHERE {key_id} NOT IN ('', $1) ORDER BY {id} ASC LIMIT $2 FOR UPDATE SKIP LOCKED",
            fields = select_fields(config),
            table = config.table_name,
            key_id = config.key_id_field_name,
            id = config.id_field_name,
        );
        let rows = match sqlx::query(&query)
            .bind(UNMIGRATED_ENCRYPTION_KEY_ID)
            .bind(config.limit)
            .fetch_all(&mut *tx)
            .await
        {
            Ok(rows) => rows,
            Err(err) => {
                let _ = tx.rollback().await;
                return Err(err.to_string());
            }
        };

        let mut decrypted = Vec::with_capacity(rows.len());
        for row in &rows {
            let (id, encrypted) = match scan_row(config.scan, row) {
                Ok(scan) => scan,
                Err(err) => {
                    let _ = tx.rollback().await;
                    return Err(err);
                }
            };
            let mut values = Vec::with_capacity(encrypted.values.len());
            for raw in &encrypted.values {
                match maybe_decrypt(key.as_deref(), raw, &encrypted.key_id).await {
                    Ok(value) => values.push(value),
                    Err(err) => {
                        let _ = tx.rollback().await;
                        return Err(err.to_string());
                    }
                }
            }
            decrypted.push((
                id,
                Encrypted {
                    values,
                    key_id: String::new(),
                },
            ));
        }

        for (id, value) in &decrypted {
            if let Err(err) = update_row(&mut tx, config, *id, value).await {
                let _ = tx.rollback().await;
                return Err(err);
            }
        }

        if let Err(err) = tx.commit().await {
            return Err(err.to_string());
        }
        Ok(decrypted.len())
    }
}

/// `fields`: the id, the key-id column and the encrypted columns.
fn select_fields(config: &EncryptionConfig) -> String {
    let mut fields = Vec::with_capacity(config.encrypted_field_names.len() + 2);
    fields.push(config.id_field_name);
    fields.push(config.key_id_field_name);
    fields.extend(config.encrypted_field_names.iter().copied());
    fields.join(", ")
}

/// The baseline's `Scan` functions.
fn scan_row(kind: ScanKind, row: &PgRow) -> Result<(i64, Encrypted), String> {
    let id = row
        .try_get::<i64, _>(0)
        .or_else(|_| row.try_get::<i32, _>(0).map(i64::from))
        .map_err(|err| err.to_string())?;
    let key_id = row
        .try_get::<Option<String>, _>(1)
        .map_err(|err| err.to_string())?
        .unwrap_or_default();

    let values = match kind {
        ScanKind::EncryptedString => {
            vec![row.try_get::<String, _>(2).map_err(|err| err.to_string())?]
        }
        ScanKind::NullableEncryptedStringPair => {
            vec![nullable_text(row, 2)?, nullable_text(row, 3)?]
        }
        ScanKind::EncryptedStringPair => vec![string_or_bytes(row, 2)?, string_or_bytes(row, 3)?],
        ScanKind::EncryptedBytea => {
            let bytes = row
                .try_get::<Vec<u8>, _>(2)
                .map_err(|err| err.to_string())?;
            vec![String::from_utf8_lossy(&bytes).into_owned()]
        }
    };
    Ok((id, Encrypted { values, key_id }))
}

/// `dbutil.NullString`: SQL NULL scans as the empty string.
fn nullable_text(row: &PgRow, index: usize) -> Result<String, String> {
    row.try_get::<Option<String>, _>(index)
        .map(|value| value.unwrap_or_default())
        .map_err(|err| err.to_string())
}

/// `scanEncryptedStringPair` scans `text` or `bytea` into a string.
fn string_or_bytes(row: &PgRow, index: usize) -> Result<String, String> {
    match row.try_get::<String, _>(index) {
        Ok(value) => Ok(value),
        Err(_) => {
            let bytes = row
                .try_get::<Vec<u8>, _>(index)
                .map_err(|err| err.to_string())?;
            Ok(String::from_utf8_lossy(&bytes).into_owned())
        }
    }
}

/// `updatePairs`: `SET` pairs sorted by column name.
fn update_pairs(config: &EncryptionConfig, value: &Encrypted) -> Vec<(&'static str, BindValue)> {
    let mut columns: BTreeMap<&'static str, &str> = BTreeMap::new();
    for (index, column) in config.encrypted_field_names.iter().enumerate() {
        columns.insert(
            column,
            value.values.get(index).map(String::as_str).unwrap_or(""),
        );
    }
    columns.insert(config.key_id_field_name, &value.key_id);

    columns
        .into_iter()
        .map(|(column, raw)| {
            let bind = if raw.is_empty()
                && column != config.key_id_field_name
                && config.treat_empty_as_null
            {
                BindValue::Null
            } else if config.binary_columns.contains(&column) {
                // `bytea` targets need a byte parameter: `sqlx` sends `text`
                // for a `String`, which Postgres rejects for `bytea`.
                // `UpdateAsBytes` itself does not change what Go stores — its
                // `[]byte` parameter is sent untyped and parsed by the target
                // column (docs §24).
                BindValue::Bytes(raw.as_bytes().to_vec())
            } else {
                BindValue::Text(raw.to_owned())
            };
            (column, bind)
        })
        .collect()
}

/// `UPDATE <table> SET <pairs> WHERE <id> = <id>` inside `tx`.
async fn update_row(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    config: &EncryptionConfig,
    id: i64,
    value: &Encrypted,
) -> Result<(), String> {
    let pairs = update_pairs(config, value);
    let mut query = format!("UPDATE {} SET ", config.table_name);
    for (index, (column, _)) in pairs.iter().enumerate() {
        if index > 0 {
            query.push_str(", ");
        }
        query.push_str(column);
        query.push_str(" = $");
        query.push_str(&(index + 1).to_string());
    }
    query.push_str(" WHERE ");
    query.push_str(config.id_field_name);
    query.push_str(" = $");
    query.push_str(&(pairs.len() + 1).to_string());

    let mut request = sqlx::query(&query);
    for (_, bind) in &pairs {
        request = match bind {
            BindValue::Null => request.bind(None::<String>),
            BindValue::Text(text) => request.bind(text.clone()),
            BindValue::Bytes(bytes) => request.bind(bytes.clone()),
        };
    }
    request
        .bind(id)
        .execute(&mut **tx)
        .await
        .map_err(|err| err.to_string())?;
    Ok(())
}

/// `newMetrics`: the five encryption metrics, registered once per process.
struct Metrics {
    num_encrypted_at_rest: IntGaugeVec,
    num_unencrypted_at_rest: IntGaugeVec,
    num_records_encrypted: IntCounterVec,
    num_records_decrypted: IntCounterVec,
    num_errors: IntCounter,
}

fn metrics() -> &'static Metrics {
    static METRICS: OnceLock<Metrics> = OnceLock::new();
    METRICS.get_or_init(|| {
        let gauge_vec = |name: &str, help: &str| {
            let gauge = IntGaugeVec::new(Opts::new(name, help), &["tableName"])
                .expect("valid encryption gauge");
            let _ = sg_metrics::registry().register(Box::new(gauge.clone()));
            gauge
        };
        let counter_vec = |name: &str, help: &str| {
            let counter = IntCounterVec::new(Opts::new(name, help), &["tableName"])
                .expect("valid encryption counter");
            let _ = sg_metrics::registry().register(Box::new(counter.clone()));
            counter
        };
        let num_errors = IntCounter::new(
            METRIC_ENCRYPTION_ERRORS,
            "The number of errors that occur during record encryption/decryption.",
        )
        .expect("valid encryption error counter");
        let _ = sg_metrics::registry().register(Box::new(num_errors.clone()));

        let metrics = Metrics {
            num_encrypted_at_rest: gauge_vec(
                METRIC_RECORDS_ENCRYPTED_AT_REST,
                "The number of database records encrypted at rest.",
            ),
            num_unencrypted_at_rest: gauge_vec(
                METRIC_RECORDS_UNENCRYPTED_AT_REST,
                "The number of database records unencrypted at rest.",
            ),
            num_records_encrypted: counter_vec(
                METRIC_RECORDS_ENCRYPTED,
                "The number of unencrypted database records that have been encrypted.",
            ),
            num_records_decrypted: counter_vec(
                METRIC_RECORDS_DECRYPTED,
                "The number of encrypted database records that have been decrypted.",
            ),
            num_errors,
        };

        for config in encryption_configs() {
            // Initialize counters to zero.
            metrics
                .num_records_encrypted
                .with_label_values(&[config.table_name])
                .inc_by(0);
            metrics
                .num_records_decrypted
                .with_label_values(&[config.table_name])
                .inc_by(0);
        }
        metrics
    })
}

/// `recordEncrypterRoutine`.
pub struct RecordEncrypterHandler {
    store: Arc<RecordEncrypterStore>,
    decrypt: bool,
}

impl RecordEncrypterHandler {
    pub fn new(store: Arc<RecordEncrypterStore>, decrypt: bool) -> Self {
        Self { store, decrypt }
    }
}

#[async_trait]
impl Handler for RecordEncrypterHandler {
    async fn handle(&self, ctx: CancellationToken) -> Result<(), HandlerError> {
        let _ = metrics();
        let mut errors = Vec::new();
        for config in encryption_configs() {
            if ctx.is_cancelled() {
                break;
            }
            let result = if self.decrypt {
                self.store.decrypt_batch(config).await
            } else {
                self.store.encrypt_batch(config).await
            };
            match result {
                Ok(0) => {}
                Ok(count) => {
                    if self.decrypt {
                        metrics()
                            .num_records_decrypted
                            .with_label_values(&[config.table_name])
                            .inc_by(count as u64);
                        tracing::debug!(table_name = config.table_name, count, "decrypted records");
                    } else {
                        metrics()
                            .num_records_encrypted
                            .with_label_values(&[config.table_name])
                            .inc_by(count as u64);
                        tracing::debug!(table_name = config.table_name, count, "encrypted records");
                    }
                }
                Err(err) => errors.push(err),
            }
        }
        match combine_errors(&errors) {
            Some(message) => Err(HandlerError::new(message)),
            None => Ok(()),
        }
    }

    fn handle_error(&self, error: &HandlerError) {
        metrics().num_errors.inc();
        let verb = if self.decrypt { "decrypt" } else { "encrypt" };
        tracing::error!(error = %error, "failed to {verb} batch of records");
    }
}

/// `recordCounter`.
pub struct RecordCounterHandler {
    store: Arc<RecordEncrypterStore>,
}

impl RecordCounterHandler {
    pub fn new(store: Arc<RecordEncrypterStore>) -> Self {
        Self { store }
    }
}

#[async_trait]
impl Handler for RecordCounterHandler {
    async fn handle(&self, _ctx: CancellationToken) -> Result<(), HandlerError> {
        let _ = metrics();
        for config in encryption_configs() {
            let (encrypted, unencrypted) =
                self.store.count(config).await.map_err(HandlerError::new)?;
            metrics()
                .num_encrypted_at_rest
                .with_label_values(&[config.table_name])
                .set(encrypted);
            metrics()
                .num_unencrypted_at_rest
                .with_label_values(&[config.table_name])
                .set(unencrypted);
        }
        Ok(())
    }

    fn handle_error(&self, error: &HandlerError) {
        metrics().num_errors.inc();
        tracing::error!(error = %error, "failed to count records");
    }
}

/// `recordEncrypterJob`.
pub struct RecordEncrypterJob {
    config: Arc<EncrypterConfig>,
}

impl RecordEncrypterJob {
    pub fn new() -> Self {
        Self {
            config: Arc::new(EncrypterConfig::default()),
        }
    }
}

impl Default for RecordEncrypterJob {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Job for RecordEncrypterJob {
    fn description(&self) -> String {
        "encrypter routines".to_owned()
    }

    fn configs(&self) -> Vec<Arc<dyn JobConfig>> {
        vec![Arc::clone(&self.config) as Arc<dyn JobConfig>]
    }

    async fn routines(
        &self,
        deps: &JobDeps,
        _setup_ctx: CancellationToken,
    ) -> Result<Vec<BackgroundRoutine>, String> {
        let _ = metrics();
        let values = self.config.snapshot();
        let store = Arc::new(RecordEncrypterStore::new(
            deps.frontend.clone(),
            Arc::clone(&deps.keyring),
        ));

        let encrypter = PeriodicRoutine::new(Arc::new(RecordEncrypterHandler::new(
            Arc::clone(&store),
            values.decrypt,
        )))
        .with_name(ROUTINE_ENCRYPTER)
        .with_description(ENCRYPTER_DESCRIPTION)
        .with_interval(values.encryption_interval);
        let counter = PeriodicRoutine::new(Arc::new(RecordCounterHandler::new(store)))
            .with_name(ROUTINE_OPERATION_METRICS)
            .with_description(OPERATION_METRICS_DESCRIPTION)
            .with_interval(values.metrics_interval);

        Ok(vec![encrypter.into(), counter.into()])
    }
}

/// The counters' sample values, for the differential harness.
pub fn metric_sample(table: &str) -> (i64, i64, u64, u64, u64) {
    let metrics = metrics();
    let encrypted = metrics
        .num_encrypted_at_rest
        .with_label_values(&[table])
        .get();
    let unencrypted = metrics
        .num_unencrypted_at_rest
        .with_label_values(&[table])
        .get();
    let records_encrypted = metrics
        .num_records_encrypted
        .with_label_values(&[table])
        .get();
    let records_decrypted = metrics
        .num_records_decrypted
        .with_label_values(&[table])
        .get();
    (
        encrypted,
        unencrypted,
        records_encrypted,
        records_decrypted,
        metrics.num_errors.get(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Tests run on parallel threads; the process environment is shared.
    static ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    fn with_env(vars: &[(&str, &str)], f: impl FnOnce()) {
        let _guard = ENV_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        for (name, value) in vars {
            std::env::set_var(name, value);
        }
        f();
        for (name, _) in vars {
            std::env::remove_var(name);
        }
    }

    #[test]
    fn job_metadata_matches_the_baseline() {
        let job = RecordEncrypterJob::new();
        assert_eq!(job.description(), "encrypter routines");
        assert_eq!(job.configs().len(), 1);
        assert_eq!(ROUTINE_ENCRYPTER, "encryption.record-encrypter");
        assert_eq!(
            ENCRYPTER_DESCRIPTION,
            "encrypts/decrypts existing data when a key is provided/removed"
        );
        assert_eq!(ROUTINE_OPERATION_METRICS, "encryption.operation-metrics");
        assert_eq!(
            OPERATION_METRICS_DESCRIPTION,
            "tracks number of encrypted vs unencrypted records"
        );
        assert_eq!(DEFAULT_ENCRYPTION_INTERVAL, Duration::from_secs(10));
        assert_eq!(DEFAULT_METRICS_INTERVAL, Duration::from_secs(30));
    }

    #[test]
    fn table_configs_match_the_baseline() {
        let configs = encryption_configs();
        let expected = [
            (
                "external_services",
                &["config"][..],
                false,
                100,
                ScanKind::EncryptedString,
            ),
            (
                "user_external_accounts",
                &["auth_data", "account_data"][..],
                false,
                100,
                ScanKind::NullableEncryptedStringPair,
            ),
            (
                "user_credentials",
                &["credential"][..],
                true,
                5,
                ScanKind::EncryptedBytea,
            ),
            (
                "batch_changes_site_credentials",
                &["credential"][..],
                true,
                5,
                ScanKind::EncryptedBytea,
            ),
            (
                "webhook_logs",
                &["request", "response"][..],
                false,
                5,
                ScanKind::EncryptedStringPair,
            ),
            (
                "executor_secrets",
                &["value"][..],
                true,
                5,
                ScanKind::EncryptedBytea,
            ),
            (
                "outbound_webhooks",
                &["url", "secret"][..],
                false,
                5,
                ScanKind::EncryptedStringPair,
            ),
        ];
        assert_eq!(configs.len(), expected.len());
        for (config, (table, fields, update_as_bytes, limit, scan)) in configs.iter().zip(expected)
        {
            assert_eq!(config.table_name, table);
            assert_eq!(config.id_field_name, "id");
            assert_eq!(config.key_id_field_name, "encryption_key_id");
            assert_eq!(config.encrypted_field_names, fields);
            assert_eq!(config.update_as_bytes, update_as_bytes);
            assert_eq!(config.limit, limit);
            assert_eq!(config.scan, scan);
        }
        assert!(
            configs[1].treat_empty_as_null,
            "only user_external_accounts"
        );
        assert_eq!(
            configs
                .iter()
                .filter(|config| config.treat_empty_as_null)
                .count(),
            1
        );
    }

    #[tokio::test]
    async fn every_table_reads_its_configured_key() {
        let site = sg_config::SiteConfiguration::parse(
            r#"{"encryption.keys": {
                "batchChangesCredentialKey": {"type": "noop"},
                "externalServiceKey": {"type": "noop"},
                "gitHubAppKey": {"type": "noop"},
                "outboundWebhookKey": {"type": "noop"},
                "userExternalAccountKey": {"type": "noop"},
                "webhookKey": {"type": "noop"},
                "webhookLogKey": {"type": "noop"},
                "executorSecretKey": {"type": "noop"}
            }}"#,
        )
        .expect("parse");
        let keyring = sg_keyring::init(&site).await.expect("noop ring");
        for config in encryption_configs() {
            let has_key = (config.key)(&keyring).is_some();
            if config.table_name == "outbound_webhooks" {
                // `NewRing` never assigns `OutboundWebhookKey` (baseline bug).
                assert!(!has_key);
            } else {
                assert!(has_key, "{} must see its configured key", config.table_name);
            }
        }
    }

    #[test]
    fn config_defaults_match_the_baseline() {
        with_env(
            &[
                ("RECORD_ENCRYPTER_INTERVAL", ""),
                ("RECORD_ENCRYPTER_METRICS_INTERVAL", ""),
                ("ALLOW_DECRYPTION", ""),
            ],
            || {
                let mut env = EnvReader::new();
                let config = EncrypterConfig::default();
                config.load(&mut env);
                assert_eq!(
                    config.snapshot(),
                    EncrypterValues {
                        encryption_interval: Duration::ZERO,
                        metrics_interval: Duration::ZERO,
                        decrypt: false,
                    }
                );
                assert_eq!(
                    config.validate().expect_err("invalid").to_string(),
                    "2 errors occurred:\n\t* invalid duration \"\" for RECORD_ENCRYPTER_INTERVAL: time: invalid duration \"\"\n\t* invalid duration \"\" for RECORD_ENCRYPTER_METRICS_INTERVAL: time: invalid duration \"\""
                );
            },
        );
    }

    #[test]
    fn config_parses_values_and_allows_decryption() {
        with_env(
            &[
                ("RECORD_ENCRYPTER_INTERVAL", "20s"),
                ("RECORD_ENCRYPTER_METRICS_INTERVAL", "1m"),
                ("ALLOW_DECRYPTION", "true"),
            ],
            || {
                let mut env = EnvReader::new();
                let config = EncrypterConfig::default();
                config.load(&mut env);
                assert!(config.validate().is_ok());
                assert_eq!(
                    config.snapshot(),
                    EncrypterValues {
                        encryption_interval: Duration::from_secs(20),
                        metrics_interval: Duration::from_secs(60),
                        decrypt: true,
                    }
                );
            },
        );

        with_env(&[("RECORD_ENCRYPTER_INTERVAL", "10x")], || {
            let mut env = EnvReader::new();
            let config = EncrypterConfig::default();
            config.load(&mut env);
            assert_eq!(
                config.validate().expect_err("invalid").to_string(),
                "invalid duration \"10x\" for RECORD_ENCRYPTER_INTERVAL: time: invalid duration \"10x\""
            );
        });
    }

    #[test]
    fn update_pairs_sort_columns_and_null_empty_values() {
        let configs = encryption_configs();
        let external = &configs[0];
        let accounts = &configs[1];
        let credentials = &configs[2];
        let webhook_logs = &configs[4];

        let pairs = update_pairs(
            external,
            &Encrypted {
                values: vec!["cipher".to_owned()],
                key_id: "key".to_owned(),
            },
        );
        assert_eq!(
            pairs,
            vec![
                ("config", BindValue::Text("cipher".to_owned())),
                ("encryption_key_id", BindValue::Text("key".to_owned())),
            ]
        );

        // `TreatEmptyAsNull`: empty values become NULL, the key ID does not.
        let pairs = update_pairs(
            accounts,
            &Encrypted {
                values: vec![String::new(), String::new()],
                key_id: String::new(),
            },
        );
        assert_eq!(
            pairs,
            vec![
                ("account_data", BindValue::Null),
                ("auth_data", BindValue::Null),
                ("encryption_key_id", BindValue::Text(String::new())),
            ]
        );

        // `UpdateAsBytes`: every column, including the text key ID, binds bytes.
        let pairs = update_pairs(
            credentials,
            &Encrypted {
                values: vec!["cipher".to_owned()],
                key_id: "key".to_owned(),
            },
        );
        assert_eq!(
            pairs,
            vec![
                ("credential", BindValue::Bytes(b"cipher".to_vec())),
                ("encryption_key_id", BindValue::Text("key".to_owned())),
            ]
        );

        // `bytea` encrypted columns bind bytes; the text key ID binds text.
        let pairs = update_pairs(
            webhook_logs,
            &Encrypted {
                values: vec!["request".to_owned(), "response".to_owned()],
                key_id: "key".to_owned(),
            },
        );
        assert_eq!(
            pairs,
            vec![
                ("encryption_key_id", BindValue::Text("key".to_owned())),
                ("request", BindValue::Bytes(b"request".to_vec())),
                ("response", BindValue::Bytes(b"response".to_vec())),
            ]
        );
    }

    #[test]
    fn select_fields_are_the_id_key_id_and_encrypted_columns() {
        let configs = encryption_configs();
        assert_eq!(select_fields(&configs[0]), "id, encryption_key_id, config");
        assert_eq!(
            select_fields(&configs[1]),
            "id, encryption_key_id, auth_data, account_data"
        );
    }

    #[test]
    fn metrics_are_zero_initialized_for_every_table() {
        let _ = metrics();
        for config in encryption_configs() {
            let sample = metric_sample(config.table_name);
            assert_eq!(
                sample,
                (0, 0, 0, 0, 0),
                "{} must start at zero",
                config.table_name
            );
        }
    }
}
