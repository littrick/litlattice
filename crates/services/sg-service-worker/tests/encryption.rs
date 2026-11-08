#![forbid(unsafe_code)]

//! M9.8l differential harness (Rust side): the `record-encrypter` job.
//!
//! Mirrors `cmd/worker/internal/encryption/encryption_harness_test.go`. The
//! fixture `test_ws/p9-go/encryption_records_fixtures.sql` is applied by
//! `test_ws/p9-go/run_encryption_records_diff.sh` immediately before each run.
//!
//! A Noop keyring is used for every entry the table configs read (except
//! `outbound_webhooks`, which the baseline `NewRing` never populates), so the
//! stored key IDs are deterministic and values pass through unchanged.
//!
//! Canonical lines: per-table counts before/after each phase and a full dump of
//! the fixture rows (`COALESCE(col::text, '<null>')`, so NULL and '' stay
//! distinguishable).

use sg_config::SiteConfiguration;
use sg_service_worker::jobs::encryption::{
    encryption_configs, EncryptionConfig, RecordEncrypterStore,
};
use sqlx::{PgPool, Row};

const NOOP_KEYRING: &str = r#"{"encryption.keys": {
    "batchChangesCredentialKey": {"type": "noop"},
    "externalServiceKey": {"type": "noop"},
    "gitHubAppKey": {"type": "noop"},
    "outboundWebhookKey": {"type": "noop"},
    "userExternalAccountKey": {"type": "noop"},
    "webhookKey": {"type": "noop"},
    "webhookLogKey": {"type": "noop"},
    "executorSecretKey": {"type": "noop"}
}}"#;

#[tokio::test(flavor = "multi_thread")]
async fn encryption_records_match_go_harness() {
    let Ok(dsn) = std::env::var("SG_QTEST_DSN") else {
        return;
    };
    let pool = PgPool::connect(&dsn).await.expect("connect");
    let site = SiteConfiguration::parse(NOOP_KEYRING).expect("site config");
    let keyring = sg_keyring::init(&site).await.expect("keyring");
    let store = RecordEncrypterStore::new(pool.clone(), keyring);

    for config in encryption_configs() {
        print_count(&store, config).await;

        match store.encrypt_batch(config).await {
            Ok(count) => println!("encrypt {} count={count}", config.table_name),
            Err(err) => println!("encrypt_error {} = {err}", config.table_name),
        }
        dump(&pool, config).await;

        print_count(&store, config).await;

        match store.decrypt_batch(config).await {
            Ok(count) => println!("decrypt {} count={count}", config.table_name),
            Err(err) => println!("decrypt_error {} = {err}", config.table_name),
        }
        dump(&pool, config).await;

        print_count(&store, config).await;
    }
}

async fn print_count(store: &RecordEncrypterStore, config: &EncryptionConfig) {
    match store.count(config).await {
        Ok((encrypted, unencrypted)) => println!(
            "count {} encrypted={encrypted} unencrypted={unencrypted}",
            config.table_name
        ),
        Err(err) => println!("count_error {} = {err}", config.table_name),
    }
}

/// `dumpTable`: every fixture row, with NULL and `''` distinguishable.
async fn dump(pool: &PgPool, config: &EncryptionConfig) {
    let mut columns = vec![
        "id::text".to_owned(),
        "COALESCE(encryption_key_id::text, '<null>')".to_owned(),
    ];
    for field in config.encrypted_field_names {
        columns.push(format!("COALESCE({field}::text, '<null>')"));
    }
    let query = format!(
        "SELECT {} FROM {} WHERE id BETWEEN 9600 AND 9699 ORDER BY id",
        columns.join(", "),
        config.table_name
    );
    let rows = sqlx::query(&query)
        .fetch_all(pool)
        .await
        .expect("dump query");

    for row in rows {
        let mut values = Vec::with_capacity(columns.len());
        for index in 0..columns.len() {
            values.push(
                row.try_get::<String, _>(index)
                    .unwrap_or_else(|err| panic!("scan {}: {err}", config.table_name)),
            );
        }
        println!("dump {} {}", config.table_name, values.join(" "));
    }
}
