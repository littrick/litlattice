//! M9.8gm differential harness: `overrideExtSvcConfig` and its watcher.
//!
//! Prints the same `ce `/`cr `/`cw ` lines as
//! `cmd/frontend/internal/cli/zz_m9gm_extsvc_config_file_oracle_test.go`.
//!
//! Gated by `SG_M9GM_HARNESS=1`, needs `SG_M9GM_DSN` (a scratch database
//! cloned from `sourcegraph`'s schema), the scenario in `SG_M9GM_SCENARIO`
//! and the file to rewrite in `EXTSVC_CONFIG_FILE`.

use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use sg_db::external_services::ExternalServiceStore;
use sg_encryption::{maybe_decrypt, Key, NoopKey};
use sg_service_frontend::configuration_source::DbConfigurationSource;
use sg_service_frontend::extsvc_config_file::{
    extsvc_config_path_from_env, override_ext_svc_config, ExtsvcConfigWatcher,
};
use sqlx::{PgPool, Row};
use time::OffsetDateTime;
use tokio_util::sync::CancellationToken;

const FILE_ONE: &str = r#"{"OTHER":[{"repos":["repo1"],"url":"https://git.internal.test/"}]}"#;
const FILE_TWO: &str = r#"{"OTHER":[{"repos":["repo1"],"url":"https://git.internal.test/"},{"repos":["repo2"],"url":"https://git.internal.test/"}]}"#;
const FILE_ONE_B: &str = r#"{"OTHER":[{"repos":["repo1"],"url":"https://git.internal.test/","repositoryPathPattern":"{base}/{repo}"}]}"#;
const FILE_MULTI_KIND: &str = r#"{"GITHUB":[{"repos":["org/repo"],"token":"secret","url":"https://github.internal.test"}],"OTHER":[{"repos":["repo1"],"url":"https://git.internal.test/"}]}"#;
const FILE_INVALID_JSON: &str = "{";
const FILE_INVALID_CONFIGS: &str = r#"{"OTHER":5}"#;
const FILE_INVALID_KIND: &str = r#"{"NOPE":[{}]}"#;
const FILE_INVALID_ITEM: &str = r#"{"OTHER":[5]}"#;
const FILE_EMPTY_OBJECT: &str = "{}";
/// Exactly `json.MarshalIndent` of the OTHER config in [`FILE_ONE`].
const ENCRYPTED_EQUIV: &str =
    "{\n  \"repos\": [\n    \"repo1\"\n  ],\n  \"url\": \"https://git.internal.test/\"\n}";
const NOOP_KEY_ID: &str = r#"{"Type":"noop","Name":"noop","Version":""}"#;

#[derive(Clone, Debug)]
struct SnapshotRow {
    kind: String,
    display: String,
    canonical: String,
    deleted: bool,
    updated_at: OffsetDateTime,
}

fn row_key(row: &SnapshotRow) -> String {
    format!("{}|{}", row.kind, row.display)
}

fn canonical(config: &str) -> String {
    match serde_json::from_str::<serde_json::Value>(config) {
        Ok(value) => serde_json::to_string(&value).unwrap_or_else(|_| config.to_owned()),
        Err(_) => config.to_owned(),
    }
}

async fn reset(pool: &PgPool) {
    sqlx::query("TRUNCATE external_services, code_hosts RESTART IDENTITY CASCADE")
        .execute(pool)
        .await
        .expect("truncate external services");
}

async fn seed(pool: &PgPool, kind: &str, display: &str, config: &str) {
    sqlx::query("INSERT INTO external_services (kind, display_name, config) VALUES ($1, $2, $3)")
        .bind(kind)
        .bind(display)
        .bind(config)
        .execute(pool)
        .await
        .expect("seed external service");
}

async fn seed_encrypted(pool: &PgPool, kind: &str, display: &str, config: &str, key_id: &str) {
    sqlx::query(
        "INSERT INTO external_services (kind, display_name, config, encryption_key_id) \
         VALUES ($1, $2, $3, $4)",
    )
    .bind(kind)
    .bind(display)
    .bind(config)
    .bind(key_id)
    .execute(pool)
    .await
    .expect("seed encrypted external service");
}

async fn snapshot(pool: &PgPool, key: Option<Arc<dyn Key>>) -> Vec<SnapshotRow> {
    let rows = sqlx::query(
        "SELECT kind, display_name, config, encryption_key_id, deleted_at IS NOT NULL AS deleted, updated_at \
         FROM external_services ORDER BY kind, display_name, deleted_at NULLS FIRST",
    )
    .fetch_all(pool)
    .await
    .expect("list external services");
    let mut result = Vec::with_capacity(rows.len());
    for row in rows {
        let kind: String = row.try_get("kind").expect("kind");
        let display: String = row.try_get("display_name").expect("display_name");
        let config: String = row.try_get("config").expect("config");
        let key_id: String = row.try_get("encryption_key_id").expect("encryption_key_id");
        let deleted: bool = row.try_get("deleted").expect("deleted");
        let updated_at: OffsetDateTime = row.try_get("updated_at").expect("updated_at");
        let decrypted = maybe_decrypt(key.as_deref(), &config, &key_id)
            .await
            .expect("decrypt external service config");
        result.push(SnapshotRow {
            kind,
            display,
            canonical: canonical(&decrypted),
            deleted,
            updated_at,
        });
    }
    result.sort_by(|a, b| (&a.kind, &a.display).cmp(&(&b.kind, &b.display)));
    result
}

fn write_file(path: &Option<PathBuf>, contents: &str) {
    let path = path.as_ref().expect("EXTSVC_CONFIG_FILE is empty");
    fs::write(path, contents).expect("write EXTSVC_CONFIG_FILE");
}

async fn print_after(
    pool: &PgPool,
    key: Option<Arc<dyn Key>>,
    tag: &str,
    override_err: Option<String>,
    before: Option<&[SnapshotRow]>,
) {
    let mut instants: HashMap<String, OffsetDateTime> = HashMap::new();
    if let Some(before) = before {
        for row in before {
            instants.insert(row_key(row), row.updated_at);
        }
    }
    let rows = snapshot(pool, key).await;
    let mut live = 0;
    let mut deleted = 0;
    for row in &rows {
        if row.deleted {
            deleted += 1;
        } else {
            live += 1;
        }
    }
    println!(
        "ce {tag} err={} live={live} deleted={deleted}",
        usize::from(override_err.is_some())
    );
    for row in &rows {
        let updated = match before {
            None => "-".to_owned(),
            Some(_) => match instants.get(&row_key(row)) {
                None => "-".to_owned(),
                Some(old) if *old == row.updated_at => "0".to_owned(),
                Some(_) => "1".to_owned(),
            },
        };
        println!(
            "cr {}|{}|config={}|deleted={}|updated={updated}",
            row.kind,
            row.display,
            row.canonical,
            usize::from(row.deleted)
        );
    }
}

fn config_path(path: &Option<PathBuf>) -> &std::path::Path {
    path.as_deref().expect("EXTSVC_CONFIG_FILE is empty")
}

#[tokio::test]
async fn extsvc_config_file_harness() {
    if std::env::var("SG_M9GM_HARNESS").as_deref() != Ok("1") {
        return;
    }
    let dsn = std::env::var("SG_M9GM_DSN").expect("SG_M9GM_DSN is not set");
    let scenario = std::env::var("SG_M9GM_SCENARIO").unwrap_or_default();
    let path = extsvc_config_path_from_env();

    let pool = PgPool::connect(&dsn).await.expect("connect");
    let source = DbConfigurationSource::new(pool.clone()).expect("configuration source");
    let store = ExternalServiceStore::new(pool.clone());

    match scenario.as_str() {
        "env-empty" => {
            reset(&pool).await;
            seed(&pool, "OTHER", "seed", ENCRYPTED_EQUIV).await;
            let before = snapshot(&pool, None).await;
            let result = match extsvc_config_path_from_env() {
                Some(path) => override_ext_svc_config(&source, &pool, None, &path)
                    .await
                    .map_err(|e| e.to_string()),
                None => Ok(false),
            };
            print_after(&pool, None, "env-empty", result.err(), Some(&before)).await;
        }
        "empty-object" => {
            reset(&pool).await;
            seed(&pool, "OTHER", "seed", ENCRYPTED_EQUIV).await;
            let before = snapshot(&pool, None).await;
            write_file(&path, FILE_EMPTY_OBJECT);
            let result = override_ext_svc_config(&source, &pool, None, config_path(&path)).await;
            print_after(&pool, None, "empty-object", result.err(), Some(&before)).await;
        }
        "add-single" => {
            reset(&pool).await;
            write_file(&path, FILE_ONE);
            let result = override_ext_svc_config(&source, &pool, None, config_path(&path)).await;
            print_after(&pool, None, "add-single", result.err(), None).await;
        }
        "add-multi" => {
            reset(&pool).await;
            write_file(&path, FILE_TWO);
            let result = override_ext_svc_config(&source, &pool, None, config_path(&path)).await;
            print_after(&pool, None, "add-multi", result.err(), None).await;
        }
        "rerun-same" => {
            reset(&pool).await;
            write_file(&path, FILE_ONE);
            override_ext_svc_config(&source, &pool, None, config_path(&path))
                .await
                .expect("first update");
            let before = snapshot(&pool, None).await;
            let result = override_ext_svc_config(&source, &pool, None, config_path(&path)).await;
            print_after(&pool, None, "rerun-same", result.err(), Some(&before)).await;
        }
        "update-config" => {
            reset(&pool).await;
            write_file(&path, FILE_ONE);
            override_ext_svc_config(&source, &pool, None, config_path(&path))
                .await
                .expect("first update");
            let before = snapshot(&pool, None).await;
            write_file(&path, FILE_ONE_B);
            let result = override_ext_svc_config(&source, &pool, None, config_path(&path)).await;
            print_after(&pool, None, "update-config", result.err(), Some(&before)).await;
        }
        "remove-one" => {
            reset(&pool).await;
            write_file(&path, FILE_TWO);
            override_ext_svc_config(&source, &pool, None, config_path(&path))
                .await
                .expect("first update");
            let before = snapshot(&pool, None).await;
            write_file(&path, FILE_ONE);
            let result = override_ext_svc_config(&source, &pool, None, config_path(&path)).await;
            print_after(&pool, None, "remove-one", result.err(), Some(&before)).await;
        }
        "multi-kind" => {
            reset(&pool).await;
            write_file(&path, FILE_MULTI_KIND);
            let result = override_ext_svc_config(&source, &pool, None, config_path(&path)).await;
            print_after(&pool, None, "multi-kind", result.err(), None).await;
        }
        "encrypted-equiv" => {
            reset(&pool).await;
            let key: Arc<dyn Key> = Arc::new(NoopKey::default());
            seed_encrypted(&pool, "OTHER", "OTHER #1", ENCRYPTED_EQUIV, NOOP_KEY_ID).await;
            let before = snapshot(&pool, Some(key.clone())).await;
            write_file(&path, FILE_ONE);
            let result =
                override_ext_svc_config(&source, &pool, Some(key.clone()), config_path(&path))
                    .await;
            print_after(
                &pool,
                Some(key),
                "encrypted-equiv",
                result.err(),
                Some(&before),
            )
            .await;
        }
        "invalid-json" | "invalid-configs" | "invalid-kind" | "invalid-item" => {
            reset(&pool).await;
            seed(&pool, "OTHER", "seed", ENCRYPTED_EQUIV).await;
            let before = snapshot(&pool, None).await;
            let contents = match scenario.as_str() {
                "invalid-json" => FILE_INVALID_JSON,
                "invalid-configs" => FILE_INVALID_CONFIGS,
                "invalid-kind" => FILE_INVALID_KIND,
                _ => FILE_INVALID_ITEM,
            };
            write_file(&path, contents);
            let result = override_ext_svc_config(&source, &pool, None, config_path(&path)).await;
            print_after(&pool, None, &scenario, result.err(), Some(&before)).await;
        }
        "idempotent" => {
            reset(&pool).await;
            write_file(&path, FILE_ONE);
            override_ext_svc_config(&source, &pool, None, config_path(&path))
                .await
                .expect("first update");
            let before = snapshot(&pool, None).await;
            override_ext_svc_config(&source, &pool, None, config_path(&path))
                .await
                .expect("second update");
            let result = override_ext_svc_config(&source, &pool, None, config_path(&path)).await;
            print_after(&pool, None, "idempotent", result.err(), Some(&before)).await;
        }
        "watch-update" => {
            reset(&pool).await;
            write_file(&path, FILE_ONE);
            override_ext_svc_config(&source, &pool, None, config_path(&path))
                .await
                .expect("initial update");
            let watcher = ExtsvcConfigWatcher::new(
                source.clone(),
                pool.clone(),
                None,
                path.as_ref().expect("path").clone(),
                Duration::from_millis(50),
            )
            .expect("watcher");
            let shutdown = CancellationToken::new();
            let handle = tokio::spawn(watcher.run(shutdown.clone()));

            write_file(&path, FILE_TWO);
            let mut changed = 0;
            for _ in 0..200 {
                tokio::time::sleep(Duration::from_millis(50)).await;
                let live = store.list().await.expect("list").len();
                if live == 2 {
                    changed = 1;
                    break;
                }
            }
            shutdown.cancel();
            let _ = handle.await;
            println!("cw watch-update changed={changed}");
        }
        other => panic!("unknown scenario {other:?}"),
    }
}
