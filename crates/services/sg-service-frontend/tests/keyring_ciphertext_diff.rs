//! M9.36a differential of the keyring ciphertext paths against the Go baseline.
//!
//! Both sides write and read the same rows with a fixed mounted keyring:
//! `user_credentials` (batches domain), `batch_changes_site_credentials`,
//! `github_apps` (`client_secret` / `private_key`) and `webhooks` (secret).
//! The Go oracle is `internal/database/zz_m936a_keyring_oracle_test.go`.
//!
//! Because the mounted key uses envelope encryption with random keys/nonces,
//! ciphertext bytes differ run to run. Each case therefore records the exact
//! `encryption_key_id`, a structural summary of the envelope (mechanism, key
//! name and field lengths) and the decrypted value or exact error text.
//!
//! Env:
//!
//! * `SG_M936A_KEYRING_CIPHERTEXT_DIFF=1`
//! * `SG_QTEST_DSN` — scratch database
//! * `M936A_CORPUS` — corpus JSON path
//! * `M936A_OUT` — output directory (one `<name>.json` per case)
//! * `M936A_KEY_SECRET` / `M936A_WRONG_SECRET` — 32-byte mounted key secrets

#![allow(clippy::unwrap_used)]

use std::collections::BTreeMap;
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::postgres::PgPoolOptions;
use sqlx::Row;

use sg_config::SiteConfiguration;
use sg_encryption::StorableEncryptedKey;
use sg_keyring::Keyring;
use sg_service_frontend::batches_credentials::BatchesCredentialsDatabase;
use sg_service_frontend::batches_graphql_store::PgBatchesGraphqlStore;
use sg_service_frontend::github_apps_graphql::GitHubAppsGraphqlDatabase;
use sg_service_frontend::github_apps_graphql_store::GitHubAppsPgDatabase;
use sg_service_frontend::githubapp_http_api::{
    GitHubAppStore, NewGitHubApp, PgGitHubAppStore, PgWebhookBackend, WebhookBackend,
};

#[derive(Deserialize)]
struct KeyConfig {
    name: String,
    version: String,
    env_var: String,
    wrong_env_var: String,
    other_name: String,
}

#[derive(Deserialize)]
struct Case {
    name: String,
    kind: String,
    #[serde(default)]
    write_key: String,
    read_key: String,
    #[serde(default)]
    plaintext: String,
    #[serde(default)]
    client_secret: String,
    #[serde(default)]
    private_key: String,
    #[serde(default)]
    update_plaintext: String,
    #[serde(default)]
    raw_key_id: Option<String>,
}

#[derive(Deserialize)]
struct Corpus {
    key: KeyConfig,
    cases: Vec<Case>,
}

#[derive(Serialize)]
struct Snapshot {
    cipher: BTreeMap<String, Value>,
    key_id: String,
    kind: String,
    read: BTreeMap<String, Value>,
}

fn string(value: impl Into<String>) -> Value {
    Value::String(value.into())
}

fn cipher_summary(raw: &str) -> Value {
    match StorableEncryptedKey::from_ciphertext(raw.as_bytes()) {
        Ok(sek) => {
            let mut map = BTreeMap::new();
            map.insert(
                "ciphertext_len".to_owned(),
                Value::from(sek.ciphertext.len() as u64),
            );
            map.insert("key_name".to_owned(), string(sek.key_name));
            map.insert("mechanism".to_owned(), string(sek.mechanism));
            map.insert("nonce_len".to_owned(), Value::from(sek.nonce.len() as u64));
            map.insert(
                "wrapped_key_len".to_owned(),
                Value::from(sek.wrapped_key.len() as u64),
            );
            serde_json::to_value(map).unwrap()
        }
        Err(err) => {
            let mut map = BTreeMap::new();
            map.insert("decode_error".to_owned(), string(err.to_string()));
            map.insert("raw_len".to_owned(), Value::from(raw.len() as u64));
            serde_json::to_value(map).unwrap()
        }
    }
}

fn ciphers(key_id: &str, pairs: &[(&str, &str)]) -> BTreeMap<String, Value> {
    let mut out = BTreeMap::new();
    let plaintext_row =
        key_id.is_empty() || key_id == sg_encryption::UNMIGRATED_ENCRYPTION_KEY_ID;
    for (field, raw) in pairs {
        let value = if plaintext_row {
            let mut plaintext = BTreeMap::new();
            plaintext.insert("plaintext".to_owned(), Value::Bool(true));
            serde_json::to_value(plaintext).unwrap()
        } else {
            cipher_summary(raw)
        };
        out.insert((*field).to_owned(), value);
    }
    out
}

async fn keyring(name: &str, version: &str, env_var: &str) -> Arc<Keyring> {
    let site = SiteConfiguration::parse(&format!(
        r#"{{"encryption.keys": {{
            "batchChangesCredentialKey": {{"type":"mounted","keyname":"{name}","version":"{version}","envVarName":"{env_var}"}},
            "gitHubAppKey": {{"type":"mounted","keyname":"{name}","version":"{version}","envVarName":"{env_var}"}},
            "webhookKey": {{"type":"mounted","keyname":"{name}","version":"{version}","envVarName":"{env_var}"}}
        }}}}"#
    ))
    .expect("site config");
    sg_keyring::init(&site).await.expect("keyring")
}

#[tokio::test(flavor = "multi_thread")]
async fn keyring_ciphertext_harness() {
    if std::env::var("SG_M936A_KEYRING_CIPHERTEXT_DIFF").as_deref() != Ok("1") {
        return;
    }
    let corpus_path = std::env::var("M936A_CORPUS").expect("M936A_CORPUS");
    let out = std::env::var("M936A_OUT").expect("M936A_OUT");
    let dsn = std::env::var("SG_QTEST_DSN").expect("SG_QTEST_DSN");
    std::fs::create_dir_all(&out).expect("out dir");
    let corpus: Corpus =
        serde_json::from_slice(&std::fs::read(&corpus_path).expect("corpus")).expect("corpus json");

    let pool = PgPoolOptions::new()
        .max_connections(4)
        .connect(&dsn)
        .await
        .expect("connect");

    let primary = keyring(&corpus.key.name, &corpus.key.version, &corpus.key.env_var).await;
    let wrong_secret =
        keyring(&corpus.key.name, &corpus.key.version, &corpus.key.wrong_env_var).await;
    let wrong_name =
        keyring(&corpus.key.other_name, &corpus.key.version, &corpus.key.env_var).await;
    let none = Arc::new(Keyring::default());

    let pick = |role: &str| -> Arc<Keyring> {
        match role {
            "primary" => primary.clone(),
            "none" => none.clone(),
            "wrong_secret" => wrong_secret.clone(),
            "wrong_name" => wrong_name.clone(),
            other => panic!("unknown key role {other}"),
        }
    };

    for stmt in [
        "INSERT INTO users (id, username) VALUES (936001, 'm936a-oracle') ON CONFLICT (id) DO NOTHING",
        "DELETE FROM user_credentials WHERE external_service_id LIKE 'm936a-%'",
        "DELETE FROM batch_changes_site_credentials WHERE external_service_id LIKE 'm936a-%'",
        "DELETE FROM github_apps WHERE name LIKE 'm936a-%'",
        "DELETE FROM webhooks WHERE name LIKE 'm936a-%'",
    ] {
        sqlx::query(stmt).execute(&pool).await.expect("clean");
    }

    for (index, case) in corpus.cases.iter().enumerate() {
        let scope_id = format!("m936a-{}", case.name);
        let mut read: BTreeMap<String, Value> = BTreeMap::new();
        let (key_id, cipher) = match case.kind.as_str() {
            "user_credential" => {
                if let Some(raw_key_id) = &case.raw_key_id {
                    sqlx::query(
                        "INSERT INTO user_credentials
                           (domain, user_id, external_service_type, external_service_id, credential,
                            encryption_key_id, github_app_id, created_at, updated_at, ssh_migration_applied)
                         VALUES ('batches', 936001, 'github', $1, $2, $3, NULL, NOW(), NOW(), TRUE)",
                    )
                    .bind(&scope_id)
                    .bind(case.plaintext.as_bytes())
                    .bind(raw_key_id)
                    .execute(&pool)
                    .await
                    .expect("raw insert");
                } else {
                    let write =
                        PgBatchesGraphqlStore::new(pool.clone()).with_keyring(pick(&case.write_key));
                    let (cipher, key_id) = write.encrypt_credential(&case.plaintext).expect("encrypt");
                    write
                        .create_user_credential(
                            936001,
                            "github",
                            &scope_id,
                            &cipher,
                            &key_id,
                            None,
                        )
                        .expect("create user credential");
                }
                let row = sqlx::query(
                    "SELECT credential, encryption_key_id FROM user_credentials
                     WHERE domain = 'batches' AND external_service_id = $1",
                )
                .bind(&scope_id)
                .fetch_one(&pool)
                .await
                .expect("select user credential");
                let raw: Vec<u8> = row.get("credential");
                let key_id: String = row.get("encryption_key_id");
                let raw = String::from_utf8_lossy(&raw).into_owned();

                let store =
                    PgBatchesGraphqlStore::new(pool.clone()).with_keyring(pick(&case.read_key));
                match store.get_user_credential_by_scope(936001, "github", &scope_id) {
                    Ok(Some(row)) => match store
                        .decrypt_credential(&row.credential, &row.encryption_key_id)
                    {
                        Ok(plaintext) => {
                            read.insert("credential".to_owned(), string(plaintext));
                        }
                        Err(err) => {
                            read.insert("error".to_owned(), string(err));
                        }
                    },
                    Ok(None) => {
                        read.insert("error".to_owned(), string("user credential not found"));
                    }
                    Err(err) => {
                        read.insert("error".to_owned(), string(err));
                    }
                }
                let cipher = ciphers(&key_id, &[("credential", &raw)]);
                (key_id, cipher)
            }
            "site_credential" => {
                let write =
                    PgBatchesGraphqlStore::new(pool.clone()).with_keyring(pick(&case.write_key));
                let (cipher, key_id) = write.encrypt_credential(&case.plaintext).expect("encrypt");
                write
                    .create_site_credential("github", &scope_id, &cipher, &key_id, None)
                    .expect("create site credential");
                let row = sqlx::query(
                    "SELECT credential, encryption_key_id FROM batch_changes_site_credentials
                     WHERE external_service_type = 'github' AND external_service_id = $1",
                )
                .bind(&scope_id)
                .fetch_one(&pool)
                .await
                .expect("select site credential");
                let raw: Vec<u8> = row.get("credential");
                let key_id: String = row.get("encryption_key_id");
                let raw = String::from_utf8_lossy(&raw).into_owned();

                let store =
                    PgBatchesGraphqlStore::new(pool.clone()).with_keyring(pick(&case.read_key));
                match store.get_site_credential_by_scope("github", &scope_id) {
                    Ok(Some(row)) => match store
                        .decrypt_credential(&row.credential, &row.encryption_key_id)
                    {
                        Ok(plaintext) => {
                            read.insert("credential".to_owned(), string(plaintext));
                        }
                        Err(err) => {
                            read.insert("error".to_owned(), string(err));
                        }
                    },
                    Ok(None) => {
                        read.insert("error".to_owned(), string("no results"));
                    }
                    Err(err) => {
                        read.insert("error".to_owned(), string(err));
                    }
                }
                let cipher = ciphers(&key_id, &[("credential", &raw)]);
                (key_id, cipher)
            }
            "github_app" => {
                let write = PgGitHubAppStore::new(pool.clone(), pick(&case.write_key));
                let app_id = write
                    .create(&NewGitHubApp {
                        app_id: 936001 + index as i32,
                        name: scope_id.clone(),
                        slug: format!("m936a-{}", case.name),
                        client_id: format!("client-id-{}", case.name),
                        client_secret: case.client_secret.clone(),
                        webhook_secret: String::new(),
                        private_key: case.private_key.clone(),
                        base_url: "https://github.com/".to_owned(),
                        app_url: format!("https://github.com/apps/m936a-{}", case.name),
                        domain: "repos".to_owned(),
                        kind: "REPO_SYNC".to_owned(),
                        logo: "logo.png".to_owned(),
                        created_by_user_id: 0,
                    })
                    .expect("create github app");
                let row = sqlx::query(
                    "SELECT client_secret, private_key, encryption_key_id
                     FROM github_apps WHERE id = $1",
                )
                .bind(app_id)
                .fetch_one(&pool)
                .await
                .expect("select github app");
                let client_secret: String = row.get("client_secret");
                let private_key: String = row.get("private_key");
                let key_id: String = row.get("encryption_key_id");

                let read_store =
                    GitHubAppsPgDatabase::new(pool.clone(), pick(&case.read_key));
                match read_store.get_app_by_id(i64::from(app_id)) {
                    Ok(Some(app)) => {
                        read.insert("client_secret".to_owned(), string(app.client_secret));
                        read.insert("private_key".to_owned(), string(app.private_key));
                    }
                    Ok(None) => {
                        read.insert("error".to_owned(), string("no github app found"));
                    }
                    Err(err) => {
                        read.insert("error".to_owned(), string(err));
                    }
                }
                let cipher = ciphers(
                    &key_id,
                    &[
                        ("client_secret", &client_secret),
                        ("private_key", &private_key),
                    ],
                );
                (key_id, cipher)
            }
            "webhook" => {
                let uuid = if let Some(raw_key_id) = &case.raw_key_id {
                    let key_id = if raw_key_id.is_empty() {
                        None
                    } else {
                        Some(raw_key_id.as_str())
                    };
                    sqlx::query(
                        "INSERT INTO webhooks
                           (name, code_host_kind, code_host_urn, secret, encryption_key_id)
                         VALUES ($1, 'GITHUB', 'https://github.com/', $2, $3)
                         RETURNING uuid::text AS uuid",
                    )
                    .bind(&scope_id)
                    .bind(&case.plaintext)
                    .bind(key_id)
                    .fetch_one(&pool)
                    .await
                    .expect("raw webhook insert")
                    .get("uuid")
                } else {
                    let backend =
                        PgWebhookBackend::new(pool.clone(), pick(&case.write_key), 0);
                    let uuid = backend
                        .create(&scope_id, "GITHUB", "https://github.com/")
                        .expect("create webhook");
                    let mut record = backend.get_by_uuid(&uuid).expect("fetch webhook");
                    record.secret = Some(if case.update_plaintext.is_empty() {
                        case.plaintext.clone()
                    } else {
                        case.update_plaintext.clone()
                    });
                    backend.update(&record).expect("update webhook");
                    uuid
                };
                let row = sqlx::query(
                    "SELECT secret, encryption_key_id FROM webhooks WHERE uuid = $1::uuid",
                )
                .bind(&uuid)
                .fetch_one(&pool)
                .await
                .expect("select webhook");
                let secret: Option<String> = row.get("secret");
                let key_id: Option<String> = row.get("encryption_key_id");
                let secret = secret.unwrap_or_default();

                let read_backend =
                    PgWebhookBackend::new(pool.clone(), pick(&case.read_key), 0);
                match read_backend.get_by_uuid(&uuid) {
                    Ok(record) => match record.secret {
                        Some(value) => {
                            read.insert("secret".to_owned(), string(value));
                        }
                        None => {
                            read.insert("secret".to_owned(), Value::Null);
                        }
                    },
                    Err(err) => {
                        read.insert("error".to_owned(), string(err));
                    }
                }
                let key_id = key_id.unwrap_or_default();
                let cipher = ciphers(&key_id, &[("secret", &secret)]);
                (key_id, cipher)
            }
            other => panic!("unknown kind {other}"),
        };

        let snapshot = Snapshot {
            cipher,
            key_id,
            kind: case.kind.clone(),
            read,
        };
        let encoded = serde_json::to_vec(&snapshot).expect("encode");
        std::fs::write(format!("{out}/{}.json", case.name), &encoded).expect("write");
        println!("RUST {} len={}", case.name, encoded.len());
    }
}
