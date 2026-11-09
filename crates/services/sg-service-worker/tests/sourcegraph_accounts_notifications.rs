//! M9.8bm differential harness: the
//! `sourcegraph-accounts-notifications-subscriber` job.
//!
//! Mirrors `cmd/worker/internal/sourcegraphaccounts/notifications_subscriber_harness_test.go`.
//! Only runs when `SG_SAMS_TEST_DSN` names the scratch frontend database.
//! Prints canonical `sams ` lines.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use sg_config::{LiveConfig, SiteConfiguration, Unified};
use sg_db::user_external_accounts::{ExternalAccount, ExternalAccountsListOptions};
use sg_keyring::Keyring;
use sqlx::PgPool;

use sg_service_worker::jobs::sourcegraph_accounts_notifications::{
    on_user_deleted, subscriber_routines, DbNotificationsStore, NotificationsStore,
    NotificationsSubscriberConfig, SamsProvider, SamsUser,
};
use sg_service_worker::jobs::{JobConfig, JobDeps};

const HARNESS_ISSUER: &str = "https://accounts.sourcegraph.com";

fn provider() -> SamsProvider {
    SamsProvider {
        provider_type: "openidconnect".to_owned(),
        issuer: HARNESS_ISSUER.to_owned(),
        client_id: "sams_cid_harness".to_owned(),
        client_secret: "sams-secret".to_owned(),
    }
}

fn format_ids(ids: &[i32]) -> String {
    ids.iter().map(i32::to_string).collect::<Vec<_>>().join(",")
}

fn format_options(options: &ExternalAccountsListOptions) -> String {
    let user_id = options
        .user_id
        .map(|id| id.to_string())
        .unwrap_or_else(|| "-".to_owned());
    format!(
        "{}|{}|{}|{}",
        options.service_type.clone().unwrap_or_default(),
        options.service_id.clone().unwrap_or_default(),
        options.account_id.clone().unwrap_or_default(),
        user_id
    )
}

/// `notificationsSubscriberStore` scripted like the Go harness's
/// `scriptedStore`.
#[derive(Default)]
struct ScriptedStore {
    user_exists: bool,
    user_err: Option<String>,
    list_result: Vec<ExternalAccount>,
    list_err: Option<String>,
    delete_err: Option<String>,
    db: Option<DbNotificationsStore>,

    list_calls: AtomicUsize,
    delete_calls: AtomicUsize,
    last_options: Mutex<Option<ExternalAccountsListOptions>>,
    deleted_ids: Mutex<Vec<i32>>,
}

impl ScriptedStore {
    fn account(user_id: i32, account_id: &str) -> ExternalAccount {
        ExternalAccount {
            id: 0,
            user_id,
            service_type: "openidconnect".to_owned(),
            service_id: HARNESS_ISSUER.to_owned(),
            client_id: "sams_cid_a".to_owned(),
            account_id: account_id.to_owned(),
            auth_data: None,
            account_data: None,
            encryption_key_id: String::new(),
            expired: false,
        }
    }
}

#[async_trait]
impl NotificationsStore for ScriptedStore {
    async fn get_sams_user_by_id(&self, account_id: &str) -> Result<Option<SamsUser>, String> {
        if let Some(err) = &self.user_err {
            return Err(err.clone());
        }
        if self.user_exists {
            return Ok(Some(SamsUser {
                id: account_id.to_owned(),
            }));
        }
        Ok(None)
    }

    async fn list_user_external_accounts(
        &self,
        options: &ExternalAccountsListOptions,
    ) -> Result<Vec<ExternalAccount>, String> {
        self.list_calls.fetch_add(1, Ordering::SeqCst);
        *self.last_options.lock().expect("options mutex") = Some(options.clone());
        if let Some(err) = &self.list_err {
            return Err(err.clone());
        }
        if let Some(db) = &self.db {
            return db.list_user_external_accounts(options).await;
        }
        Ok(self.list_result.clone())
    }

    async fn hard_delete_users(&self, user_ids: &[i32]) -> Result<(), String> {
        self.delete_calls.fetch_add(1, Ordering::SeqCst);
        *self.deleted_ids.lock().expect("ids mutex") = user_ids.to_vec();
        if let Some(err) = &self.delete_err {
            return Err(err.clone());
        }
        if let Some(db) = &self.db {
            return db.hard_delete_users(user_ids).await;
        }
        Ok(())
    }
}

async fn run_handler_case(label: &str, store: &ScriptedStore, account_id: &str) {
    let result = on_user_deleted(store, &provider(), account_id).await;
    let message = result.err().unwrap_or_else(|| "-".to_owned());
    let list_calls = store.list_calls.load(Ordering::SeqCst);
    let delete_calls = store.delete_calls.load(Ordering::SeqCst);
    let deleted = if delete_calls > 0 {
        format_ids(&store.deleted_ids.lock().expect("ids mutex"))
    } else {
        "-".to_owned()
    };
    let options = store
        .last_options
        .lock()
        .expect("options mutex")
        .clone()
        .unwrap_or_default();
    println!(
        "sams handler case={label} account={account_id} err={message} list-calls={list_calls} delete-calls={delete_calls} deleted={deleted} opts={}",
        format_options(&options)
    );
}

async fn exec(pool: &PgPool, query: &str) {
    sqlx::query(query).execute(pool).await.expect("exec");
}

const CLEANUP_SQL: [&str; 5] = [
    "DELETE FROM event_logs WHERE user_id BETWEEN 8200 AND 8299",
    "DELETE FROM security_event_logs WHERE user_id BETWEEN 8200 AND 8299",
    "DELETE FROM user_external_accounts WHERE user_id BETWEEN 8200 AND 8299",
    "DELETE FROM names WHERE user_id BETWEEN 8200 AND 8299",
    "DELETE FROM users WHERE id BETWEEN 8200 AND 8299",
];

async fn cleanup(pool: &PgPool) {
    for query in CLEANUP_SQL {
        exec(pool, query).await;
    }
}

async fn seed_user(pool: &PgPool, id: i32, username: &str) {
    sqlx::query("INSERT INTO users (id, username, site_admin) VALUES ($1, $2, false)")
        .bind(id)
        .bind(username)
        .execute(pool)
        .await
        .expect("insert user");
    sqlx::query("INSERT INTO names (name, user_id) VALUES ($1, $2)")
        .bind(username)
        .bind(id)
        .execute(pool)
        .await
        .expect("insert name");
}

#[allow(clippy::too_many_arguments)]
async fn seed_account(
    pool: &PgPool,
    id: i32,
    user_id: i32,
    service_type: &str,
    service_id: &str,
    client_id: &str,
    account_id: &str,
    deleted: bool,
) {
    let deleted_at: Option<&str> = deleted.then_some("2035-01-01T00:00:00Z");
    sqlx::query(
        "INSERT INTO user_external_accounts (id, user_id, service_type, service_id, client_id, account_id, encryption_key_id, created_at, deleted_at)
         VALUES ($1, $2, $3, $4, $5, $6, '', '2020-01-01T00:00:00Z', $7::timestamptz)",
    )
    .bind(id)
    .bind(user_id)
    .bind(service_type)
    .bind(service_id)
    .bind(client_id)
    .bind(account_id)
    .bind(deleted_at)
    .execute(pool)
    .await
    .expect("insert account");
}

async fn insert_fixtures(pool: &PgPool) {
    seed_user(pool, 8201, "user-8201").await;
    seed_account(
        pool,
        8521,
        8201,
        "openidconnect",
        HARNESS_ISSUER,
        "sams_cid_a",
        "acc-real",
        false,
    )
    .await;
    seed_user(pool, 8202, "user-8202").await;
    seed_account(
        pool,
        8522,
        8202,
        "openidconnect",
        HARNESS_ISSUER,
        "sams_cid_b",
        "acc-real",
        false,
    )
    .await;
    seed_user(pool, 8203, "user-8203").await;
    seed_account(
        pool,
        8523,
        8203,
        "openidconnect",
        HARNESS_ISSUER,
        "sams_cid_a",
        "acc-other",
        false,
    )
    .await;
    seed_user(pool, 8204, "user-8204").await;
    seed_account(
        pool,
        8524,
        8204,
        "openidconnect",
        "https://other-issuer.example.com",
        "sams_cid_a",
        "acc-real",
        false,
    )
    .await;
    seed_user(pool, 8205, "user-8205").await;
    seed_account(
        pool,
        8525,
        8205,
        "github",
        HARNESS_ISSUER,
        "",
        "acc-real",
        false,
    )
    .await;
    seed_user(pool, 8206, "user-8206").await;
    seed_account(
        pool,
        8526,
        8206,
        "openidconnect",
        HARNESS_ISSUER,
        "sams_cid_a",
        "acc-real",
        true,
    )
    .await;
}

async fn query_ids(pool: &PgPool, query: &str) -> String {
    let ids: Vec<i32> = sqlx::query_scalar(query)
        .fetch_all(pool)
        .await
        .expect("query ids");
    format_ids(&ids)
}

#[tokio::test]
async fn sams_notifications_harness() {
    let Ok(dsn) = std::env::var("SG_SAMS_TEST_DSN") else {
        return;
    };
    let pool = sg_db::connect(&dsn, 2).await.expect("connect");

    // Gate / Routines scenarios. The missing-file path is never created.
    const MISSING_CREDENTIALS_FILE: &str = "/nonexistent/sams-creds.json";
    let cases: [(&str, bool, &str, &str); 5] = [
        ("not-dotcom", false, "", "{}"),
        ("no-credentials", true, "", "{}"),
        (
            "no-provider",
            true,
            MISSING_CREDENTIALS_FILE,
            r#"{"auth.providers": [{"type": "github", "clientID": "sams_cid_not_oidc", "clientSecret": "x"}]}"#,
        ),
        (
            "non-sams-oidc",
            true,
            MISSING_CREDENTIALS_FILE,
            r#"{"auth.providers": [{"type": "openidconnect", "clientID": "other", "clientSecret": "x", "issuer": "https://idp.example.com"}]}"#,
        ),
        (
            "sams-missing-creds",
            true,
            MISSING_CREDENTIALS_FILE,
            r#"{"auth.providers": [{"type": "openidconnect", "clientID": "sams_cid_harness", "clientSecret": "sams-secret", "issuer": "https://accounts.sourcegraph.com"}]}"#,
        ),
    ];
    for (label, dotcom, credentials, site_json) in cases {
        std::env::set_var("SOURCEGRAPH_ACCOUNTS_CREDENTIALS_FILE", credentials);
        let config = NotificationsSubscriberConfig::default();
        config.load(&mut sg_core::EnvReader::new());
        let deps = JobDeps {
            frontend: pool.clone(),
            live_config: Arc::new(LiveConfig::new(Unified {
                site: SiteConfiguration::parse(site_json).expect("site config"),
                ..Unified::default()
            })),
            keyring: Arc::new(Keyring::default()),
        };
        let result = subscriber_routines(&deps, dotcom, &config).await;
        let (count, message) = match result {
            Ok(routines) => (routines.len(), "-".to_owned()),
            Err(err) => (0, err),
        };
        println!(
            "sams routines case={label} dotcom={dotcom} creds={} routines={count} err={message}",
            !credentials.is_empty()
        );
    }
    std::env::remove_var("SOURCEGRAPH_ACCOUNTS_CREDENTIALS_FILE");

    // Scripted handler scenarios (no database access).
    run_handler_case(
        "exists",
        &ScriptedStore {
            user_exists: true,
            ..ScriptedStore::default()
        },
        "acc-exists",
    )
    .await;
    run_handler_case(
        "lookup-error",
        &ScriptedStore {
            user_err: Some("sams unavailable".to_owned()),
            ..ScriptedStore::default()
        },
        "acc-lookup",
    )
    .await;
    run_handler_case(
        "list-error",
        &ScriptedStore {
            list_err: Some("boom".to_owned()),
            ..ScriptedStore::default()
        },
        "acc-list",
    )
    .await;
    run_handler_case(
        "delete-error",
        &ScriptedStore {
            list_result: vec![
                ScriptedStore::account(811, "acc-delete"),
                ScriptedStore::account(812, "acc-delete"),
            ],
            delete_err: Some("boom".to_owned()),
            ..ScriptedStore::default()
        },
        "acc-delete",
    )
    .await;
    run_handler_case("no-accounts", &ScriptedStore::default(), "acc-none").await;

    // Real database scenarios.
    for (label, account_id) in [("match", "acc-real"), ("no-match", "acc-none")] {
        cleanup(&pool).await;
        insert_fixtures(&pool).await;
        let store = ScriptedStore {
            db: Some(DbNotificationsStore::new(
                pool.clone(),
                Arc::new(LiveConfig::new(Unified::default())),
            )),
            ..ScriptedStore::default()
        };
        let result = on_user_deleted(&store, &provider(), account_id).await;
        let message = result.err().unwrap_or_else(|| "-".to_owned());
        let matched = format_ids(&store.deleted_ids.lock().expect("ids mutex"));
        let remaining = query_ids(
            &pool,
            "SELECT id FROM users WHERE id BETWEEN 8200 AND 8299 ORDER BY id",
        )
        .await;
        let accounts = query_ids(
            &pool,
            "SELECT id FROM user_external_accounts WHERE user_id BETWEEN 8200 AND 8299 ORDER BY id",
        )
        .await;
        let names = query_ids(
            &pool,
            "SELECT user_id FROM names WHERE user_id BETWEEN 8200 AND 8299 ORDER BY user_id",
        )
        .await;
        println!(
            "sams db case={label} account={account_id} err={message} matched={matched} remaining={remaining} accounts={accounts} names={names}"
        );
        cleanup(&pool).await;
    }
}
