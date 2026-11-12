#![forbid(unsafe_code)]

//! M9.22a differential harness (Rust side): account-lifecycle security and
//! analytics events (`logAccountCreatedEvent`, `logUserDeletionEvents`).
//!
//! The Go oracle is the untracked
//! `internal/database/zz_m922a_account_lifecycle_oracle_test.go`, run by
//! `test_ws/p9-go/run_account_lifecycle_diff.sh`. That script applies
//! `data/account_lifecycle/fixtures.sql` immediately before each side and
//! passes the shared case list via `M922A_LIFECYCLE_CASES`.

#![allow(clippy::unwrap_used)]

use serde::Deserialize;
use sg_service_frontend::admin_store::{AdminStore, NewUser, PgAdminStore};
use sg_service_frontend::user_provisioning::{
    ExternalIdentity, NewUserRow, PgProvisionStore, ProvisionStore,
};
use sqlx::postgres::PgPool;
use sqlx::Row;

#[derive(Debug, Deserialize)]
struct Case {
    name: String,
    kind: String,
    #[serde(default)]
    user_id: i32,
    #[serde(default)]
    username: String,
    #[serde(default)]
    email: String,
    #[serde(default = "default_true")]
    email_verified: bool,
    #[serde(default)]
    creator: i32,
    #[serde(default)]
    deleter: i32,
    #[serde(default)]
    service_type: String,
    #[serde(default)]
    service_id: String,
    #[serde(default)]
    account_id: String,
}

fn default_true() -> bool {
    true
}

async fn snapshot(pool: &PgPool) {
    for row in sqlx::query(
        "SELECT name, user_id, anonymous_user_id, source, argument::text AS arg, version \
         FROM security_event_logs \
         WHERE name IN ('AccountCreated', 'AccountDeleted', 'AccountNuked') \
           AND (user_id BETWEEN 9910 AND 10099 \
                OR COALESCE((argument->>'created')::int, 0) BETWEEN 9910 AND 10099) \
         ORDER BY name, user_id, argument::text",
    )
    .fetch_all(pool)
    .await
    .expect("select security_event_logs")
    {
        let name: String = row.try_get("name").expect("name");
        let user_id: i32 = row.try_get("user_id").expect("user_id");
        let anonymous_user_id: String = row
            .try_get("anonymous_user_id")
            .expect("anonymous_user_id");
        let source: String = row.try_get("source").expect("source");
        let arg: String = row.try_get("arg").expect("arg");
        let version: String = row.try_get("version").expect("version");
        println!(
            "al sec name={name} uid={user_id} anon={anonymous_user_id} source={source} arg={arg} version={version}"
        );
    }

    for row in sqlx::query(
        "SELECT name, user_id, anonymous_user_id, source, argument::text AS arg \
         FROM event_logs \
         WHERE name IN ('AccountCreated', 'AccountDeleted', 'AccountNuked') \
           AND (COALESCE((argument->>'created')::int, 0) BETWEEN 9910 AND 10099 \
                OR COALESCE((argument->>'deleted')::int, 0) BETWEEN 9910 AND 10099) \
         ORDER BY name, argument::text",
    )
    .fetch_all(pool)
    .await
    .expect("select event_logs")
    {
        let name: String = row.try_get("name").expect("name");
        let user_id: i32 = row.try_get("user_id").expect("user_id");
        let anonymous_user_id: String = row
            .try_get("anonymous_user_id")
            .expect("anonymous_user_id");
        let source: String = row.try_get("source").expect("source");
        let arg: String = row.try_get("arg").expect("arg");
        println!("al evt name={name} uid={user_id} anon={anonymous_user_id} source={source} arg={arg}");
    }
}

#[tokio::test]
async fn account_lifecycle_diff() {
    let Ok(dsn) = std::env::var("SG_QTEST_DSN") else {
        eprintln!("SG_QTEST_DSN is not set; skipping");
        return;
    };
    let cases_path =
        std::env::var("M922A_LIFECYCLE_CASES").expect("M922A_LIFECYCLE_CASES is not set");
    let data = std::fs::read_to_string(&cases_path).expect("read cases");
    let cases: Vec<Case> = serde_json::from_str(&data).expect("parse cases");
    let pool = PgPool::connect(&dsn).await.expect("connect");

    for case in cases {
        let error = match case.kind.as_str() {
            "create" => {
                let store = PgAdminStore::new(pool.clone())
                    .with_permissions_bind_id("email")
                    .with_security_event_logs_in_database(true);
                store
                    .create_user_as(
                        &NewUser {
                            username: case.username.clone(),
                            password: String::new(),
                            email: case.email.clone(),
                            email_is_verified: case.email_verified,
                            email_verification_code: None,
                        },
                        case.creator,
                    )
                    .await
                    .is_err()
            }
            "create_external" => {
                let store = PgProvisionStore::new(pool.clone())
                    .with_permissions_bind_id("email")
                    .with_security_event_logs_in_database(true);
                let new_user = NewUserRow {
                    username: &case.username,
                    display_name: "",
                    avatar_url: "",
                    email: &case.email,
                    email_is_verified: case.email_verified,
                };
                let identity = ExternalIdentity {
                    service_type: &case.service_type,
                    service_id: &case.service_id,
                    client_id: "",
                    account_id: &case.account_id,
                    login: &case.username,
                    email: &case.email,
                    email_verified: case.email_verified,
                    display_name: "",
                    avatar_url: "",
                    create_if_not_exist: true,
                };
                store
                    .create_with_external_account(&new_user, &identity)
                    .await
                    .is_err()
            }
            "delete" | "hard_delete" => {
                let store = PgAdminStore::new(pool.clone())
                    .with_permissions_bind_id("email")
                    .with_security_event_logs_in_database(true);
                store
                    .delete_users_as(
                        &[case.user_id],
                        case.kind == "hard_delete",
                        case.deleter,
                    )
                    .await
                    .is_err()
            }
            other => panic!("unknown case kind {other}"),
        };
        println!("al case={} error={}", case.name, error);
        snapshot(&pool).await;
    }
}
