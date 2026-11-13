#![forbid(unsafe_code)]

//! M9.24d differential harness (Rust side): initial site administrator and
//! `global_state` initialization semantics for the three user-creation paths.
//!
//! The Go oracle is the untracked
//! `internal/database/zz_m924d_initial_admin_oracle_test.go`, run by
//! `test_ws/p9-go/run_initial_site_admin_diff.sh`, against a real Postgres
//! scratch database. Both sides replay `data/initial_site_admin/cases.json`
//! and emit normalized `isa ` lines compared byte for byte:
//!
//! - `users.site_admin` after each case (first user on an uninitialized site
//!   is a site admin; soft-deleted rows still count for
//!   `NOT EXISTS(SELECT * FROM users)`);
//! - `global_state.initialized` and the row count;
//! - `user_roles` rows (USER plus SITE_ADMINISTRATOR for the initial admin);
//! - `userpasswd` error code, HTTP status and message (Go `unsafeSignUp`
//!   mapping: both new `ErrCannotCreateUser` codes fall through to the generic
//!   500 `Signup failed unexpectedly.`).

#![allow(clippy::unwrap_used)]

use serde::Deserialize;
use sg_service_frontend::admin_store::{AdminStore, NewUser, PgAdminStore};
use sg_service_frontend::user_provisioning::{
    ExternalIdentity, NewUserRow, PgProvisionStore, ProvisionStore,
};
use sg_service_frontend::userpasswd::{
    signup_error_status_message, store_error_code, NewUserData, PgUserpasswdStore,
    UserpasswdStore,
};
use sqlx::postgres::PgPool;
use sqlx::Row;

#[derive(Debug, Deserialize)]
struct Case {
    name: String,
    kind: String,
    #[serde(default)]
    username: String,
    #[serde(default)]
    email: String,
    #[serde(default)]
    email_verified: bool,
    #[serde(default)]
    fail_if_not_initial_user: bool,
    #[serde(default)]
    service_type: String,
    #[serde(default)]
    service_id: String,
    #[serde(default)]
    account_id: String,
    /// `sql` cases: statements executed before the next creation case.
    #[serde(default)]
    sql: Vec<String>,
}

async fn snapshot(pool: &PgPool) {
    for row in sqlx::query("SELECT username, site_admin FROM users ORDER BY username")
        .fetch_all(pool)
        .await
        .expect("select users")
    {
        let username: String = row.try_get("username").expect("username");
        let site_admin: bool = row.try_get("site_admin").expect("site_admin");
        println!("isa user username={username} site_admin={site_admin}");
    }

    let row = sqlx::query(
        "SELECT COUNT(*) AS rows, coalesce(bool_or(initialized), false) AS initialized \
         FROM global_state",
    )
    .fetch_one(pool)
    .await
    .expect("select global_state");
    let rows: i64 = row.try_get("rows").expect("rows");
    let initialized: bool = row.try_get("initialized").expect("initialized");
    println!("isa global rows={rows} initialized={initialized}");

    for row in sqlx::query(
        "SELECT u.username, r.name \
         FROM user_roles ur \
         JOIN users u ON u.id = ur.user_id \
         JOIN roles r ON r.id = ur.role_id \
         ORDER BY u.username, r.name",
    )
    .fetch_all(pool)
    .await
    .expect("select user_roles")
    {
        let username: String = row.try_get("username").expect("username");
        let name: String = row.try_get("name").expect("name");
        println!("isa role username={username} name={name}");
    }
}

#[tokio::test]
async fn initial_site_admin_diff() {
    let Ok(dsn) = std::env::var("SG_QTEST_DSN") else {
        eprintln!("SG_QTEST_DSN is not set; skipping");
        return;
    };
    let cases_path =
        std::env::var("M924D_INITIAL_ADMIN_CASES").expect("M924D_INITIAL_ADMIN_CASES is not set");
    let data = std::fs::read_to_string(&cases_path).expect("read cases");
    let cases: Vec<Case> = serde_json::from_str(&data).expect("parse cases");
    let pool = PgPool::connect(&dsn).await.expect("connect");

    for case in cases {
        match case.kind.as_str() {
            "sql" => {
                for statement in &case.sql {
                    sqlx::query(statement)
                        .execute(&pool)
                        .await
                        .unwrap_or_else(|error| panic!("{}: {error}", case.name));
                }
                println!("isa case={} kind=sql", case.name);
            }
            "create" => {
                // `Mutation.createUser` -> `Users().Create`
                // (`cmd/frontend/graphqlbackend/users_create.go:63`).
                let store = PgAdminStore::new(pool.clone());
                let error = store
                    .create_user_as(
                        &NewUser {
                            username: case.username.clone(),
                            password: String::new(),
                            email: case.email.clone(),
                            email_is_verified: case.email_verified,
                            email_verification_code: None,
                        },
                        0,
                    )
                    .await
                    .err();
                println!(
                    "isa case={} kind=create ok={}",
                    case.name,
                    error.is_none()
                );
            }
            "create_external" => {
                // `auth.GetAndSaveUser` -> `CreateWithExternalAccount`
                // (`cmd/frontend/auth/user.go:157`), never passing
                // `FailIfNotInitialUser`.
                let store = PgProvisionStore::new(pool.clone());
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
                let error = store
                    .create_with_external_account(&new_user, &identity)
                    .await
                    .err();
                println!(
                    "isa case={} kind=create_external ok={}",
                    case.name,
                    error.is_none()
                );
            }
            "userpasswd_create" => {
                // `pgUserpasswdStore.create_user` (Go `Users().Create` via
                // `cmd/frontend/internal/auth/userpasswd/handlers.go:257`).
                let store = PgUserpasswdStore::new(pool.clone(), "email");
                let result = store
                    .create_user(&NewUserData {
                        email: case.email.clone(),
                        username: case.username.clone(),
                        password_hash: None,
                        fail_if_not_initial_user: case.fail_if_not_initial_user,
                        email_is_verified: case.email_verified,
                        email_verification_code: None,
                    })
                    .await;
                match result {
                    Ok(_) => println!(
                        "isa case={} kind=userpasswd code=ok status=200 message=",
                        case.name
                    ),
                    Err(error) => {
                        let code = store_error_code(&error);
                        let (status, message) = signup_error_status_message(&error);
                        println!(
                            "isa case={} kind=userpasswd code={} status={} message={}",
                            case.name,
                            code,
                            status.as_u16(),
                            message
                        );
                    }
                }
            }
            other => panic!("unknown case kind {other}"),
        }
        snapshot(&pool).await;
    }
}
