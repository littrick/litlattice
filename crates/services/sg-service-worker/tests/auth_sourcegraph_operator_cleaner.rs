#![forbid(unsafe_code)]

//! Differential harness for the `auth-sourcegraph-operator-cleaner` job
//! (M9.8af).
//!
//! Mirror of the Go harness
//! `cmd/worker/internal/auth/sourcegraph_operator_cleaner_harness_test.go`:
//! same scratch database, same fixtures, same canonical `soap ` lines. Drives
//! the real [`clean_expired_sourcegraph_operator_users`] against the scratch
//! schema-only copy of the frontend database.
//!
//! Gated by `SG_SOAP_CLEANER_HARNESS_ON=1`; the connection comes from
//! `SG_SOAP_TEST_DSN`.

use std::time::Duration;

use sqlx::postgres::{PgPool, PgPoolOptions};
use sqlx::Row;

use sg_config::{cloud, LiveConfig, SiteConfiguration, Unified};
use sg_keyring::Keyring;
use sg_service_worker::jobs::auth_sourcegraph_operator_cleaner::clean_expired_sourcegraph_operator_users;

const SOAP: &str = "sourcegraph-operator";
const EXPIRED: &str = "2000-01-01T00:00:00Z";
const LIVE: &str = "2035-01-01T00:00:00Z";

async fn exec(pool: &PgPool, statement: &str) {
    sqlx::query(statement)
        .execute(pool)
        .await
        .unwrap_or_else(|err| panic!("{statement}: {err}"));
}

async fn cleanup(pool: &PgPool) {
    for statement in [
        "DELETE FROM event_logs",
        "DELETE FROM security_event_logs",
        "DELETE FROM user_roles WHERE user_id BETWEEN 8100 AND 8399",
        "DELETE FROM user_external_accounts WHERE user_id BETWEEN 8100 AND 8399",
        "DELETE FROM names WHERE user_id BETWEEN 8100 AND 8399",
        "DELETE FROM users WHERE id BETWEEN 8100 AND 8399",
    ] {
        exec(pool, statement).await;
    }
}

async fn insert_user(pool: &PgPool, id: i32, username: &str, admin: bool) {
    sqlx::query("INSERT INTO users (id, username, site_admin) VALUES ($1, $2, $3)")
        .bind(id)
        .bind(username)
        .bind(admin)
        .execute(pool)
        .await
        .unwrap_or_else(|err| panic!("insert user {id}: {err}"));
    sqlx::query("INSERT INTO names (name, user_id) VALUES ($1, $2)")
        .bind(username)
        .bind(id)
        .execute(pool)
        .await
        .unwrap_or_else(|err| panic!("insert name {username}: {err}"));
}

#[allow(clippy::too_many_arguments)]
async fn insert_account(
    pool: &PgPool,
    id: i32,
    user_id: i32,
    service_type: &str,
    account_id: &str,
    account_data: Option<&str>,
    created_at: &str,
    deleted_at: &str,
) {
    let deleted_at = if deleted_at.is_empty() {
        None
    } else {
        Some(deleted_at)
    };
    sqlx::query(
        r#"
INSERT INTO user_external_accounts (id, user_id, service_type, service_id, client_id, account_id, account_data, encryption_key_id, created_at, deleted_at)
VALUES ($1, $2, $3, 'https://sourcegraph.com', 'soap', $4, $5, '', $6::timestamptz, $7::timestamptz)"#,
    )
    .bind(id)
    .bind(user_id)
    .bind(service_type)
    .bind(account_id)
    .bind(account_data)
    .bind(created_at)
    .bind(deleted_at)
    .execute(pool)
    .await
    .unwrap_or_else(|err| panic!("insert account {id}: {err}"));
}

async fn add_admin(pool: &PgPool, user_id: i32) {
    sqlx::query("INSERT INTO user_roles (user_id, role_id) VALUES ($1, 2)")
        .bind(user_id)
        .execute(pool)
        .await
        .unwrap_or_else(|err| panic!("insert role {user_id}: {err}"));
}

async fn insert_fixtures(pool: &PgPool) {
    // 1. logan: no external accounts, not an admin -> untouched.
    insert_user(pool, 8101, "logan", false).await;
    // 2. morgan: expired SOAP + GitHub account, admin -> demoted, SOAP
    //    account deleted, GitHub account kept.
    insert_user(pool, 8102, "morgan", true).await;
    insert_account(pool, 8511, 8102, SOAP, "morgan", None, EXPIRED, "").await;
    insert_account(pool, 8512, 8102, "github", "morgan", None, LIVE, "").await;
    add_admin(pool, 8102).await;
    // 3. jordan: unexpired SOAP account, admin -> untouched.
    insert_user(pool, 8103, "jordan", true).await;
    insert_account(pool, 8513, 8103, SOAP, "jordan", None, LIVE, "").await;
    add_admin(pool, 8103).await;
    // 4. riley: expired SOAP account only, admin -> hard deleted.
    insert_user(pool, 8104, "riley", true).await;
    insert_account(pool, 8514, 8104, SOAP, "riley", None, EXPIRED, "").await;
    add_admin(pool, 8104).await;
    exec(
        pool,
        r#"
INSERT INTO event_logs (name, url, user_id, anonymous_user_id, source, argument, public_argument, version, timestamp)
VALUES ('UserActivity', 'https://example.com', 8104, '', 'WEB', '{"x":1}', '{}', '0.0.0+dev', now())"#,
    )
    .await;
    // 5. cris: only a GitHub account -> never selected.
    insert_user(pool, 8105, "cris", false).await;
    insert_account(pool, 8515, 8105, "github", "cris", None, LIVE, "").await;
    // 6. cami: expired SOAP service account, admin -> untouched.
    insert_user(pool, 8106, "cami", true).await;
    insert_account(
        pool,
        8516,
        8106,
        SOAP,
        "cami",
        Some(r#"{"serviceAccount":true}"#),
        EXPIRED,
        "",
    )
    .await;
    add_admin(pool, 8106).await;
    // 7. dani: no external accounts -> untouched.
    insert_user(pool, 8107, "dani", false).await;
    // 8. alex: one old soft-deleted SOAP account and one live unexpired SOAP
    //    account, admin -> untouched.
    insert_user(pool, 8108, "alex", true).await;
    insert_account(pool, 8517, 8108, SOAP, "alex", None, EXPIRED, LIVE).await;
    insert_account(pool, 8518, 8108, SOAP, "alex2", None, LIVE, "").await;
    add_admin(pool, 8108).await;
}

async fn insert_location_fixture(pool: &PgPool, id: i32) {
    let username = format!("location-{id}");
    insert_user(pool, id, &username, false).await;
    insert_account(pool, id + 200, id, SOAP, &username, None, EXPIRED, "").await;
}

fn mask_anon(anon: &str) -> &str {
    if anon.len() == 36 {
        "uuid"
    } else {
        anon
    }
}

async fn dump(pool: &PgPool) {
    let rows = sqlx::query(
        "SELECT id, username::text AS username, site_admin FROM users \
         WHERE id BETWEEN 8100 AND 8399 ORDER BY id",
    )
    .fetch_all(pool)
    .await
    .expect("select users");
    for row in rows {
        println!(
            "soap user id={} username={} admin={}",
            row.get::<i32, _>("id"),
            row.get::<String, _>("username"),
            row.get::<bool, _>("site_admin")
        );
    }

    let rows = sqlx::query(
        "SELECT user_id, service_type, coalesce(account_data, '<null>') AS account_data \
         FROM user_external_accounts \
         WHERE user_id BETWEEN 8100 AND 8399 AND deleted_at IS NULL \
         ORDER BY user_id, id",
    )
    .fetch_all(pool)
    .await
    .expect("select accounts");
    for row in rows {
        println!(
            "soap account user={} type={} data={}",
            row.get::<i32, _>("user_id"),
            row.get::<String, _>("service_type"),
            row.get::<String, _>("account_data")
        );
    }

    let rows = sqlx::query(
        "SELECT user_id, role_id FROM user_roles WHERE user_id BETWEEN 8100 AND 8399 \
         ORDER BY user_id, role_id",
    )
    .fetch_all(pool)
    .await
    .expect("select roles");
    for row in rows {
        println!(
            "soap role user={} role={}",
            row.get::<i32, _>("user_id"),
            row.get::<i32, _>("role_id")
        );
    }

    let rows = sqlx::query(
        "SELECT name, user_id, anonymous_user_id, source, argument::text AS argument, \
         coalesce(public_argument::text, '<null>') AS public_argument, version, \
         coalesce(feature_flags::text, '<null>') AS feature_flags \
         FROM event_logs ORDER BY id",
    )
    .fetch_all(pool)
    .await
    .expect("select event logs");
    for row in rows {
        let anon: String = row.get("anonymous_user_id");
        println!(
            "soap event name={} user={} anon={} source={} argument={} public={} version={} flags={}",
            row.get::<String, _>("name"),
            row.get::<i32, _>("user_id"),
            mask_anon(&anon),
            row.get::<String, _>("source"),
            row.get::<String, _>("argument"),
            row.get::<String, _>("public_argument"),
            row.get::<String, _>("version"),
            row.get::<String, _>("feature_flags")
        );
    }

    let rows = sqlx::query(
        "SELECT name, user_id, anonymous_user_id, source, argument::text AS argument, version \
         FROM security_event_logs ORDER BY id",
    )
    .fetch_all(pool)
    .await
    .expect("select security event logs");
    for row in rows {
        let anon: String = row.get("anonymous_user_id");
        println!(
            "soap sec name={} user={} anon={} source={} argument={} version={}",
            row.get::<String, _>("name"),
            row.get::<i32, _>("user_id"),
            mask_anon(&anon),
            row.get::<String, _>("source"),
            row.get::<String, _>("argument"),
            row.get::<String, _>("version")
        );
    }
}

fn live_config(site: &str) -> LiveConfig {
    LiveConfig::new(Unified {
        site: SiteConfiguration::parse(site).expect("valid site configuration"),
        ..Unified::default()
    })
}

async fn run(pool: &PgPool, live: &LiveConfig, label: &str) {
    let keyring = Keyring::default();
    let result = clean_expired_sourcegraph_operator_users(
        pool,
        &keyring,
        live,
        Duration::from_secs(60 * 60),
    )
    .await;
    let message = result.err().unwrap_or_else(|| "-".to_owned());
    println!("soap scenario={label} err={message}");
    dump(pool).await;
}

#[tokio::test]
async fn sourcegraph_operator_cleaner_harness() {
    if std::env::var("SG_SOAP_CLEANER_HARNESS_ON").as_deref() != Ok("1") {
        return;
    }
    let dsn = std::env::var("SG_SOAP_TEST_DSN").expect("SG_SOAP_TEST_DSN is required");
    let pool = PgPoolOptions::new()
        .max_connections(4)
        .connect(&dsn)
        .await
        .expect("connect scratch database");

    cleanup(&pool).await;

    // The scratch database is a schema-only copy, so the role rows migrations
    // seed are absent.
    exec(
        &pool,
        "INSERT INTO roles (id, name, system) VALUES (2, 'SITE_ADMINISTRATOR', true) ON CONFLICT (id) DO NOTHING",
    )
    .await;

    // Gate and lifecycle: the exact expressions `routines` uses.
    match cloud::site_config() {
        Ok(site) => println!(
            "soap gate=stub enabled={}",
            site.sourcegraph_operator_auth_provider_enabled()
        ),
        Err(err) => panic!("cloud site config: {err}"),
    }
    let operator = cloud::SchemaSiteConfig {
        auth_providers: Some(cloud::SchemaAuthProviders {
            sourcegraph_operator: Some(cloud::SchemaAuthProviderSourcegraphOperator::default()),
        }),
        site_config_allowlist: cloud::SiteConfigAllowlistSpec::default(),
    };
    println!(
        "soap gate=operator enabled={}",
        operator.sourcegraph_operator_auth_provider_enabled()
    );
    for minutes in [0, 1, 90] {
        println!(
            "soap lifecycle minutes={minutes} secs={}",
            cloud::lifecycle_duration(minutes).as_secs()
        );
    }

    let default_config = live_config("{}");

    // Nothing to clean up.
    run(&pool, &default_config, "empty").await;

    // The full fixture matrix.
    insert_fixtures(&pool).await;
    run(&pool, &default_config, "cleanup").await;

    // Security event location: database writes the audit row.
    cleanup(&pool).await;
    insert_location_fixture(&pool, 8301).await;
    let database = live_config(r#"{"log":{"securityEventLog":{"location":"database"}}}"#);
    run(&pool, &database, "location_database").await;

    // auditlog (the default) keeps the row out of the database.
    cleanup(&pool).await;
    insert_location_fixture(&pool, 8302).await;
    let auditlog = live_config(r#"{"log":{"securityEventLog":{"location":"auditlog"}}}"#);
    run(&pool, &auditlog, "location_auditlog").await;

    cleanup(&pool).await;
}
