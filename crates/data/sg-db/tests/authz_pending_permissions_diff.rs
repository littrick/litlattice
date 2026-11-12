#![forbid(unsafe_code)]

//! M9.21a differential harness (Rust side):
//! `AuthzStore.GrantPendingPermissions` (`internal/database/authz.go`).
//!
//! Mirrors the untracked Go oracle
//! `internal/database/zz_m921a_authz_pending_permissions_oracle_test.go`.
//! `test_ws/p9-go/run_authz_pending_permissions_diff.sh` applies
//! `data/authz_pending_permissions/fixtures.sql` immediately before each side
//! and passes the shared case list via `M921A_CASES`.

use serde::Deserialize;
use sg_db::authz::grant_pending_permissions;
use sqlx::postgres::PgPool;
use sqlx::Row;
use time::OffsetDateTime;

#[derive(Debug, Deserialize)]
struct Case {
    name: String,
    user_id: i32,
    config_bind_id: String,
}

/// `conf.PermissionsUserMapping().BindID`: missing/invalid -> `email`
/// (`internal/conf/computed.go:1445-1458`). The Go oracle gets this from
/// `conf.Mock`; the adapter itself receives the normalized value.
fn normalize_bind_id(raw: &str) -> &str {
    match raw {
        "email" | "username" => raw,
        _ => "email",
    }
}

fn ids(column: &str) -> String {
    format!(
        "COALESCE(array_to_string(ARRAY(SELECT unnest({column}) ORDER BY 1), ','), '') AS ids"
    )
}

async fn snapshot(pool: &PgPool) {
    let query = format!(
        r#"
SELECT user_id, permission, object_type, {}
FROM user_permissions
WHERE user_id BETWEEN 9810 AND 9899
ORDER BY user_id, permission, object_type
"#,
        ids("object_ids_ints")
    );
    for row in sqlx::query(&query)
        .fetch_all(pool)
        .await
        .expect("select user_permissions")
    {
        let user_id: i32 = row.try_get("user_id").expect("user_id");
        let permission: String = row.try_get("permission").expect("permission");
        let object_type: String = row.try_get("object_type").expect("object_type");
        let ids: String = row.try_get("ids").expect("ids");
        println!("ap up uid={user_id} perm={permission} type={object_type} ids={ids}");
    }

    let query = format!(
        r#"
SELECT repo_id, permission, unrestricted, {}
FROM repo_permissions
WHERE repo_id BETWEEN 9810 AND 9899
ORDER BY repo_id, permission
"#,
        ids("user_ids_ints")
    );
    for row in sqlx::query(&query)
        .fetch_all(pool)
        .await
        .expect("select repo_permissions")
    {
        let repo_id: i32 = row.try_get("repo_id").expect("repo_id");
        let permission: String = row.try_get("permission").expect("permission");
        let unrestricted: bool = row.try_get("unrestricted").expect("unrestricted");
        let ids: String = row.try_get("ids").expect("ids");
        println!(
            "ap rp repo={repo_id} perm={permission} unrestricted={unrestricted} ids={ids}"
        );
    }

    for row in sqlx::query(
        r#"
SELECT COALESCE(user_id::text, '-') AS user_id,
       COALESCE(user_external_account_id::text, '-') AS external_account_id,
       repo_id, source
FROM user_repo_permissions
WHERE user_id BETWEEN 9810 AND 9899 OR repo_id BETWEEN 9810 AND 9899
ORDER BY user_id NULLS FIRST, user_external_account_id NULLS FIRST, repo_id
"#,
    )
    .fetch_all(pool)
    .await
    .expect("select user_repo_permissions")
    {
        let user_id: String = row.try_get("user_id").expect("user_id");
        let external_account_id: String = row
            .try_get("external_account_id")
            .expect("external_account_id");
        let repo_id: i32 = row.try_get("repo_id").expect("repo_id");
        let source: String = row.try_get("source").expect("source");
        println!(
            "ap urp uid={user_id} ext={external_account_id} repo={repo_id} source={source}"
        );
    }

    let query = format!(
        r#"
SELECT id, bind_id, service_type, service_id, {}
FROM user_pending_permissions
WHERE id BETWEEN 98000 AND 98999
ORDER BY id
"#,
        ids("object_ids_ints")
    );
    for row in sqlx::query(&query)
        .fetch_all(pool)
        .await
        .expect("select user_pending_permissions")
    {
        let id: i64 = row.try_get("id").expect("id");
        let bind_id: String = row.try_get("bind_id").expect("bind_id");
        let service_type: String = row.try_get("service_type").expect("service_type");
        let service_id: String = row.try_get("service_id").expect("service_id");
        let ids: String = row.try_get("ids").expect("ids");
        println!(
            "ap upp id={id} bind={bind_id} st={service_type} sid={service_id} ids={ids}"
        );
    }

    let query = format!(
        r#"
SELECT repo_id, permission, {}
FROM repo_pending_permissions
WHERE repo_id BETWEEN 9810 AND 9899
ORDER BY repo_id, permission
"#,
        ids("user_ids_ints")
    );
    for row in sqlx::query(&query)
        .fetch_all(pool)
        .await
        .expect("select repo_pending_permissions")
    {
        let repo_id: i32 = row.try_get("repo_id").expect("repo_id");
        let permission: String = row.try_get("permission").expect("permission");
        let ids: String = row.try_get("ids").expect("ids");
        println!("ap rpp repo={repo_id} perm={permission} ids={ids}");
    }
}

#[tokio::test]
async fn authz_pending_permissions_diff() {
    let Ok(dsn) = std::env::var("SG_QTEST_DSN") else {
        eprintln!("SG_QTEST_DSN is not set; skipping");
        return;
    };
    let cases_path = std::env::var("M921A_CASES").expect("M921A_CASES is not set");
    let data = std::fs::read_to_string(&cases_path).expect("read cases");
    let cases: Vec<Case> = serde_json::from_str(&data).expect("parse cases");
    let pool = PgPool::connect(&dsn).await.expect("connect");
    let now = OffsetDateTime::from_unix_timestamp(1_780_272_000).expect("fixed now");

    for case in cases {
        let bind_id = normalize_bind_id(&case.config_bind_id);
        let result = grant_pending_permissions(&pool, case.user_id, bind_id, now).await;
        println!("ap case={} error={}", case.name, result.is_err());
        snapshot(&pool).await;
    }
}
