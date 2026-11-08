#![forbid(unsafe_code)]

//! M9.8ao differential harness (Rust side): the pending-permissions path.
//!
//! Mirrors `internal/database/pending_perms_harness_test.go`.
//! `test_ws/p9-go/run_pending_perms_diff.sh` applies
//! `pending_perms_fixtures.sql` immediately before each side.

use sg_db::perms_store::{
    grant_pending_permissions, list_pending_users, set_repo_pending_permissions,
    UserIdWithExternalAccountId, PERM_READ,
};
use sqlx::postgres::PgPool;
use sqlx::Row;
use time::OffsetDateTime;

fn ids(column: &str) -> String {
    format!(
        "COALESCE(array_to_string(ARRAY(SELECT unnest({column}) ORDER BY 1), ','), '') AS ids_text"
    )
}

async fn normalize(pool: &PgPool) {
    for query in [
        r#"UPDATE user_pending_permissions
           SET updated_at = '2026-06-01T00:00:00Z'
           WHERE id BETWEEN 98000 AND 98999"#,
        r#"UPDATE repo_pending_permissions
           SET updated_at = '2026-06-01T00:00:00Z'
           WHERE repo_id BETWEEN 9810 AND 9899"#,
        r#"UPDATE user_permissions
           SET updated_at = '2026-06-01T00:00:00Z', synced_at = '2026-06-01T00:00:00Z'
           WHERE user_id BETWEEN 9810 AND 9899"#,
        r#"UPDATE user_repo_permissions
           SET created_at = '2026-06-01T00:00:00Z', updated_at = '2026-06-01T00:00:00Z'
           WHERE user_id BETWEEN 9810 AND 9899 OR repo_id BETWEEN 9810 AND 9899"#,
        r#"UPDATE repo_permissions
           SET updated_at = '2026-06-01T00:00:00Z', synced_at = '2026-06-01T00:00:00Z'
           WHERE repo_id BETWEEN 9810 AND 9899"#,
    ] {
        sqlx::query(query).execute(pool).await.expect("normalize");
    }
}

async fn print_user_pending(pool: &PgPool, tag: &str) {
    let query = format!(
        r#"
SELECT id, bind_id, permission, object_type, {}
FROM user_pending_permissions
WHERE id BETWEEN 98000 AND 98999
ORDER BY id
"#,
        ids("object_ids_ints")
    );
    let rows = sqlx::query(&query)
        .fetch_all(pool)
        .await
        .expect("select user_pending_permissions");
    for row in rows {
        let id: i64 = row.try_get("id").expect("id");
        let bind_id: String = row.try_get("bind_id").expect("bind_id");
        let perm: String = row.try_get("permission").expect("permission");
        let object_type: String = row.try_get("object_type").expect("object_type");
        let object_ids: String = row.try_get("ids_text").expect("object ids");
        println!(
            "pp up {tag} id={id} bind={bind_id} perm={perm} object={object_type} ids={object_ids}"
        );
    }
}

async fn print_repo_pending(pool: &PgPool, tag: &str) {
    let query = format!(
        r#"
SELECT repo_id, permission, {}
FROM repo_pending_permissions
WHERE repo_id BETWEEN 9810 AND 9899
ORDER BY repo_id
"#,
        ids("user_ids_ints")
    );
    let rows = sqlx::query(&query)
        .fetch_all(pool)
        .await
        .expect("select repo_pending_permissions");
    for row in rows {
        let repo_id: i32 = row.try_get("repo_id").expect("repo_id");
        let perm: String = row.try_get("permission").expect("permission");
        let user_ids: String = row.try_get("ids_text").expect("user ids");
        println!("pp rp {tag} repo={repo_id} perm={perm} users={user_ids}");
    }
}

async fn print_user_perms(pool: &PgPool, tag: &str) {
    let query = format!(
        r#"
SELECT user_id, permission, object_type, {}
FROM user_permissions
WHERE user_id BETWEEN 9810 AND 9899
ORDER BY user_id
"#,
        ids("object_ids_ints")
    );
    let rows = sqlx::query(&query)
        .fetch_all(pool)
        .await
        .expect("select user_permissions");
    for row in rows {
        let user_id: i32 = row.try_get("user_id").expect("user_id");
        let perm: String = row.try_get("permission").expect("permission");
        let object_type: String = row.try_get("object_type").expect("object_type");
        let object_ids: String = row.try_get("ids_text").expect("object ids");
        println!("pp up2 {tag} user={user_id} perm={perm} object={object_type} ids={object_ids}");
    }
}

async fn print_repo_perms(pool: &PgPool, tag: &str) {
    let query = format!(
        r#"
SELECT repo_id, permission, unrestricted, {}
FROM repo_permissions
WHERE repo_id BETWEEN 9810 AND 9899
ORDER BY repo_id
"#,
        ids("user_ids_ints")
    );
    let rows = sqlx::query(&query)
        .fetch_all(pool)
        .await
        .expect("select repo_permissions");
    for row in rows {
        let repo_id: i32 = row.try_get("repo_id").expect("repo_id");
        let perm: String = row.try_get("permission").expect("permission");
        let unrestricted: bool = row.try_get("unrestricted").expect("unrestricted");
        let user_ids: String = row.try_get("ids_text").expect("user ids");
        println!(
            "pp rp2 {tag} repo={repo_id} perm={perm} unrestricted={} users={user_ids}",
            u8::from(unrestricted)
        );
    }
}

async fn print_user_repo(pool: &PgPool, tag: &str) {
    let rows = sqlx::query(
        r#"
SELECT COALESCE(user_id, 0) AS user_id,
       COALESCE(user_external_account_id, 0) AS ext_id, repo_id, source
FROM user_repo_permissions
WHERE user_id BETWEEN 9810 AND 9899 OR repo_id BETWEEN 9810 AND 9899
ORDER BY repo_id, source, user_id
"#,
    )
    .fetch_all(pool)
    .await
    .expect("select user_repo_permissions");
    for row in rows {
        let user_id: i32 = row.try_get("user_id").expect("user_id");
        let ext_id: i32 = row.try_get("ext_id").expect("ext_id");
        let repo_id: i32 = row.try_get("repo_id").expect("repo_id");
        let source: String = row.try_get("source").expect("source");
        println!("pp urp {tag} user={user_id} ext={ext_id} repo={repo_id} source={source}");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn pending_perms_matches_go_harness() {
    let Ok(dsn) = std::env::var("SG_QTEST_DSN") else {
        return;
    };
    let pool = PgPool::connect(&dsn).await.expect("connect");
    let now = OffsetDateTime::now_utc();
    let accounts = |bind_ids: &[&str]| {
        bind_ids
            .iter()
            .map(|id| (*id).to_owned())
            .collect::<Vec<_>>()
    };

    set_repo_pending_permissions(
        &pool,
        9811,
        PERM_READ,
        "github",
        "https://github.com/",
        &accounts(&["p1", "p2", "p3"]),
        now,
    )
    .await
    .expect("set_repo_pending_permissions#1");
    normalize(&pool).await;
    print_user_pending(&pool, "op1").await;
    print_repo_pending(&pool, "op1").await;

    set_repo_pending_permissions(
        &pool,
        9811,
        PERM_READ,
        "github",
        "https://github.com/",
        &accounts(&["p2"]),
        now,
    )
    .await
    .expect("set_repo_pending_permissions#2");
    normalize(&pool).await;
    print_user_pending(&pool, "op2").await;
    print_repo_pending(&pool, "op2").await;

    let bind_ids = list_pending_users(&pool, "github", "https://github.com/")
        .await
        .expect("list_pending_users");
    for bind_id in &bind_ids {
        println!("pp pending bind={bind_id}");
    }
    println!("pp pending count={}", bind_ids.len());

    let grant = || UserIdWithExternalAccountId {
        user_id: 9812,
        external_account_id: 9712,
    };
    grant_pending_permissions(&pool, grant(), "github", "https://github.com/", "p2", now)
        .await
        .expect("grant_pending_permissions#1");
    normalize(&pool).await;
    print_user_repo(&pool, "op4").await;
    print_repo_perms(&pool, "op4").await;
    print_user_perms(&pool, "op4").await;
    print_user_pending(&pool, "op4").await;
    print_repo_pending(&pool, "op4").await;

    grant_pending_permissions(&pool, grant(), "github", "https://github.com/", "p2", now)
        .await
        .expect("grant_pending_permissions#2");
    normalize(&pool).await;
    print_user_repo(&pool, "op5").await;
    print_repo_perms(&pool, "op5").await;
    print_user_perms(&pool, "op5").await;
    print_user_pending(&pool, "op5").await;
    print_repo_pending(&pool, "op5").await;
}
