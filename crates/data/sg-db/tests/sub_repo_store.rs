#![forbid(unsafe_code)]

//! M9.8au differential harness (Rust side): the remaining
//! `internal/database/sub_repo_perms_store.go` methods.
//!
//! Mirrors `cmd/worker/internal/authz/sub_repo_store_harness_test.go`.
//! `test_ws/p9-go/run_sub_repo_store_diff.sh` applies
//! `sub_repo_store_fixtures.sql` immediately before each side.

use sg_db::sub_repo_perms::{self, PathWithIp, SubRepoPermissions, SubRepoPermissionsWithIps};
use sqlx::postgres::PgPool;
use sqlx::Row;

fn paths_text(paths: &[String]) -> String {
    if paths.is_empty() {
        "<empty>".to_owned()
    } else {
        paths.join(",")
    }
}

fn paths_with_ips_text(paths: &[PathWithIp]) -> String {
    if paths.is_empty() {
        "<empty>".to_owned()
    } else {
        paths
            .iter()
            .map(|p| format!("{}@{}", p.path, p.ip))
            .collect::<Vec<_>>()
            .join(",")
    }
}

async fn print_rows(pool: &PgPool, op: u32) {
    let rows = sqlx::query(
        r#"
SELECT repo_id, user_id, version,
       CASE WHEN paths IS NULL THEN '<null>' ELSE array_to_string(paths, ',') END AS paths_text,
       ips IS NULL AS ips_null,
       CASE WHEN ips IS NULL THEN '<null>' ELSE array_to_string(ips, ',') END AS ips_text
FROM sub_repo_permissions
WHERE user_id BETWEEN 10200 AND 10299
ORDER BY repo_id, user_id
"#,
    )
    .fetch_all(pool)
    .await
    .expect("select sub_repo_permissions");

    for row in rows {
        let repo_id: i32 = row.try_get("repo_id").expect("repo_id");
        let user_id: i32 = row.try_get("user_id").expect("user_id");
        let version: i32 = row.try_get("version").expect("version");
        let paths_text: String = row.try_get("paths_text").expect("paths_text");
        let ips_null: bool = row.try_get("ips_null").expect("ips_null");
        let ips_text: String = row.try_get("ips_text").expect("ips_text");
        println!(
            "sr op={op} row repo={repo_id} user={user_id} version={version} paths={} ips_null={} ips={}",
            empty_marker(&paths_text),
            i32::from(ips_null),
            empty_marker(&ips_text),
        );
    }
}

fn empty_marker(value: &str) -> &str {
    if value.is_empty() {
        "<empty>"
    } else {
        value
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn sub_repo_store_matches_go_harness() {
    let Ok(dsn) = std::env::var("SG_QTEST_DSN") else {
        return;
    };
    let pool = PgPool::connect(&dsn).await.expect("connect");

    print_rows(&pool, 0).await;

    let err = sub_repo_perms::upsert(
        &pool,
        10205,
        10208,
        &SubRepoPermissions {
            paths: vec!["upload/**".to_owned()],
        },
    )
    .await;
    println!(
        "sr op=1 upsert repo=10208 user=10205 err={}",
        err.map_or_else(|e| e.to_string(), |_| "none".to_owned())
    );
    print_rows(&pool, 1).await;

    let err = sub_repo_perms::upsert_with_ips(
        &pool,
        10203,
        10204,
        &SubRepoPermissionsWithIps {
            paths: vec![
                PathWithIp {
                    path: "d1/**".to_owned(),
                    ip: "1.1.1.1".to_owned(),
                },
                PathWithIp {
                    path: "d2/**".to_owned(),
                    ip: "2.2.2.2".to_owned(),
                },
            ],
        },
    )
    .await;
    println!(
        "sr op=2 upsert-with-ips repo=10204 user=10203 err={}",
        err.map_or_else(|e| e.to_string(), |_| "none".to_owned())
    );
    print_rows(&pool, 2).await;

    match sub_repo_perms::get(&pool, 10202, 10201).await {
        Ok(perms) => println!(
            "sr op=3 get repo=10201 user=10202 count={} paths={} err=none",
            perms.paths.len(),
            paths_text(&perms.paths)
        ),
        Err(err) => println!("sr op=3 get repo=10201 user=10202 count=0 paths=<none> err={err}"),
    }
    // The baseline query swaps its bind arguments (`repo_id = userID`,
    // `user_id = repoID`), so this call reads row (repo 10201, user 10202).
    match sub_repo_perms::get(&pool, 10201, 10202).await {
        Ok(perms) => println!(
            "sr op=3 get repo=10202 user=10201 count={} paths={} err=none",
            perms.paths.len(),
            paths_text(&perms.paths)
        ),
        Err(err) => println!("sr op=3 get repo=10202 user=10201 count=0 paths=<none> err={err}"),
    }

    // Same argument swap as `get`: (userID, repoID) are bound to
    // (repo_id, user_id).
    get_with_ips(&pool, 4, 10202, 10201, false).await;
    get_with_ips(&pool, 4, 10201, 10202, false).await;
    get_with_ips(&pool, 4, 10204, 10201, false).await;
    get_with_ips(&pool, 4, 10204, 10201, true).await;
    get_with_ips(&pool, 4, 10205, 10205, false).await;

    get_by_user(&pool, 5, 10202, false).await;
    get_by_user(&pool, 6, 10201, true).await;
    get_by_user(&pool, 6, 10201, false).await;

    get_by_user_with_ips(&pool, 7, 10202, false, false).await;
    get_by_user_with_ips(&pool, 7, 10202, false, true).await;
    get_by_user_with_ips(&pool, 7, 10207, false, false).await;
    get_by_user_with_ips(&pool, 7, 10201, false, true).await;
    get_by_user_with_ips(&pool, 7, 10201, true, true).await;

    for repo_id in [10201, 10203, 10208, 19999] {
        match sub_repo_perms::repo_id_supported(&pool, repo_id).await {
            Ok(supported) => {
                println!("sr op=8 repo-id-supported repo={repo_id} supported={supported} err=none")
            }
            Err(err) => {
                println!("sr op=8 repo-id-supported repo={repo_id} supported=false err={err}")
            }
        }
    }

    for repo_name in ["srs/repo1", "srs/repo3", "srs/repo8", "srs/missing"] {
        match sub_repo_perms::repo_supported(&pool, repo_name).await {
            Ok(supported) => {
                println!("sr op=9 repo-supported repo={repo_name} supported={supported} err=none")
            }
            Err(err) => {
                println!("sr op=9 repo-supported repo={repo_name} supported=false err={err}")
            }
        }
    }

    let err = sub_repo_perms::delete_by_user(&pool, 10202).await;
    println!(
        "sr op=10 delete-by-user user=10202 err={}",
        err.map_or_else(|e| e.to_string(), |_| "none".to_owned())
    );
    print_rows(&pool, 10).await;
}

async fn get_with_ips(pool: &PgPool, op: u32, user_id: i32, repo_id: i32, backfill: bool) {
    match sub_repo_perms::get_with_ips(pool, user_id, repo_id, backfill).await {
        Ok(perms) => println!(
            "sr op={op} get-with-ips repo={repo_id} user={user_id} backfill={backfill} paths={} err=none",
            paths_with_ips_text(&perms.paths)
        ),
        Err(err) => println!(
            "sr op={op} get-with-ips repo={repo_id} user={user_id} backfill={backfill} paths=<none> err={err}"
        ),
    }
}

async fn get_by_user(pool: &PgPool, op: u32, user_id: i32, enforce: bool) {
    match sub_repo_perms::get_by_user(pool, user_id, enforce).await {
        Ok(perms) => {
            println!(
                "sr op={op} get-by-user user={user_id} enforce={enforce} count={} err=none",
                perms.len()
            );
            for (name, perms) in perms {
                println!(
                    "sr op={op} user-row repo={name} paths={}",
                    paths_text(&perms.paths)
                );
            }
        }
        Err(err) => {
            println!("sr op={op} get-by-user user={user_id} enforce={enforce} count=0 err={err}")
        }
    }
}

async fn get_by_user_with_ips(pool: &PgPool, op: u32, user_id: i32, enforce: bool, backfill: bool) {
    match sub_repo_perms::get_by_user_with_ips(pool, user_id, enforce, backfill).await {
        Ok(perms) => {
            println!(
                "sr op={op} get-by-user-with-ips user={user_id} enforce={enforce} backfill={backfill} count={} err=none",
                perms.len()
            );
            for (name, perms) in perms {
                println!(
                    "sr op={op} user-ip-row repo={name} paths={}",
                    paths_with_ips_text(&perms.paths)
                );
            }
        }
        Err(err) => println!(
            "sr op={op} get-by-user-with-ips user={user_id} enforce={enforce} backfill={backfill} count=0 err={err}"
        ),
    }
}
