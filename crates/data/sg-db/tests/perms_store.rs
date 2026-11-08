#![forbid(unsafe_code)]

//! M9.8an differential harness (Rust side): the permissions-store write path
//! used by permissions syncing.
//!
//! Mirrors `internal/database/perms_store_harness_test.go`.
//! `test_ws/p9-go/run_perms_store_diff.sh` applies
//! `perms_store_fixtures.sql` immediately before each side and diffs the two
//! canonical `ps ` outputs.

use sg_db::perms_store::{
    get_latest_finished_sync_job, get_user_ids_by_external_accounts, save_sync_result,
    set_repo_permissions_unrestricted, set_repo_perms, set_user_external_account_perms,
    CodeHostState, SetPermissionsResult, UserIdWithExternalAccountId, SOURCE_REPO_SYNC,
    SOURCE_USER_SYNC,
};
use sqlx::postgres::PgPool;
use sqlx::Row;
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;

fn flag(value: bool) -> u8 {
    u8::from(value)
}

async fn normalize(pool: &PgPool) {
    for query in [
        r#"UPDATE user_repo_permissions
           SET created_at = '2026-06-01T00:00:00Z', updated_at = '2026-06-01T00:00:00Z'
           WHERE user_id BETWEEN 9800 AND 9899 OR repo_id BETWEEN 9800 AND 9899"#,
        r#"UPDATE repo_permissions
           SET updated_at = '2026-06-01T00:00:00Z', synced_at = '2026-06-01T00:00:00Z'
           WHERE repo_id BETWEEN 9800 AND 9899"#,
    ] {
        sqlx::query(query).execute(pool).await.expect("normalize");
    }
}

async fn print_user_repo(pool: &PgPool, tag: &str) {
    let rows = sqlx::query(
        r#"
SELECT id, COALESCE(user_id, 0) AS user_id,
       COALESCE(user_external_account_id, 0) AS ext_id, repo_id, source
FROM user_repo_permissions
WHERE user_id BETWEEN 9800 AND 9899 OR repo_id BETWEEN 9800 AND 9899
ORDER BY id
"#,
    )
    .fetch_all(pool)
    .await
    .expect("select user_repo_permissions");
    for row in rows {
        let id: i64 = row.try_get("id").expect("id");
        let user_id: i32 = row.try_get("user_id").expect("user_id");
        let ext_id: i32 = row.try_get("ext_id").expect("ext_id");
        let repo_id: i32 = row.try_get("repo_id").expect("repo_id");
        let source: String = row.try_get("source").expect("source");
        println!("ps urp {tag} id={id} user={user_id} ext={ext_id} repo={repo_id} source={source}");
    }
}

async fn print_repo_perms(pool: &PgPool, tag: &str) {
    let rows = sqlx::query(
        r#"
SELECT repo_id, permission, unrestricted, user_ids_ints::text AS user_ids
FROM repo_permissions
WHERE repo_id BETWEEN 9800 AND 9899
ORDER BY repo_id
"#,
    )
    .fetch_all(pool)
    .await
    .expect("select repo_permissions");
    for row in rows {
        let repo_id: i32 = row.try_get("repo_id").expect("repo_id");
        let permission: String = row.try_get("permission").expect("permission");
        let unrestricted: bool = row.try_get("unrestricted").expect("unrestricted");
        let user_ids: String = row.try_get("user_ids").expect("user_ids");
        println!(
            "ps rp {tag} repo={repo_id} permission={permission} unrestricted={} user_ids={user_ids}",
            flag(unrestricted)
        );
    }
}

async fn print_jobs(pool: &PgPool, tag: &str) {
    let rows = sqlx::query(
        r#"
SELECT id, COALESCE(permissions_added, -1) AS added,
       COALESCE(permissions_removed, -1) AS removed,
       COALESCE(permissions_found, -1) AS found, is_partial_success,
       COALESCE(array_to_string(code_host_states, '|'), '-') AS states
FROM permission_sync_jobs
WHERE id BETWEEN 9600 AND 9699
ORDER BY id
"#,
    )
    .fetch_all(pool)
    .await
    .expect("select permission_sync_jobs");
    for row in rows {
        let id: i32 = row.try_get("id").expect("id");
        let added: i32 = row.try_get("added").expect("added");
        let removed: i32 = row.try_get("removed").expect("removed");
        let found: i32 = row.try_get("found").expect("found");
        let partial: bool = row.try_get("is_partial_success").expect("partial");
        let states: String = row.try_get("states").expect("states");
        println!(
            "ps job {tag} id={id} added={added} removed={removed} found={found} partial={} states={states}",
            flag(partial)
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn perms_store_matches_go_harness() {
    let Ok(dsn) = std::env::var("SG_QTEST_DSN") else {
        return;
    };

    let pool = PgPool::connect(&dsn).await.expect("connect");
    let now = OffsetDateTime::now_utc();

    // 1. Repo-centric sync.
    let result = set_repo_perms(
        &pool,
        9801,
        &[
            UserIdWithExternalAccountId {
                user_id: 9801,
                external_account_id: 9701,
            },
            UserIdWithExternalAccountId {
                user_id: 9802,
                external_account_id: 9702,
            },
        ],
        SOURCE_REPO_SYNC,
        now,
    )
    .await
    .expect("set_repo_perms");
    println!(
        "ps result op=repo_sync added={} removed={} found={}",
        result.added, result.removed, result.found
    );
    normalize(&pool).await;
    print_user_repo(&pool, "op1").await;
    print_repo_perms(&pool, "op1").await;

    // 2. User-centric sync.
    let result = set_user_external_account_perms(
        &pool,
        UserIdWithExternalAccountId {
            user_id: 9801,
            external_account_id: 9701,
        },
        &[9803, 9804],
        SOURCE_USER_SYNC,
        now,
    )
    .await
    .expect("set_user_external_account_perms");
    println!(
        "ps result op=user_sync added={} removed={} found={}",
        result.added, result.removed, result.found
    );
    normalize(&pool).await;
    print_user_repo(&pool, "op2").await;

    // 3. Mark repo 9805 unrestricted.
    set_repo_permissions_unrestricted(&pool, &[9805], true, now)
        .await
        .expect("set_repo_permissions_unrestricted(true)");
    normalize(&pool).await;
    print_user_repo(&pool, "op3").await;
    print_repo_perms(&pool, "op3").await;

    // 4. Unset the unrestricted flag.
    set_repo_permissions_unrestricted(&pool, &[9805], false, now)
        .await
        .expect("set_repo_permissions_unrestricted(false)");
    normalize(&pool).await;
    print_user_repo(&pool, "op4").await;
    print_repo_perms(&pool, "op4").await;

    // 5/6. SaveSyncResult with mixed provider results.
    let success =
        CodeHostState::new_provider_status("https://github.com/", "github", None, "FetchRepoPerms");
    let failure = CodeHostState::new_provider_status(
        "https://gitlab.com/",
        "gitlab",
        Some("boom"),
        "FetchRepoPerms",
    );
    let result = SetPermissionsResult {
        added: 1,
        removed: 2,
        found: 3,
    };
    save_sync_result(&pool, 9601, true, Some(&result), &[success, failure])
        .await
        .expect("save_sync_result(success)");
    print_jobs(&pool, "op5").await;

    let failure = CodeHostState::new_provider_status(
        "https://gitlab.com/",
        "gitlab",
        Some("boom"),
        "FetchRepoPerms",
    );
    save_sync_result(
        &pool,
        9605,
        false,
        Some(&SetPermissionsResult::default()),
        std::slice::from_ref(&failure),
    )
    .await
    .expect("save_sync_result(failure)");
    print_jobs(&pool, "op6").await;

    // 7/8. Latest finished jobs.
    match get_latest_finished_sync_job(&pool, Some(9801), None, true)
        .await
        .expect("latest user")
    {
        Some(job) => println!(
            "ps latest user=9801 id={} finished={}",
            job.id,
            job.finished_at.format(&Rfc3339).expect("format")
        ),
        None => println!("ps latest user=9801 id=-"),
    }
    match get_latest_finished_sync_job(&pool, None, Some(9801), true)
        .await
        .expect("latest repo")
    {
        Some(job) => println!(
            "ps latest repo=9801 id={} finished={}",
            job.id,
            job.finished_at.format(&Rfc3339).expect("format")
        ),
        None => println!("ps latest repo=9801 id=-"),
    }

    // 9. External account lookup.
    let accounts = get_user_ids_by_external_accounts(
        &pool,
        "github",
        "https://github.com/",
        &[
            "a1".to_owned(),
            "a2".to_owned(),
            "a3".to_owned(),
            "a4".to_owned(),
            "missing".to_owned(),
        ],
    )
    .await
    .expect("get_user_ids_by_external_accounts");
    let mut bind_ids: Vec<&String> = accounts.keys().collect();
    bind_ids.sort();
    for bind_id in &bind_ids {
        let account = accounts[*bind_id];
        println!(
            "ps account bind={bind_id} user={} ext={}",
            account.user_id, account.external_account_id
        );
    }
    println!("ps accounts count={}", accounts.len());
}
