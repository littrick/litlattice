//! M9.8aq differential harness: user-centric permission sync
//! (`permsSyncerImpl.syncUserPerms` / `saveUserPermsForAccount`).
//!
//! Mirrors `cmd/worker/internal/authz/perms_syncer_user_harness_test.go`; the
//! fixture `test_ws/p9-go/user_perms_syncer_fixtures.sql` is applied by
//! `test_ws/p9-go/run_user_perms_syncer_diff.sh` immediately before each run.
//! Gated by `SG_QTEST_DSN`.

use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use sg_db::perms_store::{CodeHostState, SetPermissionsResult};
use sg_db::users::UserRecord;
use sg_service_worker::jobs::perms_syncer::{
    AuthzProvider, FetchPermsOptions, PermsSyncer, ProviderRegistry, UserPermsFetch,
    UserPermsFetcher, UserSyncOutcome,
};
use sqlx::postgres::{PgPool, PgPoolOptions};
use sqlx::Row;

struct NoProviders;

#[async_trait]
impl ProviderRegistry for NoProviders {
    async fn providers(&self) -> Vec<Arc<dyn AuthzProvider>> {
        Vec::new()
    }
}

struct FakeFetcher;

fn success() -> CodeHostState {
    CodeHostState::new_provider_status("https://github.com/", "github", None, "FetchUserPerms")
}

#[async_trait]
impl UserPermsFetcher for FakeFetcher {
    async fn fetch_user_perms(
        &self,
        user: &UserRecord,
        _no_perms: bool,
        _opts: &FetchPermsOptions,
    ) -> UserPermsFetch {
        match user.id {
            9831 => {
                let mut repo_perms = BTreeMap::new();
                repo_perms.insert(9731, vec![9831, 9832]);
                repo_perms.insert(9732, vec![9832, 9833]);
                UserPermsFetch {
                    repo_perms,
                    sub_repo_perms: BTreeMap::new(),
                    statuses: vec![success(), success()],
                    error: None,
                }
            }
            9832 => {
                let mut repo_perms = BTreeMap::new();
                repo_perms.insert(9733, vec![9834]);
                UserPermsFetch {
                    repo_perms,
                    sub_repo_perms: BTreeMap::new(),
                    statuses: vec![success()],
                    error: None,
                }
            }
            9835 => UserPermsFetch {
                repo_perms: BTreeMap::new(),
                sub_repo_perms: BTreeMap::new(),
                statuses: vec![CodeHostState::new_provider_status(
                    "https://github.com/",
                    "github",
                    Some("boom-user"),
                    "FetchUserPerms",
                )],
                error: Some("boom-user".to_owned()),
            },
            9836 => UserPermsFetch {
                repo_perms: BTreeMap::new(),
                sub_repo_perms: BTreeMap::new(),
                statuses: vec![success()],
                error: None,
            },
            _ => UserPermsFetch::default(),
        }
    }
}

#[tokio::test]
async fn user_perms_syncer() {
    let Ok(dsn) = std::env::var("SG_QTEST_DSN") else {
        eprintln!("SG_QTEST_DSN unset; skipping");
        return;
    };
    let pool = PgPoolOptions::new()
        .max_connections(2)
        .connect(&dsn)
        .await
        .expect("connect");
    let syncer = PermsSyncer::new(pool.clone(), Arc::new(NoProviders), Arc::new(FakeFetcher));

    let ops: [(i32, bool, &str); 6] = [
        (9831, false, "two-accounts"),
        (9831, false, "repeat"),
        (9832, false, "removal"),
        (9834, false, "missing-user"),
        (9835, false, "fetch-error"),
        (9836, false, "empty"),
    ];

    for (i, (user_id, no_perms, label)) in ops.iter().enumerate() {
        let op = i + 1;
        let outcome: UserSyncOutcome = syncer
            .sync_user_perms(*user_id, *no_perms, FetchPermsOptions::default())
            .await;
        let result = match outcome.result {
            Some(SetPermissionsResult {
                added,
                removed,
                found,
            }) => format!("found:{found},added:{added},removed:{removed}"),
            None => "none".to_owned(),
        };
        println!(
            "us op={op} label={label} result={result} err={}",
            if outcome.error.is_some() {
                "yes"
            } else {
                "none"
            }
        );
        for status in &outcome.statuses {
            println!(
                "us op={op} status provider={} type={} status={} message={}",
                status.provider_id, status.provider_type, status.status, status.message
            );
        }
        print_tables(&pool, op).await;
    }
}

async fn print_tables(pool: &PgPool, op: usize) {
    let rows = sqlx::query(
        r#"
SELECT user_id, user_external_account_id, repo_id, source
FROM user_repo_permissions
WHERE user_id BETWEEN 9830 AND 9899 OR repo_id BETWEEN 9830 AND 9899
ORDER BY repo_id, user_id, user_external_account_id, source
"#,
    )
    .fetch_all(pool)
    .await
    .expect("user_repo_permissions");
    for row in rows {
        let user_id: i32 = row.get("user_id");
        let ext_id: i32 = row.get("user_external_account_id");
        let repo_id: i32 = row.get("repo_id");
        let source: String = row.get("source");
        println!("us op={op} urp user={user_id} ext={ext_id} repo={repo_id} source={source}");
    }

    let rows = sqlx::query(
        r#"
SELECT repo_id, permission, unrestricted,
       array_to_string(ARRAY(SELECT unnest(user_ids_ints) ORDER BY 1), ',') AS users
FROM repo_permissions
WHERE repo_id BETWEEN 9830 AND 9899
ORDER BY repo_id, permission
"#,
    )
    .fetch_all(pool)
    .await
    .expect("repo_permissions");
    for row in rows {
        let repo_id: i32 = row.get("repo_id");
        let permission: String = row.get("permission");
        let unrestricted: bool = row.get("unrestricted");
        let users: String = row.get("users");
        println!(
            "us op={op} rp repo={repo_id} perm={permission} unrestricted={} users={users}",
            if unrestricted { 1 } else { 0 }
        );
    }

    let rows = sqlx::query(
        r#"
SELECT user_id, permission, object_type,
       array_to_string(ARRAY(SELECT unnest(object_ids_ints) ORDER BY 1), ',') AS ids
FROM user_permissions
WHERE user_id BETWEEN 9830 AND 9899
ORDER BY user_id, permission, object_type
"#,
    )
    .fetch_all(pool)
    .await
    .expect("user_permissions");
    for row in rows {
        let user_id: i32 = row.get("user_id");
        let permission: String = row.get("permission");
        let object_type: String = row.get("object_type");
        let ids: String = row.get("ids");
        println!("us op={op} up user={user_id} perm={permission} object={object_type} ids={ids}");
    }
}
