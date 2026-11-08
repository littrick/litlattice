//! M9.8ap differential harness: repository-centric permission sync
//! (`permsSyncerImpl.syncRepoPerms`).
//!
//! Mirrors `cmd/worker/internal/authz/perms_syncer_harness_test.go`; the
//! fixture `test_ws/p9-go/repo_perms_syncer_fixtures.sql` is applied by
//! `test_ws/p9-go/run_repo_perms_syncer_diff.sh` immediately before each run.
//! Gated by `SG_QTEST_DSN`.

use std::sync::Arc;

use async_trait::async_trait;
use sg_db::perms_store::SetPermissionsResult;
use sg_db::users::UserRecord;
use sg_service_worker::jobs::perms_syncer::{
    AuthzProvider, FetchError, FetchPermsOptions, FetchRepo, PermsSyncer, ProviderRegistry,
    RepoSyncOutcome, UserPermsFetch, UserPermsFetcher,
};
use sqlx::postgres::{PgPool, PgPoolOptions};
use sqlx::Row;

struct FakeProvider;

#[async_trait]
impl AuthzProvider for FakeProvider {
    fn service_type(&self) -> &str {
        "github"
    }

    fn service_id(&self) -> &str {
        "https://github.com/"
    }

    fn urn(&self) -> &str {
        "extsvc:github:9701"
    }

    async fn fetch_repo_perms(
        &self,
        repo: &FetchRepo,
        _opts: &FetchPermsOptions,
    ) -> Result<Vec<String>, FetchError> {
        match repo.uri.as_str() {
            "https://example.com/repo9821" => Ok(vec!["a1".to_owned(), "a2".to_owned()]),
            "https://example.com/repo9824" => Err(FetchError::NotFound),
            "https://example.com/repo9825" => Err(FetchError::Unimplemented),
            "https://example.com/repo9826" => Err(FetchError::Failed {
                message: "boom-partial".to_owned(),
                partial_account_ids: vec!["a1".to_owned()],
            }),
            "https://example.com/repo9827" => Err(FetchError::Failed {
                message: "boom-abort".to_owned(),
                partial_account_ids: Vec::new(),
            }),
            _ => Ok(Vec::new()),
        }
    }
}

struct FakeRegistry;

impl ProviderRegistry for FakeRegistry {
    fn providers(&self) -> Vec<Arc<dyn AuthzProvider>> {
        vec![Arc::new(FakeProvider)]
    }
}

struct NoUserFetcher;

#[async_trait]
impl UserPermsFetcher for NoUserFetcher {
    async fn fetch_user_perms(
        &self,
        _user: &UserRecord,
        _no_perms: bool,
        _opts: &FetchPermsOptions,
    ) -> UserPermsFetch {
        UserPermsFetch::default()
    }
}

#[tokio::test]
async fn repo_perms_syncer() {
    let Ok(dsn) = std::env::var("SG_QTEST_DSN") else {
        eprintln!("SG_QTEST_DSN unset; skipping");
        return;
    };
    let pool = PgPoolOptions::new()
        .max_connections(2)
        .connect(&dsn)
        .await
        .expect("connect");
    let syncer = PermsSyncer::new(
        pool.clone(),
        Arc::new(FakeRegistry),
        Arc::new(NoUserFetcher),
    );

    let ops: [(i32, bool, &str); 8] = [
        (9821, false, "normal"),
        (9822, false, "public-skip"),
        (9823, false, "unmatched-provider-skip"),
        (9824, false, "not-found"),
        (9825, false, "unimplemented"),
        (9826, true, "partial-no-perms"),
        (9827, false, "abort"),
        (9828, false, "missing-repo"),
    ];

    for (i, (repo_id, no_perms, label)) in ops.iter().enumerate() {
        let op = i + 1;
        let outcome: RepoSyncOutcome = syncer
            .sync_repo_perms(*repo_id, *no_perms, FetchPermsOptions::default())
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
            "rs op={op} label={label} result={result} err={}",
            if outcome.error.is_some() {
                "yes"
            } else {
                "none"
            }
        );
        for status in &outcome.statuses {
            println!(
                "rs op={op} status provider={} type={} status={} message={}",
                status.provider_id, status.provider_type, status.status, status.message
            );
        }
        print_tables(&pool, op).await;
    }
}

async fn print_tables(pool: &PgPool, op: usize) {
    let rows = sqlx::query(
        r#"
SELECT repo_id, permission, unrestricted,
       array_to_string(ARRAY(SELECT unnest(user_ids_ints) ORDER BY 1), ',') AS users
FROM repo_permissions
WHERE repo_id BETWEEN 9820 AND 9899
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
            "rs op={op} rp repo={repo_id} perm={permission} unrestricted={} users={users}",
            if unrestricted { 1 } else { 0 }
        );
    }

    let rows = sqlx::query(
        r#"
SELECT user_id, user_external_account_id, repo_id, source
FROM user_repo_permissions
WHERE repo_id BETWEEN 9820 AND 9899 AND user_id BETWEEN 9810 AND 9899
ORDER BY repo_id, user_id, source
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
        println!("rs op={op} urp user={user_id} ext={ext_id} repo={repo_id} source={source}");
    }

    let rows = sqlx::query(
        r#"
SELECT user_id, permission, object_type,
       array_to_string(ARRAY(SELECT unnest(object_ids_ints) ORDER BY 1), ',') AS ids
FROM user_permissions
WHERE user_id BETWEEN 9810 AND 9899
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
        println!("rs op={op} up user={user_id} perm={permission} object={object_type} ids={ids}");
    }

    let rows = sqlx::query(
        r#"
SELECT rpp.repo_id, rpp.permission,
       coalesce((
           SELECT string_agg(upp.bind_id, ',' ORDER BY upp.bind_id)
           FROM user_pending_permissions upp
           WHERE upp.id = ANY(rpp.user_ids_ints)
       ), '') AS binds
FROM repo_pending_permissions rpp
WHERE rpp.repo_id BETWEEN 9820 AND 9899
ORDER BY rpp.repo_id, rpp.permission
"#,
    )
    .fetch_all(pool)
    .await
    .expect("repo_pending_permissions");
    for row in rows {
        let repo_id: i32 = row.get("repo_id");
        let permission: String = row.get("permission");
        let binds: String = row.get("binds");
        println!("rs op={op} rpending repo={repo_id} perm={permission} binds={binds}");
    }

    let rows = sqlx::query(
        r#"
SELECT bind_id, permission, object_type,
       array_to_string(ARRAY(SELECT unnest(object_ids_ints) ORDER BY 1), ',') AS ids
FROM user_pending_permissions
WHERE service_type = 'github'
  AND service_id = 'https://github.com/'
  AND bind_id IN ('a1', 'a2', 'a3', 'a4', 'a5')
ORDER BY bind_id, permission, object_type
"#,
    )
    .fetch_all(pool)
    .await
    .expect("user_pending_permissions");
    for row in rows {
        let bind_id: String = row.get("bind_id");
        let permission: String = row.get("permission");
        let object_type: String = row.get("object_type");
        let ids: String = row.get("ids");
        println!(
            "rs op={op} pending bind={bind_id} perm={permission} object={object_type} ids={ids}"
        );
    }
}
