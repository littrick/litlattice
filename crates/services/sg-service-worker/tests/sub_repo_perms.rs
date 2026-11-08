//! M9.8as differential harness: sub-repository permissions written by the
//! user-centric permission sync.
//!
//! Mirrors `cmd/worker/internal/authz/perms_syncer_sub_repo_harness_test.go`;
//! the fixture `test_ws/p9-go/sub_repo_perms_fixtures.sql` is applied by
//! `test_ws/p9-go/run_sub_repo_perms_diff.sh` immediately before each run.
//! Gated by `SG_QTEST_DSN`.

use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use sg_db::perms_store::SetPermissionsResult;
use sg_db::sub_repo_perms::{PathWithIp, SubRepoPermissionsWithIps};
use sg_db::user_external_accounts::{ExternalAccount, ExternalAccountSpec};
use sg_db::users::UserRecord;
use sg_service_worker::jobs::perms_syncer::{
    AuthzProvider, ExternalUserPermissions, FetchError, FetchPermsOptions, FetchRepo, PermsSyncer,
    ProviderRegistry, UserPermsError, UserSyncOutcome,
};
use sg_service_worker::jobs::perms_syncer_fetch::ExternalAccountPermsFetcher;
use sqlx::postgres::{PgPool, PgPoolOptions};
use sqlx::Row;

struct SrProvider;

struct SrRegistry {
    providers: Vec<Arc<dyn AuthzProvider>>,
}

#[async_trait]
impl ProviderRegistry for SrRegistry {
    async fn providers(&self) -> Vec<Arc<dyn AuthzProvider>> {
        self.providers.clone()
    }
}

fn perms(paths: &[(&str, &str)]) -> SubRepoPermissionsWithIps {
    SubRepoPermissionsWithIps {
        paths: paths
            .iter()
            .map(|(path, ip)| PathWithIp {
                path: (*path).to_owned(),
                ip: (*ip).to_owned(),
            })
            .collect(),
    }
}

#[async_trait]
impl AuthzProvider for SrProvider {
    fn service_type(&self) -> &str {
        "github"
    }

    fn service_id(&self) -> &str {
        "https://github.com/"
    }

    fn urn(&self) -> &str {
        "extsvc:github:10001"
    }

    async fn fetch_repo_perms(
        &self,
        _repo: &FetchRepo,
        _opts: &FetchPermsOptions,
    ) -> Result<Vec<String>, FetchError> {
        Ok(Vec::new())
    }

    async fn fetch_account(
        &self,
        _user: &UserRecord,
    ) -> Result<Option<ExternalAccountSpec>, String> {
        Ok(None)
    }

    async fn fetch_user_perms(
        &self,
        account: &ExternalAccount,
        _opts: &FetchPermsOptions,
    ) -> Result<Option<ExternalUserPermissions>, UserPermsError> {
        let mut sub_repo_permissions = BTreeMap::new();
        let perms = match account.account_id.as_str() {
            "a1" => {
                sub_repo_permissions.insert(
                    "sr-exact1".to_owned(),
                    perms(&[("src/**", "*"), ("docs/**", "192.168.1.0/24")]),
                );
                sub_repo_permissions.insert("sr-missing".to_owned(), perms(&[("x/**", "*")]));
                ExternalUserPermissions {
                    exacts: vec!["sr-exact1".to_owned()],
                    sub_repo_permissions,
                    ..Default::default()
                }
            }
            "a4" | "a5" => {
                return Err(UserPermsError::Temporary {
                    message: "503 temporary".to_owned(),
                })
            }
            "a6" => {
                sub_repo_permissions.insert("sr-partial".to_owned(), perms(&[("partial/**", "*")]));
                return Err(UserPermsError::Failed {
                    message: "boom-partial".to_owned(),
                    partial: Some(ExternalUserPermissions {
                        exacts: vec!["sr-partial".to_owned()],
                        sub_repo_permissions,
                        ..Default::default()
                    }),
                });
            }
            "a7" => {
                sub_repo_permissions.insert("sr-empty".to_owned(), perms(&[("only/**", "*")]));
                ExternalUserPermissions {
                    sub_repo_permissions,
                    ..Default::default()
                }
            }
            _ => return Ok(None),
        };
        Ok(Some(perms))
    }
}

#[tokio::test]
async fn sub_repo_perms() {
    let Ok(dsn) = std::env::var("SG_QTEST_DSN") else {
        eprintln!("SG_QTEST_DSN unset; skipping");
        return;
    };
    let pool = PgPoolOptions::new()
        .max_connections(2)
        .connect(&dsn)
        .await
        .expect("connect");

    let registry = Arc::new(SrRegistry {
        providers: vec![Arc::new(SrProvider)],
    });
    let fetcher = Arc::new(
        ExternalAccountPermsFetcher::new(pool.clone(), registry.clone()).with_dotcom_mode(false),
    );
    let syncer = PermsSyncer::new(pool.clone(), registry, fetcher);

    let ops: [(i32, bool, &str); 9] = [
        (10031, false, "success-replace"),
        (10031, false, "success-repeat"),
        (10034, false, "temporary-abort"),
        (10034, true, "temporary-retain-ips"),
        (10035, true, "temporary-retain-null-ips"),
        (10036, true, "partial-no-perms"),
        (10036, false, "partial-abort"),
        (10037, false, "subrepo-only"),
        (10038, false, "unconfigured-provider"),
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
            "sr op={op} label={label} result={result} err={}",
            if outcome.error.is_some() {
                "yes"
            } else {
                "none"
            }
        );
        for status in &outcome.statuses {
            println!(
                "sr op={op} status provider={} type={} status={} message={}",
                status.provider_id, status.provider_type, status.status, status.message
            );
        }
        print_tables(&pool, op).await;
    }
}

async fn print_tables(pool: &PgPool, op: usize) {
    let rows = sqlx::query(
        r#"
SELECT repo_id, user_id, version,
       array_to_string(paths, ',') AS paths,
       (ips IS NULL) AS ips_null,
       COALESCE(array_to_string(ips, ','), '<null>') AS ips,
       (updated_at > '2026-06-01T00:00:00Z') AS updated
FROM sub_repo_permissions
WHERE user_id BETWEEN 10030 AND 10099
ORDER BY repo_id, user_id, version
"#,
    )
    .fetch_all(pool)
    .await
    .expect("sub_repo_permissions");
    for row in rows {
        let repo_id: i32 = row.get("repo_id");
        let user_id: i32 = row.get("user_id");
        let version: i32 = row.get("version");
        let paths: String = row.get("paths");
        let ips_null: bool = row.get("ips_null");
        let ips: String = row.get("ips");
        let updated: bool = row.get("updated");
        println!(
            "sr op={op} srow repo={repo_id} user={user_id} version={version} paths={paths} \
             ips_null={} ips={ips} updated={}",
            if ips_null { 1 } else { 0 },
            if updated { 1 } else { 0 }
        );
    }

    let rows = sqlx::query(
        r#"
SELECT user_id, user_external_account_id, repo_id, source
FROM user_repo_permissions
WHERE user_id BETWEEN 10030 AND 10099 OR repo_id BETWEEN 10030 AND 10099
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
        println!("sr op={op} urp user={user_id} ext={ext_id} repo={repo_id} source={source}");
    }
}
