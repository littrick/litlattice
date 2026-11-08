//! M9.8ar differential harness: user-centric permission fetch
//! (`fetchUserPermsViaExternalAccounts` through `syncUserPerms`).
//!
//! Mirrors `cmd/worker/internal/authz/perms_syncer_user_fetch_harness_test.go`;
//! the fixture `test_ws/p9-go/fetch_user_perms_fixtures.sql` is applied by
//! `test_ws/p9-go/run_fetch_user_perms_diff.sh` immediately before each run.
//! Gated by `SG_QTEST_DSN`.

use std::sync::Arc;

use async_trait::async_trait;
use sg_db::perms_store::SetPermissionsResult;
use sg_db::user_external_accounts::{ExternalAccount, ExternalAccountSpec};
use sg_db::users::UserRecord;
use sg_service_worker::jobs::perms_syncer::{
    AuthzProvider, ExternalUserPermissions, FetchError, FetchPermsOptions, FetchRepo, PermsSyncer,
    ProviderRegistry, UserPermsError, UserSyncOutcome,
};
use sg_service_worker::jobs::perms_syncer_fetch::ExternalAccountPermsFetcher;
use sqlx::postgres::{PgPool, PgPoolOptions};
use sqlx::Row;

struct FuProvider {
    service_type: &'static str,
    service_id: &'static str,
    urn: &'static str,
}

struct FuRegistry {
    providers: Vec<Arc<dyn AuthzProvider>>,
}

#[async_trait]
impl ProviderRegistry for FuRegistry {
    async fn providers(&self) -> Vec<Arc<dyn AuthzProvider>> {
        self.providers.clone()
    }
}

#[async_trait]
impl AuthzProvider for FuProvider {
    fn service_type(&self) -> &str {
        self.service_type
    }

    fn service_id(&self) -> &str {
        self.service_id
    }

    fn urn(&self) -> &str {
        self.urn
    }

    async fn fetch_repo_perms(
        &self,
        _repo: &FetchRepo,
        _opts: &FetchPermsOptions,
    ) -> Result<Vec<String>, FetchError> {
        Err(FetchError::Unimplemented)
    }

    async fn fetch_account(
        &self,
        user: &UserRecord,
    ) -> Result<Option<ExternalAccountSpec>, String> {
        match (self.service_id, user.id) {
            ("https://github.com/", 9941) => Ok(Some(ExternalAccountSpec {
                service_type: "github".to_owned(),
                service_id: "https://github.com/".to_owned(),
                client_id: String::new(),
                account_id: "f1".to_owned(),
            })),
            ("https://fetch-error.example.com/", 9934) => Err("boom-account".to_owned()),
            _ => Ok(None),
        }
    }

    async fn fetch_user_perms(
        &self,
        account: &ExternalAccount,
        _opts: &FetchPermsOptions,
    ) -> Result<Option<ExternalUserPermissions>, UserPermsError> {
        let perms = match account.account_id.as_str() {
            "a1" => ExternalUserPermissions {
                exacts: vec!["a-exact1".into(), "a-exact2".into(), "a-public".into()],
                include_contains: vec!["a-cont%".into()],
                exclude_contains: vec!["a-cont%bar".into()],
                ..Default::default()
            },
            "a2" => {
                return Err(UserPermsError::Unauthorized {
                    message: "token is revoked".to_owned(),
                })
            }
            "a3" => {
                return Err(UserPermsError::AccountSuspended {
                    message: "account was suspended".to_owned(),
                })
            }
            "a4" => {
                return Err(UserPermsError::Forbidden {
                    message: "403 forbidden".to_owned(),
                })
            }
            "a7" => {
                return Err(UserPermsError::Unimplemented {
                    message: "FetchUserPerms is unimplemented".to_owned(),
                })
            }
            "a8" => {
                return Err(UserPermsError::Temporary {
                    message: "503 temporary".to_owned(),
                })
            }
            "a9" => {
                return Err(UserPermsError::Failed {
                    message: "boom-partial".to_owned(),
                    partial: Some(ExternalUserPermissions {
                        exacts: vec!["a-partial".into()],
                        ..Default::default()
                    }),
                })
            }
            "a10" => ExternalUserPermissions::default(),
            "g1" => ExternalUserPermissions {
                exacts: vec!["g-exact".into()],
                ..Default::default()
            },
            "f1" => ExternalUserPermissions {
                exacts: vec!["f-exact1".into()],
                ..Default::default()
            },
            _ => return Ok(None),
        };
        Ok(Some(perms))
    }
}

#[tokio::test]
async fn fetch_user_perms() {
    let Ok(dsn) = std::env::var("SG_QTEST_DSN") else {
        eprintln!("SG_QTEST_DSN unset; skipping");
        return;
    };
    let pool = PgPoolOptions::new()
        .max_connections(2)
        .connect(&dsn)
        .await
        .expect("connect");

    let providers: Vec<Arc<dyn AuthzProvider>> = vec![
        Arc::new(FuProvider {
            service_type: "github",
            service_id: "https://github.com/",
            urn: "extsvc:github:9901",
        }),
        Arc::new(FuProvider {
            service_type: "gitlab",
            service_id: "https://gitlab.example.com/",
            urn: "extsvc:gitlab:9902",
        }),
        Arc::new(FuProvider {
            service_type: "github",
            service_id: "https://fetch-error.example.com/",
            urn: "extsvc:github:9903",
        }),
        Arc::new(FuProvider {
            service_type: "github",
            service_id: "https://fetch-nil.example.com/",
            urn: "extsvc:github:9904",
        }),
    ];
    let registry = Arc::new(FuRegistry { providers });
    let fetcher = Arc::new(
        ExternalAccountPermsFetcher::new(pool.clone(), registry.clone()).with_dotcom_mode(false),
    );
    let syncer = PermsSyncer::new(pool.clone(), registry, fetcher);

    let ops: [(i32, bool, &str); 14] = [
        (9931, false, "normal"),
        (9941, false, "fetch-account"),
        (9941, false, "fetch-account-repeat"),
        (9935, false, "expired-gitlab"),
        (9932, false, "unauthorized"),
        (9933, false, "suspended"),
        (9934, false, "forbidden-account-error"),
        (9936, false, "unconfigured-provider"),
        (9938, false, "temporary-retain"),
        (9938, true, "temporary-no-perms"),
        (9939, true, "partial-no-perms"),
        (9939, false, "partial-abort"),
        (9937, false, "unimplemented"),
        (9942, false, "empty"),
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
            "fu op={op} label={label} result={result} err={}",
            if outcome.error.is_some() {
                "yes"
            } else {
                "none"
            }
        );
        for status in &outcome.statuses {
            println!(
                "fu op={op} status provider={} type={} status={} message={}",
                status.provider_id, status.provider_type, status.status, status.message
            );
        }
        print_tables(&pool, op).await;
    }
}

async fn print_tables(pool: &PgPool, op: usize) {
    let rows = sqlx::query(
        r#"
SELECT id, user_id, service_type, service_id, account_id,
       (expired_at IS NOT NULL) AS expired,
       (last_valid_at IS NOT NULL) AS valid
FROM user_external_accounts
WHERE id BETWEEN 9900 AND 9999
ORDER BY id
"#,
    )
    .fetch_all(pool)
    .await
    .expect("external accounts");
    for row in rows {
        let id: i32 = row.get("id");
        let user_id: i32 = row.get("user_id");
        let service_type: String = row.get("service_type");
        let service_id: String = row.get("service_id");
        let account_id: String = row.get("account_id");
        let expired: bool = row.get("expired");
        let valid: bool = row.get("valid");
        println!(
            "fu op={op} acct id={id} user={user_id} type={service_type} sid={service_id} \
             account={account_id} expired={} valid={}",
            if expired { 1 } else { 0 },
            if valid { 1 } else { 0 }
        );
    }

    let rows = sqlx::query(
        r#"
SELECT user_id, user_external_account_id, repo_id, source
FROM user_repo_permissions
WHERE user_id BETWEEN 9930 AND 9999 OR repo_id BETWEEN 9930 AND 9999
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
        println!("fu op={op} urp user={user_id} ext={ext_id} repo={repo_id} source={source}");
    }

    let rows = sqlx::query(
        r#"
SELECT repo_id, permission, unrestricted,
       array_to_string(ARRAY(SELECT unnest(user_ids_ints) ORDER BY 1), ',') AS users
FROM repo_permissions
WHERE repo_id BETWEEN 9930 AND 9999
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
            "fu op={op} rp repo={repo_id} perm={permission} unrestricted={} users={users}",
            if unrestricted { 1 } else { 0 }
        );
    }

    let rows = sqlx::query(
        r#"
SELECT user_id, permission, object_type,
       array_to_string(ARRAY(SELECT unnest(object_ids_ints) ORDER BY 1), ',') AS ids
FROM user_permissions
WHERE user_id BETWEEN 9930 AND 9999
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
        println!("fu op={op} up user={user_id} perm={permission} object={object_type} ids={ids}");
    }
}
