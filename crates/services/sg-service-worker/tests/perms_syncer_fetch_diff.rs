#![forbid(unsafe_code)]

//! M9.12c differential harness (Rust side): the fetch stage of the
//! user-centric sync (`ExternalAccountPermsFetcher::fetch_user_perms`, the
//! Rust half of `fetchUserPermsViaExternalAccounts`).
//!
//! Mirrors `cmd/worker/internal/authz/zz_m9_12c_fetch_oracle_test.go`
//! (untracked). The fixture `perms_syncer_fetch_fixtures.sql` is applied
//! immediately before each side by `run_perms_syncer_fetch_diff.sh`; both
//! sides print `pf ` lines. Gated by `SG_QTEST_DSN`.
//!
//! Focus of the M9.12c slice: `encryptData` on the `FetchAccount` path
//! (auth first, then account data; key ID from the last encryption), expired
//! GitLab accounts, temporary-error retention with and without IPs, partial
//! results with/without `noPerms`, and the empty/unimplemented/unauthorized
//! outcomes.

use std::sync::Arc;

use async_trait::async_trait;
use sg_db::perms_store::CodeHostState;
use sg_db::sub_repo_perms::{PathWithIp, SubRepoPermissionsWithIps};
use sg_db::user_external_accounts::{ExternalAccount, ExternalAccountSpec};
use sg_db::users::UserRecord;
use sg_encryption::NoopKey;
use sg_service_worker::jobs::perms_syncer::{
    AuthzProvider, ExternalUserPermissions, FetchError, FetchPermsOptions, FetchRepo,
    FetchedExternalAccount, ProviderRegistry, SubRepoUpsert, UserPermsError, UserPermsFetcher,
};
use sg_service_worker::jobs::perms_syncer_fetch::ExternalAccountPermsFetcher;
use sqlx::postgres::{PgPool, PgPoolOptions};
use sqlx::Row;

const GITHUB_SID: &str = "https://github.example.com/";
const GITLAB_SID: &str = "https://gitlab.example.com/";

struct PfProvider {
    service_type: &'static str,
    service_id: &'static str,
    urn: &'static str,
}

struct PfRegistry {
    providers: Vec<Arc<dyn AuthzProvider>>,
}

#[async_trait]
impl ProviderRegistry for PfRegistry {
    async fn providers(&self) -> Vec<Arc<dyn AuthzProvider>> {
        self.providers.clone()
    }
}

#[async_trait]
impl AuthzProvider for PfProvider {
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

    async fn fetch_account_with_data(
        &self,
        user: &UserRecord,
    ) -> Result<Option<FetchedExternalAccount>, String> {
        if self.service_id == GITHUB_SID && user.id == 11001 {
            return Ok(Some(FetchedExternalAccount {
                spec: ExternalAccountSpec {
                    service_type: "github".to_owned(),
                    service_id: GITHUB_SID.to_owned(),
                    client_id: String::new(),
                    account_id: "new1".to_owned(),
                },
                account_data: br#"{"login":"enc"}"#.to_vec(),
                auth_data: br#"{"token":"t"}"#.to_vec(),
            }));
        }
        Ok(None)
    }

    async fn fetch_user_perms(
        &self,
        account: &ExternalAccount,
        _opts: &FetchPermsOptions,
    ) -> Result<Option<ExternalUserPermissions>, UserPermsError> {
        match account.account_id.as_str() {
            "new1" => {
                let mut sub = std::collections::BTreeMap::new();
                sub.insert(
                    "repo-exact1".to_owned(),
                    SubRepoPermissionsWithIps {
                        paths: vec![PathWithIp {
                            path: "/happy/**".to_owned(),
                            ip: "10.9.9.9".to_owned(),
                        }],
                    },
                );
                Ok(Some(ExternalUserPermissions {
                    exacts: vec![
                        "repo-exact1".to_owned(),
                        "repo-exact2".to_owned(),
                        "repo-public".to_owned(),
                    ],
                    include_contains: vec!["repo-cont%".to_owned()],
                    exclude_contains: vec!["repo-cont%bar".to_owned()],
                    sub_repo_permissions: sub,
                }))
            }
            "a11102" => Ok(Some(ExternalUserPermissions {
                exacts: vec!["repo-exact1".to_owned()],
                ..Default::default()
            })),
            "a11103" | "a11104" => Err(UserPermsError::Temporary {
                message: "503 temporary".to_owned(),
            }),
            "a11105" => Err(UserPermsError::Failed {
                message: "boom-partial".to_owned(),
                partial: Some(ExternalUserPermissions {
                    exacts: vec!["repo-exact1".to_owned()],
                    ..Default::default()
                }),
            }),
            "a11106" => Ok(Some(ExternalUserPermissions::default())),
            "a11107" => Err(UserPermsError::Unimplemented {
                message: "FetchUserPerms is unimplemented".to_owned(),
            }),
            "a11108" => Err(UserPermsError::Unauthorized {
                message: "token is revoked".to_owned(),
            }),
            "g11109" => Ok(Some(ExternalUserPermissions {
                exacts: vec!["g-exact".to_owned()],
                ..Default::default()
            })),
            _ => Ok(None),
        }
    }
}

fn print_statuses(op: usize, statuses: &[CodeHostState]) {
    for status in statuses {
        println!(
            "pf op={op} status provider={} type={} status={} message={}",
            status.provider_id, status.provider_type, status.status, status.message
        );
    }
}

async fn print_tables(pool: &PgPool, op: usize) {
    let rows = sqlx::query(
        r#"
SELECT id, user_id, service_type, service_id, account_id,
       (expired_at IS NOT NULL) AS expired,
       (last_valid_at IS NOT NULL) AS valid,
       encryption_key_id, auth_data, account_data
FROM user_external_accounts
WHERE id BETWEEN 11100 AND 11199
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
        let key_id: Option<String> = row.get("encryption_key_id");
        let auth_data: Option<String> = row.get("auth_data");
        let account_data: Option<String> = row.get("account_data");
        println!(
            "pf op={op} acct id={id} user={user_id} type={service_type} sid={service_id} \
             account={account_id} expired={} valid={} key={} auth={} data={}",
            if expired { 1 } else { 0 },
            if valid { 1 } else { 0 },
            key_id.as_deref().unwrap_or("<null>"),
            auth_data.as_deref().unwrap_or("<null>"),
            account_data.as_deref().unwrap_or("<null>"),
        );
    }

    let rows = sqlx::query(
        r#"
SELECT user_id, repo_id,
       array_to_string(ARRAY(SELECT unnest(paths) ORDER BY 1), ',') AS paths,
       COALESCE(array_to_string(ARRAY(SELECT unnest(ips) ORDER BY 1), ','), '<null>') AS ips
FROM sub_repo_permissions
WHERE user_id BETWEEN 11000 AND 11099
ORDER BY user_id, repo_id
"#,
    )
    .fetch_all(pool)
    .await
    .expect("sub repo permissions");
    for row in rows {
        let user_id: i32 = row.get("user_id");
        let repo_id: i32 = row.get("repo_id");
        let paths: String = row.get("paths");
        let ips: String = row.get("ips");
        println!(
            "pf op={op} srp user={user_id} repo={repo_id} paths={paths} ips={ips}"
        );
    }
}

#[tokio::test]
async fn perms_syncer_fetch_matches_go_harness() {
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
        Arc::new(PfProvider {
            service_type: "github",
            service_id: GITHUB_SID,
            urn: "extsvc:github:11001",
        }),
        Arc::new(PfProvider {
            service_type: "gitlab",
            service_id: GITLAB_SID,
            urn: "extsvc:gitlab:11002",
        }),
    ];
    let registry = Arc::new(PfRegistry { providers });
    let fetcher = ExternalAccountPermsFetcher::new(pool.clone(), registry)
        .with_dotcom_mode(false)
        .with_user_external_account_key(Some(Arc::new(NoopKey::default())));

    let ops: [(i32, bool, &str); 11] = [
        (11001, false, "fetch-account-key"),
        (11001, false, "fetch-account-repeat"),
        (11002, false, "normal"),
        (11003, true, "temporary-retain-ips"),
        (11004, true, "temporary-retain-null"),
        (11005, true, "partial-no-perms"),
        (11005, false, "partial-abort"),
        (11006, false, "empty"),
        (11007, false, "unimplemented"),
        (11008, false, "unauthorized"),
        (11009, false, "expired-gitlab"),
    ];

    for (i, (user_id, no_perms, label)) in ops.iter().enumerate() {
        let op = i + 1;
        let user = UserRecord {
            id: *user_id,
            username: format!("pfuser{user_id}"),
            created_at: time::OffsetDateTime::UNIX_EPOCH,
        };
        let fetch = fetcher
            .fetch_user_perms(&user, *no_perms, &FetchPermsOptions::default())
            .await;
        println!(
            "pf op={op} label={label} err={}",
            fetch.error.as_deref().unwrap_or("none")
        );
        print_statuses(op, &fetch.statuses);
        for (account_id, repos) in &fetch.repo_perms {
            let mut repos = repos.clone();
            repos.sort_unstable();
            let joined: Vec<String> = repos.into_iter().map(|id| id.to_string()).collect();
            println!(
                "pf op={op} repo-perms account={account_id} repos={}",
                joined.join(",")
            );
        }
        for (spec, upsert) in &fetch.sub_repo_perms {
            match upsert {
                SubRepoUpsert::WithIps(perms) => {
                    let parts: Vec<String> = perms
                        .paths
                        .iter()
                        .map(|p| format!("{}@{}", p.path, p.ip))
                        .collect();
                    println!(
                        "pf op={op} sub-repo kind=ips id={} type={} sid={} paths={}",
                        spec.id,
                        spec.service_type,
                        spec.service_id,
                        parts.join(",")
                    );
                }
                SubRepoUpsert::Paths(perms) => println!(
                    "pf op={op} sub-repo kind=paths id={} type={} sid={} paths={}",
                    spec.id,
                    spec.service_type,
                    spec.service_id,
                    perms.paths.join(",")
                ),
            }
        }
        print_tables(&pool, op).await;
    }
}
