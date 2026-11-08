#![forbid(unsafe_code)]

//! M9.8aj differential harness (Rust side): the
//! `github-apps-installation-validation-job`.
//!
//! Mirrors `cmd/worker/internal/githubapps/harness_installation_test.go`.
//! `test_ws/p9-go/run_github_apps_installation_diff.sh` applies the shared
//! fixtures to a scratch frontend database immediately before each side and
//! diffs the canonical lines: job and routine metadata, the aggregated
//! `Handle` result, and every `github_app_installs` row afterwards.
//!
//! The fake factory mirrors the Go harness's `worker.MockGitHubClient`: it
//! returns a client keyed by app name, and fails for the app called
//! `broken-client`. The client page payloads are identical on both sides,
//! including `nil` installations, a `nil` account, a 404 (app deleted) and a
//! 500 (preserve installations).

use std::sync::Arc;

use async_trait::async_trait;
use sg_config::{LiveConfig, ServiceConnections, Unified};
use sg_db::github_apps::GitHubApp;
use sg_keyring::Keyring;
use sg_service_worker::jobs::github_apps_installation::{
    handle, ClientError, GitHubAppClient, GitHubAppClientFactory, GitHubAppsInstallationValidation,
    RemoteAccount, RemoteInstallation,
};
use sg_service_worker::jobs::{Job, JobDeps};
use sqlx::postgres::PgPool;
use sqlx::Row;
use tokio_util::sync::CancellationToken;

fn installation(id: i64, account: Option<RemoteAccount>) -> RemoteInstallation {
    RemoteInstallation {
        id: Some(id),
        html_url: format!("https://github.com/installations/{id}"),
        account,
    }
}

fn account(login: &str) -> RemoteAccount {
    RemoteAccount {
        login: login.to_owned(),
        avatar_url: format!("https://avatars.example.com/{login}"),
        html_url: format!("https://github.com/{login}"),
        account_type: "Organization".to_owned(),
    }
}

struct FakeClient {
    app_name: String,
}

#[async_trait]
impl GitHubAppClient for FakeClient {
    async fn get_app_installations(
        &self,
        page: i32,
    ) -> Result<(Vec<RemoteInstallation>, bool), ClientError> {
        match (self.app_name.as_str(), page) {
            ("alpha-app", 1) => Ok((
                vec![
                    installation(1001, Some(account("alpha-org"))),
                    // Already in the database; the upsert must refresh it.
                    installation(1002, Some(account("alpha-renamed"))),
                ],
                true,
            )),
            ("alpha-app", 2) => Ok((
                vec![installation(1003, Some(account("alpha-team")))],
                false,
            )),
            ("alpha-app", _) => Err(ClientError::other("unexpected page")),
            // The app itself was deleted on GitHub. The message matches the
            // Go `APIError.Error()` text so the aggregated handle error is
            // comparable verbatim.
            ("beta-app", 1) => Err(ClientError::api(
                404,
                "request to https://api.github.com/app/installations?page=1 returned status 404: Not Found",
            )),
            // The installation has no account object: nil-safe getters yield "".
            ("epsilon-app", 1) => Ok((
                vec![RemoteInstallation {
                    id: Some(7005),
                    html_url: "https://github.com/installations/7005".to_owned(),
                    account: None,
                }],
                false,
            )),
            // Nothing installed: every stored row is removed.
            ("zeta-app", 1) => Ok((Vec::new(), false)),
            // A nil installation and one without an ID are skipped, but the
            // stored row is still treated as missing and removed.
            ("eta-app", 1) => Ok((
                vec![
                    RemoteInstallation::default(),
                    RemoteInstallation {
                        id: None,
                        html_url: "https://github.com/installations/none".to_owned(),
                        account: None,
                    },
                ],
                false,
            )),
            // Any non-404 error preserves the stored installations.
            ("theta-app", 1) => Err(ClientError::api(
                500,
                "request to https://api.github.com/app/installations?page=1 returned status 500: boom",
            )),
            _ => Ok((Vec::new(), false)),
        }
    }
}

struct FakeFactory;

impl GitHubAppClientFactory for FakeFactory {
    fn create(&self, app: &GitHubApp) -> Result<Arc<dyn GitHubAppClient>, String> {
        if app.name == "broken-client" {
            return Err("create client".to_owned());
        }
        Ok(Arc::new(FakeClient {
            app_name: app.name.clone(),
        }))
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn github_apps_installation_matches_go_harness() {
    if std::env::var("SG_GHA_DIFF").ok().as_deref() != Some("1") {
        return;
    }
    let dsn = std::env::var("SG_GHA_DSN").expect("SG_GHA_DSN");
    let pool = sg_db::connect(&dsn, 10).await.expect("frontend pool");

    let job = GitHubAppsInstallationValidation::new();
    println!("gha job description={:?}", job.description());
    println!("gha job configs={}", job.configs().len());

    let deps = JobDeps {
        frontend: pool.clone(),
        live_config: Arc::new(LiveConfig::new(Unified {
            service_connections: ServiceConnections::default(),
            ..Default::default()
        })),
        keyring: Arc::new(Keyring::default()),
    };
    let routines = job
        .routines(&deps, CancellationToken::new())
        .await
        .expect("routines");
    assert_eq!(routines.len(), 1);
    let routine = &routines[0];
    println!("gha routine name={}", routine.name());
    println!("gha routine description={:?}", routine.description());
    println!("gha routine interval_ms={}", routine.interval().as_millis());

    match handle(&pool, &deps.keyring, &FakeFactory).await {
        Ok(()) => println!("gha handle err=ok"),
        // The aggregated error text, escaped onto one line.
        Err(err) => println!("gha handle err={err:?}"),
    }

    print_installations(&pool).await;
}

async fn print_installations(pool: &PgPool) {
    let rows = sqlx::query(
        "SELECT app_id, installation_id, \
                COALESCE(url, '') AS url, \
                COALESCE(account_login, '') AS login, \
                COALESCE(account_avatar_url, '') AS avatar, \
                COALESCE(account_url, '') AS account_url, \
                COALESCE(account_type, '') AS account_type \
         FROM github_app_installs \
         WHERE app_id BETWEEN 9600 AND 9699 \
         ORDER BY app_id, installation_id",
    )
    .fetch_all(pool)
    .await
    .expect("query installations");

    for row in rows {
        let app_id: i32 = row.get("app_id");
        let installation_id: i32 = row.get("installation_id");
        let url: String = row.get("url");
        let login: String = row.get("login");
        let avatar: String = row.get("avatar");
        let account_url: String = row.get("account_url");
        let account_type: String = row.get("account_type");
        println!(
            "gha install app={app_id} installation={installation_id} login={login} avatar={avatar} account_url={account_url} type={account_type} url={url}"
        );
    }
}
