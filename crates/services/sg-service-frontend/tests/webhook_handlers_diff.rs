//! M9.44b differential: GitHub webhook repo/user authz side effects
//! (`cmd/frontend/internal/httpapi/webhookhandlers/zz_m944b_webhook_handlers_oracle_test.go`,
//! driven by `run_webhook_handlers_diff.sh`).
//!
//! Case kinds:
//!
//! * `handler` — call one handler body directly (`handleGitHubRepoAuthzEvent`
//!   / `handleGitHubUserAuthzEvent`);
//! * `intended` — register the event's handlers under `GITHUB` and
//!   `Dispatch` (what `Init` meant to do);
//! * `dispatch` — the actual `webhookhandlers.Init` layout, which is inert
//!   because the event name is passed as `codeHostKind`.
//!
//! Env: `SG_M944B_WEBHOOK_HANDLERS_DIFF=1`, `M944B_CORPUS`, `M944B_OUT`,
//! `SG_QTEST_DSN`.

#![allow(clippy::unwrap_used)]

use serde::{Deserialize, Serialize};

use sg_service_frontend::webhook_authz_handlers::{
    dispatch_github, dispatch_intended, parse_event, run_handler, AuthzHandler,
    AuthzError, FetchPermsOptions, PgAuthzDeps,
};

#[derive(Deserialize)]
struct RepoFixture {
    id: i32,
    name: String,
}

#[derive(Deserialize)]
struct UserFixture {
    id: i32,
}

#[derive(Deserialize)]
struct AccountFixture {
    id: i32,
    #[serde(rename = "userId")]
    user_id: i32,
    #[serde(rename = "serviceType")]
    service_type: String,
    #[serde(rename = "serviceId")]
    service_id: String,
    #[serde(rename = "accountId")]
    account_id: String,
}

#[derive(Deserialize)]
struct Fixtures {
    repos: Vec<RepoFixture>,
    users: Vec<UserFixture>,
    #[serde(rename = "externalAccounts")]
    external_accounts: Vec<AccountFixture>,
}

#[derive(Deserialize)]
struct Case {
    name: String,
    kind: String,
    #[serde(default)]
    handler: String,
    #[serde(rename = "invalidateCaches", default)]
    invalidate_caches: bool,
    #[serde(rename = "eventType")]
    event_type: String,
    body: String,
    #[serde(default)]
    repeat: i32,
}

#[derive(Deserialize)]
struct Corpus {
    fixtures: Fixtures,
    cases: Vec<Case>,
}

#[derive(Serialize)]
struct Job {
    reason: String,
    priority: i32,
    #[serde(rename = "invalidateCaches")]
    invalidate_caches: bool,
    #[serde(rename = "userId")]
    user_id: Option<i32>,
    #[serde(rename = "repositoryId")]
    repository_id: Option<i32>,
    state: String,
    #[serde(rename = "triggeredByUserId")]
    triggered_by_user_id: Option<i32>,
    #[serde(rename = "noPerms")]
    no_perms: bool,
}

#[derive(Serialize)]
struct Output {
    error: String,
    #[serde(rename = "notFound")]
    not_found: bool,
    jobs: Vec<Job>,
}

async fn seed(pool: &sqlx::PgPool, corpus: &Corpus) {
    sqlx::query("DELETE FROM permission_sync_jobs")
        .execute(pool)
        .await
        .unwrap();
    sqlx::query("DELETE FROM user_external_accounts")
        .execute(pool)
        .await
        .unwrap();
    sqlx::query("DELETE FROM users WHERE id BETWEEN 944200 AND 944299")
        .execute(pool)
        .await
        .unwrap();
    sqlx::query("DELETE FROM repo WHERE id BETWEEN 944100 AND 944199")
        .execute(pool)
        .await
        .unwrap();
    for user in &corpus.fixtures.users {
        sqlx::query("INSERT INTO users (id, username) VALUES ($1, $2)")
            .bind(user.id)
            .bind(format!("m944buser{}", user.id))
            .execute(pool)
            .await
            .unwrap();
    }
    for repo in &corpus.fixtures.repos {
        sqlx::query("INSERT INTO repo (id, name, uri) VALUES ($1, $2, $2)")
            .bind(repo.id)
            .bind(&repo.name)
            .execute(pool)
            .await
            .unwrap();
    }
    for account in &corpus.fixtures.external_accounts {
        sqlx::query(
            "INSERT INTO user_external_accounts
             (id, user_id, service_type, service_id, account_id, client_id)
             VALUES ($1, $2, $3, $4, $5, '')",
        )
        .bind(account.id)
        .bind(account.user_id)
        .bind(&account.service_type)
        .bind(&account.service_id)
        .bind(&account.account_id)
        .execute(pool)
        .await
        .unwrap();
    }
}

async fn snapshot(pool: &sqlx::PgPool) -> Vec<Job> {
    use sqlx::Row;
    let rows = sqlx::query(
        "SELECT reason, priority, invalidate_caches, user_id, repository_id, state,
                triggered_by_user_id, no_perms
         FROM permission_sync_jobs
         ORDER BY reason ASC, COALESCE(repository_id, 0) ASC, COALESCE(user_id, 0) ASC, id ASC",
    )
    .fetch_all(pool)
    .await
    .unwrap();
    rows.into_iter()
        .map(|row| Job {
            reason: row.try_get("reason").unwrap(),
            priority: row.try_get("priority").unwrap(),
            invalidate_caches: row.try_get("invalidate_caches").unwrap(),
            user_id: row.try_get("user_id").unwrap(),
            repository_id: row.try_get("repository_id").unwrap(),
            state: row.try_get("state").unwrap(),
            triggered_by_user_id: row.try_get("triggered_by_user_id").unwrap(),
            no_perms: row.try_get("no_perms").unwrap(),
        })
        .collect()
}

fn error_fields(error: Option<&AuthzError>) -> (String, bool) {
    match error {
        Some(error) => (error.message().to_owned(), error.is_not_found()),
        None => (String::new(), false),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn webhook_handlers_diff() {
    if std::env::var("SG_M944B_WEBHOOK_HANDLERS_DIFF").as_deref() != Ok("1") {
        return;
    }
    let corpus_path = std::env::var("M944B_CORPUS").expect("M944B_CORPUS");
    let out_dir = std::env::var("M944B_OUT").expect("M944B_OUT");
    let dsn = std::env::var("SG_QTEST_DSN").expect("SG_QTEST_DSN");
    std::fs::create_dir_all(&out_dir).unwrap();
    let corpus: Corpus = serde_json::from_slice(&std::fs::read(&corpus_path).unwrap()).unwrap();

    let pool = sqlx::PgPool::connect(&dsn).await.expect("connect postgres");
    seed(&pool, &corpus).await;

    let deps = PgAuthzDeps::new(pool.clone());

    for case in &corpus.cases {
        let event = match parse_event(&case.event_type, case.body.as_bytes()) {
            Ok(event) => event,
            Err(error) => panic!("case {}: parse: {}", case.name, error.message()),
        };
        let repeat = if case.repeat == 0 { 1 } else { case.repeat };
        let mut out_error: Option<AuthzError> = None;
        for _ in 0..repeat {
            sqlx::query("DELETE FROM permission_sync_jobs")
                .execute(&pool)
                .await
                .unwrap();
            out_error = None;
            let result = match case.kind.as_str() {
                "handler" => {
                    let opts = FetchPermsOptions {
                        invalidate_caches: case.invalidate_caches,
                    };
                    let handler = if case.handler == "repo" {
                        AuthzHandler::Repo { opts }
                    } else {
                        AuthzHandler::User { opts }
                    };
                    run_handler(handler, &event, &deps)
                }
                "intended" => dispatch_intended(&case.event_type, &event, &deps),
                "dispatch" => dispatch_github(&case.event_type, &event, &deps),
                other => panic!("case {}: unknown kind {other}", case.name),
            };
            if let Err(error) = result {
                out_error = Some(error);
            }
        }

        let (error, not_found) = error_fields(out_error.as_ref());
        let output = Output {
            error,
            not_found,
            jobs: snapshot(&pool).await,
        };
        let encoded = serde_json::to_vec(&output).unwrap();
        std::fs::write(
            std::path::Path::new(&out_dir).join(format!("{}.json", case.name)),
            encoded,
        )
        .unwrap();
        println!("RUST {} done", case.name);
    }
}
