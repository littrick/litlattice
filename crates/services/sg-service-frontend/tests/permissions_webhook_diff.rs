//! M9.45b differential: `PermissionsGitHubWebhook`
//! (`cmd/frontend/internal/authz/webhooks/zz_m945b_permissions_webhook_oracle_test.go`,
//! driven by `run_permissions_webhook_diff.sh`).
//!
//! The Go oracle captures `permssync.SchedulePermsSync` intents (the real
//! write is blocked by pgx/v4's `dbutil.NullInt` path) and models the
//! postponed insert against the partial unique index. This harness writes the
//! real `permission_sync_jobs` rows with a pinned `now`, so repeated events
//! collapse through `ON CONFLICT DO NOTHING`.
//!
//! Env: `SG_M945B_PERMISSIONS_WEBHOOK_DIFF=1`, `M945B_CORPUS`, `M945B_OUT`,
//! `SG_QTEST_DSN`.

#![allow(clippy::unwrap_used)]

use serde::{Deserialize, Serialize};
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;

use sg_service_frontend::permissions_webhook::{
    handle_event, parse_event, PgPermissionsWebhookDeps,
};

#[derive(Deserialize)]
struct ExternalServiceFixture {
    id: i64,
    kind: String,
}

#[derive(Deserialize)]
struct RepoFixture {
    id: i32,
    name: String,
    #[serde(rename = "cloneUrl")]
    clone_url: String,
    #[serde(rename = "updatedAt")]
    updated_at: String,
    #[serde(rename = "deletedAt", default)]
    deleted_at: Option<String>,
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
    #[serde(rename = "expiredAt", default)]
    expired_at: Option<String>,
    #[serde(rename = "deletedAt", default)]
    deleted_at: Option<String>,
}

#[derive(Deserialize)]
struct Fixtures {
    #[serde(rename = "externalServices")]
    external_services: Vec<ExternalServiceFixture>,
    repos: Vec<RepoFixture>,
    users: Vec<UserFixture>,
    #[serde(rename = "externalAccounts")]
    external_accounts: Vec<AccountFixture>,
}

#[derive(Deserialize)]
struct Case {
    name: String,
    #[serde(rename = "eventType")]
    event_type: String,
    #[serde(rename = "codeHostURN")]
    code_host_urn: String,
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
    #[serde(rename = "processAfterSeconds")]
    process_after_seconds: i64,
}

#[derive(Serialize)]
struct Output {
    error: String,
    jobs: Vec<Job>,
}

fn parse_time(value: Option<&str>) -> Option<OffsetDateTime> {
    value.map(|value| OffsetDateTime::parse(value, &Rfc3339).unwrap())
}

async fn seed(pool: &sqlx::PgPool, corpus: &Corpus) {
    for stmt in [
        "DELETE FROM permission_sync_jobs",
        "DELETE FROM external_service_repos WHERE external_service_id BETWEEN 945500 AND 945599",
        "DELETE FROM user_external_accounts WHERE id BETWEEN 945300 AND 945399",
        "DELETE FROM users WHERE id BETWEEN 945200 AND 945299",
        "DELETE FROM repo WHERE id BETWEEN 945100 AND 945199",
        "DELETE FROM external_services WHERE id BETWEEN 945500 AND 945599",
    ] {
        sqlx::query(stmt).execute(pool).await.unwrap();
    }
    for service in &corpus.fixtures.external_services {
        sqlx::query(
            "INSERT INTO external_services (id, kind, display_name, config) \
             VALUES ($1, $2, 'm945b', '{}')",
        )
        .bind(service.id)
        .bind(&service.kind)
        .execute(pool)
        .await
        .unwrap();
    }
    for user in &corpus.fixtures.users {
        sqlx::query("INSERT INTO users (id, username) VALUES ($1, $2)")
            .bind(user.id)
            .bind(format!("m945buser{}", user.id))
            .execute(pool)
            .await
            .unwrap();
    }
    for repo in &corpus.fixtures.repos {
        sqlx::query(
            "INSERT INTO repo (id, name, uri, updated_at, deleted_at) \
             VALUES ($1, $2, $2, $3, $4)",
        )
        .bind(repo.id)
        .bind(&repo.name)
        .bind(parse_time(Some(&repo.updated_at)))
        .bind(parse_time(repo.deleted_at.as_deref()))
        .execute(pool)
        .await
        .unwrap();
    }
    let service_id = corpus.fixtures.external_services[0].id;
    for repo in &corpus.fixtures.repos {
        sqlx::query(
            "INSERT INTO external_service_repos (external_service_id, repo_id, clone_url) \
             VALUES ($1, $2, $3)",
        )
        .bind(service_id)
        .bind(repo.id)
        .bind(&repo.clone_url)
        .execute(pool)
        .await
        .unwrap();
    }
    for account in &corpus.fixtures.external_accounts {
        sqlx::query(
            "INSERT INTO user_external_accounts
             (id, user_id, service_type, service_id, account_id, client_id, expired_at, deleted_at)
             VALUES ($1, $2, $3, $4, $5, '', $6, $7)",
        )
        .bind(account.id)
        .bind(account.user_id)
        .bind(&account.service_type)
        .bind(&account.service_id)
        .bind(&account.account_id)
        .bind(parse_time(account.expired_at.as_deref()))
        .bind(parse_time(account.deleted_at.as_deref()))
        .execute(pool)
        .await
        .unwrap();
    }
}

async fn snapshot(pool: &sqlx::PgPool, fixed_now: OffsetDateTime) -> Vec<Job> {
    use sqlx::Row;
    let rows = sqlx::query(
        "SELECT reason, priority, invalidate_caches, user_id, repository_id, state,
                triggered_by_user_id, no_perms, process_after
         FROM permission_sync_jobs
         ORDER BY reason ASC, COALESCE(repository_id, 0) ASC, COALESCE(user_id, 0) ASC, id ASC",
    )
    .fetch_all(pool)
    .await
    .unwrap();
    rows.into_iter()
        .map(|row| {
            let process_after: Option<OffsetDateTime> = row.try_get("process_after").unwrap();
            Job {
                reason: row.try_get("reason").unwrap(),
                priority: row.try_get("priority").unwrap(),
                invalidate_caches: row.try_get("invalidate_caches").unwrap(),
                user_id: row.try_get("user_id").unwrap(),
                repository_id: row.try_get("repository_id").unwrap(),
                state: row.try_get("state").unwrap(),
                triggered_by_user_id: row.try_get("triggered_by_user_id").unwrap(),
                no_perms: row.try_get("no_perms").unwrap(),
                process_after_seconds: process_after
                    .map(|value| (value - fixed_now).whole_seconds())
                    .unwrap_or(0),
            }
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn permissions_webhook_diff() {
    if std::env::var("SG_M945B_PERMISSIONS_WEBHOOK_DIFF").as_deref() != Ok("1") {
        return;
    }
    let corpus_path = std::env::var("M945B_CORPUS").expect("M945B_CORPUS");
    let out_dir = std::env::var("M945B_OUT").expect("M945B_OUT");
    let dsn = std::env::var("SG_QTEST_DSN").expect("SG_QTEST_DSN");
    std::fs::create_dir_all(&out_dir).unwrap();
    let corpus: Corpus = serde_json::from_slice(&std::fs::read(&corpus_path).unwrap()).unwrap();

    let pool = sqlx::PgPool::connect(&dsn).await.expect("connect postgres");
    seed(&pool, &corpus).await;

    // Pinned `now`: `process_after` is deterministic and repeated events
    // collide on the partial unique index.
    let fixed_now = OffsetDateTime::from_unix_timestamp(1_767_225_600).unwrap();
    let deps = PgPermissionsWebhookDeps::with_now(pool.clone(), fixed_now);

    for case in &corpus.cases {
        let event = match parse_event(&case.event_type, case.body.as_bytes()) {
            Ok(event) => event,
            Err(error) => panic!("case {}: parse: {}", case.name, error.message()),
        };
        let repeat = if case.repeat == 0 { 1 } else { case.repeat };
        sqlx::query("DELETE FROM permission_sync_jobs")
            .execute(&pool)
            .await
            .unwrap();
        for _ in 0..repeat {
            // Handler errors are discarded by the baseline.
            let _ = handle_event(&event, &case.code_host_urn, &deps);
        }
        let output = Output {
            error: String::new(),
            jobs: snapshot(&pool, fixed_now).await,
        };
        let encoded = serde_json::to_vec(&output).unwrap();
        std::fs::write(
            std::path::Path::new(&out_dir).join(format!("{}.json", case.name)),
            encoded,
        )
        .unwrap();
    }
}
