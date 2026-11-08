#![forbid(unsafe_code)]

//! Differential harness for the `repo-embedding-scheduler` job (M9.8ag).
//!
//! Mirror of the Go harness
//! `cmd/worker/internal/embeddings/repo/scheduler_harness_test.go`: same
//! scratch database, same fixtures, same canonical `emb ` lines. Drives the
//! real [`embeddings_config`] and [`run_scheduler_tick`] against the scratch
//! schema-only copy of the frontend database, with a fake default-branch
//! provider standing in for `gitserver`.
//!
//! Gated by `SG_EMBED_SCHED_HARNESS_ON=1`; the connection comes from
//! `SG_EMBED_SCHED_DSN`. The harness script also exports
//! `SRC_INTERNAL_EMBEDDINGS_ALLOWED=true`, matching the baseline's package
//! variable.

use async_trait::async_trait;
use sqlx::postgres::{PgPool, PgPoolOptions};
use sqlx::Row;

use sg_config::SiteConfiguration;
use sg_service_worker::jobs::repo_embedding_scheduler::{
    embeddings_config, run_scheduler_tick, DefaultBranchProvider, EmbeddingsConfig, INTERVAL,
    JOB_DESCRIPTION, ROUTINE_DESCRIPTION, ROUTINE_NAME,
};

const MIN_REPO_ID: i32 = 7101;
const MAX_REPO_ID: i32 = 7113;

async fn exec(pool: &PgPool, statement: &str) {
    sqlx::query(statement)
        .execute(pool)
        .await
        .unwrap_or_else(|err| panic!("{statement}: {err}"));
}

async fn cleanup_fixtures(pool: &PgPool) {
    for statement in [
        "DELETE FROM repo_embedding_jobs",
        "DELETE FROM lsif_configuration_policies_repository_pattern_lookup",
        "DELETE FROM lsif_configuration_policies",
        "DELETE FROM gitserver_repos",
        "DELETE FROM repo",
        "ALTER SEQUENCE repo_embedding_jobs_id_seq RESTART WITH 1",
    ] {
        exec(pool, statement).await;
    }
}

async fn insert_policy(
    pool: &PgPool,
    id: i32,
    repository_id: Option<i32>,
    repository_patterns: Option<&str>,
    embeddings_enabled: bool,
) {
    sqlx::query(
        r#"
INSERT INTO lsif_configuration_policies (
    id, repository_id, name, type, pattern, retention_enabled,
    retain_intermediate_commits, indexing_enabled, index_intermediate_commits,
    repository_patterns, embeddings_enabled
) VALUES ($1, $2, $3, 'GIT_TREE', '**', false, false, false, false, $4, $5)"#,
    )
    .bind(id)
    .bind(repository_id)
    .bind(format!("policy-{id}"))
    .bind(repository_patterns.map(|_| vec!["github.com/acme/*".to_owned()]))
    .bind(embeddings_enabled)
    .execute(pool)
    .await
    .unwrap_or_else(|err| panic!("insert policy {id}: {err}"));
}

async fn insert_job(pool: &PgPool, repo_id: i32, revision: &str, queued_at: &str) {
    sqlx::query(&format!(
        "INSERT INTO repo_embedding_jobs (repo_id, revision, queued_at) VALUES ($1, $2, {queued_at})"
    ))
    .bind(repo_id)
    .bind(revision)
    .execute(pool)
    .await
    .unwrap_or_else(|err| panic!("insert job {repo_id}: {err}"));
}

async fn setup_fixtures(pool: &PgPool) {
    cleanup_fixtures(pool).await;

    // Repos: 7101/7102/7103 differ in stars for the global-policy LIMIT;
    // 7106 is not cloned, 7107 is blocked, 7108 is soft-deleted.
    for id in MIN_REPO_ID..=MAX_REPO_ID {
        let stars = match id {
            7103 => 100,
            7101 => 10,
            7102 => 5,
            _ => 0,
        };
        let blocked = if id == 7107 {
            "'{\"reason\":\"blocked\"}'::jsonb"
        } else {
            "NULL"
        };
        let deleted_at = if id == 7108 { "now()" } else { "NULL" };
        let name = format!("github.com/acme/repo-{id}");
        exec(
            pool,
            &format!(
                "INSERT INTO repo (id, name, uri, stars, blocked, deleted_at) VALUES ({id}, '{name}', '{name}', {stars}, {blocked}, {deleted_at})"
            ),
        )
        .await;

        // `trigger_gitserver_repo_insert` already created the row with the
        // default status, so upsert instead of inserting.
        let clone_status = if id == 7106 { "not_cloned" } else { "cloned" };
        exec(
            pool,
            &format!(
                "INSERT INTO gitserver_repos (repo_id, shard_id, clone_status) VALUES ({id}, 'shard-1', '{clone_status}') ON CONFLICT (repo_id) DO UPDATE SET shard_id = EXCLUDED.shard_id, clone_status = EXCLUDED.clone_status"
            ),
        )
        .await;
    }

    // Policies: 1 global, 2 per-repository (7103), 3 pattern (7104),
    // 4 per-repository (7106, not cloned), 5 per-repository (7105, disabled).
    insert_policy(pool, 1, None, None, true).await;
    insert_policy(pool, 2, Some(7103), None, true).await;
    insert_policy(pool, 3, None, Some("pattern"), true).await;
    insert_policy(pool, 4, Some(7106), None, true).await;
    insert_policy(pool, 5, Some(7105), None, false).await;
    exec(
        pool,
        "INSERT INTO lsif_configuration_policies_repository_pattern_lookup (policy_id, repo_id) VALUES (3, 7104)",
    )
    .await;

    reset_jobs(pool).await;
}

/// Re-seeds the fixture baseline: one recent job (7109 is excluded by the
/// minimum interval), one old job at the current revision (7110 is skipped by
/// the per-revision dedupe) and one old job at another revision (7111 gets a
/// fresh job).
async fn reset_jobs(pool: &PgPool) {
    exec(pool, "DELETE FROM repo_embedding_jobs").await;
    insert_job(pool, 7109, "rev-7109", "now()").await;
    insert_job(pool, 7110, "rev-7110", "now() - interval '30 days'").await;
    insert_job(pool, 7111, "old-rev", "now() - interval '30 days'").await;
}

struct FakeGit;

#[async_trait]
impl DefaultBranchProvider for FakeGit {
    async fn get_default_branch(&self, repo: &str) -> Result<(String, String), String> {
        let id = repo
            .rsplit('-')
            .next()
            .and_then(|suffix| suffix.parse::<i32>().ok())
            .unwrap_or(0);
        match id {
            7112 => Err("gitserver unavailable".to_owned()),
            7113 => Ok((String::new(), String::new())),
            _ => Ok(("refs/heads/main".to_owned(), format!("rev-{id}"))),
        }
    }
}

struct JobRow {
    repo_id: i32,
    revision: String,
    state: String,
}

async fn print_jobs(pool: &PgPool) {
    let rows = sqlx::query("SELECT repo_id, revision, state FROM repo_embedding_jobs")
        .fetch_all(pool)
        .await
        .expect("list jobs");
    let mut jobs: Vec<JobRow> = rows
        .into_iter()
        .map(|row| JobRow {
            repo_id: row.get("repo_id"),
            revision: row.get("revision"),
            state: row.get("state"),
        })
        .collect();
    jobs.sort_by(|a, b| {
        a.repo_id
            .cmp(&b.repo_id)
            .then_with(|| a.revision.cmp(&b.revision))
    });
    println!("emb jobs count={}", jobs.len());
    for job in jobs {
        println!(
            "emb job repo={} revision={:?} state={}",
            job.repo_id, job.revision, job.state
        );
    }
}

fn site(json: &str) -> SiteConfiguration {
    SiteConfiguration::parse(json).expect("valid site config")
}

fn default_site() -> SiteConfiguration {
    site(r#"{"cody.enabled": true, "licenseKey": "license-key"}"#)
}

fn limit_site(limit: i64) -> SiteConfiguration {
    site(&format!(
        r#"{{"cody.enabled": true, "licenseKey": "license-key", "embeddings": {{"policyRepositoryMatchLimit": {limit}}}}}"#
    ))
}

fn print_embeddings_config(name: &str, site: &SiteConfiguration, force: bool) {
    match embeddings_config(site, force) {
        None => println!("emb config name={name} enabled=false interval=- limit=-"),
        Some(EmbeddingsConfig {
            minimum_interval,
            policy_repository_match_limit,
        }) => println!(
            "emb config name={name} enabled=true interval={} limit={}",
            minimum_interval.as_secs(),
            match policy_repository_match_limit {
                Some(limit) => limit.to_string(),
                None => "none".to_owned(),
            }
        ),
    }
}

#[tokio::test]
async fn repo_embedding_scheduler_harness() {
    if std::env::var("SG_EMBED_SCHED_HARNESS_ON").as_deref() != Ok("1") {
        eprintln!("SG_EMBED_SCHED_HARNESS_ON is not set; skipping");
        return;
    }
    let dsn = std::env::var("SG_EMBED_SCHED_DSN").expect("SG_EMBED_SCHED_DSN must be set");
    // The scheduling pass holds one connection for its transaction while it
    // lists repos through the pool (the baseline does the same with
    // `db.Repos()`), so the pool needs more than one connection.
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&dsn)
        .await
        .expect("connect");

    // 1. Job metadata.
    println!("emb job description={JOB_DESCRIPTION:?}");
    println!("emb job configs=0");
    println!("emb routine name={ROUTINE_NAME}");
    println!("emb routine description={ROUTINE_DESCRIPTION:?}");
    println!("emb routine interval={}", INTERVAL.as_secs());

    // 2. `conf.GetEmbeddingsConfig` matrix.
    print_embeddings_config(
        "cody_disabled",
        &site(r#"{"cody.enabled": false, "licenseKey": "license-key"}"#),
        true,
    );
    print_embeddings_config(
        "internal_flag_off",
        &site(r#"{"cody.enabled": true, "licenseKey": "license-key"}"#),
        false,
    );
    print_embeddings_config("no_license", &site(r#"{"cody.enabled": true}"#), true);
    print_embeddings_config("default", &default_site(), true);
    print_embeddings_config(
        "custom",
        &site(
            r#"{"cody.enabled": true, "licenseKey": "license-key", "embeddings": {"minimumInterval": "30s", "policyRepositoryMatchLimit": -1}}"#,
        ),
        true,
    );
    print_embeddings_config(
        "openai_no_token",
        &site(r#"{"cody.enabled": true, "embeddings": {"provider": "openai"}}"#),
        true,
    );
    print_embeddings_config(
        "azure_missing_model",
        &site(
            r#"{"cody.enabled": true, "embeddings": {"provider": "azure-openai", "endpoint": "https://example.com/v1/embeddings"}}"#,
        ),
        true,
    );
    print_embeddings_config(
        "legacy_disabled",
        &site(
            r#"{"cody.enabled": true, "licenseKey": "license-key", "embeddings": {"enabled": false}}"#,
        ),
        true,
    );

    // 3. Scheduling scenarios.
    setup_fixtures(&pool).await;
    let git = FakeGit;

    run_scenario(&pool, &git, "schedule", &default_site()).await;
    run_scenario(&pool, &git, "schedule_twice", &default_site()).await;

    // Each later scenario starts from the fixture baseline so the minimum-
    // interval filter does not hide the arms/limit it is meant to exercise.
    reset_jobs(&pool).await;
    run_scenario(&pool, &git, "limit", &limit_site(2)).await;

    // Without a global policy only the per-repository (7103) and pattern
    // (7104) arms match.
    reset_jobs(&pool).await;
    exec(
        &pool,
        "DELETE FROM lsif_configuration_policies WHERE id = 1",
    )
    .await;
    run_scenario(&pool, &git, "no_global", &default_site()).await;
    insert_policy(&pool, 1, None, None, true).await;

    reset_jobs(&pool).await;
    run_scenario(&pool, &git, "disabled", &site(r#"{"cody.enabled": true}"#)).await;
}

async fn run_scenario(
    pool: &PgPool,
    git: &dyn DefaultBranchProvider,
    name: &str,
    site: &SiteConfiguration,
) {
    let result = run_scheduler_tick(pool, site, git).await;
    println!(
        "emb tick name={name} err={}",
        result.err().unwrap_or_else(|| "-".to_owned())
    );
    print_jobs(pool).await;
}
