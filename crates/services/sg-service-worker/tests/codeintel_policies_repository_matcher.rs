#![forbid(unsafe_code)]

//! M9.8u differential harness (Rust side): the
//! `codeintel-policies-repository-matcher` job.
//!
//! Mirrors
//! `internal/codeintel/policies/internal/background/repository_matcher/job_harness_test.go`.
//! `test_ws/p9-go/run_codeintel_policies_repository_matcher_diff.sh` resets a
//! scratch frontend database, applies the shared fixture immediately before
//! each side, and diffs the canonical lines:
//!
//! * `jconfig`: the `Config` values (the script sets the batch size to 2) and
//!   the fallback variable name that wins;
//! * `jsite`: the `codeIntelAutoIndexing.policyRepositoryMatchLimit`
//!   accessor for a missing value and 5/0/-1/-2;
//! * `jroutine`: the routine metadata;
//! * `happly`/`resolved`/`lookup`: three `handleRepositoryMatcherBatch`
//!   invocations with batch size 2, the policies each one resolved and the
//!   resulting lookup rows;
//! * `slimit`/`slimit-lookup`: `UpdateReposMatchingPatterns` with `LIMIT 2`,
//!   without a limit, then with empty patterns.

use std::sync::Arc;

use sg_config::{LiveConfig, SiteConfiguration, Unified};
use sg_core::EnvReader;
use sg_db::codeintel_policies::{self, ConfigurationPolicy};
use sg_db::OffsetDateTime;
use sg_service_worker::framework::BackgroundRoutine;
use sg_service_worker::jobs::codeintel_policies_repository_matcher::{
    apply_matcher_batch, PoliciesRepositoryMatcher, BATCH_SIZE_VARS,
};
use sg_service_worker::jobs::{Job, JobConfig, JobDeps};
use sqlx::postgres::PgPool;
use sqlx::Row;
use tokio_util::sync::CancellationToken;

const HARNESS_POLICY_IDS: [i32; 5] = [9500, 9501, 9502, 9503, 9504];

#[tokio::test(flavor = "multi_thread")]
async fn policies_repository_matcher_matches_go_harness() {
    if std::env::var("SG_POLICY_MATCHER_HARNESS_ON")
        .ok()
        .as_deref()
        != Some("1")
    {
        eprintln!("SG_POLICY_MATCHER_HARNESS_ON is not set");
        return;
    }

    let dsn = std::env::var("SG_POLICY_MATCHER_DSN").expect("SG_POLICY_MATCHER_DSN is not set");
    let pool = sg_db::connect(&dsn, 10).await.expect("frontend pool");

    // 1. Configuration.
    let job = PoliciesRepositoryMatcher::new();
    job.config().load(&mut EnvReader::new());
    let config = job.config();
    println!(
        "jconfig interval_ms={} batch_size={}",
        config.interval().as_millis(),
        config.batch_size()
    );
    println!(
        "jconfig batch_size_var={}",
        choose_fallback(&BATCH_SIZE_VARS)
    );

    // 2. Site configuration accessor.
    let site_cases: [(&str, Option<i64>); 5] = [
        ("missing", None),
        ("value=5", Some(5)),
        ("value=0", Some(0)),
        ("value=-1", Some(-1)),
        ("value=-2", Some(-2)),
    ];
    for (name, value) in site_cases {
        let raw = match value {
            Some(value) => {
                format!(r#"{{"codeIntelAutoIndexing.policyRepositoryMatchLimit": {value}}}"#)
            }
            None => "{}".to_owned(),
        };
        let unified = unified_with_site(&raw);
        println!(
            "jsite {name} limit={}",
            unified
                .site
                .codeintel_autoindexing_policy_repository_match_limit()
        );
    }

    // 3. Routine metadata.
    let live_config = Arc::new(LiveConfig::new(Unified::default()));
    let deps = JobDeps {
        frontend: pool.clone(),
        live_config: live_config.clone(),
        keyring: Arc::new(sg_keyring::Keyring::default()),
    };
    let routines = job
        .routines(&deps, CancellationToken::new())
        .await
        .expect("routines");
    let routine: &BackgroundRoutine = &routines[0];
    println!("jroutine name={}", routine.name());
    println!("jroutine description={}", routine.description());
    println!("jroutine interval_ms={}", routine.interval().as_millis());
    println!(
        "jroutine type={}",
        Arc::new(routine.clone())
            .recordable()
            .expect("periodic routine")
            .routine_type()
            .as_str()
    );

    // 4. Three matcher passes.
    for pass in 1..=3 {
        let before = last_resolved(&pool).await;
        match apply_matcher_batch(&pool, &live_config, config.batch_size()).await {
            Ok(()) => println!("happly pass={pass} err=ok"),
            Err(err) => {
                println!("happly pass={pass} err={err}");
                continue;
            }
        }
        let after = last_resolved(&pool).await;
        println!(
            "resolved pass={pass} ids={}",
            format_ids(&changed_ids(&before, &after))
        );
        print_lookup(&pool, &format!("lookup pass={pass}")).await;
    }

    // 5. Explicit limits.
    update(9504, &["r*".to_owned()], Some(2), &pool).await;
    println!("slimit policy=9504 limit=2 err=ok");
    print_lookup(&pool, "slimit-lookup limit=2").await;

    update(9504, &["r*".to_owned()], None, &pool).await;
    println!("slimit policy=9504 limit=none err=ok");
    print_lookup(&pool, "slimit-lookup limit=none").await;

    update(9504, &[], None, &pool).await;
    println!("slimit policy=9504 patterns=empty err=ok");
    print_lookup(&pool, "slimit-lookup patterns=empty").await;
}

async fn update(policy_id: i32, patterns: &[String], limit: Option<i64>, pool: &PgPool) {
    codeintel_policies::update_repos_matching_patterns(pool, patterns, policy_id, limit)
        .await
        .expect("update repos matching patterns");
}

fn unified_with_site(raw: &str) -> Unified {
    Unified {
        site: SiteConfiguration::parse(raw).expect("site config"),
        ..Default::default()
    }
}

fn choose_fallback(names: &[&str]) -> String {
    names
        .iter()
        .find(|name| {
            std::env::var(name)
                .map(|value| !value.is_empty())
                .unwrap_or(false)
        })
        .copied()
        .unwrap_or(names[0])
        .to_owned()
}

fn format_ids(ids: &[i32]) -> String {
    let parts: Vec<String> = ids.iter().map(|id| id.to_string()).collect();
    format!("[{}]", parts.join(","))
}

async fn last_resolved(pool: &PgPool) -> Vec<(i32, Option<OffsetDateTime>)> {
    let rows = sqlx::query(
        "SELECT id, last_resolved_at FROM lsif_configuration_policies \
         WHERE id BETWEEN 9500 AND 9599",
    )
    .fetch_all(pool)
    .await
    .expect("last_resolved_at");
    rows.into_iter()
        .map(|row| {
            (
                row.get::<i32, _>("id"),
                row.get::<Option<OffsetDateTime>, _>("last_resolved_at"),
            )
        })
        .collect()
}

fn changed_ids(
    before: &[(i32, Option<OffsetDateTime>)],
    after: &[(i32, Option<OffsetDateTime>)],
) -> Vec<i32> {
    let mut ids: Vec<i32> = after
        .iter()
        .filter(|(id, at)| {
            before
                .iter()
                .find(|(previous_id, _)| previous_id == id)
                .map(|(_, previous)| previous != at)
                .unwrap_or(true)
        })
        .map(|(id, _)| *id)
        .collect();
    ids.sort_unstable();
    ids
}

async fn print_lookup(pool: &PgPool, prefix: &str) {
    let rows = sqlx::query(
        "SELECT policy_id, repo_id \
         FROM lsif_configuration_policies_repository_pattern_lookup \
         WHERE policy_id BETWEEN 9500 AND 9599 \
         ORDER BY policy_id, repo_id",
    )
    .fetch_all(pool)
    .await
    .expect("lookup rows");

    for policy_id in HARNESS_POLICY_IDS {
        let repos: Vec<i32> = rows
            .iter()
            .filter(|row| row.get::<i32, _>("policy_id") == policy_id)
            .map(|row| row.get::<i32, _>("repo_id"))
            .collect();
        println!("{prefix} policy={policy_id} repos={}", format_ids(&repos));
    }
}

// Keep `ConfigurationPolicy` referenced so the import documents the mapping;
// the matcher reads `repository_patterns` through it.
#[allow(dead_code)]
fn policy_patterns(policy: &ConfigurationPolicy) -> Vec<String> {
    policy.repository_patterns.clone().unwrap_or_default()
}
