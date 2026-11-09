#![forbid(unsafe_code)]

//! M9.8cx differential harness (Rust side): the periodic half of the
//! `codeintel-autoindexing-scheduler` job (`handle_scheduler`).
//!
//! Mirrors
//! `internal/codeintel/autoindexing/internal/background/scheduler/scheduler_harness_test.go`.
//! `test_ws/p9-go/run_codeintel_autoindexing_scheduler_diff.sh` applies the
//! shared fixtures (`codeintel_autoindexing_scheduler_fixtures.sql`) to a
//! scratch frontend database immediately before each side, then diffs the
//! canonical `caix ` lines: for each case, the per-run errors, the
//! `lsif_last_index_scan` rows and every enqueue call (sorted by repo/rev,
//! with its outcome).
//!
//! gitserver is a `FakeGit` whose hooks mirror the Go harness's
//! `gitserver.MockClient` hooks. The site configuration is parsed per case so
//! the `codeIntelAutoIndexing.*` gate/global-policy/match-limit reads are the
//! production accessors.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use sg_config::SiteConfiguration;
use sg_db::OffsetDateTime;
use sg_service_worker::jobs::codeintel_autoindexing_scheduler::{
    handle_scheduler, IndexEnqueuer, QueueAutoIndexJobsError, SchedulerSettings,
};
use sg_service_worker::jobs::policies_matcher::{
    GetCommitError, MatcherCommit, MatcherGit, MatcherRef, RefType,
};
use sqlx::postgres::PgPool;
use sqlx::Row;
use time::Duration;

const ALPHA_TAG: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const ALPHA_MAIN: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
const ALPHA_FEATURE: &str = "cccccccccccccccccccccccccccccccccccccccc";
const ALPHA_INTER: &str = "dddddddddddddddddddddddddddddddddddddddd";
const BETA_MAIN: &str = "b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1";
const GAMMA_MAIN: &str = "1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a";
const EPSILON_MAIN: &str = "9e9e9e9e9e9e9e9e9e9e9e9e9e9e9e9e9e9e9e9e";
const MISSING_COMMIT: &str = "deadbeefdeadbeefdeadbeefdeadbeefdeadbeef";

/// `2020-01-01T00:00:00Z`; the Rust fixtures store no policy max age, so this
/// date only has to match the Go harness's `caixRefDate`.
fn ref_date() -> OffsetDateTime {
    OffsetDateTime::from_unix_timestamp(1_577_836_800).expect("2020-01-01")
}

fn tag(short_name: &str, commit: &str) -> MatcherRef {
    MatcherRef {
        short_name: short_name.to_owned(),
        commit: commit.to_owned(),
        created_at: ref_date(),
        ref_type: RefType::Tag,
        is_head: false,
    }
}

fn branch(short_name: &str, commit: &str, is_head: bool) -> MatcherRef {
    MatcherRef {
        short_name: short_name.to_owned(),
        commit: commit.to_owned(),
        created_at: ref_date(),
        ref_type: RefType::Branch,
        is_head,
    }
}

fn commit(id: &str, day: i64) -> MatcherCommit {
    MatcherCommit {
        id: id.to_owned(),
        committer_date: ref_date() + Duration::days(day),
    }
}

struct FakeGit;

#[async_trait]
impl MatcherGit for FakeGit {
    async fn list_refs(
        &self,
        repo: &str,
        _points_at_commit: &[String],
    ) -> Result<Vec<MatcherRef>, String> {
        Ok(match repo {
            "github.com/example/alpha" => vec![
                tag("v1", ALPHA_TAG),
                branch("main", ALPHA_MAIN, true),
                branch("feat/x", ALPHA_FEATURE, false),
            ],
            "github.com/example/beta" => vec![branch("main", BETA_MAIN, true)],
            "github.com/example/gamma" => vec![branch("main", GAMMA_MAIN, true)],
            "github.com/example/epsilon" => vec![branch("main", EPSILON_MAIN, true)],
            _ => vec![branch("main", ALPHA_MAIN, true)],
        })
    }

    async fn get_commit(
        &self,
        _repo: &str,
        commit_id: &str,
    ) -> Result<MatcherCommit, GetCommitError> {
        if commit_id == MISSING_COMMIT {
            return Err(GetCommitError::RevisionNotFound);
        }
        Ok(commit(commit_id, 0))
    }

    async fn commits(
        &self,
        repo: &str,
        ranges: &[String],
        _after: Option<OffsetDateTime>,
    ) -> Result<Vec<MatcherCommit>, String> {
        if repo == "github.com/example/alpha" && ranges == ["HEAD..".to_owned() + ALPHA_FEATURE] {
            return Ok(vec![commit(ALPHA_FEATURE, 2), commit(ALPHA_INTER, 3)]);
        }
        Ok(Vec::new())
    }
}

#[derive(Clone, Copy)]
enum Failure {
    RevisionNotFound,
    Error,
}

#[derive(Clone, serde::Serialize)]
struct Call {
    repo: i32,
    rev: String,
    configuration: String,
    force: bool,
    bypass_limit: bool,
    result: &'static str,
}

struct FakeEnqueuer {
    failures: HashMap<(i32, String), Failure>,
    calls: Mutex<Vec<Call>>,
}

impl FakeEnqueuer {
    fn new(failures: &[(i32, &str, Failure)]) -> Self {
        Self {
            failures: failures
                .iter()
                .map(|(repo, rev, failure)| ((*repo, (*rev).to_owned()), *failure))
                .collect(),
            calls: Mutex::new(Vec::new()),
        }
    }

    fn snapshot(&self) -> Vec<Call> {
        let mut calls = self
            .calls
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        // The Go harness records calls from concurrent goroutines and sorts by
        // (repo, rev); duplicate entries are identical.
        calls.sort_by(|a, b| (a.repo, &a.rev).cmp(&(b.repo, &b.rev)));
        calls
    }

    fn count(&self) -> usize {
        self.calls
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .len()
    }
}

#[async_trait]
impl IndexEnqueuer for FakeEnqueuer {
    async fn queue_auto_index_jobs(
        &self,
        repository_id: i32,
        rev: &str,
        configuration: &str,
        force: bool,
        bypass_limit: bool,
    ) -> Result<(), QueueAutoIndexJobsError> {
        let failure = self.failures.get(&(repository_id, rev.to_owned())).copied();
        let result = match failure {
            None => "ok",
            Some(Failure::RevisionNotFound) => "revision-not-found",
            Some(Failure::Error) => "error",
        };
        self.calls
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(Call {
                repo: repository_id,
                rev: rev.to_owned(),
                configuration: configuration.to_owned(),
                force,
                bypass_limit,
                result,
            });
        match failure {
            None => Ok(()),
            Some(Failure::RevisionNotFound) => Err(QueueAutoIndexJobsError::RevisionNotFound),
            Some(Failure::Error) => Err(QueueAutoIndexJobsError::Other("boom".to_owned())),
        }
    }
}

#[derive(serde::Serialize)]
struct Snapshot {
    case: &'static str,
    errors: Vec<String>,
    scanned: Vec<i32>,
    calls: Vec<Call>,
    call_count_per_run: Vec<usize>,
}

struct Case {
    name: &'static str,
    enabled: bool,
    allow_global: bool,
    match_limit: Option<i64>,
    repository_batch_size: i64,
    policy_batch_size: i64,
    inference_concurrency: i64,
    process_delay_secs: u64,
    runs: usize,
    failures: &'static [(i32, &'static str, Failure)],
}

fn site_json(case: &Case) -> String {
    let mut config = serde_json::json!({
        "codeIntelAutoIndexing.enabled": case.enabled,
        "codeIntelAutoIndexing.allowGlobalPolicies": case.allow_global,
    });
    if let Some(limit) = case.match_limit {
        config["codeIntelAutoIndexing.policyRepositoryMatchLimit"] = serde_json::json!(limit);
    }
    config.to_string()
}

async fn delete_scans(pool: &PgPool) {
    sqlx::query("DELETE FROM lsif_last_index_scan WHERE repository_id BETWEEN 9300 AND 9399")
        .execute(pool)
        .await
        .expect("reset scans");
}

async fn current_scans(pool: &PgPool) -> Vec<i32> {
    sqlx::query(
        "SELECT repository_id FROM lsif_last_index_scan \
         WHERE repository_id BETWEEN 9300 AND 9399 ORDER BY repository_id",
    )
    .fetch_all(pool)
    .await
    .expect("query scans")
    .iter()
    .map(|row| row.get("repository_id"))
    .collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn autoindexing_scheduler_matches_go_harness() {
    if std::env::var("SG_AUTOINDEXING_SCHEDULER_DIFF")
        .ok()
        .as_deref()
        != Some("1")
    {
        return;
    }
    let dsn = std::env::var("SG_AUTOINDEXING_SCHEDULER_TEST_DSN")
        .expect("SG_AUTOINDEXING_SCHEDULER_TEST_DSN");
    let pool = sg_db::connect(&dsn, 10).await.expect("frontend pool");

    let cases = vec![
        Case {
            name: "disabled",
            enabled: false,
            allow_global: false,
            match_limit: None,
            repository_batch_size: 100,
            policy_batch_size: 100,
            inference_concurrency: 16,
            process_delay_secs: 24 * 3600,
            runs: 1,
            failures: &[],
        },
        Case {
            name: "no-repositories",
            enabled: true,
            allow_global: false,
            match_limit: None,
            repository_batch_size: 0,
            policy_batch_size: 100,
            inference_concurrency: 16,
            process_delay_secs: 24 * 3600,
            runs: 1,
            failures: &[],
        },
        Case {
            name: "default",
            enabled: true,
            allow_global: false,
            match_limit: None,
            repository_batch_size: 100,
            policy_batch_size: 100,
            inference_concurrency: 16,
            process_delay_secs: 24 * 3600,
            runs: 1,
            failures: &[],
        },
        Case {
            name: "pagination",
            enabled: true,
            allow_global: false,
            match_limit: None,
            repository_batch_size: 2,
            policy_batch_size: 2,
            inference_concurrency: 16,
            process_delay_secs: 24 * 3600,
            runs: 1,
            failures: &[],
        },
        Case {
            name: "allow-global",
            enabled: true,
            allow_global: true,
            match_limit: Some(5),
            repository_batch_size: 100,
            policy_batch_size: 100,
            inference_concurrency: 16,
            process_delay_secs: 24 * 3600,
            runs: 1,
            failures: &[],
        },
        Case {
            name: "global-limit-one",
            enabled: true,
            allow_global: true,
            match_limit: Some(1),
            repository_batch_size: 100,
            policy_batch_size: 100,
            inference_concurrency: 16,
            process_delay_secs: 24 * 3600,
            runs: 1,
            failures: &[],
        },
        Case {
            name: "revision-not-found",
            enabled: true,
            allow_global: false,
            match_limit: None,
            repository_batch_size: 100,
            policy_batch_size: 100,
            inference_concurrency: 16,
            process_delay_secs: 24 * 3600,
            runs: 1,
            failures: &[(9302, "HEAD", Failure::RevisionNotFound)],
        },
        Case {
            name: "enqueuer-error",
            enabled: true,
            allow_global: false,
            match_limit: None,
            repository_batch_size: 100,
            policy_batch_size: 100,
            inference_concurrency: 16,
            process_delay_secs: 24 * 3600,
            runs: 1,
            failures: &[(9306, "HEAD", Failure::Error)],
        },
        Case {
            name: "multiple-errors",
            enabled: true,
            allow_global: false,
            match_limit: None,
            repository_batch_size: 100,
            policy_batch_size: 100,
            inference_concurrency: 1,
            process_delay_secs: 24 * 3600,
            runs: 1,
            failures: &[
                (9305, "HEAD", Failure::Error),
                (9306, "HEAD", Failure::Error),
            ],
        },
        Case {
            name: "twice-with-delay",
            enabled: true,
            allow_global: false,
            match_limit: None,
            repository_batch_size: 100,
            policy_batch_size: 100,
            inference_concurrency: 16,
            process_delay_secs: 24 * 3600,
            runs: 2,
            failures: &[],
        },
        Case {
            name: "twice-zero-delay",
            enabled: true,
            allow_global: false,
            match_limit: None,
            repository_batch_size: 100,
            policy_batch_size: 100,
            inference_concurrency: 16,
            process_delay_secs: 0,
            runs: 2,
            failures: &[],
        },
    ];

    for case in &cases {
        let site = SiteConfiguration::parse(&site_json(case)).expect("site config");
        let enqueuer = Arc::new(FakeEnqueuer::new(case.failures));
        let settings = SchedulerSettings {
            repository_process_delay: std::time::Duration::from_secs(case.process_delay_secs),
            repository_batch_size: case.repository_batch_size,
            policy_batch_size: case.policy_batch_size,
            inference_concurrency: case.inference_concurrency,
        };

        delete_scans(&pool).await;

        let mut errors = Vec::with_capacity(case.runs);
        let mut counts = Vec::with_capacity(case.runs);
        let mut previous = 0;
        for _ in 0..case.runs {
            let result = handle_scheduler(
                &pool,
                &site,
                Arc::new(FakeGit),
                Arc::clone(&enqueuer) as Arc<dyn IndexEnqueuer>,
                &settings,
                OffsetDateTime::now_utc(),
            )
            .await;
            errors.push(result.err().unwrap_or_else(|| "-".to_owned()));
            let count = enqueuer.count();
            counts.push(count - previous);
            previous = count;
        }

        let snapshot = Snapshot {
            case: case.name,
            errors,
            scanned: current_scans(&pool).await,
            calls: enqueuer.snapshot(),
            call_count_per_run: counts,
        };
        println!("caix {}", serde_json::to_string(&snapshot).expect("json"));
    }
}
