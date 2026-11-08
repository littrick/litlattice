#![forbid(unsafe_code)]

//! M9.8ai differential harness (Rust side): the
//! `codeintel-syntactic-indexing-scheduler` job.
//!
//! Mirrors `internal/codeintel/syntactic_indexing/harness_scheduler_test.go`.
//! `test_ws/p9-go/run_codeintel_syntactic_indexing_diff.sh` resets the scratch
//! database, applies the shared fixtures, and diffs the canonical lines: job
//! metadata/config, the routine metadata, and the state produced by two
//! `schedule` invocations (the second an hour later, which re-claims the
//! repositories whose `last_changed` moved past their scan).
//!
//! gitserver is a `FakeGit` derived from the repository name exactly like the
//! Go harness's `gitserver.MockClient` hooks.

use std::sync::Arc;

use async_trait::async_trait;
use sg_config::{LiveConfig, ServiceConnections, Unified};
use sg_core::EnvReader;
use sg_service_worker::jobs::codeintel_syntactic_indexing_scheduler::{
    schedule, SyntacticIndexingScheduler,
};
use sg_service_worker::jobs::policies_matcher::{
    GetCommitError, MatcherCommit, MatcherGit, MatcherRef, RefType,
};
use sg_service_worker::jobs::{Job, JobConfig, JobDeps};
use sqlx::postgres::{PgPool, PgPoolOptions};
use sqlx::Row;
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;
use tokio_util::sync::CancellationToken;

const ALPHA_TAG: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const ALPHA_MAIN: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
const ALPHA_FEATURE: &str = "cccccccccccccccccccccccccccccccccccccccc";
const MISSING_COMMIT: &str = "deadbeefdeadbeefdeadbeefdeadbeefdeadbeef";
const BETA_TAG_COMMIT: &str = "a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1";
const BETA_MAIN_COMMIT: &str = "b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1";
const GAMMA_TAG_COMMIT: &str = "2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b";
const GAMMA_MAIN_COMMIT: &str = "1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a";
const DEFAULT_MAIN_COMMIT: &str = "9999999999999999999999999999999999999999";

/// `2020-01-01T00:00:00Z`; the scheduler compares commit/ref dates against
/// the policy max age.
fn ref_date() -> OffsetDateTime {
    OffsetDateTime::from_unix_timestamp(1_577_836_800).expect("2020-01-01")
}

fn round_time(offset_hours: i64) -> OffsetDateTime {
    OffsetDateTime::parse("2024-06-01T00:00:00Z", &Rfc3339).expect("2024-06-01")
        + time::Duration::hours(offset_hours)
}

struct FakeGit;

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
        committer_date: ref_date() + time::Duration::days(day),
    }
}

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
                branch("feature", ALPHA_FEATURE, false),
            ],
            "github.com/example/beta" => vec![
                tag("x", BETA_TAG_COMMIT),
                branch("main", BETA_MAIN_COMMIT, true),
            ],
            "github.com/example/gamma" => vec![
                tag("v9", GAMMA_TAG_COMMIT),
                branch("main", GAMMA_MAIN_COMMIT, true),
            ],
            _ => vec![branch("main", DEFAULT_MAIN_COMMIT, true)],
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
        if repo == "github.com/example/alpha" && ranges == ["HEAD".to_owned()] {
            return Ok(vec![
                commit(ALPHA_MAIN, 1),
                commit("dddddddddddddddddddddddddddddddddddddddd", 2),
            ]);
        }
        Ok(Vec::new())
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn syntactic_indexing_scheduler_matches_go_harness() {
    if std::env::var("SG_SYNTACTIC_DIFF").ok().as_deref() != Some("1") {
        return;
    }

    let dsn = std::env::var("SG_SYNTACTIC_DSN").expect("SG_SYNTACTIC_DSN");

    let job = SyntacticIndexingScheduler::new();
    job.config().load(&mut EnvReader::new());
    let config = job.config();

    println!("sched meta description={:?}", job.description());
    println!(
        "sched config interval_ms={} policy_batch={} repository_batch={} repository_delay_ms={}",
        config.scheduler_interval().as_millis(),
        config.policy_batch_size(),
        config.repository_batch_size(),
        config.repository_process_delay().as_millis(),
    );

    let deps = JobDeps {
        frontend: PgPoolOptions::new()
            .connect_lazy("postgres://unused@localhost/unused")
            .expect("lazy pool"),
        live_config: Arc::new(LiveConfig::new(Unified {
            service_connections: ServiceConnections {
                git_servers: vec!["127.0.0.1:3178".to_owned()],
                ..Default::default()
            },
            ..Default::default()
        })),
        keyring: Arc::new(sg_keyring::Keyring::default()),
    };
    let routines = job
        .routines(&deps, CancellationToken::new())
        .await
        .expect("routines");
    assert_eq!(routines.len(), 1);
    let routine = &routines[0];
    println!("sched routine name={}", routine.name());
    println!("sched routine description={}", routine.description());
    println!(
        "sched routine interval_ms={}",
        routine.interval().as_millis()
    );

    let frontend = sg_db::connect(&dsn, 10).await.expect("frontend pool");
    let fake_git = FakeGit;
    let settings = config.settings();

    for (round, offset_hours) in [(1, 0), (2, 1)] {
        let err = schedule(&frontend, &fake_git, &settings, round_time(offset_hours)).await;
        if let Err(err) = &err {
            println!("sched round{round} err=some");
            eprintln!("SCHEDDBG round{round}: {err}");
        } else {
            println!("sched round{round} err=ok");
        }
        print_scans(&frontend, round).await;
        print_jobs(&frontend, round).await;
    }
}

async fn print_scans(pool: &PgPool, round: u32) {
    let rows = sqlx::query(
        "SELECT repository_id FROM syntactic_scip_last_index_scan \
         WHERE repository_id BETWEEN 9100 AND 9199 ORDER BY repository_id",
    )
    .fetch_all(pool)
    .await
    .expect("query scans");
    for row in rows {
        let id: i32 = row.get("repository_id");
        println!("sched round{round} lastscan repo={id} present=true");
    }
}

async fn print_jobs(pool: &PgPool, round: u32) {
    let rows = sqlx::query(
        "SELECT repository_id, commit, state, enqueuer_user_id \
         FROM syntactic_scip_indexing_jobs \
         WHERE repository_id BETWEEN 9100 AND 9199 \
         ORDER BY repository_id, commit, id",
    )
    .fetch_all(pool)
    .await
    .expect("query jobs");
    for row in rows {
        let repository_id: i32 = row.get("repository_id");
        let commit: String = row.get("commit");
        let state: String = row.get("state");
        let user_id: i32 = row.get("enqueuer_user_id");
        println!(
            "sched round{round} job repo={repository_id} commit={commit} state={state} uid={user_id}"
        );
    }
}
