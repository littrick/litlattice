#![forbid(unsafe_code)]

//! M9.8ah differential harness (Rust side): the `codeintel-upload-expirer`
//! job.
//!
//! Mirrors
//! `internal/codeintel/uploads/internal/background/expirer/harness_expirer_test.go`.
//! `test_ws/p9-go/run_codeintel_upload_expirer_diff.sh` resets the scratch
//! databases, applies the shared fixtures, and diffs the canonical lines:
//! job metadata/config, the routine metadata, and the state produced by two
//! `handle_expired_uploads_batch` invocations (the second must claim no
//! repository).
//!
//! gitserver is a `FakeGit` derived from the repository name exactly like the
//! Go harness's `gitserver.MockClient` hooks.

use std::sync::Arc;

use async_trait::async_trait;
use sg_config::{LiveConfig, ServiceConnections, Unified};
use sg_core::EnvReader;
use sg_service_worker::jobs::codeintel_upload_expirer::matcher::{
    GetCommitError, MatcherCommit, MatcherGit, MatcherRef, PolicyMatcher, RefType,
};
use sg_service_worker::jobs::codeintel_upload_expirer::{
    handle_expired_uploads_batch, UploadExpirer,
};
use sg_service_worker::jobs::{Job, JobConfig, JobDeps};
use sqlx::postgres::{PgPool, PgPoolOptions};
use sqlx::Row;
use time::OffsetDateTime;
use tokio_util::sync::CancellationToken;

const ALPHA_TAG: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const ALPHA_MAIN: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
const ALPHA_FEATURE: &str = "cccccccccccccccccccccccccccccccccccccccc";
const ALPHA_INTERMEDIATE: &str = "dddddddddddddddddddddddddddddddddddddddd";
const MISSING_COMMIT: &str = "deadbeefdeadbeefdeadbeefdeadbeefdeadbeef";
const BETA_TAG_COMMIT: &str = "a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1";
const BETA_MAIN_COMMIT: &str = "b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1";
const GAMMA_TAG_COMMIT: &str = "2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b";
const GAMMA_MAIN_COMMIT: &str = "1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a";
const DEFAULT_MAIN_COMMIT: &str = "9999999999999999999999999999999999999999";

/// `2020-01-01T00:00:00Z`; only `CommittedAt`/`CreatedDate`, which the
/// expirer never compares (it compares the *upload* date).
fn ref_date() -> OffsetDateTime {
    OffsetDateTime::from_unix_timestamp(1_577_836_800).expect("2020-01-01")
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

fn commit(id: &str) -> MatcherCommit {
    MatcherCommit {
        id: id.to_owned(),
        committer_date: ref_date(),
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
        Ok(commit(commit_id))
    }

    async fn commits(
        &self,
        repo: &str,
        ranges: &[String],
        _after: Option<OffsetDateTime>,
    ) -> Result<Vec<MatcherCommit>, String> {
        if repo == "github.com/example/alpha" && ranges == ["HEAD".to_owned()] {
            return Ok(vec![commit(ALPHA_MAIN), commit(ALPHA_INTERMEDIATE)]);
        }
        Ok(Vec::new())
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn upload_expirer_matches_go_harness() {
    if std::env::var("SG_EXPIRER_DIFF").ok().as_deref() != Some("1") {
        return;
    }

    let frontend_dsn = std::env::var("SG_EXPIRER_DSN").expect("SG_EXPIRER_DSN");
    let codeintel_dsn =
        std::env::var("SG_EXPIRER_CODEINTEL_DSN").expect("SG_EXPIRER_CODEINTEL_DSN");

    let job = UploadExpirer::new();
    job.config().load(&mut EnvReader::new());
    let config = job.config();

    println!("exp meta description={:?}", job.description());
    println!(
        "exp config commit_batch={} interval_ms={} policy_batch={} repository_batch={} repository_delay_ms={} upload_batch={} upload_delay_ms={}",
        config.commit_batch_size(),
        config.expirer_interval().as_millis(),
        config.policy_batch_size(),
        config.repository_batch_size(),
        config.repository_process_delay().as_millis(),
        config.upload_batch_size(),
        config.upload_process_delay().as_millis(),
    );

    let deps = JobDeps {
        frontend: PgPoolOptions::new()
            .connect_lazy("postgres://unused@localhost/unused")
            .expect("lazy pool"),
        live_config: Arc::new(LiveConfig::new(Unified {
            service_connections: ServiceConnections {
                code_intel_postgres_dsn: codeintel_dsn.clone(),
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
    println!("exp routine name={}", routine.name());
    println!("exp routine description={}", routine.description());
    println!("exp routine interval_ms={}", routine.interval().as_millis());

    let frontend = sg_db::connect(&frontend_dsn, 10)
        .await
        .expect("frontend pool");
    let codeintel = sg_db::connect(&codeintel_dsn, 10)
        .await
        .expect("codeintel pool");

    let fake_git = FakeGit;
    let matcher = PolicyMatcher::new(&fake_git, true, false);
    let settings = config.settings();

    for round in 1..=2 {
        let err = handle_expired_uploads_batch(
            &codeintel,
            &frontend,
            &matcher,
            &settings,
            OffsetDateTime::now_utc(),
        )
        .await;
        if let Err(err) = &err {
            println!("exp round{round} err=some");
            eprintln!("EXPDBG round{round}: {err}");
        } else {
            println!("exp round{round} err=ok");
        }
        print_uploads(&codeintel, round).await;
        print_last_retention_scans(&codeintel, round).await;
    }
}

async fn print_uploads(pool: &PgPool, round: u32) {
    let rows = sqlx::query(
        "SELECT id, expired, last_retention_scan_at IS NOT NULL AS scanned \
         FROM lsif_uploads WHERE id BETWEEN 81000 AND 89999 ORDER BY id",
    )
    .fetch_all(pool)
    .await
    .expect("query uploads");
    for row in rows {
        let id: i32 = row.get("id");
        let expired: bool = row.get("expired");
        let scanned: bool = row.get("scanned");
        println!("exp round{round} upload id={id} expired={expired} scanned={scanned}");
    }
}

async fn print_last_retention_scans(pool: &PgPool, round: u32) {
    let rows = sqlx::query(
        "SELECT repository_id FROM lsif_last_retention_scan \
         WHERE repository_id BETWEEN 8100 AND 8199 ORDER BY repository_id",
    )
    .fetch_all(pool)
    .await
    .expect("query last retention scans");
    for row in rows {
        let id: i32 = row.get("repository_id");
        println!("exp round{round} lastscan repo={id} present=true");
    }
}
