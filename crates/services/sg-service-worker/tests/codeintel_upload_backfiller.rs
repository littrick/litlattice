#![forbid(unsafe_code)]

//! M9.8r differential harness (Rust side): the `codeintel-upload-backfiller`
//! job.
//!
//! Mirrors
//! `internal/codeintel/uploads/internal/background/backfiller/backfill_harness_test.go`.
//! `test_ws/p9-go/run_codeintel_upload_backfiller_diff.sh` resets a scratch
//! frontend database, applies the shared fixture, and diffs the canonical
//! lines below against the Go harness. Both sides run
//! `BackfillCommittedAtBatch(batchSize=3)` three times and print the resulting
//! `codeintel_commit_dates` / `lsif_dirty_repositories` state.
//!
//! The gitserver lookup is stubbed: `bbbb...` is a `RevisionNotFound`, the
//! other commits map to fixed UTC timestamps.

use std::sync::Arc;

use async_trait::async_trait;
use sg_config::{LiveConfig, ServiceConnections, Unified};
use sg_core::EnvReader;
use sg_service_worker::jobs::codeintel_upload_backfiller::{
    backfill_committed_at_batch, CommitDate, CommitDateResolver, UploadBackfiller,
};
use sg_service_worker::jobs::{Job, JobConfig, JobDeps};
use sqlx::postgres::{PgPool, PgPoolOptions};
use sqlx::Row;
use tokio_util::sync::CancellationToken;

const COMMIT_KNOWN_1: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const COMMIT_GONE: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
const COMMIT_KNOWN_2: &str = "cccccccccccccccccccccccccccccccccccccccc";
const COMMIT_KNOWN_3: &str = "1111111111111111111111111111111111111111";

struct StubResolver;

#[async_trait]
impl CommitDateResolver for StubResolver {
    async fn resolve(&self, _repository_name: &str, commit: &str) -> Result<CommitDate, String> {
        Ok(match commit {
            // 2020-01-02T03:04:05Z, with fractional seconds to exercise the
            // RFC3339 truncation.
            COMMIT_KNOWN_1 => CommitDate::Timestamp {
                seconds: 1_577_934_245,
                nanos: 123_456_789,
            },
            // 2020-02-03T04:05:06Z.
            COMMIT_KNOWN_2 => CommitDate::Timestamp {
                seconds: 1_580_702_706,
                nanos: 0,
            },
            // 2020-03-04T05:06:07Z.
            COMMIT_KNOWN_3 => CommitDate::Timestamp {
                seconds: 1_583_298_367,
                nanos: 0,
            },
            COMMIT_GONE => CommitDate::RevisionNotFound,
            other => panic!("unexpected commit {other}"),
        })
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn upload_backfiller_matches_go_harness() {
    if std::env::var("SG_BACKFILL_DIFF").ok().as_deref() != Some("1") {
        return;
    }

    let dsn = std::env::var("SG_BACKFILL_DSN").expect("SG_BACKFILL_DSN");

    let job = UploadBackfiller::new();
    job.config().load(&mut EnvReader::new());
    let config = job.config();

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

    println!("jmeta description={:?}", job.description());
    println!(
        "jconfig interval={} batch_size={}",
        config.interval().as_secs(),
        config.batch_size()
    );
    println!(
        "jroutine {}|{}",
        routines[0].name(),
        routines[0].description()
    );

    let pool = sg_db::connect(&dsn, 10).await.expect("frontend pool");

    for n in 1..=3 {
        let result = backfill_committed_at_batch(&pool, &StubResolver, 3).await;
        println!("backfill n={n} ok={}", result.is_ok());
        if let Err(err) = result {
            panic!("backfill n={n}: {err}");
        }
        println!("backfill n={n} dates={}", commit_dates(&pool).await);
        println!("backfill n={n} dirty={}", dirty_repositories(&pool).await);
    }
}

async fn commit_dates(pool: &PgPool) -> String {
    let rows = sqlx::query(
        "SELECT repository_id, encode(commit_bytea, 'hex'), COALESCE(committed_at::text, '') \
         FROM codeintel_commit_dates ORDER BY repository_id, commit_bytea",
    )
    .fetch_all(pool)
    .await
    .expect("commit dates");
    let rendered: Vec<String> = rows
        .iter()
        .map(|row| {
            format!(
                "{}:{}={}",
                row.get::<i32, _>(0),
                row.get::<String, _>(1),
                row.get::<String, _>(2)
            )
        })
        .collect();
    format!("<{}>", rendered.join(","))
}

async fn dirty_repositories(pool: &PgPool) -> String {
    let rows = sqlx::query(
        "SELECT repository_id, dirty_token FROM lsif_dirty_repositories ORDER BY repository_id",
    )
    .fetch_all(pool)
    .await
    .expect("dirty repositories");
    let rendered: Vec<String> = rows
        .iter()
        .map(|row| format!("{}:{}", row.get::<i32, _>(0), row.get::<i32, _>(1)))
        .collect();
    format!("<{}>", rendered.join(","))
}
