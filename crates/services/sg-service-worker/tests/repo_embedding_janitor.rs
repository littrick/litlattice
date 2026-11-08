#![forbid(unsafe_code)]

//! M9.8v differential harness (Rust side): the `repo-embedding-janitor` job.
//!
//! Mirrors
//! `cmd/worker/internal/embeddings/repo/janitor_harness_test.go`.
//! `test_ws/p9-go/run_repo_embedding_janitor_diff.sh` resets a scratch
//! frontend database, applies the shared fixture immediately before each side,
//! and diffs the canonical lines:
//!
//! * `jjob`/`jconfig`: the job's description and config slice;
//! * `jname`: the resetter name (the baseline's `dbworker.Resetter[<name>]`
//!   display form is stripped there, and the Rust name is already bare);
//! * `hreset`/`hages`: `ResetStalled` over the fixture rows, the ids moved to
//!   `queued` / `failed` and the age of their last heartbeat in minutes;
//! * `hrow`: the resulting `repo_embedding_jobs` rows;
//! * `hreset2`: a second `ResetStalled`, which finds nothing left to move.

use std::sync::Arc;
use std::time::Duration;

use sg_config::{LiveConfig, Unified};
use sg_db::repo_embeddings::{self, REPO_EMBEDDING_JOB_WORKER};
use sg_db::OffsetDateTime;
use sg_service_worker::jobs::repo_embedding_janitor::RepoEmbeddingJanitor;
use sg_service_worker::jobs::{Job, JobDeps};
use sqlx::postgres::PgPool;
use sqlx::Row;
use tokio_util::sync::CancellationToken;

#[tokio::test(flavor = "multi_thread")]
async fn repo_embedding_janitor_matches_go_harness() {
    if std::env::var("SG_REPO_EMBEDDING_HARNESS_ON")
        .ok()
        .as_deref()
        != Some("1")
    {
        eprintln!("SG_REPO_EMBEDDING_HARNESS_ON is not set");
        return;
    }

    let dsn = std::env::var("SG_REPO_EMBEDDING_DSN").expect("SG_REPO_EMBEDDING_DSN is not set");
    let pool = sg_db::connect(&dsn, 10).await.expect("frontend pool");

    // 1. Job metadata.
    let job = RepoEmbeddingJanitor::new();
    println!("jjob description={:?}", job.description());
    println!("jconfig count={}", job.configs().len());

    // 2. Resetter name.
    let deps = JobDeps {
        frontend: pool.clone(),
        live_config: Arc::new(LiveConfig::new(Unified::default())),
        keyring: Arc::new(sg_keyring::Keyring::default()),
    };
    let routines = job
        .routines(&deps, CancellationToken::new())
        .await
        .expect("routines");
    assert_eq!(routines.len(), 1);
    println!("jname name={}", routines[0].name());

    // 3. Reset stalled records.
    let (reset, failed) = reset_stalled(&pool).await;
    println!(
        "hreset reset={} failed={} err=ok",
        format_ids(&reset),
        format_ids(&failed)
    );
    println!(
        "hages reset={} failed={}",
        format_ages(&reset),
        format_ages(&failed)
    );

    // 4. Resulting rows.
    print_rows(&pool).await;

    // 5. Second pass is a no-op.
    let (reset, failed) = reset_stalled(&pool).await;
    println!(
        "hreset2 reset={} failed={} err=ok",
        format_ids(&reset),
        format_ids(&failed)
    );
}

async fn reset_stalled(pool: &PgPool) -> (Vec<(i64, Duration)>, Vec<(i64, Duration)>) {
    repo_embeddings::reset_stalled(pool, REPO_EMBEDDING_JOB_WORKER, OffsetDateTime::now_utc())
        .await
        .expect("reset stalled")
}

fn format_ids(ages: &[(i64, Duration)]) -> String {
    let parts: Vec<String> = ages.iter().map(|(id, _)| id.to_string()).collect();
    format!("[{}]", parts.join(","))
}

fn format_ages(ages: &[(i64, Duration)]) -> String {
    let parts: Vec<String> = ages
        .iter()
        .map(|(id, age)| format!("{id}:{}m", age.as_secs() / 60))
        .collect();
    parts.join(",")
}

async fn print_rows(pool: &PgPool) {
    let rows = sqlx::query(
        "SELECT id, state, num_resets, \
                started_at IS NULL AS started_null, \
                finished_at IS NULL AS finished_null, \
                COALESCE(failure_message, '<NULL>') AS failure \
         FROM repo_embedding_jobs \
         WHERE id BETWEEN 9600 AND 9699 \
         ORDER BY id",
    )
    .fetch_all(pool)
    .await
    .expect("rows");

    for row in rows {
        println!(
            "hrow id={} state={} num_resets={} started_null={} finished_null={} failure={}",
            row.get::<i32, _>("id"),
            row.get::<String, _>("state"),
            row.get::<i32, _>("num_resets"),
            row.get::<bool, _>("started_null"),
            row.get::<bool, _>("finished_null"),
            row.get::<String, _>("failure"),
        );
    }
}
