#![forbid(unsafe_code)]

//! M9.8o differential harness (Rust side): the `batches-janitor` job.
//!
//! Mirrors `cmd/worker/internal/batches/janitor_harness_test.go`. The fixture
//! `test_ws/p9-go/batches_janitor_fixtures.sql` is applied by
//! `test_ws/p9-go/run_batches_janitor_diff.sh` immediately before each run.
//! The test builds the real job routines (which exercises the same stores and
//! reporter as the worker), then runs the three cleaner statements and the
//! four `ResetStalled` queries, printing the same canonical lines as the Go
//! harness.

use std::sync::Arc;
use std::time::Duration;

use sg_config::{LiveConfig, Unified};
use sg_core::EnvReader;
use sg_db::OffsetDateTime;
use sg_service_worker::jobs::batches::BatchesJanitor;
use sg_service_worker::jobs::{Job, JobConfig, JobDeps};
use sqlx::{PgPool, Row};
use tokio_util::sync::CancellationToken;

#[tokio::test(flavor = "multi_thread")]
async fn batches_janitor_matches_go_harness() {
    let Ok(dsn) = std::env::var("SG_QTEST_DSN") else {
        return;
    };
    let pool = PgPool::connect(&dsn).await.expect("connect");

    let job = BatchesJanitor::new();
    job.config().load(&mut EnvReader::new());
    println!("bmeta description={:?}", job.description());

    let deps = JobDeps {
        frontend: pool.clone(),
        live_config: Arc::new(LiveConfig::new(Unified::default())),
        keyring: Arc::new(sg_keyring::Keyring::default()),
    };
    let routines = job
        .routines(&deps, CancellationToken::new())
        .await
        .expect("routines");
    assert_eq!(routines.len(), 8);
    for (index, routine) in routines.iter().enumerate() {
        println!("broutine {index} name={}", routine.name());
    }

    let now = OffsetDateTime::now_utc();
    sg_db::batches_janitor::delete_unattached_expired_changeset_specs(&pool, now)
        .await
        .expect("delete unattached");
    println!(
        "specs after_unattached={}",
        ids(&pool, "changeset_specs").await
    );
    sg_db::batches_janitor::delete_expired_changeset_specs(&pool, now)
        .await
        .expect("delete expired specs");
    println!(
        "specs after_expired={}",
        ids(&pool, "changeset_specs").await
    );
    sg_db::batches_janitor::delete_expired_batch_specs(&pool, now)
        .await
        .expect("delete expired batch specs");
    println!("batch_specs remaining={}", ids(&pool, "batch_specs").await);

    sg_db::batches_janitor::clean_batch_spec_execution_cache_entries(&pool, 100)
        .await
        .expect("clean cache");
    println!(
        "cache remaining={}",
        ids(&pool, "batch_spec_execution_cache_entries").await
    );

    sg_db::batches_janitor::clean_detached_changesets(&pool, 90 * 24 * 3600)
        .await
        .expect("clean detached");
    println!("changesets remaining={}", ids(&pool, "changesets").await);

    reset(
        &pool,
        "changesets",
        "reconciler_state",
        sg_db::batches_janitor::RECONCILER_WORKER,
        &[9420, 9421, 9422, 9423],
    )
    .await;
    reset(
        &pool,
        "changeset_jobs",
        "state",
        sg_db::batches_janitor::BULK_OPERATION_WORKER,
        &[9440, 9441, 9442],
    )
    .await;
    reset(
        &pool,
        "batch_spec_workspace_execution_jobs",
        "state",
        sg_db::batches_janitor::WORKSPACE_EXECUTION_WORKER,
        &[9460, 9461, 9462],
    )
    .await;
    reset(
        &pool,
        "batch_spec_resolution_jobs",
        "state",
        sg_db::batches_janitor::BATCH_SPEC_RESOLUTION_WORKER,
        &[9470, 9471, 9472],
    )
    .await;
}

async fn reset(
    pool: &PgPool,
    table: &str,
    state_column: &str,
    options: sg_db::batches_janitor::ResetWorkerOptions,
    rows: &[i64],
) {
    let (reset, failed) =
        sg_db::batches_janitor::reset_stalled(pool, options, OffsetDateTime::now_utc())
            .await
            .expect("reset stalled");
    println!(
        "breset {table} reset={} failed={}",
        join_ids(&reset),
        join_ids(&failed)
    );
    for &id in rows {
        dump_reset_row(pool, table, state_column, id).await;
    }
}

fn join_ids(values: &[(i64, Duration)]) -> String {
    if values.is_empty() {
        return "<>".to_owned();
    }
    values
        .iter()
        .map(|(id, _)| id.to_string())
        .collect::<Vec<_>>()
        .join(",")
}

async fn ids(pool: &PgPool, table: &str) -> String {
    // Table names are compile-time literals from this module.
    let query = format!("SELECT id FROM {table} ORDER BY id");
    let values: Vec<i64> = sqlx::query_scalar(&query)
        .fetch_all(pool)
        .await
        .expect("ids");
    if values.is_empty() {
        return "<>".to_owned();
    }
    values
        .iter()
        .map(i64::to_string)
        .collect::<Vec<_>>()
        .join(",")
}

async fn dump_reset_row(pool: &PgPool, table: &str, state_column: &str, id: i64) {
    let query = format!(
        "SELECT {state_column}, num_resets, started_at, finished_at, failure_message \
         FROM {table} WHERE id = $1"
    );
    let row = sqlx::query(&query)
        .bind(id)
        .fetch_one(pool)
        .await
        .expect("dump reset row");
    let state: String = row.get(0);
    let resets: i32 = row.get(1);
    let started: Option<OffsetDateTime> = row.get(2);
    let finished: Option<OffsetDateTime> = row.get(3);
    let failure: Option<String> = row.get(4);
    println!(
        "brow {table} {id} state={state} resets={resets} started={} finished={} failure={}",
        started.is_some(),
        finished.is_some(),
        failure.as_deref().unwrap_or("<null>")
    );
}
