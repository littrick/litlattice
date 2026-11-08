//! M9.8at differential harness: the `repo-perms-syncer` job's queue stores,
//! per-type dequeue predicates, final-state transitions, stalled reset and the
//! handler's SaveSyncResult path.
//!
//! Mirrors `cmd/worker/internal/authz/perms_syncer_job_harness_test.go`; the
//! fixture `test_ws/p9-go/perms_syncer_job_fixtures.sql` is applied by
//! `test_ws/p9-go/run_perms_syncer_job_diff.sh` immediately before each run.
//! Gated by `SG_QTEST_DSN`.

use std::sync::Arc;

use sg_db::permission_sync_jobs::{
    PermissionSyncJob, PermissionSyncJobType, PermissionSyncJobsWorkerStore,
};
use sg_service_worker::framework::dbworker::{DbWorkerHandler, DbWorkerStore};
use sg_service_worker::jobs::perms_syncer::{PermsSyncer, ProviderRegistry};
use sg_service_worker::jobs::perms_syncer_fetch::ExternalAccountPermsFetcher;
use sg_service_worker::jobs::perms_syncer_job::{NoAuthzProviders, PermsSyncerWorkerHandler};
use sqlx::postgres::PgPoolOptions;
use sqlx::Row;
use tokio_util::sync::CancellationToken;

fn ids(values: &[i32]) -> String {
    values
        .iter()
        .map(|id| id.to_string())
        .collect::<Vec<_>>()
        .join(",")
}

fn sorted_ids(values: &[(i32, std::time::Duration)]) -> Vec<i32> {
    let mut ids: Vec<i32> = values.iter().map(|(id, _)| *id).collect();
    ids.sort_unstable();
    ids
}

fn record(id: i32) -> PermissionSyncJob {
    PermissionSyncJob {
        id,
        ..Default::default()
    }
}

#[tokio::test]
async fn perms_syncer_job() {
    let Ok(dsn) = std::env::var("SG_QTEST_DSN") else {
        eprintln!("SG_QTEST_DSN unset; skipping");
        return;
    };
    let pool = PgPoolOptions::new()
        .max_connections(4)
        .connect(&dsn)
        .await
        .expect("connect");

    let registry: Arc<dyn ProviderRegistry> = Arc::new(NoAuthzProviders);
    let fetcher = Arc::new(
        ExternalAccountPermsFetcher::new(pool.clone(), Arc::clone(&registry))
            .with_dotcom_mode(false),
    );
    let syncer = Arc::new(PermsSyncer::new(pool.clone(), registry, fetcher));

    let repo_store = Arc::new(PermissionSyncJobsWorkerStore::new(
        pool.clone(),
        PermissionSyncJobType::Repo,
    ));
    let user_store = Arc::new(PermissionSyncJobsWorkerStore::new(
        pool.clone(),
        PermissionSyncJobType::User,
    ));
    let repo_handler = Arc::new(PermsSyncerWorkerHandler::new(
        pool.clone(),
        Arc::clone(&syncer),
    ));
    let user_handler = Arc::new(PermsSyncerWorkerHandler::new(pool.clone(), syncer));

    print_tables(&pool, 0).await;

    let repo_dequeue = DbWorkerStore::dequeue(repo_store.as_ref(), "permjobhost").await;
    match &repo_dequeue {
        Ok(Some(job)) => println!("pj op=1 dequeue=repo ok=true id={} err=none", job.id),
        Ok(None) => println!("pj op=1 dequeue=repo ok=false id=0 err=none"),
        Err(err) => println!("pj op=1 dequeue=repo ok=false id=0 err=yes ({err})"),
    }
    print_tables(&pool, 1).await;

    let user_dequeue = DbWorkerStore::dequeue(user_store.as_ref(), "permjobhost").await;
    match &user_dequeue {
        Ok(Some(job)) => println!("pj op=2 dequeue=user ok=true id={} err=none", job.id),
        Ok(None) => println!("pj op=2 dequeue=user ok=false id=0 err=none"),
        Err(err) => println!("pj op=2 dequeue=user ok=false id=0 err=yes ({err})"),
    }
    print_tables(&pool, 2).await;

    let heartbeat = DbWorkerStore::heartbeat(
        repo_store.as_ref(),
        &[
            "11003".to_owned(),
            "11004".to_owned(),
            "11006".to_owned(),
            "11999".to_owned(),
        ],
    )
    .await;
    match heartbeat {
        Ok((signals, canceled)) => {
            let mut known: Vec<i32> = signals.iter().filter_map(|id| id.parse().ok()).collect();
            let mut canceled: Vec<i32> = canceled.iter().filter_map(|id| id.parse().ok()).collect();
            // The Go harness sorts the returned ids (`sort.Strings`), so the
            // order coming out of the UPDATE ... RETURNING is not meaningful.
            known.sort_unstable();
            canceled.sort_unstable();
            println!(
                "pj op=3 heartbeat known={} canceled={} err=none",
                ids(&known),
                ids(&canceled)
            );
        }
        Err(err) => println!("pj op=3 heartbeat err=yes ({err})"),
    }
    print_tables(&pool, 3).await;

    let updated = DbWorkerStore::mark_complete(repo_store.as_ref(), &record(11003)).await;
    println!(
        "pj op=4 mark-complete id=11003 updated={} err={}",
        matches!(&updated, Ok(true)),
        if updated.is_err() { "yes" } else { "none" }
    );
    print_tables(&pool, 4).await;

    let updated = DbWorkerStore::mark_errored(repo_store.as_ref(), &record(11004), "boom").await;
    println!(
        "pj op=5 mark-errored id=11004 updated={} err={}",
        matches!(&updated, Ok(true)),
        if updated.is_err() { "yes" } else { "none" }
    );
    print_tables(&pool, 5).await;

    let updated =
        DbWorkerStore::mark_failed(repo_store.as_ref(), &record(11006), "canceled-job").await;
    println!(
        "pj op=6 mark-failed id=11006 updated={} err={}",
        matches!(&updated, Ok(true)),
        if updated.is_err() { "yes" } else { "none" }
    );
    print_tables(&pool, 6).await;

    let updated = DbWorkerStore::mark_complete(repo_store.as_ref(), &record(11003)).await;
    println!(
        "pj op=7 mark-complete-again id=11003 updated={} err={}",
        matches!(&updated, Ok(true)),
        if updated.is_err() { "yes" } else { "none" }
    );
    print_tables(&pool, 7).await;

    match repo_store.reset_stalled().await {
        Ok(result) => println!(
            "pj op=8 reset-stalled reset={} failed={} err=none",
            ids(&sorted_ids(&result.reset)),
            ids(&sorted_ids(&result.failed))
        ),
        Err(err) => println!("pj op=8 reset-stalled err=yes ({err})"),
    }
    print_tables(&pool, 8).await;

    let repo_record = PermissionSyncJob {
        id: 11010,
        repository_id: Some(11021),
        ..Default::default()
    };
    let handle = DbWorkerHandler::handle(
        repo_handler.as_ref(),
        CancellationToken::new(),
        &repo_record,
    )
    .await;
    let marked = DbWorkerStore::mark_complete(repo_store.as_ref(), &repo_record).await;
    println!(
        "pj op=9 handle=repo err={} marked={} mark_err={}",
        if handle.is_err() { "yes" } else { "none" },
        matches!(&marked, Ok(true)),
        if marked.is_err() { "yes" } else { "none" }
    );
    print_tables(&pool, 9).await;

    let user_record = PermissionSyncJob {
        id: 11011,
        user_id: Some(11021),
        no_perms: true,
        invalidate_caches: true,
        ..Default::default()
    };
    let handle = DbWorkerHandler::handle(
        user_handler.as_ref(),
        CancellationToken::new(),
        &user_record,
    )
    .await;
    let marked = DbWorkerStore::mark_complete(user_store.as_ref(), &user_record).await;
    println!(
        "pj op=10 handle=user err={} marked={} mark_err={}",
        if handle.is_err() { "yes" } else { "none" },
        matches!(&marked, Ok(true)),
        if marked.is_err() { "yes" } else { "none" }
    );
    print_tables(&pool, 10).await;
}

async fn print_tables(pool: &sqlx::postgres::PgPool, op: usize) {
    let rows = sqlx::query(
        r#"
SELECT id, state, num_resets, num_failures, cancel,
       (started_at IS NOT NULL) AS started,
       (finished_at IS NOT NULL) AS finished,
       COALESCE(failure_message, '<null>') AS failure,
       worker_hostname,
       permissions_added, permissions_removed, permissions_found,
       is_partial_success,
       COALESCE(array_length(code_host_states, 1), 0) AS states
FROM permission_sync_jobs
WHERE id BETWEEN 11000 AND 11099
ORDER BY id
"#,
    )
    .fetch_all(pool)
    .await
    .expect("permission_sync_jobs");
    for row in rows {
        let id: i32 = row.get("id");
        let state: String = row.get("state");
        let num_resets: i32 = row.get("num_resets");
        let num_failures: i32 = row.get("num_failures");
        let cancel: bool = row.get("cancel");
        let started: bool = row.get("started");
        let finished: bool = row.get("finished");
        let failure: String = row.get("failure");
        let worker_hostname: String = row.get("worker_hostname");
        let added: i32 = row.get("permissions_added");
        let removed: i32 = row.get("permissions_removed");
        let found: i32 = row.get("permissions_found");
        let partial: bool = row.get("is_partial_success");
        let states: i32 = row.get("states");
        println!(
            "pj op={op} job={id} state={state} resets={num_resets} failures={num_failures} \
             cancel={} started={} finished={} failure={failure} host={worker_hostname} \
             added={added} removed={removed} found={found} partial={} states={states}",
            if cancel { 1 } else { 0 },
            if started { 1 } else { 0 },
            if finished { 1 } else { 0 },
            if partial { 1 } else { 0 }
        );
    }
}
