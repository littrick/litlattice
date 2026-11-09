#![forbid(unsafe_code)]

//! Differential harness for the backfill worker stores (M9.8ca).
//!
//! Mirror of the Go harness
//! `internal/insights/scheduler/parity_db_worker_store_harness_test.go`:
//! same fixture scenes, same canonical `ws ` lines. Drives
//! [`BackfillJobStore`] through every operation the generic
//! `dbworkerstore.Store` provides for the two backfill views
//! (`insights_jobs_backfill_new`, `insights_jobs_backfill_in_progress`).
//!
//! Gated by `SG_BACKFILL_STORE_CORPUS`; the connection comes from
//! `SG_QTEST_INSIGHTS_DSN` (a scratch clone of the `codeinsights` database).
//!
//! Wall-clock columns are reported as `set`/`-` only; `process_after` is
//! fixture-set and printed verbatim.

use std::collections::BTreeSet;
use std::time::Duration;

use sqlx::postgres::{PgPool, PgPoolOptions};
use sqlx::types::time::OffsetDateTime;
use sqlx::Row;
use time::format_description::well_known::Rfc3339;
use time::UtcOffset;

use sg_service_worker::jobs::insights_backfill_store::{
    BackfillJob, BackfillJobStore, IN_PROGRESS_STORE, NEW_STORE, STALLED_MAX_AGE,
};

fn set_label(set: bool) -> &'static str {
    if set {
        "set"
    } else {
        "-"
    }
}

fn rfc3339(time: OffsetDateTime) -> String {
    time.to_offset(UtcOffset::UTC)
        .format(&Rfc3339)
        .expect("format timestamp")
}

fn set_str(set: bool) -> String {
    set_label(set).to_owned()
}

fn opt_string(value: &Option<String>) -> String {
    value.clone().unwrap_or_else(|| "-".to_owned())
}

fn opt_i32(value: Option<i32>) -> String {
    value.map_or_else(|| "-".to_owned(), |value| value.to_string())
}

fn opt_time(value: Option<OffsetDateTime>) -> String {
    value.map_or_else(|| "-".to_owned(), rfc3339)
}

fn join_i32s(ids: &BTreeSet<i32>) -> String {
    ids.iter().map(i32::to_string).collect::<Vec<_>>().join(",")
}

fn sorted_ids(mut ids: Vec<i32>) -> BTreeSet<i32> {
    ids.sort_unstable();
    ids.into_iter().collect()
}

async fn print_row(pool: &PgPool, id: i32) {
    let row = sqlx::query(
        r#"
SELECT id, state, failure_message, num_failures, num_resets, cancel,
       worker_hostname, backfill_id, started_at, finished_at, queued_at,
       process_after, last_heartbeat_at
FROM insights_background_jobs WHERE id = $1
"#,
    )
    .bind(id)
    .fetch_one(pool)
    .await
    .unwrap_or_else(|err| panic!("read row {id}: {err}"));

    let backfill_id: Option<i32> = row.try_get("backfill_id").expect("backfill_id");
    let started_at: Option<OffsetDateTime> = row.try_get("started_at").expect("started_at");
    let finished_at: Option<OffsetDateTime> = row.try_get("finished_at").expect("finished_at");
    let queued_at: Option<OffsetDateTime> = row.try_get("queued_at").expect("queued_at");
    let process_after: Option<OffsetDateTime> =
        row.try_get("process_after").expect("process_after");
    let last_heartbeat_at: Option<OffsetDateTime> =
        row.try_get("last_heartbeat_at").expect("last_heartbeat_at");
    let failure_message: Option<String> = row.try_get("failure_message").expect("failure_message");

    println!(
        "ws row id={} state={} failure={} nf={} nr={} cancel={} host={} bid={} started={} finished={} queued={} pa={} hb={}",
        row.try_get::<i32, _>("id").expect("id"),
        row.try_get::<String, _>("state").expect("state"),
        opt_string(&failure_message),
        row.try_get::<i32, _>("num_failures").expect("num_failures"),
        row.try_get::<i32, _>("num_resets").expect("num_resets"),
        row.try_get::<bool, _>("cancel").expect("cancel"),
        row.try_get::<String, _>("worker_hostname").expect("worker_hostname"),
        opt_i32(backfill_id),
        set_str(started_at.is_some()),
        set_str(finished_at.is_some()),
        set_str(queued_at.is_some()),
        opt_time(process_after),
        set_str(last_heartbeat_at.is_some()),
    );
}

fn print_dequeue(store_name: &str, job: Option<BackfillJob>) {
    match job {
        None => println!("ws dequeue store={store_name} found=0"),
        Some(job) => println!(
            "ws dequeue store={} found=1 id={} state={} failure={} nf={} nr={} host={} bid={} started={} finished={} pa={} hb={}",
            store_name,
            job.id,
            job.state,
            opt_string(&job.failure_message),
            job.num_failures,
            job.num_resets,
            job.worker_hostname,
            opt_i32(job.backfill_id),
            set_str(job.started_at.is_some()),
            set_str(job.finished_at.is_some()),
            opt_time(job.process_after),
            set_str(job.last_heartbeat_at.is_some()),
        ),
    }
}

#[tokio::test]
async fn insights_backfill_store_db_parity() {
    let corpus_path = match std::env::var("SG_BACKFILL_STORE_CORPUS") {
        Ok(path) if !path.is_empty() => path,
        _ => return,
    };
    let dsn = std::env::var("SG_QTEST_INSIGHTS_DSN").expect("SG_QTEST_INSIGHTS_DSN is required");
    let pool = PgPoolOptions::new()
        .max_connections(4)
        .connect(&dsn)
        .await
        .expect("connect codeinsights");

    let new_store = BackfillJobStore::new(pool.clone(), NEW_STORE);
    let in_progress_store = BackfillJobStore::new(pool.clone(), IN_PROGRESS_STORE);
    let now = OffsetDateTime::now_utc();
    let corpus = std::fs::read_to_string(&corpus_path).expect("read corpus");

    for line in corpus.lines() {
        if line.is_empty() {
            continue;
        }
        let parts: Vec<&str> = line.split('\t').collect();
        match parts[0] {
            "count" => {
                let store = match parts[1] {
                    "new" => &new_store,
                    "inprogress" => &in_progress_store,
                    other => panic!("unknown store {other}"),
                };
                let states: Vec<&str> = if parts[2] == "-" {
                    Vec::new()
                } else {
                    parts[2].split(',').collect()
                };
                let count = store.count_by_state(&states).await.expect("count_by_state");
                println!(
                    "ws count store={} states={} count={}",
                    parts[1], parts[2], count
                );
            }
            "exists" => {
                let store = match parts[1] {
                    "new" => &new_store,
                    "inprogress" => &in_progress_store,
                    other => panic!("unknown store {other}"),
                };
                let states: Vec<&str> = if parts[2] == "-" {
                    Vec::new()
                } else {
                    parts[2].split(',').collect()
                };
                let exists = store.exists(&states).await.expect("exists");
                println!(
                    "ws exists store={} states={} exists={}",
                    parts[1], parts[2], exists
                );
            }
            "dequeue" => {
                let (name, store) = match parts[1] {
                    "new" => ("new", &new_store),
                    "inprogress" => ("inprogress", &in_progress_store),
                    other => panic!("unknown store {other}"),
                };
                let job = store.dequeue(parts[2], now).await.expect("dequeue");
                print_dequeue(name, job);
            }
            "heartbeat" => {
                let ids: Vec<i32> = parts[1]
                    .split(',')
                    .map(|id| id.parse::<i32>().expect("heartbeat id"))
                    .collect();
                let hostname = if parts.get(2).copied() == Some("-") || parts.len() <= 2 {
                    None
                } else {
                    Some(parts[2])
                };
                let (known, canceled) = in_progress_store
                    .heartbeat(&ids, now, hostname)
                    .await
                    .expect("heartbeat");
                println!(
                    "ws heartbeat ids={} host={} known={} canceled={}",
                    parts[1],
                    parts
                        .get(2)
                        .copied()
                        .filter(|value| *value != "-")
                        .unwrap_or("-"),
                    join_i32s(&sorted_ids(known)),
                    join_i32s(&sorted_ids(canceled)),
                );
            }
            "requeue" => {
                let after = OffsetDateTime::parse(parts[2], &Rfc3339).expect("parse requeue time");
                in_progress_store
                    .requeue(parts[1].parse().expect("requeue id"), after)
                    .await
                    .expect("requeue");
                println!("ws requeue id={} after={}", parts[1], parts[2]);
            }
            "markcomplete" => {
                let ok = in_progress_store
                    .mark_complete(parts[1].parse().expect("markcomplete id"))
                    .await
                    .expect("mark_complete");
                println!("ws markcomplete id={} ok={}", parts[1], ok);
            }
            "markerrored" => {
                let ok = in_progress_store
                    .mark_errored(parts[1].parse().expect("markerrored id"), parts[2])
                    .await
                    .expect("mark_errored");
                println!("ws markerrored id={} ok={}", parts[1], ok);
            }
            "markfailed" => {
                let ok = in_progress_store
                    .mark_failed(parts[1].parse().expect("markfailed id"), parts[2])
                    .await
                    .expect("mark_failed");
                println!("ws markfailed id={} ok={}", parts[1], ok);
            }
            "reset" => {
                let store = match parts[1] {
                    "new" => &new_store,
                    "inprogress" => &in_progress_store,
                    other => panic!("unknown store {other}"),
                };
                let (reset, failed) = store.reset_stalled_at(now).await.expect("reset_stalled_at");
                let reset: Vec<i32> = reset.into_iter().map(|(id, _)| id).collect();
                let failed: Vec<i32> = failed.into_iter().map(|(id, _)| id).collect();
                println!(
                    "ws reset store={} reset={} failed={}",
                    parts[1],
                    join_i32s(&sorted_ids(reset)),
                    join_i32s(&sorted_ids(failed)),
                );
            }
            "row" => {
                print_row(&pool, parts[1].parse().expect("row id")).await;
            }
            "rowall" => {
                let rows = sqlx::query("SELECT id FROM insights_background_jobs ORDER BY id")
                    .fetch_all(&pool)
                    .await
                    .expect("list rows");
                for row in rows {
                    print_row(&pool, row.try_get::<i32, _>("id").expect("id")).await;
                }
            }
            other => panic!("unknown op {other}"),
        }
    }

    // Keep the constant referenced so the harness records the store options
    // it is exercising.
    assert_eq!(STALLED_MAX_AGE, Duration::from_secs(30));
}
