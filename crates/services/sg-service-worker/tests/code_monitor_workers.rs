#![forbid(unsafe_code)]

//! M9.8cm differential harness (Rust side): the code monitor
//! trigger/action dbworker stores (`cm_trigger_jobs` / `cm_action_jobs`), the
//! worker/resetter option identity, dequeue ordering, retry-after, heartbeat
//! cancel flag, mark complete/errored/failed and the `MaxNumResets = 0`
//! stalled-reset behaviour.
//!
//! Mirrors `internal/codemonitors/background/cm_workers_harness_test.go`;
//! outputs are compared line for line by
//! `test_ws/p9-go/run_code_monitor_workers_diff.sh`. Both sides seed identical
//! fixtures into a scratch copy of the frontend schema and print canonical
//! `cm ` lines. Timestamps are never printed; only relative intervals well
//! inside/outside the 10s retry window and the 60s stalled age are used.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use sg_config::SiteConfiguration;
use sg_db::code_monitors::{ActionJob, TriggerJob};
use sg_service_worker::framework::dbworker::DbWorkerHandler;
use sg_service_worker::framework::recorder::Recordable;
use sg_service_worker::jobs::code_monitor_workers::{
    self, ActionJobStore, TriggerJobStore, ACTION_WORKER_DESCRIPTION, ACTION_WORKER_NAME,
    TRIGGER_WORKER_DESCRIPTION, TRIGGER_WORKER_NAME,
};
use sqlx::postgres::PgPoolOptions;
use sqlx::types::time::OffsetDateTime;
use sqlx::{PgPool, Row};
use time::Duration as TimeDuration;
use tokio_util::sync::CancellationToken;

const HOSTNAME: &str = "harness-host";

fn print_line(line: impl Into<String>) {
    println!("cm {}", line.into());
}

fn opt_str(value: &Option<String>) -> &str {
    value.as_deref().unwrap_or("<none>")
}

fn opt_time(value: &Option<OffsetDateTime>) -> &'static str {
    if value.is_some() {
        "set"
    } else {
        "<none>"
    }
}

fn opt_i64(value: Option<i64>) -> String {
    value.map_or_else(|| "<none>".to_owned(), |value| value.to_string())
}

fn join_ints<T: ToString>(values: &[T]) -> String {
    values
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join(",")
}

fn join_ids(values: Vec<i32>) -> String {
    let mut values = values;
    values.sort_unstable();
    values
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join("|")
}

/// Go's `time.Duration.String()` for the whole-minute/whole-second values
/// printed here.
fn go_duration(value: Duration) -> String {
    let seconds = value.as_secs();
    if seconds >= 60 && seconds % 60 == 0 {
        format!("{}m0s", seconds / 60)
    } else {
        format!("{seconds}s")
    }
}

async fn connect() -> Option<PgPool> {
    let dsn = std::env::var("SG_CM_WORKERS_TEST_DSN").ok()?;
    Some(
        PgPoolOptions::new()
            .max_connections(1)
            .connect(&dsn)
            .await
            .expect("connect to SG_CM_WORKERS_TEST_DSN"),
    )
}

async fn exec(pool: &PgPool, query: &str) {
    sqlx::query(query).execute(pool).await.expect(query);
}

async fn truncate(pool: &PgPool) {
    exec(
        pool,
        "TRUNCATE TABLE cm_action_jobs, cm_trigger_jobs, cm_queries, cm_webhooks, \
         cm_monitors, users RESTART IDENTITY CASCADE",
    )
    .await;
}

async fn seed_base(pool: &PgPool) {
    exec(pool, "INSERT INTO users (id, username) VALUES (1, 'alice')").await;
    exec(
        pool,
        "INSERT INTO cm_monitors (id, created_by, changed_by, description, enabled, namespace_user_id) \
         VALUES (1, 1, 1, 'mon', TRUE, 1)",
    )
    .await;
    exec(
        pool,
        "INSERT INTO cm_queries (id, monitor, query, created_by, changed_by) \
         VALUES (1, 1, 'type:commit', 1, 1)",
    )
    .await;
    exec(
        pool,
        "INSERT INTO cm_webhooks (id, monitor, url, enabled, created_by, changed_by) \
         VALUES (1, 1, 'http://example.com/hook', TRUE, 1, 1)",
    )
    .await;
}

struct TriggerSeed<'a> {
    id: i32,
    state: &'a str,
    query_string: Option<&'a str>,
    search_results: Option<&'a str>,
    num_resets: i32,
    num_failures: i32,
    cancel: bool,
    process_after: Option<OffsetDateTime>,
    last_heartbeat_at: Option<OffsetDateTime>,
}

async fn insert_trigger(pool: &PgPool, seed: TriggerSeed<'_>) {
    sqlx::query(
        "INSERT INTO cm_trigger_jobs \
         (id, query, state, query_string, search_results, num_resets, num_failures, cancel, \
          process_after, last_heartbeat_at) \
         VALUES ($1, 1, $2, $3, $4::jsonb, $5, $6, $7, $8, $9)",
    )
    .bind(seed.id)
    .bind(seed.state)
    .bind(seed.query_string)
    .bind(seed.search_results)
    .bind(seed.num_resets)
    .bind(seed.num_failures)
    .bind(seed.cancel)
    .bind(seed.process_after)
    .bind(seed.last_heartbeat_at)
    .execute(pool)
    .await
    .expect("insert cm_trigger_jobs");
}

struct TriggerRow {
    state: String,
    failures: i32,
    resets: i32,
    message: Option<String>,
    worker: String,
}

async fn read_trigger(pool: &PgPool, id: i32) -> TriggerRow {
    let row = sqlx::query(
        "SELECT state, num_failures, num_resets, failure_message, worker_hostname \
         FROM cm_trigger_jobs WHERE id = $1",
    )
    .bind(id)
    .fetch_one(pool)
    .await
    .expect("read cm_trigger_jobs");
    TriggerRow {
        state: row.get("state"),
        failures: row.get("num_failures"),
        resets: row.get("num_resets"),
        message: row.get("failure_message"),
        worker: row.get("worker_hostname"),
    }
}

async fn print_trigger_row(pool: &PgPool, id: i32) {
    let row = read_trigger(pool, id).await;
    print_line(format!(
        "trigger-row id={id} state={} failures={} resets={} message={} worker={}",
        row.state,
        row.failures,
        row.resets,
        opt_str(&row.message),
        row.worker
    ));
}

async fn print_trigger_dequeue(pool: &PgPool, record: &Option<TriggerJob>) {
    let Some(record) = record else {
        print_line("dequeue trigger empty");
        return;
    };
    let results = match &record.search_results {
        Some(serde_json::Value::Array(entries)) if !entries.is_empty() => "present",
        _ => "<none>",
    };
    let worker: String = sqlx::query("SELECT worker_hostname FROM cm_trigger_jobs WHERE id = $1")
        .bind(record.id)
        .fetch_one(pool)
        .await
        .expect("worker hostname")
        .get("worker_hostname");
    print_line(format!(
        "dequeue trigger id={} state={} query_string={} results={} started={} finished={} failure={} process_after={} resets={} failures={} worker={}",
        record.id,
        opt_str(&record.state),
        opt_str(&record.query_string),
        results,
        opt_time(&record.started_at),
        opt_time(&record.finished_at),
        opt_str(&record.failure_message),
        opt_time(&record.process_after),
        record.num_resets,
        record.num_failures,
        worker
    ));
}

struct ActionSeed<'a> {
    id: i32,
    state: &'a str,
    trigger_event: i32,
    num_resets: i32,
    num_failures: i32,
    cancel: bool,
    process_after: Option<OffsetDateTime>,
    last_heartbeat_at: Option<OffsetDateTime>,
}

async fn insert_action(pool: &PgPool, seed: ActionSeed<'_>) {
    sqlx::query(
        "INSERT INTO cm_action_jobs \
         (id, webhook, trigger_event, state, num_resets, num_failures, cancel, \
          process_after, last_heartbeat_at) \
         VALUES ($1, 1, $2, $3, $4, $5, $6, $7, $8)",
    )
    .bind(seed.id)
    .bind(seed.trigger_event)
    .bind(seed.state)
    .bind(seed.num_resets)
    .bind(seed.num_failures)
    .bind(seed.cancel)
    .bind(seed.process_after)
    .bind(seed.last_heartbeat_at)
    .execute(pool)
    .await
    .expect("insert cm_action_jobs");
}

struct ActionRow {
    state: String,
    failures: i32,
    resets: i32,
    message: Option<String>,
    worker: String,
}

async fn read_action(pool: &PgPool, id: i32) -> ActionRow {
    let row = sqlx::query(
        "SELECT state, num_failures, num_resets, failure_message, worker_hostname \
         FROM cm_action_jobs WHERE id = $1",
    )
    .bind(id)
    .fetch_one(pool)
    .await
    .expect("read cm_action_jobs");
    ActionRow {
        state: row.get("state"),
        failures: row.get("num_failures"),
        resets: row.get("num_resets"),
        message: row.get("failure_message"),
        worker: row.get("worker_hostname"),
    }
}

async fn print_action_row(pool: &PgPool, id: i32) {
    let row = read_action(pool, id).await;
    print_line(format!(
        "action-row id={id} state={} failures={} resets={} message={} worker={}",
        row.state,
        row.failures,
        row.resets,
        opt_str(&row.message),
        row.worker
    ));
}

async fn print_action_dequeue(pool: &PgPool, record: &Option<ActionJob>) {
    let Some(record) = record else {
        print_line("dequeue action empty");
        return;
    };
    let worker: String = sqlx::query("SELECT worker_hostname FROM cm_action_jobs WHERE id = $1")
        .bind(record.id)
        .fetch_one(pool)
        .await
        .expect("worker hostname")
        .get("worker_hostname");
    print_line(format!(
        "dequeue action id={} state={} email={} webhook={} slack={} trigger_event={} started={} finished={} failure={} process_after={} resets={} failures={} worker={}",
        record.id,
        opt_str(&record.state),
        opt_i64(record.email),
        opt_i64(record.webhook),
        opt_i64(record.slack_webhook),
        record.trigger_event,
        opt_time(&record.started_at),
        opt_time(&record.finished_at),
        opt_str(&record.failure_message),
        opt_time(&record.process_after),
        record.num_resets,
        record.num_failures,
        worker
    ));
}

struct NoopTriggerHandler;

#[async_trait]
impl DbWorkerHandler<TriggerJobStore> for NoopTriggerHandler {
    async fn handle(&self, _ctx: CancellationToken, _record: &TriggerJob) -> Result<(), String> {
        Ok(())
    }
}

struct NoopActionHandler;

#[async_trait]
impl DbWorkerHandler<ActionJobStore> for NoopActionHandler {
    async fn handle(&self, _ctx: CancellationToken, _record: &ActionJob) -> Result<(), String> {
        Ok(())
    }
}

#[tokio::test]
async fn code_monitor_workers_harness() {
    let Some(pool) = connect().await else {
        return;
    };

    // --- option identity through the real constructors -------------------
    let trigger_store = Arc::new(TriggerJobStore::new(pool.clone()));
    let action_store = Arc::new(ActionJobStore::new(pool.clone()));

    let trigger_worker = code_monitor_workers::new_trigger_query_runner(
        Arc::clone(&trigger_store),
        Arc::new(NoopTriggerHandler),
        code_monitor_workers::DEFAULT_CONCURRENCY,
        HOSTNAME.to_owned(),
    );
    let action_worker = code_monitor_workers::new_action_runner(
        Arc::clone(&action_store),
        Arc::new(NoopActionHandler),
        HOSTNAME.to_owned(),
    );
    print_line(format!(
        "worker trigger name={} description={} interval={}",
        trigger_worker.options().name,
        trigger_worker.options().description,
        go_duration(trigger_worker.options().interval)
    ));
    print_line(format!(
        "worker action name={} description={} interval={}",
        action_worker.options().name,
        action_worker.options().description,
        go_duration(action_worker.options().interval)
    ));
    assert_eq!(trigger_worker.options().name, TRIGGER_WORKER_NAME);
    assert_eq!(
        trigger_worker.options().description,
        TRIGGER_WORKER_DESCRIPTION
    );
    assert_eq!(action_worker.options().name, ACTION_WORKER_NAME);
    assert_eq!(
        action_worker.options().description,
        ACTION_WORKER_DESCRIPTION
    );

    let trigger_resetter =
        code_monitor_workers::new_trigger_query_resetter(Arc::clone(&trigger_store));
    let action_resetter = code_monitor_workers::new_action_job_resetter(Arc::clone(&action_store));
    print_line(format!(
        "resetter trigger name=dbworker.Resetter[{}] action name=dbworker.Resetter[{}]",
        Recordable::name(&trigger_resetter),
        Recordable::name(&action_resetter)
    ));

    let default_site = SiteConfiguration::default();
    print_line(format!(
        "conf concurrency default={}",
        code_monitor_workers::code_monitors_concurrency(&default_site)
    ));
    let configured_site =
        SiteConfiguration::parse(r#"{"codeMonitors": {"concurrency": 7}}"#).unwrap();
    print_line(format!(
        "conf concurrency configured={}",
        code_monitor_workers::code_monitors_concurrency(&configured_site)
    ));

    // --- trigger store ---------------------------------------------------
    truncate(&pool).await;
    seed_base(&pool).await;
    let store = TriggerJobStore::new(pool.clone());

    let future = OffsetDateTime::now_utc() + TimeDuration::hours(1);
    insert_trigger(
        &pool,
        TriggerSeed {
            id: 501,
            state: "queued",
            query_string: Some("type:commit after:2020"),
            search_results: Some("[{}]"),
            num_resets: 0,
            num_failures: 0,
            cancel: false,
            process_after: None,
            last_heartbeat_at: None,
        },
    )
    .await;
    insert_trigger(
        &pool,
        TriggerSeed {
            id: 502,
            state: "queued",
            query_string: None,
            search_results: None,
            num_resets: 0,
            num_failures: 0,
            cancel: false,
            process_after: Some(future),
            last_heartbeat_at: None,
        },
    )
    .await;
    insert_trigger(
        &pool,
        TriggerSeed {
            id: 503,
            state: "queued",
            query_string: None,
            search_results: None,
            num_resets: 0,
            num_failures: 0,
            cancel: false,
            process_after: None,
            last_heartbeat_at: None,
        },
    )
    .await;

    for _ in 0..2 {
        let record = store
            .dequeue(HOSTNAME, OffsetDateTime::now_utc())
            .await
            .expect("dequeue");
        print_trigger_dequeue(&pool, &record).await;
    }
    let record = store
        .dequeue(HOSTNAME, OffsetDateTime::now_utc())
        .await
        .expect("dequeue");
    print_trigger_dequeue(&pool, &record).await;

    let now = OffsetDateTime::now_utc();
    let (known, canceled) = store.heartbeat(&[501, 503], now).await.expect("heartbeat");
    print_line(format!(
        "heartbeat trigger known={} canceled={}",
        join_ids(known),
        join_ids(canceled)
    ));

    exec(
        &pool,
        "UPDATE cm_trigger_jobs SET cancel = TRUE WHERE id = 503",
    )
    .await;
    let (known, canceled) = store
        .heartbeat(&[501, 503], OffsetDateTime::now_utc())
        .await
        .expect("heartbeat");
    print_line(format!(
        "heartbeat trigger-cancel known={} canceled={}",
        join_ids(known),
        join_ids(canceled)
    ));

    store.mark_complete(501).await.expect("mark complete");
    print_trigger_row(&pool, 501).await;

    store.mark_errored(503, "boom").await.expect("mark errored");
    print_trigger_row(&pool, 503).await;

    insert_trigger(
        &pool,
        TriggerSeed {
            id: 504,
            state: "queued",
            query_string: None,
            search_results: None,
            num_resets: 0,
            num_failures: 0,
            cancel: false,
            process_after: None,
            last_heartbeat_at: None,
        },
    )
    .await;
    let record = store
        .dequeue(HOSTNAME, OffsetDateTime::now_utc())
        .await
        .expect("dequeue");
    assert!(record.is_some());
    store.mark_errored(504, "boom").await.expect("mark errored");
    print_trigger_row(&pool, 504).await;

    // Errored records are retried once finished_at is older than RetryAfter.
    exec(
        &pool,
        "UPDATE cm_trigger_jobs SET finished_at = now() - interval '11 seconds' WHERE id = 504",
    )
    .await;
    let record = store
        .dequeue(HOSTNAME, OffsetDateTime::now_utc())
        .await
        .expect("dequeue");
    print_trigger_dequeue(&pool, &record).await;

    // `num_failures + 1 >= 3` moves the record to failed.
    exec(
        &pool,
        "UPDATE cm_trigger_jobs SET num_failures = 2 WHERE id = 504",
    )
    .await;
    store.mark_errored(504, "boom").await.expect("mark errored");
    print_trigger_row(&pool, 504).await;

    insert_trigger(
        &pool,
        TriggerSeed {
            id: 505,
            state: "queued",
            query_string: None,
            search_results: None,
            num_resets: 0,
            num_failures: 0,
            cancel: false,
            process_after: None,
            last_heartbeat_at: None,
        },
    )
    .await;
    let record = store
        .dequeue(HOSTNAME, OffsetDateTime::now_utc())
        .await
        .expect("dequeue");
    assert!(record.is_some());
    store.mark_failed(505, "boom").await.expect("mark failed");
    print_trigger_row(&pool, 505).await;

    // MaxNumResets is left at zero: stalled records fail instead of requeueing.
    let stalled = OffsetDateTime::now_utc() - TimeDuration::minutes(2);
    let fresh = OffsetDateTime::now_utc() - TimeDuration::seconds(5);
    for (id, last_heartbeat_at, num_resets) in [
        (506, Some(stalled), 0),
        (507, Some(fresh), 0),
        (508, Some(stalled), 5),
    ] {
        insert_trigger(
            &pool,
            TriggerSeed {
                id,
                state: "processing",
                query_string: None,
                search_results: None,
                num_resets,
                num_failures: 0,
                cancel: false,
                process_after: None,
                last_heartbeat_at,
            },
        )
        .await;
    }

    let (queued, failed) = store
        .reset_stalled_at(OffsetDateTime::now_utc())
        .await
        .expect("reset stalled");
    let mut queued_ids: Vec<i64> = queued.into_iter().map(|(id, _)| i64::from(id)).collect();
    let mut failed_ids: Vec<i64> = failed.into_iter().map(|(id, _)| i64::from(id)).collect();
    queued_ids.sort_unstable();
    failed_ids.sort_unstable();
    print_line(format!(
        "reset trigger queued={} failed={}",
        join_ints(&queued_ids),
        join_ints(&failed_ids)
    ));
    for id in [506, 507, 508] {
        print_trigger_row(&pool, id).await;
    }

    let record = store
        .dequeue(HOSTNAME, OffsetDateTime::now_utc())
        .await
        .expect("dequeue");
    print_trigger_dequeue(&pool, &record).await;

    // --- action store ----------------------------------------------------
    truncate(&pool).await;
    seed_base(&pool).await;
    insert_trigger(
        &pool,
        TriggerSeed {
            id: 600,
            state: "queued",
            query_string: None,
            search_results: None,
            num_resets: 0,
            num_failures: 0,
            cancel: false,
            process_after: None,
            last_heartbeat_at: None,
        },
    )
    .await;
    let action_store = ActionJobStore::new(pool.clone());

    let action_future = OffsetDateTime::now_utc() + TimeDuration::hours(1);
    insert_action(
        &pool,
        ActionSeed {
            id: 601,
            state: "queued",
            trigger_event: 600,
            num_resets: 0,
            num_failures: 0,
            cancel: false,
            process_after: None,
            last_heartbeat_at: None,
        },
    )
    .await;
    insert_action(
        &pool,
        ActionSeed {
            id: 602,
            state: "queued",
            trigger_event: 600,
            num_resets: 0,
            num_failures: 0,
            cancel: false,
            process_after: Some(action_future),
            last_heartbeat_at: None,
        },
    )
    .await;

    for _ in 0..2 {
        let record = action_store
            .dequeue(HOSTNAME, OffsetDateTime::now_utc())
            .await
            .expect("action dequeue");
        print_action_dequeue(&pool, &record).await;
    }

    let (known, canceled) = action_store
        .heartbeat(&[601], OffsetDateTime::now_utc())
        .await
        .expect("action heartbeat");
    print_line(format!(
        "heartbeat action known={} canceled={}",
        join_ids(known),
        join_ids(canceled)
    ));

    action_store
        .mark_complete(601)
        .await
        .expect("mark complete");
    print_action_row(&pool, 601).await;

    insert_action(
        &pool,
        ActionSeed {
            id: 603,
            state: "queued",
            trigger_event: 600,
            num_resets: 0,
            num_failures: 0,
            cancel: false,
            process_after: None,
            last_heartbeat_at: None,
        },
    )
    .await;
    let record = action_store
        .dequeue(HOSTNAME, OffsetDateTime::now_utc())
        .await
        .expect("action dequeue");
    assert!(record.is_some());
    action_store
        .mark_errored(603, "boom")
        .await
        .expect("mark errored");
    print_action_row(&pool, 603).await;

    exec(
        &pool,
        "UPDATE cm_action_jobs SET finished_at = now() - interval '11 seconds' WHERE id = 603",
    )
    .await;
    let record = action_store
        .dequeue(HOSTNAME, OffsetDateTime::now_utc())
        .await
        .expect("action dequeue");
    print_action_dequeue(&pool, &record).await;

    exec(
        &pool,
        "UPDATE cm_action_jobs SET num_failures = 2 WHERE id = 603",
    )
    .await;
    action_store
        .mark_errored(603, "boom")
        .await
        .expect("mark errored");
    print_action_row(&pool, 603).await;

    insert_action(
        &pool,
        ActionSeed {
            id: 604,
            state: "queued",
            trigger_event: 600,
            num_resets: 0,
            num_failures: 0,
            cancel: true,
            process_after: None,
            last_heartbeat_at: None,
        },
    )
    .await;
    let record = action_store
        .dequeue(HOSTNAME, OffsetDateTime::now_utc())
        .await
        .expect("action dequeue");
    assert!(record.is_some());
    action_store
        .mark_errored(604, "boom")
        .await
        .expect("mark errored");
    print_action_row(&pool, 604).await;

    insert_action(
        &pool,
        ActionSeed {
            id: 605,
            state: "queued",
            trigger_event: 600,
            num_resets: 0,
            num_failures: 0,
            cancel: false,
            process_after: None,
            last_heartbeat_at: None,
        },
    )
    .await;
    let record = action_store
        .dequeue(HOSTNAME, OffsetDateTime::now_utc())
        .await
        .expect("action dequeue");
    assert!(record.is_some());
    action_store
        .mark_failed(605, "boom")
        .await
        .expect("mark failed");
    print_action_row(&pool, 605).await;

    let action_stalled = OffsetDateTime::now_utc() - TimeDuration::minutes(2);
    let action_fresh = OffsetDateTime::now_utc() - TimeDuration::seconds(5);
    for (id, last_heartbeat_at, num_resets) in [
        (606, Some(action_stalled), 0),
        (607, Some(action_fresh), 0),
        (608, Some(action_stalled), 5),
    ] {
        insert_action(
            &pool,
            ActionSeed {
                id,
                state: "processing",
                trigger_event: 600,
                num_resets,
                num_failures: 0,
                cancel: false,
                process_after: None,
                last_heartbeat_at,
            },
        )
        .await;
    }

    let (queued, failed) = action_store
        .reset_stalled_at(OffsetDateTime::now_utc())
        .await
        .expect("action reset stalled");
    let mut queued_ids: Vec<i64> = queued.into_iter().map(|(id, _)| i64::from(id)).collect();
    let mut failed_ids: Vec<i64> = failed.into_iter().map(|(id, _)| i64::from(id)).collect();
    queued_ids.sort_unstable();
    failed_ids.sort_unstable();
    print_line(format!(
        "reset action queued={} failed={}",
        join_ints(&queued_ids),
        join_ints(&failed_ids)
    ));
    for id in [606, 607, 608] {
        print_action_row(&pool, id).await;
    }

    let record = action_store
        .dequeue(HOSTNAME, OffsetDateTime::now_utc())
        .await
        .expect("action dequeue");
    print_action_dequeue(&pool, &record).await;
}
