#![forbid(unsafe_code)]

//! M9.8al differential harness (Rust side): the `batches-scheduler` job and
//! the rollout window package.
//!
//! Mirrors `internal/batches/types/scheduler/window/harness_scheduler_test.go`
//! (`bw ` lines) and `cmd/worker/internal/batches/scheduler_harness_test.go`
//! (`bs ` lines). `test_ws/p9-go/run_batches_scheduler_diff.sh` applies
//! `batches_scheduler_fixtures.sql` immediately before each side and diffs the
//! two canonical outputs.

use std::sync::Arc;
use std::time::Duration;

use serde_json::Value;
use sg_config::{LiveConfig, Unified};
use sg_db::batches::BatchesStore;
use sg_db::OffsetDateTime;
use sg_service_worker::jobs::batches_scheduler::window::{go_quote, Configuration, Schedule};
use sg_service_worker::jobs::batches_scheduler::BatchesScheduler;
use sg_service_worker::jobs::{Job, JobDeps};
use sqlx::{PgPool, Row};
use time::{Date, Month, PrimitiveDateTime, Time};
use tokio_util::sync::CancellationToken;

fn utc(year: i32, month: Month, day: u8, hour: u8, minute: u8) -> OffsetDateTime {
    let date = Date::from_calendar_date(year, month, day).expect("date");
    let time = Time::from_hms(hour, minute, 0).expect("time");
    PrimitiveDateTime::new(date, time).assume_utc()
}

fn flag(value: bool) -> u8 {
    u8::from(value)
}

fn opt_minutes(value: Option<u16>) -> String {
    value.map_or_else(|| "-".to_owned(), |minutes| minutes.to_string())
}

fn opt_millis(value: Option<Duration>) -> String {
    value.map_or_else(
        || "-".to_owned(),
        |duration| duration.as_millis().to_string(),
    )
}

fn print_schedule(tag: &str, schedule: &Schedule) {
    println!(
        "bw sched name={tag} rate_n={} rate_unit_s={} duration_ms={} until={} total={}",
        schedule.rate().n(),
        schedule.rate().unit().as_duration().as_secs(),
        schedule.duration().as_millis(),
        schedule.valid_until().unix_timestamp(),
        schedule.total(),
    );
}

fn print_window_harness() {
    let main: Value = serde_json::from_str(
        r#"[
            {"days":["mon","tue","wed","thu","fri"],"start":"09:00","end":"17:30","rate":"100/h"},
            {"days":["sat","sun"],"rate":"unlimited"},
            {"start":"22:00","end":"23:00","rate":"10/m"}
        ]"#,
    )
    .expect("json");
    let cfg = Configuration::parse(Some(&main)).expect("configuration");

    println!(
        "bw config windows={} has={}",
        cfg.windows().len(),
        flag(cfg.has_rollout_windows())
    );
    for (index, window) in cfg.windows().iter().enumerate() {
        println!(
            "bw window i={index} days={} start={} end={} rate_n={} rate_unit_s={}",
            window.days_mask(),
            opt_minutes(window.start_minutes()),
            opt_minutes(window.end_minutes()),
            window.rate().n(),
            window.rate().unit().as_duration().as_secs(),
        );
    }

    let times = [
        ("t0", utc(2026, Month::November, 2, 8, 0)),
        ("t1", utc(2026, Month::November, 2, 9, 0)),
        ("t2", utc(2026, Month::November, 2, 17, 30)),
        ("t3", utc(2026, Month::November, 2, 23, 30)),
        ("t4", utc(2026, Month::November, 7, 12, 0)),
        ("t5", utc(2026, Month::November, 8, 22, 30)),
    ];

    for (name, at) in times {
        println!("bw at name={name} unix={}", at.unix_timestamp());
        for (index, window) in cfg.windows().iter().enumerate() {
            println!(
                "bw isopen w={index} name={name} open={} next={}",
                flag(window.is_open(at)),
                window.next_open_after(at).unix_timestamp(),
            );
        }

        let (active, validity) = cfg.window_for(at);
        let index = active
            .and_then(|active| {
                cfg.windows()
                    .iter()
                    .position(|window| std::ptr::eq(window, active))
            })
            .map_or(-1_i32, |index| index as i32);
        println!(
            "bw windowfor name={name} index={index} validity_ms={}",
            opt_millis(validity)
        );
        print_schedule(name, &cfg.schedule_at(at));
    }

    for n in [0_i64, 1, 5, 100, 850, 851, 10_000] {
        match cfg.estimate(times[0].1, n) {
            Some(estimate) => {
                println!("bw estimate n={n} value={}", estimate.unix_timestamp())
            }
            None => println!("bw estimate n={n} value=none"),
        }
    }

    let nil_cfg = Configuration::parse(None).expect("nil configuration");
    println!(
        "bw nil windows={} has={}",
        nil_cfg.windows().len(),
        flag(nil_cfg.has_rollout_windows())
    );

    let empty: Value = serde_json::from_str("[]").expect("json");
    let empty_cfg = Configuration::parse(Some(&empty)).expect("empty configuration");
    println!(
        "bw empty windows={} has={}",
        empty_cfg.windows().len(),
        flag(empty_cfg.has_rollout_windows())
    );
    print_schedule("t0", &empty_cfg.schedule_at(times[0].1));

    let bad: Value = serde_json::from_str(
        r#"[
            {"days":["xyz"],"start":"09:00","end":"10:00","rate":"60/h"},
            {"start":"9:60","rate":0},
            {"start":"09:00","rate":0},
            {"start":"10:00","end":"09:00","rate":0},
            {"rate":5},
            {"rate":"abc"},
            {"rate":"10/"},
            {},
            null
        ]"#,
    )
    .expect("json");
    match Configuration::parse(Some(&bad)) {
        Ok(_) => panic!("expected an error for the bad windows"),
        Err(err) => println!("bw bad err={}", go_quote(&err)),
    }

    let single: Value = serde_json::from_str(r#"[{"start":"09:00","rate":0}]"#).expect("json");
    match Configuration::parse(Some(&single)) {
        Ok(_) => panic!("expected an error for the single bad window"),
        Err(err) => println!("bw single err={}", go_quote(&err)),
    }

    let numeric_zero: Value = serde_json::from_str(r#"[{"rate":0}]"#).expect("json");
    match Configuration::parse(Some(&numeric_zero)) {
        Ok(_) => panic!("expected an error for a JSON numeric rate"),
        Err(err) => println!("bw numeric_zero err={}", go_quote(&err)),
    }
}

async fn print_job_harness(pool: &PgPool) {
    let job = BatchesScheduler::new();
    println!("bs job description={}", go_quote(&job.description()));
    println!("bs job configs={}", job.configs().len());

    let deps = JobDeps {
        frontend: pool.clone(),
        live_config: Arc::new(LiveConfig::new(Unified::default())),
        keyring: Arc::new(sg_keyring::Keyring::default()),
    };
    let routines = job
        .routines(&deps, CancellationToken::new())
        .await
        .expect("routines");
    println!("bs routine count={}", routines.len());
    for routine in &routines {
        println!("bs routine name={}", routine.name());
        println!("bs routine description={}", go_quote(routine.description()));
        println!("bs routine interval_ms={}", routine.interval().as_millis());
    }

    let store = BatchesStore::new(pool.clone());
    for index in 0..4 {
        match store
            .enqueue_next_scheduled_changeset()
            .await
            .expect("enqueue")
        {
            Some(id) => println!("bs enqueue i={index} id={id}"),
            None => println!("bs enqueue i={index} err=no_results"),
        }
    }

    let rows = sqlx::query(
        "SELECT id, reconciler_state FROM changesets \
         WHERE id BETWEEN 9801 AND 9805 ORDER BY id",
    )
    .fetch_all(pool)
    .await
    .expect("states");
    for row in rows {
        let id: i64 = row.try_get("id").expect("id");
        let state: String = row.try_get("reconciler_state").expect("state");
        println!("bs row id={id} state={state}");
    }

    std::env::set_var("DISABLE_BATCH_CHANGES", "1");
    let disabled = job
        .routines(&deps, CancellationToken::new())
        .await
        .expect("routines");
    println!("bs disabled routines={}", disabled.len());
    std::env::remove_var("DISABLE_BATCH_CHANGES");
}

#[tokio::test(flavor = "multi_thread")]
async fn batches_scheduler_matches_go_harness() {
    let Ok(dsn) = std::env::var("SG_QTEST_DSN") else {
        return;
    };

    print_window_harness();

    let pool = PgPool::connect(&dsn).await.expect("connect");
    print_job_harness(&pool).await;
}
