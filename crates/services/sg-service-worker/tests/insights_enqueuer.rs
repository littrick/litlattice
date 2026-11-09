#![forbid(unsafe_code)]

//! Differential harness for the insight enqueuer and retention enqueuer
//! (M9.8cc).
//!
//! Mirror of the Go harness
//! `internal/insights/background/parity_enqueuer_harness_test.go`: same
//! fixtures, same corpus, same canonical `ie ` lines. Both sides reset the
//! scratch frontend/codeinsights databases before every corpus operation and
//! then read the `insights_query_runner_jobs`, `insight_series` and
//! `insights_data_retention_jobs` rows back.
//!
//! Gated by `SG_INSIGHTS_ENQUEUER_CORPUS`; connections come from
//! `SG_QTEST_FRONTEND_DSN` and `SG_QTEST_INSIGHTS_DSN`.

use std::collections::BTreeSet;
use std::time::Duration;

use async_trait::async_trait;
use sqlx::postgres::{PgPool, PgPoolOptions};
use sqlx::types::time::{OffsetDateTime, PrimitiveDateTime};
use sqlx::Row;

use sg_service_worker::jobs::insights_enqueuer::{
    InsightEnqueuer, JobEnqueuer, RetentionEnqueuer, Stamp, COST_INDEXED, ENQUEUER_DESCRIPTION,
    ENQUEUER_INTERVAL, ENQUEUER_NAME, PRIORITY_HIGH, RETENTION_ENQUEUER_DESCRIPTION,
    RETENTION_ENQUEUER_INTERVAL, RETENTION_ENQUEUER_NAME,
};
use sg_service_worker::jobs::insights_query_runner::insights_store::Series;
use sg_service_worker::jobs::insights_query_runner::store::{enqueue_job, NewJob};

struct FakeEnqueuer {
    pool: PgPool,
    failures: BTreeSet<String>,
}

#[async_trait]
impl JobEnqueuer for FakeEnqueuer {
    async fn enqueue(&self, job: &NewJob) -> Result<i32, String> {
        if self.failures.contains(&job.series_id) {
            return Err("boom".to_owned());
        }
        enqueue_job(&self.pool, job).await
    }
}

fn format_time(value: PrimitiveDateTime) -> String {
    format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}Z",
        value.year(),
        u8::from(value.month()),
        value.day(),
        value.hour(),
        value.minute(),
        value.second()
    )
}

fn opt_time(value: Option<PrimitiveDateTime>) -> String {
    value.map_or_else(|| "-".to_owned(), format_time)
}

fn opt_offset(value: Option<OffsetDateTime>) -> String {
    value.map_or_else(
        || "-".to_owned(),
        |value| format_time(PrimitiveDateTime::new(value.date(), value.time())),
    )
}

async fn reset(pool: &PgPool, fixture: &str) {
    sqlx::raw_sql(fixture)
        .execute(pool)
        .await
        .expect("reset fixture");
}

async fn print_state(frontend: &PgPool, insights: &PgPool, label: &str) {
    let jobs = sqlx::query(
        "SELECT series_id, search_query, state, priority, cost, persist_mode::text AS persist_mode, record_time, process_after \
         FROM insights_query_runner_jobs ORDER BY id",
    )
    .fetch_all(frontend)
    .await
    .expect("read jobs");
    for job in &jobs {
        let series_id: String = job.try_get("series_id").expect("series_id");
        let query: String = job.try_get("search_query").expect("search_query");
        let state: String = job.try_get("state").expect("state");
        let priority: i32 = job.try_get("priority").expect("priority");
        let cost: i32 = job.try_get("cost").expect("cost");
        let persist: String = job.try_get("persist_mode").expect("persist_mode");
        let record_time: Option<PrimitiveDateTime> =
            job.try_get("record_time").expect("record_time");
        let process_after: Option<OffsetDateTime> =
            job.try_get("process_after").expect("process_after");
        println!(
            "ie {label} job series={series_id} persist={persist} state={state} priority={priority} cost={cost} query={query:?} record={} process_after={}",
            opt_time(record_time),
            opt_offset(process_after)
        );
    }
    println!("ie {label} jobs count={}", jobs.len());

    let stamps = sqlx::query(
        "SELECT id, last_recorded_at, next_recording_after, last_snapshot_at, next_snapshot_after \
         FROM insight_series ORDER BY id",
    )
    .fetch_all(insights)
    .await
    .expect("read stamps");
    for stamp in &stamps {
        let id: i32 = stamp.try_get("id").expect("id");
        let last_recorded: Option<PrimitiveDateTime> =
            stamp.try_get("last_recorded_at").expect("last_recorded_at");
        let next_recording: Option<PrimitiveDateTime> = stamp
            .try_get("next_recording_after")
            .expect("next_recording_after");
        let last_snapshot: Option<PrimitiveDateTime> =
            stamp.try_get("last_snapshot_at").expect("last_snapshot_at");
        let next_snapshot: Option<PrimitiveDateTime> = stamp
            .try_get("next_snapshot_after")
            .expect("next_snapshot_after");
        println!(
            "ie {label} stamp id={id} last_recorded={} next_recording={} last_snapshot={} next_snapshot={}",
            opt_time(last_recorded),
            opt_time(next_recording),
            opt_time(last_snapshot),
            opt_time(next_snapshot)
        );
    }
}

#[tokio::test]
async fn insights_enqueuer_differential() {
    let corpus_path = match std::env::var("SG_INSIGHTS_ENQUEUER_CORPUS") {
        Ok(path) if !path.is_empty() => path,
        _ => return,
    };
    let frontend_dsn = std::env::var("SG_QTEST_FRONTEND_DSN").expect("SG_QTEST_FRONTEND_DSN");
    let insights_dsn = std::env::var("SG_QTEST_INSIGHTS_DSN").expect("SG_QTEST_INSIGHTS_DSN");
    let insights_fixture_path =
        std::env::var("SG_INSIGHTS_ENQUEUER_INSIGHTS_FIXTURE").expect("insights fixture");
    let frontend_fixture_path =
        std::env::var("SG_INSIGHTS_ENQUEUER_FRONTEND_FIXTURE").expect("frontend fixture");

    let frontend = PgPoolOptions::new()
        .max_connections(4)
        .connect(&frontend_dsn)
        .await
        .expect("connect frontend");
    let insights = PgPoolOptions::new()
        .max_connections(4)
        .connect(&insights_dsn)
        .await
        .expect("connect codeinsights");

    let insights_fixture =
        std::fs::read_to_string(insights_fixture_path).expect("insights fixture");
    let frontend_fixture =
        std::fs::read_to_string(frontend_fixture_path).expect("frontend fixture");

    println!(
        "ie routine enqueuer name={:?} description={:?} interval={}",
        ENQUEUER_NAME,
        ENQUEUER_DESCRIPTION,
        ENQUEUER_INTERVAL.as_secs()
    );
    println!(
        "ie routine retention name={:?} description={:?} interval={}",
        RETENTION_ENQUEUER_NAME,
        RETENTION_ENQUEUER_DESCRIPTION,
        RETENTION_ENQUEUER_INTERVAL.as_secs()
    );

    let corpus = std::fs::read_to_string(&corpus_path).expect("read corpus");
    for line in corpus.lines() {
        if line.is_empty() {
            continue;
        }
        let parts: Vec<&str> = line.split('\t').collect();
        let op = parts[0];
        let label = parts[1];
        reset(&frontend, &frontend_fixture).await;
        reset(&insights, &insights_fixture).await;

        match op {
            "discover" => {
                let now =
                    OffsetDateTime::parse(parts[2], &time::format_description::well_known::Rfc3339)
                        .expect("corpus timestamp");
                let failures: BTreeSet<String> = if parts[3] == "-" {
                    BTreeSet::new()
                } else {
                    parts[3].split(',').map(str::to_owned).collect()
                };
                let enqueuer = InsightEnqueuer::new(
                    move || now,
                    move || now,
                    FakeEnqueuer {
                        pool: frontend.clone(),
                        failures,
                    },
                );
                match enqueuer.discover_and_enqueue_insights(&insights).await {
                    Ok(()) => println!("ie {label} result=ok"),
                    Err(err) => println!("ie {label} result=error error={err:?}"),
                }
                print_state(&frontend, &insights, label).await;
            }

            "dedupe" => {
                let now =
                    OffsetDateTime::parse(parts[2], &time::format_description::well_known::Rfc3339)
                        .expect("corpus timestamp");
                let enqueuer = InsightEnqueuer::new(
                    move || now,
                    move || now,
                    FakeEnqueuer {
                        pool: frontend.clone(),
                        failures: BTreeSet::new(),
                    },
                );
                let base = Series {
                    id: 900,
                    series_id: "enq-global".to_owned(),
                    query: "simple search".to_owned(),
                    repositories: Vec::new(),
                    sample_interval_unit: "MONTH".to_owned(),
                    sample_interval_value: 1,
                    group_by: None,
                    repository_criteria: None,
                    just_in_time: false,
                    generation_method: "search".to_owned(),
                };
                let mut duplicate = base.clone();
                duplicate.id = 901;
                duplicate.query = "ignored second query".to_owned();
                let errors = enqueuer
                    .enqueue(&insights, &[base, duplicate], Stamp::Recording)
                    .await;
                match sg_service_worker::jobs::insights_enqueuer::render_errors(&errors) {
                    Some(err) => println!("ie {label} result=error error={err:?}"),
                    None => println!("ie {label} result=ok"),
                }
                print_state(&frontend, &insights, label).await;
            }

            "retention" => {
                let retention = RetentionEnqueuer::new(insights.clone());
                match retention.enqueue_all().await {
                    Ok(()) => {}
                    Err(err) => println!("ie {label} retention_error={err:?}"),
                }
                let rows = sqlx::query(
                    "SELECT series_id, series_id_string FROM insights_data_retention_jobs ORDER BY id",
                )
                .fetch_all(&insights)
                .await
                .expect("read retention jobs");
                for row in &rows {
                    let series_id: i32 = row.try_get("series_id").expect("series_id");
                    let series_string: String =
                        row.try_get("series_id_string").expect("series_id_string");
                    println!("ie {label} rjob series={series_id} series_string={series_string}");
                }
                println!("ie {label} result=ok rjobs count={}", rows.len());
            }

            other => panic!("unknown op {other:?}"),
        }
    }

    assert_eq!(PRIORITY_HIGH, 10);
    assert_eq!(COST_INDEXED, 500);
    assert_eq!(ENQUEUER_INTERVAL, Duration::from_secs(3600));
}
