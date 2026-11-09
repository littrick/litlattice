#![forbid(unsafe_code)]

//! Differential harness for the in-progress backfill handler (M9.8cb+3).
//!
//! Mirror of the Go harness
//! `internal/insights/scheduler/parity_inprogress_handler_harness_test.go`:
//! same fixture scenes, same canonical `ip ` lines. Drives
//! [`InProgressHandler::handle`] with a fake repository store, a scripted
//! backfiller and a mock clock, so both sides run the real iterator, backfill
//! and incomplete-point writes against the scratch `codeinsights` database.
//!
//! Gated by `SG_INSIGHTS_BACKFILL_INPROGRESS_CORPUS`; the connection comes
//! from `SG_QTEST_INSIGHTS_DSN`.

use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use sqlx::postgres::{PgPool, PgPoolOptions};
use sqlx::types::time::{OffsetDateTime, PrimitiveDateTime};
use sqlx::Row;
use time::format_description::well_known::Rfc3339;
use time::UtcOffset;

use sg_service_worker::jobs::insights_backfill_inprogress::{
    BackfillError, BackfillRequest, Backfiller, HandlerClock, HandlerConfig, InProgressHandler,
    InsightRepo, InsightRepoStore, RepoGetError,
};

/// 2021-01-01T03:00:00Z.
const BASE_MILLIS: i64 = 1_609_470_000_000;

struct FakeClock {
    millis: AtomicI64,
}

impl FakeClock {
    fn new() -> Self {
        Self {
            millis: AtomicI64::new(BASE_MILLIS),
        }
    }

    fn advance_seconds(&self, seconds: i64) {
        self.millis.fetch_add(seconds * 1_000, Ordering::SeqCst);
    }
}

impl HandlerClock for FakeClock {
    fn now(&self) -> OffsetDateTime {
        OffsetDateTime::from_unix_timestamp_nanos(
            i128::from(self.millis.load(Ordering::SeqCst)) * 1_000_000,
        )
        .expect("fake clock timestamp")
    }
}

struct FakeRepoStore {
    names: HashMap<i32, String>,
}

#[async_trait]
impl InsightRepoStore for FakeRepoStore {
    async fn get(&self, id: i32) -> Result<InsightRepo, RepoGetError> {
        if id == 46 {
            return Err(RepoGetError::Other("db down".to_owned()));
        }
        match self.names.get(&id) {
            Some(name) => Ok(InsightRepo {
                id,
                name: name.clone(),
            }),
            None => Err(RepoGetError::NotFound),
        }
    }
}

struct FakeBackfiller {
    clock: Arc<FakeClock>,
    runs: Mutex<BTreeMap<String, i32>>,
}

impl FakeBackfiller {
    fn new(clock: Arc<FakeClock>) -> Self {
        Self {
            clock,
            runs: Mutex::new(BTreeMap::new()),
        }
    }
}

#[async_trait]
impl Backfiller for FakeBackfiller {
    async fn run(&self, request: BackfillRequest) -> Result<(), BackfillError> {
        {
            let mut runs = self.runs.lock().expect("runs lock");
            *runs.entry(request.repo.name.clone()).or_insert(0) += 1;
        }
        match request.series.series_id.as_str() {
            "np-ok" | "np-reperr" => Ok(()),
            "np-interrupt" => {
                if request.repo.name == "r62" {
                    self.clock.advance_seconds(61);
                }
                Ok(())
            }
            "np-timeout" => Err(BackfillError::Timeout(
                "searchHandler: search timeout".to_owned(),
            )),
            _ => Err(BackfillError::Message("boom".to_owned())),
        }
    }
}

fn rfc3339_offset(value: OffsetDateTime) -> String {
    value
        .to_offset(UtcOffset::UTC)
        .format(&Rfc3339)
        .expect("format timestamp")
}

fn opt_offset(value: Option<OffsetDateTime>) -> String {
    value.map_or_else(|| "-".to_owned(), rfc3339_offset)
}

fn opt_wall(value: Option<PrimitiveDateTime>) -> String {
    value.map_or_else(
        || "-".to_owned(),
        |value| rfc3339_offset(value.assume_utc()),
    )
}

async fn read_iterator_errors(pool: &PgPool, iterator_id: i32) -> String {
    let rows = sqlx::query(
        "SELECT repo_id, failure_count, array_to_string(error_message, '|') AS message \
         FROM repo_iterator_errors WHERE repo_iterator_id = $1 ORDER BY repo_id",
    )
    .bind(iterator_id)
    .fetch_all(pool)
    .await
    .expect("read iterator errors");
    if rows.is_empty() {
        return "-".to_owned();
    }
    rows.iter()
        .map(|row| {
            format!(
                "{}:{}:{}",
                row.try_get::<i32, _>("repo_id").expect("repo_id"),
                row.try_get::<i32, _>("failure_count")
                    .expect("failure_count"),
                row.try_get::<String, _>("message").expect("message")
            )
        })
        .collect::<Vec<_>>()
        .join(",")
}

#[tokio::test]
async fn backfill_inprogress_handler_differential() {
    let corpus_path = match std::env::var("SG_INSIGHTS_BACKFILL_INPROGRESS_CORPUS") {
        Ok(path) if !path.is_empty() => path,
        _ => return,
    };
    let dsn = std::env::var("SG_QTEST_INSIGHTS_DSN").expect("SG_QTEST_INSIGHTS_DSN is required");
    let pool = PgPoolOptions::new()
        .max_connections(4)
        .connect(&dsn)
        .await
        .expect("connect codeinsights");

    let repo_names: HashMap<i32, String> = [
        (1, "r1"),
        (2, "r2"),
        (3, "r3"),
        (4, "r4"),
        (41, "r41"),
        (42, "r42"),
        (43, "r43"),
        (45, "r45"),
        (46, "r46"),
        (61, "r61"),
        (62, "r62"),
        (63, "r63"),
        (71, "r71"),
    ]
    .into_iter()
    .map(|(id, name)| (id, name.to_owned()))
    .collect();

    let corpus = std::fs::read_to_string(&corpus_path).expect("read corpus");
    for line in corpus.lines() {
        if line.is_empty() {
            continue;
        }
        let parts: Vec<&str> = line.split('\t').collect();
        assert_eq!(parts[0], "handle", "unknown op {}", parts[0]);
        let label = parts[1];
        let job_id: i32 = parts[2].parse().expect("job id");
        let backfill_id: i32 = parts[3].parse().expect("backfill id");
        let floor: i64 = parts[4].parse().expect("floor");
        let page_size: usize = parts[5].parse().expect("page size");
        let concurrency: usize = parts[6].parse().expect("concurrency");

        let clock = Arc::new(FakeClock::new());
        let runner = Arc::new(FakeBackfiller::new(Arc::clone(&clock)));
        let handler = InProgressHandler::new(
            pool.clone(),
            Arc::new(FakeRepoStore {
                names: repo_names.clone(),
            }),
            runner.clone(),
            clock,
            HandlerConfig {
                interrupt_after: Duration::from_secs(60),
                error_threshold_floor: floor,
                page_size,
                repo_concurrency: concurrency,
            },
        );

        match handler.handle(job_id, backfill_id).await {
            Ok(()) => println!("ip {label} result=ok"),
            Err(err) => println!("ip {label} result=error error={err}"),
        }

        let backfill = sqlx::query(
            "SELECT state, series_id, repo_iterator_id FROM insight_series_backfill WHERE id = $1",
        )
        .bind(backfill_id)
        .fetch_one(&pool)
        .await
        .expect("read backfill");
        let state: String = backfill.try_get("state").expect("state");
        let series_id: i32 = backfill.try_get("series_id").expect("series_id");
        let iterator_id: Option<i32> = backfill.try_get("repo_iterator_id").expect("iterator_id");
        println!("ip {label} backfill state={state}");

        let series = sqlx::query("SELECT backfill_completed_at FROM insight_series WHERE id = $1")
            .bind(series_id)
            .fetch_one(&pool)
            .await
            .expect("read series");
        let completed: Option<PrimitiveDateTime> = series
            .try_get("backfill_completed_at")
            .expect("backfill_completed_at");
        println!("ip {label} series completed={}", opt_wall(completed));

        match iterator_id {
            Some(iterator_id) => {
                let iterator = sqlx::query(
                    "SELECT started_at, completed_at, repo_cursor, total_count, success_count, \
                     percent_complete, runtime_duration FROM repo_iterator WHERE id = $1",
                )
                .bind(iterator_id)
                .fetch_one(&pool)
                .await
                .expect("read iterator");
                let started: Option<PrimitiveDateTime> =
                    iterator.try_get("started_at").expect("started_at");
                let done: Option<PrimitiveDateTime> =
                    iterator.try_get("completed_at").expect("completed_at");
                let cursor: i32 = iterator.try_get("repo_cursor").expect("repo_cursor");
                let total: i32 = iterator.try_get("total_count").expect("total_count");
                let success: i32 = iterator.try_get("success_count").expect("success_count");
                let pct: f64 = iterator
                    .try_get("percent_complete")
                    .expect("percent_complete");
                let runtime: i64 = iterator
                    .try_get("runtime_duration")
                    .expect("runtime_duration");
                let errors = read_iterator_errors(&pool, iterator_id).await;
                println!(
                    "ip {label} iterator started={} completed={} cursor={cursor} total={total} success={success} pct={:016x} runtime={runtime} errs={errors}",
                    opt_wall(started),
                    opt_wall(done),
                    pct.to_bits()
                );
            }
            None => {
                println!("ip {label} iterator started=- completed=- cursor=- total=- success=- pct=- runtime=- errs=-")
            }
        }

        let points = sqlx::query(
            "SELECT reason, repo_id, time FROM insight_series_incomplete_points \
             WHERE series_id = $1 ORDER BY reason, repo_id NULLS FIRST, time",
        )
        .bind(series_id)
        .fetch_all(&pool)
        .await
        .expect("read incomplete points");
        let rows: Vec<String> = points
            .iter()
            .map(|row| {
                let reason: String = row.try_get("reason").expect("reason");
                let repo_id: Option<i32> = row.try_get("repo_id").expect("repo_id");
                let at: PrimitiveDateTime = row.try_get("time").expect("time");
                format!(
                    "{reason}:{}:{}",
                    repo_id.map_or_else(|| "-".to_owned(), |id| id.to_string()),
                    rfc3339_offset(at.assume_utc())
                )
            })
            .collect();
        println!(
            "ip {label} incomplete count={} rows={}",
            rows.len(),
            if rows.is_empty() {
                "-".to_owned()
            } else {
                rows.join(",")
            }
        );

        let run_rows: Vec<String> = {
            let runs = runner.runs.lock().expect("runs lock");
            runs.iter()
                .map(|(name, count)| format!("{name}:{count}"))
                .collect()
        };
        println!(
            "ip {label} runs={}",
            if run_rows.is_empty() {
                "-".to_owned()
            } else {
                run_rows.join(",")
            }
        );

        let job =
            sqlx::query("SELECT state, process_after FROM insights_background_jobs WHERE id = $1")
                .bind(job_id)
                .fetch_one(&pool)
                .await
                .expect("read job");
        let job_state: String = job.try_get("state").expect("job state");
        let process_after: Option<OffsetDateTime> =
            job.try_get("process_after").expect("process_after");
        println!(
            "ip {label} job state={job_state} process_after={}",
            opt_offset(process_after)
        );
    }
}
