#![forbid(unsafe_code)]
// The fixture tables are tuples; a type aliases would not make them clearer.
#![allow(clippy::type_complexity)]

//! Differential harness for the `insights-query-runner-job` (M9.8ad).
//!
//! Mirror of the Go harness
//! `internal/insights/background/queryrunner/query_runner_harness_test.go`:
//! same fixture scenes, same canonical lines. Drives the real
//! [`QueryRunnerStore`], [`QueryRunnerHandler`], streaming decoders and
//! cleaner against scratch schema-only copies of the frontend and
//! codeinsights databases; only the streaming provider and the sub-repo
//! checker are fakes.
//!
//! Gated by `SG_INSIGHTS_QUERY_RUNNER_HARNESS_ON=1`; connections come from
//! `SG_QTEST_FRONTEND_DSN` (frontend) and `SG_QTEST_DSN` (codeinsights).

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use sqlx::postgres::{PgPool, PgPoolOptions};
use sqlx::types::time::PrimitiveDateTime;
use sqlx::Row;
use time::{Month, OffsetDateTime};
use tokio_util::sync::CancellationToken;

use sg_db::repos::RepoStore;
use sg_service_worker::framework::dbworker::DbWorkerHandler;
use sg_service_worker::jobs::insights_query_runner::handler::QueryRunnerHandler;
use sg_service_worker::jobs::insights_query_runner::limiter::SearchQueryLimiter;
use sg_service_worker::jobs::insights_query_runner::search::{
    classified_error, is_search_timeout, is_terminal_streaming_error, GenerationMethod,
    StreamingSearch, SubRepoChecker,
};
use sg_service_worker::jobs::insights_query_runner::store::{
    clean_jobs, will_retry, QueryRunnerStore, MAX_NUM_RESETS, MAX_NUM_RETRIES, ORDER_BY,
    RETRY_AFTER, STALLED_MAX_AGE, STORE_NAME, TABLE_NAME,
};
use sg_service_worker::jobs::insights_query_runner::streaming::{
    self, ComputeMatch, ComputeTabulationResult, SearchMatch, StreamDecoderEvents, TabulationResult,
};
use sg_service_worker::jobs::insights_query_runner::{
    InsightsQueryRunnerJob, CLEANER_DESCRIPTION, CLEANER_INTERVAL, CLEANER_NAME,
    DEFAULT_CONCURRENCY, RESETTER_INTERVAL, RESETTER_NAME, WORKER_DESCRIPTION, WORKER_INTERVAL,
    WORKER_NAME,
};
use sg_service_worker::jobs::Job as WorkerJob;

fn utc(year: i32, month: Month, day: u8) -> OffsetDateTime {
    time::Date::from_calendar_date(year, month, day)
        .expect("calendar date")
        .with_hms(0, 0, 0)
        .expect("midnight")
        .assume_utc()
}

/// Go's `time.Duration.String` for whole-second durations used here.
fn go_duration(duration: Duration) -> String {
    let secs = duration.as_secs();
    let hours = secs / 3600;
    let minutes = (secs % 3600) / 60;
    let seconds = secs % 60;
    if hours > 0 {
        format!("{hours}h{minutes}m{seconds}s")
    } else if minutes > 0 {
        format!("{minutes}m{seconds}s")
    } else {
        format!("{seconds}s")
    }
}

/// Go's `%v` formatting of a `[]string`.
fn go_vec(items: &[String]) -> String {
    format!("[{}]", items.join(" "))
}

fn go_vec_i32(items: &[i32]) -> String {
    let rendered: Vec<String> = items.iter().map(i32::to_string).collect();
    go_vec(&rendered)
}

/// Go's `%v` formatting of a `map[string]int` (keys sorted).
fn go_map(map: &BTreeMap<String, i32>) -> String {
    let entries: Vec<String> = map
        .iter()
        .map(|(key, value)| format!("{key}:{value}"))
        .collect();
    format!("map[{}]", entries.join(" "))
}

fn tabulation(entries: &[(&str, i32, i32)]) -> TabulationResult {
    let mut result = TabulationResult::default();
    for (name, id, count) in entries {
        result.repo_counts.insert(
            (*name).to_owned(),
            SearchMatch {
                repository_id: *id,
                repository_name: (*name).to_owned(),
                match_count: *count,
            },
        );
        result.total_count += *count;
    }
    result
}

fn compute_result(name: &str, id: i32, values: &[(&str, i32)]) -> ComputeTabulationResult {
    let mut value_counts = BTreeMap::new();
    for (value, count) in values {
        value_counts.insert((*value).to_owned(), *count);
    }
    let mut result = ComputeTabulationResult::default();
    result.repo_counts.insert(
        name.to_owned(),
        ComputeMatch {
            repository_id: id,
            repository_name: name.to_owned(),
            value_counts,
        },
    );
    result
}

struct FakeSearch;

#[async_trait]
impl StreamingSearch for FakeSearch {
    async fn search(
        &self,
        query: &str,
        _ctx: &CancellationToken,
    ) -> Result<TabulationResult, String> {
        match query {
            "search-basic" | "search-dependent" => Ok(tabulation(&[
                ("github.com/org/repo-a", 501, 3),
                ("github.com/org/repo-b", 502, 2),
            ])),
            "search-scoped" => Ok(tabulation(&[
                ("github.com/org/repo-a", 501, 4),
                ("github.com/org/repo-b", 502, 1),
            ])),
            "snapshot-basic" => Ok(tabulation(&[("github.com/org/repo-a", 501, 1)])),
            "search-terminal" => Ok(TabulationResult {
                events: StreamDecoderEvents {
                    errors: vec!["invalid query: bad param".to_owned()],
                    ..StreamDecoderEvents::default()
                },
                ..TabulationResult::default()
            }),
            "search-incomplete" => Ok(TabulationResult {
                events: StreamDecoderEvents {
                    alerts: vec!["Alert: boom".to_owned()],
                    ..StreamDecoderEvents::default()
                },
                ..TabulationResult::default()
            }),
            "search-timeout" => Ok(TabulationResult {
                events: StreamDecoderEvents {
                    did_timeout: true,
                    ..StreamDecoderEvents::default()
                },
                ..TabulationResult::default()
            }),
            other => Err(format!("unexpected search query {other:?}")),
        }
    }

    async fn compute_match_context(
        &self,
        query: &str,
        _ctx: &CancellationToken,
    ) -> Result<ComputeTabulationResult, String> {
        if query != "compute-basic" {
            return Err(format!("unexpected compute query {query:?}"));
        }
        Ok(compute_result(
            "github.com/org/repo-c",
            503,
            &[("x", 2), ("y", 1)],
        ))
    }

    async fn compute_text_extra(
        &self,
        query: &str,
        _ctx: &CancellationToken,
    ) -> Result<ComputeTabulationResult, String> {
        if query != "mapping-basic" {
            return Err(format!("unexpected text query {query:?}"));
        }
        Ok(compute_result(
            "github.com/org/repo-d",
            504,
            &[("one", 1), ("two", 1)],
        ))
    }
}

struct NoSubRepo;

#[async_trait]
impl SubRepoChecker for NoSubRepo {
    async fn enabled_for_repo_id(&self, _repo_id: i32) -> Result<bool, String> {
        Ok(false)
    }
}

#[tokio::test]
async fn insights_query_runner_differential_harness() {
    if std::env::var("SG_INSIGHTS_QUERY_RUNNER_HARNESS_ON").as_deref() != Ok("1") {
        eprintln!("set SG_INSIGHTS_QUERY_RUNNER_HARNESS_ON=1 to run");
        return;
    }
    let frontend_dsn =
        std::env::var("SG_QTEST_FRONTEND_DSN").expect("SG_QTEST_FRONTEND_DSN is required");
    let insights_dsn = std::env::var("SG_QTEST_DSN").expect("SG_QTEST_DSN is required");

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

    cleanup(&frontend, &insights).await;
    seed_handler_fixtures(&frontend, &insights).await;

    // ------------------------------------------------------------------
    // Job + routine metadata.
    // ------------------------------------------------------------------
    let job = InsightsQueryRunnerJob;
    println!(
        "jjjob desc={:?} configs={}",
        WorkerJob::description(&job),
        WorkerJob::configs(&job).len()
    );
    println!(
        "iroutine worker name={} description={} interval={} handlers={}",
        WORKER_NAME,
        WORKER_DESCRIPTION,
        go_duration(WORKER_INTERVAL),
        DEFAULT_CONCURRENCY
    );
    println!(
        "iroutine resetter name={} interval={}",
        RESETTER_NAME,
        go_duration(RESETTER_INTERVAL)
    );
    println!(
        "iroutine cleaner name={} description={} interval={}",
        CLEANER_NAME,
        CLEANER_DESCRIPTION,
        go_duration(CLEANER_INTERVAL)
    );
    println!(
        "iroutine store name={} table={} order={} retry_after={} max_retries={} max_resets={} stalled_max_age={}",
        STORE_NAME,
        TABLE_NAME,
        ORDER_BY,
        go_duration(RETRY_AFTER),
        MAX_NUM_RETRIES,
        MAX_NUM_RESETS,
        go_duration(STALLED_MAX_AGE)
    );

    // ------------------------------------------------------------------
    // Decoder fixtures (shared verbatim with the Go harness).
    // ------------------------------------------------------------------
    let search_body = "event: matches\ndata: [{\"type\":\"content\",\"repository\":\"github.com/org/repo-a\",\"repositoryID\":501,\"chunkMatches\":[{\"ranges\":[{},{}]},{\"ranges\":[{}]}]},{\"type\":\"path\",\"repository\":\"github.com/org/repo-a\",\"repositoryID\":501},{\"type\":\"repo\",\"repository\":\"github.com/org/repo-b\",\"repositoryID\":502},{\"type\":\"commit\",\"repository\":\"github.com/org/repo-b\",\"repositoryID\":502},{\"type\":\"symbol\",\"repository\":\"github.com/org/repo-c\",\"repositoryID\":503,\"symbols\":[{},{},{}]}]\n\nevent: progress\ndata: {\"done\":false,\"skipped\":[{\"reason\":\"shard-timeout\",\"message\":\"ignored\"}]}\n\nevent: progress\ndata: {\"done\":true,\"skipped\":[{\"reason\":\"shard-timeout\",\"message\":\"1 timed out\"},{\"reason\":\"backend-missing\",\"message\":\"zoekt-a\"},{\"reason\":\"display\",\"message\":\"limit\"}]}\n\nevent: alert\ndata: {\"title\":\"No repositories found\",\"description\":\"ignored\"}\n\nevent: alert\ndata: {\"title\":\"Alert\",\"description\":\"boom\"}\n\nevent: error\ndata: {\"message\":\"bad thing\"}\n\nevent: done\ndata: {}\n\nevent: matches\ndata: [{\"type\":\"repo\",\"repository\":\"after-done\",\"repositoryID\":999}]\n\n";
    let search_result = streaming::decode_search(search_body).expect("decode search");
    println!(
        "decoder scene=search total={} timeout={} alerts={} errors={} skipped={}",
        search_result.total_count,
        search_result.events.did_timeout,
        go_vec(&search_result.events.alerts),
        go_vec(&search_result.events.errors),
        go_vec(&search_result.events.skipped_reasons)
    );
    for repo in search_result.repo_counts.values() {
        println!(
            "decoder scene=search repo={} id={} count={}",
            repo.repository_name, repo.repository_id, repo.match_count
        );
    }

    let compute_body = "event: results\ndata: [{\"repository\":\"github.com/org/repo-c\",\"repositoryID\":503,\"matches\":[{\"environment\":{\"v\":{\"value\":\"x\"},\"w\":{\"value\":\"x\"},\"e\":{\"value\":\"\"}}},{\"environment\":{\"v\":{\"value\":\"y\"}}}]}]\n\nevent: results\ndata: [{\"repository\":\"github.com/org/repo-c\",\"repositoryID\":503,\"matches\":[{\"environment\":{\"v\":{\"value\":\"x\"}}}]}]\n\nevent: done\ndata: {}\n\n";
    let match_context =
        streaming::decode_compute_match_context(compute_body).expect("decode match context");
    println!(
        "decoder scene=match-context total={} timeout={} alerts={} errors={}",
        match_context.total_count,
        match_context.events.did_timeout,
        go_vec(&match_context.events.alerts),
        go_vec(&match_context.events.errors)
    );
    for repo in match_context.repo_counts.values() {
        println!(
            "decoder scene=match-context repo={} id={} values={}",
            repo.repository_name,
            repo.repository_id,
            go_map(&repo.value_counts)
        );
    }

    let text_value = format!("one\n\ntwo\n{}", "a".repeat(120));
    let text_body = format!(
        "event: results\ndata: [{{\"repository\":\"github.com/org/repo-d\",\"repositoryID\":504,\"value\":{}}}]\n\nevent: done\ndata: {{}}\n\n",
        serde_json::to_string(&text_value).expect("json string")
    );
    let text_result = streaming::decode_compute_text(&text_body).expect("decode text");
    println!(
        "decoder scene=text total={} alerts={} errors={}",
        text_result.total_count,
        go_vec(&text_result.events.alerts),
        go_vec(&text_result.events.errors)
    );
    for repo in text_result.repo_counts.values() {
        println!(
            "decoder scene=text repo={} id={} values={}",
            repo.repository_name,
            repo.repository_id,
            go_map(&repo.value_counts)
        );
    }

    // ------------------------------------------------------------------
    // Error classification.
    // ------------------------------------------------------------------
    let retryable = vec!["boom".to_owned()];
    let terminal = vec!["invalid query: bad param".to_owned(), "boom".to_owned()];
    println!(
        "errclass scene=search-retryable message={:?}",
        classified_error(&retryable, GenerationMethod::Search)
    );
    println!(
        "errclass scene=search-terminal message={:?}",
        classified_error(&terminal, GenerationMethod::Search)
    );
    println!(
        "errclass scene=compute-terminal message={:?}",
        classified_error(&terminal, GenerationMethod::SearchCompute)
    );
    println!(
        "errclass scene=terminal-nonretryable terminal={}",
        is_terminal_streaming_error(&classified_error(&terminal, GenerationMethod::Search))
    );
    println!(
        "errclass scene=timeout-is={}",
        is_search_timeout("searchHandler: search timeout")
    );

    // ------------------------------------------------------------------
    // Worker store scenes.
    // ------------------------------------------------------------------
    let store = QueryRunnerStore::new(frontend.clone());
    seed_queue_fixtures(&frontend).await;

    println!("queue scene=dequeue");
    for step in 1..=4 {
        match store
            .dequeue("harness", OffsetDateTime::now_utc())
            .await
            .expect("dequeue")
        {
            None => println!("queue scene=dequeue step={step} dequeued=none"),
            Some(job) => println!(
                "queue scene=dequeue step={} id={} series={} priority={} record_time_null={} persist={} num_failures={}",
                step,
                job.id,
                job.series_id,
                job.priority,
                job.record_time.is_none(),
                job.persist_mode,
                job.num_failures
            ),
        }
    }

    sqlx::query("update insights_query_runner_jobs set cancel = true where id = 9301")
        .execute(&frontend)
        .await
        .expect("cancel 9301");
    let (mut known, mut canceled) = store
        .heartbeat(&[9301, 9303, 9999], OffsetDateTime::now_utc())
        .await
        .expect("heartbeat");
    known.sort_unstable();
    canceled.sort_unstable();
    println!(
        "queue scene=heartbeat known={} canceled={}",
        go_vec_i32(&known),
        go_vec_i32(&canceled)
    );

    let mark_scenes = [
        (9401, "errored"),
        (9402, "errored"),
        (9403, "errored"),
        (9404, "failed"),
        (9405, "failed"),
        (9406, "complete"),
        (9407, "complete"),
        (9408, "errored"),
    ];
    for (id, action) in mark_scenes {
        let marked = match action {
            "complete" => store.mark_complete(id).await.expect("mark complete"),
            "errored" => store.mark_errored(id, "boom").await.expect("mark errored"),
            "failed" => store.mark_failed(id, "boom").await.expect("mark failed"),
            other => panic!("unknown action {other}"),
        };
        println!(
            "queue scene=mark id={} action={} marked={} state={} failures={} resets={} message={}",
            id,
            action,
            marked,
            query_string(
                &frontend,
                "select state from insights_query_runner_jobs where id = $1",
                id
            )
            .await,
            query_i32(
                &frontend,
                "select num_failures from insights_query_runner_jobs where id = $1",
                id
            )
            .await,
            query_i32(
                &frontend,
                "select num_resets from insights_query_runner_jobs where id = $1",
                id
            )
            .await,
            query_nullable(
                &frontend,
                "select failure_message from insights_query_runner_jobs where id = $1",
                id
            )
            .await
        );
    }

    let (mut reset, mut failed) = store
        .reset_stalled_at(OffsetDateTime::now_utc())
        .await
        .expect("reset stalled");
    reset.sort_unstable_by_key(|(id, _)| *id);
    failed.sort_unstable_by_key(|(id, _)| *id);
    for (id, _) in reset {
        println!(
            "queue scene=reset id={} to=queued state={} resets={}",
            id,
            query_string(
                &frontend,
                "select state from insights_query_runner_jobs where id = $1",
                id
            )
            .await,
            query_i32(
                &frontend,
                "select num_resets from insights_query_runner_jobs where id = $1",
                id
            )
            .await
        );
    }
    for (id, _) in failed {
        println!(
            "queue scene=reset id={} to=failed state={} message={}",
            id,
            query_string(
                &frontend,
                "select state from insights_query_runner_jobs where id = $1",
                id
            )
            .await,
            query_nullable(
                &frontend,
                "select failure_message from insights_query_runner_jobs where id = $1",
                id
            )
            .await
        );
    }
    for id in [9502, 9504] {
        println!(
            "queue scene=reset id={} untouched state={} resets={}",
            id,
            query_string(
                &frontend,
                "select state from insights_query_runner_jobs where id = $1",
                id
            )
            .await,
            query_i32(
                &frontend,
                "select num_resets from insights_query_runner_jobs where id = $1",
                id
            )
            .await
        );
    }

    println!(
        "queue scene=will-retry {} {} {}",
        will_retry(9),
        will_retry(8),
        will_retry(10)
    );

    // ------------------------------------------------------------------
    // Handler scenes. The provider and sub-repo checker are fakes; the
    // store, decoders, series lookup and persistence are real.
    // ------------------------------------------------------------------
    let handler = QueryRunnerHandler::new(
        Arc::new(QueryRunnerStore::new(frontend.clone())),
        insights.clone(),
        RepoStore::new(frontend.clone()),
        Arc::new(FakeSearch),
        Arc::new(NoSubRepo),
        Arc::new(SearchQueryLimiter::new(1000.0, 10)),
    );

    for (scene, job_id) in [
        ("search", 9201),
        ("dependent", 9202),
        ("jit", 9203),
        ("compute", 9204),
        ("mapping", 9205),
        ("snapshot", 9206),
        ("scoped", 9207),
        ("missing", 9208),
        ("bad-method", 9209),
        ("terminal", 9210),
        ("incomplete", 9211),
    ] {
        run_handler_scene(&handler, &store, &insights, scene, job_id).await;
        if scene == "incomplete" {
            print_incomplete(&insights, scene, 9110).await;
        }
    }
    run_handler_scene(&handler, &store, &insights, "timeout", 9212).await;
    print_incomplete(&insights, "timeout", 9111).await;

    // ------------------------------------------------------------------
    // Cleaner.
    // ------------------------------------------------------------------
    let before = OffsetDateTime::now_utc() - time::Duration::hours(168);
    let cleaned = clean_jobs(&frontend, before).await.expect("clean jobs");
    println!("clean deleted={cleaned}");
}

async fn run_handler_scene(
    handler: &QueryRunnerHandler,
    store: &QueryRunnerStore,
    insights: &PgPool,
    scene: &str,
    job_id: i32,
) {
    let record = store.dequeue_job(job_id).await.expect("dequeue job");
    let result = handler.handle(CancellationToken::new(), &record).await;
    match &result {
        Ok(()) => println!("handler scene={scene} err=none"),
        Err(err) => println!("handler scene={scene} err={err:?}"),
    }
    if let Err(err) = &result {
        println!(
            "handler scene={scene} terminal={}",
            is_terminal_streaming_error(err)
        );
    }
    print_series_points(insights, scene, &record.series_id).await;
    print_recording_times(insights, scene, &record.series_id).await;
}

async fn print_incomplete(insights: &PgPool, scene: &str, series_id: i32) {
    let reason = sqlx::query(
        "select reason from insight_series_incomplete_points where series_id = $1 order by id limit 1",
    )
    .bind(series_id)
    .fetch_optional(insights)
    .await
    .expect("incomplete reason")
    .map(|row| row.try_get::<String, _>(0).expect("reason"));
    let count: i64 =
        sqlx::query("select count(*) from insight_series_incomplete_points where series_id = $1")
            .bind(series_id)
            .fetch_one(insights)
            .await
            .expect("incomplete count")
            .try_get(0)
            .expect("count");
    println!(
        "handler scene={} incomplete_reason={} count={}",
        scene,
        reason.unwrap_or_else(|| "null".to_owned()),
        count
    );
}

async fn print_series_points(insights: &PgPool, scene: &str, series_id: &str) {
    let rows = sqlx::query(
        r#"select sp.series_id, coalesce(sp.repo_id, -1), coalesce(rn.name::text, '-'), coalesce(sp.capture, '-'), sp.value,
		        to_char(sp."time" at time zone 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS')
		 from series_points sp left join repo_names rn on rn.id = sp.repo_name_id
		 where sp.series_id = $1
		 order by sp.repo_id, sp.capture, sp."time", sp.value"#,
    )
    .bind(series_id)
    .fetch_all(insights)
    .await
    .expect("series points");
    for row in rows {
        let series: String = row.try_get(0).expect("series");
        let repo_id: i32 = row.try_get(1).expect("repo id");
        let repo_name: String = row.try_get(2).expect("repo name");
        let capture: String = row.try_get(3).expect("capture");
        let value: f64 = row.try_get(4).expect("value");
        let at: String = row.try_get(5).expect("time");
        println!(
            "handler scene={} point series={} repo={} repo_name={} capture={} value={} time={}",
            scene, series, repo_id, repo_name, capture, value, at
        );
    }

    let snapshot_rows = sqlx::query(
        r#"select coalesce(capture, '-'), value, to_char("time" at time zone 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS')
		 from series_points_snapshots where series_id = $1 order by repo_id, capture, "time", value"#,
    )
    .bind(series_id)
    .fetch_all(insights)
    .await
    .expect("snapshot points");
    for row in snapshot_rows {
        let capture: String = row.try_get(0).expect("capture");
        let value: f64 = row.try_get(1).expect("value");
        let at: String = row.try_get(2).expect("time");
        println!(
            "handler scene={} snapshot series={} capture={} value={} time={}",
            scene, series_id, capture, value, at
        );
    }
}

async fn print_recording_times(insights: &PgPool, scene: &str, series_id: &str) {
    let rows = sqlx::query(
        r#"select ist.insight_series_id, ist.snapshot, to_char(ist.recording_time at time zone 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS')
		 from insight_series_recording_times ist
		 join insight_series s on s.id = ist.insight_series_id
		 where s.series_id = $1 order by ist.recording_time, ist.snapshot"#,
    )
    .bind(series_id)
    .fetch_all(insights)
    .await
    .expect("recording times");
    for row in rows {
        let id: i32 = row.try_get(0).expect("id");
        let snapshot: bool = row.try_get(1).expect("snapshot");
        let at: String = row.try_get(2).expect("time");
        println!(
            "handler scene={} time series={} snapshot={} at={}",
            scene, series_id, snapshot, at
        );
        let _ = id;
    }
}

async fn query_string(pool: &PgPool, sql: &str, id: i32) -> String {
    let row = sqlx::query(sql)
        .bind(id)
        .fetch_one(pool)
        .await
        .expect("query string");
    let value: Option<String> = row.try_get(0).expect("scan string");
    value.unwrap_or_else(|| "-".to_owned())
}

async fn query_nullable(pool: &PgPool, sql: &str, id: i32) -> String {
    let row = sqlx::query(sql)
        .bind(id)
        .fetch_one(pool)
        .await
        .expect("query nullable");
    let value: Option<String> = row.try_get(0).expect("scan nullable");
    value.unwrap_or_else(|| "null".to_owned())
}

async fn query_i32(pool: &PgPool, sql: &str, id: i32) -> i32 {
    let row = sqlx::query(sql)
        .bind(id)
        .fetch_one(pool)
        .await
        .expect("query i32");
    row.try_get(0).expect("scan i32")
}

async fn cleanup(frontend: &PgPool, insights: &PgPool) {
    for sql in [
        "delete from insights_query_runner_jobs where id between 9200 and 9700",
        "delete from repo where id between 500 and 599",
    ] {
        sqlx::query(sql).execute(frontend).await.expect(sql);
    }
    for sql in [
        "delete from series_points where series_id like 'qr-%'",
        "delete from series_points_snapshots where series_id like 'qr-%'",
        "delete from insight_series_incomplete_points where series_id between 9100 and 9199",
        "delete from insight_series_recording_times where insight_series_id between 9100 and 9199",
        "delete from insight_series where id between 9100 and 9199",
        "delete from repo_names where name::text like 'github.com/org/repo-%'",
    ] {
        sqlx::query(sql).execute(insights).await.expect(sql);
    }
}

async fn seed_handler_fixtures(frontend: &PgPool, insights: &PgPool) {
    for (id, name) in [
        (501, "github.com/org/repo-a"),
        (502, "github.com/org/repo-b"),
        (503, "github.com/org/repo-c"),
        (504, "github.com/org/repo-d"),
    ] {
        sqlx::query(
            "insert into repo (id, name, private, external_service_type) values ($1, $2, false, 'github')",
        )
        .bind(id)
        .bind(name)
        .execute(frontend)
        .await
        .expect("seed repo");
    }

    let series: [(i32, &str, &str, bool, Option<Vec<String>>); 11] = [
        (9101, "qr-search", "search", false, None),
        (9102, "qr-dependent", "search", false, None),
        (9103, "qr-compute", "search-compute", false, None),
        (9104, "qr-mapping", "mapping-compute", false, None),
        (9105, "qr-snapshot", "search", false, None),
        (
            9106,
            "qr-scoped",
            "search",
            false,
            Some(vec!["github.com/org/repo-a".to_owned()]),
        ),
        (9108, "qr-bad-method", "language-stats", false, None),
        (9109, "qr-terminal", "search", false, None),
        (9110, "qr-incomplete", "search", false, None),
        (9111, "qr-timeout", "search", false, None),
        (9112, "qr-jit", "search", true, None),
    ];
    for (id, series_id, method, jit, repositories) in series {
        sqlx::query(
            "insert into insight_series (id, series_id, query, generation_method, just_in_time, repositories) values ($1, $2, 'fixture', $3, $4, $5)",
        )
        .bind(id)
        .bind(series_id)
        .bind(method)
        .bind(jit)
        .bind(repositories)
        .execute(insights)
        .await
        .expect("seed series");
    }

    let record_time = Some(utc(2024, Month::March, 1));
    let jobs: [(i32, &str, &str, Option<OffsetDateTime>, &str, i32); 12] = [
        (9201, "qr-search", "search-basic", record_time, "record", 0),
        (
            9202,
            "qr-dependent",
            "search-dependent",
            record_time,
            "record",
            0,
        ),
        (9203, "qr-jit", "search-basic", record_time, "record", 0),
        (
            9204,
            "qr-compute",
            "compute-basic",
            record_time,
            "record",
            0,
        ),
        (
            9205,
            "qr-mapping",
            "mapping-basic",
            record_time,
            "record",
            0,
        ),
        (
            9206,
            "qr-snapshot",
            "snapshot-basic",
            record_time,
            "snapshot",
            0,
        ),
        (9207, "qr-scoped", "search-scoped", record_time, "record", 0),
        (9208, "qr-missing", "search-basic", record_time, "record", 0),
        (
            9209,
            "qr-bad-method",
            "search-basic",
            record_time,
            "record",
            0,
        ),
        (
            9210,
            "qr-terminal",
            "search-terminal",
            record_time,
            "record",
            0,
        ),
        (
            9211,
            "qr-incomplete",
            "search-incomplete",
            None,
            "record",
            9,
        ),
        (9212, "qr-timeout", "search-timeout", None, "record", 9),
    ];
    for (id, series_id, query, record_time, persist_mode, failures) in jobs {
        sqlx::query(
            "insert into insights_query_runner_jobs (id, series_id, search_query, record_time, persist_mode, state, num_failures, priority, cost) values ($1, $2, $3, $4, $5::persistmode, 'processing', $6, 1, 500)",
        )
        .bind(id)
        .bind(series_id)
        .bind(query)
        .bind(record_time)
        .bind(persist_mode)
        .bind(failures)
        .execute(frontend)
        .await
        .expect("seed job");
    }
    for recording_time in [
        utc(2024, Month::February, 1),
        utc(2024, Month::February, 15),
    ] {
        let primitive = PrimitiveDateTime::new(recording_time.date(), recording_time.time());
        sqlx::query(
            "insert into insights_query_runner_jobs_dependencies (job_id, recording_time) values ($1, $2)",
        )
        .bind(9202)
        .bind(primitive)
        .execute(frontend)
        .await
        .expect("seed dependency");
    }

    sqlx::query(
        "insert into series_points_snapshots (series_id, \"time\", value) values ('qr-snapshot', '2024-01-01T00:00:00Z', 9)",
    )
    .execute(insights)
    .await
    .expect("seed snapshot");
    sqlx::query(
        "insert into series_points (series_id, \"time\", value) values ('qr-snapshot', '2024-01-01T00:00:00Z', 7)",
    )
    .execute(insights)
    .await
    .expect("seed point");
    sqlx::query(
        "insert into insight_series_recording_times (insight_series_id, recording_time, snapshot) values (9105, '2024-01-01T00:00:00Z', true)",
    )
    .execute(insights)
    .await
    .expect("seed recording time");
}

async fn seed_queue_fixtures(frontend: &PgPool) {
    let now = OffsetDateTime::now_utc();
    let queue_record_time = Some(utc(2024, Month::May, 1));
    let jobs: [(
        i32,
        i32,
        &str,
        Option<OffsetDateTime>,
        Option<OffsetDateTime>,
    ); 6] = [
        (9301, 1, "queued", None, None),
        (
            9302,
            1,
            "queued",
            Some(now + time::Duration::minutes(10)),
            None,
        ),
        (9303, 0, "queued", None, None),
        (
            9304,
            1,
            "errored",
            None,
            Some(now - time::Duration::minutes(31)),
        ),
        (
            9305,
            1,
            "errored",
            None,
            Some(now - time::Duration::minutes(1)),
        ),
        (9306, 1, "processing", None, None),
    ];
    for (id, priority, state, process_after, finished_at) in jobs {
        sqlx::query(
            "insert into insights_query_runner_jobs (id, series_id, search_query, state, priority, process_after, finished_at, record_time) values ($1, $2, 'queue', $3, $4, $5, $6, $7)",
        )
        .bind(id)
        .bind(format!("queue-{id}"))
        .bind(state)
        .bind(priority)
        .bind(process_after)
        .bind(finished_at)
        .bind(queue_record_time)
        .execute(frontend)
        .await
        .expect("seed queue job");
    }

    let marks: [(i32, &str, bool, i32); 8] = [
        (9401, "processing", false, 0),
        (9402, "processing", false, 9),
        (9403, "processing", true, 0),
        (9404, "processing", true, 0),
        (9405, "processing", false, 0),
        (9406, "processing", false, 0),
        (9407, "queued", false, 0),
        (9408, "completed", false, 0),
    ];
    for (id, state, cancel, failures) in marks {
        sqlx::query(
            "insert into insights_query_runner_jobs (id, series_id, search_query, state, cancel, num_failures) values ($1, $2, 'mark', $3, $4, $5)",
        )
        .bind(id)
        .bind(format!("mark-{id}"))
        .bind(state)
        .bind(cancel)
        .bind(failures)
        .execute(frontend)
        .await
        .expect("seed mark job");
    }

    let resets: [(i32, &str, Option<OffsetDateTime>, i32); 4] = [
        (
            9501,
            "processing",
            Some(now - time::Duration::seconds(61)),
            0,
        ),
        (
            9502,
            "processing",
            Some(now - time::Duration::seconds(59)),
            0,
        ),
        (
            9503,
            "processing",
            Some(now - time::Duration::seconds(61)),
            10,
        ),
        (9504, "queued", None, 0),
    ];
    for (id, state, last_heartbeat, resets) in resets {
        sqlx::query(
            "insert into insights_query_runner_jobs (id, series_id, search_query, state, last_heartbeat_at, num_resets) values ($1, $2, 'reset', $3, $4, $5)",
        )
        .bind(id)
        .bind(format!("reset-{id}"))
        .bind(state)
        .bind(last_heartbeat)
        .bind(resets)
        .execute(frontend)
        .await
        .expect("seed reset job");
    }

    sqlx::query(
        "insert into insights_query_runner_jobs (id, series_id, search_query, state, started_at) values
			(9601, 'clean-1', 'clean', 'completed', now() - interval '169 hours'),
			(9602, 'clean-2', 'clean', 'completed', now() - interval '167 hours'),
			(9603, 'clean-3', 'clean', 'failed', now() - interval '169 hours'),
			(9604, 'clean-4', 'clean', 'queued', now() - interval '169 hours')",
    )
    .execute(frontend)
    .await
    .expect("seed clean jobs");
}
