#![forbid(unsafe_code)]

//! Differential harness for the new-state backfill handler (M9.8cb+1).
//!
//! Mirror of the Go harness
//! `internal/insights/scheduler/parity_new_backfill_handler_harness_test.go`:
//! same fixture scenes, same canonical `bh ` lines. Drives
//! [`NewBackfillHandler::handle_new_backfill`] (the port of
//! `newBackfillHandler.Handle`) with a scripted repository selector, so both
//! sides resolve repositories identically without touching the repo tables.
//!
//! Gated by `SG_BACKFILL_HANDLER_CORPUS`; the connection comes from
//! `SG_QTEST_INSIGHTS_DSN` (a scratch clone of the `codeinsights` database).
//! Wall-clock columns (job `finished_at`, iterator `created_at`) are reported
//! as `set`/`-` or omitted.

use std::sync::Arc;

use async_trait::async_trait;
use sqlx::postgres::{PgPool, PgPoolOptions};
use sqlx::types::time::OffsetDateTime;
use sqlx::Row;
use time::format_description::well_known::Rfc3339;
use time::UtcOffset;

use sg_insights::discovery::{RepoIter, SeriesRepoSelector, SeriesScope};
use sg_service_worker::jobs::insights_backfill_handler::NewBackfillHandler;

/// Scripted `discovery.RepoIterator`.
struct VecIter {
    repos: Vec<(String, i64)>,
    fail_on: Option<i64>,
    error: String,
}

#[async_trait]
impl RepoIter for VecIter {
    async fn for_each(
        &self,
        each: &mut (dyn for<'a> FnMut(&'a str, i64) -> Result<(), String> + Send),
    ) -> Result<(), String> {
        for (name, id) in &self.repos {
            each(name, *id)?;
            if Some(*id) == self.fail_on {
                return Err(self.error.clone());
            }
        }
        Ok(())
    }
}

/// Scripted `discovery.SeriesRepoIterator`.
struct FakeSelector;

#[async_trait]
impl SeriesRepoSelector for FakeSelector {
    async fn for_series(&self, scope: &SeriesScope) -> Result<Box<dyn RepoIter>, String> {
        if let Some(criteria) = &scope.repository_criteria {
            match criteria.as_str() {
                "boom" => return Err("boom".to_owned()),
                "cbfail" => {
                    return Ok(Box::new(VecIter {
                        repos: vec![("cb1".to_owned(), 11), ("cb3".to_owned(), 13)],
                        fail_on: Some(13),
                        error: "cb-error".to_owned(),
                    }))
                }
                _ => {
                    return Ok(Box::new(VecIter {
                        repos: vec![
                            ("q1".to_owned(), 11),
                            ("q2".to_owned(), 12),
                            ("q3".to_owned(), 13),
                        ],
                        fail_on: None,
                        error: String::new(),
                    }))
                }
            }
        }
        if !scope.repositories.is_empty() {
            let mut repos = Vec::new();
            for name in &scope.repositories {
                let id = match name.as_str() {
                    "repoA" => 21,
                    "repoB" => 22,
                    _ => continue,
                };
                repos.push((name.clone(), id));
            }
            return Ok(Box::new(VecIter {
                repos,
                fail_on: None,
                error: String::new(),
            }));
        }
        Ok(Box::new(VecIter {
            repos: vec![
                ("all1".to_owned(), 31),
                ("all2".to_owned(), 32),
                ("all3".to_owned(), 33),
                ("all4".to_owned(), 34),
                ("all5".to_owned(), 35),
            ],
            fail_on: None,
            error: String::new(),
        }))
    }
}

fn rfc3339(time: OffsetDateTime) -> String {
    time.to_offset(UtcOffset::UTC)
        .format(&Rfc3339)
        .expect("format timestamp")
}

fn opt_time(value: Option<OffsetDateTime>) -> String {
    value.map_or_else(|| "-".to_owned(), rfc3339)
}

fn join_i32s(values: &[i32]) -> String {
    values
        .iter()
        .map(i32::to_string)
        .collect::<Vec<_>>()
        .join(",")
}

async fn print_state(pool: &PgPool, label: &str, job_id: i32, backfill_id: i32, series_id: i32) {
    let backfill = sqlx::query(
        "SELECT state, estimated_cost, repo_iterator_id FROM insight_series_backfill WHERE id = $1",
    )
    .bind(backfill_id)
    .fetch_one(pool)
    .await
    .unwrap_or_else(|err| panic!("read backfill {backfill_id}: {err}"));
    let state: String = backfill.try_get("state").expect("state");
    let cost: Option<f64> = backfill.try_get("estimated_cost").expect("estimated_cost");
    let iterator_id: Option<i32> = backfill
        .try_get("repo_iterator_id")
        .expect("repo_iterator_id");
    println!(
        "bh {label} backfill state={state} cost={} iterator={}",
        cost.map_or_else(|| "-".to_owned(), |cost| format!("{cost:.6}")),
        iterator_id.map_or_else(|| "-".to_owned(), |id| id.to_string())
    );

    match iterator_id {
        Some(id) => {
            let iterator =
                sqlx::query("SELECT total_count, repos FROM repo_iterator WHERE id = $1")
                    .bind(id)
                    .fetch_one(pool)
                    .await
                    .unwrap_or_else(|err| panic!("read iterator {id}: {err}"));
            let total: i32 = iterator.try_get("total_count").expect("total_count");
            let repos: Option<Vec<i32>> = iterator.try_get("repos").expect("repos");
            println!(
                "bh {label} iterator total={total} repos={}",
                repos.map_or_else(|| "-".to_owned(), |repos| join_i32s(&repos))
            );
        }
        None => println!("bh {label} iterator total=- repos=-"),
    }

    let recordings = sqlx::query(
        r#"
SELECT COUNT(*) AS count, MIN(recording_time) AS first, MAX(recording_time) AS last,
       BOOL_AND(snapshot) AS snapshot
FROM insight_series_recording_times WHERE insight_series_id = $1
"#,
    )
    .bind(series_id)
    .fetch_one(pool)
    .await
    .unwrap_or_else(|err| panic!("read recordings for {series_id}: {err}"));
    let count: i64 = recordings.try_get("count").expect("count");
    let first: Option<OffsetDateTime> = recordings.try_get("first").expect("first");
    let last: Option<OffsetDateTime> = recordings.try_get("last").expect("last");
    let snapshot: Option<bool> = recordings.try_get("snapshot").expect("snapshot");
    println!(
        "bh {label} recordings count={count} first={} last={} snapshot={}",
        opt_time(first),
        opt_time(last),
        snapshot.map_or_else(|| "-".to_owned(), |value| value.to_string())
    );

    let job = sqlx::query(
        "SELECT state, finished_at IS NOT NULL AS finished FROM insights_background_jobs WHERE id = $1",
    )
    .bind(job_id)
    .fetch_one(pool)
    .await
    .unwrap_or_else(|err| panic!("read job {job_id}: {err}"));
    let job_state: String = job.try_get("state").expect("state");
    let finished: bool = job.try_get("finished").expect("finished");
    println!(
        "bh {label} job state={job_state} finished={}",
        if finished { "set" } else { "-" }
    );

    let enqueued = sqlx::query(
        "SELECT id, state FROM insights_background_jobs WHERE backfill_id = $1 AND id <> $2 ORDER BY id",
    )
    .bind(backfill_id)
    .bind(job_id)
    .fetch_all(pool)
    .await
    .unwrap_or_else(|err| panic!("read enqueued for {backfill_id}: {err}"));
    let enqueued: Vec<String> = enqueued
        .iter()
        .map(|row| {
            let id: i32 = row.try_get("id").expect("id");
            let state: String = row.try_get("state").expect("state");
            format!("{id}:{state}")
        })
        .collect();
    println!(
        "bh {label} enqueued={}",
        if enqueued.is_empty() {
            "-".to_owned()
        } else {
            enqueued.join(",")
        }
    );
}

#[tokio::test]
async fn backfill_handler_differential() {
    let corpus_path = match std::env::var("SG_BACKFILL_HANDLER_CORPUS") {
        Ok(path) if !path.is_empty() => path,
        _ => return,
    };
    let dsn = std::env::var("SG_QTEST_INSIGHTS_DSN").expect("SG_QTEST_INSIGHTS_DSN is required");
    let pool = PgPoolOptions::new()
        .max_connections(4)
        .connect(&dsn)
        .await
        .expect("connect codeinsights");

    let handler = NewBackfillHandler::new(pool.clone(), Arc::new(FakeSelector));
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

        let series_id: i32 =
            sqlx::query("SELECT series_id FROM insight_series_backfill WHERE id = $1")
                .bind(backfill_id)
                .fetch_one(&pool)
                .await
                .expect("read backfill series")
                .try_get("series_id")
                .expect("series_id");

        match handler.handle_new_backfill(job_id, backfill_id).await {
            Ok(()) => println!("bh {label} result=ok"),
            Err(err) => println!("bh {label} result=error error={err}"),
        }
        print_state(&pool, label, job_id, backfill_id, series_id).await;
    }
}
