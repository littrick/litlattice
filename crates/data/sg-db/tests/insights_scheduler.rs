#![forbid(unsafe_code)]

//! M9.8by differential harness (Rust side): the insights scheduler storage
//! (`insights_scheduler` module) against the `codeinsights` database.
//!
//! Mirrors `internal/insights/scheduler/parity_db_harness_test.go` and
//! `internal/insights/scheduler/iterator/parity_db_harness_test.go`.
//! `test_ws/p9-go/run_insights_scheduler_db_diff.sh` applies
//! `insights_scheduler_fixtures.sql` immediately before each side and diffs
//! the canonical `bf ` / `it ` lines. Floats are printed as their 64-bit
//! pattern and timestamps as RFC3339 so both languages render them the same.
//!
//! The backfill corpus runs first (it creates repo iterator 201), then the
//! iterator corpus, which references the ids the fixture sequences produce.

use std::io::Write;

use sg_db::insights_scheduler as is;
use sqlx::postgres::PgPool;
use time::format_description::well_known::Rfc3339;
use time::{OffsetDateTime, PrimitiveDateTime};

fn atoi(spec: &str) -> i32 {
    spec.parse()
        .unwrap_or_else(|err| panic!("atoi {spec:?}: {err}"))
}

fn parse_time(spec: &str) -> OffsetDateTime {
    OffsetDateTime::parse(spec, &Rfc3339).unwrap_or_else(|err| panic!("parse time {spec:?}: {err}"))
}

fn fmt_time(time: Option<PrimitiveDateTime>) -> String {
    match time {
        Some(time) => time.assume_utc().format(&Rfc3339).expect("format time"),
        None => "-".to_owned(),
    }
}

fn bits(value: f64) -> String {
    format!("{:016x}", value.to_bits())
}

fn err_text(err: &sqlx::Error) -> String {
    match err {
        sqlx::Error::Protocol(message) => message.clone(),
        other => other.to_string(),
    }
}

fn fmt_errors(errors: &[is::IterationError]) -> String {
    if errors.is_empty() {
        return "-".to_owned();
    }
    let mut sorted: Vec<&is::IterationError> = errors.iter().collect();
    sorted.sort_by_key(|err| err.repo_id);
    sorted
        .iter()
        .map(|err| {
            format!(
                "{}:{}:{}",
                err.repo_id,
                err.failure_count,
                err.error_messages.join("|")
            )
        })
        .collect::<Vec<_>>()
        .join(",")
}

fn print_backfill(op: &str, backfill: &is::SeriesBackfill) {
    println!(
        "bf {} id={} series={} itr={} cost={} state={}",
        op,
        backfill.id,
        backfill.series_id,
        backfill.repo_iterator_id.unwrap_or(0),
        bits(backfill.estimated_cost),
        backfill.state
    );
}

fn print_iterator(op: &str, itr: &is::RepoIterator) {
    println!(
        "it {} id={} created={} started={} completed={} total={} cursor={} pct={} runtime={} success={} errs={}",
        op,
        itr.id,
        fmt_time(itr.created_at),
        fmt_time(itr.started_at),
        fmt_time(itr.completed_at),
        itr.total_count,
        itr.cursor,
        bits(itr.percent_complete),
        itr.runtime_duration,
        itr.success_count,
        fmt_errors(&itr.errors)
    );
}

async fn jobs_for(pool: &PgPool, backfill_id: i32) -> i64 {
    sqlx::query_scalar("SELECT count(*) FROM insights_background_jobs WHERE backfill_id = $1")
        .bind(backfill_id)
        .fetch_one(pool)
        .await
        .expect("count jobs")
}

async fn run_backfill_corpus(pool: &PgPool, corpus_path: &str) {
    let now = parse_time("2021-01-01T00:00:00Z");
    let corpus = std::fs::read_to_string(corpus_path).expect("read backfill corpus");
    for line in corpus.lines() {
        if line.is_empty() {
            continue;
        }
        let parts: Vec<&str> = line.split('\t').collect();
        match parts[0] {
            "newbackfill" => {
                let backfill = is::new_backfill(pool, atoi(parts[1]))
                    .await
                    .expect("NewBackfill");
                print_backfill("newbackfill", &backfill);
            }
            "loadbackfill" => {
                let backfill = is::load_backfill(pool, atoi(parts[1]))
                    .await
                    .expect("LoadBackfill");
                print_backfill("loadbackfill", &backfill);
            }
            "loadseries" => {
                let series_id = atoi(parts[1]);
                let mut backfills = is::load_series_backfills(pool, series_id)
                    .await
                    .expect("LoadSeriesBackfills");
                backfills.sort_by_key(|backfill| backfill.id);
                let rows = backfills
                    .iter()
                    .map(|backfill| {
                        format!(
                            "{}:{}:{}:{}",
                            backfill.id,
                            backfill.repo_iterator_id.unwrap_or(0),
                            bits(backfill.estimated_cost),
                            backfill.state
                        )
                    })
                    .collect::<Vec<_>>();
                println!(
                    "bf loadseries series={} count={} rows={}",
                    series_id,
                    backfills.len(),
                    if rows.is_empty() {
                        "-".to_owned()
                    } else {
                        rows.join(",")
                    }
                );
            }
            "setstate" => {
                let id = atoi(parts[1]);
                let updated = match parts[2] {
                    "completed" => is::set_completed(pool, id).await.expect("SetCompleted"),
                    "failed" => is::set_failed(pool, id).await.expect("SetFailed"),
                    other => panic!("unknown state {other:?}"),
                };
                print_backfill("setstate", &updated);
            }
            "setscope" => {
                let id = atoi(parts[1]);
                let repos: Vec<i32> = parts[2].split(',').map(atoi).collect();
                let cost: f64 = parts[3].parse().expect("cost");
                let (backfill, itr) = is::set_scope(pool, id, &repos, cost, now)
                    .await
                    .expect("SetScope");
                println!(
                    "bf setscope id={} series={} itr={} cost={} state={} created={} total={} cursor={} pct={}",
                    backfill.id,
                    backfill.series_id,
                    backfill.repo_iterator_id.unwrap_or(0),
                    bits(backfill.estimated_cost),
                    backfill.state,
                    fmt_time(itr.created_at),
                    itr.total_count,
                    itr.cursor,
                    bits(itr.percent_complete)
                );
            }
            "prioritylowest" => {
                let id = atoi(parts[1]);
                let loaded = is::load_backfill(pool, id).await.expect("LoadBackfill");
                is::set_lowest_priority(pool, id, loaded.estimated_cost)
                    .await
                    .expect("SetLowestPriority");
                let backfill = is::load_backfill(pool, id).await.expect("LoadBackfill");
                print_backfill("prioritylowest", &backfill);
            }
            "priorityhighest" => {
                let id = atoi(parts[1]);
                is::set_highest_priority(pool, id)
                    .await
                    .expect("SetHighestPriority");
                let backfill = is::load_backfill(pool, id).await.expect("LoadBackfill");
                print_backfill("priorityhighest", &backfill);
            }
            "retry" => {
                let id = atoi(parts[1]);
                if let Err(err) = is::retry_backfill_attempt(pool, id).await {
                    println!("bf retry id={} result=error:{}", id, err_text(&err));
                    continue;
                }
                let backfill = is::load_backfill(pool, id).await.expect("LoadBackfill");
                let itr = is::load_iterator(pool, backfill.repo_iterator_id.unwrap_or(0))
                    .await
                    .expect("Load iterator");
                println!(
                    "bf retry id={} result=ok state={} itr={} pct={} cursor={} success={} runtime={} errs={} jobs={}",
                    id,
                    backfill.state,
                    backfill.repo_iterator_id.unwrap_or(0),
                    bits(itr.percent_complete),
                    itr.cursor,
                    itr.success_count,
                    itr.runtime_duration,
                    itr.errors.len(),
                    jobs_for(pool, id).await
                );
            }
            "initial" => {
                let series_id = atoi(parts[1]);
                let backfill = is::initial_backfill(pool, series_id)
                    .await
                    .expect("InitialBackfill");
                println!(
                    "bf initial series={} id={} itr={} cost={} state={} jobs={}",
                    series_id,
                    backfill.id,
                    backfill.repo_iterator_id.unwrap_or(0),
                    bits(backfill.estimated_cost),
                    backfill.state,
                    jobs_for(pool, backfill.id).await
                );
            }
            "enqueue" => {
                let id = atoi(parts[1]);
                match is::enqueue_backfill(pool, id).await {
                    Err(err) => println!("bf enqueue id={} result=error:{}", id, err_text(&err)),
                    Ok(()) => println!(
                        "bf enqueue id={} result=ok jobs={}",
                        id,
                        jobs_for(pool, id).await
                    ),
                }
            }
            other => panic!("unknown op {other:?}"),
        }
        std::io::stdout().flush().expect("flush");
    }
}

async fn run_iterator_corpus(pool: &PgPool, corpus_path: &str) {
    let corpus = std::fs::read_to_string(corpus_path).expect("read iterator corpus");
    for line in corpus.lines() {
        if line.is_empty() {
            continue;
        }
        let parts: Vec<&str> = line.split('\t').collect();
        match parts[0] {
            "newiterator" => {
                let now = parse_time(parts[1]);
                let repos: Vec<i32> = parts[2].split(',').map(atoi).collect();
                let itr = is::new_iterator(pool, &repos, now)
                    .await
                    .expect("New iterator");
                print_iterator("newiterator", &itr);
            }
            "loaditerator" => {
                let itr = is::load_iterator(pool, atoi(parts[1]))
                    .await
                    .expect("Load iterator");
                print_iterator("loaditerator", &itr);
            }
            "update" => {
                let id = atoi(parts[1]);
                is::update_repo_iterator(
                    pool,
                    id,
                    atoi(parts[2]),
                    atoi(parts[3]),
                    i64::from(atoi(parts[4])),
                )
                .await
                .expect("updateRepoIterator");
                let itr = is::load_iterator(pool, id).await.expect("Load iterator");
                print_iterator("update", &itr);
            }
            "markcomplete" => {
                let now = parse_time(parts[1]);
                let id = atoi(parts[2]);
                is::mark_complete(pool, id, now)
                    .await
                    .expect("MarkComplete");
                let itr = is::load_iterator(pool, id).await.expect("Load iterator");
                print_iterator("markcomplete", &itr);
            }
            "restart" => {
                let id = atoi(parts[1]);
                is::restart_iterator(pool, id).await.expect("Restart");
                let itr = is::load_iterator(pool, id).await.expect("Load iterator");
                print_iterator("restart", &itr);
            }
            "inserterror" => {
                let id = atoi(parts[1]);
                let mut itr = is::load_iterator(pool, id).await.expect("Load iterator");
                is::insert_iteration_error(pool, id, &mut itr.errors, atoi(parts[2]), parts[3])
                    .await
                    .expect("insertIterationError");
                let itr = is::load_iterator(pool, id).await.expect("Load iterator");
                print_iterator("inserterror", &itr);
            }
            other => panic!("unknown op {other:?}"),
        }
        std::io::stdout().flush().expect("flush");
    }
}

fn fmt_opt_int(value: Option<i64>) -> String {
    value.map_or_else(|| "-".to_owned(), |value| value.to_string())
}

fn fmt_opt_i32(value: Option<i32>) -> String {
    value.map_or_else(|| "-".to_owned(), |value| value.to_string())
}

fn fmt_opt_strings(value: Option<Vec<String>>) -> String {
    value.map_or_else(|| "-".to_owned(), |messages| messages.join("|"))
}

async fn run_queue_corpus(pool: &PgPool, corpus_path: &str) {
    let corpus = std::fs::read_to_string(corpus_path).expect("read queue corpus");
    for line in corpus.lines() {
        if line.is_empty() {
            continue;
        }
        let parts: Vec<&str> = line.split('\t').collect();
        match parts[0] {
            "queueinfo" => {
                let mode = parts[1];
                let (a1, a2) = (parts[2], parts[3]);
                let mut args = is::BackfillQueueArgs::default();
                if parts[4] != "-" {
                    args.states = Some(parts[4].split(',').map(str::to_owned).collect());
                }
                if parts[5] != "-" {
                    args.text_search = Some(parts[5].to_owned());
                }
                if parts[6] != "-" {
                    args.id = Some(atoi(parts[6]));
                }
                match mode {
                    "default" => {}
                    "first" | "last" => {
                        let limit = atoi(a1);
                        let mut pagination = is::QueuePagination {
                            order_by: vec![is::QueueOrderBy {
                                field: "isb.id".to_owned(),
                                nulls: None,
                            }],
                            ..is::QueuePagination::default()
                        };
                        if mode == "first" {
                            pagination.first = Some(limit);
                        } else {
                            pagination.last = Some(limit);
                        }
                        if a2 != "-" {
                            let (key, value) = a2.split_once('=').expect("cursor");
                            let cursor = atoi(value);
                            if key == "after" {
                                pagination.after = vec![cursor];
                            } else {
                                pagination.before = vec![cursor];
                            }
                        }
                        args.pagination = Some(pagination);
                    }
                    "order" => {
                        args.pagination = Some(is::QueuePagination {
                            order_by: vec![is::QueueOrderBy {
                                field: a1.to_owned(),
                                nulls: None,
                            }],
                            ascending: a2 == "true",
                            ..is::QueuePagination::default()
                        });
                    }
                    other => panic!("unknown queueinfo mode {other:?}"),
                }
                let items = is::get_backfill_queue_info(pool, &args)
                    .await
                    .expect("GetBackfillQueueInfo");
                println!(
                    "qinfo mode={} a1={} a2={} states={} search={} id={} count={}",
                    mode,
                    a1,
                    a2,
                    parts[4],
                    parts[5],
                    parts[6],
                    items.len()
                );
                for (index, item) in items.iter().enumerate() {
                    println!(
                        "qitem {} id={} title={} sid={} uid={} label={} query={} state={} pct={} cost={} runtime={} created={} started={} completed={} pos={} creator={} errors={}",
                        index,
                        item.id,
                        item.insight_title,
                        item.series_id,
                        item.insight_unique_id,
                        item.series_label,
                        item.series_search_query,
                        item.backfill_state,
                        fmt_opt_int(item.percent_complete),
                        fmt_opt_int(item.backfill_cost),
                        fmt_opt_int(item.runtime_duration),
                        fmt_time(item.backfill_created_at),
                        fmt_time(item.backfill_started_at),
                        fmt_time(item.backfill_completed_at),
                        fmt_opt_int(item.queue_position),
                        fmt_opt_i32(item.creator_id),
                        fmt_opt_strings(item.errors.clone())
                    );
                }
            }
            "queuetotal" => {
                let mut args = is::BackfillQueueArgs::default();
                if parts[1] != "-" {
                    args.states = Some(parts[1].split(',').map(str::to_owned).collect());
                }
                if parts[2] != "-" {
                    args.text_search = Some(parts[2].to_owned());
                }
                if parts[3] != "-" {
                    args.id = Some(atoi(parts[3]));
                }
                let count = is::get_backfill_queue_total_count(pool, &args)
                    .await
                    .expect("GetBackfillQueueTotalCount");
                println!(
                    "qtotal states={} search={} id={} count={}",
                    parts[1], parts[2], parts[3], count
                );
            }
            other => panic!("unknown op {other:?}"),
        }
        std::io::stdout().flush().expect("flush");
    }
}

#[tokio::test]
async fn insights_scheduler_db_parity() {
    let Ok(dsn) = std::env::var("SG_QTEST_INSIGHTS_DSN") else {
        return;
    };
    if dsn.is_empty() {
        return;
    }
    let (Ok(backfill_corpus), Ok(iterator_corpus), Ok(queue_corpus)) = (
        std::env::var("SG_INSIGHTS_SCHEDULER_DB_BACKFILL_CORPUS"),
        std::env::var("SG_INSIGHTS_SCHEDULER_DB_ITERATOR_CORPUS"),
        std::env::var("SG_INSIGHTS_SCHEDULER_DB_QUEUE_CORPUS"),
    ) else {
        return;
    };
    let pool = PgPool::connect(&dsn).await.expect("connect database");
    run_backfill_corpus(&pool, &backfill_corpus).await;
    run_iterator_corpus(&pool, &iterator_corpus).await;
    run_queue_corpus(&pool, &queue_corpus).await;
}
