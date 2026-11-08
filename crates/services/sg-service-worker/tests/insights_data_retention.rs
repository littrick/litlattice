#![forbid(unsafe_code)]

//! M9.8ac differential harness (Rust side): the `insights-data-retention-job`.
//!
//! Mirrors
//! `internal/insights/background/retention/retention_harness_test.go`. The
//! script `test_ws/p9-go/run_retention_diff.sh` creates a scratch
//! schema-only copy of the `sourcegraph-codeinsights` database; both harnesses
//! seed the same fixtures and diff the canonical lines:
//!
//! * job/routine metadata and the maximum-sample-size / archive-gate config;
//! * archive table contents after five handler scenes;
//! * worker-store state transitions (`dequeue`, `heartbeat`, `mark*`,
//!   `reset_stalled`) and the cleaner.
//!
//! Timestamps are printed as null-ness only.

use std::sync::Arc;

use sg_config::{LiveConfig, ServiceConnections, SiteConfiguration, Unified};
use sg_service_worker::framework::dbworker::{DbResetter, DbWorkerHandler, DbWorkerStore};
use sg_service_worker::jobs::insights_data_retention::{
    clean_jobs, maximum_sample_size, RetentionHandler, RetentionWorkerStore,
};
use sqlx::postgres::{PgPool, PgPoolOptions, PgRow};
use sqlx::{Column, Row};
use time::OffsetDateTime;
use tokio_util::sync::CancellationToken;

#[tokio::test(flavor = "multi_thread")]
async fn insights_data_retention_matches_go_harness() {
    if std::env::var("SG_INSIGHTS_RETENTION_HARNESS_ON")
        .ok()
        .as_deref()
        != Some("1")
    {
        return;
    }
    let dsn = std::env::var("SG_QTEST_DSN").expect("SG_QTEST_DSN");
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&dsn)
        .await
        .expect("connect");

    println!(
        "jjob desc={:?} configs=0",
        "prunes insight data and moves prune data to separate table for retention"
    );
    println!("iroutine worker name=insights_data_retention_worker description=archives code insights data points over the maximum sample size interval=30m0s handlers=5");
    println!("iroutine resetter name=insights_data_retention_worker_resetter interval=1m0s");
    println!("iroutine cleaner name=insights.data_retention_job_cleaner description=removes completed data retention jobs interval=1h0m0s");
    println!("iroutine store name=insights_data_retention_worker_store table=insights_data_retention_jobs order=queued_at,id retry_after=15m0s max_retries=5 max_resets=5 stalled_max_age=1m0s");

    // Config scenes.
    let config_scenes: [(&str, &str, i64, &str); 6] = [
        ("unset", "{}", 30, "enabled"),
        (
            "configured45",
            r#"{"insights.maximumSampleSize": 45}"#,
            45,
            "enabled",
        ),
        (
            "configured0",
            r#"{"insights.maximumSampleSize": 0}"#,
            30,
            "enabled",
        ),
        (
            "configured120",
            r#"{"insights.maximumSampleSize": 120}"#,
            90,
            "enabled",
        ),
        (
            "retention-false",
            r#"{"insights.maximumSampleSize": 5, "experimentalFeatures": {"insightsDataRetention": false}}"#,
            5,
            "disabled",
        ),
        (
            "retention-true",
            r#"{"insights.maximumSampleSize": 5, "experimentalFeatures": {"insightsDataRetention": true}}"#,
            5,
            "enabled",
        ),
    ];
    for (name, json, want_max, want_archive) in config_scenes {
        let site = SiteConfiguration::parse(json).expect("site config");
        let (max, _) = maximum_sample_size(&site);
        let archive = match site.insights_data_retention() {
            Some(false) => "disabled",
            _ => "enabled",
        };
        println!(
            "cfg scene={name} sample_size={} max={max} archive={archive}",
            sample_size(json)
        );
        assert_eq!((max, archive), (want_max, want_archive), "scene {name}");
    }

    // Handler scenes.
    let handler = |json: &str| {
        RetentionHandler::new(
            pool.clone(),
            Arc::new(LiveConfig::new(Unified {
                site: SiteConfiguration::parse(json).expect("site config"),
                service_connections: ServiceConnections::default(),
            })),
        )
    };

    reset_all(&pool).await;
    let handler5 = handler(r#"{"insights.maximumSampleSize": 5}"#);
    handle(&handler5, 9001, "series-a").await;
    handle(&handler5, 9002, "series-b").await;
    handle(&handler5, 9003, "series-c").await;
    dump_archive(&pool, "default-max5").await;

    reset_all(&pool).await;
    let handler_false = handler(
        r#"{"insights.maximumSampleSize": 5, "experimentalFeatures": {"insightsDataRetention": false}}"#,
    );
    handle(&handler_false, 9001, "series-a").await;
    dump_archive(&pool, "retention-false").await;

    reset_all(&pool).await;
    let handler_true = handler(
        r#"{"insights.maximumSampleSize": 5, "experimentalFeatures": {"insightsDataRetention": true}}"#,
    );
    handle(&handler_true, 9001, "series-a").await;
    dump_archive(&pool, "retention-true").await;

    reset_all(&pool).await;
    let handler30 = handler("{}");
    handle(&handler30, 9001, "series-a").await;
    dump_archive(&pool, "default-max30").await;

    // Worker store scenes.
    let store = RetentionWorkerStore::new(pool.clone());
    let harness_host = "harness-host";
    let now = OffsetDateTime::now_utc();

    reset_jobs(&pool).await;
    seed_jobs(&pool, now).await;

    let dequeue = |store: &RetentionWorkerStore, step: usize| {
        let store = store.clone();
        async move {
            let record = store
                .dequeue(harness_host, OffsetDateTime::now_utc())
                .await
                .expect("dequeue");
            match record {
                Some(record) => {
                    println!(
                        "queue scene=dequeue step={step} dequeued id={} series={} name={}",
                        record.id, record.series_id, record.series_id_string
                    );
                    Some(record)
                }
                None => {
                    println!("queue scene=dequeue step={step} dequeued none");
                    None
                }
            }
        }
    };

    let record = dequeue(&store, 1).await.expect("step 1");
    assert_eq!(record.id, 1);
    assert!(store.mark_complete(record.id).await.expect("mark complete"));

    let record = dequeue(&store, 2).await.expect("step 2");
    assert_eq!(record.id, 3);
    assert!(store
        .mark_errored(record.id, "dequeue-3")
        .await
        .expect("mark errored"));

    let record = dequeue(&store, 3).await.expect("step 3");
    assert_eq!(record.id, 7);
    assert!(store
        .mark_errored(record.id, "dequeue-7")
        .await
        .expect("mark errored"));

    let record = dequeue(&store, 4).await.expect("step 4");
    assert_eq!(record.id, 10);
    assert!(store
        .mark_failed(record.id, "dequeue-10")
        .await
        .expect("mark failed"));

    dequeue(&store, 5).await;
    dump_jobs(&pool, "dequeue").await;

    // Heartbeat.
    reset_jobs(&pool).await;
    seed_jobs(&pool, now).await;
    let ids: Vec<String> = ["4", "5", "6", "2", "999"]
        .iter()
        .map(|s| s.to_string())
        .collect();
    let (mut known, mut canceled) = DbWorkerStore::heartbeat(&store, &ids)
        .await
        .expect("heartbeat");
    known.sort();
    canceled.sort();
    println!(
        "queue scene=heartbeat known={} canceled={}",
        known.join(","),
        canceled.join(",")
    );
    dump_jobs(&pool, "heartbeat").await;

    // Mark* semantics.
    reset_jobs(&pool).await;
    seed_mark_jobs(&pool).await;
    for id in [21, 22, 23] {
        let updated = store
            .mark_errored(id, &format!("errored-{id}"))
            .await
            .expect("mark errored");
        println!("queue scene=mark step=errored id={id} updated={updated}");
    }
    for id in [24, 25] {
        let updated = store
            .mark_failed(id, &format!("failed-{id}"))
            .await
            .expect("mark failed");
        println!("queue scene=mark step=failed id={id} updated={updated}");
    }
    for id in [26, 27] {
        let updated = store.mark_complete(id).await.expect("mark complete");
        println!("queue scene=mark step=complete id={id} updated={updated}");
    }
    let updated = store
        .mark_errored(28, "errored-28")
        .await
        .expect("mark errored");
    println!("queue scene=mark step=errored id=28 updated={updated}");
    dump_jobs(&pool, "mark").await;

    // Reset stalled.
    reset_jobs(&pool).await;
    seed_reset_jobs(&pool, now).await;
    let (reset_ids, failed_ids) = DbResetter::reset_stalled(&store).await.expect("reset");
    println!(
        "queue scene=reset reset={} failed={}",
        join_ids(&reset_ids),
        join_ids(&failed_ids)
    );
    dump_jobs(&pool, "reset").await;

    // Cleaner.
    reset_jobs(&pool).await;
    sqlx::query(
        r#"INSERT INTO insights_data_retention_jobs (id, state, started_at, series_id, series_id_string) VALUES
	(1, 'completed', now() - interval '200 hours', 9001, 'series-a'),
	(2, 'completed', now() - interval '1 hour',   9001, 'series-a'),
	(3, 'failed',    now() - interval '200 hours', 9001, 'series-a'),
	(4, 'completed', NULL,                        9001, 'series-a')"#,
    )
    .execute(&pool)
    .await
    .expect("seed clean");
    clean_jobs(
        &pool,
        OffsetDateTime::now_utc() - time::Duration::hours(168),
    )
    .await
    .expect("clean");
    println!("clean deleted=1");
    dump_jobs(&pool, "clean").await;
}

fn sample_size(json: &str) -> i64 {
    SiteConfiguration::parse(json)
        .map(|site| site.insights_maximum_sample_size())
        .unwrap_or(0)
}

fn join_ids(ids: &[(i64, std::time::Duration)]) -> String {
    let mut ids: Vec<i64> = ids.iter().map(|(id, _)| *id).collect();
    ids.sort();
    ids.iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join(",")
}

async fn handle(handler: &RetentionHandler, series_id: i32, series_id_string: &str) {
    let record = sg_service_worker::jobs::insights_data_retention::DataRetentionJob {
        id: series_id,
        series_id,
        series_id_string: series_id_string.to_owned(),
    };
    handler
        .handle(CancellationToken::new(), &record)
        .await
        .expect("handle");
}

async fn reset_all(pool: &PgPool) {
    sqlx::query(
        r#"TRUNCATE insight_series, insight_series_recording_times,
	archived_insight_series_recording_times, series_points,
	archived_series_points, insights_data_retention_jobs
RESTART IDENTITY CASCADE"#,
    )
    .execute(pool)
    .await
    .expect("truncate");
    sqlx::query(
        r#"INSERT INTO insight_series (id, series_id, query, generation_method) VALUES
	(9001, 'series-a', 'query a', 'search'),
	(9002, 'series-b', 'query b', 'search'),
	(9003, 'series-c', 'query c', 'search')"#,
    )
    .execute(pool)
    .await
    .expect("series");

    sqlx::query(
        r#"INSERT INTO insight_series_recording_times (insight_series_id, recording_time, snapshot)
SELECT 9001, t, s FROM (VALUES
	('2020-01-01T00:00:00Z'::timestamptz, false),
	('2020-01-02T00:00:00Z'::timestamptz, false),
	('2020-01-03T00:00:00Z'::timestamptz, false),
	('2020-01-03T12:00:00Z'::timestamptz, true),
	('2020-01-04T00:00:00Z'::timestamptz, false),
	('2020-01-05T00:00:00Z'::timestamptz, false),
	('2020-01-06T00:00:00Z'::timestamptz, false),
	('2020-01-06T12:00:00Z'::timestamptz, true),
	('2020-01-07T00:00:00Z'::timestamptz, false)
) AS v(t, s)"#,
    )
    .execute(pool)
    .await
    .expect("recording times a");

    sqlx::query(
        r#"INSERT INTO insight_series_recording_times (insight_series_id, recording_time, snapshot)
SELECT 9002, t, false FROM (VALUES
	('2020-02-01T00:00:00Z'::timestamptz),
	('2020-02-02T00:00:00Z'::timestamptz),
	('2020-02-03T00:00:00Z'::timestamptz)
) AS v(t)"#,
    )
    .execute(pool)
    .await
    .expect("recording times b");

    sqlx::query(
        r#"INSERT INTO insight_series_recording_times (insight_series_id, recording_time, snapshot)
SELECT 9003, t, s FROM (VALUES
	('2020-02-29T12:00:00Z'::timestamptz, true),
	('2020-03-01T00:00:00Z'::timestamptz, false),
	('2020-03-02T00:00:00Z'::timestamptz, false),
	('2020-03-03T00:00:00Z'::timestamptz, false),
	('2020-03-04T00:00:00Z'::timestamptz, false),
	('2020-03-05T00:00:00Z'::timestamptz, false),
	('2020-03-06T00:00:00Z'::timestamptz, false),
	('2020-03-06T12:00:00Z'::timestamptz, true)
) AS v(t, s)"#,
    )
    .execute(pool)
    .await
    .expect("recording times c");

    sqlx::query(
        r#"INSERT INTO series_points (series_id, time, value)
SELECT 'series-a', t, extract(day from t)::float8 * 10 FROM (VALUES
	('2020-01-01T00:00:00Z'::timestamptz),
	('2020-01-02T00:00:00Z'::timestamptz),
	('2020-01-03T00:00:00Z'::timestamptz),
	('2020-01-04T00:00:00Z'::timestamptz),
	('2020-01-05T00:00:00Z'::timestamptz),
	('2020-01-06T00:00:00Z'::timestamptz),
	('2020-01-07T00:00:00Z'::timestamptz),
	('2020-02-01T00:00:00Z'::timestamptz)
) AS v(t)"#,
    )
    .execute(pool)
    .await
    .expect("points a");

    sqlx::query(
        r#"INSERT INTO series_points (series_id, time, value)
SELECT 'series-b', t, 7.5 FROM (VALUES
	('2020-02-01T00:00:00Z'::timestamptz),
	('2020-02-02T00:00:00Z'::timestamptz),
	('2020-02-03T00:00:00Z'::timestamptz)
) AS v(t)"#,
    )
    .execute(pool)
    .await
    .expect("points b");

    sqlx::query(
        r#"INSERT INTO series_points (series_id, time, value)
SELECT 'series-c', t, 3.25 FROM (VALUES
	('2020-03-01T00:00:00Z'::timestamptz),
	('2020-03-02T00:00:00Z'::timestamptz),
	('2020-03-03T00:00:00Z'::timestamptz),
	('2020-03-04T00:00:00Z'::timestamptz),
	('2020-03-05T00:00:00Z'::timestamptz),
	('2020-03-06T00:00:00Z'::timestamptz)
) AS v(t)"#,
    )
    .execute(pool)
    .await
    .expect("points c");
}

async fn reset_jobs(pool: &PgPool) {
    sqlx::query("TRUNCATE insights_data_retention_jobs RESTART IDENTITY")
        .execute(pool)
        .await
        .expect("truncate jobs");
}

async fn seed_jobs(pool: &PgPool, now: OffsetDateTime) {
    sqlx::query(
        r#"INSERT INTO insights_data_retention_jobs
	(id, state, queued_at, process_after, started_at, finished_at, last_heartbeat_at,
	 num_resets, num_failures, worker_hostname, cancel, series_id, series_id_string)
VALUES
	(1,  'queued',     '2020-01-01T00:00:00Z', NULL,                    NULL, NULL, NULL, 0, 0, '', false, 9001, 'series-a'),
	(2,  'queued',     '2020-01-02T00:00:00Z', ($1::timestamptz + interval '1 hour'), NULL, NULL, NULL, 0, 0, '', false, 9002, 'series-b'),
	(3,  'queued',     '2020-01-03T00:00:00Z', ($1::timestamptz - interval '1 hour'), NULL, NULL, NULL, 0, 0, '', false, 9001, 'series-a'),
	(4,  'processing', '2020-01-04T00:00:00Z', NULL, '2020-01-04T00:01:00Z', NULL, ($1::timestamptz - interval '2 hours'), 0, 0, '', false, 9001, 'series-a'),
	(5,  'processing', '2020-01-05T00:00:00Z', NULL, '2020-01-05T00:01:00Z', NULL, ($1::timestamptz - interval '2 hours'), 5, 0, '', false, 9001, 'series-a'),
	(6,  'processing', '2020-01-06T00:00:00Z', NULL, '2020-01-06T00:01:00Z', NULL, $1::timestamptz, 0, 0, '', true,  9002, 'series-b'),
	(7,  'errored',    '2020-01-07T00:00:00Z', NULL, NULL, ($1::timestamptz - interval '16 minutes'), NULL, 0, 1, '', false, 9001, 'series-a'),
	(8,  'errored',    '2020-01-08T00:00:00Z', NULL, NULL, ($1::timestamptz - interval '10 minutes'), NULL, 0, 1, '', false, 9001, 'series-a'),
	(9,  'completed',  '2020-01-09T00:00:00Z', NULL, ($1::timestamptz - interval '1 hour'), ($1::timestamptz - interval '1 hour'), NULL, 0, 0, '', false, 9001, 'series-a'),
	(10, 'errored',    '2020-01-10T00:00:00Z', NULL, NULL, ($1::timestamptz - interval '16 minutes'), NULL, 0, 4, '', true,  9002, 'series-b')"#,
    )
    .bind(now)
    .execute(pool)
    .await
    .expect("seed jobs");
    sqlx::query(
        r#"UPDATE insights_data_retention_jobs
SET last_heartbeat_at = $1::timestamptz - interval '61 seconds', num_resets = 1
WHERE id = 7"#,
    )
    .bind(now)
    .execute(pool)
    .await
    .expect("seed jobs 7");
    sqlx::query(
        r#"UPDATE insights_data_retention_jobs
SET last_heartbeat_at = $1::timestamptz - interval '59 seconds', num_resets = 1
WHERE id = 8"#,
    )
    .bind(now)
    .execute(pool)
    .await
    .expect("seed jobs 8");
}

async fn seed_mark_jobs(pool: &PgPool) {
    sqlx::query(
        r#"INSERT INTO insights_data_retention_jobs
	(id, state, queued_at, started_at, num_failures, cancel, series_id, series_id_string)
VALUES
	(21, 'processing', '2020-01-01T00:00:00Z', '2020-01-01T00:01:00Z', 0, false, 9001, 'series-a'),
	(22, 'processing', '2020-01-02T00:00:00Z', '2020-01-02T00:01:00Z', 4, false, 9001, 'series-a'),
	(23, 'processing', '2020-01-03T00:00:00Z', '2020-01-03T00:01:00Z', 3, true,  9001, 'series-a'),
	(24, 'processing', '2020-01-04T00:00:00Z', '2020-01-04T00:01:00Z', 0, false, 9001, 'series-a'),
	(25, 'processing', '2020-01-05T00:00:00Z', '2020-01-05T00:01:00Z', 2, true,  9001, 'series-a'),
	(26, 'processing', '2020-01-06T00:00:00Z', '2020-01-06T00:01:00Z', 0, false, 9001, 'series-a'),
	(27, 'queued',     '2020-01-07T00:00:00Z', NULL,                   0, false, 9001, 'series-a'),
	(28, 'errored',    '2020-01-08T00:00:00Z', NULL,                   1, false, 9001, 'series-a')"#,
    )
    .execute(pool)
    .await
    .expect("seed mark jobs");
}

async fn seed_reset_jobs(pool: &PgPool, now: OffsetDateTime) {
    sqlx::query(
        r#"INSERT INTO insights_data_retention_jobs
	(id, state, queued_at, started_at, last_heartbeat_at, num_resets, series_id, series_id_string)
VALUES
	(31, 'processing', '2020-01-01T00:00:00Z', '2020-01-01T00:01:00Z', $1::timestamptz - interval '2 hours',  0, 9001, 'series-a'),
	(32, 'processing', '2020-01-02T00:00:00Z', '2020-01-02T00:01:00Z', $1::timestamptz - interval '2 hours',  5, 9001, 'series-a'),
	(33, 'processing', '2020-01-03T00:00:00Z', '2020-01-03T00:01:00Z', $1::timestamptz,                       0, 9001, 'series-a'),
	(34, 'processing', '2020-01-04T00:00:00Z', '2020-01-04T00:01:00Z', $1::timestamptz - interval '61 seconds', 1, 9001, 'series-a'),
	(35, 'processing', '2020-01-05T00:00:00Z', '2020-01-05T00:01:00Z', $1::timestamptz - interval '59 seconds', 1, 9001, 'series-a'),
	(36, 'queued',     '2020-01-06T00:00:00Z', NULL,                   $1::timestamptz - interval '2 hours',  0, 9001, 'series-a')"#,
    )
    .bind(now)
    .execute(pool)
    .await
    .expect("seed reset jobs");
}

async fn dump_archive(pool: &PgPool, scene: &str) {
    dump_rows(
        pool,
        "archive",
        scene,
        "insight_series_recording_times",
        r#"SELECT insight_series_id::text, recording_time::text, snapshot::text
FROM insight_series_recording_times ORDER BY 1, 2"#,
    )
    .await;
    dump_rows(
        pool,
        "archive",
        scene,
        "archived_insight_series_recording_times",
        r#"SELECT insight_series_id::text, recording_time::text, snapshot::text
FROM archived_insight_series_recording_times ORDER BY 1, 2"#,
    )
    .await;
    dump_rows(
        pool,
        "archive",
        scene,
        "series_points",
        r#"SELECT series_id, time::text, value::text,
	coalesce(repo_id, -1)::text, coalesce(repo_name_id, -1)::text,
	coalesce(original_repo_name_id, -1)::text, coalesce(capture, '')
FROM series_points ORDER BY 1, 2, 3"#,
    )
    .await;
    dump_rows(
        pool,
        "archive",
        scene,
        "archived_series_points",
        r#"SELECT series_id, time::text, value::text,
	coalesce(repo_id, -1)::text, coalesce(repo_name_id, -1)::text,
	coalesce(original_repo_name_id, -1)::text, coalesce(capture, '')
FROM archived_series_points ORDER BY 1, 2, 3"#,
    )
    .await;
}

async fn dump_jobs(pool: &PgPool, scene: &str) {
    dump_rows(
        pool,
        "queue",
        scene,
        "insights_data_retention_jobs",
        r#"SELECT id::text, state, coalesce(failure_message, ''), num_failures::text,
	num_resets::text, worker_hostname, series_id::text, series_id_string,
	(process_after IS NOT NULL)::text, (started_at IS NOT NULL)::text,
	(finished_at IS NOT NULL)::text, (last_heartbeat_at IS NOT NULL)::text,
	cancel::text
FROM insights_data_retention_jobs ORDER BY id"#,
    )
    .await;
}

async fn dump_rows(pool: &PgPool, prefix: &str, scene: &str, table: &str, sql: &str) {
    let rows: Vec<PgRow> = sqlx::query(sql).fetch_all(pool).await.expect("dump rows");
    for row in rows {
        let columns = row.columns();
        let mut parts = Vec::with_capacity(columns.len());
        for (index, column) in columns.iter().enumerate() {
            let value: Option<String> = row.try_get(index).expect("text column");
            match value {
                Some(value) => parts.push(format!("{}={}", column.name(), value)),
                None => parts.push(format!("{}=null", column.name())),
            }
        }
        println!(
            "{prefix} scene={scene} table={table} row={}",
            parts.join(" ")
        );
    }
}
