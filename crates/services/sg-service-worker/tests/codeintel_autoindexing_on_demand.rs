#![forbid(unsafe_code)]

//! M9.8cy differential harness (Rust side): the on-demand half of the
//! `codeintel-autoindexing-scheduler` job (`handle_on_demand_scheduler`).
//!
//! Mirrors
//! `internal/codeintel/autoindexing/internal/background/scheduler/scheduler_on_demand_harness_test.go`.
//! `test_ws/p9-go/run_codeintel_autoindexing_on_demand_diff.sh` resets and
//! seeds the same `codeintel_autoindex_queue` rows immediately before each
//! side, then diffs the canonical `caod ` lines: for each case, the per-run
//! errors, every seeded row's processed flag, the enqueue calls in invocation
//! order (queue `queued_at` order) and the call counts per run.
//!
//! The enqueuer is a fake; the site configuration is parsed per case so the
//! `codeIntelAutoIndexing.enabled` gate is the production accessor.

use std::sync::Mutex;

use async_trait::async_trait;
use sg_config::SiteConfiguration;
use sg_service_worker::jobs::codeintel_autoindexing_scheduler::{
    handle_on_demand_scheduler, IndexEnqueuer, QueueAutoIndexJobsError,
};
use sqlx::postgres::PgPool;
use sqlx::Row;

#[derive(Clone, serde::Serialize)]
struct Call {
    repo: i32,
    rev: String,
}

struct FakeEnqueuer {
    failure: Option<(i32, String)>,
    kind: Failure,
    calls: Mutex<Vec<Call>>,
}

#[derive(Clone, Copy)]
enum Failure {
    Error,
    RevisionNotFound,
}

impl FakeEnqueuer {
    fn new(failure: Option<(i32, &str, Failure)>) -> Self {
        Self {
            failure: failure.map(|(repo, rev, _)| (repo, rev.to_owned())),
            kind: failure.map(|(_, _, kind)| kind).unwrap_or(Failure::Error),
            calls: Mutex::new(Vec::new()),
        }
    }
}

#[async_trait]
impl IndexEnqueuer for FakeEnqueuer {
    async fn queue_auto_index_jobs(
        &self,
        repository_id: i32,
        rev: &str,
        _configuration: &str,
        _force: bool,
        _bypass_limit: bool,
    ) -> Result<(), QueueAutoIndexJobsError> {
        self.calls
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(Call {
                repo: repository_id,
                rev: rev.to_owned(),
            });
        match &self.failure {
            Some((repo, failed_rev)) if *repo == repository_id && failed_rev == rev => {
                Err(match self.kind {
                    Failure::Error => QueueAutoIndexJobsError::Other("boom".to_owned()),
                    Failure::RevisionNotFound => QueueAutoIndexJobsError::RevisionNotFound,
                })
            }
            _ => Ok(()),
        }
    }
}

#[derive(serde::Serialize)]
struct QueueRow {
    id: i32,
    repo: i32,
    rev: String,
    processed: bool,
}

#[derive(serde::Serialize)]
struct Snapshot {
    case: &'static str,
    errors: Vec<String>,
    rows: Vec<QueueRow>,
    calls: Vec<Call>,
    call_count_per_run: Vec<usize>,
}

struct SeedRow {
    id: i32,
    repo: i32,
    rev: &'static str,
    queued_at: &'static str,
    processed: bool,
}

struct Case {
    name: &'static str,
    enabled: bool,
    batch_size: i64,
    runs: usize,
    seed: &'static [SeedRow],
    failure: Option<(i32, &'static str, Failure)>,
}

const fn row(id: i32, repo: i32, queued_at: &'static str, processed: bool) -> SeedRow {
    SeedRow {
        id,
        repo,
        rev: "HEAD",
        queued_at,
        processed,
    }
}

const DISABLED: &[SeedRow] = &[
    row(1, 9401, "2024-01-01T00:00:01Z", false),
    row(2, 9402, "2024-01-01T00:00:02Z", false),
];
const ALL: &[SeedRow] = &[
    row(11, 9401, "2024-01-01T00:00:01Z", false),
    SeedRow {
        id: 12,
        repo: 9402,
        rev: "v1.0.0",
        queued_at: "2024-01-01T00:00:02Z",
        processed: false,
    },
    SeedRow {
        id: 13,
        repo: 9403,
        rev: "feature",
        queued_at: "2024-01-01T00:00:03Z",
        processed: false,
    },
];
const BATCH_TWO: &[SeedRow] = &[
    row(21, 9401, "2024-01-01T00:00:01Z", false),
    row(22, 9402, "2024-01-01T00:00:02Z", false),
    row(23, 9403, "2024-01-01T00:00:03Z", false),
    row(24, 9404, "2024-01-01T00:00:04Z", false),
];
const ONLY_UNPROCESSED: &[SeedRow] = &[
    row(31, 9401, "2024-01-01T00:00:01Z", false),
    row(32, 9402, "2024-01-01T00:00:02Z", true),
    row(33, 9403, "2024-01-01T00:00:03Z", false),
];
const ENQUEUER_ERROR: &[SeedRow] = &[
    row(41, 9401, "2024-01-01T00:00:01Z", false),
    row(42, 9402, "2024-01-01T00:00:02Z", false),
    row(43, 9403, "2024-01-01T00:00:03Z", false),
];
const REVISION_NOT_FOUND: &[SeedRow] = &[
    row(51, 9401, "2024-01-01T00:00:01Z", false),
    row(52, 9402, "2024-01-01T00:00:02Z", false),
];
const TWICE_BATCH_ONE: &[SeedRow] = &[
    row(61, 9401, "2024-01-01T00:00:01Z", false),
    row(62, 9402, "2024-01-01T00:00:02Z", false),
];
const ZERO_BATCH: &[SeedRow] = &[
    row(71, 9401, "2024-01-01T00:00:01Z", false),
    row(72, 9402, "2024-01-01T00:00:02Z", false),
];

fn cases() -> Vec<Case> {
    vec![
        Case {
            name: "disabled",
            enabled: false,
            batch_size: 100,
            runs: 1,
            seed: DISABLED,
            failure: None,
        },
        Case {
            name: "empty",
            enabled: true,
            batch_size: 100,
            runs: 1,
            seed: &[],
            failure: None,
        },
        Case {
            name: "all",
            enabled: true,
            batch_size: 10,
            runs: 1,
            seed: ALL,
            failure: None,
        },
        Case {
            name: "batch-two",
            enabled: true,
            batch_size: 2,
            runs: 1,
            seed: BATCH_TWO,
            failure: None,
        },
        Case {
            name: "only-unprocessed",
            enabled: true,
            batch_size: 10,
            runs: 1,
            seed: ONLY_UNPROCESSED,
            failure: None,
        },
        Case {
            name: "enqueuer-error",
            enabled: true,
            batch_size: 10,
            runs: 1,
            seed: ENQUEUER_ERROR,
            failure: Some((9401, "HEAD", Failure::Error)),
        },
        Case {
            name: "revision-not-found",
            enabled: true,
            batch_size: 10,
            runs: 1,
            seed: REVISION_NOT_FOUND,
            failure: Some((9401, "HEAD", Failure::RevisionNotFound)),
        },
        Case {
            name: "twice-batch-one",
            enabled: true,
            batch_size: 1,
            runs: 2,
            seed: TWICE_BATCH_ONE,
            failure: None,
        },
        Case {
            name: "zero-batch",
            enabled: true,
            batch_size: 0,
            runs: 1,
            seed: ZERO_BATCH,
            failure: None,
        },
    ]
}

async fn reset_queue(pool: &PgPool, rows: &[SeedRow]) {
    sqlx::query("DELETE FROM codeintel_autoindex_queue WHERE repository_id BETWEEN 9400 AND 9499")
        .execute(pool)
        .await
        .expect("reset queue");
    for row in rows {
        let processed_at: Option<&str> = row.processed.then_some("2024-06-01T00:00:00Z");
        sqlx::query(
            "INSERT INTO codeintel_autoindex_queue (id, repository_id, rev, queued_at, processed_at) \
             VALUES ($1, $2, $3, $4::timestamptz, $5::timestamptz)",
        )
        .bind(row.id)
        .bind(row.repo)
        .bind(row.rev)
        .bind(row.queued_at)
        .bind(processed_at)
        .execute(pool)
        .await
        .unwrap_or_else(|err| panic!("seed queue row {}: {err}", row.id));
    }
}

async fn current_rows(pool: &PgPool) -> Vec<QueueRow> {
    sqlx::query(
        "SELECT id, repository_id, rev, processed_at IS NOT NULL AS processed \
         FROM codeintel_autoindex_queue \
         WHERE repository_id BETWEEN 9400 AND 9499 ORDER BY id",
    )
    .fetch_all(pool)
    .await
    .expect("query queue")
    .iter()
    .map(|row| QueueRow {
        id: row.get("id"),
        repo: row.get("repository_id"),
        rev: row.get("rev"),
        processed: row.get("processed"),
    })
    .collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn autoindexing_on_demand_matches_go_harness() {
    if std::env::var("SG_AUTOINDEXING_ON_DEMAND_DIFF")
        .ok()
        .as_deref()
        != Some("1")
    {
        return;
    }
    let dsn = std::env::var("SG_AUTOINDEXING_ON_DEMAND_TEST_DSN")
        .expect("SG_AUTOINDEXING_ON_DEMAND_TEST_DSN");
    let pool = sg_db::connect(&dsn, 10).await.expect("frontend pool");

    for case in cases() {
        let site = SiteConfiguration::parse(&format!(
            r#"{{"codeIntelAutoIndexing.enabled": {}}}"#,
            case.enabled
        ))
        .expect("site config");
        let enqueuer = FakeEnqueuer::new(case.failure);

        reset_queue(&pool, case.seed).await;

        let mut errors = Vec::with_capacity(case.runs);
        let mut counts = Vec::with_capacity(case.runs);
        let mut previous = 0;
        for _ in 0..case.runs {
            let result = handle_on_demand_scheduler(&pool, &site, &enqueuer, case.batch_size).await;
            errors.push(result.err().unwrap_or_else(|| "-".to_owned()));
            let count = enqueuer
                .calls
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .len();
            counts.push(count - previous);
            previous = count;
        }

        let calls = enqueuer
            .calls
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        let snapshot = Snapshot {
            case: case.name,
            errors,
            rows: current_rows(&pool).await,
            calls,
            call_count_per_run: counts,
        };
        println!("caod {}", serde_json::to_string(&snapshot).expect("json"));
    }
}
