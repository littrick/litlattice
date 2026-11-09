#![forbid(unsafe_code)]

//! M9.8cz differential harness (Rust side): `sg-db`'s port of
//! `internal/codeintel/autoindexing/internal/store/enqueuer.go` (`IsQueued`,
//! `IsQueuedRootIndexer`, `InsertJobs`).
//!
//! Mirrors
//! `internal/codeintel/autoindexing/internal/store/enqueuer_harness_test.go`.
//! `test_ws/p9-go/run_codeintel_autoindexing_enqueuer_diff.sh` applies the
//! shared fixtures (`codeintel_autoindexing_enqueuer_fixtures.sql`) to a
//! scratch frontend database immediately before each side, then diffs the
//! canonical `caen ` lines: the read query's boolean, or the inserted jobs
//! (with the volatile `queued_at` reduced to a presence flag).

use sg_db::codeintel_autoindexing_enqueuer::{
    insert_jobs, is_queued, is_queued_root_indexer, AutoIndexJob, DockerStep,
};

const C0: &str = "0000000000000000000000000000000000000000";
const C1: &str = "1111111111111111111111111111111111111111";
const C2: &str = "2222222222222222222222222222222222222222";
const C3: &str = "3333333333333333333333333333333333333333";
const C4: &str = "4444444444444444444444444444444444444444";
const C5: &str = "5555555555555555555555555555555555555555";
const C6: &str = "6666666666666666666666666666666666666666";
const C7: &str = "7777777777777777777777777777777777777777";
const C8: &str = "8888888888888888888888888888888888888888";
const C9: &str = "9999999999999999999999999999999999999999";
const CA: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

#[derive(serde::Serialize)]
struct Job {
    id: i64,
    commit: String,
    queued_at_present: bool,
    state: String,
    failure_message: Option<String>,
    started_at_present: bool,
    finished_at_present: bool,
    process_after_present: bool,
    num_resets: i32,
    num_failures: i32,
    repository_id: i32,
    repository_name: String,
    docker_steps: Vec<DockerStep>,
    root: String,
    indexer: String,
    indexer_args: Vec<String>,
    outfile: String,
    execution_logs: Option<Vec<serde_json::Value>>,
    rank: Option<i64>,
    local_steps: Vec<String>,
    associated_upload_id: Option<i64>,
    should_reindex: bool,
    requested_env_vars: Option<Vec<String>>,
    enqueuer_user_id: i32,
}

impl From<AutoIndexJob> for Job {
    fn from(job: AutoIndexJob) -> Self {
        Self {
            id: job.id,
            commit: job.commit,
            queued_at_present: true,
            state: job.state,
            failure_message: job.failure_message,
            started_at_present: job.started_at.is_some(),
            finished_at_present: job.finished_at.is_some(),
            process_after_present: job.process_after.is_some(),
            num_resets: job.num_resets,
            num_failures: job.num_failures,
            repository_id: job.repository_id,
            repository_name: job.repository_name,
            docker_steps: job.docker_steps,
            root: job.root,
            indexer: job.indexer,
            indexer_args: job.indexer_args,
            outfile: job.outfile,
            execution_logs: job.execution_logs,
            rank: job.rank,
            local_steps: job.local_steps,
            associated_upload_id: job.associated_upload_id,
            should_reindex: job.should_reindex,
            requested_env_vars: job.requested_env_vars,
            enqueuer_user_id: job.enqueuer_user_id,
        }
    }
}

#[derive(serde::Serialize)]
struct Snapshot {
    case: &'static str,
    queued: Option<bool>,
    root_indexer: Option<bool>,
    jobs: Option<Vec<Job>>,
    error: String,
}

enum Kind {
    IsQueued,
    RootIndexer,
    Insert,
}

struct Case {
    name: &'static str,
    kind: Kind,
    repo: i32,
    commit: &'static str,
    root: &'static str,
    indexer: &'static str,
    jobs: Vec<AutoIndexJob>,
}

fn job(state: &str, commit: &str, repository_id: i32) -> AutoIndexJob {
    AutoIndexJob {
        state: state.to_owned(),
        commit: commit.to_owned(),
        repository_id,
        ..Default::default()
    }
}

fn cases() -> Vec<Case> {
    vec![
        Case {
            name: "is-queued-empty",
            kind: Kind::IsQueued,
            repo: 9501,
            commit: C0,
            root: "",
            indexer: "",
            jobs: Vec::new(),
        },
        Case {
            name: "is-queued-manual-upload",
            kind: Kind::IsQueued,
            repo: 9501,
            commit: C1,
            root: "",
            indexer: "",
            jobs: Vec::new(),
        },
        Case {
            name: "is-queued-associated-upload",
            kind: Kind::IsQueued,
            repo: 9501,
            commit: C2,
            root: "",
            indexer: "",
            jobs: Vec::new(),
        },
        Case {
            name: "is-queued-upload-reindex",
            kind: Kind::IsQueued,
            repo: 9501,
            commit: C3,
            root: "",
            indexer: "",
            jobs: Vec::new(),
        },
        Case {
            name: "is-queued-upload-deleted",
            kind: Kind::IsQueued,
            repo: 9501,
            commit: C4,
            root: "",
            indexer: "",
            jobs: Vec::new(),
        },
        Case {
            name: "is-queued-index",
            kind: Kind::IsQueued,
            repo: 9501,
            commit: C5,
            root: "",
            indexer: "",
            jobs: Vec::new(),
        },
        Case {
            name: "is-queued-index-one-reindex",
            kind: Kind::IsQueued,
            repo: 9501,
            commit: C6,
            root: "",
            indexer: "",
            jobs: Vec::new(),
        },
        Case {
            name: "is-queued-index-two-roots",
            kind: Kind::IsQueued,
            repo: 9501,
            commit: C7,
            root: "",
            indexer: "",
            jobs: Vec::new(),
        },
        Case {
            name: "root-indexer-empty",
            kind: Kind::RootIndexer,
            repo: 9501,
            commit: C8,
            root: "",
            indexer: "lsif-go",
            jobs: Vec::new(),
        },
        Case {
            name: "root-indexer-match",
            kind: Kind::RootIndexer,
            repo: 9501,
            commit: C9,
            root: "",
            indexer: "lsif-go",
            jobs: Vec::new(),
        },
        Case {
            name: "root-indexer-latest-reindex",
            kind: Kind::RootIndexer,
            repo: 9501,
            commit: CA,
            root: "sub",
            indexer: "scip-go",
            jobs: Vec::new(),
        },
        Case {
            name: "insert-empty",
            kind: Kind::Insert,
            repo: 0,
            commit: "",
            root: "",
            indexer: "",
            jobs: Vec::new(),
        },
        Case {
            name: "insert-two",
            kind: Kind::Insert,
            repo: 0,
            commit: "",
            root: "",
            indexer: "",
            jobs: vec![
                AutoIndexJob {
                    docker_steps: vec![DockerStep {
                        root: String::new(),
                        image: "alpine".to_owned(),
                        commands: vec!["echo".to_owned(), "hi".to_owned()],
                    }],
                    local_steps: vec!["echo local".to_owned()],
                    indexer_args: vec!["--no-progress".to_owned()],
                    outfile: "dump.lsif".to_owned(),
                    indexer: "lsif-go".to_owned(),
                    ..job("queued", C5, 9501)
                },
                AutoIndexJob {
                    root: "sub".to_owned(),
                    indexer: "scip-go".to_owned(),
                    outfile: "index.scip".to_owned(),
                    execution_logs: Some(vec![serde_json::json!({
                        "key": "build",
                        "command": ["cmd"],
                        "startTime": "2020-01-01T00:00:00Z",
                    })]),
                    requested_env_vars: Some(vec!["A".to_owned(), "B".to_owned()]),
                    ..job("queued", C6, 9502)
                },
            ],
        },
        Case {
            name: "insert-null-lists",
            kind: Kind::Insert,
            repo: 0,
            commit: "",
            root: "",
            indexer: "",
            jobs: vec![AutoIndexJob {
                indexer: "lsif-go".to_owned(),
                outfile: "dump.lsif".to_owned(),
                ..job("queued", C7, 9502)
            }],
        },
    ]
}

#[tokio::test(flavor = "multi_thread")]
async fn autoindexing_enqueuer_matches_go_harness() {
    if std::env::var("SG_AUTOINDEXING_ENQUEUER_DIFF")
        .ok()
        .as_deref()
        != Some("1")
    {
        return;
    }
    let dsn = std::env::var("SG_AUTOINDEXING_ENQUEUER_TEST_DSN")
        .expect("SG_AUTOINDEXING_ENQUEUER_TEST_DSN");
    let pool = sg_db::connect(&dsn, 10).await.expect("frontend pool");

    for case in cases() {
        let mut snapshot = Snapshot {
            case: case.name,
            queued: None,
            root_indexer: None,
            jobs: None,
            error: "-".to_owned(),
        };
        match case.kind {
            Kind::IsQueued => match is_queued(&pool, case.repo, case.commit).await {
                Ok(value) => snapshot.queued = Some(value),
                Err(err) => snapshot.error = err.to_string(),
            },
            Kind::RootIndexer => {
                match is_queued_root_indexer(&pool, case.repo, case.commit, case.root, case.indexer)
                    .await
                {
                    Ok(value) => snapshot.root_indexer = Some(value),
                    Err(err) => snapshot.error = err.to_string(),
                }
            }
            Kind::Insert => {
                // The rank window is over every queued `lsif_indexes` row;
                // remove the fixtures' (completed) rows and previous insert
                // cases.
                sqlx::query("DELETE FROM lsif_indexes WHERE repository_id BETWEEN 9500 AND 9599")
                    .execute(&pool)
                    .await
                    .expect("reset indexes");
                match insert_jobs(&pool, &case.jobs, 0).await {
                    Ok(Some(indexes)) => {
                        snapshot.jobs = Some(indexes.into_iter().map(Job::from).collect())
                    }
                    Ok(None) => {}
                    Err(err) => snapshot.error = err.to_string(),
                }
            }
        }
        println!("caen {}", serde_json::to_string(&snapshot).expect("json"));
    }
}
