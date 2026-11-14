#![forbid(unsafe_code)]

//! M9.8bl differential harness (Rust side): the `own-repo-indexing-queue`
//! job's queue store, resetter, janitor, scheduler and `recent-contributors`
//! indexer.
//!
//! Mirrors `internal/own/background/own_harness_test.go`; outputs are compared
//! line for line by `test_ws/p9-go/run_own_repo_indexing_diff.sh`. Both sides
//! seed identical fixtures into a scratch copy of the frontend schema and
//! print canonical `own ` lines. Timestamps are never printed; only relative
//! intervals (well inside/outside the 24h index interval and the 30s stalled
//! age) are used so the two runs agree.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::Duration;

use async_trait::async_trait;
use sg_git::{ChangeStatus, ChangedFile, Commit, CommitsOptions};
use sg_service_worker::framework::dbworker::{DbResetter, DbWorkerStore};
use sg_service_worker::jobs::own_repo_indexing::{
    self, OwnBackgroundStore, RecentContributionsProvider,
};
use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;
use tokio_util::sync::CancellationToken;

const HOSTNAME: &str = "harness-host";

fn print_line(line: impl Into<String>) {
    println!("own {}", line.into());
}

fn esc(value: &str) -> String {
    value.replace('\n', "\\n")
}

async fn connect() -> Option<PgPool> {
    let dsn = std::env::var("SG_OWN_TEST_DSN").ok()?;
    Some(
        PgPoolOptions::new()
            .max_connections(1)
            .connect(&dsn)
            .await
            .expect("connect to SG_OWN_TEST_DSN"),
    )
}

async fn truncate(pool: &PgPool) {
    sqlx::query(
        "TRUNCATE TABLE own_background_jobs, own_signal_configurations, \
             own_signal_recent_contribution, own_aggregate_recent_contribution, \
             repo_paths, commit_authors, gitserver_repos, repo \
         RESTART IDENTITY CASCADE",
    )
    .execute(pool)
    .await
    .expect("truncate fixtures");
}

async fn seed_signal_configurations(pool: &PgPool) {
    sqlx::query(
        "INSERT INTO own_signal_configurations (name, description, excluded_repo_patterns, enabled) \
         VALUES \
           ('recent-contributors', 'Contributions in the last 90 days', '{great-repo-5}'::text[], TRUE), \
           ('analytics', 'Number of files in a repository', '{}'::text[], FALSE)",
    )
    .execute(pool)
    .await
    .expect("seed signal configurations");
}

async fn insert_repo(
    pool: &PgPool,
    id: i32,
    name: &str,
    private: bool,
    external_service_type: Option<&str>,
    blocked: Option<&str>,
) {
    sqlx::query(
        "INSERT INTO repo (id, name, private, external_service_type, blocked) \
         VALUES ($1, $2, $3, $4, $5::jsonb)",
    )
    .bind(id)
    .bind(name)
    .bind(private)
    .bind(external_service_type)
    .bind(blocked)
    .execute(pool)
    .await
    .expect("insert repo");
}

async fn insert_gitserver_repo(pool: &PgPool, repo_id: i32, clone_status: &str) {
    // `repo` inserts create the `gitserver_repos` row through
    // `trigger_gitserver_repo_insert`; only the clone status needs setting.
    sqlx::query("UPDATE gitserver_repos SET clone_status = $2 WHERE repo_id = $1")
        .bind(repo_id)
        .bind(clone_status)
        .execute(pool)
        .await
        .expect("update gitserver repo");
}

#[allow(clippy::too_many_arguments)]
async fn insert_job(
    pool: &PgPool,
    repo_id: i32,
    job_type: i32,
    state: &str,
    finished_interval: Option<&str>,
    num_failures: i32,
    cancel: bool,
    last_heartbeat_interval: Option<&str>,
) -> i64 {
    let id: i32 = sqlx::query_scalar(
        "INSERT INTO own_background_jobs \
             (repo_id, job_type, state, finished_at, num_failures, cancel, last_heartbeat_at) \
         VALUES ($1, $2, $3, \
             CASE WHEN $4::text IS NULL THEN NULL ELSE now() - $4::interval END, \
             $5, $6, \
             CASE WHEN $7::text IS NULL THEN NULL ELSE now() - $7::interval END) \
         RETURNING id",
    )
    .bind(repo_id)
    .bind(job_type)
    .bind(state)
    .bind(finished_interval)
    .bind(num_failures)
    .bind(cancel)
    .bind(last_heartbeat_interval)
    .fetch_one(pool)
    .await
    .expect("insert job");
    i64::from(id)
}

async fn job_summary(pool: &PgPool, job_type_name: &str) -> (i64, String) {
    let rows: Vec<(i32, String, i32)> = sqlx::query_as(
        "SELECT obj.id, obj.state, obj.repo_id \
         FROM own_background_jobs obj \
         JOIN own_signal_configurations conf ON conf.id = obj.job_type \
         WHERE conf.name = $1 \
         ORDER BY obj.repo_id, obj.id",
    )
    .bind(job_type_name)
    .fetch_all(pool)
    .await
    .expect("job summary");
    let summary = rows
        .iter()
        .map(|(id, state, repo_id)| format!("{id}:{state}:{repo_id}"))
        .collect::<Vec<_>>()
        .join(",");
    (rows.len() as i64, summary)
}

async fn job_row(pool: &PgPool, id: i32) -> (String, i32, Option<String>) {
    let row: (String, i32, Option<String>) = sqlx::query_as(
        "SELECT state, num_failures, failure_message FROM own_background_jobs WHERE id = $1",
    )
    .bind(id)
    .fetch_one(pool)
    .await
    .expect("job row");
    row
}

async fn scenario_config(pool: &PgPool) {
    truncate(pool).await;
    seed_signal_configurations(pool).await;

    for name in ["recent-contributors", "analytics"] {
        let config = own_repo_indexing::load_config(pool, name)
            .await
            .expect("load config");
        print_line(format!(
            "config {name} id={} enabled={} excluded={}",
            config.id,
            config.enabled,
            config.excluded_repo_patterns.join("|")
        ));
    }

    let missing = own_repo_indexing::load_config(pool, "missing").await;
    print_line(format!(
        "config missing err={}",
        esc(&missing.expect_err("missing configuration"))
    ));
}

fn scheduler_handler(pool: &PgPool, name: &'static str) -> own_repo_indexing::SchedulerHandler {
    use sg_service_worker::jobs::own_repo_indexing::IndexJobType;
    own_repo_indexing::SchedulerHandler::new(
        pool.clone(),
        IndexJobType {
            name,
            index_interval: Duration::from_secs(24 * 60 * 60),
            refresh_interval: Duration::from_secs(5 * 60),
        },
    )
}

async fn scenario_scheduler(pool: &PgPool) {
    use sg_service_worker::framework::periodic::Handler;

    truncate(pool).await;
    seed_signal_configurations(pool).await;

    insert_repo(pool, 500, "great-repo-1", false, None, None).await;
    insert_repo(pool, 501, "great-repo-2", false, None, None).await;
    insert_repo(pool, 502, "great-repo-3", false, None, None).await;
    insert_repo(pool, 503, "great-repo-4", false, None, None).await;
    insert_repo(pool, 504, "great-repo-5", false, None, None).await;

    for (repo_id, status) in [
        (500, "cloned"),
        (501, "cloned"),
        (502, "cloned"),
        (503, "not_cloned"),
        (504, "cloned"),
    ] {
        insert_gitserver_repo(pool, repo_id, status).await;
    }

    // One finished job older than the index interval (eligible again) and one
    // processing job (never eligible).
    insert_job(pool, 501, 1, "completed", Some("48 hours"), 0, false, None).await;
    insert_job(pool, 502, 1, "processing", None, 0, false, None).await;

    for name in ["recent-contributors", "analytics"] {
        let error = scheduler_handler(pool, name)
            .handle(CancellationToken::new())
            .await
            .err()
            .map(|err| err.to_string());
        let (total, jobs) = job_summary(pool, name).await;
        match error {
            Some(error) => print_line(format!("scheduler {name} err={}", esc(&error))),
            None => print_line(format!("scheduler {name} total={total} jobs={jobs}")),
        }
    }

    // Enable analytics and run again; the baseline's `ineligible_repos` CTE
    // makes queued/processing records of the *other* job type ineligible too.
    sqlx::query("UPDATE own_signal_configurations SET enabled = TRUE WHERE name = 'analytics'")
        .execute(pool)
        .await
        .expect("enable analytics");
    let error = scheduler_handler(pool, "analytics")
        .handle(CancellationToken::new())
        .await
        .err()
        .map(|err| err.to_string());
    let (total, jobs) = job_summary(pool, "analytics").await;
    match error {
        Some(error) => print_line(format!("scheduler analytics-enabled err={}", esc(&error))),
        None => print_line(format!(
            "scheduler analytics-enabled total={total} jobs={jobs}"
        )),
    }

    // Missing configuration through the wrapper.
    let error = scheduler_handler(pool, "missing")
        .handle(CancellationToken::new())
        .await
        .err()
        .map(|err| err.to_string());
    print_line(format!(
        "scheduler missing err={}",
        esc(&error.expect("missing configuration error"))
    ));
}

async fn scenario_queue(pool: &PgPool) {
    truncate(pool).await;
    seed_signal_configurations(pool).await;
    let store = OwnBackgroundStore::new(pool.clone());

    insert_job(pool, 500, 1, "queued", None, 0, false, None).await;
    insert_job(pool, 501, 1, "queued", None, 0, false, None).await;

    let record = store
        .dequeue(HOSTNAME)
        .await
        .expect("dequeue")
        .expect("queued record");
    print_line(format!(
        "dequeue id={} state={} repo={} config={} worker={}",
        record.id, record.state, record.repo_id, record.config_name, record.worker_hostname
    ));

    let (known, cancel) = store
        .heartbeat(&[record.id.to_string()])
        .await
        .expect("heartbeat");
    let mut known = known;
    known.sort();
    let mut cancel = cancel;
    cancel.sort();
    print_line(format!(
        "heartbeat known={} canceled={}",
        known.join("|"),
        cancel.join("|")
    ));

    assert!(store.mark_complete(&record).await.expect("mark complete"));
    let (state, failures, message) = job_row(pool, record.id).await;
    print_line(format!(
        "complete state={state} failures={failures} message={}",
        message.unwrap_or_default()
    ));

    let record = store
        .dequeue(HOSTNAME)
        .await
        .expect("dequeue")
        .expect("second queued record");
    assert!(store.mark_errored(&record, "boom").await.expect("errored"));
    let (state, failures, message) = job_row(pool, record.id).await;
    print_line(format!(
        "errored state={state} failures={failures} message={}",
        message.unwrap_or_default()
    ));

    // `num_failures + 1 >= 3` moves the record to failed.
    let id = insert_job(pool, 503, 1, "queued", None, 2, false, None).await;
    let record = store
        .dequeue(HOSTNAME)
        .await
        .expect("dequeue")
        .expect("record");
    assert_eq!(i64::from(record.id), id);
    assert!(store.mark_errored(&record, "boom").await.expect("errored"));
    let (state, failures, _) = job_row(pool, record.id).await;
    print_line(format!("errored-max state={state} failures={failures}"));

    // `cancel = true` moves the record to canceled without counting a failure.
    let id = insert_job(pool, 504, 1, "queued", None, 0, true, None).await;
    let record = store
        .dequeue(HOSTNAME)
        .await
        .expect("dequeue")
        .expect("record");
    assert_eq!(i64::from(record.id), id);
    assert!(store.mark_errored(&record, "boom").await.expect("errored"));
    let (state, failures, _) = job_row(pool, record.id).await;
    print_line(format!("canceled state={state} failures={failures}"));

    let id = insert_job(pool, 505, 1, "queued", None, 0, false, None).await;
    let record = store
        .dequeue(HOSTNAME)
        .await
        .expect("dequeue")
        .expect("record");
    assert_eq!(i64::from(record.id), id);
    assert!(store.mark_failed(&record, "boom").await.expect("failed"));
    let (state, failures, _) = job_row(pool, record.id).await;
    print_line(format!("failed state={state} failures={failures}"));

    // Resetter: a stalled record with resets left is queued; one that already
    // used all its resets is failed with the default message.
    let reset_id = insert_job(
        pool,
        506,
        1,
        "processing",
        None,
        0,
        false,
        Some("60 seconds"),
    )
    .await;
    let failed_id = insert_job(
        pool,
        507,
        1,
        "processing",
        None,
        0,
        false,
        Some("60 seconds"),
    )
    .await;
    sqlx::query("UPDATE own_background_jobs SET num_resets = 10 WHERE id = $1")
        .bind(failed_id)
        .execute(pool)
        .await
        .expect("set num_resets");

    let (mut queued, mut failed) = store.reset_stalled().await.expect("reset stalled");
    queued.sort_by_key(|(id, _)| *id);
    failed.sort_by_key(|(id, _)| *id);
    print_line(format!(
        "reset queued={} failed={}",
        queued
            .iter()
            .map(|(id, _)| id.to_string())
            .collect::<Vec<_>>()
            .join("|"),
        failed
            .iter()
            .map(|(id, _)| id.to_string())
            .collect::<Vec<_>>()
            .join("|")
    ));
    let (state, failures, _) = job_row(pool, i32::try_from(reset_id).expect("id")).await;
    let resets: i32 =
        sqlx::query_scalar("SELECT num_resets FROM own_background_jobs WHERE id = $1")
            .bind(reset_id)
            .fetch_one(pool)
            .await
            .expect("num_resets");
    print_line(format!(
        "reset queued-state={state} failures={failures} resets={resets}"
    ));
    let (state, failures, message) = job_row(pool, i32::try_from(failed_id).expect("id")).await;
    print_line(format!(
        "reset failed-state={state} failures={failures} message={}",
        message.unwrap_or_default()
    ));

    // Janitor: a completed record older than 7 days and a record whose job
    // type is not in the config-aware view are deleted.
    let deletable = insert_job(pool, 508, 1, "completed", Some("8 days"), 0, false, None).await;
    let kept = insert_job(pool, 509, 1, "completed", Some("1 day"), 0, false, None).await;
    let queued_kept = insert_job(pool, 510, 1, "queued", None, 0, false, None).await;
    // The analytics configuration is disabled, so its records are invisible
    // to the view and always deleted.
    let disabled = insert_job(pool, 511, 2, "completed", Some("8 days"), 0, false, None).await;

    let deleted = own_repo_indexing::cleanup(pool).await.expect("janitor");
    let remaining: Vec<(i32, String)> =
        sqlx::query_as("SELECT id, state FROM own_background_jobs ORDER BY id")
            .fetch_all(pool)
            .await
            .expect("remaining jobs");
    print_line(format!("janitor deleted={deleted}"));
    print_line(format!(
        "janitor remaining={}",
        remaining
            .iter()
            .map(|(id, state)| format!("{id}:{state}"))
            .collect::<Vec<_>>()
            .join(",")
    ));
    let present = |id: i64| {
        remaining
            .iter()
            .any(|(candidate, _)| i64::from(*candidate) == id)
    };
    print_line(format!(
        "janitor deleted-ids={} kept={}",
        [deletable, disabled]
            .iter()
            .filter(|id| !present(**id))
            .map(i64::to_string)
            .collect::<Vec<_>>()
            .join("|"),
        [kept, queued_kept]
            .iter()
            .filter(|id| present(**id))
            .map(i64::to_string)
            .collect::<Vec<_>>()
            .join("|")
    ));
}

#[derive(Default)]
struct FakeGitserver {
    commits: HashMap<String, Vec<Commit>>,
    changed: HashMap<String, Vec<ChangedFile>>,
    commits_error: HashMap<String, String>,
    changed_error: HashMap<String, String>,
    calls: Mutex<(usize, usize)>,
}

impl FakeGitserver {
    fn reset_calls(&self) {
        *self.calls.lock().expect("calls lock") = (0, 0);
    }

    fn calls(&self) -> (usize, usize) {
        *self.calls.lock().expect("calls lock")
    }
}

#[async_trait]
impl RecentContributionsProvider for FakeGitserver {
    async fn commits(&self, repo: &str, _options: &CommitsOptions) -> Result<Vec<Commit>, String> {
        self.calls.lock().expect("calls lock").0 += 1;
        if let Some(error) = self.commits_error.get(repo) {
            return Err(error.clone());
        }
        Ok(self.commits.get(repo).cloned().unwrap_or_default())
    }

    async fn changed_files(
        &self,
        _repo: &str,
        _base: &str,
        head: &str,
    ) -> Result<Vec<ChangedFile>, String> {
        self.calls.lock().expect("calls lock").1 += 1;
        if let Some(error) = self.changed_error.get(head) {
            return Err(error.clone());
        }
        Ok(self.changed.get(head).cloned().unwrap_or_default())
    }
}

fn commit(oid: &str, parents: &[&str], name: &str, email: &str) -> Commit {
    Commit {
        oid: oid.to_owned(),
        parents: parents.iter().map(|parent| (*parent).to_owned()).collect(),
        author_name: name.to_owned(),
        author_email: email.to_owned(),
        author_date: Some(prost_types::Timestamp {
            seconds: 1_700_000_000,
            nanos: 0,
        }),
        committer_date: None,
        message: String::new(),
    }
}

fn changed(path: &str) -> ChangedFile {
    ChangedFile {
        path: path.to_owned(),
        status: ChangeStatus::Modified,
    }
}

async fn rc_counts(pool: &PgPool) -> (i64, String, String, String) {
    let signals: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM own_signal_recent_contribution")
        .fetch_one(pool)
        .await
        .expect("signal count");
    let authors: Vec<(String, String)> =
        sqlx::query_as("SELECT name, email FROM commit_authors ORDER BY id")
            .fetch_all(pool)
            .await
            .expect("authors");
    let paths: Vec<(String, Option<String>)> = sqlx::query_as(
        "SELECT p.absolute_path, parent.absolute_path \
         FROM repo_paths p \
         LEFT JOIN repo_paths parent ON parent.id = p.parent_id \
         ORDER BY p.absolute_path",
    )
    .fetch_all(pool)
    .await
    .expect("paths");
    let aggregates: Vec<(String, String, i32)> = sqlx::query_as(
        "SELECT a.email, p.absolute_path, agg.contributions_count \
         FROM own_aggregate_recent_contribution agg \
         JOIN commit_authors a ON a.id = agg.commit_author_id \
         JOIN repo_paths p ON p.id = agg.changed_file_path_id \
         ORDER BY p.absolute_path, a.email, agg.contributions_count DESC",
    )
    .fetch_all(pool)
    .await
    .expect("aggregates");

    (
        signals,
        authors
            .iter()
            .map(|(name, email)| format!("{name}|{email}"))
            .collect::<Vec<_>>()
            .join(","),
        paths
            .iter()
            .map(|(path, parent)| {
                format!(
                    "{path}:{}",
                    parent.clone().unwrap_or_else(|| "-".to_owned())
                )
            })
            .collect::<Vec<_>>()
            .join(","),
        aggregates
            .iter()
            .map(|(email, path, count)| format!("{email}:{path}:{count}"))
            .collect::<Vec<_>>()
            .join(","),
    )
}

async fn print_rc(pool: &PgPool, label: &str, fake: &FakeGitserver) {
    let (signals, authors, paths, aggregates) = rc_counts(pool).await;
    let (commits, changed) = fake.calls();
    print_line(format!("rc {label} signals={signals}"));
    print_line(format!("rc {label} authors={authors}"));
    print_line(format!("rc {label} paths={paths}"));
    print_line(format!("rc {label} aggregate={aggregates}"));
    print_line(format!(
        "rc {label} calls=commits:{commits},changed:{changed}"
    ));
}

async fn scenario_recent_contributors(pool: &PgPool) {
    truncate(pool).await;
    seed_signal_configurations(pool).await;

    insert_repo(pool, 600, "repo-ok", false, None, None).await;
    insert_repo(pool, 601, "repo-subrepo", true, Some("perforce"), None).await;
    insert_repo(
        pool,
        603,
        "repo-blocked",
        false,
        None,
        Some(r#"{"reason":"dmca"}"#),
    )
    .await;
    insert_repo(pool, 604, "repo-commits-error", false, None, None).await;
    insert_repo(pool, 605, "repo-changed-error", false, None, None).await;

    let mut fake = FakeGitserver::default();
    fake.commits.insert(
        "repo-ok".to_owned(),
        vec![
            commit("aaaaaaaa", &["p1"], "Alice", "alice@example.com"),
            commit("bbbbbbbb", &["p1", "p2"], "Merge", "merge@example.com"),
            commit("cccccccc", &[], "Bob", "bob@example.com"),
        ],
    );
    fake.changed.insert(
        "aaaaaaaa".to_owned(),
        vec![changed("src/a.go"), changed("README.md")],
    );
    fake.changed
        .insert("cccccccc".to_owned(), vec![changed("src/a.go")]);
    fake.commits.insert("repo-subrepo".to_owned(), Vec::new());
    fake.commits_error
        .insert("repo-commits-error".to_owned(), "boom".to_owned());
    fake.commits.insert(
        "repo-changed-error".to_owned(),
        vec![commit("dddddddd", &["p1"], "Carol", "carol@example.com")],
    );
    fake.changed_error
        .insert("dddddddd".to_owned(), "boom".to_owned());

    fake.reset_calls();
    own_repo_indexing::index_recent_contributors(pool, &fake, 600, true)
        .await
        .expect("index repo-ok");
    print_rc(pool, "ok", &fake).await;

    // A second pass clears and re-inserts the same signals.
    fake.reset_calls();
    own_repo_indexing::index_recent_contributors(pool, &fake, 600, true)
        .await
        .expect("re-index repo-ok");
    print_rc(pool, "rerun", &fake).await;

    // Sub-repo permissions enabled: the perforce repo is skipped entirely.
    fake.reset_calls();
    own_repo_indexing::index_recent_contributors(pool, &fake, 601, true)
        .await
        .expect("skip sub-repo repo");
    print_rc(pool, "subrepo", &fake).await;

    // Sub-repo permissions disabled at the site level: the check is skipped.
    fake.reset_calls();
    own_repo_indexing::index_recent_contributors(pool, &fake, 601, false)
        .await
        .expect("index sub-repo repo");
    print_rc(pool, "subrepo-disabled", &fake).await;

    let error = own_repo_indexing::index_recent_contributors(pool, &fake, 602, true)
        .await
        .expect_err("missing repo");
    print_line(format!("rc missing err={}", esc(&error)));

    let error = own_repo_indexing::index_recent_contributors(pool, &fake, 603, true)
        .await
        .expect_err("blocked repo");
    print_line(format!("rc blocked err={}", esc(&error)));

    let error = own_repo_indexing::index_recent_contributors(pool, &fake, 604, true)
        .await
        .expect_err("commits error");
    print_line(format!("rc commits-err err={}", esc(&error)));

    let error = own_repo_indexing::index_recent_contributors(pool, &fake, 605, true)
        .await
        .expect_err("changed files error");
    print_line(format!("rc changed-err err={}", esc(&error)));
}

fn scenario_handler_non_retryable() {
    let error = "unsupported own index job type";
    print_line(format!(
        "handler unknown err={} non-retryable={}",
        error,
        own_repo_indexing::is_non_retryable(error)
    ));
    let resolve_head = "cannot resolve HEAD: boom";
    print_line(format!(
        "handler resolve-head err={} non-retryable={}",
        esc(resolve_head),
        own_repo_indexing::is_non_retryable(resolve_head)
    ));
}

#[tokio::test]
async fn own_repo_indexing_harness() {
    let Some(pool) = connect().await else {
        eprintln!("SG_OWN_TEST_DSN is not set; skipping the own repo indexing harness");
        return;
    };

    scenario_config(&pool).await;
    scenario_scheduler(&pool).await;
    scenario_queue(&pool).await;
    scenario_recent_contributors(&pool).await;
    scenario_handler_non_retryable();
}
