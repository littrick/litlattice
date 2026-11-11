//! Differential harness for the `batches-bulk-processor` port.
//!
//! The Go oracle (`internal/batches/processor/zz_m99h_bulk_oracle_test.go`,
//! untracked) and this file read the same corpus and print the same normalised
//! JSON; `test_ws/p9-go/run_batches_bulk_diff.sh` compares them.
//!
//! No database or network is touched: `BulkProcessorStore`/`ChangesetSource`/
//! `BulkTransaction` are fakes that log a canonical event string for every
//! call, which makes the `Done` → `afterDone` ordering directly visible.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sg_service_worker::jobs::batches_bulk_processor::{
    run_bulk_job_observed, BulkError, BulkJobOutcome, BulkProcessorStore, BulkTransaction,
    Changeset, ChangesetBatchChange, ChangesetEvent, ChangesetJob, ChangesetJobKind,
    ChangesetSource, RepoRef, WebhookEnqueue,
};

#[derive(Debug, Default, Deserialize)]
struct Corpus {
    #[serde(default)]
    cases: Vec<Case>,
}

#[derive(Debug, Default, Clone, Deserialize)]
struct Case {
    #[serde(default)]
    name: String,
    #[serde(default)]
    job: JobIn,
    #[serde(default)]
    changeset: ChangesetIn,
    #[serde(default)]
    repo: RepoIn,
    #[serde(default)]
    spec: SpecIn,
    #[serde(default)]
    source: HashMap<String, String>,
    #[serde(default)]
    store: HashMap<String, String>,
    #[serde(default)]
    transaction: HashMap<String, String>,
}

#[derive(Debug, Default, Clone, Deserialize)]
struct JobIn {
    #[serde(default)]
    id: i64,
    #[serde(default)]
    bulk_group: String,
    #[serde(default)]
    batch_change_id: i64,
    #[serde(default)]
    user_id: i32,
    #[serde(default)]
    changeset_id: i64,
    #[serde(default)]
    job_type: String,
    #[serde(default)]
    payload: Value,
    #[serde(default)]
    payload_type: Option<String>,
}

#[derive(Debug, Default, Clone, Deserialize)]
struct ChangesetIn {
    #[serde(default)]
    exists: bool,
    #[serde(default)]
    repo_id: i32,
    #[serde(default)]
    current_spec_id: i64,
    #[serde(default)]
    reconciler_state: String,
    #[serde(default)]
    ui_publication_state: Option<String>,
    #[serde(default)]
    batch_changes: Vec<AssocIn>,
}

#[derive(Debug, Default, Clone, Deserialize)]
struct AssocIn {
    #[serde(default)]
    batch_change_id: i64,
    #[serde(default)]
    detach: bool,
}

#[derive(Debug, Default, Clone, Deserialize)]
struct RepoIn {
    #[serde(default)]
    exists: bool,
    #[serde(default)]
    id: i32,
    #[serde(default)]
    name: String,
    #[serde(default)]
    external_service_type: String,
    #[serde(default)]
    external_service_id: String,
}

#[derive(Debug, Default, Clone, Deserialize)]
struct SpecIn {
    #[serde(default)]
    exists: bool,
    /// `null` models `PublishedValue{Val: nil}`.
    #[serde(default)]
    published: Option<bool>,
}

#[derive(Debug, Serialize)]
struct OutcomeOut {
    name: String,
    events: Vec<String>,
    process_error: Option<String>,
    non_retryable: bool,
    after_done: Option<AfterDoneOut>,
    done_called: bool,
    done_input_error: bool,
    done_error: Option<String>,
    webhook_error: Option<String>,
    handler_error: Option<String>,
    final_state: String,
}

#[derive(Debug, Serialize)]
struct AfterDoneOut {
    event_type: String,
    changeset_id: i64,
}

fn flag(map: &HashMap<String, String>, key: &str) -> String {
    map.get(key).cloned().unwrap_or_else(|| "ok".to_owned())
}

fn job_from_case(case: &Case) -> ChangesetJob {
    ChangesetJob {
        id: case.job.id,
        bulk_group: case.job.bulk_group.clone(),
        batch_change_id: case.job.batch_change_id,
        user_id: case.job.user_id,
        changeset_id: case.job.changeset_id,
        job_type: ChangesetJobKind::from_db(&case.job.job_type),
        payload: case.job.payload.clone(),
        payload_type: case.job.payload_type.clone(),
        state: "processing".to_owned(),
        failure_message: None,
        num_resets: 0,
        num_failures: 0,
    }
}

fn changeset_from_case(case: &Case) -> Changeset {
    Changeset {
        id: case.job.changeset_id,
        repo_id: case.changeset.repo_id,
        current_spec_id: case.changeset.current_spec_id,
        reconciler_state: case.changeset.reconciler_state.clone(),
        ui_publication_state: case.changeset.ui_publication_state.clone(),
        batch_changes: case
            .changeset
            .batch_changes
            .iter()
            .map(|assoc| ChangesetBatchChange {
                batch_change_id: assoc.batch_change_id,
                detach: assoc.detach,
            })
            .collect(),
    }
}

fn repo_from_case(case: &Case) -> RepoRef {
    RepoRef {
        id: case.repo.id,
        name: case.repo.name.clone(),
        external_service_type: case.repo.external_service_type.clone(),
        external_service_id: case.repo.external_service_id.clone(),
    }
}

/// A fake store and source sharing one event log.
struct Fake {
    log: Arc<Mutex<Vec<String>>>,
    case: Case,
}

impl Fake {
    fn rec(&self, event: String) {
        self.log.lock().unwrap().push(event);
    }

    /// Logs `{prefix},result=ok|error)` and returns the matching result.
    fn store_op(&self, key: &str, prefix: String) -> Result<(), BulkError> {
        if flag(&self.case.store, key) == "error" {
            self.rec(format!("{prefix},result=error)"));
            Err(BulkError::retryable(format!("{key} boom")))
        } else {
            self.rec(format!("{prefix},result=ok)"));
            Ok(())
        }
    }

    fn source_op(&self, key: &str, prefix: String) -> Result<(), BulkError> {
        if flag(&self.case.source, key) == "error" {
            self.rec(format!("{prefix},result=error)"));
            Err(BulkError::retryable(format!("{key} boom")))
        } else {
            self.rec(format!("{prefix},result=ok)"));
            Ok(())
        }
    }
}

#[async_trait]
impl BulkProcessorStore for Fake {
    async fn get_changeset(&self, id: i64) -> Result<Option<Changeset>, BulkError> {
        if flag(&self.case.store, "get_changeset") == "error" {
            self.rec(format!("get_changeset(id={id},result=error)"));
            return Err(BulkError::retryable("get_changeset boom"));
        }
        if !self.case.changeset.exists {
            self.rec(format!("get_changeset(id={id},result=none)"));
            return Ok(None);
        }
        self.rec(format!("get_changeset(id={id},result=ok)"));
        Ok(Some(changeset_from_case(&self.case)))
    }

    async fn get_repo(&self, repo_id: i32) -> Result<Option<RepoRef>, BulkError> {
        if flag(&self.case.store, "get_repo") == "error" {
            self.rec(format!("get_repo(id={repo_id},result=error)"));
            return Err(BulkError::retryable("get_repo boom"));
        }
        if !self.case.repo.exists {
            self.rec(format!("get_repo(id={repo_id},result=none)"));
            return Ok(None);
        }
        self.rec(format!("get_repo(id={repo_id},result=ok)"));
        Ok(Some(repo_from_case(&self.case)))
    }

    async fn get_changeset_spec_published(
        &self,
        id: i64,
    ) -> Result<Option<Option<bool>>, BulkError> {
        if flag(&self.case.store, "get_spec") == "error" {
            self.rec(format!("get_spec(id={id},result=error)"));
            return Err(BulkError::retryable("get_spec boom"));
        }
        if !self.case.spec.exists {
            self.rec(format!("get_spec(id={id},result=none)"));
            return Ok(None);
        }
        self.rec(format!("get_spec(id={id},result=ok)"));
        Ok(Some(self.case.spec.published))
    }

    async fn update_changeset_batch_changes(&self, changeset: &Changeset) -> Result<(), BulkError> {
        self.store_op(
            "update_batch_changes",
            format!("update_batch_changes(changeset={}", changeset.id),
        )
    }

    async fn enqueue_changeset(
        &self,
        changeset_id: i64,
        reset_state: &str,
    ) -> Result<(), BulkError> {
        self.store_op(
            "enqueue_changeset",
            format!("enqueue_changeset(changeset={changeset_id},state={reset_state}"),
        )
    }

    async fn reenqueue_changeset(&self, changeset_id: i64) -> Result<(), BulkError> {
        self.store_op("reenqueue", format!("reenqueue(changeset={changeset_id}"))
    }

    async fn set_derived_state(&self, changeset: &Changeset) -> Result<(), BulkError> {
        self.rec(format!(
            "set_derived_state(changeset={},result=ok)",
            changeset.id
        ));
        Ok(())
    }

    async fn upsert_changeset_events(
        &self,
        changeset_id: i64,
        events: &[ChangesetEvent],
    ) -> Result<(), BulkError> {
        self.store_op(
            "upsert_events",
            format!(
                "upsert_events(changeset={changeset_id},count={}",
                events.len()
            ),
        )
    }

    async fn update_changeset_code_host_state(
        &self,
        changeset: &Changeset,
    ) -> Result<(), BulkError> {
        self.store_op(
            "update_code_host_state",
            format!("update_code_host_state(changeset={}", changeset.id),
        )
    }

    async fn update_changeset_ui_publication_state(
        &self,
        changeset: &Changeset,
    ) -> Result<(), BulkError> {
        self.store_op(
            "update_ui_publication",
            format!(
                "update_ui_publication_state(changeset={},state={}",
                changeset.id,
                changeset.ui_publication_state.as_deref().unwrap_or("null")
            ),
        )
    }

    async fn enqueue_changeset_webhook(
        &self,
        event_type: &str,
        changeset_id: i64,
    ) -> Result<(), BulkError> {
        self.store_op(
            "enqueue_webhook",
            format!("enqueue_webhook(event={event_type},changeset={changeset_id}"),
        )
    }
}

#[async_trait]
impl ChangesetSource for Fake {
    async fn for_user(&self, user_id: i32, repo: &RepoRef) -> Result<(), BulkError> {
        self.source_op(
            "for_user",
            format!("for_user(user={user_id},repo={}", repo.id),
        )
    }

    async fn get_remote_repo(
        &self,
        _repo: &RepoRef,
        changeset: &Changeset,
    ) -> Result<(), BulkError> {
        self.source_op(
            "get_remote_repo",
            format!("get_remote_repo(changeset={}", changeset.id),
        )
    }

    async fn create_comment(&self, changeset: &Changeset, message: &str) -> Result<(), BulkError> {
        self.source_op(
            "create_comment",
            format!(
                "create_comment(changeset={},message={message}",
                changeset.id
            ),
        )
    }

    async fn merge_changeset(&self, changeset: &Changeset, squash: bool) -> Result<(), BulkError> {
        self.source_op(
            "merge",
            format!("merge(changeset={},squash={squash}", changeset.id),
        )
    }

    async fn close_changeset(&self, changeset: &Changeset) -> Result<(), BulkError> {
        self.source_op("close", format!("close(changeset={}", changeset.id))
    }

    async fn changeset_events(
        &self,
        changeset: &Changeset,
    ) -> Result<Vec<ChangesetEvent>, BulkError> {
        if flag(&self.case.source, "events") == "error" {
            self.rec(format!("events(changeset={},result=error)", changeset.id));
            return Err(BulkError::retryable("events boom"));
        }
        self.rec(format!(
            "events(changeset={},result=ok,count=1)",
            changeset.id
        ));
        Ok(vec![ChangesetEvent {
            kind: "comment".to_owned(),
            key: "c1".to_owned(),
        }])
    }
}

struct FakeTx {
    log: Arc<Mutex<Vec<String>>>,
    commit_ok: bool,
}

#[async_trait]
impl BulkTransaction for FakeTx {
    async fn done(&self, err: Option<&BulkError>) -> Result<(), BulkError> {
        let mut log = self.log.lock().unwrap();
        match err {
            Some(err) => {
                log.push(format!(
                    "tx_done(input_error=error,result=error,error={})",
                    err.message
                ));
                Err(err.clone())
            }
            None if !self.commit_ok => {
                log.push("tx_done(input_error=none,result=error,error=commit boom)".to_owned());
                Err(BulkError::retryable("commit boom"))
            }
            None => {
                log.push("tx_done(input_error=none,result=ok)".to_owned());
                Ok(())
            }
        }
    }
}

async fn run_case(case: &Case) -> OutcomeOut {
    let log = Arc::new(Mutex::new(Vec::new()));
    let fake = Arc::new(Fake {
        log: Arc::clone(&log),
        case: case.clone(),
    });
    let job = job_from_case(case);
    let tx = FakeTx {
        log: Arc::clone(&log),
        commit_ok: flag(&case.transaction, "commit") != "error",
    };

    let outcome: BulkJobOutcome = if flag(&case.transaction, "begin") == "error" {
        log.lock()
            .unwrap()
            .push("tx_begin(result=error)".to_owned());
        run_bulk_job_observed(
            fake.as_ref(),
            fake.as_ref(),
            Err(BulkError::retryable("begin transaction boom")),
            &job,
        )
        .await
    } else {
        log.lock().unwrap().push("tx_begin(result=ok)".to_owned());
        run_bulk_job_observed(fake.as_ref(), fake.as_ref(), Ok(&tx), &job).await
    };

    let events = log.lock().unwrap().clone();
    OutcomeOut {
        name: case.name.clone(),
        events,
        process_error: outcome
            .process_error
            .as_ref()
            .map(|err| err.message.clone()),
        non_retryable: outcome
            .process_error
            .as_ref()
            .map(|err| err.non_retryable)
            .unwrap_or(false),
        after_done: outcome.after_done.as_ref().map(
            |WebhookEnqueue {
                 event_type,
                 changeset_id,
             }| AfterDoneOut {
                event_type: event_type.clone(),
                changeset_id: *changeset_id,
            },
        ),
        done_called: outcome.done_called,
        done_input_error: outcome.done_input_error,
        done_error: outcome.done_error.as_ref().map(|err| err.message.clone()),
        webhook_error: outcome
            .webhook_error
            .as_ref()
            .map(|err| err.message.clone()),
        handler_error: outcome
            .handler_error
            .as_ref()
            .map(|err| err.message.clone()),
        final_state: outcome.final_state().to_owned(),
    }
}

#[tokio::test]
async fn batches_bulk_differential() {
    if std::env::var("M99H_DIFF").ok().as_deref() != Some("1") {
        return;
    }
    let corpus_path = std::env::var("M99H_CORPUS").expect("M99H_CORPUS");
    let out_path = std::env::var("M99H_OUT").expect("M99H_OUT");

    let raw = std::fs::read_to_string(&corpus_path).expect("read corpus");
    let corpus: Corpus = serde_json::from_str(&raw).expect("parse corpus");

    let mut outputs = Vec::with_capacity(corpus.cases.len());
    for case in &corpus.cases {
        outputs.push(run_case(case).await);
    }

    let rendered = serde_json::to_string_pretty(&outputs).expect("render output");
    std::fs::write(&out_path, rendered + "\n").expect("write output");
}
