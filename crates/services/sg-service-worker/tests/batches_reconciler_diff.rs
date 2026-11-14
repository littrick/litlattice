//! M9.9g differential harness: the `batches-reconciler` plan / publication
//! state / non-retryable classification / worker-store options.
//!
//! Mirrors the UNTRACKED Go oracles
//! `internal/batches/reconciler/zz_m99g_oracle_test.go` and
//! `internal/batches/store/zz_m99g_store_oracle_test.go`. Both sides consume
//! `test_ws/p9-go/data/batches_reconciler/corpus.json`, write one normalized
//! JSON array (Go `json.Encoder` with `SetIndent`/`SetEscapeHTML(false)`,
//! Rust `serde_json::to_string_pretty`, both sorted map keys + trailing
//! newline), and `test_ws/p9-go/run_batches_reconciler_diff.sh` compares them
//! with `cmp`.
//!
//! A second array (worker-store options) is written to `M99G_STORE_OUT`
//! because `reconcilerWorkerStoreOpts` is unexported to package `store`.
//!
//! The `executor_*` / `handler_*` tests are Rust-only: the baseline
//! `executePlan` binds a concrete `*store.Store`, so a corpus-driven Go oracle
//! cannot inject a fake store/source. They cover operation ordering, the
//! `afterDone` webhook selection, transactional commit/rollback and error
//! classification (see the slice overview §4.5).
//!
//! Gated by `SG_M99G_DIFF=1`.

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use serde_json::{json, Map, Value};
use sg_db::batches::{
    RECONCILER_MAX_NUM_RESETS, RECONCILER_MAX_NUM_RETRIES, RECONCILER_RETRY_AFTER_SECONDS,
    RECONCILER_STALLED_MAX_AGE_SECONDS, RECONCILER_STATE_COLUMN, RECONCILER_STORE_NAME,
    RECONCILER_TABLE, RECONCILER_VIEW, RECONCILER_WORKER_DESCRIPTION, RECONCILER_WORKER_NAME,
};
use sg_service_worker::framework::dbworker::DbWorkerHandler;
use sg_service_worker::jobs::batches_reconciler::{
    calculate_publication_state, compare_changeset_specs, determine_plan, execute_plan,
    is_non_retryable, reopen_after_detach, BatchChangeAssoc, Changeset, ChangesetExternalState,
    ChangesetPublicationState, ChangesetSource, ChangesetSourceFactory, ChangesetSpec,
    ChangesetUiPublicationState, Operations, Plan, PublishedValue, ReconcilerError,
    ReconcilerHandler, ReconcilerOperation, ReconcilerStore, ReconcilerStoreFactory, Repo,
    SourceChangeset, SourceError, WebhookKind, CANNOT_PUSH_TO_ARCHIVED_REPO, NO_DRAFT_SUPPORT,
    NO_SSH_CREDENTIAL, PUBLISH_SAME_BRANCH,
};
use tokio_util::sync::CancellationToken;

fn string(value: &Value, key: &str) -> String {
    value
        .get(key)
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_owned()
}

fn string_opt(value: &Value, key: &str) -> Option<String> {
    value.get(key).and_then(Value::as_str).map(str::to_owned)
}

fn boolean(value: &Value, key: &str) -> bool {
    value.get(key).and_then(Value::as_bool).unwrap_or(false)
}

fn id(value: &Value, key: &str) -> i64 {
    value.get(key).and_then(Value::as_i64).unwrap_or(0)
}

fn published_from_json(value: &Value) -> PublishedValue {
    match value {
        Value::Bool(true) => PublishedValue::True,
        Value::Bool(false) => PublishedValue::False,
        Value::String(raw) => match raw.as_str() {
            "true" => PublishedValue::True,
            "false" => PublishedValue::False,
            "draft" => PublishedValue::Draft,
            _ => PublishedValue::Nil,
        },
        _ => PublishedValue::Nil,
    }
}

fn spec_from_json(value: &Value) -> ChangesetSpec {
    ChangesetSpec {
        id: id(value, "id"),
        external_id: string(value, "external_id"),
        base_rev: string(value, "base_rev"),
        base_ref: string(value, "base_ref"),
        head_ref: string(value, "head_ref"),
        title: string(value, "title"),
        body: string(value, "body"),
        published: value
            .get("published")
            .map(published_from_json)
            .unwrap_or(PublishedValue::Nil),
        diff: string(value, "diff").into_bytes(),
        commit_message: string(value, "commit_message"),
        commit_author_name: string(value, "commit_author_name"),
        commit_author_email: string(value, "commit_author_email"),
    }
}

fn external_state(value: &Value) -> Option<ChangesetExternalState> {
    value
        .get("external_state")
        .and_then(Value::as_str)
        .and_then(ChangesetExternalState::from_db)
}

fn ui_state(value: &Value) -> Option<ChangesetUiPublicationState> {
    value
        .get("ui_publication_state")
        .and_then(Value::as_str)
        .and_then(ChangesetUiPublicationState::from_db)
}

fn changeset_from_json(value: &Value) -> Changeset {
    let batch_changes = value
        .get("batch_changes")
        .and_then(Value::as_array)
        .map(|assocs| {
            assocs
                .iter()
                .map(|assoc| BatchChangeAssoc {
                    batch_change_id: id(assoc, "batch_change_id"),
                    detach: boolean(assoc, "detach"),
                    archive: boolean(assoc, "archive"),
                    is_archived: boolean(assoc, "is_archived"),
                })
                .collect()
        })
        .unwrap_or_default();
    Changeset {
        id: id(value, "id"),
        repo_id: id(value, "repo_id") as i32,
        batch_changes,
        external_id: string(value, "external_id"),
        external_service_type: string(value, "external_service_type"),
        external_branch: string(value, "external_branch"),
        external_state: external_state(value),
        owned_by_batch_change_id: id(value, "owned_by_batch_change_id"),
        current_spec_id: id(value, "current_spec_id"),
        previous_spec_id: id(value, "previous_spec_id"),
        publication_state: match string(value, "publication_state").as_str() {
            "PUBLISHED" => ChangesetPublicationState::Published,
            _ => ChangesetPublicationState::Unpublished,
        },
        ui_publication_state: ui_state(value),
        closing: boolean(value, "closing"),
        detached_at: None,
        failure_message: string_opt(value, "failure_message"),
        previous_failure_message: string_opt(value, "previous_failure_message"),
    }
}

fn delta_json(
    delta: Option<&sg_service_worker::jobs::batches_reconciler::ChangesetSpecDelta>,
) -> Value {
    match delta {
        None => Value::Null,
        Some(delta) => json!({
            "title_changed": delta.title_changed,
            "body_changed": delta.body_changed,
            "undraft": delta.undraft,
            "base_ref_changed": delta.base_ref_changed,
            "diff_changed": delta.diff_changed,
            "commit_message_changed": delta.commit_message_changed,
            "author_name_changed": delta.author_name_changed,
            "author_email_changed": delta.author_email_changed,
        }),
    }
}

fn operation_from_name(name: &str) -> Option<ReconcilerOperation> {
    Some(match name.to_ascii_uppercase().as_str() {
        "PUSH" => ReconcilerOperation::Push,
        "DETACH" => ReconcilerOperation::Detach,
        "ARCHIVE" => ReconcilerOperation::Archive,
        "REATTACH" => ReconcilerOperation::Reattach,
        "IMPORT" => ReconcilerOperation::Import,
        "PUBLISH" => ReconcilerOperation::Publish,
        "PUBLISH_DRAFT" => ReconcilerOperation::PublishDraft,
        "CLOSE" => ReconcilerOperation::Close,
        "REOPEN" => ReconcilerOperation::Reopen,
        "UNDRAFT" => ReconcilerOperation::Undraft,
        "UPDATE" => ReconcilerOperation::Update,
        "SLEEP" => ReconcilerOperation::Sleep,
        "SYNC" => ReconcilerOperation::Sync,
        _ => return None,
    })
}

fn operations_from_json(value: &Value) -> Operations {
    Operations(
        value
            .as_array()
            .map(|names| {
                names
                    .iter()
                    .filter_map(Value::as_str)
                    .filter_map(operation_from_name)
                    .collect()
            })
            .unwrap_or_default(),
    )
}

fn classify_message(case: &Value) -> String {
    match string(case, "error").as_str() {
        "publish-same-branch" => PUBLISH_SAME_BRANCH.to_owned(),
        "no-ssh-credential" => NO_SSH_CREDENTIAL.to_owned(),
        "missing-credentials" => format!(
            "user does not have a valid credential for repository {:?}",
            string(case, "repo")
        ),
        "no-push-credentials" => format!(
            "cannot use credentials of type {} to push commits",
            string(case, "credentials_type")
        ),
        "cannot-push-archived" => CANNOT_PUSH_TO_ARCHIVED_REPO.to_owned(),
        other => format!("some retryable failure ({other})"),
    }
}

fn main_results(corpus: &Value) -> Result<Vec<Value>, String> {
    let cases = corpus
        .get("cases")
        .and_then(Value::as_array)
        .ok_or_else(|| "corpus has no cases".to_owned())?;
    let mut results = Vec::new();
    for case in cases {
        let name = string(case, "name");
        let kind = string(case, "kind");
        let result = match kind.as_str() {
            "publication" => {
                let published = published_from_json(case.get("published").unwrap_or(&Value::Null));
                let ui = case
                    .get("ui")
                    .and_then(Value::as_str)
                    .and_then(ChangesetUiPublicationState::from_db);
                let calc = calculate_publication_state(published, ui);
                json!({
                    "name": name,
                    "kind": kind,
                    "is_published": calc.is_published(),
                    "is_draft": calc.is_draft(),
                    "is_unpublished": calc.is_unpublished(),
                })
            }
            "classify" => {
                let message = classify_message(case);
                json!({
                    "name": name,
                    "kind": kind,
                    "message": message,
                    "non_retryable": is_non_retryable(&message),
                })
            }
            "operations" => {
                let ops = operations_from_json(case.get("ops").unwrap_or(&Value::Null));
                let other = operations_from_json(case.get("other").unwrap_or(&Value::Null));
                let order: Vec<&str> = ops.execution_order().iter().map(|op| op.as_str()).collect();
                json!({
                    "name": name,
                    "kind": kind,
                    "is_none": ops.is_none(),
                    "equal": ops.equal(&other),
                    "string": ops.to_go_string(),
                    "order": order,
                })
            }
            "plan" => {
                let previous = case
                    .get("previous")
                    .filter(|v| !v.is_null())
                    .map(spec_from_json);
                let current = case
                    .get("current")
                    .filter(|v| !v.is_null())
                    .map(spec_from_json);
                let current_changeset = case
                    .get("current_changeset")
                    .filter(|v| !v.is_null())
                    .map(changeset_from_json);
                let changeset =
                    changeset_from_json(case.get("changeset").ok_or("plan case has no changeset")?);
                let reopen = reopen_after_detach(&changeset);
                match determine_plan(
                    previous.as_ref(),
                    current.as_ref(),
                    current_changeset.as_ref(),
                    &changeset,
                ) {
                    Ok(plan) => {
                        let order: Vec<&str> = plan
                            .ops
                            .execution_order()
                            .iter()
                            .map(|op| op.as_str())
                            .collect();
                        json!({
                            "name": name,
                            "kind": kind,
                            "error": Value::Null,
                            "ops": order,
                            "ops_string": plan.ops.to_go_string(),
                            "delta": delta_json(plan.delta.as_ref()),
                            "reopen": reopen,
                        })
                    }
                    Err(err) => json!({
                        "name": name,
                        "kind": kind,
                        "error": err,
                        "ops": [],
                        "ops_string": "",
                        "delta": Value::Null,
                        "reopen": reopen,
                    }),
                }
            }
            "dequeue" => {
                let mut options = Map::new();
                options.insert("name".into(), json!(RECONCILER_STORE_NAME));
                options.insert("table".into(), json!(RECONCILER_TABLE));
                options.insert("view".into(), json!(RECONCILER_VIEW));
                options.insert("state_column".into(), json!(RECONCILER_STATE_COLUMN));
                options.insert(
                    "stalled_max_age_seconds".into(),
                    json!(RECONCILER_STALLED_MAX_AGE_SECONDS),
                );
                options.insert("max_num_resets".into(), json!(RECONCILER_MAX_NUM_RESETS));
                options.insert(
                    "retry_after_seconds".into(),
                    json!(RECONCILER_RETRY_AFTER_SECONDS),
                );
                options.insert("max_num_retries".into(), json!(RECONCILER_MAX_NUM_RETRIES));
                options.insert(
                    "order_by".into(),
                    json!("changesets.reconciler_state = 'errored', changesets.updated_at DESC"),
                );
                options.insert("worker_name".into(), json!(RECONCILER_WORKER_NAME));
                options.insert(
                    "worker_description".into(),
                    json!(RECONCILER_WORKER_DESCRIPTION),
                );
                json!({"name": name, "kind": kind, "options": Value::Object(options)})
            }
            other => return Err(format!("unknown case kind {other:?}")),
        };
        results.push(result);
    }
    Ok(results)
}

fn store_results() -> Vec<Value> {
    vec![json!({
        "name": "dequeue-options",
        "kind": "dequeue",
        "options": {
            "name": RECONCILER_STORE_NAME,
            "table": RECONCILER_TABLE,
            "view": RECONCILER_VIEW,
            "state_column": RECONCILER_STATE_COLUMN,
            "stalled_max_age_seconds": RECONCILER_STALLED_MAX_AGE_SECONDS,
            "max_num_resets": RECONCILER_MAX_NUM_RESETS,
            "retry_after_seconds": RECONCILER_RETRY_AFTER_SECONDS,
            "max_num_retries": RECONCILER_MAX_NUM_RETRIES,
            "order_by": "changesets.reconciler_state = 'errored', changesets.updated_at DESC",
            "worker_name": RECONCILER_WORKER_NAME,
            "worker_description": RECONCILER_WORKER_DESCRIPTION,
        },
    })]
}

fn write_json(path: &str, value: &[Value]) -> Result<(), String> {
    let mut encoded =
        serde_json::to_string_pretty(value).map_err(|err| format!("encode output: {err}"))?;
    encoded.push('\n');
    std::fs::write(path, encoded).map_err(|err| format!("write {path}: {err}"))
}

#[test]
fn batches_reconciler_diff_harness() {
    if std::env::var("SG_M99G_DIFF").as_deref() != Ok("1") {
        return;
    }
    let corpus_path = std::env::var("M99G_CORPUS").expect("M99G_CORPUS is required");
    let out_path = std::env::var("M99G_OUT").expect("M99G_OUT is required");
    let store_out_path = std::env::var("M99G_STORE_OUT").expect("M99G_STORE_OUT is required");

    let raw = std::fs::read_to_string(&corpus_path).expect("read corpus");
    let corpus: Value = serde_json::from_str(&raw).expect("parse corpus");
    let results = main_results(&corpus).expect("harness body");
    write_json(&out_path, &results).expect("write output");
    write_json(&store_out_path, &store_results()).expect("write store output");
}

// ---------------------------------------------------------------------------
// Rust-only fake store/source tests (executor + transactional handler).
// ---------------------------------------------------------------------------

#[derive(Default)]
struct FakeStore {
    calls: Arc<Mutex<Vec<String>>>,
    external_branch_owner: Option<i64>,
    archived_repo: bool,
    done_calls: Arc<Mutex<Vec<bool>>>,
    updated: Arc<Mutex<Option<Changeset>>>,
    previous_spec: Option<ChangesetSpec>,
}

#[async_trait]
impl ReconcilerStore for FakeStore {
    async fn done(&mut self, rollback: bool) -> Result<(), String> {
        self.done_calls.lock().unwrap().push(rollback);
        Ok(())
    }

    async fn get_changeset_spec(&mut self, id: i64) -> Result<Option<ChangesetSpec>, String> {
        self.calls.lock().unwrap().push(format!("get_spec:{id}"));
        if id == 99 {
            if let Some(spec) = &self.previous_spec {
                return Ok(Some(spec.clone()));
            }
        }
        Ok(Some(test_spec(id)))
    }

    async fn get_repo(&mut self, id: i32) -> Result<Repo, String> {
        self.calls.lock().unwrap().push(format!("get_repo:{id}"));
        Ok(Repo {
            id,
            name: "github.com/acme/alpha".to_owned(),
            archived: self.archived_repo,
        })
    }

    async fn get_changeset_by_external_branch(
        &mut self,
        _external_service_type: &str,
        _repo_id: i32,
        external_branch: &str,
    ) -> Result<Option<i64>, String> {
        self.calls
            .lock()
            .unwrap()
            .push(format!("get_by_branch:{external_branch}"));
        Ok(self.external_branch_owner)
    }

    async fn batch_change_link(&mut self, _batch_change_id: i64) -> Result<String, String> {
        self.calls
            .lock()
            .unwrap()
            .push("batch_change_link".to_owned());
        Ok("[_Created by Sourcegraph batch change `acme/bc`._](https://sg/acme/bc)".to_owned())
    }

    async fn mark_repo_archived(&mut self, repo_id: i32) -> Result<(), String> {
        self.calls
            .lock()
            .unwrap()
            .push(format!("mark_archived:{repo_id}"));
        Ok(())
    }

    async fn upsert_changeset_events(&mut self, _changeset: &Changeset) -> Result<(), String> {
        self.calls.lock().unwrap().push("upsert_events".to_owned());
        Ok(())
    }

    async fn update_changeset(&mut self, changeset: &Changeset) -> Result<(), String> {
        self.calls
            .lock()
            .unwrap()
            .push("update_changeset".to_owned());
        *self.updated.lock().unwrap() = Some(changeset.clone());
        Ok(())
    }
}

struct FakeFactory {
    store: Mutex<Option<FakeStore>>,
    webhooks: Arc<Mutex<Vec<WebhookKind>>>,
}

#[async_trait]
impl ReconcilerStoreFactory for FakeFactory {
    async fn open(&self) -> Result<Box<dyn ReconcilerStore>, String> {
        Ok(Box::new(
            self.store.lock().unwrap().take().expect("single open"),
        ))
    }

    async fn enqueue_webhook(&self, kind: WebhookKind, _changeset_id: i64) -> Result<(), String> {
        self.webhooks.lock().unwrap().push(kind);
        Ok(())
    }
}

#[derive(Default)]
struct FakeSource {
    calls: Arc<Mutex<Vec<String>>>,
    fail_update: bool,
    archived_update: bool,
    fail_push: bool,
    create_exists: bool,
    outdated: bool,
}

#[async_trait]
impl ChangesetSource for FakeSource {
    async fn load_changeset(&self, _cs: &mut SourceChangeset) -> Result<(), SourceError> {
        self.calls.lock().unwrap().push("load".to_owned());
        Ok(())
    }

    async fn create_changeset(&self, _cs: &mut SourceChangeset) -> Result<bool, SourceError> {
        self.calls.lock().unwrap().push("create".to_owned());
        Ok(self.create_exists)
    }

    async fn create_draft_changeset(&self, _cs: &mut SourceChangeset) -> Result<bool, SourceError> {
        self.calls.lock().unwrap().push("create_draft".to_owned());
        Ok(self.create_exists)
    }

    async fn update_changeset(&self, cs: &mut SourceChangeset) -> Result<(), SourceError> {
        self.calls.lock().unwrap().push("update".to_owned());
        if self.archived_update {
            return Err(SourceError::archived("repository is archived"));
        }
        if self.fail_update {
            return Err(SourceError::other("boom"));
        }
        cs.changeset.external_state = Some(ChangesetExternalState::Open);
        Ok(())
    }

    async fn reopen_changeset(&self, _cs: &mut SourceChangeset) -> Result<(), SourceError> {
        self.calls.lock().unwrap().push("reopen".to_owned());
        Ok(())
    }

    async fn close_changeset(&self, _cs: &mut SourceChangeset) -> Result<(), SourceError> {
        self.calls.lock().unwrap().push("close".to_owned());
        Ok(())
    }

    async fn undraft_changeset(&self, _cs: &mut SourceChangeset) -> Result<(), SourceError> {
        self.calls.lock().unwrap().push("undraft".to_owned());
        Ok(())
    }

    fn supports_draft(&self) -> bool {
        true
    }

    async fn is_outdated(&self, _cs: &SourceChangeset) -> Result<bool, SourceError> {
        self.calls.lock().unwrap().push("is_outdated".to_owned());
        Ok(self.outdated)
    }

    async fn push_commit(
        &self,
        _target: &Repo,
        _ch: &Changeset,
        _spec: Option<&ChangesetSpec>,
        _remote: &Repo,
    ) -> Result<sg_service_worker::jobs::batches_reconciler::PushResult, SourceError> {
        self.calls.lock().unwrap().push("push".to_owned());
        if self.fail_push {
            return Err(SourceError::non_retryable(
                "creating commit from patch for repository \"r\": patch does not apply",
            ));
        }
        Ok(sg_service_worker::jobs::batches_reconciler::PushResult {
            changelist_id: Some("cl-1".to_owned()),
            rev: Some("rev".to_owned()),
            request: None,
            remote_repo: None,
        })
    }
}

struct FakeSourceFactory {
    source: Arc<FakeSource>,
}

#[async_trait]
impl ChangesetSourceFactory for FakeSourceFactory {
    async fn for_changeset(
        &self,
        _changeset: &Changeset,
        _target_repo: &Repo,
    ) -> Result<Arc<dyn ChangesetSource>, SourceError> {
        Ok(self.source.clone())
    }
}

fn test_spec(id: i64) -> ChangesetSpec {
    ChangesetSpec {
        id,
        external_id: String::new(),
        base_rev: "base".to_owned(),
        base_ref: "main".to_owned(),
        head_ref: "refs/heads/branch".to_owned(),
        title: "title".to_owned(),
        body: "body".to_owned(),
        published: PublishedValue::True,
        diff: b"diff".to_vec(),
        commit_message: "msg".to_owned(),
        commit_author_name: "name".to_owned(),
        commit_author_email: "email".to_owned(),
    }
}

fn test_changeset() -> Changeset {
    Changeset {
        id: 1,
        repo_id: 7,
        batch_changes: vec![BatchChangeAssoc {
            batch_change_id: 10,
            detach: false,
            archive: false,
            is_archived: false,
        }],
        external_id: String::new(),
        external_service_type: "github".to_owned(),
        external_branch: "refs/heads/branch".to_owned(),
        external_state: Some(ChangesetExternalState::Open),
        owned_by_batch_change_id: 10,
        current_spec_id: 100,
        previous_spec_id: 0,
        publication_state: ChangesetPublicationState::Published,
        ui_publication_state: None,
        closing: false,
        detached_at: None,
        failure_message: None,
        previous_failure_message: None,
    }
}

fn plan_with(ops: Vec<ReconcilerOperation>) -> Plan {
    Plan {
        changeset: test_changeset(),
        changeset_spec: Some(test_spec(100)),
        ops: Operations(ops),
        delta: None,
    }
}

type DoneLog = Arc<Mutex<Vec<bool>>>;
type UpdatedChangeset = Arc<Mutex<Option<Changeset>>>;

fn fake_store_handle() -> (DoneLog, UpdatedChangeset) {
    (Arc::new(Mutex::new(Vec::new())), Arc::new(Mutex::new(None)))
}

#[tokio::test]
async fn executor_close_merged_is_a_noop() {
    let (done, updated) = fake_store_handle();
    let mut store = FakeStore {
        done_calls: done,
        updated,
        ..Default::default()
    };
    let source = Arc::new(FakeSource::default());
    let factory = FakeSourceFactory {
        source: source.clone(),
    };
    let mut plan = plan_with(vec![ReconcilerOperation::Close]);
    plan.changeset.external_state = Some(ChangesetExternalState::Merged);
    plan.changeset.closing = true;

    let outcome = execute_plan(&mut store, &factory, true, &plan).await;

    assert!(outcome.result.is_ok());
    assert_eq!(outcome.after_done, None);
    assert!(!outcome.changeset.closing);
    assert!(!source.calls.lock().unwrap().contains(&"close".to_owned()));
    assert!(store
        .calls
        .lock()
        .unwrap()
        .contains(&"update_changeset".to_owned()));
}

#[tokio::test]
async fn executor_publish_creates_and_marks_published() {
    let (done, updated) = fake_store_handle();
    let mut store = FakeStore {
        done_calls: done,
        updated,
        ..Default::default()
    };
    let source = Arc::new(FakeSource::default());
    let factory = FakeSourceFactory {
        source: source.clone(),
    };
    let plan = plan_with(vec![
        ReconcilerOperation::Publish,
        ReconcilerOperation::Push,
    ]);

    let outcome = execute_plan(&mut store, &factory, true, &plan).await;

    assert!(outcome.result.is_ok(), "{:?}", outcome.result);
    assert_eq!(outcome.after_done, Some(WebhookKind::Publish));
    assert_eq!(
        outcome.changeset.publication_state,
        ChangesetPublicationState::Published
    );
    assert_eq!(outcome.changeset.external_id, "cl-1");
    assert!(source.calls.lock().unwrap().contains(&"push".to_owned()));
    assert!(source.calls.lock().unwrap().contains(&"create".to_owned()));
}

#[tokio::test]
async fn executor_publish_existing_outdated_emits_update_webhook() {
    let (done, updated) = fake_store_handle();
    let mut store = FakeStore {
        done_calls: done,
        updated,
        ..Default::default()
    };
    let source = Arc::new(FakeSource {
        create_exists: true,
        outdated: true,
        ..Default::default()
    });
    let factory = FakeSourceFactory {
        source: source.clone(),
    };
    let plan = plan_with(vec![
        ReconcilerOperation::Publish,
        ReconcilerOperation::Push,
    ]);

    let outcome = execute_plan(&mut store, &factory, true, &plan).await;

    assert!(outcome.result.is_ok(), "{:?}", outcome.result);
    assert_eq!(outcome.after_done, Some(WebhookKind::Update));
    assert!(source.calls.lock().unwrap().contains(&"update".to_owned()));
    assert_eq!(
        outcome.changeset.publication_state,
        ChangesetPublicationState::Published
    );
}

#[tokio::test]
async fn executor_push_same_branch_is_non_retryable() {
    let (done, updated) = fake_store_handle();
    let mut store = FakeStore {
        calls: Arc::new(Mutex::new(Vec::new())),
        external_branch_owner: Some(99),
        done_calls: done,
        updated,
        ..Default::default()
    };
    let source = Arc::new(FakeSource::default());
    let factory = FakeSourceFactory {
        source: source.clone(),
    };
    let plan = plan_with(vec![ReconcilerOperation::Push]);

    let outcome = execute_plan(&mut store, &factory, true, &plan).await;

    assert_eq!(outcome.after_done, Some(WebhookKind::UpdateError));
    assert!(outcome.result.unwrap_err().non_retryable);
    assert!(!source.calls.lock().unwrap().contains(&"push".to_owned()));
}

#[tokio::test]
async fn executor_push_patch_error_is_verbatim_non_retryable() {
    let (done, updated) = fake_store_handle();
    let mut store = FakeStore {
        done_calls: done,
        updated,
        ..Default::default()
    };
    let source = Arc::new(FakeSource {
        fail_push: true,
        ..Default::default()
    });
    let factory = FakeSourceFactory {
        source: source.clone(),
    };
    let plan = plan_with(vec![ReconcilerOperation::Push]);

    let outcome = execute_plan(&mut store, &factory, true, &plan).await;

    let err = outcome.result.unwrap_err();
    assert!(err.non_retryable);
    assert!(err.message.contains("patch does not apply"));
}

#[tokio::test]
async fn executor_update_archived_marks_repo_and_keeps_readonly() {
    let (done, updated) = fake_store_handle();
    let mut store = FakeStore {
        done_calls: done,
        updated,
        ..Default::default()
    };
    let source = Arc::new(FakeSource {
        archived_update: true,
        ..Default::default()
    });
    let factory = FakeSourceFactory {
        source: source.clone(),
    };
    let plan = plan_with(vec![ReconcilerOperation::Update]);

    let outcome = execute_plan(&mut store, &factory, true, &plan).await;

    assert!(outcome.result.is_ok());
    assert_eq!(outcome.after_done, Some(WebhookKind::Update));
    assert_eq!(
        outcome.changeset.external_state,
        Some(ChangesetExternalState::ReadOnly)
    );
    assert!(store
        .calls
        .lock()
        .unwrap()
        .contains(&"mark_archived:7".to_owned()));
}

#[tokio::test]
async fn handler_commits_on_success_and_enqueues_after_done() {
    let (done, updated) = fake_store_handle();
    let store = FakeStore {
        done_calls: done.clone(),
        updated,
        ..Default::default()
    };
    let webhooks = Arc::new(Mutex::new(Vec::new()));
    let source = Arc::new(FakeSource::default());
    let store_factory = Arc::new(FakeFactory {
        store: Mutex::new(Some(store)),
        webhooks: webhooks.clone(),
    });
    let handler = ReconcilerHandler::new(
        store_factory,
        Arc::new(FakeSourceFactory {
            source: source.clone(),
        }),
        true,
    );

    let mut ch = test_changeset();
    ch.publication_state = ChangesetPublicationState::Unpublished;
    // Remove the closing/ops complexity: publish + push.
    handler
        .handle(CancellationToken::new(), &record(ch))
        .await
        .expect("handler succeeds");

    assert_eq!(*done.lock().unwrap(), vec![false]);
    assert_eq!(*webhooks.lock().unwrap(), vec![WebhookKind::Publish]);
}

#[tokio::test]
async fn handler_rolls_back_on_push_error_without_triggered_webhook() {
    let (done, updated) = fake_store_handle();
    let store = FakeStore {
        done_calls: done.clone(),
        updated,
        external_branch_owner: Some(99),
        ..Default::default()
    };
    let webhooks = Arc::new(Mutex::new(Vec::new()));
    let source = Arc::new(FakeSource::default());
    let store_factory = Arc::new(FakeFactory {
        store: Mutex::new(Some(store)),
        webhooks: webhooks.clone(),
    });
    let handler = ReconcilerHandler::new(
        store_factory,
        Arc::new(FakeSourceFactory {
            source: source.clone(),
        }),
        true,
    );

    let mut ch = test_changeset();
    ch.publication_state = ChangesetPublicationState::Unpublished;
    let err = handler
        .handle(CancellationToken::new(), &record(ch))
        .await
        .expect_err("same-branch conflict fails");
    assert_eq!(err, PUBLISH_SAME_BRANCH);
    assert!(handler.non_retryable(&err));

    // `triggerUpdateWebhook` is false when the plan also publishes, so the
    // failed push has no `afterDone`; `Done` still rolls back.
    assert_eq!(*done.lock().unwrap(), vec![true]);
    assert!(webhooks.lock().unwrap().is_empty());
}

#[tokio::test]
async fn handler_rolls_back_on_update_error_and_enqueues_after_done() {
    let (done, updated) = fake_store_handle();
    let mut previous = test_spec(99);
    previous.title = "old".to_owned();
    let store = FakeStore {
        done_calls: done.clone(),
        updated,
        previous_spec: Some(previous),
        ..Default::default()
    };
    let webhooks = Arc::new(Mutex::new(Vec::new()));
    let source = Arc::new(FakeSource {
        fail_update: true,
        ..Default::default()
    });
    let store_factory = Arc::new(FakeFactory {
        store: Mutex::new(Some(store)),
        webhooks: webhooks.clone(),
    });
    let handler = ReconcilerHandler::new(
        store_factory,
        Arc::new(FakeSourceFactory {
            source: source.clone(),
        }),
        true,
    );

    let mut ch = test_changeset();
    ch.previous_spec_id = 99;
    let err = handler
        .handle(CancellationToken::new(), &record(ch))
        .await
        .expect_err("update fails");
    assert!(err.contains("updating changeset"));
    assert!(!handler.non_retryable(&err));

    assert_eq!(*done.lock().unwrap(), vec![true]);
    assert_eq!(*webhooks.lock().unwrap(), vec![WebhookKind::UpdateError]);
}

fn record(changeset: Changeset) -> sg_service_worker::jobs::batches_reconciler::ChangesetRecord {
    sg_service_worker::jobs::batches_reconciler::ChangesetRecord(changeset)
}

// Keep the `compare_changeset_specs`/`NO_DRAFT_SUPPORT` imports live for
// documentation-level checks that the differential corpus exercises through
// `determine_plan`.
#[test]
fn spec_helper_smoke() {
    let previous = test_spec(1);
    let mut current = test_spec(2);
    current.title = "other".to_owned();
    let delta = compare_changeset_specs(Some(&previous), &current, None);
    assert!(delta.title_changed);
    assert!(!NO_DRAFT_SUPPORT.is_empty());
    assert!(ReconcilerError::retryable("x").message == "x");
}
