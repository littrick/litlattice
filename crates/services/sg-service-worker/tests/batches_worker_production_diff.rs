#![forbid(unsafe_code)]

//! M9.26b differential harness: the production reconciler wiring.
//!
//! Drives [`ReconcilerHandler`] over the real Postgres store
//! ([`PgReconcilerStoreFactory`]) and the real `outbound_webhook_jobs` queue,
//! with an injected fake [`ChangesetSource`] (the source factory itself is
//! covered by the M9.24b/M9.25a source differentials). The handler runs the
//! same `process`/`done`/`afterDone` sequence as the worker:
//!
//! 1. load the queued changeset and its specs from `changesets` /
//!    `changeset_specs`,
//! 2. `determine_plan` + `execute_plan`,
//! 3. commit/rollback,
//! 4. enqueue the `afterDone` webhook through
//!    `sg_extsvc::batches_webhooks::enqueue_changeset` into
//!    `outbound_webhook_jobs`.
//!
//! The untracked Go oracle
//! `internal/batches/reconciler/zz_m926b_worker_production_oracle_test.go`
//! runs `(*Reconciler).process` + `afterDone` over the same database with the
//! baseline `FakeChangesetSource` and `httpcli.InternalDoer` mock. Both sides
//! emit the selected persisted changeset columns, the handler error (and its
//! non-retryable classification), and the enqueued webhook rows.
//!
//! `changeset_events` is deliberately not compared: `UpsertChangesetEvents`
//! remains a documented seam (`ReconcilerStore::upsert_changeset_events` is a
//! no-op because Rust does not derive code-host events from metadata yet).
//!
//! Gated by `SG_M926B_DIFF=1`.

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use serde_json::{json, Value};
use sqlx::{PgPool, Row};

use sg_db::OffsetDateTime;
use sg_service_worker::framework::dbworker::DbWorkerHandler;
use sg_service_worker::jobs::batches_reconciler::{
    BatchChangeAssoc, Changeset, ChangesetExternalState, ChangesetPublicationState,
    ChangesetRecord, ChangesetSource, ChangesetSourceFactory,
    ChangesetUiPublicationState, OutboundWebhookQueueSink, PgReconcilerStoreFactory, PushResult,
    ReconcilerHandler, Repo, SourceChangeset, SourceError, WebhookKind,
};
use sg_service_worker::jobs::outbound_webhooks::store::WebhookJobQueue;
use sg_extsvc::{HttpDoer, HttpRequest, HttpResponse};
use tokio_util::sync::CancellationToken;

/// The canned internal GraphQL response used by both sides (the Go oracle
/// installs the same bytes as `httpcli.InternalDoer`).
const GQL_RESPONSE: &str = r#"{"data": {"node": {"id": "123","externalID": "123","batchChanges": {"nodes": [{"id": "123"}]},"repository": {"id": "123","name": "github.com/test/test"},"createdAt": "2023-02-25T00:53:50Z","updatedAt": "2023-02-25T00:53:50Z","title": "title","body": "body","author": {"name": "author", "email": "email"},"state": "OPEN","labels": [],"externalURL": {"url": "some-url"},"forkNamespace": null,"reviewState": "PENDING","checkState": null,"error": null,"syncerError": null,"forkName": null,"ownedByBatchChange": null}}}"#;

struct FakeHttp;

#[async_trait]
impl HttpDoer for FakeHttp {
    async fn do_request(&self, _request: HttpRequest) -> Result<HttpResponse, String> {
        Ok(HttpResponse {
            status: 200,
            headers: Vec::new(),
            body: GQL_RESPONSE.as_bytes().to_vec(),
        })
    }
}

#[derive(Clone, Default)]
struct FakeSourceConfig {
    exists: bool,
    outdated: bool,
    fail: bool,
    external_id: String,
}

struct FakeSource {
    config: FakeSourceConfig,
    calls: Arc<Mutex<Vec<String>>>,
}

impl FakeSource {
    fn record(&self, name: &str) {
        self.calls.lock().expect("calls lock").push(name.to_owned());
    }

    fn set_metadata(&self, cs: &mut SourceChangeset) {
        cs.changeset.external_id = self.config.external_id.clone();
        cs.changeset.external_branch = "refs/heads/feature".to_owned();
        cs.changeset.external_state = Some(ChangesetExternalState::Open);
    }

    fn fail(&self) -> Result<(), SourceError> {
        if self.config.fail {
            Err(SourceError::other("boom"))
        } else {
            Ok(())
        }
    }
}

#[async_trait]
impl ChangesetSource for FakeSource {
    async fn load_changeset(&self, cs: &mut SourceChangeset) -> Result<(), SourceError> {
        self.record("load");
        self.fail()?;
        self.set_metadata(cs);
        Ok(())
    }

    async fn create_changeset(&self, cs: &mut SourceChangeset) -> Result<bool, SourceError> {
        self.record("create");
        self.fail()?;
        self.set_metadata(cs);
        Ok(self.config.exists)
    }

    async fn create_draft_changeset(&self, cs: &mut SourceChangeset) -> Result<bool, SourceError> {
        self.record("create_draft");
        self.fail()?;
        self.set_metadata(cs);
        Ok(self.config.exists)
    }

    async fn update_changeset(&self, cs: &mut SourceChangeset) -> Result<(), SourceError> {
        self.record("update");
        self.fail()?;
        self.set_metadata(cs);
        Ok(())
    }

    async fn reopen_changeset(&self, cs: &mut SourceChangeset) -> Result<(), SourceError> {
        self.record("reopen");
        self.fail()?;
        self.set_metadata(cs);
        Ok(())
    }

    async fn close_changeset(&self, cs: &mut SourceChangeset) -> Result<(), SourceError> {
        self.record("close");
        self.fail()?;
        self.set_metadata(cs);
        Ok(())
    }

    async fn undraft_changeset(&self, cs: &mut SourceChangeset) -> Result<(), SourceError> {
        self.record("undraft");
        self.fail()?;
        self.set_metadata(cs);
        Ok(())
    }

    fn supports_draft(&self) -> bool {
        true
    }

    async fn is_outdated(&self, _cs: &SourceChangeset) -> Result<bool, SourceError> {
        self.record("is_outdated");
        Ok(self.config.outdated)
    }

    async fn push_commit(
        &self,
        _target: &Repo,
        _ch: &Changeset,
        _spec: Option<&sg_service_worker::jobs::batches_reconciler::ChangesetSpec>,
        _remote: &Repo,
    ) -> Result<PushResult, SourceError> {
        self.record("push");
        // Pushing does not go through `FakeChangesetSource.Err` in the Go
        // oracle, so failure cases fail at the mutation that uses the source.
        Ok(PushResult {
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

fn string(value: &Value, key: &str) -> String {
    value
        .get(key)
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_owned()
}

fn boolean(value: &Value, key: &str) -> bool {
    value.get(key).and_then(Value::as_bool).unwrap_or(false)
}

fn id(value: &Value, key: &str) -> i64 {
    value.get(key).and_then(Value::as_i64).unwrap_or(0)
}

/// Loads the queued changeset in the shape `ChangesetWorkerStore::dequeue`
/// hands to the handler.
async fn load_changeset(pool: &PgPool, changeset_id: i64) -> Changeset {
    let row = sqlx::query(
        r#"SELECT id, repo_id, batch_change_ids, external_id, external_service_type,
                  external_branch, external_state, owned_by_batch_change_id,
                  current_spec_id, previous_spec_id, publication_state,
                  ui_publication_state, closing, detached_at, failure_message,
                  previous_failure_message
           FROM changesets WHERE id = $1"#,
    )
    .bind(changeset_id)
    .fetch_one(pool)
    .await
    .expect("load fixture changeset");

    let batch_changes: Value = row.try_get("batch_change_ids").expect("batch_change_ids");
    let batch_changes = batch_changes
        .as_object()
        .map(|object| {
            object
                .keys()
                .filter_map(|key| key.parse::<i64>().ok())
                .map(|batch_change_id| BatchChangeAssoc {
                    batch_change_id,
                    detach: false,
                    archive: false,
                    is_archived: false,
                })
                .collect()
        })
        .unwrap_or_default();

    let external_state: Option<String> = row.try_get("external_state").expect("external_state");
    let ui_state: Option<String> = row
        .try_get("ui_publication_state")
        .expect("ui_publication_state");
    let publication_state: String = row
        .try_get("publication_state")
        .expect("publication_state");
    let detached_at: Option<OffsetDateTime> = row.try_get("detached_at").expect("detached_at");

    Changeset {
        id: changeset_id,
        repo_id: row.try_get::<i32, _>("repo_id").expect("repo_id"),
        batch_changes,
        external_id: row
            .try_get::<Option<String>, _>("external_id")
            .expect("external_id")
            .unwrap_or_default(),
        external_service_type: row
            .try_get("external_service_type")
            .expect("external_service_type"),
        external_branch: row
            .try_get::<Option<String>, _>("external_branch")
            .expect("external_branch")
            .unwrap_or_default(),
        external_state: external_state
            .as_deref()
            .and_then(ChangesetExternalState::from_db),
        owned_by_batch_change_id: row
            .try_get::<Option<i64>, _>("owned_by_batch_change_id")
            .expect("owned_by_batch_change_id")
            .unwrap_or_default(),
        current_spec_id: row
            .try_get::<Option<i64>, _>("current_spec_id")
            .expect("current_spec_id")
            .unwrap_or_default(),
        previous_spec_id: row
            .try_get::<Option<i64>, _>("previous_spec_id")
            .expect("previous_spec_id")
            .unwrap_or_default(),
        publication_state: if publication_state == "PUBLISHED" {
            ChangesetPublicationState::Published
        } else {
            ChangesetPublicationState::Unpublished
        },
        ui_publication_state: ui_state
            .as_deref()
            .and_then(ChangesetUiPublicationState::from_db),
        closing: row.try_get("closing").expect("closing"),
        detached_at,
        failure_message: row
            .try_get::<Option<String>, _>("failure_message")
            .expect("failure_message"),
        previous_failure_message: row
            .try_get::<Option<String>, _>("previous_failure_message")
            .expect("previous_failure_message"),
    }
}

/// The persisted columns both implementations write (title/body/metadata and
/// `changeset_events` are documented divergences).
async fn row_json(pool: &PgPool, changeset_id: i64) -> Value {
    let row = sqlx::query(
        r#"SELECT publication_state, external_state, external_id, external_branch,
                  closing, failure_message, previous_failure_message,
                  ui_publication_state, batch_change_ids
           FROM changesets WHERE id = $1"#,
    )
    .bind(changeset_id)
    .fetch_one(pool)
    .await
    .expect("load persisted changeset");

    let batch_changes: Value = row.try_get("batch_change_ids").expect("batch_change_ids");
    // Association ids are allocated by each side; only the count is
    // comparable.
    let batch_change_count = batch_changes
        .as_object()
        .map(|object| object.len())
        .unwrap_or_default();

    // Match the Go oracle's `optional` helper: empty strings reload as nil.
    let optional = |name: &str| -> Value {
        match row
            .try_get::<Option<String>, _>(name)
            .expect("optional column")
        {
            Some(value) if !value.is_empty() => Value::String(value),
            _ => Value::Null,
        }
    };

    json!({
        "publication_state": row.try_get::<String, _>("publication_state").expect("publication_state"),
        "external_state": optional("external_state"),
        "external_id": optional("external_id"),
        "external_branch": optional("external_branch"),
        "closing": row.try_get::<bool, _>("closing").expect("closing"),
        "failure_message": optional("failure_message"),
        "previous_failure_message": optional("previous_failure_message"),
        "ui_publication_state": optional("ui_publication_state"),
        "batch_change_count": batch_change_count,
    })
}

/// The `outbound_webhook_jobs` rows written while handling one case.
async fn webhook_json(pool: &PgPool, after_id: i64) -> (i64, Value) {
    let rows = sqlx::query(
        r#"SELECT id, event_type, scope, payload FROM outbound_webhook_jobs
           WHERE id > $1 ORDER BY id ASC"#,
    )
    .bind(after_id)
    .fetch_all(pool)
    .await
    .expect("load outbound webhook jobs");

    let mut last_id = after_id;
    let mut jobs = Vec::new();
    for row in rows {
        last_id = row.try_get::<i64, _>("id").expect("webhook id");
        let scope: Option<String> = row.try_get("scope").expect("webhook scope");
        let payload: Vec<u8> = row.try_get("payload").expect("webhook payload");
        let payload = serde_json::from_slice::<Value>(&payload).unwrap_or(Value::Null);
        jobs.push(json!({
            "event_type": row.try_get::<String, _>("event_type").expect("event_type"),
            "scope": scope,
            "payload": payload,
        }));
    }
    (last_id, Value::Array(jobs))
}

#[tokio::test(flavor = "current_thread")]
async fn batches_worker_production_diff() {
    if std::env::var("SG_M926B_DIFF").as_deref() != Ok("1") {
        return;
    }
    let corpus_path = std::env::var("M926B_CORPUS").expect("M926B_CORPUS is required");
    let out_path = std::env::var("M926B_OUT").expect("M926B_OUT is required");
    let dsn = std::env::var("SG_QTEST_DSN").expect("SG_QTEST_DSN is required");

    let raw = std::fs::read_to_string(&corpus_path).expect("read corpus");
    let corpus: Value = serde_json::from_str(&raw).expect("parse corpus");
    let cases = corpus
        .get("cases")
        .and_then(Value::as_array)
        .expect("corpus cases");

    let pool = PgPool::connect(&dsn).await.expect("connect database");
    let queue = Arc::new(WebhookJobQueue::new(pool.clone(), None));
    let store_factory = Arc::new(PgReconcilerStoreFactory::new(
        pool.clone(),
        "https://sourcegraph.test".to_owned(),
        Arc::new(OutboundWebhookQueueSink::new(queue)),
        Arc::new(FakeHttp),
        "https://sourcegraph.test/.internal".to_owned(),
    ));

    let mut last_webhook_id: i64 = sqlx::query_scalar("SELECT COALESCE(MAX(id), 0) FROM outbound_webhook_jobs")
        .fetch_one(&pool)
        .await
        .expect("max webhook id");

    // The runner re-seeds the fixtures and invokes this harness once per case:
    // the database must contain a single changeset of this repository, like the
    // Go oracle's per-subtest database. `M926B_ONLY` selects that case.
    let only = std::env::var("M926B_ONLY").ok();
    let mut results = Vec::new();
    for case in cases {
        let name = string(case, "name");
        if only.as_deref().is_some_and(|only| only != name) {
            continue;
        }
        let changeset_id = id(case, "changeset_id");
        let config = FakeSourceConfig {
            exists: boolean(case, "exists"),
            outdated: boolean(case, "outdated"),
            fail: boolean(case, "fail"),
            external_id: string(case, "external_id"),
        };
        let source = Arc::new(FakeSource {
            config,
            calls: Arc::new(Mutex::new(Vec::new())),
        });
        let handler = ReconcilerHandler::new(
            store_factory.clone(),
            Arc::new(FakeSourceFactory {
                source: source.clone(),
            }),
            true,
        );

        let changeset = load_changeset(&pool, changeset_id).await;
        let result = handler
            .handle(CancellationToken::new(), &ChangesetRecord(changeset))
            .await;

        let (next_webhook_id, webhooks) = webhook_json(&pool, last_webhook_id).await;
        last_webhook_id = next_webhook_id;

        let (error, non_retryable) = match &result {
            Ok(()) => (Value::Null, false),
            Err(err) => (Value::String(err.clone()), handler.non_retryable(err)),
        };

        results.push(json!({
            "name": name,
            "row": row_json(&pool, changeset_id).await,
            "webhooks": webhooks,
            "error": error,
            "non_retryable": non_retryable,
        }));
    }

    let mut encoded = serde_json::to_string_pretty(&results).expect("encode output");
    encoded.push('\n');
    std::fs::write(&out_path, encoded).expect("write output");

    // Keep the event seam explicit: nothing above compares `changeset_events`.
    let _ = WebhookKind::Update;
}
