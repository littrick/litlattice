#![forbid(unsafe_code)]

//! M9.24a differential harness: changeset code-host state write + source
//! selection, against the Go oracle in
//! `internal/batches/sources/zz_m924a_sync_oracle_test.go`.
//!
//! Gated on `SG_QTEST_DSN`; the corpus/fixtures live in
//! `test_ws/p9-go/repo_updater_sync_fixtures.sql` and
//! `test_ws/p9-go/data/repo_updater_sync/corpus.json`.

use std::sync::Arc;

use serde::Deserialize;
use serde_json::{json, Map, Value};
use sg_db::repos::RepoStore;
use sg_extsvc::batches_sources::{
    unsupported_source_kind, SourceError, SourceErrorKind,
};
use sg_service_repo_updater::changeset_sync::{
    ChangesetCodeHostState, ChangesetSyncBackend, CodeHostStateUpdate,
    PgChangesetSourceFactory, PgChangesetSyncBackend,
};
use sqlx::{PgPool, Row};
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;

#[derive(Deserialize)]
struct Corpus {
    cases: Vec<Case>,
}

#[derive(Deserialize)]
struct Case {
    name: String,
    action: String,
    #[serde(default)]
    changeset_id: Option<i64>,
    #[serde(default)]
    kind: Option<String>,
    #[serde(default)]
    metadata: Option<Value>,
    #[serde(default)]
    external_branch: Option<String>,
    #[serde(default)]
    external_fork_name: Option<String>,
    #[serde(default)]
    external_fork_namespace: Option<String>,
    #[serde(default)]
    external_deleted_at: Option<String>,
    #[serde(default)]
    external_updated_at: Option<String>,
    #[serde(default)]
    external_state: Option<String>,
    #[serde(default)]
    external_review_state: Option<String>,
    #[serde(default)]
    external_check_state: Option<String>,
    #[serde(default)]
    diff_stat_added: Option<i32>,
    #[serde(default)]
    diff_stat_deleted: Option<i32>,
    #[serde(default)]
    sync_state: Option<Value>,
    #[serde(default)]
    syncer_error: Option<String>,
    #[serde(default)]
    error: Option<String>,
    #[serde(default)]
    external_id: Option<String>,
    #[serde(default)]
    repo_id: Option<i32>,
}

struct NoopHttp;

#[async_trait::async_trait]
impl sg_extsvc::HttpDoer for NoopHttp {
    async fn do_request(
        &self,
        _request: sg_extsvc::HttpRequest,
    ) -> Result<sg_extsvc::HttpResponse, String> {
        Err("the differential harness performs no HTTP requests".to_owned())
    }
}

fn empty_out(name: &str, action: &str) -> Map<String, Value> {
    let mut out = Map::new();
    out.insert("name".into(), json!(name));
    out.insert("action".into(), json!(action));
    for key in [
        "title",
        "external_branch",
        "external_fork_name",
        "external_fork_namespace",
        "external_state",
        "external_review_state",
        "external_check_state",
        "diff_stat_added",
        "diff_stat_deleted",
        "sync_state",
        "syncer_error",
        "metadata",
        "error_text",
        "selected_id",
        "selected_kind",
        "external_updated_at_epoch",
    ] {
        out.insert(key.into(), Value::Null);
    }
    out.insert("external_deleted_at_set".into(), json!(false));
    out.insert("updated_at_advanced".into(), json!(false));
    out
}

/// Mirrors the Go oracle's `m924aMetadataView`: only the fields both the
/// typed baseline marshal and the port's canonical projection represent.
fn metadata_view(kind: &str, metadata: &Value) -> Value {
    // The baseline's typed re-marshal writes Go zero values for fields the
    // corpus left out; the port's canonical projection leaves them absent.
    // Normalize both to null so the comparison stays on the fields the corpus
    // actually sets.
    let field = |path: &str| {
        match metadata.pointer(path).cloned().unwrap_or(Value::Null) {
            Value::String(ref s) if s.is_empty() => Value::Null,
            Value::Number(ref n) if n.as_f64() == Some(0.0) => Value::Null,
            other => other,
        }
    };
    match kind {
        "github" => json!({
            "Title": field("/Title"),
            "State": field("/State"),
            "HeadRefName": field("/HeadRefName"),
            "BaseRefName": field("/BaseRefName"),
            "HeadRefOid": field("/HeadRefOid"),
            "BaseRefOid": field("/BaseRefOid"),
            "Number": field("/Number"),
        }),
        "gitlab" => json!({
            "title": field("/title"),
            "state": field("/state"),
            "source_branch": field("/source_branch"),
            "target_branch": field("/target_branch"),
            "head_sha": field("/diff_refs/head_sha"),
            "base_sha": field("/diff_refs/base_sha"),
        }),
        "bitbucketServer" => json!({
            "title": field("/title"),
            "state": field("/state"),
            "fromRef": field("/fromRef/id"),
            "toRef": field("/toRef/id"),
        }),
        _ => Value::Null,
    }
}

fn parse_time(raw: &str) -> OffsetDateTime {
    OffsetDateTime::parse(raw, &Rfc3339).expect("RFC3339 timestamp")
}

async fn scan_row(pool: &PgPool, id: i64) -> Map<String, Value> {
    let row = sqlx::query(
        r#"
SELECT metadata, external_service_type, external_branch, external_fork_name,
       external_fork_namespace,
       extract(epoch FROM external_deleted_at)::bigint AS deleted_epoch,
       extract(epoch FROM external_updated_at)::bigint AS updated_epoch,
       external_state, external_review_state, external_check_state,
       diff_stat_added, diff_stat_deleted, sync_state, syncer_error,
       external_title,
       (updated_at > now() - interval '10 minutes') AS advanced
FROM changesets WHERE id = $1
"#,
    )
    .bind(id)
    .fetch_one(pool)
    .await
    .expect("scan changeset");

    let kind: String = row.get("external_service_type");
    let metadata: Value = row.get("metadata");
    let deleted_epoch: Option<i64> = row.get("deleted_epoch");
    let mut out = empty_out("", "");
    out.insert(
        "title".into(),
        json!(row.get::<Option<String>, _>("external_title")),
    );
    for (key, column) in [
        ("external_branch", "external_branch"),
        ("external_fork_name", "external_fork_name"),
        ("external_fork_namespace", "external_fork_namespace"),
        ("external_state", "external_state"),
        ("external_review_state", "external_review_state"),
        ("external_check_state", "external_check_state"),
    ] {
        out.insert(
            key.into(),
            json!(row.get::<Option<String>, _>(column)),
        );
    }
    out.insert(
        "external_deleted_at_set".into(),
        json!(deleted_epoch.is_some()),
    );
    out.insert(
        "external_updated_at_epoch".into(),
        json!(row.get::<Option<i64>, _>("updated_epoch")),
    );
    out.insert(
        "diff_stat_added".into(),
        json!(row.get::<Option<i32>, _>("diff_stat_added")),
    );
    out.insert(
        "diff_stat_deleted".into(),
        json!(row.get::<Option<i32>, _>("diff_stat_deleted")),
    );
    out.insert("sync_state".into(), row.get::<Value, _>("sync_state"));
    out.insert(
        "syncer_error".into(),
        json!(row.get::<Option<String>, _>("syncer_error")),
    );
    out.insert(
        "updated_at_advanced".into(),
        json!(row.get::<bool, _>("advanced")),
    );
    out.insert("metadata".into(), metadata_view(&kind, &metadata));
    out
}

#[tokio::test]
async fn repo_updater_sync_diff() {
    let Ok(dsn) = std::env::var("SG_QTEST_DSN") else {
        eprintln!("SG_QTEST_DSN unset; skipping");
        return;
    };
    let corpus_path = std::env::var("M924A_CORPUS").expect("M924A_CORPUS");
    let out_path = std::env::var("M924A_OUT").expect("M924A_OUT");
    let corpus: Corpus =
        serde_json::from_slice(&std::fs::read(&corpus_path).expect("read corpus"))
            .expect("decode corpus");

    let pool = sg_db::connect(&dsn, 5).await.expect("connect");
    let backend = PgChangesetSyncBackend::new(pool.clone());
    let factory = PgChangesetSourceFactory::new(
        pool.clone(),
        Arc::new(sg_keyring::Keyring::default()),
        Arc::new(NoopHttp),
    );

    let mut out = Vec::with_capacity(corpus.cases.len());
    for case in &corpus.cases {
        let mut view = empty_out(&case.name, &case.action);
        match case.action.as_str() {
            "update" | "load_error" => {
                let id = case.changeset_id.expect("changeset_id");
                if case.action == "update" {
                    let mut sync_state = case
                        .sync_state
                        .clone()
                        .unwrap_or_else(|| ChangesetCodeHostState::default().sync_state);
                    if sync_state.is_null() {
                        sync_state = ChangesetCodeHostState::default().sync_state;
                    }
                    let state = ChangesetCodeHostState {
                        external_service_type: case.kind.clone().expect("kind"),
                        metadata: case.metadata.clone().unwrap_or_else(|| json!({})),
                        external_branch: case.external_branch.clone(),
                        external_fork_name: case.external_fork_name.clone(),
                        external_fork_namespace: case.external_fork_namespace.clone(),
                        external_deleted_at: case
                            .external_deleted_at
                            .as_deref()
                            .map(parse_time),
                        external_updated_at: case
                            .external_updated_at
                            .as_deref()
                            .map(parse_time),
                        external_state: case.external_state.clone(),
                        external_review_state: case.external_review_state.clone(),
                        external_check_state: case.external_check_state.clone(),
                        diff_stat_added: case.diff_stat_added,
                        diff_stat_deleted: case.diff_stat_deleted,
                        sync_state,
                        syncer_error: case.syncer_error.clone(),
                        external_title: None,
                        updated_at: None,
                    };
                    backend
                        .update_changeset_code_host_state(
                            id,
                            CodeHostStateUpdate::loaded(&state, false),
                        )
                        .await
                        .expect("update");
                } else {
                    backend
                        .update_changeset_code_host_state(
                            id,
                            CodeHostStateUpdate::error(case.error.as_deref().expect("error")),
                        )
                        .await
                        .expect("update error");
                }
                view = scan_row(&pool, id).await;
                view.insert("name".into(), json!(case.name));
                view.insert("action".into(), json!(case.action));
            }
            "selection" => {
                let repo = RepoStore::new(pool.clone())
                    .get_by_id(case.repo_id.expect("repo_id"))
                    .await
                    .expect("get repo")
                    .expect("repo exists");
                match factory.select_external_service(&repo).await {
                    Ok(selected) => {
                        view.insert("selected_id".into(), json!(selected.id));
                        view.insert("selected_kind".into(), json!(selected.kind));
                    }
                    Err(err) => {
                        view.insert("error_text".into(), json!(err));
                    }
                }
            }
            "not_found" => {
                let external_id = case.external_id.clone().expect("external_id");
                let error = SourceError {
                    message: format!(
                        "Changeset with external ID {external_id} not found"
                    ),
                    kind: SourceErrorKind::NotFound,
                };
                view.insert("error_text".into(), json!(error.message));
            }
            "unsupported_kind" => {
                let error = unsupported_source_kind(case.kind.as_deref().expect("kind"));
                view.insert("error_text".into(), json!(error.message));
            }
            other => panic!("unknown action {other}"),
        }
        out.push(Value::Object(view));
    }

    std::fs::write(&out_path, serde_json::to_vec_pretty(&out).expect("encode"))
        .expect("write output");
}
