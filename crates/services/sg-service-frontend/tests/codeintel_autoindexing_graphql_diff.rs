//! M9.28b differential of the Code Intel autoindexing GraphQL surface.
//!
//! Both sides read the same scratch database (seeded by
//! `run_codeintel_autoindexing_graphql_diff.sh` immediately before each side
//! runs) and consume the same corpus. The Go oracle drives the production
//! `rootResolver` with the real store and a corpus-driven inference/enqueue
//! service; this harness drives the Rust resolver module with the production
//! Postgres adapter plus a corpus-driven
//! `CodeIntelAutoindexingJobsRuntime`.
//!
//! Env: `SG_M928B_CODEINTEL_AUTOINDEXING_GQL_DIFF=1`, `SG_QTEST_DSN`,
//! `M928B_CORPUS`, `M928B_OUT`.

#![allow(clippy::unwrap_used)]

use std::future::Future;
use std::path::Path;
use std::sync::Arc;

use base64::Engine as _;
use serde::Deserialize;
use serde_json::{json, Map, Value};
use sqlx::postgres::PgPoolOptions;
use sqlx::{PgPool, Row};
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;

use sg_graphql::exec::{Actor, Arguments, FieldValue, InputValue, ObjectValue};
use sg_graphql::Json;
use sg_service_frontend::codeintel_autoindexing_graphql::{
    mutation_field, repository_field, root_field, CodeIntelAutoindexingDatabase,
    CodeIntelAutoindexingHandle, InferredJobRow, InferenceOutcome,
};
use sg_service_frontend::codeintel_autoindexing_graphql_store::PgCodeIntelAutoindexingStore;
use sg_service_frontend::codeintel_uploads_graphql::{AutoIndexJobRow, DockerStep};

fn block_on<F: Future>(future: F) -> F::Output {
    match tokio::runtime::Handle::try_current() {
        Ok(handle) => tokio::task::block_in_place(|| handle.block_on(future)),
        Err(_) => tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("blocking runtime")
            .block_on(future),
    }
}

#[derive(Deserialize)]
struct Corpus {
    cases: Vec<Case>,
}

#[derive(Deserialize, Clone)]
struct InferenceCorpus {
    #[serde(default)]
    jobs: Vec<JobSpec>,
    #[serde(default)]
    inference_output: String,
}

#[derive(Deserialize, Clone)]
struct JobSpec {
    #[serde(default)]
    steps: Vec<DockerStepCorpus>,
    #[serde(default)]
    local_steps: Vec<String>,
    #[serde(default)]
    root: String,
    #[serde(default)]
    indexer: String,
    #[serde(default)]
    indexer_args: Vec<String>,
    #[serde(default)]
    outfile: String,
    #[serde(default, rename = "requestedEnvVars")]
    requested_env_vars: Option<Vec<String>>,
}

#[derive(Deserialize, Clone, serde::Serialize)]
struct DockerStepCorpus {
    #[serde(default)]
    root: String,
    #[serde(default)]
    image: String,
    #[serde(default)]
    commands: Vec<String>,
}

#[derive(Deserialize, Clone)]
struct QueueRowCorpus {
    id: i64,
    commit: String,
    queued_at: String,
    state: String,
    repository_id: i32,
    root: String,
    indexer: String,
    #[serde(default)]
    indexer_args: Vec<String>,
    #[serde(default)]
    local_steps: Vec<String>,
    #[serde(default)]
    docker_steps: Vec<DockerStepCorpus>,
    outfile: String,
    should_reindex: bool,
}

#[derive(Deserialize, Clone)]
struct Case {
    name: String,
    kind: String,
    #[serde(default = "default_actor")]
    actor: String,
    #[serde(default = "default_enabled")]
    enabled: bool,
    #[serde(default)]
    repository_id: i32,
    #[serde(default)]
    rev: String,
    #[serde(default)]
    script: String,
    #[serde(default)]
    configuration: String,
    #[serde(default)]
    inference: Option<InferenceCorpus>,
    #[serde(default)]
    inference_error: String,
    #[serde(default)]
    queue_rows: Vec<QueueRowCorpus>,
}

fn default_actor() -> String {
    "internal".to_owned()
}

fn default_enabled() -> bool {
    true
}

fn actor_for(name: &str) -> Actor {
    match name {
        "internal" => Actor::Internal,
        "admin" => Actor::User { uid: 928001 },
        "user" => Actor::User { uid: 928002 },
        _ => Actor::Anonymous,
    }
}

fn relay_id(id: i32) -> String {
    base64::engine::general_purpose::URL_SAFE.encode(format!("Repository:{id}"))
}

struct HarnessDb {
    inner: PgCodeIntelAutoindexingStore,
    pool: PgPool,
    inference: Option<InferenceOutcome>,
    inference_error: String,
    queue_rows: Vec<QueueRowCorpus>,
}

impl HarnessDb {
    fn infer_outcome(&self) -> Result<InferenceOutcome, String> {
        if !self.inference_error.is_empty() {
            return Err(self.inference_error.clone());
        }
        Ok(self.inference.clone().unwrap_or_default())
    }
}

impl CodeIntelAutoindexingDatabase for HarnessDb {
    fn check_site_admin(&self, actor: Actor) -> Result<(), String> {
        self.inner.check_site_admin(actor)
    }
    fn auto_indexing_enabled(&self) -> bool {
        self.inner.auto_indexing_enabled()
    }
    fn get_index_configuration(&self, repository_id: i32) -> Result<Option<Vec<u8>>, String> {
        self.inner.get_index_configuration(repository_id)
    }
    fn update_index_configuration(
        &self,
        repository_id: i32,
        data: &[u8],
    ) -> Result<(), String> {
        self.inner.update_index_configuration(repository_id, data)
    }
    fn get_inference_script(&self) -> Result<String, String> {
        self.inner.get_inference_script()
    }
    fn set_inference_script(&self, script: &str) -> Result<(), String> {
        self.inner.set_inference_script(script)
    }
    fn infer_index_configuration(
        &self,
        _repository_id: i32,
        _commit: &str,
        _local_override_script: &str,
        _bypass_limit: bool,
    ) -> Result<InferenceOutcome, String> {
        self.infer_outcome()
    }
    fn queue_auto_index_jobs(
        &self,
        _repository_id: i32,
        _rev: &str,
        _configuration: &str,
        _force: bool,
        _bypass_limit: bool,
    ) -> Result<Vec<AutoIndexJobRow>, String> {
        let mut rows = Vec::new();
        for row in &self.queue_rows {
            let docker_steps = serde_json::to_string(&row.docker_steps)
                .map_err(|error| error.to_string())?;
            let indexer_args = serde_json::to_string(&row.indexer_args)
                .map_err(|error| error.to_string())?;
            let local_steps =
                serde_json::to_string(&row.local_steps).map_err(|error| error.to_string())?;
            block_on(
                sqlx::query(
                    "INSERT INTO lsif_indexes (
                        id, commit, queued_at, state, repository_id, docker_steps, root,
                        indexer, indexer_args, outfile, execution_logs, local_steps,
                        should_reindex, requested_envvars)
                    VALUES ($1, $2, $3::timestamptz, 'queued', $4,
                        ARRAY(SELECT jsonb_array_elements($5::jsonb)), $6, $7,
                        ARRAY(SELECT jsonb_array_elements_text($8::jsonb)), $9,
                        ARRAY[]::jsonb[], ARRAY(SELECT jsonb_array_elements_text($10::jsonb)), $11, NULL)",
                )
                .bind(row.id)
                .bind(&row.commit)
                .bind(&row.queued_at)
                .bind(row.repository_id)
                .bind(docker_steps)
                .bind(&row.root)
                .bind(&row.indexer)
                .bind(indexer_args)
                .bind(&row.outfile)
                .bind(local_steps)
                .bind(row.should_reindex)
                .execute(&self.pool),
            )
            .map_err(|error| error.to_string())?;
            rows.push(AutoIndexJobRow {
                id: row.id,
                commit: row.commit.clone(),
                queued_at: OffsetDateTime::parse(&row.queued_at, &Rfc3339)
                    .map_err(|error| error.to_string())?,
                state: row.state.clone(),
                failure_message: None,
                started_at: None,
                finished_at: None,
                repository_id: row.repository_id,
                docker_steps: row
                    .docker_steps
                    .iter()
                    .map(|step| DockerStep {
                        root: step.root.clone(),
                        image: step.image.clone(),
                        commands: step.commands.clone(),
                    })
                    .collect(),
                root: row.root.clone(),
                indexer: row.indexer.clone(),
                indexer_args: row.indexer_args.clone(),
                outfile: row.outfile.clone(),
                execution_logs: Vec::new(),
                rank: None,
                local_steps: row.local_steps.clone(),
                associated_upload_id: None,
                should_reindex: row.should_reindex,
                requested_env_vars: Vec::new(),
            });
        }
        Ok(rows)
    }
}

fn json_to_serde(value: &Json) -> Value {
    match value {
        Json::Null => Value::Null,
        Json::Bool(value) => Value::Bool(*value),
        Json::Int(value) => Value::from(*value),
        Json::UInt(value) => Value::from(*value),
        Json::Float(value) => Value::from(*value),
        Json::Str(value) => Value::String(value.clone()),
        Json::Raw(value) => serde_json::from_str(value).unwrap_or(Value::Null),
        Json::Arr(items) => Value::Array(items.iter().map(json_to_serde).collect()),
        Json::Obj(entries) => Value::Object(
            entries
                .iter()
                .map(|(key, value)| (key.clone(), json_to_serde(value)))
                .collect(),
        ),
    }
}

fn fields_for(type_name: &str) -> Option<&'static [&'static str]> {
    match type_name {
        "IndexConfiguration" => Some(&[
            "configuration",
            "parsedConfiguration",
            "inferredConfiguration",
        ]),
        "InferredConfiguration" => Some(&["configuration", "parsedConfiguration", "limitError"]),
        "AutoIndexJobDescription" => Some(&["root", "indexer", "comparisonKey", "steps"]),
        "InferAutoIndexJobsResult" => Some(&["jobs", "inferenceOutput"]),
        "PreciseIndex" => Some(&[
            "id",
            "state",
            "inputCommit",
            "inputRoot",
            "inputIndexer",
            "failure",
            "shouldReindex",
            "isLatestForRepo",
            "queuedAt",
            "indexer",
        ]),
        "CodeIntelIndexer" => Some(&["key", "name", "url", "imageName"]),
        "IndexSteps" => Some(&["setup", "preIndex", "index", "upload", "teardown"]),
        "PreIndexStep" => Some(&["root", "image", "commands", "logEntry"]),
        "IndexStep" => Some(&[
            "commands",
            "indexerArgs",
            "outfile",
            "requestedEnvVars",
            "logEntry",
        ]),
        _ => None,
    }
}

fn error_doc(message: String) -> Value {
    json!({ "__error": message })
}

fn render_field(value: FieldValue) -> Result<Value, Value> {
    match value {
        FieldValue::Null => Ok(Value::Null),
        FieldValue::Json(value) => Ok(json_to_serde(&value)),
        FieldValue::List { items, .. } => {
            let mut rendered = Vec::with_capacity(items.len());
            for item in items {
                rendered.push(render_field(item).map_err(|error| error)?);
            }
            Ok(Value::Array(rendered))
        }
        FieldValue::Object(object) => Ok(render_object(object.as_ref())),
    }
}

fn render_object(object: &dyn ObjectValue) -> Value {
    let type_name = object.type_name();
    let Some(fields) = fields_for(type_name) else {
        return error_doc(format!("unhandled type {type_name}"));
    };
    let mut map = Map::new();
    for field in fields {
        let rendered = match object.field(field, &Arguments::default(), Actor::Internal) {
            Ok(Some(value)) => render_field(value).unwrap_or_else(|error| error),
            Ok(None) => error_doc(format!("unresolved {type_name}.{field}")),
            Err(error) => error_doc(error.message),
        };
        map.insert((*field).to_owned(), rendered);
    }
    Value::Object(map)
}

fn render_result(result: Result<Option<FieldValue>, sg_graphql::exec::ExecError>) -> Value {
    match result {
        Ok(Some(value)) => render_field(value).unwrap_or_else(|error| error),
        Ok(None) => Value::Null,
        Err(error) => json!({ "error": error.message }),
    }
}

fn job_specs(inference: &Option<InferenceCorpus>) -> Vec<InferredJobRow> {
    inference
        .as_ref()
        .map(|inference| {
            inference
                .jobs
                .iter()
                .map(|job| InferredJobRow {
                    steps: job
                        .steps
                        .iter()
                        .map(|step| DockerStep {
                            root: step.root.clone(),
                            image: step.image.clone(),
                            commands: step.commands.clone(),
                        })
                        .collect(),
                    local_steps: job.local_steps.clone(),
                    root: job.root.clone(),
                    indexer: job.indexer.clone(),
                    indexer_args: job.indexer_args.clone(),
                    outfile: job.outfile.clone(),
                    requested_env_vars: job.requested_env_vars.clone(),
                })
                .collect()
        })
        .unwrap_or_default()
}

fn arguments(entries: Vec<(&str, InputValue)>) -> Arguments {
    Arguments::from_entries(
        entries
            .into_iter()
            .map(|(name, value)| (name.to_owned(), value))
            .collect(),
    )
}

fn config_snapshot(pool: &PgPool) -> Value {
    let rows = block_on(
        sqlx::query(
            "SELECT repository_id, data::text AS data
             FROM lsif_index_configuration
             WHERE repository_id IN (92800, 92801)
             ORDER BY repository_id",
        )
        .fetch_all(pool),
    )
    .expect("config snapshot");
    Value::Array(
        rows.iter()
            .map(|row| {
                json!({
                    "repositoryId": row.get::<i32, _>("repository_id"),
                    "data": row.get::<String, _>("data"),
                })
            })
            .collect(),
    )
}

fn script_snapshot(pool: &PgPool) -> Value {
    let row = block_on(
        sqlx::query(
            "SELECT script FROM codeintel_inference_scripts
             ORDER BY insert_timestamp DESC LIMIT 1",
        )
        .fetch_optional(pool),
    )
    .expect("script snapshot");
    match row {
        Some(row) => Value::String(row.get::<String, _>("script")),
        None => Value::Null,
    }
}

fn index_snapshot(pool: &PgPool) -> Value {
    let rows = block_on(
        sqlx::query(
            "SELECT id, commit, state, repository_id, docker_steps::text AS docker_steps,
                    root, indexer, to_json(indexer_args)::text AS indexer_args, outfile,
                    to_json(local_steps)::text AS local_steps, should_reindex
             FROM lsif_indexes
             WHERE id BETWEEN 928000 AND 928099
             ORDER BY id",
        )
        .fetch_all(pool),
    )
    .expect("index snapshot");
    Value::Array(
        rows.iter()
            .map(|row| {
                json!({
                    "id": row.get::<i64, _>("id"),
                    "commit": row.get::<String, _>("commit"),
                    "state": row.get::<String, _>("state"),
                    "repositoryId": row.get::<i32, _>("repository_id"),
                    "dockerSteps": row.get::<String, _>("docker_steps"),
                    "root": row.get::<String, _>("root"),
                    "indexer": row.get::<String, _>("indexer"),
                    "indexerArgs": row.get::<String, _>("indexer_args"),
                    "outfile": row.get::<String, _>("outfile"),
                    "localSteps": row.get::<String, _>("local_steps"),
                    "shouldReindex": row.get::<bool, _>("should_reindex"),
                })
            })
            .collect(),
    )
}

#[tokio::test(flavor = "multi_thread")]
async fn codeintel_autoindexing_graphql_harness() {
    if std::env::var("SG_M928B_CODEINTEL_AUTOINDEXING_GQL_DIFF").as_deref() != Ok("1") {
        return;
    }
    let corpus_path = std::env::var("M928B_CORPUS").expect("M928B_CORPUS");
    let out = std::env::var("M928B_OUT").expect("M928B_OUT");
    let dsn = std::env::var("SG_QTEST_DSN").expect("SG_QTEST_DSN");
    std::fs::create_dir_all(&out).expect("out dir");
    let corpus: Corpus = serde_json::from_slice(&std::fs::read(&corpus_path).expect("corpus"))
        .expect("corpus json");

    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&dsn)
        .await
        .expect("connect");

    for case in &corpus.cases {
        let enabled = case.enabled;
        let inner = PgCodeIntelAutoindexingStore::new(
            pool.clone(),
            Arc::new(move || enabled),
            None,
        );
        let db = Arc::new(HarnessDb {
            inner,
            pool: pool.clone(),
            inference: Some(InferenceOutcome {
                jobs: job_specs(&case.inference),
                inference_output: case
                    .inference
                    .as_ref()
                    .map(|inference| inference.inference_output.clone())
                    .unwrap_or_default(),
            }),
            inference_error: case.inference_error.clone(),
            queue_rows: case.queue_rows.clone(),
        });
        let handle: CodeIntelAutoindexingHandle = db.clone();
        let actor = actor_for(&case.actor);
        let repository = relay_id(case.repository_id);

        let document = match case.kind.as_str() {
            "config_read" => render_result(repository_field(
                &handle,
                case.repository_id,
                "indexConfiguration",
                &Arguments::default(),
                actor,
            )),
            "update_config" => {
                let args = arguments(vec![
                    ("repository", InputValue::Str(repository)),
                    ("configuration", InputValue::Str(case.configuration.clone())),
                ]);
                let result =
                    mutation_field(&handle, "updateRepositoryIndexConfiguration", &args, actor);
                let mut document = Map::new();
                document.insert("config".to_owned(), config_snapshot(&pool));
                match result {
                    Ok(_) => {
                        document.insert("result".to_owned(), Value::String("ok".to_owned()));
                    }
                    Err(error) => {
                        document.insert("error".to_owned(), Value::String(error.message));
                    }
                }
                Value::Object(document)
            }
            "get_script" => match root_field(
                &handle,
                "codeIntelligenceInferenceScript",
                &Arguments::default(),
                actor,
            ) {
                Ok(Some(FieldValue::Json(Json::Str(script)))) => json!({ "script": script }),
                Ok(Some(value)) => json!({ "script": render_field(value).unwrap_or(Value::Null) }),
                Ok(None) => Value::Null,
                Err(error) => json!({ "error": error.message }),
            },
            "update_script" => {
                let args = arguments(vec![
                    ("script", InputValue::Str(case.script.clone())),
                ]);
                let result = mutation_field(
                    &handle,
                    "updateCodeIntelligenceInferenceScript",
                    &args,
                    actor,
                );
                let mut document = Map::new();
                document.insert("script".to_owned(), script_snapshot(&pool));
                match result {
                    Ok(_) => {
                        document.insert("result".to_owned(), Value::String("ok".to_owned()));
                    }
                    Err(error) => {
                        document.insert("error".to_owned(), Value::String(error.message));
                    }
                }
                Value::Object(document)
            }
            "infer" => {
                let args = arguments(vec![
                    ("repository", InputValue::Str(repository)),
                    ("rev", InputValue::Str(case.rev.clone())),
                    ("script", InputValue::Str(case.script.clone())),
                ]);
                render_result(root_field(
                    &handle,
                    "inferAutoIndexJobsForRepo",
                    &args,
                    actor,
                ))
            }
            "queue" => {
                let args = arguments(vec![
                    ("repository", InputValue::Str(repository)),
                    ("rev", InputValue::Str(case.rev.clone())),
                ]);
                let result = mutation_field(&handle, "queueAutoIndexJobsForRepo", &args, actor);
                let mut document = Map::new();
                document.insert("rows".to_owned(), index_snapshot(&pool));
                match result {
                    Ok(Some(value)) => {
                        document.insert(
                            "nodes".to_owned(),
                            render_field(value).unwrap_or_else(|error| error),
                        );
                    }
                    Ok(None) => {
                        document.insert("nodes".to_owned(), Value::Null);
                    }
                    Err(error) => {
                        document.insert("error".to_owned(), Value::String(error.message));
                    }
                }
                Value::Object(document)
            }
            other => panic!("unknown case kind {other}"),
        };

        let body = serde_json::to_string(&document).expect("serialize");
        std::fs::write(Path::new(&out).join(format!("{}.json", case.name)), &body)
            .expect("write");
        println!("RUST {} len={}", case.name, body.len());
    }
}
