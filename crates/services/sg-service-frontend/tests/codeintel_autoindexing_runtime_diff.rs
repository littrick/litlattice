//! M9.44c differential of the production autoindexing runtime.
//!
//! Drives [`ProductionAutoindexingJobsRuntime`] — the same object the frontend
//! production assembly injects behind
//! `CodeIntelAutoindexingJobsRuntime` — over the corpus emitted to
//! `/home/developer/ws/test_ws/p9-go/data/codeintel_autoindexing/runtime_corpus.json`.
//! The Go oracle
//! (`internal/codeintel/autoindexing/internal/inference/zz_m944c_runtime_oracle_test.go`)
//! runs the real Lua inference service, job selector and `IndexEnqueuer` with
//! the same fake gitserver and scratch database; the script compares one
//! canonical JSON document per case.
//!
//! Env: `SG_M944C_AUTOINDEXING_RUNTIME_DIFF=1`, `SG_QTEST_DSN`,
//! `M944C_CORPUS`, `M944C_OUT`.

#![allow(clippy::unwrap_used)]

use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use serde::Deserialize;
use serde_json::{json, Value};
use sqlx::postgres::PgPoolOptions;
use sqlx::{PgPool, Row};

use sg_luasandbox::Service as LuaSandboxService;
use sg_service_frontend::autoindexing_runtime::inference_production::InferenceAdapter;
use sg_service_frontend::autoindexing_runtime::{
    ArchiveOptions, DirEntry, FileReadError, GitService, IndexEnqueuer, InferenceService,
    InferenceSettings, JobSelector, JobSelectorSettings, PackageRepoInference, RateLimiter,
    ResolveRevisionError, RevisionResolver, SourcegraphYamlReader, VersionedPackageRepo,
};
use sg_service_frontend::autoindexing_runtime::inference::SandboxService;
use sg_service_frontend::autoindexing_runtime::runtime::{
    CommitCheckError, ProductionAutoindexingJobsRuntime, RepositoryGit,
};
use sg_service_frontend::codeintel_autoindexing_graphql::CodeIntelAutoindexingJobsRuntime;

#[derive(Debug, Deserialize, Clone)]
struct Case {
    name: String,
    kind: String,
    #[serde(default)]
    repository_id: i32,
    #[serde(default)]
    commit: String,
    #[serde(default)]
    rev: String,
    #[serde(default)]
    bypass_limit: bool,
    #[serde(default)]
    limit: i64,
    #[serde(default)]
    force: bool,
    #[serde(default)]
    configuration: String,
    #[serde(default)]
    override_script: String,
    #[serde(default)]
    empty_default_branch: bool,
    #[serde(default)]
    files: BTreeMap<String, String>,
    #[serde(default)]
    override_files: BTreeMap<String, String>,
}

/// The `override` field is named `override` in the JSON corpus; serde needs the
/// rename.
#[derive(Debug, Deserialize)]
struct RawCorpus {
    cases: Vec<serde_json::Value>,
}

fn load_cases(path: &str) -> Vec<Case> {
    let raw = std::fs::read(path).expect("corpus");
    let raw: RawCorpus = serde_json::from_slice(&raw).expect("corpus json");
    raw.cases
        .into_iter()
        .map(|mut value| {
            if let Some(object) = value.as_object_mut() {
                if let Some(override_) = object.remove("override") {
                    object.insert("override_script".to_owned(), override_);
                }
            }
            serde_json::from_value(value).expect("case")
        })
        .collect()
}

/// Fake `GitService` for the inference service (same shape as the M9.8dm
/// harness): `ReadDir` lists sorted paths, `Archive` builds the in-memory tar.
struct FakeGit {
    paths: Vec<String>,
    files: BTreeMap<String, String>,
}

#[async_trait]
impl GitService for FakeGit {
    async fn resolve_revision(&self, _repo: &str, _spec: &str) -> Result<String, String> {
        Ok(String::new())
    }

    async fn read_dir(
        &self,
        _repo: &str,
        _commit: &str,
        _path: &str,
        _recurse: bool,
    ) -> Result<Vec<DirEntry>, String> {
        Ok(self
            .paths
            .iter()
            .map(|name| DirEntry {
                name: name.clone(),
                is_dir: false,
            })
            .collect())
    }

    async fn archive(&self, _repo: &str, options: ArchiveOptions) -> Result<Vec<u8>, String> {
        let mut builder = tar::Builder::new(Vec::new());
        for path in &options.paths {
            let contents = self.files.get(path).cloned().unwrap_or_default();
            let mut header = tar::Header::new_gnu();
            header.set_size(contents.len() as u64);
            header.set_mode(0o644);
            builder
                .append_data(&mut header, path, contents.as_bytes())
                .map_err(|error| error.to_string())?;
        }
        builder.into_inner().map_err(|error| error.to_string())
    }
}

struct NoopLimiter;

#[async_trait]
impl RateLimiter for NoopLimiter {
    async fn wait(&self) -> Result<(), String> {
        Ok(())
    }
}

struct MissingYaml;

#[async_trait]
impl SourcegraphYamlReader for MissingYaml {
    async fn read(&self, _repo: &str, _commit: &str) -> Result<String, FileReadError> {
        Err(FileReadError::NotFound)
    }
}

struct FakeRepositoryGit {
    empty_default_branch: bool,
}

#[async_trait]
impl RepositoryGit for FakeRepositoryGit {
    async fn default_branch_commit(&self, _repo: &str) -> Result<(String, String), String> {
        let sha = if self.empty_default_branch {
            String::new()
        } else {
            "cccccccccccccccccccccccccccccccccccccccc".to_owned()
        };
        Ok(("refs/heads/main".to_owned(), sha))
    }

    async fn get_commit(&self, _repo: &str, _commit: &str) -> Result<(), CommitCheckError> {
        Ok(())
    }
}

struct FakeRevisionResolver;

#[async_trait]
impl RevisionResolver for FakeRevisionResolver {
    async fn resolve_revision(
        &self,
        _repo: &str,
        rev: &str,
    ) -> Result<String, ResolveRevisionError> {
        if rev == "missing" {
            Err(ResolveRevisionError::NotFound)
        } else {
            Ok("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".to_owned())
        }
    }
}

struct NoPackageInference;

impl PackageRepoInference for NoPackageInference {
    fn infer_repository_and_revision(
        &self,
        _package: &VersionedPackageRepo,
    ) -> Option<(String, String)> {
        None
    }
}

/// `m944cNormalizeTimestamps`: replace `[<RFC3339>]` print timestamps.
fn normalize_timestamps(logs: &str) -> String {
    let mut logs = logs.to_owned();
    loop {
        let Some(start) = logs.find('[') else { break };
        let Some(end_rel) = logs[start..].find(']') else { break };
        let end = start + end_rel;
        let inner = &logs[start + 1..end];
        if inner.contains('T') && inner.contains('Z') {
            logs = format!("{}[<ts>]{}", &logs[..start], &logs[end + 1..]);
        } else {
            let rest = &logs[end + 1..];
            if !rest.contains('[') {
                break;
            }
            let prefix = logs[..=end].to_owned();
            logs = format!("{}{}", prefix, normalize_timestamps(rest));
            break;
        }
    }
    logs
}

fn job_views(outcome: &sg_service_frontend::codeintel_autoindexing_graphql::InferenceOutcome) -> Vec<Value> {
    let mut encoded: Vec<String> = outcome
        .jobs
        .iter()
        .map(|job| {
            let steps: Vec<Value> = job
                .steps
                .iter()
                .map(|step| {
                    json!({
                        "root": step.root,
                        "image": step.image,
                        "commands": step.commands,
                    })
                })
                .collect();
            json!({
                "steps": steps,
                "local_steps": job.local_steps,
                "root": job.root,
                "indexer": job.indexer,
                "indexer_args": job.indexer_args,
                "outfile": job.outfile,
                "requested_env_vars": job.requested_env_vars.clone().unwrap_or_default(),
            })
            .to_string()
        })
        .collect();
    encoded.sort();
    encoded
        .into_iter()
        .map(|value| serde_json::from_str(&value).expect("job json"))
        .collect()
}

async fn rows(pool: &PgPool, repository_id: i32) -> Vec<Value> {
    let rows = sqlx::query(
        "SELECT commit, repository_id, state, root, indexer,
                to_json(indexer_args)::text AS indexer_args,
                to_json(local_steps)::text AS local_steps,
                docker_steps::text AS docker_steps, outfile, should_reindex
         FROM lsif_indexes WHERE repository_id = $1
         ORDER BY root, indexer, commit",
    )
    .bind(repository_id)
    .fetch_all(pool)
    .await
    .expect("rows");
    rows.iter()
        .map(|row| {
            json!({
                "commit": row.get::<String, _>("commit"),
                "repository_id": row.get::<i32, _>("repository_id"),
                "state": row.get::<String, _>("state"),
                "root": row.get::<String, _>("root"),
                "indexer": row.get::<String, _>("indexer"),
                "indexer_args": row.get::<String, _>("indexer_args"),
                "local_steps": row.get::<String, _>("local_steps"),
                "docker_steps": row.get::<String, _>("docker_steps"),
                "outfile": row.get::<String, _>("outfile"),
                "should_reindex": row.get::<bool, _>("should_reindex"),
            })
        })
        .collect()
}

fn build_runtime(
    pool: PgPool,
    case: &Case,
) -> (ProductionAutoindexingJobsRuntime, Arc<InferenceService>) {
    let files = if case.override_files.is_empty() {
        &case.files
    } else {
        &case.override_files
    };
    let paths: Vec<String> = files.keys().cloned().collect();
    let fake_git = Arc::new(FakeGit {
        paths,
        files: files.clone(),
    });
    let sandbox: Arc<dyn SandboxService> = Arc::new(LuaSandboxService::new());
    let inference = Arc::new(InferenceService::new(
        sandbox,
        fake_git,
        Arc::new(NoopLimiter),
        InferenceSettings::default(),
    ));
    let selection: Arc<dyn sg_service_frontend::autoindexing_runtime::jobselector::InferenceService> =
        Arc::new(InferenceAdapter::new(Arc::clone(&inference)));
    let settings = JobSelectorSettings {
        maximum_index_jobs_per_inferred_configuration: if case.limit > 0 {
            case.limit
        } else {
            50
        },
        override_script: String::new(),
    };
    let make_selector = || {
        JobSelector::new(
            pool.clone(),
            Arc::clone(&selection),
            Arc::new(MissingYaml),
            settings.clone(),
        )
    };
    let enqueuer = Arc::new(IndexEnqueuer::new(
        pool.clone(),
        Arc::new(FakeRevisionResolver),
        Arc::new(NoPackageInference),
        make_selector(),
    ));
    let runtime = ProductionAutoindexingJobsRuntime::new(
        pool.clone(),
        Arc::new(FakeRepositoryGit {
            empty_default_branch: case.empty_default_branch,
        }),
        make_selector(),
        enqueuer,
    );
    (runtime, inference)
}

#[tokio::test(flavor = "multi_thread")]
async fn codeintel_autoindexing_runtime_harness() {
    if std::env::var("SG_M944C_AUTOINDEXING_RUNTIME_DIFF").as_deref() != Ok("1") {
        return;
    }
    let corpus_path = std::env::var("M944C_CORPUS").expect("M944C_CORPUS");
    let out = std::env::var("M944C_OUT").expect("M944C_OUT");
    let dsn = std::env::var("SG_QTEST_DSN").expect("SG_QTEST_DSN");
    std::fs::create_dir_all(&out).expect("out dir");

    let cases = load_cases(&corpus_path);
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&dsn)
        .await
        .expect("connect");

    for case in &cases {
        let (runtime, _inference) = build_runtime(pool.clone(), case);
        let mut document = json!({
            "case": case.name,
            "kind": case.kind,
            "ok": true,
            "jobs": Vec::<Value>::new(),
            "inference_output": "",
            "nodes": Vec::<Value>::new(),
            "rows": Vec::<Value>::new(),
        });

        match case.kind.as_str() {
            "infer" => {
                sqlx::query("DELETE FROM codeintel_inference_scripts")
                    .execute(&pool)
                    .await
                    .expect("clear script");
                let outcome = runtime.infer_index_configuration(
                    case.repository_id,
                    &case.commit,
                    &case.override_script,
                    case.bypass_limit,
                );
                match outcome {
                    Ok(outcome) => {
                        document["jobs"] = Value::Array(job_views(&outcome));
                        document["inference_output"] =
                            Value::String(normalize_timestamps(&outcome.inference_output));
                    }
                    Err(_) => document["ok"] = Value::Bool(false),
                }
            }
            "queue" => {
                sqlx::query("DELETE FROM lsif_indexes WHERE repository_id = $1")
                    .bind(case.repository_id)
                    .execute(&pool)
                    .await
                    .expect("clear indexes");
                sqlx::query("DELETE FROM lsif_uploads WHERE repository_id = $1")
                    .bind(case.repository_id)
                    .execute(&pool)
                    .await
                    .expect("clear uploads");
                let result = runtime.queue_auto_index_jobs(
                    case.repository_id,
                    &case.rev,
                    &case.configuration,
                    case.force,
                    false,
                );
                match result {
                    Ok(nodes) => {
                        document["nodes"] = Value::Array(
                            nodes
                                .iter()
                                .map(|node| {
                                    json!({
                                        "root": node.root,
                                        "indexer": node.indexer,
                                        "indexer_args": node.indexer_args,
                                    })
                                })
                                .collect(),
                        );
                    }
                    Err(_) => document["ok"] = Value::Bool(false),
                }
                document["rows"] = Value::Array(rows(&pool, case.repository_id).await);
            }
            other => panic!("unknown case kind {other}"),
        }

        std::fs::write(
            std::path::Path::new(&out).join(format!("{}.json", case.name)),
            serde_json::to_vec(&document).expect("encode"),
        )
        .expect("write");
    }
}
