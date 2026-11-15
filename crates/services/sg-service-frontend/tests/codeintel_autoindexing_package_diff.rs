//! M9.45c differential of `QueueAutoIndexJobsForPackage`.
//!
//! Drives [`ProductionAutoindexingJobsRuntime::queue_auto_index_jobs_for_package`]
//! — the same enqueuer + shared
//! [`DefaultPackageRepoInference`] the worker schedulers and the frontend
//! production assembly use — over the corpus emitted to
//! `/home/developer/ws/test_ws/p9-go/data/codeintel_autoindexing/m945c_package_corpus.json`.
//! The Go oracle
//! (`internal/codeintel/autoindexing/internal/enqueuer/zz_m945c_package_oracle_test.go`)
//! runs the real `inference.InferRepositoryAndRevision` and
//! `IndexEnqueuer.QueueAutoIndexJobsForPackage` with the same fake gitserver,
//! fixed job-selection inference and scratch database; the script compares one
//! canonical JSON document per case.
//!
//! Env: `SG_M945C_PACKAGE_DIFF=1`, `SG_QTEST_DSN`, `M945C_CORPUS`,
//! `M945C_OUT`.

#![allow(clippy::unwrap_used)]

use std::sync::Arc;

use async_trait::async_trait;
use serde::Deserialize;
use serde_json::{json, Value};
use sqlx::postgres::PgPoolOptions;
use sqlx::{PgPool, Row};

use sg_service_frontend::autoindexing_runtime::autoindex_config::{
    AutoIndexJobSpec, DockerStep as ConfigDockerStep,
};
use sg_service_frontend::autoindexing_runtime::jobselector::InferenceService;
use sg_service_frontend::autoindexing_runtime::{
    infer_repository_and_revision, DefaultPackageRepoInference, FileReadError, IndexEnqueuer,
    InferenceResult, JobSelector, JobSelectorSettings, ResolveRevisionError, RevisionResolver,
    SourcegraphYamlReader, VersionedPackageRepo,
};
use sg_service_frontend::autoindexing_runtime::runtime::{
    CommitCheckError, ProductionAutoindexingJobsRuntime, RepositoryGit,
};

#[derive(Debug, Deserialize)]
struct Case {
    name: String,
    kind: String,
    scheme: String,
    #[serde(rename = "package")]
    package_name: String,
    version: String,
    repository_id: i32,
}

fn load_cases(path: &str) -> Vec<Case> {
    let raw = std::fs::read(path).expect("corpus");
    let raw: serde_json::Value = serde_json::from_slice(&raw).expect("corpus json");
    raw["cases"]
        .as_array()
        .expect("cases")
        .iter()
        .map(|case| serde_json::from_value(case.clone()).expect("case"))
        .collect()
}

/// The fixed job-selection inference shared by both sides, so the differential
/// isolates mapping/lookup/resolution/enqueue semantics.
struct FixedInference;

#[async_trait]
impl InferenceService for FixedInference {
    async fn infer_index_jobs(
        &self,
        _repo: &str,
        _commit: &str,
        _override_script: &str,
    ) -> Result<InferenceResult, String> {
        Ok(InferenceResult {
            index_jobs: vec![AutoIndexJobSpec {
                steps: Some(vec![ConfigDockerStep {
                    root: String::new(),
                    image: "sourcegraph/lsif-go:latest".to_owned(),
                    commands: Some(vec!["go mod download".to_owned()]),
                }]),
                local_steps: None,
                root: String::new(),
                indexer: "sourcegraph/lsif-go:latest".to_owned(),
                indexer_args: Some(vec!["lsif-go".to_owned(), "--no-animation".to_owned()]),
                outfile: String::new(),
                requested_env_vars: None,
            }],
            inference_output: String::new(),
        })
    }
}

struct MissingYaml;

#[async_trait]
impl SourcegraphYamlReader for MissingYaml {
    async fn read(&self, _repo: &str, _commit: &str) -> Result<String, FileReadError> {
        Err(FileReadError::NotFound)
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

struct UnusedRepositoryGit;

#[async_trait]
impl RepositoryGit for UnusedRepositoryGit {
    async fn default_branch_commit(&self, _repo: &str) -> Result<(String, String), String> {
        Ok((
            "refs/heads/main".to_owned(),
            "cccccccccccccccccccccccccccccccccccccccc".to_owned(),
        ))
    }

    async fn get_commit(&self, _repo: &str, _commit: &str) -> Result<(), CommitCheckError> {
        Ok(())
    }
}

fn build_runtime(pool: PgPool) -> ProductionAutoindexingJobsRuntime {
    let settings = JobSelectorSettings {
        maximum_index_jobs_per_inferred_configuration: 50,
        override_script: String::new(),
    };
    let make_selector = || {
        JobSelector::new(
            pool.clone(),
            Arc::new(FixedInference),
            Arc::new(MissingYaml),
            settings.clone(),
        )
    };
    let enqueuer = Arc::new(IndexEnqueuer::new(
        pool.clone(),
        Arc::new(FakeRevisionResolver),
        Arc::new(DefaultPackageRepoInference),
        make_selector(),
    ));
    ProductionAutoindexingJobsRuntime::new(
        pool.clone(),
        Arc::new(UnusedRepositoryGit),
        make_selector(),
        enqueuer,
    )
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

#[tokio::test(flavor = "multi_thread")]
async fn codeintel_autoindexing_package_harness() {
    if std::env::var("SG_M945C_PACKAGE_DIFF").as_deref() != Ok("1") {
        return;
    }
    let corpus_path = std::env::var("M945C_CORPUS").expect("M945C_CORPUS");
    let out = std::env::var("M945C_OUT").expect("M945C_OUT");
    let dsn = std::env::var("SG_QTEST_DSN").expect("SG_QTEST_DSN");
    std::fs::create_dir_all(&out).expect("out dir");

    let cases = load_cases(&corpus_path);
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&dsn)
        .await
        .expect("connect");
    let runtime = build_runtime(pool.clone());

    for case in &cases {
        if case.kind != "package" {
            continue;
        }
        let package = VersionedPackageRepo {
            scheme: case.scheme.clone(),
            name: case.package_name.clone(),
            version: case.version.clone(),
        };
        let inferred = infer_repository_and_revision(&package).map(|(repository, revision)| {
            json!({ "repository": repository, "revision": revision })
        });

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

        let ok = runtime
            .queue_auto_index_jobs_for_package(&package)
            .is_ok();
        let document = json!({
            "case": case.name,
            "kind": case.kind,
            "ok": ok,
            "inferred": inferred,
            "rows": rows(&pool, case.repository_id).await,
        });

        std::fs::write(
            std::path::Path::new(&out).join(format!("{}.json", case.name)),
            serde_json::to_vec(&document).expect("encode"),
        )
        .expect("write");
    }
}
