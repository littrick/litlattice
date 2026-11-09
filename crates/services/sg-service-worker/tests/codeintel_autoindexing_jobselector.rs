#![forbid(unsafe_code)]

//! M9.8dc differential harness (Rust side): the job selector's precedence
//! orchestration (`job_selector.go`).
//!
//! Mirrors
//! `internal/codeintel/autoindexing/internal/jobselector/job_selector_harness_test.go`.
//! `test_ws/p9-go/run_codeintel_autoindexing_jobselector_diff.sh` applies
//! `codeintel_autoindexing_jobselector_fixtures.sql` to a scratch frontend
//! database immediately before each side. Each harness then runs the same
//! per-case setup and prints one canonical `sjs ` JSON line. Error text is
//! not compared (only `ok`), because each side reports its own wording.

use std::sync::Mutex;

use async_trait::async_trait;
use sg_db::codeintel_autoindexing_config::get_inference_script;
use sg_db::codeintel_autoindexing_enqueuer::AutoIndexJob;
use sg_service_worker::jobs::autoindex_config::{AutoIndexJobSpec, DockerStep as ConfigDockerStep};
use sg_service_worker::jobs::codeintel_autoindexing_jobselector::{
    make_queued_jobs, FileReadError, InferenceResult, InferenceService, JobSelector,
    JobSelectorSettings, SourcegraphYamlReader,
};
use sqlx::postgres::PgPool;

const COMMIT: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

const EXPLICIT_CONFIGURATION: &str = r#"{"index_jobs":[{"steps":[{"root":"explicit/","image":"golang:1.21","commands":["make"]}],"local_steps":["true"],"root":"exp","indexer":"explsif","indexer_args":["-x"],"outfile":"exp.dump","requestedEnvVars":["EXPLICIT"]},{"indexer":"second"}]}"#;
const DB_CONFIGURATION: &str = r#"{"index_jobs":[{"root":"db-root","indexer":"db-indexer","steps":[{"root":"db/","image":"golang:1.22","commands":["go build"]}]}]}"#;
const YAML_CONFIGURATION: &str = "index_jobs:\n  - root: yaml-root\n    indexer: yaml-indexer\n    steps:\n      - image: golang:1.22\n        commands:\n          - go build\n";
const BROKEN_CONFIGURATION: &str = r#"{"index_jobs": ["#;
const BROKEN_YAML: &str = "index_jobs: \"not a sequence\"\n";

#[derive(serde::Serialize)]
struct StepView {
    root: String,
    image: String,
    commands: Vec<String>,
}

#[derive(serde::Serialize)]
struct JobView {
    commit: String,
    repository_id: i32,
    state: String,
    docker_steps: Vec<StepView>,
    local_steps: Vec<String>,
    root: String,
    indexer: String,
    indexer_args: Vec<String>,
    outfile: String,
    requested_env_vars: Option<Vec<String>>,
}

#[derive(serde::Serialize)]
struct Snapshot {
    case: &'static str,
    ok: bool,
    jobs: Vec<JobView>,
    result_nil: bool,
    inference_calls: usize,
    script_kind: &'static str,
    inference_repo: String,
    inference_commit: String,
    inference_output: String,
}

struct FakeInference {
    jobs: Vec<AutoIndexJobSpec>,
    error: Option<String>,
    calls: Mutex<Vec<(String, String, String)>>,
}

impl FakeInference {
    fn new(jobs: Vec<AutoIndexJobSpec>, error: Option<String>) -> Self {
        Self {
            jobs,
            error,
            calls: Mutex::new(Vec::new()),
        }
    }
}

#[async_trait]
impl InferenceService for FakeInference {
    async fn infer_index_jobs(
        &self,
        repo: &str,
        commit: &str,
        override_script: &str,
    ) -> Result<InferenceResult, String> {
        self.calls.lock().expect("calls").push((
            repo.to_owned(),
            commit.to_owned(),
            override_script.to_owned(),
        ));
        if let Some(error) = &self.error {
            return Err(error.clone());
        }
        Ok(InferenceResult {
            index_jobs: self.jobs.clone(),
            inference_output: "fake-output".to_owned(),
        })
    }
}

enum YamlResponse {
    NotFound,
    Other(String),
    Content(String),
}

struct FakeYaml {
    response: YamlResponse,
}

#[async_trait]
impl SourcegraphYamlReader for FakeYaml {
    async fn read(&self, _repo: &str, _commit: &str) -> Result<String, FileReadError> {
        match &self.response {
            YamlResponse::NotFound => Err(FileReadError::NotFound),
            YamlResponse::Other(error) => Err(FileReadError::Other(error.clone())),
            YamlResponse::Content(content) => Ok(content.clone()),
        }
    }
}

struct Case {
    name: &'static str,
    repo: i32,
    configuration: &'static str,
    bypass: bool,
    max: i64,
    db_config: &'static str,
    exceptions: &'static str,
    yaml: YamlResponse,
    infer_jobs: usize,
    infer_err: bool,
    direct: bool,
    local: &'static str,
    db_script: &'static str,
}

fn not_found() -> YamlResponse {
    YamlResponse::NotFound
}

fn file(content: &str) -> YamlResponse {
    YamlResponse::Content(content.to_owned())
}

fn inferred_specs(n: usize) -> Vec<AutoIndexJobSpec> {
    (0..n)
        .map(|i| AutoIndexJobSpec {
            steps: Some(vec![ConfigDockerStep {
                root: format!("root-{i}/"),
                image: "golang:1.22".to_owned(),
                commands: Some(vec!["go build ./...".to_owned()]),
            }]),
            local_steps: Some(vec!["go mod download".to_owned()]),
            root: format!("module-{i}"),
            indexer: format!("scip-go-{i}"),
            indexer_args: Some(vec!["--verbose".to_owned()]),
            outfile: format!("dump-{i}.scip"),
            requested_env_vars: Some(vec!["GOFLAGS".to_owned()]),
        })
        .collect()
}

fn cases() -> Vec<Case> {
    vec![
        Case {
            name: "schedule-disabled",
            repo: 9701,
            configuration: "",
            bypass: false,
            max: 50,
            db_config: "",
            exceptions: "schedule",
            yaml: not_found(),
            infer_jobs: 5,
            infer_err: false,
            direct: false,
            local: "",
            db_script: "",
        },
        Case {
            name: "explicit-config-wins",
            repo: 9701,
            configuration: EXPLICIT_CONFIGURATION,
            bypass: false,
            max: 50,
            db_config: DB_CONFIGURATION,
            exceptions: "",
            yaml: file(YAML_CONFIGURATION),
            infer_jobs: 5,
            infer_err: false,
            direct: false,
            local: "",
            db_script: "",
        },
        Case {
            name: "explicit-config-broken",
            repo: 9701,
            configuration: BROKEN_CONFIGURATION,
            bypass: false,
            max: 50,
            db_config: DB_CONFIGURATION,
            exceptions: "",
            yaml: file(YAML_CONFIGURATION),
            infer_jobs: 5,
            infer_err: false,
            direct: false,
            local: "",
            db_script: "",
        },
        Case {
            name: "explicit-config-empty-object",
            repo: 9701,
            configuration: "{}",
            bypass: false,
            max: 50,
            db_config: DB_CONFIGURATION,
            exceptions: "",
            yaml: file(YAML_CONFIGURATION),
            infer_jobs: 5,
            infer_err: false,
            direct: false,
            local: "",
            db_script: "",
        },
        Case {
            name: "db-config-wins",
            repo: 9701,
            configuration: "",
            bypass: false,
            max: 50,
            db_config: DB_CONFIGURATION,
            exceptions: "",
            yaml: file(YAML_CONFIGURATION),
            infer_jobs: 5,
            infer_err: false,
            direct: false,
            local: "",
            db_script: "",
        },
        Case {
            name: "db-config-broken",
            repo: 9701,
            configuration: "",
            bypass: false,
            max: 50,
            db_config: BROKEN_CONFIGURATION,
            exceptions: "",
            yaml: file(YAML_CONFIGURATION),
            infer_jobs: 5,
            infer_err: false,
            direct: false,
            local: "",
            db_script: "",
        },
        Case {
            name: "db-config-empty-list",
            repo: 9702,
            configuration: "",
            bypass: false,
            max: 50,
            db_config: r#"{"index_jobs":[]}"#,
            exceptions: "",
            yaml: file(YAML_CONFIGURATION),
            infer_jobs: 5,
            infer_err: false,
            direct: false,
            local: "",
            db_script: "",
        },
        Case {
            name: "repo-yaml-wins",
            repo: 9702,
            configuration: "",
            bypass: false,
            max: 50,
            db_config: "",
            exceptions: "",
            yaml: file(YAML_CONFIGURATION),
            infer_jobs: 5,
            infer_err: false,
            direct: false,
            local: "",
            db_script: "",
        },
        Case {
            name: "repo-yaml-missing",
            repo: 9703,
            configuration: "",
            bypass: false,
            max: 50,
            db_config: "",
            exceptions: "",
            yaml: not_found(),
            infer_jobs: 3,
            infer_err: false,
            direct: false,
            local: "",
            db_script: "",
        },
        Case {
            name: "repo-yaml-broken",
            repo: 9703,
            configuration: "",
            bypass: false,
            max: 50,
            db_config: "",
            exceptions: "",
            yaml: file(BROKEN_YAML),
            infer_jobs: 3,
            infer_err: false,
            direct: false,
            local: "",
            db_script: "",
        },
        Case {
            name: "repo-yaml-empty",
            repo: 9704,
            configuration: "",
            bypass: false,
            max: 50,
            db_config: "",
            exceptions: "",
            yaml: file(""),
            infer_jobs: 3,
            infer_err: false,
            direct: false,
            local: "",
            db_script: "",
        },
        Case {
            name: "repo-yaml-read-error",
            repo: 9704,
            configuration: "",
            bypass: false,
            max: 50,
            db_config: "",
            exceptions: "",
            yaml: YamlResponse::Other("gitserver read failed".to_owned()),
            infer_jobs: 3,
            infer_err: false,
            direct: false,
            local: "",
            db_script: "",
        },
        Case {
            name: "inference-empty",
            repo: 9705,
            configuration: "",
            bypass: false,
            max: 50,
            db_config: "",
            exceptions: "",
            yaml: not_found(),
            infer_jobs: 0,
            infer_err: false,
            direct: false,
            local: "",
            db_script: "",
        },
        Case {
            name: "inference-over-limit",
            repo: 9705,
            configuration: "",
            bypass: false,
            max: 2,
            db_config: "",
            exceptions: "",
            yaml: not_found(),
            infer_jobs: 3,
            infer_err: false,
            direct: false,
            local: "",
            db_script: "",
        },
        Case {
            name: "inference-over-limit-bypass",
            repo: 9705,
            configuration: "",
            bypass: true,
            max: 2,
            db_config: "",
            exceptions: "",
            yaml: not_found(),
            infer_jobs: 3,
            infer_err: false,
            direct: false,
            local: "",
            db_script: "",
        },
        Case {
            name: "inference-at-limit",
            repo: 9706,
            configuration: "",
            bypass: false,
            max: 2,
            db_config: "",
            exceptions: "",
            yaml: not_found(),
            infer_jobs: 2,
            infer_err: false,
            direct: false,
            local: "",
            db_script: "",
        },
        Case {
            name: "inference-error",
            repo: 9706,
            configuration: "",
            bypass: false,
            max: 50,
            db_config: "",
            exceptions: "",
            yaml: not_found(),
            infer_jobs: 0,
            infer_err: true,
            direct: false,
            local: "",
            db_script: "",
        },
        Case {
            name: "repo-not-found",
            repo: 9799,
            configuration: "",
            bypass: false,
            max: 50,
            db_config: "",
            exceptions: "",
            yaml: not_found(),
            infer_jobs: 3,
            infer_err: false,
            direct: false,
            local: "",
            db_script: "",
        },
        Case {
            name: "blocked-repo",
            repo: 9710,
            configuration: "",
            bypass: false,
            max: 50,
            db_config: "",
            exceptions: "",
            yaml: not_found(),
            infer_jobs: 3,
            infer_err: false,
            direct: false,
            local: "",
            db_script: "",
        },
        Case {
            name: "infer-db-script",
            repo: 9707,
            configuration: "",
            bypass: false,
            max: 50,
            db_config: "",
            exceptions: "",
            yaml: not_found(),
            infer_jobs: 1,
            infer_err: false,
            direct: true,
            local: "",
            db_script: "db-script",
        },
        Case {
            name: "infer-local-override",
            repo: 9707,
            configuration: "",
            bypass: false,
            max: 50,
            db_config: "",
            exceptions: "",
            yaml: not_found(),
            infer_jobs: 1,
            infer_err: false,
            direct: true,
            local: "local-script",
            db_script: "db-script",
        },
        Case {
            name: "infer-default-script",
            repo: 9708,
            configuration: "",
            bypass: false,
            max: 50,
            db_config: "",
            exceptions: "",
            yaml: not_found(),
            infer_jobs: 1,
            infer_err: false,
            direct: true,
            local: "",
            db_script: "",
        },
        Case {
            name: "infer-disabled-direct",
            repo: 9709,
            configuration: "",
            bypass: false,
            max: 50,
            db_config: "",
            exceptions: "infer",
            yaml: not_found(),
            infer_jobs: 1,
            infer_err: false,
            direct: true,
            local: "",
            db_script: "",
        },
        Case {
            name: "infer-over-limit-direct",
            repo: 9709,
            configuration: "",
            bypass: false,
            max: 2,
            db_config: "",
            exceptions: "",
            yaml: not_found(),
            infer_jobs: 3,
            infer_err: false,
            direct: true,
            local: "",
            db_script: "",
        },
        Case {
            name: "infer-over-limit-bypass-direct",
            repo: 9709,
            configuration: "",
            bypass: true,
            max: 2,
            db_config: "",
            exceptions: "",
            yaml: not_found(),
            infer_jobs: 3,
            infer_err: false,
            direct: true,
            local: "",
            db_script: "",
        },
        Case {
            name: "infer-error-direct",
            repo: 9709,
            configuration: "",
            bypass: false,
            max: 50,
            db_config: "",
            exceptions: "",
            yaml: not_found(),
            infer_jobs: 0,
            infer_err: true,
            direct: true,
            local: "",
            db_script: "",
        },
    ]
}

fn classify_script(script: &str, store_script: &str) -> &'static str {
    match script {
        "" => "",
        "db-script" => "db-script",
        "local-script" => "local-script",
        _ if script == store_script => "store-default",
        _ => "other",
    }
}

fn view_jobs(jobs: Vec<AutoIndexJob>) -> Vec<JobView> {
    jobs.into_iter()
        .map(|job| JobView {
            commit: job.commit,
            repository_id: job.repository_id,
            state: job.state,
            docker_steps: job
                .docker_steps
                .into_iter()
                .map(|step| StepView {
                    root: step.root,
                    image: step.image,
                    commands: step.commands,
                })
                .collect(),
            local_steps: job.local_steps,
            root: job.root,
            indexer: job.indexer,
            indexer_args: job.indexer_args,
            outfile: job.outfile,
            requested_env_vars: job.requested_env_vars,
        })
        .collect()
}

async fn exec(pool: &PgPool, query: &str, value: i32) {
    sqlx::query(query)
        .bind(value)
        .execute(pool)
        .await
        .expect("setup");
}

#[tokio::test(flavor = "multi_thread")]
async fn autoindexing_job_selector_matches_go_harness() {
    if std::env::var("SG_AUTOINDEXING_JOB_SELECTOR_DIFF")
        .ok()
        .as_deref()
        != Some("1")
    {
        return;
    }
    let dsn = std::env::var("SG_AUTOINDEXING_JOB_SELECTOR_TEST_DSN")
        .expect("SG_AUTOINDEXING_JOB_SELECTOR_TEST_DSN");
    let pool = sg_db::connect(&dsn, 10).await.expect("frontend pool");

    for case in cases() {
        let mut snapshot = Snapshot {
            case: case.name,
            ok: true,
            jobs: Vec::new(),
            result_nil: false,
            inference_calls: 0,
            script_kind: "",
            inference_repo: String::new(),
            inference_commit: String::new(),
            inference_output: String::new(),
        };

        // Reset per-case state.
        exec(
            &pool,
            "DELETE FROM codeintel_autoindexing_exceptions WHERE repository_id = $1",
            case.repo,
        )
        .await;
        exec(
            &pool,
            "DELETE FROM lsif_index_configuration WHERE repository_id = $1",
            case.repo,
        )
        .await;
        sqlx::query("DELETE FROM codeintel_inference_scripts")
            .execute(&pool)
            .await
            .expect("delete scripts");
        match case.exceptions {
            "schedule" => {
                sqlx::query(
                    "INSERT INTO codeintel_autoindexing_exceptions \
                     (repository_id, disable_scheduling, disable_inference) VALUES ($1, true, false)",
                )
                .bind(case.repo)
                .execute(&pool)
                .await
                .expect("insert exceptions");
            }
            "infer" => {
                sqlx::query(
                    "INSERT INTO codeintel_autoindexing_exceptions \
                     (repository_id, disable_scheduling, disable_inference) VALUES ($1, false, true)",
                )
                .bind(case.repo)
                .execute(&pool)
                .await
                .expect("insert exceptions");
            }
            _ => {}
        }
        if !case.db_config.is_empty() {
            sqlx::query(
                "INSERT INTO lsif_index_configuration (repository_id, data) \
                 VALUES ($1, convert_to($2, 'UTF8'))",
            )
            .bind(case.repo)
            .bind(case.db_config)
            .execute(&pool)
            .await
            .expect("insert configuration");
        }
        if !case.db_script.is_empty() {
            sqlx::query("INSERT INTO codeintel_inference_scripts (script) VALUES ($1)")
                .bind(case.db_script)
                .execute(&pool)
                .await
                .expect("insert script");
        }
        let store_script = get_inference_script(&pool).await.expect("store script");

        let inference = std::sync::Arc::new(FakeInference::new(
            inferred_specs(case.infer_jobs),
            if case.infer_err {
                Some("inference exploded".to_owned())
            } else {
                None
            },
        ));
        let yaml = std::sync::Arc::new(FakeYaml {
            response: case.yaml,
        });
        let selector = JobSelector::new(
            pool.clone(),
            inference.clone(),
            yaml,
            JobSelectorSettings {
                maximum_index_jobs_per_inferred_configuration: case.max,
                override_script: String::new(),
            },
        );

        if case.direct {
            match selector
                .infer_index_jobs_from_repository_structure(
                    case.repo,
                    COMMIT,
                    case.local,
                    case.bypass,
                )
                .await
            {
                Ok(None) => snapshot.result_nil = true,
                Ok(Some(result)) => {
                    snapshot.inference_output = result.inference_output.clone();
                    snapshot.jobs = view_jobs(make_queued_jobs(
                        Some(&result.index_jobs),
                        case.repo,
                        COMMIT,
                    ));
                }
                Err(_) => snapshot.ok = false,
            }
        } else {
            match selector
                .get_jobs(case.repo, COMMIT, case.configuration, case.bypass)
                .await
            {
                Ok(jobs) => snapshot.jobs = view_jobs(jobs),
                Err(_) => snapshot.ok = false,
            }
        }

        let calls = inference.calls.lock().expect("calls");
        snapshot.inference_calls = calls.len();
        if let Some((repo, commit, script)) = calls.last() {
            snapshot.script_kind = classify_script(script, &store_script);
            snapshot.inference_repo = repo.clone();
            snapshot.inference_commit = commit.clone();
        }
        drop(calls);

        println!(
            "sjs {}",
            serde_json::to_string(&snapshot).expect("json snapshot")
        );
    }
}
