#![forbid(unsafe_code)]

//! M9.8dd differential harness (Rust side): the `IndexEnqueuer` orchestration
//! (`enqueuer.go`).
//!
//! Mirrors
//! `internal/codeintel/autoindexing/internal/enqueuer/enqueuer_harness_test.go`.
//! `test_ws/p9-go/run_codeintel_autoindexing_index_enqueuer_diff.sh` applies
//! `codeintel_autoindexing_index_enqueuer_fixtures.sql` to a scratch frontend
//! database immediately before each side. Each harness then runs the same
//! per-case setup and prints one canonical `siq ` JSON line. Error text is
//! not compared (only `ok`); the line carries the inserted rows, the returned
//! jobs and the ResolveRevision calls.

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use sg_db::codeintel_autoindexing_enqueuer::AutoIndexJob;
use sg_service_worker::jobs::autoindex_config::{AutoIndexJobSpec, DockerStep as ConfigDockerStep};
use sg_service_worker::jobs::codeintel_autoindexing_enqueuer::{
    IndexEnqueuer, PackageRepoInference, ResolveRevisionError, RevisionResolver,
    VersionedPackageRepo,
};
use sg_service_worker::jobs::codeintel_autoindexing_jobselector::{
    FileReadError, InferenceResult, InferenceService, JobSelector, JobSelectorSettings,
    SourcegraphYamlReader,
};
use sqlx::postgres::PgPool;
use sqlx::Row;

const COMMIT: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

const EXPLICIT_CONFIGURATION: &str = r#"{"index_jobs":[{"root":"one","indexer":"indexer-one","steps":[{"root":"","image":"golang:1.22","commands":["go build"]}]},{"root":"two","indexer":"indexer-two"}]}"#;
const DUPLICATE_CONFIGURATION: &str = r#"{"index_jobs":[{"root":"dup-root","indexer":"dup-indexer"},{"root":"new-root","indexer":"new-indexer"}]}"#;
const EMPTY_CONFIGURATION: &str = "{}";

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
struct RowView {
    commit: String,
    repository_id: i32,
    state: String,
    root: String,
    indexer: String,
}

#[derive(serde::Serialize)]
struct ResolveView {
    repo: String,
    rev: String,
}

#[derive(serde::Serialize)]
struct Snapshot {
    case: &'static str,
    ok: bool,
    jobs: Vec<JobView>,
    rows: Vec<RowView>,
    resolves: Vec<ResolveView>,
    inference_calls: usize,
}

struct FakeInference {
    jobs: Vec<AutoIndexJobSpec>,
    calls: Mutex<usize>,
}

#[async_trait]
impl InferenceService for FakeInference {
    async fn infer_index_jobs(
        &self,
        _repo: &str,
        _commit: &str,
        _override_script: &str,
    ) -> Result<InferenceResult, String> {
        *self.calls.lock().expect("calls") += 1;
        Ok(InferenceResult {
            index_jobs: self.jobs.clone(),
            inference_output: "fake-output".to_owned(),
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

struct FakeResolver {
    calls: Mutex<Vec<(String, String)>>,
}

#[async_trait]
impl RevisionResolver for FakeResolver {
    async fn resolve_revision(
        &self,
        repo: &str,
        rev: &str,
    ) -> Result<String, ResolveRevisionError> {
        self.calls
            .lock()
            .expect("calls")
            .push((repo.to_owned(), rev.to_owned()));
        match rev {
            "missing" | "deadbeef" => Err(ResolveRevisionError::NotFound),
            "boom" | "cafebabe" => Err(ResolveRevisionError::Other("resolve failed".to_owned())),
            _ => Ok(COMMIT.to_owned()),
        }
    }
}

struct FakePackageInference {
    mapping: Option<(String, String)>,
}

impl PackageRepoInference for FakePackageInference {
    fn infer_repository_and_revision(
        &self,
        _package: &VersionedPackageRepo,
    ) -> Option<(String, String)> {
        self.mapping.clone()
    }
}

struct Case {
    name: &'static str,
    repo: i32,
    rev: &'static str,
    configuration: &'static str,
    force: bool,
    bypass: bool,
    max: i64,
    setup: &'static str,
    package: Option<VersionedPackageRepo>,
    mapping: Option<(String, String)>,
}

fn package(scheme: &str, name: &str, version: &str) -> VersionedPackageRepo {
    VersionedPackageRepo {
        scheme: scheme.to_owned(),
        name: name.to_owned(),
        version: version.to_owned(),
    }
}

fn cases() -> Vec<Case> {
    vec![
        Case {
            name: "queue-inserts-jobs",
            repo: 9721,
            rev: "main",
            configuration: EXPLICIT_CONFIGURATION,
            force: false,
            bypass: false,
            max: 50,
            setup: "",
            package: None,
            mapping: None,
        },
        Case {
            name: "queue-forced-bypasses-is-queued",
            repo: 9722,
            rev: "main",
            configuration: EXPLICIT_CONFIGURATION,
            force: true,
            bypass: false,
            max: 50,
            setup: "upload",
            package: None,
            mapping: None,
        },
        Case {
            name: "queue-not-forced-noop-when-queued",
            repo: 9722,
            rev: "main",
            configuration: EXPLICIT_CONFIGURATION,
            force: false,
            bypass: false,
            max: 50,
            setup: "upload",
            package: None,
            mapping: None,
        },
        Case {
            name: "queue-root-indexer-filters",
            repo: 9723,
            rev: "main",
            configuration: DUPLICATE_CONFIGURATION,
            force: false,
            bypass: false,
            max: 50,
            setup: "index-root",
            package: None,
            mapping: None,
        },
        Case {
            name: "queue-root-indexer-reindex-allows",
            repo: 9724,
            rev: "main",
            configuration: DUPLICATE_CONFIGURATION,
            force: false,
            bypass: false,
            max: 50,
            setup: "index-root-reindex",
            package: None,
            mapping: None,
        },
        Case {
            name: "queue-empty-selection",
            repo: 9725,
            rev: "main",
            configuration: EMPTY_CONFIGURATION,
            force: false,
            bypass: false,
            max: 50,
            setup: "",
            package: None,
            mapping: None,
        },
        Case {
            name: "queue-revision-not-found",
            repo: 9726,
            rev: "missing",
            configuration: EXPLICIT_CONFIGURATION,
            force: false,
            bypass: false,
            max: 50,
            setup: "",
            package: None,
            mapping: None,
        },
        Case {
            name: "queue-revision-error",
            repo: 9726,
            rev: "boom",
            configuration: EXPLICIT_CONFIGURATION,
            force: false,
            bypass: false,
            max: 50,
            setup: "",
            package: None,
            mapping: None,
        },
        Case {
            name: "queue-repo-not-found",
            repo: 9798,
            rev: "main",
            configuration: EXPLICIT_CONFIGURATION,
            force: false,
            bypass: false,
            max: 50,
            setup: "",
            package: None,
            mapping: None,
        },
        Case {
            name: "queue-blocked-repo",
            repo: 9730,
            rev: "main",
            configuration: EXPLICIT_CONFIGURATION,
            force: false,
            bypass: false,
            max: 50,
            setup: "",
            package: None,
            mapping: None,
        },
        Case {
            name: "queue-limit-suppresses",
            repo: 9727,
            rev: "main",
            configuration: "",
            force: false,
            bypass: false,
            max: 2,
            setup: "",
            package: None,
            mapping: None,
        },
        Case {
            name: "queue-limit-bypass",
            repo: 9727,
            rev: "main",
            configuration: "",
            force: false,
            bypass: true,
            max: 2,
            setup: "",
            package: None,
            mapping: None,
        },
        Case {
            name: "queue-package-ok",
            repo: 9728,
            rev: "",
            configuration: "",
            force: false,
            bypass: false,
            max: 50,
            setup: "",
            package: Some(package(
                "gomod",
                "https://github.com/example/enq-pkg-a",
                "v1.2.3-abcdef123456",
            )),
            mapping: Some((
                "github.com/example/enq-pkg-a".to_owned(),
                "abcdef123456".to_owned(),
            )),
        },
        Case {
            name: "queue-package-infer-none",
            repo: 9797,
            rev: "",
            configuration: "",
            force: false,
            bypass: false,
            max: 50,
            setup: "",
            package: Some(package("nonsense", "x", "1")),
            mapping: None,
        },
        Case {
            name: "queue-package-repo-missing",
            repo: 9797,
            rev: "",
            configuration: "",
            force: false,
            bypass: false,
            max: 50,
            setup: "",
            package: Some(package(
                "gomod",
                "https://github.com/example/enq-pkg-missing",
                "v1.2.3-abcdef123456",
            )),
            mapping: Some((
                "github.com/example/enq-pkg-missing".to_owned(),
                "abcdef123456".to_owned(),
            )),
        },
        Case {
            name: "queue-package-blocked",
            repo: 9730,
            rev: "",
            configuration: "",
            force: false,
            bypass: false,
            max: 50,
            setup: "",
            package: Some(package(
                "gomod",
                "https://github.com/example/enq-blocked",
                "v1.2.3-abcdef123456",
            )),
            mapping: Some((
                "github.com/example/enq-blocked".to_owned(),
                "abcdef123456".to_owned(),
            )),
        },
        Case {
            name: "queue-package-revision-not-found",
            repo: 9729,
            rev: "",
            configuration: "",
            force: false,
            bypass: false,
            max: 50,
            setup: "",
            package: Some(package(
                "gomod",
                "https://github.com/example/enq-pkg-b",
                "v9.9.9-deadbeef",
            )),
            mapping: Some((
                "github.com/example/enq-pkg-b".to_owned(),
                "deadbeef".to_owned(),
            )),
        },
        Case {
            name: "queue-package-revision-error",
            repo: 9729,
            rev: "",
            configuration: "",
            force: false,
            bypass: false,
            max: 50,
            setup: "",
            package: Some(package(
                "gomod",
                "https://github.com/example/enq-pkg-b",
                "v9.9.8-cafebabe",
            )),
            mapping: Some((
                "github.com/example/enq-pkg-b".to_owned(),
                "cafebabe".to_owned(),
            )),
        },
        Case {
            name: "queue-package-queued-noop",
            repo: 9728,
            rev: "",
            configuration: "",
            force: false,
            bypass: false,
            max: 50,
            setup: "upload",
            package: Some(package(
                "gomod",
                "https://github.com/example/enq-pkg-a",
                "v1.2.3-abcdef123456",
            )),
            mapping: Some((
                "github.com/example/enq-pkg-a".to_owned(),
                "abcdef123456".to_owned(),
            )),
        },
    ]
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

async fn exec(pool: &PgPool, query: &str) {
    sqlx::query(query).execute(pool).await.expect("setup");
}

async fn rows(pool: &PgPool, repo: i32) -> Vec<RowView> {
    sqlx::query(
        "SELECT commit, repository_id, state, root, indexer \
         FROM lsif_indexes WHERE repository_id = $1 ORDER BY id",
    )
    .bind(repo)
    .fetch_all(pool)
    .await
    .expect("query rows")
    .iter()
    .map(|row| RowView {
        commit: row.get("commit"),
        repository_id: row.get("repository_id"),
        state: row.get("state"),
        root: row.get("root"),
        indexer: row.get("indexer"),
    })
    .collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn autoindexing_index_enqueuer_matches_go_harness() {
    if std::env::var("SG_AUTOINDEXING_INDEX_ENQUEUER_DIFF")
        .ok()
        .as_deref()
        != Some("1")
    {
        return;
    }
    let dsn = std::env::var("SG_AUTOINDEXING_INDEX_ENQUEUER_TEST_DSN")
        .expect("SG_AUTOINDEXING_INDEX_ENQUEUER_TEST_DSN");
    let pool = sg_db::connect(&dsn, 10).await.expect("frontend pool");

    for case in cases() {
        let mut snapshot = Snapshot {
            case: case.name,
            ok: true,
            jobs: Vec::new(),
            rows: Vec::new(),
            resolves: Vec::new(),
            inference_calls: 0,
        };

        // Reset per-case state.
        exec(
            &pool,
            "DELETE FROM lsif_indexes WHERE repository_id BETWEEN 9720 AND 9799",
        )
        .await;
        exec(
            &pool,
            "DELETE FROM lsif_uploads WHERE repository_id BETWEEN 9720 AND 9799",
        )
        .await;
        exec(
            &pool,
            "DELETE FROM codeintel_autoindexing_exceptions WHERE repository_id BETWEEN 9720 AND 9799",
        )
        .await;
        exec(
            &pool,
            "DELETE FROM lsif_index_configuration WHERE repository_id BETWEEN 9720 AND 9799",
        )
        .await;
        exec(&pool, "DELETE FROM codeintel_inference_scripts").await;
        match case.setup {
            "upload" => {
                sqlx::query(
                    "INSERT INTO lsif_uploads \
                     (commit, repository_id, indexer, state, num_parts, uploaded_parts) \
                     VALUES ($1, $2, 'lsif-go', 'completed', 1, '{}')",
                )
                .bind(COMMIT)
                .bind(case.repo)
                .execute(&pool)
                .await
                .expect("insert upload");
            }
            "index-root" | "index-root-reindex" => {
                let should_reindex = case.setup == "index-root-reindex";
                sqlx::query(
                    "INSERT INTO lsif_indexes \
                     (commit, repository_id, state, docker_steps, local_steps, root, indexer, \
                      indexer_args, outfile, should_reindex, queued_at) \
                     VALUES ($1, $2, 'completed', '{}', '{}', 'dup-root', 'dup-indexer', \
                      '{}', '', $3, now())",
                )
                .bind(COMMIT)
                .bind(case.repo)
                .bind(should_reindex)
                .execute(&pool)
                .await
                .expect("insert index row");
            }
            _ => {}
        }

        let inference = Arc::new(FakeInference {
            jobs: inferred_specs(3),
            calls: Mutex::new(0),
        });
        let resolver = Arc::new(FakeResolver {
            calls: Mutex::new(Vec::new()),
        });
        let package_inference = Arc::new(FakePackageInference {
            mapping: case.mapping.clone(),
        });
        let selector = JobSelector::new(
            pool.clone(),
            inference.clone(),
            Arc::new(MissingYaml),
            JobSelectorSettings {
                maximum_index_jobs_per_inferred_configuration: case.max,
                override_script: String::new(),
            },
        );
        let enqueuer =
            IndexEnqueuer::new(pool.clone(), resolver.clone(), package_inference, selector);

        match &case.package {
            Some(package) => {
                if enqueuer
                    .queue_auto_index_jobs_for_package(package)
                    .await
                    .is_err()
                {
                    snapshot.ok = false;
                }
            }
            None => match enqueuer
                .queue_auto_index_jobs(
                    case.repo,
                    case.rev,
                    case.configuration,
                    case.force,
                    case.bypass,
                )
                .await
            {
                Ok(jobs) => snapshot.jobs = view_jobs(jobs),
                Err(_) => snapshot.ok = false,
            },
        }

        snapshot.inference_calls = *inference.calls.lock().expect("calls");
        snapshot.resolves = resolver
            .calls
            .lock()
            .expect("calls")
            .iter()
            .map(|(repo, rev)| ResolveView {
                repo: repo.clone(),
                rev: rev.clone(),
            })
            .collect();
        snapshot.rows = rows(&pool, case.repo).await;

        println!(
            "siq {}",
            serde_json::to_string(&snapshot).expect("json snapshot")
        );
    }
}
