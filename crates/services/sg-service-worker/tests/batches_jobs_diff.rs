//! M9.17b differential harness: `stepsForRepo` / `template.IsStaticBool`
//! static condition evaluation.
//!
//! Mirrors the UNTRACKED Go oracle
//! `internal/batches/service/zz_m917b_jobs_oracle_test.go`. Both sides consume
//! `test_ws/p9-go/data/batches_jobs/corpus.json`, write one normalized JSON
//! array, and `run_batches_jobs_diff.sh` compares them with `cmp`.
//!
//! Gated by `SG_M917B_JOBS=1`.

use serde::Deserialize;
use serde_json::{Map, Value};
use sg_service_worker::jobs::batches_workspace_resolver::{
    is_static_bool, steps_for_repo, BatchSpec, RepoRevision, Step, StepContext,
};

#[derive(Debug, Default, Deserialize)]
struct CorpusRepo {
    #[serde(default)]
    name: String,
    #[serde(default)]
    branch: String,
    #[serde(default)]
    file_matches: Vec<String>,
}

#[derive(Debug, Default, Deserialize)]
struct CorpusBatchChange {
    #[serde(default)]
    name: String,
    #[serde(default)]
    description: String,
}

#[derive(Debug, Default, Deserialize)]
struct CorpusStep {
    #[serde(default)]
    run: String,
    #[serde(default, rename = "if")]
    condition: String,
}

#[derive(Debug, Default, Deserialize)]
struct CorpusCase {
    #[serde(default)]
    op: String,
    #[serde(default)]
    condition: String,
    #[serde(default)]
    repo: CorpusRepo,
    #[serde(default)]
    batch_change: CorpusBatchChange,
    #[serde(default)]
    steps: Vec<CorpusStep>,
}

fn err_value(err: &str) -> Value {
    Value::String(err.to_owned())
}

#[test]
fn batches_jobs_diff() {
    if std::env::var("SG_M917B_JOBS").ok().as_deref() != Some("1") {
        return;
    }
    let corpus_path = std::env::var("M917B_CORPUS").expect("M917B_CORPUS");
    let out_path = std::env::var("M917B_JOBS_OUT").expect("M917B_JOBS_OUT");
    let raw = std::fs::read_to_string(&corpus_path).expect("read corpus");
    let cases: Vec<CorpusCase> = serde_json::from_str(&raw).expect("decode corpus");

    let mut results: Vec<Value> = Vec::with_capacity(cases.len());
    for case in &cases {
        let mut out = Map::new();
        out.insert("op".to_owned(), Value::String(case.op.clone()));
        let ctx = StepContext {
            repository_name: case.repo.name.clone(),
            repository_branch: case.repo.branch.clone(),
            repository_file_matches: case.repo.file_matches.clone(),
            batch_change_name: case.batch_change.name.clone(),
            batch_change_description: case.batch_change.description.clone(),
        };
        match case.op.as_str() {
            "is_static_bool" => match is_static_bool(&case.condition, &ctx) {
                Ok((is_static, value)) => {
                    out.insert("static".to_owned(), Value::Bool(is_static));
                    out.insert("value".to_owned(), Value::Bool(value));
                    out.insert("error".to_owned(), Value::Null);
                }
                Err(err) => {
                    out.insert("error".to_owned(), err_value(&err));
                }
            },
            "steps_for_repo" => {
                let spec = BatchSpec {
                    name: case.batch_change.name.clone(),
                    description: case.batch_change.description.clone(),
                    steps: case
                        .steps
                        .iter()
                        .map(|step| Step {
                            run: step.run.clone(),
                            if_condition: step.condition.clone(),
                        })
                        .collect(),
                    ..Default::default()
                };
                let repo = RepoRevision {
                    repo_name: case.repo.name.clone(),
                    branch: case.repo.branch.clone(),
                    file_matches: case.repo.file_matches.clone(),
                    ..Default::default()
                };
                match steps_for_repo(&spec, &repo) {
                    Ok(steps) => {
                        out.insert(
                            "runs".to_owned(),
                            Value::Array(
                                steps
                                    .iter()
                                    .map(|step| Value::String(step.run.clone()))
                                    .collect(),
                            ),
                        );
                        out.insert("error".to_owned(), Value::Null);
                    }
                    Err(err) => {
                        out.insert("error".to_owned(), err_value(&err));
                    }
                }
            }
            other => panic!("unknown op {other:?}"),
        }
        results.push(Value::Object(out));
    }

    let encoded = serde_json::to_string_pretty(&results).expect("encode results");
    std::fs::write(out_path, format!("{encoded}\n")).expect("write output");
}
