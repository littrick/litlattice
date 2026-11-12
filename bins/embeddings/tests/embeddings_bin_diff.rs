//! Differential harness (M9.17c) for `cmd/embeddings/shared` configuration and
//! the legacy `POST /search` handler.
//!
//! Replays `test_ws/p9-go/data/embeddings_bin/corpus.json` through
//! [`sg_bin_embeddings::Config`] and [`sg_bin_embeddings::Handler`] and writes
//! one JSON line per case to `M917C_EMBEDDINGS_OUT`. The Go oracle
//! (`cmd/embeddings/shared/zz_m917c_embeddings_oracle_test.go`) replays the
//! same corpus one case per process (the baseline `internal/env` caches the
//! environment); `run_embeddings_bin_diff.sh` compares the outputs.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use serde::Deserialize;
use serde_json::{json, Value};
use sg_bin_embeddings::{Config, EmbeddingBackend, EmbeddingError, Handler, RepoEmbeddingIndex};

#[derive(Deserialize, Default)]
struct Corpus {
    #[serde(default)]
    cases: Vec<Case>,
}

#[derive(Deserialize)]
struct Case {
    name: String,
    #[serde(default)]
    env: BTreeMap<String, String>,
    query: QuerySpec,
    #[serde(default)]
    repos: HashMap<String, RepoSpec>,
    #[serde(default)]
    requests: Vec<Request>,
}

#[derive(Deserialize, Default)]
struct QuerySpec {
    #[serde(default)]
    model: String,
    #[serde(default)]
    error: Option<ErrorSpec>,
}

#[derive(Deserialize, Default, Clone)]
struct ErrorSpec {
    #[serde(default)]
    kind: Option<String>,
    #[serde(default)]
    message: String,
}

#[derive(Deserialize)]
struct RepoSpec {
    #[serde(default)]
    model: String,
    #[serde(default)]
    error: Option<ErrorSpec>,
}

#[derive(Deserialize)]
struct Request {
    method: String,
    path: String,
    #[serde(default)]
    body: String,
}

impl ErrorSpec {
    fn to_error(&self) -> EmbeddingError {
        match self.kind.as_deref() {
            Some("not_found") => EmbeddingError::not_found(self.message.clone()),
            // The baseline returns `client.RateLimitExceededError`, whose
            // Error() is always this text.
            Some("rate_limited") => EmbeddingError::rate_limited("rate limit exceeded"),
            _ => EmbeddingError::other(self.message.clone()),
        }
    }
}

struct FakeBackend {
    query: Option<ErrorSpec>,
    query_model: String,
    repos: HashMap<String, RepoSpec>,
}

impl EmbeddingBackend for FakeBackend {
    fn get_query_embedding(&self, _query: &str) -> Result<(Vec<f32>, String), EmbeddingError> {
        if let Some(error) = &self.query {
            return Err(error.to_error());
        }
        Ok((Vec::new(), self.query_model.clone()))
    }

    fn get_repo_embedding_index(
        &self,
        _repo_id: i32,
        repo_name: &str,
    ) -> Result<RepoEmbeddingIndex, EmbeddingError> {
        let Some(repo) = self.repos.get(repo_name) else {
            return Err(EmbeddingError::other(format!("no fake repo {repo_name:?}")));
        };
        if let Some(error) = &repo.error {
            return Err(error.to_error());
        }
        Ok(RepoEmbeddingIndex {
            embeddings_model: repo.model.clone(),
            repo_name: repo_name.to_owned(),
            ..RepoEmbeddingIndex::default()
        })
    }
}

#[test]
fn embeddings_bin_diff() {
    if std::env::var("SG_M917C_EMBEDDINGS_DIFF").as_deref() != Ok("1") {
        return;
    }
    let corpus_path = std::env::var("M917C_EMBEDDINGS_CORPUS").expect("M917C_EMBEDDINGS_CORPUS");
    let out_path = std::env::var("M917C_EMBEDDINGS_OUT").expect("M917C_EMBEDDINGS_OUT");
    let corpus: Corpus =
        serde_json::from_str(&std::fs::read_to_string(&corpus_path).expect("read corpus"))
            .expect("parse corpus");

    let mut lines = Vec::new();
    for case in &corpus.cases {
        let config = Config::load_from_map(&case.env).expect("valid corpus cache size");
        let validate = config.validate_error();

        let backend = Arc::new(FakeBackend {
            query: case.query.error.clone(),
            query_model: case.query.model.clone(),
            repos: case
                .repos
                .iter()
                .map(|(name, repo)| {
                    (
                        name.clone(),
                        RepoSpec {
                            model: repo.model.clone(),
                            error: repo.error.clone(),
                        },
                    )
                })
                .collect(),
        });
        let handler = Handler::new(backend);

        let responses: Vec<Value> = case
            .requests
            .iter()
            .map(|request| {
                let response = handler.handle(&request.path, &request.method, request.body.as_bytes());
                json!({
                    "status": response.status,
                    "content_type": response.content_type,
                    "body": String::from_utf8_lossy(&response.body),
                })
            })
            .collect();

        let line = json!({
            "kind": "case",
            "name": case.name,
            "config": {
                "kind": "config",
                "backend": config.backend,
                "manage_bucket": config.manage_bucket,
                "bucket": config.bucket,
                "s3_region": config.s3_region,
                "s3_endpoint": config.s3_endpoint,
                "s3_use_path_style": config.s3_use_path_style,
                "s3_access_key_id": config.s3_access_key_id,
                "s3_secret_access_key": config.s3_secret_access_key,
                "s3_session_token": config.s3_session_token,
                "gcs_project_id": config.gcs_project_id,
                "gcs_credentials_file": config.gcs_credentials_file,
                "gcs_credentials_file_contents": config.gcs_credentials_file_contents,
                "embeddings_cache_size": config.embeddings_cache_size,
                "validate": validate,
            },
            "responses": responses,
        });
        lines.push(serde_json::to_string(&line).expect("encode case"));
    }

    let mut output = lines.join("\n");
    output.push('\n');
    std::fs::write(&out_path, output).expect("write output");
}
