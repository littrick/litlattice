//! M9.19c differential harness: deterministic embeddings-backend helpers.
//!
//! Replays `test_ws/p9-go/data/embeddings_backend/corpus.json` through
//! [`sg_bin_embeddings::index`]'s name functions and
//! [`sg_bin_embeddings::backend::ObjectStorageDownloader`] with a failing store,
//! writing the same JSON document as the Go oracle
//! (`internal/embeddings/zz_m919c_backend_oracle_test.go`, UNTRACKED, gated by
//! `SG_M919C_EMBEDDINGS=1`).
//!
//! Gated on `M919C_EMBEDDINGS_CORPUS`/`M919C_EMBEDDINGS_OUT`;
//! `run_embeddings_backend_diff.sh` sets them.

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::json;
use sg_bin_embeddings::backend::{
    EmbeddingIndexDownloader, IndexObjectStore, ObjectStorageDownloader,
};
use sg_bin_embeddings::index::{
    repo_embedding_index_name, repo_embedding_index_name_deprecated,
    CONTEXT_DETECTION_INDEX_NAME,
};

#[derive(Deserialize)]
struct Corpus {
    names: Vec<NameInput>,
    downloads: Vec<DownloadInput>,
}

#[derive(Deserialize)]
struct NameInput {
    repo_id: i32,
    repo_name: String,
}

#[derive(Deserialize)]
struct DownloadInput {
    name: String,
    #[serde(default)]
    primary_error: String,
    #[serde(default)]
    deprecated_error: String,
}

#[derive(Serialize)]
struct NameOutput {
    repo_id: i32,
    repo_name: String,
    index_name: String,
    deprecated_name: String,
}

#[derive(Serialize)]
struct DownloadOutput {
    name: String,
    primary_error: String,
    deprecated_error: String,
    keys: Vec<String>,
    error: String,
}

struct FailingStore {
    keys: Mutex<Vec<String>>,
    primary: String,
    primary_error: String,
    deprecated_error: String,
}

#[async_trait]
impl IndexObjectStore for FailingStore {
    async fn get(&self, key: &str) -> Result<Vec<u8>, String> {
        self.keys.lock().unwrap().push(key.to_owned());
        if key == self.primary {
            Err(self.primary_error.clone())
        } else {
            Err(self.deprecated_error.clone())
        }
    }
}

#[tokio::test]
async fn embeddings_backend_diff() {
    let (Ok(corpus_path), Ok(out_path)) = (
        std::env::var("M919C_EMBEDDINGS_CORPUS"),
        std::env::var("M919C_EMBEDDINGS_OUT"),
    ) else {
        eprintln!("M919C_EMBEDDINGS_CORPUS/M919C_EMBEDDINGS_OUT not set; skipping");
        return;
    };
    let corpus: Corpus =
        serde_json::from_str(&std::fs::read_to_string(&corpus_path).expect("read corpus"))
            .expect("parse corpus");

    let names: Vec<NameOutput> = corpus
        .names
        .iter()
        .map(|name| NameOutput {
            repo_id: name.repo_id,
            repo_name: name.repo_name.clone(),
            index_name: repo_embedding_index_name(name.repo_id),
            deprecated_name: repo_embedding_index_name_deprecated(&name.repo_name),
        })
        .collect();

    let mut downloads = Vec::new();
    for case in &corpus.downloads {
        let store = Arc::new(FailingStore {
            keys: Mutex::new(Vec::new()),
            primary: repo_embedding_index_name(1),
            primary_error: case.primary_error.clone(),
            deprecated_error: case.deprecated_error.clone(),
        });
        let downloader = ObjectStorageDownloader::new(store.clone());
        let error = match downloader.download(1, "github.com/foo/bar").await {
            Ok(_) => String::new(),
            Err(error) => error,
        };
        downloads.push(DownloadOutput {
            name: case.name.clone(),
            primary_error: case.primary_error.clone(),
            deprecated_error: case.deprecated_error.clone(),
            keys: store.keys.lock().unwrap().clone(),
            error,
        });
    }

    let document = json!({
        "context_detection": CONTEXT_DETECTION_INDEX_NAME,
        "names": names,
        "downloads": downloads,
    });
    std::fs::write(&out_path, format!("{document}\n")).expect("write output");
}
