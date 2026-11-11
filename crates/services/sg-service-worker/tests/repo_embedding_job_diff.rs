#![forbid(unsafe_code)]

//! M9.8io differential harness (Rust side): the repo-embedding job's embedding
//! core and incremental index update.
//!
//! Mirrors the untracked Go oracle
//! `cmd/worker/internal/embeddings/repo/zz_m9io_oracle_test.go`. Both sides
//! read the same corpus and emit canonical JSON for:
//!
//!   - chunk boundaries (`split_into_embeddable_chunks`),
//!   - compiled include/exclude filters (`get_file_filter_path_patterns`),
//!   - `embed_repo` batches, progress snapshots, skip stats, index rows and
//!     error paths (real algorithms, fake gitserver/read-lister, fake
//!     embeddings client),
//!   - `update_repo_embedding_index` (fake object storage; the uploaded bytes
//!     are decoded back through a `get` of the primary key),
//!   - index names (`repo_embedding_index_name{,_deprecated}`).
//!
//! Only runs when `SG_M9IO_DIFF=1`; reads `M9IO_CORPUS` and writes `M9IO_OUT`.
//! `test_ws/p9-go/run_repo_embedding_job_diff.sh` compares the two files after
//! normalizing JSON.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use serde::Deserialize;
use serde_json::{json, Map, Value};
use sg_service_worker::jobs::exhaustive_search_service::ObjectStorageBackend;
use sg_service_worker::jobs::repo_embedding_job::{
    compile_glob_patterns, download_repo_embedding_index, embed_repo,
    get_file_filter_path_patterns, is_excluded_file_path_match, is_included_file_path_match,
    repo_embedding_index_name, repo_embedding_index_name_deprecated, split_into_embeddable_chunks,
    split_options, update_repo_embedding_index, ContextService, DefaultContextService,
    EmbedFilesStats, EmbedRepoOpts, EmbedRepoStats, EmbeddableChunk, EmbeddingIndex,
    EmbeddingsClient, EmbeddingsClientError, EmbeddingsJobConfig, EmbeddingsResults, FileEntry,
    FileFilters, FileReadLister, RepoEmbeddingIndex, RepoEmbeddingRowMetadata, RepoPathRanks,
    RepoRecord,
};
use tokio_util::sync::CancellationToken;

// ---------------------------------------------------------------------------
// Corpus
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize, Default)]
struct Corpus {
    #[serde(default, rename = "splitCases")]
    split_cases: Vec<SplitCase>,
    #[serde(default, rename = "filterCases")]
    filter_cases: Vec<FilterCase>,
    #[serde(default, rename = "embedCases")]
    embed_cases: Vec<EmbedCase>,
    #[serde(default, rename = "updateCases")]
    update_cases: Vec<UpdateCase>,
    #[serde(default, rename = "nameCases")]
    name_cases: Vec<NameCase>,
}

#[derive(Debug, Deserialize, Default)]
struct SplitCase {
    #[serde(default)]
    name: String,
    #[serde(default, rename = "fileName")]
    file_name: String,
    #[serde(default)]
    text: String,
}

#[derive(Debug, Deserialize, Default)]
struct FilterCase {
    #[serde(default)]
    name: String,
    #[serde(default)]
    include: Vec<String>,
    #[serde(default)]
    exclude: Vec<String>,
    #[serde(default)]
    paths: Vec<String>,
}

#[derive(Debug, Clone, Deserialize, Default)]
struct CaseFile {
    #[serde(default)]
    name: String,
    #[serde(default)]
    size: Option<i64>,
    #[serde(default)]
    content: String,
    #[serde(default, rename = "contentHex")]
    content_hex: String,
}

impl CaseFile {
    fn content(&self) -> Vec<u8> {
        if self.content_hex.is_empty() {
            self.content.clone().into_bytes()
        } else {
            decode_hex(&self.content_hex)
        }
    }

    fn size(&self) -> i64 {
        self.size.unwrap_or(self.content().len() as i64)
    }
}

fn decode_hex(value: &str) -> Vec<u8> {
    value
        .as_bytes()
        .chunks(2)
        .map(|pair| {
            assert_eq!(pair.len(), 2, "odd-length contentHex");
            u8::from_str_radix(std::str::from_utf8(pair).expect("hex"), 16).expect("hex")
        })
        .collect()
}

#[derive(Debug, Clone, Deserialize, Default)]
struct Changed {
    #[serde(default)]
    path: String,
    #[serde(default)]
    status: String,
}

#[derive(Debug, Deserialize, Default)]
struct Embedder {
    #[serde(default)]
    mode: String,
    #[serde(default)]
    failed: Vec<i64>,
}

#[derive(Debug, Deserialize, Default)]
struct EmbedCase {
    #[serde(default)]
    name: String,
    #[serde(default, rename = "repoId")]
    repo_id: i32,
    #[serde(default, rename = "repoName")]
    repo_name: String,
    #[serde(default)]
    revision: String,
    #[serde(default, rename = "indexedRevision")]
    indexed_revision: String,
    #[serde(default)]
    files: Vec<CaseFile>,
    #[serde(default)]
    changed: Vec<Changed>,
    #[serde(default, rename = "diffError")]
    diff_error: bool,
    #[serde(default)]
    ranks: BTreeMap<String, f64>,
    #[serde(default)]
    dimensions: usize,
    #[serde(default)]
    model: String,
    #[serde(default, rename = "batchSize")]
    batch_size: usize,
    #[serde(default, rename = "maxCode")]
    max_code: usize,
    #[serde(default, rename = "maxText")]
    max_text: usize,
    #[serde(default, rename = "maxFileSize")]
    max_file_size: i64,
    #[serde(default)]
    exclude: Vec<String>,
    #[serde(default)]
    include: Vec<String>,
    #[serde(default, rename = "excludeChunks")]
    exclude_chunks: bool,
    #[serde(default, rename = "tolerableFailureRatio")]
    tolerable_failure_ratio: f64,
    #[serde(default)]
    embedder: Embedder,
}

#[derive(Debug, Deserialize, Default)]
struct RowMetadata {
    #[serde(default, rename = "fileName")]
    file_name: String,
    #[serde(default, rename = "startLine")]
    start_line: i64,
    #[serde(default, rename = "endLine")]
    end_line: i64,
}

#[derive(Debug, Deserialize, Default)]
struct IndexRepr {
    #[serde(default)]
    embeddings: Vec<i8>,
    #[serde(default, rename = "columnDimension")]
    column_dimension: i64,
    #[serde(default, rename = "rowMetadata")]
    row_metadata: Vec<RowMetadata>,
    #[serde(default)]
    ranks: Vec<f32>,
}

#[derive(Debug, Deserialize, Default)]
struct RepoIndexRepr {
    #[serde(default, rename = "repoName")]
    repo_name: String,
    #[serde(default)]
    revision: String,
    #[serde(default, rename = "embeddingsModel")]
    embeddings_model: String,
    #[serde(default, rename = "codeIndex")]
    code_index: IndexRepr,
    #[serde(default, rename = "textIndex")]
    text_index: IndexRepr,
}

impl RepoIndexRepr {
    fn to_index(&self) -> RepoEmbeddingIndex {
        RepoEmbeddingIndex {
            repo_name: self.repo_name.clone(),
            revision: self.revision.clone(),
            embeddings_model: self.embeddings_model.clone(),
            code_index: index(&self.code_index),
            text_index: index(&self.text_index),
        }
    }
}

fn index(repr: &IndexRepr) -> EmbeddingIndex {
    EmbeddingIndex {
        embeddings: repr.embeddings.clone(),
        column_dimension: repr.column_dimension,
        row_metadata: repr
            .row_metadata
            .iter()
            .map(|row| RepoEmbeddingRowMetadata {
                file_name: row.file_name.clone(),
                start_line: row.start_line,
                end_line: row.end_line,
            })
            .collect(),
        ranks: repr.ranks.clone(),
    }
}

#[derive(Debug, Deserialize, Default)]
struct UpdateCase {
    #[serde(default)]
    name: String,
    #[serde(default, rename = "repoId")]
    repo_id: i32,
    #[serde(default, rename = "repoName")]
    repo_name: String,
    #[serde(default)]
    previous: RepoIndexRepr,
    #[serde(default)]
    new: RepoIndexRepr,
    #[serde(default, rename = "toRemove")]
    to_remove: Vec<String>,
    #[serde(default)]
    ranks: BTreeMap<String, f64>,
}

#[derive(Debug, Deserialize, Default)]
struct NameCase {
    #[serde(default, rename = "repoId")]
    repo_id: i32,
    #[serde(default, rename = "repoName")]
    repo_name: String,
}

// ---------------------------------------------------------------------------
// Fakes
// ---------------------------------------------------------------------------

/// `revisionFetcher` over the corpus' file list, read contents and diff.
struct CorpusReadLister {
    files: Vec<CaseFile>,
    changed: Vec<Changed>,
    diff_error: bool,
}

impl CorpusReadLister {
    fn entries(&self) -> Vec<FileEntry> {
        self.files
            .iter()
            .map(|file| FileEntry {
                name: file.name.clone(),
                size: file.size(),
            })
            .collect()
    }
}

#[async_trait]
impl FileReadLister for CorpusReadLister {
    async fn read(&self, file_name: &str) -> Result<Vec<u8>, String> {
        self.files
            .iter()
            .find(|file| file.name == file_name)
            .map(CaseFile::content)
            .ok_or_else(|| format!("no such file {file_name:?}"))
    }

    async fn list(&self) -> Result<Vec<FileEntry>, String> {
        Ok(self.entries())
    }

    async fn diff(&self, _old_commit: &str) -> Result<(Vec<FileEntry>, Vec<String>), String> {
        if self.diff_error {
            return Err("diff boom".to_owned());
        }
        let mut to_remove = Vec::new();
        let mut changed_new = Vec::new();
        for changed in &self.changed {
            match changed.status.as_str() {
                "deleted" => to_remove.push(changed.path.clone()),
                "modified" => {
                    to_remove.push(changed.path.clone());
                    changed_new.push(changed.path.clone());
                }
                "added" => changed_new.push(changed.path.clone()),
                "typeChanged" => {}
                other => panic!("unknown status {other}"),
            }
        }
        let changed_new: std::collections::HashSet<&str> =
            changed_new.iter().map(String::as_str).collect();
        let to_index = self
            .entries()
            .into_iter()
            .filter(|entry| changed_new.contains(entry.name.as_str()))
            .collect();
        Ok((to_index, to_remove))
    }
}

struct FakeEmbeddingsClient {
    dimensions: usize,
    model: String,
    mode: String,
    failed: Vec<i64>,
    batches: Mutex<Vec<Vec<String>>>,
}

#[async_trait]
impl EmbeddingsClient for FakeEmbeddingsClient {
    async fn get_document_embeddings(
        &self,
        documents: Vec<String>,
    ) -> Result<EmbeddingsResults, EmbeddingsClientError> {
        self.batches.lock().unwrap().push(documents.clone());

        match self.mode.as_str() {
            "error" => return Err(EmbeddingsClientError::Other("bomb".to_owned())),
            "ratelimit" => return Err(EmbeddingsClientError::RateLimitExceeded),
            _ => {}
        }

        let mut embeddings = Vec::with_capacity(documents.len() * self.dimensions);
        for document in &documents {
            for j in 0..self.dimensions {
                let bucket = ((document.len() + j) % 5) as i64 - 2;
                embeddings.push((bucket as f64 / 2.0) as f32);
            }
        }
        if self.mode == "badLength" && !embeddings.is_empty() {
            embeddings.pop();
        }
        let failed = if self.mode == "failed" {
            self.failed.clone()
        } else {
            Vec::new()
        };
        Ok(EmbeddingsResults {
            embeddings,
            failed,
            dimensions: self.dimensions,
        })
    }

    fn get_dimensions(&self) -> Result<usize, String> {
        Ok(self.dimensions)
    }

    fn get_model_identifier(&self) -> String {
        self.model.clone()
    }
}

#[derive(Default)]
struct FakeObjectStorage {
    uploaded: Mutex<Option<(String, Vec<u8>)>>,
    keys_requested: Mutex<Vec<String>>,
}

#[async_trait]
impl ObjectStorageBackend for FakeObjectStorage {
    async fn init(&self) -> Result<(), String> {
        Ok(())
    }

    async fn get(&self, key: &str) -> Result<Vec<u8>, String> {
        self.keys_requested.lock().unwrap().push(key.to_owned());
        let uploaded = self.uploaded.lock().unwrap().clone();
        match uploaded {
            Some((uploaded_key, bytes)) if uploaded_key == key => Ok(bytes),
            _ => Err(format!("not found: {key}")),
        }
    }

    async fn upload(&self, key: &str, data: Vec<u8>) -> Result<i64, String> {
        let len = data.len() as i64;
        *self.uploaded.lock().unwrap() = Some((key.to_owned(), data));
        Ok(len)
    }

    async fn delete(&self, _key: &str) -> Result<(), String> {
        Ok(())
    }

    async fn list_objects(&self, _prefix: &str) -> Result<Vec<String>, String> {
        Ok(Vec::new())
    }

    async fn expire_objects(
        &self,
        _prefix: &str,
        _max_age: std::time::Duration,
    ) -> Result<(), String> {
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Canonical JSON
// ---------------------------------------------------------------------------

fn files_stats(stats: &EmbedFilesStats) -> Value {
    let skipped: Map<String, Value> = stats
        .files_skipped
        .iter()
        .map(|(reason, count)| (reason.clone(), json!(count)))
        .collect();
    json!({
        "filesScheduled": stats.files_scheduled,
        "filesEmbedded": stats.files_embedded,
        "filesSkipped": Value::Object(skipped),
        "chunksEmbedded": stats.chunks_embedded,
        "chunksExcluded": stats.chunks_excluded,
        "bytesEmbedded": stats.bytes_embedded,
    })
}

fn repo_stats(stats: &EmbedRepoStats) -> Value {
    json!({
        "codeIndexStats": files_stats(&stats.code_index_stats),
        "textIndexStats": files_stats(&stats.text_index_stats),
        "isIncremental": stats.is_incremental,
    })
}

fn embedding_index(index: &EmbeddingIndex) -> Value {
    let rows: Vec<Value> = index
        .row_metadata
        .iter()
        .map(|row| {
            json!({
                "fileName": row.file_name,
                "startLine": row.start_line,
                "endLine": row.end_line,
            })
        })
        .collect();
    let ranks: Vec<Value> = index
        .ranks
        .iter()
        .map(|rank| Value::String(format!("{:.6}", f64::from(*rank))))
        .collect();
    json!({
        "embeddings": index.embeddings,
        "columnDimension": index.column_dimension,
        "rowMetadata": rows,
        "ranks": ranks,
    })
}

fn repo_index(index: &RepoEmbeddingIndex) -> Value {
    json!({
        "repoName": index.repo_name,
        "revision": index.revision,
        "embeddingsModel": index.embeddings_model,
        "codeIndex": embedding_index(&index.code_index),
        "textIndex": embedding_index(&index.text_index),
    })
}

// ---------------------------------------------------------------------------
// Case runners
// ---------------------------------------------------------------------------

fn run_split_case(case: &SplitCase) -> Value {
    let chunks: Vec<Value> =
        split_into_embeddable_chunks(&case.text, &case.file_name, split_options())
            .iter()
            .map(chunk_json)
            .collect();
    json!({ "name": case.name, "chunks": chunks })
}

fn chunk_json(chunk: &EmbeddableChunk) -> Value {
    json!({
        "fileName": chunk.file_name,
        "startLine": chunk.start_line,
        "endLine": chunk.end_line,
        "content": chunk.content,
    })
}

fn run_filter_case(case: &FilterCase) -> Value {
    let config = EmbeddingsJobConfig {
        provider: String::new(),
        incremental: false,
        excluded_file_path_patterns: case.exclude.clone(),
        included_file_path_patterns: case.include.clone(),
        max_file_size_bytes: 0,
        max_code_embeddings_per_repo: 0,
        max_text_embeddings_per_repo: 0,
        exclude_chunk_on_error: false,
    };
    let (include, exclude) = get_file_filter_path_patterns(&config);
    let paths: Vec<Value> = case
        .paths
        .iter()
        .map(|path| {
            json!({
                "path": path,
                "included": is_included_file_path_match(path, &include),
                "excluded": is_excluded_file_path_match(path, &exclude),
            })
        })
        .collect();
    json!({ "name": case.name, "paths": paths })
}

async fn run_embed_case(case: &EmbedCase) -> Value {
    let client = FakeEmbeddingsClient {
        dimensions: case.dimensions,
        model: case.model.clone(),
        mode: case.embedder.mode.clone(),
        failed: case.embedder.failed.clone(),
        batches: Mutex::new(Vec::new()),
    };
    let read_lister = CorpusReadLister {
        files: case.files.clone(),
        changed: case.changed.clone(),
        diff_error: case.diff_error,
    };
    let ranks = RepoPathRanks {
        mean_rank: 0.0,
        paths: case.ranks.clone(),
    };
    let opts = EmbedRepoOpts {
        repo_name: case.repo_name.clone(),
        revision: case.revision.clone(),
        file_filters: FileFilters {
            exclude_patterns: compile_glob_patterns(&case.exclude),
            include_patterns: compile_glob_patterns(&case.include),
            max_file_size_bytes: case.max_file_size,
        },
        split_options: split_options(),
        max_code_embeddings: case.max_code,
        max_text_embeddings: case.max_text,
        batch_size: case.batch_size,
        exclude_chunks: case.exclude_chunks,
        tolerable_failure_ratio: case.tolerable_failure_ratio,
        indexed_revision: case.indexed_revision.clone(),
    };

    let context_service: Arc<dyn ContextService> = Arc::new(DefaultContextService);
    let repo = RepoRecord {
        id: case.repo_id,
        name: case.repo_name.clone(),
    };

    let mut progress: Vec<Value> = Vec::new();
    let mut report = |stats: &EmbedRepoStats| progress.push(repo_stats(stats));

    let cancellation = CancellationToken::new();
    let result = embed_repo(
        &cancellation,
        &client,
        context_service.as_ref(),
        &read_lister,
        &repo,
        &ranks,
        &opts,
        &mut report,
    )
    .await;

    let batches: Vec<Value> = client
        .batches
        .lock()
        .unwrap()
        .iter()
        .map(|batch| json!(batch))
        .collect();

    let mut output = json!({
        "name": case.name,
        "batches": batches,
        "progress": progress,
        "error": Value::Null,
        "index": Value::Null,
        "toRemove": [],
        "stats": Value::Null,
    });
    match result {
        Ok((index, to_remove, stats)) => {
            output["index"] = repo_index(&index);
            output["toRemove"] = json!(to_remove);
            output["stats"] = repo_stats(&stats);
        }
        Err(error) => output["error"] = json!(error),
    }
    output
}

async fn run_update_case(case: &UpdateCase) -> Value {
    let store = FakeObjectStorage::default();
    let key = repo_embedding_index_name(case.repo_id);
    let mut previous = case.previous.to_index();
    let new = case.new.to_index();
    let ranks = RepoPathRanks {
        mean_rank: 0.0,
        paths: case.ranks.clone(),
    };

    let mut output = json!({
        "name": case.name,
        "error": Value::Null,
        "uploadKey": "",
        "downloadKeys": [],
        "index": Value::Null,
    });

    if let Err(error) =
        update_repo_embedding_index(&store, &key, &mut previous, &new, &case.to_remove, &ranks)
            .await
    {
        output["error"] = json!(error);
        return output;
    }
    output["uploadKey"] = json!(key);

    let decoded = match download_repo_embedding_index(&store, case.repo_id, &case.repo_name).await {
        Ok(index) => index,
        Err(error) => {
            output["error"] = json!(error);
            output["downloadKeys"] = json!(store.keys_requested.lock().unwrap().clone());
            return output;
        }
    };
    output["downloadKeys"] = json!(store.keys_requested.lock().unwrap().clone());
    output["index"] = repo_index(&decoded);
    output
}

fn run_name_case(case: &NameCase) -> Value {
    json!({
        "repoId": case.repo_id,
        "repoName": case.repo_name,
        "name": repo_embedding_index_name(case.repo_id),
        "deprecated": repo_embedding_index_name_deprecated(&case.repo_name),
    })
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

#[tokio::test]
async fn repo_embedding_job_diff() {
    if std::env::var("SG_M9IO_DIFF").as_deref() != Ok("1") {
        return;
    }

    let corpus_path = std::env::var("M9IO_CORPUS").expect("M9IO_CORPUS is not set");
    let out_path = std::env::var("M9IO_OUT").expect("M9IO_OUT is not set");

    let raw = std::fs::read_to_string(&corpus_path).expect("read corpus");
    let corpus: Corpus = serde_json::from_str(&raw).expect("parse corpus");

    let split_cases: Vec<Value> = corpus.split_cases.iter().map(run_split_case).collect();
    let filter_cases: Vec<Value> = corpus.filter_cases.iter().map(run_filter_case).collect();

    let mut embed_cases = Vec::with_capacity(corpus.embed_cases.len());
    for case in &corpus.embed_cases {
        embed_cases.push(run_embed_case(case).await);
    }

    let mut update_cases = Vec::with_capacity(corpus.update_cases.len());
    for case in &corpus.update_cases {
        update_cases.push(run_update_case(case).await);
    }

    let name_cases: Vec<Value> = corpus.name_cases.iter().map(run_name_case).collect();

    let output = json!({
        "splitCases": split_cases,
        "filterCases": filter_cases,
        "embedCases": embed_cases,
        "updateCases": update_cases,
        "nameCases": name_cases,
    });

    let mut encoded = serde_json::to_string(&output).expect("encode output");
    encoded.push('\n');
    std::fs::write(&out_path, encoded).expect("write output");

    eprintln!(
        "m9io rust wrote {} split, {} filter, {} embed, {} update, {} name cases",
        corpus.split_cases.len(),
        corpus.filter_cases.len(),
        corpus.embed_cases.len(),
        corpus.update_cases.len(),
        corpus.name_cases.len()
    );
}
