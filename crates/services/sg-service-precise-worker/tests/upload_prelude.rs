#![forbid(unsafe_code)]

//! Go/Rust differential for the `HandleRawUpload` prelude (M9.7d).
//!
//! The Go counterpart is
//! `internal/codeintel/uploads/preludeharness/prelude_test.go`; both sides
//! apply the same `lsif_uploads` fixture (`test_ws/p9-go/prelude_fixtures.sql`)
//! to the scratch database, create the same upload objects in the blobstore,
//! and print one canonical line per case:
//!
//! ```text
//! case=301 requeued=false temp=memory committed_at=2024-06-03T00:00:00Z err=none
//! ```
//!
//! `committed_at` is the value present after the case ran (the harness deletes
//! it before the call, so presence proves `UpdateCommittedAt` ran). `err` is a
//! bucketed classification rather than the exact error text, because the AWS
//! SDK and the Go object store render the same failure differently.

use std::io::Write as _;
use std::sync::Arc;

use sg_git::GitserverClient;
use sg_objectstorage::{ObjectByteStream, ObjectStore, S3Config, StorageConfig};
use sg_service_precise_worker::prelude::{
    handle_raw_upload_prelude, PreludeDeps, PreludeError, PreludeOutcome,
};
use sg_service_precise_worker::queue::{Upload, UploadQueueStore};
use sqlx::postgres::PgPool;
use sqlx::Row;
use time::OffsetDateTime;

const UNCOMPRESSED_LIMIT: i64 = 512 * 1024 * 1024;

fn env_or(name: &str, default: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| default.to_owned())
}

fn upload_key(id: i32) -> String {
    format!("upload-{id}.lsif.gz")
}

fn gzip_bytes(payload: &[u8]) -> Vec<u8> {
    let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    encoder.write_all(payload).expect("gzip write");
    encoder.finish().expect("gzip finish")
}

fn classify(err: &PreludeError) -> &'static str {
    match err {
        PreludeError::DeprecatedLsif => "deprecated",
        PreludeError::UnsupportedContentType(_) => "unsupported_content_type",
        PreludeError::RepoLookup(_) => "repo_lookup",
        PreludeError::UploadGet(_) | PreludeError::ReadUpload(_) => "upload_get",
        PreludeError::Decompress(_) => "decompress",
        _ => "other",
    }
}

/// Build the handler input from the `lsif_uploads` row directly, bypassing the
/// repository join that hides uploads whose repository has been soft-deleted
/// (the Go harness does the same for case 305).
async fn load_raw_upload(pool: &PgPool, id: i32) -> Upload {
    let row = sqlx::query(
        "SELECT commit, root, repository_id, content_type, upload_size, uncompressed_size \
         FROM lsif_uploads WHERE id = $1",
    )
    .bind(id)
    .fetch_one(pool)
    .await
    .expect("load raw upload");

    Upload {
        id,
        commit: row.get("commit"),
        root: row.get("root"),
        visible_at_tip: false,
        uploaded_at: OffsetDateTime::now_utc(),
        state: "queued".to_owned(),
        failure_message: None,
        started_at: None,
        finished_at: None,
        process_after: None,
        num_resets: 0,
        num_failures: 0,
        repository_id: row.get("repository_id"),
        repository_name: String::new(),
        indexer: String::new(),
        indexer_version: String::new(),
        num_parts: 1,
        uploaded_parts: vec![0],
        upload_size: row.get("upload_size"),
        associated_index_id: None,
        content_type: row.get("content_type"),
        should_reindex: false,
        rank: None,
        uncompressed_size: row.get("uncompressed_size"),
    }
}

async fn read_committed(pool: &PgPool, repository_id: i32, commit: &str) -> Option<String> {
    sqlx::query_scalar::<_, Option<String>>(
        "SELECT to_char(committed_at AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"') \
         FROM codeintel_commit_dates \
         WHERE repository_id = $1 AND commit_bytea = decode($2, 'hex')",
    )
    .bind(repository_id)
    .bind(commit)
    .fetch_optional(pool)
    .await
    .expect("read commit date")
    .flatten()
}

#[tokio::test]
async fn prelude_diff() {
    let Ok(dsn) = std::env::var("SG_PRELUDE_DSN") else {
        eprintln!("skipping upload_prelude diff: SG_PRELUDE_DSN is not set");
        return;
    };

    let pool = PgPool::connect(&dsn).await.expect("connect to database");
    let queue = Arc::new(UploadQueueStore::new(pool.clone()));

    let store = Arc::new(
        ObjectStore::new(StorageConfig {
            backend: "blobstore".to_owned(),
            manage_bucket: false,
            bucket: env_or("SG_OBJ_BUCKET", "lsif-uploads"),
            s3: S3Config {
                is_blobstore: false,
                region: env_or("SG_OBJ_REGION", "us-east-1"),
                endpoint: env_or("SG_OBJ_ENDPOINT", "http://127.0.0.1:9000"),
                use_path_style: false,
                access_key_id: "AKIAIOSFODNN7EXAMPLE".to_owned(),
                secret_access_key: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".to_owned(),
                session_token: String::new(),
            },
            gcs: Default::default(),
        })
        .await
        .expect("construct object store"),
    );
    store.init().await.expect("init bucket");

    let git = Arc::new(GitserverClient::new(vec![env_or(
        "SG_GIT_ADDR",
        "127.0.0.1:3178",
    )]));

    let deps = PreludeDeps::new(pool.clone(), Arc::clone(&queue), git, Arc::clone(&store));

    // The same fixture payloads the Go harness uploads: 306 intentionally has
    // no object; 307 is not gzip at all.
    let not_scip = gzip_bytes(b"not-scip");
    let payloads: Vec<(i32, Vec<u8>)> = vec![
        (301, not_scip.clone()),
        (303, not_scip.clone()),
        (304, not_scip.clone()),
        (307, b"not gzip".to_vec()),
        (308, not_scip),
    ];
    for (id, payload) in payloads {
        store
            .upload(&upload_key(id), ObjectByteStream::from(payload))
            .await
            .expect("upload fixture object");
    }
    let _ = store.delete(&upload_key(306)).await;

    for id in [301_i32, 302, 303, 304, 305, 306, 307, 308] {
        let upload = match queue.get_upload(id).await.expect("get upload") {
            Some(upload) => upload,
            // 305's repository is soft-deleted, so the queue view cannot see
            // the record; the baseline still reaches the handler with such a
            // record through a delete-after-dequeue race.
            None => load_raw_upload(&pool, id).await,
        };

        // Clear any date inserted by an earlier case with the same
        // (repository, commit) so that presence after the call proves this
        // case performed UpdateCommittedAt.
        sqlx::query(
            "DELETE FROM codeintel_commit_dates \
             WHERE repository_id = $1 AND commit_bytea = decode($2, 'hex')",
        )
        .bind(upload.repository_id)
        .bind(&upload.commit)
        .execute(&pool)
        .await
        .expect("clear commit date");

        let outcome = handle_raw_upload_prelude(&deps, &upload).await;

        let mut requeued = false;
        let mut err_class = match &outcome {
            Ok(PreludeOutcome::Requeued { .. }) => {
                requeued = true;
                "none"
            }
            Ok(PreludeOutcome::Ready { .. }) => "none",
            Err(err) => classify(err),
        };

        let committed = read_committed(&pool, upload.repository_id, &upload.commit)
            .await
            .unwrap_or_else(|| "-".to_owned());
        if committed != "-" {
            err_class = "none";
        }

        let temp = match (upload.uncompressed_size, upload.upload_size) {
            (Some(size), _) if size > UNCOMPRESSED_LIMIT => "disk",
            (None, Some(size)) if size.saturating_mul(5) > UNCOMPRESSED_LIMIT => "disk",
            _ => "memory",
        };

        println!(
            "case={id} requeued={requeued} temp={temp} committed_at={committed} err={err_class}"
        );
    }
}
