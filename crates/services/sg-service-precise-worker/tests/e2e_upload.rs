#![forbid(unsafe_code)]

//! M9.7f differential harness (Rust side): a full upload, end to end.
//!
//! Drives the real [`UploadProcessorHandler`] against a scratch frontend
//! database, the live gitserver and the live blobstore: the upload object is
//! put into the `lsif-uploads` bucket, the handler fetches it, parses the SCIP
//! index, writes the code graph, runs the uploads completion transaction and
//! deletes the object. The worker loop's terminal state transition is then
//! emulated (`mark_complete`), and both databases are dumped in canonical
//! `e2e`/`store`/`write` lines.
//!
//! The Go counterpart is
//! `internal/codeintel/uploads/internal/background/processor/e2e_harness_test.go`;
//! `test_ws/p9-go/run_e2e_upload_diff.sh` applies `e2e_upload_fixtures.sql`
//! before each side and compares the outputs.
//!
//! Requires `SG_E2E_FRONTEND_DSN`, `SG_E2E_CODEINTEL_DSN` and
//! `SG_SCIP_FIXTURE`; without them the test is skipped. The fixture repository
//! `test/e2e-fixture` must exist in the live gitserver and its commit must be
//! the one inserted by the fixture.

use std::sync::Arc;

use sqlx::postgres::PgPool;
use sqlx::Row;
use tokio::io::AsyncReadExt;
use tokio_util::sync::CancellationToken;

use sg_git::GitserverClient;
use sg_objectstorage::{ObjectByteStream, ObjectStore, S3Config, StorageConfig};
use sg_service_precise_worker::handler::{HandlerDeps, UploadProcessorHandler};
use sg_service_precise_worker::prelude::PreludeDeps;
use sg_service_precise_worker::queue::UploadQueueStore;
use sg_service_precise_worker::worker::UploadHandler;

mod common;

use common::dump_scip_tables;

const E2E_UPLOAD_ID: i32 = 401;

const TRUNCATE_SCIP_TABLES: &str = "
TRUNCATE
    codeintel_scip_metadata,
    codeintel_scip_documents,
    codeintel_scip_document_lookup,
    codeintel_scip_symbol_names,
    codeintel_scip_symbols,
    codeintel_scip_document_lookup_schema_versions,
    codeintel_scip_symbols_schema_versions,
    codeintel_scip_documents_dereference_logs
RESTART IDENTITY
";

fn env_or(name: &str, default: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| default.to_owned())
}

fn nullable(value: Option<String>) -> String {
    value.unwrap_or_else(|| "NULL".to_owned())
}

#[tokio::test]
async fn e2e_upload_matches_go_harness() {
    let (Ok(frontend_dsn), Ok(codeintel_dsn), Ok(fixture)) = (
        std::env::var("SG_E2E_FRONTEND_DSN"),
        std::env::var("SG_E2E_CODEINTEL_DSN"),
        std::env::var("SG_SCIP_FIXTURE"),
    ) else {
        eprintln!(
            "SG_E2E_FRONTEND_DSN / SG_E2E_CODEINTEL_DSN / SG_SCIP_FIXTURE are not set; \
             skipping the end-to-end upload harness"
        );
        return;
    };

    let frontend = PgPool::connect(&frontend_dsn)
        .await
        .expect("connect to frontend database");
    let codeintel = PgPool::connect(&codeintel_dsn)
        .await
        .expect("connect to codeintel database");
    sqlx::query(TRUNCATE_SCIP_TABLES)
        .execute(&codeintel)
        .await
        .expect("truncate SCIP tables");

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

    let queue = Arc::new(UploadQueueStore::new(frontend.clone()));
    let git = Arc::new(GitserverClient::new(vec![env_or(
        "SG_GIT_ADDR",
        "127.0.0.1:3178",
    )]));
    let deps = PreludeDeps::new(
        frontend.clone(),
        Arc::clone(&queue),
        git,
        Arc::clone(&store),
    );
    let handler = UploadProcessorHandler::new(
        HandlerDeps {
            prelude: deps,
            codeintel: codeintel.clone(),
        },
        0,
    );

    let key = format!("upload-{E2E_UPLOAD_ID}.lsif.gz");
    let payload = std::fs::read(&fixture).expect("read fixture");
    let _ = store.delete(&key).await;
    store
        .upload(&key, ObjectByteStream::from(payload))
        .await
        .expect("upload fixture object");

    let upload = queue
        .get_upload(E2E_UPLOAD_ID)
        .await
        .expect("get upload")
        .expect("upload row");

    let result = handler.handle(upload, CancellationToken::new()).await;
    println!("e2e handler_ok={}", result.is_ok());

    // The worker loop performs the terminal state transition; this harness
    // calls the handler directly, so do it here (same store, same options).
    let marked = match &result {
        Ok(()) => queue
            .mark_complete(E2E_UPLOAD_ID, None)
            .await
            .expect("mark complete"),
        Err(err) => queue
            .mark_errored(E2E_UPLOAD_ID, &err.message, None)
            .await
            .expect("mark errored"),
    };
    println!("e2e marked_terminal={marked}");

    // Probe "the full object can be read" (the Go store returns a lazy pipe
    // reader, so its harness drains the body too).
    let present = match store.get(&key).await {
        Ok(body) => {
            let mut body = body.into_async_read();
            let mut sink = Vec::new();
            body.read_to_end(&mut sink).await.is_ok()
        }
        Err(_) => false,
    };
    println!("e2e object_present={present}");

    dump_frontend(&frontend).await;
    dump_scip_tables(&codeintel).await;
}

async fn dump_frontend(pool: &PgPool) {
    let rows = sqlx::query(
        "SELECT id, state, failure_message, num_failures FROM lsif_uploads ORDER BY id",
    )
    .fetch_all(pool)
    .await
    .expect("uploads query");
    for row in rows {
        println!(
            "store upload id={} state={} failure={} num_failures={}",
            row.get::<i32, _>("id"),
            row.get::<String, _>("state"),
            nullable(row.get::<Option<String>, _>("failure_message")),
            row.get::<i32, _>("num_failures"),
        );
    }

    let rows = sqlx::query(
        "SELECT upload_id, reason, operation::text AS operation, \
         transition_columns::text AS transitions \
         FROM lsif_uploads_audit_logs ORDER BY upload_id",
    )
    .fetch_all(pool)
    .await
    .expect("audit query");
    for row in rows {
        println!(
            "store audit upload_id={} reason={} operation={} transitions={}",
            row.get::<i32, _>("upload_id"),
            nullable(row.get::<Option<String>, _>("reason")),
            row.get::<String, _>("operation"),
            nullable(row.get::<Option<String>, _>("transitions")),
        );
    }

    let rows = sqlx::query(
        "SELECT dump_id, scheme, manager, name, version \
         FROM lsif_packages ORDER BY dump_id, scheme, manager, name, version",
    )
    .fetch_all(pool)
    .await
    .expect("packages query");
    for row in rows {
        println!(
            "store package dump_id={} scheme={} manager={} name={} version={}",
            row.get::<i32, _>("dump_id"),
            row.get::<String, _>("scheme"),
            row.get::<String, _>("manager"),
            row.get::<String, _>("name"),
            nullable(row.get::<Option<String>, _>("version")),
        );
    }

    let rows = sqlx::query(
        "SELECT dump_id, scheme, manager, name, version \
         FROM lsif_references ORDER BY dump_id, scheme, manager, name, version",
    )
    .fetch_all(pool)
    .await
    .expect("references query");
    for row in rows {
        println!(
            "store reference dump_id={} scheme={} manager={} name={} version={}",
            row.get::<i32, _>("dump_id"),
            row.get::<String, _>("scheme"),
            row.get::<String, _>("manager"),
            row.get::<String, _>("name"),
            nullable(row.get::<Option<String>, _>("version")),
        );
    }

    let rows =
        sqlx::query("SELECT id, upload_id, state FROM lsif_dependency_syncing_jobs ORDER BY id")
            .fetch_all(pool)
            .await
            .expect("dependency jobs query");
    for row in rows {
        println!(
            "store dependency_job id={} upload_id={} state={}",
            row.get::<i32, _>("id"),
            row.get::<Option<i32>, _>("upload_id")
                .map_or_else(|| "NULL".to_owned(), |id| id.to_string()),
            row.get::<String, _>("state"),
        );
    }

    let rows = sqlx::query(
        "SELECT repository_id, dirty_token, update_token \
         FROM lsif_dirty_repositories ORDER BY repository_id",
    )
    .fetch_all(pool)
    .await
    .expect("dirty query");
    for row in rows {
        println!(
            "store dirty repository_id={} dirty_token={} update_token={}",
            row.get::<i32, _>("repository_id"),
            row.get::<i32, _>("dirty_token"),
            row.get::<i32, _>("update_token"),
        );
    }

    let rows = sqlx::query(
        "SELECT repository_id, encode(commit_bytea, 'hex') AS commit, \
         to_char(committed_at AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"') AS committed_at \
         FROM codeintel_commit_dates ORDER BY repository_id, commit_bytea",
    )
    .fetch_all(pool)
    .await
    .expect("commit dates query");
    for row in rows {
        println!(
            "store commit_date repository_id={} commit={} committed_at={}",
            row.get::<i32, _>("repository_id"),
            row.get::<String, _>("commit"),
            nullable(row.get::<Option<String>, _>("committed_at")),
        );
    }
}
