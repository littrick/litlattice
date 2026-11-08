#![forbid(unsafe_code)]

//! M9.7e differential harness (Rust side): the code-graph write path.
//!
//! Runs [`prepare_scip_data_stream`] followed by [`write_scip_documents`]
//! against a scratch code-intel database, then dumps the SCIP tables in the
//! same canonical form as the Go harness
//! (`internal/codeintel/uploads/internal/background/processor/write_harness_test.go`).
//! `test_ws/p9-go/run_codegraph_write_diff.sh` compares the outputs.
//!
//! Requires `SG_CODEINTEL_DSN` (a disposable database with the `codeintel`
//! schema) and `SG_SCIP_FIXTURE`; without them the test is skipped.

use std::collections::HashMap;

use sqlx::postgres::PgPool;

use sg_service_precise_worker::codegraph::write_scip_documents;
use sg_service_precise_worker::pathexistence::DirectoryChildren;
use sg_service_precise_worker::prelude::UploadData;
use sg_service_precise_worker::scip_stream::prepare_scip_data_stream;

mod common;

use common::{dump_scip_tables, print_packages};

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

#[tokio::test]
async fn codegraph_write_matches_go_harness() {
    let (Ok(dsn), Ok(fixture)) = (
        std::env::var("SG_CODEINTEL_DSN"),
        std::env::var("SG_SCIP_FIXTURE"),
    ) else {
        eprintln!(
            "SG_CODEINTEL_DSN / SG_SCIP_FIXTURE are not set; skipping the code-graph harness"
        );
        return;
    };

    let pool = PgPool::connect(&dsn).await.expect("connect");
    sqlx::query(TRUNCATE_SCIP_TABLES)
        .execute(&pool)
        .await
        .expect("truncate SCIP tables");

    let bytes = std::fs::read(&fixture).expect("read fixture");
    let children = directory_children();

    let stream = prepare_scip_data_stream(
        UploadData::from_bytes(bytes),
        "",
        move |dirnames: Vec<String>| {
            let children = children.clone();
            async move {
                let listed: DirectoryChildren = dirnames
                    .into_iter()
                    .map(|dirname| {
                        let entries = children.get(&dirname).cloned().unwrap_or_default();
                        (dirname, entries)
                    })
                    .collect();
                Ok::<DirectoryChildren, ()>(listed)
            }
        },
    )
    .await
    .expect("prepare SCIP data stream");

    let mut tx = pool.begin().await.expect("begin transaction");
    let packages = write_scip_documents(&mut tx, 1, "scip-typescript", &stream)
        .await
        .expect("write SCIP documents");
    tx.commit().await.expect("commit");

    print_packages(&packages);
    dump_scip_tables(&pool).await;
}

fn directory_children() -> HashMap<String, Vec<String>> {
    HashMap::from([
        (String::new(), vec!["template".to_owned()]),
        ("template".to_owned(), vec!["template/src".to_owned()]),
        (
            "template/src".to_owned(),
            vec![
                "template/src/extension.ts".to_owned(),
                "template/src/indicators.ts".to_owned(),
                "template/src/language.ts".to_owned(),
                "template/src/logging.ts".to_owned(),
                "template/src/util".to_owned(),
            ],
        ),
        (
            "template/src/util".to_owned(),
            vec![
                "template/src/util/api.ts".to_owned(),
                "template/src/util/graphql.ts".to_owned(),
                "template/src/util/ix.test.ts".to_owned(),
                "template/src/util/ix.ts".to_owned(),
                "template/src/util/promise.ts".to_owned(),
                "template/src/util/uri.test.ts".to_owned(),
                "template/src/util/uri.ts".to_owned(),
            ],
        ),
    ])
}
