#![forbid(unsafe_code)]

//! M9.7f differential harness (Rust side): the uploads-store completion
//! transaction.
//!
//! Runs the five store mutations `HandleRawUpload` performs after the code
//! graph has been written, against a scratch copy of the frontend schema, and
//! dumps the affected tables in the same canonical form as the Go harness
//! (`internal/codeintel/uploads/storeharness`).
//! `test_ws/p9-go/run_uploads_store_diff.sh` compares the outputs.
//!
//! Requires `SG_USTORE_DSN`; without it the test is skipped.

use sqlx::postgres::PgPool;
use sqlx::Row;

use sg_scip::Package;
use sg_service_precise_worker::scip_stream::PackageReference;
use sg_service_precise_worker::uploads_store;

const COMMIT_A: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

#[tokio::test]
async fn uploads_store_matches_go_harness() {
    let Ok(dsn) = std::env::var("SG_USTORE_DSN") else {
        eprintln!("SG_USTORE_DSN is not set; skipping the uploads-store harness");
        return;
    };

    let pool = PgPool::connect(&dsn).await.expect("connect");

    let packages = vec![
        Package {
            scheme: "npm".to_owned(),
            manager: "npm".to_owned(),
            name: "lodash".to_owned(),
            version: "4.17.21".to_owned(),
        },
        Package {
            scheme: "gomod".to_owned(),
            manager: "gomod".to_owned(),
            name: "golang.org/x/text".to_owned(),
            version: "v0.3.0".to_owned(),
        },
        Package {
            scheme: "npm".to_owned(),
            manager: "npm".to_owned(),
            name: "lodash".to_owned(),
            version: "4.17.21".to_owned(),
        },
        Package {
            scheme: "npm".to_owned(),
            manager: "npm".to_owned(),
            name: "react".to_owned(),
            version: "18.2.0".to_owned(),
        },
    ];
    let references = vec![
        PackageReference {
            package: Package {
                scheme: "npm".to_owned(),
                manager: "npm".to_owned(),
                name: "@types/node".to_owned(),
                version: "20.0.0".to_owned(),
            },
        },
        PackageReference {
            package: Package {
                scheme: "gomod".to_owned(),
                manager: "gomod".to_owned(),
                name: "github.com/pkg/errors".to_owned(),
                version: "v0.9.1".to_owned(),
            },
        },
    ];

    // The completion transaction from `HandleRawUpload`.
    let mut tx = pool.begin().await.expect("begin transaction");
    uploads_store::delete_overlapping_completed_uploads(
        &mut tx,
        9101,
        COMMIT_A,
        "",
        "scip-typescript",
    )
    .await
    .expect("delete overlapping completed uploads");
    uploads_store::update_packages(&mut tx, 207, &packages)
        .await
        .expect("update packages");
    uploads_store::update_package_references(&mut tx, 207, &references)
        .await
        .expect("update package references");
    let job_id = uploads_store::insert_dependency_syncing_job(&mut tx, 207)
        .await
        .expect("insert dependency syncing job");
    println!("store dep_job returned={job_id}");
    for repository_id in [9101, 9102, 9103] {
        uploads_store::set_repository_as_dirty(&mut tx, repository_id)
            .await
            .expect("set repository as dirty");
    }
    tx.commit().await.expect("commit");

    dump(&pool).await;
}

async fn dump(pool: &PgPool) {
    let rows = sqlx::query("SELECT id, state FROM lsif_uploads ORDER BY id")
        .fetch_all(pool)
        .await
        .expect("query uploads");
    for row in rows {
        let id: i32 = row.get(0);
        let state: String = row.get(1);
        println!("store upload id={id} state={state}");
    }

    let rows = sqlx::query(
        "SELECT upload_id, reason, operation::text, transition_columns::text \
         FROM lsif_uploads_audit_logs ORDER BY upload_id",
    )
    .fetch_all(pool)
    .await
    .expect("query audit logs");
    for row in rows {
        let upload_id: i32 = row.get(0);
        let reason: String = row.get(1);
        let operation: String = row.get(2);
        let transition: Option<String> = row.get(3);
        println!(
            "store audit upload_id={upload_id} reason={reason} operation={operation} transition={}",
            transition.unwrap_or_else(|| "NULL".to_owned())
        );
    }

    let rows = sqlx::query(
        "SELECT dump_id, scheme, manager, name, version FROM lsif_packages \
         ORDER BY dump_id, scheme, manager, name, version",
    )
    .fetch_all(pool)
    .await
    .expect("query packages");
    for row in rows {
        let dump_id: i32 = row.get(0);
        let scheme: String = row.get(1);
        let manager: String = row.get(2);
        let name: String = row.get(3);
        let version: String = row.get(4);
        println!(
            "store package dump_id={dump_id} scheme={scheme} manager={manager} name={name} version={version}"
        );
    }

    let rows = sqlx::query(
        "SELECT dump_id, scheme, manager, name, version FROM lsif_references \
         ORDER BY dump_id, scheme, manager, name, version",
    )
    .fetch_all(pool)
    .await
    .expect("query references");
    for row in rows {
        let dump_id: i32 = row.get(0);
        let scheme: String = row.get(1);
        let manager: String = row.get(2);
        let name: String = row.get(3);
        let version: String = row.get(4);
        println!(
            "store reference dump_id={dump_id} scheme={scheme} manager={manager} name={name} version={version}"
        );
    }

    let rows = sqlx::query(
        "SELECT id, COALESCE(upload_id::text, 'NULL') FROM lsif_dependency_syncing_jobs \
         ORDER BY id",
    )
    .fetch_all(pool)
    .await
    .expect("query dependency syncing jobs");
    for row in rows {
        let id: i32 = row.get(0);
        let upload_id: String = row.get(1);
        println!("store dep_job id={id} upload_id={upload_id}");
    }

    let rows = sqlx::query(
        "SELECT repository_id, dirty_token, update_token, \
                set_dirty_at = TIMESTAMPTZ '2000-01-01 00:00:00+00' \
         FROM lsif_dirty_repositories ORDER BY repository_id",
    )
    .fetch_all(pool)
    .await
    .expect("query dirty repositories");
    for row in rows {
        let repository_id: i32 = row.get(0);
        let dirty_token: i32 = row.get(1);
        let update_token: i32 = row.get(2);
        let kept: bool = row.get(3);
        println!(
            "store dirty repository_id={repository_id} dirty_token={dirty_token} update_token={update_token} set_dirty_at_kept={kept}"
        );
    }
}
