#![forbid(unsafe_code)]

//! M9.8da differential harness (Rust side): the configuration half of
//! `sg-db`'s autoindexing store (`repository_exceptions`,
//! `set_repository_exceptions`, `get_index_configuration_by_repository_id`,
//! `update_index_configuration_by_repository_id`, `get_inference_script`) plus
//! `insert_dependency_indexing_job` and `queue_repo_rev`.
//!
//! Mirrors
//! `internal/codeintel/autoindexing/internal/store/config_harness_test.go`.
//! `test_ws/p9-go/run_codeintel_autoindexing_store_misc_diff.sh` applies the
//! shared fixtures (`codeintel_autoindexing_store_misc_fixtures.sql`) to a
//! scratch frontend database immediately before each side; each case then
//! runs the same setup statements and diffs the canonical `caex ` lines.

use sg_db::codeintel_autoindexing_config::{
    get_index_configuration_by_repository_id, get_inference_script, repository_exceptions,
    update_index_configuration_by_repository_id,
};
use sg_db::codeintel_autoindexing_dependencies::{insert_dependency_indexing_job, queue_repo_rev};
use sqlx::postgres::PgPool;
use sqlx::Row;
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;

#[derive(serde::Serialize)]
struct Snapshot {
    case: &'static str,
    result: serde_json::Value,
    error: String,
}

enum Kind {
    Exceptions,
    GetConfig,
    UpdateConfig,
    Script,
    DepJob,
    Queue,
}

struct Case {
    name: &'static str,
    kind: Kind,
    repo: i32,
    data: &'static [u8],
    script: &'static str,
    rev: &'static str,
    setup: &'static str,
}

fn cases() -> Vec<Case> {
    vec![
        Case {
            name: "repository-exceptions-none",
            kind: Kind::Exceptions,
            repo: 9601,
            data: b"",
            script: "",
            rev: "",
            setup: "",
        },
        Case {
            name: "repository-exceptions-schedule-disabled",
            kind: Kind::Exceptions,
            repo: 9602,
            data: b"",
            script: "",
            rev: "",
            setup: "schedule",
        },
        Case {
            name: "repository-exceptions-infer-disabled",
            kind: Kind::Exceptions,
            repo: 9603,
            data: b"",
            script: "",
            rev: "",
            setup: "infer",
        },
        Case {
            name: "repository-exceptions-both-disabled",
            kind: Kind::Exceptions,
            repo: 9604,
            data: b"",
            script: "",
            rev: "",
            setup: "both",
        },
        Case {
            name: "get-index-config-none",
            kind: Kind::GetConfig,
            repo: 9601,
            data: b"",
            script: "",
            rev: "",
            setup: "",
        },
        Case {
            name: "get-index-config-present",
            kind: Kind::GetConfig,
            repo: 9602,
            data: b"",
            script: "",
            rev: "",
            setup: "",
        },
        Case {
            name: "update-index-config-insert",
            kind: Kind::UpdateConfig,
            repo: 9603,
            data: br#"{"index_jobs":[]}"#,
            script: "",
            rev: "",
            setup: "",
        },
        Case {
            name: "update-index-config-update",
            kind: Kind::UpdateConfig,
            repo: 9603,
            data: br#"{"index_jobs":[{"indexer":"x"}]}"#,
            script: "",
            rev: "",
            setup: "",
        },
        Case {
            name: "get-inference-script-default",
            kind: Kind::Script,
            repo: 0,
            data: b"",
            script: "none",
            rev: "",
            setup: "",
        },
        Case {
            name: "get-inference-script-custom",
            kind: Kind::Script,
            repo: 0,
            data: b"",
            script: "custom",
            rev: "",
            setup: "",
        },
        Case {
            name: "get-inference-script-empty",
            kind: Kind::Script,
            repo: 0,
            data: b"",
            script: "empty",
            rev: "",
            setup: "",
        },
        Case {
            name: "insert-dependency-job",
            kind: Kind::DepJob,
            repo: 0,
            data: b"",
            script: "",
            rev: "",
            setup: "",
        },
        Case {
            name: "queue-repo-rev-insert",
            kind: Kind::Queue,
            repo: 9607,
            data: b"",
            script: "",
            rev: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            setup: "none",
        },
        Case {
            name: "queue-repo-rev-already-queued",
            kind: Kind::Queue,
            repo: 9606,
            data: b"",
            script: "",
            rev: "dddddddddddddddddddddddddddddddddddddddd",
            setup: "upload",
        },
        Case {
            name: "queue-repo-rev-existing-row",
            kind: Kind::Queue,
            repo: 9607,
            data: b"",
            script: "",
            rev: "cccccccccccccccccccccccccccccccccccccccc",
            setup: "row",
        },
    ]
}

async fn exec(pool: &PgPool, query: &str, repo: i32) {
    sqlx::query(query)
        .bind(repo)
        .execute(pool)
        .await
        .unwrap_or_else(|err| panic!("setup {query}: {err}"));
}

async fn queue_rows(pool: &PgPool, repo: i32) -> Vec<serde_json::Value> {
    sqlx::query(
        "SELECT repository_id, rev FROM codeintel_autoindex_queue \
         WHERE repository_id = $1 ORDER BY repository_id, rev",
    )
    .bind(repo)
    .fetch_all(pool)
    .await
    .expect("query queue")
    .iter()
    .map(|row| {
        serde_json::json!({
            "repository_id": row.get::<i32, _>("repository_id"),
            "rev": row.get::<String, _>("rev"),
        })
    })
    .collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn autoindexing_store_misc_matches_go_harness() {
    if std::env::var("SG_AUTOINDEXING_STORE_MISC_DIFF")
        .ok()
        .as_deref()
        != Some("1")
    {
        return;
    }
    let dsn = std::env::var("SG_AUTOINDEXING_STORE_MISC_TEST_DSN")
        .expect("SG_AUTOINDEXING_STORE_MISC_TEST_DSN");
    let pool = sg_db::connect(&dsn, 10).await.expect("frontend pool");
    let sync_time = OffsetDateTime::parse("2020-01-01T00:00:00Z", &Rfc3339).expect("time");

    for case in cases() {
        let mut result = serde_json::Map::new();
        let mut error = "-".to_owned();
        match case.kind {
            Kind::Exceptions => {
                exec(
                    &pool,
                    "DELETE FROM codeintel_autoindexing_exceptions WHERE repository_id = $1",
                    case.repo,
                )
                .await;
                let setup = match case.setup {
                    "schedule" => Some((true, false)),
                    "infer" => Some((false, true)),
                    "both" => Some((true, true)),
                    _ => None,
                };
                if let Some((disable_scheduling, disable_inference)) = setup {
                    sqlx::query(
                        "INSERT INTO codeintel_autoindexing_exceptions \
                         (repository_id, disable_scheduling, disable_inference) \
                         VALUES ($1, $2, $3)",
                    )
                    .bind(case.repo)
                    .bind(disable_scheduling)
                    .bind(disable_inference)
                    .execute(&pool)
                    .await
                    .expect("insert exceptions");
                }
                match repository_exceptions(&pool, case.repo).await {
                    Ok((can_schedule, can_infer)) => {
                        result.insert("can_schedule".to_owned(), can_schedule.into());
                        result.insert("can_infer".to_owned(), can_infer.into());
                    }
                    Err(err) => error = err.to_string(),
                }
            }
            Kind::GetConfig => {
                match get_index_configuration_by_repository_id(&pool, case.repo).await {
                    Ok(Some(configuration)) => {
                        result.insert("found".to_owned(), true.into());
                        result.insert("id".to_owned(), configuration.id.into());
                        result.insert(
                            "repository_id".to_owned(),
                            configuration.repository_id.into(),
                        );
                        result.insert(
                            "data".to_owned(),
                            String::from_utf8_lossy(&configuration.data)
                                .into_owned()
                                .into(),
                        );
                    }
                    Ok(None) => {
                        result.insert("found".to_owned(), false.into());
                    }
                    Err(err) => error = err.to_string(),
                }
            }
            Kind::UpdateConfig => {
                exec(
                    &pool,
                    "DELETE FROM lsif_index_configuration WHERE repository_id = $1",
                    case.repo,
                )
                .await;
                sqlx::query("ALTER SEQUENCE lsif_index_configuration_id_seq RESTART WITH 1")
                    .execute(&pool)
                    .await
                    .expect("reset sequence");
                match update_index_configuration_by_repository_id(&pool, case.repo, case.data).await
                {
                    Ok(()) => {
                        match get_index_configuration_by_repository_id(&pool, case.repo).await {
                            Ok(Some(configuration)) => {
                                result.insert("found".to_owned(), true.into());
                                result.insert("id".to_owned(), configuration.id.into());
                                result.insert(
                                    "repository_id".to_owned(),
                                    configuration.repository_id.into(),
                                );
                                result.insert(
                                    "data".to_owned(),
                                    String::from_utf8_lossy(&configuration.data)
                                        .into_owned()
                                        .into(),
                                );
                            }
                            Ok(None) => {
                                result.insert("found".to_owned(), false.into());
                            }
                            Err(err) => error = err.to_string(),
                        }
                    }
                    Err(err) => error = err.to_string(),
                }
            }
            Kind::Script => {
                sqlx::query("DELETE FROM codeintel_inference_scripts")
                    .execute(&pool)
                    .await
                    .expect("reset scripts");
                match case.script {
                    "custom" => {
                        sqlx::query(
                            "INSERT INTO codeintel_inference_scripts (script) VALUES ('SELECT 1')",
                        )
                        .execute(&pool)
                        .await
                        .expect("insert script");
                    }
                    "empty" => {
                        sqlx::query("INSERT INTO codeintel_inference_scripts (script) VALUES ('')")
                            .execute(&pool)
                            .await
                            .expect("insert script");
                    }
                    _ => {}
                }
                match get_inference_script(&pool).await {
                    Ok(script) => {
                        result.insert("script".to_owned(), script.into());
                    }
                    Err(err) => error = err.to_string(),
                }
            }
            Kind::DepJob => {
                sqlx::query("DELETE FROM lsif_dependency_indexing_jobs WHERE upload_id = 960501")
                    .execute(&pool)
                    .await
                    .expect("reset jobs");
                sqlx::query("ALTER SEQUENCE lsif_dependency_indexing_jobs_id_seq1 RESTART WITH 1")
                    .execute(&pool)
                    .await
                    .expect("reset sequence");
                match insert_dependency_indexing_job(&pool, 960501, "GITHUB", sync_time).await {
                    Ok(id) => {
                        result.insert("id".to_owned(), id.into());
                        let row = sqlx::query(
                            "SELECT upload_id, external_service_kind, external_service_sync \
                             FROM lsif_dependency_indexing_jobs WHERE id = $1",
                        )
                        .bind(id)
                        .fetch_one(&pool)
                        .await
                        .expect("read job");
                        let mut row_result = serde_json::Map::new();
                        row_result.insert(
                            "upload_id".to_owned(),
                            row.get::<i32, _>("upload_id").into(),
                        );
                        row_result.insert(
                            "external_service_kind".to_owned(),
                            row.get::<String, _>("external_service_kind").into(),
                        );
                        let sync: Option<OffsetDateTime> = row.get("external_service_sync");
                        row_result.insert(
                            "external_service_sync".to_owned(),
                            match sync {
                                Some(sync) => sync.format(&Rfc3339).expect("format").into(),
                                None => serde_json::Value::Null,
                            },
                        );
                        result.insert("row".to_owned(), row_result.into());
                    }
                    Err(err) => error = err.to_string(),
                }
            }
            Kind::Queue => {
                exec(
                    &pool,
                    "DELETE FROM codeintel_autoindex_queue WHERE repository_id = $1",
                    case.repo,
                )
                .await;
                if case.setup == "row" {
                    sqlx::query(
                        "INSERT INTO codeintel_autoindex_queue (repository_id, rev) VALUES ($1, $2)",
                    )
                    .bind(case.repo)
                    .bind(case.rev)
                    .execute(&pool)
                    .await
                    .expect("insert queue row");
                }
                match queue_repo_rev(&pool, case.repo, case.rev).await {
                    Ok(()) => {
                        let rows: Vec<serde_json::Value> = queue_rows(&pool, case.repo).await;
                        result.insert("rows".to_owned(), rows.into());
                    }
                    Err(err) => error = err.to_string(),
                }
            }
        }
        let snapshot = Snapshot {
            case: case.name,
            result: serde_json::Value::Object(result),
            error,
        };
        println!("caex {}", serde_json::to_string(&snapshot).expect("json"));
    }
}
