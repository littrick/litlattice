//! M9.8ei differential harness: prints the same `cr` lines as
//! `cmd/frontend/internal/cli/zz_configuration_source_harness_test.go`.
//!
//! Expects `SG_CONFIG_SOURCE_HARNESS=1`, `SG_CONFIG_SOURCE_DSN` pointing at a
//! scratch database cloned from the `sourcegraph` schema, and the scenario in
//! `SG_CONFIG_SOURCE_SCENARIO`. Each scenario resets the table itself.

use std::time::Duration;

use sg_config::server::ConfigurationSource;
use sg_config::RawUnified;
use sg_service_frontend::configuration_server::FrontendConfigurationServer;
use sg_service_frontend::configuration_source::DbConfigurationSource;
use sqlx::{PgPool, Row};
use tokio_util::sync::CancellationToken;

fn flat(input: &str) -> String {
    input
        .replace('\\', "\\\\")
        .replace('\n', "\\n")
        .replace('\t', "\\t")
        .replace('\r', "\\r")
}

async fn reset(pool: &PgPool) {
    sqlx::query("TRUNCATE critical_and_site_config RESTART IDENTITY")
        .execute(pool)
        .await
        .expect("truncate");
}

async fn rows(pool: &PgPool) -> i64 {
    sqlx::query_scalar("SELECT count(*) FROM critical_and_site_config")
        .fetch_one(pool)
        .await
        .expect("count")
}

/// The latest row: `(id, author_user_id, contents)`, NULL author as 0.
async fn latest(pool: &PgPool) -> (i32, i32, String) {
    let row = sqlx::query(
        "SELECT id, author_user_id, contents FROM critical_and_site_config \
         ORDER BY id DESC LIMIT 1",
    )
    .fetch_one(pool)
    .await
    .expect("latest");
    let id: i32 = row.try_get("id").expect("id");
    let author: Option<i32> = row.try_get("author_user_id").expect("author");
    let contents: String = row.try_get("contents").expect("contents");
    (id, author.unwrap_or(0), contents)
}

/// The schema requires `auth.providers` (the custom check rejects an empty
/// list), so the write scenarios use the smallest valid document.
const NEW_SITE: &str = r#"{"auth.providers":[{"type":"builtin","allowSignup":true}]}"#;

fn print_write(scenario: &str, result: Result<(), String>, rows: i64, latest: (i32, i32, String)) {
    match result {
        Ok(()) => println!("cw {scenario} write-err=nil rows={rows}"),
        Err(err) => println!("cw {scenario} write-err={} rows={rows}", flat(&err)),
    }
    println!(
        "cw {scenario} latest id={} author={} contents={}",
        latest.0,
        latest.1,
        flat(&latest.2)
    );
}

async fn write_case(
    pool: &PgPool,
    source: &DbConfigurationSource,
    scenario: &str,
    last_id: i32,
    author_user_id: i32,
) {
    let result = source
        .write(
            RawUnified {
                site: NEW_SITE.to_owned(),
                ..RawUnified::default()
            },
            last_id,
            author_user_id,
        )
        .await;
    print_write(scenario, result, rows(pool).await, latest(pool).await);
}

fn print_raw(tag: &str, result: Result<sg_config::RawUnified, String>, rows: i64) {
    match result {
        Ok(raw) => println!(
            "cr {tag} id={} site={} pg={} git={} searchers={} rows={rows}",
            raw.id,
            flat(&raw.site),
            raw.service_connections.postgres_dsn,
            raw.service_connections.git_servers.join(","),
            raw.service_connections.searchers.join(","),
        ),
        Err(err) => println!("cr {tag} err={} rows={rows}", flat(&err)),
    }
}

#[tokio::test]
async fn configuration_source_harness() {
    if std::env::var("SG_CONFIG_SOURCE_HARNESS").as_deref() != Ok("1") {
        return;
    }
    let dsn = std::env::var("SG_CONFIG_SOURCE_DSN").expect("SG_CONFIG_SOURCE_DSN is not set");
    let scenario = std::env::var("SG_CONFIG_SOURCE_SCENARIO").unwrap_or_default();

    let pool = PgPool::connect(&dsn).await.expect("connect");
    let source = DbConfigurationSource::new(pool.clone()).expect("configuration source");

    match scenario.as_str() {
        "seed" => {
            reset(&pool).await;
            let result = source.read().await;
            print_raw("seed", result, rows(&pool).await);
        }
        "again" => {
            reset(&pool).await;
            let first = source.read().await;
            print_raw("again-first", first, rows(&pool).await);
            let second = source.read().await;
            print_raw("again-second", second, rows(&pool).await);
        }
        "existing" => {
            reset(&pool).await;
            sqlx::query(
                "INSERT INTO critical_and_site_config (type, author_user_id, contents, redacted_contents) \
                 VALUES ('site', 7, '{\"externalURL\":\"https://example.com\"}', 'rd')",
            )
            .execute(&pool)
            .await
            .expect("insert");
            let result = source.read().await;
            print_raw("existing", result, rows(&pool).await);
        }
        "dev" => {
            reset(&pool).await;
            let result = source.read().await;
            print_raw("dev", result, rows(&pool).await);
        }
        "write-ok" => {
            reset(&pool).await;
            let raw = source.read().await.expect("seed read");
            write_case(&pool, &source, "write-ok", raw.id, 3).await;
        }
        "write-stale" => {
            reset(&pool).await;
            let raw = source.read().await.expect("seed read");
            write_case(&pool, &source, "write-stale", raw.id + 100, 3).await;
        }
        "write-empty" => {
            reset(&pool).await;
            write_case(&pool, &source, "write-empty", 0, 3).await;
        }
        "write-author" => {
            reset(&pool).await;
            let raw = source.read().await.expect("seed read");
            write_case(&pool, &source, "write-author", raw.id, 7).await;
        }
        "write-zero-author" => {
            reset(&pool).await;
            let raw = source.read().await.expect("seed read");
            write_case(&pool, &source, "write-zero-author", raw.id, 0).await;
        }
        "wiring" => {
            reset(&pool).await;
            let shutdown = CancellationToken::new();
            let wiring = FrontendConfigurationServer::start_with_pool(pool.clone(), shutdown)
                .await
                .expect("start configuration server");
            let initial = wiring.store().raw();
            println!(
                "cw wiring init id={} site={} rows={}",
                initial.id,
                flat(&initial.site),
                rows(&pool).await
            );

            let result = wiring
                .server()
                .write(
                    RawUnified {
                        site: NEW_SITE.to_owned(),
                        ..RawUnified::default()
                    },
                    initial.id,
                    3,
                )
                .await
                .map_err(|err| err.to_string());
            let after = wiring.store().raw();
            let mut restart = 0;
            for _ in 0..50 {
                if wiring.server().need_server_restart() {
                    restart = 1;
                    break;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            let error = match result {
                Ok(()) => "nil".to_owned(),
                Err(err) => flat(&err),
            };
            println!(
                "cw wiring write-err={} rows={} after-id={} after-site={} restart={}",
                error,
                rows(&pool).await,
                after.id,
                flat(&after.site),
                restart
            );
            wiring.shutdown().await;
        }
        other => panic!("unknown scenario {other}"),
    }
}
