//! M9.8ei differential harness: prints the same `cr` lines as
//! `cmd/frontend/internal/cli/zz_configuration_source_harness_test.go`.
//!
//! Expects `SG_CONFIG_SOURCE_HARNESS=1`, `SG_CONFIG_SOURCE_DSN` pointing at a
//! scratch database cloned from the `sourcegraph` schema, and the scenario in
//! `SG_CONFIG_SOURCE_SCENARIO`. Each scenario resets the table itself.

use sg_config::server::ConfigurationSource;
use sg_service_frontend::configuration_source::DbConfigurationSource;
use sqlx::PgPool;

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
        other => panic!("unknown scenario {other}"),
    }
}
