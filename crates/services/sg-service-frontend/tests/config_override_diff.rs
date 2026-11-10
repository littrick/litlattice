//! M9.8ge differential harness: `overrideSiteConfig` + `overrideGlobalSettings`
//! and their file watchers.
//!
//! Prints the same `co `/`cg `/`cl ` lines as
//! `cmd/frontend/internal/cli/zz_m9ge_config_override_test.go`.
//!
//! Gated by `SG_CONFIG_OVERRIDE_HARNESS=1`, needs `SG_CONFIG_OVERRIDE_DSN`
//! (a scratch database cloned from `sourcegraph`'s schema) and the scenario in
//! `SG_CONFIG_OVERRIDE_SCENARIO`. File paths come from `SITE_CONFIG_FILE` /
//! `GLOBAL_SETTINGS_FILE` so Go and Rust see the same bytes (the multi-file
//! merge embeds the paths in the document).

use std::fs;
use std::path::PathBuf;
use std::time::Duration;

use sg_service_frontend::configuration_server::FrontendConfigurationServer;
use sg_service_frontend::configuration_source::DbConfigurationSource;
use sg_service_frontend::configuration_startup::override_site_config;
use sg_service_frontend::configuration_watch::{
    override_global_settings, ConfigOverrideWatcher, ConfigWatchPaths,
};
use sg_service_frontend::site_config_file::split_paths;
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
        .expect("truncate critical_and_site_config");
    sqlx::query("TRUNCATE settings RESTART IDENTITY")
        .execute(pool)
        .await
        .expect("truncate settings");
}

async fn site_rows(pool: &PgPool) -> i64 {
    sqlx::query_scalar("SELECT count(*) FROM critical_and_site_config")
        .fetch_one(pool)
        .await
        .expect("count site rows")
}

async fn settings_rows(pool: &PgPool) -> i64 {
    sqlx::query_scalar("SELECT count(*) FROM settings")
        .fetch_one(pool)
        .await
        .expect("count settings rows")
}

/// `(id, author (NULL as 0), contents)` of the newest site config row.
async fn latest_site(pool: &PgPool) -> (i32, i32, String) {
    let row = sqlx::query(
        "SELECT id, COALESCE(author_user_id, 0) AS author, contents \
         FROM critical_and_site_config ORDER BY id DESC LIMIT 1",
    )
    .fetch_one(pool)
    .await
    .expect("latest site config");
    (
        row.try_get("id").expect("id"),
        row.try_get("author").expect("author"),
        row.try_get("contents").expect("contents"),
    )
}

/// `(id, author (NULL as 0), contents)` of the newest settings row.
async fn latest_settings(pool: &PgPool) -> (i32, i32, String) {
    let row = sqlx::query(
        "SELECT id, COALESCE(author_user_id, 0) AS author, contents \
         FROM settings ORDER BY id DESC LIMIT 1",
    )
    .fetch_one(pool)
    .await
    .expect("latest settings");
    (
        row.try_get("id").expect("id"),
        row.try_get("author").expect("author"),
        row.try_get("contents").expect("contents"),
    )
}

fn site_paths_from_env() -> Vec<PathBuf> {
    split_paths(&std::env::var("SITE_CONFIG_FILE").unwrap_or_default())
}

fn global_path_from_env() -> Option<PathBuf> {
    let value = std::env::var("GLOBAL_SETTINGS_FILE").unwrap_or_default();
    if value.is_empty() {
        None
    } else {
        Some(PathBuf::from(value))
    }
}

/// `override_global_settings_from_env`, with the path injected (no global env
/// mutation in parallel tests).
async fn run_global(pool: &PgPool) -> Result<bool, String> {
    match global_path_from_env() {
        Some(path) => override_global_settings(pool, &path).await,
        None => Ok(false),
    }
}

async fn print_site(pool: &PgPool, tag: &str, result: &Result<bool, String>) {
    match result {
        Ok(_) => {
            let (id, author, contents) = latest_site(pool).await;
            println!(
                "co {tag} err=nil rows={} id={id} author={author} contents={}",
                site_rows(pool).await,
                flat(&contents)
            );
        }
        Err(err) => {
            let (id, author, contents) = latest_site(pool).await;
            println!(
                "co {tag} err={} rows={} id={id} author={author} contents={}",
                flat(err),
                site_rows(pool).await,
                flat(&contents)
            );
        }
    }
}

async fn print_settings(pool: &PgPool, tag: &str, updated: bool, result: &Result<bool, String>) {
    match result {
        Ok(_) => {
            let rows = settings_rows(pool).await;
            if rows == 0 {
                println!("cg {tag} err=nil updated={} rows=0", i32::from(updated));
                return;
            }
            let (id, author, contents) = latest_settings(pool).await;
            println!(
                "cg {tag} err=nil updated={} rows={rows} id={id} author={author} contents={}",
                i32::from(updated),
                flat(&contents)
            );
        }
        Err(err) => {
            println!(
                "cg {tag} err={} rows={}",
                flat(err),
                settings_rows(pool).await
            );
        }
    }
}

const WATCH_NEW_SITE: &str =
    r#"{"auth.providers":[{"type":"builtin","allowSignup":true}],"disableAutoGitUpdates":true}"#;

#[tokio::test]
async fn config_override_harness() {
    if std::env::var("SG_CONFIG_OVERRIDE_HARNESS").as_deref() != Ok("1") {
        return;
    }
    let dsn = std::env::var("SG_CONFIG_OVERRIDE_DSN").expect("SG_CONFIG_OVERRIDE_DSN is not set");
    let scenario = std::env::var("SG_CONFIG_OVERRIDE_SCENARIO").unwrap_or_default();

    let pool = PgPool::connect(&dsn).await.expect("connect");
    let source = DbConfigurationSource::new(pool.clone()).expect("configuration source");

    match scenario.as_str() {
        "site-apply" | "site-same" | "site-missing" | "site-multi" => {
            reset(&pool).await;
            let paths = site_paths_from_env();
            print_site(
                &pool,
                &format!("{scenario}-first"),
                &override_site_config(&source, &paths).await,
            )
            .await;
            if scenario == "site-same" {
                print_site(
                    &pool,
                    &format!("{scenario}-second"),
                    &override_site_config(&source, &paths).await,
                )
                .await;
            }
        }
        "site-invalid" => {
            // The exact parse error text differs from Go's JSONC parser; only
            // compare that the write is rejected and no row is added.
            reset(&pool).await;
            let paths = site_paths_from_env();
            let result = override_site_config(&source, &paths).await;
            println!(
                "co site-invalid error={} rows={}",
                i32::from(result.is_err()),
                site_rows(&pool).await
            );
        }
        "global-apply" | "global-same" | "global-human" | "global-missing" | "global-invalid"
        | "global-empty" => {
            reset(&pool).await;
            if scenario == "global-human" {
                // `settings.author_user_id` has an FK to `users`.
                sqlx::query(
                    "INSERT INTO users (id, username) VALUES (7, 'm9ge-human') \
                     ON CONFLICT DO NOTHING",
                )
                .execute(&pool)
                .await
                .expect("insert human user");
                sqlx::query(
                    "INSERT INTO settings (author_user_id, contents) \
                     VALUES (7, '{\"search.contextLines\":1}')",
                )
                .execute(&pool)
                .await
                .expect("insert human settings");
            }
            let before = settings_rows(&pool).await;
            let result = run_global(&pool).await;
            let updated = settings_rows(&pool).await > before;
            if scenario == "global-invalid" {
                println!(
                    "cg global-invalid error={} rows={}",
                    i32::from(result.is_err()),
                    settings_rows(&pool).await
                );
            } else {
                print_settings(&pool, &format!("{scenario}-first"), updated, &result).await;
                if scenario == "global-same" {
                    let before = settings_rows(&pool).await;
                    let second = run_global(&pool).await;
                    let updated = settings_rows(&pool).await > before;
                    print_settings(&pool, &format!("{scenario}-second"), updated, &second).await;
                }
            }
        }
        "site-watch-db" => {
            reset(&pool).await;
            let paths = site_paths_from_env();
            let path = paths.first().expect("SITE_CONFIG_FILE path").clone();
            let _ = override_site_config(&source, &paths)
                .await
                .expect("initial override");
            let (initial_id, _, _) = latest_site(&pool).await;

            let watcher = ConfigOverrideWatcher::new(
                source.clone(),
                pool.clone(),
                ConfigWatchPaths {
                    site_config_files: paths.clone(),
                    global_settings_file: None,
                },
                Duration::from_millis(50),
            )
            .expect("watcher");
            let shutdown = CancellationToken::new();
            let handle = tokio::spawn(watcher.run(shutdown.clone()));

            fs::write(&path, WATCH_NEW_SITE).expect("rewrite watched file");
            let mut changed = 0;
            for _ in 0..200 {
                tokio::time::sleep(Duration::from_millis(50)).await;
                if latest_site(&pool).await.2 == WATCH_NEW_SITE {
                    changed = 1;
                    break;
                }
            }
            shutdown.cancel();
            let _ = handle.await;

            println!(
                "cl site-watch-db initial-id={initial_id} rows={} changed={changed} contents={}",
                site_rows(&pool).await,
                flat(&latest_site(&pool).await.2)
            );
        }
        "site-watch-live" => {
            reset(&pool).await;
            let paths = site_paths_from_env();
            let path = paths.first().expect("SITE_CONFIG_FILE path").clone();
            let initial_contents = fs::read_to_string(&path).expect("read initial file");
            let _ = override_site_config(&source, &paths)
                .await
                .expect("initial override");

            let config_shutdown = CancellationToken::new();
            let wiring = FrontendConfigurationServer::start_with_source(
                source.clone(),
                config_shutdown.clone(),
            )
            .await
            .expect("start configuration server");

            let watcher = ConfigOverrideWatcher::new(
                source.clone(),
                pool.clone(),
                ConfigWatchPaths {
                    site_config_files: paths.clone(),
                    global_settings_file: None,
                },
                Duration::from_millis(50),
            )
            .expect("watcher");
            let watcher_shutdown = CancellationToken::new();
            let handle = tokio::spawn(watcher.run(watcher_shutdown.clone()));

            fs::write(&path, WATCH_NEW_SITE).expect("rewrite watched file");
            let mut changed = 0;
            for _ in 0..400 {
                tokio::time::sleep(Duration::from_millis(50)).await;
                // The update loop's raw source advanced to the new document.
                if wiring.store().raw().site == WATCH_NEW_SITE {
                    changed = 1;
                    break;
                }
            }
            // Item 3: the `LiveConfig` snapshot reflects the same update.
            let live = wiring.live().current();
            let live_flag = i32::from(
                live.site
                    .raw()
                    .get("disableAutoGitUpdates")
                    .and_then(|value| value.as_bool())
                    == Some(true),
            );

            watcher_shutdown.cancel();
            let _ = handle.await;
            wiring.shutdown().await;

            println!(
                "cl site-watch-live initial={} changed={changed} live={live_flag}",
                flat(&initial_contents)
            );
        }
        other => panic!("unknown scenario {other}"),
    }
}
