#![forbid(unsafe_code)]

//! Differential harness for the syncer slice: drives the Rust port through the
//! same scenario as the baseline `repos.Syncer` (see the Go program
//! `p9syncerdiff` in the sourcegraph checkout) and prints one JSON object per
//! phase.
//!
//! Usage: `SG_TEST_DATABASE_DSN=postgres://... cargo run --example syncer_dump <suffix>`

use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use sg_db::external_service_repos::ExternalServiceReposStore;
use sg_db::external_services::ExternalServiceStore;
use sg_service_repo_updater::scheduler::SystemClock;
use sg_service_repo_updater::syncer::{
    ExtsvcSourceFactory, ProgressRecorder, SyncProgress, Syncer,
};
use sqlx::postgres::PgPool;
use tokio_util::sync::CancellationToken;

struct RecordingProgress(Arc<Mutex<Option<SyncProgress>>>);

#[async_trait]
impl ProgressRecorder for RecordingProgress {
    async fn record(&self, progress: SyncProgress, final_call: bool) -> Result<(), String> {
        if final_call {
            *self.0.lock().expect("progress") = Some(progress);
        }
        Ok(())
    }
}

fn normalize(name: &str, suffix: &str) -> String {
    let mut out = String::with_capacity(name.len());
    let mut rest = name;
    loop {
        match rest.find("DELETED-") {
            Some(index) => {
                out.push_str(&rest[..index]);
                let tail = &rest[index..];
                let digits: String = tail["DELETED-".len()..]
                    .chars()
                    .take_while(|c| c.is_ascii_digit() || *c == '.')
                    .collect();
                if digits.is_empty() {
                    out.push_str("DELETED-");
                    rest = &tail["DELETED-".len()..];
                } else {
                    out.push_str("DELETED-<ts>-");
                    rest = &tail["DELETED-".len() + digits.len()..];
                    // The timestamp separator is part of the prefix.
                    rest = rest.strip_prefix('-').unwrap_or(rest);
                }
            }
            None => {
                out.push_str(rest);
                break;
            }
        }
    }
    out.replace(suffix, "<suffix>")
}

fn scrub(value: &str, suffix: &str) -> String {
    value.replace(suffix, "<suffix>")
}

async fn dump_state(
    pool: &PgPool,
    service_id: i64,
    suffix: &str,
) -> (Vec<serde_json::Value>, bool, i64) {
    type RepoRow = (String, String, bool, String, String, bool, bool, String);
    let rows: Vec<RepoRow> = sqlx::query_as(
        "SELECT r.name, r.uri, r.private, r.external_service_type, r.external_service_id, \
                r.deleted_at IS NOT NULL, r.updated_at IS NOT NULL, COALESCE(esr.clone_url, '') \
         FROM repo r \
         LEFT JOIN external_service_repos esr ON esr.repo_id = r.id AND esr.external_service_id = $1 \
         WHERE r.name LIKE '%' || $2 || '%' \
         ORDER BY r.name",
    )
    .bind(service_id)
    .bind(suffix)
    .fetch_all(pool)
    .await
    .expect("dump repos");

    let mut repos: Vec<serde_json::Value> = rows
        .into_iter()
        .map(
            |(name, uri, private, ext_type, ext_id, deleted, updated_set, clone_url)| {
                serde_json::json!({
                    "name": normalize(&name, suffix),
                    "uri": scrub(&uri, suffix),
                    "private": private,
                    "external_service_type": ext_type,
                    "external_service_id": scrub(&ext_id, suffix),
                    "deleted": deleted,
                    "updated_at_set": updated_set,
                    "clone_url": scrub(&clone_url, suffix),
                })
            },
        )
        .collect();
    repos.sort_by(|a, b| a["name"].as_str().cmp(&b["name"].as_str()));

    let (last, next): (Option<sg_db::OffsetDateTime>, Option<sg_db::OffsetDateTime>) =
        sqlx::query_as("SELECT last_sync_at, next_sync_at FROM external_services WHERE id = $1")
            .bind(service_id)
            .fetch_one(pool)
            .await
            .expect("timestamps");
    match (last, next) {
        (Some(last), Some(next)) => (repos, true, (next - last).whole_seconds()),
        _ => (repos, last.is_some(), -1),
    }
}

#[tokio::main]
async fn main() {
    let Ok(dsn) = std::env::var("SG_TEST_DATABASE_DSN") else {
        eprintln!("SG_TEST_DATABASE_DSN is required");
        std::process::exit(2);
    };
    let suffix = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "p9rust".to_owned());
    let base = format!("https://{suffix}.example.com");

    let pool: PgPool = sg_db::connect(&dsn, 5).await.expect("connect");

    let service_id: i64 = sqlx::query_scalar(
        "INSERT INTO external_services(kind, display_name, config, unrestricted) \
         VALUES ('OTHER', 'p9-syncer-diff', $1, true) RETURNING id",
    )
    .bind(format!(
        r#"{{"url":"{base}","repos":["/r1.git","/r2.git"]}}"#
    ))
    .fetch_one(&pool)
    .await
    .expect("insert service");

    let syncer = Syncer::new(
        ExternalServiceStore::new(pool.clone()),
        ExternalServiceReposStore::new(pool.clone()),
        Arc::new(ExtsvcSourceFactory::new(None)),
        None,
        Arc::new(SystemClock),
    );
    let token = CancellationToken::new();

    let update_config = |list: &'static str| {
        let pool = pool.clone();
        let base = base.clone();
        async move {
            sqlx::query("UPDATE external_services SET config = $1 WHERE id = $2")
                .bind(format!(r#"{{"url":"{base}","repos":{list}}}"#))
                .bind(service_id)
                .execute(&pool)
                .await
                .expect("update config");
        }
    };

    let run = |phase: &'static str, progress: Arc<Mutex<Option<SyncProgress>>>| {
        let syncer = &syncer;
        let pool = pool.clone();
        let suffix = suffix.clone();
        let token = token.clone();
        async move {
            let result = syncer
                .sync_external_service(
                    service_id,
                    Duration::from_secs(60),
                    &RecordingProgress(progress.clone()),
                    &token,
                )
                .await;
            let final_progress = *progress.lock().expect("progress");
            let (repos, last_set, delta) = dump_state(&pool, service_id, &suffix).await;
            let mut out = serde_json::json!({
                "phase": phase,
                "sync_failed": result.is_err(),
                "sync_error": result.err().map(|err| scrub(&err, &suffix)).unwrap_or_default(),
                "repos": repos,
                "last_sync_set": last_set,
                "next_minus_last_seconds": delta,
            });
            if let Some(p) = final_progress {
                out["progress"] = serde_json::json!({
                    "synced": p.synced,
                    "errors": p.errors,
                    "added": p.added,
                    "removed": p.removed,
                    "modified": p.modified,
                    "unmodified": p.unmodified,
                    "deleted": p.deleted,
                });
            }
            println!("{}", serde_json::to_string(&out).expect("json"));
        }
    };

    let progress = || Arc::new(Mutex::new(None::<SyncProgress>));

    run("A_first_sync", progress()).await;
    run("B_second_sync", progress()).await;

    update_config(r#"["/r1.git","/r3.git"]"#).await;
    run("C_config_change", progress()).await;

    update_config(r#"["src-expose"]"#).await;
    run("D_source_error", progress()).await;

    update_config(r#"["/r1.git","/r3.git"]"#).await;
    sqlx::query("UPDATE repo SET deleted_at = NOW() WHERE name LIKE '%' || $1 || '%' AND deleted_at IS NULL")
        .bind(&suffix)
        .execute(&pool)
        .await
        .expect("soft delete out of band");
    run("E_revive", progress()).await;

    if std::env::var("KEEP").is_err() {
        sqlx::query("DELETE FROM external_service_repos WHERE external_service_id = $1")
            .bind(service_id)
            .execute(&pool)
            .await
            .expect("cleanup associations");
        sqlx::query("DELETE FROM repo WHERE name LIKE '%' || $1 || '%'")
            .bind(&suffix)
            .execute(&pool)
            .await
            .expect("cleanup repos");
        sqlx::query("DELETE FROM external_services WHERE id = $1")
            .bind(service_id)
            .execute(&pool)
            .await
            .expect("cleanup service");
    }
}
