//! M9.15b differential: the production adapters behind the M9.14c diagnostics
//! seams.
//!
//! Sections:
//!   * `jobs-*`: `recorder.GetBackgroundJobInfos` semantics. The Go oracle
//!     seeds the same dataset into Redis; this side uses
//!     [`InMemoryRecorderSource`], so the comparison is reader-for-reader.
//!   * `gitservers-*`: `clientImplementor.SystemsInfo` aggregation.
//!   * `db`: the production `ProductionDiagnosticsStore` SQL inputs
//!     (`GetLatestSyncErrors`, `GetRepoStatistics`, `ZoektRepos().GetStatistics`,
//!     `GetGitserverGitDirSize`, `CountRepoEmbeddings`) against a scratch
//!     database seeded by `diagnostics_production_fixtures.sql`.
//!
//! Gated by `M9_15B_DIAGNOSTICS_OUT`; the corpus path comes from
//! `M9_15B_DIAGNOSTICS_CORPUS`, the scratch-DB DSN from `SG_QTEST_DSN`.
//! Timestamps inside `runStats:` dataset keys use `__TODAY__`,
//! `__YESTERDAY__`, `__2DAYS__` placeholders resolved to the current UTC day
//! by both sides.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use serde::Deserialize;
use time::format_description::well_known::Rfc3339;
use time::{Duration, OffsetDateTime, UtcOffset};

use sg_service_frontend::diagnostics_adapters::{
    get_background_job_infos, systems_info, DiskInfoData, EmptyIndexedStatsSource,
    GitserverDiskInfoSource, InMemoryRecorderSource, ProductionDiagnosticsStore,
};
use sg_service_frontend::diagnostics_graphql::{DiagnosticsStore, GitserverInfoData, RoutineStatsData};

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Case {
    name: String,
    kind: String,
    #[serde(default)]
    after: String,
    #[serde(default)]
    recent_run_count: i32,
    #[serde(default)]
    day_count: i32,
    #[serde(default)]
    hash: HashMap<String, HashMap<String, String>>,
    #[serde(default)]
    values: HashMap<String, String>,
    #[serde(default)]
    lists: HashMap<String, Vec<String>>,
    #[serde(default)]
    servers: Vec<ServerSpec>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ServerSpec {
    address: String,
    #[serde(default)]
    free_space: u64,
    #[serde(default)]
    total_space: u64,
    #[serde(default)]
    percent_used: f32,
    #[serde(default)]
    error: Option<String>,
}

fn output_dir() -> Option<PathBuf> {
    let out = std::env::var("M9_15B_DIAGNOSTICS_OUT").ok()?;
    Some(PathBuf::from(out))
}

fn corpus() -> Vec<Case> {
    let path = std::env::var("M9_15B_DIAGNOSTICS_CORPUS").expect("corpus path");
    let raw = std::fs::read_to_string(path).expect("read corpus");
    serde_json::from_str(&raw).expect("parse corpus")
}

fn write_json(out: &PathBuf, name: &str, value: &serde_json::Value) {
    std::fs::write(
        out.join(format!("{name}.json")),
        serde_json::to_vec(value).expect("encode"),
    )
    .expect("write output");
}

fn date_key(now: OffsetDateTime, days_ago: i64) -> String {
    let date = (now.to_offset(UtcOffset::UTC) - Duration::days(days_ago)).date();
    format!("{:04}-{:02}-{:02}", date.year(), u8::from(date.month()), date.day())
}

fn expand_placeholders(key: &str, now: OffsetDateTime) -> String {
    key.replace("__TODAY__", &date_key(now, 0))
        .replace("__YESTERDAY__", &date_key(now, 1))
        .replace("__2DAYS__", &date_key(now, 2))
}

fn time_string(value: Option<OffsetDateTime>) -> String {
    match value {
        Some(value) => value
            .format(&Rfc3339)
            .unwrap_or_else(|_| "0001-01-01T00:00:00Z".to_owned()),
        None => "0001-01-01T00:00:00Z".to_owned(),
    }
}

fn stats_json(stats: &RoutineStatsData) -> serde_json::Value {
    serde_json::json!({
        "since": time_string(stats.since),
        "runCount": stats.run_count,
        "errorCount": stats.error_count,
        "minDurationMs": stats.min_duration_ms,
        "avgDurationMs": stats.avg_duration_ms,
        "maxDurationMs": stats.max_duration_ms,
    })
}

#[tokio::test]
async fn background_jobs_match_the_go_oracle() {
    let Some(out) = output_dir() else {
        return;
    };
    let now = OffsetDateTime::now_utc();
    for case in corpus().into_iter().filter(|case| case.kind == "jobs") {
        let source = InMemoryRecorderSource::new();
        for (hash, fields) in &case.hash {
            for (field, value) in fields {
                source.set_hash(hash, field, value);
            }
        }
        for (key, value) in &case.values {
            source.set_value(&expand_placeholders(key, now), value);
        }
        for (key, items) in &case.lists {
            source.set_list(&expand_placeholders(key, now), items.clone());
        }

        let jobs = get_background_job_infos(
            &source,
            &case.after,
            case.recent_run_count,
            case.day_count,
            now,
        )
        .await
        .expect("reader");

        let encoded = jobs
            .iter()
            .map(|job| {
                serde_json::json!({
                    "id": job.id,
                    "name": job.name,
                    "Routines": job.routines.iter().map(|routine| serde_json::json!({
                        "name": routine.name,
                        "type": routine.routine_type,
                        "jobName": job.name,
                        "description": routine.description,
                        "intervalMs": routine.interval_ms,
                        "Instances": routine.instances.iter().map(|instance| serde_json::json!({
                            "hostName": instance.host_name,
                            "lastStartedAt": instance.last_started_at.map(|value| time_string(Some(value))),
                            "LastStoppedAt": instance.last_stopped_at.map(|value| time_string(Some(value))),
                        })).collect::<Vec<_>>(),
                        "RecentRuns": routine.recent_runs.iter().map(|run| serde_json::json!({
                            "at": time_string(Some(run.at)),
                            "hostname": run.host_name,
                            "durationMs": run.duration_ms,
                            "errorMessage": run.error_message,
                        })).collect::<Vec<_>>(),
                        "Stats": stats_json(&routine.stats),
                    })).collect::<Vec<_>>(),
                })
            })
            .collect::<Vec<_>>();
        write_json(&out, &case.name, &serde_json::Value::Array(encoded));
    }
}

struct FakeServers {
    servers: Vec<ServerSpec>,
}

#[async_trait]
impl GitserverDiskInfoSource for FakeServers {
    async fn addresses(&self) -> Vec<String> {
        self.servers.iter().map(|server| server.address.clone()).collect()
    }

    async fn disk_info(&self, address: &str) -> Result<DiskInfoData, String> {
        let server = self
            .servers
            .iter()
            .find(|server| server.address == address)
            .expect("address in corpus");
        match &server.error {
            Some(message) => Err(message.clone()),
            None => Ok(DiskInfoData {
                free_space: server.free_space,
                total_space: server.total_space,
                percent_used: server.percent_used,
            }),
        }
    }
}

#[tokio::test]
async fn gitservers_match_the_go_oracle() {
    let Some(out) = output_dir() else {
        return;
    };
    for case in corpus().into_iter().filter(|case| case.kind == "gitservers") {
        let source = FakeServers {
            servers: case.servers,
        };
        let (infos, error) = match systems_info(&source).await {
            Ok(mut infos) => {
                infos.sort_by(|a, b| a.address.cmp(&b.address));
                (infos, None)
            }
            Err(message) => (Vec::<GitserverInfoData>::new(), Some(message)),
        };
        let encoded = serde_json::json!({
            "infos": infos.iter().map(|info| serde_json::json!({
                "address": info.address,
                "freeSpace": info.free_space,
                "totalSpace": info.total_space,
                "percentUsed": info.percent_used(),
            })).collect::<Vec<_>>(),
            "error": error,
        });
        write_json(&out, &case.name, &encoded);
    }
}

struct NoGitservers;

#[async_trait]
impl GitserverDiskInfoSource for NoGitservers {
    async fn addresses(&self) -> Vec<String> {
        Vec::new()
    }

    async fn disk_info(&self, _address: &str) -> Result<DiskInfoData, String> {
        Err("no address".to_owned())
    }
}

#[tokio::test]
async fn production_store_sql_matches_the_go_oracle() {
    let Some(out) = output_dir() else {
        return;
    };
    let Ok(dsn) = std::env::var("SG_QTEST_DSN") else {
        return;
    };
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(2)
        .connect(&dsn)
        .await
        .expect("connect scratch db");
    let store = ProductionDiagnosticsStore::new(
        pool,
        Arc::new(InMemoryRecorderSource::new()),
        Arc::new(NoGitservers),
        Some(Arc::new(EmptyIndexedStatsSource)),
    );
    let inputs = store.status_message_inputs(true).await.expect("inputs");
    let repository = store.repository_stats().await;
    let stats = repository.stats.expect("repo statistics");
    let zoekt = inputs.zoekt.expect("zoekt statistics");
    let encoded = serde_json::json!({
        "syncErrors": inputs.sync_errors.iter().map(|error| serde_json::json!({
            "serviceId": error.service_id,
            "message": error.message,
        })).collect::<Vec<_>>(),
        "stats": {
            "total": stats.total,
            "softDeleted": stats.soft_deleted,
            "notCloned": stats.not_cloned,
            "cloning": stats.cloning,
            "cloned": stats.cloned,
            "failedFetch": stats.failed_fetch,
            "corrupted": stats.corrupted,
        },
        "zoekt": {
            "total": zoekt.total,
            "indexed": zoekt.indexed,
            "notIndexed": zoekt.not_indexed,
        },
        "gitDirBytes": repository.git_dir_bytes.expect("git dir bytes"),
        "embedded": repository.embedded.expect("embedded count"),
    });
    write_json(&out, "db", &encoded);
}
