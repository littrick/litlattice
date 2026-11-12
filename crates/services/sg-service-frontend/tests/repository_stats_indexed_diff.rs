//! M9.15b differential: `RepositoryStats.indexed` / `indexedLinesCount`.
//!
//! The Go oracle uses `backend.FakeStreamer` + `search.ListAllIndexed` and
//! replays the six-line `repository_stats.go` computation (the resolver's
//! `search.Indexed()` global is not injectable). This side feeds the identical
//! list response through `list_all_indexed` and then through the real
//! `RepositoryStatsObject` resolver, so the `int32` conversions, the
//! `min(indexed, total)` clamp, the per-field error precedence and the
//! `BigInt`/`Int` encoding are all the production implementation.
//!
//! Gated by `M9_15B_INDEXED_OUT`; corpus path from `M9_15B_INDEXED_CORPUS`.

use std::collections::BTreeMap;
use std::fmt;
use std::path::PathBuf;

use serde::Deserialize;

use sg_db::repo_statistics::RepoStatistics;
use sg_graphql::exec::{Actor, Arguments, FieldValue, ObjectValue};
use sg_graphql::json::Json;
use sg_service_frontend::diagnostics_adapters::IndexedStats;
use sg_service_frontend::diagnostics_graphql::{RepositoryStatsData, RepositoryStatsObject};
use sg_service_frontend::search_env::list_all_indexed;
use sg_zoekt::query::Query;
use sg_zoekt::streamer::Streamer;
use sg_zoekt::types::{
    ListOptions, ListResponse, MinimalRepoListEntry, RepoListEntry, RepoStats, Repository,
    SearchChunk, SearchOptions,
};

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Case {
    name: String,
    #[serde(default)]
    repos: Vec<RepoSpec>,
    #[serde(default)]
    total: i64,
    #[serde(default)]
    list_error: Option<String>,
    #[serde(default)]
    repo_stats_error: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RepoSpec {
    id: u32,
    #[serde(default)]
    default_branch_lines: u64,
    #[serde(default)]
    other_branches_lines: u64,
}

#[derive(Debug, Clone)]
struct FakeError(String);

impl fmt::Display for FakeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl std::error::Error for FakeError {}

struct FakeStreamer {
    repos: Vec<RepoSpec>,
    error: Option<String>,
}

impl Streamer for FakeStreamer {
    type Error = FakeError;

    async fn list(
        &self,
        _query: &Query,
        _opts: Option<ListOptions>,
    ) -> Result<ListResponse, Self::Error> {
        if let Some(message) = &self.error {
            return Err(FakeError(message.clone()));
        }
        let mut repos_map = BTreeMap::new();
        let mut stats = RepoStats::default();
        for repo in &self.repos {
            repos_map.insert(repo.id, MinimalRepoListEntry::default());
            stats.default_branch_new_lines_count += repo.default_branch_lines;
            stats.other_branches_new_lines_count += repo.other_branches_lines;
        }
        // `HorizontalSearcher.List` overwrites `Stats.Repos` with
        // `len(Repos) + len(ReposMap)`.
        stats.repos = self.repos.len() as i64;
        Ok(ListResponse {
            repos: self
                .repos
                .iter()
                .map(|repo| RepoListEntry {
                    repository: Repository {
                        id: repo.id,
                        name: format!("repo-{}", repo.id),
                        ..Repository::default()
                    },
                    index_time_unix: 0,
                })
                .collect(),
            repos_map,
            crashes: 0,
            stats,
        })
    }

    async fn stream_search(
        &self,
        _query: &Query,
        _options: &SearchOptions,
        _sender: &mut (dyn FnMut(SearchChunk) + Send),
    ) -> Result<(), Self::Error> {
        Ok(())
    }
}

fn output_dir() -> Option<PathBuf> {
    let out = std::env::var("M9_15B_INDEXED_OUT").ok()?;
    Some(PathBuf::from(out))
}

fn corpus() -> Vec<Case> {
    let path = std::env::var("M9_15B_INDEXED_CORPUS").expect("corpus path");
    let raw = std::fs::read_to_string(path).expect("read corpus");
    serde_json::from_str(&raw).expect("parse corpus")
}

fn field_json(object: &RepositoryStatsObject, name: &str) -> serde_json::Value {
    let arguments = Arguments::default();
    match object.field(name, &arguments, Actor::Anonymous) {
        Ok(Some(FieldValue::Json(Json::Int(value)))) => {
            serde_json::json!({"value": value, "error": null})
        }
        Ok(Some(FieldValue::Json(Json::Str(value)))) => {
            serde_json::json!({"value": value, "error": null})
        }
        Ok(Some(FieldValue::Null)) | Ok(None) => serde_json::json!({"value": null, "error": null}),
        Ok(Some(_)) => panic!("unexpected field value for {name}"),
        Err(error) => serde_json::json!({"value": null, "error": error.message}),
    }
}

#[tokio::test]
async fn repository_stats_indexed_matches_the_go_oracle() {
    let Some(out) = output_dir() else {
        return;
    };
    for case in corpus() {
        let streamer = FakeStreamer {
            repos: case.repos,
            error: case.list_error.clone(),
        };
        let indexed = match list_all_indexed(&streamer).await {
            Ok(all) => Some(Ok(IndexedStats::from_parts(
                all.stats.repos as i64,
                all.stats.default_branch_new_lines_count,
                all.stats.other_branches_new_lines_count,
            ))),
            Err(error) => Some(Err(error.to_string())),
        };
        let data = RepositoryStatsData {
            stats: match &case.repo_stats_error {
                Some(message) => Err(message.clone()),
                None => Ok(RepoStatistics {
                    total: case.total,
                    ..RepoStatistics::default()
                }),
            },
            git_dir_bytes: Ok(0),
            embedded: Ok(0),
            indexed,
        };
        let object = RepositoryStatsObject { data };
        let encoded = serde_json::json!({
            "indexed": field_json(&object, "indexed"),
            "indexedLinesCount": field_json(&object, "indexedLinesCount"),
        });
        std::fs::write(
            out.join(format!("{}.json", case.name)),
            serde_json::to_vec(&encoded).expect("encode"),
        )
        .expect("write output");
    }
}
