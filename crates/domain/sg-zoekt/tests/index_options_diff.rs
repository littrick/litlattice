//! M9.8gs differential harness: `internal/search/backend/index_options.go`
//! against [`sg_zoekt::index_options`].
//!
//! Run with `SG_M9GS_DIFF=1 M9GS_CORPUS=<json> M9GS_OUT=<json>` (the wrapper
//! script in `test_ws/p9-go` does this). Both sides emit the same normalized
//! JSON; database/gitserver lookups and `GetVersion` are injected as closures.

use std::collections::{BTreeMap, BTreeSet, HashMap};

use serde::Deserialize;
use serde_json::json;

use sg_zoekt::index_options::{
    get_index_options, GetVersionFn, RepoIndexOptions, SiteConfiguration,
};

#[derive(Deserialize)]
struct CorpusCase {
    name: String,
    #[serde(default)]
    config: Option<serde_json::Value>,
    #[serde(default)]
    repos: Vec<RepoSpec>,
}

#[derive(Deserialize, Clone, Default)]
struct RepoSpec {
    id: i32,
    #[serde(default)]
    name: String,
    #[serde(default)]
    public: bool,
    #[serde(default)]
    fork: bool,
    #[serde(default)]
    archived: bool,
    #[serde(default)]
    priority: f64,
    #[serde(default)]
    document_ranks_version: String,
    #[serde(default)]
    repo_error: String,
    #[serde(default)]
    get_version: Option<GetVersionSpec>,
    #[serde(default)]
    context_revs: Option<Vec<String>>,
    #[serde(default)]
    context_error: String,
}

#[derive(Deserialize, Clone, Default)]
struct GetVersionSpec {
    #[serde(default)]
    mode: String,
    #[serde(default)]
    empty: Vec<String>,
    #[serde(default)]
    error: String,
    #[serde(default)]
    versions: BTreeMap<String, String>,
}

fn version_fn(spec: Option<&GetVersionSpec>) -> GetVersionFn {
    let spec = spec.cloned().unwrap_or_default();
    let empty: BTreeSet<String> = spec.empty.into_iter().collect();
    Box::new(move |branch: &str| match spec.mode.as_str() {
        "fail" => {
            let message = if spec.error.is_empty() {
                "boom".to_owned()
            } else {
                spec.error.clone()
            };
            Err(message)
        }
        "empty" => {
            if empty.contains(branch) {
                Ok(String::new())
            } else {
                Ok(format!("!{branch}"))
            }
        }
        "versions" => Ok(spec.versions.get(branch).cloned().unwrap_or_default()),
        "bang" | "" => Ok(format!("!{branch}")),
        other => Err(format!("unknown get_version mode {other:?}")),
    })
}

#[test]
fn index_options_diff() {
    if std::env::var("SG_M9GS_DIFF").as_deref() != Ok("1") {
        return;
    }
    let corpus_path =
        std::env::var("M9GS_CORPUS").expect("M9GS_CORPUS must be set when SG_M9GS_DIFF=1");
    let out_path = std::env::var("M9GS_OUT").expect("M9GS_OUT must be set when SG_M9GS_DIFF=1");

    let raw = std::fs::read_to_string(&corpus_path).expect("read corpus");
    let cases: Vec<CorpusCase> = serde_json::from_str(&raw).expect("parse corpus");

    let mut out = Vec::with_capacity(cases.len());
    for case in &cases {
        let config: SiteConfiguration = match &case.config {
            Some(config) => serde_json::from_value(config.clone()).expect("parse site config"),
            None => SiteConfiguration::default(),
        };

        let by_id: HashMap<i32, RepoSpec> = case
            .repos
            .iter()
            .cloned()
            .map(|repo| (repo.id, repo))
            .collect();
        let ids: Vec<i32> = case.repos.iter().map(|repo| repo.id).collect();

        let get_repo_index_options = |repo_id: i32| -> Result<RepoIndexOptions, String> {
            let repo = by_id
                .get(&repo_id)
                .ok_or_else(|| format!("unknown repo {repo_id}"))?;
            if !repo.repo_error.is_empty() {
                return Err(repo.repo_error.clone());
            }
            Ok(RepoIndexOptions {
                name: repo.name.clone(),
                repo_id,
                public: repo.public,
                priority: repo.priority,
                document_ranks_version: repo.document_ranks_version.clone(),
                fork: repo.fork,
                archived: repo.archived,
                get_version: version_fn(repo.get_version.as_ref()),
            })
        };

        let get_search_context_revisions = |repo_id: i32| -> Result<Vec<String>, String> {
            let repo = by_id
                .get(&repo_id)
                .ok_or_else(|| format!("unknown repo {repo_id}"))?;
            if !repo.context_error.is_empty() {
                return Err(repo.context_error.clone());
            }
            Ok(repo.context_revs.clone().unwrap_or_default())
        };

        let options = get_index_options(
            &config,
            get_repo_index_options,
            get_search_context_revisions,
            &ids,
        );
        out.push(json!({
            "name": case.name,
            "options": options,
        }));
    }

    let encoded = serde_json::to_string(&out).expect("encode output");
    std::fs::write(&out_path, encoded).expect("write output");
}
