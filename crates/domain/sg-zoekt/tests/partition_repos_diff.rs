//! M9.8gx differential harness: `internal/search/zoekt/indexed_search.go`
//! repository partitioning (`zoektIndexedRepos`, and the portable part of
//! `PartitionRepos`) against
//! [`sg_zoekt::indexed_repo_revs::{zoekt_indexed_repos, partition_repos}`].
//!
//! Run with `SG_M9GX_DIFF=1 M9GX_CORPUS=<json> M9GX_OUT=<json>` (the wrapper
//! script in `test_ws/p9-go` does this). Both sides emit the same normalized
//! JSON: repo id keys are decimal strings, roaring bitmaps decode to ascending
//! ids, unindexed lists keep input order, and partition errors collapse to a
//! classification string (`none` / `index_only_unavailable`).

use std::collections::BTreeMap;

use serde::Deserialize;
use serde_json::{json, Value};

use sg_search_query::validate::YesNoOnly;
use sg_zoekt::indexed_repo_revs::{
    partition_repos, zoekt_indexed_repos, IndexedRepoRevs, PartitionError, RepoMap,
    RepositoryRevisions, RepositoryRevisionsRepo,
};
use sg_zoekt::query::IndexedRequestType;
use sg_zoekt::types::{ListResponse, MinimalRepoListEntry, RepositoryBranch};

#[derive(Deserialize)]
struct Corpus {
    #[serde(default)]
    zoekt_indexed_repos: Vec<IndexedCase>,
    #[serde(default)]
    partition_repos: Vec<PartitionCase>,
}

#[derive(Deserialize)]
struct IndexedCase {
    name: String,
    #[serde(default)]
    revs: Vec<RepoSpec>,
    #[serde(default)]
    entries: Vec<EntrySpec>,
    #[serde(default)]
    filter: String,
}

#[derive(Deserialize)]
struct PartitionCase {
    name: String,
    #[serde(default)]
    repos: Vec<RepoSpec>,
    #[serde(default)]
    typ: String,
    #[serde(default)]
    use_index: String,
    #[serde(default)]
    contains_ref_globs: bool,
    #[serde(default)]
    list: ListSpec,
}

#[derive(Deserialize, Default)]
struct ListSpec {
    #[serde(default)]
    error: bool,
    #[serde(default)]
    entries: Vec<EntrySpec>,
}

#[derive(Deserialize)]
struct RepoSpec {
    id: u32,
    #[serde(default)]
    name: String,
    #[serde(default)]
    revs: Vec<String>,
}

#[derive(Deserialize)]
struct EntrySpec {
    id: u32,
    #[serde(default)]
    has_symbols: bool,
    #[serde(default)]
    branches: Vec<BranchSpec>,
}

#[derive(Deserialize)]
struct BranchSpec {
    name: String,
    #[serde(default)]
    version: String,
}

impl RepoSpec {
    fn to_repository_revisions(&self) -> RepositoryRevisions {
        RepositoryRevisions {
            repo: RepositoryRevisionsRepo {
                id: self.id,
                name: self.name.clone(),
            },
            revs: self.revs.clone(),
        }
    }
}

impl EntrySpec {
    fn to_entry(&self) -> MinimalRepoListEntry {
        MinimalRepoListEntry {
            has_symbols: self.has_symbols,
            branches: self
                .branches
                .iter()
                .map(|branch| RepositoryBranch {
                    name: branch.name.clone(),
                    version: branch.version.clone(),
                })
                .collect(),
            ..MinimalRepoListEntry::default()
        }
    }
}

#[test]
fn partition_repos_diff() {
    if std::env::var("SG_M9GX_DIFF").as_deref() != Ok("1") {
        return;
    }
    let corpus_path =
        std::env::var("M9GX_CORPUS").expect("M9GX_CORPUS must be set when SG_M9GX_DIFF=1");
    let out_path = std::env::var("M9GX_OUT").expect("M9GX_OUT must be set when SG_M9GX_DIFF=1");

    let raw = std::fs::read_to_string(&corpus_path).expect("read corpus");
    let corpus: Corpus = serde_json::from_str(&raw).expect("parse corpus");

    let indexed_out: Vec<Value> = corpus
        .zoekt_indexed_repos
        .iter()
        .map(run_indexed_case)
        .collect();
    let partition_out: Vec<Value> = corpus
        .partition_repos
        .iter()
        .map(run_partition_case)
        .collect();

    let output = json!({
        "zoekt_indexed_repos": indexed_out,
        "partition_repos": partition_out,
    });
    std::fs::write(
        &out_path,
        serde_json::to_string(&output).expect("encode output"),
    )
    .expect("write output");
}

fn run_indexed_case(case: &IndexedCase) -> Value {
    let indexed_set: RepoMap = case
        .entries
        .iter()
        .map(|entry| (entry.id, entry.to_entry()))
        .collect();
    let revs: Vec<RepositoryRevisions> = case
        .revs
        .iter()
        .map(RepoSpec::to_repository_revisions)
        .collect();

    let filter = |entry: &MinimalRepoListEntry| entry.has_symbols;
    let filter: Option<&dyn Fn(&MinimalRepoListEntry) -> bool> = if case.filter == "symbols" {
        Some(&filter)
    } else {
        None
    };

    let (indexed, unindexed) = zoekt_indexed_repos(&indexed_set, &revs, filter);
    json!({
        "name": case.name,
        "indexed": indexed_json(&indexed),
        "unindexed": unindexed_json(&unindexed),
    })
}

fn run_partition_case(case: &PartitionCase) -> Value {
    let repos: Vec<RepositoryRevisions> = case
        .repos
        .iter()
        .map(RepoSpec::to_repository_revisions)
        .collect();

    let typ = if case.typ == "symbol" {
        IndexedRequestType::Symbol
    } else {
        IndexedRequestType::Text
    };
    let use_index = match case.use_index.as_str() {
        "no" => YesNoOnly::No,
        "only" => YesNoOnly::Only,
        "invalid" => YesNoOnly::Invalid,
        _ => YesNoOnly::Yes,
    };

    let response = ListResponse {
        repos_map: case
            .list
            .entries
            .iter()
            .map(|entry| (entry.id, entry.to_entry()))
            .collect(),
        ..ListResponse::default()
    };
    let list_error = case.list.error;
    let mut list_calls = 0;

    let result = partition_repos(&repos, typ, use_index, case.contains_ref_globs, || {
        list_calls += 1;
        if list_error {
            Err::<ListResponse, &'static str>("list failed")
        } else {
            Ok(response.clone())
        }
    });

    let (indexed, unindexed, error) = match result {
        Ok((indexed, unindexed)) => (indexed, unindexed, "none"),
        Err(PartitionError::IndexOnlyUnavailable) => (
            IndexedRepoRevs::default(),
            Vec::new(),
            "index_only_unavailable",
        ),
    };

    json!({
        "name": case.name,
        "indexed": indexed_json(&indexed),
        "unindexed": unindexed_json(&unindexed),
        "error": error,
        "list_calls": list_calls,
    })
}

fn indexed_json(indexed: &IndexedRepoRevs) -> Value {
    json!({
        "repo_revs": indexed
            .repo_revs
            .iter()
            .map(|(id, reporev)| (id.to_string(), reporev.revs.clone()))
            .collect::<BTreeMap<String, Vec<String>>>(),
        "branch_repos": indexed
            .branch_repos()
            .into_iter()
            .map(|branch| (branch.branch, branch.repos.iter().collect::<Vec<u32>>()))
            .collect::<BTreeMap<String, Vec<u32>>>(),
    })
}

fn unindexed_json(unindexed: &[RepositoryRevisions]) -> Value {
    Value::Array(
        unindexed
            .iter()
            .map(|reporev| {
                json!({
                    "id": reporev.repo.id,
                    "name": reporev.repo.name,
                    "revs": reporev.revs,
                })
            })
            .collect(),
    )
}
