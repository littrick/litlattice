//! M9.8gv differential harness: `internal/search/zoekt/indexed_search.go`
//! repo/rev attribution (`IndexedRepoRevs.add`, `BranchRepos`,
//! `GetRepoRevsFromBranchRepos`, `getRepoInputRev`) against
//! [`sg_zoekt::indexed_repo_revs`].
//!
//! Run with `SG_M9GV_DIFF=1 M9GV_CORPUS=<json> M9GV_OUT=<json>` (the wrapper
//! script in `test_ws/p9-go` does this). Both sides emit the same normalized
//! JSON. Repo id keys are decimal strings so Go's map-key ordering and Rust's
//! `BTreeMap<String, _>` ordering agree.

use std::collections::BTreeMap;

use serde::Deserialize;
use serde_json::{json, Value};

use sg_zoekt::indexed_repo_revs::{IndexedRepoRevs, RepositoryRevisions, RepositoryRevisionsRepo};
use sg_zoekt::types::{FileMatch, MinimalRepoListEntry, RepositoryBranch};

#[derive(Deserialize)]
struct CorpusCase {
    name: String,
    #[serde(default)]
    steps: Vec<CorpusStep>,
    #[serde(default)]
    files: Vec<CorpusFile>,
}

#[derive(Deserialize)]
struct CorpusStep {
    repo: CorpusRepo,
    entry: CorpusEntry,
}

#[derive(Deserialize)]
struct CorpusRepo {
    id: u32,
    #[serde(default)]
    name: String,
    #[serde(default)]
    revs: Vec<String>,
}

#[derive(Deserialize, Default)]
struct CorpusEntry {
    #[serde(default)]
    branches: Vec<CorpusBranch>,
}

#[derive(Deserialize)]
struct CorpusBranch {
    name: String,
    #[serde(default)]
    version: String,
}

#[derive(Deserialize)]
struct CorpusFile {
    repository_id: u32,
    #[serde(default)]
    repository: String,
    #[serde(default)]
    version: String,
    #[serde(default)]
    branches: Vec<String>,
}

impl CorpusRepo {
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

impl CorpusEntry {
    fn to_entry(&self) -> MinimalRepoListEntry {
        MinimalRepoListEntry {
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

impl CorpusFile {
    fn to_file_match(&self) -> FileMatch {
        FileMatch {
            repository_id: self.repository_id,
            repository: self.repository.clone(),
            version: self.version.clone(),
            branches: self.branches.clone(),
            ..FileMatch::default()
        }
    }
}

#[test]
fn indexed_repo_revs_diff() {
    if std::env::var("SG_M9GV_DIFF").as_deref() != Ok("1") {
        return;
    }
    let corpus_path =
        std::env::var("M9GV_CORPUS").expect("M9GV_CORPUS must be set when SG_M9GV_DIFF=1");
    let out_path = std::env::var("M9GV_OUT").expect("M9GV_OUT must be set when SG_M9GV_DIFF=1");

    let raw = std::fs::read_to_string(&corpus_path).expect("read corpus");
    let cases: Vec<CorpusCase> = serde_json::from_str(&raw).expect("parse corpus");

    let entries: Vec<Value> = cases.iter().map(run_case).collect();

    std::fs::write(
        &out_path,
        serde_json::to_string(&entries).expect("encode output"),
    )
    .expect("write output");
}

fn run_case(case: &CorpusCase) -> Value {
    let mut indexed = IndexedRepoRevs::default();

    let mut steps = Vec::with_capacity(case.steps.len());
    for step in &case.steps {
        let unindexed = indexed.add(&step.repo.to_repository_revisions(), step.entry.to_entry());
        steps.push(json!({
            "unindexed": unindexed,
            "repo_revs": repo_revs_map(&indexed),
            "branch_repos": branch_repos_map(&indexed),
        }));
    }

    let from_branch_repos: BTreeMap<String, Vec<String>> = indexed
        .get_repo_revs_from_branch_repos()
        .into_iter()
        .map(|(id, reporev)| (id.to_string(), reporev.revs))
        .collect();

    let files: Vec<Value> = case
        .files
        .iter()
        .map(|file| {
            let (repo, revs) = indexed.get_repo_input_rev(&file.to_file_match());
            json!({
                "id": repo.id,
                "name": repo.name,
                "revs": revs,
            })
        })
        .collect();

    json!({
        "name": case.name,
        "steps": steps,
        "get_repo_revs_from_branch_repos": from_branch_repos,
        "get_repo_input_rev": files,
    })
}

fn repo_revs_map(indexed: &IndexedRepoRevs) -> BTreeMap<String, Vec<String>> {
    indexed
        .repo_revs
        .iter()
        .map(|(id, reporev)| (id.to_string(), reporev.revs.clone()))
        .collect()
}

fn branch_repos_map(indexed: &IndexedRepoRevs) -> BTreeMap<String, Vec<u32>> {
    indexed
        .branch_repos()
        .into_iter()
        .map(|branch| (branch.branch, branch.repos.iter().collect()))
        .collect()
}
