//! M9.31c differential of `policies.Matcher.CommitsDescribedByPolicy`.
//!
//! Both sides consume `data/codeintel_matcher/corpus.json` and write one JSON
//! document per case (normalized: map keys sorted, match slices sorted by
//! name/policy id/commit date). The Go oracle is the untracked gated
//! `internal/codeintel/policies/zz_m931c_policy_matcher_oracle_test.go`.
//!
//! Env:
//!
//! * `SG_M931C_POLICY_MATCHER_DIFF=1`
//! * `M931C_CORPUS` — corpus JSON path
//! * `M931C_OUT` — output directory

#![allow(clippy::unwrap_used)]

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use time::{Duration, OffsetDateTime};

use sg_db::codeintel_policies::ConfigurationPolicy;
use sg_service_frontend::codeintel_policies_matcher::{
    Extractor, GetCommitError, MatcherCommit, MatcherGit, MatcherRef, PolicyMatcher, RefType,
};

#[derive(Deserialize)]
struct Corpus {
    cases: Vec<Case>,
}

#[derive(Deserialize)]
struct Case {
    name: String,
    extractor: String,
    include_tip: bool,
    filter_by_created_date: bool,
    now: i64,
    #[serde(default)]
    policies: Vec<Policy>,
    #[serde(default)]
    refs: Vec<Ref>,
    #[serde(default)]
    get_commit: BTreeMap<String, Commit>,
    #[serde(default)]
    commits: BTreeMap<String, Vec<Commit>>,
    #[serde(default)]
    filter_commits: Vec<String>,
}

#[derive(Deserialize)]
struct Policy {
    id: i32,
    #[serde(rename = "type")]
    policy_type: String,
    pattern: String,
    #[serde(default)]
    retention_duration_hours: Option<i32>,
    #[serde(default)]
    retain_intermediate_commits: bool,
    #[serde(default)]
    index_commit_max_age_hours: Option<i32>,
    #[serde(default)]
    index_intermediate_commits: bool,
}

#[derive(Deserialize)]
struct Ref {
    short_name: String,
    commit: String,
    created_at: i64,
    #[serde(rename = "type")]
    ref_type: String,
    is_head: bool,
}

#[derive(Deserialize)]
struct Commit {
    id: String,
    committed_at: i64,
}

#[derive(Serialize)]
struct OutMatch {
    name: String,
    policy_id: Option<i32>,
    duration_hours: Option<i64>,
    committed_at: i64,
}

struct FakeGit<'a> {
    case: &'a Case,
}

#[async_trait::async_trait]
impl MatcherGit for FakeGit<'_> {
    async fn list_refs(
        &self,
        _repo_id: i32,
        _repo: &str,
        points_at_commit: &[String],
    ) -> Result<Vec<MatcherRef>, String> {
        Ok(self
            .case
            .refs
            .iter()
            .filter(|git_ref| {
                points_at_commit.is_empty() || points_at_commit.iter().any(|c| c == &git_ref.commit)
            })
            .map(|git_ref| MatcherRef {
                short_name: git_ref.short_name.clone(),
                commit: git_ref.commit.clone(),
                created_at: OffsetDateTime::from_unix_timestamp(git_ref.created_at)
                    .unwrap_or(OffsetDateTime::UNIX_EPOCH),
                ref_type: match git_ref.ref_type.as_str() {
                    "tag" => RefType::Tag,
                    "branch" => RefType::Branch,
                    _ => RefType::Unknown,
                },
                is_head: git_ref.is_head,
            })
            .collect())
    }

    async fn get_commit(
        &self,
        _repo_id: i32,
        _repo: &str,
        commit: &str,
    ) -> Result<MatcherCommit, GetCommitError> {
        match self.case.get_commit.get(commit) {
            Some(commit) => Ok(MatcherCommit {
                id: commit.id.clone(),
                committer_date: OffsetDateTime::from_unix_timestamp(commit.committed_at)
                    .unwrap_or(OffsetDateTime::UNIX_EPOCH),
            }),
            None => Err(GetCommitError::RevisionNotFound),
        }
    }

    async fn commits(
        &self,
        _repo_id: i32,
        _repo: &str,
        ranges: &[String],
        after: Option<OffsetDateTime>,
    ) -> Result<Vec<MatcherCommit>, String> {
        let key = ranges.first().cloned().unwrap_or_default();
        Ok(self
            .case
            .commits
            .get(&key)
            .map(Vec::as_slice)
            .unwrap_or(&[])
            .iter()
            .filter(|commit| {
                let date = OffsetDateTime::from_unix_timestamp(commit.committed_at)
                    .unwrap_or(OffsetDateTime::UNIX_EPOCH);
                after.is_none_or(|after| date >= after)
            })
            .map(|commit| MatcherCommit {
                id: commit.id.clone(),
                committer_date: OffsetDateTime::from_unix_timestamp(commit.committed_at)
                    .unwrap_or(OffsetDateTime::UNIX_EPOCH),
            })
            .collect())
    }
}

fn build_policy(policy: &Policy) -> ConfigurationPolicy {
    ConfigurationPolicy {
        id: policy.id,
        repository_id: None,
        repository_patterns: None,
        name: None,
        policy_type: policy.policy_type.clone(),
        pattern: policy.pattern.clone(),
        protected: false,
        retention_enabled: false,
        retention_duration_hours: policy.retention_duration_hours,
        retain_intermediate_commits: policy.retain_intermediate_commits,
        indexing_enabled: false,
        syntactic_indexing_enabled: false,
        index_commit_max_age_hours: policy.index_commit_max_age_hours,
        index_intermediate_commits: policy.index_intermediate_commits,
        embeddings_enabled: false,
    }
}

fn extractor(name: &str) -> Extractor {
    match name {
        "indexing" => Extractor::Indexing,
        "noop" => Extractor::Noop,
        _ => Extractor::Retention,
    }
}

fn timestamp(seconds: i64) -> OffsetDateTime {
    OffsetDateTime::from_unix_timestamp(seconds).unwrap_or(OffsetDateTime::UNIX_EPOCH)
}

#[tokio::test(flavor = "multi_thread")]
async fn codeintel_policies_matcher_diff() {
    if std::env::var("SG_M931C_POLICY_MATCHER_DIFF").as_deref() != Ok("1") {
        return;
    }
    let corpus_path = std::env::var("M931C_CORPUS").expect("M931C_CORPUS");
    let out = std::env::var("M931C_OUT").expect("M931C_OUT");
    std::fs::create_dir_all(&out).expect("create out");
    let raw = std::fs::read_to_string(corpus_path).expect("read corpus");
    let corpus: Corpus = serde_json::from_str(&raw).expect("parse corpus");

    for case in &corpus.cases {
        let policies: Vec<ConfigurationPolicy> = case.policies.iter().map(build_policy).collect();
        let fake = FakeGit { case };
        let matcher = PolicyMatcher::new(
            &fake,
            extractor(&case.extractor),
            case.include_tip,
            case.filter_by_created_date,
        );
        let matches = matcher
            .commits_described_by_policy(
                1,
                "repo",
                &policies,
                timestamp(case.now),
                &case.filter_commits,
            )
            .await
            .expect("matcher");

        let output: BTreeMap<String, Vec<OutMatch>> = matches
            .into_iter()
            .map(|(commit, policy_matches)| {
                let mut items: Vec<OutMatch> = policy_matches
                    .into_iter()
                    .map(|policy_match| OutMatch {
                        name: policy_match.name,
                        policy_id: policy_match.policy_id,
                        duration_hours: policy_match
                            .policy_duration
                            .map(|duration: Duration| duration.whole_hours()),
                        committed_at: policy_match.committed_at.unix_timestamp(),
                    })
                    .collect();
                items.sort_by(|a, b| {
                    a.name
                        .cmp(&b.name)
                        .then_with(|| {
                            a.policy_id
                                .unwrap_or(-1)
                                .cmp(&b.policy_id.unwrap_or(-1))
                        })
                        .then_with(|| a.committed_at.cmp(&b.committed_at))
                });
                (commit, items)
            })
            .collect();

        let body = serde_json::to_string(&output).expect("serialize");
        std::fs::write(std::path::Path::new(&out).join(format!("{}.json", case.name)), body)
            .expect("write");
        println!("RUST {} len={}", case.name, output.len());
    }
}
