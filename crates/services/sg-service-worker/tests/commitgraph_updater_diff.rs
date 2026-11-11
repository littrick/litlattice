//! M9.8in differential harness: drive the ported `codeintel-commitgraph-updater`
//! job over a corpus with fake store/gitserver/locker/config seams and emit
//! normalized JSON for a byte-for-byte comparison with the Go oracle
//! (`internal/codeintel/uploads/internal/commitgraph/zz_m9in_oracle_test.go`).
//!
//! The Go oracle reproduces the (unexported) `commitGraphUpdater` control flow
//! because it lives in the package under test; this harness exercises the real
//! Rust `CommitGraphUpdater` plus the real `sg-codeintel` graph construction in
//! the fake store's `update_uploads_visible_to_commits`.
//!
//! Env:
//!
//! * `SG_M9IN_DIFF=1` enable the harness
//! * `M9IN_CORPUS` corpus JSON
//! * `M9IN_OUT` output JSON

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use prost_types::Timestamp;
use serde::Deserialize;
use serde_json::{json, Value};
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;

use sg_codeintel::commitgraph::{CommitGraph, CommitGraphView, Graph, UploadMeta};
use sg_git::{CommitsOptions, CommitsOrder, GitRef, RefType};
use sg_service_worker::jobs::codeintel_commitgraph_updater::{
    CommitGraphConfigSource, CommitGraphGitserver, CommitGraphLocker, CommitGraphStore,
    CommitGraphUpdater, CommitWithDate, DirtyRepository, LockGuard, RefsByCommit,
};

#[derive(Debug, Deserialize)]
struct Case {
    name: String,
    #[serde(default)]
    site_config_default_branch_only: Vec<String>,
    #[serde(default)]
    dirty_error: String,
    #[serde(default)]
    max_age_for_non_stale_branches_seconds: i64,
    #[serde(default)]
    max_age_for_non_stale_tags_seconds: i64,
    #[serde(default)]
    repositories: Vec<RepoSpec>,
}

#[derive(Debug, Default, Clone, Deserialize)]
struct RepoSpec {
    #[serde(default)]
    repository_id: i64,
    #[serde(default)]
    repository_name: String,
    #[serde(default)]
    dirty_token: i64,
    #[serde(default)]
    oldest_upload: Option<OldestUploadSpec>,
    #[serde(default)]
    oldest_upload_error: String,
    #[serde(default)]
    lock_acquired: Option<bool>,
    #[serde(default)]
    lock_error: String,
    #[serde(default)]
    default_branch: Option<DefaultBranchSpec>,
    #[serde(default)]
    commits: Vec<CommitSpec>,
    #[serde(default)]
    commits_error: String,
    #[serde(default)]
    refs: Vec<RefSpec>,
    #[serde(default)]
    refs_error: String,
    #[serde(default)]
    update_error: String,
    #[serde(default)]
    unlock_error: String,
    #[serde(default)]
    uploads: Vec<UploadSpec>,
    #[serde(default)]
    visible_at_commits: Vec<String>,
}

#[derive(Debug, Clone, Deserialize)]
struct OldestUploadSpec {
    #[serde(default)]
    commit: String,
    #[serde(default)]
    committer_date: String,
}

#[derive(Debug, Clone, Deserialize)]
struct DefaultBranchSpec {
    #[serde(default)]
    ref_name: String,
    #[serde(default)]
    commit: String,
    #[serde(default)]
    error: String,
}

#[derive(Debug, Clone, Deserialize)]
struct CommitSpec {
    #[serde(default)]
    id: String,
    #[serde(default)]
    parents: Vec<String>,
}

#[derive(Debug, Clone, Deserialize)]
struct RefSpec {
    #[serde(default)]
    name: String,
    #[serde(default)]
    commit_id: String,
}

#[derive(Debug, Clone, Deserialize)]
struct UploadSpec {
    #[serde(default)]
    upload_id: i64,
    #[serde(default)]
    commit: String,
    #[serde(default)]
    token: String,
    #[serde(default)]
    distance: u32,
}

// ----- timestamps ----------------------------------------------------------

fn parse_timestamp(value: &str) -> Timestamp {
    let date = OffsetDateTime::parse(value, &Rfc3339)
        .unwrap_or_else(|err| panic!("invalid RFC3339 timestamp {value:?}: {err}"));
    Timestamp {
        seconds: date.unix_timestamp(),
        nanos: date.nanosecond() as i32,
    }
}

fn format_timestamp(timestamp: &Timestamp) -> String {
    let date = OffsetDateTime::from_unix_timestamp(timestamp.seconds)
        .expect("timestamp seconds in range")
        .replace_nanosecond(timestamp.nanos as u32)
        .expect("timestamp nanos in range");
    date.format(&Rfc3339).expect("format RFC3339")
}

// ----- fake seams ----------------------------------------------------------

struct FakeStore {
    spec: Arc<RepoSpec>,
    out: Arc<Mutex<RepoOut>>,
}

#[async_trait]
impl CommitGraphStore for FakeStore {
    async fn get_dirty_repositories(&self) -> Result<Vec<DirtyRepository>, String> {
        Ok(vec![DirtyRepository {
            repository_id: self.spec.repository_id,
            repository_name: self.spec.repository_name.clone(),
            dirty_token: self.spec.dirty_token,
        }])
    }

    async fn get_commit_and_date_for_oldest_upload(
        &self,
        _repository_id: i64,
    ) -> Result<Option<CommitWithDate>, String> {
        self.out.lock().unwrap().oldest_upload_called = true;
        if !self.spec.oldest_upload_error.is_empty() {
            return Err(self.spec.oldest_upload_error.clone());
        }
        let Some(spec) = &self.spec.oldest_upload else {
            return Ok(None);
        };
        let committer_date = parse_timestamp(&spec.committer_date);
        self.out.lock().unwrap().oldest_upload =
            Some((spec.commit.clone(), format_timestamp(&committer_date)));
        Ok(Some(CommitWithDate {
            commit: spec.commit.clone(),
            committer_date,
        }))
    }

    async fn update_uploads_visible_to_commits(
        &self,
        repository_id: i64,
        commit_graph: &CommitGraph,
        refs: &RefsByCommit,
        max_age_for_non_stale_branches: std::time::Duration,
        max_age_for_non_stale_tags: std::time::Duration,
        dirty_token: i64,
        now: Option<Timestamp>,
    ) -> Result<(), String> {
        {
            let mut out = self.out.lock().unwrap();
            out.update_called = true;
            out.update_call = Some(json!({
                "repository_id": repository_id,
                "max_age_for_non_stale_branches_seconds": max_age_for_non_stale_branches.as_secs(),
                "max_age_for_non_stale_tags_seconds": max_age_for_non_stale_tags.as_secs(),
                "dirty_token": dirty_token,
                "now_zero": now.is_none(),
            }));
        }
        if !self.spec.update_error.is_empty() {
            return Err(self.spec.update_error.clone());
        }

        let mut view = CommitGraphView::default();
        for upload in &self.spec.uploads {
            view.add(
                UploadMeta {
                    upload_id: upload.upload_id,
                    distance: upload.distance,
                },
                &upload.commit,
                &upload.token,
            );
        }
        let decorated = Graph::new(commit_graph, &view);
        let refs_by_commit = render_refs_by_commit(refs);
        let graph = render_graph(commit_graph, &decorated, &self.spec.visible_at_commits);

        let mut out = self.out.lock().unwrap();
        out.refs_by_commit = Some(refs_by_commit);
        out.graph = Some(graph);
        Ok(())
    }
}

struct DirtyErrorStore {
    error: String,
}

#[async_trait]
impl CommitGraphStore for DirtyErrorStore {
    async fn get_dirty_repositories(&self) -> Result<Vec<DirtyRepository>, String> {
        Err(self.error.clone())
    }

    async fn get_commit_and_date_for_oldest_upload(
        &self,
        _repository_id: i64,
    ) -> Result<Option<CommitWithDate>, String> {
        unreachable!("dirty repository query failed")
    }

    async fn update_uploads_visible_to_commits(
        &self,
        _repository_id: i64,
        _commit_graph: &CommitGraph,
        _refs: &RefsByCommit,
        _max_age_for_non_stale_branches: std::time::Duration,
        _max_age_for_non_stale_tags: std::time::Duration,
        _dirty_token: i64,
        _now: Option<Timestamp>,
    ) -> Result<(), String> {
        unreachable!("dirty repository query failed")
    }
}

struct FakeLocker {
    spec: Arc<RepoSpec>,
    out: Arc<Mutex<RepoOut>>,
}

struct FakeLockGuard {
    out: Arc<Mutex<RepoOut>>,
    unlock_error: String,
}

#[async_trait]
impl LockGuard for FakeLockGuard {
    async fn unlock(self: Box<Self>, error: Option<String>) -> Option<String> {
        self.out.lock().unwrap().unlock_called = true;
        if self.unlock_error.is_empty() {
            error
        } else {
            Some(self.unlock_error)
        }
    }
}

#[async_trait]
impl CommitGraphLocker for FakeLocker {
    async fn lock(&self, key: i64, blocking: bool) -> Result<Option<Box<dyn LockGuard>>, String> {
        {
            let mut out = self.out.lock().unwrap();
            out.lock_called = true;
            out.lock_key = key;
            out.lock_blocking = blocking;
        }
        if !self.spec.lock_error.is_empty() {
            return Err(self.spec.lock_error.clone());
        }
        let acquired = self.spec.lock_acquired.unwrap_or(true);
        self.out.lock().unwrap().lock_acquired = acquired;
        if !acquired {
            return Ok(None);
        }
        Ok(Some(Box::new(FakeLockGuard {
            out: Arc::clone(&self.out),
            unlock_error: self.spec.unlock_error.clone(),
        })))
    }
}

struct FakeGitserver {
    spec: Arc<RepoSpec>,
    out: Arc<Mutex<RepoOut>>,
}

#[async_trait]
impl CommitGraphGitserver for FakeGitserver {
    async fn list_refs(
        &self,
        repo: &str,
        heads_only: bool,
        tags_only: bool,
    ) -> Result<Vec<GitRef>, String> {
        self.out.lock().unwrap().list_refs_call = Some(json!({
            "repo": repo,
            "heads_only": heads_only,
            "tags_only": tags_only,
        }));
        if !self.spec.refs_error.is_empty() {
            return Err(self.spec.refs_error.clone());
        }
        Ok(self
            .spec
            .refs
            .iter()
            .map(|reference| GitRef {
                name: reference.name.clone(),
                short_name: String::new(),
                target_commit: reference.commit_id.clone(),
                ref_oid: String::new(),
                created_at: None,
                ref_type: RefType::Unknown,
                is_head: false,
            })
            .collect())
    }

    async fn get_default_branch(
        &self,
        repo: &str,
        short: bool,
    ) -> Result<(String, String), String> {
        {
            let mut out = self.out.lock().unwrap();
            out.default_branch_called = true;
            out.default_branch_call = Some(json!({"repo": repo, "short": short}));
        }
        match &self.spec.default_branch {
            None => Err("no default branch configured".to_owned()),
            Some(branch) if !branch.error.is_empty() => Err(branch.error.clone()),
            Some(branch) => Ok((branch.ref_name.clone(), branch.commit.clone())),
        }
    }

    async fn commits(
        &self,
        repo: &str,
        options: &CommitsOptions,
    ) -> Result<Vec<sg_git::Commit>, String> {
        self.out.lock().unwrap().commits_call = Some(json!({
            "repo": repo,
            "all_refs": options.all_refs,
            "ranges": options.ranges,
            "order": render_order(options.order),
            "after": options.after.as_ref().map(format_timestamp),
            "max_commits": options.max_commits,
        }));
        if !self.spec.commits_error.is_empty() {
            return Err(self.spec.commits_error.clone());
        }
        let commits: Vec<sg_git::Commit> = self
            .spec
            .commits
            .iter()
            .map(|commit| sg_git::Commit {
                oid: commit.id.clone(),
                parents: commit.parents.clone(),
                author_name: String::new(),
                author_email: String::new(),
                author_date: None,
                committer_date: None,
                message: String::new(),
            })
            .collect();
        self.out.lock().unwrap().commits = Some(json!(render_commits(&self.spec.commits)));
        Ok(commits)
    }
}

struct StaticPatterns(Vec<String>);

impl CommitGraphConfigSource for StaticPatterns {
    fn default_branch_only(&self) -> Vec<String> {
        self.0.clone()
    }
}

struct NoLocker;

#[async_trait]
impl CommitGraphLocker for NoLocker {
    async fn lock(&self, _key: i64, _blocking: bool) -> Result<Option<Box<dyn LockGuard>>, String> {
        unreachable!("no repositories are processed after a dirty query failure")
    }
}

struct NoGitserver;

#[async_trait]
impl CommitGraphGitserver for NoGitserver {
    async fn list_refs(
        &self,
        _repo: &str,
        _heads_only: bool,
        _tags_only: bool,
    ) -> Result<Vec<GitRef>, String> {
        unreachable!("no repositories are processed after a dirty query failure")
    }

    async fn get_default_branch(
        &self,
        _repo: &str,
        _short: bool,
    ) -> Result<(String, String), String> {
        unreachable!("no repositories are processed after a dirty query failure")
    }

    async fn commits(
        &self,
        _repo: &str,
        _options: &CommitsOptions,
    ) -> Result<Vec<sg_git::Commit>, String> {
        unreachable!("no repositories are processed after a dirty query failure")
    }
}

// ----- normalized output ---------------------------------------------------

#[derive(Default)]
struct RepoOut {
    repository_id: i64,
    repository_name: String,
    dirty_token: i64,

    lock_called: bool,
    lock_key: i64,
    lock_blocking: bool,
    lock_acquired: bool,
    unlock_called: bool,
    oldest_upload_called: bool,
    oldest_upload: Option<(String, String)>,
    default_branch_called: bool,
    default_branch_call: Option<Value>,
    commits_call: Option<Value>,
    commits: Option<Value>,
    list_refs_call: Option<Value>,
    refs_by_commit: Option<Value>,
    update_called: bool,
    update_call: Option<Value>,
    graph: Option<Value>,
    error: Option<String>,
}

impl RepoOut {
    fn new(spec: &RepoSpec) -> Self {
        Self {
            repository_id: spec.repository_id,
            repository_name: spec.repository_name.clone(),
            dirty_token: spec.dirty_token,
            ..Self::default()
        }
    }

    fn to_json(&self) -> Value {
        json!({
            "repository_id": self.repository_id,
            "repository_name": self.repository_name,
            "dirty_token": self.dirty_token,
            "lock_called": self.lock_called,
            "lock_key": self.lock_key,
            "lock_blocking": self.lock_blocking,
            "lock_acquired": self.lock_acquired,
            "unlock_called": self.unlock_called,
            "oldest_upload_called": self.oldest_upload_called,
            "oldest_upload": self.oldest_upload.as_ref().map(|(commit, date)| json!({
                "commit": commit,
                "committer_date": date,
            })),
            "default_branch_called": self.default_branch_called,
            "default_branch_call": self.default_branch_call,
            "commits_call": self.commits_call,
            "commits": self.commits,
            "list_refs_call": self.list_refs_call,
            "refs_by_commit": self.refs_by_commit,
            "update_called": self.update_called,
            "update_call": self.update_call,
            "graph": self.graph,
            "error": self.error,
        })
    }
}

fn render_order(order: Option<CommitsOrder>) -> Option<&'static str> {
    match order {
        Some(CommitsOrder::TopoDate) => Some("topo-date"),
        Some(CommitsOrder::CommitDate) => Some("commit-date"),
        Some(CommitsOrder::Default) => Some("default"),
        None => None,
    }
}

fn render_commits(commits: &[CommitSpec]) -> Vec<Value> {
    commits
        .iter()
        .map(|commit| {
            json!({
                "id": commit.id,
                "parents": commit.parents,
            })
        })
        .collect()
}

fn render_refs_by_commit(refs: &RefsByCommit) -> Value {
    let mut rendered = serde_json::Map::new();
    for (commit, commit_refs) in refs {
        rendered.insert(
            commit.clone(),
            json!(commit_refs
                .iter()
                .map(|reference| reference.name.clone())
                .collect::<Vec<_>>()),
        );
    }
    Value::Object(rendered)
}

fn render_upload_metas(list: &[UploadMeta]) -> Vec<Value> {
    let mut sorted: Vec<&UploadMeta> = list.iter().collect();
    sorted.sort_by_key(|upload| upload.upload_id);
    sorted
        .into_iter()
        .map(|upload| {
            json!({
                "upload_id": upload.upload_id,
                "distance": upload.distance,
            })
        })
        .collect()
}

fn render_graph(
    commit_graph: &CommitGraph,
    decorated: &Graph,
    visible_at_commits: &[String],
) -> Value {
    let nodes = commit_graph
        .graph()
        .iter()
        .map(|(commit, parents)| {
            json!({
                "commit": commit,
                "parents": parents,
            })
        })
        .collect::<Vec<_>>();

    let order = decorated.commits().to_vec();

    let (uploads, links) = decorated.gather();
    let uploads_out = uploads
        .iter()
        .map(|(commit, metas)| {
            json!({
                "commit": commit,
                "uploads": render_upload_metas(metas),
            })
        })
        .collect::<Vec<_>>();
    let links_out = links
        .values()
        .map(|link| {
            json!({
                "commit": link.commit,
                "ancestor": link.ancestor_commit,
                "distance": link.distance,
            })
        })
        .collect::<Vec<_>>();

    let visible_at = visible_at_commits
        .iter()
        .map(|commit| {
            json!({
                "commit": commit,
                "uploads": render_upload_metas(&decorated.uploads_visible_at_commit(commit)),
            })
        })
        .collect::<Vec<_>>();

    json!({
        "nodes": nodes,
        "order": order,
        "uploads": uploads_out,
        "links": links_out,
        "visible_at": visible_at,
    })
}

// ----- test entrypoint -----------------------------------------------------

#[tokio::test]
async fn commitgraph_updater_diff() {
    if std::env::var("SG_M9IN_DIFF").as_deref() != Ok("1") {
        return;
    }
    let corpus_path = std::env::var("M9IN_CORPUS").expect("M9IN_CORPUS must be set");
    let out_path = std::env::var("M9IN_OUT").expect("M9IN_OUT must be set");

    let raw = std::fs::read_to_string(&corpus_path).expect("read corpus");
    let cases: Vec<Case> = serde_json::from_str(&raw).expect("parse corpus");

    let mut output = Vec::with_capacity(cases.len());
    for case in &cases {
        output.push(run_case(case).await);
    }

    let encoded = serde_json::to_vec(&Value::Array(output)).expect("encode output");
    std::fs::write(&out_path, encoded).expect("write output");
}

async fn run_case(case: &Case) -> Value {
    let max_age_for_non_stale_branches =
        std::time::Duration::from_secs(case.max_age_for_non_stale_branches_seconds as u64);
    let max_age_for_non_stale_tags =
        std::time::Duration::from_secs(case.max_age_for_non_stale_tags_seconds as u64);

    if !case.dirty_error.is_empty() {
        let updater = CommitGraphUpdater::new(
            Arc::new(DirtyErrorStore {
                error: case.dirty_error.clone(),
            }),
            Arc::new(NoLocker),
            Arc::new(NoGitserver),
            Arc::new(StaticPatterns(case.site_config_default_branch_only.clone())),
        );
        let errors = updater
            .update_all_dirty_commit_graphs(
                max_age_for_non_stale_branches,
                max_age_for_non_stale_tags,
            )
            .await
            .expect_err("dirty repository query must fail");
        return json!({
            "name": case.name,
            "dirty_error": errors.first(),
            "repositories": [],
            "errors": [],
        });
    }

    let mut repository_outputs = Vec::new();
    let mut errors = Vec::new();

    for spec in &case.repositories {
        let spec = Arc::new(spec.clone());
        let out = Arc::new(Mutex::new(RepoOut::new(&spec)));
        let updater = CommitGraphUpdater::new(
            Arc::new(FakeStore {
                spec: Arc::clone(&spec),
                out: Arc::clone(&out),
            }),
            Arc::new(FakeLocker {
                spec: Arc::clone(&spec),
                out: Arc::clone(&out),
            }),
            Arc::new(FakeGitserver {
                spec: Arc::clone(&spec),
                out: Arc::clone(&out),
            }),
            Arc::new(StaticPatterns(case.site_config_default_branch_only.clone())),
        );

        let result = updater
            .update_all_dirty_commit_graphs(
                max_age_for_non_stale_branches,
                max_age_for_non_stale_tags,
            )
            .await;
        if let Err(repository_errors) = result {
            let error = repository_errors
                .first()
                .cloned()
                .expect("a failed repository has at least one error");
            out.lock().unwrap().error = Some(error.clone());
            errors.push(Value::String(error));
        }
        repository_outputs.push(out.lock().unwrap().to_json());
    }

    json!({
        "name": case.name,
        "dirty_error": Value::Null,
        "repositories": repository_outputs,
        "errors": errors,
    })
}
