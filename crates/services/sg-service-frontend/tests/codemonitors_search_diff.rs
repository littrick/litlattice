//! M9.9f differential harness: `codemonitors.Search` / `Snapshot` /
//! `addCodeMonitorHook` / `hookWithID`.
//!
//! Mirrors `internal/codemonitors/zz_m9f_oracle_test.go` (UNTRACKED, gated by
//! `SG_M9F_DIFF=1`). Both sides consume
//! `test_ws/p9-go/data/codemonitors_search/corpus.json` and print one JSON
//! document; the driver normalizes both with sorted keys before comparing.
//!
//! Env: `SG_M9F_DIFF=1`, `M9F_CORPUS`, `M9F_OUT`.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use serde::{Deserialize, Serialize};

use prost_types::Timestamp;
use sg_proto::gitserver as pb;
use sg_service_frontend::codemonitors_search::{
    add_code_monitor_hook, commit_matches_json, hook_with_id, search, snapshot,
    TriggerSearchStore,
};
use sg_service_frontend::search_client::{Protocol, SearchClient, SearchMode};
use sg_service_frontend::search_commit_job::{
    CodeMonitorHook, CodeMonitorHookHandle, CommitUserDB, CommitUserEmail, DoSearchFunc,
    GitserverClient,
};
use sg_service_frontend::search_job::{
    JobContext, JobError, JobStreamer, PrivateRepoLister, ReposListOptions, SearchJob,
};
use sg_service_frontend::search_jobutil_repos::{RepoPage, RepoPageSource, ZoektRepoLister};
use sg_service_frontend::search_jobutil_tree::{new_plan_job, JobNode};
use sg_service_frontend::search_runtime::CommitRuntimeClientsOwned;
use sg_service_frontend::search_searcher_client::{SearcherError, SearcherErrorKind};
use sg_service_frontend::search_stream::{
    default_plan_deps, inputs_from_plan, tree_inputs, RepoPagerRuntimeClients, TreeRuntimeClients,
};
use sg_zoekt::indexed_repo_revs::{
    MinimalRepo, RepositoryRevisions as ZoektRepositoryRevisions, RepositoryRevisionsRepo,
};
use sg_zoekt::query::Query;
use sg_zoekt::types::{ListResponse, SearchOptions, SearchResult};
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;

#[derive(Deserialize)]
struct Corpus {
    cases: Vec<Case>,
}

#[derive(Default, Deserialize)]
struct Case {
    name: String,
    kind: String,
    #[serde(default)]
    query: String,
    #[serde(default)]
    monitor_id: i64,
    #[serde(default)]
    trigger_id: i32,
    #[serde(default)]
    repos: Vec<RepoSpec>,
    #[serde(default)]
    resolutions: Vec<Resolution>,
    #[serde(default)]
    last_searched: Vec<LastSearched>,
    #[serde(default)]
    store_errors: StoreErrors,
    #[serde(default)]
    search: BTreeMap<String, SearchSpec>,
    #[serde(default)]
    in_transaction: bool,
    #[serde(default)]
    request: RequestSpec,
    #[serde(default)]
    do_search_error: String,
}

#[derive(Default, Deserialize, Clone)]
struct RepoSpec {
    #[serde(default)]
    id: i32,
    #[serde(default)]
    name: String,
}

#[derive(Default, Deserialize, Clone)]
struct Resolution {
    #[serde(default)]
    repo: String,
    #[serde(default)]
    rev: String,
    #[serde(default)]
    sha: String,
    #[serde(default)]
    error_kind: String,
    #[serde(default)]
    error: String,
}

#[derive(Default, Deserialize)]
struct LastSearched {
    #[serde(default)]
    repo_id: i32,
    #[serde(default)]
    commits: Vec<String>,
}

#[derive(Default, Deserialize)]
struct StoreErrors {
    #[serde(default)]
    get: String,
    #[serde(default)]
    upsert: String,
    #[serde(default)]
    update: String,
}

#[derive(Default, Deserialize, Clone)]
struct SearchSpec {
    #[serde(default)]
    commits: Vec<CommitSpec>,
    #[serde(default)]
    error: String,
}

#[derive(Default, Deserialize)]
struct RequestSpec {
    #[serde(default)]
    repo: String,
    #[serde(default)]
    repo_id: i32,
    #[serde(default)]
    revisions: Vec<String>,
    #[serde(default)]
    include_diff: bool,
}

#[derive(Default, Deserialize, Clone)]
struct CommitSpec {
    #[serde(default)]
    oid: String,
    #[serde(default)]
    author: SignatureSpec,
    #[serde(default)]
    committer: SignatureSpec,
    #[serde(default)]
    parents: Vec<String>,
    #[serde(default)]
    message: MatchedSpec,
    #[serde(default)]
    diff: MatchedSpec,
    #[serde(default)]
    modified_files: Vec<String>,
}

#[derive(Default, Deserialize, Clone)]
struct SignatureSpec {
    #[serde(default)]
    name: String,
    #[serde(default)]
    email: String,
    #[serde(default)]
    date: String,
}

#[derive(Default, Deserialize, Clone)]
struct MatchedSpec {
    #[serde(default)]
    content: String,
    #[serde(default)]
    ranges: Vec<RangeSpec>,
}

#[derive(Default, Deserialize, Clone)]
struct RangeSpec {
    #[serde(default)]
    start: Vec<i32>,
    #[serde(default)]
    end: Vec<i32>,
}

#[derive(Serialize)]
struct CaseOutput {
    kind: String,
    name: String,
    result: serde_json::Value,
}

#[derive(Serialize, Clone)]
struct ResolveCall {
    repo: String,
    rev: String,
    ensure_revision: bool,
}

#[derive(Serialize, Clone)]
struct SearchCall {
    repo: String,
    revisions: Vec<String>,
    include_diff: bool,
}

#[derive(Serialize, Clone)]
struct Upsert {
    monitor_id: i64,
    repo_id: i32,
    commit_oids: Vec<String>,
    error: String,
}

#[derive(Serialize)]
struct SearchOutput {
    error: Option<String>,
    non_retryable: Option<bool>,
    results: serde_json::Value,
    resolve_calls: Vec<ResolveCall>,
    search_calls: Vec<SearchCall>,
    logs: Vec<String>,
    upserts: Vec<Upsert>,
}

#[derive(Serialize)]
struct AddHookOutput {
    error: Option<String>,
    names: Vec<String>,
    commit_jobs: usize,
    hooks: usize,
}

#[derive(Serialize)]
struct HookOutput {
    error: Option<String>,
    resolve_calls: Vec<ResolveCall>,
    logs: Vec<String>,
    search_calls: Vec<SearchCall>,
    upserts: Vec<Upsert>,
}

#[derive(Serialize)]
struct SnapshotOutput {
    error: Option<String>,
    resolve_calls: Vec<ResolveCall>,
    snapshot: BTreeMap<String, Vec<String>>,
}

/// In-memory stand-in for the gitserver gRPC client. Search errors are
/// reported exactly like the Go gRPC client does (`rpc error: code = ...`),
/// because the Go oracle goes through a real gRPC connection.
struct FakeGitserver {
    resolutions: Mutex<BTreeMap<String, Resolution>>,
    searches: BTreeMap<String, SearchSpec>,
    resolve_calls: Mutex<Vec<ResolveCall>>,
    search_calls: Mutex<Vec<SearchCall>>,
}

impl FakeGitserver {
    fn new(case: &Case) -> Self {
        let resolutions = case
            .resolutions
            .iter()
            .map(|resolution| {
                (
                    format!("{}\u{0}{}", resolution.repo, resolution.rev),
                    resolution.clone(),
                )
            })
            .collect();
        Self {
            resolutions: Mutex::new(resolutions),
            searches: case.search.clone(),
            resolve_calls: Mutex::new(Vec::new()),
            search_calls: Mutex::new(Vec::new()),
        }
    }

    fn take_resolve_calls(&self) -> Vec<ResolveCall> {
        self.resolve_calls.lock().expect("resolve calls").clone()
    }

    fn take_search_calls(&self) -> Vec<SearchCall> {
        self.search_calls.lock().expect("search calls").clone()
    }
}

impl GitserverClient for FakeGitserver {
    fn search(
        &self,
        request: &pb::SearchRequest,
        on_matches: &mut dyn FnMut(Vec<pb::CommitMatch>),
    ) -> Result<bool, SearcherError> {
        let revisions: Vec<String> = request
            .revisions
            .iter()
            .map(|revision| revision.rev_spec.clone())
            .collect();
        self.search_calls
            .lock()
            .expect("search calls")
            .push(SearchCall {
                repo: request.repo.clone(),
                revisions,
                include_diff: request.include_diff,
            });
        let Some(spec) = self.searches.get(&request.repo) else {
            return Ok(false);
        };
        if !spec.error.is_empty() {
            return Err(SearcherError::other(format!(
                "rpc error: code = Unknown desc = {}",
                spec.error
            )));
        }
        let matches: Vec<pb::CommitMatch> = spec.commits.iter().map(proto_commit).collect();
        if !matches.is_empty() {
            on_matches(matches);
        }
        Ok(false)
    }

    fn resolve_revision(
        &self,
        repo: &str,
        rev: &str,
        ensure_revision: bool,
    ) -> Result<String, SearcherError> {
        self.resolve_calls
            .lock()
            .expect("resolve calls")
            .push(ResolveCall {
                repo: repo.to_owned(),
                rev: rev.to_owned(),
                ensure_revision,
            });
        let resolution = self
            .resolutions
            .lock()
            .expect("resolutions")
            .get(&format!("{repo}\u{0}{rev}"))
            .cloned();
        let Some(resolution) = resolution else {
            return Err(revision_not_found(repo, rev));
        };
        if !resolution.error.is_empty() {
            return if resolution.error_kind == "not_found" {
                Err(revision_not_found(repo, rev))
            } else {
                Err(SearcherError::other(resolution.error))
            };
        }
        Ok(resolution.sha)
    }
}

fn revision_not_found(repo: &str, rev: &str) -> SearcherError {
    SearcherError::new(
        SearcherErrorKind::RevisionNotFound,
        format!("revision not found: {repo}@{rev}"),
    )
}

fn timestamp(value: &str) -> Option<Timestamp> {
    if value.is_empty() {
        // Go's zero `time.Time` (`0001-01-01T00:00:00Z`).
        return Some(Timestamp {
            seconds: -62_135_596_800,
            nanos: 0,
        });
    }
    let parsed = OffsetDateTime::parse(value, &Rfc3339).expect("valid corpus date");
    Some(Timestamp {
        seconds: parsed.unix_timestamp(),
        nanos: parsed.nanosecond() as i32,
    })
}

fn proto_signature(spec: &SignatureSpec) -> pb::commit_match::Signature {
    pb::commit_match::Signature {
        name: spec.name.clone(),
        email: spec.email.clone(),
        date: timestamp(&spec.date),
    }
}

fn proto_location(triple: &[i32]) -> pb::commit_match::Location {
    pb::commit_match::Location {
        offset: *triple.first().unwrap_or(&0) as u32,
        line: *triple.get(1).unwrap_or(&0) as u32,
        column: *triple.get(2).unwrap_or(&0) as u32,
    }
}

fn proto_matched(spec: &MatchedSpec) -> pb::commit_match::MatchedString {
    pb::commit_match::MatchedString {
        content: spec.content.clone(),
        ranges: spec
            .ranges
            .iter()
            .map(|range| pb::commit_match::Range {
                start: Some(proto_location(&range.start)),
                end: Some(proto_location(&range.end)),
            })
            .collect(),
    }
}

fn proto_commit(spec: &CommitSpec) -> pb::CommitMatch {
    pb::CommitMatch {
        oid: spec.oid.clone(),
        author: Some(proto_signature(&spec.author)),
        committer: Some(proto_signature(&spec.committer)),
        parents: spec.parents.clone(),
        refs: Vec::new(),
        source_refs: Vec::new(),
        message: Some(proto_matched(&spec.message)),
        diff: Some(proto_matched(&spec.diff)),
        modified_files: spec.modified_files.clone(),
    }
}

struct FakeStore {
    last_searched: BTreeMap<i32, Vec<String>>,
    errors: StoreErrors,
    logs: Mutex<Vec<String>>,
    upserts: Mutex<Vec<Upsert>>,
}

impl FakeStore {
    fn new(case: &Case) -> Self {
        let last_searched = case
            .last_searched
            .iter()
            .map(|spec| (spec.repo_id, spec.commits.clone()))
            .collect();
        Self {
            last_searched,
            errors: StoreErrors {
                get: case.store_errors.get.clone(),
                upsert: case.store_errors.upsert.clone(),
                update: case.store_errors.update.clone(),
            },
            logs: Mutex::new(Vec::new()),
            upserts: Mutex::new(Vec::new()),
        }
    }

    fn logs(&self) -> Vec<String> {
        self.logs.lock().expect("logs").clone()
    }

    fn upserts(&self) -> Vec<Upsert> {
        self.upserts.lock().expect("upserts").clone()
    }
}

impl TriggerSearchStore for FakeStore {
    fn get_last_searched(
        &self,
        _monitor_id: i64,
        repo_id: i32,
    ) -> Result<Option<Vec<String>>, JobError> {
        if !self.errors.get.is_empty() {
            return Err(JobError(self.errors.get.clone()));
        }
        Ok(self.last_searched.get(&repo_id).cloned())
    }

    fn upsert_last_searched(
        &self,
        monitor_id: i64,
        repo_id: i32,
        commit_oids: &[String],
    ) -> Result<(), JobError> {
        self.upserts.lock().expect("upserts").push(Upsert {
            monitor_id,
            repo_id,
            commit_oids: commit_oids.to_vec(),
            error: self.errors.upsert.clone(),
        });
        if self.errors.upsert.is_empty() {
            Ok(())
        } else {
            Err(JobError(self.errors.upsert.clone()))
        }
    }

    fn update_trigger_job_with_logs(
        &self,
        _trigger_id: i32,
        message: &str,
    ) -> Result<(), JobError> {
        self.logs.lock().expect("logs").push(message.to_owned());
        if self.errors.update.is_empty() {
            Ok(())
        } else {
            Err(JobError(self.errors.update.clone()))
        }
    }
}

/// One page of `RepoRevisions`; the baseline's resolver yields `Revs == [""]`
/// for a query without repo revision filters.
struct FakePages {
    repos: Vec<RepoSpec>,
}

impl RepoPageSource for FakePages {
    fn pages(&self) -> Vec<RepoPage> {
        let repo_revs = self
            .repos
            .iter()
            .map(|repo| ZoektRepositoryRevisions {
                repo: RepositoryRevisionsRepo {
                    id: repo.id as u32,
                    name: repo.name.clone(),
                },
                revs: vec![String::new()],
            })
            .collect();
        vec![RepoPage {
            repo_revs,
            backends_missing: 0,
        }]
    }

    fn error(&self) -> Option<JobError> {
        None
    }
}

/// Unused for commit-only trees (`skipPartitioning`); present because
/// [`TreeRuntimeClients`] requires a zoekt lister.
struct FakeZoektLister;

impl ZoektRepoLister for FakeZoektLister {
    fn list(&self) -> Result<ListResponse, JobError> {
        Ok(ListResponse::default())
    }
}

struct FakePrivateRepoLister {
    repos: Vec<RepoSpec>,
}

impl PrivateRepoLister for FakePrivateRepoLister {
    fn list_minimal_repos(
        &self,
        _options: &ReposListOptions,
    ) -> Result<Vec<MinimalRepo>, JobError> {
        Ok(self
            .repos
            .iter()
            .map(|repo| MinimalRepo {
                id: repo.id as u32,
                name: repo.name.clone(),
            })
            .collect())
    }
}

struct FakeJobStreamer;

impl JobStreamer for FakeJobStreamer {
    fn stream_search(
        &self,
        _query: &Query,
        _options: &SearchOptions,
        _sender: &mut dyn FnMut(SearchResult),
    ) -> Result<(), JobError> {
        Err(JobError(
            "zoekt is not configured for this test".to_owned(),
        ))
    }
}

struct FakeUserDB;

impl CommitUserDB for FakeUserDB {
    fn get_user_id_by_username(&self, _username: &str) -> Result<Option<i32>, JobError> {
        Ok(None)
    }

    fn list_emails_by_user(&self, _user_id: i32) -> Result<Vec<CommitUserEmail>, JobError> {
        Ok(Vec::new())
    }
}

fn runtime(case: &Case, gitserver: Arc<FakeGitserver>) -> TreeRuntimeClients {
    TreeRuntimeClients {
        db: Arc::new(FakePrivateRepoLister {
            repos: case.repos.clone(),
        }),
        zoekt: Arc::new(FakeJobStreamer),
        max_repos: None,
        searcher: None,
        commit: Some(Arc::new(CommitRuntimeClientsOwned {
            db: Arc::new(FakeUserDB),
            gitserver,
            leaf: None,
            user_external_account_key_present: false,
        })),
        repo_pager: Some(Arc::new(RepoPagerRuntimeClients::new(
            Arc::new(FakePages {
                repos: case.repos.clone(),
            }),
            Arc::new(FakeZoektLister),
        ))),
    }
}

fn plan_tree(query: &str) -> Result<JobNode, JobError> {
    let client = SearchClient::new(default_plan_deps());
    let planned = client
        .plan(
            "V3",
            None,
            query,
            SearchMode::Precise,
            Protocol::Streaming,
            Some(0),
        )
        .map_err(|error| JobError(error.to_string()))?;
    let inputs = inputs_from_plan(planned);
    let tree_inputs = tree_inputs(&inputs);
    new_plan_job(&tree_inputs, &inputs.plan)
}

/// Pre-order `Job.Name()` over a constructed tree (`m9fNames`).
fn tree_names(node: &JobNode) -> Vec<String> {
    let mut names = vec![node.name().to_owned()];
    for child in node.children() {
        names.extend(tree_names(child));
    }
    names
}

fn empty_results() -> serde_json::Value {
    serde_json::Value::Array(Vec::new())
}

fn run_search(case: &Case) -> serde_json::Value {
    let gitserver = Arc::new(FakeGitserver::new(case));
    let store = Arc::new(FakeStore::new(case));
    let store_trait: Arc<dyn TriggerSearchStore + Send + Sync> = store.clone();
    let runtime = runtime(case, gitserver.clone());

    let (error, non_retryable, results) = match search(
        &runtime,
        store_trait,
        &case.query,
        case.monitor_id,
        case.trigger_id,
        sg_service_frontend::search_contexts::RequestContext::anonymous(),
    ) {
        Ok(matches) => (None, None, commit_matches_json(&matches)),
        Err(failure) => (
            Some(failure.message),
            Some(failure.non_retryable),
            empty_results(),
        ),
    };
    serde_json::to_value(SearchOutput {
        error,
        non_retryable,
        results,
        resolve_calls: gitserver.take_resolve_calls(),
        search_calls: gitserver.take_search_calls(),
        logs: store.logs(),
        upserts: store.upserts(),
    })
    .expect("search output")
}

fn noop_hook(
    _ctx: &JobContext,
    _db: &dyn CommitUserDB,
    _gitserver: &dyn GitserverClient,
    _request: &pb::SearchRequest,
    _repo_id: u32,
    _do_search: &mut DoSearchFunc<'_>,
) -> Result<(), JobError> {
    Ok(())
}

fn run_add_hook(case: &Case) -> serde_json::Value {
    let hook: Arc<CodeMonitorHook> = Arc::new(noop_hook);
    let (error, names, hooks) = match plan_tree(&case.query) {
        Err(error) => (Some(error.0), Vec::new(), 0),
        Ok(tree) => match add_code_monitor_hook(&tree, CodeMonitorHookHandle::new(hook)) {
            Err(error) => (Some(error.0), Vec::new(), 0),
            Ok(tree) => {
                let names = tree_names(&tree);
                let hooks = tree.code_monitor_hook_count();
                (None, names, hooks)
            }
        },
    };
    let commit_jobs = names
        .iter()
        .filter(|name| name.as_str() == "CommitSearchJob" || name.as_str() == "DiffSearchJob")
        .count();
    serde_json::to_value(AddHookOutput {
        error,
        names,
        commit_jobs,
        hooks,
    })
    .expect("add hook output")
}

fn run_hook(case: &Case) -> serde_json::Value {
    let gitserver = FakeGitserver::new(case);
    let store = FakeStore::new(case);

    let request = pb::SearchRequest {
        repo: case.request.repo.clone(),
        revisions: case
            .request
            .revisions
            .iter()
            .map(|rev| pb::RevisionSpecifier {
                rev_spec: rev.clone(),
            })
            .collect(),
        include_diff: case.request.include_diff,
        ..Default::default()
    };
    let mut search_calls: Vec<SearchCall> = Vec::new();
    let mut do_search = |args: &pb::SearchRequest| -> Result<(), JobError> {
        search_calls.push(SearchCall {
            repo: args.repo.clone(),
            revisions: args
                .revisions
                .iter()
                .map(|revision| revision.rev_spec.clone())
                .collect(),
            include_diff: args.include_diff,
        });
        if case.do_search_error.is_empty() {
            Ok(())
        } else {
            Err(JobError(case.do_search_error.clone()))
        }
    };
    let ctx = JobContext { deadline: None };
    let result = hook_with_id(
        &ctx,
        &store,
        &gitserver,
        case.monitor_id,
        case.trigger_id,
        case.request.repo_id as u32,
        &request,
        &mut do_search,
    );
    serde_json::to_value(HookOutput {
        error: result.err().map(|error| error.0),
        resolve_calls: gitserver.take_resolve_calls(),
        logs: store.logs(),
        search_calls,
        upserts: store.upserts(),
    })
    .expect("hook output")
}

fn run_snapshot(case: &Case) -> serde_json::Value {
    let gitserver = Arc::new(FakeGitserver::new(case));
    let runtime = runtime(case, gitserver.clone());
    let (error, snapshot) = match snapshot(
        &runtime,
        &case.query,
        case.in_transaction,
        sg_service_frontend::search_contexts::RequestContext::anonymous(),
    ) {
        Ok(resolved) => (
            None,
            resolved
                .into_iter()
                .map(|(repo_id, hashes)| (repo_id.to_string(), hashes))
                .collect(),
        ),
        Err(error) => (Some(error.0), BTreeMap::new()),
    };
    serde_json::to_value(SnapshotOutput {
        error,
        resolve_calls: gitserver.take_resolve_calls(),
        snapshot,
    })
    .expect("snapshot output")
}

#[test]
fn codemonitors_search_diff() {
    if std::env::var("SG_M9F_DIFF").as_deref() != Ok("1") {
        return;
    }
    let corpus_path = std::env::var("M9F_CORPUS").expect("M9F_CORPUS");
    let out_path = std::env::var("M9F_OUT").expect("M9F_OUT");
    let raw = std::fs::read_to_string(&corpus_path).expect("read corpus");
    let corpus: Corpus = serde_json::from_str(&raw).expect("parse corpus");

    let mut outputs = Vec::with_capacity(corpus.cases.len());
    for case in &corpus.cases {
        let result = match case.kind.as_str() {
            "search" => run_search(case),
            "add_hook" => run_add_hook(case),
            "hook" => run_hook(case),
            "snapshot" => run_snapshot(case),
            other => panic!("unknown kind {other}"),
        };
        outputs.push(CaseOutput {
            kind: case.kind.clone(),
            name: case.name.clone(),
            result,
        });
    }
    let mut encoded = serde_json::to_string_pretty(&outputs).expect("encode output");
    encoded.push('\n');
    std::fs::write(&out_path, encoded).expect("write output");
}
