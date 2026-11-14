//! M9.10a differential harness: `search_repos::Resolver`.
//!
//! Mirrors `internal/search/repos/zz_m910a_oracle_test.go` (UNTRACKED, gated by
//! `SG_M10A_DIFF=1`). Both sides consume
//! `test_ws/p9-go/data/search_repos/corpus.json` and print one JSON document;
//! the driver normalizes both with sorted keys before comparing.
//!
//! The corpus supplies the DB pages, gitserver resolve/refs/commits maps, zoekt
//! list responses and searcher match keys; the recorded `RepoListOptions` cover
//! the query-construction part of `doQueryDB` without a real database.
//!
//! Env: `SG_M10A_DIFF=1`, `M10A_CORPUS`, `M10A_OUT`.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};

use serde::Deserialize;
use serde_json::{json, Value};

use sg_db::repos::{RepoChangelistIDs, RepoListItem, RepoListOptions};
use sg_search_query::basic::RepoHasFileContentArgs;
use sg_search_query::validate::YesNoOnly;
use sg_service_frontend::search_contexts::{
    ParsedRepoFilter, RepoVisibility, RequestContext, RevAtTime, RevisionSpecifier, SearchContext,
    SearchContextRepositoryRevisions,
};
use sg_service_frontend::search_job::JobError;
use sg_service_frontend::search_jobutil_job_builders::{
    RepoHasCommitAfterArgs, RepoHasTopic, RepoKvpFilter,
};
use sg_service_frontend::search_repos::{
    maybe_missing_repo_revs_error, GitserverError, RepoResolverDb, RepoRevSpecs, ResolveOptions,
    Resolver, ResolverGitserver, ResolverSearcher, ResolverZoektLister,
};
use sg_service_frontend::search_searcher_client::protocol;
use sg_zoekt::indexed_repo_revs::RepositoryRevisions;
use sg_zoekt::query::Query;
use sg_zoekt::types::{
    ListOptions, ListResponse, MinimalRepoListEntry, RepositoryBranch,
};
use time::OffsetDateTime;

// ---------------------------------------------------------------------------
// Corpus
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
struct Corpus {
    cases: Vec<Case>,
}

#[derive(Default, Deserialize)]
struct Case {
    name: String,
    #[serde(default)]
    options: OptionsIn,
    #[serde(default)]
    db_pages: Vec<Vec<DbRepoIn>>,
    #[serde(default)]
    gitserver: GitserverIn,
    #[serde(default)]
    zoekt: Vec<ZoektIn>,
    #[serde(default)]
    searcher: BTreeMap<String, bool>,
    #[serde(default)]
    changelists: BTreeMap<String, BTreeMap<String, String>>,
    #[serde(default)]
    minimal_repos_by_ids: MinimalReposByIDsIn,
}

#[derive(Default, Deserialize)]
struct MinimalReposByIDsIn {
    #[serde(default)]
    ids: Vec<i32>,
    #[serde(default)]
    repos: Vec<DbRepoIn>,
}

#[derive(Default, Deserialize)]
struct OptionsIn {
    repo_filters: Option<Vec<FilterIn>>,
    minus_repo_filters: Option<Vec<String>>,
    description_patterns: Option<Vec<String>>,
    case_sensitive_repo_filters: Option<bool>,
    search_context_spec: Option<String>,
    commit_after: Option<CommitAfterIn>,
    visibility: Option<String>,
    limit: Option<i64>,
    use_index: Option<String>,
    has_file_content: Option<Vec<FileContentIn>>,
    has_kvps: Option<Vec<KvpIn>>,
    has_topics: Option<Vec<TopicIn>>,
    no_forks: Option<bool>,
    only_forks: Option<bool>,
    only_cloned: Option<bool>,
    no_archived: Option<bool>,
    only_archived: Option<bool>,
    perforce_changelist_mapping: Option<bool>,
}

#[derive(Default, Deserialize)]
struct FilterIn {
    repo: String,
    #[serde(default)]
    revs: Vec<RevIn>,
}

#[derive(Default, Deserialize)]
struct RevIn {
    rev_spec: Option<String>,
    ref_glob: Option<String>,
    exclude_ref_glob: Option<String>,
    rev_at_time: Option<RevAtTimeIn>,
}

/// `query.RevAtTime` payload (`{"RevSpec","Timestamp"}`, RFC3339 timestamp).
#[derive(Default, Deserialize)]
struct RevAtTimeIn {
    rev_spec: Option<String>,
    timestamp: Option<String>,
}

#[derive(Deserialize)]
struct CommitAfterIn {
    time_ref: String,
    #[serde(default)]
    negated: bool,
}

#[derive(Deserialize)]
struct FileContentIn {
    #[serde(default)]
    path: String,
    #[serde(default)]
    content: String,
    #[serde(default)]
    negated: bool,
}

#[derive(Deserialize)]
struct KvpIn {
    key: String,
    value: Option<String>,
    #[serde(default)]
    negated: bool,
    #[serde(default)]
    key_only: bool,
}

#[derive(Deserialize)]
struct TopicIn {
    topic: String,
    #[serde(default)]
    negated: bool,
}

#[derive(Default, Deserialize)]
struct DbRepoIn {
    id: i32,
    name: String,
    #[serde(default)]
    stars: i64,
    #[serde(default)]
    external_service_type: String,
}

#[derive(Default, Deserialize)]
struct GitserverIn {
    #[serde(default)]
    resolve: BTreeMap<String, String>,
    #[serde(default)]
    refs: BTreeMap<String, Vec<String>>,
    #[serde(default)]
    commits_after: BTreeMap<String, usize>,
    /// `repo@rev` → resolved commit. An empty value means "not found"; a
    /// missing key is a revision-not-found error.
    #[serde(default)]
    rev_at_time: BTreeMap<String, String>,
}

#[derive(Default, Deserialize)]
struct ZoektIn {
    #[serde(default)]
    repos_map: BTreeMap<String, ZoektEntryIn>,
    #[serde(default)]
    crashes: i64,
}

#[derive(Default, Deserialize)]
struct ZoektEntryIn {
    #[serde(default)]
    branches: Vec<ZoektBranchIn>,
}

#[derive(Default, Deserialize)]
struct ZoektBranchIn {
    name: String,
    #[serde(default)]
    version: String,
}

fn rev(rev: &RevIn) -> RevisionSpecifier {
    RevisionSpecifier {
        rev_spec: rev.rev_spec.clone().unwrap_or_default(),
        ref_glob: rev.ref_glob.clone().unwrap_or_default(),
        exclude_ref_glob: rev.exclude_ref_glob.clone().unwrap_or_default(),
        rev_at_time: rev.rev_at_time.is_some(),
        rev_at_time_payload: rev.rev_at_time.as_ref().map(|rev_at_time| {
            let timestamp = rev_at_time
                .timestamp
                .as_deref()
                .unwrap_or("1970-01-01T00:00:00Z");
            let timestamp = time::OffsetDateTime::parse(
                timestamp,
                &time::format_description::well_known::Rfc3339,
            )
            .expect("RFC3339 timestamp");
            std::sync::Arc::new(RevAtTime::new(
                rev_at_time.rev_spec.clone().unwrap_or_default(),
                timestamp,
            ))
        }),
    }
}

fn resolve_options(options: &OptionsIn) -> ResolveOptions {
    ResolveOptions {
        repo_filters: options
            .repo_filters
            .as_deref()
            .unwrap_or_default()
            .iter()
            .map(|filter| ParsedRepoFilter {
                repo: filter.repo.clone(),
                revs: filter.revs.iter().map(rev).collect(),
            })
            .collect(),
        minus_repo_filters: options.minus_repo_filters.clone().unwrap_or_default(),
        description_patterns: options.description_patterns.clone().unwrap_or_default(),
        case_sensitive_repo_filters: options.case_sensitive_repo_filters.unwrap_or(false),
        search_context_spec: options.search_context_spec.clone().unwrap_or_default(),
        commit_after: options.commit_after.as_ref().map(|commit_after| {
            RepoHasCommitAfterArgs {
                time_ref: commit_after.time_ref.clone(),
                negated: commit_after.negated,
            }
        }),
        visibility: match options.visibility.as_deref() {
            Some("private") => RepoVisibility::Private,
            Some("public") => RepoVisibility::Public,
            _ => RepoVisibility::Any,
        },
        limit: options.limit.unwrap_or(0),
        cursors: Vec::new(),
        use_index: match options.use_index.as_deref() {
            Some("no") => YesNoOnly::No,
            Some("only") => YesNoOnly::Only,
            _ => YesNoOnly::Yes,
        },
        has_file_content: options
            .has_file_content
            .as_deref()
            .unwrap_or_default()
            .iter()
            .map(|arg| RepoHasFileContentArgs {
                path: arg.path.clone(),
                content: arg.content.clone(),
                negated: arg.negated,
            })
            .collect(),
        has_kvps: options
            .has_kvps
            .as_deref()
            .unwrap_or_default()
            .iter()
            .map(|kvp| RepoKvpFilter {
                key: kvp.key.clone(),
                value: kvp.value.clone(),
                negated: kvp.negated,
                key_only: kvp.key_only,
            })
            .collect(),
        has_topics: options
            .has_topics
            .as_deref()
            .unwrap_or_default()
            .iter()
            .map(|topic| RepoHasTopic {
                topic: topic.topic.clone(),
                negated: topic.negated,
            })
            .collect(),
        no_forks: options.no_forks.unwrap_or(false),
        only_forks: options.only_forks.unwrap_or(false),
        only_cloned: options.only_cloned.unwrap_or(false),
        no_archived: options.no_archived.unwrap_or(false),
        only_archived: options.only_archived.unwrap_or(false),
        perforce_changelist_mapping: options.perforce_changelist_mapping.unwrap_or(false),
        request_context: RequestContext::anonymous(),
    }
}

// ---------------------------------------------------------------------------
// Fakes
// ---------------------------------------------------------------------------

struct FakeDb {
    pages: Vec<Vec<RepoListItem>>,
    index: Mutex<usize>,
    options: Mutex<Vec<Value>>,
    changelists: BTreeMap<u32, BTreeMap<i64, String>>,
    changelist_calls: Mutex<Vec<Value>>,
    by_ids_repos: Vec<RepoListItem>,
    by_ids_calls: Mutex<Vec<Vec<i32>>>,
}

impl RepoResolverDb for FakeDb {
    fn resolve_search_context(
        &self,
        _spec: &str,
        _ctx: &RequestContext,
    ) -> Result<SearchContext, JobError> {
        Ok(SearchContext {
            name: "global".to_owned(),
            public: true,
            auto_defined: true,
            ..SearchContext::default()
        })
    }

    fn list_minimal_repos(
        &self,
        options: &RepoListOptions,
    ) -> Result<Vec<RepoListItem>, JobError> {
        self.options.lock().unwrap().push(options_json(options));
        let mut index = self.index.lock().unwrap();
        let page = self
            .pages
            .get(*index)
            .cloned()
            .unwrap_or_default();
        *index += 1;
        Ok(page)
    }

    fn get_search_context_repository_revisions(
        &self,
        _id: i64,
    ) -> Result<Vec<SearchContextRepositoryRevisions>, JobError> {
        Ok(Vec::new())
    }

    fn batch_get_repo_commit_changelist(
        &self,
        repos: &[RepoChangelistIDs],
    ) -> Result<BTreeMap<u32, BTreeMap<i64, String>>, JobError> {
        for repo in repos {
            self.changelist_calls.lock().unwrap().push(json!({
                "repo_id": repo.repo_id,
                "ids": repo.changelist_ids,
            }));
        }
        Ok(self.changelists.clone())
    }

    fn list_minimal_repos_by_ids(&self, ids: &[i32]) -> Result<Vec<RepoListItem>, JobError> {
        self.by_ids_calls.lock().unwrap().push(ids.to_vec());
        let wanted: BTreeSet<i32> = ids.iter().copied().collect();
        Ok(self
            .by_ids_repos
            .iter()
            .filter(|repo| wanted.contains(&repo.id))
            .cloned()
            .collect())
    }
}

struct FakeGitserver {
    resolve: BTreeMap<String, String>,
    refs: BTreeMap<String, Vec<String>>,
    commits_after: BTreeMap<String, usize>,
    rev_at_time: BTreeMap<String, String>,
    calls: Mutex<Vec<String>>,
}

impl FakeGitserver {
    fn record(&self, call: String) {
        self.calls.lock().unwrap().push(call);
    }
}

impl ResolverGitserver for FakeGitserver {
    fn resolve_revision(&self, repo: &str, rev: &str) -> Result<String, GitserverError> {
        let key = format!("{repo}@{rev}");
        self.record(format!("resolve:{key}"));
        self.resolve
            .get(&key)
            .cloned()
            .ok_or(GitserverError::Missing)
    }

    fn list_refs(&self, repo: &str) -> Result<Vec<String>, JobError> {
        self.record(format!("refs:{repo}"));
        Ok(self.refs.get(repo).cloned().unwrap_or_default())
    }

    fn commits_after(
        &self,
        repo: &str,
        rev: &str,
        _after: OffsetDateTime,
    ) -> Result<usize, GitserverError> {
        let key = format!("{repo}@{rev}");
        self.record(format!("commits:{key}"));
        self.commits_after
            .get(&key)
            .copied()
            .ok_or(GitserverError::Missing)
    }

    fn rev_at_time(
        &self,
        repo: &str,
        rev: &str,
        _timestamp: OffsetDateTime,
    ) -> Result<Option<String>, GitserverError> {
        let key = format!("{repo}@{rev}");
        self.record(format!("rev_at_time:{key}"));
        match self.rev_at_time.get(&key) {
            Some(commit) if commit.is_empty() => Ok(None),
            Some(commit) => Ok(Some(commit.clone())),
            None => Err(GitserverError::Missing),
        }
    }
}

struct FakeZoekt {
    responses: Vec<ListResponse>,
    index: Mutex<usize>,
    calls: Mutex<usize>,
}

impl ResolverZoektLister for FakeZoekt {
    fn list(&self, _query: &Query, _opts: Option<ListOptions>) -> Result<ListResponse, JobError> {
        *self.calls.lock().unwrap() += 1;
        let mut index = self.index.lock().unwrap();
        let response = self
            .responses
            .get(*index)
            .cloned()
            .unwrap_or(ListResponse {
                repos: Vec::new(),
                repos_map: BTreeMap::new(),
                crashes: 0,
                stats: Default::default(),
            });
        *index += 1;
        Ok(response)
    }
}

struct FakeSearcher {
    matches: BTreeMap<String, bool>,
    calls: Mutex<Vec<Value>>,
}

impl ResolverSearcher for FakeSearcher {
    fn search(&self, request: &protocol::Request) -> Result<bool, JobError> {
        let (value, negated, is_regexp) = match request.pattern_info.query.as_ref() {
            Some(protocol::QueryNode::Pattern(pattern)) => {
                (pattern.value.clone(), pattern.is_negated, pattern.is_regexp)
            }
            _ => (String::new(), false, false),
        };
        let key = format!("{}@{}||{}", request.repo, request.commit, value);
        self.calls.lock().unwrap().push(json!({
            "repo": request.repo,
            "commit": request.commit,
            "path": request.pattern_info.include_paths,
            "content": value,
            "negated": negated,
            "regexp": is_regexp,
            "limit": request.pattern_info.limit,
            "matches_content": request.pattern_info.pattern_matches_content,
        }));
        Ok(self.matches.get(&key).copied().unwrap_or(false))
    }
}

// ---------------------------------------------------------------------------
// JSON helpers
// ---------------------------------------------------------------------------

fn cursor_json(cursor: &sg_db::repos::RepoListCursor) -> Value {
    json!({
        "column": cursor.column,
        "value": cursor.value,
        "direction": cursor.direction,
    })
}

fn cursors_json(cursors: &[sg_db::repos::RepoListCursor]) -> Value {
    Value::Array(cursors.iter().map(cursor_json).collect())
}

fn options_json(options: &RepoListOptions) -> Value {
    json!({
        "include_patterns": options.include_patterns,
        "exclude_pattern": options.exclude_pattern,
        "description_patterns": options.description_patterns,
        "case_sensitive_patterns": options.case_sensitive_patterns,
        "kvp_filters": options.kvp_filters.iter().map(|filter| json!({
            "key": filter.key,
            "value": filter.value,
            "negated": filter.negated,
            "key_only": filter.key_only,
        })).collect::<Vec<_>>(),
        "topic_filters": options.topic_filters.iter().map(|filter| json!({
            "topic": filter.topic,
            "negated": filter.negated,
        })).collect::<Vec<_>>(),
        "cursors": cursors_json(&options.cursors),
        "limit": options.limit,
        "no_forks": options.no_forks,
        "only_forks": options.only_forks,
        "no_archived": options.no_archived,
        "only_archived": options.only_archived,
        "no_private": options.no_private,
        "only_private": options.only_private,
        "only_cloned": options.only_cloned,
        "search_context_id": options.search_context_id,
        "order_by": options.order_by.iter().map(|sort| json!({
            "field": sort.field,
            "descending": sort.descending,
            "nulls": sort.nulls.clone().unwrap_or_default(),
        })).collect::<Vec<_>>(),
    })
}

fn repo_revs_json(repo_revs: &[RepositoryRevisions]) -> Value {
    Value::Array(
        repo_revs
            .iter()
            .map(|repo_rev| {
                json!({
                    "id": repo_rev.repo.id,
                    "name": repo_rev.repo.name,
                    "revs": repo_rev.revs,
                })
            })
            .collect(),
    )
}

fn spec_json(spec: &RevisionSpecifier) -> Value {
    json!({
        "rev_spec": spec.rev_spec,
        "ref_glob": spec.ref_glob,
        "exclude_ref_glob": spec.exclude_ref_glob,
        "rev_at_time": spec.rev_at_time || spec.rev_at_time_payload.is_some(),
    })
}

fn repo_rev_specs_json(specs: &[RepoRevSpecs]) -> Value {
    Value::Array(
        specs
            .iter()
            .map(|spec| {
                json!({
                    "id": spec.repo.id,
                    "name": spec.repo.name,
                    "revs": spec.revs.iter().map(spec_json).collect::<Vec<_>>(),
                })
            })
            .collect(),
    )
}

fn error_json(error: &Option<JobError>) -> Value {
    match error {
        Some(error) => Value::String(error.0.clone()),
        None => Value::Null,
    }
}

fn calls_json(
    db: &Arc<FakeDb>,
    gitserver: &Arc<FakeGitserver>,
    zoekt: &Arc<FakeZoekt>,
    searcher: &Arc<FakeSearcher>,
) -> Value {
    json!({
        "db_options": db.options.lock().unwrap().clone(),
        "gitserver": gitserver.calls.lock().unwrap().clone(),
        "zoekt": *zoekt.calls.lock().unwrap(),
        "searcher": searcher.calls.lock().unwrap().clone(),
        "changelists": db.changelist_calls.lock().unwrap().clone(),
    })
}

// ---------------------------------------------------------------------------
// Phase construction
// ---------------------------------------------------------------------------

fn build_fakes(case: &Case) -> (Arc<FakeDb>, Arc<FakeGitserver>, Arc<FakeZoekt>, Arc<FakeSearcher>) {
    let pages = case
        .db_pages
        .iter()
        .map(|page| {
            page.iter()
                .map(|repo| RepoListItem {
                    id: repo.id,
                    name: repo.name.clone(),
                    stars: repo.stars,
                    external_id: String::new(),
                    external_service_type: repo.external_service_type.clone(),
                    external_service_id: String::new(),
                })
                .collect()
        })
        .collect();
    let mut changelists: BTreeMap<u32, BTreeMap<i64, String>> = BTreeMap::new();
    for (repo_id, entries) in &case.changelists {
        let Ok(repo_id) = repo_id.parse::<u32>() else {
            continue;
        };
        let mut sub_map = BTreeMap::new();
        for (changelist, commit) in entries {
            if let Ok(changelist) = changelist.parse::<i64>() {
                sub_map.insert(changelist, commit.clone());
            }
        }
        changelists.insert(repo_id, sub_map);
    }
    let responses = case
        .zoekt
        .iter()
        .map(|response| {
            let mut repos_map = BTreeMap::new();
            for (id, entry) in &response.repos_map {
                let Ok(id) = id.parse::<u32>() else {
                    continue;
                };
                repos_map.insert(
                    id,
                    MinimalRepoListEntry {
                        has_symbols: false,
                        branches: entry
                            .branches
                            .iter()
                            .map(|branch| RepositoryBranch {
                                name: branch.name.clone(),
                                version: branch.version.clone(),
                            })
                            .collect(),
                        index_time_unix: 0,
                    },
                );
            }
            ListResponse {
                repos: Vec::new(),
                repos_map,
                crashes: response.crashes,
                stats: Default::default(),
            }
        })
        .collect();
    let by_ids_repos = case
        .minimal_repos_by_ids
        .repos
        .iter()
        .map(|repo| RepoListItem {
            id: repo.id,
            name: repo.name.clone(),
            stars: repo.stars,
            external_id: String::new(),
            external_service_type: repo.external_service_type.clone(),
            external_service_id: String::new(),
        })
        .collect();
    (
        Arc::new(FakeDb {
            pages,
            index: Mutex::new(0),
            options: Mutex::new(Vec::new()),
            changelists,
            changelist_calls: Mutex::new(Vec::new()),
            by_ids_repos,
            by_ids_calls: Mutex::new(Vec::new()),
        }),
        Arc::new(FakeGitserver {
            resolve: case.gitserver.resolve.clone(),
            refs: case.gitserver.refs.clone(),
            commits_after: case.gitserver.commits_after.clone(),
            rev_at_time: case.gitserver.rev_at_time.clone(),
            calls: Mutex::new(Vec::new()),
        }),
        Arc::new(FakeZoekt {
            responses,
            index: Mutex::new(0),
            calls: Mutex::new(0),
        }),
        Arc::new(FakeSearcher {
            matches: case.searcher.clone(),
            calls: Mutex::new(Vec::new()),
        }),
    )
}

fn build_resolver(
    case: &Case,
) -> (
    Arc<Resolver>,
    Arc<FakeDb>,
    Arc<FakeGitserver>,
    Arc<FakeZoekt>,
    Arc<FakeSearcher>,
) {
    let (db, gitserver, zoekt, searcher) = build_fakes(case);
    let resolver = Arc::new(Resolver {
        db: db.clone(),
        gitserver: gitserver.clone(),
        zoekt: zoekt.clone(),
        searcher: Some(searcher.clone()),
        max_repos: None,
    });
    (resolver, db, gitserver, zoekt, searcher)
}

fn phase_resolve(case: &Case) -> Value {
    let (resolver, db, gitserver, zoekt, searcher) = build_resolver(case);
    let options = resolve_options(&case.options);
    let page = resolver.resolve_page(&options);
    let result = json!({
        "repo_revs": repo_revs_json(&page.resolved.repo_revs),
        "backends_missing": page.resolved.backends_missing,
        "next": match &page.next {
            Some(next) => cursors_json(next),
            None => Value::Null,
        },
        "error": error_json(
            &page
                .error
                .clone()
                .or_else(|| maybe_missing_repo_revs_error(&page.missing)),
        ),
    });
    json!({
        "result": result,
        "calls": calls_json(&db, &gitserver, &zoekt, &searcher),
    })
}

fn phase_iterate(case: &Case) -> Value {
    let (resolver, db, gitserver, zoekt, searcher) = build_resolver(case);
    let options = resolve_options(&case.options);
    let outcome = resolver.iterate(&options);
    let result = json!({
        "pages": outcome.pages.iter().map(|page| json!({
            "repo_revs": repo_revs_json(&page.repo_revs),
            "backends_missing": page.backends_missing,
        })).collect::<Vec<_>>(),
        "error": error_json(&outcome.error),
    });
    json!({
        "result": result,
        "calls": calls_json(&db, &gitserver, &zoekt, &searcher),
    })
}

fn phase_iterate_repo_revs(case: &Case) -> Value {
    let (resolver, db, gitserver, zoekt, searcher) = build_resolver(case);
    let options = resolve_options(&case.options);
    let (specs, error) = resolver.iterate_repo_revs(&options);
    let result = json!({
        "repo_revs": repo_rev_specs_json(&specs),
        "error": error_json(&error),
    });
    json!({
        "result": result,
        "calls": calls_json(&db, &gitserver, &zoekt, &searcher),
    })
}

fn phase_resolve_rev_specs(case: &Case) -> Value {
    let (resolver, db, gitserver, zoekt, searcher) = build_resolver(case);
    let options = resolve_options(&case.options);
    let result = match resolver.do_query_db(&options) {
        Ok((db_resolved, _)) => {
            let (resolved, error) =
                resolver.resolve_rev_specs(&options, db_resolved.associated);
            json!({
                "repo_revs": repo_revs_json(&resolved.repo_revs),
                "backends_missing": resolved.backends_missing,
                "error": error_json(&error),
            })
        }
        Err(error) => json!({
            "repo_revs": [],
            "backends_missing": 0,
            "error": error_json(&Some(error)),
        }),
    };
    json!({
        "result": result,
        "calls": calls_json(&db, &gitserver, &zoekt, &searcher),
    })
}

fn minimal_repos_by_ids_json(repos: &[RepoListItem]) -> Value {
    Value::Array(
        repos
            .iter()
            .map(|repo| {
                json!({
                    "id": repo.id,
                    "name": repo.name,
                    "stars": repo.stars,
                    "external_service_type": repo.external_service_type,
                })
            })
            .collect(),
    )
}

/// `Repos.ListMinimalRepos{IDs}` — the query the exhaustive searcher issues per
/// repo and `Resolver::minimal_repos_by_ids` exposes.
fn phase_minimal_repos_by_ids(case: &Case) -> Value {
    let (resolver, db, _gitserver, _zoekt, _searcher) = build_resolver(case);
    let repos = resolver.minimal_repos_by_ids(&case.minimal_repos_by_ids.ids);
    let calls = db.by_ids_calls.lock().unwrap().clone();
    json!({
        "result": {
            "repos": match &repos {
                Ok(repos) => minimal_repos_by_ids_json(repos),
                Err(_) => Value::Array(Vec::new()),
            },
            "error": match &repos {
                Ok(_) => Value::Null,
                Err(error) => Value::String(error.0.clone()),
            },
        },
        "calls": {
            "db_options": calls.iter().map(|ids| json!({"ids": ids})).collect::<Vec<_>>(),
        },
    })
}

#[test]
fn search_repos_diff() {
    if std::env::var("SG_M10A_DIFF").as_deref() != Ok("1") {
        eprintln!("SG_M10A_DIFF not set; skipping");
        return;
    }
    let corpus_path = std::env::var("M10A_CORPUS").expect("M10A_CORPUS");
    let out_path = std::env::var("M10A_OUT").expect("M10A_OUT");
    let raw = std::fs::read_to_string(&corpus_path).expect("read corpus");
    let corpus: Corpus = serde_json::from_str(&raw).expect("parse corpus");
    let cases = corpus
        .cases
        .iter()
        .map(|case| {
            json!({
                "name": case.name,
                "resolve": phase_resolve(case),
                "iterate": phase_iterate(case),
                "iterate_repo_revs": phase_iterate_repo_revs(case),
                "resolve_rev_specs": phase_resolve_rev_specs(case),
                "minimal_repos_by_ids": phase_minimal_repos_by_ids(case),
            })
        })
        .collect::<Vec<_>>();
    let document = json!({ "cases": cases });
    std::fs::write(&out_path, serde_json::to_string(&document).expect("serialize"))
        .expect("write output");
}
