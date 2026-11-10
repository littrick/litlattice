//! M9.8hl differential harness: the jobutil repository pager and repository
//! search (`internal/search/job/jobutil/{repo_pager_job,repos}.go`) against
//! [`sg_service_frontend::search_jobutil_repos`].
//!
//! Run with `SG_M9HL_DIFF=1 M9HL_CORPUS=<json> M9HL_OUT=<json>` (the wrapper
//! script in `test_ws/p9-go` does this). Both sides read the same corpus and
//! emit byte-identical normalized JSON.

use std::collections::BTreeMap;

use regex::Regex;
use serde::Deserialize;
use serde_json::{json, Value};

use sg_service_frontend::search_job::{JobContext, JobError, SearchJob, Verbosity};
use sg_service_frontend::search_jobutil_repos::{
    set_repos, RepoDescriptionLister, RepoJob, RepoOptions, RepoPage, RepoPageSource, RepoPagerJob,
    RepoSearchJob, ReposPartialJob, ResolvedRepos, ZoektRepoLister,
};
use sg_service_frontend::search_stream::convert::{Match, RepoMatch};
use sg_service_frontend::search_stream::events::{Location, Range};
use sg_service_frontend::search_stream::progress::{SearchEvent, Stats};
use sg_zoekt::indexed_repo_revs::{IndexedRepoRevs, RepositoryRevisions, RepositoryRevisionsRepo};
use sg_zoekt::types::ListResponse;

// ---------------------------------------------------------------------------
// Corpus schema
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize, Default, Clone)]
struct RepoRevSpec {
    #[serde(default)]
    id: i32,
    #[serde(default)]
    name: String,
    #[serde(default)]
    revs: Vec<String>,
    /// Only used by passthrough event repo matches.
    #[serde(default)]
    rev: String,
}

#[derive(Debug, Deserialize, Default, Clone)]
struct IndexedSpec {
    #[serde(default)]
    repo_revs: Vec<RepoRevSpec>,
}

#[derive(Debug, Deserialize, Default, Clone)]
struct EventSpec {
    #[serde(default)]
    repo_matches: Vec<RepoRevSpec>,
}

#[derive(Debug, Deserialize, Default, Clone)]
struct JobSpec {
    #[serde(rename = "type", default)]
    typ: String,
    #[serde(default)]
    diff: bool,
    #[serde(default)]
    label: String,
    #[serde(default)]
    children: Vec<JobSpec>,
    #[serde(default)]
    events: Vec<EventSpec>,
}

#[derive(Debug, Deserialize, Default, Clone)]
struct RepoOptsSpec {
    #[serde(default)]
    minus_repo_filters: Vec<String>,
    #[serde(default)]
    description_patterns: Vec<String>,
    #[serde(default)]
    case_sensitive_repo_filters: bool,
    #[serde(default)]
    search_context_spec: String,
    #[serde(default)]
    visibility: String,
    #[serde(default)]
    limit: i64,
    #[serde(default)]
    use_index: String,
    #[serde(default)]
    fork_set: bool,
    #[serde(default)]
    no_forks: bool,
    #[serde(default)]
    only_forks: bool,
    #[serde(default)]
    only_cloned: bool,
    #[serde(default)]
    archived_set: bool,
    #[serde(default)]
    no_archived: bool,
    #[serde(default)]
    only_archived: bool,
}

#[derive(Debug, Deserialize, Default, Clone)]
struct PageSpec {
    #[serde(default)]
    repo_revs: Vec<RepoRevSpec>,
    #[serde(default)]
    backends_missing: i64,
}

#[derive(Debug, Deserialize, Default)]
struct Case {
    #[serde(default)]
    name: String,
    #[serde(default)]
    kind: String,
    #[serde(default)]
    job: Option<JobSpec>,
    #[serde(default)]
    indexed: Option<IndexedSpec>,
    #[serde(default)]
    unindexed: Vec<RepoRevSpec>,
    #[serde(default)]
    verbosity: String,
    #[serde(default)]
    repo_opts: RepoOptsSpec,
    #[serde(default)]
    contains_ref_globs: bool,
    #[serde(default)]
    skip_partitioning: bool,
    #[serde(default)]
    pages: Vec<PageSpec>,
    #[serde(default)]
    description_patterns: Vec<String>,
    #[serde(default)]
    repo_name_patterns: Vec<String>,
    #[serde(default)]
    descriptions: BTreeMap<String, String>,
    #[serde(default)]
    description_error: String,
    #[serde(default)]
    iterator_error: String,
}

// ---------------------------------------------------------------------------
// Fakes
// ---------------------------------------------------------------------------

struct EmptyPages;

impl RepoPageSource for EmptyPages {
    fn pages(&self) -> Vec<RepoPage> {
        Vec::new()
    }
    fn error(&self) -> Option<JobError> {
        None
    }
}

struct EmptyZoekt;

impl ZoektRepoLister for EmptyZoekt {
    fn list(&self) -> Result<ListResponse, JobError> {
        Ok(ListResponse::default())
    }
}

struct VecPages {
    pages: Vec<RepoPage>,
    error: Option<JobError>,
}

impl RepoPageSource for VecPages {
    fn pages(&self) -> Vec<RepoPage> {
        self.pages.clone()
    }
    fn error(&self) -> Option<JobError> {
        self.error.clone()
    }
}

struct MapDescriptions {
    map: BTreeMap<u32, String>,
    error: Option<JobError>,
}

impl RepoDescriptionLister for MapDescriptions {
    fn get_repo_descriptions_by_ids(
        &self,
        _ids: &[u32],
    ) -> Result<BTreeMap<u32, String>, JobError> {
        match &self.error {
            Some(error) => Err(error.clone()),
            None => Ok(self.map.clone()),
        }
    }
}

// ---------------------------------------------------------------------------
// Builders
// ---------------------------------------------------------------------------

fn to_repo_revisions(spec: &RepoRevSpec) -> RepositoryRevisions {
    RepositoryRevisions {
        repo: RepositoryRevisionsRepo {
            id: spec.id as u32,
            name: spec.name.clone(),
        },
        revs: spec.revs.clone(),
    }
}

fn indexed_from(spec: &Option<IndexedSpec>) -> Option<IndexedRepoRevs> {
    spec.as_ref().map(|indexed| {
        let mut repo_revs = BTreeMap::new();
        for repo_rev in &indexed.repo_revs {
            repo_revs.insert(repo_rev.id as u32, to_repo_revisions(repo_rev));
        }
        IndexedRepoRevs {
            repo_revs,
            branch_repos: BTreeMap::new(),
        }
    })
}

fn build_events(specs: &[EventSpec]) -> Vec<SearchEvent> {
    specs
        .iter()
        .map(|event| SearchEvent {
            results: event
                .repo_matches
                .iter()
                .map(|repo_match| {
                    Match::Repo(RepoMatch {
                        name: repo_match.name.clone(),
                        id: repo_match.id,
                        rev: repo_match.rev.clone(),
                        description_matches: Vec::new(),
                        repo_name_matches: Vec::new(),
                    })
                })
                .collect(),
            stats: Stats::default(),
        })
        .collect()
}

fn build_job(spec: &JobSpec) -> RepoJob {
    match spec.typ.as_str() {
        "zoekt_text" => RepoJob::ZoektRepoSubsetText { repos: None },
        "searcher_text" => RepoJob::SearcherText { repos: Vec::new() },
        "zoekt_symbol" => RepoJob::ZoektSymbol { repos: None },
        "searcher_symbol" => RepoJob::SearcherSymbol { repos: Vec::new() },
        "commit" => RepoJob::CommitSearch {
            diff: spec.diff,
            repos: Vec::new(),
        },
        "structural" => RepoJob::StructuralSearch {
            indexed: None,
            unindexed: Vec::new(),
        },
        "container" => RepoJob::Container {
            label: spec.label.clone(),
            children: spec.children.iter().map(build_job).collect(),
        },
        "passthrough" => RepoJob::Passthrough {
            label: spec.label.clone(),
            events: build_events(&spec.events),
        },
        other => RepoJob::Container {
            label: format!("unknown:{other}"),
            children: Vec::new(),
        },
    }
}

fn repo_options(spec: &RepoOptsSpec) -> RepoOptions {
    RepoOptions {
        minus_repo_filters: spec.minus_repo_filters.clone(),
        description_patterns: spec.description_patterns.clone(),
        case_sensitive_repo_filters: spec.case_sensitive_repo_filters,
        search_context_spec: spec.search_context_spec.clone(),
        visibility: spec.visibility.clone(),
        limit: spec.limit,
        use_index: spec.use_index.clone(),
        fork_set: spec.fork_set,
        no_forks: spec.no_forks,
        only_forks: spec.only_forks,
        only_cloned: spec.only_cloned,
        archived_set: spec.archived_set,
        no_archived: spec.no_archived,
        only_archived: spec.only_archived,
    }
}

fn compile(patterns: &[String]) -> Vec<Regex> {
    patterns
        .iter()
        .map(|pattern| Regex::new(pattern).expect("valid corpus regexp"))
        .collect()
}

fn verbosity(name: &str) -> Verbosity {
    match name {
        "basic" => Verbosity::Basic,
        "max" => Verbosity::Max,
        _ => Verbosity::None,
    }
}

// ---------------------------------------------------------------------------
// Normalization
// ---------------------------------------------------------------------------

fn repo_rev_out(repo_rev: &RepositoryRevisions) -> Value {
    json!({
        "id": repo_rev.repo.id as i32,
        "name": repo_rev.repo.name,
        "revs": repo_rev.revs,
    })
}

fn indexed_out(indexed: &Option<IndexedRepoRevs>) -> Value {
    match indexed {
        None => Value::Null,
        Some(indexed) => json!({
            "repo_revs": indexed.repo_revs.values().map(repo_rev_out).collect::<Vec<_>>(),
        }),
    }
}

fn unindexed_out(repo_revs: &[RepositoryRevisions]) -> Value {
    Value::Array(repo_revs.iter().map(repo_rev_out).collect())
}

fn location_out(location: &Location) -> Value {
    json!({
        "offset": location.offset,
        "line": location.line,
        "column": location.column,
    })
}

fn ranges_out(ranges: &[Range]) -> Value {
    Value::Array(
        ranges
            .iter()
            .map(|range| {
                json!({
                    "start": location_out(&range.start),
                    "end": location_out(&range.end),
                })
            })
            .collect(),
    )
}

fn match_out(match_: &Match) -> Value {
    match match_ {
        Match::Repo(repo) => json!({
            "kind": "repo",
            "name": repo.name,
            "id": repo.id,
            "rev": repo.rev,
            "description_matches": ranges_out(&repo.description_matches),
            "repo_name_matches": ranges_out(&repo.repo_name_matches),
        }),
        _ => json!({ "kind": "unknown" }),
    }
}

fn events_out(events: &[SearchEvent]) -> Value {
    Value::Array(
        events
            .iter()
            .map(|event| {
                json!({
                    "results": event.results.iter().map(match_out).collect::<Vec<_>>(),
                    "stats": {
                        "backends_missing": event.stats.backends_missing,
                        "is_limit_hit": event.stats.is_limit_hit,
                    },
                })
            })
            .collect(),
    )
}

fn node_out(job: &RepoJob) -> Value {
    match job {
        RepoJob::ZoektRepoSubsetText { repos } => json!({
            "kind": "zoekt_repo_subset_text",
            "name": job.name(),
            "indexed": indexed_out(repos),
        }),
        RepoJob::SearcherText { repos } => json!({
            "kind": "searcher_text",
            "name": job.name(),
            "unindexed": unindexed_out(repos),
        }),
        RepoJob::ZoektSymbol { repos } => json!({
            "kind": "zoekt_symbol",
            "name": job.name(),
            "indexed": indexed_out(repos),
        }),
        RepoJob::SearcherSymbol { repos } => json!({
            "kind": "searcher_symbol",
            "name": job.name(),
            "unindexed": unindexed_out(repos),
        }),
        RepoJob::CommitSearch { repos, .. } => json!({
            "kind": "commit_search",
            "name": job.name(),
            "unindexed": unindexed_out(repos),
        }),
        RepoJob::StructuralSearch { indexed, unindexed } => json!({
            "kind": "structural_search",
            "name": job.name(),
            "indexed": indexed_out(indexed),
            "unindexed": unindexed_out(unindexed),
        }),
        RepoJob::Container { label, children } => json!({
            "kind": "container",
            "label": label,
            "name": job.name(),
            "children": children.iter().map(node_out).collect::<Vec<_>>(),
        }),
        RepoJob::Passthrough { label, events } => json!({
            "kind": "passthrough",
            "label": label,
            "name": job.name(),
            "events": events_out(events),
        }),
    }
}

fn attributes_out(attributes: Vec<(String, String)>) -> Value {
    Value::Array(
        attributes
            .into_iter()
            .map(|(key, value)| json!([key, value]))
            .collect(),
    )
}

// ---------------------------------------------------------------------------
// Case runners
// ---------------------------------------------------------------------------

fn run_case(case: &Case) -> Value {
    match case.kind.as_str() {
        "set_repos" => {
            let tree = build_job(case.job.as_ref().expect("set_repos requires a job"));
            let indexed = indexed_from(&case.indexed);
            let unindexed = case
                .unindexed
                .iter()
                .map(to_repo_revisions)
                .collect::<Vec<_>>();
            let resolved = set_repos(&tree, indexed.as_ref(), &unindexed);
            json!({ "tree": node_out(&resolved) })
        }
        "resolve" => {
            let tree = build_job(case.job.as_ref().expect("resolve requires a job"));
            let partial = ReposPartialJob::new(tree);
            let resolved = partial.resolve(&ResolvedRepos::new(
                indexed_from(&case.indexed),
                case.unindexed
                    .iter()
                    .map(to_repo_revisions)
                    .collect::<Vec<_>>(),
            ));
            json!({ "tree": node_out(&resolved) })
        }
        "describe" => {
            let tree = build_job(case.job.as_ref().expect("describe requires a job"));
            let pages = EmptyPages;
            let zoekt = EmptyZoekt;
            let pager = RepoPagerJob {
                repo_opts: repo_options(&case.repo_opts),
                contains_ref_globs: case.contains_ref_globs,
                child: ReposPartialJob::new(tree),
                skip_partitioning: case.skip_partitioning,
                pages: &pages,
                zoekt: &zoekt,
            };
            let verbosity = verbosity(&case.verbosity);
            json!({
                "pager": {
                    "name": pager.name(),
                    "attributes": attributes_out(pager.attributes(verbosity)),
                    "children": vec![pager.child.name()],
                },
                "partial": {
                    "name": pager.child.name(),
                    "attributes": attributes_out(pager.child.attributes(verbosity)),
                    "children": pager.child.children_names(),
                },
            })
        }
        "repo_search" => run_repo_search(case),
        "run_resolved" => {
            let tree = build_job(case.job.as_ref().expect("run_resolved requires a job"));
            let resolved = ReposPartialJob::new(tree).resolve(&ResolvedRepos::new(
                indexed_from(&case.indexed),
                case.unindexed
                    .iter()
                    .map(to_repo_revisions)
                    .collect::<Vec<_>>(),
            ));
            let mut job = resolved;
            let mut events = Vec::new();
            let error = job
                .run(&JobContext::default(), &mut |event| events.push(event))
                .err()
                .map(|error| error.0);
            json!({ "events": events_out(&events), "error": error })
        }
        other => panic!("unknown kind {other}"),
    }
}

fn run_repo_search(case: &Case) -> Value {
    let pages = VecPages {
        pages: case
            .pages
            .iter()
            .map(|page| RepoPage {
                repo_revs: page
                    .repo_revs
                    .iter()
                    .map(to_repo_revisions)
                    .collect::<Vec<_>>(),
                backends_missing: page.backends_missing,
            })
            .collect(),
        error: if case.iterator_error.is_empty() {
            None
        } else {
            Some(JobError(case.iterator_error.clone()))
        },
    };
    let mut descriptions = BTreeMap::new();
    for (id, description) in &case.descriptions {
        if let Ok(id) = id.parse::<u32>() {
            descriptions.insert(id, description.clone());
        }
    }
    let description_store = MapDescriptions {
        map: descriptions,
        error: if case.description_error.is_empty() {
            None
        } else {
            Some(JobError(case.description_error.clone()))
        },
    };

    let mut job = RepoSearchJob {
        repo_opts: repo_options(&case.repo_opts),
        description_patterns: compile(&case.description_patterns),
        repo_name_patterns: compile(&case.repo_name_patterns),
        pages: &pages,
        descriptions: &description_store,
    };
    let mut events = Vec::new();
    let error = job
        .run_events(&mut |event| events.push(event))
        .err()
        .map(|error| error.0);
    json!({ "events": events_out(&events), "error": error })
}

#[test]
fn search_jobutil_repos_diff() {
    if std::env::var("SG_M9HL_DIFF").as_deref() != Ok("1") {
        return;
    }
    let corpus_path = std::env::var("M9HL_CORPUS").expect("M9HL_CORPUS");
    let out_path = std::env::var("M9HL_OUT").expect("M9HL_OUT");
    let raw = std::fs::read_to_string(&corpus_path).expect("read corpus");
    let cases: Vec<Case> = serde_json::from_str(&raw).expect("parse corpus");

    let mut output = Vec::with_capacity(cases.len());
    for case in &cases {
        output.push(json!({
            "name": case.name,
            "kind": case.kind,
            "result": run_case(case),
        }));
    }

    let encoded = serde_json::to_string(&output).expect("serialize output");
    std::fs::write(&out_path, encoded).expect("write output");
}
