//! M9.9a differential harness: replays the corpus against
//! `exhaustive_search_searcher` with the **production** [`PlanSearchClient`]
//! and injected fake runtime seams, writing the same normalized JSON as the Go
//! oracle (`internal/search/exhaustive/service/zz_m99a_oracle_test.go`).
//!
//! Gated by `SG_M99A_DIFF=1`; reads `M99A_CORPUS` and writes `M99A_OUT`.

#![forbid(unsafe_code)]

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicI32, Ordering};
use std::sync::{Arc, Mutex};

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sg_db::exhaustive_search as ex;
use sg_service_worker::jobs::exhaustive_search_job::{MatchWriter, NewSearcher};
use sg_service_worker::jobs::exhaustive_search_searcher::{
    Exhaustive, FromSearchClient, Inputs, JobClients, JobRunner, PlanSearchClient, Protocol,
    RepoOptions, RepoResolver, RepoRevSpecs, RepoRevSpecsIterator, ResolveOutcome, Resolved,
    RevisionSpecifier, RuntimeError, SearchClient, SearchEvent, SearchMode, SearchOutcome,
    UserContext,
};
use sg_zoekt::indexed_repo_revs::{MinimalRepo, RepositoryRevisions, RepositoryRevisionsRepo};

#[derive(Debug, Clone, Deserialize)]
struct RepoSpec {
    id: i32,
    name: String,
    #[serde(default)]
    branches: BTreeMap<String, String>,
}

#[derive(Debug, Deserialize)]
struct Corpus {
    repos: Vec<RepoSpec>,
    cases: Vec<CaseSpec>,
}

#[derive(Debug, Deserialize)]
struct CaseSpec {
    name: String,
    kind: String,
    query: String,
    #[serde(default)]
    user_id: i32,
    #[serde(default)]
    actor_uid: i32,
}

#[derive(Debug, Serialize)]
struct Out {
    name: String,
    ok: bool,
    error: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    plan_query: Option<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    refspecs: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    repo_revs: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    results: Vec<BTreeMap<String, Value>>,
}

impl Out {
    fn new(name: &str) -> Out {
        Out {
            name: name.to_owned(),
            ok: false,
            error: String::new(),
            plan_query: None,
            refspecs: Vec::new(),
            repo_revs: Vec::new(),
            results: Vec::new(),
        }
    }
}

struct FakeUserContext {
    uid: AtomicI32,
}

impl UserContext for FakeUserContext {
    fn actor_uid(&self) -> Option<i32> {
        Some(self.uid.load(Ordering::SeqCst))
    }
}

struct FakeResolver {
    repos: Arc<Vec<RepoSpec>>,
}

impl FakeResolver {
    fn repo(&self, id: i32) -> Option<&RepoSpec> {
        self.repos.iter().find(|repo| repo.id == id)
    }
}

fn filter_matches(repo: &RepoSpec, filter: &str) -> bool {
    filter == "." || repo.name.contains(filter)
}

impl RepoResolver for FakeResolver {
    fn iterate_repo_revs(&self, options: &RepoOptions) -> RepoRevSpecsIterator {
        let matched: Vec<&RepoSpec> = self
            .repos
            .iter()
            .filter(|repo| {
                options
                    .repo_filters
                    .iter()
                    .all(|filter| filter_matches(repo, &filter.repo))
            })
            .collect();
        if matched.is_empty() {
            return RepoRevSpecsIterator {
                pages: Vec::new(),
                error: Some(RuntimeError::repos_missing("no repos")),
            };
        }
        let mut page = Vec::new();
        for repo in matched {
            let mut revs = Vec::new();
            let mut any_filter = false;
            for filter in &options.repo_filters {
                if !filter_matches(repo, &filter.repo) {
                    continue;
                }
                any_filter = true;
                if filter.revs.is_empty() {
                    revs.push(RevisionSpecifier::RevSpec("HEAD".to_owned()));
                } else {
                    revs.extend(filter.revs.iter().cloned());
                }
            }
            if !any_filter {
                revs.push(RevisionSpecifier::RevSpec("HEAD".to_owned()));
            }
            page.push(RepoRevSpecs {
                repo: MinimalRepo {
                    id: repo.id as u32,
                    name: repo.name.clone(),
                },
                revs,
            });
        }
        RepoRevSpecsIterator {
            pages: vec![page],
            error: None,
        }
    }

    fn resolve_revision_specs(
        &self,
        _options: &RepoOptions,
        specs: &[RepoRevSpecs],
    ) -> ResolveOutcome {
        let mut resolved = Vec::new();
        let mut missing = false;
        for spec in specs {
            let Some(repo) = self.repo(spec.repo.id as i32) else {
                missing = true;
                continue;
            };
            let mut include_globs = Vec::new();
            let mut exclude_globs = Vec::new();
            let mut revs = Vec::new();
            for rev in &spec.revs {
                match rev {
                    RevisionSpecifier::RevSpec(value) if value.is_empty() || value == "HEAD" => {
                        revs.push("HEAD".to_owned());
                    }
                    RevisionSpecifier::RevSpec(value) => {
                        if repo.branches.contains_key(value)
                            || repo.branches.values().any(|commit| commit == value)
                        {
                            revs.push(value.clone());
                        } else {
                            missing = true;
                        }
                    }
                    RevisionSpecifier::RefGlob(glob) => include_globs.push(glob.clone()),
                    RevisionSpecifier::ExcludeRefGlob(glob) => exclude_globs.push(glob.clone()),
                    RevisionSpecifier::RevAtTime(_) => missing = true,
                }
            }
            if !include_globs.is_empty() {
                let mut names: Vec<String> = repo
                    .branches
                    .keys()
                    .map(|branch| format!("refs/heads/{branch}"))
                    .collect();
                names.sort();
                for name in names {
                    if include_globs.iter().any(|glob| glob_match(glob, &name))
                        && !exclude_globs.iter().any(|glob| glob_match(glob, &name))
                    {
                        revs.push(name.trim_start_matches("refs/heads/").to_owned());
                    }
                }
            }
            if revs.is_empty() {
                missing = true;
            } else {
                resolved.push(RepositoryRevisions {
                    repo: RepositoryRevisionsRepo {
                        id: repo.id as u32,
                        name: repo.name.clone(),
                    },
                    revs,
                });
            }
        }
        ResolveOutcome {
            resolved: Resolved {
                repo_revs: resolved,
                backends_missing: 0,
            },
            error: if missing {
                Some(RuntimeError::repos_missing("missing repo revs"))
            } else {
                None
            },
        }
    }

    fn list_minimal_repos(&self, repo_id: i32) -> Result<Vec<MinimalRepo>, String> {
        Ok(self
            .repo(repo_id)
            .map(|repo| {
                vec![MinimalRepo {
                    id: repo.id as u32,
                    name: repo.name.clone(),
                }]
            })
            .unwrap_or_default())
    }
}

fn glob_match(pattern: &str, value: &str) -> bool {
    fn helper(pattern: &[u8], value: &[u8]) -> bool {
        match pattern.split_first() {
            None => value.is_empty(),
            Some((b'*', rest)) => {
                helper(rest, value) || (!value.is_empty() && helper(pattern, &value[1..]))
            }
            Some((b'?', rest)) => !value.is_empty() && helper(rest, &value[1..]),
            Some((byte, rest)) => value.first() == Some(byte) && helper(rest, &value[1..]),
        }
    }
    helper(pattern.as_bytes(), value.as_bytes())
}

struct FakeRunner {
    repos: Arc<Vec<RepoSpec>>,
}

impl JobRunner for FakeRunner {
    fn run(&self, _exhaustive: &Exhaustive, repo_revs: &RepositoryRevisions) -> SearchOutcome {
        let repo = self
            .repos
            .iter()
            .find(|repo| repo.id == repo_revs.repo.id as i32);
        let Some(repo) = repo else {
            return SearchOutcome {
                events: Vec::new(),
                error: Some(RuntimeError::revision_not_found("repo not found")),
            };
        };
        let mut events = Vec::new();
        for rev in &repo_revs.revs {
            match repo.branches.get(rev) {
                Some(commit) => events.push(SearchEvent {
                    results: vec![json!({
                        "type": "content",
                        "repositoryID": repo.id,
                        "commit": commit,
                    })],
                }),
                None => {
                    return SearchOutcome {
                        events,
                        error: Some(RuntimeError::revision_not_found("revision not found")),
                    }
                }
            }
        }
        SearchOutcome {
            events,
            error: None,
        }
    }
}

/// Wraps the production plan client so the harness can record the query the
/// facade handed to `Plan`.
struct RecordingPlanClient {
    inner: PlanSearchClient,
    recorded: Mutex<Option<String>>,
}

impl SearchClient for RecordingPlanClient {
    fn plan(
        &self,
        version: &str,
        pattern_type: Option<&str>,
        query: &str,
        search_mode: SearchMode,
        protocol: Protocol,
        context_lines: Option<i32>,
    ) -> Result<Inputs, String> {
        *self.recorded.lock().unwrap() = Some(query.to_owned());
        self.inner.plan(
            version,
            pattern_type,
            query,
            search_mode,
            protocol,
            context_lines,
        )
    }

    fn job_clients(&self) -> JobClients {
        self.inner.job_clients()
    }
}

struct CollectingWriter {
    values: Mutex<Vec<Value>>,
    fail: bool,
}

impl MatchWriter for CollectingWriter {
    fn write(&self, r#match: Value) -> Result<(), String> {
        if self.fail {
            return Err("writer boom".to_owned());
        }
        self.values.lock().unwrap().push(r#match);
        Ok(())
    }

    fn flush(&self) -> Result<(), String> {
        Ok(())
    }
}

fn normalize_results(writer: &CollectingWriter) -> Vec<BTreeMap<String, Value>> {
    writer
        .values
        .lock()
        .unwrap()
        .iter()
        .map(|value| {
            let mut map = BTreeMap::new();
            for key in ["type", "repositoryID", "commit"] {
                map.insert(
                    key.to_owned(),
                    value.get(key).cloned().unwrap_or(Value::Null),
                );
            }
            map
        })
        .collect()
}

fn refspec_strings(specs: &[ex::RepositoryRevSpecs]) -> Vec<String> {
    specs
        .iter()
        .map(|spec| {
            format!(
                "RepositoryRevSpec{{{}@{}}}",
                spec.repository, spec.revision_specifiers
            )
        })
        .collect()
}

fn repo_rev_strings(repo_revs: &[ex::RepositoryRevision]) -> Vec<String> {
    repo_revs
        .iter()
        .map(|repo_rev| {
            format!(
                "RepositoryRevision{{{}@{}}}",
                repo_rev.repository_rev_specs.repository, repo_rev.revision
            )
        })
        .collect()
}

#[tokio::test]
async fn exhaustive_searcher_diff() {
    if std::env::var("SG_M99A_DIFF").as_deref() != Ok("1") {
        return;
    }
    let corpus_path = std::env::var("M99A_CORPUS").expect("M99A_CORPUS");
    let out_path = std::env::var("M99A_OUT").expect("M99A_OUT");

    let raw = std::fs::read_to_string(&corpus_path).expect("read corpus");
    let corpus: Corpus = serde_json::from_str(&raw).expect("parse corpus");
    let repos = Arc::new(corpus.repos);

    let mut outputs = Vec::new();
    for case in &corpus.cases {
        outputs.push(run_case(case, Arc::clone(&repos)).await);
    }

    let mut encoded = serde_json::to_string_pretty(&outputs).expect("encode");
    encoded.push('\n');
    std::fs::write(&out_path, encoded).expect("write output");
}

async fn run_case(case: &CaseSpec, repos: Arc<Vec<RepoSpec>>) -> Out {
    let mut out = Out::new(&case.name);

    let user_context = Arc::new(FakeUserContext {
        uid: AtomicI32::new(case.actor_uid),
    });

    let clients = JobClients::new(
        Arc::new(FakeResolver {
            repos: Arc::clone(&repos),
        }),
        Arc::new(FakeRunner {
            repos: Arc::clone(&repos),
        }),
    );
    let client = Arc::new(RecordingPlanClient {
        inner: PlanSearchClient::new(clients),
        recorded: Mutex::new(None),
    });
    let factory = FromSearchClient::new(client.clone(), user_context.clone());

    let query = factory.new_search(case.user_id, &case.query).await;
    if let Some(recorded) = client.recorded.lock().unwrap().clone() {
        if !recorded.is_empty() {
            out.plan_query = Some(recorded);
        }
    }
    let query = match query {
        Ok(query) => query,
        Err(error) => {
            out.error = error;
            return out;
        }
    };

    match case.kind.as_str() {
        "new_search" => {
            out.ok = true;
        }
        "full" => {
            let refspecs = match query.repository_rev_specs().await {
                Ok(refspecs) => refspecs,
                Err(error) => {
                    out.error = error;
                    return out;
                }
            };
            out.refspecs = refspec_strings(&refspecs);

            let mut repo_revs = Vec::new();
            for refspec in &refspecs {
                match query.resolve_repository_rev_spec(refspec).await {
                    Ok(part) => repo_revs.extend(part),
                    Err(error) => {
                        out.error = error;
                        return out;
                    }
                }
            }
            out.repo_revs = repo_rev_strings(&repo_revs);

            let writer = CollectingWriter {
                values: Mutex::new(Vec::new()),
                fail: false,
            };
            for repo_rev in &repo_revs {
                if let Err(error) = query.search(repo_rev, &writer).await {
                    out.error = error;
                    return out;
                }
            }
            out.results = normalize_results(&writer);
            out.ok = true;
        }
        "resolve_unknown_repo" => {
            let spec = ex::RepositoryRevSpecs {
                repository: 99,
                revision_specifiers: "HEAD".to_owned(),
            };
            match query.resolve_repository_rev_spec(&spec).await {
                Ok(_) => out.ok = true,
                Err(error) => out.error = error,
            }
        }
        "revspecs_user_mismatch" => {
            user_context.uid.store(2, Ordering::SeqCst);
            match query.repository_rev_specs().await {
                Ok(refspecs) => {
                    out.refspecs = refspec_strings(&refspecs);
                    out.ok = true;
                }
                Err(error) => out.error = error,
            }
        }
        "resolve_user_mismatch" => {
            let refspecs = match query.repository_rev_specs().await {
                Ok(refspecs) => refspecs,
                Err(error) => {
                    out.error = error;
                    return out;
                }
            };
            out.refspecs = refspec_strings(&refspecs);
            user_context.uid.store(2, Ordering::SeqCst);
            match query.resolve_repository_rev_spec(&refspecs[0]).await {
                Ok(_) => out.ok = true,
                Err(error) => out.error = error,
            }
        }
        "search_user_mismatch" => {
            let refspecs = match query.repository_rev_specs().await {
                Ok(refspecs) => refspecs,
                Err(error) => {
                    out.error = error;
                    return out;
                }
            };
            out.refspecs = refspec_strings(&refspecs);
            let mut repo_revs = Vec::new();
            for refspec in &refspecs {
                match query.resolve_repository_rev_spec(refspec).await {
                    Ok(part) => repo_revs.extend(part),
                    Err(error) => {
                        out.error = error;
                        return out;
                    }
                }
            }
            out.repo_revs = repo_rev_strings(&repo_revs);
            user_context.uid.store(2, Ordering::SeqCst);
            let writer = CollectingWriter {
                values: Mutex::new(Vec::new()),
                fail: true,
            };
            match query.search(&repo_revs[0], &writer).await {
                Ok(()) => out.ok = true,
                Err(error) => out.error = error,
            }
        }
        "writer_error" => {
            let refspecs = match query.repository_rev_specs().await {
                Ok(refspecs) => refspecs,
                Err(error) => {
                    out.error = error;
                    return out;
                }
            };
            out.refspecs = refspec_strings(&refspecs);
            let mut repo_revs = Vec::new();
            for refspec in &refspecs {
                match query.resolve_repository_rev_spec(refspec).await {
                    Ok(part) => repo_revs.extend(part),
                    Err(error) => {
                        out.error = error;
                        return out;
                    }
                }
            }
            out.repo_revs = repo_rev_strings(&repo_revs);
            let writer = CollectingWriter {
                values: Mutex::new(Vec::new()),
                fail: true,
            };
            match query.search(&repo_revs[0], &writer).await {
                Ok(()) => out.ok = true,
                Err(error) => out.error = error,
            }
        }
        other => panic!("unknown case kind {other}"),
    }

    out
}
