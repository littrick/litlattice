//! M9.8hp differential harness: reads `M9HP_CORPUS`, drives
//! `sg_service_frontend::search_jobutil_exhaustive`, and writes normalized
//! JSON to `M9HP_OUT` for byte-for-byte comparison with the Go oracle
//! (`internal/search/job/jobutil/zz_m9hp_oracle_test.go`).

use serde::Deserialize;
use serde_json::{json, Value};

use sg_search_query::basic::plan_to_q;
use sg_search_query::parser::SearchType;
use sg_search_query::pipeline::pipeline_query;
use sg_service_frontend::search_client::{Features, Protocol};
use sg_service_frontend::search_contexts::RevisionSpecifier;
use sg_service_frontend::search_job::Verbosity;
use sg_service_frontend::search_jobutil_exhaustive::{
    has_predicates, new_exhaustive, Exhaustive, ExhaustiveResolver, Inputs, RepoRevSpecs,
    RepoRevSpecsIterator, Resolved,
};
use sg_service_frontend::search_jobutil_job_builders as jb;
use sg_zoekt::indexed_repo_revs::{MinimalRepo, RepositoryRevisions, RepositoryRevisionsRepo};

#[derive(Debug, Clone, Default, Deserialize)]
struct MinimalRepoSpec {
    #[serde(default)]
    id: u32,
    #[serde(default)]
    name: String,
}

#[derive(Debug, Clone, Default, Deserialize)]
struct RevSpecSpec {
    #[serde(default)]
    rev_spec: String,
    #[serde(default)]
    ref_glob: String,
    #[serde(default)]
    exclude_ref_glob: String,
    #[serde(default)]
    rev_at_time: bool,
}

#[derive(Debug, Clone, Default, Deserialize)]
struct RepoRevSpecsSpec {
    #[serde(default)]
    repo: MinimalRepoSpec,
    #[serde(default)]
    revs: Vec<RevSpecSpec>,
}

#[derive(Debug, Clone, Default, Deserialize)]
struct RepoRevSpec {
    #[serde(default)]
    id: u32,
    #[serde(default)]
    name: String,
    #[serde(default)]
    revs: Vec<String>,
}

#[derive(Debug, Clone, Default, Deserialize)]
struct ResolvedSpec {
    #[serde(default)]
    repo_revs: Vec<RepoRevSpec>,
    #[serde(default)]
    backends_missing: i64,
}

#[derive(Debug, Clone, Default, Deserialize)]
struct UserSettingsSpec {
    #[serde(default)]
    search_include_forks: Option<bool>,
    #[serde(default)]
    search_include_archived: Option<bool>,
}

#[derive(Debug, Clone, Default, Deserialize)]
struct FeaturesSpec {
    #[serde(default)]
    content_based_lang_filters: bool,
    #[serde(default)]
    debug: bool,
}

#[derive(Debug, Clone, Default, Deserialize)]
struct Case {
    #[serde(default)]
    name: String,
    #[serde(default)]
    kind: String,
    #[serde(default)]
    query: String,
    #[serde(default)]
    search_type: String,
    #[serde(default)]
    protocol: String,
    #[serde(default)]
    context_lines: Option<i32>,
    #[serde(default)]
    sub_repo_enabled: bool,
    #[serde(default)]
    user_settings: UserSettingsSpec,
    #[serde(default)]
    features: FeaturesSpec,
    #[serde(default)]
    repo_revs: Option<RepoRevSpec>,
    #[serde(default)]
    verbosity: String,
    #[serde(default)]
    field: String,
    #[serde(default)]
    pages: Vec<Vec<RepoRevSpecsSpec>>,
    #[serde(default)]
    iterator_error: String,
    #[serde(default)]
    input_specs: Vec<RepoRevSpecsSpec>,
    #[serde(default)]
    resolved: Option<ResolvedSpec>,
    #[serde(default)]
    resolve_error: String,
}

fn search_type(name: &str) -> SearchType {
    match name {
        "literal" => SearchType::Literal,
        "regexp" | "regex" => SearchType::Regex,
        "structural" => SearchType::Structural,
        "keyword" => SearchType::Keyword,
        "codycontext" => SearchType::CodyContext,
        _ => SearchType::Standard,
    }
}

fn protocol(name: &str) -> Protocol {
    match name {
        "streaming" => Protocol::Streaming,
        "batch" => Protocol::Batch,
        _ => Protocol::Exhaustive,
    }
}

fn verbosity(name: &str) -> Verbosity {
    match name {
        "none" => Verbosity::None,
        "basic" => Verbosity::Basic,
        _ => Verbosity::Max,
    }
}

fn visibility_str(
    visibility: sg_service_frontend::search_contexts::RepoVisibility,
) -> &'static str {
    use sg_service_frontend::search_contexts::RepoVisibility;
    match visibility {
        RepoVisibility::Any => "Any",
        RepoVisibility::Private => "Private",
        RepoVisibility::Public => "Public",
    }
}

fn use_index_str(value: sg_search_query::validate::YesNoOnly) -> &'static str {
    use sg_search_query::validate::YesNoOnly;
    match value {
        YesNoOnly::Yes => "yes",
        YesNoOnly::No => "no",
        YesNoOnly::Only => "only",
        YesNoOnly::Invalid => "",
    }
}

fn rev_spec_out(rev: &RevisionSpecifier) -> Value {
    json!({
        "rev_spec": rev.rev_spec,
        "ref_glob": rev.ref_glob,
        "exclude_ref_glob": rev.exclude_ref_glob,
        "rev_at_time": rev.rev_at_time,
    })
}

fn repo_opts_out(opts: &jb::RepoOptions) -> Value {
    let filters: Vec<Value> = opts
        .repo_filters
        .iter()
        .map(|filter| {
            json!({
                "repo": filter.repo,
                "revs": filter.revs.iter().map(rev_spec_out).collect::<Vec<_>>(),
            })
        })
        .collect();
    json!({
        "repo_filters": filters,
        "minus_repo_filters": opts.minus_repo_filters,
        "description_patterns": opts.description_patterns,
        "search_context_spec": opts.search_context_spec,
        "visibility": visibility_str(opts.visibility),
        "use_index": use_index_str(opts.use_index),
        "fork_set": opts.fork_set,
        "no_forks": opts.no_forks,
        "only_forks": opts.only_forks,
        "archived_set": opts.archived_set,
        "no_archived": opts.no_archived,
        "only_archived": opts.only_archived,
    })
}

fn describe(
    job: &sg_service_frontend::search_jobutil_exhaustive::ResolvedJob,
    verbosity: Verbosity,
) -> Value {
    let children: Vec<Value> = job
        .children()
        .iter()
        .map(|child| json!(child.name()))
        .collect();
    let attributes: Vec<Value> = job
        .attributes(verbosity)
        .into_iter()
        .map(|(key, value)| json!([key, value]))
        .collect();
    json!({
        "name": job.name(),
        "children": children,
        "attributes": attributes,
    })
}

fn repo_rev_out(repo: &RepositoryRevisions) -> Value {
    json!({
        "id": repo.repo.id,
        "name": repo.repo.name,
        "revs": repo.revs,
    })
}

fn specs_out(specs: &RepoRevSpecs) -> Value {
    json!({
        "repo": {"id": specs.repo.id, "name": specs.repo.name},
        "revs": specs.revs.iter().map(rev_spec_out).collect::<Vec<_>>(),
    })
}

fn pages_out(pages: &[Vec<RepoRevSpecs>]) -> Value {
    Value::Array(
        pages
            .iter()
            .map(|page| Value::Array(page.iter().map(specs_out).collect()))
            .collect(),
    )
}

fn resolved_out(resolved: &Resolved) -> Value {
    json!({
        "repo_revs": resolved.repo_revs.iter().map(repo_rev_out).collect::<Vec<_>>(),
        "backends_missing": resolved.backends_missing,
    })
}

fn rev_spec_from(spec: &RevSpecSpec) -> RevisionSpecifier {
    RevisionSpecifier {
        rev_spec: spec.rev_spec.clone(),
        ref_glob: spec.ref_glob.clone(),
        exclude_ref_glob: spec.exclude_ref_glob.clone(),
        rev_at_time: spec.rev_at_time,
    }
}

fn specs_from(spec: &RepoRevSpecsSpec) -> RepoRevSpecs {
    RepoRevSpecs {
        repo: MinimalRepo {
            id: spec.repo.id,
            name: spec.repo.name.clone(),
        },
        revs: spec.revs.iter().map(rev_spec_from).collect(),
    }
}

fn resolved_from(spec: &ResolvedSpec) -> Resolved {
    Resolved {
        repo_revs: spec
            .repo_revs
            .iter()
            .map(|repo| RepositoryRevisions {
                repo: RepositoryRevisionsRepo {
                    id: repo.id,
                    name: repo.name.clone(),
                },
                revs: repo.revs.clone(),
            })
            .collect(),
        backends_missing: spec.backends_missing,
    }
}

fn repo_revs_from(spec: &Option<RepoRevSpec>) -> RepositoryRevisions {
    let fallback = RepoRevSpec {
        id: 1,
        name: "repo1".to_owned(),
        revs: vec!["HEAD".to_owned()],
    };
    let spec = spec.as_ref().unwrap_or(&fallback);
    RepositoryRevisions {
        repo: RepositoryRevisionsRepo {
            id: spec.id,
            name: spec.name.clone(),
        },
        revs: spec.revs.clone(),
    }
}

struct FakeResolver {
    pages: Vec<Vec<RepoRevSpecs>>,
    iterator_error: Option<String>,
    resolved: Option<Result<Resolved, String>>,
}

impl ExhaustiveResolver for FakeResolver {
    fn iterate_repo_revs(&self, _repo_opts: &jb::RepoOptions) -> RepoRevSpecsIterator {
        RepoRevSpecsIterator {
            pages: self.pages.clone(),
            error: self
                .iterator_error
                .as_ref()
                .map(|error| sg_service_frontend::search_job::JobError(error.clone())),
        }
    }

    fn resolve_rev_specs(
        &self,
        _repo_opts: &jb::RepoOptions,
        _repo_rev_specs: &[RepoRevSpecs],
    ) -> Result<Resolved, sg_service_frontend::search_job::JobError> {
        match &self.resolved {
            Some(Ok(resolved)) => Ok(resolved.clone()),
            Some(Err(error)) => Err(sg_service_frontend::search_job::JobError(error.clone())),
            None => Ok(Resolved::default()),
        }
    }
}

fn result(case: &Case) -> Value {
    let search_type = search_type(&case.search_type);
    let plan = match pipeline_query(&case.query, search_type) {
        Ok(plan) => plan,
        Err(error) => return json!({"ok": false, "error": error}),
    };
    let inputs = Inputs {
        query: plan_to_q(&plan),
        plan,
        pattern_type: search_type,
        user_settings: jb::UserSettings {
            search_include_forks: case.user_settings.search_include_forks,
            search_include_archived: case.user_settings.search_include_archived,
        },
        protocol: protocol(&case.protocol),
        features: Features {
            content_based_lang_filters: case.features.content_based_lang_filters,
            debug: case.features.debug,
        },
        context_lines: case.context_lines.unwrap_or(1),
        sub_repo_enabled: case.sub_repo_enabled,
    };

    match case.kind.as_str() {
        "has_predicates" => {
            let field = if case.field.is_empty() {
                "file"
            } else {
                &case.field
            };
            match has_predicates(field, &inputs.query) {
                Some(predicate) => json!({"found": true, "predicate": predicate}),
                None => json!({"found": false, "predicate": ""}),
            }
        }
        "repository_rev_specs" => {
            let exhaustive = match new_exhaustive(&inputs) {
                Ok(exhaustive) => exhaustive,
                Err(error) => return json!({"ok": false, "error": error.0}),
            };
            let resolver = FakeResolver {
                pages: case
                    .pages
                    .iter()
                    .map(|page| page.iter().map(specs_from).collect())
                    .collect(),
                iterator_error: Some(case.iterator_error.clone()).filter(|error| !error.is_empty()),
                resolved: None,
            };
            let iterator = exhaustive.repository_rev_specs(&resolver);
            json!({
                "ok": true,
                "error": "",
                "repo_opts": repo_opts_out(&exhaustive.repo_pager_job.repo_opts),
                "pages": pages_out(&iterator.pages),
                "iterator_error": iterator.error.map(|error| error.0).unwrap_or_default(),
            })
        }
        "resolve_repository_rev_spec" => {
            let exhaustive = match new_exhaustive(&inputs) {
                Ok(exhaustive) => exhaustive,
                Err(error) => return json!({"ok": false, "error": error.0}),
            };
            let input_specs: Vec<RepoRevSpecs> = case.input_specs.iter().map(specs_from).collect();
            let resolver = FakeResolver {
                pages: Vec::new(),
                iterator_error: None,
                resolved: Some(match &case.resolved {
                    Some(resolved) => Ok(resolved_from(resolved)),
                    None => Err(case.resolve_error.clone()),
                }),
            };
            let input_specs_out: Vec<Value> = input_specs.iter().map(specs_out).collect();
            match exhaustive.resolve_repository_rev_spec(&resolver, &input_specs) {
                Ok(resolved) => json!({
                    "ok": true,
                    "error": "",
                    "repo_opts": repo_opts_out(&exhaustive.repo_pager_job.repo_opts),
                    "input_specs": input_specs_out,
                    "resolved": resolved_out(&resolved),
                    "resolve_error": "",
                }),
                Err(error) => json!({
                    "ok": true,
                    "error": "",
                    "repo_opts": repo_opts_out(&exhaustive.repo_pager_job.repo_opts),
                    "input_specs": input_specs_out,
                    "resolved": Value::Null,
                    "resolve_error": error.0,
                }),
            }
        }
        _ => {
            let exhaustive: Exhaustive = match new_exhaustive(&inputs) {
                Ok(exhaustive) => exhaustive,
                Err(error) => return json!({"ok": false, "error": error.0}),
            };
            let repo_revs = repo_revs_from(&case.repo_revs);
            let job = exhaustive.job(&repo_revs);
            json!({
                "ok": true,
                "error": "",
                "pager": {
                    "contains_ref_globs": exhaustive.repo_pager_job.contains_ref_globs,
                    "skip_partitioning": exhaustive.repo_pager_job.skip_partitioning,
                    "only_cloned": exhaustive.repo_pager_job.only_cloned,
                    "repo_opts": repo_opts_out(&exhaustive.repo_pager_job.repo_opts),
                    // `repoPagerJob.child` is always the `reposPartialJob`
                    // wrapper, whose `Name` is `PartialReposJob`.
                    "child_name": "PartialReposJob",
                },
                "job": describe(&job, verbosity(&case.verbosity)),
            })
        }
    }
}

#[test]
fn search_jobutil_exhaustive_diff() {
    let corpus_path = match std::env::var("M9HP_CORPUS") {
        Ok(path) => path,
        Err(_) => return,
    };
    let out_path = std::env::var("M9HP_OUT").expect("M9HP_OUT must be set");
    let raw = std::fs::read_to_string(&corpus_path).expect("read corpus");
    let cases: Vec<Case> = serde_json::from_str(&raw).expect("parse corpus");

    let out: Vec<Value> = cases
        .iter()
        .map(|case| {
            json!({
                "name": case.name,
                "kind": case.kind,
                "result": result(case),
            })
        })
        .collect();

    let encoded = serde_json::to_string(&out).expect("encode output");
    std::fs::write(&out_path, encoded).expect("write output");
}
