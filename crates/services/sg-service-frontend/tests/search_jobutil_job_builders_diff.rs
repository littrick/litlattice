//! M9.8hn differential harness: drive the Rust port of the `job.go` pure
//! construction helpers over the shared corpus and emit normalized JSON for a
//! byte-for-byte comparison with the Go oracle
//! (`internal/search/job/jobutil/zz_m9hn_oracle_test.go`).
//!
//! Env:
//!
//! * `SG_M9HN_DIFF=1` enable the harness
//! * `M9HN_CORPUS` corpus JSON
//! * `M9HN_OUT` output JSON

use std::collections::BTreeMap;

use serde::Deserialize;
use serde_json::Value;

use sg_search_query::basic::Basic;
use sg_search_query::parser::SearchType;
use sg_search_query::pipeline::pipeline_query;
use sg_search_query::validate::YesNoOnly;
use sg_service_frontend::search_client::{Features, Protocol};
use sg_service_frontend::search_contexts::RepoVisibility;
use sg_service_frontend::search_jobutil_job_builders as jb;
use sg_service_searcher::protocol::QueryNode;
use sg_zoekt::languages::{case_insensitive_regexp, regexp_quote_meta};
use sg_zoekt::query::{Query, TypeKind};

// ---------------------------------------------------------------------------
// corpus schema (mirrors the Go oracle)
// ---------------------------------------------------------------------------

#[derive(Debug, Default, Deserialize)]
struct Case {
    name: String,
    kind: String,
    #[serde(default)]
    query: String,
    #[serde(default)]
    search_type: String,
    #[serde(default)]
    result_types: Vec<String>,
    #[serde(default)]
    default_types: Vec<String>,
    #[serde(default)]
    default_limit: i64,
    #[serde(default)]
    features: Option<FeaturesSpec>,
    #[serde(default)]
    user_settings: UserSettingsSpec,
    #[serde(default)]
    protocol: String,
    #[serde(default)]
    pattern_type: String,
    #[serde(default)]
    on_sourcegraph_dot_com: bool,
    #[serde(default)]
    max_timeout_seconds: u64,
    #[serde(default)]
    contributors: Vec<String>,
    #[serde(default)]
    case_sensitive: bool,
    #[serde(default)]
    values: Vec<String>,
    #[serde(default)]
    map_op: String,
    #[serde(default)]
    aliases: Vec<String>,
    #[serde(default)]
    select: String,
    #[serde(default)]
    cursors: usize,
    #[serde(default)]
    tree: Option<JobSpec>,
    #[serde(default)]
    zoekt: Option<ZoektSpec>,
}

#[derive(Debug, Default, Deserialize)]
struct FeaturesSpec {
    #[serde(default)]
    content_based_lang_filters: bool,
}

#[derive(Debug, Default, Deserialize)]
struct UserSettingsSpec {
    #[serde(default)]
    search_include_forks: Option<bool>,
    #[serde(default)]
    search_include_archived: Option<bool>,
}

#[derive(Debug, Default, Deserialize)]
struct JobSpec {
    #[serde(rename = "type")]
    kind: String,
    #[serde(default)]
    ensure_unique: bool,
    #[serde(default)]
    children: Vec<JobSpec>,
    #[serde(default)]
    child: Option<Box<JobSpec>>,
}

#[derive(Debug, Default, Deserialize)]
struct ZoektSpec {
    #[serde(rename = "type")]
    kind: String,
    #[serde(default)]
    pattern: String,
    #[serde(default)]
    case_sensitive: bool,
    #[serde(default)]
    file_name: bool,
    #[serde(default)]
    content: bool,
    #[serde(default)]
    children: Vec<ZoektSpec>,
    #[serde(default)]
    child: Option<Box<ZoektSpec>>,
}

// ---------------------------------------------------------------------------
// JSON helpers
// ---------------------------------------------------------------------------

/// Build a JSON object whose keys are inserted in lexicographic order, matching
/// Go's `json.Marshal` map-key ordering (and `serde_json`'s sorted map in this
/// workspace, where `preserve_order` is off).
fn obj(pairs: Vec<(&str, Value)>) -> Value {
    let map: BTreeMap<String, Value> = pairs
        .into_iter()
        .map(|(key, value)| (key.to_owned(), value))
        .collect();
    Value::Object(map.into_iter().collect())
}

fn strings(values: &[String]) -> Value {
    Value::Array(values.iter().cloned().map(Value::String).collect())
}

fn regexp_strings(regexps: &[regex::Regex]) -> Value {
    strings(
        &regexps
            .iter()
            .map(|regexp| regexp.as_str().to_owned())
            .collect::<Vec<_>>(),
    )
}

fn base(case: &Case, result: Value) -> Value {
    obj(vec![
        ("kind", Value::String(case.kind.clone())),
        ("name", Value::String(case.name.clone())),
        ("result", result),
    ])
}

// ---------------------------------------------------------------------------
// corpus / Go value translation
// ---------------------------------------------------------------------------

fn search_type(value: &str) -> SearchType {
    match value {
        "" | "regexp" => SearchType::Regex,
        "literal" => SearchType::Literal,
        "structural" => SearchType::Structural,
        "standard" => SearchType::Standard,
        "keyword" => SearchType::Keyword,
        "codycontext" => SearchType::CodyContext,
        other => panic!("unknown search type {other}"),
    }
}

fn protocol(value: &str) -> Protocol {
    match value {
        "" | "streaming" => Protocol::Streaming,
        "batch" => Protocol::Batch,
        "exhaustive" => Protocol::Exhaustive,
        other => panic!("unknown protocol {other}"),
    }
}

fn basic(case: &Case) -> Basic {
    let plan = pipeline_query(&case.query, search_type(&case.search_type))
        .unwrap_or_else(|error| panic!("{}: parse {:?}: {error}", case.name, case.query));
    assert_eq!(
        plan.len(),
        1,
        "{}: expected one basic query, got {}",
        case.name,
        plan.len()
    );
    plan.into_iter().next().unwrap()
}

fn result_types(names: &[String]) -> jb::ResultTypes {
    let mut types = jb::ResultTypes::default();
    for name in names {
        let value = jb::ResultTypes::from_string(name)
            .unwrap_or_else(|| panic!("unknown result type {name:?}"));
        types = types.with(value);
    }
    types
}

fn features(case: &Case) -> Features {
    let mut features = Features::default();
    if let Some(spec) = &case.features {
        features.content_based_lang_filters = spec.content_based_lang_filters;
    }
    features
}

fn user_settings(case: &Case) -> jb::UserSettings {
    jb::UserSettings {
        search_include_forks: case.user_settings.search_include_forks,
        search_include_archived: case.user_settings.search_include_archived,
    }
}

fn yes_no(value: YesNoOnly) -> Value {
    Value::String(
        match value {
            YesNoOnly::Yes => "yes",
            YesNoOnly::No => "no",
            YesNoOnly::Only => "only",
            YesNoOnly::Invalid => "invalid",
        }
        .to_owned(),
    )
}

fn visibility(value: RepoVisibility) -> Value {
    Value::String(
        match value {
            RepoVisibility::Private => "private",
            RepoVisibility::Public => "public",
            RepoVisibility::Any => "any",
        }
        .to_owned(),
    )
}

fn query_node(node: &QueryNode) -> Value {
    match node {
        QueryNode::Pattern(pattern) => obj(vec![(
            "pattern",
            obj(vec![
                ("boost", Value::Bool(pattern.boost)),
                ("is_negated", Value::Bool(pattern.is_negated)),
                ("is_regexp", Value::Bool(pattern.is_regexp)),
                ("value", Value::String(pattern.value.clone())),
            ]),
        )]),
        QueryNode::And(children) => obj(vec![(
            "and",
            Value::Array(children.iter().map(query_node).collect()),
        )]),
        QueryNode::Or(children) => obj(vec![(
            "or",
            Value::Array(children.iter().map(query_node).collect()),
        )]),
    }
}

fn serialize_repo_options(options: &jb::RepoOptions) -> Value {
    let filters = options
        .repo_filters
        .iter()
        .map(|filter| {
            let revs = filter
                .revs
                .iter()
                .map(|rev| {
                    obj(vec![
                        (
                            "exclude_ref_glob",
                            Value::String(rev.exclude_ref_glob.clone()),
                        ),
                        ("ref_glob", Value::String(rev.ref_glob.clone())),
                        (
                            "rev_at_time",
                            Value::Bool(rev.rev_at_time || rev.rev_at_time_payload.is_some()),
                        ),
                        ("rev_spec", Value::String(rev.rev_spec.clone())),
                    ])
                })
                .collect::<Vec<_>>();
            obj(vec![
                ("repo", Value::String(filter.repo.clone())),
                ("revs", Value::Array(revs)),
            ])
        })
        .collect::<Vec<_>>();

    let commit_after = match &options.commit_after {
        Some(commit_after) => obj(vec![
            ("negated", Value::Bool(commit_after.negated)),
            ("time_ref", Value::String(commit_after.time_ref.clone())),
        ]),
        None => Value::Null,
    };

    let file_content = options
        .has_file_content
        .iter()
        .map(|args| {
            obj(vec![
                ("content", Value::String(args.content.clone())),
                ("negated", Value::Bool(args.negated)),
                ("path", Value::String(args.path.clone())),
            ])
        })
        .collect::<Vec<_>>();

    let kvps = options
        .has_kvps
        .iter()
        .map(|kvp| {
            obj(vec![
                ("key", Value::String(kvp.key.clone())),
                ("key_only", Value::Bool(kvp.key_only)),
                ("negated", Value::Bool(kvp.negated)),
                (
                    "value",
                    match &kvp.value {
                        Some(value) => Value::String(value.clone()),
                        None => Value::Null,
                    },
                ),
            ])
        })
        .collect::<Vec<_>>();

    let topics = options
        .has_topics
        .iter()
        .map(|topic| {
            obj(vec![
                ("negated", Value::Bool(topic.negated)),
                ("topic", Value::String(topic.topic.clone())),
            ])
        })
        .collect::<Vec<_>>();

    obj(vec![
        ("archived_set", Value::Bool(options.archived_set)),
        ("commit_after", commit_after),
        ("cursors", Value::from(options.cursors)),
        (
            "description_patterns",
            strings(&options.description_patterns),
        ),
        ("fork_set", Value::Bool(options.fork_set)),
        ("has_file_content", Value::Array(file_content)),
        ("has_kvps", Value::Array(kvps)),
        ("has_topics", Value::Array(topics)),
        ("minus_repo_filters", strings(&options.minus_repo_filters)),
        ("no_archived", Value::Bool(options.no_archived)),
        ("no_forks", Value::Bool(options.no_forks)),
        ("only_archived", Value::Bool(options.only_archived)),
        ("only_forks", Value::Bool(options.only_forks)),
        ("repo_filters", Value::Array(filters)),
        (
            "search_context_spec",
            Value::String(options.search_context_spec.clone()),
        ),
        ("use_index", yes_no(options.use_index)),
        ("visibility", visibility(options.visibility)),
    ])
}

fn build_job(spec: &JobSpec) -> jb::RacingJob {
    match spec.kind.as_str() {
        "noop" => jb::RacingJob::Noop,
        "repo_pager" => {
            let child = spec.child.as_ref().expect("repo_pager requires child");
            jb::RacingJob::RepoPager(Box::new(build_job(child)))
        }
        "repo_search" => jb::RacingJob::RepoSearch,
        "searcher_text" => jb::RacingJob::SearcherText,
        "zoekt_repo_subset_text" => jb::RacingJob::ZoektRepoSubsetText,
        "zoekt_global_text" => jb::RacingJob::ZoektGlobalText,
        "sequential" => jb::RacingJob::Sequential {
            ensure_unique: spec.ensure_unique,
            children: spec.children.iter().map(build_job).collect(),
        },
        "parallel" => jb::RacingJob::Parallel(spec.children.iter().map(build_job).collect()),
        other => panic!("unknown job type {other}"),
    }
}

fn serialize_job(job: &jb::RacingJob) -> Value {
    match job {
        jb::RacingJob::Noop => obj(vec![("type", Value::String("noop".to_owned()))]),
        jb::RacingJob::RepoPager(child) => obj(vec![
            ("child", serialize_job(child)),
            ("type", Value::String("repo_pager".to_owned())),
        ]),
        jb::RacingJob::RepoSearch => obj(vec![("type", Value::String("repo_search".to_owned()))]),
        jb::RacingJob::SearcherText => {
            obj(vec![("type", Value::String("searcher_text".to_owned()))])
        }
        jb::RacingJob::ZoektRepoSubsetText => obj(vec![(
            "type",
            Value::String("zoekt_repo_subset_text".to_owned()),
        )]),
        jb::RacingJob::ZoektGlobalText => obj(vec![(
            "type",
            Value::String("zoekt_global_text".to_owned()),
        )]),
        jb::RacingJob::Sequential {
            ensure_unique,
            children,
        } => obj(vec![
            (
                "children",
                Value::Array(children.iter().map(serialize_job).collect()),
            ),
            ("ensure_unique", Value::Bool(*ensure_unique)),
            ("type", Value::String("sequential".to_owned())),
        ]),
        jb::RacingJob::Parallel(children) => obj(vec![
            (
                "children",
                Value::Array(children.iter().map(serialize_job).collect()),
            ),
            ("type", Value::String("parallel".to_owned())),
        ]),
    }
}

fn build_zoekt(spec: &ZoektSpec) -> Query {
    match spec.kind.as_str() {
        "regexp" => Query::Regexp {
            regexp: spec.pattern.clone(),
            file_name: spec.file_name,
            content: spec.content,
            case_sensitive: spec.case_sensitive,
        },
        "substring" => Query::Substring {
            pattern: spec.pattern.clone(),
            case_sensitive: spec.case_sensitive,
            file_name: spec.file_name,
            content: spec.content,
        },
        "and" => Query::And(spec.children.iter().map(build_zoekt).collect()),
        "or" => Query::Or(spec.children.iter().map(build_zoekt).collect()),
        "not" => Query::Not(Box::new(build_zoekt(
            spec.child.as_ref().expect("not requires child"),
        ))),
        "type" => Query::Type {
            child: Box::new(build_zoekt(
                spec.child.as_ref().expect("type requires child"),
            )),
            kind: TypeKind::FileName,
        },
        "boost" => Query::Boost {
            child: Box::new(build_zoekt(
                spec.child.as_ref().expect("boost requires child"),
            )),
            boost: 2.0,
        },
        "language" => Query::Language(spec.pattern.clone()),
        "repo" => Query::Repo(spec.pattern.clone()),
        "const" => Query::Const(true),
        "symbol" => Query::Symbol(Box::new(build_zoekt(
            spec.child.as_ref().expect("symbol requires child"),
        ))),
        other => panic!("unknown zoekt type {other}"),
    }
}

// ---------------------------------------------------------------------------
// case handlers
// ---------------------------------------------------------------------------

fn run(case: &Case) -> Value {
    match case.kind.as_str() {
        "path_regexps" => {
            let basic = basic(case);
            match jb::to_text_pattern_info(
                &basic,
                result_types(&case.result_types),
                &features(case),
                case.default_limit,
            ) {
                Ok(info) => base(
                    case,
                    obj(vec![(
                        "regexps",
                        regexp_strings(&jb::get_path_regexps(&basic, &info)),
                    )]),
                ),
                Err(error) => base(case, obj(vec![("error", Value::String(error))])),
            }
        }
        "file_match_limit" => {
            let basic = basic(case);
            base(
                case,
                obj(vec![(
                    "limit",
                    Value::from(jb::compute_file_match_limit(&basic, case.default_limit)),
                )]),
            )
        }
        "ownership" => {
            let basic = basic(case);
            let (include, exclude, ok) = jb::is_ownership_search(&basic);
            base(
                case,
                obj(vec![
                    ("exclude", strings(&exclude)),
                    ("include", strings(&include)),
                    ("ok", Value::Bool(ok)),
                ]),
            )
        }
        "contributor" => {
            let basic = basic(case);
            let (include, exclude, ok) = jb::is_contributor_search(&basic);
            base(
                case,
                obj(vec![
                    ("exclude", strings(&exclude)),
                    ("include", strings(&include)),
                    ("ok", Value::Bool(ok)),
                ]),
            )
        }
        "select_owners" => {
            let path = jb::select_path_from_string(&case.select).unwrap_or_default();
            base(
                case,
                obj(vec![
                    ("owners", Value::Bool(jb::is_select_owners_search(&path))),
                    ("path", strings(&path)),
                ]),
            )
        }
        "contributors_as_regexp" => base(
            case,
            obj(vec![(
                "regexps",
                regexp_strings(&jb::contributors_as_regexp(
                    &case.contributors,
                    case.case_sensitive,
                )),
            )]),
        ),
        "timeout" => {
            let basic = basic(case);
            let max = if case.max_timeout_seconds == 0 {
                jb::DEFAULT_MAX_TIMEOUT_SECONDS
            } else {
                case.max_timeout_seconds
            };
            base(
                case,
                obj(vec![(
                    "nanos",
                    Value::from(jb::timeout_duration(protocol(&case.protocol), &basic, max)),
                )]),
            )
        }
        "map_slice" => {
            let mapped = match case.map_op.as_str() {
                "" | "identity" => jb::map_slice(&case.values, &|value| value.to_owned()),
                "quote_meta" => jb::map_slice(&case.values, &|value| regexp_quote_meta(value)),
                "case_insensitive" => {
                    jb::map_slice(&case.values, &|value| case_insensitive_regexp(value))
                }
                other => panic!("unknown map op {other}"),
            };
            base(case, obj(vec![("values", strings(&mapped))]))
        }
        "lang_filters" => base(
            case,
            obj(vec![(
                "langs",
                strings(&jb::to_lang_filters(&case.aliases)),
            )]),
        ),
        "result_types" => {
            let basic = basic(case);
            let types = jb::compute_result_types(
                &basic,
                search_type(&case.search_type),
                result_types(&case.default_types),
            );
            base(case, obj(vec![("types", Value::String(types.as_string()))]))
        }
        "text_pattern_info" => {
            let basic = basic(case);
            match jb::to_text_pattern_info(
                &basic,
                result_types(&case.result_types),
                &features(case),
                case.default_limit,
            ) {
                Err(error) => base(
                    case,
                    obj(vec![
                        ("error", Value::String(error)),
                        ("ok", Value::Bool(false)),
                    ]),
                ),
                Ok(info) => base(
                    case,
                    obj(vec![
                        (
                            "info",
                            obj(vec![
                                ("comby_rule", Value::String(info.comby_rule.clone())),
                                ("exclude_langs", strings(&info.exclude_langs)),
                                ("exclude_paths", Value::String(info.exclude_paths.clone())),
                                ("file_match_limit", Value::from(info.file_match_limit)),
                                ("include_langs", strings(&info.include_langs)),
                                ("include_paths", strings(&info.include_paths)),
                                ("index", yes_no(info.index)),
                                ("is_case_sensitive", Value::Bool(info.is_case_sensitive)),
                                ("is_structural_pat", Value::Bool(info.is_structural_pat)),
                                ("languages", strings(&info.languages)),
                                (
                                    "path_patterns_are_case_sensitive",
                                    Value::Bool(info.path_patterns_are_case_sensitive),
                                ),
                                (
                                    "pattern_matches_content",
                                    Value::Bool(info.pattern_matches_content),
                                ),
                                (
                                    "pattern_matches_path",
                                    Value::Bool(info.pattern_matches_path),
                                ),
                                ("query", query_node(&info.query)),
                                ("select", strings(&info.select)),
                            ]),
                        ),
                        ("ok", Value::Bool(true)),
                    ]),
                ),
            }
        }
        "symbol_search_request" => {
            let basic = basic(case);
            match jb::to_symbol_search_request(&basic, &features(case)) {
                Err(error) => base(
                    case,
                    obj(vec![
                        ("error", Value::String(error)),
                        ("ok", Value::Bool(false)),
                    ]),
                ),
                Ok(request) => base(
                    case,
                    obj(vec![
                        ("ok", Value::Bool(true)),
                        (
                            "request",
                            obj(vec![
                                ("exclude_langs", strings(&request.exclude_langs)),
                                (
                                    "exclude_pattern",
                                    Value::String(request.exclude_pattern.clone()),
                                ),
                                ("include_langs", strings(&request.include_langs)),
                                ("include_patterns", strings(&request.include_patterns)),
                                ("is_case_sensitive", Value::Bool(request.is_case_sensitive)),
                                (
                                    "regexp_pattern",
                                    Value::String(request.regexp_pattern.clone()),
                                ),
                            ]),
                        ),
                    ]),
                ),
            }
        }
        "repo_options" => {
            let basic = basic(case);
            base(
                case,
                serialize_repo_options(&jb::to_repo_options(&basic, &user_settings(case))),
            )
        }
        "job_mode" => {
            let basic = basic(case);
            let mut options = jb::to_repo_options(&basic, &jb::UserSettings::default());
            options.cursors = case.cursors;
            let inputs = jb::SearchInputs {
                protocol: protocol(&case.protocol),
                pattern_type: search_type(&case.pattern_type),
                on_sourcegraph_dot_com: case.on_sourcegraph_dot_com,
            };
            let (repo_universe, skip_subset, run_zoekt) =
                jb::job_mode(&basic, &options, result_types(&case.result_types), &inputs);
            base(
                case,
                obj(vec![
                    ("repo_universe_search", Value::Bool(repo_universe)),
                    ("run_zoekt_over_repos", Value::Bool(run_zoekt)),
                    ("skip_repo_subset_search", Value::Bool(skip_subset)),
                ]),
            )
        }
        "is_global" => {
            let basic = basic(case);
            let mut options = jb::to_repo_options(&basic, &jb::UserSettings::default());
            options.cursors = case.cursors;
            base(
                case,
                obj(vec![("global", Value::Bool(jb::is_global(&options)))]),
            )
        }
        "order_racing_jobs" => {
            let tree = case.tree.as_ref().expect("tree required");
            base(
                case,
                serialize_job(&jb::order_racing_jobs(&build_job(tree))),
            )
        }
        "zoekt_regexps" => {
            let spec = case.zoekt.as_ref().expect("zoekt required");
            base(
                case,
                obj(vec![(
                    "regexps",
                    regexp_strings(&jb::zoekt_query_patterns_as_regexps(&build_zoekt(spec))),
                )]),
            )
        }
        other => panic!("unknown kind {other}"),
    }
}

#[test]
fn search_jobutil_job_builders_differential() {
    if std::env::var("SG_M9HN_DIFF").as_deref() != Ok("1") {
        eprintln!("SG_M9HN_DIFF not set; skipping");
        return;
    }
    let corpus_path = std::env::var("M9HN_CORPUS").expect("M9HN_CORPUS must be set");
    let out_path = std::env::var("M9HN_OUT").expect("M9HN_OUT must be set");

    let raw = std::fs::read_to_string(&corpus_path)
        .unwrap_or_else(|error| panic!("read {corpus_path}: {error}"));
    let cases: Vec<Case> =
        serde_json::from_str(&raw).unwrap_or_else(|error| panic!("parse {corpus_path}: {error}"));

    let output: Vec<Value> = cases.iter().map(run).collect();
    let encoded = serde_json::to_string(&output).expect("encode output");
    std::fs::write(&out_path, encoded).unwrap_or_else(|error| panic!("write {out_path}: {error}"));
}
