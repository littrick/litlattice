//! M9.41d differential harness: the Plan-stage request actor reaches
//! `searchcontexts.ResolveSearchContextSpec` (`client.go:106`). Go reads the
//! actor from `context.Context`; the Rust port passes a
//! [`RequestContext`] into `SearchClient::plan_with_context` and from there
//! into the injected resolver.
//!
//! Env:
//!
//! * `M941D_PLAN_CORPUS` corpus JSON
//! * `M941D_PLAN_OUT` output JSON

use std::collections::BTreeMap;
use std::sync::Arc;

use serde::Deserialize;
use serde_json::{json, Value};

use sg_search_query::parser::SearchType;
use sg_search_query::to_json;
use sg_service_frontend::search_client::{
    ExperimentalFeatures, FeatureFlagLoader, PlanDeps, Protocol, SanitizePatternsLoader,
    SearchClient, SearchContextResolver, SearchMode, Settings, SettingsLoader,
};
use sg_service_frontend::search_contexts::{
    resolve_search_context_spec, Error, GetSearchContextOptions, MinimalRepo, Namespace,
    OrgMembership, ReposListOptions, RequestContext, SearchContext,
    SearchContextRepositoryRevisions, SearchContextsDatabase, User,
};

// ---------------------------------------------------------------------------
// corpus
// ---------------------------------------------------------------------------

#[derive(Debug, Default, Deserialize, Clone)]
struct ActorSpec {
    #[serde(default)]
    uid: i32,
    #[serde(default)]
    internal: bool,
}

#[derive(Debug, Default, Deserialize, Clone)]
struct NamespaceDef {
    #[serde(default)]
    name: String,
    #[serde(default)]
    user: i32,
    #[serde(default)]
    organization: i32,
}

#[derive(Debug, Default, Deserialize, Clone)]
struct OrgMemberDef {
    #[serde(default)]
    org_id: i32,
    #[serde(default)]
    user_id: i32,
}

#[derive(Debug, Default, Deserialize, Clone)]
struct ContextSpec {
    #[serde(default)]
    query: String,
    #[serde(default)]
    error: Option<String>,
    #[serde(default)]
    namespace: String,
}

#[derive(Debug, Default, Deserialize)]
struct Case {
    #[serde(default)]
    name: String,
    #[serde(default)]
    version: String,
    #[serde(default)]
    pattern_type: Option<String>,
    #[serde(default)]
    query: String,
    #[serde(default)]
    search_mode: i32,
    #[serde(default)]
    protocol: i32,
    #[serde(default)]
    context_lines: Option<i32>,
    #[serde(default)]
    on_sourcegraph_dot_com: bool,
    #[serde(default)]
    actor: Option<ActorSpec>,
    #[serde(default)]
    namespaces: Vec<NamespaceDef>,
    #[serde(default)]
    org_members: Vec<OrgMemberDef>,
    #[serde(default)]
    contexts: BTreeMap<String, ContextSpec>,
}

// ---------------------------------------------------------------------------
// read-only fake `SearchContextsDatabase`
// ---------------------------------------------------------------------------

#[derive(Debug, Default, Clone)]
struct ContextDef {
    name: String,
    namespace_user_id: i32,
    namespace_org_id: i32,
    query: String,
}

struct FakeDb {
    namespaces: Vec<NamespaceDef>,
    org_members: Vec<OrgMemberDef>,
    contexts: BTreeMap<String, ContextDef>,
    errors: BTreeMap<String, String>,
}

impl FakeDb {
    fn from_case(case: &Case) -> FakeDb {
        let mut contexts = BTreeMap::new();
        let mut errors = BTreeMap::new();
        for (name, spec) in &case.contexts {
            let namespace = case
                .namespaces
                .iter()
                .find(|namespace| namespace.name == spec.namespace);
            contexts.insert(
                name.clone(),
                ContextDef {
                    name: name.clone(),
                    namespace_user_id: namespace.map_or(0, |namespace| namespace.user),
                    namespace_org_id: namespace.map_or(0, |namespace| namespace.organization),
                    query: spec.query.clone(),
                },
            );
            if let Some(error) = &spec.error {
                errors.insert(name.clone(), error.clone());
            }
        }
        FakeDb {
            namespaces: case.namespaces.clone(),
            org_members: case.org_members.clone(),
            contexts,
            errors,
        }
    }
}

impl SearchContextsDatabase for FakeDb {
    fn get_namespace_by_name(&self, name: &str) -> Result<Namespace, Error> {
        self.namespaces
            .iter()
            .find(|namespace| namespace.name == name)
            .map(|namespace| Namespace {
                user: namespace.user,
                organization: namespace.organization,
            })
            .ok_or_else(Error::namespace_not_found)
    }

    fn get_org_member_by_org_id_and_user_id(
        &self,
        org_id: i32,
        user_id: i32,
    ) -> Result<Option<OrgMembership>, Error> {
        if self
            .org_members
            .iter()
            .any(|member| member.org_id == org_id && member.user_id == user_id)
        {
            return Ok(Some(OrgMembership { org_id, user_id }));
        }
        Err(Error::org_member_not_found())
    }

    fn get_current_user(&self, _uid: i32) -> Result<Option<User>, Error> {
        Ok(None)
    }

    fn get_search_context(&self, opts: &GetSearchContextOptions) -> Result<SearchContext, Error> {
        let Some(context) = self.contexts.get(&opts.name) else {
            return Err(Error::search_context_not_found());
        };
        if let Some(error) = self.errors.get(&opts.name) {
            return Err(Error::other(error.clone()));
        }
        Ok(SearchContext {
            name: context.name.clone(),
            namespace_user_id: context.namespace_user_id,
            namespace_org_id: context.namespace_org_id,
            query: context.query.clone(),
            ..SearchContext::default()
        })
    }

    fn create_search_context_with_repository_revisions(
        &self,
        search_context: &SearchContext,
        _repository_revisions: &[SearchContextRepositoryRevisions],
    ) -> Result<SearchContext, Error> {
        Ok(search_context.clone())
    }

    fn update_search_context_with_repository_revisions(
        &self,
        search_context: &SearchContext,
        _repository_revisions: &[SearchContextRepositoryRevisions],
    ) -> Result<SearchContext, Error> {
        Ok(search_context.clone())
    }

    fn delete_search_context(&self, _search_context_id: i64) -> Result<(), Error> {
        Ok(())
    }

    fn get_all_revisions_for_repos(
        &self,
        _repo_ids: &[i32],
    ) -> Result<BTreeMap<i32, Vec<String>>, Error> {
        Ok(BTreeMap::new())
    }

    fn get_all_queries(&self) -> Result<Vec<String>, Error> {
        Ok(Vec::new())
    }

    fn get_search_context_repository_revisions(
        &self,
        _search_context_id: i64,
    ) -> Result<Vec<SearchContextRepositoryRevisions>, Error> {
        Ok(Vec::new())
    }

    fn list_minimal_repos(&self, _opts: &ReposListOptions) -> Result<Vec<MinimalRepo>, Error> {
        Ok(Vec::new())
    }

    fn create_search_context_star_for_user(
        &self,
        _user_id: i32,
        _search_context_id: i64,
    ) -> Result<(), Error> {
        Ok(())
    }

    fn delete_search_context_star_for_user(
        &self,
        _user_id: i32,
        _search_context_id: i64,
    ) -> Result<(), Error> {
        Ok(())
    }

    fn set_user_default_search_context_id(
        &self,
        _user_id: i32,
        _search_context_id: i64,
    ) -> Result<(), Error> {
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// execution
// ---------------------------------------------------------------------------

fn search_mode(value: i32) -> SearchMode {
    if value == 1 {
        SearchMode::SmartSearch
    } else {
        SearchMode::Precise
    }
}

fn protocol(value: i32) -> Protocol {
    match value {
        1 => Protocol::Batch,
        2 => Protocol::Exhaustive,
        _ => Protocol::Streaming,
    }
}

fn search_type_str(search_type: SearchType) -> &'static str {
    match search_type {
        SearchType::Regex => "regex",
        SearchType::Literal => "literal",
        SearchType::Structural => "structural",
        SearchType::Standard => "standard",
        SearchType::CodyContext => "codycontext",
        SearchType::Keyword => "keyword",
    }
}

fn run_case(case: &Case) -> Value {
    let user_settings: SettingsLoader = Box::new(|| {
        Ok(Settings {
            experimental_features: Some(ExperimentalFeatures::default()),
            ..Settings::default()
        })
    });
    let feature_flags: FeatureFlagLoader = Box::new(BTreeMap::new);
    let sanitize: SanitizePatternsLoader = Box::new(Vec::new);

    let db = Arc::new(FakeDb::from_case(case));
    let resolver_db = Arc::clone(&db);
    let resolve_search_context: SearchContextResolver = Arc::new(move |spec, ctx| {
        resolve_search_context_spec(resolver_db.as_ref(), ctx, spec)
            .map(|context| context.query)
            .map_err(|error| error.to_string())
    });

    let deps = PlanDeps {
        user_settings,
        feature_flags,
        resolve_search_context,
        sanitize_search_patterns: sanitize,
        on_sourcegraph_dot_com: case.on_sourcegraph_dot_com,
    };

    let actor = case.actor.clone().unwrap_or_default();
    let request_context = RequestContext {
        actor_uid: actor.uid,
        internal: actor.internal,
        sourcegraph_dot_com_mode: case.on_sourcegraph_dot_com,
    };

    let result = SearchClient::new(deps).plan_with_context(
        &case.version,
        case.pattern_type.as_deref(),
        &case.query,
        search_mode(case.search_mode),
        protocol(case.protocol),
        case.context_lines,
        &request_context,
    );

    let mut entry: BTreeMap<&str, Value> = BTreeMap::new();
    entry.insert("name", json!(case.name));
    match result {
        Err(err) => {
            entry.insert("ok", json!(false));
            entry.insert("error", json!(err.to_string()));
        }
        Ok(inputs) => {
            entry.insert("ok", json!(true));
            entry.insert("original_query", json!(inputs.original_query));
            entry.insert("search_mode", json!(inputs.search_mode.as_int()));
            entry.insert("pattern_type", json!(search_type_str(inputs.pattern_type)));
            entry.insert("protocol", json!(inputs.protocol.as_str()));
            entry.insert("context_lines", json!(inputs.context_lines));
            entry.insert(
                "on_sourcegraph_dot_com",
                json!(inputs.on_sourcegraph_dot_com),
            );
            entry.insert(
                "features",
                json!({
                    "content_based_lang_filters": inputs.features.content_based_lang_filters,
                    "debug": inputs.features.debug,
                }),
            );

            let plan: Vec<Value> = inputs
                .plan
                .iter()
                .map(|basic| {
                    serde_json::from_str(&to_json(&basic.to_parse_tree()))
                        .expect("basic parse tree is valid JSON")
                })
                .collect();
            entry.insert("plan", Value::Array(plan));

            let query: Value =
                serde_json::from_str(&to_json(&inputs.query)).expect("query is valid JSON");
            entry.insert("query", query);
        }
    }
    Value::Object(entry.into_iter().map(|(k, v)| (k.to_owned(), v)).collect())
}

#[test]
fn m941d_plan_actor_diff() {
    if std::env::var("SG_M941D_PLAN_DIFF").as_deref() != Ok("1") {
        return;
    }
    let corpus_path =
        std::env::var("M941D_PLAN_CORPUS").expect("M941D_PLAN_CORPUS must be set");
    let out_path = std::env::var("M941D_PLAN_OUT").expect("M941D_PLAN_OUT must be set");
    let raw = std::fs::read_to_string(&corpus_path).expect("read corpus");
    let cases: Vec<Case> = serde_json::from_str(&raw).expect("parse corpus");
    let entries: Vec<Value> = cases.iter().map(run_case).collect();
    std::fs::write(
        &out_path,
        serde_json::to_string(&entries).expect("encode output"),
    )
    .expect("write output");
}
