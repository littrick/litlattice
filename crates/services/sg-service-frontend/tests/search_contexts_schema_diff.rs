//! M9.8hu differential of the search contexts GraphQL schema registration
//! (`cmd/frontend/graphqlbackend/schema.go:58-61`,
//! `graphqlbackend.go:440-445,447,538-546,687-713,832`,
//! `cmd/frontend/internal/searchcontexts/init.go:26`) against
//! `cmd/frontend/graphqlbackend/zz_m9hu_oracle_test.go`.
//!
//! Both sides assemble the same SDL set, extract the same introspection subset,
//! derive the field -> handler table, verify the disabled (nil optional
//! resolver) absence, and dispatch a few resolver calls through the registered
//! surface using an in-memory fake database. The run script compares the bytes
//! produced by [`marshal_go`] with Go `encoding/json`.
//!
//! Env:
//!
//! * `SG_M9HU_DIFF=1`
//! * `M9HU_CORPUS` — the corpus JSON
//! * `M9HU_OUT` — output file

#![allow(clippy::unwrap_used)]

use std::collections::BTreeMap;

use serde::Deserialize;
use serde_json::{json, Map, Value};
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;

use sg_graphql::schema::{assemble_oss, Schema};
use sg_service_frontend::search_contexts::{
    Error, GetSearchContextOptions, MinimalRepo, Namespace, OrgMembership, ReposListOptions,
    RequestContext, SearchContext, SearchContextRepositoryRevisions, SearchContextsDatabase, User,
};
use sg_service_frontend::search_contexts_graphql::{
    marshal_search_context_id, marshal_user_id, ListSearchContextsOptions,
    ListSearchContextsPageOptions, NamespaceOrg, NamespaceUser, SearchContextRecord,
    SearchContextsGraphqlDatabase,
};
use sg_service_frontend::search_contexts_schema::{
    register_search_contexts_schema, SearchContextsFieldArgs, SearchContextsFieldBehavior,
    SearchContextsFieldValue, SearchContextsOperation, SEARCH_CONTEXTS_FIELD_HANDLERS,
    SEARCH_CONTEXT_NODE_HANDLER, SEARCH_CONTEXT_NODE_KIND,
};
use sg_service_frontend::settings_merge::marshal_go;

// ---------------------------------------------------------------------------
// Corpus
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Deserialize, Default)]
struct UserDef {
    #[serde(default)]
    id: i32,
    #[serde(default)]
    username: String,
    #[serde(default)]
    site_admin: bool,
}

#[derive(Debug, Clone, Deserialize, Default)]
struct ContextDef {
    #[serde(default)]
    id: i64,
    #[serde(default)]
    name: String,
    #[serde(default)]
    description: String,
    #[serde(default)]
    public: bool,
    #[serde(default)]
    namespace_user_id: i32,
    #[serde(default)]
    namespace_org_id: i32,
    #[serde(default)]
    namespace_user_name: String,
    #[serde(default)]
    namespace_org_name: String,
    #[serde(default)]
    query: String,
    #[serde(default)]
    auto_defined: bool,
    #[serde(default)]
    updated_at: String,
}

#[derive(Debug, Clone, Deserialize, Default)]
struct DefaultDef {
    #[serde(default)]
    user_id: i32,
    #[serde(default)]
    search_context_id: i64,
}

#[derive(Debug, Clone, Deserialize, Default)]
struct World {
    #[serde(default)]
    users: Vec<UserDef>,
    #[serde(default)]
    search_contexts: Vec<ContextDef>,
    #[serde(default)]
    defaults: Vec<DefaultDef>,
}

#[derive(Debug, Clone, Deserialize, Default)]
struct Case {
    #[serde(default)]
    name: String,
    #[serde(default)]
    kind: String,
    #[serde(default)]
    field: String,
    #[serde(default)]
    spec: String,
    #[serde(default)]
    id: String,
    #[serde(default)]
    search_context_id: String,
    #[serde(default)]
    user_id: i32,
    #[serde(default)]
    actor_uid: i32,
    #[serde(default)]
    select: Vec<String>,
}

#[derive(Debug, Clone, Deserialize, Default)]
struct Corpus {
    #[serde(default)]
    world: World,
    #[serde(default)]
    cases: Vec<Case>,
}

fn parse_time(value: &str) -> OffsetDateTime {
    if value.is_empty() {
        return sg_service_frontend::search_contexts_graphql::go_zero_time();
    }
    OffsetDateTime::parse(value, &Rfc3339).expect("corpus timestamp")
}

// ---------------------------------------------------------------------------
// Fake database
// ---------------------------------------------------------------------------

struct FakeDb {
    world: World,
}

impl FakeDb {
    fn new(world: World) -> Self {
        Self { world }
    }

    fn base_context(&self, context: &ContextDef) -> SearchContext {
        SearchContext {
            id: context.id,
            name: context.name.clone(),
            description: context.description.clone(),
            public: context.public,
            namespace_user_id: context.namespace_user_id,
            namespace_org_id: context.namespace_org_id,
            namespace_user_name: context.namespace_user_name.clone(),
            namespace_org_name: context.namespace_org_name.clone(),
            query: context.query.clone(),
            auto_defined: context.auto_defined,
        }
    }

    fn record(&self, context: &ContextDef, uid: i32) -> SearchContextRecord {
        SearchContextRecord {
            id: context.id,
            name: context.name.clone(),
            description: context.description.clone(),
            public: context.public,
            namespace_user_id: context.namespace_user_id,
            namespace_org_id: context.namespace_org_id,
            namespace_user_name: context.namespace_user_name.clone(),
            namespace_org_name: context.namespace_org_name.clone(),
            query: context.query.clone(),
            auto_defined: context.auto_defined,
            updated_at: parse_time(&context.updated_at),
            default: self
                .world
                .defaults
                .iter()
                .any(|d| d.user_id == uid && d.search_context_id == context.id),
            starred: false,
        }
    }
}

impl SearchContextsDatabase for FakeDb {
    fn get_namespace_by_name(&self, name: &str) -> Result<Namespace, Error> {
        for context in &self.world.search_contexts {
            if context.namespace_user_name == name {
                return Ok(Namespace {
                    user: context.namespace_user_id,
                    organization: 0,
                });
            }
            if context.namespace_org_name == name {
                return Ok(Namespace {
                    user: 0,
                    organization: context.namespace_org_id,
                });
            }
        }
        if let Some(user) = self.world.users.iter().find(|u| u.username == name) {
            return Ok(Namespace {
                user: user.id,
                organization: 0,
            });
        }
        Err(Error::namespace_not_found())
    }

    fn get_org_member_by_org_id_and_user_id(
        &self,
        _org_id: i32,
        _user_id: i32,
    ) -> Result<Option<OrgMembership>, Error> {
        Ok(None)
    }

    fn get_current_user(&self, uid: i32) -> Result<Option<User>, Error> {
        Ok(self
            .world
            .users
            .iter()
            .find(|user| user.id == uid)
            .map(|user| User {
                id: user.id,
                site_admin: user.site_admin,
            }))
    }

    fn get_search_context(&self, opts: &GetSearchContextOptions) -> Result<SearchContext, Error> {
        for context in &self.world.search_contexts {
            if context.name == opts.name
                && context.namespace_user_id == opts.namespace_user_id
                && context.namespace_org_id == opts.namespace_org_id
            {
                return Ok(self.base_context(context));
            }
        }
        Err(Error::search_context_not_found())
    }

    fn create_search_context_with_repository_revisions(
        &self,
        _search_context: &SearchContext,
        _repository_revisions: &[SearchContextRepositoryRevisions],
    ) -> Result<SearchContext, Error> {
        Err(Error::other("not used"))
    }

    fn update_search_context_with_repository_revisions(
        &self,
        _search_context: &SearchContext,
        _repository_revisions: &[SearchContextRepositoryRevisions],
    ) -> Result<SearchContext, Error> {
        Err(Error::other("not used"))
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

impl SearchContextsGraphqlDatabase for FakeDb {
    fn as_search_contexts_database(&self) -> &dyn SearchContextsDatabase {
        self
    }

    fn get_search_context_by_id(
        &self,
        ctx: &RequestContext,
        id: i64,
    ) -> Result<SearchContextRecord, Error> {
        self.world
            .search_contexts
            .iter()
            .find(|context| context.id == id)
            .map(|context| self.record(context, ctx.actor_uid))
            .ok_or_else(Error::search_context_not_found)
    }

    fn list_search_contexts(
        &self,
        _ctx: &RequestContext,
        _page: &ListSearchContextsPageOptions,
        _opts: &ListSearchContextsOptions,
    ) -> Result<Vec<SearchContextRecord>, Error> {
        Ok(Vec::new())
    }

    fn count_search_contexts(
        &self,
        _ctx: &RequestContext,
        _opts: &ListSearchContextsOptions,
    ) -> Result<i32, Error> {
        Ok(0)
    }

    fn get_default_search_context_for_current_user(
        &self,
        ctx: &RequestContext,
    ) -> Result<SearchContextRecord, Error> {
        let default = self
            .world
            .defaults
            .iter()
            .find(|default| default.user_id == ctx.actor_uid)
            .and_then(|default| {
                self.world
                    .search_contexts
                    .iter()
                    .find(|context| context.id == default.search_context_id)
            });
        match default {
            Some(context) => {
                let mut record = self.record(context, ctx.actor_uid);
                record.default = true;
                Ok(record)
            }
            None => {
                Ok(sg_service_frontend::search_contexts_graphql::global_search_context_record())
            }
        }
    }

    fn get_repos_set_by_ids(
        &self,
        _ids: &[i32],
    ) -> Result<BTreeMap<i32, sg_service_frontend::search_contexts_graphql::MinimalRepoRef>, Error>
    {
        Ok(BTreeMap::new())
    }

    fn get_user_by_id(&self, id: i32) -> Result<NamespaceUser, Error> {
        self.world
            .users
            .iter()
            .find(|user| user.id == id)
            .map(|user| NamespaceUser {
                id: user.id,
                username: user.username.clone(),
            })
            .ok_or_else(|| Error::other("user not found"))
    }

    fn get_org_by_id(&self, _id: i32) -> Result<NamespaceOrg, Error> {
        Err(Error::other("org not found"))
    }

    fn get_orgs_by_user_id(&self, _user_id: i32) -> Result<Vec<NamespaceOrg>, Error> {
        Ok(Vec::new())
    }
}

// ---------------------------------------------------------------------------
// Introspection subset (mirrors the Go oracle)
// ---------------------------------------------------------------------------

const ADDED_TYPES: &[&str] = &[
    "SearchContext",
    "SearchContextConnection",
    "SearchContextEditInput",
    "SearchContextInput",
    "SearchContextRepositoryRevisions",
    "SearchContextRepositoryRevisionsInput",
    "SearchContextsOrderBy",
];

const ADDED_QUERY_FIELDS: &[&str] = &[
    "defaultSearchContext",
    "isSearchContextAvailable",
    "searchContextBySpec",
    "searchContexts",
];

const ADDED_MUTATION_FIELDS: &[&str] = &[
    "createSearchContext",
    "createSearchContextStar",
    "deleteSearchContext",
    "deleteSearchContextStar",
    "setDefaultSearchContext",
    "updateSearchContext",
];

fn type_map(schema: &Value) -> BTreeMap<String, Value> {
    let mut map = BTreeMap::new();
    if let Some(types) = schema["__schema"]["types"].as_array() {
        for ty in types {
            if let Some(name) = ty["name"].as_str() {
                map.insert(name.to_owned(), ty.clone());
            }
        }
    }
    map
}

fn normalize_inputs(values: &Value) -> Value {
    let mut out: Vec<Value> = values
        .as_array()
        .map(|values| {
            values
                .iter()
                .map(|value| {
                    json!({
                        "name": value["name"],
                        "type": value["type"],
                        "defaultValue": value["defaultValue"],
                    })
                })
                .collect()
        })
        .unwrap_or_default();
    out.sort_by(|a, b| a["name"].as_str().cmp(&b["name"].as_str()));
    Value::Array(out)
}

fn normalize_fields(values: &Value) -> Value {
    let mut out: Vec<Value> = values
        .as_array()
        .map(|values| {
            values
                .iter()
                .map(|field| {
                    let mut entry = Map::new();
                    entry.insert("name".to_owned(), field["name"].clone());
                    entry.insert("type".to_owned(), field["type"].clone());
                    if field["args"].as_array().is_some() {
                        entry.insert("args".to_owned(), normalize_inputs(&field["args"]));
                    }
                    Value::Object(entry)
                })
                .collect()
        })
        .unwrap_or_default();
    out.sort_by(|a, b| a["name"].as_str().cmp(&b["name"].as_str()));
    Value::Array(out)
}

fn names(values: &Value) -> Value {
    let mut out: Vec<Value> = values
        .as_array()
        .map(|values| values.iter().map(|value| value["name"].clone()).collect())
        .unwrap_or_default();
    out.sort_by(|a, b| a.as_str().cmp(&b.as_str()));
    Value::Array(out)
}

fn inventory_types(schema: &Value) -> Value {
    let types = type_map(schema);
    let mut out = Vec::new();
    for name in ADDED_TYPES {
        let Some(ty) = types.get(*name) else {
            continue;
        };
        let mut entry = Map::new();
        entry.insert("name".to_owned(), json!(name));
        entry.insert("kind".to_owned(), ty["kind"].clone());
        if ty["fields"].as_array().is_some() {
            entry.insert("fields".to_owned(), normalize_fields(&ty["fields"]));
        }
        if ty["inputFields"].as_array().is_some() {
            entry.insert(
                "inputFields".to_owned(),
                normalize_inputs(&ty["inputFields"]),
            );
        }
        if ty["enumValues"].as_array().is_some() {
            entry.insert("enumValues".to_owned(), names(&ty["enumValues"]));
        }
        if ty["interfaces"].as_array().is_some() {
            entry.insert("interfaces".to_owned(), names(&ty["interfaces"]));
        }
        out.push(Value::Object(entry));
    }
    Value::Array(out)
}

fn root_fields(schema: &Value, root: &str, wanted: &[&str]) -> Value {
    let types = type_map(schema);
    let Some(ty) = types.get(root) else {
        return Value::Array(Vec::new());
    };
    let fields = ty["fields"].as_array().cloned().unwrap_or_default();
    let selected: Vec<Value> = fields
        .into_iter()
        .filter(|field| {
            field["name"]
                .as_str()
                .is_some_and(|name| wanted.contains(&name))
        })
        .collect();
    normalize_fields(&Value::Array(selected))
}

fn present_types(schema: &Value) -> BTreeMap<String, bool> {
    type_map(schema)
        .into_keys()
        .map(|name| (name, true))
        .collect()
}

fn present_fields(schema: &Value, root: &str) -> BTreeMap<String, bool> {
    let types = type_map(schema);
    let mut present = BTreeMap::new();
    if let Some(ty) = types.get(root) {
        if let Some(fields) = ty["fields"].as_array() {
            for field in fields {
                if let Some(name) = field["name"].as_str() {
                    present.insert(name.to_owned(), true);
                }
            }
        }
    }
    present
}

fn absent(present: &BTreeMap<String, bool>, wanted: &[&str]) -> Value {
    let mut out: Vec<Value> = wanted
        .iter()
        .filter(|name| !present.contains_key(**name))
        .map(|name| json!(name))
        .collect();
    out.sort_by(|a, b| a.as_str().cmp(&b.as_str()));
    Value::Array(out)
}

fn schema_inventory(schema: &Schema) -> Value {
    let intro: Value = serde_json::from_str(&schema.to_json()).expect("introspection JSON");
    json!({
        "types": inventory_types(&intro),
        "query_fields": root_fields(&intro, "Query", ADDED_QUERY_FIELDS),
        "mutation_fields": root_fields(&intro, "Mutation", ADDED_MUTATION_FIELDS),
    })
}

fn disabled_inventory(schema: &Schema) -> Value {
    let intro: Value = serde_json::from_str(&schema.to_json()).expect("introspection JSON");
    json!({
        "absent_types": absent(&present_types(&intro), ADDED_TYPES),
        "absent_query_fields": absent(&present_fields(&intro, "Query"), ADDED_QUERY_FIELDS),
        "absent_mutation_fields": absent(
            &present_fields(&intro, "Mutation"),
            ADDED_MUTATION_FIELDS,
        ),
    })
}

// ---------------------------------------------------------------------------
// Registration table
// ---------------------------------------------------------------------------

fn registration() -> Value {
    let table = |operation: SearchContextsOperation| {
        let mut rows: Vec<Value> = SEARCH_CONTEXTS_FIELD_HANDLERS
            .iter()
            .filter(|handler| handler.operation == operation)
            .map(|handler| {
                json!({
                    "field": handler.field,
                    "handler": handler.handler,
                    "behavior": handler.behavior.as_str(),
                })
            })
            .collect();
        rows.sort_by(|a, b| a["field"].as_str().cmp(&b["field"].as_str()));
        Value::Array(rows)
    };
    let mut node = Map::new();
    node.insert(
        SEARCH_CONTEXT_NODE_KIND.to_owned(),
        json!(SEARCH_CONTEXT_NODE_HANDLER),
    );
    json!({
        "query": table(SearchContextsOperation::Query),
        "mutation": table(SearchContextsOperation::Mutation),
        "node": Value::Object(node),
    })
}

// ---------------------------------------------------------------------------
// Resolver calls
// ---------------------------------------------------------------------------

fn request_context(uid: i32) -> RequestContext {
    RequestContext {
        actor_uid: uid,
        internal: false,
        sourcegraph_dot_com_mode: false,
    }
}

fn context_value(
    resolver: &sg_service_frontend::search_contexts_graphql::SearchContextResolver<'_>,
    select: &[String],
) -> Value {
    let mut out = Map::new();
    for field in select {
        let value = match field.as_str() {
            "id" => json!(resolver.id()),
            "name" => json!(resolver.name()),
            "description" => json!(resolver.description()),
            "spec" => json!(resolver.spec()),
            "public" => json!(resolver.public()),
            "autoDefined" => json!(resolver.auto_defined()),
            "query" => json!(resolver.query()),
            "viewerCanManage" => json!(resolver.viewer_can_manage(&request_context(0))),
            "viewerHasAsDefault" => json!(resolver.viewer_has_as_default()),
            "viewerHasStarred" => json!(resolver.viewer_has_starred()),
            other => panic!("unsupported selected field: {other}"),
        };
        out.insert(field.clone(), value);
    }
    Value::Object(out)
}

fn run_case(
    dispatch: &sg_service_frontend::search_contexts_schema::SearchContextsResolverDispatch<'_>,
    case: &Case,
) -> Result<Value, Error> {
    let ctx = request_context(case.actor_uid);
    if case.kind == "node" {
        let id = marshal_search_context_id(&case.id);
        let resolver = dispatch
            .node_by_id(&ctx, SEARCH_CONTEXT_NODE_KIND, &id)?
            .ok_or_else(|| Error::other("SearchContext node resolver not registered"))?;
        return Ok(context_value(&resolver, &case.select));
    }
    if case.kind == "mutation" {
        let args = SearchContextsFieldArgs {
            search_context_id: Some(marshal_search_context_id(&case.search_context_id)),
            user_id: Some(marshal_user_id(case.user_id)),
            ..SearchContextsFieldArgs::default()
        };
        let value = dispatch
            .field(SearchContextsOperation::Mutation, &case.field, &args, &ctx)?
            .ok_or_else(|| Error::other("field not registered"))?;
        return Ok(match value {
            SearchContextsFieldValue::EmptyResponse => {
                let mut out = Map::new();
                for field in &case.select {
                    out.insert(field.clone(), Value::Null);
                }
                Value::Object(out)
            }
            _ => panic!("unexpected mutation value"),
        });
    }
    let args = SearchContextsFieldArgs {
        spec: Some(case.spec.clone()),
        ..SearchContextsFieldArgs::default()
    };
    let value = dispatch
        .field(SearchContextsOperation::Query, &case.field, &args, &ctx)?
        .ok_or_else(|| Error::other("field not registered"))?;
    Ok(match value {
        SearchContextsFieldValue::SearchContext(resolver) => context_value(&resolver, &case.select),
        SearchContextsFieldValue::Bool(value) => json!(value),
        _ => panic!("unexpected query value"),
    })
}

// ---------------------------------------------------------------------------
// Test
// ---------------------------------------------------------------------------

#[test]
fn search_contexts_schema_harness() {
    if std::env::var("SG_M9HU_DIFF").as_deref() != Ok("1") {
        return;
    }
    let corpus_path = std::env::var("M9HU_CORPUS").expect("M9HU_CORPUS");
    let out_path = std::env::var("M9HU_OUT").expect("M9HU_OUT");
    let corpus: Corpus =
        serde_json::from_slice(&std::fs::read(corpus_path).expect("corpus")).expect("corpus json");

    let base = assemble_oss().expect("oss schema");
    let db = FakeDb::new(corpus.world);
    let enabled =
        register_search_contexts_schema(&base, Some(&db as &dyn SearchContextsGraphqlDatabase))
            .expect("enabled schema");
    let disabled = register_search_contexts_schema(&base, None).expect("disabled schema");
    let dispatch = enabled.dispatch.as_ref().expect("dispatch");

    let mut output = Map::new();
    output.insert(
        "schema".to_owned(),
        json!({
            "enabled": schema_inventory(&enabled.schema),
            "disabled": disabled_inventory(&disabled.schema),
        }),
    );
    output.insert("registration".to_owned(), registration());

    let calls: Vec<Value> = corpus
        .cases
        .iter()
        .map(|case| {
            let mut entry = Map::new();
            entry.insert("name".to_owned(), json!(case.name));
            match run_case(dispatch, case) {
                Ok(value) => {
                    entry.insert("ok".to_owned(), json!(true));
                    entry.insert("value".to_owned(), value);
                }
                Err(err) => {
                    entry.insert("ok".to_owned(), json!(false));
                    entry.insert("error".to_owned(), json!(err.to_string()));
                }
            }
            Value::Object(entry)
        })
        .collect();
    output.insert("resolver_calls".to_owned(), Value::Array(calls));

    std::fs::write(out_path, marshal_go(&Value::Object(output))).expect("write output");
}

#[test]
fn registration_behaviour_is_read_or_write() {
    for handler in SEARCH_CONTEXTS_FIELD_HANDLERS {
        let expected = match handler.operation {
            SearchContextsOperation::Query => SearchContextsFieldBehavior::Read,
            SearchContextsOperation::Mutation => SearchContextsFieldBehavior::Write,
        };
        assert_eq!(handler.behavior, expected);
    }
}
