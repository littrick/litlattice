//! M9.8hg differential of `internal/search/searchcontexts`
//! (`search_contexts.go` + `conf.go`) against
//! `internal/search/searchcontexts/zz_m9hg_oracle_test.go`.
//!
//! Both sides read the same corpus, drive the same in-memory "world" (a small
//! fake database) and emit one canonical JSON object; the run script compares
//! the bytes produced by [`marshal_go`], which matches Go `encoding/json`.
//!
//! Env:
//!
//! * `SG_M9HG_DIFF=1`
//! * `M9HG_CORPUS` — the corpus JSON
//! * `M9HG_OUT` — output file

#![allow(clippy::unwrap_used)]

use std::cell::RefCell;
use std::collections::BTreeMap;

use serde::Deserialize;
use serde_json::{Map, Value};

use sg_service_frontend::search_contexts::{
    create_search_context_with_repository_revisions, delete_search_context,
    get_global_search_context, get_search_context_spec, is_enabled, is_global_search_context,
    is_global_search_context_spec, parse_repo_opts, parse_search_context_spec,
    resolve_search_context_spec, update_search_context_with_repository_revisions,
    validate_search_context_description, validate_search_context_name,
    validate_search_context_query, validate_search_context_repository_revisions,
    validate_search_context_write_access_for_current_user, Error, GetSearchContextOptions,
    MinimalRepo, Namespace, OrgMembership, ParsedSearchContextSpec, RepoOpts, ReposListOptions,
    RequestContext, SearchContext, SearchContextRepositoryRevisions, SearchContextsDatabase, User,
};
use sg_service_frontend::settings_merge::marshal_go;

#[derive(Debug, Clone, Deserialize, Default)]
struct NamespaceDef {
    #[serde(default)]
    name: String,
    #[serde(default)]
    user: i32,
    #[serde(default)]
    organization: i32,
}

#[derive(Debug, Clone, Deserialize, Default)]
struct UserDef {
    #[serde(default)]
    id: i32,
    #[serde(default)]
    site_admin: bool,
}

#[derive(Debug, Clone, Deserialize, Default)]
struct OrgMemberDef {
    #[serde(default)]
    org_id: i32,
    #[serde(default)]
    user_id: i32,
}

#[derive(Debug, Clone, Deserialize, Default)]
struct RepoDef {
    #[serde(default)]
    id: i32,
    #[serde(default)]
    name: String,
}

#[derive(Debug, Clone, Deserialize, Default)]
struct RepoRevisionsDef {
    #[serde(default)]
    repo: RepoDef,
    #[serde(default)]
    revisions: Vec<String>,
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
}

impl ContextDef {
    fn to_context(&self) -> SearchContext {
        SearchContext {
            id: self.id,
            name: self.name.clone(),
            description: self.description.clone(),
            public: self.public,
            namespace_user_id: self.namespace_user_id,
            namespace_org_id: self.namespace_org_id,
            namespace_user_name: self.namespace_user_name.clone(),
            namespace_org_name: self.namespace_org_name.clone(),
            query: self.query.clone(),
            auto_defined: self.auto_defined,
        }
    }
}

#[derive(Debug, Clone, Deserialize, Default)]
struct World {
    #[serde(default)]
    namespaces: Vec<NamespaceDef>,
    #[serde(default)]
    users: Vec<UserDef>,
    #[serde(default)]
    org_members: Vec<OrgMemberDef>,
    #[serde(default)]
    search_contexts: Vec<ContextDef>,
    #[serde(default)]
    revisions: Vec<RepoRevisionsDef>,
    #[serde(default)]
    queries: Vec<String>,
}

struct FakeDb {
    world: RefCell<World>,
}

impl FakeDb {
    fn new(world: World) -> FakeDb {
        FakeDb {
            world: RefCell::new(world),
        }
    }
}

impl SearchContextsDatabase for FakeDb {
    fn get_namespace_by_name(&self, name: &str) -> Result<Namespace, Error> {
        let world = self.world.borrow();
        for namespace in &world.namespaces {
            if namespace.name == name {
                return Ok(Namespace {
                    user: namespace.user,
                    organization: namespace.organization,
                });
            }
        }
        Err(Error::namespace_not_found())
    }

    fn get_org_member_by_org_id_and_user_id(
        &self,
        org_id: i32,
        user_id: i32,
    ) -> Result<Option<OrgMembership>, Error> {
        let world = self.world.borrow();
        for member in &world.org_members {
            if member.org_id == org_id && member.user_id == user_id {
                return Ok(Some(OrgMembership { org_id, user_id }));
            }
        }
        Err(Error::org_member_not_found())
    }

    fn get_current_user(&self, uid: i32) -> Result<Option<User>, Error> {
        if uid == 0 {
            return Ok(None);
        }
        let world = self.world.borrow();
        Ok(world
            .users
            .iter()
            .find(|user| user.id == uid)
            .map(|user| User {
                id: user.id,
                site_admin: user.site_admin,
            }))
    }

    fn get_search_context(&self, opts: &GetSearchContextOptions) -> Result<SearchContext, Error> {
        let world = self.world.borrow();
        for context in &world.search_contexts {
            if context.name == opts.name
                && context.namespace_user_id == opts.namespace_user_id
                && context.namespace_org_id == opts.namespace_org_id
            {
                return Ok(context.to_context());
            }
        }
        Err(Error::search_context_not_found())
    }

    fn create_search_context_with_repository_revisions(
        &self,
        search_context: &SearchContext,
        _repository_revisions: &[SearchContextRepositoryRevisions],
    ) -> Result<SearchContext, Error> {
        let mut world = self.world.borrow_mut();
        let mut next = 1_i64;
        for context in &world.search_contexts {
            if context.id >= next {
                next = context.id + 1;
            }
        }
        let mut created = search_context.clone();
        created.id = next;
        world.search_contexts.push(context_def(&created));
        Ok(created)
    }

    fn update_search_context_with_repository_revisions(
        &self,
        search_context: &SearchContext,
        _repository_revisions: &[SearchContextRepositoryRevisions],
    ) -> Result<SearchContext, Error> {
        let mut world = self.world.borrow_mut();
        let updated = context_def(search_context);
        for context in &mut world.search_contexts {
            if context.id == updated.id {
                *context = updated.clone();
            }
        }
        Ok(search_context.clone())
    }

    fn delete_search_context(&self, search_context_id: i64) -> Result<(), Error> {
        let mut world = self.world.borrow_mut();
        world
            .search_contexts
            .retain(|context| context.id != search_context_id);
        Ok(())
    }

    fn get_all_revisions_for_repos(
        &self,
        repo_ids: &[i32],
    ) -> Result<BTreeMap<i32, Vec<String>>, Error> {
        let world = self.world.borrow();
        let mut revs = BTreeMap::new();
        for entry in &world.revisions {
            if repo_ids.contains(&entry.repo.id) {
                revs.insert(entry.repo.id, entry.revisions.clone());
            }
        }
        Ok(revs)
    }

    fn get_all_queries(&self) -> Result<Vec<String>, Error> {
        Ok(self.world.borrow().queries.clone())
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

fn context_def(context: &SearchContext) -> ContextDef {
    ContextDef {
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

fn context_value(context: &SearchContext) -> Value {
    let mut map = Map::new();
    map.insert("id".to_owned(), Value::from(context.id));
    map.insert("name".to_owned(), Value::from(context.name.clone()));
    map.insert(
        "description".to_owned(),
        Value::from(context.description.clone()),
    );
    map.insert("public".to_owned(), Value::from(context.public));
    map.insert(
        "namespace_user_id".to_owned(),
        Value::from(context.namespace_user_id),
    );
    map.insert(
        "namespace_org_id".to_owned(),
        Value::from(context.namespace_org_id),
    );
    map.insert(
        "namespace_user_name".to_owned(),
        Value::from(context.namespace_user_name.clone()),
    );
    map.insert(
        "namespace_org_name".to_owned(),
        Value::from(context.namespace_org_name.clone()),
    );
    map.insert("query".to_owned(), Value::from(context.query.clone()));
    map.insert("auto_defined".to_owned(), Value::from(context.auto_defined));
    Value::Object(map)
}

fn optional_context_value(context: Option<&SearchContext>) -> Value {
    context.map_or(Value::Null, context_value)
}

fn error_string(error: Option<Error>) -> Value {
    Value::from(error.map_or_else(String::new, |error| error.to_string()))
}

fn repo_revisions(entries: &[RepoRevisionsDef]) -> Vec<SearchContextRepositoryRevisions> {
    entries
        .iter()
        .map(|entry| SearchContextRepositoryRevisions {
            repo: MinimalRepo {
                id: entry.repo.id,
                name: entry.repo.name.clone(),
            },
            revisions: entry.revisions.clone(),
        })
        .collect()
}

fn repo_opts_value(opts: &[RepoOpts]) -> Value {
    let values: Vec<Value> = opts
        .iter()
        .map(|opt| {
            let mut map = Map::new();
            map.insert(
                "include_patterns".to_owned(),
                Value::from(opt.repos_list_options.include_patterns.clone()),
            );
            map.insert(
                "exclude_pattern".to_owned(),
                Value::from(opt.repos_list_options.exclude_pattern.clone()),
            );
            map.insert(
                "case_sensitive_patterns".to_owned(),
                Value::from(opt.repos_list_options.case_sensitive_patterns),
            );
            map.insert(
                "only_forks".to_owned(),
                Value::from(opt.repos_list_options.only_forks),
            );
            map.insert(
                "no_forks".to_owned(),
                Value::from(opt.repos_list_options.no_forks),
            );
            map.insert(
                "only_archived".to_owned(),
                Value::from(opt.repos_list_options.only_archived),
            );
            map.insert(
                "no_archived".to_owned(),
                Value::from(opt.repos_list_options.no_archived),
            );
            map.insert(
                "no_private".to_owned(),
                Value::from(opt.repos_list_options.no_private),
            );
            map.insert(
                "only_private".to_owned(),
                Value::from(opt.repos_list_options.only_private),
            );
            map.insert("rev_specs".to_owned(), Value::from(opt.rev_specs.clone()));
            Value::Object(map)
        })
        .collect();
    Value::Array(values)
}

#[derive(Debug, Deserialize)]
struct ResolveCase {
    #[serde(default)]
    spec: String,
    #[serde(default)]
    dotcom: bool,
    #[serde(default)]
    uid: i32,
    #[serde(default)]
    world: String,
}

#[derive(Debug, Deserialize)]
struct WriteAccessCase {
    #[serde(default)]
    namespace_user_id: i32,
    #[serde(default)]
    namespace_org_id: i32,
    #[serde(default)]
    public: bool,
    #[serde(default)]
    uid: i32,
    #[serde(default)]
    world: String,
}

#[derive(Debug, Deserialize)]
struct MutationCase {
    #[serde(default)]
    search_context: ContextDef,
    #[serde(default)]
    revisions: Vec<RepoRevisionsDef>,
    #[serde(default)]
    uid: i32,
    #[serde(default)]
    world: String,
}

#[derive(Debug, Deserialize)]
struct IsEnabledCase {
    #[serde(default)]
    env: Option<String>,
}

#[derive(Debug, Deserialize, Default)]
struct Corpus {
    #[serde(default)]
    worlds: BTreeMap<String, World>,
    #[serde(default)]
    parse_spec: Vec<String>,
    #[serde(default)]
    get_spec: Vec<ContextDef>,
    #[serde(default)]
    is_global_spec: Vec<String>,
    #[serde(default)]
    is_global_context: Vec<ContextDef>,
    #[serde(default)]
    validate_name: Vec<String>,
    #[serde(default)]
    validate_description: Vec<String>,
    #[serde(default)]
    validate_revisions: Vec<Vec<RepoRevisionsDef>>,
    #[serde(default)]
    validate_query: Vec<String>,
    #[serde(default)]
    parse_repo_opts: Vec<String>,
    #[serde(default)]
    resolve: Vec<ResolveCase>,
    #[serde(default)]
    write_access: Vec<WriteAccessCase>,
    #[serde(default)]
    create: Vec<MutationCase>,
    #[serde(default)]
    update: Vec<MutationCase>,
    #[serde(default)]
    delete: Vec<MutationCase>,
    #[serde(default)]
    is_enabled: Vec<IsEnabledCase>,
}

impl Corpus {
    fn world(&self, name: &str) -> FakeDb {
        FakeDb::new(self.worlds.get(name).cloned().unwrap_or_default())
    }
}

fn mutation_results(cases: &[MutationCase], corpus: &Corpus, is_update: bool) -> Vec<Value> {
    cases
        .iter()
        .map(|case| {
            let db = corpus.world(&case.world);
            let ctx = RequestContext {
                actor_uid: case.uid,
                internal: false,
                sourcegraph_dot_com_mode: false,
            };
            let search_context = case.search_context.to_context();
            let revisions = repo_revisions(&case.revisions);
            let result = if is_update {
                update_search_context_with_repository_revisions(
                    &db,
                    &ctx,
                    &search_context,
                    &revisions,
                )
            } else {
                create_search_context_with_repository_revisions(
                    &db,
                    &ctx,
                    &search_context,
                    &revisions,
                )
            };
            let (context, error) = match result {
                Ok(context) => (Some(context), None),
                Err(error) => (None, Some(error)),
            };
            let mut map = Map::new();
            map.insert(
                "context".to_owned(),
                optional_context_value(context.as_ref()),
            );
            map.insert("error".to_owned(), error_string(error));
            Value::Object(map)
        })
        .collect()
}

#[test]
fn search_contexts_harness() {
    if std::env::var("SG_M9HG_DIFF").as_deref() != Ok("1") {
        return;
    }
    let corpus_path = std::env::var("M9HG_CORPUS").expect("M9HG_CORPUS");
    let out_path = std::env::var("M9HG_OUT").expect("M9HG_OUT");
    let corpus: Corpus =
        serde_json::from_slice(&std::fs::read(corpus_path).expect("corpus")).expect("corpus json");

    let mut out = Map::new();

    let parse_spec: Vec<Value> = corpus
        .parse_spec
        .iter()
        .map(|spec| {
            let parsed: ParsedSearchContextSpec = parse_search_context_spec(spec);
            let mut map = Map::new();
            map.insert(
                "namespace_name".to_owned(),
                Value::from(parsed.namespace_name),
            );
            map.insert(
                "search_context_name".to_owned(),
                Value::from(parsed.search_context_name),
            );
            Value::Object(map)
        })
        .collect();
    out.insert("parse_spec".to_owned(), Value::Array(parse_spec));

    let get_spec: Vec<Value> = corpus
        .get_spec
        .iter()
        .map(|context| Value::from(get_search_context_spec(&context.to_context())))
        .collect();
    out.insert("get_spec".to_owned(), Value::Array(get_spec));

    let is_global_spec: Vec<Value> = corpus
        .is_global_spec
        .iter()
        .map(|spec| Value::from(is_global_search_context_spec(spec)))
        .collect();
    out.insert("is_global_spec".to_owned(), Value::Array(is_global_spec));

    let is_global_context: Vec<Value> = corpus
        .is_global_context
        .iter()
        .map(|context| Value::from(is_global_search_context(&context.to_context())))
        .collect();
    out.insert(
        "is_global_context".to_owned(),
        Value::Array(is_global_context),
    );

    out.insert(
        "get_global".to_owned(),
        Value::Array(vec![context_value(&get_global_search_context())]),
    );

    let validate_name: Vec<Value> = corpus
        .validate_name
        .iter()
        .map(|name| error_string(validate_search_context_name(name).err()))
        .collect();
    out.insert("validate_name".to_owned(), Value::Array(validate_name));

    let validate_description: Vec<Value> = corpus
        .validate_description
        .iter()
        .map(|description| error_string(validate_search_context_description(description).err()))
        .collect();
    out.insert(
        "validate_description".to_owned(),
        Value::Array(validate_description),
    );

    let validate_revisions: Vec<Value> = corpus
        .validate_revisions
        .iter()
        .map(|entries| {
            error_string(
                validate_search_context_repository_revisions(&repo_revisions(entries)).err(),
            )
        })
        .collect();
    out.insert(
        "validate_revisions".to_owned(),
        Value::Array(validate_revisions),
    );

    let validate_query: Vec<Value> = corpus
        .validate_query
        .iter()
        .map(|query| error_string(validate_search_context_query(query).err()))
        .collect();
    out.insert("validate_query".to_owned(), Value::Array(validate_query));

    let parse_repo_opts_results: Vec<Value> = corpus
        .parse_repo_opts
        .iter()
        .map(|query| {
            let (opts, error) = match parse_repo_opts(query) {
                Ok(opts) => (opts, None),
                Err(error) => (Vec::new(), Some(error)),
            };
            let mut map = Map::new();
            map.insert("opts".to_owned(), repo_opts_value(&opts));
            map.insert("error".to_owned(), error_string(error));
            Value::Object(map)
        })
        .collect();
    out.insert(
        "parse_repo_opts".to_owned(),
        Value::Array(parse_repo_opts_results),
    );

    let resolve: Vec<Value> = corpus
        .resolve
        .iter()
        .map(|case| {
            let db = corpus.world(&case.world);
            let ctx = RequestContext {
                actor_uid: case.uid,
                internal: false,
                sourcegraph_dot_com_mode: case.dotcom,
            };
            let (context, error) = match resolve_search_context_spec(&db, &ctx, &case.spec) {
                Ok(context) => (Some(context), None),
                Err(error) => (None, Some(error)),
            };
            let mut map = Map::new();
            map.insert(
                "context".to_owned(),
                optional_context_value(context.as_ref()),
            );
            map.insert("error".to_owned(), error_string(error));
            Value::Object(map)
        })
        .collect();
    out.insert("resolve".to_owned(), Value::Array(resolve));

    let write_access: Vec<Value> = corpus
        .write_access
        .iter()
        .map(|case| {
            let db = corpus.world(&case.world);
            let ctx = RequestContext {
                actor_uid: case.uid,
                internal: false,
                sourcegraph_dot_com_mode: false,
            };
            error_string(
                validate_search_context_write_access_for_current_user(
                    &db,
                    &ctx,
                    case.namespace_user_id,
                    case.namespace_org_id,
                    case.public,
                )
                .err(),
            )
        })
        .collect();
    out.insert("write_access".to_owned(), Value::Array(write_access));

    out.insert(
        "create".to_owned(),
        Value::Array(mutation_results(&corpus.create, &corpus, false)),
    );
    out.insert(
        "update".to_owned(),
        Value::Array(mutation_results(&corpus.update, &corpus, true)),
    );

    let delete: Vec<Value> = corpus
        .delete
        .iter()
        .map(|case| {
            let db = corpus.world(&case.world);
            let ctx = RequestContext {
                actor_uid: case.uid,
                internal: false,
                sourcegraph_dot_com_mode: false,
            };
            error_string(delete_search_context(&db, &ctx, &case.search_context.to_context()).err())
        })
        .collect();
    out.insert("delete".to_owned(), Value::Array(delete));

    let original = std::env::var_os("DISABLE_SEARCH_CONTEXTS");
    let is_enabled_results: Vec<Value> = corpus
        .is_enabled
        .iter()
        .map(|case| {
            match &case.env {
                Some(value) => std::env::set_var("DISABLE_SEARCH_CONTEXTS", value),
                None => std::env::remove_var("DISABLE_SEARCH_CONTEXTS"),
            }
            Value::from(is_enabled())
        })
        .collect();
    match original {
        Some(value) => std::env::set_var("DISABLE_SEARCH_CONTEXTS", value),
        None => std::env::remove_var("DISABLE_SEARCH_CONTEXTS"),
    }
    out.insert("is_enabled".to_owned(), Value::Array(is_enabled_results));

    let encoded = marshal_go(&Value::Object(out));
    std::fs::write(&out_path, &encoded).expect("write output");
    println!("RUST search-contexts len={}", encoded.len());
}
