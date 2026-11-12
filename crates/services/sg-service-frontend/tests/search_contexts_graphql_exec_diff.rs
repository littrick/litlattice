#![allow(clippy::unwrap_used, clippy::too_many_arguments)]

//! M9.8hz differential of the search contexts GraphQL executor wiring
//! (`graphqlbackend.go` schema assembly, `search_contexts.go:20-39` root
//! binding, `node.go` Node dispatch, `internal/searchcontexts/init.go:26`).
//!
//! Go oracle: `cmd/frontend/graphqlbackend/zz_m9hz_oracle_test.go`.
//! The Rust side drives the real `SchemaGraphqlDeps::exec` (full OSS schema +
//! the M9.8hu/M9.8hq resolver stack); the Go side drives graph-gophers with a
//! minimal execution SDL and an in-memory world with the same semantics.
//!
//! Env:
//!
//! * `SG_M9HZ_DIFF=1`
//! * `M9HZ_CORPUS` — the corpus JSON
//! * `M9HZ_OUT` — output file

use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use serde::Deserialize;
use serde_json::{Map, Value};
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;

use sg_graphql::exec::Actor;
use sg_graphql::schema::assemble_oss;
use sg_service_frontend::graphql_api::{
    GraphqlDeps, GraphqlExecRequest, GraphqlResponse, SchemaGraphqlDeps,
};
use sg_service_frontend::graphql_resolvers::SearchContextsDatabaseHandle;
use sg_service_frontend::search_contexts::{
    Error, GetSearchContextOptions, MinimalRepo, Namespace, OrgMembership, ReposListOptions,
    RequestContext, SearchContext, SearchContextRepositoryRevisions, SearchContextsDatabase, User,
};
use sg_service_frontend::search_contexts_graphql::{
    ListSearchContextsOptions, ListSearchContextsPageOptions, NamespaceOrg, NamespaceUser,
    SearchContextRecord, SearchContextsGraphqlDatabase, SearchContextsOrderBy,
};

const CREATE_TIME: &str = "2024-06-01T00:00:00Z";
const UPDATE_TIME: &str = "2024-06-02T00:00:00Z";

fn parse_time(value: &str) -> OffsetDateTime {
    if value.is_empty() {
        return OffsetDateTime::UNIX_EPOCH;
    }
    OffsetDateTime::parse(value, &Rfc3339).expect("corpus timestamp")
}

// ---------------------------------------------------------------------------
// Corpus
// ---------------------------------------------------------------------------

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
    username: String,
    #[serde(default)]
    site_admin: bool,
}

#[derive(Debug, Clone, Deserialize, Default)]
struct OrgDef {
    #[serde(default)]
    id: i32,
    #[serde(default)]
    name: String,
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
struct StarDef {
    #[serde(default)]
    user_id: i32,
    #[serde(default)]
    search_context_id: i64,
}

#[derive(Debug, Clone, Deserialize, Default)]
struct RepoRevisionDef {
    #[serde(default)]
    search_context_id: i64,
    #[serde(default)]
    repo: RepoDef,
    #[serde(default)]
    revisions: Vec<String>,
}

#[derive(Debug, Clone, Deserialize, Default)]
struct World {
    #[serde(default)]
    namespaces: Vec<NamespaceDef>,
    #[serde(default)]
    users: Vec<UserDef>,
    #[serde(default)]
    orgs: Vec<OrgDef>,
    #[serde(default)]
    org_members: Vec<OrgMemberDef>,
    #[serde(default)]
    repositories: Vec<RepoDef>,
    #[serde(default)]
    search_contexts: Vec<ContextDef>,
    #[serde(default)]
    defaults: Vec<DefaultDef>,
    #[serde(default)]
    stars: Vec<StarDef>,
    #[serde(default)]
    repo_revisions: Vec<RepoRevisionDef>,
}

// ---------------------------------------------------------------------------
// Fake database
// ---------------------------------------------------------------------------

struct FakeDb {
    world: Mutex<World>,
}

impl FakeDb {
    fn new(world: World) -> Self {
        Self {
            world: Mutex::new(world),
        }
    }

    fn context_def_from_base(base: &SearchContext, updated_at: &str) -> ContextDef {
        ContextDef {
            id: base.id,
            name: base.name.clone(),
            description: base.description.clone(),
            public: base.public,
            namespace_user_id: base.namespace_user_id,
            namespace_org_id: base.namespace_org_id,
            namespace_user_name: base.namespace_user_name.clone(),
            namespace_org_name: base.namespace_org_name.clone(),
            query: base.query.clone(),
            auto_defined: base.auto_defined,
            updated_at: updated_at.to_owned(),
        }
    }

    fn namespace_names(world: &World, user_id: i32, org_id: i32) -> (String, String) {
        let user_name = world
            .users
            .iter()
            .find(|u| u.id == user_id)
            .map(|u| u.username.clone())
            .unwrap_or_default();
        let org_name = world
            .orgs
            .iter()
            .find(|o| o.id == org_id)
            .map(|o| o.name.clone())
            .unwrap_or_default();
        (user_name, org_name)
    }

    fn filtered(&self, uid: i32, opts: &ListSearchContextsOptions) -> Vec<SearchContextRecord> {
        let world = self.world.lock().expect("world lock");
        let mut records = Vec::with_capacity(world.search_contexts.len() + 1);
        records.push(self.global_record_with(&world, uid));
        for context in &world.search_contexts {
            records.push(self.full_record_with(&world, context, uid));
        }

        let has_namespace_filter = opts.no_namespace
            || !opts.namespace_user_ids.is_empty()
            || !opts.namespace_org_ids.is_empty();
        records.retain(|context| {
            if has_namespace_filter {
                let no_namespace = context.namespace_user_id == 0 && context.namespace_org_id == 0;
                let matches_user = opts.namespace_user_ids.contains(&context.namespace_user_id);
                let matches_org = opts.namespace_org_ids.contains(&context.namespace_org_id);
                if !((opts.no_namespace && no_namespace) || matches_user || matches_org) {
                    return false;
                }
            }
            if !opts.name.is_empty()
                && !context
                    .name
                    .to_lowercase()
                    .contains(&opts.name.to_lowercase())
            {
                return false;
            }
            if !opts.namespace_name.is_empty() {
                let namespace_name = if !context.namespace_user_name.is_empty() {
                    &context.namespace_user_name
                } else {
                    &context.namespace_org_name
                };
                if !namespace_name
                    .to_lowercase()
                    .contains(&opts.namespace_name.to_lowercase())
                {
                    return false;
                }
            }
            true
        });

        records.sort_by(|a, b| compare_records(a, b, opts));
        records
    }

    fn global_record_with(&self, world: &World, uid: i32) -> SearchContextRecord {
        let has_default = world.defaults.iter().any(|d| d.user_id == uid);
        SearchContextRecord {
            id: 0,
            name: "global".to_owned(),
            description: "All repositories on Sourcegraph".to_owned(),
            public: true,
            auto_defined: true,
            updated_at: OffsetDateTime::UNIX_EPOCH,
            default: !has_default,
            ..SearchContextRecord::default()
        }
    }

    fn full_record_with(
        &self,
        world: &World,
        context: &ContextDef,
        uid: i32,
    ) -> SearchContextRecord {
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
            default: world
                .defaults
                .iter()
                .any(|d| d.user_id == uid && d.search_context_id == context.id),
            starred: world
                .stars
                .iter()
                .any(|s| s.user_id == uid && s.search_context_id == context.id),
        }
    }
}

fn namespace_name(context: &SearchContextRecord) -> Option<&str> {
    if !context.namespace_user_name.is_empty() {
        Some(&context.namespace_user_name)
    } else if !context.namespace_org_name.is_empty() {
        Some(&context.namespace_org_name)
    } else {
        None
    }
}

/// PostgreSQL `ASC NULLS LAST` / `DESC NULLS FIRST` for an optional string.
fn compare_optional_name(a: Option<&str>, b: Option<&str>, descending: bool) -> Ordering {
    match (a, b) {
        (None, None) => Ordering::Equal,
        (None, Some(_)) => {
            if descending {
                Ordering::Less
            } else {
                Ordering::Greater
            }
        }
        (Some(_), None) => {
            if descending {
                Ordering::Greater
            } else {
                Ordering::Less
            }
        }
        (Some(a), Some(b)) => {
            let ordering = a.cmp(b);
            if descending {
                ordering.reverse()
            } else {
                ordering
            }
        }
    }
}

fn compare_text(a: &str, b: &str, descending: bool) -> Ordering {
    let ordering = a.cmp(b);
    if descending {
        ordering.reverse()
    } else {
        ordering
    }
}

fn compare_records(
    a: &SearchContextRecord,
    b: &SearchContextRecord,
    opts: &ListSearchContextsOptions,
) -> Ordering {
    // `autodefined DESC, user_default DESC, user_starred DESC`.
    let mut ordering = b
        .auto_defined
        .cmp(&a.auto_defined)
        .then_with(|| b.default.cmp(&a.default))
        .then_with(|| b.starred.cmp(&a.starred));
    if ordering == Ordering::Equal {
        ordering = match opts.order_by {
            SearchContextsOrderBy::Spec => compare_optional_name(
                namespace_name(a),
                namespace_name(b),
                opts.order_by_descending,
            )
            .then_with(|| compare_text(&a.name, &b.name, opts.order_by_descending)),
            SearchContextsOrderBy::UpdatedAt => {
                let ordering = a.updated_at.cmp(&b.updated_at);
                if opts.order_by_descending {
                    ordering.reverse()
                } else {
                    ordering
                }
            }
        };
    }
    if ordering == Ordering::Equal {
        // Deterministic tie-break; the baseline SQL leaves ties unspecified.
        ordering = a.id.cmp(&b.id);
    }
    ordering
}

impl SearchContextsDatabase for FakeDb {
    fn get_namespace_by_name(&self, name: &str) -> Result<Namespace, Error> {
        let world = self.world.lock().expect("world lock");
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
        let world = self.world.lock().expect("world lock");
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
        let world = self.world.lock().expect("world lock");
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
        let world = self.world.lock().expect("world lock");
        for context in &world.search_contexts {
            if context.name == opts.name
                && context.namespace_user_id == opts.namespace_user_id
                && context.namespace_org_id == opts.namespace_org_id
            {
                return Ok(SearchContext {
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
                });
            }
        }
        Err(Error::search_context_not_found())
    }

    fn create_search_context_with_repository_revisions(
        &self,
        search_context: &SearchContext,
        repository_revisions: &[SearchContextRepositoryRevisions],
    ) -> Result<SearchContext, Error> {
        let mut world = self.world.lock().expect("world lock");
        let mut next = 1_i64;
        for context in &world.search_contexts {
            if context.id >= next {
                next = context.id + 1;
            }
        }
        let (user_name, org_name) = FakeDb::namespace_names(
            &world,
            search_context.namespace_user_id,
            search_context.namespace_org_id,
        );
        let mut created = search_context.clone();
        created.id = next;
        created.namespace_user_name = user_name;
        created.namespace_org_name = org_name;
        world
            .search_contexts
            .push(FakeDb::context_def_from_base(&created, CREATE_TIME));
        world
            .repo_revisions
            .retain(|entry| entry.search_context_id != next);
        for entry in repository_revisions {
            world.repo_revisions.push(RepoRevisionDef {
                search_context_id: next,
                repo: RepoDef {
                    id: entry.repo.id,
                    name: entry.repo.name.clone(),
                },
                revisions: entry.revisions.clone(),
            });
        }
        Ok(created)
    }

    fn update_search_context_with_repository_revisions(
        &self,
        search_context: &SearchContext,
        repository_revisions: &[SearchContextRepositoryRevisions],
    ) -> Result<SearchContext, Error> {
        let mut world = self.world.lock().expect("world lock");
        let (user_name, org_name) = FakeDb::namespace_names(
            &world,
            search_context.namespace_user_id,
            search_context.namespace_org_id,
        );
        let mut updated = search_context.clone();
        updated.namespace_user_name = user_name;
        updated.namespace_org_name = org_name;
        for context in &mut world.search_contexts {
            if context.id == updated.id {
                *context = FakeDb::context_def_from_base(&updated, UPDATE_TIME);
            }
        }
        world
            .repo_revisions
            .retain(|entry| entry.search_context_id != updated.id);
        for entry in repository_revisions {
            world.repo_revisions.push(RepoRevisionDef {
                search_context_id: updated.id,
                repo: RepoDef {
                    id: entry.repo.id,
                    name: entry.repo.name.clone(),
                },
                revisions: entry.revisions.clone(),
            });
        }
        Ok(updated)
    }

    fn delete_search_context(&self, search_context_id: i64) -> Result<(), Error> {
        let mut world = self.world.lock().expect("world lock");
        world
            .search_contexts
            .retain(|context| context.id != search_context_id);
        world
            .repo_revisions
            .retain(|entry| entry.search_context_id != search_context_id);
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
        search_context_id: i64,
    ) -> Result<Vec<SearchContextRepositoryRevisions>, Error> {
        let world = self.world.lock().expect("world lock");
        let mut entries: Vec<SearchContextRepositoryRevisions> = world
            .repo_revisions
            .iter()
            .filter(|entry| entry.search_context_id == search_context_id)
            .map(|entry| {
                let mut revisions = entry.revisions.clone();
                revisions.sort();
                SearchContextRepositoryRevisions {
                    repo: MinimalRepo {
                        id: entry.repo.id,
                        name: entry.repo.name.clone(),
                    },
                    revisions,
                }
            })
            .collect();
        entries.sort_by_key(|entry| entry.repo.id);
        Ok(entries)
    }

    fn list_minimal_repos(&self, _opts: &ReposListOptions) -> Result<Vec<MinimalRepo>, Error> {
        Ok(Vec::new())
    }

    fn create_search_context_star_for_user(
        &self,
        user_id: i32,
        search_context_id: i64,
    ) -> Result<(), Error> {
        let mut world = self.world.lock().expect("world lock");
        if !world
            .stars
            .iter()
            .any(|s| s.user_id == user_id && s.search_context_id == search_context_id)
        {
            world.stars.push(StarDef {
                user_id,
                search_context_id,
            });
        }
        Ok(())
    }

    fn delete_search_context_star_for_user(
        &self,
        user_id: i32,
        search_context_id: i64,
    ) -> Result<(), Error> {
        let mut world = self.world.lock().expect("world lock");
        world
            .stars
            .retain(|s| !(s.user_id == user_id && s.search_context_id == search_context_id));
        Ok(())
    }

    fn set_user_default_search_context_id(
        &self,
        user_id: i32,
        search_context_id: i64,
    ) -> Result<(), Error> {
        let mut world = self.world.lock().expect("world lock");
        world.defaults.retain(|d| d.user_id != user_id);
        if search_context_id != 0 {
            world.defaults.push(DefaultDef {
                user_id,
                search_context_id,
            });
        }
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
        let world = self.world.lock().expect("world lock");
        for context in &world.search_contexts {
            if context.id == id {
                return Ok(self.full_record_with(&world, context, ctx.actor_uid));
            }
        }
        Err(Error::search_context_not_found())
    }

    fn list_search_contexts(
        &self,
        ctx: &RequestContext,
        page: &ListSearchContextsPageOptions,
        opts: &ListSearchContextsOptions,
    ) -> Result<Vec<SearchContextRecord>, Error> {
        let records = self.filtered(ctx.actor_uid, opts);
        let start = if page.after <= 0 {
            0
        } else {
            (page.after as usize).min(records.len())
        };
        let limit = if page.first <= 0 {
            0
        } else {
            page.first as usize
        };
        let end = (start + limit).min(records.len());
        Ok(records[start..end].to_vec())
    }

    fn count_search_contexts(
        &self,
        ctx: &RequestContext,
        opts: &ListSearchContextsOptions,
    ) -> Result<i32, Error> {
        Ok(self.filtered(ctx.actor_uid, opts).len() as i32)
    }

    fn get_default_search_context_for_current_user(
        &self,
        ctx: &RequestContext,
    ) -> Result<SearchContextRecord, Error> {
        let world = self.world.lock().expect("world lock");
        for default in &world.defaults {
            if default.user_id == ctx.actor_uid {
                for context in &world.search_contexts {
                    if context.id == default.search_context_id {
                        return Ok(self.full_record_with(&world, context, ctx.actor_uid));
                    }
                }
            }
        }
        Ok(self.global_record_with(&world, ctx.actor_uid))
    }

    fn get_repos_set_by_ids(&self, ids: &[i32]) -> Result<BTreeMap<i32, MinimalRepoRef>, Error> {
        let world = self.world.lock().expect("world lock");
        let mut out = BTreeMap::new();
        for repository in &world.repositories {
            if ids.contains(&repository.id) {
                out.insert(
                    repository.id,
                    MinimalRepo {
                        id: repository.id,
                        name: repository.name.clone(),
                    },
                );
            }
        }
        Ok(out)
    }

    fn get_user_by_id(&self, id: i32) -> Result<NamespaceUser, Error> {
        let world = self.world.lock().expect("world lock");
        world
            .users
            .iter()
            .find(|user| user.id == id)
            .map(|user| NamespaceUser {
                id: user.id,
                username: user.username.clone(),
            })
            .ok_or_else(|| Error::other("user not found"))
    }

    fn get_org_by_id(&self, id: i32) -> Result<NamespaceOrg, Error> {
        let world = self.world.lock().expect("world lock");
        world
            .orgs
            .iter()
            .find(|org| org.id == id)
            .map(|org| NamespaceOrg {
                id: org.id,
                name: org.name.clone(),
            })
            .ok_or_else(|| Error::other("org not found"))
    }

    fn get_orgs_by_user_id(&self, user_id: i32) -> Result<Vec<NamespaceOrg>, Error> {
        let world = self.world.lock().expect("world lock");
        let mut orgs: Vec<NamespaceOrg> = world
            .org_members
            .iter()
            .filter(|member| member.user_id == user_id)
            .filter_map(|member| {
                world
                    .orgs
                    .iter()
                    .find(|org| org.id == member.org_id)
                    .map(|org| NamespaceOrg {
                        id: org.id,
                        name: org.name.clone(),
                    })
            })
            .collect();
        orgs.sort_by_key(|org| org.id);
        orgs.dedup_by_key(|org| org.id);
        Ok(orgs)
    }
}

// `get_repos_set_by_ids` uses the re-exported alias.
use sg_service_frontend::search_contexts_graphql::MinimalRepoRef;

// ---------------------------------------------------------------------------
// Corpus / harness
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
struct ExecCase {
    name: String,
    world: String,
    #[serde(default)]
    uid: i32,
    #[serde(default)]
    enabled: bool,
    #[serde(default)]
    queries: Vec<String>,
}

#[derive(Debug, Deserialize, Default)]
struct ExecCorpus {
    #[serde(default)]
    worlds: BTreeMap<String, World>,
    #[serde(default)]
    cases: Vec<ExecCase>,
}

/// Expand `{sc:spec}`, `{user:id}`, `{org:id}`, `{repo:id}` and `{cursor:n}`
/// placeholders to Relay IDs / cursors (the corpus stays free of base64
/// literals).
fn expand_placeholders(query: &str) -> String {
    use sg_service_frontend::search_contexts_graphql::{
        marshal_org_id, marshal_repository_id, marshal_search_context_cursor,
        marshal_search_context_id, marshal_user_id,
    };
    let pattern = regex::Regex::new(r"\{(sc|user|org|repo|cursor):([^}]*)\}").expect("regexp");
    pattern
        .replace_all(query, |captures: &regex::Captures<'_>| {
            let value = &captures[2];
            match &captures[1] {
                "sc" => marshal_search_context_id(value),
                "user" => marshal_user_id(value.parse().expect("user id")),
                "org" => marshal_org_id(value.parse().expect("org id")),
                "repo" => marshal_repository_id(value.parse().expect("repo id")),
                "cursor" => marshal_search_context_cursor(value.parse().expect("cursor")),
                _ => unreachable!("regexp group"),
            }
        })
        .into_owned()
}

fn normalized_response(response: &GraphqlResponse) -> Value {
    let data = response
        .data
        .as_ref()
        .map(|data| serde_json::from_str::<Value>(data).expect("data json"))
        .unwrap_or(Value::Null);
    let errors = response
        .errors
        .iter()
        .map(|error| {
            Value::Object(Map::from_iter([
                ("message".to_owned(), Value::from(error.message.clone())),
                (
                    "path".to_owned(),
                    Value::Array(error.path.clone().unwrap_or_default()),
                ),
            ]))
        })
        .collect();
    Value::Object(Map::from_iter([
        ("data".to_owned(), data),
        ("errors".to_owned(), Value::Array(errors)),
    ]))
}

#[tokio::test]
async fn search_contexts_graphql_exec_harness() {
    if std::env::var("SG_M9HZ_DIFF").as_deref() != Ok("1") {
        return;
    }
    let corpus_path = std::env::var("M9HZ_CORPUS").expect("M9HZ_CORPUS");
    let out_path = std::env::var("M9HZ_OUT").expect("M9HZ_OUT");
    let corpus: ExecCorpus =
        serde_json::from_slice(&std::fs::read(corpus_path).expect("corpus")).expect("corpus json");
    let base_schema = Arc::new(assemble_oss().expect("oss schema"));

    let mut cases = Vec::with_capacity(corpus.cases.len());
    for case in &corpus.cases {
        let world = corpus
            .worlds
            .get(&case.world)
            .unwrap_or_else(|| panic!("unknown world {}", case.world))
            .clone();
        let deps = if case.enabled {
            let handle: SearchContextsDatabaseHandle = Arc::new(FakeDb::new(world));
            SchemaGraphqlDeps::new(base_schema.clone())
                .with_search_contexts(handle)
                .expect("search contexts schema")
        } else {
            SchemaGraphqlDeps::new(base_schema.clone())
        };
        let actor = if case.uid == 0 {
            Actor::Anonymous
        } else {
            Actor::User { uid: case.uid }
        };
        let mut responses = Vec::with_capacity(case.queries.len());
        for query in &case.queries {
            let query = expand_placeholders(query);
            let variables = Map::new();
            let response = deps
                .exec(GraphqlExecRequest {
                    query: &query,
                    operation_name: "",
                    variables: &variables,
                    actor,
                    anonymous_uid: "",
                })
                .await;
            responses.push(normalized_response(&response));
        }
        cases.push(Value::Object(Map::from_iter([
            ("name".to_owned(), Value::from(case.name.clone())),
            ("responses".to_owned(), Value::Array(responses)),
        ])));
    }
    let document = Value::Object(Map::from_iter([("cases".to_owned(), Value::Array(cases))]));
    std::fs::write(out_path, serde_json::to_string(&document).expect("json")).expect("write");
}
