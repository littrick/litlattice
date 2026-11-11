//! M9.8hq differential of the search contexts GraphQL resolver layer
//! (`cmd/frontend/internal/searchcontexts/resolvers/resolvers.go`) against
//! `cmd/frontend/internal/searchcontexts/resolvers/zz_m9hq_oracle_test.go`.
//!
//! Both sides read the same corpus, drive the same in-memory "world" (a fake
//! database + fake user/org stores) and emit one canonical JSON object; the run
//! script compares the bytes produced by [`marshal_go`] with Go
//! `encoding/json`.
//!
//! Env:
//!
//! * `SG_M9HQ_DIFF=1`
//! * `M9HQ_CORPUS` — the corpus JSON
//! * `M9HQ_OUT` — output file

#![allow(clippy::unwrap_used)]

use std::cell::RefCell;
use std::cmp::Ordering;
use std::collections::BTreeMap;

use serde::Deserialize;
use serde_json::{Map, Value};
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;

use sg_service_frontend::search_contexts::{
    Error, GetSearchContextOptions, MinimalRepo, Namespace, OrgMembership, ReposListOptions,
    RequestContext, SearchContext, SearchContextRepositoryRevisions, SearchContextsDatabase, User,
};
use sg_service_frontend::search_contexts_graphql::{
    format_gqlutil_datetime, marshal_org_id, marshal_repository_id, marshal_search_context_id,
    marshal_user_id, CreateSearchContextArgs, DeleteSearchContextArgs, ListSearchContextsArgs,
    ListSearchContextsOptions, ListSearchContextsPageOptions, NamespaceOrg, NamespaceResolver,
    NamespaceUser, Resolver, SearchContextEditInputArgs, SearchContextInputArgs,
    SearchContextRecord, SearchContextRepositoryRevisionsInputArgs, SearchContextStarArgs,
    SearchContextsGraphqlDatabase, SearchContextsOrderBy, SetDefaultSearchContextArgs,
    UpdateSearchContextArgs,
};
use sg_service_frontend::settings_merge::marshal_go;

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

#[derive(Debug, Deserialize, Default)]
struct NamespaceInputDef {
    #[serde(default)]
    user_id: Option<i32>,
    #[serde(default)]
    org_id: Option<i32>,
    #[serde(default)]
    raw_id: Option<String>,
}

#[derive(Debug, Deserialize, Default)]
struct RepoInputDef {
    #[serde(default)]
    id: Option<i32>,
    #[serde(default)]
    raw_id: Option<String>,
    #[serde(default)]
    revisions: Vec<String>,
}

#[derive(Debug, Deserialize, Default)]
struct LookupCase {
    #[serde(default)]
    spec: String,
    #[serde(default)]
    raw_id: Option<String>,
    #[serde(default)]
    uid: i32,
    #[serde(default)]
    dotcom: bool,
    #[serde(default)]
    world: String,
}

#[derive(Debug, Deserialize, Default)]
struct DefaultCase {
    #[serde(default)]
    uid: i32,
    #[serde(default)]
    world: String,
}

#[derive(Debug, Deserialize, Default)]
struct MutationCase {
    #[serde(default)]
    id: String,
    #[serde(default)]
    raw_id: Option<String>,
    #[serde(default)]
    search_context: ContextInputDef,
    #[serde(default)]
    repositories: Vec<RepoInputDef>,
    #[serde(default)]
    uid: i32,
    #[serde(default)]
    world: String,
}

#[derive(Debug, Deserialize, Default)]
struct ContextInputDef {
    #[serde(default)]
    name: String,
    #[serde(default)]
    description: String,
    #[serde(default)]
    public: bool,
    #[serde(default)]
    namespace: Option<NamespaceInputDef>,
    #[serde(default)]
    query: String,
}

#[derive(Debug, Deserialize, Default)]
struct StarCase {
    #[serde(default)]
    spec: String,
    #[serde(default)]
    raw_id: Option<String>,
    #[serde(default)]
    user_id: Option<i32>,
    #[serde(default)]
    raw_user_id: Option<String>,
    #[serde(default)]
    uid: i32,
    #[serde(default)]
    world: String,
}

#[derive(Debug, Deserialize, Default)]
struct ListCase {
    #[serde(default)]
    first: i32,
    #[serde(default)]
    after: Option<String>,
    #[serde(default)]
    query: Option<String>,
    #[serde(default)]
    namespaces: Vec<Option<NamespaceInputDef>>,
    #[serde(default)]
    order_by: String,
    #[serde(default)]
    descending: bool,
    #[serde(default)]
    uid: i32,
    #[serde(default)]
    world: String,
}

#[derive(Debug, Deserialize, Default)]
struct NodeCase {
    #[serde(default)]
    kind: String,
    #[serde(default)]
    spec: String,
    #[serde(default)]
    raw_id: Option<String>,
    #[serde(default)]
    uid: i32,
    #[serde(default)]
    world: String,
}

#[derive(Debug, Deserialize, Default)]
struct Corpus {
    #[serde(default)]
    worlds: BTreeMap<String, World>,
    #[serde(default)]
    by_spec: Vec<LookupCase>,
    #[serde(default)]
    by_id: Vec<LookupCase>,
    #[serde(default)]
    is_available: Vec<LookupCase>,
    #[serde(default)]
    default: Vec<DefaultCase>,
    #[serde(default)]
    create: Vec<MutationCase>,
    #[serde(default)]
    update: Vec<MutationCase>,
    #[serde(default)]
    delete: Vec<MutationCase>,
    #[serde(default)]
    star_create: Vec<StarCase>,
    #[serde(default)]
    star_delete: Vec<StarCase>,
    #[serde(default)]
    set_default: Vec<StarCase>,
    #[serde(default)]
    list: Vec<ListCase>,
    #[serde(default)]
    node: Vec<NodeCase>,
}

// ---------------------------------------------------------------------------
// Fake database
// ---------------------------------------------------------------------------

struct FakeDb {
    world: RefCell<World>,
}

impl FakeDb {
    fn new(world: World) -> Self {
        Self {
            world: RefCell::new(world),
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
        let world = self.world.borrow();
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
        let mut world = self.world.borrow_mut();
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
        let mut world = self.world.borrow_mut();
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
        let mut world = self.world.borrow_mut();
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
        let world = self.world.borrow();
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
        let mut world = self.world.borrow_mut();
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
        let mut world = self.world.borrow_mut();
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
        let mut world = self.world.borrow_mut();
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
        let world = self.world.borrow();
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
        let world = self.world.borrow();
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
        let world = self.world.borrow();
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
        let world = self.world.borrow();
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
        let world = self.world.borrow();
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
        let world = self.world.borrow();
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
// Canonical JSON
// ---------------------------------------------------------------------------

fn namespace_value(namespace: &NamespaceResolver) -> Value {
    let mut map = Map::new();
    if let Some(user) = namespace.as_user() {
        map.insert("kind".to_owned(), Value::from("User"));
        map.insert("id".to_owned(), Value::from(namespace.id()));
        map.insert("url".to_owned(), Value::from(namespace.url()));
        map.insert(
            "namespace_name".to_owned(),
            Value::from(namespace.namespace_name()),
        );
        map.insert("user_id".to_owned(), Value::from(user.id));
    } else if let Some(org) = namespace.as_org() {
        map.insert("kind".to_owned(), Value::from("Org"));
        map.insert("id".to_owned(), Value::from(namespace.id()));
        map.insert("url".to_owned(), Value::from(namespace.url()));
        map.insert(
            "namespace_name".to_owned(),
            Value::from(namespace.namespace_name()),
        );
        map.insert("org_id".to_owned(), Value::from(org.id));
    }
    Value::Object(map)
}

fn resolver_value(
    ctx: &RequestContext,
    resolver: &sg_service_frontend::search_contexts_graphql::SearchContextResolver<'_>,
) -> Value {
    let mut map = Map::new();
    map.insert("id".to_owned(), Value::from(resolver.id()));
    map.insert("name".to_owned(), Value::from(resolver.name()));
    map.insert(
        "description".to_owned(),
        Value::from(resolver.description()),
    );
    map.insert("public".to_owned(), Value::from(resolver.public()));
    map.insert(
        "auto_defined".to_owned(),
        Value::from(resolver.auto_defined()),
    );
    map.insert("spec".to_owned(), Value::from(resolver.spec()));
    map.insert(
        "updated_at".to_owned(),
        Value::from(format_gqlutil_datetime(resolver.updated_at())),
    );
    map.insert(
        "viewer_can_manage".to_owned(),
        Value::from(resolver.viewer_can_manage(ctx)),
    );
    map.insert(
        "viewer_has_as_default".to_owned(),
        Value::from(resolver.viewer_has_as_default()),
    );
    map.insert(
        "viewer_has_starred".to_owned(),
        Value::from(resolver.viewer_has_starred()),
    );
    map.insert("query".to_owned(), Value::from(resolver.query()));

    match resolver.namespace(ctx) {
        Ok(Some(namespace)) => {
            map.insert("namespace".to_owned(), namespace_value(&namespace));
            map.insert("namespace_error".to_owned(), Value::from(""));
        }
        Ok(None) => {
            map.insert("namespace".to_owned(), Value::Null);
            map.insert("namespace_error".to_owned(), Value::from(""));
        }
        Err(error) => {
            map.insert("namespace".to_owned(), Value::Null);
            map.insert("namespace_error".to_owned(), Value::from(error.to_string()));
        }
    }

    match resolver.repositories() {
        Ok(repositories) => {
            let values: Vec<Value> = repositories
                .iter()
                .map(|entry| {
                    let mut map = Map::new();
                    let mut repository = Map::new();
                    repository.insert("id".to_owned(), Value::from(entry.repository().id()));
                    repository.insert("name".to_owned(), Value::from(entry.repository().name()));
                    repository.insert("url".to_owned(), Value::from(entry.repository().url()));
                    map.insert("repository".to_owned(), Value::Object(repository));
                    map.insert(
                        "revisions".to_owned(),
                        Value::from(entry.revisions().to_vec()),
                    );
                    Value::Object(map)
                })
                .collect();
            map.insert("repositories".to_owned(), Value::Array(values));
            map.insert("repositories_error".to_owned(), Value::from(""));
        }
        Err(error) => {
            map.insert("repositories".to_owned(), Value::Array(Vec::new()));
            map.insert(
                "repositories_error".to_owned(),
                Value::from(error.to_string()),
            );
        }
    }
    Value::Object(map)
}

fn lookup_value(
    result: Result<sg_service_frontend::search_contexts_graphql::SearchContextResolver<'_>, Error>,
    ctx: &RequestContext,
) -> Value {
    match result {
        Ok(resolver) => {
            let mut map = Map::new();
            map.insert("resolver".to_owned(), resolver_value(ctx, &resolver));
            map.insert("resolve_error".to_owned(), Value::from(""));
            Value::Object(map)
        }
        Err(error) => {
            let mut map = Map::new();
            map.insert("resolver".to_owned(), Value::Null);
            map.insert("resolve_error".to_owned(), Value::from(error.to_string()));
            Value::Object(map)
        }
    }
}

fn failure_value(error: Result<(), Error>) -> Value {
    let mut map = Map::new();
    map.insert(
        "error".to_owned(),
        Value::from(
            error
                .err()
                .map_or_else(String::new, |error| error.to_string()),
        ),
    );
    Value::Object(map)
}

// ---------------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------------

fn request_context(uid: i32, dotcom: bool) -> RequestContext {
    RequestContext {
        actor_uid: uid,
        internal: false,
        sourcegraph_dot_com_mode: dotcom,
    }
}

fn case_id(spec: &str, raw_id: Option<&String>) -> String {
    raw_id
        .cloned()
        .unwrap_or_else(|| marshal_search_context_id(spec))
}

fn case_user_id(user_id: Option<i32>, raw_user_id: Option<&String>) -> String {
    raw_user_id
        .cloned()
        .unwrap_or_else(|| marshal_user_id(user_id.unwrap_or(0)))
}

fn repo_inputs(inputs: &[RepoInputDef]) -> Vec<SearchContextRepositoryRevisionsInputArgs> {
    inputs
        .iter()
        .map(|input| SearchContextRepositoryRevisionsInputArgs {
            repository_id: input
                .raw_id
                .clone()
                .unwrap_or_else(|| marshal_repository_id(input.id.unwrap_or(0))),
            revisions: input.revisions.clone(),
        })
        .collect()
}

fn namespace_input(input: &Option<NamespaceInputDef>) -> Option<String> {
    input.as_ref().map(|namespace| {
        if let Some(raw) = &namespace.raw_id {
            raw.clone()
        } else if let Some(org_id) = namespace.org_id {
            marshal_org_id(org_id)
        } else {
            marshal_user_id(namespace.user_id.unwrap_or(0))
        }
    })
}

#[test]
fn search_contexts_graphql_harness() {
    if std::env::var("SG_M9HQ_DIFF").as_deref() != Ok("1") {
        return;
    }
    let corpus_path = std::env::var("M9HQ_CORPUS").expect("M9HQ_CORPUS");
    let out_path = std::env::var("M9HQ_OUT").expect("M9HQ_OUT");
    let corpus: Corpus =
        serde_json::from_slice(&std::fs::read(corpus_path).expect("corpus")).expect("corpus json");

    let mut out = Map::new();
    let world = |name: &str| FakeDb::new(corpus.worlds.get(name).cloned().unwrap_or_default());

    let by_spec: Vec<Value> = corpus
        .by_spec
        .iter()
        .map(|case| {
            let db = world(&case.world);
            let resolver = Resolver::new(&db);
            let ctx = request_context(case.uid, case.dotcom);
            lookup_value(resolver.search_context_by_spec(&ctx, &case.spec), &ctx)
        })
        .collect();
    out.insert("by_spec".to_owned(), Value::Array(by_spec));

    let by_id: Vec<Value> = corpus
        .by_id
        .iter()
        .map(|case| {
            let db = world(&case.world);
            let resolver = Resolver::new(&db);
            let ctx = request_context(case.uid, false);
            let id = case_id(&case.spec, case.raw_id.as_ref());
            lookup_value(resolver.search_context_by_id(&ctx, &id), &ctx)
        })
        .collect();
    out.insert("by_id".to_owned(), Value::Array(by_id));

    let is_available: Vec<Value> = corpus
        .is_available
        .iter()
        .map(|case| {
            let db = world(&case.world);
            let resolver = Resolver::new(&db);
            let ctx = request_context(case.uid, case.dotcom);
            match resolver.is_search_context_available(&ctx, &case.spec) {
                Ok(value) => Value::from(value),
                Err(error) => Value::from(error.to_string()),
            }
        })
        .collect();
    out.insert("is_available".to_owned(), Value::Array(is_available));

    let default: Vec<Value> = corpus
        .default
        .iter()
        .map(|case| {
            let db = world(&case.world);
            let resolver = Resolver::new(&db);
            let ctx = request_context(case.uid, false);
            lookup_value(resolver.default_search_context(&ctx), &ctx)
        })
        .collect();
    out.insert("default".to_owned(), Value::Array(default));

    let create: Vec<Value> = corpus
        .create
        .iter()
        .map(|case| {
            let db = world(&case.world);
            let resolver = Resolver::new(&db);
            let ctx = request_context(case.uid, false);
            let args = CreateSearchContextArgs {
                search_context: SearchContextInputArgs {
                    name: case.search_context.name.clone(),
                    description: case.search_context.description.clone(),
                    public: case.search_context.public,
                    namespace: namespace_input(&case.search_context.namespace),
                    query: case.search_context.query.clone(),
                },
                repositories: repo_inputs(&case.repositories),
            };
            lookup_value(resolver.create_search_context(&ctx, &args), &ctx)
        })
        .collect();
    out.insert("create".to_owned(), Value::Array(create));

    let update: Vec<Value> = corpus
        .update
        .iter()
        .map(|case| {
            let db = world(&case.world);
            let resolver = Resolver::new(&db);
            let ctx = request_context(case.uid, false);
            let args = UpdateSearchContextArgs {
                id: case_id(&case.id, case.raw_id.as_ref()),
                search_context: SearchContextEditInputArgs {
                    name: case.search_context.name.clone(),
                    description: case.search_context.description.clone(),
                    public: case.search_context.public,
                    query: case.search_context.query.clone(),
                },
                repositories: repo_inputs(&case.repositories),
            };
            lookup_value(resolver.update_search_context(&ctx, &args), &ctx)
        })
        .collect();
    out.insert("update".to_owned(), Value::Array(update));

    let delete: Vec<Value> = corpus
        .delete
        .iter()
        .map(|case| {
            let db = world(&case.world);
            let resolver = Resolver::new(&db);
            let ctx = request_context(case.uid, false);
            let args = DeleteSearchContextArgs {
                id: case_id(&case.id, case.raw_id.as_ref()),
            };
            failure_value(resolver.delete_search_context(&ctx, &args))
        })
        .collect();
    out.insert("delete".to_owned(), Value::Array(delete));

    let star_create: Vec<Value> = corpus
        .star_create
        .iter()
        .map(|case| {
            let db = world(&case.world);
            let resolver = Resolver::new(&db);
            let ctx = request_context(case.uid, false);
            let args = SearchContextStarArgs {
                search_context_id: case_id(&case.spec, case.raw_id.as_ref()),
                user_id: case_user_id(case.user_id, case.raw_user_id.as_ref()),
            };
            failure_value(resolver.create_search_context_star(&ctx, &args))
        })
        .collect();
    out.insert("star_create".to_owned(), Value::Array(star_create));

    let star_delete: Vec<Value> = corpus
        .star_delete
        .iter()
        .map(|case| {
            let db = world(&case.world);
            let resolver = Resolver::new(&db);
            let ctx = request_context(case.uid, false);
            let args = SearchContextStarArgs {
                search_context_id: case_id(&case.spec, case.raw_id.as_ref()),
                user_id: case_user_id(case.user_id, case.raw_user_id.as_ref()),
            };
            failure_value(resolver.delete_search_context_star(&ctx, &args))
        })
        .collect();
    out.insert("star_delete".to_owned(), Value::Array(star_delete));

    let set_default: Vec<Value> = corpus
        .set_default
        .iter()
        .map(|case| {
            let db = world(&case.world);
            let resolver = Resolver::new(&db);
            let ctx = request_context(case.uid, false);
            let args = SetDefaultSearchContextArgs {
                search_context_id: case_id(&case.spec, case.raw_id.as_ref()),
                user_id: case_user_id(case.user_id, case.raw_user_id.as_ref()),
            };
            failure_value(resolver.set_default_search_context(&ctx, &args))
        })
        .collect();
    out.insert("set_default".to_owned(), Value::Array(set_default));

    let list: Vec<Value> = corpus
        .list
        .iter()
        .map(|case| {
            let db = world(&case.world);
            let resolver = Resolver::new(&db);
            let ctx = request_context(case.uid, false);
            let order_by = if case.order_by == "updated_at" {
                SearchContextsOrderBy::UpdatedAt
            } else {
                SearchContextsOrderBy::Spec
            };
            let args = ListSearchContextsArgs {
                first: case.first,
                after: case.after.clone(),
                query: case.query.clone(),
                namespaces: case.namespaces.iter().map(namespace_input).collect(),
                order_by,
                descending: case.descending,
            };
            match resolver.search_contexts(&ctx, &args) {
                Ok(connection) => {
                    let mut nodes = Vec::new();
                    for node in connection.nodes() {
                        let mut map = Map::new();
                        map.insert("id".to_owned(), Value::from(node.id()));
                        map.insert("spec".to_owned(), Value::from(node.spec()));
                        map.insert("name".to_owned(), Value::from(node.name()));
                        map.insert(
                            "updated_at".to_owned(),
                            Value::from(format_gqlutil_datetime(node.updated_at())),
                        );
                        map.insert(
                            "default".to_owned(),
                            Value::from(node.viewer_has_as_default()),
                        );
                        map.insert("starred".to_owned(), Value::from(node.viewer_has_starred()));
                        nodes.push(Value::Object(map));
                    }
                    let page_info = connection.page_info();
                    let mut map = Map::new();
                    map.insert("nodes".to_owned(), Value::Array(nodes));
                    map.insert(
                        "total_count".to_owned(),
                        Value::from(connection.total_count()),
                    );
                    let mut page = Map::new();
                    page.insert(
                        "has_next_page".to_owned(),
                        Value::from(page_info.has_next_page),
                    );
                    page.insert(
                        "end_cursor".to_owned(),
                        match page_info.end_cursor {
                            Some(cursor) => Value::from(cursor),
                            None => Value::Null,
                        },
                    );
                    map.insert("page_info".to_owned(), Value::Object(page));
                    map.insert("error".to_owned(), Value::from(""));
                    Value::Object(map)
                }
                Err(error) => {
                    let mut map = Map::new();
                    map.insert("nodes".to_owned(), Value::Array(Vec::new()));
                    map.insert("total_count".to_owned(), Value::from(0));
                    map.insert("page_info".to_owned(), Value::Null);
                    map.insert("error".to_owned(), Value::from(error.to_string()));
                    Value::Object(map)
                }
            }
        })
        .collect();
    out.insert("list".to_owned(), Value::Array(list));

    let node: Vec<Value> = corpus
        .node
        .iter()
        .map(|case| {
            let db = world(&case.world);
            let resolver = Resolver::new(&db);
            let nodes = resolver.node_resolvers();
            let ctx = request_context(case.uid, false);
            let id = case_id(&case.spec, case.raw_id.as_ref());
            let mut map = Map::new();
            match nodes.node_by_id(&ctx, &case.kind, &id) {
                Ok(Some(found)) => {
                    map.insert("handled".to_owned(), Value::from(true));
                    map.insert("resolver".to_owned(), resolver_value(&ctx, &found));
                    map.insert("error".to_owned(), Value::from(""));
                }
                Ok(None) => {
                    map.insert("handled".to_owned(), Value::from(false));
                    map.insert("resolver".to_owned(), Value::Null);
                    map.insert("error".to_owned(), Value::from(""));
                }
                Err(error) => {
                    map.insert("handled".to_owned(), Value::from(true));
                    map.insert("resolver".to_owned(), Value::Null);
                    map.insert("error".to_owned(), Value::from(error.to_string()));
                }
            }
            Value::Object(map)
        })
        .collect();
    out.insert("node".to_owned(), Value::Array(node));

    let encoded = marshal_go(&Value::Object(out));
    std::fs::write(&out_path, &encoded).expect("write output");
    println!("RUST search-contexts-graphql len={}", encoded.len());
}
