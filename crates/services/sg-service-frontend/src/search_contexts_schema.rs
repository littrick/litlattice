#![forbid(unsafe_code)]

//! Search contexts GraphQL schema registration
//! (`cmd/frontend/graphqlbackend/schema.go:58-61`,
//! `graphqlbackend.go:440-445,447,538-546,687-713,832`,
//! `cmd/frontend/internal/searchcontexts/init.go:26`,
//! `search_contexts.go:20-39`).
//!
//! This slice is the schema-assembly layer only. It embeds the baseline
//! `search_contexts.graphql` document, appends it to an existing assembled
//! schema when the enterprise resolver is present, and exposes the
//! field -> handler registration table plus a resolver dispatch backed by the
//! M9.8hq [`crate::search_contexts_graphql`] resolver. The database is injected
//! through [`SearchContextsGraphqlDatabase`].
//!
//! # Why the executor is not wired
//!
//! [`crate::graphql_api::SchemaGraphqlDeps`] owns a fixed
//! `Arc<sg_graphql::Schema>` and a synchronous [`FrontendRootResolver`] whose
//! `QueryRoot`/`MutationRoot` are closed structs (database lookups happen in
//! async prefetch passes before execution). Adding search contexts fields there
//! requires changing `graphql_resolvers.rs`'s root objects and its request
//! prefetch pipeline, which is an executor change outside this slice's scope.
//! The registration function is therefore exposed for the eventual executor
//! wiring (and exercised by `tests/search_contexts_schema_diff.rs`); neither
//! `graphql_api.rs` nor `graphql_resolvers.rs` is modified.
//!
//! [`FrontendRootResolver`]: crate::graphql_resolvers::FrontendRootResolver

use sg_graphql::schema::{assemble, Schema, SchemaError};

use crate::search_contexts::{Error, RequestContext};
use crate::search_contexts_graphql::{
    CreateSearchContextArgs, DeleteSearchContextArgs, ListSearchContextsArgs,
    SearchContextConnection, SearchContextEditInputArgs, SearchContextInputArgs,
    SearchContextRepositoryRevisionsInputArgs, SearchContextResolver, SearchContextStarArgs,
    SearchContextsGraphqlDatabase, SearchContextsOrderBy, SetDefaultSearchContextArgs,
    UpdateSearchContextArgs,
};

/// `//go:embed search_contexts.graphql` (`schema.go:58-61`), byte-identical to
/// the baseline file.
pub const SEARCH_CONTEXTS_SCHEMA: &str = include_str!("search_contexts.graphql");

/// `relay.MarshalID` kind registered by `NodeResolvers` (`resolvers.go:29-35`).
pub const SEARCH_CONTEXT_NODE_KIND: &str = "SearchContext";

/// The handler the baseline registers for [`SEARCH_CONTEXT_NODE_KIND`].
pub const SEARCH_CONTEXT_NODE_HANDLER: &str = "SearchContextByID";

/// The GraphQL operation a root field belongs to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SearchContextsOperation {
    Query,
    Mutation,
}

impl SearchContextsOperation {
    /// Introspection root type name (`Query`/`Mutation`).
    pub const fn root_type(self) -> &'static str {
        match self {
            Self::Query => "Query",
            Self::Mutation => "Mutation",
        }
    }
}

/// Whether a root field reads or writes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SearchContextsFieldBehavior {
    Read,
    Write,
}

impl SearchContextsFieldBehavior {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Read => "read",
            Self::Write => "write",
        }
    }
}

/// One row of the schema registration table: a GraphQL root field and the
/// Go resolver method graph-gophers binds to it (the method name is the field
/// name with an upper-cased first letter).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SearchContextsFieldHandler {
    pub operation: SearchContextsOperation,
    pub field: &'static str,
    pub handler: &'static str,
    pub behavior: SearchContextsFieldBehavior,
}

/// The `SearchContextsResolver` surface (`search_contexts.go:20-39`).
pub const SEARCH_CONTEXTS_FIELD_HANDLERS: &[SearchContextsFieldHandler] = &[
    SearchContextsFieldHandler {
        operation: SearchContextsOperation::Query,
        field: "searchContexts",
        handler: "SearchContexts",
        behavior: SearchContextsFieldBehavior::Read,
    },
    SearchContextsFieldHandler {
        operation: SearchContextsOperation::Query,
        field: "searchContextBySpec",
        handler: "SearchContextBySpec",
        behavior: SearchContextsFieldBehavior::Read,
    },
    SearchContextsFieldHandler {
        operation: SearchContextsOperation::Query,
        field: "isSearchContextAvailable",
        handler: "IsSearchContextAvailable",
        behavior: SearchContextsFieldBehavior::Read,
    },
    SearchContextsFieldHandler {
        operation: SearchContextsOperation::Query,
        field: "defaultSearchContext",
        handler: "DefaultSearchContext",
        behavior: SearchContextsFieldBehavior::Read,
    },
    SearchContextsFieldHandler {
        operation: SearchContextsOperation::Mutation,
        field: "createSearchContext",
        handler: "CreateSearchContext",
        behavior: SearchContextsFieldBehavior::Write,
    },
    SearchContextsFieldHandler {
        operation: SearchContextsOperation::Mutation,
        field: "deleteSearchContext",
        handler: "DeleteSearchContext",
        behavior: SearchContextsFieldBehavior::Write,
    },
    SearchContextsFieldHandler {
        operation: SearchContextsOperation::Mutation,
        field: "updateSearchContext",
        handler: "UpdateSearchContext",
        behavior: SearchContextsFieldBehavior::Write,
    },
    SearchContextsFieldHandler {
        operation: SearchContextsOperation::Mutation,
        field: "createSearchContextStar",
        handler: "CreateSearchContextStar",
        behavior: SearchContextsFieldBehavior::Write,
    },
    SearchContextsFieldHandler {
        operation: SearchContextsOperation::Mutation,
        field: "deleteSearchContextStar",
        handler: "DeleteSearchContextStar",
        behavior: SearchContextsFieldBehavior::Write,
    },
    SearchContextsFieldHandler {
        operation: SearchContextsOperation::Mutation,
        field: "setDefaultSearchContext",
        handler: "SetDefaultSearchContext",
        behavior: SearchContextsFieldBehavior::Write,
    },
];

/// Look up the registered handler for `operation.field`.
pub fn field_handler(
    operation: SearchContextsOperation,
    field: &str,
) -> Option<&'static SearchContextsFieldHandler> {
    SEARCH_CONTEXTS_FIELD_HANDLERS
        .iter()
        .find(|handler| handler.operation == operation && handler.field == field)
}

/// `searchcontexts.IsEnabled()` (`internal/search/searchcontexts/conf.go:8-13`):
/// false when `DISABLE_SEARCH_CONTEXTS` parses to true.
pub fn is_enabled() -> bool {
    crate::search_contexts::is_enabled()
}

/// Arguments for a single root-field dispatch. Root fields share one struct so
/// the registration layer does not expose ten separate entry points.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SearchContextsFieldArgs {
    /// `ID!` for `deleteSearchContext`/`deleteSearchContextStar`/etc.
    pub id: Option<String>,
    /// `spec: String!` / `query: String` depending on the field.
    pub spec: Option<String>,
    /// `first: Int = 50`.
    pub first: Option<i32>,
    /// `after: String`.
    pub after: Option<String>,
    /// `query: String` for `searchContexts`.
    pub query: Option<String>,
    /// `namespaces: [ID] = []`; `None` is the instance-level namespace.
    pub namespaces: Vec<Option<String>>,
    /// `orderBy: SearchContextsOrderBy = SEARCH_CONTEXT_SPEC`.
    pub order_by: Option<SearchContextsOrderBy>,
    /// `descending: Boolean = false`.
    pub descending: Option<bool>,
    /// `searchContext: SearchContextInput!`.
    pub search_context: Option<SearchContextInputArgs>,
    /// `searchContext: SearchContextEditInput!`.
    pub search_context_edit: Option<SearchContextEditInputArgs>,
    /// `repositories: [SearchContextRepositoryRevisionsInput!]!`.
    pub repositories: Vec<SearchContextRepositoryRevisionsInputArgs>,
    /// `searchContextID: ID!`.
    pub search_context_id: Option<String>,
    /// `userID: ID!`.
    pub user_id: Option<String>,
}

/// One dispatched root field. Mirrors the return type of the Go interface
/// method.
pub enum SearchContextsFieldValue<'a> {
    SearchContext(SearchContextResolver<'a>),
    Connection(SearchContextConnection<'a>),
    Bool(bool),
    /// `graphqlbackend.EmptyResponse`.
    EmptyResponse,
}

/// Resolver dispatch returned by [`register_search_contexts_schema`] when the
/// optional resolver is enabled.
pub struct SearchContextsResolverDispatch<'a> {
    resolver: crate::search_contexts_graphql::Resolver<'a>,
}

impl<'a> SearchContextsResolverDispatch<'a> {
    pub fn new(db: &'a dyn SearchContextsGraphqlDatabase) -> Self {
        Self {
            resolver: crate::search_contexts_graphql::Resolver::new(db),
        }
    }

    /// The underlying M9.8hq resolver.
    pub fn resolver(&self) -> &crate::search_contexts_graphql::Resolver<'a> {
        &self.resolver
    }

    /// `SearchContextsResolver.NodeResolvers()`.
    pub fn node_resolvers(&self) -> crate::search_contexts_graphql::NodeResolvers<'a> {
        self.resolver.node_resolvers()
    }

    /// `NodeByIDFunc` dispatch: only `kind == "SearchContext"` is owned.
    pub fn node_by_id(
        &self,
        ctx: &RequestContext,
        kind: &str,
        id: &str,
    ) -> Result<Option<SearchContextResolver<'a>>, Error> {
        self.resolver.node_resolvers().node_by_id(ctx, kind, id)
    }

    /// Dispatch a root field. `Ok(None)` means the field is not part of the
    /// search contexts surface (or the resolver is disabled).
    pub fn field(
        &self,
        operation: SearchContextsOperation,
        field: &str,
        args: &SearchContextsFieldArgs,
        ctx: &RequestContext,
    ) -> Result<Option<SearchContextsFieldValue<'a>>, Error> {
        if field_handler(operation, field).is_none() {
            return Ok(None);
        }
        let value = match (operation, field) {
            (SearchContextsOperation::Query, "searchContexts") => {
                let list_args = ListSearchContextsArgs {
                    first: args.first.unwrap_or(50),
                    after: args.after.clone(),
                    query: args.query.clone(),
                    namespaces: args.namespaces.clone(),
                    order_by: args.order_by.unwrap_or_default(),
                    descending: args.descending.unwrap_or(false),
                };
                SearchContextsFieldValue::Connection(
                    self.resolver.search_contexts(ctx, &list_args)?,
                )
            }
            (SearchContextsOperation::Query, "searchContextBySpec") => {
                let spec = require(args.spec.as_deref(), "spec")?;
                SearchContextsFieldValue::SearchContext(
                    self.resolver.search_context_by_spec(ctx, spec)?,
                )
            }
            (SearchContextsOperation::Query, "isSearchContextAvailable") => {
                let spec = require(args.spec.as_deref(), "spec")?;
                SearchContextsFieldValue::Bool(
                    self.resolver.is_search_context_available(ctx, spec)?,
                )
            }
            (SearchContextsOperation::Query, "defaultSearchContext") => {
                SearchContextsFieldValue::SearchContext(self.resolver.default_search_context(ctx)?)
            }
            (SearchContextsOperation::Mutation, "createSearchContext") => {
                let create_args = CreateSearchContextArgs {
                    search_context: require(args.search_context.as_ref(), "searchContext")?.clone(),
                    repositories: args.repositories.clone(),
                };
                SearchContextsFieldValue::SearchContext(
                    self.resolver.create_search_context(ctx, &create_args)?,
                )
            }
            (SearchContextsOperation::Mutation, "deleteSearchContext") => {
                let delete_args = DeleteSearchContextArgs {
                    id: require(args.id.as_deref(), "id")?.to_owned(),
                };
                self.resolver.delete_search_context(ctx, &delete_args)?;
                SearchContextsFieldValue::EmptyResponse
            }
            (SearchContextsOperation::Mutation, "updateSearchContext") => {
                let update_args = UpdateSearchContextArgs {
                    id: require(args.id.as_deref(), "id")?.to_owned(),
                    search_context: require(args.search_context_edit.as_ref(), "searchContext")?
                        .clone(),
                    repositories: args.repositories.clone(),
                };
                SearchContextsFieldValue::SearchContext(
                    self.resolver.update_search_context(ctx, &update_args)?,
                )
            }
            (SearchContextsOperation::Mutation, "createSearchContextStar") => {
                let star_args = star_args(args)?;
                self.resolver.create_search_context_star(ctx, &star_args)?;
                SearchContextsFieldValue::EmptyResponse
            }
            (SearchContextsOperation::Mutation, "deleteSearchContextStar") => {
                let star_args = star_args(args)?;
                self.resolver.delete_search_context_star(ctx, &star_args)?;
                SearchContextsFieldValue::EmptyResponse
            }
            (SearchContextsOperation::Mutation, "setDefaultSearchContext") => {
                let default_args = SetDefaultSearchContextArgs {
                    search_context_id: require(
                        args.search_context_id.as_deref(),
                        "searchContextID",
                    )?
                    .to_owned(),
                    user_id: require(args.user_id.as_deref(), "userID")?.to_owned(),
                };
                self.resolver
                    .set_default_search_context(ctx, &default_args)?;
                SearchContextsFieldValue::EmptyResponse
            }
            // `field_handler` above guarantees one of the arms matches.
            _ => return Ok(None),
        };
        Ok(Some(value))
    }
}

fn require<'a, T: ?Sized>(value: Option<&'a T>, name: &str) -> Result<&'a T, Error> {
    value.ok_or_else(|| Error::other(format!("missing required argument: {name}")))
}

fn star_args(args: &SearchContextsFieldArgs) -> Result<SearchContextStarArgs, Error> {
    Ok(SearchContextStarArgs {
        search_context_id: require(args.search_context_id.as_deref(), "searchContextID")?
            .to_owned(),
        user_id: require(args.user_id.as_deref(), "userID")?.to_owned(),
    })
}

/// The result of schema registration. `dispatch` is `None` when the optional
/// resolver is absent (disabled), which reproduces the baseline `absence`.
pub struct RegisteredSearchContexts<'a> {
    pub schema: Schema,
    pub dispatch: Option<SearchContextsResolverDispatch<'a>>,
}

impl RegisteredSearchContexts<'_> {
    pub fn is_enabled(&self) -> bool {
        self.dispatch.is_some()
    }
}

/// Append [`SEARCH_CONTEXTS_SCHEMA`] to an already assembled schema, exactly
/// like `schemas = append(schemas, searchContextsSchema)` followed by
/// `strings.Join(schemas, "\n")`.
///
/// This re-runs the SDL parser over the base document; [`Schema::schema_string`]
/// stores the user SDL passed to the original assembly, so the result is the
/// same document set `NewSchema` parses.
pub fn append_search_contexts_schema(base: &Schema) -> Result<Schema, SchemaError> {
    assemble(&[base.schema_string.as_str(), SEARCH_CONTEXTS_SCHEMA])
}

/// Register the search contexts surface into `base`.
///
/// `db == None` mirrors a nil `OptionalResolver.SearchContextsResolver`: the
/// SDL is not appended and no handlers are registered, so the types and root
/// fields are absent from the returned schema.
pub fn register_search_contexts_schema<'a>(
    base: &Schema,
    db: Option<&'a dyn SearchContextsGraphqlDatabase>,
) -> Result<RegisteredSearchContexts<'a>, SchemaError> {
    match db {
        Some(db) => Ok(RegisteredSearchContexts {
            schema: append_search_contexts_schema(base)?,
            dispatch: Some(SearchContextsResolverDispatch::new(db)),
        }),
        None => Ok(RegisteredSearchContexts {
            schema: base.clone(),
            dispatch: None,
        }),
    }
}

/// `init.go:26`'s switch: register only when the feature is enabled.
pub fn register_with_switch<'a>(
    base: &Schema,
    db: &'a dyn SearchContextsGraphqlDatabase,
    enabled: bool,
) -> Result<RegisteredSearchContexts<'a>, SchemaError> {
    if enabled && is_enabled() {
        register_search_contexts_schema(base, Some(db))
    } else {
        register_search_contexts_schema(base, None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn oss_schema() -> Schema {
        sg_graphql::schema::assemble_oss().expect("oss schema")
    }

    #[test]
    fn appends_types_and_root_fields() {
        let base = oss_schema();
        assert!(!base.types.contains_key("SearchContext"));
        let registered = register_search_contexts_schema(&base, None).expect("disabled");
        assert!(!registered.is_enabled());
        assert!(!registered.schema.types.contains_key("SearchContext"));

        let registered = register_search_contexts_schema(&base, Some(&NoopDb)).expect("enabled");
        let schema = &registered.schema;
        assert!(schema.types.contains_key("SearchContext"));
        assert!(schema.types.contains_key("SearchContextConnection"));
        assert!(schema.types.contains_key("SearchContextsOrderBy"));
        let query = &schema.types["Query"];
        assert!(query.fields.iter().any(|f| f.name == "searchContextBySpec"));
        let mutation = &schema.types["Mutation"];
        assert!(mutation
            .fields
            .iter()
            .any(|f| f.name == "createSearchContextStar"));
    }

    #[test]
    fn registration_table_is_complete() {
        let query: Vec<_> = SEARCH_CONTEXTS_FIELD_HANDLERS
            .iter()
            .filter(|h| h.operation == SearchContextsOperation::Query)
            .collect();
        let mutation: Vec<_> = SEARCH_CONTEXTS_FIELD_HANDLERS
            .iter()
            .filter(|h| h.operation == SearchContextsOperation::Mutation)
            .collect();
        assert_eq!(query.len(), 4);
        assert_eq!(mutation.len(), 6);
        assert!(query
            .iter()
            .all(|h| h.behavior == SearchContextsFieldBehavior::Read));
        assert!(mutation
            .iter()
            .all(|h| h.behavior == SearchContextsFieldBehavior::Write));
        assert_eq!(
            field_handler(SearchContextsOperation::Query, "searchContexts").map(|h| h.handler),
            Some("SearchContexts")
        );
        assert_eq!(
            field_handler(SearchContextsOperation::Query, "notAField"),
            None
        );
    }

    #[test]
    fn node_kind_is_registered() {
        assert_eq!(SEARCH_CONTEXT_NODE_KIND, "SearchContext");
        assert_eq!(SEARCH_CONTEXT_NODE_HANDLER, "SearchContextByID");
    }

    /// Minimal database for schema-assembly tests; no resolver method runs.
    struct NoopDb;

    impl crate::search_contexts::SearchContextsDatabase for NoopDb {
        fn get_namespace_by_name(
            &self,
            _name: &str,
        ) -> Result<crate::search_contexts::Namespace, Error> {
            Err(Error::namespace_not_found())
        }
        fn get_org_member_by_org_id_and_user_id(
            &self,
            _org_id: i32,
            _user_id: i32,
        ) -> Result<Option<crate::search_contexts::OrgMembership>, Error> {
            Ok(None)
        }
        fn get_current_user(
            &self,
            _uid: i32,
        ) -> Result<Option<crate::search_contexts::User>, Error> {
            Ok(None)
        }
        fn get_search_context(
            &self,
            _opts: &crate::search_contexts::GetSearchContextOptions,
        ) -> Result<crate::search_contexts::SearchContext, Error> {
            Err(Error::search_context_not_found())
        }
        fn create_search_context_with_repository_revisions(
            &self,
            _search_context: &crate::search_contexts::SearchContext,
            _repository_revisions: &[crate::search_contexts::SearchContextRepositoryRevisions],
        ) -> Result<crate::search_contexts::SearchContext, Error> {
            Err(Error::other("not used"))
        }
        fn update_search_context_with_repository_revisions(
            &self,
            _search_context: &crate::search_contexts::SearchContext,
            _repository_revisions: &[crate::search_contexts::SearchContextRepositoryRevisions],
        ) -> Result<crate::search_contexts::SearchContext, Error> {
            Err(Error::other("not used"))
        }
        fn delete_search_context(&self, _search_context_id: i64) -> Result<(), Error> {
            Ok(())
        }
        fn get_all_revisions_for_repos(
            &self,
            _repo_ids: &[i32],
        ) -> Result<std::collections::BTreeMap<i32, Vec<String>>, Error> {
            Ok(Default::default())
        }
        fn get_all_queries(&self) -> Result<Vec<String>, Error> {
            Ok(Vec::new())
        }
        fn get_search_context_repository_revisions(
            &self,
            _search_context_id: i64,
        ) -> Result<Vec<crate::search_contexts::SearchContextRepositoryRevisions>, Error> {
            Ok(Vec::new())
        }
        fn list_minimal_repos(
            &self,
            _opts: &crate::search_contexts::ReposListOptions,
        ) -> Result<Vec<crate::search_contexts::MinimalRepo>, Error> {
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

    impl SearchContextsGraphqlDatabase for NoopDb {
        fn as_search_contexts_database(
            &self,
        ) -> &dyn crate::search_contexts::SearchContextsDatabase {
            self
        }
        fn get_search_context_by_id(
            &self,
            _ctx: &RequestContext,
            _id: i64,
        ) -> Result<crate::search_contexts_graphql::SearchContextRecord, Error> {
            Err(Error::search_context_not_found())
        }
        fn list_search_contexts(
            &self,
            _ctx: &RequestContext,
            _page: &crate::search_contexts_graphql::ListSearchContextsPageOptions,
            _opts: &crate::search_contexts_graphql::ListSearchContextsOptions,
        ) -> Result<Vec<crate::search_contexts_graphql::SearchContextRecord>, Error> {
            Ok(Vec::new())
        }
        fn count_search_contexts(
            &self,
            _ctx: &RequestContext,
            _opts: &crate::search_contexts_graphql::ListSearchContextsOptions,
        ) -> Result<i32, Error> {
            Ok(0)
        }
        fn get_default_search_context_for_current_user(
            &self,
            _ctx: &RequestContext,
        ) -> Result<crate::search_contexts_graphql::SearchContextRecord, Error> {
            Err(Error::search_context_not_found())
        }
        fn get_repos_set_by_ids(
            &self,
            _ids: &[i32],
        ) -> Result<
            std::collections::BTreeMap<i32, crate::search_contexts_graphql::MinimalRepoRef>,
            Error,
        > {
            Ok(Default::default())
        }
        fn get_user_by_id(
            &self,
            _id: i32,
        ) -> Result<crate::search_contexts_graphql::NamespaceUser, Error> {
            Err(Error::other("not used"))
        }
        fn get_org_by_id(
            &self,
            _id: i32,
        ) -> Result<crate::search_contexts_graphql::NamespaceOrg, Error> {
            Err(Error::other("not used"))
        }
        fn get_orgs_by_user_id(
            &self,
            _user_id: i32,
        ) -> Result<Vec<crate::search_contexts_graphql::NamespaceOrg>, Error> {
            Ok(Vec::new())
        }
    }
}
