#![forbid(unsafe_code)]

//! `Query.viewer` and the `Viewer` GraphQL interface (M9.67a).
//!
//! Ports `cmd/frontend/graphqlbackend/{viewer,affiliated_namespaces}.go`:
//!
//! - `Query.viewer` (`viewer.go:11-25`): the current user when authenticated,
//!   otherwise a `Visitor` — but only when
//!   `auth.CheckUnauthenticatedVisitorAccess` passes (dotcom mode);
//! - `Query.viewerCanChangeLibraryItemVisibilityToPublic` (`viewer.go:48-56`):
//!   a site-admin check whose `ErrNotAuthenticated`/`ErrMustBeSiteAdmin`
//!   outcomes collapse to `false`;
//! - `User.affiliatedNamespaces` / `Visitor.affiliatedNamespaces`
//!   (`affiliated_namespaces.go:11-41`): the user's own namespace plus every
//!   organization they are a member of, gated by
//!   `auth.CheckSiteAdminOrSameUser`, as a
//!   `gqlutil.NewSliceConnectionResolver` (`namespaces.go:152-154`).
//!
//! The database is injected through [`AffiliatedNamespacesStore`], a
//! synchronous seam bridged to sqlx with `block_in_place` like the other
//! GraphQL stores. No table, column or Redis key is added.

use std::collections::HashSet;
use std::future::Future;
use std::sync::Arc;

use sg_graphql::exec::{Actor, Arguments, ExecError, FieldValue, ObjectValue};
use sg_graphql::json::Json;
use sqlx::PgPool;

use crate::admin_store::check_site_admin_or_same_user;
use crate::graphql_resolvers::{
    ConnectionPageInfoObject, OrgData, OrgObject, UserObject, MUST_BE_SITE_ADMIN_OR_SAME_USER,
};

/// `auth.ErrNoUnauthenticatedVisitorAccess` (`internal/auth/visitor.go:24`).
pub const UNAUTHENTICATED_VISITORS_FORBIDDEN: &str = "unauthenticated visitors are forbidden";

/// The database seam for `User.affiliatedNamespaces`
/// (`db.Orgs().GetByUserID` plus the viewer's memberships).
pub trait AffiliatedNamespacesStore: Send + Sync {
    fn affiliated_orgs(
        &self,
        user_id: i32,
        viewer_uid: i32,
        viewer_site_admin: bool,
    ) -> Result<Vec<OrgData>, String>;
}

pub type ViewerHandle = Arc<dyn AffiliatedNamespacesStore>;

/// `Debug`/`PartialEq` wrapper so handles can live in [`NestedPrefetch`]
/// (`crate::graphql_resolvers::NestedPrefetch`).
#[derive(Clone)]
pub struct ViewerHandleWrapper(pub ViewerHandle);

impl std::fmt::Debug for ViewerHandleWrapper {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ViewerHandle")
    }
}

impl PartialEq for ViewerHandleWrapper {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}

impl Eq for ViewerHandleWrapper {}

// ---------------------------------------------------------------------------
// Connection objects
// ---------------------------------------------------------------------------

/// `gqlutil.NewSliceConnectionResolver(nodes, n, n)` over `NamespaceResolver`
/// (`graphqlbackend/namespaces.go:152-154`).
pub struct NamespaceConnectionObject {
    nodes: Vec<Arc<dyn ObjectValue>>,
}

impl NamespaceConnectionObject {
    pub fn new(nodes: Vec<Arc<dyn ObjectValue>>) -> Self {
        Self { nodes }
    }

    pub fn empty() -> Self {
        Self { nodes: Vec::new() }
    }
}

impl ObjectValue for NamespaceConnectionObject {
    fn type_name(&self) -> &'static str {
        "NamespaceConnection"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "nodes" => Ok(Some(FieldValue::List {
                element_type: Some("Namespace"),
                items: self
                    .nodes
                    .iter()
                    .map(|node| FieldValue::Object(Arc::clone(node)))
                    .collect(),
            })),
            "totalCount" => Ok(Some(FieldValue::Json(Json::Int(self.nodes.len() as i64)))),
            // `sliceConnectionResolver.PageInfo`: only `hasNextPage`, derived
            // from `total > currentEnd`; no cursors are set.
            "pageInfo" => Ok(Some(FieldValue::Object(Arc::new(
                ConnectionPageInfoObject::new(None, None, false, false),
            )))),
            _ => Ok(None),
        }
    }
}

/// `visitorResolver` (`affiliated_namespaces.go:39-41`).
pub struct VisitorObject;

impl ObjectValue for VisitorObject {
    fn type_name(&self) -> &'static str {
        "Visitor"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "affiliatedNamespaces" => Ok(Some(FieldValue::Object(Arc::new(
                NamespaceConnectionObject::empty(),
            )))),
            _ => Ok(None),
        }
    }
}

// ---------------------------------------------------------------------------
// Resolvers
// ---------------------------------------------------------------------------

/// `UserResolver.AffiliatedNamespaces` (`affiliated_namespaces.go:11-31`).
///
/// The user's own namespace comes first, then every organization they are a
/// member of. `ErrExecError` carries the baseline
/// `auth.CheckSiteAdminOrSameUser` message.
pub fn user_affiliated_namespaces(
    user: &UserObject,
    actor: Actor,
    store: &dyn AffiliatedNamespacesStore,
) -> Result<FieldValue, ExecError> {
    let data = user.user_data();
    check_site_admin_or_same_user(actor, user.is_viewer_site_admin(), data.id)
        .map_err(|_| ExecError::new(MUST_BE_SITE_ADMIN_OR_SAME_USER))?;
    let orgs = store
        .affiliated_orgs(
            data.id,
            actor.uid().unwrap_or(0),
            user.is_viewer_site_admin(),
        )
        .map_err(ExecError::new)?;

    let mut nodes: Vec<Arc<dyn ObjectValue>> = Vec::with_capacity(orgs.len() + 1);
    nodes.push(Arc::new(user.clone()));
    for org in orgs {
        nodes.push(Arc::new(OrgObject::from_data_as_viewer(
            org,
            user.is_viewer_site_admin(),
        )));
    }
    Ok(FieldValue::Object(Arc::new(
        NamespaceConnectionObject::new(nodes),
    )))
}

/// `schemaResolver.Viewer` (`viewer.go:11-25`): the current user when
/// authenticated, otherwise a `Visitor` only in dotcom mode.
pub fn viewer(
    user: Option<&UserObject>,
    dotcom_mode: bool,
    _actor: Actor,
    nested: Arc<crate::graphql_resolvers::NestedPrefetch>,
) -> Result<FieldValue, ExecError> {
    if let Some(user) = user {
        return Ok(FieldValue::Object(Arc::new(
            user.clone().with_nested(nested),
        )));
    }
    // `auth.CheckUnauthenticatedVisitorAccess`: non-dotcom sites forbid
    // unauthenticated visitors outright.
    if dotcom_mode {
        Ok(FieldValue::Object(Arc::new(VisitorObject)))
    } else {
        Err(ExecError::new(UNAUTHENTICATED_VISITORS_FORBIDDEN))
    }
}

/// `schemaResolver.ViewerCanChangeLibraryItemVisibilityToPublic`
/// (`viewer.go:48-56`): the boolean site-admin check with the baseline error
/// swallowing.
pub fn viewer_can_change_library_item_visibility_to_public(
    actor: Actor,
    viewer_site_admin: bool,
) -> FieldValue {
    let ok = crate::admin_store::check_current_user_is_site_admin(actor, viewer_site_admin).is_ok();
    FieldValue::Json(Json::Bool(ok))
}

// ---------------------------------------------------------------------------
// Production store
// ---------------------------------------------------------------------------

fn block_on<F: Future>(future: F) -> F::Output {
    match tokio::runtime::Handle::try_current() {
        Ok(handle) => tokio::task::block_in_place(|| handle.block_on(future)),
        Err(_) => tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("viewer blocking runtime")
            .block_on(future),
    }
}

/// `db.Orgs().GetByUserID` + `db.OrgMembers().GetByUserID` membership flags
/// (`internal/database/orgs.go:151-159`,
/// `internal/database/org_members.go:72-86`).
pub struct PgAffiliatedNamespacesStore {
    pool: PgPool,
}

impl PgAffiliatedNamespacesStore {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

impl AffiliatedNamespacesStore for PgAffiliatedNamespacesStore {
    fn affiliated_orgs(
        &self,
        user_id: i32,
        viewer_uid: i32,
        viewer_site_admin: bool,
    ) -> Result<Vec<OrgData>, String> {
        let pool = self.pool.clone();
        block_on(async move {
            let records = sg_db::orgs::orgs_for_user(&pool, user_id)
                .await
                .map_err(|error| error.to_string())?;
            let ids: Vec<i32> = records.iter().map(|org| org.id).collect();
            let member_ids: HashSet<i32> = if viewer_uid == 0 || ids.is_empty() {
                HashSet::new()
            } else {
                sg_db::orgs::org_members_for_user(&pool, viewer_uid, &ids)
                    .await
                    .map_err(|error| error.to_string())?
                    .into_iter()
                    .collect()
            };
            Ok(records
                .into_iter()
                .map(|record| {
                    let member = member_ids.contains(&record.id);
                    OrgData {
                        id: record.id,
                        name: record.name,
                        display_name: record.display_name,
                        created_at: record.created_at,
                        viewer_can_administer: viewer_site_admin || member,
                        viewer_is_member: member,
                    }
                })
                .collect())
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graphql_resolvers::{FrontendRootResolver, UserData};
    use sg_graphql::exec::{execute_with, ExecRequest};
    use time::OffsetDateTime;

    const SDL: &str = "
        type Query {
            viewer: Viewer!
            viewerCanChangeLibraryItemVisibilityToPublic: Boolean!
        }
        interface Viewer { affiliatedNamespaces: NamespaceConnection! }
        type Visitor implements Viewer { affiliatedNamespaces: NamespaceConnection! }
        type User implements Viewer {
            id: ID!
            username: String!
            namespaceName: String!
            affiliatedNamespaces: NamespaceConnection!
        }
        interface Namespace { id: ID! namespaceName: String! }
        type Org implements Namespace { id: ID! namespaceName: String! }
        type NamespaceConnection {
            nodes: [Namespace!]!
            totalCount: Int!
            pageInfo: PageInfo!
        }
        type PageInfo {
            startCursor: String
            endCursor: String
            hasNextPage: Boolean!
            hasPreviousPage: Boolean!
        }
    ";

    fn user(id: i32, name: &str, site_admin: bool) -> UserObject {
        UserObject::from_data_as_viewer(
            UserData {
                id,
                username: name.to_owned(),
                display_name: String::new(),
                avatar_url: String::new(),
                created_at: OffsetDateTime::UNIX_EPOCH,
                updated_at: OffsetDateTime::UNIX_EPOCH,
                site_admin,
                tos_accepted: false,
                builtin_auth: false,
                scim_controlled: false,
                completions_quota: None,
                code_completions_quota: None,
                primary_email: String::new(),
            },
            site_admin,
        )
    }

    fn org(id: i32, name: &str) -> OrgData {
        OrgData {
            id,
            name: name.to_owned(),
            display_name: None,
            created_at: OffsetDateTime::UNIX_EPOCH,
            viewer_can_administer: false,
            viewer_is_member: true,
        }
    }

    struct FakeStore(Vec<OrgData>);

    impl AffiliatedNamespacesStore for FakeStore {
        fn affiliated_orgs(
            &self,
            _user_id: i32,
            _viewer_uid: i32,
            _viewer_site_admin: bool,
        ) -> Result<Vec<OrgData>, String> {
            Ok(self.0.clone())
        }
    }

    fn execute(
        roots: &FrontendRootResolver,
        query: &str,
        actor: Actor,
    ) -> sg_graphql::exec::ExecResponse {
        let schema = sg_graphql::schema::assemble_sdl(SDL).expect("schema");
        execute_with(
            &schema,
            ExecRequest {
                query,
                operation_name: "",
                variables: &serde_json::Map::new(),
                actor,
            },
            Some(roots),
        )
    }

    #[test]
    fn viewer_lists_the_user_then_their_orgs() {
        let store: ViewerHandle = Arc::new(FakeStore(vec![org(7, "team")]));
        let roots = FrontendRootResolver::with_user(Some(user(1, "alice", false)))
            .with_viewer(Some(store), false);
        let response = execute(
            &roots,
            "{ viewer { affiliatedNamespaces { totalCount nodes { namespaceName } } } }",
            Actor::User { uid: 1 },
        );
        assert_eq!(
            response.data_json().as_deref(),
            Some(
                r#"{"viewer":{"affiliatedNamespaces":{"totalCount":2,"nodes":[{"namespaceName":"alice"},{"namespaceName":"team"}]}}}"#
            )
        );
    }

    #[test]
    fn affiliated_namespaces_require_same_user_or_site_admin() {
        let store: ViewerHandle = Arc::new(FakeStore(vec![]));
        let roots = FrontendRootResolver::with_user(Some(user(1, "alice", false)))
            .with_viewer(Some(store), false);
        let response = execute(
            &roots,
            "{ viewer { affiliatedNamespaces { totalCount } } }",
            Actor::User { uid: 2 },
        );
        assert_eq!(response.errors.len(), 1);
        assert_eq!(
            response.errors[0].message,
            "must be authenticated as the authorized user or site admin"
        );
    }

    #[test]
    fn anonymous_viewer_is_a_visitor_only_in_dotcom_mode() {
        let store: ViewerHandle = Arc::new(FakeStore(vec![]));
        let roots = FrontendRootResolver::new().with_viewer(Some(store), true);
        let response = execute(
            &roots,
            "{ viewer { affiliatedNamespaces { totalCount } } }",
            Actor::Anonymous,
        );
        assert_eq!(
            response.data_json().as_deref(),
            Some(r#"{"viewer":{"affiliatedNamespaces":{"totalCount":0}}}"#)
        );

        let roots = FrontendRootResolver::new()
            .with_viewer(Some(Arc::new(FakeStore(vec![])) as ViewerHandle), false);
        let response = execute(&roots, "{ viewer { __typename } }", Actor::Anonymous);
        assert_eq!(response.errors.len(), 1);
        assert_eq!(
            response.errors[0].message,
            "unauthenticated visitors are forbidden"
        );
    }

    #[test]
    fn visitor_type_is_reported_for_anonymous_viewers() {
        let roots = FrontendRootResolver::new()
            .with_viewer(Some(Arc::new(FakeStore(vec![])) as ViewerHandle), true);
        let response = execute(&roots, "{ viewer { __typename } }", Actor::Anonymous);
        assert_eq!(
            response.data_json().as_deref(),
            Some(r#"{"viewer":{"__typename":"Visitor"}}"#)
        );
    }

    #[test]
    fn viewer_can_change_library_visibility_is_site_admin_only() {
        let roots = FrontendRootResolver::with_user(Some(user(1, "alice", false)))
            .with_viewer(Some(Arc::new(FakeStore(vec![]))), false);
        let response = execute(
            &roots,
            "{ viewerCanChangeLibraryItemVisibilityToPublic }",
            Actor::User { uid: 1 },
        );
        assert_eq!(
            response.data_json().as_deref(),
            Some(r#"{"viewerCanChangeLibraryItemVisibilityToPublic":false}"#)
        );

        let roots = FrontendRootResolver::with_user(Some(user(1, "alice", true)))
            .with_viewer(Some(Arc::new(FakeStore(vec![]))), false);
        let response = execute(
            &roots,
            "{ viewerCanChangeLibraryItemVisibilityToPublic }",
            Actor::User { uid: 1 },
        );
        assert_eq!(
            response.data_json().as_deref(),
            Some(r#"{"viewerCanChangeLibraryItemVisibilityToPublic":true}"#)
        );
    }
}
