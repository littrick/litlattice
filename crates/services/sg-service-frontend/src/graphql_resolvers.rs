#![forbid(unsafe_code)]

//! Business GraphQL resolvers for the frontend schema.
//!
//! Only fields backed by the session/current-user slice are ported here. The
//! executor owns selection merging, nullability and JSON; these objects just
//! answer one field at a time. Unported fields return `Ok(None)` so the
//! executor reports `no ported resolver` instead of fabricating data.
//!
//! `Query.currentUser` is `graphqlbackend.CurrentUser`:
//! `db.Users().GetByCurrentAuthUser(ctx)`, which is `nil` for anonymous actors
//! (`user.go:535-544`). The session middleware supplies
//! [`Actor::User`](sg_graphql::exec::Actor::User) for authenticated requests;
//! [`SchemaGraphqlDeps`](crate::graphql_api::SchemaGraphqlDeps) prefetches the
//! user row and passes it to [`FrontendRootResolver::with_user`].
//!
//! `User.id` is the Relay ID: `relay.MarshalID("User", id)` =
//! `base64.URLEncoding(kind + ":" + json.Marshal(id))` (`relay/relay.go:14-19`).

use std::sync::Arc;

use async_trait::async_trait;
use base64::Engine;
use sg_graphql::exec::{Actor, Arguments, ExecError, FieldValue, ObjectValue, RootResolver};
use sg_graphql::json::Json;

/// The `users` row fields the first DB-backed `User` resolvers read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UserData {
    pub id: i32,
    pub username: String,
    pub display_name: String,
    pub avatar_url: String,
}

/// The database seam for `Query.currentUser` / `User` (`db.Users()`).
#[async_trait]
pub trait UserProfiles: Send + Sync {
    async fn profile(&self, uid: i32) -> Result<Option<UserData>, String>;
}

/// Root objects for the assembled OSS schema.
pub struct FrontendRootResolver {
    query: QueryRoot,
}

impl FrontendRootResolver {
    /// Transport tests without a database: anonymous `currentUser` is `null`;
    /// an authenticated actor has "no ported resolver" because the row cannot
    /// be loaded.
    pub fn new() -> Self {
        Self {
            query: QueryRoot { user: None },
        }
    }

    /// The production resolver: `user` is the prefetched current user, if the
    /// actor is authenticated and the row still exists.
    pub fn with_user(user: Option<UserObject>) -> Self {
        Self {
            query: QueryRoot { user },
        }
    }
}

impl Default for FrontendRootResolver {
    fn default() -> Self {
        Self::new()
    }
}

impl RootResolver for FrontendRootResolver {
    fn root(&self, root_type: &str) -> Option<&dyn ObjectValue> {
        if root_type == self.query.type_name() {
            Some(&self.query)
        } else {
            None
        }
    }
}

/// `schemaResolver` (`cmd/frontend/graphqlbackend/graphqlbackend.go:674`).
struct QueryRoot {
    /// `None` when the actor is anonymous, the row is gone, or no lookup is
    /// available. The `Actor` decides which case applies.
    user: Option<UserObject>,
}

impl ObjectValue for QueryRoot {
    fn type_name(&self) -> &'static str {
        "Query"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            // `CurrentUser` -> `db.Users().GetByCurrentAuthUser`: anonymous
            // actors get `nil`; an authenticated actor gets the prefetched row
            // (or `nil` if it disappeared between authentication and here).
            "currentUser" => match actor {
                Actor::Anonymous => Ok(Some(FieldValue::Null)),
                Actor::User { .. } => match &self.user {
                    Some(user) => Ok(Some(FieldValue::Object(Arc::new(user.clone())))),
                    None => Ok(Some(FieldValue::Null)),
                },
            },
            _ => Ok(None),
        }
    }
}

/// `UserResolver` (`cmd/frontend/graphqlbackend/user.go:122-168`), limited to
/// the fields backed by the current-user row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UserObject {
    data: UserData,
}

impl UserObject {
    pub fn from_data(data: UserData) -> Self {
        Self { data }
    }

    /// `relay.MarshalID("User", id)`: `base64.URLEncoding("User:" + json(id))`.
    pub fn relay_id(id: i32) -> String {
        base64::engine::general_purpose::URL_SAFE.encode(format!("User:{id}"))
    }
}

impl ObjectValue for UserObject {
    fn type_name(&self) -> &'static str {
        "User"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "id" => Ok(Some(FieldValue::Json(Json::Str(Self::relay_id(
                self.data.id,
            ))))),
            "databaseID" => Ok(Some(FieldValue::Json(Json::Int(self.data.id as i64)))),
            "username" => Ok(Some(FieldValue::Json(Json::Str(
                self.data.username.clone(),
            )))),
            // `DisplayName`/`AvatarURL` return nil for the empty string.
            "displayName" => Ok(Some(if self.data.display_name.is_empty() {
                FieldValue::Null
            } else {
                FieldValue::Json(Json::Str(self.data.display_name.clone()))
            })),
            "avatarURL" => Ok(Some(if self.data.avatar_url.is_empty() {
                FieldValue::Null
            } else {
                FieldValue::Json(Json::Str(self.data.avatar_url.clone()))
            })),
            _ => Ok(None),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::Map;
    use sg_graphql::exec::{execute_with, ExecRequest};
    use sg_graphql::schema::assemble_sdl;

    fn schema() -> sg_graphql::Schema {
        assemble_sdl(
            "type Query { currentUser: User other: String } \
             type User { id: ID! databaseID: Int! username: String! \
                         displayName: String avatarURL: String createdAt: DateTime } \
             scalar DateTime",
        )
        .expect("schema")
    }

    fn run(
        schema: &sg_graphql::Schema,
        query: &str,
        actor: Actor,
        roots: &FrontendRootResolver,
    ) -> String {
        let variables = Map::new();
        execute_with(
            schema,
            ExecRequest {
                query,
                operation_name: "",
                variables: &variables,
                actor,
            },
            Some(roots),
        )
        .to_json()
    }

    fn user() -> UserObject {
        UserObject::from_data(UserData {
            id: 1,
            username: "alice".to_owned(),
            display_name: "Alice A.".to_owned(),
            avatar_url: "https://example.com/a.png".to_owned(),
        })
    }

    #[test]
    fn anonymous_current_user_is_null() {
        let schema = schema();
        assert_eq!(
            run(
                &schema,
                "{ currentUser { id username } }",
                Actor::Anonymous,
                &FrontendRootResolver::new()
            ),
            r#"{"data":{"currentUser":null}}"#
        );
    }

    #[test]
    fn authenticated_current_user_without_row_is_null() {
        // `GetByCurrentAuthUser` maps not-found to `(nil, nil)`.
        let schema = schema();
        assert_eq!(
            run(
                &schema,
                "{ currentUser { id } }",
                Actor::User { uid: 1 },
                &FrontendRootResolver::new()
            ),
            r#"{"data":{"currentUser":null}}"#
        );
    }

    #[test]
    fn authenticated_current_user_resolves_basic_fields() {
        let schema = schema();
        assert_eq!(
            run(
                &schema,
                "{ currentUser { id databaseID username displayName avatarURL } }",
                Actor::User { uid: 1 },
                &FrontendRootResolver::with_user(Some(user()))
            ),
            r#"{"data":{"currentUser":{"id":"VXNlcjox","databaseID":1,"username":"alice","displayName":"Alice A.","avatarURL":"https://example.com/a.png"}}}"#
        );
    }

    #[test]
    fn empty_display_name_and_avatar_are_null() {
        let schema = schema();
        let user = UserObject::from_data(UserData {
            id: 123,
            username: "bob".to_owned(),
            display_name: String::new(),
            avatar_url: String::new(),
        });
        assert_eq!(
            run(
                &schema,
                "{ currentUser { id displayName avatarURL } }",
                Actor::User { uid: 123 },
                &FrontendRootResolver::with_user(Some(user))
            ),
            r#"{"data":{"currentUser":{"id":"VXNlcjoxMjM=","displayName":null,"avatarURL":null}}}"#
        );
    }

    #[test]
    fn user_created_at_is_unported() {
        let schema = schema();
        assert_eq!(
            run(
                &schema,
                "{ currentUser { createdAt } }",
                Actor::User { uid: 1 },
                &FrontendRootResolver::with_user(Some(user()))
            ),
            r#"{"errors":[{"message":"GraphQL field \"createdAt\" of \"User\" has no ported resolver","path":["currentUser","createdAt"]}],"data":{"currentUser":{"createdAt":null}}}"#
        );
    }

    #[test]
    fn unported_root_fields_report_their_name() {
        let schema = schema();
        assert_eq!(
            run(
                &schema,
                "{ other }",
                Actor::Anonymous,
                &FrontendRootResolver::new()
            ),
            r#"{"errors":[{"message":"GraphQL field \"other\" of \"Query\" has no ported resolver","path":["other"]}],"data":{"other":null}}"#
        );
    }

    #[test]
    fn relay_id_matches_go() {
        // `base64.URLEncoding.EncodeToString([]byte("User:1"))`.
        assert_eq!(UserObject::relay_id(1), "VXNlcjox");
        assert_eq!(UserObject::relay_id(123), "VXNlcjoxMjM=");
    }
}
