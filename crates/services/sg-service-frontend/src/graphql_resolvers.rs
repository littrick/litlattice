#![forbid(unsafe_code)]

//! Business GraphQL resolvers for the frontend schema.
//!
//! Only fields whose behavior does not require the database are ported here.
//! The executor owns selection merging, nullability and JSON; these objects
//! just answer one field at a time. Unported fields return `Ok(None)` so the
//! executor reports `no ported resolver` instead of fabricating data.
//!
//! The HTTP transport has no session middleware yet, so requests reach the
//! resolver as [`Actor::Anonymous`]; `Query.currentUser` is therefore `null`
//! (matching `graphqlbackend.CurrentUser` for an anonymous actor).

use sg_graphql::exec::{Actor, Arguments, ExecError, FieldValue, ObjectValue, RootResolver};

/// Root objects for the assembled OSS schema.
pub struct FrontendRootResolver {
    query: QueryRoot,
}

impl FrontendRootResolver {
    pub fn new() -> Self {
        Self { query: QueryRoot }
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
struct QueryRoot;

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
            // actors get `nil` (`user.go:535-544`). The authenticated lookup
            // needs the database and is not ported yet.
            "currentUser" => match actor {
                Actor::Anonymous => Ok(Some(FieldValue::Null)),
                Actor::User { .. } => Ok(None),
            },
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
             type User { id: ID! username: String! }",
        )
        .expect("schema")
    }

    fn run(schema: &sg_graphql::Schema, query: &str, actor: Actor) -> String {
        let variables = Map::new();
        execute_with(
            schema,
            ExecRequest {
                query,
                operation_name: "",
                variables: &variables,
                actor,
            },
            Some(&FrontendRootResolver::new()),
        )
        .to_json()
    }

    #[test]
    fn anonymous_current_user_is_null() {
        let schema = schema();
        assert_eq!(
            run(&schema, "{ currentUser { id username } }", Actor::Anonymous),
            r#"{"data":{"currentUser":null}}"#
        );
    }

    #[test]
    fn authenticated_current_user_is_unported() {
        let schema = schema();
        assert_eq!(
            run(&schema, "{ currentUser { id } }", Actor::User { uid: 1 }),
            r#"{"errors":[{"message":"GraphQL field \"currentUser\" of \"Query\" has no ported resolver","path":["currentUser"]}],"data":{"currentUser":null}}"#
        );
    }

    #[test]
    fn unported_root_fields_report_their_name() {
        let schema = schema();
        assert_eq!(
            run(&schema, "{ other }", Actor::Anonymous),
            r#"{"errors":[{"message":"GraphQL field \"other\" of \"Query\" has no ported resolver","path":["other"]}],"data":{"other":null}}"#
        );
    }
}
