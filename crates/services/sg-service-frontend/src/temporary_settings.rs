#![forbid(unsafe_code)]

//! `Query.temporarySettings` and the two temporary-setting mutations
//! (`cmd/frontend/graphqlbackend/temporary_settings.go:1-52`).
//!
//! The executor resolves fields synchronously while the database is async, so
//! the read runs in [`prefetch_temporary_settings`] and the writes in
//! [`prefetch_temporary_settings_mutations`] before execution; the resolver
//! objects only replay the outcome. All three fields require an authenticated
//! actor and report `not authenticated` otherwise.

use sg_graphql::exec::{Arguments, FieldValue, ObjectValue, RootField};
use sg_graphql::json::Json;
use sqlx::PgPool;

/// `errors.New("not authenticated")` (`temporary_settings.go:22,34,45`).
pub const NOT_AUTHENTICATED: &str = "not authenticated";

/// `Query.temporarySettings`: `None` when the operation does not select the
/// field; otherwise the stored jsonb text (`{}` when the user has no row) or
/// the lookup error.
pub async fn prefetch_temporary_settings(
    pool: &PgPool,
    uid: i32,
    fields: &[RootField],
) -> Option<Result<String, String>> {
    if !fields.iter().any(|field| field.name == "temporarySettings") {
        return None;
    }
    if uid == 0 {
        return Some(Err(NOT_AUTHENTICATED.to_owned()));
    }
    Some(sg_db::temporary_settings::get_temporary_settings(pool, uid).await)
}

/// The two temporary-setting mutations, executed in document order. Repeated
/// occurrences of the same field name each run (the baseline resolves every
/// occurrence), but the `MutationRoot` slot keeps the last outcome; the corpus
/// does not repeat them.
pub async fn prefetch_temporary_settings_mutations(
    pool: &PgPool,
    uid: i32,
    fields: &[RootField],
) -> Vec<(String, Result<(), String>)> {
    let mut outcomes = Vec::new();
    for field in fields {
        let outcome = match field.name.as_str() {
            "overwriteTemporarySettings" => {
                overwrite(pool, uid, field.arguments.string("contents")).await
            }
            "editTemporarySettings" => {
                edit(pool, uid, field.arguments.string("settingsToEdit")).await
            }
            _ => continue,
        };
        outcomes.push((field.name.clone(), outcome));
    }
    outcomes
}

async fn overwrite(pool: &PgPool, uid: i32, contents: Option<&str>) -> Result<(), String> {
    if uid == 0 {
        return Err(NOT_AUTHENTICATED.to_owned());
    }
    let Some(contents) = contents else {
        return Err("missing contents argument".to_owned());
    };
    sg_db::temporary_settings::overwrite_temporary_settings(pool, uid, contents)
        .await
        .map_err(|error| error.to_string())
}

async fn edit(pool: &PgPool, uid: i32, settings_to_edit: Option<&str>) -> Result<(), String> {
    if uid == 0 {
        return Err(NOT_AUTHENTICATED.to_owned());
    }
    let Some(settings_to_edit) = settings_to_edit else {
        return Err("missing settingsToEdit argument".to_owned());
    };
    sg_db::temporary_settings::edit_temporary_settings(pool, uid, settings_to_edit)
        .await
        .map_err(|error| error.to_string())
}

/// `TemporarySettingsResolver` (`temporary_settings.go:10-16`).
pub struct TemporarySettingsObject {
    pub contents: String,
}

impl ObjectValue for TemporarySettingsObject {
    fn type_name(&self) -> &'static str {
        "TemporarySettings"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: sg_graphql::exec::Actor,
    ) -> Result<Option<FieldValue>, sg_graphql::exec::ExecError> {
        match field {
            "contents" => Ok(Some(FieldValue::Json(Json::Str(self.contents.clone())))),
            _ => Ok(None),
        }
    }
}

/// `&EmptyResponse{}`: the two mutations' return type is non-null, so a
/// successful write must produce an object, not GraphQL `null`.
pub struct EmptyResponseObject;

impl ObjectValue for EmptyResponseObject {
    fn type_name(&self) -> &'static str {
        "EmptyResponse"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: sg_graphql::exec::Actor,
    ) -> Result<Option<FieldValue>, sg_graphql::exec::ExecError> {
        match field {
            // `func (*EmptyResponse) AlwaysNil() *string { return nil }`.
            "alwaysNil" => Ok(Some(FieldValue::Null)),
            _ => Ok(None),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sg_graphql::exec::Actor;

    #[test]
    fn objects_expose_the_baseline_fields() {
        let settings = TemporarySettingsObject {
            contents: "{\"a\": 1}".to_owned(),
        };
        assert_eq!(settings.type_name(), "TemporarySettings");
        assert!(matches!(
            settings
                .field("contents", &Arguments::default(), Actor::Anonymous)
                .expect("contents"),
            Some(FieldValue::Json(Json::Str(contents))) if contents == "{\"a\": 1}"
        ));

        let empty = EmptyResponseObject;
        assert_eq!(empty.type_name(), "EmptyResponse");
        assert!(matches!(
            empty
                .field("alwaysNil", &Arguments::default(), Actor::Anonymous)
                .expect("alwaysNil"),
            Some(FieldValue::Null)
        ));
    }

    #[tokio::test]
    async fn unauthenticated_actors_are_rejected_without_database_access() {
        // The pool is unreachable on purpose: the auth check must run first.
        let pool = PgPool::connect_lazy("postgres://invalid:invalid@127.0.0.1:1/none").unwrap();
        let fields = vec![RootField {
            name: "overwriteTemporarySettings".to_owned(),
            arguments: Arguments::default(),
            selections: Vec::new(),
            type_conditions: Vec::new(),
        }];
        let read = vec![RootField {
            name: "temporarySettings".to_owned(),
            ..fields[0].clone()
        }];
        assert_eq!(
            prefetch_temporary_settings(&pool, 0, &read).await,
            Some(Err(NOT_AUTHENTICATED.to_owned()))
        );

        let outcomes = prefetch_temporary_settings_mutations(&pool, 0, &fields).await;
        assert_eq!(
            outcomes,
            vec![(
                "overwriteTemporarySettings".to_owned(),
                Err(NOT_AUTHENTICATED.to_owned())
            )]
        );
    }
}
