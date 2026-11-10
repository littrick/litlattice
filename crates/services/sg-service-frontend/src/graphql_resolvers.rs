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

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use base64::Engine;
use sg_graphql::exec::{
    Actor, Arguments, ExecError, FieldValue, ObjectValue, RootField, RootResolver,
};
use sg_graphql::json::Json;
use time::{OffsetDateTime, UtcOffset};

/// The `users` row fields the DB-backed `User` resolvers read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UserData {
    pub id: i32,
    pub username: String,
    pub display_name: String,
    pub avatar_url: String,
    pub created_at: OffsetDateTime,
    pub updated_at: OffsetDateTime,
    pub site_admin: bool,
    pub tos_accepted: bool,
}

/// `gqlutil.DateTime.MarshalJSON`: `json.Marshal(t.UTC().Format(time.RFC3339))`
/// — UTC, seconds precision, no fractional part.
fn datetime_rfc3339(t: OffsetDateTime) -> String {
    let t = t.to_offset(UtcOffset::UTC);
    format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}Z",
        t.year(),
        t.month() as u8,
        t.day(),
        t.hour(),
        t.minute(),
        t.second()
    )
}

/// The database seam for `Query.currentUser` / `User` (`db.Users()`).
#[async_trait]
pub trait UserProfiles: Send + Sync {
    async fn profile(&self, uid: i32) -> Result<Option<UserData>, String>;
}

/// The `repo` row fields the `Repository` resolvers read (one
/// `repoStore.GetByName` row).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RepositoryData {
    pub id: i32,
    pub name: String,
    pub uri: String,
    pub description: String,
    pub fork: bool,
    pub archived: bool,
    pub private: bool,
    pub stars: i32,
    pub external_id: String,
    pub external_service_type: String,
    pub external_service_id: String,
}

/// The result of prefetching one `Query.repository` argument set.
#[derive(Debug, Clone, PartialEq)]
pub enum RepoLookup {
    Found(RepositoryData),
    /// `errcode.IsNotFound` → GraphQL `null`.
    NotFound,
    /// `BlockedRepoError` → GraphQL error (the reason is part of the message).
    Blocked {
        name: String,
        reason: String,
    },
    /// A database failure, surfaced as the resolver error.
    Error(String),
}

/// `Query.repository`'s lookup key. Name and URI are queried separately so a
/// name match is preferred (`repoStore.GetByName`).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum RepoKey {
    Name(String),
    Uri(String),
}

impl RepoKey {
    fn value(&self) -> &str {
        match self {
            RepoKey::Name(value) | RepoKey::Uri(value) => value,
        }
    }
}

/// The database seam for `Query.repository` (`backend.Repos().GetByName` with
/// the repository permission predicate).
#[async_trait]
pub trait RepositoryLookup: Send + Sync {
    async fn lookup(
        &self,
        name_or_uri: &str,
        authenticated_user_id: i32,
        bypass_authz: bool,
    ) -> Result<RepoLookup, String>;
}

/// What `Query.repository` arguments ask for.
enum RepositoryArg {
    Key(RepoKey),
    /// `cloneURL` needs code-host configuration (`cloneurls`), not ported.
    CloneUrl,
    /// Neither `name`/`uri` nor `cloneURL`: the baseline errors.
    Missing,
}

/// `schemaResolver.Repository`: `uri` is a deprecated alias for `name` when
/// `name` is absent (`graphqlbackend.go:847-856`).
fn repository_arg(arguments: &Arguments) -> RepositoryArg {
    if let Some(name) = arguments.string("name") {
        return RepositoryArg::Key(RepoKey::Name(name.to_owned()));
    }
    if let Some(uri) = arguments.string("uri") {
        return RepositoryArg::Key(RepoKey::Uri(uri.to_owned()));
    }
    if arguments.string("cloneURL").is_some() {
        return RepositoryArg::CloneUrl;
    }
    RepositoryArg::Missing
}

/// Look up every `Query.repository` root field in `fields`, deduplicated by
/// key. `actor_site_admin` is the authenticated actor's `users.site_admin`
/// (false for anonymous actors); site admins bypass repository permissions
/// unless `authz.enforceForSiteAdmins` is enabled (not read here).
pub async fn prefetch_repositories(
    store: &dyn RepositoryLookup,
    fields: &[RootField],
    actor: Actor,
    actor_site_admin: bool,
) -> HashMap<RepoKey, RepoLookup> {
    let authenticated_user_id = actor.uid().unwrap_or(0);
    let bypass_authz = actor.is_authenticated() && actor_site_admin;
    let mut repositories = HashMap::new();
    for field in fields {
        if field.name != "repository" {
            continue;
        }
        let RepositoryArg::Key(key) = repository_arg(&field.arguments) else {
            continue;
        };
        if repositories.contains_key(&key) {
            continue;
        }
        let lookup = match store
            .lookup(key.value(), authenticated_user_id, bypass_authz)
            .await
        {
            Ok(lookup) => lookup,
            Err(error) => RepoLookup::Error(error),
        };
        repositories.insert(key, lookup);
    }
    repositories
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
            query: QueryRoot {
                user: None,
                repositories: HashMap::new(),
            },
        }
    }

    /// The production resolver: `user` is the prefetched current user, if the
    /// actor is authenticated and the row still exists.
    pub fn with_user(user: Option<UserObject>) -> Self {
        Self {
            query: QueryRoot {
                user,
                repositories: HashMap::new(),
            },
        }
    }

    /// The production resolver with the prefetched `Query.repository` rows.
    pub fn with_user_and_repositories(
        user: Option<UserObject>,
        repositories: HashMap<RepoKey, RepoLookup>,
    ) -> Self {
        Self {
            query: QueryRoot { user, repositories },
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
    /// Prefetched `Query.repository` results, keyed by the looked-up name/URI.
    repositories: HashMap<RepoKey, RepoLookup>,
}

impl ObjectValue for QueryRoot {
    fn type_name(&self) -> &'static str {
        "Query"
    }

    fn field(
        &self,
        field: &str,
        arguments: &Arguments,
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
            // `schemaResolver.Repository` -> `RepositoryRedirect`: a missing
            // repository is `null`; a blocked repository is an error.
            "repository" => match repository_arg(arguments) {
                RepositoryArg::Key(key) => match self.repositories.get(&key) {
                    Some(RepoLookup::Found(data)) => Ok(Some(FieldValue::Object(Arc::new(
                        RepositoryObject::from_data(data.clone()),
                    )))),
                    Some(RepoLookup::NotFound) => Ok(Some(FieldValue::Null)),
                    Some(RepoLookup::Blocked { name, reason }) => Err(ExecError::new(format!(
                        "repository {name} has been blocked. reason: {reason}"
                    ))),
                    Some(RepoLookup::Error(message)) => Err(ExecError::new(message.clone())),
                    // No prefetch was performed for this request (e.g. the
                    // transport-only resolver); report the field as unported.
                    None => Ok(None),
                },
                RepositoryArg::CloneUrl => Ok(None),
                RepositoryArg::Missing => Err(ExecError::new("neither name nor cloneURL given")),
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
        actor: Actor,
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
            // `UserResolver.URL`: `"/users/" + user.Username`.
            "url" => Ok(Some(FieldValue::Json(Json::Str(format!(
                "/users/{}",
                self.data.username
            ))))),
            // `SettingsURL` is `url() + "/settings"` and is always non-nil.
            "settingsURL" => Ok(Some(FieldValue::Json(Json::Str(format!(
                "/users/{}/settings",
                self.data.username
            ))))),
            "createdAt" => Ok(Some(FieldValue::Json(Json::Str(datetime_rfc3339(
                self.data.created_at,
            ))))),
            // `UpdatedAt` always returns a non-nil DateTime (the users column
            // is NOT NULL), although the schema field is nullable.
            "updatedAt" => Ok(Some(FieldValue::Json(Json::Str(datetime_rfc3339(
                self.data.updated_at,
            ))))),
            // `SiteAdmin` checks `CheckSiteAdminOrSameUserFromActor`. The
            // same-user branch needs no database; the site-admin branch does,
            // so another actor's view stays unported.
            "siteAdmin" => match actor {
                Actor::User { uid } if uid == self.data.id => {
                    Ok(Some(FieldValue::Json(Json::Bool(self.data.site_admin))))
                }
                _ => Ok(None),
            },
            "tosAccepted" => Ok(Some(FieldValue::Json(Json::Bool(self.data.tos_accepted)))),
            _ => Ok(None),
        }
    }
}

/// `RepositoryResolver` (`cmd/frontend/graphqlbackend/repository.go`), limited
/// to the fields present in the row returned by `repoStore.GetByName`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RepositoryObject {
    data: RepositoryData,
}

impl RepositoryObject {
    pub fn from_data(data: RepositoryData) -> Self {
        Self { data }
    }

    /// `relay.MarshalID("Repository", id)`:
    /// `base64.URLEncoding("Repository:" + json(id))`.
    pub fn relay_id(id: i32) -> String {
        base64::engine::general_purpose::URL_SAFE.encode(format!("Repository:{id}"))
    }
}

impl ObjectValue for RepositoryObject {
    fn type_name(&self) -> &'static str {
        "Repository"
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
            "name" => Ok(Some(FieldValue::Json(Json::Str(self.data.name.clone())))),
            "uri" => Ok(Some(FieldValue::Json(Json::Str(self.data.uri.clone())))),
            "description" => Ok(Some(FieldValue::Json(Json::Str(
                self.data.description.clone(),
            )))),
            "isFork" => Ok(Some(FieldValue::Json(Json::Bool(self.data.fork)))),
            "isArchived" => Ok(Some(FieldValue::Json(Json::Bool(self.data.archived)))),
            "isPrivate" => Ok(Some(FieldValue::Json(Json::Bool(self.data.private)))),
            "stars" => Ok(Some(FieldValue::Json(Json::Int(i64::from(
                self.data.stars,
            ))))),
            // `RepositoryResolver.URL` = `(&url.URL{Path: "/" + name}).String()`.
            "url" => Ok(Some(FieldValue::Json(Json::Str(go_url_path(&format!(
                "/{}",
                self.data.name
            )))))),
            // `UpdatedAt` always returns nil.
            "updatedAt" => Ok(Some(FieldValue::Null)),
            "enabled" => Ok(Some(FieldValue::Json(Json::Bool(true)))),
            // `SourceType`: `external_service_type == "perforce"` is a depot.
            "sourceType" => Ok(Some(FieldValue::Json(Json::Str(
                if self.data.external_service_type == "perforce" {
                    "PERFORCE_DEPOT"
                } else {
                    "GIT_REPOSITORY"
                }
                .to_owned(),
            )))),
            "externalRepository" => Ok(Some(FieldValue::Object(Arc::new(
                ExternalRepositoryObject {
                    id: self.data.external_id.clone(),
                    service_type: self.data.external_service_type.clone(),
                    service_id: self.data.external_service_id.clone(),
                },
            )))),
            _ => Ok(None),
        }
    }
}

/// `api.ExternalRepoSpec` (`ExternalRepository` in the GraphQL schema).
struct ExternalRepositoryObject {
    id: String,
    service_type: String,
    service_id: String,
}

impl ObjectValue for ExternalRepositoryObject {
    fn type_name(&self) -> &'static str {
        "ExternalRepository"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "id" => Ok(Some(FieldValue::Json(Json::Str(self.id.clone())))),
            "serviceType" => Ok(Some(FieldValue::Json(Json::Str(self.service_type.clone())))),
            "serviceID" => Ok(Some(FieldValue::Json(Json::Str(self.service_id.clone())))),
            _ => Ok(None),
        }
    }
}

/// Go's `url.URL{Path: path}.String()` path escaping (`encodePath` mode):
/// unreserved characters and `/` plus the path-allowed reserved characters
/// stay literal; `?`, `#`, `%`, spaces and non-ASCII bytes become uppercase
/// `%XX` escapes.
fn go_url_path(path: &str) -> String {
    let mut escaped = String::with_capacity(path.len());
    for byte in path.bytes() {
        let literal = byte.is_ascii_alphanumeric()
            || matches!(
                byte,
                b'-' | b'_'
                    | b'.'
                    | b'~'
                    | b'$'
                    | b'&'
                    | b'+'
                    | b','
                    | b'/'
                    | b':'
                    | b';'
                    | b'='
                    | b'@'
            );
        if literal {
            escaped.push(char::from(byte));
        } else {
            escaped.push_str(&format!("%{byte:02X}"));
        }
    }
    escaped
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
                         displayName: String avatarURL: String \
                         url: String! settingsURL: String \
                         createdAt: DateTime! updatedAt: DateTime \
                         siteAdmin: Boolean! tosAccepted: Boolean! \
                         builtinAuth: Boolean! } \
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

    fn at(s: &str) -> OffsetDateTime {
        use time::format_description::well_known::Rfc3339;
        OffsetDateTime::parse(s, &Rfc3339).unwrap()
    }

    fn user() -> UserObject {
        UserObject::from_data(UserData {
            id: 1,
            username: "alice".to_owned(),
            display_name: "Alice A.".to_owned(),
            avatar_url: "https://example.com/a.png".to_owned(),
            created_at: at("2026-01-02T03:04:05.123456789Z"),
            updated_at: at("2026-02-03T04:05:06+02:00"),
            site_admin: true,
            tos_accepted: true,
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
            created_at: at("2026-01-02T03:04:05Z"),
            updated_at: at("2026-01-02T03:04:05Z"),
            site_admin: false,
            tos_accepted: false,
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
    fn user_url_created_updated_and_flags_resolve() {
        let schema = schema();
        // `DateTime.MarshalJSON` is UTC RFC3339 without fractional seconds; the
        // `updatedAt` corpus value is +02:00 and must be converted.
        assert_eq!(
            run(
                &schema,
                "{ currentUser { url settingsURL createdAt updatedAt siteAdmin tosAccepted } }",
                Actor::User { uid: 1 },
                &FrontendRootResolver::with_user(Some(user()))
            ),
            r#"{"data":{"currentUser":{"url":"/users/alice","settingsURL":"/users/alice/settings","createdAt":"2026-01-02T03:04:05Z","updatedAt":"2026-02-03T02:05:06Z","siteAdmin":true,"tosAccepted":true}}}"#
        );
    }

    #[test]
    fn site_admin_for_another_actor_is_unported() {
        // The baseline would query the database to see whether this actor is a
        // site admin; only the same-user branch is ported.
        let schema = schema();
        assert_eq!(
            run(
                &schema,
                "{ currentUser { siteAdmin } }",
                Actor::User { uid: 2 },
                &FrontendRootResolver::with_user(Some(user()))
            ),
            r#"{"errors":[{"message":"GraphQL field \"siteAdmin\" of \"User\" has no ported resolver","path":["currentUser","siteAdmin"]}],"data":{"currentUser":null}}"#
        );
    }

    #[test]
    fn builtin_auth_is_unported() {
        let schema = schema();
        assert_eq!(
            run(
                &schema,
                "{ currentUser { builtinAuth } }",
                Actor::User { uid: 1 },
                &FrontendRootResolver::with_user(Some(user()))
            ),
            r#"{"errors":[{"message":"GraphQL field \"builtinAuth\" of \"User\" has no ported resolver","path":["currentUser","builtinAuth"]}],"data":{"currentUser":null}}"#
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
        assert_eq!(RepositoryObject::relay_id(42), "UmVwb3NpdG9yeTo0Mg==");
    }

    fn repository_schema() -> sg_graphql::Schema {
        assemble_sdl(
            "type Query { \
                 repository(name: String, uri: String, cloneURL: String): Repository \
             } \
             type Repository { \
                 id: ID! name: String! uri: String! description: String! \
                 isFork: Boolean! isArchived: Boolean! isPrivate: Boolean! \
                 stars: Int! url: String! updatedAt: DateTime enabled: Boolean! \
                 sourceType: RepositoryType! externalRepository: ExternalRepository! \
             } \
             type ExternalRepository { id: String! serviceType: String! serviceID: String! } \
             enum RepositoryType { GIT_REPOSITORY PERFORCE_DEPOT } \
             scalar DateTime",
        )
        .expect("schema")
    }

    fn repository_data(name: &str) -> RepositoryData {
        RepositoryData {
            id: 42,
            name: name.to_owned(),
            uri: "github.com/a/b".to_owned(),
            description: "A repository".to_owned(),
            fork: true,
            archived: false,
            private: true,
            stars: 7,
            external_id: "ext-1".to_owned(),
            external_service_type: "github".to_owned(),
            external_service_id: "https://github.com/".to_owned(),
        }
    }

    struct FakeRepositories(HashMap<String, RepoLookup>);

    #[async_trait]
    impl RepositoryLookup for FakeRepositories {
        async fn lookup(
            &self,
            name_or_uri: &str,
            _authenticated_user_id: i32,
            _bypass_authz: bool,
        ) -> Result<RepoLookup, String> {
            Ok(self
                .0
                .get(name_or_uri)
                .cloned()
                .unwrap_or(RepoLookup::NotFound))
        }
    }

    async fn run_repository(query: &str, actor: Actor, lookups: &[(&str, RepoLookup)]) -> String {
        let schema = repository_schema();
        let variables = Map::new();
        let request = ExecRequest {
            query,
            operation_name: "",
            variables: &variables,
            actor,
        };
        let fields = sg_graphql::exec::root_fields(&schema, &request);
        let store = FakeRepositories(
            lookups
                .iter()
                .map(|(key, value)| ((*key).to_owned(), value.clone()))
                .collect(),
        );
        let repositories = prefetch_repositories(&store, &fields, actor, false).await;
        let roots = FrontendRootResolver::with_user_and_repositories(None, repositories);
        execute_with(&schema, request, Some(&roots)).to_json()
    }

    #[tokio::test]
    async fn repository_fields_resolve() {
        let lookups = [(
            "github.com/a/b",
            RepoLookup::Found(repository_data("github.com/a/b")),
        )];
        assert_eq!(
            run_repository(
                "{ repository(name: \"github.com/a/b\") { id name uri description isFork \
                  isArchived isPrivate stars url updatedAt enabled sourceType \
                  externalRepository { id serviceType serviceID } } }",
                Actor::Anonymous,
                &lookups,
            )
            .await,
            r#"{"data":{"repository":{"id":"UmVwb3NpdG9yeTo0Mg==","name":"github.com/a/b","uri":"github.com/a/b","description":"A repository","isFork":true,"isArchived":false,"isPrivate":true,"stars":7,"url":"/github.com/a/b","updatedAt":null,"enabled":true,"sourceType":"GIT_REPOSITORY","externalRepository":{"id":"ext-1","serviceType":"github","serviceID":"https://github.com/"}}}}"#
        );
    }

    #[tokio::test]
    async fn repository_uri_alias_and_url_escaping() {
        // `uri` is the deprecated alias for `name`; only when `name` is absent.
        let lookups = [("m9/uri", RepoLookup::Found(repository_data("a b/c?d")))];
        assert_eq!(
            run_repository(
                "{ repository(uri: \"m9/uri\") { name url } }",
                Actor::Anonymous,
                &lookups,
            )
            .await,
            r#"{"data":{"repository":{"name":"a b/c?d","url":"/a%20b/c%3Fd"}}}"#
        );
    }

    #[tokio::test]
    async fn missing_and_blocked_repositories() {
        assert_eq!(
            run_repository(
                "{ repository(name: \"r/x\") { name } }",
                Actor::Anonymous,
                &[],
            )
            .await,
            r#"{"data":{"repository":null}}"#
        );
        let blocked = [(
            "r/x",
            RepoLookup::Blocked {
                name: "r/x".to_owned(),
                reason: "dmca".to_owned(),
            },
        )];
        assert_eq!(
            run_repository(
                "{ repository(name: \"r/x\") { name } }",
                Actor::Anonymous,
                &blocked,
            )
            .await,
            r#"{"errors":[{"message":"repository r/x has been blocked. reason: dmca","path":["repository"]}],"data":{"repository":null}}"#
        );
    }

    #[tokio::test]
    async fn clone_url_and_missing_arguments() {
        assert_eq!(
            run_repository(
                "{ repository(cloneURL: \"https://example.com/r.git\") { name } }",
                Actor::Anonymous,
                &[],
            )
            .await,
            r#"{"errors":[{"message":"GraphQL field \"repository\" of \"Query\" has no ported resolver","path":["repository"]}],"data":{"repository":null}}"#
        );
        assert_eq!(
            run_repository("{ repository { name } }", Actor::Anonymous, &[]).await,
            r#"{"errors":[{"message":"neither name nor cloneURL given","path":["repository"]}],"data":{"repository":null}}"#
        );
    }

    #[tokio::test]
    async fn perforce_repositories_have_a_depot_source_type() {
        let mut data = repository_data("depot/main");
        data.external_service_type = "perforce".to_owned();
        let lookups = [("depot/main", RepoLookup::Found(data))];
        assert_eq!(
            run_repository(
                "{ repository(name: \"depot/main\") { sourceType } }",
                Actor::Anonymous,
                &lookups,
            )
            .await,
            r#"{"data":{"repository":{"sourceType":"PERFORCE_DEPOT"}}}"#
        );
    }

    #[test]
    fn go_url_path_matches_url_url_string() {
        assert_eq!(go_url_path("/github.com/a/b"), "/github.com/a/b");
        // `$&+,/:;=@` stay literal in Go's path mode; `?` and `#` escape.
        assert_eq!(
            go_url_path("/a$&+,/:;=@?b#c%d e"),
            "/a$&+,/:;=@%3Fb%23c%25d%20e"
        );
        assert_eq!(go_url_path("/ä"), "/%C3%A4");
    }
}
