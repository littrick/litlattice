#![forbid(unsafe_code)]

//! The GraphQL HTTP transport (`serveGraphQL`,
//! `cmd/frontend/internal/httpapi/graphql.go:106-233`).
//!
//! This module ports everything around schema execution: the `JsonMiddleware`
//! content type, the `ErrorHandler` error contract, gzip request bodies,
//! `encoding/json` decoding of `graphQLQueryParams`, the query cost limits
//! (400 with `ErrQueryComplexityLimitExceeded`), the rate-limit response (429
//! with `Retry-After`), and the final `schema.Exec` JSON write.
//!
//! Schema assembly, validation and resolvers are **not** part of this slice.
//! They sit behind [`GraphqlDeps::exec`]; the production wiring
//! ([`UnportedGraphqlDeps`]) answers with a GraphQL error until the schema
//! executor is ported. The rate limiter (`BasicLimitWatcher` +
//! `GCRARateLimiterCtx`) is likewise a seam: [`GraphqlDeps::rate_limit`]
//! returns `None` (watcher disabled) in production, while the differential
//! harness injects a fake to verify the 429 byte contract.

use std::collections::BTreeMap;
use std::fmt;
use std::future::poll_fn;
use std::io::Read;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use flate2::read::GzDecoder;
use http::header::{self, HeaderValue};
use http::{HeaderMap, Request, Response, StatusCode};
use hyper::body::Body;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use tonic::body::BoxBody;

use sg_config::{LiveConfig, SiteConfiguration};
use sg_graphql::exec::{Actor, ExecError, ExecResponse, PathSegment};
use sg_graphql::json::Json;
use sg_graphql::{estimate_query_cost, QueryCost};
use sg_session::auth::{self, SessionAuthOutcome, SessionStore, SessionUserLookup};
use sg_session::{AnonymousReason, COOKIE_NAME};

/// `HTTAPI_GZIP_FILE_SIZE_LIMIT` default (`graphql.go:35`): 500 MiB of
/// decompressed request body.
const GZIP_FILE_SIZE_LIMIT: u64 = 500 * 1024 * 1024;

/// `graphql.go:81`: `docs_url` fallback when `conf.ExternalURL()` cannot be
/// parsed.
const FALLBACK_DOCS_URL: &str = "https://sourcegraph.com/help/api/graphql#cost-limits";

/// `writeViolationError`'s extension code (`graphql.go:91`).
const COMPLEXITY_CODE: &str = "ErrQueryComplexityLimitExceeded";

/// `rate_limit.go`'s disabled-watcher sentinel: the harness injects the
/// enabled/fake variants.
pub const UNKNOWN_UID: &str = "unknown";

/// The header value `session.CookieMiddlewareWithCSRFSafety` adds to every
/// external API response (`session.go:334`).
const SESSION_VARY: &str = "Cookie, Authorization, X-Requested-With";

// ---------------------------------------------------------------------------
// Wire models (graph-gophers `graphql.Response` / `errors.QueryError`)
// ---------------------------------------------------------------------------

/// `errors.Location` (`graph-gophers/graphql-go/errors/errors.go:17-20`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Location {
    pub line: i64,
    pub column: i64,
}

/// `errors.QueryError` (`graphql-go/errors/errors.go:7-15`). The unexported
/// `Err`/`Rule`/`ResolverError` fields are not serialized by the baseline.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
pub struct QueryError {
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub locations: Option<Vec<Location>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub path: Option<Vec<Value>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub extensions: Option<Map<String, Value>>,
}

impl QueryError {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            ..Self::default()
        }
    }
}

/// `graphql.Response` (`graph-gophers/graphql-go/graphql.go:175-179`):
/// `errors,omitempty`, `data,omitempty` (a `json.RawMessage`), then
/// `extensions,omitempty`. `data` stays raw: the baseline never re-marshals
/// it, so its selection order and escaping belong to the executor.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct GraphqlResponse {
    pub errors: Vec<QueryError>,
    pub data: Option<String>,
    pub extensions: Option<Map<String, Value>>,
}

impl GraphqlResponse {
    /// Map an [`ExecResponse`] onto the wire shape. Only non-empty
    /// `locations`/`path` are serialized (`omitempty`).
    pub fn from_exec(response: &ExecResponse) -> Self {
        Self {
            errors: response.errors.iter().map(query_error_from_exec).collect(),
            data: response.data_json(),
            extensions: None,
        }
    }

    /// `json.Marshal(Response)`: fixed field order `errors, data,
    /// extensions`, each omitted when empty; Go HTML escaping.
    pub fn to_json(&self) -> Result<String, String> {
        let mut entries: Vec<(&str, Json)> = Vec::new();
        if !self.errors.is_empty() {
            let errors = self
                .errors
                .iter()
                .map(query_error_json)
                .collect::<Result<Vec<_>, String>>()?;
            entries.push(("errors", Json::Arr(errors)));
        }
        if let Some(data) = &self.data {
            entries.push(("data", Json::Raw(data.clone())));
        }
        if let Some(extensions) = self.extensions.as_ref().filter(|map| !map.is_empty()) {
            entries.push((
                "extensions",
                Json::from_serde(&Value::Object(extensions.clone()))?,
            ));
        }
        let json = Json::obj(entries);
        json.check_finite()?;
        Ok(json.to_compact_string())
    }
}

fn query_error_from_exec(error: &ExecError) -> QueryError {
    QueryError {
        message: error.message.clone(),
        locations: (!error.locations.is_empty()).then(|| {
            error
                .locations
                .iter()
                .map(|location| Location {
                    line: location.line as i64,
                    column: location.column as i64,
                })
                .collect()
        }),
        path: (!error.path.is_empty()).then(|| {
            error
                .path
                .iter()
                .map(|segment| match segment {
                    PathSegment::Field(name) => Value::String(name.clone()),
                    PathSegment::Index(index) => Value::from(*index as u64),
                })
                .collect()
        }),
        extensions: None,
    }
}

fn query_error_json(error: &QueryError) -> Result<Json, String> {
    let mut entries: Vec<(&str, Json)> = vec![("message", Json::Str(error.message.clone()))];
    if let Some(locations) = error.locations.as_ref().filter(|list| !list.is_empty()) {
        entries.push((
            "locations",
            Json::Arr(
                locations
                    .iter()
                    .map(|location| {
                        Json::obj(vec![
                            ("line", Json::Int(location.line)),
                            ("column", Json::Int(location.column)),
                        ])
                    })
                    .collect(),
            ),
        ));
    }
    if let Some(path) = error.path.as_ref().filter(|list| !list.is_empty()) {
        entries.push((
            "path",
            Json::Arr(
                path.iter()
                    .map(Json::from_serde)
                    .collect::<Result<Vec<_>, String>>()?,
            ),
        ));
    }
    if let Some(extensions) = error.extensions.as_ref().filter(|map| !map.is_empty()) {
        entries.push((
            "extensions",
            Json::from_serde(&Value::Object(extensions.clone()))?,
        ));
    }
    Ok(Json::obj(entries))
}

/// `graphQLQueryParams` (`graphql.go:235-239`).
#[derive(Debug, Clone, PartialEq)]
pub struct GraphqlQueryParams {
    pub query: String,
    pub operation_name: String,
    pub variables: Map<String, Value>,
    /// Go leaves `Variables` nil when the field is absent or `null`; the
    /// in-memory [`Map`] cannot express that, but the audit log can
    /// (`"null"` vs `"{}"`).
    pub variables_are_nil: bool,
}

impl Default for GraphqlQueryParams {
    fn default() -> Self {
        Self {
            query: String::new(),
            operation_name: String::new(),
            variables: Map::new(),
            variables_are_nil: true,
        }
    }
}

/// A request handed to [`GraphqlDeps::exec`] (`schema.Exec` arguments).
#[derive(Debug, Clone, Copy)]
pub struct GraphqlExecRequest<'a> {
    pub query: &'a str,
    pub operation_name: &'a str,
    pub variables: &'a Map<String, Value>,
    pub actor: Actor,
}

/// `graphqlbackend.LimiterArgs` (`rate_limit.go:395-403`).
#[derive(Debug, Clone, Copy)]
pub struct LimiterArgs<'a> {
    pub is_ip: bool,
    pub anonymous: bool,
    pub request_name: &'a str,
    pub request_source: &'a str,
}

/// The call `serveGraphQL` makes into `Limiter.RateLimit`.
#[derive(Debug, Clone, Copy)]
pub struct GraphqlRateLimitRequest<'a> {
    pub uid: &'a str,
    pub cost: i64,
    pub args: LimiterArgs<'a>,
}

/// The observable outcome of `rlw.Get()` + `RateLimit`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RateLimitOutcome {
    NotLimited,
    /// `throttled.RateLimitResult.RetryAfter` truncated to whole seconds.
    Limited {
        retry_after_seconds: i64,
    },
}

/// The schema-execution and rate-limit seams. See the module docs.
#[async_trait]
pub trait GraphqlDeps: Send + Sync {
    async fn exec(&self, request: GraphqlExecRequest<'_>) -> GraphqlResponse;

    /// `None` means `rlw.Get()` reported `enabled == false` (the default:
    /// `ExperimentalFeatures.RateLimitAnonymous` unset/0).
    async fn rate_limit(&self, _request: GraphqlRateLimitRequest<'_>) -> Option<RateLimitOutcome> {
        None
    }

    /// `session.authenticateByCookie`'s read path. The transport calls this
    /// only for trusted external requests that carry an `sgs` cookie. The
    /// default (also used by the differential harness) treats every request
    /// as unauthenticated.
    async fn authenticate(&self, _cookie: Option<&str>) -> SessionAuthOutcome {
        SessionAuthOutcome::Anonymous(AnonymousReason::NoCookie)
    }
}

/// The production anonymous rate limiter (`rate_limit.go:424-473`) and the
/// configuration snapshot that drives its hourly limit. The baseline updates
/// the watcher from `conf.Watch`; here every request re-reads
/// `experimentalFeatures.rateLimitAnonymous` from [`LiveConfig`].
struct RateLimitDeps {
    watcher: crate::rate_limit::BasicLimitWatcher,
    live: Arc<LiveConfig>,
}

/// The production session-authentication dependencies.
struct SessionDeps {
    store: Arc<dyn SessionStore>,
    users: Arc<dyn SessionUserLookup>,
    profiles: Arc<dyn crate::graphql_resolvers::UserProfiles>,
    /// `SRC_SESSION_COOKIE_KEY` (empty by default in the baseline too).
    cookie_key: Vec<u8>,
}

/// Production schema execution: the assembled OSS [`sg_graphql::Schema`],
/// executed by `sg-graphql`'s introspection executor.
///
/// [`SchemaGraphqlDeps::new`] has no database/session dependencies (the
/// transport tests); [`SchemaGraphqlDeps::with_session`] is the frontend
/// wiring: it authenticates the `sgs` cookie and loads the current user for
/// the DB-backed `User` fields.
pub struct SchemaGraphqlDeps {
    schema: Arc<sg_graphql::Schema>,
    session: Option<SessionDeps>,
    /// `Query.repository`'s database seam; `None` in transport-only tests.
    repositories: Option<Arc<dyn crate::graphql_resolvers::RepositoryLookup>>,
    /// `Query.user`'s database seam; `None` in transport-only tests.
    users: Option<Arc<dyn crate::graphql_resolvers::UserLookupStore>>,
    /// `Query.users`'s database seam; `None` in transport-only tests.
    users_connections: Option<Arc<dyn crate::graphql_resolvers::UsersConnectionStore>>,
    /// `Query.organization`'s database seam; `None` in transport-only tests.
    organizations: Option<Arc<dyn crate::graphql_resolvers::OrganizationStore>>,
    /// `Query.organizations`'s database seam; `None` in transport-only tests.
    organizations_connections:
        Option<Arc<dyn crate::graphql_resolvers::OrganizationsConnectionStore>>,
    /// `User.organizations`'s database seam; `None` in transport-only tests.
    user_organizations: Option<Arc<dyn crate::graphql_resolvers::UserOrganizationsStore>>,
    /// `Org.members`'s database seam; `None` in transport-only tests.
    org_members: Option<Arc<dyn crate::graphql_resolvers::OrgMembersStore>>,
    /// `User.organizationMemberships`'s database seam; `None` in
    /// transport-only tests.
    org_memberships: Option<Arc<dyn crate::graphql_resolvers::OrgMembershipsStore>>,
    /// `Mutation.settingsMutation`'s write seam (the `settings` table);
    /// `None` in transport-only tests.
    pool: Option<sqlx::PgPool>,
    /// Anonymous GraphQL rate limiter; `None` in transport-only tests and when
    /// the frontend could not reach Redis at startup.
    rate_limit: Option<RateLimitDeps>,
}

impl SchemaGraphqlDeps {
    pub fn new(schema: Arc<sg_graphql::Schema>) -> Self {
        Self {
            schema,
            session: None,
            repositories: None,
            users: None,
            users_connections: None,
            organizations: None,
            organizations_connections: None,
            user_organizations: None,
            org_members: None,
            org_memberships: None,
            pool: None,
            rate_limit: None,
        }
    }

    /// Wire session authentication, the current-user lookup and the
    /// repository/user lookups.
    pub fn with_session(schema: Arc<sg_graphql::Schema>, pool: sqlx::PgPool) -> Self {
        let users = Arc::new(crate::session_auth::PgSessionUsers::new(pool.clone()));
        let profiles = Arc::new(crate::session_auth::PgSessionUsers::new(pool.clone()));
        Self {
            schema,
            session: Some(SessionDeps {
                store: Arc::new(crate::session_auth::RedisSessionStore::from_env()),
                users,
                profiles,
                cookie_key: session_cookie_key(),
            }),
            repositories: Some(Arc::new(crate::repository_store::PgRepositoryStore::new(
                pool.clone(),
            ))),
            users: Some(Arc::new(crate::user_store::PgUserStore::new(pool.clone()))),
            users_connections: Some(Arc::new(crate::user_store::PgUserStore::new(pool.clone()))),
            organizations: Some(Arc::new(crate::org_store::PgOrgStore::new(pool.clone()))),
            organizations_connections: Some(Arc::new(crate::org_store::PgOrgStore::new(
                pool.clone(),
            ))),
            user_organizations: Some(Arc::new(crate::org_store::PgOrgStore::new(pool.clone()))),
            org_members: Some(Arc::new(crate::org_store::PgOrgStore::new(pool.clone()))),
            org_memberships: Some(Arc::new(crate::org_store::PgOrgStore::new(pool.clone()))),
            pool: Some(pool),
            rate_limit: None,
        }
    }

    /// Test hook: use a repository lookup without a real database.
    pub fn with_repository_store(
        mut self,
        repositories: Arc<dyn crate::graphql_resolvers::RepositoryLookup>,
    ) -> Self {
        self.repositories = Some(repositories);
        self
    }

    /// Test hook: use a user lookup without a real database.
    pub fn with_user_store(
        mut self,
        users: Arc<dyn crate::graphql_resolvers::UserLookupStore>,
    ) -> Self {
        self.users = Some(users);
        self
    }

    /// Production rate limiting (`serve_cmd.go:436-448`): the Redis-backed
    /// GCRA watcher plus the live site configuration that carries
    /// `experimentalFeatures.rateLimitAnonymous`.
    pub fn with_rate_limit(
        mut self,
        watcher: crate::rate_limit::BasicLimitWatcher,
        live: Arc<LiveConfig>,
    ) -> Self {
        self.rate_limit = Some(RateLimitDeps { watcher, live });
        self
    }

    /// Test hook: inject the store/user lookup without Redis or Postgres.
    #[cfg(test)]
    fn with_session_parts(
        schema: Arc<sg_graphql::Schema>,
        store: Arc<dyn SessionStore>,
        users: Arc<dyn SessionUserLookup>,
        profiles: Arc<dyn crate::graphql_resolvers::UserProfiles>,
        cookie_key: Vec<u8>,
    ) -> Self {
        Self {
            schema,
            session: Some(SessionDeps {
                store,
                users,
                profiles,
                cookie_key,
            }),
            repositories: None,
            users: None,
            users_connections: None,
            organizations: None,
            organizations_connections: None,
            user_organizations: None,
            org_members: None,
            org_memberships: None,
            pool: None,
            rate_limit: None,
        }
    }
}

#[async_trait]
impl GraphqlDeps for SchemaGraphqlDeps {
    async fn rate_limit(&self, request: GraphqlRateLimitRequest<'_>) -> Option<RateLimitOutcome> {
        let deps = self.rate_limit.as_ref()?;
        // `conf.Watch`'s `updateFromConfig(e.RateLimitAnonymous)`: the site
        // configuration drives the quota and the watcher disables itself for
        // `limit <= 0`.
        let limit = deps.live.current().site.rate_limit_anonymous();
        deps.watcher.update_from_config(limit);
        deps.watcher.check(request.args).await
    }

    async fn exec(&self, request: GraphqlExecRequest<'_>) -> GraphqlResponse {
        // `CurrentUser` resolves `db.Users().GetByCurrentAuthUser`; prefetch
        // the row once per request. A lookup failure is logged and treated as
        // no current user (the baseline would surface a resolver error; no
        // resolver-level error plumbing exists for DB failures yet).
        let mut site_admin = false;
        let current_user = match (request.actor, self.session.as_ref()) {
            (Actor::User { uid }, Some(session)) => match session.profiles.profile(uid).await {
                Ok(profile) => profile.inspect(|profile| {
                    site_admin = profile.site_admin;
                }),
                Err(error) => {
                    tracing::error!(uid, %error, "failed to look up current user");
                    None
                }
            },
            _ => None,
        };

        let exec_request = sg_graphql::exec::ExecRequest {
            query: request.query,
            operation_name: request.operation_name,
            variables: request.variables,
            actor: request.actor,
        };
        // `Query.repository`/`Query.user`/`Query.users`/`Query.organization`/
        // `Query.organizations` need async database lookups, so prefetch the
        // root fields identified by the executor before executing.
        let fields = sg_graphql::exec::root_fields(&self.schema, &exec_request);
        let repositories = match &self.repositories {
            Some(store) => {
                crate::graphql_resolvers::prefetch_repositories(
                    store.as_ref(),
                    &fields,
                    request.actor,
                    site_admin,
                )
                .await
            }
            None => std::collections::HashMap::new(),
        };
        let users = match &self.users {
            Some(store) => crate::graphql_resolvers::prefetch_users(store.as_ref(), &fields).await,
            None => std::collections::HashMap::new(),
        };
        let users_connections = match &self.users_connections {
            Some(store) => {
                crate::graphql_resolvers::prefetch_users_connections(store.as_ref(), &fields).await
            }
            None => std::collections::HashMap::new(),
        };
        let organizations = match &self.organizations {
            Some(store) => {
                crate::graphql_resolvers::prefetch_organizations(
                    store.as_ref(),
                    &fields,
                    request.actor,
                    site_admin,
                )
                .await
            }
            None => std::collections::HashMap::new(),
        };
        let organizations_connections = match &self.organizations_connections {
            Some(store) => {
                crate::graphql_resolvers::prefetch_organizations_connections(
                    store.as_ref(),
                    &fields,
                    request.actor,
                    site_admin,
                )
                .await
            }
            None => std::collections::HashMap::new(),
        };
        // `Mutation.settingsMutation`: the executor resolves fields
        // synchronously, so run the (async) write path now. Only mutation
        // operations perform side effects.
        let mutation = match (&self.pool, &self.org_memberships) {
            (Some(pool), Some(store)) if sg_graphql::exec::is_mutation_operation(&exec_request) => {
                crate::settings_mutation::prefetch_settings_mutation(
                    pool,
                    store.as_ref(),
                    &fields,
                    request.actor,
                    site_admin,
                )
                .await
            }
            _ => crate::settings_mutation::MutationRoot::default(),
        };
        let nodes = match (&self.org_memberships, &self.repositories) {
            (Some(memberships_store), Some(repositories)) => {
                crate::graphql_resolvers::prefetch_node_roots(
                    memberships_store.as_ref(),
                    repositories.as_ref(),
                    &fields,
                    request.actor,
                    site_admin,
                )
                .await
            }
            _ => crate::graphql_resolvers::NodeRoots::default(),
        };
        let nested = match (
            &self.user_organizations,
            &self.org_members,
            &self.org_memberships,
        ) {
            (Some(store), Some(members_store), Some(memberships_store)) => Arc::new(
                crate::graphql_resolvers::prefetch_nested(
                    store.as_ref(),
                    members_store.as_ref(),
                    memberships_store.as_ref(),
                    &fields,
                    current_user.as_ref(),
                    &users,
                    &users_connections,
                    &organizations,
                    &organizations_connections,
                    &nodes,
                    request.actor,
                    site_admin,
                )
                .await,
            ),
            _ => Arc::new(crate::graphql_resolvers::NestedPrefetch::default()),
        };
        let user = current_user.map(crate::graphql_resolvers::UserObject::from_data);
        let roots = crate::graphql_resolvers::FrontendRootResolver::with_prefetched_organizations(
            user,
            repositories,
            users,
            users_connections,
            organizations,
            organizations_connections,
            site_admin,
        )
        .with_nested(nested)
        .with_nodes(nodes)
        .with_mutation(mutation);
        let response = sg_graphql::exec::execute_with(&self.schema, exec_request, Some(&roots));
        GraphqlResponse::from_exec(&response)
    }

    async fn authenticate(&self, cookie: Option<&str>) -> SessionAuthOutcome {
        let Some(session) = self.session.as_ref() else {
            return SessionAuthOutcome::Anonymous(AnonymousReason::NoCookie);
        };
        auth::authenticate(
            session.store.as_ref(),
            session.users.as_ref(),
            &session.cookie_key,
            cookie,
            time::OffsetDateTime::now_utc(),
        )
        .await
    }
}

/// `sessionCookieKey` is `SRC_SESSION_COOKIE_KEY` with an empty default; an
/// empty HMAC key is valid (and is what an unconfigured OSS instance uses).
fn session_cookie_key() -> Vec<u8> {
    std::env::var("SRC_SESSION_COOKIE_KEY")
        .unwrap_or_default()
        .into_bytes()
}

/// The placeholder used by tests that only exercise the transport.
pub struct UnportedGraphqlDeps;

#[async_trait]
impl GraphqlDeps for UnportedGraphqlDeps {
    async fn exec(&self, _request: GraphqlExecRequest<'_>) -> GraphqlResponse {
        GraphqlResponse {
            errors: vec![QueryError::new(
                "GraphQL schema execution is not yet ported",
            )],
            ..GraphqlResponse::default()
        }
    }
}

// ---------------------------------------------------------------------------
// Handler
// ---------------------------------------------------------------------------

/// `jsonHandler(serveGraphQL(logger, schema, rlw, isInternal))`: the whole
/// wrapper chain whose observable behavior is status + headers + body.
///
/// `internal` selects `ErrorHandler.WriteErrBody` (`httpapi.go:364`): internal
/// responses carry the error text, external ones only the newline.
pub async fn graphql_response<B>(
    req: &mut Request<B>,
    live: &LiveConfig,
    internal: bool,
    deps: Arc<dyn GraphqlDeps>,
) -> Response<BoxBody>
where
    B: Body<Data = Bytes> + Unpin,
    B::Error: fmt::Display,
{
    let mut response = graphql_response_inner(req, live, internal, deps).await;
    // The session middleware adds `Vary` before doing anything else, so it is
    // present on error responses too. The internal listener has no session
    // middleware.
    if !internal {
        response
            .headers_mut()
            .append(header::VARY, HeaderValue::from_static(SESSION_VARY));
    }
    response
}

async fn graphql_response_inner<B>(
    req: &mut Request<B>,
    live: &LiveConfig,
    internal: bool,
    deps: Arc<dyn GraphqlDeps>,
) -> Response<BoxBody>
where
    B: Body<Data = Bytes> + Unpin,
    B::Error: fmt::Display,
{
    let write_err_body = internal;

    // `serveGraphQL`'s defensive method check; the routers already answer 405.
    if req.method() != http::Method::POST {
        return handler_error("method must be POST", write_err_body);
    }

    // `session.CookieMiddlewareWithCSRFSafety`: only trusted external requests
    // authenticate from the cookie; the internal listener uses the actor
    // header middleware instead (not ported), so it stays anonymous.
    let actor = if internal {
        Actor::Anonymous
    } else {
        match session_actor(req.headers(), live, deps.as_ref()).await {
            Ok(actor) => actor,
            Err(()) => return session_store_error(),
        }
    };

    // `graphql.go:116-119`: the raw query string names the request; an empty
    // query (including a bare `?`) is "unknown".
    let request_name = match req.uri().query() {
        Some(query) if !query.is_empty() => query.to_owned(),
        _ => "unknown".to_owned(),
    };
    // `graphql.go:120`: `search.GuessSource(r)`.
    let request_source = crate::search_source::guess_source(
        &req.headers()
            .get(header::USER_AGENT)
            .map(|value| String::from_utf8_lossy(value.as_bytes()).into_owned())
            .unwrap_or_default(),
    );
    let uid = get_uid(req.headers(), actor);

    let gzip = req
        .headers()
        .get(header::CONTENT_ENCODING)
        .and_then(|value| value.to_str().ok())
        == Some("gzip");

    let raw = match read_request_body(req.body_mut()).await {
        Ok(body) => body,
        // A transport read error surfaces from `json.Decoder.Decode` in the
        // baseline, so it is wrapped with "failed to decode request".
        Err(message) => {
            return handler_error(
                &format!("failed to decode request: {message}"),
                write_err_body,
            )
        }
    };

    let (body, gzip_read_error) = if gzip {
        match gunzip(&raw) {
            Ok(decompressed) => decompressed,
            Err(message) => {
                return handler_error(
                    &format!("failed to decompress request body: {message}"),
                    write_err_body,
                )
            }
        }
    } else {
        (raw, None)
    };

    let params = match decode_params(&body) {
        Ok(params) => params,
        Err(message) => {
            // `json.Decoder` reports the underlying read error when it needs
            // more input; otherwise the syntax/type error wins.
            let message = match gzip_read_error {
                Some(read_error) if is_eof_error(&message) => read_error,
                _ => message,
            };
            return handler_error(
                &format!("failed to decode request: {message}"),
                write_err_body,
            );
        }
    };

    let variables: BTreeMap<String, Value> = params
        .variables
        .iter()
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect();

    // `graphql.go:147-151`: `defer instrumentGraphQL; recordAuditLog`, i.e.
    // from here on every return path observes the duration and (when enabled)
    // writes the audit log.
    let audit_context = crate::audit::GraphqlAuditContext {
        actor: crate::audit::AuditActor::new(
            actor.uid(),
            match actor {
                Actor::User { .. } => String::new(),
                Actor::Anonymous => actor_anonymous_uid(req.headers()),
            },
        ),
        client: crate::audit::client_info(
            req.headers(),
            req.extensions()
                .get::<sg_http::RemoteAddr>()
                .map(|remote| remote.0.as_str()),
        ),
        query: params.query.clone(),
        variables_json: crate::audit::GraphqlAuditContext::variables_json(
            if params.variables_are_nil {
                None
            } else {
                Some(&params.variables)
            },
        ),
        request_name: request_name.clone(),
        request_source: request_source.clone(),
        successful: true,
    };
    let site = live.current().site.clone();
    let mut timer = RequestTimer::new(&params.query, &request_name, site, audit_context);

    // Calculating the cost is cheaper than validating the schema
    // (`graphql.go:159`).
    if let Ok(cost) = estimate_query_cost(&params.query, &variables) {
        // `graphql.go:168-169`: observed before the violation and rate-limit
        // checks, so rejected requests still count.
        crate::graphql_metrics::observe_cost(
            &crate::graphql_metrics::actor_type_label(
                internal,
                matches!(actor, Actor::Anonymous),
                &request_source,
            ),
            cost.field_count,
        );
        if !internal {
            let site = live.current();
            let limits = rate_limits(&site.site);
            let violations = violations(&cost, &limits);
            if !violations.is_empty() {
                return violation_response(&violations, &site.site);
            }
        }

        let (rate_uid, is_ip, anonymous) = uid;
        let outcome = deps
            .rate_limit(GraphqlRateLimitRequest {
                uid: &rate_uid,
                cost: cost.field_count,
                args: LimiterArgs {
                    is_ip,
                    anonymous,
                    request_name: &request_name,
                    request_source: &request_source,
                },
            })
            .await;
        if let Some(RateLimitOutcome::Limited {
            retry_after_seconds,
        }) = outcome
        {
            let mut response = empty_body_response(StatusCode::TOO_MANY_REQUESTS);
            response.headers_mut().insert(
                header::CONTENT_TYPE,
                HeaderValue::from_static("application/json"),
            );
            if let Ok(value) = HeaderValue::from_str(&retry_after_seconds.to_string()) {
                response.headers_mut().insert(header::RETRY_AFTER, value);
            }
            return response;
        }
    }

    // `graphql.go:220`: the duration histogram measures execution only; the
    // timer's earlier phase was used for the pre-execution return paths.
    timer.exec_started();
    let response = deps
        .exec(GraphqlExecRequest {
            query: &params.query,
            operation_name: &params.operation_name,
            variables: &params.variables,
            actor,
        })
        .await;
    timer.finish(response.errors.is_empty());
    let body = match response.to_json() {
        Ok(body) => body.into_bytes(),
        Err(error) => {
            return handler_error(
                &format!("failed to marshal GraphQL response: {error}"),
                write_err_body,
            )
        }
    };

    text_response(StatusCode::OK, "application/json", body, false)
}

/// The `defer instrumentGraphQL(traceData); recordAuditLog(...)`
/// (`graphql.go:146-151`); `Drop` runs the two in that order.
///
/// `exec_start` stays [`None`] on the pre-execution return paths (cost
/// violations, 429), where the baseline computes `time.Since(time.Time{})`
/// and therefore records a `+Inf` observation. The exact seconds of that
/// observation depend on the wall clock, so only its bucket is contractual.
struct RequestTimer {
    mutation: bool,
    route: String,
    success: bool,
    started: Instant,
    exec_start: Option<Instant>,
    /// Site config snapshot for `recordAuditLog`; the baseline reads
    /// `conf.SiteConfig()` in the deferred call, but a whole request sees a
    /// stable config in practice.
    site: SiteConfiguration,
    /// `recordAuditLog(traceData)`; gating and formatting happen in `Drop`
    /// because the audit log is part of the same defer as the metric.
    audit: crate::audit::GraphqlAuditContext,
}

impl RequestTimer {
    fn new(
        query: &str,
        route: &str,
        site: SiteConfiguration,
        audit: crate::audit::GraphqlAuditContext,
    ) -> Self {
        Self {
            mutation: crate::graphql_metrics::is_mutation_query(query),
            route: route.to_owned(),
            success: true,
            started: Instant::now(),
            exec_start: None,
            site,
            audit,
        }
    }

    fn exec_started(&mut self) {
        self.exec_start = Some(Instant::now());
    }

    fn finish(&mut self, success: bool) {
        self.success = success;
    }
}

impl Drop for RequestTimer {
    fn drop(&mut self) {
        let seconds = match self.exec_start {
            Some(start) => start.elapsed().as_secs_f64(),
            None => {
                crate::graphql_metrics::PRE_EXEC_DURATION_SECONDS
                    + self.started.elapsed().as_secs_f64()
            }
        };
        crate::graphql_metrics::observe_request_duration(
            self.mutation,
            &self.route,
            self.success,
            seconds,
        );
        // `recordAuditLog` runs after `instrumentGraphQL` in the same defer.
        self.audit.successful = self.success;
        if let Some(record) = crate::audit::graphql_record(&self.site, &self.audit) {
            record.emit(&crate::audit::log_level_from_env());
        }
    }
}

/// `getUID` (`graphql.go:259-273`): an authenticated actor wins, then the
/// anonymous-UID cookie, then `X-Forwarded-For`.
fn get_uid(headers: &HeaderMap, actor: Actor) -> (String, bool, bool) {
    if let Actor::User { uid } = actor {
        return (uid.to_string(), false, false);
    }
    if let Some(uid) = anonymous_uid(headers) {
        if !uid.is_empty() {
            return (uid, false, true);
        }
    }
    if let Some(ip) = headers
        .get("x-forwarded-for")
        .and_then(|value| value.to_str().ok())
        .filter(|value| !value.is_empty())
    {
        return (ip.to_owned(), true, true);
    }
    (UNKNOWN_UID.to_owned(), false, true)
}

/// `isTrustedOrigin` plus the `X-Requested-With` presence check from
/// `CookieMiddlewareWithCSRFSafety` (`session.go:332-344`).
fn is_trusted_request(headers: &HeaderMap, live: &LiveConfig) -> bool {
    if headers.contains_key("x-requested-with") {
        return true;
    }
    let origin = headers
        .get(header::ORIGIN)
        .and_then(|value| value.to_str().ok())
        .unwrap_or("");
    crate::mux::SecurityConfig::from_site_config(&live.current().site).is_trusted_origin(origin)
}

/// The `sgs` request cookie (`r.Cookie(cookieName)`). Untrusted requests never
/// consult Redis (CSRF safety).
async fn session_actor(
    headers: &HeaderMap,
    live: &LiveConfig,
    deps: &dyn GraphqlDeps,
) -> Result<Actor, ()> {
    if !is_trusted_request(headers, live) {
        return Ok(Actor::Anonymous);
    }
    let Some(cookie) = session_cookie(headers) else {
        return Ok(Actor::Anonymous);
    };
    match deps.authenticate(Some(&cookie)).await {
        SessionAuthOutcome::Authenticated { uid } => Ok(Actor::User { uid }),
        SessionAuthOutcome::Anonymous(_) => Ok(Actor::Anonymous),
        SessionAuthOutcome::StoreUnavailable => Err(()),
    }
}

/// `r.Cookie("sgs")`: the first matching `Cookie` header value, unquoted.
fn session_cookie(headers: &HeaderMap) -> Option<String> {
    for value in headers.get_all(header::COOKIE) {
        let Ok(cookies) = value.to_str() else {
            continue;
        };
        for cookie in cookies.split(';') {
            let cookie = cookie.trim();
            let Some((name, value)) = cookie.split_once('=') else {
                continue;
            };
            if name.trim() == COOKIE_NAME {
                return Some(unquote_cookie_value(value.trim()));
            }
        }
    }
    None
}

/// `net/http` strips one pair of surrounding double quotes from request cookie
/// values.
fn unquote_cookie_value(value: &str) -> String {
    value
        .strip_prefix('"')
        .and_then(|value| value.strip_suffix('"'))
        .unwrap_or(value)
        .to_owned()
}

/// The baseline's Redis-error response: `w.WriteHeader(500)` then the message
/// with no newline and no JSON content type (this middleware runs outside
/// `JsonMiddleware`). It deliberately does not clear the cookie.
fn session_store_error() -> Response<BoxBody> {
    text_response(
        StatusCode::INTERNAL_SERVER_ERROR,
        "text/plain; charset=utf-8",
        "failed to read session actor",
        false,
    )
}

/// `cookie.AnonymousUID` (`internal/cookie/cookie.go:9-30`): the first
/// `sourcegraphAnonymousUid` cookie in the request's `Cookie` headers.
fn anonymous_uid(headers: &HeaderMap) -> Option<String> {
    for value in headers.get_all(header::COOKIE) {
        let Ok(cookies) = value.to_str() else {
            continue;
        };
        for cookie in cookies.split(';') {
            let cookie = cookie.trim();
            let Some((name, value)) = cookie.split_once('=') else {
                continue;
            };
            if name.trim() == "sourcegraphAnonymousUid" {
                let value = value.trim();
                let value = value
                    .strip_prefix('"')
                    .and_then(|value| value.strip_suffix('"'))
                    .unwrap_or(value);
                return Some(value.to_owned());
            }
        }
    }
    None
}

/// `AnonymousUIDMiddleware` (`internal/actor/http.go:180-205`): the cookie is
/// authoritative, and the `X-Sourcegraph-Actor-Anonymous-UID` propagation
/// header is consulted only when there is no cookie. Used for the audit
/// actor's ID; `getUID` deliberately keeps its own cookie-only lookup.
fn actor_anonymous_uid(headers: &HeaderMap) -> String {
    if let Some(uid) = anonymous_uid(headers) {
        return uid;
    }
    headers
        .get("x-sourcegraph-actor-anonymous-uid")
        .map(|value| String::from_utf8_lossy(value.as_bytes()).into_owned())
        .unwrap_or_default()
}

// ---------------------------------------------------------------------------
// Request body decoding
// ---------------------------------------------------------------------------

async fn read_request_body<B>(body: &mut B) -> Result<Bytes, String>
where
    B: Body<Data = Bytes> + Unpin,
    B::Error: fmt::Display,
{
    let mut out = BytesMut::new();
    let mut body = Pin::new(body);
    while let Some(frame) = poll_fn(|cx| body.as_mut().poll_frame(cx)).await {
        let frame = frame.map_err(|error| error.to_string())?;
        if let Some(data) = frame.data_ref() {
            out.extend_from_slice(data);
        }
    }
    Ok(out.freeze())
}

/// `json.NewDecoder(...).Decode(&graphQLQueryParams)`: one JSON value, then
/// type checks that reproduce `encoding/json`'s error strings.
fn decode_params(body: &[u8]) -> Result<GraphqlQueryParams, String> {
    let mut deserializer = serde_json::Deserializer::from_slice(body);
    let value = match Value::deserialize(&mut deserializer) {
        Ok(value) => value,
        Err(error) => return Err(go_json_syntax_error(body, &error)),
    };
    let object = match value {
        Value::Null => return Ok(GraphqlQueryParams::default()),
        Value::Object(object) => object,
        other => {
            return Err(format!(
                "json: cannot unmarshal {} into Go value of type httpapi.graphQLQueryParams",
                go_value_type(&other)
            ))
        }
    };

    let mut params = GraphqlQueryParams::default();
    for (key, value) in object {
        if key.eq_ignore_ascii_case("query") {
            params.query = go_string_field("query", value)?;
        } else if key.eq_ignore_ascii_case("operationName") {
            params.operation_name = go_string_field("operationName", value)?;
        } else if key.eq_ignore_ascii_case("variables") {
            params.variables = match value {
                Value::Null => Map::new(),
                Value::Object(variables) => {
                    params.variables_are_nil = false;
                    variables
                }
                other => {
                    return Err(format!(
                        "json: cannot unmarshal {} into Go struct field \
                         graphQLQueryParams.variables of type map[string]interface {{}}",
                        go_value_type(&other)
                    ))
                }
            };
        }
    }
    Ok(params)
}

fn go_string_field(field: &str, value: Value) -> Result<String, String> {
    match value {
        Value::Null => Ok(String::new()),
        Value::String(string) => Ok(string),
        other => Err(format!(
            "json: cannot unmarshal {} into Go struct field graphQLQueryParams.{field} \
             of type string",
            go_value_type(&other)
        )),
    }
}

fn go_value_type(value: &Value) -> &'static str {
    match value {
        Value::Null => "null",
        Value::Bool(_) => "bool",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}

/// `encoding/json`'s decoder errors deliberately have no line/column; map the
/// `serde_json` categories used by the differential corpus onto the Go text.
fn go_json_syntax_error(body: &[u8], error: &serde_json::Error) -> String {
    let message = error.to_string();
    let message = match message.find(" at line ") {
        Some(index) => message[..index].to_owned(),
        None => message,
    };
    if message == "expected value" {
        if let Some(character) = offending_character(body, error) {
            return format!(
                "invalid character {} looking for beginning of value",
                go_rune(character)
            );
        }
    }
    if message == "trailing comma" {
        if let Some(character) = offending_character(body, error) {
            return format!(
                "invalid character {} looking for beginning of object key string",
                go_rune(character)
            );
        }
    }
    if message == "key must be a string" {
        if let Some(character) = offending_character(body, error) {
            return format!(
                "invalid character {} looking for beginning of object key string",
                go_rune(character)
            );
        }
    }
    if message == "expected `:`" {
        if let Some(character) = offending_character(body, error) {
            return format!("invalid character {} after object key", go_rune(character));
        }
    }
    if message.starts_with("expected `,` or `}`") {
        if let Some(character) = offending_character(body, error) {
            return format!(
                "invalid character {} after object key:value pair",
                go_rune(character)
            );
        }
    }
    if message.starts_with("expected `,` or `]`") {
        if let Some(character) = offending_character(body, error) {
            return format!(
                "invalid character {} after array element",
                go_rune(character)
            );
        }
    }
    if message.starts_with("EOF while parsing") {
        // `json.Decoder` returns `io.EOF` when the input has no value to
        // parse at all (empty or whitespace-only); a truncated value is
        // `io.ErrUnexpectedEOF`.
        if body.iter().all(u8::is_ascii_whitespace) {
            return "EOF".to_owned();
        }
        return "unexpected EOF".to_owned();
    }
    message
}

fn offending_character(body: &[u8], error: &serde_json::Error) -> Option<char> {
    if error.line() == 0 || error.column() == 0 {
        return None;
    }
    let line_start: usize = body
        .split(|byte| *byte == b'\n')
        .take(error.line() - 1)
        .map(|line| line.len() + 1)
        .sum();
    let index = line_start + error.column() - 1;
    body.get(index)
        .and_then(|byte| std::str::from_utf8(std::slice::from_ref(byte)).ok())
        .and_then(|text| text.chars().next())
}

/// Go's `fmt.Sprintf("%q", rune)`.
fn go_rune(character: char) -> String {
    let escaped = match character {
        '\'' => "\\'".to_owned(),
        '\\' => "\\\\".to_owned(),
        '\n' => "\\n".to_owned(),
        '\r' => "\\r".to_owned(),
        '\t' => "\\t".to_owned(),
        character if character.is_control() => format!("\\x{:02x}", character as u32),
        character => character.to_string(),
    };
    format!("'{escaped}'")
}

fn is_eof_error(message: &str) -> bool {
    matches!(
        message,
        "EOF" | "unexpected EOF" | "unexpected end of file" | "incomplete deflate stream"
    ) || message.starts_with("EOF while parsing")
}

// ---------------------------------------------------------------------------
// gzip request bodies (`lib/limitedgzip` + `compress/gzip`)
// ---------------------------------------------------------------------------

/// Decompress one gzip member. `Err` is a `gzip.NewReader` header failure
/// (wrapped as "failed to decompress request body"); a later read failure is
/// returned alongside the bytes produced so far, because a `json.Decoder` can
/// succeed on them (wrapped as "failed to decode request" only when it cannot).
fn gunzip(body: &[u8]) -> Result<(Bytes, Option<String>), String> {
    validate_gzip_header(body)?;

    let mut decoder = GzDecoder::new(body);
    let mut out = Vec::new();
    let mut limited = (&mut decoder).take(GZIP_FILE_SIZE_LIMIT);
    match limited.read_to_end(&mut out) {
        Ok(_) => Ok((Bytes::from(out), None)),
        Err(error) => Ok((Bytes::from(out), Some(go_gzip_read_error(&error)))),
    }
}

/// The `compress/gzip` header checks `gzip.NewReader` performs eagerly.
fn validate_gzip_header(data: &[u8]) -> Result<(), String> {
    if data.is_empty() {
        return Err("EOF".to_owned());
    }
    if data.len() < 10 {
        return Err("unexpected EOF".to_owned());
    }
    if data[0] != 0x1f || data[1] != 0x8b || data[2] != 8 {
        return Err("gzip: invalid header".to_owned());
    }
    let flags = data[3];
    // Reserved FLG bits must be zero.
    if flags & 0xe0 != 0 {
        return Err("gzip: invalid header".to_owned());
    }
    let mut index = 10;
    if flags & 0x04 != 0 {
        // FEXTRA: little-endian XLEN followed by XLEN bytes.
        if data.len() < index + 2 {
            return Err("unexpected EOF".to_owned());
        }
        let extra_len = u16::from_le_bytes([data[index], data[index + 1]]) as usize;
        index += 2;
        if data.len() < index + extra_len {
            return Err("unexpected EOF".to_owned());
        }
        index += extra_len;
    }
    for flag in [0x08, 0x10] {
        // FNAME / FCOMMENT: NUL-terminated.
        if flags & flag != 0 {
            let Some(end) = data[index..].iter().position(|byte| *byte == 0) else {
                return Err("unexpected EOF".to_owned());
            };
            index += end + 1;
        }
    }
    if flags & 0x02 != 0 {
        // FHCRC: the low 16 bits of the CRC32 of the header.
        if data.len() < index + 2 {
            return Err("unexpected EOF".to_owned());
        }
        let want = u16::from_le_bytes([data[index], data[index + 1]]);
        let mut crc = flate2::Crc::new();
        crc.update(&data[..index]);
        if (crc.sum() & 0xffff) as u16 != want {
            return Err("gzip: invalid header".to_owned());
        }
    }
    Ok(())
}

/// `compress/gzip`'s read errors; `flate2` uses different text.
fn go_gzip_read_error(error: &std::io::Error) -> String {
    use std::io::ErrorKind;
    match error.kind() {
        ErrorKind::UnexpectedEof => "unexpected EOF".to_owned(),
        _ => match error.to_string().as_str() {
            "corrupt gzip stream does not have a matching checksum" => {
                "gzip: invalid checksum".to_owned()
            }
            "invalid gzip header" => "gzip: invalid header".to_owned(),
            "incomplete deflate stream" => "unexpected EOF".to_owned(),
            other => other.to_owned(),
        },
    }
}

// ---------------------------------------------------------------------------
// Cost limits (`internal/conf/computed.go:667-689`, `graphql.go:171-196`)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct GraphqlLimits {
    aliases: i64,
    field_count: i64,
    depth: i64,
    duplicate: i64,
    unique: i64,
}

impl Default for GraphqlLimits {
    fn default() -> Self {
        Self {
            aliases: 500,
            field_count: 500_000,
            depth: 30,
            duplicate: 500,
            unique: 500,
        }
    }
}

/// `conf.RateLimits`: a configured value replaces the default only when it is
/// `<= 0` (upstream bug); duplicate/unique counts are not configurable.
fn rate_limits(site: &SiteConfiguration) -> GraphqlLimits {
    let mut limits = GraphqlLimits::default();
    let Some(configured) = site.raw().get("rateLimits").and_then(Value::as_object) else {
        return limits;
    };
    let apply = |key: &str, slot: &mut i64| {
        if let Some(value) = configured.get(key).and_then(Value::as_i64) {
            if value <= 0 {
                *slot = value;
            }
        }
    };
    apply("graphQLMaxAliases", &mut limits.aliases);
    apply("graphQLMaxFieldCount", &mut limits.field_count);
    apply("graphQLMaxDepth", &mut limits.depth);
    limits
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Violation {
    violation_type: &'static str,
    actual: i64,
    limit: i64,
}

fn violations(cost: &QueryCost, limits: &GraphqlLimits) -> Vec<Violation> {
    let checks = [
        (cost.alias_count, limits.aliases, "alias count"),
        (cost.field_count, limits.field_count, "field count"),
        (cost.max_depth, limits.depth, "query depth"),
        (
            cost.highest_duplicate_field_count,
            limits.duplicate,
            "duplicate field count",
        ),
        (cost.unique_field_count, limits.unique, "unique field count"),
    ];
    checks
        .into_iter()
        .filter(|(actual, limit, _)| actual > limit)
        .map(|(actual, limit, violation_type)| Violation {
            violation_type,
            actual,
            limit,
        })
        .collect()
}

/// `writeViolationError` (`graphql.go:76-104`): 400, `application/json`,
/// `Encoder`-style trailing newline.
///
/// Erratum: `writeViolationError` calls `w.WriteHeader` *before* `writeJSON`,
/// so the `application/json; charset=utf-8` that `writeJSON` sets is discarded
/// (headers are already committed). The wire value is `JsonMiddleware`'s
/// `application/json`.
fn violation_response(violations: &[Violation], site: &SiteConfiguration) -> Response<BoxBody> {
    let docs_url = docs_url(site);
    let errors = violations
        .iter()
        .map(|violation| QueryError {
            message: format!("Query exceeds maximum {} limit", violation.violation_type),
            extensions: Some(Map::from_iter([
                ("code".to_owned(), Value::String(COMPLEXITY_CODE.to_owned())),
                (
                    "type".to_owned(),
                    Value::String(violation.violation_type.to_owned()),
                ),
                ("limit".to_owned(), Value::from(violation.limit)),
                ("actual".to_owned(), Value::from(violation.actual)),
                ("docs_url".to_owned(), Value::String(docs_url.clone())),
            ])),
            ..QueryError::default()
        })
        .collect();
    let response = GraphqlResponse {
        errors,
        ..GraphqlResponse::default()
    };
    let mut body = response.to_json().unwrap_or_default().into_bytes();
    body.push(b'\n');
    text_response(StatusCode::BAD_REQUEST, "application/json", body, false)
}

/// `conf.ExternalURL()` + `ResolveReference({Path:"/help/api/graphql",
/// Fragment:"cost-limits"})`. The reference path is absolute, so only the
/// base's scheme and authority survive; unparsable bases use
/// [`FALLBACK_DOCS_URL`].
fn docs_url(site: &SiteConfiguration) -> String {
    let external_url = site
        .raw()
        .get("externalURL")
        .and_then(Value::as_str)
        .unwrap_or_default();
    resolve_docs_url(external_url).unwrap_or_else(|| FALLBACK_DOCS_URL.to_owned())
}

fn resolve_docs_url(external_url: &str) -> Option<String> {
    if !valid_percent_escapes(external_url)
        || external_url.bytes().any(|byte| byte < 0x20 || byte == 0x7f)
    {
        return None;
    }
    let without_fragment = external_url.split('#').next().unwrap_or_default();
    let (scheme, rest) = split_scheme(without_fragment)?;
    if let Some(after) = rest.strip_prefix("//") {
        let end = after.find(['/', '?']).unwrap_or(after.len());
        let authority = &after[..end];
        validate_authority(authority)?;
        return Some(if scheme.is_empty() {
            format!("//{authority}/help/api/graphql#cost-limits")
        } else {
            format!("{scheme}://{authority}/help/api/graphql#cost-limits")
        });
    }
    if !scheme.is_empty() {
        // Opaque base (`mailto:...`): `URL.String` writes `scheme:///path`.
        return Some(format!("{scheme}:///help/api/graphql#cost-limits"));
    }
    Some("/help/api/graphql#cost-limits".to_owned())
}

fn split_scheme(url: &str) -> Option<(String, &str)> {
    let mut scheme_end = None;
    for (index, byte) in url.bytes().enumerate() {
        match byte {
            b':' => {
                scheme_end = Some(index);
                break;
            }
            b'/' | b'?' | b'#' => break,
            _ => {}
        }
    }
    let Some(end) = scheme_end else {
        return Some((String::new(), url));
    };
    let scheme = &url[..end];
    if scheme.is_empty() {
        // Go: "missing protocol scheme".
        return None;
    }
    let mut bytes = scheme.bytes();
    let first = bytes.next()?;
    if !first.is_ascii_alphabetic()
        || !bytes.all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'+' | b'-' | b'.'))
    {
        // Go: "first path segment in URL cannot contain colon".
        return None;
    }
    Some((scheme.to_ascii_lowercase(), &url[end + 1..]))
}

fn valid_percent_escapes(url: &str) -> bool {
    let bytes = url.as_bytes();
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'%' {
            if index + 2 >= bytes.len()
                || !bytes[index + 1].is_ascii_hexdigit()
                || !bytes[index + 2].is_ascii_hexdigit()
            {
                return false;
            }
            index += 3;
        } else {
            index += 1;
        }
    }
    true
}

fn validate_authority(authority: &str) -> Option<()> {
    let host_port = match authority.rsplit_once('@') {
        Some((_userinfo, host_port)) => host_port,
        None => authority,
    };
    if host_port.is_empty() {
        return None;
    }
    if host_port.bytes().any(|byte| {
        matches!(
            byte,
            b' ' | b'"' | b'<' | b'>' | b'\\' | b'^' | b'`' | b'{' | b'|' | b'}'
        )
    }) {
        return None;
    }
    let (host, port) = if host_port.starts_with('[') {
        // IPv6 literal: the port, if any, follows `]:`.
        match host_port.rfind("]:") {
            Some(index) => (&host_port[..index + 1], Some(&host_port[index + 2..])),
            None => (host_port, None),
        }
    } else {
        match host_port.rsplit_once(':') {
            Some((host, port)) => (host, Some(port)),
            None => (host_port, None),
        }
    };
    if host.is_empty() {
        return None;
    }
    if let Some(port) = port {
        if !port.is_empty() && !port.bytes().all(|byte| byte.is_ascii_digit()) {
            return None;
        }
    }
    Some(())
}

// ---------------------------------------------------------------------------
// Response helpers
// ---------------------------------------------------------------------------

/// `ErrorHandler.Handle` + `http.Error` for a 500 (`graphql.go` errors are
/// plain, so `httpErrCode` is 500).
fn handler_error(message: &str, write_body: bool) -> Response<BoxBody> {
    let body = if write_body {
        format!("{message}\n")
    } else {
        "\n".to_owned()
    };
    let mut response = text_response(
        StatusCode::INTERNAL_SERVER_ERROR,
        "text/plain; charset=utf-8",
        body.into_bytes(),
        true,
    );
    response.headers_mut().insert(
        header::CACHE_CONTROL,
        HeaderValue::from_static("no-cache, max-age=0"),
    );
    response
}

fn empty_body_response(status: StatusCode) -> Response<BoxBody> {
    let mut response = Response::new(tonic::body::boxed(http_body_util::Full::new(Bytes::new())));
    *response.status_mut() = status;
    response
}

fn text_response(
    status: StatusCode,
    content_type: &str,
    body: impl Into<Bytes>,
    nosniff: bool,
) -> Response<BoxBody> {
    let mut response = Response::new(tonic::body::boxed(http_body_util::Full::new(body.into())));
    *response.status_mut() = status;
    if let Ok(value) = HeaderValue::from_str(content_type) {
        response.headers_mut().insert(header::CONTENT_TYPE, value);
    }
    if nosniff {
        response.headers_mut().insert(
            header::X_CONTENT_TYPE_OPTIONS,
            HeaderValue::from_static("nosniff"),
        );
    }
    response
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::Method;
    use http_body_util::{BodyExt, Full};
    use serde_json::json;
    use sg_config::{ServiceConnections, Unified};
    use sg_session::auth::SessionStoreError;

    fn live(site: &str) -> LiveConfig {
        LiveConfig::new(Unified {
            site: SiteConfiguration::parse(site).expect("site"),
            service_connections: ServiceConnections::default(),
        })
    }

    fn request(
        method: Method,
        path: &str,
        headers: &[(&str, &str)],
        body: &[u8],
    ) -> Request<Full<Bytes>> {
        let mut builder = Request::builder().method(method).uri(path);
        for (name, value) in headers {
            builder = builder.header(*name, *value);
        }
        builder
            .body(Full::new(Bytes::copy_from_slice(body)))
            .expect("request")
    }

    struct FakeDeps {
        response: GraphqlResponse,
        limited: Option<i64>,
    }

    #[async_trait]
    impl GraphqlDeps for FakeDeps {
        async fn exec(&self, _request: GraphqlExecRequest<'_>) -> GraphqlResponse {
            self.response.clone()
        }

        async fn rate_limit(
            &self,
            _request: GraphqlRateLimitRequest<'_>,
        ) -> Option<RateLimitOutcome> {
            self.limited
                .map(|retry_after_seconds| RateLimitOutcome::Limited {
                    retry_after_seconds,
                })
        }
    }

    fn deps(response: GraphqlResponse) -> Arc<dyn GraphqlDeps> {
        Arc::new(FakeDeps {
            response,
            limited: None,
        })
    }

    async fn run(
        site: &str,
        internal: bool,
        method: Method,
        path: &str,
        headers: &[(&str, &str)],
        body: &[u8],
        deps: Arc<dyn GraphqlDeps>,
    ) -> (StatusCode, http::HeaderMap, Vec<u8>) {
        let live = live(site);
        let mut request = request(method, path, headers, body);
        let response = graphql_response(&mut request, &live, internal, deps).await;
        let status = response.status();
        let headers = response.headers().clone();
        let body = response
            .into_body()
            .collect()
            .await
            .expect("body")
            .to_bytes();
        (status, headers, body.to_vec())
    }

    fn header(headers: &http::HeaderMap, name: header::HeaderName) -> Option<String> {
        headers
            .get(name)
            .and_then(|value| value.to_str().ok())
            .map(str::to_owned)
    }

    #[test]
    fn docs_url_resolution() {
        let cases = [
            ("", "/help/api/graphql#cost-limits"),
            (
                "https://example.com",
                "https://example.com/help/api/graphql#cost-limits",
            ),
            (
                "https://example.com/",
                "https://example.com/help/api/graphql#cost-limits",
            ),
            (
                "https://example.com/sub?q=1#f",
                "https://example.com/help/api/graphql#cost-limits",
            ),
            (
                "https://user:pass@example.com:3443/",
                "https://user:pass@example.com:3443/help/api/graphql#cost-limits",
            ),
            ("//host/path", "//host/help/api/graphql#cost-limits"),
            ("mailto:a@b.com", "mailto:///help/api/graphql#cost-limits"),
            (
                "https://sourcegraph.com/help/api/graphql#cost-limits",
                FALLBACK_DOCS_URL,
            ),
            ("not a url", "/help/api/graphql#cost-limits"),
        ];
        for (external_url, want) in cases {
            let site =
                SiteConfiguration::parse(&json!({ "externalURL": external_url }).to_string())
                    .expect("site");
            assert_eq!(docs_url(&site), want, "externalURL {external_url:?}");
        }
        for invalid in ["://bad", "https://exa mple.com", "https://example.com/%zz"] {
            let site = SiteConfiguration::parse(&json!({ "externalURL": invalid }).to_string())
                .expect("site");
            assert_eq!(
                docs_url(&site),
                FALLBACK_DOCS_URL,
                "externalURL {invalid:?}"
            );
        }
    }

    #[test]
    fn configured_limits_only_apply_when_non_positive() {
        let site = SiteConfiguration::parse(
            r#"{"rateLimits":{"graphQLMaxAliases":-1,"graphQLMaxFieldCount":7,"graphQLMaxDepth":0}}"#,
        )
        .expect("site");
        let limits = rate_limits(&site);
        assert_eq!(limits.aliases, -1);
        // A positive configured value keeps the default (upstream bug).
        assert_eq!(limits.field_count, 500_000);
        // Zero is `<= 0`, so it replaces the default.
        assert_eq!(limits.depth, 0);
        assert_eq!(limits.duplicate, 500);
        assert_eq!(limits.unique, 500);
    }

    #[test]
    fn decode_errors_match_encoding_json() {
        let cases: &[(&[u8], &str)] = &[
            (b"", "EOF"),
            (b"  \n\t ", "EOF"),
            (b"[]", "json: cannot unmarshal array into Go value of type httpapi.graphQLQueryParams"),
            (b"5", "json: cannot unmarshal number into Go value of type httpapi.graphQLQueryParams"),
            (
                b"{\"query\":5}",
                "json: cannot unmarshal number into Go struct field graphQLQueryParams.query of type string",
            ),
            (
                b"{\"variables\":[]}",
                "json: cannot unmarshal array into Go struct field graphQLQueryParams.variables of type map[string]interface {}",
            ),
            (b"x", "invalid character 'x' looking for beginning of value"),
            (b"{\"query\":\"{ hello }\"", "unexpected EOF"),
            (
                b"{\"query\":\"{ hello }\",}",
                "invalid character '}' looking for beginning of object key string",
            ),
        ];
        for (body, want) in cases {
            assert_eq!(
                decode_params(body),
                Err((*want).to_owned()),
                "body {body:?}"
            );
        }
        // `null` is a valid zero params value.
        assert_eq!(decode_params(b"null"), Ok(GraphqlQueryParams::default()));
        // Case-insensitive keys and a trailing second value (ignored).
        let params =
            decode_params(br#"{"QUERY":"{ hello }"}{"query":"{ nope }"}"#).expect("params");
        assert_eq!(params.query, "{ hello }");
    }

    #[test]
    fn gzip_header_errors_match_compress_gzip() {
        assert_eq!(validate_gzip_header(b""), Err("EOF".to_owned()));
        assert_eq!(
            validate_gzip_header(b"ab"),
            Err("unexpected EOF".to_owned())
        );
        assert_eq!(
            validate_gzip_header(b"not a gzip stream"),
            Err("gzip: invalid header".to_owned())
        );
        // Valid 10-byte header (magic, deflate, no flags) with no stream.
        assert_eq!(
            validate_gzip_header(&[0x1f, 0x8b, 0x08, 0x00, 0, 0, 0, 0, 0, 0x03]),
            Ok(())
        );
        // Reserved FLG bits.
        assert_eq!(
            validate_gzip_header(&[0x1f, 0x8b, 0x08, 0xe0, 0, 0, 0, 0, 0, 0x03]),
            Err("gzip: invalid header".to_owned())
        );
    }

    #[test]
    fn uid_prefers_actor_then_cookie_then_forwarded_for() {
        let mut headers = HeaderMap::new();
        assert_eq!(
            get_uid(&headers, Actor::Anonymous),
            ("unknown".to_owned(), false, true)
        );
        headers.insert("x-forwarded-for", HeaderValue::from_static("10.0.0.1"));
        assert_eq!(
            get_uid(&headers, Actor::Anonymous),
            ("10.0.0.1".to_owned(), true, true)
        );
        headers.insert(
            header::COOKIE,
            HeaderValue::from_static("a=b; sourcegraphAnonymousUid=\"anon\" ; c=d"),
        );
        assert_eq!(
            get_uid(&headers, Actor::Anonymous),
            ("anon".to_owned(), false, true)
        );
        assert_eq!(
            get_uid(&headers, Actor::User { uid: 42 }),
            ("42".to_owned(), false, false)
        );
    }

    #[test]
    fn audit_actor_uid_prefers_cookie_over_propagation_header() {
        let mut headers = HeaderMap::new();
        headers.insert(
            "x-sourcegraph-actor-anonymous-uid",
            HeaderValue::from_static("header-uid"),
        );
        assert_eq!(actor_anonymous_uid(&headers), "header-uid");
        headers.insert(
            header::COOKIE,
            HeaderValue::from_static("sourcegraphAnonymousUid=cookie-uid"),
        );
        assert_eq!(actor_anonymous_uid(&headers), "cookie-uid");
        headers.insert(
            header::COOKIE,
            HeaderValue::from_static("sourcegraphAnonymousUid="),
        );
        // An empty cookie is authoritative: the header fallback is not used.
        assert_eq!(actor_anonymous_uid(&headers), "");
    }

    #[tokio::test]
    async fn success_body_has_no_trailing_newline() {
        let (status, headers, body) = run(
            "{}",
            false,
            Method::POST,
            "/.api/graphql",
            &[],
            br#"{"query":"{ hello }"}"#,
            deps(GraphqlResponse {
                data: Some(r#"{"hello":"world"}"#.to_owned()),
                ..GraphqlResponse::default()
            }),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(
            header(&headers, header::CONTENT_TYPE).as_deref(),
            Some("application/json")
        );
        assert_eq!(body, br#"{"data":{"hello":"world"}}"#);
    }

    #[tokio::test]
    async fn cost_violation_is_application_json_with_newline() {
        let query = format!(
            "{{ {} }}",
            (1..=501)
                .map(|index| format!("a{index}: hello"))
                .collect::<Vec<_>>()
                .join(" ")
        );
        let body = json!({ "query": query }).to_string();
        let (status, headers, raw) = run(
            "{}",
            false,
            Method::POST,
            "/.api/graphql",
            &[],
            body.as_bytes(),
            deps(GraphqlResponse::default()),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(
            header(&headers, header::CONTENT_TYPE).as_deref(),
            Some("application/json")
        );
        assert!(raw.ends_with(b"\n"));
        let value: Value = serde_json::from_slice(&raw).expect("json");
        let errors = value["errors"].as_array().expect("errors");
        assert!(errors.iter().any(|error| {
            error["extensions"]["type"] == json!("alias count")
                && error["extensions"]["docs_url"] == json!("/help/api/graphql#cost-limits")
        }));
    }

    #[tokio::test]
    async fn internal_errors_carry_the_body_and_external_errors_do_not() {
        for (internal, want) in [
            (true, &b"failed to decode request: EOF\n"[..]),
            (false, b"\n"),
        ] {
            let (status, headers, body) = run(
                "{}",
                internal,
                Method::POST,
                "/graphql",
                &[],
                b"",
                deps(GraphqlResponse::default()),
            )
            .await;
            assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
            assert_eq!(
                header(&headers, header::CONTENT_TYPE).as_deref(),
                Some("text/plain; charset=utf-8")
            );
            assert_eq!(
                header(&headers, header::CACHE_CONTROL).as_deref(),
                Some("no-cache, max-age=0")
            );
            assert_eq!(
                header(&headers, header::X_CONTENT_TYPE_OPTIONS).as_deref(),
                Some("nosniff")
            );
            assert_eq!(body, want);
        }
    }

    #[tokio::test]
    async fn rate_limited_answers_429_with_empty_body() {
        let (status, headers, body) = run(
            "{}",
            false,
            Method::POST,
            "/.api/graphql",
            &[],
            br#"{"query":"{ hello }"}"#,
            Arc::new(FakeDeps {
                response: GraphqlResponse::default(),
                limited: Some(7),
            }),
        )
        .await;
        assert_eq!(status, StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(
            header(&headers, header::CONTENT_TYPE).as_deref(),
            Some("application/json")
        );
        assert_eq!(header(&headers, header::RETRY_AFTER).as_deref(), Some("7"));
        assert!(body.is_empty());
    }

    #[tokio::test]
    async fn gzip_request_body_is_decompressed() {
        use flate2::write::GzEncoder;
        use std::io::Write;

        let mut encoder = GzEncoder::new(Vec::new(), flate2::Compression::default());
        encoder
            .write_all(br#"{"query":"{ hello }"}"#)
            .expect("write");
        let compressed = encoder.finish().expect("finish");
        let (status, _, body) = run(
            "{}",
            false,
            Method::POST,
            "/.api/graphql",
            &[("content-encoding", "gzip")],
            &compressed,
            deps(GraphqlResponse {
                data: Some(r#"{"hello":"world"}"#.to_owned()),
                ..GraphqlResponse::default()
            }),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body, br#"{"data":{"hello":"world"}}"#);
    }

    #[test]
    fn response_json_matches_go_encoding() {
        let response = GraphqlResponse {
            errors: vec![QueryError {
                message: "bad <&>\u{2028}".to_owned(),
                locations: Some(vec![Location { line: 3, column: 7 }]),
                path: Some(vec![Value::String("x".to_owned()), Value::from(1)]),
                extensions: Some(Map::from_iter([(
                    "code".to_owned(),
                    Value::String("E<".to_owned()),
                )])),
            }],
            data: Some(r#"{"hello":"x\u003cy"}"#.to_owned()),
            extensions: Some(Map::from_iter([(
                "t".to_owned(),
                Value::String("<".to_owned()),
            )])),
        };
        assert_eq!(
            response.to_json().expect("marshal"),
            r#"{"errors":[{"message":"bad \u003c\u0026\u003e\u2028","locations":[{"line":3,"column":7}],"path":["x",1],"extensions":{"code":"E\u003c"}}],"data":{"hello":"x\u003cy"},"extensions":{"t":"\u003c"}}"#
        );

        assert_eq!(GraphqlResponse::default().to_json().unwrap(), "{}");
        let errors_only = GraphqlResponse {
            errors: vec![QueryError::new("boom")],
            ..GraphqlResponse::default()
        };
        assert_eq!(
            errors_only.to_json().unwrap(),
            r#"{"errors":[{"message":"boom"}]}"#
        );
    }

    struct FakeStore(Result<Option<Vec<u8>>, SessionStoreError>);

    #[async_trait]
    impl SessionStore for FakeStore {
        async fn load(&self, _id: &str) -> Result<Option<Vec<u8>>, SessionStoreError> {
            self.0.clone()
        }
    }

    struct FakeUsers(Result<Option<sg_session::SessionUser>, String>);

    #[async_trait]
    impl SessionUserLookup for FakeUsers {
        async fn get(&self, _uid: i32) -> Result<Option<sg_session::SessionUser>, String> {
            self.0.clone()
        }
    }

    struct FakeProfiles;

    #[async_trait]
    impl crate::graphql_resolvers::UserProfiles for FakeProfiles {
        async fn profile(
            &self,
            uid: i32,
        ) -> Result<Option<crate::graphql_resolvers::UserData>, String> {
            Ok(Some(crate::graphql_resolvers::UserData {
                id: uid,
                username: "alice".to_owned(),
                display_name: String::new(),
                avatar_url: String::new(),
                created_at: fixed_created_at(),
                updated_at: fixed_created_at(),
                site_admin: false,
                tos_accepted: false,
                primary_email: String::new(),
            }))
        }
    }

    fn current_user_schema() -> Arc<sg_graphql::Schema> {
        Arc::new(
            sg_graphql::schema::assemble_sdl(
                "type Query { currentUser: User } \
                 type User { id: ID! username: String! }",
            )
            .expect("schema"),
        )
    }

    const CREATED_AT: &str = "2026-01-01T00:00:00Z";

    fn fixed_created_at() -> time::OffsetDateTime {
        time::OffsetDateTime::parse(CREATED_AT, &time::format_description::well_known::Rfc3339)
            .expect("created at")
    }

    fn session_cookie_and_payload() -> (String, Vec<u8>) {
        let now = time::OffsetDateTime::now_utc();
        let info = sg_session::SessionInfo {
            actor: Some(sg_session::Actor {
                uid: 1,
                ..Default::default()
            }),
            last_active: sg_session::GoTime(now),
            expiry_period: 90 * 24 * 3_600 * 1_000_000_000,
            user_created_at: sg_session::GoTime(fixed_created_at()),
        };
        let json = serde_json::to_vec(&info).expect("json");
        let payload = sg_session::Session {
            timestamp: 0,
            id: "ID".to_owned(),
            values: vec![("actor".to_owned(), json)],
        }
        .encode_redis();
        let cookie = sg_session::securecookie::encode_session_id(
            COOKIE_NAME,
            b"",
            now.unix_timestamp(),
            "ID",
        );
        (cookie, payload)
    }

    fn session_user() -> sg_session::SessionUser {
        sg_session::SessionUser {
            id: 1,
            created_at: fixed_created_at(),
            invalidated_sessions_at: fixed_created_at() - time::Duration::days(1),
        }
    }

    fn session_deps(
        store: Result<Option<Vec<u8>>, SessionStoreError>,
        users: Result<Option<sg_session::SessionUser>, String>,
    ) -> Arc<dyn GraphqlDeps> {
        Arc::new(SchemaGraphqlDeps::with_session_parts(
            current_user_schema(),
            Arc::new(FakeStore(store)),
            Arc::new(FakeUsers(users)),
            Arc::new(FakeProfiles),
            Vec::new(),
        ))
    }

    #[tokio::test]
    async fn trusted_session_cookie_authenticates_current_user() {
        let (cookie, payload) = session_cookie_and_payload();
        let deps = session_deps(Ok(Some(payload)), Ok(Some(session_user())));
        let cookie_header = format!("sgs={cookie}");
        let (status, headers, body) = run(
            "{}",
            false,
            Method::POST,
            "/.api/graphql",
            &[
                ("x-requested-with", "sourcegraph"),
                ("cookie", cookie_header.as_str()),
            ],
            br#"{"query":"{ currentUser { id username } }"}"#,
            deps,
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(
            header(&headers, header::VARY).as_deref(),
            Some(SESSION_VARY)
        );
        assert_eq!(
            body,
            br#"{"data":{"currentUser":{"id":"VXNlcjox","username":"alice"}}}"#
        );
    }

    #[tokio::test]
    async fn trusted_origin_session_cookie_authenticates() {
        let (cookie, payload) = session_cookie_and_payload();
        let deps = session_deps(Ok(Some(payload)), Ok(Some(session_user())));
        let cookie_header = format!("sgs={cookie}");
        let (status, _, body) = run(
            r#"{"externalURL":"https://example.com"}"#,
            false,
            Method::POST,
            "/.api/graphql",
            &[
                ("origin", "https://example.com"),
                ("cookie", cookie_header.as_str()),
            ],
            br#"{"query":"{ currentUser { username } }"}"#,
            deps,
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body, br#"{"data":{"currentUser":{"username":"alice"}}}"#);
    }

    #[tokio::test]
    async fn untrusted_requests_never_consult_the_session() {
        let (cookie, _) = session_cookie_and_payload();
        // A Redis failure would produce a 500 if the cookie were read.
        let deps = session_deps(
            Err(SessionStoreError("connection refused".to_owned())),
            Ok(None),
        );
        let cookie_header = format!("sgs={cookie}");
        for headers in [
            vec![("cookie", cookie_header.as_str())],
            vec![
                ("origin", "https://evil.example"),
                ("cookie", cookie_header.as_str()),
            ],
        ] {
            let (status, _, body) = run(
                r#"{"externalURL":"https://example.com"}"#,
                false,
                Method::POST,
                "/.api/graphql",
                &headers,
                br#"{"query":"{ currentUser { username } }"}"#,
                Arc::clone(&deps),
            )
            .await;
            assert_eq!(status, StatusCode::OK);
            assert_eq!(body, br#"{"data":{"currentUser":null}}"#);
        }
    }

    #[tokio::test]
    async fn redis_failure_is_the_baseline_500() {
        let (cookie, _) = session_cookie_and_payload();
        let deps = session_deps(
            Err(SessionStoreError("connection refused".to_owned())),
            Ok(None),
        );
        let cookie_header = format!("sgs={cookie}");
        let (status, headers, body) = run(
            "{}",
            false,
            Method::POST,
            "/.api/graphql",
            &[
                ("x-requested-with", "sourcegraph"),
                ("cookie", cookie_header.as_str()),
            ],
            br#"{"query":"{ currentUser { username } }"}"#,
            deps,
        )
        .await;
        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(
            header(&headers, header::CONTENT_TYPE).as_deref(),
            Some("text/plain; charset=utf-8")
        );
        assert_eq!(
            header(&headers, header::VARY).as_deref(),
            Some(SESSION_VARY)
        );
        assert_eq!(body, b"failed to read session actor");
    }

    #[tokio::test]
    async fn invalid_session_cookie_is_anonymous() {
        let deps = session_deps(Ok(None), Ok(None));
        let (status, _, body) = run(
            "{}",
            false,
            Method::POST,
            "/.api/graphql",
            &[
                ("x-requested-with", "sourcegraph"),
                ("cookie", "sgs=garbage"),
            ],
            br#"{"query":"{ currentUser { username } }"}"#,
            deps,
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body, br#"{"data":{"currentUser":null}}"#);
    }

    #[tokio::test]
    async fn internal_requests_ignore_the_session_cookie() {
        let (cookie, payload) = session_cookie_and_payload();
        let deps = session_deps(Ok(Some(payload)), Ok(None));
        let cookie_header = format!("sgs={cookie}");
        let (status, headers, body) = run(
            "{}",
            true,
            Method::POST,
            "/graphql",
            &[
                ("x-requested-with", "sourcegraph"),
                ("cookie", cookie_header.as_str()),
            ],
            br#"{"query":"{ currentUser { username } }"}"#,
            deps,
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(header(&headers, header::VARY), None);
        assert_eq!(body, br#"{"data":{"currentUser":null}}"#);
    }

    #[test]
    fn session_cookie_parsing_matches_net_http() {
        let mut headers = HeaderMap::new();
        assert_eq!(session_cookie(&headers), None);
        headers.insert(
            header::COOKIE,
            HeaderValue::from_static("a=b; sgs=\"quoted\"; c=d"),
        );
        assert_eq!(session_cookie(&headers).as_deref(), Some("quoted"));
        let mut headers = HeaderMap::new();
        headers.insert(header::COOKIE, HeaderValue::from_static("sgs=first"));
        headers.append(header::COOKIE, HeaderValue::from_static("sgs=second"));
        assert_eq!(session_cookie(&headers).as_deref(), Some("first"));
    }

    #[tokio::test]
    async fn schema_deps_executes_introspection_through_the_handler() {
        let schema =
            sg_graphql::schema::assemble_sdl("type Query { hello: String! }").expect("schema");
        let deps: Arc<dyn GraphqlDeps> = Arc::new(SchemaGraphqlDeps::new(Arc::new(schema)));

        let (status, _, body) = run(
            "{}",
            false,
            Method::POST,
            "/.api/graphql",
            &[],
            br#"{"query":"{ __schema { queryType { name } } }"}"#,
            Arc::clone(&deps),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(
            body,
            br#"{"data":{"__schema":{"queryType":{"name":"Query"}}}}"#
        );

        let (status, _, body) = run(
            "{}",
            false,
            Method::POST,
            "/.api/graphql",
            &[],
            br#"{"query":"{ hello }"}"#,
            deps,
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        let body = String::from_utf8(body).expect("utf8");
        assert!(
            body.starts_with(r#"{"errors":[{"message":"GraphQL field \"hello\" of \"Query\" has no ported resolver""#),
            "unexpected body: {body}"
        );
    }
}
