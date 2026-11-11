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
use sg_session::AnonymousReason;

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

/// `(*errors.QueryError).Error()` (`graphql-go/errors/errors.go:41`), used by
/// the slow-request capture (`graphqlbackend.go:170`).
fn query_error_string(error: &QueryError) -> String {
    let locations = error
        .locations
        .iter()
        .flatten()
        .map(|location| (location.line, location.column))
        .collect::<Vec<_>>();
    crate::slow_requests_tracer::query_error_string(&error.message, &locations)
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

    /// `requestTracer.TraceQuery`'s `captureSlowRequest` hook
    /// (`graphqlbackend.go:176`). The default (transport tests, unported deps)
    /// does nothing; production inserts into the slow-request store when the
    /// request exceeds `observability.logSlowGraphQLRequests`.
    async fn capture_slow_request(
        &self,
        _capture: crate::slow_requests_tracer::SlowRequestCapture,
    ) {
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

/// `Query.clientConfiguration`'s dependencies: the `external_services` table,
/// the keyring used to decrypt service configs, and the site configuration
/// that carries `parentSourcegraph`.
struct ClientConfigurationDeps {
    source: Arc<dyn crate::client_configuration::ExternalServiceConfigSource>,
    keyring: Arc<sg_keyring::Keyring>,
    live: Arc<LiveConfig>,
}

/// `Query.externalServices`'s dependencies: the `external_services` table and
/// the keyring used to decrypt service configs. The user lookup comes from the
/// shared `users` seam.
struct ExternalServicesDeps {
    store: Arc<dyn crate::external_services::ExternalServiceConnectionStore>,
    keyring: Arc<sg_keyring::Keyring>,
    /// `conf.ExternalURL()` for `ExternalService.webhookURL`.
    live: Arc<LiveConfig>,
}

/// `Query.webhookLogs`'s dependencies: the `webhook_logs` table, the
/// `external_services` table for `WebhookLog.externalService`, and the keyring
/// used to decrypt logs and service configs.
struct WebhookLogsDeps {
    logs: Arc<dyn crate::webhook_logs::WebhookLogConnectionStore>,
    external_services: Arc<dyn crate::external_services::ExternalServiceConnectionStore>,
    keyring: Arc<sg_keyring::Keyring>,
    /// `conf.ExternalURL()` for `WebhookLog.externalService.webhookURL`.
    live: Arc<LiveConfig>,
}

/// `Query.repository(cloneURL:)` / `Query.repositoryRedirect(cloneURL:)`'s
/// code-host mapping seam (`internal/cloneurls`), backed by the live site
/// configuration, `external_services` and the keyring.
struct CloneUrlDeps {
    resolver: Arc<dyn crate::graphql_resolvers::CloneUrlResolver>,
}

/// `Mutation.logEvent` / `Mutation.logEvents`'s storage seams: the
/// `event_logs` table (shared `pool`), `redispool.Store` for the all-time
/// flags and the live site configuration (`experimentalFeatures.eventLogging`
/// and `externalURL`).
struct LogEventsDeps {
    store: Option<Arc<sg_redis::KeyValue>>,
    live: Arc<LiveConfig>,
}

/// Production ranking wiring (M9.8ii): the Pg-backed `RankingService` plus the
/// configured root graph key. The site-admin checker is built per request
/// because the baseline reads the actor from `context.Context`.
pub struct RankingServiceDeps {
    service: Arc<dyn crate::ranking_graphql::RankingService>,
    raw_graph_key_root: String,
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
    /// `Query.clientConfiguration`'s seams; `None` in transport-only tests.
    client_configuration: Option<ClientConfigurationDeps>,
    /// `Query.externalServices`'s seams; `None` in transport-only tests.
    external_services: Option<ExternalServicesDeps>,
    /// `Query.webhookLogs`'s seams; `None` in transport-only tests.
    webhook_logs: Option<WebhookLogsDeps>,
    /// `cloneURL` mapping; `None` in transport-only tests.
    clone_urls: Option<CloneUrlDeps>,
    /// `Mutation.logEvent(s)`'s seams; `None` in transport-only tests.
    log_events: Option<LogEventsDeps>,
    /// `Query.featureFlags` / `Query.featureFlag`'s database seam; `None` in
    /// transport-only tests.
    feature_flags: Option<Arc<dyn crate::graphql_resolvers::FeatureFlagStore>>,
    /// `Mutation.updateSiteConfiguration`'s configuration store/server seams;
    /// `None` in transport-only tests.
    site_configuration: Option<crate::site_configuration_mutation::SiteConfigurationDeps>,
    /// `Query.site.configuration`'s read-only seams (M9.8je); `None` leaves
    /// the configuration field unported while `Site`'s other fields resolve.
    config_graphql: Option<crate::config_graphql::ConfigGraphqlDeps>,
    /// `Query.repositories`'s database seam; `None` in transport-only tests.
    repositories_connections: Option<Arc<dyn crate::repositories::RepositoriesConnectionStore>>,
    /// `SearchContextsResolver`'s database seam (M9.8hz); `None` reproduces an
    /// absent optional resolver (SDL not appended, root fields/types absent).
    search_contexts: Option<crate::graphql_resolvers::SearchContextsDatabaseHandle>,
    /// `RankingServiceResolver`'s root resolver (M9.8ic); `None` reproduces an
    /// absent ranking surface (SDL not appended, root fields/types absent).
    ranking: Option<crate::graphql_resolvers::RankingResolverHandle>,
    /// Production ranking seam (M9.8ii): a `RankingService` whose per-request
    /// site-admin checker is derived from the executor's actor/`site_admin`.
    /// Mutually exclusive with `ranking` in practice; `ranking` wins.
    ranking_service: Option<RankingServiceDeps>,
    /// `schemaResolver.Search`'s execution seam (M9.8ie). `None` reproduces
    /// the pre-wiring frontend: the `search` root field has no prefetched
    /// value and resolves as unported (`null`).
    search_executor: Option<Arc<dyn crate::search_graphql::SearchExecutor>>,
    /// `Repository.commit`, `GitObject` and `GitRef.target`'s gitserver seam
    /// (M9.8ix). `None` reproduces the pre-wiring frontend: those fields
    /// resolve as unported (`null`/error).
    git_objects: Option<Arc<dyn crate::git_graphql::GitObjectSource>>,
    /// `symbol.DefaultZoektSymbolsClient()`'s symbols-service seam (M9.8iy).
    /// `None` makes `symbols`/`symbol` report `symbols: no endpoints
    /// configured`.
    symbols: Option<Arc<dyn crate::search_searcher_client::SymbolsClient + Send + Sync>>,
    /// `Query.slowRequests`'s capture-list seam (M9.8jf). `None` keeps the
    /// field unported (the pre-wiring frontend).
    slow_requests: Option<Arc<crate::slow_requests_tracer::SlowRequestsSurface>>,
    /// `Site.authProviders`'s live configuration source (M9.8jg). `None` keeps
    /// the field unported (transport-only tests without a configuration).
    auth_providers: Option<Arc<LiveConfig>>,
    /// `User.externalAccounts` / `Query.node(ExternalAccount)` seams (M9.8ka).
    /// `None` keeps both surfaces unported.
    external_accounts: Option<ExternalAccountsDeps>,
}

/// Seams for the external-account GraphQL surface (M9.8ka): the
/// `user_external_accounts` table, the user-external-account encryption key and
/// the live configuration that supplies `auth.providers`/`ssc.samsHostName`
/// plus the dotcom switch.
pub struct ExternalAccountsDeps {
    pub store: Arc<dyn crate::external_accounts_graphql::ExternalAccountsStore>,
    pub key: Option<Arc<dyn sg_encryption::Key>>,
    pub live: Arc<LiveConfig>,
    pub dotcom: bool,
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
            client_configuration: None,
            external_services: None,
            webhook_logs: None,
            clone_urls: None,
            log_events: None,
            feature_flags: None,
            site_configuration: None,
            config_graphql: None,
            repositories_connections: None,
            search_contexts: None,
            ranking: None,
            ranking_service: None,
            search_executor: None,
            git_objects: None,
            symbols: None,
            slow_requests: None,
            auth_providers: None,
            external_accounts: None,
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
            pool: Some(pool.clone()),
            rate_limit: None,
            client_configuration: None,
            external_services: None,
            webhook_logs: None,
            clone_urls: None,
            log_events: None,
            feature_flags: Some(Arc::new(
                crate::feature_flag_store::PgFeatureFlagStore::new(pool.clone()),
            )),
            site_configuration: None,
            config_graphql: None,
            repositories_connections: Some(Arc::new(
                crate::repositories::PgRepositoriesConnectionStore::new(pool),
            )),
            search_contexts: None,
            ranking: None,
            ranking_service: None,
            search_executor: None,
            git_objects: None,
            symbols: None,
            slow_requests: None,
            auth_providers: None,
            external_accounts: None,
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

    /// `serve_cmd.go:143`'s `keyring.Init` + the `external_services` table:
    /// enables `Query.clientConfiguration`.
    pub fn with_client_configuration(
        mut self,
        source: Arc<dyn crate::client_configuration::ExternalServiceConfigSource>,
        keyring: Arc<sg_keyring::Keyring>,
        live: Arc<LiveConfig>,
    ) -> Self {
        self.client_configuration = Some(ClientConfigurationDeps {
            source,
            keyring,
            live,
        });
        self
    }

    /// Enable `Query.externalServices`: the `external_services` table and the
    /// keyring used to decrypt/redact service configs.
    pub fn with_external_services(
        mut self,
        store: Arc<dyn crate::external_services::ExternalServiceConnectionStore>,
        keyring: Arc<sg_keyring::Keyring>,
        live: Arc<LiveConfig>,
    ) -> Self {
        self.external_services = Some(ExternalServicesDeps {
            store,
            keyring,
            live,
        });
        self
    }

    /// Enable `Query.webhookLogs`: the `webhook_logs` table, the
    /// `external_services` table for `WebhookLog.externalService`, and the
    /// keyring used to decrypt logs and service configs.
    pub fn with_webhook_logs(
        mut self,
        logs: Arc<dyn crate::webhook_logs::WebhookLogConnectionStore>,
        external_services: Arc<dyn crate::external_services::ExternalServiceConnectionStore>,
        keyring: Arc<sg_keyring::Keyring>,
        live: Arc<LiveConfig>,
    ) -> Self {
        self.webhook_logs = Some(WebhookLogsDeps {
            logs,
            external_services,
            keyring,
            live,
        });
        self
    }

    /// Enable `cloneURL` resolution for `Query.repository` /
    /// `Query.repositoryRedirect` (`internal/cloneurls`).
    pub fn with_clone_urls(
        mut self,
        resolver: Arc<dyn crate::graphql_resolvers::CloneUrlResolver>,
    ) -> Self {
        self.clone_urls = Some(CloneUrlDeps { resolver });
        self
    }

    /// Enable the symbols service calls behind `TreeEntry.symbols` /
    /// `TreeEntry.symbol` / `GitCommit.symbols` (M9.8iy).
    pub fn with_symbols(
        mut self,
        client: Arc<dyn crate::search_searcher_client::SymbolsClient + Send + Sync>,
    ) -> Self {
        self.symbols = Some(client);
        self
    }

    /// Enable the deprecated `Mutation.logEvent` / `Mutation.logEvents`
    /// writes: the `event_logs` table (via `pool`), the store Redis pool and
    /// the live site configuration that gates logging.
    pub fn with_log_events(
        mut self,
        store: Option<Arc<sg_redis::KeyValue>>,
        live: Arc<LiveConfig>,
    ) -> Self {
        self.log_events = Some(LogEventsDeps { store, live });
        self
    }

    /// Enable `Mutation.updateSiteConfiguration`: the process' raw
    /// configuration store and its configuration server.
    pub fn with_site_configuration(
        mut self,
        store: Arc<sg_config::store::ConfigStore>,
        server: Arc<crate::configuration_server::DbConfigServer>,
    ) -> Self {
        self.site_configuration =
            Some(crate::site_configuration_mutation::SiteConfigurationDeps { store, server });
        self
    }

    /// Enable `Site.authProviders` (M9.8jg): the provider directory is derived
    /// from the live `auth.providers` site configuration on every request.
    pub fn with_auth_providers(mut self, live: Arc<LiveConfig>) -> Self {
        self.auth_providers = Some(live);
        self
    }

    /// Enable the external-account GraphQL surface (M9.8ka): the
    /// `user_external_accounts` table, the user-external-account encryption key
    /// (`keyring.UserExternalAccountKey`) and the live configuration.
    pub fn with_external_accounts(
        mut self,
        store: Arc<dyn crate::external_accounts_graphql::ExternalAccountsStore>,
        key: Option<Arc<dyn sg_encryption::Key>>,
        live: Arc<LiveConfig>,
        dotcom: bool,
    ) -> Self {
        self.external_accounts = Some(ExternalAccountsDeps {
            store,
            key,
            live,
            dotcom,
        });
        self
    }

    /// Enable the read-only configuration surface (`Query.site.configuration`,
    /// M9.8je): the configuration store, the `critical_and_site_config` table
    /// and the shared `product_licenses` adapter.
    pub fn with_config_graphql(
        mut self,
        store: Arc<sg_config::store::ConfigStore>,
        pool: sqlx::PgPool,
    ) -> Self {
        self.config_graphql = Some(crate::config_graphql::ConfigGraphqlDeps::new(pool, store));
        self
    }

    /// Test hook: install configuration-surface seams directly (a fake license
    /// store can be supplied through
    /// [`crate::config_graphql::ConfigGraphqlDeps::with_license_store`]).
    pub fn with_config_graphql_deps(
        mut self,
        deps: crate::config_graphql::ConfigGraphqlDeps,
    ) -> Self {
        self.config_graphql = Some(deps);
        self
    }

    /// Enable the search contexts GraphQL surface (M9.8hz): append the
    /// `searchContexts.graphql` SDL to the assembled schema and hand the
    /// database seam to the executor's root objects.
    ///
    /// This mirrors `NewSchema`'s
    /// `schemaResolver.SearchContextsResolver != nil` branch: the optional
    /// resolver is present, so the root fields and Node kind are registered.
    pub fn with_search_contexts(
        mut self,
        db: crate::graphql_resolvers::SearchContextsDatabaseHandle,
    ) -> Result<Self, sg_graphql::schema::SchemaError> {
        let registered = crate::search_contexts_schema::register_search_contexts_schema(
            &self.schema,
            Some(db.as_ref()),
        )?;
        self.schema = Arc::new(registered.schema);
        self.search_contexts = Some(db);
        Ok(self)
    }

    /// `internal/searchcontexts/init.go:26`'s switch: register the surface
    /// only when `enabled` **and** `searchcontexts.IsEnabled()`. When either is
    /// false the base schema stays untouched, reproducing the optional
    /// resolver's absence.
    pub fn with_search_contexts_switch(
        mut self,
        db: crate::graphql_resolvers::SearchContextsDatabaseHandle,
        enabled: bool,
    ) -> Result<Self, sg_graphql::schema::SchemaError> {
        let registered = crate::search_contexts_schema::register_with_switch(
            &self.schema,
            db.as_ref(),
            enabled,
        )?;
        let enabled = registered.is_enabled();
        self.schema = Arc::new(registered.schema);
        self.search_contexts = enabled.then_some(db);
        Ok(self)
    }

    /// Enable the ranking GraphQL surface (M9.8ic): append
    /// `codeintel.ranking.graphql` to the assembled schema and hand the root
    /// resolver to the executor.
    ///
    /// This mirrors an `enterpriseServices.CodeIntelResolver` that includes the
    /// ranking root resolver: the optional surface is present, so
    /// `Query.rankingSummary` and the two ranking mutations are registered.
    /// When it is never called the base schema and every existing response stay
    /// byte-identical (the optional resolver's absence).
    pub fn with_ranking(
        mut self,
        resolver: crate::graphql_resolvers::RankingResolverHandle,
    ) -> Result<Self, sg_graphql::schema::SchemaError> {
        self.schema = Arc::new(crate::ranking_graphql::append_ranking_schema(&self.schema)?);
        self.ranking = Some(resolver);
        Ok(self)
    }

    /// Production ranking wiring (M9.8ii): append the ranking SDL and drive the
    /// three root operations through `service` after a per-request site-admin
    /// check, mirroring `serve_cmd.go`'s
    /// `rankinggraphql.NewRootResolver(..., RankingService, siteAdminChecker)`.
    ///
    /// The caller decides whether the surface is available (e.g. the ranking
    /// tables exist); when never called the base schema and every response stay
    /// byte-identical.
    pub fn with_ranking_service(
        mut self,
        service: Arc<dyn crate::ranking_graphql::RankingService>,
        raw_graph_key_root: impl Into<String>,
    ) -> Result<Self, sg_graphql::schema::SchemaError> {
        self.schema = Arc::new(crate::ranking_graphql::append_ranking_schema(&self.schema)?);
        self.ranking_service = Some(RankingServiceDeps {
            service,
            raw_graph_key_root: raw_graph_key_root.into(),
        });
        Ok(self)
    }

    /// Inject the `Query.search` execution seam (M9.8ie).
    ///
    /// The baseline `schemaResolver.Search` plans and executes through the
    /// search client; this port hands the async planning/execution to the
    /// executor so `exec` can prefetch the root field and replay the owned
    /// result snapshot synchronously. When never called, no prefetch happens
    /// and every existing response stays byte-identical.
    pub fn with_search_executor(
        mut self,
        executor: Arc<dyn crate::search_graphql::SearchExecutor>,
    ) -> Self {
        self.search_executor = Some(executor);
        self
    }

    /// Wire the gitserver object/commit seam (M9.8ix).
    ///
    /// `Repository.commit`, `GitObject` and `GitRef.target` read through
    /// gitserver, so `exec` performs those lookups asynchronously and replays
    /// them through the synchronous resolvers. When never called the fields
    /// keep their pre-wiring (unported) behavior.
    pub fn with_git_objects(
        mut self,
        source: Arc<dyn crate::git_graphql::GitObjectSource>,
    ) -> Self {
        self.git_objects = Some(source);
        self
    }

    /// Wire the `Query.slowRequests` capture-list seam (M9.8jf).
    ///
    /// The surface carries the FIFO store and the configured capture limit;
    /// when never called the field keeps its pre-wiring (unported) behavior.
    pub fn with_slow_requests(
        mut self,
        surface: Arc<crate::slow_requests_tracer::SlowRequestsSurface>,
    ) -> Self {
        self.slow_requests = Some(surface);
        self
    }

    /// Assemble the M9.8ii production surfaces in one testable step.
    ///
    /// This is the same chain `service.rs` uses: search contexts are gated by
    /// [`search_contexts_enabled`](crate::service::search_contexts_enabled), the
    /// ranking surface is only installed when its tables exist, and
    /// `Query.search` reuses the `SearchDeps` behind `/.api/search/stream`.
    /// `None` for any surface leaves the schema untouched, so the existing
    /// Unported/absent paths stay byte-identical.
    pub fn with_production_surfaces(
        mut self,
        search_contexts: Option<crate::graphql_resolvers::SearchContextsDatabaseHandle>,
        search_contexts_enabled: bool,
        ranking: Option<(Arc<dyn crate::ranking_graphql::RankingService>, String)>,
        search_executor: Option<Arc<dyn crate::search_graphql::SearchExecutor>>,
    ) -> Result<Self, sg_graphql::schema::SchemaError> {
        if let Some(db) = search_contexts {
            self = self.with_search_contexts_switch(db, search_contexts_enabled)?;
        }
        if let Some((service, raw_graph_key_root)) = ranking {
            self = self.with_ranking_service(service, raw_graph_key_root)?;
        }
        if let Some(executor) = search_executor {
            self = self.with_search_executor(executor);
        }
        Ok(self)
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
            client_configuration: None,
            external_services: None,
            webhook_logs: None,
            clone_urls: None,
            log_events: None,
            feature_flags: None,
            site_configuration: None,
            config_graphql: None,
            repositories_connections: None,
            search_contexts: None,
            ranking: None,
            ranking_service: None,
            search_executor: None,
            git_objects: None,
            symbols: None,
            slow_requests: None,
            auth_providers: None,
            external_accounts: None,
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
        // Internal actors are treated as site admins by every
        // `internal/auth` gate (`CheckCurrentUserIsSiteAdmin`,
        // `CheckSiteAdminOrSameUserFromActor`, `CheckOrgAccessOrSiteAdmin`
        // all return nil for `IsInternal`). They still have no current user
        // (`UID == 0` → `GetByCurrentAuthUser` misses).
        if request.actor.is_internal() {
            site_admin = true;
        }

        // `conf.ExternalURL()` for the `ExternalService.webhookURL` fields.
        let external_url = self
            .external_services
            .as_ref()
            .map(|deps| deps.live.as_ref())
            .or_else(|| self.webhook_logs.as_ref().map(|deps| deps.live.as_ref()))
            .map(|live| crate::log_events::external_url(&live.current().site).to_owned())
            .unwrap_or_default();

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
        let clone_urls = self
            .clone_urls
            .as_ref()
            .map(|deps| deps.resolver.as_ref() as &dyn crate::graphql_resolvers::CloneUrlResolver);
        let repositories = match &self.repositories {
            Some(store) => {
                crate::graphql_resolvers::prefetch_repositories(
                    store.as_ref(),
                    clone_urls,
                    &fields,
                    request.actor,
                    site_admin,
                )
                .await
            }
            None => std::collections::HashMap::new(),
        };
        // `Query.repositoryRedirect` shares the repository store and authz
        // predicate with `Query.repository`.
        let repository_redirects = match &self.repositories {
            Some(store) => {
                crate::graphql_resolvers::prefetch_repository_redirects(
                    store.as_ref(),
                    clone_urls,
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
        // `Mutation.settingsMutation` / `Mutation.logEvent(s)`: the executor
        // resolves fields synchronously, so run the (async) write paths now.
        // Only mutation operations perform side effects.
        let is_mutation = sg_graphql::exec::is_mutation_operation(&exec_request);
        // `CodeIntelResolver.RankingSummary` / `BumpDerivativeGraphKey` /
        // `DeleteRankingProgress` (M9.8ic): the resolver is async, so run the
        // selected root operations before the synchronous executor.
        let ranking_roots = match (&self.ranking, &self.ranking_service) {
            (Some(resolver), _) => {
                crate::graphql_resolvers::prefetch_ranking_roots(
                    resolver.as_ref(),
                    &fields,
                    is_mutation,
                )
                .await
            }
            (None, Some(deps)) => {
                // `auth.CheckCurrentUserIsSiteAdmin(ctx, db)` reads the actor
                // from the request context; build the equivalent checker from
                // the authenticated request and run the same prefetch path.
                let checker: Arc<dyn crate::ranking_graphql::SiteAdminChecker> = Arc::new(
                    crate::ranking_graphql::ViewerSiteAdminChecker::new(request.actor, site_admin),
                );
                let resolver = crate::ranking_graphql::RootResolver::new(
                    Arc::clone(&deps.service),
                    checker,
                    Arc::new(crate::ranking_graphql::NoopObserver),
                    deps.raw_graph_key_root.clone(),
                );
                crate::graphql_resolvers::prefetch_ranking_roots(&resolver, &fields, is_mutation)
                    .await
            }
            (None, None) => crate::graphql_resolvers::RankingRoots::default(),
        };
        // `schemaResolver.Search` (M9.8ie): plan + execute through the
        // injected seam before the synchronous executor replays the snapshot.
        let search_root = match &self.search_executor {
            Some(executor) => {
                crate::search_graphql::prefetch_search_roots(executor.as_ref(), &fields).await
            }
            None => None,
        };
        let mut mutation = match (&self.pool, &self.org_memberships) {
            (Some(pool), Some(store)) if is_mutation => {
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
        if let (Some(pool), Some(deps)) = (&self.pool, &self.log_events) {
            if is_mutation {
                let site = deps.live.current();
                let outcomes = crate::log_events::prefetch_log_events(
                    pool,
                    deps.store.as_deref(),
                    &site.site,
                    request.actor.uid().unwrap_or(0),
                    &fields,
                )
                .await;
                for (field, outcome) in outcomes {
                    mutation = mutation.with_log_events(field, outcome);
                }
            }
        }
        // `Query.temporarySettings` and the two temporary-setting mutations
        // share the `temporary_settings` table and the auth check
        // (`temporary_settings.go:18-52`). A document is either a query or a
        // mutation, so at most one side runs per request.
        let (temporary_settings, temporary_settings_writes) = match &self.pool {
            Some(pool) => {
                let uid = request.actor.uid().unwrap_or(0);
                let read =
                    crate::temporary_settings::prefetch_temporary_settings(pool, uid, &fields)
                        .await;
                let writes = if is_mutation {
                    crate::temporary_settings::prefetch_temporary_settings_mutations(
                        pool, uid, &fields,
                    )
                    .await
                } else {
                    Vec::new()
                };
                (read, writes)
            }
            None => (None, Vec::new()),
        };
        for (field, outcome) in temporary_settings_writes {
            mutation = mutation.with_temporary_settings(field, outcome);
        }
        // `Mutation.createFeatureFlag` / `updateFeatureFlag` /
        // `deleteFeatureFlag`: site-admin gated writes through the
        // `feature_flags` table (`feature_flags.go:236-288`).
        if let (Some(store), Some(namespaces)) = (&self.feature_flags, &self.org_memberships) {
            if is_mutation {
                let outcomes = crate::feature_flag_mutations::prefetch_feature_flag_mutations(
                    store.as_ref(),
                    namespaces.as_ref(),
                    &fields,
                    request.actor,
                    site_admin,
                )
                .await;
                for (field, outcome) in outcomes {
                    mutation = mutation.with_feature_flag_mutation(field, outcome);
                }
            }
        }
        // `Mutation.updateSiteConfiguration`: site-admin gated write through
        // the configuration server (`site.go:294-362`).
        if let (Some(pool), Some(deps)) = (&self.pool, &self.site_configuration) {
            if is_mutation {
                let outcomes =
                    crate::site_configuration_mutation::prefetch_update_site_configuration(
                        pool,
                        deps,
                        &fields,
                        request.actor,
                        site_admin,
                    )
                    .await;
                for (field, outcome) in outcomes {
                    mutation = mutation.with_site_configuration(field, outcome);
                }
            }
        }
        let mut nodes = match (&self.org_memberships, &self.repositories) {
            (Some(memberships_store), Some(repositories)) => {
                crate::graphql_resolvers::prefetch_node_roots(
                    memberships_store.as_ref(),
                    repositories.as_ref(),
                    self.external_services
                        .as_ref()
                        .map(|deps| deps.store.as_ref()),
                    self.webhook_logs.as_ref().map(|deps| deps.logs.as_ref()),
                    self.users.as_ref().map(|users| {
                        users.as_ref() as &dyn crate::graphql_resolvers::UserLookupStore
                    }),
                    self.external_services
                        .as_ref()
                        .and_then(|deps| deps.keyring.external_service_key())
                        .or_else(|| {
                            self.webhook_logs
                                .as_ref()
                                .and_then(|deps| deps.keyring.external_service_key())
                        }),
                    self.webhook_logs
                        .as_ref()
                        .and_then(|deps| deps.keyring.webhook_log_key()),
                    &external_url,
                    &fields,
                    request.actor,
                    site_admin,
                )
                .await
            }
            _ => crate::graphql_resolvers::NodeRoots::default(),
        };
        // `Query.node(kind: "ExternalAccount")` (M9.8ka): the row lookup and
        // `ExternalAccount.user` are async, so resolve the whole node now.
        let external_accounts_registry = self.external_accounts.as_ref().map(|deps| {
            crate::external_accounts_graphql::registry_from_site(&deps.live.current().site)
        });
        if let (Some(deps), Some(registry)) = (&self.external_accounts, &external_accounts_registry)
        {
            let lookups = crate::external_accounts_graphql::prefetch_external_account_nodes(
                deps.store.as_ref(),
                registry,
                deps.key.clone(),
                self.users
                    .as_ref()
                    .map(|users| users.as_ref() as &dyn crate::graphql_resolvers::UserLookupStore),
                &fields,
                request.actor,
                site_admin,
            )
            .await;
            if !lookups.is_empty() {
                nodes.external_accounts = lookups;
            }
        }
        // `Repository.commit`, `GitObject` and `GitRef.target` (M9.8ix) read
        // through gitserver; perform those lookups now and replay them from the
        // synchronous resolvers. `repositories`/`repository_redirects`/`nodes`/
        // `search_root` are still owned here (they move into the roots below).
        let git = match &self.git_objects {
            Some(source) => Some(Arc::new(
                crate::git_graphql::prefetch_git_with(
                    source.as_ref(),
                    &repositories,
                    &repository_redirects,
                    &nodes,
                    search_root.as_ref(),
                    &fields,
                    self.symbols.clone(),
                    self.clone_urls.as_ref().map(|deps| {
                        deps.resolver.as_ref() as &dyn crate::graphql_resolvers::CloneUrlResolver
                    }),
                )
                .await,
            )),
            None => None,
        };
        // `Query.namespace`/`Query.namespaceByName` share the user/org store
        // seam with `Query.node`.
        let namespaces = match &self.org_memberships {
            Some(store) => {
                crate::graphql_resolvers::prefetch_namespace_roots(
                    store.as_ref(),
                    &fields,
                    request.actor,
                    site_admin,
                )
                .await
            }
            None => crate::graphql_resolvers::NamespaceRoots::default(),
        };
        // `Query.featureFlags` / `Query.featureFlag`: both are site-admin
        // gated; the override namespaces reuse the user/org membership store.
        let feature_flags = match (&self.feature_flags, &self.org_memberships) {
            (Some(store), Some(namespaces)) => {
                crate::graphql_resolvers::prefetch_feature_flags(
                    store.as_ref(),
                    namespaces.as_ref(),
                    &fields,
                    request.actor,
                    site_admin,
                )
                .await
            }
            _ => crate::graphql_resolvers::FeatureFlagRoots::default(),
        };
        let mut nested = match (
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
        // `User.externalAccounts` (M9.8ka): prefetch every selected connection
        // and the `CodySubscription` inputs the node objects replay.
        if let (Some(deps), Some(registry)) = (&self.external_accounts, &external_accounts_registry)
        {
            let lookup = crate::graphql_resolvers::prefetch_user_external_accounts(
                deps.store.as_ref(),
                registry,
                deps.key.clone(),
                &fields,
                current_user.as_ref(),
                &users,
                &users_connections,
                &nodes,
                request.actor,
                site_admin,
            )
            .await;
            if !lookup.is_empty() {
                Arc::make_mut(&mut nested).user_external_accounts = lookup;
            }
            Arc::make_mut(&mut nested).external_account_cody = Some(Arc::new(
                crate::external_accounts_graphql::CodySubscriptionConfig {
                    dotcom: deps.dotcom,
                    sams_service_id: crate::external_accounts_graphql::sams_service_id(
                        &deps.live.current().site,
                    ),
                },
            ));
        }
        let user = current_user.map(crate::graphql_resolvers::UserObject::from_data);
        // `Query.clientConfiguration` reads `external_services` and the
        // site configuration's `parentSourcegraph`; prefetch only when the
        // operation selects it (the baseline resolves fields lazily).
        let client_configuration = match &self.client_configuration {
            Some(deps) => {
                crate::client_configuration::prefetch_client_configuration(
                    deps.source.as_ref(),
                    deps.keyring.external_service_key(),
                    crate::client_configuration::parent_sourcegraph_url(&deps.live.current().site),
                    &fields,
                )
                .await
            }
            None => None,
        };
        // `Query.externalServices` is site-admin only; the prefetched lookup
        // carries the connection or the authorization/argument error.
        let external_services = match &self.external_services {
            Some(deps) => {
                crate::external_services::prefetch_external_services(
                    deps.store.as_ref(),
                    self.users.as_ref().map(|users| {
                        users.as_ref() as &dyn crate::graphql_resolvers::UserLookupStore
                    }),
                    deps.keyring.external_service_key(),
                    &external_url,
                    &fields,
                    request.actor,
                    user.as_ref().map(|user| user.user_data()),
                )
                .await
            }
            None => std::collections::HashMap::new(),
        };
        // `Query.webhookLogs` is site-admin only; the prefetched lookup
        // carries the connection, filters or the authorization/argument
        // error.
        let webhook_logs = match &self.webhook_logs {
            Some(deps) => {
                crate::webhook_logs::prefetch_webhook_logs(
                    deps.logs.as_ref(),
                    deps.external_services.as_ref(),
                    self.users.as_ref().map(|users| {
                        users.as_ref() as &dyn crate::graphql_resolvers::UserLookupStore
                    }),
                    deps.keyring.webhook_log_key(),
                    deps.keyring.external_service_key(),
                    &external_url,
                    &fields,
                    request.actor,
                    site_admin,
                )
                .await
            }
            None => std::collections::HashMap::new(),
        };
        // `Query.repositories`: the connection's page and total count are
        // prefetched with the actor/viewer used by `AuthzQueryConds`.
        let repositories_connections = match &self.repositories_connections {
            Some(store) => {
                crate::repositories::prefetch_repositories_connections(
                    store.as_ref(),
                    &fields,
                    request.actor,
                    site_admin,
                )
                .await
            }
            None => std::collections::HashMap::new(),
        };
        // `ExternalService.webhookLogs` is a nested connection; prefetch it
        // wherever an `ExternalService` selection set includes it. The field
        // is not part of the OSS SDL in this snapshot (overview §0), so this
        // is data-layer only, but keeps the resolver semantics aligned.
        if let (Some(logs_deps), Some(services_deps)) =
            (&self.webhook_logs, &self.external_services)
        {
            let nested_webhook_logs = crate::webhook_logs::prefetch_external_service_webhook_logs(
                logs_deps.logs.as_ref(),
                services_deps.store.as_ref(),
                self.users
                    .as_ref()
                    .map(|users| users.as_ref() as &dyn crate::graphql_resolvers::UserLookupStore),
                logs_deps.keyring.webhook_log_key(),
                services_deps.keyring.external_service_key(),
                &external_url,
                &fields,
                &external_services,
                &webhook_logs,
                &nodes,
                request.actor,
                site_admin,
            )
            .await;
            if !nested_webhook_logs.is_empty() {
                Arc::make_mut(&mut nested).external_service_webhook_logs = nested_webhook_logs;
            }
        }
        // `Query.site.configuration` (`site.go:100-272`): admin checks,
        // `conf.Raw()`, `product_licenses` and the config history are async, so
        // prefetch the selected read-only fields before the synchronous
        // executor replays them.
        let (site, settings_subject_site, cascade_site_configuration) = match &self.config_graphql {
            Some(deps) => {
                let backend = crate::config_graphql::ProductionSiteConfigurationBackend::new(
                    deps.pool.clone(),
                    Arc::clone(&deps.store),
                    site_admin,
                );
                let validator = crate::config_graphql::ConfigStoreSiteConfigValidator::new(
                    Arc::clone(&deps.store),
                );
                let history =
                    crate::config_graphql::PgSiteConfigHistoryStore::new(deps.pool.clone());
                let lookup_users = self
                    .users
                    .as_ref()
                    .map(|users| users.as_ref() as &dyn crate::graphql_resolvers::UserLookupStore);
                let site = crate::config_graphql::prefetch_site(
                    &backend,
                    deps.licenses.as_ref(),
                    &validator,
                    &history,
                    lookup_users,
                    &fields,
                    request.actor,
                )
                .await;
                // `Query.settingsSubject(id: <Site>).configuration` is a separate
                // root entry point; the baseline audits/checks it per resolver.
                let settings_subject_site =
                    crate::config_graphql::prefetch_settings_subject_site_configuration(
                        &backend,
                        deps.licenses.as_ref(),
                        &validator,
                        &history,
                        lookup_users,
                        &fields,
                        request.actor,
                    )
                    .await
                    .map(|configuration| crate::config_graphql::SiteRoot {
                        configuration: Some(configuration),
                    });
                // `settingsCascade.subjects[... on Site].configuration`
                // (`settings_cascade.go:27-37`, `site.go:100-138`): same
                // resolver, prefetched once per operation (M9.8jl).
                let cascade_site_configuration =
                    crate::config_graphql::prefetch_cascade_subject_site_configuration(
                        &backend,
                        deps.licenses.as_ref(),
                        &validator,
                        &history,
                        lookup_users,
                        &fields,
                        request.actor,
                    )
                    .await;
                (site, settings_subject_site, cascade_site_configuration)
            }
            None => (None, None, None),
        };
        // The Site subject reached through a settings cascade replays the
        // prefetched configuration from `NestedPrefetch` (M9.8jl).
        Arc::make_mut(&mut nested).cascade_site_configuration = cascade_site_configuration;
        // `Query.slowRequests` (`slow_requests_tracer.go:63`): the `after`
        // cursor and the nodes' `user`/`repository` rows are async, so resolve
        // whole connections before the sync executor replays the fields.
        let slow_requests_prefetch = match &self.slow_requests {
            Some(surface) => {
                crate::slow_requests_tracer::prefetch_slow_requests(
                    surface.as_ref(),
                    self.users.as_ref().map(|users| {
                        users.as_ref() as &dyn crate::graphql_resolvers::UserLookupStore
                    }),
                    self.repositories.as_ref().map(|store| {
                        store.as_ref() as &dyn crate::graphql_resolvers::RepositoryLookup
                    }),
                    &fields,
                    request.actor,
                    site_admin,
                )
                .await
            }
            None => std::collections::HashMap::new(),
        };
        // `Site.authProviders` (`auth_providers.go:10`): the baseline's
        // `providers.SortedProviders()` reflects the current `conf.Get()`; do
        // the same from the live snapshot, per request. Both `Query.site` and
        // the cascade's Site subject replay this snapshot (M9.8jl).
        let auth_providers_root = self.auth_providers.as_ref().map(|live| {
            Arc::new(crate::auth_providers_graphql::AuthProvidersRoot::from_site(
                &live.current().site,
            ))
        });
        Arc::make_mut(&mut nested).auth_providers = auth_providers_root.clone();
        let mut roots =
            crate::graphql_resolvers::FrontendRootResolver::with_prefetched_organizations(
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
            .with_namespaces(namespaces)
            .with_repository_redirects(repository_redirects)
            .with_temporary_settings(temporary_settings)
            .with_feature_flags(feature_flags)
            .with_external_services(external_services)
            .with_webhook_logs(webhook_logs)
            .with_repositories_connections(repositories_connections)
            .with_mutation(mutation)
            .with_search_contexts(self.search_contexts.clone())
            .with_ranking_roots(ranking_roots)
            .with_search_roots(search_root)
            .with_git(git)
            .with_site(site)
            .with_settings_subject_site_configuration(settings_subject_site);
        if let Some(auth_providers_root) = auth_providers_root {
            roots = roots.with_auth_providers(Some(auth_providers_root));
        }
        if let Some(surface) = &self.slow_requests {
            roots = roots.with_slow_requests(Arc::clone(surface));
        }
        roots = roots.with_slow_requests_prefetch(slow_requests_prefetch);
        if let Some(client_configuration) = client_configuration {
            roots = roots.with_client_configuration(client_configuration);
        }
        let response = sg_graphql::exec::execute_with(&self.schema, exec_request, Some(&roots));
        GraphqlResponse::from_exec(&response)
    }

    async fn capture_slow_request(&self, capture: crate::slow_requests_tracer::SlowRequestCapture) {
        let Some(surface) = self.slow_requests.as_ref() else {
            return;
        };
        // The baseline logs marshal/insert failures and continues
        // (`captureSlowRequest`).
        if let Err(error) = surface.capture(&capture) {
            tracing::warn!(%error, "failed to capture slow GraphQL request");
        }
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
    // The external actor chain already appended the session middleware's
    // `Vary` values before dispatching (and resolved the session actor); only
    // direct callers without the chain need the fallback.
    if !internal
        && req
            .extensions()
            .get::<crate::actor_chain::ChainActor>()
            .is_none()
    {
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

    // The external chain (`session.CookieMiddlewareWithCSRFSafety`) and the
    // internal actor middleware both insert the resolved actor. Direct callers
    // (tests) fall back to the session lookup / anonymous actor.
    let actor = if let Some(crate::actor_chain::ChainActor(chain_actor)) =
        req.extensions().get::<crate::actor_chain::ChainActor>()
    {
        // An internal actor keeps its distinct executor shape; Go's
        // `IsAuthenticated` is `UID != 0`, so an internal actor (UID 0) would
        // otherwise be indistinguishable from anonymous.
        chain_actor.to_graphql()
    } else if internal {
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
        actor: {
            let mut audit_actor = crate::audit::AuditActor::new(
                actor.uid(),
                match actor {
                    Actor::User { .. } => String::new(),
                    Actor::Anonymous | Actor::Internal => actor_anonymous_uid(req.headers()),
                },
            );
            // `audit.Log` drops internal actors unless `internalTraffic`
            // auditing is enabled (`internal/audit/audit.go:29-33`).
            audit_actor.internal = actor.is_internal();
            audit_actor
        },
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
    // `conf.Get().ObservabilityLogSlowGraphQLRequests`, read at request time.
    let slow_threshold_ms = crate::trace_policy::log_slow_graphql_requests(&site);
    let mut timer = RequestTimer::new(&params.query, &request_name, site, audit_context);

    // Calculating the cost is cheaper than validating the schema
    // (`graphql.go:159`).
    if let Ok(cost) = estimate_query_cost(&params.query, &variables) {
        // `graphql.go:168-169`: observed before the violation and rate-limit
        // checks, so rejected requests still count.
        crate::graphql_metrics::observe_cost(
            &crate::graphql_metrics::actor_type_label(
                internal,
                !actor.is_authenticated(),
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
    // `requestTracer.TraceQuery`: `start := time.Now()` (before schema.Exec)
    // then `time.Since(start)` in the finish callback.
    let slow_start = time::OffsetDateTime::now_utc();
    let slow_exec_started = Instant::now();
    let response = deps
        .exec(GraphqlExecRequest {
            query: &params.query,
            operation_name: &params.operation_name,
            variables: &params.variables,
            actor,
        })
        .await;
    let slow_duration_ns =
        i64::try_from(slow_exec_started.elapsed().as_nanos()).unwrap_or(i64::MAX);
    timer.finish(response.errors.is_empty());
    deps.capture_slow_request(crate::slow_requests_tracer::SlowRequestCapture {
        start: crate::slow_requests_tracer::rfc3339_nanos(slow_start),
        duration_ns: slow_duration_ns,
        threshold_ms: slow_threshold_ms,
        user_id: actor.uid().unwrap_or(0),
        name: request_name,
        source: request_source,
        variables: params.variables.clone(),
        errors: response.errors.iter().map(query_error_string).collect(),
        query: params.query.clone(),
    })
    .await;
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
    let security = crate::mux::SecurityConfig::from_site_config(&live.current().site);
    crate::actor_middleware::is_trusted_request(headers, &security)
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
    crate::actor_middleware::session_cookie(headers)
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
    crate::actor_middleware::cookie_anonymous_uid(headers)
}

/// `AnonymousUIDMiddleware` (`internal/actor/http.go:180-205`): the cookie is
/// authoritative, and the `X-Sourcegraph-Actor-Anonymous-UID` propagation
/// header is consulted only when there is no cookie. Used for the audit
/// actor's ID; `getUID` deliberately keeps its own cookie-only lookup.
fn actor_anonymous_uid(headers: &HeaderMap) -> String {
    crate::actor_middleware::actor_anonymous_uid(headers)
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
            sg_session::COOKIE_NAME,
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
