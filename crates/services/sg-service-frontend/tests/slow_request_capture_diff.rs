//! M9.8jj differential for the slow-request capture point and the OTLP
//! exporter selection against
//! `cmd/frontend/graphqlbackend/zz_m9jj_oracle_test.go`.
//!
//! Both sides read the same corpus and emit normalised JSON. The Go oracle
//! transcribes the Redis FIFO and drives the live `slowRequestResolver`
//! methods with a mock `database.DB`; this harness exercises the Rust
//! `SlowRequestsSurface`, the capture helpers and the exporter seam directly.
//!
//! Env:
//!
//! * `SG_M9JJ_DIFF=1`
//! * `M9JJ_CORPUS`, `M9JJ_OUT`

#![allow(clippy::unwrap_used)]

use std::collections::HashMap;
use std::fs;
use std::sync::Arc;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use time::{format_description::well_known::Rfc3339, OffsetDateTime};

use sg_graphql::exec::{Actor, Arguments, FieldValue, ObjectValue};
use sg_graphql::json::Json;

use sg_service_frontend::graphql_resolvers::{
    RepoLookup, RepositoryData, RepositoryLookup, UserData, UserKey, UserLookupStore,
};
use sg_service_frontend::slow_requests_tracer::{
    maybe_capture_slow_request, rfc3339_nanos, InMemorySlowRequestStore, SlowRequest,
    SlowRequestCapture, SlowRequestStore, SlowRequestsSurface,
};
use sg_service_frontend::trace_policy::{
    build_span_exporter, otlp_exporter_config_from_env, NoopSpanTransport, TracerType,
};

// ---------------------------------------------------------------------------
// Corpus
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
struct Corpus {
    db: Db,
    stores: HashMap<String, StoreSeed>,
    #[serde(default)]
    captures: Vec<CaptureCase>,
    #[serde(default)]
    surfaces: Vec<SurfaceCase>,
    #[serde(default)]
    exporters: Vec<ExporterCase>,
}

#[derive(Deserialize)]
struct Db {
    #[serde(default)]
    users: HashMap<String, DbUser>,
    #[serde(default)]
    repos: HashMap<String, DbRepo>,
}

#[derive(Deserialize)]
struct DbUser {
    id: i32,
    #[serde(default)]
    username: String,
}

#[derive(Deserialize)]
struct DbRepo {
    #[serde(default)]
    id: i32,
    #[serde(default)]
    name: String,
    #[serde(default)]
    blocked: String,
    #[serde(default)]
    error: String,
}

#[derive(Deserialize)]
struct StoreSeed {
    limit: i64,
    #[serde(default)]
    synthetic: usize,
    #[serde(default)]
    requests: Vec<RequestIn>,
}

#[derive(Deserialize)]
struct RequestIn {
    start: String,
    duration: i64,
    #[serde(rename = "userId", default)]
    user_id: i32,
    #[serde(default)]
    name: String,
    #[serde(default)]
    source: String,
    #[serde(default)]
    variables: Map<String, Value>,
    #[serde(default)]
    errors: Vec<String>,
    #[serde(default)]
    query: String,
}

impl RequestIn {
    fn slow_request(&self) -> SlowRequest {
        let start = OffsetDateTime::parse(&self.start, &Rfc3339).unwrap();
        SlowRequest {
            index: String::new(),
            start: rfc3339_nanos(start),
            duration: self.duration,
            user_id: self.user_id,
            name: self.name.clone(),
            source: self.source.clone(),
            variables: self.variables.clone(),
            errors: self.errors.clone(),
            query: self.query.clone(),
            filepath: String::new(),
        }
    }
}

#[derive(Deserialize)]
struct CaptureCase {
    name: String,
    capacity: i64,
    #[serde(default)]
    attempts: Vec<AttemptIn>,
}

#[derive(Deserialize)]
struct AttemptIn {
    threshold: i64,
    start: String,
    duration: i64,
    #[serde(rename = "userId", default)]
    user_id: i32,
    #[serde(default)]
    name: String,
    #[serde(default)]
    source: String,
    #[serde(default)]
    variables: Map<String, Value>,
    #[serde(default)]
    errors: Vec<String>,
    #[serde(default)]
    query: String,
}

#[derive(Deserialize)]
struct SurfaceCase {
    name: String,
    store: String,
    #[serde(default)]
    after: Option<String>,
    actor: String,
    #[serde(rename = "siteAdmin", default)]
    site_admin: bool,
    limit: i64,
}

#[derive(Deserialize)]
struct ExporterCase {
    name: String,
    #[serde(rename = "tracerType")]
    tracer_type: String,
    #[serde(default)]
    endpoint: Option<String>,
    #[serde(default)]
    protocol: Option<String>,
}

// ---------------------------------------------------------------------------
// Output (field order matches the Go structs)
// ---------------------------------------------------------------------------

#[derive(Serialize)]
struct Output {
    captures: Vec<CaptureOut>,
    surfaces: Vec<SurfaceOut>,
    exporters: Vec<ExporterOut>,
}

#[derive(Serialize)]
struct CaptureOut {
    name: String,
    captured: Vec<bool>,
    store: Vec<String>,
}

#[derive(Serialize)]
struct LookupOut {
    status: &'static str,
    id: String,
    username: String,
    name: String,
    error: String,
}

impl Default for LookupOut {
    fn default() -> Self {
        Self {
            status: "null",
            id: String::new(),
            username: String::new(),
            name: String::new(),
            error: String::new(),
        }
    }
}

#[derive(Serialize)]
struct NodeOut {
    index: String,
    start: String,
    duration: f64,
    user: LookupOut,
    name: String,
    source: String,
    repository: LookupOut,
    variables: String,
    errors: Vec<String>,
    query: String,
    filepath: Option<String>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct SurfaceOut {
    name: String,
    error: String,
    nodes: Vec<NodeOut>,
    nodes_error: String,
    total_count: i32,
    total_count_error: String,
    has_next_page: bool,
    end_cursor: Option<String>,
    page_info_error: String,
}

impl SurfaceOut {
    fn new(name: &str) -> Self {
        Self {
            name: name.to_owned(),
            error: String::new(),
            nodes: Vec::new(),
            nodes_error: String::new(),
            total_count: 0,
            total_count_error: String::new(),
            has_next_page: false,
            end_cursor: None,
            page_info_error: String::new(),
        }
    }
}

#[derive(Serialize)]
struct ExporterOut {
    name: String,
    selection: String,
    error: String,
    endpoint: String,
    protocol: String,
    insecure: bool,
}

// ---------------------------------------------------------------------------
// Fakes for the async database seams
// ---------------------------------------------------------------------------

struct FakeUsers {
    users: HashMap<i32, UserData>,
}

#[async_trait]
impl UserLookupStore for FakeUsers {
    async fn lookup(&self, key: &UserKey) -> Result<Option<UserData>, String> {
        match key {
            UserKey::DatabaseId(id) => Ok(self.users.get(id).cloned()),
            _ => Ok(None),
        }
    }
}

struct FakeRepos {
    repos: HashMap<String, RepoLookup>,
}

#[async_trait]
impl RepositoryLookup for FakeRepos {
    async fn lookup(
        &self,
        name_or_uri: &str,
        _authenticated_user_id: i32,
        _bypass_authz: bool,
    ) -> Result<RepoLookup, String> {
        Ok(self
            .repos
            .get(name_or_uri)
            .cloned()
            .unwrap_or(RepoLookup::NotFound))
    }

    async fn by_id(
        &self,
        _repo_id: i32,
        _authenticated_user_id: i32,
        _bypass_authz: bool,
    ) -> Result<RepoLookup, String> {
        Ok(RepoLookup::NotFound)
    }

    async fn by_hashed_name(
        &self,
        _hashed_name: &str,
        _authenticated_user_id: i32,
        _bypass_authz: bool,
    ) -> Result<RepoLookup, String> {
        Ok(RepoLookup::NotFound)
    }
}

fn user_data(id: i32, username: &str) -> UserData {
    let epoch = OffsetDateTime::from_unix_timestamp(0).unwrap();
    UserData {
        id,
        username: username.to_owned(),
        display_name: String::new(),
        avatar_url: String::new(),
        created_at: epoch,
        updated_at: epoch,
        site_admin: false,
        tos_accepted: false,
        primary_email: String::new(),
    }
}

fn repo_data(id: i32, name: &str) -> RepositoryData {
    RepositoryData {
        id,
        name: name.to_owned(),
        uri: String::new(),
        description: String::new(),
        fork: false,
        archived: false,
        private: false,
        stars: 0,
        external_id: String::new(),
        external_service_type: String::new(),
        external_service_id: String::new(),
    }
}

// ---------------------------------------------------------------------------
// ObjectValue helpers
// ---------------------------------------------------------------------------

fn field(object: &dyn ObjectValue, name: &str) -> Result<Option<FieldValue>, String> {
    object
        .field(name, &Arguments::default(), Actor::Anonymous)
        .map_err(|error| error.message)
}

fn string_field(object: &dyn ObjectValue, name: &str) -> Result<String, String> {
    match field(object, name)? {
        Some(FieldValue::Json(Json::Str(value))) => Ok(value),
        _ => Err(format!("field {name} was not a string")),
    }
}

fn float_field(object: &dyn ObjectValue, name: &str) -> Result<f64, String> {
    match field(object, name)? {
        Some(FieldValue::Json(Json::Float(value))) => Ok(value),
        Some(FieldValue::Json(Json::Int(value))) => Ok(value as f64),
        _ => Err(format!("field {name} was not a number")),
    }
}

fn lookup_out(result: Result<Option<FieldValue>, String>) -> LookupOut {
    let value = match result {
        Err(error) => {
            return LookupOut {
                status: "error",
                error,
                ..LookupOut::default()
            }
        }
        Ok(value) => value,
    };
    let Some(FieldValue::Object(object)) = value else {
        return LookupOut::default();
    };
    let id = string_field(object.as_ref(), "id").unwrap_or_default();
    let username = string_field(object.as_ref(), "username").unwrap_or_default();
    let name = string_field(object.as_ref(), "name").unwrap_or_default();
    LookupOut {
        status: "found",
        id,
        username,
        name,
        error: String::new(),
    }
}

fn node_out(item: FieldValue) -> Result<NodeOut, String> {
    let FieldValue::Object(object) = item else {
        return Err("nodes item was not an object".to_owned());
    };
    let errors = match field(object.as_ref(), "errors")? {
        Some(FieldValue::Json(Json::Arr(items))) => items
            .into_iter()
            .map(|item| match item {
                Json::Str(value) => Ok(value),
                _ => Err("errors item was not a string".to_owned()),
            })
            .collect::<Result<Vec<_>, String>>()?,
        _ => Vec::new(),
    };
    let filepath = match field(object.as_ref(), "filepath")? {
        Some(FieldValue::Null) | None => None,
        Some(FieldValue::Json(Json::Str(value))) => Some(value),
        _ => return Err("filepath was not a string".to_owned()),
    };
    Ok(NodeOut {
        index: string_field(object.as_ref(), "index")?,
        start: string_field(object.as_ref(), "start")?,
        duration: float_field(object.as_ref(), "duration")?,
        user: lookup_out(field(object.as_ref(), "user")),
        name: string_field(object.as_ref(), "name")?,
        source: string_field(object.as_ref(), "source")?,
        repository: lookup_out(field(object.as_ref(), "repository")),
        variables: string_field(object.as_ref(), "variables")?,
        errors,
        query: string_field(object.as_ref(), "query")?,
        filepath,
    })
}

// ---------------------------------------------------------------------------
// Evaluation
// ---------------------------------------------------------------------------

fn capture_out(case: &CaptureCase) -> Result<CaptureOut, String> {
    let store = InMemorySlowRequestStore::new(case.capacity);
    let mut captured = Vec::new();
    for attempt in &case.attempts {
        let start = OffsetDateTime::parse(&attempt.start, &Rfc3339)
            .map_err(|error| format!("bad start: {error}"))?;
        let capture = SlowRequestCapture {
            start: rfc3339_nanos(start),
            duration_ns: attempt.duration,
            threshold_ms: attempt.threshold,
            user_id: attempt.user_id,
            name: attempt.name.clone(),
            source: attempt.source.clone(),
            variables: attempt.variables.clone(),
            errors: attempt.errors.clone(),
            query: attempt.query.clone(),
        };
        captured.push(maybe_capture_slow_request(&store, &capture)?);
    }
    let len = store.len()?;
    let rows = if len > 0 {
        store.slice(0, len - 1)?
    } else {
        Vec::new()
    };
    Ok(CaptureOut {
        name: case.name.clone(),
        captured,
        store: rows
            .iter()
            .map(|request| serde_json::to_string(request).unwrap())
            .collect(),
    })
}

async fn surface_out(
    case: &SurfaceCase,
    stores: &HashMap<String, Arc<InMemorySlowRequestStore>>,
    users: &FakeUsers,
    repos: &FakeRepos,
) -> Result<SurfaceOut, String> {
    let store: Arc<dyn SlowRequestStore> = stores
        .get(&case.store)
        .ok_or_else(|| format!("unknown store {}", case.store))?
        .clone();
    let surface = SlowRequestsSurface::new(store, case.limit);
    let actor = if case.actor == "anonymous" {
        Actor::Anonymous
    } else {
        Actor::User { uid: 1 }
    };
    let mut out = SurfaceOut::new(&case.name);
    let connection = match surface
        .resolve_prefetch(
            Some(users as &dyn UserLookupStore),
            Some(repos as &dyn RepositoryLookup),
            actor,
            case.site_admin,
            case.after.as_deref(),
        )
        .await
    {
        Ok(connection) => connection,
        Err(error) => {
            out.error = error;
            return Ok(out);
        }
    };
    match field(&connection, "nodes") {
        Err(error) => out.nodes_error = error,
        Ok(Some(FieldValue::List { items, .. })) => {
            for item in items {
                out.nodes.push(node_out(item)?);
            }
        }
        _ => {}
    }
    match field(&connection, "totalCount") {
        Err(error) => out.total_count_error = error,
        Ok(Some(FieldValue::Json(Json::Int(value)))) => out.total_count = value as i32,
        _ => {}
    }
    match field(&connection, "pageInfo") {
        Err(error) => out.page_info_error = error,
        Ok(Some(FieldValue::Object(page_info))) => {
            out.has_next_page = matches!(
                field(page_info.as_ref(), "hasNextPage"),
                Ok(Some(FieldValue::Json(Json::Bool(true))))
            );
            if let Ok(Some(FieldValue::Json(Json::Str(cursor)))) =
                field(page_info.as_ref(), "endCursor")
            {
                out.end_cursor = Some(cursor);
            }
        }
        _ => {}
    }
    Ok(out)
}

fn tracer_type(value: &str) -> Option<TracerType> {
    match value {
        "none" => Some(TracerType::None),
        "jaeger" => Some(TracerType::Jaeger),
        "opentelemetry" => Some(TracerType::OpenTelemetry),
        _ => None,
    }
}

fn exporter_out(case: &ExporterCase) -> ExporterOut {
    let env = |key: &str| match key {
        "OTEL_EXPORTER_OTLP_ENDPOINT" => case.endpoint.clone(),
        "OTEL_EXPORTER_OTLP_PROTOCOL" => case.protocol.clone(),
        _ => None,
    };
    let mut out = ExporterOut {
        name: case.name.clone(),
        selection: String::new(),
        error: String::new(),
        endpoint: String::new(),
        protocol: String::new(),
        insecure: false,
    };
    let Some(tracer) = tracer_type(&case.tracer_type) else {
        out.selection = "error".to_owned();
        out.error = format!("unexpected tracer type {:?}", case.tracer_type);
        return out;
    };
    match build_span_exporter(tracer, Arc::new(NoopSpanTransport), env) {
        Err(error) => {
            out.selection = "error".to_owned();
            out.error = error;
        }
        Ok(None) => out.selection = "none".to_owned(),
        Ok(Some(_)) if tracer == TracerType::Jaeger => out.selection = "jaeger".to_owned(),
        Ok(Some(_)) => match otlp_exporter_config_from_env(env) {
            Ok(config) => {
                out.selection = "otlp".to_owned();
                out.endpoint = config.endpoint;
                out.protocol = config.protocol.as_str().to_owned();
                out.insecure = config.insecure;
            }
            Err(error) => {
                out.selection = "error".to_owned();
                out.error = error;
            }
        },
    }
    out
}

// ---------------------------------------------------------------------------
// Test
// ---------------------------------------------------------------------------

#[tokio::test]
async fn slow_request_capture_diff() {
    if std::env::var("SG_M9JJ_DIFF").as_deref() != Ok("1") {
        eprintln!("skipping: set SG_M9JJ_DIFF=1");
        return;
    }
    let corpus_path = std::env::var("M9JJ_CORPUS").unwrap();
    let out_path = std::env::var("M9JJ_OUT").unwrap();
    let corpus: Corpus = serde_json::from_slice(&fs::read(corpus_path).unwrap()).unwrap();

    let mut captures = Vec::new();
    for case in &corpus.captures {
        captures.push(capture_out(case).unwrap());
    }

    let mut stores = HashMap::new();
    for (name, seed) in &corpus.stores {
        let store = InMemorySlowRequestStore::new(seed.limit);
        for index in 0..seed.synthetic {
            store
                .insert(SlowRequest {
                    start: "2024-01-01T00:00:00Z".to_owned(),
                    duration: 1_500_000_000,
                    name: format!("S{index}"),
                    source: "other".to_owned(),
                    variables: Map::new(),
                    errors: Vec::new(),
                    ..SlowRequest::default()
                })
                .unwrap();
        }
        for request in &seed.requests {
            store.insert(request.slow_request()).unwrap();
        }
        stores.insert(name.clone(), Arc::new(store));
    }

    let mut users = HashMap::new();
    for (id, user) in &corpus.db.users {
        users.insert(
            id.parse::<i32>().unwrap(),
            user_data(user.id, &user.username),
        );
    }
    let users = FakeUsers { users };
    let mut repos = HashMap::new();
    for (name, repo) in &corpus.db.repos {
        let lookup = if !repo.error.is_empty() {
            RepoLookup::Error(repo.error.clone())
        } else if !repo.blocked.is_empty() {
            RepoLookup::Blocked {
                name: name.clone(),
                reason: repo.blocked.clone(),
            }
        } else {
            RepoLookup::Found(repo_data(repo.id, &repo.name))
        };
        repos.insert(name.clone(), lookup);
    }
    let repos = FakeRepos { repos };

    let mut surfaces = Vec::new();
    for case in &corpus.surfaces {
        surfaces.push(surface_out(case, &stores, &users, &repos).await.unwrap());
    }

    let exporters = corpus.exporters.iter().map(exporter_out).collect();

    let output = Output {
        captures,
        surfaces,
        exporters,
    };
    let mut encoded = serde_json::to_vec(&output).unwrap();
    encoded.push(b'\n');
    fs::write(&out_path, &encoded).unwrap();
    eprintln!("RUST wrote {} bytes", encoded.len());
}
