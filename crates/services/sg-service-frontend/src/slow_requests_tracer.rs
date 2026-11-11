#![forbid(unsafe_code)]

//! `cmd/frontend/graphqlbackend/slow_requests_tracer.go`'s data path.
//!
//! The baseline keeps the most recent slow GraphQL requests in a Redis FIFO
//! list (`slow-graphql-requests-list`, capacity
//! `observability.captureSlowGraphQLRequestsLimit`) and exposes them through
//! `Query.slowRequests`. This port models the list as an injectable
//! [`SlowRequestStore`] (an in-memory FIFO is provided for the frontend) so
//! the aggregation and GraphQL field semantics can be exercised without
//! Redis.
//!
//! Two baseline details are preserved verbatim:
//!
//! * `getSlowRequestsAfter` assigns `index = i + after` to each row;
//! * a non-numeric `after` only fails `PageInfo` — `fetch` overwrites the
//!   `strconv.Atoi` error with the (successful) list read and treats the
//!   cursor as 0 (`slow_requests_tracer.go:107-121`).
//!
//! `SlowRequest.user`/`repository` perform database lookups in the baseline;
//! the GraphQL objects below return `null` and leave that as a seam.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, Mutex};

use serde::{Deserialize, Serialize};
use serde_json::Value;

use sg_graphql::exec::{Actor, Arguments, ExecError, FieldValue, ObjectValue};
use sg_graphql::json::Json;

use crate::graphql_resolvers::PageInfoObject;

/// `slowRequestRedisFIFOListPerPage` (`slow_requests_tracer.go:24`).
pub const SLOW_REQUEST_REDIS_FIFO_LIST_PER_PAGE: usize = 50;

/// `types.SlowRequest` (`internal/types/types.go:2043`).
///
/// `duration` is Go's `time.Duration` JSON representation (integer
/// nanoseconds); `start` is the `gqlutil.DateTime` wire form (UTC RFC3339,
/// second precision).
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct SlowRequest {
    #[serde(default)]
    pub index: String,
    #[serde(default)]
    pub start: String,
    #[serde(default)]
    pub duration: i64,
    #[serde(rename = "userId", default)]
    pub user_id: i32,
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub source: String,
    #[serde(default)]
    pub variables: serde_json::Map<String, Value>,
    #[serde(default)]
    pub errors: Vec<String>,
    #[serde(default)]
    pub query: String,
    #[serde(default)]
    pub filepath: String,
}

impl SlowRequest {
    /// `slowRequestResolver.Duration`'s `Duration.Seconds()`.
    pub fn duration_seconds(&self) -> f64 {
        duration_seconds(self.duration)
    }

    /// `slowRequestResolver.Variables`'s `json.Marshal(variables)`.
    pub fn variables_json(&self) -> String {
        variables_json(&self.variables)
    }

    /// `slowRequestResolver.Filepath`: `filePath` then `path`.
    pub fn filepath(&self) -> Option<String> {
        filepath_from_variables(&self.variables)
    }
}

/// Convert a Go `time.Duration` (nanoseconds) to seconds.
pub fn duration_seconds(duration_ns: i64) -> f64 {
    duration_ns as f64 / 1_000_000_000.0
}

/// `slowRequestResolver.Variables`: marshal the variables object. Marshal
/// failures yield the empty string (the baseline ignores the error).
pub fn variables_json(variables: &serde_json::Map<String, Value>) -> String {
    serde_json::to_string(&Value::Object(variables.clone())).unwrap_or_default()
}

/// `guessRepoName` (`slow_requests_tracer.go:195`): `repoName` wins over
/// `repository`; only string values count.
pub fn guess_repo_name(variables: &serde_json::Map<String, Value>) -> Option<String> {
    for key in ["repoName", "repository"] {
        if let Some(value) = variables.get(key).and_then(Value::as_str) {
            return Some(value.to_owned());
        }
    }
    None
}

/// `slowRequestResolver.Filepath` (`slow_requests_tracer.go:229`):
/// `filePath` wins over `path`; only string values count.
pub fn filepath_from_variables(variables: &serde_json::Map<String, Value>) -> Option<String> {
    for key in ["filePath", "path"] {
        if let Some(value) = variables.get(key).and_then(Value::as_str) {
            return Some(value.to_owned());
        }
    }
    None
}

/// The FIFO list seam standing in for `rcache.FIFOList`.
///
/// Indexes are Redis `LRANGE` indexes (inclusive `from`/`to`, non-negative
/// here); `len` is the Redis `LLEN` equivalent.
pub trait SlowRequestStore: Send + Sync {
    fn slice(&self, from: i64, to: i64) -> Result<Vec<SlowRequest>, String>;
    fn len(&self) -> Result<i64, String>;
    fn max_size(&self) -> i64;
    /// `FIFOList.Insert`: push at the head and trim to the configured size.
    fn insert(&self, request: SlowRequest) -> Result<(), String>;

    fn is_empty(&self) -> Result<bool, String> {
        Ok(self.len()? == 0)
    }
}

/// In-memory `rcache.FIFOList` equivalent: newest entries first, trimmed to
/// `max_size`.
///
/// `max_size == 0` reproduces the baseline's `LTRIM key 0 0`: `Insert` keeps
/// the newest entry, but `Slice` clamps every read to zero elements.
pub struct InMemorySlowRequestStore {
    items: Mutex<VecDeque<SlowRequest>>,
    max_size: AtomicI64,
}

impl InMemorySlowRequestStore {
    pub fn new(max_size: i64) -> Self {
        Self {
            items: Mutex::new(VecDeque::new()),
            max_size: AtomicI64::new(max_size),
        }
    }

    pub fn set_max_size(&self, max_size: i64) {
        self.max_size.store(max_size, Ordering::SeqCst);
        self.trim();
    }

    fn maximum(&self) -> i64 {
        self.max_size.load(Ordering::SeqCst).max(0)
    }

    fn trim(&self) {
        let maximum = self.maximum();
        if maximum == 0 {
            // `LTRIM key 0 0` retains only the head (newest) element.
            if let Ok(mut items) = self.items.lock() {
                while items.len() > 1 {
                    items.pop_back();
                }
            }
            return;
        }
        if let Ok(mut items) = self.items.lock() {
            while items.len() > maximum as usize {
                items.pop_back();
            }
        }
    }
}

impl SlowRequestStore for InMemorySlowRequestStore {
    /// `FIFOList.Insert`.
    fn insert(&self, request: SlowRequest) -> Result<(), String> {
        if let Ok(mut items) = self.items.lock() {
            items.push_front(request);
        }
        self.trim();
        Ok(())
    }

    fn slice(&self, from: i64, to: i64) -> Result<Vec<SlowRequest>, String> {
        let items = self
            .items
            .lock()
            .map_err(|_| "slow request store lock poisoned".to_owned())?;
        let len = items.len() as i64;
        let start = redis_index(from, len);
        let stop = redis_index(to, len);
        if start > stop || start >= len || stop < 0 {
            return Ok(Vec::new());
        }
        let take = (stop - start + 1) as usize;
        let mut selected: Vec<SlowRequest> = items
            .iter()
            .skip(start as usize)
            .take(take)
            .cloned()
            .collect();
        let maximum = self.maximum();
        if selected.len() as i64 > maximum {
            selected.truncate(maximum as usize);
        }
        Ok(selected)
    }

    fn len(&self) -> Result<i64, String> {
        let items = self
            .items
            .lock()
            .map_err(|_| "slow request store lock poisoned".to_owned())?;
        Ok(items.len() as i64)
    }

    fn max_size(&self) -> i64 {
        self.maximum()
    }
}

/// Redis negative indexes count from the end (`-1` is the last element).
fn redis_index(index: i64, len: i64) -> i64 {
    if index < 0 {
        (len + index).max(0)
    } else if index >= len {
        len
    } else {
        index
    }
}

/// `captureSlowRequest` (`slow_requests_tracer.go:32`): serialise then insert.
/// A marshal failure is reported to the caller (the baseline logs it).
pub fn capture_slow_request(
    store: &dyn SlowRequestStore,
    request: &SlowRequest,
) -> Result<(), String> {
    // Marshal first, like `captureSlowRequest`: a marshal failure means the
    // request is not captured.
    serde_json::to_vec(request)
        .map_err(|error| format!("failed to marshal slowRequest: {error}"))?;
    store.insert(request.clone())
}

/// `getSlowRequestsAfter` (`slow_requests_tracer.go:44`): fetch the inclusive
/// slice and assign `index = i + after`.
pub fn get_slow_requests_after(
    store: &dyn SlowRequestStore,
    after: i64,
    limit: i64,
) -> Result<Vec<SlowRequest>, String> {
    let mut requests = store.slice(after, after + limit - 1)?;
    for (offset, request) in requests.iter_mut().enumerate() {
        request.index = (offset as i64 + after).to_string();
    }
    Ok(requests)
}

/// `strconv.Atoi` with Go's error text.
pub fn atoi(input: &str) -> Result<i64, String> {
    match input.parse::<i64>() {
        Ok(value) => Ok(value),
        Err(error) => {
            let reason = match error.kind() {
                std::num::IntErrorKind::PosOverflow | std::num::IntErrorKind::NegOverflow => {
                    "value out of range"
                }
                _ => "invalid syntax",
            };
            Err(format!("strconv.Atoi: parsing \"{input}\": {reason}"))
        }
    }
}

/// The result of evaluating `slowRequestConnectionResolver`'s fields.
///
/// The three baseline fields share one `sync.Once` fetch but surface errors
/// independently, so each carries its own optional error.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct SlowRequestConnection {
    pub nodes: Vec<SlowRequest>,
    pub nodes_error: Option<String>,
    pub total_count: i32,
    pub total_count_error: Option<String>,
    pub has_next_page: bool,
    pub end_cursor: Option<String>,
    pub page_info_error: Option<String>,
}

/// Evaluate `slowRequestConnectionResolver` (`slow_requests_tracer.go:83-165`).
pub fn load_connection(
    store: &dyn SlowRequestStore,
    after: &str,
    per_page: usize,
) -> SlowRequestConnection {
    // `fetch`: the `Atoi` error is overwritten by the list read, so a bad
    // cursor behaves as index 0.
    let fetch_index = atoi(after).unwrap_or(0);
    let (nodes, nodes_error) = match get_slow_requests_after(store, fetch_index, per_page as i64) {
        Ok(requests) => (requests, None),
        Err(error) => (Vec::new(), Some(error)),
    };
    let (total_count, total_count_error) = match store.len() {
        Ok(size) => (size as i32, None),
        Err(error) => (0, Some(error)),
    };

    // `PageInfo` parses the cursor independently and surfaces the error.
    let page_info_error = atoi(after).err();
    let (has_next_page, end_cursor) = if page_info_error.is_some() {
        (false, None)
    } else {
        let index = fetch_index;
        if index + per_page as i64 >= total_count as i64 {
            (false, None)
        } else {
            match nodes.last() {
                Some(last) => (true, Some(last.index.clone())),
                None => (false, None),
            }
        }
    };

    SlowRequestConnection {
        nodes,
        nodes_error,
        total_count,
        total_count_error,
        has_next_page,
        end_cursor,
        page_info_error,
    }
}

/// `slowRequestResolver` as a GraphQL `SlowRequest` object.
pub struct SlowRequestObject {
    request: SlowRequest,
}

impl SlowRequestObject {
    pub fn new(request: SlowRequest) -> Self {
        Self { request }
    }

    pub fn request(&self) -> &SlowRequest {
        &self.request
    }
}

impl ObjectValue for SlowRequestObject {
    fn type_name(&self) -> &'static str {
        "SlowRequest"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        let request = &self.request;
        match field {
            "index" => Ok(Some(FieldValue::Json(Json::Str(request.index.clone())))),
            "start" => Ok(Some(FieldValue::Json(Json::Str(request.start.clone())))),
            "duration" => Ok(Some(FieldValue::Json(Json::Float(
                request.duration_seconds(),
            )))),
            // `UserByID` is a database seam.
            "user" => Ok(Some(FieldValue::Null)),
            "name" => Ok(Some(FieldValue::Json(Json::Str(request.name.clone())))),
            "source" => Ok(Some(FieldValue::Json(Json::Str(request.source.clone())))),
            // `db.Repos().GetByName` is a database seam; `guess_repo_name`
            // implements the name guess used by the baseline.
            "repository" => Ok(Some(FieldValue::Null)),
            "variables" => Ok(Some(FieldValue::Json(Json::Str(request.variables_json())))),
            "errors" => Ok(Some(FieldValue::Json(Json::Arr(
                request
                    .errors
                    .iter()
                    .map(|error| Json::Str(error.clone()))
                    .collect(),
            )))),
            "query" => Ok(Some(FieldValue::Json(Json::Str(request.query.clone())))),
            "filepath" => Ok(Some(match request.filepath() {
                Some(filepath) => FieldValue::Json(Json::Str(filepath)),
                None => FieldValue::Null,
            })),
            _ => Ok(None),
        }
    }
}

/// `slowRequestConnectionResolver` as a GraphQL `SlowRequestConnection`.
#[derive(Debug)]
pub struct SlowRequestConnectionObject {
    connection: SlowRequestConnection,
}

impl SlowRequestConnectionObject {
    pub fn new(connection: SlowRequestConnection) -> Self {
        Self { connection }
    }

    pub fn connection(&self) -> &SlowRequestConnection {
        &self.connection
    }
}

impl ObjectValue for SlowRequestConnectionObject {
    fn type_name(&self) -> &'static str {
        "SlowRequestConnection"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        let connection = &self.connection;
        match field {
            "nodes" => match &connection.nodes_error {
                Some(error) => Err(ExecError::new(error.clone())),
                None => Ok(Some(FieldValue::List {
                    items: connection
                        .nodes
                        .iter()
                        .cloned()
                        .map(|request| {
                            FieldValue::Object(Arc::new(SlowRequestObject::new(request)))
                        })
                        .collect(),
                    element_type: Some("SlowRequest"),
                })),
            },
            "totalCount" => match &connection.total_count_error {
                Some(error) => Err(ExecError::new(error.clone())),
                None => Ok(Some(FieldValue::Json(Json::Int(i64::from(
                    connection.total_count,
                ))))),
            },
            "pageInfo" => match &connection.page_info_error {
                Some(error) => Err(ExecError::new(error.clone())),
                None => Ok(Some(FieldValue::Object(Arc::new(PageInfoObject::new(
                    connection.has_next_page,
                    connection.end_cursor.clone(),
                ))))),
            },
            _ => Ok(None),
        }
    }
}

/// The injected `Query.slowRequests` surface: the FIFO store plus the
/// configured capture limit.
pub struct SlowRequestsSurface {
    store: Arc<dyn SlowRequestStore>,
    limit: i64,
}

impl SlowRequestsSurface {
    pub fn new(store: Arc<dyn SlowRequestStore>, limit: i64) -> Self {
        Self { store, limit }
    }

    pub fn store(&self) -> &Arc<dyn SlowRequestStore> {
        &self.store
    }

    pub fn limit(&self) -> i64 {
        self.limit
    }

    /// `schemaResolver.SlowRequests` (`slow_requests_tracer.go:63`).
    ///
    /// The anonymous/non-admin branches match
    /// `auth.CheckCurrentUserIsSiteAdmin`, which also returns nil for internal
    /// actors (`internal/auth/site_admin.go:19-21`).
    pub fn resolve(
        &self,
        actor: Actor,
        viewer_site_admin: bool,
        after: Option<&str>,
    ) -> Result<SlowRequestConnectionObject, String> {
        if self.limit == 0 {
            return Err("slow graphql requests capture is not enabled".to_owned());
        }
        match actor {
            Actor::Internal => {}
            Actor::Anonymous => return Err("not authenticated".to_owned()),
            Actor::User { .. } if !viewer_site_admin => {
                return Err("must be site admin".to_owned());
            }
            Actor::User { .. } => {}
        }
        let after = after.unwrap_or("0");
        Ok(SlowRequestConnectionObject::new(load_connection(
            self.store.as_ref(),
            after,
            SLOW_REQUEST_REDIS_FIFO_LIST_PER_PAGE,
        )))
    }
}

impl std::fmt::Debug for SlowRequestsSurface {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SlowRequestsSurface")
            .field("limit", &self.limit)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request(index: &str, name: &str) -> SlowRequest {
        SlowRequest {
            index: index.to_owned(),
            start: "2024-01-02T03:04:05Z".to_owned(),
            duration: 1_500_000_000,
            name: name.to_owned(),
            ..SlowRequest::default()
        }
    }

    #[test]
    fn fifo_keeps_newest_and_assigns_indexes() {
        let store = InMemorySlowRequestStore::new(2);
        store.insert(request("", "a")).unwrap();
        store.insert(request("", "b")).unwrap();
        store.insert(request("", "c")).unwrap();
        assert_eq!(store.len().unwrap(), 2);
        let fetched = get_slow_requests_after(&store, 0, 50).unwrap();
        assert_eq!(
            fetched.iter().map(|r| r.name.as_str()).collect::<Vec<_>>(),
            vec!["c", "b"]
        );
        assert_eq!(fetched[0].index, "0");
        assert_eq!(fetched[1].index, "1");
    }

    #[test]
    fn bad_after_only_breaks_page_info() {
        let store = InMemorySlowRequestStore::new(10);
        store.insert(request("", "a")).unwrap();
        let connection = load_connection(&store, "bad", 50);
        assert!(connection.nodes_error.is_none());
        assert_eq!(connection.total_count, 1);
        assert_eq!(connection.nodes[0].name, "a");
        assert_eq!(
            connection.page_info_error.as_deref(),
            Some("strconv.Atoi: parsing \"bad\": invalid syntax")
        );
    }

    #[test]
    fn page_info_reports_the_last_index_as_cursor() {
        let store = InMemorySlowRequestStore::new(10);
        for name in ["a", "b", "c"] {
            store.insert(request("", name)).unwrap();
        }
        let connection = load_connection(&store, "0", 1);
        assert!(connection.has_next_page);
        assert_eq!(connection.end_cursor.as_deref(), Some("0"));
        let connection = load_connection(&store, "2", 2);
        assert!(!connection.has_next_page);
        assert_eq!(connection.end_cursor, None);
    }

    #[test]
    fn field_helpers_follow_baseline_precedence() {
        let mut variables = serde_json::Map::new();
        variables.insert("repoName".to_owned(), Value::String("a/b".to_owned()));
        variables.insert("repository".to_owned(), Value::String("c/d".to_owned()));
        assert_eq!(guess_repo_name(&variables).as_deref(), Some("a/b"));
        variables.insert("filePath".to_owned(), Value::String("x.rs".to_owned()));
        variables.insert("path".to_owned(), Value::String("y.rs".to_owned()));
        assert_eq!(filepath_from_variables(&variables).as_deref(), Some("x.rs"));
        assert_eq!(duration_seconds(1_500_000_000), 1.5);
    }

    #[test]
    fn surface_enforces_limit_and_site_admin() {
        let store: Arc<dyn SlowRequestStore> = Arc::new(InMemorySlowRequestStore::new(10));
        let disabled = SlowRequestsSurface::new(Arc::clone(&store), 0);
        assert_eq!(
            disabled
                .resolve(Actor::User { uid: 1 }, true, None)
                .unwrap_err(),
            "slow graphql requests capture is not enabled"
        );
        let surface = SlowRequestsSurface::new(store, 10);
        assert_eq!(
            surface.resolve(Actor::Anonymous, false, None).unwrap_err(),
            "not authenticated"
        );
        assert_eq!(
            surface
                .resolve(Actor::User { uid: 1 }, false, None)
                .unwrap_err(),
            "must be site admin"
        );
        assert!(surface.resolve(Actor::User { uid: 1 }, true, None).is_ok());
    }
}
