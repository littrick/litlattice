#![forbid(unsafe_code)]

//! Search-jobs GraphQL surface (M9.68a).
//!
//! Ports `cmd/frontend/graphqlbackend/search_jobs.graphql` plus
//! `cmd/frontend/internal/search/resolvers/{resolver,search_job,
//! search_job_stats}.go`:
//!
//! - `Query.searchJobs` with the `gqlutil.ConnectionResolver` pagination
//!   (max page size 100, composite `(orderBy, id)` cursors encoded as
//!   `SearchJobsCursor` Relay IDs, `first`/`last` direction reversal);
//! - `Query.validateSearchJob` / `Mutation.createSearchJob`
//!   (`service.ValidateSearchJob`: `index:no` prefix, structural rejection,
//!   planner validation);
//! - `Mutation.cancelSearchJob` / `deleteSearchJob`;
//! - `Query.node(id: <SearchJob>)`;
//! - `SearchJob` / `SearchJobStats` object fields.
//!
//! The database is injected through [`SearchJobsDatabase`], a synchronous
//! seam bridged to `sg_db::exhaustive_search` with `block_in_place`.
//!
//! Errata: the baseline `service.DeleteSearchJob` also deletes the
//! object-storage blobs for the job; the frontend has no upload-store seam in
//! this port yet, so only the database rows are deleted. Seams for both
//! `db.Users().GetByID` (creator) and the connection (`ListExhaustiveSearchJobs`
//! with pagination, added to `sg-db`) are covered.

use std::collections::BTreeMap;
use std::future::Future;
use std::sync::{Arc, OnceLock};

use base64::Engine as _;
use sg_graphql::exec::{Actor, Arguments, ExecError, FieldValue, InputValue, ObjectValue};
use sg_graphql::json::Json;
use sqlx::PgPool;
use time::OffsetDateTime;

use sg_db::exhaustive_search::{
    self as store, Actor as StoreActor, CursorValue, ExhaustiveSearchJob, JobState,
    ListArgs as StoreListArgs, PaginationArgs as StorePaginationArgs,
};

use crate::graphql_resolvers::{
    datetime_rfc3339, relay_kind, unmarshal_relay_id, UserData, UserObject,
};

/// `resolvers.searchJobIDKind`.
pub const SEARCH_JOB_KIND: &str = "SearchJob";
/// `resolvers.searchJobsCursorKind`.
pub const SEARCH_JOB_CURSOR_KIND: &str = "SearchJobsCursor";
/// `gqlutil.DefaultMaxPageSize`.
pub const DEFAULT_MAX_PAGE_SIZE: i32 = 100;
/// `SearchJobsOrderBy` default (`search_jobs.graphql`).
pub const DEFAULT_ORDER_BY: &str = "CREATED_AT";

/// `types.RepoRevJobStats` (`getAggregateRepoRevState` in the service).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RepoRevJobStats {
    pub total: i32,
    pub completed: i32,
    pub failed: i32,
    pub in_progress: i32,
}

// ---------------------------------------------------------------------------
// Seams
// ---------------------------------------------------------------------------

/// The database operations the resolvers need.
pub trait SearchJobsDatabase: Send + Sync {
    fn list(
        &self,
        actor: StoreActor,
        args: &StoreListArgs,
    ) -> Result<Vec<ExhaustiveSearchJob>, String>;
    fn get(&self, actor: StoreActor, id: i64) -> Result<ExhaustiveSearchJob, String>;
    fn create(&self, actor: StoreActor, query: &str, initiator_id: i32) -> Result<i64, String>;
    fn cancel(&self, actor: StoreActor, id: i64, now: OffsetDateTime) -> Result<(), String>;
    fn delete(&self, actor: StoreActor, id: i64) -> Result<(), String>;
    fn aggregate_states(&self, actor: StoreActor, id: i64)
        -> Result<BTreeMap<String, i64>, String>;
    /// `db.Users().GetByID`; `Ok(None)` is the baseline's `errcode.IsNotFound`
    /// (`creator: null`).
    fn user_by_id(&self, id: i32) -> Result<Option<UserData>, String>;
}

/// `service.FromSearchClient`: the plan-only validation of a search-job query.
pub trait SearchJobQueryValidator: Send + Sync {
    /// Validate `query` (already prefixed with `index:no ` by the resolver
    /// caller). `Err` is the GraphQL error message.
    fn validate(&self, query: &str) -> Result<(), String>;
}

pub struct SearchJobsResolver {
    db: Arc<dyn SearchJobsDatabase>,
    validator: Option<Arc<dyn SearchJobQueryValidator>>,
    /// `conf.Get().ExternalURL` (live).
    external_url: Arc<dyn Fn() -> String + Send + Sync>,
}

pub type SearchJobsHandle = Arc<SearchJobsResolver>;

impl SearchJobsResolver {
    pub fn new(
        db: Arc<dyn SearchJobsDatabase>,
        validator: Option<Arc<dyn SearchJobQueryValidator>>,
        external_url: Arc<dyn Fn() -> String + Send + Sync>,
    ) -> Self {
        Self {
            db,
            validator,
            external_url,
        }
    }

    fn external_url(&self) -> String {
        (self.external_url)()
    }

    /// `service.ValidateSearchJob` (`service.go:124-131`) with the baseline's
    /// authenticated-user check and the structural-query rejection from
    /// `FromSearchClient` (`searcher.go:33-45`).
    fn validate_query(&self, query: &str) -> Result<(), String> {
        if query
            .to_ascii_lowercase()
            .contains("patterntype:structural")
        {
            return Err("Structural search is not supported in Search Jobs".to_owned());
        }
        let prefixed = format!("index:no {query}");
        match &self.validator {
            Some(validator) => validator.validate(&prefixed),
            // Without a planner the baseline cannot validate; keep the
            // resolver usable in transport tests while production always
            // injects the search executor.
            None => Ok(()),
        }
    }
}

// ---------------------------------------------------------------------------
// Relay IDs and cursors
// ---------------------------------------------------------------------------

fn relay_decode(id: &str) -> Result<String, String> {
    let decoded = base64::engine::general_purpose::URL_SAFE
        .decode(id)
        .map_err(|_| "invalid graphql.ID".to_owned())?;
    Ok(String::from_utf8_lossy(&decoded).into_owned())
}

/// `resolvers.UnmarshalSearchJobID`: `relay.UnmarshalSpec(id, &int64)`.
pub fn unmarshal_search_job_id(id: &str) -> Result<i64, String> {
    let text = relay_decode(id)?;
    let Some((_, spec)) = text.split_once(':') else {
        return Err("invalid graphql.ID".to_owned());
    };
    serde_json::from_str::<i64>(spec).map_err(|_| "invalid graphql.ID".to_owned())
}

/// `relay.MarshalID("SearchJob", id)`.
pub fn marshal_search_job_id(id: i64) -> String {
    base64::engine::general_purpose::URL_SAFE.encode(format!("{SEARCH_JOB_KIND}:{id}"))
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct CursorSpec {
    #[serde(rename = "c")]
    column: String,
    #[serde(rename = "v")]
    value: String,
    #[serde(rename = "d")]
    direction: String,
}

/// `searchJobsConnectionStore.MarshalCursor`
/// (`resolver.go:154-186`).
fn marshal_cursor(job: &ExhaustiveSearchJob, column: &str) -> Result<String, String> {
    let value = match column {
        "created_at" => go_rfc3339_nano(job.created_at),
        "agg_state" => job.agg_state.to_graphql().to_ascii_lowercase(),
        "query" => job.query.clone(),
        other => {
            return Err(format!(
                "invalid OrderBy.Field. Expected one of (created_at, agg_state, query). Actual: {other}"
            ))
        }
    };
    let spec = CursorSpec {
        column: column.to_owned(),
        value: format!("{value}@{}", job.id),
        direction: String::new(),
    };
    let json = serde_json::to_string(&spec).expect("cursor JSON");
    let json = crate::oauth_login::go_json_escape(&json);
    Ok(
        base64::engine::general_purpose::URL_SAFE
            .encode(format!("{SEARCH_JOB_CURSOR_KIND}:{json}")),
    )
}

/// `time.RFC3339Nano` (`2026-01-02T03:04:05.123456789Z`, trailing zeros
/// trimmed).
fn go_rfc3339_nano(t: OffsetDateTime) -> String {
    let t = t.to_offset(time::UtcOffset::UTC);
    let nanos = t.nanosecond();
    let mut fraction = format!("{nanos:09}");
    while fraction.ends_with('0') {
        fraction.pop();
    }
    let base = format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}",
        t.year(),
        t.month() as u8,
        t.day(),
        t.hour(),
        t.minute(),
        t.second()
    );
    if fraction.is_empty() {
        format!("{base}Z")
    } else {
        format!("{base}.{fraction}Z")
    }
}

/// `searchJobsConnectionStore.UnmarshalCursor` (`resolver.go:188-226`).
fn unmarshal_cursor(cursor: &str, column: &str) -> Result<Vec<CursorValue>, String> {
    let kind = relay_kind(cursor).unwrap_or_default();
    if kind != SEARCH_JOB_CURSOR_KIND {
        return Err(format!(
            "expected a {SEARCH_JOB_CURSOR_KIND:?} cursor, got {kind:?}"
        ));
    }
    let text = relay_decode(cursor)?;
    let Some((_, spec)) = text.split_once(':') else {
        return Err("invalid graphql.ID".to_owned());
    };
    let spec: CursorSpec = serde_json::from_str(spec).map_err(|error| error.to_string())?;
    if spec.column != column {
        return Err(format!(
            "expected a {column:?} cursor, got {:?}",
            spec.column
        ));
    }
    let Some(index) = spec.value.rfind('@') else {
        return Err(format!(
            "Invalid cursor. Expected Value: <{column}>@<id> Actual Value: {}",
            spec.value
        ));
    };
    let (value, id) = spec.value.split_at(index);
    let id = id[1..]
        .parse::<i64>()
        .map_err(|error| format!("strconv.Atoi: {error}"))?;
    match column {
        "created_at" | "agg_state" | "query" => Ok(vec![
            CursorValue::Text(value.to_owned()),
            CursorValue::Int(id),
        ]),
        _ => Err("Invalid OrderBy Field.".to_owned()),
    }
}

// ---------------------------------------------------------------------------
// Argument parsing
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SearchJobsConnectionArgs {
    pub first: Option<i32>,
    pub last: Option<i32>,
    pub after: Option<String>,
    pub before: Option<String>,
    pub query: String,
    pub states: Vec<String>,
    pub user_ids: Vec<i32>,
    pub order_by: String,
    pub descending: bool,
}

fn int32_argument(arguments: &Arguments, name: &str) -> Option<i32> {
    arguments
        .i64(name)
        .and_then(|value| i32::try_from(value).ok())
}

fn list_argument(arguments: &Arguments, name: &str) -> Vec<InputValue> {
    match arguments.get(name) {
        Some(InputValue::List(items)) => items.clone(),
        _ => Vec::new(),
    }
}

pub fn connection_args(arguments: &Arguments) -> Result<SearchJobsConnectionArgs, ExecError> {
    let mut user_ids = Vec::new();
    for item in list_argument(arguments, "userIDs") {
        let Some(id) = item.as_str() else {
            continue;
        };
        user_ids.push(unmarshal_relay_id(id).map_err(ExecError::new)?);
    }
    let states = list_argument(arguments, "states")
        .into_iter()
        .filter_map(|item| item.as_str().map(str::to_owned))
        .collect();
    Ok(SearchJobsConnectionArgs {
        first: int32_argument(arguments, "first"),
        last: int32_argument(arguments, "last"),
        after: arguments.string("after").map(str::to_owned),
        before: arguments.string("before").map(str::to_owned),
        query: arguments.string("query").unwrap_or_default().to_owned(),
        states,
        user_ids,
        order_by: arguments
            .string("orderBy")
            .unwrap_or(DEFAULT_ORDER_BY)
            .to_owned(),
        descending: arguments.bool("descending").unwrap_or(false),
    })
}

/// `resolvers.normalize` (`resolver.go:112-119`).
fn normalize_order_by(order_by: &str) -> String {
    if order_by == "STATE" {
        "agg_state".to_owned()
    } else {
        order_by.to_ascii_lowercase()
    }
}

fn order_columns(order_by: &str) -> Vec<String> {
    vec![normalize_order_by(order_by), "id".to_owned()]
}

fn apply_max_page_size(limit: i32) -> i32 {
    if limit == 0 {
        DEFAULT_MAX_PAGE_SIZE
    } else {
        limit.min(DEFAULT_MAX_PAGE_SIZE)
    }
}

// ---------------------------------------------------------------------------
// Stats
// ---------------------------------------------------------------------------

fn repo_rev_stats(states: &BTreeMap<String, i64>) -> Result<RepoRevJobStats, String> {
    let mut stats = RepoRevJobStats::default();
    for (state, count) in states {
        let count = i32::try_from(*count).unwrap_or(i32::MAX);
        match state.as_str() {
            JobState::COMPLETED => stats.completed = stats.completed.saturating_add(count),
            JobState::FAILED => stats.failed = stats.failed.saturating_add(count),
            JobState::PROCESSING | JobState::ERRORED | JobState::QUEUED => {
                stats.in_progress = stats.in_progress.saturating_add(count)
            }
            JobState::CANCELED => stats.in_progress = 0,
            other => return Err(format!("unknown job state {other:?}")),
        }
    }
    stats.total = stats
        .completed
        .saturating_add(stats.failed)
        .saturating_add(stats.in_progress);
    Ok(stats)
}

// ---------------------------------------------------------------------------
// Objects
// ---------------------------------------------------------------------------

pub struct SearchJobStatsObject {
    stats: RepoRevJobStats,
}

impl ObjectValue for SearchJobStatsObject {
    fn type_name(&self) -> &'static str {
        "SearchJobStats"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        let value = match field {
            "total" => self.stats.total,
            "completed" => self.stats.completed,
            "failed" => self.stats.failed,
            "inProgress" => self.stats.in_progress,
            _ => return Ok(None),
        };
        Ok(Some(FieldValue::Json(Json::Int(i64::from(value)))))
    }
}

pub struct SearchJobObject {
    job: ExhaustiveSearchJob,
    handle: SearchJobsHandle,
}

impl SearchJobObject {
    pub fn new(job: ExhaustiveSearchJob, handle: SearchJobsHandle) -> Self {
        Self { job, handle }
    }
}

impl ObjectValue for SearchJobObject {
    fn type_name(&self) -> &'static str {
        "SearchJob"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "id" => Ok(Some(FieldValue::Json(Json::Str(marshal_search_job_id(
                self.job.id,
            ))))),
            "query" => Ok(Some(FieldValue::Json(Json::Str(self.job.query.clone())))),
            // `searchJobResolver.State`: `Job.AggState.ToGraphQL()`.
            "state" => Ok(Some(FieldValue::Json(Json::Str(
                self.job.agg_state.to_graphql(),
            )))),
            "creator" => {
                let user = self
                    .handle
                    .db
                    .user_by_id(self.job.initiator_id)
                    .map_err(ExecError::new)?;
                Ok(Some(match user {
                    Some(data) => FieldValue::Object(Arc::new(UserObject::from_data(data))),
                    None => FieldValue::Null,
                }))
            }
            "createdAt" => Ok(Some(FieldValue::Json(Json::Str(datetime_rfc3339(
                self.job.created_at,
            ))))),
            "startedAt" => Ok(Some(match self.job.worker.started_at {
                Some(time) => FieldValue::Json(Json::Str(datetime_rfc3339(time))),
                None => FieldValue::Null,
            })),
            "finishedAt" => Ok(Some(match self.job.worker.finished_at {
                Some(time) => FieldValue::Json(Json::Str(datetime_rfc3339(time))),
                None => FieldValue::Null,
            })),
            // `url.JoinPath(conf.Get().ExternalURL, "/.api/search/export/<id>.<ext>")`.
            "URL" | "logURL" => {
                let base = self.handle.external_url();
                let base = base.trim_end_matches('/');
                let extension = if field == "URL" { "jsonl" } else { "log" };
                Ok(Some(FieldValue::Json(Json::Str(format!(
                    "{base}/.api/search/export/{}.{extension}",
                    self.job.id
                )))))
            }
            "repoStats" => {
                let db_actor = store_actor(actor);
                let states = self
                    .handle
                    .db
                    .aggregate_states(db_actor, self.job.id)
                    .map_err(ExecError::new)?;
                let stats = repo_rev_stats(&states).map_err(ExecError::new)?;
                Ok(Some(FieldValue::Object(Arc::new(SearchJobStatsObject {
                    stats,
                }))))
            }
            _ => Ok(None),
        }
    }
}

fn store_actor(actor: Actor) -> StoreActor {
    match actor {
        Actor::Internal => StoreActor::internal(),
        Actor::User { uid } => StoreActor::user(uid),
        Actor::Anonymous => StoreActor::default(),
    }
}

// ---------------------------------------------------------------------------
// Connection
// ---------------------------------------------------------------------------

struct ComputedPage {
    /// The trimmed nodes the connection exposes.
    nodes: Vec<ExhaustiveSearchJob>,
    /// `fetchedNodesCount` (before trimming, after the `last` reversal).
    fetched_count: usize,
    page_size: i32,
}

pub struct SearchJobsConnectionObject {
    handle: SearchJobsHandle,
    args: SearchJobsConnectionArgs,
    actor: Actor,
    page: OnceLock<Result<ComputedPage, String>>,
    total: OnceLock<Result<i32, String>>,
}

impl SearchJobsConnectionObject {
    pub fn new(handle: SearchJobsHandle, args: SearchJobsConnectionArgs, actor: Actor) -> Self {
        Self {
            handle,
            args,
            actor,
            page: OnceLock::new(),
            total: OnceLock::new(),
        }
    }

    fn page_size(&self) -> i32 {
        apply_max_page_size(self.args.first.or(self.args.last).unwrap_or(0))
    }

    fn store_list_args(&self, pagination: Option<StorePaginationArgs>) -> StoreListArgs {
        StoreListArgs {
            query: self.args.query.clone(),
            states: self.args.states.clone(),
            user_ids: self.args.user_ids.clone(),
            pagination,
        }
    }

    fn compute_page(&self) -> &Result<ComputedPage, String> {
        self.page.get_or_init(|| {
            let page_size = self.page_size();
            let limit = page_size.wrapping_add(1);
            let (first, last) = if self.args.first.is_some() {
                (Some(limit), None)
            } else if self.args.last.is_some() {
                (None, Some(limit))
            } else {
                return Err(
                    "you must provide a `first` or `last` value to properly paginate".to_owned(),
                );
            };
            let columns = order_columns(&self.args.order_by);
            let column = columns[0].clone();
            let after = match &self.args.after {
                Some(cursor) => unmarshal_cursor(cursor, &column)?,
                None => Vec::new(),
            };
            let before = match &self.args.before {
                Some(cursor) => unmarshal_cursor(cursor, &column)?,
                None => Vec::new(),
            };
            let pagination = StorePaginationArgs {
                first,
                last,
                after,
                before,
                ascending: !self.args.descending,
                order_by: columns,
            };
            let mut nodes = self
                .handle
                .db
                .list(
                    store_actor(self.actor),
                    &self.store_list_args(Some(pagination)),
                )
                .map_err(|error| error.to_string())?;
            // `ConnectionResolver.Nodes`: the `last` query runs in the
            // opposite direction; reverse back before trimming.
            if self.args.last.is_some() {
                nodes.reverse();
            }
            let fetched_count = nodes.len();
            if nodes.len() as i64 > i64::from(page_size) {
                if self.args.last.is_some() {
                    if !nodes.is_empty() {
                        nodes.remove(0);
                    }
                } else {
                    nodes.pop();
                }
            }
            Ok(ComputedPage {
                nodes,
                fetched_count,
                page_size,
            })
        })
    }

    fn compute_total(&self) -> &Result<i32, String> {
        self.total.get_or_init(|| {
            self.handle
                .db
                .list(store_actor(self.actor), &self.store_list_args(None))
                .map(|jobs| i32::try_from(jobs.len()).unwrap_or(i32::MAX))
                .map_err(|error| error.to_string())
        })
    }
}

impl ObjectValue for SearchJobsConnectionObject {
    fn type_name(&self) -> &'static str {
        "SearchJobConnection"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "nodes" => {
                let page = self
                    .compute_page()
                    .as_ref()
                    .map_err(|error| ExecError::new(error.clone()))?;
                Ok(Some(FieldValue::List {
                    element_type: Some("SearchJob"),
                    items: page
                        .nodes
                        .iter()
                        .cloned()
                        .map(|job| {
                            FieldValue::Object(Arc::new(SearchJobObject::new(
                                job,
                                Arc::clone(&self.handle),
                            )))
                        })
                        .collect(),
                }))
            }
            "totalCount" => {
                let total = self
                    .compute_total()
                    .as_ref()
                    .map_err(|error| ExecError::new(error.clone()))?;
                Ok(Some(FieldValue::Json(Json::Int(i64::from(*total)))))
            }
            "pageInfo" => {
                let page = self
                    .compute_page()
                    .as_ref()
                    .map_err(|error| ExecError::new(error.clone()))?;
                let page_size = i64::from(page.page_size);
                let fetched = page.fetched_count as i64;
                let has_next_page = if self.args.first.is_some() {
                    fetched > page_size
                } else if fetched == 0 {
                    false
                } else {
                    self.args.before.is_some()
                };
                let has_previous_page = if self.args.last.is_some() {
                    fetched > page_size
                } else if fetched == 0 {
                    false
                } else {
                    self.args.after.is_some()
                };
                let column = normalize_order_by(&self.args.order_by);
                let start_cursor = page
                    .nodes
                    .first()
                    .map(|job| marshal_cursor(job, &column))
                    .transpose()
                    .map_err(ExecError::new)?;
                let end_cursor = page
                    .nodes
                    .last()
                    .map(|job| marshal_cursor(job, &column))
                    .transpose()
                    .map_err(ExecError::new)?;
                Ok(Some(FieldValue::Object(Arc::new(
                    SearchJobsPageInfoObject {
                        start_cursor,
                        end_cursor,
                        has_next_page,
                        has_previous_page,
                    },
                ))))
            }
            _ => Ok(None),
        }
    }
}

pub struct SearchJobsPageInfoObject {
    start_cursor: Option<String>,
    end_cursor: Option<String>,
    has_next_page: bool,
    has_previous_page: bool,
}

impl ObjectValue for SearchJobsPageInfoObject {
    fn type_name(&self) -> &'static str {
        "BidirectionalPageInfo"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "startCursor" => Ok(Some(match &self.start_cursor {
                Some(cursor) => FieldValue::Json(Json::Str(cursor.clone())),
                None => FieldValue::Null,
            })),
            "endCursor" => Ok(Some(match &self.end_cursor {
                Some(cursor) => FieldValue::Json(Json::Str(cursor.clone())),
                None => FieldValue::Null,
            })),
            "hasNextPage" => Ok(Some(FieldValue::Json(Json::Bool(self.has_next_page)))),
            "hasPreviousPage" => Ok(Some(FieldValue::Json(Json::Bool(self.has_previous_page)))),
            _ => Ok(None),
        }
    }
}

// ---------------------------------------------------------------------------
// Root fields
// ---------------------------------------------------------------------------

fn empty_response() -> FieldValue {
    FieldValue::Object(Arc::new(crate::temporary_settings::EmptyResponseObject))
}

fn require_authenticated(actor: Actor, message: &str) -> Result<(), ExecError> {
    if actor.is_authenticated() {
        Ok(())
    } else {
        Err(ExecError::new(message))
    }
}

pub const ROOT_FIELDS: &[&str] = &["searchJobs", "validateSearchJob"];
pub const MUTATION_FIELDS: &[&str] = &["createSearchJob", "cancelSearchJob", "deleteSearchJob"];

pub fn is_root_field(field: &str) -> bool {
    ROOT_FIELDS.contains(&field)
}

pub fn is_mutation_field(field: &str) -> bool {
    MUTATION_FIELDS.contains(&field)
}

/// `Query.searchJobs` / `Query.validateSearchJob`.
pub fn root_field(
    handle: &SearchJobsHandle,
    field: &str,
    arguments: &Arguments,
    actor: Actor,
) -> Result<Option<FieldValue>, ExecError> {
    match field {
        "searchJobs" => {
            let args = connection_args(arguments)?;
            Ok(Some(FieldValue::Object(Arc::new(
                SearchJobsConnectionObject::new(Arc::clone(handle), args, actor),
            ))))
        }
        "validateSearchJob" => {
            // `service.ValidateSearchJob`: authenticated users only.
            require_authenticated(
                actor,
                "search jobs can only be validated by an authenticated user",
            )?;
            let query = arguments.string("query").unwrap_or_default();
            handle.validate_query(query).map_err(ExecError::new)?;
            Ok(Some(empty_response()))
        }
        _ => Ok(None),
    }
}

/// `Mutation.createSearchJob` / `cancelSearchJob` / `deleteSearchJob`.
pub fn mutation_field(
    handle: &SearchJobsHandle,
    field: &str,
    arguments: &Arguments,
    actor: Actor,
) -> Result<Option<FieldValue>, ExecError> {
    match field {
        "createSearchJob" => {
            // `service.CreateSearchJob` (`service.go:133-168`).
            require_authenticated(
                actor,
                "search jobs can only be created by an authenticated user",
            )?;
            let query = arguments.string("query").unwrap_or_default();
            handle.validate_query(query).map_err(ExecError::new)?;
            let uid = actor.uid().ok_or_else(|| {
                ExecError::new("search jobs can only be created by an authenticated user")
            })?;
            let db_actor = store_actor(actor);
            let id = handle
                .db
                .create(db_actor, query, uid)
                .map_err(ExecError::new)?;
            let job = handle.db.get(db_actor, id).map_err(ExecError::new)?;
            Ok(Some(FieldValue::Object(Arc::new(SearchJobObject::new(
                job,
                Arc::clone(handle),
            )))))
        }
        "cancelSearchJob" => {
            let id = arguments.string("id").unwrap_or_default();
            let job_id = unmarshal_search_job_id(id).map_err(ExecError::new)?;
            handle
                .db
                .cancel(store_actor(actor), job_id, OffsetDateTime::now_utc())
                .map_err(ExecError::new)?;
            Ok(Some(empty_response()))
        }
        "deleteSearchJob" => {
            let id = arguments.string("id").unwrap_or_default();
            let job_id = unmarshal_search_job_id(id).map_err(ExecError::new)?;
            handle
                .db
                .delete(store_actor(actor), job_id)
                .map_err(ExecError::new)?;
            Ok(Some(empty_response()))
        }
        _ => Ok(None),
    }
}

/// `NodeResolvers()["SearchJob"]` (M9.68a).
pub fn node_field(
    handle: &SearchJobsHandle,
    id: &str,
    actor: Actor,
) -> Result<Option<FieldValue>, ExecError> {
    let job_id = unmarshal_search_job_id(id).map_err(ExecError::new)?;
    let job = handle
        .db
        .get(store_actor(actor), job_id)
        .map_err(ExecError::new)?;
    Ok(Some(FieldValue::Object(Arc::new(SearchJobObject::new(
        job,
        Arc::clone(handle),
    )))))
}

// ---------------------------------------------------------------------------
// Production adapter
// ---------------------------------------------------------------------------

fn block_on<F: Future>(future: F) -> F::Output {
    match tokio::runtime::Handle::try_current() {
        Ok(handle) => tokio::task::block_in_place(|| handle.block_on(future)),
        Err(_) => tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("search jobs blocking runtime")
            .block_on(future),
    }
}

/// `sg_db::exhaustive_search` over the frontend pool.
pub struct PgSearchJobsDatabase {
    pool: PgPool,
}

impl PgSearchJobsDatabase {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

impl SearchJobsDatabase for PgSearchJobsDatabase {
    fn list(
        &self,
        actor: StoreActor,
        args: &StoreListArgs,
    ) -> Result<Vec<ExhaustiveSearchJob>, String> {
        let pool = self.pool.clone();
        let args = args.clone();
        block_on(async move {
            store::list_exhaustive_search_jobs(&pool, actor, &args)
                .await
                .map_err(|error| error.to_string())
        })
    }

    fn get(&self, actor: StoreActor, id: i64) -> Result<ExhaustiveSearchJob, String> {
        let pool = self.pool.clone();
        block_on(async move {
            store::get_exhaustive_search_job(&pool, actor, id)
                .await
                .map_err(|error| error.to_string())
        })
    }

    fn create(&self, actor: StoreActor, query: &str, initiator_id: i32) -> Result<i64, String> {
        let pool = self.pool.clone();
        let query = query.to_owned();
        block_on(async move {
            store::create_exhaustive_search_job(&pool, actor, &query, initiator_id)
                .await
                .map_err(|error| error.to_string())
        })
    }

    fn cancel(&self, actor: StoreActor, id: i64, now: OffsetDateTime) -> Result<(), String> {
        let pool = self.pool.clone();
        block_on(async move {
            store::cancel_search_job(&pool, actor, id, now)
                .await
                .map(|_| ())
                .map_err(|error| error.to_string())
        })
    }

    fn delete(&self, actor: StoreActor, id: i64) -> Result<(), String> {
        let pool = self.pool.clone();
        block_on(async move {
            store::delete_exhaustive_search_job(&pool, actor, id)
                .await
                .map_err(|error| error.to_string())
        })
    }

    fn aggregate_states(
        &self,
        actor: StoreActor,
        id: i64,
    ) -> Result<BTreeMap<String, i64>, String> {
        let pool = self.pool.clone();
        block_on(async move {
            store::get_aggregate_repo_rev_state(&pool, actor, id)
                .await
                .map_err(|error| error.to_string())
        })
    }

    fn user_by_id(&self, id: i32) -> Result<Option<UserData>, String> {
        let pool = self.pool.clone();
        block_on(async move {
            let record = sg_db::users::get_session_user(&pool, id)
                .await
                .map_err(|error| error.to_string())?;
            Ok(record.map(crate::user_store::to_user_data))
        })
    }
}

/// `service.FromSearchClient`'s planner validation wired through the
/// frontend's `SearchExecutor` seam.
pub struct ExecutorSearchJobValidator {
    executor: Arc<dyn crate::search_graphql::SearchExecutor>,
}

impl ExecutorSearchJobValidator {
    pub fn new(executor: Arc<dyn crate::search_graphql::SearchExecutor>) -> Self {
        Self { executor }
    }
}

impl SearchJobQueryValidator for ExecutorSearchJobValidator {
    fn validate(&self, query: &str) -> Result<(), String> {
        let args = crate::search_graphql::SearchArgs {
            version: "V3".to_owned(),
            pattern_type: None,
            query: query.to_owned(),
        };
        let executor = Arc::clone(&self.executor);
        block_on(async move {
            executor
                .plan(&args, crate::search_graphql::SearchStreamMode::Batch)
                .await
                .map(|_| ())
                .map_err(|error| match error {
                    crate::search_graphql::SearchExecutionError::Query { message, .. } => message,
                    crate::search_graphql::SearchExecutionError::Other(message) => message,
                })
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graphql_resolvers::{FrontendRootResolver, UserObject};
    use sg_db::exhaustive_search::WorkerJob;
    use sg_graphql::exec::{execute_with, ExecRequest};
    use std::sync::Mutex;

    const BASE_SDL: &str = "
        scalar DateTime
        interface Node { id: ID! }
        type Query { dummy: String node(id: ID!): Node }
        type Mutation { dummy: String }
        type BidirectionalPageInfo {
            startCursor: String
            endCursor: String
            hasPreviousPage: Boolean!
            hasNextPage: Boolean!
        }
        type User implements Node {
            id: ID!
            username: String!
            displayName: String
            avatarURL: String
            namespaceName: String!
        }
        type EmptyResponse { alwaysNil: String }
    ";

    fn at(seconds: i64) -> OffsetDateTime {
        OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(seconds)
    }

    fn job(id: i64, query: &str, state: &str, created: i64) -> ExhaustiveSearchJob {
        ExhaustiveSearchJob {
            worker: WorkerJob {
                state: JobState::new(state),
                failure_message: String::new(),
                started_at: Some(at(created + 10)),
                finished_at: Some(at(created + 20)),
                process_after: None,
                num_resets: 0,
                num_failures: 0,
                worker_hostname: String::new(),
                cancel: false,
            },
            id,
            initiator_id: 1,
            query: query.to_owned(),
            created_at: at(created),
            updated_at: at(created),
            agg_state: JobState::new(state),
            is_aggregated: true,
        }
    }

    #[derive(Default)]
    struct FakeDb {
        jobs: Vec<ExhaustiveSearchJob>,
        calls: Mutex<Vec<String>>,
    }

    impl SearchJobsDatabase for FakeDb {
        fn list(
            &self,
            _actor: StoreActor,
            args: &StoreListArgs,
        ) -> Result<Vec<ExhaustiveSearchJob>, String> {
            self.calls.lock().unwrap().push("list".to_owned());
            let mut jobs: Vec<ExhaustiveSearchJob> = self
                .jobs
                .iter()
                .filter(|job| args.query.is_empty() || job.query.contains(&args.query))
                .filter(|job| {
                    args.states.is_empty()
                        || args
                            .states
                            .iter()
                            .any(|state| state.eq_ignore_ascii_case(job.agg_state.as_str()))
                })
                .cloned()
                .collect();
            let Some(pagination) = &args.pagination else {
                return Ok(jobs);
            };
            let ascending = if pagination.last.is_some() {
                !pagination.ascending
            } else {
                pagination.ascending
            };
            jobs.sort_by_key(|job| job.created_at);
            if !ascending {
                jobs.reverse();
            }
            let limit = pagination
                .first
                .or(pagination.last)
                .map(|limit| limit as usize);
            if let Some(limit) = limit {
                jobs.truncate(limit);
            }
            Ok(jobs)
        }

        fn get(&self, _actor: StoreActor, id: i64) -> Result<ExhaustiveSearchJob, String> {
            self.jobs
                .iter()
                .find(|job| job.id == id)
                .cloned()
                .ok_or_else(|| "sql: no rows in result set".to_owned())
        }

        fn create(
            &self,
            _actor: StoreActor,
            query: &str,
            initiator_id: i32,
        ) -> Result<i64, String> {
            self.calls
                .lock()
                .unwrap()
                .push(format!("create:{query}:{initiator_id}"));
            Ok(42)
        }

        fn cancel(&self, _actor: StoreActor, id: i64, _now: OffsetDateTime) -> Result<(), String> {
            self.calls.lock().unwrap().push(format!("cancel:{id}"));
            Ok(())
        }

        fn delete(&self, _actor: StoreActor, id: i64) -> Result<(), String> {
            self.calls.lock().unwrap().push(format!("delete:{id}"));
            Ok(())
        }

        fn aggregate_states(
            &self,
            _actor: StoreActor,
            _id: i64,
        ) -> Result<BTreeMap<String, i64>, String> {
            Ok(BTreeMap::from([
                ("completed".to_owned(), 2),
                ("failed".to_owned(), 1),
                ("processing".to_owned(), 3),
            ]))
        }

        fn user_by_id(&self, id: i32) -> Result<Option<UserData>, String> {
            Ok(Some(UserData {
                id,
                username: "alice".to_owned(),
                display_name: "Alice".to_owned(),
                avatar_url: String::new(),
                created_at: at(0),
                updated_at: at(0),
                site_admin: false,
                tos_accepted: false,
                builtin_auth: false,
                scim_controlled: false,
                completions_quota: None,
                code_completions_quota: None,
                primary_email: String::new(),
            }))
        }
    }

    fn validator(ok: bool) -> Arc<dyn SearchJobQueryValidator> {
        struct Fixed(bool);
        impl SearchJobQueryValidator for Fixed {
            fn validate(&self, _query: &str) -> Result<(), String> {
                if self.0 {
                    Ok(())
                } else {
                    Err("bad query".to_owned())
                }
            }
        }
        Arc::new(Fixed(ok))
    }

    fn schema() -> Arc<sg_graphql::Schema> {
        Arc::new(
            sg_graphql::schema::assemble(&[BASE_SDL, sg_graphql::schema::SEARCH_JOBS_SCHEMA])
                .expect("schema"),
        )
    }

    fn handle(db: FakeDb) -> SearchJobsHandle {
        Arc::new(SearchJobsResolver::new(
            Arc::new(db),
            Some(validator(true)),
            Arc::new(|| "https://sg.example.com".to_owned()),
        ))
    }

    fn user() -> UserObject {
        UserObject::from_data(UserData {
            id: 1,
            username: "alice".to_owned(),
            display_name: String::new(),
            avatar_url: String::new(),
            created_at: at(0),
            updated_at: at(0),
            site_admin: false,
            tos_accepted: false,
            builtin_auth: false,
            scim_controlled: false,
            completions_quota: None,
            code_completions_quota: None,
            primary_email: String::new(),
        })
    }

    fn execute(
        roots: &FrontendRootResolver,
        query: &str,
        actor: Actor,
    ) -> sg_graphql::exec::ExecResponse {
        execute_with(
            &schema(),
            ExecRequest {
                query,
                operation_name: "",
                variables: &serde_json::Map::new(),
                actor,
            },
            Some(roots),
        )
    }

    fn data(response: &sg_graphql::exec::ExecResponse) -> serde_json::Value {
        assert!(response.errors.is_empty(), "errors: {:?}", response.errors);
        serde_json::from_str(&response.data_json().expect("data")).expect("json")
    }

    #[test]
    fn connection_paginates_and_reports_page_info() {
        let db = FakeDb {
            jobs: vec![
                job(1, "one", "completed", 100),
                job(2, "two", "failed", 200),
                job(3, "three", "processing", 300),
            ],
            calls: Mutex::new(Vec::new()),
        };
        let roots =
            FrontendRootResolver::with_user(Some(user())).with_search_jobs(Some(handle(db)));
        let response = execute(
            &roots,
            "{ searchJobs(first: 2) { totalCount pageInfo { hasNextPage hasPreviousPage endCursor } nodes { id query state } } }",
            Actor::User { uid: 1 },
        );
        let value = data(&response);
        let connection = &value["searchJobs"];
        assert_eq!(connection["totalCount"], 3);
        assert_eq!(connection["pageInfo"]["hasNextPage"], true);
        assert_eq!(connection["pageInfo"]["hasPreviousPage"], false);
        assert_eq!(connection["nodes"][0]["query"], "one");
        assert_eq!(connection["nodes"][1]["query"], "two");
        assert!(connection["pageInfo"]["endCursor"].is_string());
    }

    #[test]
    fn job_fields_match_the_baseline() {
        let db = FakeDb {
            jobs: vec![job(7, "repo:foo", "completed", 500)],
            calls: Mutex::new(Vec::new()),
        };
        let roots =
            FrontendRootResolver::with_user(Some(user())).with_search_jobs(Some(handle(db)));
        let response = execute(
            &roots,
            "{ searchJobs(first: 1) { nodes { id query state URL logURL creator { username } repoStats { total completed failed inProgress } } } }",
            Actor::User { uid: 1 },
        );
        let value = data(&response);
        let node = &value["searchJobs"]["nodes"][0];
        assert_eq!(node["id"], marshal_search_job_id(7));
        assert_eq!(node["query"], "repo:foo");
        assert_eq!(node["state"], "COMPLETED");
        assert_eq!(
            node["URL"],
            "https://sg.example.com/.api/search/export/7.jsonl"
        );
        assert_eq!(
            node["logURL"],
            "https://sg.example.com/.api/search/export/7.log"
        );
        assert_eq!(node["creator"]["username"], "alice");
        assert_eq!(node["repoStats"]["completed"], 2);
        assert_eq!(node["repoStats"]["failed"], 1);
        assert_eq!(node["repoStats"]["inProgress"], 3);
        assert_eq!(node["repoStats"]["total"], 6);
    }

    #[test]
    fn create_requires_authentication_and_validates_the_query() {
        let roots = FrontendRootResolver::with_user(Some(user()))
            .with_search_jobs(Some(handle(FakeDb::default())));
        let response = execute(
            &roots,
            r#"mutation { createSearchJob(query: "patterntype:structural foo") { id } }"#,
            Actor::User { uid: 1 },
        );
        assert_eq!(response.errors.len(), 1);
        assert_eq!(
            response.errors[0].message,
            "Structural search is not supported in Search Jobs"
        );

        let response = execute(
            &roots,
            r#"mutation { createSearchJob(query: "foo") { id } }"#,
            Actor::Anonymous,
        );
        assert_eq!(response.errors.len(), 1);
        assert_eq!(
            response.errors[0].message,
            "search jobs can only be created by an authenticated user"
        );
    }

    #[test]
    fn create_cancel_and_delete_dispatch_to_the_store() {
        let db = Arc::new(FakeDb {
            jobs: vec![job(42, "foo", "queued", 100)],
            calls: Mutex::new(Vec::new()),
        });
        let handle: SearchJobsHandle = Arc::new(SearchJobsResolver::new(
            db.clone(),
            Some(validator(true)),
            Arc::new(String::new),
        ));
        let roots = FrontendRootResolver::with_user(Some(user())).with_search_jobs(Some(handle));
        let response = execute(
            &roots,
            r#"mutation { createSearchJob(query: "foo") { id } }"#,
            Actor::User { uid: 1 },
        );
        assert_eq!(
            data(&response)["createSearchJob"]["id"],
            marshal_search_job_id(42)
        );
        let response = execute(
            &roots,
            &format!(
                r#"mutation {{ cancelSearchJob(id: "{}") {{ alwaysNil }} }}"#,
                marshal_search_job_id(42)
            ),
            Actor::User { uid: 1 },
        );
        assert!(response.errors.is_empty());
        let response = execute(
            &roots,
            &format!(
                r#"mutation {{ deleteSearchJob(id: "{}") {{ alwaysNil }} }}"#,
                marshal_search_job_id(42)
            ),
            Actor::User { uid: 1 },
        );
        assert!(response.errors.is_empty());
        assert_eq!(
            *db.calls.lock().unwrap(),
            vec![
                "create:foo:1".to_owned(),
                "cancel:42".to_owned(),
                "delete:42".to_owned()
            ]
        );
    }

    #[test]
    fn node_lookup_resolves_search_jobs() {
        let db = FakeDb {
            jobs: vec![job(9, "foo", "completed", 100)],
            calls: Mutex::new(Vec::new()),
        };
        let roots =
            FrontendRootResolver::with_user(Some(user())).with_search_jobs(Some(handle(db)));
        let response = execute(
            &roots,
            &format!(
                r#"{{ node(id: "{}") {{ __typename ... on SearchJob {{ query state }} }} }}"#,
                marshal_search_job_id(9)
            ),
            Actor::User { uid: 1 },
        );
        let value = data(&response);
        assert_eq!(value["node"]["__typename"], "SearchJob");
        assert_eq!(value["node"]["query"], "foo");
    }

    #[test]
    fn cursor_round_trips_and_validates_the_column() {
        let record = job(5, "foo", "processing", 1_700_000_000);
        let cursor = marshal_cursor(&record, "created_at").expect("cursor");
        assert_eq!(
            unmarshal_cursor(&cursor, "created_at").expect("values"),
            vec![
                CursorValue::Text(go_rfc3339_nano(record.created_at)),
                CursorValue::Int(5)
            ]
        );
        let error = unmarshal_cursor(&cursor, "query").expect_err("column mismatch");
        assert_eq!(error, "expected a \"query\" cursor, got \"created_at\"");
    }

    #[test]
    fn aggregate_stats_follow_the_service_mapping() {
        let states = BTreeMap::from([
            ("completed".to_owned(), 2),
            ("failed".to_owned(), 1),
            ("queued".to_owned(), 3),
        ]);
        let stats = repo_rev_stats(&states).expect("stats");
        assert_eq!(
            stats,
            RepoRevJobStats {
                total: 6,
                completed: 2,
                failed: 1,
                in_progress: 3
            }
        );
        let canceled = BTreeMap::from([("completed".to_owned(), 2), ("canceled".to_owned(), 1)]);
        let stats = repo_rev_stats(&canceled).expect("stats");
        assert_eq!(stats.in_progress, 0);
        // Canceled does not contribute to any bucket, so `Total` is the sum
        // of completed + failed + inProgress (the baseline service mapping).
        assert_eq!(stats.total, 2);
    }
}
