#![forbid(unsafe_code)]

//! Read-only diagnostic `Query` fields (M9.14c).
//!
//! This module replaces the M9.13b deferral ledger: the four fields are now
//! wired through an injectable [`DiagnosticsStore`] seam. A request only
//! carries a [`DiagnosticsPrefetch`] when a store was injected, and a field is
//! only present in it when the operation selected that field. When the seam is
//! absent (or a field was not selected), the resolvers return `Ok(None)`, so
//! responses keep the pre-slice "no ported resolver" behaviour.
//!
//! | GraphQL field | Go baseline |
//! |---|---|
//! | `Query.statusMessages` | `cmd/frontend/graphqlbackend/status_messages.go`, `internal/repos/status_messages.go` |
//! | `Query.repositoryStats` | `cmd/frontend/graphqlbackend/repository_stats.go` |
//! | `Query.backgroundJobs` | `cmd/frontend/graphqlbackend/background_jobs.go`, `internal/goroutine/recorder` |
//! | `Query.gitservers` | `cmd/frontend/graphqlbackend/gitserver.go` |
//!
//! Explicitly not ported (see the slice overview §5):
//!
//! * `RepositoryStats.indexed` / `indexedLinesCount`: the baseline uses the
//!   process-global `search.Indexed()` zoekt streamer, which has no injectable
//!   seam. Both fields resolve as `Ok(None)`.
//! * `ExternalServiceSyncError.externalService`: needs the external-service
//!   row plus the resolver's URL transformation.
//!
//! The executor is synchronous; everything here is prefetched per request and
//! replayed from [`DiagnosticsPrefetch`].

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use base64::Engine as _;
use time::OffsetDateTime;

use sg_db::repo_statistics::RepoStatistics;
use sg_graphql::exec::{Actor, Arguments, ExecError, FieldValue, ObjectValue, RootField};
use sg_graphql::json::Json;

use crate::graphql_resolvers::{datetime_rfc3339, unmarshal_relay_spec_string, PageInfoObject};

/// `defaultRecentRunCount` (`background_jobs.go:22`).
pub const DEFAULT_RECENT_RUN_COUNT: i32 = 5;

/// `dayCountForStats` (`background_jobs.go:19`); part of the store contract
/// (the reader aggregates this many days), not a GraphQL input.
pub const DAY_COUNT_FOR_STATS: i32 = 7;

/// The storage seam for the four diagnostic fields.
#[async_trait]
pub trait DiagnosticsStore: Send + Sync {
    /// Inputs of `repos.FetchStatusMessages` before composition: external
    /// service sync errors, `repo_statistics`, and (unless `include_zoekt` is
    /// false, i.e. dotcom) the `zoekt_repos` statistics. A query failure is
    /// returned as the baseline's wrapped error text.
    async fn status_message_inputs(
        &self,
        include_zoekt: bool,
    ) -> Result<StatusMessageInputs, String>;

    /// `RepoStatistics.GetRepoStatistics`, `GetGitserverGitDirSize` and
    /// `CountRepoEmbeddings`. Each arm keeps its own error so the GraphQL
    /// fields fail independently like the baseline's per-`sync.Once` getters.
    async fn repository_stats(&self) -> RepositoryStatsData;

    /// `recorder.GetBackgroundJobInfos(cache, after, recentRunCount,
    /// dayCountForStats)`: jobs ordered by name, already filtered by `after`.
    async fn background_jobs(
        &self,
        after: &str,
        recent_run_count: i32,
    ) -> Result<Vec<BackgroundJobData>, String>;

    /// `gitserverClient.SystemsInfo`.
    async fn gitservers(&self) -> Result<Vec<GitserverInfoData>, String>;
}

/// Static inputs of one request's diagnostic fields. `dotcom` mirrors
/// `dotcom.SourcegraphDotComMode()`; the other two are site configuration
/// values read per request.
#[derive(Clone)]
pub struct DiagnosticsDeps {
    pub store: Arc<dyn DiagnosticsStore>,
    pub dotcom: bool,
    pub disable_auto_git_updates: bool,
    /// `SiteConfig().GitserverDiskUsageWarningThreshold` (`None` -> 90).
    pub disk_usage_threshold: Option<i32>,
}

/// Raw `FetchStatusMessages` inputs.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct StatusMessageInputs {
    pub sync_errors: Vec<SyncErrorInput>,
    pub stats: RepoStatistics,
    /// `None` when dotcom mode skips the zoekt statistics query.
    pub zoekt: Option<ZoektStats>,
}

/// One `database.ExternalServiceSyncError`.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct SyncErrorInput {
    pub message: String,
    pub service_id: i64,
}

/// `database.ZoektRepoStatistics` subset used by the status message.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ZoektStats {
    pub total: i64,
    pub indexed: i32,
    pub not_indexed: i32,
}

/// `protocol.SystemInfo` plus the percentage the gitserver client computes.
/// `percent_used_bits` keeps the snapshot `Eq`; use [`Self::percent_used`].
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct GitserverInfoData {
    pub address: String,
    pub free_space: u64,
    pub total_space: u64,
    pub percent_used_bits: u32,
}

impl GitserverInfoData {
    pub fn with_percent_used(
        address: impl Into<String>,
        free_space: u64,
        total_space: u64,
        percent_used: f32,
    ) -> Self {
        Self {
            address: address.into(),
            free_space,
            total_space,
            percent_used_bits: percent_used.to_bits(),
        }
    }

    pub fn percent_used(&self) -> f32 {
        f32::from_bits(self.percent_used_bits)
    }
}

/// `repositoryStatsResolver`'s three lazily-computed halves.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RepositoryStatsData {
    pub stats: Result<RepoStatistics, String>,
    pub git_dir_bytes: Result<i64, String>,
    pub embedded: Result<i32, String>,
}

/// One `recorder.JobInfo`.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct BackgroundJobData {
    pub id: String,
    pub name: String,
    pub routines: Vec<RoutineData>,
}

/// One `recorder.RoutineInfo`.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct RoutineData {
    pub name: String,
    pub routine_type: String,
    pub description: String,
    pub interval_ms: i32,
    pub instances: Vec<RoutineInstanceData>,
    pub recent_runs: Vec<RecentRunData>,
    pub stats: RoutineStatsData,
}

/// One `recorder.RoutineInstanceInfo`.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct RoutineInstanceData {
    pub host_name: String,
    pub last_started_at: Option<OffsetDateTime>,
    pub last_stopped_at: Option<OffsetDateTime>,
}

/// One `recorder.RoutineRun`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecentRunData {
    pub at: OffsetDateTime,
    pub host_name: String,
    pub duration_ms: i32,
    pub error_message: String,
}

impl Default for RecentRunData {
    fn default() -> Self {
        Self {
            at: OffsetDateTime::from_unix_timestamp(0).expect("epoch is representable"),
            host_name: String::new(),
            duration_ms: 0,
            error_message: String::new(),
        }
    }
}

/// One `recorder.RoutineRunStats`.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct RoutineStatsData {
    pub since: Option<OffsetDateTime>,
    pub run_count: i32,
    pub error_count: i32,
    pub min_duration_ms: i32,
    pub avg_duration_ms: i32,
    pub max_duration_ms: i32,
}

/// `backgroundJobsArgs` after graph-gophers applied schema defaults.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Default)]
pub struct BackgroundJobsArgs {
    pub first: Option<i32>,
    pub after: Option<String>,
    pub recent_run_count: Option<i32>,
}

impl BackgroundJobsArgs {
    pub fn from_arguments(arguments: &Arguments) -> Self {
        Self {
            first: arguments.i64("first").map(|value| value as i32),
            after: arguments.string("after").map(str::to_owned),
            recent_run_count: arguments.i64("recentRunCount").map(|value| value as i32),
        }
    }
}

/// A fully-resolved `backgroundJobConnectionResolver`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BackgroundJobsConnectionData {
    /// The reader's output (ordered by name, `after` already applied). The
    /// resolver applies `first` truncation and page-info independently.
    pub jobs: Vec<BackgroundJobData>,
    pub first: Option<i32>,
}

/// The per-request diagnostic snapshot. A `None` arm means "not prefetched"
/// and replays as the unported-resolver behaviour.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DiagnosticsPrefetch {
    pub status_messages: Option<Result<Vec<StatusMessageData>, String>>,
    pub repository_stats: Option<RepositoryStatsData>,
    pub background_jobs:
        HashMap<BackgroundJobsArgs, Result<BackgroundJobsConnectionData, String>>,
    pub gitservers: Option<Result<Vec<GitserverInfoData>, String>>,
}

/// One composed `repos.StatusMessage` union member.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StatusMessageData {
    GitUpdatesDisabled {
        message: String,
    },
    NoRepositoriesDetected {
        message: String,
    },
    Cloning {
        message: String,
    },
    ExternalServiceSyncError {
        message: String,
        external_service_id: i64,
    },
    SyncError {
        message: String,
    },
    Indexing {
        not_indexed: i32,
        indexed: i32,
    },
    GitserverDiskThresholdReached {
        message: String,
    },
}

fn pluralize<'a>(count: i64, singular: &'a str, plural: &'a str) -> &'a str {
    if count == 1 {
        singular
    } else {
        plural
    }
}

/// `repos.FetchStatusMessages` composition, after the async inputs were
/// fetched. `gitservers` is only consulted when sync errors exist, matching the
/// baseline's early return before `SystemsInfo`.
pub fn compose_status_messages(
    disable_auto_git_updates: bool,
    disk_usage_threshold: Option<i32>,
    inputs: &StatusMessageInputs,
    gitservers: &[GitserverInfoData],
) -> Vec<StatusMessageData> {
    let mut messages = Vec::new();
    if disable_auto_git_updates {
        messages.push(StatusMessageData::GitUpdatesDisabled {
            message: "Repositories will not be cloned or updated.".to_owned(),
        });
    }

    if inputs.sync_errors.is_empty() {
        if inputs.stats.total == 0 {
            messages.push(StatusMessageData::NoRepositoriesDetected {
                message: "No repositories have been added to Sourcegraph.".to_owned(),
            });
        }
        return messages;
    }

    for sync_error in &inputs.sync_errors {
        if !sync_error.message.is_empty() {
            messages.push(StatusMessageData::ExternalServiceSyncError {
                message: sync_error.message.clone(),
                external_service_id: sync_error.service_id,
            });
        }
    }

    let stats = &inputs.stats;
    if stats.failed_fetch > 0 {
        messages.push(StatusMessageData::SyncError {
            message: format!(
                "{} {} failed last attempt to sync content from code host",
                stats.failed_fetch,
                pluralize(stats.failed_fetch, "repository", "repositories")
            ),
        });
    }

    let uncloned = stats.not_cloned + stats.cloning;
    if uncloned > 0 {
        let mut sentences = Vec::new();
        if stats.not_cloned > 0 {
            sentences.push(format!(
                "{} {} enqueued for cloning.",
                stats.not_cloned,
                pluralize(stats.not_cloned, "repository", "repositories")
            ));
        }
        if stats.cloning > 0 {
            sentences.push(format!(
                "{} {} currently cloning...",
                stats.cloning,
                pluralize(stats.cloning, "repository", "repositories")
            ));
        }
        messages.push(StatusMessageData::Cloning {
            message: sentences.join(" "),
        });
    }

    if let Some(zoekt) = &inputs.zoekt {
        if zoekt.not_indexed > 0 {
            messages.push(StatusMessageData::Indexing {
                not_indexed: zoekt.not_indexed,
                indexed: zoekt.indexed,
            });
        }
    }

    let threshold = disk_usage_threshold.unwrap_or(90);
    for server in gitservers {
        if server.percent_used() >= threshold as f32 {
            messages.push(StatusMessageData::GitserverDiskThresholdReached {
                message: format!(
                    "The disk usage on gitserver {:?} is over {}% ({:.2}% used).",
                    server.address,
                    threshold,
                    server.percent_used()
                ),
            });
        }
    }

    messages
}

async fn load_gitservers(
    deps: &DiagnosticsDeps,
    cache: &mut Option<Result<Vec<GitserverInfoData>, String>>,
) -> Result<Vec<GitserverInfoData>, String> {
    if let Some(cached) = cache {
        return cached.clone();
    }
    let result = deps.store.gitservers().await;
    *cache = Some(result.clone());
    result
}

/// Prefetch every selected diagnostic root field, applying the same site-admin
/// gate as the baseline. Prefetching errors are stored and replayed per field.
pub async fn prefetch_diagnostics(
    deps: &DiagnosticsDeps,
    fields: &[RootField],
    actor: Actor,
    viewer_site_admin: bool,
) -> DiagnosticsPrefetch {
    let mut prefetch = DiagnosticsPrefetch::default();
    let admin = crate::admin_store::check_current_user_is_site_admin(actor, viewer_site_admin);
    let mut gitservers_lookup: Option<Result<Vec<GitserverInfoData>, String>> = None;

    if field_selected(fields, "gitservers") {
        prefetch.gitservers = Some(match &admin {
            Err(message) => Err(message.clone()),
            Ok(()) => load_gitservers(deps, &mut gitservers_lookup).await,
        });
    }

    if field_selected(fields, "statusMessages") {
        prefetch.status_messages = Some(match &admin {
            Err(message) => Err(message.clone()),
            Ok(()) => match deps.store.status_message_inputs(!deps.dotcom).await {
                Err(message) => Err(message),
                Ok(inputs) => {
                    let servers = if inputs.sync_errors.is_empty() {
                        // `FetchStatusMessages` returns before `SystemsInfo`.
                        Vec::new()
                    } else {
                        match load_gitservers(deps, &mut gitservers_lookup).await {
                            Ok(servers) => servers,
                            Err(message) => {
                                return finish_diagnostics(
                                    prefetch_with_error(prefetch, message),
                                    &admin,
                                    deps,
                                    fields,
                                )
                                .await;
                            }
                        }
                    };
                    Ok(compose_status_messages(
                        deps.disable_auto_git_updates,
                        deps.disk_usage_threshold,
                        &inputs,
                        &servers,
                    ))
                }
            },
        });
    }

    finish_diagnostics(prefetch, &admin, deps, fields).await
}

fn prefetch_with_error(
    mut prefetch: DiagnosticsPrefetch,
    message: String,
) -> DiagnosticsPrefetch {
    prefetch.status_messages = Some(Err(message));
    prefetch
}

async fn finish_diagnostics(
    mut prefetch: DiagnosticsPrefetch,
    admin: &Result<(), String>,
    deps: &DiagnosticsDeps,
    fields: &[RootField],
) -> DiagnosticsPrefetch {
    if field_selected(fields, "repositoryStats") {
        prefetch.repository_stats = Some(match admin {
            Err(message) => RepositoryStatsData {
                stats: Err(message.clone()),
                git_dir_bytes: Err(message.clone()),
                embedded: Err(message.clone()),
            },
            Ok(()) => deps.store.repository_stats().await,
        });
    }

    for field in fields {
        if field.name != "backgroundJobs" {
            continue;
        }
        let args = BackgroundJobsArgs::from_arguments(&field.arguments);
        if prefetch.background_jobs.contains_key(&args) {
            continue;
        }
        let outcome = match admin {
            Err(message) => Err(message.clone()),
            Ok(()) => {
                let after = match &args.after {
                    None => Ok(String::new()),
                    Some(raw) => unmarshal_relay_spec_string(raw),
                };
                match after {
                    Err(message) => Err(message),
                    Ok(after) => {
                        let recent = args.recent_run_count.unwrap_or(DEFAULT_RECENT_RUN_COUNT);
                        match deps.store.background_jobs(&after, recent).await {
                            Err(message) => Err(message),
                            Ok(jobs) => Ok(BackgroundJobsConnectionData {
                                jobs,
                                first: args.first,
                            }),
                        }
                    }
                }
            }
        };
        prefetch.background_jobs.insert(args, outcome);
    }

    prefetch
}

/// Recursively test whether any selection set (including inline fragments and
/// fragment spreads) selects `name`.
pub fn field_selected(fields: &[RootField], name: &str) -> bool {
    for field in fields {
        if field.name == name {
            return true;
        }
        if field_selected(&field.selections, name) {
            return true;
        }
        for (_, selection) in &field.type_conditions {
            if field_selected(selection, name) {
                return true;
            }
        }
    }
    false
}

/// `relay.MarshalID(kind, spec)` for a string spec.
fn marshal_relay_string(kind: &str, spec: &str) -> String {
    base64::engine::general_purpose::URL_SAFE.encode(format!(
        "{kind}:{}",
        serde_json::to_string(spec).expect("string is JSON-serializable")
    ))
}

/// `Query.statusMessages`: the list of union members.
pub struct StatusMessageObject {
    pub message: StatusMessageData,
}

impl ObjectValue for StatusMessageObject {
    fn type_name(&self) -> &'static str {
        match &self.message {
            StatusMessageData::GitUpdatesDisabled { .. } => "GitUpdatesDisabled",
            StatusMessageData::NoRepositoriesDetected { .. } => "NoRepositoriesDetected",
            StatusMessageData::Cloning { .. } => "CloningProgress",
            StatusMessageData::ExternalServiceSyncError { .. } => "ExternalServiceSyncError",
            StatusMessageData::SyncError { .. } => "SyncError",
            StatusMessageData::Indexing { .. } => "IndexingProgress",
            StatusMessageData::GitserverDiskThresholdReached { .. } => {
                "GitserverDiskThresholdReached"
            }
        }
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            // `statusMessageResolver.Message`: `Indexing` is not handled and
            // falls through to the "unknown type" error (baseline quirk).
            "message" => match &self.message {
                StatusMessageData::GitUpdatesDisabled { message }
                | StatusMessageData::NoRepositoriesDetected { message }
                | StatusMessageData::Cloning { message }
                | StatusMessageData::ExternalServiceSyncError { message, .. }
                | StatusMessageData::SyncError { message }
                | StatusMessageData::GitserverDiskThresholdReached { message } => {
                    Ok(Some(FieldValue::Json(Json::Str(message.clone()))))
                }
                StatusMessageData::Indexing { .. } => {
                    Err(ExecError::new("status message is of unknown type"))
                }
            },
            // `indexingProgressMessageResolver`.
            "notIndexed" => match &self.message {
                StatusMessageData::Indexing { not_indexed, .. } => {
                    Ok(Some(FieldValue::Json(Json::Int(i64::from(*not_indexed)))))
                }
                _ => Ok(None),
            },
            "indexed" => match &self.message {
                StatusMessageData::Indexing { indexed, .. } => {
                    Ok(Some(FieldValue::Json(Json::Int(i64::from(*indexed)))))
                }
                _ => Ok(None),
            },
            // `statusMessageResolver.ExternalService`: not ported (needs the
            // `external_services` row lookup and URL transformation).
            "externalService" => Ok(None),
            _ => Ok(None),
        }
    }
}

/// `Query.repositoryStats`.
pub struct RepositoryStatsObject {
    pub data: RepositoryStatsData,
}

impl ObjectValue for RepositoryStatsObject {
    fn type_name(&self) -> &'static str {
        "RepositoryStats"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        let int_field = |value: Result<i64, String>| match value {
            Ok(value) => Ok(Some(FieldValue::Json(Json::Int(value)))),
            Err(message) => Err(ExecError::new(message)),
        };
        match field {
            // `GitDirBytes` is a `BigInt` (decimal string).
            "gitDirBytes" => match &self.data.git_dir_bytes {
                Ok(value) => Ok(Some(FieldValue::Json(Json::Str(value.to_string())))),
                Err(message) => Err(ExecError::new(message.clone())),
            },
            "total" => int_field(self.data.stats.clone().map(|stats| stats.total)),
            "cloned" => int_field(self.data.stats.clone().map(|stats| stats.cloned)),
            "cloning" => int_field(self.data.stats.clone().map(|stats| stats.cloning)),
            "notCloned" => int_field(self.data.stats.clone().map(|stats| stats.not_cloned)),
            "failedFetch" => int_field(self.data.stats.clone().map(|stats| stats.failed_fetch)),
            "corrupted" => int_field(self.data.stats.clone().map(|stats| stats.corrupted)),
            "embedded" => int_field(self.data.embedded.clone().map(i64::from)),
            // `indexed` / `indexedLinesCount`: requires the global zoekt
            // streamer, not ported (overview §5.2).
            "indexed" | "indexedLinesCount" => Ok(None),
            _ => Ok(None),
        }
    }
}

/// `Query.backgroundJobs`.
pub struct BackgroundJobConnectionObject {
    pub data: BackgroundJobsConnectionData,
}

impl BackgroundJobConnectionObject {
    fn page(&self) -> (Vec<BackgroundJobData>, bool, Option<String>) {
        let jobs = &self.data.jobs;
        let first = self.data.first.filter(|value| *value > -1);
        let has_next = match first {
            Some(first) => jobs.len() > usize::try_from(first).unwrap_or(0),
            None => false,
        };
        let truncated: Vec<BackgroundJobData> = match first {
            Some(first) if has_next => jobs[..usize::try_from(first).unwrap_or(0)]
                .iter()
                .cloned()
                .collect(),
            _ => jobs.clone(),
        };
        let end_cursor = if has_next && first.unwrap_or(0) > 0 {
            jobs.get(usize::try_from(first.unwrap_or(0) - 1).unwrap_or(0))
                .map(|job| marshal_relay_string("BackgroundJob", &job.id))
        } else {
            None
        };
        (truncated, has_next, end_cursor)
    }
}

impl ObjectValue for BackgroundJobConnectionObject {
    fn type_name(&self) -> &'static str {
        "BackgroundJobConnection"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "nodes" => {
                let (jobs, _, _) = self.page();
                Ok(Some(FieldValue::List {
                    element_type: Some("BackgroundJob"),
                    items: jobs
                        .into_iter()
                        .map(|job| FieldValue::Object(Arc::new(BackgroundJobObject { job })))
                        .collect(),
                }))
            }
            "totalCount" => Ok(Some(FieldValue::Json(Json::Int(
                i64::try_from(self.data.jobs.len()).unwrap_or(i64::MAX),
            )))),
            "pageInfo" => {
                let (_, has_next, end_cursor) = self.page();
                Ok(Some(FieldValue::Object(Arc::new(PageInfoObject::new(
                    has_next, end_cursor,
                )))))
            }
            _ => Ok(None),
        }
    }
}

pub struct BackgroundJobObject {
    pub job: BackgroundJobData,
}

impl ObjectValue for BackgroundJobObject {
    fn type_name(&self) -> &'static str {
        "BackgroundJob"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "id" => Ok(Some(FieldValue::Json(Json::Str(marshal_relay_string(
                "BackgroundJob",
                &self.job.id,
            ))))),
            "name" => Ok(Some(FieldValue::Json(Json::Str(self.job.name.clone())))),
            "routines" => Ok(Some(FieldValue::List {
                element_type: Some("BackgroundRoutine"),
                items: self
                    .job
                    .routines
                    .iter()
                    .cloned()
                    .map(|routine| {
                        FieldValue::Object(Arc::new(BackgroundRoutineObject { routine }))
                    })
                    .collect(),
            })),
            _ => Ok(None),
        }
    }
}

pub struct BackgroundRoutineObject {
    pub routine: RoutineData,
}

impl ObjectValue for BackgroundRoutineObject {
    fn type_name(&self) -> &'static str {
        "BackgroundRoutine"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "name" => Ok(Some(FieldValue::Json(Json::Str(self.routine.name.clone())))),
            "type" => Ok(Some(FieldValue::Json(Json::Str(
                self.routine.routine_type.clone(),
            )))),
            "description" => Ok(Some(FieldValue::Json(Json::Str(
                self.routine.description.clone(),
            )))),
            // `IntervalMs` returns nil for 0.
            "intervalMs" => Ok(Some(if self.routine.interval_ms == 0 {
                FieldValue::Null
            } else {
                FieldValue::Json(Json::Int(i64::from(self.routine.interval_ms)))
            })),
            "instances" => Ok(Some(FieldValue::List {
                element_type: Some("BackgroundRoutineInstance"),
                items: self
                    .routine
                    .instances
                    .iter()
                    .cloned()
                    .map(|instance| {
                        FieldValue::Object(Arc::new(BackgroundRoutineInstanceObject { instance }))
                    })
                    .collect(),
            })),
            "recentRuns" => Ok(Some(FieldValue::List {
                element_type: Some("BackgroundRoutineRecentRun"),
                items: self
                    .routine
                    .recent_runs
                    .iter()
                    .cloned()
                    .map(|recent_run| {
                        FieldValue::Object(Arc::new(BackgroundRoutineRecentRunObject { recent_run }))
                    })
                    .collect(),
            })),
            "stats" => Ok(Some(FieldValue::Object(Arc::new(
                BackgroundRoutineStatsObject {
                    stats: self.routine.stats.clone(),
                },
            )))),
            _ => Ok(None),
        }
    }
}

pub struct BackgroundRoutineInstanceObject {
    pub instance: RoutineInstanceData,
}

fn optional_datetime(value: Option<OffsetDateTime>) -> FieldValue {
    match value {
        Some(value) => FieldValue::Json(Json::Str(datetime_rfc3339(value))),
        None => FieldValue::Null,
    }
}

impl ObjectValue for BackgroundRoutineInstanceObject {
    fn type_name(&self) -> &'static str {
        "BackgroundRoutineInstance"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "hostName" => Ok(Some(FieldValue::Json(Json::Str(
                self.instance.host_name.clone(),
            )))),
            "lastStartedAt" => Ok(Some(optional_datetime(self.instance.last_started_at))),
            "lastStoppedAt" => Ok(Some(optional_datetime(self.instance.last_stopped_at))),
            _ => Ok(None),
        }
    }
}

pub struct BackgroundRoutineRecentRunObject {
    pub recent_run: RecentRunData,
}

impl ObjectValue for BackgroundRoutineRecentRunObject {
    fn type_name(&self) -> &'static str {
        "BackgroundRoutineRecentRun"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "at" => Ok(Some(FieldValue::Json(Json::Str(datetime_rfc3339(
                self.recent_run.at,
            ))))),
            "hostName" => Ok(Some(FieldValue::Json(Json::Str(
                self.recent_run.host_name.clone(),
            )))),
            "durationMs" => Ok(Some(FieldValue::Json(Json::Int(i64::from(
                self.recent_run.duration_ms,
            ))))),
            // `ErrorMessage` returns nil for "".
            "errorMessage" => Ok(Some(if self.recent_run.error_message.is_empty() {
                FieldValue::Null
            } else {
                FieldValue::Json(Json::Str(self.recent_run.error_message.clone()))
            })),
            _ => Ok(None),
        }
    }
}

pub struct BackgroundRoutineStatsObject {
    pub stats: RoutineStatsData,
}

impl ObjectValue for BackgroundRoutineStatsObject {
    fn type_name(&self) -> &'static str {
        "BackgroundRoutineStats"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            // `Stats.Since` is nil for the zero time.
            "since" => Ok(Some(optional_datetime(self.stats.since))),
            "runCount" => Ok(Some(FieldValue::Json(Json::Int(i64::from(
                self.stats.run_count,
            ))))),
            "errorCount" => Ok(Some(FieldValue::Json(Json::Int(i64::from(
                self.stats.error_count,
            ))))),
            "minDurationMs" => Ok(Some(FieldValue::Json(Json::Int(i64::from(
                self.stats.min_duration_ms,
            ))))),
            "avgDurationMs" => Ok(Some(FieldValue::Json(Json::Int(i64::from(
                self.stats.avg_duration_ms,
            ))))),
            "maxDurationMs" => Ok(Some(FieldValue::Json(Json::Int(i64::from(
                self.stats.max_duration_ms,
            ))))),
            _ => Ok(None),
        }
    }
}

/// `Query.gitservers`: `gqlutil.NewSliceConnectionResolver(servers, n, n)`,
/// whose PageInfo has a nil end cursor and `hasNextPage: false`.
pub struct GitserverInstanceConnectionObject {
    pub servers: Vec<GitserverInfoData>,
}

impl ObjectValue for GitserverInstanceConnectionObject {
    fn type_name(&self) -> &'static str {
        "GitserverInstanceConnection"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "nodes" => Ok(Some(FieldValue::List {
                element_type: Some("GitserverInstance"),
                items: self
                    .servers
                    .iter()
                    .cloned()
                    .map(|server| FieldValue::Object(Arc::new(GitserverInstanceObject { server })))
                    .collect(),
            })),
            "totalCount" => Ok(Some(FieldValue::Json(Json::Int(
                i64::try_from(self.servers.len()).unwrap_or(i64::MAX),
            )))),
            "pageInfo" => Ok(Some(FieldValue::Object(Arc::new(PageInfoObject::new(
                false, None,
            ))))),
            _ => Ok(None),
        }
    }
}

pub struct GitserverInstanceObject {
    pub server: GitserverInfoData,
}

impl ObjectValue for GitserverInstanceObject {
    fn type_name(&self) -> &'static str {
        "GitserverInstance"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "id" => Ok(Some(FieldValue::Json(Json::Str(marshal_relay_string(
                "GitserverInstance",
                &self.server.address,
            ))))),
            "address" => Ok(Some(FieldValue::Json(Json::Str(
                self.server.address.clone(),
            )))),
            "freeDiskSpaceBytes" => Ok(Some(FieldValue::Json(Json::Str(
                self.server.free_space.to_string(),
            )))),
            "totalDiskSpaceBytes" => Ok(Some(FieldValue::Json(Json::Str(
                self.server.total_space.to_string(),
            )))),
            _ => Ok(None),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn status_messages_compose_like_go() {
        let inputs = StatusMessageInputs {
            sync_errors: vec![
                SyncErrorInput {
                    message: "boom".into(),
                    service_id: 7,
                },
                SyncErrorInput {
                    message: String::new(),
                    service_id: 8,
                },
            ],
            stats: RepoStatistics {
                total: 10,
                failed_fetch: 1,
                not_cloned: 2,
                cloning: 1,
                ..RepoStatistics::default()
            },
            zoekt: Some(ZoektStats {
                total: 10,
                indexed: 6,
                not_indexed: 4,
            }),
        };
        let servers = vec![GitserverInfoData::with_percent_used("gs1", 10, 100, 95.5)];
        let messages = compose_status_messages(true, None, &inputs, &servers);
        assert_eq!(
            messages,
            vec![
                StatusMessageData::GitUpdatesDisabled {
                    message: "Repositories will not be cloned or updated.".into()
                },
                StatusMessageData::ExternalServiceSyncError {
                    message: "boom".into(),
                    external_service_id: 7
                },
                StatusMessageData::SyncError {
                    message: "1 repository failed last attempt to sync content from code host"
                        .into()
                },
                StatusMessageData::Cloning {
                    message:
                        "2 repositories enqueued for cloning. 1 repository currently cloning..."
                            .into()
                },
                StatusMessageData::Indexing {
                    not_indexed: 4,
                    indexed: 6
                },
                StatusMessageData::GitserverDiskThresholdReached {
                    message: "The disk usage on gitserver \"gs1\" is over 90% (95.50% used).".into()
                },
            ]
        );
    }

    #[test]
    fn no_sync_errors_returns_early() {
        let inputs = StatusMessageInputs {
            sync_errors: vec![],
            stats: RepoStatistics::default(),
            zoekt: Some(ZoektStats::default()),
        };
        assert_eq!(
            compose_status_messages(false, None, &inputs, &[]),
            vec![StatusMessageData::NoRepositoriesDetected {
                message: "No repositories have been added to Sourcegraph.".into()
            }]
        );
    }

    #[test]
    fn background_job_page_info_matches_go() {
        let jobs = vec![
            BackgroundJobData {
                id: "a".into(),
                ..BackgroundJobData::default()
            },
            BackgroundJobData {
                id: "b".into(),
                ..BackgroundJobData::default()
            },
        ];
        let object = BackgroundJobConnectionObject {
            data: BackgroundJobsConnectionData {
                jobs,
                first: Some(1),
            },
        };
        let (nodes, has_next, end) = object.page();
        assert_eq!(nodes.len(), 1);
        assert!(has_next);
        assert_eq!(end, Some(marshal_relay_string("BackgroundJob", "a")));
    }

    #[test]
    fn background_job_first_minus_one_returns_everything() {
        let jobs = vec![
            BackgroundJobData {
                id: "a".into(),
                ..BackgroundJobData::default()
            },
            BackgroundJobData {
                id: "b".into(),
                ..BackgroundJobData::default()
            },
        ];
        let object = BackgroundJobConnectionObject {
            data: BackgroundJobsConnectionData {
                jobs,
                first: Some(-1),
            },
        };
        let (nodes, has_next, end) = object.page();
        assert_eq!(nodes.len(), 2);
        assert!(!has_next);
        assert_eq!(end, None);
    }

    #[test]
    fn background_job_nullability_matches_go() {
        let arguments = Arguments::default();
        let routine = RoutineData {
            interval_ms: 0,
            recent_runs: vec![RecentRunData {
                error_message: String::new(),
                ..RecentRunData::default()
            }],
            ..RoutineData::default()
        };
        let routine_object = BackgroundRoutineObject {
            routine: routine.clone(),
        };
        assert!(matches!(
            routine_object.field("intervalMs", &arguments, Actor::Anonymous),
            Ok(Some(FieldValue::Null))
        ));
        let recent_run = BackgroundRoutineRecentRunObject {
            recent_run: routine.recent_runs[0].clone(),
        };
        assert!(matches!(
            recent_run.field("errorMessage", &arguments, Actor::Anonymous),
            Ok(Some(FieldValue::Null))
        ));
        let stats = BackgroundRoutineStatsObject {
            stats: RoutineStatsData {
                since: None,
                ..RoutineStatsData::default()
            },
        };
        assert!(matches!(
            stats.field("since", &arguments, Actor::Anonymous),
            Ok(Some(FieldValue::Null))
        ));
        let instance = BackgroundRoutineInstanceObject {
            instance: RoutineInstanceData::default(),
        };
        assert!(matches!(
            instance.field("lastStartedAt", &arguments, Actor::Anonymous),
            Ok(Some(FieldValue::Null))
        ));
    }

    #[test]
    fn repository_stats_embedded_and_git_dir_bytes() {
        let arguments = Arguments::default();
        let object = RepositoryStatsObject {
            data: RepositoryStatsData {
                stats: Ok(RepoStatistics {
                    total: 10,
                    cloned: 5,
                    ..RepoStatistics::default()
                }),
                git_dir_bytes: Ok(1_234_567_890_123),
                embedded: Ok(3),
            },
        };
        assert!(matches!(
            object.field("embedded", &arguments, Actor::Anonymous),
            Ok(Some(FieldValue::Json(Json::Int(3))))
        ));
        assert!(matches!(
            object.field("total", &arguments, Actor::Anonymous),
            Ok(Some(FieldValue::Json(Json::Int(10))))
        ));
        match object.field("gitDirBytes", &arguments, Actor::Anonymous) {
            Ok(Some(FieldValue::Json(Json::Str(value)))) => {
                assert_eq!(value, "1234567890123");
            }
            _ => panic!("unexpected gitDirBytes field"),
        }
        // `indexed` / `indexedLinesCount` stay unported.
        assert!(matches!(
            object.field("indexed", &arguments, Actor::Anonymous),
            Ok(None)
        ));
    }

    #[test]
    fn gitserver_instance_bytes_and_id_match_go() {
        let arguments = Arguments::default();
        let object = GitserverInstanceObject {
            server: GitserverInfoData::with_percent_used("10.0.0.1:3178", 1000, 4000, 12.5),
        };
        match object.field("id", &arguments, Actor::Anonymous) {
            Ok(Some(FieldValue::Json(Json::Str(value)))) => {
                assert_eq!(value, "R2l0c2VydmVySW5zdGFuY2U6IjEwLjAuMC4xOjMxNzgi");
            }
            _ => panic!("unexpected id field"),
        }
        assert!(matches!(
            object.field("freeDiskSpaceBytes", &arguments, Actor::Anonymous),
            Ok(Some(FieldValue::Json(Json::Str(value)))) if value == "1000"
        ));
        assert!(matches!(
            object.field("totalDiskSpaceBytes", &arguments, Actor::Anonymous),
            Ok(Some(FieldValue::Json(Json::Str(value)))) if value == "4000"
        ));
    }
}
