#![forbid(unsafe_code)]

//! Authz permissions GraphQL surface (M9.48a).
//!
//! Ports the opt-in `authz.graphql` resolver set from
//! `cmd/frontend/internal/authz/resolvers` and
//! `cmd/frontend/graphqlbackend/authz.graphql`:
//!
//! * `Query.permissionsSyncJobs` / `permissionsSyncingStats` /
//!   `bitbucketProjectPermissionJobs` / `usersWithPendingPermissions` /
//!   `authorizedUserRepositories`;
//! * `Repository.authorizedUsers` / `Repository.permissionsInfo` and
//!   `User.permissionsInfo`;
//! * the seven authz mutations (`set*`, `schedule*`,
//!   `cancelPermissionsSyncJob`).
//!
//! Every value comes from an existing table (`permission_sync_jobs`,
//! `user_repo_permissions`, `repo_permissions`, `user_pending_permissions`,
//! `explicit_permissions_bitbucket_projects_jobs`, `repo`, `users`,
//! `external_services`); no table, column or Redis key is added.
//!
//! The database is injected through [`AuthzDatabase`], a synchronous seam;
//! the production adapter ([`crate::authz_graphql_store`]) bridges to sqlx
//! with `tokio::task::block_in_place`.
//!
//! Authorization and error strings follow the baseline:
//! `auth.CheckCurrentUserIsSiteAdmin`, `auth.CheckSiteAdminOrSameUser`, the
//! `licensing.Check(FeatureACLs|FeatureExplicitPermissionsAPI)` gates and
//! `dotcom.SourcegraphDotComMode()`.

use std::fmt;
use std::sync::Arc;

use base64::Engine as _;
use sg_graphql::exec::{Actor, Arguments, ExecError, FieldValue, InputValue, ObjectValue};
use sg_graphql::json::Json;
use time::OffsetDateTime;

use crate::graphql_resolvers::{datetime_rfc3339, ConnectionPageInfoObject, PageInfoObject};

/// `permissionsSyncJobIDKind` (`permissions_sync_jobs.go:21`).
pub const PERMISSIONS_SYNC_JOB_KIND: &str = "PermissionsSyncJob";

/// `auth.ErrNotAuthenticated`.
pub const NOT_AUTHENTICATED: &str = "not authenticated";
/// `auth.ErrMustBeSiteAdmin`.
pub const MUST_BE_SITE_ADMIN: &str = "must be site admin";
/// `auth.ErrMustBeSiteAdminOrSameUser`.
pub const MUST_BE_SITE_ADMIN_OR_SAME_USER: &str =
    "must be authenticated as the authorized user or site admin";
/// `errDisabledSourcegraphDotCom` (`resolver.go:38`).
pub const DISABLED_DOTCOM: &str = "not enabled on sourcegraph.com";

/// `CancelPermissionsSyncJobResultMessage` (`authz.go:128-134`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CancelPermissionsSyncJobResult {
    Success,
    NotFound,
    Error,
}

impl CancelPermissionsSyncJobResult {
    fn as_str(self) -> &'static str {
        match self {
            Self::Success => "SUCCESS",
            Self::NotFound => "NOT_FOUND",
            Self::Error => "ERROR",
        }
    }
}

/// `database.PermissionsSyncJobReason.ResolveGroup`
/// (`internal/database/permission_sync_jobs.go:138-171`).
pub fn resolve_reason_group(reason: &str) -> &'static str {
    match reason {
        "REASON_MANUAL_REPO_SYNC" | "REASON_MANUAL_USER_SYNC" => "MANUAL",
        "REASON_GITHUB_USER_EVENT"
        | "REASON_GITHUB_USER_ADDED_EVENT"
        | "REASON_GITHUB_USER_REMOVED_EVENT"
        | "REASON_GITHUB_USER_MEMBERSHIP_ADDED_EVENT"
        | "REASON_GITHUB_USER_MEMBERSHIP_REMOVED_EVENT"
        | "REASON_GITHUB_TEAM_ADDED_TO_REPO_EVENT"
        | "REASON_GITHUB_TEAM_REMOVED_FROM_REPO_EVENT"
        | "REASON_GITHUB_ORG_MEMBER_ADDED_EVENT"
        | "REASON_GITHUB_ORG_MEMBER_REMOVED_EVENT"
        | "REASON_GITHUB_REPO_EVENT"
        | "REASON_GITHUB_REPO_MADE_PRIVATE_EVENT" => "WEBHOOK",
        "REASON_USER_OUTDATED_PERMS"
        | "REASON_USER_NO_PERMS"
        | "REASON_REPO_OUTDATED_PERMS"
        | "REASON_REPO_NO_PERMS"
        | "REASON_REPO_UPDATED_FROM_CODE_HOST" => "SCHEDULE",
        "REASON_USER_EMAIL_REMOVED"
        | "REASON_USER_EMAIL_VERIFIED"
        | "REASON_USER_ADDED"
        | "REASON_USER_ADDED_TO_ORG"
        | "REASON_USER_REMOVED_FROM_ORG"
        | "REASON_USER_ACCEPTED_ORG_INVITE"
        | "REASON_EXTERNAL_ACCOUNT_ADDED"
        | "REASON_EXTERNAL_ACCOUNT_DELETED" => "SOURCEGRAPH",
        _ => "UNKNOWN",
    }
}

/// `database.PermissionsSyncJobPriority.ToString` (`permission_sync_jobs.go:56-66`).
pub fn priority_to_string(priority: i32) -> &'static str {
    match priority {
        10 => "HIGH",
        5 => "MEDIUM",
        _ => "LOW",
    }
}

/// `relay.MarshalID(permissionsSyncJobIDKind, id)`.
pub fn marshal_permissions_sync_job_id(id: i32) -> String {
    base64::engine::general_purpose::URL_SAFE.encode(format!("{PERMISSIONS_SYNC_JOB_KIND}:{id}"))
}

/// `unmarshalPermissionsSyncJobID`.
pub fn unmarshal_permissions_sync_job_id(id: &str) -> Result<i32, String> {
    // `relay.UnmarshalKind` returns "" when the ID does not decode.
    let decoded = base64::engine::general_purpose::URL_SAFE.decode(id).ok();
    let kind = decoded
        .as_deref()
        .map(String::from_utf8_lossy)
        .and_then(|text| text.split_once(':').map(|(kind, _)| kind.to_owned()))
        .unwrap_or_default();
    if kind != PERMISSIONS_SYNC_JOB_KIND {
        return Err(format!(
            "expected graphql ID to have kind \"{PERMISSIONS_SYNC_JOB_KIND}\"; got \"{kind}\""
        ));
    }
    let text = String::from_utf8_lossy(decoded.as_deref().expect("decoded"));
    let (_, spec) = text.split_once(':').ok_or("invalid graphql.ID")?;
    serde_json::from_str(spec).map_err(|error| error.to_string())
}

/// Go's zero `time.Time` (`0001-01-01T00:00:00Z`).
pub fn go_zero_time() -> OffsetDateTime {
    OffsetDateTime::from_unix_timestamp(-62_135_596_800).expect("Go zero time is representable")
}

fn datetime(value: Option<OffsetDateTime>) -> FieldValue {
    match value {
        // `gqlutil.FromTime`: a zero Go time is `null`.
        Some(value) if value != go_zero_time() => {
            FieldValue::Json(Json::Str(datetime_rfc3339(value)))
        }
        _ => FieldValue::Null,
    }
}

fn non_null_datetime(value: Option<OffsetDateTime>) -> FieldValue {
    FieldValue::Json(Json::Str(datetime_rfc3339(value.unwrap_or_else(go_zero_time))))
}

fn opt_str(value: &Option<String>) -> FieldValue {
    match value {
        Some(value) => FieldValue::Json(Json::Str(value.clone())),
        None => FieldValue::Null,
    }
}

// ---------------------------------------------------------------------------
// Data
// ---------------------------------------------------------------------------

/// `database.PermissionSyncCodeHostState` / `GraphQL CodeHostState`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CodeHostStateData {
    pub provider_id: String,
    pub provider_type: String,
    pub status: String,
    pub message: String,
}

/// One job plus its resolved `subject` (`resolveSubject`). `subject` is `None`
/// when the referenced user/repo was deleted, in which case the baseline drops
/// the job from the connection.
#[derive(Debug, Clone, PartialEq)]
pub struct PermissionsSyncJobNodeData {
    pub job: sg_db::permission_sync_jobs::PermissionSyncJobListRow,
    pub subject: Option<SubjectData>,
    /// `TriggeredByUser` (`permissions_sync_jobs.go:213-220`); `None` for a
    /// zero `triggered_by_user_id` or a deleted user.
    pub triggered_by_user: Option<crate::graphql_resolvers::UserData>,
}

/// `graphqlbackend.PermissionsSyncJobSubject` (`subject` in
/// `permissions_sync_jobs.go:130-137,321-339`).
#[derive(Debug, Clone, PartialEq)]
pub enum SubjectData {
    Repository(crate::graphql_resolvers::RepositoryData),
    User(crate::graphql_resolvers::UserData),
}

/// The `PermissionsSyncJobsConnection` payload. `nodes`/`page_info` carry the
/// `gqlutil.ConnectionResolver` pagination error (`first`/`last` missing)
/// while `total_count` still resolves, exactly like the baseline.
#[derive(Debug, Clone, PartialEq)]
pub struct PermissionsSyncJobsConnectionData {
    pub nodes: Result<Vec<PermissionsSyncJobNodeData>, String>,
    pub total_count: i32,
    pub page_info: Result<ConnectionPageInfoData, String>,
}

/// `gqlutil.ConnectionPageInfo` for the jobs connection.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ConnectionPageInfoData {
    pub start_cursor: Option<String>,
    pub end_cursor: Option<String>,
    pub has_next_page: bool,
    pub has_previous_page: bool,
}

/// `database.UserPermission` for the bitbucket job payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UserPermissionData {
    pub bind_id: String,
    pub permission: String,
}

/// `types.BitbucketProjectPermissionJob` projected to the GraphQL fields.
#[derive(Debug, Clone, PartialEq)]
pub struct BitbucketProjectPermissionJobData {
    pub id: i32,
    pub state: String,
    pub failure_message: Option<String>,
    pub queued_at: Option<OffsetDateTime>,
    pub started_at: Option<OffsetDateTime>,
    pub finished_at: Option<OffsetDateTime>,
    pub process_after: Option<OffsetDateTime>,
    pub num_resets: i32,
    pub num_failures: i32,
    pub project_key: String,
    pub external_service_id: i64,
    pub permissions: Vec<UserPermissionData>,
    pub unrestricted: bool,
}

/// `permissionsInfoResolver` data (`permissions_info.go:22-31`).
#[derive(Debug, Clone, PartialEq)]
pub struct PermissionsInfoData {
    pub user_id: i32,
    pub repo_id: i32,
    pub synced_at: Option<OffsetDateTime>,
    pub updated_at: Option<OffsetDateTime>,
    pub source: Option<String>,
    pub unrestricted: bool,
}

/// `database.UserPermission` reason/updatedAt (`ListUserPermissions`).
#[derive(Debug, Clone, PartialEq)]
pub struct PermissionsInfoRepositoryNodeData {
    pub repo: crate::graphql_resolvers::RepositoryData,
    pub reason: String,
    pub updated_at: Option<OffsetDateTime>,
}

/// `database.RepoPermission` (`ListRepoPermissions`).
#[derive(Debug, Clone, PartialEq)]
pub struct PermissionsInfoUserNodeData {
    pub user: crate::graphql_resolvers::UserData,
    pub reason: String,
    pub updated_at: Option<OffsetDateTime>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PermissionsInfoRepositoriesData {
    pub nodes: Result<Vec<PermissionsInfoRepositoryNodeData>, String>,
    pub total_count: i32,
    pub page_info: Result<ConnectionPageInfoData, String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PermissionsInfoUsersData {
    pub nodes: Result<Vec<PermissionsInfoUserNodeData>, String>,
    pub total_count: i32,
    pub page_info: Result<ConnectionPageInfoData, String>,
}

/// `authorizedUserRepositories` / `Repository.authorizedUsers` payloads.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct AuthorizedRepositoriesData {
    pub repos: Vec<crate::graphql_resolvers::RepositoryData>,
    pub total_count: i32,
    pub has_next_page: bool,
    pub end_cursor: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct AuthorizedUsersData {
    pub users: Vec<crate::graphql_resolvers::UserData>,
    pub total_count: i32,
    pub has_next_page: bool,
    pub end_cursor: Option<String>,
}

/// `PermissionsSyncingStats` (`resolver.go:656-706`).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PermissionsSyncingStatsData {
    pub queue_size: i32,
    pub users_with_latest_job_failing: i32,
    pub repos_with_latest_job_failing: i32,
    pub users_with_no_permissions: i32,
    pub repos_with_no_permissions: i32,
    pub users_with_stale_permissions: i32,
    pub repos_with_stale_permissions: i32,
}

/// `graphqlbackend.UserPermissionInput` / `UserSubRepoPermission` inputs.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct UserPermissionInput {
    pub bind_id: String,
    pub permission: String,
}

/// The deprecated `pathIncludes`/`pathExcludes` and the new `paths` field.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct SubRepoPermissionInput {
    pub bind_id: String,
    pub path_includes: Option<Vec<String>>,
    pub path_excludes: Option<Vec<String>>,
    pub paths: Option<Vec<String>>,
}

/// Arguments of `Query.permissionsSyncJobs`.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct PermissionsSyncJobsArgs {
    pub first: Option<i64>,
    pub last: Option<i64>,
    pub after: Option<String>,
    pub before: Option<String>,
    pub reason_group: Option<String>,
    pub state: Option<String>,
    pub search_type: Option<String>,
    pub query: Option<String>,
    pub user_id: Option<i32>,
    pub repo_id: Option<i32>,
    pub partial: Option<bool>,
}

/// Arguments of `Query.bitbucketProjectPermissionJobs`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct BitbucketProjectPermissionJobsArgs {
    pub project_keys: Option<Vec<String>>,
    pub status: Option<String>,
    pub count: Option<i32>,
}

/// Pagination + search arguments of `PermissionsInfo.repositories/users`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct InfoConnectionArgs {
    pub first: Option<i32>,
    pub last: Option<i32>,
    pub after: Option<String>,
    pub before: Option<String>,
    pub query: Option<String>,
}

/// `Query.authorizedUserRepositories` arguments.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AuthorizedUserRepositoriesArgs {
    pub username: Option<String>,
    pub email: Option<String>,
    pub first: i32,
    pub after: Option<String>,
}

/// `Repository.authorizedUsers` arguments.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AuthorizedUsersArgs {
    pub first: i32,
    pub after: Option<String>,
}

// ---------------------------------------------------------------------------
// Database seam
// ---------------------------------------------------------------------------

/// Synchronous database seam for the authz GraphQL surface. The production
/// adapter bridges each call to sqlx with `block_in_place`/`Handle::block_on`.
pub trait AuthzDatabase: Send + Sync {
    /// `dotcom.SourcegraphDotComMode()`.
    fn dotcom(&self) -> bool;
    /// `licensing.Check(licensing.FeatureACLs)`.
    fn check_license_acls(&self) -> Result<(), String>;
    /// `licensing.Check(licensing.FeatureExplicitPermissionsAPI)`.
    fn check_license_explicit_permissions(&self) -> Result<(), String>;

    fn permissions_sync_jobs(
        &self,
        args: &PermissionsSyncJobsArgs,
    ) -> Result<PermissionsSyncJobsConnectionData, String>;
    fn permissions_syncing_stats(&self) -> Result<PermissionsSyncingStatsData, String>;
    fn bitbucket_project_permission_jobs(
        &self,
        args: &BitbucketProjectPermissionJobsArgs,
    ) -> Result<Vec<BitbucketProjectPermissionJobData>, String>;
    fn repository_permissions_info(&self, repo_id: i32) -> Result<PermissionsInfoData, String>;
    fn user_permissions_info(&self, user_id: i32) -> Result<PermissionsInfoData, String>;
    fn permissions_info_repositories(
        &self,
        user_id: i32,
        args: &InfoConnectionArgs,
    ) -> Result<PermissionsInfoRepositoriesData, String>;
    fn permissions_info_users(
        &self,
        repo_id: i32,
        args: &InfoConnectionArgs,
    ) -> Result<PermissionsInfoUsersData, String>;
    fn authorized_user_repositories(
        &self,
        args: &AuthorizedUserRepositoriesArgs,
    ) -> Result<AuthorizedRepositoriesData, String>;
    fn users_with_pending_permissions(&self) -> Result<Vec<String>, String>;
    fn authorized_users(
        &self,
        repo_id: i32,
        args: &AuthorizedUsersArgs,
    ) -> Result<AuthorizedUsersData, String>;

    fn set_repository_permissions_for_users(
        &self,
        repo_id: i32,
        permissions: &[UserPermissionInput],
        now: OffsetDateTime,
    ) -> Result<(), String>;
    fn set_repository_permissions_unrestricted(
        &self,
        repo_ids: &[i32],
        unrestricted: bool,
        now: OffsetDateTime,
    ) -> Result<(), String>;
    fn schedule_repository_permissions_sync(
        &self,
        repo_id: i32,
        triggered_by_user_id: i32,
        now: OffsetDateTime,
    ) -> Result<(), String>;
    fn schedule_user_permissions_sync(
        &self,
        user_id: i32,
        invalidate_caches: bool,
        triggered_by_user_id: i32,
        now: OffsetDateTime,
    ) -> Result<(), String>;
    fn set_sub_repository_permissions_for_users(
        &self,
        repo_id: i32,
        permissions: &[SubRepoPermissionInput],
        now: OffsetDateTime,
    ) -> Result<(), String>;
    fn set_repository_permissions_for_bitbucket_project(
        &self,
        project_key: &str,
        external_service_id: i64,
        permissions: &[UserPermissionInput],
        unrestricted: bool,
        now: OffsetDateTime,
    ) -> Result<(), String>;
    fn cancel_permissions_sync_job(
        &self,
        id: i32,
        reason: &str,
        now: OffsetDateTime,
    ) -> Result<CancelPermissionsSyncJobResult, String>;
}

/// Clonable handle stored on the root resolvers. `PartialEq`/`Eq` compare the
/// underlying `Arc` pointer so [`crate::graphql_resolvers::NestedPrefetch`]
/// keeps its derives.
#[derive(Clone)]
pub struct AuthzHandle(pub Arc<dyn AuthzDatabase>);

impl AuthzHandle {
    pub fn new(database: Arc<dyn AuthzDatabase>) -> Self {
        Self(database)
    }

    pub fn database(&self) -> &dyn AuthzDatabase {
        self.0.as_ref()
    }
}

impl fmt::Debug for AuthzHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("AuthzHandle")
    }
}

impl PartialEq for AuthzHandle {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}

impl Eq for AuthzHandle {}

// ---------------------------------------------------------------------------
// Objects
// ---------------------------------------------------------------------------

struct CodeHostStateObject {
    state: CodeHostStateData,
}

impl ObjectValue for CodeHostStateObject {
    fn type_name(&self) -> &'static str {
        "CodeHostState"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        Ok(Some(match field {
            "providerID" => FieldValue::Json(Json::Str(self.state.provider_id.clone())),
            "providerType" => FieldValue::Json(Json::Str(self.state.provider_type.clone())),
            "status" => FieldValue::Json(Json::Str(self.state.status.clone())),
            "message" => FieldValue::Json(Json::Str(self.state.message.clone())),
            _ => return Ok(None),
        }))
    }
}

struct PermissionsSyncJobReasonObject {
    group: &'static str,
    reason: String,
}

impl ObjectValue for PermissionsSyncJobReasonObject {
    fn type_name(&self) -> &'static str {
        "PermissionsSyncJobReasonWithGroup"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        Ok(Some(match field {
            "group" => FieldValue::Json(Json::Str(self.group.to_owned())),
            "reason" => {
                if self.reason.is_empty() {
                    FieldValue::Null
                } else {
                    FieldValue::Json(Json::Str(self.reason.clone()))
                }
            }
            _ => return Ok(None),
        }))
    }
}

/// `permissionsSyncJobResolver` (`permissions_sync_jobs.go:167-290`).
pub struct PermissionsSyncJobObject {
    job: sg_db::permission_sync_jobs::PermissionSyncJobListRow,
    subject: SubjectData,
    triggered_by_user: Option<crate::graphql_resolvers::UserData>,
}

impl PermissionsSyncJobObject {
    fn ran_for_ms(&self) -> i32 {
        match (self.job.started_at, self.job.finished_at) {
            (Some(started), Some(finished)) => {
                let millis = (finished - started).whole_milliseconds();
                i32::try_from(millis).unwrap_or_else(|_| {
                    if millis < 0 {
                        i32::MIN
                    } else {
                        i32::MAX
                    }
                })
            }
            _ => 0,
        }
    }
}

impl ObjectValue for PermissionsSyncJobObject {
    fn type_name(&self) -> &'static str {
        "PermissionsSyncJob"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        Ok(Some(match field {
            "id" => FieldValue::Json(Json::Str(marshal_permissions_sync_job_id(self.job.id))),
            "state" => FieldValue::Json(Json::Str(self.job.state.to_uppercase())),
            "failureMessage" => opt_str(&self.job.failure_message),
            "reason" => FieldValue::Object(Arc::new(PermissionsSyncJobReasonObject {
                group: resolve_reason_group(&self.job.reason),
                reason: self.job.reason.clone(),
            })),
            "cancellationReason" => opt_str(&self.job.cancellation_reason),
            // `TriggeredByUser` is resolved eagerly by the store; a missing
            // user is an error, which the store already rejects.
            "triggeredByUser" => match &self.triggered_by_user {
                Some(user) => FieldValue::Object(Arc::new(
                    crate::graphql_resolvers::UserObject::from_data(user.clone()),
                )),
                None => FieldValue::Null,
            },
            "queuedAt" => non_null_datetime(self.job.queued_at),
            "startedAt" => datetime(self.job.started_at),
            "finishedAt" => datetime(self.job.finished_at),
            "processAfter" => datetime(self.job.process_after),
            "ranForMs" => FieldValue::Json(Json::Int(i64::from(self.ran_for_ms()))),
            "numResets" => FieldValue::Json(Json::Int(i64::from(self.job.num_resets))),
            "numFailures" => FieldValue::Json(Json::Int(i64::from(self.job.num_failures))),
            "lastHeartbeatAt" => datetime(self.job.last_heartbeat_at),
            "workerHostname" => FieldValue::Json(Json::Str(self.job.worker_hostname.clone())),
            "cancel" => FieldValue::Json(Json::Bool(self.job.cancel)),
            "subject" => match &self.subject {
                SubjectData::Repository(repo) => FieldValue::Object(Arc::new(
                    crate::graphql_resolvers::RepositoryObject::from_data(repo.clone()),
                )),
                SubjectData::User(user) => FieldValue::Object(Arc::new(
                    crate::graphql_resolvers::UserObject::from_data(user.clone()),
                )),
            },
            "priority" => FieldValue::Json(Json::Str(
                priority_to_string(self.job.priority).to_owned(),
            )),
            "noPerms" => FieldValue::Json(Json::Bool(self.job.no_perms)),
            "invalidateCaches" => FieldValue::Json(Json::Bool(self.job.invalidate_caches)),
            "permissionsAdded" => FieldValue::Json(Json::Int(i64::from(self.job.permissions_added))),
            "permissionsRemoved" => {
                FieldValue::Json(Json::Int(i64::from(self.job.permissions_removed)))
            }
            "permissionsFound" => FieldValue::Json(Json::Int(i64::from(self.job.permissions_found))),
            "codeHostStates" => FieldValue::List {
                element_type: Some("CodeHostState"),
                items: self
                    .job
                    .code_host_states
                    .iter()
                    .filter_map(|value| {
                        serde_json::from_value::<sg_db::perms_store::CodeHostState>(value.clone())
                            .ok()
                    })
                    .map(|state| {
                        FieldValue::Object(Arc::new(CodeHostStateObject {
                            state: CodeHostStateData {
                                provider_id: state.provider_id,
                                provider_type: state.provider_type,
                                status: state.status,
                                message: state.message,
                            },
                        }))
                    })
                    .collect(),
            },
            "partialSuccess" => FieldValue::Json(Json::Bool(self.job.is_partial_success)),
            "placeInQueue" => match self.job.place_in_queue {
                Some(place) => FieldValue::Json(Json::Int(i64::from(place))),
                None => FieldValue::Null,
            },
            _ => return Ok(None),
        }))
    }
}

struct PermissionsSyncJobsConnectionObject {
    data: PermissionsSyncJobsConnectionData,
}

impl ObjectValue for PermissionsSyncJobsConnectionObject {
    fn type_name(&self) -> &'static str {
        "PermissionsSyncJobsConnection"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        Ok(Some(match field {
            "nodes" => {
                let nodes = self
                    .data
                    .nodes
                    .as_ref()
                    .map_err(|message| ExecError::new(message.clone()))?;
                FieldValue::List {
                    element_type: Some("PermissionsSyncJob"),
                    items: nodes
                        .iter()
                        .map(|node| {
                            FieldValue::Object(Arc::new(PermissionsSyncJobObject {
                                job: node.job.clone(),
                                subject: node.subject.clone().expect(
                                    "subjects are resolved before the connection is built",
                                ),
                                triggered_by_user: node.triggered_by_user.clone(),
                            }))
                        })
                        .collect(),
                }
            }
            "totalCount" => FieldValue::Json(Json::Int(i64::from(self.data.total_count))),
            "pageInfo" => {
                let page_info = self
                    .data
                    .page_info
                    .as_ref()
                    .map_err(|message| ExecError::new(message.clone()))?;
                FieldValue::Object(Arc::new(ConnectionPageInfoObject::new(
                    page_info.start_cursor.clone(),
                    page_info.end_cursor.clone(),
                    page_info.has_next_page,
                    page_info.has_previous_page,
                )))
            }
            _ => return Ok(None),
        }))
    }
}

struct UserPermissionObject {
    data: UserPermissionData,
}

impl ObjectValue for UserPermissionObject {
    fn type_name(&self) -> &'static str {
        "UserPermission"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        Ok(Some(match field {
            "bindID" => FieldValue::Json(Json::Str(self.data.bind_id.clone())),
            "permission" => FieldValue::Json(Json::Str(self.data.permission.to_uppercase())),
            _ => return Ok(None),
        }))
    }
}

struct BitbucketProjectPermissionJobObject {
    data: BitbucketProjectPermissionJobData,
}

impl ObjectValue for BitbucketProjectPermissionJobObject {
    fn type_name(&self) -> &'static str {
        "BitbucketProjectPermissionJob"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        Ok(Some(match field {
            "InternalJobID" => FieldValue::Json(Json::Int(i64::from(self.data.id))),
            "State" => FieldValue::Json(Json::Str(self.data.state.clone())),
            "FailureMessage" => opt_str(&self.data.failure_message),
            "QueuedAt" => non_null_datetime(self.data.queued_at),
            "StartedAt" => datetime(self.data.started_at),
            "FinishedAt" => datetime(self.data.finished_at),
            "ProcessAfter" => datetime(self.data.process_after),
            "NumResets" => FieldValue::Json(Json::Int(i64::from(self.data.num_resets))),
            "NumFailures" => FieldValue::Json(Json::Int(i64::from(self.data.num_failures))),
            "ProjectKey" => FieldValue::Json(Json::Str(self.data.project_key.clone())),
            // `graphqlbackend.MarshalExternalServiceID`.
            "ExternalServiceID" => FieldValue::Json(Json::Str(
                base64::engine::general_purpose::URL_SAFE
                    .encode(format!("ExternalService:{}", self.data.external_service_id)),
            )),
            "Permissions" => FieldValue::List {
                element_type: Some("UserPermission"),
                items: self
                    .data
                    .permissions
                    .iter()
                    .cloned()
                    .map(|permission| {
                        FieldValue::Object(Arc::new(UserPermissionObject { data: permission }))
                    })
                    .collect(),
            },
            "Unrestricted" => FieldValue::Json(Json::Bool(self.data.unrestricted)),
            _ => return Ok(None),
        }))
    }
}

struct BitbucketProjectPermissionJobsObject {
    jobs: Vec<BitbucketProjectPermissionJobData>,
}

impl ObjectValue for BitbucketProjectPermissionJobsObject {
    fn type_name(&self) -> &'static str {
        "BitbucketProjectPermissionJobs"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        Ok(Some(match field {
            "totalCount" => FieldValue::Json(Json::Int(self.jobs.len() as i64)),
            "nodes" => FieldValue::List {
                element_type: Some("BitbucketProjectPermissionJob"),
                items: self
                    .jobs
                    .iter()
                    .cloned()
                    .map(|data| {
                        FieldValue::Object(Arc::new(BitbucketProjectPermissionJobObject {
                            data,
                        }))
                    })
                    .collect(),
            },
            _ => return Ok(None),
        }))
    }
}

struct PermissionsSyncingStatsObject {
    data: PermissionsSyncingStatsData,
}

impl ObjectValue for PermissionsSyncingStatsObject {
    fn type_name(&self) -> &'static str {
        "PermissionsSyncingStats"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        let value = match field {
            "queueSize" => self.data.queue_size,
            "usersWithLatestJobFailing" => self.data.users_with_latest_job_failing,
            "reposWithLatestJobFailing" => self.data.repos_with_latest_job_failing,
            "usersWithNoPermissions" => self.data.users_with_no_permissions,
            "reposWithNoPermissions" => self.data.repos_with_no_permissions,
            "usersWithStalePermissions" => self.data.users_with_stale_permissions,
            "reposWithStalePermissions" => self.data.repos_with_stale_permissions,
            _ => return Ok(None),
        };
        Ok(Some(FieldValue::Json(Json::Int(i64::from(value)))))
    }
}

struct PermissionsInfoObject {
    data: PermissionsInfoData,
    authz: AuthzHandle,
}

impl ObjectValue for PermissionsInfoObject {
    fn type_name(&self) -> &'static str {
        "PermissionsInfo"
    }

    fn field(
        &self,
        field: &str,
        arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        Ok(Some(match field {
            "permissions" => FieldValue::List {
                element_type: Some("RepositoryPermission"),
                items: vec![FieldValue::Json(Json::Str("READ".to_owned()))],
            },
            "syncedAt" => datetime(self.data.synced_at),
            "updatedAt" => datetime(self.data.updated_at),
            "source" => match &self.data.source {
                Some(source) if !source.is_empty() => {
                    FieldValue::Json(Json::Str(source.clone()))
                }
                _ => FieldValue::Null,
            },
            "unrestricted" => FieldValue::Json(Json::Bool(self.data.unrestricted)),
            "repositories" => {
                if self.data.user_id == 0 {
                    FieldValue::Null
                } else {
                    let args = info_connection_args(arguments);
                    let connection = self
                        .authz
                        .database()
                        .permissions_info_repositories(self.data.user_id, &args)
                        .map_err(ExecError::new)?;
                    FieldValue::Object(Arc::new(PermissionsInfoRepositoriesConnectionObject {
                        data: connection,
                        authz: self.authz.clone(),
                    }))
                }
            }
            "users" => {
                if self.data.repo_id == 0 {
                    FieldValue::Null
                } else {
                    let args = info_connection_args(arguments);
                    let connection = self
                        .authz
                        .database()
                        .permissions_info_users(self.data.repo_id, &args)
                        .map_err(ExecError::new)?;
                    FieldValue::Object(Arc::new(PermissionsInfoUsersConnectionObject {
                        data: connection,
                        authz: self.authz.clone(),
                    }))
                }
            }
            _ => return Ok(None),
        }))
    }
}

struct PermissionsInfoRepositoriesConnectionObject {
    data: PermissionsInfoRepositoriesData,
    authz: AuthzHandle,
}

impl ObjectValue for PermissionsInfoRepositoriesConnectionObject {
    fn type_name(&self) -> &'static str {
        "PermissionsInfoRepositoriesConnection"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        Ok(Some(match field {
            "nodes" => {
                let nodes = self
                    .data
                    .nodes
                    .as_ref()
                    .map_err(|message| ExecError::new(message.clone()))?;
                FieldValue::List {
                    element_type: Some("PermissionsInfoRepositoryNode"),
                    items: nodes
                        .iter()
                        .cloned()
                        .map(|node| {
                            FieldValue::Object(Arc::new(PermissionsInfoRepositoryNodeObject {
                                node,
                                authz: self.authz.clone(),
                            }))
                        })
                        .collect(),
                }
            }
            "totalCount" => FieldValue::Json(Json::Int(i64::from(self.data.total_count))),
            "pageInfo" => {
                let page_info = self
                    .data
                    .page_info
                    .as_ref()
                    .map_err(|message| ExecError::new(message.clone()))?;
                FieldValue::Object(Arc::new(ConnectionPageInfoObject::new(
                    page_info.start_cursor.clone(),
                    page_info.end_cursor.clone(),
                    page_info.has_next_page,
                    page_info.has_previous_page,
                )))
            }
            _ => return Ok(None),
        }))
    }
}

struct PermissionsInfoRepositoryNodeObject {
    node: PermissionsInfoRepositoryNodeData,
    authz: AuthzHandle,
}

impl ObjectValue for PermissionsInfoRepositoryNodeObject {
    fn type_name(&self) -> &'static str {
        "PermissionsInfoRepositoryNode"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        Ok(Some(match field {
            "id" => FieldValue::Json(Json::Str(
                base64::engine::general_purpose::URL_SAFE
                    .encode(format!("Repository:{}", self.node.repo.id)),
            )),
            "repository" => FieldValue::Object(Arc::new(
                crate::graphql_resolvers::RepositoryObject::from_data(self.node.repo.clone())
                    .with_authz(Some(self.authz.clone())),
            )),
            "reason" => FieldValue::Json(Json::Str(self.node.reason.clone())),
            "updatedAt" => datetime(self.node.updated_at),
            _ => return Ok(None),
        }))
    }
}

struct PermissionsInfoUsersConnectionObject {
    data: PermissionsInfoUsersData,
    authz: AuthzHandle,
}

impl ObjectValue for PermissionsInfoUsersConnectionObject {
    fn type_name(&self) -> &'static str {
        "PermissionsInfoUsersConnection"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        Ok(Some(match field {
            "nodes" => {
                let nodes = self
                    .data
                    .nodes
                    .as_ref()
                    .map_err(|message| ExecError::new(message.clone()))?;
                FieldValue::List {
                    element_type: Some("PermissionsInfoUserNode"),
                    items: nodes
                        .iter()
                        .cloned()
                        .map(|node| {
                            FieldValue::Object(Arc::new(PermissionsInfoUserNodeObject {
                                node,
                                authz: self.authz.clone(),
                            }))
                        })
                        .collect(),
                }
            }
            // `permissionsInfoUsersStore.ComputeTotal` is a TODO returning 0.
            "totalCount" => FieldValue::Json(Json::Int(0)),
            "pageInfo" => {
                let page_info = self
                    .data
                    .page_info
                    .as_ref()
                    .map_err(|message| ExecError::new(message.clone()))?;
                FieldValue::Object(Arc::new(ConnectionPageInfoObject::new(
                    page_info.start_cursor.clone(),
                    page_info.end_cursor.clone(),
                    page_info.has_next_page,
                    page_info.has_previous_page,
                )))
            }
            _ => return Ok(None),
        }))
    }
}

struct PermissionsInfoUserNodeObject {
    node: PermissionsInfoUserNodeData,
    authz: AuthzHandle,
}

impl ObjectValue for PermissionsInfoUserNodeObject {
    fn type_name(&self) -> &'static str {
        "PermissionsInfoUserNode"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        Ok(Some(match field {
            "id" => FieldValue::Json(Json::Str(
                base64::engine::general_purpose::URL_SAFE
                    .encode(format!("User:{}", self.node.user.id)),
            )),
            "user" => FieldValue::Object(Arc::new(
                crate::graphql_resolvers::UserObject::from_data(self.node.user.clone())
                    .with_authz(Some(self.authz.clone())),
            )),
            "reason" => FieldValue::Json(Json::Str(self.node.reason.clone())),
            "updatedAt" => datetime(self.node.updated_at),
            _ => return Ok(None),
        }))
    }
}

/// `authorizedUserRepositories` returns `RepositoryConnection` (the legacy
/// `gqlutil.PageInfo` connection, not `NewRepositoryConnection`).
struct AuthorizedRepositoriesObject {
    data: AuthorizedRepositoriesData,
    authz: AuthzHandle,
}

impl ObjectValue for AuthorizedRepositoriesObject {
    fn type_name(&self) -> &'static str {
        "RepositoryConnection"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        Ok(Some(match field {
            "nodes" => FieldValue::List {
                element_type: Some("Repository"),
                items: self
                    .data
                    .repos
                    .iter()
                    .cloned()
                    .map(|repo| {
                        FieldValue::Object(Arc::new(
                            crate::graphql_resolvers::RepositoryObject::from_data(repo)
                                .with_authz(Some(self.authz.clone())),
                        ))
                    })
                    .collect(),
            },
            "totalCount" => FieldValue::Json(Json::Int(i64::from(self.data.total_count))),
            "pageInfo" => FieldValue::Object(Arc::new(PageInfoObject::new(
                self.data.has_next_page,
                self.data.end_cursor.clone(),
            ))),
            _ => return Ok(None),
        }))
    }
}

/// `Repository.authorizedUsers` returns `UserConnection`.
struct AuthorizedUsersObject {
    data: AuthorizedUsersData,
    authz: AuthzHandle,
}

impl ObjectValue for AuthorizedUsersObject {
    fn type_name(&self) -> &'static str {
        "UserConnection"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        Ok(Some(match field {
            "nodes" => FieldValue::List {
                element_type: Some("User"),
                items: self
                    .data
                    .users
                    .iter()
                    .cloned()
                    .map(|user| {
                        FieldValue::Object(Arc::new(
                            crate::graphql_resolvers::UserObject::from_data(user)
                                .with_authz(Some(self.authz.clone())),
                        ))
                    })
                    .collect(),
            },
            "totalCount" => FieldValue::Json(Json::Int(i64::from(self.data.total_count))),
            "pageInfo" => FieldValue::Object(Arc::new(PageInfoObject::new(
                self.data.has_next_page,
                self.data.end_cursor.clone(),
            ))),
            _ => return Ok(None),
        }))
    }
}

// ---------------------------------------------------------------------------
// Argument parsing
// ---------------------------------------------------------------------------

fn info_connection_args(arguments: &Arguments) -> InfoConnectionArgs {
    InfoConnectionArgs {
        first: arguments.i64("first").map(|value| value as i32),
        last: arguments.i64("last").map(|value| value as i32),
        after: arguments.string("after").map(str::to_owned),
        before: arguments.string("before").map(str::to_owned),
        query: arguments.string("query").map(str::to_owned),
    }
}

fn input_strings(value: Option<&InputValue>) -> Option<Vec<String>> {
    match value {
        None | Some(InputValue::Null) => None,
        Some(InputValue::List(items)) => Some(
            items
                .iter()
                .filter_map(|item| item.as_str().map(str::to_owned))
                .collect(),
        ),
        Some(other) => other.as_str().map(|value| vec![value.to_owned()]),
    }
}

fn user_permission_inputs(arguments: &Arguments) -> Vec<UserPermissionInput> {
    let Some(InputValue::List(items)) = arguments.get("userPermissions") else {
        return Vec::new();
    };
    items
        .iter()
        .filter_map(|item| match item {
            InputValue::Object(fields) => {
                let bind_id = fields.get("bindID").and_then(InputValue::as_str)?;
                let permission = fields
                    .get("permission")
                    .and_then(InputValue::as_str)
                    .unwrap_or("READ");
                Some(UserPermissionInput {
                    bind_id: bind_id.to_owned(),
                    permission: permission.to_owned(),
                })
            }
            _ => None,
        })
        .collect()
}

fn sub_repo_permission_inputs(arguments: &Arguments) -> Vec<SubRepoPermissionInput> {
    let Some(InputValue::List(items)) = arguments.get("userPermissions") else {
        return Vec::new();
    };
    items
        .iter()
        .filter_map(|item| match item {
            InputValue::Object(fields) => {
                let bind_id = fields.get("bindID").and_then(InputValue::as_str)?;
                Some(SubRepoPermissionInput {
                    bind_id: bind_id.to_owned(),
                    path_includes: input_strings(fields.get("pathIncludes")),
                    path_excludes: input_strings(fields.get("pathExcludes")),
                    paths: input_strings(fields.get("paths")),
                })
            }
            _ => None,
        })
        .collect()
}

fn permissions_sync_jobs_args(arguments: &Arguments) -> PermissionsSyncJobsArgs {
    PermissionsSyncJobsArgs {
        first: arguments.i64("first"),
        last: arguments.i64("last"),
        after: arguments.string("after").map(str::to_owned),
        before: arguments.string("before").map(str::to_owned),
        reason_group: arguments.string("reasonGroup").map(str::to_owned),
        state: arguments.string("state").map(str::to_owned),
        search_type: arguments.string("searchType").map(str::to_owned),
        query: arguments.string("query").map(str::to_owned),
        // Invalid relay IDs are silently ignored (`getListArgs`).
        user_id: arguments
            .string("userID")
            .and_then(|raw| crate::graphql_resolvers::unmarshal_relay_id(raw).ok()),
        repo_id: arguments
            .string("repoID")
            .and_then(|raw| crate::graphql_resolvers::unmarshal_relay_id(raw).ok()),
        partial: arguments.bool("partial"),
    }
}

fn bitbucket_jobs_args(arguments: &Arguments) -> BitbucketProjectPermissionJobsArgs {
    BitbucketProjectPermissionJobsArgs {
        project_keys: input_strings(arguments.get("projectKeys")),
        status: arguments.string("status").map(str::to_owned),
        count: arguments.i64("count").map(|value| value as i32),
    }
}

// ---------------------------------------------------------------------------
// Authorization gates
// ---------------------------------------------------------------------------

fn check_site_admin(actor: Actor, viewer_site_admin: bool) -> Result<(), String> {
    crate::admin_store::check_current_user_is_site_admin(actor, viewer_site_admin)
}

fn check_site_admin_or_same_user(
    actor: Actor,
    viewer_site_admin: bool,
    subject_user_id: i32,
) -> Result<(), String> {
    crate::admin_store::check_site_admin_or_same_user(actor, viewer_site_admin, subject_user_id)
}

fn dotcom_gate(database: &dyn AuthzDatabase) -> Result<(), String> {
    if database.dotcom() {
        Err(DISABLED_DOTCOM.to_owned())
    } else {
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Query roots
// ---------------------------------------------------------------------------

pub fn is_root_field(name: &str) -> bool {
    matches!(
        name,
        "authorizedUserRepositories"
            | "usersWithPendingPermissions"
            | "permissionsSyncJobs"
            | "permissionsSyncingStats"
            | "bitbucketProjectPermissionJobs"
    )
}

/// Dispatch `Query.<authz field>`.
pub fn root_field(
    authz: &AuthzHandle,
    field: &str,
    arguments: &Arguments,
    actor: Actor,
    viewer_site_admin: bool,
) -> Result<Option<FieldValue>, ExecError> {
    let database = authz.database();
    match field {
        "permissionsSyncJobs" => {
            let args = permissions_sync_jobs_args(arguments);
            // `Resolver.PermissionsSyncJobs` (`resolver.go:638-654`).
            if let Some(user_id) = args.user_id {
                check_site_admin_or_same_user(actor, viewer_site_admin, user_id)
                    .map_err(ExecError::new)?;
            } else {
                check_site_admin(actor, viewer_site_admin).map_err(ExecError::new)?;
            }
            if args.user_id.is_some() && args.repo_id.is_some() {
                return Err(ExecError::new(
                    "please provide either userID or repoID, but not both.",
                ));
            }
            // `NewPermissionsSyncJobsResolver` also rejects the pair; the
            // baseline checks it before constructing the connection.
            let connection = database
                .permissions_sync_jobs(&args)
                .map_err(ExecError::new)?;
            Ok(Some(FieldValue::Object(Arc::new(
                PermissionsSyncJobsConnectionObject { data: connection },
            ))))
        }
        "permissionsSyncingStats" => {
            check_site_admin(actor, viewer_site_admin).map_err(ExecError::new)?;
            let stats = database
                .permissions_syncing_stats()
                .map_err(ExecError::new)?;
            Ok(Some(FieldValue::Object(Arc::new(
                PermissionsSyncingStatsObject { data: stats },
            ))))
        }
        "bitbucketProjectPermissionJobs" => {
            dotcom_gate(database).map_err(ExecError::new)?;
            check_site_admin(actor, viewer_site_admin).map_err(ExecError::new)?;
            let mut args = bitbucket_jobs_args(arguments);
            // `strings.ToLower(strings.TrimSpace(getOrDefault(args.Status)))`.
            let status = args
                .status
                .as_deref()
                .unwrap_or("")
                .trim()
                .to_lowercase();
            if !status.is_empty() && !is_job_status(&status) {
                return Err(ExecError::new(
                    "Please provide one of the following job statuses: queued, processing, \
                     completed, canceled, errored, failed",
                ));
            }
            args.status = Some(status);
            let jobs = database
                .bitbucket_project_permission_jobs(&args)
                .map_err(ExecError::new)?;
            Ok(Some(FieldValue::Object(Arc::new(
                BitbucketProjectPermissionJobsObject { jobs },
            ))))
        }
        "usersWithPendingPermissions" => {
            check_site_admin(actor, viewer_site_admin).map_err(ExecError::new)?;
            let users = database
                .users_with_pending_permissions()
                .map_err(ExecError::new)?;
            Ok(Some(FieldValue::List {
                element_type: Some("String"),
                items: users
                    .into_iter()
                    .map(|bind_id| FieldValue::Json(Json::Str(bind_id)))
                    .collect(),
            }))
        }
        "authorizedUserRepositories" => {
            dotcom_gate(database).map_err(ExecError::new)?;
            check_site_admin(actor, viewer_site_admin).map_err(ExecError::new)?;
            let username = arguments.string("username");
            let email = arguments.string("email");
            let (Some(username), Some(email)) = (username, email) else {
                // Exactly one may be given; `LoadUserPermissions` is used when
                // the identity resolves to a user, otherwise the pending path.
                if username.is_none() && email.is_none() {
                    return Err(ExecError::new(
                        "neither email nor username is given to identify a user",
                    ));
                }
                let args = AuthorizedUserRepositoriesArgs {
                    username: username.map(str::to_owned),
                    email: email.map(str::to_owned),
                    first: arguments.i64("first").unwrap_or(0) as i32,
                    after: arguments.string("after").map(str::to_owned),
                };
                let data = database
                    .authorized_user_repositories(&args)
                    .map_err(ExecError::new)?;
                return Ok(Some(FieldValue::Object(Arc::new(AuthorizedRepositoriesObject {
                    data,
                    authz: authz.clone(),
                }))));
            };
            // Both provided: `AuthorizedUserRepositories` prefers `email`.
            let args = AuthorizedUserRepositoriesArgs {
                username: Some(username.to_owned()),
                email: Some(email.to_owned()),
                first: arguments.i64("first").unwrap_or(0) as i32,
                after: arguments.string("after").map(str::to_owned),
            };
            let data = database
                .authorized_user_repositories(&args)
                .map_err(ExecError::new)?;
            Ok(Some(FieldValue::Object(Arc::new(AuthorizedRepositoriesObject {
                data,
                authz: authz.clone(),
            }))))
        }
        _ => Ok(None),
    }
}

fn is_job_status(status: &str) -> bool {
    matches!(
        status,
        "queued" | "processing" | "completed" | "canceled" | "errored" | "failed"
    )
}

// ---------------------------------------------------------------------------
// Nested Repository/User fields
// ---------------------------------------------------------------------------

/// `Repository.authorizedUsers` / `Repository.permissionsInfo`.
pub fn is_repository_field(name: &str) -> bool {
    matches!(name, "authorizedUsers" | "permissionsInfo")
}

pub fn repository_field(
    authz: &AuthzHandle,
    repo_id: i32,
    field: &str,
    arguments: &Arguments,
    actor: Actor,
    viewer_site_admin: bool,
) -> Result<Option<FieldValue>, ExecError> {
    let database = authz.database();
    match field {
        "authorizedUsers" => {
            check_site_admin(actor, viewer_site_admin).map_err(ExecError::new)?;
            let args = AuthorizedUsersArgs {
                first: arguments.i64("first").unwrap_or(0) as i32,
                after: arguments.string("after").map(str::to_owned),
            };
            let data = database
                .authorized_users(repo_id, &args)
                .map_err(ExecError::new)?;
            Ok(Some(FieldValue::Object(Arc::new(AuthorizedUsersObject {
                data,
                authz: authz.clone(),
            }))))
        }
        "permissionsInfo" => {
            check_site_admin(actor, viewer_site_admin).map_err(ExecError::new)?;
            let info = database
                .repository_permissions_info(repo_id)
                .map_err(ExecError::new)?;
            Ok(Some(FieldValue::Object(Arc::new(PermissionsInfoObject {
                data: info,
                authz: authz.clone(),
            }))))
        }
        _ => Ok(None),
    }
}

/// `User.permissionsInfo`.
pub fn is_user_field(name: &str) -> bool {
    name == "permissionsInfo"
}

pub fn user_field(
    authz: &AuthzHandle,
    user_id: i32,
    field: &str,
    actor: Actor,
    viewer_site_admin: bool,
) -> Result<Option<FieldValue>, ExecError> {
    let database = authz.database();
    match field {
        "permissionsInfo" => {
            check_site_admin_or_same_user(actor, viewer_site_admin, user_id)
                .map_err(ExecError::new)?;
            let info = database
                .user_permissions_info(user_id)
                .map_err(ExecError::new)?;
            Ok(Some(FieldValue::Object(Arc::new(PermissionsInfoObject {
                data: info,
                authz: authz.clone(),
            }))))
        }
        _ => Ok(None),
    }
}

// ---------------------------------------------------------------------------
// Mutations
// ---------------------------------------------------------------------------

pub fn is_mutation_field(name: &str) -> bool {
    matches!(
        name,
        "setRepositoryPermissionsForUsers"
            | "setRepositoryPermissionsUnrestricted"
            | "scheduleRepositoryPermissionsSync"
            | "scheduleUserPermissionsSync"
            | "setSubRepositoryPermissionsForUsers"
            | "setRepositoryPermissionsForBitbucketProject"
            | "cancelPermissionsSyncJob"
    )
}

fn empty_response() -> FieldValue {
    FieldValue::Object(Arc::new(
        crate::temporary_settings::EmptyResponseObject,
    ))
}

/// Dispatch `Mutation.<authz field>`.
pub fn mutation_field(
    authz: &AuthzHandle,
    field: &str,
    arguments: &Arguments,
    actor: Actor,
    viewer_site_admin: bool,
) -> Result<Option<FieldValue>, ExecError> {
    let database = authz.database();
    let now = OffsetDateTime::now_utc();
    let actor_uid = actor.uid().unwrap_or(0);
    match field {
        "setRepositoryPermissionsForUsers" => {
            dotcom_gate(database).map_err(ExecError::new)?;
            database
                .check_license_explicit_permissions()
                .map_err(ExecError::new)?;
            check_site_admin(actor, viewer_site_admin).map_err(ExecError::new)?;
            let raw = arguments
                .string("repository")
                .ok_or_else(|| ExecError::new("invalid graphql.ID"))?;
            let repo_id = crate::graphql_resolvers::unmarshal_relay_id(raw)
                .map_err(ExecError::new)?;
            let permissions = user_permission_inputs(arguments);
            database
                .set_repository_permissions_for_users(repo_id, &permissions, now)
                .map_err(ExecError::new)?;
            Ok(Some(empty_response()))
        }
        "setRepositoryPermissionsUnrestricted" => {
            dotcom_gate(database).map_err(ExecError::new)?;
            database
                .check_license_explicit_permissions()
                .map_err(ExecError::new)?;
            check_site_admin(actor, viewer_site_admin).map_err(ExecError::new)?;
            let Some(InputValue::List(items)) = arguments.get("repositories") else {
                return Ok(Some(empty_response()));
            };
            let mut ids = Vec::with_capacity(items.len());
            for item in items {
                let raw = item
                    .as_str()
                    .ok_or_else(|| ExecError::new("invalid graphql.ID"))?;
                let id = crate::graphql_resolvers::unmarshal_relay_id(raw)
                    .map_err(|error| ExecError::new(format!("unmarshalling id: {error}")))?;
                ids.push(id);
            }
            let unrestricted = arguments.bool("unrestricted").unwrap_or(false);
            database
                .set_repository_permissions_unrestricted(&ids, unrestricted, now)
                .map_err(ExecError::new)?;
            Ok(Some(empty_response()))
        }
        "scheduleRepositoryPermissionsSync" => {
            database
                .check_license_acls()
                .map_err(ExecError::new)?;
            check_site_admin(actor, viewer_site_admin).map_err(ExecError::new)?;
            let raw = arguments
                .string("repository")
                .ok_or_else(|| ExecError::new("invalid graphql.ID"))?;
            let repo_id = crate::graphql_resolvers::unmarshal_relay_id(raw)
                .map_err(ExecError::new)?;
            database
                .schedule_repository_permissions_sync(repo_id, actor_uid, now)
                .map_err(ExecError::new)?;
            Ok(Some(empty_response()))
        }
        "scheduleUserPermissionsSync" => {
            database
                .check_license_acls()
                .map_err(ExecError::new)?;
            let raw = arguments
                .string("user")
                .ok_or_else(|| ExecError::new("invalid graphql.ID"))?;
            let user_id = crate::graphql_resolvers::unmarshal_relay_id(raw)
                .map_err(ExecError::new)?;
            check_site_admin_or_same_user(actor, viewer_site_admin, user_id)
                .map_err(ExecError::new)?;
            let invalidate_caches = match arguments.get("options") {
                Some(InputValue::Object(options)) => {
                    options.get("invalidateCaches").and_then(InputValue::as_bool)
                        == Some(true)
                }
                _ => false,
            };
            database
                .schedule_user_permissions_sync(user_id, invalidate_caches, actor_uid, now)
                .map_err(ExecError::new)?;
            Ok(Some(empty_response()))
        }
        "setSubRepositoryPermissionsForUsers" => {
            database
                .check_license_explicit_permissions()
                .map_err(ExecError::new)?;
            dotcom_gate(database).map_err(ExecError::new)?;
            check_site_admin(actor, viewer_site_admin).map_err(ExecError::new)?;
            let raw = arguments
                .string("repository")
                .ok_or_else(|| ExecError::new("invalid graphql.ID"))?;
            let repo_id = crate::graphql_resolvers::unmarshal_relay_id(raw)
                .map_err(ExecError::new)?;
            let permissions = sub_repo_permission_inputs(arguments);
            database
                .set_sub_repository_permissions_for_users(repo_id, &permissions, now)
                .map_err(ExecError::new)?;
            Ok(Some(empty_response()))
        }
        "setRepositoryPermissionsForBitbucketProject" => {
            dotcom_gate(database).map_err(ExecError::new)?;
            database
                .check_license_explicit_permissions()
                .map_err(ExecError::new)?;
            check_site_admin(actor, viewer_site_admin).map_err(ExecError::new)?;
            let project_key = arguments
                .string("projectKey")
                .ok_or_else(|| ExecError::new("missing projectKey"))?;
            let raw = arguments
                .string("codeHost")
                .ok_or_else(|| ExecError::new("invalid graphql.ID"))?;
            // `UnmarshalExternalServiceID` validates the relay kind.
            let kind = crate::graphql_resolvers::relay_kind(raw).unwrap_or_default();
            if kind != "ExternalService" {
                return Err(ExecError::new(format!(
                    "expected graphql ID to have kind \"ExternalService\"; got \"{kind}\""
                )));
            }
            let external_service_id =
                unmarshal_external_service_id(raw).map_err(ExecError::new)?;
            let unrestricted = arguments.bool("unrestricted").unwrap_or(false);
            let permissions = user_permission_inputs(arguments);
            database
                .set_repository_permissions_for_bitbucket_project(
                    project_key,
                    external_service_id,
                    &permissions,
                    unrestricted,
                    now,
                )
                .map_err(ExecError::new)?;
            Ok(Some(empty_response()))
        }
        "cancelPermissionsSyncJob" => {
            database
                .check_license_acls()
                .map_err(ExecError::new)?;
            check_site_admin(actor, viewer_site_admin).map_err(ExecError::new)?;
            let raw = arguments
                .string("job")
                .ok_or_else(|| ExecError::new("invalid graphql.ID"))?;
            let id = unmarshal_permissions_sync_job_id(raw).map_err(ExecError::new)?;
            let reason = arguments.string("reason").unwrap_or("");
            let result = database
                .cancel_permissions_sync_job(id, reason, now)
                .map_err(ExecError::new)?;
            Ok(Some(FieldValue::Json(Json::Str(
                result.as_str().to_owned(),
            ))))
        }
        _ => Ok(None),
    }
}

/// `graphqlbackend.UnmarshalExternalServiceID` spec (int64).
fn unmarshal_external_service_id(raw: &str) -> Result<i64, String> {
    let decoded = base64::engine::general_purpose::URL_SAFE
        .decode(raw)
        .map_err(|error| error.to_string())?;
    let text = String::from_utf8_lossy(&decoded);
    let (_, spec) = text.split_once(':').ok_or("invalid graphql.ID")?;
    serde_json::from_str(spec).map_err(|error| error.to_string())
}
