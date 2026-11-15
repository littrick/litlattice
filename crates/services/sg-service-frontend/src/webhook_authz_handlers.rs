#![forbid(unsafe_code)]

//! M9.44b: GitHub webhook authz side-effect handlers.
//!
//! Ports `cmd/frontend/internal/httpapi/webhookhandlers/`
//! (`handlers.go`, `handle_repo_authz_event.go`,
//! `handle_user_authz_event.go`) plus the queue half of
//! `internal/authz/permssync.SchedulePermsSync` (`CreateRepoSyncJob` /
//! `CreateUserSyncJob`). Reuses the existing `repo`, `user_external_accounts`
//! and `permission_sync_jobs` tables — no new table/column/Redis key.
//!
//! Errata (source wins):
//!
//! * `webhookhandlers.Init` calls `Register(handler, "<event>")`, but
//!   `Router.Register`'s second positional argument is **`codeHostKind`**.
//!   The event names land as bogus code-host keys with no event types, so
//!   `Dispatch("GITHUB", <event>, …)` never reaches them. The side effects
//!   are therefore inert on the HTTP path; [`dispatch_github`] reproduces
//!   that, while [`run_handler`] / [`dispatch_intended`] exercise the handler
//!   bodies directly (the oracle does the same, calling the unexported
//!   handler functions).
//! * `"organisation"` is registered for the user handler but go-github v55's
//!   `ParseWebHook` only knows `"organization"`; the former fails to parse
//!   (HTTP 400) and the latter has no handler under `webhookhandlers`, so the
//!   user handler's `membershipGetter` branch is unreachable through HTTP.
//! * `errgroup` runs the handlers for one event concurrently; the returned
//!   error is whichever finishes first. The corpus avoids triggering two
//!   errors for one event so the differential stays deterministic.

use serde::Deserialize;
use sqlx::postgres::PgPool;
use sqlx::types::time::OffsetDateTime;
use sqlx::Row;

/// `database.ReasonGitHubRepoEvent`.
pub const REASON_GITHUB_REPO_EVENT: &str = "REASON_GITHUB_REPO_EVENT";
/// `database.ReasonGitHubUserEvent`.
pub const REASON_GITHUB_USER_EVENT: &str = "REASON_GITHUB_USER_EVENT";
/// `database.HighPriorityPermissionsSync`.
pub const HIGH_PRIORITY_PERMISSIONS_SYNC: i32 = 10;

/// `authz.FetchPermsOptions` (only the field the handlers read).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct FetchPermsOptions {
    pub invalidate_caches: bool,
}

// ---------------------------------------------------------------------------
// go-github payload shapes (only the fields the handlers read)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct GhRepo {
    pub full_name: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct GhUser {
    pub id: Option<i64>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct GhMembership {
    pub user: Option<GhUser>,
}

/// The concrete go-github event types the handlers can receive, reduced to the
/// fields they read. The variant determines the type assertions
/// (`repoGetter` / `memberGetter` / `membershipGetter`) and the `%T` text.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GithubAuthzEvent {
    Public { repo: Option<GhRepo> },
    Repository { repo: Option<GhRepo> },
    Member { repo: Option<GhRepo>, member: Option<GhUser> },
    TeamAdd { repo: Option<GhRepo> },
    Organization { membership: Option<GhMembership> },
    Membership { member: Option<GhUser> },
}

impl GithubAuthzEvent {
    /// `fmt.Sprintf("%T", payload)` for the go-github event.
    pub fn go_type_name(&self) -> &'static str {
        match self {
            Self::Public { .. } => "*github.PublicEvent",
            Self::Repository { .. } => "*github.RepositoryEvent",
            Self::Member { .. } => "*github.MemberEvent",
            Self::TeamAdd { .. } => "*github.TeamAddEvent",
            Self::Organization { .. } => "*github.OrganizationEvent",
            Self::Membership { .. } => "*github.MembershipEvent",
        }
    }

    /// `payload.(repoGetter)`: `Some(repo)` when the type implements
    /// `GetRepo() *gh.Repository` (the repo may itself be nil).
    pub fn repo_getter(&self) -> Option<Option<GhRepo>> {
        match self {
            Self::Public { repo }
            | Self::Repository { repo }
            | Self::Member { repo, .. }
            | Self::TeamAdd { repo } => Some(repo.clone()),
            Self::Organization { .. } | Self::Membership { .. } => None,
        }
    }

    /// The `memberGetter` / `membershipGetter` type switch in
    /// `handleGitHubUserAuthzEvent`. `None` means neither interface matches
    /// (e.g. `*github.PublicEvent`), which the handler reports as an error.
    pub fn user_getter(&self) -> Option<Option<GhUser>> {
        match self {
            Self::Member { member, .. } | Self::Membership { member } => Some(member.clone()),
            Self::Organization { membership } => {
                Some(membership.as_ref().and_then(|m| m.user.clone()))
            }
            Self::Public { .. } | Self::Repository { .. } | Self::TeamAdd { .. } => None,
        }
    }
}

/// `github.ParseWebHook` failure kinds.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ParseEventError {
    /// `unknown X-Github-Event in message: <type>` (HTTP 400).
    UnknownType(String),
    /// `encoding/json` decode error (HTTP 400).
    Json(String),
}

impl ParseEventError {
    pub fn message(&self) -> String {
        match self {
            Self::UnknownType(event) => format!("unknown X-Github-Event in message: {event}"),
            Self::Json(message) => message.clone(),
        }
    }
}

#[derive(Deserialize, Default)]
struct RawRepo {
    #[serde(default)]
    full_name: Option<String>,
}

#[derive(Deserialize, Default)]
struct RawUser {
    #[serde(default)]
    id: Option<i64>,
}

#[derive(Deserialize, Default)]
struct RawMembership {
    #[serde(default)]
    user: Option<RawUser>,
}

/// One shape covers every event type; go-github's concrete structs likewise
/// ignore unknown fields.
#[derive(Deserialize, Default)]
struct RawEvent {
    #[serde(default)]
    repository: Option<RawRepo>,
    #[serde(default)]
    member: Option<RawUser>,
    #[serde(default)]
    membership: Option<RawMembership>,
}

fn raw_user(user: Option<RawUser>) -> Option<GhUser> {
    user.map(|user| GhUser { id: user.id })
}

/// `github.ParseWebHook(eventType, payload)` narrowed to the authz handlers'
/// event types.
pub fn parse_event(event_type: &str, payload: &[u8]) -> Result<GithubAuthzEvent, ParseEventError> {
    match event_type {
        "public" | "repository" | "member" | "team_add" | "organization" | "membership" => {}
        other => return Err(ParseEventError::UnknownType(other.to_owned())),
    }
    let raw: RawEvent = serde_json::from_slice(payload).map_err(|error| {
        if payload.is_empty() {
            ParseEventError::Json("unexpected end of JSON input".to_owned())
        } else {
            ParseEventError::Json(error.to_string())
        }
    })?;
    let repo = raw
        .repository
        .map(|repository| GhRepo { full_name: repository.full_name });
    let member = raw_user(raw.member);
    let membership = raw.membership.map(|membership| GhMembership {
        user: raw_user(membership.user),
    });
    Ok(match event_type {
        "public" => GithubAuthzEvent::Public { repo },
        "repository" => GithubAuthzEvent::Repository { repo },
        "member" => GithubAuthzEvent::Member { repo, member },
        "team_add" => GithubAuthzEvent::TeamAdd { repo },
        "organization" => GithubAuthzEvent::Organization { membership },
        "membership" => GithubAuthzEvent::Membership { member },
        _ => unreachable!("event type checked above"),
    })
}

// ---------------------------------------------------------------------------
// Handlers and registration
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuthzHandler {
    Repo { opts: FetchPermsOptions },
    User { opts: FetchPermsOptions },
}

/// The event → handler mapping `webhookhandlers.Init` *intends*, in
/// registration order (a single event may carry two handlers).
pub fn intended_registrations() -> Vec<(&'static str, AuthzHandler)> {
    let none = FetchPermsOptions::default();
    let invalidate = FetchPermsOptions {
        invalidate_caches: true,
    };
    vec![
        ("public", AuthzHandler::Repo { opts: none }),
        ("repository", AuthzHandler::Repo { opts: none }),
        ("member", AuthzHandler::Repo { opts: none }),
        ("member", AuthzHandler::User { opts: none }),
        ("team_add", AuthzHandler::Repo { opts: invalidate }),
        ("organisation", AuthzHandler::User { opts: invalidate }),
        ("membership", AuthzHandler::User { opts: invalidate }),
    ]
}

/// The handlers the event type would run if the registrations had used
/// `Register(handler, "GITHUB", eventType)`.
pub fn intended_handlers(event_type: &str) -> Vec<AuthzHandler> {
    intended_registrations()
        .into_iter()
        .filter(|(event, _)| *event == event_type)
        .map(|(_, handler)| handler)
        .collect()
}

/// `Router.Dispatch(ctx, eventType, "GITHUB", …)` under the actual
/// `webhookhandlers.Init` layout: the handlers were registered with the event
/// name as `codeHostKind`, so a `"GITHUB"` lookup never matches. Always a
/// no-op (empty `Ok`).
pub fn dispatch_github(
    event_type: &str,
    _event: &GithubAuthzEvent,
    _deps: &dyn AuthzDeps,
) -> Result<(), AuthzError> {
    let _ = event_type;
    Ok(())
}

// ---------------------------------------------------------------------------
// Seams
// ---------------------------------------------------------------------------

/// The database reads and queue writes the handlers perform.
pub trait AuthzDeps: Send + Sync {
    /// `db.Repos().GetByName`: `Ok(None)` is `RepoNotFoundErr`.
    fn get_repo_by_name(&self, name: &str) -> Result<Option<i32>, String>;
    /// `db.UserExternalAccounts().List(ServiceType:"github", AccountID:…)`
    /// returning `acc.UserID` in `id ASC` order.
    fn list_github_account_user_ids(&self, account_id: &str) -> Result<Vec<i32>, String>;
    /// `permssync.SchedulePermsSync` repo half. Enqueue failures are logged
    /// and swallowed, mirroring the baseline.
    fn enqueue_repo_perms_sync(&self, repo_id: i32, opts: FetchPermsOptions, reason: &str);
    /// `permssync.SchedulePermsSync` user half.
    fn enqueue_user_perms_sync(&self, user_ids: &[i32], opts: FetchPermsOptions, reason: &str);
}

/// `Dispatch`'s error surface: `errcode.IsNotFound(err)` decides 404 vs 500.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuthzError {
    NotFound(String),
    Handler(String),
}

impl AuthzError {
    pub fn message(&self) -> &str {
        match self {
            Self::NotFound(message) | Self::Handler(message) => message,
        }
    }

    pub fn is_not_found(&self) -> bool {
        matches!(self, Self::NotFound(_))
    }
}

/// Run one handler body (`handleGitHubRepoAuthzEvent` /
/// `handleGitHubUserAuthzEvent`).
pub fn run_handler(
    handler: AuthzHandler,
    event: &GithubAuthzEvent,
    deps: &dyn AuthzDeps,
) -> Result<(), AuthzError> {
    match handler {
        AuthzHandler::Repo { opts } => handle_repo_event(event, opts, deps),
        AuthzHandler::User { opts } => handle_user_event(event, opts, deps),
    }
}

/// Run every handler the event type maps to (the intended registration),
/// returning the first error but still applying all side effects. Mirrors
/// `Router.Dispatch` + `errgroup` minus its completion-order nondeterminism.
pub fn dispatch_intended(
    event_type: &str,
    event: &GithubAuthzEvent,
    deps: &dyn AuthzDeps,
) -> Result<(), AuthzError> {
    let mut first_error = None;
    for handler in intended_handlers(event_type) {
        if let Err(error) = run_handler(handler, event, deps) {
            if first_error.is_none() {
                first_error = Some(error);
            }
        }
    }
    match first_error {
        Some(error) => Err(error),
        None => Ok(()),
    }
}

fn handle_repo_event(
    event: &GithubAuthzEvent,
    opts: FetchPermsOptions,
    deps: &dyn AuthzDeps,
) -> Result<(), AuthzError> {
    let repo = match event.repo_getter() {
        Some(repo) => repo,
        None => {
            return Err(AuthzError::Handler(format!(
                "incorrect event type sent to github event handler: {}",
                event.go_type_name()
            )))
        }
    };
    let Some(repo) = repo else {
        return Ok(());
    };

    let name = format!("github.com/{}", repo.full_name.as_deref().unwrap_or(""));
    match deps.get_repo_by_name(&name) {
        Ok(Some(repo_id)) => {
            deps.enqueue_repo_perms_sync(repo_id, opts, REASON_GITHUB_REPO_EVENT);
            Ok(())
        }
        Ok(None) => Err(AuthzError::NotFound(format!(
            "repo not found: name=\"{name}\""
        ))),
        Err(error) => Err(AuthzError::Handler(error)),
    }
}

fn handle_user_event(
    event: &GithubAuthzEvent,
    opts: FetchPermsOptions,
    deps: &dyn AuthzDeps,
) -> Result<(), AuthzError> {
    let user = match event.user_getter() {
        Some(user) => user,
        None => return Err(extract_user_error(event)),
    };
    let Some(user) = user else {
        return Err(extract_user_error(event));
    };

    let account_id = user.id.unwrap_or(0).to_string();
    match deps.list_github_account_user_ids(&account_id) {
        Ok(user_ids) if user_ids.is_empty() => Ok(()),
        Ok(user_ids) => {
            deps.enqueue_user_perms_sync(&user_ids, opts, REASON_GITHUB_USER_EVENT);
            Ok(())
        }
        Err(error) => Err(AuthzError::Handler(error)),
    }
}

fn extract_user_error(event: &GithubAuthzEvent) -> AuthzError {
    AuthzError::Handler(format!(
        "could not extract GitHub user from {} GitHub event",
        event.go_type_name()
    ))
}

// ---------------------------------------------------------------------------
// Postgres-backed deps
// ---------------------------------------------------------------------------

fn block_on<F: std::future::Future>(future: F) -> F::Output {
    match tokio::runtime::Handle::try_current() {
        Ok(handle) => tokio::task::block_in_place(|| handle.block_on(future)),
        Err(_) => tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("webhook authz blocking runtime")
            .block_on(future),
    }
}

/// `repo`, `user_external_accounts` and `permission_sync_jobs` backed by
/// Postgres.
pub struct PgAuthzDeps {
    pool: PgPool,
}

impl PgAuthzDeps {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

impl AuthzDeps for PgAuthzDeps {
    fn get_repo_by_name(&self, name: &str) -> Result<Option<i32>, String> {
        let row = block_on(
            sqlx::query(
                "SELECT id FROM repo WHERE name = $1 AND deleted_at IS NULL \
                 ORDER BY id ASC LIMIT 1",
            )
            .bind(name)
            .fetch_optional(&self.pool),
        )
        .map_err(|error| error.to_string())?;
        match row {
            Some(row) => row
                .try_get::<i32, _>("id")
                .map(Some)
                .map_err(|error| error.to_string()),
            None => Ok(None),
        }
    }

    fn list_github_account_user_ids(&self, account_id: &str) -> Result<Vec<i32>, String> {
        let rows = block_on(
            sqlx::query(
                "SELECT user_id FROM user_external_accounts \
                 WHERE deleted_at IS NULL AND service_type = 'github' AND account_id = $1 \
                 ORDER BY id ASC",
            )
            .bind(account_id)
            .fetch_all(&self.pool),
        )
        .map_err(|error| error.to_string())?;
        rows.into_iter()
            .map(|row| row.try_get::<i32, _>("user_id").map_err(|error| error.to_string()))
            .collect()
    }

    fn enqueue_repo_perms_sync(&self, repo_id: i32, opts: FetchPermsOptions, reason: &str) {
        let _ = block_on(create_sync_job(
            &self.pool,
            Some(repo_id),
            None,
            reason,
            opts.invalidate_caches,
        ));
    }

    fn enqueue_user_perms_sync(&self, user_ids: &[i32], opts: FetchPermsOptions, reason: &str) {
        for user_id in user_ids {
            let _ = block_on(create_sync_job(
                &self.pool,
                None,
                Some(*user_id),
                reason,
                opts.invalidate_caches,
            ));
        }
    }
}

const QUEUED_DUPLICATES_FOR_USER: &str = r#"
SELECT id, priority FROM permission_sync_jobs
WHERE user_id = $1
  AND state = lower('queued')
  AND is_partial_success = FALSE
  AND process_after IS NULL
  AND cancel = false
ORDER BY permission_sync_jobs.id
LIMIT 1
"#;

const QUEUED_DUPLICATES_FOR_REPO: &str = r#"
SELECT id, priority FROM permission_sync_jobs
WHERE repository_id = $1
  AND state = lower('queued')
  AND is_partial_success = FALSE
  AND process_after IS NULL
  AND cancel = false
ORDER BY permission_sync_jobs.id
LIMIT 1
"#;

const CANCEL_QUEUED_JOB: &str = r#"
UPDATE permission_sync_jobs
SET cancel = TRUE, state = 'canceled', finished_at = $2, cancellation_reason = $3
WHERE id = $1 AND state = 'queued' AND cancel IS FALSE
"#;

const CANCELLATION_REASON_HIGHER_PRIORITY: &str = "A job with higher priority was added.";

/// `permissionSyncJobCreateQueryFmtstr` with NULL columns bound explicitly;
/// `invalidate_caches` is the one value the shared `sg-db` helper hardcodes to
/// FALSE, so the insert lives here.
const CREATE_SYNC_JOB: &str = r#"
INSERT INTO permission_sync_jobs (
	reason,
	triggered_by_user_id,
	process_after,
	repository_id,
	user_id,
	priority,
	invalidate_caches,
	no_perms
)
VALUES (
	$1,
	NULL,
	NULL,
	$2,
	$3,
	$4,
	$5,
	FALSE
)
ON CONFLICT DO NOTHING
RETURNING id
"#;

/// `createSyncJob` + `checkDuplicateAndCreateSyncJob` for the webhook jobs
/// (no `process_after`, high priority). Exactly one of `repo_id`/`user_id` is
/// set.
async fn create_sync_job(
    pool: &PgPool,
    repo_id: Option<i32>,
    user_id: Option<i32>,
    reason: &str,
    invalidate_caches: bool,
) -> Result<(), sqlx::Error> {
    let (duplicates_query, id) = match (repo_id, user_id) {
        (Some(repo_id), None) => (QUEUED_DUPLICATES_FOR_REPO, repo_id),
        (None, Some(user_id)) => (QUEUED_DUPLICATES_FOR_USER, user_id),
        _ => return Err(sqlx::Error::RowNotFound),
    };

    let mut tx = pool.begin().await?;
    let duplicate = sqlx::query(duplicates_query)
        .bind(id)
        .fetch_optional(&mut *tx)
        .await?;
    if let Some(row) = duplicate {
        let existing_priority: i32 = row.try_get("priority")?;
        if existing_priority >= HIGH_PRIORITY_PERMISSIONS_SYNC {
            tx.commit().await?;
            return Ok(());
        }
        let existing_id: i32 = row.try_get("id")?;
        sqlx::query(CANCEL_QUEUED_JOB)
            .bind(existing_id)
            .bind(OffsetDateTime::now_utc())
            .bind(CANCELLATION_REASON_HIGHER_PRIORITY)
            .execute(&mut *tx)
            .await?;
    }

    let inserted = sqlx::query(CREATE_SYNC_JOB)
        .bind(reason)
        .bind(repo_id)
        .bind(user_id)
        .bind(HIGH_PRIORITY_PERMISSIONS_SYNC)
        .bind(invalidate_caches)
        .fetch_optional(&mut *tx)
        .await?;
    if inserted.is_none() {
        tx.rollback().await?;
        return Err(sqlx::Error::RowNotFound);
    }

    tx.commit().await
}
