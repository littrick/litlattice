#![forbid(unsafe_code)]

//! M9.45b: `PermissionsGitHubWebhook`
//! (`cmd/frontend/internal/authz/webhooks/github.go`).
//!
//! Coexists with the M9.44b handlers in [`crate::webhook_authz_handlers`]: the
//! baseline registers both on the same `webhooks.Router`, the newer handler
//! under the correct `Register(handler, extsvc.KindGitHub, events...)` shape,
//! so unlike the old `webhookhandlers.Init` registration these handlers are
//! *active* on the HTTP path.
//!
//! Reuses the existing `permission_sync_jobs`, `repo`, `external_service_repos`
//! and `user_external_accounts` tables — no new table/column/Redis key.
//!
//! Errata (source wins):
//!
//! * `ProcessAfter` is always `now + 10s` (`sleepTime`), so
//!   `createSyncJob` takes the non-zero branch: it inserts right away and
//!   **skips** `checkDuplicateAndCreateSyncJob`. The only deduplication is the
//!   partial unique index
//!   `permission_sync_jobs_unique (priority, user_id, repository_id, cancel, process_after) WHERE state='queued'`
//!   plus `ON CONFLICT DO NOTHING` — it does not include `reason`, so two
//!   different reasons for the same target and `process_after` collapse into
//!   one row.
//! * `team_add` is in `githubEvents` but `handleGitHubWebhook`'s type switch
//!   has no `*gh.TeamAddEvent` arm: parsed, then a no-op.
//! * Every handler error is discarded (`_ = h.handle…`), so the HTTP response
//!   stays 200 even when the repo/account lookup or enqueue fails.
//! * The external-account lookup passes only `ServiceID` and `AccountID`
//!   (no `ServiceType` filter) with `ExcludeExpired: true`
//!   (`expired_at IS NULL`), ordered by `id ASC`, and uses **only the first**
//!   account's `user_id`.

use serde::Deserialize;
use sqlx::postgres::PgPool;
use sqlx::types::time::OffsetDateTime;
use sqlx::Row;

/// `githubEvents` in registration order.
pub const GITHUB_EVENTS: [&str; 6] = [
    "repository",
    "member",
    "organization",
    "membership",
    "team",
    "team_add",
];

/// `extsvc.KindGitHub`.
pub const KIND_GITHUB: &str = "GITHUB";

/// `database.HighPriorityPermissionsSync`.
pub const HIGH_PRIORITY_PERMISSIONS_SYNC: i32 = 10;

/// `sleepTime` (`var sleepTime = 10 * time.Second`).
pub const SLEEP_TIME_SECONDS: i64 = 10;

pub const REASON_GITHUB_REPO_MADE_PRIVATE_EVENT: &str =
    "REASON_GITHUB_REPO_MADE_PRIVATE_EVENT";
pub const REASON_GITHUB_USER_ADDED_EVENT: &str = "REASON_GITHUB_USER_ADDED_EVENT";
pub const REASON_GITHUB_USER_REMOVED_EVENT: &str = "REASON_GITHUB_USER_REMOVED_EVENT";
pub const REASON_GITHUB_USER_MEMBERSHIP_ADDED_EVENT: &str =
    "REASON_GITHUB_USER_MEMBERSHIP_ADDED_EVENT";
pub const REASON_GITHUB_USER_MEMBERSHIP_REMOVED_EVENT: &str =
    "REASON_GITHUB_USER_MEMBERSHIP_REMOVED_EVENT";
pub const REASON_GITHUB_TEAM_ADDED_TO_REPO_EVENT: &str =
    "REASON_GITHUB_TEAM_ADDED_TO_REPO_EVENT";
pub const REASON_GITHUB_TEAM_REMOVED_FROM_REPO_EVENT: &str =
    "REASON_GITHUB_TEAM_REMOVED_FROM_REPO_EVENT";
pub const REASON_GITHUB_ORG_MEMBER_ADDED_EVENT: &str =
    "REASON_GITHUB_ORG_MEMBER_ADDED_EVENT";
pub const REASON_GITHUB_ORG_MEMBER_REMOVED_EVENT: &str =
    "REASON_GITHUB_ORG_MEMBER_REMOVED_EVENT";

// ---------------------------------------------------------------------------
// go-github payload shapes (only the fields the handlers read)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct GhRepository {
    pub clone_url: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct GhUser {
    pub id: Option<i64>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct GhMembership {
    pub user: Option<GhUser>,
}

/// The concrete go-github event types the handler's type switch matches.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PermissionsEvent {
    Repository {
        action: Option<String>,
        repo: Option<GhRepository>,
    },
    Member {
        action: Option<String>,
        repo: Option<GhRepository>,
        member: Option<GhUser>,
    },
    Organization {
        action: Option<String>,
        membership: Option<GhMembership>,
    },
    Membership {
        action: Option<String>,
        member: Option<GhUser>,
    },
    Team {
        action: Option<String>,
        repo: Option<GhRepository>,
    },
    /// Registered (`team_add`) but no arm in `handleGitHubWebhook`.
    TeamAdd,
}

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
    clone_url: Option<String>,
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

#[derive(Deserialize, Default)]
struct RawEvent {
    #[serde(default)]
    action: Option<String>,
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

fn raw_repo(repo: Option<RawRepo>) -> Option<GhRepository> {
    repo.map(|repo| GhRepository {
        clone_url: repo.clone_url,
    })
}

/// `github.ParseWebHook(eventType, payload)` narrowed to the events the
/// permissions handler knows about.
pub fn parse_event(event_type: &str, payload: &[u8]) -> Result<PermissionsEvent, ParseEventError> {
    match event_type {
        "repository" | "member" | "organization" | "membership" | "team" | "team_add" => {}
        // `public` is not registered by this handler; keep the old handler's
        // error surface for event types it owns.
        other => return Err(ParseEventError::UnknownType(other.to_owned())),
    }
    let raw: RawEvent = serde_json::from_slice(payload).map_err(|error| {
        if payload.is_empty() {
            ParseEventError::Json("unexpected end of JSON input".to_owned())
        } else {
            ParseEventError::Json(error.to_string())
        }
    })?;
    let repo = raw_repo(raw.repository);
    let member = raw_user(raw.member);
    let membership = raw.membership.map(|membership| GhMembership {
        user: raw_user(membership.user),
    });
    Ok(match event_type {
        "repository" => PermissionsEvent::Repository {
            action: raw.action,
            repo,
        },
        "member" => PermissionsEvent::Member {
            action: raw.action,
            repo,
            member,
        },
        "organization" => PermissionsEvent::Organization {
            action: raw.action,
            membership,
        },
        "membership" => PermissionsEvent::Membership {
            action: raw.action,
            member,
        },
        "team" => PermissionsEvent::Team {
            action: raw.action,
            repo,
        },
        "team_add" => PermissionsEvent::TeamAdd,
        _ => unreachable!("event type checked above"),
    })
}

// ---------------------------------------------------------------------------
// Handler
// ---------------------------------------------------------------------------

/// The DB reads and queue writes the handler performs.
pub trait PermissionsWebhookDeps: Send + Sync {
    /// `time.Now()` seam (the differential harness pins it).
    fn now(&self) -> OffsetDateTime {
        OffsetDateTime::now_utc()
    }

    /// `GetFirstRepoByCloneURL`: `Ok(None)` covers both "no
    /// `external_service_repos` row" and the `GetByName` miss.
    fn get_first_repo_by_clone_url(&self, clone_url: &str) -> Result<Option<i32>, String>;

    /// `UserExternalAccounts().List(ServiceID, AccountID, ExcludeExpired:true)`
    /// (`ORDER BY id ASC LIMIT 1`): the first account's `user_id`.
    fn first_github_account_user_id(
        &self,
        service_id: &str,
        account_id: &str,
    ) -> Result<Option<i32>, String>;

    /// `permssync.SchedulePermsSync` repo half: postponed insert, failures
    /// swallowed.
    fn enqueue_repo_perms_sync(&self, repo_id: i32, reason: &str, process_after: OffsetDateTime);

    /// `permssync.SchedulePermsSync` user half.
    fn enqueue_user_perms_sync(&self, user_id: i32, reason: &str, process_after: OffsetDateTime);
}

/// Run one parsed event through the handler body. Mirrors
/// `handleGitHubWebhook`'s type switch, including the discarded errors: the
/// returned value is always `Ok(())` at the dispatch boundary. The `Result`
/// is kept so callers can inspect the inner error in tests.
pub fn handle_event(
    event: &PermissionsEvent,
    code_host_urn: &str,
    deps: &dyn PermissionsWebhookDeps,
) -> Result<(), String> {
    let inner = match event {
        PermissionsEvent::Repository { action, repo } => {
            handle_repository_event(action.as_deref(), repo.as_ref(), deps)
        }
        PermissionsEvent::Member {
            action,
            repo,
            member,
        } => handle_member_event(action.as_deref(), repo.as_ref(), member.as_ref(), code_host_urn, deps),
        PermissionsEvent::Organization { action, membership } => handle_organization_event(
            action.as_deref(),
            membership.as_ref(),
            code_host_urn,
            deps,
        ),
        PermissionsEvent::Membership { action, member } => handle_membership_event(
            action.as_deref(),
            member.as_ref(),
            code_host_urn,
            deps,
        ),
        PermissionsEvent::Team { action, repo } => {
            handle_team_event(action.as_deref(), repo.as_ref(), deps)
        }
        // No arm in the baseline's type switch.
        PermissionsEvent::TeamAdd => Ok(()),
    };
    // `_ = h.handle…(...)`: every error is discarded.
    let _ = inner;
    Ok(())
}

fn handle_repository_event(
    action: Option<&str>,
    repo: Option<&GhRepository>,
    deps: &dyn PermissionsWebhookDeps,
) -> Result<(), String> {
    if action != Some("privatized") {
        return Ok(());
    }
    let repo = repo.cloned().unwrap_or_default();
    get_repo_and_sync_perms(&repo, REASON_GITHUB_REPO_MADE_PRIVATE_EVENT, deps)
}

fn handle_member_event(
    action: Option<&str>,
    repo: Option<&GhRepository>,
    member: Option<&GhUser>,
    code_host_urn: &str,
    deps: &dyn PermissionsWebhookDeps,
) -> Result<(), String> {
    let reason = match action {
        Some("added") => REASON_GITHUB_USER_ADDED_EVENT,
        Some("removed") => REASON_GITHUB_USER_REMOVED_EVENT,
        _ => return Ok(()),
    };
    let _ = repo;
    get_user_and_sync_perms(member.cloned().unwrap_or_default(), code_host_urn, reason, deps)
}

fn handle_organization_event(
    action: Option<&str>,
    membership: Option<&GhMembership>,
    code_host_urn: &str,
    deps: &dyn PermissionsWebhookDeps,
) -> Result<(), String> {
    let reason = match action {
        Some("member_added") => REASON_GITHUB_ORG_MEMBER_ADDED_EVENT,
        Some("member_removed") => REASON_GITHUB_ORG_MEMBER_REMOVED_EVENT,
        _ => return Ok(()),
    };
    let user = membership
        .and_then(|membership| membership.user.clone())
        .unwrap_or_default();
    get_user_and_sync_perms(user, code_host_urn, reason, deps)
}

fn handle_membership_event(
    action: Option<&str>,
    member: Option<&GhUser>,
    code_host_urn: &str,
    deps: &dyn PermissionsWebhookDeps,
) -> Result<(), String> {
    let reason = match action {
        Some("added") => REASON_GITHUB_USER_MEMBERSHIP_ADDED_EVENT,
        Some("removed") => REASON_GITHUB_USER_MEMBERSHIP_REMOVED_EVENT,
        _ => return Ok(()),
    };
    get_user_and_sync_perms(
        member.cloned().unwrap_or_default(),
        code_host_urn,
        reason,
        deps,
    )
}

fn handle_team_event(
    action: Option<&str>,
    repo: Option<&GhRepository>,
    deps: &dyn PermissionsWebhookDeps,
) -> Result<(), String> {
    let reason = match action {
        Some("added_to_repository") => REASON_GITHUB_TEAM_ADDED_TO_REPO_EVENT,
        Some("removed_from_repository") => REASON_GITHUB_TEAM_REMOVED_FROM_REPO_EVENT,
        _ => return Ok(()),
    };
    let repo = repo.cloned().unwrap_or_default();
    get_repo_and_sync_perms(&repo, reason, deps)
}

fn get_user_and_sync_perms(
    user: GhUser,
    code_host_urn: &str,
    reason: &str,
    deps: &dyn PermissionsWebhookDeps,
) -> Result<(), String> {
    // `user.GetID()` on a nil receiver returns 0.
    let account_id = user.id.unwrap_or(0).to_string();
    let user_id = deps.first_github_account_user_id(code_host_urn, &account_id)?;
    let Some(user_id) = user_id else {
        return Err(format!(
            "no github external accounts found with account id {account_id}"
        ));
    };
    deps.enqueue_user_perms_sync(user_id, reason, process_after(deps));
    Ok(())
}

fn get_repo_and_sync_perms(
    repo: &GhRepository,
    reason: &str,
    deps: &dyn PermissionsWebhookDeps,
) -> Result<(), String> {
    let clone_url = repo.clone_url.as_deref().unwrap_or("");
    let clone_url = clone_url.strip_suffix(".git").unwrap_or(clone_url);
    let repo_id = deps.get_first_repo_by_clone_url(clone_url)?;
    let Some(repo_id) = repo_id else {
        return Err(format!(
            "repo not found: clone_url=\"{clone_url}\""
        ));
    };
    deps.enqueue_repo_perms_sync(repo_id, reason, process_after(deps));
    Ok(())
}

fn process_after(deps: &dyn PermissionsWebhookDeps) -> OffsetDateTime {
    deps.now() + time::Duration::seconds(SLEEP_TIME_SECONDS)
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
            .expect("permissions webhook blocking runtime")
            .block_on(future),
    }
}

const FIRST_REPO_NAME_BY_CLONE_URL: &str = r#"
SELECT r.name
FROM repo r
JOIN external_service_repos esr ON r.id = esr.repo_id
WHERE esr.clone_url = $1
ORDER BY r.updated_at DESC
LIMIT 1
"#;

const REPO_ID_BY_NAME: &str = r#"
SELECT id FROM repo
WHERE name = $1 AND deleted_at IS NULL
ORDER BY id ASC
LIMIT 1
"#;

const FIRST_ACCOUNT_USER_ID: &str = r#"
SELECT user_id FROM user_external_accounts
WHERE deleted_at IS NULL
  AND service_id = $1
  AND account_id = $2
  AND expired_at IS NULL
ORDER BY id ASC
LIMIT 1
"#;

const CREATE_REPO_SYNC_JOB: &str = r#"
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
VALUES ($1, NULL, $2, $3, NULL, $4, FALSE, FALSE)
ON CONFLICT DO NOTHING
RETURNING id
"#;

const CREATE_USER_SYNC_JOB: &str = r#"
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
VALUES ($1, NULL, $2, NULL, $3, $4, FALSE, FALSE)
ON CONFLICT DO NOTHING
RETURNING id
"#;

/// `repo`, `external_service_repos`, `user_external_accounts` and
/// `permission_sync_jobs` backed by Postgres.
pub struct PgPermissionsWebhookDeps {
    pool: PgPool,
    fixed_now: Option<OffsetDateTime>,
}

impl PgPermissionsWebhookDeps {
    pub fn new(pool: PgPool) -> Self {
        Self {
            pool,
            fixed_now: None,
        }
    }

    /// Pin `time.Now()` for deterministic `process_after` values.
    pub fn with_now(pool: PgPool, now: OffsetDateTime) -> Self {
        Self {
            pool,
            fixed_now: Some(now),
        }
    }
}

impl PermissionsWebhookDeps for PgPermissionsWebhookDeps {
    fn now(&self) -> OffsetDateTime {
        self.fixed_now.unwrap_or_else(OffsetDateTime::now_utc)
    }

    fn get_first_repo_by_clone_url(&self, clone_url: &str) -> Result<Option<i32>, String> {
        let name = block_on(
            sqlx::query(FIRST_REPO_NAME_BY_CLONE_URL)
                .bind(clone_url)
                .fetch_optional(&self.pool),
        )
        .map_err(|error| error.to_string())?;
        let Some(name) = name else {
            return Ok(None);
        };
        let name: String = name.try_get("name").map_err(|error| error.to_string())?;
        let row = block_on(
            sqlx::query(REPO_ID_BY_NAME)
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

    fn first_github_account_user_id(
        &self,
        service_id: &str,
        account_id: &str,
    ) -> Result<Option<i32>, String> {
        let row = block_on(
            sqlx::query(FIRST_ACCOUNT_USER_ID)
                .bind(service_id)
                .bind(account_id)
                .fetch_optional(&self.pool),
        )
        .map_err(|error| error.to_string())?;
        match row {
            Some(row) => row
                .try_get::<i32, _>("user_id")
                .map(Some)
                .map_err(|error| error.to_string()),
            None => Ok(None),
        }
    }

    fn enqueue_repo_perms_sync(&self, repo_id: i32, reason: &str, process_after: OffsetDateTime) {
        // `createSyncJob` with a non-zero `ProcessAfter` inserts directly; a
        // conflict yields `sql.ErrNoRows` (logged, swallowed).
        let _ = block_on(
            sqlx::query(CREATE_REPO_SYNC_JOB)
                .bind(reason)
                .bind(process_after)
                .bind(repo_id)
                .bind(HIGH_PRIORITY_PERMISSIONS_SYNC)
                .fetch_optional(&self.pool),
        );
    }

    fn enqueue_user_perms_sync(&self, user_id: i32, reason: &str, process_after: OffsetDateTime) {
        let _ = block_on(
            sqlx::query(CREATE_USER_SYNC_JOB)
                .bind(reason)
                .bind(process_after)
                .bind(user_id)
                .bind(HIGH_PRIORITY_PERMISSIONS_SYNC)
                .fetch_optional(&self.pool),
        );
    }
}
