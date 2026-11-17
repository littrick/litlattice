#![forbid(unsafe_code)]

//! Production Postgres adapter for [`crate::authz_graphql::AuthzDatabase`]
//! (M9.48a).
//!
//! Reuses the existing authz tables through `sg-db` where the store already
//! exposed the query, and issues the remaining read/write statements with the
//! baseline SQL copied from `internal/database/{permission_sync_jobs,
//! perms_store,bitbucket_project_permissions,repos_perm}.go`.
//!
//! No table, column or Redis key is added. The synchronous GraphQL seam is
//! bridged to sqlx with `tokio::task::block_in_place` (the same pattern as the
//! other GraphQL stores).

use std::collections::{HashMap, HashSet};

use base64::Engine as _;
use std::future::Future;
use std::sync::Arc;

use sg_config::LiveConfig;
use sqlx::postgres::{PgPool, PgRow};
use sqlx::Row;
use time::OffsetDateTime;

use crate::authz_graphql as gql;
use crate::authz_graphql::{
    AuthzDatabase, AuthorizedRepositoriesData, AuthorizedUserRepositoriesArgs,
    AuthorizedUsersArgs, AuthorizedUsersData, BitbucketProjectPermissionJobData,
    BitbucketProjectPermissionJobsArgs, CancelPermissionsSyncJobResult, ConnectionPageInfoData,
    InfoConnectionArgs, PermissionsInfoData, PermissionsInfoRepositoriesData,
    PermissionsInfoRepositoryNodeData, PermissionsInfoUserNodeData, PermissionsInfoUsersData,
    PermissionsSyncJobNodeData, PermissionsSyncJobsArgs, PermissionsSyncJobsConnectionData,
    PermissionsSyncingStatsData, SubjectData, SubRepoPermissionInput, UserPermissionData,
    UserPermissionInput,
};

fn block_on<F: Future>(future: F) -> F::Output {
    match tokio::runtime::Handle::try_current() {
        Ok(handle) => tokio::task::block_in_place(|| handle.block_on(future)),
        Err(_) => tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("authz GraphQL blocking runtime")
            .block_on(future),
    }
}

const SOURCE_API: &str = "api";
const SOURCE_REPO_SYNC: &str = "repo_sync";
const SOURCE_USER_SYNC: &str = "user_sync";
const PERM_READ: &str = "read";
const PERM_OBJECT_REPOS: &str = "repos";
const SOURCEGRAPH_SERVICE_TYPE: &str = "sourcegraph";
const SOURCEGRAPH_SERVICE_ID: &str = "https://sourcegraph.com/";

pub struct PgAuthzStore {
    pool: PgPool,
    live: Arc<LiveConfig>,
    /// Whether the license gates are evaluated. Differential harnesses disable
    /// them to mirror `licensing.TestingSkipFeatureChecks`; production keeps
    /// them on.
    license_checks: bool,
}

impl PgAuthzStore {
    pub fn new(pool: PgPool, live: Arc<LiveConfig>) -> Self {
        Self {
            pool,
            live,
            license_checks: true,
        }
    }

    /// Test seam: `licensing.TestingSkipFeatureChecks`.
    pub fn with_test_license(mut self) -> Self {
        self.license_checks = false;
        self
    }

    fn site(&self) -> std::sync::Arc<sg_config::Unified> {
        self.live.current()
    }

    fn dotcom(&self) -> bool {
        crate::service_connections::sourcegraph_dotcom_mode(&|key: &str| {
            std::env::var(key).ok()
        })
    }

    fn authz_enforce_for_site_admins(&self) -> bool {
        self.site().site.authz_enforce_for_site_admins()
    }

    fn permissions_user_mapping(&self) -> (bool, String) {
        let site = self.site();
        let config = site.site.raw().get("permissions.userMapping");
        let enabled = config
            .and_then(|value| value.get("enabled"))
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false);
        let bind_id = config
            .and_then(|value| value.get("bindID"))
            .and_then(serde_json::Value::as_str)
            .unwrap_or("email");
        if bind_id != "email" && bind_id != "username" {
            return (false, "email".to_owned());
        }
        (enabled, bind_id.to_owned())
    }

    fn sync_backoff(&self, key: &str) -> time::Duration {
        let seconds = self
            .site()
            .site
            .raw()
            .get(key)
            .and_then(serde_json::Value::as_i64)
            .unwrap_or(0);
        if seconds <= 0 {
            time::Duration::seconds(60)
        } else {
            time::Duration::seconds(seconds)
        }
    }
}

#[derive(Debug, Clone)]
struct PermissionRow {
    user_id: i32,
    repo_id: i32,
    updated_at: OffsetDateTime,
    source: Option<String>,
}

async fn load_repository_data(
    pool: &PgPool,
    repo_id: i32,
) -> Result<Option<crate::graphql_resolvers::RepositoryData>, String> {
    let row = sqlx::query(
        "SELECT id, name::text AS name, uri::text AS uri, description, fork, archived, private, \
         stars, external_id, external_service_type, external_service_id \
         FROM repo WHERE id = $1 AND deleted_at IS NULL",
    )
    .bind(repo_id)
    .fetch_optional(pool)
    .await
    .map_err(|error| error.to_string())?;
    Ok(row.map(|row| scan_repository(&row)))
}

fn scan_repository(row: &PgRow) -> crate::graphql_resolvers::RepositoryData {
    crate::graphql_resolvers::RepositoryData {
        id: row.get("id"),
        name: row.get("name"),
        uri: row.try_get::<Option<String>, _>("uri").ok().flatten().unwrap_or_default(),
        description: row
            .try_get::<Option<String>, _>("description")
            .ok()
            .flatten()
            .unwrap_or_default(),
        fork: row.try_get("fork").unwrap_or(false),
        archived: row.try_get("archived").unwrap_or(false),
        private: row.try_get("private").unwrap_or(false),
        stars: row.try_get("stars").unwrap_or(0),
        external_id: row
            .try_get::<Option<String>, _>("external_id")
            .ok()
            .flatten()
            .unwrap_or_default(),
        external_service_type: row
            .try_get::<Option<String>, _>("external_service_type")
            .ok()
            .flatten()
            .unwrap_or_default(),
        external_service_id: row
            .try_get::<Option<String>, _>("external_service_id")
            .ok()
            .flatten()
            .unwrap_or_default(),
    }
}

async fn load_user_data(
    pool: &PgPool,
    user_id: i32,
) -> Result<Option<crate::graphql_resolvers::UserData>, String> {
    let row = sqlx::query(
        "SELECT id, username::text AS username, display_name, avatar_url, created_at, updated_at, \
         site_admin, tos_accepted, (passwd IS NOT NULL) AS builtin_auth, EXISTS (SELECT 1 FROM user_external_accounts uea WHERE uea.service_type='scim' AND uea.user_id = users.id AND uea.deleted_at IS NULL) AS scim_controlled FROM users WHERE id = $1 AND deleted_at IS NULL",
    )
    .bind(user_id)
    .fetch_optional(pool)
    .await
    .map_err(|error| error.to_string())?;
    Ok(row.map(|row| crate::graphql_resolvers::UserData {
        id: row.get("id"),
        username: row.get("username"),
        display_name: row
            .try_get::<Option<String>, _>("display_name")
            .ok()
            .flatten()
            .unwrap_or_default(),
        avatar_url: row
            .try_get::<Option<String>, _>("avatar_url")
            .ok()
            .flatten()
            .unwrap_or_default(),
        created_at: row.get("created_at"),
        updated_at: row.get("updated_at"),
        site_admin: row.try_get("site_admin").unwrap_or(false),
        tos_accepted: row.try_get("tos_accepted").unwrap_or(false),
            builtin_auth: row.try_get("builtin_auth").unwrap_or(false),
            scim_controlled: row.try_get("scim_controlled").unwrap_or(false),
        primary_email: String::new(),
    }))
}

async fn load_user_site_admin(pool: &PgPool, user_id: i32) -> Result<Option<bool>, String> {
    sqlx::query_scalar("SELECT site_admin FROM users WHERE id = $1 AND deleted_at IS NULL")
        .bind(user_id)
        .fetch_optional(pool)
        .await
        .map_err(|error| error.to_string())
}

async fn load_user_permissions(
    pool: &PgPool,
    user_id: i32,
) -> Result<Vec<PermissionRow>, String> {
    let rows = sqlx::query(
        "SELECT user_id, repo_id, updated_at, source FROM user_repo_permissions \
         WHERE user_id = $1",
    )
    .bind(user_id)
    .fetch_all(pool)
    .await
    .map_err(|error| error.to_string())?;
    Ok(rows.iter().map(scan_permission).collect())
}

async fn load_repo_permissions(pool: &PgPool, repo_id: i32) -> Result<Vec<PermissionRow>, String> {
    let rows = sqlx::query(
        "SELECT user_id, repo_id, updated_at, source FROM user_repo_permissions \
         WHERE repo_id = $1",
    )
    .bind(repo_id)
    .fetch_all(pool)
    .await
    .map_err(|error| error.to_string())?;
    let permissions: Vec<PermissionRow> = rows.iter().map(scan_permission).collect();
    // `LoadRepoPermissions` collapses to the unrestricted marker row.
    if let Some(row) = permissions.iter().find(|row| row.user_id == 0) {
        return Ok(vec![row.clone()]);
    }
    Ok(permissions)
}

fn scan_permission(row: &PgRow) -> PermissionRow {
    PermissionRow {
        user_id: row
            .try_get::<Option<i32>, _>("user_id")
            .ok()
            .flatten()
            .unwrap_or(0),
        repo_id: row.get("repo_id"),
        updated_at: row.get("updated_at"),
        source: row.try_get("source").ok(),
    }
}

fn source_to_graphql(source: &str) -> Option<&'static str> {
    match source {
        SOURCE_API => Some("API"),
        SOURCE_REPO_SYNC => Some("REPO_SYNC"),
        SOURCE_USER_SYNC => Some("USER_SYNC"),
        _ => None,
    }
}

async fn map_users(
    pool: &PgPool,
    bind_ids: &[String],
    bind_id_kind: &str,
) -> Result<HashMap<String, i32>, String> {
    let filtered: Vec<String> = bind_ids
        .iter()
        .map(|bind_id| bind_id.trim().to_owned())
        .filter(|bind_id| !bind_id.is_empty())
        .collect();
    let mut mapping = HashMap::new();
    if filtered.is_empty() {
        return Ok(mapping);
    }
    match bind_id_kind {
        "email" => {
            let rows = sqlx::query(
                "SELECT users.id, user_emails.email::text AS email FROM user_emails \
                 JOIN users ON users.id = user_emails.user_id \
                 WHERE user_emails.email = ANY($1) AND user_emails.verified_at IS NOT NULL \
                 AND users.deleted_at IS NULL",
            )
            .bind(&filtered)
            .fetch_all(pool)
            .await
            .map_err(|error| error.to_string())?;
            for row in &rows {
                let email: String = row.get("email");
                if filtered.contains(&email) {
                    mapping.insert(email, row.get("id"));
                }
            }
        }
        "username" => {
            let rows = sqlx::query(
                "SELECT id, username::text AS username FROM users \
                 WHERE username = ANY($1) AND deleted_at IS NULL",
            )
            .bind(&filtered)
            .fetch_all(pool)
            .await
            .map_err(|error| error.to_string())?;
            for row in &rows {
                let username: String = row.get("username");
                if filtered.contains(&username) {
                    mapping.insert(username, row.get("id"));
                }
            }
        }
        _ => {}
    }
    Ok(mapping)
}

fn authz_query(bypass: bool, _user_id: i32) -> String {
    if bypass {
        return "TRUE".to_owned();
    }
    let unrestricted = "EXISTS (SELECT FROM user_repo_permissions WHERE repo_id = repo.id AND user_id IS NULL)";
    let external = "(NOT repo.private OR EXISTS (SELECT FROM external_services AS es \
        JOIN external_service_repos AS esr ON (esr.external_service_id = es.id \
        AND esr.repo_id = repo.id AND es.unrestricted = TRUE AND es.deleted_at IS NULL)))";
    let restricted = "EXISTS (SELECT repo_id FROM user_repo_permissions \
        WHERE repo_id = repo.id AND user_id = $1::int)";
    format!("({unrestricted} OR {external} OR {restricted})")
}

fn go_zero_time() -> OffsetDateTime {
    OffsetDateTime::from_unix_timestamp(-62_135_596_800).expect("Go zero time")
}

fn maybe_datetime(value: OffsetDateTime) -> Option<OffsetDateTime> {
    if value == go_zero_time() {
        None
    } else {
        Some(value)
    }
}

// ---------------------------------------------------------------------------
// PermissionsSyncJobs connection
// ---------------------------------------------------------------------------

fn pagination_cursor(raw: Option<&str>) -> Result<Option<i32>, String> {
    match raw {
        None => Ok(None),
        Some(raw) => gql::unmarshal_permissions_sync_job_id(raw).map(Some),
    }
}

fn apply_page_info(
    nodes: &[i32],
    fetched_count: usize,
    page_size: usize,
    args: &PermissionsSyncJobsArgs,
) -> ConnectionPageInfoData {
    ConnectionPageInfoData {
        start_cursor: nodes.first().map(|id| gql::marshal_permissions_sync_job_id(*id)),
        end_cursor: nodes.last().map(|id| gql::marshal_permissions_sync_job_id(*id)),
        has_next_page: if args.first.is_some() {
            fetched_count > page_size
        } else if fetched_count == 0 {
            false
        } else {
            args.before.is_some()
        },
        has_previous_page: if args.last.is_some() {
            fetched_count > page_size
        } else if fetched_count == 0 {
            false
        } else {
            args.after.is_some()
        },
    }
}

async fn permissions_sync_jobs(
    pool: &PgPool,
    args: &PermissionsSyncJobsArgs,
) -> Result<PermissionsSyncJobsConnectionData, String> {
    let mut opts = sg_db::permission_sync_jobs::PermissionSyncJobListOpts {
        with_place_in_queue: true,
        reason_group: args.reason_group.clone().unwrap_or_default(),
        state: args.state.clone().unwrap_or_default(),
        partial_success: args.partial.unwrap_or(false),
        not_canceled: false,
        user_id: args.user_id.unwrap_or(0),
        repo_id: args.repo_id.unwrap_or(0),
        search_type: args.search_type.clone(),
        query: args.query.clone().unwrap_or_default(),
        ..Default::default()
    };
    let total_count = sg_db::permission_sync_jobs::count_permission_sync_jobs(pool, &opts)
        .await
        .map_err(|error| error.to_string())? as i32;

    let page_size: usize = {
        let limit = args.first.or(args.last).unwrap_or(0);
        if limit == 0 {
            100
        } else {
            limit.min(100) as usize
        }
    };
    let no_pagination = args.first.is_none() && args.last.is_none();
    if no_pagination {
        // `gqlutil.ConnectionResolver.paginationArgs` errors before any query.
        return Ok(PermissionsSyncJobsConnectionData {
            nodes: Err(
                "you must provide a `first` or `last` value to properly paginate".to_owned(),
            ),
            total_count,
            page_info: Err(
                "you must provide a `first` or `last` value to properly paginate".to_owned(),
            ),
        });
    }

    let after = pagination_cursor(args.after.as_deref())?;
    let before = pagination_cursor(args.before.as_deref())?;
    let limit = page_size as i64 + 1;
    if args.first.is_some() {
        opts.first = Some(limit);
    } else {
        opts.last = Some(limit);
    }
    opts.after = after;
    opts.before = before;

    let rows = sg_db::permission_sync_jobs::list_permission_sync_jobs(pool, &opts)
        .await
        .map_err(|error| error.to_string())?;

    // `ComputeNodes` resolves the subject and drops jobs whose user/repo no
    // longer exists, *before* `Nodes` reverses/trims the page.
    let mut resolved = Vec::with_capacity(rows.len());
    for row in rows {
        let subject = match (row.user_id, row.repository_id) {
            (Some(user_id), _) if user_id > 0 => match load_user_data(pool, user_id).await? {
                Some(user) => SubjectData::User(user),
                // Deleted users are dropped from the connection.
                None => continue,
            },
            (_, Some(repo_id)) if repo_id > 0 => {
                match load_repository_data(pool, repo_id).await? {
                    Some(repo) => SubjectData::Repository(repo),
                    None => continue,
                }
            }
            _ => continue,
        };
        let triggered_by_user = match row.triggered_by_user_id {
            Some(user_id) if user_id > 0 => load_user_data(pool, user_id).await?,
            _ => None,
        };
        resolved.push((
            row,
            PermissionsSyncJobNodeData {
                job: Default::default(),
                subject: Some(subject),
                triggered_by_user,
            },
        ));
    }
    let fetched_count = resolved.len();
    if args.last.is_some() {
        resolved.reverse();
    }
    if fetched_count > page_size {
        if args.last.is_some() {
            resolved.remove(0);
        } else {
            resolved.pop();
        }
    }

    let ids: Vec<i32> = resolved.iter().map(|(row, _)| row.id).collect();
    let page_info = apply_page_info(&ids, fetched_count, page_size, args);

    let nodes = resolved
        .into_iter()
        .map(|(row, mut node)| {
            node.job = row;
            node
        })
        .collect();

    Ok(PermissionsSyncJobsConnectionData {
        nodes: Ok(nodes),
        total_count,
        page_info: Ok(page_info),
    })
}

// ---------------------------------------------------------------------------
// Stats
// ---------------------------------------------------------------------------

async fn count_scalar(pool: &PgPool, sql: &str) -> Result<i64, String> {
    sqlx::query_scalar(sql)
        .fetch_one(pool)
        .await
        .map_err(|error| error.to_string())
}

async fn permissions_syncing_stats(
    pool: &PgPool,
    enforce_for_site_admins: bool,
    user_backoff: time::Duration,
    repo_backoff: time::Duration,
) -> Result<PermissionsSyncingStatsData, String> {
    let queue_size = sg_db::permission_sync_jobs::count_permission_sync_jobs(
        pool,
        &sg_db::permission_sync_jobs::PermissionSyncJobListOpts {
            state: "queued".to_owned(),
            ..Default::default()
        },
    )
    .await
    .map_err(|error| error.to_string())? as i32;
    let users_with_latest_job_failing =
        sg_db::permission_sync_jobs::count_users_with_failing_sync_job(pool)
            .await
            .map_err(|error| error.to_string())? as i32;
    let repos_with_latest_job_failing =
        sg_db::permission_sync_jobs::count_repos_with_failing_sync_job(pool)
            .await
            .map_err(|error| error.to_string())? as i32;

    let filter_site_admins = if enforce_for_site_admins {
        "TRUE"
    } else {
        "users.site_admin = FALSE"
    };
    let users_with_no_permissions = count_scalar(
        pool,
        &format!(
            "WITH users_having_permissions AS (SELECT DISTINCT user_id FROM user_repo_permissions) \
             SELECT COUNT(users.id) FROM users \
             LEFT OUTER JOIN users_having_permissions ON users_having_permissions.user_id = users.id \
             WHERE users.deleted_at IS NULL AND {filter_site_admins} \
             AND users_having_permissions.user_id IS NULL"
        ),
    )
    .await? as i32;
    let repos_with_no_permissions = count_scalar(
        pool,
        "WITH repos_with_permissions AS (SELECT DISTINCT repo_id FROM user_repo_permissions) \
         SELECT COUNT(repo.id) FROM repo \
         LEFT OUTER JOIN repos_with_permissions ON repos_with_permissions.repo_id = repo.id \
         WHERE repo.deleted_at IS NULL AND repo.private = TRUE \
         AND repos_with_permissions.repo_id IS NULL",
    )
    .await? as i32;

    let users_with_stale_permissions = count_stale(pool, true, user_backoff).await? as i32;
    let repos_with_stale_permissions = count_stale(pool, false, repo_backoff).await? as i32;

    Ok(PermissionsSyncingStatsData {
        queue_size,
        users_with_latest_job_failing,
        repos_with_latest_job_failing,
        users_with_no_permissions,
        repos_with_no_permissions,
        users_with_stale_permissions,
        repos_with_stale_permissions,
    })
}

async fn count_stale(
    pool: &PgPool,
    users: bool,
    age: time::Duration,
) -> Result<i64, String> {
    let cutoff = OffsetDateTime::now_utc() - age;
    let (column, join_table) = if users {
        ("user_id", "users")
    } else {
        ("repository_id", "repo")
    };
    let sql = format!(
        "WITH us AS (SELECT DISTINCT ON({column}) {column}, finished_at \
         FROM permission_sync_jobs \
         INNER JOIN {join_table} ON {join_table}.id = {column} AND {join_table}.deleted_at IS NULL \
         WHERE {column} IS NOT NULL ORDER BY {column} ASC, finished_at DESC) \
         SELECT COUNT({column}) FROM us WHERE finished_at < $1 OR finished_at IS NULL"
    );
    sqlx::query_scalar(&sql)
        .bind(cutoff)
        .fetch_one(pool)
        .await
        .map_err(|error| error.to_string())
}

// ---------------------------------------------------------------------------
// Bitbucket project jobs
// ---------------------------------------------------------------------------

async fn bitbucket_project_permission_jobs(
    pool: &PgPool,
    args: &BitbucketProjectPermissionJobsArgs,
) -> Result<Vec<BitbucketProjectPermissionJobData>, String> {
    let mut where_clauses = Vec::new();
    let mut params: Vec<String> = Vec::new();
    if let Some(keys) = &args.project_keys {
        if !keys.is_empty() {
            let keys = &keys[..keys.len().min(500)];
            let placeholders: Vec<String> = keys
                .iter()
                .map(|key| {
                    params.push(key.clone());
                    format!("${}", params.len())
                })
                .collect();
            where_clauses.push(format!("project_key IN ({})", placeholders.join(",")));
        }
    }
    if let Some(status) = args.status.as_deref().filter(|status| !status.is_empty()) {
        params.push(status.to_owned());
        where_clauses.push(format!("state = ${}", params.len()));
    }
    let limit = match args.count.unwrap_or(0) {
        count if count > 0 && count < 500 => count,
        count if count >= 500 => 500,
        _ => 100,
    };
    let where_sql = if where_clauses.is_empty() {
        String::new()
    } else {
        format!("WHERE {}", where_clauses.join(" AND "))
    };
    let sql = format!(
        "SELECT id, state, failure_message, queued_at, started_at, finished_at, process_after, \
         num_resets, num_failures, execution_logs, worker_hostname, project_key, \
         external_service_id, permissions, unrestricted \
         FROM explicit_permissions_bitbucket_projects_jobs {where_sql} \
         ORDER BY queued_at DESC LIMIT {limit}"
    );
    let mut query = sqlx::query(&sql);
    for param in &params {
        query = query.bind(param);
    }
    let rows = query
        .fetch_all(pool)
        .await
        .map_err(|error| error.to_string())?;
    let mut jobs = Vec::with_capacity(rows.len());
    for row in &rows {
        let permissions: Option<Vec<serde_json::Value>> =
            row.try_get("permissions").unwrap_or(None);
        let permissions = permissions
            .unwrap_or_default()
            .into_iter()
            .filter_map(|value| {
                let bind_id = value.get("bindID")?.as_str()?.to_owned();
                let permission = value
                    .get("permission")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("READ")
                    .to_owned();
                Some(UserPermissionData {
                    bind_id,
                    permission,
                })
            })
            .collect();
        jobs.push(BitbucketProjectPermissionJobData {
            id: row.get("id"),
            state: row.get("state"),
            failure_message: row.try_get("failure_message").ok().flatten(),
            queued_at: row.try_get("queued_at").ok().flatten(),
            started_at: row.try_get("started_at").ok().flatten(),
            finished_at: row.try_get("finished_at").ok().flatten(),
            process_after: row.try_get("process_after").ok().flatten(),
            num_resets: row.try_get("num_resets").unwrap_or(0),
            num_failures: row.try_get("num_failures").unwrap_or(0),
            project_key: row.get("project_key"),
            external_service_id: i64::from(row.get::<i32, _>("external_service_id")),
            permissions,
            unrestricted: row.try_get("unrestricted").unwrap_or(false),
        });
    }
    Ok(jobs)
}

// ---------------------------------------------------------------------------
// PermissionsInfo
// ---------------------------------------------------------------------------

async fn repository_permissions_info(
    pool: &PgPool,
    repo_id: i32,
) -> Result<PermissionsInfoData, String> {
    let permissions = load_repo_permissions(pool, repo_id).await?;
    let unrestricted = permissions.len() == 1 && permissions[0].user_id == 0;
    let updated_at = permissions
        .iter()
        .map(|permission| permission.updated_at)
        .max()
        .and_then(maybe_datetime);
    let latest = sg_db::perms_store::get_latest_finished_sync_job(pool, None, Some(repo_id), true)
        .await
        .map_err(|error| error.to_string())?;
    Ok(PermissionsInfoData {
        user_id: 0,
        repo_id,
        synced_at: latest.map(|job| job.finished_at).and_then(maybe_datetime),
        updated_at,
        source: None,
        unrestricted,
    })
}

async fn user_permissions_info(
    pool: &PgPool,
    user_id: i32,
) -> Result<PermissionsInfoData, String> {
    let permissions = load_user_permissions(pool, user_id).await?;
    let mut updated_at: Option<OffsetDateTime> = None;
    let mut source: Option<String> = None;
    for permission in &permissions {
        if updated_at.map_or(true, |current| permission.updated_at > current) {
            updated_at = Some(permission.updated_at);
            source = permission
                .source
                .as_deref()
                .and_then(source_to_graphql)
                .map(str::to_owned);
        }
    }
    Ok(PermissionsInfoData {
        user_id,
        repo_id: 0,
        synced_at: None,
        updated_at: updated_at.and_then(maybe_datetime),
        source,
        unrestricted: false,
    })
}

async fn permissions_info_repositories(
    pool: &PgPool,
    user_id: i32,
    args: &InfoConnectionArgs,
    enforce_for_site_admins: bool,
) -> Result<PermissionsInfoRepositoriesData, String> {
    let target_site_admin = load_user_site_admin(pool, user_id)
        .await?
        .unwrap_or(false);
    let bypass = target_site_admin && !enforce_for_site_admins;

    let page_size: usize = {
        let limit = args.first.or(args.last).unwrap_or(0);
        if limit <= 0 {
            100
        } else {
            limit.min(100) as usize
        }
    };
    let no_pagination = args.first.is_none() && args.last.is_none();
    if no_pagination {
        return Ok(PermissionsInfoRepositoriesData {
            nodes: Err(
                "you must provide a `first` or `last` value to properly paginate".to_owned(),
            ),
            total_count: count_visible_repos(pool, user_id, bypass, args.query.as_deref()).await?
                as i32,
            page_info: Err(
                "you must provide a `first` or `last` value to properly paginate".to_owned(),
            ),
        });
    }

    let total_count =
        count_visible_repos(pool, user_id, bypass, args.query.as_deref()).await? as i32;

    let ascending = args.last.is_none();
    // `$1` is always the target user id: the authz condition and the
    // `user_repo_permissions` join both reference it.
    let mut params: Vec<String> = vec![user_id.to_string()];
    let mut conditions: Vec<String> = vec![authz_query(bypass, user_id)];
    let after_id = args
        .after
        .as_deref()
        .map(unmarshal_repository_id)
        .transpose()?;
    let before_id = args
        .before
        .as_deref()
        .map(unmarshal_repository_id)
        .transpose()?;
    let mut next_placeholder = 1;
    if let Some(after) = after_id {
        next_placeholder += 1;
        let op = if ascending { ">" } else { "<" };
        params.push(after.to_string());
        conditions.push(format!("(repo.id) {op} (${next_placeholder})::int"));
    }
    if let Some(before) = before_id {
        next_placeholder += 1;
        let op = if ascending { "<" } else { ">" };
        params.push(before.to_string());
        conditions.push(format!("(repo.id) {op} (${next_placeholder})::int"));
    }
    if let Some(query) = args.query.as_deref().filter(|query| !query.is_empty()) {
        next_placeholder += 1;
        params.push(query.to_owned());
        conditions.push(format!("repo.name ILIKE ${next_placeholder}"));
    }
    let direction = if ascending { "ASC" } else { "DESC" };
    let limit = page_size as i64 + 1;
    let sql = format!(
        "WITH accessible_repos AS ( \
            SELECT repo.id, repo.name, repo.private, BOOL_OR(es.unrestricted) AS unrestricted, \
            row_number() OVER () AS row_id \
            FROM repo \
            LEFT JOIN external_service_repos AS esr ON esr.repo_id = repo.id \
            LEFT JOIN external_services AS es ON esr.external_service_id = es.id \
            WHERE repo.deleted_at IS NULL AND {conds} \
            GROUP BY repo.id ORDER BY repo.id {direction} LIMIT {limit} \
         ) \
         SELECT ar.id, ar.name, ar.private, ar.unrestricted, urp.updated_at AS permission_updated_at, \
         urp.source \
         FROM accessible_repos AS ar \
         LEFT JOIN user_repo_permissions AS urp ON urp.user_id = $1::int AND urp.repo_id = ar.id \
         ORDER BY row_id",
        conds = conditions.join(" AND "),
    );
    let mut query = sqlx::query(&sql);
    for param in &params {
        query = query.bind(param);
    }
    let rows = query
        .fetch_all(pool)
        .await
        .map_err(|error| error.to_string())?;
    let fetched_count = rows.len();
    let mut rows: Vec<&PgRow> = rows.iter().collect();
    if args.last.is_some() {
        rows.reverse();
    }
    if fetched_count > page_size {
        if args.last.is_some() {
            rows.remove(0);
        } else {
            rows.pop();
        }
    }

    let mut nodes = Vec::with_capacity(rows.len());
    for row in &rows {
        let repo = crate::graphql_resolvers::RepositoryData {
            id: row.get("id"),
            name: row.try_get("name").unwrap_or_default(),
            uri: String::new(),
            description: String::new(),
            fork: false,
            archived: false,
            private: row.try_get("private").unwrap_or(false),
            stars: 0,
            external_id: String::new(),
            external_service_type: String::new(),
            external_service_id: String::new(),
        };
        let unrestricted: Option<bool> = row.try_get("unrestricted").ok().flatten();
        let source: Option<String> = row.try_get("source").ok().flatten();
        let reason = user_repo_permission_reason(
            repo.private,
            unrestricted.unwrap_or(false),
            bypass,
            source.as_deref(),
        );
        nodes.push(PermissionsInfoRepositoryNodeData {
            repo,
            reason,
            updated_at: row
                .try_get::<Option<OffsetDateTime>, _>("permission_updated_at")
                .ok()
                .flatten()
                .and_then(maybe_datetime),
        });
    }

    let repo_ids: Vec<i32> = nodes.iter().map(|node| node.repo.id).collect();
    let page_info = ConnectionPageInfoData {
        start_cursor: repo_ids.first().map(|id| marshal_repository_id(*id)),
        end_cursor: repo_ids.last().map(|id| marshal_repository_id(*id)),
        has_next_page: if args.first.is_some() {
            fetched_count > page_size
        } else if fetched_count == 0 {
            false
        } else {
            args.before.is_some()
        },
        has_previous_page: if args.last.is_some() {
            fetched_count > page_size
        } else if fetched_count == 0 {
            false
        } else {
            args.after.is_some()
        },
    };

    Ok(PermissionsInfoRepositoriesData {
        nodes: Ok(nodes),
        total_count,
        page_info: Ok(page_info),
    })
}

async fn count_visible_repos(
    pool: &PgPool,
    user_id: i32,
    bypass: bool,
    query: Option<&str>,
) -> Result<i64, String> {
    let mut sql = String::from("SELECT COUNT(*) FROM repo WHERE repo.deleted_at IS NULL");
    let mut params: Vec<String> = Vec::new();
    if !bypass {
        let authz_sql = authz_query(false, user_id);
        sql.push_str(" AND ");
        sql.push_str(&authz_sql);
        params.push(user_id.to_string());
    }
    if let Some(query) = query.filter(|query| !query.is_empty()) {
        params.push(format!("%{query}%"));
        sql.push_str(&format!(" AND repo.name ILIKE ${}", params.len()));
    }
    let mut query = sqlx::query_scalar(&sql);
    for param in &params {
        query = query.bind(param);
    }
    query
        .fetch_one(pool)
        .await
        .map_err(|error| error.to_string())
}

fn user_repo_permission_reason(
    private: bool,
    unrestricted: bool,
    bypass_site_admin: bool,
    source: Option<&str>,
) -> String {
    if !private {
        "Public".to_owned()
    } else if unrestricted {
        "Unrestricted".to_owned()
    } else if bypass_site_admin {
        "Site Admin".to_owned()
    } else if source == Some(SOURCE_API) {
        "Explicit API".to_owned()
    } else if matches!(source, Some(SOURCE_REPO_SYNC) | Some(SOURCE_USER_SYNC)) {
        "Permissions Sync".to_owned()
    } else {
        String::new()
    }
}

async fn permissions_info_users(
    pool: &PgPool,
    repo_id: i32,
    args: &InfoConnectionArgs,
    enforce_for_site_admins: bool,
) -> Result<PermissionsInfoUsersData, String> {
    let page_size: usize = {
        let limit = args.first.or(args.last).unwrap_or(0);
        if limit <= 0 {
            100
        } else {
            limit.min(100) as usize
        }
    };
    let no_pagination = args.first.is_none() && args.last.is_none();
    if no_pagination {
        return Ok(PermissionsInfoUsersData {
            nodes: Err(
                "you must provide a `first` or `last` value to properly paginate".to_owned(),
            ),
            total_count: 0,
            page_info: Err(
                "you must provide a `first` or `last` value to properly paginate".to_owned(),
            ),
        });
    }

    let repo = sqlx::query("SELECT private FROM repo WHERE id = $1 AND deleted_at IS NULL")
        .bind(repo_id)
        .fetch_optional(pool)
        .await
        .map_err(|error| error.to_string())?
        .ok_or_else(|| format!("repo not found: {repo_id}"))?;
    let private: bool = repo.get("private");
    let unrestricted = is_repo_unrestricted(pool, repo_id).await?;

    let ascending = args.last.is_none();
    let direction = if ascending { "ASC" } else { "DESC" };
    let limit = page_size as i64 + 1;

    // Permissions conditions are ORed together, then ANDed with pagination and
    // search (`permsQueryConditions` + `where` in `ListRepoPermissions`).
    let mut permission_conditions = Vec::new();
    let mut filter_conditions = Vec::new();
    let mut params: Vec<String> = vec![repo_id.to_string()];
    if unrestricted {
        permission_conditions.push("TRUE".to_owned());
    } else {
        if !enforce_for_site_admins {
            permission_conditions.push("users.site_admin".to_owned());
        }
        permission_conditions.push("urp.repo_id = $1::int".to_owned());
    }
    let mut next_placeholder = 1;
    let after_username = args.after.clone();
    let before_username = args.before.clone();
    if let Some(after) = after_username {
        next_placeholder += 1;
        let op = if ascending { ">" } else { "<" };
        params.push(after);
        filter_conditions.push(format!(
            "(users.username) {op} (${next_placeholder}::citext)"
        ));
    }
    if let Some(before) = before_username {
        next_placeholder += 1;
        let op = if ascending { "<" } else { ">" };
        params.push(before);
        filter_conditions.push(format!(
            "(users.username) {op} (${next_placeholder}::citext)"
        ));
    }
    if let Some(query) = args.query.as_deref().filter(|query| !query.is_empty()) {
        next_placeholder += 1;
        let pattern = format!("%{query}%");
        params.push(pattern.clone());
        let first = next_placeholder;
        next_placeholder += 1;
        params.push(pattern);
        filter_conditions.push(format!(
            "(users.username ILIKE ${first} OR users.display_name ILIKE ${next_placeholder})"
        ));
    }
    let mut where_clause = format!("({})", permission_conditions.join(" OR "));
    for condition in filter_conditions {
        where_clause.push_str(" AND ");
        where_clause.push_str(&condition);
    }

    let sql = format!(
        "SELECT users.id, users.username::text AS username, users.display_name, users.avatar_url, \
         users.created_at, users.updated_at, users.site_admin, users.tos_accepted, (users.passwd IS NOT NULL) AS builtin_auth, \
         EXISTS (SELECT 1 FROM user_external_accounts uea WHERE uea.service_type='scim' AND uea.user_id = users.id AND uea.deleted_at IS NULL) AS scim_controlled, \
         urp.updated_at AS permission_updated_at, urp.source \
         FROM users \
         LEFT JOIN user_repo_permissions urp ON urp.user_id = users.id AND urp.repo_id = $1::int \
         WHERE users.deleted_at IS NULL AND {where_clause} \
         ORDER BY users.username {direction} LIMIT {limit}"
    );
    let mut query = sqlx::query(&sql);
    for param in &params {
        query = query.bind(param);
    }
    let rows = query
        .fetch_all(pool)
        .await
        .map_err(|error| error.to_string())?;
    let fetched_count = rows.len();
    let mut rows: Vec<&PgRow> = rows.iter().collect();
    if args.last.is_some() {
        rows.reverse();
    }
    if fetched_count > page_size {
        if args.last.is_some() {
            rows.remove(0);
        } else {
            rows.pop();
        }
    }

    let mut nodes = Vec::with_capacity(rows.len());
    for row in &rows {
        let user = crate::graphql_resolvers::UserData {
            id: row.get("id"),
            username: row.get("username"),
            display_name: row
                .try_get::<Option<String>, _>("display_name")
                .ok()
                .flatten()
                .unwrap_or_default(),
            avatar_url: row
                .try_get::<Option<String>, _>("avatar_url")
                .ok()
                .flatten()
                .unwrap_or_default(),
            created_at: row.get("created_at"),
            updated_at: row.get("updated_at"),
            site_admin: row.try_get("site_admin").unwrap_or(false),
            tos_accepted: row.try_get("tos_accepted").unwrap_or(false),
            builtin_auth: row.try_get("builtin_auth").unwrap_or(false),
            scim_controlled: row.try_get("scim_controlled").unwrap_or(false),
            primary_email: String::new(),
        };
        let source: Option<String> = row.try_get("source").ok().flatten();
        let reason = repo_user_permission_reason(
            private,
            unrestricted,
            !enforce_for_site_admins && user.site_admin,
            source.as_deref(),
        );
        nodes.push(PermissionsInfoUserNodeData {
            user,
            reason,
            updated_at: row
                .try_get::<Option<OffsetDateTime>, _>("permission_updated_at")
                .ok()
                .flatten()
                .and_then(maybe_datetime),
        });
    }

    let usernames: Vec<String> = nodes.iter().map(|node| node.user.username.clone()).collect();
    let page_info = ConnectionPageInfoData {
        start_cursor: usernames.first().cloned(),
        end_cursor: usernames.last().cloned(),
        has_next_page: if args.first.is_some() {
            fetched_count > page_size
        } else if fetched_count == 0 {
            false
        } else {
            args.before.is_some()
        },
        has_previous_page: if args.last.is_some() {
            fetched_count > page_size
        } else if fetched_count == 0 {
            false
        } else {
            args.after.is_some()
        },
    };

    Ok(PermissionsInfoUsersData {
        nodes: Ok(nodes),
        total_count: 0,
        page_info: Ok(page_info),
    })
}

async fn is_repo_unrestricted(pool: &PgPool, repo_id: i32) -> Result<bool, String> {
    let row = sqlx::query(
        "SELECT (EXISTS (SELECT FROM user_repo_permissions WHERE repo_id = repo.id AND user_id IS NULL) \
         OR (NOT repo.private OR EXISTS (SELECT FROM external_services AS es \
            JOIN external_service_repos AS esr ON (esr.external_service_id = es.id \
            AND esr.repo_id = repo.id AND es.unrestricted = TRUE AND es.deleted_at IS NULL)))) AS unrestricted \
         FROM repo WHERE repo.id = $1 AND repo.deleted_at IS NULL",
    )
    .bind(repo_id)
    .fetch_optional(pool)
    .await
    .map_err(|error| error.to_string())?;
    Ok(row.and_then(|row| row.try_get("unrestricted").ok()).unwrap_or(false))
}

fn repo_user_permission_reason(
    private: bool,
    unrestricted: bool,
    site_admin: bool,
    source: Option<&str>,
) -> String {
    if !private {
        "Public".to_owned()
    } else if unrestricted {
        "Unrestricted".to_owned()
    } else if site_admin {
        "Site Admin".to_owned()
    } else if source == Some(SOURCE_API) {
        "Explicit API".to_owned()
    } else if matches!(source, Some(SOURCE_REPO_SYNC) | Some(SOURCE_USER_SYNC)) {
        "Permissions Sync".to_owned()
    } else {
        String::new()
    }
}

fn unmarshal_repository_id(raw: &str) -> Result<i32, String> {
    crate::graphql_resolvers::unmarshal_relay_id(raw)
}

fn marshal_repository_id(id: i32) -> String {
    base64::engine::general_purpose::URL_SAFE.encode(format!("Repository:{id}"))
}

fn marshal_user_id(id: i32) -> String {
    base64::engine::general_purpose::URL_SAFE.encode(format!("User:{id}"))
}

// ---------------------------------------------------------------------------
// authorizedUserRepositories / authorizedUsers
// ---------------------------------------------------------------------------

async fn authorized_user_repositories(
    pool: &PgPool,
    args: &AuthorizedUserRepositoriesArgs,
) -> Result<AuthorizedRepositoriesData, String> {
    let mut user_id: Option<i32> = None;
    let mut bind_id = String::new();
    if let Some(email) = &args.email {
        bind_id = email.clone();
        user_id = sqlx::query_scalar(
            "SELECT user_id FROM user_emails WHERE email = $1 AND verified_at IS NOT NULL LIMIT 1",
        )
        .bind(email)
        .fetch_optional(pool)
        .await
        .map_err(|error| error.to_string())?;
    } else if let Some(username) = &args.username {
        bind_id = username.clone();
        user_id = sqlx::query_scalar(
            "SELECT id FROM users WHERE username = $1 AND deleted_at IS NULL LIMIT 1",
        )
        .bind(username)
        .fetch_optional(pool)
        .await
        .map_err(|error| error.to_string())?;
    } else {
        return Err("neither email nor username is given to identify a user".to_owned());
    }

    let mut ids: Vec<i32> = match user_id {
        Some(user_id) => {
            let permissions = load_user_permissions(pool, user_id).await?;
            let mut ids: Vec<i32> = permissions.iter().map(|permission| permission.repo_id).collect();
            ids.sort_unstable();
            ids
        }
        None => {
            let row = sqlx::query(
                "SELECT object_ids_ints FROM user_pending_permissions \
                 WHERE service_type = $1 AND service_id = $2 AND permission = $3 \
                 AND object_type = $4 AND bind_id = $5",
            )
            .bind(SOURCEGRAPH_SERVICE_TYPE)
            .bind(SOURCEGRAPH_SERVICE_ID)
            .bind(PERM_READ)
            .bind(PERM_OBJECT_REPOS)
            .bind(&bind_id)
            .fetch_optional(pool)
            .await
            .map_err(|error| error.to_string())?;
            let mut ids: Vec<i32> = row
                .and_then(|row| row.try_get("object_ids_ints").ok())
                .unwrap_or_default();
            ids.sort_unstable();
            ids
        }
    };
    ids.dedup();
    Ok(repository_connection(pool, ids, args.first, args.after.as_deref()).await?)
}

async fn authorized_users(
    pool: &PgPool,
    repo_id: i32,
    args: &AuthorizedUsersArgs,
) -> Result<AuthorizedUsersData, String> {
    let permissions = load_repo_permissions(pool, repo_id).await?;
    let mut ids: Vec<i32> = permissions.iter().map(|permission| permission.user_id).collect();
    ids.sort_unstable();
    user_connection(pool, ids, args.first, args.after.as_deref()).await
}

async fn repository_connection(
    pool: &PgPool,
    ids: Vec<i32>,
    first: i32,
    after: Option<&str>,
) -> Result<AuthorizedRepositoriesData, String> {
    let total_count = ids.len() as i32;
    let id_subset = id_subset(&ids, after, first)?;
    if id_subset.is_empty() {
        return Ok(AuthorizedRepositoriesData {
            repos: Vec::new(),
            total_count,
            has_next_page: false,
            end_cursor: None,
        });
    }
    let rows = sqlx::query(
        "SELECT id, name::text AS name, uri::text AS uri, description, fork, archived, private, \
         stars, external_id, external_service_type, external_service_id \
         FROM repo WHERE id = ANY($1) AND deleted_at IS NULL ORDER BY id",
    )
    .bind(&id_subset)
    .fetch_all(pool)
    .await
    .map_err(|error| error.to_string())?;
    let repos: Vec<crate::graphql_resolvers::RepositoryData> =
        rows.iter().map(scan_repository).collect();
    let (has_next_page, end_cursor) = page_boundary(
        &id_subset,
        ids.last().copied(),
        marshal_repository_id,
    );
    Ok(AuthorizedRepositoriesData {
        repos,
        total_count,
        has_next_page,
        end_cursor,
    })
}

async fn user_connection(
    pool: &PgPool,
    ids: Vec<i32>,
    first: i32,
    after: Option<&str>,
) -> Result<AuthorizedUsersData, String> {
    let total_count = ids.len() as i32;
    let id_subset = id_subset(&ids, after, first)?;
    if id_subset.is_empty() {
        return Ok(AuthorizedUsersData {
            users: Vec::new(),
            total_count,
            has_next_page: false,
            end_cursor: None,
        });
    }
    let rows = sqlx::query(
        "SELECT id, username::text AS username, display_name, avatar_url, created_at, updated_at, \
         site_admin, tos_accepted, (passwd IS NOT NULL) AS builtin_auth, EXISTS (SELECT 1 FROM user_external_accounts uea WHERE uea.service_type='scim' AND uea.user_id = users.id AND uea.deleted_at IS NULL) AS scim_controlled FROM users WHERE id = ANY($1) AND deleted_at IS NULL ORDER BY id",
    )
    .bind(&id_subset)
    .fetch_all(pool)
    .await
    .map_err(|error| error.to_string())?;
    let users: Vec<crate::graphql_resolvers::UserData> = rows
        .iter()
        .map(|row| crate::graphql_resolvers::UserData {
            id: row.get("id"),
            username: row.get("username"),
            display_name: row
                .try_get::<Option<String>, _>("display_name")
                .ok()
                .flatten()
                .unwrap_or_default(),
            avatar_url: row
                .try_get::<Option<String>, _>("avatar_url")
                .ok()
                .flatten()
                .unwrap_or_default(),
            created_at: row.get("created_at"),
            updated_at: row.get("updated_at"),
            site_admin: row.try_get("site_admin").unwrap_or(false),
            tos_accepted: row.try_get("tos_accepted").unwrap_or(false),
            builtin_auth: row.try_get("builtin_auth").unwrap_or(false),
            scim_controlled: row.try_get("scim_controlled").unwrap_or(false),
            primary_email: String::new(),
        })
        .collect();
    let (has_next_page, end_cursor) =
        page_boundary(&id_subset, ids.last().copied(), marshal_user_id);
    Ok(AuthorizedUsersData {
        users,
        total_count,
        has_next_page,
        end_cursor,
    })
}

/// `repositoryConnectionResolver.compute` / `userConnectionResolver.compute`
/// id-slice selection.
fn id_subset(ids: &[i32], after: Option<&str>, first: i32) -> Result<Vec<i32>, String> {
    let mut subset: Vec<i32> = match after {
        None => ids.to_vec(),
        Some(raw) => {
            let after_id = crate::graphql_resolvers::unmarshal_relay_id(raw)?;
            let mut chosen = Vec::new();
            for (index, id) in ids.iter().enumerate() {
                if *id == after_id {
                    if index < ids.len() - 1 {
                        chosen = ids[index + 1..].to_vec();
                    }
                    break;
                } else if *id > after_id {
                    if index < ids.len() - 1 {
                        chosen = ids[index..].to_vec();
                    }
                    break;
                }
            }
            chosen
        }
    };
    if first >= 0 && subset.len() > first as usize {
        subset.truncate(first as usize);
    }
    Ok(subset)
}

fn page_boundary(
    subset: &[i32],
    last_id: Option<i32>,
    marshal: fn(i32) -> String,
) -> (bool, Option<String>) {
    match (subset.last().copied(), last_id) {
        (Some(last), Some(overall)) if last == overall => (false, None),
        (Some(last), _) => (true, Some(marshal(last))),
        (None, _) => (false, None),
    }
}

// ---------------------------------------------------------------------------
// Mutations
// ---------------------------------------------------------------------------

async fn set_repository_permissions_for_users(
    pool: &PgPool,
    repo_id: i32,
    permissions: &[UserPermissionInput],
    bind_id_kind: &str,
    now: OffsetDateTime,
) -> Result<(), String> {
    load_repository_data(pool, repo_id)
        .await?
        .ok_or_else(|| format!("repository not found: {repo_id}"))?;
    let bind_ids: Vec<String> = permissions
        .iter()
        .map(|permission| permission.bind_id.clone())
        .collect();
    let mapping = map_users(pool, &bind_ids, bind_id_kind).await?;
    let pending: Vec<String> = bind_ids
        .iter()
        .filter(|bind_id| !mapping.contains_key(*bind_id))
        .cloned()
        .collect();
    let user_ids: HashSet<i32> = mapping.values().copied().collect();
    let users: Vec<sg_db::perms_store::UserIdWithExternalAccountId> = user_ids
        .into_iter()
        .map(|user_id| sg_db::perms_store::UserIdWithExternalAccountId {
            user_id,
            external_account_id: 0,
        })
        .collect();
    sg_db::perms_store::set_repo_perms(pool, repo_id, &users, SOURCE_API, now)
        .await
        .map_err(|error| format!("set user repo permissions: {error}"))?;
    sg_db::perms_store::set_repo_pending_permissions(
        pool,
        repo_id,
        PERM_READ,
        SOURCEGRAPH_SERVICE_TYPE,
        SOURCEGRAPH_SERVICE_ID,
        &pending,
        now,
    )
    .await
    .map_err(|error| format!("set repository pending permissions: {error}"))?;
    Ok(())
}

async fn schedule_repository_permissions_sync(
    pool: &PgPool,
    repo_id: i32,
    triggered_by_user_id: i32,
    now: OffsetDateTime,
) -> Result<(), String> {
    let opts = sg_db::permission_sync_jobs::CreatePermissionSyncJobOpts {
        reason: "REASON_MANUAL_REPO_SYNC".to_owned(),
        priority: 10,
        invalidate_caches: false,
        triggered_by_user_id,
        process_after: None,
    };
    // `permssync.SchedulePermsSync` logs enqueue failures and never returns
    // them.
    let _ = sg_db::permission_sync_jobs::create_repo_sync_job_with_opts(pool, repo_id, &opts, now)
        .await;
    Ok(())
}

async fn schedule_user_permissions_sync(
    pool: &PgPool,
    user_id: i32,
    invalidate_caches: bool,
    triggered_by_user_id: i32,
    now: OffsetDateTime,
) -> Result<(), String> {
    let opts = sg_db::permission_sync_jobs::CreatePermissionSyncJobOpts {
        reason: "REASON_MANUAL_USER_SYNC".to_owned(),
        priority: 10,
        invalidate_caches,
        triggered_by_user_id,
        process_after: None,
    };
    let _ = sg_db::permission_sync_jobs::create_user_sync_job_with_opts(pool, user_id, &opts, now)
        .await;
    Ok(())
}

fn normalize_paths(permission: &SubRepoPermissionInput) -> Result<Vec<String>, String> {
    if permission.paths.is_none()
        && (permission.path_includes.is_none() || permission.path_excludes.is_none())
    {
        return Err(
            "either both pathIncludes and pathExcludes needs to be set, or paths needs to be set"
                .to_owned(),
        );
    }
    let mut paths = Vec::new();
    if let Some(raw_paths) = &permission.paths {
        for path in raw_paths {
            if let Some(stripped) = path.strip_prefix('-') {
                if !stripped.starts_with('/') {
                    paths.push(format!("-/{stripped}"));
                } else {
                    paths.push(path.clone());
                }
            } else if path.starts_with('/') {
                paths.push(path.clone());
            } else {
                paths.push(format!("/{path}"));
            }
        }
    } else {
        for include in permission.path_includes.as_deref().unwrap_or(&[]) {
            if include.starts_with('/') {
                paths.push(include.clone());
            } else {
                paths.push(format!("/{include}"));
            }
        }
        for exclude in permission.path_excludes.as_deref().unwrap_or(&[]) {
            let exclude = if exclude.starts_with('/') {
                exclude.clone()
            } else {
                format!("/{exclude}")
            };
            paths.push(format!("-{exclude}"));
        }
    }
    Ok(paths)
}

async fn set_sub_repository_permissions_for_users(
    pool: &PgPool,
    repo_id: i32,
    permissions: &[SubRepoPermissionInput],
    bind_id_kind: &str,
    now: OffsetDateTime,
) -> Result<(), String> {
    let _ = now;
    load_repository_data(pool, repo_id)
        .await?
        .ok_or_else(|| format!("repository not found: {repo_id}"))?;
    let bind_ids: Vec<String> = permissions
        .iter()
        .map(|permission| permission.bind_id.clone())
        .collect();
    let mapping = map_users(pool, &bind_ids, bind_id_kind).await?;
    // Validate after mapping, like the baseline (map first, then check paths).
    let mut normalized = Vec::with_capacity(permissions.len());
    for permission in permissions {
        let user_id = *mapping
            .get(&permission.bind_id)
            .ok_or_else(|| format!("user \"{}\" not found", permission.bind_id))?;
        normalized.push((user_id, normalize_paths(permission)?));
    }
    for (user_id, paths) in normalized {
        sg_db::sub_repo_perms::upsert(
            pool,
            user_id,
            repo_id,
            &sg_db::sub_repo_perms::SubRepoPermissions { paths },
        )
        .await
        .map_err(|error| format!("upserting sub-repo permissions: {error}"))?;
    }
    Ok(())
}

async fn set_repository_permissions_for_bitbucket_project(
    pool: &PgPool,
    project_key: &str,
    external_service_id: i64,
    permissions: &[UserPermissionInput],
    unrestricted: bool,
    now: OffsetDateTime,
) -> Result<(), String> {
    let _ = now;
    let kind: Option<String> = sqlx::query_scalar(
        "SELECT kind FROM external_services WHERE id = $1 AND deleted_at IS NULL",
    )
    .bind(external_service_id)
    .fetch_optional(pool)
    .await
    .map_err(|error| error.to_string())?;
    let kind = kind.ok_or_else(|| format!("external service not found: {external_service_id}"))?;
    if kind != "BITBUCKETSERVER" {
        return Err(format!(
            "expected Bitbucket Server external service, got: {kind}"
        ));
    }
    if !permissions.is_empty() && unrestricted {
        return Err("cannot specify permissions when unrestricted is true".to_owned());
    }
    if permissions.is_empty() && !unrestricted {
        return Err("must specify permissions when unrestricted is false".to_owned());
    }
    let permissions_json: Vec<serde_json::Value> = permissions
        .iter()
        .map(|permission| {
            serde_json::json!({
                "bindID": permission.bind_id,
                "permission": permission.permission,
            })
        })
        .collect();
    let mut tx = pool.begin().await.map_err(|error| error.to_string())?;
    sqlx::query(
        "UPDATE explicit_permissions_bitbucket_projects_jobs SET state = 'canceled' \
         WHERE project_key = $1 AND external_service_id = $2 AND state = 'queued'",
    )
    .bind(project_key)
    .bind(external_service_id)
    .execute(&mut *tx)
    .await
    .map_err(|error| error.to_string())?;
    sqlx::query(
        "INSERT INTO explicit_permissions_bitbucket_projects_jobs \
         (project_key, external_service_id, permissions, unrestricted) VALUES ($1, $2, $3, $4)",
    )
    .bind(project_key)
    .bind(external_service_id)
    .bind(&permissions_json)
    .bind(unrestricted)
    .execute(&mut *tx)
    .await
    .map_err(|error| error.to_string())?;
    tx.commit().await.map_err(|error| error.to_string())?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Trait impl
// ---------------------------------------------------------------------------

impl AuthzDatabase for PgAuthzStore {
    fn dotcom(&self) -> bool {
        PgAuthzStore::dotcom(self)
    }

    fn check_license_acls(&self) -> Result<(), String> {
        if !self.license_checks {
            return Ok(());
        }
        let key = self.site().site.license_key();
        match sg_licensing::check_acls_error(&key) {
            None => Ok(()),
            Some(message) => Err(message),
        }
    }

    fn check_license_explicit_permissions(&self) -> Result<(), String> {
        if !self.license_checks {
            return Ok(());
        }
        let key = self.site().site.license_key();
        match sg_licensing::configured_license_info(&key) {
            Err(error) => Err(format!(
                "checking feature \"explicit-permissions-api\" activation: {error}"
            )),
            Ok(_) => {
                if sg_licensing::check_acls(&key) {
                    Ok(())
                } else {
                    Err(
                        "The feature \"explicit-permissions-api\" is not activated in your \
                         Sourcegraph license. Upgrade your Sourcegraph subscription to use this \
                         feature."
                            .to_owned(),
                    )
                }
            }
        }
    }

    fn permissions_sync_jobs(
        &self,
        args: &PermissionsSyncJobsArgs,
    ) -> Result<PermissionsSyncJobsConnectionData, String> {
        block_on(permissions_sync_jobs(&self.pool, args))
    }

    fn permissions_syncing_stats(&self) -> Result<PermissionsSyncingStatsData, String> {
        block_on(permissions_syncing_stats(
            &self.pool,
            self.authz_enforce_for_site_admins(),
            self.sync_backoff("permissions.syncUsersBackoffSeconds"),
            self.sync_backoff("permissions.syncReposBackoffSeconds"),
        ))
    }

    fn bitbucket_project_permission_jobs(
        &self,
        args: &BitbucketProjectPermissionJobsArgs,
    ) -> Result<Vec<BitbucketProjectPermissionJobData>, String> {
        block_on(bitbucket_project_permission_jobs(&self.pool, args))
    }

    fn repository_permissions_info(&self, repo_id: i32) -> Result<PermissionsInfoData, String> {
        block_on(repository_permissions_info(&self.pool, repo_id))
    }

    fn user_permissions_info(&self, user_id: i32) -> Result<PermissionsInfoData, String> {
        block_on(user_permissions_info(&self.pool, user_id))
    }

    fn permissions_info_repositories(
        &self,
        user_id: i32,
        args: &InfoConnectionArgs,
    ) -> Result<PermissionsInfoRepositoriesData, String> {
        block_on(permissions_info_repositories(
            &self.pool,
            user_id,
            args,
            self.authz_enforce_for_site_admins(),
        ))
    }

    fn permissions_info_users(
        &self,
        repo_id: i32,
        args: &InfoConnectionArgs,
    ) -> Result<PermissionsInfoUsersData, String> {
        block_on(permissions_info_users(
            &self.pool,
            repo_id,
            args,
            self.authz_enforce_for_site_admins(),
        ))
    }

    fn authorized_user_repositories(
        &self,
        args: &AuthorizedUserRepositoriesArgs,
    ) -> Result<AuthorizedRepositoriesData, String> {
        block_on(authorized_user_repositories(&self.pool, args))
    }

    fn users_with_pending_permissions(&self) -> Result<Vec<String>, String> {
        // `ListPendingUsers` has no `ORDER BY`; the returned order is the
        // table's physical order. Reproduce the exact query so the Go and
        // Rust sides observe the same rows in the same order.
        block_on(async {
            let rows = sqlx::query(
                "SELECT bind_id, object_ids_ints FROM user_pending_permissions \
                 WHERE service_type = $1 AND service_id = $2",
            )
            .bind(SOURCEGRAPH_SERVICE_TYPE)
            .bind(SOURCEGRAPH_SERVICE_ID)
            .fetch_all(&self.pool)
            .await
            .map_err(|error| error.to_string())?;
            let mut bind_ids = Vec::new();
            for row in &rows {
                let ids: Vec<i32> = row.try_get("object_ids_ints").unwrap_or_default();
                if ids.is_empty() {
                    continue;
                }
                bind_ids.push(row.try_get("bind_id").map_err(|error| error.to_string())?);
            }
            Ok(bind_ids)
        })
    }

    fn authorized_users(
        &self,
        repo_id: i32,
        args: &AuthorizedUsersArgs,
    ) -> Result<AuthorizedUsersData, String> {
        block_on(authorized_users(&self.pool, repo_id, args))
    }

    fn set_repository_permissions_for_users(
        &self,
        repo_id: i32,
        permissions: &[UserPermissionInput],
        now: OffsetDateTime,
    ) -> Result<(), String> {
        let bind_id_kind = self.permissions_user_mapping().1;
        block_on(set_repository_permissions_for_users(
            &self.pool,
            repo_id,
            permissions,
            &bind_id_kind,
            now,
        ))
    }

    fn set_repository_permissions_unrestricted(
        &self,
        repo_ids: &[i32],
        unrestricted: bool,
        now: OffsetDateTime,
    ) -> Result<(), String> {
        block_on(sg_db::perms_store::set_repo_permissions_unrestricted(
            &self.pool,
            repo_ids,
            unrestricted,
            now,
        ))
        .map_err(|error| error.to_string())
    }

    fn schedule_repository_permissions_sync(
        &self,
        repo_id: i32,
        triggered_by_user_id: i32,
        now: OffsetDateTime,
    ) -> Result<(), String> {
        block_on(schedule_repository_permissions_sync(
            &self.pool,
            repo_id,
            triggered_by_user_id,
            now,
        ))
    }

    fn schedule_user_permissions_sync(
        &self,
        user_id: i32,
        invalidate_caches: bool,
        triggered_by_user_id: i32,
        now: OffsetDateTime,
    ) -> Result<(), String> {
        block_on(schedule_user_permissions_sync(
            &self.pool,
            user_id,
            invalidate_caches,
            triggered_by_user_id,
            now,
        ))
    }

    fn set_sub_repository_permissions_for_users(
        &self,
        repo_id: i32,
        permissions: &[SubRepoPermissionInput],
        now: OffsetDateTime,
    ) -> Result<(), String> {
        let bind_id_kind = self.permissions_user_mapping().1;
        block_on(set_sub_repository_permissions_for_users(
            &self.pool,
            repo_id,
            permissions,
            &bind_id_kind,
            now,
        ))
    }

    fn set_repository_permissions_for_bitbucket_project(
        &self,
        project_key: &str,
        external_service_id: i64,
        permissions: &[UserPermissionInput],
        unrestricted: bool,
        now: OffsetDateTime,
    ) -> Result<(), String> {
        block_on(set_repository_permissions_for_bitbucket_project(
            &self.pool,
            project_key,
            external_service_id,
            permissions,
            unrestricted,
            now,
        ))
    }

    fn cancel_permissions_sync_job(
        &self,
        id: i32,
        reason: &str,
        now: OffsetDateTime,
    ) -> Result<CancelPermissionsSyncJobResult, String> {
        let canceled = block_on(sg_db::permission_sync_jobs::cancel_queued_job(
            &self.pool,
            id,
            reason,
            now,
        ))
        .map_err(|error| error.to_string())?;
        Ok(if canceled {
            CancelPermissionsSyncJobResult::Success
        } else {
            CancelPermissionsSyncJobResult::NotFound
        })
    }
}

/// `pgx` binds for `citext` comparisons arrive as text; the explicit casts in
/// the callers keep the query plans identical to the baseline.
const _: () = ();
