#![forbid(unsafe_code)]

//! Production [`crate::own_graphql::OwnGraphqlDatabase`] adapter (M9.48c).
//!
//! Every query reads/writes an existing table (`codeowners`, `assigned_owners`,
//! `assigned_teams`, `repo_paths`, `ownership_path_stats`,
//! `own_signal_configurations`, `own_aggregate_recent_view`,
//! `own_aggregate_recent_contribution`/`commit_authors`, `repo`, `users`,
//! `user_emails`, `teams`, `permissions`/`role_permissions`/`user_roles`).
//! No table, column or Redis key is added.
//!
//! The Go store is synchronous while `sqlx` is async, so the adapter bridges
//! with the same `block_in_place`/`block_on` helper used by the saved-search
//! store.

use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;

use sg_graphql::exec::Actor;
use sqlx::{PgPool, Row};

use sg_db::own_signal_configurations::{load_configurations, LoadSignalConfigurationArgs};
use sg_db::repository_lookup::{find_by_id, find_by_name_or_uri, RepoLookupRecord};
use sg_db::users::{find_user_by_username, find_user_by_verified_email, get_session_user};
use sg_db::user_emails::list_by_user;

use crate::git_graphql::GitObjectSource;
use crate::graphql_resolvers::{RepositoryData, UserData};
use crate::own_graphql::{
    CodeownersFileRow, OwnGraphqlDatabase, OwnRuleset, OwnStatsCounts, OwnTeamRow, RecentAuthor,
    RecentView, RulesetSource,
};
use crate::search_owners::{parse_codeowners, AssignedOwners, AssignedTeams, Ruleset, CODEOWNERS_LOCATIONS};

fn block_on<F: Future>(future: F) -> F::Output {
    match tokio::runtime::Handle::try_current() {
        Ok(handle) => tokio::task::block_in_place(|| handle.block_on(future)),
        Err(_) => tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("own blocking runtime")
            .block_on(future),
    }
}

/// Production own GraphQL store.
pub struct PgOwnGraphqlStore {
    pool: PgPool,
    objects: Arc<dyn GitObjectSource>,
    dotcom: bool,
}

impl PgOwnGraphqlStore {
    pub fn new(pool: PgPool, objects: Arc<dyn GitObjectSource>, dotcom: bool) -> Self {
        Self {
            pool,
            objects,
            dotcom,
        }
    }
}

fn user_data(record: sg_db::users::UserSessionRecord, primary_email: String) -> UserData {
    UserData {
        id: record.id,
        username: record.username,
        display_name: record.display_name,
        avatar_url: record.avatar_url,
        created_at: record.created_at,
        updated_at: record.updated_at,
        site_admin: record.site_admin,
        tos_accepted: record.tos_accepted,
        builtin_auth: record.builtin_auth,
        scim_controlled: record.scim_controlled,
        completions_quota: record.completions_quota,
        code_completions_quota: record.code_completions_quota,
        primary_email,
    }
}

fn first_verified_email(pool: &PgPool, user_id: i32, fallback: String) -> String {
    block_on(list_by_user(pool, user_id, true))
        .ok()
        .and_then(|emails| emails.into_iter().next().map(|email| email.email))
        .unwrap_or(fallback)
}

fn repo_data(record: RepoLookupRecord) -> RepositoryData {
    RepositoryData {
        id: record.id,
        name: record.name,
        uri: record.uri,
        description: record.description,
        fork: record.fork,
        archived: record.archived,
        private: record.private,
        stars: record.stars,
        external_id: record.external_id,
        external_service_type: record.external_service_type,
        external_service_id: record.external_service_id,
    }
}

impl OwnGraphqlDatabase for PgOwnGraphqlStore {
    fn sourcegraph_dotcom_mode(&self) -> bool {
        self.dotcom
    }

    fn current_user(&self, actor: Actor) -> Result<Option<UserData>, String> {
        let Some(uid) = actor.uid() else {
            return Ok(None);
        };
        let record = block_on(get_session_user(&self.pool, uid))
            .map_err(|err| err.to_string())?;
        Ok(record.map(|record| {
            let email = first_verified_email(&self.pool, record.id, record.email.clone());
            user_data(record, email)
        }))
    }

    fn check_ownership_assign_permission(
        &self,
        actor: Actor,
    ) -> Result<(), String> {
        if actor.is_internal() {
            return Ok(());
        }
        let uid = actor.uid().ok_or_else(|| "not authenticated".to_owned())?;
        let permitted: bool = block_on(
            sqlx::query_scalar(
                "SELECT EXISTS ( \
                     SELECT 1 FROM permissions \
                     INNER JOIN role_permissions ON role_permissions.permission_id = permissions.id \
                     INNER JOIN user_roles ON user_roles.role_id = role_permissions.role_id \
                     WHERE permissions.action = 'ASSIGN' \
                       AND permissions.namespace = 'OWNERSHIP' \
                       AND user_roles.user_id = $1)",
            )
            .bind(uid)
            .fetch_one(&self.pool),
        )
        .map_err(|err| err.to_string())?;
        if permitted {
            Ok(())
        } else {
            Err("user is missing permission OWNERSHIP#ASSIGN".to_owned())
        }
    }

    fn load_signal_configurations(
        &self,
    ) -> Result<Vec<sg_db::own_signal_configurations::SignalConfiguration>, String> {
        block_on(load_configurations(
            &self.pool,
            &LoadSignalConfigurationArgs::default(),
        ))
        .map_err(|err| err.to_string())
    }

    fn update_signal_configuration(
        &self,
        name: &str,
        excluded_repo_patterns: &[String],
        enabled: bool,
    ) -> Result<(), String> {
        block_on(
            sqlx::query(
                "UPDATE own_signal_configurations \
                 SET enabled = $1, excluded_repo_patterns = $2 \
                 WHERE name = $3",
            )
            .bind(enabled)
            .bind(excluded_repo_patterns)
            .bind(name)
            .execute(&self.pool),
        )
        .map(|_| ())
        .map_err(|err| err.to_string())
    }

    fn signal_enabled(&self, name: &str) -> Result<bool, String> {
        let enabled: Option<bool> = block_on(
            sqlx::query_scalar("SELECT enabled FROM own_signal_configurations WHERE name = $1")
                .bind(name)
                .fetch_optional(&self.pool),
        )
        .map_err(|err| err.to_string())?;
        enabled.ok_or_else(|| "signal configuration not found".to_owned())
    }

    fn recent_authors(&self, repo_id: i32, path: &str) -> Result<Vec<RecentAuthor>, String> {
        let rows = block_on(sg_db::recent_contribution_signals::find_recent_authors(
            &self.pool,
            repo_id,
            path,
        ))
        .map_err(|err| err.to_string())?;
        Ok(rows
            .into_iter()
            .map(|row| RecentAuthor {
                author_email: row.author_email,
                contribution_count: row.contribution_count,
            })
            .collect())
    }

    fn recent_views(&self, repo_id: i32, path: &str) -> Result<Vec<RecentView>, String> {
        let rows = block_on(
            sqlx::query(
                "SELECT o.viewer_id, o.views_count \
                 FROM own_aggregate_recent_view AS o \
                 INNER JOIN repo_paths AS p ON p.id = o.viewed_file_path_id \
                 WHERE p.repo_id = $1 AND p.absolute_path = $2 \
                 ORDER BY 3 DESC",
            )
            .bind(repo_id)
            .bind(path)
            .fetch_all(&self.pool),
        )
        .map_err(|err| err.to_string())?;
        rows.into_iter()
            .map(|row| {
                Ok(RecentView {
                    user_id: row.try_get("viewer_id").map_err(|err| err.to_string())?,
                    views_count: row.try_get("views_count").map_err(|err| err.to_string())?,
                })
            })
            .collect()
    }

    fn repo_by_id(
        &self,
        actor: Actor,
        id: i32,
    ) -> Result<Option<RepositoryData>, String> {
        let uid = actor.uid().unwrap_or(0);
        let bypass = actor.is_internal()
            || matches!(actor, Actor::User { .. })
                && block_on(get_session_user(&self.pool, uid))
                    .ok()
                    .flatten()
                    .is_some_and(|user| user.site_admin);
        let record = block_on(find_by_id(&self.pool, id, uid, bypass))
            .map_err(|err| err.to_string())?;
        Ok(record.map(repo_data))
    }

    fn repo_by_name(
        &self,
        actor: Actor,
        name: &str,
    ) -> Result<Option<RepositoryData>, String> {
        let uid = actor.uid().unwrap_or(0);
        let bypass = actor.is_internal()
            || matches!(actor, Actor::User { .. })
                && block_on(get_session_user(&self.pool, uid))
                    .ok()
                    .flatten()
                    .is_some_and(|user| user.site_admin);
        let record = block_on(find_by_name_or_uri(&self.pool, name, uid, bypass))
            .map_err(|err| err.to_string())?;
        Ok(record.map(repo_data))
    }

    fn ruleset(
        &self,
        repo_id: i32,
        repo_name: &str,
        commit: &str,
    ) -> Result<Option<OwnRuleset>, String> {
        // `RulesetForRepo`: ingested files take priority over the repo copy.
        let ingested = block_on(sg_db::ownership_stats::ingested_codeowners(&self.pool, repo_id))?;
        if let Some(contents) = ingested {
            let parsed = parse_codeowners(&contents)?;
            return Ok(Some(OwnRuleset {
                ruleset: Ruleset::new(parsed.rules),
                source: RulesetSource::Ingested { repo_id },
            }));
        }
        for path in CODEOWNERS_LOCATIONS {
            match block_on(self.objects.read_file(repo_id, repo_name, commit, path)) {
                Ok(bytes) => {
                    let contents = String::from_utf8_lossy(&bytes).into_owned();
                    let parsed = parse_codeowners(&contents)?;
                    return Ok(Some(OwnRuleset {
                        ruleset: Ruleset::new(parsed.rules),
                        source: RulesetSource::Git {
                            repo_id,
                            commit: commit.to_owned(),
                            path: path.to_owned(),
                        },
                    }));
                }
                Err(err) if err.not_exist => continue,
                Err(err) => return Err(err.message),
            }
        }
        Ok(None)
    }

    fn assigned_owners(&self, repo_id: i32) -> Result<AssignedOwners, String> {
        let rows = block_on(
            sqlx::query(
                "SELECT a.owner_user_id, p.absolute_path \
                 FROM assigned_owners AS a \
                 INNER JOIN repo_paths AS p ON p.id = a.file_path_id \
                 WHERE p.repo_id = $1",
            )
            .bind(repo_id)
            .fetch_all(&self.pool),
        )
        .map_err(|err| err.to_string())?;
        let mut assigned: AssignedOwners = HashMap::new();
        for row in rows {
            let owner_user_id: i32 = row.try_get("owner_user_id").map_err(|err| err.to_string())?;
            let file_path: String = row.try_get("absolute_path").map_err(|err| err.to_string())?;
            assigned.entry(file_path).or_default().push(owner_user_id);
        }
        Ok(assigned)
    }

    fn assigned_teams(&self, repo_id: i32) -> Result<AssignedTeams, String> {
        let rows = block_on(sg_db::teams::list_assigned_teams_for_repo(
            &self.pool, repo_id,
        ))?;
        let mut assigned: AssignedTeams = HashMap::new();
        for row in rows {
            assigned
                .entry(row.file_path)
                .or_default()
                .push(row.owner_team_id);
        }
        Ok(assigned)
    }

    fn user_by_id(&self, id: i32) -> Result<Option<UserData>, String> {
        let record = block_on(get_session_user(&self.pool, id)).map_err(|err| err.to_string())?;
        Ok(record.map(|record| {
            let email = first_verified_email(&self.pool, record.id, record.email.clone());
            user_data(record, email)
        }))
    }

    fn user_by_username(&self, username: &str) -> Result<Option<UserData>, String> {
        let record = block_on(find_user_by_username(&self.pool, username))
            .map_err(|err| err.to_string())?;
        Ok(record.map(|record| {
            let email = first_verified_email(&self.pool, record.id, record.email.clone());
            user_data(record, email)
        }))
    }

    fn user_by_verified_email(&self, email: &str) -> Result<Option<UserData>, String> {
        let record = block_on(find_user_by_verified_email(&self.pool, email))
            .map_err(|err| err.to_string())?;
        Ok(record.map(|record| {
            let email = first_verified_email(&self.pool, record.id, record.email.clone());
            user_data(record, email)
        }))
    }

    fn team_by_id(&self, id: i32) -> Result<Option<OwnTeamRow>, String> {
        match block_on(sg_db::teams::get_team_by_id(&self.pool, id)) {
            Ok(team) => Ok(Some(OwnTeamRow {
                id: team.id,
                name: team.name,
                display_name: team.display_name,
                readonly: team.readonly,
            })),
            Err(_) => Ok(None),
        }
    }

    fn team_by_name(&self, name: &str) -> Result<Option<OwnTeamRow>, String> {
        match block_on(sg_db::teams::get_team_by_name(&self.pool, name)) {
            Ok(team) => Ok(Some(OwnTeamRow {
                id: team.id,
                name: team.name,
                display_name: team.display_name,
                readonly: team.readonly,
            })),
            Err(_) => Ok(None),
        }
    }

    fn ownership_stats(
        &self,
        location: Option<(i32, String)>,
    ) -> Result<OwnStatsCounts, String> {
        let (path, repo_id) = match &location {
            Some((repo_id, path)) => (path.as_str(), Some(*repo_id)),
            None => ("", None),
        };
        let sql = if repo_id.is_some() {
            "WITH signal_config AS (SELECT * FROM own_signal_configurations WHERE name = 'analytics' LIMIT 1) \
             SELECT SUM(COALESCE(s.tree_codeowned_files_count, 0)), \
                    SUM(COALESCE(s.tree_assigned_ownership_files_count, 0)), \
                    SUM(COALESCE(s.tree_any_ownership_files_count, 0)), \
                    MAX(s.last_updated_at) \
             FROM ownership_path_stats AS s \
             INNER JOIN repo_paths AS p ON s.file_path_id = p.id \
             WHERE p.absolute_path = $1 AND p.repo_id = $2 \
               AND p.repo_id NOT IN (SELECT repo.id FROM repo, signal_config WHERE repo.name ~~ ANY(signal_config.excluded_repo_patterns))"
        } else {
            "WITH signal_config AS (SELECT * FROM own_signal_configurations WHERE name = 'analytics' LIMIT 1) \
             SELECT SUM(COALESCE(s.tree_codeowned_files_count, 0)), \
                    SUM(COALESCE(s.tree_assigned_ownership_files_count, 0)), \
                    SUM(COALESCE(s.tree_any_ownership_files_count, 0)), \
                    MAX(s.last_updated_at) \
             FROM ownership_path_stats AS s \
             INNER JOIN repo_paths AS p ON s.file_path_id = p.id \
             WHERE p.absolute_path = $1 \
               AND p.repo_id NOT IN (SELECT repo.id FROM repo, signal_config WHERE repo.name ~~ ANY(signal_config.excluded_repo_patterns))"
        };
        let mut query = sqlx::query(sql).bind(path);
        if let Some(repo_id) = repo_id {
            query = query.bind(repo_id);
        }
        let row = block_on(query.fetch_one(&self.pool)).map_err(|err| err.to_string())?;
        let codeowned: Option<i64> = row.try_get(0).map_err(|err| err.to_string())?;
        let assigned: Option<i64> = row.try_get(1).map_err(|err| err.to_string())?;
        let total_owned: Option<i64> = row.try_get(2).map_err(|err| err.to_string())?;
        let updated_at: Option<time::OffsetDateTime> =
            row.try_get(3).map_err(|err| err.to_string())?;
        Ok(OwnStatsCounts {
            codeowned: codeowned.unwrap_or(0) as i32,
            assigned: assigned.unwrap_or(0) as i32,
            total_owned: total_owned.unwrap_or(0) as i32,
            updated_at,
        })
    }

    fn total_files(&self, location: Option<(i32, String)>) -> Result<i32, String> {
        let (path, repo_id) = match &location {
            Some((repo_id, path)) => (path.as_str(), Some(*repo_id)),
            None => ("", None),
        };
        let sql = if repo_id.is_some() {
            "WITH signal_config AS (SELECT * FROM own_signal_configurations WHERE name = 'analytics' LIMIT 1) \
             SELECT SUM(COALESCE(p.tree_files_count, 0)) \
             FROM repo_paths AS p \
             WHERE p.absolute_path = $1 AND p.repo_id = $2 \
               AND p.repo_id NOT IN (SELECT repo.id FROM repo, signal_config WHERE repo.name ~~ ANY(signal_config.excluded_repo_patterns))"
        } else {
            "WITH signal_config AS (SELECT * FROM own_signal_configurations WHERE name = 'analytics' LIMIT 1) \
             SELECT SUM(COALESCE(p.tree_files_count, 0)) \
             FROM repo_paths AS p \
             WHERE p.absolute_path = $1 \
               AND p.repo_id NOT IN (SELECT repo.id FROM repo, signal_config WHERE repo.name ~~ ANY(signal_config.excluded_repo_patterns))"
        };
        let mut query = sqlx::query_scalar::<_, Option<i64>>(sql).bind(path);
        if let Some(repo_id) = repo_id {
            query = query.bind(repo_id);
        }
        let count = block_on(query.fetch_one(&self.pool)).map_err(|err| err.to_string())?;
        Ok(count.unwrap_or(0) as i32)
    }

    fn list_codeowners(
        &self,
        cursor: i32,
        limit: Option<i32>,
    ) -> Result<(Vec<CodeownersFileRow>, i32), String> {
        let rows = match limit {
            Some(limit) if limit > 0 => block_on(
                sqlx::query(
                    "SELECT contents, repo_id, created_at, updated_at FROM codeowners \
                     WHERE repo_id >= $1 ORDER BY repo_id ASC LIMIT $2",
                )
                .bind(cursor)
                .bind(i64::from(limit) + 1)
                .fetch_all(&self.pool),
            )
            .map_err(|err| err.to_string())?,
            _ => block_on(
                sqlx::query(
                    "SELECT contents, repo_id, created_at, updated_at FROM codeowners \
                     WHERE repo_id >= $1 ORDER BY repo_id ASC",
                )
                .bind(cursor)
                .fetch_all(&self.pool),
            )
            .map_err(|err| err.to_string())?,
        };
        let mut files = rows
            .into_iter()
            .map(|row| {
                Ok(CodeownersFileRow {
                    repo_id: row.try_get("repo_id").map_err(|err| err.to_string())?,
                    contents: row.try_get("contents").map_err(|err| err.to_string())?,
                    created_at: row.try_get("created_at").map_err(|err| err.to_string())?,
                    updated_at: row.try_get("updated_at").map_err(|err| err.to_string())?,
                })
            })
            .collect::<Result<Vec<_>, String>>()?;
        let mut next = 0;
        if let Some(limit) = limit.filter(|limit| *limit > 0) {
            if files.len() as i32 == limit + 1 {
                next = files[limit as usize].repo_id;
                files.truncate(limit as usize);
            }
        }
        Ok((files, next))
    }

    fn count_codeowners(&self) -> Result<i32, String> {
        let count: i64 =
            block_on(sqlx::query_scalar("SELECT COUNT(*) FROM codeowners").fetch_one(&self.pool))
                .map_err(|err| err.to_string())?;
        Ok(count as i32)
    }

    fn codeowners_for_repo(&self, repo_id: i32) -> Result<Option<CodeownersFileRow>, String> {
        let row = block_on(
            sqlx::query(
                "SELECT contents, repo_id, created_at, updated_at FROM codeowners \
                 WHERE repo_id = $1 LIMIT 1",
            )
            .bind(repo_id)
            .fetch_optional(&self.pool),
        )
        .map_err(|err| err.to_string())?;
        row.map(|row| {
            Ok(CodeownersFileRow {
                repo_id: row.try_get("repo_id").map_err(|err| err.to_string())?,
                contents: row.try_get("contents").map_err(|err| err.to_string())?,
                created_at: row.try_get("created_at").map_err(|err| err.to_string())?,
                updated_at: row.try_get("updated_at").map_err(|err| err.to_string())?,
            })
        })
        .transpose()
    }

    fn create_codeowners(
        &self,
        repo_id: i32,
        contents: &str,
        proto: &[u8],
    ) -> Result<CodeownersFileRow, String> {
        let row = block_on(
            sqlx::query(
                "INSERT INTO codeowners (contents, contents_proto, repo_id, created_at, updated_at) \
                 VALUES ($1, $2, $3, now(), now()) \
                 RETURNING contents, repo_id, created_at, updated_at",
            )
            .bind(contents)
            .bind(proto)
            .bind(repo_id)
            .fetch_one(&self.pool),
        )
        .map_err(|err| match &err {
            sqlx::Error::Database(error)
                if error.constraint() == Some("codeowners_repo_id_key") =>
            {
                "codeowners file has already been ingested for this repository".to_owned()
            }
            other => other.to_string(),
        })?;
        Ok(CodeownersFileRow {
            repo_id: row.try_get("repo_id").map_err(|err| err.to_string())?,
            contents: row.try_get("contents").map_err(|err| err.to_string())?,
            created_at: row.try_get("created_at").map_err(|err| err.to_string())?,
            updated_at: row.try_get("updated_at").map_err(|err| err.to_string())?,
        })
    }

    fn update_codeowners(
        &self,
        repo_id: i32,
        contents: &str,
        proto: &[u8],
    ) -> Result<CodeownersFileRow, String> {
        let row = block_on(
            sqlx::query(
                "UPDATE codeowners SET contents = $1, contents_proto = $2, updated_at = now() \
                 WHERE repo_id = $3 \
                 RETURNING contents, repo_id, created_at, updated_at",
            )
            .bind(contents)
            .bind(proto)
            .bind(repo_id)
            .fetch_optional(&self.pool),
        )
        .map_err(|err| err.to_string())?;
        let Some(row) = row else {
            return Err(format!("codeowners file not found: {repo_id}"));
        };
        Ok(CodeownersFileRow {
            repo_id: row.try_get("repo_id").map_err(|err| err.to_string())?,
            contents: row.try_get("contents").map_err(|err| err.to_string())?,
            created_at: row.try_get("created_at").map_err(|err| err.to_string())?,
            updated_at: row.try_get("updated_at").map_err(|err| err.to_string())?,
        })
    }

    fn delete_codeowners(&self, repo_ids: &[i32]) -> Result<(), String> {
        let result = block_on(
            sqlx::query("DELETE FROM codeowners WHERE repo_id = ANY($1)")
                .bind(repo_ids)
                .execute(&self.pool),
        )
        .map_err(|err| err.to_string())?;
        if result.rows_affected() == 0 {
            return Err("codeowners file not found".to_owned());
        }
        Ok(())
    }

    fn insert_assigned_owner(
        &self,
        user_id: i32,
        repo_id: i32,
        path: &str,
        who: i32,
    ) -> Result<(), String> {
        block_on(sg_db::repo_paths::ensure_repo_paths(
            &self.pool,
            &[path.to_owned()],
            repo_id,
        ))
        .map_err(|_| "cannot insert repo paths".to_owned())?;
        block_on(
            sqlx::query(
                "WITH repo_path AS ( \
                     SELECT id FROM repo_paths WHERE absolute_path = $1 AND repo_id = $2 \
                 ) \
                 INSERT INTO assigned_owners(owner_user_id, file_path_id, who_assigned_user_id) \
                 SELECT $3, p.id, $4 FROM repo_path AS p ON CONFLICT DO NOTHING",
            )
            .bind(path)
            .bind(repo_id)
            .bind(user_id)
            .bind(who)
            .execute(&self.pool),
        )
        .map(|_| ())
        .map_err(|err| err.to_string())
    }

    fn delete_assigned_owner(
        &self,
        user_id: i32,
        repo_id: i32,
        path: &str,
    ) -> Result<(), String> {
        let result = block_on(
            sqlx::query(
                "WITH repo_path AS ( \
                     SELECT id FROM repo_paths WHERE absolute_path = $1 AND repo_id = $2 \
                 ) \
                 DELETE FROM assigned_owners \
                 WHERE owner_user_id = $3 AND file_path_id = (SELECT p.id FROM repo_path AS p)",
            )
            .bind(path)
            .bind(repo_id)
            .bind(user_id)
            .execute(&self.pool),
        )
        .map_err(|err| err.to_string())?;
        if result.rows_affected() == 0 {
            return Err(format!(
                "cannot delete assigned owner with ID={user_id} for \"{path}\" path for repo with ID={repo_id}"
            ));
        }
        Ok(())
    }

    fn insert_assigned_team(
        &self,
        team_id: i32,
        repo_id: i32,
        path: &str,
        who: i32,
    ) -> Result<(), String> {
        block_on(sg_db::teams::insert_assigned_team(
            &self.pool, team_id, repo_id, path, who,
        ))
    }

    fn delete_assigned_team(
        &self,
        team_id: i32,
        repo_id: i32,
        path: &str,
    ) -> Result<(), String> {
        let result = block_on(
            sqlx::query(
                "WITH repo_path AS ( \
                     SELECT id FROM repo_paths WHERE absolute_path = $1 AND repo_id = $2 \
                 ) \
                 DELETE FROM assigned_teams \
                 WHERE owner_team_id = $3 AND file_path_id = (SELECT p.id FROM repo_path AS p)",
            )
            .bind(path)
            .bind(repo_id)
            .bind(team_id)
            .execute(&self.pool),
        )
        .map_err(|err| err.to_string())?;
        if result.rows_affected() == 0 {
            return Err(format!(
                "cannot delete assigned owner team with ID={team_id} for \"{path}\" path for repo with ID={repo_id}"
            ));
        }
        Ok(())
    }
}
