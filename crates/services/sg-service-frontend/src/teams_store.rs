#![forbid(unsafe_code)]

//! Production Postgres adapter for [`crate::teams_graphql::TeamsGraphqlDatabase`]
//! (M9.27a).
//!
//! Bridges the synchronous GraphQL seam to `sg-db::teams` with
//! `tokio::task::block_in_place`, and ports the user lookups the mutations use
//! (`Users().List({UserIDs|Usernames})`, `Users().GetByVerifiedEmail`,
//! `UserExternalAccounts().List`). No table, column or index is added.

use std::future::Future;

use sqlx::{PgPool, Row};
use time::OffsetDateTime;

use sg_db::teams as store;
use sg_db::teams::Team;

use crate::graphql_resolvers::UserData;
use crate::teams_graphql::{
    ExternalAccountRow, TeamMemberCursor, TeamMemberRow, TeamMembersListOpts, TeamRow,
    TeamsGraphqlDatabase, TeamsListOpts,
};

fn block_on<F: Future>(future: F) -> F::Output {
    match tokio::runtime::Handle::try_current() {
        Ok(handle) => tokio::task::block_in_place(|| handle.block_on(future)),
        Err(_) => tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("teams blocking runtime")
            .block_on(future),
    }
}

fn row_from_team(team: Team) -> TeamRow {
    TeamRow {
        id: team.id,
        name: team.name,
        display_name: team.display_name,
        readonly: team.readonly,
        parent_team_id: team.parent_team_id,
        creator_id: team.creator_id,
    }
}

fn to_store_team(team: &TeamRow) -> Team {
    Team {
        id: team.id,
        name: team.name.clone(),
        display_name: team.display_name.clone(),
        readonly: team.readonly,
        parent_team_id: team.parent_team_id,
        creator_id: team.creator_id,
        // Zero timestamps are replaced with `now` by the store (Go
        // `timeutil.Now`).
        created_at: OffsetDateTime::UNIX_EPOCH,
        updated_at: OffsetDateTime::UNIX_EPOCH,
    }
}

fn to_store_opts(opts: &TeamsListOpts) -> store::ListTeamsOpts {
    store::ListTeamsOpts {
        cursor: opts.cursor,
        with_parent_id: opts.with_parent_id,
        except_ancestor_id: opts.except_ancestor_id,
        root_only: opts.root_only,
        search: opts.search.clone(),
        for_user_member: opts.for_user_member,
        limit: opts.limit,
        offset: 0,
    }
}

fn to_store_member_opts(opts: &TeamMembersListOpts) -> store::ListTeamMembersOpts {
    store::ListTeamMembersOpts {
        cursor: store::TeamMemberListCursor {
            team_id: opts.cursor.team_id,
            user_id: opts.cursor.user_id,
        },
        team_id: opts.team_id,
        search: opts.search.clone(),
        limit: opts.limit,
        offset: 0,
    }
}

/// `areTeamEndpointsAvailable`: `DISABLE_OWN` and dotcom mode.
pub struct PgTeamsGraphqlStore {
    pool: PgPool,
    pub own_enabled: bool,
    pub dotcom: bool,
}

impl PgTeamsGraphqlStore {
    pub fn new(pool: PgPool) -> Self {
        let own_enabled = std::env::var("DISABLE_OWN")
            .ok()
            .and_then(|value| value.parse::<bool>().ok())
            .map(|disabled| !disabled)
            .unwrap_or(true);
        Self {
            pool,
            own_enabled,
            dotcom: false,
        }
    }

    pub fn with_own_enabled(mut self, enabled: bool) -> Self {
        self.own_enabled = enabled;
        self
    }

    pub fn with_dotcom(mut self, dotcom: bool) -> Self {
        self.dotcom = dotcom;
        self
    }

    fn scan_user(row: &sqlx::postgres::PgRow) -> Result<UserData, sqlx::Error> {
        let display_name: Option<String> = row.try_get("display_name")?;
        let avatar_url: Option<String> = row.try_get("avatar_url")?;
        Ok(UserData {
            id: row.try_get("id")?,
            username: row.try_get("username")?,
            display_name: display_name.unwrap_or_default(),
            avatar_url: avatar_url.unwrap_or_default(),
            created_at: row.try_get("created_at")?,
            updated_at: row.try_get("updated_at")?,
            site_admin: row.try_get("site_admin")?,
            tos_accepted: row.try_get("tos_accepted")?,
            primary_email: String::new(),
        })
    }

    const USER_COLUMNS: &'static str = "id, username, display_name, avatar_url, created_at, \
         updated_at, site_admin, tos_accepted";
}

impl TeamsGraphqlDatabase for PgTeamsGraphqlStore {
    fn endpoints_available(&self) -> Result<(), String> {
        if self.dotcom {
            return Err("teams are not available on sourcegraph.com".to_owned());
        }
        if !self.own_enabled {
            return Err("teams are disabled".to_owned());
        }
        Ok(())
    }

    fn get_team_by_id(&self, id: i32) -> Result<Option<TeamRow>, String> {
        match block_on(store::get_team_by_id(&self.pool, id)) {
            Ok(team) => Ok(Some(row_from_team(team))),
            Err(message) if message.starts_with("team not found:") => Ok(None),
            Err(message) => Err(message),
        }
    }

    fn get_team_by_name(&self, name: &str) -> Result<Option<TeamRow>, String> {
        match block_on(store::get_team_by_name(&self.pool, name)) {
            Ok(team) => Ok(Some(row_from_team(team))),
            Err(message) if message.starts_with("team not found:") => Ok(None),
            Err(message) => Err(message),
        }
    }

    fn list_teams(&self, opts: &TeamsListOpts) -> Result<(Vec<TeamRow>, i32), String> {
        let (teams, next) = block_on(store::list_teams(&self.pool, &to_store_opts(opts)))?;
        Ok((teams.into_iter().map(row_from_team).collect(), next))
    }

    fn count_teams(&self, opts: &TeamsListOpts) -> Result<i32, String> {
        block_on(store::count_teams(&self.pool, &to_store_opts(opts)))
    }

    fn contains_team(&self, id: i32, opts: &TeamsListOpts) -> Result<bool, String> {
        block_on(store::contains_team(&self.pool, id, &to_store_opts(opts)))
    }

    fn list_team_members(
        &self,
        opts: &TeamMembersListOpts,
    ) -> Result<(Vec<TeamMemberRow>, Option<TeamMemberCursor>), String> {
        let (members, next) =
            block_on(store::list_team_members(&self.pool, &to_store_member_opts(opts)))?;
        Ok((
            members
                .into_iter()
                .map(|member| TeamMemberRow {
                    team_id: member.team_id,
                    user_id: member.user_id,
                })
                .collect(),
            next.map(|cursor| TeamMemberCursor {
                team_id: cursor.team_id,
                user_id: cursor.user_id,
            }),
        ))
    }

    fn count_team_members(&self, opts: &TeamMembersListOpts) -> Result<i32, String> {
        block_on(store::count_team_members(
            &self.pool,
            &to_store_member_opts(opts),
        ))
    }

    fn create_team(&self, team: &TeamRow) -> Result<TeamRow, String> {
        block_on(store::create_team(&self.pool, &to_store_team(team)))
            .map(row_from_team)
    }

    fn update_team(&self, team: &TeamRow) -> Result<TeamRow, String> {
        block_on(store::update_team(&self.pool, &to_store_team(team)))
            .map(row_from_team)
    }

    fn delete_team(&self, id: i32) -> Result<(), String> {
        block_on(store::delete_team(&self.pool, id))
    }

    fn create_team_members(&self, members: &[TeamMemberRow]) -> Result<(), String> {
        let rows: Vec<store::TeamMember> = members
            .iter()
            .map(|member| store::TeamMember {
                team_id: member.team_id,
                user_id: member.user_id,
                created_at: OffsetDateTime::UNIX_EPOCH,
                updated_at: OffsetDateTime::UNIX_EPOCH,
            })
            .collect();
        block_on(store::create_team_member(&self.pool, &rows))
    }

    fn delete_team_members(&self, members: &[TeamMemberRow]) -> Result<(), String> {
        let rows: Vec<store::TeamMember> = members
            .iter()
            .map(|member| store::TeamMember {
                team_id: member.team_id,
                user_id: member.user_id,
                created_at: OffsetDateTime::UNIX_EPOCH,
                updated_at: OffsetDateTime::UNIX_EPOCH,
            })
            .collect();
        block_on(store::delete_team_member(&self.pool, &rows))
    }

    fn is_team_member(&self, team_id: i32, user_id: i32) -> Result<bool, String> {
        block_on(store::is_team_member(&self.pool, team_id, user_id))
    }

    fn user_by_id(&self, id: i32) -> Result<Option<UserData>, String> {
        let sql = format!(
            "SELECT {} FROM users WHERE id = $1 AND deleted_at IS NULL LIMIT 1",
            Self::USER_COLUMNS
        );
        let rows = block_on(sqlx::query(&sql).bind(id).fetch_all(&self.pool))
            .map_err(|err| err.to_string())?;
        match rows.first() {
            Some(row) => Self::scan_user(row)
                .map(Some)
                .map_err(|err| err.to_string()),
            None => Ok(None),
        }
    }

    fn users_by_ids(&self, ids: &[i32]) -> Result<Vec<UserData>, String> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        let sql = format!(
            "SELECT {} FROM users WHERE id = ANY($1) AND deleted_at IS NULL ORDER BY id ASC",
            Self::USER_COLUMNS
        );
        let rows = block_on(sqlx::query(&sql).bind(ids).fetch_all(&self.pool))
            .map_err(|err| err.to_string())?;
        rows.iter()
            .map(Self::scan_user)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|err| err.to_string())
    }

    fn users_by_usernames(&self, usernames: &[String]) -> Result<Vec<UserData>, String> {
        if usernames.is_empty() {
            return Ok(Vec::new());
        }
        let sql = format!(
            "SELECT {} FROM users WHERE username = ANY($1) AND deleted_at IS NULL ORDER BY id ASC",
            Self::USER_COLUMNS
        );
        let rows = block_on(sqlx::query(&sql).bind(usernames).fetch_all(&self.pool))
            .map_err(|err| err.to_string())?;
        rows.iter()
            .map(Self::scan_user)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|err| err.to_string())
    }

    fn user_by_verified_email(&self, email: &str) -> Result<Option<UserData>, String> {
        let sql = format!(
            "SELECT {} FROM users WHERE id = (SELECT user_id FROM user_emails WHERE email = $1 \
             AND verified_at IS NOT NULL) AND deleted_at IS NULL LIMIT 1",
            Self::USER_COLUMNS
        );
        let rows = block_on(sqlx::query(&sql).bind(email).fetch_all(&self.pool))
            .map_err(|err| err.to_string())?;
        match rows.first() {
            Some(row) => Self::scan_user(row)
                .map(Some)
                .map_err(|err| err.to_string()),
            None => Ok(None),
        }
    }

    fn external_accounts(
        &self,
        service_type: &str,
        service_id: &str,
    ) -> Result<Vec<ExternalAccountRow>, String> {
        let rows = block_on(
            sqlx::query(
                "SELECT user_id, account_id, account_data FROM user_external_accounts \
                 WHERE service_type = $1 AND service_id = $2 AND deleted_at IS NULL",
            )
            .bind(service_type)
            .bind(service_id)
            .fetch_all(&self.pool),
        )
        .map_err(|err| err.to_string())?;
        rows.iter()
            .map(|row| {
                let data: Option<String> = row.try_get("account_data").unwrap_or(None);
                let login = data
                    .as_deref()
                    .and_then(|text| serde_json::from_str::<serde_json::Value>(text).ok())
                    .and_then(|value| {
                        value
                            .get("login")
                            .and_then(|login| login.as_str())
                            .map(str::to_owned)
                    })
                    .unwrap_or_default();
                Ok(ExternalAccountRow {
                    user_id: row.try_get("user_id")?,
                    account_id: row.try_get("account_id")?,
                    login,
                })
            })
            .collect::<Result<Vec<_>, sqlx::Error>>()
            .map_err(|err| err.to_string())
    }
}
