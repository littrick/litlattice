#![forbid(unsafe_code)]

//! `internal/database/teams.go` and `internal/database/assigned_teams.go`
//! (M9.27a).
//!
//! Ports the Teams store queries verbatim, including:
//!
//! - the `teams.id >= cursor` cursor predicate (Go `ListTeamsOpts.SQL`);
//! - `LIMIT limit + 1` overshoot and the trailing row used as `next`;
//! - `ListTeamMembers`'s `(team_id, user_id) >= (cursor.team, cursor.user)`,
//!   the optional `Search` join and the unconditional `users.deleted_at IS
//!   NULL` filter;
//! - `CreateTeam`'s two-step insert (`teams` then the shared `names` namespace
//!   reservation) and the `teams_name` / `names_pkey` conflict mapping;
//! - assigned teams (`Insert` ensures the `repo_paths` ancestor chain first,
//!   `ListAssignedTeamsForRepo`, `DeleteOwnerTeam`).
//!
//! No table, column or index is added.

use sqlx::postgres::PgPool;
use sqlx::postgres::PgRow;
use sqlx::{Postgres, Row, Transaction};
use time::OffsetDateTime;

/// `types.Team` (`internal/types/types.go:2056-2065`). Go uses the zero value
/// for NULL `display_name` / `parent_team_id` / `creator_id`; the Rust store
/// keeps `Option` internally and the GraphQL layer maps it back.
#[derive(Debug, Clone, PartialEq)]
pub struct Team {
    pub id: i32,
    pub name: String,
    pub display_name: Option<String>,
    pub readonly: bool,
    pub parent_team_id: Option<i32>,
    pub creator_id: Option<i32>,
    pub created_at: OffsetDateTime,
    pub updated_at: OffsetDateTime,
}

/// `types.TeamMember`.
#[derive(Debug, Clone, PartialEq)]
pub struct TeamMember {
    pub team_id: i32,
    pub user_id: i32,
    pub created_at: OffsetDateTime,
    pub updated_at: OffsetDateTime,
}

/// `database.TeamMemberListCursor`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TeamMemberListCursor {
    pub team_id: i32,
    pub user_id: i32,
}

/// `database.ListTeamsOpts` (limit is the already-incremented SQL limit).
#[derive(Debug, Clone, Default)]
pub struct ListTeamsOpts {
    pub cursor: i32,
    pub with_parent_id: i32,
    pub except_ancestor_id: i32,
    pub root_only: bool,
    pub search: String,
    pub for_user_member: i32,
    /// `LimitOffset.Limit`; `0` means no limit.
    pub limit: i32,
    pub offset: i32,
}

/// `database.ListTeamMembersOpts` (limit is the already-incremented SQL
/// limit).
#[derive(Debug, Clone, Default)]
pub struct ListTeamMembersOpts {
    pub cursor: TeamMemberListCursor,
    pub team_id: i32,
    pub search: String,
    pub limit: i32,
    pub offset: i32,
}

/// `TeamNotFoundError`.
pub fn team_not_found(args: impl std::fmt::Display) -> String {
    format!("team not found: {args}")
}

/// `ErrTeamNameAlreadyExists`.
pub const TEAM_NAME_ALREADY_EXISTS: &str =
    "team name is already taken (by a user, organization, or another team)";

const TEAM_COLUMNS: &str = "teams.id, teams.name, teams.display_name, teams.readonly, \
     teams.parent_team_id, teams.creator_id, teams.created_at, teams.updated_at";

fn scan_team(row: &PgRow) -> Result<Team, sqlx::Error> {
    Ok(Team {
        id: row.try_get("id")?,
        name: row.try_get("name")?,
        display_name: row.try_get("display_name")?,
        readonly: row.try_get("readonly")?,
        parent_team_id: row.try_get("parent_team_id")?,
        creator_id: row.try_get("creator_id")?,
        created_at: row.try_get("created_at")?,
        updated_at: row.try_get("updated_at")?,
    })
}

fn scan_team_member(row: &PgRow) -> Result<TeamMember, sqlx::Error> {
    Ok(TeamMember {
        team_id: row.try_get("team_id")?,
        user_id: row.try_get("user_id")?,
        created_at: row.try_get("created_at")?,
        updated_at: row.try_get("updated_at")?,
    })
}

/// The `(where, joins, ctes)` triple of `ListTeamsOpts.SQL`, plus the bind
/// values in `$n` order.
struct TeamSql {
    where_sql: String,
    joins: Vec<String>,
    cte: Option<String>,
    binds: Vec<TeamBind>,
}

enum TeamBind {
    Int(i32),
    Text(String),
}

fn list_teams_sql(opts: &ListTeamsOpts) -> TeamSql {
    let mut conds = vec!["teams.id >= $1".to_owned()];
    let mut binds = vec![TeamBind::Int(opts.cursor)];
    let mut next = 2usize;
    let mut joins = Vec::new();
    let mut cte = None;

    if opts.with_parent_id != 0 {
        conds.push(format!("teams.parent_team_id = ${next}"));
        binds.push(TeamBind::Int(opts.with_parent_id));
        next += 1;
    }
    if opts.root_only {
        conds.push("teams.parent_team_id IS NULL".to_owned());
    }
    if !opts.search.is_empty() {
        let term = format!("%{}%", opts.search);
        conds.push(format!(
            "(teams.name ILIKE ${next} OR teams.display_name ILIKE ${})",
            next + 1
        ));
        binds.push(TeamBind::Text(term.clone()));
        binds.push(TeamBind::Text(term));
        next += 2;
    }
    if opts.for_user_member != 0 {
        joins.push("JOIN team_members ON team_members.team_id = teams.id".to_owned());
        conds.push(format!("team_members.user_id = ${next}"));
        binds.push(TeamBind::Int(opts.for_user_member));
        next += 1;
    }
    if opts.except_ancestor_id != 0 {
        joins.push("LEFT JOIN descendants ON descendants.team_id = teams.id".to_owned());
        conds.push("descendants.team_id IS NULL".to_owned());
        cte = Some(format!(
            "WITH RECURSIVE descendants AS ( \
                 SELECT id AS team_id FROM teams WHERE id = ${next} \
             UNION ALL \
                 SELECT t.id AS team_id FROM teams t \
                 INNER JOIN descendants d ON t.parent_team_id = d.team_id \
             )"
        ));
        binds.push(TeamBind::Int(opts.except_ancestor_id));
        next += 1;
    }

    let _ = next;
    TeamSql {
        where_sql: conds.join(" AND "),
        joins,
        cte,
        binds,
    }
}

fn bind_team<'q>(
    mut query: sqlx::query::Query<'q, Postgres, sqlx::postgres::PgArguments>,
    binds: &'q [TeamBind],
) -> sqlx::query::Query<'q, Postgres, sqlx::postgres::PgArguments> {
    for bind in binds {
        query = match bind {
            TeamBind::Int(value) => query.bind(*value),
            TeamBind::Text(value) => query.bind(value.as_str()),
        };
    }
    query
}

/// `GetTeamByID`.
pub async fn get_team_by_id(pool: &PgPool, id: i32) -> Result<Team, String> {
    get_team(pool, "teams.id = $1", vec![TeamBind::Int(id)]).await
}

/// `GetTeamByName`.
pub async fn get_team_by_name(pool: &PgPool, name: &str) -> Result<Team, String> {
    get_team(
        pool,
        "teams.name = $1",
        vec![TeamBind::Text(name.to_owned())],
    )
    .await
}

async fn get_team(pool: &PgPool, cond: &str, binds: Vec<TeamBind>) -> Result<Team, String> {
    let sql = format!(
        "SELECT {TEAM_COLUMNS} FROM teams WHERE {cond} LIMIT 1"
    );
    let query = bind_team(sqlx::query(&sql), &binds);
    let rows = query.fetch_all(pool).await.map_err(|err| err.to_string())?;
    if rows.len() != 1 {
        return Err(team_not_found(cond));
    }
    scan_team(&rows[0]).map_err(|err| err.to_string())
}

/// `ListTeams`: returns the page plus `next` (0 when there is no next page).
pub async fn list_teams(
    pool: &PgPool,
    opts: &ListTeamsOpts,
) -> Result<(Vec<Team>, i32), String> {
    let built = list_teams_sql(opts);
    let mut sql = String::new();
    if let Some(cte) = &built.cte {
        sql.push_str(cte);
        sql.push('\n');
    }
    sql.push_str(&format!(
        "SELECT {TEAM_COLUMNS} FROM teams\n{}\nWHERE {}\nORDER BY teams.id ASC",
        built.joins.join("\n"),
        built.where_sql
    ));
    let mut binds = built.binds;
    if opts.limit > 0 {
        sql.push_str(&format!("\nLIMIT ${}", binds.len() + 1));
        binds.push(TeamBind::Int(opts.limit));
        if opts.offset > 0 {
            sql.push_str(&format!(" OFFSET ${}", binds.len() + 1));
            binds.push(TeamBind::Int(opts.offset));
        }
    }
    let query = bind_team(sqlx::query(&sql), &binds);
    let rows = query.fetch_all(pool).await.map_err(|err| err.to_string())?;
    let mut teams = rows
        .iter()
        .map(scan_team)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|err| err.to_string())?;
    let mut next = 0;
    if opts.limit > 0 && teams.len() == opts.limit as usize {
        next = teams[teams.len() - 1].id;
        teams.pop();
    }
    Ok((teams, next))
}

/// `CountTeams` (cursor forced to 0, no limit).
pub async fn count_teams(pool: &PgPool, opts: &ListTeamsOpts) -> Result<i32, String> {
    let mut opts = opts.clone();
    opts.cursor = 0;
    opts.limit = 0;
    opts.offset = 0;
    let built = list_teams_sql(&opts);
    let mut sql = String::new();
    if let Some(cte) = &built.cte {
        sql.push_str(cte);
        sql.push('\n');
    }
    sql.push_str(&format!(
        "SELECT COUNT(*) FROM teams\n{}\nWHERE {}",
        built.joins.join("\n"),
        built.where_sql
    ));
    let query = bind_team(sqlx::query(&sql), &built.binds);
    let count: i64 = query
        .fetch_one(pool)
        .await
        .map_err(|err| err.to_string())?
        .try_get(0)
        .map_err(|err| err.to_string())?;
    Ok(count as i32)
}

/// `ContainsTeam`.
pub async fn contains_team(
    pool: &PgPool,
    id: i32,
    opts: &ListTeamsOpts,
) -> Result<bool, String> {
    let mut opts = opts.clone();
    opts.cursor = 0;
    opts.limit = 0;
    opts.offset = 0;
    let built = list_teams_sql(&opts);
    let mut sql = String::new();
    if let Some(cte) = &built.cte {
        sql.push_str(cte);
        sql.push('\n');
    }
    let mut binds = built.binds;
    let id_param = binds.len() + 1;
    binds.push(TeamBind::Int(id));
    sql.push_str(&format!(
        "\nSELECT 1 FROM teams\n{}\nWHERE teams.id = ${id_param}\nAND {}\nLIMIT 1",
        built.joins.join("\n"),
        built.where_sql
    ));
    let query = bind_team(sqlx::query(&sql), &binds);
    let rows = query.fetch_all(pool).await.map_err(|err| err.to_string())?;
    Ok(!rows.is_empty())
}

fn team_member_conditions(opts: &ListTeamMembersOpts) -> (Vec<String>, Vec<TeamBind>, Vec<String>) {
    let mut conds = vec![
        "team_members.team_id >= $1".to_owned(),
        "team_members.user_id >= $2".to_owned(),
    ];
    let mut binds = vec![
        TeamBind::Int(opts.cursor.team_id),
        TeamBind::Int(opts.cursor.user_id),
    ];
    let mut next = 3usize;
    let mut joins = Vec::new();
    if opts.team_id != 0 {
        conds.push(format!("team_members.team_id = ${next}"));
        binds.push(TeamBind::Int(opts.team_id));
        next += 1;
    }
    if !opts.search.is_empty() {
        joins.push("JOIN users ON users.id = team_members.user_id".to_owned());
        let term = format!("%{}%", opts.search);
        conds.push(format!(
            "(users.username ILIKE ${next} OR users.display_name ILIKE ${})",
            next + 1
        ));
        binds.push(TeamBind::Text(term.clone()));
        binds.push(TeamBind::Text(term));
        next += 1;
        next += 1;
    } else {
        joins.push("LEFT JOIN users ON team_members.user_id = users.id".to_owned());
    }
    conds.push("users.deleted_at IS NULL".to_owned());
    let _ = next;
    (conds, binds, joins)
}

/// `ListTeamMembers`: returns the page plus the next cursor.
pub async fn list_team_members(
    pool: &PgPool,
    opts: &ListTeamMembersOpts,
) -> Result<(Vec<TeamMember>, Option<TeamMemberListCursor>), String> {
    let (conds, mut binds, joins) = team_member_conditions(opts);
    let mut sql = format!(
        "SELECT team_members.team_id, team_members.user_id, team_members.created_at, \
         team_members.updated_at FROM team_members\n{}\nWHERE {}\nORDER BY \
         team_members.team_id ASC, team_members.user_id ASC",
        joins.join("\n"),
        conds.join(" AND ")
    );
    if opts.limit > 0 {
        sql.push_str(&format!("\nLIMIT ${}", binds.len() + 1));
        binds.push(TeamBind::Int(opts.limit));
        if opts.offset > 0 {
            sql.push_str(&format!(" OFFSET ${}", binds.len() + 1));
            binds.push(TeamBind::Int(opts.offset));
        }
    }
    let query = bind_team(sqlx::query(&sql), &binds);
    let rows = query.fetch_all(pool).await.map_err(|err| err.to_string())?;
    let mut members = rows
        .iter()
        .map(scan_team_member)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|err| err.to_string())?;
    let mut next = None;
    if opts.limit > 0 && members.len() == opts.limit as usize {
        let last = &members[members.len() - 1];
        next = Some(TeamMemberListCursor {
            team_id: last.team_id,
            user_id: last.user_id,
        });
        members.pop();
    }
    Ok((members, next))
}

/// `CountTeamMembers` (cursor forced to zero, no limit).
pub async fn count_team_members(
    pool: &PgPool,
    opts: &ListTeamMembersOpts,
) -> Result<i32, String> {
    let mut opts = opts.clone();
    opts.cursor = TeamMemberListCursor::default();
    opts.limit = 0;
    opts.offset = 0;
    let (conds, binds, joins) = team_member_conditions(&opts);
    let sql = format!(
        "SELECT COUNT(1) FROM team_members\n{}\nWHERE {}",
        joins.join("\n"),
        conds.join(" AND ")
    );
    let query = bind_team(sqlx::query(&sql), &binds);
    let count: i64 = query
        .fetch_one(pool)
        .await
        .map_err(|err| err.to_string())?
        .try_get(0)
        .map_err(|err| err.to_string())?;
    Ok(count as i32)
}

fn map_constraint_error(err: &sqlx::Error) -> Option<String> {
    let sqlx::Error::Database(database) = err else {
        return None;
    };
    match database.constraint() {
        Some("teams_name") | Some("names_pkey") => Some(TEAM_NAME_ALREADY_EXISTS.to_owned()),
        Some("teams_name_max_length")
        | Some("teams_name_valid_chars")
        | Some("orgs_name_max_length")
        | Some("orgs_name_valid_chars") => Some(format!(
            "team name invalid: {}",
            database.constraint().expect("checked")
        )),
        Some("teams_display_name_max_length") | Some("orgs_display_name_max_length") => Some(
            format!(
                "team display name invalid: {}",
                database.constraint().expect("checked")
            ),
        ),
        _ => None,
    }
}

/// Message produced by the Go baseline's `CreateTeam` for a non-zero
/// `creator_id` (see [`create_team`]).
pub const NULL_INT32_CREATOR_DRIVER_ERROR: &str =
    "sql: converting argument $5 type: non-Value type int32 returned from Value";
/// Same driver quirk on `UpdateTeam` for a non-zero `parent_team_id` (`$2`).
pub const NULL_INT32_PARENT_DRIVER_ERROR: &str =
    "sql: converting argument $2 type: non-Value type int32 returned from Value";

/// `CreateTeam`: insert the row, then reserve the name in `names`.
pub async fn create_team(pool: &PgPool, team: &Team) -> Result<Team, String> {
    // The Go baseline binds `creator_id` through `dbutil.NewNullInt32`, whose
    // `Value()` returns a bare `int32` for non-zero values. `database/sql`
    // rejects that at parameter-conversion time, so `CreateTeam` always fails
    // for an authenticated creator (UID != 0) before any SQL runs. Reproduce
    // the observable behaviour byte for byte; creators with UID 0 still insert
    // (the value lowers to NULL).
    if team.creator_id.unwrap_or(0) != 0 {
        return Err(NULL_INT32_CREATOR_DRIVER_ERROR.to_owned());
    }
    let mut tx = pool.begin().await.map_err(|err| err.to_string())?;
    let team = create_team_in_tx(&mut tx, team).await?;
    tx.commit().await.map_err(|err| err.to_string())?;
    Ok(team)
}

async fn create_team_in_tx(tx: &mut Transaction<'_, Postgres>, team: &Team) -> Result<Team, String> {
    let now = OffsetDateTime::now_utc();
    let created_at = if team.created_at == OffsetDateTime::UNIX_EPOCH {
        now
    } else {
        team.created_at
    };
    let updated_at = if team.updated_at == OffsetDateTime::UNIX_EPOCH {
        created_at
    } else {
        team.updated_at
    };
    let sql = format!(
        "INSERT INTO teams (name, display_name, readonly, parent_team_id, creator_id, created_at, updated_at) \
         VALUES ($1, $2, $3, $4, $5, $6, $7) RETURNING {TEAM_COLUMNS}"
    );
    let row = sqlx::query(&sql)
        .bind(&team.name)
        .bind(team.display_name.as_deref())
        .bind(team.readonly)
        .bind(team.parent_team_id)
        .bind(team.creator_id)
        .bind(created_at)
        .bind(updated_at)
        .fetch_one(&mut **tx)
        .await
        .map_err(|err| map_constraint_error(&err).unwrap_or_else(|| err.to_string()))?;
    let created = scan_team(&row).map_err(|err| err.to_string())?;
    let result = sqlx::query("INSERT INTO names (name, team_id) VALUES ($1, $2)")
        .bind(&created.name)
        .bind(created.id)
        .execute(&mut **tx)
        .await;
    if let Err(err) = result {
        return Err(map_constraint_error(&err).unwrap_or_else(|| err.to_string()));
    }
    Ok(created)
}

/// `UpdateTeam`: returns the refreshed row.
pub async fn update_team(pool: &PgPool, team: &Team) -> Result<Team, String> {
    // Same `dbutil.NewNullInt32` quirk as in `create_team`, on `$2`
    // (`parent_team_id`).
    if team.parent_team_id.unwrap_or(0) != 0 {
        return Err(NULL_INT32_PARENT_DRIVER_ERROR.to_owned());
    }
    let updated_at = OffsetDateTime::now_utc();
    let sql = format!(
        "UPDATE teams SET display_name = $1, parent_team_id = $2, updated_at = $3 \
         WHERE id = $4 RETURNING {TEAM_COLUMNS}"
    );
    let rows = sqlx::query(&sql)
        .bind(team.display_name.as_deref())
        .bind(team.parent_team_id)
        .bind(updated_at)
        .bind(team.id)
        .fetch_all(pool)
        .await
        .map_err(|err| err.to_string())?;
    if rows.is_empty() {
        return Err(team_not_found(format!("id = {}", team.id)));
    }
    scan_team(&rows[0]).map_err(|err| err.to_string())
}

/// `DeleteTeam`: delete the row, then release the `names` reservation.
pub async fn delete_team(pool: &PgPool, id: i32) -> Result<(), String> {
    let mut tx = pool.begin().await.map_err(|err| err.to_string())?;
    let result = sqlx::query("DELETE FROM teams WHERE teams.id = $1")
        .bind(id)
        .execute(&mut *tx)
        .await
        .map_err(|err| err.to_string())?;
    if result.rows_affected() == 0 {
        return Err(team_not_found(format!("teams.id = {id}")));
    }
    sqlx::query("DELETE FROM names WHERE names.team_id = $1")
        .bind(id)
        .execute(&mut *tx)
        .await
        .map_err(|err| err.to_string())?;
    tx.commit().await.map_err(|err| err.to_string())
}

/// `CreateTeamMember`: multi-row `ON CONFLICT DO NOTHING`.
pub async fn create_team_member(pool: &PgPool, members: &[TeamMember]) -> Result<(), String> {
    if members.is_empty() {
        return Ok(());
    }
    let mut sql =
        String::from("INSERT INTO team_members (team_id, user_id, created_at, updated_at) VALUES ");
    let mut placeholders = Vec::with_capacity(members.len());
    for index in 0..members.len() {
        let base = index * 4;
        placeholders.push(format!(
            "(${}, ${}, ${}, ${})",
            base + 1,
            base + 2,
            base + 3,
            base + 4
        ));
    }
    sql.push_str(&placeholders.join(", "));
    sql.push_str(" ON CONFLICT DO NOTHING");
    let now = OffsetDateTime::now_utc();
    let mut query = sqlx::query(&sql);
    for member in members {
        let created_at = member.created_at;
        let updated_at = if member.updated_at == OffsetDateTime::UNIX_EPOCH {
            created_at
        } else {
            member.updated_at
        };
        let _ = now;
        query = query
            .bind(member.team_id)
            .bind(member.user_id)
            .bind(created_at)
            .bind(updated_at);
    }
    query.execute(pool).await.map_err(|err| err.to_string())?;
    Ok(())
}

/// `DeleteTeamMember`.
pub async fn delete_team_member(pool: &PgPool, members: &[TeamMember]) -> Result<(), String> {
    if members.is_empty() {
        return Ok(());
    }
    let mut placeholders = Vec::with_capacity(members.len());
    for index in 0..members.len() {
        placeholders.push(format!("(${}, ${})", index * 2 + 1, index * 2 + 2));
    }
    let sql = format!(
        "DELETE FROM team_members WHERE (team_id, user_id) IN ({})",
        placeholders.join(", ")
    );
    let mut query = sqlx::query(&sql);
    for member in members {
        query = query.bind(member.team_id).bind(member.user_id);
    }
    query.execute(pool).await.map_err(|err| err.to_string())?;
    Ok(())
}

/// `IsTeamMember`.
pub async fn is_team_member(pool: &PgPool, team_id: i32, user_id: i32) -> Result<bool, String> {
    let row = sqlx::query(
        "SELECT COUNT(*) = 1 FROM team_members WHERE team_id = $1 AND user_id = $2",
    )
    .bind(team_id)
    .bind(user_id)
    .fetch_one(pool)
    .await
    .map_err(|err| err.to_string())?;
    row.try_get(0).map_err(|err| err.to_string())
}

// ---------------------------------------------------------------------------
// assigned_teams (`internal/database/assigned_teams.go`)
// ---------------------------------------------------------------------------

/// `AssignedTeamSummary`.
#[derive(Debug, Clone, PartialEq)]
pub struct AssignedTeamSummary {
    pub owner_team_id: i32,
    pub file_path: String,
    pub repo_id: i32,
    pub who_assigned_user_id: Option<i32>,
    pub assigned_at: OffsetDateTime,
}

/// `assignedTeamsStore.Insert`: ensure the path exists, then insert.
pub async fn insert_assigned_team(
    pool: &PgPool,
    assigned_team_id: i32,
    repo_id: i32,
    absolute_path: &str,
    who_assigned_user_id: i32,
) -> Result<(), String> {
    crate::repo_paths::ensure_repo_paths(pool, &[absolute_path.to_owned()], repo_id)
        .await
        .map_err(|_| "cannot insert repo paths".to_owned())?;
    sqlx::query(
        "WITH repo_path AS ( \
             SELECT id FROM repo_paths WHERE absolute_path = $1 AND repo_id = $2 \
         ) \
         INSERT INTO assigned_teams(owner_team_id, file_path_id, who_assigned_team_id) \
         SELECT $3, p.id, $4 FROM repo_path AS p ON CONFLICT DO NOTHING",
    )
    .bind(absolute_path)
    .bind(repo_id)
    .bind(assigned_team_id)
    .bind(who_assigned_user_id)
    .execute(pool)
    .await
    .map_err(|err| err.to_string())?;
    Ok(())
}

/// `ListAssignedTeamsForRepo`.
pub async fn list_assigned_teams_for_repo(
    pool: &PgPool,
    repo_id: i32,
) -> Result<Vec<AssignedTeamSummary>, String> {
    let rows = sqlx::query(
        "SELECT a.owner_team_id, p.absolute_path, p.repo_id, a.who_assigned_team_id, a.assigned_at \
         FROM assigned_teams AS a \
         INNER JOIN repo_paths AS p ON p.id = a.file_path_id \
         WHERE p.repo_id = $1",
    )
    .bind(repo_id)
    .fetch_all(pool)
    .await
    .map_err(|err| err.to_string())?;
    rows.iter()
        .map(|row| {
            Ok(AssignedTeamSummary {
                owner_team_id: row.try_get("owner_team_id")?,
                file_path: row.try_get("absolute_path")?,
                repo_id: row.try_get("repo_id")?,
                who_assigned_user_id: row.try_get("who_assigned_team_id")?,
                assigned_at: row.try_get("assigned_at")?,
            })
        })
        .collect::<Result<Vec<_>, sqlx::Error>>()
        .map_err(|err| err.to_string())
}

/// `DeleteOwnerTeam`.
pub async fn delete_owner_team(
    pool: &PgPool,
    assigned_team_id: i32,
    repo_id: i32,
    absolute_path: &str,
) -> Result<(), String> {
    let result = sqlx::query(
        "WITH repo_path AS ( \
             SELECT id FROM repo_paths WHERE absolute_path = $1 AND repo_id = $2 \
         ) \
         DELETE FROM assigned_teams \
         WHERE owner_team_id = $3 AND file_path_id = (SELECT p.id FROM repo_path AS p)",
    )
    .bind(absolute_path)
    .bind(repo_id)
    .bind(assigned_team_id)
    .execute(pool)
    .await
    .map_err(|err| format!("executing SQL query: {err}"))?;
    if result.rows_affected() == 0 {
        return Err(format!(
            "cannot delete assigned owner team with ID={assigned_team_id} for {absolute_path:?} path for repo with ID={repo_id}"
        ));
    }
    Ok(())
}
