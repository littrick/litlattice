#![forbid(unsafe_code)]

//! Teams GraphQL surface (M9.27a): `cmd/frontend/graphqlbackend/teams.go`.
//!
//! Ports the whole SDL surface actually present in `schema.graphql`:
//!
//! - `Query.team(name:)`, `Query.teams(first,after,search,exceptAncestor,
//!   includeChildTeams)`, and the `Query.node` kind `Team`;
//! - `Team` (`id`, `name`, `url`, `avatarURL`, `displayName`, `readonly`,
//!   `members`, `parentTeam`, `childTeams`, `viewerCanAdminister`, `creator`,
//!   `external`);
//! - `TeamConnection` / `TeamMemberConnection` with the exact Go pagination
//!   cursors (`base64("<id>")` and `base64(json{"team","user"})`);
//! - `User.teams` (errata: the Go resolver ignores membership and lists root
//!   teams — `ForUserMember` is never set, `teams.go:856-867`);
//! - `createTeam`, `updateTeam`, `deleteTeam`, `addTeamMembers`,
//!   `setTeamMembers`, `removeTeamMembers`.
//!
//! Storage is injected through the synchronous [`TeamsGraphqlDatabase`] seam;
//! the production adapter is [`crate::teams_store::PgTeamsGraphqlStore`].
//! No table, column or Redis key is added.
//!
//! Errata: the baseline SDL has no `Site.teams`, `Org.teams` or
//! `Query.node(TeamMember)`, so those entries are deliberately absent here too.

use std::sync::Arc;

use base64::Engine as _;
use sg_graphql::exec::{Actor, Arguments, ExecError, FieldValue, InputValue, ObjectValue};
use sg_graphql::json::Json;

use crate::executor_secrets_graphql::{decode_int_cursor, encode_int_cursor};
use crate::graphql_resolvers::{unmarshal_relay_id, PageInfoObject, UserData, UserObject};

pub type TeamsHandle = Arc<dyn TeamsGraphqlDatabase + Send + Sync>;

/// Wrapper so [`crate::graphql_resolvers::NestedPrefetch`] can keep its
/// `PartialEq`/`Eq` derive (handles compare by pointer).
#[derive(Clone)]
pub struct TeamsHandleWrapper(pub TeamsHandle);

impl std::fmt::Debug for TeamsHandleWrapper {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("TeamsHandle")
    }
}

impl PartialEq for TeamsHandleWrapper {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}

impl Eq for TeamsHandleWrapper {}

// ---------------------------------------------------------------------------
// Rows and seams
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq)]
pub struct TeamRow {
    pub id: i32,
    pub name: String,
    pub display_name: Option<String>,
    pub readonly: bool,
    pub parent_team_id: Option<i32>,
    pub creator_id: Option<i32>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TeamMemberRow {
    pub team_id: i32,
    pub user_id: i32,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TeamMemberCursor {
    pub team_id: i32,
    pub user_id: i32,
}

/// `database.ListTeamsOpts` (already carries the SQL limit).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TeamsListOpts {
    pub cursor: i32,
    pub with_parent_id: i32,
    pub except_ancestor_id: i32,
    pub root_only: bool,
    pub search: String,
    pub for_user_member: i32,
    pub limit: i32,
}

/// `database.ListTeamMembersOpts`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TeamMembersListOpts {
    pub cursor: TeamMemberCursor,
    pub team_id: i32,
    pub search: String,
    pub limit: i32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExternalAccountRow {
    pub user_id: i32,
    pub account_id: String,
    pub login: String,
}

/// Synchronous seam over `db.Teams()` / `db.Users()` /
/// `db.UserExternalAccounts()`. Implementations bridge to sqlx with
/// `block_in_place`.
pub trait TeamsGraphqlDatabase: Send + Sync {
    /// `areTeamEndpointsAvailable`: `Err(message)` reproduces the Go error.
    fn endpoints_available(&self) -> Result<(), String>;

    fn get_team_by_id(&self, id: i32) -> Result<Option<TeamRow>, String>;
    fn get_team_by_name(&self, name: &str) -> Result<Option<TeamRow>, String>;
    fn list_teams(&self, opts: &TeamsListOpts) -> Result<(Vec<TeamRow>, i32), String>;
    fn count_teams(&self, opts: &TeamsListOpts) -> Result<i32, String>;
    fn contains_team(&self, id: i32, opts: &TeamsListOpts) -> Result<bool, String>;
    fn list_team_members(
        &self,
        opts: &TeamMembersListOpts,
    ) -> Result<(Vec<TeamMemberRow>, Option<TeamMemberCursor>), String>;
    fn count_team_members(&self, opts: &TeamMembersListOpts) -> Result<i32, String>;
    fn create_team(&self, team: &TeamRow) -> Result<TeamRow, String>;
    fn update_team(&self, team: &TeamRow) -> Result<TeamRow, String>;
    fn delete_team(&self, id: i32) -> Result<(), String>;
    fn create_team_members(&self, members: &[TeamMemberRow]) -> Result<(), String>;
    fn delete_team_members(&self, members: &[TeamMemberRow]) -> Result<(), String>;
    fn is_team_member(&self, team_id: i32, user_id: i32) -> Result<bool, String>;

    fn user_by_id(&self, id: i32) -> Result<Option<UserData>, String>;
    fn users_by_ids(&self, ids: &[i32]) -> Result<Vec<UserData>, String>;
    fn users_by_usernames(&self, usernames: &[String]) -> Result<Vec<UserData>, String>;
    fn user_by_verified_email(&self, email: &str) -> Result<Option<UserData>, String>;
    fn external_accounts(
        &self,
        service_type: &str,
        service_id: &str,
    ) -> Result<Vec<ExternalAccountRow>, String>;
}

// ---------------------------------------------------------------------------
// Relay IDs / url
// ---------------------------------------------------------------------------

pub const TEAM_KIND: &str = "Team";

/// `relay.MarshalID("Team", id)`.
pub fn marshal_team_id(id: i32) -> String {
    base64::engine::general_purpose::URL_SAFE.encode(format!("Team:{id}"))
}

/// `url.URL{Path: "/teams/" + name}.String()` for a path where only
/// non-ASCII / reserved bytes are escaped (`encodePath` keeps `/`).
fn team_url(name: &str) -> String {
    // Go's `url.URL.String()` `encodePath` leaves the unreserved set
    // (`A-Z a-z 0-9 - . _ ~`) and `/` unescaped and percent-encodes every
    // other byte of the UTF-8 encoding (uppercase hex).
    let mut encoded = String::with_capacity(name.len());
    for byte in name.bytes() {
        let keep = byte.is_ascii_alphanumeric()
            || matches!(byte, b'-' | b'.' | b'_' | b'~' | b'/');
        if keep {
            encoded.push(byte as char);
        } else {
            encoded.push_str(&format!("%{byte:02X}"));
        }
    }
    format!("/teams/{encoded}")
}

// ---------------------------------------------------------------------------
// Permission helpers
// ---------------------------------------------------------------------------

pub const ERR_NO_ACCESS_TO_TEAM: &str = "user cannot modify team";
pub const ERR_NOT_AUTHENTICATED: &str = "not authenticated";

/// `canModifyTeam`.
pub fn can_modify_team(
    handle: &dyn TeamsGraphqlDatabase,
    actor: Actor,
    team: &TeamRow,
    viewer_site_admin: bool,
) -> Result<bool, String> {
    if team.id == 0 {
        return Ok(false);
    }
    if viewer_site_admin {
        return Ok(true);
    }
    if team.readonly {
        return Ok(false);
    }
    let uid = match actor {
        Actor::User { uid } => uid,
        Actor::Anonymous | Actor::Internal => {
            return Err(ERR_NOT_AUTHENTICATED.to_owned())
        }
    };
    if team.creator_id.is_some_and(|creator| creator == uid) {
        return Ok(true);
    }
    handle.is_team_member(team.id, uid)
}

/// `findTeam`: ID or name lookup. `Ok(None)` when both are absent.
fn find_team(
    handle: &dyn TeamsGraphqlDatabase,
    id: Option<&str>,
    name: Option<&str>,
) -> Result<Option<TeamRow>, String> {
    if let Some(raw) = id {
        let team_id = unmarshal_relay_id(raw)
            .map_err(|error| format!("cannot interpret team id: {raw:?}: {error}"))?;
        return match handle.get_team_by_id(team_id)? {
            Some(team) => Ok(Some(team)),
            None => Err(format!(
                "team id={team_id} not found: team not found: teams.id = {team_id}"
            )),
        };
    }
    if let Some(name) = name {
        return match handle.get_team_by_name(name)? {
            Some(team) => Ok(Some(team)),
            None => Err(format!(
                "team name={name:?} not found: team not found: teams.name = {name:?}"
            )),
        };
    }
    Ok(None)
}

// ---------------------------------------------------------------------------
// Team object
// ---------------------------------------------------------------------------

pub struct TeamObject {
    pub handle: TeamsHandle,
    pub team: TeamRow,
    pub viewer_site_admin: bool,
}

impl TeamObject {
    fn external(&self) -> bool {
        self.team.id == 0
    }

    fn connection_args(
        &self,
        arguments: &Arguments,
        parent_id: i32,
        root_only: bool,
    ) -> Result<TeamsListOpts, String> {
        let mut opts = TeamsListOpts {
            with_parent_id: parent_id,
            root_only,
            ..TeamsListOpts::default()
        };
        if let Some(after) = arguments.string("after") {
            opts.cursor = decode_int_cursor(after)?;
        }
        if let Some(search) = arguments.string("search") {
            opts.search = search.to_owned();
        }
        if let Some(first) = arguments.i64("first") {
            opts.limit = first as i32;
        }
        Ok(opts)
    }
}

impl ObjectValue for TeamObject {
    fn type_name(&self) -> &'static str {
        "Team"
    }

    fn field(
        &self,
        field: &str,
        arguments: &Arguments,
        actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "id" => Ok(Some(FieldValue::Json(Json::Str(marshal_team_id(
                self.team.id,
            ))))),
            "name" => Ok(Some(FieldValue::Json(Json::Str(self.team.name.clone())))),
            "url" => Ok(Some(FieldValue::Json(Json::Str(if self.external() {
                String::new()
            } else {
                team_url(&self.team.name)
            })))),
            "avatarURL" => Ok(Some(FieldValue::Null)),
            "displayName" => Ok(Some(
                match self.team.display_name.as_deref().filter(|name| !name.is_empty()) {
                    Some(name) => FieldValue::Json(Json::Str(name.to_owned())),
                    None => FieldValue::Null,
                },
            )),
            "readonly" => Ok(Some(FieldValue::Json(Json::Bool(
                self.team.readonly || self.external(),
            )))),
            "external" => Ok(Some(FieldValue::Json(Json::Bool(self.external())))),
            "members" => {
                if self.external() {
                    return Err(ExecError::new("cannot get members of external team"));
                }
                let mut cursor = TeamMemberCursor::default();
                if let Some(after) = arguments.string("after") {
                    if !after.is_empty() {
                        cursor = decode_member_cursor(after)
                            .map_err(|error| ExecError::new(error))?;
                    }
                }
                let search = arguments.string("search").unwrap_or("").to_owned();
                let limit = arguments.i64("first").map(|first| first as i32).unwrap_or(0);
                Ok(Some(FieldValue::Object(Arc::new(
                    TeamMemberConnectionObject {
                        handle: self.handle.clone(),
                        team_id: self.team.id,
                        cursor,
                        search,
                        limit,
                        viewer_site_admin: self.viewer_site_admin,
                    },
                ))))
            }
            "childTeams" => {
                if self.external() {
                    return Err(ExecError::new("cannot get child teams of external team"));
                }
                let opts = self
                    .connection_args(arguments, self.team.id, false)
                    .map_err(ExecError::new)?;
                Ok(Some(FieldValue::Object(Arc::new(TeamConnectionObject {
                    handle: self.handle.clone(),
                    opts,
                    viewer_site_admin: self.viewer_site_admin,
                }))))
            }
            "parentTeam" => match self.team.parent_team_id.filter(|id| *id != 0) {
                None => Ok(Some(FieldValue::Null)),
                Some(parent_id) => match self.handle.get_team_by_id(parent_id) {
                    Ok(Some(parent)) => Ok(Some(FieldValue::Object(Arc::new(TeamObject {
                        handle: self.handle.clone(),
                        team: parent,
                        viewer_site_admin: self.viewer_site_admin,
                    })))),
                    Ok(None) => Err(ExecError::new(format!(
                        "team id={parent_id} not found: team not found: teams.id = {parent_id}"
                    ))),
                    Err(message) => Err(ExecError::new(message)),
                },
            },
            "viewerCanAdminister" => {
                let allowed = can_modify_team(
                    self.handle.as_ref(),
                    actor,
                    &self.team,
                    self.viewer_site_admin,
                )
                .map_err(ExecError::new)?;
                Ok(Some(FieldValue::Json(Json::Bool(allowed))))
            }
            "creator" => match self.team.creator_id.filter(|id| *id != 0) {
                None => Ok(Some(FieldValue::Null)),
                Some(creator_id) => match self.handle.user_by_id(creator_id) {
                    Ok(Some(user)) => Ok(Some(FieldValue::Object(Arc::new(
                        UserObject::from_data_as_viewer(user, self.viewer_site_admin)
                            .with_teams(Some(self.handle.clone())),
                    )))),
                    Ok(None) => Err(ExecError::new(format!(
                        "user not found: [{creator_id}]"
                    ))),
                    Err(message) => Err(ExecError::new(message)),
                },
            },
            _ => Ok(None),
        }
    }
}

// ---------------------------------------------------------------------------
// Connections
// ---------------------------------------------------------------------------

pub struct TeamConnectionObject {
    pub handle: TeamsHandle,
    pub opts: TeamsListOpts,
    pub viewer_site_admin: bool,
}

impl TeamConnectionObject {
    fn list(&self) -> Result<(Vec<TeamRow>, i32), String> {
        let mut opts = self.opts.clone();
        opts.limit = if opts.limit > 0 { opts.limit + 1 } else { 0 };
        self.handle.list_teams(&opts)
    }

    fn page_info(&self) -> Result<PageInfoObject, String> {
        let (_, next) = self.list()?;
        if next > 0 {
            Ok(PageInfoObject::new(true, Some(encode_int_cursor(next))))
        } else {
            Ok(PageInfoObject::new(false, None))
        }
    }
}

impl ObjectValue for TeamConnectionObject {
    fn type_name(&self) -> &'static str {
        "TeamConnection"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            // `TotalCount` ignores the cursor, limit and `ExceptAncestorID`.
            "totalCount" => {
                let mut opts = self.opts.clone();
                opts.cursor = 0;
                opts.limit = 0;
                opts.except_ancestor_id = 0;
                let count = self
                    .handle
                    .count_teams(&opts)
                    .map_err(ExecError::new)?;
                Ok(Some(FieldValue::Json(Json::Int(i64::from(count)))))
            }
            "pageInfo" => Ok(Some(FieldValue::Object(Arc::new(
                self.page_info().map_err(ExecError::new)?,
            )))),
            "nodes" => {
                let (teams, _) = self.list().map_err(ExecError::new)?;
                Ok(Some(FieldValue::List {
                    element_type: Some("Team"),
                    items: teams
                        .into_iter()
                        .map(|team| {
                            FieldValue::Object(Arc::new(TeamObject {
                                handle: self.handle.clone(),
                                team,
                                viewer_site_admin: self.viewer_site_admin,
                            }))
                        })
                        .collect(),
                }))
            }
            _ => Ok(None),
        }
    }
}

pub struct TeamMemberConnectionObject {
    pub handle: TeamsHandle,
    pub team_id: i32,
    pub cursor: TeamMemberCursor,
    pub search: String,
    pub limit: i32,
    pub viewer_site_admin: bool,
}

impl TeamMemberConnectionObject {
    fn list(&self) -> Result<(Vec<TeamMemberRow>, Option<TeamMemberCursor>), String> {
        let opts = TeamMembersListOpts {
            cursor: self.cursor,
            team_id: self.team_id,
            search: self.search.clone(),
            limit: if self.limit > 0 { self.limit + 1 } else { 0 },
        };
        self.handle.list_team_members(&opts)
    }

    fn page_info(&self) -> Result<PageInfoObject, String> {
        let (_, next) = self.list()?;
        match next {
            Some(cursor) => Ok(PageInfoObject::new(
                true,
                Some(encode_member_cursor(cursor)),
            )),
            None => Ok(PageInfoObject::new(false, None)),
        }
    }
}

impl ObjectValue for TeamMemberConnectionObject {
    fn type_name(&self) -> &'static str {
        "TeamMemberConnection"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "totalCount" => {
                let opts = TeamMembersListOpts {
                    cursor: TeamMemberCursor::default(),
                    team_id: self.team_id,
                    search: self.search.clone(),
                    limit: 0,
                };
                let count = self
                    .handle
                    .count_team_members(&opts)
                    .map_err(ExecError::new)?;
                Ok(Some(FieldValue::Json(Json::Int(i64::from(count)))))
            }
            "pageInfo" => Ok(Some(FieldValue::Object(Arc::new(
                self.page_info().map_err(ExecError::new)?,
            )))),
            "nodes" => {
                let (members, _) = self.list().map_err(ExecError::new)?;
                let mut items = Vec::with_capacity(members.len());
                for member in members {
                    if member.user_id == 0 {
                        items.push(FieldValue::Null);
                        continue;
                    }
                    match self.handle.user_by_id(member.user_id) {
                        Ok(Some(user)) => items.push(FieldValue::Object(Arc::new(
                            UserObject::from_data_as_viewer(user, self.viewer_site_admin)
                                .with_teams(Some(self.handle.clone())),
                        ))),
                        Ok(None) => {
                            return Err(ExecError::new(format!(
                                "user not found: [{}]",
                                member.user_id
                            )))
                        }
                        Err(message) => return Err(ExecError::new(message)),
                    }
                }
                Ok(Some(FieldValue::List {
                    element_type: Some("TeamMember"),
                    items,
                }))
            }
            _ => Ok(None),
        }
    }
}

// ---------------------------------------------------------------------------
// Cursors
// ---------------------------------------------------------------------------

/// `gqlutil.EncodeCursor(&json.Marshal(teamMemberListCursor))`.
pub fn encode_member_cursor(cursor: TeamMemberCursor) -> String {
    let json = member_cursor_json(cursor);
    base64::engine::general_purpose::STANDARD.encode(json)
}

fn member_cursor_json(cursor: TeamMemberCursor) -> String {
    let mut entries = Vec::new();
    if cursor.team_id != 0 {
        entries.push(format!("\"team\":{}", cursor.team_id));
    }
    if cursor.user_id != 0 {
        entries.push(format!("\"user\":{}", cursor.user_id));
    }
    format!("{{{}}}", entries.join(","))
}

/// `gqlutil.DecodeCursor` then `json.Unmarshal`.
pub fn decode_member_cursor(value: &str) -> Result<TeamMemberCursor, String> {
    let decoded = base64::engine::general_purpose::STANDARD
        .decode(value)
        .map_err(|error| error.to_string())?;
    let text = String::from_utf8(decoded).map_err(|error| error.to_string())?;
    let parsed: serde_json::Value =
        serde_json::from_str(&text).map_err(|error| error.to_string())?;
    Ok(TeamMemberCursor {
        team_id: parsed
            .get("team")
            .and_then(serde_json::Value::as_i64)
            .unwrap_or(0) as i32,
        user_id: parsed
            .get("user")
            .and_then(serde_json::Value::as_i64)
            .unwrap_or(0) as i32,
    })
}

// ---------------------------------------------------------------------------
// Root fields
// ---------------------------------------------------------------------------

pub fn is_root_field(field: &str) -> bool {
    matches!(field, "team" | "teams")
}

pub fn root_field(
    handle: &TeamsHandle,
    field: &str,
    arguments: &Arguments,
    viewer_site_admin: bool,
) -> Result<Option<FieldValue>, ExecError> {
    handle
        .endpoints_available()
        .map_err(ExecError::new)?;
    match field {
        // `schemaResolver.Team`.
        "team" => {
            let Some(name) = arguments.string("name") else {
                return Ok(None);
            };
            match handle.get_team_by_name(name).map_err(ExecError::new)? {
                None => Ok(Some(FieldValue::Null)),
                Some(team) => Ok(Some(FieldValue::Object(Arc::new(TeamObject {
                    handle: handle.clone(),
                    team,
                    viewer_site_admin,
                })))),
            }
        }
        // `schemaResolver.Teams`.
        "teams" => {
            let mut opts = TeamsListOpts {
                root_only: !arguments.bool("includeChildTeams").unwrap_or(false),
                ..TeamsListOpts::default()
            };
            if let Some(after) = arguments.string("after") {
                opts.cursor = decode_int_cursor(after).map_err(ExecError::new)?;
            }
            if let Some(search) = arguments.string("search") {
                opts.search = search.to_owned();
            }
            if let Some(first) = arguments.i64("first") {
                opts.limit = first as i32;
            }
            if let Some(raw) = arguments.string("exceptAncestor") {
                opts.except_ancestor_id = unmarshal_relay_id(raw).map_err(|error| {
                    ExecError::new(format!(
                        "cannot interpret exceptAncestor id: {raw:?}: {error}"
                    ))
                })?;
            }
            Ok(Some(FieldValue::Object(Arc::new(TeamConnectionObject {
                handle: handle.clone(),
                opts,
                viewer_site_admin,
            }))))
        }
        _ => Ok(None),
    }
}

pub fn is_node_kind(kind: &str) -> bool {
    kind == TEAM_KIND
}

/// `teamByID` / `findTeam(... GetTeamByID)`.
pub fn node_field(
    handle: &TeamsHandle,
    id: &str,
    viewer_site_admin: bool,
) -> Result<Option<FieldValue>, ExecError> {
    handle
        .endpoints_available()
        .map_err(ExecError::new)?;
    let team_id = unmarshal_relay_id(id)
        .map_err(|error| ExecError::new(format!("cannot interpret team id: {id:?}: {error}")))?;
    match handle.get_team_by_id(team_id).map_err(ExecError::new)? {
        Some(team) => Ok(Some(FieldValue::Object(Arc::new(TeamObject {
            handle: handle.clone(),
            team,
            viewer_site_admin,
        })))),
        None => Err(ExecError::new(format!(
            "team id={team_id} not found: team not found: teams.id = {team_id}"
        ))),
    }
}

/// `UserResolver.Teams`: gate, then a root-only `TeamConnection`. The Go
/// resolver never sets `ForUserMember`, so membership is ignored (errata).
pub fn user_teams(
    handle: &TeamsHandle,
    arguments: &Arguments,
    viewer_site_admin: bool,
) -> Result<Option<FieldValue>, ExecError> {
    handle
        .endpoints_available()
        .map_err(ExecError::new)?;
    let mut opts = TeamsListOpts {
        root_only: true,
        ..TeamsListOpts::default()
    };
    if let Some(after) = arguments.string("after") {
        opts.cursor = decode_int_cursor(after).map_err(ExecError::new)?;
    }
    if let Some(search) = arguments.string("search") {
        opts.search = search.to_owned();
    }
    if let Some(first) = arguments.i64("first") {
        opts.limit = first as i32;
    }
    Ok(Some(FieldValue::Object(Arc::new(TeamConnectionObject {
        handle: handle.clone(),
        opts,
        viewer_site_admin,
    }))))
}

// ---------------------------------------------------------------------------
// Mutations
// ---------------------------------------------------------------------------

pub fn is_mutation_field(field: &str) -> bool {
    matches!(
        field,
        "createTeam"
            | "updateTeam"
            | "deleteTeam"
            | "addTeamMembers"
            | "setTeamMembers"
            | "removeTeamMembers"
    )
}

/// `TeamMemberInput`.
#[derive(Debug, Clone, Default)]
pub struct TeamMemberInput {
    pub user_id: Option<String>,
    pub username: Option<String>,
    pub email: Option<String>,
    pub external_account_service_id: Option<String>,
    pub external_account_service_type: Option<String>,
    pub external_account_account_id: Option<String>,
    pub external_account_login: Option<String>,
}

impl TeamMemberInput {
    fn parse(value: &InputValue) -> Option<Self> {
        let InputValue::Object(map) = value else {
            return None;
        };
        let string = |key: &str| map.get(key).and_then(InputValue::as_str).map(str::to_owned);
        Some(Self {
            user_id: string("userID"),
            username: string("username"),
            email: string("email"),
            external_account_service_id: string("externalAccountServiceID"),
            external_account_service_type: string("externalAccountServiceType"),
            external_account_account_id: string("externalAccountAccountID"),
            external_account_login: string("externalAccountLogin"),
        })
    }
}

impl std::fmt::Display for TeamMemberInput {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut conds = Vec::new();
        if let Some(id) = &self.user_id {
            conds.push(format!("ID={id}"));
        }
        if let Some(username) = &self.username {
            conds.push(format!("Username={username}"));
        }
        if let Some(email) = &self.email {
            conds.push(format!("Email={email}"));
        }
        if let Some(service_id) = &self.external_account_service_id {
            conds.push(format!(
                "ExternalAccount(ServiceID={}, ServiceType={}, AccountID={}, Login={})",
                service_id,
                self.external_account_service_type.as_deref().unwrap_or(""),
                self.external_account_account_id.as_deref().unwrap_or(""),
                self.external_account_login.as_deref().unwrap_or("")
            ));
        }
        write!(f, "team member ({})", conds.join(","))
    }
}

fn parse_members(arguments: &Arguments) -> Vec<TeamMemberInput> {
    match arguments.get("members") {
        Some(InputValue::List(items)) => items.iter().filter_map(TeamMemberInput::parse).collect(),
        _ => Vec::new(),
    }
}

/// The result of `usersForTeamMembers`: matched users plus the inputs that did
/// not match any predicate.
struct MatchedUsers {
    users: Vec<UserData>,
    not_found: Vec<TeamMemberInput>,
}

/// `usersForTeamMembers` (`teams.go:875-977`): precedence ID → username →
/// verified email → external account. An ID/username that finds no row is
/// silently dropped; email lookup errors propagate; unmatched external-account
/// inputs remain in `not_found`.
fn users_for_team_members(
    handle: &dyn TeamsGraphqlDatabase,
    inputs: Vec<TeamMemberInput>,
) -> Result<MatchedUsers, String> {
    let mut remaining: Vec<TeamMemberInput> = Vec::new();
    let mut users: Vec<UserData> = Vec::new();

    // IDs.
    let mut ids = Vec::new();
    for member in inputs {
        if let Some(raw_id) = &member.user_id {
            let id = unmarshal_relay_id(raw_id)?;
            ids.push(id);
        } else {
            remaining.push(member);
        }
    }
    if !ids.is_empty() {
        users.extend(handle.users_by_ids(&ids)?);
    }

    // Usernames.
    let mut next: Vec<TeamMemberInput> = Vec::new();
    let mut usernames = Vec::new();
    for member in remaining {
        if let Some(username) = &member.username {
            usernames.push(username.clone());
        } else {
            next.push(member);
        }
    }
    remaining = next;
    if !usernames.is_empty() {
        users.extend(handle.users_by_usernames(&usernames)?);
    }

    // Verified email.
    next = Vec::new();
    for member in remaining {
        if let Some(email) = &member.email {
            match handle.user_by_verified_email(email)? {
                Some(user) => users.push(user),
                None => {
                    return Err(format!("user not found by verified email: {email}"));
                }
            }
        } else {
            next.push(member);
        }
    }
    remaining = next;

    // External account.
    next = Vec::new();
    for member in remaining {
        let (Some(service_id), Some(service_type)) = (
            member.external_account_service_id.as_deref(),
            member.external_account_service_type.as_deref(),
        ) else {
            next.push(member);
            continue;
        };
        let accounts = handle.external_accounts(service_type, service_id)?;
        let matched = if let Some(account_id) = &member.external_account_account_id {
            accounts.iter().find(|account| &account.account_id == account_id)
        } else if let Some(login) = &member.external_account_login {
            accounts.iter().find(|account| &account.login == login)
        } else {
            None
        };
        match matched {
            Some(account) => match handle.user_by_id(account.user_id)? {
                Some(user) => users.push(user),
                None => next.push(member),
            },
            None => next.push(member),
        }
    }

    Ok(MatchedUsers {
        users,
        not_found: next,
    })
}

fn team_identified(
    arguments: &Arguments,
    verb: &str,
) -> Result<(Option<String>, Option<String>), ExecError> {
    let id = arguments.string("team").map(str::to_owned);
    let name = arguments.string("teamName").map(str::to_owned);
    if id.is_none() && name.is_none() {
        return Err(ExecError::new(format!(
            "team must be identified by either id (team parameter) or name (teamName parameter), none specified"
        )));
    }
    if id.is_some() && name.is_some() {
        return Err(ExecError::new(
            "team must be identified by either id (team parameter) or name (teamName parameter), both specified",
        ));
    }
    let _ = verb;
    Ok((id, name))
}

fn require_team(
    handle: &TeamsHandle,
    id: Option<&str>,
    name: Option<&str>,
) -> Result<TeamRow, ExecError> {
    match find_team(handle.as_ref(), id, name) {
        Ok(Some(team)) => Ok(team),
        Ok(None) => Err(ExecError::new("team not found")),
        Err(message) => Err(ExecError::new(message)),
    }
}

fn require_modify(
    handle: &TeamsHandle,
    actor: Actor,
    team: &TeamRow,
    viewer_site_admin: bool,
) -> Result<(), ExecError> {
    match can_modify_team(handle.as_ref(), actor, team, viewer_site_admin) {
        Ok(true) => Ok(()),
        Ok(false) => Err(ExecError::new(ERR_NO_ACCESS_TO_TEAM)),
        Err(message) => Err(ExecError::new(message)),
    }
}

fn team_value(handle: &TeamsHandle, team: TeamRow, viewer_site_admin: bool) -> FieldValue {
    FieldValue::Object(Arc::new(TeamObject {
        handle: handle.clone(),
        team,
        viewer_site_admin,
    }))
}

pub fn mutation_field(
    handle: &TeamsHandle,
    field: &str,
    arguments: &Arguments,
    actor: Actor,
    viewer_site_admin: bool,
) -> Result<Option<FieldValue>, ExecError> {
    handle
        .endpoints_available()
        .map_err(ExecError::new)?;
    match field {
        // `schemaResolver.CreateTeam`.
        "createTeam" => {
            let readonly = arguments.bool("readonly").unwrap_or(false);
            if readonly && !viewer_site_admin {
                return Err(ExecError::new(
                    "only site admins can create read-only teams",
                ));
            }
            let parent = arguments.string("parentTeam");
            let parent_name = arguments.string("parentTeamName");
            if parent.is_some() && parent_name.is_some() {
                return Err(ExecError::new(
                    "must specify at most one: ParentTeam or ParentTeamName",
                ));
            }
            let parent_team = match find_team(handle.as_ref(), parent, parent_name) {
                Ok(team) => team,
                Err(message) => return Err(ExecError::new(format!("parent team: {message}"))),
            };
            let mut team = TeamRow {
                id: 0,
                name: arguments.string("name").unwrap_or("").to_owned(),
                display_name: arguments.string("displayName").map(str::to_owned),
                readonly,
                parent_team_id: None,
                creator_id: match actor {
                    Actor::User { uid } => Some(uid),
                    Actor::Anonymous | Actor::Internal => Some(0),
                },
            };
            if let Some(parent_team) = &parent_team {
                team.parent_team_id = Some(parent_team.id);
                require_modify(handle, actor, parent_team, viewer_site_admin)?;
            }
            let created = handle.create_team(&team).map_err(ExecError::new)?;
            Ok(Some(team_value(handle, created, viewer_site_admin)))
        }
        // `schemaResolver.UpdateTeam`.
        "updateTeam" => {
            let id = arguments.string("id").map(str::to_owned);
            let name = arguments.string("name").map(str::to_owned);
            if id.is_none() && name.is_none() {
                return Err(ExecError::new(
                    "team to update is identified by either id or name, but neither was specified",
                ));
            }
            if id.is_some() && name.is_some() {
                return Err(ExecError::new(
                    "team to update is identified by either id or name, but both were specified",
                ));
            }
            let parent = arguments.string("parentTeam");
            let parent_name = arguments.string("parentTeamName");
            let make_root = arguments.bool("makeRoot");
            if (parent.is_some() || parent_name.is_some()) && make_root.is_some() {
                return Err(ExecError::new(
                    "specifying a parent team contradicts making a team root (no parent team)",
                ));
            }
            if parent.is_some() && parent_name.is_some() {
                return Err(ExecError::new(
                    "parent team is identified by either id or name, but both were specified",
                ));
            }
            if make_root == Some(false) {
                return Err(ExecError::new(
                    "the only possible value for makeRoot is true (if set); to assign a parent team please use parentTeam or parentTeamName parameters",
                ));
            }
            let mut team = require_team(handle, id.as_deref(), name.as_deref())?;
            require_modify(handle, actor, &team, viewer_site_admin)?;
            let mut needs_update = false;
            if let Some(display_name) = arguments.string("displayName") {
                if team.display_name.as_deref().unwrap_or("") != display_name {
                    needs_update = true;
                    team.display_name = Some(display_name.to_owned());
                }
            }
            if parent.is_some() || parent_name.is_some() {
                let parent_team = match find_team(handle.as_ref(), parent, parent_name) {
                    Ok(team) => team,
                    Err(message) => {
                        return Err(ExecError::new(format!(
                            "cannot find parent team: {message}"
                        )))
                    }
                };
                let parent_team = parent_team.ok_or_else(|| {
                    ExecError::new("cannot find parent team")
                })?;
                if Some(parent_team.id) != team.parent_team_id {
                    let contains = handle
                        .contains_team(
                            parent_team.id,
                            &TeamsListOpts {
                                except_ancestor_id: team.id,
                                ..TeamsListOpts::default()
                            },
                        )
                        .map_err(|error| {
                            ExecError::new(format!(
                                "could not determine ancestorship on team update: {error}"
                            ))
                        })?;
                    if !contains {
                        return Err(ExecError::new(format!(
                            "circular dependency: new parent {:?} is descendant of updated team {:?}",
                            parent_team.name, team.name
                        )));
                    }
                    needs_update = true;
                    team.parent_team_id = Some(parent_team.id);
                }
            }
            if make_root == Some(true) && team.parent_team_id.unwrap_or(0) != 0 {
                needs_update = true;
                team.parent_team_id = None;
            }
            if needs_update {
                team = handle.update_team(&team).map_err(ExecError::new)?;
            }
            Ok(Some(team_value(handle, team, viewer_site_admin)))
        }
        // `schemaResolver.DeleteTeam`.
        "deleteTeam" => {
            let id = arguments.string("id").map(str::to_owned);
            let name = arguments.string("name").map(str::to_owned);
            if id.is_none() && name.is_none() {
                return Err(ExecError::new(
                    "team to delete is identified by either id or name, but neither was specified",
                ));
            }
            if id.is_some() && name.is_some() {
                return Err(ExecError::new(
                    "team to delete is identified by either id or name, but both were specified",
                ));
            }
            let team = require_team(handle, id.as_deref(), name.as_deref())?;
            require_modify(handle, actor, &team, viewer_site_admin)?;
            handle.delete_team(team.id).map_err(ExecError::new)?;
            Ok(Some(FieldValue::Object(Arc::new(EmptyResponseObject))))
        }
        "addTeamMembers" | "setTeamMembers" | "removeTeamMembers" => {
            let (id, name) = team_identified(arguments, field)?;
            let inputs = parse_members(arguments);
            let matched = users_for_team_members(handle.as_ref(), inputs)
                .map_err(ExecError::new)?;
            let skip_unmatched = arguments.bool("skipUnmatchedMembers").unwrap_or(false);
            if !matched.not_found.is_empty() && !skip_unmatched {
                let message = matched
                    .not_found
                    .iter()
                    .map(|member| format!("member not found: {member}"))
                    .collect::<Vec<_>>()
                    .join("\n");
                return Err(ExecError::new(message));
            }
            let team = require_team(handle, id.as_deref(), name.as_deref())?;
            require_modify(handle, actor, &team, viewer_site_admin)?;
            match field {
                "addTeamMembers" => {
                    let members: Vec<TeamMemberRow> = matched
                        .users
                        .iter()
                        .map(|user| TeamMemberRow {
                            team_id: team.id,
                            user_id: user.id,
                        })
                        .collect();
                    handle.create_team_members(&members).map_err(ExecError::new)?;
                }
                "removeTeamMembers" => {
                    let members: Vec<TeamMemberRow> = matched
                        .users
                        .iter()
                        .map(|user| TeamMemberRow {
                            team_id: team.id,
                            user_id: user.id,
                        })
                        .collect();
                    handle.delete_team_members(&members).map_err(ExecError::new)?;
                }
                "setTeamMembers" => {
                    let wanted: std::collections::HashSet<i32> =
                        matched.users.iter().map(|user| user.id).collect();
                    let mut remove = Vec::new();
                    let mut cursor = TeamMemberCursor::default();
                    loop {
                        let (existing, next) = handle
                            .list_team_members(&TeamMembersListOpts {
                                cursor,
                                team_id: team.id,
                                search: String::new(),
                                limit: 0,
                            })
                            .map_err(ExecError::new)?;
                        for member in &existing {
                            if !wanted.contains(&member.user_id) {
                                remove.push(TeamMemberRow {
                                    team_id: team.id,
                                    user_id: member.user_id,
                                });
                            }
                        }
                        match next {
                            Some(next) => cursor = next,
                            None => break,
                        }
                    }
                    let add: Vec<TeamMemberRow> = matched
                        .users
                        .iter()
                        .map(|user| TeamMemberRow {
                            team_id: team.id,
                            user_id: user.id,
                        })
                        .collect();
                    if !remove.is_empty() {
                        handle.delete_team_members(&remove).map_err(ExecError::new)?;
                    }
                    if !add.is_empty() {
                        handle.create_team_members(&add).map_err(ExecError::new)?;
                    }
                }
                _ => unreachable!("matched mutation field"),
            }
            Ok(Some(team_value(handle, team, viewer_site_admin)))
        }
        _ => Ok(None),
    }
}

/// `EmptyResponse` renders `{alwaysNil: null}`.
pub struct EmptyResponseObject;

impl ObjectValue for EmptyResponseObject {
    fn type_name(&self) -> &'static str {
        "EmptyResponse"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "alwaysNil" => Ok(Some(FieldValue::Null)),
            _ => Ok(None),
        }
    }
}

