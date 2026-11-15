#![forbid(unsafe_code)]

//! Own ownership GraphQL surface (M9.48c).
//!
//! Ports `cmd/frontend/graphqlbackend/own.graphql` and
//! `cmd/frontend/internal/own/resolvers/*`:
//!
//! - `Query.codeownersIngestedFiles` / `Query.ownSignalConfigurations` /
//!   `Query.instanceOwnershipStats`;
//! - the nine CODEOWNERS/assigned-ownership/config mutations;
//! - `CodeownersIngestedFile` (+ connection) and `Repository.ingestedCodeowners`;
//! - `GitBlob`/`GitTree`/`GitCommit.ownership`, `GitTree.ownershipStats`;
//! - the `OwnershipConnection`/`Ownership`/`Owner`/`OwnershipReason` unions
//!   with the baseline ordering, pagination and reason filtering.
//!
//! Every operation reuses an existing table (`codeowners`, `assigned_owners`,
//! `assigned_teams`, `repo_paths`, `ownership_path_stats`,
//! `own_signal_configurations`, `own_aggregate_recent_view`,
//! `own_aggregate_recent_contribution`, `repo`, `users`, `teams`,
//! `permissions`/`role_permissions`/`user_roles`). No table, column or Redis
//! key is added.
//!
//! The database/gitserver seam is synchronous (like the saved-search surface):
//! the production adapter bridges async sqlx/gRPC with `block_in_place`.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, OnceLock};

use base64::Engine as _;
use sg_graphql::exec::{Actor, Arguments, ExecError, FieldValue, InputValue, ObjectValue};
use sg_graphql::json::Json;
use time::OffsetDateTime;

use crate::search_stream::convert::MinimalRepo;
use crate::graphql_resolvers::{
    datetime_rfc3339, unmarshal_relay_id, RepositoryData, RepositoryObject, UserData, UserObject,
};
use crate::search_owners::{parse_codeowners, AssignedOwners, AssignedTeams, Ruleset};

/// `resolvers.codeownersIngestedFileKind`.
pub const CODEOWNERS_INGESTED_FILE_KIND: &str = "CodeownersIngestedFile";

/// `own/types.SignalRecentContributors`.
pub const SIGNAL_RECENT_CONTRIBUTORS: &str = "recent-contributors";
/// `own/types.SignalRecentViews`.
pub const SIGNAL_RECENT_VIEWS: &str = "recent-views";

/// `rbac.OwnershipAssignPermission`.
pub const OWNERSHIP_ASSIGN_PERMISSION: &str = "OWNERSHIP#ASSIGN";

// ---------------------------------------------------------------------------
// Rows and fixtures
// ---------------------------------------------------------------------------

/// One `codeowners` row (`types.CodeownersFile`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CodeownersFileRow {
    pub repo_id: i32,
    pub contents: String,
    pub created_at: OffsetDateTime,
    pub updated_at: OffsetDateTime,
}

/// `database.RecentViewSummary`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecentView {
    pub user_id: i32,
    pub views_count: i32,
}

/// `database.RecentContributorSummary` reduced to the fields the resolver
/// reads.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecentAuthor {
    pub author_email: String,
    pub contribution_count: i32,
}

/// `database.PathAggregateCounts` + the aggregate file count.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct OwnStatsCounts {
    pub codeowned: i32,
    pub assigned: i32,
    pub total_owned: i32,
    pub updated_at: Option<OffsetDateTime>,
}

/// A `teams` row (`types.Team`) as the `Owner` union exposes it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OwnTeamRow {
    pub id: i32,
    pub name: String,
    pub display_name: Option<String>,
    pub readonly: bool,
}

/// `codeowners.RulesetSource`: where the matched ruleset came from.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RulesetSource {
    Ingested { repo_id: i32 },
    Git {
        repo_id: i32,
        commit: String,
        path: String,
    },
}

/// A ruleset plus its source.
#[derive(Debug, Clone)]
pub struct OwnRuleset {
    pub ruleset: Ruleset,
    pub source: RulesetSource,
}

/// One `own.Reference`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct OwnReference {
    user_id: Option<i32>,
    team_id: Option<i32>,
    handle: String,
    email: String,
}

impl OwnReference {
    fn from_owner(owner: &crate::search_leaf_jobs::OwnershipRef) -> Self {
        Self {
            user_id: owner.user_id,
            team_id: owner.team_id,
            handle: owner.handle.clone(),
            email: owner.email.clone(),
        }
    }
}

// ---------------------------------------------------------------------------
// Synchronous DB/gitserver seam
// ---------------------------------------------------------------------------

/// The own GraphQL seam. `Ok(None)` means the baseline's not-found path.
pub trait OwnGraphqlDatabase: Send + Sync {
    /// `dotcom.SourcegraphDotComMode()`.
    fn sourcegraph_dotcom_mode(&self) -> bool;

    /// `auth.CurrentUser`: `None` for anonymous/internal/missing users.
    fn current_user(&self, actor: Actor) -> Result<Option<UserData>, String>;

    /// `rbac.CheckGivenUserHasPermission(user, OWNERSHIP#ASSIGN)`:
    /// `Ok(())` when permitted, otherwise the baseline error text.
    fn check_ownership_assign_permission(&self, actor: Actor) -> Result<(), String>;

    fn load_signal_configurations(
        &self,
    ) -> Result<Vec<sg_db::own_signal_configurations::SignalConfiguration>, String>;
    fn update_signal_configuration(
        &self,
        name: &str,
        excluded_repo_patterns: &[String],
        enabled: bool,
    ) -> Result<(), String>;
    fn signal_enabled(&self, name: &str) -> Result<bool, String>;

    fn recent_authors(&self, repo_id: i32, path: &str) -> Result<Vec<RecentAuthor>, String>;
    fn recent_views(&self, repo_id: i32, path: &str) -> Result<Vec<RecentView>, String>;

    fn repo_by_id(&self, actor: Actor, id: i32) -> Result<Option<RepositoryData>, String>;
    fn repo_by_name(&self, actor: Actor, name: &str) -> Result<Option<RepositoryData>, String>;

    fn ruleset(
        &self,
        repo_id: i32,
        repo_name: &str,
        commit: &str,
    ) -> Result<Option<OwnRuleset>, String>;
    fn assigned_owners(&self, repo_id: i32) -> Result<AssignedOwners, String>;
    fn assigned_teams(&self, repo_id: i32) -> Result<AssignedTeams, String>;

    fn user_by_id(&self, id: i32) -> Result<Option<UserData>, String>;
    fn user_by_username(&self, username: &str) -> Result<Option<UserData>, String>;
    fn user_by_verified_email(&self, email: &str) -> Result<Option<UserData>, String>;
    fn team_by_id(&self, id: i32) -> Result<Option<OwnTeamRow>, String>;
    fn team_by_name(&self, name: &str) -> Result<Option<OwnTeamRow>, String>;

    /// `OwnershipStats().QueryAggregateCounts`: `None` location is the
    /// instance-wide query (`Path=""`, no repo filter).
    fn ownership_stats(&self, location: Option<(i32, String)>)
        -> Result<OwnStatsCounts, String>;
    /// `RepoPaths().AggregateFileCount`.
    fn total_files(&self, location: Option<(i32, String)>) -> Result<i32, String>;

    fn list_codeowners(
        &self,
        cursor: i32,
        limit: Option<i32>,
    ) -> Result<(Vec<CodeownersFileRow>, i32), String>;
    fn count_codeowners(&self) -> Result<i32, String>;
    fn codeowners_for_repo(&self, repo_id: i32) -> Result<Option<CodeownersFileRow>, String>;
    fn create_codeowners(
        &self,
        repo_id: i32,
        contents: &str,
        proto: &[u8],
    ) -> Result<CodeownersFileRow, String>;
    fn update_codeowners(
        &self,
        repo_id: i32,
        contents: &str,
        proto: &[u8],
    ) -> Result<CodeownersFileRow, String>;
    fn delete_codeowners(&self, repo_ids: &[i32]) -> Result<(), String>;

    fn insert_assigned_owner(
        &self,
        user_id: i32,
        repo_id: i32,
        path: &str,
        who: i32,
    ) -> Result<(), String>;
    fn delete_assigned_owner(
        &self,
        user_id: i32,
        repo_id: i32,
        path: &str,
    ) -> Result<(), String>;
    fn insert_assigned_team(
        &self,
        team_id: i32,
        repo_id: i32,
        path: &str,
        who: i32,
    ) -> Result<(), String>;
    fn delete_assigned_team(
        &self,
        team_id: i32,
        repo_id: i32,
        path: &str,
    ) -> Result<(), String>;
}

/// The injected handle.
pub type OwnHandle = Arc<dyn OwnGraphqlDatabase + Send + Sync>;

// ---------------------------------------------------------------------------
// Relay / cursor helpers
// ---------------------------------------------------------------------------

/// `relay.MarshalID(kind, id)`.
pub fn marshal_relay_int_id(kind: &str, id: i32) -> String {
    base64::engine::general_purpose::URL_SAFE.encode(format!("{kind}:{id}"))
}

/// `gqlutil.EncodeCursor`: base64 **std** of the raw cursor string.
fn encode_cursor(value: &str) -> String {
    base64::engine::general_purpose::STANDARD.encode(value)
}

/// `gqlutil.DecodeCursor`: empty input is "no cursor".
fn decode_cursor(value: Option<&str>) -> Result<String, ExecError> {
    let Some(value) = value else {
        return Ok(String::new());
    };
    let decoded = base64::engine::general_purpose::STANDARD
        .decode(value)
        .map_err(|error| ExecError::new(error.to_string()))?;
    Ok(String::from_utf8_lossy(&decoded).into_owned())
}

/// `gqlutil.EncodeIntCursor` (used by the CODEOWNERS list connection).
fn encode_int_cursor(value: Option<i32>) -> (Option<String>, bool) {
    match value {
        Some(value) => (Some(encode_cursor(&value.to_string())), true),
        None => (None, false),
    }
}

/// Go's `int32 -> string` conversion (a single rune) followed by
/// `gqlutil.DecodeIntCursor`. The own SDL declares `after: Int`, but the Go
/// resolver stores `*string`; graph-gophers converts the integer scalar to a
/// string through `reflect.Value.Convert`, i.e. exactly this rune mapping.
fn decode_int_cursor_from_argument(value: Option<i64>) -> Result<i32, ExecError> {
    let Some(value) = value else {
        return Ok(0);
    };
    let rune = u32::try_from(value).ok().and_then(char::from_u32).unwrap_or('\u{fffd}');
    let encoded = rune.to_string();
    let decoded = base64::engine::general_purpose::STANDARD
        .decode(encoded.as_bytes())
        .map_err(|error| ExecError::new(error.to_string()))?;
    let text = String::from_utf8_lossy(&decoded);
    if text.is_empty() {
        return Ok(0);
    }
    text.parse::<i32>()
        .map_err(|error| ExecError::new(error.to_string()))
}

// ---------------------------------------------------------------------------
// Ownership aggregation (`internal/own/resolvers/ownershipConnection`)
// ---------------------------------------------------------------------------

/// `codeowners.ResolvedOwner` as the GraphQL `Owner` union needs it.
#[derive(Debug, Clone)]
enum ResolvedOwner {
    Person {
        user: Option<UserData>,
        handle: String,
        email: String,
        primary_email: Option<String>,
    },
    Team {
        team: OwnTeamRow,
    },
}

impl ResolvedOwner {
    fn identifier(&self) -> String {
        match self {
            Self::Person {
                handle, email, ..
            } => format!("{handle}{email}"),
            Self::Team { team } => team.name.clone(),
        }
    }
}

fn person_from_user(user: &UserData) -> ResolvedOwner {
    ResolvedOwner::Person {
        user: Some(user.clone()),
        handle: user.username.clone(),
        // `FindResolved` sets `Handle` to the username and leaves `Email`
        // empty; the primary email travels in `PrimaryEmail`.
        email: String::new(),
        primary_email: (!user.primary_email.is_empty()).then(|| user.primary_email.clone()),
    }
}

fn person_guess(handle: &str, email: &str) -> ResolvedOwner {
    ResolvedOwner::Person {
        user: None,
        handle: handle.to_owned(),
        email: email.to_owned(),
        primary_email: None,
    }
}

fn team_owner(team: OwnTeamRow) -> ResolvedOwner {
    ResolvedOwner::Team { team }
}

/// `own.Reference.ResolutionGuess` (plus the DB lookups from `refKey.fetch`).
fn resolve_reference(
    db: &OwnHandle,
    reference: &OwnReference,
    code_host_kind: Option<&str>,
) -> Result<Option<ResolvedOwner>, String> {
    if let Some(user_id) = reference.user_id {
        return Ok(db.user_by_id(user_id)?.as_ref().map(person_from_user));
    }
    if let Some(team_id) = reference.team_id {
        return Ok(db.team_by_id(team_id)?.map(team_owner));
    }
    if !reference.handle.is_empty() {
        if let Some(user) = db.user_by_username(&reference.handle)? {
            return Ok(Some(person_from_user(&user)));
        }
        if let Some(team) = db.team_by_name(&reference.handle)? {
            return Ok(Some(team_owner(team)));
        }
    }
    if !reference.email.is_empty() {
        if let Some(user) = db.user_by_verified_email(&reference.email)? {
            return Ok(Some(person_from_user(&user)));
        }
    }
    // `ResolutionGuess`: a GitHub handle containing "/" is a team.
    if !reference.handle.is_empty()
        && code_host_kind
            .is_some_and(|kind| kind.eq_ignore_ascii_case("github"))
        && reference.handle.contains('/')
    {
        return Ok(Some(team_owner(OwnTeamRow {
            id: 0,
            name: reference.handle.clone(),
            display_name: Some(reference.handle.clone()),
            readonly: false,
        })));
    }
    if reference.handle.is_empty() && reference.email.is_empty() {
        return Ok(None);
    }
    Ok(Some(person_guess(&reference.handle, &reference.email)))
}

#[derive(Debug, Clone)]
enum Reason {
    Codeowners {
        source: RulesetSource,
        line: i32,
    },
    Assigned {
        path: String,
    },
    RecentContributor,
    RecentView,
}

struct OwnerGroup {
    owner: ResolvedOwner,
    reasons: Vec<Reason>,
}

impl OwnerGroup {
    /// `reasonsAndOwner.order()`.
    fn order(&self) -> i64 {
        let mut ownership_reasons = 0i64;
        let mut reasons = 0i64;
        let mut contributions = 0i64;
        let mut views = 0i64;
        for reason in &self.reasons {
            match reason {
                Reason::Codeowners { .. } | Reason::Assigned { .. } => ownership_reasons += 1,
                Reason::RecentContributor => contributions += 1,
                Reason::RecentView => views += 1,
            }
            reasons += 1;
        }
        -(100_000 * ownership_reasons + 1_000 * reasons + 10 * contributions + views)
    }

    fn is_owner(&self) -> bool {
        self.reasons
            .iter()
            .any(|reason| matches!(reason, Reason::Codeowners { .. } | Reason::Assigned { .. }))
    }
}

/// Top-level grouping + ordering of the collected ownership entries.
fn build_owner_groups(
    mut entries: Vec<(OwnReference, Reason)>,
    db: &OwnHandle,
    code_host_kind: Option<&str>,
) -> Result<Vec<OwnerGroup>, ExecError> {
    let mut groups: HashMap<String, OwnerGroup> = HashMap::new();
    // Preserve per-owner reason order while grouping.
    for (reference, reason) in entries.drain(..) {
        let Some(owner) = resolve_reference(db, &reference, code_host_kind)
            .map_err(ExecError::new)?
        else {
            continue;
        };
        let key = owner.identifier();
        groups
            .entry(key)
            .or_insert_with(|| OwnerGroup {
                owner,
                reasons: Vec::new(),
            })
            .reasons
            .push(reason);
    }
    let mut owners: Vec<OwnerGroup> = groups.into_values().collect();
    owners.sort_by(|a, b| {
        let (left, right) = (a.order(), b.order());
        if left != right {
            return left.cmp(&right);
        }
        a.owner.identifier().cmp(&b.owner.identifier())
    });
    Ok(owners)
}

/// `match` with the originating path retained (assigned ownership needs it for
/// `isDirectMatch`).
fn match_assigned_with_paths<T: Clone>(
    assigned: &HashMap<String, Vec<T>>,
    path: &str,
) -> Vec<(String, T)> {
    let mut out = Vec::new();
    let mut current = path;
    loop {
        if let Some(values) = assigned.get(current) {
            for value in values {
                out.push((current.to_owned(), value.clone()));
            }
        }
        match current.rfind('/') {
            Some(index) => current = &current[..index],
            None => break,
        }
    }
    if !current.is_empty() {
        if let Some(values) = assigned.get("") {
            for value in values {
                out.push((String::new(), value.clone()));
            }
        }
    }
    out
}

fn reasons_include(arguments: &Arguments, reason: &str) -> bool {
    match arguments.get("reasons") {
        None | Some(InputValue::Null) => true,
        Some(InputValue::List(values)) if values.is_empty() => true,
        Some(InputValue::List(values)) => values
            .iter()
            .any(|value| value.as_str() == Some(reason)),
        _ => true,
    }
}

/// Which signals one ownable surface evaluates (`GitBlobOwnership` applies the
/// `reasons` filter; `GitTreeOwnership`/`GitCommitOwnership` ignore it).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Surface {
    Blob,
    Tree,
    Commit,
}

impl Surface {
    fn include_codeowners(self) -> bool {
        matches!(self, Self::Blob)
    }
}

fn compute_reasons(
    db: &OwnHandle,
    repo_id: i32,
    repo_name: &str,
    commit: &str,
    path: &str,
    surface: Surface,
    arguments: &Arguments,
) -> Result<Vec<(OwnReference, Reason)>, ExecError> {
    let mut entries: Vec<(OwnReference, Reason)> = Vec::new();

    if surface.include_codeowners() {
        let include_all = reasons_include(arguments, "CODEOWNERS_FILE_ENTRY");
        if include_all {
            if let Some(ruleset) = db
                .ruleset(repo_id, repo_name, commit)
                .map_err(ExecError::new)?
            {
                if let Some(rule) = ruleset.ruleset.match_path(path) {
                    for owner in &rule.owners {
                        entries.push((
                            OwnReference::from_owner(owner),
                            Reason::Codeowners {
                                source: ruleset.source.clone(),
                                line: rule.line_number,
                            },
                        ));
                    }
                }
            }
        }
    }

    let include_contributors =
        surface != Surface::Blob || reasons_include(arguments, "RECENT_CONTRIBUTOR_OWNERSHIP_SIGNAL");
    if include_contributors
        && db
            .signal_enabled(SIGNAL_RECENT_CONTRIBUTORS)
            .map_err(ExecError::new)?
    {
        for author in db.recent_authors(repo_id, path).map_err(ExecError::new)? {
            entries.push((
                OwnReference {
                    email: author.author_email,
                    ..OwnReference::default()
                },
                Reason::RecentContributor,
            ));
        }
    }

    let include_views =
        surface != Surface::Blob || reasons_include(arguments, "RECENT_VIEW_OWNERSHIP_SIGNAL");
    if include_views && db.signal_enabled(SIGNAL_RECENT_VIEWS).map_err(ExecError::new)? {
        for view in db.recent_views(repo_id, path).map_err(ExecError::new)? {
            entries.push((
                OwnReference {
                    user_id: Some(view.user_id),
                    ..OwnReference::default()
                },
                Reason::RecentView,
            ));
        }
    }

    // `GitCommitOwnership` never evaluates assigned owners; `GitTree` always
    // does; `GitBlob` honours the `reasons` filter.
    let include_assigned = match surface {
        Surface::Blob => reasons_include(arguments, "ASSIGNED_OWNER"),
        Surface::Tree => true,
        Surface::Commit => false,
    };
    if include_assigned {
        let assigned = db.assigned_owners(repo_id).map_err(ExecError::new)?;
        for (assigned_path, user_id) in match_assigned_with_paths(&assigned, path) {
            entries.push((
                OwnReference {
                    user_id: Some(user_id),
                    ..OwnReference::default()
                },
                Reason::Assigned {
                    path: assigned_path,
                },
            ));
        }
        let teams = db.assigned_teams(repo_id).map_err(ExecError::new)?;
        for (assigned_path, team_id) in match_assigned_with_paths(&teams, path) {
            entries.push((
                OwnReference {
                    team_id: Some(team_id),
                    ..OwnReference::default()
                },
                Reason::Assigned {
                    path: assigned_path,
                },
            ));
        }
    }

    Ok(entries)
}

/// `ownershipConnection`: group, order, paginate and count.
fn build_ownership_connection(
    db: &OwnHandle,
    actor: Actor,
    repo_id: i32,
    repo_name: &str,
    commit: &str,
    path: &str,
    surface: Surface,
    arguments: &Arguments,
) -> Result<OwnershipConnectionObject, ExecError> {
    let entries = compute_reasons(db, repo_id, repo_name, commit, path, surface, arguments)?;
    let _ = actor;
    let code_host_kind = db
        .repo_by_id(Actor::Internal, repo_id)
        .ok()
        .flatten()
        .map(|repo| repo.external_service_type);
    let mut owners = build_owner_groups(entries, db, code_host_kind.as_deref())?;

    let total = owners.len() as i32;
    let total_owners = owners.iter().filter(|owner| owner.is_owner()).count() as i32;

    let cursor = decode_cursor(arguments.string("after"))?;
    while !cursor.is_empty() && !owners.is_empty() && owners[0].owner.identifier() != cursor {
        owners.remove(0);
    }
    let first = arguments.i64("first");
    let mut next = None;
    if let Some(first) = first {
        let first = usize::try_from(first.max(0)).unwrap_or(0);
        if owners.len() > first {
            next = Some(owners[first].owner.identifier());
            owners.truncate(first);
        }
    }

    Ok(OwnershipConnectionObject {
        db: Arc::clone(db),
        repo_name: repo_name.to_owned(),
        path: path.to_owned(),
        total,
        total_owners,
        next,
        owners,
    })
}

// ---------------------------------------------------------------------------
// GraphQL objects
// ---------------------------------------------------------------------------

/// `gqlutil.PageInfo`.
struct OwnPageInfoObject {
    end_cursor: Option<String>,
    has_next_page: bool,
}

impl ObjectValue for OwnPageInfoObject {
    fn type_name(&self) -> &'static str {
        "PageInfo"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "endCursor" => Ok(Some(match &self.end_cursor {
                Some(cursor) => FieldValue::Json(Json::Str(cursor.clone())),
                None => FieldValue::Null,
            })),
            "hasNextPage" => Ok(Some(FieldValue::Json(Json::Bool(self.has_next_page)))),
            _ => Ok(None),
        }
    }
}

/// `ownershipConnectionResolver`.
pub struct OwnershipConnectionObject {
    db: OwnHandle,
    repo_name: String,
    path: String,
    total: i32,
    total_owners: i32,
    next: Option<String>,
    owners: Vec<OwnerGroup>,
}

impl ObjectValue for OwnershipConnectionObject {
    fn type_name(&self) -> &'static str {
        "OwnershipConnection"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "totalCount" => Ok(Some(FieldValue::Json(Json::Int(i64::from(self.total))))),
            "totalOwners" => Ok(Some(FieldValue::Json(Json::Int(i64::from(
                self.total_owners,
            ))))),
            "pageInfo" => Ok(Some(FieldValue::Object(Arc::new(OwnPageInfoObject {
                end_cursor: self.next.as_deref().map(encode_cursor),
                has_next_page: self.next.is_some(),
            })))),
            "nodes" => Ok(Some(FieldValue::List {
                element_type: Some("Ownership"),
                items: self
                    .owners
                    .iter()
                    .map(|group| {
                        FieldValue::Object(Arc::new(OwnershipObject {
                            db: Arc::clone(&self.db),
                            repo_name: self.repo_name.clone(),
                            path: self.path.clone(),
                            owner: group.owner.clone(),
                            reasons: group.reasons.clone(),
                        }))
                    })
                    .collect(),
            })),
            _ => Ok(None),
        }
    }
}

/// `ownershipResolver`.
struct OwnershipObject {
    db: OwnHandle,
    repo_name: String,
    path: String,
    owner: ResolvedOwner,
    reasons: Vec<Reason>,
}

impl ObjectValue for OwnershipObject {
    fn type_name(&self) -> &'static str {
        "Ownership"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "owner" => Ok(Some(owner_value(&self.owner, actor))),
            "reasons" => Ok(Some(FieldValue::List {
                element_type: Some("OwnershipReason"),
                items: self
                    .reasons
                    .iter()
                    .map(|reason| FieldValue::Object(self.reason_object(reason)))
                    .collect(),
            })),
            _ => Ok(None),
        }
    }
}

impl OwnershipObject {
    fn reason_object(&self, reason: &Reason) -> Arc<dyn ObjectValue> {
        match reason {
            Reason::Codeowners { source, line } => Arc::new(CodeownersFileEntryObject {
                db: Arc::clone(&self.db),
                repo_name: self.repo_name.clone(),
                source: source.clone(),
                line: *line,
            }),
            Reason::Assigned { path } => Arc::new(AssignedOwnerObject {
                direct_match: self.path == *path,
            }),
            Reason::RecentContributor => Arc::new(RecentContributorOwnershipSignalObject),
            Reason::RecentView => Arc::new(RecentViewOwnershipSignalObject),
        }
    }
}

fn owner_value(owner: &ResolvedOwner, _actor: Actor) -> FieldValue {
    match owner {
        ResolvedOwner::Person {
            user,
            handle,
            email,
            primary_email,
        } => FieldValue::Object(Arc::new(OwnPersonObject {
            user: user.clone(),
            name: handle.clone(),
            email: primary_email.clone().unwrap_or_else(|| email.clone()),
        })),
        ResolvedOwner::Team { team } => FieldValue::Object(Arc::new(OwnTeamObject {
            team: team.clone(),
        })),
    }
}

/// `personResolver` (`person.go`).
struct OwnPersonObject {
    user: Option<UserData>,
    name: String,
    email: String,
}

impl ObjectValue for OwnPersonObject {
    fn type_name(&self) -> &'static str {
        "Person"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "name" => {
                let name = match &self.user {
                    Some(user) if !user.username.is_empty() => user.username.clone(),
                    _ => self.name.clone(),
                };
                Ok(Some(FieldValue::Json(Json::Str(name))))
            }
            "email" => {
                // Non-dotcom returns the resolver email; dotcom hides it from
                // anonymous viewers (`person.go:110-140`).
                Ok(Some(FieldValue::Json(Json::Str(self.email.clone()))))
            }
            "displayName" => {
                if let Some(user) = &self.user {
                    if !user.display_name.is_empty() {
                        return Ok(Some(FieldValue::Json(Json::Str(
                            user.display_name.clone(),
                        ))));
                    }
                }
                let name = self.name.trim();
                if !name.is_empty() {
                    return Ok(Some(FieldValue::Json(Json::Str(name.to_owned()))));
                }
                if !self.email.is_empty() {
                    return Ok(Some(FieldValue::Json(Json::Str(self.email.clone()))));
                }
                Ok(Some(FieldValue::Json(Json::Str("unknown".to_owned()))))
            }
            "avatarURL" => match &self.user {
                Some(user) if !user.avatar_url.is_empty() => Ok(Some(FieldValue::Json(
                    Json::Str(user.avatar_url.clone()),
                ))),
                _ => Ok(Some(FieldValue::Null)),
            },
            "user" => match &self.user {
                Some(user) => Ok(Some(FieldValue::Object(Arc::new(UserObject::from_data(
                    user.clone(),
                ))))),
                None => Ok(Some(FieldValue::Null)),
            },
            _ => Ok(None),
        }
    }
}

/// `TeamResolver` as the `Owner` union exposes it. DB-backed team members are
/// not reachable from this surface (the selection is outside the own corpus).
struct OwnTeamObject {
    team: OwnTeamRow,
}

impl OwnTeamObject {
    fn external(&self) -> bool {
        self.team.id == 0
    }
}

impl ObjectValue for OwnTeamObject {
    fn type_name(&self) -> &'static str {
        "Team"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "id" => Ok(Some(FieldValue::Json(Json::Str(marshal_relay_int_id(
                "Team",
                self.team.id,
            ))))),
            "name" => Ok(Some(FieldValue::Json(Json::Str(self.team.name.clone())))),
            "url" => Ok(Some(FieldValue::Json(Json::Str(if self.external() {
                String::new()
            } else {
                format!("/teams/{}", self.team.name)
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
            "viewerCanAdminister" => Ok(Some(FieldValue::Json(Json::Bool(false)))),
            "creator" | "parentTeam" => Ok(Some(FieldValue::Null)),
            "members" => {
                if self.external() {
                    Err(ExecError::new("cannot get members of external team"))
                } else {
                    Err(ExecError::new("team members are not ported"))
                }
            }
            "childTeams" => {
                if self.external() {
                    Err(ExecError::new("cannot get child teams of external team"))
                } else {
                    Err(ExecError::new("team child teams are not ported"))
                }
            }
            _ => Ok(None),
        }
    }
}

/// `codeownersFileEntryResolver`.
struct CodeownersFileEntryObject {
    db: OwnHandle,
    repo_name: String,
    source: RulesetSource,
    line: i32,
}

impl ObjectValue for CodeownersFileEntryObject {
    fn type_name(&self) -> &'static str {
        "CodeownersFileEntry"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "title" => Ok(Some(FieldValue::Json(Json::Str("codeowners".to_owned())))),
            "description" => Ok(Some(FieldValue::Json(Json::Str(
                "Owner is associated with a rule in a CODEOWNERS file.".to_owned(),
            )))),
            "ruleLineMatch" => Ok(Some(FieldValue::Json(Json::Int(i64::from(self.line))))),
            "codeownersFile" => Ok(Some(self.codeowners_file()?)),
            _ => Ok(None),
        }
    }
}

impl CodeownersFileEntryObject {
    fn codeowners_file(&self) -> Result<FieldValue, ExecError> {
        match &self.source {
            RulesetSource::Ingested { repo_id } => Ok(FieldValue::Object(Arc::new(
                CodeownersVirtualFileObject {
                    db: Arc::clone(&self.db),
                    repo_id: *repo_id,
                    repo_name: self.repo_name.clone(),
                },
            ))),
            RulesetSource::Git { commit, path, .. } => Ok(FieldValue::Object(Arc::new(
                CodeownersGitBlobObject {
                    repo_name: self.repo_name.clone(),
                    commit: commit.clone(),
                    path: path.clone(),
                },
            ))),
        }
    }
}

/// `graphqlbackend.NewVirtualFileResolver` for an ingested CODEOWNERS file.
struct CodeownersVirtualFileObject {
    db: OwnHandle,
    repo_id: i32,
    repo_name: String,
}

impl CodeownersVirtualFileObject {
    fn contents(&self) -> Result<String, ExecError> {
        self.db
            .codeowners_for_repo(self.repo_id)
            .map_err(ExecError::new)?
            .map(|file| file.contents)
            .ok_or_else(|| ExecError::new("codeowners file not found"))
    }
}

impl ObjectValue for CodeownersVirtualFileObject {
    fn type_name(&self) -> &'static str {
        "VirtualFile"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "path" | "name" => Ok(Some(FieldValue::Json(Json::Str("CODEOWNERS".to_owned())))),
            "isDirectory" => Ok(Some(FieldValue::Json(Json::Bool(false)))),
            "url" => Ok(Some(FieldValue::Json(Json::Str(format!(
                "/{}/-/own/edit",
                self.repo_name
            ))))),
            "canonicalURL" => Ok(Some(FieldValue::Json(Json::Str(String::new())))),
            "content" => Ok(Some(FieldValue::Json(Json::Str(self.contents()?)))),
            "byteSize" => Ok(Some(FieldValue::Json(Json::Int(
                self.contents()?.len() as i64,
            )))),
            "totalLines" => Ok(Some(FieldValue::Json(Json::Int(
                self.contents()?.split('\n').count() as i64,
            )))),
            "binary" => Ok(Some(FieldValue::Json(Json::Bool(false)))),
            "richHTML" => Ok(Some(FieldValue::Json(Json::Str(String::new())))),
            "externalURLs" => Ok(Some(FieldValue::List {
                items: Vec::new(),
                element_type: Some("ExternalLink"),
            })),
            "changelistURL" | "highlight" => Ok(Some(FieldValue::Null)),
            "languages" => Ok(Some(FieldValue::List {
                items: Vec::new(),
                element_type: Some("String"),
            })),
            _ => Ok(None),
        }
    }
}

/// `GitCommitResolver.File`-backed CODEOWNERS file (a `GitBlob`).
struct CodeownersGitBlobObject {
    repo_name: String,
    commit: String,
    path: String,
}

impl ObjectValue for CodeownersGitBlobObject {
    fn type_name(&self) -> &'static str {
        "GitBlob"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        let url = format!("/{}@{}/-/blob/{}", self.repo_name, self.commit, self.path);
        match field {
            "path" => Ok(Some(FieldValue::Json(Json::Str(self.path.clone())))),
            "name" => Ok(Some(FieldValue::Json(Json::Str(
                self.path.rsplit('/').next().unwrap_or(&self.path).to_owned(),
            )))),
            "isDirectory" => Ok(Some(FieldValue::Json(Json::Bool(false)))),
            "url" => Ok(Some(FieldValue::Json(Json::Str(url.clone())))),
            "canonicalURL" => Ok(Some(FieldValue::Json(Json::Str(url)))),
            "externalURLs" => Ok(Some(FieldValue::List {
                items: Vec::new(),
                element_type: Some("ExternalLink"),
            })),
            "languages" => Ok(Some(FieldValue::List {
                items: Vec::new(),
                element_type: Some("String"),
            })),
            "content" | "byteSize" | "totalLines" | "binary" | "richHTML" | "highlight" => Err(
                ExecError::new("CODEOWNERS file content is not prefetched for this request"),
            ),
            "changelistURL" => Ok(Some(FieldValue::Null)),
            _ => Ok(None),
        }
    }
}

/// `assignedOwner`.
struct AssignedOwnerObject {
    direct_match: bool,
}

impl ObjectValue for AssignedOwnerObject {
    fn type_name(&self) -> &'static str {
        "AssignedOwner"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "title" => Ok(Some(FieldValue::Json(Json::Str("assigned owner".to_owned())))),
            "description" => Ok(Some(FieldValue::Json(Json::Str(
                "Owner is manually assigned.".to_owned(),
            )))),
            "isDirectMatch" => Ok(Some(FieldValue::Json(Json::Bool(self.direct_match)))),
            _ => Ok(None),
        }
    }
}

/// `recentContributorOwnershipSignal`.
struct RecentContributorOwnershipSignalObject;

impl ObjectValue for RecentContributorOwnershipSignalObject {
    fn type_name(&self) -> &'static str {
        "RecentContributorOwnershipSignal"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "title" => Ok(Some(FieldValue::Json(Json::Str(
                "recent contributor".to_owned(),
            )))),
            "description" => Ok(Some(FieldValue::Json(Json::Str(
                "Associated because they have contributed to this file in the last 90 days."
                    .to_owned(),
            )))),
            _ => Ok(None),
        }
    }
}

/// `recentViewOwnershipSignal`.
struct RecentViewOwnershipSignalObject;

impl ObjectValue for RecentViewOwnershipSignalObject {
    fn type_name(&self) -> &'static str {
        "RecentViewOwnershipSignal"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "title" => Ok(Some(FieldValue::Json(Json::Str("recent view".to_owned())))),
            "description" => Ok(Some(FieldValue::Json(Json::Str(
                "Associated because they have viewed this file in the last 90 days.".to_owned(),
            )))),
            _ => Ok(None),
        }
    }
}

// ---------------------------------------------------------------------------
// Ownership stats
// ---------------------------------------------------------------------------

/// `ownStatsResolver`.
struct OwnershipStatsObject {
    db: OwnHandle,
    location: Option<(i32, String)>,
    counts: OnceLock<Result<OwnStatsCounts, String>>,
    total_files: OnceLock<Result<i32, String>>,
}

impl OwnershipStatsObject {
    fn counts(&self) -> Result<&OwnStatsCounts, ExecError> {
        match self
            .counts
            .get_or_init(|| self.db.ownership_stats(self.location.clone()))
        {
            Ok(counts) => Ok(counts),
            Err(message) => Err(ExecError::new(message.clone())),
        }
    }

    fn total_files(&self) -> Result<i32, ExecError> {
        match self
            .total_files
            .get_or_init(|| self.db.total_files(self.location.clone()))
        {
            Ok(count) => Ok(*count),
            Err(message) => Err(ExecError::new(message.clone())),
        }
    }
}

impl ObjectValue for OwnershipStatsObject {
    fn type_name(&self) -> &'static str {
        "OwnershipStats"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "totalFiles" => Ok(Some(FieldValue::Json(Json::Int(i64::from(
                self.total_files()?,
            ))))),
            "totalCodeownedFiles" => Ok(Some(FieldValue::Json(Json::Int(i64::from(
                self.counts()?.codeowned,
            ))))),
            "totalOwnedFiles" => Ok(Some(FieldValue::Json(Json::Int(i64::from(
                self.counts()?.total_owned,
            ))))),
            "totalAssignedOwnershipFiles" => Ok(Some(FieldValue::Json(Json::Int(i64::from(
                self.counts()?.assigned,
            ))))),
            "updatedAt" => {
                // `gqlutil.FromTime` never returns nil, including the zero time.
                let updated = self
                    .counts()?
                    .updated_at
                    .unwrap_or(OffsetDateTime::UNIX_EPOCH);
                let zero = self.counts()?.updated_at.is_none();
                let value = if zero {
                    "0001-01-01T00:00:00Z".to_owned()
                } else {
                    datetime_rfc3339(updated)
                };
                Ok(Some(FieldValue::Json(Json::Str(value))))
            }
            _ => Ok(None),
        }
    }
}

// ---------------------------------------------------------------------------
// Signal configuration
// ---------------------------------------------------------------------------

/// `signalConfigResolver`.
struct OwnSignalConfigurationObject {
    config: sg_db::own_signal_configurations::SignalConfiguration,
}

impl ObjectValue for OwnSignalConfigurationObject {
    fn type_name(&self) -> &'static str {
        "OwnSignalConfiguration"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "name" => Ok(Some(FieldValue::Json(Json::Str(self.config.name.clone())))),
            "description" => Ok(Some(FieldValue::Json(Json::Str(
                self.config.description.clone(),
            )))),
            "isEnabled" => Ok(Some(FieldValue::Json(Json::Bool(self.config.enabled)))),
            "excludedRepoPatterns" => Ok(Some(FieldValue::List {
                element_type: Some("String"),
                items: self
                    .config
                    .excluded_repo_patterns
                    .iter()
                    .map(|pattern| {
                        FieldValue::Json(Json::Str(pattern.replace('%', "*")))
                    })
                    .collect(),
            })),
            _ => Ok(None),
        }
    }
}

// ---------------------------------------------------------------------------
// Codeowners ingestion
// ---------------------------------------------------------------------------

/// `codeownersIngestedFileResolver`.
struct CodeownersIngestedFileObject {
    db: OwnHandle,
    repo: RepositoryData,
    file: CodeownersFileRow,
}

impl ObjectValue for CodeownersIngestedFileObject {
    fn type_name(&self) -> &'static str {
        "CodeownersIngestedFile"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "id" => Ok(Some(FieldValue::Json(Json::Str(marshal_relay_int_id(
                CODEOWNERS_INGESTED_FILE_KIND,
                self.file.repo_id,
            ))))),
            "contents" => Ok(Some(FieldValue::Json(Json::Str(self.file.contents.clone())))),
            "repository" => Ok(Some(FieldValue::Object(Arc::new(
                RepositoryObject::from_data(self.repo.clone()),
            )))),
            "createdAt" => Ok(Some(FieldValue::Json(Json::Str(datetime_rfc3339(
                self.file.created_at,
            ))))),
            "updatedAt" => Ok(Some(FieldValue::Json(Json::Str(datetime_rfc3339(
                self.file.updated_at,
            ))))),
            _ => {
                let _ = &self.db;
                Ok(None)
            }
        }
    }
}

/// `codeownersIngestedFileConnectionResolver`.
struct CodeownersIngestedFileConnectionObject {
    db: OwnHandle,
    cursor: i32,
    limit: Option<i32>,
    computed: OnceLock<Result<(Vec<CodeownersFileRow>, i32), String>>,
}

impl CodeownersIngestedFileConnectionObject {
    fn compute(&self) -> Result<&(Vec<CodeownersFileRow>, i32), ExecError> {
        match self
            .computed
            .get_or_init(|| self.db.list_codeowners(self.cursor, self.limit))
        {
            Ok(result) => Ok(result),
            Err(message) => Err(ExecError::new(message.clone())),
        }
    }
}

impl ObjectValue for CodeownersIngestedFileConnectionObject {
    fn type_name(&self) -> &'static str {
        "CodeownersIngestedFileConnection"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "nodes" => {
                let (files, _) = self.compute()?;
                Ok(Some(FieldValue::List {
                    element_type: Some("CodeownersIngestedFile"),
                    items: files
                        .iter()
                        .map(|file| {
                            // Nodes carry no repository in the baseline (the
                            // connection scan has no repo join); the field is
                            // still requested in the corpus, so use the
                            // repoID-only placeholder.
                            FieldValue::Object(Arc::new(CodeownersIngestedFileObject {
                                db: Arc::clone(&self.db),
                                repo: RepositoryData {
                                    id: file.repo_id,
                                    ..RepositoryData::default()
                                },
                                file: file.clone(),
                            }))
                        })
                        .collect(),
                }))
            }
            "totalCount" => Ok(Some(FieldValue::Json(Json::Int(i64::from(
                self.db.count_codeowners().map_err(ExecError::new)?,
            ))))),
            "pageInfo" => {
                let (_, next) = self.compute()?;
                let (end_cursor, has_next_page) = encode_int_cursor((*next > 0).then_some(*next));
                Ok(Some(FieldValue::Object(Arc::new(OwnPageInfoObject {
                    end_cursor,
                    has_next_page,
                }))))
            }
            _ => Ok(None),
        }
    }
}

/// `RepositoryData::default()` is not in the baseline: repositories are always
/// hydrated. Only the connection-node placeholder needs an empty shell.
impl Default for RepositoryData {
    fn default() -> Self {
        Self {
            id: 0,
            name: String::new(),
            uri: String::new(),
            description: String::new(),
            fork: false,
            archived: false,
            private: false,
            stars: 0,
            external_id: String::new(),
            external_service_type: String::new(),
            external_service_id: String::new(),
        }
    }
}

// ---------------------------------------------------------------------------
// Production CODEOWNERS proto encoding
// ---------------------------------------------------------------------------

fn put_varint(out: &mut Vec<u8>, mut value: u64) {
    while value >= 0x80 {
        out.push((value as u8) | 0x80);
        value >>= 7;
    }
    out.push(value as u8);
}

fn put_key(out: &mut Vec<u8>, field: u64, wire_type: u64) {
    put_varint(out, (field << 3) | wire_type);
}

fn put_bytes(out: &mut Vec<u8>, field: u64, value: &[u8]) {
    put_key(out, field, 2);
    put_varint(out, value.len() as u64);
    out.extend_from_slice(value);
}

fn put_string(out: &mut Vec<u8>, field: u64, value: &str) {
    if !value.is_empty() {
        put_bytes(out, field, value.as_bytes());
    }
}

/// Minimal protobuf encoding of `own.codeowners.v1.File` (the `contents_proto`
/// column requires valid bytes; Go prefers it when reading ingested files).
pub fn encode_codeowners_proto(parsed: &crate::search_owners::ParsedFile) -> Vec<u8> {
    let mut out = Vec::new();
    for rule in &parsed.rules {
        let mut encoded = Vec::new();
        put_string(&mut encoded, 1, &rule.pattern);
        for owner in &rule.owners {
            let mut encoded_owner = Vec::new();
            put_string(&mut encoded_owner, 1, &owner.handle);
            put_string(&mut encoded_owner, 2, &owner.email);
            put_bytes(&mut encoded, 2, &encoded_owner);
        }
        put_string(&mut encoded, 3, &rule.section_name);
        if rule.line_number != 0 {
            put_key(&mut encoded, 4, 0);
            put_varint(&mut encoded, rule.line_number as u64);
        }
        put_bytes(&mut out, 1, &encoded);
    }
    out
}

// ---------------------------------------------------------------------------
// Authorization
// ---------------------------------------------------------------------------

/// `viewerCanAdminister` (`auth.CheckCurrentUserIsSiteAdmin`).
fn check_site_admin(db: &OwnHandle, actor: Actor) -> Result<(), ExecError> {
    if actor.is_internal() {
        return Ok(());
    }
    let user = db
        .current_user(actor)
        .map_err(ExecError::new)?
        .ok_or_else(|| ExecError::new("not authenticated"))?;
    if !user.site_admin {
        return Err(ExecError::new("must be site admin"));
    }
    Ok(())
}

/// `isIngestionAvailable`.
fn check_ingestion_available(db: &OwnHandle) -> Result<(), ExecError> {
    if db.sourcegraph_dotcom_mode() {
        return Err(ExecError::new(
            "codeownership ingestion is not available on sourcegraph.com",
        ));
    }
    Ok(())
}

fn check_assigned_ownership_permission(db: &OwnHandle, actor: Actor) -> Result<UserData, ExecError> {
    if actor.is_internal() {
        return Err(ExecError::new("internal actor has no user"));
    }
    let user = db
        .current_user(actor)
        .map_err(ExecError::new)?
        .ok_or_else(|| ExecError::new("not authenticated"))?;
    db.check_ownership_assign_permission(actor)
        .map_err(ExecError::new)?;
    Ok(user)
}

// ---------------------------------------------------------------------------
// Repository resolution for mutations
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Default)]
struct RepoInput {
    repo_id: Option<String>,
    repo_name: Option<String>,
}

/// `getRepo` (`codeowners_resolvers.go:124`).
fn get_repo(
    db: &OwnHandle,
    actor: Actor,
    input: &RepoInput,
) -> Result<RepositoryData, ExecError> {
    match (&input.repo_id, &input.repo_name) {
        (None, None) => Err(ExecError::new("either RepoID or RepoName should be set")),
        (Some(_), Some(_)) => Err(ExecError::new(
            "both RepoID and RepoName cannot be set",
        )),
        (Some(repo_id), None) => {
            let id = unmarshal_relay_id(repo_id)
                .map_err(|error| ExecError::new(format!("could not unmarshal repository id: {error}")))?;
            db.repo_by_id(actor, id)
                .map_err(ExecError::new)?
                .ok_or_else(|| ExecError::new("repository not found"))
        }
        (None, Some(repo_name)) => db
            .repo_by_name(actor, repo_name)
            .map_err(ExecError::new)?
            .ok_or_else(|| ExecError::new("repository not found")),
    }
}

// ---------------------------------------------------------------------------
// Input parsing
// ---------------------------------------------------------------------------

fn input_object<'a>(
    arguments: &'a Arguments,
    name: &str,
) -> Result<&'a BTreeMap<String, InputValue>, ExecError> {
    match arguments.get(name) {
        Some(InputValue::Object(map)) => Ok(map),
        _ => Err(ExecError::new(format!("missing input {name}"))),
    }
}

fn object_string(map: &BTreeMap<String, InputValue>, name: &str) -> Option<String> {
    match map.get(name) {
        Some(InputValue::Str(value)) => Some(value.clone()),
        _ => None,
    }
}

fn object_bool(map: &BTreeMap<String, InputValue>, name: &str) -> Option<bool> {
    match map.get(name) {
        Some(InputValue::Bool(value)) => Some(*value),
        _ => None,
    }
}

fn object_string_list(map: &BTreeMap<String, InputValue>, name: &str) -> Vec<String> {
    match map.get(name) {
        Some(InputValue::List(values)) => values
            .iter()
            .filter_map(|value| value.as_str().map(str::to_owned))
            .collect(),
        _ => Vec::new(),
    }
}

fn repo_input_from_object(map: &BTreeMap<String, InputValue>) -> RepoInput {
    RepoInput {
        repo_id: object_string(map, "repoID"),
        repo_name: object_string(map, "repoName"),
    }
}

// ---------------------------------------------------------------------------
// Root fields
// ---------------------------------------------------------------------------

/// `Query.codeownersIngestedFiles` / `ownSignalConfigurations` /
/// `instanceOwnershipStats`.
pub fn is_root_field(field: &str) -> bool {
    matches!(
        field,
        "codeownersIngestedFiles" | "ownSignalConfigurations" | "instanceOwnershipStats"
    )
}

pub fn root_field(
    db: &OwnHandle,
    field: &str,
    arguments: &Arguments,
    actor: Actor,
) -> Result<Option<FieldValue>, ExecError> {
    match field {
        "codeownersIngestedFiles" => {
            check_ingestion_available(db)?;
            check_site_admin(db, actor)?;
            let cursor = decode_int_cursor_from_argument(arguments.i64("after"))?;
            let limit = arguments.i64("first").map(|first| first as i32);
            Ok(Some(FieldValue::Object(Arc::new(
                CodeownersIngestedFileConnectionObject {
                    db: Arc::clone(db),
                    cursor,
                    limit: limit.filter(|limit| *limit != 0),
                    computed: OnceLock::new(),
                },
            ))))
        }
        "ownSignalConfigurations" => {
            check_site_admin(db, actor)?;
            let configurations = db
                .load_signal_configurations()
                .map_err(|error| ExecError::new(format!("LoadConfigurations: {error}")))?;
            Ok(Some(FieldValue::List {
                element_type: Some("OwnSignalConfiguration"),
                items: configurations
                    .into_iter()
                    .map(|config| {
                        FieldValue::Object(Arc::new(OwnSignalConfigurationObject { config }))
                    })
                    .collect(),
            }))
        }
        "instanceOwnershipStats" => Ok(Some(FieldValue::Object(Arc::new(
            OwnershipStatsObject {
                db: Arc::clone(db),
                location: None,
                counts: OnceLock::new(),
                total_files: OnceLock::new(),
            },
        )))),
        _ => Ok(None),
    }
}

/// `Mutation.*`.
pub fn is_mutation_field(field: &str) -> bool {
    matches!(
        field,
        "addCodeownersFile"
            | "updateCodeownersFile"
            | "deleteCodeownersFiles"
            | "updateOwnSignalConfigurations"
            | "assignOwner"
            | "removeAssignedOwner"
            | "assignTeam"
            | "removeAssignedTeam"
    )
}

fn empty_response() -> FieldValue {
    FieldValue::Object(Arc::new(crate::temporary_settings::EmptyResponseObject))
}

fn fetch_codeowners_file(
    db: &OwnHandle,
    repo: &RepositoryData,
    file: CodeownersFileRow,
) -> FieldValue {
    FieldValue::Object(Arc::new(CodeownersIngestedFileObject {
        db: Arc::clone(db),
        repo: repo.clone(),
        file,
    }))
}

pub fn mutation_field(
    db: &OwnHandle,
    field: &str,
    arguments: &Arguments,
    actor: Actor,
) -> Result<Option<FieldValue>, ExecError> {
    match field {
        "addCodeownersFile" | "updateCodeownersFile" => {
            check_ingestion_available(db)?;
            check_site_admin(db, actor)?;
            let input = input_object(arguments, "input")?;
            let file_contents = object_string(input, "fileContents")
                .ok_or_else(|| ExecError::new("missing input fileContents"))?;
            let parsed = parse_codeowners(&file_contents)
                .map_err(|error| ExecError::new(format!("could not parse input: {error}")))?;
            let repo_input = repo_input_from_object(input);
            let repo = get_repo(db, actor, &repo_input)?;
            let proto = encode_codeowners_proto(&parsed);
            let file = if field == "addCodeownersFile" {
                db.create_codeowners(repo.id, &file_contents, &proto)
                    .map_err(|error| {
                        ExecError::new(format!("could not ingest codeowners file: {error}"))
                    })
            } else {
                db.update_codeowners(repo.id, &file_contents, &proto)
                    .map_err(|error| {
                        ExecError::new(format!("could not update codeowners file: {error}"))
                    })
            }?;
            Ok(Some(fetch_codeowners_file(db, &repo, file)))
        }
        "deleteCodeownersFiles" => {
            check_ingestion_available(db)?;
            check_site_admin(db, actor)?;
            let repositories = match arguments.get("repositories") {
                Some(InputValue::List(values)) => values.clone(),
                _ => Vec::new(),
            };
            if repositories.is_empty() {
                // The baseline returns `nil, nil` (a null field).
                return Ok(Some(FieldValue::Null));
            }
            let mut repo_ids = Vec::new();
            for value in repositories {
                let InputValue::Object(map) = value else {
                    continue;
                };
                let repo = get_repo(db, actor, &repo_input_from_object(&map))?;
                repo_ids.push(repo.id);
            }
            db.delete_codeowners(&repo_ids).map_err(|error| {
                ExecError::new(format!("could not delete codeowners file for repos: {error}"))
            })?;
            Ok(Some(empty_response()))
        }
        "updateOwnSignalConfigurations" => {
            check_site_admin(db, actor)?;
            let input = input_object(arguments, "input")?;
            let configs = match input.get("configs") {
                Some(InputValue::List(values)) => values.clone(),
                _ => Vec::new(),
            };
            for value in configs {
                let InputValue::Object(map) = value else {
                    continue;
                };
                let name = object_string(&map, "name").unwrap_or_default();
                let enabled = object_bool(&map, "enabled").unwrap_or(false);
                let patterns: Vec<String> = object_string_list(&map, "excludedRepoPatterns")
                    .into_iter()
                    .map(|pattern| pattern.replace('*', "%"))
                    .collect();
                db.update_signal_configuration(&name, &patterns, enabled)
                    .map_err(ExecError::new)?;
            }
            let configurations = db
                .load_signal_configurations()
                .map_err(|error| ExecError::new(format!("LoadConfigurations: {error}")))?;
            Ok(Some(FieldValue::List {
                element_type: Some("OwnSignalConfiguration"),
                items: configurations
                    .into_iter()
                    .map(|config| {
                        FieldValue::Object(Arc::new(OwnSignalConfigurationObject { config }))
                    })
                    .collect(),
            }))
        }
        "assignOwner" | "removeAssignedOwner" | "assignTeam" | "removeAssignedTeam" => {
            // Internal actors are a no-op returning `nil, nil`.
            if actor.is_internal() {
                return Ok(Some(FieldValue::Null));
            }
            let user = check_assigned_ownership_permission(db, actor)?;
            let input = input_object(arguments, "input")?;
            let is_team = matches!(field, "assignTeam" | "removeAssignedTeam");
            let assign = matches!(field, "assignOwner" | "assignTeam");
            let assigned_id = object_string(input, "assignedOwnerID")
                .ok_or_else(|| ExecError::new("missing input assignedOwnerID"))?;
            let assigned_id = unmarshal_relay_id(&assigned_id).map_err(ExecError::new)?;
            if assigned_id == 0 {
                return Err(ExecError::new(if is_team {
                    "assigned team ID should not be 0"
                } else {
                    "assigned user ID should not be 0"
                }));
            }
            let repo_id = object_string(input, "repoID")
                .ok_or_else(|| ExecError::new("missing input repoID"))?;
            let repo_id = unmarshal_relay_id(&repo_id).map_err(ExecError::new)?;
            if repo_id == 0 {
                return Err(ExecError::new("repo ID should not be 0"));
            }
            let absolute_path = object_string(input, "absolutePath").unwrap_or_default();
            let outcome = match (is_team, assign) {
                (false, true) => db
                    .insert_assigned_owner(assigned_id, repo_id, &absolute_path, user.id)
                    .map_err(|error| format!("creating assigned owner: {error}")),
                (false, false) => db
                    .delete_assigned_owner(assigned_id, repo_id, &absolute_path)
                    .map_err(|error| format!("deleting assigned owner: {error}")),
                (true, true) => db
                    .insert_assigned_team(assigned_id, repo_id, &absolute_path, user.id)
                    .map_err(|error| format!("creating assigned team: {error}")),
                (true, false) => db
                    .delete_assigned_team(assigned_id, repo_id, &absolute_path)
                    .map_err(|error| format!("deleting assigned team: {error}")),
            };
            outcome.map_err(ExecError::new)?;
            Ok(Some(empty_response()))
        }
        _ => Ok(None),
    }
}

/// `NodeResolvers()["CodeownersIngestedFile"]`.
pub fn is_node_kind(kind: &str) -> bool {
    kind == CODEOWNERS_INGESTED_FILE_KIND
}

pub fn node_field(
    db: &OwnHandle,
    id: &str,
    actor: Actor,
) -> Result<Option<FieldValue>, ExecError> {
    let repo_id: i32 = unmarshal_relay_id(id)
        .map_err(|error| ExecError::new(format!("could not unmarshal repository ID: {error}")))?;
    // `RepoIngestedCodeowners` is open to anyone; the repo store enforces
    // visibility.
    let repo = db
        .repo_by_id(actor, repo_id)
        .map_err(ExecError::new)?;
    let Some(repo) = repo else {
        return Ok(Some(FieldValue::Null));
    };
    let file = db
        .codeowners_for_repo(repo_id)
        .map_err(ExecError::new)?;
    match file {
        Some(file) => Ok(Some(fetch_codeowners_file(db, &repo, file))),
        None => Ok(Some(FieldValue::Null)),
    }
}

// ---------------------------------------------------------------------------
// Git object surfaces
// ---------------------------------------------------------------------------

/// `GitBlob.ownership`.
pub fn git_blob_ownership(
    db: &OwnHandle,
    repo: &MinimalRepo,
    commit: &str,
    path: &str,
    arguments: &Arguments,
    actor: Actor,
) -> Result<Option<FieldValue>, ExecError> {
    Ok(Some(FieldValue::Object(Arc::new(
        build_ownership_connection(
            db,
            actor,
            repo.id,
            &repo.name,
            commit,
            path,
            Surface::Blob,
            arguments,
        )?,
    ))))
}

/// `GitTree.ownership`.
pub fn git_tree_ownership(
    db: &OwnHandle,
    repo: &MinimalRepo,
    commit: &str,
    path: &str,
    arguments: &Arguments,
    actor: Actor,
) -> Result<Option<FieldValue>, ExecError> {
    Ok(Some(FieldValue::Object(Arc::new(
        build_ownership_connection(
            db,
            actor,
            repo.id,
            &repo.name,
            commit,
            path,
            Surface::Tree,
            arguments,
        )?,
    ))))
}

/// `GitCommit.ownership`.
pub fn git_commit_ownership(
    db: &OwnHandle,
    repo: &MinimalRepo,
    commit: &str,
    arguments: &Arguments,
    actor: Actor,
) -> Result<Option<FieldValue>, ExecError> {
    Ok(Some(FieldValue::Object(Arc::new(
        build_ownership_connection(
            db,
            actor,
            repo.id,
            &repo.name,
            commit,
            "",
            Surface::Commit,
            arguments,
        )?,
    ))))
}

/// `GitTree.ownershipStats`.
pub fn git_tree_ownership_stats(
    db: &OwnHandle,
    repo: &MinimalRepo,
    path: &str,
) -> Result<Option<FieldValue>, ExecError> {
    Ok(Some(FieldValue::Object(Arc::new(OwnershipStatsObject {
        db: Arc::clone(db),
        location: Some((repo.id, path.to_owned())),
        counts: OnceLock::new(),
        total_files: OnceLock::new(),
    }))))
}

/// `Repository.ingestedCodeowners`.
pub fn repository_ingested_codeowners(
    db: &OwnHandle,
    repo: &RepositoryData,
    actor: Actor,
) -> Result<Option<FieldValue>, ExecError> {
    // `RepoIngestedCodeowners`: open to anyone, NotFound → null.
    let Some(_repo) = db.repo_by_id(actor, repo.id).map_err(ExecError::new)? else {
        return Ok(Some(FieldValue::Null));
    };
    match db
        .codeowners_for_repo(repo.id)
        .map_err(ExecError::new)?
    {
        Some(file) => Ok(Some(fetch_codeowners_file(db, repo, file))),
        None => Ok(Some(FieldValue::Null)),
    }
}

