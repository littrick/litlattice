//! M9.48c differential of the Own ownership GraphQL surface against the Go
//! baseline (`cmd/frontend/internal/own/resolvers` + graph-gophers).
//!
//! Both sides execute the same query strings against in-memory fixtures. The
//! Rust side assembles the vendored `own.graphql` on top of the main OSS schema
//! and executes through the production `FrontendRootResolver` + own seam.
//!
//! Env:
//!
//! * `SG_M948C_OWN_DIFF=1`
//! * `M9_48C_OWN_CORPUS` — corpus JSON path
//! * `M9_48C_OWN_OUT` — output directory (one `<name>.json` per case)

#![allow(clippy::unwrap_used, clippy::too_many_arguments)]

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::{Arc, Mutex};

use serde::Deserialize;
use serde_json::Map;
use sg_graphql::exec::{execute_with, Actor, ExecRequest};
use sg_graphql::schema::{assemble, MAIN_SCHEMA, OUTBOUND_WEBHOOKS_SCHEMA, OWN_SCHEMA, VIEWER_SCHEMA};
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;

use sg_service_frontend::git_graphql::{CommitLookup, GitPrefetch, RevisionLookup};
use sg_service_frontend::graphql_resolvers::{
    FrontendRootResolver, RepoKey, RepoLookup, RepositoryData, UserData, UserObject,
};
use sg_service_frontend::own_graphql::{
    CodeownersFileRow, OwnGraphqlDatabase, OwnRuleset, OwnStatsCounts, OwnTeamRow, RecentAuthor,
    RecentView, RulesetSource,
};
use sg_service_frontend::search_owners::{
    parse_codeowners, AssignedOwners, AssignedTeams, Ruleset, CODEOWNERS_LOCATIONS,
};

#[derive(Deserialize)]
struct Corpus {
    #[serde(default)]
    now: Option<String>,
    #[serde(default)]
    dotcom: bool,
    #[serde(default)]
    fixtures: Fixtures,
    #[serde(default)]
    cases: Vec<Case>,
}

#[derive(Deserialize, Default)]
struct Fixtures {
    #[serde(default)]
    users: Vec<UserFixture>,
    #[serde(default)]
    teams: Vec<TeamFixture>,
    #[serde(default)]
    repos: Vec<RepoFixture>,
    #[serde(default)]
    codeowners: Vec<CodeownersFixture>,
    #[serde(default)]
    assigned_owners: Vec<AssignedOwnerFixture>,
    #[serde(default)]
    assigned_teams: Vec<AssignedTeamFixture>,
    #[serde(default)]
    signal_configs: Vec<SignalConfigFixture>,
    #[serde(default)]
    recent_authors: Vec<RecentAuthorFixture>,
    #[serde(default)]
    recent_views: Vec<RecentViewFixture>,
    #[serde(default)]
    stats: StatsFixture,
    #[serde(default)]
    total_files: TotalFilesFixture,
    #[serde(default)]
    git: GitFixture,
}

#[derive(Deserialize, Clone)]
struct UserFixture {
    id: i32,
    username: String,
    #[serde(default)]
    display_name: String,
    #[serde(default)]
    avatar_url: String,
    #[serde(default)]
    site_admin: bool,
    #[serde(default)]
    primary_email: String,
    #[serde(default)]
    verified_emails: Vec<String>,
    #[serde(default)]
    ownership_assign: bool,
}

#[derive(Deserialize, Clone)]
struct TeamFixture {
    id: i32,
    name: String,
    #[serde(default)]
    display_name: Option<String>,
    #[serde(default)]
    readonly: bool,
}

#[derive(Deserialize, Clone)]
struct RepoFixture {
    id: i32,
    name: String,
    #[serde(default)]
    external_service_type: String,
}

#[derive(Deserialize, Clone)]
struct CodeownersFixture {
    repo_id: i32,
    contents: String,
    #[serde(default)]
    created_at: Option<String>,
    #[serde(default)]
    updated_at: Option<String>,
}

#[derive(Deserialize, Clone)]
struct AssignedOwnerFixture {
    repo_id: i32,
    user_id: i32,
    path: String,
}

#[derive(Deserialize, Clone)]
struct AssignedTeamFixture {
    repo_id: i32,
    team_id: i32,
    path: String,
}

#[derive(Deserialize, Clone)]
struct SignalConfigFixture {
    name: String,
    #[serde(default)]
    description: String,
    #[serde(default)]
    enabled: bool,
    #[serde(default)]
    excluded_repo_patterns: Vec<String>,
}

#[derive(Deserialize, Clone)]
struct RecentAuthorFixture {
    repo_id: i32,
    path: String,
    author_email: String,
    #[serde(default)]
    contribution_count: i32,
}

#[derive(Deserialize, Clone)]
struct RecentViewFixture {
    repo_id: i32,
    path: String,
    user_id: i32,
    #[serde(default)]
    views_count: i32,
}

#[derive(Deserialize, Default)]
struct StatsFixture {
    #[serde(default)]
    instance: Option<StatsRow>,
    #[serde(default)]
    paths: Vec<PathStatsRow>,
}

#[derive(Deserialize, Default, Clone)]
struct StatsRow {
    #[serde(default)]
    codeowned: i32,
    #[serde(default)]
    assigned: i32,
    #[serde(default)]
    total_owned: i32,
    #[serde(default)]
    updated_at: Option<String>,
}

#[derive(Deserialize, Clone)]
struct PathStatsRow {
    repo_id: i32,
    path: String,
    #[serde(default)]
    codeowned: i32,
    #[serde(default)]
    assigned: i32,
    #[serde(default)]
    total_owned: i32,
    #[serde(default)]
    updated_at: Option<String>,
}

#[derive(Deserialize, Default)]
struct TotalFilesFixture {
    #[serde(default)]
    instance: Option<i32>,
    #[serde(default)]
    paths: Vec<PathCountRow>,
}

#[derive(Deserialize, Clone)]
struct PathCountRow {
    repo_id: i32,
    path: String,
    count: i32,
}

#[derive(Deserialize, Default)]
struct GitFixture {
    #[serde(default)]
    revisions: Vec<RevisionFixture>,
    #[serde(default)]
    tree_entries: Vec<TreeEntryFixture>,
    #[serde(default)]
    commits: Vec<CommitFixture>,
    #[serde(default)]
    files: Vec<GitFileFixture>,
}

#[derive(Deserialize, Clone)]
struct RevisionFixture {
    repo_id: i32,
    rev: String,
    oid: String,
}

#[derive(Deserialize, Clone)]
struct TreeEntryFixture {
    repo_id: i32,
    oid: String,
    path: String,
    #[serde(default)]
    is_dir: bool,
}

#[derive(Deserialize, Clone)]
struct CommitFixture {
    repo_id: i32,
    oid: String,
    #[serde(default)]
    message: String,
}

#[derive(Deserialize, Clone)]
struct GitFileFixture {
    repo_id: i32,
    commit: String,
    path: String,
    contents: String,
}

#[derive(Deserialize)]
struct Case {
    name: String,
    query: String,
    #[serde(default)]
    viewer: String,
    #[serde(default)]
    uid: i32,
    #[serde(default)]
    variables: serde_json::Value,
}

fn default_time() -> String {
    "2024-01-01T00:00:00Z".to_owned()
}

fn parse_time(value: &str) -> OffsetDateTime {
    OffsetDateTime::parse(value, &Rfc3339).expect("rfc3339 timestamp")
}

fn user_data(fixture: &UserFixture) -> UserData {
    let created = parse_time(&default_time());
    UserData {
        id: fixture.id,
        username: fixture.username.clone(),
        display_name: fixture.display_name.clone(),
        avatar_url: fixture.avatar_url.clone(),
        created_at: created,
        updated_at: created,
        site_admin: fixture.site_admin,
        tos_accepted: false,
        builtin_auth: false,
        scim_controlled: false,
        completions_quota: None,
        code_completions_quota: None,
        primary_email: fixture.primary_email.clone(),
    }
}

fn repo_data(fixture: &RepoFixture) -> RepositoryData {
    RepositoryData {
        id: fixture.id,
        name: fixture.name.clone(),
        uri: String::new(),
        description: String::new(),
        fork: false,
        archived: false,
        private: false,
        stars: 0,
        external_id: String::new(),
        external_service_type: fixture.external_service_type.clone(),
        external_service_id: String::new(),
    }
}

struct InMemoryOwn {
    now: OffsetDateTime,
    fixtures: Fixtures,
    dotcom: bool,
    codeowners: Mutex<Vec<CodeownersFileRow>>,
    signal_configs: Mutex<Vec<SignalConfigFixture>>,
    assigned_owners: Mutex<Vec<AssignedOwnerFixture>>,
    assigned_teams: Mutex<Vec<AssignedTeamFixture>>,
    assign_permitted: Mutex<BTreeMap<i32, bool>>,
}

impl InMemoryOwn {
    fn new(corpus: &Corpus) -> Self {
        let now = parse_time(corpus.now.as_deref().unwrap_or(&default_time()));
        let codeowners = corpus
            .fixtures
            .codeowners
            .iter()
            .map(|file| CodeownersFileRow {
                repo_id: file.repo_id,
                contents: file.contents.clone(),
                created_at: file
                    .created_at
                    .as_deref()
                    .map(parse_time)
                    .unwrap_or(now),
                updated_at: file
                    .updated_at
                    .as_deref()
                    .map(parse_time)
                    .unwrap_or(now),
            })
            .collect();
        let assign_permitted = corpus
            .fixtures
            .users
            .iter()
            .map(|user| (user.id, user.ownership_assign))
            .collect();
        Self {
            now,
            fixtures: Fixtures {
                users: corpus.fixtures.users.clone(),
                teams: corpus.fixtures.teams.clone(),
                repos: corpus.fixtures.repos.clone(),
                codeowners: Vec::new(),
                assigned_owners: Vec::new(),
                assigned_teams: Vec::new(),
                signal_configs: Vec::new(),
                recent_authors: corpus.fixtures.recent_authors.clone(),
                recent_views: corpus.fixtures.recent_views.clone(),
                stats: StatsFixture {
                    instance: corpus.fixtures.stats.instance.clone(),
                    paths: corpus.fixtures.stats.paths.clone(),
                },
                total_files: TotalFilesFixture {
                    instance: corpus.fixtures.total_files.instance,
                    paths: corpus.fixtures.total_files.paths.clone(),
                },
                git: GitFixture {
                    revisions: corpus.fixtures.git.revisions.clone(),
                    tree_entries: corpus.fixtures.git.tree_entries.clone(),
                    commits: corpus.fixtures.git.commits.clone(),
                    files: corpus.fixtures.git.files.clone(),
                },
            },
            dotcom: corpus.dotcom,
            codeowners: Mutex::new(codeowners),
            signal_configs: Mutex::new(corpus.fixtures.signal_configs.clone()),
            assigned_owners: Mutex::new(corpus.fixtures.assigned_owners.clone()),
            assigned_teams: Mutex::new(corpus.fixtures.assigned_teams.clone()),
            assign_permitted: Mutex::new(assign_permitted),
        }
    }

    fn user(&self, id: i32) -> Option<UserData> {
        self.fixtures
            .users
            .iter()
            .find(|user| user.id == id)
            .map(user_data)
    }

    fn repo(&self, id: i32) -> Option<RepositoryData> {
        self.fixtures
            .repos
            .iter()
            .find(|repo| repo.id == id)
            .map(repo_data)
    }

    fn stats_counts(&self, location: Option<(i32, String)>) -> OwnStatsCounts {
        let row: Option<StatsRow> = match &location {
            None => self.fixtures.stats.instance.clone(),
            Some((repo_id, path)) => self
                .fixtures
                .stats
                .paths
                .iter()
                .find(|row| row.repo_id == *repo_id && row.path == *path)
                .map(|row| StatsRow {
                    codeowned: row.codeowned,
                    assigned: row.assigned,
                    total_owned: row.total_owned,
                    updated_at: row.updated_at.clone(),
                }),
        };
        match row {
            Some(row) => OwnStatsCounts {
                codeowned: row.codeowned,
                assigned: row.assigned,
                total_owned: row.total_owned,
                updated_at: row.updated_at.as_deref().map(parse_time),
            },
            None => OwnStatsCounts::default(),
        }
    }
}

impl OwnGraphqlDatabase for InMemoryOwn {
    fn sourcegraph_dotcom_mode(&self) -> bool {
        self.dotcom
    }

    fn current_user(&self, actor: Actor) -> Result<Option<UserData>, String> {
        Ok(actor.uid().and_then(|uid| self.user(uid)))
    }

    fn check_ownership_assign_permission(&self, actor: Actor) -> Result<(), String> {
        if actor.is_internal() {
            return Ok(());
        }
        let uid = actor.uid().ok_or_else(|| "not authenticated".to_owned())?;
        if *self
            .assign_permitted
            .lock()
            .unwrap()
            .get(&uid)
            .unwrap_or(&false)
        {
            Ok(())
        } else {
            Err("user is missing permission OWNERSHIP#ASSIGN".to_owned())
        }
    }

    fn load_signal_configurations(
        &self,
    ) -> Result<Vec<sg_db::own_signal_configurations::SignalConfiguration>, String> {
        Ok(self
            .signal_configs
            .lock()
            .unwrap()
            .iter()
            .enumerate()
            .map(
                |(index, config)| sg_db::own_signal_configurations::SignalConfiguration {
                    id: index as i32 + 1,
                    name: config.name.clone(),
                    description: config.description.clone(),
                    excluded_repo_patterns: config.excluded_repo_patterns.clone(),
                    enabled: config.enabled,
                },
            )
            .collect())
    }

    fn update_signal_configuration(
        &self,
        name: &str,
        excluded_repo_patterns: &[String],
        enabled: bool,
    ) -> Result<(), String> {
        for config in self.signal_configs.lock().unwrap().iter_mut() {
            if config.name == name {
                config.excluded_repo_patterns = excluded_repo_patterns.to_vec();
                config.enabled = enabled;
            }
        }
        Ok(())
    }

    fn signal_enabled(&self, name: &str) -> Result<bool, String> {
        self.signal_configs
            .lock()
            .unwrap()
            .iter()
            .find(|config| config.name == name)
            .map(|config| config.enabled)
            .ok_or_else(|| "signal configuration not found".to_owned())
    }

    fn recent_authors(&self, repo_id: i32, path: &str) -> Result<Vec<RecentAuthor>, String> {
        Ok(self
            .fixtures
            .recent_authors
            .iter()
            .filter(|author| author.repo_id == repo_id && author.path == path)
            .map(|author| RecentAuthor {
                author_email: author.author_email.clone(),
                contribution_count: author.contribution_count,
            })
            .collect())
    }

    fn recent_views(&self, repo_id: i32, path: &str) -> Result<Vec<RecentView>, String> {
        Ok(self
            .fixtures
            .recent_views
            .iter()
            .filter(|view| view.repo_id == repo_id && view.path == path)
            .map(|view| RecentView {
                user_id: view.user_id,
                views_count: view.views_count,
            })
            .collect())
    }

    fn repo_by_id(&self, _actor: Actor, id: i32) -> Result<Option<RepositoryData>, String> {
        Ok(self.repo(id))
    }

    fn repo_by_name(&self, _actor: Actor, name: &str) -> Result<Option<RepositoryData>, String> {
        Ok(self
            .fixtures
            .repos
            .iter()
            .find(|repo| repo.name == name)
            .map(repo_data))
    }

    fn ruleset(
        &self,
        repo_id: i32,
        _repo_name: &str,
        commit: &str,
    ) -> Result<Option<OwnRuleset>, String> {
        if let Some(file) = self
            .codeowners
            .lock()
            .unwrap()
            .iter()
            .find(|file| file.repo_id == repo_id)
        {
            let parsed = parse_codeowners(&file.contents)?;
            return Ok(Some(OwnRuleset {
                ruleset: Ruleset::new(parsed.rules),
                source: RulesetSource::Ingested { repo_id },
            }));
        }
        for path in CODEOWNERS_LOCATIONS {
            if let Some(file) = self
                .fixtures
                .git
                .files
                .iter()
                .find(|file| {
                    file.repo_id == repo_id && file.commit == commit && file.path == path
                })
            {
                let parsed = parse_codeowners(&file.contents)?;
                return Ok(Some(OwnRuleset {
                    ruleset: Ruleset::new(parsed.rules),
                    source: RulesetSource::Git {
                        repo_id,
                        commit: commit.to_owned(),
                        path: path.to_owned(),
                    },
                }));
            }
        }
        Ok(None)
    }

    fn assigned_owners(&self, repo_id: i32) -> Result<AssignedOwners, String> {
        let mut assigned: AssignedOwners = std::collections::HashMap::new();
        for owner in self.assigned_owners.lock().unwrap().iter() {
            if owner.repo_id == repo_id {
                assigned
                    .entry(owner.path.clone())
                    .or_default()
                    .push(owner.user_id);
            }
        }
        Ok(assigned)
    }

    fn assigned_teams(&self, repo_id: i32) -> Result<AssignedTeams, String> {
        let mut assigned: AssignedTeams = std::collections::HashMap::new();
        for team in self.assigned_teams.lock().unwrap().iter() {
            if team.repo_id == repo_id {
                assigned
                    .entry(team.path.clone())
                    .or_default()
                    .push(team.team_id);
            }
        }
        Ok(assigned)
    }

    fn user_by_id(&self, id: i32) -> Result<Option<UserData>, String> {
        Ok(self.user(id))
    }

    fn user_by_username(&self, username: &str) -> Result<Option<UserData>, String> {
        Ok(self
            .fixtures
            .users
            .iter()
            .find(|user| user.username.eq_ignore_ascii_case(username))
            .map(user_data))
    }

    fn user_by_verified_email(&self, email: &str) -> Result<Option<UserData>, String> {
        Ok(self
            .fixtures
            .users
            .iter()
            .find(|user| {
                user.verified_emails.iter().any(|candidate| candidate == email)
                    || (user.primary_email == email)
            })
            .map(user_data))
    }

    fn team_by_id(&self, id: i32) -> Result<Option<OwnTeamRow>, String> {
        Ok(self
            .fixtures
            .teams
            .iter()
            .find(|team| team.id == id)
            .map(|team| OwnTeamRow {
                id: team.id,
                name: team.name.clone(),
                display_name: team.display_name.clone(),
                readonly: team.readonly,
            }))
    }

    fn team_by_name(&self, name: &str) -> Result<Option<OwnTeamRow>, String> {
        Ok(self
            .fixtures
            .teams
            .iter()
            .find(|team| team.name == name)
            .map(|team| OwnTeamRow {
                id: team.id,
                name: team.name.clone(),
                display_name: team.display_name.clone(),
                readonly: team.readonly,
            }))
    }

    fn ownership_stats(
        &self,
        location: Option<(i32, String)>,
    ) -> Result<OwnStatsCounts, String> {
        Ok(self.stats_counts(location))
    }

    fn total_files(&self, location: Option<(i32, String)>) -> Result<i32, String> {
        match location {
            None => Ok(self.fixtures.total_files.instance.unwrap_or(0)),
            Some((repo_id, path)) => Ok(self
                .fixtures
                .total_files
                .paths
                .iter()
                .find(|row| row.repo_id == repo_id && row.path == path)
                .map(|row| row.count)
                .unwrap_or(0)),
        }
    }

    fn list_codeowners(
        &self,
        cursor: i32,
        limit: Option<i32>,
    ) -> Result<(Vec<CodeownersFileRow>, i32), String> {
        let mut files: Vec<CodeownersFileRow> = self
            .codeowners
            .lock()
            .unwrap()
            .iter()
            .filter(|file| file.repo_id >= cursor)
            .cloned()
            .collect();
        files.sort_by_key(|file| file.repo_id);
        let mut next = 0;
        if let Some(limit) = limit.filter(|limit| *limit > 0) {
            if files.len() as i32 > limit {
                next = files[limit as usize].repo_id;
                files.truncate(limit as usize);
            }
        }
        Ok((files, next))
    }

    fn count_codeowners(&self) -> Result<i32, String> {
        Ok(self.codeowners.lock().unwrap().len() as i32)
    }

    fn codeowners_for_repo(&self, repo_id: i32) -> Result<Option<CodeownersFileRow>, String> {
        Ok(self
            .codeowners
            .lock()
            .unwrap()
            .iter()
            .find(|file| file.repo_id == repo_id)
            .cloned())
    }

    fn create_codeowners(
        &self,
        repo_id: i32,
        contents: &str,
        _proto: &[u8],
    ) -> Result<CodeownersFileRow, String> {
        let mut files = self.codeowners.lock().unwrap();
        if files.iter().any(|file| file.repo_id == repo_id) {
            return Err("codeowners file has already been ingested for this repository".to_owned());
        }
        let file = CodeownersFileRow {
            repo_id,
            contents: contents.to_owned(),
            created_at: self.now,
            updated_at: self.now,
        };
        files.push(file.clone());
        Ok(file)
    }

    fn update_codeowners(
        &self,
        repo_id: i32,
        contents: &str,
        _proto: &[u8],
    ) -> Result<CodeownersFileRow, String> {
        let mut files = self.codeowners.lock().unwrap();
        let Some(file) = files.iter_mut().find(|file| file.repo_id == repo_id) else {
            return Err(format!("codeowners file not found: {repo_id}"));
        };
        file.contents = contents.to_owned();
        file.updated_at = self.now;
        Ok(file.clone())
    }

    fn delete_codeowners(&self, repo_ids: &[i32]) -> Result<(), String> {
        let mut files = self.codeowners.lock().unwrap();
        let before = files.len();
        files.retain(|file| !repo_ids.contains(&file.repo_id));
        if files.len() == before {
            return Err("codeowners file not found".to_owned());
        }
        Ok(())
    }

    fn insert_assigned_owner(
        &self,
        user_id: i32,
        repo_id: i32,
        path: &str,
        _who: i32,
    ) -> Result<(), String> {
        let mut owners = self.assigned_owners.lock().unwrap();
        if !owners.iter().any(|owner| {
            owner.user_id == user_id && owner.repo_id == repo_id && owner.path == path
        }) {
            owners.push(AssignedOwnerFixture {
                repo_id,
                user_id,
                path: path.to_owned(),
            });
        }
        Ok(())
    }

    fn delete_assigned_owner(
        &self,
        user_id: i32,
        repo_id: i32,
        path: &str,
    ) -> Result<(), String> {
        let mut owners = self.assigned_owners.lock().unwrap();
        let before = owners.len();
        owners.retain(|owner| {
            !(owner.user_id == user_id && owner.repo_id == repo_id && owner.path == path)
        });
        if owners.len() == before {
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
        _who: i32,
    ) -> Result<(), String> {
        let mut teams = self.assigned_teams.lock().unwrap();
        if !teams.iter().any(|team| {
            team.team_id == team_id && team.repo_id == repo_id && team.path == path
        }) {
            teams.push(AssignedTeamFixture {
                repo_id,
                team_id,
                path: path.to_owned(),
            });
        }
        Ok(())
    }

    fn delete_assigned_team(
        &self,
        team_id: i32,
        repo_id: i32,
        path: &str,
    ) -> Result<(), String> {
        let mut teams = self.assigned_teams.lock().unwrap();
        let before = teams.len();
        teams.retain(|team| {
            !(team.team_id == team_id && team.repo_id == repo_id && team.path == path)
        });
        if teams.len() == before {
            return Err(format!(
                "cannot delete assigned owner team with ID={team_id} for \"{path}\" path for repo with ID={repo_id}"
            ));
        }
        Ok(())
    }
}

fn normalized(response: &sg_graphql::exec::ExecResponse) -> String {
    let mut entries: Vec<(&str, serde_json::Value)> = Vec::new();
    if !response.errors.is_empty() {
        entries.push((
            "errors",
            serde_json::Value::Array(
                response
                    .errors
                    .iter()
                    .map(|error| serde_json::json!({ "message": error.message }))
                    .collect(),
            ),
        ));
    }
    if let Some(data) = response.data_json() {
        entries.push(("data", serde_json::from_str(&data).unwrap()));
    }
    let map: BTreeMap<&str, serde_json::Value> = entries.into_iter().collect();
    serde_json::to_string(&map).unwrap()
}

fn build_git_prefetch(fixtures: &Fixtures) -> GitPrefetch {
    let mut prefetch = GitPrefetch::default();
    for revision in &fixtures.git.revisions {
        prefetch.revisions.insert(
            (revision.repo_id, revision.rev.clone()),
            RevisionLookup::Found(revision.oid.clone()),
        );
    }
    for entry in &fixtures.git.tree_entries {
        prefetch.tree_entries.insert(
            (entry.repo_id, entry.oid.clone(), entry.path.clone()),
            sg_service_frontend::git_graphql::TreeEntryLookup::Found(sg_git::FileStat::synthetic(
                &entry.path,
                entry.is_dir,
            )),
        );
    }
    for commit in &fixtures.git.commits {
        prefetch.commits.insert(
            (commit.repo_id, commit.oid.clone()),
            CommitLookup::Found(sg_git::CommitDetails {
                oid: commit.oid.clone(),
                parents: Vec::new(),
                author: sg_git::CommitSignature {
                    name: String::new(),
                    email: String::new(),
                    date: None,
                },
                committer: None,
                message: commit.message.clone(),
            }),
        );
    }
    prefetch
}

#[tokio::test(flavor = "multi_thread")]
async fn own_graphql_harness() {
    if std::env::var("SG_M948C_OWN_DIFF").as_deref() != Ok("1") {
        return;
    }
    let corpus_path = std::env::var("M9_48C_OWN_CORPUS").expect("M9_48C_OWN_CORPUS");
    let out = std::env::var("M9_48C_OWN_OUT").expect("M9_48C_OWN_OUT");
    std::fs::create_dir_all(&out).expect("out dir");
    let corpus: Corpus = serde_json::from_slice(&std::fs::read(&corpus_path).expect("corpus"))
        .expect("corpus json");
    let schema = assemble(&[
        MAIN_SCHEMA,
        OUTBOUND_WEBHOOKS_SCHEMA,
        VIEWER_SCHEMA,
        OWN_SCHEMA,
    ])
    .expect("schema");

    for case in &corpus.cases {
        let db: Arc<dyn OwnGraphqlDatabase + Send + Sync> = Arc::new(InMemoryOwn::new(&corpus));
        let (actor, user) = match case.viewer.as_str() {
            "anonymous" | "" => (Actor::Anonymous, None),
            "internal" => (Actor::Internal, None),
            _ => {
                let fixture = corpus
                    .fixtures
                    .users
                    .iter()
                    .find(|user| user.id == case.uid)
                    .expect("viewer fixture");
                let data = user_data(fixture);
                let object = UserObject::from_data_as_viewer(data.clone(), data.site_admin);
                (Actor::User { uid: case.uid }, Some(object))
            }
        };
        let repositories: std::collections::HashMap<RepoKey, RepoLookup> = corpus
            .fixtures
            .repos
            .iter()
            .map(|repo| {
                (
                    RepoKey::Name(repo.name.clone()),
                    RepoLookup::Found(repo_data(repo)),
                )
            })
            .collect();
        let mut prefetch = build_git_prefetch(&corpus.fixtures);
        prefetch.own = Some(Arc::clone(&db));
        let roots = FrontendRootResolver::with_user_and_repositories(user, repositories)
            .with_own(Some(db))
            .with_git(Some(Arc::new(prefetch)));
        let variables: Map<String, serde_json::Value> = case
            .variables
            .as_object()
            .cloned()
            .unwrap_or_default();
        let request = ExecRequest {
            query: &case.query,
            operation_name: "",
            variables: &variables,
            actor,
        };
        let response = execute_with(&schema, request, Some(&roots));
        let body = normalized(&response);
        std::fs::write(Path::new(&out).join(format!("{}.json", case.name)), &body).expect("write");
        println!("RUST {} len={}", case.name, body.len());
    }
    println!("RUST done {} cases", corpus.cases.len());
}
