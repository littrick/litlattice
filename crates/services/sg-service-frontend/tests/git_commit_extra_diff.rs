//! M9.8iz differential of the remaining `GitCommitResolver` fields
//! (`ancestors`, `diff`, `behindAhead`, `languages`, `languageStatistics`,
//! `fileNames`, `ownership`) against graph-gophers.
//!
//! Both sides drive the same corpus through their real resolver stacks with a
//! fake gitserver/inventory/own backend.
//!
//! Env:
//!
//! * `SG_M9IZ_DIFF=1`
//! * `M9IZ_CORPUS` — path to `corpus.json`
//! * `M9IZ_OUT` — output directory (one `<name>.json` per case)

#![allow(clippy::unwrap_used)]

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use async_trait::async_trait;
use serde::Deserialize;
use serde_json::Map;

use sg_git::{
    CommitDetails, CommitSignature, CommitsOptions, DiffRangeType, GitObject, GitObjectType,
    RawDiffOptions,
};
use sg_graphql::exec::{execute_with, root_fields, Actor, ExecRequest};
use sg_graphql::schema::assemble_sdl;

use sg_service_frontend::git_commit_extra::{GitCommitExtraSource, OwnershipSummary};
use sg_service_frontend::git_graphql::{prefetch_git, GitObjectSource, GitSourceError};
use sg_service_frontend::graphql_resolvers::{
    FrontendRootResolver, NodeRoots, RepoKey, RepoLookup, RepositoryData, UserData,
};
use sg_service_frontend::own_graphql::{
    CodeownersFileRow, OwnGraphqlDatabase, OwnRuleset, OwnStatsCounts, OwnTeamRow, RecentAuthor,
    RecentView,
};
use sg_service_frontend::search_owners::{AssignedOwners, AssignedTeams};
use time::OffsetDateTime;
use sg_service_frontend::search_graphql::LanguageStat;

const SDL: &str = "
schema { query: Query }

scalar GitObjectID

type Query {
  repository(name: String, cloneURL: String, uri: String): Repository
}

type Repository {
  id: ID!
  name: String!
  commit(rev: String!, inputRevspec: String): GitCommit
}

type GitCommit {
  id: ID!
  oid: GitObjectID!
  message: String!
  ancestors(first: Int, query: String, path: String, follow: Boolean!, after: String, afterCursor: String, before: String): GitCommitConnection!
  diff(base: String): RepositoryComparison!
  behindAhead(revspec: String!): BehindAheadCounts!
  fileNames: [String!]!
  languages: [String!]!
  languageStatistics: [LanguageStatistics!]!
  ownership(first: Int, after: String, reasons: [OwnershipReasonType!]): OwnershipConnection!
}

type GitCommitConnection {
  nodes: [GitCommit!]!
  totalCount: Int
  pageInfo: PageInfo!
}

type PageInfo {
  hasNextPage: Boolean!
  endCursor: String
}

type BehindAheadCounts {
  behind: Int!
  ahead: Int!
}

type LanguageStatistics {
  name: String!
  totalBytes: Float!
  totalLines: Int!
}

type RepositoryComparison {
  baseRepository: Repository!
  headRepository: Repository!
  range: GitRevisionRange!
  commits(first: Int, path: String): GitCommitConnection!
  fileDiffs(first: Int, after: String, paths: [String!]): FileDiffConnection!
}

type GitRevisionRange {
  expr: String!
  base: GitRevSpec!
  baseRevSpec: GitRevSpecExpr!
  head: GitRevSpec!
  headRevSpec: GitRevSpecExpr!
  mergeBase: GitObject
}

union GitRevSpec = GitRef | GitRevSpecExpr | GitObject

type GitRef {
  name: String!
}

type GitRevSpecExpr {
  expr: String!
  object: GitObject
}

type GitObject {
  oid: GitObjectID!
}

type FileDiffConnection {
  nodes: [FileDiff!]!
  totalCount: Int
  pageInfo: PageInfo!
  diffStat: DiffStat!
  rawDiff: String!
}

type FileDiff {
  oldPath: String
  newPath: String
  hunks: [FileDiffHunk!]!
  stat: DiffStat!
  internalID: String!
}

type FileDiffHunk {
  oldRange: FileDiffHunkRange!
  newRange: FileDiffHunkRange!
  section: String
  oldNoNewlineAt: Boolean!
  body: String!
}

type FileDiffHunkRange {
  startLine: Int!
  lines: Int!
}

type DiffStat {
  added: Int!
  deleted: Int!
}

type OwnershipConnection {
  totalCount: Int!
  totalOwners: Int!
  pageInfo: PageInfo!
  nodes: [Ownership!]!
}

type Ownership {
  owner: Owner!
  reasons: [OwnershipReason!]!
}

type Owner {
  ownerField: String!
}

type OwnershipReason {
  title: String!
  description: String!
}

enum OwnershipReasonType {
  CODEOWNERS_FILE_ENTRY
  ASSIGNED_OWNER
  RECENT_CONTRIBUTOR_OWNERSHIP_SIGNAL
  RECENT_VIEW_OWNERSHIP_SIGNAL
}
";

#[derive(Clone, Deserialize)]
struct RepoFixture {
    id: i32,
    name: String,
    uri: String,
    description: String,
    fork: bool,
    archived: bool,
    private: bool,
    stars: i32,
}

#[derive(Clone, Deserialize)]
struct SignatureFixture {
    name: String,
    email: String,
    date: i64,
}

#[derive(Clone, Deserialize)]
struct CommitFixture {
    oid: String,
    #[serde(default)]
    parents: Vec<String>,
    author: SignatureFixture,
    committer: Option<SignatureFixture>,
    message: String,
}

#[derive(Clone, Deserialize)]
struct BehindAheadFixture {
    behind: u32,
    ahead: u32,
}

#[derive(Clone, Deserialize)]
struct FileFixture {
    name: String,
    is_dir: bool,
}

#[derive(Clone, Deserialize)]
struct LangFixture {
    name: String,
    #[serde(rename = "totalBytes")]
    total_bytes: u64,
    #[serde(rename = "totalLines")]
    total_lines: u64,
}

#[derive(Clone, Deserialize)]
struct InventoryFixture {
    languages: Vec<LangFixture>,
}

#[derive(Clone, Deserialize)]
struct OwnershipFixture {
    #[serde(rename = "totalCount")]
    total_count: i32,
    #[serde(rename = "totalOwners")]
    total_owners: i32,
}

#[derive(Deserialize)]
struct Case {
    name: String,
    query: String,
}

#[derive(Deserialize)]
struct Corpus {
    repos: Vec<RepoFixture>,
    revisions: HashMap<String, String>,
    commits: HashMap<String, CommitFixture>,
    merge_bases: HashMap<String, String>,
    diffs: HashMap<String, String>,
    commits_log: HashMap<String, Vec<CommitFixture>>,
    behind_ahead: HashMap<String, BehindAheadFixture>,
    file_names: HashMap<String, Vec<FileFixture>>,
    inventories: HashMap<String, InventoryFixture>,
    ownership: OwnershipFixture,
    cases: Vec<Case>,
}

fn repo_data(fixture: &RepoFixture) -> RepositoryData {
    RepositoryData {
        id: fixture.id,
        name: fixture.name.clone(),
        uri: fixture.uri.clone(),
        description: fixture.description.clone(),
        fork: fixture.fork,
        archived: fixture.archived,
        private: fixture.private,
        stars: fixture.stars,
        external_id: String::new(),
        external_service_type: String::new(),
        external_service_id: String::new(),
    }
}

fn signature(fixture: &SignatureFixture) -> CommitSignature {
    CommitSignature {
        name: fixture.name.clone(),
        email: fixture.email.clone(),
        date: Some(prost_types::Timestamp {
            seconds: fixture.date,
            nanos: 0,
        }),
    }
}

fn commit_details(fixture: &CommitFixture) -> CommitDetails {
    CommitDetails {
        oid: fixture.oid.clone(),
        parents: fixture.parents.clone(),
        author: signature(&fixture.author),
        committer: fixture.committer.as_ref().map(signature),
        message: fixture.message.clone(),
    }
}

fn key(repo: &str, spec: &str) -> String {
    format!("{repo}|{spec}")
}

struct FakeGit {
    revisions: HashMap<String, String>,
    commits: HashMap<String, CommitFixture>,
    merge_bases: HashMap<String, String>,
    diffs: HashMap<String, String>,
    commits_log: HashMap<String, Vec<CommitFixture>>,
    behind_ahead: HashMap<String, BehindAheadFixture>,
    file_names: HashMap<String, Vec<FileFixture>>,
    inventories: HashMap<String, InventoryFixture>,
    ownership: OwnershipFixture,
}

fn commits_key(repo: &str, options: &CommitsOptions) -> String {
    format!(
        "{}|{}|{}|{}|{}|{}|{}",
        repo,
        options.ranges.join(","),
        options.max_commits,
        options.skip,
        options.message_query,
        options.path,
        options.follow_path_renames
    )
}

#[async_trait]
impl GitObjectSource for FakeGit {
    async fn resolve_revision(
        &self,
        _repo_id: i32,
        repo_name: &str,
        rev: &str,
    ) -> Result<String, GitSourceError> {
        if let Some(oid) = self.revisions.get(&key(repo_name, rev)) {
            return Ok(oid.clone());
        }
        Err(GitSourceError::revision_not_found(format!(
            "revision not found: {repo_name}@{rev}"
        )))
    }

    async fn get_commit(
        &self,
        _repo_id: i32,
        repo_name: &str,
        oid: &str,
    ) -> Result<CommitDetails, GitSourceError> {
        if let Some(fixture) = self.commits.get(&key(repo_name, oid)) {
            return Ok(commit_details(fixture));
        }
        Err(GitSourceError::revision_not_found(format!(
            "revision not found: {repo_name}@{oid}"
        )))
    }

    async fn get_object(
        &self,
        _repo_id: i32,
        repo_name: &str,
        revspec: &str,
    ) -> Result<GitObject, GitSourceError> {
        if let Some(oid) = self.revisions.get(&key(repo_name, revspec)) {
            return Ok(GitObject {
                oid: oid.clone(),
                object_type: GitObjectType::Commit,
            });
        }
        Err(GitSourceError::revision_not_found(format!(
            "revision not found: {repo_name}@{revspec}"
        )))
    }

    fn commit_extra(&self) -> Option<&dyn GitCommitExtraSource> {
        Some(self)
    }
}

#[async_trait]
impl GitCommitExtraSource for FakeGit {
    async fn commit_log(
        &self,
        _repo_id: i32,
        repo_name: &str,
        options: &CommitsOptions,
    ) -> Result<Vec<CommitDetails>, GitSourceError> {
        let fixtures = self
            .commits_log
            .get(&commits_key(repo_name, options))
            .cloned()
            .unwrap_or_default();
        Ok(fixtures.iter().map(commit_details).collect())
    }

    async fn merge_base(
        &self,
        _repo_id: i32,
        repo_name: &str,
        base: &str,
        head: &str,
    ) -> Result<String, GitSourceError> {
        self.merge_bases
            .get(&format!("{repo_name}|{base}|{head}"))
            .cloned()
            .ok_or_else(|| GitSourceError::new(format!("merge base not found: {base}|{head}")))
    }

    async fn raw_diff(
        &self,
        _repo_id: i32,
        repo_name: &str,
        options: &RawDiffOptions,
    ) -> Result<Vec<u8>, GitSourceError> {
        let range = match options.range_type {
            Some(DiffRangeType::OnlyInHead) => "..",
            Some(DiffRangeType::Intersection) | None => "...",
        };
        let k = format!(
            "{}|{}|{}|{}|{}",
            repo_name,
            options.base,
            options.head,
            range,
            options.paths.join(",")
        );
        self.diffs
            .get(&k)
            .map(|diff| diff.clone().into_bytes())
            .ok_or_else(|| GitSourceError::new(format!("diff not found: {k}")))
    }

    async fn behind_ahead(
        &self,
        _repo_id: i32,
        repo_name: &str,
        left: &str,
        right: &str,
    ) -> Result<(u32, u32), GitSourceError> {
        let k = format!("{repo_name}|{left}|{right}");
        self.behind_ahead
            .get(&k)
            .map(|fixture| (fixture.behind, fixture.ahead))
            .ok_or_else(|| GitSourceError::new(format!("behind ahead not found: {k}")))
    }

    async fn file_names(
        &self,
        _repo_id: i32,
        repo_name: &str,
        commit: &str,
    ) -> Result<Vec<String>, GitSourceError> {
        let k = key(repo_name, commit);
        let fixtures = self
            .file_names
            .get(&k)
            .ok_or_else(|| GitSourceError::new(format!("read dir not found: {k}")))?;
        Ok(fixtures
            .iter()
            .filter(|fixture| !fixture.is_dir)
            .map(|fixture| fixture.name.clone())
            .collect())
    }

    async fn language_inventory(
        &self,
        _repo_id: i32,
        repo_name: &str,
        commit: &str,
    ) -> Result<Vec<LanguageStat>, GitSourceError> {
        let k = key(repo_name, commit);
        let fixture = self
            .inventories
            .get(&k)
            .ok_or_else(|| GitSourceError::new(format!("inventory not found: {k}")))?;
        Ok(fixture
            .languages
            .iter()
            .map(|language| LanguageStat {
                name: language.name.clone(),
                total_bytes: language.total_bytes,
                total_lines: language.total_lines as i64,
            })
            .collect())
    }

    async fn commit_ownership(
        &self,
        _repo_id: i32,
        _repo_name: &str,
        _commit: &str,
    ) -> Result<OwnershipSummary, GitSourceError> {
        Ok(OwnershipSummary {
            total_count: self.ownership.total_count,
            total_owners: self.ownership.total_owners,
        })
    }
}


/// M9.48c moved `GitCommit.ownership` onto the own GraphQL seam. Commit
/// ownership only reads the recent-contributor and recent-view signals (the
/// baseline `GitCommitOwnership` never evaluates CODEOWNERS/assigned owners),
/// so this fake supplies exactly those.
struct FakeOwn {
    repo: RepositoryData,
    viewer: UserData,
}

impl OwnGraphqlDatabase for FakeOwn {
    fn sourcegraph_dotcom_mode(&self) -> bool {
        false
    }
    fn current_user(&self, _actor: Actor) -> Result<Option<UserData>, String> {
        Ok(None)
    }
    fn check_ownership_assign_permission(&self, _actor: Actor) -> Result<(), String> {
        Ok(())
    }
    fn load_signal_configurations(
        &self,
    ) -> Result<Vec<sg_db::own_signal_configurations::SignalConfiguration>, String> {
        Ok(Vec::new())
    }
    fn update_signal_configuration(
        &self,
        _name: &str,
        _excluded_repo_patterns: &[String],
        _enabled: bool,
    ) -> Result<(), String> {
        Ok(())
    }
    fn signal_enabled(&self, _name: &str) -> Result<bool, String> {
        Ok(true)
    }
    fn recent_authors(&self, _repo_id: i32, _path: &str) -> Result<Vec<RecentAuthor>, String> {
        Ok(vec![
            RecentAuthor { author_email: "alice@example.com".to_owned(), contribution_count: 2 },
            RecentAuthor { author_email: "bob@example.com".to_owned(), contribution_count: 1 },
        ])
    }
    fn recent_views(&self, _repo_id: i32, _path: &str) -> Result<Vec<RecentView>, String> {
        Ok(vec![RecentView { user_id: self.viewer.id, views_count: 1 }])
    }
    fn repo_by_id(&self, _actor: Actor, id: i32) -> Result<Option<RepositoryData>, String> {
        Ok((id == self.repo.id).then(|| self.repo.clone()))
    }
    fn repo_by_name(&self, _actor: Actor, name: &str) -> Result<Option<RepositoryData>, String> {
        Ok((name == self.repo.name).then(|| self.repo.clone()))
    }
    fn ruleset(
        &self,
        _repo_id: i32,
        _repo_name: &str,
        _commit: &str,
    ) -> Result<Option<OwnRuleset>, String> {
        Ok(None)
    }
    fn assigned_owners(&self, _repo_id: i32) -> Result<AssignedOwners, String> {
        Ok(AssignedOwners::new())
    }
    fn assigned_teams(&self, _repo_id: i32) -> Result<AssignedTeams, String> {
        Ok(AssignedTeams::new())
    }
    fn user_by_id(&self, id: i32) -> Result<Option<UserData>, String> {
        Ok((id == self.viewer.id).then(|| self.viewer.clone()))
    }
    fn user_by_username(&self, _username: &str) -> Result<Option<UserData>, String> {
        Ok(None)
    }
    fn user_by_verified_email(&self, _email: &str) -> Result<Option<UserData>, String> {
        Ok(None)
    }
    fn team_by_id(&self, _id: i32) -> Result<Option<OwnTeamRow>, String> {
        Ok(None)
    }
    fn team_by_name(&self, _name: &str) -> Result<Option<OwnTeamRow>, String> {
        Ok(None)
    }
    fn ownership_stats(
        &self,
        _location: Option<(i32, String)>,
    ) -> Result<OwnStatsCounts, String> {
        Ok(OwnStatsCounts::default())
    }
    fn total_files(&self, _location: Option<(i32, String)>) -> Result<i32, String> {
        Ok(0)
    }
    fn list_codeowners(
        &self,
        _cursor: i32,
        _limit: Option<i32>,
    ) -> Result<(Vec<CodeownersFileRow>, i32), String> {
        Ok((Vec::new(), 0))
    }
    fn count_codeowners(&self) -> Result<i32, String> {
        Ok(0)
    }
    fn codeowners_for_repo(&self, _repo_id: i32) -> Result<Option<CodeownersFileRow>, String> {
        Ok(None)
    }
    fn create_codeowners(
        &self,
        _repo_id: i32,
        _contents: &str,
        _proto: &[u8],
    ) -> Result<CodeownersFileRow, String> {
        Err("not implemented".to_owned())
    }
    fn update_codeowners(
        &self,
        _repo_id: i32,
        _contents: &str,
        _proto: &[u8],
    ) -> Result<CodeownersFileRow, String> {
        Err("not implemented".to_owned())
    }
    fn delete_codeowners(&self, _repo_ids: &[i32]) -> Result<(), String> {
        Ok(())
    }
    fn insert_assigned_owner(
        &self,
        _user_id: i32,
        _repo_id: i32,
        _path: &str,
        _who: i32,
    ) -> Result<(), String> {
        Ok(())
    }
    fn delete_assigned_owner(
        &self,
        _user_id: i32,
        _repo_id: i32,
        _path: &str,
    ) -> Result<(), String> {
        Ok(())
    }
    fn insert_assigned_team(
        &self,
        _team_id: i32,
        _repo_id: i32,
        _path: &str,
        _who: i32,
    ) -> Result<(), String> {
        Ok(())
    }
    fn delete_assigned_team(
        &self,
        _team_id: i32,
        _repo_id: i32,
        _path: &str,
    ) -> Result<(), String> {
        Ok(())
    }
}

#[tokio::test]
async fn git_commit_extra_harness() {
    if std::env::var("SG_M9IZ_DIFF").as_deref() != Ok("1") {
        return;
    }
    let corpus_path = std::env::var("M9IZ_CORPUS").expect("M9IZ_CORPUS");
    let out = std::env::var("M9IZ_OUT").expect("M9IZ_OUT");
    std::fs::create_dir_all(&out).expect("out dir");
    let corpus: Corpus =
        serde_json::from_slice(&std::fs::read(&corpus_path).expect("corpus")).expect("corpus json");
    let schema = assemble_sdl(SDL).expect("schema");

    let fake = FakeGit {
        revisions: corpus.revisions.clone(),
        commits: corpus.commits.clone(),
        merge_bases: corpus.merge_bases.clone(),
        diffs: corpus.diffs.clone(),
        commits_log: corpus.commits_log.clone(),
        behind_ahead: corpus.behind_ahead.clone(),
        file_names: corpus.file_names.clone(),
        inventories: corpus.inventories.clone(),
        ownership: corpus.ownership.clone(),
    };

    let mut repositories: HashMap<RepoKey, RepoLookup> = HashMap::new();
    let mut nodes = NodeRoots::default();
    for fixture in &corpus.repos {
        let data = repo_data(fixture);
        repositories.insert(
            RepoKey::Name(fixture.name.clone()),
            RepoLookup::Found(data.clone()),
        );
        nodes
            .repositories
            .insert(fixture.id, RepoLookup::Found(data));
    }

    for case in &corpus.cases {
        let variables = Map::new();
        let request = ExecRequest {
            query: &case.query,
            operation_name: "",
            variables: &variables,
            actor: Actor::Anonymous,
        };
        let fields = root_fields(&schema, &request);
        let mut git =
            prefetch_git(&fake, &repositories, &HashMap::new(), &nodes, None, &fields).await;
        if let Some(repo_fixture) = corpus.repos.first().cloned() {
            git.own = Some(Arc::new(FakeOwn {
                repo: RepositoryData {
                    id: repo_fixture.id,
                    name: repo_fixture.name.clone(),
                    uri: repo_fixture.uri.clone(),
                    description: String::new(),
                    fork: false,
                    archived: false,
                    private: false,
                    stars: 0,
                    external_id: String::new(),
                    external_service_type: "github".to_owned(),
                    external_service_id: String::new(),
                },
                viewer: UserData {
                    id: 9001,
                    username: "viewer".to_owned(),
                    display_name: "Viewer".to_owned(),
                    avatar_url: String::new(),
                    created_at: OffsetDateTime::UNIX_EPOCH,
                    updated_at: OffsetDateTime::UNIX_EPOCH,
                    site_admin: false,
                    tos_accepted: false,
                    primary_email: String::new(),
                },
            }));
        }
        let roots = FrontendRootResolver::with_prefetched_organizations(
            None,
            repositories.clone(),
            HashMap::new(),
            HashMap::new(),
            HashMap::new(),
            HashMap::new(),
            false,
        )
        .with_nodes(nodes.clone())
        .with_git(Some(Arc::new(git)));
        let response = execute_with(&schema, request, Some(&roots));
        let body = response.to_json();
        std::fs::write(Path::new(&out).join(format!("{}.json", case.name)), &body).expect("write");
        println!("RUST {} len={}", case.name, body.len());
    }
}
