//! M9.8jd differential of `Repository.comparison`, the full
//! `RepositoryComparison` surface and the remaining `FileDiff` fields
//! (`oldFile`/`newFile`/`mostRelevantFile`, hunk `highlight`, `stat`,
//! `internalID`, range/revspec objects) against graph-gophers.
//!
//! Both sides drive the same corpus through their real resolver stacks with a
//! fake gitserver. Highlighting is limited to the offline `HTML_PLAINTEXT`
//! subset (`highlight.CodeAsLines` without gosyntect).
//!
//! Env:
//!
//! * `SG_M9JD_DIFF=1`
//! * `M9JD_CORPUS` — path to `corpus.json`
//! * `M9JD_OUT` — output directory (one `<name>.json` per case)

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

use sg_service_frontend::git_commit_extra::GitCommitExtraSource;
use sg_service_frontend::git_graphql::{prefetch_git, GitObjectSource, GitSourceError};
use sg_service_frontend::graphql_resolvers::{
    FrontendRootResolver, NodeRoots, RepoKey, RepoLookup, RepositoryData,
};

const SDL: &str = "
schema { query: Query }

scalar GitObjectID

type Query {
  repository(name: String, cloneURL: String, uri: String): Repository
}

type Repository {
  id: ID!
  name: String!
  comparison(base: String, head: String, fetchMissing: Boolean = true): RepositoryComparison!
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

type GitCommitConnection {
  nodes: [GitCommit!]!
  totalCount: Int
  pageInfo: PageInfo!
}

type GitCommit {
  oid: GitObjectID!
  message: String!
}

type PageInfo {
  hasNextPage: Boolean!
  endCursor: String
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
  oldFile: File2
  newFile: File2
  mostRelevantFile: File2!
  hunks: [FileDiffHunk!]!
  stat: DiffStat!
  internalID: String!
}

interface File2 {
  path: String!
  name: String!
  isDirectory: Boolean!
  content(startLine: Int, endLine: Int): String!
  byteSize: Int!
  totalLines: Int!
  binary: Boolean!
  url: String!
}

type GitBlob implements File2 {
  path: String!
  name: String!
  isDirectory: Boolean!
  content(startLine: Int, endLine: Int): String!
  byteSize: Int!
  totalLines: Int!
  binary: Boolean!
  url: String!
}

type FileDiffHunk {
  oldRange: FileDiffHunkRange!
  newRange: FileDiffHunkRange!
  section: String
  oldNoNewlineAt: Boolean!
  body: String!
  highlight(
    disableTimeout: Boolean!
    highlightLongLines: Boolean = false
    format: HighlightResponseFormat = HTML_HIGHLIGHT
    startLine: Int
    endLine: Int
  ): HighlightedDiffHunkBody!
}

type FileDiffHunkRange {
  startLine: Int!
  lines: Int!
}

type HighlightedDiffHunkBody {
  aborted: Boolean!
  lines: [HighlightedDiffHunkLine!]!
}

type HighlightedDiffHunkLine {
  html: String!
  kind: DiffHunkLineType!
}

enum DiffHunkLineType {
  UNCHANGED
  ADDED
  DELETED
}

enum HighlightResponseFormat {
  HTML_HIGHLIGHT
  HTML_PLAINTEXT
  JSON_DEBUG
}

type DiffStat {
  added: Int!
  deleted: Int!
}
";

#[derive(Deserialize)]
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
    contents: HashMap<String, String>,
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
    contents: HashMap<String, String>,
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

    async fn read_file(
        &self,
        _repo_id: i32,
        repo_name: &str,
        oid: &str,
        path: &str,
    ) -> Result<Vec<u8>, GitSourceError> {
        let k = format!("{repo_name}|{oid}|{path}");
        self.contents
            .get(&k)
            .map(|text| text.clone().into_bytes())
            .ok_or_else(|| GitSourceError::new(format!("file not found: {k}")))
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
}

#[tokio::test]
async fn comparison_harness() {
    if std::env::var("SG_M9JD_DIFF").as_deref() != Ok("1") {
        return;
    }
    let corpus_path = std::env::var("M9JD_CORPUS").expect("M9JD_CORPUS");
    let out = std::env::var("M9JD_OUT").expect("M9JD_OUT");
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
        contents: corpus.contents.clone(),
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
        let git = prefetch_git(&fake, &repositories, &HashMap::new(), &nodes, None, &fields).await;
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
