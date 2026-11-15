//! M9.47b differential of the GitTree finish fields: `richHTML`,
//! `rawZipArchiveURL`, entries-node `blame`, and the `languages` content
//! heuristics (`git_tree_entry.go:239-247,419-425`, `file.go:34-43`,
//! `git_blob.go`, `lib/codeintel/languages/languages.go:99`).
//!
//! Both sides drive the same corpus through their real resolver stacks with a
//! fake gitserver (`gitserver.NewMockClient` / [`GitObjectSource`]), writing
//! one JSON document per case.
//!
//! Env:
//!
//! * `SG_M947B_DIFF=1`
//! * `M947B_CORPUS` — path to `m947b_git_tree_finish_corpus.json`
//! * `M947B_OUT` — output directory (one `<name>.json` per case)

#![allow(clippy::unwrap_used)]

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use async_trait::async_trait;
use base64::Engine as _;
use serde::Deserialize;
use serde_json::Map;

use sg_git::{
    BlameHunk, CommitDetails, CommitSignature, CommitsOptions, FileStat, GitObject, Signature,
    Submodule,
};
use sg_graphql::exec::{execute_with, root_fields, Actor, ExecRequest};
use sg_graphql::schema::assemble_sdl;

use sg_service_frontend::git_commit_extra::GitCommitExtraSource;
use sg_service_frontend::git_graphql::{prefetch_git_with, GitObjectSource, GitSourceError};
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
  uri: String!
  url: String!
  commit(rev: String!, inputRevspec: String): GitCommit
}

type GitCommit {
  oid: GitObjectID!
  abbreviatedOID: String!
  url: String!
  canonicalURL: String!
  tree(path: String = \"\"): GitTree
  blob(path: String!): GitBlob
}

interface TreeEntry {
  path: String!
  name: String!
  isDirectory: Boolean!
  url: String!
}

type GitTree implements TreeEntry {
  path: String!
  name: String!
  isDirectory: Boolean!
  isRoot: Boolean!
  url: String!
  entries(first: Int, recursive: Boolean = false, ancestors: Boolean = false): [TreeEntry!]!
  files(first: Int, recursive: Boolean = false): [File!]!
  directories(first: Int, recursive: Boolean = false): [GitTree!]!
  history(first: Int, after: String): TreeEntryConnection!
  rawZipArchiveURL: String!
}

type File {
  path: String!
  name: String!
  isDirectory: Boolean!
  url: String!
  languages: [String!]!
}

type GitBlob implements TreeEntry {
  path: String!
  name: String!
  isDirectory: Boolean!
  url: String!
  content(startLine: Int, endLine: Int): String!
  byteSize: Int!
  totalLines: Int!
  binary: Boolean!
  languages: [String!]!
  richHTML(startLine: Int, endLine: Int): String!
  blame(startLine: Int, endLine: Int, ignoreWhitespace: Boolean = false): [Hunk!]!
  history(first: Int, after: String): TreeEntryConnection!
}

type TreeEntryConnection {
  nodes: [TreeEntry!]!
  totalCount: Int
  pageInfo: PageInfo!
}

type PageInfo {
  hasNextPage: Boolean!
  endCursor: String
}

type Hunk {
  startLine: Int!
  endLine: Int!
  startByte: Int!
  endByte: Int!
  rev: String!
  author: Signature!
  message: String!
  commit: GitCommit!
  filename: String!
}

type Signature {
  person: Person!
  date: String!
}

type Person {
  name: String!
  email: String!
  displayName: String!
}
";

#[derive(Clone, Deserialize)]
struct RepoFixture {
    id: i32,
    name: String,
    uri: String,
    #[serde(default)]
    description: String,
    #[serde(default)]
    fork: bool,
    #[serde(default)]
    archived: bool,
    #[serde(default)]
    private: bool,
    #[serde(default)]
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
struct SubmoduleFixture {
    url: String,
    path: String,
    commit_sha: String,
}

#[derive(Clone, Deserialize)]
struct EntryFixture {
    name: String,
    mode: u32,
    submodule: Option<SubmoduleFixture>,
}

#[derive(Clone, Deserialize)]
struct DirErrorFixture {
    message: String,
    #[serde(default)]
    not_exist: bool,
}

#[derive(Clone, Deserialize)]
struct ContentFixture {
    text: Option<String>,
    base64: Option<String>,
}

impl ContentFixture {
    fn bytes(&self) -> Vec<u8> {
        if let Some(encoded) = &self.base64 {
            return base64::engine::general_purpose::STANDARD
                .decode(encoded)
                .expect("base64");
        }
        self.text.clone().unwrap_or_default().into_bytes()
    }
}

#[derive(Clone, Deserialize)]
struct BlameFixture {
    start_line: u32,
    end_line: u32,
    start_byte: u32,
    end_byte: u32,
    commit_id: String,
    author: SignatureFixture,
    message: String,
    filename: String,
}

impl BlameFixture {
    fn hunk(&self) -> BlameHunk {
        BlameHunk {
            start_line: self.start_line,
            end_line: self.end_line,
            start_byte: self.start_byte,
            end_byte: self.end_byte,
            commit_id: self.commit_id.clone(),
            previous_commit: None,
            author: Signature {
                name: self.author.name.clone(),
                email: self.author.email.clone(),
                date: Some(prost_types::Timestamp {
                    seconds: self.author.date,
                    nanos: 0,
                }),
            },
            message: self.message.clone(),
            filename: self.filename.clone(),
        }
    }
}

#[derive(Deserialize)]
struct Case {
    name: String,
    query: String,
}

#[derive(Deserialize)]
struct Corpus {
    repos: Vec<RepoFixture>,
    #[serde(default)]
    revisions: HashMap<String, String>,
    #[serde(default)]
    revision_errors: HashMap<String, String>,
    #[serde(default)]
    commits: HashMap<String, CommitFixture>,
    #[serde(default)]
    commit_errors: HashMap<String, String>,
    #[serde(default)]
    entries: HashMap<String, EntryFixture>,
    #[serde(default)]
    entry_errors: HashMap<String, String>,
    #[serde(default)]
    dir_entries: HashMap<String, Vec<EntryFixture>>,
    #[serde(default)]
    dir_entry_errors: HashMap<String, DirErrorFixture>,
    #[serde(default)]
    contents: HashMap<String, ContentFixture>,
    #[serde(default)]
    content_errors: HashMap<String, String>,
    #[serde(default)]
    commits_log: HashMap<String, Vec<CommitFixture>>,
    #[serde(default)]
    commits_log_errors: HashMap<String, String>,
    #[serde(default)]
    blames: HashMap<String, Vec<BlameFixture>>,
    #[serde(default)]
    blame_errors: HashMap<String, String>,
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

fn key(repo: &str, spec: &str) -> String {
    format!("{repo}|{spec}")
}

fn entry_key(repo: &str, oid: &str, path: &str) -> String {
    format!("{repo}|{oid}|{path}")
}

fn entry_stat(fixture: &EntryFixture) -> FileStat {
    FileStat {
        name: fixture.name.clone(),
        size: 0,
        mode: fixture.mode,
        blob_oid: String::new(),
        submodule: fixture.submodule.as_ref().map(|submodule| Submodule {
            url: submodule.url.clone(),
            path: submodule.path.clone(),
            commit_sha: submodule.commit_sha.clone(),
        }),
    }
}

struct FakeGit {
    revisions: HashMap<String, String>,
    revision_errors: HashMap<String, String>,
    commits: HashMap<String, CommitFixture>,
    commit_errors: HashMap<String, String>,
    entries: HashMap<String, EntryFixture>,
    entry_errors: HashMap<String, String>,
    dir_entries: HashMap<String, Vec<EntryFixture>>,
    dir_entry_errors: HashMap<String, DirErrorFixture>,
    contents: HashMap<String, ContentFixture>,
    content_errors: HashMap<String, String>,
    commits_log: HashMap<String, Vec<CommitFixture>>,
    commits_log_errors: HashMap<String, String>,
    blames: HashMap<String, Vec<BlameFixture>>,
    blame_errors: HashMap<String, String>,
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

fn blame_key(
    repo: &str,
    oid: &str,
    path: &str,
    start_line: Option<i32>,
    end_line: Option<i32>,
    ignore_whitespace: bool,
) -> String {
    format!("{repo}|{oid}|{path}|{start_line:?}|{end_line:?}|{ignore_whitespace}")
}

#[async_trait]
impl GitObjectSource for FakeGit {
    async fn resolve_revision(
        &self,
        _repo_id: i32,
        repo_name: &str,
        rev: &str,
    ) -> Result<String, GitSourceError> {
        let k = key(repo_name, rev);
        if let Some(message) = self.revision_errors.get(&k) {
            return Err(GitSourceError::new(message.clone()));
        }
        if let Some(oid) = self.revisions.get(&k) {
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
        let k = key(repo_name, oid);
        if let Some(message) = self.commit_errors.get(&k) {
            return Err(GitSourceError::new(message.clone()));
        }
        if let Some(fixture) = self.commits.get(&k) {
            return Ok(CommitDetails {
                oid: fixture.oid.clone(),
                parents: fixture.parents.clone(),
                author: signature(&fixture.author),
                committer: fixture.committer.as_ref().map(signature),
                message: fixture.message.clone(),
            });
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
        Err(GitSourceError::revision_not_found(format!(
            "revision not found: {repo_name}@{revspec}"
        )))
    }

    async fn stat(
        &self,
        _repo_id: i32,
        repo_name: &str,
        oid: &str,
        path: &str,
    ) -> Result<FileStat, GitSourceError> {
        let k = entry_key(repo_name, oid, path);
        if let Some(message) = self.entry_errors.get(&k) {
            return Err(GitSourceError::new(message.clone()));
        }
        if let Some(fixture) = self.entries.get(&k) {
            return Ok(entry_stat(fixture));
        }
        Err(GitSourceError::not_exist(format!(
            "ls-tree: {repo_name}@{oid}:{path}: file does not exist"
        )))
    }

    async fn read_dir_stats(
        &self,
        _repo_id: i32,
        repo_name: &str,
        oid: &str,
        path: &str,
        recursive: bool,
    ) -> Result<Vec<FileStat>, GitSourceError> {
        let k = format!("{repo_name}|{oid}|{path}|{recursive}");
        if let Some(fixture) = self.dir_entry_errors.get(&k) {
            return if fixture.not_exist {
                Err(GitSourceError::not_exist(fixture.message.clone()))
            } else {
                Err(GitSourceError::new(fixture.message.clone()))
            };
        }
        Ok(self
            .dir_entries
            .get(&k)
            .map(|entries| entries.iter().map(entry_stat).collect())
            .unwrap_or_default())
    }

    async fn read_file(
        &self,
        _repo_id: i32,
        repo_name: &str,
        oid: &str,
        path: &str,
    ) -> Result<Vec<u8>, GitSourceError> {
        let k = entry_key(repo_name, oid, path);
        if let Some(message) = self.content_errors.get(&k) {
            return Err(GitSourceError::new(message.clone()));
        }
        if let Some(fixture) = self.contents.get(&k) {
            return Ok(fixture.bytes());
        }
        Err(GitSourceError::not_exist(format!(
            "open: {repo_name}@{oid}:{path}: file does not exist"
        )))
    }

    fn commit_extra(&self) -> Option<&dyn GitCommitExtraSource> {
        Some(self)
    }

    async fn blame(
        &self,
        _repo_id: i32,
        repo_name: &str,
        oid: &str,
        path: &str,
        start_line: Option<i32>,
        end_line: Option<i32>,
        ignore_whitespace: bool,
    ) -> Result<Vec<BlameHunk>, GitSourceError> {
        let k = blame_key(repo_name, oid, path, start_line, end_line, ignore_whitespace);
        if let Some(message) = self.blame_errors.get(&k) {
            return Err(GitSourceError::new(message.clone()));
        }
        let hunks = self
            .blames
            .get(&k)
            .cloned()
            .unwrap_or_default()
            .iter()
            .map(BlameFixture::hunk)
            .collect();
        Ok(hunks)
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
        let k = commits_key(repo_name, options);
        if let Some(message) = self.commits_log_errors.get(&k) {
            return Err(GitSourceError::new(message.clone()));
        }
        Ok(self
            .commits_log
            .get(&k)
            .cloned()
            .unwrap_or_default()
            .iter()
            .map(commit_details)
            .collect())
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

#[tokio::test]
async fn m947b_languages_harness() {
    if std::env::var("SG_M947B_DIFF").as_deref() != Ok("1") {
        return;
    }
    let corpus_path = std::env::var("M947B_CORPUS").expect("M947B_CORPUS");
    let out = std::env::var("M947B_OUT").expect("M947B_OUT");
    std::fs::create_dir_all(&out).expect("out dir");
    let corpus: Corpus =
        serde_json::from_slice(&std::fs::read(&corpus_path).expect("corpus")).expect("corpus json");
    let schema = assemble_sdl(SDL).expect("schema");

    let fake = FakeGit {
        revisions: corpus.revisions.clone(),
        revision_errors: corpus.revision_errors.clone(),
        commits: corpus.commits.clone(),
        commit_errors: corpus.commit_errors.clone(),
        entries: corpus.entries.clone(),
        entry_errors: corpus.entry_errors.clone(),
        dir_entries: corpus.dir_entries.clone(),
        dir_entry_errors: corpus.dir_entry_errors.clone(),
        contents: corpus.contents.clone(),
        content_errors: corpus.content_errors.clone(),
        commits_log: corpus.commits_log.clone(),
        commits_log_errors: corpus.commits_log_errors.clone(),
        blames: corpus.blames.clone(),
        blame_errors: corpus.blame_errors.clone(),
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
        let mut git = prefetch_git_with(
            &fake,
            &repositories,
            &HashMap::new(),
            &nodes,
            None,
            &fields,
            None,
            None,
        )
        .await;
        // Go's `conf.ExternalURLParsed` default when site config is unset.
        git.external_url = "http://example.com".to_owned();
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
