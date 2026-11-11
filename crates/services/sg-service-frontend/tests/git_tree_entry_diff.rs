//! M9.8iy differential of the GraphQL tree entry / symbols resolvers against
//! graph-gophers: `GitTreeEntryResolver` (`git_tree_entry.go`),
//! `GitCommitResolver`'s `Tree`/`Blob`/`File`/`Path` (`git_commit.go:240-308`)
//! and `symbols.go`.
//!
//! Both sides drive the same corpus through their real resolver stacks. The Go
//! oracle swaps `gitserver.NewMockClient` and runs an in-process fake symbols
//! gRPC service; this harness injects the equivalent [`GitObjectSource`] into
//! [`prefetch_git_with`] together with a synchronous fake [`SymbolsClient`] and
//! a fake [`CloneUrlResolver`].
//!
//! Env:
//!
//! * `SG_M9IY_DIFF=1`
//! * `M9IY_CORPUS` — path to `corpus.json`
//! * `M9IY_OUT` — output directory (one `<name>.json` per case)

#![allow(clippy::unwrap_used)]

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use async_trait::async_trait;
use base64::Engine as _;
use regex::Regex;
use serde::Deserialize;
use serde_json::Map;

use sg_git::{CommitDetails, CommitSignature, FileStat, GitObject, Submodule};
use sg_graphql::exec::{execute_with, root_fields, Actor, ExecRequest};
use sg_graphql::schema::assemble_sdl;

use sg_service_frontend::git_graphql::{prefetch_git_with, GitObjectSource, GitSourceError};
use sg_service_frontend::graphql_resolvers::{
    CloneUrlResolver, FrontendRootResolver, NodeRoots, RepoKey, RepoLookup, RepositoryData,
};
use sg_service_frontend::search_searcher_client::{
    SearcherError, SearcherErrorKind, SymbolsClient, SymbolsParameters, SymbolsResponse,
};
use sg_service_frontend::search_stream::convert;

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
  path(path: String = \"\"): GitTreeOrBlob
  tree(path: String = \"\"): GitTree
  blob(path: String!): GitBlob
  file(path: String!): File2
  symbols(first: Int, query: String, includePatterns: [String!]): SymbolConnection!
}

interface TreeEntry {
  path: String!
  name: String!
  isDirectory: Boolean!
  url: String!
  canonicalURL: String!
  externalURLs: [ExternalLink!]!
  symbols(first: Int, query: String): SymbolConnection!
  submodule: Submodule
  isSingleChild: Boolean!
  commit: GitCommit!
}

type GitTree implements TreeEntry {
  path: String!
  name: String!
  isDirectory: Boolean!
  isRoot: Boolean!
  url: String!
  canonicalURL: String!
  externalURLs: [ExternalLink!]!
  symbols(first: Int, query: String): SymbolConnection!
  submodule: Submodule
  isSingleChild: Boolean!
  commit: GitCommit!
}

type GitBlob implements TreeEntry & File2 {
  path: String!
  name: String!
  isDirectory: Boolean!
  content(startLine: Int, endLine: Int): String!
  byteSize: Int!
  totalLines: Int!
  binary: Boolean!
  url: String!
  canonicalURL: String!
  externalURLs: [ExternalLink!]!
  symbols(first: Int, query: String): SymbolConnection!
  symbol(line: Int!, character: Int!): Symbol
  submodule: Submodule
  isSingleChild: Boolean!
  commit: GitCommit!
  repository: Repository!
}

interface File2 {
  path: String!
  name: String!
  isDirectory: Boolean!
  content(startLine: Int, endLine: Int): String!
  url: String!
}

union GitTreeOrBlob = GitTree | GitBlob

type Submodule {
  url: String!
  commit: String!
  path: String!
}

type SymbolConnection {
  nodes: [Symbol!]!
  pageInfo: PageInfo!
}

type Symbol {
  name: String!
  containerName: String
  kind: SymbolKind!
  language: String!
  location: Location!
  url: String!
  canonicalURL: String!
  fileLocal: Boolean!
}

enum SymbolKind {
  UNKNOWN
  FILE
  MODULE
  NAMESPACE
  PACKAGE
  CLASS
  METHOD
  PROPERTY
  FIELD
  CONSTRUCTOR
  ENUM
  INTERFACE
  FUNCTION
  VARIABLE
  CONSTANT
  STRING
  NUMBER
  BOOLEAN
  ARRAY
  OBJECT
  KEY
  NULL
  ENUM_MEMBER
  STRUCT
  EVENT
  OPERATOR
  TYPE_PARAMETER
}

type Location {
  resource: CodeIntelGitBlob!
  range: Range!
  url: String!
  canonicalURL: String!
}

type CodeIntelGitBlob {
  path: String!
  name: String!
  url: String!
}

type Range {
  start: Position!
  end: Position!
}

type Position {
  line: Int!
  character: Int!
}

type PageInfo {
  hasNextPage: Boolean!
  endCursor: String
}

type ExternalLink {
  url: String!
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
struct SymbolFixture {
    name: String,
    path: String,
    line: i32,
    character: i32,
    kind: String,
    language: String,
    #[serde(default)]
    parent: String,
    #[serde(default)]
    parent_kind: String,
    #[serde(default)]
    file_limited: bool,
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
    contents: HashMap<String, ContentFixture>,
    #[serde(default)]
    content_errors: HashMap<String, String>,
    #[serde(default)]
    dirs: HashMap<String, Vec<String>>,
    #[serde(default)]
    dir_errors: HashMap<String, String>,
    #[serde(default)]
    submodule_repos: HashMap<String, String>,
    #[serde(default)]
    symbols: HashMap<String, Vec<SymbolFixture>>,
    #[serde(default)]
    symbol_errors: HashMap<String, String>,
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

struct FakeGit {
    revisions: HashMap<String, String>,
    revision_errors: HashMap<String, String>,
    commits: HashMap<String, CommitFixture>,
    commit_errors: HashMap<String, String>,
    entries: HashMap<String, EntryFixture>,
    entry_errors: HashMap<String, String>,
    contents: HashMap<String, ContentFixture>,
    content_errors: HashMap<String, String>,
    dirs: HashMap<String, Vec<String>>,
    dir_errors: HashMap<String, String>,
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
            return Ok(FileStat {
                name: fixture.name.clone(),
                size: 0,
                mode: fixture.mode,
                blob_oid: String::new(),
                submodule: fixture.submodule.as_ref().map(|submodule| Submodule {
                    url: submodule.url.clone(),
                    path: submodule.path.clone(),
                    commit_sha: submodule.commit_sha.clone(),
                }),
            });
        }
        Err(GitSourceError::not_exist(format!(
            "ls-tree: {repo_name}@{oid}:{path}: file does not exist"
        )))
    }

    async fn read_dir(
        &self,
        _repo_id: i32,
        repo_name: &str,
        oid: &str,
        path: &str,
        _recursive: bool,
    ) -> Result<Vec<String>, GitSourceError> {
        let k = entry_key(repo_name, oid, path);
        if let Some(message) = self.dir_errors.get(&k) {
            return Err(GitSourceError::new(message.clone()));
        }
        Ok(self.dirs.get(&k).cloned().unwrap_or_default())
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
}

struct FakeSymbols {
    symbols: HashMap<String, Vec<SymbolFixture>>,
    errors: HashMap<String, String>,
}

impl SymbolsClient for FakeSymbols {
    fn search(&self, params: &SymbolsParameters) -> Result<SymbolsResponse, SearcherError> {
        let k = key(&params.repo, &params.commit_id);
        if let Some(message) = self.errors.get(&k) {
            return Err(SearcherError::new(
                SearcherErrorKind::Other,
                message.clone(),
            ));
        }
        let query = params.query.to_lowercase();
        let mut matched: Vec<(convert::Symbol, bool)> = Vec::new();
        for fixture in self.symbols.get(&k).map(Vec::as_slice).unwrap_or(&[]) {
            if !query.is_empty() && !fixture.name.to_lowercase().contains(&query) {
                continue;
            }
            let mut keep = true;
            for pattern in &params.include_patterns {
                match Regex::new(pattern) {
                    Ok(regex) if regex.is_match(&fixture.path) => {}
                    _ => {
                        keep = false;
                        break;
                    }
                }
            }
            if !keep {
                continue;
            }
            matched.push((
                convert::Symbol {
                    name: fixture.name.clone(),
                    path: fixture.path.clone(),
                    line: fixture.line,
                    character: fixture.character,
                    kind: fixture.kind.clone(),
                    language: fixture.language.clone(),
                    parent: fixture.parent.clone(),
                    parent_kind: fixture.parent_kind.clone(),
                },
                fixture.file_limited,
            ));
        }
        let mut limit_hit = false;
        if params.first >= 0 && matched.len() > params.first as usize {
            matched.truncate(params.first as usize);
            limit_hit = true;
        }
        let (symbols, file_limited) = matched.into_iter().unzip();
        Ok(SymbolsResponse {
            symbols,
            limit_hit,
            file_limited,
        })
    }
}

struct FakeCloneUrls {
    repos: HashMap<String, String>,
}

#[async_trait]
impl CloneUrlResolver for FakeCloneUrls {
    async fn repo_name_by_clone_url(&self, clone_url: &str) -> Result<Option<String>, String> {
        Ok(self.repos.get(clone_url).cloned())
    }
}

#[tokio::test]
async fn git_tree_entry_symbols_harness() {
    if std::env::var("SG_M9IY_DIFF").as_deref() != Ok("1") {
        return;
    }
    let corpus_path = std::env::var("M9IY_CORPUS").expect("M9IY_CORPUS");
    let out = std::env::var("M9IY_OUT").expect("M9IY_OUT");
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
        contents: corpus.contents.clone(),
        content_errors: corpus.content_errors.clone(),
        dirs: corpus.dirs.clone(),
        dir_errors: corpus.dir_errors.clone(),
    };
    let symbols = Arc::new(FakeSymbols {
        symbols: corpus.symbols.clone(),
        errors: corpus.symbol_errors.clone(),
    }) as Arc<dyn SymbolsClient + Send + Sync>;
    let clone_urls = FakeCloneUrls {
        repos: corpus.submodule_repos.clone(),
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
        let git = prefetch_git_with(
            &fake,
            &repositories,
            &HashMap::new(),
            &nodes,
            None,
            &fields,
            Some(symbols.clone()),
            Some(&clone_urls),
        )
        .await;
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
