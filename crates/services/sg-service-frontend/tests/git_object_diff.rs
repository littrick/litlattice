//! M9.8ix differential of the git object / commit resolvers against
//! graph-gophers: `Repository.commit(rev:)` (`repository.go:296-310`),
//! `GitCommitResolver`'s core fields (`git_commit.go`), `GitRef.target`
//! (`git_ref.go:100-127`) and `GitObjectResolver` (`git_object.go`).
//!
//! Both sides drive the same corpus through their real resolver stacks, with a
//! fake gitserver: the Go oracle swaps `gitserver.NewMockClient`; this harness
//! injects the equivalent [`GitObjectSource`] into [`prefetch_git`] and replays
//! it through `FrontendRootResolver`.
//!
//! Env:
//!
//! * `SG_M9IX_DIFF=1`
//! * `M9IX_CORPUS` — path to `corpus.json`
//! * `M9IX_OUT` — output directory (one `<name>.json` per case)

#![allow(clippy::unwrap_used)]

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use async_trait::async_trait;
use serde::Deserialize;
use serde_json::Map;

use sg_git::{CommitDetails, CommitSignature, GitObject, GitObjectType};
use sg_graphql::exec::{execute_with, root_fields, Actor, ExecRequest};
use sg_graphql::schema::assemble_sdl;

use sg_service_frontend::git_graphql::{prefetch_git, GitObjectSource, GitSourceError};
use sg_service_frontend::graphql_resolvers::{
    FrontendRootResolver, NodeRoots, RepoKey, RepoLookup, RepositoryData,
};

const SDL: &str = "
schema { query: Query }

scalar GitObjectID

type Query {
  repository(name: String, cloneURL: String, uri: String): Repository
  node(id: ID!): Node
}

interface Node {
  id: ID!
}

type Repository implements Node {
  id: ID!
  name: String!
  commit(rev: String!, inputRevspec: String): GitCommit
}

type GitCommit implements Node {
  id: ID!
  repository: Repository!
  oid: GitObjectID!
  abbreviatedOID: String!
  author: Signature!
  committer: Signature
  message: String!
  subject: String!
  body: String
  parents: [GitCommit!]!
  url: String!
  canonicalURL: String!
  externalURLs: [ExternalLink!]!
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

type GitRef implements Node {
  id: ID!
  name: String!
  abbrevName: String!
  displayName: String!
  prefix: String!
  type: String!
  target: GitObject!
  repository: Repository!
  url: String!
}

type GitObject {
  oid: GitObjectID!
  abbreviatedOID: String!
  type: GitObjectType!
  commit: GitCommit
}

enum GitObjectType {
  GIT_COMMIT
  GIT_TAG
  GIT_TREE
  GIT_BLOB
  GIT_UNKNOWN
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
struct ObjectFixture {
    oid: String,
    #[serde(rename = "type")]
    object_type: String,
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
    #[serde(default)]
    revisions: HashMap<String, String>,
    #[serde(default)]
    revision_errors: HashMap<String, String>,
    #[serde(default)]
    objects: HashMap<String, ObjectFixture>,
    #[serde(default)]
    object_errors: HashMap<String, String>,
    #[serde(default)]
    commits: HashMap<String, CommitFixture>,
    #[serde(default)]
    commit_errors: HashMap<String, String>,
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

fn object_type(value: &str) -> GitObjectType {
    match value {
        "commit" => GitObjectType::Commit,
        "tag" => GitObjectType::Tag,
        "tree" => GitObjectType::Tree,
        "blob" => GitObjectType::Blob,
        _ => GitObjectType::Unknown,
    }
}

struct FakeGit {
    revisions: HashMap<String, String>,
    revision_errors: HashMap<String, String>,
    objects: HashMap<String, ObjectFixture>,
    object_errors: HashMap<String, String>,
    commits: HashMap<String, CommitFixture>,
    commit_errors: HashMap<String, String>,
}

fn key(repo: &str, spec: &str) -> String {
    format!("{repo}|{spec}")
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
        let k = key(repo_name, revspec);
        if let Some(message) = self.object_errors.get(&k) {
            return Err(GitSourceError::new(message.clone()));
        }
        if let Some(fixture) = self.objects.get(&k) {
            return Ok(GitObject {
                oid: fixture.oid.clone(),
                object_type: object_type(&fixture.object_type),
            });
        }
        Err(GitSourceError::revision_not_found(format!(
            "revision not found: {repo_name}@{revspec}"
        )))
    }
}

#[tokio::test]
async fn git_object_commit_harness() {
    if std::env::var("SG_M9IX_DIFF").as_deref() != Ok("1") {
        return;
    }
    let corpus_path = std::env::var("M9IX_CORPUS").expect("M9IX_CORPUS");
    let out = std::env::var("M9IX_OUT").expect("M9IX_OUT");
    std::fs::create_dir_all(&out).expect("out dir");
    let corpus: Corpus =
        serde_json::from_slice(&std::fs::read(&corpus_path).expect("corpus")).expect("corpus json");
    let schema = assemble_sdl(SDL).expect("schema");

    let fake = FakeGit {
        revisions: corpus.revisions.clone(),
        revision_errors: corpus.revision_errors.clone(),
        objects: corpus.objects.clone(),
        object_errors: corpus.object_errors.clone(),
        commits: corpus.commits.clone(),
        commit_errors: corpus.commit_errors.clone(),
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
