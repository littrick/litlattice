#![forbid(unsafe_code)]

//! GraphQL git object and commit resolvers (M9.8ix).
//!
//! Ports the read-only part of `git_object.go`, `git_ref.go`'s `Target()`
//! and `git_commit.go`'s core fields. `sg-graphql`'s `ObjectValue::field` is
//! synchronous, so every gitserver lookup happens in [`prefetch_git`] and is
//! replayed from [`GitPrefetch`] while the executor walks the selection set.
//!
//! ## Baseline contract
//!
//! - `GitObjectResolver` resolves its revspec lazily with
//!   `gitserver.Client.GetObject`; the 20-byte proto ID is hex-encoded and a
//!   malformed length becomes the zero OID (`gitdomain.GitObject.FromProto`).
//! - `GitRef.Target()` is a preseeded `gitObject` when the search result
//!   carried a target; search's `CommitSearchResult.refs` does not, so it is a
//!   lazy `gitObjectResolver{revspec: name}` (`git_ref.go:103-110`).
//! - `Repository.Commit(rev:)` is `backend.Repos().ResolveRev` (gitserver
//!   `ResolveRevision` with `EnsureRevision=true`); a
//!   `gitdomain.RevisionNotFoundError` is GraphQL `null`, every other error
//!   propagates (`repository.go:296-310`). `InputRevspec`, when given, becomes
//!   the resolver's `inputRev`.
//! - `GitCommitResolver.resolveCommit` is `GetCommit`; on
//!   `RevisionNotFoundError` it retries `ResolveRevision(EnsureRevision=true)`
//!   and `GetCommit` once, keeping the first error when the retry fails
//!   (`git_commit.go:98-117`). `Parents()` builds resolvers for each parent
//!   OID without a `ResolveRev` round-trip.
//! - `GitCommit.id` is `relay.MarshalID("GitCommit", {"r": repo relay,
//!   "c": oid})`; `url` uses `inputRev-or-oid`, `canonicalURL` the OID.
//! - `Signature.date` is `time.Time.Format(time.RFC3339)`; a nil proto
//!   timestamp is the Unix epoch (`(*timestamppb.Timestamp)(nil).AsTime()`).
//! - `Person` name/email/displayName fall back exactly like `person.go`; the
//!   `user`/`avatarURL` lookups need the users store and are not ported, so
//!   they answer `null` (matching a store that finds no verified email).
//!
//! Out of scope (next round): `tree`/`blob`/`file`/`path`/`diff`/`ancestors`/
//! `ownership`/`languages`/`fileNames` and `GitCommit.externalURLs`' code-host
//! linker. See `docs/overviews/m9_8ix_git_object_commit_overview.md`.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use async_trait::async_trait;
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;

use sg_git::GitObjectType;
use sg_graphql::exec::{Actor, Arguments, ExecError, FieldValue, ObjectValue, RootField};
use sg_graphql::json::Json;

use crate::graphql_resolvers::{
    repository_arg, NodeRoots, RepoKey, RepoLookup, RepositoryArg, RepositoryRedirectArg,
    RepositoryRedirectKey,
};
use crate::search_stream::convert::{go_url_path, Match, MinimalRepo};

/// Maximum parent-commit recursion depth prefetched for a selection set.
/// Cycles are only possible through the query's nested `parents` selections.
const MAX_COMMIT_DEPTH: usize = 16;

/// `gitdomain.GitObjectType` as the GraphQL `GitObjectType` enum.
pub fn git_object_type_name(object_type: GitObjectType) -> &'static str {
    match object_type {
        GitObjectType::Commit => "GIT_COMMIT",
        GitObjectType::Tag => "GIT_TAG",
        GitObjectType::Tree => "GIT_TREE",
        GitObjectType::Blob => "GIT_BLOB",
        GitObjectType::Unknown => "GIT_UNKNOWN",
    }
}

/// A gitserver error as the resolvers see it. `revision_not_found` mirrors
/// `errors.HasType[*gitdomain.RevisionNotFoundError]`, which is the only
/// error the baseline turns into `null` (`Repository.Commit`) or retries
/// (`GitCommitResolver.resolveCommit`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GitSourceError {
    pub message: String,
    pub revision_not_found: bool,
    /// `os.IsNotExist`: only a gitserver `FileNotFoundPayload` (never a
    /// `RevisionNotFoundPayload`), which the tree-entry `path()` lookup turns
    /// into `(nil, nil)`.
    pub not_exist: bool,
}

impl GitSourceError {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            revision_not_found: false,
            not_exist: false,
        }
    }

    pub fn revision_not_found(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            revision_not_found: true,
            not_exist: false,
        }
    }

    /// `&os.PathError{Op: "open", Err: os.ErrNotExist}`.
    pub fn not_exist(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            revision_not_found: false,
            not_exist: true,
        }
    }
}

impl From<sg_git::Error> for GitSourceError {
    fn from(error: sg_git::Error) -> Self {
        let not_exist = matches!(error, sg_git::Error::NotFound(_))
            || matches!(error, sg_git::Error::FileNotExist { .. });
        Self {
            revision_not_found: error.is_revision_not_found(),
            message: error.to_string(),
            not_exist,
        }
    }
}

/// The gitserver reads the git GraphQL resolvers need.
///
/// The production implementation is [`GitserverGitObjects`]; the differential
/// harness injects a fake so both sides observe the same fixture data.
#[async_trait]
pub trait GitObjectSource: Send + Sync {
    /// `backend.Repos().ResolveRev`: `ResolveRevision` with
    /// `EnsureRevision=true`.
    async fn resolve_revision(
        &self,
        repo_id: i32,
        repo_name: &str,
        rev: &str,
    ) -> Result<String, GitSourceError>;

    /// `gitserver.Client.GetCommit`.
    async fn get_commit(
        &self,
        repo_id: i32,
        repo_name: &str,
        oid: &str,
    ) -> Result<sg_git::CommitDetails, GitSourceError>;

    /// `gitserver.Client.GetObject`.
    async fn get_object(
        &self,
        repo_id: i32,
        repo_name: &str,
        revspec: &str,
    ) -> Result<sg_git::GitObject, GitSourceError>;

    /// `gitserver.Client.Stat` (M9.8iy).
    ///
    /// Defaulted so the existing M9.8ix fakes keep compiling; the production
    /// implementation overrides it and the tree-entry prefetcher requires it.
    async fn stat(
        &self,
        _repo_id: i32,
        _repo_name: &str,
        _oid: &str,
        _path: &str,
    ) -> Result<sg_git::FileStat, GitSourceError> {
        Err(GitSourceError::new(
            "GitObjectSource::stat is not implemented",
        ))
    }

    /// `gitserver.Client.ReadDir` (M9.8iy); only the entry names are needed
    /// (`IsSingleChild`).
    async fn read_dir(
        &self,
        _repo_id: i32,
        _repo_name: &str,
        _oid: &str,
        _path: &str,
        _recursive: bool,
    ) -> Result<Vec<String>, GitSourceError> {
        Err(GitSourceError::new(
            "GitObjectSource::read_dir is not implemented",
        ))
    }

    /// `gitserver.Client.NewFileReader` (M9.8iy): the full file content.
    async fn read_file(
        &self,
        _repo_id: i32,
        _repo_name: &str,
        _oid: &str,
        _path: &str,
    ) -> Result<Vec<u8>, GitSourceError> {
        Err(GitSourceError::new(
            "GitObjectSource::read_file is not implemented",
        ))
    }
}

/// Production [`GitObjectSource`] over the live site configuration's
/// gitserver addresses (the same shard selection as [`crate::blame`]).
pub struct GitserverGitObjects {
    live: Arc<sg_config::LiveConfig>,
    clients: tokio::sync::Mutex<HashMap<Vec<String>, Arc<sg_git::GitserverClient>>>,
}

impl GitserverGitObjects {
    pub fn new(live: Arc<sg_config::LiveConfig>) -> Self {
        Self {
            live,
            clients: tokio::sync::Mutex::new(HashMap::new()),
        }
    }

    async fn client_for(
        &self,
        repo: &str,
    ) -> Result<(Arc<sg_git::GitserverClient>, String), GitSourceError> {
        let unified = self.live.current();
        let addresses = sg_git::addresses::GitserverAddresses::from_unified(&unified);
        let addr = addresses
            .addr_for_repo(repo)
            .ok_or_else(|| GitSourceError::new("no gitserver configured"))?
            .to_owned();
        let mut clients = self.clients.lock().await;
        let client = match clients.get(&addresses.addresses) {
            Some(client) => Arc::clone(client),
            None => {
                let client = Arc::new(sg_git::GitserverClient::new(addresses.addresses.clone()));
                clients.insert(addresses.addresses.clone(), Arc::clone(&client));
                client
            }
        };
        Ok((client, addr))
    }
}

#[async_trait]
impl GitObjectSource for GitserverGitObjects {
    async fn resolve_revision(
        &self,
        _repo_id: i32,
        repo_name: &str,
        rev: &str,
    ) -> Result<String, GitSourceError> {
        let (client, addr) = self.client_for(repo_name).await?;
        client
            .resolve_revision_at(&addr, repo_name, rev, true)
            .await
            .map_err(GitSourceError::from)
    }

    async fn get_commit(
        &self,
        _repo_id: i32,
        repo_name: &str,
        oid: &str,
    ) -> Result<sg_git::CommitDetails, GitSourceError> {
        let (client, addr) = self.client_for(repo_name).await?;
        client
            .get_commit_details_at(&addr, repo_name, oid)
            .await
            .map_err(GitSourceError::from)
    }

    async fn get_object(
        &self,
        _repo_id: i32,
        repo_name: &str,
        revspec: &str,
    ) -> Result<sg_git::GitObject, GitSourceError> {
        let (client, addr) = self.client_for(repo_name).await?;
        client
            .get_object_at(&addr, repo_name, revspec)
            .await
            .map_err(GitSourceError::from)
    }

    async fn stat(
        &self,
        _repo_id: i32,
        repo_name: &str,
        oid: &str,
        path: &str,
    ) -> Result<sg_git::FileStat, GitSourceError> {
        let (client, addr) = self.client_for(repo_name).await?;
        client
            .stat_at(&addr, repo_name, oid, path)
            .await
            .map_err(GitSourceError::from)
    }

    async fn read_dir(
        &self,
        _repo_id: i32,
        repo_name: &str,
        oid: &str,
        path: &str,
        recursive: bool,
    ) -> Result<Vec<String>, GitSourceError> {
        let (client, addr) = self.client_for(repo_name).await?;
        let entries = client
            .read_dir_entries_at(&addr, repo_name, oid, path, recursive)
            .await
            .map_err(GitSourceError::from)?;
        Ok(entries.into_iter().map(|entry| entry.name).collect())
    }

    async fn read_file(
        &self,
        _repo_id: i32,
        repo_name: &str,
        oid: &str,
        path: &str,
    ) -> Result<Vec<u8>, GitSourceError> {
        let (client, addr) = self.client_for(repo_name).await?;
        client
            .read_file_at(&addr, repo_name, oid, path)
            .await
            .map_err(GitSourceError::from)
    }
}

/// `Repository.Commit(rev:)`'s `ResolveRev` outcome.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RevisionLookup {
    Found(String),
    /// `RevisionNotFoundError` → GraphQL `null`.
    NotFound,
    Error(String),
}

/// `GitObjectResolver.resolve`'s `GetObject` outcome.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ObjectLookup {
    Found(sg_git::GitObject),
    Error(String),
}

/// `resolveCommit`'s `GetCommit` outcome (after the ensure-revision retry).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CommitLookup {
    Found(sg_git::CommitDetails),
    Error(String),
}

/// `gitserver.Client.Stat` outcome for one tree entry (`GitCommit.path`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TreeEntryLookup {
    Found(sg_git::FileStat),
    /// `os.IsNotExist`: `GitCommit.tree`/`blob`/`file` resolve to `null`.
    NotFound,
    Error(String),
}

/// `NewFileReader` outcome (`GitTreeEntry.content`/`byteSize`/`totalLines`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ContentLookup {
    Found(Vec<u8>),
    Error(String),
}

/// `ReadDir` outcome (`GitTreeEntry.isSingleChild`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DirLookup {
    Found(Vec<String>),
    Error(String),
}

/// All gitserver data prefetched for one GraphQL request.
///
/// Keys are `(repo id, revspec/oid)`; the repository name travels with the
/// objects that need it (URLs and `repository` fields). Tree-entry keys are
/// `(repo id, oid, path)`.
#[derive(Default)]
pub struct GitPrefetch {
    pub revisions: HashMap<(i32, String), RevisionLookup>,
    pub objects: HashMap<(i32, String), ObjectLookup>,
    pub commits: HashMap<(i32, String), CommitLookup>,
    pub tree_entries: HashMap<(i32, String, String), TreeEntryLookup>,
    pub contents: HashMap<(i32, String, String), ContentLookup>,
    pub dirs: HashMap<(i32, String, String), DirLookup>,
    /// Submodule clone URLs seen while prefetching tree entries (the
    /// `path.Join(repoName, submoduleURL)` form, M9.8iy).
    pub submodule_urls: Vec<String>,
    /// `cloneURLToRepoName` results for [`Self::submodule_urls`], keyed by the
    /// same string. `None` records a failed/unresolvable lookup, matching the
    /// baseline's empty `url.URL{}`.
    pub submodule_repo_names: HashMap<String, Option<String>>,
    /// `symbol.DefaultZoektSymbolsClient()` injection point. `GitTreeEntry` /
    /// `GitCommit` `symbols` call the (synchronous) gRPC client here.
    pub symbols: Option<Arc<dyn crate::search_searcher_client::SymbolsClient + Send + Sync>>,
}

impl Clone for GitPrefetch {
    fn clone(&self) -> Self {
        Self {
            revisions: self.revisions.clone(),
            objects: self.objects.clone(),
            commits: self.commits.clone(),
            tree_entries: self.tree_entries.clone(),
            contents: self.contents.clone(),
            dirs: self.dirs.clone(),
            submodule_urls: self.submodule_urls.clone(),
            submodule_repo_names: self.submodule_repo_names.clone(),
            symbols: self.symbols.clone(),
        }
    }
}

impl std::fmt::Debug for GitPrefetch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GitPrefetch")
            .field("revisions", &self.revisions)
            .field("objects", &self.objects)
            .field("commits", &self.commits)
            .field("tree_entries", &self.tree_entries)
            .field("contents", &self.contents)
            .field("dirs", &self.dirs)
            .field("submodule_urls", &self.submodule_urls)
            .field("submodule_repo_names", &self.submodule_repo_names)
            .field(
                "symbols",
                &self.symbols.as_ref().map(|_| "<symbols client>"),
            )
            .finish()
    }
}

impl GitPrefetch {
    fn object(&self, repo_id: i32, revspec: &str) -> Option<&ObjectLookup> {
        self.objects.get(&(repo_id, revspec.to_owned()))
    }

    /// The prefetched `Stat` for `(oid, path)`.
    pub fn tree_entry(&self, repo_id: i32, oid: &str, path: &str) -> Option<&TreeEntryLookup> {
        self.tree_entries
            .get(&(repo_id, oid.to_owned(), path.to_owned()))
    }

    /// The prefetched content for `(oid, path)`.
    pub fn content(&self, repo_id: i32, oid: &str, path: &str) -> Option<&ContentLookup> {
        self.contents
            .get(&(repo_id, oid.to_owned(), path.to_owned()))
    }

    /// The prefetched directory listing for `(oid, path)`.
    pub fn dir(&self, repo_id: i32, oid: &str, path: &str) -> Option<&DirLookup> {
        self.dirs.get(&(repo_id, oid.to_owned(), path.to_owned()))
    }

    /// `cloneURLToRepoName` result for a submodule URL (`None` when the lookup
    /// was not prefetched or failed).
    pub fn submodule_repo_name(&self, url: &str) -> Option<String> {
        self.submodule_repo_names.get(url).cloned().flatten()
    }

    /// The commit data for `oid` and the error that prevented loading it.
    pub fn commit_data(
        &self,
        repo_id: i32,
        oid: &str,
    ) -> (Option<sg_git::CommitDetails>, Option<String>) {
        match self.commits.get(&(repo_id, oid.to_owned())) {
            Some(CommitLookup::Found(commit)) => (Some(commit.clone()), None),
            Some(CommitLookup::Error(message)) => (None, Some(message.clone())),
            None => (None, None),
        }
    }
}

// ---------------------------------------------------------------------------
// Selection-set helpers
// ---------------------------------------------------------------------------

fn find_field<'a>(selections: &'a [RootField], name: &str) -> Option<&'a RootField> {
    selections.iter().find(|field| field.name == name)
}

/// Selections that apply to `type_name` at a field that may hold inline
/// fragments (e.g. `Query.node` and `SearchResults.results`).
fn selections_for_type<'a>(field: &'a RootField, type_name: &str) -> Vec<&'a RootField> {
    let mut out: Vec<&RootField> = field.selections.iter().collect();
    for (condition, selections) in &field.type_conditions {
        if condition == type_name {
            out.extend(selections.iter());
        }
    }
    out
}

/// Whether any field named `name` in `selections`' subtree has a `target`
/// descendant (used to decide if `GitRef.target` was selected under search).
fn subtree_mentions_target(selections: &[RootField], name: &str) -> bool {
    fn contains(selections: &[RootField], name: &str) -> bool {
        selections.iter().any(|field| {
            field.name == name
                || contains(&field.selections, name)
                || field
                    .type_conditions
                    .iter()
                    .any(|(_, nested)| contains(nested, name))
        })
    }
    for field in selections {
        if field.name == name
            && (contains(&field.selections, "target")
                || field
                    .type_conditions
                    .iter()
                    .any(|(_, nested)| contains(nested, "target")))
        {
            return true;
        }
        if subtree_mentions_target(&field.selections, name)
            || field
                .type_conditions
                .iter()
                .any(|(_, nested)| subtree_mentions_target(nested, name))
        {
            return true;
        }
    }
    false
}

fn first_field_deep<'a>(selections: &'a [RootField], name: &str) -> Option<&'a RootField> {
    for field in selections {
        if field.name == name {
            return Some(field);
        }
        if let Some(found) = first_field_deep(&field.selections, name) {
            return Some(found);
        }
        for (_, nested) in &field.type_conditions {
            if let Some(found) = first_field_deep(nested, name) {
                return Some(found);
            }
        }
    }
    None
}

/// The `GitCommit` fields that force `resolveCommit` (everything except the
/// purely textual/ID fields and `perforceChangelist`).
fn commit_needs_data(selections: &[RootField]) -> bool {
    const DATA_FIELDS: [&str; 6] = [
        "author",
        "committer",
        "message",
        "subject",
        "body",
        "parents",
    ];
    selections
        .iter()
        .any(|field| DATA_FIELDS.contains(&field.name.as_str()))
        || selections.iter().any(|field| {
            field.type_conditions.iter().any(|(_, nested)| {
                nested
                    .iter()
                    .any(|field| DATA_FIELDS.contains(&field.name.as_str()))
            })
        })
}

// ---------------------------------------------------------------------------
// Prefetch
// ---------------------------------------------------------------------------

fn repo_of(data: &crate::graphql_resolvers::RepositoryData) -> MinimalRepo {
    MinimalRepo {
        id: data.id,
        name: data.name.clone(),
    }
}

async fn load_revision(
    source: &dyn GitObjectSource,
    prefetch: &mut GitPrefetch,
    repo: &MinimalRepo,
    rev: &str,
) -> RevisionLookup {
    let key = (repo.id, rev.to_owned());
    if let Some(lookup) = prefetch.revisions.get(&key) {
        return lookup.clone();
    }
    let lookup = match source.resolve_revision(repo.id, &repo.name, rev).await {
        Ok(oid) => RevisionLookup::Found(oid),
        Err(error) if error.revision_not_found => RevisionLookup::NotFound,
        Err(error) => RevisionLookup::Error(error.message),
    };
    prefetch.revisions.insert(key, lookup.clone());
    lookup
}

async fn load_object(
    source: &dyn GitObjectSource,
    prefetch: &mut GitPrefetch,
    repo: &MinimalRepo,
    revspec: &str,
) -> ObjectLookup {
    let key = (repo.id, revspec.to_owned());
    if let Some(lookup) = prefetch.objects.get(&key) {
        return lookup.clone();
    }
    let lookup = match source.get_object(repo.id, &repo.name, revspec).await {
        Ok(object) => ObjectLookup::Found(object),
        Err(error) => ObjectLookup::Error(error.message),
    };
    prefetch.objects.insert(key, lookup.clone());
    lookup
}

/// `GitCommitResolver.resolveCommit`: `GetCommit`, retrying through
/// `ResolveRevision(EnsureRevision=true)` + `GetCommit` when the first call
/// reports a missing revision. The first error survives a failed retry.
async fn load_commit(
    source: &dyn GitObjectSource,
    prefetch: &mut GitPrefetch,
    repo: &MinimalRepo,
    oid: &str,
) -> CommitLookup {
    let key = (repo.id, oid.to_owned());
    if let Some(lookup) = prefetch.commits.get(&key) {
        return lookup.clone();
    }
    let lookup = match source.get_commit(repo.id, &repo.name, oid).await {
        Ok(commit) => CommitLookup::Found(commit),
        Err(error) if error.revision_not_found => {
            match source.resolve_revision(repo.id, &repo.name, oid).await {
                Err(_) => CommitLookup::Error(error.message),
                Ok(_) => match source.get_commit(repo.id, &repo.name, oid).await {
                    Ok(commit) => CommitLookup::Found(commit),
                    Err(second) => CommitLookup::Error(second.message),
                },
            }
        }
        Err(error) => CommitLookup::Error(error.message),
    };
    prefetch.commits.insert(key, lookup.clone());
    lookup
}

/// Prefetch one commit whose OID is already absolute, as `Parents()` builds
/// it: no `ResolveRev`, only `GetCommit` (with the retry above).
async fn prefetch_parent(
    source: &dyn GitObjectSource,
    prefetch: &mut GitPrefetch,
    repo: &MinimalRepo,
    oid: &str,
    selections: &[RootField],
    depth: usize,
) {
    prefetch_commit_fields(source, prefetch, repo, oid, selections, depth).await;
}

/// Prefetch a commit reached through `Repository.Commit` (i.e. after a
/// `ResolveRev`), then its selected parents.
pub(crate) async fn prefetch_commit_data(
    source: &dyn GitObjectSource,
    prefetch: &mut GitPrefetch,
    repo: &MinimalRepo,
    oid: &str,
    selections: &[RootField],
    depth: usize,
) {
    prefetch_commit_fields(source, prefetch, repo, oid, selections, depth).await;
}

/// The shared body of [`prefetch_parent`]/[`prefetch_commit_data`].
///
/// `tree`/`blob`/`file`/`path` (M9.8iy) do not need `GetCommit`, so their
/// prefetch runs before the `commit_needs_data` early return; `GitCommit.tree`
/// then re-enters this function for the nested commit selection.
async fn prefetch_commit_fields(
    source: &dyn GitObjectSource,
    prefetch: &mut GitPrefetch,
    repo: &MinimalRepo,
    oid: &str,
    selections: &[RootField],
    depth: usize,
) {
    if depth > MAX_COMMIT_DEPTH {
        return;
    }
    crate::git_tree_entry_graphql::prefetch_commit_entries(
        source, prefetch, repo, oid, selections, depth,
    )
    .await;
    if !commit_needs_data(selections) {
        return;
    }
    load_commit(source, prefetch, repo, oid).await;
    let parents = match prefetch.commits.get(&(repo.id, oid.to_owned())) {
        Some(CommitLookup::Found(commit)) => Some(commit.parents.clone()),
        _ => None,
    };
    if let Some(parents) = parents {
        if let Some(parents_field) = find_field(selections, "parents") {
            for parent in parents {
                Box::pin(prefetch_parent(
                    source,
                    prefetch,
                    repo,
                    &parent,
                    &parents_field.selections,
                    depth + 1,
                ))
                .await;
            }
        }
    }
}

/// `Repository.Commit(rev:)` / `GitObject.Commit()`: resolve the revspec, then
/// prefetch the commit data the selection set needs.
async fn prefetch_commit_rev(
    source: &dyn GitObjectSource,
    prefetch: &mut GitPrefetch,
    repo: &MinimalRepo,
    rev: &str,
    selections: &[RootField],
) {
    if let RevisionLookup::Found(oid) = load_revision(source, prefetch, repo, rev).await {
        prefetch_commit_data(source, prefetch, repo, &oid, selections, 0).await;
    }
}

/// The `commit(rev:)` field of a repository-shaped selection set.
async fn prefetch_repository_selections(
    source: &dyn GitObjectSource,
    prefetch: &mut GitPrefetch,
    repo: &MinimalRepo,
    selections: &[RootField],
) {
    let Some(commit) = find_field(selections, "commit") else {
        return;
    };
    let rev = commit.arguments.string("rev").unwrap_or_default();
    prefetch_commit_rev(source, prefetch, repo, rev, &commit.selections).await;
}

/// The `GitObject` selection under `GitRef.target`: prefetch its `commit`
/// child, which `Repository.Commit(Rev: oid)` resolves through `ResolveRev`.
async fn prefetch_object_selections(
    source: &dyn GitObjectSource,
    prefetch: &mut GitPrefetch,
    repo: &MinimalRepo,
    oid: &str,
    selections: &[RootField],
) {
    if let Some(commit) = find_field(selections, "commit") {
        prefetch_commit_rev(source, prefetch, repo, oid, &commit.selections).await;
    }
}

/// Prefetch every gitserver read the selected `repository`/`node`/`search`
/// fields need. Replayed by the resolvers through [`GitPrefetch`].
pub async fn prefetch_git(
    source: &dyn GitObjectSource,
    repositories: &HashMap<RepoKey, RepoLookup>,
    repository_redirects: &HashMap<RepositoryRedirectKey, RepoLookup>,
    nodes: &NodeRoots,
    search: Option<&crate::search_graphql::SearchRoot>,
    fields: &[RootField],
) -> GitPrefetch {
    let mut prefetch = GitPrefetch::default();
    let mut seen: HashSet<(i32, &'static str, String)> = HashSet::new();
    for field in fields {
        match field.name.as_str() {
            "repository" => {
                let lookup = match repository_arg(&field.arguments) {
                    RepositoryArg::Key(key) => repositories.get(&key),
                    RepositoryArg::CloneUrl(url) => repositories.get(&RepoKey::CloneUrl(url)),
                    RepositoryArg::Missing => None,
                };
                if let Some(RepoLookup::Found(data)) = lookup {
                    prefetch_repository_selections(
                        source,
                        &mut prefetch,
                        &repo_of(data),
                        &field.selections,
                    )
                    .await;
                }
            }
            "repositoryRedirect" => {
                let lookup =
                    match crate::graphql_resolvers::repository_redirect_arg(&field.arguments) {
                        RepositoryRedirectArg::Key(key) => repository_redirects.get(&key),
                        RepositoryRedirectArg::CloneUrl(url) => {
                            repository_redirects.get(&RepositoryRedirectKey::CloneUrl(url))
                        }
                        RepositoryRedirectArg::Missing => None,
                    };
                if let Some(RepoLookup::Found(data)) = lookup {
                    prefetch_repository_selections(
                        source,
                        &mut prefetch,
                        &repo_of(data),
                        &field.selections,
                    )
                    .await;
                }
            }
            "node" => {
                let Some(id) = field.arguments.string("id") else {
                    continue;
                };
                let Some(kind) = crate::graphql_resolvers::relay_kind(id) else {
                    continue;
                };
                match kind.as_str() {
                    "GitCommit" => {
                        let Ok(spec) = parse_git_commit_relay_id(id) else {
                            continue;
                        };
                        let Ok(repo_id) =
                            crate::graphql_resolvers::unmarshal_relay_id(&spec.repository)
                        else {
                            continue;
                        };
                        let Some(RepoLookup::Found(data)) = nodes.repositories.get(&repo_id) else {
                            continue;
                        };
                        let repo = repo_of(data);
                        let selections: Vec<RootField> = selections_for_type(field, "GitCommit")
                            .into_iter()
                            .cloned()
                            .collect();
                        if !selections.is_empty()
                            && seen.insert((repo.id, "commit", spec.commit.clone()))
                        {
                            prefetch_commit_rev(
                                source,
                                &mut prefetch,
                                &repo,
                                &spec.commit,
                                &selections,
                            )
                            .await;
                        }
                    }
                    "GitRef" => {
                        let Ok(spec) = parse_git_ref_relay_id(id) else {
                            continue;
                        };
                        let Ok(repo_id) =
                            crate::graphql_resolvers::unmarshal_relay_id(&spec.repository)
                        else {
                            continue;
                        };
                        let Some(RepoLookup::Found(data)) = nodes.repositories.get(&repo_id) else {
                            continue;
                        };
                        let repo = repo_of(data);
                        let selections: Vec<RootField> = selections_for_type(field, "GitRef")
                            .into_iter()
                            .cloned()
                            .collect();
                        let Some(target) = find_field(&selections, "target") else {
                            continue;
                        };
                        if !seen.insert((repo.id, "object", spec.rev.clone())) {
                            continue;
                        }
                        let object = load_object(source, &mut prefetch, &repo, &spec.rev).await;
                        if let ObjectLookup::Found(object) = object {
                            prefetch_object_selections(
                                source,
                                &mut prefetch,
                                &repo,
                                &object.oid,
                                &target.selections,
                            )
                            .await;
                        }
                    }
                    "Repository" => {
                        let Ok(repo_id) = crate::graphql_resolvers::unmarshal_relay_id(id) else {
                            continue;
                        };
                        let Some(RepoLookup::Found(data)) = nodes.repositories.get(&repo_id) else {
                            continue;
                        };
                        let repo = repo_of(data);
                        let selections: Vec<RootField> = selections_for_type(field, "Repository")
                            .into_iter()
                            .cloned()
                            .collect();
                        prefetch_repository_selections(source, &mut prefetch, &repo, &selections)
                            .await;
                    }
                    _ => {}
                }
            }
            "search" => {
                let wants_refs = subtree_mentions_target(&field.selections, "refs");
                let wants_source_refs = subtree_mentions_target(&field.selections, "sourceRefs");
                if !wants_refs && !wants_source_refs {
                    continue;
                }
                let Some(crate::search_graphql::SearchRoot::Completed(execution)) = search else {
                    continue;
                };
                let target = first_field_deep(&field.selections, "target");
                for match_ in &execution.matches {
                    let Match::Commit(commit) = match_ else {
                        continue;
                    };
                    let mut refs: Vec<&String> = Vec::new();
                    if wants_refs {
                        refs.extend(commit.refs.iter());
                    }
                    if wants_source_refs {
                        refs.extend(commit.source_refs.iter());
                    }
                    for name in refs {
                        if !seen.insert((commit.repo.id, "object", name.clone())) {
                            continue;
                        }
                        let object = load_object(source, &mut prefetch, &commit.repo, name).await;
                        if let (ObjectLookup::Found(object), Some(target)) = (&object, target) {
                            prefetch_object_selections(
                                source,
                                &mut prefetch,
                                &commit.repo,
                                &object.oid,
                                &target.selections,
                            )
                            .await;
                        }
                    }
                }
            }
            _ => {}
        }
    }
    prefetch
}

/// [`prefetch_git`] plus the M9.8iy seams: the symbols client
/// (`symbol.DefaultZoektSymbolsClient`) and the `cloneURLToRepoName`
/// resolver used by submodule URLs.
#[allow(clippy::too_many_arguments)]
pub async fn prefetch_git_with(
    source: &dyn GitObjectSource,
    repositories: &HashMap<RepoKey, RepoLookup>,
    repository_redirects: &HashMap<RepositoryRedirectKey, RepoLookup>,
    nodes: &NodeRoots,
    search: Option<&crate::search_graphql::SearchRoot>,
    fields: &[RootField],
    symbols: Option<Arc<dyn crate::search_searcher_client::SymbolsClient + Send + Sync>>,
    clone_urls: Option<&dyn crate::graphql_resolvers::CloneUrlResolver>,
) -> GitPrefetch {
    let mut prefetch = prefetch_git(
        source,
        repositories,
        repository_redirects,
        nodes,
        search,
        fields,
    )
    .await;
    crate::git_tree_entry_graphql::prefetch_submodule_repos(&mut prefetch, clone_urls).await;
    prefetch.symbols = symbols;
    prefetch
}

// ---------------------------------------------------------------------------
// Relay IDs
// ---------------------------------------------------------------------------

/// `gitRefGQLID`/`gitCommitGQLID`'s repository member: the raw repository
/// Relay ID string (a `Repository:<id>` base64 ID).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GitCommitRelaySpec {
    pub repository: String,
    pub commit: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GitRefRelaySpec {
    pub repository: String,
    pub rev: String,
}

fn unmarshal_relay_spec(id: &str) -> Result<String, String> {
    use base64::Engine as _;
    let decoded = base64::engine::general_purpose::URL_SAFE
        .decode(id)
        .map_err(|error| error.to_string())?;
    let text = String::from_utf8_lossy(&decoded);
    let Some((_, spec)) = text.split_once(':') else {
        return Err("invalid graphql.ID".to_owned());
    };
    Ok(spec.to_owned())
}

/// `unmarshalGitCommitID`: `relay.UnmarshalSpec` into
/// `{"r": <graphql.ID>, "c": <GitObjectID>}`.
pub fn parse_git_commit_relay_id(id: &str) -> Result<GitCommitRelaySpec, String> {
    #[derive(serde::Deserialize)]
    struct Spec {
        r: String,
        c: String,
    }
    let spec = unmarshal_relay_spec(id)?;
    let parsed: Spec = serde_json::from_str(&spec).map_err(|error| error.to_string())?;
    Ok(GitCommitRelaySpec {
        repository: parsed.r,
        commit: parsed.c,
    })
}

/// `unmarshalGitRefID`: `relay.UnmarshalSpec` into
/// `{"r": <graphql.ID>, "v": string}`.
pub fn parse_git_ref_relay_id(id: &str) -> Result<GitRefRelaySpec, String> {
    #[derive(serde::Deserialize)]
    struct Spec {
        r: String,
        v: String,
    }
    let spec = unmarshal_relay_spec(id)?;
    let parsed: Spec = serde_json::from_str(&spec).map_err(|error| error.to_string())?;
    Ok(GitRefRelaySpec {
        repository: parsed.r,
        rev: parsed.v,
    })
}

/// `marshalGitCommitID(repo relay, oid)`.
pub fn git_commit_relay_id(repo_id: i32, oid: &str) -> String {
    use base64::Engine as _;
    let repo_relay = crate::graphql_resolvers::RepositoryObject::relay_id(repo_id);
    base64::engine::general_purpose::URL_SAFE.encode(format!(
        "GitCommit:{{\"r\":\"{repo_relay}\",\"c\":\"{oid}\"}}"
    ))
}

// ---------------------------------------------------------------------------
// Resolvers
// ---------------------------------------------------------------------------

pub(crate) fn repository_data(repo: &MinimalRepo) -> crate::graphql_resolvers::RepositoryData {
    crate::graphql_resolvers::RepositoryData {
        id: repo.id,
        name: repo.name.clone(),
        uri: repo.name.clone(),
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

fn signature_date(date: Option<&prost_types::Timestamp>) -> String {
    let seconds = date.map(|timestamp| timestamp.seconds).unwrap_or(0);
    let datetime =
        OffsetDateTime::from_unix_timestamp(seconds).unwrap_or(OffsetDateTime::UNIX_EPOCH);
    datetime
        .format(&Rfc3339)
        .unwrap_or_else(|_| datetime.to_string())
}

/// `gitdomain.Message.Subject()` (`common.go:219-226`).
fn message_subject(message: &str) -> String {
    match message.find('\n') {
        None => message.trim().to_owned(),
        Some(index) => message[..index].trim().to_owned(),
    }
}

/// `gitdomain.Message.Body()` (`common.go:229-236`).
fn message_body(message: &str) -> String {
    match message.find('\n') {
        None => String::new(),
        Some(index) => message[index..].trim().to_owned(),
    }
}

/// `GitObjectResolver`/`gitObject`. `oid`/`object_type` are known for a
/// preseeded target; otherwise the object is replayed from [`GitPrefetch`].
pub struct GitObjectResolverObject {
    pub repo: MinimalRepo,
    pub oid: Option<String>,
    pub object_type: Option<GitObjectType>,
    pub revspec: Option<String>,
    pub git: Option<Arc<GitPrefetch>>,
    /// A prefetched `GetObject` error. Go's `gitObjectResolver` defers the
    /// error to the object's fields (`OID`/`Type`/`Commit`), so the parent
    /// `GitRef.target` field itself still resolves.
    pub error: Option<String>,
}

impl GitObjectResolverObject {
    pub fn from_object(
        repo: MinimalRepo,
        object: sg_git::GitObject,
        git: Option<Arc<GitPrefetch>>,
    ) -> Self {
        Self {
            repo,
            oid: Some(object.oid),
            object_type: Some(object.object_type),
            revspec: None,
            git,
            error: None,
        }
    }

    pub fn lazy(repo: MinimalRepo, revspec: String, git: Option<Arc<GitPrefetch>>) -> Self {
        Self {
            repo,
            oid: None,
            object_type: None,
            revspec: Some(revspec),
            git,
            error: None,
        }
    }

    /// A `GetObject` failure replayed by the fields.
    pub fn with_error(repo: MinimalRepo, message: String, git: Option<Arc<GitPrefetch>>) -> Self {
        Self {
            repo,
            oid: None,
            object_type: None,
            revspec: None,
            git,
            error: Some(message),
        }
    }

    fn resolve(&self) -> Result<(String, GitObjectType), ExecError> {
        if let Some(message) = &self.error {
            return Err(ExecError::new(message.clone()));
        }
        if let (Some(oid), Some(object_type)) = (&self.oid, self.object_type) {
            return Ok((oid.clone(), object_type));
        }
        let Some(revspec) = &self.revspec else {
            return Err(ExecError::new("GitObject is not resolved"));
        };
        let Some(git) = &self.git else {
            return Err(ExecError::new(
                "GitObject requires a gitserver object resolver, which is not ported",
            ));
        };
        match git.object(self.repo.id, revspec) {
            Some(ObjectLookup::Found(object)) => Ok((object.oid.clone(), object.object_type)),
            Some(ObjectLookup::Error(message)) => Err(ExecError::new(message.clone())),
            None => Err(ExecError::new("GitObject is not resolved (not prefetched)")),
        }
    }
}

impl ObjectValue for GitObjectResolverObject {
    fn type_name(&self) -> &'static str {
        "GitObject"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "oid" => Ok(Some(FieldValue::Json(Json::Str(self.resolve()?.0)))),
            "abbreviatedOID" => Ok(Some(FieldValue::Json(Json::Str(
                self.resolve()?.0.chars().take(7).collect(),
            )))),
            "type" => Ok(Some(FieldValue::Json(Json::Str(
                git_object_type_name(self.resolve()?.1).to_owned(),
            )))),
            // `gitObject.Commit`: `repo.Commit(Rev: oid)`. The commit itself is
            // the revision's resolved OID (which can differ from the object's,
            // e.g. for an annotated tag), while `inputRev` stays the object OID.
            // A missing revision makes the nullable field `null`.
            "commit" => {
                let object_oid = self.resolve()?.0;
                let Some(git) = &self.git else {
                    return Err(ExecError::new(
                        "GitObject.commit requires a gitserver commit resolver, which is not ported",
                    ));
                };
                match git.revisions.get(&(self.repo.id, object_oid.clone())) {
                    Some(RevisionLookup::Found(commit_oid)) => {
                        let (commit, commit_error) = git.commit_data(self.repo.id, commit_oid);
                        Ok(Some(FieldValue::Object(Arc::new(
                            GitCommitResolverObject {
                                repo: self.repo.clone(),
                                oid: commit_oid.clone(),
                                input_rev: Some(object_oid),
                                commit,
                                commit_error,
                                git: self.git.clone(),
                            },
                        ))))
                    }
                    Some(RevisionLookup::NotFound) => Ok(Some(FieldValue::Null)),
                    Some(RevisionLookup::Error(message)) => Err(ExecError::new(message.clone())),
                    None => Ok(None),
                }
            }
            _ => Ok(None),
        }
    }
}

/// `GitCommitResolver`'s ported fields (`git_commit.go`).
pub struct GitCommitResolverObject {
    pub repo: MinimalRepo,
    pub oid: String,
    /// `inputRev`: the revspec the user asked for (`Repository.Commit`) or
    /// the OID (`GitObject.Commit`, `Query.node`).
    pub input_rev: Option<String>,
    pub commit: Option<sg_git::CommitDetails>,
    pub commit_error: Option<String>,
    pub git: Option<Arc<GitPrefetch>>,
}

impl GitCommitResolverObject {
    fn input_rev_or_immutable_rev(&self) -> String {
        match &self.input_rev {
            Some(rev) if !rev.is_empty() => rev.clone(),
            _ => self.oid.clone(),
        }
    }

    fn commit(&self, field: &str) -> Result<&sg_git::CommitDetails, ExecError> {
        if let Some(commit) = &self.commit {
            return Ok(commit);
        }
        if let Some(message) = &self.commit_error {
            return Err(ExecError::new(message.clone()));
        }
        Err(ExecError::new(format!(
            "commit fields are not ported without a loaded commit ({field})"
        )))
    }

    fn signature(&self, field: &str) -> Result<Option<FieldValue>, ExecError> {
        let commit = self.commit(field)?;
        let signature = if field == "author" {
            Some(&commit.author)
        } else {
            commit.committer.as_ref()
        };
        match signature {
            Some(signature) => Ok(Some(FieldValue::Object(Arc::new(SignatureObject {
                name: signature.name.clone(),
                email: signature.email.clone(),
                date: signature.date,
            })))),
            None => Ok(Some(FieldValue::Null)),
        }
    }

    fn parent(&self, oid: &str) -> GitCommitResolverObject {
        let (commit, commit_error) = match &self.git {
            Some(git) => git.commit_data(self.repo.id, oid),
            None => (None, None),
        };
        GitCommitResolverObject {
            repo: self.repo.clone(),
            oid: oid.to_owned(),
            // `Parents()` creates `NewGitCommitResolver(..., parent, nil)`
            // without an input rev.
            input_rev: None,
            commit,
            commit_error,
            git: self.git.clone(),
        }
    }
}

impl ObjectValue for GitCommitResolverObject {
    fn type_name(&self) -> &'static str {
        "GitCommit"
    }

    fn field(
        &self,
        field: &str,
        arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "id" => Ok(Some(FieldValue::Json(Json::Str(git_commit_relay_id(
                self.repo.id,
                &self.oid,
            ))))),
            "repository" => Ok(Some(FieldValue::Object(Arc::new(
                crate::graphql_resolvers::RepositoryObject::from_data(repository_data(&self.repo))
                    .with_git(self.git.clone()),
            )))),
            "oid" => Ok(Some(FieldValue::Json(Json::Str(self.oid.clone())))),
            "abbreviatedOID" => Ok(Some(FieldValue::Json(Json::Str(
                self.oid.chars().take(7).collect(),
            )))),
            // `PerforceChangelist` is the enterprise resolver; without it the
            // baseline returns `nil` (the field is nullable).
            "perforceChangelist" => Ok(Some(FieldValue::Null)),
            "author" | "committer" => self.signature(field),
            "message" => Ok(Some(FieldValue::Json(Json::Str(
                self.commit(field)?.message.clone(),
            )))),
            "subject" => Ok(Some(FieldValue::Json(Json::Str(message_subject(
                &self.commit(field)?.message,
            ))))),
            "body" => {
                let body = message_body(&self.commit(field)?.message);
                if body.is_empty() {
                    Ok(Some(FieldValue::Null))
                } else {
                    Ok(Some(FieldValue::Json(Json::Str(body))))
                }
            }
            "parents" => {
                let parents = self.commit(field)?.parents.clone();
                Ok(Some(FieldValue::List {
                    element_type: Some("GitCommit"),
                    items: parents
                        .iter()
                        .map(|parent| FieldValue::Object(Arc::new(self.parent(parent))))
                        .collect(),
                }))
            }
            "url" => Ok(Some(FieldValue::Json(Json::Str(go_url_path(&format!(
                "/{}/-/commit/{}",
                self.repo.name,
                self.input_rev_or_immutable_rev()
            )))))),
            "canonicalURL" => Ok(Some(FieldValue::Json(Json::Str(go_url_path(&format!(
                "/{}/-/commit/{}",
                self.repo.name, self.oid
            )))))),
            // `ExternalURLs` needs the repository's code-host linker; no linker
            // is wired, and an empty list is the no-external-service shape.
            "externalURLs" => Ok(Some(FieldValue::List {
                items: Vec::new(),
                element_type: Some("ExternalLink"),
            })),
            // M9.8iy: `GitCommitResolver.Tree`/`Blob`/`File`/`Path`
            // (`git_commit.go:240-308`).
            "tree" | "blob" | "file" | "path" => {
                crate::git_tree_entry_graphql::commit_tree_entry_field(self, field, arguments)
            }
            // `GitCommitResolver.Symbols` (`symbols.go:59-76`).
            "symbols" => crate::git_tree_entry_graphql::commit_symbols_field(self, arguments),
            _ => Ok(None),
        }
    }
}

/// `signatureResolver` (`signature.go`).
pub struct SignatureObject {
    pub name: String,
    pub email: String,
    pub date: Option<prost_types::Timestamp>,
}

impl ObjectValue for SignatureObject {
    fn type_name(&self) -> &'static str {
        "Signature"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "person" => Ok(Some(FieldValue::Object(Arc::new(PersonObject {
                name: self.name.clone(),
                email: self.email.clone(),
            })))),
            "date" => Ok(Some(FieldValue::Json(Json::Str(signature_date(
                self.date.as_ref(),
            ))))),
            _ => Ok(None),
        }
    }
}

/// `PersonResolver` (`person.go`), without the users-store lookups: `user`
/// and `avatarURL` are `null` exactly like a `GetByVerifiedEmail` miss.
pub struct PersonObject {
    pub name: String,
    pub email: String,
}

impl ObjectValue for PersonObject {
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
            "name" => Ok(Some(FieldValue::Json(Json::Str(self.name.clone())))),
            "email" => Ok(Some(FieldValue::Json(Json::Str(self.email.clone())))),
            "displayName" => {
                let name = self.name.trim();
                let display = if !name.is_empty() {
                    name.to_owned()
                } else if !self.email.is_empty() {
                    self.email.clone()
                } else {
                    "unknown".to_owned()
                };
                Ok(Some(FieldValue::Json(Json::Str(display))))
            }
            "avatarURL" | "user" => Ok(Some(FieldValue::Null)),
            _ => Ok(None),
        }
    }
}
