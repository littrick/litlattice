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
pub(crate) const MAX_COMMIT_DEPTH: usize = 16;

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

    /// The extra commit reads (ancestors/diff/languages/...). Sources that do
    /// not implement them leave the extra resolvers unavailable (M9.8iz).
    fn commit_extra(&self) -> Option<&dyn crate::git_commit_extra::GitCommitExtraSource> {
        None
    }

    /// The syntax highlighter backing `FileDiffHunk.highlight` (M9.8jd).
    /// Production has no gosyntect client, so the default (offline
    /// `HTML_PLAINTEXT` only) applies.
    fn highlighter(&self) -> Option<&dyn crate::git_comparison_graphql::HighlightSource> {
        None
    }

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

    /// `gitserver.Client.ReadDir` (M9.43b): rich entries for
    /// `GitTree.entries`/`files`/`directories`.
    ///
    /// The default maps the name-only [`Self::read_dir`] to non-directory
    /// synthetic stats, which keeps older fakes compiling; production and the
    /// differential harness override it to preserve the directory flag.
    async fn read_dir_stats(
        &self,
        repo_id: i32,
        repo_name: &str,
        oid: &str,
        path: &str,
        recursive: bool,
    ) -> Result<Vec<sg_git::FileStat>, GitSourceError> {
        let names = self
            .read_dir(repo_id, repo_name, oid, path, recursive)
            .await?;
        Ok(names
            .into_iter()
            .map(|name| sg_git::FileStat::synthetic(&name, false))
            .collect())
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

    /// `gitserver.Client.ListRefs` (M9.19a, `repository_git_refs.go`).
    async fn list_refs(
        &self,
        _repo_id: i32,
        _repo_name: &str,
        _heads_only: bool,
        _tags_only: bool,
    ) -> Result<Vec<sg_git::GitRef>, GitSourceError> {
        Err(GitSourceError::new(
            "GitObjectSource::list_refs is not implemented",
        ))
    }

    /// `gitserver.Client.ListRefs` with `PointsAtCommit` (M9.31c, the
    /// `policies.Matcher` seam). Defaulted so existing fakes keep compiling;
    /// sources that cannot filter by commit fall back to [`Self::list_refs`].
    async fn list_refs_with_points(
        &self,
        repo_id: i32,
        repo_name: &str,
        heads_only: bool,
        tags_only: bool,
        _points_at_commit: &[String],
    ) -> Result<Vec<sg_git::GitRef>, GitSourceError> {
        self.list_refs(repo_id, repo_name, heads_only, tags_only)
            .await
    }

    /// `gitserver.Client.Commits` (`CommitsOptions` + `CommitLog`). Defaulted
    /// so existing fakes keep compiling; the matcher reports the not-found
    /// semantics when a source has no commit reader (it must never fabricate
    /// commits).
    async fn commits(
        &self,
        _repo_id: i32,
        _repo_name: &str,
        _options: &sg_git::CommitsOptions,
    ) -> Result<Vec<sg_git::Commit>, GitSourceError> {
        Err(GitSourceError::new(
            "GitObjectSource::commits is not implemented",
        ))
    }

    /// `gitserver.Client.ListRefs(TagsOnly:true, PointsAtCommit:[oid])`
    /// (M9.30d, `gitresolvers/commit.go:58-76`). An empty result is
    /// `Ok(vec![])`, never an error.
    async fn list_refs_at_commit(
        &self,
        _repo_id: i32,
        _repo_name: &str,
        _oid: &str,
    ) -> Result<Vec<sg_git::GitRef>, GitSourceError> {
        Err(GitSourceError::new(
            "GitObjectSource::list_refs_at_commit is not implemented",
        ))
    }

    /// `gitserver.Client.GetDefaultBranch(repo, false)` (M9.19a). `Ok(None)`
    /// is the empty default branch (`refName == ""`).
    async fn default_branch(
        &self,
        _repo_id: i32,
        _repo_name: &str,
    ) -> Result<Option<String>, GitSourceError> {
        Err(GitSourceError::new(
            "GitObjectSource::default_branch is not implemented",
        ))
    }

    /// `gitserver.Client.ContributorCount` (M9.19a). `after_date` is the raw
    /// `afterDate` argument; `sg_git` has no contributor API yet, so only the
    /// injected harness source implements this.
    async fn contributor_count(
        &self,
        _repo_id: i32,
        _repo_name: &str,
        _revision_range: Option<&str>,
        _after_date: Option<&str>,
        _path: Option<&str>,
    ) -> Result<Vec<ContributorCountData>, GitSourceError> {
        Err(GitSourceError::new(
            "GitObjectSource::contributor_count is not implemented",
        ))
    }

    /// `gitserver.Client.StreamBlameFile` (M9.19a, `git_blob.go:18-56`).
    async fn blame(
        &self,
        _repo_id: i32,
        _repo_name: &str,
        _oid: &str,
        _path: &str,
        _start_line: Option<i32>,
        _end_line: Option<i32>,
        _ignore_whitespace: bool,
    ) -> Result<Vec<sg_git::BlameHunk>, GitSourceError> {
        Err(GitSourceError::new(
            "GitObjectSource::blame is not implemented",
        ))
    }
}

/// `gitdomain.ContributorCount` (`repository_contributors.go`).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ContributorCountData {
    pub name: String,
    pub email: String,
    pub count: i32,
}

/// `Repository.gitRefs` / `branches` / `tags` cache key (M9.19a).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct RefsKey {
    pub repo_id: i32,
    pub heads_only: bool,
    pub tags_only: bool,
}

/// `Repository.contributors` cache key: only the arguments that reach
/// `gitserver.ContributorCount` (pagination is applied at replay time).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ContributorsKey {
    pub repo_id: i32,
    pub revision_range: Option<String>,
    pub after_date: Option<String>,
    pub path: Option<String>,
}

/// One `Repository.contributors` gitserver lookup. An unparseable
/// `afterDate` is returned by the `contributors` field itself
/// (`repository_contributors.go:30-36`); a gitserver failure is reported by
/// `totalCount`/`nodes`/`pageInfo` (the connection is still constructed).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ContributorLookup {
    InvalidDate(String),
    Counts(Result<Vec<ContributorCountData>, String>),
}

/// `GetDefaultBranch` outcome. `NotFound` is an empty ref name.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DefaultBranchLookup {
    Found(String),
    NotFound,
    Error(String),
}

/// One `StreamBlameFile` result.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BlameLookup {
    Found(Vec<sg_git::BlameHunk>),
    Error(String),
}

/// Cache key for `GitBlob.blame`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct BlameKey {
    pub repo_id: i32,
    pub oid: String,
    pub path: String,
    pub start_line: Option<i32>,
    pub end_line: Option<i32>,
    pub ignore_whitespace: bool,
}

/// Production [`GitObjectSource`] over the live site configuration's
/// gitserver addresses (the same shard selection as [`crate::blame`]).
pub struct GitserverGitObjects {
    live: Arc<sg_config::LiveConfig>,
    clients: tokio::sync::Mutex<HashMap<Vec<String>, Arc<sg_git::GitserverClient>>>,
    /// M9.36d: production syntax highlighting (`highlight.CodeAsLines`).
    highlighter: crate::git_comparison_graphql::CodeHighlighterSource,
}

impl GitserverGitObjects {
    pub fn new(live: Arc<sg_config::LiveConfig>) -> Self {
        Self {
            live,
            clients: tokio::sync::Mutex::new(HashMap::new()),
            highlighter: crate::git_comparison_graphql::CodeHighlighterSource::production(),
        }
    }

    /// `connection.GitserverAddresses.AddrForRepo` under the live
    /// configuration (M9.9d). Pure configuration lookup: no dial and no
    /// client construction, so the differential harness can assert the
    /// production wiring resolves the configured fixed endpoint offline.
    #[doc(hidden)]
    pub fn address_for_repo(&self, repo: &str) -> Option<String> {
        let unified = self.live.current();
        let addresses = sg_git::addresses::GitserverAddresses::from_unified(&unified);
        addresses.addr_for_repo(repo).map(str::to_owned)
    }

    pub(crate) async fn client_for(
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

    fn commit_extra(&self) -> Option<&dyn crate::git_commit_extra::GitCommitExtraSource> {
        Some(self)
    }

    /// M9.36d: wire the existing M9.23c `CodeHighlighter` into the production
    /// `GitObjectSource` so `FileDiffHunk.highlight` returns real markup.
    fn highlighter(&self) -> Option<&dyn crate::git_comparison_graphql::HighlightSource> {
        Some(&self.highlighter)
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

    /// `Repository.contributors` (`repository_contributors.go:23-31`):
    /// `gitdomain.ParseGitDate` then `gitserver.ContributorCount`.
    async fn contributor_count(
        &self,
        _repo_id: i32,
        repo_name: &str,
        revision_range: Option<&str>,
        after_date: Option<&str>,
        path: Option<&str>,
    ) -> Result<Vec<ContributorCountData>, GitSourceError> {
        let (client, addr) = self.client_for(repo_name).await?;
        let options = sg_git::ContributorOptions {
            range: revision_range.unwrap_or_default().to_owned(),
            after: after_date
                .and_then(crate::search_commit_job::parse_git_date)
                .map(|datetime| datetime.unix_timestamp()),
            path: path.unwrap_or_default().to_owned(),
        };
        client
            .contributor_count_at(&addr, repo_name, &options)
            .await
            .map(|counts| {
                counts
                    .into_iter()
                    .map(|count| ContributorCountData {
                        name: count.name,
                        email: count.email,
                        count: count.count,
                    })
                    .collect()
            })
            .map_err(GitSourceError::from)
    }

    /// M9.43b: `GitTree.entries`/`files`/`directories`. gitserver already
    /// returns full paths and the directory bit, which is all the list
    /// resolvers read.
    async fn read_dir_stats(
        &self,
        _repo_id: i32,
        repo_name: &str,
        oid: &str,
        path: &str,
        recursive: bool,
    ) -> Result<Vec<sg_git::FileStat>, GitSourceError> {
        let (client, addr) = self.client_for(repo_name).await?;
        let entries = client
            .read_dir_entries_at(&addr, repo_name, oid, path, recursive)
            .await
            .map_err(GitSourceError::from)?;
        Ok(entries
            .into_iter()
            .map(|entry| sg_git::FileStat::synthetic(&entry.name, entry.is_dir))
            .collect())
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

    async fn list_refs(
        &self,
        _repo_id: i32,
        repo_name: &str,
        heads_only: bool,
        tags_only: bool,
    ) -> Result<Vec<sg_git::GitRef>, GitSourceError> {
        let (client, _addr) = self.client_for(repo_name).await?;
        client
            .list_refs(repo_name, heads_only, tags_only, &[], None)
            .await
            .map_err(GitSourceError::from)
    }

    async fn list_refs_with_points(
        &self,
        _repo_id: i32,
        repo_name: &str,
        heads_only: bool,
        tags_only: bool,
        points_at_commit: &[String],
    ) -> Result<Vec<sg_git::GitRef>, GitSourceError> {
        let (client, _addr) = self.client_for(repo_name).await?;
        client
            .list_refs(repo_name, heads_only, tags_only, points_at_commit, None)
            .await
            .map_err(GitSourceError::from)
    }

    async fn commits(
        &self,
        _repo_id: i32,
        repo_name: &str,
        options: &sg_git::CommitsOptions,
    ) -> Result<Vec<sg_git::Commit>, GitSourceError> {
        let (client, _addr) = self.client_for(repo_name).await?;
        client
            .commits(repo_name, options)
            .await
            .map_err(GitSourceError::from)
    }

    async fn list_refs_at_commit(
        &self,
        _repo_id: i32,
        repo_name: &str,
        oid: &str,
    ) -> Result<Vec<sg_git::GitRef>, GitSourceError> {
        let (client, _addr) = self.client_for(repo_name).await?;
        client
            .list_refs(repo_name, false, true, &[oid.to_owned()], None)
            .await
            .map_err(GitSourceError::from)
    }

    async fn default_branch(
        &self,
        _repo_id: i32,
        repo_name: &str,
    ) -> Result<Option<String>, GitSourceError> {
        let (client, _addr) = self.client_for(repo_name).await?;
        let (name, _) = client
            .default_branch_commit(repo_name)
            .await
            .map_err(GitSourceError::from)?;
        Ok(if name.is_empty() { None } else { Some(name) })
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
    ) -> Result<Vec<sg_git::BlameHunk>, GitSourceError> {
        use tokio_stream::StreamExt as _;
        let (client, addr) = self.client_for(repo_name).await?;
        // `sg_git`'s `StreamBlameFile` hardcodes `range: None` and
        // `ignore_whitespace: false`; the requested range is applied to the
        // returned hunks below (production approximation, M9.19a).
        let mut stream = client
            .stream_blame_file_at(&addr, repo_name, path, oid)
            .await
            .map_err(GitSourceError::from)?;
        let mut hunks = Vec::new();
        while let Some(hunk) = stream.next().await {
            hunks.push(hunk.map_err(GitSourceError::from)?);
        }
        let _ = ignore_whitespace;
        if let (Some(start), Some(end)) = (start_line, end_line) {
            let start = u32::try_from(start.max(0)).unwrap_or(0);
            let end = u32::try_from(end.max(0)).unwrap_or(0);
            hunks.retain(|hunk| hunk.start_line <= end && hunk.end_line >= start);
        }
        Ok(hunks)
    }
}

/// The production [`GitObjectSource`] the frontend installs on
/// [`SchemaGraphqlDeps`](crate::graphql_api::SchemaGraphqlDeps) (M9.9d).
///
/// `service.rs` calls this with the live configuration; the differential
/// harness uses the same constructor with a fixed endpoint to assert the
/// assembly and address resolution without touching the network.
pub fn production_git_source(live: Arc<sg_config::LiveConfig>) -> Arc<GitserverGitObjects> {
    Arc::new(GitserverGitObjects::new(live))
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

/// Rich `ReadDir` outcome for `GitTree.entries`/`files`/`directories`
/// (M9.43b). `Found(vec![])` is the baseline's swallowed
/// `file does not exist` empty-tree case.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DirEntriesLookup {
    Found(Vec<sg_git::FileStat>),
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
    /// M9.8iz: the remaining `GitCommit` fields.
    pub extra: crate::git_commit_extra::CommitExtraPrefetch,
    /// M9.8jd: `Repository.comparison` and file-diff content/highlight.
    pub comparison: crate::git_comparison_graphql::ComparisonPrefetch,
    pub tree_entries: HashMap<(i32, String, String), TreeEntryLookup>,
    pub contents: HashMap<(i32, String, String), ContentLookup>,
    pub dirs: HashMap<(i32, String, String), DirLookup>,
    /// M9.43b: `GitTree.entries`/`files`/`directories` listings, keyed by
    /// `(repo id, oid, path, recursive)`.
    pub dir_entries: HashMap<(i32, String, String, bool), DirEntriesLookup>,
    /// Submodule clone URLs seen while prefetching tree entries (the
    /// `path.Join(repoName, submoduleURL)` form, M9.8iy).
    pub submodule_urls: Vec<String>,
    /// `cloneURLToRepoName` results for [`Self::submodule_urls`], keyed by the
    /// same string. `None` records a failed/unresolvable lookup, matching the
    /// baseline's empty `url.URL{}`.
    pub submodule_repo_names: HashMap<String, Option<String>>,
    /// `GitBlob.blame` results (M9.19a), keyed by commit/path/arguments.
    pub blame: HashMap<BlameKey, BlameLookup>,
    /// `gitserver.ListRefs` results (M9.19a).
    pub refs: HashMap<RefsKey, Result<Vec<sg_git::GitRef>, String>>,
    /// `gitserver.GetDefaultBranch(repo, false)` results (M9.19a).
    pub default_branches: HashMap<i32, DefaultBranchLookup>,
    /// `gitserver.ContributorCount` results (M9.19a).
    pub contributors: HashMap<ContributorsKey, ContributorLookup>,
    /// `symbol.DefaultZoektSymbolsClient()` injection point. `GitTreeEntry` /
    /// `GitCommit` `symbols` call the (synchronous) gRPC client here.
    pub symbols: Option<Arc<dyn crate::search_searcher_client::SymbolsClient + Send + Sync>>,
    /// M9.51a: the concrete symbols gRPC client used by
    /// `GitBlob.localCodeIntel` / `GitBlob.symbolInfo`
    /// (`symbols.DefaultClient` in the baseline). `None` renders the
    /// unassembled nil-payload/null shapes.
    pub symbols_service: Option<Arc<crate::symbols_client::GrpcSymbolsClient>>,
    /// M9.37c: `GitBlob.highlight` / `GitTreeEntry.highlight` results, keyed
    /// like the comparison file-diff highlights.
    pub tree_highlights: HashMap<
        crate::git_comparison_graphql::HighlightKey,
        Result<crate::git_comparison_graphql::HighlightedFile, String>,
    >,
    /// M9.46b: `conf.ExternalURL()`-equivalent used by
    /// `GitTreeEntry.rawZipArchiveURL`. Empty means "no live site config";
    /// the resolver then falls back to Go's cached `http://example.com`.
    pub external_url: String,
    /// M9.48c: optional own GraphQL surface backing `GitBlob.ownership`,
    /// `GitTree.ownership`/`ownershipStats` and `GitCommit.ownership`.
    /// `None` leaves those fields unported.
    pub own: Option<crate::own_graphql::OwnHandle>,
    /// M9.48b: the Code Intel code-navigation seam behind
    /// `GitBlob.lsif`/`GitTree.lsif`/`GitBlob.codeGraphData`. `None` keeps the
    /// optional surface unassembled.
    pub codenav: Option<crate::codeintel_codenav_graphql::CodeIntelCodenavHandle>,
}

impl Clone for GitPrefetch {
    fn clone(&self) -> Self {
        Self {
            revisions: self.revisions.clone(),
            objects: self.objects.clone(),
            commits: self.commits.clone(),
            extra: self.extra.clone(),
            comparison: self.comparison.clone(),
            tree_entries: self.tree_entries.clone(),
            contents: self.contents.clone(),
            dirs: self.dirs.clone(),
            dir_entries: self.dir_entries.clone(),
            submodule_urls: self.submodule_urls.clone(),
            submodule_repo_names: self.submodule_repo_names.clone(),
            blame: self.blame.clone(),
            refs: self.refs.clone(),
            default_branches: self.default_branches.clone(),
            contributors: self.contributors.clone(),
            symbols: self.symbols.clone(),
            symbols_service: self.symbols_service.clone(),
            tree_highlights: self.tree_highlights.clone(),
            external_url: self.external_url.clone(),
            own: self.own.clone(),
            codenav: self.codenav.clone(),
        }
    }
}

impl std::fmt::Debug for GitPrefetch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GitPrefetch")
            .field("revisions", &self.revisions)
            .field("objects", &self.objects)
            .field("commits", &self.commits)
            .field("extra", &self.extra)
            .field("comparison", &self.comparison)
            .field("tree_entries", &self.tree_entries)
            .field("contents", &self.contents)
            .field("dirs", &self.dirs)
            .field("dir_entries", &self.dir_entries)
            .field("submodule_urls", &self.submodule_urls)
            .field("submodule_repo_names", &self.submodule_repo_names)
            .field("blame", &self.blame)
            .field("refs", &self.refs)
            .field("default_branches", &self.default_branches)
            .field("contributors", &self.contributors)
            .field(
                "symbols",
                &self.symbols.as_ref().map(|_| "<symbols client>"),
            )
            .field(
                "symbols_service",
                &self.symbols_service.as_ref().map(|_| "<symbols service>"),
            )
            .field("tree_highlights", &self.tree_highlights)
            .field("external_url", &self.external_url)
            .field("own", &self.own.as_ref().map(|_| "<own handle>"))
            .field("codenav", &self.codenav.as_ref().map(|_| "<codenav>"))
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

    /// The prefetched rich directory listing for
    /// `(oid, path, recursive)` (`GitTree.entries`/`files`/`directories`).
    pub fn dir_entries(
        &self,
        repo_id: i32,
        oid: &str,
        path: &str,
        recursive: bool,
    ) -> Option<&DirEntriesLookup> {
        self.dir_entries
            .get(&(repo_id, oid.to_owned(), path.to_owned(), recursive))
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

    /// The prefetched blame for one blob/argument set (M9.19a).
    pub fn blame(&self, key: &BlameKey) -> Option<&BlameLookup> {
        self.blame.get(key)
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

pub(crate) async fn load_revision(
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
    // The extra commit fields only need the OID, so prefetch them even when no
    // core data field is selected.
    crate::git_commit_extra::prefetch_commit_extra(source, prefetch, repo, oid, selections, depth)
        .await;
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
    // The extra commit fields only need the OID, so prefetch them even when no
    // core data field is selected.
    crate::git_commit_extra::prefetch_commit_extra(source, prefetch, repo, oid, selections, depth)
        .await;
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
    prefetch_blame(source, prefetch, repo, oid, selections, depth).await;
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

/// `GitTreeEntryResolver.Blame` (`git_blob.go:18-56`) for the
/// `tree`/`blob`/`file`/`path` fields whose selection set asks for `blame`.
///
/// Only non-directory entries expose it (`ToGitBlob` returns
/// `!r.IsDirectory()`); the baseline validates `startLine`/`endLine` at resolve
/// time, so a mismatched pair is not fetched here.
async fn prefetch_blame(
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
    for field in selections
        .iter()
        .filter(|field| matches!(field.name.as_str(), "tree" | "blob" | "file" | "path"))
    {
        let nested: Vec<&RootField> = field
            .selections
            .iter()
            .chain(field.type_conditions.iter().flat_map(|(_, nested)| nested.iter()))
            .collect();
        let Some(blame) = nested.iter().find(|selection| selection.name == "blame") else {
            continue;
        };
        let path = field.arguments.string("path").unwrap_or("");
        let key = (repo.id, oid.to_owned(), path.to_owned());
        let is_blob = matches!(
            prefetch.tree_entries.get(&key),
            Some(TreeEntryLookup::Found(stat)) if !stat.is_dir()
        );
        if !is_blob {
            continue;
        }
        let start_line = blame.arguments.i64("startLine").map(|line| line as i32);
        let end_line = blame.arguments.i64("endLine").map(|line| line as i32);
        if start_line.is_some() != end_line.is_some() {
            continue;
        }
        let ignore_whitespace = blame.arguments.bool("ignoreWhitespace").unwrap_or(false);
        let blame_key = BlameKey {
            repo_id: repo.id,
            oid: oid.to_owned(),
            path: path.to_owned(),
            start_line,
            end_line,
            ignore_whitespace,
        };
        if !prefetch.blame.contains_key(&blame_key) {
            let lookup = match source
                .blame(
                    repo.id,
                    &repo.name,
                    oid,
                    path,
                    start_line,
                    end_line,
                    ignore_whitespace,
                )
                .await
            {
                Ok(hunks) => BlameLookup::Found(hunks),
                Err(error) => BlameLookup::Error(error.message),
            };
            prefetch.blame.insert(blame_key.clone(), lookup);
        }
        let Some(BlameLookup::Found(hunks)) = prefetch.blame.get(&blame_key).cloned() else {
            continue;
        };
        if let Some(commit) = nested
            .iter()
            .find(|selection| selection.name == "commit")
            .copied()
        {
            let mut seen: HashSet<String> = HashSet::new();
            for hunk in hunks {
                if !seen.insert(hunk.commit_id.clone()) {
                    continue;
                }
                Box::pin(prefetch_commit_data(
                    source,
                    prefetch,
                    repo,
                    &hunk.commit_id,
                    &commit.selections,
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

/// `gitserver.GetDefaultBranch`, cached per repository.
pub(crate) async fn load_default_branch(
    source: &dyn GitObjectSource,
    prefetch: &mut GitPrefetch,
    repo: &MinimalRepo,
) -> DefaultBranchLookup {
    if let Some(lookup) = prefetch.default_branches.get(&repo.id) {
        return lookup.clone();
    }
    let lookup = match source.default_branch(repo.id, &repo.name).await {
        Ok(None) => DefaultBranchLookup::NotFound,
        Ok(Some(name)) => DefaultBranchLookup::Found(name),
        Err(error) => DefaultBranchLookup::Error(error.message),
    };
    prefetch.default_branches.insert(repo.id, lookup.clone());
    lookup
}

/// `GitRefType`: `GIT_BRANCH` → heads only, `GIT_TAG` → tags only, `null` →
/// both, anything else → neither (`repository_git_refs.go:28-34`).
fn refs_type(arguments: &Arguments) -> (bool, bool) {
    match arguments.string("type") {
        None => (true, true),
        Some("GIT_BRANCH") => (true, false),
        Some("GIT_TAG") => (false, true),
        Some(_) => (false, false),
    }
}

/// The M9.19a gitserver reads used by repository object fields: default
/// branch, `ListRefs` and `ContributorCount`.
async fn prefetch_repository_inventory_d(
    source: &dyn GitObjectSource,
    prefetch: &mut GitPrefetch,
    repo: &MinimalRepo,
    selections: &[RootField],
) {
    let mut needs_default_branch = false;
    for field in selections {
        match field.name.as_str() {
            "externalURLs" | "textSearchIndex" | "defaultBranch" => {
                needs_default_branch = true;
            }
            "gitRefs" => {
                let (heads, tags) = refs_type(&field.arguments);
                let key = RefsKey {
                    repo_id: repo.id,
                    heads_only: heads,
                    tags_only: tags,
                };
                if !prefetch.refs.contains_key(&key) {
                    let lookup = match source
                        .list_refs(repo.id, &repo.name, heads, tags)
                        .await
                    {
                        Ok(refs) => Ok(refs),
                        Err(error) => Err(error.message),
                    };
                    prefetch.refs.insert(key, lookup);
                }
            }
            "branches" | "tags" => {
                let key = RefsKey {
                    repo_id: repo.id,
                    heads_only: field.name == "branches",
                    tags_only: field.name == "tags",
                };
                if !prefetch.refs.contains_key(&key) {
                    let lookup = match source
                        .list_refs(repo.id, &repo.name, key.heads_only, key.tags_only)
                        .await
                    {
                        Ok(refs) => Ok(refs),
                        Err(error) => Err(error.message),
                    };
                    prefetch.refs.insert(key, lookup);
                }
            }
            "contributors" => {
                let after_date = field
                    .arguments
                    .string("afterDate")
                    .filter(|value| !value.is_empty())
                    .map(str::to_owned);
                let key = ContributorsKey {
                    repo_id: repo.id,
                    revision_range: field
                        .arguments
                        .string("revisionRange")
                        .map(str::to_owned),
                    after_date: after_date.clone(),
                    path: field.arguments.string("path").map(str::to_owned),
                };
                if prefetch.contributors.contains_key(&key) {
                    continue;
                }
                let lookup = match &after_date {
                    Some(value) if crate::search_commit_job::parse_git_date(value).is_none() => {
                        ContributorLookup::InvalidDate(
                            "failed to parse after date: invalid date format".to_owned(),
                        )
                    }
                    _ => match source
                        .contributor_count(
                            repo.id,
                            &repo.name,
                            key.revision_range.as_deref(),
                            key.after_date.as_deref(),
                            key.path.as_deref(),
                        )
                        .await
                    {
                        Ok(counts) => ContributorLookup::Counts(Ok(counts)),
                        Err(error) => ContributorLookup::Counts(Err(error.message)),
                    },
                };
                prefetch.contributors.insert(key, lookup);
            }
            _ => {}
        }
    }
    if needs_default_branch {
        load_default_branch(source, prefetch, repo).await;
    }
}

/// The `commit(rev:)` field of a repository-shaped selection set.
async fn prefetch_repository_selections(
    source: &dyn GitObjectSource,
    prefetch: &mut GitPrefetch,
    repo: &MinimalRepo,
    selections: &[RootField],
) {
    if let Some(commit) = find_field(selections, "commit") {
        let rev = commit.arguments.string("rev").unwrap_or_default();
        prefetch_commit_rev(source, prefetch, repo, rev, &commit.selections).await;
    }
    prefetch_repository_inventory_d(source, prefetch, repo, selections).await;
    crate::git_comparison_graphql::prefetch_repository_comparison(
        source, prefetch, repo, selections,
    )
    .await;
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
                    // `repositoryRedirect` is the `RepositoryRedirect` union:
                    // the `Repository` fields (including `defaultBranch`) live
                    // in the `... on Repository` inline fragment.
                    let selections: Vec<RootField> =
                        selections_for_type(field, "Repository")
                            .into_iter()
                            .cloned()
                            .collect();
                    prefetch_repository_selections(
                        source,
                        &mut prefetch,
                        &repo_of(data),
                        &selections,
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

pub(crate) fn signature_date(date: Option<&prost_types::Timestamp>) -> String {
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
                "GitObject requires the gitserver GitSource (not wired for this request)",
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
                        "GitObject.commit requires the gitserver GitSource (not wired for this request)",
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
        actor: Actor,
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
                let value = crate::git_tree_entry_graphql::commit_tree_entry_field(
                    self, field, arguments,
                )?;
                // M9.19a: a file entry is a `GitBlob`, which exposes `blame`.
                Ok(value.map(|value| match value {
                    FieldValue::Object(inner) if inner.type_name() == "GitBlob" => {
                        FieldValue::Object(Arc::new(crate::graphql_inventory_d::GitBlobD {
                            inner,
                            repo: self.repo.clone(),
                            oid: self.oid.clone(),
                            path: arguments.string("path").unwrap_or("").to_owned(),
                            git: self.git.clone(),
                        }))
                    }
                    other => other,
                }))
            }
            // `GitCommitResolver.Symbols` (`symbols.go:59-76`).
            "symbols" => crate::git_tree_entry_graphql::commit_symbols_field(self, arguments),
            // M9.48c: `GitCommit.ownership` (Go `git_commit.go:471-473`).
            "ownership" => {
                let Some(own) = self.git.as_ref().and_then(|git| git.own.as_ref()) else {
                    return Ok(None);
                };
                crate::own_graphql::git_commit_ownership(
                    own,
                    &self.repo,
                    &self.oid,
                    arguments,
                    actor,
                )
            }
            _ => crate::git_commit_extra::commit_extra_field(self, field, arguments),
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
