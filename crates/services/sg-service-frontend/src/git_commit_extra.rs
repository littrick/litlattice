#![forbid(unsafe_code)]

//! `GitCommitResolver`'s remaining fields (M9.8iz): `ancestors`, `diff`
//! (`RepositoryComparison` core), `behindAhead`, `languages`,
//! `languageStatistics`, `fileNames` and `ownership`.
//!
//! All gitserver reads are still prefetched into [`CommitExtraPrefetch`] (a
//! field of [`crate::git_graphql::GitPrefetch`]) because `ObjectValue::field`
//! is synchronous; the resolvers only replay those results. The implementation
//! lives in this module and `git_graphql.rs` only delegates unknown
//! `GitCommit` fields here, to stay mergeable with M9.8iy's tree/symbol work.
//!
//! Baseline: `cmd/frontend/graphqlbackend/{git_commit.go,git_commits.go,
//! repository_comparison.go,git_revision.go}`, `cmd/frontend/internal/inventory`,
//! `internal/own` and the go-diff printing/stat helpers.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use async_trait::async_trait;
use sha2::{Digest, Sha256};

use sg_git::{CommitDetails, CommitsOptions, DiffRangeType, RawDiffOptions};
use sg_graphql::exec::{
    Actor, Arguments, ExecError, FieldValue, InputValue, ObjectValue, RootField,
};
use sg_graphql::json::Json;
use sg_service_gitserver::diff::{parse_multi_file_diff, FileDiff, Hunk};

use crate::git_graphql::{
    load_revision, prefetch_commit_data, CommitLookup, GitCommitResolverObject,
    GitObjectResolverObject, GitObjectSource, GitPrefetch, GitSourceError, GitserverGitObjects,
    RevisionLookup, MAX_COMMIT_DEPTH,
};
use crate::graphql_resolvers::{PageInfoObject, RepositoryData};
use crate::search_graphql::LanguageStat;
use crate::search_stream::convert::MinimalRepo;

// ---------------------------------------------------------------------------
// Source seam
// ---------------------------------------------------------------------------

/// The gitserver reads behind the remaining `GitCommit` fields.
///
/// `GitserverGitObjects` implements the production version in this module.
/// Test sources implement only what they need: every method has a default
/// that reports the read as unavailable.
#[async_trait]
pub trait GitCommitExtraSource: Send + Sync {
    /// `gitserver.Client.Commits` with full commit details.
    async fn commit_log(
        &self,
        _repo_id: i32,
        _repo_name: &str,
        _options: &CommitsOptions,
    ) -> Result<Vec<CommitDetails>, GitSourceError> {
        Err(GitSourceError::new("commit log is not available"))
    }

    /// `gitserver.Client.MergeBase`.
    async fn merge_base(
        &self,
        _repo_id: i32,
        _repo_name: &str,
        _base: &str,
        _head: &str,
    ) -> Result<String, GitSourceError> {
        Err(GitSourceError::new("merge base is not available"))
    }

    /// `gitserver.Client.Diff` (raw multi-file unified diff).
    async fn raw_diff(
        &self,
        _repo_id: i32,
        _repo_name: &str,
        _options: &RawDiffOptions,
    ) -> Result<Vec<u8>, GitSourceError> {
        Err(GitSourceError::new("raw diff is not available"))
    }

    /// `gitserver.Client.BehindAhead`, as `(behind, ahead)`.
    async fn behind_ahead(
        &self,
        _repo_id: i32,
        _repo_name: &str,
        _left: &str,
        _right: &str,
    ) -> Result<(u32, u32), GitSourceError> {
        Err(GitSourceError::new("behind/ahead counts are not available"))
    }

    /// `gitserver.Client.ReadDir(recursive=true)`, file names only.
    async fn file_names(
        &self,
        _repo_id: i32,
        _repo_name: &str,
        _commit: &str,
    ) -> Result<Vec<String>, GitSourceError> {
        Err(GitSourceError::new("file names are not available"))
    }

    /// `backend.Repos().GetInventory`: the per-language statistics.
    async fn language_inventory(
        &self,
        _repo_id: i32,
        _repo_name: &str,
        _commit: &str,
    ) -> Result<Vec<LanguageStat>, GitSourceError> {
        Err(GitSourceError::new("language inventory is not available"))
    }

    /// Enterprise `OwnResolver.GitCommitOwnership` summary. OSS has no own
    /// resolver, so the default is an empty connection.
    async fn commit_ownership(
        &self,
        _repo_id: i32,
        _repo_name: &str,
        _commit: &str,
    ) -> Result<OwnershipSummary, GitSourceError> {
        Ok(OwnershipSummary::default())
    }
}

/// The fields of an ownership connection this slice represents. The actual
/// owners (CODEOWNERS/recent-contributor signals) are an enterprise seam.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct OwnershipSummary {
    pub total_count: i32,
    pub total_owners: i32,
}

// ---------------------------------------------------------------------------
// Prefetch
// ---------------------------------------------------------------------------

/// Arguments of `GitCommit.ancestors`; part of the prefetch cache key.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct AncestorsKey {
    pub oid: String,
    pub first: Option<i32>,
    pub query: Option<String>,
    pub path: Option<String>,
    pub follow: bool,
    pub after: Option<String>,
    pub after_cursor: Option<String>,
    pub before: Option<String>,
}

/// `GitCommit.diff(base:)`; `base` is the effective revspec (`oid+"~"` when
/// the argument is absent).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DiffKey {
    pub oid: String,
    pub base: String,
}

/// `NewRepositoryComparison`'s eagerly-resolved revisions plus the lazily
/// computed `fileDiffs`/`commits` sub-results.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ComparisonData {
    pub base_revspec: String,
    pub head_revspec: String,
    pub range_type: String,
    pub base_oid: Option<String>,
    pub head_oid: String,
    pub file_diffs: Option<Result<Vec<FileDiff>, String>>,
    pub commits: Option<Result<Vec<CommitDetails>, String>>,
}

/// `(repo id, left revspec, right revspec)`.
type BehindAheadKey = (i32, String, String);
type BehindAheadLookup = Result<(u32, u32), String>;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CommitExtraPrefetch {
    pub ancestors: HashMap<AncestorsKey, Result<Vec<CommitDetails>, String>>,
    pub comparisons: HashMap<DiffKey, Result<ComparisonData, String>>,
    pub behind_ahead: HashMap<BehindAheadKey, BehindAheadLookup>,
    pub file_names: HashMap<(i32, String), Result<Vec<String>, String>>,
    pub inventories: HashMap<(i32, String), Result<Vec<LanguageStat>, String>>,
    pub ownerships: HashMap<(i32, String), Result<OwnershipSummary, String>>,
}

// ---------------------------------------------------------------------------
// Selection helpers
// ---------------------------------------------------------------------------

fn selected<'a>(selections: &'a [RootField], name: &str) -> Option<&'a RootField> {
    selections.iter().find(|field| field.name == name)
}

fn int_arg(arguments: &Arguments, name: &str) -> Option<i32> {
    arguments.i64(name).map(|value| value as i32)
}

fn string_list(arguments: &Arguments, name: &str) -> Vec<String> {
    match arguments.get(name) {
        Some(InputValue::List(items)) => items
            .iter()
            .filter_map(|item| item.as_str().map(str::to_owned))
            .collect(),
        _ => Vec::new(),
    }
}

fn repository_data(repo: &MinimalRepo) -> RepositoryData {
    RepositoryData {
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

fn ancestors_key(oid: &str, field: &RootField) -> AncestorsKey {
    AncestorsKey {
        oid: oid.to_owned(),
        first: int_arg(&field.arguments, "first"),
        query: field.arguments.string("query").map(str::to_owned),
        path: field.arguments.string("path").map(str::to_owned),
        follow: field.arguments.bool("follow").unwrap_or(false),
        after: field.arguments.string("after").map(str::to_owned),
        after_cursor: field.arguments.string("afterCursor").map(str::to_owned),
        before: field.arguments.string("before").map(str::to_owned),
    }
}

fn diff_key(oid: &str, field: &RootField) -> DiffKey {
    DiffKey {
        oid: oid.to_owned(),
        base: field
            .arguments
            .string("base")
            .map(str::to_owned)
            .unwrap_or_else(|| format!("{oid}~")),
    }
}

/// `strconv.Atoi` on `afterCursor` (empty is 0).
fn parse_after_cursor(value: Option<&str>) -> Result<i32, String> {
    match value {
        None | Some("") => Ok(0),
        Some(text) => text
            .parse::<i32>()
            .map_err(|_| format!("strconv.Atoi: parsing {text:?}: invalid syntax")),
    }
}

// ---------------------------------------------------------------------------
// Prefetch implementation
// ---------------------------------------------------------------------------

/// Prefetch the extra fields selected on a commit. Called for every commit
/// reached by the core prefetcher (including parents and ancestors' nodes).
pub(crate) async fn prefetch_commit_extra(
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
    let Some(extra) = source.commit_extra() else {
        return;
    };
    for field in selections {
        match field.name.as_str() {
            "ancestors" => {
                prefetch_ancestors(extra, source, prefetch, repo, oid, field, depth).await
            }
            "diff" => prefetch_comparison(extra, source, prefetch, repo, oid, field, depth).await,
            "behindAhead" => prefetch_behind_ahead(extra, prefetch, repo, oid, field).await,
            "languages" | "languageStatistics" => {
                prefetch_inventory(extra, prefetch, repo, oid).await
            }
            "fileNames" => prefetch_file_names(extra, prefetch, repo, oid).await,
            "ownership" => prefetch_ownership(extra, prefetch, repo, oid).await,
            _ => {}
        }
    }
}

async fn prefetch_ancestors(
    extra: &dyn GitCommitExtraSource,
    source: &dyn GitObjectSource,
    prefetch: &mut GitPrefetch,
    repo: &MinimalRepo,
    oid: &str,
    field: &RootField,
    depth: usize,
) {
    let key = ancestors_key(oid, field);
    if !prefetch.extra.ancestors.contains_key(&key) {
        let result = compute_ancestors(extra, source, prefetch, repo, oid, &key).await;
        prefetch.extra.ancestors.insert(key.clone(), result);
    }
    let Ok(commits) = prefetch.extra.ancestors[&key].clone() else {
        return;
    };
    // `gitCommitConnectionResolver.Nodes` presets each commit resolver with the
    // commit data from `Commits`; replay it and recurse for selected children.
    let Some(nodes) = selected(&field.selections, "nodes") else {
        return;
    };
    for commit in commits {
        prefetch.commits.insert(
            (repo.id, commit.oid.clone()),
            CommitLookup::Found(commit.clone()),
        );
        Box::pin(prefetch_commit_data(
            source,
            prefetch,
            repo,
            &commit.oid,
            &nodes.selections,
            depth + 1,
        ))
        .await;
    }
}

async fn compute_ancestors(
    extra: &dyn GitCommitExtraSource,
    source: &dyn GitObjectSource,
    prefetch: &mut GitPrefetch,
    repo: &MinimalRepo,
    oid: &str,
    key: &AncestorsKey,
) -> Result<Vec<CommitDetails>, String> {
    let mut n = key.first.unwrap_or(0);
    if n > 1 {
        n += 1;
    }
    let skip = match parse_after_cursor(key.after_cursor.as_deref()) {
        Ok(skip) => skip.max(0),
        Err(error) => return Err(format!("failed to parse afterCursor: {error}")),
    };
    // The baseline resolves the revision range before listing commits.
    match load_revision(source, prefetch, repo, oid).await {
        RevisionLookup::Found(_) => {}
        RevisionLookup::NotFound => {
            return Err(format!(
                "failed to resolve revision range: revision not found: {}@{oid}",
                repo.name
            ))
        }
        RevisionLookup::Error(message) => {
            return Err(format!("failed to resolve revision range: {message}"))
        }
    }
    let options = CommitsOptions {
        ranges: vec![if oid.is_empty() { "HEAD" } else { oid }.to_owned()],
        max_commits: n.max(0) as u32,
        skip: skip as u32,
        message_query: key.query.clone().unwrap_or_default(),
        author: String::new(),
        path: key.path.clone().unwrap_or_default(),
        follow_path_renames: key.follow,
        ..CommitsOptions::default()
    };
    extra
        .commit_log(repo.id, &repo.name, &options)
        .await
        .map_err(|error| error.message)
}

async fn prefetch_behind_ahead(
    extra: &dyn GitCommitExtraSource,
    prefetch: &mut GitPrefetch,
    repo: &MinimalRepo,
    oid: &str,
    field: &RootField,
) {
    let Some(revspec) = field.arguments.string("revspec") else {
        return;
    };
    let key = (repo.id, oid.to_owned(), revspec.to_owned());
    if prefetch.extra.behind_ahead.contains_key(&key) {
        return;
    }
    let result = match extra.behind_ahead(repo.id, &repo.name, revspec, oid).await {
        Ok(counts) => Ok(counts),
        Err(error) => Err(error.message),
    };
    prefetch.extra.behind_ahead.insert(key, result);
}

async fn prefetch_inventory(
    extra: &dyn GitCommitExtraSource,
    prefetch: &mut GitPrefetch,
    repo: &MinimalRepo,
    oid: &str,
) {
    let key = (repo.id, oid.to_owned());
    if prefetch.extra.inventories.contains_key(&key) {
        return;
    }
    let result = extra
        .language_inventory(repo.id, &repo.name, oid)
        .await
        .map_err(|error| error.message);
    prefetch.extra.inventories.insert(key, result);
}

async fn prefetch_file_names(
    extra: &dyn GitCommitExtraSource,
    prefetch: &mut GitPrefetch,
    repo: &MinimalRepo,
    oid: &str,
) {
    let key = (repo.id, oid.to_owned());
    if prefetch.extra.file_names.contains_key(&key) {
        return;
    }
    let result = extra
        .file_names(repo.id, &repo.name, oid)
        .await
        .map_err(|error| error.message);
    prefetch.extra.file_names.insert(key, result);
}

async fn prefetch_ownership(
    extra: &dyn GitCommitExtraSource,
    prefetch: &mut GitPrefetch,
    repo: &MinimalRepo,
    oid: &str,
) {
    let key = (repo.id, oid.to_owned());
    if prefetch.extra.ownerships.contains_key(&key) {
        return;
    }
    let result = extra
        .commit_ownership(repo.id, &repo.name, oid)
        .await
        .map_err(|error| error.message);
    prefetch.extra.ownerships.insert(key, result);
}

async fn prefetch_comparison(
    extra: &dyn GitCommitExtraSource,
    source: &dyn GitObjectSource,
    prefetch: &mut GitPrefetch,
    repo: &MinimalRepo,
    oid: &str,
    field: &RootField,
    depth: usize,
) {
    let key = diff_key(oid, field);
    if !prefetch.extra.comparisons.contains_key(&key) {
        let result = compute_comparison(extra, source, prefetch, repo, oid, &key).await;
        prefetch.extra.comparisons.insert(key.clone(), result);
    }
    let Ok(data) = prefetch.extra.comparisons[&key].clone() else {
        return;
    };

    // `fileDiffs` and `commits` are lazy in Go: only compute them when the
    // selection set can observe them.
    if let Some(file_diffs_field) = selected(&field.selections, "fileDiffs") {
        let paths = string_list(&file_diffs_field.arguments, "paths");
        let result = compute_file_diffs(extra, repo, &data, paths).await;
        if let Ok(files) = &result {
            crate::git_comparison_graphql::prefetch_file_diff_extras(
                source,
                prefetch,
                repo,
                &data,
                files,
                &file_diffs_field.selections,
            )
            .await;
        }
        if let Some(Ok(comparison)) = prefetch.extra.comparisons.get_mut(&key) {
            comparison.file_diffs = Some(result);
        }
    }
    if let Some(commits_field) = selected(&field.selections, "commits") {
        let first = int_arg(&commits_field.arguments, "first");
        let path = commits_field
            .arguments
            .string("path")
            .map(str::to_owned)
            .unwrap_or_default();
        let result = compute_comparison_commits(
            extra,
            source,
            prefetch,
            repo,
            &data,
            first,
            &path,
            &commits_field.selections,
            depth,
        )
        .await;
        if let Some(Ok(comparison)) = prefetch.extra.comparisons.get_mut(&key) {
            comparison.commits = Some(result);
        }
    }
}

async fn compute_comparison(
    extra: &dyn GitCommitExtraSource,
    source: &dyn GitObjectSource,
    prefetch: &mut GitPrefetch,
    repo: &MinimalRepo,
    oid: &str,
    key: &DiffKey,
) -> Result<ComparisonData, String> {
    let base_revspec = key.base.clone();
    let head_revspec = oid.to_owned();

    // `getCommit(head)`.
    let head_oid = match load_revision(source, prefetch, repo, &head_revspec).await {
        RevisionLookup::Found(oid) => oid,
        RevisionLookup::NotFound => {
            return Err(format!("revision not found: {}@{head_revspec}", repo.name))
        }
        RevisionLookup::Error(message) => return Err(message),
    };

    let mut range_type = "..".to_owned();
    let mut base_oid = None;
    if base_revspec != sg_git::DEV_NULL_SHA {
        range_type = "...".to_owned();
        let merge_base = extra
            .merge_base(repo.id, &repo.name, &base_revspec, &head_revspec)
            .await
            .map_err(|error| error.message)?;
        let base_commit = if merge_base.is_empty() {
            range_type = "..".to_owned();
            base_revspec.clone()
        } else {
            merge_base
        };
        if base_commit == sg_git::DEV_NULL_SHA {
            base_oid = None;
        } else {
            match load_revision(source, prefetch, repo, &base_commit).await {
                RevisionLookup::Found(oid) => base_oid = Some(oid),
                RevisionLookup::NotFound => {
                    return Err(format!("revision not found: {}@{base_commit}", repo.name))
                }
                RevisionLookup::Error(message) => return Err(message),
            }
        }
        // `GitRevisionRange.base.object` resolves the original base revspec.
        load_revision(source, prefetch, repo, &base_revspec).await;
    }

    Ok(ComparisonData {
        base_revspec,
        head_revspec,
        range_type,
        base_oid,
        head_oid,
        file_diffs: None,
        commits: None,
    })
}

pub(crate) async fn compute_file_diffs(
    extra: &dyn GitCommitExtraSource,
    repo: &MinimalRepo,
    data: &ComparisonData,
    paths: Vec<String>,
) -> Result<Vec<FileDiff>, String> {
    let range_type = if data.range_type == ".." {
        Some(DiffRangeType::OnlyInHead)
    } else {
        Some(DiffRangeType::Intersection)
    };
    let raw = extra
        .raw_diff(
            repo.id,
            &repo.name,
            &RawDiffOptions {
                base: data
                    .base_oid
                    .clone()
                    .unwrap_or_else(|| data.base_revspec.clone()),
                head: data.head_oid.clone(),
                range_type,
                paths,
            },
        )
        .await
        .map_err(|error| error.message)?;
    parse_multi_file_diff(&raw).map_err(|error| error.to_string())
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn compute_comparison_commits(
    extra: &dyn GitCommitExtraSource,
    source: &dyn GitObjectSource,
    prefetch: &mut GitPrefetch,
    repo: &MinimalRepo,
    data: &ComparisonData,
    first: Option<i32>,
    path: &str,
    node_selections: &[RootField],
    depth: usize,
) -> Result<Vec<CommitDetails>, String> {
    let range = format!("{}..{}", data.base_revspec, data.head_revspec);
    match load_revision(source, prefetch, repo, &range).await {
        RevisionLookup::Found(_) => {}
        RevisionLookup::NotFound => {
            return Err(format!(
                "failed to resolve revision range: revision not found: {}@{range}",
                repo.name
            ))
        }
        RevisionLookup::Error(message) => {
            return Err(format!("failed to resolve revision range: {message}"))
        }
    }
    let mut n = first.unwrap_or(0);
    if n > 1 {
        n += 1;
    }
    let options = CommitsOptions {
        ranges: vec![if range.is_empty() { "HEAD" } else { &range }.to_owned()],
        max_commits: n.max(0) as u32,
        path: path.to_owned(),
        ..CommitsOptions::default()
    };
    let commits = extra
        .commit_log(repo.id, &repo.name, &options)
        .await
        .map_err(|error| error.message)?;
    for commit in &commits {
        prefetch.commits.insert(
            (repo.id, commit.oid.clone()),
            CommitLookup::Found(commit.clone()),
        );
        Box::pin(prefetch_commit_data(
            source,
            prefetch,
            repo,
            &commit.oid,
            node_selections,
            depth + 1,
        ))
        .await;
    }
    Ok(commits)
}

// ---------------------------------------------------------------------------
// Production source
// ---------------------------------------------------------------------------

#[async_trait]
impl GitCommitExtraSource for GitserverGitObjects {
    async fn commit_log(
        &self,
        _repo_id: i32,
        repo_name: &str,
        options: &CommitsOptions,
    ) -> Result<Vec<CommitDetails>, GitSourceError> {
        let (client, _addr) = self.client_for(repo_name).await?;
        client
            .commits_details(repo_name, options)
            .await
            .map_err(GitSourceError::from)
    }

    async fn merge_base(
        &self,
        _repo_id: i32,
        repo_name: &str,
        base: &str,
        head: &str,
    ) -> Result<String, GitSourceError> {
        let (client, _addr) = self.client_for(repo_name).await?;
        client
            .merge_base(repo_name, base, head)
            .await
            .map_err(GitSourceError::from)
    }

    async fn raw_diff(
        &self,
        _repo_id: i32,
        repo_name: &str,
        options: &RawDiffOptions,
    ) -> Result<Vec<u8>, GitSourceError> {
        let (client, _addr) = self.client_for(repo_name).await?;
        client
            .raw_diff(repo_name, options)
            .await
            .map_err(GitSourceError::from)
    }

    async fn behind_ahead(
        &self,
        _repo_id: i32,
        repo_name: &str,
        left: &str,
        right: &str,
    ) -> Result<(u32, u32), GitSourceError> {
        let (client, _addr) = self.client_for(repo_name).await?;
        client
            .behind_ahead(repo_name, left, right)
            .await
            .map_err(GitSourceError::from)
    }

    async fn file_names(
        &self,
        _repo_id: i32,
        repo_name: &str,
        commit: &str,
    ) -> Result<Vec<String>, GitSourceError> {
        let (client, _addr) = self.client_for(repo_name).await?;
        let entries = client
            .read_dir_entries(repo_name, commit, "", true)
            .await
            .map_err(GitSourceError::from)?;
        Ok(entries
            .into_iter()
            .filter(|entry| !entry.is_dir)
            .map(|entry| entry.name)
            .collect())
    }

    async fn language_inventory(
        &self,
        _repo_id: i32,
        repo_name: &str,
        commit: &str,
    ) -> Result<Vec<LanguageStat>, GitSourceError> {
        let (client, _addr) = self.client_for(repo_name).await?;
        let entries = client
            .read_dir_entries(repo_name, commit, "", true)
            .await
            .map_err(GitSourceError::from)?;
        let mut by_language: BTreeMap<String, (u64, i64)> = BTreeMap::new();
        for entry in entries.into_iter().filter(|entry| !entry.is_dir) {
            let content = client
                .read_file(repo_name, commit, &entry.name)
                .await
                .map_err(GitSourceError::from)?;
            let languages = sg_service_searcher::langmatch::get_languages(
                &entry.name,
                Some(|| -> std::io::Result<Vec<u8>> { Ok(content.clone()) }),
            )
            .map_err(|error| GitSourceError::new(error.to_string()))?;
            let Some(language) = languages.into_iter().next() else {
                continue;
            };
            let stats = by_language.entry(language).or_insert((0, 0));
            stats.0 += content.len() as u64;
            stats.1 += count_lines(&content) as i64;
        }
        let mut languages: Vec<LanguageStat> = by_language
            .into_iter()
            .map(|(name, (total_bytes, total_lines))| LanguageStat {
                name,
                total_bytes,
                total_lines,
            })
            .collect();
        languages.sort_by(|left, right| {
            right
                .total_lines
                .cmp(&left.total_lines)
                .then_with(|| right.total_bytes.cmp(&left.total_bytes))
                .then_with(|| left.name.cmp(&right.name))
        });
        Ok(languages)
    }

    async fn commit_ownership(
        &self,
        _repo_id: i32,
        _repo_name: &str,
        _commit: &str,
    ) -> Result<OwnershipSummary, GitSourceError> {
        // OSS has no own resolver; the enterprise implementation is a seam.
        Ok(OwnershipSummary::default())
    }
}

/// `internal/inventory.countLines`: number of `\n` plus a final unterminated
/// non-empty line.
fn count_lines(content: &[u8]) -> usize {
    let newlines = content.iter().filter(|&&byte| byte == b'\n').count();
    if !content.is_empty() && !content.ends_with(b"\n") {
        newlines + 1
    } else {
        newlines
    }
}

// ---------------------------------------------------------------------------
// Resolvers
// ---------------------------------------------------------------------------

fn git_of<'a>(
    obj: &'a GitCommitResolverObject,
    field: &str,
) -> Result<&'a Arc<GitPrefetch>, ExecError> {
    obj.git.as_ref().ok_or_else(|| {
        ExecError::new(format!(
            "{field} requires the gitserver GitSource (not wired for this request)"
        ))
    })
}

/// Delegate for the `GitCommit` fields implemented in this module. `Ok(None)`
/// keeps the executor's "not ported" behavior for everything else.
pub fn commit_extra_field(
    obj: &GitCommitResolverObject,
    field: &str,
    arguments: &Arguments,
) -> Result<Option<FieldValue>, ExecError> {
    match field {
        "ancestors" => {
            let git = git_of(obj, field)?;
            Ok(Some(FieldValue::Object(Arc::new(CommitConnectionObject {
                repo: obj.repo.clone(),
                commits: ancestors_data(git, &obj.oid, arguments),
                first: int_arg(arguments, "first"),
                after_cursor: parse_after_cursor(arguments.string("afterCursor")).unwrap_or(0),
                git: obj.git.clone(),
            }))))
        }
        "diff" => {
            let git = git_of(obj, field)?;
            let key = DiffKey {
                oid: obj.oid.clone(),
                base: arguments
                    .string("base")
                    .map(str::to_owned)
                    .unwrap_or_else(|| format!("{}~", obj.oid)),
            };
            match git.extra.comparisons.get(&key) {
                Some(Ok(data)) => Ok(Some(FieldValue::Object(Arc::new(
                    RepositoryComparisonObject {
                        repo: obj.repo.clone(),
                        data: data.clone(),
                        git: obj.git.clone(),
                    },
                )))),
                Some(Err(message)) => Err(ExecError::new(message.clone())),
                None => Err(ExecError::new("diff is not prefetched")),
            }
        }
        "behindAhead" => {
            let git = git_of(obj, field)?;
            let revspec = arguments
                .string("revspec")
                .ok_or_else(|| ExecError::new("behindAhead requires revspec"))?;
            match git
                .extra
                .behind_ahead
                .get(&(obj.repo.id, obj.oid.clone(), revspec.to_owned()))
            {
                Some(Ok((behind, ahead))) => Ok(Some(FieldValue::Object(Arc::new(
                    BehindAheadCountsObject {
                        behind: *behind,
                        ahead: *ahead,
                    },
                )))),
                Some(Err(message)) => Err(ExecError::new(message.clone())),
                None => Err(ExecError::new("behindAhead is not prefetched")),
            }
        }
        "languages" => {
            let git = git_of(obj, field)?;
            let languages = inventory_data(git, &obj.repo, &obj.oid)?;
            Ok(Some(FieldValue::List {
                items: languages
                    .iter()
                    .map(|language| FieldValue::Json(Json::Str(language.name.clone())))
                    .collect(),
                element_type: Some("String"),
            }))
        }
        "languageStatistics" => {
            let git = git_of(obj, field)?;
            let languages = inventory_data(git, &obj.repo, &obj.oid)?;
            Ok(Some(FieldValue::List {
                items: languages
                    .iter()
                    .map(|language| {
                        FieldValue::Object(Arc::new(LanguageStatisticsObject {
                            language: language.clone(),
                        }))
                    })
                    .collect(),
                element_type: Some("LanguageStatistics"),
            }))
        }
        "fileNames" => {
            let git = git_of(obj, field)?;
            match git.extra.file_names.get(&(obj.repo.id, obj.oid.clone())) {
                Some(Ok(names)) => Ok(Some(FieldValue::List {
                    items: names
                        .iter()
                        .map(|name| FieldValue::Json(Json::Str(name.clone())))
                        .collect(),
                    element_type: Some("String"),
                })),
                Some(Err(message)) => Err(ExecError::new(message.clone())),
                None => Err(ExecError::new("fileNames is not prefetched")),
            }
        }
        "ownership" => {
            let git = git_of(obj, field)?;
            match git.extra.ownerships.get(&(obj.repo.id, obj.oid.clone())) {
                Some(Ok(summary)) => Ok(Some(FieldValue::Object(Arc::new(
                    OwnershipConnectionObject {
                        summary: summary.clone(),
                    },
                )))),
                Some(Err(message)) => Err(ExecError::new(message.clone())),
                None => Err(ExecError::new("ownership is not prefetched")),
            }
        }
        _ => Ok(None),
    }
}

fn ancestors_data(
    git: &GitPrefetch,
    oid: &str,
    arguments: &Arguments,
) -> Result<Vec<CommitDetails>, String> {
    let key = AncestorsKey {
        oid: oid.to_owned(),
        first: int_arg(arguments, "first"),
        query: arguments.string("query").map(str::to_owned),
        path: arguments.string("path").map(str::to_owned),
        follow: arguments.bool("follow").unwrap_or(false),
        after: arguments.string("after").map(str::to_owned),
        after_cursor: arguments.string("afterCursor").map(str::to_owned),
        before: arguments.string("before").map(str::to_owned),
    };
    match git.extra.ancestors.get(&key) {
        Some(Ok(commits)) => Ok(commits.clone()),
        Some(Err(message)) => Err(message.clone()),
        None => Err("ancestors is not prefetched".to_owned()),
    }
}

fn inventory_data(
    git: &GitPrefetch,
    repo: &MinimalRepo,
    oid: &str,
) -> Result<Vec<LanguageStat>, ExecError> {
    match git.extra.inventories.get(&(repo.id, oid.to_owned())) {
        Some(Ok(languages)) => Ok(languages.clone()),
        Some(Err(message)) => Err(ExecError::new(message.clone())),
        None => Err(ExecError::new("languages are not prefetched")),
    }
}

/// `gitCommitConnectionResolver` (ancestors and `RepositoryComparison.commits`).
pub struct CommitConnectionObject {
    pub repo: MinimalRepo,
    pub commits: Result<Vec<CommitDetails>, String>,
    pub first: Option<i32>,
    pub after_cursor: i32,
    pub git: Option<Arc<GitPrefetch>>,
}

impl ObjectValue for CommitConnectionObject {
    fn type_name(&self) -> &'static str {
        "GitCommitConnection"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        let commits = self
            .commits
            .as_ref()
            .map_err(|message| ExecError::new(message.clone()))?;
        match field {
            "nodes" => {
                let nodes = match self.first {
                    Some(first) if commits.len() > first.max(0) as usize => {
                        &commits[..first.max(0) as usize]
                    }
                    _ => commits.as_slice(),
                };
                Ok(Some(FieldValue::List {
                    items: nodes
                        .iter()
                        .map(|commit| {
                            FieldValue::Object(Arc::new(GitCommitResolverObject {
                                repo: self.repo.clone(),
                                oid: commit.oid.clone(),
                                input_rev: None,
                                commit: Some(commit.clone()),
                                commit_error: None,
                                git: self.git.clone(),
                            }))
                        })
                        .collect(),
                    element_type: Some("GitCommit"),
                }))
            }
            "totalCount" => {
                if self.first.is_some() {
                    Ok(Some(FieldValue::Null))
                } else {
                    Ok(Some(FieldValue::Json(Json::Int(commits.len() as i64))))
                }
            }
            "pageInfo" => {
                let page_info = match self.first {
                    None => PageInfoObject::new(false, None),
                    Some(first) => {
                        let limit = first.max(0) as usize;
                        let total = commits.len();
                        let got_single = limit == 1 && total == limit;
                        if total > limit || got_single {
                            PageInfoObject::new(
                                true,
                                Some((limit as i32 + self.after_cursor).to_string()),
                            )
                        } else {
                            PageInfoObject::new(false, None)
                        }
                    }
                };
                Ok(Some(FieldValue::Object(Arc::new(page_info))))
            }
            _ => Ok(None),
        }
    }
}

pub struct BehindAheadCountsObject {
    pub behind: u32,
    pub ahead: u32,
}

impl ObjectValue for BehindAheadCountsObject {
    fn type_name(&self) -> &'static str {
        "BehindAheadCounts"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "behind" => Ok(Some(FieldValue::Json(Json::Int(self.behind as i64)))),
            "ahead" => Ok(Some(FieldValue::Json(Json::Int(self.ahead as i64)))),
            _ => Ok(None),
        }
    }
}

pub struct LanguageStatisticsObject {
    pub language: LanguageStat,
}

impl ObjectValue for LanguageStatisticsObject {
    fn type_name(&self) -> &'static str {
        "LanguageStatistics"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "name" => Ok(Some(FieldValue::Json(Json::Str(
                self.language.name.clone(),
            )))),
            "totalBytes" => Ok(Some(FieldValue::Json(Json::Float(
                self.language.total_bytes as f64,
            )))),
            "totalLines" => Ok(Some(FieldValue::Json(Json::Int(self.language.total_lines)))),
            _ => Ok(None),
        }
    }
}

/// The OSS-reduced ownership connection (enterprise signals are a seam).
pub struct OwnershipConnectionObject {
    pub summary: OwnershipSummary,
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
            "totalCount" => Ok(Some(FieldValue::Json(Json::Int(
                self.summary.total_count as i64,
            )))),
            "totalOwners" => Ok(Some(FieldValue::Json(Json::Int(
                self.summary.total_owners as i64,
            )))),
            "pageInfo" => Ok(Some(FieldValue::Object(Arc::new(PageInfoObject::new(
                false, None,
            ))))),
            "nodes" => Ok(Some(FieldValue::List {
                items: Vec::new(),
                element_type: Some("Ownership"),
            })),
            _ => Ok(None),
        }
    }
}

// ---------------------------------------------------------------------------
// RepositoryComparison
// ---------------------------------------------------------------------------

pub struct RepositoryComparisonObject {
    pub repo: MinimalRepo,
    pub data: ComparisonData,
    pub git: Option<Arc<GitPrefetch>>,
}

impl ObjectValue for RepositoryComparisonObject {
    fn type_name(&self) -> &'static str {
        "RepositoryComparison"
    }

    fn field(
        &self,
        field: &str,
        arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "baseRepository" | "headRepository" => Ok(Some(FieldValue::Object(Arc::new(
                crate::graphql_resolvers::RepositoryObject::from_data(repository_data(&self.repo))
                    .with_git(self.git.clone()),
            )))),
            "range" => Ok(Some(FieldValue::Object(Arc::new(GitRevisionRangeObject {
                repo: self.repo.clone(),
                base_revspec: self.data.base_revspec.clone(),
                head_revspec: self.data.head_revspec.clone(),
                git: self.git.clone(),
            })))),
            "commits" => Ok(Some(FieldValue::Object(Arc::new(CommitConnectionObject {
                repo: self.repo.clone(),
                commits: self
                    .data
                    .commits
                    .clone()
                    .unwrap_or_else(|| Err("commits are not prefetched".to_owned())),
                first: int_arg(arguments, "first"),
                after_cursor: 0,
                git: self.git.clone(),
            })))),
            "fileDiffs" => Ok(Some(FieldValue::Object(Arc::new(
                FileDiffConnectionObject {
                    repo: self.repo.clone(),
                    file_diffs: self
                        .data
                        .file_diffs
                        .clone()
                        .unwrap_or_else(|| Err("fileDiffs are not prefetched".to_owned())),
                    first: int_arg(arguments, "first"),
                    after: arguments.string("after").map(str::to_owned),
                    base_oid: self.data.base_oid.clone(),
                    head_oid: self.data.head_oid.clone(),
                    new_file: crate::git_comparison_graphql::NewFileSource::Tree,
                    git: self.git.clone(),
                },
            )))),
            _ => Ok(None),
        }
    }
}

pub struct GitRevisionRangeObject {
    pub repo: MinimalRepo,
    pub base_revspec: String,
    pub head_revspec: String,
    pub git: Option<Arc<GitPrefetch>>,
}

impl ObjectValue for GitRevisionRangeObject {
    fn type_name(&self) -> &'static str {
        "GitRevisionRange"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "expr" => Ok(Some(FieldValue::Json(Json::Str(format!(
                "{}...{}",
                self.base_revspec, self.head_revspec
            ))))),
            "base" | "baseRevSpec" => {
                Ok(Some(FieldValue::Object(Arc::new(GitRevSpecExprObject {
                    repo: self.repo.clone(),
                    expr: self.base_revspec.clone(),
                    git: self.git.clone(),
                }))))
            }
            "head" | "headRevSpec" => {
                Ok(Some(FieldValue::Object(Arc::new(GitRevSpecExprObject {
                    repo: self.repo.clone(),
                    expr: self.head_revspec.clone(),
                    git: self.git.clone(),
                }))))
            }
            "mergeBase" => Ok(Some(FieldValue::Null)),
            _ => Ok(None),
        }
    }
}

pub struct GitRevSpecExprObject {
    pub repo: MinimalRepo,
    pub expr: String,
    pub git: Option<Arc<GitPrefetch>>,
}

impl ObjectValue for GitRevSpecExprObject {
    fn type_name(&self) -> &'static str {
        "GitRevSpecExpr"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "expr" => Ok(Some(FieldValue::Json(Json::Str(self.expr.clone())))),
            "object" => {
                let Some(git) = &self.git else {
                    return Err(ExecError::new("GitRevSpecExpr.object is not available"));
                };
                match git.revisions.get(&(self.repo.id, self.expr.clone())) {
                    Some(RevisionLookup::Found(oid)) => Ok(Some(FieldValue::Object(Arc::new(
                        GitObjectResolverObject::from_object(
                            self.repo.clone(),
                            sg_git::GitObject {
                                oid: oid.clone(),
                                object_type: sg_git::GitObjectType::Commit,
                            },
                            self.git.clone(),
                        ),
                    )))),
                    Some(RevisionLookup::NotFound) => Err(ExecError::new(format!(
                        "revision not found: {}@{}",
                        self.repo.name, self.expr
                    ))),
                    Some(RevisionLookup::Error(message)) => Err(ExecError::new(message.clone())),
                    None => Err(ExecError::new("GitRevSpecExpr.object is not prefetched")),
                }
            }
            _ => Ok(None),
        }
    }
}

// ---------------------------------------------------------------------------
// FileDiffConnection
// ---------------------------------------------------------------------------

/// Go's `computeRepositoryComparisonDiff` view: the collected prefix (stopping
/// after `first + after` and peeking one more for `hasNextPage`), the parsed
/// `after` offset and the has-next flag.
struct DiffView {
    files: Vec<FileDiff>,
    after_idx: usize,
    has_next_page: bool,
}

fn diff_view(
    files: &[FileDiff],
    first: Option<i32>,
    after: Option<&str>,
) -> Result<DiffView, String> {
    let after_idx = match after {
        None => 0usize,
        Some(text) => {
            let parsed: i64 = text
                .parse()
                .map_err(|_| format!("strconv.ParseInt: parsing {text:?}: invalid syntax"))?;
            parsed.max(0) as usize
        }
    };
    match first {
        None => Ok(DiffView {
            files: files.to_vec(),
            after_idx,
            has_next_page: false,
        }),
        Some(first) => {
            let limit = first.max(0) as usize + after_idx;
            if limit == 0 {
                // Go's loop check `len == first+after` never fires for 0, so
                // the whole stream is collected without a next-page probe.
                return Ok(DiffView {
                    files: files.to_vec(),
                    after_idx,
                    has_next_page: false,
                });
            }
            let collected = if files.len() <= limit {
                files.to_vec()
            } else {
                files[..limit].to_vec()
            };
            Ok(DiffView {
                files: collected,
                after_idx,
                has_next_page: files.len() > limit,
            })
        }
    }
}

pub struct FileDiffConnectionObject {
    pub repo: MinimalRepo,
    pub file_diffs: Result<Vec<FileDiff>, String>,
    pub first: Option<i32>,
    pub after: Option<String>,
    /// `Base` commit OID (`oldFile`), absent for a `DevNullSHA` base.
    pub base_oid: Option<String>,
    /// `Head` commit OID (`newFile`).
    pub head_oid: String,
    /// Head-commit tree entry or preview virtual file.
    pub new_file: crate::git_comparison_graphql::NewFileSource,
    pub git: Option<Arc<GitPrefetch>>,
}

impl ObjectValue for FileDiffConnectionObject {
    fn type_name(&self) -> &'static str {
        "FileDiffConnection"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        let files = self
            .file_diffs
            .as_ref()
            .map_err(|message| ExecError::new(message.clone()))?;
        let view = diff_view(files, self.first, self.after.as_deref()).map_err(ExecError::new)?;
        match field {
            "nodes" => {
                let nodes = if view.after_idx <= view.files.len() {
                    &view.files[view.after_idx..]
                } else {
                    &[]
                };
                Ok(Some(FieldValue::List {
                    items: nodes
                        .iter()
                        .map(|file| {
                            FieldValue::Object(Arc::new(FileDiffObject {
                                file: file.clone(),
                                repo: self.repo.clone(),
                                base_oid: self.base_oid.clone(),
                                head_oid: self.head_oid.clone(),
                                new_file: self.new_file.clone(),
                                git: self.git.clone(),
                            }))
                        })
                        .collect(),
                    element_type: Some("FileDiff"),
                }))
            }
            "totalCount" => {
                if view.has_next_page {
                    Ok(Some(FieldValue::Null))
                } else {
                    Ok(Some(FieldValue::Json(Json::Int(view.files.len() as i64))))
                }
            }
            "pageInfo" => {
                if !view.has_next_page {
                    Ok(Some(FieldValue::Object(Arc::new(PageInfoObject::new(
                        false, None,
                    )))))
                } else {
                    let next = view.after_idx + self.first.unwrap_or(0).max(0) as usize;
                    Ok(Some(FieldValue::Object(Arc::new(PageInfoObject::new(
                        true,
                        Some(next.to_string()),
                    )))))
                }
            }
            "diffStat" => {
                let mut added = 0i64;
                let mut deleted = 0i64;
                for file in &view.files {
                    let (file_added, file_deleted) = file_diff_stat(file);
                    added += file_added;
                    deleted += file_deleted;
                }
                Ok(Some(FieldValue::Object(Arc::new(DiffStatObject {
                    added,
                    deleted,
                }))))
            }
            "rawDiff" => Ok(Some(FieldValue::Json(Json::Str(
                String::from_utf8_lossy(&print_multi_file_diff(&view.files)).into_owned(),
            )))),
            _ => Ok(None),
        }
    }
}

pub struct FileDiffObject {
    pub file: FileDiff,
    pub repo: MinimalRepo,
    pub base_oid: Option<String>,
    pub head_oid: String,
    pub new_file: crate::git_comparison_graphql::NewFileSource,
    pub git: Option<Arc<GitPrefetch>>,
}

impl FileDiffObject {
    fn old_path(&self) -> Option<String> {
        crate::git_comparison_graphql::diff_path(&self.file.orig_name)
    }

    fn new_path(&self) -> Option<String> {
        crate::git_comparison_graphql::diff_path(&self.file.new_name)
    }

    fn old_file_value(&self) -> FieldValue {
        match self.old_path() {
            None => FieldValue::Null,
            Some(path) => {
                FieldValue::Object(Arc::new(crate::git_comparison_graphql::tree_file_object(
                    &self.repo,
                    self.base_oid.as_deref(),
                    &path,
                    self.git.clone(),
                )))
            }
        }
    }

    fn new_file_value(&self) -> FieldValue {
        match self.new_path() {
            None => FieldValue::Null,
            Some(path) => crate::git_comparison_graphql::new_file_value(
                &self.repo,
                &self.new_file,
                &path,
                &self.head_oid,
                self.git.clone(),
            ),
        }
    }

    fn preview(&self) -> Option<&crate::git_comparison_graphql::PreviewData> {
        match &self.new_file {
            crate::git_comparison_graphql::NewFileSource::Preview(preview) => {
                Some(preview.as_ref())
            }
            crate::git_comparison_graphql::NewFileSource::Tree => None,
        }
    }
}

impl ObjectValue for FileDiffObject {
    fn type_name(&self) -> &'static str {
        "FileDiff"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "oldPath" => Ok(Some(path_value(&self.file.orig_name))),
            "oldFile" => Ok(Some(self.old_file_value())),
            "newPath" => Ok(Some(path_value(&self.file.new_name))),
            "newFile" => Ok(Some(self.new_file_value())),
            "mostRelevantFile" => {
                let value = self.new_file_value();
                if matches!(value, FieldValue::Null) {
                    Ok(Some(self.old_file_value()))
                } else {
                    Ok(Some(value))
                }
            }
            "hunks" => {
                let old_path = self.old_path();
                let new_path = self.new_path();
                let preview = self.preview();
                Ok(Some(FieldValue::List {
                    items: self
                        .file
                        .hunks
                        .iter()
                        .map(|hunk| {
                            FieldValue::Object(Arc::new(DiffHunkObject {
                                hunk: hunk.clone(),
                                repo: self.repo.clone(),
                                old_path: old_path.clone(),
                                new_path: new_path.clone(),
                                base_oid: self.base_oid.clone(),
                                head_oid: self.head_oid.clone(),
                                preview: preview.cloned().map(Arc::new),
                                git: self.git.clone(),
                            }))
                        })
                        .collect(),
                    element_type: Some("FileDiffHunk"),
                }))
            }
            "stat" => {
                let (added, deleted) = file_diff_stat(&self.file);
                Ok(Some(FieldValue::Object(Arc::new(DiffStatObject {
                    added,
                    deleted,
                }))))
            }
            "internalID" => {
                let mut hasher = Sha256::new();
                hasher.update(format!(
                    "{}:{}:{}",
                    self.file.orig_name.len(),
                    String::from_utf8_lossy(&self.file.orig_name),
                    String::from_utf8_lossy(&self.file.new_name)
                ));
                let digest = hasher.finalize();
                Ok(Some(FieldValue::Json(Json::Str(
                    digest
                        .iter()
                        .map(|byte| format!("{byte:02x}"))
                        .collect::<String>()[..32]
                        .to_owned(),
                ))))
            }
            _ => Ok(None),
        }
    }
}

fn path_value(path: &[u8]) -> FieldValue {
    if path.is_empty() || path == b"/dev/null" {
        FieldValue::Null
    } else {
        FieldValue::Json(Json::Str(String::from_utf8_lossy(path).into_owned()))
    }
}

pub struct DiffHunkObject {
    pub hunk: Hunk,
    pub repo: MinimalRepo,
    pub old_path: Option<String>,
    pub new_path: Option<String>,
    pub base_oid: Option<String>,
    pub head_oid: String,
    pub preview: Option<Arc<crate::git_comparison_graphql::PreviewData>>,
    pub git: Option<Arc<GitPrefetch>>,
}

impl ObjectValue for DiffHunkObject {
    fn type_name(&self) -> &'static str {
        "FileDiffHunk"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "oldRange" => Ok(Some(FieldValue::Object(Arc::new(DiffHunkRangeObject {
                start_line: self.hunk.orig_start_line,
                lines: self.hunk.orig_lines,
            })))),
            "newRange" => Ok(Some(FieldValue::Object(Arc::new(DiffHunkRangeObject {
                start_line: self.hunk.new_start_line,
                lines: self.hunk.new_lines,
            })))),
            "oldNoNewlineAt" => Ok(Some(FieldValue::Json(Json::Bool(
                self.hunk.orig_no_newline_at != 0,
            )))),
            "section" => {
                if self.hunk.section.is_empty() {
                    Ok(Some(FieldValue::Null))
                } else {
                    Ok(Some(FieldValue::Json(Json::Str(
                        String::from_utf8_lossy(&self.hunk.section).into_owned(),
                    ))))
                }
            }
            "body" => Ok(Some(FieldValue::Json(Json::Str(
                String::from_utf8_lossy(&self.hunk.body).into_owned(),
            )))),
            "highlight" => Ok(Some(FieldValue::Object(Arc::new(
                crate::git_comparison_graphql::hunk_highlight_body(
                    &self.repo,
                    &self.hunk,
                    self.old_path.as_deref(),
                    self.new_path.as_deref(),
                    self.base_oid.as_deref(),
                    Some(&self.head_oid),
                    self.preview.as_deref(),
                    self.git.as_ref(),
                    _arguments,
                )?,
            )))),
            _ => Ok(None),
        }
    }
}

pub struct DiffHunkRangeObject {
    pub start_line: i32,
    pub lines: i32,
}

impl ObjectValue for DiffHunkRangeObject {
    fn type_name(&self) -> &'static str {
        "FileDiffHunkRange"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "startLine" => Ok(Some(FieldValue::Json(Json::Int(self.start_line as i64)))),
            "lines" => Ok(Some(FieldValue::Json(Json::Int(self.lines as i64)))),
            _ => Ok(None),
        }
    }
}

pub struct DiffStatObject {
    pub added: i64,
    pub deleted: i64,
}

impl ObjectValue for DiffStatObject {
    fn type_name(&self) -> &'static str {
        "DiffStat"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "added" => Ok(Some(FieldValue::Json(Json::Int(self.added)))),
            "deleted" => Ok(Some(FieldValue::Json(Json::Int(self.deleted)))),
            _ => Ok(None),
        }
    }
}

// ---------------------------------------------------------------------------
// go-diff Stat / PrintMultiFileDiff
// ---------------------------------------------------------------------------

/// `go-diff` `Hunk.Stat`: `-`/`+` pairs collapse into `changed`.
fn hunk_stat(body: &[u8]) -> (i64, i64, i64) {
    let mut added = 0i64;
    let mut changed = 0i64;
    let mut deleted = 0i64;
    let mut last = 0u8;
    for line in body.split(|&byte| byte == b'\n') {
        if line.is_empty() {
            last = 0;
            continue;
        }
        match line[0] {
            b'-' => {
                if last == b'+' {
                    added -= 1;
                    changed += 1;
                    last = 0;
                } else {
                    deleted += 1;
                    last = b'-';
                }
            }
            b'+' => {
                if last == b'-' {
                    deleted -= 1;
                    changed += 1;
                    last = 0;
                } else {
                    added += 1;
                    last = b'+';
                }
            }
            _ => last = 0,
        }
    }
    (added, changed, deleted)
}

/// `NewDiffStat(FileDiff.Stat())`: `added=Added+Changed`,
/// `deleted=Deleted+Changed`.
fn file_diff_stat(file: &FileDiff) -> (i64, i64) {
    let mut added = 0i64;
    let mut changed = 0i64;
    let mut deleted = 0i64;
    for hunk in &file.hunks {
        let (hunk_added, hunk_changed, hunk_deleted) = hunk_stat(&hunk.body);
        added += hunk_added;
        changed += hunk_changed;
        deleted += hunk_deleted;
    }
    (added + changed, deleted + changed)
}

/// `go-diff.PrintMultiFileDiff`.
fn print_multi_file_diff(files: &[FileDiff]) -> Vec<u8> {
    let mut out = Vec::new();
    for file in files {
        for header in &file.extended {
            out.extend_from_slice(header);
            out.push(b'\n');
        }
        if file.new_name.is_empty() {
            let original = String::from_utf8_lossy(&file.orig_name);
            out.extend_from_slice(
                format!("Only in {}: {}\n", go_dir(&original), go_base(&original)).as_bytes(),
            );
            continue;
        }
        if file.hunks.is_empty() {
            continue;
        }
        out.extend_from_slice(b"--- ");
        out.extend_from_slice(&file.orig_name);
        out.push(b'\n');
        out.extend_from_slice(b"+++ ");
        out.extend_from_slice(&file.new_name);
        out.push(b'\n');
        print_hunks(&mut out, &file.hunks);
    }
    out
}

fn print_hunks(out: &mut Vec<u8>, hunks: &[Hunk]) {
    for hunk in hunks {
        out.extend_from_slice(
            format!(
                "@@ -{},{} +{},{} @@",
                hunk.orig_start_line, hunk.orig_lines, hunk.new_start_line, hunk.new_lines
            )
            .as_bytes(),
        );
        if !hunk.section.is_empty() {
            out.push(b' ');
            out.extend_from_slice(&hunk.section);
        }
        out.push(b'\n');
        if hunk.orig_no_newline_at == 0 {
            out.extend_from_slice(&hunk.body);
        } else {
            let at = hunk.orig_no_newline_at.max(0) as usize;
            let at = at.min(hunk.body.len());
            out.extend_from_slice(&hunk.body[..at]);
            out.extend_from_slice(b"\\ No newline at end of file\n");
            out.extend_from_slice(&hunk.body[at..]);
        }
        if !hunk.body.ends_with(b"\n") {
            out.push(b'\n');
            out.extend_from_slice(b"\\ No newline at end of file\n");
        }
    }
}

fn go_dir(path: &str) -> String {
    match path.rfind('/') {
        None => ".".to_owned(),
        Some(0) => "/".to_owned(),
        Some(index) => path[..index].to_owned(),
    }
}

fn go_base(path: &str) -> &str {
    match path.rfind('/') {
        None => path,
        Some(index) => &path[index + 1..],
    }
}
