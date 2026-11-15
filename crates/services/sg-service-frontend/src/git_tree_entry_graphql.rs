#![forbid(unsafe_code)]

//! GraphQL tree entry / symbols resolvers (M9.8iy).
//!
//! Ports `GitTreeEntryResolver` (`git_tree_entry.go`), `GitCommitResolver`'s
//! `Tree`/`Blob`/`File`/`Path` (`git_commit.go:240-308`) and the symbols
//! resolvers (`symbols.go`, `internal/search/symbol/symbol.go`,
//! `internal/search/result/symbol.go`, `location.go`).
//!
//! `sg-graphql`'s `ObjectValue::field` is synchronous, so the gitserver reads
//! (`Stat`/`ReadDir`/`NewFileReader`) are performed by
//! [`prefetch_commit_entries`] and replayed from [`GitPrefetch`]. The symbols
//! service call stays at execution time because [`SymbolsClient::search`] is
//! itself synchronous (it drives the gRPC call with `run_to_completion`).
//!
//! `GitTree.entries`/`files`/`directories` are ported (M9.43b): their
//! `ReadDir` results are prefetched in [`prefetch_commit_entries`]. M9.44a
//! additionally walks the selection set of the returned nodes and prefetches
//! their content-backed heavy fields (`content`/`byteSize`/`totalLines`/
//! `binary`/`highlight`; `prefetch_listed_entry_fields`).
//!
//! M9.45a adds `history` (`git_tree_history.go`), `languages`
//! (`git_tree_entry.go:297-326`, [`crate::enry_languages`]) and makes `blame`
//! reachable on history nodes (the `GitBlobD` wrapper from M9.19a).
//!
//! Out of scope: `ownership`/`richHTML`/`rawZipArchiveURL` and the
//! zoekt symbol index branch (`search.ListAllIndexed`, not ported — the
//! baseline falls back to the symbols service when it returns nothing).
//! `symbols` is synchronous at execution time via `prefetch.symbols`, so it
//! needs no node-level prefetch. `Location.resource`
//! (`CodeIntelGitBlob`) is ported as a thin wrapper over
//! the same entry resolver; it is only fully resolved for prefetched entries.

use std::sync::Arc;
use std::time::Duration;

use sg_git::FileStat;
use sg_graphql::exec::{
    Actor, Arguments, ExecError, FieldValue, InputValue, ObjectValue, RootField,
};
use sg_graphql::json::Json;

use crate::git_comparison_graphql::{
    compute_highlight, document_to_html, lines_for_ranges, lsif_document_json,
    parse_highlight_args, split_highlighted_rows, HighlightArgsKey, HighlightError, HighlightKey,
    HighlightRequest, HighlightedFile,
};
use crate::git_commit_extra::AncestorsKey;
use crate::git_graphql::{
    BlameKey, BlameLookup, CommitLookup, ContentLookup, DirEntriesLookup, DirLookup,
    GitCommitResolverObject, GitObjectSource, GitPrefetch, TreeEntryLookup, MAX_COMMIT_DEPTH,
};
use crate::graphql_resolvers::{CloneUrlResolver, PageInfoObject, RepositoryObject};
use crate::search_searcher_client::{SymbolsParameters, SymbolsResponse};
use crate::search_stream::convert::{go_url_path, quote_meta, MinimalRepo, Symbol};

/// `symbol.DefaultSymbolLimit` (`internal/search/symbol/symbol.go:31`).
const DEFAULT_SYMBOL_LIMIT: i32 = 100;
/// `Compute`'s server-side deadline (`symbol.go:82`).
const SYMBOLS_SERVER_TIMEOUT: Duration = Duration::from_secs(5);
/// `GetMatchAtLineCharacter`'s over-fetch (`symbol.go:129`).
const SYMBOLS_MATCH_FIRST: i32 = 999_999;
/// `git_tree_entry.go:271-274`: pagination is only supported for plaintext.
const HIGHLIGHT_PAGINATION_ERROR: &str = "pagination is not supported with formats other than HTML_PLAINTEXT, don't set startLine or endLine with other formats";
/// `syntaxHighlightFileBlocklist` (`git_tree_entry.go:264-269`).
const SYNTAX_HIGHLIGHT_FILE_BLOCKLIST: [&str; 3] =
    ["yarn.lock", "pnpm-lock.yaml", "package-lock.json"];

/// `GitTreeEntryResolver` fields that need the blob's full content.
/// (`highlight` reads it through `Content` first, `git_tree_entry.go:277`.)
const CONTENT_BACKED_FIELDS: [&str; 6] = [
    "content",
    "byteSize",
    "totalLines",
    "binary",
    "highlight",
    "richHTML",
];

/// `GitTreeEntryResolver.Highlight`'s pagination guard, evaluated on the raw
/// (pre-blocklist) format exactly like the baseline.
fn highlight_pagination_unsupported(args: &HighlightArgsKey) -> bool {
    (args.start_line.is_some() || args.end_line.is_some()) && args.format != "HTML_PLAINTEXT"
}

/// Apply `syntaxHighlightFileBlocklist`: lock files are forced to plaintext
/// instead of hitting the syntax highlighter.
fn normalized_tree_highlight_args(path: &str, args: &HighlightArgsKey) -> HighlightArgsKey {
    let mut out = args.clone();
    if SYNTAX_HIGHLIGHT_FILE_BLOCKLIST
        .iter()
        .any(|suffix| path.ends_with(suffix))
    {
        out.format = "HTML_PLAINTEXT".to_owned();
    }
    out
}

// ---------------------------------------------------------------------------
// Prefetch
// ---------------------------------------------------------------------------

/// Selections of `field`, including the branches of inline fragments.
fn flattened_selections(field: &RootField) -> Vec<&RootField> {
    let mut out: Vec<&RootField> = field.selections.iter().collect();
    for (_, nested) in &field.type_conditions {
        out.extend(nested.iter());
    }
    out
}

fn selected(field: &RootField, name: &str) -> bool {
    flattened_selections(field)
        .iter()
        .any(|selection| selection.name == name)
}

fn selected_field<'a>(field: &'a RootField, name: &str) -> Option<&'a RootField> {
    flattened_selections(field)
        .into_iter()
        .find(|selection| selection.name == name)
}

/// `path.Join(repoName, submoduleURL)` for the `../` form, else the URL as-is.
fn submodule_clone_url(repo_name: &str, submodule_url: &str) -> String {
    if submodule_url.starts_with("../") {
        go_path_join(&[repo_name, submodule_url])
    } else {
        submodule_url.to_owned()
    }
}

/// `GitTreeEntryResolver.entries`'s `ReadDir` (`git_tree.go:51-72`): a
/// `file does not exist` error is an empty tree, everything else is surfaced.
async fn prefetch_dir_entries(
    source: &dyn GitObjectSource,
    prefetch: &mut GitPrefetch,
    repo: &MinimalRepo,
    oid: &str,
    path: &str,
    recursive: bool,
) {
    let key = (repo.id, oid.to_owned(), path.to_owned(), recursive);
    if prefetch.dir_entries.contains_key(&key) {
        return;
    }
    let lookup = match source
        .read_dir_stats(repo.id, &repo.name, oid, path, recursive)
        .await
    {
        Ok(entries) => DirEntriesLookup::Found(entries),
        Err(error) if error.not_exist || error.message.contains("file does not exist") => {
            DirEntriesLookup::Found(Vec::new())
        }
        Err(error) => DirEntriesLookup::Error(error.message),
    };
    prefetch.dir_entries.insert(key, lookup);
}

// ---------------------------------------------------------------------------
// M9.45a: `history` / `languages` / `blame` prefetch
// ---------------------------------------------------------------------------

/// `GitTreeEntryResolver.History` → `GitCommit.Ancestors`,
/// (`git_tree_history.go:14-26`). Go puts the schema `after` argument into
/// `AncestorsArgs.AfterCursor` (a pagination offset) and sets `Path` to the
/// entry's full path; `follow` is not implemented by the baseline.
pub(crate) fn history_key(
    oid: &str,
    path: &str,
    first: Option<i32>,
    after: Option<String>,
) -> AncestorsKey {
    AncestorsKey {
        oid: oid.to_owned(),
        first,
        query: None,
        path: Some(path.to_owned()),
        follow: false,
        after: None,
        after_cursor: after,
        before: None,
    }
}

/// Prefetch a tree entry `history` connection and the selected node fields.
async fn prefetch_history(
    source: &dyn GitObjectSource,
    prefetch: &mut GitPrefetch,
    repo: &MinimalRepo,
    oid: &str,
    stat: &FileStat,
    history: &RootField,
    depth: usize,
) {
    if depth > MAX_COMMIT_DEPTH {
        return;
    }
    let key = history_key(
        oid,
        &stat.name,
        history.arguments.i64("first").map(|value| value as i32),
        history.arguments.string("after").map(str::to_owned),
    );
    if !prefetch.extra.ancestors.contains_key(&key) {
        let result = match source.commit_extra() {
            Some(extra) => {
                crate::git_commit_extra::compute_ancestors(extra, source, prefetch, repo, oid, &key)
                    .await
            }
            None => Err(
                "history requires the commit log source (not wired for this request)".to_owned(),
            ),
        };
        prefetch.extra.ancestors.insert(key.clone(), result);
    }
    let Ok(commits) = prefetch.extra.ancestors[&key].clone() else {
        return;
    };
    let Some(nodes) = flattened_selections(history)
        .into_iter()
        .find(|field| field.name == "nodes")
    else {
        return;
    };
    let fields = flattened_selections(nodes);
    for commit in &commits {
        // `gitCommitConnectionResolver.Nodes` presets the ancestor commit data.
        prefetch
            .commits
            .entry((repo.id, commit.oid.clone()))
            .or_insert_with(|| CommitLookup::Found(commit.clone()));
        Box::pin(prefetch_history_node(
            source,
            prefetch,
            repo,
            &commit.oid,
            stat,
            &fields,
            depth + 1,
        ))
        .await;
    }
}

/// Prefetch the fields selected on one `history` node. The node reuses the
/// original entry's `stat` and only changes the commit oid.
async fn prefetch_history_node(
    source: &dyn GitObjectSource,
    prefetch: &mut GitPrefetch,
    repo: &MinimalRepo,
    oid: &str,
    stat: &FileStat,
    fields: &[&RootField],
    depth: usize,
) {
    if depth > MAX_COMMIT_DEPTH {
        return;
    }
    let path = stat.name.as_str();
    if !stat.is_dir() {
        let needs_content = fields.iter().any(|field| {
            CONTENT_BACKED_FIELDS.contains(&field.name.as_str()) || field.name == "languages"
        });
        let key = (repo.id, oid.to_owned(), path.to_owned());
        if needs_content && !prefetch.contents.contains_key(&key) {
            let lookup = match source.read_file(repo.id, &repo.name, oid, path).await {
                Ok(bytes) => ContentLookup::Found(bytes),
                Err(error) => ContentLookup::Error(error.message),
            };
            prefetch.contents.insert(key.clone(), lookup);
        }
        for highlight_field in fields.iter().filter(|field| field.name == "highlight") {
            prefetch_entry_highlight(source, prefetch, repo, oid, path, highlight_field).await;
        }
        if let Some(blame) = fields.iter().find(|field| field.name == "blame") {
            prefetch_entry_blame(source, prefetch, repo, oid, path, blame).await;
        }
    } else {
        for listing in fields.iter().filter(|field| {
            matches!(
                field.name.as_str(),
                "entries" | "files" | "directories"
            )
        }) {
            let recursive = listing.arguments.bool("recursive").unwrap_or(false);
            prefetch_dir_entries(source, prefetch, repo, oid, path, recursive).await;
            let kind = match listing.name.as_str() {
                "files" => EntryKind::Files,
                "directories" => EntryKind::Directories,
                _ => EntryKind::All,
            };
            let ancestors =
                kind == EntryKind::All && listing.arguments.bool("ancestors").unwrap_or(false);
            if ancestors && !is_root_path(path) {
                let mut parent = path.to_owned();
                loop {
                    parent = go_path_dir(&parent);
                    prefetch_dir_entries(source, prefetch, repo, oid, &parent, false).await;
                    if is_root_path(&parent) {
                        break;
                    }
                }
            }
            let first = listing.arguments.i64("first").map(|value| value as i32);
            if let Ok(nested) = resolve_listing(
                prefetch, repo.id, oid, path, recursive, ancestors, first, kind,
            ) {
                Box::pin(prefetch_listed_entry_fields(
                    source,
                    prefetch,
                    repo,
                    oid,
                    &nested,
                    listing,
                    depth + 1,
                ))
                .await;
            }
        }
    }
    if let Some(history) = fields.iter().find(|field| field.name == "history") {
        Box::pin(prefetch_history(
            source, prefetch, repo, oid, stat, history, depth + 1,
        ))
        .await;
    }
    if let Some(commit_field) = fields.iter().find(|field| field.name == "commit") {
        Box::pin(crate::git_graphql::prefetch_commit_data(
            source,
            prefetch,
            repo,
            oid,
            &commit_field.selections,
            depth + 1,
        ))
        .await;
    }
}

/// Prefetch one tree-entry `highlight` selection (shared by history nodes).
async fn prefetch_entry_highlight(
    source: &dyn GitObjectSource,
    prefetch: &mut GitPrefetch,
    repo: &MinimalRepo,
    oid: &str,
    path: &str,
    highlight_field: &RootField,
) {
    let raw_args = parse_highlight_args(&highlight_field.arguments);
    let content_key = (repo.id, oid.to_owned(), path.to_owned());
    if !prefetch.contents.contains_key(&content_key) {
        let lookup = match source.read_file(repo.id, &repo.name, oid, path).await {
            Ok(bytes) => ContentLookup::Found(bytes),
            Err(error) => ContentLookup::Error(error.message),
        };
        prefetch.contents.insert(content_key.clone(), lookup);
    }
    let (highlight_key, result) = if highlight_pagination_unsupported(&raw_args) {
        (
            HighlightKey {
                repo_id: repo.id,
                oid: oid.to_owned(),
                path: path.to_owned(),
                args: raw_args.clone(),
            },
            Err(HIGHLIGHT_PAGINATION_ERROR.to_owned()),
        )
    } else {
        let args = normalized_tree_highlight_args(path, &raw_args);
        let highlight_key = HighlightKey {
            repo_id: repo.id,
            oid: oid.to_owned(),
            path: path.to_owned(),
            args: args.clone(),
        };
        let result = match prefetch.contents.get(&content_key) {
            Some(ContentLookup::Found(bytes)) => {
                let request = HighlightRequest {
                    content: bytes.clone(),
                    filepath: path.to_owned(),
                    disable_timeout: args.disable_timeout,
                    highlight_long_lines: args.highlight_long_lines,
                    format: args.format.clone(),
                    start_line: args.start_line,
                    end_line: args.end_line,
                    keep_final_newline: false,
                };
                match compute_highlight(source, &request).await {
                    Ok(highlighted) => Ok(highlighted),
                    Err(HighlightError::Binary) => Err("cannot render binary file".to_owned()),
                    Err(HighlightError::Message(message)) => Err(message),
                }
            }
            Some(ContentLookup::Error(message)) => Err(message.clone()),
            None => Err("tree entry content is not prefetched".to_owned()),
        };
        (highlight_key, result)
    };
    prefetch
        .tree_highlights
        .entry(highlight_key)
        .or_insert(result);
}

/// Prefetch `GitTreeEntryResolver.Blame` for one entry (`git_blob.go:18-56`).
/// The both-or-neither `startLine`/`endLine` validation happens at resolve
/// time in Go, so a mismatched pair is not fetched here.
async fn prefetch_entry_blame(
    source: &dyn GitObjectSource,
    prefetch: &mut GitPrefetch,
    repo: &MinimalRepo,
    oid: &str,
    path: &str,
    blame: &RootField,
) {
    let start_line = blame.arguments.i64("startLine").map(|line| line as i32);
    let end_line = blame.arguments.i64("endLine").map(|line| line as i32);
    if start_line.is_some() != end_line.is_some() {
        return;
    }
    let ignore_whitespace = blame.arguments.bool("ignoreWhitespace").unwrap_or(false);
    let key = BlameKey {
        repo_id: repo.id,
        oid: oid.to_owned(),
        path: path.to_owned(),
        start_line,
        end_line,
        ignore_whitespace,
    };
    if prefetch.blame.contains_key(&key) {
        return;
    }
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
    prefetch.blame.insert(key, lookup);
}

/// Prefetch `Stat`/`NewFileReader`/`ReadDir`/`GetCommit` for every
/// `tree`/`blob`/`file`/`path` field of one commit (`GitCommit.path`).
// The `contains_key`/`insert` pair cannot use `HashMap::entry` because the
// value is produced by an `await` that would need to borrow the map.
#[allow(clippy::map_entry)]
pub async fn prefetch_commit_entries(
    source: &dyn GitObjectSource,
    prefetch: &mut GitPrefetch,
    repo: &MinimalRepo,
    oid: &str,
    selections: &[RootField],
    depth: usize,
) {
    for field in selections
        .iter()
        .filter(|field| matches!(field.name.as_str(), "tree" | "blob" | "file" | "path"))
    {
        let path = field.arguments.string("path").unwrap_or("");
        let key = (repo.id, oid.to_owned(), path.to_owned());
        if !prefetch.tree_entries.contains_key(&key) {
            let lookup = if path.is_empty() {
                // `GitCommitResolver.path`'s root shortcut: no gitserver call.
                TreeEntryLookup::Found(FileStat::synthetic("", true))
            } else {
                match source.stat(repo.id, &repo.name, oid, path).await {
                    Ok(stat) => TreeEntryLookup::Found(stat),
                    Err(error) if error.not_exist => TreeEntryLookup::NotFound,
                    Err(error) => TreeEntryLookup::Error(error.message),
                }
            };
            prefetch.tree_entries.insert(key.clone(), lookup);
        }
        let stat = match prefetch.tree_entries.get(&key) {
            Some(TreeEntryLookup::Found(stat)) => stat.clone(),
            _ => continue,
        };

        if (selected(field, "content")
            || selected(field, "byteSize")
            || selected(field, "totalLines")
            || selected(field, "binary")
            || selected(field, "languages")
            || selected(field, "highlight")
            || selected(field, "richHTML"))
            && !prefetch.contents.contains_key(&key)
        {
            let lookup = match source.read_file(repo.id, &repo.name, oid, path).await {
                Ok(bytes) => ContentLookup::Found(bytes),
                Err(error) => ContentLookup::Error(error.message),
            };
            prefetch.contents.insert(key.clone(), lookup);
        }

        // M9.37c: `GitBlob.highlight` / `GitTreeEntry.highlight`. Content was
        // sliced by `Content(startLine,endLine)` *before* the highlighter sees
        // it (`GitTreeEntryResolver.Highlight`, `git_tree_entry.go:270-300`).
        if !stat.is_dir() {
            if let Some(highlight_field) = selected_field(field, "highlight") {
                let raw_args = parse_highlight_args(&highlight_field.arguments);
                let (highlight_key, result) = if highlight_pagination_unsupported(&raw_args) {
                    let args = raw_args.clone();
                    (
                        HighlightKey {
                            repo_id: repo.id,
                            oid: oid.to_owned(),
                            path: path.to_owned(),
                            args,
                        },
                        Err(HIGHLIGHT_PAGINATION_ERROR.to_owned()),
                    )
                } else {
                    let args = normalized_tree_highlight_args(path, &raw_args);
                    let highlight_key = HighlightKey {
                        repo_id: repo.id,
                        oid: oid.to_owned(),
                        path: path.to_owned(),
                        args: args.clone(),
                    };
                    let result = match prefetch.contents.get(&key) {
                        Some(ContentLookup::Found(bytes)) => {
                            let request = HighlightRequest {
                                content: bytes.clone(),
                                filepath: path.to_owned(),
                                disable_timeout: args.disable_timeout,
                                highlight_long_lines: args.highlight_long_lines,
                                format: args.format.clone(),
                                start_line: args.start_line,
                                end_line: args.end_line,
                                // `highlightContent` uses `highlight.Code`'s
                                // default: drop one final newline.
                                keep_final_newline: false,
                            };
                            match compute_highlight(source, &request).await {
                                Ok(highlighted) => Ok(highlighted),
                                Err(HighlightError::Binary) => {
                                    Err("cannot render binary file".to_owned())
                                }
                                Err(HighlightError::Message(message)) => Err(message),
                            }
                        }
                        Some(ContentLookup::Error(message)) => Err(message.clone()),
                        None => Err("tree entry content is not prefetched".to_owned()),
                    };
                    (highlight_key, result)
                };
                prefetch
                    .tree_highlights
                    .entry(highlight_key)
                    .or_insert(result);
            }
        }

        // M9.43b: `GitTree.entries`/`files`/`directories`. `ObjectValue::field`
        // is synchronous, so every listing (including the ancestor chain) is
        // read here.
        if stat.is_dir() {
            for selection in flattened_selections(field) {
                if !matches!(selection.name.as_str(), "entries" | "files" | "directories") {
                    continue;
                }
                let recursive = selection.arguments.bool("recursive").unwrap_or(false);
                prefetch_dir_entries(source, prefetch, repo, oid, path, recursive).await;
                if selection.name == "entries"
                    && selection.arguments.bool("ancestors").unwrap_or(false)
                    && !is_root_path(path)
                {
                    let mut parent = path.to_owned();
                    loop {
                        parent = go_path_dir(&parent);
                        prefetch_dir_entries(source, prefetch, repo, oid, &parent, false).await;
                        if is_root_path(&parent) {
                            break;
                        }
                    }
                }

                // M9.44a: heavy fields selected on the *entries* nodes
                // (`content`/`highlight`/...). The listing is already
                // prefetched above, so the exact returned set (filter, sort,
                // `first`, `recursive`, `ancestors`) can be reproduced here
                // and each node's content/highlight prefetched. Without this
                // an entry's `content`/`highlight` would reach execution with
                // no prefetched data.
                let kind = match selection.name.as_str() {
                    "files" => EntryKind::Files,
                    "directories" => EntryKind::Directories,
                    _ => EntryKind::All,
                };
                let ancestors = kind == EntryKind::All
                    && selection.arguments.bool("ancestors").unwrap_or(false);
                let first = selection
                    .arguments
                    .i64("first")
                    .map(|value| value as i32);
                if let Ok(listed) = resolve_listing(
                    prefetch, repo.id, oid, path, recursive, ancestors, first, kind,
                ) {
                    Box::pin(prefetch_listed_entry_fields(
                        source, prefetch, repo, oid, &listed, selection, depth + 1,
                    ))
                    .await;
                }
            }
        }

        if stat.is_dir() && selected(field, "isSingleChild") {
            let dir_path = go_path_dir(path);
            let dir_key = (repo.id, oid.to_owned(), dir_path.clone());
            if !prefetch.dirs.contains_key(&dir_key) {
                let lookup = match source
                    .read_dir(repo.id, &repo.name, oid, &dir_path, false)
                    .await
                {
                    Ok(names) => DirLookup::Found(names),
                    Err(error) => DirLookup::Error(error.message),
                };
                prefetch.dirs.insert(dir_key, lookup);
            }
        }

        if let Some(history_field) = selected_field(field, "history") {
            Box::pin(prefetch_history(
                source,
                prefetch,
                repo,
                oid,
                &stat,
                history_field,
                depth + 1,
            ))
            .await;
        }

        if let Some(commit_field) = selected_field(field, "commit") {
            Box::pin(crate::git_graphql::prefetch_commit_data(
                source,
                prefetch,
                repo,
                oid,
                &commit_field.selections,
                depth + 1,
            ))
            .await;
        }

        if let Some(submodule) = &stat.submodule {
            let url = submodule_clone_url(&repo.name, &submodule.url);
            if !prefetch.submodule_urls.contains(&url) {
                prefetch.submodule_urls.push(url);
            }
        }
    }
}

/// Prefetch the content-backed heavy fields selected on the nodes returned by
/// one `entries`/`files`/`directories` listing (M9.44a).
///
/// `entries` is the exact node set [`resolve_listing`] would return, so the
/// prefetch honors the listing's filter (`files`/`directories`), sort order,
/// `first` cap, `recursive` de-nesting and `ancestors` chain. Directories with
/// their own nested listing selection are descended recursively.
#[allow(clippy::too_many_arguments)]
async fn prefetch_listed_entry_fields(
    source: &dyn GitObjectSource,
    prefetch: &mut GitPrefetch,
    repo: &MinimalRepo,
    oid: &str,
    entries: &[FileStat],
    selection: &RootField,
    depth: usize,
) {
    if depth > MAX_COMMIT_DEPTH {
        return;
    }
    // Selections of the element node, including inline fragments (`TreeEntry`
    // is an interface; `files` elements are the concrete `File` type).
    let fields = flattened_selections(selection);
    let wants_content = fields.iter().any(|field| {
        CONTENT_BACKED_FIELDS.contains(&field.name.as_str()) || field.name == "languages"
    });
    let highlight_fields: Vec<&RootField> = fields
        .iter()
        .copied()
        .filter(|field| field.name == "highlight")
        .collect();
    let listing_fields: Vec<&RootField> = fields
        .iter()
        .copied()
        .filter(|field| matches!(field.name.as_str(), "entries" | "files" | "directories"))
        .collect();

    for stat in entries {
        // `stat.name` is the full path relative to the repo root.
        let path = stat.name.as_str();
        if !stat.is_dir() {
            let key = (repo.id, oid.to_owned(), path.to_owned());
            if wants_content && !prefetch.contents.contains_key(&key) {
                let lookup = match source.read_file(repo.id, &repo.name, oid, path).await {
                    Ok(bytes) => ContentLookup::Found(bytes),
                    Err(error) => ContentLookup::Error(error.message),
                };
                prefetch.contents.insert(key.clone(), lookup);
            }
            for highlight_field in &highlight_fields {
                let raw_args = parse_highlight_args(&highlight_field.arguments);
                let (highlight_key, result) = if highlight_pagination_unsupported(&raw_args) {
                    (
                        HighlightKey {
                            repo_id: repo.id,
                            oid: oid.to_owned(),
                            path: path.to_owned(),
                            args: raw_args.clone(),
                        },
                        Err(HIGHLIGHT_PAGINATION_ERROR.to_owned()),
                    )
                } else {
                    let args = normalized_tree_highlight_args(path, &raw_args);
                    let highlight_key = HighlightKey {
                        repo_id: repo.id,
                        oid: oid.to_owned(),
                        path: path.to_owned(),
                        args: args.clone(),
                    };
                    let result = match prefetch.contents.get(&key) {
                        Some(ContentLookup::Found(bytes)) => {
                            let request = HighlightRequest {
                                content: bytes.clone(),
                                filepath: path.to_owned(),
                                disable_timeout: args.disable_timeout,
                                highlight_long_lines: args.highlight_long_lines,
                                format: args.format.clone(),
                                start_line: args.start_line,
                                end_line: args.end_line,
                                keep_final_newline: false,
                            };
                            match compute_highlight(source, &request).await {
                                Ok(highlighted) => Ok(highlighted),
                                Err(HighlightError::Binary) => {
                                    Err("cannot render binary file".to_owned())
                                }
                                Err(HighlightError::Message(message)) => Err(message),
                            }
                        }
                        Some(ContentLookup::Error(message)) => Err(message.clone()),
                        None => Err("tree entry content is not prefetched".to_owned()),
                    };
                    (highlight_key, result)
                };
                prefetch
                    .tree_highlights
                    .entry(highlight_key)
                    .or_insert(result);
            }
            if let Some(blame) = fields.iter().find(|field| field.name == "blame") {
                prefetch_entry_blame(source, prefetch, repo, oid, path, blame).await;
            }
        } else {
            for listing in &listing_fields {
                let recursive = listing.arguments.bool("recursive").unwrap_or(false);
                prefetch_dir_entries(source, prefetch, repo, oid, path, recursive).await;
                let kind = match listing.name.as_str() {
                    "files" => EntryKind::Files,
                    "directories" => EntryKind::Directories,
                    _ => EntryKind::All,
                };
                let ancestors =
                    kind == EntryKind::All && listing.arguments.bool("ancestors").unwrap_or(false);
                if ancestors && !is_root_path(path) {
                    let mut parent = path.to_owned();
                    loop {
                        parent = go_path_dir(&parent);
                        prefetch_dir_entries(source, prefetch, repo, oid, &parent, false).await;
                        if is_root_path(&parent) {
                            break;
                        }
                    }
                }
                let first = listing.arguments.i64("first").map(|value| value as i32);
                if let Ok(nested) = resolve_listing(
                    prefetch, repo.id, oid, path, recursive, ancestors, first, kind,
                ) {
                    Box::pin(prefetch_listed_entry_fields(
                        source, prefetch, repo, oid, &nested, listing, depth + 1,
                    ))
                    .await;
                }
            }
        }
        if let Some(history) = fields.iter().find(|field| field.name == "history") {
            Box::pin(prefetch_history(
                source,
                prefetch,
                repo,
                oid,
                stat,
                history,
                depth + 1,
            ))
            .await;
        }
        if let Some(commit_field) = fields.iter().find(|field| field.name == "commit") {
            Box::pin(crate::git_graphql::prefetch_commit_data(
                source,
                prefetch,
                repo,
                oid,
                &commit_field.selections,
                depth + 1,
            ))
            .await;
        }
    }
}

/// Resolve the `cloneURLToRepoName` lookups collected by
/// [`prefetch_commit_entries`] (baseline `GitTreeEntryResolver.url`).
pub async fn prefetch_submodule_repos(
    prefetch: &mut GitPrefetch,
    resolver: Option<&dyn CloneUrlResolver>,
) {
    let Some(resolver) = resolver else {
        return;
    };
    let urls = prefetch.submodule_urls.clone();
    for url in urls {
        if prefetch.submodule_repo_names.contains_key(&url) {
            continue;
        }
        let name = match resolver.repo_name_by_clone_url(&url).await {
            Ok(Some(name)) => Some(name),
            // The baseline logs and returns an empty `url.URL{}` for both the
            // "no matching code host" and resolver-error cases.
            Ok(None) | Err(_) => None,
        };
        prefetch.submodule_repo_names.insert(url, name);
    }
}

// ---------------------------------------------------------------------------
// Go `path` helpers
// ---------------------------------------------------------------------------

/// Go `path.Clean`.
fn go_path_clean(path: &str) -> String {
    if path.is_empty() {
        return ".".to_owned();
    }
    let rooted = path.starts_with('/');
    let mut parts: Vec<&str> = Vec::new();
    for part in path.split('/') {
        match part {
            "" | "." => {}
            ".." => {
                if parts.last().is_some_and(|last| *last != "..") {
                    parts.pop();
                } else if !rooted {
                    parts.push("..");
                }
            }
            part => parts.push(part),
        }
    }
    let mut out = String::new();
    if rooted {
        out.push('/');
    }
    out.push_str(&parts.join("/"));
    if out.is_empty() {
        ".".to_owned()
    } else {
        out
    }
}

/// Go `path.Join`.
fn go_path_join(parts: &[&str]) -> String {
    let joined = parts
        .iter()
        .filter(|part| !part.is_empty())
        .copied()
        .collect::<Vec<_>>()
        .join("/");
    if joined.is_empty() {
        String::new()
    } else {
        go_path_clean(&joined)
    }
}

/// Go `path.Base`.
fn go_path_base(path: &str) -> String {
    if path.is_empty() {
        return ".".to_owned();
    }
    let trimmed = path.trim_end_matches('/');
    if trimmed.is_empty() {
        return "/".to_owned();
    }
    match trimmed.rfind('/') {
        Some(index) => trimmed[index + 1..].to_owned(),
        None => trimmed.to_owned(),
    }
}

/// Go `path.Dir`.
fn go_path_dir(path: &str) -> String {
    match path.rfind('/') {
        Some(0) => "/".to_owned(),
        Some(index) => go_path_clean(&path[..index]),
        None => ".".to_owned(),
    }
}

/// Go `path.Ext("/a/b.md")` = `".md"`; empty when the base name has no dot.
fn go_path_ext(path: &str) -> String {
    let base = go_path_base(path);
    match base.rfind('.') {
        Some(index) => base[index..].to_owned(),
        None => String::new(),
    }
}

/// `richHTML` (`file.go:34-43`).
///
/// Markdown families render through the byte-compatible goldmark port
/// ([`crate::markdown_graphql::render_markdown`]). `.ipynb` needs Go's
/// `nb`/goldmark-jupyter/synth stack, which is not ported (M9.46b errata);
/// everything else is the empty string.
fn rich_html(content: &str, ext: &str) -> Result<String, String> {
    match ext.to_ascii_lowercase().as_str() {
        ".md" | ".mdown" | ".markdown" | ".markdn" => {
            crate::markdown_graphql::render_markdown(content)
        }
        ".ipynb" => Err("ipynb rendering is not ported".to_owned()),
        _ => Ok(String::new()),
    }
}

// ---------------------------------------------------------------------------
// Content helpers (`pageContent`/`lineCount`/`nthIndex`/`binary.IsBinary`)
// ---------------------------------------------------------------------------

/// Go `pageContent` (`git_tree_entry.go:169-215`), 1-indexed.
///
/// Shared with the offline highlighter: `fileDiffHighlighter` slices the file
/// content through `GitTreeEntryResolver.Content` *before* calling
/// `highlight.CodeAsLines`, so the highlight range and the highlighted lines
/// line up exactly (`repository_comparison.go:491-515`).
pub fn page_content(content: &[u8], start_line: Option<i32>, end_line: Option<i32>) -> &[u8] {
    if start_line.is_none() && end_line.is_none() {
        return content;
    }
    let total_line_count = line_count(content) as i64;

    let mut start_cursor: i64 = 0;
    let mut end_cursor: i64 = total_line_count;
    if let Some(start_line) = start_line {
        start_cursor = if start_line < 1 {
            0
        } else if i64::from(start_line) > total_line_count {
            total_line_count
        } else {
            i64::from(start_line) - 1
        };
    }
    if let Some(end_line) = end_line {
        if end_line > 0 {
            end_cursor = i64::from(end_line);
        }
    }
    if end_cursor < start_cursor {
        return content;
    }

    let mut start = nth_index(content, b'\n', start_cursor);
    if start == -1 {
        start = 0;
    }
    let mut end = nth_index(content, b'\n', end_cursor);
    if end == -1 {
        end = content.len() as i64;
    }
    if end < start {
        return content;
    }
    &content[start as usize..end as usize]
}

/// Go `lineCount`: the final newline does not start a new line.
fn line_count(content: &[u8]) -> usize {
    let newlines = content.iter().filter(|byte| **byte == b'\n').count();
    if !content.is_empty() && content[content.len() - 1] != b'\n' {
        newlines + 1
    } else {
        newlines
    }
}

/// Go `nthIndex`.
fn nth_index(content: &[u8], separator: u8, n: i64) -> i64 {
    if n < 0 || content.is_empty() {
        return -1;
    }
    let mut start = 0usize;
    for _ in 0..n {
        match content[start..].iter().position(|byte| *byte == separator) {
            Some(index) => start += index + 1,
            None => return -1,
        }
    }
    start as i64
}

fn skip_leading_whitespace(data: &[u8]) -> &[u8] {
    let mut start = 0;
    while start < data.len() && matches!(data[start], b'\t' | b'\n' | 0x0c | b'\r' | b' ') {
        start += 1;
    }
    &data[start..]
}

/// Approximation of `http.DetectContentType(content)`'s `text/` results:
/// `binary.IsBinary` (`internal/binary/binary.go:10-18`) asks only whether the
/// detected type starts with `text/`. Invalid UTF-8 with no text signature is
/// `application/octet-stream`.
fn detected_as_text(content: &[u8]) -> bool {
    let head = &content[..content.len().min(512)];
    // BOMs (`DetectContentType`'s BOM table).
    if head.starts_with(&[0xEF, 0xBB, 0xBF])
        || head.starts_with(&[0xFF, 0xFE])
        || head.starts_with(&[0xFE, 0xFF])
    {
        return true;
    }
    let trimmed = skip_leading_whitespace(head);
    let lower: Vec<u8> = trimmed
        .iter()
        .map(|byte| byte.to_ascii_lowercase())
        .collect();
    const TEXT_SIGNATURES: &[&[u8]] = &[
        b"<!doctype html",
        b"<html",
        b"<head",
        b"<script",
        b"<iframe",
        b"<h1",
        b"<div",
        b"<font",
        b"<table",
        b"<a",
        b"<style",
        b"<title",
        b"<b",
        b"<body",
        b"<br",
        b"<p",
        b"<!--",
        b"<?xml",
        b"{\\rtf",
    ];
    TEXT_SIGNATURES
        .iter()
        .any(|signature| lower.starts_with(signature))
}

/// `binary.IsBinary` (`internal/binary/binary.go:10-18`).
fn is_binary(content: &[u8]) -> bool {
    if std::str::from_utf8(content).is_ok() {
        return false;
    }
    !detected_as_text(content)
}

// ---------------------------------------------------------------------------
// URLs (`GitTreeEntryResolver.url`/`urlPath`)
// ---------------------------------------------------------------------------

/// `GitCommitResolver.repoRevURL` as a path (`/repo[@rev]`); when `input_rev`
/// is `None` the OID is used, when `Some("")` the `@rev` suffix is omitted.
fn repo_rev_path(repo_name: &str, oid: &str, input_rev: Option<&str>) -> String {
    let mut path = format!("/{repo_name}");
    match input_rev {
        Some(rev) => {
            if !rev.is_empty() {
                path.push('@');
                path.push_str(rev);
            }
        }
        None => {
            path.push('@');
            path.push_str(oid);
        }
    }
    path
}

/// `GitCommitResolver.canonicalRepoRevURL` as a path.
fn canonical_repo_rev_path(repo_name: &str, oid: &str) -> String {
    format!("/{repo_name}@{oid}")
}

fn is_root_path(path: &str) -> bool {
    matches!(go_path_clean(path).as_str(), "/" | "." | "")
}

fn url_path(prefix: &str, is_root: bool, is_dir: bool, path: &str) -> String {
    if is_root {
        return prefix.to_owned();
    }
    let kind = if is_dir { "tree" } else { "blob" };
    go_path_join(&[prefix, "-", kind, path])
}

// ---------------------------------------------------------------------------
// Symbol helpers (`result.Symbol.Range`, `location.go`)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Position {
    pub line: i32,
    pub character: i32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SymbolRange {
    pub start: Position,
    pub end: Position,
}

/// `result.Symbol.Range()` (`symbol.go:91-97`): 0-based, `Character +
/// len(Name)` (Go `len` = bytes).
fn symbol_range(symbol: &Symbol) -> SymbolRange {
    let line = symbol.line - 1;
    SymbolRange {
        start: Position {
            line,
            character: symbol.character,
        },
        end: Position {
            line,
            character: symbol.character + symbol.name.len() as i32,
        },
    }
}

/// `positionResolver.urlFragment` (`location.go:103-108`).
fn position_url_fragment(position: Position, force_include_character: bool) -> String {
    if !force_include_character && position.character == 0 {
        return format!("{}", position.line + 1);
    }
    format!("{}:{}", position.line + 1, position.character + 1)
}

/// `rangeResolver.urlFragment` (`location.go:83-89`).
fn range_url_fragment(range: SymbolRange) -> String {
    if range.start == range.end {
        return position_url_fragment(range.start, false);
    }
    let has_character = range.start.character != 0 || range.end.character != 0;
    format!(
        "{}-{}",
        position_url_fragment(range.start, has_character),
        position_url_fragment(range.end, has_character)
    )
}

// ---------------------------------------------------------------------------
// Symbols compute (`internal/search/symbol/symbol.go`)
// ---------------------------------------------------------------------------

fn include_patterns(arguments: &Arguments) -> Vec<String> {
    match arguments.get("includePatterns") {
        Some(sg_graphql::exec::InputValue::List(items)) => items
            .iter()
            .filter_map(|item| item.as_str().map(str::to_owned))
            .collect(),
        _ => Vec::new(),
    }
}

fn first_arg(arguments: &Arguments) -> Option<i32> {
    arguments.i64("first").map(|first| first as i32)
}

/// `ZoektSymbolsClient.Compute`'s symbols-service branch (`symbol.go:78-125`).
///
/// `query` is `None` for an absent GraphQL argument and `Some("")` for the
/// internal `GetMatchAtLineCharacter` call; both produce `Query = ""`.
fn compute_symbols(
    prefetch: &GitPrefetch,
    repo: &MinimalRepo,
    commit_id: &str,
    query: Option<&str>,
    first: Option<i32>,
    include_patterns: Vec<String>,
) -> Result<Vec<(Symbol, bool)>, String> {
    let Some(client) = prefetch.symbols.as_ref() else {
        return Err("symbols: no endpoints configured".to_owned());
    };
    let limit = first.unwrap_or(DEFAULT_SYMBOL_LIMIT);
    let params = SymbolsParameters {
        repo: repo.name.clone(),
        commit_id: commit_id.to_owned(),
        query: query.unwrap_or("").to_owned(),
        is_regexp: false,
        is_case_sensitive: false,
        include_patterns,
        exclude_pattern: String::new(),
        include_langs: Vec::new(),
        exclude_langs: Vec::new(),
        // Add 1 so PageInfo.hasNextPage can be determined.
        first: limit + 1,
        timeout: SYMBOLS_SERVER_TIMEOUT,
        deadline: None,
    };
    let SymbolsResponse {
        mut symbols,
        limit_hit: _,
        file_limited,
    } = client.search(&params).map_err(|error| error.message)?;

    // Callers expect 1-indexed lines.
    for symbol in &mut symbols {
        symbol.line += 1;
    }
    Ok(symbols
        .into_iter()
        .enumerate()
        .map(|(index, symbol)| (symbol, file_limited.get(index).copied().unwrap_or(false)))
        .collect())
}

/// `ZoektSymbolsClient.GetMatchAtLineCharacter` (`symbol.go:127-148`).
fn match_at_line_character(
    prefetch: &GitPrefetch,
    repo: &MinimalRepo,
    commit_id: &str,
    file_path: &str,
    line: i32,
    character: i32,
) -> Result<Option<(Symbol, bool)>, String> {
    let symbols = compute_symbols(
        prefetch,
        repo,
        commit_id,
        Some(""),
        Some(SYMBOLS_MATCH_FIRST),
        vec![quote_meta(file_path)],
    )?;
    let mut best: Option<(Symbol, bool)> = None;
    for (symbol, file_limited) in symbols {
        let range = symbol_range(&symbol);
        let is_within_range = line >= range.start.line
            && character >= range.start.character
            && line <= range.end.line
            && character <= range.end.character;
        if is_within_range
            && best
                .as_ref()
                .is_none_or(|current| symbol.name.len() < current.0.name.len())
        {
            best = Some((symbol, file_limited));
        }
    }
    Ok(best)
}

// ---------------------------------------------------------------------------
// Resolvers
// ---------------------------------------------------------------------------

/// Which `GitTreeEntryResolver.entries` filter to apply.
#[derive(Clone, Copy, PartialEq, Eq)]
enum EntryKind {
    All,
    Files,
    Directories,
}

/// `GitTreeEntryResolver.entries`' node set (`git_tree.go:43-128`).
///
/// Shared by execution ([`GitTreeEntryObject::entries_resolve`]) and prefetch
/// ([`prefetch_listed_entry_fields`]) so the heavy-field prefetch sees exactly
/// the nodes the resolver returns. Errors are the resolver-visible message:
/// a missing `ReadDir` prefetch, an errored listing, or the gitserver error.
fn resolve_listing(
    git: &GitPrefetch,
    repo_id: i32,
    oid: &str,
    path: &str,
    recursive: bool,
    ancestors: bool,
    first: Option<i32>,
    kind: EntryKind,
) -> Result<Vec<FileStat>, String> {
    let mut entries = match git.dir_entries(repo_id, oid, path, recursive) {
        Some(DirEntriesLookup::Found(entries)) => entries.clone(),
        Some(DirEntriesLookup::Error(message)) => return Err(message.clone()),
        None => return Err("tree entries are not prefetched".to_owned()),
    };

    // `recursive: true` returns the parent trees too (`ls-tree -r -t`); drop
    // one entry per level of nesting below the root.
    if recursive && !is_root_path(path) {
        let depth = path.trim_matches('/').split('/').count();
        if depth >= entries.len() {
            entries.clear();
        } else {
            entries.drain(..depth);
        }
    }

    entries.sort_by(|left, right| match (left.is_dir(), right.is_dir()) {
        (true, false) => std::cmp::Ordering::Less,
        (false, true) => std::cmp::Ordering::Greater,
        _ => left.name.cmp(&right.name),
    });

    let max_resolvers = match first {
        Some(first) => (entries.len() as i32).min(first).max(0) as usize,
        None => entries.len(),
    };
    let mut resolvers: Vec<FileStat> = Vec::with_capacity(max_resolvers);
    for entry in entries {
        if resolvers.len() >= max_resolvers {
            break;
        }
        let keep = match kind {
            EntryKind::All => true,
            EntryKind::Files => !entry.is_dir(),
            EntryKind::Directories => entry.is_dir(),
        };
        if keep {
            resolvers.push(entry);
        }
    }

    if ancestors && !is_root_path(path) {
        let parent_path = go_path_dir(path.trim_matches('/'));
        // The baseline recurses with `{Ancestors: true}` only: no `first`,
        // no `recursive`, no filter.
        let mut ancestors_entries =
            resolve_listing(git, repo_id, oid, &parent_path, false, true, None, EntryKind::All)?;
        ancestors_entries.extend(resolvers);
        return Ok(ancestors_entries);
    }

    Ok(resolvers)
}

/// `GitTreeEntryResolver` (`git_tree_entry.go:42-53`).
#[derive(Clone)]
pub struct GitTreeEntryObject {
    pub repo: MinimalRepo,
    pub oid: String,
    /// `GitCommitResolver.inputRev` of the containing commit.
    pub input_rev: Option<String>,
    /// `stat`: `Name()` is the full path relative to the repository root.
    pub stat: FileStat,
    pub git: Option<Arc<GitPrefetch>>,
}

impl GitTreeEntryObject {
    fn path(&self) -> &str {
        &self.stat.name
    }

    fn is_root(&self) -> bool {
        is_root_path(self.path())
    }

    fn url(&self) -> String {
        let Some(submodule) = &self.stat.submodule else {
            let prefix = repo_rev_path(&self.repo.name, &self.oid, self.input_rev.as_deref());
            return go_url_path(&url_path(
                &prefix,
                self.is_root(),
                self.stat.is_dir(),
                self.path(),
            ));
        };
        let clone_url = submodule_clone_url(&self.repo.name, &submodule.url);
        let repo_name = self
            .git
            .as_ref()
            .and_then(|git| git.submodule_repo_name(&clone_url));
        match repo_name {
            Some(repo_name) => go_url_path(&format!("/{repo_name}@{}", submodule.commit_sha)),
            // The baseline's empty `&url.URL{}`.
            None => String::new(),
        }
    }

    fn canonical_url(&self) -> String {
        let prefix = canonical_repo_rev_path(&self.repo.name, &self.oid);
        go_url_path(&url_path(
            &prefix,
            self.is_root(),
            self.stat.is_dir(),
            self.path(),
        ))
    }

    fn commit_object(&self) -> GitCommitResolverObject {
        let (commit, commit_error) = match &self.git {
            Some(git) => git.commit_data(self.repo.id, &self.oid),
            None => (None, None),
        };
        GitCommitResolverObject {
            repo: self.repo.clone(),
            oid: self.oid.clone(),
            input_rev: self.input_rev.clone(),
            commit,
            commit_error,
            git: self.git.clone(),
        }
    }

    fn repository_object(&self) -> RepositoryObject {
        RepositoryObject::from_data(crate::git_graphql::repository_data(&self.repo))
            .with_git(self.git.clone())
    }

    /// The prefetched `NewFileReader` bytes (`Content`'s `sync.Once` result).
    fn content_bytes(&self) -> Result<&[u8], ExecError> {
        let Some(git) = &self.git else {
            return Err(ExecError::new(
                "tree entry content requires the gitserver GitSource (not wired for this request)",
            ));
        };
        match git.content(self.repo.id, &self.oid, self.path()) {
            Some(ContentLookup::Found(bytes)) => Ok(bytes),
            Some(ContentLookup::Error(message)) => Err(ExecError::new(message.clone())),
            None => Err(ExecError::new("tree entry content is not prefetched")),
        }
    }

    fn content_field(&self, arguments: &Arguments) -> Result<Option<FieldValue>, ExecError> {
        let start_line = arguments.i64("startLine").map(|line| line as i32);
        let end_line = arguments.i64("endLine").map(|line| line as i32);
        let bytes = self.content_bytes()?;
        let page = page_content(bytes, start_line, end_line);
        Ok(Some(FieldValue::Json(Json::Str(
            String::from_utf8_lossy(page).into_owned(),
        ))))
    }

    fn byte_size_field(&self) -> Result<Option<FieldValue>, ExecError> {
        // `int32(len(...))`: the baseline truncates at 2 GiB.
        Ok(Some(FieldValue::Json(Json::Int(i64::from(
            self.content_bytes()?.len() as i32,
        )))))
    }

    fn total_lines_field(&self) -> Result<Option<FieldValue>, ExecError> {
        let bytes = self.content_bytes()?;
        let lines = if is_binary(bytes) {
            0
        } else {
            line_count(bytes) as i32
        };
        Ok(Some(FieldValue::Json(Json::Int(i64::from(lines)))))
    }

    fn binary_field(&self) -> Result<Option<FieldValue>, ExecError> {
        Ok(Some(FieldValue::Json(Json::Bool(is_binary(
            self.content_bytes()?,
        )))))
    }

    /// `GitTreeEntryResolver.Languages` (`git_tree_entry.go:297-326`).
    ///
    /// Directories resolve to an empty list without reading content. For a
    /// file the detector runs on the prefetched `NewFileReader` bytes; the
    /// content is only required when the filename/extension decision is
    /// ambiguous, so an unreadable file with an unambiguous extension still
    /// resolves exactly like Go.
    fn languages_field(&self) -> Result<Option<FieldValue>, ExecError> {
        if self.stat.is_dir() {
            return Ok(Some(FieldValue::List {
                items: Vec::new(),
                element_type: Some("String"),
            }));
        }
        let name = go_path_base(self.path());
        let content = if crate::enry_languages::needs_content(&name) {
            let Some(git) = &self.git else {
                return Err(ExecError::new(
                    "languages requires the gitserver GitSource (not wired for this request)",
                ));
            };
            match git.content(self.repo.id, &self.oid, self.path()) {
                Some(ContentLookup::Found(bytes)) => Some(bytes.as_slice()),
                Some(ContentLookup::Error(message)) => {
                    return Err(ExecError::new(message.clone()))
                }
                None => return Err(ExecError::new("languages content is not prefetched")),
            }
        } else {
            None
        };
        let languages = crate::enry_languages::get_languages(&name, content);
        Ok(Some(FieldValue::List {
            items: languages
                .into_iter()
                .map(|language| FieldValue::Json(Json::Str(language)))
                .collect(),
            element_type: Some("String"),
        }))
    }

    /// `GitTreeEntryResolver.RichHTML` (`git_tree_entry.go:239-247`,
    /// `file.go:34-43`): page `Content` first, then render by extension.
    fn rich_html_field(&self, arguments: &Arguments) -> Result<Option<FieldValue>, ExecError> {
        let start_line = arguments.i64("startLine").map(|line| line as i32);
        let end_line = arguments.i64("endLine").map(|line| line as i32);
        let bytes = self.content_bytes()?;
        let page = page_content(bytes, start_line, end_line);
        let html = rich_html(
            &String::from_utf8_lossy(page),
            &go_path_ext(self.path()),
        )
        .map_err(ExecError::new)?;
        Ok(Some(FieldValue::Json(Json::Str(html))))
    }

    /// `GitTreeEntryResolver.RawZipArchiveURL` (`git_tree_entry.go:419-425`):
    /// `conf.ExternalURLParsed().ResolveReference(Repository.URL() + "-/raw/" +
    /// Path())` with `format=zip`. The reference path is absolute, so the
    /// external URL's own path is discarded by `ResolveReference` (matching Go).
    fn raw_zip_archive_url(&self) -> String {
        let repo_url = format!("/{}", self.repo.name);
        let joined = go_path_join(&[&repo_url, "-/raw/", self.path()]);
        let external = self
            .git
            .as_ref()
            .map(|git| git.external_url.as_str())
            .unwrap_or("");
        // `conf.ExternalURLParsed`'s cached default when site config is unset.
        let base = if external.is_empty() {
            "http://example.com"
        } else {
            external
        };
        let mut url = url::Url::parse(base)
            .or_else(|_| url::Url::parse("http://example.com"))
            .expect("static default URL parses");
        url.set_path(&joined);
        url.set_query(Some("format=zip"));
        url.to_string()
    }

    /// `GitTreeEntryResolver.Blame` (`git_blob.go:18-56`), reachable from
    /// `GitTree.entries` nodes as well as `GitBlob` (M9.46b).
    fn blame_field(&self, arguments: &Arguments) -> Result<Option<FieldValue>, ExecError> {
        let start_line = arguments.i64("startLine").map(|line| line as i32);
        let end_line = arguments.i64("endLine").map(|line| line as i32);
        if start_line.is_some() != end_line.is_some() {
            return Err(ExecError::new(
                "both startLine and endLine must be specified or neither",
            ));
        }
        let ignore_whitespace = arguments.bool("ignoreWhitespace").unwrap_or(false);
        let Some(git) = &self.git else {
            return Err(ExecError::new(
                "blame requires the gitserver GitSource (not wired for this request)",
            ));
        };
        let key = BlameKey {
            repo_id: self.repo.id,
            oid: self.oid.clone(),
            path: self.path().to_owned(),
            start_line,
            end_line,
            ignore_whitespace,
        };
        match git.blame(&key) {
            Some(BlameLookup::Found(hunks)) => Ok(Some(FieldValue::List {
                items: hunks
                    .iter()
                    .map(|hunk| {
                        FieldValue::Object(Arc::new(crate::graphql_inventory_d::HunkObject {
                            repo: self.repo.clone(),
                            git: self.git.clone(),
                            hunk: hunk.clone(),
                        }))
                    })
                    .collect(),
                element_type: Some("Hunk"),
            })),
            Some(BlameLookup::Error(message)) => Err(ExecError::new(message.clone())),
            None => Err(ExecError::new("blame is not prefetched")),
        }
    }

    /// `GitTreeEntryResolver.History` (`git_tree_history.go:9-26`).
    fn history_field(&self, arguments: &Arguments) -> Result<Option<FieldValue>, ExecError> {
        let Some(git) = &self.git else {
            return Err(ExecError::new(
                "history requires the gitserver GitSource (not wired for this request)",
            ));
        };
        let first = arguments.i64("first").map(|value| value as i32);
        let after = arguments.string("after").map(str::to_owned);
        let key = history_key(&self.oid, self.path(), first, after.clone());
        let commits = match git.extra.ancestors.get(&key) {
            Some(result) => result.clone(),
            None => return Err(ExecError::new("history is not prefetched")),
        };
        let after_cursor = after
            .as_deref()
            .filter(|value| !value.is_empty())
            .and_then(|value| value.parse::<i32>().ok())
            .unwrap_or(0);
        Ok(Some(FieldValue::Object(Arc::new(
            TreeEntryHistoryConnectionObject {
                repo: self.repo.clone(),
                stat: self.stat.clone(),
                commits,
                first,
                after_cursor,
                git: self.git.clone(),
            },
        ))))
    }

    /// `GitTreeEntryResolver.entries`/`files`/`directories`
    /// (`git_tree.go:31-128`).
    fn entries_field(
        &self,
        kind: EntryKind,
        arguments: &Arguments,
    ) -> Result<Option<FieldValue>, ExecError> {
        let first = arguments.i64("first").map(|value| value as i32);
        if first.is_some_and(|first| first < 0) {
            return Err(ExecError::new(
                "invalid argument for first, must be non-negative",
            ));
        }
        let recursive = arguments.bool("recursive").unwrap_or(false);
        let ancestors = kind == EntryKind::All && arguments.bool("ancestors").unwrap_or(false);
        let resolvers = self.entries_resolve(recursive, ancestors, first, kind)?;
        let element_type = match kind {
            EntryKind::All => "TreeEntry",
            EntryKind::Files => "File",
            EntryKind::Directories => "GitTree",
        };
        Ok(Some(FieldValue::List {
            items: resolvers
                .into_iter()
                .map(|entry| {
                    let object: Arc<dyn ObjectValue> = if kind == EntryKind::Files {
                        // `files` is declared `[File!]!` (a concrete schema
                        // type), so `__typename` is "File" in the baseline.
                        Arc::new(FileObject(entry))
                    } else {
                        Arc::new(entry)
                    };
                    FieldValue::Object(object)
                })
                .collect(),
            element_type: Some(element_type),
        }))
    }

    fn entries_resolve(
        &self,
        recursive: bool,
        ancestors: bool,
        first: Option<i32>,
        kind: EntryKind,
    ) -> Result<Vec<GitTreeEntryObject>, ExecError> {
        let Some(git) = &self.git else {
            return Err(ExecError::new(
                "tree entries require the gitserver GitSource (not wired for this request)",
            ));
        };
        let entries = resolve_listing(
            git,
            self.repo.id,
            &self.oid,
            self.path(),
            recursive,
            ancestors,
            first,
            kind,
        )
        .map_err(ExecError::new)?;
        Ok(entries
            .into_iter()
            .map(|entry| GitTreeEntryObject {
                repo: self.repo.clone(),
                oid: self.oid.clone(),
                input_rev: self.input_rev.clone(),
                stat: entry,
                git: self.git.clone(),
            })
            .collect())
    }

    fn is_single_child_field(&self) -> Result<Option<FieldValue>, ExecError> {
        if !self.stat.is_dir() {
            return Ok(Some(FieldValue::Json(Json::Bool(false))));
        }
        let Some(git) = &self.git else {
            return Err(ExecError::new(
                "tree entry isSingleChild requires the gitserver GitSource (not wired for this request)",
            ));
        };
        let dir_path = go_path_dir(self.path());
        match git.dir(self.repo.id, &self.oid, &dir_path) {
            // Baseline `IsSingleChild`: it reads two entries from the *parent*
            // listing and only reports true when both reads return `io.EOF`,
            // i.e. when the listing is empty. (For a real directory the entry
            // itself is in the listing, so the answer is normally false.)
            Some(DirLookup::Found(names)) => {
                Ok(Some(FieldValue::Json(Json::Bool(names.is_empty()))))
            }
            Some(DirLookup::Error(message)) => Err(ExecError::new(message.clone())),
            None => Err(ExecError::new("tree entry isSingleChild is not prefetched")),
        }
    }

    fn symbols_field(&self, arguments: &Arguments) -> Result<Option<FieldValue>, ExecError> {
        let Some(git) = &self.git else {
            return Err(ExecError::new(
                "tree entry symbols requires the gitserver GitSource (not wired for this request)",
            ));
        };
        let symbols = compute_symbols(
            git,
            &self.repo,
            &self.oid,
            arguments.string("query"),
            first_arg(arguments),
            include_patterns(arguments),
        )
        .map_err(ExecError::new)?;
        Ok(Some(FieldValue::Object(Arc::new(SymbolConnectionObject {
            first: first_arg(arguments),
            symbols,
            repo: self.repo.clone(),
            oid: self.oid.clone(),
            input_rev: self.input_rev.clone(),
            git: self.git.clone(),
        }))))
    }

    fn symbol_field(&self, arguments: &Arguments) -> Result<Option<FieldValue>, ExecError> {
        let Some(git) = &self.git else {
            return Err(ExecError::new(
                "tree entry symbol requires the gitserver GitSource (not wired for this request)",
            ));
        };
        let line = arguments.i64("line").unwrap_or_default() as i32;
        let character = arguments.i64("character").unwrap_or_default() as i32;
        let found =
            match_at_line_character(git, &self.repo, &self.oid, self.path(), line, character)
                .map_err(ExecError::new)?;
        Ok(Some(match found {
            Some((symbol, file_limited)) => FieldValue::Object(Arc::new(SymbolObject {
                symbol,
                file_limited,
                repo: self.repo.clone(),
                oid: self.oid.clone(),
                input_rev: self.input_rev.clone(),
                git: self.git.clone(),
            })),
            None => FieldValue::Null,
        }))
    }

    /// `GitTreeEntryResolver.Highlight` / `GitBlob.highlight`
    /// (`git_tree_entry.go:270-300`). The highlighter is asynchronous, so the
    /// actual `highlight.CodeAsLines` ran during prefetch; this only applies
    /// the pagination guard and reads the result.
    fn highlight_field(&self, arguments: &Arguments) -> Result<Option<FieldValue>, ExecError> {
        let raw_args = parse_highlight_args(arguments);
        if highlight_pagination_unsupported(&raw_args) {
            return Err(ExecError::new(HIGHLIGHT_PAGINATION_ERROR));
        }
        let args = normalized_tree_highlight_args(self.path(), &raw_args);
        let Some(git) = &self.git else {
            return Err(ExecError::new(
                "tree entry highlight requires the gitserver GitSource (not wired for this request)",
            ));
        };
        let key = HighlightKey {
            repo_id: self.repo.id,
            oid: self.oid.clone(),
            path: self.path().to_owned(),
            args,
        };
        match git.tree_highlights.get(&key) {
            Some(Ok(file)) => Ok(Some(FieldValue::Object(Arc::new(HighlightedFileObject {
                file: file.clone(),
            })))),
            Some(Err(message)) => Err(ExecError::new(message.clone())),
            None => Err(ExecError::new("tree entry highlight is not prefetched")),
        }
    }
}

impl ObjectValue for GitTreeEntryObject {
    fn type_name(&self) -> &'static str {
        if self.stat.is_dir() {
            "GitTree"
        } else {
            "GitBlob"
        }
    }

    fn field(
        &self,
        field: &str,
        arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "path" => Ok(Some(FieldValue::Json(Json::Str(self.path().to_owned())))),
            "name" => Ok(Some(FieldValue::Json(Json::Str(go_path_base(self.path()))))),
            "isDirectory" => Ok(Some(FieldValue::Json(Json::Bool(self.stat.is_dir())))),
            "isRoot" => Ok(Some(FieldValue::Json(Json::Bool(self.is_root())))),
            "url" => Ok(Some(FieldValue::Json(Json::Str(self.url())))),
            "canonicalURL" => Ok(Some(FieldValue::Json(Json::Str(self.canonical_url())))),
            "externalURLs" => Ok(Some(FieldValue::List {
                items: Vec::new(),
                element_type: Some("ExternalLink"),
            })),
            "submodule" => Ok(Some(match &self.stat.submodule {
                Some(submodule) => FieldValue::Object(Arc::new(SubmoduleObject {
                    url: submodule.url.clone(),
                    commit: submodule.commit_sha.clone(),
                    path: submodule.path.clone(),
                })),
                None => FieldValue::Null,
            })),
            "isSingleChild" => self.is_single_child_field(),
            "entries" => self.entries_field(EntryKind::All, arguments),
            "files" => self.entries_field(EntryKind::Files, arguments),
            "directories" => self.entries_field(EntryKind::Directories, arguments),
            "commit" => Ok(Some(FieldValue::Object(Arc::new(self.commit_object())))),
            "repository" => Ok(Some(FieldValue::Object(Arc::new(self.repository_object())))),
            "content" => self.content_field(arguments),
            "byteSize" => self.byte_size_field(),
            "totalLines" => self.total_lines_field(),
            "binary" => self.binary_field(),
            "highlight" => self.highlight_field(arguments),
            "languages" => self.languages_field(),
            "richHTML" => self.rich_html_field(arguments),
            "rawZipArchiveURL" => Ok(Some(FieldValue::Json(Json::Str(
                self.raw_zip_archive_url(),
            )))),
            // M9.46b: `GitBlob.blame` is reachable on `GitTree.entries` nodes
            // too (Go dispatches the same `GitTreeEntryResolver.Blame`).
            "blame" => self.blame_field(arguments),
            "history" => self.history_field(arguments),
            "symbols" => self.symbols_field(arguments),
            "symbol" => self.symbol_field(arguments),
            _ => Ok(None),
        }
    }
}

/// `treeEntryHistoryConnection` (`git_tree_history.go:28-56`): the nodes are
/// tree entries for each ancestor commit, reusing the original entry's `stat`.
pub struct TreeEntryHistoryConnectionObject {
    pub repo: MinimalRepo,
    pub stat: FileStat,
    pub commits: Result<Vec<sg_git::CommitDetails>, String>,
    pub first: Option<i32>,
    pub after_cursor: i32,
    pub git: Option<Arc<GitPrefetch>>,
}

impl ObjectValue for TreeEntryHistoryConnectionObject {
    fn type_name(&self) -> &'static str {
        "TreeEntryConnection"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        // Every member fails together (`compute()` in `git_commits.go`).
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
                    element_type: Some("TreeEntry"),
                    items: nodes
                        .iter()
                        .map(|commit| {
                            let entry = GitTreeEntryObject {
                                repo: self.repo.clone(),
                                oid: commit.oid.clone(),
                                input_rev: None,
                                stat: self.stat.clone(),
                                git: self.git.clone(),
                            };
                            if self.stat.is_dir() {
                                FieldValue::Object(Arc::new(entry))
                            } else {
                                // `GitBlob` also exposes `blame` (M9.45a).
                                FieldValue::Object(Arc::new(
                                    crate::graphql_inventory_d::GitBlobD {
                                        inner: Arc::new(entry),
                                        repo: self.repo.clone(),
                                        oid: commit.oid.clone(),
                                        path: self.stat.name.clone(),
                                        git: self.git.clone(),
                                    },
                                ))
                            }
                        })
                        .collect(),
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

/// `GitTree.files` elements are declared as the concrete schema type `File`
/// (`schema.graphql:5482-5491`), so `__typename` must be "File" even though
/// the entry is a blob. Every field delegates to the entry resolver.
pub struct FileObject(pub GitTreeEntryObject);

impl ObjectValue for FileObject {
    fn type_name(&self) -> &'static str {
        "File"
    }

    fn field(
        &self,
        field: &str,
        arguments: &Arguments,
        actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        self.0.field(field, arguments, actor)
    }
}

/// `HighlightedFileResolver` (`cmd/frontend/graphqlbackend/highlight.go`).
pub struct HighlightedFileObject {
    pub file: HighlightedFile,
}

/// `lineRanges(ranges: [HighlightLineRange!]!)` argument extraction.
fn parse_line_ranges(arguments: &Arguments) -> Vec<(i32, i32)> {
    match arguments.get("ranges") {
        Some(InputValue::List(items)) => items
            .iter()
            .filter_map(|item| match item {
                InputValue::Object(map) => {
                    let start = map
                        .get("startLine")
                        .and_then(InputValue::as_i64)
                        .unwrap_or(0) as i32;
                    let end = map.get("endLine").and_then(InputValue::as_i64).unwrap_or(0) as i32;
                    Some((start, end))
                }
                _ => None,
            })
            .collect(),
        _ => Vec::new(),
    }
}

impl HighlightedFileObject {
    /// `HighlightedFileResolver.LineRanges` (`graphqlbackend/highlight.go:76`):
    /// if `response.LSIF() != nil` use `LinesForRanges` (SCIP), otherwise
    /// `SplitLineRanges(response.HTML())`. `SplitLineRanges` clamps each range
    /// (`start<0→0`, `end>lines→lines`, `start>end→(0,0)`, in that order) then
    /// returns the rendered `<tr>` rows.
    fn line_ranges_field(&self, arguments: &Arguments) -> Result<Option<FieldValue>, ExecError> {
        let ranges = parse_line_ranges(arguments);
        let line_ranges: Vec<FieldValue> = match &self.file.lsif {
            Some(document) => {
                let code = self.file.code.as_deref().unwrap_or("");
                lines_for_ranges(code, document, &ranges)
                    .map_err(ExecError::new)?
                    .into_iter()
                    .map(|rows| FieldValue::List {
                        items: rows
                            .into_iter()
                            .map(|row| FieldValue::Json(Json::Str(row)))
                            .collect(),
                        element_type: Some("String"),
                    })
                    .collect()
            }
            None => {
                let rows = split_highlighted_rows(self.file.html.as_deref().unwrap_or(""));
                ranges
                    .iter()
                    .map(|&(start_line, end_line)| {
                        let mut start = start_line.max(0);
                        let mut end = end_line;
                        if end > rows.len() as i32 {
                            end = rows.len() as i32;
                        }
                        if start > end {
                            start = 0;
                            end = 0;
                        }
                        let items = rows[start as usize..end as usize]
                            .iter()
                            .map(|row| FieldValue::Json(Json::Str(row.clone())))
                            .collect();
                        FieldValue::List {
                            items,
                            element_type: Some("String"),
                        }
                    })
                    .collect()
            }
        };
        Ok(Some(FieldValue::List {
            items: line_ranges,
            element_type: None,
        }))
    }
}

impl ObjectValue for HighlightedFileObject {
    fn type_name(&self) -> &'static str {
        "HighlightedFile"
    }

    fn field(
        &self,
        field: &str,
        arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            // `HighlightedCode.HTML()`: a non-nil SCIP document renders the
            // table from the code; otherwise the stored syntect/plain HTML.
            "html" => {
                let html = match &self.file.lsif {
                    Some(document) => {
                        document_to_html(self.file.code.as_deref().unwrap_or(""), document)
                    }
                    None => self.file.html.clone().unwrap_or_default(),
                };
                Ok(Some(FieldValue::Json(Json::Str(html))))
            }
            "aborted" => Ok(Some(FieldValue::Json(Json::Bool(self.file.aborted)))),
            // `response == nil` → "{}"; a nil `scip.Document` also fails
            // `jsonpb.Marshal` and yields "{}". An empty document marshals to
            // "{}" as well.
            "lsif" => {
                let text = self
                    .file
                    .lsif
                    .as_ref()
                    .map(lsif_document_json)
                    .unwrap_or_else(|| "{}".to_owned());
                Ok(Some(FieldValue::Json(Json::Str(text))))
            }
            "lineRanges" => self.line_ranges_field(arguments),
            _ => Ok(None),
        }
    }
}

/// `type_name`-covering wrapper for `Location.resource`
/// (`CodeIntelGitBlob!`, `schema.graphql:10466-10483`). Every field is
/// delegated to the tree entry resolver, matching graph-gophers' method-name
/// dispatch for concrete types.
pub struct CodeIntelGitBlobObject(pub GitTreeEntryObject);

impl ObjectValue for CodeIntelGitBlobObject {
    fn type_name(&self) -> &'static str {
        "CodeIntelGitBlob"
    }

    fn field(
        &self,
        field: &str,
        arguments: &Arguments,
        actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        self.0.field(field, arguments, actor)
    }
}

/// `gitSubmoduleResolver` (`git_tree_submodule.go`).
pub struct SubmoduleObject {
    pub url: String,
    pub commit: String,
    pub path: String,
}

impl ObjectValue for SubmoduleObject {
    fn type_name(&self) -> &'static str {
        "Submodule"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "url" => Ok(Some(FieldValue::Json(Json::Str(self.url.clone())))),
            "commit" => Ok(Some(FieldValue::Json(Json::Str(self.commit.clone())))),
            "path" => Ok(Some(FieldValue::Json(Json::Str(self.path.clone())))),
            _ => Ok(None),
        }
    }
}

/// `symbolConnectionResolver` (`symbols.go:94-116`).
pub struct SymbolConnectionObject {
    pub first: Option<i32>,
    pub symbols: Vec<(Symbol, bool)>,
    pub repo: MinimalRepo,
    pub oid: String,
    pub input_rev: Option<String>,
    pub git: Option<Arc<GitPrefetch>>,
}

impl SymbolConnectionObject {
    fn limit(&self) -> i32 {
        self.first.unwrap_or(DEFAULT_SYMBOL_LIMIT)
    }
}

impl ObjectValue for SymbolConnectionObject {
    fn type_name(&self) -> &'static str {
        "SymbolConnection"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "nodes" => {
                let limit = self.limit().max(0) as usize;
                let nodes = self
                    .symbols
                    .iter()
                    .take(limit)
                    .map(|(symbol, file_limited)| {
                        FieldValue::Object(Arc::new(SymbolObject {
                            symbol: symbol.clone(),
                            file_limited: *file_limited,
                            repo: self.repo.clone(),
                            oid: self.oid.clone(),
                            input_rev: self.input_rev.clone(),
                            git: self.git.clone(),
                        }))
                    })
                    .collect();
                Ok(Some(FieldValue::List {
                    items: nodes,
                    element_type: Some("Symbol"),
                }))
            }
            "pageInfo" => Ok(Some(FieldValue::Object(Arc::new(PageInfoObject::new(
                self.symbols.len() as i64 > i64::from(self.limit()),
                None,
            ))))),
            _ => Ok(None),
        }
    }
}

/// `symbolResolver` (`symbols.go:118-161`).
pub struct SymbolObject {
    pub symbol: Symbol,
    pub file_limited: bool,
    pub repo: MinimalRepo,
    pub oid: String,
    pub input_rev: Option<String>,
    pub git: Option<Arc<GitPrefetch>>,
}

impl SymbolObject {
    fn location(&self) -> LocationObject {
        let stat = FileStat::synthetic(&self.symbol.path, false);
        LocationObject {
            resource: GitTreeEntryObject {
                repo: self.repo.clone(),
                oid: self.oid.clone(),
                input_rev: self.input_rev.clone(),
                stat,
                git: self.git.clone(),
            },
            range: symbol_range(&self.symbol),
        }
    }

    fn location_url(&self) -> String {
        let location = self.location();
        format!(
            "{}?L{}",
            location.resource.url(),
            range_url_fragment(location.range)
        )
    }

    fn location_canonical_url(&self) -> String {
        let location = self.location();
        format!(
            "{}?L{}",
            location.resource.canonical_url(),
            range_url_fragment(location.range)
        )
    }
}

impl ObjectValue for SymbolObject {
    fn type_name(&self) -> &'static str {
        "Symbol"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "name" => Ok(Some(FieldValue::Json(Json::Str(self.symbol.name.clone())))),
            "containerName" => Ok(Some(if self.symbol.parent.is_empty() {
                FieldValue::Null
            } else {
                FieldValue::Json(Json::Str(self.symbol.parent.clone()))
            })),
            "kind" => Ok(Some(FieldValue::Json(Json::Str(
                crate::search_graphql::symbol_kind(&self.symbol.kind),
            )))),
            "language" => Ok(Some(FieldValue::Json(Json::Str(
                self.symbol.language.clone(),
            )))),
            "location" => Ok(Some(FieldValue::Object(Arc::new(self.location())))),
            "url" => Ok(Some(FieldValue::Json(Json::Str(self.location_url())))),
            "canonicalURL" => Ok(Some(FieldValue::Json(Json::Str(
                self.location_canonical_url(),
            )))),
            "fileLocal" => Ok(Some(FieldValue::Json(Json::Bool(self.file_limited)))),
            _ => Ok(None),
        }
    }
}

/// `locationResolver` (`location.go:18-60`).
pub struct LocationObject {
    pub resource: GitTreeEntryObject,
    pub range: SymbolRange,
}

impl ObjectValue for LocationObject {
    fn type_name(&self) -> &'static str {
        "Location"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "resource" => Ok(Some(FieldValue::Object(Arc::new(CodeIntelGitBlobObject(
                self.resource.clone(),
            ))))),
            "range" => Ok(Some(FieldValue::Object(Arc::new(RangeObject {
                range: self.range,
            })))),
            "url" => Ok(Some(FieldValue::Json(Json::Str(format!(
                "{}?L{}",
                self.resource.url(),
                range_url_fragment(self.range)
            ))))),
            "canonicalURL" => Ok(Some(FieldValue::Json(Json::Str(format!(
                "{}?L{}",
                self.resource.canonical_url(),
                range_url_fragment(self.range)
            ))))),
            _ => Ok(None),
        }
    }
}

/// `rangeResolver` (`location.go:67-89`).
pub struct RangeObject {
    pub range: SymbolRange,
}

impl ObjectValue for RangeObject {
    fn type_name(&self) -> &'static str {
        "Range"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "start" => Ok(Some(FieldValue::Object(Arc::new(PositionObject {
                position: self.range.start,
            })))),
            "end" => Ok(Some(FieldValue::Object(Arc::new(PositionObject {
                position: self.range.end,
            })))),
            _ => Ok(None),
        }
    }
}

/// `positionResolver` (`location.go:96-108`).
pub struct PositionObject {
    pub position: Position,
}

impl ObjectValue for PositionObject {
    fn type_name(&self) -> &'static str {
        "Position"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "line" => Ok(Some(FieldValue::Json(Json::Int(i64::from(
                self.position.line,
            ))))),
            "character" => Ok(Some(FieldValue::Json(Json::Int(i64::from(
                self.position.character,
            ))))),
            _ => Ok(None),
        }
    }
}

// ---------------------------------------------------------------------------
// `GitCommit.tree`/`blob`/`file`/`path` (`git_commit.go:240-308`)
// ---------------------------------------------------------------------------

/// `GitCommitResolver.path` with its caller's validation.
pub fn commit_tree_entry_field(
    commit: &GitCommitResolverObject,
    field: &str,
    arguments: &Arguments,
) -> Result<Option<FieldValue>, ExecError> {
    let path = arguments.string("path").unwrap_or("");
    let Some(git) = &commit.git else {
        return Err(ExecError::new(
            "GitCommit.path requires the gitserver GitSource (not wired for this request)",
        ));
    };
    match git.tree_entry(commit.repo.id, &commit.oid, path) {
        Some(TreeEntryLookup::Found(stat)) => {
            // The empty-path shortcut returns the root tree *before* running
            // `validate`, so `tree`/`blob`/`file`/`path` all resolve to the
            // root entry (graph-gophers dispatches object-typed fields by
            // method name and never consults `ToGitBlob` here).
            if !path.is_empty() {
                match field {
                    "tree" if !stat.is_dir() => {
                        return Err(ExecError::new(format!("not a directory: {path:?}")));
                    }
                    "blob" | "file" if !(stat.is_regular() || stat.is_symlink()) => {
                        return Err(ExecError::new(format!("not a blob: {path:?}")));
                    }
                    _ => {}
                }
            }
            Ok(Some(FieldValue::Object(Arc::new(GitTreeEntryObject {
                repo: commit.repo.clone(),
                oid: commit.oid.clone(),
                input_rev: commit.input_rev.clone(),
                stat: stat.clone(),
                git: commit.git.clone(),
            }))))
        }
        // `os.IsNotExist`.
        Some(TreeEntryLookup::NotFound) => Ok(Some(FieldValue::Null)),
        Some(TreeEntryLookup::Error(message)) => Err(ExecError::new(message.clone())),
        None => Err(ExecError::new("GitCommit.path is not prefetched")),
    }
}

/// `GitCommitResolver.Symbols` (`symbols.go:59-76`).
pub fn commit_symbols_field(
    commit: &GitCommitResolverObject,
    arguments: &Arguments,
) -> Result<Option<FieldValue>, ExecError> {
    let Some(git) = &commit.git else {
        return Err(ExecError::new(
            "GitCommit.symbols requires the gitserver GitSource (not wired for this request)",
        ));
    };
    let symbols = compute_symbols(
        git,
        &commit.repo,
        &commit.oid,
        arguments.string("query"),
        first_arg(arguments),
        include_patterns(arguments),
    )
    .map_err(ExecError::new)?;
    Ok(Some(FieldValue::Object(Arc::new(SymbolConnectionObject {
        first: first_arg(arguments),
        symbols,
        repo: commit.repo.clone(),
        oid: commit.oid.clone(),
        input_rev: commit.input_rev.clone(),
        git: commit.git.clone(),
    }))))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn go_path_helpers_match_the_standard_library() {
        assert_eq!(go_path_clean(""), ".");
        assert_eq!(go_path_clean("/"), "/");
        assert_eq!(go_path_clean("a//b/./c/../d"), "a/b/d");
        assert_eq!(go_path_clean("../../a"), "../../a");
        assert_eq!(
            go_path_join(&["/repo@oid", "-", "blob", "a/b"]),
            "/repo@oid/-/blob/a/b"
        );
        assert_eq!(go_path_join(&["", ""]), "");
        assert_eq!(go_path_base(""), ".");
        assert_eq!(go_path_base("/"), "/");
        assert_eq!(go_path_base("a/b/"), "b");
        assert_eq!(go_path_dir(""), ".");
        assert_eq!(go_path_dir("a"), ".");
        assert_eq!(go_path_dir("/a"), "/");
        assert_eq!(go_path_dir("a/b/"), "a/b");
    }

    #[test]
    fn page_content_matches_go() {
        let content = b"a\nb\nc\n";
        assert_eq!(page_content(content, None, None), content);
        assert_eq!(page_content(content, Some(2), None), b"b\nc\n");
        assert_eq!(page_content(content, Some(0), Some(2)), b"a\nb\n");
        assert_eq!(page_content(content, Some(3), Some(1)), content);
        assert_eq!(page_content(content, Some(9), None), b"");
        assert_eq!(line_count(content), 3);
        assert_eq!(line_count(b"a\nb"), 2);
        assert_eq!(line_count(b""), 0);
    }

    #[test]
    fn binary_detection_matches_the_text_approximation() {
        assert!(!is_binary(b"plain text\n"));
        assert!(!is_binary("héllo\n".as_bytes()));
        assert!(is_binary(&[0x00, 0x01, 0xFE, 0xED]));
        // Invalid UTF-8 without a text signature is `application/octet-stream`.
        assert!(is_binary(&[0xFF, 0x41]));
    }

    #[test]
    fn symbol_range_and_url_fragment_match_the_baseline() {
        let symbol = Symbol {
            name: "abc".to_owned(),
            path: "p".to_owned(),
            line: 3,
            character: 4,
            kind: "function".to_owned(),
            language: "Go".to_owned(),
            parent: String::new(),
            parent_kind: String::new(),
        };
        let range = symbol_range(&symbol);
        assert_eq!(
            range,
            SymbolRange {
                start: Position {
                    line: 2,
                    character: 4
                },
                end: Position {
                    line: 2,
                    character: 7
                }
            }
        );
        assert_eq!(range_url_fragment(range), "3:5-3:8");
        let zero = SymbolRange {
            start: Position {
                line: 0,
                character: 0,
            },
            end: Position {
                line: 0,
                character: 0,
            },
        };
        assert_eq!(range_url_fragment(zero), "1");
    }

    #[test]
    fn submodule_clone_urls_join_relative_urls() {
        assert_eq!(
            submodule_clone_url("github.com/o/r", "../s"),
            "github.com/o/s"
        );
        assert_eq!(
            submodule_clone_url("github.com/o/r", "https://example.com/s"),
            "https://example.com/s"
        );
    }
}
