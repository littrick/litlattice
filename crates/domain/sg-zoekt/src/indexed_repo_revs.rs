#![forbid(unsafe_code)]

//! Repository/ref attribution for zoekt-indexed search
//! (`internal/search/zoekt/indexed_search.go:36-206`).
//!
//! [`IndexedRepoRevs`] records which of a search's `(repository, revisions)`
//! pairs an index can serve and groups them by branch into roaring bitmaps.
//! The methods ported here are pure data logic: [`IndexedRepoRevs::add`],
//! [`IndexedRepoRevs::branch_repos`],
//! [`IndexedRepoRevs::get_repo_revs_from_branch_repos`] and
//! [`IndexedRepoRevs::get_repo_input_rev`].
//!
//! [`zoekt_indexed_repos`] splits requested revisions into indexed and
//! unindexed halves from a `zoekt.ReposMap`, and [`partition_repos`] ports the
//! `containsRefGlobs` / `index:no` / `index:only` / symbol-filter decisions of
//! `PartitionRepos`, taking the `zoekt.Streamer.List` result from an injected
//! synchronous closure. The baseline's context, timeout, trace and logger
//! side effects are not ported (see the M9.8gx overview, §4).
//!
//! Not ported (dependencies outside this slice):
//! - `zoektFileMatchTo*`, which need the frontend result model.

use std::collections::BTreeMap;
use std::fmt;

use roaring::RoaringBitmap;
use sg_search_query::validate::YesNoOnly;

use crate::query::IndexedRequestType;
use crate::types::{FileMatch, ListResponse, MinimalRepoListEntry};

/// `zoekt.ReposMap`: indexed repo id -> minimal list entry
/// (`ListResponse.repos_map`).
pub type RepoMap = BTreeMap<u32, MinimalRepoListEntry>;

/// Minimal form of `search.RepositoryRevisions`.
///
/// The baseline also carries `types.MinimalRepo`'s `Stars` and `ExternalRepo`;
/// this port only keeps the fields the attribution logic reads (`ID`, `Name`)
/// and `Revs`. `Copy` is replaced by `Clone`.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct RepositoryRevisions {
    pub repo: RepositoryRevisionsRepo,
    pub revs: Vec<String>,
}

/// Minimal form of `types.MinimalRepo` inside [`RepositoryRevisions`].
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct RepositoryRevisionsRepo {
    pub id: u32,
    pub name: String,
}

/// Minimal form of `types.MinimalRepo` returned by
/// [`IndexedRepoRevs::get_repo_input_rev`]. `Stars` and `ExternalRepo` are
/// omitted: callers of this slice only use `ID`/`Name`.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct MinimalRepo {
    pub id: u32,
    pub name: String,
}

/// `zoektquery.BranchRepos` with the bitmap kept in its native form. Wire
/// serialization is the caller's concern (see [`crate::query::BranchRepos`]).
#[derive(Debug, Clone, PartialEq, Default)]
pub struct IndexedBranchRepos {
    pub branch: String,
    pub repos: RoaringBitmap,
}

/// `IndexedRepoRevs`: the Sourcegraph and Zoekt representation of the
/// repository revisions an index can search.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct IndexedRepoRevs {
    /// Sourcegraph-side repo id -> repository revisions that are indexed.
    pub repo_revs: BTreeMap<u32, RepositoryRevisions>,
    /// branch name -> repo ids indexed for that branch.
    pub branch_repos: BTreeMap<String, RoaringBitmap>,
}

impl IndexedRepoRevs {
    /// `(*IndexedRepoRevs).add`.
    ///
    /// Adds `reporev` when its revs are a subset of `repo`'s branches and
    /// returns the revision specifiers that could not be added. A repository
    /// that is already present is treated as if it were not indexed: its revs
    /// are returned verbatim and no state changes.
    pub fn add(
        &mut self,
        reporev: &RepositoryRevisions,
        repo: MinimalRepoListEntry,
    ) -> Vec<String> {
        // A repo should only appear once in revs. However, in case this
        // invariant is broken we treat later revs as if they aren't indexed.
        if self.repo_revs.contains_key(&reporev.repo.id) {
            return reporev.revs.clone();
        }

        let mut unindexed = Vec::new();
        let mut branches = Vec::with_capacity(reporev.revs.len());
        let mut indexed = Vec::with_capacity(reporev.revs.len());

        for input_rev in &reporev.revs {
            // Empty string in Sourcegraph means HEAD.
            let rev = if input_rev.is_empty() {
                "HEAD"
            } else {
                input_rev.as_str()
            };

            let mut found = false;
            for branch in &repo.branches {
                if branch.name == rev {
                    branches.push(branch.name.clone());
                    found = true;
                    break;
                }
                // Check if rev is an abbrev commit SHA.
                if rev.len() >= 4 && branch.version.starts_with(rev) {
                    branches.push(branch.name.clone());
                    found = true;
                    break;
                }
            }

            if found {
                indexed.push(input_rev.clone());
            } else {
                unindexed.push(input_rev.clone());
            }
        }

        // We found indexed branches! Track them.
        if !indexed.is_empty() {
            self.repo_revs.insert(
                reporev.repo.id,
                RepositoryRevisions {
                    repo: reporev.repo.clone(),
                    revs: indexed,
                },
            );
            for branch in branches {
                self.branch_repos
                    .entry(branch)
                    .or_default()
                    .insert(reporev.repo.id);
            }
        }

        unindexed
    }

    /// `(*IndexedRepoRevs).BranchRepos`.
    ///
    /// BTreeMap iteration yields the branches in name order; the baseline's
    /// Go map iteration order is unspecified, so differential tests sort both
    /// sides.
    pub fn branch_repos(&self) -> Vec<IndexedBranchRepos> {
        self.branch_repos
            .iter()
            .map(|(branch, repos)| IndexedBranchRepos {
                branch: branch.clone(),
                repos: repos.clone(),
            })
            .collect()
    }

    /// `query.BranchRepos` wire form of [`Self::branch_repos`], for
    /// `zoektquery.NewAnd(&zoektquery.BranchesRepos{...}, q)` in
    /// `FilterRepoHasFileContent`.
    pub fn branch_repos_query(&self) -> Vec<crate::query::BranchRepos> {
        self.branch_repos()
            .into_iter()
            .map(|branch| branch.to_query_branch_repos())
            .collect()
    }

    /// `(*IndexedRepoRevs).GetRepoRevsFromBranchRepos`.
    ///
    /// Replaces revision values that are not branch names known to zoekt with
    /// the single indexed branch when there is exactly one, else falls back to
    /// HEAD (the empty string).
    pub fn get_repo_revs_from_branch_repos(&self) -> BTreeMap<u32, RepositoryRevisions> {
        let mut repo_revs = BTreeMap::new();

        for (repo_id, repo_rev) in &self.repo_revs {
            let mut updated = repo_rev.clone();

            for rev in &mut updated.revs {
                // Check whether revision should be used as a branch name for
                // zoekt branchRepos queries and replace if not.
                if !rev.is_empty() && !self.branch_repos.contains_key(rev) {
                    if self.branch_repos.len() == 1 {
                        // Use the single branch zoekt returned.
                        if let Some(branch) = self.branch_repos.keys().next() {
                            *rev = branch.clone();
                        }
                    } else {
                        // Multiple branches: fall back to HEAD, identified to
                        // zoekt by the empty string regardless of repo ID.
                        rev.clear();
                    }
                }
            }

            repo_revs.insert(*repo_id, updated);
        }

        repo_revs
    }

    /// `(*IndexedRepoRevs).getRepoInputRev`.
    ///
    /// Returns the repo and input revs associated with `file`.
    pub fn get_repo_input_rev(&self, file: &FileMatch) -> (MinimalRepo, Vec<String>) {
        let Some(repo_rev) = self.repo_revs.get(&file.repository_id) else {
            // We search zoekt by repo ID. It is possible that the name has
            // come out of sync, so fall back to the rev hash to avoid a nil
            // panic.
            let repo = MinimalRepo {
                id: file.repository_id,
                name: file.repository.clone(),
            };
            return (repo, vec![file.version.clone()]);
        };

        // Inverse of `add`: work out the revspec from zoekt's branches.
        //
        // Note: RevSpec is guaranteed to be explicit via zoektIndexedRepos.
        let mut input_revs = Vec::with_capacity(file.branches.len());
        for rev in &repo_rev.revs {
            // We rely on Sourcegraph indexing HEAD as "HEAD" rather than
            // resolving the symref.
            let rev_branch_name = if rev.is_empty() { "HEAD" } else { rev.as_str() };

            if file.branches.iter().any(|branch| branch == rev_branch_name) {
                input_revs.push(rev.clone());
                continue;
            }

            // Check if rev is an abbrev commit SHA.
            if rev.len() >= 4 && file.version.starts_with(rev.as_str()) {
                input_revs.push(rev.clone());
                continue;
            }
        }

        if input_revs.is_empty() {
            // Unexpected, but fall back to file.Version to generate correct
            // links.
            input_revs.push(file.version.clone());
        }

        (
            MinimalRepo {
                id: repo_rev.repo.id,
                name: repo_rev.repo.name.clone(),
            },
            input_revs,
        )
    }
}

impl IndexedBranchRepos {
    /// Convert to the `query.BranchRepos` wire form (portable roaring).
    pub fn to_query_branch_repos(&self) -> crate::query::BranchRepos {
        let mut repos = Vec::new();
        self.repos
            .serialize_into(&mut repos)
            .expect("serializing a roaring bitmap into a Vec cannot fail");
        crate::query::BranchRepos {
            branch: self.branch.clone(),
            repos,
        }
    }
}

/// `zoektIndexedRepos` (`indexed_search.go:559-582`).
///
/// Splits `revs` into the subset whose repository appears in `indexed_set`
/// (and passes `filter`, when given) and the rest. Repositories found in
/// `indexed_set` go through [`IndexedRepoRevs::add`]; revisions that `add`
/// could not attribute are copied into a new [`RepositoryRevisions`] which is
/// appended to the unindexed list. Both outputs preserve the input order of
/// `revs`; `indexed.repo_revs` is keyed by repo id, as in the baseline.
///
/// `filter` is only consulted for repositories present in `indexed_set`,
/// matching the baseline's `!ok || (filter != nil && !filter(repo))` short
/// circuit.
pub fn zoekt_indexed_repos(
    indexed_set: &RepoMap,
    revs: &[RepositoryRevisions],
    filter: Option<&dyn Fn(&MinimalRepoListEntry) -> bool>,
) -> (IndexedRepoRevs, Vec<RepositoryRevisions>) {
    let mut indexed = IndexedRepoRevs::default();
    let mut unindexed = Vec::new();

    for reporev in revs {
        let Some(entry) = indexed_set.get(&reporev.repo.id) else {
            unindexed.push(reporev.clone());
            continue;
        };

        if filter.is_some_and(|filter| !filter(entry)) {
            unindexed.push(reporev.clone());
            continue;
        }

        let unindexed_revs = indexed.add(reporev, entry.clone());
        if !unindexed_revs.is_empty() {
            unindexed.push(RepositoryRevisions {
                repo: reporev.repo.clone(),
                revs: unindexed_revs,
            });
        }
    }

    (indexed, unindexed)
}

/// Errors [`partition_repos`] can return.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PartitionError {
    /// `index:only` was requested but `zoekt.List` failed
    /// (`internal/search/zoekt/indexed_search.go:240`).
    IndexOnlyUnavailable,
}

impl fmt::Display for PartitionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::IndexOnlyUnavailable => {
                f.write_str("index:only failed since indexed search is not available yet")
            }
        }
    }
}

impl std::error::Error for PartitionError {}

/// Portable part of `PartitionRepos` (`indexed_search.go:207-285`).
///
/// Returns the indexed and unindexed halves of `repos`:
///
/// 1. `contains_ref_globs` or `use_index == No`: no indexed search, every
///    repository is returned unindexed and `list` is not called.
/// 2. Otherwise `list` supplies the `zoekt.Streamer.List` result for
///    `RepoListFieldReposMap`; a `SymbolRequest` only keeps entries with
///    `has_symbols`.
/// 3. `use_index == Only` clears the unindexed half.
///
/// List failure follows the baseline: with `use_index == Only` this returns
/// [`PartitionError::IndexOnlyUnavailable`] (baseline text above); otherwise
/// it falls back to an empty indexed set plus every repository unindexed,
/// **without** an error. That asymmetry is intentional: the baseline returns
/// `ctx.Err()`, which is nil for a plain list error and only non-nil when the
/// caller's context is cancelled or the one-minute deadline expires (the
/// baseline then returns that context error and skips the `index:only` text).
/// This port does not take a context, so the list error is treated as the
/// non-cancelled case and the baseline's `logger.Warn` is not emitted; wrap
/// `list` to observe its error if needed.
///
/// `YesNoOnly::Invalid` takes the same path as `Yes`, as in the baseline
/// (only `No` and `Only` are special-cased).
pub fn partition_repos<E>(
    repos: &[RepositoryRevisions],
    typ: IndexedRequestType,
    use_index: YesNoOnly,
    contains_ref_globs: bool,
    mut list: impl FnMut() -> Result<ListResponse, E>,
) -> Result<(IndexedRepoRevs, Vec<RepositoryRevisions>), PartitionError> {
    // Fallback to unindexed if the query contains valid ref-globs or index:no.
    if contains_ref_globs || use_index == YesNoOnly::No {
        return Ok((IndexedRepoRevs::default(), repos.to_vec()));
    }

    let has_symbols = |entry: &MinimalRepoListEntry| entry.has_symbols;
    let filter: Option<&dyn Fn(&MinimalRepoListEntry) -> bool> =
        if typ == IndexedRequestType::Symbol {
            Some(&has_symbols)
        } else {
            None
        };

    let response = match list() {
        Ok(response) => response,
        Err(_) => {
            // Only hard fail if the user specified index:only.
            if use_index == YesNoOnly::Only {
                return Err(PartitionError::IndexOnlyUnavailable);
            }
            return Ok((IndexedRepoRevs::default(), repos.to_vec()));
        }
    };

    // Note: `list.Crashes` is not handled since unavailable repositories fall
    // back to unindexed search.
    let (indexed, mut unindexed) = zoekt_indexed_repos(&response.repos_map, repos, filter);

    // Disable unindexed search.
    if use_index == YesNoOnly::Only {
        unindexed.clear();
    }

    Ok((indexed, unindexed))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::RepositoryBranch;

    fn owner(id: u32, name: &str, revs: &[&str]) -> RepositoryRevisions {
        RepositoryRevisions {
            repo: RepositoryRevisionsRepo {
                id,
                name: name.to_owned(),
            },
            revs: revs.iter().map(|rev| (*rev).to_owned()).collect(),
        }
    }

    fn entry(branches: &[(&str, &str)]) -> MinimalRepoListEntry {
        MinimalRepoListEntry {
            branches: branches
                .iter()
                .map(|(name, version)| RepositoryBranch {
                    name: (*name).to_owned(),
                    version: (*version).to_owned(),
                })
                .collect(),
            ..MinimalRepoListEntry::default()
        }
    }

    fn file(id: u32, name: &str, version: &str, branches: &[&str]) -> FileMatch {
        FileMatch {
            repository_id: id,
            repository: name.to_owned(),
            version: version.to_owned(),
            branches: branches.iter().map(|b| (*b).to_owned()).collect(),
            ..FileMatch::default()
        }
    }

    #[test]
    fn empty_rev_matches_head_branch() {
        let mut indexed = IndexedRepoRevs::default();
        let unindexed = indexed.add(&owner(1, "r", &[""]), entry(&[("HEAD", "abc123")]));
        assert!(unindexed.is_empty());
        assert_eq!(indexed.repo_revs[&1].revs, vec!["".to_owned()]);
        assert_eq!(
            indexed.branch_repos["HEAD"].iter().collect::<Vec<_>>(),
            vec![1]
        );
    }

    #[test]
    fn abbrev_sha_needs_four_characters() {
        let mut indexed = IndexedRepoRevs::default();
        let unindexed = indexed.add(
            &owner(1, "r", &["abcd", "abc"]),
            entry(&[("main", "abcdef123456")]),
        );
        assert_eq!(unindexed, vec!["abc".to_owned()]);
        assert_eq!(indexed.repo_revs[&1].revs, vec!["abcd".to_owned()]);
    }

    #[test]
    fn duplicate_repo_is_returned_verbatim() {
        let mut indexed = IndexedRepoRevs::default();
        assert!(indexed
            .add(&owner(1, "r", &["main"]), entry(&[("main", "s1")]))
            .is_empty());
        let unindexed = indexed.add(
            &owner(1, "r", &["main", "dev"]),
            entry(&[("main", "s2"), ("dev", "s3")]),
        );
        assert_eq!(unindexed, vec!["main".to_owned(), "dev".to_owned()]);
        assert_eq!(indexed.repo_revs[&1].revs, vec!["main".to_owned()]);
        assert!(!indexed.branch_repos.contains_key("dev"));
    }

    #[test]
    fn single_branch_falls_back_and_multiple_use_head() {
        let mut single = IndexedRepoRevs::default();
        single.add(&owner(1, "r", &["abcd"]), entry(&[("main", "abcdef")]));
        let updated = single.get_repo_revs_from_branch_repos();
        assert_eq!(updated[&1].revs, vec!["main".to_owned()]);

        // Two distinct indexed branches are needed: `add` only records the
        // branch that actually matched a rev.
        let mut multiple = IndexedRepoRevs::default();
        multiple.add(&owner(2, "r2", &["abcd"]), entry(&[("main", "abcdef")]));
        multiple.add(&owner(3, "r3", &["dev"]), entry(&[("dev", "999999")]));
        let updated = multiple.get_repo_revs_from_branch_repos();
        assert_eq!(updated[&2].revs, vec!["".to_owned()]);
        assert_eq!(updated[&3].revs, vec!["dev".to_owned()]);
    }

    #[test]
    fn get_repo_input_rev_falls_back_to_file_version() {
        let mut indexed = IndexedRepoRevs::default();
        indexed.add(&owner(1, "r", &["main"]), entry(&[("main", "s")]));

        let (repo, revs) = indexed.get_repo_input_rev(&file(1, "r", "unrelated", &[]));
        assert_eq!(
            repo,
            MinimalRepo {
                id: 1,
                name: "r".to_owned()
            }
        );
        assert_eq!(revs, vec!["unrelated".to_owned()]);

        let (repo, revs) = indexed.get_repo_input_rev(&file(9, "other", "v9", &["HEAD"]));
        assert_eq!(
            repo,
            MinimalRepo {
                id: 9,
                name: "other".to_owned()
            }
        );
        assert_eq!(revs, vec!["v9".to_owned()]);
    }

    fn minimal_entry(
        id: u32,
        has_symbols: bool,
        branches: &[(&str, &str)],
    ) -> (u32, MinimalRepoListEntry) {
        (
            id,
            MinimalRepoListEntry {
                has_symbols,
                branches: branches
                    .iter()
                    .map(|(name, version)| RepositoryBranch {
                        name: (*name).to_owned(),
                        version: (*version).to_owned(),
                    })
                    .collect(),
                ..MinimalRepoListEntry::default()
            },
        )
    }

    #[test]
    fn zoekt_indexed_repos_splits_partial_and_missing() {
        let indexed_set: RepoMap = [
            minimal_entry(1, false, &[("main", "s1")]),
            minimal_entry(3, false, &[("dev", "s3")]),
        ]
        .into_iter()
        .collect();
        let revs = vec![
            owner(1, "one", &["main", "dev"]),
            owner(2, "two", &["main"]),
            owner(3, "three", &["dev"]),
        ];

        let (indexed, unindexed) = zoekt_indexed_repos(&indexed_set, &revs, None);

        assert_eq!(indexed.repo_revs[&1].revs, vec!["main".to_owned()]);
        assert_eq!(indexed.repo_revs[&3].revs, vec!["dev".to_owned()]);
        assert!(!indexed.repo_revs.contains_key(&2));
        assert_eq!(
            indexed.branch_repos["main"].iter().collect::<Vec<_>>(),
            vec![1]
        );
        // Partial repo is copied with only its unindexed revs, in input order.
        assert_eq!(
            unindexed,
            vec![owner(1, "one", &["dev"]), owner(2, "two", &["main"])]
        );
    }

    #[test]
    fn zoekt_indexed_repos_filter_applies_only_on_hits() {
        let indexed_set: RepoMap = [
            minimal_entry(1, false, &[("main", "s1")]),
            minimal_entry(2, true, &[("main", "s2")]),
        ]
        .into_iter()
        .collect();
        let revs = vec![
            owner(1, "no-symbols", &["main"]),
            owner(2, "symbols", &["main"]),
            owner(3, "missing", &["main"]),
        ];
        let filter = |entry: &MinimalRepoListEntry| entry.has_symbols;

        let (indexed, unindexed) = zoekt_indexed_repos(&indexed_set, &revs, Some(&filter));

        assert_eq!(
            indexed.repo_revs.keys().copied().collect::<Vec<_>>(),
            vec![2]
        );
        assert_eq!(
            unindexed,
            vec![
                owner(1, "no-symbols", &["main"]),
                owner(3, "missing", &["main"])
            ]
        );
    }

    #[test]
    fn zoekt_indexed_repos_duplicate_repo_is_unindexed_copy() {
        let indexed_set: RepoMap = [minimal_entry(1, false, &[("main", "s1")])]
            .into_iter()
            .collect();
        let revs = vec![owner(1, "one", &["main"]), owner(1, "one", &["main"])];

        let (indexed, unindexed) = zoekt_indexed_repos(&indexed_set, &revs, None);

        assert_eq!(indexed.repo_revs[&1].revs, vec!["main".to_owned()]);
        assert_eq!(unindexed, vec![owner(1, "one", &["main"])]);
    }

    #[test]
    fn partition_repos_fallbacks_skip_list() {
        for (contains_ref_globs, use_index, typ) in [
            (true, YesNoOnly::Yes, IndexedRequestType::Text),
            (false, YesNoOnly::No, IndexedRequestType::Symbol),
        ] {
            let mut called = false;
            let repos = vec![owner(1, "one", &["main"])];
            let (indexed, unindexed) = partition_repos(
                &repos,
                typ,
                use_index,
                contains_ref_globs,
                || -> Result<ListResponse, &'static str> {
                    called = true;
                    Ok(ListResponse::default())
                },
            )
            .expect("fallback does not fail");

            assert!(!called);
            assert_eq!(indexed, IndexedRepoRevs::default());
            assert_eq!(unindexed, repos);
        }
    }

    #[test]
    fn partition_repos_only_clears_unindexed() {
        let repos = vec![owner(1, "one", &["main"]), owner(2, "missing", &["main"])];
        let response = ListResponse {
            repos_map: [minimal_entry(1, false, &[("main", "s1")])]
                .into_iter()
                .collect(),
            ..ListResponse::default()
        };

        let (indexed, unindexed) = partition_repos(
            &repos,
            IndexedRequestType::Text,
            YesNoOnly::Only,
            false,
            || Ok::<_, &'static str>(response.clone()),
        )
        .expect("list succeeded");

        assert_eq!(indexed.repo_revs[&1].revs, vec!["main".to_owned()]);
        assert!(unindexed.is_empty());
    }

    #[test]
    fn partition_repos_only_list_failure_errors() {
        let repos = vec![owner(1, "one", &["main"])];
        let err = partition_repos(
            &repos,
            IndexedRequestType::Text,
            YesNoOnly::Only,
            false,
            || Err::<ListResponse, _>("boom"),
        )
        .expect_err("index:only must hard fail");

        assert_eq!(err, PartitionError::IndexOnlyUnavailable);
        assert_eq!(
            err.to_string(),
            "index:only failed since indexed search is not available yet"
        );
    }

    #[test]
    fn partition_repos_non_only_list_failure_falls_back_silently() {
        let repos = vec![owner(1, "one", &["main"])];
        let (indexed, unindexed) = partition_repos(
            &repos,
            IndexedRequestType::Text,
            YesNoOnly::Yes,
            false,
            || Err::<ListResponse, _>("boom"),
        )
        .expect("baseline returns ctx.Err(), nil for a plain list error");

        assert_eq!(indexed, IndexedRepoRevs::default());
        assert_eq!(unindexed, repos);
    }

    #[test]
    fn partition_repos_symbol_request_filters_has_symbols() {
        let repos = vec![
            owner(1, "no-symbols", &["main"]),
            owner(2, "symbols", &["main"]),
        ];
        let response = ListResponse {
            repos_map: [
                minimal_entry(1, false, &[("main", "s1")]),
                minimal_entry(2, true, &[("main", "s2")]),
            ]
            .into_iter()
            .collect(),
            ..ListResponse::default()
        };

        let (indexed, unindexed) = partition_repos(
            &repos,
            IndexedRequestType::Symbol,
            YesNoOnly::Yes,
            false,
            || Ok::<_, &'static str>(response.clone()),
        )
        .expect("list succeeded");

        assert_eq!(
            indexed.repo_revs.keys().copied().collect::<Vec<_>>(),
            vec![2]
        );
        assert_eq!(unindexed, vec![owner(1, "no-symbols", &["main"])]);
    }
}
