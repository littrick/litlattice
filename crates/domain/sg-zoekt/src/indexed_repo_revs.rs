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
//! Not ported (dependencies outside this slice):
//! - `PartitionRepos` / `zoektIndexedRepos`, which need the database and a
//!   `zoekt.Streamer`.
//! - `zoektFileMatchTo*`, which need the frontend result model.

use std::collections::BTreeMap;

use roaring::RoaringBitmap;

use crate::types::{FileMatch, MinimalRepoListEntry};

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
}
