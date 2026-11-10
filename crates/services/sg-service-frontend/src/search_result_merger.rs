//! `internal/search/result/merger.go`: the n-way result merger.
//!
//! The merger tracks every distinct [`Key`] seen from any source and the set
//! of sources that have reported it. [`Merger::add_matches`] returns the
//! matches that just became visible in **all** sources (the n-way
//! intersection, streamed as it completes); [`Merger::unsent_tracked`] returns
//! the tracked matches that were never seen by every source, i.e. the union
//! minus the intersection, ordered by descending source count.
//!
//! The result model is reused from [`crate::search_stream::convert`]. The Go
//! `result.Key` type has no counterpart there, so this module defines a public
//! [`Key`] with the same fields and equality semantics as
//! `internal/search/result/match.go:57-120`.

use std::collections::HashMap;

use crate::search_stream::convert::{
    dedup_symbols, CommitMatch, FileMatch, Match, RepoMatch, ResolvedOwner, SymbolMatch,
};

/// Baseline match ranks (`internal/search/result/match.go:44-50`).
pub mod rank {
    pub const FILE: i32 = 0;
    pub const COMMIT: i32 = 1;
    pub const DIFF: i32 = 2;
    pub const REPO: i32 = 3;
    pub const OWNER: i32 = 4;
}

/// `result.Key` (`internal/search/result/match.go:57-86`).
///
/// Author dates are stored as `(unix nanos, utc offset seconds)`: Go compares
/// `time.Time` values by struct equality for map keys, and encodes instants as
/// nanoseconds for ordering.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Key {
    /// `Key.Repo`.
    pub repo: String,
    /// `Key.Rev`.
    pub rev: String,
    /// `Key.AuthorDate` as Unix nanoseconds (absolute instant).
    pub author_nanos: i128,
    /// `Key.AuthorDate` UTC offset, so structural equality distinguishes
    /// different locations like Go's `time.Time` struct equality.
    pub author_offset: i32,
    /// `Key.Commit`.
    pub commit: String,
    /// `Key.Path`.
    pub path: String,
    /// `Key.OwnerMetadata`.
    pub owner_metadata: String,
    /// `Key.TypeRank`.
    pub type_rank: i32,
}

impl Key {
    /// Build the key for a match, mirroring each `Match.Key()` implementation
    /// (`file.go:214`, `commit.go:153`, `repo.go:60`, `owner.go:81`).
    pub fn from_match(match_: &Match) -> Self {
        match match_ {
            Match::File(fm) => Key {
                repo: fm.file.repo.name.clone(),
                rev: fm.file.input_rev.clone().unwrap_or_default(),
                author_nanos: 0,
                author_offset: 0,
                commit: fm.file.commit_id.clone(),
                path: fm.file.path.clone(),
                owner_metadata: String::new(),
                type_rank: rank::FILE,
            },
            Match::Repo(rm) => Key {
                repo: rm.name.clone(),
                rev: rm.rev.clone(),
                author_nanos: 0,
                author_offset: 0,
                commit: String::new(),
                path: String::new(),
                owner_metadata: String::new(),
                type_rank: rank::REPO,
            },
            Match::Commit(cm) => Key {
                repo: cm.repo.name.clone(),
                rev: String::new(),
                author_nanos: cm.commit.author.date.unix_timestamp_nanos(),
                author_offset: cm.commit.author.date.offset().whole_seconds(),
                commit: cm.commit.id.clone(),
                path: String::new(),
                owner_metadata: String::new(),
                type_rank: if cm.diff_preview.is_some() {
                    rank::DIFF
                } else {
                    rank::COMMIT
                },
            },
            Match::Owner(om) => Key {
                repo: om.repo.name.clone(),
                // `convert::OwnerMatch` does not model `InputRev`/`CommitID`,
                // so those Go key fields are approximated as empty.
                rev: String::new(),
                author_nanos: 0,
                author_offset: 0,
                commit: String::new(),
                path: String::new(),
                owner_metadata: owner_metadata(&om.owner),
                type_rank: rank::OWNER,
            },
        }
    }
}

/// `OwnerMatch.Key`'s `OwnerMetadata = Type() + Identifier()`
/// (`internal/search/result/owner.go:20-40,81-93`).
fn owner_metadata(owner: &ResolvedOwner) -> String {
    match owner {
        ResolvedOwner::Person { handle, email, .. } => {
            format!("personPerson:{handle}{email}")
        }
        ResolvedOwner::Team { name, .. } => format!("teamTeam:{name}"),
    }
}

/// One tracked entry (`mergeVal`, `merger.go:33-37`).
struct MergeVal {
    match_: Match,
    seen: Vec<bool>,
    sent: bool,
}

impl MergeVal {
    fn pop_count(&self) -> usize {
        self.seen.iter().filter(|seen| **seen).count()
    }
}

/// Port of `result.merger` (`internal/search/result/merger.go:20-135`).
pub struct Merger {
    num_sources: usize,
    matches: HashMap<Key, MergeVal>,
}

impl Merger {
    /// `NewMerger`. `num_sources` is the number of result sets; sources are
    /// numbered `[0, num_sources)`.
    pub fn new(num_sources: usize) -> Self {
        Self {
            num_sources,
            matches: HashMap::with_capacity(100),
        }
    }

    /// `Merger.AddMatches`: add one source's matches and return the matches
    /// that are now streamable (seen by every source), in input order.
    pub fn add_matches(&mut self, matches: Vec<Match>, source: usize) -> Vec<Match> {
        let mut streamable = Vec::with_capacity(matches.len());
        for match_ in matches {
            if let Some(streamable_match) = self.add_match(match_, source) {
                streamable.push(streamable_match);
            }
        }
        streamable
    }

    /// `Merger.addMatch` (`merger.go:63-110`).
    fn add_match(&mut self, match_: Match, source: usize) -> Option<Match> {
        let key = Key::from_match(&match_);
        match self.matches.get_mut(&key) {
            Some(prev) if prev.sent => {
                // Already returned as streamable; ignore duplicate keys.
                None
            }
            Some(prev) => {
                // Merge any mergeable types. The types cannot mismatch because
                // the keys match.
                append_matches(&mut prev.match_, &match_);
                set_seen(&mut prev.seen, source);
                if all_seen(&prev.seen) {
                    // Mark the match as returned so it is not returned again.
                    prev.sent = true;
                    Some(prev.match_.clone())
                } else {
                    None
                }
            }
            None => {
                let mut seen = vec![false; self.num_sources];
                set_seen(&mut seen, source);
                self.matches.insert(
                    key,
                    MergeVal {
                        match_,
                        seen,
                        sent: false,
                    },
                );
                None
            }
        }
    }

    /// `Merger.UnsentTracked`: the tracked matches that were never seen by all
    /// sources (union minus intersection), in descending source count.
    ///
    /// The baseline sorts with an unstable comparator, so ties are
    /// nondeterministic; this port uses a stable sort, preserving insertion
    /// order for equal counts.
    pub fn unsent_tracked(&self) -> Vec<Match> {
        let mut values: Vec<&MergeVal> = self.matches.values().filter(|val| !val.sent).collect();
        // Stable sort by descending popcount.
        values.sort_by_key(|val| std::cmp::Reverse(val.pop_count()));
        values.into_iter().map(|val| val.match_.clone()).collect()
    }
}

/// `bitset.BitSet.Set`: setting a bit past the current length grows the set,
/// leaving the newly added bits unset. This matters for out-of-range source
/// numbers, where `All()` must stay false.
fn set_seen(seen: &mut Vec<bool>, source: usize) {
    if source >= seen.len() {
        seen.resize(source + 1, false);
    }
    seen[source] = true;
}

/// `bitset.BitSet.All`: true when every bit is set; vacuously true for an
/// empty bitset.
fn all_seen(seen: &[bool]) -> bool {
    seen.iter().all(|bit| *bit)
}

/// `AppendMatches` (`file.go:169`, `commit.go:145`, `repo.go:54`). The
/// baseline's type assertions cannot fail because the keys match.
fn append_matches(dst: &mut Match, src: &Match) {
    match (dst, src) {
        (Match::File(dst), Match::File(src)) => append_file_matches(dst, src),
        (Match::Commit(dst), Match::Commit(src)) => append_commit_matches(dst, src),
        (Match::Repo(dst), Match::Repo(src)) => append_repo_matches(dst, src),
        // `OwnerMatch` has no `AppendMatches`; mismatched types cannot occur.
        _ => {}
    }
}

/// `FileMatch.AppendMatches` + `FileMatch.mergeSymbols` (`file.go:169-178`).
fn append_file_matches(dst: &mut FileMatch, src: &FileMatch) {
    dst.chunk_matches.extend(src.chunk_matches.iter().cloned());
    dst.symbols.extend(src.symbols.iter().cloned());
    dst.symbols.sort_by(compare_symbol_matches);
    let symbols = std::mem::take(&mut dst.symbols);
    dst.symbols = dedup_symbols(symbols);
    dst.limit_hit = dst.limit_hit || src.limit_hit;
}

/// `CommitMatch.AppendMatches` (`commit.go:145-150`): merge message highlight
/// ranges only; diff highlights are deliberately not merged.
fn append_commit_matches(dst: &mut CommitMatch, src: &CommitMatch) {
    if let (Some(dst), Some(src)) = (&mut dst.message_preview, &src.message_preview) {
        dst.matched_ranges
            .extend(src.matched_ranges.iter().copied());
    }
}

/// `RepoMatch.AppendMatches` (`repo.go:54-58`).
fn append_repo_matches(dst: &mut RepoMatch, src: &RepoMatch) {
    dst.repo_name_matches
        .extend(src.repo_name_matches.iter().copied());
    dst.description_matches
        .extend(src.description_matches.iter().copied());
}

/// `compareSymbolMatches` (`internal/search/result/symbol.go:115-133`).
fn compare_symbol_matches(left: &SymbolMatch, right: &SymbolMatch) -> std::cmp::Ordering {
    left.symbol
        .line
        .cmp(&right.symbol.line)
        .then_with(|| left.symbol.name.cmp(&right.symbol.name))
        .then_with(|| left.symbol.kind.cmp(&right.symbol.kind))
        .then_with(|| left.symbol.parent.cmp(&right.symbol.parent))
        .then_with(|| left.symbol.parent_kind.cmp(&right.symbol.parent_kind))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::search_stream::convert::{ChunkMatch, FileRef, MinimalRepo};
    use crate::search_stream::events::{Location, Range};

    fn file_match(path: &str, lines: &[i64]) -> Match {
        let repo = MinimalRepo {
            id: 1,
            name: "r".to_owned(),
        };
        let file = FileRef {
            repo: repo.clone(),
            commit_id: String::new(),
            path: path.to_owned(),
            input_rev: None,
            precise_language: String::new(),
        };
        let chunk_matches = lines
            .iter()
            .map(|line| ChunkMatch {
                content: String::new(),
                content_start: Location {
                    offset: 0,
                    line: *line,
                    column: 0,
                },
                ranges: vec![Range {
                    start: Location {
                        offset: 0,
                        line: *line,
                        column: 0,
                    },
                    end: Location {
                        offset: 0,
                        line: *line,
                        column: 0,
                    },
                }],
            })
            .collect();
        Match::File(FileMatch {
            file,
            chunk_matches,
            symbols: Vec::new(),
            path_matches: Vec::new(),
            limit_hit: false,
            debug: None,
        })
    }

    #[test]
    fn intersection_requires_every_source() {
        let mut merger = Merger::new(3);
        assert!(merger
            .add_matches(vec![file_match("all", &[1])], 0)
            .is_empty());
        assert!(merger
            .add_matches(vec![file_match("all", &[1])], 1)
            .is_empty());
        let streamed = merger.add_matches(vec![file_match("all", &[1])], 2);
        assert_eq!(streamed.len(), 1);
        assert!(merger.unsent_tracked().is_empty());
    }

    #[test]
    fn unsent_tracked_descending_popcount() {
        let mut merger = Merger::new(3);
        merger.add_matches(
            vec![
                file_match("all", &[1]),
                file_match("2_of_3", &[1]),
                file_match("1_of_3", &[1]),
            ],
            0,
        );
        merger.add_matches(vec![file_match("all", &[1]), file_match("2_of_3", &[1])], 1);
        assert_eq!(
            merger
                .add_matches(vec![file_match("all", &[1])], 2)
                .len(),
            1
        );

        let unsent = merger.unsent_tracked();
        let paths: Vec<String> = unsent
            .iter()
            .map(|match_| match match_ {
                Match::File(fm) => fm.file.path.clone(),
                _ => panic!("unexpected match type"),
            })
            .collect();
        assert_eq!(paths, vec!["2_of_3".to_owned(), "1_of_3".to_owned()]);
    }

    #[test]
    fn duplicate_key_after_sent_is_ignored() {
        let mut merger = Merger::new(2);
        merger.add_matches(vec![file_match("f", &[1])], 0);
        assert_eq!(merger.add_matches(vec![file_match("f", &[1])], 1).len(), 1);
        assert!(merger
            .add_matches(vec![file_match("f", &[1])], 1)
            .is_empty());
    }

    #[test]
    fn first_observation_is_not_streamed() {
        // Even with a single source, the baseline only streams on a repeat
        // observation (the first add creates the tracked entry).
        let mut merger = Merger::new(1);
        assert!(merger
            .add_matches(vec![file_match("f", &[1])], 0)
            .is_empty());
        assert_eq!(merger.add_matches(vec![file_match("f", &[1])], 0).len(), 1);
    }

    #[test]
    fn merged_file_chunks_and_symbols() {
        let mut merger = Merger::new(2);
        merger.add_matches(vec![file_match("f", &[1])], 0);
        let streamed = merger.add_matches(vec![file_match("f", &[2, 3])], 1);
        assert_eq!(streamed.len(), 1);
        match &streamed[0] {
            Match::File(fm) => {
                assert_eq!(fm.chunk_matches.len(), 3);
                assert_eq!(fm.chunk_matches[0].ranges.len(), 1);
            }
            _ => panic!("unexpected match type"),
        }
    }

    #[test]
    fn empty_input_and_zero_sources() {
        let mut merger = Merger::new(0);
        assert!(merger.add_matches(Vec::new(), 0).is_empty());
        assert!(merger.unsent_tracked().is_empty());
    }
}
