#![forbid(unsafe_code)]

//! File ranking for zoekt search results.
//!
//! Mirrors `github.com/sourcegraph/zoekt`'s `SortFiles` and
//! `boostNovelExtension` (`contentprovider.go`). The aggregator calls
//! [`sort_files`] after collecting results and before truncating to
//! `MaxDocDisplayCount`.

use std::cmp::Ordering;

use crate::types::FileMatch;

/// `path.Ext` from Go's `path` package, operating on raw bytes.
///
/// Only `/` is treated as a separator (Go's `path`, not `filepath`). Returns
/// the suffix beginning at the final `.` of the final path element, or an
/// empty slice when there is none. `.bashrc` therefore yields `".bashrc"`,
/// `foo` yields `""`, and `a.b.c` yields `".c"`.
pub fn path_ext(path: &[u8]) -> &[u8] {
    for i in (0..path.len()).rev() {
        match path[i] {
            b'/' => return &[],
            b'.' => return &path[i..],
            _ => {}
        }
    }
    &[]
}

/// `zoekt.SortFiles`: rank file matches for presentation.
///
/// Sorts by `Score` descending, then boosts a file with a novel extension to
/// position `boostOffset` (the third result). Go uses the unstable
/// `sort.Sort`; this port uses a stable sort. For the insertion-sort range
/// used by Go's pdqsort (`len <= 12`) both orders agree, including on ties.
pub fn sort_files(files: &mut [FileMatch]) {
    files.sort_by(|left, right| {
        right
            .score
            .partial_cmp(&left.score)
            .unwrap_or(Ordering::Equal)
    });

    // Boost a file extension not in the top 3 to the third file match.
    boost_novel_extension(files, 2, 0.9);
}

/// `zoekt.boostNovelExtension`.
///
/// Finds the first candidate at or after `boost_offset` whose score is within
/// `min_score_ratio` of the first candidate and whose extension is not already
/// present in the top `boost_offset` entries, then bubbles it up to the front
/// of the candidate slice.
pub fn boost_novel_extension(files: &mut [FileMatch], boost_offset: usize, min_score_ratio: f64) {
    if files.len() <= boost_offset + 1 {
        return;
    }

    let (top, candidates) = files.split_at_mut(boost_offset);
    let min_score_for_novelty = candidates[0].score * min_score_ratio;

    let exts: Vec<&[u8]> = top.iter().map(|file| path_ext(&file.file_name)).collect();

    for i in 0..candidates.len() {
        if candidates[i].score < min_score_for_novelty {
            continue;
        }

        if exts.contains(&path_ext(&candidates[i].file_name)) {
            continue;
        }

        // Found what we are looking for; boost to the front of candidates
        // (which is `files[boost_offset]`).
        for j in (1..=i).rev() {
            candidates.swap(j, j - 1);
        }
        return;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn file(name: &str, score: f64) -> FileMatch {
        FileMatch {
            file_name: name.as_bytes().to_vec(),
            score,
            ..FileMatch::default()
        }
    }

    #[test]
    fn path_ext_matches_go_path_semantics() {
        assert_eq!(path_ext(b"dir/file.go"), b".go");
        assert_eq!(path_ext(b"dir/a.b.c"), b".c");
        assert_eq!(path_ext(b"file"), b"");
        assert_eq!(path_ext(b"dir.v2/file"), b"");
        assert_eq!(path_ext(b".bashrc"), b".bashrc");
        assert_eq!(path_ext(b"a."), b".");
        assert_eq!(path_ext(b""), b"");
    }

    #[test]
    fn sorts_by_score_descending() {
        let mut files = vec![file("a.go", 1.0), file("b.go", 3.0), file("c.go", 2.0)];
        sort_files(&mut files);
        let names: Vec<_> = files.iter().map(|f| f.file_name.clone()).collect();
        assert_eq!(
            names,
            vec![b"b.go".to_vec(), b"c.go".to_vec(), b"a.go".to_vec()]
        );
    }

    #[test]
    fn boosts_a_novel_extension_to_the_third_slot() {
        let mut files = vec![
            file("a.go", 10.0),
            file("b.go", 9.0),
            file("c.go", 8.0),
            file("d.md", 7.5),
            file("e.go", 7.0),
        ];
        sort_files(&mut files);
        let names: Vec<_> = files.iter().map(|f| f.file_name.clone()).collect();
        assert_eq!(
            names,
            vec![
                b"a.go".to_vec(),
                b"b.go".to_vec(),
                b"d.md".to_vec(),
                b"c.go".to_vec(),
                b"e.go".to_vec(),
            ]
        );
    }

    #[test]
    fn does_not_boost_a_score_below_the_ratio() {
        let mut files = vec![
            file("a.go", 10.0),
            file("b.go", 9.0),
            file("c.go", 8.0),
            file("d.md", 1.0),
        ];
        sort_files(&mut files);
        assert_eq!(files[2].file_name, b"c.go");
    }
}
