#![forbid(unsafe_code)]

//! Query-to-regexp compilation and file matching (`matchtree.go`).
//!
//! Literal-substring pruning is not ported here: it is a pure optimization.
//! The zoekt query conversion ([`MatchTree::to_zoekt_query`]) feeds hybrid
//! search (`hybrid.rs`).

use std::fmt;

use regex::bytes::{Regex, RegexBuilder};

use sg_zoekt::query::{Query as ZoektQuery, ZOEKT_SCORE_BOOST};

use crate::protocol::{PatternNode, QueryNode};

/// Byte range of one match.
pub type MatchRange = (usize, usize);

/// Baseline `matchTree`.
#[derive(Debug, Clone)]
pub enum MatchTree {
    All,
    Regex(RegexMatchTree),
    And(Vec<MatchTree>),
    Or(Vec<MatchTree>),
}

/// Baseline `regexMatchTree`.
#[derive(Debug, Clone)]
pub struct RegexMatchTree {
    regex: Regex,
    /// The baseline lower-cases instead of relying on the regex engine's
    /// case-insensitive flag; zoekt needs to know the original setting.
    ignore_case: bool,
    is_negated: bool,
    /// Baseline `boost`: wrap the zoekt query in `Boost`.
    boost: bool,
}

impl MatchTree {
    /// Baseline `MatchesString`.
    pub fn matches_string(&self, value: &str) -> bool {
        match self {
            Self::All => true,
            Self::Regex(tree) => tree.regex.is_match(value.as_bytes()) == !tree.is_negated,
            Self::And(children) => children.iter().all(|child| child.matches_string(value)),
            Self::Or(children) => children.iter().any(|child| child.matches_string(value)),
        }
    }

    /// Baseline `MatchesFile`: whether the file matches plus the (sorted,
    /// merged, limited) match ranges. Negated patterns return their hits but
    /// invert the boolean.
    pub fn matches_file(&self, content: &[u8], limit: usize) -> (bool, Vec<MatchRange>) {
        match self {
            Self::All => (true, Vec::new()),
            Self::Regex(tree) => {
                let matches = tree.matches_file(content, limit);
                let is_match = !matches.is_empty() == !tree.is_negated;
                // Negated patterns do not contribute ranges to the result:
                // Go returns the regexp's hits as-is, but callers only use
                // them when `match` is true, which cannot happen for a
                // negated tree with hits. Keep the same shape.
                (is_match, matches)
            }
            Self::And(children) => {
                let mut matches = Vec::new();
                for child in children {
                    let (child_match, child_matches) = child.matches_file(content, limit);
                    if !child_match {
                        return (false, Vec::new());
                    }
                    matches.extend(child_matches);
                }
                (true, merge_matches(matches, limit))
            }
            Self::Or(children) => {
                let mut is_match = false;
                let mut matches = Vec::new();
                for child in children {
                    let (child_match, child_matches) = child.matches_file(content, limit);
                    is_match |= child_match;
                    matches.extend(child_matches);
                }
                (is_match, merge_matches(matches, limit))
            }
        }
    }

    /// Whether this tree carries no content/path constraint (baseline
    /// `*allMatchTree`), letting the caller skip loading the file.
    pub fn is_all(&self) -> bool {
        matches!(self, Self::All)
    }

    /// Baseline `ToZoektQuery`: represent this tree as a zoekt query.
    ///
    /// The regexp string is the one our `regex` engine compiled (the baseline
    /// sends the equivalent Go syntax string); zoekt parses it with its own
    /// engine, so a pattern accepted here but rejected there surfaces as a
    /// zoekt search error, which hybrid search treats as a fallback.
    pub fn to_zoekt_query(&self, match_content: bool, match_path: bool) -> ZoektQuery {
        match self {
            Self::All => ZoektQuery::Const(true),
            Self::Regex(tree) => tree.to_zoekt_query(match_content, match_path),
            Self::And(children) => ZoektQuery::And(
                children
                    .iter()
                    .map(|child| child.to_zoekt_query(match_content, match_path))
                    .collect(),
            ),
            Self::Or(children) => ZoektQuery::Or(
                children
                    .iter()
                    .map(|child| child.to_zoekt_query(match_content, match_path))
                    .collect(),
            ),
        }
    }
}

impl RegexMatchTree {
    fn to_zoekt_query(&self, match_content: bool, match_path: bool) -> ZoektQuery {
        let case_sensitive = !self.ignore_case;
        let regexp = |content: bool, file_name: bool| ZoektQuery::Regexp {
            regexp: self.regex.as_str().to_owned(),
            file_name,
            content,
            case_sensitive,
        };

        let mut result = if match_content && match_path {
            ZoektQuery::or(vec![
                self.negate_if_needed(regexp(true, false)),
                self.negate_if_needed(regexp(false, true)),
            ])
        } else {
            self.negate_if_needed(regexp(match_content, match_path))
        };
        if self.boost {
            result = ZoektQuery::Boost {
                child: Box::new(result),
                boost: ZOEKT_SCORE_BOOST,
            };
        }
        result
    }

    fn negate_if_needed(&self, query: ZoektQuery) -> ZoektQuery {
        if self.is_negated {
            ZoektQuery::negate(query)
        } else {
            query
        }
    }

    fn matches_file(&self, content: &[u8], limit: usize) -> Vec<MatchRange> {
        if limit == 0 {
            return Vec::new();
        }
        self.regex
            .find_iter(content)
            .take(limit)
            .map(|matched| (matched.start(), matched.end()))
            .collect()
    }
}

impl fmt::Display for MatchTree {
    /// Baseline `matchTree.String`.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::All => f.write_str("all"),
            Self::Regex(tree) => write!(f, "re: {:?}", tree.regex.as_str()),
            Self::And(children) => write!(f, "AND ({} children)", children.len()),
            Self::Or(children) => write!(f, "OR ({} children)", children.len()),
        }
    }
}

/// Baseline `toMatchTree`.
pub fn to_match_tree(node: &QueryNode, is_case_sensitive: bool) -> Result<MatchTree, regex::Error> {
    match node {
        QueryNode::Pattern(pattern) => to_regexp_tree(pattern, is_case_sensitive),
        QueryNode::And(children) => Ok(MatchTree::And(
            children
                .iter()
                .map(|child| to_match_tree(child, is_case_sensitive))
                .collect::<Result<_, _>>()?,
        )),
        QueryNode::Or(children) => Ok(MatchTree::Or(
            children
                .iter()
                .map(|child| to_match_tree(child, is_case_sensitive))
                .collect::<Result<_, _>>()?,
        )),
    }
}

/// Baseline `toRegexpTree`.
fn to_regexp_tree(node: &PatternNode, is_case_sensitive: bool) -> Result<MatchTree, regex::Error> {
    if node.value.is_empty() {
        return Ok(MatchTree::All);
    }

    let mut pattern = if node.is_regexp {
        // We search whole files rather than line by line, so anchors must
        // consider newlines.
        format!("(?m:{})", node.value)
    } else {
        regex::escape(&node.value)
    };

    // The baseline lower-cases content and pattern instead of using `(?i)`
    // (an ASCII-only transform). `(?i)` produces the same result for ASCII
    // and differs only for Unicode case folding, which is documented as a
    // divergence.
    if !is_case_sensitive {
        pattern = format!("(?i:{pattern})");
    }

    let regex = RegexBuilder::new(&pattern).build()?;

    Ok(MatchTree::Regex(RegexMatchTree {
        regex,
        ignore_case: !is_case_sensitive,
        is_negated: node.is_negated,
        boost: node.boost,
    }))
}

/// Baseline `mergeMatches`: sort by start, drop overlapping ranges, truncate
/// to `limit`.
pub fn merge_matches(mut matches: Vec<MatchRange>, limit: usize) -> Vec<MatchRange> {
    if matches.is_empty() {
        return matches;
    }
    matches.sort_by_key(|matched| matched.0);

    let mut merged: Vec<MatchRange> = Vec::with_capacity(matches.len());
    let mut previous: Option<MatchRange> = None;
    for matched in matches {
        if previous.is_none_or(|prev| matched.0 >= prev.1) {
            merged.push(matched);
            previous = Some(matched);
        }
    }

    merged.truncate(limit);
    merged
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pattern(value: &str, is_regexp: bool, is_negated: bool) -> QueryNode {
        QueryNode::Pattern(PatternNode {
            value: value.to_owned(),
            is_negated,
            is_regexp,
            boost: false,
        })
    }

    #[test]
    fn matches_literals_and_anchors() {
        let tree = to_match_tree(&pattern("needle", false, false), true).expect("tree");
        assert!(tree.matches_string("a needle here"));
        assert!(!tree.matches_string("a Needle here"));
        let (matched, ranges) = tree.matches_file(b"a needle here\n", 10);
        assert!(matched);
        assert_eq!(ranges, vec![(2, 8)]);

        // Regexp anchors are multiline.
        let tree = to_match_tree(&pattern("^needle$", true, false), true).expect("tree");
        assert!(tree.matches_string("a\nneedle\nb"));
    }

    #[test]
    fn honours_case_sensitivity() {
        let tree = to_match_tree(&pattern("needle", false, false), false).expect("tree");
        assert!(tree.matches_string("NEEDLE"));
        assert_eq!(tree.matches_file(b"a NEEDLE here", 10).1, vec![(2, 8)]);
    }

    #[test]
    fn inverts_negated_patterns() {
        let tree = to_match_tree(&pattern("needle", false, true), true).expect("tree");
        assert!(!tree.matches_string("a needle"));
        assert_eq!(tree.matches_file(b"a needle\n", 10), (false, vec![(2, 8)]));
        assert_eq!(tree.matches_file(b"nothing\n", 10), (true, Vec::new()));
    }

    #[test]
    fn empty_pattern_matches_everything() {
        let tree = to_match_tree(&pattern("", false, false), true).expect("tree");
        assert!(tree.is_all());
        assert_eq!(tree.matches_file(b"anything", 10), (true, Vec::new()));
    }

    #[test]
    fn merges_and_or_children() {
        let node = QueryNode::And(vec![pattern("a", false, false), pattern("b", false, false)]);
        let tree = to_match_tree(&node, true).expect("tree");
        let (matched, ranges) = tree.matches_file(b"a x b\n", 10);
        assert!(matched);
        assert_eq!(ranges, vec![(0, 1), (4, 5)]);
        assert!(!tree.matches_string("only a"));

        let node = QueryNode::Or(vec![pattern("a", false, false), pattern("b", false, false)]);
        let tree = to_match_tree(&node, true).expect("tree");
        assert!(tree.matches_string("only b"));
    }

    #[test]
    fn deduplicates_overlapping_ranges() {
        let node = QueryNode::Or(vec![
            pattern("abc", false, false),
            pattern("bcd", true, false),
        ]);
        let tree = to_match_tree(&node, true).expect("tree");
        let (matched, ranges) = tree.matches_file(b"abcd", 10);
        assert!(matched);
        // [0,3) and [1,4) overlap; only the first survives.
        assert_eq!(ranges, vec![(0, 3)]);
    }

    #[test]
    fn truncates_to_the_limit() {
        let tree = to_match_tree(&pattern("a", false, false), true).expect("tree");
        assert_eq!(tree.matches_file(b"aaa", 2).1, vec![(0, 1), (1, 2)]);
    }

    #[test]
    fn rejects_invalid_regexps() {
        assert!(to_match_tree(&pattern("(", true, false), true).is_err());
    }

    #[test]
    fn converts_to_a_zoekt_query() {
        use sg_zoekt::query::Query as ZoektQuery;

        let tree = to_match_tree(&pattern("needle", false, false), true).expect("tree");
        assert_eq!(
            tree.to_zoekt_query(true, false),
            ZoektQuery::Regexp {
                regexp: "needle".to_owned(),
                file_name: false,
                content: true,
                case_sensitive: true,
            }
        );

        // Content and path matching uses an OR of two regexps.
        let query = tree.to_zoekt_query(true, true);
        let ZoektQuery::Or(children) = query else {
            panic!("expected OR, got {query:?}");
        };
        assert_eq!(children.len(), 2);
        assert!(matches!(
            &children[0],
            ZoektQuery::Regexp {
                content: true,
                file_name: false,
                ..
            }
        ));
        assert!(matches!(
            &children[1],
            ZoektQuery::Regexp {
                content: false,
                file_name: true,
                ..
            }
        ));

        // Negation wraps the child.
        let negated = to_match_tree(&pattern("skip", false, true), true).expect("tree");
        let query = negated.to_zoekt_query(true, false);
        let ZoektQuery::Not(child) = query else {
            panic!("expected NOT, got {query:?}");
        };
        assert!(matches!(child.as_ref(), ZoektQuery::Regexp { .. }));

        // Boost wraps the regexp.
        let mut node = pattern("boosted", false, false);
        if let QueryNode::Pattern(pattern) = &mut node {
            pattern.boost = true;
        }
        let tree = to_match_tree(&node, true).expect("tree");
        let query = tree.to_zoekt_query(true, false);
        let ZoektQuery::Boost { boost, .. } = query else {
            panic!("expected BOOST, got {query:?}");
        };
        assert_eq!(boost, ZOEKT_SCORE_BOOST);
    }

    #[test]
    fn zoekt_queries_keep_case_insensitivity() {
        use sg_zoekt::query::Query as ZoektQuery;

        let tree = to_match_tree(&pattern("Needle", false, false), false).expect("tree");
        let query = tree.to_zoekt_query(true, false);
        let ZoektQuery::Regexp {
            regexp,
            case_sensitive,
            ..
        } = query
        else {
            panic!("expected regexp");
        };
        // The pattern carries the inline flag; zoekt is told it is
        // case-insensitive so it does not prune case-folded ngrams.
        assert_eq!(regexp, "(?i:Needle)");
        assert!(!case_sensitive);
    }

    #[test]
    fn formats_like_the_baseline() {
        let tree = to_match_tree(&pattern("x", false, false), true).expect("tree");
        assert_eq!(tree.to_string(), "re: \"x\"");
        let node = QueryNode::And(vec![pattern("x", false, false)]);
        assert_eq!(
            to_match_tree(&node, true).expect("tree").to_string(),
            "AND (1 children)"
        );
    }
}
