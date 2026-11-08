#![forbid(unsafe_code)]

//! Port of `internal/codeintel/shared/trie` (`rune_trie.go`,
//! `string_trie.go`, `frozen_trie.go`, `trie.go`).
//!
//! The code-graph writer stores symbol names as a compressed prefix tree in
//! `codeintel_scip_symbol_names`: each row is `(upload_id, id, name_segment,
//! prefix_id)`, where `name_segment` is the edge label from its parent and
//! `prefix_id` is the parent's id. [`new_trie`] returns the nodes in the same
//! pre-order (sorted-child) numbering the baseline assigns, so the stored ids
//! match.

use std::collections::BTreeMap;

/// `minimumSegmentLength`: shorter shared prefixes are inlined into each
/// child instead of becoming their own node.
const MINIMUM_SEGMENT_LENGTH: usize = 16;

#[derive(Debug, Default)]
struct RuneTrieNode {
    terminates_value: bool,
    children: BTreeMap<char, RuneTrieNode>,
}

impl RuneTrieNode {
    fn insert(&mut self, value: &str) {
        if value.is_empty() {
            self.terminates_value = true;
            return;
        }
        let mut chars = value.chars();
        let head = chars.next().expect("non-empty value has a character");
        let rest = chars.as_str();
        self.children.entry(head).or_default().insert(rest);
    }
}

#[derive(Debug, Default)]
struct StringTrieNode {
    children: BTreeMap<String, StringTrieNode>,
}

impl RuneTrieNode {
    fn compress(&self, prefix: &str) -> BTreeMap<String, StringTrieNode> {
        if self.children.is_empty() {
            if !prefix.is_empty() {
                return BTreeMap::from([(prefix.to_owned(), StringTrieNode::default())]);
            }
            return BTreeMap::new();
        }

        if self.terminates_value {
            // A value ends here: emit a node so it keeps a stable identifier.
            return self.map_nontrivial("", prefix);
        }

        if self.children.len() == 1 {
            // Collapse a linear run into a single node.
            if let Some((child_prefix, child)) = self.children.iter().next() {
                return child.compress(&format!("{prefix}{child_prefix}"));
            }
        }

        if prefix.len() < MINIMUM_SEGMENT_LENGTH {
            self.map_nontrivial(prefix, "")
        } else {
            self.map_nontrivial("", prefix)
        }
    }

    fn map_nontrivial(
        &self,
        inlined_prefix: &str,
        shared_prefix: &str,
    ) -> BTreeMap<String, StringTrieNode> {
        let mut children = BTreeMap::new();
        for (child_prefix, child) in &self.children {
            for (prefix, node) in child.compress(&format!("{inlined_prefix}{child_prefix}")) {
                children.insert(prefix, node);
            }
        }

        if shared_prefix.is_empty() {
            children
        } else {
            BTreeMap::from([(shared_prefix.to_owned(), StringTrieNode { children })])
        }
    }
}

/// One node of the frozen (numbered) trie: the row inserted into
/// `codeintel_scip_symbol_names` plus the reconstructed full name (used to map
/// symbol-name → id when inserting `codeintel_scip_symbols`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TrieNode {
    pub id: i32,
    pub parent_id: Option<i32>,
    /// `name_segment`: the edge label from the parent.
    pub prefix: String,
    /// The concatenation of all segments from the root (the symbol name).
    pub full_name: String,
}

/// `NewTrie`: build the compressed prefix trie for `values` (which must be
/// sorted for reproducible numbering) and number its nodes starting at
/// `start_id`. Returns the nodes in pre-order and the next unused id.
pub fn new_trie(values: &[String], start_id: i32) -> (Vec<TrieNode>, i32) {
    let mut rune_trie = RuneTrieNode::default();
    for value in values {
        rune_trie.insert(value);
    }

    let compressed = rune_trie.compress("");
    let mut nodes = Vec::new();
    let next_id = freeze(&compressed, start_id, None, "", &mut nodes);
    (nodes, next_id)
}

fn freeze(
    children: &BTreeMap<String, StringTrieNode>,
    next_id: i32,
    parent_id: Option<i32>,
    parent_name: &str,
    out: &mut Vec<TrieNode>,
) -> i32 {
    let mut next = next_id;
    for (prefix, child) in children {
        let id = next;
        let full_name = format!("{parent_name}{prefix}");
        out.push(TrieNode {
            id,
            parent_id,
            prefix: prefix.clone(),
            full_name: full_name.clone(),
        });
        next = freeze(&child.children, id + 1, Some(id), &full_name, out);
    }
    next
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shares_short_prefixes_without_extra_nodes() {
        // "abc"/"abd": the shared segment "ab" is below the threshold, so it
        // is inlined into both children: nodes "abc" and "abd", ids 0 and 1.
        let values = vec!["abc".to_owned(), "abd".to_owned()];
        let (nodes, next) = new_trie(&values, 0);

        assert_eq!(next, 2);
        assert_eq!(nodes.len(), 2);
        assert_eq!(nodes[0].full_name, "abc");
        assert_eq!(nodes[0].parent_id, None);
        assert_eq!(nodes[1].full_name, "abd");
        assert_eq!(nodes[1].parent_id, None);
    }

    #[test]
    fn long_shared_prefix_becomes_a_node() {
        let shared = "0123456789abcdef"; // 16 bytes, at the threshold
        let values = vec![format!("{shared}1"), format!("{shared}2")];
        let (nodes, next) = new_trie(&values, 5);

        assert_eq!(next, 8);
        assert_eq!(nodes.len(), 3);
        assert_eq!(
            nodes[0],
            TrieNode {
                id: 5,
                parent_id: None,
                prefix: shared.to_owned(),
                full_name: shared.to_owned(),
            }
        );
        assert_eq!(nodes[1].id, 6);
        assert_eq!(nodes[1].parent_id, Some(5));
        assert_eq!(nodes[1].prefix, "1");
        assert_eq!(nodes[1].full_name, format!("{shared}1"));
        assert_eq!(nodes[2].id, 7);
        assert_eq!(nodes[2].parent_id, Some(5));
    }

    #[test]
    fn terminated_value_keeps_its_own_node() {
        // "ab" terminates a value, so it cannot be compressed into "abc".
        let values = vec!["ab".to_owned(), "abc".to_owned()];
        let (nodes, next) = new_trie(&values, 0);

        assert_eq!(next, 2);
        assert_eq!(nodes[0].full_name, "ab");
        assert_eq!(nodes[0].parent_id, None);
        assert_eq!(nodes[1].full_name, "abc");
        assert_eq!(nodes[1].parent_id, Some(0));
        assert_eq!(nodes[1].prefix, "c");
    }

    #[test]
    fn empty_input_produces_no_nodes() {
        let (nodes, next) = new_trie(&[], 3);
        assert!(nodes.is_empty());
        assert_eq!(next, 3);
    }
}
