#![forbid(unsafe_code)]

//! Port of `internal/codeintel/shared/trie` (rune trie, compression, freezing).
//!
//! Used by the SCIP migration to assign stable integer identifiers to symbol
//! names and to persist the compressed prefix rows in
//! `codeintel_scip_symbol_names`.

use std::collections::BTreeMap;

const MINIMUM_SEGMENT_LENGTH: usize = 16;

#[derive(Default)]
struct RuneTrieNode {
    terminates_value: bool,
    children: BTreeMap<char, RuneTrieNode>,
}

fn construct_rune_trie(values: &[String]) -> RuneTrieNode {
    let mut root = RuneTrieNode::default();
    for value in values {
        root = rune_trie_insert(root, value);
    }
    root
}

fn rune_trie_insert(mut node: RuneTrieNode, value: &str) -> RuneTrieNode {
    let Some(head) = value.chars().next() else {
        node.terminates_value = true;
        return node;
    };
    let rest = &value[head.len_utf8()..];
    let child = node.children.remove(&head).unwrap_or_default();
    node.children.insert(head, rune_trie_insert(child, rest));
    node
}

#[derive(Default)]
struct StringTrieNode {
    children: BTreeMap<String, StringTrieNode>,
}

fn compress_trie(node: RuneTrieNode) -> StringTrieNode {
    StringTrieNode {
        children: compress_trie_internal(node, ""),
    }
}

fn compress_trie_internal(node: RuneTrieNode, prefix: &str) -> BTreeMap<String, StringTrieNode> {
    if node.children.is_empty() {
        if prefix.is_empty() {
            return BTreeMap::new();
        }
        return BTreeMap::from([(prefix.to_string(), StringTrieNode::default())]);
    }

    if node.terminates_value {
        return map_nontrivial_rune_node(node, "", prefix);
    }

    if node.children.len() == 1 {
        let (child_prefix, child) = node.children.into_iter().next().unwrap();
        return compress_trie_internal(child, &format!("{prefix}{child_prefix}"));
    }

    if prefix.len() < MINIMUM_SEGMENT_LENGTH {
        return map_nontrivial_rune_node(node, prefix, "");
    }

    map_nontrivial_rune_node(node, "", prefix)
}

fn map_nontrivial_rune_node(
    node: RuneTrieNode,
    inlined_prefix: &str,
    shared_prefix: &str,
) -> BTreeMap<String, StringTrieNode> {
    let mut children = BTreeMap::new();
    for (child_prefix, child) in node.children {
        let combined = format!("{inlined_prefix}{child_prefix}");
        for (prefix, sub_node) in compress_trie_internal(child, &combined) {
            children.insert(prefix, sub_node);
        }
    }

    if shared_prefix.is_empty() {
        children
    } else {
        BTreeMap::from([(shared_prefix.to_string(), StringTrieNode { children })])
    }
}

#[derive(Default)]
struct FrozenTrieNode {
    children: Vec<DecoratedFrozenTrieNode>,
}

struct DecoratedFrozenTrieNode {
    id: i32,
    parent_id: Option<i32>,
    prefix: String,
    node: FrozenTrieNode,
}

fn freeze_trie(node: StringTrieNode, start_id: i32) -> (FrozenTrieNode, i32) {
    freeze_trie_internal(node, start_id, None)
}

fn freeze_trie_internal(
    node: StringTrieNode,
    mut next_id: i32,
    parent_id: Option<i32>,
) -> (FrozenTrieNode, i32) {
    // `StringTrieNode` uses a `BTreeMap`, so iteration is already sorted by
    // key like Go's `sort.Strings(keys)`.
    let mut children = Vec::with_capacity(node.children.len());
    for (prefix, child) in node.children {
        let id = next_id;
        let (frozen, following) = freeze_trie_internal(child, id + 1, Some(id));
        next_id = following;
        children.push(DecoratedFrozenTrieNode {
            id,
            parent_id,
            prefix,
            node: frozen,
        });
    }
    (FrozenTrieNode { children }, next_id)
}

/// A frozen prefix trie (`trie.Trie`).
pub struct Trie {
    root: FrozenTrieNode,
}

impl Trie {
    /// `NewTrie`: build from `values`, assigning ids starting at `start_id`.
    /// Returns the trie and the first unused identifier.
    pub fn new(values: &[String], start_id: i32) -> (Self, i32) {
        let compressed = compress_trie(construct_rune_trie(values));
        let (root, next_id) = freeze_trie(compressed, start_id);
        (Self { root }, next_id)
    }

    /// `Traverse`: pre-order walk over every node in the trie.
    pub fn traverse<F>(&self, mut callback: F) -> Result<(), String>
    where
        F: FnMut(i32, Option<i32>, &str) -> Result<(), String>,
    {
        traverse(&self.root, &mut callback)
    }
}

fn traverse<F>(node: &FrozenTrieNode, callback: &mut F) -> Result<(), String>
where
    F: FnMut(i32, Option<i32>, &str) -> Result<(), String>,
{
    for child in &node.children {
        callback(child.id, child.parent_id, &child.prefix)?;
        traverse(&child.node, callback)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    #[test]
    fn reconstructs_values_and_assigns_unique_ids() {
        let values: Vec<String> = [
            "lsif . 1 . `100`.",
            "lsif . 1 . `101`.",
            "lsif . 1 . `200`.",
            "gomod . pkg v1 `Foo`.",
        ]
        .iter()
        .map(|value| value.to_string())
        .collect();

        let (trie, next_id) = Trie::new(&values, 0);

        let mut by_id: BTreeMap<i32, String> = BTreeMap::new();
        let mut seen_ids = std::collections::BTreeSet::new();
        trie.traverse(|id, parent_id, prefix| {
            assert!(seen_ids.insert(id), "duplicate id {id}");
            let name = match parent_id {
                None => prefix.to_string(),
                Some(parent_id) => {
                    let parent = by_id
                        .get(&parent_id)
                        .ok_or_else(|| format!("parent {parent_id} visited later"))?;
                    format!("{parent}{prefix}")
                }
            };
            by_id.insert(id, name);
            Ok(())
        })
        .unwrap();

        let reconstructed: std::collections::BTreeSet<&String> = by_id.values().collect();
        for value in &values {
            assert!(reconstructed.contains(value), "missing {value}");
        }
        assert!(next_id > 0);
    }

    #[test]
    fn empty_input_has_no_nodes() {
        let (trie, next_id) = Trie::new(&[], 7);
        let mut count = 0;
        trie.traverse(|_, _, _| {
            count += 1;
            Ok(())
        })
        .unwrap();
        assert_eq!(count, 0);
        assert_eq!(next_id, 7);
    }
}
