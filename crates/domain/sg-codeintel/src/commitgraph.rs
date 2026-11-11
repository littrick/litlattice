#![forbid(unsafe_code)]

//! `internal/codeintel/uploads/internal/commitgraph`: the in-memory commit
//! graph decorated with the code-intel uploads visible from each commit.
//!
//! The `codeintel-commitgraph-updater` job builds a [`CommitGraph`] from the
//! output of `git log --topo-order` (`ParseCommitGraph`), decorates it with
//! the completed uploads of a repository ([`CommitGraphView`]) via
//! [`Graph`], and streams the result into the three denormalized tables the
//! closest-upload queries read (`lsif_nearest_uploads`,
//! `lsif_nearest_uploads_links`, `lsif_uploads_visible_at_tip`).
//!
//! Ported as pure domain code: the caller supplies commit parents and the
//! upload metadata read from Postgres, so no database or gitserver type leaks
//! into the algorithm.

use std::collections::HashMap;

/// One row of `git log` output: a commit and its parents.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommitNode {
    pub id: String,
    pub parents: Vec<String>,
}

/// The undecorated graph produced by [`parse_commit_graph`].
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CommitGraph {
    graph: HashMap<String, Vec<String>>,
    order: Vec<String>,
}

impl CommitGraph {
    pub fn graph(&self) -> &HashMap<String, Vec<String>> {
        &self.graph
    }

    pub fn order(&self) -> &[String] {
        &self.order
    }
}

/// `ParseCommitGraph`: map commits to their parents, and produce a topological
/// ordering in which parents come before children. The input must be ordered
/// like `git log --topo-order` (children before parents). A commit that is
/// listed but has no ancestors gets an empty parent slice, but is still
/// present in the map and the ordering. Parents referenced by a commit but not
/// listed are prepended to the ordering as roots.
pub fn parse_commit_graph(commits: &[CommitNode]) -> CommitGraph {
    // Process the commits backwards so that all parents are seen before their
    // children; the input is in child-before-parent order.
    let mut graph: HashMap<String, Vec<String>> = HashMap::with_capacity(commits.len());
    let mut order: Vec<String> = Vec::with_capacity(commits.len());
    let mut prefix: Vec<String> = Vec::new();

    for commit in commits.iter().rev() {
        graph.insert(commit.id.clone(), commit.parents.clone());
        order.push(commit.id.clone());

        for parent in &commit.parents {
            if !graph.contains_key(parent) {
                graph.insert(parent.clone(), Vec::new());
                prefix.push(parent.clone());
            }
        }
    }

    let mut full_order = prefix;
    full_order.extend(order);

    CommitGraph {
        graph,
        order: full_order,
    }
}

/// `UploadMeta`: the visibility of an upload from a particular location in a
/// repository's commit graph.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UploadMeta {
    pub upload_id: i32,
    pub distance: u32,
}

/// `CommitGraphView`: a space-efficient view of the commit graph decorated
/// with the set of uploads visible at every commit with an upload.
#[derive(Debug, Default)]
pub struct CommitGraphView {
    /// `CommitGraphView.Meta`: commit -> visible upload metadata.
    pub meta: HashMap<String, Vec<UploadMeta>>,
    /// `CommitGraphView.Tokens`: upload id -> `md5(root || ':' || indexer)`.
    /// Equality of this token for two uploads means they can shadow one
    /// another.
    pub tokens: HashMap<i32, String>,
}

impl CommitGraphView {
    pub fn new() -> Self {
        Self::default()
    }

    /// `CommitGraphView.Add`.
    pub fn add(&mut self, meta: UploadMeta, commit: &str, token: &str) {
        self.meta.entry(commit.to_owned()).or_default().push(meta);
        self.tokens.insert(meta.upload_id, token.to_owned());
    }
}

/// `VisibilityRelationship`: the commits and uploads visible from it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VisibilityRelationship {
    pub commit: String,
    pub uploads: Vec<UploadMeta>,
}

/// `LinkRelationship`: the nearest ancestor that stores data, plus the
/// distance to it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LinkRelationship {
    pub commit: String,
    pub ancestor_commit: String,
    pub distance: u32,
}

/// `commitgraph.Envelope`: either a visibility or a link relationship.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Envelope {
    Uploads(VisibilityRelationship),
    Links(LinkRelationship),
}

/// `commitgraph.Graph`: the commit graph decorated with the set of uploads
/// visible from each commit.
#[derive(Debug)]
pub struct Graph {
    graph: HashMap<String, Vec<String>>,
    commits: Vec<String>,
    ancestor_uploads: HashMap<String, HashMap<String, UploadMeta>>,
}

impl Graph {
    /// `NewGraph`.
    pub fn new(commit_graph: &CommitGraph, commit_graph_view: &CommitGraphView) -> Self {
        let order = commit_graph.order().to_vec();
        let ancestor_uploads =
            populate_uploads_by_traversal(&commit_graph.graph, &order, commit_graph_view);

        // The baseline sorts the order in place after decorating (`slices.Sort`),
        // so `Stream` emits commits in lexicographic order.
        let mut commits = order;
        commits.sort();

        Self {
            graph: commit_graph.graph.clone(),
            commits,
            ancestor_uploads,
        }
    }

    /// `UploadsVisibleAtCommit`.
    pub fn uploads_visible_at_commit(&self, commit: &str) -> Vec<UploadMeta> {
        let (ancestor_uploads, distance) = self.traverse_for_uploads(commit);
        adjust_visible_uploads(ancestor_uploads, distance)
    }

    /// `Stream`: lazily yields either the set of visible uploads at a commit
    /// or the nearest ancestor that stores them.
    pub fn stream(&self) -> impl Iterator<Item = Envelope> + '_ {
        self.commits.iter().filter_map(move |commit| {
            let (ancestor_commit, distance, found) = self.traverse_for_commit(commit);
            if !found {
                return None;
            }

            let ancestor_visible_uploads = self.ancestor_uploads.get(ancestor_commit);
            let len = ancestor_visible_uploads.map_or(0, HashMap::len);

            if distance == 0 || len == 1 {
                // Either a single upload (cheap enough to store), or multiple
                // uploads assigned to this commit (which descendants need).
                Some(Envelope::Uploads(VisibilityRelationship {
                    commit: commit.clone(),
                    uploads: adjust_visible_uploads(ancestor_visible_uploads, distance),
                }))
            } else if len > 1 {
                // Reconstructable cheaply from the ancestor; store the link.
                Some(Envelope::Links(LinkRelationship {
                    commit: commit.clone(),
                    ancestor_commit: ancestor_commit.to_owned(),
                    distance,
                }))
            } else {
                None
            }
        })
    }

    /// `Gather`: read the stream to completion (testing/convenience only).
    pub fn gather(
        &self,
    ) -> (
        HashMap<String, Vec<UploadMeta>>,
        HashMap<String, LinkRelationship>,
    ) {
        let mut uploads = HashMap::new();
        let mut links = HashMap::new();
        for envelope in self.stream() {
            match envelope {
                Envelope::Uploads(visibility) => {
                    uploads.insert(visibility.commit, visibility.uploads);
                }
                Envelope::Links(link) => {
                    links.insert(link.commit.clone(), link);
                }
            }
        }
        (uploads, links)
    }

    fn traverse_for_uploads(
        &self,
        commit: &str,
    ) -> (Option<&HashMap<String, UploadMeta>>, u32) {
        let (commit, distance, found) = self.traverse_for_commit(commit);
        if !found {
            return (None, 0);
        }
        (self.ancestor_uploads.get(commit), distance)
    }

    /// `traverseForCommit`: the first ancestor with upload data, and the
    /// distance to it.
    fn traverse_for_commit(&self, commit: &str) -> (&str, u32, bool) {
        let mut commit = commit.to_owned();
        let mut distance = 0u32;
        loop {
            if self.ancestor_uploads.contains_key(&commit) {
                let result = self
                    .ancestor_uploads
                    .get_key_value(&commit)
                    .map(|(key, _)| key.as_str())
                    .unwrap_or_default();
                return (result, distance, true);
            }

            match self.graph.get(&commit) {
                Some(parents) if !parents.is_empty() => {
                    commit = parents[0].clone();
                }
                _ => return ("", 0, false),
            }
            distance += 1;
        }
    }
}

/// `reverseGraph`.
fn reverse_graph(graph: &HashMap<String, Vec<String>>) -> HashMap<String, Vec<String>> {
    let mut reverse: HashMap<String, Vec<String>> = HashMap::with_capacity(graph.len());
    for child in graph.keys() {
        reverse.insert(child.clone(), Vec::new());
    }
    for (child, parents) in graph {
        for parent in parents {
            reverse
                .entry(parent.clone())
                .or_default()
                .push(child.clone());
        }
    }
    reverse
}

/// `populateUploadsByTraversal`: populate the uploads stored for the commits
/// the graph keeps data for. A commit is skipped when it defines no upload,
/// has at most one parent, and no child with multiple parents.
fn populate_uploads_by_traversal(
    graph: &HashMap<String, Vec<String>>,
    order: &[String],
    commit_graph_view: &CommitGraphView,
) -> HashMap<String, HashMap<String, UploadMeta>> {
    let reverse = reverse_graph(graph);

    let mut uploads: HashMap<String, HashMap<String, UploadMeta>> =
        HashMap::with_capacity(order.len());

    for commit in order {
        let parents = graph.get(commit).map(Vec::as_slice).unwrap_or_default();

        if !commit_graph_view.meta.contains_key(commit) && parents.len() <= 1 {
            let dedicated_children = reverse
                .get(commit)
                .map(|children| {
                    children
                        .iter()
                        .all(|child| graph.get(child).map(Vec::len).unwrap_or(0) <= 1)
                })
                .unwrap_or(true);
            if dedicated_children {
                continue;
            }
        }

        let mut ancestors = parents.to_vec();
        let mut distance = 1u32;

        // Find nearest ancestors with data. Several ancestors of equal
        // distance are kept; they are all processed already and store data.
        while ancestors.len() == 1 {
            if uploads.contains_key(&ancestors[0]) {
                break;
            }
            distance += 1;
            ancestors = graph
                .get(&ancestors[0])
                .cloned()
                .unwrap_or_default();
        }

        uploads.insert(
            commit.clone(),
            populate_uploads_for_commit(&uploads, &ancestors, distance, commit_graph_view, commit),
        );
    }

    uploads
}

/// `populateUploadsForCommit`.
fn populate_uploads_for_commit(
    uploads: &HashMap<String, HashMap<String, UploadMeta>>,
    ancestors: &[String],
    distance: u32,
    commit_graph_view: &CommitGraphView,
    commit: &str,
) -> HashMap<String, UploadMeta> {
    let mut uploads_by_token: HashMap<String, UploadMeta> = HashMap::new();

    // Uploads defined here.
    if let Some(meta) = commit_graph_view.meta.get(commit) {
        for upload in meta {
            if let Some(token) = commit_graph_view.tokens.get(&upload.upload_id) {
                uploads_by_token.insert(token.clone(), *upload);
            }
        }
    }

    // Combine with the uploads visible from the nearest ancestors.
    for ancestor in ancestors {
        let Some(ancestor_uploads) = uploads.get(ancestor) else {
            continue;
        };
        for upload in ancestor_uploads.values() {
            let Some(token) = commit_graph_view.tokens.get(&upload.upload_id) else {
                continue;
            };

            let mut upload = *upload;
            upload.distance += distance;

            let replace = match uploads_by_token.get(token) {
                Some(current) => replaces(&upload, current),
                None => true,
            };
            if replace {
                uploads_by_token.insert(token.clone(), upload);
            }
        }
    }

    uploads_by_token
}

/// `adjustVisibleUploads`. The baseline returns the uploads in Go map order;
/// the Rust port sorts by upload id so callers and tests are deterministic.
fn adjust_visible_uploads(
    ancestor_visible_uploads: Option<&HashMap<String, UploadMeta>>,
    ancestor_distance: u32,
) -> Vec<UploadMeta> {
    let Some(ancestor_visible_uploads) = ancestor_visible_uploads else {
        return Vec::new();
    };
    let mut uploads: Vec<UploadMeta> = ancestor_visible_uploads
        .values()
        .map(|upload| UploadMeta {
            upload_id: upload.upload_id,
            distance: upload.distance + ancestor_distance,
        })
        .collect();
    uploads.sort_by_key(|upload| upload.upload_id);
    uploads
}

/// `replaces`: whether `upload1` shadows `upload2`. Ties are broken by the
/// maximum upload identifier, so newer uploads win deterministically.
fn replaces(upload1: &UploadMeta, upload2: &UploadMeta) -> bool {
    upload1.distance < upload2.distance
        || (upload1.distance == upload2.distance && upload1.upload_id > upload2.upload_id)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn commit(id: &str, parents: &[&str]) -> CommitNode {
        CommitNode {
            id: id.to_owned(),
            parents: parents.iter().map(|parent| (*parent).to_owned()).collect(),
        }
    }

    /// `TestCalculateVisibleUploads`'s graph layout:
    ///
    /// ```text
    ///       +--- b -------------------------------+-- [j]
    ///       |                                     |
    /// [a] --+         +-- d             +-- [h] --+--- k -- [m]
    ///       |         |                 |
    ///       +-- [c] --+       +-- [f] --+
    ///                 |       |         |
    ///                 +-- e --+         +-- [i] ------ l -- [n]
    ///                         |
    ///                         +--- g
    /// ```
    fn test_graph() -> Graph {
        let commit_graph = parse_commit_graph(&[
            commit("n", &["l"]),
            commit("m", &["k"]),
            commit("k", &["h"]),
            commit("j", &["b", "h"]),
            commit("h", &["f"]),
            commit("l", &["i"]),
            commit("i", &["f"]),
            commit("f", &["e"]),
            commit("g", &["e"]),
            commit("e", &["c"]),
            commit("d", &["c"]),
            commit("c", &["a"]),
            commit("b", &["a"]),
        ]);

        let mut view = CommitGraphView::new();
        view.add(UploadMeta { upload_id: 45, distance: 0 }, "n", "sub3/:lsif-go");
        view.add(UploadMeta { upload_id: 50, distance: 0 }, "a", "sub1/:lsif-go");
        view.add(UploadMeta { upload_id: 51, distance: 0 }, "j", "sub2/:lsif-go");
        view.add(UploadMeta { upload_id: 52, distance: 0 }, "c", "sub3/:lsif-go");
        view.add(UploadMeta { upload_id: 53, distance: 0 }, "f", "sub3/:lsif-go");
        view.add(UploadMeta { upload_id: 54, distance: 0 }, "i", "sub3/:lsif-go");
        view.add(UploadMeta { upload_id: 55, distance: 0 }, "h", "sub3/:lsif-go");
        view.add(UploadMeta { upload_id: 56, distance: 0 }, "m", "sub3/:lsif-go");

        Graph::new(&commit_graph, &view)
    }

    fn meta(upload_id: i32, distance: u32) -> UploadMeta {
        UploadMeta {
            upload_id,
            distance,
        }
    }

    #[test]
    fn visible_uploads_match_the_baseline() {
        let graph = test_graph();
        let (visible, links) = graph.gather();

        let expected: Vec<(&str, Vec<UploadMeta>)> = vec![
            ("a", vec![meta(50, 0)]),
            ("b", vec![meta(50, 1)]),
            ("c", vec![meta(50, 1), meta(52, 0)]),
            ("f", vec![meta(50, 3), meta(53, 0)]),
            ("i", vec![meta(50, 4), meta(54, 0)]),
            ("h", vec![meta(50, 4), meta(55, 0)]),
            (
                "j",
                vec![meta(50, 2), meta(51, 0), meta(55, 1)],
            ),
            ("m", vec![meta(50, 6), meta(56, 0)]),
            ("n", vec![meta(45, 0), meta(50, 6)]),
        ];
        for (commit, uploads) in expected {
            let mut got = visible.get(commit).cloned().unwrap_or_default();
            got.sort_by_key(|upload| upload.upload_id);
            let mut want = uploads;
            want.sort_by_key(|upload| upload.upload_id);
            assert_eq!(got, want, "commit {commit}");
        }

        let expected_links: Vec<(&str, &str, u32)> = vec![
            ("d", "c", 1),
            ("e", "c", 1),
            ("g", "c", 2),
            ("k", "h", 1),
            ("l", "i", 1),
        ];
        assert_eq!(links.len(), expected_links.len());
        for (commit, ancestor, distance) in expected_links {
            let link = links.get(commit).unwrap_or_else(|| panic!("{commit}"));
            assert_eq!(link.ancestor_commit, ancestor);
            assert_eq!(link.distance, distance);
        }
    }

    #[test]
    fn uploads_visible_at_commit_matches_the_stream() {
        let graph = test_graph();

        let mut uploads = graph.uploads_visible_at_commit("j");
        uploads.sort_by_key(|upload| upload.upload_id);
        assert_eq!(uploads, vec![meta(50, 2), meta(51, 0), meta(55, 1)]);

        // A commit that only has a link relationship reconstructs the
        // ancestor's uploads at one greater distance.
        let mut uploads = graph.uploads_visible_at_commit("d");
        uploads.sort_by_key(|upload| upload.upload_id);
        assert_eq!(uploads, vec![meta(50, 2), meta(52, 1)]);

        // An unknown commit has no visible uploads.
        assert!(graph.uploads_visible_at_commit("zzz").is_empty());
    }

    /// `TestCalculateVisibleUploadsAlternateCommitGraph`.
    ///
    /// ```text
    ///       [b] ------+                                          +------ n --- p
    ///                 |                                          |
    ///             +-- d --+                                  +-- l --+
    ///             |       |                                  |       |
    /// [a] -- c ---+       +-- f -- g -- h -- [i] -- j -- k --+       +-- o -- [q]
    ///             |       |                                  |       |
    ///             +-- e --+                                  +-- m --+
    /// ```
    #[test]
    fn alternate_graph_matches_the_baseline() {
        let commit_graph = parse_commit_graph(&[
            commit("q", &["o"]),
            commit("p", &["n"]),
            commit("o", &["l", "m"]),
            commit("n", &["l"]),
            commit("m", &["k"]),
            commit("l", &["k"]),
            commit("k", &["j"]),
            commit("j", &["i"]),
            commit("i", &["h"]),
            commit("h", &["g"]),
            commit("g", &["f"]),
            commit("f", &["d", "e"]),
            commit("e", &["c"]),
            commit("d", &["b", "c"]),
            commit("c", &["a"]),
        ]);

        let mut view = CommitGraphView::new();
        view.add(UploadMeta { upload_id: 50, distance: 0 }, "a", "sub1/:lsif-go");
        view.add(UploadMeta { upload_id: 51, distance: 0 }, "b", "sub1/:lsif-go");
        view.add(UploadMeta { upload_id: 52, distance: 0 }, "i", "sub2/:lsif-go");
        view.add(UploadMeta { upload_id: 53, distance: 0 }, "q", "sub3/:lsif-go");

        let graph = Graph::new(&commit_graph, &view);
        let (visible, links) = graph.gather();

        let expected: Vec<(&str, Vec<UploadMeta>)> = vec![
            ("a", vec![meta(50, 0)]),
            ("b", vec![meta(51, 0)]),
            ("c", vec![meta(50, 1)]),
            ("d", vec![meta(51, 1)]),
            ("e", vec![meta(50, 2)]),
            ("f", vec![meta(51, 2)]),
            ("g", vec![meta(51, 3)]),
            ("h", vec![meta(51, 4)]),
            ("i", vec![meta(51, 5), meta(52, 0)]),
            ("l", vec![meta(51, 8), meta(52, 3)]),
            ("m", vec![meta(51, 8), meta(52, 3)]),
            ("o", vec![meta(51, 9), meta(52, 4)]),
            (
                "q",
                vec![meta(51, 10), meta(52, 5), meta(53, 0)],
            ),
        ];
        for (commit, uploads) in expected {
            let mut got = visible.get(commit).cloned().unwrap_or_default();
            got.sort_by_key(|upload| upload.upload_id);
            let mut want = uploads;
            want.sort_by_key(|upload| upload.upload_id);
            assert_eq!(got, want, "commit {commit}");
        }

        let expected_links: Vec<(&str, &str, u32)> = vec![
            ("j", "i", 1),
            ("k", "i", 2),
            ("n", "l", 1),
            ("p", "l", 2),
        ];
        assert_eq!(links.len(), expected_links.len());
        for (commit, ancestor, distance) in expected_links {
            let link = links.get(commit).unwrap_or_else(|| panic!("{commit}"));
            assert_eq!(link.ancestor_commit, ancestor);
            assert_eq!(link.distance, distance);
        }
    }

    #[test]
    fn parse_commit_graph_prepends_unseen_parents() {
        let graph = parse_commit_graph(&[commit("b", &["a"]), commit("a", &[])]);
        assert_eq!(graph.graph().get("a"), Some(&Vec::new()));
        assert_eq!(graph.graph().get("b"), Some(&vec!["a".to_owned()]));
        assert_eq!(graph.order(), &["a".to_owned(), "b".to_owned()]);

        // A parent that is not part of the input is a root in the ordering.
        let graph = parse_commit_graph(&[commit("b", &["missing"])]);
        assert_eq!(graph.order(), &["missing".to_owned(), "b".to_owned()]);
    }
}
