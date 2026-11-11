#![forbid(unsafe_code)]

//! `internal/codeintel/uploads/internal/commitgraph`:
//! `commit_graph.go`, `commit_graph_view.go` and `git_commit_graph.go`.
//!
//! The port keeps the baseline's data flow:
//!
//! 1. [`parse_commit_graph`] turns gitserver's `CommitsOrderTopoDate` output
//!    (children first) into a parent map plus a topological order in which
//!    parents come before children. Parents that were not themselves listed
//!    are appended to the *front* of the order as prefix vertices.
//! 2. [`CommitGraphView`] decorates selected commits with upload metadata
//!    (`UploadMeta`) and maps upload identifiers to the `root:indexer` token
//!    that lets uploads with the same root/indexer shadow one another.
//! 3. [`Graph::new`] populates the visible uploads of "interesting" commits
//!    (those with uploads, several parents, or a merge child), then
//!    [`Graph::stream`]/[`Graph::gather`] emit either an upload set or a
//!    parent link depending on how expensive the set is to reconstruct.
//!
//! Distances and tie-breaking are observable: the same `token` is won by the
//! smaller distance, and equal distances prefer the **larger** upload id
//! (`upload-tie-breaking`). Internal maps use `BTreeMap` for deterministic
//! iteration; the baseline iterates Go maps, but the strict `(distance,
//! upload id)` order makes the result independent of iteration order. The
//! order of uploads *within* a returned slice still follows the baseline's
//! unspecified map order, so callers that need a stable rendering sort it.

use std::collections::{BTreeMap, HashMap};

/// The subset of `gitdomain.Commit` the graph build reads.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommitNode {
    pub commit: String,
    pub parents: Vec<String>,
}

/// `commitgraph.CommitGraph`: parent map plus topological order.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CommitGraph {
    graph: BTreeMap<String, Vec<String>>,
    order: Vec<String>,
}

impl CommitGraph {
    /// `CommitGraph.Graph`.
    pub fn graph(&self) -> &BTreeMap<String, Vec<String>> {
        &self.graph
    }

    /// `CommitGraph.Order`.
    pub fn order(&self) -> &[String] {
        &self.order
    }
}

/// `ParseCommitGraph`: the input is ordered children-first (as gitserver's
/// `CommitsOrderTopoDate` returns it). Processing it backwards yields parents
/// before children; parents missing from the input become prefix vertices.
pub fn parse_commit_graph(commits: &[CommitNode]) -> CommitGraph {
    let mut graph: BTreeMap<String, Vec<String>> = BTreeMap::new();
    let mut order: Vec<String> = Vec::with_capacity(commits.len());
    let mut prefix: Vec<String> = Vec::new();

    for commit in commits.iter().rev() {
        graph.insert(commit.commit.clone(), commit.parents.clone());
        order.push(commit.commit.clone());

        for parent in &commit.parents {
            if !graph.contains_key(parent) {
                graph.insert(parent.clone(), Vec::new());
                prefix.push(parent.clone());
            }
        }
    }

    prefix.extend(order);
    CommitGraph {
        graph,
        order: prefix,
    }
}

/// `commitgraph.UploadMeta`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UploadMeta {
    pub upload_id: i64,
    pub distance: u32,
}

/// `commitgraph.CommitGraphView`.
#[derive(Debug, Clone, Default)]
pub struct CommitGraphView {
    /// `Meta`: commit -> visible upload metadata at that commit.
    pub meta: HashMap<String, Vec<UploadMeta>>,
    /// `Tokens`: upload id -> `md5(root || ':' || indexer)`.
    pub tokens: HashMap<i64, String>,
}

impl CommitGraphView {
    /// `CommitGraphView.Add`.
    pub fn add(&mut self, meta: UploadMeta, commit: &str, token: &str) {
        self.meta
            .entry(commit.to_owned())
            .or_default()
            .push(meta.clone());
        self.tokens.insert(meta.upload_id, token.to_owned());
    }

    fn meta_for(&self, commit: &str) -> &[UploadMeta] {
        self.meta.get(commit).map(Vec::as_slice).unwrap_or(&[])
    }

    fn token_for(&self, upload_id: i64) -> &str {
        self.tokens
            .get(&upload_id)
            .map(String::as_str)
            .unwrap_or("")
    }
}

/// `commitgraph.VisibilityRelationship`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VisibilityRelationship {
    pub commit: String,
    pub uploads: Vec<UploadMeta>,
}

/// `commitgraph.LinkRelationship`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LinkRelationship {
    pub commit: String,
    pub ancestor_commit: String,
    pub distance: u32,
}

/// `commitgraph.Envelope` (the baseline's two optional pointers).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Envelope {
    Uploads(VisibilityRelationship),
    Links(LinkRelationship),
}

/// `commitgraph.Graph`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Graph {
    graph: BTreeMap<String, Vec<String>>,
    commits: Vec<String>,
    ancestor_uploads: BTreeMap<String, BTreeMap<String, UploadMeta>>,
}

impl Graph {
    /// `commitgraph.NewGraph`.
    pub fn new(commit_graph: &CommitGraph, commit_graph_view: &CommitGraphView) -> Self {
        let graph = commit_graph.graph.clone();
        let mut order = commit_graph.order.clone();

        let ancestor_uploads = populate_uploads_by_traversal(&graph, &order, commit_graph_view);
        order.sort();

        Self {
            graph,
            commits: order,
            ancestor_uploads,
        }
    }

    /// The raw parent map (used by the differential harness).
    pub fn graph(&self) -> &BTreeMap<String, Vec<String>> {
        &self.graph
    }

    /// The sorted commit order (`Graph.commits`).
    pub fn commits(&self) -> &[String] {
        &self.commits
    }

    /// `Graph.UploadsVisibleAtCommit`.
    pub fn uploads_visible_at_commit(&self, commit: &str) -> Vec<UploadMeta> {
        let (ancestor, distance) =
            traverse_for_uploads(&self.graph, &self.ancestor_uploads, commit);
        let ancestor_uploads = self
            .ancestor_uploads
            .get(&ancestor)
            .cloned()
            .unwrap_or_default();
        adjust_visible_uploads(&ancestor_uploads, distance)
    }

    /// `Graph.Stream`, collected into a vector (the baseline returns a
    /// channel; the ordering is identical).
    pub fn stream(&self) -> Vec<Envelope> {
        let mut envelopes = Vec::new();

        for commit in &self.commits {
            let Some((ancestor_commit, ancestor_distance)) =
                traverse_for_commit_found(&self.graph, &self.ancestor_uploads, commit)
            else {
                continue;
            };

            let ancestor_visible_uploads = self
                .ancestor_uploads
                .get(&ancestor_commit)
                .expect("traverseForCommit returned a stored ancestor");

            if ancestor_distance == 0 || ancestor_visible_uploads.len() == 1 {
                envelopes.push(Envelope::Uploads(VisibilityRelationship {
                    commit: commit.clone(),
                    uploads: adjust_visible_uploads(ancestor_visible_uploads, ancestor_distance),
                }));
            } else if ancestor_visible_uploads.len() > 1 {
                envelopes.push(Envelope::Links(LinkRelationship {
                    commit: commit.clone(),
                    ancestor_commit,
                    distance: ancestor_distance,
                }));
            }
        }

        envelopes
    }

    /// `Graph.Gather`.
    #[allow(clippy::type_complexity)]
    pub fn gather(
        &self,
    ) -> (
        BTreeMap<String, Vec<UploadMeta>>,
        BTreeMap<String, LinkRelationship>,
    ) {
        let mut uploads = BTreeMap::new();
        let mut links = BTreeMap::new();

        for envelope in self.stream() {
            match envelope {
                Envelope::Uploads(relationship) => {
                    uploads.insert(relationship.commit, relationship.uploads);
                }
                Envelope::Links(relationship) => {
                    links.insert(relationship.commit.clone(), relationship);
                }
            }
        }

        (uploads, links)
    }
}

/// `reverseGraph`.
fn reverse_graph(graph: &BTreeMap<String, Vec<String>>) -> BTreeMap<String, Vec<String>> {
    let mut reverse: BTreeMap<String, Vec<String>> = BTreeMap::new();
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

/// `populateUploadsByTraversal`.
fn populate_uploads_by_traversal(
    graph: &BTreeMap<String, Vec<String>>,
    order: &[String],
    commit_graph_view: &CommitGraphView,
) -> BTreeMap<String, BTreeMap<String, UploadMeta>> {
    let reverse_graph = reverse_graph(graph);

    let mut uploads: BTreeMap<String, BTreeMap<String, UploadMeta>> = BTreeMap::new();
    for commit in order {
        let parents = graph.get(commit).cloned().unwrap_or_default();

        if !commit_graph_view.meta.contains_key(commit) && parents.len() <= 1 {
            let mut dedicated_children = true;
            if let Some(children) = reverse_graph.get(commit) {
                for child in children {
                    if graph.get(child).map(Vec::len).unwrap_or(0) > 1 {
                        dedicated_children = false;
                    }
                }
            }

            if dedicated_children {
                continue;
            }
        }

        let mut ancestors = parents;
        let mut distance: u32 = 1;

        // Find nearest ancestors with data. If the loop ends with multiple
        // ancestors, they are all the same distance away and store data.
        while ancestors.len() == 1 {
            if uploads.contains_key(&ancestors[0]) {
                break;
            }
            distance += 1;
            ancestors = graph.get(&ancestors[0]).cloned().unwrap_or_default();
        }

        let populated =
            populate_uploads_for_commit(&uploads, &ancestors, distance, commit_graph_view, commit);
        uploads.insert(commit.clone(), populated);
    }

    uploads
}

/// `populateUploadsForCommit`: uploads defined at the commit shadow inherited
/// ones for the same token; nearest ancestor wins, then the larger upload id.
fn populate_uploads_for_commit(
    uploads: &BTreeMap<String, BTreeMap<String, UploadMeta>>,
    ancestors: &[String],
    distance: u32,
    commit_graph_view: &CommitGraphView,
    commit: &str,
) -> BTreeMap<String, UploadMeta> {
    let mut uploads_by_token: BTreeMap<String, UploadMeta> = BTreeMap::new();

    // Populate uploads defined here.
    for upload in commit_graph_view.meta_for(commit) {
        let token = commit_graph_view.token_for(upload.upload_id).to_owned();
        uploads_by_token.insert(token, upload.clone());
    }

    // Combine with uploads visible from the nearest ancestors.
    for ancestor in ancestors {
        let Some(ancestor_uploads) = uploads.get(ancestor) else {
            continue;
        };
        for upload in ancestor_uploads.values() {
            let token = commit_graph_view.token_for(upload.upload_id).to_owned();

            let mut upload = upload.clone();
            upload.distance = upload.distance.wrapping_add(distance);

            match uploads_by_token.get(&token) {
                Some(current) if !replaces(&upload, current) => {}
                _ => {
                    uploads_by_token.insert(token, upload);
                }
            }
        }
    }

    uploads_by_token
}

/// `traverseForUploads`.
fn traverse_for_uploads(
    graph: &BTreeMap<String, Vec<String>>,
    uploads: &BTreeMap<String, BTreeMap<String, UploadMeta>>,
    commit: &str,
) -> (String, u32) {
    let (commit, distance, _found) = traverse_for_commit(graph, uploads, commit);
    (commit, distance)
}

/// `traverseForCommit`.
fn traverse_for_commit(
    graph: &BTreeMap<String, Vec<String>>,
    uploads: &BTreeMap<String, BTreeMap<String, UploadMeta>>,
    commit: &str,
) -> (String, u32, bool) {
    let mut commit = commit.to_owned();
    let mut distance: u32 = 0;

    loop {
        if uploads.contains_key(&commit) {
            return (commit, distance, true);
        }

        let parents = graph.get(&commit).cloned().unwrap_or_default();
        if parents.is_empty() {
            return (String::new(), 0, false);
        }

        commit = parents[0].clone();
        distance += 1;
    }
}

/// The `(ancestor, distance)` pair when [`traverse_for_commit`] finds data.
fn traverse_for_commit_found(
    graph: &BTreeMap<String, Vec<String>>,
    uploads: &BTreeMap<String, BTreeMap<String, UploadMeta>>,
    commit: &str,
) -> Option<(String, u32)> {
    match traverse_for_commit(graph, uploads, commit) {
        (commit, distance, true) => Some((commit, distance)),
        _ => None,
    }
}

/// `adjustVisibleUploads`.
fn adjust_visible_uploads(
    ancestor_visible_uploads: &BTreeMap<String, UploadMeta>,
    ancestor_distance: u32,
) -> Vec<UploadMeta> {
    ancestor_visible_uploads
        .values()
        .map(|upload| UploadMeta {
            upload_id: upload.upload_id,
            distance: upload.distance.wrapping_add(ancestor_distance),
        })
        .collect()
}

/// `replaces`: smaller distance wins; ties prefer the larger upload id.
fn replaces(upload1: &UploadMeta, upload2: &UploadMeta) -> bool {
    upload1.distance < upload2.distance
        || (upload1.distance == upload2.distance && upload1.upload_id > upload2.upload_id)
}

/// `regexp.MatchString` over the site config's
/// `experimentalFeatures.commitGraphUpdates.defaultBranchOnly` patterns:
/// the commit graph updater switches a repository to the default-branch-only
/// strategy when any pattern matches. An invalid pattern is treated as a
/// non-match, exactly like `err == nil && matched`.
pub fn matches_default_branch_only(patterns: &[String], repo: &str) -> bool {
    patterns.iter().any(|pattern| {
        regex::Regex::new(pattern)
            .map(|regex| regex.is_match(repo))
            .unwrap_or(false)
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn node(commit: &str, parents: &[&str]) -> CommitNode {
        CommitNode {
            commit: commit.to_owned(),
            parents: parents.iter().map(|parent| (*parent).to_owned()).collect(),
        }
    }

    #[test]
    fn parse_commit_graph_reverses_and_prefixes_missing_parents() {
        // Children-first input, as gitserver returns it.
        let graph = parse_commit_graph(&[node("b", &["a"]), node("a", &[])]);
        assert_eq!(graph.order(), ["a".to_owned(), "b".to_owned()].as_slice());
        assert_eq!(graph.graph()["a"], Vec::<String>::new());
        assert_eq!(graph.graph()["b"], vec!["a".to_owned()]);

        // A parent that is not listed becomes a prefix vertex.
        let graph = parse_commit_graph(&[node("b", &["a"])]);
        assert_eq!(graph.order(), ["a".to_owned(), "b".to_owned()].as_slice());
        assert_eq!(graph.graph()["a"], Vec::<String>::new());
    }

    #[test]
    fn empty_graph_has_no_uploads() {
        let graph = Graph::new(&parse_commit_graph(&[]), &CommitGraphView::default());
        assert!(graph.gather().0.is_empty());
        assert!(graph.gather().1.is_empty());
    }

    #[test]
    fn tie_breaking_prefers_the_larger_upload_id() {
        let mut view = CommitGraphView::default();
        view.add(
            UploadMeta {
                upload_id: 1,
                distance: 0,
            },
            "a",
            "root:indexer",
        );
        view.add(
            UploadMeta {
                upload_id: 2,
                distance: 0,
            },
            "b",
            "root:indexer",
        );
        // `a` is the merge parent; `c` has both as ancestors.
        let graph = parse_commit_graph(&[node("c", &["a", "b"]), node("b", &[]), node("a", &[])]);
        let decorated = Graph::new(&graph, &view);
        let (visible, _links) = decorated.gather();
        let uploads_at_c = visible.get("c").expect("c has visible uploads");
        assert_eq!(uploads_at_c.len(), 1);
        assert_eq!(uploads_at_c[0].upload_id, 2);
    }

    #[test]
    fn matches_default_branch_only_ignores_invalid_patterns() {
        assert!(matches_default_branch_only(
            &["github\\.com/acme/.*".to_owned()],
            "github.com/acme/one"
        ));
        assert!(!matches_default_branch_only(
            &["([".to_owned()],
            "github.com/acme/one"
        ));
    }
}
