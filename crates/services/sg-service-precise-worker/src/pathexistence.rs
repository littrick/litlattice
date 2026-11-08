#![forbid(unsafe_code)]

//! Port of `lib/codeintel/pathexistence`.
//!
//! The upload processor needs to know which documents referenced by a SCIP
//! index actually exist in the git clone at the upload's commit. Asking
//! gitserver for every document path individually would be far too expensive,
//! so the baseline builds a directory tree from the index's relative paths and
//! requests directory listings in pruned batches: subtrees that gitserver does
//! not report are never descended into (this is what skips `node_modules`).
//!
//! Paths are slash-separated and root-relative; the `root` is the upload's
//! `Upload.Root` (the directory inside the repository the index describes).

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::future::Future;
use std::path::Path;

/// `pathexistence.StringSet`: the set of entries a directory listing returned.
pub type StringSet = HashSet<String>;

/// `pathexistence.GetChildrenFunc` result: directory name → full paths of its
/// entries. Directory names are relative to the repository root (or prefixed
/// with `root`), exactly like gitserver's `ReadDir` output.
pub type DirectoryChildren = HashMap<String, Vec<String>>;

/// `dirWithoutDot`: the directory of `path`, with `.` normalized to the empty
/// string (the repository root).
pub fn dir_without_dot(path: &str) -> String {
    match Path::new(path).parent() {
        Some(parent) if parent != Path::new(".") => parent.to_string_lossy().into_owned(),
        _ => String::new(),
    }
}

/// `filepath.Join` for slash-separated paths: empty components are dropped and
/// `.`/`..` are resolved lexically.
pub fn join_path(base: &str, name: &str) -> String {
    match (base.is_empty(), name.is_empty()) {
        (true, true) => String::new(),
        (true, false) => clean_path(name),
        (false, true) => clean_path(base),
        (false, false) => clean_path(&format!("{base}/{name}")),
    }
}

/// `path.Clean` for relative slash paths (the upload paths never start with
/// `/`; a leading `/` is preserved if it does appear).
fn clean_path(path: &str) -> String {
    let absolute = path.starts_with('/');
    let mut parts: Vec<&str> = Vec::new();

    for part in path.split('/') {
        match part {
            "" | "." => {}
            ".." => match parts.last() {
                Some(last) if *last != ".." => {
                    parts.pop();
                }
                _ if !absolute => parts.push(".."),
                _ => {}
            },
            other => parts.push(other),
        }
    }

    let joined = parts.join("/");
    if absolute {
        format!("/{joined}")
    } else {
        joined
    }
}

/// A single directory in the tree built from the index's document paths.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct DirTreeNode {
    pub name: String,
    pub children: Vec<DirTreeNode>,
}

/// `makeTree`: build a directory-only tree from the given file paths, with
/// `root` prepended to every path. Paths that escape the root (`..`) are
/// dropped.
pub fn make_tree(root: &str, paths: &[String]) -> DirTreeNode {
    let mut directories: BTreeSet<String> = BTreeSet::new();
    for path in paths {
        let dir = dir_without_dot(&join_path(root, path));
        if !dir.starts_with("..") {
            directories.insert(dir);
        }
    }

    let mut tree = DirTreeNode::default();
    for dir in directories {
        let segments: Vec<&str> = dir.split('/').collect();
        tree = insert_path_segments(tree, &segments);
    }
    tree
}

fn insert_path_segments(mut node: DirTreeNode, segments: &[&str]) -> DirTreeNode {
    let Some((first, rest)) = segments.split_first() else {
        return node;
    };

    if let Some(index) = node.children.iter().position(|child| child.name == *first) {
        let child = std::mem::take(&mut node.children[index]);
        node.children[index] = insert_path_segments(child, rest);
        return node;
    }

    let child = insert_path_segments(
        DirTreeNode {
            name: (*first).to_owned(),
            children: Vec::new(),
        },
        rest,
    );
    node.children.push(child);
    node
}

/// `pathexistence.RequestBatch`: directory subtrees to request on the next
/// round, keyed by the full path of the nodes' parent.
pub type RequestBatch = BTreeMap<String, Vec<DirTreeNode>>;

/// `makeInitialRequestBatch`.
pub fn make_initial_request_batch(root: &str, paths: &[String]) -> RequestBatch {
    let node = make_tree(root, paths);
    let mut batch = RequestBatch::new();
    if root.is_empty() {
        batch.insert(String::new(), vec![node]);
    } else {
        // Skip requesting "" if a root is supplied.
        batch.insert(String::new(), node.children);
    }
    batch
}

/// `RequestBatch.dirnames`: the sorted, de-duplicated list of full directory
/// paths in the batch.
pub fn batch_dirnames(batch: &RequestBatch) -> Vec<String> {
    let mut dirnames = Vec::new();
    for (parent, nodes) in batch {
        for node in nodes {
            dirnames.push(join_path(parent, &node.name));
        }
    }
    dirnames.sort();
    dirnames
}

/// `RequestBatch.next`: children of nodes whose parent directory was visible in
/// the previous response.
pub fn next_batch(batch: &RequestBatch, contents: &HashMap<String, StringSet>) -> RequestBatch {
    let mut next = RequestBatch::new();
    for (parent, nodes) in batch {
        for node in nodes {
            let new_path = join_path(parent, &node.name);
            if !node.children.is_empty()
                && contents.get(&new_path).is_some_and(|set| !set.is_empty())
            {
                next.insert(new_path, node.children.clone());
            }
        }
    }
    next
}

/// `directoryContents`: construct directory name → visible entries by calling
/// `get_children` the minimal number of times. Errors from `get_children` are
/// returned unchanged.
pub async fn directory_contents<E, F, Fut>(
    root: &str,
    paths: &[String],
    mut get_children: F,
) -> Result<HashMap<String, StringSet>, E>
where
    F: FnMut(Vec<String>) -> Fut,
    Fut: Future<Output = Result<DirectoryChildren, E>>,
{
    let mut contents: HashMap<String, StringSet> = HashMap::new();
    let mut batch = make_initial_request_batch(root, paths);

    while !batch.is_empty() {
        let batch_results = get_children(batch_dirnames(&batch)).await?;
        for (directory, children) in batch_results {
            if !children.is_empty() {
                contents.insert(directory, children.into_iter().collect());
            }
        }
        batch = next_batch(&batch, &contents);
    }

    Ok(contents)
}

/// `pathexistence.ExistenceChecker`.
pub struct ExistenceChecker {
    root: String,
    directory_contents: HashMap<String, StringSet>,
}

impl ExistenceChecker {
    /// `NewExistenceChecker`.
    pub async fn new<E, F, Fut>(root: &str, paths: &[String], get_children: F) -> Result<Self, E>
    where
        F: FnMut(Vec<String>) -> Fut,
        Fut: Future<Output = Result<DirectoryChildren, E>>,
    {
        let directory_contents = directory_contents(root, paths, get_children).await?;
        Ok(Self {
            root: root.to_owned(),
            directory_contents,
        })
    }

    /// `Exists`: does `path` appear in the listing of its parent directory?
    pub fn exists(&self, path: &str) -> bool {
        let path = join_path(&self.root, path);
        self.directory_contents
            .get(&dir_without_dot(&path))
            .is_some_and(|children| children.contains(&path))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The baseline's `TestDirectoryContents` oracle and requests.
    #[tokio::test]
    async fn directory_contents_prunes_missing_subtrees() {
        let oracle: HashMap<String, Vec<String>> = HashMap::from([
            (String::new(), vec!["web/".to_owned()]),
            (
                "web".to_owned(),
                vec!["web/core/".to_owned(), "web/shared/".to_owned()],
            ),
            (
                "web/core".to_owned(),
                vec![
                    "web/core/foo.ts".to_owned(),
                    "web/core/bar.ts".to_owned(),
                    "web/core/baz.ts".to_owned(),
                ],
            ),
            (
                "web/shared".to_owned(),
                vec![
                    "web/shared/bonk.ts".to_owned(),
                    "web/shared/quux.ts".to_owned(),
                ],
            ),
        ]);

        let mut requests: Vec<Vec<String>> = Vec::new();
        let mut paths = vec![
            "web/core/foo.ts".to_owned(),
            "web/core/bar.ts".to_owned(),
            "web/core/baz.ts".to_owned(),
            "web/shared/bonk.ts".to_owned(),
            "web/shared/quux.ts".to_owned(),
            "web/shared/quux.generated.ts".to_owned(),
        ];
        for i in 0..100 {
            paths.push(format!("web/node_modules/{i}/deeply/nested/lib/file.ts"));
        }

        let values: HashMap<String, StringSet> = directory_contents("", &paths, |dirnames| {
            let oracle = oracle.clone();
            requests.push(dirnames.clone());
            async move {
                let children: DirectoryChildren = dirnames
                    .into_iter()
                    .map(|dirname| {
                        let children = oracle.get(&dirname).cloned().unwrap_or_default();
                        (dirname, children)
                    })
                    .collect();
                Ok::<DirectoryChildren, ()>(children)
            }
        })
        .await
        .expect("directory contents");

        let expected: HashMap<String, StringSet> = HashMap::from([
            (String::new(), HashSet::from(["web/".to_owned()])),
            (
                "web".to_owned(),
                HashSet::from(["web/core/".to_owned(), "web/shared/".to_owned()]),
            ),
            (
                "web/core".to_owned(),
                HashSet::from([
                    "web/core/foo.ts".to_owned(),
                    "web/core/bar.ts".to_owned(),
                    "web/core/baz.ts".to_owned(),
                ]),
            ),
            (
                "web/shared".to_owned(),
                HashSet::from([
                    "web/shared/bonk.ts".to_owned(),
                    "web/shared/quux.ts".to_owned(),
                ]),
            ),
        ]);
        assert_eq!(values, expected);

        let expected_requests = vec![
            vec![String::new()],
            vec!["web".to_owned()],
            vec![
                "web/core".to_owned(),
                "web/node_modules".to_owned(),
                "web/shared".to_owned(),
            ],
        ];
        assert_eq!(requests, expected_requests);
    }

    #[tokio::test]
    async fn directory_contents_with_root() {
        let oracle: HashMap<String, Vec<String>> = HashMap::from([
            (String::new(), vec!["root/".to_owned()]),
            ("root".to_owned(), vec!["root/web/".to_owned()]),
            (
                "root/web".to_owned(),
                vec!["root/web/core/".to_owned(), "root/web/shared/".to_owned()],
            ),
            (
                "root/web/core".to_owned(),
                vec![
                    "root/web/core/foo.ts".to_owned(),
                    "root/web/core/bar.ts".to_owned(),
                    "root/web/core/baz.ts".to_owned(),
                ],
            ),
            (
                "root/web/shared".to_owned(),
                vec![
                    "root/web/shared/bonk.ts".to_owned(),
                    "root/web/shared/quux.ts".to_owned(),
                ],
            ),
        ]);

        let mut requests: Vec<Vec<String>> = Vec::new();
        let paths: Vec<String> = [
            "web/core/foo.ts",
            "web/core/bar.ts",
            "web/core/baz.ts",
            "web/shared/bonk.ts",
            "web/shared/quux.ts",
            "web/shared/quux.generated.ts",
        ]
        .into_iter()
        .map(str::to_owned)
        .collect();

        let values: HashMap<String, StringSet> = directory_contents("root", &paths, |dirnames| {
            let oracle = oracle.clone();
            requests.push(dirnames.clone());
            async move {
                let children: DirectoryChildren = dirnames
                    .into_iter()
                    .map(|dirname| {
                        let children = oracle.get(&dirname).cloned().unwrap_or_default();
                        (dirname, children)
                    })
                    .collect();
                Ok::<DirectoryChildren, ()>(children)
            }
        })
        .await
        .expect("directory contents");

        let expected: HashMap<String, StringSet> = HashMap::from([
            ("root".to_owned(), HashSet::from(["root/web/".to_owned()])),
            (
                "root/web".to_owned(),
                HashSet::from(["root/web/core/".to_owned(), "root/web/shared/".to_owned()]),
            ),
            (
                "root/web/core".to_owned(),
                HashSet::from([
                    "root/web/core/foo.ts".to_owned(),
                    "root/web/core/bar.ts".to_owned(),
                    "root/web/core/baz.ts".to_owned(),
                ]),
            ),
            (
                "root/web/shared".to_owned(),
                HashSet::from([
                    "root/web/shared/bonk.ts".to_owned(),
                    "root/web/shared/quux.ts".to_owned(),
                ]),
            ),
        ]);
        assert_eq!(values, expected);

        assert_eq!(
            requests,
            vec![
                vec!["root".to_owned()],
                vec!["root/web".to_owned()],
                vec!["root/web/core".to_owned(), "root/web/shared".to_owned()],
            ]
        );
    }

    #[test]
    fn existence_checker_uses_parent_directory_listing() {
        let oracle: HashMap<String, Vec<String>> = HashMap::from([
            (String::new(), vec!["a.ts".to_owned(), "dir".to_owned()]),
            ("dir".to_owned(), vec!["dir/b.ts".to_owned()]),
        ]);
        let checker = tokio::runtime::Runtime::new()
            .expect("runtime")
            .block_on(ExistenceChecker::new(
                "",
                &[
                    "a.ts".to_owned(),
                    "dir/b.ts".to_owned(),
                    "missing/c.ts".to_owned(),
                ],
                |dirnames| {
                    let oracle = oracle.clone();
                    async move {
                        let children: DirectoryChildren = dirnames
                            .into_iter()
                            .map(|dirname| {
                                let children = oracle.get(&dirname).cloned().unwrap_or_default();
                                (dirname, children)
                            })
                            .collect();
                        Ok::<DirectoryChildren, ()>(children)
                    }
                },
            ))
            .expect("checker");

        assert!(checker.exists("a.ts"));
        assert!(checker.exists("dir/b.ts"));
        assert!(!checker.exists("missing/c.ts"));
        assert!(!checker.exists("dir/c.ts"));
    }

    #[test]
    fn clean_join_matches_filepath_join() {
        assert_eq!(join_path("", ""), "");
        assert_eq!(join_path("", "a.ts"), "a.ts");
        assert_eq!(join_path("root", ""), "root");
        assert_eq!(join_path("root", "src/a.ts"), "root/src/a.ts");
        assert_eq!(join_path("root", "./a.ts"), "root/a.ts");
        assert_eq!(clean_path("a//b/../c"), "a/c");
        assert_eq!(dir_without_dot("a.ts"), "");
        assert_eq!(dir_without_dot("src/a.ts"), "src");
        assert_eq!(dir_without_dot(""), "");
    }

    /// The baseline's `TestMakeTree`.
    #[test]
    fn make_tree_builds_directory_only_tree() {
        let paths: Vec<String> = [
            "go.mod",
            "go.sum",
            "LICENSE",
            "README.md",
            "cmd/lsif-go/main.go",
            "internal/gomod/module.go",
            "internal/index/astutil.go",
            "internal/index/helper.go",
            "internal/index/indexer.go",
            "internal/index/types.go",
            "protocol/protocol.go",
            "protocol/writer.go",
        ]
        .into_iter()
        .map(str::to_owned)
        .collect();

        let tree = make_tree("projects/lsif-go", &paths);
        let expected_children = vec!["projects".to_owned()];
        assert_eq!(
            tree.children
                .iter()
                .map(|c| c.name.clone())
                .collect::<Vec<_>>(),
            expected_children
        );

        let projects = &tree.children[0];
        let lsif_go = &projects.children[0];
        let mut names: Vec<&str> = lsif_go.children.iter().map(|c| c.name.as_str()).collect();
        names.sort_unstable();
        assert_eq!(names, vec!["cmd", "internal", "protocol"]);

        let internal = lsif_go
            .children
            .iter()
            .find(|c| c.name == "internal")
            .expect("internal");
        let mut names: Vec<&str> = internal.children.iter().map(|c| c.name.as_str()).collect();
        names.sort_unstable();
        assert_eq!(names, vec!["gomod", "index"]);
    }
}
