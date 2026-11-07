#![forbid(unsafe_code)]

//! Local code intelligence, a port of the Go baseline's `squirrel` package
//! (`cmd/symbols/internal/squirrel`).
//!
//! The Go implementation keeps `*sitter.Node` pointers alive across parses.
//! Rust tree-sitter nodes borrow their `Tree`, so this port instead snapshots
//! each parsed tree into index-addressed nodes and runs the same traversal
//! algorithms over those indices. Queries are still executed by tree-sitter;
//! captures are mapped back into the snapshot.

use std::cell::Cell;
use std::collections::{HashMap, HashSet};
use std::rc::Rc;

use tree_sitter::{Node as TsNode, Parser, Query, QueryCursor, Tree};

mod hover;
mod java;
mod languages;
mod python;
mod starlark;

pub(crate) use languages::LangSpec;

/// Errors mirrored to gRPC statuses by the service layer. The two sentinel
/// variants keep the baseline's client-visible messages.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum Error {
    #[error("unrecognized file extension")]
    UnrecognizedFileExtension,
    #[error("unsupported language")]
    UnsupportedLanguage,
    #[error("{0}")]
    Message(String),
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RepoCommitPath {
    pub repo: String,
    pub commit: String,
    pub path: String,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Point {
    pub row: u32,
    pub column: u32,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub struct Range {
    pub row: u32,
    pub column: u32,
    pub length: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Symbol {
    pub name: String,
    pub hover: String,
    pub def: Range,
    pub refs: Vec<Range>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Definition {
    pub repo_commit_path: RepoCommitPath,
    pub range: Option<Range>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SymbolInfo {
    pub definition: Definition,
    pub hover: Option<String>,
}

/// A symbol found by the symbols store; the subset squirrel needs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FoundSymbol {
    pub name: String,
    pub path: String,
    pub line: u32,
    pub character: u32,
}

/// Reads file contents at a commit (gitserver `ReadFile`).
pub trait FileSource {
    fn read_file(&self, path: &RepoCommitPath) -> Result<Vec<u8>, String>;
}

/// The symbol search used to resolve imports. `None` for `LocalCodeIntel`
/// requests, matching the baseline's `squirrel.New(readFile, nil)`.
pub trait SymbolSource {
    fn search_symbols(
        &self,
        repo: &str,
        commit: &str,
        include: &[String],
        ident: &str,
    ) -> Result<Vec<FoundSymbol>, String>;
}

/// `squirrel.SquirrelService` for one request.
pub struct Service<'a> {
    files: &'a dyn FileSource,
    symbols: Option<&'a dyn SymbolSource>,
    depth: Cell<u32>,
}

impl<'a> Service<'a> {
    pub fn new(files: &'a dyn FileSource, symbols: Option<&'a dyn SymbolSource>) -> Self {
        Self {
            files,
            symbols,
            depth: Cell::new(0),
        }
    }

    /// `LocalCodeIntel`: definitions and references of local variables.
    pub fn local_code_intel(&self, path: RepoCommitPath) -> Result<Vec<Symbol>, Error> {
        let root = self.parse(path)?;

        // Scopes are keyed by range exactly like the baseline's `nodeId`
        // (`fmt.Sprint(nodeToRange(node))`), so nodes that share a range share
        // a scope. Inserting a fresh map mirrors the baseline overwrite.
        let mut scopes: HashMap<Range, HashMap<String, PartialSymbol>> = HashMap::new();
        for captures in root.query_matches(root.file.lang.locals_query) {
            if let Some(scope) = captures.get("scope") {
                scopes.insert(scope.to_range(), HashMap::new());
            }
        }

        // Collect definitions into the nearest enclosing scope.
        for captures in root.query_matches(root.file.lang.locals_query) {
            for (capture_name, node) in captures {
                if !capture_name.starts_with("definition") {
                    continue;
                }
                let symbol_name = node.content_lossy();
                let mut current = Some(node.clone());
                while let Some(cur) = current {
                    if let Some(scope) = scopes.get_mut(&cur.to_range()) {
                        if !scope.contains_key(&symbol_name) {
                            scope.insert(
                                symbol_name.clone(),
                                PartialSymbol {
                                    name: symbol_name.clone(),
                                    hover: hover::find_hover(&node),
                                    def: node.to_range(),
                                    refs: HashSet::new(),
                                },
                            );
                        }
                        break;
                    }
                    current = cur.parent();
                }
            }
        }

        // Collect references anywhere in the tree.
        for node in root.all_nodes() {
            if !node.kind().contains("identifier") {
                continue;
            }
            let symbol_name = node.content_lossy();
            let mut current = Some(node.clone());
            while let Some(cur) = current {
                if let Some(scope) = scopes.get_mut(&cur.to_range()) {
                    if let Some(symbol) = scope.get_mut(&symbol_name) {
                        symbol.refs.insert(node.to_range());
                        break;
                    }
                }
                current = cur.parent();
            }
        }

        let mut symbols = Vec::new();
        for scope in scopes.values() {
            for partial in scope.values() {
                symbols.push(Symbol {
                    name: partial.name.clone(),
                    hover: partial.hover.clone(),
                    def: partial.def,
                    refs: partial.refs.iter().copied().collect(),
                });
            }
        }
        Ok(symbols)
    }

    /// `SymbolInfo`: the definition and hover at a point.
    pub fn symbol_info(
        &self,
        path: RepoCommitPath,
        point: Point,
    ) -> Result<Option<SymbolInfo>, Error> {
        let root = self.parse(path)?;
        let start = root
            .named_descendant_for_point(point)
            .ok_or_else(|| Error::Message("node is nil".to_owned()))?;

        let Some(found) = self.get_def(&start)? else {
            return Ok(None);
        };

        let definition = match &found {
            FoundDef::Node(node) => Definition {
                repo_commit_path: node.file.rc.clone(),
                range: Some(node.to_range()),
            },
            FoundDef::Dir(path) => Definition {
                repo_commit_path: path.clone(),
                range: None,
            },
        };

        let Some(range) = definition.range else {
            return Ok(Some(SymbolInfo {
                hover: Some(format!("Directory {}", definition.repo_commit_path.path)),
                definition,
            }));
        };

        let end_root = self.parse(definition.repo_commit_path.clone())?;
        let end = end_root
            .named_descendant_for_point(Point {
                row: range.row,
                column: range.column,
            })
            .ok_or_else(|| Error::Message(format!("no node at {}:{}", range.row, range.column)))?;

        Ok(Some(SymbolInfo {
            hover: Some(hover::find_hover(&end)),
            definition,
        }))
    }

    /// `getSymbols`: top-level symbols (only Java defines a query in the
    /// baseline). Kept for parity tests; not part of the RPC surface.
    pub fn top_level_symbols(&self, path: RepoCommitPath) -> Result<Vec<FoundSymbol>, Error> {
        let root = self.parse(path)?;
        let query = match root.file.lang.name {
            "java" => JAVA_TOP_LEVEL_SYMBOLS_QUERY,
            _ => return Ok(Vec::new()),
        };
        let mut symbols = Vec::new();
        for capture in root.query_all(query) {
            let start = capture.start_point();
            symbols.push(FoundSymbol {
                name: capture.content_lossy(),
                path: root.file.rc.path.clone(),
                line: start.0,
                character: start.1,
            });
        }
        Ok(symbols)
    }

    pub(crate) fn parse(&self, path: RepoCommitPath) -> Result<Node, Error> {
        let lang = languages::lang_for_path(&path.path)?;
        let contents = self.files.read_file(&path).map_err(Error::Message)?;
        let mut parser = Parser::new();
        parser
            .set_language(lang.language)
            .map_err(|err| Error::Message(format!("failed to set language: {err}")))?;
        let tree = parser
            .parse(contents.as_slice(), None)
            .ok_or_else(|| Error::Message("failed to parse file contents: no tree".to_owned()))?;
        let file = File::new(path, contents, lang, tree);
        Ok(Node { file, idx: 0 })
    }

    pub(crate) fn get_def(&self, node: &Node) -> Result<Option<FoundDef>, Error> {
        let depth = self.depth.get() + 1;
        if depth > 100 {
            return Err(Error::Message(
                "max squirrel stack depth exceeded".to_owned(),
            ));
        }
        self.depth.set(depth);
        let result = match node.file.lang.name {
            "java" => java::get_def(self, node),
            "python" => python::get_def(self, node),
            "starlark" => starlark::get_def(self, node),
            _ => Ok(None),
        };
        self.depth.set(depth - 1);
        result
    }

    pub(crate) fn symbol_search_one(
        &self,
        repo: &str,
        commit: &str,
        include: Vec<String>,
        ident: &str,
    ) -> Result<Option<Node>, Error> {
        let Some(search) = self.symbols else {
            return Ok(None);
        };
        let found = search
            .search_symbols(repo, commit, &include, ident)
            .map_err(Error::Message)?;
        let Some(symbol) = found.first() else {
            return Ok(None);
        };
        let path = RepoCommitPath {
            repo: repo.to_owned(),
            commit: commit.to_owned(),
            path: symbol.path.clone(),
        };
        let file = match self.parse(path) {
            Ok(file) => file,
            Err(Error::UnsupportedLanguage | Error::UnrecognizedFileExtension) => {
                return Ok(None);
            }
            Err(err) => return Err(err),
        };
        Ok(file.named_descendant_for_point(Point {
            row: symbol.line,
            column: symbol.character,
        }))
    }
}

/// Result of `getDef`: usually a node, sometimes a directory (a Go package).
pub(crate) enum FoundDef {
    Node(Node),
    Dir(RepoCommitPath),
}

impl FoundDef {
    pub(crate) fn node(self) -> Option<Node> {
        match self {
            FoundDef::Node(node) => Some(node),
            FoundDef::Dir(_) => None,
        }
    }
}

struct PartialSymbol {
    name: String,
    hover: String,
    def: Range,
    refs: HashSet<Range>,
}

#[derive(Clone)]
pub(crate) struct Node {
    file: Rc<File>,
    idx: usize,
}

impl Node {
    pub(crate) fn file(&self) -> &File {
        &self.file
    }

    pub(crate) fn idx(&self) -> usize {
        self.idx
    }

    pub(crate) fn kind(&self) -> &str {
        &self.file.nodes[self.idx].kind
    }

    pub(crate) fn content_lossy(&self) -> String {
        let data = &self.file.nodes[self.idx];
        String::from_utf8_lossy(&self.file.contents[data.start_byte..data.end_byte]).into_owned()
    }

    pub(crate) fn start_byte(&self) -> usize {
        self.file.nodes[self.idx].start_byte
    }

    pub(crate) fn start_point(&self) -> (u32, u32) {
        let data = &self.file.nodes[self.idx];
        (data.start_row, data.start_col)
    }

    pub(crate) fn to_range(&self) -> Range {
        let data = &self.file.nodes[self.idx];
        let length = if data.start_row == data.end_row {
            data.end_col.saturating_sub(data.start_col)
        } else {
            1
        };
        Range {
            row: data.start_row,
            column: data.start_col,
            length,
        }
    }

    pub(crate) fn parent(&self) -> Option<Node> {
        self.file.nodes[self.idx]
            .parent
            .map(|parent| self.at(parent))
    }

    pub(crate) fn named_children(&self) -> Vec<Node> {
        self.file.nodes[self.idx]
            .named_children
            .iter()
            .map(|idx| self.at(*idx))
            .collect()
    }

    pub(crate) fn named_child(&self, index: usize) -> Option<Node> {
        self.file.nodes[self.idx]
            .named_children
            .get(index)
            .map(|idx| self.at(*idx))
    }

    pub(crate) fn named_child_count(&self) -> usize {
        self.file.nodes[self.idx].named_children.len()
    }

    pub(crate) fn child_count(&self) -> usize {
        self.file.nodes[self.idx].children.len()
    }

    pub(crate) fn is_named(&self) -> bool {
        self.file.nodes[self.idx].named
    }

    pub(crate) fn child(&self, index: usize) -> Option<Node> {
        self.file.nodes[self.idx]
            .children
            .get(index)
            .map(|idx| self.at(*idx))
    }

    pub(crate) fn child_by_field_name(&self, field: &str) -> Option<Node> {
        // Ask tree-sitter instead of the cached field names: under
        // tree-sitter 0.20.10, `field_name_for_child` reports misaligned field
        // names for grammars generated with ABI 14 (Java, JavaScript,
        // TypeScript), while `child_by_field_name` works for every ABI.
        let ts_node = self.file.ts_node(self.idx)?;
        let found = ts_node.child_by_field_name(field)?;
        self.at_ts(found)
    }

    pub(crate) fn prev_named_sibling(&self) -> Option<Node> {
        let parent = self.file.nodes[self.idx].parent?;
        let siblings = &self.file.nodes[parent].named_children;
        let position = siblings.iter().position(|idx| *idx == self.idx)?;
        position.checked_sub(1).map(|prev| self.at(siblings[prev]))
    }

    pub(crate) fn named_descendant_for_point(&self, point: Point) -> Option<Node> {
        let ts_node = self.file.ts_node(self.idx)?;
        let start = tree_sitter::Point {
            row: point.row as usize,
            column: point.column as usize,
        };
        let found = ts_node.named_descendant_for_point_range(start, start)?;
        self.at_ts(found)
    }

    pub(crate) fn query_matches(&self, query: &str) -> Vec<HashMap<String, Node>> {
        let Some(ts_node) = self.file.ts_node(self.idx) else {
            return Vec::new();
        };
        let Ok(sitter_query) = Query::new(self.file.lang.language, query) else {
            return Vec::new();
        };
        let mut cursor = QueryCursor::new();
        let contents = self.file.contents.as_slice();
        let mut matches = Vec::new();
        for item in cursor.matches(&sitter_query, ts_node, contents) {
            let mut name_to_node = HashMap::new();
            for capture in item.captures {
                let name = sitter_query.capture_names()[capture.index as usize].clone();
                if let Some(node) = self.at_ts(capture.node) {
                    name_to_node.insert(name, node);
                }
            }
            matches.push(name_to_node);
        }
        matches
    }

    pub(crate) fn query_all(&self, query: &str) -> Vec<Node> {
        self.query_matches(query)
            .into_iter()
            .flat_map(|captures| captures.into_values())
            .collect()
    }

    /// Query captures in tree-sitter's emission order (per match). Needed where
    /// the baseline indexes `match.Captures` positionally, e.g. Starlark's
    /// `load()` resolution.
    pub(crate) fn query_captures(&self, query: &str) -> Vec<Vec<(String, Node)>> {
        let Some(ts_node) = self.file.ts_node(self.idx) else {
            return Vec::new();
        };
        let Ok(sitter_query) = Query::new(self.file.lang.language, query) else {
            return Vec::new();
        };
        let mut cursor = QueryCursor::new();
        let contents = self.file.contents.as_slice();
        let mut matches = Vec::new();
        for item in cursor.matches(&sitter_query, ts_node, contents) {
            let mut captures = Vec::new();
            for capture in item.captures {
                let name = sitter_query.capture_names()[capture.index as usize].clone();
                if let Some(node) = self.at_ts(capture.node) {
                    captures.push((name, node));
                }
            }
            matches.push(captures);
        }
        matches
    }

    /// The root node of the parsed file.
    pub(crate) fn root(&self) -> Node {
        self.at(0)
    }

    pub(crate) fn all_nodes(&self) -> Vec<Node> {
        (0..self.file.nodes.len()).map(|idx| self.at(idx)).collect()
    }

    fn at(&self, idx: usize) -> Node {
        Node {
            file: Rc::clone(&self.file),
            idx,
        }
    }

    fn at_ts(&self, ts_node: TsNode<'_>) -> Option<Node> {
        self.file.ids.get(&ts_node.id()).map(|idx| self.at(*idx))
    }
}

struct NodeData {
    kind: String,
    start_byte: usize,
    end_byte: usize,
    start_row: u32,
    start_col: u32,
    end_row: u32,
    end_col: u32,
    parent: Option<usize>,
    child_pos: usize,
    children: Vec<usize>,
    named_children: Vec<usize>,
    named: bool,
}

pub(crate) struct File {
    pub(crate) rc: RepoCommitPath,
    pub(crate) contents: Vec<u8>,
    pub(crate) lang: &'static LangSpec,
    tree: Tree,
    nodes: Vec<NodeData>,
    ids: HashMap<usize, usize>,
}

impl File {
    fn new(rc: RepoCommitPath, contents: Vec<u8>, lang: &'static LangSpec, tree: Tree) -> Rc<Self> {
        let mut nodes: Vec<NodeData> = Vec::new();
        let mut ids = HashMap::new();

        // Iterative pre-order traversal. Children are pushed in reverse so the
        // stack yields them in document order.
        let mut stack: Vec<(TsNode<'_>, Option<usize>)> = vec![(tree.root_node(), None)];
        while let Some((ts_node, parent)) = stack.pop() {
            let idx = nodes.len();
            let start = ts_node.start_position();
            let end = ts_node.end_position();
            let child_pos = parent.map_or(0, |parent| nodes[parent].children.len());
            nodes.push(NodeData {
                kind: ts_node.kind().to_owned(),
                start_byte: ts_node.start_byte(),
                end_byte: ts_node.end_byte(),
                start_row: start.row as u32,
                start_col: start.column as u32,
                end_row: end.row as u32,
                end_col: end.column as u32,
                parent,
                child_pos,
                children: Vec::new(),
                named_children: Vec::new(),
                named: ts_node.is_named(),
            });
            ids.insert(ts_node.id(), idx);
            if let Some(parent) = parent {
                nodes[parent].children.push(idx);
                if ts_node.is_named() {
                    nodes[parent].named_children.push(idx);
                }
            }
            for child_index in (0..ts_node.child_count()).rev() {
                let Some(child) = ts_node.child(child_index) else {
                    continue;
                };
                stack.push((child, Some(idx)));
            }
        }

        Rc::new(Self {
            rc,
            contents,
            lang,
            tree,
            nodes,
            ids,
        })
    }

    fn ts_node(&self, idx: usize) -> Option<TsNode<'_>> {
        let mut path = Vec::new();
        let mut current = idx;
        while let Some(parent) = self.nodes[current].parent {
            path.push(self.nodes[current].child_pos);
            current = parent;
        }
        let mut node = self.tree.root_node();
        for child_pos in path.iter().rev() {
            node = node.child(*child_pos)?;
        }
        Some(node)
    }
}

const JAVA_TOP_LEVEL_SYMBOLS_QUERY: &str = r#"
(program (class_declaration     name: (identifier) @symbol))
(program (enum_declaration      name: (identifier) @symbol))
(program (interface_declaration name: (identifier) @symbol))
"#;
