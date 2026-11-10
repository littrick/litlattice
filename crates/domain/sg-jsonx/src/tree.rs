//! `jsonx/tree.go`: the tolerant JSON parse tree.
//!
//! The Go tree uses pointers (identity comparisons and parent links). The
//! Rust port keeps the same shape in an arena: a node is referenced by its
//! index into `Tree::nodes`, and `find_node_at_location` returns that index.

use crate::visitor::{walk, ParseErrorCode, ParseOptions, Visitor};

/// `jsonx.Literal` (the Go `interface{}` value attached to a node).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Literal {
    Str(String),
    Number(String),
    Bool(bool),
    Null,
}

/// `jsonx.NodeType`; the names feed the edit error messages.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeType {
    Object,
    Array,
    Property,
    String,
    Number,
    Boolean,
    Null,
}

impl NodeType {
    /// `NodeType.String()` (stringer output).
    pub fn as_str(self) -> &'static str {
        match self {
            NodeType::Object => "Object",
            NodeType::Array => "Array",
            NodeType::Property => "Property",
            NodeType::String => "String",
            NodeType::Number => "Number",
            NodeType::Boolean => "Boolean",
            NodeType::Null => "Null",
        }
    }
}

/// A node of the parse tree. Offsets and lengths are in `char`s; the
/// artificial root has offset/length `-1`.
#[derive(Debug, Clone)]
pub struct Node {
    pub kind: NodeType,
    pub value: Option<Literal>,
    pub offset: i64,
    pub length: i64,
    pub column_offset: i64,
    pub parent: Option<usize>,
    pub children: Vec<usize>,
}

/// The parse tree, arena-indexed.
#[derive(Debug, Clone)]
pub struct Tree {
    pub nodes: Vec<Node>,
    pub root: Option<usize>,
}

impl Tree {
    pub fn node(&self, index: usize) -> &Node {
        &self.nodes[index]
    }
}

/// `jsonx.Segment`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Segment {
    Property(String),
    Index(i64),
}

/// `jsonx.Path`.
pub type Path = Vec<Segment>;

/// `jsonx.PropertyPath`.
pub fn property_path(properties: &[&str]) -> Path {
    properties
        .iter()
        .map(|p| Segment::Property((*p).to_owned()))
        .collect()
}

/// `jsonx.ParseTree`.
pub fn parse_tree(text: &str, options: ParseOptions) -> (Tree, Vec<ParseErrorCode>) {
    let mut builder = TreeBuilder::new();
    let mut errors = Vec::new();
    // `walk` reports errors through the visitor; `TreeBuilder` collects them.
    walk(text, options, &mut builder);
    errors.append(&mut builder.errors);
    let mut tree = builder.tree;
    if let Some(root) = tree.nodes[0].children.first().copied() {
        tree.root = Some(root);
        tree.nodes[root].parent = None;
    }
    (tree, errors)
}

struct TreeBuilder {
    tree: Tree,
    current_parent: usize,
    errors: Vec<ParseErrorCode>,
}

impl TreeBuilder {
    fn new() -> Self {
        let root = Node {
            kind: NodeType::Array,
            value: None,
            offset: -1,
            length: -1,
            column_offset: 0,
            parent: None,
            children: Vec::new(),
        };
        Self {
            tree: Tree {
                nodes: vec![root],
                root: None,
            },
            current_parent: 0,
            errors: Vec::new(),
        }
    }

    fn append_value(&mut self, node: Node) -> usize {
        let index = self.tree.nodes.len();
        self.tree.nodes.push(node);
        self.tree.nodes[self.current_parent].children.push(index);
        index
    }

    fn ensure_property_complete(&mut self, end_offset: i64) {
        if self.tree.nodes[self.current_parent].kind == NodeType::Property {
            let node = &mut self.tree.nodes[self.current_parent];
            node.length = end_offset - node.offset;
            self.current_parent = node.parent.unwrap_or(0);
        }
    }
}

impl Visitor for TreeBuilder {
    fn on_object_begin(&mut self, offset: usize, _length: usize) {
        let parent = self.current_parent;
        let index = self.append_value(Node {
            kind: NodeType::Object,
            value: None,
            offset: offset as i64,
            length: -1,
            column_offset: 0,
            parent: Some(parent),
            children: Vec::new(),
        });
        self.current_parent = index;
    }

    fn on_object_property(&mut self, name: &str, offset: usize, length: usize) {
        let parent = self.current_parent;
        let index = self.append_value(Node {
            kind: NodeType::Property,
            value: None,
            offset: offset as i64,
            length: -1,
            column_offset: 0,
            parent: Some(parent),
            children: Vec::new(),
        });
        self.current_parent = index;
        let parent = index;
        let name_node = Node {
            kind: NodeType::String,
            value: Some(Literal::Str(name.to_owned())),
            offset: offset as i64,
            length: length as i64,
            column_offset: 0,
            parent: Some(parent),
            children: Vec::new(),
        };
        let index = self.tree.nodes.len();
        self.tree.nodes.push(name_node);
        self.tree.nodes[parent].children.push(index);
    }

    fn on_object_end(&mut self, offset: usize, length: usize) {
        let end = (offset + length) as i64;
        let node = &mut self.tree.nodes[self.current_parent];
        node.length = end - node.offset;
        self.current_parent = node.parent.unwrap_or(0);
        self.ensure_property_complete(end);
    }

    fn on_array_begin(&mut self, offset: usize, _length: usize) {
        let parent = self.current_parent;
        let index = self.append_value(Node {
            kind: NodeType::Array,
            value: None,
            offset: offset as i64,
            length: -1,
            column_offset: 0,
            parent: Some(parent),
            children: Vec::new(),
        });
        self.current_parent = index;
    }

    fn on_array_end(&mut self, offset: usize, length: usize) {
        let end = (offset + length) as i64;
        let node = &mut self.tree.nodes[self.current_parent];
        node.length = end - node.offset;
        self.current_parent = node.parent.unwrap_or(0);
        self.ensure_property_complete(end);
    }

    fn on_literal_value(&mut self, value: &Literal, offset: usize, length: usize) {
        let kind = match value {
            Literal::Str(_) => NodeType::String,
            Literal::Number(_) => NodeType::Number,
            Literal::Bool(_) => NodeType::Boolean,
            Literal::Null => NodeType::Null,
        };
        self.append_value(Node {
            kind,
            value: Some(value.clone()),
            offset: offset as i64,
            length: length as i64,
            column_offset: 0,
            parent: Some(self.current_parent),
            children: Vec::new(),
        });
        self.ensure_property_complete((offset + length) as i64);
    }

    fn on_separator(&mut self, character: char, offset: usize, _length: usize) {
        if self.tree.nodes[self.current_parent].kind == NodeType::Property {
            if character == ':' {
                self.tree.nodes[self.current_parent].column_offset = offset as i64;
            } else if character == ',' {
                self.ensure_property_complete(offset as i64);
            }
        }
    }

    fn on_error(&mut self, error_code: ParseErrorCode, _offset: usize, _length: usize) {
        self.errors.push(error_code);
    }
}

/// `jsonx.FindNodeAtLocation`.
pub fn find_node_at_location(tree: &Tree, root: Option<usize>, path: &[Segment]) -> Option<usize> {
    let mut node = root?;
    for segment in path {
        match segment {
            Segment::Property(property) => {
                if tree.nodes[node].kind != NodeType::Object {
                    return None;
                }
                let mut found = None;
                for &property_node in &tree.nodes[node].children {
                    let children = &tree.nodes[property_node].children;
                    let name = children
                        .first()
                        .and_then(|&name| tree.nodes[name].value.clone());
                    if name == Some(Literal::Str(property.clone())) {
                        found = children.get(1).copied();
                        break;
                    }
                }
                node = found?;
            }
            Segment::Index(index) => {
                if tree.nodes[node].kind != NodeType::Array || *index < 0 {
                    return None;
                }
                let index = usize::try_from(*index).ok()?;
                if index >= tree.nodes[node].children.len() {
                    return None;
                }
                node = tree.nodes[node].children[index];
            }
        }
    }
    Some(node)
}

/// `jsonx.ObjectPropertyNames`.
pub fn object_property_names(tree: &Tree, node: usize) -> Vec<String> {
    let node = &tree.nodes[node];
    assert!(node.kind == NodeType::Object, "node.kind != Object");
    node.children
        .iter()
        .map(|&property| {
            let name = tree.nodes[property].children[0];
            match &tree.nodes[name].value {
                Some(Literal::Str(name)) => name.clone(),
                _ => String::new(),
            }
        })
        .collect()
}
