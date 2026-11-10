//! `jsonx.Parse` / `jsonx.ParseWithDetailedErrors`
//! (`jsonx/parser.go:16-156`).
//!
//! Builds the plain-JSON value tree from the tolerant walker. Go keeps
//! numbers as `json.Number` (the original lexeme) and only marshals at the
//! end; we keep [`JsonValue::Number`] as the lexeme string for the same
//! round-trip fidelity (notably for JSON-Schema integer checks).

use std::collections::BTreeMap;

use crate::tree::Literal;
use crate::visitor::{walk, ParseErrorCode, ParseOptions, Visitor};

/// Plain JSON value produced by [`parse`]. Object key order is sorted
/// (`BTreeMap`); Go uses `map` and serializes with sorted keys, so the
/// observable JSON is the same.
#[derive(Debug, Clone, PartialEq)]
pub enum JsonValue {
    Null,
    Bool(bool),
    /// The original number lexeme (Go's `json.Number`).
    Number(String),
    String(String),
    Array(Vec<JsonValue>),
    Object(BTreeMap<String, JsonValue>),
}

/// `jsonx.ParseError`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ParseError {
    pub code: ParseErrorCode,
    pub offset: usize,
    pub length: usize,
}

impl ParseError {
    /// The stringer name of the code (`jsonx/json_stringer.go:30-38`).
    pub fn code_name(&self) -> &'static str {
        code_name(self.code)
    }
}

/// Stringer name for a [`ParseErrorCode`].
pub fn code_name(code: ParseErrorCode) -> &'static str {
    match code {
        ParseErrorCode::InvalidSymbol => "InvalidSymbol",
        ParseErrorCode::InvalidNumberFormat => "InvalidNumberFormat",
        ParseErrorCode::PropertyNameExpected => "PropertyNameExpected",
        ParseErrorCode::ValueExpected => "ValueExpected",
        ParseErrorCode::ColonExpected => "ColonExpected",
        ParseErrorCode::CommaExpected => "CommaExpected",
        ParseErrorCode::CloseBraceExpected => "CloseBraceExpected",
        ParseErrorCode::CloseBracketExpected => "CloseBracketExpected",
        ParseErrorCode::EndOfFileExpected => "EndOfFileExpected",
        ParseErrorCode::InvalidCommentToken => "InvalidCommentToken",
        ParseErrorCode::ParseErrorUnexpectedEndOfComment => "ParseErrorUnexpectedEndOfComment",
        ParseErrorCode::ParseErrorUnexpectedEndOfString => "ParseErrorUnexpectedEndOfString",
        ParseErrorCode::ParseErrorUnexpectedEndOfNumber => "ParseErrorUnexpectedEndOfNumber",
        ParseErrorCode::ParseErrorInvalidUnicode => "ParseErrorInvalidUnicode",
        ParseErrorCode::ParseErrorInvalidEscapeCharacter => "ParseErrorInvalidEscapeCharacter",
        ParseErrorCode::ParseErrorInvalidCharacter => "ParseErrorInvalidCharacter",
        ParseErrorCode::InvalidScanErrorCode => "InvalidScanErrorCode",
    }
}

/// `jsonx.Parse`: values plus error codes only.
pub fn parse_codes(text: &str, options: ParseOptions) -> (Option<JsonValue>, Vec<ParseErrorCode>) {
    let (value, errors) = parse(text, options);
    (value, errors.into_iter().map(|error| error.code).collect())
}

/// `jsonx.ParseWithDetailedErrors`: returns the first root value (if any)
/// and every parse error encountered.
pub fn parse(text: &str, options: ParseOptions) -> (Option<JsonValue>, Vec<ParseError>) {
    let mut builder = ParseBuilder::default();
    walk(text, options, &mut builder);
    let errors = std::mem::take(&mut builder.errors);
    (builder.finish(), errors)
}

enum Node {
    Null,
    Bool(bool),
    Number(String),
    String(String),
    Array(Vec<usize>),
    Object(BTreeMap<String, usize>),
}

#[derive(Default)]
struct ParseBuilder {
    nodes: Vec<Node>,
    stack: Vec<usize>,
    roots: Vec<usize>,
    current_property: Option<String>,
    errors: Vec<ParseError>,
}

impl ParseBuilder {
    fn push_node(&mut self, node: Node) -> usize {
        self.nodes.push(node);
        self.nodes.len() - 1
    }

    fn append(&mut self, id: usize) {
        let property = self.current_property.clone();
        match self.stack.last().copied() {
            Some(parent) => match &mut self.nodes[parent] {
                Node::Array(items) => items.push(id),
                Node::Object(properties) => {
                    // Go: `currentParent.object[currentProperty.name] = value`.
                    properties.insert(property.unwrap_or_default(), id);
                }
                _ => unreachable!("parent is always a container"),
            },
            None => self.roots.push(id),
        }
    }

    fn finish(self) -> Option<JsonValue> {
        let root = *self.roots.first()?;
        Some(self.to_value(root))
    }

    fn to_value(&self, id: usize) -> JsonValue {
        match &self.nodes[id] {
            Node::Null => JsonValue::Null,
            Node::Bool(value) => JsonValue::Bool(*value),
            Node::Number(value) => JsonValue::Number(value.clone()),
            Node::String(value) => JsonValue::String(value.clone()),
            Node::Array(items) => {
                JsonValue::Array(items.iter().map(|&item| self.to_value(item)).collect())
            }
            Node::Object(properties) => JsonValue::Object(
                properties
                    .iter()
                    .map(|(key, &value)| (key.clone(), self.to_value(value)))
                    .collect(),
            ),
        }
    }
}

impl Visitor for ParseBuilder {
    fn on_object_begin(&mut self, _offset: usize, _length: usize) {
        let id = self.push_node(Node::Object(BTreeMap::new()));
        self.append(id);
        self.stack.push(id);
        // Go resets the current property after entering a container.
        self.current_property = None;
    }

    fn on_object_property(&mut self, property: &str, _offset: usize, _length: usize) {
        self.current_property = Some(property.to_owned());
    }

    fn on_object_end(&mut self, _offset: usize, _length: usize) {
        self.stack.pop();
    }

    fn on_array_begin(&mut self, _offset: usize, _length: usize) {
        let id = self.push_node(Node::Array(Vec::new()));
        self.append(id);
        self.stack.push(id);
        self.current_property = None;
    }

    fn on_array_end(&mut self, _offset: usize, _length: usize) {
        self.stack.pop();
    }

    fn on_literal_value(&mut self, value: &Literal, _offset: usize, _length: usize) {
        let node = match value {
            Literal::Null => Node::Null,
            Literal::Bool(value) => Node::Bool(*value),
            Literal::Number(value) => Node::Number(value.clone()),
            Literal::Str(value) => Node::String(value.clone()),
        };
        let id = self.push_node(node);
        self.append(id);
    }

    fn on_error(&mut self, error_code: ParseErrorCode, offset: usize, length: usize) {
        self.errors.push(ParseError {
            code: error_code,
            offset,
            length,
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const PARSE: ParseOptions = ParseOptions {
        comments: true,
        trailing_commas: true,
    };

    #[test]
    fn parses_comments_and_trailing_commas() {
        let (value, errors) = parse("// x\n{\"a\": [1, 2,], /* y */ \"b\": true}", PARSE);
        assert!(errors.is_empty());
        let value = value.expect("root");
        let JsonValue::Object(object) = value else {
            panic!("object");
        };
        assert_eq!(
            object.get("a"),
            Some(&JsonValue::Array(vec![
                JsonValue::Number("1".to_owned()),
                JsonValue::Number("2".to_owned()),
            ]))
        );
        assert_eq!(object.get("b"), Some(&JsonValue::Bool(true)));
    }

    #[test]
    fn empty_document_has_no_root() {
        let (value, errors) = parse("// only a comment", PARSE);
        assert!(value.is_none());
        assert!(errors.is_empty());
    }

    #[test]
    fn duplicate_properties_last_wins() {
        let (value, _) = parse("{\"a\": 1, \"a\": 2}", PARSE);
        let JsonValue::Object(object) = value.expect("root") else {
            panic!("object");
        };
        assert_eq!(object.get("a"), Some(&JsonValue::Number("2".to_owned())));
    }

    #[test]
    fn error_codes_match_go() {
        let (_, errors) = parse("{\"a\": tru}", PARSE);
        assert_eq!(
            errors.iter().map(ParseError::code_name).collect::<Vec<_>>(),
            vec!["InvalidSymbol", "ValueExpected"]
        );
        let (_, errors) = parse("{\"a\": \"x", PARSE);
        assert_eq!(
            errors.iter().map(ParseError::code_name).collect::<Vec<_>>(),
            vec!["ParseErrorUnexpectedEndOfString", "CloseBraceExpected"]
        );
    }
}
