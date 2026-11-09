//! A `text/template` / `html/template` subset sufficient for the e-mail
//! templates shipped in the Go baseline.
//!
//! Supported constructs:
//!
//! * text and `{{ … }}` actions, with the `{{-` / `-}}` white-space trim
//!   markers (Go's marker grammar: `-` followed by an ASCII space character on
//!   the left, space followed by `-` on the right);
//! * `{{/* … */}}` comments;
//! * pipelines consisting of a single field chain: `.`, `$`, `.Field`,
//!   `.Field.Nested`, `$.Field.Nested`;
//! * `{{if pipeline}} … {{else}} … {{end}}`, including `{{else if …}}` and
//!   nesting;
//! * `{{range pipeline}} … {{end}}` over slices (and over `null`, which yields
//!   zero iterations).
//!
//! Anything else — function calls, pipelines with pipes, variables, `with`,
//! `template`, `define`, map ranges, integer ranges — is rejected with
//! [`TemplateError::Unsupported`]. The Go parser would accept more; rejecting
//! keeps unported constructs visible instead of rendering them differently.
//!
//! HTML mode performs the contextual escaping `html/template` applies: text
//! pipelines use the HTML escaper, pipelines inside quoted URL attributes
//! (`href`, `src`, …) use `urlFilter` → `urlNormalizer` → HTML escaper. The
//! context is tracked over the text that is actually rendered, which matches
//! the static analysis for the templates in this repository (their conditional
//! branches return to the same context). `script`/`style`/CSS/JS contexts and
//! pipelines used as attribute names are not modelled.

use serde_json::Value;

use crate::escape;

/// Whether a template is executed like `text/template` or `html/template`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TemplateMode {
    Text,
    Html,
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum TemplateError {
    #[error("unclosed action")]
    UnclosedAction,
    #[error("unclosed comment")]
    UnclosedComment,
    #[error("comment ends before closing delimiter")]
    CommentEndsBeforeDelimiter,
    #[error("unexpected {{end}}")]
    UnexpectedEnd,
    #[error("unexpected {{else}}")]
    UnexpectedElse,
    #[error("missing {{end}}")]
    MissingEnd,
    #[error("unsupported template construct: {0}")]
    Unsupported(String),
    #[error("range can't iterate over {0}")]
    RangeOverNonList(String),
}

/// One parsed template.
#[derive(Clone, Debug)]
pub struct Template {
    nodes: Vec<Node>,
    mode: TemplateMode,
}

impl Template {
    pub fn parse(source: &str, mode: TemplateMode) -> Result<Self, TemplateError> {
        let tokens = lex(source)?;
        let mut parser = Parser { tokens, pos: 0 };
        let (nodes, terminator) = parser.parse_list(false)?;
        match terminator {
            None => Ok(Self { nodes, mode }),
            Some(Terminator::End) => Err(TemplateError::UnexpectedEnd),
            Some(Terminator::Else) => Err(TemplateError::UnexpectedElse),
        }
    }

    /// Executes the template against `data` (which becomes both `.` and `$`).
    pub fn execute(&self, data: &Value) -> Result<String, TemplateError> {
        let mut out = String::new();
        let mut scanner = (self.mode == TemplateMode::Html).then(HtmlScanner::new);
        exec_nodes(&self.nodes, data, data, &mut out, scanner.as_mut())?;
        Ok(out)
    }
}

#[derive(Clone, Debug)]
enum Token {
    Text(String),
    Action(String),
}

/// Lexes `{{ … }}` actions and text, applying the trim markers the same way the
/// Go lexer does (trimming ASCII space characters only).
fn lex(source: &str) -> Result<Vec<Token>, TemplateError> {
    let mut tokens = Vec::new();
    let mut pos = 0;

    while pos < source.len() {
        let Some(relative) = source[pos..].find("{{") else {
            push_text(&mut tokens, &source[pos..]);
            break;
        };
        let open = pos + relative;
        let mut text = &source[pos..open];
        let mut action_start = open + 2;

        if has_left_trim_marker(&source[action_start..]) {
            text = trim_right_space(text);
            action_start += 2; // the '-' and the single space character
        }

        if source[action_start..].starts_with("/*") {
            let rest = &source[action_start + 2..];
            let Some(close_rel) = rest.find("*/") else {
                return Err(TemplateError::UnclosedComment);
            };
            let mut after = action_start + 2 + close_rel + 2;
            let (delimiter_len, right_trim) = right_delimiter(&source[after..])?;
            after += delimiter_len;
            if right_trim {
                after = skip_left_space(source, after);
            }
            push_text(&mut tokens, text);
            pos = after;
            continue;
        }

        let Some(close_rel) = source[action_start..].find("}}") else {
            return Err(TemplateError::UnclosedAction);
        };
        let close = action_start + close_rel;
        // Inspect bytes directly: `close - 2` need not be a char boundary when
        // the action ends with a multi-byte character.
        let bytes = source.as_bytes();
        let right_trim =
            close >= action_start + 2 && is_space(bytes[close - 2]) && bytes[close - 1] == b'-';
        let content_end = if right_trim { close - 2 } else { close };

        push_text(&mut tokens, text);
        let content = trim_space(&source[action_start..content_end]);
        tokens.push(Token::Action(content.to_owned()));

        pos = close + 2;
        if right_trim {
            pos = skip_left_space(source, pos);
        }
    }

    Ok(tokens)
}

/// Go's `hasLeftTrimMarker`: `-` followed by one ASCII space character.
fn has_left_trim_marker(input: &str) -> bool {
    let bytes = input.as_bytes();
    bytes.len() >= 2 && bytes[0] == b'-' && is_space(bytes[1])
}

/// Go's `hasRightTrimMarker`: one ASCII space character followed by `-`.
fn has_right_trim_marker(input: &str) -> bool {
    let bytes = input.as_bytes();
    bytes.len() >= 2 && is_space(bytes[0]) && bytes[1] == b'-'
}

/// Right-delimiter check used after a comment: either `}}` or ` -}}`.
fn right_delimiter(input: &str) -> Result<(usize, bool), TemplateError> {
    if has_right_trim_marker(input) && input[2..].starts_with("}}") {
        return Ok((4, true));
    }
    if input.starts_with("}}") {
        return Ok((2, false));
    }
    Err(TemplateError::CommentEndsBeforeDelimiter)
}

fn push_text(tokens: &mut Vec<Token>, text: &str) {
    if !text.is_empty() {
        tokens.push(Token::Text(text.to_owned()));
    }
}

fn is_space(byte: u8) -> bool {
    matches!(byte, b' ' | b'\t' | b'\r' | b'\n')
}

fn trim_right_space(input: &str) -> &str {
    input.trim_end_matches(|c: char| c.is_ascii() && is_space(c as u8))
}

fn skip_left_space(source: &str, mut pos: usize) -> usize {
    let bytes = source.as_bytes();
    while pos < bytes.len() && is_space(bytes[pos]) {
        pos += 1;
    }
    pos
}

fn trim_space(input: &str) -> &str {
    input.trim_matches(|c: char| c.is_ascii() && is_space(c as u8))
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Terminator {
    End,
    Else,
}

struct Parser {
    tokens: Vec<Token>,
    pos: usize,
}

impl Parser {
    /// Parses until `end`/`else` (inside a block) or EOF (top level).
    fn parse_list(
        &mut self,
        in_block: bool,
    ) -> Result<(Vec<Node>, Option<Terminator>), TemplateError> {
        let mut nodes = Vec::new();
        while let Some(token) = self.tokens.get(self.pos) {
            match token {
                Token::Text(text) => {
                    nodes.push(Node::Text(text.clone()));
                    self.pos += 1;
                }
                Token::Action(action) => {
                    let action = action.clone();
                    match keyword(&action) {
                        Some("end") => {
                            self.pos += 1;
                            if in_block {
                                return Ok((nodes, Some(Terminator::End)));
                            }
                            return Err(TemplateError::UnexpectedEnd);
                        }
                        Some("else") => {
                            let rest = trim_space(&action["else".len()..]);
                            if rest.is_empty() {
                                self.pos += 1;
                                if in_block {
                                    return Ok((nodes, Some(Terminator::Else)));
                                }
                                return Err(TemplateError::UnexpectedElse);
                            }
                            if keyword(rest) == Some("if") {
                                // `{{else if …}}` is one action in Go; rewrite
                                // it to the nested if the else arm expects and
                                // leave it for `parse_if` to consume.
                                self.tokens[self.pos] = Token::Action(rest.to_owned());
                                if in_block {
                                    return Ok((nodes, Some(Terminator::Else)));
                                }
                                return Err(TemplateError::UnexpectedElse);
                            }
                            return Err(TemplateError::Unsupported(action));
                        }
                        Some("if") => {
                            self.pos += 1;
                            nodes.push(self.parse_if(&action["if".len()..])?);
                        }
                        Some("range") => {
                            self.pos += 1;
                            let pipeline = parse_pipeline(&action["range".len()..])?;
                            let (body, terminator) = self.parse_list(true)?;
                            if terminator != Some(Terminator::End) {
                                return Err(TemplateError::MissingEnd);
                            }
                            nodes.push(Node::Range {
                                sequence: pipeline,
                                body,
                            });
                        }
                        _ => {
                            self.pos += 1;
                            nodes.push(Node::Output(parse_pipeline(&action)?));
                        }
                    }
                }
            }
        }
        if in_block {
            return Err(TemplateError::MissingEnd);
        }
        Ok((nodes, None))
    }

    fn parse_if(&mut self, condition: &str) -> Result<Node, TemplateError> {
        let condition = parse_pipeline(condition)?;
        let (then_nodes, terminator) = self.parse_list(true)?;
        let otherwise = match terminator {
            Some(Terminator::End) => Vec::new(),
            Some(Terminator::Else) => match self.tokens.get(self.pos) {
                // `{{else if …}}`: Go parses it as an if node in the else arm.
                Some(Token::Action(action)) if keyword(action) == Some("if") => {
                    let action = action.clone();
                    self.pos += 1;
                    vec![self.parse_if(&action["if".len()..])?]
                }
                _ => {
                    let (nodes, terminator) = self.parse_list(true)?;
                    if terminator != Some(Terminator::End) {
                        return Err(TemplateError::MissingEnd);
                    }
                    nodes
                }
            },
            None => return Err(TemplateError::MissingEnd),
        };
        Ok(Node::If {
            condition,
            then_nodes,
            otherwise,
        })
    }
}

fn keyword(action: &str) -> Option<&'static str> {
    let action = action.trim_start_matches(|c: char| c.is_ascii() && is_space(c as u8));
    ["end", "else", "if", "range"].into_iter().find(|keyword| {
        action == *keyword
            || (action.starts_with(keyword)
                && action[keyword.len()..].starts_with([' ', '\t', '\r', '\n']))
    })
}

#[derive(Clone, Debug)]
enum Node {
    Text(String),
    Output(Pipeline),
    If {
        condition: Pipeline,
        then_nodes: Vec<Node>,
        otherwise: Vec<Node>,
    },
    Range {
        sequence: Pipeline,
        body: Vec<Node>,
    },
}

#[derive(Clone, Debug)]
enum Pipeline {
    Dot,
    Root,
    Field { from_root: bool, path: Vec<String> },
}

fn parse_pipeline(input: &str) -> Result<Pipeline, TemplateError> {
    let input = input.trim_matches(|c: char| c.is_ascii() && is_space(c as u8));
    match input {
        "." => return Ok(Pipeline::Dot),
        "$" => return Ok(Pipeline::Root),
        "" => return Err(TemplateError::Unsupported("empty pipeline".to_owned())),
        _ => {}
    }
    let (from_root, rest) = if let Some(rest) = input.strip_prefix("$.") {
        (true, rest)
    } else if let Some(rest) = input.strip_prefix('.') {
        (false, rest)
    } else {
        return Err(TemplateError::Unsupported(input.to_owned()));
    };
    let mut path = Vec::new();
    for segment in rest.split('.') {
        if segment.is_empty()
            || !segment
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '_')
        {
            return Err(TemplateError::Unsupported(input.to_owned()));
        }
        path.push(segment.to_owned());
    }
    Ok(Pipeline::Field { from_root, path })
}

const NULL: Value = Value::Null;

fn resolve<'a>(pipeline: &Pipeline, dot: &'a Value, root: &'a Value) -> &'a Value {
    match pipeline {
        Pipeline::Dot => dot,
        Pipeline::Root => root,
        Pipeline::Field { from_root, path } => {
            let mut value = if *from_root { root } else { dot };
            for segment in path {
                value = match value {
                    Value::Object(map) => map.get(segment).unwrap_or(&NULL),
                    _ => &NULL,
                };
            }
            value
        }
    }
}

fn is_true(value: &Value) -> bool {
    match value {
        Value::Null => false,
        Value::Bool(value) => *value,
        Value::Number(number) => {
            if let Some(value) = number.as_i64() {
                value != 0
            } else if let Some(value) = number.as_u64() {
                value != 0
            } else {
                number.as_f64().is_some_and(|value| value != 0.0)
            }
        }
        Value::String(value) => !value.is_empty(),
        Value::Array(values) => !values.is_empty(),
        Value::Object(map) => !map.is_empty(),
    }
}

fn exec_nodes(
    nodes: &[Node],
    dot: &Value,
    root: &Value,
    out: &mut String,
    scanner: Option<&mut HtmlScanner>,
) -> Result<(), TemplateError> {
    let mut scanner = scanner;
    for node in nodes {
        match node {
            Node::Text(text) => match scanner.as_deref_mut() {
                Some(scanner) => scanner.write_text(text, out),
                None => out.push_str(text),
            },
            Node::Output(pipeline) => {
                let rendered = format_value(resolve(pipeline, dot, root));
                match scanner.as_deref_mut() {
                    Some(scanner) => out.push_str(&scanner.escape(&rendered)),
                    None => out.push_str(&rendered),
                }
            }
            Node::If {
                condition,
                then_nodes,
                otherwise,
            } => {
                let branch = if is_true(resolve(condition, dot, root)) {
                    then_nodes
                } else {
                    otherwise
                };
                exec_nodes(branch, dot, root, out, scanner.as_deref_mut())?;
            }
            Node::Range { sequence, body } => match resolve(sequence, dot, root) {
                Value::Array(values) => {
                    for value in values {
                        exec_nodes(body, value, root, out, scanner.as_deref_mut())?;
                    }
                }
                Value::Null => {}
                other => {
                    return Err(TemplateError::RangeOverNonList(format_value(other)));
                }
            },
        }
    }
    Ok(())
}

/// `fmt.Sprint`-like rendering of a pipeline result. The Go templates print
/// strings, numbers and booleans; slices and maps are supported for
/// completeness (Go prints `[a b]` / `map[k:v]` with sorted map keys).
fn format_value(value: &Value) -> String {
    match value {
        Value::Null => "<no value>".to_owned(),
        Value::Bool(value) => value.to_string(),
        Value::Number(number) => number.to_string(),
        Value::String(value) => value.clone(),
        Value::Array(values) => {
            let items = values.iter().map(format_value).collect::<Vec<_>>();
            format!("[{}]", items.join(" "))
        }
        Value::Object(map) => {
            let items = map
                .iter()
                .map(|(key, value)| format!("{key}:{}", format_value(value)))
                .collect::<Vec<_>>();
            format!("map[{}]", items.join(" "))
        }
    }
}

/// Tracks the `html/template` context (text vs. quoted attribute value) across
/// the text nodes executed so far.
#[derive(Clone, Debug)]
struct HtmlScanner {
    state: HtmlState,
}

#[derive(Clone, Debug)]
enum HtmlState {
    Text,
    Comment,
    Tag { name: String, after_equals: bool },
    AttrValue { name: String, quote: Option<u8> },
}

impl HtmlScanner {
    fn new() -> Self {
        Self {
            state: HtmlState::Text,
        }
    }

    fn write_text(&mut self, text: &str, out: &mut String) {
        out.push_str(text);
        self.scan(text.as_bytes());
    }

    fn escape(&self, value: &str) -> String {
        match &self.state {
            HtmlState::AttrValue {
                name,
                quote: Some(_),
            } if is_url_attribute(name) => escape::url_attribute(value),
            _ => escape::html(value),
        }
    }

    fn scan(&mut self, bytes: &[u8]) {
        let mut index = 0;
        while index < bytes.len() {
            let mut state = std::mem::replace(&mut self.state, HtmlState::Text);
            match &mut state {
                HtmlState::Text => {
                    if bytes[index] == b'<' {
                        if bytes[index..].starts_with(b"<!--") {
                            state = HtmlState::Comment;
                            index += 4;
                            continue;
                        }
                        state = HtmlState::Tag {
                            name: String::new(),
                            after_equals: false,
                        };
                    }
                }
                HtmlState::Comment => {
                    if bytes[index..].starts_with(b"-->") {
                        state = HtmlState::Text;
                        index += 3;
                        continue;
                    }
                }
                HtmlState::Tag { name, after_equals } => {
                    let byte = bytes[index];
                    if byte == b'>' {
                        state = HtmlState::Text;
                    } else if byte == b'=' {
                        *after_equals = true;
                    } else if *after_equals && (byte == b'"' || byte == b'\'') {
                        state = HtmlState::AttrValue {
                            name: std::mem::take(name),
                            quote: Some(byte),
                        };
                    } else if is_space(byte) {
                        if *after_equals {
                            state = HtmlState::AttrValue {
                                name: std::mem::take(name),
                                quote: None,
                            };
                        } else {
                            name.clear();
                        }
                    } else if *after_equals {
                        state = HtmlState::AttrValue {
                            name: std::mem::take(name),
                            quote: None,
                        };
                    } else {
                        name.push((byte as char).to_ascii_lowercase());
                    }
                }
                HtmlState::AttrValue { quote, .. } => match quote {
                    Some(quote) if bytes[index] == *quote => {
                        state = HtmlState::Tag {
                            name: String::new(),
                            after_equals: false,
                        };
                    }
                    None if is_space(bytes[index]) => {
                        state = HtmlState::Tag {
                            name: String::new(),
                            after_equals: false,
                        };
                    }
                    None if bytes[index] == b'>' => state = HtmlState::Text,
                    _ => {}
                },
            }
            self.state = state;
            index += 1;
        }
    }
}

/// `html/template`'s `attrTypeMap` entries with `contentTypeURL`.
fn is_url_attribute(name: &str) -> bool {
    matches!(
        name,
        "action"
            | "archive"
            | "background"
            | "cite"
            | "classid"
            | "codebase"
            | "data"
            | "formaction"
            | "href"
            | "icon"
            | "longdesc"
            | "manifest"
            | "poster"
            | "profile"
            | "src"
            | "usemap"
            | "xmlns"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn render(template: &str, mode: TemplateMode, data: Value) -> String {
        Template::parse(template, mode)
            .expect("parse")
            .execute(&data)
            .expect("execute")
    }

    #[test]
    fn trims_white_space_like_go() {
        assert_eq!(
            render(
                "A \n\t {{- .V -}} \n\t C",
                TemplateMode::Text,
                serde_json::json!({"V": "B"})
            ),
            "ABC"
        );
        assert_eq!(
            render(
                "{{- if .A -}}\nx\n{{- else -}}\ny\n{{- end -}}\n",
                TemplateMode::Text,
                serde_json::json!({"A": false})
            ),
            "y"
        );
    }

    #[test]
    fn renders_ranges_and_root_fields() {
        let data = serde_json::json!({
            "Name": "root",
            "Items": [{"Name": "a", "N": 1}, {"Name": "b", "N": 2}]
        });
        assert_eq!(
            render(
                "{{- range .Items }}[{{.Name}}={{.N}}]{{- end }}{{$.Name}}",
                TemplateMode::Text,
                data
            ),
            "[a=1][b=2]root"
        );
    }

    #[test]
    fn escapes_html_and_url_attributes() {
        let data = serde_json::json!({
            "Text": "a<b>&c+d\"e'f",
            "URL": "https://example.com/s?q=a&b=c+d",
            "Bad": "javascript:alert(1)"
        });
        assert_eq!(
            render(
                r#"<p>{{.Text}}</p><a href="{{.URL}}">u</a><a href="{{.Bad}}">b</a>"#,
                TemplateMode::Html,
                data
            ),
            "<p>a&lt;b&gt;&amp;c&#43;d&#34;e&#39;f</p>\
             <a href=\"https://example.com/s?q=a&amp;b=c&#43;d\">u</a>\
             <a href=\"#ZgotmplZ\">b</a>"
        );
    }

    #[test]
    fn rejects_unsupported_constructs() {
        for source in [
            "{{.X | printf}}",
            "{{ template \"x\" }}",
            "{{ with .X }}{{ end }}",
        ] {
            assert!(matches!(
                Template::parse(source, TemplateMode::Text),
                Err(TemplateError::Unsupported(_))
            ));
        }
    }
}
