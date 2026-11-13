#![forbid(unsafe_code)]

//! A small, byte-compatible subset of [chroma v2] as used by
//! `internal/markdown.Render` (`goldmark-highlighting` +
//! `htmlutil.SyntaxHighlightingOptions`).
//!
//! The Go renderer tokenises a fenced code block whose info string names a
//! language chroma knows, runs the tokens through `chroma.Coalesce`, splits
//! them into lines (`chroma.SplitTokensIntoLines`) and formats them with the
//! HTML formatter configured with `WithClasses(true)` and (optionally)
//! `WithLineNumbers`. Sourcegraph then patches every `chroma.StandardTypes`
//! entry with a `chroma-` prefix (`internal/htmlutil/syntax.go`) and sanitizes
//! the result with the bluemonday policy from `internal/htmlutil/policy.go`.
//!
//! This module reproduces that pipeline for the five lexers reachable from the
//! differential corpus: Go, Python, JavaScript, SQL and Bash. It emits the
//! *sanitized* form directly (e.g. the classless `<span>` that the policy
//! leaves behind for `Text` tokens), which is idempotent under the frontend's
//! own sanitizer.
//!
//! ## Deliberate simplifications
//!
//! - Only the five lexers above are implemented; any other language returns
//!   `None` and the caller falls back to goldmark's plain
//!   `<pre><code class="language-X">` output. (The Go baseline would highlight
//!   other known languages.)
//! - Go raw strings delegate their body to the Go text/template lexer in the
//!   Go baseline; here the body is a single `LiteralString` token.
//! - Bash here-documents (`<<EOF ... EOF`) are not tokenised as a single string
//!   token.
//! - Unicode details of `\b`/`\s` follow the `regex` crate; chroma uses
//!   `regexp2` (.NET semantics). The corpus is ASCII.
//!
//! [chroma v2]: https://github.com/alecthomas/chroma

use std::collections::HashMap;
use std::sync::OnceLock;

use regex::Regex;

// ---------------------------------------------------------------------------
// Public surface
// ---------------------------------------------------------------------------

/// `linenos` attribute state for a fenced code block.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LineNumbers {
    Off,
    Inline,
    Table,
}

/// The language aliases this module can highlight (`lexers.Get`).
pub fn is_known_language(language: &str) -> bool {
    lexer_for(language).is_some()
}

/// Highlight `code` as `language`, returning the sanitized HTML, or `None`
/// when the language is not one of the ported lexers.
pub fn highlight(
    language: &str,
    line_numbers: LineNumbers,
    line_start: usize,
    code: &str,
) -> Option<String> {
    let lexer = lexer_for(language)?;
    let tokens = coalesce(tokenise(lexer, code, false));
    Some(format_tokens(tokens, line_numbers, line_start))
}

/// Parse the fenced-info attribute list the way `goldmark-highlighting` does
/// and return the requested line-number mode.
///
/// Bare attributes (`{linenos}`) make goldmark's `ParseAttributes` fail
/// entirely, so they never enable line numbers.
pub fn line_numbers_from_info(info: &str) -> LineNumbers {
    let Some(attrs) = info_attributes(info) else {
        return LineNumbers::Off;
    };
    match attrs.get("linenos") {
        Some(AttributeValue::Bool(true)) => LineNumbers::Inline,
        Some(AttributeValue::Str(value)) => {
            if value == "table" {
                LineNumbers::Table
            } else {
                LineNumbers::Inline
            }
        }
        _ => LineNumbers::Off,
    }
}

/// `{nohl}` (with any value) disables highlighting: goldmark falls back to a
/// plain `<pre><code>` block even for a known language.
pub fn is_no_highlight(info: &str) -> bool {
    info_attributes(info).is_some_and(|attrs| attrs.contains_key("nohl"))
}

/// `linenostart` from the info attributes (default 1).
pub fn line_numbers_start_from_info(info: &str) -> usize {
    let Some(attrs) = info_attributes(info) else {
        return 1;
    };
    match attrs.get("linenostart") {
        Some(AttributeValue::Num(value)) if *value >= 0.0 => *value as usize,
        _ => 1,
    }
}

fn info_attributes(info: &str) -> Option<HashMap<String, AttributeValue>> {
    let start = info.find('{')?;
    if start == 0 {
        return None;
    }
    parse_attributes(&info[start..])
}

// ---------------------------------------------------------------------------
// goldmark attribute parser (`parser.ParseAttributes`)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
#[allow(dead_code)] // array values are parsed but only `hl_lines` would read them
enum AttributeValue {
    Bool(bool),
    Null,
    Num(f64),
    Str(String),
    Array(Vec<AttributeValue>),
}

/// Parse a `{...}` attribute list. Any syntax error makes the whole list
/// invalid (goldmark resets its reader and reports "no attributes").
fn parse_attributes(input: &str) -> Option<HashMap<String, AttributeValue>> {
    let chars: Vec<char> = input.chars().collect();
    let mut pos = 0usize;
    let mut attrs = HashMap::new();
    skip_spaces(&chars, &mut pos);
    if chars.get(pos) != Some(&'{') {
        return None;
    }
    pos += 1;
    loop {
        if chars.get(pos) == Some(&'}') {
            return Some(attrs);
        }
        let (name, value) = parse_attribute(&chars, &mut pos)?;
        if name == "class" {
            let AttributeValue::Str(value) = value else {
                return None;
            };
            let entry = attrs
                .entry("class".to_string())
                .or_insert_with(|| AttributeValue::Str(String::new()));
            if let AttributeValue::Str(existing) = entry {
                if existing.is_empty() {
                    *existing = value;
                } else {
                    existing.push(' ');
                    existing.push_str(&value);
                }
            }
        } else {
            attrs.insert(name, value);
        }
        skip_spaces(&chars, &mut pos);
        if chars.get(pos) == Some(&',') {
            pos += 1;
            skip_spaces(&chars, &mut pos);
        }
    }
}

fn skip_spaces(chars: &[char], pos: &mut usize) {
    while matches!(chars.get(*pos), Some(c) if c.is_whitespace()) {
        *pos += 1;
    }
}

fn parse_attribute(chars: &[char], pos: &mut usize) -> Option<(String, AttributeValue)> {
    skip_spaces(chars, pos);
    let first = *chars.get(*pos)?;
    if first == '#' || first == '.' {
        *pos += 1;
        let name = if first == '#' { "id" } else { "class" };
        let start = *pos;
        while let Some(&c) = chars.get(*pos) {
            if c.is_whitespace() || !is_attr_name_char(c) {
                break;
            }
            *pos += 1;
        }
        return Some((
            name.to_string(),
            AttributeValue::Str(chars[start..*pos].iter().collect()),
        ));
    }
    if !is_attr_name_start(first) {
        return None;
    }
    let start = *pos;
    while let Some(&c) = chars.get(*pos) {
        if !is_attr_name_char(c) {
            break;
        }
        *pos += 1;
    }
    let name: String = chars[start..*pos].iter().collect();
    skip_spaces(chars, pos);
    if chars.get(*pos) != Some(&'=') {
        return None;
    }
    *pos += 1;
    skip_spaces(chars, pos);
    let value = parse_attribute_value(chars, pos)?;
    Some((name, value))
}

fn is_attr_name_start(c: char) -> bool {
    c.is_ascii_alphabetic() || c == '_' || c == ':'
}

fn is_attr_name_char(c: char) -> bool {
    is_attr_name_start(c) || c.is_ascii_digit() || matches!(c, '-' | '.')
}

fn parse_attribute_value(chars: &[char], pos: &mut usize) -> Option<AttributeValue> {
    skip_spaces(chars, pos);
    match chars.get(*pos)? {
        '{' => {
            let rest: String = chars[*pos..].iter().collect();
            let _nested = parse_attributes(&rest)?;
            let mut depth = 0usize;
            while let Some(&c) = chars.get(*pos) {
                match c {
                    '{' => depth += 1,
                    '}' => {
                        depth = depth.saturating_sub(1);
                        if depth == 0 {
                            *pos += 1;
                            break;
                        }
                    }
                    _ => {}
                }
                *pos += 1;
            }
            Some(AttributeValue::Str(String::new()))
        }
        '[' => {
            *pos += 1;
            let mut values = Vec::new();
            loop {
                let c = *chars.get(*pos)?;
                if c == ']' {
                    *pos += 1;
                    return Some(AttributeValue::Array(values));
                }
                let value = parse_attribute_value(chars, pos)?;
                values.push(value);
                skip_spaces(chars, pos);
                if chars.get(*pos) == Some(&',') {
                    *pos += 1;
                    skip_spaces(chars, pos);
                }
            }
        }
        '"' => {
            *pos += 1;
            let mut value = String::new();
            while let Some(&c) = chars.get(*pos) {
                if c == '"' {
                    *pos += 1;
                    return Some(AttributeValue::Str(value));
                }
                if c == '\\' {
                    if let Some(&next) = chars.get(*pos + 1) {
                        match next {
                            '"' | '/' | '\\' => value.push(next),
                            'b' => value.push('\u{0008}'),
                            'f' => value.push('\u{000c}'),
                            'n' => value.push('\n'),
                            'r' => value.push('\r'),
                            't' => value.push('\t'),
                            _ => {
                                value.push('\\');
                                *pos += 1;
                                continue;
                            }
                        }
                        *pos += 2;
                        continue;
                    }
                }
                value.push(c);
                *pos += 1;
            }
            None
        }
        c if *c == '-' || *c == '+' || c.is_ascii_digit() => {
            let mut number = String::new();
            let mut sign = 1.0;
            if *c == '-' {
                sign = -1.0;
                *pos += 1;
            } else if *c == '+' {
                *pos += 1;
            }
            if !chars.get(*pos)?.is_ascii_digit() {
                return None;
            }
            while let Some(&d) = chars.get(*pos) {
                if d.is_ascii_digit() {
                    number.push(d);
                    *pos += 1;
                } else {
                    break;
                }
            }
            if chars.get(*pos) == Some(&'.') {
                number.push('.');
                *pos += 1;
                while let Some(&d) = chars.get(*pos) {
                    if d.is_ascii_digit() {
                        number.push(d);
                        *pos += 1;
                    } else {
                        break;
                    }
                }
            }
            if matches!(chars.get(*pos), Some('e' | 'E')) {
                number.push(*chars.get(*pos).unwrap());
                *pos += 1;
                if matches!(chars.get(*pos), Some('-' | '+')) {
                    number.push(*chars.get(*pos).unwrap());
                    *pos += 1;
                }
                while let Some(&d) = chars.get(*pos) {
                    if d.is_ascii_digit() {
                        number.push(d);
                        *pos += 1;
                    } else {
                        break;
                    }
                }
            }
            let value: f64 = number.parse().ok()?;
            Some(AttributeValue::Num(sign * value))
        }
        c if is_attr_name_start(*c) => {
            let start = *pos;
            while let Some(&c) = chars.get(*pos) {
                if !is_attr_name_char(c) {
                    break;
                }
                *pos += 1;
            }
            let word: String = chars[start..*pos].iter().collect();
            match word.as_str() {
                "true" => Some(AttributeValue::Bool(true)),
                "false" => Some(AttributeValue::Bool(false)),
                "null" => Some(AttributeValue::Null),
                _ => Some(AttributeValue::Str(word)),
            }
        }
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// Lexer engine
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
struct Token {
    ty: TokenType,
    value: String,
}

#[derive(Debug, Clone)]
struct Rule {
    re: Regex,
    emit: Emit,
    mutation: Mutation,
    line_start: bool,
    text_start: bool,
    word_start: bool,
    prev_not: Option<char>,
    next: Next,
}

#[derive(Debug, Clone)]
enum Emit {
    None,
    One(TokenType),
    Groups(Vec<GroupEmitter>),
}

#[derive(Debug, Clone)]
enum GroupEmitter {
    Type(TokenType),
    /// `UsingSelf("root")`: re-lex the group text with this lexer from root.
    SelfRoot,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Mutation {
    None,
    Push(&'static str),
    Pop(usize),
    /// `Push("#pop", state)`: pop one state, then push `state` (JS bad regex).
    Replace(&'static str),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Next {
    Any,
    /// `(?= |$)`.
    SpaceOrEol,
    /// `(?! )`.
    NotSpace,
    /// `(?=[\s)`])` for the Bash builtins.
    BuiltinBoundary,
    /// `(?=\n)`.
    Newline,
    /// `^(?=\s|/|<!--)` (zero-width; paired with `line_start`).
    SlashOrComment,
    /// `(?=/)` (zero-width).
    Slash,
}

struct Lexer {
    states: HashMap<&'static str, Vec<Rule>>,
    ensure_nl: bool,
}

fn lexer_for(language: &str) -> Option<&'static Lexer> {
    match language.to_ascii_lowercase().as_str() {
        "go" | "golang" => Some(go_lexer()),
        "python" | "py" | "python3" | "py3" | "sage" => Some(python_lexer()),
        "js" | "javascript" => Some(js_lexer()),
        "sql" => Some(sql_lexer()),
        "sh" | "bash" | "ksh" | "zsh" | "shell" => Some(bash_lexer()),
        _ => None,
    }
}

fn go_lexer() -> &'static Lexer {
    static LEXER: OnceLock<Lexer> = OnceLock::new();
    LEXER.get_or_init(build_go)
}

fn python_lexer() -> &'static Lexer {
    static LEXER: OnceLock<Lexer> = OnceLock::new();
    LEXER.get_or_init(build_python)
}

fn js_lexer() -> &'static Lexer {
    static LEXER: OnceLock<Lexer> = OnceLock::new();
    LEXER.get_or_init(build_js)
}

fn sql_lexer() -> &'static Lexer {
    static LEXER: OnceLock<Lexer> = OnceLock::new();
    LEXER.get_or_init(build_sql)
}

fn bash_lexer() -> &'static Lexer {
    static LEXER: OnceLock<Lexer> = OnceLock::new();
    LEXER.get_or_init(build_bash)
}

fn tokenise(lexer: &Lexer, text: &str, nested: bool) -> Vec<Token> {
    // `regexp.go` Tokenise: EnsureLF, then EnsureNL for non-nested lexing.
    let mut text = text.replace("\r\n", "\n").replace('\r', "\n");
    let mut newline_added = false;
    if !nested && lexer.ensure_nl && !text.ends_with('\n') {
        text.push('\n');
        newline_added = true;
    }
    let chars: Vec<char> = text.chars().collect();
    let byte_offsets = char_byte_offsets(&text);
    let end = if newline_added {
        chars.len().saturating_sub(1)
    } else {
        chars.len()
    };
    let mut pos = 0usize;
    let mut stack: Vec<&'static str> = vec!["root"];
    let mut out: Vec<Token> = Vec::new();
    while pos < end && !stack.is_empty() {
        let state = *stack.last().unwrap();
        let rules = lexer
            .states
            .get(state)
            .unwrap_or_else(|| panic!("unknown state {state}"));
        let hay = &text[byte_offsets[pos]..];
        let mut matched = false;
        for rule in rules {
            if let Some(len) = try_match(rule, &chars, pos, hay) {
                matched = true;
                let value: String = chars[pos..pos + len].iter().collect();
                emit_groups(lexer, rule, hay, &value, &mut out);
                pos += len;
                apply_mutation(&mut stack, rule.mutation);
                break;
            }
        }
        if matched {
            continue;
        }
        if chars[pos] == '\n' && state != "root" {
            // Pygments behaviour: an error on a newline resets to root.
            stack = vec!["root"];
            continue;
        }
        out.push(Token {
            ty: TokenType::Error,
            value: chars[pos].to_string(),
        });
        pos += 1;
    }
    out
}

fn char_byte_offsets(text: &str) -> Vec<usize> {
    let mut offsets: Vec<usize> = text.char_indices().map(|(idx, _)| idx).collect();
    offsets.push(text.len());
    offsets
}

fn try_match(rule: &Rule, chars: &[char], pos: usize, hay: &str) -> Option<usize> {
    if rule.text_start && pos != 0 {
        return None;
    }
    if rule.line_start && !(pos == 0 || chars[pos - 1] == '\n') {
        return None;
    }
    if rule.word_start && pos > 0 && is_word_char(chars[pos - 1]) {
        return None;
    }
    if let Some(prev_not) = rule.prev_not {
        if pos > 0 && chars[pos - 1] == prev_not {
            return None;
        }
    }
    if rule.next == Next::SlashOrComment {
        if !(pos == 0 || chars[pos - 1] == '\n') {
            return None;
        }
        let next = chars.get(pos).copied();
        let rest: String = chars[pos..].iter().take(4).collect();
        let ok = next.is_none()
            || next.is_some_and(char::is_whitespace)
            || next == Some('/')
            || rest.starts_with("<!--");
        return ok.then_some(0);
    }
    if rule.next == Next::Slash {
        return (chars.get(pos) == Some(&'/')).then_some(0);
    }
    let found = rule.re.find(hay)?;
    if found.start() != 0 {
        return None;
    }
    let len = hay[..found.end()].chars().count();
    let next = chars.get(pos + len).copied();
    let next_ok = match rule.next {
        Next::Any => true,
        Next::SpaceOrEol => next.is_none() || next == Some(' ') || next == Some('\n'),
        Next::NotSpace => next != Some(' '),
        Next::BuiltinBoundary => {
            next.is_none()
                || next.is_some_and(|c| c.is_whitespace() || matches!(c, ')' | '`' | ']'))
        }
        Next::Newline => next == Some('\n'),
        Next::SlashOrComment | Next::Slash => unreachable!(),
    };
    next_ok.then_some(len)
}

fn is_word_char(c: char) -> bool {
    c.is_alphanumeric() || c == '_'
}

fn apply_mutation(stack: &mut Vec<&'static str>, mutation: Mutation) {
    match mutation {
        Mutation::None => {}
        Mutation::Push(state) => stack.push(state),
        Mutation::Pop(depth) => {
            for _ in 0..depth {
                stack.pop();
            }
        }
        Mutation::Replace(state) => {
            stack.pop();
            stack.push(state);
        }
    }
}

fn emit_groups(
    lexer: &Lexer,
    rule: &Rule,
    hay: &str,
    value: &str,
    out: &mut Vec<Token>,
) {
    match &rule.emit {
        Emit::None => {}
        Emit::One(ty) => out.push(Token {
            ty: *ty,
            value: value.to_string(),
        }),
        Emit::Groups(emitters) => {
            let Some(caps) = rule.re.captures(hay) else {
                return;
            };
            for (index, emitter) in emitters.iter().enumerate() {
                let group = caps.get(index + 1).map(|m| m.as_str()).unwrap_or("");
                match emitter {
                    GroupEmitter::Type(ty) => out.push(Token {
                        ty: *ty,
                        value: group.to_string(),
                    }),
                    GroupEmitter::SelfRoot => out.extend(tokenise(lexer, group, true)),
                }
            }
        }
    }
}

fn coalesce(tokens: Vec<Token>) -> Vec<Token> {
    let mut out: Vec<Token> = Vec::new();
    for token in tokens {
        if token.value.is_empty() {
            continue;
        }
        match out.last_mut() {
            Some(prev) if prev.ty == token.ty && prev.value.len() < 8192 => {
                prev.value.push_str(&token.value);
            }
            _ => out.push(token),
        }
    }
    out
}

fn split_lines(tokens: Vec<Token>) -> Vec<Vec<Token>> {
    let mut lines: Vec<Vec<Token>> = Vec::new();
    let mut line: Vec<Token> = Vec::new();
    for mut token in tokens {
        while let Some(idx) = token.value.find('\n') {
            let tail = token.value.split_off(idx + 1);
            let head = std::mem::replace(&mut token.value, tail);
            line.push(Token {
                ty: token.ty,
                value: head,
            });
            lines.push(std::mem::take(&mut line));
        }
        line.push(token);
    }
    if !line.is_empty() {
        lines.push(line);
    }
    if let Some(last) = lines.last() {
        if last.len() == 1 && last[0].value.is_empty() {
            lines.pop();
        }
    }
    lines
}

fn format_tokens(tokens: Vec<Token>, line_numbers: LineNumbers, line_start: usize) -> String {
    let lines = split_lines(tokens);
    let last_number = if lines.is_empty() {
        line_start.saturating_sub(1)
    } else {
        line_start + lines.len() - 1
    };
    let width = last_number.to_string().len();
    let mut out = String::new();
    if line_numbers == LineNumbers::Table {
        out.push_str("<div>\n<table><tr><td>\n<pre class=\"chroma\">");
        for index in 0..lines.len() {
            out.push_str("<span class=\"chroma-lnt\">");
            out.push_str(&format!("{:>width$}", line_start + index, width = width));
            out.push_str("\n</span>");
        }
        out.push_str("</pre></td>\n<td>\n");
    }
    out.push_str("<pre class=\"chroma\"><code>");
    for (index, line) in lines.iter().enumerate() {
        out.push_str("<span class=\"chroma-line\">");
        if line_numbers == LineNumbers::Inline {
            out.push_str("<span class=\"chroma-ln\">");
            out.push_str(&format!("{:>width$}", line_start + index, width = width));
            out.push_str("</span>");
        }
        out.push_str("<span class=\"chroma-cl\">");
        for token in line {
            let class = token.ty.class();
            if class.is_empty() {
                out.push_str("<span>");
            } else {
                out.push_str("<span class=\"chroma-");
                out.push_str(class);
                out.push_str("\">");
            }
            out.push_str(&escape_html(&token.value));
            out.push_str("</span>");
        }
        out.push_str("</span></span>");
    }
    out.push_str("</code></pre>");
    if line_numbers == LineNumbers::Table {
        out.push_str("</td></tr></table>\n</div>\n");
    }
    out
}

fn escape_html(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for c in value.chars() {
        match c {
            '&' => out.push_str("&amp;"),
            '\'' => out.push_str("&#39;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&#34;"),
            _ => out.push(c),
        }
    }
    out
}

// ---------------------------------------------------------------------------
// Token types and CSS classes (`chroma.StandardTypes` after
// `htmlutil.patchChromaTypes`; the `chroma-` prefix is added by the formatter)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TokenType {
    Text,
    TextWhitespace,
    Error,
    Keyword,
    KeywordConstant,
    KeywordDeclaration,
    KeywordNamespace,
    KeywordReserved,
    KeywordType,
    Name,
    NameBuiltin,
    NameBuiltinPseudo,
    NameClass,
    NameDecorator,
    NameException,
    NameFunction,
    NameFunctionMagic,
    NameNamespace,
    NameOther,
    NameVariable,
    NameVariableMagic,
    Str,
    StrAffix,
    StrBacktick,
    StrChar,
    StrDoc,
    StrDouble,
    StrEscape,
    StrInterpol,
    StrSingle,
    StrRegex,
    Number,
    NumberBin,
    NumberFloat,
    NumberHex,
    NumberInteger,
    NumberOct,
    Operator,
    OperatorWord,
    Punctuation,
    Comment,
    CommentHashbang,
    CommentMultiline,
    CommentPreproc,
    CommentSingle,
}

impl TokenType {
    /// Class suffix, or `""` for `Text` (whose patched class `chroma-` is
    /// stripped by the sanitizer, leaving a classless `<span>`).
    fn class(self) -> &'static str {
        match self {
            TokenType::Text => "",
            TokenType::TextWhitespace => "w",
            TokenType::Error => "err",
            TokenType::Keyword => "k",
            TokenType::KeywordConstant => "kc",
            TokenType::KeywordDeclaration => "kd",
            TokenType::KeywordNamespace => "kn",
            TokenType::KeywordReserved => "kr",
            TokenType::KeywordType => "kt",
            TokenType::Name => "n",
            TokenType::NameBuiltin => "nb",
            TokenType::NameBuiltinPseudo => "bp",
            TokenType::NameClass => "nc",
            TokenType::NameDecorator => "nd",
            TokenType::NameException => "ne",
            TokenType::NameFunction => "nf",
            TokenType::NameFunctionMagic => "fm",
            TokenType::NameNamespace => "nn",
            TokenType::NameOther => "nx",
            TokenType::NameVariable => "nv",
            TokenType::NameVariableMagic => "vm",
            TokenType::Str => "s",
            TokenType::StrAffix => "sa",
            TokenType::StrBacktick => "sb",
            TokenType::StrChar => "sc",
            TokenType::StrDoc => "sd",
            TokenType::StrDouble => "s2",
            TokenType::StrEscape => "se",
            TokenType::StrInterpol => "si",
            TokenType::StrSingle => "s1",
            TokenType::StrRegex => "sr",
            TokenType::Number => "m",
            TokenType::NumberBin => "mb",
            TokenType::NumberFloat => "mf",
            TokenType::NumberHex => "mh",
            TokenType::NumberInteger => "mi",
            TokenType::NumberOct => "mo",
            TokenType::Operator => "o",
            TokenType::OperatorWord => "ow",
            TokenType::Punctuation => "p",
            TokenType::Comment => "c",
            TokenType::CommentHashbang => "ch",
            TokenType::CommentMultiline => "cm",
            TokenType::CommentPreproc => "cp",
            TokenType::CommentSingle => "c1",
        }
    }
}

// ---------------------------------------------------------------------------
// Rule builders
// ---------------------------------------------------------------------------

struct Builder {
    states: HashMap<&'static str, Vec<Rule>>,
    multiline: bool,
    ensure_nl: bool,
}

impl Builder {
    fn new(multiline: bool, ensure_nl: bool) -> Self {
        Self {
            states: HashMap::new(),
            multiline,
            ensure_nl,
        }
    }

    fn compile(&self, pattern: &str, ci: bool, dotall: bool) -> Regex {
        let mut flags = String::new();
        if self.multiline {
            flags.push('m');
        }
        if ci {
            flags.push('i');
        }
        if dotall {
            flags.push('s');
        }
        let pattern = if flags.is_empty() {
            pattern.to_string()
        } else {
            format!("(?{flags}){pattern}")
        };
        Regex::new(&pattern).unwrap_or_else(|error| panic!("bad lexer regex {pattern}: {error}"))
    }

    fn add(&mut self, state: &'static str, rule: Rule) {
        self.states.entry(state).or_default().push(rule);
    }

    fn add_all(&mut self, state: &'static str, rules: Vec<Rule>) {
        self.states.entry(state).or_default().extend(rules);
    }

    fn include(&self, state: &'static str) -> Vec<Rule> {
        self.states.get(state).cloned().unwrap_or_default()
    }

    fn finish(self) -> Lexer {
        Lexer {
            states: self.states,
            ensure_nl: self.ensure_nl,
        }
    }
}

fn rule(builder: &Builder, pattern: &str, ty: TokenType) -> Rule {
    Rule {
        re: builder.compile(pattern, false, false),
        emit: Emit::One(ty),
        mutation: Mutation::None,
        line_start: false,
        text_start: false,
        word_start: false,
        prev_not: None,
        next: Next::Any,
    }
}

fn rule_ci(builder: &Builder, pattern: &str, ty: TokenType) -> Rule {
    Rule {
        re: builder.compile(pattern, true, false),
        ..rule(builder, "", ty)
    }
}

fn rule_dotall(builder: &Builder, pattern: &str, ty: TokenType) -> Rule {
    Rule {
        re: builder.compile(pattern, false, true),
        ..rule(builder, "", ty)
    }
}

fn rule_groups(builder: &Builder, pattern: &str, parts: Vec<GroupEmitter>) -> Rule {
    Rule {
        re: builder.compile(pattern, false, false),
        emit: Emit::Groups(parts),
        mutation: Mutation::None,
        line_start: false,
        text_start: false,
        word_start: false,
        prev_not: None,
        next: Next::Any,
    }
}

fn rule_groups_ci(builder: &Builder, pattern: &str, parts: Vec<GroupEmitter>) -> Rule {
    Rule {
        re: builder.compile(pattern, true, false),
        ..rule_groups(builder, "", parts)
    }
}

fn no_emit(mut rule: Rule) -> Rule {
    rule.emit = Emit::None;
    rule
}

fn push(mut rule: Rule, state: &'static str) -> Rule {
    rule.mutation = Mutation::Push(state);
    rule
}

fn pop(mut rule: Rule, depth: usize) -> Rule {
    rule.mutation = Mutation::Pop(depth);
    rule
}

fn replace(mut rule: Rule, state: &'static str) -> Rule {
    rule.mutation = Mutation::Replace(state);
    rule
}

fn at_line_start(mut rule: Rule) -> Rule {
    rule.line_start = true;
    rule
}

fn at_text_start(mut rule: Rule) -> Rule {
    rule.text_start = true;
    rule
}

fn word_start(mut rule: Rule) -> Rule {
    rule.word_start = true;
    rule
}

fn not_after(mut rule: Rule, c: char) -> Rule {
    rule.prev_not = Some(c);
    rule
}

fn followed_by(mut rule: Rule, next: Next) -> Rule {
    rule.next = next;
    rule
}

fn plain_group(ty: TokenType) -> GroupEmitter {
    GroupEmitter::Type(ty)
}

/// `<rule><pop depth="1"/></rule>`: matches the empty string and pops.
fn pop_empty(builder: &Builder) -> Rule {
    no_emit(pop(rule(builder, "", TokenType::Text), 1))
}

// ---------------------------------------------------------------------------
// Go (`lexers/go.go`, EnsureNL)
// ---------------------------------------------------------------------------

const GO_BUILTINS: &[&str] = &[
    "uint", "uint8", "uint16", "uint32", "uint64", "int", "int8", "int16", "int32", "int64",
    "float", "float32", "float64", "complex64", "complex128", "byte", "rune", "string", "bool",
    "error", "uintptr", "print", "println", "panic", "recover", "close", "complex", "real", "imag",
    "len", "cap", "append", "copy", "delete", "new", "make", "clear", "min", "max",
];

const GO_TYPES: &[&str] = &[
    "uint", "uint8", "uint16", "uint32", "uint64", "int", "int8", "int16", "int32", "int64",
    "float", "float32", "float64", "complex64", "complex128", "byte", "rune", "string", "bool",
    "error", "uintptr",
];

fn word_alternation(words: &[&str]) -> String {
    // `chroma.Words` sorts longest-first. With a trailing `\b` the order is
    // semantically irrelevant, but this keeps the patterns comparable to the
    // upstream lexers.
    let mut sorted: Vec<&str> = words.to_vec();
    sorted.sort_by_key(|word| std::cmp::Reverse(word.len()));
    sorted.join("|")
}

fn build_go() -> Lexer {
    let mut builder = Builder::new(true, true);
    let keywords = [
        "break", "default", "select", "case", "defer", "go", "else", "goto", "switch",
        "fallthrough", "if", "range", "continue", "for", "return",
    ];
    builder.add("root", rule(&builder, r"\n", TokenType::Text));
    builder.add("root", rule(&builder, r"\s+", TokenType::Text));
    builder.add("root", rule(&builder, r"\\\n", TokenType::Text));
    builder.add(
        "root",
        rule(&builder, r"//[^\n]*\n", TokenType::CommentSingle),
    );
    builder.add(
        "root",
        rule_dotall(&builder, r"/\*.*?\*/", TokenType::CommentMultiline),
    );
    builder.add(
        "root",
        rule(&builder, r"(import|package)\b", TokenType::KeywordNamespace),
    );
    builder.add(
        "root",
        rule(
            &builder,
            r"(var|func|struct|map|chan|type|interface|const)\b",
            TokenType::KeywordDeclaration,
        ),
    );
    builder.add(
        "root",
        rule(
            &builder,
            &format!(r"({})\b", keywords.join("|")),
            TokenType::Keyword,
        ),
    );
    builder.add(
        "root",
        rule(
            &builder,
            r"(true|false|iota|nil)\b",
            TokenType::KeywordConstant,
        ),
    );
    builder.add(
        "root",
        rule_groups(
            &builder,
            &format!(r"({})\b(\()", word_alternation(GO_BUILTINS)),
            vec![
                plain_group(TokenType::NameBuiltin),
                plain_group(TokenType::Punctuation),
            ],
        ),
    );
    builder.add(
        "root",
        rule(
            &builder,
            &format!(r"({})\b", word_alternation(GO_TYPES)),
            TokenType::KeywordType,
        ),
    );
    for (pattern, ty) in [
        (r"\d+i", TokenType::Number),
        (r"\d+\.\d*([Ee][-+]\d+)?i", TokenType::Number),
        (r"\.\d+([Ee][-+]\d+)?i", TokenType::Number),
        (r"\d+[Ee][-+]\d+i", TokenType::Number),
        (
            r"\d+(\.\d+[eE][+\-]?\d+|\.\d*|[eE][+\-]?\d+)",
            TokenType::NumberFloat,
        ),
        (r"\.\d+([eE][+\-]?\d+)?", TokenType::NumberFloat),
        (r"0[0-7]+", TokenType::NumberOct),
        (r"0[xX][0-9a-fA-F_]+", TokenType::NumberHex),
        (r"0b[01_]+", TokenType::NumberBin),
        (r"(0|[1-9][0-9_]*)", TokenType::NumberInteger),
    ] {
        builder.add("root", rule(&builder, pattern, ty));
    }
    builder.add(
        "root",
        rule(
            &builder,
            r#"'(\\['"\\abfnrtv]|\\x[0-9a-fA-F]{2}|\\[0-7]{1,3}|\\u[0-9a-fA-F]{4}|\\U[0-9a-fA-F]{8}|[^\\])'"#,
            TokenType::StrChar,
        ),
    );
    builder.add(
        "root",
        rule_groups(
            &builder,
            r"(`)([^`]*)(`)",
            vec![
                plain_group(TokenType::Str),
                plain_group(TokenType::Str),
                plain_group(TokenType::Str),
            ],
        ),
    );
    builder.add(
        "root",
        rule(&builder, r#""(\\\\|\\"|[^"])*""#, TokenType::Str),
    );
    builder.add(
        "root",
        rule(
            &builder,
            r"(<<=|>>=|<<|>>|<=|>=|&\^=|&\^|\+=|-=|\*=|/=|%=|&=|\|=|&&|\|\||<-|\+\+|--|==|!=|:=|\.\.\.|[+\-*/%&])",
            TokenType::Operator,
        ),
    );
    builder.add(
        "root",
        rule_groups(
            &builder,
            r"([a-zA-Z_]\w*)(\s*)(\()",
            vec![
                plain_group(TokenType::NameFunction),
                GroupEmitter::SelfRoot,
                plain_group(TokenType::Punctuation),
            ],
        ),
    );
    builder.add(
        "root",
        rule(&builder, r"[|^<>=!()\[\]{}.,;:]", TokenType::Punctuation),
    );
    builder.add("root", rule(&builder, r"[^\W\d]\w*", TokenType::NameOther));
    builder.finish()
}

// ---------------------------------------------------------------------------
// SQL (`lexers/embedded/sql.xml`, case-insensitive, not multiline)
// ---------------------------------------------------------------------------

const SQL_KEYWORDS: &str = "DATETIME_INTERVAL_PRECISION|PARAMETER_SPECIFIC_CATALOG|PARAMATER_ORDINAL_POSITION|USER_DEFINED_TYPE_CATALOG|PARAMATER_SPECIFIC_SCHEMA|TRANSACTIONS_ROLLED_BACK|USER_DEFINED_TYPE_SCHEMA|PARAMETER_SPECIFIC_NAME|DATETIME_INTERVAL_CODE|TRANSACTIONS_COMMITTED|USER_DEFINED_TYPE_NAME|CHARACTER_SET_CATALOG|DYNAMIC_FUNCTION_CODE|COMMAND_FUNCTION_CODE|RETURNED_OCTET_LENGTH|MESSAGE_OCTET_LENGTH|CHARACTER_SET_SCHEMA|CONSTRAINT_CATALOG|TRANSACTION_ACTIVE|CHARACTER_SET_NAME|CURRENT_TIMESTAMP|CONSTRAINT_SCHEMA|COLLATION_CATALOG|RETURNED_SQLSTATE|DYNAMIC_FUNCTION|CONDITION_NUMBER|CHARACTER_LENGTH|COMMAND_FUNCTION|COLLATION_SCHEMA|CHARACTERISTICS|TRIGGER_CATALOG|CONNECTION_NAME|SUBCLASS_ORIGIN|RETURNED_LENGTH|TIMEZONE_MINUTE|CONSTRAINT_NAME|ROUTINE_CATALOG|TRIGGER_SCHEMA|ROUTINE_SCHEMA|LOCALTIMESTAMP|IMPLEMENTATION|PARAMATER_NAME|MESSAGE_LENGTH|PARAMETER_MODE|COLLATION_NAME|TIMEZONE_HOUR|SPECIFIC_NAME|DETERMINISTIC|CORRESPONTING|AUTHORIZATION|INSTANTIABLE|CURRENT_TIME|CURRENT_USER|ROUTINE_NAME|NOCREATEUSER|MESSAGE_TEXT|SQLEXCEPTION|CATALOG_NAME|SESSION_USER|CLASS_ORIGIN|CURRENT_ROLE|SPECIFICTYPE|SERIALIZABLE|CURRENT_DATE|OCTET_LENGTH|CURRENT_PATH|TRIGGER_NAME|CHAR_LENGTH|SYSTEM_USER|REFERENCING|UNENCRYPTED|COLUMN_NAME|SQLWARNINIG|DIAGNOSTICS|CURSOR_NAME|SERVER_NAME|INSENSITIVE|SCHEMA_NAME|UNCOMMITTED|TRANSACTION|CONSTRUCTOR|LANCOMPILER|CARDINALITY|CONSTRAINTS|TRANSLATION|CHECKPOINT|CONSTRAINT|CONNECTION|PRIVILEGES|COMPLETION|CONVERSION|DELIMITERS|TABLE_NAME|INDITCATOR|INITIALIZE|DESCRIPTOR|REPEATABLE|CREATEUSER|DEFERRABLE|DESTRUCTOR|PROCEDURAL|DICTIONARY|DISCONNECT|TRANSFORMS|KEY_MEMBER|BIT_LENGTH|ASYMMETRIC|ASSIGNMENT|ASENSITIVE|OVERRIDING|PARAMETERS|REFERENCES|ORDINALITY|NOCREATEDB|STATISTICS|DEALLOCATE|SAVE_POINT|RECURSIVE|STRUCTURE|SUBSTRING|IMMEDIATE|GENERATED|SYMMETRIC|STATEMENT|INCREMENT|IMMUTABLE|INCLUDING|COMMITTED|TEMPORARY|INITIALLY|TERMINATE|PRECISION|DELIMITER|TIMESTAMP|INTERSECT|ISOLATION|TRANSFORM|TRANSLATE|ROW_COUNT|ASSERTION|PARAMETER|EXCLUSIVE|LOCALTIME|VALIDATOR|AGGREGATE|EXCLUDING|SENSITIVE|EXCEPTION|ENCRYPTED|OPERATION|HIERARCHY|COLLATION|PROCEDURE|CONTINUE|ENCODING|MINVALUE|SPECIFIC|ABSOLUTE|SECURITY|WHENEVER|EXISTING|VOLATILE|MAXVALUE|EXTERNAL|NULLABLE|VARIABLE|SQLERROR|DISTINCT|DISPATCH|END-EXEC|LOCATION|ALLOCATE|OVERLAPS|UNLISTEN|ROLLBACK|TRUNCATE|DESCRIBE|SQLSTATE|BACKWARD|FUNCTION|LANGUAGE|KEY_TYPE|CASCADED|POSITION|TRAILING|DEFERRED|RELATIVE|DEFAULTS|COALSECE|PREORDER|GROUPING|MODIFIES|INHERITS|PRESERVE|DATABASE|RESTRICT|IDENTITY|TEMPLATE|NATIONAL|CONTAINS|CREATEDB|IMPLICIT|OPERATOR|CONVERT|CURRENT|CONNECT|RECHECK|PRIMARY|STORAGE|DECLARE|DEFAULT|HANDLER|COLLATE|PREPARE|REINDEX|GRANTED|CHECKED|POSTFIX|REPLACE|INSTEAD|CATALOG|RESTART|INVOKER|PLACING|PENDANT|DEFINED|ITERATE|PARTIAL|CASCADE|BREADTH|GENERAL|TRIGGER|SESSION|BETWEEN|DEFINER|LATERAL|LEADING|RETURNS|TRUSTED|UNKNOWN|FORWARD|UNNAMED|OVERLAY|FORTRAN|ANALYZE|OPTIONS|ANALYSE|FOREIGN|ROUTINE|LOCATOR|DESTROY|SUBLIST|VERBOSE|EXTRACT|NOTNULL|EXPLAIN|VERSION|SQLCODE|EXECUTE|NOTHING|DYNAMIC|WITHOUT|SIMILAR|NATURAL|COMMENT|CLUSTER|PASCAL|SOURCE|EQUALS|CALLED|ESCAPE|EXCEPT|SELECT|ISNULL|DOMAIN|SEARCH|SCROLL|SIMPLE|BITVAR|MINUTE|EXISTS|SCHEMA|ATOMIC|METHOD|NOTIFY|ACCESS|UNIQUE|ROLLUP|NULLIF|OBJECT|STABLE|COLUMN|REVOKE|OFFSET|COMMIT|MODIFY|FREEZE|DELETE|RETURN|RESULT|UNNEST|OPTION|GLOBAL|VALUES|RENAME|SYSTEM|STATIC|UPDATE|OUTPUT|LISTEN|STDOUT|STRICT|PUBLIC|IGNORE|PREFIX|SECOND|CREATE|LENGTH|BEFORE|HAVING|INSERT|VACUUM|CURSOR|ELSIF|USING|ALTER|STYPE|CYCLE|LARGE|INPUT|CROSS|INOUT|INNER|INFIX|INDEX|LEVEL|USAGE|ILIKE|VALID|OWNER|GRANT|READS|UPPER|LIMIT|OUTER|STDIN|SYSID|GROUP|ALIAS|ORDER|UNTIL|LOCAL|RESET|START|COUNT|LOWER|TABLE| TEMP|PRIOR|AFTER|STATE|ADMIN|RIGHT|COBOL|FOUND|MATCH|FORCE|ABORT|FIRST|FINAL|CLOSE|DEREF|FETCH|WHERE|FALSE|SCALE|BEGIN|CLASS|TOAST|WRITE|NCLOB|NCHAR|CHECK|CHAIN|SPACE|NAMES|EVERY|MUMPS|CACHE|UNION|UNDER|SETOF|MONTH|SHARE|SCOPE|TREAT|SHOW|SIZE|SOME|SETS|SELF|ELSE|EACH|DROP|TYPE|FROM|RULE|DESC|ROWS|ZONE|ROLE|TRUE|FREE|FULL|GOTO|TRIM|HOLD|HOST|DATA|READ|INTO|USER|JOIN|CUBE|LAST|LEFT|LESS|LIKE|LOAD|LOCK|OPEN|COPY|ONLY|OIDS|VIEW|WHEN|THAN|THEN|NULL|NONE|WITH|WORK|NEXT|YEAR|MODE|CAST|CASE|MOVE|CALL|MORE|BOTH|EXEC|CLOB|OUT|MOD|ARE|SUM|DAY|GET|AVG|NEW|SQL|ABS|MIN|ASC|END|ROW|NOT|FOR|ANY|PLI|MAX|REF|MAP|ADA|KEY|AND|ADD|ALL|OLD|OFF|PAD|SET|OR|ON|TO|IS|OF|IN|IF|GO|AS|DO|AT|NO|BY|C|G";

const SQL_TYPES: &str = "CHARACTER|SMALLINT|INTERVAL|DECIMAL|SERIAL8|VARYING|BOOLEAN|VARCHAR|INTEGER|NUMERIC|SERIAL|BINARY|BIGINT|NUMBER|FLOAT|ARRAY|TEXT|REAL|INT8|DATE|CHAR|BLOB|DEC|BIT|INT";

fn build_sql() -> Lexer {
    let mut builder = Builder::new(false, false);
    builder.add("root", rule(&builder, r"\s+", TokenType::TextWhitespace));
    builder.add("root", rule(&builder, r"--.*\n?", TokenType::CommentSingle));
    builder.add(
        "root",
        push(
            rule(&builder, r"/\*", TokenType::CommentMultiline),
            "multiline-comments",
        ),
    );
    builder.add(
        "root",
        push(rule(&builder, r"'", TokenType::StrSingle), "string"),
    );
    builder.add(
        "root",
        push(rule(&builder, r#"""#, TokenType::StrDouble), "double-string"),
    );
    builder.add(
        "root",
        rule_ci(
            &builder,
            &format!(r"({})\b", SQL_KEYWORDS),
            TokenType::Keyword,
        ),
    );
    builder.add(
        "root",
        rule_ci(
            &builder,
            &format!(r"({})\b", SQL_TYPES),
            TokenType::NameBuiltin,
        ),
    );
    builder.add(
        "root",
        rule(&builder, r"[+*/<>=~!@#%^&|`?-]", TokenType::Operator),
    );
    builder.add(
        "root",
        rule(&builder, r"[0-9]+", TokenType::NumberInteger),
    );
    builder.add("root", rule(&builder, r"[a-z_][\w$]*", TokenType::Name));
    builder.add(
        "root",
        rule(&builder, r"[;:()\[\],.]", TokenType::Punctuation),
    );

    builder.add(
        "multiline-comments",
        push(
            rule(&builder, r"/\*", TokenType::CommentMultiline),
            "multiline-comments",
        ),
    );
    builder.add(
        "multiline-comments",
        pop(rule(&builder, r"\*/", TokenType::CommentMultiline), 1),
    );
    builder.add(
        "multiline-comments",
        rule(&builder, r"[^/*]+", TokenType::CommentMultiline),
    );
    builder.add(
        "multiline-comments",
        rule(&builder, r"[/*]", TokenType::CommentMultiline),
    );

    builder.add("string", rule(&builder, r"[^']+", TokenType::StrSingle));
    builder.add("string", rule(&builder, r"''", TokenType::StrSingle));
    builder.add("string", pop(rule(&builder, r"'", TokenType::StrSingle), 1));

    builder.add(
        "double-string",
        rule(&builder, r#"[^"]+"#, TokenType::StrDouble),
    );
    builder.add("double-string", rule(&builder, r#""""#, TokenType::StrDouble));
    builder.add(
        "double-string",
        pop(rule(&builder, r#"""#, TokenType::StrDouble), 1),
    );
    builder.finish()
}

// ---------------------------------------------------------------------------
// JavaScript (`lexers/embedded/javascript.xml`, dot_all + EnsureNL)
// ---------------------------------------------------------------------------

fn build_js() -> Lexer {
    let mut builder = Builder::new(true, true);

    // commentsandwhitespace
    let mut comments = Vec::new();
    comments.push(rule(&builder, r"\s+", TokenType::Text));
    comments.push(rule(&builder, r"<!--", TokenType::Comment));
    comments.push(rule(&builder, r"//.*?\n", TokenType::CommentSingle));
    comments.push(rule_dotall(
        &builder,
        r"/\*.*?\*/",
        TokenType::CommentMultiline,
    ));

    // slashstartsregex
    builder.add_all("slashstartsregex", comments);
    builder.add(
        "slashstartsregex",
        pop(
            rule(
                &builder,
                r"/(\\.|[^\[/\\\n]|\[(\\.|[^\]\\\n])*])+/([gimuy]+\b|\B)",
                TokenType::StrRegex,
            ),
            1,
        ),
    );
    builder.add(
        "slashstartsregex",
        replace(
            followed_by(rule(&builder, "", TokenType::Text), Next::Slash),
            "badregex",
        ),
    );
    builder.add("slashstartsregex", pop_empty(&builder));

    // badregex: only the newline rule; anything else is an error token.
    builder.add("badregex", pop(rule(&builder, r"\n", TokenType::Text), 1));

    // interp / interp-inside
    builder.add(
        "interp",
        pop(rule(&builder, r"`", TokenType::StrBacktick), 1),
    );
    builder.add("interp", rule(&builder, r"\\\\", TokenType::StrBacktick));
    builder.add("interp", rule(&builder, r"\\`", TokenType::StrBacktick));
    builder.add(
        "interp",
        rule(&builder, r"\\[^`\\]", TokenType::StrBacktick),
    );
    builder.add(
        "interp",
        push(
            rule(&builder, r"\$\{", TokenType::StrInterpol),
            "interp-inside",
        ),
    );
    builder.add("interp", rule(&builder, r"\$", TokenType::StrBacktick));
    builder.add(
        "interp",
        rule(&builder, r"[^`\\$]+", TokenType::StrBacktick),
    );

    builder.add(
        "interp-inside",
        pop(rule(&builder, r"\}", TokenType::StrInterpol), 1),
    );
    let root_rules = js_root_rules(&builder);
    builder.add_all("interp-inside", root_rules.clone());
    builder.add_all("root", root_rules);
    builder.finish()
}

fn js_root_rules(builder: &Builder) -> Vec<Rule> {
    vec![
        at_text_start(rule(builder, r"#! ?/.*?\n", TokenType::CommentHashbang)),
        replace(
            followed_by(rule(builder, "", TokenType::Text), Next::SlashOrComment),
            "slashstartsregex",
        ),
        rule(builder, r"\s+", TokenType::Text),
        rule(builder, r"<!--", TokenType::Comment),
        rule(builder, r"//.*?\n", TokenType::CommentSingle),
        rule_dotall(builder, r"/\*.*?\*/", TokenType::CommentMultiline),
        rule(
            builder,
            r"\d+(\.\d*|[eE][+\-]?\d+)",
            TokenType::NumberFloat,
        ),
        rule(builder, r"0[bB][01]+", TokenType::NumberBin),
        rule(builder, r"0[oO][0-7]+", TokenType::NumberOct),
        rule(builder, r"0[xX][0-9a-fA-F]+", TokenType::NumberHex),
        rule(builder, r"[0-9][0-9_]*", TokenType::NumberInteger),
        rule(builder, r"\.\.\.|=>", TokenType::Punctuation),
        push(
            rule(
                builder,
                r"\+\+|--|~|&&|\?|:|\|\||(<<|>>>?|==?|!=?|[-<>+*%&|^/])=?",
                TokenType::Operator,
            ),
            "slashstartsregex",
        ),
        followed_by(rule(builder, r"\\", TokenType::Operator), Next::Newline),
        push(
            rule(builder, r"[{(\[;,]", TokenType::Punctuation),
            "slashstartsregex",
        ),
        rule(builder, r"[})\].]", TokenType::Punctuation),
        push(
            rule(
                builder,
                r"(for|in|while|do|break|return|continue|switch|case|default|if|else|throw|try|catch|finally|new|delete|typeof|instanceof|void|yield|this|of)\b",
                TokenType::Keyword,
            ),
            "slashstartsregex",
        ),
        push(
            rule(
                builder,
                r"(var|let|with|function)\b",
                TokenType::KeywordDeclaration,
            ),
            "slashstartsregex",
        ),
        rule(
            builder,
            r"(abstract|async|await|boolean|byte|char|class|const|debugger|double|enum|export|extends|final|float|goto|implements|import|int|interface|long|native|package|private|protected|public|short|static|super|synchronized|throws|transient|volatile)\b",
            TokenType::KeywordReserved,
        ),
        rule(
            builder,
            r"(true|false|null|NaN|Infinity|undefined)\b",
            TokenType::KeywordConstant,
        ),
        rule(
            builder,
            r"(Array|Boolean|Date|Error|Function|Math|netscape|Number|Object|Packages|RegExp|String|Promise|Proxy|sun|decodeURI|decodeURIComponent|encodeURI|encodeURIComponent|Error|eval|isFinite|isNaN|isSafeInteger|parseFloat|parseInt|document|this|window)\b",
            TokenType::NameBuiltin,
        ),
        rule(
            builder,
            r"(?:[$_\p{L}\p{N}]|\\u[a-fA-F0-9]{4})(?:(?:[$\p{L}\p{N}]|\\u[a-fA-F0-9]{4}))*",
            TokenType::NameOther,
        ),
        rule(
            builder,
            r#""(\\\\|\\"|[^"])*""#,
            TokenType::StrDouble,
        ),
        rule(builder, r#"'(\\\\|\\'|[^'])*'"#, TokenType::StrSingle),
        push(rule(builder, r"`", TokenType::StrBacktick), "interp"),
    ]
}

// ---------------------------------------------------------------------------
// Bash (`lexers/embedded/bash.xml`, initial state `root` = basic + data +
// interp)
// ---------------------------------------------------------------------------

fn build_bash() -> Lexer {
    let mut builder = Builder::new(true, false);
    let basic = bash_basic_rules(&builder);
    let interp = bash_interp_rules(&builder);

    // data
    builder.add(
        "data",
        rule_dotall(
            &builder,
            r#"\$?"(\\\\|\\[0-7]+|\\.|[^"\\$])*""#,
            TokenType::StrDouble,
        ),
    );
    builder.add(
        "data",
        push(rule(&builder, r#"""#, TokenType::StrDouble), "string"),
    );
    builder.add(
        "data",
        rule_dotall(
            &builder,
            r#"\$'(\\\\|\\[0-7]+|\\.|[^'\\])*'"#,
            TokenType::StrSingle,
        ),
    );
    builder.add(
        "data",
        rule_dotall(&builder, r"'.*?'", TokenType::StrSingle),
    );
    builder.add("data", rule(&builder, r";", TokenType::Punctuation));
    builder.add("data", rule(&builder, r"&", TokenType::Punctuation));
    builder.add("data", rule(&builder, r"\|", TokenType::Punctuation));
    builder.add("data", rule(&builder, r"\s+", TokenType::Text));
    builder.add(
        "data",
        followed_by(rule(&builder, r"\d+", TokenType::Number), Next::SpaceOrEol),
    );
    builder.add(
        "data",
        rule(
            &builder,
            r#"[^=\s\[\]{}()$"'`\\<&|;]+"#,
            TokenType::Text,
        ),
    );
    builder.add("data", rule(&builder, r"<", TokenType::Text));

    // string
    builder.add(
        "string",
        pop(rule(&builder, r#"""#, TokenType::StrDouble), 1),
    );
    builder.add(
        "string",
        rule_dotall(
            &builder,
            r#"(\\\\|\\[0-7]+|\\.|[^"\\$])+"#,
            TokenType::StrDouble,
        ),
    );
    builder.add_all("string", interp.clone());

    // interp
    builder.add_all("interp", interp.clone());

    // paren
    builder.add("paren", pop(rule(&builder, r"\)", TokenType::Keyword), 1));
    builder.add_all("paren", basic.clone());

    // math
    builder.add("math", pop(rule(&builder, r"\)\)", TokenType::Keyword), 1));
    builder.add(
        "math",
        rule(&builder, r"[-+*/%^|&]|\*\*|\|\|", TokenType::Operator),
    );
    builder.add("math", rule(&builder, r"\d+#\d+", TokenType::Number));
    builder.add(
        "math",
        followed_by(rule(&builder, r"\d+#", TokenType::Number), Next::NotSpace),
    );
    builder.add("math", rule(&builder, r"\d+", TokenType::Number));
    builder.add_all("math", basic.clone());

    // backticks
    builder.add(
        "backticks",
        pop(rule(&builder, r"`", TokenType::StrBacktick), 1),
    );
    builder.add_all("backticks", basic.clone());

    // curly
    builder.add(
        "curly",
        pop(rule(&builder, r"\}", TokenType::StrInterpol), 1),
    );
    builder.add("curly", rule(&builder, r":-", TokenType::Keyword));
    builder.add("curly", rule(&builder, r"\w+", TokenType::NameVariable));
    builder.add(
        "curly",
        rule(&builder, r#"[^}:"'`$\\]+"#, TokenType::Punctuation),
    );
    builder.add("curly", rule(&builder, r":", TokenType::Punctuation));
    builder.add_all("curly", basic.clone());

    // root: basic, backticks, data, interp
    builder.add_all("root", basic);
    builder.add(
        "root",
        push(rule(&builder, r"`", TokenType::StrBacktick), "backticks"),
    );
    builder.add(
        "root",
        rule_dotall(
            &builder,
            r#"\$?"(\\\\|\\[0-7]+|\\.|[^"\\$])*""#,
            TokenType::StrDouble,
        ),
    );
    builder.add(
        "root",
        push(rule(&builder, r#"""#, TokenType::StrDouble), "string"),
    );
    builder.add(
        "root",
        rule_dotall(
            &builder,
            r#"\$'(\\\\|\\[0-7]+|\\.|[^'\\])*'"#,
            TokenType::StrSingle,
        ),
    );
    builder.add(
        "root",
        rule_dotall(&builder, r"'.*?'", TokenType::StrSingle),
    );
    builder.add("root", rule(&builder, r";", TokenType::Punctuation));
    builder.add("root", rule(&builder, r"&", TokenType::Punctuation));
    builder.add("root", rule(&builder, r"\|", TokenType::Punctuation));
    builder.add("root", rule(&builder, r"\s+", TokenType::Text));
    builder.add(
        "root",
        followed_by(rule(&builder, r"\d+", TokenType::Number), Next::SpaceOrEol),
    );
    builder.add(
        "root",
        rule(
            &builder,
            r#"[^=\s\[\]{}()$"'`\\<&|;]+"#,
            TokenType::Text,
        ),
    );
    builder.add("root", rule(&builder, r"<", TokenType::Text));
    builder.add_all("root", interp);

    builder.finish()
}

fn bash_interp_rules(builder: &Builder) -> Vec<Rule> {
    vec![
        push(rule(builder, r"\$\(\(", TokenType::Keyword), "math"),
        push(rule(builder, r"\$\(", TokenType::Keyword), "paren"),
        push(rule(builder, r"\$\{#?", TokenType::StrInterpol), "curly"),
        rule(builder, r"\$[a-zA-Z_]\w*", TokenType::NameVariable),
        rule(builder, r"\$(?:\d+|[#$?!_*@-])", TokenType::NameVariable),
        rule(builder, r"\$", TokenType::Text),
    ]
}

fn bash_basic_rules(builder: &Builder) -> Vec<Rule> {
    let keywords = "if|fi|else|while|do|done|for|then|return|function|case|select|continue|until|esac|elif";
    let builtins = "alias|bg|bind|break|builtin|caller|cd|command|compgen|complete|declare|dirs|disown|echo|enable|eval|exec|exit|export|false|fc|fg|getopts|hash|help|history|jobs|kill|let|local|logout|popd|printf|pushd|pwd|read|readonly|set|shift|shopt|source|suspend|test|time|times|trap|true|type|typeset|ulimit|umask|unalias|unset|wait";
    vec![
        word_start(rule_groups(
            builder,
            &format!(r"({keywords})(\s*)\b"),
            vec![
                plain_group(TokenType::Keyword),
                plain_group(TokenType::Text),
            ],
        )),
        word_start(followed_by(
            rule(builder, &format!(r"({builtins})"), TokenType::NameBuiltin),
            Next::BuiltinBoundary,
        )),
        at_text_start(rule(builder, r"#!.+\n", TokenType::CommentPreproc)),
        rule(builder, r"#.*(\S|$)", TokenType::CommentSingle),
        rule(builder, r"\\[\s\S]", TokenType::StrEscape),
        rule_groups(
            builder,
            r"(\b\w+)(\s*)(\+?=)",
            vec![
                plain_group(TokenType::NameVariable),
                plain_group(TokenType::Text),
                plain_group(TokenType::Operator),
            ],
        ),
        rule(builder, r"[\[\]{}()=]", TokenType::Operator),
        rule(builder, r"<<<", TokenType::Operator),
        rule(builder, r"&&|\|\|", TokenType::Operator),
    ]
}

// ---------------------------------------------------------------------------
// Python (`lexers/embedded/python.xml`)
// ---------------------------------------------------------------------------

const PY_KEYWORDS: &str = "yield from|nonlocal|continue|finally|except|lambda|assert|global|return|raise|yield|while|break|await|async|pass|else|elif|with|try|for|del|as|if|match|case";

const PY_BUILTINS: &str = "staticmethod|classmethod|memoryview|__import__|issubclass|isinstance|frozenset|bytearray|enumerate|reversed|property|compile|complex|delattr|hasattr|setattr|globals|getattr|divmod|filter|locals|format|object|sorted|slice|print|bytes|range|input|tuple|round|super|float|eval|list|dict|repr|type|vars|hash|next|bool|open|iter|oct|pow|min|zip|max|map|bin|len|set|any|dir|all|abs|str|sum|chr|int|hex|ord|id";

const PY_EXCEPTIONS: &str = "PendingDeprecationWarning|ConnectionAbortedError|ConnectionRefusedError|UnicodeTranslateError|ConnectionResetError|ModuleNotFoundError|NotImplementedError|FloatingPointError|StopAsyncIteration|UnicodeDecodeError|DeprecationWarning|UnicodeEncodeError|NotADirectoryError|ProcessLookupError|ZeroDivisionError|IsADirectoryError|FileNotFoundError|UnboundLocalError|KeyboardInterrupt|ChildProcessError|EnvironmentError|IndentationError|InterruptedError|BlockingIOError|ArithmeticError|ConnectionError|BrokenPipeError|FileExistsError|ResourceWarning|PermissionError|RuntimeWarning|ReferenceError|AttributeError|AssertionError|UnicodeWarning|RecursionError|StopIteration|BaseException|OverflowError|SyntaxWarning|FutureWarning|GeneratorExit|ImportWarning|UnicodeError|TimeoutError|WindowsError|RuntimeError|BytesWarning|SystemError|UserWarning|MemoryError|ImportError|LookupError|BufferError|SyntaxError|SystemExit|ValueError|IndexError|NameError|Exception|TypeError|TabError|EOFError|KeyError|VMSError|Warning|OSError|IOError";

const PY_MAGICFUNCS: &str = "__instancecheck__|__subclasscheck__|__getattribute__|__length_hint__|__rfloordiv__|__ifloordiv__|__itruediv__|__contains__|__floordiv__|__rtruediv__|__reversed__|__setitem__|__complex__|__rdivmod__|__delattr__|__rmatmul__|__ilshift__|__prepare__|__delitem__|__rrshift__|__imatmul__|__rlshift__|__setattr__|__truediv__|__getitem__|__missing__|__getattr__|__irshift__|__rshift__|__format__|__invert__|__matmul__|__divmod__|__delete__|__aenter__|__lshift__|__await__|__bytes__|__anext__|__aiter__|__aexit__|__round__|__float__|__enter__|__index__|__iadd__|__ipow__|__rpow__|__iter__|__init__|__ixor__|__rmul__|__rmod__|__imul__|__imod__|__iand__|__hash__|__rsub__|__exit__|__rxor__|__bool__|__call__|__rand__|__next__|__radd__|__isub__|__repr__|__set__|__add__|__new__|__neg__|__xor__|__and__|__mul__|__mod__|__sub__|__len__|__str__|__ror__|__ior__|__pos__|__del__|__get__|__dir__|__abs__|__int__|__pow__|__eq__|__gt__|__le__|__lt__|__ne__|__or__|__ge__";

const PY_MAGICVARS: &str = "__annotations__|__kwdefaults__|__qualname__|__objclass__|__defaults__|__closure__|__globals__|__weakref__|__module__|__class__|__bases__|__slots__|__file__|__code__|__name__|__func__|__dict__|__self__|__mro__|__doc__";

fn build_python() -> Lexer {
    let mut builder = Builder::new(true, false);

    // numbers
    let mut numbers = Vec::new();
    numbers.push(rule(
        &builder,
        r"(\d(?:_?\d)*\.(?:\d(?:_?\d)*)?|(?:\d(?:_?\d)*)?\.\d(?:_?\d)*)([eE][+-]?\d(?:_?\d)*)?",
        TokenType::NumberFloat,
    ));
    numbers.push(rule(
        &builder,
        r"\d(?:_?\d)*[eE][+-]?\d(?:_?\d)*j?",
        TokenType::NumberFloat,
    ));
    numbers.push(rule(&builder, r"0[oO](?:_?[0-7])+", TokenType::NumberOct));
    numbers.push(rule(&builder, r"0[bB](?:_?[01])+", TokenType::NumberBin));
    numbers.push(rule(
        &builder,
        r"0[xX](?:_?[a-fA-F0-9])+",
        TokenType::NumberHex,
    ));
    numbers.push(rule(&builder, r"\d(?:_?\d)*", TokenType::NumberInteger));
    builder.add_all("numbers", numbers);

    // keywords
    builder.add(
        "keywords",
        rule(&builder, &format!(r"({PY_KEYWORDS})\b"), TokenType::Keyword),
    );
    builder.add(
        "keywords",
        rule(&builder, r"(False|True|None)\b", TokenType::KeywordConstant),
    );

    // builtins
    builder.add(
        "builtins",
        not_after(
            rule(
                &builder,
                &format!(r"({PY_BUILTINS})\b"),
                TokenType::NameBuiltin,
            ),
            '.',
        ),
    );
    builder.add(
        "builtins",
        not_after(
            rule(
                &builder,
                r"(self|Ellipsis|NotImplemented|cls)\b",
                TokenType::NameBuiltinPseudo,
            ),
            '.',
        ),
    );
    builder.add(
        "builtins",
        not_after(
            rule(
                &builder,
                &format!(r"({PY_EXCEPTIONS})\b"),
                TokenType::NameException,
            ),
            '.',
        ),
    );

    // magicfuncs / magicvars
    builder.add(
        "magicfuncs",
        rule(
            &builder,
            &format!(r"({PY_MAGICFUNCS})\b"),
            TokenType::NameFunctionMagic,
        ),
    );
    builder.add(
        "magicvars",
        rule(
            &builder,
            &format!(r"({PY_MAGICVARS})\b"),
            TokenType::NameVariableMagic,
        ),
    );

    // name
    builder.add(
        "name",
        rule(
            &builder,
            r"@[_\p{L}][_\p{L}\p{N}]*(\s*\.\s*[_\p{L}][_\p{L}\p{N}]*)*",
            TokenType::NameDecorator,
        ),
    );
    builder.add("name", rule(&builder, r"@", TokenType::Operator));
    builder.add(
        "name",
        rule(&builder, r"[_\p{L}][_\p{L}\p{N}]*", TokenType::Name),
    );

    // expr-keywords
    builder.add(
        "expr-keywords",
        rule(
            &builder,
            r"(yield from|async for|lambda|yield|await|else|for|if)\b",
            TokenType::Keyword,
        ),
    );
    builder.add(
        "expr-keywords",
        rule(&builder, r"(False|True|None)\b", TokenType::KeywordConstant),
    );

    // strings-double / strings-single
    let interpolation = r"%(\(\w+\))?[-#0 +]*([0-9]+|[*])?(\.([0-9]+|[*]))?[hlL]?[E-GXc-giorsaux%]";
    let brace = r"\{((\w+)((\.\w+)|(\[[^\]]+\]))*)?(\![sra])?(\:(.?[<>=\^])?[-+ ]?#?0?(\d+)?,?(\.\d+)?[E-GXb-gnosx%]?)?\}";
    for (name, ty) in [
        ("strings-double", TokenType::StrDouble),
        ("strings-single", TokenType::StrSingle),
    ] {
        builder.add(name, rule(&builder, interpolation, TokenType::StrInterpol));
        builder.add(name, rule(&builder, brace, TokenType::StrInterpol));
        let pattern = r#"[^\\'"%{\n]+"#;
        builder.add(name, rule(&builder, pattern, ty));
        builder.add(name, rule(&builder, r#"[\'"\\]"#, ty));
        builder.add(name, rule(&builder, r"%|(\{{1,2})", ty));
    }

    // stringescape / rfstringescape / fstringescape
    builder.add(
        "stringescape",
        rule(
            &builder,
            r#"\\([\\abfnrtv"']|\n|N\{.*?\}|u[a-fA-F0-9]{4}|U[a-fA-F0-9]{8}|x[a-fA-F0-9]{2}|[0-7]{1,3})"#,
            TokenType::StrEscape,
        ),
    );
    builder.add(
        "rfstringescape",
        rule(&builder, r"\{\{", TokenType::StrEscape),
    );
    builder.add(
        "rfstringescape",
        rule(&builder, r"\}\}", TokenType::StrEscape),
    );
    let rf_escape = builder.include("rfstringescape");
    let string_escape = builder.include("stringescape");
    builder.add_all("fstringescape", rf_escape);
    builder.add_all("fstringescape", string_escape);

    // plain string states
    for (name, closer, ty, strings, allow_newline) in [
        ("dqs", r#"""#.to_string(), TokenType::StrDouble, "strings-double", false),
        ("sqs", "'".to_string(), TokenType::StrSingle, "strings-single", false),
        (
            "tdqs",
            "\"\"\"".to_string(),
            TokenType::StrDouble,
            "strings-double",
            true,
        ),
        (
            "tsqs",
            "'''".to_string(),
            TokenType::StrSingle,
            "strings-single",
            true,
        ),
    ] {
        builder.add(name, pop(rule(&builder, &closer, ty), 1));
        if !allow_newline {
            let escape = if ty == TokenType::StrDouble {
                r#"\\\\|\\"|\\\n"#
            } else {
                r"\\\\|\\'|\\\n"
            };
            builder.add(name, rule(&builder, escape, TokenType::StrEscape));
        }
        let included = builder.include(strings);
        builder.add_all(name, included);
        if allow_newline {
            builder.add(name, rule(&builder, r"\n", ty));
        }
    }

    // f-string body states
    for (name, ty) in [
        ("fstrings-double", TokenType::StrDouble),
        ("fstrings-single", TokenType::StrSingle),
    ] {
        builder.add(name, rule(&builder, r"\}", TokenType::StrInterpol));
        builder.add(
            name,
            push(
                rule(&builder, r"\{", TokenType::StrInterpol),
                "expr-inside-fstring",
            ),
        );
        builder.add(name, rule(&builder, r#"[^\\'"{}\n]+"#, ty));
        builder.add(name, rule(&builder, r#"[\'"\\]"#, ty));
    }

    // f-string quote states (dqf/sqf/tdqf/tsqf)
    for (name, closer, ty, strings, allow_newline) in [
        ("dqf", r#"""#.to_string(), TokenType::StrDouble, "fstrings-double", false),
        ("sqf", "'".to_string(), TokenType::StrSingle, "fstrings-single", false),
        (
            "tdqf",
            "\"\"\"".to_string(),
            TokenType::StrDouble,
            "fstrings-double",
            true,
        ),
        (
            "tsqf",
            "'''".to_string(),
            TokenType::StrSingle,
            "fstrings-single",
            true,
        ),
    ] {
        builder.add(name, pop(rule(&builder, &closer, ty), 1));
        let escape = if ty == TokenType::StrDouble {
            r#"\\\\|\\"|\\\n"#
        } else {
            r"\\\\|\\'|\\\n"
        };
        builder.add(name, rule(&builder, escape, TokenType::StrEscape));
        let included = builder.include(strings);
        builder.add_all(name, included);
        if allow_newline {
            builder.add(name, rule(&builder, r"\n", ty));
        }
    }

    // Combined states (`chroma.Combined`): rfstringescape/fstringescape first,
    // then the f-string body state.
    for (combined, escape_state, body) in [
        ("comb_rf_tdqf", "rfstringescape", "tdqf"),
        ("comb_rf_tsqf", "rfstringescape", "tsqf"),
        ("comb_rf_dqf", "rfstringescape", "dqf"),
        ("comb_rf_sqf", "rfstringescape", "sqf"),
        ("comb_f_tdqf", "fstringescape", "tdqf"),
        ("comb_f_tsqf", "fstringescape", "tsqf"),
        ("comb_f_dqf", "fstringescape", "dqf"),
        ("comb_f_sqf", "fstringescape", "sqf"),
    ] {
        let escape_rules = builder.include(escape_state);
        let body_rules = builder.include(body);
        builder.add_all(combined, escape_rules);
        builder.add_all(combined, body_rules);
    }

    // funcname / classname / import / fromimport
    let magicfuncs = builder.include("magicfuncs");
    builder.add_all("funcname", magicfuncs);
    builder.add(
        "funcname",
        pop(
            rule(
                &builder,
                r"[_\p{L}][_\p{L}\p{N}]*",
                TokenType::NameFunction,
            ),
            1,
        ),
    );
    builder.add("funcname", pop_empty(&builder));
    builder.add(
        "classname",
        pop(
            rule(&builder, r"[_\p{L}][_\p{L}\p{N}]*", TokenType::NameClass),
            1,
        ),
    );

    builder.add(
        "import",
        rule_groups(
            &builder,
            r"(\s+)(as)(\s+)",
            vec![
                plain_group(TokenType::Text),
                plain_group(TokenType::Keyword),
                plain_group(TokenType::Text),
            ],
        ),
    );
    builder.add("import", rule(&builder, r"\.", TokenType::NameNamespace));
    builder.add(
        "import",
        rule(
            &builder,
            r"[_\p{L}][_\p{L}\p{N}]*",
            TokenType::NameNamespace,
        ),
    );
    builder.add(
        "import",
        rule_groups(
            &builder,
            r"(\s*)(,)(\s*)",
            vec![
                plain_group(TokenType::Text),
                plain_group(TokenType::Operator),
                plain_group(TokenType::Text),
            ],
        ),
    );
    builder.add("import", pop_empty(&builder));

    builder.add(
        "fromimport",
        pop(
            rule_groups(
                &builder,
                r"(\s+)(import)\b",
                vec![
                    plain_group(TokenType::Text),
                    plain_group(TokenType::KeywordNamespace),
                ],
            ),
            1,
        ),
    );
    builder.add(
        "fromimport",
        rule(&builder, r"\.", TokenType::NameNamespace),
    );
    builder.add(
        "fromimport",
        pop(rule(&builder, r"None\b", TokenType::NameBuiltinPseudo), 1),
    );
    builder.add(
        "fromimport",
        rule(
            &builder,
            r"[_\p{L}][_\p{L}\p{N}]*",
            TokenType::NameNamespace,
        ),
    );
    builder.add("fromimport", pop_empty(&builder));

    // expr
    let mut expr = Vec::new();
    let mut push_prefixed = |pattern: &str, quote: TokenType, state: &'static str, ci: bool| {
        let groups = vec![plain_group(TokenType::StrAffix), plain_group(quote)];
        let built = if ci {
            rule_groups_ci(&builder, pattern, groups)
        } else {
            rule_groups(&builder, pattern, groups)
        };
        expr.push(push(built, state));
    };
    push_prefixed(r#"(rf|fr)(""")"#, TokenType::StrDouble, "comb_rf_tdqf", true);
    push_prefixed(r#"(rf|fr)(''')"#, TokenType::StrSingle, "comb_rf_tsqf", true);
    push_prefixed(r#"(rf|fr)(")"#, TokenType::StrDouble, "comb_rf_dqf", true);
    push_prefixed(r#"(rf|fr)(')"#, TokenType::StrSingle, "comb_rf_sqf", true);
    push_prefixed(r#"(f)(""")"#, TokenType::StrDouble, "comb_f_tdqf", false);
    push_prefixed(r#"(f)(''')"#, TokenType::StrSingle, "comb_f_tsqf", false);
    push_prefixed(r#"(f)(")"#, TokenType::StrDouble, "comb_f_dqf", false);
    push_prefixed(r#"(f)(')"#, TokenType::StrSingle, "comb_f_sqf", false);
    push_prefixed(r#"(rb|br|r)(""")"#, TokenType::StrDouble, "tdqs", true);
    push_prefixed(r#"(rb|br|r)(''')"#, TokenType::StrSingle, "tsqs", true);
    push_prefixed(r#"(rb|br|r)(")"#, TokenType::StrDouble, "dqs", true);
    push_prefixed(r#"(rb|br|r)(')"#, TokenType::StrSingle, "sqs", true);
    push_prefixed(r#"([uUbB]?)(""")"#, TokenType::StrDouble, "tdqs", false);
    push_prefixed(r#"([uUbB]?)(''')"#, TokenType::StrSingle, "tsqs", false);
    push_prefixed(r#"([uUbB]?)(")"#, TokenType::StrDouble, "dqs", false);
    push_prefixed(r#"([uUbB]?)(')"#, TokenType::StrSingle, "sqs", false);
    expr.push(rule(&builder, r"[^\S\n]+", TokenType::Text));
    expr.extend(builder.include("numbers"));
    expr.push(rule(
        &builder,
        r"!=|==|<<|>>|:=|[-~+/*%=<>&^|.]",
        TokenType::Operator,
    ));
    expr.push(rule(&builder, r"[\[\]{}:(),;]", TokenType::Punctuation));
    expr.push(rule(
        &builder,
        r"(in|is|and|or|not)\b",
        TokenType::OperatorWord,
    ));
    expr.extend(builder.include("expr-keywords"));
    expr.extend(builder.include("builtins"));
    expr.extend(builder.include("magicfuncs"));
    expr.extend(builder.include("magicvars"));
    expr.extend(builder.include("name"));
    builder.add_all("expr", expr);

    // expr-inside-fstring / inner
    builder.add(
        "expr-inside-fstring",
        push(
            rule(&builder, r"[{(\[]", TokenType::Punctuation),
            "expr-inside-fstring-inner",
        ),
    );
    builder.add(
        "expr-inside-fstring",
        pop(
            rule(&builder, r"(=\s*)?(\![sraf])?\}", TokenType::StrInterpol),
            1,
        ),
    );
    builder.add(
        "expr-inside-fstring",
        pop(
            rule(&builder, r"(=\s*)?(\![sraf])?:", TokenType::StrInterpol),
            1,
        ),
    );
    builder.add(
        "expr-inside-fstring",
        rule(&builder, r"\s+", TokenType::Text),
    );
    let expr_rules = builder.include("expr");
    builder.add_all("expr-inside-fstring", expr_rules.clone());

    builder.add(
        "expr-inside-fstring-inner",
        push(
            rule(&builder, r"[{(\[]", TokenType::Punctuation),
            "expr-inside-fstring-inner",
        ),
    );
    builder.add(
        "expr-inside-fstring-inner",
        pop(rule(&builder, r"[])}]", TokenType::Punctuation), 1),
    );
    builder.add(
        "expr-inside-fstring-inner",
        rule(&builder, r"\s+", TokenType::Text),
    );
    builder.add_all("expr-inside-fstring-inner", expr_rules);

    // root
    builder.add("root", rule(&builder, r"\n", TokenType::Text));
    builder.add(
        "root",
        at_line_start(rule_groups(
            &builder,
            r#"(\s*)([rRuUbB]{0,2})("""(?:.|\n)*?""")"#,
            vec![
                plain_group(TokenType::Text),
                plain_group(TokenType::StrAffix),
                plain_group(TokenType::StrDoc),
            ],
        )),
    );
    builder.add(
        "root",
        at_line_start(rule_groups(
            &builder,
            r#"(\s*)([rRuUbB]{0,2})('''(?:.|\n)*?''')"#,
            vec![
                plain_group(TokenType::Text),
                plain_group(TokenType::StrAffix),
                plain_group(TokenType::StrDoc),
            ],
        )),
    );
    builder.add(
        "root",
        at_text_start(rule(&builder, r"#!.+$", TokenType::CommentHashbang)),
    );
    builder.add("root", rule(&builder, r"#.*$", TokenType::CommentSingle));
    builder.add("root", rule(&builder, r"\\\n", TokenType::Text));
    builder.add("root", rule(&builder, r"\\", TokenType::Text));
    let keywords = builder.include("keywords");
    builder.add_all("root", keywords);
    builder.add(
        "root",
        push(
            rule_groups(
                &builder,
                r"(def)((?:\s|\\\s)+)",
                vec![
                    plain_group(TokenType::Keyword),
                    plain_group(TokenType::Text),
                ],
            ),
            "funcname",
        ),
    );
    builder.add(
        "root",
        push(
            rule_groups(
                &builder,
                r"(class)((?:\s|\\\s)+)",
                vec![
                    plain_group(TokenType::Keyword),
                    plain_group(TokenType::Text),
                ],
            ),
            "classname",
        ),
    );
    builder.add(
        "root",
        push(
            rule_groups(
                &builder,
                r"(from)((?:\s|\\\s)+)",
                vec![
                    plain_group(TokenType::KeywordNamespace),
                    plain_group(TokenType::Text),
                ],
            ),
            "fromimport",
        ),
    );
    builder.add(
        "root",
        push(
            rule_groups(
                &builder,
                r"(import)((?:\s|\\\s)+)",
                vec![
                    plain_group(TokenType::KeywordNamespace),
                    plain_group(TokenType::Text),
                ],
            ),
            "import",
        ),
    );
    let expr_rules = builder.include("expr");
    builder.add_all("root", expr_rules);

    builder.finish()
}
