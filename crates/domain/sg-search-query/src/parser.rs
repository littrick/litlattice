#![forbid(unsafe_code)]

//! `internal/search/query/parser.go`: the query scanner and parser.
//!
//! The port keeps the baseline's byte-oriented scanning and the heuristic
//! flags (`parensAsPatterns`, `allowDanglingParens`, `disambiguated`,
//! `balancedPattern`, `emptyParens`). Strings are valid UTF-8, so the only
//! unreachable baseline path is invalid-byte input; a literal U+FFFD is
//! treated like Go's `RuneError`.

use crate::fields::is_known_field;
use crate::labels::Labels;
use crate::node::{
    new_operator, partition_parameters, Annotation, Node, OperatorKind, Parameter, Pattern,
};
use crate::predicate::predicate_table;
use crate::range::Range;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SearchType {
    Regex,
    Literal,
    Structural,
    Standard,
    CodyContext,
    Keyword,
}

/// Marker error for calls that expect an operand (`(` or `x or`). `Parse`
/// retries those with the dangling-paren fallback parser.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ParseError {
    ExpectedOperand(String),
    Other(String),
}

impl std::fmt::Display for ParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ParseError::ExpectedOperand(msg) | ParseError::Other(msg) => f.write_str(msg),
        }
    }
}

impl std::error::Error for ParseError {}

const PARENS_AS_PATTERNS: u32 = 1 << 0;
const ALLOW_DANGLING_PARENS: u32 = 1 << 1;
const DISAMBIGUATED: u32 = 1 << 2;
const BALANCED_PATTERN: u32 = 1 << 3;
const EMPTY_PARENS: u32 = 1 << 4;

fn is_set(heuristics: u32, heuristic: u32) -> bool {
    heuristics & heuristic != 0
}

fn decode_rune(buf: &[u8], pos: usize) -> (char, usize) {
    if pos >= buf.len() {
        return ('\u{FFFD}', 0);
    }
    let b = buf[pos];
    if b < 0x80 {
        return (b as char, 1);
    }
    // Inputs are valid UTF-8, so the remainder decodes.
    let s = std::str::from_utf8(&buf[pos..]).expect("valid utf8");
    let c = s.chars().next().expect("non-empty");
    (c, c.len_utf8())
}

/// `utf8.DecodeRune([]byte{b})`: only the first byte, so continuation bytes
/// decode as `RuneError`. Used where the baseline slices a single byte.
fn decode_single_byte(buf: &[u8], pos: usize) -> char {
    if pos >= buf.len() {
        return '\u{FFFD}';
    }
    let b = buf[pos];
    if b < 0x80 {
        b as char
    } else {
        '\u{FFFD}'
    }
}

fn is_space_bytes(buf: &[u8]) -> bool {
    if buf.is_empty() {
        return false;
    }
    match std::str::from_utf8(buf) {
        Ok(s) => s.chars().all(char::is_whitespace),
        Err(_) => false,
    }
}

fn skip_space_bytes(buf: &[u8]) -> usize {
    match std::str::from_utf8(buf) {
        Ok(s) => s
            .char_indices()
            .find(|(_, c)| !c.is_whitespace())
            .map(|(i, _)| i)
            .unwrap_or(buf.len()),
        Err(_) => 0,
    }
}

// ---------------------------------------------------------------------------
// Scanners
// ---------------------------------------------------------------------------

/// `ScanAnyPattern`.
pub fn scan_any_pattern(buf: &[u8]) -> (String, usize) {
    let mut pos = 0;
    let mut result = String::new();
    while pos < buf.len() {
        let start = pos;
        let (r, advance) = decode_rune(buf, pos);
        pos += advance;
        if r.is_whitespace() {
            pos = start;
            break;
        }
        result.push(r);
    }
    (result, pos)
}

/// `ScanBalancedPattern`.
pub fn scan_balanced_pattern(buf: &[u8]) -> (String, usize, bool) {
    let keep_scanning = |buf: &[u8]| -> bool {
        if scan_field(buf).is_some() {
            return false;
        }
        let lookahead = |v: &str| -> bool {
            if buf.len() < v.len() {
                return false;
            }
            std::str::from_utf8(&buf[..v.len()])
                .map(|s| s.eq_ignore_ascii_case(v))
                .unwrap_or(false)
        };
        if lookahead("and ") || lookahead("or ") || lookahead("not ") {
            return false;
        }
        true
    };

    if !keep_scanning(buf) {
        return (String::new(), 0, false);
    }

    let mut pos = 0;
    let mut count = 0;
    let mut balanced: i32 = 0;
    let mut result = String::new();

    while pos < buf.len() {
        let start = count;
        let (r, advance) = decode_rune(buf, pos);
        pos += advance;
        count += advance;
        if r.is_whitespace() && balanced == 0 {
            count = start;
            break;
        } else if r == '(' {
            if !keep_scanning(&buf[pos..]) {
                return (String::new(), 0, false);
            }
            balanced += 1;
            result.push(r);
        } else if r == ')' {
            balanced -= 1;
            if balanced < 0 {
                count = start;
                balanced = 0;
                break;
            }
            result.push(r);
        } else if r.is_whitespace() {
            if !keep_scanning(&buf[pos..]) {
                return (String::new(), 0, false);
            }
            result.push(r);
        } else if r == '\\' {
            if pos < buf.len() {
                let (escaped, advance) = decode_rune(buf, pos);
                pos += advance;
                count += advance;
                result.push('\\');
                result.push(escaped);
            } else {
                result.push(r);
            }
        } else {
            result.push(r);
        }
    }

    (result, count, balanced == 0)
}

/// `ScanPredicate`: a registered `name(...)` for the current field.
pub fn scan_predicate(field: &str, buf: &[u8]) -> Option<(String, usize)> {
    let table = predicate_table(field)?;
    let (name, name_advance) = scan_predicate_name(buf, table)?;
    let buf = &buf[name_advance..];
    if buf.is_empty() || buf[0] != b'(' {
        return None;
    }
    let (params, params_advance) = scan_balanced_parens(buf)?;
    Some((format!("{name}{params}"), name_advance + params_advance))
}

/// `ScanPredicateName`.
pub fn scan_predicate_name(buf: &[u8], table: &[&str]) -> Option<(String, usize)> {
    let mut advance = 0;
    let mut predicate_name: Option<String> = None;
    loop {
        let (r, i) = decode_rune(buf, advance);
        if r == '\u{FFFD}' {
            break;
        }
        if !(r.is_alphabetic() || r == '.') {
            predicate_name = Some(String::from_utf8_lossy(&buf[..advance]).into_owned());
            break;
        }
        advance += i;
    }
    let predicate_name = predicate_name?;
    if !table.contains(&predicate_name.as_str()) {
        return None;
    }
    Some((predicate_name, advance))
}

/// `ScanBalancedParens`.
pub fn scan_balanced_parens(buf: &[u8]) -> Option<(String, usize)> {
    let mut pos = 0;
    let mut count = 0;
    let mut result = String::new();

    let (r, advance) = decode_rune(buf, pos);
    pos += advance;
    count += advance;
    result.push(r);
    if r != '(' {
        return None;
    }
    let mut balance: i32 = 1;

    loop {
        let (r, advance) = decode_rune(buf, pos);
        pos += advance;
        count += advance;
        result.push(r);
        if r == '\u{FFFD}' {
            return None;
        }
        match r {
            '(' => balance += 1,
            ')' => balance -= 1,
            '\\' => {
                let (escaped, advance) = decode_rune(buf, pos);
                pos += advance;
                count += advance;
                result.push(escaped);
            }
            _ => {}
        }
        if balance == 0 {
            break;
        }
    }

    Some((result, count))
}

/// `ScanDelimited`.
pub fn scan_delimited(
    buf: &[u8],
    strict: bool,
    delimiter: char,
) -> Result<(String, usize), String> {
    let mut pos = 0;
    let mut count = 0;
    let mut result = String::new();

    let (r, advance) = decode_rune(buf, pos);
    pos += advance;
    count += advance;
    if r != delimiter {
        return Err(format!(
            "ScanDelimited expects the input buffer to start with delimiter {delimiter}, but it starts with {r}."
        ));
    }

    let mut last = r;
    while pos < buf.len() {
        let (r, advance) = decode_rune(buf, pos);
        pos += advance;
        count += advance;
        last = r;
        if r == delimiter {
            break;
        } else if r == '\\' {
            if pos < buf.len() {
                let (escaped, advance) = decode_rune(buf, pos);
                pos += advance;
                count += advance;
                match escaped {
                    'a' | 'b' | 'f' | 'v' => {
                        result.push('\\');
                        result.push(escaped);
                    }
                    'n' => result.push('\n'),
                    'r' => result.push('\r'),
                    't' => result.push('\t'),
                    '\\' => result.push('\\'),
                    c if c == delimiter => result.push(c),
                    c => {
                        if strict {
                            return Err("unrecognized escape sequence".to_owned());
                        }
                        result.push('\\');
                        result.push(c);
                    }
                }
                if pos >= buf.len() {
                    return Err(format!("unterminated literal: expected {delimiter}"));
                }
            } else {
                return Err("unterminated escape sequence".to_owned());
            }
        } else {
            result.push(r);
        }
    }

    if last != delimiter || (last == delimiter && count == 1) {
        return Err(format!("unterminated literal: expected {delimiter}"));
    }
    Ok((result, count))
}

/// `Delimit`: inverse of `ScanDelimited` for a value.
pub fn delimit(s: &str, delimiter: char) -> String {
    let ds = delimiter.to_string();
    let mut out = String::with_capacity(s.len() + 2);
    out.push(delimiter);
    for c in s.chars() {
        match c {
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            '\\' => out.push_str("\\\\"),
            c if c == delimiter => {
                out.push('\\');
                out.push(c);
            }
            c => out.push(c),
        }
    }
    out.push_str(&ds);
    out
}

/// `ScanField`.
pub fn scan_field(buf: &[u8]) -> Option<(String, bool, usize)> {
    let mut pos = 0;
    let mut count = 0;

    let (r, advance) = decode_rune(buf, pos);
    pos += advance;
    count += advance;
    if r != '-' && !r.is_ascii_alphabetic() {
        return None;
    }
    let mut result = String::new();
    result.push(r);

    let mut success = false;
    while pos < buf.len() {
        let (r, advance) = decode_rune(buf, pos);
        pos += advance;
        count += advance;
        if r.is_ascii_alphabetic() {
            result.push(r);
            continue;
        }
        if r == ':' && (!result.starts_with('-') || result.len() > 1) {
            success = true;
        }
        break;
    }
    if !success {
        return None;
    }

    let negated = result.starts_with('-');
    let field = if negated { &result[1..] } else { &result };
    if !is_known_field(&field.to_lowercase()) {
        return None;
    }
    Some((field.to_owned(), negated, count))
}

/// `ScanValue`.
pub fn scan_value(buf: &[u8], allow_dangling_parens: bool) -> (String, usize) {
    let mut pos = 0;
    let mut count = 0;
    let mut result = String::new();

    while pos < buf.len() {
        let start = count;
        let (r, advance) = decode_rune(buf, pos);
        pos += advance;
        count += advance;
        if r.is_whitespace() {
            count = start;
            break;
        }
        if r == '(' || r == ')' {
            if allow_dangling_parens {
                result.push(r);
                continue;
            }
            count = start;
            break;
        }
        if r == '\\' && pos < buf.len() {
            let (escaped, advance) = decode_rune(buf, pos);
            pos += advance;
            count += advance;
            result.push('\\');
            result.push(escaped);
            continue;
        }
        result.push(r);
    }
    (result, count)
}

// ---------------------------------------------------------------------------
// Parser
// ---------------------------------------------------------------------------

struct Parser<'a> {
    buf: &'a [u8],
    heuristics: u32,
    pos: usize,
    balanced: i32,
    leaf_parser: SearchType,
    seen_right_paren: bool,
    seen_or: bool,
}

impl<'a> Parser<'a> {
    fn done(&self) -> bool {
        self.pos >= self.buf.len()
    }

    fn peek(&self, n: usize) -> Option<String> {
        let mut pos = self.pos;
        let mut result = String::new();
        for _ in 0..n {
            if pos >= self.buf.len() {
                return None;
            }
            let (r, advance) = decode_rune(self.buf, pos);
            pos += advance;
            result.push(r);
        }
        Some(result)
    }

    fn match_kw(&self, keyword: &str) -> bool {
        match self.peek(keyword.len()) {
            Some(v) => v.eq_ignore_ascii_case(keyword),
            None => false,
        }
    }

    fn expect_kw(&mut self, keyword: &str) -> bool {
        if !self.match_kw(keyword) {
            return false;
        }
        self.pos += keyword.len();
        true
    }

    fn match_keyword(&self, keyword: &str) -> bool {
        if self.pos == 0 {
            return false;
        }
        if is_space_bytes(&self.buf[..self.pos]) {
            return false;
        }
        if !(is_space_bytes(&self.buf[self.pos - 1..self.pos]) || self.buf[self.pos - 1] == b')') {
            return false;
        }
        let Some(v) = self.peek(keyword.len()) else {
            return false;
        };
        let after = self.pos + keyword.len();
        if after >= self.buf.len()
            || !(is_space_bytes(&self.buf[after..after + 1]) || self.buf[after] == b'(')
        {
            return false;
        }
        v.eq_ignore_ascii_case(keyword)
    }

    fn match_unary_keyword(&self, keyword: &str) -> bool {
        if self.pos != 0 {
            let prev = self.buf[self.pos - 1];
            if !(is_space_bytes(&self.buf[self.pos - 1..self.pos]) || prev == b')' || prev == b'(')
            {
                return false;
            }
        }
        let Some(v) = self.peek(keyword.len()) else {
            return false;
        };
        let after = self.pos + keyword.len();
        if after >= self.buf.len() || !is_space_bytes(&self.buf[after..after + 1]) {
            return false;
        }
        v.eq_ignore_ascii_case(keyword)
    }

    fn skip_spaces(&mut self) {
        self.pos += skip_space_bytes(&self.buf[self.pos..]);
    }

    fn parse_quoted(&mut self, delimiter: char) -> Option<String> {
        let start = self.pos;
        let value = match scan_delimited(&self.buf[self.pos..], false, delimiter) {
            Ok((value, advance)) => {
                self.pos += advance;
                value
            }
            Err(_) => return None,
        };
        if !self.done() {
            let r = decode_single_byte(self.buf, self.pos);
            if !r.is_whitespace() && !self.match_kw(")") {
                self.pos = start;
                return None;
            }
        }
        Some(value)
    }

    fn parse_string_quotes(&mut self) -> Option<Pattern> {
        let start = self.pos;
        if self.match_kw("\"") {
            if let Some(v) = self.parse_quoted('"') {
                return Some(Pattern {
                    value: v,
                    negated: false,
                    annotation: Annotation {
                        labels: Labels::LITERAL | Labels::QUOTED,
                        range: Range::new(start, self.pos),
                    },
                });
            }
        }
        if self.match_kw("'") {
            if let Some(v) = self.parse_quoted('\'') {
                return Some(Pattern {
                    value: v,
                    negated: false,
                    annotation: Annotation {
                        labels: Labels::LITERAL | Labels::QUOTED,
                        range: Range::new(start, self.pos),
                    },
                });
            }
        }
        None
    }

    fn parse_regexp_quotes(&mut self) -> Option<Pattern> {
        if !self.match_kw("/") {
            return None;
        }
        let start = self.pos;
        let mut v = self.parse_quoted('/')?;
        let mut labels = Labels::REGEXP;
        if v.is_empty() {
            v = "//".to_owned();
            labels = Labels::LITERAL;
        }
        Some(Pattern {
            value: v,
            negated: false,
            annotation: Annotation {
                labels,
                range: Range::new(start, self.pos),
            },
        })
    }

    fn parse_field_value(&mut self, field: &str) -> Result<(String, Labels), ParseError> {
        if self.match_kw("'") {
            let (value, advance) =
                scan_delimited(&self.buf[self.pos..], true, '\'').map_err(ParseError::Other)?;
            self.pos += advance;
            return Ok((value, Labels::QUOTED));
        }
        if self.match_kw("\"") {
            let (value, advance) =
                scan_delimited(&self.buf[self.pos..], true, '"').map_err(ParseError::Other)?;
            self.pos += advance;
            return Ok((value, Labels::QUOTED));
        }

        if let Some((value, advance)) = scan_predicate(field, &self.buf[self.pos..]) {
            self.pos += advance;
            return Ok((value, Labels::IS_PREDICATE));
        }

        let (value, advance, ok) = scan_balanced_pattern(&self.buf[self.pos..]);
        if ok {
            self.pos += advance;
            return Ok((value, Labels::NONE));
        }

        let (value, advance) = scan_value(&self.buf[self.pos..], false);
        self.pos += advance;
        Ok((value, Labels::NONE))
    }

    fn parse_parameter(&mut self) -> Result<Option<Parameter>, ParseError> {
        let start = self.pos;
        let Some((field, negated, advance)) = scan_field(&self.buf[self.pos..]) else {
            return Ok(None);
        };
        self.pos += advance;
        let (value, labels) = self.parse_field_value(&field)?;
        Ok(Some(Parameter {
            field,
            value,
            negated,
            annotation: Annotation {
                range: Range::new(start, self.pos),
                labels,
            },
        }))
    }

    fn try_scan_balanced_pattern(&mut self, label: Labels) -> Option<Pattern> {
        let (value, advance, ok) = scan_balanced_pattern(&self.buf[self.pos..]);
        if !ok {
            return None;
        }
        let pattern = Pattern {
            value,
            negated: false,
            annotation: Annotation {
                labels: label,
                range: Range::new(self.pos, self.pos + advance),
            },
        };
        self.pos += advance;
        Some(pattern)
    }

    fn parse_pattern(&mut self, mut label: Labels) -> Pattern {
        if label.is_set(Labels::STANDARD | Labels::REGEXP) {
            if let Some(pattern) = self.parse_regexp_quotes() {
                return pattern;
            }
        }

        if label.is_set(Labels::REGEXP | Labels::QUOTES_AS_LITERALS) {
            if let Some(pattern) = self.parse_string_quotes() {
                return pattern;
            }
        }

        if is_set(self.heuristics, BALANCED_PATTERN) {
            if let Some(pattern) = self.try_scan_balanced_pattern(label) {
                return pattern;
            }
        }

        let start = self.pos;
        let (value, advance) = if label.is_set(Labels::REGEXP) {
            scan_value(
                &self.buf[self.pos..],
                is_set(self.heuristics, ALLOW_DANGLING_PARENS),
            )
        } else {
            scan_any_pattern(&self.buf[self.pos..])
        };
        if is_set(self.heuristics, ALLOW_DANGLING_PARENS) {
            label.set(Labels::HEURISTIC_DANGLING_PARENS);
        }
        self.pos += advance;
        Pattern {
            value,
            negated: false,
            annotation: Annotation {
                labels: label,
                range: Range::new(start, self.pos),
            },
        }
    }

    fn parse_leaves(&mut self, mut label: Labels) -> Result<Vec<Node>, ParseError> {
        let mut nodes: Vec<Node> = Vec::new();
        let start = self.pos;
        loop {
            self.skip_spaces();
            if self.done() {
                break;
            }
            if self.match_kw("(") && !is_set(self.heuristics, ALLOW_DANGLING_PARENS) {
                if is_set(self.heuristics, PARENS_AS_PATTERNS) {
                    let (value, advance, ok) = scan_balanced_pattern(&self.buf[self.pos..]);
                    if ok {
                        if label.is_set(Labels::LITERAL) {
                            label.set(Labels::HEURISTIC_PARENS_AS_PATTERNS);
                        }
                        nodes.push(Node::Pattern(Pattern {
                            value,
                            negated: false,
                            annotation: Annotation {
                                labels: label,
                                range: Range::new(self.pos, self.pos + advance),
                            },
                        }));
                        self.pos += advance;
                        continue;
                    }
                }
                // Treat the paren as part of an and/or expression.
                self.expect_kw("("); // Guaranteed to succeed.
                self.balanced += 1;
                if self.seen_or {
                    self.heuristics |= DISAMBIGUATED;
                }
                let mut result = self.parse_or()?;
                nodes.append(&mut result);
            } else if self.match_kw(")") && !is_set(self.heuristics, ALLOW_DANGLING_PARENS) {
                if self.balanced <= 0 {
                    if label.is_set(Labels::QUOTES_AS_LITERALS) {
                        return Err(ParseError::Other("unsupported expression. The combination of parentheses in the query has an unclear meaning. Use \"...\" to quote patterns that contain parentheses".to_owned()));
                    }
                    return Err(ParseError::Other("unsupported expression. The combination of parentheses in the query have an unclear meaning. Try using the content: filter to quote patterns that contain parentheses".to_owned()));
                }
                self.balanced -= 1;
                self.seen_right_paren = true;
                if nodes.is_empty() {
                    if is_set(self.heuristics, EMPTY_PARENS) {
                        nodes.push(Node::Pattern(Pattern {
                            value: "()".to_owned(),
                            negated: false,
                            annotation: Annotation {
                                labels: Labels::LITERAL | Labels::HEURISTIC_PARENS_AS_PATTERNS,
                                range: Range::new(start, self.pos),
                            },
                        }));
                    } else {
                        nodes.push(Node::Parameter(Parameter {
                            field: String::new(),
                            value: String::new(),
                            negated: false,
                            annotation: Annotation::default(),
                        }));
                    }
                }
                break;
            } else if self.match_keyword("and") {
                break;
            } else if self.match_keyword("or") {
                self.seen_or = true;
                if self.seen_right_paren {
                    self.heuristics |= DISAMBIGUATED;
                }
                break;
            } else if self.match_unary_keyword("not") {
                let start = self.pos;
                self.expect_kw("not");
                self.skip_spaces();
                if self.match_kw("(") {
                    return Err(ParseError::Other("it looks like you tried to use an expression after NOT. The NOT operator can only be used with simple search patterns or filters, and is not supported for expressions or subqueries".to_owned()));
                }
                if let Some(mut parameter) = self.parse_parameter()? {
                    if parameter.negated {
                        return Err(ParseError::Other(format!(
                            "unexpected NOT before \"-{}:{}\". Remove NOT and try again",
                            parameter.field, parameter.value
                        )));
                    }
                    parameter.negated = true;
                    parameter.annotation.range = Range::new(start, self.pos);
                    nodes.push(Node::Parameter(parameter));
                    continue;
                }
                let mut pattern = self.parse_pattern(label);
                pattern.negated = true;
                pattern.annotation.range = Range::new(start, self.pos);
                nodes.push(Node::Pattern(pattern));
            } else {
                match self.parse_parameter()? {
                    Some(parameter) => nodes.push(Node::Parameter(parameter)),
                    None => {
                        let pattern = self.parse_pattern(label);
                        nodes.push(Node::Pattern(pattern));
                    }
                }
            }
        }
        Ok(partition_parameters(nodes))
    }

    fn parse_and(&mut self) -> Result<Vec<Node>, ParseError> {
        let label = match self.leaf_parser {
            SearchType::Regex => Labels::REGEXP,
            SearchType::Literal | SearchType::Structural => Labels::LITERAL,
            SearchType::Standard => Labels::LITERAL | Labels::STANDARD,
            SearchType::Keyword => Labels::LITERAL | Labels::STANDARD | Labels::QUOTES_AS_LITERALS,
            SearchType::CodyContext => Labels::LITERAL | Labels::STANDARD,
        };
        let left = self.parse_leaves(label)?;
        if left.is_empty() {
            return Err(ParseError::ExpectedOperand(format!(
                "expected operand at {}",
                self.pos
            )));
        }
        if !self.expect_kw("and") {
            return Ok(left);
        }
        let right = self.parse_and()?;
        let mut nodes = left;
        nodes.extend(right);
        Ok(new_operator(nodes, OperatorKind::And))
    }

    fn parse_or(&mut self) -> Result<Vec<Node>, ParseError> {
        let left = self.parse_and()?;
        if left.is_empty() {
            return Err(ParseError::ExpectedOperand(format!(
                "expected operand at {}",
                self.pos
            )));
        }

        // parseAnd might have parsed an expression, in which case the parser
        // position points to a RPAREN.
        self.expect_kw(")");

        if !self.expect_kw("or") {
            return Ok(left);
        }
        let right = self.parse_or()?;
        let mut nodes = left;
        nodes.extend(right);
        Ok(new_operator(nodes, OperatorKind::Or))
    }

    fn try_fallback_parser(&self, input: &str) -> Result<Vec<Node>, ParseError> {
        let mut parser = Parser {
            buf: input.as_bytes(),
            heuristics: ALLOW_DANGLING_PARENS,
            pos: 0,
            balanced: 0,
            leaf_parser: self.leaf_parser,
            seen_right_paren: false,
            seen_or: false,
        };
        let nodes = parser.parse_or()?;
        if let Ok(hoisted) = crate::transformer::hoist(&nodes) {
            return Ok(new_operator(hoisted, OperatorKind::And));
        }
        Ok(new_operator(nodes, OperatorKind::And))
    }
}

/// `Parse`: parse a raw query string into a tree of [`Node`]s.
pub fn parse(input: &str, search_type: SearchType) -> Result<Vec<Node>, ParseError> {
    if input.trim().is_empty() {
        return Ok(Vec::new());
    }

    let heuristics = match search_type {
        SearchType::Keyword => BALANCED_PATTERN | EMPTY_PARENS,
        _ => BALANCED_PATTERN | EMPTY_PARENS | PARENS_AS_PATTERNS,
    };
    let mut parser = Parser {
        buf: input.as_bytes(),
        heuristics,
        pos: 0,
        balanced: 0,
        leaf_parser: search_type,
        seen_right_paren: false,
        seen_or: false,
    };

    let mut nodes = match parser.parse_or() {
        Ok(nodes) => nodes,
        Err(err) => {
            if matches!(err, ParseError::ExpectedOperand(_)) {
                // The query may be unbalanced or malformed as in "(" or
                // "x or" and expects an operand. Try harder to parse it.
                if let Ok(nodes) = parser.try_fallback_parser(input) {
                    return Ok(nodes);
                }
            }
            return Err(err);
        }
    };

    if parser.balanced != 0 {
        if let Ok(nodes) = parser.try_fallback_parser(input) {
            return Ok(nodes);
        }
        return Err(ParseError::Other("unbalanced expression".to_owned()));
    }

    if !is_set(parser.heuristics, DISAMBIGUATED) {
        if let Ok(hoisted) = crate::transformer::hoist(&nodes) {
            nodes = hoisted;
        }
    }

    if matches!(search_type, SearchType::Literal | SearchType::Standard) {
        crate::transformer::validate_pure_literal_pattern(&nodes, parser.balanced == 0)
            .map_err(ParseError::Other)?;
    }

    Ok(new_operator(nodes, OperatorKind::And))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::node::to_string;

    fn parse_str(input: &str, leaf: SearchType) -> String {
        match parse(input, leaf) {
            Ok(nodes) => to_string(&nodes),
            Err(err) => format!("error: {err}"),
        }
    }

    #[test]
    fn parses_simple_literal_query() {
        assert_eq!(
            parse_str("repo:foo bar", SearchType::Literal),
            "(and \"repo:foo\" \"bar\")"
        );
    }

    #[test]
    fn parses_or_and_not() {
        assert_eq!(parse_str("a or b", SearchType::Literal), "(or \"a\" \"b\")");
        assert_eq!(parse_str("not a", SearchType::Literal), "(not \"a\")");
    }

    #[test]
    fn scans_balanced_pattern() {
        let (value, count, ok) = scan_balanced_pattern(b"func(a int)");
        assert!(ok);
        assert_eq!(value, "func(a int)");
        assert_eq!(count, 11);
    }

    #[test]
    fn scans_quoted_value() {
        assert_eq!(scan_delimited(b"\"a\\nb\"", true, '"').unwrap().0, "a\nb");
    }
}
