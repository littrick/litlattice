#![forbid(unsafe_code)]

//! The parts of Go's `regexp/syntax` that `zoektquery.ParseRe` observes.
//!
//! `ParseRe` only distinguishes two outcomes: a top-level `OpLiteral` becomes a
//! zoekt `Substring` (with the decoded runes), everything else becomes a
//! `Regexp`. This module therefore implements just enough of the Go parser to
//! answer:
//!
//! - is the whole expression a concatenation of literals (`OpLiteral`)?
//! - is it the empty match (`OpEmptyMatch`, which `query.Simplify` folds to
//!   `Const(true)`)?
//! - or is it any other valid expression (a `Regexp`)?
//!
//! It also decodes literal escapes (`\n`, `\x41`, `\x{...}`, octal,
//! `\Q...\E`) and applies Go's `minFoldRune` to literals under an inline
//! `(?i)` flag. Non-literal patterns are syntax-checked with `regex-syntax`;
//! Go-only constructs (`\C`, `\Q` mixed with operators) are documented
//! limitations of the differential harness (see the M9.8go overview).

/// The literal shape of a Go-regexp pattern (see the module docs).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Literal {
    /// `OpEmptyMatch`.
    Empty,
    /// `OpLiteral` with the decoded rune string. `fold` records whether the
    /// literal was parsed under `FoldCase` (it does not affect `Substring`,
    /// which always uses the query's `case:` flag).
    Literal { text: String, fold: bool },
    /// Any other valid expression.
    NonLiteral,
}

/// Malformed Go regexp syntax.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InvalidSyntax;

impl std::fmt::Display for InvalidSyntax {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("invalid regexp syntax")
    }
}

impl std::error::Error for InvalidSyntax {}

/// Validate a non-literal pattern with a RE2-compatible parser. Go-only
/// syntax not understood here is a known limitation.
pub fn validate(pattern: &str) -> Result<(), String> {
    regex_syntax::ast::parse::Parser::new()
        .parse(pattern)
        .map(|_| ())
        .map_err(|err| err.to_string())
}

/// Parse `pattern` and classify its top-level operator like `regexp/syntax`
/// would under `ClassNL | PerlX | UnicodeGroups`.
pub fn parse(pattern: &str) -> Result<Literal, InvalidSyntax> {
    let mut parser = Parser {
        chars: pattern.chars().collect(),
        pos: 0,
        fold: false,
    };
    let literal = parser.parse_alt()?;
    if parser.pos != parser.chars.len() {
        // A top-level `)` can only be an unexpected closing paren.
        return Err(InvalidSyntax);
    }
    Ok(literal)
}

struct Parser {
    chars: Vec<char>,
    pos: usize,
    fold: bool,
}

impl Parser {
    fn peek(&self) -> Option<char> {
        self.chars.get(self.pos).copied()
    }

    fn peek_at(&self, offset: usize) -> Option<char> {
        self.chars.get(self.pos + offset).copied()
    }

    fn bump(&mut self) -> Option<char> {
        let c = self.peek()?;
        self.pos += 1;
        Some(c)
    }

    fn starts_with(&self, text: &str) -> bool {
        text.chars()
            .enumerate()
            .all(|(offset, expected)| self.peek_at(offset) == Some(expected))
    }

    fn parse_alt(&mut self) -> Result<Literal, InvalidSyntax> {
        let mut branches = vec![self.parse_concat()?];
        while self.peek() == Some('|') {
            self.bump();
            branches.push(self.parse_concat()?);
        }
        if branches.len() == 1 {
            return Ok(branches.pop().expect("one branch"));
        }
        Ok(combine_alternation(branches))
    }

    fn parse_concat(&mut self) -> Result<Literal, InvalidSyntax> {
        let mut text = String::new();
        let mut fold: Option<bool> = None;
        let mut saw_literal = false;
        let mut non_literal = false;
        loop {
            match self.peek() {
                None | Some('|') | Some(')') => break,
                _ => {}
            }
            // `(?flags)` updates subsequent literals and produces no atom.
            if self.starts_with("(?") && !self.starts_with("(?:") {
                if let Some(new_fold) = self.parse_flag_setter()? {
                    self.fold = new_fold;
                    continue;
                }
            }
            let atom = self.parse_atom()?;
            let mut atom = atom;
            if self.parse_quantifier()? {
                atom = Literal::NonLiteral;
            }
            match atom {
                Literal::Empty => {}
                Literal::NonLiteral => non_literal = true,
                Literal::Literal {
                    text: atom_text,
                    fold: atom_fold,
                } => {
                    if saw_literal && fold != Some(atom_fold) {
                        // Go keeps a Concat of literals with different flags.
                        non_literal = true;
                    }
                    saw_literal = true;
                    fold = Some(atom_fold);
                    text.push_str(&atom_text);
                }
            }
        }
        if non_literal {
            return Ok(Literal::NonLiteral);
        }
        if !saw_literal {
            return Ok(Literal::Empty);
        }
        Ok(Literal::Literal {
            text,
            fold: fold.unwrap_or(false),
        })
    }

    /// Parse `(?flags)` when present. Returns `Some(new_fold)`; `None` when the
    /// construct is not a flag setter (e.g. `(?:`, `(?P<`, `(?i:`).
    fn parse_flag_setter(&mut self) -> Result<Option<bool>, InvalidSyntax> {
        let start = self.pos;
        debug_assert!(self.starts_with("(?"));
        self.pos += 2;
        let Some(flags) = self.read_flags()? else {
            self.pos = start;
            return Ok(None);
        };
        if self.peek() != Some(')') {
            self.pos = start;
            return Ok(None);
        }
        self.bump();
        Ok(Some(apply_flags(self.fold, &flags)))
    }

    /// Read `[imsU-]*` and return the spec, or `None` when the next char is
    /// `:`/`P`/`<`/`)`-less (a group, not a flag setter).
    fn read_flags(&mut self) -> Result<Option<String>, InvalidSyntax> {
        let mut flags = String::new();
        while let Some(c) = self.peek() {
            if matches!(c, 'i' | 'm' | 's' | 'U' | '-') {
                flags.push(c);
                self.bump();
            } else {
                break;
            }
        }
        if flags.is_empty() {
            return Ok(None);
        }
        Ok(Some(flags))
    }

    fn parse_atom(&mut self) -> Result<Literal, InvalidSyntax> {
        let c = self.peek().ok_or(InvalidSyntax)?;
        match c {
            '(' => self.parse_group(),
            '[' => self.parse_class(),
            '\\' => self.parse_escape(false),
            '.' | '^' | '$' => {
                self.bump();
                Ok(Literal::NonLiteral)
            }
            '*' | '+' | '?' => Err(InvalidSyntax),
            '{' => {
                // A `{` that does not start a valid repetition is a literal.
                self.bump();
                self.literal_char('{')
            }
            ']' => {
                self.bump();
                self.literal_char(']')
            }
            _ => {
                self.bump();
                self.literal_char(c)
            }
        }
    }

    fn literal_char(&self, c: char) -> Result<Literal, InvalidSyntax> {
        let c = if self.fold { min_fold(c) } else { c };
        Ok(Literal::Literal {
            text: c.to_string(),
            fold: self.fold,
        })
    }

    fn parse_group(&mut self) -> Result<Literal, InvalidSyntax> {
        debug_assert_eq!(self.peek(), Some('('));
        self.bump();
        if self.peek() == Some('?') {
            self.bump();
            match self.peek() {
                Some(':') => {
                    self.bump();
                    let save = self.fold;
                    let literal = self.parse_alt()?;
                    self.expect(')')?;
                    self.fold = save;
                    Ok(literal)
                }
                Some('P') | Some('<') => {
                    // Named capture: `(?P<name>...)` or `(?<name>...)`.
                    if self.peek() == Some('P') {
                        self.bump();
                    }
                    self.expect('<')?;
                    let mut closed = false;
                    while let Some(c) = self.bump() {
                        if c == '>' {
                            closed = true;
                            break;
                        }
                    }
                    if !closed {
                        return Err(InvalidSyntax);
                    }
                    let save = self.fold;
                    let literal = self.parse_alt()?;
                    self.expect(')')?;
                    self.fold = save;
                    // `OptimizeRegexp` turns captures into non-capturing groups,
                    // so their contents can still reduce to a literal.
                    Ok(literal)
                }
                Some('i') | Some('m') | Some('s') | Some('U') | Some('-') => {
                    let save = self.fold;
                    let Some(flags) = self.read_flags()? else {
                        return Err(InvalidSyntax);
                    };
                    if self.peek() != Some(':') {
                        // `(?flags)` inside an atom position is handled by
                        // `parse_concat`; a bare setter here is malformed.
                        return Err(InvalidSyntax);
                    }
                    self.bump();
                    self.fold = apply_flags(save, &flags);
                    let literal = self.parse_alt()?;
                    self.expect(')')?;
                    self.fold = save;
                    Ok(literal)
                }
                _ => Err(InvalidSyntax),
            }
        } else {
            // Capturing group: `OptimizeRegexp` turns it into a non-capturing
            // group, so its literal contents still reduce to a literal.
            let save = self.fold;
            let literal = self.parse_alt()?;
            self.expect(')')?;
            self.fold = save;
            Ok(literal)
        }
    }

    fn expect(&mut self, expected: char) -> Result<(), InvalidSyntax> {
        if self.peek() == Some(expected) {
            self.bump();
            Ok(())
        } else {
            Err(InvalidSyntax)
        }
    }

    fn parse_quantifier(&mut self) -> Result<bool, InvalidSyntax> {
        match self.peek() {
            Some('*') | Some('+') | Some('?') => {
                self.bump();
                // Lazy quantifier (PerlX).
                if self.peek() == Some('?') {
                    self.bump();
                }
                if matches!(self.peek(), Some('*') | Some('+') | Some('?')) {
                    // `a++`, `a**`, ... are invalid under PerlX.
                    return Err(InvalidSyntax);
                }
                if self.peek() == Some('{') && self.looks_like_repeat() {
                    return Err(InvalidSyntax);
                }
                Ok(true)
            }
            Some('{') if self.looks_like_repeat() => {
                let (min, max) = self.parse_repeat()?;
                if let Some(max) = max {
                    if min > max {
                        return Err(InvalidSyntax);
                    }
                }
                if self.peek() == Some('?') {
                    self.bump();
                }
                if matches!(self.peek(), Some('*') | Some('+') | Some('?')) {
                    return Err(InvalidSyntax);
                }
                Ok(true)
            }
            _ => Ok(false),
        }
    }

    fn looks_like_repeat(&self) -> bool {
        // `{` digits (`,` digits?)? `}`
        let mut offset = 1;
        let mut saw_digit = false;
        while matches!(self.peek_at(offset), Some(c) if c.is_ascii_digit()) {
            offset += 1;
            saw_digit = true;
        }
        if !saw_digit {
            return false;
        }
        if self.peek_at(offset) == Some(',') {
            offset += 1;
            while matches!(self.peek_at(offset), Some(c) if c.is_ascii_digit()) {
                offset += 1;
            }
        }
        self.peek_at(offset) == Some('}')
    }

    fn parse_repeat(&mut self) -> Result<(u32, Option<u32>), InvalidSyntax> {
        self.expect('{')?;
        let min = self.parse_int()?;
        let max = if self.peek() == Some(',') {
            self.bump();
            if self.peek() == Some('}') {
                None
            } else {
                Some(self.parse_int()?)
            }
        } else {
            Some(min)
        };
        self.expect('}')?;
        Ok((min, max))
    }

    fn parse_int(&mut self) -> Result<u32, InvalidSyntax> {
        let mut value: u32 = 0;
        let mut saw_digit = false;
        while let Some(c) = self.peek() {
            let Some(digit) = c.to_digit(10) else {
                break;
            };
            saw_digit = true;
            value = value
                .checked_mul(10)
                .ok_or(InvalidSyntax)?
                .checked_add(digit)
                .ok_or(InvalidSyntax)?;
            self.bump();
        }
        if saw_digit {
            Ok(value)
        } else {
            Err(InvalidSyntax)
        }
    }

    fn parse_escape(&mut self, in_class: bool) -> Result<Literal, InvalidSyntax> {
        debug_assert_eq!(self.peek(), Some('\\'));
        self.bump();
        let c = self.bump().ok_or(InvalidSyntax)?;
        match c {
            'Q' if !in_class => {
                let mut text = String::new();
                while let Some(next) = self.peek() {
                    if next == '\\' && self.peek_at(1) == Some('E') {
                        self.bump();
                        self.bump();
                        break;
                    }
                    let next = self.bump().expect("peeked");
                    text.push(if self.fold { min_fold(next) } else { next });
                }
                if text.is_empty() {
                    Ok(Literal::Empty)
                } else {
                    Ok(Literal::Literal {
                        text,
                        fold: self.fold,
                    })
                }
            }
            'a' => self.escape_char('\u{7}'),
            'f' => self.escape_char('\u{c}'),
            'n' => self.escape_char('\n'),
            'r' => self.escape_char('\r'),
            't' => self.escape_char('\t'),
            'v' => self.escape_char('\u{b}'),
            'b' if in_class => self.escape_char('\u{8}'),
            'A' | 'z' | 'b' | 'B' if !in_class => Ok(Literal::NonLiteral),
            'd' | 'D' | 's' | 'S' | 'w' | 'W' => Ok(Literal::NonLiteral),
            'p' | 'P' => {
                if self.peek() == Some('{') {
                    self.bump();
                    while let Some(next) = self.bump() {
                        if next == '}' {
                            return Ok(Literal::NonLiteral);
                        }
                    }
                    Err(InvalidSyntax)
                } else {
                    self.bump().ok_or(InvalidSyntax)?;
                    Ok(Literal::NonLiteral)
                }
            }
            'x' => {
                let value = if self.peek() == Some('{') {
                    self.bump();
                    let mut value: u32 = 0;
                    let mut saw_digit = false;
                    while let Some(next) = self.peek() {
                        if next == '}' {
                            break;
                        }
                        let Some(digit) = next.to_digit(16) else {
                            return Err(InvalidSyntax);
                        };
                        saw_digit = true;
                        value = value
                            .checked_mul(16)
                            .ok_or(InvalidSyntax)?
                            .checked_add(digit)
                            .ok_or(InvalidSyntax)?;
                        self.bump();
                    }
                    self.expect('}')?;
                    if !saw_digit {
                        return Err(InvalidSyntax);
                    }
                    value
                } else {
                    let mut value: u32 = 0;
                    for _ in 0..2 {
                        let digit = self
                            .bump()
                            .and_then(|c| c.to_digit(16))
                            .ok_or(InvalidSyntax)?;
                        value = value * 16 + digit;
                    }
                    value
                };
                let Some(c) = char::from_u32(value) else {
                    return Err(InvalidSyntax);
                };
                self.escape_char(c)
            }
            '0'..='7' => {
                let mut value = c.to_digit(8).expect("octal digit");
                for _ in 0..2 {
                    let Some(next) = self.peek() else { break };
                    let Some(digit) = next.to_digit(8) else {
                        break;
                    };
                    value = value * 8 + digit;
                    self.bump();
                }
                if value > 0xFF {
                    return Err(InvalidSyntax);
                }
                let c = char::from_u32(value).ok_or(InvalidSyntax)?;
                self.escape_char(c)
            }
            '8' | '9' => Err(InvalidSyntax),
            c if c.is_ascii_alphanumeric() => Err(InvalidSyntax),
            c => self.escape_char(c),
        }
    }

    fn escape_char(&self, c: char) -> Result<Literal, InvalidSyntax> {
        let c = if self.fold { min_fold(c) } else { c };
        Ok(Literal::Literal {
            text: c.to_string(),
            fold: self.fold,
        })
    }

    fn parse_class(&mut self) -> Result<Literal, InvalidSyntax> {
        debug_assert_eq!(self.peek(), Some('['));
        self.bump();
        let mut negated = false;
        if self.peek() == Some('^') {
            negated = true;
            self.bump();
        }
        let mut runes: Vec<char> = Vec::new();
        let mut non_literal = false;
        let mut first = true;
        loop {
            let Some(c) = self.peek() else {
                return Err(InvalidSyntax);
            };
            if c == ']' && !first {
                self.bump();
                break;
            }
            first = false;
            let start = if c == '\\' {
                match self.parse_class_escape()? {
                    ClassItem::Rune(r) => r,
                    ClassItem::Set => {
                        non_literal = true;
                        continue;
                    }
                }
            } else if c == '[' && self.peek_at(1) == Some(':') {
                // POSIX class `[:alpha:]`.
                self.bump();
                self.bump();
                while let Some(next) = self.bump() {
                    if next == ':' && self.peek() == Some(']') {
                        self.bump();
                        break;
                    }
                }
                non_literal = true;
                continue;
            } else if c == ']' {
                // A leading `]` is a literal.
                self.bump();
                ']'
            } else {
                self.bump();
                c
            };
            // Range `start-end`, where `-` is literal at the end.
            if self.peek() == Some('-') && self.peek_at(1).is_some() && self.peek_at(1) != Some(']')
            {
                self.bump();
                let end = if self.peek() == Some('\\') {
                    match self.parse_class_escape()? {
                        ClassItem::Rune(r) => r,
                        ClassItem::Set => {
                            non_literal = true;
                            continue;
                        }
                    }
                } else {
                    self.bump().expect("range end")
                };
                if start == end {
                    runes.push(start);
                } else {
                    non_literal = true;
                }
            } else {
                runes.push(start);
            }
        }
        if negated || non_literal {
            return Ok(Literal::NonLiteral);
        }
        runes.sort_unstable();
        runes.dedup();
        if runes.is_empty() {
            return Ok(Literal::NonLiteral);
        }
        // A class with one rune, or whose runes are all in one simple-fold
        // cycle, is simplified to a literal by Go's parser.
        let folded: Vec<char> = runes.iter().map(|c| min_fold(*c)).collect();
        if folded.windows(2).all(|pair| pair[0] == pair[1]) {
            let (text, fold) = if runes.len() == 1 {
                if self.fold {
                    (folded[0], true)
                } else {
                    (runes[0], false)
                }
            } else {
                (folded[0], true)
            };
            Ok(Literal::Literal {
                text: text.to_string(),
                fold,
            })
        } else {
            Ok(Literal::NonLiteral)
        }
    }

    fn parse_class_escape(&mut self) -> Result<ClassItem, InvalidSyntax> {
        debug_assert_eq!(self.peek(), Some('\\'));
        self.bump();
        let c = self.bump().ok_or(InvalidSyntax)?;
        match c {
            'a' => Ok(ClassItem::Rune('\u{7}')),
            'f' => Ok(ClassItem::Rune('\u{c}')),
            'n' => Ok(ClassItem::Rune('\n')),
            'r' => Ok(ClassItem::Rune('\r')),
            't' => Ok(ClassItem::Rune('\t')),
            'v' => Ok(ClassItem::Rune('\u{b}')),
            'b' => Ok(ClassItem::Rune('\u{8}')),
            'd' | 'D' | 's' | 'S' | 'w' | 'W' => Ok(ClassItem::Set),
            'p' | 'P' => {
                if self.peek() == Some('{') {
                    self.bump();
                    while let Some(next) = self.bump() {
                        if next == '}' {
                            return Ok(ClassItem::Set);
                        }
                    }
                    Err(InvalidSyntax)
                } else {
                    self.bump().ok_or(InvalidSyntax)?;
                    Ok(ClassItem::Set)
                }
            }
            'x' => {
                let value = if self.peek() == Some('{') {
                    self.bump();
                    let mut value: u32 = 0;
                    while let Some(next) = self.peek() {
                        if next == '}' {
                            break;
                        }
                        let digit = next.to_digit(16).ok_or(InvalidSyntax)?;
                        value = value * 16 + digit;
                        self.bump();
                    }
                    self.expect('}')?;
                    value
                } else {
                    let mut value = 0;
                    for _ in 0..2 {
                        value = value * 16
                            + self
                                .bump()
                                .and_then(|c| c.to_digit(16))
                                .ok_or(InvalidSyntax)?;
                    }
                    value
                };
                Ok(ClassItem::Rune(char::from_u32(value).ok_or(InvalidSyntax)?))
            }
            '0'..='7' => {
                let mut value = c.to_digit(8).expect("octal");
                for _ in 0..2 {
                    let Some(next) = self.peek() else { break };
                    let Some(digit) = next.to_digit(8) else { break };
                    value = value * 8 + digit;
                    self.bump();
                }
                Ok(ClassItem::Rune(char::from_u32(value).ok_or(InvalidSyntax)?))
            }
            '8' | '9' => Err(InvalidSyntax),
            c if c.is_ascii_alphanumeric() => Err(InvalidSyntax),
            c => Ok(ClassItem::Rune(c)),
        }
    }
}

enum ClassItem {
    Rune(char),
    Set,
}

fn combine_alternation(branches: Vec<Literal>) -> Literal {
    if branches.iter().all(|b| matches!(b, Literal::Empty)) {
        return Literal::Empty;
    }
    let mut single: Option<(char, bool)> = None;
    for branch in branches {
        match branch {
            Literal::Literal { text, fold } => {
                let mut chars = text.chars();
                let Some(c) = chars.next() else {
                    // Empty literal branch (e.g. `a|`) is not a literal union.
                    return Literal::NonLiteral;
                };
                if chars.next().is_some() {
                    return Literal::NonLiteral;
                }
                match single {
                    None => single = Some((c, fold)),
                    Some((expected, expected_fold)) if expected == c && expected_fold == fold => {}
                    Some(_) => return Literal::NonLiteral,
                }
            }
            Literal::Empty | Literal::NonLiteral => return Literal::NonLiteral,
        }
    }
    match single {
        Some((c, fold)) => Literal::Literal {
            text: c.to_string(),
            fold,
        },
        None => Literal::Empty,
    }
}

/// Go `applyFlags` for the flags we track (`i`); the rest of the spec is
/// accepted and ignored.
fn apply_flags(mut fold: bool, flags: &str) -> bool {
    let mut clearing = false;
    for c in flags.chars() {
        match c {
            '-' => clearing = true,
            'i' => fold = !clearing,
            _ => {}
        }
    }
    fold
}

/// Go `minFoldRune` approximation: the smallest rune in the simple-fold cycle.
/// ASCII is exact; non-ASCII uses the single-character lower/upper mappings
/// (multi-character expansions such as `ß` are left alone, matching Go).
fn min_fold(c: char) -> char {
    let mut min = c;
    if let Some(lower) = single_char_case(c.to_lowercase()) {
        min = min.min(lower);
    }
    if let Some(upper) = single_char_case(c.to_uppercase()) {
        min = min.min(upper);
    }
    min
}

fn single_char_case(mut iter: impl Iterator<Item = char>) -> Option<char> {
    let first = iter.next()?;
    if iter.next().is_some() {
        None
    } else {
        Some(first)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn literal(pattern: &str) -> Option<String> {
        match parse(pattern).unwrap_or_else(|InvalidSyntax| panic!("invalid {pattern:?}")) {
            Literal::Literal { text, .. } => Some(text),
            _ => None,
        }
    }

    #[test]
    fn classifies_literal_patterns_like_go() {
        assert_eq!(literal("foo"), Some("foo".to_owned()));
        assert_eq!(literal("foo\\.bar"), Some("foo.bar".to_owned()));
        assert_eq!(literal("(?i)foo"), Some("FOO".to_owned()));
        assert_eq!(literal("(?i:foo)"), Some("FOO".to_owned()));
        assert_eq!(literal("\\Qa.b\\E"), Some("a.b".to_owned()));
        assert_eq!(literal("\\x41\\x{42}\\103"), Some("ABC".to_owned()));
        assert_eq!(literal("[a]"), Some("a".to_owned()));
        assert_eq!(literal("[aA]"), Some("A".to_owned()));
        assert_eq!(literal("(?:abc)"), Some("abc".to_owned()));
        assert_eq!(literal("(foo)"), Some("foo".to_owned()));
        assert_eq!(literal("(foo)(bar)"), Some("foobar".to_owned()));
        assert_eq!(literal("(?P<n>abc)"), Some("abc".to_owned()));
        assert_eq!(literal("a|a"), Some("a".to_owned()));
        assert_eq!(literal("{"), Some("{".to_owned()));
    }

    #[test]
    fn classifies_non_literals_like_go() {
        assert_eq!(parse("").expect("valid"), Literal::Empty);
        assert_eq!(parse("(?:)").expect("valid"), Literal::Empty);
        assert_eq!(parse("(?:|)").expect("valid"), Literal::Empty);
        for pattern in [
            "f.o",
            "a|b",
            "[a-z]",
            "a*",
            "^foo$",
            "\\d+",
            "a{2,3}",
            "a|",
            "ab|ab",
            "foo(?i)bar",
            "[ab]",
            "(?i)ab(?-i:ab)",
            "(?:ab)+",
            "(?:(?i)foo)bar",
        ] {
            assert_eq!(
                parse(pattern).expect(pattern),
                Literal::NonLiteral,
                "{pattern:?}"
            );
        }
    }

    #[test]
    fn rejects_invalid_patterns() {
        for pattern in ["[", "(foo", "*foo", "\\q", "\\8", "a++"] {
            assert!(parse(pattern).is_err(), "{pattern:?} should be invalid");
        }
    }

    #[test]
    fn validates_re2_syntax() {
        assert!(validate("f.o").is_ok());
        assert!(validate("a{2,3}").is_ok());
        assert!(validate("[a-z]+").is_ok());
        assert!(validate("[").is_err());
    }
}
