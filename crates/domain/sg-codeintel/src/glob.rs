#![forbid(unsafe_code)]

//! A [gobwas/glob](https://github.com/gobwas/glob) compatible pattern
//! compiler, used by the `internal/codeintel/policies` configuration-policy
//! matcher (`glob.Compile(pattern)`).
//!
//! The baseline compiles every non-commit configuration-policy pattern with
//! `gobwas/glob` in its default configuration: **no separators**, so `*` and
//! `**` both match any run of characters (including `/`), and `?` matches
//! exactly one character. The rest of the syntax, reproduced here from
//! `gobwas/glob/syntax/{lexer,ast}`:
//!
//! * `*` — any sequence (empty allowed);
//! * `**` — the "super" star; identical to `*` without separators;
//! * `?` — exactly one character;
//! * `[abc]`, `[a-z]` — character set / range; a leading `!` negates
//!   (`[!abc]`). gobwas accepts *either* a range or a set of characters, not
//!   both: `[a-z0-9]` is a syntax error, and the range must be followed
//!   immediately by `]`;
//! * `{a,b}` — alternatives (nested braces and empty alternatives allowed);
//! * `\x` — escapes the next character; a trailing `\` is dropped, like
//!   gobwas's lexer;
//! * `]`, `}`, `,` outside a range/term are literal characters.
//!
//! Invalid patterns produce the same *classes* of failures as gobwas
//! ("unexpected end of input", "expected close range character", "could not
//! parse range", "hi character ... should be greater than lo ..."). The error
//! text is ours; the caller wraps it in the baseline's
//! `failed to compile glob pattern %q in configuration policy %d` message.
//!
//! Alternatives are expanded into a list of token sequences at compile time,
//! so matching is a simple recursive walk with backtracking over `*`.

use std::fmt;

/// One parsed pattern element.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Token {
    /// A run of literal characters.
    Text(String),
    /// `*` or `**`.
    Any,
    /// `?`.
    Single,
    /// `[a-z]` / `[!a-z]`.
    Range { lo: char, hi: char, not: bool },
    /// `[abc]` / `[!abc]`.
    List { chars: Vec<char>, not: bool },
    /// `{...}`; expanded by [`expand`] before matching.
    AnyOf(Vec<Vec<Token>>),
}

/// A compiled glob pattern.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Glob {
    /// One token sequence per alternative produced by brace expansion.
    alternatives: Vec<Vec<Token>>,
}

impl Glob {
    /// `glob.Compile(pattern)`.
    pub fn compile(pattern: &str) -> Result<Self, String> {
        let mut parser = Parser {
            chars: pattern.chars().collect(),
            pos: 0,
        };
        let tokens = parser.parse_sequence(false)?;
        Ok(Self {
            alternatives: expand(&tokens),
        })
    }

    /// `glob.Glob.Match(value)`.
    pub fn matches(&self, value: &str) -> bool {
        let value: Vec<char> = value.chars().collect();
        self.alternatives
            .iter()
            .any(|tokens| match_sequence(tokens, &value))
    }
}

impl fmt::Display for Glob {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("glob")
    }
}

struct Parser {
    chars: Vec<char>,
    pos: usize,
}

impl Parser {
    fn peek(&self) -> Option<char> {
        self.chars.get(self.pos).copied()
    }

    fn advance(&mut self) -> Option<char> {
        let value = self.peek();
        if value.is_some() {
            self.pos += 1;
        }
        value
    }

    /// Parses a token sequence. At the top level it runs to end of input;
    /// inside a `{...}` term it stops (without consuming) at the first
    /// top-level `,` or `}`.
    fn parse_sequence(&mut self, in_terms: bool) -> Result<Vec<Token>, String> {
        let mut tokens = Vec::new();
        loop {
            let Some(c) = self.peek() else {
                if in_terms {
                    return Err("unexpected end of input".to_owned());
                }
                return Ok(tokens);
            };
            if in_terms && (c == ',' || c == '}') {
                return Ok(tokens);
            }
            match c {
                '\\' => {
                    self.pos += 1;
                    // `fetchText`: the escaped character is read next; a
                    // trailing backslash hits EOF and is dropped.
                    if let Some(escaped) = self.advance() {
                        push_text(&mut tokens, escaped);
                    }
                }
                '*' => {
                    self.pos += 1;
                    if self.peek() == Some('*') {
                        self.pos += 1;
                    }
                    tokens.push(Token::Any);
                }
                '?' => {
                    self.pos += 1;
                    tokens.push(Token::Single);
                }
                '[' => tokens.push(self.parse_range()?),
                '{' => {
                    self.pos += 1;
                    tokens.push(Token::AnyOf(self.parse_terms()?));
                }
                _ => {
                    let mut text = String::new();
                    while let Some(c) = self.peek() {
                        if matches!(c, '\\' | '*' | '?' | '[' | '{') {
                            break;
                        }
                        if in_terms && (c == ',' || c == '}') {
                            break;
                        }
                        text.push(c);
                        self.pos += 1;
                    }
                    match tokens.last_mut() {
                        Some(Token::Text(existing)) => existing.push_str(&text),
                        _ => tokens.push(Token::Text(text)),
                    }
                }
            }
        }
    }

    /// Parses the alternatives of a `{...}` term. The opening brace has been
    /// consumed; the closing brace is consumed here.
    fn parse_terms(&mut self) -> Result<Vec<Vec<Token>>, String> {
        let mut alternatives = Vec::new();
        loop {
            alternatives.push(self.parse_sequence(true)?);
            match self.advance() {
                Some(',') => continue,
                Some('}') => break,
                _ => return Err("unexpected end of input".to_owned()),
            }
        }
        Ok(alternatives)
    }

    /// `fetchRange` + `parserRange`. The opening bracket has been consumed.
    fn parse_range(&mut self) -> Result<Token, String> {
        self.pos += 1;

        let mut not = false;
        let mut seen_not = false;
        let mut want_hi = false;
        let mut want_close = false;
        let mut lo: Option<char> = None;
        let mut hi: Option<char> = None;
        let mut chars: Option<String> = None;

        loop {
            if want_close {
                match self.advance() {
                    Some(']') => {}
                    Some(_) => return Err("expected close range character".to_owned()),
                    None => return Err("unexpected end of input".to_owned()),
                }
                break;
            }
            if want_hi {
                let Some(character) = self.advance() else {
                    return Err("unexpected end of input".to_owned());
                };
                hi = Some(character);
                want_close = true;
                continue;
            }

            let Some(c) = self.advance() else {
                return Err("unexpected end of input".to_owned());
            };
            if !seen_not && c == '!' {
                seen_not = true;
                not = true;
                continue;
            }
            if self.peek() == Some('-') {
                self.pos += 1;
                lo = Some(c);
                want_hi = true;
                continue;
            }

            // Unread `c` and read the literal run up to the closing bracket.
            self.pos -= 1;
            let mut text = String::new();
            loop {
                let Some(c) = self.advance() else {
                    return Err("unexpected end of input".to_owned());
                };
                if c == ']' {
                    self.pos -= 1;
                    break;
                }
                if c == '\\' {
                    if let Some(escaped) = self.advance() {
                        text.push(escaped);
                    }
                    continue;
                }
                text.push(c);
            }
            chars = Some(text);
            want_close = true;
        }

        let is_range = lo.is_some() && hi.is_some();
        let is_chars = chars.as_ref().is_some_and(|text| !text.is_empty());
        if is_chars == is_range {
            return Err("could not parse range".to_owned());
        }
        match (lo, hi) {
            (Some(lo), Some(hi)) => {
                if hi < lo {
                    return Err(format!(
                        "hi character '{hi}' should be greater than lo '{lo}'"
                    ));
                }
                Ok(Token::Range { lo, hi, not })
            }
            _ => Ok(Token::List {
                chars: chars.unwrap_or_default().chars().collect(),
                not,
            }),
        }
    }
}

fn push_text(tokens: &mut Vec<Token>, character: char) {
    match tokens.last_mut() {
        Some(Token::Text(existing)) => existing.push(character),
        _ => tokens.push(Token::Text(character.to_string())),
    }
}

/// Expands every [`Token::AnyOf`] into the cross product of its alternatives.
fn expand(tokens: &[Token]) -> Vec<Vec<Token>> {
    let Some((first, rest)) = tokens.split_first() else {
        return vec![Vec::new()];
    };
    let suffixes = expand(rest);
    match first {
        Token::AnyOf(alternatives) => {
            let mut result = Vec::new();
            for alternative in alternatives {
                for expanded in expand(alternative) {
                    for suffix in &suffixes {
                        let mut sequence = expanded.clone();
                        sequence.extend(suffix.iter().cloned());
                        result.push(sequence);
                    }
                }
            }
            result
        }
        token => {
            let mut result = Vec::with_capacity(suffixes.len());
            for suffix in suffixes {
                let mut sequence = Vec::with_capacity(suffix.len() + 1);
                sequence.push(token.clone());
                sequence.extend(suffix);
                result.push(sequence);
            }
            result
        }
    }
}

fn match_sequence(tokens: &[Token], value: &[char]) -> bool {
    let Some((first, rest)) = tokens.split_first() else {
        return value.is_empty();
    };
    match first {
        // `AnyOf` is removed by `expand`.
        Token::AnyOf(_) => false,
        Token::Text(text) => {
            let mut offset = 0;
            for c in text.chars() {
                if value.get(offset) != Some(&c) {
                    return false;
                }
                offset += 1;
            }
            match_sequence(rest, &value[offset..])
        }
        Token::Single => !value.is_empty() && match_sequence(rest, &value[1..]),
        Token::Range { lo, hi, not } => match value.first() {
            Some(c) => ((lo <= c && c <= hi) != *not) && match_sequence(rest, &value[1..]),
            None => false,
        },
        Token::List { chars, not } => match value.first() {
            Some(c) => (chars.contains(c) != *not) && match_sequence(rest, &value[1..]),
            None => false,
        },
        Token::Any => (0..=value.len())
            .rev()
            .any(|offset| match_sequence(rest, &value[offset..])),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn matches(pattern: &str, value: &str) -> bool {
        Glob::compile(pattern).unwrap().matches(value)
    }

    #[test]
    fn literals_and_wildcards() {
        assert!(matches("main", "main"));
        assert!(!matches("main", "main2"));
        assert!(matches("refs/heads/*", "refs/heads/main"));
        assert!(matches("*", "refs/heads/main"));
        assert!(matches("**", "a/b"));
        assert!(matches("v*", "v1.2.3"));
        assert!(matches("?ain", "main"));
        assert!(!matches("?ain", "ain"));
        assert!(matches("", ""));
        assert!(!matches("", "a"));
    }

    #[test]
    fn character_lists_and_ranges() {
        assert!(matches("[vt]ags", "tags"));
        assert!(!matches("[vt]ags", "wags"));
        assert!(matches("[a-z]*", "release"));
        assert!(matches("[!a-z]*", "Release"));
        assert!(!matches("[!a-z]*", "release"));
        // gobwas rejects these: the first `]` of `[]]` closes an empty set
        // ("could not parse range"), and `[a-]` wants a hi character before
        // the closing bracket ("unexpected end of input").
        assert_eq!(Glob::compile("[]]").unwrap_err(), "could not parse range");
        assert_eq!(
            Glob::compile("[a-]").unwrap_err(),
            "unexpected end of input"
        );
    }

    #[test]
    fn braces_and_escapes() {
        assert!(matches("{main,dev}", "main"));
        assert!(matches("{main,dev}", "dev"));
        assert!(!matches("{main,dev}", "release"));
        assert!(matches("refs/{heads,tags}/*", "refs/tags/v1"));
        assert!(matches("{a,{b,c}}", "c"));
        assert!(matches("{a,}", ""));
        assert!(matches(r"a\*b", "a*b"));
        assert!(!matches(r"a\*b", "axb"));
        assert!(matches("trailing\\", "trailing"));
        assert!(matches("a}b", "a}b"));
        assert!(matches("a,b", "a,b"));
    }

    #[test]
    fn syntax_errors_match_the_baseline_classes() {
        assert!(Glob::compile("[abc").is_err());
        assert!(Glob::compile("[]").is_err());
        assert!(Glob::compile("[!]").is_err());
        assert!(Glob::compile("[z-a]").is_err());
        assert!(Glob::compile("[a-z0-9]").is_err());
        assert!(Glob::compile("{a,b").is_err());
        assert!(Glob::compile("a{b").is_err());
    }
}
