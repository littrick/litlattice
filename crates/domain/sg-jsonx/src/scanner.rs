//! `jsonx/scanner.go`: the fault-tolerant JSON scanner.
//!
//! Offsets and lengths are counted in `char`s (Go `rune`s); the scanner
//! returns decoded string values (escapes resolved) and keeps the raw
//! number lexeme, exactly like the baseline.

/// `jsonx.ScanOptions`.
#[derive(Debug, Clone, Copy, Default)]
pub struct ScanOptions {
    /// Scan and emit whitespace and comment elements (false to ignore).
    pub trivia: bool,
}

/// `jsonx.ScanErrorCode`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScanErrorCode {
    None,
    UnexpectedEndOfComment,
    UnexpectedEndOfString,
    UnexpectedEndOfNumber,
    InvalidUnicode,
    InvalidEscapeCharacter,
    InvalidCharacter,
}

/// `jsonx.SyntaxKind`. The discriminants matter: the walker compares token
/// ranges (`LineCommentTrivia..=Trivia`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum SyntaxKind {
    Unknown = 0,
    OpenBraceToken,
    CloseBraceToken,
    OpenBracketToken,
    CloseBracketToken,
    CommaToken,
    ColonToken,
    NullKeyword,
    TrueKeyword,
    FalseKeyword,
    StringLiteral,
    NumericLiteral,
    LineCommentTrivia,
    BlockCommentTrivia,
    LineBreakTrivia,
    Trivia,
    EOF,
}

/// `jsonx.Scanner`.
pub struct Scanner {
    text: Vec<char>,
    options: ScanOptions,
    pos: usize,
    len: usize,
    value: Vec<char>,
    token_offset: usize,
    token: SyntaxKind,
    err: ScanErrorCode,
}

impl Scanner {
    /// `jsonx.NewScanner`.
    pub fn new(text: &str, options: ScanOptions) -> Self {
        let text: Vec<char> = text.chars().collect();
        let len = text.len();
        Self {
            text,
            options,
            pos: 0,
            len,
            value: Vec::new(),
            token_offset: 0,
            token: SyntaxKind::Unknown,
            err: ScanErrorCode::None,
        }
    }

    pub fn pos(&self) -> usize {
        self.pos
    }

    pub fn value(&self) -> String {
        self.value.iter().collect()
    }

    pub fn token_offset(&self) -> usize {
        self.token_offset
    }

    pub fn token_length(&self) -> usize {
        self.pos - self.token_offset
    }

    pub fn token(&self) -> SyntaxKind {
        self.token
    }

    pub fn err(&self) -> ScanErrorCode {
        self.err
    }

    /// `Scanner.SetPosition`.
    pub fn set_position(&mut self, new_position: usize) {
        self.pos = new_position;
        self.value.clear();
        self.token_offset = 0;
        self.token = SyntaxKind::Unknown;
        self.err = ScanErrorCode::None;
    }

    /// `Scanner.Scan`.
    pub fn scan(&mut self) -> SyntaxKind {
        if self.options.trivia {
            self.scan_next()
        } else {
            self.scan_next_non_trivia()
        }
    }

    fn char_at(&self, index: usize) -> Option<char> {
        self.text.get(index).copied()
    }

    fn scan_hex_digits(&mut self, count: usize, exact: bool) -> i64 {
        let mut digits = 0usize;
        let mut value: i64 = 0;
        while digits < count || !exact {
            let Some(ch) = self.char_at(self.pos) else {
                break;
            };
            if ch.is_ascii_digit() {
                value = value * 16 + (ch as i64 - '0' as i64);
            } else if ('A'..='F').contains(&ch) {
                value = value * 16 + (ch as i64 - 'A' as i64) + 10;
            } else if ('a'..='f').contains(&ch) {
                value = value * 16 + (ch as i64 - 'a' as i64) + 10;
            } else {
                break;
            }
            self.pos += 1;
            digits += 1;
        }
        if digits < count {
            value = -1;
        }
        value
    }

    fn scan_number(&mut self) -> Vec<char> {
        let start = self.pos;
        if self.char_at(self.pos) == Some('0') {
            self.pos += 1;
        } else {
            self.pos += 1;
            while self.pos < self.len && is_digit(self.text[self.pos]) {
                self.pos += 1;
            }
        }
        if self.pos < self.len && self.text[self.pos] == '.' {
            self.pos += 1;
            if self.pos < self.len && is_digit(self.text[self.pos]) {
                self.pos += 1;
                while self.pos < self.len && is_digit(self.text[self.pos]) {
                    self.pos += 1;
                }
            } else {
                self.err = ScanErrorCode::UnexpectedEndOfNumber;
                return self.text[start..self.pos].to_vec();
            }
        }
        let mut end = self.pos;
        if self.pos < self.len && (self.text[self.pos] == 'e' || self.text[self.pos] == 'E') {
            self.pos += 1;
            if self.pos < self.len && (self.text[self.pos] == '+' || self.text[self.pos] == '-') {
                self.pos += 1;
            }
            if self.pos < self.len && is_digit(self.text[self.pos]) {
                self.pos += 1;
                while self.pos < self.len && is_digit(self.text[self.pos]) {
                    self.pos += 1;
                }
                end = self.pos;
            } else {
                self.err = ScanErrorCode::UnexpectedEndOfNumber;
            }
        }
        self.text[start..end].to_vec()
    }

    fn scan_string(&mut self) -> Vec<char> {
        let mut result: Vec<char> = Vec::new();
        let mut start = self.pos;

        loop {
            if self.pos >= self.len {
                result.extend_from_slice(&self.text[start..self.pos]);
                self.err = ScanErrorCode::UnexpectedEndOfString;
                break;
            }
            let ch = self.text[self.pos];
            if ch == '"' {
                result.extend_from_slice(&self.text[start..self.pos]);
                self.pos += 1;
                break;
            }
            if ch == '\\' {
                result.extend_from_slice(&self.text[start..self.pos]);
                self.pos += 1;
                if self.pos >= self.len {
                    self.err = ScanErrorCode::UnexpectedEndOfString;
                    break;
                }
                let ch = self.text[self.pos];
                self.pos += 1;
                match ch {
                    '"' => result.push('"'),
                    '\\' => result.push('\\'),
                    '/' => result.push('/'),
                    'b' => result.push('\u{8}'),
                    'f' => result.push('\u{c}'),
                    'n' => result.push('\n'),
                    'r' => result.push('\r'),
                    't' => result.push('\t'),
                    'u' => {
                        let ch = self.scan_hex_digits(4, true);
                        if ch >= 0 {
                            // Go appends the rune; surrogates are kept as-is.
                            if let Some(c) = char::from_u32(ch as u32) {
                                result.push(c);
                            } else {
                                result.push(char::REPLACEMENT_CHARACTER);
                            }
                        } else {
                            self.err = ScanErrorCode::InvalidUnicode;
                        }
                    }
                    _ => self.err = ScanErrorCode::InvalidEscapeCharacter,
                }
                start = self.pos;
                continue;
            }
            if ch <= '\u{1f}' {
                if is_line_break(ch) {
                    result.extend_from_slice(&self.text[start..self.pos]);
                    self.err = ScanErrorCode::UnexpectedEndOfString;
                    break;
                } else {
                    self.err = ScanErrorCode::InvalidCharacter;
                    // Mark as error but continue with the string.
                }
            }
            self.pos += 1;
        }
        result
    }

    fn scan_next(&mut self) -> SyntaxKind {
        self.value.clear();
        self.err = ScanErrorCode::None;

        self.token_offset = self.pos;

        if self.pos >= self.len {
            self.token_offset = self.len;
            self.token = SyntaxKind::EOF;
            return self.token;
        }

        let mut code = self.text[self.pos];

        // Trivia: whitespace.
        if is_white_space(code) {
            loop {
                self.pos += 1;
                self.value.push(code);
                if self.pos >= self.len {
                    break;
                }
                code = self.text[self.pos];
                if !is_white_space(code) {
                    break;
                }
            }
            self.token = SyntaxKind::Trivia;
            return self.token;
        }

        // Trivia: newlines.
        if is_line_break(code) {
            self.pos += 1;
            self.value.push(code);
            if code == '\r' && self.pos < self.len && self.text[self.pos] == '\n' {
                self.pos += 1;
                self.value.push('\n');
            }
            self.token = SyntaxKind::LineBreakTrivia;
            return self.token;
        }

        match code {
            '{' => {
                self.pos += 1;
                self.token = SyntaxKind::OpenBraceToken;
            }
            '}' => {
                self.pos += 1;
                self.token = SyntaxKind::CloseBraceToken;
            }
            '[' => {
                self.pos += 1;
                self.token = SyntaxKind::OpenBracketToken;
            }
            ']' => {
                self.pos += 1;
                self.token = SyntaxKind::CloseBracketToken;
            }
            ':' => {
                self.pos += 1;
                self.token = SyntaxKind::ColonToken;
            }
            ',' => {
                self.pos += 1;
                self.token = SyntaxKind::CommaToken;
            }
            '"' => {
                self.pos += 1;
                self.value = self.scan_string();
                self.token = SyntaxKind::StringLiteral;
            }
            '/' => {
                let start = self.pos.saturating_sub(1);
                // Single-line comment.
                if self.pos + 1 < self.text.len() && self.text[self.pos + 1] == '/' {
                    self.pos += 2;
                    while self.pos < self.len {
                        if is_line_break(self.text[self.pos]) {
                            break;
                        }
                        self.pos += 1;
                    }
                    self.value = self.text[start..self.pos].to_vec();
                    self.token = SyntaxKind::LineCommentTrivia;
                    return self.token;
                }
                // Multi-line comment.
                if self.pos + 1 < self.text.len() && self.text[self.pos + 1] == '*' {
                    self.pos += 2;
                    let safe_length = self.len.saturating_sub(1);
                    let mut comment_closed = false;
                    while self.pos < safe_length {
                        let ch = self.text[self.pos];
                        if ch == '*' && self.text[self.pos + 1] == '/' {
                            self.pos += 2;
                            comment_closed = true;
                            break;
                        }
                        self.pos += 1;
                    }
                    if !comment_closed {
                        self.pos += 1;
                        self.err = ScanErrorCode::UnexpectedEndOfComment;
                    }
                    self.value = self.text[start..self.pos].to_vec();
                    self.token = SyntaxKind::BlockCommentTrivia;
                    return self.token;
                }
                // Just a single slash.
                self.value.push(code);
                self.pos += 1;
                self.token = SyntaxKind::Unknown;
            }
            '-' => {
                self.value.push(code);
                self.pos += 1;
                if self.pos == self.len || !is_digit(self.text[self.pos]) {
                    self.token = SyntaxKind::Unknown;
                    return self.token;
                }
                let number = self.scan_number();
                self.value.extend(number);
                self.token = SyntaxKind::NumericLiteral;
            }
            '0'..='9' => {
                let number = self.scan_number();
                self.value.extend(number);
                self.token = SyntaxKind::NumericLiteral;
            }
            _ => {
                // Is a literal? Read the full word.
                while self.pos < self.len && is_unknown_content_character(code) {
                    self.pos += 1;
                    if self.pos == self.len {
                        break;
                    }
                    code = self.text[self.pos];
                }
                if self.token_offset != self.pos {
                    self.value = self.text[self.token_offset..self.pos].to_vec();
                    let word: String = self.value.iter().collect();
                    self.token = match word.as_str() {
                        "true" => SyntaxKind::TrueKeyword,
                        "false" => SyntaxKind::FalseKeyword,
                        "null" => SyntaxKind::NullKeyword,
                        _ => SyntaxKind::Unknown,
                    };
                    return self.token;
                }
                self.value.push(code);
                self.pos += 1;
                self.token = SyntaxKind::Unknown;
            }
        }
        self.token
    }

    fn scan_next_non_trivia(&mut self) -> SyntaxKind {
        loop {
            let result = self.scan_next();
            if !(result >= SyntaxKind::LineCommentTrivia && result <= SyntaxKind::Trivia) {
                return result;
            }
        }
    }
}

/// `jsonx.isWhiteSpace`.
pub fn is_white_space(ch: char) -> bool {
    ch == ' '
        || ch == '\t'
        || ch == '\u{b}'
        || ch == '\u{c}'
        || ch == '\u{a0}'
        || ch == '\u{1680}'
        || ('\u{2000}'..='\u{200b}').contains(&ch)
        || ch == '\u{202f}'
        || ch == '\u{205f}'
        || ch == '\u{3000}'
        || ch == '\u{feff}'
}

/// `jsonx.isLineBreak`.
pub fn is_line_break(ch: char) -> bool {
    ch == '\n' || ch == '\r' || ch == '\u{2028}' || ch == '\u{2029}'
}

/// `jsonx.isDigit`.
pub fn is_digit(ch: char) -> bool {
    ch.is_ascii_digit()
}

fn is_unknown_content_character(code: char) -> bool {
    if is_white_space(code) || is_line_break(code) {
        return false;
    }
    !matches!(code, '}' | ']' | '{' | '[' | '"' | ':' | ',')
}
