//! `jsonx/visitor.go`: the fault-tolerant recursive-descent walker.

use crate::scanner::{ScanErrorCode, ScanOptions, Scanner, SyntaxKind};
use crate::tree::Literal;

/// `jsonx.ParseOptions`.
#[derive(Debug, Clone, Copy, Default)]
pub struct ParseOptions {
    /// Allow comments (`//` and `/* ... */`).
    pub comments: bool,
    /// Allow trailing commas in objects and arrays.
    pub trailing_commas: bool,
}

/// `jsonx.ParseErrorCode`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ParseErrorCode {
    InvalidSymbol,
    InvalidNumberFormat,
    PropertyNameExpected,
    ValueExpected,
    ColonExpected,
    CommaExpected,
    CloseBraceExpected,
    CloseBracketExpected,
    EndOfFileExpected,
    InvalidCommentToken,
    ParseErrorUnexpectedEndOfComment,
    ParseErrorUnexpectedEndOfString,
    ParseErrorUnexpectedEndOfNumber,
    ParseErrorInvalidUnicode,
    ParseErrorInvalidEscapeCharacter,
    ParseErrorInvalidCharacter,
    InvalidScanErrorCode,
}

/// `jsonx.Visitor`: every callback defaults to a no-op.
pub trait Visitor {
    fn on_object_begin(&mut self, _offset: usize, _length: usize) {}
    fn on_object_property(&mut self, _property: &str, _offset: usize, _length: usize) {}
    fn on_object_end(&mut self, _offset: usize, _length: usize) {}
    fn on_array_begin(&mut self, _offset: usize, _length: usize) {}
    fn on_array_end(&mut self, _offset: usize, _length: usize) {}
    fn on_literal_value(&mut self, _value: &Literal, _offset: usize, _length: usize) {}
    fn on_separator(&mut self, _character: char, _offset: usize, _length: usize) {}
    fn on_error(&mut self, _error_code: ParseErrorCode, _offset: usize, _length: usize) {}
}

/// `jsonx.Walk`: parses the document and calls `visitor` for each element.
/// Returns false when no value could be parsed at the top level.
pub fn walk(text: &str, options: ParseOptions, visitor: &mut dyn Visitor) -> bool {
    let scanner = Scanner::new(text, ScanOptions { trivia: true });
    let mut walker = Walker {
        scanner,
        options,
        visitor,
    };
    walker.walk()
}

struct Walker<'a> {
    scanner: Scanner,
    options: ParseOptions,
    visitor: &'a mut dyn Visitor,
}

impl Walker<'_> {
    fn walk(&mut self) -> bool {
        self.scan_next();
        if self.scanner.token() == SyntaxKind::EOF {
            return true;
        }
        if !self.parse_value() {
            self.handle_error(ParseErrorCode::ValueExpected, &[], &[]);
            return false;
        }
        if self.scanner.token() != SyntaxKind::EOF {
            self.handle_error(ParseErrorCode::EndOfFileExpected, &[], &[]);
        }
        true
    }

    fn on_object_begin(&mut self) {
        self.visitor
            .on_object_begin(self.scanner.token_offset(), self.scanner.token_length());
    }

    fn on_object_property(&mut self, property: &str) {
        self.visitor.on_object_property(
            property,
            self.scanner.token_offset(),
            self.scanner.token_length(),
        );
    }

    fn on_object_end(&mut self) {
        self.visitor
            .on_object_end(self.scanner.token_offset(), self.scanner.token_length());
    }

    fn on_array_begin(&mut self) {
        self.visitor
            .on_array_begin(self.scanner.token_offset(), self.scanner.token_length());
    }

    fn on_array_end(&mut self) {
        self.visitor
            .on_array_end(self.scanner.token_offset(), self.scanner.token_length());
    }

    fn on_literal_value(&mut self, value: &Literal) {
        self.visitor.on_literal_value(
            value,
            self.scanner.token_offset(),
            self.scanner.token_length(),
        );
    }

    fn on_separator(&mut self, character: char) {
        self.visitor.on_separator(
            character,
            self.scanner.token_offset(),
            self.scanner.token_length(),
        );
    }

    fn on_error(&mut self, error_code: ParseErrorCode) {
        self.visitor.on_error(
            error_code,
            self.scanner.token_offset(),
            self.scanner.token_length(),
        );
    }

    fn scan_next(&mut self) -> SyntaxKind {
        loop {
            let token = self.scanner.scan();
            match self.scanner.err() {
                ScanErrorCode::None => {}
                ScanErrorCode::UnexpectedEndOfComment => {
                    if self.options.comments {
                        self.handle_error(
                            ParseErrorCode::ParseErrorUnexpectedEndOfComment,
                            &[],
                            &[],
                        );
                    }
                }
                ScanErrorCode::UnexpectedEndOfString => {
                    self.handle_error(ParseErrorCode::ParseErrorUnexpectedEndOfString, &[], &[]);
                }
                ScanErrorCode::UnexpectedEndOfNumber => {
                    self.handle_error(ParseErrorCode::ParseErrorUnexpectedEndOfNumber, &[], &[]);
                }
                ScanErrorCode::InvalidUnicode => {
                    self.handle_error(ParseErrorCode::ParseErrorInvalidUnicode, &[], &[]);
                }
                ScanErrorCode::InvalidEscapeCharacter => {
                    self.handle_error(ParseErrorCode::ParseErrorInvalidEscapeCharacter, &[], &[]);
                }
                ScanErrorCode::InvalidCharacter => {
                    self.handle_error(ParseErrorCode::ParseErrorInvalidCharacter, &[], &[]);
                }
            }

            match token {
                SyntaxKind::LineCommentTrivia | SyntaxKind::BlockCommentTrivia => {
                    if !self.options.comments {
                        self.handle_error(ParseErrorCode::InvalidCommentToken, &[], &[]);
                    }
                }
                SyntaxKind::Unknown => {
                    self.handle_error(ParseErrorCode::InvalidSymbol, &[], &[]);
                }
                SyntaxKind::Trivia | SyntaxKind::LineBreakTrivia => {}
                _ => return token,
            }
        }
    }

    fn handle_error(
        &mut self,
        error_code: ParseErrorCode,
        skip_until_after: &[SyntaxKind],
        skip_until: &[SyntaxKind],
    ) {
        self.on_error(error_code);
        if !skip_until_after.is_empty() || !skip_until.is_empty() {
            let mut token = self.scanner.token();
            while token != SyntaxKind::EOF {
                if skip_until_after.contains(&token) {
                    self.scan_next();
                    break;
                } else if skip_until.contains(&token) {
                    break;
                }
                token = self.scan_next();
            }
        }
    }

    fn parse_string(&mut self, is_value: bool) -> bool {
        let value = self.scanner.value();
        if is_value {
            self.on_literal_value(&Literal::Str(value));
        } else {
            self.on_object_property(&value);
        }
        self.scan_next();
        true
    }

    fn parse_literal(&mut self) -> bool {
        match self.scanner.token() {
            SyntaxKind::NumericLiteral => {
                let value = self.scanner.value();
                if value.parse::<f64>().is_err() {
                    self.handle_error(ParseErrorCode::InvalidNumberFormat, &[], &[]);
                }
                self.on_literal_value(&Literal::Number(value));
            }
            SyntaxKind::NullKeyword => self.on_literal_value(&Literal::Null),
            SyntaxKind::TrueKeyword => self.on_literal_value(&Literal::Bool(true)),
            SyntaxKind::FalseKeyword => self.on_literal_value(&Literal::Bool(false)),
            _ => return false,
        }
        self.scan_next();
        true
    }

    fn parse_property(&mut self) -> bool {
        if self.scanner.token() != SyntaxKind::StringLiteral {
            self.handle_error(
                ParseErrorCode::PropertyNameExpected,
                &[],
                &[SyntaxKind::CloseBraceToken, SyntaxKind::CommaToken],
            );
            return false;
        }
        self.parse_string(false);
        if self.scanner.token() == SyntaxKind::ColonToken {
            self.on_separator(':');
            self.scan_next(); // consume colon

            if !self.parse_value() {
                self.handle_error(
                    ParseErrorCode::ValueExpected,
                    &[],
                    &[SyntaxKind::CloseBraceToken, SyntaxKind::CommaToken],
                );
            }
        } else {
            self.handle_error(
                ParseErrorCode::ColonExpected,
                &[],
                &[SyntaxKind::CloseBraceToken, SyntaxKind::CommaToken],
            );
        }
        true
    }

    fn parse_object(&mut self) -> bool {
        self.on_object_begin();
        self.scan_next(); // consume open brace

        let mut needs_comma = false;
        while self.scanner.token() != SyntaxKind::CloseBraceToken
            && self.scanner.token() != SyntaxKind::EOF
        {
            if self.scanner.token() == SyntaxKind::CommaToken {
                if !needs_comma {
                    self.handle_error(ParseErrorCode::ValueExpected, &[], &[]);
                }
                self.on_separator(',');
                self.scan_next(); // consume comma
                if self.scanner.token() == SyntaxKind::CloseBraceToken
                    && self.options.trailing_commas
                {
                    break;
                }
            } else if needs_comma {
                self.handle_error(ParseErrorCode::CommaExpected, &[], &[]);
            }
            if !self.parse_property() {
                self.handle_error(
                    ParseErrorCode::ValueExpected,
                    &[],
                    &[SyntaxKind::CloseBraceToken, SyntaxKind::CommaToken],
                );
            }
            needs_comma = true;
        }
        self.on_object_end();
        if self.scanner.token() != SyntaxKind::CloseBraceToken {
            self.handle_error(
                ParseErrorCode::CloseBraceExpected,
                &[SyntaxKind::CloseBraceToken],
                &[],
            );
        } else {
            self.scan_next(); // consume close brace
        }
        true
    }

    fn parse_array(&mut self) -> bool {
        self.on_array_begin();
        self.scan_next(); // consume open bracket

        let mut needs_comma = false;
        while self.scanner.token() != SyntaxKind::CloseBracketToken
            && self.scanner.token() != SyntaxKind::EOF
        {
            if self.scanner.token() == SyntaxKind::CommaToken {
                if !needs_comma {
                    self.handle_error(ParseErrorCode::ValueExpected, &[], &[]);
                }
                self.on_separator(',');
                self.scan_next(); // consume comma
                if self.scanner.token() == SyntaxKind::CloseBracketToken
                    && self.options.trailing_commas
                {
                    break;
                }
            } else if needs_comma {
                self.handle_error(ParseErrorCode::CommaExpected, &[], &[]);
            }
            if !self.parse_value() {
                self.handle_error(
                    ParseErrorCode::ValueExpected,
                    &[],
                    &[SyntaxKind::CloseBracketToken, SyntaxKind::CommaToken],
                );
            }
            needs_comma = true;
        }
        self.on_array_end();
        if self.scanner.token() != SyntaxKind::CloseBracketToken {
            self.handle_error(
                ParseErrorCode::CloseBracketExpected,
                &[SyntaxKind::CloseBracketToken],
                &[],
            );
        } else {
            self.scan_next(); // consume close bracket
        }
        true
    }

    fn parse_value(&mut self) -> bool {
        match self.scanner.token() {
            SyntaxKind::OpenBracketToken => self.parse_array(),
            SyntaxKind::OpenBraceToken => self.parse_object(),
            SyntaxKind::StringLiteral => self.parse_string(true),
            _ => self.parse_literal(),
        }
    }
}
