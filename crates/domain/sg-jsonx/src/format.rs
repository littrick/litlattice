//! `jsonx/format.go`: the JSON formatter (VS Code `jsonFormatter.ts`).

use crate::edit::Edit;
use crate::scanner::{ScanOptions, Scanner, SyntaxKind};

/// `jsonx.FormatOptions`.
#[derive(Debug, Clone, Default)]
pub struct FormatOptions {
    pub tab_size: i64,
    pub insert_spaces: bool,
    pub eol: String,
}

/// `conf.FormatOptions` (`internal/conf/conf.go:252-254`).
pub fn settings_format_options() -> FormatOptions {
    FormatOptions {
        tab_size: 2,
        insert_spaces: true,
        eol: "\n".to_owned(),
    }
}

/// `jsonx.Format`: format edits for the whole document.
pub fn format(text: &str, options: &FormatOptions) -> Vec<Edit> {
    let chars: Vec<char> = text.chars().collect();
    format_range(text, 0, chars.len(), options)
}

/// `jsonx.FormatRange`.
pub fn format_range(
    text: &str,
    offset: usize,
    length: usize,
    options: &FormatOptions,
) -> Vec<Edit> {
    let chars: Vec<char> = text.chars().collect();

    let mut range_start = offset;
    let mut range_end = range_start + length;
    while range_start > 0 && !is_eol(&chars, range_start - 1) {
        range_start -= 1;
    }

    {
        let mut scanner = Scanner::new(text, ScanOptions { trivia: false });
        scanner.set_position(range_end);
        scanner.scan();
        range_end = scanner.pos();
    }

    let value: String = chars[range_start..range_end].iter().collect();
    let value_chars: Vec<char> = value.chars().collect();
    let initial_indent_level = compute_indent_level(&value_chars, 0, options);

    let eol = get_eol(options, &chars);

    let indent_value = if options.insert_spaces {
        " ".repeat(usize::try_from(options.tab_size).unwrap_or(0))
    } else {
        "\t".to_owned()
    };

    let scanner = Scanner::new(&value, ScanOptions { trivia: true });
    let mut formatter = Formatter {
        input: chars,
        scanner,
        eol,
        indent_level: 0,
        indent_value,
        initial_indent_level,
        line_break: false,
        edits: Vec::new(),
    };
    formatter.format(range_start, range_end)
}

struct Formatter {
    input: Vec<char>,
    scanner: Scanner,
    eol: String,
    indent_level: i64,
    indent_value: String,
    initial_indent_level: i64,
    line_break: bool,
    edits: Vec<Edit>,
}

impl Formatter {
    fn format(&mut self, range_start: usize, _range_end: usize) -> Vec<Edit> {
        let mut first_token = self.scan_next();
        if first_token != SyntaxKind::EOF {
            let first_token_start = self.scanner.token_offset() + range_start;
            let initial_indent = self
                .indent_value
                .repeat(usize::try_from(self.initial_indent_level).unwrap_or(0));
            self.add_edit(&initial_indent, range_start, first_token_start);
        }

        while first_token != SyntaxKind::EOF {
            let mut first_token_end =
                self.scanner.token_offset() + self.scanner.token_length() + range_start;
            let mut second_token = self.scan_next();

            let mut replace_content = String::new();
            while !self.line_break
                && (second_token == SyntaxKind::LineCommentTrivia
                    || second_token == SyntaxKind::BlockCommentTrivia)
            {
                // Comments on the same line: keep them on the same line, but
                // ignore them otherwise.
                let comment_token_start = self.scanner.token_offset() + range_start;
                self.add_edit(" ", first_token_end, comment_token_start);
                first_token_end =
                    self.scanner.token_offset() + self.scanner.token_length() + range_start;
                if second_token == SyntaxKind::LineCommentTrivia {
                    replace_content = self.new_line_and_indent();
                } else {
                    replace_content = String::new();
                }
                second_token = self.scan_next();
            }

            if second_token == SyntaxKind::CloseBraceToken {
                if first_token != SyntaxKind::OpenBraceToken {
                    self.indent_level -= 1;
                    replace_content = self.new_line_and_indent();
                }
            } else if second_token == SyntaxKind::CloseBracketToken {
                if first_token != SyntaxKind::OpenBracketToken {
                    self.indent_level -= 1;
                    replace_content = self.new_line_and_indent();
                }
            } else if second_token != SyntaxKind::EOF {
                match first_token {
                    SyntaxKind::OpenBracketToken | SyntaxKind::OpenBraceToken => {
                        self.indent_level += 1;
                        replace_content = self.new_line_and_indent();
                    }
                    SyntaxKind::CommaToken | SyntaxKind::LineCommentTrivia => {
                        replace_content = self.new_line_and_indent();
                    }
                    SyntaxKind::BlockCommentTrivia => {
                        if self.line_break {
                            replace_content = self.new_line_and_indent();
                        } else {
                            // Symbol following comment on the same line: keep on
                            // same line, separate with ' '.
                            replace_content = " ".to_owned();
                        }
                    }
                    SyntaxKind::ColonToken => {
                        replace_content = " ".to_owned();
                    }
                    // Go's `case NullKeyword, TrueKeyword, FalseKeyword,
                    // NumericLiteral` only adds a space when the second token
                    // is one of the latter three (not TrueKeyword).
                    SyntaxKind::NullKeyword
                    | SyntaxKind::TrueKeyword
                    | SyntaxKind::FalseKeyword
                    | SyntaxKind::NumericLiteral
                        if matches!(
                            second_token,
                            SyntaxKind::NullKeyword
                                | SyntaxKind::FalseKeyword
                                | SyntaxKind::NumericLiteral
                        ) =>
                    {
                        replace_content = " ".to_owned();
                    }
                    _ => {}
                }
                if self.line_break
                    && (second_token == SyntaxKind::LineCommentTrivia
                        || second_token == SyntaxKind::BlockCommentTrivia)
                {
                    replace_content = self.new_line_and_indent();
                }
            }
            let second_token_start = self.scanner.token_offset() + range_start;
            self.add_edit(&replace_content, first_token_end, second_token_start);
            first_token = second_token;
        }
        std::mem::take(&mut self.edits)
    }

    fn new_line_and_indent(&self) -> String {
        let n = self.initial_indent_level + self.indent_level;
        let n = usize::try_from(n.max(0)).unwrap_or(0);
        format!("{}{}", self.eol, self.indent_value.repeat(n))
    }

    fn scan_next(&mut self) -> SyntaxKind {
        let mut token = self.scanner.scan();
        self.line_break = false;
        while token == SyntaxKind::Trivia || token == SyntaxKind::LineBreakTrivia {
            self.line_break = self.line_break || token == SyntaxKind::LineBreakTrivia;
            token = self.scanner.scan();
        }
        token
    }

    fn add_edit(&mut self, text: &str, start_offset: usize, end_offset: usize) {
        let current: String = self.input[start_offset..end_offset].iter().collect();
        if current != text {
            self.edits.push(Edit {
                offset: start_offset as i64,
                length: (end_offset - start_offset) as i64,
                content: text.to_owned(),
            });
        }
    }
}

fn compute_indent_level(chars: &[char], _offset: usize, options: &FormatOptions) -> i64 {
    let mut i = 0usize;
    let mut n_chars: i64 = 0;
    let mut tab_size = options.tab_size;
    if tab_size == 0 {
        tab_size = 4;
    }
    while i < chars.len() {
        let ch = chars[i];
        if ch == ' ' {
            n_chars += 1;
        } else if ch == '\t' {
            n_chars += tab_size;
        } else {
            break;
        }
        i += 1;
    }
    n_chars / tab_size
}

fn get_eol(options: &FormatOptions, chars: &[char]) -> String {
    for (i, &ch) in chars.iter().enumerate() {
        if ch == '\r' {
            if i + 1 < chars.len() && chars[i + 1] == '\n' {
                return "\r\n".to_owned();
            }
            return "\r".to_owned();
        } else if ch == '\n' {
            return "\n".to_owned();
        }
    }
    if !options.eol.is_empty() {
        return options.eol.clone();
    }
    "\n".to_owned()
}

fn is_eol(chars: &[char], offset: usize) -> bool {
    matches!(chars[offset], '\r' | '\n')
}
