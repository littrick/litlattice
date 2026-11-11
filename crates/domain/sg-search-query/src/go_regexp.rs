#![forbid(unsafe_code)]

//! Go `regexp` (RE2, `syntax.Perl`) validity and error text.
//!
//! `validate.go` compiles user patterns with `github.com/grafana/regexp`
//! (`regexp.Compile`), and predicates parse arguments with `regexp/syntax`
//! (`syntax.Parse(_, syntax.Perl)`). Both return `*syntax.Error` whose
//! `Error()` is `error parsing regexp: <code>: `<expr>``.
//!
//! This module reproduces that contract with `regex-syntax` plus a few
//! Go-specific pre-checks. See the M9.9e overview §3 for the RE2/Rust syntax
//! differences that are deliberately not covered (set operations in classes,
//! nesting-size limits, exotic Unicode tables).

use regex_syntax::ast::ErrorKind;

struct GoError {
    /// Approximate byte offset where Go's parser detects the error; used to
    /// pick the earliest failure when several checks apply.
    pos: usize,
    code: &'static str,
    expr: String,
}

/// `regexp.Compile` (and `syntax.Parse(_, syntax.Perl)`): `Ok(())` when the
/// pattern is accepted, otherwise Go's `Error.Error()` text.
pub fn compile(pattern: &str) -> Result<(), String> {
    let folded = fold_quoted_literals(pattern);
    let (captured, capture_error) = normalize_capture_names(&folded);
    let (source, flag_error) = normalize_perl_flags(&captured);

    let mut candidates: Vec<GoError> = Vec::new();
    if let Some(e) = scan_go_regexp_error(&source) {
        candidates.push(e);
    }
    if let Some(e) = flag_error {
        candidates.push(e);
    }
    if let Some(e) = capture_error {
        candidates.push(e);
    }
    if let Some(e) = scan_posix_class_error(&source) {
        candidates.push(e);
    }

    // Go treats a `{` that does not begin a valid counted repetition as a
    // literal (`a{`, `a{}`, `a{x}`, `a{1,2` are all valid); escaping those
    // braces stops `regex-syntax` from reporting a repetition error.
    let ast_source = normalize_for_ast(&source);
    match regex_syntax::ast::parse::Parser::new().parse(&ast_source) {
        Ok(_) => {}
        // Go treats a `{` that does not begin a valid counted repetition as a
        // literal (`a{`, `a{}`, `a{x}` are all valid).
        Err(err)
            if matches!(
                err.kind(),
                ErrorKind::RepetitionCountDecimalEmpty | ErrorKind::RepetitionCountUnclosed
            ) => {}
        Err(err) => {
            let pos = match err.kind() {
                // Detected when the parser reaches EOF.
                ErrorKind::GroupUnclosed | ErrorKind::ClassUnclosed => ast_source.len(),
                _ => err.span().start.offset.min(ast_source.len()),
            };
            let (code, expr) = map_ast_error(&ast_source, &err);
            candidates.push(GoError { pos, code, expr });
        }
    }

    match candidates.into_iter().min_by_key(|e| e.pos) {
        Some(e) => Err(go_message(e.code, &e.expr)),
        None => Ok(()),
    }
}

/// `regexp/syntax.Error.Error()` (`parse.go:17-23`).
fn go_message(code: &str, expr: &str) -> String {
    format!("error parsing regexp: {code}: `{expr}`")
}

/// Go's `\Q...\E` quoted literal. `regex-syntax` does not know `\Q`, so the
/// quoted span is replaced by its escaped literal form before parsing. A
/// missing `\E` quotes to the end of the pattern, like Go.
fn fold_quoted_literals(pattern: &str) -> String {
    let chars: Vec<char> = pattern.chars().collect();
    let mut out = String::with_capacity(pattern.len());
    let mut i = 0;
    while i < chars.len() {
        if chars[i] == '\\' && i + 1 < chars.len() {
            match chars[i + 1] {
                'Q' => {
                    i += 2;
                    let mut literal = String::new();
                    while i < chars.len() {
                        if chars[i] == '\\' && i + 1 < chars.len() && chars[i + 1] == 'E' {
                            i += 2;
                            break;
                        }
                        literal.push(chars[i]);
                        i += 1;
                    }
                    out.push_str(&regex_syntax::escape(&literal));
                }
                _ => {
                    out.push(chars[i]);
                    out.push(chars[i + 1]);
                    i += 2;
                }
            }
        } else {
            out.push(chars[i]);
            i += 1;
        }
    }
    out
}

/// Go's named captures accept `[A-Za-z0-9_]+` and duplicate names, while
/// `regex-syntax` requires a leading non-digit and unique names. Rewrite every
/// well-formed name to a unique synthetic one, and reproduce Go's
/// `invalid named capture` errors for the rest.
fn normalize_capture_names(source: &str) -> (String, Option<GoError>) {
    let bytes = source.as_bytes();
    let mut out = String::with_capacity(source.len());
    let mut i = 0;
    let mut next_name = 0usize;
    while i < bytes.len() {
        match bytes[i] {
            b'\\' => {
                let end = advance_escape(source, i);
                out.push_str(&source[i..end]);
                i = end;
            }
            b'[' => {
                let start = i;
                i += 1;
                while i < bytes.len() {
                    if bytes[i] == b'\\' {
                        i = advance_escape(source, i);
                    } else if bytes[i] == b']' {
                        i += 1;
                        break;
                    } else {
                        i += 1;
                    }
                }
                out.push_str(&source[start..i.min(source.len())]);
            }
            b'(' if bytes.get(i + 1) == Some(&b'?') => {
                let starts_with_p = bytes.get(i + 2) == Some(&b'P')
                    && bytes.get(i + 3) == Some(&b'<')
                    && bytes.len() - i > 4;
                let starts_with_name = bytes.get(i + 2) == Some(&b'<') && bytes.len() - i > 3;
                if starts_with_p || starts_with_name {
                    let expr_start = if starts_with_name { i + 3 } else { i + 4 };
                    match bytes[expr_start..].iter().position(|b| *b == b'>') {
                        None => {
                            return (
                                source.to_owned(),
                                Some(GoError {
                                    pos: i,
                                    code: "invalid named capture",
                                    expr: source[i..].to_owned(),
                                }),
                            );
                        }
                        Some(offset) => {
                            let gt = expr_start + offset;
                            let name = &source[expr_start..gt];
                            let valid = !name.is_empty()
                                && name
                                    .bytes()
                                    .all(|b| b.is_ascii_alphanumeric() || b == b'_');
                            if !valid {
                                return (
                                    source.to_owned(),
                                    Some(GoError {
                                        pos: i,
                                        code: "invalid named capture",
                                        expr: source[i..=gt].to_owned(),
                                    }),
                                );
                            }
                            out.push_str(&source[i..expr_start]);
                            out.push_str(&format!("g{next_name}"));
                            out.push('>');
                            next_name += 1;
                            i = gt + 1;
                        }
                    }
                } else {
                    out.push('(');
                    i += 1;
                }
            }
            _ => {
                let end = next_char_end(source, i);
                out.push_str(&source[i..end]);
                i = end;
            }
        }
    }
    (out, None)
}

/// Index just past the character starting at `i`.
fn next_char_end(source: &str, i: usize) -> usize {
    let mut end = i + 1;
    while end < source.len() && !source.is_char_boundary(end) {
        end += 1;
    }
    end
}

/// Index just past the escape sequence starting at `\` (`i`), so that a `{`
/// belonging to `\x{...}`/`\p{...}` is not mistaken for a repetition.
fn advance_escape(source: &str, i: usize) -> usize {
    let bytes = source.as_bytes();
    if i + 1 >= bytes.len() {
        return bytes.len();
    }
    match bytes[i + 1] {
        b'x' => {
            let mut j = i + 2;
            if bytes.get(j) == Some(&b'{') {
                j += 1;
                while j < bytes.len() && bytes[j] != b'}' {
                    j += 1;
                }
                if j < bytes.len() {
                    j += 1;
                }
                j
            } else {
                let mut digits = 0;
                while j < bytes.len() && digits < 2 && bytes[j].is_ascii_hexdigit() {
                    j += 1;
                    digits += 1;
                }
                j
            }
        }
        b'p' | b'P' => {
            let mut j = i + 2;
            if bytes.get(j) == Some(&b'{') {
                j += 1;
                while j < bytes.len() && bytes[j] != b'}' {
                    j += 1;
                }
                if j < bytes.len() {
                    j += 1;
                }
                j
            } else {
                next_char_end(source, j.min(source.len()))
            }
        }
        b'0'..=b'7' => {
            let mut j = i + 1;
            let mut digits = 0;
            while j < bytes.len() && digits < 3 && (b'0'..=b'7').contains(&bytes[j]) {
                j += 1;
                digits += 1;
            }
            j
        }
        _ => next_char_end(source, i + 1),
    }
}

/// Inline checks that `regex-syntax` does not perform like Go's parser:
/// trailing backslash, nested repetition operators, and the 1000-repeat limit.
fn scan_go_regexp_error(source: &str) -> Option<GoError> {
    let bytes = source.as_bytes();
    let mut i = 0;
    // Byte range of the repetition operator attached to the previous atom.
    let mut last_repeat: Option<(usize, usize)> = None;
    while i < bytes.len() {
        match bytes[i] {
            b'\\' => {
                if i + 1 >= bytes.len() {
                    return Some(GoError {
                        pos: i,
                        code: "trailing backslash at end of expression",
                        expr: String::new(),
                    });
                }
                i = advance_escape(source, i);
                last_repeat = None;
            }
            b'[' => {
                // Skip a bracketed class; an unterminated one is reported by
                // the AST pass as `ClassUnclosed`.
                i += 1;
                while i < bytes.len() {
                    if bytes[i] == b'\\' {
                        i = advance_escape(source, i);
                    } else if bytes[i] == b']' {
                        i += 1;
                        break;
                    } else {
                        i += 1;
                    }
                }
                last_repeat = None;
            }
            b'*' | b'+' | b'?' => {
                if let Some((start, end)) = last_repeat {
                    if end == i {
                        if bytes[i] == b'?' {
                            // A trailing `?` is the non-greedy modifier and
                            // remains part of the repetition.
                            last_repeat = Some((start, i + 1));
                        } else {
                            return Some(GoError {
                                pos: start,
                                code: "invalid nested repetition operator",
                                expr: source[start..=i].to_owned(),
                            });
                        }
                        i += 1;
                        continue;
                    }
                }
                last_repeat = Some((i, i + 1));
                i += 1;
            }
            b'{' => {
                let Some((end, min, max)) = parse_counted_repeat(source, i) else {
                    // Not a counted repetition: Go treats `{` as a literal.
                    last_repeat = None;
                    i += 1;
                    continue;
                };
                if let Some((start, repeat_end)) = last_repeat {
                    if repeat_end == i {
                        return Some(GoError {
                            pos: start,
                            code: "invalid nested repetition operator",
                            expr: source[start..end].to_owned(),
                        });
                    }
                }
                let too_large = min > 1000 || max.is_some_and(|max| max > 1000 || min > max);
                if too_large {
                    return Some(GoError {
                        pos: i,
                        code: "invalid repeat count",
                        expr: source[i..end].to_owned(),
                    });
                }
                last_repeat = Some((i, end));
                i = end;
            }
            _ => {
                last_repeat = None;
                i = next_char_end(source, i);
            }
        }
    }
    None
}

/// Parse `{n}`, `{n,}`, `{n,m}` at `start`; `None` when `start` does not begin
/// a syntactically valid counted repetition (Go then treats `{` as a literal).
fn parse_counted_repeat(source: &str, start: usize) -> Option<(usize, usize, Option<usize>)> {
    let bytes = source.as_bytes();
    let mut i = start + 1;
    let first_start = i;
    while i < bytes.len() && bytes[i].is_ascii_digit() {
        i += 1;
    }
    if i == first_start {
        return None;
    }
    let min: usize = source[first_start..i].parse().ok()?;
    let mut max = None;
    if i < bytes.len() && bytes[i] == b',' {
        i += 1;
        let second_start = i;
        while i < bytes.len() && bytes[i].is_ascii_digit() {
            i += 1;
        }
        if i > second_start {
            max = Some(source[second_start..i].parse().ok()?);
        }
    }
    if i >= bytes.len() || bytes[i] != b'}' {
        return None;
    }
    Some((i + 1, min, max))
}

/// POSIX named classes Go's `posixGroup` understands
/// (`regexp/syntax/perl_groups.go`); each also accepts a leading `^`.
const POSIX_CLASS_NAMES: &[&str] = &[
    "alnum", "alpha", "ascii", "blank", "cntrl", "digit", "graph", "lower", "print", "punct",
    "space", "upper", "word", "xdigit",
];

/// `parseNamedClass` (`parse.go`): inside a character class, `[:name:]`
/// must use one of Go's POSIX names. `regex-syntax` accepts arbitrary names,
/// so report Go's `invalid character class range` here.
fn scan_posix_class_error(source: &str) -> Option<GoError> {
    let bytes = source.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'\\' => i = advance_escape(source, i),
            b'[' => {
                let mut j = i + 1;
                // A leading `^` and literal `]` do not close the class.
                if j < bytes.len() && bytes[j] == b'^' {
                    j += 1;
                }
                if j < bytes.len() && bytes[j] == b']' {
                    j += 1;
                }
                while j < bytes.len() {
                    match bytes[j] {
                        b'\\' => j = advance_escape(source, j),
                        b']' => {
                            j += 1;
                            break;
                        }
                        b'[' if j + 1 < bytes.len() && bytes[j + 1] == b':' => {
                            let mut k = j + 2;
                            let mut close = None;
                            while k + 1 < bytes.len() {
                                if bytes[k] == b'\\' {
                                    k = advance_escape(source, k);
                                } else if bytes[k] == b':' && bytes[k + 1] == b']' {
                                    close = Some(k);
                                    break;
                                } else {
                                    k += 1;
                                }
                            }
                            match close {
                                Some(k) => {
                                    let name = &source[j..k + 2];
                                    let inner = &source[j + 2..k];
                                    let plain = inner.strip_prefix('^').unwrap_or(inner);
                                    if !POSIX_CLASS_NAMES.contains(&plain) {
                                        return Some(GoError {
                                            pos: j,
                                            code: "invalid character class range",
                                            expr: name.to_owned(),
                                        });
                                    }
                                    j = k + 2;
                                }
                                None => j += 1,
                            }
                        }
                        _ => j = next_char_end(source, j),
                    }
                }
                i = j;
            }
            _ => i = next_char_end(source, i),
        }
    }
    None
}

/// Rewrite the source into a form `regex-syntax` accepts with Go's meaning:
/// - `{` that Go parses as a literal (not a counted repetition) is escaped,
///   so `regex-syntax` does not report `RepetitionMissing`;
/// - a `-` after a multi-character class escape (`\d`, `\p{...}`, `[:alpha:]`)
///   is escaped, since Go treats it as a literal and `regex-syntax` rejects it
///   as a class range (`[\d-z]` is valid in Go).
fn normalize_for_ast(source: &str) -> String {
    let bytes = source.as_bytes();
    let mut out = String::with_capacity(source.len());
    let mut i = 0;
    let mut in_class = false;
    let mut class_first = false;
    let mut prev_multi = false;
    while i < bytes.len() {
        match bytes[i] {
            b'\\' => {
                let end = advance_escape(source, i);
                let esc = &source[i..end];
                prev_multi = in_class
                    && esc
                        .as_bytes()
                        .get(1)
                        .is_some_and(|b| b"dDsSwWpP".contains(b));
                out.push_str(esc);
                class_first = false;
                i = end;
            }
            b'[' if !in_class => {
                in_class = true;
                class_first = true;
                prev_multi = false;
                out.push('[');
                i += 1;
            }
            b'^' if in_class && class_first => {
                out.push('^');
                class_first = false;
                i += 1;
            }
            // A `]` immediately after `[` or `[^` is a literal in Go.
            b']' if in_class && class_first => {
                out.push(']');
                class_first = false;
                prev_multi = false;
                i += 1;
            }
            b'[' if in_class && bytes.get(i + 1) == Some(&b':') => match named_class_end(source, i)
            {
                Some(k) => {
                    out.push_str(&source[i..k + 2]);
                    class_first = false;
                    prev_multi = true;
                    i = k + 2;
                }
                None => {
                    out.push('[');
                    class_first = false;
                    prev_multi = false;
                    i += 1;
                }
            },
            b'-' if in_class && prev_multi && bytes.get(i + 1).is_some_and(|b| *b != b']') => {
                out.push_str("\\-");
                class_first = false;
                prev_multi = false;
                i += 1;
            }
            b']' if in_class => {
                out.push(']');
                in_class = false;
                class_first = false;
                prev_multi = false;
                i += 1;
            }
            b'{' if !in_class => {
                if parse_counted_repeat(source, i).is_some() {
                    out.push('{');
                } else {
                    out.push_str("\\{");
                }
                class_first = false;
                prev_multi = false;
                i += 1;
            }
            _ => {
                let end = next_char_end(source, i);
                out.push_str(&source[i..end]);
                class_first = false;
                prev_multi = false;
                i = end;
            }
        }
    }
    out
}

/// Byte index of the `:` in the `:]` that closes a `[:name:]` class starting
/// at `start`.
fn named_class_end(source: &str, start: usize) -> Option<usize> {
    let bytes = source.as_bytes();
    let mut k = start + 2;
    while k + 1 < bytes.len() {
        if bytes[k] == b'\\' {
            k = advance_escape(source, k);
        } else if bytes[k] == b':' && bytes[k + 1] == b']' {
            return Some(k);
        } else {
            k += 1;
        }
    }
    None
}

/// `parsePerlFlags` (`regexp/syntax/parse.go:1144-1260`): Go accepts only
/// `i/m/s/U` (plus `-` and the `:`/`)` terminator) and tolerates repeated
/// flags. `regex-syntax` accepts more flags and rejects duplicates, so this
/// pass rejects Go-invalid syntax and rewrites each flag group with its flags
/// deduplicated per sign section (e.g. `(?ii)` → `(?i)`).
fn normalize_perl_flags(source: &str) -> (String, Option<GoError>) {
    let bytes = source.as_bytes();
    let mut out = String::with_capacity(source.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'\\' => {
                let end = advance_escape(source, i);
                out.push_str(&source[i..end]);
                i = end;
            }
            b'[' => {
                let start = i;
                i += 1;
                while i < bytes.len() {
                    if bytes[i] == b'\\' {
                        i = advance_escape(source, i);
                    } else if bytes[i] == b']' {
                        i += 1;
                        break;
                    } else {
                        i += 1;
                    }
                }
                out.push_str(&source[start..i.min(source.len())]);
            }
            b'(' if bytes.get(i + 1) == Some(&b'?') => {
                // Named captures are handled by `normalize_capture_names`.
                let starts_with_p = bytes.get(i + 2) == Some(&b'P')
                    && bytes.get(i + 3) == Some(&b'<')
                    && bytes.len() - i > 4;
                let starts_with_name = bytes.get(i + 2) == Some(&b'<') && bytes.len() - i > 3;
                if starts_with_p || starts_with_name {
                    out.push_str(&source[i..i + 2]);
                    i += 2;
                    continue;
                }
                let mut j = i + 2;
                let mut sign = 1i32;
                let mut saw_flag = false;
                let mut flags = String::new();
                let mut positive: Vec<char> = Vec::new();
                let mut negative: Vec<char> = Vec::new();
                loop {
                    if j >= bytes.len() {
                        // Like Go, `(?` at EOF is rejected.
                        return (
                            source.to_owned(),
                            Some(GoError {
                                pos: i,
                                code: "invalid or unsupported Perl syntax",
                                expr: source[i..].to_owned(),
                            }),
                        );
                    }
                    let c = bytes[j] as char;
                    match c {
                        'i' | 'm' | 's' | 'U' => {
                            saw_flag = true;
                            let seen = if sign < 0 {
                                &mut negative
                            } else {
                                &mut positive
                            };
                            if !seen.contains(&c) {
                                seen.push(c);
                                flags.push(c);
                            }
                            j += 1;
                        }
                        '-' => {
                            if sign < 0 {
                                return (
                                    source.to_owned(),
                                    Some(GoError {
                                        pos: i,
                                        code: "invalid or unsupported Perl syntax",
                                        expr: source[i..=j].to_owned(),
                                    }),
                                );
                            }
                            sign = -1;
                            saw_flag = false;
                            flags.push('-');
                            j += 1;
                        }
                        ':' | ')' => {
                            if sign < 0 && !saw_flag {
                                return (
                                    source.to_owned(),
                                    Some(GoError {
                                        pos: i,
                                        code: "invalid or unsupported Perl syntax",
                                        expr: source[i..=j].to_owned(),
                                    }),
                                );
                            }
                            break;
                        }
                        _ => {
                            // Offending character consumed, like `nextRune`.
                            return (
                                source.to_owned(),
                                Some(GoError {
                                    pos: i,
                                    code: "invalid or unsupported Perl syntax",
                                    expr: source[i..=j].to_owned(),
                                }),
                            );
                        }
                    }
                }
                out.push('(');
                out.push('?');
                out.push_str(&flags);
                out.push(bytes[j] as char);
                i = j + 1;
            }
            _ => {
                let end = next_char_end(source, i);
                out.push_str(&source[i..end]);
                i = end;
            }
        }
    }
    (out, None)
}

/// Go's `\x`/`\x{...}` error expression: `parseEscape` slices `s` up to the
/// offending character it consumed (see `regexp/syntax/parse.go:1508-1535`).
fn hex_escape_expr(source: &str, escape_start: usize) -> String {
    let bytes = source.as_bytes();
    let after = (escape_start + 2).min(source.len());
    if bytes.get(after) == Some(&b'{') {
        let mut idx = after + 1;
        let mut value: u32 = 0;
        loop {
            if idx >= source.len() {
                return source[escape_start..].to_owned();
            }
            let c = source[idx..].chars().next().unwrap();
            if c == '}' {
                return source[escape_start..idx + 1].to_owned();
            }
            match c.to_digit(16) {
                Some(v) => {
                    value = value.saturating_mul(16).saturating_add(v);
                    idx += c.len_utf8();
                    if value > 0x10FFFF {
                        return source[escape_start..idx].to_owned();
                    }
                }
                None => return source[escape_start..idx + c.len_utf8()].to_owned(),
            }
        }
    } else {
        // The non-braced form consumes up to two characters after `\x`.
        let mut idx = after;
        let mut consumed = 0;
        while idx < source.len() && consumed < 2 {
            let c = source[idx..].chars().next().unwrap();
            idx += c.len_utf8();
            consumed += 1;
        }
        source[escape_start..idx].to_owned()
    }
}

/// Map a `regex-syntax` AST error to Go's error code and offending expression.
/// Go's `expr` frequently spans more (or less) than the Rust span, so the raw
/// source is sliced to match `regexp/syntax`.
fn map_ast_error(source: &str, err: &regex_syntax::ast::Error) -> (&'static str, String) {
    let start = err.span().start.offset.min(source.len());
    let end = err.span().end.offset.min(source.len());
    let spanned = &source[start..end];

    match err.kind() {
        // Go's `wholeClass` starts at `[` and runs to the end of input.
        ErrorKind::ClassUnclosed => ("missing closing ]", source[start..].to_owned()),
        // Go errors carry the whole expression (`p.wholeRegexp`).
        ErrorKind::GroupUnclosed => ("missing closing )", source.to_owned()),
        ErrorKind::GroupUnopened => ("unexpected )", source.to_owned()),
        ErrorKind::RepetitionMissing => {
            // `regex-syntax` reports an empty span just before the operator;
            // Go's `before[:len-before-after]` is the operator itself.
            let expr = if spanned.is_empty() {
                source[start..].chars().next().map(String::from)
            } else {
                Some(spanned.to_owned())
            }
            .unwrap_or_default();
            ("missing argument to repetition operator", expr)
        }
        ErrorKind::RepetitionCountInvalid => ("invalid repeat count", spanned.to_owned()),
        ErrorKind::EscapeUnrecognized
        | ErrorKind::ClassEscapeInvalid
        | ErrorKind::UnsupportedBackreference => ("invalid escape sequence", spanned.to_owned()),
        ErrorKind::EscapeUnexpectedEof
        | ErrorKind::EscapeHexEmpty
        | ErrorKind::EscapeHexInvalid
        | ErrorKind::EscapeHexInvalidDigit => {
            // Go reports the escape starting at the preceding `\` up to the
            // offending character (not always the end of input).
            let escape_start = source[..start].rfind('\\').unwrap_or(start);
            ("invalid escape sequence", hex_escape_expr(source, escape_start))
        }
        ErrorKind::ClassRangeInvalid | ErrorKind::ClassRangeLiteral => {
            ("invalid character class range", spanned.to_owned())
        }
        ErrorKind::UnsupportedLookAround
        | ErrorKind::FlagUnrecognized
        | ErrorKind::FlagUnexpectedEof
        | ErrorKind::FlagDanglingNegation
        | ErrorKind::FlagDuplicate { .. }
        | ErrorKind::FlagRepeatedNegation { .. } => {
            ("invalid or unsupported Perl syntax", spanned.to_owned())
        }
        ErrorKind::GroupNameUnexpectedEof
        | ErrorKind::GroupNameEmpty
        | ErrorKind::GroupNameInvalid
        | ErrorKind::GroupNameDuplicate { .. } => {
            let group_start = source[..start].rfind("(?P").unwrap_or(start);
            ("invalid named capture", source[group_start..].to_owned())
        }
        _ => ("invalid or unsupported Perl syntax", spanned.to_owned()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Calibrated against Go 1.22's `regexp.Compile` (see M9.9e overview).
    #[test]
    fn matches_go_errors() {
        let cases = [
            ("(", "error parsing regexp: missing closing ): `(`"),
            (")", "error parsing regexp: unexpected ): `)`"),
            ("[a", "error parsing regexp: missing closing ]: `[a`"),
            ("a)", "error parsing regexp: unexpected ): `a)`"),
            (
                "*",
                "error parsing regexp: missing argument to repetition operator: `*`",
            ),
            (
                "a**",
                "error parsing regexp: invalid nested repetition operator: `**`",
            ),
            (
                "a*+",
                "error parsing regexp: invalid nested repetition operator: `*+`",
            ),
            (
                "a{1001}",
                "error parsing regexp: invalid repeat count: `{1001}`",
            ),
            (
                "a{2,1}",
                "error parsing regexp: invalid repeat count: `{2,1}`",
            ),
            (
                "a{1,2000}",
                "error parsing regexp: invalid repeat count: `{1,2000}`",
            ),
            (
                "\\Z",
                "error parsing regexp: invalid escape sequence: `\\Z`",
            ),
            (
                "\\q",
                "error parsing regexp: invalid escape sequence: `\\q`",
            ),
            (
                "\\x",
                "error parsing regexp: invalid escape sequence: `\\x`",
            ),
            (
                "\\xg",
                "error parsing regexp: invalid escape sequence: `\\xg`",
            ),
            (
                "\\x{}",
                "error parsing regexp: invalid escape sequence: `\\x{}`",
            ),
            (
                "\\x{110000}",
                "error parsing regexp: invalid escape sequence: `\\x{110000`",
            ),
            (
                "(?x)",
                "error parsing regexp: invalid or unsupported Perl syntax: `(?x`",
            ),
            (
                "(?u)",
                "error parsing regexp: invalid or unsupported Perl syntax: `(?u`",
            ),
            (
                "(?R)",
                "error parsing regexp: invalid or unsupported Perl syntax: `(?R`",
            ),
            (
                "(?'n'a)",
                "error parsing regexp: invalid or unsupported Perl syntax: `(?'`",
            ),
            (
                "(?P<>a)",
                "error parsing regexp: invalid named capture: `(?P<>`",
            ),
            (
                "(?P<n",
                "error parsing regexp: invalid named capture: `(?P<n`",
            ),
            (
                "(?<",
                "error parsing regexp: invalid or unsupported Perl syntax: `(?<`",
            ),
            (
                "(?P<",
                "error parsing regexp: invalid or unsupported Perl syntax: `(?P`",
            ),
            (
                "(?<=a)",
                "error parsing regexp: invalid named capture: `(?<=a)`",
            ),
            (
                "(?P<a-b>x)",
                "error parsing regexp: invalid named capture: `(?P<a-b>`",
            ),
            (
                "[z-a]",
                "error parsing regexp: invalid character class range: `z-a`",
            ),
            (
                "[[:foo:]]",
                "error parsing regexp: invalid character class range: `[:foo:]`",
            ),
            (
                "\\C",
                "error parsing regexp: invalid escape sequence: `\\C`",
            ),
            (
                "\\E",
                "error parsing regexp: invalid escape sequence: `\\E`",
            ),
            (
                "a\\",
                "error parsing regexp: trailing backslash at end of expression: ``",
            ),
        ];
        for (pattern, expected) in cases {
            assert_eq!(
                compile(pattern).err().as_deref(),
                Some(expected),
                "pattern {pattern:?}"
            );
        }
    }

    #[test]
    fn accepts_go_valid_patterns() {
        for pattern in [
            "(?U)a",
            "(?i)a",
            "(?i:abc)",
            "(?-i)a",
            "(?i-s:a)",
            "(?ii)a",
            "(?P<n>a)",
            "(?<n>a)",
            "(?P<n>a)(?P<n>b)",
            "(?P<1n>a)",
            "(?P<name_1>x)",
            "a|",
            "|a",
            "{}",
            "a{1,2",
            "a{,2}",
            "a{0,0}",
            "a{1000}",
            "a{,}",
            "\\Qa.b\\E",
            "\\Qabc",
            "\\Q",
            "\\Q\\E",
            "\\x{41}",
            "\\x{41}2",
            "\\x41",
            "[\\d-z]",
            "[[:alpha:]]",
            "[a-]",
            "[]a]",
            "\\p{Greek}",
            "\\pL",
        ] {
            assert!(
                compile(pattern).is_ok(),
                "pattern {pattern:?}: {:?}",
                compile(pattern)
            );
        }
    }
}
