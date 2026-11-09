//! Subset of Go's `net/mail` used by `jordan-wright/email`'s `headerToBytes`.
//!
//! `headerToBytes` splits `From`/`To`/`Cc`/`Bcc` values on every comma and runs
//! `mail.ParseAddress` on each piece, skipping the pieces that fail to parse
//! (including pieces that a comma inside a quoted display name split apart).
//! Surviving participants are re-formatted with `mail.Address.String`, which is
//! already ported in [`crate::address`].
//!
//! This module ports the parser only as far as the forms the baseline can
//! produce or receive in transactional e-mail: bare `addr-spec`, `phrase
//! <addr-spec>` with atoms/quoted-strings/RFC 2047 encoded-words, and comments.
//! Groups and RFC 2047 charsets other than utf-8 / us-ascii / iso-8859-1 are not
//! supported and are treated as unparsable (the participant is skipped, like an
//! invalid address in the baseline).

use base64::engine::general_purpose::STANDARD;
use base64::Engine;

use crate::address::{mail_address, q_encoding_encode};

/// `headerToBytes`'s handling of an address header value: every comma-split
/// participant that parses is re-formatted, and participants that do not parse
/// are kept verbatim (the baseline's `continue` leaves the slice entry
/// untouched).
pub(crate) fn format_address_list(value: &str) -> String {
    let mut formatted = Vec::new();
    for participant in value.split(',') {
        match parse_single_address(participant) {
            Some((name, address)) => formatted.push(mail_address(&name, &address)),
            None => formatted.push(participant.to_owned()),
        }
    }
    formatted.join(", ")
}

/// `mail.ParseAddress` followed by `Address.String()`'s `Name`/`Address`
/// fields. Returns `None` where the baseline returns an error (the caller then
/// skips the participant).
fn parse_single_address(input: &str) -> Option<(String, String)> {
    let mut parser = Parser::new(input);
    parser.skip_space();
    if parser.empty() {
        return None;
    }

    // addr-spec has a more restricted grammar than name-addr, so Go tries it
    // first and falls back when it fails.
    if let Some(spec) = parser.consume_addr_spec() {
        let mut name = String::new();
        parser.skip_space();
        if parser.peek() == Some('(') {
            name = parser.consume_display_name_comment()?;
        }
        return parser.finish().then_some((name, spec));
    }

    // name-addr = [display-name] angle-addr
    let mut display_name = String::new();
    if parser.peek() != Some('<') {
        display_name = parser.consume_phrase()?;
    }
    parser.skip_space();
    if parser.peek() == Some(':') {
        // RFC 5322 group syntax is outside the ported subset.
        return None;
    }
    if !parser.consume('<') {
        return None;
    }
    let spec = parser.consume_addr_spec()?;
    if !parser.consume('>') {
        return None;
    }
    parser.finish().then_some((display_name, spec))
}

struct Parser<'a> {
    input: &'a str,
    pos: usize,
}

impl<'a> Parser<'a> {
    fn new(input: &'a str) -> Self {
        Self { input, pos: 0 }
    }

    fn rest(&self) -> &'a str {
        &self.input[self.pos..]
    }

    fn empty(&self) -> bool {
        self.pos >= self.input.len()
    }

    fn peek(&self) -> Option<char> {
        self.rest().chars().next()
    }

    fn consume(&mut self, expected: char) -> bool {
        if self.peek() == Some(expected) {
            self.pos += expected.len_utf8();
            true
        } else {
            false
        }
    }

    fn skip_space(&mut self) {
        while matches!(self.peek(), Some(' ') | Some('\t')) {
            self.pos += 1;
        }
    }

    /// `skipCFWS`: spaces plus parenthetical comments.
    fn skip_cfws(&mut self) -> bool {
        self.skip_space();
        while self.consume('(') {
            if self.consume_comment().is_none() {
                return false;
            }
            self.skip_space();
        }
        true
    }

    /// `parseSingleAddress`'s tail: only CFWS may remain.
    fn finish(&mut self) -> bool {
        self.skip_cfws() && self.empty()
    }

    fn consume_addr_spec(&mut self) -> Option<String> {
        let saved = self.pos;
        let result = self.consume_addr_spec_inner();
        if result.is_none() {
            self.pos = saved;
        }
        result
    }

    fn consume_addr_spec_inner(&mut self) -> Option<String> {
        self.skip_space();
        if self.empty() {
            return None;
        }
        let local = if self.peek() == Some('"') {
            let local = self.consume_quoted_string()?;
            if local.is_empty() {
                return None;
            }
            local
        } else {
            self.consume_atom(true, false)?
        };
        if !self.consume('@') {
            return None;
        }
        self.skip_space();
        if self.empty() {
            return None;
        }
        let domain = self.consume_atom(true, false)?;
        Some(format!("{local}@{domain}"))
    }

    /// `consumePhrase`, including RFC 2047 word decoding and the
    /// "adjacent encoded words concatenate" rule.
    fn consume_phrase(&mut self) -> Option<String> {
        let mut words: Vec<String> = Vec::new();
        let mut previous_encoded = false;
        let mut failed = false;

        loop {
            if !words.is_empty() && !self.skip_cfws() {
                // Go returns immediately for a malformed comment, even after
                // successfully reading words.
                return None;
            }
            self.skip_space();
            if self.empty() {
                break;
            }
            let (word, encoded) = if self.peek() == Some('"') {
                match self.consume_quoted_string() {
                    Some(word) => (word, false),
                    None => {
                        failed = true;
                        break;
                    }
                }
            } else {
                match self.consume_atom(true, true) {
                    Some(atom) => match decode_encoded_word(&atom) {
                        EncodedWord::Decoded(word) => (word, true),
                        EncodedWord::Plain(atom) => (atom, false),
                        EncodedWord::Error => {
                            failed = true;
                            break;
                        }
                    },
                    None => {
                        failed = true;
                        break;
                    }
                }
            };
            if previous_encoded && encoded {
                words.last_mut().expect("non-empty").push_str(&word);
            } else {
                words.push(word);
            }
            previous_encoded = encoded;
        }

        if failed && words.is_empty() {
            return None;
        }
        Some(words.join(" "))
    }

    fn consume_quoted_string(&mut self) -> Option<String> {
        debug_assert_eq!(self.peek(), Some('"'));
        self.pos += 1;
        let mut out = String::new();
        let mut escaped = false;
        loop {
            let character = self.peek()?;
            if escaped {
                if !is_vchar(character) && !is_wsp(character) {
                    return None;
                }
                out.push(character);
                escaped = false;
            } else if is_qtext(character) || is_wsp(character) {
                out.push(character);
            } else if character == '"' {
                self.pos += 1;
                return Some(out);
            } else if character == '\\' {
                escaped = true;
            } else {
                return None;
            }
            self.pos += character.len_utf8();
        }
    }

    /// `consumeAtom(dot, permissive)`.
    fn consume_atom(&mut self, dot: bool, permissive: bool) -> Option<String> {
        let mut end = self.pos;
        for character in self.rest().chars() {
            if !is_atext(character, dot) {
                break;
            }
            end += character.len_utf8();
        }
        if end == self.pos {
            return None;
        }
        let atom = &self.input[self.pos..end];
        if !permissive && (atom.starts_with('.') || atom.contains("..") || atom.ends_with('.')) {
            return None;
        }
        self.pos = end;
        Some(atom.to_owned())
    }

    fn consume_display_name_comment(&mut self) -> Option<String> {
        if !self.consume('(') {
            return None;
        }
        let comment = self.consume_comment()?;
        let words = comment
            .split([' ', '\t'])
            .filter(|word| !word.is_empty())
            .map(|word| match decode_encoded_word(word) {
                EncodedWord::Decoded(decoded) => Some(decoded),
                EncodedWord::Plain(word) => Some(word),
                EncodedWord::Error => None,
            })
            .collect::<Option<Vec<_>>>()?;
        Some(words.join(" "))
    }

    /// `consumeComment`; `(` has already been consumed. Returns the comment
    /// text without its outer parentheses.
    fn consume_comment(&mut self) -> Option<String> {
        let mut depth = 1usize;
        let mut out = String::new();
        while !self.empty() && depth > 0 {
            let character = self.peek()?;
            if character == '\\' && self.rest().len() > 1 {
                self.pos += 1;
            } else if character == '(' {
                depth += 1;
            } else if character == ')' {
                depth -= 1;
            }
            if depth > 0 {
                let character = self.peek()?;
                out.push(character);
            }
            self.pos += character.len_utf8();
        }
        (depth == 0).then_some(out)
    }
}

enum EncodedWord {
    Decoded(String),
    Plain(String),
    Error,
}

/// `decodeRFC2047Word`: invalid encoded-word syntax is kept as plain text;
/// a valid word whose charset cannot be decoded is an error.
fn decode_encoded_word(word: &str) -> EncodedWord {
    if word.len() < 8
        || !word.starts_with("=?")
        || !word.ends_with("?=")
        || word.matches('?').count() != 4
    {
        return EncodedWord::Plain(word.to_owned());
    }
    let inner = &word[2..word.len() - 2];
    let Some((charset, rest)) = inner.split_once('?') else {
        return EncodedWord::Plain(word.to_owned());
    };
    let Some((encoding, text)) = rest.split_once('?') else {
        return EncodedWord::Plain(word.to_owned());
    };
    if charset.is_empty() || encoding.len() != 1 {
        return EncodedWord::Plain(word.to_owned());
    }
    let content = match encoding.as_bytes()[0] {
        b'B' | b'b' => match STANDARD.decode(text) {
            Ok(bytes) => bytes,
            Err(_) => return EncodedWord::Plain(word.to_owned()),
        },
        b'Q' | b'q' => match q_decode(text) {
            Some(bytes) => bytes,
            None => return EncodedWord::Plain(word.to_owned()),
        },
        _ => return EncodedWord::Plain(word.to_owned()),
    };
    match decode_charset(charset, &content) {
        Some(decoded) => EncodedWord::Decoded(decoded),
        None => EncodedWord::Error,
    }
}

fn decode_charset(charset: &str, content: &[u8]) -> Option<String> {
    if charset.eq_ignore_ascii_case("utf-8") {
        String::from_utf8(content.to_vec()).ok()
    } else if charset.eq_ignore_ascii_case("iso-8859-1") {
        Some(content.iter().map(|&byte| byte as char).collect())
    } else if charset.eq_ignore_ascii_case("us-ascii") {
        Some(
            content
                .iter()
                .map(|&byte| {
                    if byte < 0x80 {
                        byte as char
                    } else {
                        '\u{FFFD}'
                    }
                })
                .collect(),
        )
    } else {
        None
    }
}

fn q_decode(input: &str) -> Option<Vec<u8>> {
    let bytes = input.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        match bytes[index] {
            b'_' => {
                out.push(b' ');
                index += 1;
            }
            b'=' => {
                if index + 2 >= bytes.len() {
                    return None;
                }
                let high = hex_value(bytes[index + 1])?;
                let low = hex_value(bytes[index + 2])?;
                out.push(high << 4 | low);
                index += 3;
            }
            byte => {
                out.push(byte);
                index += 1;
            }
        }
    }
    Some(out)
}

fn hex_value(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

/// `mail.isAtext(r, dot)`.
fn is_atext(character: char, dot: bool) -> bool {
    if character == '.' {
        return dot;
    }
    if matches!(
        character,
        '(' | ')' | '<' | '>' | '[' | ']' | ':' | ';' | '@' | '\\' | ',' | '"'
    ) {
        return false;
    }
    is_vchar(character)
}

fn is_qtext(character: char) -> bool {
    character != '\\' && character != '"' && is_vchar(character)
}

fn is_vchar(character: char) -> bool {
    ('!'..='~').contains(&character) || is_multibyte(character)
}

fn is_wsp(character: char) -> bool {
    character == ' ' || character == '\t'
}

fn is_multibyte(character: char) -> bool {
    character as u32 >= 0x80
}

/// `mime.QEncoding.Encode` as called by `headerToBytes` (charset `UTF-8`).
pub(crate) fn encode_header_value(value: &str) -> String {
    q_encoding_encode("UTF-8", value)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reformats_plain_and_named_addresses() {
        assert_eq!(format_address_list("a@b.c"), "<a@b.c>");
        assert_eq!(format_address_list("a@b.c, d@e.f"), "<a@b.c>, <d@e.f>");
        assert_eq!(
            format_address_list("Sourcegraph <a@b.c>"),
            "\"Sourcegraph\" <a@b.c>"
        );
        // A comma inside a quoted display name splits the participant in two;
        // neither half parses, so both are kept verbatim and re-joined.
        assert_eq!(
            format_address_list("\"Doe, John\" <j@x.y>"),
            "\"Doe,  John\" <j@x.y>"
        );
        assert_eq!(
            format_address_list("\"a.b\" <first.last@b.c>"),
            "\"a.b\" <first.last@b.c>"
        );
    }

    #[test]
    fn decodes_and_reencodes_encoded_words() {
        assert_eq!(
            format_address_list("=?utf-8?q?Jos=C3=A9?= <a@b.c>"),
            "=?utf-8?q?Jos=C3=A9?= <a@b.c>"
        );
        // Adjacent encoded words concatenate before re-encoding; the '.' in the
        // display name switches the re-encoding to base64.
        assert_eq!(
            format_address_list("=?utf-8?q?Jos=C3=A9?= =?utf-8?q?_Jr.?= <a@b.c>"),
            "=?utf-8?b?Sm9zw6kgSnIu?= <a@b.c>"
        );
        assert_eq!(
            format_address_list("=?UTF-8?q?h=C3=A9llo?= <a@b.c>"),
            "=?utf-8?q?h=C3=A9llo?= <a@b.c>"
        );
    }

    #[test]
    fn keeps_unparsable_participants_verbatim() {
        assert_eq!(format_address_list("not-an-address"), "not-an-address");
        assert_eq!(format_address_list("<no-at@>"), "<no-at@>");
        assert_eq!(format_address_list("a@b.c,"), "<a@b.c>, ");
    }

    #[test]
    fn encodes_header_values_with_upper_case_charset() {
        assert_eq!(encode_header_value("plain"), "plain");
        assert_eq!(encode_header_value("héllo"), "=?UTF-8?q?h=C3=A9llo?=");
    }
}
