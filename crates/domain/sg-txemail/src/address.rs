//! Port of Go's `net/mail.Address.String` (Go 1.22.4) and the RFC 2047
//! `QEncoding`/`BEncoding` encoders it invokes for non-ASCII display names.
//!
//! `txemail.render` builds the `From` header as
//! `(&mail.Address{Name: senderName, Address: fromAddress}).String()`.

use base64::engine::general_purpose::STANDARD;
use base64::Engine;

/// Formats `Name <address>` exactly like Go's `net/mail`.
pub fn mail_address(name: &str, address: &str) -> String {
    let (local, domain) = match address.rfind('@') {
        Some(index) => (&address[..index], &address[index + 1..]),
        None => (address, ""),
    };
    let local = if needs_quote_local(local) {
        quote_string(local)
    } else {
        local.to_owned()
    };
    let formatted = format!("<{local}@{domain}>");
    if name.is_empty() {
        return formatted;
    }

    let all_printable = name
        .chars()
        .all(|character| (is_vchar(character) || is_wsp(character)) && !is_multibyte(character));
    if all_printable {
        return format!("{} {}", quote_string(name), formatted);
    }

    // Text in an encoded-word display name must not contain these characters,
    // so Go falls back to base64 encoding when they appear.
    if name
        .chars()
        .any(|character| "\"#$%&'(),.:;<>@[]^`{|}~".contains(character))
    {
        return format!("{} {}", b_encode("utf-8", name), formatted);
    }
    format!("{} {}", q_encode("utf-8", name), formatted)
}

fn needs_quote_local(local: &str) -> bool {
    for (index, character) in local.char_indices() {
        if is_atext(character) {
            continue;
        }
        if character == '.'
            && index > 0
            && local.as_bytes()[index - 1] != b'.'
            && index < local.len() - 1
        {
            // Dots are okay when surrounded by atext.
            continue;
        }
        return true;
    }
    false
}

fn quote_string(input: &str) -> String {
    let mut out = String::with_capacity(input.len() + 2);
    out.push('"');
    for character in input.chars() {
        if is_qtext(character) || is_wsp(character) {
            out.push(character);
        } else if is_vchar(character) {
            out.push('\\');
            out.push(character);
        }
        // Other characters (controls) are dropped, like Go's quoteString.
    }
    out.push('"');
    out
}

/// Go's `mail.isAtext(r, false)`.
fn is_atext(character: char) -> bool {
    if matches!(
        character,
        '(' | ')' | '<' | '>' | '[' | ']' | ':' | ';' | '@' | '\\' | ',' | '"' | '.'
    ) {
        return false;
    }
    is_vchar(character)
}

/// Go's `mail.isQtext`.
fn is_qtext(character: char) -> bool {
    character != '\\' && character != '"' && is_vchar(character)
}

/// Go's `mail.isVchar`; multi-byte characters count as VCHAR.
fn is_vchar(character: char) -> bool {
    ('!'..='~').contains(&character) || is_multibyte(character)
}

fn is_wsp(character: char) -> bool {
    character == ' ' || character == '\t'
}

fn is_multibyte(character: char) -> bool {
    character as u32 >= 0x80
}

// The longest encoded-word is 75 characters (RFC 2047 §2); the content limit
// subtracts the wrapping used by Go's mime encoder.
const MAX_ENCODED_WORD_LEN: usize = 75;
const MAX_CONTENT_LEN: usize = MAX_ENCODED_WORD_LEN - "=?UTF-8?q?".len() - "?=".len();

fn needs_encoding(input: &str) -> bool {
    input
        .chars()
        .any(|character| !(' '..='~').contains(&character) && character != '\t')
}

fn open_word(out: &mut String, charset: &str, encoding: u8) {
    out.push_str("=?");
    out.push_str(charset);
    out.push('?');
    out.push(encoding as char);
    out.push('?');
}

fn close_word(out: &mut String) {
    out.push_str("?=");
}

fn split_word(out: &mut String, charset: &str, encoding: u8) {
    close_word(out);
    out.push(' ');
    open_word(out, charset, encoding);
}

fn write_q_string(out: &mut String, input: &str) {
    for byte in input.bytes() {
        match byte {
            b' ' => out.push('_'),
            b'!'..=b'~' if byte != b'=' && byte != b'?' && byte != b'_' => out.push(byte as char),
            _ => {
                out.push('=');
                out.push(upper_hex(byte >> 4));
                out.push(upper_hex(byte & 0x0f));
            }
        }
    }
}

fn q_encode(charset: &str, input: &str) -> String {
    if !needs_encoding(input) {
        return input.to_owned();
    }
    let mut out = String::new();
    open_word(&mut out, charset, b'q');
    if !charset.eq_ignore_ascii_case("utf-8") {
        write_q_string(&mut out, input);
    } else {
        let bytes = input.as_bytes();
        let mut index = 0;
        let mut current_len = 0;
        while index < bytes.len() {
            let byte = bytes[index];
            let (rune_len, encoded_len) =
                if (b' '..=b'~').contains(&byte) && byte != b'=' && byte != b'?' && byte != b'_' {
                    (1, 1)
                } else {
                    let rune_len = utf8_len(byte);
                    (rune_len, 3 * rune_len)
                };
            if current_len + encoded_len > MAX_CONTENT_LEN {
                split_word(&mut out, charset, b'q');
                current_len = 0;
            }
            write_q_string(&mut out, &input[index..index + rune_len]);
            current_len += encoded_len;
            index += rune_len;
        }
    }
    close_word(&mut out);
    out
}

fn b_encode(charset: &str, input: &str) -> String {
    if !needs_encoding(input) {
        return input.to_owned();
    }
    let mut out = String::new();
    open_word(&mut out, charset, b'b');
    let encoded_len = input.len().div_ceil(3) * 4;
    if !charset.eq_ignore_ascii_case("utf-8") || encoded_len <= MAX_CONTENT_LEN {
        out.push_str(&STANDARD.encode(input.as_bytes()));
    } else {
        // Go: base64.StdEncoding.DecodedLen(maxContentLen).
        let max_raw_len = MAX_CONTENT_LEN / 4 * 3;
        let bytes = input.as_bytes();
        let mut index = 0;
        let mut last = 0;
        let mut current_len = 0;
        while index < bytes.len() {
            let rune_len = utf8_len(bytes[index]);
            if current_len + rune_len <= max_raw_len {
                current_len += rune_len;
            } else {
                out.push_str(&STANDARD.encode(&bytes[last..index]));
                split_word(&mut out, charset, b'b');
                last = index;
                current_len = rune_len;
            }
            index += rune_len;
        }
        out.push_str(&STANDARD.encode(&bytes[last..]));
    }
    close_word(&mut out);
    out
}

fn utf8_len(first_byte: u8) -> usize {
    match first_byte {
        0x00..=0x7f => 1,
        0xc0..=0xdf => 2,
        0xe0..=0xef => 3,
        _ => 4,
    }
}

fn upper_hex(nibble: u8) -> char {
    match nibble {
        0..=9 => (b'0' + nibble) as char,
        _ => (b'A' + (nibble - 10)) as char,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn formats_like_go_net_mail() {
        assert_eq!(mail_address("", "a@b.c"), "<a@b.c>");
        assert_eq!(
            mail_address("Sourcegraph", "a@b.c"),
            "\"Sourcegraph\" <a@b.c>"
        );
        assert_eq!(mail_address("John Doe", "a@b.c"), "\"John Doe\" <a@b.c>");
        assert_eq!(
            mail_address("José", "a@b.c"),
            "=?utf-8?q?Jos=C3=A9?= <a@b.c>"
        );
        assert_eq!(mail_address("", "weird local@b.c"), "<\"weird local\"@b.c>");
        assert_eq!(
            mail_address("a.b", "first.last@b.c"),
            "\"a.b\" <first.last@b.c>"
        );
    }

    #[test]
    fn uses_base64_when_specials_and_non_ascii_mix() {
        let encoded = mail_address("José, Jr.", "a@b.c");
        assert!(encoded.starts_with("=?utf-8?b?"), "{encoded}");
        assert!(encoded.ends_with("?= <a@b.c>"), "{encoded}");
    }
}
