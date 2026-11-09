//! Escaping used by the HTML template mode, ported from Go's `html/template`.
//!
//! * [`html`] is `html/template`'s `htmlReplacementTable` (`htmlEscaper`).
//! * [`url_attribute`] applies `urlFilter` → `urlNormalizer` → `htmlEscaper`,
//!   the chain `html/template` uses for pipelines inside quoted URL attributes
//!   such as `href="{{.URL}}"`.

/// Escapes a value inserted into HTML text or a quoted attribute value.
pub fn html(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    for character in input.chars() {
        match character {
            '\0' => out.push('\u{FFFD}'),
            '"' => out.push_str("&#34;"),
            '&' => out.push_str("&amp;"),
            '\'' => out.push_str("&#39;"),
            '+' => out.push_str("&#43;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            character => out.push(character),
        }
    }
    out
}

/// Escapes a value inserted into a quoted URL attribute.
pub fn url_attribute(input: &str) -> String {
    html(&normalize(&filter(input)))
}

/// `urlFilter`: rejects schemes other than http, https and mailto.
fn filter(input: &str) -> String {
    if !is_safe_url(input) {
        return "#ZgotmplZ".to_owned();
    }
    input.to_owned()
}

fn is_safe_url(input: &str) -> bool {
    let bytes = input.as_bytes();
    let Some(colon) = bytes.iter().position(|&byte| byte == b':') else {
        return true;
    };
    let protocol = &input[..colon];
    if protocol.contains('/') {
        return true;
    }
    protocol.eq_ignore_ascii_case("http")
        || protocol.eq_ignore_ascii_case("https")
        || protocol.eq_ignore_ascii_case("mailto")
}

/// `urlNormalizer`: percent-encodes bytes that cannot appear in a URL, leaving
/// valid escapes and RFC 3986 sub-delims (which the HTML escaper handles)
/// alone.
fn normalize(input: &str) -> String {
    let bytes = input.as_bytes();
    let mut out = Vec::with_capacity(bytes.len() + 16);
    let mut written = 0;

    for (index, &byte) in bytes.iter().enumerate() {
        let keep = match byte {
            b'!' | b'#' | b'$' | b'&' | b'*' | b'+' | b',' | b'/' | b':' | b';' | b'=' | b'?'
            | b'@' | b'[' | b']' => true,
            b'-' | b'.' | b'_' | b'~' => true,
            b'%' => index + 2 < bytes.len() && is_hex(bytes[index + 1]) && is_hex(bytes[index + 2]),
            b'a'..=b'z' | b'A'..=b'Z' | b'0'..=b'9' => true,
            _ => false,
        };
        if keep {
            continue;
        }
        out.extend_from_slice(&bytes[written..index]);
        out.push(b'%');
        out.push(lower_hex(byte >> 4));
        out.push(lower_hex(byte & 0x0f));
        written = index + 1;
    }
    out.extend_from_slice(&bytes[written..]);
    // The input is valid UTF-8 and only ASCII bytes were added.
    String::from_utf8(out).expect("valid UTF-8")
}

fn is_hex(byte: u8) -> bool {
    byte.is_ascii_hexdigit()
}

fn lower_hex(nibble: u8) -> u8 {
    match nibble {
        0..=9 => b'0' + nibble,
        _ => b'a' + (nibble - 10),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn escapes_text() {
        assert_eq!(
            html("a<b>&c+d\"e'f\0g"),
            "a&lt;b&gt;&amp;c&#43;d&#34;e&#39;f\u{FFFD}g"
        );
        assert_eq!(html("unicode 日本"), "unicode 日本");
    }

    #[test]
    fn escapes_url_attributes_like_html_template() {
        for (input, expected) in [
            ("https://x.test/a?b=1&c=2", "https://x.test/a?b=1&amp;c=2"),
            ("javascript:alert(1)", "#ZgotmplZ"),
            ("a b", "a%20b"),
            ("a+b", "a&#43;b"),
            ("a%zz", "a%25zz"),
            ("a%2Fb", "a%2Fb"),
            ("a\"b", "a%22b"),
            ("<x>", "%3cx%3e"),
            (
                "https://x.test/路径?q=あ",
                "https://x.test/%e8%b7%af%e5%be%84?q=%e3%81%82",
            ),
            ("mailto:a@b.c", "mailto:a@b.c"),
            ("//x/y?z=1&w=2", "//x/y?z=1&amp;w=2"),
            ("a'b(c)", "a%27b%28c%29"),
        ] {
            assert_eq!(url_attribute(input), expected, "input {input:?}");
        }
    }
}
