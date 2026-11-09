//! Port of `github.com/k3a/html2text` v1.1.0, which `txemail.ParseTemplate`
//! uses to derive the plain-text body when `Templates.Text` is empty.
//!
//! The converter is a character scanner, not an HTML parser: it walks the
//! input rune by rune and reproduces the library's state exactly (tag
//! suppression, the `<head>/<script>/<style>/<a>` depth counter, whitespace
//! collapsing, named/numeric entities, `<p>`/`<br>`/`<h1>`-`<h6>`/`<ul>`/`<li>`
//! line breaks, and the legacy Windows line break default).
//!
//! [`html_entities_to_text`] is the library's exported helper; `html2text`
//! uses it for link `href` values. The HTML5 named-character table is
//! generated verbatim in [`crate::html2text_entities`].
//!
//! Only the default option set is ported: `linksInnerText` is never enabled,
//! and the Unix line-break option is exposed as
//! [`html2text_with_line_break`] for tests but never selected by `txemail`.

use std::sync::OnceLock;

use regex::Regex;

use crate::html2text_entities::entity;

/// The library's default line break (`WIN_LBR`).
pub const WINDOWS_LINE_BREAK: &str = "\r\n";
/// `UNIX_LBR`.
pub const UNIX_LINE_BREAK: &str = "\n";

fn bad_tagnames_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        // Go: `^(head|script|style|a)($|\s+)`; Go's `\s` is ASCII-only.
        Regex::new(r"^(head|script|style|a)(?:$|[ \t\n\f\r]+)").expect("bad tag names regex")
    })
}

fn link_tag_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r#"a.*href=('([^']*?)'|"([^"]*?)")"#).expect("link tag regex"))
}

fn headers_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r"^/?h[1-6]").expect("headers regex"))
}

fn numeric_entity_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r"(?i)^#(x?[a-f0-9]+)$").expect("numeric entity regex"))
}

/// Go's `badLinkHrefRE.MatchString` (a literal substring search).
fn is_bad_link(href: &str) -> bool {
    href.contains("javascript:")
}

/// `HTML2Text` with the library's default (`\r\n`) line break.
pub fn html2text(input: &str) -> String {
    html2text_with_line_break(input, WINDOWS_LINE_BREAK)
}

/// `HTML2TextWithOptions` for the link-less default option set, with an
/// explicit line break (`WithUnixLineBreaks` otherwise).
pub fn html2text_with_line_break(input: &str, line_break: &str) -> String {
    let in_len = input.len();
    let mut tag_start = 0usize;
    let mut in_ent = false;
    let mut bad_tag_stack_depth: i32 = 0;
    let mut should_output = true;
    let mut can_print_newline = false;
    let mut out = String::new();

    for (index, rune) in input.char_indices() {
        if in_len > 0 && index == in_len - 1 {
            // Prevent a new line at the end of the document.
            can_print_newline = false;
        }

        // New lines and spaces: collapse to a single space.
        // Go: `r <= 0xD, r == 0x85, r == 0x2028, r == 0x2029,
        //      r == ' ', r >= 0x2008 && r <= 0x200B`.
        if rune <= '\u{d}'
            || rune == '\u{85}'
            || rune == '\u{2028}'
            || rune == '\u{2029}'
            || rune == ' '
            || ('\u{2008}'..='\u{200b}').contains(&rune)
        {
            if should_output && bad_tag_stack_depth == 0 && !in_ent {
                write_space(&mut out);
            }
            continue;
        }

        if rune == ';' && in_ent {
            in_ent = false;
            continue;
        }

        if rune == '&' && should_output {
            if let Some(ent) = parse_entity_at(input, index) {
                out.push(ent);
                in_ent = true;
                continue;
            }
        } else if rune == '<' {
            tag_start = index + 1;
            should_output = false;
            continue;
        } else if rune == '>' {
            should_output = true;
            let tag = &input[tag_start..index];
            let tag_name_lowercase = tag.to_lowercase();
            handle_tag(
                &mut out,
                &mut bad_tag_stack_depth,
                &mut can_print_newline,
                line_break,
                tag,
                &tag_name_lowercase,
            );
            continue;
        }

        if should_output && bad_tag_stack_depth == 0 && !in_ent {
            can_print_newline = true;
            out.push(rune);
        }
    }

    out
}

fn handle_tag(
    out: &mut String,
    bad_tag_stack_depth: &mut i32,
    can_print_newline: &mut bool,
    line_break: &str,
    tag: &str,
    tag_name_lowercase: &str,
) {
    if matches!(tag_name_lowercase, "/ul" | "li" | "li/") {
        out.push_str(line_break);
    } else if headers_re().is_match(tag_name_lowercase) {
        if *can_print_newline {
            out.push_str(line_break);
            out.push_str(line_break);
        }
        *can_print_newline = false;
    } else if tag_name_lowercase == "br" || tag_name_lowercase == "br/" {
        out.push_str(line_break);
    } else if tag_name_lowercase == "p" || tag_name_lowercase == "/p" {
        if *can_print_newline {
            out.push_str(line_break);
            out.push_str(line_break);
        }
        *can_print_newline = false;
    } else if bad_tagnames_re().is_match(tag_name_lowercase) {
        // Unwanted block (`<head>`, `<script>`, `<style>`, `<a>`).
        *bad_tag_stack_depth += 1;

        // The baseline parses the href from the original-cased tag here, so a
        // lowercase "href=" with an upper-case tag name is not recognized.
        if let Some(href) = link_href(tag) {
            if !is_bad_link(href) {
                out.push_str(&html_entities_to_text(href));
            }
        }
    } else if !tag_name_lowercase.is_empty()
        && tag_name_lowercase.starts_with('/')
        && bad_tagnames_re().is_match(&tag_name_lowercase[1..])
    {
        *bad_tag_stack_depth -= 1;
    }
}

/// Extracts the href value with the library's regex, returning `None` when the
/// pattern does not match or the value is empty after both alternatives.
fn link_href(tag: &str) -> Option<&str> {
    let captures = link_tag_re().captures(tag)?;
    let single = captures.get(2).map(|value| value.as_str()).unwrap_or("");
    if !single.is_empty() {
        return Some(single);
    }
    let double = captures.get(3).map(|value| value.as_str()).unwrap_or("");
    Some(double)
}

fn write_space(out: &mut String) {
    if !out.is_empty() && !out.ends_with(' ') {
        out.push(' ');
    }
}

/// `HTMLEntitiesToText`: decodes entities in a standalone string (used for
/// link targets). Its state machine mirrors the exported Go helper exactly.
pub fn html_entities_to_text(input: &str) -> String {
    let mut out = String::new();
    let mut in_ent = false;

    for (index, rune) in input.char_indices() {
        if rune == ';' && in_ent {
            in_ent = false;
            continue;
        }

        if rune == '&' {
            if let Some(ent) = parse_entity_at(input, index) {
                out.push(ent);
                in_ent = true;
                continue;
            }
        }

        if !in_ent {
            out.push(rune);
        }
    }

    out
}

/// Parses the entity starting at the `&` at `index` (if any), following the
/// library's rules: at most 10 name characters, and a terminating semicolon.
fn parse_entity_at(input: &str, index: usize) -> Option<char> {
    let mut ent_name = String::new();
    let mut is_ent = false;
    let mut chars = 0;
    for rune in input[index + 1..].chars() {
        if rune == ';' {
            is_ent = true;
            break;
        }
        ent_name.push(rune);
        chars += 1;
        if chars == 10 {
            break;
        }
    }
    if !is_ent {
        return None;
    }
    parse_html_entity(&ent_name)
}

/// `parseHTMLEntity`: named lookup first, then `#` numeric (decimal or `x` hex).
fn parse_html_entity(name: &str) -> Option<char> {
    if let Some(value) = entity(name) {
        return Some(value);
    }

    let captures = numeric_entity_re().captures(name)?;
    let digits = captures.get(1)?.as_str();
    let number: i64 = if digits.starts_with('x') || digits.starts_with('X') {
        i64::from_str_radix(&digits[1..], 16).ok()?
    } else {
        digits.parse::<i64>().ok()?
    };

    if number == 9 || number == 10 || number == 13 || number > 31 {
        // Go converts the int64 to a rune (int32, truncating) and then to a
        // string, which maps invalid code points to U+FFFD.
        let rune = number as i32;
        Some(char::from_u32(rune as u32).unwrap_or('\u{fffd}'))
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn matches_the_upstream_examples() {
        assert_eq!(html2text("<div></div>"), "");
        assert_eq!(html2text("<div>simple text</div>"), "simple text");
        assert_eq!(html2text(r#"click <a href="test">here</a>"#), "click test");
        assert_eq!(
            html2text(r#"click <a class="x" href="test">here</a>"#),
            "click test"
        );
        assert_eq!(
            html2text(r#"click <a href="test"><span>here</span> or here</a>"#),
            "click test"
        );
        assert_eq!(
            html2text(r#"click <a href="javascript:void(0)">here</a>"#),
            "click "
        );
        assert_eq!(
            html2text("should    ignore more spaces"),
            "should ignore more spaces"
        );
        assert_eq!(html2text("two<br>line<br/>breaks"), "two\r\nline\r\nbreaks");
        assert_eq!(
            html2text("<p>two</p><p>paragraphs</p>"),
            "two\r\n\r\nparagraphs"
        );
        assert_eq!(
            html2text("<h1>First</h1>main text"),
            "First\r\n\r\nmain text"
        );
        assert_eq!(
            html2text("<h7>Not Header</h7>next section"),
            "Not Headernext section"
        );
        assert_eq!(html2text("two&nbsp;&nbsp;spaces"), "two\u{a0}\u{a0}spaces");
        assert_eq!(html2text("&copy; 2017 K3A"), "© 2017 K3A");
        assert_eq!(html2text("&lt;printtag&gt;"), "<printtag>");
        assert_eq!(
            html2text("Tom & Jerry is not an entity"),
            "Tom & Jerry is not an entity"
        );
        assert_eq!(
            html2text("this &neither; as you see"),
            "this &neither; as you see"
        );
        assert_eq!(
            html2text("list of items<ul><li>One</li><li>Two</li><li>Three</li></ul>"),
            "list of items\r\nOne\r\nTwo\r\nThree\r\n"
        );
        assert_eq!(
            html2text("&#8268; decimal and hex entities supported &#x204D;"),
            "⁌ decimal and hex entities supported ⁍"
        );
        assert_eq!(html2text(""), "");
        assert_eq!(
            html2text("<html><head><title>Good</title></head><body>x</body>"),
            "x"
        );
        assert_eq!(
            html2text(r#"we are not <script type="javascript"></script>interested in scripts"#),
            "we are not interested in scripts"
        );
        assert_eq!(html2text("<aa x=\"1\">hello</aa>"), "hello");
    }

    #[test]
    fn decodes_standalone_entities() {
        assert_eq!(html_entities_to_text("&abcdefghij;"), "&abcdefghij;");
        assert_eq!(
            html_entities_to_text("&#39;single quotes&#39; and &#52765;"),
            "'single quotes' and 츝"
        );
    }

    #[test]
    fn supports_unix_line_breaks() {
        assert_eq!(
            html2text_with_line_break("two<br>line<br/>breaks", UNIX_LINE_BREAK),
            "two\nline\nbreaks"
        );
        assert_eq!(
            html2text_with_line_break("<p>two</p><p>paragraphs</p>", UNIX_LINE_BREAK),
            "two\n\nparagraphs"
        );
    }

    #[test]
    fn maps_invalid_numeric_entities_to_replacement_character() {
        // Surrogate code point.
        assert_eq!(html2text("&#xD800;"), "\u{fffd}");
        // Beyond the Unicode range.
        assert_eq!(html2text("&#x110000;"), "\u{fffd}");
        // Control characters other than tab/LF/CR are dropped.
        assert_eq!(html2text("&#0;x"), "&#0;x");
        assert_eq!(html2text("&#31;x"), "&#31;x");
    }
}
