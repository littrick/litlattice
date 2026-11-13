#![forbid(unsafe_code)]

//! `Query.renderMarkdown` / `Query.highlightCode`
//! (`cmd/frontend/graphqlbackend/file.go:48-80`).
//!
//! `renderMarkdown` is a byte-compatible port of the goldmark pipeline used by
//! `internal/markdown.Render`: CommonMark + GFM parsing, the `mdLinkHeaders`
//! anchor injection, goldmark's HTML rendering rules, followed by the
//! bluemonday UGC policy from `internal/htmlutil/policy.go`.
//!
//! `highlightCode` is the syntect-server HTTP path of
//! `cmd/frontend/internal/highlight.Code`: the resolver maps the fuzzy language
//! through `SyntectLanguageMap`, calls `highlight.Code`, and returns the
//! response HTML (or a plain-text table fallback). The Go tree-sitter/SCIP
//! engine path is out of scope; see the slice overview for the enumerated
//! coverage boundary.
//!
//! ## Uncovered items
//!
//! - Fenced code blocks whose info string is a language chroma knows produce
//!   chroma token markup in the Go baseline. This module always emits the plain
//!   goldmark `<pre><code class="language-X">` form.
//! - goldmark's GFM `linkify` extension (bare URL auto-linking) has no
//!   pulldown-cmark equivalent here.
//! - enry's content/modeline/classifier strategies are not ported; language
//!   detection uses the extension table below (which mirrors enry's extension
//!   strategy for the extensions reachable through `SyntectLanguageMap`).
//! - HTML5 tree construction for malformed raw HTML is not reproduced; the
//!   sanitizer is a tokenizer with bluemonday's policy semantics.

use std::collections::{HashMap, HashSet};
use std::sync::OnceLock;
use std::time::Duration;

use pulldown_cmark::{CodeBlockKind, Event, HeadingLevel, LinkType, Options, Parser, Tag, TagEnd};
use regex::Regex;

// ---------------------------------------------------------------------------
// `Query.renderMarkdown` (`internal/markdown.Render`)
// ---------------------------------------------------------------------------

/// `markdown.Render`: goldmark-equivalent HTML, then the bluemonday policy.
pub fn render_markdown(markdown: &str) -> Result<String, String> {
    Ok(sanitize_html(&render_goldmark(markdown)))
}

/// `internal/markdown.Renderer` implementation backed by [`render_markdown`].
#[derive(Debug, Default, Clone, Copy)]
pub struct GoldmarkRenderer;

impl crate::graphql_inventory_c::MarkdownRenderer for GoldmarkRenderer {
    fn render(&self, markdown: &str) -> Result<String, String> {
        render_markdown(markdown)
    }
}

fn render_goldmark(source: &str) -> String {
    let mut opts = Options::empty();
    opts.insert(Options::ENABLE_TABLES);
    opts.insert(Options::ENABLE_STRIKETHROUGH);
    opts.insert(Options::ENABLE_TASKLISTS);
    opts.insert(Options::ENABLE_GFM);
    let events: Vec<(Event<'_>, std::ops::Range<usize>)> =
        Parser::new_ext(source, opts).into_offset_iter().collect();
    let mut renderer = Renderer {
        source,
        out: String::new(),
        ids: HashSet::new(),
        list_stack: Vec::new(),
        item_depth: 0,
        in_table_head: false,
        tbody_open: false,
        image_stack: Vec::new(),
    };
    let mut idx = 0;
    while idx < events.len() {
        match &events[idx].0 {
            Event::Start(tag) => renderer.start(tag, &events[idx].1, &events, idx),
            Event::End(tag) => renderer.end(tag),
            Event::Text(text) => renderer.text(text),
            Event::Code(text) => renderer.code(text),
            Event::Html(html) | Event::InlineHtml(html) => renderer.html(html),
            Event::SoftBreak => renderer.soft_break(),
            Event::HardBreak => renderer.hard_break(),
            Event::Rule => renderer.rule(),
            Event::TaskListMarker(checked) => renderer.task_marker(*checked),
            Event::FootnoteReference(_) | Event::InlineMath(_) | Event::DisplayMath(_) => {}
        }
        idx += 1;
    }
    renderer.out
}

struct PendingImage {
    dest: String,
    title: String,
    alt: String,
}

struct Renderer<'a> {
    source: &'a str,
    out: String,
    ids: HashSet<String>,
    /// `true` for ordered lists.
    list_stack: Vec<bool>,
    item_depth: usize,
    in_table_head: bool,
    tbody_open: bool,
    image_stack: Vec<PendingImage>,
}

impl<'a> Renderer<'a> {
    fn start(
        &mut self,
        tag: &Tag<'_>,
        range: &std::ops::Range<usize>,
        events: &[(Event<'_>, std::ops::Range<usize>)],
        idx: usize,
    ) {
        if !self.image_stack.is_empty() {
            if let Tag::Image {
                dest_url, title, ..
            } = tag
            {
                self.image_stack.push(PendingImage {
                    dest: dest_url.to_string(),
                    title: title.to_string(),
                    alt: String::new(),
                });
            }
            return;
        }
        match tag {
            Tag::Paragraph => {
                self.ensure_newline_in_item();
                self.out.push_str("<p>");
            }
            Tag::Heading { level, .. } => {
                let id = self.heading_id(range);
                let n = heading_level(*level);
                self.out.push_str(&format!(
                    "<h{n} id=\"{id}\"><a href=\"#{id}\" class=\"anchor\" rel=\"nofollow\" aria-hidden=\"true\" name=\"{id}\"></a>"
                ));
            }
            Tag::BlockQuote(_) => {
                self.ensure_newline_in_item();
                self.out.push_str("<blockquote>\n");
            }
            Tag::CodeBlock(kind) => {
                self.ensure_newline_in_item();
                self.out.push_str("<pre><code");
                if let CodeBlockKind::Fenced(info) = kind {
                    if let Some(language) = info.split_whitespace().next() {
                        if !language.is_empty() {
                            self.out.push_str(" class=\"language-");
                            self.out.push_str(&escape_html(language));
                            self.out.push('"');
                        }
                    }
                }
                self.out.push('>');
            }
            Tag::HtmlBlock => {}
            Tag::List(start) => {
                self.ensure_newline_in_item();
                let ordered = start.is_some();
                self.list_stack.push(ordered);
                if let Some(start) = start {
                    if *start != 1 {
                        self.out.push_str(&format!("<ol start=\"{start}\">\n"));
                    } else {
                        self.out.push_str("<ol>\n");
                    }
                } else {
                    self.out.push_str("<ul>\n");
                }
            }
            Tag::Item => {
                self.item_depth += 1;
                self.out.push_str("<li>");
                if item_first_child_is_block(events.get(idx + 1)) {
                    self.out.push('\n');
                }
            }
            Tag::Table(_) => {
                self.ensure_newline_in_item();
                self.out.push_str("<table>\n");
            }
            Tag::TableHead => {
                self.in_table_head = true;
                self.out.push_str("<thead>\n<tr>\n");
            }
            Tag::TableRow => {
                if !self.tbody_open {
                    self.out.push_str("<tbody>\n");
                    self.tbody_open = true;
                }
                self.out.push_str("<tr>\n");
            }
            Tag::TableCell => {
                self.out
                    .push_str(if self.in_table_head { "<th>" } else { "<td>" });
            }
            Tag::Emphasis => self.out.push_str("<em>"),
            Tag::Strong => self.out.push_str("<strong>"),
            Tag::Strikethrough => self.out.push_str("<del>"),
            Tag::Link {
                link_type,
                dest_url,
                title,
                ..
            } => {
                // goldmark renders `<foo@example.com>` as `mailto:foo@example.com`.
                let mut dest = dest_url.to_string();
                if matches!(link_type, LinkType::Email) && !dest.starts_with("mailto:") {
                    dest = format!("mailto:{dest}");
                }
                self.out.push_str("<a href=\"");
                self.out.push_str(&escape_html(&url_escape(&dest)));
                self.out.push('"');
                if !title.is_empty() {
                    self.out.push_str(" title=\"");
                    self.out.push_str(&escape_html(title));
                    self.out.push('"');
                }
                self.out.push('>');
            }
            Tag::Image {
                dest_url, title, ..
            } => {
                self.image_stack.push(PendingImage {
                    dest: dest_url.to_string(),
                    title: title.to_string(),
                    alt: String::new(),
                });
            }
            _ => {}
        }
    }

    fn end(&mut self, tag: &TagEnd) {
        if !self.image_stack.is_empty() {
            if let TagEnd::Image = tag {
                let image = self.image_stack.pop().expect("image");
                if self.image_stack.is_empty() {
                    self.out.push_str("<img src=\"");
                    self.out.push_str(&escape_html(&url_escape(&image.dest)));
                    self.out.push_str("\" alt=\"");
                    self.out.push_str(&escape_html(&image.alt));
                    self.out.push('"');
                    if !image.title.is_empty() {
                        self.out.push_str(" title=\"");
                        self.out.push_str(&escape_html(&image.title));
                        self.out.push('"');
                    }
                    self.out.push('>');
                }
            }
            return;
        }
        match tag {
            TagEnd::Paragraph => self.out.push_str("</p>\n"),
            TagEnd::Heading(level) => {
                let n = heading_level(*level);
                self.out.push_str(&format!("</h{n}>\n"));
            }
            TagEnd::BlockQuote(_) => self.out.push_str("</blockquote>\n"),
            TagEnd::CodeBlock => self.out.push_str("</code></pre>\n"),
            TagEnd::HtmlBlock => {}
            TagEnd::List(_) => {
                let ordered = self.list_stack.pop().unwrap_or(false);
                self.out
                    .push_str(if ordered { "</ol>\n" } else { "</ul>\n" });
            }
            TagEnd::Item => {
                self.item_depth = self.item_depth.saturating_sub(1);
                self.out.push_str("</li>\n");
            }
            TagEnd::Table => {
                if self.tbody_open {
                    self.out.push_str("</tbody>\n");
                    self.tbody_open = false;
                }
                self.out.push_str("</table>\n");
            }
            TagEnd::TableHead => {
                self.in_table_head = false;
                self.out.push_str("</tr>\n</thead>\n");
            }
            TagEnd::TableRow => self.out.push_str("</tr>\n"),
            TagEnd::TableCell => {
                self.out.push_str(if self.in_table_head {
                    "</th>\n"
                } else {
                    "</td>\n"
                });
            }
            TagEnd::Emphasis => self.out.push_str("</em>"),
            TagEnd::Strong => self.out.push_str("</strong>"),
            TagEnd::Strikethrough => self.out.push_str("</del>"),
            TagEnd::Link => self.out.push_str("</a>"),
            _ => {}
        }
    }

    fn text(&mut self, text: &str) {
        if let Some(image) = self.image_stack.last_mut() {
            image.alt.push_str(text);
            return;
        }
        self.out.push_str(&escape_html(text));
    }

    fn code(&mut self, text: &str) {
        if let Some(image) = self.image_stack.last_mut() {
            image.alt.push_str(text);
            return;
        }
        self.out.push_str("<code>");
        self.out.push_str(&escape_html(text));
        self.out.push_str("</code>");
    }

    fn html(&mut self, html: &str) {
        if !self.image_stack.is_empty() {
            // `nodeToHTMLText` emits nothing for raw HTML leaves.
            return;
        }
        self.ensure_newline_in_item();
        self.out.push_str(html);
    }

    fn soft_break(&mut self) {
        if let Some(image) = self.image_stack.last_mut() {
            image.alt.push('\n');
            return;
        }
        self.out.push('\n');
    }

    fn hard_break(&mut self) {
        if !self.image_stack.is_empty() {
            // `nodeToHTMLText` only honours soft line breaks.
            return;
        }
        self.out.push_str("<br>\n");
    }

    fn rule(&mut self) {
        self.ensure_newline_in_item();
        self.out.push_str("<hr>\n");
    }

    fn task_marker(&mut self, checked: bool) {
        if checked {
            self.out
                .push_str("<input checked=\"\" disabled=\"\" type=\"checkbox\"> ");
        } else {
            self.out
                .push_str("<input disabled=\"\" type=\"checkbox\"> ");
        }
    }

    /// goldmark's `renderTextBlock` newline before a following block sibling.
    fn ensure_newline_in_item(&mut self) {
        if self.item_depth > 0 && !self.out.is_empty() && !self.out.ends_with('\n') {
            self.out.push('\n');
        }
    }

    fn heading_id(&mut self, range: &std::ops::Range<usize>) -> String {
        let line = heading_source_line(&self.source[range.clone()]);
        let raw = generate_heading_id(&line);
        let mut candidate = raw.clone();
        let mut suffix = 1;
        while self.ids.contains(&candidate) {
            candidate = format!("{raw}-{suffix}");
            suffix += 1;
        }
        self.ids.insert(candidate.clone());
        candidate
    }
}

fn heading_level(level: HeadingLevel) -> u8 {
    match level {
        HeadingLevel::H1 => 1,
        HeadingLevel::H2 => 2,
        HeadingLevel::H3 => 3,
        HeadingLevel::H4 => 4,
        HeadingLevel::H5 => 5,
        HeadingLevel::H6 => 6,
    }
}

fn item_first_child_is_block(next: Option<&(Event<'_>, std::ops::Range<usize>)>) -> bool {
    match next {
        Some((Event::Html(_), _)) => true,
        Some((
            Event::Start(
                Tag::Paragraph
                | Tag::BlockQuote(_)
                | Tag::CodeBlock(_)
                | Tag::List(_)
                | Tag::Table(_),
            ),
            _,
        )) => true,
        _ => false,
    }
}

/// The source line goldmark's `node.Lines()` uses for `Generate`.
fn heading_source_line(block: &str) -> String {
    let mut lines: Vec<&str> = block.split('\n').collect();
    if lines.last().is_some_and(|line| line.is_empty()) {
        lines.pop();
    }
    let first = lines.first().copied().unwrap_or("");
    let trimmed_start = first.trim_start_matches(is_goldmark_space);
    if trimmed_start.starts_with('#') {
        atx_heading_content(first)
    } else {
        // Setext heading: the last content line precedes the underline.
        if lines.len() >= 2 {
            lines[lines.len() - 2].to_string()
        } else {
            String::new()
        }
    }
}

fn is_goldmark_space(c: char) -> bool {
    matches!(c, ' ' | '\t' | '\n' | '\r' | '\u{000b}' | '\u{000c}')
}

fn atx_heading_content(line: &str) -> String {
    let chars: Vec<char> = line.chars().collect();
    let mut i = 0;
    while i < chars.len() && chars[i] == ' ' {
        i += 1;
    }
    let start_hashes = i;
    while i < chars.len() && chars[i] == '#' {
        i += 1;
    }
    if i == start_hashes {
        return String::new();
    }
    let level = i - start_hashes;
    let _ = level;
    if i >= chars.len() {
        return String::new();
    }
    let mut l = i;
    while l < chars.len() && chars[l] == ' ' {
        l += 1;
    }
    let spaces = l - i;
    if spaces == 0 {
        return String::new();
    }
    let mut start = i + spaces;
    if start >= chars.len() {
        start = chars.len() - 1;
    }
    let orig_start = start;
    let mut stop = chars.len();
    while stop > 0 && is_goldmark_space(chars[stop - 1]) {
        stop -= 1;
    }
    if stop <= start {
        stop = start;
    } else {
        let mut i = stop - 1;
        while i >= start && chars[i] == '#' {
            if i == 0 {
                break;
            }
            i -= 1;
        }
        let hash_stop = i + 1;
        let all_hashes = chars[start..stop].iter().all(|c| *c == '#');
        stop = if all_hashes {
            start
        } else if hash_stop != stop && !is_goldmark_space(chars[i]) {
            stop
        } else {
            hash_stop
        };
        if stop < start {
            stop = start;
        }
        start = orig_start;
    }
    if start > stop || start > chars.len() {
        return String::new();
    }
    chars[start..stop].iter().collect()
}

/// goldmark `ids.Generate`.
fn generate_heading_id(line: &str) -> String {
    let trimmed = line.trim_matches(is_goldmark_space);
    let mut result = String::new();
    for ch in trimmed.chars() {
        if ch.is_ascii_alphanumeric() {
            result.push(ch.to_ascii_lowercase());
        } else if ch == ' ' || ch == '-' || ch == '_' {
            result.push('-');
        }
    }
    if result.is_empty() {
        result.push_str("heading");
    }
    result
}

/// goldmark `util.URLEscape(v, false)`.
fn url_escape(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    let bytes = input.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        let b = bytes[i];
        if b == b'%' && i + 2 < bytes.len() && bytes[i + 1].is_ascii_hexdigit() {
            // Mirrors goldmark's `IsHexDecimal(v[i+1])` check.
            out.push_str(&input[i..i + 3]);
            i += 3;
            continue;
        }
        if is_url_safe(b) {
            out.push(b as char);
            i += 1;
            continue;
        }
        if b == b' ' {
            out.push_str("%20");
            i += 1;
            continue;
        }
        let ch_len = utf8_char_len(b);
        if ch_len == 0 || i + ch_len > bytes.len() {
            i += 1;
            continue;
        }
        for byte in &bytes[i..i + ch_len] {
            out.push_str(&format!("%{byte:02X}"));
        }
        i += ch_len;
    }
    out
}

fn is_url_safe(b: u8) -> bool {
    b.is_ascii_alphanumeric()
        || matches!(
            b,
            b';' | b'/'
                | b'?'
                | b':'
                | b'@'
                | b'&'
                | b'='
                | b'+'
                | b'$'
                | b','
                | b'-'
                | b'_'
                | b'.'
                | b'!'
                | b'~'
                | b'*'
                | b'\''
                | b'('
                | b')'
                | b'#'
        )
}

fn utf8_char_len(b: u8) -> usize {
    if b < 0x80 {
        1
    } else if b & 0xE0 == 0xC0 {
        2
    } else if b & 0xF0 == 0xE0 {
        3
    } else if b & 0xF8 == 0xF0 {
        4
    } else {
        0
    }
}

// ---------------------------------------------------------------------------
// Sanitizer (`internal/htmlutil/policy.go`, bluemonday UGCPolicy)
// ---------------------------------------------------------------------------

/// x/net/html `EscapeString`, which the sanitizer uses for text and attributes.
fn escape_html(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    for ch in input.chars() {
        match ch {
            '&' => out.push_str("&amp;"),
            '\'' => out.push_str("&#39;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&#34;"),
            '\r' => out.push_str("&#13;"),
            _ => out.push(ch),
        }
    }
    out
}

enum Token {
    Text(String),
    Start {
        name: String,
        attrs: Vec<(String, String)>,
        self_closing: bool,
    },
    End(String),
}

struct Tokenizer<'a> {
    input: &'a [u8],
    pos: usize,
    raw_tag: Option<&'static str>,
}

impl<'a> Tokenizer<'a> {
    fn new(input: &'a str) -> Self {
        Self {
            input: input.as_bytes(),
            pos: 0,
            raw_tag: None,
        }
    }

    fn next_token(&mut self) -> Result<Option<Token>, ()> {
        if self.pos >= self.input.len() {
            return Ok(None);
        }
        if let Some(tag) = self.raw_tag {
            let end = find_raw_end(self.input, self.pos, tag);
            let text = String::from_utf8_lossy(&self.input[self.pos..end]).into_owned();
            self.pos = end;
            self.raw_tag = None;
            return Ok(Some(Token::Text(text)));
        }
        if self.input[self.pos] != b'<' {
            let end = self.input[self.pos..]
                .iter()
                .position(|b| *b == b'<')
                .map(|offset| self.pos + offset)
                .unwrap_or(self.input.len());
            let text = String::from_utf8_lossy(&self.input[self.pos..end]).into_owned();
            self.pos = end;
            return Ok(Some(Token::Text(text)));
        }
        // At '<'.
        let rest = &self.input[self.pos..];
        if rest.starts_with(b"<!--") {
            let end = find_bytes(self.input, self.pos + 4, b"-->")
                .map(|p| p + 3)
                .ok_or(())?;
            self.pos = end;
            return Ok(Some(Token::Text(String::new())));
        }
        if rest.starts_with(b"<!") || rest.starts_with(b"<?") {
            let end = self
                .input
                .iter()
                .skip(self.pos)
                .position(|b| *b == b'>')
                .map(|offset| self.pos + offset + 1)
                .ok_or(())?;
            self.pos = end;
            return Ok(Some(Token::Text(String::new())));
        }
        if rest.starts_with(b"</") {
            let (name, end) = parse_end_tag(self.input, self.pos)?;
            self.pos = end;
            return Ok(Some(Token::End(name)));
        }
        if rest.len() < 2 || !rest[1].is_ascii_alphabetic() {
            // A literal '<' in text.
            self.pos += 1;
            return Ok(Some(Token::Text("<".to_string())));
        }
        let (name, attrs, self_closing, end) = parse_start_tag(self.input, self.pos)?;
        self.pos = end;
        if name == "script" || name == "style" {
            self.raw_tag = Some(if name == "script" { "script" } else { "style" });
        }
        Ok(Some(Token::Start {
            name,
            attrs,
            self_closing,
        }))
    }
}

fn find_bytes(haystack: &[u8], from: usize, needle: &[u8]) -> Option<usize> {
    if needle.is_empty() || from > haystack.len() {
        return None;
    }
    haystack[from..]
        .windows(needle.len())
        .position(|window| window.eq_ignore_ascii_case(needle))
        .map(|offset| from + offset)
}

/// Position of `</tag` (case-insensitive) or end of input.
fn find_raw_end(haystack: &[u8], from: usize, tag: &str) -> usize {
    let needle = format!("</{tag}");
    find_bytes(haystack, from, needle.as_bytes()).unwrap_or(haystack.len())
}

fn parse_end_tag(input: &[u8], start: usize) -> Result<(String, usize), ()> {
    let mut i = start + 2;
    let name_start = i;
    while i < input.len() && !is_tag_name_end(input[i]) {
        i += 1;
    }
    let name = String::from_utf8_lossy(&input[name_start..i]).to_ascii_lowercase();
    while i < input.len() && input[i] != b'>' {
        i += 1;
    }
    if i >= input.len() {
        return Err(());
    }
    Ok((name, i + 1))
}

fn parse_start_tag(
    input: &[u8],
    start: usize,
) -> Result<(String, Vec<(String, String)>, bool, usize), ()> {
    let mut i = start + 1;
    let name_start = i;
    while i < input.len() && !is_tag_name_end(input[i]) {
        i += 1;
    }
    let name = String::from_utf8_lossy(&input[name_start..i]).to_ascii_lowercase();
    let mut attrs = Vec::new();
    let mut self_closing = false;
    loop {
        while i < input.len() && is_html_space(input[i]) {
            i += 1;
        }
        if i >= input.len() {
            return Err(());
        }
        if input[i] == b'>' {
            i += 1;
            break;
        }
        if input[i] == b'/' {
            if i + 1 < input.len() && input[i + 1] == b'>' {
                self_closing = true;
                i += 2;
                break;
            }
            i += 1;
            continue;
        }
        let attr_start = i;
        while i < input.len()
            && !is_html_space(input[i])
            && input[i] != b'='
            && input[i] != b'>'
            && input[i] != b'/'
        {
            i += 1;
        }
        let attr_name = String::from_utf8_lossy(&input[attr_start..i]).to_ascii_lowercase();
        while i < input.len() && is_html_space(input[i]) {
            i += 1;
        }
        let mut value = String::new();
        if i < input.len() && input[i] == b'=' {
            i += 1;
            while i < input.len() && is_html_space(input[i]) {
                i += 1;
            }
            if i < input.len() && (input[i] == b'"' || input[i] == b'\'') {
                let quote = input[i];
                i += 1;
                let value_start = i;
                while i < input.len() && input[i] != quote {
                    i += 1;
                }
                value = String::from_utf8_lossy(&input[value_start..i]).into_owned();
                if i < input.len() {
                    i += 1;
                }
            } else {
                let value_start = i;
                while i < input.len() && !is_html_space(input[i]) && input[i] != b'>' {
                    i += 1;
                }
                value = String::from_utf8_lossy(&input[value_start..i]).into_owned();
            }
        }
        attrs.push((attr_name, decode_entities(&value)));
    }
    Ok((name, attrs, self_closing, i))
}

fn is_tag_name_end(b: u8) -> bool {
    is_html_space(b) || b == b'/' || b == b'>'
}

fn is_html_space(b: u8) -> bool {
    matches!(b, b' ' | b'\t' | b'\n' | b'\r' | 0x0b | 0x0c)
}

fn decode_entities(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    let mut rest = input;
    while let Some(pos) = rest.find('&') {
        out.push_str(&rest[..pos]);
        rest = &rest[pos..];
        if let Some((decoded, consumed)) = decode_entity(rest) {
            out.push(decoded);
            rest = &rest[consumed..];
        } else {
            out.push('&');
            rest = &rest[1..];
        }
    }
    out.push_str(rest);
    out
}

fn decode_entity(input: &str) -> Option<(char, usize)> {
    let bytes = input.as_bytes();
    if bytes.len() < 3 || bytes[0] != b'&' {
        return None;
    }
    if bytes[1] == b'#' {
        let (hex, digits_start) = if bytes.get(2) == Some(&b'x') || bytes.get(2) == Some(&b'X') {
            (true, 3)
        } else {
            (false, 2)
        };
        let mut i = digits_start;
        while i < bytes.len() && bytes[i] != b';' {
            i += 1;
        }
        if i >= bytes.len() || i == digits_start {
            return None;
        }
        let digits = &input[digits_start..i];
        let value = if hex {
            u32::from_str_radix(digits, 16).ok()?
        } else {
            digits.parse::<u32>().ok()?
        };
        let ch = char::from_u32(value).unwrap_or('\u{fffd}');
        return Some((ch, i + 1));
    }
    let mut i = 1;
    while i < bytes.len() && bytes[i] != b';' && i <= 32 {
        i += 1;
    }
    if i >= bytes.len() || bytes[i] != b';' {
        return None;
    }
    let name = &input[1..i];
    let ch = match name {
        "amp" => '&',
        "lt" => '<',
        "gt" => '>',
        "quot" => '"',
        "apos" => '\'',
        "nbsp" => '\u{a0}',
        "copy" => '\u{a9}',
        "reg" => '\u{ae}',
        "hellip" => '\u{2026}',
        "mdash" => '\u{2014}',
        "ndash" => '\u{2013}',
        "lsquo" => '\u{2018}',
        "rsquo" => '\u{2019}',
        "ldquo" => '\u{201c}',
        "rdquo" => '\u{201d}',
        "trade" => '\u{2122}',
        _ => return None,
    };
    Some((ch, i + 1))
}

#[derive(Default)]
struct Policy {
    /// element -> attribute -> policies (`None` means "no regexp").
    elements: HashMap<&'static str, HashMap<&'static str, Vec<Option<Regex>>>>,
    no_attrs: HashSet<&'static str>,
    skip_content: HashSet<&'static str>,
    globals: HashMap<&'static str, Vec<Option<Regex>>>,
}

fn regex(pattern: &str) -> Option<Regex> {
    Some(Regex::new(pattern).expect("valid sanitizer regexp"))
}

impl Policy {
    fn allow_element(&mut self, name: &'static str) {
        self.elements.entry(name).or_default();
    }

    fn allow_attr(&mut self, el: &'static str, key: &'static str, pattern: Option<&str>) {
        self.elements
            .entry(el)
            .or_default()
            .entry(key)
            .or_default()
            .push(pattern.and_then(regex));
    }

    fn allow_global(&mut self, key: &'static str, pattern: Option<&str>) {
        self.globals
            .entry(key)
            .or_default()
            .push(pattern.and_then(regex));
    }
}

fn sanitizer_policy() -> &'static Policy {
    static POLICY: OnceLock<Policy> = OnceLock::new();
    POLICY.get_or_init(build_policy)
}

fn build_policy() -> Policy {
    let mut p = Policy::default();

    // AllowStandardAttributes.
    p.allow_global("dir", Some("(?i)^(rtl|ltr)$"));
    p.allow_global("lang", Some("[a-zA-Z]{2,20}"));
    p.allow_global("id", Some(r"[a-zA-Z0-9:\-_.]+"));
    p.allow_global("title", Some(PARAGRAPH));

    // UGCPolicy AllowElements.
    for element in [
        "article",
        "aside",
        "figure",
        "section",
        "summary",
        "hgroup",
        "h1",
        "h2",
        "h3",
        "h4",
        "h5",
        "h6",
        "br",
        "div",
        "hr",
        "p",
        "span",
        "wbr",
        "abbr",
        "acronym",
        "cite",
        "code",
        "dfn",
        "em",
        "figcaption",
        "mark",
        "s",
        "samp",
        "strong",
        "sub",
        "sup",
        "var",
        "b",
        "i",
        "pre",
        "small",
        "strike",
        "tt",
        "u",
        "rp",
        "rt",
        "ruby",
        "dl",
        "dt",
        "dd",
        "caption",
    ] {
        p.allow_element(element);
    }
    p.allow_attr("details", "open", Some("(?i)^(|open)$"));
    p.allow_attr("blockquote", "cite", None);
    p.allow_attr("a", "href", None);
    p.allow_attr("map", "name", Some(r"^([\p{L}\p{N}_-]+)$"));
    p.allow_attr("area", "alt", Some(PARAGRAPH));
    p.allow_attr("area", "coords", Some("^([0-9]+,)+[0-9]+$"));
    p.allow_attr("area", "href", None);
    p.allow_attr("area", "rel", Some(SPACE_SEPARATED_TOKENS));
    p.allow_attr("area", "shape", Some("(?i)^(default|circle|rect|poly)$"));
    p.allow_attr("img", "usemap", Some(r"(?i)^#[\p{L}\p{N}_-]+$"));
    p.allow_attr("q", "cite", None);
    p.allow_attr("time", "datetime", Some(ISO8601));
    p.allow_attr("bdi", "dir", Some("(?i)^(rtl|ltr)$"));
    p.allow_attr("bdo", "dir", Some("(?i)^(rtl|ltr)$"));
    p.allow_attr("del", "cite", Some(PARAGRAPH));
    p.allow_attr("del", "datetime", Some(ISO8601));
    p.allow_attr("ins", "cite", Some(PARAGRAPH));
    p.allow_attr("ins", "datetime", Some(ISO8601));

    // AllowLists.
    p.allow_element("ol");
    p.allow_element("ul");
    p.allow_element("li");
    p.allow_attr("ol", "type", Some(LIST_TYPE));
    p.allow_attr("ul", "type", Some(LIST_TYPE));
    p.allow_attr("li", "type", Some(LIST_TYPE));
    p.allow_attr("li", "value", Some("^[0-9]+$"));

    // AllowTables.
    p.allow_element("table");
    p.allow_element("col");
    p.allow_element("colgroup");
    p.allow_element("thead");
    p.allow_element("tr");
    p.allow_element("td");
    p.allow_element("th");
    p.allow_element("tbody");
    p.allow_element("tfoot");
    p.allow_attr("table", "height", Some(NUMBER_OR_PERCENT));
    p.allow_attr("table", "width", Some(NUMBER_OR_PERCENT));
    p.allow_attr("table", "summary", Some(PARAGRAPH));
    for el in ["col", "colgroup"] {
        p.allow_attr(el, "align", Some(CELL_ALIGN));
        p.allow_attr(el, "height", Some(NUMBER_OR_PERCENT));
        p.allow_attr(el, "width", Some(NUMBER_OR_PERCENT));
        p.allow_attr(el, "span", Some("^[0-9]+$"));
        p.allow_attr(el, "valign", Some(CELL_VERTICAL_ALIGN));
    }
    for el in ["thead", "tr"] {
        p.allow_attr(el, "align", Some(CELL_ALIGN));
        p.allow_attr(el, "valign", Some(CELL_VERTICAL_ALIGN));
    }
    for el in ["td", "th"] {
        p.allow_attr(el, "abbr", Some(PARAGRAPH));
        p.allow_attr(el, "align", Some(CELL_ALIGN));
        p.allow_attr(el, "colspan", Some("^[0-9]+$"));
        p.allow_attr(el, "rowspan", Some("^[0-9]+$"));
        p.allow_attr(el, "headers", Some(SPACE_SEPARATED_TOKENS));
        p.allow_attr(el, "height", Some(NUMBER_OR_PERCENT));
        p.allow_attr(el, "width", Some(NUMBER_OR_PERCENT));
        p.allow_attr(el, "scope", Some("(?i)(?:row|col)(?:group)?"));
        p.allow_attr(el, "valign", Some(CELL_VERTICAL_ALIGN));
        p.allow_attr(el, "nowrap", Some("(?i)|nowrap"));
    }
    for el in ["tbody", "tfoot"] {
        p.allow_attr(el, "align", Some(CELL_ALIGN));
        p.allow_attr(el, "valign", Some(CELL_VERTICAL_ALIGN));
    }
    p.allow_attr("meter", "value", Some(NUMBER));
    p.allow_attr("meter", "min", Some(NUMBER));
    p.allow_attr("meter", "max", Some(NUMBER));
    p.allow_attr("meter", "low", Some(NUMBER));
    p.allow_attr("meter", "high", Some(NUMBER));
    p.allow_attr("meter", "optimum", Some(NUMBER));
    p.allow_attr("progress", "value", Some(NUMBER));
    p.allow_attr("progress", "max", Some(NUMBER));

    // AllowImages.
    p.allow_element("img");
    p.allow_attr("img", "align", None);
    p.allow_attr("img", "alt", Some(PARAGRAPH));
    p.allow_attr("img", "height", Some(NUMBER_OR_PERCENT));
    p.allow_attr("img", "width", Some(NUMBER_OR_PERCENT));
    p.allow_attr("img", "src", None);

    // Sourcegraph additions (`internal/htmlutil/policy.go`).
    p.allow_attr("a", "name", Some(SPACE_SEPARATED_TOKENS));
    p.allow_attr("a", "rel", Some("^nofollow$"));
    p.allow_attr("a", "class", Some("^anchor$"));
    p.allow_attr("a", "aria-hidden", Some("^true$"));
    p.allow_attr("input", "type", Some("^checkbox$"));
    p.allow_attr("input", "checked", Some("^$"));
    p.allow_attr("input", "disabled", Some("^$"));
    for el in ["pre", "code"] {
        p.allow_attr(
            el,
            "class",
            Some(r"^(?:chroma-[a-zA-Z0-9\-]+)|chroma|language-[a-zA-Z]+$"),
        );
    }
    p.allow_attr(
        "span",
        "class",
        Some(r"^(?:(chroma|ansi)-[a-zA-Z0-9\-]+)|chroma$"),
    );
    p.allow_attr("div", "class", Some(r"^jp-[a-zA-Z0-9\-]+"));
    p.allow_attr("ol", "start", None);
    p.allow_attr("img", "align", None);
    p.allow_attr("p", "align", None);
    p.allow_element("picture");
    p.allow_element("video");
    p.allow_element("track");
    p.allow_element("source");
    p.allow_element("input");
    for key in ["srcset", "src", "type", "media", "width", "height", "sizes"] {
        p.allow_attr("source", key, None);
    }
    for key in [
        "playsinline",
        "muted",
        "autoplay",
        "loop",
        "controls",
        "width",
        "height",
        "poster",
        "src",
    ] {
        p.allow_attr("video", key, None);
    }
    for key in ["src", "kind", "srclang", "default", "label"] {
        p.allow_attr("track", key, None);
    }

    // `addDefaultElementsWithoutAttrs`, intersected with allowed elements.
    for element in [
        "abbr",
        "acronym",
        "article",
        "aside",
        "b",
        "bdi",
        "blockquote",
        "br",
        "caption",
        "cite",
        "code",
        "col",
        "colgroup",
        "dd",
        "del",
        "details",
        "dfn",
        "div",
        "dl",
        "dt",
        "em",
        "figcaption",
        "figure",
        "h1",
        "h2",
        "h3",
        "h4",
        "h5",
        "h6",
        "hgroup",
        "hr",
        "i",
        "ins",
        "li",
        "mark",
        "ol",
        "p",
        "picture",
        "pre",
        "q",
        "rp",
        "rt",
        "ruby",
        "s",
        "samp",
        "section",
        "small",
        "span",
        "strike",
        "strong",
        "sub",
        "summary",
        "sup",
        "table",
        "tbody",
        "td",
        "tfoot",
        "th",
        "thead",
        "time",
        "tr",
        "tt",
        "u",
        "ul",
        "var",
        "video",
        "wbr",
    ] {
        p.no_attrs.insert(element);
    }

    // `addDefaultSkipElementContent`.
    for element in [
        "frame", "frameset", "iframe", "noembed", "noframes", "noscript", "nostyle", "object",
        "script", "style", "title",
    ] {
        p.skip_content.insert(element);
    }

    p
}

const PARAGRAPH: &str = r"^[\p{L}\p{N}\t\n\f\r \-_',\[\]!./\\()]*$";
const SPACE_SEPARATED_TOKENS: &str = r"^([\t\n\f\r \p{L}\p{N}_-]+)$";
const ISO8601: &str = r"^[0-9]{4}(-[0-9]{2}(-[0-9]{2}([ T][0-9]{2}(:[0-9]{2}){1,2}(.[0-9]{1,6})?Z?([\+-][0-9]{2}:[0-9]{2})?)?)?)?$";
const LIST_TYPE: &str = "(?i)^(circle|disc|square|a|A|i|I|1)$";
const CELL_ALIGN: &str = "(?i)^(center|justify|left|right|char)$";
const CELL_VERTICAL_ALIGN: &str = "(?i)^(baseline|bottom|middle|top)$";
const NUMBER: &str = r"^[-+]?[0-9]*\.?[0-9]+([eE][-+]?[0-9]+)?$";
const NUMBER_OR_PERCENT: &str = r"^[0-9]+[%]?$";

struct Sanitizer {
    out: String,
    skip_element_content: bool,
    skipping_elements_count: i64,
    skip_closing_tag: bool,
    closing_tag_to_skip_stack: Vec<String>,
    most_recently_started_token: String,
}

fn sanitize_html(input: &str) -> String {
    let policy = sanitizer_policy();
    let mut tokenizer = Tokenizer::new(input);
    let mut sanitizer = Sanitizer {
        out: String::new(),
        skip_element_content: false,
        skipping_elements_count: 0,
        skip_closing_tag: false,
        closing_tag_to_skip_stack: Vec::new(),
        most_recently_started_token: String::new(),
    };
    loop {
        let token = match tokenizer.next_token() {
            Ok(Some(token)) => token,
            Ok(None) => break,
            Err(()) => return String::new(),
        };
        match token {
            Token::Text(text) => {
                if sanitizer.skip_element_content {
                    continue;
                }
                if matches!(
                    sanitizer.most_recently_started_token.as_str(),
                    "script" | "style"
                ) {
                    // `allowUnsafe` is false: raw script/style text is dropped.
                    continue;
                }
                sanitizer
                    .out
                    .push_str(&escape_html(&decode_entities(&text)));
            }
            Token::Start {
                name,
                attrs,
                self_closing,
            } => sanitizer.start_tag(policy, name, attrs, self_closing),
            Token::End(name) => sanitizer.end_tag(policy, name),
        }
    }
    sanitizer.out
}

impl Sanitizer {
    fn start_tag(
        &mut self,
        policy: &Policy,
        name: String,
        attrs: Vec<(String, String)>,
        self_closing: bool,
    ) {
        self.most_recently_started_token = name.clone();
        if name == "script" || name == "style" {
            return;
        }
        let Some(rules) = policy.elements.get(name.as_str()) else {
            if policy.skip_content.contains(name.as_str()) {
                self.skip_element_content = true;
                self.skipping_elements_count += 1;
            }
            return;
        };
        let clean = sanitize_attrs(policy, &name, &attrs, rules);
        if clean.is_empty() && !policy.no_attrs.contains(name.as_str()) {
            self.skip_closing_tag = true;
            self.closing_tag_to_skip_stack.push(name);
            return;
        }
        if !self.skip_element_content {
            self.out
                .push_str(&render_start_tag(&name, &clean, self_closing));
        }
    }

    fn end_tag(&mut self, policy: &Policy, name: String) {
        if self.most_recently_started_token == name {
            self.most_recently_started_token.clear();
        }
        if name == "script" || name == "style" {
            return;
        }
        if self.skip_closing_tag && self.closing_tag_to_skip_stack.last() == Some(&name) {
            self.closing_tag_to_skip_stack.pop();
            if self.closing_tag_to_skip_stack.is_empty() {
                self.skip_closing_tag = false;
            }
            return;
        }
        if !policy.elements.contains_key(name.as_str()) {
            if policy.skip_content.contains(name.as_str()) {
                self.skipping_elements_count -= 1;
                if self.skipping_elements_count <= 0 {
                    self.skipping_elements_count = 0;
                    self.skip_element_content = false;
                }
            }
            return;
        }
        if !self.skip_element_content {
            self.out.push_str("</");
            self.out.push_str(&name);
            self.out.push('>');
        }
    }
}

fn sanitize_attrs(
    policy: &Policy,
    element: &str,
    attrs: &[(String, String)],
    element_rules: &HashMap<&'static str, Vec<Option<Regex>>>,
) -> Vec<(String, String)> {
    let mut clean: Vec<(String, String)> = Vec::new();
    for (key, value) in attrs {
        let mut allowed = false;
        if let Some(policies) = element_rules.get(key.as_str()) {
            for pattern in policies {
                if pattern.as_ref().is_none_or(|re| re.is_match(value)) {
                    allowed = true;
                    break;
                }
            }
        }
        if !allowed {
            if let Some(policies) = policy.globals.get(key.as_str()) {
                allowed = policies
                    .iter()
                    .any(|pattern| pattern.as_ref().is_none_or(|re| re.is_match(value)));
            }
        }
        if allowed {
            clean.push((key.clone(), value.clone()));
        }
    }
    if clean.is_empty() {
        return clean;
    }

    if linkable(element) {
        if let Some(attr_name) = url_attribute(element) {
            clean = clean
                .into_iter()
                .filter_map(|(key, value)| {
                    if key == attr_name {
                        valid_url(&value).map(|url| (key, url))
                    } else {
                        Some((key, value))
                    }
                })
                .collect();
        }
        if matches!(element, "a" | "area" | "base" | "link") && !clean.is_empty() {
            clean = add_link_rel_and_target(element, clean);
        }
    }
    clean
}

fn linkable(element: &str) -> bool {
    matches!(
        element,
        "a" | "area"
            | "base"
            | "link"
            | "blockquote"
            | "del"
            | "ins"
            | "q"
            | "audio"
            | "embed"
            | "iframe"
            | "img"
            | "input"
            | "script"
            | "source"
            | "track"
            | "video"
    )
}

fn url_attribute(element: &str) -> Option<&'static str> {
    match element {
        "a" | "area" | "base" | "link" => Some("href"),
        "blockquote" | "del" | "ins" | "q" => Some("cite"),
        "audio" | "embed" | "iframe" | "img" | "script" | "source" | "track" | "video" => {
            Some("src")
        }
        _ => None,
    }
}

fn add_link_rel_and_target(element: &str, clean: Vec<(String, String)>) -> Vec<(String, String)> {
    if clean.is_empty() {
        return clean;
    }
    let mut href_found = false;
    let mut external_link = false;
    for (key, value) in &clean {
        if key == "href" {
            href_found = true;
            if !parse_url_host(value).is_empty() {
                external_link = true;
            }
        }
    }
    if !href_found {
        return clean;
    }

    let add_no_follow = true;
    let add_target_blank = external_link;
    let mut attrs = Vec::with_capacity(clean.len() + 2);
    let mut no_follow_found = false;
    let mut target_blank_found = false;
    for (key, mut value) in clean {
        if key == "rel" && add_no_follow {
            if !value.contains("nofollow") {
                value.push_str(" nofollow");
            }
            no_follow_found = true;
            attrs.push((key, value));
            continue;
        }
        if element == "a" && key == "target" {
            if value == "_blank" {
                target_blank_found = true;
            }
            if add_target_blank && !target_blank_found {
                value = "_blank".to_string();
                target_blank_found = true;
            }
            attrs.push((key, value));
            continue;
        }
        attrs.push((key, value));
    }
    if add_no_follow && !no_follow_found {
        attrs.push(("rel".to_string(), "nofollow".to_string()));
    }
    if element == "a" && add_target_blank && !target_blank_found {
        attrs.push(("target".to_string(), "_blank".to_string()));
        target_blank_found = true;
    }
    if target_blank_found {
        let mut no_opener_added = false;
        let mut with_opener = Vec::with_capacity(attrs.len() + 1);
        for (key, mut value) in attrs.drain(..) {
            if key == "rel" {
                if !value.contains("noopener") {
                    value.push_str(" noopener");
                }
                no_opener_added = true;
                with_opener.push((key, value));
            } else {
                with_opener.push((key, value));
            }
        }
        attrs = with_opener;
        if !no_opener_added {
            attrs.push(("rel".to_string(), "noopener".to_string()));
        }
    }
    attrs
}

fn render_start_tag(name: &str, attrs: &[(String, String)], self_closing: bool) -> String {
    let mut out = String::from("<");
    out.push_str(name);
    for (key, value) in attrs {
        out.push(' ');
        out.push_str(key);
        out.push_str("=\"");
        out.push_str(&escape_html(value));
        out.push('"');
    }
    if self_closing {
        out.push_str("/>");
    } else {
        out.push('>');
    }
    out
}

/// `url.Parse(raw).Host` for the external-link check.
fn parse_url_host(raw: &str) -> String {
    let trimmed = raw.trim();
    let mut rest = trimmed;
    if let Some(colon) = trimmed.find(':') {
        if is_scheme(&trimmed[..colon]) {
            rest = &trimmed[colon + 1..];
        }
    }
    let Some(authority) = rest.strip_prefix("//") else {
        return String::new();
    };
    let end = authority.find(['/', '?', '#']).unwrap_or(authority.len());
    authority[..end].to_string()
}

fn is_scheme(candidate: &str) -> bool {
    let mut chars = candidate.chars();
    match chars.next() {
        Some(first) if first.is_ascii_alphabetic() => {}
        _ => return false,
    }
    chars.all(|c| c.is_ascii_alphanumeric() || matches!(c, '+' | '-' | '.'))
}

/// `bluemonday.Policy.validURL`, returning `url.Parse(...).String()`.
fn valid_url(raw: &str) -> Option<String> {
    let trimmed = raw.trim();
    if trimmed.contains([' ', '\t', '\n']) {
        return None;
    }
    let mut rest = trimmed;
    let mut scheme = String::new();
    if let Some(colon) = trimmed.find(':') {
        let candidate = &trimmed[..colon];
        if is_scheme(candidate) {
            scheme = candidate.to_ascii_lowercase();
            rest = &trimmed[colon + 1..];
        }
    }
    if !scheme.is_empty() {
        if !matches!(scheme.as_str(), "mailto" | "http" | "https") {
            return None;
        }
        let mut serialized = scheme.clone();
        serialized.push(':');
        serialized.push_str(rest);
        return Some(serialized);
    }
    if !trimmed.is_empty() {
        return Some(trimmed.to_string());
    }
    None
}

// ---------------------------------------------------------------------------
// `Query.highlightCode` (`cmd/frontend/internal/highlight`)
// ---------------------------------------------------------------------------

const DEFAULT_SYNTECT_SERVER: &str = "http://syntect-server:9238";
const LINE_LENGTH_LIMIT: i32 = 2000;

/// `highlight.Code` over the syntect-server HTTP contract.
#[derive(Debug, Clone)]
pub struct SyntectCodeHighlighter {
    base_url: String,
    client: reqwest::Client,
}

impl Default for SyntectCodeHighlighter {
    fn default() -> Self {
        Self::from_env()
    }
}

impl SyntectCodeHighlighter {
    /// Production assembly: `SRC_SYNTECT_SERVER` (default
    /// `http://syntect-server:9238`), with the trailing `/` removed.
    pub fn from_env() -> Self {
        let base = std::env::var("SRC_SYNTECT_SERVER")
            .unwrap_or_else(|_| DEFAULT_SYNTECT_SERVER.to_string());
        Self::with_base_url(&base)
    }

    pub fn with_base_url(base: &str) -> Self {
        Self {
            base_url: base.trim_end_matches('/').to_string(),
            client: reqwest::Client::new(),
        }
    }
}

#[async_trait::async_trait]
impl crate::graphql_inventory_c::CodeHighlighter for SyntectCodeHighlighter {
    async fn highlight(
        &self,
        code: &str,
        filepath: &str,
        disable_timeout: bool,
    ) -> Result<String, String> {
        // `highlight.Code`: normalize filepath, detect language, drop one final
        // newline, then request syntect-server.
        let filepath = normalize_filepath(filepath);
        let language = detect_language(&filepath, code);
        let mut code = code.to_string();
        if let Some(stripped) = code.strip_suffix('\n') {
            code = stripped.to_string();
        }

        let body = build_highlight_request(&filepath, &language, &code);
        let mut request = self
            .client
            .post(format!("{}/", self.base_url))
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .body(body);
        if disable_timeout {
            request = request.header("X-Stabilize-Timeout", "30s");
        }

        let send = request.send();
        let response = if disable_timeout {
            send.await
        } else {
            match tokio::time::timeout(Duration::from_secs(3), send).await {
                Ok(response) => response,
                Err(_) => return Ok(generate_plain_table(&code)),
            }
        };

        let response = match response {
            Ok(response) => response,
            Err(_) => return Ok(generate_plain_table(&code)),
        };
        if response.status() == reqwest::StatusCode::BAD_REQUEST {
            return Ok(generate_plain_table(&code));
        }
        let bytes = match response.bytes().await {
            Ok(bytes) => bytes,
            Err(_) => return Ok(generate_plain_table(&code)),
        };
        let decoded: serde_json::Value = match serde_json::from_slice(&bytes) {
            Ok(decoded) => decoded,
            Err(_) => return Ok(generate_plain_table(&code)),
        };
        let error = decoded
            .get("error")
            .and_then(|value| value.as_str())
            .unwrap_or("");
        if !error.is_empty() {
            // `highlight.Code` surfaces all server-side highlighting errors as
            // a plain-text table (`err == nil`).
            return Ok(generate_plain_table(&code));
        }
        let scip = decoded
            .get("scip")
            .and_then(|value| value.as_str())
            .unwrap_or("");
        if !scip.is_empty() {
            return Ok(scip.to_string());
        }
        Ok(decoded
            .get("data")
            .and_then(|value| value.as_str())
            .unwrap_or("")
            .to_string())
    }
}

fn normalize_filepath(filepath: &str) -> String {
    match filepath.rfind('.') {
        Some(pos) => format!(
            "{}.{}",
            &filepath[..pos],
            filepath[pos + 1..].to_lowercase()
        ),
        None => filepath.to_string(),
    }
}

/// `DetectSyntaxHighlightingLanguage` + enry's extension strategy.
fn detect_language(filepath: &str, content: &str) -> String {
    let extension = filepath.rsplit('.').next().unwrap_or("");
    // `baseHighlightConfig.Extensions` overrides enry.
    let base = match extension {
        "tsx" => Some("tsx"),
        "ncl" => Some("nickel"),
        "sbt" => Some("scala"),
        "sc" => Some("scala"),
        "xlsg" => Some("xlsg"),
        _ => None,
    };
    let language = match base {
        Some(language) => language,
        None => {
            // enry's content strategies are not ported. These three extensions
            // are reachable through `SyntectLanguageMap` and enry's
            // extension-only answer (`ecmarkup`/`plsql`/`hack`) disagrees with
            // the content-aware answer, so pin the canonical language.
            match extension {
                "html" => "html",
                "sql" => "sql",
                "php" => "php",
                _ => {
                    let _ = content;
                    enry_language(extension)
                }
            }
        }
    };
    normalize_language(language)
}

fn normalize_language(language: &str) -> String {
    let lowered = language.to_lowercase();
    match lowered.as_str() {
        "c#" => "c_sharp".to_string(),
        _ => lowered,
    }
}

fn enry_language(extension: &str) -> &'static str {
    ENRY_EXTENSIONS
        .iter()
        .find(|(ext, _)| *ext == extension)
        .map(|(_, language)| *language)
        .unwrap_or("")
}

/// Go `encoding/json` string escaping (`json.Marshal`, HTML-safe encoding).
fn go_json_string(input: &str) -> String {
    let mut out = String::with_capacity(input.len() + 2);
    out.push('"');
    for ch in input.chars() {
        match ch {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            '<' => out.push_str("\\u003c"),
            '>' => out.push_str("\\u003e"),
            '&' => out.push_str("\\u0026"),
            '\u{2028}' => out.push_str("\\u2028"),
            '\u{2029}' => out.push_str("\\u2029"),
            c if (c as u32) < 0x20 => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

fn build_highlight_request(filepath: &str, language: &str, code: &str) -> String {
    let mut body = String::from("{\"filepath\":");
    body.push_str(&go_json_string(filepath));
    body.push_str(",\"filetype\":");
    body.push_str(&go_json_string(language));
    body.push_str(",\"code\":");
    body.push_str(&go_json_string(code));
    body.push_str(",\"line_length_limit\":");
    body.push_str(&LINE_LENGTH_LIMIT.to_string());
    body.push_str(",\"engine\":\"syntect\"}");
    body
}

/// `generatePlainTable` (`highlight.go:539-580`).
fn generate_plain_table(code: &str) -> String {
    let mut out = String::from("<table>");
    for (row, line) in code.split('\n').enumerate() {
        let line = line.strip_suffix('\r').unwrap_or(line);
        let text = if line.is_empty() { "\n" } else { line };
        out.push_str("<tr><td class=\"line\" data-line=\"");
        out.push_str(&(row + 1).to_string());
        out.push_str("\"></td><td class=\"code\"><span>");
        out.push_str(&escape_html(text));
        out.push_str("</span></td></tr>");
    }
    out.push_str("</table>");
    out
}

// ---------------------------------------------------------------------------
// Tables copied from the Go baseline
// ---------------------------------------------------------------------------

/// `SyntectLanguageMap` (`syntect_language_map.go`, all 348 entries).
pub fn syntect_language(language: &str) -> &'static str {
    SYNTECT_LANGUAGE_MAP
        .iter()
        .find(|(key, _)| *key == language)
        .map(|(_, extension)| *extension)
        .unwrap_or("")
}

const SYNTECT_LANGUAGE_MAP: &[(&str, &str)] = &[
    ("txt", "txt"),
    ("apex", "apex"),
    ("asa", "asa"),
    ("asp", "asp"),
    ("actionscript", "as"),
    ("as", "as"),
    ("applescript", "applescript"),
    ("script editor", "script editor"),
    ("batchfile", "bat"),
    ("bat", "bat"),
    ("cmd", "cmd"),
    ("build", "build"),
    ("c#", "cs"),
    ("cs", "cs"),
    ("csx", "csx"),
    ("cpp", "cpp"),
    ("cc", "cc"),
    ("cp", "cp"),
    ("cxx", "cxx"),
    ("c++", "c++"),
    ("c", "c"),
    ("h", "h"),
    ("hh", "hh"),
    ("hpp", "hpp"),
    ("hxx", "hxx"),
    ("h++", "h++"),
    ("inl", "inl"),
    ("ipp", "ipp"),
    ("cmakecache.txt", "CMakeCache.txt"),
    ("cmakelists.txt", "CMakeLists.txt"),
    ("cmake", "cmake"),
    ("css", "css"),
    ("css.erb", "css.erb"),
    ("css.liquid", "css.liquid"),
    ("capnp", "capnp"),
    ("cg", "cg"),
    ("clojure", "clj"),
    ("clj", "clj"),
    ("cljs", "cljs"),
    ("cljc", "cljc"),
    ("cljx", "cljx"),
    ("crontab", "crontab"),
    ("d", "d"),
    ("di", "di"),
    ("diff", "diff"),
    ("patch", "patch"),
    ("dockerfile", "Dockerfile"),
    ("erlang", "erl"),
    ("erl", "erl"),
    ("hrl", "hrl"),
    ("emakefile", "emakefile"),
    ("yaws", "yaws"),
    ("fsharp", "fs"),
    ("forth", "frt"),
    ("frt", "frt"),
    ("essl", "essl"),
    ("f.essl", "f.essl"),
    ("v.essl", "v.essl"),
    ("_v.essl", "_v.essl"),
    ("_f.essl", "_f.essl"),
    ("_vs.essl", "_vs.essl"),
    ("_fs.essl", "_fs.essl"),
    ("vs", "vs"),
    ("fs", "fs"),
    ("gs", "gs"),
    ("vsh", "vsh"),
    ("fsh", "fsh"),
    ("gsh", "gsh"),
    ("vshader", "vshader"),
    ("fshader", "fshader"),
    ("gshader", "gshader"),
    ("vert", "vert"),
    ("frag", "frag"),
    ("geom", "geom"),
    ("tesc", "tesc"),
    ("tese", "tese"),
    ("comp", "comp"),
    ("glsl", "glsl"),
    ("attributes", "attributes"),
    ("gitattributes", "gitattributes"),
    (".gitattributes", "gitattributes"),
    ("commit_editmsg", "COMMIT_EDITMSG"),
    ("merge_msg", "MERGE_MSG"),
    ("tag_editmsg", "TAG_EDITMSG"),
    ("gitconfig", "gitconfig"),
    (".gitconfig", "gitconfig"),
    (".gitmodules", "gitmodules"),
    ("exclude", "exclude"),
    ("gitignore", "gitignore"),
    (".gitignore", "gitignore"),
    ("gitlink", "git"),
    (".git", "git"),
    ("gitlog", "gitlog"),
    ("git-rebase-todo", "git-rebase-todo"),
    ("go", "go"),
    ("graphviz-dot", "dot"),
    ("dot", "dot"),
    ("gv", "gv"),
    ("groovy", "groovy"),
    ("gvy", "gvy"),
    ("gradle", "gradle"),
    ("fx", "fx"),
    ("fxh", "fxh"),
    ("hlsl", "hlsl"),
    ("hlsli", "hlsli"),
    ("usf", "usf"),
    ("html", "html"),
    ("htm", "htm"),
    ("shtml", "shtml"),
    ("xhtml", "xhtml"),
    ("tmpl", "tmpl"),
    ("tpl", "tpl"),
    ("haskell", "hs"),
    ("hs", "hs"),
    ("literatehaskell", "lhs"),
    ("lhs", "lhs"),
    ("ini", "ini"),
    ("inf", "INF"),
    ("reg", "reg"),
    ("lng", "lng"),
    ("cfg", "cfg"),
    ("url", "url"),
    (".editorconfig", "editorconfig"),
    ("jsp", "jsp"),
    ("java", "java"),
    ("bsh", "bsh"),
    ("javaproperties", "javaproperties"),
    ("properties", "properties"),
    ("json", "json"),
    ("sublime-settings", "sublime-settings"),
    ("sublime-menu", "sublime-menu"),
    ("sublime-keymap", "sublime-keymap"),
    ("sublime-mousemap", "sublime-mousemap"),
    ("sublime-theme", "sublime-theme"),
    ("sublime-build", "sublime-build"),
    ("sublime-project", "sublime-project"),
    ("sublime-completions", "sublime-completions"),
    ("sublime-commands", "sublime-commands"),
    ("sublime-macro", "sublime-macro"),
    ("sublime-color-scheme", "sublime-color-scheme"),
    ("javascript", "js"),
    ("js", "js"),
    ("jsx", "jsx"),
    ("babel", "babel"),
    ("es6", "es6"),
    ("less", "less"),
    ("bibteX", "bib"),
    ("bib", "bib"),
    ("latex", "ltx"),
    ("tex", "tex"),
    ("ltx", "ltx"),
    ("sty", "sty"),
    ("cls", "cls"),
    ("lisp", "lisp"),
    ("cl", "cl"),
    ("clisp", "clisp"),
    ("l", "l"),
    ("mud", "mud"),
    ("el", "el"),
    ("scm", "scm"),
    ("ss", "ss"),
    ("lsp", "lsp"),
    ("fasl", "fasl"),
    ("lua", "lua"),
    ("proj", "proj"),
    ("targets", "targets"),
    ("msbuild", "msbuild"),
    ("csproj", "csproj"),
    ("vbproj", "vbproj"),
    ("fsproj", "fsproj"),
    ("vcxproj", "vcxproj"),
    ("make", "make"),
    ("gnumakefile", "GNUmakefile"),
    ("makefile", "makefile"),
    ("ocamlmakefile", "OCamlMakefile"),
    ("mak", "mak"),
    ("mk", "mk"),
    ("man", "man"),
    ("md", "md"),
    ("mdown", "mdown"),
    ("markdown", "markdown"),
    ("markdn", "markdn"),
    ("matlab", "matlab"),
    ("pom.xml", "pom.xml"),
    ("mediawiki", "mediawiki"),
    ("wikipedia", "wikipedia"),
    ("wiki", "wiki"),
    ("ninja", "ninja"),
    ("ocaml", "ml"),
    ("ml", "ml"),
    ("mli", "mli"),
    ("ocamllex", "mll"),
    ("mll", "mll"),
    ("ocamlyacc", "mly"),
    ("mly", "mly"),
    ("objective-c++", "mm"),
    ("mm", "mm"),
    ("objective-c", "m"),
    ("m", "m"),
    ("php", "php"),
    ("php3", "php3"),
    ("php4", "php4"),
    ("php5", "php5"),
    ("php7", "php7"),
    ("phps", "phps"),
    ("phpt", "phpt"),
    ("phtml", "phtml"),
    ("pascal", "pas"),
    ("pas", "pas"),
    ("p", "p"),
    ("dpr", "dpr"),
    ("spec", "spec"),
    ("client", "client"),
    ("perl", "pl"),
    ("pm", "pm"),
    ("pod", "pod"),
    ("t", "t"),
    ("pl", "pl"),
    ("postscript", "ps"),
    ("ps", "ps"),
    ("eps", "eps"),
    ("powershell", "ps1"),
    ("ps1", "ps1"),
    ("psm1", "psm1"),
    ("psd1", "psd1"),
    ("python", "py"),
    ("py", "py"),
    ("py3", "py3"),
    ("pyw", "pyw"),
    ("pyi", "pyi"),
    ("pyx", "pyx"),
    ("pyx.in", "pyx.in"),
    ("pxd", "pxd"),
    ("pxd.in", "pxd.in"),
    ("pxi", "pxi"),
    ("pxi.in", "pxi.in"),
    ("rpy", "rpy"),
    ("cpy", "cpy"),
    ("sconstruct", "sconstruct"),
    ("sconscript", "SConscript"),
    ("gyp", "gyp"),
    ("gypi", "gypi"),
    ("snakefile", "snakefile"),
    ("wscript", "wscript"),
    ("r", "r"),
    ("s", "s"),
    ("rprofile", "Rprofile"),
    ("rd", "rd"),
    ("rails", "rails"),
    ("rhtml", "rhtml"),
    ("erb", "erb"),
    ("html.erb", "html.erb"),
    ("js.erb", "js.erb"),
    ("haml", "haml"),
    ("rubyonrails", "rxml"),
    ("rxml", "rxml"),
    ("builder", "builder"),
    ("erbsql", "erbsql"),
    ("sql.erb", "sql.erb"),
    ("re", "re"),
    ("restructuredtext", "rst"),
    ("rst", "rst"),
    ("rest", "rest"),
    ("ruby", "rb"),
    ("rb", "rb"),
    ("rbi", "rb"),
    ("appfile", "Appfile"),
    ("appraisals", "Appraisals"),
    ("berksfile", "Berksfile"),
    ("brewfile", "Brewfile"),
    ("capfile", "capfile"),
    ("cgi", "cgi"),
    ("cheffile", "cheffile"),
    ("config.ru", "config.ru"),
    ("deliverfile", "Deliverfile"),
    ("fastfile", "Fastfile"),
    ("fcgi", "fcgi"),
    ("gemfile", "Gemfile"),
    ("gemspec", "gemspec"),
    ("guardfile", "Guardfile"),
    ("irbrc", "irbrc"),
    ("jbuilder", "jbuilder"),
    ("podspec", "podspec"),
    ("prawn", "prawn"),
    ("rabl", "rabl"),
    ("rake", "rake"),
    ("rakefile", "Rakefile"),
    ("rantfile", "Rantfile"),
    ("rbx", "rbx"),
    ("rjs", "rjs"),
    ("ruby.rail", "ruby.rail"),
    ("scanfile", "Scanfile"),
    ("simplecov", "simplecov"),
    ("snapfile", "Snapfile"),
    ("thor", "thor"),
    ("thorfile", "Thorfile"),
    ("vagrantfile", "Vagrantfile"),
    ("rs", "rs"),
    ("sass", "sass"),
    ("scss", "scss"),
    ("sql", "sql"),
    ("ddl", "ddl"),
    ("dml", "dml"),
    ("scala", "scala"),
    ("sbt", "sbt"),
    ("sh", "sh"),
    ("bash", "bash"),
    ("zsh", "zsh"),
    ("fish", "fish"),
    (".bash_aliases", "bash_aliases"),
    (".bash_completions", "bash_completions"),
    (".bash_functions", "bash_functions"),
    (".bash_login", "bash_login"),
    (".bash_logout", "bash_logout"),
    (".bash_profile", "bash_profile"),
    (".bash_variables", "bash_variables"),
    (".bashrc", "bashrc"),
    (".profile", "profile"),
    (".textmate_init", "textmate_init"),
    ("smalltalk", "st"),
    ("st", "st"),
    ("swift", "swift"),
    ("adp", "adp"),
    ("tcl", "tcl"),
    ("toml", "toml"),
    ("tml", "tml"),
    ("lock", "lock"),
    ("textile", "textile"),
    ("thrift", "thrift"),
    ("typescript", "ts"),
    ("ts", "ts"),
    ("typescriptreact", "tsx"),
    ("tsx", "tsx"),
    ("xlsg", "xlsg"),
    ("xml", "xml"),
    ("xsd", "xsd"),
    ("xslt", "xslt"),
    ("tld", "tld"),
    ("dtml", "dtml"),
    ("rss", "rss"),
    ("opml", "opml"),
    ("svg", "svg"),
    ("yaml", "yaml"),
    ("yml", "yml"),
    ("sublime-syntax", "sublime-syntax"),
    ("kotlin", "kt"),
    ("kt", "kt"),
    ("kts", "kts"),
];

/// enry's extension strategy for every extension reachable through
/// `SyntectLanguageMap` (empty content). Content/modeline/classifier strategies
/// are not ported; `html`, `sql` and `php` are pinned to their canonical enry
/// language because enry's classifier otherwise reports `ecmarkup`, `plsql`
/// and `hack` for extension-only detection.
const ENRY_EXTENSIONS: &[(&str, &str)] = &[
    ("Appfile", ""),
    ("Appraisals", ""),
    ("Berksfile", ""),
    ("Brewfile", ""),
    ("CMakeCache.txt", "text"),
    ("CMakeLists.txt", "text"),
    ("COMMIT_EDITMSG", ""),
    ("Deliverfile", ""),
    ("Dockerfile", "dockerfile"),
    ("Fastfile", ""),
    ("GNUmakefile", ""),
    ("Gemfile", ""),
    ("Guardfile", ""),
    ("INF", ""),
    ("MERGE_MSG", ""),
    ("OCamlMakefile", ""),
    ("Rakefile", ""),
    ("Rantfile", ""),
    ("Rprofile", ""),
    ("SConscript", ""),
    ("Scanfile", ""),
    ("Snapfile", ""),
    ("TAG_EDITMSG", ""),
    ("Thorfile", ""),
    ("Vagrantfile", ""),
    ("_f.essl", ""),
    ("_fs.essl", ""),
    ("_v.essl", ""),
    ("_vs.essl", ""),
    ("adp", ""),
    ("apex", "apex"),
    ("applescript", "applescript"),
    ("as", "actionscript"),
    ("asa", ""),
    ("asp", "classic asp"),
    ("attributes", ""),
    ("babel", ""),
    ("bash", "shell"),
    ("bash_aliases", ""),
    ("bash_completions", ""),
    ("bash_functions", ""),
    ("bash_login", ""),
    ("bash_logout", ""),
    ("bash_profile", ""),
    ("bash_variables", ""),
    ("bashrc", ""),
    ("bat", "batchfile"),
    ("bib", "bibtex"),
    ("bsh", ""),
    ("build", ""),
    ("builder", "ruby"),
    ("c", "c"),
    ("c++", "c++"),
    ("capfile", ""),
    ("capnp", "cap'n proto"),
    ("cc", "c++"),
    ("cfg", "haproxy"),
    ("cg", ""),
    ("cgi", "perl"),
    ("cheffile", ""),
    ("cl", "common lisp"),
    ("client", ""),
    ("clisp", ""),
    ("clj", "clojure"),
    ("cljc", "clojure"),
    ("cljs", "clojure"),
    ("cljx", "clojure"),
    ("cls", "apex"),
    ("cmake", "cmake"),
    ("cmd", "batchfile"),
    ("comp", ""),
    ("config.ru", "ruby"),
    ("cp", "c++"),
    ("cpp", "c++"),
    ("cpy", "cobol"),
    ("crontab", ""),
    ("cs", "c_sharp"),
    ("csproj", "xml"),
    ("css", "css"),
    ("css.erb", "html+erb"),
    ("css.liquid", "liquid"),
    ("csx", "c_sharp"),
    ("cxx", "c++"),
    ("d", "d"),
    ("ddl", "plsql"),
    ("di", "d"),
    ("diff", "diff"),
    ("dml", ""),
    ("dot", ""),
    ("dpr", "pascal"),
    ("dtml", ""),
    ("editorconfig", "editorconfig"),
    ("el", "emacs lisp"),
    ("emakefile", ""),
    ("eps", "postscript"),
    ("erb", "html+erb"),
    ("erbsql", ""),
    ("erl", "erlang"),
    ("es6", "javascript"),
    ("essl", ""),
    ("exclude", ""),
    ("f.essl", ""),
    ("fasl", ""),
    ("fcgi", "lua"),
    ("fish", "fish"),
    ("frag", "glsl"),
    ("frt", "forth"),
    ("fs", "f#"),
    ("fsh", "glsl"),
    ("fshader", "glsl"),
    ("fsproj", "xml"),
    ("fx", "flux"),
    ("fxh", "hlsl"),
    ("gemspec", "ruby"),
    ("geom", "glsl"),
    ("git", ""),
    ("git-rebase-todo", ""),
    ("gitattributes", ""),
    ("gitconfig", "git config"),
    ("gitignore", "ignore list"),
    ("gitlog", ""),
    ("gitmodules", ""),
    ("glsl", "glsl"),
    ("go", "go"),
    ("gradle", "gradle"),
    ("groovy", "groovy"),
    ("gs", "glsl"),
    ("gsh", "gsc"),
    ("gshader", "glsl"),
    ("gv", "graphviz (dot)"),
    ("gvy", "groovy"),
    ("gyp", "python"),
    ("gypi", "python"),
    ("h", "c"),
    ("h++", "c++"),
    ("haml", "haml"),
    ("hh", "c++"),
    ("hlsl", "hlsl"),
    ("hlsli", "hlsl"),
    ("hpp", "c++"),
    ("hrl", "erlang"),
    ("hs", "haskell"),
    ("htm", "html"),
    ("html", "ecmarkup"),
    ("html.erb", "html+erb"),
    ("hxx", "c++"),
    ("ini", "ini"),
    ("inl", "c++"),
    ("ipp", "c++"),
    ("irbrc", ""),
    ("java", "java"),
    ("javaproperties", ""),
    ("jbuilder", "ruby"),
    ("js", "javascript"),
    ("js.erb", "javascript+erb"),
    ("json", "json"),
    ("jsp", "java server pages"),
    ("jsx", "javascript"),
    ("kt", "kotlin"),
    ("kts", "kotlin"),
    ("l", "common lisp"),
    ("less", "less"),
    ("lhs", "literate haskell"),
    ("lisp", "common lisp"),
    ("lng", ""),
    ("lock", ""),
    ("lsp", "common lisp"),
    ("ltx", "tex"),
    ("lua", "lua"),
    ("m", "limbo"),
    ("mak", "makefile"),
    ("make", "makefile"),
    ("makefile", "makefile"),
    ("man", "roff"),
    ("markdn", ""),
    ("markdown", "markdown"),
    ("matlab", "matlab"),
    ("md", "markdown"),
    ("mdown", "markdown"),
    ("mediawiki", "wikitext"),
    ("mk", "makefile"),
    ("ml", "ocaml"),
    ("mli", "ocaml"),
    ("mll", "ocaml"),
    ("mly", "ocaml"),
    ("mm", "objective-c++"),
    ("msbuild", ""),
    ("mud", "zil"),
    ("ninja", "ninja"),
    ("opml", ""),
    ("p", "gnuplot"),
    ("pas", "pascal"),
    ("patch", "diff"),
    ("php", "hack"),
    ("php3", "php"),
    ("php4", "php"),
    ("php5", "php"),
    ("php7", ""),
    ("phps", "php"),
    ("phpt", "php"),
    ("phtml", "html+php"),
    ("pl", "perl"),
    ("pm", "perl"),
    ("pod", "pod"),
    ("podspec", "ruby"),
    ("pom.xml", "xml"),
    ("prawn", "ruby"),
    ("profile", ""),
    ("proj", "xml"),
    ("properties", "ini"),
    ("ps", "postscript"),
    ("ps1", "powershell"),
    ("psd1", "powershell"),
    ("psm1", "powershell"),
    ("pxd", "cython"),
    ("pxd.in", ""),
    ("pxi", "cython"),
    ("pxi.in", ""),
    ("py", "python"),
    ("py3", "python"),
    ("pyi", "python"),
    ("pyw", "python"),
    ("pyx", "cython"),
    ("pyx.in", ""),
    ("r", "r"),
    ("rabl", "ruby"),
    ("rails", ""),
    ("rake", "ruby"),
    ("rb", "ruby"),
    ("rbx", "ruby"),
    ("rd", "r"),
    ("re", "c++"),
    ("reg", "windows registry entries"),
    ("rest", "restructuredtext"),
    ("rhtml", "html+erb"),
    ("rjs", ""),
    ("rpy", "python"),
    ("rs", "rust"),
    ("rss", "xml"),
    ("rst", "restructuredtext"),
    ("ruby.rail", ""),
    ("rxml", ""),
    ("s", "motorola 68k assembly"),
    ("sass", "sass"),
    ("sbt", "scala"),
    ("scala", "scala"),
    ("scm", "scheme"),
    ("sconstruct", ""),
    ("script editor", ""),
    ("scss", "scss"),
    ("sh", "shell"),
    ("shtml", ""),
    ("simplecov", ""),
    ("snakefile", "snakemake"),
    ("spec", "python"),
    ("sql", "plsql"),
    ("sql.erb", "html+erb"),
    ("ss", "scheme"),
    ("st", "smalltalk"),
    ("sty", "tex"),
    ("sublime-build", "json with comments"),
    ("sublime-color-scheme", ""),
    ("sublime-commands", "json with comments"),
    ("sublime-completions", "json with comments"),
    ("sublime-keymap", "json with comments"),
    ("sublime-macro", "json with comments"),
    ("sublime-menu", "json with comments"),
    ("sublime-mousemap", "json with comments"),
    ("sublime-project", "json with comments"),
    ("sublime-settings", "json with comments"),
    ("sublime-syntax", "yaml"),
    ("sublime-theme", "json with comments"),
    ("svg", "svg"),
    ("swift", "swift"),
    ("t", "perl"),
    ("targets", "xml"),
    ("tcl", "tcl"),
    ("tesc", "glsl"),
    ("tese", "glsl"),
    ("tex", "tex"),
    ("textile", "textile"),
    ("textmate_init", ""),
    ("thor", "ruby"),
    ("thrift", "thrift"),
    ("tld", ""),
    ("tml", "xml"),
    ("tmpl", ""),
    ("toml", "toml"),
    ("tpl", "smarty"),
    ("ts", "typescript"),
    ("tsx", "tsx"),
    ("txt", "text"),
    ("url", "ini"),
    ("usf", ""),
    ("v.essl", ""),
    ("vbproj", "xml"),
    ("vcxproj", "xml"),
    ("vert", "glsl"),
    ("vs", "glsl"),
    ("vsh", "glsl"),
    ("vshader", "glsl"),
    ("wiki", "wikitext"),
    ("wikipedia", ""),
    ("wscript", ""),
    ("xhtml", "html"),
    ("xlsg", ""),
    ("xml", "xml"),
    ("xsd", "xml"),
    ("xslt", "xslt"),
    ("yaml", "yaml"),
    ("yaws", ""),
    ("yml", "yaml"),
    ("zsh", "shell"),
];
