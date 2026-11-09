#![forbid(unsafe_code)]

//! `lib/codeintel/autoindex/config`: the auto-index job specification types
//! and their two parsers.
//!
//! * [`unmarshal_json`] ports `UnmarshalJSON` (`json.go`). The baseline feeds
//!   the payload through `sourcegraph/jsonx` with comments and trailing commas
//!   enabled, then hands the normalized text to `encoding/json`. This module
//!   parses the same tolerant syntax directly (line/block comments anywhere
//!   whitespace is allowed, trailing commas in arrays and objects), then maps
//!   the result onto the structs with `encoding/json`'s rules: unknown fields
//!   ignored, field names matched exactly and then case-insensitively,
//!   duplicate keys last-wins, `null` meaning the zero value.
//! * [`unmarshal_yaml`] ports `UnmarshalYAML` (`yaml.go`) for the subset of
//!   `gopkg.in/yaml.v2` that configuration files use: block mappings and
//!   sequences, flow sequences and mappings, quoted and plain scalars,
//!   comments, document markers, duplicate keys last-wins, and scalar → string
//!   coercion (numbers and booleans keep their source text). Anchors, aliases,
//!   tags, block scalars (`|`/`>`) and multi-line plain scalars are not
//!   implemented and report an error.
//! * [`marshal_json`] ports the package-level `MarshalJSON`, which normalizes
//!   nil slices to empty ones and prints with `json.MarshalIndent(…, "", "
//!   ")` (four spaces), including Go's HTML escaping.
//!
//! Error text is intentionally not byte-identical: the callers only log the
//! error and treat a broken configuration as "present but empty" (no
//! fallback), so acceptance is what matters. JSON errors are prefixed
//! `invalid JSON: ` and YAML errors `invalid YAML: `, like the baseline.

use serde::Serialize;

/// `config.AutoIndexJobSpecList`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct AutoIndexJobSpecList {
    #[serde(rename = "index_jobs")]
    pub job_specs: Option<Vec<AutoIndexJobSpec>>,
}

/// `config.AutoIndexJobSpec`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct AutoIndexJobSpec {
    pub steps: Option<Vec<DockerStep>>,
    pub local_steps: Option<Vec<String>>,
    pub root: String,
    pub indexer: String,
    pub indexer_args: Option<Vec<String>>,
    pub outfile: String,
    #[serde(rename = "requestedEnvVars")]
    pub requested_env_vars: Option<Vec<String>>,
}

impl AutoIndexJobSpec {
    /// `AutoIndexJobSpec.GetRoot`.
    pub fn get_root(&self) -> &str {
        &self.root
    }

    /// `AutoIndexJobSpec.GetIndexerName`.
    pub fn get_indexer_name(&self) -> String {
        extract_indexer_name(&self.indexer)
    }
}

/// `config.DockerStep`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct DockerStep {
    pub root: String,
    pub image: String,
    pub commands: Option<Vec<String>>,
}

/// `extractIndexerName`: strip the `sourcegraph/` prefix and the
/// `@sha256:…` suffix.
pub fn extract_indexer_name(name: &str) -> String {
    let start = if name.starts_with("sourcegraph/") {
        "sourcegraph/".len()
    } else {
        0
    };
    let end = name.rfind('@').unwrap_or(name.len());
    name[start..end].to_owned()
}

// ---------------------------------------------------------------------------
// JSON
// ---------------------------------------------------------------------------

/// `UnmarshalJSON`. The error always starts with `invalid JSON: `.
pub fn unmarshal_json(data: &str) -> Result<AutoIndexJobSpecList, String> {
    let value = parse_json_document(data)
        .map_err(|err| format!("invalid JSON: failed to parse JSON: {err}"))?;
    decode_json_list(&value).map_err(|err| format!("invalid JSON: {err}"))
}

/// Parses the tolerant document. An input that is only whitespace yields
/// `Json::Null` (the baseline returns the zero configuration before
/// `json.Unmarshal` is reached); an input with only comments is a parse error
/// because the baseline reaches `json.Unmarshal` with empty data.
fn parse_json_document(data: &str) -> Result<Json, String> {
    let mut parser = JsonParser::new(data);
    let value = parser.parse_document()?;
    match value {
        Some(value) => Ok(value),
        None => {
            if data.trim().is_empty() {
                Ok(Json::Null)
            } else {
                Err("unexpected end of JSON input".to_owned())
            }
        }
    }
}

#[derive(Debug, PartialEq)]
enum Json {
    Null,
    Bool,
    Number,
    Str(String),
    Array(Vec<Json>),
    /// Insertion order is preserved: duplicate keys are last-wins and Go's
    /// field folding walks the keys in document order.
    Object(Vec<(String, Json)>),
}

struct JsonParser {
    chars: Vec<char>,
    pos: usize,
}

impl JsonParser {
    fn new(text: &str) -> Self {
        Self {
            chars: text.chars().collect(),
            pos: 0,
        }
    }

    fn peek(&self) -> Option<char> {
        self.chars.get(self.pos).copied()
    }

    fn next(&mut self) -> Option<char> {
        let ch = self.peek()?;
        self.pos += 1;
        Some(ch)
    }

    fn error<T>(&self, message: impl Into<String>) -> Result<T, String> {
        Err(format!("{} at offset {}", message.into(), self.pos))
    }

    /// Skips whitespace, `// …` and `/* … */` comments.
    fn skip_trivia(&mut self) -> Result<(), String> {
        loop {
            match self.peek() {
                Some(ch) if ch.is_whitespace() => {
                    self.pos += 1;
                }
                Some('/') if self.chars.get(self.pos + 1) == Some(&'/') => {
                    self.pos += 2;
                    while let Some(ch) = self.peek() {
                        self.pos += 1;
                        if ch == '\n' {
                            break;
                        }
                    }
                }
                Some('/') if self.chars.get(self.pos + 1) == Some(&'*') => {
                    self.pos += 2;
                    let mut closed = false;
                    while let Some(ch) = self.next() {
                        if ch == '*' && self.peek() == Some('/') {
                            self.pos += 1;
                            closed = true;
                            break;
                        }
                    }
                    if !closed {
                        return self.error("unterminated block comment");
                    }
                }
                _ => return Ok(()),
            }
        }
    }

    /// Parses the whole document: one value (or nothing) and then trivia only.
    fn parse_document(&mut self) -> Result<Option<Json>, String> {
        self.skip_trivia()?;
        if self.peek().is_none() {
            return Ok(None);
        }
        let value = self.parse_value()?;
        self.skip_trivia()?;
        if self.peek().is_some() {
            return self.error("unexpected trailing content");
        }
        Ok(Some(value))
    }

    fn parse_value(&mut self) -> Result<Json, String> {
        self.skip_trivia()?;
        match self.peek() {
            Some('{') => self.parse_object(),
            Some('[') => self.parse_array(),
            Some('"') => Ok(Json::Str(self.parse_string()?)),
            Some('t') => {
                self.expect_literal("true")?;
                Ok(Json::Bool)
            }
            Some('f') => {
                self.expect_literal("false")?;
                Ok(Json::Bool)
            }
            Some('n') => {
                self.expect_literal("null")?;
                Ok(Json::Null)
            }
            Some(ch) if ch == '-' || ch.is_ascii_digit() => self.parse_number(),
            Some(_) => self.error("unexpected symbol"),
            None => self.error("unexpected end of JSON input"),
        }
    }

    fn expect_literal(&mut self, literal: &str) -> Result<(), String> {
        for expected in literal.chars() {
            if self.next() != Some(expected) {
                return self.error(format!("expected `{literal}`"));
            }
        }
        Ok(())
    }

    fn parse_object(&mut self) -> Result<Json, String> {
        self.pos += 1; // '{'
        let mut entries = Vec::new();
        loop {
            self.skip_trivia()?;
            match self.peek() {
                Some('}') => {
                    self.pos += 1;
                    return Ok(Json::Object(entries));
                }
                Some('"') => {}
                _ => return self.error("expected an object key"),
            }
            let key = self.parse_string()?;
            self.skip_trivia()?;
            if self.next() != Some(':') {
                return self.error("expected ':' after object key");
            }
            let value = self.parse_value()?;
            entries.push((key, value));
            self.skip_trivia()?;
            match self.next() {
                Some(',') => {
                    self.skip_trivia()?;
                    if self.peek() == Some('}') {
                        self.pos += 1;
                        return Ok(Json::Object(entries));
                    }
                }
                Some('}') => return Ok(Json::Object(entries)),
                _ => return self.error("expected ',' or '}'"),
            }
        }
    }

    fn parse_array(&mut self) -> Result<Json, String> {
        self.pos += 1; // '['
        let mut values = Vec::new();
        loop {
            self.skip_trivia()?;
            if self.peek() == Some(']') {
                self.pos += 1;
                return Ok(Json::Array(values));
            }
            values.push(self.parse_value()?);
            self.skip_trivia()?;
            match self.next() {
                Some(',') => {
                    self.skip_trivia()?;
                    if self.peek() == Some(']') {
                        self.pos += 1;
                        return Ok(Json::Array(values));
                    }
                }
                Some(']') => return Ok(Json::Array(values)),
                _ => return self.error("expected ',' or ']'"),
            }
        }
    }

    fn parse_string(&mut self) -> Result<String, String> {
        debug_assert_eq!(self.peek(), Some('"'));
        self.pos += 1;
        let mut out = String::new();
        loop {
            let ch = match self.next() {
                Some(ch) => ch,
                None => return self.error("unterminated string"),
            };
            match ch {
                '"' => return Ok(out),
                '\\' => {
                    let escaped = match self.next() {
                        Some(ch) => ch,
                        None => return self.error("unterminated escape"),
                    };
                    match escaped {
                        '"' => out.push('"'),
                        '\\' => out.push('\\'),
                        '/' => out.push('/'),
                        'b' => out.push('\u{0008}'),
                        'f' => out.push('\u{000c}'),
                        'n' => out.push('\n'),
                        'r' => out.push('\r'),
                        't' => out.push('\t'),
                        'u' => {
                            let first = self.parse_hex4()?;
                            let codepoint = if (0xD800..=0xDBFF).contains(&first) {
                                // A high surrogate needs its low half; anything
                                // else becomes the replacement character, as
                                // `encoding/json` does.
                                if self.peek() == Some('\\')
                                    && self.chars.get(self.pos + 1) == Some(&'u')
                                {
                                    self.pos += 2;
                                    let second = self.parse_hex4()?;
                                    if (0xDC00..=0xDFFF).contains(&second) {
                                        0x10000
                                            + ((first as u32 - 0xD800) << 10)
                                            + (second as u32 - 0xDC00)
                                    } else {
                                        0xFFFD
                                    }
                                } else {
                                    0xFFFD
                                }
                            } else if (0xDC00..=0xDFFF).contains(&first) {
                                0xFFFD
                            } else {
                                first as u32
                            };
                            out.push(char::from_u32(codepoint).unwrap_or('\u{FFFD}'));
                        }
                        _ => return self.error("invalid escape sequence"),
                    }
                }
                ch if (ch as u32) < 0x20 => {
                    return self.error("control character in string");
                }
                ch => out.push(ch),
            }
        }
    }

    fn parse_hex4(&mut self) -> Result<u16, String> {
        let mut value = 0u16;
        for _ in 0..4 {
            let ch = match self.next() {
                Some(ch) => ch,
                None => return self.error("truncated unicode escape"),
            };
            let digit = match ch.to_digit(16) {
                Some(digit) => digit as u16,
                None => return self.error("invalid unicode escape"),
            };
            value = value * 16 + digit;
        }
        Ok(value)
    }

    fn parse_number(&mut self) -> Result<Json, String> {
        let start = self.pos;
        if self.peek() == Some('-') {
            self.pos += 1;
        }
        match self.peek() {
            Some('0') => self.pos += 1,
            Some(ch) if ch.is_ascii_digit() => {
                while matches!(self.peek(), Some(ch) if ch.is_ascii_digit()) {
                    self.pos += 1;
                }
            }
            _ => return self.error("invalid number"),
        };
        let mut is_float = false;
        if self.peek() == Some('.') {
            is_float = true;
            self.pos += 1;
            if !matches!(self.peek(), Some(ch) if ch.is_ascii_digit()) {
                return self.error("invalid number");
            }
            while matches!(self.peek(), Some(ch) if ch.is_ascii_digit()) {
                self.pos += 1;
            }
        }
        if matches!(self.peek(), Some('e') | Some('E')) {
            is_float = true;
            self.pos += 1;
            if matches!(self.peek(), Some('+') | Some('-')) {
                self.pos += 1;
            }
            if !matches!(self.peek(), Some(ch) if ch.is_ascii_digit()) {
                return self.error("invalid number");
            }
            while matches!(self.peek(), Some(ch) if ch.is_ascii_digit()) {
                self.pos += 1;
            }
        }
        if is_float {
            let text: String = self.chars[start..self.pos].iter().collect();
            match text.parse::<f64>() {
                Ok(value) if value.is_finite() => {}
                _ => return self.error("invalid number format"),
            }
        }
        Ok(Json::Number)
    }
}

fn decode_json_list(value: &Json) -> Result<AutoIndexJobSpecList, String> {
    match value {
        Json::Null => Ok(AutoIndexJobSpecList::default()),
        Json::Object(entries) => {
            let mut list = AutoIndexJobSpecList::default();
            // Walk the keys in document order so duplicate keys stay last-wins.
            for (key, value) in entries {
                if key == "index_jobs" || key.eq_ignore_ascii_case("index_jobs") {
                    list.job_specs = decode_json_specs(value)?;
                }
            }
            Ok(list)
        }
        _ => Err("json: cannot unmarshal value into config.AutoIndexJobSpecList".to_owned()),
    }
}

fn decode_json_specs(value: &Json) -> Result<Option<Vec<AutoIndexJobSpec>>, String> {
    match value {
        Json::Null => Ok(None),
        Json::Array(values) => {
            let mut specs = Vec::with_capacity(values.len());
            for value in values {
                specs.push(decode_json_spec(value)?);
            }
            Ok(Some(specs))
        }
        _ => Err("json: cannot unmarshal value into []config.AutoIndexJobSpec".to_owned()),
    }
}

fn decode_json_spec(value: &Json) -> Result<AutoIndexJobSpec, String> {
    match value {
        Json::Null => Ok(AutoIndexJobSpec::default()),
        Json::Object(entries) => {
            let mut spec = AutoIndexJobSpec::default();
            for (key, value) in entries {
                match key.as_str() {
                    "steps" => spec.steps = decode_json_steps(value)?,
                    "local_steps" => spec.local_steps = decode_json_strings(value)?,
                    "root" => spec.root = decode_json_string(value)?,
                    "indexer" => spec.indexer = decode_json_string(value)?,
                    "indexer_args" => spec.indexer_args = decode_json_strings(value)?,
                    "outfile" => spec.outfile = decode_json_string(value)?,
                    "requestedEnvVars" => spec.requested_env_vars = decode_json_strings(value)?,
                    other => {
                        // Go matches field names case-insensitively after the
                        // exact pass; apply the same fold for every known field.
                        if other.eq_ignore_ascii_case("steps") {
                            spec.steps = decode_json_steps(value)?;
                        } else if other.eq_ignore_ascii_case("local_steps") {
                            spec.local_steps = decode_json_strings(value)?;
                        } else if other.eq_ignore_ascii_case("root") {
                            spec.root = decode_json_string(value)?;
                        } else if other.eq_ignore_ascii_case("indexer") {
                            spec.indexer = decode_json_string(value)?;
                        } else if other.eq_ignore_ascii_case("indexer_args") {
                            spec.indexer_args = decode_json_strings(value)?;
                        } else if other.eq_ignore_ascii_case("outfile") {
                            spec.outfile = decode_json_string(value)?;
                        } else if other.eq_ignore_ascii_case("requestedEnvVars") {
                            spec.requested_env_vars = decode_json_strings(value)?;
                        }
                    }
                }
            }
            Ok(spec)
        }
        _ => Err("json: cannot unmarshal value into config.AutoIndexJobSpec".to_owned()),
    }
}

fn decode_json_steps(value: &Json) -> Result<Option<Vec<DockerStep>>, String> {
    match value {
        Json::Null => Ok(None),
        Json::Array(values) => {
            let mut steps = Vec::with_capacity(values.len());
            for value in values {
                steps.push(decode_json_step(value)?);
            }
            Ok(Some(steps))
        }
        _ => Err("json: cannot unmarshal value into []config.DockerStep".to_owned()),
    }
}

fn decode_json_step(value: &Json) -> Result<DockerStep, String> {
    match value {
        Json::Null => Ok(DockerStep::default()),
        Json::Object(entries) => {
            let mut step = DockerStep::default();
            for (key, value) in entries {
                if key == "root" || key.eq_ignore_ascii_case("root") {
                    step.root = decode_json_string(value)?;
                } else if key == "image" || key.eq_ignore_ascii_case("image") {
                    step.image = decode_json_string(value)?;
                } else if key == "commands" || key.eq_ignore_ascii_case("commands") {
                    step.commands = decode_json_strings(value)?;
                }
            }
            Ok(step)
        }
        _ => Err("json: cannot unmarshal value into config.DockerStep".to_owned()),
    }
}

fn decode_json_string(value: &Json) -> Result<String, String> {
    match value {
        Json::Null => Ok(String::new()),
        Json::Str(value) => Ok(value.clone()),
        _ => Err("json: cannot unmarshal value into string".to_owned()),
    }
}

fn decode_json_strings(value: &Json) -> Result<Option<Vec<String>>, String> {
    match value {
        Json::Null => Ok(None),
        Json::Array(values) => {
            let mut strings = Vec::with_capacity(values.len());
            for value in values {
                strings.push(decode_json_string(value)?);
            }
            Ok(Some(strings))
        }
        _ => Err("json: cannot unmarshal value into []string".to_owned()),
    }
}

// ---------------------------------------------------------------------------
// YAML
// ---------------------------------------------------------------------------

/// `UnmarshalYAML`. The error always starts with `invalid YAML: `.
pub fn unmarshal_yaml(data: &str) -> Result<AutoIndexJobSpecList, String> {
    let value = parse_yaml_document(data).map_err(|err| format!("invalid YAML: {err}"))?;
    decode_yaml_list(&value).map_err(|err| format!("invalid YAML: {err}"))
}

#[derive(Debug, PartialEq)]
enum Yaml {
    Null,
    /// Scalars keep their source text; numbers and booleans coerce to that
    /// text when the target field is a string, as `yaml.v2` does.
    Scalar(String),
    Sequence(Vec<Yaml>),
    Mapping(Vec<(String, Yaml)>),
}

struct YamlLine {
    indent: usize,
    content: String,
}

fn parse_yaml_document(data: &str) -> Result<Yaml, String> {
    let lines = preprocess_yaml_lines(data)?;
    if lines.is_empty() {
        return Ok(Yaml::Null);
    }
    let mut parser = YamlParser { lines, pos: 0 };
    let value = parser.parse_block(parser.lines[0].indent)?;
    if parser.pos < parser.lines.len() {
        return Err(format!(
            "line {}: unexpected content",
            parser.lines[parser.pos].indent
        ));
    }
    Ok(value.unwrap_or(Yaml::Null))
}

fn preprocess_yaml_lines(data: &str) -> Result<Vec<YamlLine>, String> {
    let mut lines = Vec::new();
    let mut seen_document = false;
    for (index, raw) in data.lines().enumerate() {
        let line_number = index + 1;
        if raw.contains('\t') {
            // Tabs are not allowed for indentation; yaml.v2 rejects them.
            return Err(format!("line {line_number}: found a tab character"));
        }
        let trimmed = strip_yaml_comment(raw);
        let trimmed = trimmed.trim_end();
        let content = trimmed.trim_start();
        if content.is_empty() {
            continue;
        }
        if content == "---" {
            if seen_document {
                return Err(format!("line {line_number}: multiple documents"));
            }
            seen_document = true;
            continue;
        }
        if content == "..." {
            break;
        }
        let indent = trimmed.len() - content.len();
        lines.push(YamlLine {
            indent,
            content: content.to_owned(),
        });
    }
    Ok(lines)
}

/// Removes a `#` comment that starts the line or follows whitespace and is
/// outside quotes.
fn strip_yaml_comment(line: &str) -> String {
    let chars: Vec<char> = line.chars().collect();
    let mut out = String::new();
    let mut index = 0;
    let mut quote: Option<char> = None;
    while index < chars.len() {
        let ch = chars[index];
        match quote {
            Some('\'') => {
                out.push(ch);
                if ch == '\'' {
                    if chars.get(index + 1) == Some(&'\'') {
                        out.push('\'');
                        index += 2;
                        continue;
                    }
                    quote = None;
                }
                index += 1;
            }
            Some('"') => {
                out.push(ch);
                if ch == '\\' {
                    if let Some(next) = chars.get(index + 1) {
                        out.push(*next);
                        index += 2;
                        continue;
                    }
                }
                if ch == '"' {
                    quote = None;
                }
                index += 1;
            }
            _ => {
                if ch == '#' && (index == 0 || chars[index - 1].is_whitespace()) {
                    break;
                }
                if ch == '\'' || ch == '"' {
                    quote = Some(ch);
                }
                out.push(ch);
                index += 1;
            }
        }
    }
    out
}

struct YamlParser {
    lines: Vec<YamlLine>,
    pos: usize,
}

impl YamlParser {
    fn parse_block(&mut self, indent: usize) -> Result<Option<Yaml>, String> {
        if self.pos >= self.lines.len() || self.lines[self.pos].indent < indent {
            return Ok(None);
        }
        if self.lines[self.pos].indent > indent {
            return Err("bad indentation".to_owned());
        }
        if self.is_sequence_line(indent) {
            return self.parse_sequence(indent).map(Some);
        }
        if self.looks_like_mapping(&self.lines[self.pos].content.clone()) {
            self.parse_mapping(indent).map(Some)
        } else {
            let content = self.lines[self.pos].content.clone();
            self.pos += 1;
            parse_inline_yaml(&content).map(Some)
        }
    }

    fn is_sequence_line(&self, indent: usize) -> bool {
        self.lines[self.pos].indent == indent
            && (self.lines[self.pos].content == "-"
                || self.lines[self.pos].content.starts_with("- "))
    }

    fn parse_sequence(&mut self, indent: usize) -> Result<Yaml, String> {
        let mut items = Vec::new();
        while self.pos < self.lines.len()
            && self.lines[self.pos].indent == indent
            && self.is_sequence_line(indent)
        {
            let content = self.lines[self.pos].content.clone();
            let rest = content[1..].trim_start().to_owned();
            self.pos += 1;
            if rest.is_empty() {
                let nested = match self.lines.get(self.pos).map(|line| line.indent) {
                    Some(next_indent) if next_indent > indent => {
                        self.parse_block(next_indent)?.unwrap_or(Yaml::Null)
                    }
                    _ => Yaml::Null,
                };
                items.push(nested);
                continue;
            }
            if (rest.starts_with('[')
                || rest.starts_with('{')
                || rest.starts_with('\'')
                || rest.starts_with('"'))
                == false
                && self.looks_like_mapping(&rest)
            {
                let item_indent = indent + 2;
                let mut mapping = Vec::new();
                self.parse_mapping_entry(&rest, item_indent, &mut mapping)?;
                while self.pos < self.lines.len()
                    && self.lines[self.pos].indent == item_indent
                    && self.lines[self.pos].content != "-"
                    && !self.lines[self.pos].content.starts_with("- ")
                {
                    let line = self.lines[self.pos].content.clone();
                    self.pos += 1;
                    self.parse_mapping_entry(&line, item_indent, &mut mapping)?;
                }
                items.push(Yaml::Mapping(mapping));
            } else {
                items.push(parse_inline_yaml(&rest)?);
            }
        }
        Ok(Yaml::Sequence(items))
    }

    fn parse_mapping(&mut self, indent: usize) -> Result<Yaml, String> {
        let mut entries = Vec::new();
        while self.pos < self.lines.len() && self.lines[self.pos].indent == indent {
            let content = self.lines[self.pos].content.clone();
            if content == "-" || content.starts_with("- ") {
                break;
            }
            self.pos += 1;
            self.parse_mapping_entry(&content, indent, &mut entries)?;
        }
        Ok(Yaml::Mapping(entries))
    }

    fn parse_mapping_entry(
        &mut self,
        line: &str,
        indent: usize,
        entries: &mut Vec<(String, Yaml)>,
    ) -> Result<(), String> {
        let (key, rest) = split_yaml_key(line)?;
        if rest.is_empty() {
            let nested = match self.lines.get(self.pos).map(|line| line.indent) {
                Some(next_indent) if next_indent > indent => {
                    self.parse_block(next_indent)?.unwrap_or(Yaml::Null)
                }
                _ => Yaml::Null,
            };
            entries.push((key, nested));
        } else {
            entries.push((key, parse_inline_yaml(rest)?));
        }
        Ok(())
    }

    fn looks_like_mapping(&self, content: &str) -> bool {
        split_yaml_key(content).is_ok()
    }
}

/// Splits `key: value` at the first colon that is outside quotes and flow
/// collections and is followed by whitespace or end of line. YAML also allows
/// `key:value`, but configuration files use the canonical spacing.
fn split_yaml_key(line: &str) -> Result<(String, &str), String> {
    let chars: Vec<char> = line.chars().collect();
    let mut index = 0;
    let mut quote: Option<char> = None;
    while index < chars.len() {
        let ch = chars[index];
        match quote {
            Some(q) => {
                if ch == '\\' && q == '"' {
                    index += 2;
                    continue;
                }
                if ch == q {
                    quote = None;
                }
                index += 1;
            }
            _ => {
                if ch == '\'' || ch == '"' {
                    quote = Some(ch);
                    index += 1;
                    continue;
                }
                if ch == ':' {
                    let after = chars.get(index + 1).copied();
                    if after.is_none() || after == Some(' ') || after == Some('\t') {
                        let key = parse_yaml_key(&line[..byte_index(line, index)])?;
                        return Ok((key, line[byte_index(line, index + 1)..].trim()));
                    }
                }
                index += 1;
            }
        }
    }
    Err("expected a mapping key".to_owned())
}

/// Byte offset of the char at `char_index` (the string is valid UTF-8).
fn byte_index(text: &str, char_index: usize) -> usize {
    text.char_indices()
        .nth(char_index)
        .map(|(byte, _)| byte)
        .unwrap_or(text.len())
}

fn parse_yaml_key(raw: &str) -> Result<String, String> {
    let raw = raw.trim();
    if raw.starts_with('\'') {
        parse_single_quoted(raw)
    } else if raw.starts_with('"') {
        parse_double_quoted(raw)
    } else {
        Ok(raw.to_owned())
    }
}

fn parse_inline_yaml(text: &str) -> Result<Yaml, String> {
    let text = text.trim();
    if text.is_empty() {
        return Ok(Yaml::Null);
    }
    if text.starts_with('[') || text.starts_with('{') {
        let mut flow = FlowParser {
            chars: text.chars().collect(),
            pos: 0,
        };
        let value = flow.parse_value()?;
        flow.skip_whitespace();
        if flow.pos != flow.chars.len() {
            return Err("unexpected content after flow collection".to_owned());
        }
        return Ok(value);
    }
    if text.starts_with('\'') {
        return parse_single_quoted(text).map(Yaml::Scalar);
    }
    if text.starts_with('"') {
        return parse_double_quoted(text).map(Yaml::Scalar);
    }
    Ok(resolve_plain_scalar(text))
}

fn resolve_plain_scalar(text: &str) -> Yaml {
    let trimmed = text.trim();
    match trimmed {
        "" | "~" | "null" | "Null" | "NULL" => Yaml::Null,
        _ => Yaml::Scalar(trimmed.to_owned()),
    }
}

fn parse_single_quoted(text: &str) -> Result<String, String> {
    let chars: Vec<char> = text.chars().collect();
    debug_assert_eq!(chars.first(), Some(&'\''));
    let mut out = String::new();
    let mut index = 1;
    while index < chars.len() {
        if chars[index] == '\'' {
            if chars.get(index + 1) == Some(&'\'') {
                out.push('\'');
                index += 2;
                continue;
            }
            return Ok(out);
        }
        out.push(chars[index]);
        index += 1;
    }
    Err("unterminated single-quoted scalar".to_owned())
}

fn parse_double_quoted(text: &str) -> Result<String, String> {
    let chars: Vec<char> = text.chars().collect();
    debug_assert_eq!(chars.first(), Some(&'"'));
    let mut out = String::new();
    let mut index = 1;
    while index < chars.len() {
        let ch = chars[index];
        match ch {
            '"' => return Ok(out),
            '\\' => {
                index += 1;
                let escaped = match chars.get(index) {
                    Some(ch) => *ch,
                    None => return Err("unterminated escape".to_owned()),
                };
                match escaped {
                    '0' => out.push('\0'),
                    'a' => out.push('\u{0007}'),
                    'b' => out.push('\u{0008}'),
                    't' | '\t' => out.push('\t'),
                    'n' => out.push('\n'),
                    'v' => out.push('\u{000b}'),
                    'f' => out.push('\u{000c}'),
                    'r' => out.push('\r'),
                    'e' => out.push('\u{001b}'),
                    ' ' => out.push(' '),
                    '"' => out.push('"'),
                    '/' => out.push('/'),
                    '\\' => out.push('\\'),
                    'x' => {
                        let value = parse_hex_chars(&chars, index + 1, 2)?;
                        index += 2;
                        out.push(char::from_u32(value).unwrap_or('\u{FFFD}'));
                    }
                    'u' => {
                        let value = parse_hex_chars(&chars, index + 1, 4)?;
                        index += 4;
                        out.push(char::from_u32(value).unwrap_or('\u{FFFD}'));
                    }
                    'U' => {
                        let value = parse_hex_chars(&chars, index + 1, 8)?;
                        index += 8;
                        out.push(char::from_u32(value).unwrap_or('\u{FFFD}'));
                    }
                    other => out.push(other),
                }
                index += 1;
            }
            ch => {
                out.push(ch);
                index += 1;
            }
        }
    }
    Err("unterminated double-quoted scalar".to_owned())
}

fn parse_hex_chars(chars: &[char], start: usize, count: usize) -> Result<u32, String> {
    let mut value = 0u32;
    for offset in 0..count {
        let ch = chars
            .get(start + offset)
            .ok_or_else(|| "truncated hex escape".to_owned())?;
        let digit = ch
            .to_digit(16)
            .ok_or_else(|| "invalid hex escape".to_owned())?;
        value = value * 16 + digit;
    }
    Ok(value)
}

struct FlowParser {
    chars: Vec<char>,
    pos: usize,
}

impl FlowParser {
    fn skip_whitespace(&mut self) {
        while matches!(self.chars.get(self.pos), Some(ch) if ch.is_whitespace()) {
            self.pos += 1;
        }
    }

    fn peek(&self) -> Option<char> {
        self.chars.get(self.pos).copied()
    }

    fn parse_value(&mut self) -> Result<Yaml, String> {
        self.skip_whitespace();
        match self.peek() {
            Some('[') => self.parse_sequence(),
            Some('{') => self.parse_mapping(),
            Some('\'') | Some('"') => {
                let rest: String = self.chars[self.pos..].iter().collect();
                let value = if self.peek() == Some('\'') {
                    parse_single_quoted(&rest)?
                } else {
                    parse_double_quoted(&rest)?
                };
                // Advance past the quoted scalar by counting the consumed
                // source characters.
                let consumed = if self.peek() == Some('\'') {
                    consumed_single_quoted(&rest)
                } else {
                    consumed_double_quoted(&rest)
                };
                self.pos += consumed;
                Ok(Yaml::Scalar(value))
            }
            Some(_) => {
                let start = self.pos;
                while let Some(ch) = self.peek() {
                    if matches!(ch, ',' | ']' | '}') {
                        break;
                    }
                    self.pos += 1;
                }
                let text: String = self.chars[start..self.pos].iter().collect();
                Ok(resolve_plain_scalar(&text))
            }
            None => Err("unexpected end of flow collection".to_owned()),
        }
    }

    fn parse_sequence(&mut self) -> Result<Yaml, String> {
        self.pos += 1; // '['
        let mut items = Vec::new();
        loop {
            self.skip_whitespace();
            if self.peek() == Some(']') {
                self.pos += 1;
                return Ok(Yaml::Sequence(items));
            }
            items.push(self.parse_value()?);
            self.skip_whitespace();
            match self.peek() {
                Some(',') => self.pos += 1,
                Some(']') => {
                    self.pos += 1;
                    return Ok(Yaml::Sequence(items));
                }
                _ => return Err("expected ',' or ']'".to_owned()),
            }
        }
    }

    /// A flow mapping key: a quoted scalar or the text before `:`.
    fn parse_mapping_key(&mut self) -> Result<String, String> {
        self.skip_whitespace();
        match self.peek() {
            Some('\'') | Some('"') => match self.parse_value()? {
                Yaml::Scalar(text) => Ok(text),
                _ => Err("unsupported flow key".to_owned()),
            },
            Some(_) => {
                let start = self.pos;
                while let Some(ch) = self.peek() {
                    if matches!(ch, ':' | ',' | ']' | '}') {
                        break;
                    }
                    self.pos += 1;
                }
                let text: String = self.chars[start..self.pos].iter().collect();
                Ok(text.trim().to_owned())
            }
            None => Err("unexpected end of flow mapping".to_owned()),
        }
    }

    fn parse_mapping(&mut self) -> Result<Yaml, String> {
        self.pos += 1; // '{'
        let mut entries = Vec::new();
        loop {
            self.skip_whitespace();
            if self.peek() == Some('}') {
                self.pos += 1;
                return Ok(Yaml::Mapping(entries));
            }
            let key = self.parse_mapping_key()?;
            self.skip_whitespace();
            if self.peek() != Some(':') {
                return Err("expected ':' in flow mapping".to_owned());
            }
            self.pos += 1;
            let value = self.parse_value()?;
            entries.push((key, value));
            self.skip_whitespace();
            match self.peek() {
                Some(',') => self.pos += 1,
                Some('}') => {
                    self.pos += 1;
                    return Ok(Yaml::Mapping(entries));
                }
                _ => return Err("expected ',' or '}'".to_owned()),
            }
        }
    }
}

fn consumed_single_quoted(text: &str) -> usize {
    let chars: Vec<char> = text.chars().collect();
    let mut index = 1;
    while index < chars.len() {
        if chars[index] == '\'' {
            if chars.get(index + 1) == Some(&'\'') {
                index += 2;
                continue;
            }
            return index + 1;
        }
        index += 1;
    }
    chars.len()
}

fn consumed_double_quoted(text: &str) -> usize {
    let chars: Vec<char> = text.chars().collect();
    let mut index = 1;
    while index < chars.len() {
        if chars[index] == '\\' {
            index += 2;
            continue;
        }
        if chars[index] == '"' {
            return index + 1;
        }
        index += 1;
    }
    chars.len()
}

fn decode_yaml_list(value: &Yaml) -> Result<AutoIndexJobSpecList, String> {
    match value {
        Yaml::Null => Ok(AutoIndexJobSpecList::default()),
        Yaml::Mapping(entries) => {
            let mut list = AutoIndexJobSpecList::default();
            for (key, value) in entries {
                if key == "index_jobs" {
                    list.job_specs = decode_yaml_specs(value)?;
                }
            }
            Ok(list)
        }
        _ => Err("cannot unmarshal value into config.AutoIndexJobSpecList".to_owned()),
    }
}

fn decode_yaml_specs(value: &Yaml) -> Result<Option<Vec<AutoIndexJobSpec>>, String> {
    match value {
        Yaml::Null => Ok(None),
        Yaml::Sequence(values) => {
            let mut specs = Vec::with_capacity(values.len());
            for value in values {
                specs.push(decode_yaml_spec(value)?);
            }
            Ok(Some(specs))
        }
        _ => Err("cannot unmarshal value into []config.AutoIndexJobSpec".to_owned()),
    }
}

fn decode_yaml_spec(value: &Yaml) -> Result<AutoIndexJobSpec, String> {
    match value {
        Yaml::Null => Ok(AutoIndexJobSpec::default()),
        Yaml::Mapping(entries) => {
            let mut spec = AutoIndexJobSpec::default();
            for (key, value) in entries {
                match key.as_str() {
                    "steps" => spec.steps = decode_yaml_steps(value)?,
                    "local_steps" => spec.local_steps = decode_yaml_strings(value)?,
                    "root" => spec.root = decode_yaml_string(value)?,
                    "indexer" => spec.indexer = decode_yaml_string(value)?,
                    "indexer_args" => spec.indexer_args = decode_yaml_strings(value)?,
                    "outfile" => spec.outfile = decode_yaml_string(value)?,
                    "requestedEnvVars" => spec.requested_env_vars = decode_yaml_strings(value)?,
                    _ => {}
                }
            }
            Ok(spec)
        }
        _ => Err("cannot unmarshal value into config.AutoIndexJobSpec".to_owned()),
    }
}

fn decode_yaml_steps(value: &Yaml) -> Result<Option<Vec<DockerStep>>, String> {
    match value {
        Yaml::Null => Ok(None),
        Yaml::Sequence(values) => {
            let mut steps = Vec::with_capacity(values.len());
            for value in values {
                steps.push(decode_yaml_step(value)?);
            }
            Ok(Some(steps))
        }
        _ => Err("cannot unmarshal value into []config.DockerStep".to_owned()),
    }
}

fn decode_yaml_step(value: &Yaml) -> Result<DockerStep, String> {
    match value {
        Yaml::Null => Ok(DockerStep::default()),
        Yaml::Mapping(entries) => {
            let mut step = DockerStep::default();
            for (key, value) in entries {
                match key.as_str() {
                    "root" => step.root = decode_yaml_string(value)?,
                    "image" => step.image = decode_yaml_string(value)?,
                    "commands" => step.commands = decode_yaml_strings(value)?,
                    _ => {}
                }
            }
            Ok(step)
        }
        _ => Err("cannot unmarshal value into config.DockerStep".to_owned()),
    }
}

/// `yaml.v2` coerces any scalar to a string using its source text; only
/// collections are rejected.
fn decode_yaml_string(value: &Yaml) -> Result<String, String> {
    match value {
        Yaml::Null => Ok(String::new()),
        Yaml::Scalar(text) => Ok(text.clone()),
        _ => Err("cannot unmarshal value into string".to_owned()),
    }
}

fn decode_yaml_strings(value: &Yaml) -> Result<Option<Vec<String>>, String> {
    match value {
        Yaml::Null => Ok(None),
        Yaml::Sequence(values) => {
            let mut strings = Vec::with_capacity(values.len());
            for value in values {
                strings.push(decode_yaml_string(value)?);
            }
            Ok(Some(strings))
        }
        _ => Err("cannot unmarshal value into []string".to_owned()),
    }
}

// ---------------------------------------------------------------------------
// Marshal
// ---------------------------------------------------------------------------

/// The package-level `config.MarshalJSON`: nil slices are normalized to empty
/// ones, then the value is printed with `json.MarshalIndent(…, "", "    ")`.
pub fn marshal_json(config: &AutoIndexJobSpecList) -> String {
    let mut out = String::new();
    out.push('{');
    out.push('\n');
    out.push_str("    \"index_jobs\": ");
    match &config.job_specs {
        Some(specs) => write_json_spec_array(&mut out, specs, 1),
        None => {
            // MarshalJSON normalizes nil to an empty slice.
            out.push_str("[]");
        }
    }
    out.push('\n');
    out.push('}');
    out
}

fn write_json_spec_array(out: &mut String, specs: &[AutoIndexJobSpec], depth: usize) {
    if specs.is_empty() {
        out.push_str("[]");
        return;
    }
    out.push('[');
    for (index, spec) in specs.iter().enumerate() {
        if index > 0 {
            out.push(',');
        }
        out.push('\n');
        push_indent(out, depth + 1);
        write_json_spec(out, spec, depth + 1);
    }
    out.push('\n');
    push_indent(out, depth);
    out.push(']');
}

fn write_json_spec(out: &mut String, spec: &AutoIndexJobSpec, depth: usize) {
    out.push('{');
    out.push('\n');
    push_indent(out, depth + 1);
    out.push_str("\"steps\": ");
    match &spec.steps {
        Some(steps) => write_json_step_array(out, steps, depth + 1),
        None => out.push_str("[]"),
    }
    out.push_str(",\n");
    push_indent(out, depth + 1);
    out.push_str("\"local_steps\": ");
    write_json_string_array(out, spec.local_steps.as_deref().unwrap_or(&[]), depth + 1);
    out.push_str(",\n");
    push_indent(out, depth + 1);
    out.push_str("\"root\": ");
    write_json_string(out, &spec.root);
    out.push_str(",\n");
    push_indent(out, depth + 1);
    out.push_str("\"indexer\": ");
    write_json_string(out, &spec.indexer);
    out.push_str(",\n");
    push_indent(out, depth + 1);
    out.push_str("\"indexer_args\": ");
    write_json_string_array(out, spec.indexer_args.as_deref().unwrap_or(&[]), depth + 1);
    out.push_str(",\n");
    push_indent(out, depth + 1);
    out.push_str("\"outfile\": ");
    write_json_string(out, &spec.outfile);
    out.push_str(",\n");
    push_indent(out, depth + 1);
    out.push_str("\"requestedEnvVars\": ");
    match &spec.requested_env_vars {
        Some(values) => write_json_string_array(out, values, depth + 1),
        None => out.push_str("null"),
    }
    out.push('\n');
    push_indent(out, depth);
    out.push('}');
}

fn write_json_step_array(out: &mut String, steps: &[DockerStep], depth: usize) {
    if steps.is_empty() {
        out.push_str("[]");
        return;
    }
    out.push('[');
    for (index, step) in steps.iter().enumerate() {
        if index > 0 {
            out.push(',');
        }
        out.push('\n');
        push_indent(out, depth + 1);
        write_json_step(out, step, depth + 1);
    }
    out.push('\n');
    push_indent(out, depth);
    out.push(']');
}

fn write_json_step(out: &mut String, step: &DockerStep, depth: usize) {
    out.push('{');
    out.push('\n');
    push_indent(out, depth + 1);
    out.push_str("\"root\": ");
    write_json_string(out, &step.root);
    out.push_str(",\n");
    push_indent(out, depth + 1);
    out.push_str("\"image\": ");
    write_json_string(out, &step.image);
    out.push_str(",\n");
    push_indent(out, depth + 1);
    out.push_str("\"commands\": ");
    write_json_string_array(out, step.commands.as_deref().unwrap_or(&[]), depth + 1);
    out.push('\n');
    push_indent(out, depth);
    out.push('}');
}

fn write_json_string_array(out: &mut String, values: &[String], depth: usize) {
    if values.is_empty() {
        out.push_str("[]");
        return;
    }
    out.push('[');
    for (index, value) in values.iter().enumerate() {
        if index > 0 {
            out.push(',');
        }
        out.push('\n');
        push_indent(out, depth + 1);
        write_json_string(out, value);
    }
    out.push('\n');
    push_indent(out, depth);
    out.push(']');
}

fn push_indent(out: &mut String, depth: usize) {
    for _ in 0..depth {
        out.push_str("    ");
    }
}

/// `encoding/json` string escaping, including HTML escaping and U+2028/2029.
fn write_json_string(out: &mut String, value: &str) {
    out.push('"');
    for ch in value.chars() {
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
            ch if (ch as u32) < 0x20 => {
                out.push_str(&format!("\\u{:04x}", ch as u32));
            }
            ch => out.push(ch),
        }
    }
    out.push('"');
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lsif_go_spec() -> AutoIndexJobSpec {
        AutoIndexJobSpec {
            steps: Some(vec![DockerStep {
                root: String::new(),
                image: "go:latest".to_owned(),
                commands: Some(vec!["go mod vendor".to_owned()]),
            }]),
            indexer: "lsif-go".to_owned(),
            indexer_args: Some(vec!["--no-animation".to_owned()]),
            ..Default::default()
        }
    }

    #[test]
    fn unmarshal_json_accepts_comments_and_trailing_commas() {
        let input = r#"
        {
            // comment
            "index_jobs": [
                {
                    "steps": [{"image": "go:latest", "commands": ["go mod vendor"],},],
                    "indexer": "lsif-go",
                    "indexer_args": ["--no-animation"],
                },
            ],
        }
        "#;
        let list = unmarshal_json(input).expect("parse");
        assert_eq!(list.job_specs, Some(vec![lsif_go_spec()]));
    }

    #[test]
    fn unmarshal_json_matches_go_zero_values() {
        let list = unmarshal_json("null").expect("null");
        assert_eq!(list, AutoIndexJobSpecList::default());
        let list = unmarshal_json("").expect("empty");
        assert_eq!(list, AutoIndexJobSpecList::default());
        let list = unmarshal_json(r#"{"index_jobs": []}"#).expect("empty array");
        assert_eq!(list.job_specs, Some(vec![]));
    }

    #[test]
    fn unmarshal_json_is_case_insensitive_for_field_names() {
        let list = unmarshal_json(r#"{"INDEX_JOBS": [{"INDEXER": "a"}]}"#).expect("parse");
        let specs = list.job_specs.expect("specs");
        assert_eq!(specs[0].indexer, "a");
    }

    #[test]
    fn unmarshal_json_rejects_wrong_types() {
        assert!(unmarshal_json(r#"{"index_jobs": {}}"#).is_err());
        assert!(unmarshal_json(r#"{"index_jobs": [{"local_steps": [1]}]}"#).is_err());
        assert!(unmarshal_json("[1,2]").is_err());
        assert!(unmarshal_json(r#"{"index_jobs": []} trailing"#).is_err());
    }

    #[test]
    fn unmarshal_yaml_table_matches_the_baseline_example() {
        let input = r#"
index_jobs:
  -
    steps:
      - image: go:latest
        commands:
          - go mod vendor
    indexer: lsif-go
    indexer_args:
      - --no-animation
  -
    root: web/
    indexer: scip-typescript
    indexer_args: ['index', '--yarn-workspaces']
    outfile: lsif.dump
"#;
        let list = unmarshal_yaml(input).expect("parse");
        let specs = list.job_specs.expect("specs");
        assert_eq!(specs.len(), 2);
        assert_eq!(specs[0], lsif_go_spec());
        assert_eq!(specs[1].root, "web/");
        assert_eq!(
            specs[1].indexer_args,
            Some(vec!["index".to_owned(), "--yarn-workspaces".to_owned()])
        );
        assert_eq!(specs[1].outfile, "lsif.dump");
    }

    #[test]
    fn unmarshal_yaml_coerces_scalars_to_strings() {
        let list =
            unmarshal_yaml("index_jobs:\n  - root: 123\n    indexer: true\n").expect("parse");
        let specs = list.job_specs.expect("specs");
        assert_eq!(specs[0].root, "123");
        assert_eq!(specs[0].indexer, "true");
    }

    #[test]
    fn unmarshal_yaml_rejects_scalar_for_sequence() {
        assert!(unmarshal_yaml("index_jobs: \"x\"\n").is_err());
        assert!(unmarshal_yaml("index_jobs:\n  - local_steps: go\n").is_err());
    }

    #[test]
    fn marshal_json_normalizes_nil_slices() {
        let output = marshal_json(&AutoIndexJobSpecList::default());
        assert_eq!(output, "{\n    \"index_jobs\": []\n}");

        let list = AutoIndexJobSpecList {
            job_specs: Some(vec![AutoIndexJobSpec {
                indexer: "<&>".to_owned(),
                ..Default::default()
            }]),
        };
        let output = marshal_json(&list);
        assert!(output.contains("\"indexer\": \"\\u003c\\u0026\\u003e\""));
        assert!(output.contains("\"steps\": []"));
        assert!(output.contains("\"requestedEnvVars\": null"));
    }

    #[test]
    fn extract_indexer_name_strips_prefix_and_suffix() {
        assert_eq!(extract_indexer_name("lsif-go"), "lsif-go");
        assert_eq!(extract_indexer_name("sourcegraph/lsif-go"), "lsif-go");
        assert_eq!(
            extract_indexer_name("sourcegraph/lsif-go@sha256:..."),
            "lsif-go"
        );
        assert_eq!(extract_indexer_name("myownlsif-go"), "myownlsif-go");
    }
}
