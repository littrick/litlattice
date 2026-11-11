#![forbid(unsafe_code)]

//! Environment snapshot compatible with the baseline `SetDefaultEnv` /
//! `os.Getenv` semantics, plus the small parsing helpers the `cmd/server`
//! startup sequence needs.

use std::collections::BTreeMap;

/// A deterministic (sorted) environment map. The baseline mutates the process
/// environment; the Rust port keeps the same set-if-absent rules in a map so the
/// startup plan is testable without leaking state between cases.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct EnvMap {
    values: BTreeMap<String, String>,
}

impl EnvMap {
    pub fn new() -> Self {
        Self::default()
    }

    /// Snapshot of the current process environment.
    pub fn from_process() -> Self {
        let mut values = BTreeMap::new();
        for (key, value) in std::env::vars() {
            values.insert(key, value);
        }
        Self { values }
    }

    pub fn from_pairs<I, K, V>(pairs: I) -> Self
    where
        I: IntoIterator<Item = (K, V)>,
        K: Into<String>,
        V: Into<String>,
    {
        let mut values = BTreeMap::new();
        for (key, value) in pairs {
            values.insert(key.into(), value.into());
        }
        Self { values }
    }

    pub fn get(&self, key: &str) -> Option<&str> {
        self.values.get(key).map(String::as_str)
    }

    /// `os.Getenv`: an unset variable reads as the empty string.
    pub fn value(&self, key: &str) -> String {
        self.values.get(key).cloned().unwrap_or_default()
    }

    pub fn is_set(&self, key: &str) -> bool {
        self.values.contains_key(key)
    }

    pub fn is_non_empty(&self, key: &str) -> bool {
        self.values
            .get(key)
            .map(|value| !value.is_empty())
            .unwrap_or(false)
    }

    pub fn set(&mut self, key: impl Into<String>, value: impl Into<String>) {
        self.values.insert(key.into(), value.into());
    }

    /// `shared.SetDefaultEnv`: set only when the variable is absent.
    pub fn set_default(&mut self, key: impl Into<String>, value: impl Into<String>) {
        self.values.entry(key.into()).or_insert_with(|| value.into());
    }

    pub fn iter(&self) -> impl Iterator<Item = (&String, &String)> {
        self.values.iter()
    }

    pub fn as_map(&self) -> &BTreeMap<String, String> {
        &self.values
    }
}

/// Go `strconv.ParseBool`. Returns `None` for a value Go would reject; callers
/// that ignore the error use `.unwrap_or(false)`.
pub fn parse_go_bool(raw: &str) -> Option<bool> {
    match raw {
        "1" | "t" | "T" | "TRUE" | "true" | "True" => Some(true),
        "0" | "f" | "F" | "FALSE" | "false" | "False" => Some(false),
        _ => None,
    }
}

/// Go `fmt.Sprintf("%q", s)` for the ASCII host strings used by the Procfile.
pub fn go_quote(value: &str) -> String {
    let mut out = String::with_capacity(value.len() + 2);
    out.push('"');
    for byte in value.bytes() {
        match byte {
            b'"' => out.push_str("\\\""),
            b'\\' => out.push_str("\\\\"),
            b'\n' => out.push_str("\\n"),
            b'\r' => out.push_str("\\r"),
            b'\t' => out.push_str("\\t"),
            0x20..=0x7e => out.push(byte as char),
            _ => out.push_str(&format!("\\x{byte:02x}")),
        }
    }
    out.push('"');
    out
}

/// `os.ExpandEnv`: `$NAME` and `${NAME}` are replaced from `get`.
pub fn go_expand_env(value: &str, get: &dyn Fn(&str) -> String) -> String {
    let bytes = value.as_bytes();
    let mut out = String::with_capacity(value.len());
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] != b'$' {
            out.push(bytes[index] as char);
            index += 1;
            continue;
        }
        if index + 1 < bytes.len() && bytes[index + 1] == b'{' {
            if let Some(end) = value[index + 2..].find('}') {
                let name = &value[index + 2..index + 2 + end];
                out.push_str(&get(name));
                index += 2 + end + 1;
                continue;
            }
        }
        let mut end = index + 1;
        let mut name = String::new();
        while end < bytes.len() && (bytes[end].is_ascii_alphanumeric() || bytes[end] == b'_') {
            name.push(bytes[end] as char);
            end += 1;
        }
        if name.is_empty() {
            out.push('$');
            index += 1;
        } else {
            out.push_str(&get(&name));
            index = end;
        }
    }
    out
}

/// A small `godotenv.Load` subset: `KEY=VALUE`, blank lines, `#` comments,
/// optional `export ` and single/double quoted values. The server startup only
/// relies on these shapes.
pub fn parse_dotenv(contents: &str) -> Vec<(String, String)> {
    let mut pairs = Vec::new();
    for raw_line in contents.split('\n') {
        let line = raw_line.trim_end_matches('\r').trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let line = line.strip_prefix("export ").unwrap_or(line).trim();
        let Some((key, raw_value)) = line.split_once('=') else {
            continue;
        };
        let key = key.trim();
        if key.is_empty() {
            continue;
        }
        let raw_value = raw_value.trim();
        let value = if raw_value.len() >= 2
            && raw_value.starts_with('"')
            && raw_value.ends_with('"')
        {
            unescape_double_quoted(&raw_value[1..raw_value.len() - 1])
        } else if raw_value.len() >= 2
            && raw_value.starts_with('\'')
            && raw_value.ends_with('\'')
        {
            raw_value[1..raw_value.len() - 1].to_owned()
        } else if raw_value.starts_with('#') {
            String::new()
        } else {
            raw_value.to_owned()
        };
        pairs.push((key.to_owned(), value));
    }
    pairs
}

fn unescape_double_quoted(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    let mut chars = value.chars();
    while let Some(ch) = chars.next() {
        if ch != '\\' {
            out.push(ch);
            continue;
        }
        match chars.next() {
            Some('n') => out.push('\n'),
            Some('r') => out.push('\r'),
            Some('t') => out.push('\t'),
            Some('"') => out.push('"'),
            Some('\\') => out.push('\\'),
            Some('$') => out.push('$'),
            Some(other) => {
                out.push('\\');
                out.push(other);
            }
            None => out.push('\\'),
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn go_bool_matches_strconv_parse_bool() {
        for (raw, expected) in [
            ("1", Some(true)),
            ("t", Some(true)),
            ("TRUE", Some(true)),
            ("True", Some(true)),
            ("0", Some(false)),
            ("F", Some(false)),
            ("false", Some(false)),
            ("yes", None),
            ("", None),
        ] {
            assert_eq!(parse_go_bool(raw), expected, "{raw}");
        }
    }

    #[test]
    fn quote_and_expand() {
        assert_eq!(go_quote("127.0.0.1:3178"), "\"127.0.0.1:3178\"");
        assert_eq!(
            go_expand_env("$HOME/.netrc", &|key| match key {
                "HOME" => "/root".to_owned(),
                _ => String::new(),
            }),
            "/root/.netrc"
        );
        assert_eq!(
            go_expand_env("${A}x$B", &|key| key.to_owned()),
            "AxB"
        );
    }

    #[test]
    fn dotenv_subset() {
        let pairs = parse_dotenv(
            "# comment\nexport A=1\nB=\"two words\"\nC='lit $x'\nD=\nE=value # inline\n",
        );
        assert_eq!(
            pairs,
            vec![
                ("A".to_owned(), "1".to_owned()),
                ("B".to_owned(), "two words".to_owned()),
                ("C".to_owned(), "lit $x".to_owned()),
                ("D".to_owned(), String::new()),
                ("E".to_owned(), "value # inline".to_owned()),
            ]
        );
    }
}
