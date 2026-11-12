#![forbid(unsafe_code)]

//! `golang.org/x/mod/module` + `golang.org/x/mod/semver` (v0.18.0), reduced to
//! the functions the Go module package source and proxy client call:
//!
//! - `module.Check`, `module.CheckPath`, `module.EscapePath`,
//!   `module.EscapeVersion` (with `SplitPathVersion`/`CheckPathMajor`);
//! - `semver.IsValid`, `semver.Major`, `semver.Build`.
//!
//! Error strings are byte-for-byte the baseline's `ModuleError` /
//! `InvalidPathError` / `InvalidVersionError` formatting.

/// `module.Version`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash)]
pub struct ModuleVersion {
    pub path: String,
    pub version: String,
}

impl ModuleVersion {
    /// `module.Version.String`.
    pub fn to_syntax(&self) -> String {
        if self.version.is_empty() {
            self.path.clone()
        } else {
            format!("{}@{}", self.path, self.version)
        }
    }
}

/// `module.Check`: validate a path/version pair.
pub fn check(path: &str, version: &str) -> Result<(), String> {
    check_path(path)?;
    if !is_valid(version) {
        // `module.ModuleError` special-cases its wrapped `InvalidVersionError`.
        return Err(format!(
            "{path}@{version}: invalid version: not a semantic version"
        ));
    }
    let (_, path_major, _) = split_path_version(path);
    check_path_major(version, &path_major)
        .map_err(|detail| format!("{path}@{version}: invalid version: {detail}"))
}

/// `module.CheckPath`.
pub fn check_path(path: &str) -> Result<(), String> {
    check_general_path(path, PathKind::Module)
        .map_err(|err| format!("malformed module path {}: {err}", go_quote(path)))?;

    let first_end = path.find('/').unwrap_or(path.len());
    if first_end == 0 {
        return Err(format!(
            "malformed module path {}: leading slash",
            go_quote(path)
        ));
    }
    if !path[..first_end].contains('.') {
        return Err(format!(
            "malformed module path {}: missing dot in first path element",
            go_quote(path)
        ));
    }
    if path.starts_with('-') {
        return Err(format!(
            "malformed module path {}: leading dash in first path element",
            go_quote(path)
        ));
    }
    for ch in path[..first_end].chars() {
        if !first_path_ok(ch) {
            return Err(format!(
                "malformed module path {}: invalid char {} in first path element",
                go_quote(path),
                go_quote_char(ch)
            ));
        }
    }
    if !split_path_version(path).2 {
        return Err(format!(
            "malformed module path {}: invalid version",
            go_quote(path)
        ));
    }
    Ok(())
}

fn first_path_ok(ch: char) -> bool {
    ch == '-' || ch == '.' || ch.is_ascii_digit() || ch.is_ascii_lowercase()
}

fn mod_path_ok(ch: char) -> bool {
    ch == '-' || ch == '.' || ch == '_' || ch == '~' || ch.is_ascii_alphanumeric()
}

/// `checkElem`'s `fileNameOK` for ASCII (the only characters `EscapeVersion`
/// can see after `checkElem`).
fn file_name_ok(ch: char) -> bool {
    if ch.is_ascii() {
        if ch.is_ascii_alphanumeric() {
            return true;
        }
        return "!#$%&()+,-.=@[]^_{}~ ".contains(ch);
    }
    ch.is_alphabetic()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PathKind {
    Module,
    File,
}

/// `checkPath`.
fn check_general_path(path: &str, kind: PathKind) -> Result<(), String> {
    if path.is_empty() {
        return Err("empty string".to_owned());
    }
    if path.starts_with('-') && kind != PathKind::File {
        return Err("leading dash".to_owned());
    }
    if path.contains("//") {
        return Err("double slash".to_owned());
    }
    if path.ends_with('/') {
        return Err("trailing slash".to_owned());
    }
    let mut elem_start = 0;
    for (index, ch) in path.char_indices() {
        if ch == '/' {
            check_elem(&path[elem_start..index], kind)?;
            elem_start = index + 1;
        }
    }
    check_elem(&path[elem_start..], kind)
}

/// `checkElem`.
fn check_elem(elem: &str, kind: PathKind) -> Result<(), String> {
    if elem.is_empty() {
        return Err("empty path element".to_owned());
    }
    if elem.chars().all(|ch| ch == '.') {
        return Err(format!("invalid path element {}", go_quote(elem)));
    }
    if elem.starts_with('.') && kind == PathKind::Module {
        return Err("leading dot in path element".to_owned());
    }
    if elem.ends_with('.') {
        return Err("trailing dot in path element".to_owned());
    }
    for ch in elem.chars() {
        let ok = match kind {
            PathKind::Module => mod_path_ok(ch),
            PathKind::File => file_name_ok(ch),
        };
        if !ok {
            return Err(format!("invalid char {}", go_quote_char(ch)));
        }
    }

    let short = elem.split('.').next().unwrap_or(elem);
    for bad in BAD_WINDOWS_NAMES {
        if bad.eq_ignore_ascii_case(short) {
            return Err(format!(
                "{} disallowed as path element component on Windows",
                go_quote(short)
            ));
        }
    }

    if kind == PathKind::File {
        return Ok(());
    }

    if let Some(tilde) = short.rfind('~') {
        if tilde < short.len() - 1 {
            let suffix = &short[tilde + 1..];
            if suffix.chars().all(|ch| ch.is_ascii_digit()) {
                return Err("trailing tilde and digits in path element".to_owned());
            }
        }
    }
    Ok(())
}

const BAD_WINDOWS_NAMES: &[&str] = &[
    "CON", "PRN", "AUX", "NUL", "COM0", "COM1", "COM2", "COM3", "COM4", "COM5", "COM6", "COM7",
    "COM8", "COM9", "LPT0", "LPT1", "LPT2", "LPT3", "LPT4", "LPT5", "LPT6", "LPT7", "LPT8", "LPT9",
];

/// `module.SplitPathVersion`.
pub fn split_path_version(path: &str) -> (String, String, bool) {
    if path.starts_with("gopkg.in/") {
        return split_gopkg_in(path);
    }

    let bytes = path.as_bytes();
    let mut i = bytes.len();
    let mut dot = false;
    while i > 0 && (bytes[i - 1].is_ascii_digit() || bytes[i - 1] == b'.') {
        if bytes[i - 1] == b'.' {
            dot = true;
        }
        i -= 1;
    }
    if i <= 1 || i == bytes.len() || bytes[i - 1] != b'v' || bytes[i - 2] != b'/' {
        return (path.to_owned(), String::new(), true);
    }
    let prefix = &path[..i - 2];
    let path_major = &path[i - 2..];
    if dot || path_major.len() <= 2 || path_major.as_bytes()[2] == b'0' || path_major == "/v1" {
        return (path.to_owned(), String::new(), false);
    }
    (prefix.to_owned(), path_major.to_owned(), true)
}

/// `splitGopkgIn`.
fn split_gopkg_in(path: &str) -> (String, String, bool) {
    if !path.starts_with("gopkg.in/") {
        return (path.to_owned(), String::new(), false);
    }
    let bytes = path.as_bytes();
    let mut i = bytes.len();
    if path.ends_with("-unstable") {
        i -= "-unstable".len();
    }
    while i > 0 && bytes[i - 1].is_ascii_digit() {
        i -= 1;
    }
    if i <= 1 || bytes[i - 1] != b'v' || bytes[i - 2] != b'.' {
        return (path.to_owned(), String::new(), false);
    }
    let prefix = &path[..i - 2];
    let path_major = &path[i - 2..];
    if path_major.len() <= 2
        || (path_major.as_bytes()[2] == b'0' && path_major != ".v0")
    {
        return (path.to_owned(), String::new(), false);
    }
    (prefix.to_owned(), path_major.to_owned(), true)
}

/// `module.CheckPathMajor`; returns the detail carried by the baseline's
/// `InvalidVersionError` (without its `version "..." invalid: ` prefix).
fn check_path_major(version: &str, path_major: &str) -> Result<(), String> {
    let mut path_major = path_major.to_owned();
    if path_major.starts_with(".v") && path_major.ends_with("-unstable") {
        path_major.truncate(path_major.len() - "-unstable".len());
    }
    if version.starts_with("v0.0.0-") && path_major == ".v1" {
        return Ok(());
    }
    let m = major(version);
    if path_major.is_empty() {
        if m == "v0" || m == "v1" || build(version) == "+incompatible" {
            return Ok(());
        }
        path_major = "v0 or v1".to_owned();
    } else if path_major.starts_with('/') || path_major.starts_with('.') {
        if m == path_major[1..] {
            return Ok(());
        }
        path_major = path_major[1..].to_owned();
    }
    Err(format!("should be {path_major}, not {m}"))
}

/// `module.EscapePath`.
pub fn escape_path(path: &str) -> Result<String, String> {
    check_path(path)?;
    escape_string(path)
}

/// `module.EscapeVersion`.
pub fn escape_version(version: &str) -> Result<String, String> {
    if check_elem(version, PathKind::File).is_err() || version.contains('!') {
        return Err(format!(
            "version {} invalid: disallowed version string",
            go_quote(version)
        ));
    }
    escape_string(version)
}

/// `escapeString`: uppercase letters become `!` plus the lowercase letter.
fn escape_string(value: &str) -> Result<String, String> {
    let mut have_upper = false;
    for ch in value.chars() {
        if ch == '!' || !ch.is_ascii() {
            return Err("internal error: inconsistency in EscapePath".to_owned());
        }
        if ch.is_ascii_uppercase() {
            have_upper = true;
        }
    }
    if !have_upper {
        return Ok(value.to_owned());
    }
    let mut out = String::with_capacity(value.len());
    for ch in value.chars() {
        if ch.is_ascii_uppercase() {
            out.push('!');
            out.push(ch.to_ascii_lowercase());
        } else {
            out.push(ch);
        }
    }
    Ok(out)
}

// ---------------------------------------------------------------------------
// golang.org/x/mod/semver subset
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Default)]
struct Parsed {
    major: String,
    minor: String,
    patch: String,
    prerelease: String,
    build: String,
}

/// `semver.IsValid`.
pub fn is_valid(version: &str) -> bool {
    parse(version).is_some()
}

/// `semver.Major`.
pub fn major(version: &str) -> String {
    match parse(version) {
        Some(parsed) => version[..1 + parsed.major.len()].to_owned(),
        None => String::new(),
    }
}

/// `semver.Build`.
pub fn build(version: &str) -> String {
    parse(version).map(|parsed| parsed.build).unwrap_or_default()
}

fn parse(version: &str) -> Option<Parsed> {
    let bytes = version.as_bytes();
    if bytes.is_empty() || bytes[0] != b'v' {
        return None;
    }
    let mut parsed = Parsed::default();
    let (major, rest) = parse_int(&version[1..])?;
    parsed.major = major;
    let mut rest = rest;
    if rest.is_empty() {
        parsed.minor = "0".to_owned();
        parsed.patch = "0".to_owned();
        return Some(parsed);
    }
    if !rest.starts_with('.') {
        return None;
    }
    let (minor, after_minor) = parse_int(&rest[1..])?;
    parsed.minor = minor;
    rest = after_minor;
    if rest.is_empty() {
        parsed.patch = "0".to_owned();
        return Some(parsed);
    }
    if !rest.starts_with('.') {
        return None;
    }
    let (patch, after_patch) = parse_int(&rest[1..])?;
    parsed.patch = patch;
    rest = after_patch;
    if rest.starts_with('-') {
        let (prerelease, after_pre) = parse_prerelease(rest)?;
        parsed.prerelease = prerelease;
        rest = after_pre;
    }
    if rest.starts_with('+') {
        let (build, after_build) = parse_build(rest)?;
        parsed.build = build;
        rest = after_build;
    }
    if !rest.is_empty() {
        return None;
    }
    Some(parsed)
}

fn parse_int(value: &str) -> Option<(String, &str)> {
    let bytes = value.as_bytes();
    if bytes.is_empty() || !bytes[0].is_ascii_digit() {
        return None;
    }
    let mut i = 1;
    while i < bytes.len() && bytes[i].is_ascii_digit() {
        i += 1;
    }
    if bytes[0] == b'0' && i != 1 {
        return None;
    }
    Some((value[..i].to_owned(), &value[i..]))
}

fn parse_prerelease(value: &str) -> Option<(String, &str)> {
    let bytes = value.as_bytes();
    if bytes.is_empty() || bytes[0] != b'-' {
        return None;
    }
    let mut i = 1;
    let mut start = 1;
    while i < bytes.len() && bytes[i] != b'+' {
        if !is_ident_char(bytes[i]) && bytes[i] != b'.' {
            return None;
        }
        if bytes[i] == b'.' {
            if start == i || is_bad_num(&value[start..i]) {
                return None;
            }
            start = i + 1;
        }
        i += 1;
    }
    if start == i || is_bad_num(&value[start..i]) {
        return None;
    }
    Some((value[..i].to_owned(), &value[i..]))
}

fn parse_build(value: &str) -> Option<(String, &str)> {
    let bytes = value.as_bytes();
    if bytes.is_empty() || bytes[0] != b'+' {
        return None;
    }
    let mut i = 1;
    let mut start = 1;
    while i < bytes.len() {
        if !is_ident_char(bytes[i]) && bytes[i] != b'.' {
            return None;
        }
        if bytes[i] == b'.' {
            if start == i {
                return None;
            }
            start = i + 1;
        }
        i += 1;
    }
    if start == i {
        return None;
    }
    Some((value[..i].to_owned(), &value[i..]))
}

fn is_ident_char(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || byte == b'-'
}

fn is_bad_num(value: &str) -> bool {
    let bytes = value.as_bytes();
    let mut i = 0;
    while i < bytes.len() && bytes[i].is_ascii_digit() {
        i += 1;
    }
    i == bytes.len() && i > 1 && bytes[0] == b'0'
}

// ---------------------------------------------------------------------------
// Go formatting helpers
// ---------------------------------------------------------------------------

/// `fmt.Sprintf("%q", s)` for the ASCII strings these checks produce.
fn go_quote(value: &str) -> String {
    let mut out = String::with_capacity(value.len() + 2);
    out.push('"');
    for ch in value.chars() {
        match ch {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            ch if (ch as u32) < 0x20 => out.push_str(&format!("\\x{:02x}", ch as u32)),
            ch => out.push(ch),
        }
    }
    out.push('"');
    out
}

/// `fmt.Sprintf("%q", r)` for single runes (`'x'` / `'\u{...}'`).
fn go_quote_char(ch: char) -> String {
    match ch {
        '\'' => "'\\''".to_owned(),
        '\\' => "'\\\\'".to_owned(),
        '\n' => "'\\n'".to_owned(),
        '\r' => "'\\r'".to_owned(),
        '\t' => "'\\t'".to_owned(),
        ch if (ch as u32) < 0x20 || (ch as u32) == 0x7f => {
            format!("'\\x{:02x}'", ch as u32)
        }
        ch => format!("'{ch}'"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn valid_paths_and_versions() {
        assert!(check("github.com/gorilla/mux", "v1.8.0").is_ok());
        assert!(check("gopkg.in/yaml.v2", "v2.4.0").is_ok());
        assert!(check("example.com/mod/v2", "v2.0.0").is_ok());
        assert!(check_path("example.com/mod").is_ok());
    }

    #[test]
    fn error_texts_match_the_baseline() {
        assert_eq!(
            check("example.com/mod", "1.0.0").unwrap_err(),
            "example.com/mod@1.0.0: invalid version: not a semantic version"
        );
        assert_eq!(
            check("example.com/mod/v2", "v1.0.0").unwrap_err(),
            "example.com/mod/v2@v1.0.0: invalid version: should be v2, not v1"
        );
        assert_eq!(
            check_path("nodot/mod").unwrap_err(),
            "malformed module path \"nodot/mod\": missing dot in first path element"
        );
        assert_eq!(
            check_path("example.com/pkg/v1").unwrap_err(),
            "malformed module path \"example.com/pkg/v1\": invalid version"
        );
    }

    #[test]
    fn escaping_matches_the_baseline() {
        assert_eq!(
            escape_path("github.com/Azure/azure-sdk-for-go").unwrap(),
            "github.com/!azure/azure-sdk-for-go"
        );
        assert_eq!(escape_version("v1.0.0").unwrap(), "v1.0.0");
        assert_eq!(
            escape_version("bad!version").unwrap_err(),
            "version \"bad!version\" invalid: disallowed version string"
        );
    }

    #[test]
    fn semver_subset() {
        assert!(is_valid("v1"));
        assert!(is_valid("v1.2"));
        assert!(is_valid("v1.2.3-pre+build"));
        assert!(!is_valid("1.2.3"));
        assert!(!is_valid("v1.2.3-01"));
        assert_eq!(major("v2.1.0"), "v2");
        assert_eq!(build("v2.1.0+meta"), "+meta");
    }
}
