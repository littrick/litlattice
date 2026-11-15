#![forbid(unsafe_code)]

//! `lib/codeintel/languages.GetLanguages` (`git_tree_entry.go:297-326`).
//!
//! The data tables in [`crate::enry_data`] are vendored from the pinned
//! `go-enry v2.8.8` data package (Linguist commit `4ac734c1`) that the Go
//! baseline imports, so filename, extension, interpreter and alias decisions
//! are exact. The repo-specific override maps below are copied verbatim from
//! `lib/codeintel/languages/extensions.go`.
//!
//! Deliberate approximation: `enry.GetLanguagesByContent` (rule/regex
//! heuristics) and `GetLanguagesByClassifier` (a trained model) are not
//! ported and contribute no candidates. Only ambiguous extensions whose answer
//! depends on file content can therefore differ; the extension-level candidate
//! list is returned instead. Modeline and shebang are ported faithfully.

use crate::enry_data::{
    LANGUAGES_BY_EXTENSION, LANGUAGES_BY_FILENAME, LANGUAGES_BY_INTERPRETER, LANGUAGE_BY_ALIAS,
};

/// `unsupportedByEnryExtensionToNameMap` (`extensions.go`).
const UNSUPPORTED_EXTENSIONS: &[(&str, &str)] = &[
    (".apex", "Apex"),
    (".apxt", "Apex"),
    (".apxc", "Apex"),
    (".cls", "Apex"),
    (".trigger", "Apex"),
    (".pkl", "Pkl"),
    (".magik", "Magik"),
];

/// `overrideAmbiguousExtensionsMap` (`extensions.go`).
const OVERRIDE_EXTENSIONS: &[(&str, &str)] = &[
    (".cs", "C#"),
    (".fs", "F#"),
    (".json", "JSON"),
    (".md", "Markdown"),
    (".rs", "Rust"),
    (".tsx", "TSX"),
    (".ts", "TypeScript"),
    (".txt", "Text"),
    (".yaml", "YAML"),
    (".yml", "YAML"),
];

/// `commonBinaryFileExtensionsList` (`extensions.go`).
const BINARY_EXTENSIONS: &[&str] = &[
    "3dm", "3ds", "3g2", "3gp", "7z", "a", "aac", "adp", "ai", "aif", "aiff", "alz", "ape", "apk",
    "appimage", "ar", "arj", "asf", "au", "avi", "bak", "baml", "bh", "bin", "bk", "bmp", "btif",
    "bz2", "bzip2", "cab", "caf", "cgm", "class", "cmx", "cpio", "cr2", "cur", "dat", "dcm", "deb",
    "dex", "djvu", "dll", "dmg", "dng", "doc", "docm", "docx", "dot", "dotm", "dra", "DS_Store",
    "dsk", "dts", "dtshd", "dvb", "dwg", "dxf", "ecelp4800", "ecelp7470", "ecelp9600", "egg",
    "eol", "eot", "epub", "exe", "f4v", "fbs", "fla", "flac", "fh", "flatpak", "fli", "flv",
    "fpx", "fst", "fvt", "g3", "gh", "gif", "graffle", "gz", "gzip", "h261", "h263", "h264",
    "icns", "ico", "ief", "img", "ipa", "iso", "jar", "jpeg", "jpg", "jpgv", "jpm", "jxr", "key",
    "ktx", "lha", "lib", "lvp", "lz", "lzh", "lzma", "lzo", "m3u", "m4a", "m4v", "mar", "mdi",
    "mht", "mid", "midi", "mj2", "mka", "mkv", "mmr", "mng", "mobi", "mov", "movie", "mp3",
    "mp4", "mp4a", "mpeg", "mpg", "mpga", "mxu", "nef", "npx", "numbers", "nupkg", "o", "odp",
    "ods", "odt", "oga", "ogg", "ogv", "otf", "ott", "pages", "pbm", "pcx", "pdb", "pdf", "pea",
    "pgm", "pic", "png", "pnm", "pot", "potm", "potx", "ppa", "ppam", "ppm", "pps", "ppsm",
    "ppsx", "ppt", "pptm", "pptx", "psd", "pya", "pyc", "pyo", "pyv", "qt", "rar", "ras", "raw",
    "resources", "rgb", "rip", "rlc", "rmf", "rmvb", "rpm", "rtf", "rz", "s3m", "s7z", "scpt", "sgi",
    "shar", "snap", "sil", "sketch", "slk", "smv", "snk", "so", "stl", "suo", "sub", "swf", "tar",
    "tbz", "tbz2", "tga", "tgz", "thmx", "tif", "tiff", "tlz", "ttc", "ttf", "txz", "udf", "uvh",
    "uvi", "uvm", "uvp", "uvs", "uvu", "viv", "vob", "war", "wav", "wax", "wbmp", "wdp", "weba",
    "webm", "webp", "whl", "wim", "wm", "wma", "wmv", "wmx", "woff", "woff2", "wrm", "wvx",
    "xbm", "xif", "xla", "xlam", "xls", "xlsb", "xlsm", "xlsx", "xlt", "xltm", "xltx", "xm",
    "xmind", "xpi", "xpm", "xwd", "xz", "z", "zip", "zipx",
];

/// `getLanguagesByExtension`: the repo's overrides plus enry's table.
/// Returns `(candidates, is_likely_binary)`.
fn languages_by_extension(path: &str) -> (Vec<String>, bool) {
    let ext = filepath_ext(path);
    if ext.is_empty() {
        return (Vec::new(), false);
    }
    if let Some(language) = lookup(UNSUPPORTED_EXTENSIONS, ext) {
        return (vec![language.to_owned()], false);
    }
    if BINARY_EXTENSIONS.iter().any(|candidate| *candidate == &ext[1..]) {
        return (Vec::new(), true);
    }
    if let Some(language) = lookup(OVERRIDE_EXTENSIONS, ext) {
        return (vec![language.to_owned()], false);
    }
    (enry_languages_by_extension(path), false)
}

/// `enry.GetLanguagesByExtension`: lowercase the whole path and try every
/// dot-suffix, longest first.
fn enry_languages_by_extension(path: &str) -> Vec<String> {
    let lower = path.to_lowercase();
    if !lower.contains('.') {
        return Vec::new();
    }
    for (index, byte) in lower.bytes().enumerate() {
        if byte != b'.' {
            continue;
        }
        let suffix = &lower[index..];
        if let Ok(found) = LANGUAGES_BY_EXTENSION
            .binary_search_by(|(key, _)| key.cmp(&suffix))
        {
            return LANGUAGES_BY_EXTENSION[found]
                .1
                .iter()
                .map(|language| (*language).to_owned())
                .collect();
        }
    }
    Vec::new()
}

fn filepath_ext(path: &str) -> &str {
    let base_start = path.rfind('/').map_or(0, |index| index + 1);
    let base = &path[base_start..];
    match base.rfind('.') {
        Some(index) => &base[index..],
        None => "",
    }
}

fn filepath_base(path: &str) -> &str {
    match path.rfind('/') {
        Some(index) => &path[index + 1..],
        None => path,
    }
}

fn lookup<'a>(table: &'a [(&'a str, &'a str)], key: &str) -> Option<&'a str> {
    table
        .binary_search_by(|(candidate, _)| (*candidate).cmp(key))
        .ok()
        .map(|index| table[index].1)
}

fn lookup_list(table: &[(&str, &[&str])], key: &str) -> Vec<String> {
    match table.binary_search_by(|(candidate, _)| (*candidate).cmp(key)) {
        Ok(index) => table[index]
            .1
            .iter()
            .map(|language| (*language).to_owned())
            .collect(),
        Err(_) => Vec::new(),
    }
}

/// `enry.IsBinary`: a NUL byte in the first 8000 bytes.
pub fn is_binary(content: &[u8]) -> bool {
    let sniff = &content[..content.len().min(8000)];
    sniff.contains(&0)
}

/// The part of `GetLanguages` that needs only the path. `Decided` returns
/// without reading content; `Binary` returns an empty list without reading
/// content; `NeedContent` carries the candidate set to narrow.
pub enum Preliminary {
    Decided(Vec<String>),
    Binary,
    NeedContent(Vec<String>),
}

pub fn preliminary(path: &str) -> Preliminary {
    let filename = lookup_list(LANGUAGES_BY_FILENAME, filepath_base(path));
    if filename.len() == 1 {
        return Preliminary::Decided(filename);
    }
    let (mut candidates, likely_binary) = languages_by_extension(path);
    if likely_binary {
        return Preliminary::Binary;
    }
    match candidates.len() {
        0 => Preliminary::NeedContent(filename),
        1 => Preliminary::Decided(candidates),
        _ => {
            // `langs = newLangs` (the extension candidates win over a
            // multi-valued filename match).
            candidates.shrink_to_fit();
            Preliminary::NeedContent(candidates)
        }
    }
}

/// Whether resolving `path` requires reading the file contents.
pub fn needs_content(path: &str) -> bool {
    matches!(preliminary(path), Preliminary::NeedContent(_))
}

/// `languages.GetLanguages(path, getContent)` with the content already read.
///
/// * `content == None` is Go's `getContent == nil` (filename/extension only).
/// * An empty or binary content short-circuits to the candidate set / empty.
pub fn get_languages(path: &str, content: Option<&[u8]>) -> Vec<String> {
    let (mut languages, content) = match preliminary(path) {
        Preliminary::Decided(languages) => return languages,
        Preliminary::Binary => return Vec::new(),
        Preliminary::NeedContent(candidates) => (candidates, content),
    };
    let Some(content) = content else {
        return languages;
    };
    if content.is_empty() || is_binary(content) {
        return if content.is_empty() {
            languages
        } else {
            Vec::new()
        };
    }

    // modeline, then shebang; content heuristics and classifier are no-ops.
    if let Some(candidates) = modeline(content) {
        match candidates.len() {
            0 => {}
            1 => return candidates,
            _ => languages = candidates,
        }
    }
    let candidates = shebang(content);
    match candidates.len() {
        0 => {}
        1 => return candidates,
        _ => languages = candidates,
    }
    // `GetLanguagesByContent` and `GetLanguagesByClassifier`: not ported.
    languages
}

fn normalize_alias(alias: &str) -> String {
    let first = alias.split(',').next().unwrap_or(alias);
    first.replace(' ', "_").to_lowercase()
}

fn language_by_alias(alias: &str) -> Option<String> {
    let key = normalize_alias(alias);
    LANGUAGE_BY_ALIAS
        .binary_search_by(|(candidate, _)| (*candidate).cmp(key.as_str()))
        .ok()
        .map(|index| LANGUAGE_BY_ALIAS[index].1.to_owned())
}

fn regexes() -> &'static ModelineRegexes {
    static REGEXES: std::sync::OnceLock<ModelineRegexes> = std::sync::OnceLock::new();
    REGEXES.get_or_init(|| ModelineRegexes {
        emacs: regex::bytes::Regex::new(r"(?m)^.*-\*-[ \t\n\f\r]*(.+?)[ \t\n\f\r]*-\*-.*$")
            .expect("emacs"),
        emacs_lang: regex::bytes::Regex::new(
            r".*(?i:mode)[ \t\n\f\r]*:[ \t\n\f\r]*([^ \t\n\f\r;]+)[ \t\n\f\r]*;*.*",
        )
        .expect("emacs lang"),
        vim: regex::bytes::Regex::new(
            r"(?m)(?:(?:[ \t\n\f\r]|^)vi(?:m[<=>]?[0-9]+|m)?|[\t\x20]*ex)[ \t\n\f\r]*:[ \t\n\f\r]*(.*)$",
        )
        .expect("vim"),
        vim_lang: regex::bytes::Regex::new(
            r"(?i:filetype|ft|syntax)[ \t\n\f\r]*=([0-9A-Za-z_]+)(?:[ \t\n\f\r]|:|$)",
        )
        .expect("vim lang"),
        exec_hack: regex::bytes::Regex::new(r"exec ([0-9A-Za-z_]+).+\$0.+\$@").expect("exec"),
        python_version: regex::bytes::Regex::new(r"python[0-9]\.[0-9]+").expect("python"),
        env_opt: regex::bytes::Regex::new(r"-[i0uCSv]*|--[^ \t\n\f\r]+").expect("env opt"),
        env_var: regex::bytes::Regex::new(
            r"[^ \t\n\f\r]+=[^ \t\n\f\r]+",
        )
        .expect("env var"),
    })
}

struct ModelineRegexes {
    emacs: regex::bytes::Regex,
    emacs_lang: regex::bytes::Regex,
    vim: regex::bytes::Regex,
    vim_lang: regex::bytes::Regex,
    exec_hack: regex::bytes::Regex,
    python_version: regex::bytes::Regex,
    env_opt: regex::bytes::Regex,
    env_var: regex::bytes::Regex,
}

/// `getHeaderAndFooter` (5-line header + 5-line footer when the file has at
/// least 10 newlines).
fn header_and_footer(content: &[u8]) -> Vec<u8> {
    const SEARCH_SCOPE: usize = 5;
    let newlines = content.iter().filter(|&&byte| byte == b'\n').count();
    if content.is_empty() || newlines < 2 * SEARCH_SCOPE {
        return content.to_vec();
    }
    let mut header = 0usize;
    for _ in 0..SEARCH_SCOPE {
        match content[header..].iter().position(|&byte| byte == b'\n') {
            Some(pos) => header += pos + 1,
            None => return content.to_vec(),
        }
    }
    header = header.saturating_sub(1);
    let mut footer = content.len();
    for _ in 0..SEARCH_SCOPE {
        match content[..footer].iter().rposition(|&byte| byte == b'\n') {
            Some(pos) => footer = pos + 1,
            None => return content.to_vec(),
        }
    }
    let mut out = Vec::with_capacity(header + (content.len() - footer));
    out.extend_from_slice(&content[..header]);
    out.extend_from_slice(&content[footer..]);
    out
}

fn capture_to_string(matched: &[u8]) -> String {
    String::from_utf8_lossy(matched).into_owned()
}

/// `GetLanguagesByModeline`: Emacs first, then Vim; last match wins.
fn modeline(content: &[u8]) -> Option<Vec<String>> {
    let regexes = regexes();
    let header_footer = header_and_footer(content);

    // Emacs.
    let matches: Vec<_> = regexes.emacs.captures_iter(&header_footer).collect();
    if let Some(last) = matches.last() {
        let last_line = last.get(1).map(|m| m.as_bytes()).unwrap_or_default();
        let alias = match regexes.emacs_lang.captures(last_line) {
            Some(captures) => capture_to_string(captures.get(1).unwrap().as_bytes()),
            None => String::from_utf8_lossy(last_line).into_owned(),
        };
        if let Some(language) = language_by_alias(&alias) {
            return Some(vec![language]);
        }
        return None;
    }

    // Vim.
    let matches: Vec<_> = regexes.vim.captures_iter(&header_footer).collect();
    let last = matches.last()?;
    let last_line = last.get(1).map(|m| m.as_bytes()).unwrap_or_default();
    let aliases: Vec<String> = regexes
        .vim_lang
        .captures_iter(last_line)
        .filter_map(|captures| {
            captures
                .get(1)
                .map(|m| capture_to_string(m.as_bytes()))
        })
        .collect();
    if aliases.is_empty() {
        return None;
    }
    let alias = aliases[0].clone();
    if aliases.iter().any(|other| *other != alias) {
        return None;
    }
    language_by_alias(&alias).map(|language| vec![language])
}

/// `getFirstLine`: up to the first newline (whole content when absent).
fn first_line(content: &[u8]) -> &[u8] {
    match content.iter().position(|&byte| byte == b'\n') {
        Some(index) => &content[..index],
        None => content,
    }
}

/// `lookForMultilineExec`.
fn look_for_multiline_exec(content: &[u8]) -> String {
    let regexes = regexes();
    let mut interpreter = "sh".to_owned();
    let text = String::from_utf8_lossy(content);
    for line in text.split('\n').take(5) {
        if let Some(captures) = regexes.exec_hack.captures(line.as_bytes()) {
            interpreter = capture_to_string(captures.get(1).unwrap().as_bytes());
            break;
        }
    }
    interpreter
}

/// `enry.getInterpreter`.
fn interpreter(content: &[u8]) -> String {
    let regexes = regexes();
    let line = first_line(content);
    if !line.starts_with(b"#!") {
        return String::new();
    }
    let trimmed = &line[2..];
    let trimmed = trimmed
        .iter()
        .position(|byte| !matches!(*byte, b' ' | b'\t' | b'\n' | b'\r'))
        .map_or(&trimmed[trimmed.len()..], |index| &trimmed[index..]);
    let fields: Vec<&[u8]> = trimmed
        .split(|byte| matches!(*byte, b' ' | b'\t' | b'\n' | b'\r'))
        .filter(|field| !field.is_empty())
        .collect();
    if fields.is_empty() {
        return String::new();
    }
    let base = |value: &[u8]| -> String {
        let value = String::from_utf8_lossy(value);
        value
            .rsplit('/')
            .next()
            .unwrap_or_default()
            .to_owned()
    };
    let mut interpreter = base(fields[0]);
    if interpreter == "env" {
        if fields.len() == 1 {
            return String::new();
        }
        let mut fields = fields.clone();
        while fields.len() > 2 {
            let second = fields[1];
            if regexes.env_opt.is_match(second) || regexes.env_var.is_match(second) {
                fields.remove(1);
                continue;
            }
            break;
        }
        interpreter = base(fields[1]);
    }
    if interpreter == "sh" {
        interpreter = look_for_multiline_exec(content);
    }
    if let Some(found) = regexes.python_version.find(interpreter.as_bytes()) {
        interpreter = capture_to_string(found.as_bytes());
        if let Some(dot) = interpreter.find('.') {
            interpreter.truncate(dot);
        }
    }
    if interpreter == "osascript" && line.windows(2).any(|window| window == b"-l") {
        interpreter.clear();
    }
    interpreter
}

/// `getLanguagesByShebang` (the repo's Pod workaround included).
fn shebang(content: &[u8]) -> Vec<String> {
    let mut languages = lookup_list(LANGUAGES_BY_INTERPRETER, &interpreter(content));
    if languages.len() == 2 {
        if languages == ["Perl", "Pod"] {
            return vec!["Perl".to_owned()];
        }
        if languages == ["Pod 6", "Raku"] {
            return vec!["Raku".to_owned()];
        }
    }
    languages.shrink_to_fit();
    languages
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extension_and_filename_decisions() {
        assert_eq!(get_languages("src/main.go", None), vec!["Go"]);
        assert_eq!(get_languages("Dockerfile", None), vec!["Dockerfile"]);
        assert_eq!(get_languages("x.rs", None), vec!["Rust"]);
        assert_eq!(get_languages("x.cls", None), vec!["Apex"]);
        assert_eq!(get_languages("x.png", Some(&[0, 1, 2])), Vec::<String>::new());
        assert!(get_languages("x.zzz", None).is_empty());
    }

    #[test]
    fn modelines_and_shebangs() {
        assert_eq!(
            get_languages("script", Some(b"# -*- mode: ruby -*-\nputs 1\n")),
            vec!["Ruby"]
        );
        assert_eq!(
            get_languages("script", Some(b"# vim: ft=python\nprint(1)\n")),
            vec!["Python"]
        );
        assert_eq!(
            get_languages("tool", Some(b"#!/usr/bin/env python3\nprint(1)\n")),
            vec!["Python"]
        );
    }
}
