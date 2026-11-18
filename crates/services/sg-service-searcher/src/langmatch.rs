#![forbid(unsafe_code)]

//! Language detection for `lang:` filters, mirroring Sourcegraph's
//! `lib/codeintel/languages` wrapper around go-enry v2.8.8.
//!
//! The data tables in [`enry_data`] are generated from the vendored go-enry
//! module and the Sourcegraph overrides by `dev/rs/scripts/searcher/gen_enry_data.py`.
//!
//! Known gaps (documented in `dev/rs/docs/modules/04_searcher.md`): the naive
//! Bayes classifier is not ported, so ambiguous extensions whose content
//! heuristics do not match fall back to enry's candidate list (the baseline
//! would rank those candidates). Emacs/Vim modelines, shebangs and content
//! heuristics are ported.

pub mod enry_data;

use std::collections::HashMap;
use std::io;
use std::sync::OnceLock;

use regex::bytes::Regex;

use crate::protocol::PatternInfo;

/// go-enry's `binSniffLen`.
const BIN_SNIFF_LEN: usize = 8000;

/// Baseline `langMatcher`.
#[derive(Debug, Clone)]
pub enum LangMatcher {
    All,
    Enry(EnryLangMatcher),
}

/// Baseline `enryLangMatcher`.
#[derive(Debug, Clone)]
pub struct EnryLangMatcher {
    pub include_langs: Vec<String>,
    pub exclude_langs: Vec<String>,
    pub only_excludes: bool,
}

impl LangMatcher {
    /// Baseline `langMatcher.Matches`: does the file pass the language
    /// filters, and which language was detected (`""` when none)?
    ///
    /// `get_content` is only called when filename/extension detection leaves
    /// the language ambiguous, mirroring the lazy callback in the baseline.
    pub fn matches<F>(&self, path: &str, get_content: F) -> (bool, String)
    where
        F: FnOnce() -> io::Result<Vec<u8>>,
    {
        let matcher = match self {
            Self::All => return (true, String::new()),
            Self::Enry(matcher) => matcher,
        };

        let langs = match get_languages(path, Some(get_content)) {
            Ok(langs) => langs,
            Err(_) => return (false, String::new()),
        };
        if langs.is_empty() {
            // It's fine if a file has no detected language, as long as there
            // are no include filters.
            return (matcher.only_excludes, String::new());
        }

        // Choose the most likely language. (The baseline compares against
        // every include filter, which for multiple filters only matches when
        // they are all equal; replicated verbatim.)
        let lang = langs[0].clone();
        for include in &matcher.include_langs {
            if lang != *include {
                return (false, String::new());
            }
        }
        let matched = !matcher.exclude_langs.contains(&lang);
        (matched, lang)
    }
}

/// Baseline `toLangMatcher`.
pub fn to_lang_matcher(pattern_info: &PatternInfo) -> LangMatcher {
    if pattern_info.include_langs.is_empty() && pattern_info.exclude_langs.is_empty() {
        return LangMatcher::All;
    }
    LangMatcher::Enry(EnryLangMatcher {
        include_langs: pattern_info.include_langs.clone(),
        exclude_langs: pattern_info.exclude_langs.clone(),
        only_excludes: pattern_info.include_langs.is_empty(),
    })
}

/// Baseline `languages.GetLanguages`: filename, then extension, then
/// (optionally) content strategies.
pub fn get_languages<F>(path: &str, get_content: Option<F>) -> io::Result<Vec<String>>
where
    F: FnOnce() -> io::Result<Vec<u8>>,
{
    let mut langs: Vec<String> = languages_by_filename(path)
        .iter()
        .map(|lang| (*lang).to_owned())
        .collect();
    if langs.len() == 1 {
        return Ok(langs);
    }

    let (new_langs, is_likely_binary_file) = languages_by_extension(path);
    if is_likely_binary_file {
        return Ok(Vec::new());
    }
    match new_langs.len() {
        0 => {}
        1 => return Ok(new_langs.iter().map(|lang| (*lang).to_owned()).collect()),
        _ => {
            langs = new_langs.iter().map(|lang| (*lang).to_owned()).collect();
        }
    }

    let Some(get_content) = get_content else {
        return Ok(langs);
    };
    let content = get_content()?;
    if content.is_empty() {
        return Ok(langs);
    }
    if is_binary(&content) {
        return Ok(Vec::new());
    }

    // The baseline additionally runs enry's naive Bayes classifier here. It
    // only ranks the remaining candidates, so it is intentionally omitted.
    if let Some(languages) = languages_by_modeline(&content) {
        if languages.len() == 1 {
            return Ok(languages);
        }
        langs = languages;
    }
    if let Some(languages) = languages_by_shebang(&content) {
        if languages.len() == 1 {
            return Ok(languages);
        }
        langs = languages;
    }
    if let Some(languages) = languages_by_content(path, &content) {
        if languages.len() == 1 {
            return Ok(languages);
        }
        langs = languages;
    }

    Ok(langs)
}

/// enry `IsBinary`: a NUL byte in the first 8000 bytes.
pub fn is_binary(content: &[u8]) -> bool {
    content[..content.len().min(BIN_SNIFF_LEN)].contains(&0)
}

// ---------------------------------------------------------------------------
// Filename / extension detection
// ---------------------------------------------------------------------------

/// enry `GetLanguagesByFilename`.
fn languages_by_filename(path: &str) -> &'static [&'static str] {
    if path.is_empty() {
        return &[];
    }
    let base = path_base(path);
    lookup_slice_map(
        slice_map_index(
            &LANGUAGES_BY_FILENAME_INDEX,
            enry_data::LANGUAGES_BY_FILENAME,
        ),
        base,
    )
    .unwrap_or(&[])
}

/// Sourcegraph `getLanguagesByExtension`.
fn languages_by_extension(path: &str) -> (&'static [&'static str], bool) {
    let Some(extension) = filepath_ext(path) else {
        return (&[], false);
    };
    if let Some(lang) = lookup_str_map(
        str_map_index(
            &UNSUPPORTED_EXTENSIONS_INDEX,
            enry_data::UNSUPPORTED_EXTENSIONS,
        ),
        extension,
    ) {
        return (lang_slice(lang), false);
    }
    // The binary list stores extensions without the leading dot.
    if extension.len() > 1 && lookup_set(common_binary_extensions_set(), &extension[1..]) {
        return (&[], true);
    }
    if let Some(lang) = lookup_str_map(
        str_map_index(
            &OVERRIDE_AMBIGUOUS_EXTENSIONS_INDEX,
            enry_data::OVERRIDE_AMBIGUOUS_EXTENSIONS,
        ),
        extension,
    ) {
        return (lang_slice(lang), false);
    }
    (enry_languages_by_extension(path), false)
}

/// enry `GetLanguagesByExtension`: lower-case the path and try each dot
/// suffix left to right.
fn enry_languages_by_extension(path: &str) -> &'static [&'static str] {
    if !path.contains('.') {
        return &[];
    }
    let lowered = path.to_lowercase();
    let table = slice_map_index(
        &LANGUAGES_BY_EXTENSION_INDEX,
        enry_data::LANGUAGES_BY_EXTENSION,
    );
    for (index, _) in lowered.match_indices('.') {
        if let Some(languages) = lookup_slice_map(table, &lowered[index..]) {
            return languages;
        }
    }
    &[]
}

/// Go `filepath.Ext`: the suffix beginning at the final dot of the final
/// path element, if any.
fn filepath_ext(path: &str) -> Option<&str> {
    let base_start = path.rfind('/').map_or(0, |index| index + 1);
    let base = &path[base_start..];
    let dot = base.rfind('.')?;
    Some(&base[dot..])
}

/// Go `path.Base` (sufficient for repository paths).
fn path_base(path: &str) -> &str {
    if path.is_empty() {
        return ".";
    }
    let trimmed = path.trim_end_matches('/');
    if trimmed.is_empty() {
        return "/";
    }
    trimmed.rsplit('/').next().unwrap_or(trimmed)
}

// ---------------------------------------------------------------------------
// Modelines
// ---------------------------------------------------------------------------

/// enry `GetLanguagesByModeline`.
fn languages_by_modeline(content: &[u8]) -> Option<Vec<String>> {
    let head_foot = get_header_and_footer(content);
    let emacs = languages_by_emacs_modeline(&head_foot);
    if emacs.is_some() {
        return emacs;
    }
    languages_by_vim_modeline(&head_foot)
}

/// enry `getHeaderAndFooter`: the first and last five lines when the file is
/// long enough to make scanning the whole thing wasteful.
fn get_header_and_footer(content: &[u8]) -> Vec<u8> {
    const SEARCH_SCOPE: usize = 5;
    if content.is_empty()
        || content.iter().filter(|byte| **byte == b'\n').count() < 2 * SEARCH_SCOPE
    {
        return content.to_vec();
    }
    let header = head_scope(content, SEARCH_SCOPE);
    let footer = foot_scope(content, SEARCH_SCOPE);
    let mut out = Vec::with_capacity(header + content.len() - footer);
    out.extend_from_slice(&content[..header]);
    out.extend_from_slice(&content[footer..]);
    out
}

/// enry `headScope`.
fn head_scope(content: &[u8], scope: usize) -> usize {
    let mut remaining = content;
    let mut index = 0usize;
    for _ in 0..scope {
        let eol = remaining
            .iter()
            .position(|byte| *byte == b'\n')
            .unwrap_or(remaining.len());
        remaining = &remaining[(eol + 1).min(remaining.len())..];
        index += eol;
    }
    index + scope - 1
}

/// enry `footScope`.
fn foot_scope(content: &[u8], scope: usize) -> usize {
    let mut remaining = content;
    let mut index = 0usize;
    for _ in 0..scope {
        index = remaining
            .iter()
            .rposition(|byte| *byte == b'\n')
            .unwrap_or(0);
        remaining = &remaining[..index];
    }
    index + 1
}

/// enry `GetLanguagesByEmacsModeline`.
fn languages_by_emacs_modeline(content: &[u8]) -> Option<Vec<String>> {
    static MODELINE: OnceLock<Regex> = OnceLock::new();
    static LANG: OnceLock<Regex> = OnceLock::new();
    let modeline = MODELINE
        .get_or_init(|| Regex::new(r"(?m).*-\*-\s*(.+?)\s*-\*-.*$").expect("emacs modeline"));
    let lang_re = LANG
        .get_or_init(|| Regex::new(r"(?m).*(?i:mode)\s*:\s*([^\s;]+)\s*;*.*").expect("emacs lang"));

    let last = modeline.captures_iter(content).last()?;
    let matched = last.get(1)?;
    let line = matched.as_bytes();

    let alias = match lang_re.captures(line) {
        Some(captures) => captures.get(1).map(|group| group.as_bytes().to_vec()),
        None => Some(line.to_vec()),
    }?;
    let language = language_by_alias(&String::from_utf8_lossy(&alias))?;
    Some(vec![language.to_owned()])
}

/// enry `GetLanguagesByVimModeline`.
fn languages_by_vim_modeline(content: &[u8]) -> Option<Vec<String>> {
    static MODELINE: OnceLock<Regex> = OnceLock::new();
    static LANG: OnceLock<Regex> = OnceLock::new();
    let modeline = MODELINE.get_or_init(|| {
        Regex::new(r"(?m)(?:(?:\s|^)vi(?:m[<=>]?[0-9]+|m)?|[\t ]*ex)\s*:\s*(.*)$")
            .expect("vim modeline")
    });
    let lang_re = LANG.get_or_init(|| {
        Regex::new(r"(?i)(?:filetype|ft|syntax)\s*=([0-9A-Za-z_]+)(?:\s|:|$)").expect("vim lang")
    });

    let last = modeline.captures_iter(content).last()?;
    let line = last.get(1)?.as_bytes();

    let aliases: Vec<&[u8]> = lang_re
        .captures_iter(line)
        .filter_map(|captures| captures.get(1).map(|group| group.as_bytes()))
        .collect();
    let first = *aliases.first()?;
    // Mixed `filetype`/`ft`/`syntax` values are ambiguous.
    if aliases.iter().any(|alias| *alias != first) {
        return None;
    }

    let language = language_by_alias(&String::from_utf8_lossy(first))?;
    Some(vec![language.to_owned()])
}

// ---------------------------------------------------------------------------
// Shebangs
// ---------------------------------------------------------------------------

/// Sourcegraph `getLanguagesByShebang`.
fn languages_by_shebang(content: &[u8]) -> Option<Vec<String>> {
    let interpreter = get_interpreter(content);
    let languages = lookup_slice_map(
        slice_map_index(
            &LANGUAGES_BY_INTERPRETER_INDEX,
            enry_data::LANGUAGES_BY_INTERPRETER,
        ),
        &interpreter,
    )?;
    // go-enry also considers non-programming languages for shebangs; the
    // Sourcegraph wrapper drops the Pod aliases.
    let mut languages: Vec<String> = languages.iter().map(|lang| (*lang).to_owned()).collect();
    if languages == ["Perl", "Pod"] {
        languages = vec!["Perl".to_owned()];
    } else if languages == ["Pod 6", "Raku"] {
        languages = vec!["Raku".to_owned()];
    }
    Some(languages)
}

/// enry `getInterpreter`.
fn get_interpreter(content: &[u8]) -> String {
    let line = first_line(content);
    if !line.starts_with(b"#!") {
        return String::new();
    }
    let line = trim_ascii_space(&line[2..]);
    let fields: Vec<&[u8]> = line
        .split(|byte| byte.is_ascii_whitespace())
        .filter(|field| !field.is_empty())
        .collect();
    let Some(first) = fields.first() else {
        return String::new();
    };

    let mut interpreter = path_base(&String::from_utf8_lossy(first)).to_owned();

    // `#!/usr/bin/env [...]
    if interpreter == "env" {
        if fields.len() == 1 {
            return String::new();
        }
        let mut fields = fields;
        while fields.len() > 2 {
            let candidate = String::from_utf8_lossy(fields[1]);
            if env_opt_args().is_match(candidate.as_bytes())
                || env_var_args().is_match(candidate.as_bytes())
            {
                fields.remove(1);
                continue;
            }
            break;
        }
        interpreter = path_base(&String::from_utf8_lossy(fields[1])).to_owned();
    }

    if interpreter == "sh" {
        interpreter = look_for_multiline_exec(content);
    }

    if python_version().is_match(interpreter.as_bytes()) {
        if let Some(dot) = interpreter.find('.') {
            interpreter.truncate(dot);
        }
    }

    // `osascript -l` selects a different language; do not rely on the
    // interpreter name.
    if interpreter == "osascript" && line.windows(2).any(|window| window == b"-l") {
        interpreter.clear();
    }

    interpreter
}

fn env_opt_args() -> &'static Regex {
    static REGEX: OnceLock<Regex> = OnceLock::new();
    REGEX.get_or_init(|| Regex::new(r"-[i0uCSv]*|--[^ \t\r\n\f\v]+").expect("env opt args"))
}

fn env_var_args() -> &'static Regex {
    static REGEX: OnceLock<Regex> = OnceLock::new();
    REGEX.get_or_init(|| Regex::new(r"[^ \t\r\n\f\v]+=[^ \t\r\n\f\v]+").expect("env var args"))
}

fn python_version() -> &'static Regex {
    static REGEX: OnceLock<Regex> = OnceLock::new();
    REGEX.get_or_init(|| Regex::new(r"python[0-9]\.[0-9]+").expect("python version"))
}

/// enry `lookForMultilineExec`: some `sh` wrappers exec the real interpreter.
fn look_for_multiline_exec(content: &[u8]) -> String {
    static HACK: OnceLock<Regex> = OnceLock::new();
    let hack =
        HACK.get_or_init(|| Regex::new(r"exec ([0-9A-Za-z_]+).+\$0.+\$@").expect("exec hack"));

    for line in content.split(|byte| *byte == b'\n').take(5) {
        let line = line.strip_suffix(b"\r").unwrap_or(line);
        if let Some(captures) = hack.captures(line) {
            if let Some(interpreter) = captures.get(1) {
                return String::from_utf8_lossy(interpreter.as_bytes()).into_owned();
            }
        }
    }
    "sh".to_owned()
}

fn first_line(content: &[u8]) -> &[u8] {
    match content.iter().position(|byte| *byte == b'\n') {
        Some(index) => &content[..index],
        None => content,
    }
}

fn trim_ascii_space(mut bytes: &[u8]) -> &[u8] {
    while bytes.first().is_some_and(u8::is_ascii_whitespace) {
        bytes = &bytes[1..];
    }
    while bytes.last().is_some_and(u8::is_ascii_whitespace) {
        bytes = &bytes[..bytes.len() - 1];
    }
    bytes
}

// ---------------------------------------------------------------------------
// Content heuristics
// ---------------------------------------------------------------------------

/// enry `GetLanguagesByContent`.
fn languages_by_content(path: &str, content: &[u8]) -> Option<Vec<String>> {
    let extension = filepath_ext(path)?.to_lowercase();
    let heuristics = lookup_heuristics(&extension)?;
    for heuristic in heuristics {
        if rule_matches(&heuristic.rule, content) {
            return Some(
                heuristic
                    .languages
                    .iter()
                    .map(|lang| (*lang).to_owned())
                    .collect(),
            );
        }
    }
    None
}

/// `data.Heuristics.Match`: the first matching rule decides.
fn rule_matches(rule: &enry_data::Rule, content: &[u8]) -> bool {
    use enry_data::Rule;
    match rule {
        Rule::And(children) => children.iter().all(|rule| rule_matches(rule, content)),
        Rule::Or(children) => children.iter().any(|rule| rule_matches(rule, content)),
        Rule::Not(children) => !children.iter().any(|rule| rule_matches(rule, content)),
        Rule::Always => true,
        Rule::Pattern(index) => patterns()[*index]
            .as_ref()
            .is_some_and(|pattern| pattern.is_match(content)),
        // Ruby/Oniguruma-only rules are compiled away in the default build.
        Rule::Ruby => false,
    }
}

fn patterns() -> &'static [Option<Regex>] {
    static PATTERNS: OnceLock<Vec<Option<Regex>>> = OnceLock::new();
    PATTERNS.get_or_init(|| {
        enry_data::PATTERNS
            .iter()
            .map(|pattern| compile_enry_pattern(pattern))
            .collect()
    })
}

/// RE2 treats a `{` that does not start a repetition as a literal, while the
/// `regex` crate rejects it. Escape those braces so the generated patterns
/// compile unchanged.
fn compile_enry_pattern(pattern: &str) -> Option<Regex> {
    if let Ok(regex) = Regex::new(pattern) {
        return Some(regex);
    }
    let mut repaired = String::with_capacity(pattern.len());
    let mut chars = pattern.chars().peekable();
    while let Some(ch) = chars.next() {
        if ch == '{' && !chars.peek().is_some_and(|next| next.is_ascii_digit()) {
            repaired.push_str(r"\{");
        } else {
            repaired.push(ch);
        }
    }
    Regex::new(&repaired).ok()
}

fn lookup_heuristics(extension: &str) -> Option<&'static [enry_data::Heuristic]> {
    static HEURISTICS: OnceLock<HashMap<&'static str, &'static [enry_data::Heuristic]>> =
        OnceLock::new();
    HEURISTICS
        .get_or_init(|| enry_data::HEURISTICS.iter().copied().collect())
        .get(extension)
        .copied()
}

// ---------------------------------------------------------------------------
// Alias lookup and `lang:` path patterns
// ---------------------------------------------------------------------------

/// Sourcegraph `GetLanguageByNameOrAlias`.
pub fn language_by_name_or_alias(name_or_alias: &str) -> Option<&'static str> {
    let alias = convert_to_alias_key(name_or_alias);
    if let Some(lang) = unsupported_alias_map().get(alias.as_str()) {
        return Some(lang);
    }
    language_by_alias(&alias)
}

/// enry `LanguageByAlias`.
fn language_by_alias(alias: &str) -> Option<&'static str> {
    let key = convert_to_alias_key(alias);
    lookup_str_map(
        str_map_index(&LANGUAGE_ALIASES_INDEX, enry_data::LANGUAGE_ALIASES),
        &key,
    )
    .copied()
}

/// enry `convertToAliasKey`.
fn convert_to_alias_key(name: &str) -> String {
    let first = name.split(',').next().unwrap_or(name);
    first.replace(' ', "_").to_lowercase()
}

/// Sourcegraph `GetLanguageExtensions` (with niche extensions removed by the
/// generator).
pub fn language_extensions(language: &str) -> &'static [&'static str] {
    lookup_slice_map(
        slice_map_index(
            &EXTENSIONS_BY_LANGUAGE_INDEX,
            enry_data::EXTENSIONS_BY_LANGUAGE,
        ),
        language,
    )
    .unwrap_or(&[])
}

/// Filenames associated with a language, sorted (Go `filenamesFromLanguage`).
pub fn filenames_for_language(language: &str) -> &'static [&'static str] {
    static FILENAMES: OnceLock<HashMap<&'static str, Vec<&'static str>>> = OnceLock::new();
    FILENAMES
        .get_or_init(|| {
            let mut map: HashMap<&'static str, Vec<&'static str>> = HashMap::new();
            for (filename, languages) in enry_data::LANGUAGES_BY_FILENAME {
                for language in *languages {
                    map.entry(language).or_default().push(filename);
                }
            }
            for filenames in map.values_mut() {
                filenames.sort_unstable();
            }
            map
        })
        .get(language)
        .map_or(&[], Vec::as_slice)
}

/// Sourcegraph `query.LangToFileRegexp`: a case-insensitive union of the
/// language's `\.ext$` patterns and `(^|/)filename$` patterns.
pub fn lang_to_file_regexp(language: &str) -> String {
    // Invariant: the language is valid; unknown names behave like Go's
    // `"Other"`.
    let canonical = language_by_name_or_alias(language).unwrap_or("Other");
    let mut patterns: Vec<String> = language_extensions(canonical)
        .iter()
        .map(|extension| format!("{}$", regex::escape(extension)))
        .collect();
    patterns.extend(
        filenames_for_language(canonical)
            .iter()
            .map(|filename| format!("(^|/){}", regex::escape(filename))),
    );
    case_insensitive_regexp(&union_regexps(&patterns))
}

/// Sourcegraph `query.UnionRegExps`.
fn union_regexps(values: &[String]) -> String {
    match values {
        [] => String::new(),
        [value] => value.clone(),
        _ => values
            .iter()
            .map(|value| format!("(?:{value})"))
            .collect::<Vec<_>>()
            .join("|"),
    }
}

/// Sourcegraph `query.CaseInsensitiveRegExp`.
fn case_insensitive_regexp(value: &str) -> String {
    if value.starts_with("(?i)") {
        value.to_owned()
    } else {
        format!("(?i){value}")
    }
}

fn unsupported_alias_map() -> &'static HashMap<String, &'static str> {
    static MAP: OnceLock<HashMap<String, &'static str>> = OnceLock::new();
    MAP.get_or_init(|| {
        let mut map = HashMap::new();
        for language in enry_data::UNSUPPORTED_EXTENSIONS
            .iter()
            .map(|(_, language)| *language)
        {
            map.entry(convert_to_alias_key(language))
                .or_insert(language);
        }
        map
    })
}

/// Turn `"Apex"` into a one-element static slice for extension overrides.
fn lang_slice(language: &str) -> &'static [&'static str] {
    // The override maps only contain a handful of languages; leak a tiny
    // static table keyed by the generated data instead of the input.
    static LANGS: OnceLock<HashMap<&'static str, &'static [&'static str]>> = OnceLock::new();
    LANGS
        .get_or_init(|| {
            let mut map: HashMap<&'static str, &'static [&'static str]> = HashMap::new();
            for (_, language) in enry_data::UNSUPPORTED_EXTENSIONS {
                map.insert(language, std::slice::from_ref(language));
            }
            for (_, language) in enry_data::OVERRIDE_AMBIGUOUS_EXTENSIONS {
                map.insert(language, std::slice::from_ref(language));
            }
            map
        })
        .get(language)
        .copied()
        .unwrap_or(&[])
}

// ---------------------------------------------------------------------------
// Static table indexes
// ---------------------------------------------------------------------------

fn build_slice_map(
    entries: &'static [(&'static str, &'static [&'static str])],
) -> HashMap<&'static str, &'static [&'static str]> {
    entries.iter().copied().collect()
}

fn build_str_map(
    entries: &'static [(&'static str, &'static str)],
) -> HashMap<&'static str, &'static str> {
    entries.iter().copied().collect()
}

fn build_set(entries: &'static [&'static str]) -> std::collections::HashSet<&'static str> {
    entries.iter().copied().collect()
}

fn lookup_slice_map(
    map: &'static HashMap<&'static str, &'static [&'static str]>,
    key: &str,
) -> Option<&'static [&'static str]> {
    map.get(key).copied()
}

fn lookup_str_map<'a>(
    map: &'a HashMap<&'static str, &'static str>,
    key: &str,
) -> Option<&'a &'static str> {
    map.get(key)
}

fn lookup_set(set: &'static std::collections::HashSet<&'static str>, key: &str) -> bool {
    set.contains(key)
}

// Static and function share a name in different namespaces (type vs value),
// so each table needs distinct identifiers.
static LANGUAGES_BY_EXTENSION_INDEX: OnceLock<HashMap<&'static str, &'static [&'static str]>> =
    OnceLock::new();
static LANGUAGES_BY_FILENAME_INDEX: OnceLock<HashMap<&'static str, &'static [&'static str]>> =
    OnceLock::new();
static EXTENSIONS_BY_LANGUAGE_INDEX: OnceLock<HashMap<&'static str, &'static [&'static str]>> =
    OnceLock::new();
static LANGUAGES_BY_INTERPRETER_INDEX: OnceLock<HashMap<&'static str, &'static [&'static str]>> =
    OnceLock::new();
static LANGUAGE_ALIASES_INDEX: OnceLock<HashMap<&'static str, &'static str>> = OnceLock::new();
static OVERRIDE_AMBIGUOUS_EXTENSIONS_INDEX: OnceLock<HashMap<&'static str, &'static str>> =
    OnceLock::new();
static UNSUPPORTED_EXTENSIONS_INDEX: OnceLock<HashMap<&'static str, &'static str>> =
    OnceLock::new();

fn slice_map_index(
    index: &'static OnceLock<HashMap<&'static str, &'static [&'static str]>>,
    entries: &'static [(&'static str, &'static [&'static str])],
) -> &'static HashMap<&'static str, &'static [&'static str]> {
    index.get_or_init(|| build_slice_map(entries))
}

fn str_map_index(
    index: &'static OnceLock<HashMap<&'static str, &'static str>>,
    entries: &'static [(&'static str, &'static str)],
) -> &'static HashMap<&'static str, &'static str> {
    index.get_or_init(|| build_str_map(entries))
}

static COMMON_BINARY_EXTENSIONS_SET: OnceLock<std::collections::HashSet<&'static str>> =
    OnceLock::new();
fn common_binary_extensions_set() -> &'static std::collections::HashSet<&'static str> {
    COMMON_BINARY_EXTENSIONS_SET.get_or_init(|| build_set(enry_data::COMMON_BINARY_EXTENSIONS))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn languages(path: &str, content: &'static [u8]) -> Vec<String> {
        get_languages(path, Some(|| Ok(content.to_vec()))).expect("languages")
    }

    #[test]
    fn resolves_aliases_like_sourcegraph() {
        assert_eq!(language_by_name_or_alias("go"), Some("Go"));
        assert_eq!(language_by_name_or_alias("Go"), Some("Go"));
        assert_eq!(language_by_name_or_alias("ada95"), Some("Ada"));
        // Spaces become underscores; the part after a comma is dropped.
        assert_eq!(
            language_by_name_or_alias("Visual Basic .NET"),
            Some("Visual Basic .NET")
        );
        assert_eq!(language_by_name_or_alias("apex"), Some("Apex"));
        assert_eq!(language_by_name_or_alias("not-a-language"), None);
    }

    #[test]
    fn builds_lang_path_patterns() {
        let regexp = lang_to_file_regexp("go");
        assert!(regexp.starts_with("(?i)"));
        let compiled = regex::Regex::new(&regexp).expect("compile");
        assert!(compiled.is_match("main.go"));
        assert!(compiled.is_match("MAIN.GO"));
        assert!(!compiled.is_match("main.rs"));

        let regexp = lang_to_file_regexp("dockerfile");
        let compiled = regex::Regex::new(&regexp).expect("compile");
        assert!(compiled.is_match("Dockerfile"));
        assert!(compiled.is_match("dir/Dockerfile"));
        // Like Go's unanchored `regexp.MatchString`, `(^|/)Dockerfile$`
        // matches any path ending in the filename.
        assert!(compiled.is_match("dir/sub/Dockerfile"));
    }

    #[test]
    fn detects_by_filename_and_extension() {
        assert_eq!(languages("Makefile", b""), ["Makefile"]);
        assert_eq!(languages("src/main.go", b""), ["Go"]);
        assert_eq!(languages("src/Main.GO", b""), ["Go"]);
        // Sourcegraph's overrides collapse otherwise ambiguous extensions.
        assert_eq!(languages("app.ts", b""), ["TypeScript"]);
        assert_eq!(languages("README.md", b""), ["Markdown"]);
        assert_eq!(languages("value.rs", b""), ["Rust"]);
        assert_eq!(languages("Salesforce.cls", b""), ["Apex"]);
        assert_eq!(languages("icon.png", b"not really"), Vec::<String>::new());
    }

    #[test]
    fn uses_content_heuristics_for_ambiguous_extensions() {
        assert_eq!(languages("thing.m", b"% matlab comment\n"), ["MATLAB"]);
        assert_eq!(languages("thing.m", b"@interface Foo\n"), ["Objective-C"]);
        assert_eq!(languages("thing.m", b":- module foo.\n"), ["Mercury"]);
        assert_eq!(
            languages("thing.m", b"(* mathematica *)\n"),
            ["Mathematica"]
        );
    }

    #[test]
    fn detects_shebangs() {
        assert_eq!(
            languages("script", b"#!/usr/bin/env python3\nprint(1)\n"),
            ["Python"]
        );
        assert_eq!(
            languages("script", b"#!/bin/sh\nexec python3.11 \"$0\" \"$@\"\n"),
            ["Python"]
        );
    }

    #[test]
    fn detects_modelines() {
        // `.txt` is resolved by Sourcegraph's extension override before the
        // content strategies run, so use an extension enry does not know.
        assert_eq!(
            languages("file.unknown", b"# -*- mode: ruby -*-\n"),
            ["Ruby"]
        );
        assert_eq!(
            languages("file.unknown", b"# vim: filetype=python\n"),
            ["Python"]
        );
    }

    #[test]
    fn skips_content_for_binary_files() {
        let mut content = vec![b'a'; 10];
        content[0] = 0;
        assert!(is_binary(&content));
        // The extension is unknown, so detection stops at "no language".
        let content: &'static [u8] = Box::leak(content.into_boxed_slice());
        assert_eq!(languages("blob.unknown", content), Vec::<String>::new());
    }

    #[test]
    fn applies_include_and_exclude_filters() {
        let matcher = LangMatcher::Enry(EnryLangMatcher {
            include_langs: vec!["Go".to_owned()],
            exclude_langs: Vec::new(),
            only_excludes: false,
        });
        assert_eq!(
            matcher.matches("main.go", || Ok(Vec::new())),
            (true, "Go".to_owned())
        );
        assert_eq!(
            matcher.matches("main.rs", || Ok(Vec::new())),
            (false, String::new())
        );

        let matcher = LangMatcher::Enry(EnryLangMatcher {
            include_langs: Vec::new(),
            exclude_langs: vec!["Go".to_owned()],
            only_excludes: true,
        });
        assert_eq!(
            matcher.matches("main.go", || Ok(Vec::new())),
            (false, "Go".to_owned())
        );
        assert_eq!(
            matcher.matches("main.rs", || Ok(Vec::new())),
            (true, "Rust".to_owned())
        );
        // Files without a detected language pass when only excludes are set.
        assert_eq!(
            matcher.matches("blob.unknown", || Ok(Vec::new())),
            (true, String::new())
        );
    }

    #[test]
    fn all_heuristic_patterns_compile() {
        let compiled = patterns();
        for (index, pattern) in compiled.iter().enumerate() {
            assert!(
                pattern.is_some(),
                "pattern {index} {:?} failed to compile",
                enry_data::PATTERNS[index]
            );
        }
    }

    #[test]
    fn to_lang_matcher_avoids_content_checks_without_filters() {
        let matcher = to_lang_matcher(&PatternInfo::default());
        assert!(matches!(matcher, LangMatcher::All));

        let info = PatternInfo {
            exclude_langs: vec!["Go".to_owned()],
            ..PatternInfo::default()
        };
        match to_lang_matcher(&info) {
            LangMatcher::Enry(matcher) => assert!(matcher.only_excludes),
            LangMatcher::All => panic!("expected enry matcher"),
        }
    }
}
