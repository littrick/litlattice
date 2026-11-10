#![forbid(unsafe_code)]

//! The language helpers `query.LangToFileRegexp` and
//! `languages.GetLanguageByNameOrAlias` observe
//! (`lib/codeintel/languages/extensions.go`, `internal/search/query/helpers.go`).
//!
//! The tables in [`enry_data`] are generated from the searcher's copy of the
//! go-enry data (which already applies Sourcegraph's niche-extension
//! overrides), so this crate does not depend on `sg-service-searcher` (that
//! crate depends on `sg-zoekt`).

pub mod enry_data;

use std::collections::HashMap;
use std::sync::OnceLock;

/// `convertToAliasKey`: the part before the first comma, spaces to
/// underscores, lowercased.
pub fn convert_to_alias_key(name: &str) -> String {
    let name = name.split(',').next().unwrap_or("");
    name.replace(' ', "_").to_lowercase()
}

fn alias_map() -> &'static HashMap<&'static str, &'static str> {
    static MAP: OnceLock<HashMap<&'static str, &'static str>> = OnceLock::new();
    MAP.get_or_init(|| {
        let mut map: HashMap<&'static str, &'static str> =
            enry_data::LANGUAGE_ALIASES.iter().copied().collect();
        // `unsupportedByEnryAliasMap`: Apex/Pkl/Magik are recognized through
        // the wrapper's extension overrides even though enry does not know
        // them. Their aliases are the transformed canonical names.
        for (_, language) in enry_data::UNSUPPORTED_EXTENSIONS.iter().copied() {
            let alias: &'static str = Box::leak(convert_to_alias_key(language).into_boxed_str());
            map.entry(alias).or_insert(language);
        }
        map
    })
}

fn extensions_map() -> &'static HashMap<&'static str, &'static [&'static str]> {
    static MAP: OnceLock<HashMap<&'static str, &'static [&'static str]>> = OnceLock::new();
    MAP.get_or_init(|| enry_data::EXTENSIONS_BY_LANGUAGE.iter().copied().collect())
}

fn filenames_map() -> &'static HashMap<&'static str, Vec<&'static str>> {
    static MAP: OnceLock<HashMap<&'static str, Vec<&'static str>>> = OnceLock::new();
    MAP.get_or_init(|| {
        let mut map: HashMap<&'static str, Vec<&'static str>> = HashMap::new();
        for (filename, languages) in enry_data::LANGUAGES_BY_FILENAME {
            for language in *languages {
                map.entry(*language).or_default().push(*filename);
            }
        }
        for filenames in map.values_mut() {
            filenames.sort_unstable();
        }
        map
    })
}

/// `languages.GetLanguageByNameOrAlias`. Returns `"Other"` for an unknown
/// language, like `enry.GetLanguageByAlias`.
pub fn get_language_by_name_or_alias(name_or_alias: &str) -> &'static str {
    let alias = convert_to_alias_key(name_or_alias);
    alias_map().get(alias.as_str()).copied().unwrap_or("Other")
}

/// `languages.GetLanguageExtensions`.
pub fn get_language_extensions(language: &str) -> &'static [&'static str] {
    extensions_map().get(language).copied().unwrap_or(&[])
}

/// `filenamesFromLanguage`: full filenames associated with a language,
/// sorted like the Go map's values.
pub fn filenames_from_language(language: &str) -> &'static [&'static str] {
    filenames_map()
        .get(language)
        .map(Vec::as_slice)
        .unwrap_or(&[])
}

/// `query.UnionRegExps`.
pub fn union_regexps(values: &[String]) -> String {
    match values.len() {
        0 => String::new(),
        1 => values[0].clone(),
        _ => format!("(?:{})", values.join(")|(?:").as_str()),
    }
}

/// `query.CaseInsensitiveRegExp`.
pub fn case_insensitive_regexp(value: &str) -> String {
    if value.starts_with("(?i)") {
        value.to_owned()
    } else {
        format!("(?i){value}")
    }
}

/// `query.LangToFileRegexp`. The input must be a valid language (the query
/// validator guarantees this for `lang:` values).
pub fn lang_to_file_regexp(lang: &str) -> String {
    let lang = get_language_by_name_or_alias(lang);
    let extensions = get_language_extensions(lang);
    let mut patterns: Vec<String> = extensions
        .iter()
        .map(|extension| format!("{}$", regexp_quote_meta(extension)))
        .collect();
    for filename in filenames_from_language(lang) {
        patterns.push(format!("(^|/){}$", regexp_quote_meta(filename)));
    }
    // Language filters are always case insensitive.
    case_insensitive_regexp(&union_regexps(&patterns))
}

/// `regexp.QuoteMeta` (`\.+*?()|[]{}^$`), rune-wise so non-ASCII filenames
/// survive unchanged.
pub fn regexp_quote_meta(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for c in value.chars() {
        if matches!(
            c,
            '\\' | '.' | '+' | '*' | '?' | '(' | ')' | '|' | '[' | ']' | '{' | '}' | '^' | '$'
        ) {
            out.push('\\');
        }
        out.push(c);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolves_names_and_aliases() {
        assert_eq!(get_language_by_name_or_alias("go"), "Go");
        assert_eq!(get_language_by_name_or_alias("golang"), "Go");
        assert_eq!(get_language_by_name_or_alias("Go"), "Go");
        assert_eq!(get_language_by_name_or_alias("ada95"), "Ada");
        assert_eq!(get_language_by_name_or_alias("typescript"), "TypeScript");
        assert_eq!(get_language_by_name_or_alias("not-a-language"), "Other");
        assert_eq!(get_language_by_name_or_alias("apex"), "Apex");
    }

    #[test]
    fn converts_languages_to_insensitive_file_regexps() {
        assert_eq!(lang_to_file_regexp("go"), r"(?i)\.go$");
        // The union form is `(?:a)|(?:b)` surrounded by a single `(?i)`.
        let ts = lang_to_file_regexp("typescript");
        assert!(ts.starts_with("(?i)(?:"), "{ts}");
        assert!(ts.contains(r"\.ts$"), "{ts}");
        assert!(ts.contains(r"\.mts$"), "{ts}");
    }

    #[test]
    fn helper_regexp_shapes() {
        assert_eq!(union_regexps(&[]), "");
        assert_eq!(union_regexps(&["a".to_owned()]), "a");
        assert_eq!(
            union_regexps(&["a".to_owned(), "b".to_owned()]),
            "(?:a)|(?:b)"
        );
        assert_eq!(case_insensitive_regexp("x"), "(?i)x");
        assert_eq!(case_insensitive_regexp("(?i)x"), "(?i)x");
        assert_eq!(regexp_quote_meta("a.go"), r"a\.go");
    }
}
