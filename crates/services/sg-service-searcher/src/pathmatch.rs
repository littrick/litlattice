#![forbid(unsafe_code)]

//! Include/exclude path filters (`pathmatch.go`).

use std::fmt;

use regex::Regex;

use crate::langmatch::lang_to_file_regexp;
use crate::protocol::PatternInfo;

/// Baseline `pathMatcher`.
#[derive(Debug, Clone)]
pub struct PathMatcher {
    include: Vec<Regex>,
    exclude: Option<Regex>,
}

impl PathMatcher {
    /// Baseline `pathMatcher.Matches`: every include must match and the
    /// exclude must not.
    pub fn matches(&self, path: &str) -> bool {
        if !self.include.iter().all(|regex| regex.is_match(path)) {
            return false;
        }
        self.exclude
            .as_ref()
            .is_none_or(|regex| !regex.is_match(path))
    }
}

impl fmt::Display for PathMatcher {
    /// Baseline `pathMatcher.String`.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut parts: Vec<String> = self
            .include
            .iter()
            .map(|regex| regex.as_str().to_owned())
            .collect();
        if let Some(exclude) = &self.exclude {
            parts.push(format!("!{}", exclude.as_str()));
        }
        f.write_str(&parts.join(" "))
    }
}

/// Baseline `toPathMatcher`: all include patterns are ANDed and the single
/// exclude pattern is negated. Language filters are added as path patterns
/// purely as a fast pre-filter.
pub fn to_path_matcher(pattern_info: &PatternInfo) -> Result<PathMatcher, regex::Error> {
    let compile = |pattern: &str| {
        if pattern_info.path_patterns_are_case_sensitive {
            Regex::new(pattern)
        } else {
            // Like the baseline, keep any `(?i:...)` the user already wrote.
            Regex::new(&format!("(?i:{pattern})"))
        }
    };

    let mut include =
        Vec::with_capacity(pattern_info.include_paths.len() + pattern_info.include_langs.len());
    for pattern in &pattern_info.include_paths {
        include.push(compile(pattern)?);
    }
    for lang in &pattern_info.include_langs {
        include.push(compile(&lang_to_file_regexp(lang))?);
    }

    let exclude = if pattern_info.exclude_paths.is_empty() {
        None
    } else {
        Some(compile(&pattern_info.exclude_paths)?)
    };

    Ok(PathMatcher { include, exclude })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn info() -> PatternInfo {
        PatternInfo::default()
    }

    #[test]
    fn matches_all_includes_and_no_exclude() {
        let matcher = to_path_matcher(&PatternInfo {
            include_paths: vec!["\\.go$".to_owned(), "cmd/".to_owned()],
            exclude_paths: "_test\\.go$".to_owned(),
            ..info()
        })
        .expect("matcher");
        assert!(matcher.matches("cmd/main.go"));
        assert!(!matcher.matches("cmd/main_test.go"));
        assert!(!matcher.matches("internal/main.go"));
    }

    #[test]
    fn is_case_insensitive_by_default() {
        let matcher = to_path_matcher(&PatternInfo {
            include_paths: vec!["README".to_owned()],
            ..info()
        })
        .expect("matcher");
        assert!(matcher.matches("readme.md"));

        let matcher = to_path_matcher(&PatternInfo {
            include_paths: vec!["README".to_owned()],
            path_patterns_are_case_sensitive: true,
            ..info()
        })
        .expect("matcher");
        assert!(!matcher.matches("readme.md"));
    }

    #[test]
    fn adds_language_patterns() {
        let matcher = to_path_matcher(&PatternInfo {
            include_langs: vec!["go".to_owned()],
            ..info()
        })
        .expect("matcher");
        assert!(matcher.matches("main.go"));
        assert!(!matcher.matches("main.rs"));
    }

    #[test]
    fn rejects_invalid_patterns() {
        let error = to_path_matcher(&PatternInfo {
            include_paths: vec!["(".to_owned()],
            ..info()
        })
        .expect_err("invalid regexp");
        assert!(!error.to_string().is_empty());
    }
}
