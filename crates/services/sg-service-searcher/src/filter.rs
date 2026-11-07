#![forbid(unsafe_code)]

//! Tar filtering, mirroring `filter.go` and zoekt's `ignore` package.
//!
//! `.sourcegraph/ignore` patterns are globs compiled by `gobwas/glob` with
//! `/` as the separator: `*` does not cross a slash while `**` does. The
//! site-config `search.largeFiles` list uses doublestar semantics.

use sha2::{Digest, Sha256};

/// `maxFileSize` in `store.go`: files larger than this keep only a name entry.
pub const MAX_FILE_SIZE: u64 = 2 << 20;

/// The ignore file read from the searched commit.
pub const IGNORE_FILE: &str = ".sourcegraph/ignore";

/// `ignore.Matcher`.
#[derive(Debug, Clone, Default)]
pub struct IgnoreMatcher {
    patterns: Vec<regex::Regex>,
}

impl IgnoreMatcher {
    /// `ignore.ParseIgnoreFile`.
    pub fn parse(input: &str) -> Result<Self, String> {
        let mut patterns = Vec::new();
        for raw in input.lines() {
            let line = raw.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            let line = line.strip_prefix('/').unwrap_or(line);
            // implicit ** for patterns without glob characters
            let line = if !line.contains(['.', ']', '[', '*', '?']) {
                format!("{line}**")
            } else {
                line.to_owned()
            };
            patterns.push(
                glob_regex(&line)
                    .map_err(|err| format!("invalid ignore pattern {line:?}: {err}"))?,
            );
        }
        Ok(Self { patterns })
    }

    /// `ignore.Matcher.Match`: true when any pattern matches the whole path.
    pub fn matches(&self, path: &str) -> bool {
        self.patterns.iter().any(|pattern| pattern.is_match(path))
    }
}

/// Filter for one repository revision (`searchableFilter`).
#[derive(Debug, Clone)]
pub struct SearchableFilter {
    /// `None` when `.sourcegraph/ignore` does not exist; the baseline then
    /// ignores nothing, including large files.
    commit_ignore: Option<IgnoreMatcher>,
    /// Site configuration `search.largeFiles`.
    pub search_large_files: Vec<String>,
}

impl SearchableFilter {
    /// `NewFilterFactory`: a missing ignore file means "match nothing".
    pub fn new(
        ignore_file: Option<&[u8]>,
        search_large_files: Vec<String>,
    ) -> Result<Self, String> {
        let commit_ignore = match ignore_file {
            Some(contents) => Some(IgnoreMatcher::parse(&String::from_utf8_lossy(contents))?),
            None => None,
        };
        Ok(Self {
            commit_ignore,
            search_large_files,
        })
    }

    /// `searchableFilter.Ignore`.
    pub fn ignore(&self, name: &str, size: u64) -> bool {
        match &self.commit_ignore {
            None => false,
            Some(ignore) => size > MAX_FILE_SIZE || ignore.matches(name),
        }
    }

    /// `searchableFilter.SkipContent`.
    pub fn skip_content(&self, name: &str, size: u64) -> bool {
        if size <= MAX_FILE_SIZE {
            return false;
        }

        // A pattern match will override preceding pattern matches.
        for pattern in self.search_large_files.iter().rev() {
            let pattern = pattern.trim();
            let (negated, pattern) = match pattern.strip_prefix('!') {
                Some(pattern) => (true, pattern),
                None => (false, pattern),
            };
            if doublestar_match(pattern, name) {
                return negated;
            }
        }
        true
    }

    /// `searchableFilter.HashKey`.
    pub fn hash_key(&self, hasher: &mut Sha256) {
        hasher.update(b"\x00SearchLargeFiles");
        for pattern in &self.search_large_files {
            hasher.update([0u8]);
            hasher.update(pattern.as_bytes());
        }
    }
}

/// Compile a gobwas-style glob (`/` separator, `**` crosses separators).
pub fn glob_regex(pattern: &str) -> Result<regex::Regex, String> {
    let translated = translate_glob(pattern, GlobDialect::Gobwas)?;
    regex::Regex::new(&translated).map_err(|err| err.to_string())
}

/// Compile a doublestar path glob.
pub fn doublestar_match(pattern: &str, path: &str) -> bool {
    translate_glob(pattern, GlobDialect::Doublestar)
        .ok()
        .and_then(|translated| regex::Regex::new(&translated).ok())
        .is_some_and(|regex| regex.is_match(path))
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum GlobDialect {
    Gobwas,
    Doublestar,
}

fn translate_glob(pattern: &str, dialect: GlobDialect) -> Result<String, String> {
    Ok(format!(
        "(?s)\\A{}\\z",
        translate_glob_body(pattern, dialect)?
    ))
}

fn translate_glob_body(pattern: &str, dialect: GlobDialect) -> Result<String, String> {
    let mut out = String::new();
    let chars: Vec<char> = pattern.chars().collect();
    let mut index = 0;
    while index < chars.len() {
        match chars[index] {
            '*' => {
                if index + 1 < chars.len() && chars[index + 1] == '*' {
                    match dialect {
                        GlobDialect::Gobwas => out.push_str(".*"),
                        GlobDialect::Doublestar => {
                            let at_segment_start = index == 0 || chars[index - 1] == '/';
                            let next = chars.get(index + 2);
                            let segment_terminated = next.is_none() || next == Some(&'/');
                            if at_segment_start && segment_terminated {
                                if next == Some(&'/') {
                                    // `**/` matches zero or more segments.
                                    if index == 0 {
                                        out.push_str("(?:.*/)?");
                                    } else {
                                        // `` already wrote `/` before `**`.
                                        // Replace it with an optional group
                                        // that also keeps the separator.
                                        out.pop();
                                        out.push_str("(?:/|/.*/)");
                                    }
                                    index += 3;
                                    continue;
                                }
                                // Trailing `/**` matches the directory and
                                // everything below it.
                                if index > 0 {
                                    out.pop();
                                    out.push_str("(?:/.*)?");
                                } else {
                                    out.push_str(".*");
                                }
                                index += 2;
                                continue;
                            }
                            out.push_str("[^/]*");
                        }
                    }
                    index += 2;
                } else {
                    out.push_str("[^/]*");
                    index += 1;
                }
            }
            '?' => {
                out.push_str("[^/]");
                index += 1;
            }
            '[' => {
                let (class, next) = translate_class(&chars, index)?;
                out.push_str(&class);
                index = next;
            }
            '{' => {
                let (group, next) = translate_alternation(&chars, index, dialect)?;
                out.push_str(&group);
                index = next;
            }
            '\\' => {
                if let Some(next) = chars.get(index + 1) {
                    out.push_str(&regex::escape(&next.to_string()));
                    index += 2;
                } else {
                    out.push_str("\\\\");
                    index += 1;
                }
            }
            c => {
                out.push_str(&regex::escape(&c.to_string()));
                index += 1;
            }
        }
    }
    Ok(out)
}

fn translate_class(chars: &[char], start: usize) -> Result<(String, usize), String> {
    let mut out = String::from("[");
    let mut index = start + 1;
    if chars.get(index) == Some(&'!') {
        out.push('^');
        index += 1;
    } else if chars.get(index) == Some(&'^') {
        out.push('^');
        index += 1;
        // A literal `]` first in the class is allowed.
        if chars.get(index) == Some(&']') {
            out.push_str("\\]");
            index += 1;
        }
    }
    while index < chars.len() {
        match chars[index] {
            ']' => {
                out.push(']');
                return Ok((out, index + 1));
            }
            '\\' => {
                if let Some(next) = chars.get(index + 1) {
                    out.push('\\');
                    out.push(*next);
                    index += 2;
                } else {
                    return Err("trailing backslash in character class".to_owned());
                }
            }
            '[' => return Err("unclosed character class".to_owned()),
            c => {
                // Escape regex metacharacters that would change the class.
                if c == '&' || c == '~' || c == '|' || c == '{' || c == '}' {
                    out.push('\\');
                }
                out.push(c);
                index += 1;
            }
        }
    }
    Err("unclosed character class".to_owned())
}

fn translate_alternation(
    chars: &[char],
    start: usize,
    dialect: GlobDialect,
) -> Result<(String, usize), String> {
    // Split top-level commas inside the braces.
    let mut depth = 0;
    let mut alternatives = Vec::new();
    let mut current = String::new();
    let mut index = start + 1;
    while index < chars.len() {
        match chars[index] {
            '{' => {
                depth += 1;
                current.push('{');
                index += 1;
            }
            '}' if depth == 0 => {
                alternatives.push(current);
                let mut out = String::from("(?:");
                for (i, alternative) in alternatives.iter().enumerate() {
                    if i > 0 {
                        out.push('|');
                    }
                    out.push_str(&translate_glob_body(alternative, dialect)?);
                }
                out.push(')');
                // Tell the caller to strip the inner anchors later.
                return Ok((out, index + 1));
            }
            '}' => {
                depth -= 1;
                current.push('}');
                index += 1;
            }
            ',' if depth == 0 => {
                alternatives.push(std::mem::take(&mut current));
                index += 1;
            }
            c => {
                current.push(c);
                index += 1;
            }
        }
    }
    Err("unclosed alternation".to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_zoekt_ignore_patterns() {
        let matcher = IgnoreMatcher::parse("# ignore this \n  \n foo\n bar/").expect("parse");
        assert_eq!(matcher.patterns.len(), 2);
        assert!(matcher.patterns[0].is_match("foo"));
        assert!(matcher.patterns[0].is_match("foobar"));
        assert!(matcher.patterns[1].is_match("bar/baz"));

        let matcher = IgnoreMatcher::parse("/foo/bar \n /qux \n *.go\nfoo.go").expect("parse");
        assert_eq!(matcher.patterns.len(), 4);
        assert!(matcher.matches("foo/bar"));
        assert!(matcher.matches("qux"));
        assert!(matcher.matches("foo.go"));
        // `*` does not cross `/` with the gobwas separator.
        assert!(!matcher.matches("dir2/foo.go"));
    }

    #[test]
    fn matches_the_zoekt_examples() {
        let matcher = IgnoreMatcher::parse("dir1/\n*.go\n**/data.*").expect("parse");
        assert!(matcher.matches("dir1/readme.md"));
        assert!(matcher.matches("dir1/dir2/readme.md"));
        assert!(matcher.matches("foo.go"));
        assert!(!matcher.matches("dir2/foo.go"));
        assert!(matcher.matches("dir3/data.xyz"));
    }

    #[test]
    fn filters_like_new_filter() {
        let filter = SearchableFilter::new(Some(b"foo/"), Vec::new()).expect("filter");
        assert!(filter.ignore("foo/ignore-me.go", 0));
        assert!(!filter.ignore("bar/dont-ignore-me.go", 0));
        assert!(filter.ignore("bar/large-file.go", 2 << 21));

        let filter = SearchableFilter::new(None, Vec::new()).expect("filter");
        assert!(!filter.ignore("foo/ignore-me.go", 0));
        // The baseline does not apply the size limit when the ignore file is
        // missing.
        assert!(!filter.ignore("bar/large-file.go", 2 << 21));
    }

    #[test]
    fn skips_large_files_unless_allowed() {
        let filter = SearchableFilter::new(None, vec!["*.md".to_owned(), "!vendor/**".to_owned()])
            .expect("filter");
        assert!(!filter.skip_content("small.go", 10));
        assert!(filter.skip_content("large.go", MAX_FILE_SIZE + 1));
        assert!(!filter.skip_content("README.md", MAX_FILE_SIZE + 1));
        assert!(filter.skip_content("vendor/large.md", MAX_FILE_SIZE + 1));
    }

    #[test]
    fn matches_doublestar_patterns() {
        assert!(doublestar_match("*.md", "README.md"));
        assert!(!doublestar_match("*.md", "docs/README.md"));
        assert!(doublestar_match("**/*.md", "docs/README.md"));
        assert!(doublestar_match("docs/**", "docs/a/b.md"));
        assert!(doublestar_match("docs/**", "docs"));
        assert!(doublestar_match("a/**/b", "a/b"));
        assert!(doublestar_match("a/**/b", "a/x/y/b"));
        assert!(doublestar_match("*.{md,go}", "main.go"));
    }

    #[test]
    fn hashes_large_file_patterns() {
        use sha2::Digest;

        fn digest(filter: &SearchableFilter) -> String {
            let mut hasher = Sha256::new();
            filter.hash_key(&mut hasher);
            hex(&hasher.finalize())
        }
        fn hex(bytes: &[u8]) -> String {
            bytes.iter().map(|byte| format!("{byte:02x}")).collect()
        }

        let empty = SearchableFilter::new(None, Vec::new()).expect("filter");
        let with_pattern = SearchableFilter::new(None, vec!["*.md".to_owned()]).expect("filter");
        assert_ne!(digest(&empty), digest(&with_pattern));
    }
}
