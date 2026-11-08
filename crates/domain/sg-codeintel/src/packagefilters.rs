#![forbid(unsafe_code)]

//! `internal/packagefilters`: the package repository filter matcher.
//!
//! Two residents:
//!
//! * a port of `glob.go`'s `GlobToRegex` (gobwas/glob's AST → anchored Go
//!   regexp string), used as the matching engine; and
//! * a port of `package_filters.go`'s `NewFilterLists`, `IsPackageAllowed`
//!   and `IsVersionedPackageAllowed`.
//!
//! The baseline matches with `glob.Compile(pattern).Match(s)` (no separators,
//! so `*` and `**` both match any sequence and `?` any single character).
//! `GlobToRegex` is the repository's own equivalence mapping between that
//! matcher and a regular expression, so this port reuses it and compiles the
//! result with [`regex`]; [`CompiledGlob`] enables `dot_matches_new_line`
//! because gobwas' `.`-equivalent (`*` with no separators) also matches a
//! newline.
//!
//! Syntax accepted (gobwas/glob v0.2.3, the version pinned by the baseline):
//! `*`, `**`, `?`, `[...]` (with `!` negation and `a-w` ranges), `{a,b}`
//! alternation and `\x` escapes.

use std::collections::HashMap;

use regex::Regex;

/// `GlobToRegex`: parse the gobwas pattern and render it as `^...$`.
pub fn glob_to_regex(pattern: &str) -> Result<String, String> {
    let tokens = lex(pattern)?;
    let mut parser = Parser {
        tokens,
        position: 0,
    };

    let mut root = Vec::new();
    parser.parse_group(&mut root)?;

    let mut body = String::new();
    for node in &root {
        body.push_str(&node.to_regex());
    }
    Ok(format!("^{body}$"))
}

/// A glob compiled for matching, the equivalent of `glob.Glob`.
#[derive(Debug, Clone)]
pub struct CompiledGlob {
    regex: Regex,
}

impl CompiledGlob {
    /// `glob.Compile(pattern)` with no separators.
    pub fn compile(pattern: &str) -> Result<Self, String> {
        let source = glob_to_regex(pattern)?;
        let regex = regex::RegexBuilder::new(&source)
            .dot_matches_new_line(true)
            .build()
            .map_err(|err| format!("invalid glob {pattern:?}: {err}"))?;
        Ok(Self { regex })
    }

    /// `Glob.Match`.
    pub fn is_match(&self, s: &str) -> bool {
        self.regex.is_match(s)
    }
}

/// `regexp.QuoteMeta`: escape exactly the Go regexp metacharacters (the
/// baseline's `glob.go` uses Go's `regexp.QuoteMeta`, not Rust's broader
/// `regex::escape`).
fn quote_meta(s: &str) -> String {
    const SPECIAL: &[char] = &[
        '\\', '.', '+', '*', '?', '(', ')', '|', '[', ']', '{', '}', '^', '$',
    ];
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        if SPECIAL.contains(&c) {
            out.push('\\');
        }
        out.push(c);
    }
    out
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Token {
    Text(String),
    Any,
    Super,
    Single,
    RangeOpen,
    Not,
    RangeLo(char),
    RangeBetween,
    RangeHi(char),
    RangeClose,
    TermsOpen,
    Separator,
    TermsClose,
    Eof,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Node {
    Any,
    Super,
    Single,
    Text(String),
    AnyOf(Vec<Node>),
    Pattern(Vec<Node>),
    List { not: bool, chars: String },
    Range { not: bool, lo: char, hi: char },
}

impl Node {
    fn to_regex(&self) -> String {
        match self {
            Node::Any | Node::Super => ".*".to_owned(),
            Node::Single => ".".to_owned(),
            Node::Text(text) => quote_meta(text),
            Node::AnyOf(children) => {
                let parts: Vec<String> = children.iter().map(Node::to_regex).collect();
                format!("(?:{})", parts.join("|"))
            }
            Node::Pattern(children) => children.iter().map(Node::to_regex).collect(),
            Node::List { not, chars } => {
                let prefix = if *not { "^" } else { "" };
                format!("[{prefix}{}]", quote_meta(chars))
            }
            Node::Range { not, lo, hi } => {
                let prefix = if *not { "^" } else { "" };
                format!(
                    "[{prefix}{}-{}]",
                    quote_meta(&lo.to_string()),
                    quote_meta(&hi.to_string())
                )
            }
        }
    }
}

/// `syntax/lexer/lexer.go`, reduced to the token stream the parser consumes.
fn lex(pattern: &str) -> Result<Vec<Token>, String> {
    Lexer {
        chars: pattern.chars().collect(),
        position: 0,
        terms_level: 0,
        tokens: Vec::new(),
        last_read_width: 0,
    }
    .run()
}

struct Lexer {
    chars: Vec<char>,
    position: usize,
    terms_level: usize,
    tokens: Vec<Token>,
    /// Width of the most recent successful [`Lexer::read`] (gobwas'
    /// `lastRuneSize`); `0` after hitting EOF, so `unread` is a no-op there.
    last_read_width: usize,
}

impl Lexer {
    fn run(mut self) -> Result<Vec<Token>, String> {
        loop {
            self.fetch_item()?;
            if self.tokens.last() == Some(&Token::Eof) {
                return Ok(self.tokens);
            }
        }
    }

    fn in_terms(&self) -> bool {
        self.terms_level > 0
    }

    fn read(&mut self) -> Option<char> {
        match self.chars.get(self.position).copied() {
            Some(c) => {
                self.position += 1;
                self.last_read_width = 1;
                Some(c)
            }
            None => {
                self.last_read_width = 0;
                None
            }
        }
    }

    fn peek(&self) -> Option<char> {
        self.chars.get(self.position).copied()
    }

    fn unread(&mut self) {
        self.position -= self.last_read_width;
        self.last_read_width = 0;
    }

    fn fetch_item(&mut self) -> Result<(), String> {
        match self.read() {
            None => self.tokens.push(Token::Eof),
            Some('{') => {
                self.terms_level += 1;
                self.tokens.push(Token::TermsOpen);
            }
            Some(',') if self.in_terms() => self.tokens.push(Token::Separator),
            Some('}') if self.in_terms() => {
                self.tokens.push(Token::TermsClose);
                self.terms_level -= 1;
            }
            Some('[') => {
                self.tokens.push(Token::RangeOpen);
                self.fetch_range()?;
            }
            Some('?') => self.tokens.push(Token::Single),
            Some('*') => {
                if self.read() == Some('*') {
                    self.tokens.push(Token::Super);
                } else {
                    self.unread();
                    self.tokens.push(Token::Any);
                }
            }
            Some(_) => {
                self.unread();
                let breakers: &[char] = if self.in_terms() {
                    &['?', '*', '[', '{', '}', ',']
                } else {
                    &['?', '*', '[', '{']
                };
                self.fetch_text(breakers);
            }
        }
        Ok(())
    }

    fn fetch_range(&mut self) -> Result<(), String> {
        let mut want_hi = false;
        let mut want_close = false;
        let mut seen_not = false;
        loop {
            let Some(r) = self.read() else {
                return Err("unexpected end of input".to_owned());
            };

            if want_close {
                if r != ']' {
                    return Err("expected close range character".to_owned());
                }
                self.tokens.push(Token::RangeClose);
                return Ok(());
            }

            if want_hi {
                self.tokens.push(Token::RangeHi(r));
                want_close = true;
                continue;
            }

            if !seen_not && r == '!' {
                self.tokens.push(Token::Not);
                seen_not = true;
                continue;
            }

            if self.peek() == Some('-') {
                self.position += 1;
                self.tokens.push(Token::RangeLo(r));
                self.tokens.push(Token::RangeBetween);
                want_hi = true;
                continue;
            }

            self.unread();
            self.fetch_text(&[']']);
            want_close = true;
        }
    }

    fn fetch_text(&mut self, breakers: &[char]) {
        let mut data = String::new();
        let mut escaped = false;
        while let Some(r) = self.read() {
            if !escaped {
                if r == '\\' {
                    escaped = true;
                    continue;
                }
                if breakers.contains(&r) {
                    self.unread();
                    break;
                }
            }
            escaped = false;
            data.push(r);
        }
        if !data.is_empty() {
            self.tokens.push(Token::Text(data));
        }
    }
}

/// `syntax/ast/parser.go`'s `parserMain`/`parserRange`.
struct Parser {
    tokens: Vec<Token>,
    position: usize,
}

/// Why [`Parser::parse_group`] returned: the token that terminated the group.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum GroupEnd {
    Eof,
    Separator,
    TermsClose,
}

impl Parser {
    fn next(&mut self) -> Token {
        let token = self
            .tokens
            .get(self.position)
            .cloned()
            .unwrap_or(Token::Eof);
        self.position += 1;
        token
    }

    fn parse_group(&mut self, tree: &mut Vec<Node>) -> Result<GroupEnd, String> {
        loop {
            match self.next() {
                Token::Eof => return Ok(GroupEnd::Eof),
                Token::Text(text) => tree.push(Node::Text(text)),
                Token::Any => tree.push(Node::Any),
                Token::Super => tree.push(Node::Super),
                Token::Single => tree.push(Node::Single),
                Token::RangeOpen => {
                    let node = self.parse_range()?;
                    tree.push(node);
                }
                Token::TermsOpen => {
                    let mut alternatives = Vec::new();
                    loop {
                        let mut pattern = Vec::new();
                        let end = self.parse_group(&mut pattern)?;
                        alternatives.push(Node::Pattern(pattern));
                        match end {
                            GroupEnd::Separator => continue,
                            GroupEnd::TermsClose => break,
                            // gobwas ends the whole parse at EOF, leaving the
                            // (possibly unterminated) alternation in place.
                            GroupEnd::Eof => {
                                tree.push(Node::AnyOf(alternatives));
                                return Ok(GroupEnd::Eof);
                            }
                        }
                    }
                    tree.push(Node::AnyOf(alternatives));
                }
                Token::Separator => return Ok(GroupEnd::Separator),
                Token::TermsClose => return Ok(GroupEnd::TermsClose),
                other => return Err(format!("unexpected token: {other:?}")),
            }
        }
    }

    fn parse_range(&mut self) -> Result<Node, String> {
        let mut not = false;
        let mut lo = None;
        let mut hi = None;
        let mut chars = String::new();

        loop {
            match self.next() {
                Token::Eof => return Err("unexpected end".to_owned()),
                Token::Not => not = true,
                Token::RangeLo(c) => lo = Some(c),
                Token::RangeBetween => {}
                Token::RangeHi(c) => {
                    if let Some(lo) = lo {
                        if c < lo {
                            return Err(format!(
                                "hi character '{c}' should be greater than lo '{lo}'"
                            ));
                        }
                    }
                    hi = Some(c);
                }
                Token::Text(text) => chars = text,
                Token::RangeClose => {
                    let is_range = lo.is_some() && hi.is_some();
                    let is_chars = !chars.is_empty();
                    if is_chars == is_range {
                        return Err("could not parse range".to_owned());
                    }
                    return match (lo, hi) {
                        (Some(lo), Some(hi)) if is_range => Ok(Node::Range { not, lo, hi }),
                        _ => Ok(Node::List { not, chars }),
                    };
                }
                other => return Err(format!("unexpected token: {other:?}")),
            }
        }
    }
}

/// One stored `package_repo_filters` row, reduced to what the matchers need
/// (the worker maps `sg_db::codeintel_dependencies::PackageRepoFilter` onto
/// this so this crate does not depend on the database layer).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FilterSpec {
    /// `"ALLOW"` or `"BLOCK"`; anything else is ignored by
    /// `NewFilterLists`, exactly like the baseline's `switch`.
    pub behaviour: String,
    pub package_scheme: String,
    /// `NameFilter.PackageGlob`.
    pub name_glob: Option<String>,
    /// `VersionFilter.PackageName` / `VersionFilter.VersionGlob`.
    pub version: Option<VersionFilterSpec>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VersionFilterSpec {
    pub package_name: String,
    pub version_glob: String,
}

#[derive(Debug, Clone)]
enum PackageMatcher {
    Name(CompiledGlob),
    Version {
        package_name: String,
        glob_str: String,
        glob: CompiledGlob,
    },
}

impl PackageMatcher {
    /// `PackageMatcher.Matches(pkg, version)`. A name matcher ignores the
    /// version; a version matcher requires the package name to match exactly.
    fn matches(&self, package: &str, version: &str) -> bool {
        match self {
            Self::Name(glob) => glob.is_match(package),
            Self::Version {
                package_name, glob, ..
            } => package == package_name && glob.is_match(version),
        }
    }
}

/// `packagefilters.PackageFilters`.
#[derive(Debug, Clone, Default)]
pub struct PackageFilters {
    allowlists: HashMap<String, Vec<PackageMatcher>>,
    blocklists: HashMap<String, Vec<PackageMatcher>>,
}

impl PackageFilters {
    /// `NewFilterLists`.
    pub fn new(filters: &[FilterSpec]) -> Result<Self, String> {
        let mut package_filters = Self::default();

        for filter in filters {
            let matcher = match (&filter.name_glob, &filter.version) {
                (Some(name_glob), _) => {
                    PackageMatcher::Name(CompiledGlob::compile(name_glob).map_err(|err| {
                        format!("error building glob matcher for {name_glob:?}: {err}")
                    })?)
                }
                (None, Some(version)) => PackageMatcher::Version {
                    package_name: version.package_name.clone(),
                    glob_str: version.version_glob.clone(),
                    glob: CompiledGlob::compile(&version.version_glob).map_err(|err| {
                        format!(
                            "error building glob matcher for {:?} {:?}: {err}",
                            version.package_name, version.version_glob
                        )
                    })?,
                },
                (None, None) => {
                    // The baseline would dereference a nil filter here.
                    return Err(
                        "package repo filter has neither a name nor a version matcher".to_owned(),
                    );
                }
            };

            match filter.behaviour.as_str() {
                "ALLOW" => package_filters
                    .allowlists
                    .entry(filter.package_scheme.clone())
                    .or_default()
                    .push(matcher),
                "BLOCK" => package_filters
                    .blocklists
                    .entry(filter.package_scheme.clone())
                    .or_default()
                    .push(matcher),
                _ => {}
            }
        }

        Ok(package_filters)
    }

    /// `IsPackageAllowed`: is the package as a whole (ignoring versions)
    /// allowed?
    pub fn is_package_allowed(&self, scheme: &str, package: &str) -> bool {
        let blocklist = self.blocklists.get(scheme);
        if let Some(blocklist) = blocklist {
            for block in blocklist {
                // Non-all-encompassing version globs don't apply to unversioned
                // packages; the baseline skips them here.
                if let PackageMatcher::Version { glob_str, .. } = block {
                    if glob_str != "*" {
                        continue;
                    }
                }
                if block.matches(package, "") {
                    return false;
                }
            }
        }

        // Allow if allow filters are empty (no restrictions), if any name
        // filter matches, or if any version filter applies to this name.
        let allowlist = self.allowlists.get(scheme);
        let mut is_allowed = allowlist.is_none();
        if let Some(allowlist) = allowlist {
            for allow in allowlist {
                is_allowed = is_allowed
                    || match allow {
                        PackageMatcher::Name(glob) => glob.is_match(package),
                        PackageMatcher::Version { package_name, .. } => package == package_name,
                    };
            }
        }
        is_allowed
    }

    /// `IsVersionedPackageAllowed`: is one specific version allowed?
    pub fn is_versioned_package_allowed(&self, scheme: &str, package: &str, version: &str) -> bool {
        if let Some(blocklist) = self.blocklists.get(scheme) {
            for block in blocklist {
                if block.matches(package, version) {
                    return false;
                }
            }
        }

        // By default anything is allowed unless a specific allowlist exists.
        let allowlist = self.allowlists.get(scheme);
        let mut is_allowed = allowlist.is_none();
        if let Some(allowlist) = allowlist {
            for allow in allowlist {
                is_allowed = is_allowed || allow.matches(package, version);
            }
        }
        is_allowed
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `glob_test.go`'s `Test_GlobToRegex` table.
    #[test]
    fn glob_to_regex_matches_the_baseline_table() {
        for (glob, expected) in [
            ("com.sourcegraph.*", r"^com\.sourcegraph\..*$"),
            ("xyz[a-w]", "^xyz[a-w]$"),
            ("asd[!f]", "^asd[^f]$"),
            ("b?n?n?", "^b.n.n.$"),
            ("asdf[!!]", "^asdf[^!]$"),
            ("*****", "^.*.*.*$"),
            ("!fdsa]", r"^!fdsa\]$"),
            (r"[\d]", "^[d]$"),
            ("{asd,abc}f", "^(?:asd|abc)f$"),
            ("asdf", "^asdf$"),
        ] {
            assert_eq!(glob_to_regex(glob).unwrap(), expected, "glob {glob:?}");
        }
    }

    #[test]
    fn glob_matching_uses_the_baseline_semantics() {
        let cases = [
            ("com.sourcegraph.*", "com.sourcegraph.foo", true),
            ("com.sourcegraph.*", "com.sourcegraph.foo.bar", true),
            ("com.sourcegraph.*", "com.othergraph.foo", false),
            ("b?n?n?", "banana", true),
            ("b?n?n?", "banan", false),
            ("asdf[!!]", "asdf!", false),
            ("asdf[!!]", "asdfx", true),
            ("{asd,abc}f", "abcf", true),
            ("{asd,abc}f", "asdf", true),
            ("{asd,abc}f", "asdff", false),
            ("*", "", true),
            ("*", "a\nb", true),
            ("asdf", "asdf", true),
            ("asdf", "asdf\n", false),
            (r"\*", "*", true),
            (r"\*", "x", false),
            ("**", "anything/at/all", true),
        ];
        for (glob, input, expected) in cases {
            let compiled = CompiledGlob::compile(glob).unwrap();
            assert_eq!(
                compiled.is_match(input),
                expected,
                "glob {glob:?} input {input:?}"
            );
        }
    }

    fn block_name(scheme: &str, name_glob: &str) -> FilterSpec {
        FilterSpec {
            behaviour: "BLOCK".to_owned(),
            package_scheme: scheme.to_owned(),
            name_glob: Some(name_glob.to_owned()),
            version: None,
        }
    }

    fn allow_name(scheme: &str, name_glob: &str) -> FilterSpec {
        FilterSpec {
            behaviour: "ALLOW".to_owned(),
            ..block_name(scheme, name_glob)
        }
    }

    fn block_version(scheme: &str, package: &str, version_glob: &str) -> FilterSpec {
        FilterSpec {
            behaviour: "BLOCK".to_owned(),
            package_scheme: scheme.to_owned(),
            name_glob: None,
            version: Some(VersionFilterSpec {
                package_name: package.to_owned(),
                version_glob: version_glob.to_owned(),
            }),
        }
    }

    fn allow_version(scheme: &str, package: &str, version_glob: &str) -> FilterSpec {
        FilterSpec {
            behaviour: "ALLOW".to_owned(),
            ..block_version(scheme, package, version_glob)
        }
    }

    #[test]
    fn block_only_filters_the_whole_package() {
        let filters = PackageFilters::new(&[block_name("npm", "ba*")]).unwrap();
        assert!(!filters.is_package_allowed("npm", "ba"));
        assert!(!filters.is_package_allowed("npm", "bax"));
        assert!(filters.is_package_allowed("npm", "abc"));
        // Versioned check: the name matcher ignores the version.
        assert!(!filters.is_versioned_package_allowed("npm", "ba", "1.0.0"));
        // Different scheme is untouched.
        assert!(filters.is_package_allowed("go", "ba"));
    }

    #[test]
    fn version_blocklists_do_not_block_unversioned_packages() {
        let filters = PackageFilters::new(&[block_version("npm", "ba", "1.0.[!1]")]).unwrap();
        // Unversioned package is not blocked by a non-`*` version glob.
        assert!(filters.is_package_allowed("npm", "ba"));
        // Versioned packages are.
        assert!(!filters.is_versioned_package_allowed("npm", "ba", "1.0.0"));
        assert!(filters.is_versioned_package_allowed("npm", "ba", "1.0.1"));
        // The package name must match exactly.
        assert!(filters.is_versioned_package_allowed("npm", "ba2", "1.0.0"));

        // A `*` version glob does block the unversioned package.
        let filters = PackageFilters::new(&[block_version("npm", "ba", "*")]).unwrap();
        assert!(!filters.is_package_allowed("npm", "ba"));
        assert!(filters.is_package_allowed("npm", "ba2"));
    }

    #[test]
    fn allowlists_restrict_packages_and_versions() {
        // The baseline's `TestPackageRepoFiltersBlockAllow` filters.
        let filters = PackageFilters::new(&[
            allow_version("rust-analyzer", "burger", "1.0.[!1]"),
            allow_version("rust-analyzer", "frogger", "3*"),
        ])
        .unwrap();

        // A version allowlist allows the package itself (a version filter
        // applies to this name) but not other names.
        assert!(filters.is_package_allowed("rust-analyzer", "burger"));
        assert!(filters.is_package_allowed("rust-analyzer", "frogger"));
        assert!(!filters.is_package_allowed("rust-analyzer", "frog"));
        // Other schemes have no filters at all.
        assert!(filters.is_package_allowed("semanticdb", "burger"));

        assert!(filters.is_versioned_package_allowed("rust-analyzer", "burger", "1.0.0"));
        assert!(!filters.is_versioned_package_allowed("rust-analyzer", "burger", "1.0.1"));
        assert!(filters.is_versioned_package_allowed("rust-analyzer", "burger", "1.0.2"));
        assert!(filters.is_versioned_package_allowed("rust-analyzer", "frogger", "3.0.0"));
        assert!(!filters.is_versioned_package_allowed("rust-analyzer", "frogger", "4.1.2"));

        // A name allowlist lets every version through for the allowed name.
        let filters =
            PackageFilters::new(&[block_name("npm", "ba*"), allow_name("npm", "foo")]).unwrap();
        assert!(filters.is_package_allowed("npm", "foo"));
        assert!(filters.is_versioned_package_allowed("npm", "foo", "1.0.0"));
        assert!(!filters.is_package_allowed("npm", "zzz"));
        // The blocklist wins over any allowlist entry.
        assert!(!filters.is_package_allowed("npm", "bar"));
        assert!(!filters.is_versioned_package_allowed("npm", "bar", "2.0.0"));
    }

    #[test]
    fn invalid_globs_are_reported() {
        for pattern in ["[a-]", "[]", "[a", "{"] {
            // `{` is accepted by gobwas (unterminated terms are tolerated).
            let result = glob_to_regex(pattern);
            if pattern == "{" {
                assert_eq!(result.unwrap(), "^(?:)$");
            } else {
                assert!(result.is_err(), "glob {pattern:?} should be invalid");
            }
        }
        assert!(PackageFilters::new(&[block_name("npm", "[a-]")]).is_err());
    }
}
