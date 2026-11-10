#![forbid(unsafe_code)]

//! `internal/search/zoekt/zoekt_global.go`: the static repository scope for a
//! global zoekt search (`DefaultGlobalQueryScope`) and the
//! [`GlobalZoektQuery`] wrapper that applies runtime private-repository
//! filters.
//!
//! The baseline builds `zoektquery` trees directly; this port constructs the
//! same [`Query`] nodes. `RawConfig` bit values and the `-repo` filter error
//! wrapping follow the Go implementation byte for byte (see the M9.8gy
//! overview).

use regex_syntax::ast::ErrorKind;

use crate::query::{
    self, Query, RC_NO_ARCHIVED, RC_NO_FORKS, RC_ONLY_ARCHIVED, RC_ONLY_FORKS, RC_ONLY_PUBLIC,
};

/// `query.RepoVisibility` (`internal/search/query/visibility.go`). The Go type
/// is a string; unknown values are represented by [`RepoVisibility::Other`].
/// [`default_global_query_scope`] only distinguishes `Public`/`Any` from
/// everything else.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RepoVisibility {
    /// `query.Any`.
    Any,
    /// `query.Private`.
    Private,
    /// `query.Public`.
    Public,
    /// Any value the baseline's `ParseVisibility` would not produce.
    Other(String),
}

impl Default for RepoVisibility {
    fn default() -> Self {
        // The Go zero value is `""`, which is neither Public nor Any.
        RepoVisibility::Other(String::new())
    }
}

impl RepoVisibility {
    /// The Go string value.
    pub fn as_str(&self) -> &str {
        match self {
            RepoVisibility::Any => "Any",
            RepoVisibility::Private => "Private",
            RepoVisibility::Public => "Public",
            RepoVisibility::Other(value) => value,
        }
    }
}

/// `search.RepoOptions`, reduced to the fields `DefaultGlobalQueryScope`
/// reads. `search/types.go:427-459`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RepoOptions {
    pub visibility: RepoVisibility,
    pub minus_repo_filters: Vec<String>,
    pub only_archived: bool,
    pub no_archived: bool,
    pub only_forks: bool,
    pub no_forks: bool,
}

/// `DefaultGlobalQueryScope` (`zoekt_global.go:16-47`).
///
/// Returns `None` when `visibility` is neither `Public` nor `Any`; otherwise
/// an `And` of a `HEAD` branch, a `RawConfig` scope (always `RcOnlyPublic`
/// plus the requested archived/fork bits), and one `Not(RepoRegexp)` per
/// `-repo` filter. The tree is not simplified, matching `zoektquery.NewAnd`.
pub fn default_global_query_scope(options: &RepoOptions) -> Result<Option<Query>, query::Error> {
    if !(matches!(
        &options.visibility,
        RepoVisibility::Public | RepoVisibility::Any
    )) {
        return Ok(None);
    }

    let mut raw_config = RC_ONLY_PUBLIC;
    if options.only_archived {
        raw_config |= RC_ONLY_ARCHIVED;
    }
    if options.no_archived {
        raw_config |= RC_NO_ARCHIVED;
    }
    if options.only_forks {
        raw_config |= RC_ONLY_FORKS;
    }
    if options.no_forks {
        raw_config |= RC_NO_FORKS;
    }

    let mut children = vec![
        Query::Branch {
            pattern: "HEAD".to_owned(),
            exact: true,
        },
        Query::RawConfig(raw_config),
    ];
    for pattern in &options.minus_repo_filters {
        let compiled = compile_repo_filter(pattern)?;
        children.push(Query::Not(Box::new(Query::RepoRegexp(compiled))));
    }
    Ok(Some(Query::and(children)))
}

/// `GlobalZoektQuery` (`zoekt_global.go:49-89`). Represents a query across
/// all zoekt replicas plus the repo scope siblings used to restrict it.
#[derive(Debug, Clone, PartialEq)]
pub struct GlobalZoektQuery {
    pub query: Query,
    pub repo_scope: Vec<Query>,
    pub include_private: bool,
}

impl GlobalZoektQuery {
    /// `NewGlobalZoektQuery`.
    pub fn new(query: Query, scope: Option<Query>, include_private: bool) -> Self {
        let mut repo_scope = Vec::new();
        if let Some(scope) = scope {
            repo_scope.push(scope);
        }
        Self {
            query,
            repo_scope,
            include_private,
        }
    }

    /// `ApplyPrivateFilter`: add the user's private repositories to the scope
    /// as one `HEAD` `BranchesRepos` node, but only when the construction
    /// allowed private repositories and the list is non-empty. Repeated calls
    /// append again (the baseline does not deduplicate).
    pub fn apply_private_filter(&mut self, repos: &[(u32, String)]) {
        if self.include_private && !repos.is_empty() {
            let ids: Vec<u32> = repos.iter().map(|(id, _)| *id).collect();
            self.repo_scope
                .push(Query::single_branches_repos("HEAD", &ids));
        }
    }

    /// `Generate`: `Simplify(And(Query, Or(RepoScope...)))`.
    pub fn generate(&self) -> Query {
        Query::and(vec![self.query.clone(), Query::or(self.repo_scope.clone())]).simplify()
    }
}

/// `regexp.Compile("(?i)" + pat)` for a `-repo` filter. On success returns the
/// source pattern `(?i)pat` (which Go's `syntax.Regexp.String()` prints
/// verbatim for the patterns this filter sees). On failure the error's
/// `Display` matches Go's `errors.Wrapf` text byte for byte.
fn compile_repo_filter(pattern: &str) -> Result<String, query::Error> {
    let source = format!("(?i){pattern}");

    // Go's parser detects trailing backslashes, stacked repetition operators
    // and invalid repeat counts inline, before/independently of the AST
    // checks below.
    if let Some((code, expr)) = scan_go_regexp_error(&source) {
        return Err(query::Error::InvalidRepoFilter {
            pattern: pattern.to_owned(),
            message: go_regexp_message(code, &expr),
        });
    }

    match regex_syntax::ast::parse::Parser::new().parse(&source) {
        Ok(_) => Ok(source),
        // Go treats a `{` that does not begin a valid counted repetition as a
        // literal (`a{`, `a{}`, `a{x}` are all valid).
        Err(err)
            if matches!(
                err.kind(),
                ErrorKind::RepetitionCountDecimalEmpty | ErrorKind::RepetitionCountUnclosed
            ) =>
        {
            Ok(source)
        }
        Err(err) => {
            let (code, expr) = map_ast_error(&source, &err);
            Err(query::Error::InvalidRepoFilter {
                pattern: pattern.to_owned(),
                message: go_regexp_message(code, &expr),
            })
        }
    }
}

/// `Error.Error()` from `regexp/syntax/parse.go`.
fn go_regexp_message(code: &str, expr: &str) -> String {
    format!("error parsing regexp: {code}: `{expr}`")
}

/// Inline checks that `regex-syntax` does not perform like Go's parser:
/// trailing backslash, nested repetition operators, and the 1000-repeat limit.
/// Returns the Go error code and offending expression.
fn scan_go_regexp_error(source: &str) -> Option<(&'static str, String)> {
    let bytes = source.as_bytes();
    let mut i = 0;
    // Byte range of the repetition operator attached to the previous atom.
    let mut last_repeat: Option<(usize, usize)> = None;
    while i < bytes.len() {
        match bytes[i] {
            b'\\' => {
                if i + 1 >= bytes.len() {
                    return Some(("trailing backslash at end of expression", String::new()));
                }
                i += 2;
                last_repeat = None;
            }
            b'[' => {
                // Skip a bracketed class; an unterminated one is reported by
                // the AST pass as `ClassUnclosed`.
                i += 1;
                while i < bytes.len() {
                    if bytes[i] == b'\\' {
                        i += 2;
                    } else if bytes[i] == b']' {
                        i += 1;
                        break;
                    } else {
                        i += 1;
                    }
                }
                last_repeat = None;
            }
            b'*' | b'+' | b'?' => {
                if let Some((start, end)) = last_repeat {
                    if end == i {
                        if bytes[i] == b'?' {
                            // A trailing `?` is the non-greedy modifier and
                            // remains part of the repetition.
                            last_repeat = Some((start, i + 1));
                        } else {
                            return Some((
                                "invalid nested repetition operator",
                                source[start..=i].to_owned(),
                            ));
                        }
                        i += 1;
                        continue;
                    }
                }
                last_repeat = Some((i, i + 1));
                i += 1;
            }
            b'{' => {
                let Some((end, min, max)) = parse_counted_repeat(source, i) else {
                    // Not a counted repetition: Go treats it as a literal.
                    last_repeat = None;
                    i += 1;
                    continue;
                };
                if let Some((start, repeat_end)) = last_repeat {
                    if repeat_end == i {
                        return Some((
                            "invalid nested repetition operator",
                            source[start..end].to_owned(),
                        ));
                    }
                }
                let too_large = min > 1000 || max.is_some_and(|max| max > 1000 || min > max);
                if too_large {
                    return Some(("invalid repeat count", source[i..end].to_owned()));
                }
                last_repeat = Some((i, end));
                i = end;
            }
            _ => {
                last_repeat = None;
                i += 1;
            }
        }
    }
    None
}

/// Parse `{n}`, `{n,}`, `{n,m}` at `start`; `None` when `start` does not begin
/// a syntactically valid counted repetition (Go then treats `{` as a literal).
fn parse_counted_repeat(source: &str, start: usize) -> Option<(usize, usize, Option<usize>)> {
    let bytes = source.as_bytes();
    let mut i = start + 1;
    let first_start = i;
    while i < bytes.len() && bytes[i].is_ascii_digit() {
        i += 1;
    }
    if i == first_start {
        return None;
    }
    let min: usize = source[first_start..i].parse().ok()?;
    let mut max = None;
    if i < bytes.len() && bytes[i] == b',' {
        i += 1;
        let second_start = i;
        while i < bytes.len() && bytes[i].is_ascii_digit() {
            i += 1;
        }
        if i > second_start {
            max = Some(source[second_start..i].parse().ok()?);
        }
    }
    if i >= bytes.len() || bytes[i] != b'}' {
        return None;
    }
    Some((i + 1, min, max))
}

/// Map a `regex-syntax` AST error to Go's error code and offending
/// expression. Go's `expr` frequently spans more (or less) than the Rust
/// span, so the raw source is sliced to match `regexp/syntax`.
fn map_ast_error(source: &str, err: &regex_syntax::ast::Error) -> (&'static str, String) {
    let start = err.span().start.offset.min(source.len());
    let end = err.span().end.offset.min(source.len());
    let spanned = &source[start..end];

    match err.kind() {
        // Go's `wholeClass` starts at `[` and runs to the end of input.
        ErrorKind::ClassUnclosed => ("missing closing ]", source[start..].to_owned()),
        // Go errors carry the whole expression (`p.wholeRegexp`).
        ErrorKind::GroupUnclosed => ("missing closing )", source.to_owned()),
        ErrorKind::GroupUnopened => ("unexpected )", source.to_owned()),
        ErrorKind::RepetitionMissing => {
            // `regex-syntax` reports an empty span just before the operator;
            // Go's `before[:len-before-after]` is the operator itself.
            let expr = if spanned.is_empty() {
                source[start..].chars().next().map(String::from)
            } else {
                Some(spanned.to_owned())
            }
            .unwrap_or_default();
            ("missing argument to repetition operator", expr)
        }
        ErrorKind::RepetitionCountInvalid => ("invalid repeat count", spanned.to_owned()),
        ErrorKind::EscapeUnrecognized
        | ErrorKind::ClassEscapeInvalid
        | ErrorKind::UnsupportedBackreference => ("invalid escape sequence", spanned.to_owned()),
        ErrorKind::EscapeUnexpectedEof
        | ErrorKind::EscapeHexEmpty
        | ErrorKind::EscapeHexInvalid
        | ErrorKind::EscapeHexInvalidDigit => {
            // Go reports the whole escape starting at the preceding `\`.
            let escape_start = source[..start].rfind('\\').unwrap_or(start);
            ("invalid escape sequence", source[escape_start..].to_owned())
        }
        ErrorKind::ClassRangeInvalid | ErrorKind::ClassRangeLiteral => {
            ("invalid character class range", spanned.to_owned())
        }
        ErrorKind::UnsupportedLookAround
        | ErrorKind::FlagUnrecognized
        | ErrorKind::FlagUnexpectedEof
        | ErrorKind::FlagDanglingNegation
        | ErrorKind::FlagDuplicate { .. }
        | ErrorKind::FlagRepeatedNegation { .. } => {
            ("invalid or unsupported Perl syntax", spanned.to_owned())
        }
        ErrorKind::GroupNameUnexpectedEof if source.ends_with("(?P<") => {
            // `(?P<` at EOF is rejected as Perl syntax, not a named capture.
            ("invalid or unsupported Perl syntax", "(?P".to_owned())
        }
        ErrorKind::GroupNameUnexpectedEof
        | ErrorKind::GroupNameEmpty
        | ErrorKind::GroupNameInvalid
        | ErrorKind::GroupNameDuplicate { .. } => {
            let group_start = source[..start].rfind("(?P").unwrap_or(start);
            ("invalid named capture", source[group_start..].to_owned())
        }
        _ => ("invalid or unsupported Perl syntax", spanned.to_owned()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn public_options() -> RepoOptions {
        RepoOptions {
            visibility: RepoVisibility::Public,
            ..RepoOptions::default()
        }
    }

    fn scope_display(options: &RepoOptions) -> Option<String> {
        default_global_query_scope(options)
            .expect("valid options")
            .map(|query| query.to_string())
    }

    #[test]
    fn public_and_any_have_a_scope() {
        let expected = "(and branch=\"HEAD\" rawConfig:RcOnlyPublic)";
        assert_eq!(scope_display(&public_options()).as_deref(), Some(expected));
        let any = RepoOptions {
            visibility: RepoVisibility::Any,
            ..RepoOptions::default()
        };
        assert_eq!(scope_display(&any).as_deref(), Some(expected));
    }

    #[test]
    fn other_visibilities_have_no_scope() {
        for visibility in [
            RepoVisibility::Private,
            RepoVisibility::Other("Frobnicate".to_owned()),
            RepoVisibility::Other(String::new()),
        ] {
            let options = RepoOptions {
                visibility,
                ..RepoOptions::default()
            };
            assert_eq!(scope_display(&options), None);
        }
    }

    #[test]
    fn applies_archived_and_fork_bits() {
        let options = RepoOptions {
            only_archived: true,
            no_archived: true,
            only_forks: true,
            no_forks: true,
            ..public_options()
        };
        assert_eq!(
            scope_display(&options).as_deref(),
            Some(
                "(and branch=\"HEAD\" \
                 rawConfig:RcOnlyPublic|RcOnlyForks|RcNoForks|RcOnlyArchived|RcNoArchived)"
            )
        );
    }

    #[test]
    fn compiles_minus_repo_filters_case_insensitively() {
        let options = RepoOptions {
            minus_repo_filters: vec!["Foo".to_owned(), "a.b|[c-d]+".to_owned()],
            ..public_options()
        };
        assert_eq!(
            scope_display(&options).as_deref(),
            Some(
                "(and branch=\"HEAD\" rawConfig:RcOnlyPublic \
                 (not reporegex:\"(?i)Foo\") (not reporegex:\"(?i)a.b|[c-d]+\"))"
            )
        );
    }

    #[test]
    fn reports_go_style_filter_errors() {
        let cases = [
            (
                "[",
                "invalid regex for -repo filter \"[\": error parsing regexp: \
                 missing closing ]: `[`",
            ),
            (
                "a[b",
                "invalid regex for -repo filter \"a[b\": error parsing regexp: \
                 missing closing ]: `[b`",
            ),
            (
                "(",
                "invalid regex for -repo filter \"(\": error parsing regexp: \
                 missing closing ): `(?i)(`",
            ),
            (
                ")",
                "invalid regex for -repo filter \")\": error parsing regexp: \
                 unexpected ): `(?i))`",
            ),
            (
                "*",
                "invalid regex for -repo filter \"*\": error parsing regexp: \
                 missing argument to repetition operator: `*`",
            ),
            (
                "a*+",
                "invalid regex for -repo filter \"a*+\": error parsing regexp: \
                 invalid nested repetition operator: `*+`",
            ),
            (
                "a{2,1}",
                "invalid regex for -repo filter \"a{2,1}\": error parsing regexp: \
                 invalid repeat count: `{2,1}`",
            ),
            (
                "a{1001}",
                "invalid regex for -repo filter \"a{1001}\": error parsing regexp: \
                 invalid repeat count: `{1001}`",
            ),
            (
                "\\",
                "invalid regex for -repo filter \"\\\\\": error parsing regexp: \
                 trailing backslash at end of expression: ``",
            ),
            (
                "\\C",
                "invalid regex for -repo filter \"\\\\C\": error parsing regexp: \
                 invalid escape sequence: `\\C`",
            ),
            (
                "[z-a]",
                "invalid regex for -repo filter \"[z-a]\": error parsing regexp: \
                 invalid character class range: `z-a`",
            ),
        ];
        for (pattern, expected) in cases {
            let options = RepoOptions {
                minus_repo_filters: vec![pattern.to_owned()],
                ..public_options()
            };
            let error = default_global_query_scope(&options).expect_err(pattern);
            assert_eq!(error.to_string(), expected, "pattern {pattern:?}");
        }
    }

    #[test]
    fn treats_valid_brace_literals_and_counts() {
        for pattern in ["a{", "a{}", "a{x}", "a{1000}", "a{1000,1000}", "a??", "a*?"] {
            let options = RepoOptions {
                minus_repo_filters: vec![pattern.to_owned()],
                ..public_options()
            };
            assert!(
                default_global_query_scope(&options).is_ok(),
                "pattern {pattern:?}"
            );
        }
    }

    #[test]
    fn new_puts_the_scope_in_the_repo_scope_list() {
        let query = Query::Substring {
            pattern: "needle".to_owned(),
            case_sensitive: false,
            file_name: false,
            content: true,
        };
        let none = GlobalZoektQuery::new(query.clone(), None, false);
        assert!(none.repo_scope.is_empty());
        let scope = Query::Branch {
            pattern: "HEAD".to_owned(),
            exact: true,
        };
        let some = GlobalZoektQuery::new(query, Some(scope.clone()), true);
        assert_eq!(some.repo_scope, vec![scope]);
        assert!(some.include_private);
    }

    #[test]
    fn apply_private_filter_respects_the_flag_and_empty_lists() {
        let query = Query::Const(true);
        let mut permitted = GlobalZoektQuery::new(query.clone(), None, true);
        permitted.apply_private_filter(&[]);
        assert!(permitted.repo_scope.is_empty());
        permitted.apply_private_filter(&[(5, "five".to_owned())]);
        assert_eq!(permitted.generate().to_string(), "(branchesrepos HEAD={5})");

        let mut denied = GlobalZoektQuery::new(query, None, false);
        denied.apply_private_filter(&[(5, "five".to_owned())]);
        assert!(denied.repo_scope.is_empty());
        assert_eq!(denied.generate(), Query::Const(false));
    }

    #[test]
    fn apply_private_filter_appends_on_every_call() {
        let query = Query::Const(true);
        let mut global = GlobalZoektQuery::new(query, None, true);
        let repos = [(1, "one".to_owned()), (2, "two".to_owned())];
        global.apply_private_filter(&repos);
        global.apply_private_filter(&repos);
        assert_eq!(global.repo_scope.len(), 2);
        assert_eq!(
            global.generate().to_string(),
            "(or (branchesrepos HEAD:2) (branchesrepos HEAD:2))"
        );
    }

    #[test]
    fn generate_flattens_a_single_scope() {
        let query = Query::Substring {
            pattern: "needle".to_owned(),
            case_sensitive: false,
            file_name: false,
            content: true,
        };
        let scope = Query::Branch {
            pattern: "HEAD".to_owned(),
            exact: true,
        };
        let global = GlobalZoektQuery::new(query, Some(scope), false);
        assert_eq!(
            global.generate().to_string(),
            "(and content_substr:\"needle\" branch=\"HEAD\")"
        );
    }

    #[test]
    fn generate_with_an_empty_scope_is_false() {
        let global = GlobalZoektQuery::new(Query::Const(true), None, false);
        assert_eq!(global.generate(), Query::Const(false));
    }
}
