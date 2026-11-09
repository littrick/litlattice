#![forbid(unsafe_code)]

//! Repository-filter helpers used by the insights query builder:
//! `internal/search/query/repo_revs.go` (`ParseRepositoryRevisions`),
//! `internal/search/query/types.go` (`Parameters.Repositories`) and
//! `internal/search/repos.ExactlyOneRepo`.
//!
//! `ParseRevisionSpecifier`'s `rat=` branch decodes a base64 JSON payload in
//! the baseline. Nothing in this slice inspects the decoded value (callers
//! only count revisions), so the raw payload is kept and the decode is
//! omitted; see docs §84.

use sg_search_query::fields::FIELD_REPO;
use sg_search_query::labels::Labels;
use sg_search_query::node::visit_field;
use sg_search_query::{Annotation, Parameter, Parameters};

/// `query.RevisionSpecifier`: at most one of the fields is set.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RevisionSpecifier {
    /// A revision range specifier suitable for git.
    pub rev_spec: String,
    /// A reference glob (`*glob`).
    pub ref_glob: String,
    /// A glob for references to exclude (`*!glob`).
    pub exclude_ref_glob: String,
    /// `rat=` revision-at-time payload. The baseline base64/JSON-decodes it;
    /// this port keeps the raw payload (only presence is observed).
    pub rev_at_time: Option<String>,
}

impl RevisionSpecifier {
    pub fn has_ref_glob(&self) -> bool {
        !self.ref_glob.is_empty() || !self.exclude_ref_glob.is_empty()
    }
}

/// `query.ParsedRepoFilter`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedRepoFilter {
    pub repo: String,
    pub revs: Vec<RevisionSpecifier>,
}

const REV_AT_TIME_PREFIX: &str = "rat=";

/// `query.ParseRevisionSpecifier`.
pub fn parse_revision_specifier(spec: &str) -> RevisionSpecifier {
    if let Some(rest) = spec.strip_prefix("*!") {
        return RevisionSpecifier {
            exclude_ref_glob: rest.to_owned(),
            ..RevisionSpecifier::default()
        };
    }
    if let Some(rest) = spec.strip_prefix('*') {
        return RevisionSpecifier {
            ref_glob: rest.to_owned(),
            ..RevisionSpecifier::default()
        };
    }
    if let Some(rest) = spec.strip_prefix(REV_AT_TIME_PREFIX) {
        return RevisionSpecifier {
            rev_at_time: Some(rest.to_owned()),
            ..RevisionSpecifier::default()
        };
    }
    RevisionSpecifier {
        rev_spec: spec.to_owned(),
        ..RevisionSpecifier::default()
    }
}

/// `query.ParseRepositoryRevisions`: split `repo@rev1:rev2` and reject a repo
/// pattern that is not a valid regular expression (compiled case-insensitively
/// in the baseline).
pub fn parse_repository_revisions(repo_and_optional_rev: &str) -> Result<ParsedRepoFilter, String> {
    let (repo, revs) = match repo_and_optional_rev.find('@') {
        None => (repo_and_optional_rev.to_owned(), Vec::new()),
        Some(i) => {
            let mut revs: Vec<RevisionSpecifier> = repo_and_optional_rev[i + 1..]
                .split(':')
                .filter(|part| !part.is_empty())
                .map(parse_revision_specifier)
                .collect();
            if revs.is_empty() {
                // The default branch.
                revs.push(RevisionSpecifier::default());
            }
            (repo_and_optional_rev[..i].to_owned(), revs)
        }
    };

    // The baseline compiles `(?i) + repo` with `regexp.Compile`; a parser
    // error is the only thing observed by callers.
    regex_syntax::ast::parse::Parser::new()
        .parse(&format!("(?i){repo}"))
        .map_err(|e| e.to_string())?;

    Ok(ParsedRepoFilter { repo, revs })
}

/// `Parameters.Repositories`: collect non-predicate, non-negated repo filters
/// (parsed) and negated repo values.
pub fn repositories(parameters: &Parameters) -> (Vec<ParsedRepoFilter>, Vec<String>) {
    let mut repos = Vec::new();
    let mut negated_repos = Vec::new();
    let nodes = parameters.to_nodes();
    visit_field(
        &nodes,
        FIELD_REPO,
        &mut |value: &str, negated: bool, annotation: &Annotation| {
            if annotation.labels.is_set(Labels::IS_PREDICATE) {
                return;
            }
            if negated {
                negated_repos.push(value.to_owned());
            } else {
                match parse_repository_revisions(value) {
                    Ok(filter) => repos.push(filter),
                    // The baseline panics: the repo field was already
                    // validated when the plan was built.
                    Err(err) => panic!("repo field {value:?} is an invalid regex: {err}"),
                }
            }
        },
    );
    (repos, negated_repos)
}

/// `searchrepos.ExactlyOneRepo`: exactly one repo filter, anchored with `^`
/// and `$`, whose body parses to a literal regex.
pub fn exactly_one_repo(repo_filters: &[ParsedRepoFilter]) -> bool {
    if let [filter] = repo_filters {
        let repo = &filter.repo;
        if repo.starts_with('^') && repo.ends_with('$') && repo.len() >= 2 {
            let body = &repo[1..repo.len() - 1];
            return is_literal_regex(body);
        }
    }
    false
}

/// Whether `pattern` parses to a plain literal (the baseline checks that
/// `regexp/syntax.Parse` yields `OpLiteral`). Inline flags such as `(?i)` keep
/// the literal shape, matching Go's case-folded `OpLiteral`.
fn is_literal_regex(pattern: &str) -> bool {
    use regex_syntax::ast::{parse::Parser, Ast};

    fn walk(ast: &Ast, has_literal: &mut bool) -> bool {
        match ast {
            Ast::Literal(_) => {
                *has_literal = true;
                true
            }
            Ast::Flags(_) => true,
            Ast::Concat(concat) => concat.asts.iter().all(|ast| walk(ast, has_literal)),
            _ => false,
        }
    }

    match Parser::new().parse(pattern) {
        Ok(ast) => {
            let mut has_literal = false;
            walk(&ast, &mut has_literal) && has_literal
        }
        Err(_) => false,
    }
}

/// Convenience constructor for the filter parameters this crate builds.
pub(crate) fn parameter(field: &str, value: impl Into<String>, negated: bool) -> Parameter {
    Parameter {
        field: field.to_owned(),
        value: value.into(),
        negated,
        annotation: Annotation::default(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_repo_without_rev() {
        let filter = parse_repository_revisions("github.com/foo/bar").unwrap();
        assert_eq!(filter.repo, "github.com/foo/bar");
        assert!(filter.revs.is_empty());
    }

    #[test]
    fn parses_repo_with_revs() {
        let filter = parse_repository_revisions("foo@v1:v2").unwrap();
        assert_eq!(filter.repo, "foo");
        assert_eq!(filter.revs.len(), 2);
        assert_eq!(filter.revs[0].rev_spec, "v1");
        assert_eq!(filter.revs[1].rev_spec, "v2");
    }

    #[test]
    fn empty_revs_after_at_is_the_default_branch() {
        let filter = parse_repository_revisions("foo@").unwrap();
        assert_eq!(filter.revs, vec![RevisionSpecifier::default()]);
    }

    #[test]
    fn invalid_regex_is_rejected() {
        assert!(parse_repository_revisions("foo(").is_err());
    }

    #[test]
    fn exactly_one_repo_requires_anchored_literal() {
        assert!(exactly_one_repo(&[parse_repository_revisions(
            "^github\\.com/a/b$"
        )
        .unwrap()]));
        // `.` parses to a character class, so this is not a literal.
        assert!(!exactly_one_repo(&[parse_repository_revisions(
            "^github.com/a/b$"
        )
        .unwrap()]));
        assert!(!exactly_one_repo(&[
            parse_repository_revisions("^(a|b)$").unwrap()
        ]));
        assert!(!exactly_one_repo(&[
            parse_repository_revisions("^a.+$").unwrap()
        ]));
        assert!(!exactly_one_repo(&[
            parse_repository_revisions("a$").unwrap()
        ]));
        assert!(!exactly_one_repo(&[]));
    }
}
