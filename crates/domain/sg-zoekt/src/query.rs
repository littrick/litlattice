#![forbid(unsafe_code)]

//! The zoekt query tree (`github.com/sourcegraph/zoekt/query`), its
//! `Simplify` pass and the proto representation used on the wire.

use std::collections::{BTreeMap, BTreeSet};

use sg_search_query::basic::{Basic, RepoHasFileContentArgs};
use sg_search_query::fields::{FIELD_FILE, FIELD_LANG};
use sg_search_query::labels::Labels;
use sg_search_query::node::{Node, OperatorKind};

use crate::go_regexp::{self, Literal};
use crate::languages;
use crate::proto;
use crate::proto::q;

/// `query.ZoektScoreBoost` (`internal/search/query/helpers.go`).
pub const ZOEKT_SCORE_BOOST: f64 = 20.0;

// `zoektquery.RawConfig` flags (`zoekt/query/query.go:42-49`).
/// `query.RcOnlyPublic`.
pub const RC_ONLY_PUBLIC: u64 = 1;
/// `query.RcOnlyPrivate`.
pub const RC_ONLY_PRIVATE: u64 = 1 << 1;
/// `query.RcOnlyForks`.
pub const RC_ONLY_FORKS: u64 = 1 << 2;
/// `query.RcNoForks`.
pub const RC_NO_FORKS: u64 = 2 << 2;
/// `query.RcOnlyArchived`.
pub const RC_ONLY_ARCHIVED: u64 = 1 << 4;
/// `query.RcNoArchived`.
pub const RC_NO_ARCHIVED: u64 = 2 << 4;

/// `query.BranchRepos`: a branch plus the Sourcegraph repo IDs to match.
/// `repos` is the portable-roaring serialization z oekt expects on the wire.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BranchRepos {
    pub branch: String,
    pub repos: Vec<u8>,
}

impl BranchRepos {
    /// `query.NewSingleBranchesRepos`: one branch and the given repo IDs.
    pub fn single_branch(branch: impl Into<String>, repo_ids: &[u32]) -> Self {
        let bitmap: roaring::RoaringBitmap = repo_ids.iter().copied().collect();
        let mut repos = Vec::new();
        bitmap
            .serialize_into(&mut repos)
            .expect("serializing a roaring bitmap into a Vec cannot fail");
        Self {
            branch: branch.into(),
            repos,
        }
    }
}

/// Result type selector (`query.Type`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TypeKind {
    FileMatch,
    FileName,
    Repo,
}

/// Baseline `query.Q`. Only the nodes exercised by Sourcegraph are modeled;
/// the enum is closed so unknown proto nodes are representable for round trips.
#[derive(Debug, Clone, PartialEq)]
pub enum Query {
    RawConfig(u64),
    Regexp {
        regexp: String,
        file_name: bool,
        content: bool,
        case_sensitive: bool,
    },
    Symbol(Box<Query>),
    Language(String),
    Const(bool),
    /// `query.Repo`: a regular expression over repository names.
    Repo(String),
    RepoRegexp(String),
    BranchesRepos(Vec<BranchRepos>),
    /// Serialized roaring bitmap of repo IDs (`query.RepoIDs`).
    RepoIds(Vec<u8>),
    RepoSet(BTreeMap<String, bool>),
    FileNameSet(BTreeSet<String>),
    Type {
        child: Box<Query>,
        kind: TypeKind,
    },
    Substring {
        pattern: String,
        case_sensitive: bool,
        file_name: bool,
        content: bool,
    },
    And(Vec<Query>),
    Or(Vec<Query>),
    Not(Box<Query>),
    Branch {
        pattern: String,
        exact: bool,
    },
    Boost {
        child: Box<Query>,
        boost: f64,
    },
}

impl Query {
    /// `query.NewAnd`.
    pub fn and(children: Vec<Query>) -> Self {
        Self::And(children)
    }

    /// `query.NewOr`.
    pub fn or(children: Vec<Query>) -> Self {
        Self::Or(children)
    }

    /// `query.NewNot`.
    pub fn negate(child: Query) -> Self {
        Self::Not(Box::new(child))
    }

    /// `query.NewSingleBranchesRepos`.
    pub fn single_branches_repos(branch: impl Into<String>, repo_ids: &[u32]) -> Self {
        Self::BranchesRepos(vec![BranchRepos::single_branch(branch, repo_ids)])
    }

    /// `query.NewRepoSet`.
    pub fn repo_set(repos: impl IntoIterator<Item = String>) -> Self {
        Self::RepoSet(repos.into_iter().map(|repo| (repo, true)).collect())
    }

    /// `query.NewFileNameSet`.
    pub fn file_name_set(names: impl IntoIterator<Item = String>) -> Self {
        Self::FileNameSet(names.into_iter().collect())
    }

    /// `query.Simplify`: constant folding followed by flattening of nested
    /// `And`/`Or` nodes until the tree stops changing.
    pub fn simplify(self) -> Query {
        let mut query = eval_constants(self);
        loop {
            let (next, changed) = flatten(query);
            query = next;
            if !changed {
                return query;
            }
        }
    }
}

/// `query.evalAndOrConstants`.
fn eval_and_or_constants(children: Vec<Query>, is_and: bool) -> Query {
    let children: Vec<Query> = children.into_iter().map(eval_constants).collect();

    let mut kept = Vec::with_capacity(children.len());
    for child in children {
        if let Query::Const(value) = child {
            if value == is_and {
                // `true` under AND, `false` under OR: drop the constant.
                continue;
            }
            // `false` under AND, `true` under OR: the whole node folds.
            return child;
        }
        kept.push(child);
    }
    if kept.is_empty() {
        return Query::Const(is_and);
    }
    if is_and {
        Query::And(kept)
    } else {
        Query::Or(kept)
    }
}

/// `query.evalConstants`.
fn eval_constants(query: Query) -> Query {
    match query {
        Query::And(children) => eval_and_or_constants(children, true),
        Query::Or(children) => eval_and_or_constants(children, false),
        Query::Not(child) => {
            let child = eval_constants(*child);
            match child {
                Query::Const(value) => Query::Const(!value),
                other => Query::Not(Box::new(other)),
            }
        }
        Query::Type { child, kind } => {
            let child = eval_constants(*child);
            match child {
                // Baseline folds any const child, changing the result type at
                // the root; reproduced as-is.
                Query::Const(value) => Query::Const(value),
                other => Query::Type {
                    child: Box::new(other),
                    kind,
                },
            }
        }
        Query::Boost { child, boost } => {
            let child = eval_constants(*child);
            match child {
                Query::Const(_) => child,
                other => Query::Boost {
                    child: Box::new(other),
                    boost,
                },
            }
        }
        Query::Substring { pattern, .. } if pattern.is_empty() => Query::Const(true),
        Query::Regexp { regexp, .. } if regexp.is_empty() => Query::Const(true),
        Query::Branch { pattern, .. } if pattern.is_empty() => Query::Const(true),
        Query::RepoSet(set) if set.is_empty() => Query::Const(false),
        Query::FileNameSet(set) if set.is_empty() => Query::Const(false),
        other => other,
    }
}

/// `query.flattenAndOr`: splice same-typed children one level up.
fn flatten_and_or(children: Vec<Query>, is_and: bool) -> (Vec<Query>, bool) {
    let mut flat = Vec::new();
    let mut changed = false;
    for child in children {
        let (child, sub_changed) = flatten(child);
        changed |= sub_changed;
        let same_type = matches!(
            (&child, is_and),
            (Query::And(_), true) | (Query::Or(_), false)
        );
        if same_type {
            changed = true;
            if let Query::And(sub) | Query::Or(sub) = child {
                flat.extend(sub);
            }
        } else {
            flat.push(child);
        }
    }
    (flat, changed)
}

/// `query.flatten`.
fn flatten(query: Query) -> (Query, bool) {
    match query {
        Query::And(children) => {
            if children.len() == 1 {
                return (children.into_iter().next().expect("one child"), true);
            }
            let (flat, changed) = flatten_and_or(children, true);
            (Query::And(flat), changed)
        }
        Query::Or(children) => {
            if children.len() == 1 {
                return (children.into_iter().next().expect("one child"), true);
            }
            let (flat, changed) = flatten_and_or(children, false);
            (Query::Or(flat), changed)
        }
        Query::Not(child) => {
            let (child, changed) = flatten(*child);
            (Query::Not(Box::new(child)), changed)
        }
        Query::Type { child, kind } => {
            let (child, changed) = flatten(*child);
            (
                Query::Type {
                    child: Box::new(child),
                    kind,
                },
                changed,
            )
        }
        Query::Boost { child, boost } => {
            let (child, changed) = flatten(*child);
            (
                Query::Boost {
                    child: Box::new(child),
                    boost,
                },
                changed,
            )
        }
        other => (other, false),
    }
}

// -------------------------------------------------- query conversion
//
// `internal/search/zoekt/query.go`: `QueryToZoektQuery` and its helpers.

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::InvalidRegexp { pattern, message } => {
                write!(f, "invalid regexp {pattern:?}: {message}")
            }
            Error::InvalidRepoFilter { pattern, message } => {
                // `errors.Wrapf(err, "invalid regex for -repo filter %q", pat)`.
                write!(
                    f,
                    "invalid regex for -repo filter {}: {message}",
                    go_quote(pattern)
                )
            }
            Error::Predicate(message) => write!(f, "{message}"),
            Error::BrokenInvariant(message) => write!(f, "broken invariant: {message}"),
        }
    }
}

impl std::error::Error for Error {}

impl Error {
    fn invalid_regexp(pattern: &str, message: impl Into<String>) -> Error {
        Error::InvalidRegexp {
            pattern: pattern.to_owned(),
            message: message.into(),
        }
    }
}

/// Failure converting a Sourcegraph query to a zoekt query.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    /// `regexp/syntax.Parse` rejected a pattern.
    InvalidRegexp { pattern: String, message: String },
    /// `DefaultGlobalQueryScope` rejected a `-repo` filter; wraps the Go
    /// regexp syntax error with the `errors.Wrapf` message.
    InvalidRepoFilter { pattern: String, message: String },
    /// A typed predicate's arguments did not parse. The baseline panics; the
    /// port propagates the error instead.
    Predicate(String),
    /// A node kind the conversion does not handle (`toZoektPattern`'s
    /// unreachable branches).
    BrokenInvariant(String),
}

/// `result.Types` bitset. Only the bits the conversion observes are modeled.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ResultTypes(u16);

impl ResultTypes {
    /// `result.TypeRepo`.
    pub const REPO: u16 = 1;
    /// `result.TypeSymbol`.
    pub const SYMBOL: u16 = 2;
    /// `result.TypeFile`.
    pub const FILE: u16 = 4;
    /// `result.TypePath`.
    pub const PATH: u16 = 8;
    /// `result.TypeDiff`.
    pub const DIFF: u16 = 16;
    /// `result.TypeCommit`.
    pub const COMMIT: u16 = 32;
    /// `result.TypeStructural`.
    pub const STRUCTURAL: u16 = 64;

    /// `Types.Has`.
    pub fn has(self, kind: u16) -> bool {
        self.0 & kind != 0
    }

    /// `Types.With`.
    pub fn with(self, kind: u16) -> ResultTypes {
        ResultTypes(self.0 | kind)
    }

    /// The default when a query has no `type:` filters:
    /// `TypeFile | TypePath | TypeRepo`.
    pub fn file_path_repo() -> ResultTypes {
        ResultTypes(Self::FILE | Self::PATH | Self::REPO)
    }

    /// `type:file` with no path matches.
    pub fn file_only() -> ResultTypes {
        ResultTypes(Self::FILE)
    }

    /// `type:path` with no content matches.
    pub fn path_only() -> ResultTypes {
        ResultTypes(Self::PATH)
    }
}

/// `search.Features` bits the conversion reads.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Features {
    /// `ContentBasedLangFilters`: use zoekt's native `Language` filters instead
    /// of file regexps.
    pub content_based_lang_filters: bool,
}

/// `search.IndexedRequestType`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum IndexedRequestType {
    /// `search.TextRequest`.
    #[default]
    Text,
    /// `search.SymbolRequest`.
    Symbol,
}

/// `QueryToZoektQuery`: convert one basic query to a zoekt query.
///
/// Result types drive whether a pattern matches file paths, content, or both
/// (see `toZoektPattern`). Excluded languages and files become `Not` nodes;
/// `repo:has.*` filters become typed (repo-scoped) child queries.
pub fn from_basic(
    basic: &Basic,
    result_types: ResultTypes,
    features: &Features,
    indexed_request_type: IndexedRequestType,
) -> Result<Query, Error> {
    let is_case_sensitive = basic.is_case_sensitive();

    let mut and = Vec::new();
    if let Some(pattern) = &basic.pattern {
        and.push(to_zoekt_pattern(
            pattern,
            is_case_sensitive,
            result_types.has(ResultTypes::FILE),
            result_types.has(ResultTypes::PATH),
            indexed_request_type,
        )?);
    }

    // `file:` and `-file:` filters.
    let (files_include, files_exclude) = basic.include_exclude_values(FIELD_FILE);

    // `lang:` and `-lang:` filters. By default a language becomes a
    // case-insensitive file regexp; with content-based detection it becomes a
    // native zoekt `Language` filter.
    let (lang_include, lang_exclude) = basic.include_exclude_values(FIELD_LANG);
    if features.content_based_lang_filters {
        for lang in &lang_include {
            and.push(to_lang_filter(lang));
        }
        for lang in &lang_exclude {
            and.push(Query::Not(Box::new(to_lang_filter(lang))));
        }
    } else {
        let include: Vec<String> = lang_include
            .iter()
            .map(|lang| languages::lang_to_file_regexp(lang))
            .collect();
        let exclude: Vec<String> = lang_exclude
            .iter()
            .map(|lang| languages::lang_to_file_regexp(lang))
            .collect();
        and.extend(to_file_filters(&include, &exclude, false)?);
    }

    and.extend(to_file_filters(
        &files_include,
        &files_exclude,
        is_case_sensitive,
    )?);

    let mut repo_has_filters = Vec::new();
    for filter in basic.repo_has_file_content().map_err(Error::Predicate)? {
        repo_has_filters.push(query_for_file_content_args(&filter, is_case_sensitive));
    }
    if !repo_has_filters.is_empty() {
        and.push(Query::And(repo_has_filters));
    }

    Ok(Query::And(and).simplify())
}

/// `toLangFilter`.
fn to_lang_filter(lang: &str) -> Query {
    Query::Language(languages::get_language_by_name_or_alias(lang).to_owned())
}

/// `toFileFilters`: convert file regexp patterns to zoekt file filters.
fn to_file_filters(
    files_include: &[String],
    files_exclude: &[String],
    is_case_sensitive: bool,
) -> Result<Vec<Query>, Error> {
    let mut filters = Vec::new();
    for pattern in files_include {
        filters.push(parse_re(pattern, true, false, is_case_sensitive)?);
    }
    if !files_exclude.is_empty() {
        let union = languages::union_regexps(files_exclude);
        let query = parse_re(&union, true, false, is_case_sensitive)?;
        filters.push(Query::Not(Box::new(query)));
    }
    Ok(filters)
}

/// `QueryForFileContentArgs`: path/content regexps scoped to repository
/// matches, optionally negated.
pub fn query_for_file_content_args(opt: &RepoHasFileContentArgs, case_sensitive: bool) -> Query {
    let mut children = Vec::new();
    if !opt.path.is_empty() {
        children.push(Query::Regexp {
            regexp: opt.path.clone(),
            file_name: true,
            content: false,
            case_sensitive,
        });
    }
    if !opt.content.is_empty() {
        children.push(Query::Regexp {
            regexp: opt.content.clone(),
            file_name: false,
            content: true,
            case_sensitive,
        });
    }
    let mut query = Query::Type {
        child: Box::new(Query::And(children)),
        kind: TypeKind::Repo,
    };
    if opt.negated {
        query = Query::Not(Box::new(query));
    }
    query.simplify()
}

/// `toZoektPattern`: fold a pattern AST into a zoekt query.
fn to_zoekt_pattern(
    expression: &Node,
    is_case_sensitive: bool,
    pattern_matches_content: bool,
    pattern_matches_path: bool,
    typ: IndexedRequestType,
) -> Result<Query, Error> {
    match expression {
        Node::Operator(operator) => {
            let mut children = Vec::with_capacity(operator.operands.len());
            for operand in &operator.operands {
                children.push(to_zoekt_pattern(
                    operand,
                    is_case_sensitive,
                    pattern_matches_content,
                    pattern_matches_path,
                    typ,
                )?);
            }
            match operator.kind {
                OperatorKind::Or => Ok(Query::Or(children)),
                OperatorKind::And => Ok(Query::And(children)),
                OperatorKind::Concat => Err(Error::BrokenInvariant(format!(
                    "don't know what to do with node {:?} in toZoektPattern",
                    operator.kind
                ))),
            }
        }
        Node::Pattern(pattern) => {
            let file_name_only = pattern_matches_path && !pattern_matches_content;
            let content_only = !pattern_matches_path && pattern_matches_content;

            let mut query = parse_re(
                &pattern.regexp_pattern(),
                file_name_only,
                content_only,
                is_case_sensitive,
            )?;

            if typ == IndexedRequestType::Symbol {
                query = Query::Symbol(Box::new(query));
            }
            if pattern.negated {
                query = Query::Not(Box::new(query));
            }
            if pattern.annotation.labels.is_set(Labels::BOOST) {
                query = Query::Boost {
                    child: Box::new(query),
                    boost: ZOEKT_SCORE_BOOST,
                };
            }
            Ok(query)
        }
        Node::Parameter(_) => Err(Error::BrokenInvariant(
            "don't know what to do with node Parameter in toZoektPattern".to_owned(),
        )),
    }
}

/// `zoektquery.ParseRe`: parse a pattern and classify it as a substring when
/// the Go regexp parser would produce `OpLiteral`. Literal detection happens
/// after `OptimizeRegexp`, which is why capturing groups can still become
/// substrings.
fn parse_re(
    pattern: &str,
    file_name_only: bool,
    content_only: bool,
    case_sensitive: bool,
) -> Result<Query, Error> {
    match go_regexp::parse(pattern) {
        Ok(Literal::Empty) => Ok(Query::Regexp {
            regexp: String::new(),
            file_name: file_name_only,
            content: content_only,
            case_sensitive,
        }),
        Ok(Literal::Literal { text, .. }) => Ok(Query::Substring {
            pattern: text,
            case_sensitive,
            file_name: file_name_only,
            content: content_only,
        }),
        Ok(Literal::NonLiteral) => {
            go_regexp::validate(pattern)
                .map_err(|message| Error::invalid_regexp(pattern, message))?;
            Ok(Query::Regexp {
                regexp: pattern.to_owned(),
                file_name: file_name_only,
                content: content_only,
                case_sensitive,
            })
        }
        Err(go_regexp::InvalidSyntax) => {
            let message = go_regexp::validate(pattern)
                .err()
                .unwrap_or_else(|| "invalid regular expression".to_owned());
            Err(Error::invalid_regexp(pattern, message))
        }
    }
}

// ---------------------------------------------------------------- protobuf

/// `query.QToProto`.
pub fn to_proto(query: &Query) -> proto::Q {
    let value = match query {
        Query::RawConfig(flags) => q::Query::RawConfig(proto::RawConfig {
            flags: raw_config_flags(*flags),
        }),
        Query::Regexp {
            regexp,
            file_name,
            content,
            case_sensitive,
        } => q::Query::Regexp(proto::Regexp {
            regexp: regexp.clone(),
            file_name: *file_name,
            content: *content,
            case_sensitive: *case_sensitive,
        }),
        Query::Symbol(child) => q::Query::Symbol(Box::new(proto::Symbol {
            expr: Some(Box::new(to_proto(child))),
        })),
        Query::Language(language) => q::Query::Language(proto::Language {
            language: language.clone(),
        }),
        Query::Const(value) => q::Query::Const(*value),
        Query::Repo(regexp) => q::Query::Repo(proto::Repo {
            regexp: regexp.clone(),
        }),
        Query::RepoRegexp(regexp) => q::Query::RepoRegexp(proto::RepoRegexp {
            regexp: regexp.clone(),
        }),
        Query::BranchesRepos(list) => q::Query::BranchesRepos(proto::BranchesRepos {
            list: list
                .iter()
                .map(|branch| proto::BranchRepos {
                    branch: branch.branch.clone(),
                    repos: branch.repos.clone(),
                })
                .collect(),
        }),
        Query::RepoIds(repos) => q::Query::RepoIds(proto::RepoIds {
            repos: repos.clone(),
        }),
        Query::RepoSet(set) => q::Query::RepoSet(proto::RepoSet {
            set: set.iter().map(|(repo, ok)| (repo.clone(), *ok)).collect(),
        }),
        Query::FileNameSet(set) => q::Query::FileNameSet(proto::FileNameSet {
            set: set.iter().cloned().collect(),
        }),
        Query::Type { child, kind } => q::Query::Type(Box::new(proto::Type {
            child: Some(Box::new(to_proto(child))),
            r#type: match kind {
                TypeKind::FileMatch => proto::r#type::Kind::FileMatch,
                TypeKind::FileName => proto::r#type::Kind::FileName,
                TypeKind::Repo => proto::r#type::Kind::Repo,
            } as i32,
        })),
        Query::Substring {
            pattern,
            case_sensitive,
            file_name,
            content,
        } => q::Query::Substring(proto::Substring {
            pattern: pattern.clone(),
            case_sensitive: *case_sensitive,
            file_name: *file_name,
            content: *content,
        }),
        Query::And(children) => q::Query::And(proto::And {
            children: children.iter().map(to_proto).collect(),
        }),
        Query::Or(children) => q::Query::Or(proto::Or {
            children: children.iter().map(to_proto).collect(),
        }),
        Query::Not(child) => q::Query::Not(Box::new(proto::Not {
            child: Some(Box::new(to_proto(child))),
        })),
        Query::Branch { pattern, exact } => q::Query::Branch(proto::Branch {
            pattern: pattern.clone(),
            exact: *exact,
        }),
        Query::Boost { child, boost } => q::Query::Boost(Box::new(proto::Boost {
            child: Some(Box::new(to_proto(child))),
            boost: *boost,
        })),
    };
    proto::Q { query: Some(value) }
}

fn raw_config_flags(flags: u64) -> Vec<i32> {
    // `query.RawConfig` bit values from zoekt; the proto enum uses the same
    // masks.
    let mut out = Vec::new();
    for (mask, flag) in [
        (0x01, proto::raw_config::Flag::OnlyPublic),
        (0x02, proto::raw_config::Flag::OnlyPrivate),
        (0x04, proto::raw_config::Flag::OnlyForks),
        (0x08, proto::raw_config::Flag::NoForks),
        (0x10, proto::raw_config::Flag::OnlyArchived),
        (0x20, proto::raw_config::Flag::NoArchived),
    ] {
        if flags & mask != 0 {
            out.push(flag as i32);
        }
    }
    out
}

// ------------------------------------------------------------- formatting
//
// `github.com/sourcegraph/zoekt/query`'s `Q.String()` implementations. Only
// used by the metered searcher's `queryString` label. Notes:
//
// * `Regexp` stores the source pattern, while Go prints
//   `syntaxutil.RegexpString` (the `regexp/syntax` canonical form). For the
//   simple patterns in the differential corpus the two agree; exotic patterns
//   may differ.
// * `RepoIds`/`BranchesRepos` print a structural approximation because Go
//   delegates to `roaring.Bitmap.String()`.

impl std::fmt::Display for Query {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Query::RawConfig(flags) => {
                let mut labels = Vec::new();
                for (mask, label) in [
                    (0x01u64, "RcOnlyPublic"),
                    (0x02, "RcOnlyPrivate"),
                    (0x04, "RcOnlyForks"),
                    (0x08, "RcNoForks"),
                    (0x10, "RcOnlyArchived"),
                    (0x20, "RcNoArchived"),
                ] {
                    if flags & mask != 0 {
                        labels.push(label);
                    }
                }
                write!(f, "rawConfig:{}", labels.join("|"))
            }
            Query::Regexp {
                regexp,
                file_name,
                case_sensitive,
                ..
            } => {
                let mut prefix = String::new();
                if *file_name {
                    prefix.push_str("file_");
                }
                if *case_sensitive {
                    prefix.insert_str(0, "case_");
                }
                write!(f, "{prefix}regex:{}", go_quote(regexp))
            }
            Query::Symbol(child) => write!(f, "sym:{child}"),
            Query::Language(language) => write!(f, "lang:{language}"),
            Query::Const(value) => f.write_str(if *value { "TRUE" } else { "FALSE" }),
            Query::Repo(regexp) => write!(f, "repo:{regexp}"),
            Query::RepoRegexp(regexp) => write!(f, "reporegex:{}", go_quote(regexp)),
            Query::BranchesRepos(list) => {
                f.write_str("(branchesrepos")?;
                for branch in list {
                    // `BranchesRepos.String` prints the cardinality when it
                    // is larger than one, and the bitmap itself otherwise.
                    let bitmap = roaring::RoaringBitmap::deserialize_from(&branch.repos[..])
                        .unwrap_or_default();
                    if bitmap.len() > 1 {
                        write!(f, " {}:{}", branch.branch, bitmap.len())?;
                    } else {
                        write!(f, " {}={}", branch.branch, roaring_string(&bitmap))?;
                    }
                }
                f.write_str(")")
            }
            Query::RepoIds(repos) => {
                let count = roaring::RoaringBitmap::deserialize_from(&repos[..])
                    .map(|bitmap| bitmap.len())
                    .unwrap_or(0);
                write!(f, "(repoids repoid=count:{count})")
            }
            Query::RepoSet(set) => {
                let mut repos: Vec<&str> = set.keys().map(String::as_str).collect();
                repos.sort_unstable();
                if repos.len() > 5 {
                    write!(f, "(reposet size={})", repos.len())
                } else {
                    write!(f, "(reposet {})", repos.join(" "))
                }
            }
            Query::FileNameSet(set) => {
                let mut names: Vec<&str> = set.iter().map(String::as_str).collect();
                names.sort_unstable();
                if names.len() > 5 {
                    write!(f, "(filenameset size={})", names.len())
                } else {
                    write!(f, "(filenameset {})", names.join(" "))
                }
            }
            Query::Type { child, kind } => {
                let kind = match kind {
                    TypeKind::FileMatch => "filematch",
                    TypeKind::FileName => "filename",
                    TypeKind::Repo => "repo",
                };
                write!(f, "(type:{kind} {child})")
            }
            Query::Substring {
                pattern,
                case_sensitive,
                file_name,
                content,
            } => {
                let mut out = String::new();
                if *file_name {
                    out.push_str("file_");
                } else if *content {
                    out.push_str("content_");
                }
                out.push_str("substr:");
                out.push_str(&go_quote(pattern));
                if *case_sensitive {
                    out.insert_str(0, "case_");
                }
                f.write_str(&out)
            }
            Query::And(children) => {
                f.write_str("(and ")?;
                write_joined(f, children)?;
                f.write_str(")")
            }
            Query::Or(children) => {
                f.write_str("(or ")?;
                write_joined(f, children)?;
                f.write_str(")")
            }
            Query::Not(child) => write!(f, "(not {child})"),
            Query::Branch { pattern, exact } => {
                if *exact {
                    write!(f, "branch={}", go_quote(pattern))
                } else {
                    write!(f, "branch:{}", go_quote(pattern))
                }
            }
            Query::Boost { child, boost } => write!(f, "(boost {boost:.2} {child})"),
        }
    }
}

fn write_joined(f: &mut std::fmt::Formatter<'_>, children: &[Query]) -> std::fmt::Result {
    for (index, child) in children.iter().enumerate() {
        if index > 0 {
            f.write_str(" ")?;
        }
        write!(f, "{child}")?;
    }
    Ok(())
}

/// `roaring.Bitmap.String`: `{}` when empty, `{id}` / `{id,id}` otherwise.
fn roaring_string(bitmap: &roaring::RoaringBitmap) -> String {
    let ids: Vec<String> = bitmap.iter().map(|id| id.to_string()).collect();
    format!("{{{}}}", ids.join(","))
}

/// `strconv.Quote` for the subset `fmt.Sprintf("%q", s)` exercises here.
fn go_quote(value: &str) -> String {
    let mut out = String::with_capacity(value.len() + 2);
    out.push('"');
    for ch in value.chars() {
        match ch {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\u{07}' => out.push_str("\\a"),
            '\u{08}' => out.push_str("\\b"),
            '\u{0c}' => out.push_str("\\f"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            '\u{0b}' => out.push_str("\\v"),
            ch if ch.is_control() => {
                let code = ch as u32;
                if code < 0x100 {
                    out.push_str(&format!("\\x{code:02x}"));
                } else if code < 0x10000 {
                    out.push_str(&format!("\\u{code:04x}"));
                } else {
                    out.push_str(&format!("\\U{code:08x}"));
                }
            }
            ch => out.push(ch),
        }
    }
    out.push('"');
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn regex(pattern: &str) -> Query {
        Query::Regexp {
            regexp: pattern.to_owned(),
            file_name: false,
            content: true,
            case_sensitive: true,
        }
    }

    #[test]
    fn folds_constants_like_zoekt() {
        // true AND x => x
        assert_eq!(
            Query::and(vec![Query::Const(true), regex("x")]).simplify(),
            regex("x")
        );
        // false AND x => false
        assert_eq!(
            Query::and(vec![Query::Const(false), regex("x")]).simplify(),
            Query::Const(false)
        );
        // false OR x => x
        assert_eq!(
            Query::or(vec![Query::Const(false), regex("x")]).simplify(),
            regex("x")
        );
        // true OR x => true
        assert_eq!(
            Query::or(vec![Query::Const(true), regex("x")]).simplify(),
            Query::Const(true)
        );
        // Empty AND/OR.
        assert_eq!(Query::and(vec![]).simplify(), Query::Const(true));
        assert_eq!(Query::or(vec![]).simplify(), Query::Const(false));
        // NOT true => false.
        assert_eq!(
            Query::negate(Query::Const(true)).simplify(),
            Query::Const(false)
        );
        // BOOST const folds away.
        assert_eq!(
            Query::Boost {
                child: Box::new(Query::Const(true)),
                boost: ZOEKT_SCORE_BOOST
            }
            .simplify(),
            Query::Const(true)
        );
        // An empty set matches nothing.
        assert_eq!(
            Query::file_name_set(Vec::<String>::new()).simplify(),
            Query::Const(false)
        );
        assert_eq!(
            Query::repo_set(Vec::<String>::new()).simplify(),
            Query::Const(false)
        );
        // An empty branch pattern matches everything.
        assert_eq!(
            Query::Branch {
                pattern: String::new(),
                exact: false
            }
            .simplify(),
            Query::Const(true)
        );
        // An empty regexp is the empty match.
        assert_eq!(regex("").simplify(), Query::Const(true));
    }

    #[test]
    fn flattens_nested_and_or() {
        let nested = Query::and(vec![
            regex("a"),
            Query::and(vec![regex("b"), regex("c")]),
            Query::or(vec![regex("d")]),
        ]);
        assert_eq!(
            nested.simplify(),
            Query::And(vec![regex("a"), regex("b"), regex("c"), regex("d")])
        );
    }

    #[test]
    fn keeps_negations_wrapped() {
        let query = Query::negate(Query::negate(regex("x"))).simplify();
        assert_eq!(
            query,
            Query::Not(Box::new(Query::Not(Box::new(regex("x")))))
        );
    }

    #[test]
    fn encodes_a_single_branch_repos_bitmap() {
        let query = Query::single_branches_repos("HEAD", &[42]);
        let proto = to_proto(&query);
        let Some(q::Query::BranchesRepos(encoded)) = proto.query else {
            panic!("expected branches_repos, got {proto:?}");
        };
        assert_eq!(encoded.list.len(), 1);
        assert_eq!(encoded.list[0].branch, "HEAD");
        let bitmap = roaring::RoaringBitmap::deserialize_from(encoded.list[0].repos.as_slice())
            .expect("decode");
        assert_eq!(bitmap.iter().collect::<Vec<_>>(), vec![42]);
    }

    #[test]
    fn encodes_the_hybrid_query_shape() {
        // The hybrid search wraps the text query in
        // (and branch_repos text (not (file_name_set ignored))).
        let query = Query::and(vec![
            Query::single_branches_repos("HEAD", &[7]),
            regex("needle"),
            Query::negate(Query::file_name_set(vec!["a.go".to_owned()])),
        ])
        .simplify();
        let proto = to_proto(&query);
        let Some(q::Query::And(and)) = proto.query else {
            panic!("expected and, got {proto:?}");
        };
        assert_eq!(and.children.len(), 3);
        assert!(matches!(
            and.children[0].query,
            Some(q::Query::BranchesRepos(_))
        ));
        assert!(matches!(and.children[1].query, Some(q::Query::Regexp(_))));
        let Some(q::Query::Not(not)) = &and.children[2].query else {
            panic!("expected not");
        };
        let Some(child) = &not.child else {
            panic!("missing child");
        };
        let Some(q::Query::FileNameSet(names)) = &child.query else {
            panic!("expected file name set");
        };
        assert_eq!(names.set, vec!["a.go".to_owned()]);
    }

    #[test]
    fn sorts_file_name_sets_for_a_stable_wire_encoding() {
        let query = Query::file_name_set(vec![
            "b.go".to_owned(),
            "a.go".to_owned(),
            "c.go".to_owned(),
        ]);
        let proto = to_proto(&query);
        let Some(q::Query::FileNameSet(names)) = proto.query else {
            panic!("expected file name set");
        };
        assert_eq!(names.set, vec!["a.go", "b.go", "c.go"]);
    }

    // ------------------------------------------------ from_basic (baseline
    // `internal/search/zoekt/query_test.go`)

    use sg_search_query::parse_regexp;
    use sg_search_query::transformer::to_basic_query;

    fn convert(
        input: &str,
        result_types: ResultTypes,
        features: Features,
        typ: IndexedRequestType,
    ) -> Query {
        let nodes = parse_regexp(input).unwrap_or_else(|err| panic!("{input:?}: {err}"));
        let basic = to_basic_query(&nodes).unwrap_or_else(|err| panic!("{input:?}: {err}"));
        from_basic(&basic, result_types, &features, typ)
            .unwrap_or_else(|err| panic!("{input:?}: {err}"))
    }

    fn regexp(pattern: &str, file_name: bool, content: bool, case_sensitive: bool) -> Query {
        Query::Regexp {
            regexp: pattern.to_owned(),
            file_name,
            content,
            case_sensitive,
        }
    }

    fn substring(pattern: &str, file_name: bool, content: bool, case_sensitive: bool) -> Query {
        Query::Substring {
            pattern: pattern.to_owned(),
            case_sensitive,
            file_name,
            content,
        }
    }

    #[test]
    fn converts_plain_literals_with_result_type_scoping() {
        assert_eq!(
            convert(
                "foo patterntype:regexp",
                ResultTypes::file_path_repo(),
                Features::default(),
                IndexedRequestType::Text,
            ),
            substring("foo", false, false, false)
        );
        assert_eq!(
            convert(
                "test type:path",
                ResultTypes::path_only(),
                Features::default(),
                IndexedRequestType::Text,
            ),
            substring("test", true, false, false)
        );
        assert_eq!(
            convert(
                "test type:file patterntype:literal",
                ResultTypes::file_only(),
                Features::default(),
                IndexedRequestType::Text,
            ),
            substring("test", false, true, false)
        );
        assert_eq!(
            convert(
                "foo case:yes",
                ResultTypes::file_path_repo(),
                Features::default(),
                IndexedRequestType::Text,
            ),
            substring("foo", false, false, true)
        );
    }

    #[test]
    fn converts_file_filters_and_languages() {
        assert_eq!(
            convert(
                r"file:\.go$",
                ResultTypes::file_path_repo(),
                Features::default(),
                IndexedRequestType::Text,
            ),
            regexp(r"\.go$", true, false, false)
        );
        // Language filters are file regexps and stay case insensitive.
        assert_eq!(
            convert(
                "lang:go",
                ResultTypes::file_path_repo(),
                Features::default(),
                IndexedRequestType::Text,
            ),
            regexp(r"(?i)\.go$", true, false, false)
        );
        // With content-based detection they are native Language nodes.
        assert_eq!(
            convert(
                "lang:go lang:typescript -lang:markdown",
                ResultTypes::file_path_repo(),
                Features {
                    content_based_lang_filters: true,
                },
                IndexedRequestType::Text,
            ),
            Query::And(vec![
                Query::Language("Go".to_owned()),
                Query::Language("TypeScript".to_owned()),
                Query::Not(Box::new(Query::Language("Markdown".to_owned()))),
            ])
        );
    }

    #[test]
    fn converts_repo_has_filters() {
        assert_eq!(
            convert(
                "repo:has.content(foo)",
                ResultTypes::file_path_repo(),
                Features::default(),
                IndexedRequestType::Text,
            ),
            Query::Type {
                child: Box::new(regexp("foo", false, true, false)),
                kind: TypeKind::Repo,
            }
        );
    }

    #[test]
    fn wraps_symbol_requests_and_reports_invalid_patterns() {
        assert_eq!(
            convert(
                "foo",
                ResultTypes::file_path_repo(),
                Features::default(),
                IndexedRequestType::Symbol,
            ),
            Query::Symbol(Box::new(substring("foo", false, false, false)))
        );
        assert!(matches!(
            parse_re("[", true, false, false),
            Err(Error::InvalidRegexp { .. })
        ));
    }
}
