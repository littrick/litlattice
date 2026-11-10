#![forbid(unsafe_code)]

//! `internal/search/job/jobutil/job.go` pure construction helpers.
//!
//! Ported from the baseline:
//!
//! * [`get_path_regexps`], [`compute_file_match_limit`],
//!   [`is_ownership_search`], [`is_select_owners_search`],
//!   [`is_contributor_search`], [`contributors_as_regexp`];
//! * [`timeout_duration`], [`map_slice`], [`to_lang_filters`];
//! * [`compute_result_types`], [`to_text_pattern_info`],
//!   [`to_symbol_search_request`], [`to_repo_options`];
//! * [`zoekt_query_patterns_as_regexps`], [`job_mode`], [`is_global`] and the
//!   structural rewriting of [`order_racing_jobs`].
//!
//! Not ported: `NewPlanJob`/`NewBasicJob`/`NewTextSearchJob`/`NewFlatJob`, the
//! expression job builders, `jobBuilder.newZoekt*`, the full job tree, and any
//! GraphQL/HTTP/gRPC/DB wiring. `toRepoOptions` takes a user-settings snapshot
//! instead of reading the database, and `timeoutDuration` takes the site's
//! `MaxTimeoutSeconds` explicitly instead of calling `conf.Get()`.
//!
//! See `docs/overviews/m9_8hn_jobutil_job_builders_overview.md`.

use regex::Regex;

use sg_search_query::basic::{q_string_value, Basic, Parameters, RepoHasFileContentArgs};
use sg_search_query::fields::{
    FIELD_COMBY_RULE, FIELD_CONTEXT, FIELD_COUNT, FIELD_FILE, FIELD_LANG, FIELD_REPO,
    FIELD_REPO_HAS_COMMIT_AFTER, FIELD_SELECT, FIELD_TIMEOUT, FIELD_TYPE, FIELD_VISIBILITY,
};
use sg_search_query::labels::Labels;
use sg_search_query::node::{visit_field, visit_pattern, Node, OperatorKind};
use sg_search_query::parser::SearchType;
use sg_search_query::predicate::parse_as_predicate;
use sg_search_query::validate::YesNoOnly;
use sg_service_searcher::protocol::{PatternNode, QueryNode};
use sg_zoekt::languages::{
    case_insensitive_regexp, get_language_by_name_or_alias, lang_to_file_regexp, regexp_quote_meta,
    union_regexps,
};
use sg_zoekt::query::Query;

use crate::search_client::{Features, Protocol};
use crate::search_contexts::{
    is_global_search_context_spec, parse_repository_revisions, parse_visibility, ParsedRepoFilter,
    RepoVisibility,
};

// `internal/search/limits/limits.go`.
/// `limits.DefaultMaxSearchResults`.
pub const DEFAULT_MAX_SEARCH_RESULTS: i64 = 30;
/// `limits.DefaultTimeout` (20s), in nanoseconds.
pub const DEFAULT_TIMEOUT_NANOS: i64 = 20_000_000_000;
/// `limits.DefaultTimeoutExhaustive` (1h), in nanoseconds.
pub const DEFAULT_TIMEOUT_EXHAUSTIVE_NANOS: i64 = 3_600_000_000_000;
/// `query.CountAllLimit`.
pub const COUNT_ALL_LIMIT: i64 = 99_999_999;
/// `limits.SearchLimits(...).MaxTimeoutSeconds` default (`limits.go:45`).
pub const DEFAULT_MAX_TIMEOUT_SECONDS: u64 = 60;

// ---------------------------------------------------------------------------
// result types (`internal/search/result/result_type.go`)
// ---------------------------------------------------------------------------

/// `result.Types`: a bit set of result kinds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ResultTypes(pub u8);

impl ResultTypes {
    pub const EMPTY: ResultTypes = ResultTypes(0);
    pub const REPO: ResultTypes = ResultTypes(1 << 0);
    pub const SYMBOL: ResultTypes = ResultTypes(1 << 1);
    pub const FILE: ResultTypes = ResultTypes(1 << 2);
    pub const PATH: ResultTypes = ResultTypes(1 << 3);
    pub const DIFF: ResultTypes = ResultTypes(1 << 4);
    pub const COMMIT: ResultTypes = ResultTypes(1 << 5);
    pub const STRUCTURAL: ResultTypes = ResultTypes(1 << 6);

    /// `result.Types.Has`: non-empty intersection.
    pub fn has(self, other: ResultTypes) -> bool {
        self.0 & other.0 != 0
    }

    /// `result.Types.With`.
    pub fn with(self, other: ResultTypes) -> ResultTypes {
        ResultTypes(self.0 | other.0)
    }

    /// `result.TypeFromString`.
    pub fn from_string(value: &str) -> Option<ResultTypes> {
        Some(match value {
            "repo" => Self::REPO,
            "symbol" => Self::SYMBOL,
            "file" => Self::FILE,
            "path" => Self::PATH,
            "diff" => Self::DIFF,
            "commit" => Self::COMMIT,
            "structural" => Self::STRUCTURAL,
            _ => return None,
        })
    }

    /// `result.Types.String`: the set names sorted and joined with `, `.
    pub fn as_string(self) -> String {
        let mut names: Vec<&str> = Vec::new();
        for (name, value) in [
            ("repo", Self::REPO),
            ("symbol", Self::SYMBOL),
            ("file", Self::FILE),
            ("path", Self::PATH),
            ("diff", Self::DIFF),
            ("commit", Self::COMMIT),
            ("structural", Self::STRUCTURAL),
        ] {
            if self.has(value) {
                names.push(name);
            }
        }
        names.sort_unstable();
        names.join(", ")
    }
}

impl std::ops::BitOr for ResultTypes {
    type Output = ResultTypes;
    fn bitor(self, rhs: ResultTypes) -> ResultTypes {
        ResultTypes(self.0 | rhs.0)
    }
}

/// `defaultResultTypes` (`job.go:755`).
pub const DEFAULT_RESULT_TYPES: ResultTypes =
    ResultTypes(ResultTypes::FILE.0 | ResultTypes::PATH.0 | ResultTypes::REPO.0);

// ---------------------------------------------------------------------------
// select paths (`internal/search/filter/select.go`)
// ---------------------------------------------------------------------------

/// `filter.SelectPath` (`[]string`).
pub type SelectPath = Vec<String>;

/// `filter.SelectPathFromString`: split on `.` and validate against the
/// selector tree. Returns an empty path on validation failure, matching the
/// baseline's `SelectPath{}, err`.
pub fn select_path_from_string(value: &str) -> Result<SelectPath, String> {
    let fields: Vec<String> = value.split('.').map(str::to_owned).collect();
    let mut valid = true;
    for (index, field) in fields.iter().enumerate() {
        let is_valid = match index {
            0 => matches!(
                field.as_str(),
                "commit" | "content" | "file" | "repo" | "symbol"
            ),
            1 => match fields[0].as_str() {
                "commit" => matches!(field.as_str(), "diff"),
                "file" => matches!(field.as_str(), "directory" | "path" | "owners"),
                "symbol" => matches!(
                    field.as_str(),
                    "file"
                        | "module"
                        | "namespace"
                        | "package"
                        | "class"
                        | "method"
                        | "property"
                        | "field"
                        | "constructor"
                        | "enum"
                        | "interface"
                        | "function"
                        | "variable"
                        | "constant"
                        | "string"
                        | "number"
                        | "boolean"
                        | "array"
                        | "object"
                        | "key"
                        | "null"
                        | "enum-member"
                        | "struct"
                        | "event"
                        | "operator"
                        | "type-parameter"
                ),
                _ => false,
            },
            2 => match fields[0].as_str() {
                "commit" => matches!(field.as_str(), "added" | "removed"),
                _ => false,
            },
            _ => false,
        };
        if !is_valid {
            valid = false;
            break;
        }
    }
    if valid {
        Ok(fields)
    } else {
        Err(format!("invalid select path {value:?}"))
    }
}

/// `isSelectOwnersSearch` (`job.go:592`).
pub fn is_select_owners_search(path: &SelectPath) -> bool {
    path.first().map(String::as_str) == Some("file") && path.len() == 2 && path[1] == "owners"
}

// ---------------------------------------------------------------------------
// parameter accessors mirroring `query.Parameters`
// ---------------------------------------------------------------------------

fn visit_parameters<F: FnMut(&str, bool)>(parameters: &Parameters, field: &str, mut f: F) {
    let nodes = parameters.to_nodes();
    visit_field(&nodes, field, &mut |value, negated, _annotation| {
        f(value, negated);
    });
}

/// `Parameters.Count` (last value wins, `strconv.Atoi` semantics).
pub fn parameters_count(parameters: &Parameters) -> Option<i64> {
    let mut count = None;
    visit_parameters(parameters, FIELD_COUNT, |value, _negated| {
        count = Some(parse_go_int(value));
    });
    count
}

/// `Parameters.GetTimeout` (last value wins, `time.ParseDuration` semantics),
/// returned as Go `time.Duration` nanoseconds.
pub fn parameters_timeout_nanos(parameters: &Parameters) -> Option<i64> {
    let mut timeout = None;
    visit_parameters(parameters, FIELD_TIMEOUT, |value, _negated| {
        timeout = Some(parse_go_duration(value).expect("timeout value already validated"));
    });
    timeout
}

/// `Parameters.MaxResults`.
pub fn parameters_max_results(parameters: &Parameters, default_limit: i64) -> i64 {
    if let Some(count) = parameters_count(parameters) {
        return count;
    }
    if default_limit != 0 {
        return default_limit;
    }
    DEFAULT_MAX_SEARCH_RESULTS
}

fn parse_go_int(value: &str) -> i64 {
    value
        .parse::<i64>()
        .unwrap_or_else(|_| panic!("Value {value:?} cannot be parsed as an int"))
}

/// `time.ParseDuration` for the positive/negative decimal inputs the search
/// validator accepts. Returns nanoseconds.
pub fn parse_go_duration(value: &str) -> Option<i64> {
    let bytes = value.as_bytes();
    if bytes.is_empty() {
        return None;
    }
    let mut index = 0;
    let negative = match bytes[index] {
        b'-' => {
            index += 1;
            true
        }
        b'+' => {
            index += 1;
            false
        }
        _ => false,
    };
    if index == bytes.len() {
        return None;
    }
    if &value[index..] == "0" {
        return Some(0);
    }

    let mut total: i64 = 0;
    while index < bytes.len() {
        let number_start = index;
        while index < bytes.len() && bytes[index].is_ascii_digit() {
            index += 1;
        }
        if index == number_start {
            return None;
        }
        let integer = parse_go_int(&value[number_start..index]);
        let mut fraction: i64 = 0;
        let mut fraction_scale: i64 = 1;
        if index < bytes.len() && bytes[index] == b'.' {
            index += 1;
            let fraction_start = index;
            while index < bytes.len() && bytes[index].is_ascii_digit() {
                fraction = fraction * 10 + i64::from(bytes[index] - b'0');
                fraction_scale *= 10;
                index += 1;
            }
            if index == fraction_start {
                return None;
            }
        }
        let unit_start = index;
        while index < bytes.len() && !bytes[index].is_ascii_digit() && bytes[index] != b'.' {
            index += 1;
        }
        let unit = &value[unit_start..index];
        let mult: i64 = match unit {
            "ns" => 1,
            "us" | "\u{b5}s" | "\u{3bc}s" => 1_000,
            "ms" => 1_000_000,
            "s" => 1_000_000_000,
            "m" => 60_000_000_000,
            "h" => 3_600_000_000_000,
            _ => return None,
        };
        total = total
            .checked_add(integer.checked_mul(mult)?)?
            .checked_add(fraction.checked_mul(mult)? / fraction_scale)?;
    }
    Some(if negative { -total } else { total })
}

/// `searchrepos.ExactlyOneRepo`. A `^...$` anchored pattern is a single repo
/// only when the inner regexp is a pure literal (the baseline compares
/// `regexp/syntax`'s `OpLiteral`).
pub fn exactly_one_repo(repo_filters: &[ParsedRepoFilter]) -> bool {
    if repo_filters.len() != 1 {
        return false;
    }
    let repo = &repo_filters[0].repo;
    let Some(inner) = repo
        .strip_prefix('^')
        .and_then(|value| value.strip_suffix('$'))
    else {
        return false;
    };
    is_regexp_literal(inner)
}

fn is_regexp_literal(pattern: &str) -> bool {
    if pattern.is_empty() {
        // `regexp/syntax` parses "" as OpEmptyMatch, not OpLiteral.
        return false;
    }
    let mut chars = pattern.chars();
    while let Some(c) = chars.next() {
        match c {
            '\\' => match chars.next() {
                None => return false,
                Some('Q') => {
                    // `\Q...\E`: everything up to `\E` is literal.
                    while let Some(next) = chars.next() {
                        if next == '\\' && chars.next() == Some('E') {
                            break;
                        }
                    }
                }
                // Escaped metacharacters and escapes that produce a rune.
                Some('n' | 't' | 'r' | 'a' | 'f' | 'v' | 'x' | '0'..='7') => {}
                // Perl classes and zero-width assertions are operators.
                Some(
                    'A' | 'b' | 'B' | 'C' | 'd' | 'D' | 'p' | 'P' | 's' | 'S' | 'w' | 'W' | 'z'
                    | 'Z',
                ) => return false,
                Some(_) => {}
            },
            '.' | '+' | '*' | '?' | '(' | ')' | '|' | '[' | ']' | '{' | '}' | '^' | '$' => {
                return false;
            }
            _ => {}
        }
    }
    true
}

// ---------------------------------------------------------------------------
// typed predicate accessors
// ---------------------------------------------------------------------------

fn canonical_predicate(field: &str, name: &str) -> Option<&'static str> {
    Some(match (field, name) {
        ("repo", "contains.file" | "has.file") => "contains.file",
        ("repo", "contains.path" | "has.path") => "contains.path",
        ("repo", "contains.content" | "has.content") => "contains.content",
        ("repo", "contains.commit.after" | "has.commit.after") => "contains.commit.after",
        ("repo", "has.description") => "has.description",
        ("repo", "has.meta") => "has.meta",
        ("repo", "has.topic") => "has.topic",
        ("repo", "has.tag") => "has.tag",
        ("repo", "has") => "has",
        ("repo", "has.key") => "has.key",
        ("repo", "contains") => "contains",
        ("file", "has.owner") => "has.owner",
        ("file", "has.contributor") => "has.contributor",
        ("file", "contains.content" | "has.content") => "contains.content",
        ("rev", "at.time") => "at.time",
        _ => return None,
    })
}

fn visit_typed_predicate(
    basic: &Basic,
    field: &str,
    canonical: &str,
    f: &mut dyn FnMut(&str, bool),
) {
    let nodes = basic.parameters.to_nodes();
    visit_field(&nodes, field, &mut |value, negated, annotation| {
        if !annotation.labels.is_set(Labels::IS_PREDICATE) {
            return;
        }
        let Some((name, args)) = parse_as_predicate(value) else {
            return;
        };
        if canonical_predicate(field, &name) != Some(canonical) {
            return;
        }
        f(&args, negated);
    });
}

/// `Parameters.FileHasOwner`.
pub fn file_has_owner(basic: &Basic) -> (Vec<String>, Vec<String>) {
    let mut include = Vec::new();
    let mut exclude = Vec::new();
    visit_typed_predicate(basic, FIELD_FILE, "has.owner", &mut |owner, negated| {
        if negated {
            exclude.push(owner.to_owned());
        } else {
            include.push(owner.to_owned());
        }
    });
    (include, exclude)
}

/// `Parameters.FileHasContributor`.
pub fn file_has_contributor(basic: &Basic) -> (Vec<String>, Vec<String>) {
    let mut include = Vec::new();
    let mut exclude = Vec::new();
    visit_typed_predicate(
        basic,
        FIELD_FILE,
        "has.contributor",
        &mut |contributor, negated| {
            if negated {
                exclude.push(contributor.to_owned());
            } else {
                include.push(contributor.to_owned());
            }
        },
    );
    (include, exclude)
}

/// `Parameters.RepoHasDescription`.
pub fn repo_has_description(basic: &Basic) -> Vec<String> {
    let mut patterns = Vec::new();
    visit_typed_predicate(
        basic,
        FIELD_REPO,
        "has.description",
        &mut |pattern, _negated| {
            let split: Vec<&str> = pattern.split(' ').collect();
            patterns.push(format!("(?:{})", split.join(").*?(?:")));
        },
    );
    patterns
}

/// `query.RepoHasCommitAfterArgs`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RepoHasCommitAfterArgs {
    pub time_ref: String,
    pub negated: bool,
}

/// `Parameters.RepoContainsCommitAfter`.
pub fn repo_contains_commit_after(basic: &Basic) -> Option<RepoHasCommitAfterArgs> {
    let mut result = None;
    basic.parameters.find_parameter(
        FIELD_REPO_HAS_COMMIT_AFTER,
        &mut |value, negated, _annotation| {
            result = Some(RepoHasCommitAfterArgs {
                time_ref: value.to_owned(),
                negated,
            });
        },
    );
    visit_typed_predicate(
        basic,
        FIELD_REPO,
        "contains.commit.after",
        &mut |value, negated| {
            result = Some(RepoHasCommitAfterArgs {
                time_ref: value.to_owned(),
                negated,
            });
        },
    );
    result
}

/// `query.RepoKVPFilter`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RepoKvpFilter {
    pub key: String,
    pub value: Option<String>,
    pub negated: bool,
    pub key_only: bool,
}

/// `query.RepoHasTopicPredicate`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RepoHasTopic {
    pub topic: String,
    pub negated: bool,
}

/// `Parameters.RepoHasTopics`.
pub fn repo_has_topics(basic: &Basic) -> Vec<RepoHasTopic> {
    let mut topics = Vec::new();
    visit_typed_predicate(basic, FIELD_REPO, "has.topic", &mut |topic, negated| {
        topics.push(RepoHasTopic {
            topic: topic.to_owned(),
            negated,
        });
    });
    topics
}

fn exact_regexp_pattern(input: &str) -> String {
    format!("^{}$", regexp_quote_meta(input))
}

/// `query.ScanDelimited`.
fn scan_delimited(data: &str, strict: bool, delimiter: char) -> Result<(String, usize), String> {
    let mut chars = data.char_indices();
    let (_, first) = chars.next().ok_or_else(|| "empty literal".to_owned())?;
    if first != delimiter {
        return Err(format!("expected delimiter {delimiter:?}"));
    }
    let mut result = String::new();
    let mut consumed;
    loop {
        let (offset, c) = chars
            .next()
            .ok_or_else(|| format!("unterminated literal: expected {delimiter:?}"))?;
        consumed = offset + c.len_utf8();
        match c {
            c if c == delimiter => break,
            '\\' => {
                let (_, escaped) = chars
                    .next()
                    .ok_or_else(|| "unterminated escape sequence".to_owned())?;
                match escaped {
                    'a' | 'b' | 'f' | 'v' => {
                        result.push('\\');
                        result.push(escaped);
                    }
                    'n' => result.push('\n'),
                    'r' => result.push('\r'),
                    't' => result.push('\t'),
                    '\\' => result.push('\\'),
                    c if c == delimiter => result.push(c),
                    _ => {
                        if strict {
                            return Err("unrecognized escape sequence".to_owned());
                        }
                        result.push('\\');
                        result.push(escaped);
                    }
                }
            }
            c => result.push(c),
        }
    }
    Ok((result, consumed))
}

/// `RepoHasMetaPredicate.Unmarshal`'s inner `scanLiteral`.
fn scan_literal(data: &str) -> Result<(String, bool, usize), String> {
    if data.starts_with('"') {
        let (value, advance) = scan_delimited(data, true, '"')?;
        return Ok((value, false, advance));
    }
    if data.starts_with('\'') {
        let (value, advance) = scan_delimited(data, true, '\'')?;
        return Ok((value, false, advance));
    }
    if data.starts_with('/') {
        let (value, advance) = scan_delimited(data, false, '/')?;
        return Ok((value, true, advance));
    }
    if let Some(index) = data.find(':') {
        return Ok((data[..index].to_owned(), false, index));
    }
    Ok((data.to_owned(), false, data.len()))
}

fn parse_has_meta(params: &str, negated: bool) -> RepoKvpFilter {
    let params = params.trim_matches([' ', '\t']);
    let (key_text, is_regexp, advance) =
        scan_literal(params).expect("has.meta arguments already validated");
    assert!(!key_text.is_empty(), "key cannot be empty");
    let key = if is_regexp {
        key_text
    } else {
        exact_regexp_pattern(&key_text)
    };
    let mut rest = &params[advance..];
    let mut key_only = false;
    let mut value = None;
    if let Some(stripped) = rest.strip_prefix(':') {
        rest = stripped;
        let (value_text, is_regexp, advance) =
            scan_literal(rest).expect("has.meta value already validated");
        rest = &rest[advance..];
        assert!(rest.is_empty(), "unexpected extra content");
        if !value_text.is_empty() {
            value = Some(if is_regexp {
                value_text
            } else {
                exact_regexp_pattern(&value_text)
            });
        }
    } else {
        key_only = true;
    }
    RepoKvpFilter {
        key,
        value,
        negated,
        key_only,
    }
}

fn scan_kvp_literal(data: &str) -> Result<(String, usize), String> {
    if data.starts_with('"') {
        let (value, advance) = scan_delimited(data, true, '"')?;
        return Ok((value, advance));
    }
    if data.starts_with('\'') {
        let (value, advance) = scan_delimited(data, true, '\'')?;
        return Ok((value, advance));
    }
    if let Some(index) = data.find(':') {
        return Ok((data[..index].to_owned(), index));
    }
    Ok((data.to_owned(), data.len()))
}

fn parse_has_kvp(params: &str, negated: bool) -> RepoKvpFilter {
    let params = params.trim_matches([' ', '\t']);
    let (key, advance) = scan_kvp_literal(params).expect("has arguments already validated");
    let rest = &params[advance..];
    let rest = rest
        .strip_prefix(':')
        .expect("expected params of the form key:value");
    let (value, advance) = scan_kvp_literal(rest).expect("has value already validated");
    let rest = &rest[advance..];
    assert!(rest.is_empty(), "unexpected extra content");
    assert!(!key.is_empty(), "key cannot be empty");
    RepoKvpFilter {
        key: exact_regexp_pattern(&key),
        value: Some(exact_regexp_pattern(&value)),
        negated,
        key_only: false,
    }
}

/// `Parameters.RepoHasKVPs`, grouped by predicate kind in the baseline's
/// four-pass order.
pub fn repo_has_kvps(basic: &Basic) -> Vec<RepoKvpFilter> {
    let mut filters = Vec::new();
    visit_typed_predicate(basic, FIELD_REPO, "has.meta", &mut |args, negated| {
        filters.push(parse_has_meta(args, negated));
    });
    visit_typed_predicate(basic, FIELD_REPO, "has", &mut |args, negated| {
        filters.push(parse_has_kvp(args, negated));
    });
    visit_typed_predicate(basic, FIELD_REPO, "has.tag", &mut |args, negated| {
        filters.push(RepoKvpFilter {
            key: exact_regexp_pattern(args),
            value: None,
            negated,
            key_only: false,
        });
    });
    visit_typed_predicate(basic, FIELD_REPO, "has.key", &mut |args, negated| {
        filters.push(RepoKvpFilter {
            key: exact_regexp_pattern(args),
            value: None,
            negated,
            key_only: true,
        });
    });
    filters
}

fn repositories(basic: &Basic) -> (Vec<ParsedRepoFilter>, Vec<String>) {
    let mut repos = Vec::new();
    let mut minus_repos = Vec::new();
    let nodes = basic.parameters.to_nodes();
    visit_field(&nodes, FIELD_REPO, &mut |value, negated, annotation| {
        if annotation.labels.is_set(Labels::IS_PREDICATE) {
            return;
        }
        if negated {
            minus_repos.push(value.to_owned());
        } else {
            repos.push(
                parse_repository_revisions(value)
                    .unwrap_or_else(|err| panic!("repo field {value:?} is invalid: {err}")),
            );
        }
    });
    (repos, minus_repos)
}

// ---------------------------------------------------------------------------
// getPathRegexps / limits / ownership
// ---------------------------------------------------------------------------

/// `searcher.SymbolSearchRequest` (`internal/search/searcher/symbol_search_job.go:176`).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct SymbolSearchRequest {
    pub regexp_pattern: String,
    pub is_case_sensitive: bool,
    pub include_patterns: Vec<String>,
    pub exclude_pattern: String,
    pub include_langs: Vec<String>,
    pub exclude_langs: Vec<String>,
}

/// `search.TextPatternInfo` (`internal/search/types.go:265`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TextPatternInfo {
    pub query: QueryNode,
    pub is_structural_pat: bool,
    pub comby_rule: String,
    pub is_case_sensitive: bool,
    pub file_match_limit: i32,
    pub index: YesNoOnly,
    pub select: SelectPath,
    pub include_paths: Vec<String>,
    pub exclude_paths: String,
    pub include_langs: Vec<String>,
    pub exclude_langs: Vec<String>,
    pub path_patterns_are_case_sensitive: bool,
    pub pattern_matches_content: bool,
    pub pattern_matches_path: bool,
    pub languages: Vec<String>,
}

/// `protocol.FromJobNode`.
pub fn from_job_node(node: Option<&Node>) -> QueryNode {
    let Some(node) = node else {
        // In searcher, nil queries are represented as empty regex patterns.
        return QueryNode::Pattern(PatternNode {
            value: String::new(),
            is_negated: false,
            is_regexp: true,
            boost: false,
        });
    };
    match node {
        Node::Pattern(pattern) => QueryNode::Pattern(PatternNode {
            value: pattern.value.clone(),
            is_negated: pattern.negated,
            is_regexp: pattern.is_regexp(),
            boost: pattern.annotation.labels.is_set(Labels::BOOST),
        }),
        Node::Operator(operator) => {
            let children = operator
                .operands
                .iter()
                .map(|operand| from_job_node(Some(operand)))
                .collect();
            match operator.kind {
                OperatorKind::And => QueryNode::And(children),
                OperatorKind::Or => QueryNode::Or(children),
                OperatorKind::Concat => panic!("unexpected operator kind (concat)"),
            }
        }
        Node::Parameter(_) => panic!("unexpected query node type (Parameter)"),
    }
}

/// `getPathRegexps` (`job.go:521`).
pub fn get_path_regexps(basic: &Basic, info: &TextPatternInfo) -> Vec<Regex> {
    let mut regexps = Vec::new();
    for pattern in &info.include_paths {
        let pattern = if info.is_case_sensitive {
            pattern.clone()
        } else {
            case_insensitive_regexp(pattern)
        };
        regexps.push(Regex::new(&pattern).expect("include path is a valid regular expression"));
    }

    if let Some(node) = &basic.pattern {
        if info.pattern_matches_path {
            visit_pattern(
                std::slice::from_ref(node),
                &mut |value, negated, annotation| {
                    if negated {
                        return;
                    }
                    let mut pattern = value.to_owned();
                    if !annotation.labels.is_set(Labels::REGEXP) {
                        pattern = regexp_quote_meta(&pattern);
                    }
                    let pattern = if info.is_case_sensitive {
                        pattern
                    } else {
                        case_insensitive_regexp(&pattern)
                    };
                    regexps
                        .push(Regex::new(&pattern).expect("pattern is a valid regular expression"));
                },
            );
        }
    }
    regexps
}

/// `computeFileMatchLimit` (`job.go:554`).
pub fn compute_file_match_limit(basic: &Basic, default_limit: i64) -> i64 {
    if is_contributor_search(basic).2 {
        return COUNT_ALL_LIMIT;
    }
    if is_ownership_search(basic).2 {
        return COUNT_ALL_LIMIT;
    }
    let select = q_string_value(&basic.to_parse_tree(), FIELD_SELECT);
    if !select.is_empty() {
        let path = select_path_from_string(&select).unwrap_or_default();
        if is_select_owners_search(&path) {
            return COUNT_ALL_LIMIT;
        }
    }
    parameters_max_results(&basic.parameters, default_limit)
}

/// `isOwnershipSearch` (`job.go:582`).
pub fn is_ownership_search(basic: &Basic) -> (Vec<String>, Vec<String>, bool) {
    let (include, exclude) = file_has_owner(basic);
    if !include.is_empty() || !exclude.is_empty() {
        (include, exclude, true)
    } else {
        (Vec::new(), Vec::new(), false)
    }
}

/// `isContributorSearch` (`job.go:598`).
pub fn is_contributor_search(basic: &Basic) -> (Vec<String>, Vec<String>, bool) {
    let (include, exclude) = file_has_contributor(basic);
    if !include.is_empty() || !exclude.is_empty() {
        (include, exclude, true)
    } else {
        (Vec::new(), Vec::new(), false)
    }
}

/// `contributorsAsRegexp` (`job.go:608`).
pub fn contributors_as_regexp(contributors: &[String], is_case_sensitive: bool) -> Vec<Regex> {
    contributors
        .iter()
        .map(|pattern| {
            let pattern = if is_case_sensitive {
                pattern.clone()
            } else {
                case_insensitive_regexp(pattern)
            };
            Regex::new(&pattern).expect("contributor is a valid regular expression")
        })
        .collect()
}

// ---------------------------------------------------------------------------
// timeout / map
// ---------------------------------------------------------------------------

/// `timeoutDuration` (`job.go:618`). `max_timeout_seconds` stands in for
/// `limits.SearchLimits(conf.Get()).MaxTimeoutSeconds`; the result is Go
/// `time.Duration` nanoseconds.
pub fn timeout_duration(protocol: Protocol, basic: &Basic, max_timeout_seconds: u64) -> i64 {
    if protocol == Protocol::Exhaustive {
        return parameters_timeout_nanos(&basic.parameters)
            .unwrap_or(DEFAULT_TIMEOUT_EXHAUSTIVE_NANOS);
    }

    let max_timeout = i64::try_from(max_timeout_seconds)
        .unwrap_or(i64::MAX)
        .saturating_mul(1_000_000_000);
    let mut duration = DEFAULT_TIMEOUT_NANOS;
    if let Some(timeout) = parameters_timeout_nanos(&basic.parameters) {
        duration = timeout;
    } else if parameters_count(&basic.parameters).is_some() {
        duration = max_timeout;
    }
    if duration > max_timeout {
        duration = max_timeout;
    }
    duration
}

/// `mapSlice` (`job.go:645`).
pub fn map_slice(values: &[String], f: &dyn Fn(&str) -> String) -> Vec<String> {
    values.iter().map(|value| f(value)).collect()
}

/// `toLangFilters` (`job.go:744`).
pub fn to_lang_filters(aliases: &[String]) -> Vec<String> {
    let mut filters: Vec<String> = Vec::new();
    for alias in aliases {
        let language = get_language_by_name_or_alias(alias).to_owned();
        if !filters.contains(&language) {
            filters.push(language);
        }
    }
    filters
}

// ---------------------------------------------------------------------------
// result types / pattern info
// ---------------------------------------------------------------------------

/// `computeResultTypes` (`job.go:757`).
pub fn compute_result_types(
    basic: &Basic,
    search_type: SearchType,
    default_types: ResultTypes,
) -> ResultTypes {
    if search_type == SearchType::Structural && !basic.is_empty_pattern() {
        return ResultTypes::STRUCTURAL;
    }

    let (types, _) = basic.include_exclude_values(FIELD_TYPE);
    if types.is_empty() {
        if let Some(node) = &basic.pattern {
            let mut has_pattern = false;
            let mut all_is_content = true;
            visit_pattern(
                std::slice::from_ref(node),
                &mut |_value, _negated, annotation| {
                    has_pattern = true;
                    all_is_content = all_is_content && annotation.labels.is_set(Labels::IS_CONTENT);
                },
            );
            if has_pattern && all_is_content {
                return ResultTypes::FILE;
            }
        }
    }

    if types.is_empty() {
        return default_types;
    }

    let mut result = ResultTypes::EMPTY;
    for t in types {
        if let Some(types) = ResultTypes::from_string(&t) {
            result = result.with(types);
        }
    }
    result
}

/// `toTextPatternInfo` (`job.go:690`).
pub fn to_text_pattern_info(
    basic: &Basic,
    result_types: ResultTypes,
    features: &Features,
    default_limit: i64,
) -> Result<TextPatternInfo, String> {
    let (mut files_include, mut files_exclude) = basic.include_exclude_values(FIELD_FILE);
    let (lang_alias_include, lang_alias_exclude) = basic.include_exclude_values(FIELD_LANG);
    let mut lang_include = Vec::new();
    let mut lang_exclude = Vec::new();
    if features.content_based_lang_filters {
        lang_include = to_lang_filters(&lang_alias_include);
        lang_exclude = to_lang_filters(&lang_alias_exclude);
    } else {
        files_include.extend(map_slice(&lang_alias_include, &lang_to_file_regexp));
        files_exclude.extend(map_slice(&lang_alias_exclude, &lang_to_file_regexp));
    }

    let selector =
        select_path_from_string(&basic.parameters.find_value(FIELD_SELECT)).unwrap_or_default();
    let count = parameters_max_results(&basic.parameters, default_limit);
    let query = from_job_node(basic.pattern.as_ref());
    if let QueryNode::Pattern(pattern) = &query {
        // The baseline checks `langExclude` twice; `langInclude` is not part of
        // this guard.
        if pattern.value.is_empty()
            && files_exclude.is_empty()
            && files_include.is_empty()
            && lang_exclude.is_empty()
        {
            return Err(
                "At least one of pattern and include/exclude patterns must be non-empty".to_owned(),
            );
        }
    }

    Ok(TextPatternInfo {
        query,
        is_structural_pat: basic.is_structural(),
        is_case_sensitive: basic.is_case_sensitive(),
        file_match_limit: count as i32,
        languages: lang_alias_include,
        include_paths: files_include,
        exclude_paths: union_regexps(&files_exclude),
        include_langs: lang_include,
        exclude_langs: lang_exclude,
        pattern_matches_path: result_types.has(ResultTypes::PATH),
        pattern_matches_content: result_types.has(ResultTypes::FILE),
        path_patterns_are_case_sensitive: basic.is_case_sensitive(),
        comby_rule: basic.parameters.find_value(FIELD_COMBY_RULE),
        index: basic.parameters.index(),
        select: selector,
    })
}

/// `toSymbolSearchRequest` (`job.go:653`). `Flat` is represented by its
/// `Basic` form (`Flat.ToBasic`).
pub fn to_symbol_search_request(
    basic: &Basic,
    features: &Features,
) -> Result<SymbolSearchRequest, String> {
    if let Some(Node::Pattern(pattern)) = &basic.pattern {
        if pattern.negated {
            return Err("symbol search does not support negation.".to_owned());
        }
    }

    let regexp_pattern = basic.pattern_string();
    let (mut files_include, mut files_exclude) = basic.include_exclude_values(FIELD_FILE);
    let (lang_alias_include, lang_alias_exclude) = basic.include_exclude_values(FIELD_LANG);
    let mut lang_include = Vec::new();
    let mut lang_exclude = Vec::new();
    if features.content_based_lang_filters {
        lang_include = to_lang_filters(&lang_alias_include);
        lang_exclude = to_lang_filters(&lang_alias_exclude);
    } else {
        files_include.extend(map_slice(&lang_alias_include, &lang_to_file_regexp));
        files_exclude.extend(map_slice(&lang_alias_exclude, &lang_to_file_regexp));
    }

    Ok(SymbolSearchRequest {
        regexp_pattern,
        is_case_sensitive: basic.is_case_sensitive(),
        include_patterns: files_include,
        exclude_pattern: union_regexps(&files_exclude),
        include_langs: lang_include,
        exclude_langs: lang_exclude,
    })
}

// ---------------------------------------------------------------------------
// repo options / job mode
// ---------------------------------------------------------------------------

/// `schema.Settings` fields `toRepoOptions` reads, injected as a snapshot.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct UserSettings {
    pub search_include_forks: Option<bool>,
    pub search_include_archived: Option<bool>,
}

/// `search.RepoOptions` subset assigned by `toRepoOptions`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RepoOptions {
    pub repo_filters: Vec<ParsedRepoFilter>,
    pub minus_repo_filters: Vec<String>,
    pub description_patterns: Vec<String>,
    pub search_context_spec: String,
    pub commit_after: Option<RepoHasCommitAfterArgs>,
    pub visibility: RepoVisibility,
    /// `RepoOptions.Cursors`; never set by `toRepoOptions`, but `isGlobal`
    /// reads it.
    pub cursors: usize,
    pub use_index: YesNoOnly,
    pub has_file_content: Vec<RepoHasFileContentArgs>,
    pub has_kvps: Vec<RepoKvpFilter>,
    pub has_topics: Vec<RepoHasTopic>,
    pub fork_set: bool,
    pub no_forks: bool,
    pub only_forks: bool,
    pub archived_set: bool,
    pub no_archived: bool,
    pub only_archived: bool,
}

impl Default for RepoOptions {
    fn default() -> Self {
        RepoOptions {
            repo_filters: Vec::new(),
            minus_repo_filters: Vec::new(),
            description_patterns: Vec::new(),
            search_context_spec: String::new(),
            commit_after: None,
            visibility: RepoVisibility::default(),
            cursors: 0,
            use_index: YesNoOnly::Yes,
            has_file_content: Vec::new(),
            has_kvps: Vec::new(),
            has_topics: Vec::new(),
            fork_set: false,
            no_forks: false,
            only_forks: false,
            archived_set: false,
            no_archived: false,
            only_archived: false,
        }
    }
}

/// `toRepoOptions` (`job.go:794`).
pub fn to_repo_options(basic: &Basic, user_settings: &UserSettings) -> RepoOptions {
    let (repo_filters, minus_repo_filters) = repositories(basic);

    let setting_forks = user_settings.search_include_forks.unwrap_or(false);
    let setting_archived = user_settings.search_include_archived.unwrap_or(false);

    let mut fork = YesNoOnly::No;
    if exactly_one_repo(&repo_filters) || setting_forks {
        fork = YesNoOnly::Yes;
    }
    if let Some(set_fork) = basic.parameters.fork() {
        fork = set_fork;
    }

    let mut archived = YesNoOnly::No;
    if exactly_one_repo(&repo_filters) || setting_archived {
        archived = YesNoOnly::Yes;
    }
    if let Some(set_archived) = basic.parameters.archived() {
        archived = set_archived;
    }

    RepoOptions {
        repo_filters,
        minus_repo_filters,
        description_patterns: repo_has_description(basic),
        search_context_spec: basic.parameters.find_value(FIELD_CONTEXT),
        fork_set: basic.parameters.fork().is_some(),
        only_forks: fork == YesNoOnly::Only,
        no_forks: fork == YesNoOnly::No,
        archived_set: basic.parameters.archived().is_some(),
        only_archived: archived == YesNoOnly::Only,
        no_archived: archived == YesNoOnly::No,
        visibility: parse_visibility(&basic.parameters.find_value(FIELD_VISIBILITY)),
        has_file_content: basic
            .repo_has_file_content()
            .expect("repo:contains predicates already validated"),
        commit_after: repo_contains_commit_after(basic),
        use_index: basic.parameters.index(),
        has_kvps: repo_has_kvps(basic),
        has_topics: repo_has_topics(basic),
        cursors: 0,
    }
}

/// `isGlobal` (`job.go:1132`).
pub fn is_global(options: &RepoOptions) -> bool {
    if !options.repo_filters.is_empty() {
        return false;
    }
    if !options.description_patterns.is_empty() {
        return false;
    }
    if !options.has_kvps.is_empty() {
        return false;
    }
    if !options.has_topics.is_empty() {
        return false;
    }
    if !is_global_search_context_spec(&options.search_context_spec) {
        return false;
    }
    if options.commit_after.is_some() {
        return false;
    }
    if options.cursors > 0 {
        return false;
    }
    if options.use_index == YesNoOnly::No {
        return false;
    }
    true
}

/// `search.Inputs` fields `jobMode` reads.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SearchInputs {
    pub protocol: Protocol,
    pub pattern_type: SearchType,
    pub on_sourcegraph_dot_com: bool,
}

/// `jobMode` (`job.go:961`).
pub fn job_mode(
    basic: &Basic,
    repo_options: &RepoOptions,
    result_types: ResultTypes,
    inputs: &SearchInputs,
) -> (bool, bool, bool) {
    if inputs.protocol == Protocol::Exhaustive {
        return (false, false, false);
    }

    let is_global_search = is_global(repo_options) && inputs.pattern_type != SearchType::Structural;
    let has_global_search_result_type =
        result_types.has(ResultTypes::FILE | ResultTypes::PATH | ResultTypes::SYMBOL);
    let is_indexed_search = basic.parameters.index() != YesNoOnly::No;
    let no_pattern = basic.is_empty_pattern();
    let no_file = !basic.parameters.exists(FIELD_FILE);
    let no_lang = !basic.parameters.exists(FIELD_LANG);
    let is_empty = no_pattern && no_file && no_lang;

    let repo_universe_search =
        is_global_search && is_indexed_search && has_global_search_result_type && !is_empty;
    let skip_repo_subset_search =
        is_empty || (repo_universe_search && inputs.on_sourcegraph_dot_com);
    let run_zoekt_over_repos = !repo_universe_search || inputs.on_sourcegraph_dot_com;

    (
        repo_universe_search,
        skip_repo_subset_search,
        run_zoekt_over_repos,
    )
}

// ---------------------------------------------------------------------------
// zoekt query atoms
// ---------------------------------------------------------------------------

fn visit_atoms(query: &Query, f: &mut dyn FnMut(&Query)) {
    match query {
        Query::And(children) | Query::Or(children) => {
            for child in children {
                visit_atoms(child, f);
            }
        }
        Query::Not(child) | Query::Type { child, .. } | Query::Boost { child, .. } => {
            visit_atoms(child, f);
        }
        // Everything else is an atom, including Repo/Language/Symbol/Const.
        atom => f(atom),
    }
}

/// `zoektQueryPatternsAsRegexps` (`job.go:935`).
pub fn zoekt_query_patterns_as_regexps(query: &Query) -> Vec<Regex> {
    let mut regexps = Vec::new();
    visit_atoms(query, &mut |atom| match atom {
        Query::Regexp {
            regexp,
            content,
            case_sensitive,
            ..
        } if !content => {
            let pattern = if *case_sensitive {
                regexp.clone()
            } else {
                case_insensitive_regexp(regexp)
            };
            regexps.push(Regex::new(&pattern).expect("zoekt regexp is a valid regular expression"));
        }
        Query::Substring {
            pattern,
            content,
            case_sensitive,
            ..
        } if !content => {
            let quoted = regexp_quote_meta(pattern);
            let pattern = if *case_sensitive {
                quoted
            } else {
                case_insensitive_regexp(&quoted)
            };
            regexps
                .push(Regex::new(&pattern).expect("zoekt substring is a valid regular expression"));
        }
        _ => {}
    });
    regexps
}

// ---------------------------------------------------------------------------
// orderRacingJobs
// ---------------------------------------------------------------------------

/// Minimal job tree used by [`order_racing_jobs`]. Mirrors the baseline's
/// `NoopJob`, `repoPagerJob`, `RepoSearchJob`, `searcher.TextSearchJob`,
/// `zoekt.RepoSubsetTextSearchJob`, `zoekt.GlobalTextSearchJob`,
/// `SequentialJob` and `ParallelJob`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RacingJob {
    Noop,
    RepoPager(Box<RacingJob>),
    RepoSearch,
    SearcherText,
    ZoektRepoSubsetText,
    ZoektGlobalText,
    Sequential {
        ensure_unique: bool,
        children: Vec<RacingJob>,
    },
    Parallel(Vec<RacingJob>),
}

fn map_racing_children(job: &RacingJob, f: &mut dyn FnMut(RacingJob) -> RacingJob) -> RacingJob {
    match job {
        RacingJob::Noop => RacingJob::Noop,
        RacingJob::RepoSearch => RacingJob::RepoSearch,
        RacingJob::SearcherText => RacingJob::SearcherText,
        RacingJob::ZoektRepoSubsetText => RacingJob::ZoektRepoSubsetText,
        RacingJob::ZoektGlobalText => RacingJob::ZoektGlobalText,
        RacingJob::RepoPager(child) => RacingJob::RepoPager(Box::new(map_racing(child, f))),
        RacingJob::Sequential {
            ensure_unique,
            children,
        } => RacingJob::Sequential {
            ensure_unique: *ensure_unique,
            children: children.iter().map(|child| map_racing(child, f)).collect(),
        },
        RacingJob::Parallel(children) => {
            RacingJob::Parallel(children.iter().map(|child| map_racing(child, f)).collect())
        }
    }
}

/// `job.Map`: children first (post-order), then `f` on the rebuilt node.
fn map_racing(job: &RacingJob, f: &mut dyn FnMut(RacingJob) -> RacingJob) -> RacingJob {
    let mapped = map_racing_children(job, f);
    f(mapped)
}

fn has_descendent_searcher_text(job: &RacingJob) -> bool {
    match job {
        RacingJob::SearcherText => true,
        RacingJob::RepoPager(child) => has_descendent_searcher_text(child),
        RacingJob::Sequential { children, .. } | RacingJob::Parallel(children) => {
            children.iter().any(has_descendent_searcher_text)
        }
        _ => false,
    }
}

fn has_descendent_zoekt_repo_subset_text(job: &RacingJob) -> bool {
    match job {
        RacingJob::ZoektRepoSubsetText => true,
        RacingJob::RepoPager(child) => has_descendent_zoekt_repo_subset_text(child),
        RacingJob::Sequential { children, .. } | RacingJob::Parallel(children) => {
            children.iter().any(has_descendent_zoekt_repo_subset_text)
        }
        _ => false,
    }
}

/// `orderRacingJobs` (`job.go:308`).
pub fn order_racing_jobs(job: &RacingJob) -> RacingJob {
    let mut collection = Vec::new();

    let new_job = {
        let mut f = |node: RacingJob| {
            if let RacingJob::RepoPager(_) = &node {
                if has_descendent_searcher_text(&node) {
                    collection.push(node.clone());
                    return RacingJob::Noop;
                }
            }
            node
        };
        map_racing(job, &mut f)
    };

    let new_job = {
        let mut f = |node: RacingJob| {
            if let RacingJob::RepoSearch = &node {
                collection.push(node.clone());
                return RacingJob::Noop;
            }
            node
        };
        map_racing(&new_job, &mut f)
    };

    if collection.is_empty() {
        return job.clone();
    }

    let mut seen_zoekt_repo_search = false;
    let new_job = {
        let mut f = |node: RacingJob| {
            if let RacingJob::RepoPager(_) = &node {
                if has_descendent_zoekt_repo_subset_text(&node) {
                    seen_zoekt_repo_search = true;
                    let mut children = vec![node];
                    children.extend(collection.clone());
                    return RacingJob::Sequential {
                        ensure_unique: false,
                        children,
                    };
                }
            }
            node
        };
        map_racing(&new_job, &mut f)
    };

    let mut seen_zoekt_global_search = false;
    let new_job = {
        let mut f = |node: RacingJob| {
            if let RacingJob::ZoektGlobalText = &node {
                if !seen_zoekt_global_search {
                    seen_zoekt_global_search = true;
                    let mut children = vec![node];
                    children.extend(collection.clone());
                    return RacingJob::Sequential {
                        ensure_unique: false,
                        children,
                    };
                }
            }
            node
        };
        map_racing(&new_job, &mut f)
    };

    if !seen_zoekt_repo_search && !seen_zoekt_global_search {
        // No Zoekt jobs, so no need to modify the tree. Return the original.
        return job.clone();
    }

    new_job
}

#[cfg(test)]
mod tests {
    use super::*;
    use sg_search_query::node::{Annotation, Parameter};

    fn basic(parameters: &[(&str, &str)], pattern: Option<Node>) -> Basic {
        let parameters = parameters
            .iter()
            .map(|(field, value)| Parameter {
                field: (*field).to_owned(),
                value: (*value).to_owned(),
                negated: false,
                annotation: Annotation::default(),
            })
            .collect();
        Basic::new(Parameters(parameters), pattern)
    }

    fn literal(value: &str) -> Node {
        Node::pattern(value, Labels::LITERAL, Default::default())
    }

    #[test]
    fn select_owners_matches_baseline() {
        assert!(is_select_owners_search(
            &select_path_from_string("file.owners").unwrap()
        ));
        assert!(!is_select_owners_search(
            &select_path_from_string("file.path").unwrap()
        ));
        assert!(!is_select_owners_search(
            &select_path_from_string("").unwrap_or_default()
        ));
        assert!(!is_select_owners_search(
            &select_path_from_string("file.bogus").unwrap_or_default()
        ));
    }

    #[test]
    fn exactly_one_repo_requires_anchored_literal() {
        let plain = parse_repository_revisions("^foo$").unwrap();
        assert!(exactly_one_repo(&[plain]));
        let dotted = parse_repository_revisions("^github.com/foo$").unwrap();
        assert!(!exactly_one_repo(&[dotted]));
        let escaped = parse_repository_revisions(r"^github\.com/foo$").unwrap();
        assert!(exactly_one_repo(&[escaped]));
        let unanchored = parse_repository_revisions("foo").unwrap();
        assert!(!exactly_one_repo(&[unanchored]));
    }

    #[test]
    fn duration_and_limits_match_baseline() {
        assert_eq!(parse_go_duration("1h"), Some(3_600_000_000_000));
        assert_eq!(parse_go_duration("500ms"), Some(500_000_000));
        assert_eq!(parse_go_duration("-1.5s"), Some(-1_500_000_000));
        assert_eq!(parse_go_duration("0"), Some(0));
        assert_eq!(parse_go_duration("bogus"), None);

        let b = basic(&[], Some(literal("foo")));
        assert_eq!(compute_file_match_limit(&b, 10), 10);
        let b = basic(&[("count", "5")], Some(literal("foo")));
        assert_eq!(compute_file_match_limit(&b, 10), 5);
        // `count:` without `timeout:` uses the capped max timeout.
        assert_eq!(
            timeout_duration(Protocol::Streaming, &b, 60),
            60_000_000_000
        );
        let b = basic(&[("timeout", "5s")], Some(literal("foo")));
        assert_eq!(timeout_duration(Protocol::Streaming, &b, 60), 5_000_000_000);
    }

    #[test]
    fn path_regexps_quote_literals_and_honor_case() {
        let b = basic(
            &[("file", "src/"), ("file", "docs/")],
            Some(literal("foo.bar")),
        );
        let types = ResultTypes::FILE | ResultTypes::PATH;
        let info = to_text_pattern_info(&b, types, &Features::default(), 10).unwrap();
        let compiled = get_path_regexps(&b, &info);
        let regexps: Vec<&str> = compiled.iter().map(Regex::as_str).collect();
        assert_eq!(regexps, vec!["(?i)src/", "(?i)docs/", "(?i)foo\\.bar"]);
    }

    #[test]
    fn order_racing_jobs_appends_after_zoekt() {
        let tree = RacingJob::Parallel(vec![
            RacingJob::RepoPager(Box::new(RacingJob::SearcherText)),
            RacingJob::ZoektGlobalText,
        ]);
        assert_eq!(
            order_racing_jobs(&tree),
            RacingJob::Parallel(vec![
                RacingJob::Noop,
                RacingJob::Sequential {
                    ensure_unique: false,
                    children: vec![
                        RacingJob::ZoektGlobalText,
                        RacingJob::RepoPager(Box::new(RacingJob::SearcherText)),
                    ],
                },
            ])
        );
    }
}
