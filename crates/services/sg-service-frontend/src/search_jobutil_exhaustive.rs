#![forbid(unsafe_code)]

//! `internal/search/job/jobutil/exhaustive_job.go`: the search-jobs product
//! facade over the job infrastructure.
//!
//! Ported:
//!
//! * [`new_exhaustive`]: the protocol/plan/select/file-predicate/regex/
//!   result-type validation sequence and the commit/diff and text job
//!   construction branches (`exhaustive_job.go:27-114`);
//! * [`has_predicates`] (`exhaustive_job.go:116-125`);
//! * [`Exhaustive::job`]: `repoPagerJob.child.Resolve(resolvedRepos{unindexed:
//!   [repoRevs]})`, exposed as [`ResolvedJob`] with the baseline's
//!   `Name`/`Children`/`Attributes` (`exhaustive_job.go:127-133`);
//! * [`Exhaustive::repository_rev_specs`] and
//!   [`Exhaustive::resolve_repository_rev_spec`]: `reposNewResolver(...)`
//!   wrappers that forward the constructed `RepoOptions`
//!   (`exhaustive_job.go:135-147`).
//!
//! The text/commit leaf `Name`/`Attributes` formats and
//! `TextPatternInfo.String()` are reproduced because they are the observable
//! output of [`ResolvedJob`]; `commit.QueryToGitQuery` (and a deterministic
//! subset of `gitprotocol.Reduce`) is modelled privately to render the commit
//! `query` attribute.
//!
//! Repository listing/resolution is injected through [`ExhaustiveResolver`]
//! instead of constructing the baseline's `repos.Resolver` from a real
//! DB/gitserver/zoekt client set. Full job-tree registration, execution,
//! GraphQL/HTTP/gRPC and the search backends are not ported.
//!
//! See `docs/overviews/m9_8hp_jobutil_exhaustive_overview.md`.

use sg_proto::gitserver as pb;
use sg_search_query::basic::{q_string_value, Basic, Plan};
use sg_search_query::fields::{
    FIELD_AFTER, FIELD_AUTHOR, FIELD_BEFORE, FIELD_COMMITTER, FIELD_CONTENT, FIELD_FILE,
    FIELD_LANG, FIELD_MESSAGE, FIELD_SELECT,
};
use sg_search_query::labels::Labels;
use sg_search_query::node::{go_quote, visit_field, Node, OperatorKind, Parameter, Pattern};
use sg_search_query::parser::{scan_predicate, SearchType};
use sg_zoekt::indexed_repo_revs::{MinimalRepo, RepositoryRevisions};
use sg_zoekt::languages::lang_to_file_regexp;

use crate::search_client::{Features, Protocol};
use crate::search_contexts::RevisionSpecifier;
use crate::search_job::{JobError, Verbosity};
use crate::search_jobutil_job_builders as jb;

// `exhaustive_job.go:29-32`.
/// `exhaustiveSupportedResultTypes`.
pub const EXHAUSTIVE_SUPPORTED_RESULT_TYPES: jb::ResultTypes = jb::ResultTypes(
    jb::ResultTypes::COMMIT.0
        | jb::ResultTypes::DIFF.0
        | jb::ResultTypes::FILE.0
        | jb::ResultTypes::PATH.0,
);
/// `exhaustiveDefaultResultTypes`.
pub const EXHAUSTIVE_DEFAULT_RESULT_TYPES: jb::ResultTypes =
    jb::ResultTypes(jb::ResultTypes::FILE.0 | jb::ResultTypes::PATH.0);

// ---------------------------------------------------------------------------
// Inputs
// ---------------------------------------------------------------------------

/// The `search.Inputs` fields [`new_exhaustive`] reads (`types.go:31-43`).
#[derive(Debug, Clone, PartialEq)]
pub struct Inputs {
    pub plan: Plan,
    /// `Plan.ToQ()`.
    pub query: Vec<Node>,
    pub pattern_type: SearchType,
    pub user_settings: jb::UserSettings,
    pub protocol: Protocol,
    pub features: Features,
    pub context_lines: i32,
    /// `authz.SubRepoEnabled(authz.DefaultSubRepoPermsChecker)`.
    pub sub_repo_enabled: bool,
}

impl Inputs {
    /// `Inputs.DefaultLimit` (`types.go:50-63`).
    pub fn default_limit(&self) -> i64 {
        match self.protocol {
            Protocol::Streaming => 10_000,
            Protocol::Batch => 30,
            Protocol::Exhaustive => 1_000_000,
        }
    }
}

// ---------------------------------------------------------------------------
// gitprotocol query nodes (`internal/gitserver/protocol`)
// ---------------------------------------------------------------------------

/// `gitprotocol.OperatorKind`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GitOperatorKind {
    And,
    Or,
    Not,
}

/// Minimal `gitprotocol.Node`. `RevAtTime` does not affect the rendered
/// `String`; `IgnoreCase` is kept because `mergeOrRegexp` keys on it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GitQueryNode {
    AuthorMatches {
        expr: String,
        ignore_case: bool,
    },
    CommitterMatches {
        expr: String,
        ignore_case: bool,
    },
    MessageMatches {
        expr: String,
        ignore_case: bool,
    },
    DiffMatches {
        expr: String,
        ignore_case: bool,
    },
    DiffModifiesFile {
        expr: String,
        ignore_case: bool,
    },
    /// `*gitprotocol.CommitBefore`; the rendered time is stored because
    /// `Time.String()` is the only observable form.
    CommitBefore {
        time: String,
    },
    /// `*gitprotocol.CommitAfter`.
    CommitAfter {
        time: String,
    },
    Boolean(bool),
    Operator {
        kind: GitOperatorKind,
        operands: Vec<GitQueryNode>,
    },
}

impl GitQueryNode {
    fn operator(kind: GitOperatorKind, operands: Vec<GitQueryNode>) -> GitQueryNode {
        GitQueryNode::Operator { kind, operands }
    }

    /// `estimatedNodeCost` (`search_reduce.go:243-267`).
    fn estimated_cost(&self) -> f64 {
        match self {
            GitQueryNode::Operator { operands, .. } => {
                operands.iter().map(GitQueryNode::estimated_cost).sum()
            }
            GitQueryNode::Boolean(_) => 0.0,
            GitQueryNode::CommitBefore { .. } | GitQueryNode::CommitAfter { .. } => 1.0,
            GitQueryNode::AuthorMatches { .. } | GitQueryNode::CommitterMatches { .. } => 5.0,
            GitQueryNode::MessageMatches { .. } => 10.0,
            GitQueryNode::DiffModifiesFile { .. } => 1000.0,
            GitQueryNode::DiffMatches { .. } => 10000.0,
        }
    }
}

impl std::fmt::Display for GitQueryNode {
    /// `gitprotocol.Node.String`.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            GitQueryNode::AuthorMatches { expr, .. } => {
                write!(f, "*protocol.AuthorMatches({expr})")
            }
            GitQueryNode::CommitterMatches { expr, .. } => {
                write!(f, "*protocol.CommitterMatches({expr})")
            }
            GitQueryNode::MessageMatches { expr, .. } => {
                write!(f, "*protocol.MessageMatches({expr})")
            }
            GitQueryNode::DiffMatches { expr, .. } => write!(f, "*protocol.DiffMatches({expr})"),
            GitQueryNode::DiffModifiesFile { expr, .. } => {
                write!(f, "*protocol.DiffModifiesFile({expr})")
            }
            GitQueryNode::CommitBefore { time } => {
                write!(f, "*protocol.CommitBefore({time})")
            }
            GitQueryNode::CommitAfter { time } => write!(f, "*protocol.CommitAfter({time})"),
            GitQueryNode::Boolean(value) => write!(f, "*protocol.Boolean({value})"),
            GitQueryNode::Operator { kind, operands } => {
                let (sep, prefix) = match kind {
                    GitOperatorKind::And => (" AND ", ""),
                    GitOperatorKind::Or => (" OR ", ""),
                    GitOperatorKind::Not => (" AND NOT ", "NOT "),
                };
                let children: Vec<String> = operands.iter().map(ToString::to_string).collect();
                write!(f, "({prefix}{})", children.join(sep))
            }
        }
    }
}

/// `gitprotocol.NewAnd`.
fn new_and(operands: Vec<GitQueryNode>) -> GitQueryNode {
    match operands.len() {
        0 => GitQueryNode::Boolean(true),
        1 => operands.into_iter().next().expect("one operand"),
        _ => {
            let mut flattened = Vec::with_capacity(operands.len());
            for operand in operands {
                match operand {
                    GitQueryNode::Operator {
                        kind: GitOperatorKind::And,
                        operands,
                    } => flattened.extend(operands),
                    other => flattened.push(other),
                }
            }
            GitQueryNode::operator(GitOperatorKind::And, flattened)
        }
    }
}

/// `gitprotocol.NewOr`.
fn new_or(operands: Vec<GitQueryNode>) -> GitQueryNode {
    match operands.len() {
        0 => GitQueryNode::Boolean(false),
        1 => operands.into_iter().next().expect("one operand"),
        _ => {
            let mut flattened = Vec::with_capacity(operands.len());
            for operand in operands {
                match operand {
                    GitQueryNode::Operator {
                        kind: GitOperatorKind::Or,
                        operands,
                    } => flattened.extend(operands),
                    other => flattened.push(other),
                }
            }
            GitQueryNode::operator(GitOperatorKind::Or, flattened)
        }
    }
}

/// `gitprotocol.NewNot`.
fn new_not(operand: GitQueryNode) -> GitQueryNode {
    match operand {
        GitQueryNode::Operator {
            kind: GitOperatorKind::Not,
            mut operands,
        } => operands.remove(0),
        other => GitQueryNode::operator(GitOperatorKind::Not, vec![other]),
    }
}

/// `propagateBoolean` (`search_reduce.go:52-97`).
fn propagate_boolean(node: GitQueryNode) -> GitQueryNode {
    let GitQueryNode::Operator { kind, operands } = node else {
        return node;
    };
    match kind {
        GitOperatorKind::Not => {
            if let Some(GitQueryNode::Boolean(value)) = operands.first() {
                GitQueryNode::Boolean(!value)
            } else {
                GitQueryNode::operator(GitOperatorKind::Not, operands)
            }
        }
        GitOperatorKind::And => {
            let mut filtered = Vec::with_capacity(operands.len());
            for operand in operands {
                match operand {
                    GitQueryNode::Boolean(false) => return GitQueryNode::Boolean(false),
                    GitQueryNode::Boolean(true) => {}
                    other => filtered.push(other),
                }
            }
            GitQueryNode::operator(GitOperatorKind::And, filtered)
        }
        GitOperatorKind::Or => {
            let mut filtered = Vec::with_capacity(operands.len());
            for operand in operands {
                match operand {
                    GitQueryNode::Boolean(true) => return GitQueryNode::Boolean(true),
                    GitQueryNode::Boolean(false) => {}
                    other => filtered.push(other),
                }
            }
            GitQueryNode::operator(GitOperatorKind::Or, filtered)
        }
    }
}

/// `flatten` (`search_reduce.go:129-146`).
fn flatten(node: GitQueryNode) -> GitQueryNode {
    let GitQueryNode::Operator { kind, operands } = node else {
        return node;
    };
    if kind == GitOperatorKind::Not {
        return GitQueryNode::operator(kind, operands);
    }
    let mut flattened = Vec::with_capacity(operands.len());
    for operand in operands {
        match operand {
            GitQueryNode::Operator {
                kind: nested_kind,
                operands: nested_operands,
            } if nested_kind == kind => flattened.extend(nested_operands),
            other => flattened.push(other),
        }
    }
    GitQueryNode::operator(kind, flattened)
}

/// `sortAndByCost` (`search_reduce.go:269-282`). The baseline's `sort.Slice`
/// is unstable; with the small operand counts this slice produces (and no
/// equal-cost ties in the corpus) the ordering is unambiguous.
fn sort_and_by_cost(node: GitQueryNode) -> GitQueryNode {
    let GitQueryNode::Operator {
        kind: GitOperatorKind::And,
        mut operands,
    } = node
    else {
        return node;
    };
    operands.sort_by(|left, right| {
        left.estimated_cost()
            .partial_cmp(&right.estimated_cost())
            .expect("finite costs")
    });
    GitQueryNode::operator(GitOperatorKind::And, operands)
}

/// `rewriteConjunctive` (`search_reduce.go:90-119`).
fn rewrite_conjunctive(node: GitQueryNode) -> GitQueryNode {
    let GitQueryNode::Operator {
        kind: GitOperatorKind::Or,
        operands,
    } = node
    else {
        return node;
    };

    let mut and_operands: Vec<Vec<GitQueryNode>> = Vec::new();
    let mut siblings: Vec<GitQueryNode> = Vec::new();
    for operand in operands {
        match operand {
            GitQueryNode::Operator {
                kind: GitOperatorKind::And,
                operands,
            } => and_operands.push(operands),
            other => siblings.push(other),
        }
    }

    if and_operands.is_empty() {
        // No nested and operands, so don't modify the node.
        return GitQueryNode::operator(GitOperatorKind::Or, siblings);
    }

    let distributed = distribute(&and_operands, &siblings);
    let new_and_operands = distributed
        .into_iter()
        .map(|row| GitQueryNode::operator(GitOperatorKind::Or, row))
        .collect();
    GitQueryNode::operator(GitOperatorKind::And, new_and_operands)
}

/// `distribute` (`search_reduce.go:121-135`): expand prefixes into every
/// choice of one node from each prefix, appending each to `nodes`.
fn distribute(prefixes: &[Vec<GitQueryNode>], nodes: &[GitQueryNode]) -> Vec<Vec<GitQueryNode>> {
    if prefixes.is_empty() {
        return vec![nodes.to_vec()];
    }

    let distributed = distribute(&prefixes[1..], nodes);
    let mut result = Vec::with_capacity(distributed.len() * prefixes[0].len());
    for prefix in &prefixes[0] {
        for row in &distributed {
            let mut new_row = row.clone();
            new_row.push(prefix.clone());
            result.push(new_row);
        }
    }
    result
}

/// `mergeOrRegexp` (`search_reduce.go:159-235`). The baseline iterates a Go
/// map, whose order is unspecified; this port uses a `BTreeMap` keyed by node
/// kind (and `IgnoreCase`) so a single mergeable key (the corpus case) is
/// byte-identical and multiple keys are deterministic rather than random.
fn merge_or_regexp(node: GitQueryNode) -> GitQueryNode {
    let GitQueryNode::Operator {
        kind: GitOperatorKind::Or,
        operands,
    } = node
    else {
        return node;
    };

    fn union(left: &str, right: &str) -> String {
        format!("(?:{left})|(?:{right})")
    }

    #[derive(PartialEq, Eq, PartialOrd, Ord)]
    enum Key {
        Author(bool),
        Committer(bool),
        Message(bool),
        Diff(bool),
        DiffModifiesFile(bool),
    }

    let mut unmergeable: Vec<GitQueryNode> = Vec::new();
    let mut mergeable: std::collections::BTreeMap<Key, GitQueryNode> =
        std::collections::BTreeMap::new();
    for operand in operands {
        match operand {
            GitQueryNode::AuthorMatches { expr, ignore_case } => {
                let key = Key::Author(ignore_case);
                let expr = match mergeable.remove(&key) {
                    Some(GitQueryNode::AuthorMatches { expr: prev, .. }) => union(&prev, &expr),
                    _ => expr,
                };
                mergeable.insert(key, GitQueryNode::AuthorMatches { expr, ignore_case });
            }
            GitQueryNode::CommitterMatches { expr, ignore_case } => {
                let key = Key::Committer(ignore_case);
                let expr = match mergeable.remove(&key) {
                    Some(GitQueryNode::CommitterMatches { expr: prev, .. }) => union(&prev, &expr),
                    _ => expr,
                };
                mergeable.insert(key, GitQueryNode::CommitterMatches { expr, ignore_case });
            }
            GitQueryNode::MessageMatches { expr, ignore_case } => {
                let key = Key::Message(ignore_case);
                let expr = match mergeable.remove(&key) {
                    Some(GitQueryNode::MessageMatches { expr: prev, .. }) => union(&prev, &expr),
                    _ => expr,
                };
                mergeable.insert(key, GitQueryNode::MessageMatches { expr, ignore_case });
            }
            GitQueryNode::DiffMatches { expr, ignore_case } => {
                let key = Key::Diff(ignore_case);
                let expr = match mergeable.remove(&key) {
                    Some(GitQueryNode::DiffMatches { expr: prev, .. }) => union(&prev, &expr),
                    _ => expr,
                };
                mergeable.insert(key, GitQueryNode::DiffMatches { expr, ignore_case });
            }
            GitQueryNode::DiffModifiesFile { expr, ignore_case } => {
                let key = Key::DiffModifiesFile(ignore_case);
                let expr = match mergeable.remove(&key) {
                    Some(GitQueryNode::DiffModifiesFile { expr: prev, .. }) => union(&prev, &expr),
                    _ => expr,
                };
                mergeable.insert(key, GitQueryNode::DiffModifiesFile { expr, ignore_case });
            }
            other => unmergeable.push(other),
        }
    }

    let mut result = unmergeable;
    result.extend(mergeable.into_values());
    GitQueryNode::operator(GitOperatorKind::Or, result)
}

/// `Reduce` with the baseline's default passes (`search_reduce.go:7-37`).
fn reduce(node: GitQueryNode) -> GitQueryNode {
    let mut node = match node {
        GitQueryNode::Operator { kind, operands } => {
            GitQueryNode::operator(kind, operands.into_iter().map(reduce).collect::<Vec<_>>())
        }
        other => other,
    };
    node = propagate_boolean(node);
    node = rewrite_conjunctive(node);
    node = flatten(node);
    node = merge_or_regexp(node);
    node = sort_and_by_cost(node);
    node
}

/// `queryPatternToPredicate` (`commit.go:238-270`).
fn query_pattern_to_predicate(
    node: Option<&Node>,
    case_sensitive: bool,
    diff: bool,
) -> Option<GitQueryNode> {
    let node = node?;
    match node {
        Node::Operator(operator) => {
            let operands = operator
                .operands
                .iter()
                .filter_map(|operand| {
                    query_pattern_to_predicate(Some(operand), case_sensitive, diff)
                })
                .collect::<Vec<_>>();
            match operator.kind {
                OperatorKind::And => Some(new_and(operands)),
                OperatorKind::Or => Some(new_or(operands)),
                OperatorKind::Concat => None,
            }
        }
        Node::Pattern(pattern) => Some(pattern_atom_to_predicate(pattern, case_sensitive, diff)),
        Node::Parameter(_) => None,
    }
}

/// `patternAtomToPredicate` (`commit.go:272-286`).
fn pattern_atom_to_predicate(pattern: &Pattern, case_sensitive: bool, diff: bool) -> GitQueryNode {
    let expr = pattern.regexp_pattern();
    let ignore_case = !case_sensitive;
    let atom = if diff {
        GitQueryNode::DiffMatches { expr, ignore_case }
    } else {
        GitQueryNode::MessageMatches { expr, ignore_case }
    };
    if pattern.negated {
        new_not(atom)
    } else {
        atom
    }
}

/// `queryParameterToPredicate` (`commit.go:288-325`).
fn query_parameter_to_predicate(
    parameter: &Parameter,
    case_sensitive: bool,
    diff: bool,
) -> Option<GitQueryNode> {
    let value = parameter.value.clone();
    let ignore_case = !case_sensitive;
    let predicate = match parameter.field.as_str() {
        FIELD_AUTHOR => Some(GitQueryNode::AuthorMatches {
            expr: value,
            ignore_case,
        }),
        FIELD_COMMITTER => Some(GitQueryNode::CommitterMatches {
            expr: value,
            ignore_case,
        }),
        FIELD_BEFORE => Some(GitQueryNode::CommitBefore {
            time: parse_git_date(&value)?,
        }),
        FIELD_AFTER => Some(GitQueryNode::CommitAfter {
            time: parse_git_date(&value)?,
        }),
        FIELD_MESSAGE => Some(GitQueryNode::MessageMatches {
            expr: value,
            ignore_case,
        }),
        FIELD_CONTENT => Some(if diff {
            GitQueryNode::DiffMatches {
                expr: value,
                ignore_case,
            }
        } else {
            GitQueryNode::MessageMatches {
                expr: value,
                ignore_case,
            }
        }),
        FIELD_FILE => Some(GitQueryNode::DiffModifiesFile {
            expr: value,
            ignore_case,
        }),
        FIELD_LANG => Some(GitQueryNode::DiffModifiesFile {
            expr: lang_to_file_regexp(&value),
            ignore_case: true,
        }),
        _ => None,
    };
    match (parameter.negated, predicate) {
        (true, Some(predicate)) => Some(new_not(predicate)),
        (_, predicate) => predicate,
    }
}

/// `commit.QueryToGitQuery` (`commit.go:213-236`).
fn query_to_git_query(basic: &Basic, diff: bool) -> GitQueryNode {
    let case_sensitive = basic.parameters.is_case_sensitive();
    let mut nodes = Vec::with_capacity(basic.parameters.len() + 2);
    for parameter in basic.parameters.iter() {
        if parameter.annotation.labels.is_set(Labels::IS_PREDICATE) {
            continue;
        }
        if let Some(predicate) = query_parameter_to_predicate(parameter, case_sensitive, diff) {
            nodes.push(predicate);
        }
    }
    if let Some(predicate) =
        query_pattern_to_predicate(basic.pattern.as_ref(), case_sensitive, diff)
    {
        nodes.push(predicate);
    }
    reduce(new_and(nodes))
}

/// Translate the exhaustive commit leaf's [`GitQueryNode`] into the gitserver
/// `QueryNode` proto executed by `search_commit_job` (the same conversion as
/// [`crate::search_commit_job::query_to_git_query`], but from the already
/// reduced tree).
pub fn git_query_node_to_proto(node: &GitQueryNode) -> pb::QueryNode {
    use pb::query_node::Value;

    fn timestamp(value: &str) -> prost_types::Timestamp {
        match crate::search_commit_job::parse_git_date(value) {
            Some(datetime) => prost_types::Timestamp {
                seconds: datetime.unix_timestamp(),
                nanos: datetime.nanosecond() as i32,
            },
            // `time.Time{}` in Go (`search_commit_job::GO_ZERO_SECONDS`).
            None => prost_types::Timestamp {
                seconds: -62_135_596_800,
                nanos: 0,
            },
        }
    }

    let value = match node {
        GitQueryNode::Boolean(value) => Value::Boolean(pb::BooleanNode { value: *value }),
        GitQueryNode::AuthorMatches { expr, ignore_case } => {
            Value::AuthorMatches(pb::AuthorMatchesNode {
                expr: expr.clone(),
                ignore_case: *ignore_case,
            })
        }
        GitQueryNode::CommitterMatches { expr, ignore_case } => {
            Value::CommitterMatches(pb::CommitterMatchesNode {
                expr: expr.clone(),
                ignore_case: *ignore_case,
            })
        }
        GitQueryNode::CommitBefore { time } => Value::CommitBefore(pb::CommitBeforeNode {
            timestamp: Some(timestamp(time)),
        }),
        GitQueryNode::CommitAfter { time } => Value::CommitAfter(pb::CommitAfterNode {
            timestamp: Some(timestamp(time)),
        }),
        GitQueryNode::MessageMatches { expr, ignore_case } => {
            Value::MessageMatches(pb::MessageMatchesNode {
                expr: expr.clone(),
                ignore_case: *ignore_case,
            })
        }
        GitQueryNode::DiffMatches { expr, ignore_case } => {
            Value::DiffMatches(pb::DiffMatchesNode {
                expr: expr.clone(),
                ignore_case: *ignore_case,
            })
        }
        GitQueryNode::DiffModifiesFile { expr, ignore_case } => {
            Value::DiffModifiesFile(pb::DiffModifiesFileNode {
                expr: expr.clone(),
                ignore_case: *ignore_case,
            })
        }
        GitQueryNode::Operator { kind, operands } => Value::Operator(pb::OperatorNode {
            kind: match kind {
                GitOperatorKind::And => pb::OperatorKind::And as i32,
                GitOperatorKind::Or => pb::OperatorKind::Or as i32,
                GitOperatorKind::Not => pb::OperatorKind::Not as i32,
            },
            operands: operands.iter().map(git_query_node_to_proto).collect(),
        }),
    };
    pb::QueryNode { value: Some(value) }
}

/// `gitdomain.ParseGitDate` (`date.go:17-90`), returning the rendered
/// `time.Time.String()` that `CommitBefore`/`CommitAfter` emit.
///
/// The deterministic layouts are ported; the `go-naturaldate` fallback is
/// not (see the slice overview §4).
fn parse_git_date(value: &str) -> Option<String> {
    use time::format_description::well_known::Rfc3339;
    use time::{Date, OffsetDateTime, PrimitiveDateTime, Time, UtcOffset};

    fn render(datetime: OffsetDateTime, location: &str) -> String {
        let offset = datetime.offset();
        let offset_str = if offset.is_utc() {
            "+0000".to_owned()
        } else {
            let seconds = offset.whole_seconds();
            let sign = if seconds < 0 { '-' } else { '+' };
            let seconds = seconds.abs();
            format!("{sign}{:02}{:02}", seconds / 3600, (seconds % 3600) / 60)
        };
        format!(
            "{:04}-{:02}-{:02} {:02}:{:02}:{:02} {offset_str} {location}",
            datetime.year(),
            datetime.month() as u8,
            datetime.day(),
            datetime.hour(),
            datetime.minute(),
            datetime.second(),
        )
    }

    fn numeric_location(offset: UtcOffset) -> String {
        if offset.is_utc() {
            "+0000".to_owned()
        } else {
            let seconds = offset.whole_seconds();
            let sign = if seconds < 0 { '-' } else { '+' };
            let seconds = seconds.abs();
            format!("{sign}{:02}{:02}", seconds / 3600, (seconds % 3600) / 60)
        }
    }

    // Git internal format: epoch seconds plus an optional ±HHMM offset.
    if let Some((epoch, zone)) = parse_git_internal(value) {
        let datetime = OffsetDateTime::from_unix_timestamp(epoch).ok()?;
        let offset = match zone {
            Some((sign, hours, minutes)) => {
                let seconds = ((hours * 3600 + minutes * 60) * sign) as i32;
                UtcOffset::from_whole_seconds(seconds).ok()?
            }
            None => UtcOffset::UTC,
        };
        let datetime = datetime.to_offset(offset);
        return Some(render(datetime, &numeric_location(offset)));
    }

    // Deterministic fixed layouts (in the baseline's order).
    const DATE: &[time::format_description::FormatItem<'static>] =
        time::macros::format_description!("[year]-[month]-[day]");
    const DATE_TIME: &[time::format_description::FormatItem<'static>] =
        time::macros::format_description!("[year]-[month]-[day]T[hour]:[minute]:[second]");
    const DATE_TIME_SPACE: &[time::format_description::FormatItem<'static>] =
        time::macros::format_description!("[year]-[month]-[day] [hour]:[minute]:[second]");
    const DATE_TIME_SPACE_ZONE: &[time::format_description::FormatItem<'static>] = time::macros::format_description!(
        "[year]-[month]-[day] [hour]:[minute]:[second][offset_hour sign:mandatory]:[offset_minute]"
    );
    const RFC2822: &[time::format_description::FormatItem<'static>] =
        time::macros::format_description!(
            "[weekday repr:short], [day] [month repr:short] [year] [hour]:[minute]:[second] [offset_hour sign:mandatory][offset_minute]"
        );
    const DOTTED_YMD: &[time::format_description::FormatItem<'static>] =
        time::macros::format_description!("[year].[month].[day]");
    const SLASH_MDY: &[time::format_description::FormatItem<'static>] =
        time::macros::format_description!("[month]/[day]/[year]");
    const DOTTED_DMY: &[time::format_description::FormatItem<'static>] =
        time::macros::format_description!("[day].[month].[year]");

    if let Ok(date) = Date::parse(value, DATE) {
        return Some(render(date.with_time(Time::MIDNIGHT).assume_utc(), "UTC"));
    }
    if let Ok(datetime) = OffsetDateTime::parse(value, &Rfc3339) {
        let name = if datetime.offset().is_utc() {
            "UTC".to_owned()
        } else {
            numeric_location(datetime.offset())
        };
        return Some(render(datetime, &name));
    }
    if let Ok(datetime) = PrimitiveDateTime::parse(value, DATE_TIME) {
        return Some(render(datetime.assume_utc(), "UTC"));
    }
    if let Ok(datetime) = OffsetDateTime::parse(value, DATE_TIME_SPACE_ZONE) {
        let name = if datetime.offset().is_utc() {
            "UTC".to_owned()
        } else {
            numeric_location(datetime.offset())
        };
        return Some(render(datetime, &name));
    }
    if let Ok(datetime) = PrimitiveDateTime::parse(value, DATE_TIME_SPACE) {
        return Some(render(datetime.assume_utc(), "UTC"));
    }
    if let Ok(datetime) = OffsetDateTime::parse(value, RFC2822) {
        return Some(render(datetime, &numeric_location(datetime.offset())));
    }
    if let Ok(date) = Date::parse(value, DOTTED_YMD) {
        return Some(render(date.with_time(Time::MIDNIGHT).assume_utc(), "UTC"));
    }
    if let Ok(date) = Date::parse(value, SLASH_MDY) {
        return Some(render(date.with_time(Time::MIDNIGHT).assume_utc(), "UTC"));
    }
    if let Ok(date) = Date::parse(value, DOTTED_DMY) {
        return Some(render(date.with_time(Time::MIDNIGHT).assume_utc(), "UTC"));
    }
    if let Some(date) = parse_simple_date(value) {
        return Some(render(date.with_time(Time::MIDNIGHT).assume_utc(), "UTC"));
    }
    // `go-naturaldate` is not ported.
    None
}

/// `parseGitInternalFormat` (`date.go:93-131`).
type GitInternalDate = (i64, Option<(i64, i64, i64)>);

fn parse_git_internal(value: &str) -> Option<GitInternalDate> {
    let mut parts = value.split(' ');
    let epoch_str = parts.next()?;
    if epoch_str.len() < 5 || !epoch_str.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    let epoch: i64 = epoch_str.parse().ok()?;
    match parts.next() {
        None => Some((epoch, None)),
        Some(zone) if parts.next().is_none() => {
            let bytes = zone.as_bytes();
            if bytes.len() != 5
                || !matches!(bytes[0], b'+' | b'-')
                || !bytes[1..].iter().all(|byte| byte.is_ascii_digit())
            {
                return None;
            }
            let hours: i64 = zone[1..3].parse().ok()?;
            let minutes: i64 = zone[3..5].parse().ok()?;
            let sign = if bytes[0] == b'-' { -1 } else { 1 };
            Some((epoch, Some((sign, hours, minutes))))
        }
        Some(_) => None,
    }
}

/// `parseSimpleDate` (`date.go:135-190`): `Month D, YYYY` / `D Month YYYY`.
fn parse_simple_date(value: &str) -> Option<time::Date> {
    fn month(name: &str) -> Option<time::Month> {
        match name.to_lowercase().as_str() {
            "january" | "jan" => Some(time::Month::January),
            "february" | "feb" => Some(time::Month::February),
            "march" | "mar" => Some(time::Month::March),
            "april" | "apr" => Some(time::Month::April),
            "may" => Some(time::Month::May),
            "june" | "jun" => Some(time::Month::June),
            "july" | "jul" => Some(time::Month::July),
            "august" | "aug" => Some(time::Month::August),
            "september" | "sep" | "sept" => Some(time::Month::September),
            "october" | "oct" => Some(time::Month::October),
            "november" | "nov" => Some(time::Month::November),
            "december" | "dec" => Some(time::Month::December),
            _ => None,
        }
    }

    let tokens: Vec<&str> = value
        .split(|c: char| c.is_whitespace() || c == ',')
        .filter(|token| !token.is_empty())
        .collect();
    let (month_token, day_token, year_token) = match tokens.as_slice() {
        [month, day, year] if month.chars().next().is_some_and(|c| c.is_alphabetic()) => {
            (month, day, year)
        }
        [day, month, year] if month.chars().next().is_some_and(|c| c.is_alphabetic()) => {
            (month, day, year)
        }
        _ => return None,
    };
    let month = month(month_token)?;
    let day: u8 = day_token.parse().ok()?;
    let year: i32 = year_token.parse().ok()?;
    time::Date::from_calendar_date(year, month, day).ok()
}

// ---------------------------------------------------------------------------
// Job construction
// ---------------------------------------------------------------------------

/// `searcher.TextSearchJob` as built by `NewTextSearchJob` (`job.go:282-304`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SearcherTextJob {
    pub pattern_info: jb::TextPatternInfo,
    /// `PathRegexps`, rendered as their pattern source.
    pub path_regexps: Vec<String>,
    pub use_full_deadline: bool,
    pub indexed: bool,
    pub num_context_lines: i32,
}

/// `commit.SearchJob` as built by `NewExhaustive` (`exhaustive_job.go:74-87`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommitSearchJob {
    pub query: GitQueryNode,
    pub diff: bool,
    pub limit: i64,
    pub include_modified_files: bool,
    pub concurrency: i32,
}

/// The child of the exhaustive `repoPagerJob`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExhaustiveChild {
    SearcherText(SearcherTextJob),
    Commit(CommitSearchJob),
}

/// `repoPagerJob` reduced to the fields `NewExhaustive`/`Exhaustive` read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RepoPagerJob {
    pub repo_opts: jb::RepoOptions,
    /// The commit branch sets `repoOptionsCopy.OnlyCloned = true`; the text
    /// branch leaves it at the zero value.
    pub only_cloned: bool,
    pub contains_ref_globs: bool,
    pub skip_partitioning: bool,
    pub child: ExhaustiveChild,
}

/// `jobutil.Exhaustive`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Exhaustive {
    pub repo_pager_job: RepoPagerJob,
}

/// `hasPredicates` (`exhaustive_job.go:116-125`): first positive value, then
/// negated values; the first registered predicate wins.
pub fn has_predicates(field: &str, query: &[Node]) -> Option<String> {
    let mut values = Vec::new();
    let mut negated = Vec::new();
    visit_field(query, field, &mut |value, is_negated, _annotation| {
        if is_negated {
            negated.push(value.to_owned());
        } else {
            values.push(value.to_owned());
        }
    });
    for value in values.iter().chain(negated.iter()) {
        if let Some((predicate, _)) = scan_predicate(field, value.as_bytes()) {
            return Some(predicate);
        }
    }
    None
}

/// `NewExhaustive` (`exhaustive_job.go:27-114`).
pub fn new_exhaustive(inputs: &Inputs) -> Result<Exhaustive, JobError> {
    if inputs.protocol != Protocol::Exhaustive {
        return Err(JobError(
            "only works for exhaustive search inputs".to_owned(),
        ));
    }

    if inputs.plan.len() != 1 {
        return Err(JobError(
            "Invalid query: Search Jobs does not support using AND/OR with filters (for example \"file:.go or file:.rs\"). Consider splitting your query into multiple subqueries and creating separate search jobs for each.".to_owned(),
        ));
    }

    let basic = &inputs.plan[0];

    if !q_string_value(&basic.to_parse_tree(), FIELD_SELECT).is_empty() {
        return Err(JobError(
            "select: is not supported in Search Jobs".to_owned(),
        ));
    }

    if let Some(predicate) = has_predicates(FIELD_FILE, &inputs.query) {
        return Err(JobError(format!(
            "file predicates are not supported. Got {predicate}"
        )));
    }

    if inputs.pattern_type == SearchType::Regex {
        if let Some(Node::Pattern(term)) = &basic.pattern {
            if term.value == ".*" {
                return Err(JobError("regex search with .* is not supported".to_owned()));
            }
        }
    }

    let repo_options = jb::to_repo_options(basic, &inputs.user_settings);
    let result_types =
        jb::compute_result_types(basic, inputs.pattern_type, EXHAUSTIVE_DEFAULT_RESULT_TYPES);

    let unsupported = jb::ResultTypes(result_types.0 & !EXHAUSTIVE_SUPPORTED_RESULT_TYPES.0);
    if unsupported.0 != 0 {
        return Err(JobError(format!(
            "your query contains the following type filters: {}. However Search Jobs only supports: {}.",
            result_types.as_string(),
            EXHAUSTIVE_SUPPORTED_RESULT_TYPES.as_string(),
        )));
    }

    let (child, only_cloned, skip_partitioning) = if result_types.has(jb::ResultTypes(
        jb::ResultTypes::COMMIT.0 | jb::ResultTypes::DIFF.0,
    )) {
        let (_, _, own) = jb::is_ownership_search(basic);
        let diff = result_types.has(jb::ResultTypes::DIFF);
        let limit = jb::parameters_max_results(&basic.parameters, inputs.default_limit());
        let child = ExhaustiveChild::Commit(CommitSearchJob {
            query: query_to_git_query(basic, diff),
            diff,
            limit,
            include_modified_files: inputs.sub_repo_enabled || own,
            concurrency: 4,
        });
        (child, true, true)
    } else if result_types.has(jb::ResultTypes(
        jb::ResultTypes::FILE.0 | jb::ResultTypes::PATH.0,
    )) {
        let use_full_deadline = jb::parameters_timeout_nanos(&basic.parameters).is_some()
            || jb::parameters_count(&basic.parameters).is_some()
            || inputs.protocol != Protocol::Batch;
        let pattern_info = jb::to_text_pattern_info(
            basic,
            result_types,
            &inputs.features,
            inputs.default_limit(),
        )
        .map_err(JobError)?;
        let path_regexps = jb::get_path_regexps(basic, &pattern_info)
            .into_iter()
            .map(|regex| regex.as_str().to_owned())
            .collect();
        let child = ExhaustiveChild::SearcherText(SearcherTextJob {
            pattern_info,
            path_regexps,
            use_full_deadline,
            indexed: false,
            num_context_lines: inputs.context_lines,
        });
        (child, false, false)
    } else {
        return Err(JobError(format!(
            "internal error: unsupported result types {}",
            result_types.as_string()
        )));
    };

    let contains_ref_globs = repo_options
        .repo_filters
        .iter()
        .any(|filter| filter.revs.iter().any(RevisionSpecifier::has_ref_glob));

    Ok(Exhaustive {
        repo_pager_job: RepoPagerJob {
            repo_opts: repo_options,
            only_cloned,
            contains_ref_globs,
            skip_partitioning,
            child,
        },
    })
}

/// `TextPatternInfo.String()` (`types.go:336-369`).
pub fn text_pattern_info_string(pattern_info: &jb::TextPatternInfo) -> String {
    let mut args = vec![pattern_info.query.to_string()];

    if pattern_info.is_structural_pat {
        if pattern_info.comby_rule.is_empty() {
            args.push("comby".to_owned());
        } else {
            args.push(format!("comby:{}", pattern_info.comby_rule));
        }
    }
    if pattern_info.is_case_sensitive {
        args.push("case".to_owned());
    }
    if !pattern_info.pattern_matches_content {
        args.push("nocontent".to_owned());
    }
    if !pattern_info.pattern_matches_path {
        args.push("nopath".to_owned());
    }
    if pattern_info.file_match_limit > 0 {
        args.push(format!("filematchlimit:{}", pattern_info.file_match_limit));
    }
    for language in &pattern_info.languages {
        args.push(format!("lang:{language}"));
    }

    let path = if pattern_info.path_patterns_are_case_sensitive {
        "F"
    } else {
        "f"
    };
    if !pattern_info.exclude_paths.is_empty() {
        args.push(format!("-{path}:{}", go_quote(&pattern_info.exclude_paths)));
    }
    for include in &pattern_info.include_paths {
        args.push(format!("{path}:{}", go_quote(include)));
    }

    format!("TextPatternInfo{{{}}}", args.join(","))
}

fn bool_string(value: bool) -> String {
    if value {
        "true".to_owned()
    } else {
        "false".to_owned()
    }
}

fn string_slice<'a, I: IntoIterator<Item = &'a String>>(values: I) -> String {
    let joined = values.into_iter().cloned().collect::<Vec<_>>().join(" ");
    format!("[{joined}]")
}

/// The job returned by `Exhaustive.Job`: one `search.RepositoryRevisions`
/// resolved into the pager child.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedJob {
    child: ExhaustiveChild,
    num_repos: usize,
}

impl ResolvedJob {
    /// `Describer.Name`.
    pub fn name(&self) -> &'static str {
        match &self.child {
            ExhaustiveChild::SearcherText(_) => "SearcherTextSearchJob",
            ExhaustiveChild::Commit(job) => {
                if job.diff {
                    "DiffSearchJob"
                } else {
                    "CommitSearchJob"
                }
            }
        }
    }

    /// `Describer.Children`; the leaf jobs have none.
    pub fn children(&self) -> Vec<&ResolvedJob> {
        Vec::new()
    }

    /// `Describer.Attributes`, normalized to `(key, value)` strings.
    pub fn attributes(&self, verbosity: Verbosity) -> Vec<(String, String)> {
        let mut attributes = Vec::new();
        match &self.child {
            ExhaustiveChild::SearcherText(job) => {
                if verbosity == Verbosity::Max {
                    attributes.push((
                        "useFullDeadline".to_owned(),
                        bool_string(job.use_full_deadline),
                    ));
                    attributes.push((
                        "patternInfo".to_owned(),
                        text_pattern_info_string(&job.pattern_info),
                    ));
                    attributes.push(("numRepos".to_owned(), self.num_repos.to_string()));
                    attributes.push((
                        "pathRegexps".to_owned(),
                        string_slice(job.path_regexps.iter()),
                    ));
                }
                if matches!(verbosity, Verbosity::Basic | Verbosity::Max) {
                    attributes.push(("indexed".to_owned(), bool_string(job.indexed)));
                }
            }
            ExhaustiveChild::Commit(job) => {
                if verbosity == Verbosity::Max {
                    attributes.push((
                        "includeModifiedFiles".to_owned(),
                        bool_string(job.include_modified_files),
                    ));
                }
                if matches!(verbosity, Verbosity::Basic | Verbosity::Max) {
                    attributes.push(("query".to_owned(), job.query.to_string()));
                    attributes.push(("diff".to_owned(), bool_string(job.diff)));
                    attributes.push(("limit".to_owned(), job.limit.to_string()));
                }
            }
        }
        attributes
    }
}

// ---------------------------------------------------------------------------
// resolver injection (`reposNewResolver` / `repos.Resolver`)
// ---------------------------------------------------------------------------

/// `repos.RepoRevSpecs`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RepoRevSpecs {
    pub repo: MinimalRepo,
    pub revs: Vec<RevisionSpecifier>,
}

/// The page sequence `repos.Resolver.IterateRepoRevs` yields, plus the
/// terminal iterator error.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct RepoRevSpecsIterator {
    pub pages: Vec<Vec<RepoRevSpecs>>,
    pub error: Option<JobError>,
}

/// `repos.Resolved`.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Resolved {
    pub repo_revs: Vec<RepositoryRevisions>,
    pub backends_missing: i64,
}

/// Minimal stand-in for `repos.Resolver` (`repos.go:84-185`). The baseline
/// builds it from the runtime clients; this port injects the two methods the
/// `Exhaustive` wrappers call and forwards the constructed `RepoOptions`.
pub trait ExhaustiveResolver {
    /// `Resolver.IterateRepoRevs`.
    fn iterate_repo_revs(&self, repo_opts: &jb::RepoOptions) -> RepoRevSpecsIterator;
    /// `Resolver.ResolveRevSpecs`.
    fn resolve_rev_specs(
        &self,
        repo_opts: &jb::RepoOptions,
        repo_rev_specs: &[RepoRevSpecs],
    ) -> Result<Resolved, JobError>;
}

/// `reposNewResolver` (`exhaustive_job.go:145-147`). The baseline constructs a
/// `repos.Resolver` from `clients`; here the resolver is injected.
pub fn repos_new_resolver(resolver: &dyn ExhaustiveResolver) -> &dyn ExhaustiveResolver {
    resolver
}

impl Exhaustive {
    /// `Exhaustive.Job` (`exhaustive_job.go:127-133`). The single
    /// `RepositoryRevisions` becomes the unindexed repo set assigned by
    /// `setRepos`.
    pub fn job(&self, _repo_revs: &RepositoryRevisions) -> ResolvedJob {
        ResolvedJob {
            child: self.repo_pager_job.child.clone(),
            num_repos: 1,
        }
    }

    /// `Exhaustive.RepositoryRevSpecs` (`exhaustive_job.go:136-138`).
    pub fn repository_rev_specs(&self, resolver: &dyn ExhaustiveResolver) -> RepoRevSpecsIterator {
        repos_new_resolver(resolver).iterate_repo_revs(&self.repo_pager_job.repo_opts)
    }

    /// `Exhaustive.ResolveRepositoryRevSpec` (`exhaustive_job.go:141-143`).
    pub fn resolve_repository_rev_spec(
        &self,
        resolver: &dyn ExhaustiveResolver,
        repo_rev_specs: &[RepoRevSpecs],
    ) -> Result<Resolved, JobError> {
        repos_new_resolver(resolver)
            .resolve_rev_specs(&self.repo_pager_job.repo_opts, repo_rev_specs)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sg_search_query::basic::plan_to_q;
    use sg_search_query::pipeline::pipeline_query;

    fn inputs(query: &str, search_type: SearchType) -> Inputs {
        let plan = pipeline_query(query, search_type).expect("valid query");
        Inputs {
            query: plan_to_q(&plan),
            plan,
            pattern_type: search_type,
            user_settings: jb::UserSettings::default(),
            protocol: Protocol::Exhaustive,
            features: Features::default(),
            context_lines: 1,
            sub_repo_enabled: false,
        }
    }

    fn repo_revs() -> RepositoryRevisions {
        RepositoryRevisions {
            repo: sg_zoekt::indexed_repo_revs::RepositoryRevisionsRepo {
                id: 1,
                name: "repo1".to_owned(),
            },
            revs: vec!["HEAD".to_owned()],
        }
    }

    #[test]
    fn rejects_wrong_protocol() {
        let mut inputs = inputs("foo type:file", SearchType::Standard);
        inputs.protocol = Protocol::Streaming;
        assert_eq!(
            new_exhaustive(&inputs).unwrap_err().0,
            "only works for exhaustive search inputs"
        );
    }

    #[test]
    fn rejects_multiple_plans() {
        let inputs = inputs(
            "type:file index:no (repo:repo1 or repo:repo2) content",
            SearchType::Standard,
        );
        assert!(inputs.plan.len() > 1);
        assert!(new_exhaustive(&inputs)
            .unwrap_err()
            .0
            .starts_with("Invalid query: Search Jobs does not support"));
    }

    #[test]
    fn rejects_select() {
        let inputs = inputs("index:no foo select:file.owners", SearchType::Standard);
        assert_eq!(
            new_exhaustive(&inputs).unwrap_err().0,
            "select: is not supported in Search Jobs"
        );
    }

    #[test]
    fn rejects_file_predicates() {
        let inputs = inputs(
            "type:file index:no file:has.content(content)",
            SearchType::Standard,
        );
        assert_eq!(
            new_exhaustive(&inputs).unwrap_err().0,
            "file predicates are not supported. Got has.content(content)"
        );
    }

    #[test]
    fn rejects_regex_dot_star() {
        let inputs = inputs("type:file index:no r:repo .*", SearchType::Regex);
        assert_eq!(
            new_exhaustive(&inputs).unwrap_err().0,
            "regex search with .* is not supported"
        );
    }

    #[test]
    fn rejects_unsupported_result_types() {
        let inputs = inputs("index:no type:repo", SearchType::Standard);
        assert_eq!(
            new_exhaustive(&inputs).unwrap_err().0,
            "your query contains the following type filters: repo. However Search Jobs only supports: commit, diff, file, path."
        );
    }

    #[test]
    fn builds_text_job() {
        let inputs = inputs("foo type:file", SearchType::Standard);
        let exhaustive = new_exhaustive(&inputs).unwrap();
        assert!(!exhaustive.repo_pager_job.only_cloned);
        assert!(!exhaustive.repo_pager_job.skip_partitioning);
        let job = exhaustive.job(&repo_revs());
        assert_eq!(job.name(), "SearcherTextSearchJob");
        assert!(job.children().is_empty());
        let attributes = job.attributes(Verbosity::Max);
        assert_eq!(
            attributes[0],
            ("useFullDeadline".to_owned(), "true".to_owned())
        );
        assert_eq!(
            attributes[1].1,
            "TextPatternInfo{\"foo\",nopath,filematchlimit:1000000}"
        );
        assert_eq!(attributes[2], ("numRepos".to_owned(), "1".to_owned()));
        assert_eq!(attributes.last().unwrap().0, "indexed");
    }

    #[test]
    fn builds_commit_job() {
        let inputs = inputs("type:commit author:alice foo", SearchType::Standard);
        let exhaustive = new_exhaustive(&inputs).unwrap();
        assert!(exhaustive.repo_pager_job.only_cloned);
        assert!(exhaustive.repo_pager_job.skip_partitioning);
        let job = exhaustive.job(&repo_revs());
        assert_eq!(job.name(), "CommitSearchJob");
        let attributes = job.attributes(Verbosity::Max);
        assert_eq!(
            attributes,
            vec![
                ("includeModifiedFiles".to_owned(), "false".to_owned()),
                (
                    "query".to_owned(),
                    "(*protocol.AuthorMatches(alice) AND *protocol.MessageMatches(foo))".to_owned(),
                ),
                ("diff".to_owned(), "false".to_owned()),
                ("limit".to_owned(), "1000000".to_owned()),
            ]
        );
    }

    #[test]
    fn text_branch_records_repo_options() {
        let inputs = inputs("repo:repo1@main type:file foo", SearchType::Standard);
        let exhaustive = new_exhaustive(&inputs).unwrap();
        assert_eq!(exhaustive.repo_pager_job.repo_opts.repo_filters.len(), 1);
        assert_eq!(
            exhaustive.repo_pager_job.repo_opts.repo_filters[0].repo,
            "repo1"
        );
        assert_eq!(
            exhaustive.repo_pager_job.repo_opts.repo_filters[0].revs[0].rev_spec,
            "main"
        );
        assert!(!exhaustive.repo_pager_job.contains_ref_globs);
    }

    #[test]
    fn text_branch_detects_ref_globs() {
        let inputs = inputs(
            "repo:repo1@*refs/heads/* type:file foo",
            SearchType::Standard,
        );
        let exhaustive = new_exhaustive(&inputs).unwrap();
        assert!(exhaustive.repo_pager_job.contains_ref_globs);
    }

    struct FakeResolver {
        seen: std::cell::RefCell<Vec<jb::RepoOptions>>,
    }

    impl ExhaustiveResolver for FakeResolver {
        fn iterate_repo_revs(&self, repo_opts: &jb::RepoOptions) -> RepoRevSpecsIterator {
            self.seen.borrow_mut().push(repo_opts.clone());
            RepoRevSpecsIterator {
                pages: vec![vec![RepoRevSpecs {
                    repo: MinimalRepo {
                        id: 3,
                        name: "alpha".to_owned(),
                    },
                    revs: vec![RevisionSpecifier::default()],
                }]],
                error: None,
            }
        }

        fn resolve_rev_specs(
            &self,
            repo_opts: &jb::RepoOptions,
            specs: &[RepoRevSpecs],
        ) -> Result<Resolved, JobError> {
            self.seen.borrow_mut().push(repo_opts.clone());
            Ok(Resolved {
                repo_revs: vec![RepositoryRevisions {
                    repo: sg_zoekt::indexed_repo_revs::RepositoryRevisionsRepo {
                        id: specs[0].repo.id,
                        name: specs[0].repo.name.clone(),
                    },
                    revs: vec!["HEAD".to_owned()],
                }],
                backends_missing: 2,
            })
        }
    }

    #[test]
    fn resolver_wrappers_forward_repo_options() {
        let inputs = inputs("type:commit repo:repo1 foo", SearchType::Standard);
        let exhaustive = new_exhaustive(&inputs).unwrap();
        let resolver = FakeResolver {
            seen: std::cell::RefCell::new(Vec::new()),
        };
        let iterator = exhaustive.repository_rev_specs(&resolver);
        assert_eq!(iterator.pages.len(), 1);
        let specs = &iterator.pages[0];
        let resolved = exhaustive
            .resolve_repository_rev_spec(&resolver, specs)
            .unwrap();
        assert_eq!(resolved.backends_missing, 2);
        assert_eq!(
            resolver.seen.borrow()[0].repo_filters[0].repo,
            "repo1".to_owned()
        );
        assert_eq!(
            resolver.seen.borrow()[1].repo_filters[0].repo,
            "repo1".to_owned()
        );
    }

    #[test]
    fn rewrite_conjunctive_distributes_and_over_or() {
        let node = GitQueryNode::operator(
            GitOperatorKind::Or,
            vec![
                GitQueryNode::operator(
                    GitOperatorKind::And,
                    vec![
                        GitQueryNode::MessageMatches {
                            expr: "foo".to_owned(),
                            ignore_case: false,
                        },
                        GitQueryNode::MessageMatches {
                            expr: "bar".to_owned(),
                            ignore_case: false,
                        },
                    ],
                ),
                GitQueryNode::MessageMatches {
                    expr: "baz".to_owned(),
                    ignore_case: false,
                },
            ],
        );
        assert_eq!(
            rewrite_conjunctive(node).to_string(),
            "((*protocol.MessageMatches(baz) OR *protocol.MessageMatches(foo)) AND \
             (*protocol.MessageMatches(baz) OR *protocol.MessageMatches(bar)))"
        );
    }

    #[test]
    fn merge_or_regexp_merges_same_kind_and_keeps_unmergeable() {
        let node = GitQueryNode::operator(
            GitOperatorKind::Or,
            vec![
                GitQueryNode::MessageMatches {
                    expr: "a".to_owned(),
                    ignore_case: false,
                },
                GitQueryNode::Boolean(true),
                GitQueryNode::MessageMatches {
                    expr: "b".to_owned(),
                    ignore_case: false,
                },
            ],
        );
        assert_eq!(
            merge_or_regexp(node).to_string(),
            "(*protocol.Boolean(true) OR *protocol.MessageMatches((?:a)|(?:b)))"
        );
    }

    #[test]
    fn parses_git_dates_like_time_string() {
        assert_eq!(
            parse_git_date("2020-01-02").as_deref(),
            Some("2020-01-02 00:00:00 +0000 UTC")
        );
        assert_eq!(
            parse_git_date("2020-01-02T03:04:05Z").as_deref(),
            Some("2020-01-02 03:04:05 +0000 UTC")
        );
        assert_eq!(
            parse_git_date("1700000000").as_deref(),
            Some("2023-11-14 22:13:20 +0000 +0000")
        );
        assert_eq!(
            parse_git_date("Jan 2, 2020").as_deref(),
            Some("2020-01-02 00:00:00 +0000 UTC")
        );
        assert_eq!(
            parse_git_date("2 January 2020").as_deref(),
            Some("2020-01-02 00:00:00 +0000 UTC")
        );
        assert_eq!(parse_git_date("not a date"), None);
    }
}
