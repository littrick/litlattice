#![forbid(unsafe_code)]

//! The zoekt query tree (`github.com/sourcegraph/zoekt/query`), its
//! `Simplify` pass and the proto representation used on the wire.

use std::collections::{BTreeMap, BTreeSet};

use crate::proto;
use crate::proto::q;

/// `query.ZoektScoreBoost` (`internal/search/query/helpers.go`).
pub const ZOEKT_SCORE_BOOST: f64 = 20.0;

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
}
