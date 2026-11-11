#![forbid(unsafe_code)]

//! Internal request types, mirroring `internal/searcher/protocol`.

use std::fmt;
use std::time::Duration;

use sg_proto::searcher as pb;

/// Baseline `protocol.Request`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Request {
    pub repo: String,
    pub repo_id: u32,
    pub commit: String,
    pub branch: String,
    pub pattern_info: PatternInfo,
    pub fetch_timeout: Duration,
    pub indexed: bool,
    pub num_context_lines: i32,
}

/// Baseline `protocol.PatternInfo`.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct PatternInfo {
    pub query: Option<QueryNode>,
    pub is_structural: bool,
    pub is_case_sensitive: bool,
    pub exclude_paths: String,
    pub include_paths: Vec<String>,
    pub include_langs: Vec<String>,
    pub exclude_langs: Vec<String>,
    pub path_patterns_are_case_sensitive: bool,
    pub limit: i32,
    pub pattern_matches_content: bool,
    pub pattern_matches_path: bool,
    pub comby_rule: String,
    pub select: String,
    pub languages: Vec<String>,
}

/// Baseline `protocol.QueryNode`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QueryNode {
    Pattern(PatternNode),
    And(Vec<QueryNode>),
    Or(Vec<QueryNode>),
}

/// Baseline `protocol.PatternNode`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PatternNode {
    pub value: String,
    pub is_negated: bool,
    pub is_regexp: bool,
    pub boost: bool,
}

impl Request {
    /// Baseline `Request.FromProto`.
    pub fn from_proto(request: &pb::SearchRequest) -> Self {
        let info = request.pattern_info.clone().unwrap_or_default();
        Self {
            repo: request.repo.clone(),
            repo_id: request.repo_id,
            commit: request.commit_oid.clone(),
            branch: request.branch.clone(),
            pattern_info: PatternInfo {
                query: info.query.as_ref().and_then(node_from_proto),
                is_structural: info.is_structural,
                is_case_sensitive: info.is_case_sensitive,
                exclude_paths: info.exclude_pattern,
                include_paths: info.include_patterns,
                include_langs: info.include_langs,
                exclude_langs: info.exclude_langs,
                path_patterns_are_case_sensitive: info.path_patterns_are_case_sensitive,
                limit: i32::try_from(info.limit).unwrap_or(i32::MAX),
                pattern_matches_content: info.pattern_matches_content,
                pattern_matches_path: info.pattern_matches_path,
                comby_rule: info.comby_rule,
                select: info.select,
                languages: info.languages,
            },
            fetch_timeout: request
                .fetch_timeout
                .map(|timeout| {
                    Duration::new(timeout.seconds.max(0) as u64, timeout.nanos.max(0) as u32)
                })
                .unwrap_or_default(),
            indexed: request.indexed,
            num_context_lines: request.num_context_lines,
        }
    }

    /// Baseline `Request.ToProto`.
    pub fn to_proto(&self) -> pb::SearchRequest {
        pb::SearchRequest {
            repo: self.repo.clone(),
            repo_id: self.repo_id,
            commit_oid: self.commit.clone(),
            indexed: self.indexed,
            pattern_info: Some(pb::PatternInfo {
                is_structural: self.pattern_info.is_structural,
                is_case_sensitive: self.pattern_info.is_case_sensitive,
                exclude_pattern: self.pattern_info.exclude_paths.clone(),
                include_patterns: self.pattern_info.include_paths.clone(),
                path_patterns_are_case_sensitive: self
                    .pattern_info
                    .path_patterns_are_case_sensitive,
                limit: i64::from(self.pattern_info.limit),
                pattern_matches_content: self.pattern_info.pattern_matches_content,
                pattern_matches_path: self.pattern_info.pattern_matches_path,
                comby_rule: self.pattern_info.comby_rule.clone(),
                languages: self.pattern_info.languages.clone(),
                select: self.pattern_info.select.clone(),
                include_langs: self.pattern_info.include_langs.clone(),
                exclude_langs: self.pattern_info.exclude_langs.clone(),
                query: self.pattern_info.query.as_ref().map(QueryNode::to_proto),
            }),
            branch: self.branch.clone(),
            fetch_timeout: Some(prost_types::Duration {
                seconds: self.fetch_timeout.as_secs() as i64,
                nanos: self.fetch_timeout.subsec_nanos() as i32,
            }),
            num_context_lines: self.num_context_lines,
        }
    }

    /// Baseline `validateParams`.
    pub fn validate(&self) -> Result<(), String> {
        if self.repo.is_empty() {
            return Err("Repo must be non-empty".to_owned());
        }
        // Surprisingly this is the same sanity check used in the git source.
        if self.commit.len() != 40 {
            return Err(format!(
                "Commit must be resolved (Commit={:?})",
                self.commit
            ));
        }

        if let Some(QueryNode::Pattern(pattern)) = &self.pattern_info.query {
            if pattern.value.is_empty()
                && self.pattern_info.exclude_paths.is_empty()
                && self.pattern_info.include_paths.is_empty()
                && self.pattern_info.include_langs.is_empty()
                && self.pattern_info.exclude_langs.is_empty()
            {
                return Err(
                    "At least one of pattern and include/exclude patterns must be non-empty"
                        .to_owned(),
                );
            }
        }
        Ok(())
    }
}

/// A file that matched the search query (`protocol.FileMatch`).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct FileMatch {
    pub path: String,
    pub language: String,
    pub chunk_matches: Vec<ChunkMatch>,
    /// The limit was hit while searching this file.
    pub limit_hit: bool,
}

impl FileMatch {
    /// Baseline `FileMatch.MatchCount`: a path match still counts as one.
    pub fn match_count(&self) -> i32 {
        if self.chunk_matches.is_empty() {
            return 1;
        }
        self.chunk_matches
            .iter()
            .map(|chunk| chunk.ranges.len() as i32)
            .sum()
    }

    /// Baseline `FileMatch.Limit`: truncate ranges to `limit` matches,
    /// dropping chunks that no longer carry a range.
    pub fn limit(&mut self, mut limit: i32) {
        for index in 0..self.chunk_matches.len() {
            let len = self.chunk_matches[index].ranges.len() as i32;
            if len <= limit {
                limit -= len;
                continue;
            }
            // Invariant: limit < len.
            self.chunk_matches[index]
                .ranges
                .truncate(limit.max(0) as usize);
            if limit > 0 {
                self.chunk_matches.truncate(index + 1);
            } else {
                self.chunk_matches.truncate(index);
            }
            self.limit_hit = true;
            return;
        }
    }

    /// Baseline `FileMatch.ToProto`.
    pub fn to_proto(&self) -> pb::FileMatch {
        pb::FileMatch {
            path: self.path.as_bytes().to_vec(),
            language: self.language.as_bytes().to_vec(),
            chunk_matches: self
                .chunk_matches
                .iter()
                .map(ChunkMatch::to_proto)
                .collect(),
            limit_hit: self.limit_hit,
        }
    }

    /// Baseline `FileMatch.FromProto`.
    pub fn from_proto(file_match: &pb::FileMatch) -> Self {
        Self {
            path: String::from_utf8_lossy(&file_match.path).into_owned(),
            language: String::from_utf8_lossy(&file_match.language).into_owned(),
            chunk_matches: file_match
                .chunk_matches
                .iter()
                .map(ChunkMatch::from_proto)
                .collect(),
            limit_hit: file_match.limit_hit,
        }
    }
}

/// A contiguous region of a file around one or more matches.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ChunkMatch {
    pub content: String,
    pub content_start: Location,
    pub ranges: Vec<Range>,
}

impl ChunkMatch {
    pub fn to_proto(&self) -> pb::ChunkMatch {
        pb::ChunkMatch {
            content: self.content.as_bytes().to_vec(),
            content_start: Some(self.content_start.to_proto()),
            ranges: self.ranges.iter().map(Range::to_proto).collect(),
        }
    }

    /// Baseline `ChunkMatch.FromProto` (bytes `Content` -> string).
    pub fn from_proto(chunk: &pb::ChunkMatch) -> Self {
        Self {
            content: String::from_utf8_lossy(&chunk.content).into_owned(),
            content_start: chunk
                .content_start
                .as_ref()
                .map(Location::from_proto)
                .unwrap_or_default(),
            ranges: chunk.ranges.iter().map(Range::from_proto).collect(),
        }
    }
}

/// A match span within a chunk.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Range {
    pub start: Location,
    pub end: Location,
}

impl Range {
    pub fn to_proto(&self) -> pb::Range {
        pb::Range {
            start: Some(self.start.to_proto()),
            end: Some(self.end.to_proto()),
        }
    }

    /// Baseline `Range.FromProto`.
    pub fn from_proto(range: &pb::Range) -> Self {
        Self {
            start: range
                .start
                .as_ref()
                .map(Location::from_proto)
                .unwrap_or_default(),
            end: range
                .end
                .as_ref()
                .map(Location::from_proto)
                .unwrap_or_default(),
        }
    }
}

/// A byte offset plus its 0-based line and rune-based column.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Location {
    pub offset: i32,
    pub line: i32,
    pub column: i32,
}

impl Location {
    pub fn to_proto(&self) -> pb::Location {
        pb::Location {
            offset: self.offset,
            line: self.line,
            column: self.column,
        }
    }

    /// Baseline `Location.FromProto`.
    pub fn from_proto(location: &pb::Location) -> Self {
        Self {
            offset: location.offset,
            line: location.line,
            column: location.column,
        }
    }
}

fn node_from_proto(node: &pb::QueryNode) -> Option<QueryNode> {
    use pb::query_node::Value;

    match node.value.as_ref()? {
        Value::Pattern(pattern) => Some(QueryNode::Pattern(PatternNode {
            value: pattern.value.clone(),
            is_negated: pattern.is_negated,
            is_regexp: pattern.is_regexp,
            boost: pattern.boost,
        })),
        Value::And(and) => Some(QueryNode::And(
            and.children.iter().filter_map(node_from_proto).collect(),
        )),
        Value::Or(or) => Some(QueryNode::Or(
            or.children.iter().filter_map(node_from_proto).collect(),
        )),
    }
}

impl QueryNode {
    /// Baseline `QueryNode.ToProto`.
    pub fn to_proto(&self) -> pb::QueryNode {
        match self {
            Self::Pattern(pattern) => pb::QueryNode {
                value: Some(pb::query_node::Value::Pattern(pattern.to_proto())),
            },
            Self::And(children) => pb::QueryNode {
                value: Some(pb::query_node::Value::And(pb::AndNode {
                    children: children.iter().map(QueryNode::to_proto).collect(),
                })),
            },
            Self::Or(children) => pb::QueryNode {
                value: Some(pb::query_node::Value::Or(pb::OrNode {
                    children: children.iter().map(QueryNode::to_proto).collect(),
                })),
            },
        }
    }
}

impl PatternNode {
    /// Baseline `PatternNode.ToProto`.
    pub fn to_proto(&self) -> pb::PatternNode {
        pb::PatternNode {
            value: self.value.clone(),
            is_negated: self.is_negated,
            is_regexp: self.is_regexp,
            boost: self.boost,
        }
    }
}

impl fmt::Display for QueryNode {
    /// Baseline `QueryNode.String`.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Pattern(pattern) => pattern.fmt(f),
            Self::And(children) => write_joined(f, children, " AND "),
            Self::Or(children) => write_joined(f, children, " OR "),
        }
    }
}

fn write_joined(f: &mut fmt::Formatter<'_>, children: &[QueryNode], sep: &str) -> fmt::Result {
    f.write_str("(")?;
    for (index, child) in children.iter().enumerate() {
        if index > 0 {
            f.write_str(sep)?;
        }
        write!(f, "{child}")?;
    }
    f.write_str(")")
}

impl fmt::Display for PatternNode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let prefix = if self.is_negated { "NOT " } else { "" };
        if self.is_regexp {
            write!(f, "{prefix}/",)?;
            f.write_str(&self.value)?;
            f.write_str("/")
        } else {
            write!(f, "{prefix}\"")?;
            f.write_str(&self.value)?;
            f.write_str("\"")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn valid_request() -> Request {
        Request {
            repo: "github.com/gorilla/mux".to_owned(),
            repo_id: 1,
            commit: "0123456789012345678901234567890123456789".to_owned(),
            branch: String::new(),
            pattern_info: PatternInfo {
                query: Some(QueryNode::Pattern(PatternNode {
                    value: "route".to_owned(),
                    is_negated: false,
                    is_regexp: false,
                    boost: false,
                })),
                limit: 10,
                pattern_matches_content: true,
                ..PatternInfo::default()
            },
            fetch_timeout: Duration::default(),
            indexed: false,
            num_context_lines: 0,
        }
    }

    #[test]
    fn validates_like_the_baseline() {
        assert!(valid_request().validate().is_ok());

        let mut request = valid_request();
        request.repo.clear();
        assert_eq!(request.validate(), Err("Repo must be non-empty".to_owned()));

        let mut request = valid_request();
        request.commit = "HEAD".to_owned();
        assert_eq!(
            request.validate(),
            Err("Commit must be resolved (Commit=\"HEAD\")".to_owned())
        );

        let mut request = valid_request();
        request.pattern_info.query = Some(QueryNode::Pattern(PatternNode {
            value: String::new(),
            is_negated: false,
            is_regexp: true,
            boost: false,
        }));
        assert_eq!(
            request.validate(),
            Err(
                "At least one of pattern and include/exclude patterns must be non-empty".to_owned()
            )
        );
    }

    #[test]
    fn formats_queries_like_go() {
        let node = QueryNode::And(vec![
            QueryNode::Pattern(PatternNode {
                value: "needle".to_owned(),
                is_negated: false,
                is_regexp: false,
                boost: false,
            }),
            QueryNode::Or(vec![QueryNode::Pattern(PatternNode {
                value: "a.*b".to_owned(),
                is_negated: true,
                is_regexp: true,
                boost: false,
            })]),
        ]);
        assert_eq!(node.to_string(), "(\"needle\" AND (NOT /a.*b/))");
    }

    fn chunk(ranges: usize) -> ChunkMatch {
        ChunkMatch {
            content: String::new(),
            content_start: Location::default(),
            ranges: vec![Range::default(); ranges],
        }
    }

    #[test]
    fn counts_matches_like_the_baseline() {
        let mut file_match = FileMatch::default();
        assert_eq!(file_match.match_count(), 1);

        file_match.chunk_matches = vec![chunk(2), chunk(3)];
        assert_eq!(file_match.match_count(), 5);
    }

    #[test]
    fn truncates_matches_at_the_limit() {
        let mut file_match = FileMatch {
            chunk_matches: vec![chunk(2), chunk(3), chunk(1)],
            ..FileMatch::default()
        };
        // Exactly consumes the first chunk and part of the second.
        file_match.limit(4);
        assert_eq!(file_match.chunk_matches.len(), 2);
        assert_eq!(file_match.chunk_matches[1].ranges.len(), 2);
        assert!(file_match.limit_hit);

        // A limit that lands on a chunk boundary drops the remaining chunks.
        let mut file_match = FileMatch {
            chunk_matches: vec![chunk(2), chunk(3)],
            ..FileMatch::default()
        };
        file_match.limit(2);
        assert_eq!(file_match.chunk_matches.len(), 1);
        assert!(file_match.limit_hit);

        // A limit larger than the match count leaves the file untouched.
        let mut file_match = FileMatch {
            chunk_matches: vec![chunk(2)],
            ..FileMatch::default()
        };
        file_match.limit(5);
        assert_eq!(file_match.chunk_matches.len(), 1);
        assert!(!file_match.limit_hit);
    }

    #[test]
    fn converts_from_proto() {
        let request = pb::SearchRequest {
            repo: "r".to_owned(),
            repo_id: 2,
            commit_oid: "c".to_owned(),
            indexed: true,
            pattern_info: Some(pb::PatternInfo {
                query: Some(pb::QueryNode {
                    value: Some(pb::query_node::Value::Pattern(pb::PatternNode {
                        value: "x".to_owned(),
                        is_regexp: true,
                        ..pb::PatternNode::default()
                    })),
                }),
                limit: i64::from(i32::MAX),
                ..pb::PatternInfo::default()
            }),
            fetch_timeout: None,
            branch: "main".to_owned(),
            num_context_lines: 2,
        };
        let converted = Request::from_proto(&request);
        assert_eq!(converted.repo, "r");
        assert!(converted.indexed);
        assert_eq!(converted.pattern_info.limit, i32::MAX);
        assert_eq!(
            converted.pattern_info.query,
            Some(QueryNode::Pattern(PatternNode {
                value: "x".to_owned(),
                is_negated: false,
                is_regexp: true,
                boost: false,
            }))
        );
    }

    #[test]
    fn converts_to_proto_and_back() {
        let request = valid_request();
        let proto = request.to_proto();
        assert_eq!(proto.repo, "github.com/gorilla/mux");
        assert_eq!(proto.repo_id, 1);
        assert_eq!(proto.commit_oid, "0123456789012345678901234567890123456789");
        assert_eq!(proto.pattern_info.as_ref().unwrap().limit, 10);
        assert_eq!(Request::from_proto(&proto), request);

        let mut request = valid_request();
        request.fetch_timeout = Duration::new(2, 500_000_000);
        let proto = request.to_proto();
        let fetch_timeout = proto.fetch_timeout.expect("fetch timeout");
        assert_eq!(fetch_timeout.seconds, 2);
        assert_eq!(fetch_timeout.nanos, 500_000_000);
        assert_eq!(
            Request::from_proto(&proto).fetch_timeout,
            request.fetch_timeout
        );
    }

    #[test]
    fn converts_file_matches_from_proto() {
        let file_match = pb::FileMatch {
            path: b"src/main.go".to_vec(),
            language: b"go".to_vec(),
            limit_hit: true,
            chunk_matches: vec![pb::ChunkMatch {
                content: b"route".to_vec(),
                content_start: Some(pb::Location {
                    offset: 5,
                    line: 2,
                    column: 3,
                }),
                ranges: vec![pb::Range {
                    start: Some(pb::Location {
                        offset: 5,
                        line: 2,
                        column: 3,
                    }),
                    end: Some(pb::Location {
                        offset: 10,
                        line: 2,
                        column: 8,
                    }),
                }],
            }],
        };
        let converted = FileMatch::from_proto(&file_match);
        assert_eq!(converted.path, "src/main.go");
        assert_eq!(converted.language, "go");
        assert!(converted.limit_hit);
        assert_eq!(converted.chunk_matches.len(), 1);
        assert_eq!(converted.chunk_matches[0].content, "route");
        assert_eq!(
            converted.chunk_matches[0].content_start,
            Location {
                offset: 5,
                line: 2,
                column: 3
            }
        );
        assert_eq!(
            converted.chunk_matches[0].ranges[0].end,
            Location {
                offset: 10,
                line: 2,
                column: 8
            }
        );
        assert_eq!(FileMatch::from_proto(&converted.to_proto()), converted);
    }
}
