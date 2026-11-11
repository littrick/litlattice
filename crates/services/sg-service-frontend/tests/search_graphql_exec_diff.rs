#![allow(clippy::unwrap_used)]

//! M9.8ie differential of the `Query.search` GraphQL resolver wiring
//! (`cmd/frontend/graphqlbackend/search.go:26,33,60`,
//! `search_results.go:127,131,166,181,189,402,412`,
//! `search_alert.go:52-70` and the per-result resolver files).
//!
//! Go oracle: `cmd/frontend/graphqlbackend/zz_m9ie_oracle_test.go`.
//! The Rust side drives the real `SchemaGraphqlDeps::exec` with a fake
//! `SearchExecutor` returning the same fixed events as the Go oracle; the Go
//! side drives graph-gophers with the real `SearchResultsResolver` /
//! `FileMatchResolver` / `CommitSearchResultResolver`.
//!
//! Env:
//!
//! * `SG_M9IE_DIFF=1`
//! * `M9IE_CORPUS` — the corpus JSON
//! * `M9IE_OUT` — output file

use std::collections::HashSet;
use std::sync::Arc;

use async_trait::async_trait;
use serde::Deserialize;
use serde_json::{Map, Value};
use time::OffsetDateTime;

use sg_graphql::exec::Actor;
use sg_graphql::schema::assemble_oss;
use sg_search_query::SearchType;
use sg_service_frontend::graphql_api::{
    GraphqlDeps, GraphqlExecRequest, GraphqlResponse, SchemaGraphqlDeps,
};
use sg_service_frontend::search_client::{Features, Protocol, SearchMode};
use sg_service_frontend::search_graphql::{
    PlannedSearch, SearchArgs, SearchExecution, SearchExecutionError, SearchExecutor,
    SearchStreamMode,
};
use sg_service_frontend::search_stream::convert::{
    ChunkMatch, CommitMatch, FileMatch, FileRef, GitCommit, Match, MatchedString, MinimalRepo,
    RepoMatch, Signature, Symbol, SymbolMatch,
};
use sg_service_frontend::search_stream::events::{
    Annotation, EventAlert, Location, QueryDescription, Range,
};
use sg_service_frontend::search_stream::progress::{repo_status, RepoStatusMap, Stats};
use sg_service_frontend::search_stream::SearchInputs;

// ---------------------------------------------------------------------------
// Corpus
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Deserialize)]
struct ExecCase {
    name: String,
    scenario: String,
    #[serde(default)]
    plan_message: String,
    #[serde(default)]
    expected_operand: bool,
    #[serde(default)]
    error_message: String,
    queries: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct ExecCorpus {
    cases: Vec<ExecCase>,
}

// ---------------------------------------------------------------------------
// Fake executor (same fixed data as the Go oracle)
// ---------------------------------------------------------------------------

fn repo(id: i32, name: &str) -> MinimalRepo {
    MinimalRepo {
        id,
        name: name.to_owned(),
    }
}

fn location(offset: i64, line: i64, column: i64) -> Location {
    Location {
        offset,
        line,
        column,
    }
}

fn range(
    start_offset: i64,
    line: i64,
    start_column: i64,
    end_offset: i64,
    end_column: i64,
) -> Range {
    Range {
        start: location(start_offset, line, start_column),
        end: location(end_offset, line, end_column),
    }
}

fn file_match() -> FileMatch {
    let file = FileRef {
        repo: repo(1, "github.com/foo/bar"),
        commit_id: "abcdef1234567890".to_owned(),
        path: "src/main.go".to_owned(),
        input_rev: Some("main".to_owned()),
        precise_language: "Go".to_owned(),
    };
    FileMatch {
        file: file.clone(),
        chunk_matches: vec![ChunkMatch {
            content: "func main() {\n\tfmt.Println(\"hi\")\n}\n".to_owned(),
            content_start: location(104, 9, 0),
            ranges: vec![range(109, 9, 5, 113, 9), range(123, 10, 5, 130, 12)],
        }],
        symbols: vec![SymbolMatch {
            symbol: Symbol {
                name: "main".to_owned(),
                path: "src/main.go".to_owned(),
                line: 10,
                character: 5,
                kind: "function".to_owned(),
                language: "Go".to_owned(),
                parent: String::new(),
                parent_kind: String::new(),
            },
            file,
        }],
        path_matches: vec![range(0, 0, 0, 3, 3)],
        limit_hit: false,
        debug: None,
    }
}

fn author_date() -> OffsetDateTime {
    OffsetDateTime::from_unix_timestamp(1609556645).expect("author date")
}

fn committer_date() -> OffsetDateTime {
    OffsetDateTime::from_unix_timestamp(1609560245).expect("committer date")
}

fn commit_match() -> CommitMatch {
    CommitMatch {
        commit: GitCommit {
            id: "1234567890abcdef".to_owned(),
            message: "Fix bug\n\nLonger body line one\nSecond body line\n".to_owned(),
            author: Signature {
                name: "Alice".to_owned(),
                email: "alice@example.com".to_owned(),
                date: author_date(),
            },
            committer: Signature {
                name: "Bob".to_owned(),
                email: "bob@example.com".to_owned(),
                date: committer_date(),
            },
            parents: Vec::new(),
        },
        repo: repo(2, "gitlab.com/a/b"),
        message_preview: Some(MatchedString {
            content: "Fix bug".to_owned(),
            matched_ranges: vec![range(0, 0, 0, 3, 3)],
        }),
        diff_preview: None,
        refs: Vec::new(),
        source_refs: Vec::new(),
        diff: Vec::new(),
        modified_files: Vec::new(),
    }
}

fn repo_match() -> RepoMatch {
    RepoMatch {
        name: "example.com/x/y".to_owned(),
        id: 3,
        rev: String::new(),
        description_matches: Vec::new(),
        repo_name_matches: Vec::new(),
    }
}

fn matches() -> Vec<Match> {
    vec![
        Match::File(file_match()),
        Match::Commit(commit_match()),
        Match::Repo(repo_match()),
    ]
}

fn stats(limit_hit: bool) -> Stats {
    let mut status = RepoStatusMap::default();
    status.update(3, repo_status::CLONING);
    status.update(4, repo_status::MISSING);
    status.update(5, repo_status::TIMED_OUT);
    Stats {
        is_limit_hit: limit_hit,
        repos: HashSet::from([1, 2, 3]),
        status,
        backends_missing: 0,
        excluded_forks: 0,
        excluded_archived: 0,
    }
}

fn alert() -> EventAlert {
    EventAlert {
        title: "Some alert".to_owned(),
        description: "Alert description".to_owned(),
        kind: "alert-kind".to_owned(),
        proposed_queries: Some(vec![QueryDescription {
            description: "Remove quotes".to_owned(),
            query: "foo bar".to_owned(),
            annotations: vec![Annotation {
                name: "name".to_owned(),
                value: "value".to_owned(),
            }],
        }]),
    }
}

fn inputs(query: &str) -> SearchInputs {
    SearchInputs {
        max_results: 10000,
        original_query: query.to_owned(),
        plan: sg_search_query::pipeline_query(query, SearchType::Standard).expect("plan"),
        search_mode: SearchMode::Precise,
        pattern_type: SearchType::Standard,
        features: Features::default(),
        protocol: Protocol::Batch,
        context_lines: 1,
        sanitize_search_patterns: Vec::new(),
        on_sourcegraph_dot_com: false,
        cody_context: None,
    }
}

struct FakeExecutor {
    case: ExecCase,
    executed: std::sync::atomic::AtomicBool,
}

#[async_trait]
impl SearchExecutor for FakeExecutor {
    async fn plan(
        &self,
        args: &SearchArgs,
        _mode: SearchStreamMode,
    ) -> Result<PlannedSearch, SearchExecutionError> {
        if self.case.scenario == "plan_query_error" {
            return Err(SearchExecutionError::Query {
                message: self.case.plan_message.clone(),
                expected_operand: self.case.expected_operand,
            });
        }
        Ok(PlannedSearch {
            inputs: inputs(&args.query),
            query: args.query.clone(),
        })
    }

    async fn execute(
        &self,
        _planned: &PlannedSearch,
        _mode: SearchStreamMode,
    ) -> Result<SearchExecution, SearchExecutionError> {
        self.executed
            .store(true, std::sync::atomic::Ordering::SeqCst);
        if self.case.scenario == "exec_error" {
            return Err(SearchExecutionError::Other(self.case.error_message.clone()));
        }
        Ok(SearchExecution {
            matches: matches(),
            stats: stats(self.case.scenario == "limit"),
            alert: Some(alert()),
            repo_names: vec![
                (3, "example.com/x/y".to_owned()),
                (4, "archived.example.com/old/repo".to_owned()),
                (5, "gitlab.com/c/d".to_owned()),
            ],
            elapsed_ms: 0,
        })
    }
}

// ---------------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------------

fn normalized_response(response: &GraphqlResponse) -> Value {
    let data = response
        .data
        .as_ref()
        .map(|data| serde_json::from_str::<Value>(data).expect("data json"))
        .unwrap_or(Value::Null);
    let errors: Vec<Value> = response
        .errors
        .iter()
        .map(|error| {
            Value::Object(Map::from_iter([
                ("message".to_owned(), Value::from(error.message.clone())),
                (
                    "path".to_owned(),
                    Value::Array(error.path.clone().unwrap_or_default()),
                ),
            ]))
        })
        .collect();
    Value::Object(Map::from_iter([
        ("data".to_owned(), data),
        ("errors".to_owned(), Value::Array(errors)),
    ]))
}

#[tokio::test]
async fn search_graphql_exec_harness() {
    if std::env::var("SG_M9IE_DIFF").as_deref() != Ok("1") {
        return;
    }
    let corpus_path = std::env::var("M9IE_CORPUS").expect("M9IE_CORPUS");
    let out_path = std::env::var("M9IE_OUT").expect("M9IE_OUT");
    let corpus: ExecCorpus =
        serde_json::from_slice(&std::fs::read(corpus_path).expect("corpus")).expect("corpus json");
    let base_schema = Arc::new(assemble_oss().expect("oss schema"));

    let mut cases = Vec::with_capacity(corpus.cases.len());
    for case in &corpus.cases {
        let executor = Arc::new(FakeExecutor {
            case: case.clone(),
            executed: std::sync::atomic::AtomicBool::new(false),
        });
        let deps = SchemaGraphqlDeps::new(base_schema.clone()).with_search_executor(executor);
        let mut responses = Vec::with_capacity(case.queries.len());
        for query in &case.queries {
            let variables = Map::new();
            let response = deps
                .exec(GraphqlExecRequest {
                    query,
                    operation_name: "",
                    variables: &variables,
                    actor: Actor::Anonymous,
                })
                .await;
            responses.push(normalized_response(&response));
        }
        cases.push(Value::Object(Map::from_iter([
            ("name".to_owned(), Value::from(case.name.clone())),
            ("responses".to_owned(), Value::Array(responses)),
        ])));
    }
    let document = Value::Object(Map::from_iter([("cases".to_owned(), Value::Array(cases))]));
    std::fs::write(out_path, serde_json::to_string(&document).expect("json")).expect("write");
}
