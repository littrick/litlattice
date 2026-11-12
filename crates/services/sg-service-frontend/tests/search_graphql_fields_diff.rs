#![allow(clippy::unwrap_used)]

//! M9.8ij differential of the remaining `Query.search` result fields:
//! `SearchResults.sparkline` / `SearchResultsStats.sparkline`,
//! `SearchResultsStats.languages`, `CommitSearchResult.refs`/`sourceRefs`
//! (`GitRefObject`) and `Markdown.HTML()`.
//!
//! Go oracle: `cmd/frontend/graphqlbackend/zz_m9ij_oracle_test.go`.
//!
//! Env:
//!
//! * `SG_M9IJ_DIFF=1`
//! * `M9IJ_CORPUS` — the corpus JSON
//! * `M9IJ_OUT` — output file

use std::collections::HashSet;
use std::sync::Arc;

use async_trait::async_trait;
use serde::Deserialize;
use serde_json::{Map, Value};
use time::{Duration, OffsetDateTime};

use sg_graphql::exec::Actor;
use sg_graphql::schema::assemble_oss;
use sg_search_query::SearchType;
use sg_service_frontend::graphql_api::{
    GraphqlDeps, GraphqlExecRequest, GraphqlResponse, SchemaGraphqlDeps,
};
use sg_service_frontend::search_client::{Features, Protocol, SearchMode};
use sg_service_frontend::search_graphql::{
    markdown_html, search_results_stat_languages, PlannedSearch, SearchArgs, SearchExecution,
    SearchExecutionError, SearchExecutor, SearchStreamMode,
};
use sg_service_frontend::search_stream::convert::{
    ChunkMatch, CommitMatch, FileMatch, FileRef, GitCommit, Match, MatchedString, MinimalRepo,
    RepoMatch, Signature,
};
use sg_service_frontend::search_stream::events::{Location, Range};
use sg_service_frontend::search_stream::progress::Stats;
use sg_service_frontend::search_stream::SearchInputs;

// ---------------------------------------------------------------------------
// Corpus
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Deserialize)]
struct RefsCase {
    name: String,
    refs: Vec<String>,
    source_refs: Vec<String>,
}

#[derive(Debug, Clone, Deserialize)]
struct LanguageFile {
    path: String,
    chunks: Vec<usize>,
}

#[derive(Debug, Clone, Deserialize)]
struct LanguageCase {
    name: String,
    #[serde(default)]
    commit: bool,
    #[serde(default)]
    files: Vec<LanguageFile>,
}

#[derive(Debug, Clone, Deserialize)]
struct MarkdownCase {
    name: String,
    input: String,
}

#[derive(Debug, Deserialize)]
struct FieldsCorpus {
    refs_case: RefsCase,
    sparkline_offsets_days: Vec<f64>,
    languages: Vec<LanguageCase>,
    markdown: Vec<MarkdownCase>,
}

// ---------------------------------------------------------------------------
// Fixed search results (same data as the Go oracle)
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

fn author_date() -> OffsetDateTime {
    OffsetDateTime::from_unix_timestamp(1609556645).expect("author date")
}

fn committer_date() -> OffsetDateTime {
    OffsetDateTime::from_unix_timestamp(1609560245).expect("committer date")
}

fn commit_match(refs: Vec<String>, source_refs: Vec<String>) -> CommitMatch {
    CommitMatch {
        commit: GitCommit {
            id: "1234567890abcdef".to_owned(),
            message: "Fix bug & \"quotes\"\n\nLonger body line\n".to_owned(),
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
            content: "Fix bug & \"quotes\"".to_owned(),
            matched_ranges: vec![range(0, 0, 0, 3, 0)],
        }),
        diff_preview: None,
        refs,
        source_refs,
        diff: Vec::new(),
        modified_files: Vec::new(),
    }
}

fn sparkline_matches(offsets_days: &[f64]) -> Vec<Match> {
    let now = OffsetDateTime::now_utc();
    let mut matches: Vec<Match> = offsets_days
        .iter()
        .map(|offset| {
            let mut commit = commit_match(Vec::new(), Vec::new());
            commit.commit.author.date =
                now - Duration::milliseconds((offset * 86_400_000.0) as i64);
            Match::Commit(commit)
        })
        .collect();
    matches.push(Match::Repo(RepoMatch {
        name: "example.com/x/y".to_owned(),
        id: 3,
        rev: String::new(),
        description_matches: Vec::new(),
        repo_name_matches: Vec::new(),
    }));
    matches
}

// ---------------------------------------------------------------------------
// Fake executor
// ---------------------------------------------------------------------------

struct FakeExecutor {
    matches: Vec<Match>,
    stats: Stats,
}

#[async_trait]
impl SearchExecutor for FakeExecutor {
    async fn plan(
        &self,
        args: &SearchArgs,
        _mode: SearchStreamMode,
    ) -> Result<PlannedSearch, SearchExecutionError> {
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
        Ok(SearchExecution {
            matches: self.matches.clone(),
            stats: self.stats.clone(),
            alert: None,
            repo_names: Vec::new(),
            elapsed_ms: 0,
        })
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

fn empty_stats() -> Stats {
    Stats {
        is_limit_hit: false,
        repos: HashSet::new(),
        status: Default::default(),
        backends_missing: 0,
        excluded_forks: 0,
        excluded_archived: 0,
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

async fn run_query(
    base_schema: &Arc<sg_graphql::Schema>,
    matches: Vec<Match>,
    stats: Stats,
    query: &str,
) -> Value {
    let executor = Arc::new(FakeExecutor { matches, stats });
    let deps = SchemaGraphqlDeps::new(base_schema.clone()).with_search_executor(executor);
    let variables = Map::new();
    let response = deps
        .exec(GraphqlExecRequest {
            query,
            operation_name: "",
            variables: &variables,
            actor: Actor::Anonymous,
            anonymous_uid: "",
        })
        .await;
    normalized_response(&response)
}

const REFS_QUERY: &str = r#"query M9IJRefs { search(query: "refs", version: V1, patternType: literal) { results { results { __typename ... on CommitSearchResult { label { text html } detail { text html } url refs { id name abbrevName displayName prefix type repository { name url } url } sourceRefs { id name abbrevName displayName prefix type repository { name url } url } matches { url body { text html } } } } } } }"#;

const SPARKLINE_QUERY: &str = r#"query M9IJSparkline { search(query: "sparkline", version: V1, patternType: literal) { results { sparkline } stats { sparkline approximateResultCount } } }"#;

fn language_matches(case: &LanguageCase) -> Vec<Match> {
    case.files
        .iter()
        .map(|file| {
            let chunk_matches: Vec<ChunkMatch> = file
                .chunks
                .iter()
                .map(|count| ChunkMatch {
                    content: String::new(),
                    content_start: location(0, 0, 0),
                    ranges: (0..*count)
                        .map(|line| range(0, line as i64, 0, 0, 0))
                        .collect(),
                })
                .collect();
            Match::File(FileMatch {
                file: FileRef {
                    repo: repo(1, "github.com/foo/bar"),
                    commit_id: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_owned(),
                    path: file.path.clone(),
                    input_rev: None,
                    precise_language: String::new(),
                },
                chunk_matches,
                symbols: Vec::new(),
                path_matches: Vec::new(),
                limit_hit: false,
                debug: None,
            })
        })
        .collect()
}

fn languages_value(case: &LanguageCase) -> Value {
    let matches = if case.commit {
        vec![Match::Commit(commit_match(Vec::new(), Vec::new()))]
    } else {
        language_matches(case)
    };
    match search_results_stat_languages(&matches) {
        Ok(languages) => Value::Object(Map::from_iter([
            ("name".to_owned(), Value::from(case.name.clone())),
            (
                "languages".to_owned(),
                Value::Array(
                    languages
                        .into_iter()
                        .map(|lang| {
                            Value::Object(Map::from_iter([
                                ("name".to_owned(), Value::from(lang.name)),
                                ("totalBytes".to_owned(), Value::from(lang.total_bytes)),
                                ("totalLines".to_owned(), Value::from(lang.total_lines)),
                            ]))
                        })
                        .collect(),
                ),
            ),
        ])),
        Err(message) => Value::Object(Map::from_iter([
            ("name".to_owned(), Value::from(case.name.clone())),
            ("error".to_owned(), Value::from(message)),
        ])),
    }
}

#[tokio::test]
async fn search_graphql_fields_harness() {
    if std::env::var("SG_M9IJ_DIFF").as_deref() != Ok("1") {
        return;
    }
    let corpus_path = std::env::var("M9IJ_CORPUS").expect("M9IJ_CORPUS");
    let out_path = std::env::var("M9IJ_OUT").expect("M9IJ_OUT");
    let corpus: FieldsCorpus =
        serde_json::from_slice(&std::fs::read(corpus_path).expect("corpus")).expect("corpus json");
    let base_schema = Arc::new(assemble_oss().expect("oss schema"));

    let refs_response = run_query(
        &base_schema,
        vec![Match::Commit(commit_match(
            corpus.refs_case.refs.clone(),
            corpus.refs_case.source_refs.clone(),
        ))],
        empty_stats(),
        REFS_QUERY,
    )
    .await;

    let sparkline_response = run_query(
        &base_schema,
        sparkline_matches(&corpus.sparkline_offsets_days),
        empty_stats(),
        SPARKLINE_QUERY,
    )
    .await;

    let languages: Vec<Value> = corpus.languages.iter().map(languages_value).collect();

    let markdown: Vec<Value> = corpus
        .markdown
        .iter()
        .map(|case| match markdown_html(&case.input) {
            Ok(html) => Value::Object(Map::from_iter([
                ("name".to_owned(), Value::from(case.name.clone())),
                ("html".to_owned(), Value::from(html)),
            ])),
            Err(message) => Value::Object(Map::from_iter([
                ("name".to_owned(), Value::from(case.name.clone())),
                ("error".to_owned(), Value::from(message)),
            ])),
        })
        .collect();

    let document = Value::Object(Map::from_iter([
        (
            "refs".to_owned(),
            Value::Object(Map::from_iter([
                (
                    "name".to_owned(),
                    Value::from(corpus.refs_case.name.clone()),
                ),
                ("response".to_owned(), refs_response),
            ])),
        ),
        (
            "sparkline".to_owned(),
            Value::Object(Map::from_iter([(
                "response".to_owned(),
                sparkline_response,
            )])),
        ),
        ("languages".to_owned(), Value::Array(languages)),
        ("markdown".to_owned(), Value::Array(markdown)),
    ]));
    std::fs::write(out_path, serde_json::to_string(&document).expect("json")).expect("write");
}
