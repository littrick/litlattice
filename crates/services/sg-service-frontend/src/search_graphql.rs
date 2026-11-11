//! `Query.search`: the batch search GraphQL resolver
//! (`cmd/frontend/graphqlbackend/search.go`,
//! `search_results.go`, `search_alert.go` and the per-result resolver files).
//!
//! The real search backend is not ported. [`SearchExecutor`] is the seam the
//! frontend injects: it plans a query (`search_stream::SearchDeps::plan`) and
//! materializes the execution stream into matches/stats/alert. The GraphQL
//! objects below are owned `ObjectValue` snapshots, replayed synchronously by
//! `graphql_resolvers::QueryRoot`.
//!
//! See `docs/overviews/m9_8ie_search_graphql_executor_overview.md`.

use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use time::format_description::well_known::Rfc3339;
use time::{Duration, OffsetDateTime};
use tokio_stream::StreamExt;

use sg_graphql::exec::{Actor, Arguments, ExecError, FieldValue, ObjectValue, RootField};
use sg_graphql::json::Json;
use sg_service_searcher::langmatch;

use crate::graphql_resolvers::{RepositoryData, RepositoryObject};
use crate::search_client::Protocol;
use crate::search_stream::args::Args;
use crate::search_stream::convert::{
    go_url_path, FileMatch, FileRef, Match, MinimalRepo, Symbol, SymbolMatch,
};
use crate::search_stream::events::{
    alert_for_query, EventAlert, Location, QueryDescription, Range,
};
use crate::search_stream::filters::{Filter, SearchFilters};
use crate::search_stream::progress::{repo_status, SearchEvent, Stats};
use crate::search_stream::{ExecuteItem, PlanError, SearchDeps, SearchInputs};

/// `graphqlbackend.SearchArgs`.
#[derive(Debug, Clone, Default)]
pub struct SearchArgs {
    pub version: String,
    pub pattern_type: Option<String>,
    pub query: String,
}

/// The `client.Plan` protocol the GraphQL root uses. The baseline
/// `schemaResolver.Search` plans with `search.Batch`; the streaming route
/// keeps `Streaming` for callers that reuse the executor.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SearchStreamMode {
    Batch,
    Streaming,
}

/// A planned search (`*search.Inputs` plus the original query).
#[derive(Debug, Clone)]
pub struct PlannedSearch {
    pub inputs: SearchInputs,
    pub query: String,
}

/// A `client.Plan`/`client.Execute` error.
#[derive(Debug, Clone)]
pub enum SearchExecutionError {
    /// `*client.QueryError`; the GraphQL resolver returns the
    /// `alertSearchImplementer` equivalent (`AlertForQuery`).
    Query {
        message: String,
        expected_operand: bool,
    },
    /// Any other error: the root field returns a GraphQL error.
    Other(String),
}

/// The materialized `client.Execute` result: the aggregating stream's matches
/// and stats plus the trailing alert.
#[derive(Debug, Clone, Default)]
pub struct SearchExecution {
    pub matches: Vec<Match>,
    pub stats: Stats,
    pub alert: Option<EventAlert>,
    /// Best-effort ID → name lookup for the status lists
    /// (`SearchResults.cloning`/`missing`/`timedout`).
    pub repo_names: Vec<(i32, String)>,
    pub elapsed_ms: i32,
}

/// The outcome of the `search` root field. `Alert` mirrors
/// `alertSearchImplementer` (plan error: alert only, no stats resolver).
#[derive(Debug, Clone)]
pub enum SearchRoot {
    Completed(SearchExecution),
    Alert { alert: EventAlert },
    Error(String),
}

/// The execution seam injected into `SchemaGraphqlDeps`.
#[async_trait]
pub trait SearchExecutor: Send + Sync {
    /// `client.Plan` with the GraphQL arguments and protocol mode.
    async fn plan(
        &self,
        args: &SearchArgs,
        mode: SearchStreamMode,
    ) -> Result<PlannedSearch, SearchExecutionError>;

    /// `client.Execute`, materialized into an aggregating result.
    async fn execute(
        &self,
        planned: &PlannedSearch,
        mode: SearchStreamMode,
    ) -> Result<SearchExecution, SearchExecutionError>;
}

/// Production executor: reuses the ported `search_stream` deps.
pub struct BackendSearchExecutor {
    deps: Arc<dyn SearchDeps>,
}

impl BackendSearchExecutor {
    pub fn new(deps: Arc<dyn SearchDeps>) -> Self {
        Self { deps }
    }
}

#[async_trait]
impl SearchExecutor for BackendSearchExecutor {
    async fn plan(
        &self,
        args: &SearchArgs,
        mode: SearchStreamMode,
    ) -> Result<PlannedSearch, SearchExecutionError> {
        let stream_args = Args {
            query: args.query.clone(),
            version: args.version.clone(),
            pattern_type: args.pattern_type.clone().unwrap_or_default(),
            display: -1,
            max_line_len: -1,
            enable_chunk_matches: false,
            // `search.Precise`
            search_mode: 0,
            context_lines: None,
            zoekt_search_options_override: String::new(),
        };
        let mut inputs = self
            .deps
            .plan(&stream_args)
            .await
            .map_err(|error| match error {
                PlanError::Query {
                    message,
                    expected_operand,
                } => SearchExecutionError::Query {
                    message,
                    expected_operand,
                },
                PlanError::Other(message) => SearchExecutionError::Other(message),
            })?;
        if mode == SearchStreamMode::Batch {
            // `client.Plan(... search.Batch ...)`. `BackendDeps::plan` always
            // plans with `Streaming`; the protocol only affects the default
            // result limit, which callers pass through the plan's `count:`.
            inputs.protocol = Protocol::Batch;
        }
        Ok(PlannedSearch {
            inputs,
            query: args.query.clone(),
        })
    }

    async fn execute(
        &self,
        planned: &PlannedSearch,
        _mode: SearchStreamMode,
    ) -> Result<SearchExecution, SearchExecutionError> {
        let start = Instant::now();
        let mut stream = self.deps.execute(&planned.inputs, &planned.query).await;
        let mut execution = SearchExecution::default();
        while let Some(item) = stream.next().await {
            match item {
                ExecuteItem::Event(event) => {
                    execution.matches.extend(event.results);
                    execution.stats.update(&event.stats);
                }
                ExecuteItem::Alert(alert) => execution.alert = Some(alert),
                ExecuteItem::Error { message, .. } => {
                    return Err(SearchExecutionError::Other(message));
                }
            }
        }
        let status_ids: Vec<i32> = execution
            .stats
            .status
            .entries()
            .into_iter()
            .map(|(id, _)| id)
            .collect();
        if !status_ids.is_empty() {
            execution.repo_names = self.deps.repo_names(&status_ids).await;
        }
        execution.elapsed_ms = start.elapsed().as_millis().min(i32::MAX as u128) as i32;
        Ok(execution)
    }
}

/// Prefetch the selected `search` root field. `None` when the field was not
/// selected or no executor is injected.
pub async fn prefetch_search_roots(
    executor: &dyn SearchExecutor,
    fields: &[RootField],
) -> Option<SearchRoot> {
    let field = fields.iter().find(|field| field.name == "search")?;
    let args = SearchArgs {
        version: field.arguments.string("version").unwrap_or("V1").to_owned(),
        pattern_type: field.arguments.string("patternType").map(str::to_owned),
        query: field.arguments.string("query").unwrap_or("").to_owned(),
    };
    Some(match executor.plan(&args, SearchStreamMode::Batch).await {
        Ok(planned) => match executor.execute(&planned, SearchStreamMode::Batch).await {
            Ok(execution) => SearchRoot::Completed(execution),
            Err(error) => SearchRoot::Error(execution_error_message(error)),
        },
        Err(SearchExecutionError::Query {
            message,
            expected_operand,
        }) => SearchRoot::Alert {
            alert: alert_for_query(&message, expected_operand),
        },
        Err(error) => SearchRoot::Error(execution_error_message(error)),
    })
}

fn execution_error_message(error: SearchExecutionError) -> String {
    match error {
        SearchExecutionError::Query { message, .. } | SearchExecutionError::Other(message) => {
            message
        }
    }
}

/// `Query.search`'s field value: a `Search` object, or the plan/execute error.
pub fn search_field(
    root: &SearchRoot,
    git: Option<Arc<crate::git_graphql::GitPrefetch>>,
) -> Result<Option<FieldValue>, ExecError> {
    match root {
        SearchRoot::Error(message) => Err(ExecError::new(message.clone())),
        SearchRoot::Completed(_) | SearchRoot::Alert { .. } => {
            Ok(Some(FieldValue::Object(Arc::new(SearchObject {
                root: root.clone(),
                git,
            }))))
        }
    }
}

fn empty_stats() -> Stats {
    Stats::default()
}

// ---------------------------------------------------------------------------
// `Search`
// ---------------------------------------------------------------------------

struct SearchObject {
    root: SearchRoot,
    git: Option<Arc<crate::git_graphql::GitPrefetch>>,
}

impl ObjectValue for SearchObject {
    fn type_name(&self) -> &'static str {
        "Search"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "results" => {
                let results = match &self.root {
                    SearchRoot::Completed(execution) => SearchResultsObject {
                        matches: execution.matches.clone(),
                        stats: execution.stats.clone(),
                        alert: execution.alert.clone(),
                        repo_names: execution.repo_names.clone(),
                        elapsed_ms: execution.elapsed_ms,
                        git: self.git.clone(),
                    },
                    SearchRoot::Alert { alert } => SearchResultsObject {
                        matches: Vec::new(),
                        stats: empty_stats(),
                        alert: Some(alert.clone()),
                        repo_names: Vec::new(),
                        elapsed_ms: 0,
                        git: self.git.clone(),
                    },
                    SearchRoot::Error(_) => unreachable!(),
                };
                Ok(Some(FieldValue::Object(Arc::new(results))))
            }
            "stats" => match &self.root {
                SearchRoot::Completed(execution) => Ok(Some(FieldValue::Object(Arc::new(
                    SearchResultsStatsObject {
                        matches: execution.matches.clone(),
                        stats: execution.stats.clone(),
                    },
                )))),
                // `alertSearchImplementer.Stats` returns `(nil, nil)`.
                SearchRoot::Alert { .. } => Ok(Some(FieldValue::Null)),
                SearchRoot::Error(_) => unreachable!(),
            },
            _ => Ok(None),
        }
    }
}

// ---------------------------------------------------------------------------
// `SearchResults`
// ---------------------------------------------------------------------------

struct SearchResultsObject {
    matches: Vec<Match>,
    stats: Stats,
    alert: Option<EventAlert>,
    repo_names: Vec<(i32, String)>,
    elapsed_ms: i32,
    git: Option<Arc<crate::git_graphql::GitPrefetch>>,
}

impl SearchResultsObject {
    fn match_count(&self) -> i32 {
        self.matches
            .iter()
            .map(|match_| match_.result_count())
            .sum::<i64>() as i32
    }

    fn approximate_result_count(&self) -> String {
        let count = self.match_count();
        if self.stats.is_limit_hit
            || self
                .stats
                .status
                .any(repo_status::CLONING | repo_status::TIMED_OUT)
        {
            format!("{count}+")
        } else {
            count.to_string()
        }
    }

    /// `matchesRepoIDs` → minimal repos, sorted by ID.
    fn match_repos(&self) -> Vec<MinimalRepo> {
        let mut repos: BTreeMap<i32, String> = BTreeMap::new();
        for match_ in &self.matches {
            let repo = match_.repo_name();
            repos.entry(repo.id).or_insert(repo.name);
        }
        repos
            .into_iter()
            .map(|(id, name)| MinimalRepo { id, name })
            .collect()
    }

    fn repos_by_status(&self, mask: u8) -> Vec<MinimalRepo> {
        let names: std::collections::HashMap<i32, String> =
            self.repo_names.iter().cloned().collect();
        self.stats
            .status
            .filter(mask)
            .into_iter()
            .filter_map(|id| {
                names.get(&id).map(|name| MinimalRepo {
                    id,
                    name: name.clone(),
                })
            })
            .collect()
    }
}

impl ObjectValue for SearchResultsObject {
    fn type_name(&self) -> &'static str {
        "SearchResults"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "results" => {
                let items: Vec<FieldValue> = self
                    .matches
                    .iter()
                    .filter_map(|match_| match_to_object(match_, self.git.clone()))
                    .collect();
                Ok(Some(FieldValue::List {
                    items,
                    element_type: Some("SearchResult"),
                }))
            }
            "matchCount" => Ok(Some(FieldValue::Json(Json::Int(i64::from(
                self.match_count(),
            ))))),
            "resultCount" => Ok(Some(FieldValue::Json(Json::Int(i64::from(
                self.match_count(),
            ))))),
            "approximateResultCount" => Ok(Some(FieldValue::Json(Json::Str(
                self.approximate_result_count(),
            )))),
            "limitHit" => Ok(Some(FieldValue::Json(Json::Bool(self.stats.is_limit_hit)))),
            // File matches are blamed through gitserver in the baseline; no
            // blame backend is ported, so they contribute nothing (the same
            // zero-value the Go blame failure path produces).
            "sparkline" => Ok(Some(sparkline_json(
                &self.matches,
                OffsetDateTime::now_utc(),
            ))),
            "repositories" => Ok(Some(FieldValue::List {
                items: self
                    .match_repos()
                    .into_iter()
                    .map(|repo| FieldValue::Object(Arc::new(repository_object(&repo))))
                    .collect(),
                element_type: Some("Repository"),
            })),
            "repositoriesCount" => Ok(Some(FieldValue::Json(Json::Int(i64::from(
                self.match_repos().len() as i32,
            ))))),
            "cloning" => Ok(Some(status_repositories(
                self.repos_by_status(repo_status::CLONING),
            ))),
            "missing" => Ok(Some(status_repositories(
                self.repos_by_status(repo_status::MISSING),
            ))),
            "timedout" => Ok(Some(status_repositories(
                self.repos_by_status(repo_status::TIMED_OUT),
            ))),
            "indexUnavailable" => Ok(Some(FieldValue::Json(Json::Bool(false)))),
            "alert" => match &self.alert {
                Some(alert) => Ok(Some(FieldValue::Object(Arc::new(SearchAlertObject {
                    alert: alert.clone(),
                })))),
                None => Ok(Some(FieldValue::Null)),
            },
            "elapsedMilliseconds" => Ok(Some(FieldValue::Json(Json::Int(i64::from(
                self.elapsed_ms,
            ))))),
            "dynamicFilters" => {
                let mut filters = SearchFilters::new();
                filters.update(&SearchEvent {
                    results: self.matches.clone(),
                    stats: self.stats.clone(),
                });
                Ok(Some(FieldValue::List {
                    items: filters
                        .compute()
                        .into_iter()
                        .map(|filter| FieldValue::Object(Arc::new(SearchFilterObject { filter })))
                        .collect(),
                    element_type: Some("SearchFilter"),
                }))
            }
            _ => Ok(None),
        }
    }
}

fn status_repositories(repos: Vec<MinimalRepo>) -> FieldValue {
    FieldValue::List {
        items: repos
            .into_iter()
            .map(|repo| FieldValue::Object(Arc::new(repository_object(&repo))))
            .collect(),
        element_type: Some("Repository"),
    }
}

fn match_to_object(
    match_: &Match,
    git: Option<Arc<crate::git_graphql::GitPrefetch>>,
) -> Option<FieldValue> {
    match match_ {
        Match::File(file) => Some(FieldValue::Object(Arc::new(FileMatchObject {
            match_: file.clone(),
        }))),
        Match::Commit(commit) => Some(FieldValue::Object(Arc::new(CommitSearchResultObject {
            match_: commit.clone(),
            git,
        }))),
        Match::Repo(repo) => Some(FieldValue::Object(Arc::new(repository_object(
            &MinimalRepo {
                id: repo.id,
                name: repo.name.clone(),
            },
        )))),
        // `matchesToResolvers` ignores owner matches (todo(own)).
        Match::Owner(_) => None,
    }
}

// ---------------------------------------------------------------------------
// `SearchResultsStats`
// ---------------------------------------------------------------------------

struct SearchResultsStatsObject {
    matches: Vec<Match>,
    stats: Stats,
}

impl ObjectValue for SearchResultsStatsObject {
    fn type_name(&self) -> &'static str {
        "SearchResultsStats"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "approximateResultCount" => {
                let results = SearchResultsObject {
                    matches: self.matches.clone(),
                    stats: self.stats.clone(),
                    alert: None,
                    repo_names: Vec::new(),
                    elapsed_ms: 0,
                    git: None,
                };
                Ok(Some(FieldValue::Json(Json::Str(
                    results.approximate_result_count(),
                ))))
            }
            "sparkline" => Ok(Some(sparkline_json(
                &self.matches,
                OffsetDateTime::now_utc(),
            ))),
            "languages" => {
                let languages = search_results_stat_languages(&self.matches)
                    .map_err(|message| ExecError::new(message.as_str()))?;
                Ok(Some(FieldValue::List {
                    items: languages
                        .into_iter()
                        .map(|lang| FieldValue::Object(Arc::new(LanguageStatisticsObject { lang })))
                        .collect(),
                    element_type: Some("LanguageStatistics"),
                }))
            }
            _ => Ok(None),
        }
    }
}

// ---------------------------------------------------------------------------
// `SearchResult` union
// ---------------------------------------------------------------------------

struct FileMatchObject {
    match_: FileMatch,
}

impl ObjectValue for FileMatchObject {
    fn type_name(&self) -> &'static str {
        "FileMatch"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "file" => Ok(Some(FieldValue::Object(Arc::new(GitBlobObject {
                file: self.match_.file.clone(),
            })))),
            "repository" => Ok(Some(FieldValue::Object(Arc::new(repository_object(
                &self.match_.file.repo,
            ))))),
            "revSpec" => match &self.match_.file.input_rev {
                Some(rev) if !rev.is_empty() => {
                    Ok(Some(FieldValue::Object(Arc::new(GitRevSpecExprObject {
                        expr: rev.clone(),
                    }))))
                }
                _ => Ok(Some(FieldValue::Null)),
            },
            "symbols" => Ok(Some(FieldValue::List {
                items: self
                    .match_
                    .symbols
                    .iter()
                    .map(|symbol| {
                        FieldValue::Object(Arc::new(SymbolObject {
                            symbol: symbol.clone(),
                        }))
                    })
                    .collect(),
                element_type: Some("Symbol"),
            })),
            "lineMatches" => {
                let lines = as_line_matches(&self.match_.chunk_matches);
                Ok(Some(FieldValue::List {
                    items: lines
                        .into_iter()
                        .map(|line| FieldValue::Object(Arc::new(LineMatchObject { line })))
                        .collect(),
                    element_type: Some("LineMatch"),
                }))
            }
            "chunkMatches" => Ok(Some(FieldValue::List {
                items: self
                    .match_
                    .chunk_matches
                    .iter()
                    .map(|chunk| {
                        FieldValue::Object(Arc::new(ChunkMatchObject {
                            chunk: chunk.clone(),
                        }))
                    })
                    .collect(),
                element_type: Some("ChunkMatch"),
            })),
            "pathMatches" => Ok(Some(range_list(&self.match_.path_matches))),
            "limitHit" => Ok(Some(FieldValue::Json(Json::Bool(self.match_.limit_hit)))),
            "languages" => Ok(Some(FieldValue::Json(Json::Arr(
                file_languages(&self.match_.file)
                    .into_iter()
                    .map(Json::Str)
                    .collect(),
            )))),
            _ => Ok(None),
        }
    }
}

struct GitBlobObject {
    file: FileRef,
}

impl ObjectValue for GitBlobObject {
    fn type_name(&self) -> &'static str {
        "GitBlob"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "path" => Ok(Some(FieldValue::Json(Json::Str(self.file.path.clone())))),
            "name" => Ok(Some(FieldValue::Json(Json::Str(go_path_base(
                &self.file.path,
            ))))),
            "isDirectory" => Ok(Some(FieldValue::Json(Json::Bool(false)))),
            "url" => Ok(Some(FieldValue::Json(Json::Str(blob_url(
                &self.file.repo.name,
                &self.file.commit_id,
                self.file.input_rev.as_deref(),
                &self.file.path,
            ))))),
            // `GitTreeEntryResolver.CanonicalURL` always pins the OID.
            "canonicalURL" => Ok(Some(FieldValue::Json(Json::Str(blob_url(
                &self.file.repo.name,
                &self.file.commit_id,
                Some(self.file.commit_id.as_str()),
                &self.file.path,
            ))))),
            "commit" => Ok(Some(FieldValue::Object(Arc::new(GitCommitObject {
                repo: self.file.repo.clone(),
                oid: self.file.commit_id.clone(),
                commit: None,
                input_rev: self.file.input_rev.clone(),
            })))),
            "repository" => Ok(Some(FieldValue::Object(Arc::new(repository_object(
                &self.file.repo,
            ))))),
            // Content/size/rich-HTML fields require gitserver; the port has no
            // such backend, so they report the zero values.
            "content" | "richHTML" => Ok(Some(FieldValue::Json(Json::Str(String::new())))),
            "byteSize" | "totalLines" => Ok(Some(FieldValue::Json(Json::Int(0)))),
            "binary" => Ok(Some(FieldValue::Json(Json::Bool(false)))),
            _ => Ok(None),
        }
    }
}

struct GitRevSpecExprObject {
    expr: String,
}

impl ObjectValue for GitRevSpecExprObject {
    fn type_name(&self) -> &'static str {
        "GitRevSpecExpr"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "expr" => Ok(Some(FieldValue::Json(Json::Str(self.expr.clone())))),
            "object" => Ok(Some(FieldValue::Null)),
            _ => Ok(None),
        }
    }
}

struct SymbolObject {
    symbol: SymbolMatch,
}

impl ObjectValue for SymbolObject {
    fn type_name(&self) -> &'static str {
        "Symbol"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        let symbol = &self.symbol.symbol;
        match field {
            "name" => Ok(Some(FieldValue::Json(Json::Str(symbol.name.clone())))),
            "containerName" => {
                if symbol.parent.is_empty() {
                    Ok(Some(FieldValue::Null))
                } else {
                    Ok(Some(FieldValue::Json(Json::Str(symbol.parent.clone()))))
                }
            }
            "kind" => Ok(Some(FieldValue::Json(Json::Str(symbol_kind(&symbol.kind))))),
            "language" => Ok(Some(FieldValue::Json(Json::Str(symbol.language.clone())))),
            "location" => Ok(Some(FieldValue::Object(Arc::new(LocationObject {
                file: self.symbol.file.clone(),
                symbol: symbol.clone(),
            })))),
            "url" => Ok(Some(FieldValue::Json(Json::Str(symbol_url(
                &self.symbol.file,
                symbol,
                false,
            ))))),
            "canonicalURL" => Ok(Some(FieldValue::Json(Json::Str(symbol_url(
                &self.symbol.file,
                symbol,
                true,
            ))))),
            "fileLocal" => Ok(Some(FieldValue::Json(Json::Bool(false)))),
            _ => Ok(None),
        }
    }
}

struct LocationObject {
    file: FileRef,
    symbol: Symbol,
}

impl ObjectValue for LocationObject {
    fn type_name(&self) -> &'static str {
        "Location"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "resource" => Ok(Some(FieldValue::Object(Arc::new(GitBlobObject {
                file: FileRef {
                    path: self.symbol.path.clone(),
                    ..self.file.clone()
                },
            })))),
            "range" => Ok(Some(FieldValue::Object(Arc::new(RangeObject {
                range: Range {
                    start: Location {
                        offset: 0,
                        line: i64::from(self.symbol.line - 1),
                        column: i64::from(self.symbol.character),
                    },
                    end: Location {
                        offset: 0,
                        line: i64::from(self.symbol.line - 1),
                        column: i64::from(self.symbol.character + self.symbol.name.len() as i32),
                    },
                },
            })))),
            "url" => Ok(Some(FieldValue::Json(Json::Str(symbol_url(
                &self.file,
                &self.symbol,
                false,
            ))))),
            "canonicalURL" => Ok(Some(FieldValue::Json(Json::Str(symbol_url(
                &self.file,
                &self.symbol,
                true,
            ))))),
            _ => Ok(None),
        }
    }
}

struct PositionObject {
    line: i32,
    character: i32,
}

impl ObjectValue for PositionObject {
    fn type_name(&self) -> &'static str {
        "Position"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "line" => Ok(Some(FieldValue::Json(Json::Int(i64::from(self.line))))),
            "character" => Ok(Some(FieldValue::Json(Json::Int(i64::from(self.character))))),
            _ => Ok(None),
        }
    }
}

struct RangeObject {
    range: Range,
}

impl ObjectValue for RangeObject {
    fn type_name(&self) -> &'static str {
        "Range"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "start" => Ok(Some(FieldValue::Object(Arc::new(PositionObject {
                line: self.range.start.line as i32,
                character: self.range.start.column as i32,
            })))),
            "end" => Ok(Some(FieldValue::Object(Arc::new(PositionObject {
                line: self.range.end.line as i32,
                character: self.range.end.column as i32,
            })))),
            _ => Ok(None),
        }
    }
}

fn range_list(ranges: &[Range]) -> FieldValue {
    FieldValue::List {
        items: ranges
            .iter()
            .map(|range| FieldValue::Object(Arc::new(RangeObject { range: *range })))
            .collect(),
        element_type: Some("Range"),
    }
}

struct LineMatchData {
    preview: String,
    line_number: i32,
    offset_and_lengths: Vec<[i32; 2]>,
}

struct LineMatchObject {
    line: LineMatchData,
}

impl ObjectValue for LineMatchObject {
    fn type_name(&self) -> &'static str {
        "LineMatch"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "preview" => Ok(Some(FieldValue::Json(Json::Str(self.line.preview.clone())))),
            "lineNumber" => Ok(Some(FieldValue::Json(Json::Int(i64::from(
                self.line.line_number,
            ))))),
            "offsetAndLengths" => Ok(Some(FieldValue::Json(Json::Arr(
                self.line
                    .offset_and_lengths
                    .iter()
                    .map(|pair| {
                        Json::Arr(vec![
                            Json::Int(i64::from(pair[0])),
                            Json::Int(i64::from(pair[1])),
                        ])
                    })
                    .collect(),
            )))),
            "limitHit" => Ok(Some(FieldValue::Json(Json::Bool(false)))),
            _ => Ok(None),
        }
    }
}

struct ChunkMatchObject {
    chunk: crate::search_stream::convert::ChunkMatch,
}

impl ObjectValue for ChunkMatchObject {
    fn type_name(&self) -> &'static str {
        "ChunkMatch"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "content" => Ok(Some(FieldValue::Json(Json::Str(
                self.chunk.content.clone(),
            )))),
            "contentStart" => Ok(Some(FieldValue::Object(Arc::new(PositionObject {
                line: self.chunk.content_start.line as i32,
                character: self.chunk.content_start.column as i32,
            })))),
            "ranges" => Ok(Some(range_list(&self.chunk.ranges))),
            _ => Ok(None),
        }
    }
}

struct CommitSearchResultObject {
    match_: crate::search_stream::convert::CommitMatch,
    git: Option<Arc<crate::git_graphql::GitPrefetch>>,
}

impl ObjectValue for CommitSearchResultObject {
    fn type_name(&self) -> &'static str {
        "CommitSearchResult"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "label" => Ok(Some(FieldValue::Object(Arc::new(MarkdownObject {
                text: commit_label(&self.match_),
            })))),
            "url" => Ok(Some(FieldValue::Json(Json::Str(commit_url(&self.match_))))),
            "detail" => Ok(Some(FieldValue::Object(Arc::new(MarkdownObject {
                text: commit_detail(&self.match_, OffsetDateTime::now_utc()),
            })))),
            "matches" => {
                let body = commit_body(&self.match_);
                let highlights: Vec<[i32; 3]> = body
                    .matched_ranges
                    .iter()
                    .flat_map(|range| range_to_highlights(&body.content, range))
                    .collect();
                Ok(Some(FieldValue::List {
                    items: vec![FieldValue::Object(Arc::new(SearchResultMatchObject {
                        url: commit_url(&self.match_),
                        body: body.content,
                        highlights,
                    }))],
                    element_type: Some("SearchResultMatch"),
                }))
            }
            "commit" => Ok(Some(FieldValue::Object(Arc::new(GitCommitObject {
                repo: self.match_.repo.clone(),
                oid: self.match_.commit.id.clone(),
                commit: Some(self.match_.commit.clone()),
                input_rev: None,
            })))),
            "refs" => Ok(Some(git_ref_list(
                &self.match_.repo,
                &self.match_.refs,
                self.git.clone(),
            ))),
            "sourceRefs" => Ok(Some(git_ref_list(
                &self.match_.repo,
                &self.match_.source_refs,
                self.git.clone(),
            ))),
            "messagePreview" => match &self.match_.message_preview {
                Some(preview) => Ok(Some(FieldValue::Object(Arc::new(
                    HighlightedStringObject::from_matched_string(preview),
                )))),
                None => Ok(Some(FieldValue::Null)),
            },
            "diffPreview" => match &self.match_.diff_preview {
                Some(preview) => Ok(Some(FieldValue::Object(Arc::new(
                    HighlightedStringObject::from_matched_string(preview),
                )))),
                None => Ok(Some(FieldValue::Null)),
            },
            _ => Ok(None),
        }
    }
}

struct GitCommitObject {
    repo: MinimalRepo,
    oid: String,
    commit: Option<crate::search_stream::convert::GitCommit>,
    input_rev: Option<String>,
}

impl GitCommitObject {
    fn input_rev_or_immutable_rev(&self) -> String {
        match &self.input_rev {
            Some(rev) if !rev.is_empty() => rev.clone(),
            _ => self.oid.clone(),
        }
    }

    fn signature(&self, field: &str) -> Result<Option<FieldValue>, ExecError> {
        let Some(commit) = &self.commit else {
            return Err(ExecError::new(format!(
                "commit fields are not ported without a loaded commit ({field})"
            )));
        };
        let signature = if field == "author" {
            Some(&commit.author)
        } else {
            Some(&commit.committer)
        };
        match signature {
            Some(signature) => Ok(Some(FieldValue::Object(Arc::new(SignatureObject {
                name: signature.name.clone(),
                email: signature.email.clone(),
                date: signature.date,
            })))),
            None => Ok(Some(FieldValue::Null)),
        }
    }
}

impl ObjectValue for GitCommitObject {
    fn type_name(&self) -> &'static str {
        "GitCommit"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "id" => Ok(Some(FieldValue::Json(Json::Str(commit_relay_id(
                self.repo.id,
                &self.oid,
            ))))),
            "repository" => Ok(Some(FieldValue::Object(Arc::new(repository_object(
                &self.repo,
            ))))),
            "oid" => Ok(Some(FieldValue::Json(Json::Str(self.oid.clone())))),
            "abbreviatedOID" => Ok(Some(FieldValue::Json(Json::Str(
                self.oid.chars().take(7).collect(),
            )))),
            "perforceChangelist" => Ok(Some(FieldValue::Null)),
            "author" => self.signature("author"),
            "committer" => self.signature("committer"),
            "message" => match &self.commit {
                Some(commit) => Ok(Some(FieldValue::Json(Json::Str(commit.message.clone())))),
                None => Err(ExecError::new(
                    "commit message is not ported without a loaded commit",
                )),
            },
            "subject" => match &self.commit {
                Some(commit) => Ok(Some(FieldValue::Json(Json::Str(commit_subject(
                    &commit.message,
                ))))),
                None => Err(ExecError::new(
                    "commit subject is not ported without a loaded commit",
                )),
            },
            "body" => match &self.commit {
                Some(commit) => match commit_body_message(&commit.message) {
                    Some(body) => Ok(Some(FieldValue::Json(Json::Str(body)))),
                    None => Ok(Some(FieldValue::Null)),
                },
                None => Err(ExecError::new(
                    "commit body is not ported without a loaded commit",
                )),
            },
            "parents" => Ok(Some(FieldValue::List {
                items: Vec::new(),
                element_type: Some("GitCommit"),
            })),
            "url" => Ok(Some(FieldValue::Json(Json::Str(go_url_path(&format!(
                "/{}/-/commit/{}",
                self.repo.name,
                self.input_rev_or_immutable_rev()
            )))))),
            "canonicalURL" => Ok(Some(FieldValue::Json(Json::Str(go_url_path(&format!(
                "/{}/-/commit/{}",
                self.repo.name, self.oid
            )))))),
            "externalURLs" => Ok(Some(FieldValue::List {
                items: Vec::new(),
                element_type: Some("ExternalLink"),
            })),
            _ => Ok(None),
        }
    }
}

struct SignatureObject {
    name: String,
    email: String,
    date: OffsetDateTime,
}

impl ObjectValue for SignatureObject {
    fn type_name(&self) -> &'static str {
        "Signature"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "person" => Ok(Some(FieldValue::Object(Arc::new(PersonObject {
                name: self.name.clone(),
                email: self.email.clone(),
            })))),
            "date" => Ok(Some(FieldValue::Json(Json::Str(
                self.date
                    .format(&Rfc3339)
                    .unwrap_or_else(|_| self.date.to_string()),
            )))),
            _ => Ok(None),
        }
    }
}

struct PersonObject {
    name: String,
    email: String,
}

impl ObjectValue for PersonObject {
    fn type_name(&self) -> &'static str {
        "Person"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "name" => Ok(Some(FieldValue::Json(Json::Str(self.name.clone())))),
            "email" => Ok(Some(FieldValue::Json(Json::Str(self.email.clone())))),
            "displayName" => {
                let name = self.name.trim();
                let display = if !name.is_empty() {
                    name.to_owned()
                } else if !self.email.is_empty() {
                    self.email.clone()
                } else {
                    "unknown".to_owned()
                };
                Ok(Some(FieldValue::Json(Json::Str(display))))
            }
            "avatarURL" | "user" => Ok(Some(FieldValue::Null)),
            _ => Ok(None),
        }
    }
}

struct MarkdownObject {
    text: String,
}

impl ObjectValue for MarkdownObject {
    fn type_name(&self) -> &'static str {
        "Markdown"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "text" => Ok(Some(FieldValue::Json(Json::Str(self.text.clone())))),
            "html" => Ok(Some(FieldValue::Json(Json::Str(
                markdown_html(&self.text).map_err(|message| ExecError::new(message.as_str()))?,
            )))),
            _ => Ok(None),
        }
    }
}

struct HighlightedStringObject {
    value: String,
    highlights: Vec<[i32; 3]>,
}

impl HighlightedStringObject {
    fn from_matched_string(matched: &crate::search_stream::convert::MatchedString) -> Self {
        let highlights = matched
            .matched_ranges
            .iter()
            .flat_map(|range| range_to_highlights(&matched.content, range))
            .collect();
        Self {
            value: matched.content.clone(),
            highlights,
        }
    }
}

impl ObjectValue for HighlightedStringObject {
    fn type_name(&self) -> &'static str {
        "HighlightedString"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "value" => Ok(Some(FieldValue::Json(Json::Str(self.value.clone())))),
            "highlights" => Ok(Some(highlight_list(&self.highlights))),
            _ => Ok(None),
        }
    }
}

fn highlight_list(highlights: &[[i32; 3]]) -> FieldValue {
    FieldValue::List {
        items: highlights
            .iter()
            .map(|highlight| {
                FieldValue::Object(Arc::new(HighlightObject {
                    line: highlight[0],
                    character: highlight[1],
                    length: highlight[2],
                }))
            })
            .collect(),
        element_type: Some("Highlight"),
    }
}

struct HighlightObject {
    line: i32,
    character: i32,
    length: i32,
}

impl ObjectValue for HighlightObject {
    fn type_name(&self) -> &'static str {
        "Highlight"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "line" => Ok(Some(FieldValue::Json(Json::Int(i64::from(self.line))))),
            "character" => Ok(Some(FieldValue::Json(Json::Int(i64::from(self.character))))),
            "length" => Ok(Some(FieldValue::Json(Json::Int(i64::from(self.length))))),
            _ => Ok(None),
        }
    }
}

struct SearchResultMatchObject {
    url: String,
    body: String,
    highlights: Vec<[i32; 3]>,
}

impl ObjectValue for SearchResultMatchObject {
    fn type_name(&self) -> &'static str {
        "SearchResultMatch"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "url" => Ok(Some(FieldValue::Json(Json::Str(self.url.clone())))),
            "body" => Ok(Some(FieldValue::Object(Arc::new(MarkdownObject {
                text: self.body.clone(),
            })))),
            "highlights" => Ok(Some(highlight_list(&self.highlights))),
            _ => Ok(None),
        }
    }
}

// ---------------------------------------------------------------------------
// Alerts and filters
// ---------------------------------------------------------------------------

struct SearchAlertObject {
    alert: EventAlert,
}

impl ObjectValue for SearchAlertObject {
    fn type_name(&self) -> &'static str {
        "SearchAlert"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "title" => Ok(Some(FieldValue::Json(Json::Str(self.alert.title.clone())))),
            "description" => {
                if self.alert.description.is_empty() {
                    Ok(Some(FieldValue::Null))
                } else {
                    Ok(Some(FieldValue::Json(Json::Str(
                        self.alert.description.clone(),
                    ))))
                }
            }
            "kind" => {
                if self.alert.kind.is_empty() {
                    Ok(Some(FieldValue::Null))
                } else {
                    Ok(Some(FieldValue::Json(Json::Str(self.alert.kind.clone()))))
                }
            }
            "proposedQueries" => match &self.alert.proposed_queries {
                Some(queries) if !queries.is_empty() => Ok(Some(FieldValue::List {
                    items: queries
                        .iter()
                        .map(|query| {
                            FieldValue::Object(Arc::new(SearchQueryDescriptionObject {
                                query: query.clone(),
                            }))
                        })
                        .collect(),
                    element_type: Some("SearchQueryDescription"),
                })),
                _ => Ok(Some(FieldValue::Null)),
            },
            _ => Ok(None),
        }
    }
}

struct SearchQueryDescriptionObject {
    query: QueryDescription,
}

impl ObjectValue for SearchQueryDescriptionObject {
    fn type_name(&self) -> &'static str {
        "SearchQueryDescription"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "description" => {
                if self.query.description.is_empty() {
                    Ok(Some(FieldValue::Null))
                } else {
                    Ok(Some(FieldValue::Json(Json::Str(
                        self.query.description.clone(),
                    ))))
                }
            }
            "query" => Ok(Some(FieldValue::Json(Json::Str(self.query.query.clone())))),
            "annotations" => {
                if self.query.annotations.is_empty() {
                    Ok(Some(FieldValue::Null))
                } else {
                    Ok(Some(FieldValue::List {
                        items: self
                            .query
                            .annotations
                            .iter()
                            .map(|annotation| {
                                FieldValue::Object(Arc::new(SearchQueryAnnotationObject {
                                    name: annotation.name.clone(),
                                    value: annotation.value.clone(),
                                }))
                            })
                            .collect(),
                        element_type: Some("SearchQueryAnnotation"),
                    }))
                }
            }
            _ => Ok(None),
        }
    }
}

struct SearchQueryAnnotationObject {
    name: String,
    value: String,
}

impl ObjectValue for SearchQueryAnnotationObject {
    fn type_name(&self) -> &'static str {
        "SearchQueryAnnotation"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "name" => Ok(Some(FieldValue::Json(Json::Str(self.name.clone())))),
            "value" => Ok(Some(FieldValue::Json(Json::Str(self.value.clone())))),
            _ => Ok(None),
        }
    }
}

struct SearchFilterObject {
    filter: Filter,
}

impl ObjectValue for SearchFilterObject {
    fn type_name(&self) -> &'static str {
        "SearchFilter"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "value" => Ok(Some(FieldValue::Json(Json::Str(self.filter.value.clone())))),
            "label" => Ok(Some(FieldValue::Json(Json::Str(self.filter.label.clone())))),
            "count" => Ok(Some(FieldValue::Json(Json::Int(self.filter.count)))),
            // `searchFilterResolver.LimitHit` always returns true.
            "limitHit" => Ok(Some(FieldValue::Json(Json::Bool(true)))),
            "kind" => Ok(Some(FieldValue::Json(Json::Str(self.filter.kind.clone())))),
            _ => Ok(None),
        }
    }
}

// ---------------------------------------------------------------------------
// Go-compatible helpers
// ---------------------------------------------------------------------------

fn repository_object(repo: &MinimalRepo) -> RepositoryObject {
    RepositoryObject::from_data(RepositoryData {
        id: repo.id,
        name: repo.name.clone(),
        uri: repo.name.clone(),
        description: String::new(),
        fork: false,
        archived: false,
        private: false,
        stars: 0,
        external_id: String::new(),
        external_service_type: String::new(),
        external_service_id: String::new(),
    })
}

fn commit_relay_id(repo_id: i32, oid: &str) -> String {
    use base64::Engine as _;
    let repo_relay = RepositoryObject::relay_id(repo_id);
    base64::engine::general_purpose::URL_SAFE.encode(format!(
        "GitCommit:{{\"r\":\"{repo_relay}\",\"c\":\"{oid}\"}}"
    ))
}

/// `CommitMatch.URL().String()`.
fn commit_url(commit: &crate::search_stream::convert::CommitMatch) -> String {
    go_url_path(&format!(
        "/{}/-/commit/{}",
        commit.repo.name, commit.commit.id
    ))
}

/// `CommitMatch.Label()`.
fn commit_label(commit: &crate::search_stream::convert::CommitMatch) -> String {
    let message = commit_subject(&commit.commit.message);
    let author = &commit.commit.author.name;
    let repo_name = display_repo_name(&commit.repo.name);
    let repo_url = go_url_path(&format!("/{}", commit.repo.name));
    let commit_url = commit_url(commit);
    format!("[{repo_name}]({repo_url}) › [{author}]({commit_url}): [{message}]({commit_url})")
}

/// `CommitMatch.Detail()`. The suffix uses `time.Now()`; callers pass the
/// resolver's request time.
fn commit_detail(
    commit: &crate::search_stream::convert::CommitMatch,
    now: OffsetDateTime,
) -> String {
    let commit_hash: String = commit.commit.id.chars().take(7).collect();
    format!(
        "[`{commit_hash}` {}]({})",
        crate::search_stream::convert::timeago_english_no_max(commit.commit.author.date, now),
        commit_url(commit)
    )
}

/// `CommitMatch.Body()`.
fn commit_body(
    commit: &crate::search_stream::convert::CommitMatch,
) -> crate::search_stream::convert::MatchedString {
    if let Some(diff) = &commit.diff_preview {
        return crate::search_stream::convert::MatchedString {
            content: format!("```diff\n{}\n```", diff.content),
            matched_ranges: add_shift(&diff.matched_ranges, "```diff\n"),
        };
    }
    let message =
        commit
            .message_preview
            .clone()
            .unwrap_or(crate::search_stream::convert::MatchedString {
                content: String::new(),
                matched_ranges: Vec::new(),
            });
    crate::search_stream::convert::MatchedString {
        content: format!("```COMMIT_EDITMSG\n{}\n```", message.content),
        matched_ranges: add_shift(&message.matched_ranges, "```COMMIT_EDITMSG\n"),
    }
}

fn add_shift(ranges: &[Range], prefix: &str) -> Vec<Range> {
    let shift = Location {
        offset: prefix.len() as i64,
        line: 1,
        column: 0,
    };
    ranges
        .iter()
        .map(|range| Range {
            start: Location {
                offset: range.start.offset + shift.offset,
                line: range.start.line + shift.line,
                column: range.start.column + shift.column,
            },
            end: Location {
                offset: range.end.offset + shift.offset,
                line: range.end.line + shift.line,
                column: range.end.column + shift.column,
            },
        })
        .collect()
}

/// `result.rangeToHighlights` (`bufio.Scanner` line semantics).
fn range_to_highlights(content: &str, range: &Range) -> Vec<[i32; 3]> {
    let bytes = content.as_bytes();
    let start = range.start.offset.max(0) as usize;
    let end = range.end.offset.max(0) as usize;
    if start > bytes.len() || end > bytes.len() || start > end {
        return Vec::new();
    }
    let mut out = Vec::new();
    for (line_num, line) in (range.start.line..).zip(scan_lines(&bytes[start..end])) {
        let mut character = 0i64;
        if line_num == range.start.line {
            character = range.start.column;
        }
        let mut length = line.len() as i64;
        if line_num == range.end.line {
            length = range.end.column - character;
        }
        if length > 0 {
            out.push([line_num as i32, character as i32, length as i32]);
        }
    }
    out
}

fn scan_lines(data: &[u8]) -> Vec<&[u8]> {
    if data.is_empty() {
        return Vec::new();
    }
    let mut lines: Vec<&[u8]> = data.split(|byte| *byte == b'\n').collect();
    if data.ends_with(b"\n") {
        lines.pop();
    }
    lines
        .into_iter()
        .map(|line| line.strip_suffix(b"\r").unwrap_or(line))
        .collect()
}

/// `ChunkMatch.AsLineMatches` (`result.LineMatch` fields).
fn as_line_matches(chunks: &[crate::search_stream::convert::ChunkMatch]) -> Vec<LineMatchData> {
    let mut out = Vec::new();
    for chunk in chunks {
        let content = chunk.content.strip_suffix('\n').unwrap_or(&chunk.content);
        let lines: Vec<&str> = content.split('\n').collect();
        for (i, line) in lines.iter().enumerate() {
            let line_number = chunk.content_start.line + i as i64;
            let mut offset_and_lengths: Vec<[i32; 2]> = Vec::new();
            for range in &chunk.ranges {
                let mut range_line = range.start.line;
                while range_line <= range.end.line {
                    if range_line == line_number {
                        let mut start = 0i64;
                        if range_line == range.start.line {
                            start = range.start.column;
                        }
                        let mut end = line.chars().count() as i64;
                        if range_line == range.end.line {
                            end = range.end.column;
                        }
                        if start != end {
                            offset_and_lengths.push([start as i32, (end - start) as i32]);
                        }
                    }
                    range_line += 1;
                }
            }
            if !offset_and_lengths.is_empty() {
                out.push(LineMatchData {
                    preview: (*line).to_owned(),
                    line_number: line_number as i32,
                    offset_and_lengths,
                });
            }
        }
    }
    out
}

/// `File.Languages()`.
fn file_languages(file: &FileRef) -> Vec<String> {
    if !file.precise_language.is_empty() {
        return vec![file.precise_language.clone()];
    }
    langmatch::get_languages::<fn() -> std::io::Result<Vec<u8>>>(&file.path, None)
        .unwrap_or_default()
}

/// `path.Base`.
fn go_path_base(path: &str) -> String {
    if path.is_empty() {
        return ".".to_owned();
    }
    let trimmed = path.trim_end_matches('/');
    if trimmed.is_empty() {
        return "/".to_owned();
    }
    trimmed.rsplit('/').next().unwrap_or(trimmed).to_owned()
}

/// `GitTreeEntryResolver.url` (`repoRevURL` + `/-/blob/` + path).
fn blob_url(repo: &str, commit_id: &str, input_rev: Option<&str>, path: &str) -> String {
    let rev = match input_rev {
        Some(rev) => rev,
        None => commit_id,
    };
    let mut url = format!("/{repo}");
    if !rev.is_empty() {
        url.push('@');
        url.push_str(rev);
    }
    url.push_str("/-/blob/");
    url.push_str(path);
    go_url_path(&url)
}

/// `symbolResolver.URL`/`CanonicalURL` (`Location.URL` + `?L…`).
fn symbol_url(file: &FileRef, symbol: &Symbol, canonical: bool) -> String {
    let rev = if canonical { commit_key(file) } else { None };
    let base = blob_url(
        &file.repo.name,
        &file.commit_id,
        rev.as_deref().or(file.input_rev.as_deref()),
        &symbol.path,
    );
    format!("{base}?{}", url_fragment_from_range(symbol))
}

fn commit_key(file: &FileRef) -> Option<String> {
    Some(file.commit_id.clone())
}

/// `urlFragmentFromRange`.
fn url_fragment_from_range(symbol: &Symbol) -> String {
    let start_line = symbol.line - 1;
    let start_char = symbol.character;
    let end_line = start_line;
    let end_char = symbol.character + symbol.name.len() as i32;
    if start_line == end_line && start_char == end_char {
        return format!("L{}", start_line + 1);
    }
    let has_character = start_char != 0 || end_char != 0;
    format!(
        "L{}-{}",
        line_spec_from_position(start_line, start_char, has_character),
        line_spec_from_position(end_line, end_char, has_character)
    )
}

fn line_spec_from_position(line: i32, character: i32, force_include_character: bool) -> String {
    if !force_include_character && character == 0 {
        return (line + 1).to_string();
    }
    format!("{}:{}", line + 1, character + 1)
}

/// `Symbol.Kind`: empty when the kind is unknown, else the LSP kind name
/// uppercased. Shared with the M9.8iy tree-entry `symbol` resolver.
pub(crate) fn symbol_kind(kind: &str) -> String {
    match lsp_kind(kind) {
        None => "UNKNOWN".to_owned(),
        Some(kind) => lsp_kind_name(kind).to_ascii_uppercase(),
    }
}

fn lsp_kind(kind: &str) -> Option<u16> {
    Some(match kind {
        "file" => 1,
        "module" => 2,
        "namespace" => 3,
        "package" | "packagename" | "subprogspec" => 4,
        "class" | "classes" | "type" | "service" | "typedef" | "union" | "section" | "subtype"
        | "component" => 5,
        "accessor" | "getter" | "method" | "methodalias" | "methodspec" | "setter" => 6,
        "property" => 7,
        "field" | "member" | "anonmember" | "recordfield" => 8,
        "constructor" => 9,
        "enum" | "enumerator" => 10,
        "interface" => 11,
        "function" | "func" | "subroutine" | "macro" | "subprogram" | "procedure" | "command"
        | "singletonmethod" => 12,
        "variable" | "var" | "functionvar" | "define" | "alias" | "val" => 13,
        "constant" | "const" => 14,
        "string" | "message" | "heredoc" => 15,
        "number" => 16,
        "bool" | "boolean" => 17,
        "array" => 18,
        "object" | "literal" | "map" => 19,
        "key" | "label" | "target" | "selector" | "id" | "tag" => 20,
        "null" => 21,
        "enum member" | "enumconstant" | "enummember" => 22,
        "struct" => 23,
        "event" => 24,
        "operator" => 25,
        "type parameter" | "annotation" => 26,
        _ => return None,
    })
}

fn lsp_kind_name(kind: u16) -> &'static str {
    match kind {
        1 => "File",
        2 => "Module",
        3 => "Namespace",
        4 => "Package",
        5 => "Class",
        6 => "Method",
        7 => "Property",
        8 => "Field",
        9 => "Constructor",
        10 => "Enum",
        11 => "Interface",
        12 => "Function",
        13 => "Variable",
        14 => "Constant",
        15 => "String",
        16 => "Number",
        17 => "Boolean",
        18 => "Array",
        19 => "Object",
        20 => "Key",
        21 => "Null",
        22 => "EnumMember",
        23 => "Struct",
        24 => "Event",
        25 => "Operator",
        26 => "TypeParameter",
        _ => "UNKNOWN",
    }
}

fn display_repo_name(repo_path: &str) -> String {
    let mut parts: Vec<&str> = repo_path.split('/').collect();
    if parts.len() >= 3 && parts[0].contains('.') {
        parts.remove(0);
    }
    parts.join("/")
}

fn commit_subject(message: &str) -> String {
    match message.find('\n') {
        None => message.trim().to_owned(),
        Some(index) => message[..index].trim().to_owned(),
    }
}

fn commit_body_message(message: &str) -> Option<String> {
    let index = message.find('\n')?;
    let body = message[index..].trim();
    if body.is_empty() {
        None
    } else {
        Some(body.to_owned())
    }
}

// ---------------------------------------------------------------------------
// M9.8ij: sparkline, language statistics, GitRef and Markdown HTML
// ---------------------------------------------------------------------------

/// `SearchResultsResolver.Sparkline` (`search_results.go:266-334`).
///
/// File matches are blamed through gitserver in the baseline; without a blame
/// backend they contribute nothing (the zero time is outside the window, just
/// like the baseline's blame-error path).
pub fn sparkline(matches: &[Match], now: OffsetDateTime) -> Vec<i32> {
    let mut points = vec![0i32; 30];
    for match_ in matches {
        if let Match::Commit(commit) = match_ {
            add_sparkline_point(&mut points, commit.commit.author.date, now);
        }
    }
    points
}

fn sparkline_json(matches: &[Match], now: OffsetDateTime) -> FieldValue {
    FieldValue::Json(Json::Arr(
        sparkline(matches, now)
            .into_iter()
            .map(|point| Json::Int(i64::from(point)))
            .collect(),
    ))
}

fn add_sparkline_point(sparkline: &mut [i32], t: OffsetDateTime, now: OffsetDateTime) {
    if t < now - Duration::days(30) {
        // Outside the range of the sparkline.
        return;
    }
    for (n, point) in sparkline.iter_mut().enumerate() {
        let n = n as i64;
        let d1 = now - Duration::days(n);
        let d2 = now - Duration::days(n - 1);
        if t > d1 && t < d2 {
            *point += 1;
        }
    }
}

/// One entry of `SearchResultsStats.languages`
/// (`internal/inventory.Lang`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LanguageStat {
    pub name: String,
    pub total_bytes: u64,
    pub total_lines: i64,
}

/// `searchResultsStatsLanguages` (`search_results_stats_languages.go:66-190`)
/// without a gitserver inventory backend.
///
/// The language of each file comes from the same detection the file resolver
/// uses. Line counts are the number of matched ranges; bytes are always 0
/// because the baseline reads the file through gitserver. Files without chunk
/// matches still contribute their language with 0 lines (the baseline
/// inventories them, but an empty reader yields 0 bytes/lines).
pub fn search_results_stat_languages(matches: &[Match]) -> Result<Vec<LanguageStat>, String> {
    let mut by_language: BTreeMap<String, LanguageStat> = BTreeMap::new();
    for match_ in matches {
        match match_ {
            Match::Commit(_) => {
                return Err("language statistics do not support diff searches".to_owned());
            }
            Match::File(file) => {
                let matched_lines: i64 = file
                    .chunk_matches
                    .iter()
                    .map(|chunk| chunk.ranges.len() as i64)
                    .sum();
                if let Some(name) = file_languages(&file.file).into_iter().next() {
                    if !name.is_empty() {
                        let stat =
                            by_language
                                .entry(name.clone())
                                .or_insert_with(|| LanguageStat {
                                    name,
                                    total_bytes: 0,
                                    total_lines: 0,
                                });
                        stat.total_lines += matched_lines;
                    }
                }
            }
            // Repo matches are only inventoried when every match is a repo
            // match, which needs the gitserver inventory backend (unported).
            Match::Repo(_) => {}
            // `OwnerMatch` is ignored by the baseline as well.
            Match::Owner(_) => {}
        }
    }

    let mut languages: Vec<LanguageStat> = by_language.into_values().collect();
    // `inventory.Sum`: lines descending, bytes descending, name ascending.
    languages.sort_by(|a, b| {
        b.total_lines
            .cmp(&a.total_lines)
            .then_with(|| b.total_bytes.cmp(&a.total_bytes))
            .then_with(|| a.name.cmp(&b.name))
    });
    Ok(languages)
}

struct LanguageStatisticsObject {
    lang: LanguageStat,
}

impl ObjectValue for LanguageStatisticsObject {
    fn type_name(&self) -> &'static str {
        "LanguageStatistics"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "name" => Ok(Some(FieldValue::Json(Json::Str(self.lang.name.clone())))),
            "totalBytes" => Ok(Some(FieldValue::Json(Json::Float(
                self.lang.total_bytes as f64,
            )))),
            "totalLines" => Ok(Some(FieldValue::Json(Json::Int(self.lang.total_lines)))),
            _ => Ok(None),
        }
    }
}

/// `GitRefResolver` (`git_ref.go:72-133`).
pub(crate) struct GitRefObject {
    repo: MinimalRepo,
    name: String,
    git: Option<Arc<crate::git_graphql::GitPrefetch>>,
}

impl GitRefObject {
    pub(crate) fn new(
        repo: MinimalRepo,
        name: String,
        git: Option<Arc<crate::git_graphql::GitPrefetch>>,
    ) -> Self {
        Self { repo, name, git }
    }

    fn abbrev_name(&self) -> &str {
        let prefix = git_ref_prefix(&self.name);
        self.name.strip_prefix(prefix).unwrap_or(&self.name)
    }

    fn url(&self) -> String {
        go_url_path(&format!("/{}@{}", self.repo.name, self.abbrev_name()))
    }
}

impl ObjectValue for GitRefObject {
    fn type_name(&self) -> &'static str {
        "GitRef"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "id" => Ok(Some(FieldValue::Json(Json::Str(git_ref_relay_id(
                &self.repo, &self.name,
            ))))),
            "name" => Ok(Some(FieldValue::Json(Json::Str(self.name.clone())))),
            "abbrevName" => Ok(Some(FieldValue::Json(Json::Str(
                self.abbrev_name().to_owned(),
            )))),
            "displayName" => Ok(Some(FieldValue::Json(Json::Str(git_ref_display_name(
                &self.name,
            ))))),
            "prefix" => Ok(Some(FieldValue::Json(Json::Str(
                git_ref_prefix(&self.name).to_owned(),
            )))),
            "type" => Ok(Some(FieldValue::Json(Json::Str(
                git_ref_type(&self.name).to_owned(),
            )))),
            // `GitRefResolver.Target` (`git_ref.go:100-127`) loads the object
            // through gitserver; the prefetch phase has already done so.
            "target" => match &self.git {
                Some(git) => match git.objects.get(&(self.repo.id, self.name.clone())) {
                    Some(crate::git_graphql::ObjectLookup::Found(object)) => {
                        Ok(Some(FieldValue::Object(Arc::new(
                            crate::git_graphql::GitObjectResolverObject::from_object(
                                self.repo.clone(),
                                object.clone(),
                                Some(git.clone()),
                            ),
                        ))))
                    }
                    Some(crate::git_graphql::ObjectLookup::Error(message)) => {
                        Ok(Some(FieldValue::Object(Arc::new(
                            crate::git_graphql::GitObjectResolverObject::with_error(
                                self.repo.clone(),
                                message.clone(),
                                Some(git.clone()),
                            ),
                        ))))
                    }
                    None => Err(ExecError::new(
                        "GitRef.target was not prefetched for this ref revision",
                    )),
                },
                None => Err(ExecError::new(
                    "GitRef.target requires the gitserver GitSource (not wired for this request)",
                )),
            },
            "repository" => Ok(Some(FieldValue::Object(Arc::new(repository_object(
                &self.repo,
            ))))),
            "url" => Ok(Some(FieldValue::Json(Json::Str(self.url())))),
            _ => Ok(None),
        }
    }
}

fn git_ref_list(
    repo: &MinimalRepo,
    refs: &[String],
    git: Option<Arc<crate::git_graphql::GitPrefetch>>,
) -> FieldValue {
    FieldValue::List {
        items: refs
            .iter()
            .map(|name| {
                FieldValue::Object(Arc::new(GitRefObject {
                    repo: repo.clone(),
                    name: name.clone(),
                    git: git.clone(),
                }))
            })
            .collect(),
        element_type: Some("GitRef"),
    }
}

fn git_ref_prefix(name: &str) -> &'static str {
    if name.starts_with("refs/heads/") {
        "refs/heads/"
    } else if name.starts_with("refs/tags/") {
        "refs/tags/"
    } else if name.starts_with("refs/pull/") {
        "refs/pull/"
    } else if name.starts_with("refs/") {
        "refs/"
    } else {
        ""
    }
}

fn git_ref_type(name: &str) -> &'static str {
    if name.starts_with("refs/heads/") {
        "GIT_BRANCH"
    } else if name.starts_with("refs/tags/") {
        "GIT_TAG"
    } else {
        "GIT_REF_OTHER"
    }
}

fn git_ref_display_name(name: &str) -> String {
    let prefix = git_ref_prefix(name);
    if prefix == "refs/pull/" && (name.ends_with("/head") || name.ends_with("/merge")) {
        let rest = &name[prefix.len()..];
        if let Some(slash) = rest.find('/') {
            if let Ok(number) = rest[..slash].parse::<i64>() {
                return format!("#{number}");
            }
        }
    }
    name.strip_prefix(prefix).unwrap_or(name).to_owned()
}

fn git_ref_relay_id(repo: &MinimalRepo, name: &str) -> String {
    use base64::Engine as _;
    let repo_relay = RepositoryObject::relay_id(repo.id);
    let rev = serde_json::to_string(name).unwrap_or_else(|_| "\"\"".to_owned());
    base64::engine::general_purpose::URL_SAFE
        .encode(format!("GitRef:{{\"r\":\"{repo_relay}\",\"v\":{rev}}}"))
}

/// Deterministic subset of `internal/markdown.Render` (goldmark + GFM +
/// chroma, sanitized by `htmlutil.SanitizeReader`).
///
/// Supported: paragraphs and two-space hard breaks, ATX headings with
/// goldmark-style auto IDs, unordered/ordered lists (one nesting level),
/// blockquotes, thematic breaks, fenced code (plain or `COMMIT_EDITMSG`;
/// highlighted languages fail explicitly), inline code, `*em*`/`**strong**`
/// and links with the sanitizer's `rel`/`target` policy.
pub fn markdown_html(content: &str) -> Result<String, String> {
    let lines: Vec<&str> = content.split('\n').collect();
    let mut ids = HashSet::new();
    render_markdown_blocks(&lines, &mut ids)
}

fn render_markdown_blocks(lines: &[&str], ids: &mut HashSet<String>) -> Result<String, String> {
    let mut out = String::new();
    let mut i = 0;
    while i < lines.len() {
        if lines[i].trim().is_empty() {
            i += 1;
            continue;
        }
        if let Some((html, next)) = render_fence(lines, i)? {
            out.push_str(&html);
            i = next;
            continue;
        }
        if let Some((html, next)) = render_heading(lines, i, ids)? {
            out.push_str(&html);
            i = next;
            continue;
        }
        if is_thematic_break(lines[i]) {
            out.push_str("<hr>\n");
            i += 1;
            continue;
        }
        if lines[i].trim_start().starts_with('>') {
            let (html, next) = render_blockquote(lines, i, ids)?;
            out.push_str(&html);
            i = next;
            continue;
        }
        if list_marker(lines[i]).is_some() {
            let (html, next) = render_list(lines, i)?;
            out.push_str(&html);
            i = next;
            continue;
        }
        let (html, next) = render_paragraph(lines, i)?;
        out.push_str(&html);
        i = next;
    }
    Ok(out)
}

fn render_paragraph(lines: &[&str], start: usize) -> Result<(String, usize), String> {
    let mut end = start;
    while end < lines.len() && !lines[end].trim().is_empty() && !starts_markdown_block(lines[end]) {
        end += 1;
    }
    let paragraph = &lines[start..end];
    let mut out = String::from("<p>");
    for (n, line) in paragraph.iter().enumerate() {
        if n > 0 {
            out.push('\n');
        }
        if n + 1 < paragraph.len() && line.ends_with("  ") {
            out.push_str(&render_inline(line.trim_end())?);
            out.push_str("<br>");
        } else {
            out.push_str(&render_inline(line)?);
        }
    }
    out.push_str("</p>\n");
    Ok((out, end))
}

fn render_heading(
    lines: &[&str],
    i: usize,
    ids: &mut HashSet<String>,
) -> Result<Option<(String, usize)>, String> {
    let trimmed = lines[i].trim_start();
    let Some(hashes) = heading_start(trimmed) else {
        return Ok(None);
    };
    let mut text = trimmed[hashes..].trim();
    let without_closing = text.trim_end_matches('#');
    if without_closing.len() != text.len()
        && (without_closing.is_empty() || without_closing.ends_with(' '))
    {
        text = without_closing.trim_end();
    }
    let id = heading_id(text, ids);
    let inner = render_inline(text)?;
    Ok(Some((
        format!(
            "<h{hashes} id=\"{id}\"><a href=\"#{id}\" class=\"anchor\" rel=\"nofollow\" aria-hidden=\"true\" name=\"{id}\"></a>{inner}</h{hashes}>\n"
        ),
        i + 1,
    )))
}

fn heading_start(trimmed: &str) -> Option<usize> {
    let hashes = trimmed.chars().take_while(|ch| *ch == '#').count();
    if hashes == 0 || hashes > 6 {
        return None;
    }
    let rest = &trimmed[hashes..];
    if rest.is_empty() || rest.starts_with(' ') {
        Some(hashes)
    } else {
        None
    }
}

fn heading_id(text: &str, ids: &mut HashSet<String>) -> String {
    let mut base = String::new();
    for ch in text.chars() {
        if ch.is_ascii_alphanumeric() {
            base.push(ch.to_ascii_lowercase());
        } else if ch.is_whitespace() || ch == '-' || ch == '_' {
            base.push('-');
        }
    }
    if base.is_empty() {
        base.push_str("heading");
    }
    let mut candidate = base.clone();
    let mut n = 1;
    while !ids.insert(candidate.clone()) {
        candidate = format!("{base}-{n}");
        n += 1;
    }
    candidate
}

fn render_fence(lines: &[&str], start: usize) -> Result<Option<(String, usize)>, String> {
    let trimmed = lines[start].trim_start();
    let ticks = trimmed.chars().take_while(|ch| *ch == '`').count();
    if ticks < 3 {
        return Ok(None);
    }
    let info = trimmed[ticks..].trim();
    let language = info.split_whitespace().next().unwrap_or("");

    let mut content = String::new();
    let mut i = start + 1;
    let mut closed = false;
    while i < lines.len() {
        let line = lines[i].trim_start();
        let line_ticks = line.chars().take_while(|ch| *ch == '`').count();
        if line_ticks >= ticks && line[line_ticks..].trim().is_empty() {
            closed = true;
            i += 1;
            break;
        }
        content.push_str(lines[i]);
        content.push('\n');
        i += 1;
    }
    if !closed && !content.is_empty() {
        // EOF without a trailing newline: goldmark keeps the last line as-is.
        content.pop();
    }
    let escaped = escape_html(&content);
    if language.is_empty() || language == "COMMIT_EDITMSG" {
        return Ok(Some((format!("<pre><code>{escaped}</code></pre>\n"), i)));
    }
    if is_highlighted_language(language) {
        return Err(format!(
            "markdown: chroma-highlighted code fence `{language}` is not ported"
        ));
    }
    Ok(Some((
        format!(
            "<pre><code class=\"language-{}\">{escaped}</code></pre>\n",
            escape_html(language)
        ),
        i,
    )))
}

/// Languages the baseline renders through chroma (and therefore cannot match
/// byte-for-byte without a port of the highlighter).
fn is_highlighted_language(language: &str) -> bool {
    matches!(
        language.to_ascii_lowercase().as_str(),
        "diff"
            | "go"
            | "golang"
            | "text"
            | "plaintext"
            | "rust"
            | "python"
            | "javascript"
            | "js"
            | "typescript"
            | "ts"
            | "java"
            | "c"
            | "cpp"
            | "shell"
            | "bash"
            | "sh"
            | "json"
            | "yaml"
            | "yml"
            | "markdown"
            | "html"
            | "css"
            | "sql"
            | "toml"
            | "ruby"
            | "php"
            | "csharp"
    )
}

fn render_blockquote(
    lines: &[&str],
    start: usize,
    ids: &mut HashSet<String>,
) -> Result<(String, usize), String> {
    let mut inner = Vec::new();
    let mut i = start;
    while i < lines.len() {
        let trimmed = lines[i].trim_start();
        let Some(rest) = trimmed.strip_prefix('>') else {
            break;
        };
        inner.push(rest.strip_prefix(' ').unwrap_or(rest));
        i += 1;
    }
    let rendered = render_markdown_blocks(&inner, ids)?;
    Ok((format!("<blockquote>\n{rendered}</blockquote>\n"), i))
}

/// `(indent, ordered, content)` of a list item line.
fn list_marker(line: &str) -> Option<(usize, bool, String)> {
    let indent = line.len() - line.trim_start_matches(' ').len();
    let trimmed = &line[indent..];
    for marker in ["- ", "* ", "+ "] {
        if let Some(rest) = trimmed.strip_prefix(marker) {
            return Some((indent, false, rest.to_owned()));
        }
    }
    let digits = trimmed.chars().take_while(|ch| ch.is_ascii_digit()).count();
    if digits > 0 {
        let rest = &trimmed[digits..];
        if rest.starts_with(". ") || rest.starts_with(") ") {
            return Some((indent, true, rest[2..].to_owned()));
        }
    }
    None
}

fn render_list(lines: &[&str], start: usize) -> Result<(String, usize), String> {
    let (indent, ordered, _) = list_marker(lines[start]).expect("list marker");
    let tag = if ordered { "ol" } else { "ul" };
    let mut out = format!("<{tag}>\n");
    let mut i = start;
    while i < lines.len() {
        let Some((item_indent, item_ordered, content)) = list_marker(lines[i]) else {
            break;
        };
        if item_indent != indent || item_ordered != ordered {
            break;
        }
        let mut item = render_inline(&content)?;
        let mut next = i + 1;
        if next < lines.len() {
            if let Some((nested_indent, _, _)) = list_marker(lines[next]) {
                if nested_indent > indent {
                    let (nested, after) = render_list(lines, next)?;
                    item.push('\n');
                    item.push_str(&nested);
                    next = after;
                }
            }
        }
        out.push_str(&format!("<li>{item}</li>\n"));
        i = next;
    }
    out.push_str(&format!("</{tag}>\n"));
    Ok((out, i))
}

fn starts_markdown_block(line: &str) -> bool {
    let trimmed = line.trim_start();
    trimmed.starts_with("```")
        || is_thematic_break(line)
        || trimmed.starts_with('>')
        || list_marker(line).is_some()
        || heading_start(trimmed).is_some()
}

fn is_thematic_break(line: &str) -> bool {
    let mut marker: Option<char> = None;
    let mut count = 0;
    for ch in line.chars() {
        if ch == ' ' || ch == '\t' {
            continue;
        }
        if !matches!(ch, '-' | '*' | '_') {
            return false;
        }
        match marker {
            None => marker = Some(ch),
            Some(previous) if previous == ch => {}
            Some(_) => return false,
        }
        count += 1;
    }
    count >= 3
}

fn render_inline(text: &str) -> Result<String, String> {
    let bytes = text.as_bytes();
    let mut out = String::new();
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'\\' => {
                if i + 1 < bytes.len() {
                    let ch = text[i + 1..].chars().next().expect("char");
                    out.push_str(&escape_char(ch));
                    i += 1 + ch.len_utf8();
                } else {
                    out.push('\\');
                    i += 1;
                }
            }
            b'`' => {
                let run = run_len(bytes, i, b'`');
                if let Some(end) = find_run(bytes, i + run, run, b'`') {
                    out.push_str("<code>");
                    out.push_str(&escape_html(&text[i + run..end]));
                    out.push_str("</code>");
                    i = end + run;
                } else {
                    out.push_str(&escape_html(&text[i..i + run]));
                    i += run;
                }
            }
            delimiter @ (b'*' | b'_') => {
                let run = run_len(bytes, i, delimiter);
                let width = run.min(2);
                if let Some(end) = find_run(bytes, i + width, width, delimiter) {
                    let inner = render_inline(&text[i + width..end])?;
                    let tag = if width == 2 { "strong" } else { "em" };
                    out.push_str(&format!("<{tag}>{inner}</{tag}>"));
                    i = end + width;
                } else {
                    out.push_str(&escape_html(&text[i..i + width]));
                    i += width;
                }
            }
            b'[' => match render_link(text, i)? {
                Some((html, next)) => {
                    out.push_str(&html);
                    i = next;
                }
                None => {
                    out.push('[');
                    i += 1;
                }
            },
            _ => {
                let ch = text[i..].chars().next().expect("char");
                out.push_str(&escape_char(ch));
                i += ch.len_utf8();
            }
        }
    }
    Ok(out)
}

fn run_len(bytes: &[u8], from: usize, byte: u8) -> usize {
    bytes[from..]
        .iter()
        .take_while(|value| **value == byte)
        .count()
}

fn find_run(bytes: &[u8], from: usize, width: usize, byte: u8) -> Option<usize> {
    let mut i = from;
    while i + width <= bytes.len() {
        if bytes[i..i + width].iter().all(|value| *value == byte)
            && (i == 0 || bytes[i - 1] != byte)
        {
            return Some(i);
        }
        i += 1;
    }
    None
}

fn render_link(text: &str, start: usize) -> Result<Option<(String, usize)>, String> {
    let bytes = text.as_bytes();
    let Some(label_end) = find_byte(bytes, start + 1, b']') else {
        return Ok(None);
    };
    if label_end + 1 >= bytes.len() || bytes[label_end + 1] != b'(' {
        return Ok(None);
    }
    let Some(destination_end) = find_byte(bytes, label_end + 2, b')') else {
        return Ok(None);
    };
    let (destination, title) = parse_link_target(&text[label_end + 2..destination_end]);
    let label = render_inline(&text[start + 1..label_end])?;
    let mut html = format!("<a href=\"{}\"", escape_html(destination));
    if let Some(title) = title {
        html.push_str(&format!(" title=\"{}\"", escape_html(title)));
    }
    if is_fully_qualified_url(destination) {
        html.push_str(" rel=\"nofollow noopener\" target=\"_blank\"");
    } else {
        html.push_str(" rel=\"nofollow\"");
    }
    html.push('>');
    html.push_str(&label);
    html.push_str("</a>");
    Ok(Some((html, destination_end + 1)))
}

fn find_byte(bytes: &[u8], from: usize, byte: u8) -> Option<usize> {
    bytes
        .iter()
        .enumerate()
        .skip(from)
        .find(|(_, value)| **value == byte)
        .map(|(index, _)| index)
}

fn parse_link_target(inner: &str) -> (&str, Option<&str>) {
    let inner = inner.trim();
    let mut parts = inner.splitn(2, char::is_whitespace);
    let destination = parts.next().unwrap_or("");
    if let Some(rest) = parts.next() {
        let rest = rest.trim();
        if rest.len() >= 2 && rest.starts_with('"') && rest.ends_with('"') {
            return (destination, Some(&rest[1..rest.len() - 1]));
        }
    }
    (inner, None)
}

/// `htmlutil` adds `target="_blank"` plus `noopener` to fully-qualified links.
fn is_fully_qualified_url(destination: &str) -> bool {
    destination.starts_with("http://") || destination.starts_with("https://")
}

fn escape_html(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    for ch in text.chars() {
        out.push_str(&escape_char(ch));
    }
    out
}

fn escape_char(ch: char) -> String {
    match ch {
        '&' => "&amp;".to_owned(),
        '<' => "&lt;".to_owned(),
        '>' => "&gt;".to_owned(),
        '"' => "&#34;".to_owned(),
        '\'' => "&#39;".to_owned(),
        _ => ch.to_string(),
    }
}
