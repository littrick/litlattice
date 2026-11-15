#![forbid(unsafe_code)]

//! `cmd/frontend/internal/codycontext`: the production `CodyContextClient`
//! (content filters, embeddings/keyword partitioning, result merging) plus the
//! two content-filter implementations.
//!
//! Baseline: `dev/origin/repo` @ `c864f15af`.
//!
//! ```text
//! GetCodyContext
//!   isEnabled                       (.cody/ignore flag / enterprise config)
//!   contentFilter.getMatcher        (dotcom vs enterprise, per M9.9b seams)
//!   partitionRepos                  (RepoEmbeddingExists)
//!   embeddings : keyword ratio      (float32 arithmetic, int32 truncation)
//!   getEmbeddingsContext            (embeddings.Client -> Cody Gateway)
//!   getKeywordContext               (search.Client.Plan + Execute)
//!   merge by cartesian product      ("<name> (embeddings)")
//! ```
//!
//! All external collaborators are injected:
//!
//! * [`CodyGitserver`] — `GetDefaultBranch` / `NewFileReader`;
//! * [`EmbeddingsSearch`](super::embeddings::EmbeddingsSearch) — the gateway
//!   client, backed by an injectable HTTP transport;
//! * [`RepoEmbeddingStore`] — `db.Repos().RepoEmbeddingExists`;
//! * [`KeywordContextSearcher`] — `searchClient.Plan` + `Execute`.

use std::collections::BTreeSet;
use std::sync::Arc;

use async_trait::async_trait;
use regex::Regex;
use sg_search_query::node::{go_quote, regexp_quote_meta};

use super::embeddings::{EmbeddingsSearch, EmbeddingsSearchParameters};
use crate::codycontext::{CodyContextOptions, CodyFileMatcher, RepoContentFilter};
use crate::search_client::{Protocol, SearchMode};
use crate::search_stream::convert::FileMatch;
use crate::search_stream::SearchInputs;

/// `types.RepoIDName`.
#[derive(Debug, Clone, Default, PartialEq, Eq, PartialOrd, Ord)]
pub struct RepoIdName {
    pub id: i32,
    pub name: String,
}

/// `gitserver.Client` subset used by `dotcomRepoFilter`.
#[async_trait]
pub trait CodyGitserver: Send + Sync {
    /// `GetDefaultBranch(ctx, repo, true)` → `(branch, commit)`.
    async fn default_branch(&self, repo: &str) -> Result<(String, String), String>;

    /// `NewFileReader(ctx, repo, commit, ".cody/ignore")`.
    ///
    /// `Ok(None)` is `os.IsNotExist` (ignore nothing); `Err` is any other
    /// error and excludes the repo.
    async fn read_cody_ignore(&self, repo: &str, commit: &str) -> Result<Option<String>, String>;
}

/// `dotcomRepoFilter` (`dotcom.go:25-94`). `enabled` is the
/// `CodyContextIgnore` experimental flag, *not* dotcom mode: an enterprise
/// deployment never uses this filter (`newRepoContentFilter`, `filter.go:21`).
#[derive(Debug, Clone, Copy, Default)]
pub struct DotcomContentFilter {
    pub enabled: bool,
}

impl DotcomContentFilter {
    /// `dotcomRepoFilter.getMatcher` + `getFilter` + `getIgnoreMatcher`.
    ///
    /// Repos whose default branch cannot be read, whose default branch commit
    /// is empty, whose `.cody/ignore` cannot be read, or whose ignore file
    /// cannot be parsed are excluded. `.cody/ignore` absence ignores nothing.
    pub async fn get_matcher(
        &self,
        repos: &[RepoIdName],
        gitserver: &dyn CodyGitserver,
    ) -> (Vec<RepoIdName>, CodyFileMatcher) {
        if !self.enabled {
            return (repos.to_vec(), CodyFileMatcher::AcceptAll);
        }

        let mut filter = RepoContentFilter::new();
        let mut filterable_repos = Vec::with_capacity(repos.len());

        for repo in repos {
            let commit = match gitserver.default_branch(&repo.name).await {
                Ok((_branch, commit)) => commit,
                Err(error) => {
                    tracing::warn!(
                        repo = repo.id,
                        %error,
                        "couldn't get default branch, removing repo"
                    );
                    continue;
                }
            };
            // No commit signals an empty repo, should be nothing to filter.
            // Also we can't lookup the ignore file without a commit.
            if commit.is_empty() {
                tracing::info!(repo = repo.id, "empty repo removing");
                continue;
            }

            let contents = match gitserver.read_cody_ignore(&repo.name, &commit).await {
                Ok(contents) => contents,
                Err(error) => {
                    tracing::warn!(repo = repo.id, %error, "unable to process ignore file");
                    continue;
                }
            };

            match filter.insert(repo.id, contents.as_deref()) {
                Ok(()) => filterable_repos.push(repo.clone()),
                Err(error) => {
                    tracing::warn!(repo = repo.id, %error, "unable to process ignore file");
                }
            }
        }

        (filterable_repos, filter.into_file_matcher())
    }
}

/// `enterpriseRepoFilter.getMatcher`: the configuration is invalid (no matcher
/// exists, so everything is filtered out) and the exact baseline error text.
pub const INVALID_CONFIG_ERROR: &str =
    "Cody context filters configuration is invalid. Please contact your admin.";

/// `enterpriseRepoFilter` (`enterprise.go:43-102`) after `parseCodyContextFilters`.
///
/// The 128-entry LRU inside `filtersConfig` is only a cache of a pure function
/// of the repo name, so it is not modelled (no Redis or table is added).
#[derive(Debug, Clone, Default)]
pub struct EnterpriseContentFilter {
    include: Vec<Regex>,
    exclude: Vec<Regex>,
    valid: bool,
}

impl EnterpriseContentFilter {
    /// `parseCodyContextFilters`: compile every include pattern then every
    /// exclude pattern. Any error leaves the config invalid (all zero fields).
    pub fn parse(include: &[String], exclude: &[String]) -> Self {
        let mut items = Vec::with_capacity(include.len());
        for pattern in include {
            match Regex::new(pattern) {
                Ok(regex) => items.push(regex),
                Err(_) => return Self::default(),
            }
        }
        let mut exclusions = Vec::with_capacity(exclude.len());
        for pattern in exclude {
            match Regex::new(pattern) {
                Ok(regex) => exclusions.push(regex),
                Err(_) => return Self::default(),
            }
        }
        Self {
            include: items,
            exclude: exclusions,
            valid: true,
        }
    }

    /// Invalid configuration (mirrors the baseline return shape: empty repo
    /// list, always-false matcher, error).
    pub fn invalid() -> Self {
        Self::default()
    }

    /// `enterpriseRepoFilter.getMatcher`.
    pub fn get_matcher(
        &self,
        repos: &[RepoIdName],
    ) -> (Vec<RepoIdName>, CodyFileMatcher, Option<String>) {
        if !self.valid {
            return (
                Vec::new(),
                CodyFileMatcher::Custom(Arc::new(|_repo_id, _path| false)),
                Some(INVALID_CONFIG_ERROR.to_owned()),
            );
        }

        let included_repos: Vec<RepoIdName> = repos
            .iter()
            .filter(|repo| self.is_repo_included(repo))
            .cloned()
            .collect();
        let included_ids: BTreeSet<i32> = included_repos.iter().map(|repo| repo.id).collect();
        let matcher = CodyFileMatcher::Custom(Arc::new(move |repo_id, _path| {
            included_ids.contains(&repo_id)
        }));
        (included_repos, matcher, None)
    }

    /// `filtersConfig.isRepoIncluded`.
    fn is_repo_included(&self, repo: &RepoIdName) -> bool {
        let mut included = true;

        if !self.include.is_empty() {
            for pattern in &self.include {
                included = pattern.is_match(&repo.name);
                if included {
                    break;
                }
            }
        }

        if !self.exclude.is_empty() {
            for pattern in &self.exclude {
                if pattern.is_match(&repo.name) {
                    included = false;
                    break;
                }
            }
        }

        included
    }
}

/// `db.Repos().RepoEmbeddingExists`.
#[async_trait]
pub trait RepoEmbeddingStore: Send + Sync {
    async fn repo_embedding_exists(&self, repo_id: i32) -> Result<bool, String>;
}

/// `GetContextArgs` (`context.go:86-110`).
///
/// `RepoStats` is reduced to a presence flag: the IDF expansion
/// (`evalDictionaryExpandedQuery`) is not ported (overview §4).
#[derive(Debug, Clone, Default)]
pub struct GetContextArgs {
    pub repos: Vec<RepoIdName>,
    pub repo_stats_present: bool,
    pub file_patterns: Vec<String>,
    pub query: String,
    pub code_results_count: i32,
    pub text_results_count: i32,
}

/// `FileChunkContext` (`context.go:30-36`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileChunkContext {
    pub repo_name: String,
    pub repo_id: i32,
    pub commit_id: String,
    pub path: String,
    pub start_line: i32,
}

/// `ContextList` (`context.go:112-115`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContextList {
    pub name: String,
    pub file_chunks: Vec<FileChunkContext>,
}

/// `GetCodyContextResult` (`context.go:117-119`).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct GetCodyContextResult {
    pub context_lists: Vec<ContextList>,
}

/// The `searchClient.Plan` arguments `getKeywordContext` builds.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeywordPlanRequest {
    pub version: String,
    pub pattern_type: Option<String>,
    pub query: String,
    pub search_mode: SearchMode,
    pub protocol: Protocol,
    pub context_lines: Option<i32>,
    /// `actor.FromContext(ctx)`: the request actor controls settings,
    /// search-context resolution and sub-repo filtering. Anonymous requests
    /// keep [`crate::search_contexts::RequestContext::anonymous`] and never
    /// inherit another user's context.
    pub request_context: crate::search_contexts::RequestContext,
}

/// The subset of `searchClient.Execute` the client consumes: only
/// `*result.FileMatch` results, plus the returned alert.
#[derive(Debug, Clone, Default)]
pub struct KeywordSearchOutcome {
    pub file_matches: Vec<FileMatch>,
    /// `(title, description)` of the returned `*search.Alert`, if any.
    pub alert: Option<(String, String)>,
}

/// `client.SearchClient` subset used by `getKeywordContext`.
///
/// `plan` is the production `SearchClient::plan`; `execute` builds and runs
/// the job tree (M9.8jw/jy) so the codycontext job's `addLimitsAndFilter`
/// options take effect.
#[async_trait]
pub trait KeywordContextSearcher: Send + Sync {
    async fn plan(&self, request: &KeywordPlanRequest) -> Result<SearchInputs, String>;
    async fn execute(
        &self,
        inputs: &SearchInputs,
        request_context: crate::search_contexts::RequestContext,
    ) -> Result<KeywordSearchOutcome, String>;
}

/// The ported `CodyContextClient`.
pub struct CodyContextClient {
    /// `dotcom.SourcegraphDotComMode()` (`newRepoContentFilter`).
    pub dotcom: bool,
    /// `conf.ExperimentalFeatures.CodyContextIgnore` (`isEnabled`).
    pub dotcom_ignore_enabled: bool,
    /// `cody.IsCodyEnabled` / `CheckVerifiedEmailRequirement` seam. `Some`
    /// skips every collaborator and returns
    /// `cody is not enabled: <reason>`.
    pub cody_disabled_reason: Option<String>,
    pub gitserver: Arc<dyn CodyGitserver>,
    pub enterprise: EnterpriseContentFilter,
    /// `conf.EmbeddingsEnabled()`.
    pub embeddings_enabled: bool,
    /// `embeddings.NewDefaultClient()`. `None` reproduces the baseline
    /// `"an embeddings service has not been configured"` error.
    pub embeddings: Option<Arc<dyn EmbeddingsSearch>>,
    pub repo_embeddings: Arc<dyn RepoEmbeddingStore>,
    pub keyword: Arc<dyn KeywordContextSearcher>,
}

impl CodyContextClient {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        dotcom: bool,
        dotcom_ignore_enabled: bool,
        cody_disabled_reason: Option<String>,
        gitserver: Arc<dyn CodyGitserver>,
        enterprise: EnterpriseContentFilter,
        embeddings_enabled: bool,
        embeddings: Option<Arc<dyn EmbeddingsSearch>>,
        repo_embeddings: Arc<dyn RepoEmbeddingStore>,
        keyword: Arc<dyn KeywordContextSearcher>,
    ) -> Self {
        Self {
            dotcom,
            dotcom_ignore_enabled,
            cody_disabled_reason,
            gitserver,
            enterprise,
            embeddings_enabled,
            embeddings,
            repo_embeddings,
            keyword,
        }
    }

    /// `newRepoContentFilter` dispatch, returning the baseline triple
    /// `(repos, matcher, error)`.
    pub async fn get_matcher(
        &self,
        repos: &[RepoIdName],
    ) -> (Vec<RepoIdName>, CodyFileMatcher, Option<String>) {
        if self.dotcom {
            let filter = DotcomContentFilter {
                enabled: self.dotcom_ignore_enabled,
            };
            let (repos, matcher) = filter.get_matcher(repos, self.gitserver.as_ref()).await;
            (repos, matcher, None)
        } else {
            self.enterprise.get_matcher(repos)
        }
    }

    /// `partitionRepos` (`context.go:207-226`).
    pub async fn partition_repos(
        &self,
        input: &[RepoIdName],
    ) -> Result<(Vec<RepoIdName>, Vec<RepoIdName>), String> {
        if !self.embeddings_enabled {
            return Ok((Vec::new(), input.to_vec()));
        }

        let mut embedded = Vec::new();
        let mut not_embedded = Vec::new();
        for repo in input {
            if self.repo_embeddings.repo_embedding_exists(repo.id).await? {
                embedded.push(repo.clone());
            } else {
                not_embedded.push(repo.clone());
            }
        }
        Ok((embedded, not_embedded))
    }

    /// `getEmbeddingsContext` (`context.go:228-283`).
    pub async fn get_embeddings_context(
        &self,
        args: &GetContextArgs,
        matcher: &CodyFileMatcher,
    ) -> Result<Vec<ContextList>, String> {
        if args.repos.is_empty() || (args.code_results_count == 0 && args.text_results_count == 0) {
            // Don't bother doing an API request if we can't actually have any
            // results.
            return Ok(Vec::new());
        }

        let client = self
            .embeddings
            .as_ref()
            .ok_or_else(|| "an embeddings service has not been configured".to_owned())?;

        let parameters = EmbeddingsSearchParameters {
            repo_names: args.repos.iter().map(|repo| repo.name.clone()).collect(),
            repo_ids: args.repos.iter().map(|repo| repo.id).collect(),
            query: args.query.clone(),
            code_results_count: args.code_results_count,
            text_results_count: args.text_results_count,
            use_document_ranks: false,
        };
        let results = client.search(&parameters).await?;

        let ids_by_name: std::collections::BTreeMap<&str, i32> = args
            .repos
            .iter()
            .map(|repo| (repo.name.as_str(), repo.id))
            .collect();

        let filtered: Vec<FileChunkContext> = results
            .code_results
            .iter()
            .chain(results.text_results.iter())
            .map(|result| FileChunkContext {
                repo_name: result.repo_name.clone(),
                // `idsByName[repoName]`: an unknown name maps to the zero ID.
                repo_id: ids_by_name
                    .get(result.repo_name.as_str())
                    .copied()
                    .unwrap_or(0),
                commit_id: result.revision.clone(),
                path: result.file_name.clone(),
                start_line: result.start_line,
            })
            // `if !matcher(chunk.RepoID, chunk.Path)` keeps the chunk.
            .filter(|chunk| !matcher.accepts(chunk.repo_id, &chunk.path))
            .collect();

        Ok(vec![ContextList {
            name: "embeddings".to_owned(),
            file_chunks: filtered,
        }])
    }

    /// `getKeywordContext` (`context.go:304-395`).
    pub async fn get_keyword_context(
        &self,
        args: &GetContextArgs,
        matcher: &CodyFileMatcher,
        request_context: crate::search_contexts::RequestContext,
    ) -> Result<Vec<ContextList>, String> {
        if args.repos.is_empty() {
            // The search query `repo:^$` returns all repos, not zero repos.
            return Ok(Vec::new());
        }

        let mut context_lists = Vec::new();
        for variant in get_query_variants(args) {
            let zoekt_query = to_zoekt_query(&args.repos, &args.file_patterns, &variant.query);
            let request = KeywordPlanRequest {
                version: "V3".to_owned(),
                pattern_type: Some("codycontext".to_owned()),
                query: zoekt_query,
                search_mode: SearchMode::Precise,
                protocol: Protocol::Streaming,
                context_lines: Some(0),
                request_context,
            };
            let mut plan = self.keyword.plan(&request).await?;
            add_limits_and_filter(
                &mut plan,
                args.code_results_count,
                args.text_results_count,
                matcher,
            );
            let outcome = self.keyword.execute(&plan, request_context).await?;
            if let Some((title, description)) = &outcome.alert {
                tracing::warn!(
                    %title,
                    %description,
                    "received alert from keyword search execution"
                );
            }
            context_lists.push(ContextList {
                name: variant.name,
                file_chunks: outcome
                    .file_matches
                    .iter()
                    .map(file_match_to_context_match)
                    .collect(),
            });
        }
        Ok(context_lists)
    }

    /// `GetCodyContext` (`context.go:121-204`).
    pub async fn get_cody_context(
        &self,
        args: &GetContextArgs,
        request_context: crate::search_contexts::RequestContext,
    ) -> Result<GetCodyContextResult, String> {
        if let Some(reason) = &self.cody_disabled_reason {
            return Err(format!("cody is not enabled: {reason}"));
        }

        // Generating the content filter removes any repos where the filter can
        // not be determined.
        let (filterable_repos, context_filter, error) = self.get_matcher(&args.repos).await;
        if let Some(error) = error {
            return Err(error);
        }

        let (embedding_repos, keyword_repos) = self.partition_repos(&filterable_repos).await?;

        // NOTE: same simple heuristic as the baseline: the ratio of repos with
        // embeddings decides how much of the limit is reserved for embeddings.
        let embeddings_result_ratio =
            embeddings_ratio(embedding_repos.len(), filterable_repos.len());

        let embeddings_args = GetContextArgs {
            repos: embedding_repos,
            repo_stats_present: args.repo_stats_present,
            // Not supported for embeddings context.
            file_patterns: Vec::new(),
            query: args.query.clone(),
            code_results_count: go_float32_to_i32(
                args.code_results_count as f32 * embeddings_result_ratio,
            ),
            text_results_count: go_float32_to_i32(
                args.text_results_count as f32 * embeddings_result_ratio,
            ),
        };
        let keyword_args = GetContextArgs {
            repos: keyword_repos,
            repo_stats_present: args.repo_stats_present,
            query: args.query.clone(),
            file_patterns: args.file_patterns.clone(),
            // Assign the remaining result budget to keyword search.
            code_results_count: args
                .code_results_count
                .wrapping_sub(embeddings_args.code_results_count),
            text_results_count: args
                .text_results_count
                .wrapping_sub(embeddings_args.text_results_count),
        };

        // Fetch keyword results and embeddings results concurrently
        // (`conc/pool.WithErrors`).
        let (embeddings_result, keyword_result) = tokio::join!(
            self.get_embeddings_context(&embeddings_args, &context_filter),
            self.get_keyword_context(&keyword_args, &context_filter, request_context),
        );
        match (embeddings_result, keyword_result) {
            (Ok(embeddings), Ok(keyword)) => Ok(merge_context_lists(embeddings, keyword)),
            (Err(embeddings), Err(keyword)) => {
                // `multierror.Join` concatenates the errors.
                Err(format!("{embeddings}\n{keyword}"))
            }
            (Err(error), _) | (_, Err(error)) => Err(error),
        }
    }
}

/// `len(embedded)/len(filterable)` in `float32`. `0/0` is NaN (see
/// [`go_float32_to_i32`]).
fn embeddings_ratio(embedded: usize, filterable: usize) -> f32 {
    embedded as f32 / filterable as f32
}

/// `int32(float32_value)` on amd64: truncates toward zero; NaN, infinities and
/// out-of-range values become `-2147483648` (`CVTTSS2SI`). Rust's `as i32`
/// saturates instead, so this is spelled out.
pub fn go_float32_to_i32(value: f32) -> i32 {
    // 2^31 is exactly representable as f32; `-2^31` is in range.
    if value.is_nan() || value >= 2147483648.0_f32 || value <= -2147483649.0_f32 {
        i32::MIN
    } else {
        value as i32
    }
}

/// `addLimitsAndFilter` (`context.go:407-415`): the M9.8kb seam writes the
/// three cody fields onto `Inputs.Features` after `Plan`.
///
/// A nil `Features` is created; an `AcceptAll` matcher is still written as a
/// concrete always-true `Custom` matcher, matching the baseline's non-nil
/// `fileMatcher`.
pub fn add_limits_and_filter(
    plan: &mut SearchInputs,
    code_count: i32,
    text_count: i32,
    matcher: &CodyFileMatcher,
) {
    let file_matcher: Arc<crate::codycontext::FileMatcherFn> = match matcher {
        CodyFileMatcher::AcceptAll => Arc::new(|_repo_id, _path| true),
        CodyFileMatcher::Custom(matcher) => matcher.clone(),
    };
    plan.cody_context = Some(CodyContextOptions {
        code_count,
        text_count,
        file_matcher: Some(file_matcher),
    });
}

/// `fileMatchToContextMatch` (`context.go:417-437`).
pub fn file_match_to_context_match(file_match: &FileMatch) -> FileChunkContext {
    let start_line = if let Some(symbol) = file_match.symbols.first() {
        // 5 lines of leading context, clamped to zero.
        symbol.symbol.line.saturating_sub(5).max(0)
    } else if let Some(chunk) = file_match.chunk_matches.first() {
        // To provide some context variety, we just use the top-ranked chunk
        // (the first chunk) from each file match. `StartLine` is `int` in Go;
        // the Rust field is `i32`, so clamp at the type boundary.
        (chunk.content_start.line - 5).clamp(0, i64::from(i32::MAX)) as i32
    } else {
        // Filename-only match: a single chunk at the start of the file.
        0
    };

    FileChunkContext {
        repo_name: file_match.file.repo.name.clone(),
        repo_id: file_match.file.repo.id,
        commit_id: file_match.file.commit_id.clone(),
        path: file_match.file.path.clone(),
        start_line,
    }
}

/// `getQueryVariants` (`context.go:439-465`).
///
/// The IDF `expandedKeyword_5` variant is not ported (overview §4); with no
/// `RepoStats` the baseline produces exactly the single `keyword(%q)` variant.
fn get_query_variants(args: &GetContextArgs) -> Vec<QueryVariant> {
    vec![QueryVariant {
        name: format!("keyword({})", go_quote(&args.query)),
        query: args.query.clone(),
    }]
}

struct QueryVariant {
    name: String,
    query: String,
}

/// `toZoektQuery` (`context.go:313-321`).
fn to_zoekt_query(repos: &[RepoIdName], file_patterns: &[String], base_query: &str) -> String {
    let file_patterns = file_patterns.join("|");
    format!(
        "repo:{} file:(?:{}) {} {}",
        repos_as_regexp(repos),
        file_patterns,
        get_keyword_context_exclude_file_paths_query(),
        base_query,
    )
}

/// `getKeywordContextExcludeFilePathsQuery` (`context.go:285-301`).
fn get_keyword_context_exclude_file_paths_query() -> String {
    const EXCLUDE_FILE_PATHS: &[&str] = &[
        r"\.min\.js$",
        r"\.map$",
        r"\.tsbuildinfo$",
        r"(\/|^)umd\/",
        r"(\/|^)amd\/",
        r"(\/|^)cjs\/",
    ];
    EXCLUDE_FILE_PATHS
        .iter()
        .map(|path| format!("-file:{path}"))
        .collect::<Vec<_>>()
        .join(" ")
}

/// `reposAsRegexp` (`context.go:397-405`) + `query.UnionRegExps`.
fn repos_as_regexp(repos: &[RepoIdName]) -> String {
    let values: Vec<String> = repos
        .iter()
        .map(|repo| format!("^{}$", regexp_quote_meta(&repo.name)))
        .collect();
    union_reg_exps(&values)
}

/// `query.UnionRegExps` (`internal/search/query/helpers.go:15-30`).
fn union_reg_exps(values: &[String]) -> String {
    if values.is_empty() {
        // "" means "no regexp" (no values) for the internal search engine.
        String::new()
    } else if values.len() == 1 {
        values[0].clone()
    } else {
        format!("(?:{})", values.join(")|(?:"))
    }
}

/// `GetCodyContext`'s result combination (`context.go:189-203`).
fn merge_context_lists(
    embeddings_results: Vec<ContextList>,
    keyword_results: Vec<ContextList>,
) -> GetCodyContextResult {
    if embeddings_results.is_empty() {
        return GetCodyContextResult {
            context_lists: keyword_results,
        };
    }
    if keyword_results.is_empty() {
        return GetCodyContextResult {
            context_lists: embeddings_results,
        };
    }

    let mut merged_context_lists =
        Vec::with_capacity(embeddings_results.len() * keyword_results.len());
    for embeddings_result in &embeddings_results {
        for keyword_result in &keyword_results {
            let mut file_chunks = embeddings_result.file_chunks.clone();
            file_chunks.extend(keyword_result.file_chunks.iter().cloned());
            merged_context_lists.push(ContextList {
                name: format!("{} (embeddings)", embeddings_result.name),
                file_chunks,
            });
        }
    }
    GetCodyContextResult {
        context_lists: merged_context_lists,
    }
}
