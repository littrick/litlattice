#![forbid(unsafe_code)]

//! The M9.48d embeddings GraphQL surface:
//! `cmd/frontend/graphqlbackend/embeddings.graphql` plus
//! `cmd/frontend/internal/embeddings/resolvers`.
//!
//! All state comes from the existing `repo_embedding_jobs` /
//! `repo_embedding_job_stats` tables (`sg-db::repo_embeddings`) and the
//! existing gitserver file seam; no table, column or Redis key is added.
//!
//! Async work is prefetched before execution (the executor's business
//! resolvers are synchronous): [`prefetch_embeddings_graphql`] fills
//! [`EmbeddingsGraphqlPrefetch`], and the object resolvers replay it.
//!
//! Degradation follows the Go resolver exactly:
//!
//! * `conf.EmbeddingsEnabled()` false → `embeddings are not configured or disabled`;
//! * `cody.IsCodyEnabled` false → `cody is not enabled: <reason>`;
//! * required verified email → `cody requires a verified email address`;
//! * no embeddings service injected → `an embeddings service has not been
//!   configured` (`internal/embeddings/client.go`'s `Endpoints == nil`);
//! * `repoEmbeddingJobs`/`embeddingJobs` are site-admin only.

use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use base64::Engine;
use sg_db::repo_embeddings::{
    RepoEmbeddingJobRow, RepoEmbeddingJobsFilter, RepoEmbeddingJobsPage, RepoEmbeddingStatsRow,
};
use sg_graphql::exec::{Actor, Arguments, ExecError, FieldValue, InputValue, ObjectValue, RootField};
use sg_graphql::json::Json;
use time::OffsetDateTime;

use crate::embeddings_graphql::{field_selected, EmbeddingsPrefetch, RepositoryEmbeddingsStore};
use crate::git_graphql::GitCommitResolverObject;
use crate::graphql_resolvers::{
    datetime_rfc3339, unmarshal_relay_id, ConnectionPageInfoObject, RepositoryData,
    RepositoryObject,
};
use crate::search_stream::convert::MinimalRepo;

/// `resolvers.go`'s first gate.
pub const EMBEDDINGS_DISABLED: &str = "embeddings are not configured or disabled";
/// `internal/embeddings/client.go`'s `Endpoints == nil` error.
pub const NO_EMBEDDINGS_SERVICE: &str = "an embeddings service has not been configured";
/// `auth.CheckCurrentUserIsSiteAdmin`'s anonymous error.
pub const NOT_AUTHENTICATED: &str = "not authenticated";
/// `auth.CheckCurrentUserIsSiteAdmin`'s non-admin error.
pub const MUST_BE_SITE_ADMIN: &str = "must be site admin";
/// `gqlutil.ConnectionResolver`'s missing pagination error.
pub const PAGINATION_REQUIRED: &str =
    "you must provide a `first` or `last` value to properly paginate";

/// The cody gates the embeddings resolvers consult (the same values the
/// production `cody.IsCodyEnabled` / `CheckVerifiedEmailRequirement` compute).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CodyGate {
    /// `cody.IsCodyEnabled`'s reason when disabled.
    pub cody_disabled_reason: Option<String>,
    /// `CheckVerifiedEmailRequirement`'s error (dotcom + unverified).
    pub verified_email_error: Option<String>,
}

impl CodyGate {
    /// `cody.IsCodyEnabled` + `CheckVerifiedEmailRequirement`.
    pub fn check(&self) -> Result<(), String> {
        if let Some(reason) = &self.cody_disabled_reason {
            return Err(format!("cody is not enabled: {reason}"));
        }
        if let Some(error) = &self.verified_email_error {
            return Err(error.clone());
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Seams
// ---------------------------------------------------------------------------

/// `repoEmbeddingJobsStore`'s GraphQL read/write subset.
#[async_trait]
pub trait EmbeddingsJobsStore: Send + Sync {
    async fn count_repo_embedding_jobs(
        &self,
        filter: &RepoEmbeddingJobsFilter<'_>,
    ) -> Result<i64, String>;
    async fn list_repo_embedding_jobs(
        &self,
        filter: &RepoEmbeddingJobsFilter<'_>,
        page: &RepoEmbeddingJobsPage,
    ) -> Result<Vec<RepoEmbeddingJobRow>, String>;
    async fn get_repo_embedding_job_stats(
        &self,
        job_id: i32,
    ) -> Result<RepoEmbeddingStatsRow, String>;
    async fn cancel_repo_embedding_job(&self, job_id: i32) -> Result<(), String>;
}

/// Production store backed by `repo_embedding_jobs` / `repo_embedding_job_stats`.
#[derive(Debug, Clone)]
pub struct PgEmbeddingsJobsStore {
    pool: sqlx::PgPool,
}

impl PgEmbeddingsJobsStore {
    pub fn new(pool: sqlx::PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl EmbeddingsJobsStore for PgEmbeddingsJobsStore {
    async fn count_repo_embedding_jobs(
        &self,
        filter: &RepoEmbeddingJobsFilter<'_>,
    ) -> Result<i64, String> {
        sg_db::repo_embeddings::count_repo_embedding_jobs(&self.pool, filter)
            .await
            .map_err(|error| error.to_string())
    }

    async fn list_repo_embedding_jobs(
        &self,
        filter: &RepoEmbeddingJobsFilter<'_>,
        page: &RepoEmbeddingJobsPage,
    ) -> Result<Vec<RepoEmbeddingJobRow>, String> {
        sg_db::repo_embeddings::list_repo_embedding_jobs(&self.pool, filter, page)
            .await
            .map_err(|error| error.to_string())
    }

    async fn get_repo_embedding_job_stats(
        &self,
        job_id: i32,
    ) -> Result<RepoEmbeddingStatsRow, String> {
        sg_db::repo_embeddings::get_repo_embedding_job_stats(&self.pool, job_id)
            .await
            .map_err(|error| error.to_string())
    }

    async fn cancel_repo_embedding_job(&self, job_id: i32) -> Result<(), String> {
        sg_db::repo_embeddings::cancel_repo_embedding_job(&self.pool, job_id).await
    }
}

/// A repository row needed by `RepoEmbeddingJob.repo`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EmbeddingsRepo {
    pub id: i32,
    pub name: String,
}

/// `db.Repos().GetByIDs` / `GetByName` subset.
#[async_trait]
pub trait EmbeddingsRepoLookup: Send + Sync {
    async fn by_id(&self, repo_id: i32) -> Result<Option<EmbeddingsRepo>, String>;
    async fn by_name(&self, name: &str) -> Result<Option<EmbeddingsRepo>, String>;
}

/// Adapter over the existing production [`crate::graphql_resolvers::
/// RepositoryLookup`] (the same `repo` table/authz predicate as Go).
#[derive(Clone)]
pub struct RepositoryLookupAdapter {
    pub store: Arc<dyn crate::graphql_resolvers::RepositoryLookup>,
}

impl RepositoryLookupAdapter {
    fn convert(lookup: crate::graphql_resolvers::RepoLookup) -> Result<Option<EmbeddingsRepo>, String> {
        match lookup {
            crate::graphql_resolvers::RepoLookup::Found(data) => Ok(Some(EmbeddingsRepo {
                id: data.id,
                name: data.name,
            })),
            crate::graphql_resolvers::RepoLookup::NotFound => Ok(None),
            crate::graphql_resolvers::RepoLookup::Blocked { name, reason } => {
                Err(format!("repo {name} is blocked: {reason}"))
            }
            crate::graphql_resolvers::RepoLookup::Redirect { url } => Err(format!(
                "repo not found at this location, but might exist at {url}"
            )),
            crate::graphql_resolvers::RepoLookup::Error(message) => Err(message),
        }
    }
}

#[async_trait]
impl EmbeddingsRepoLookup for RepositoryLookupAdapter {
    async fn by_id(&self, repo_id: i32) -> Result<Option<EmbeddingsRepo>, String> {
        Self::convert(self.store.by_id(repo_id, 0, false).await?)
    }

    async fn by_name(&self, name: &str) -> Result<Option<EmbeddingsRepo>, String> {
        Self::convert(self.store.lookup(name, 0, false).await?)
    }
}

/// `embeddings.Client.Search` arguments (`internal/embeddings/client.go`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EmbeddingsSearchParams {
    pub repo_names: Vec<String>,
    pub repo_ids: Vec<i32>,
    pub query: String,
    pub code_results_count: i32,
    pub text_results_count: i32,
}

/// One `EmbeddingSearchResult` before its file content is fetched.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EmbeddingsRawResult {
    pub repo_name: String,
    pub revision: String,
    pub file_name: String,
    pub start_line: i32,
    pub end_line: i32,
}

/// `EmbeddingCombinedSearchResults`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct EmbeddingsRawResults {
    pub code: Vec<EmbeddingsRawResult>,
    pub text: Vec<EmbeddingsRawResult>,
}

/// `embeddings.Client` seam.
#[async_trait]
pub trait EmbeddingsSearchService: Send + Sync {
    async fn search(
        &self,
        params: EmbeddingsSearchParams,
    ) -> Result<EmbeddingsRawResults, String>;
}

/// `gitserver.Client.NewFileReader` seam (`io.ReadAll`).
#[async_trait]
pub trait EmbeddingsFileReader: Send + Sync {
    async fn read_file(
        &self,
        repo_name: &str,
        revision: &str,
        file_name: &str,
    ) -> Result<Vec<u8>, String>;
}

/// `embeddings.ScheduleRepositories` seam. The production implementation
/// resolves repo names, the default branch and the duplicate-job check; `None`
/// keeps scheduling an explicit not-wired error instead of fabricating jobs.
#[async_trait]
pub trait EmbeddingsScheduleService: Send + Sync {
    async fn schedule(&self, repo_names: &[String], force: bool) -> Result<(), String>;
}

/// The injected embeddings GraphQL dependencies.
pub struct EmbeddingsDeps {
    /// `conf.EmbeddingsEnabled()`.
    pub enabled: bool,
    /// `Repository.embeddingExists` (M9.14c), unchanged.
    pub repository: Arc<dyn RepositoryEmbeddingsStore>,
    pub jobs: Arc<dyn EmbeddingsJobsStore>,
    pub repos: Option<Arc<dyn EmbeddingsRepoLookup>>,
    pub search: Option<Arc<dyn EmbeddingsSearchService>>,
    pub files: Option<Arc<dyn EmbeddingsFileReader>>,
    pub scheduler: Option<Arc<dyn EmbeddingsScheduleService>>,
    pub cody_gate: CodyGate,
}

impl std::fmt::Debug for EmbeddingsDeps {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EmbeddingsDeps")
            .field("enabled", &self.enabled)
            .field("repos", &self.repos.is_some())
            .field("search", &self.search.is_some())
            .field("files", &self.files.is_some())
            .field("scheduler", &self.scheduler.is_some())
            .field("cody_gate", &self.cody_gate)
            .finish()
    }
}

// ---------------------------------------------------------------------------
// Keys and data
// ---------------------------------------------------------------------------

/// `embeddingsSearch`/`embeddingsMultiSearch` arguments.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct SearchKey {
    pub multi: bool,
    pub repo_ids: Vec<i32>,
    pub query: String,
    pub code_results_count: i32,
    pub text_results_count: i32,
}

/// `repoEmbeddingJobs` / `Repository.embeddingJobs` arguments.
#[derive(Debug, Clone, Default, PartialEq, Eq, PartialOrd, Ord)]
pub struct EmbeddingsJobsQuery {
    pub first: Option<i32>,
    pub last: Option<i32>,
    pub after: Option<String>,
    pub before: Option<String>,
    pub query: Option<String>,
    pub state: Option<String>,
    /// `Repository.embeddingJobs` forces the repository filter.
    pub repo_id: Option<i32>,
}

impl EmbeddingsJobsQuery {
    /// Also the prefetch map key, so it does not derive separately.
    pub fn key(&self) -> EmbeddingsJobsQuery {
        self.clone()
    }
}

/// One result after `extractLineRange`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EmbeddingsSearchResultData {
    pub repo_name: String,
    pub revision: String,
    pub file_name: String,
    pub start_line: i32,
    pub end_line: i32,
    pub content: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct EmbeddingsSearchResultsData {
    pub code_results: Vec<EmbeddingsSearchResultData>,
    pub text_results: Vec<EmbeddingsSearchResultData>,
}

/// `repoEmbeddingJobStatsResolver` output.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RepoEmbeddingsStatsData {
    pub files_scheduled: i64,
    pub files_embedded: i64,
    pub files_skipped: i64,
}

/// One `RepoEmbeddingJob` resolver payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RepoEmbeddingJobData {
    pub id: i32,
    pub state: String,
    pub failure_message: Option<String>,
    pub queued_at: OffsetDateTime,
    pub started_at: Option<OffsetDateTime>,
    pub finished_at: Option<OffsetDateTime>,
    pub process_after: Option<OffsetDateTime>,
    pub num_resets: i32,
    pub num_failures: i32,
    pub last_heartbeat_at: Option<OffsetDateTime>,
    pub worker_hostname: String,
    pub cancel: bool,
    pub revision: String,
    pub repo: Result<Option<EmbeddingsRepo>, String>,
    pub stats: Result<RepoEmbeddingsStatsData, String>,
}

/// `jqlutil.ConnectionPageInfo` output.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RepoEmbeddingJobsConnectionData {
    pub nodes: Vec<RepoEmbeddingJobData>,
    pub total_count: i64,
    pub start_cursor: Option<String>,
    pub end_cursor: Option<String>,
    pub has_next_page: bool,
    pub has_previous_page: bool,
}

/// The per-request prefetch map.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct EmbeddingsGraphqlPrefetch {
    pub searches: BTreeMap<SearchKey, Result<EmbeddingsSearchResultsData, String>>,
    pub jobs: BTreeMap<EmbeddingsJobsQuery, Result<RepoEmbeddingJobsConnectionData, String>>,
    pub mutations: BTreeMap<String, Result<(), String>>,
    pub context_required: Option<Result<bool, String>>,
}

// ---------------------------------------------------------------------------
// Prefetch
// ---------------------------------------------------------------------------

/// Prefetch every selected embeddings surface field.
///
/// `repo_ids` are the repositories reached by the request's root prefetchers;
/// `Repository.embeddingJobs` is prefetched for each of them and for each
/// selected argument set.
pub async fn prefetch_embeddings_graphql(
    deps: &EmbeddingsDeps,
    enabled: bool,
    fields: &[RootField],
    repo_ids: impl IntoIterator<Item = i32>,
    actor: Actor,
    site_admin: bool,
    is_mutation: bool,
) -> EmbeddingsPrefetch {
    let repo_ids: Vec<i32> = repo_ids.into_iter().collect();
    let mut prefetch = crate::embeddings_graphql::prefetch_embeddings(
        deps.repository.as_ref(),
        fields,
        repo_ids.clone(),
        enabled,
    )
    .await;
    let mut graphql = EmbeddingsGraphqlPrefetch::default();

    if is_mutation {
        for field in fields {
            match field.name.as_str() {
                "scheduleRepositoriesForEmbedding" => {
                    let outcome =
                        schedule_repositories(deps, enabled, &field.arguments, site_admin).await;
                    graphql
                        .mutations
                        .insert(field.name.clone(), outcome);
                }
                "cancelRepoEmbeddingJob" => {
                    let outcome = cancel_repo_embedding_job(
                        deps,
                        &field.arguments,
                        site_admin,
                    )
                    .await;
                    graphql
                        .mutations
                        .insert(field.name.clone(), outcome);
                }
                _ => {}
            }
        }
    } else {
        for field in fields {
            match field.name.as_str() {
                "embeddingsSearch" | "embeddingsMultiSearch" => {
                    if let Some(key) = search_key(field) {
                        let outcome = embeddings_search(deps, enabled, &key, actor).await;
                        graphql.searches.insert(key, outcome);
                    }
                }
                "isContextRequiredForChatQuery" => {
                    graphql.context_required = Some(
                        is_context_required_for_chat_query(deps, &field.arguments, actor),
                    );
                }
                _ => {}
            }
        }
        // Root `repoEmbeddingJobs`.
        if let Some(field) = fields.iter().find(|field| field.name == "repoEmbeddingJobs") {
            let query = jobs_query(&field.arguments, None);
            let outcome = jobs_connection(deps, enabled, &query, site_admin).await;
            graphql.jobs.insert(query, outcome);
        }
        // `Repository.embeddingJobs` is reached through an object selection.
        let mut argument_sets: Vec<EmbeddingsJobsQuery> = Vec::new();
        collect_jobs_arguments(fields, &mut argument_sets);
        if !argument_sets.is_empty() {
            let mut ids = repo_ids;
            ids.sort_unstable();
            ids.dedup();
            for repo_id in ids {
                for args in &argument_sets {
                    let mut query = args.clone();
                    query.repo_id = Some(repo_id);
                    if !graphql.jobs.contains_key(&query) {
                        let outcome = jobs_connection(deps, enabled, &query, site_admin).await;
                        graphql.jobs.insert(query, outcome);
                    }
                }
            }
        }
    }

    prefetch.graphql = Some(Arc::new(graphql));
    let _ = actor;
    prefetch
}

fn collect_jobs_arguments(fields: &[RootField], out: &mut Vec<EmbeddingsJobsQuery>) {
    for field in fields {
        if field.name == "embeddingJobs" {
            let query = jobs_query(&field.arguments, None);
            if !out.contains(&query) {
                out.push(query);
            }
        }
        collect_jobs_arguments(&field.selections, out);
        for (_, selections) in &field.type_conditions {
            collect_jobs_arguments(selections, out);
        }
    }
}

fn search_key(field: &RootField) -> Option<SearchKey> {
    let repo_ids = if field.name == "embeddingsSearch" {
        vec![unmarshal_relay_id(field.arguments.string("repo")?).ok()?]
    } else {
        let mut ids = Vec::new();
        if let Some(InputValue::List(items)) = field.arguments.get("repos") {
            for item in items {
                ids.push(unmarshal_relay_id(item.as_str()?).ok()?);
            }
        }
        ids
    };
    Some(SearchKey {
        multi: field.name == "embeddingsMultiSearch",
        repo_ids,
        query: field.arguments.string("query")?.to_owned(),
        code_results_count: field
            .arguments
            .i64("codeResultsCount")
            .unwrap_or_default() as i32,
        text_results_count: field
            .arguments
            .i64("textResultsCount")
            .unwrap_or_default() as i32,
    })
}

/// The argument set of `repoEmbeddingJobs`/`Repository.embeddingJobs`.
pub fn jobs_query(arguments: &Arguments, repo_id: Option<i32>) -> EmbeddingsJobsQuery {
    EmbeddingsJobsQuery {
        first: arguments
            .i64("first")
            .and_then(|value| i32::try_from(value).ok()),
        last: arguments
            .i64("last")
            .and_then(|value| i32::try_from(value).ok()),
        after: arguments.string("after").map(str::to_owned),
        before: arguments.string("before").map(str::to_owned),
        query: arguments.string("query").map(str::to_owned),
        state: arguments.string("state").map(str::to_owned),
        repo_id,
    }
}

async fn embeddings_search(
    deps: &EmbeddingsDeps,
    enabled: bool,
    key: &SearchKey,
    actor: Actor,
) -> Result<EmbeddingsSearchResultsData, String> {
    gate(deps, enabled, actor)?;
    let repos = fetch_repos(deps, &key.repo_ids).await?;
    let search = deps.search.as_ref().ok_or_else(|| NO_EMBEDDINGS_SERVICE.to_owned())?;
    let raw = search
        .search(EmbeddingsSearchParams {
            repo_names: repos.iter().map(|repo| repo.name.clone()).collect(),
            repo_ids: repos.iter().map(|repo| repo.id).collect(),
            query: key.query.clone(),
            code_results_count: key.code_results_count,
            text_results_count: key.text_results_count,
        })
        .await?;
    Ok(EmbeddingsSearchResultsData {
        code_results: results_with_content(deps, raw.code).await,
        text_results: results_with_content(deps, raw.text).await,
    })
}

fn gate(deps: &EmbeddingsDeps, enabled: bool, actor: Actor) -> Result<(), String> {
    if !enabled {
        return Err(EMBEDDINGS_DISABLED.to_owned());
    }
    // `cody.IsCodyEnabled`: an unauthenticated actor is disabled first.
    if !actor.is_authenticated() {
        return Err("cody is not enabled: not authenticated".to_owned());
    }
    deps.cody_gate.check()
}

async fn fetch_repos(
    deps: &EmbeddingsDeps,
    repo_ids: &[i32],
) -> Result<Vec<EmbeddingsRepo>, String> {
    let lookup = deps.repos.as_ref().ok_or_else(|| {
        "repository lookup is not wired for the embeddings surface".to_owned()
    })?;
    let mut repos = Vec::with_capacity(repo_ids.len());
    for repo_id in repo_ids {
        if let Some(repo) = lookup.by_id(*repo_id).await? {
            repos.push(repo);
        }
    }
    Ok(repos)
}

async fn results_with_content(
    deps: &EmbeddingsDeps,
    raw: Vec<EmbeddingsRawResult>,
) -> Vec<EmbeddingsSearchResultData> {
    let Some(files) = deps.files.as_ref() else {
        // `gs.NewFileReader` failing skips the result (the baseline logs and
        // continues), so an unconnected reader yields no fabricated content.
        return Vec::new();
    };
    let mut out = Vec::with_capacity(raw.len());
    for result in raw {
        let Ok(contents) = files
            .read_file(&result.repo_name, &result.revision, &result.file_name)
            .await
        else {
            continue;
        };
        out.push(EmbeddingsSearchResultData {
            repo_name: result.repo_name,
            revision: result.revision,
            file_name: result.file_name,
            start_line: result.start_line,
            end_line: result.end_line,
            content: String::from_utf8_lossy(&extract_line_range(
                &contents,
                result.start_line,
                result.end_line,
            ))
            .into_owned(),
        });
    }
    out
}

/// `extractLineRange`: `bytes.Split(content, "\n")`, clamp both endpoints and
/// re-join with `"\n"`.
pub fn extract_line_range(content: &[u8], start_line: i32, end_line: i32) -> Vec<u8> {
    let lines: Vec<&[u8]> = content.split(|byte| *byte == b'\n').collect();
    let start = clamp(start_line, 0, lines.len() as i32) as usize;
    let end = clamp(end_line, 0, lines.len() as i32) as usize;
    lines[start..end].join(&b'\n')
}

fn clamp(input: i32, min: i32, max: i32) -> i32 {
    if input > max {
        max
    } else if input < min {
        min
    } else {
        input
    }
}

fn is_context_required_for_chat_query(
    deps: &EmbeddingsDeps,
    arguments: &Arguments,
    actor: Actor,
) -> Result<bool, String> {
    if !actor.is_authenticated() {
        return Err("cody is not enabled: not authenticated".to_owned());
    }
    deps.cody_gate.check()?;
    let query = arguments.string("query").unwrap_or_default();
    Ok(crate::embeddings_graphql::is_context_required_for_chat_query(query))
}

async fn schedule_repositories(
    deps: &EmbeddingsDeps,
    enabled: bool,
    arguments: &Arguments,
    site_admin: bool,
) -> Result<(), String> {
    if !enabled {
        return Err(EMBEDDINGS_DISABLED.to_owned());
    }
    check_site_admin(site_admin)?;
    let mut names = Vec::new();
    if let Some(InputValue::List(items)) = arguments.get("repoNames") {
        for item in items {
            if let Some(name) = item.as_str() {
                names.push(name.to_owned());
            }
        }
    }
    let force = arguments.bool("force").unwrap_or(false);
    let scheduler = deps
        .scheduler
        .as_ref()
        .ok_or_else(|| "scheduling repo embeddings is not wired".to_owned())?;
    scheduler.schedule(&names, force).await
}

async fn cancel_repo_embedding_job(
    deps: &EmbeddingsDeps,
    arguments: &Arguments,
    site_admin: bool,
) -> Result<(), String> {
    // Erratum: the baseline `CancelRepoEmbeddingJob` does **not** check
    // `conf.EmbeddingsEnabled()`; only the site-admin check runs.
    check_site_admin(site_admin)?;
    let job = arguments.string("job").unwrap_or_default();
    let job_id = unmarshal_relay_id(job)?;
    deps.jobs.cancel_repo_embedding_job(job_id).await
}

fn check_site_admin(site_admin: bool) -> Result<(), String> {
    if site_admin {
        Ok(())
    } else {
        Err(MUST_BE_SITE_ADMIN.to_owned())
    }
}

async fn jobs_connection(
    deps: &EmbeddingsDeps,
    enabled: bool,
    query: &EmbeddingsJobsQuery,
    site_admin: bool,
) -> Result<RepoEmbeddingJobsConnectionData, String> {
    if !enabled {
        return Err(EMBEDDINGS_DISABLED.to_owned());
    }
    check_site_admin(site_admin)?;

    let (page_size, last) = match (query.first, query.last) {
        (Some(first), _) => (first, false),
        (None, Some(last)) => (last, true),
        (None, None) => return Err(PAGINATION_REQUIRED.to_owned()),
    };
    let page_size = if page_size == 0 {
        100
    } else if page_size < 100 {
        page_size
    } else {
        100
    };
    let limit = i64::from(page_size) + 1;
    let after = query
        .after
        .as_deref()
        .map(unmarshal_relay_id)
        .transpose()?;
    let before = query
        .before
        .as_deref()
        .map(unmarshal_relay_id)
        .transpose()?;
    let filter = RepoEmbeddingJobsFilter {
        query: query.query.as_deref(),
        state: query.state.as_deref(),
        repo_id: query.repo_id,
    };
    let page = if last {
        RepoEmbeddingJobsPage::Last {
            limit,
            after,
            before,
        }
    } else {
        RepoEmbeddingJobsPage::First {
            limit,
            after,
            before,
        }
    };
    let total_count = deps.jobs.count_repo_embedding_jobs(&filter).await?;
    let mut rows = deps.jobs.list_repo_embedding_jobs(&filter, &page).await?;
    let fetched_count = rows.len();
    if last {
        rows.reverse();
    }
    if rows.len() > page_size as usize {
        if last {
            rows.remove(0);
        } else {
            rows.pop();
        }
    }

    let mut nodes = Vec::with_capacity(rows.len());
    for row in rows {
        let repo = match &deps.repos {
            Some(lookup) => lookup.by_id(row.repo_id).await,
            None => Ok(None),
        };
        let stats = deps
            .jobs
            .get_repo_embedding_job_stats(row.id)
            .await
            .map(|stats| stats_data(&stats));
        nodes.push(RepoEmbeddingJobData {
            id: row.id,
            state: upper_state(&row.state),
            failure_message: row.failure_message,
            queued_at: row.queued_at,
            started_at: row.started_at,
            finished_at: row.finished_at,
            process_after: row.process_after,
            num_resets: row.num_resets,
            num_failures: row.num_failures,
            // Erratum: the baseline uses `ProcessAfter == nil` as the
            // `LastHeartbeatAt` emptiness check (repo_embedding_jobs.go).
            last_heartbeat_at: if row.process_after.is_none() {
                None
            } else {
                row.last_heartbeat_at
            },
            worker_hostname: row.worker_hostname,
            cancel: row.cancel,
            revision: row.revision,
            repo,
            stats,
        });
    }

    let has_next_page = if query.first.is_some() {
        fetched_count > page_size as usize
    } else if fetched_count == 0 {
        false
    } else {
        query.before.is_some()
    };
    let has_previous_page = if query.last.is_some() {
        fetched_count > page_size as usize
    } else if fetched_count == 0 {
        false
    } else {
        query.after.is_some()
    };
    let start_cursor = nodes.first().map(|node| marshal_job_id(node.id));
    let end_cursor = nodes.last().map(|node| marshal_job_id(node.id));
    Ok(RepoEmbeddingJobsConnectionData {
        nodes,
        total_count,
        start_cursor,
        end_cursor,
        has_next_page,
        has_previous_page,
    })
}

/// `strings.ToUpper(job.State)`.
pub fn upper_state(state: &str) -> String {
    state.to_uppercase()
}

/// `gqlutil.DateTime.MarshalJSON`.
pub fn datetime_string(value: OffsetDateTime) -> String {
    datetime_rfc3339(value)
}

/// `repoEmbeddingJobStatsResolver`'s code+text aggregation.
pub fn stats_data(stats: &RepoEmbeddingStatsRow) -> RepoEmbeddingsStatsData {
    RepoEmbeddingsStatsData {
        files_scheduled: stats.code_files_scheduled + stats.text_files_scheduled,
        files_embedded: stats.code_files_embedded + stats.text_files_embedded,
        files_skipped: stats.code_files_skipped.values().sum::<i64>()
            + stats.text_files_skipped.values().sum::<i64>(),
    }
}

/// `relay.MarshalID("RepoEmbeddingJob", id)`.
pub fn marshal_job_id(id: i32) -> String {
    base64::engine::general_purpose::URL_SAFE.encode(format!("RepoEmbeddingJob:{id}"))
}

// ---------------------------------------------------------------------------
// Object resolvers
// ---------------------------------------------------------------------------

struct EmbeddingsSearchResultsObject {
    data: EmbeddingsSearchResultsData,
}

impl ObjectValue for EmbeddingsSearchResultsObject {
    fn type_name(&self) -> &'static str {
        "EmbeddingsSearchResults"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "codeResults" => Ok(Some(results_value(&self.data.code_results))),
            "textResults" => Ok(Some(results_value(&self.data.text_results))),
            _ => Ok(None),
        }
    }
}

fn results_value(results: &[EmbeddingsSearchResultData]) -> FieldValue {
    FieldValue::List {
        element_type: Some("EmbeddingsSearchResult"),
        items: results
            .iter()
            .cloned()
            .map(|result| {
                FieldValue::Object(Arc::new(EmbeddingsSearchResultObject { result }))
            })
            .collect(),
    }
}

struct EmbeddingsSearchResultObject {
    result: EmbeddingsSearchResultData,
}

impl ObjectValue for EmbeddingsSearchResultObject {
    fn type_name(&self) -> &'static str {
        "EmbeddingsSearchResult"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "repoName" => Ok(Some(json_str(self.result.repo_name.clone()))),
            "revision" => Ok(Some(json_str(self.result.revision.clone()))),
            "fileName" => Ok(Some(json_str(self.result.file_name.clone()))),
            "startLine" => Ok(Some(int_value(i64::from(self.result.start_line)))),
            "endLine" => Ok(Some(int_value(i64::from(self.result.end_line)))),
            "content" => Ok(Some(json_str(self.result.content.clone()))),
            _ => Ok(None),
        }
    }
}

struct RepoEmbeddingJobsConnectionObject {
    data: RepoEmbeddingJobsConnectionData,
}

impl ObjectValue for RepoEmbeddingJobsConnectionObject {
    fn type_name(&self) -> &'static str {
        "RepoEmbeddingJobsConnection"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "nodes" => Ok(Some(FieldValue::List {
                element_type: Some("RepoEmbeddingJob"),
                items: self
                    .data
                    .nodes
                    .iter()
                    .cloned()
                    .map(|job| {
                        FieldValue::Object(Arc::new(RepoEmbeddingJobObject { job }))
                    })
                    .collect(),
            })),
            "totalCount" => Ok(Some(int_value(self.data.total_count))),
            "pageInfo" => Ok(Some(FieldValue::Object(Arc::new(
                ConnectionPageInfoObject::new(
                    self.data.start_cursor.clone(),
                    self.data.end_cursor.clone(),
                    self.data.has_next_page,
                    self.data.has_previous_page,
                ),
            )))),
            _ => Ok(None),
        }
    }
}

struct RepoEmbeddingJobObject {
    job: RepoEmbeddingJobData,
}

impl ObjectValue for RepoEmbeddingJobObject {
    fn type_name(&self) -> &'static str {
        "RepoEmbeddingJob"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "id" => Ok(Some(json_str(marshal_job_id(self.job.id)))),
            "state" => Ok(Some(json_str(self.job.state.clone()))),
            "failureMessage" => Ok(Some(optional_str(
                self.job.failure_message.as_deref(),
            ))),
            "queuedAt" => Ok(Some(json_str(datetime_rfc3339(self.job.queued_at)))),
            "startedAt" => Ok(Some(optional_datetime(self.job.started_at))),
            "finishedAt" => Ok(Some(optional_datetime(self.job.finished_at))),
            "processAfter" => Ok(Some(optional_datetime(self.job.process_after))),
            "numResets" => Ok(Some(int_value(i64::from(self.job.num_resets)))),
            "numFailures" => Ok(Some(int_value(i64::from(self.job.num_failures)))),
            "lastHeartbeatAt" => Ok(Some(optional_datetime(self.job.last_heartbeat_at))),
            "workerHostname" => Ok(Some(json_str(self.job.worker_hostname.clone()))),
            "cancel" => Ok(Some(FieldValue::Json(Json::Bool(self.job.cancel)))),
            "repo" => match &self.job.repo {
                Err(message) => Err(ExecError::new(message.clone())),
                Ok(None) => Ok(Some(FieldValue::Null)),
                Ok(Some(repo)) => Ok(Some(FieldValue::Object(Arc::new(
                    RepositoryObject::from_data(repository_data(repo)),
                )))),
            },
            "revision" => {
                let repo = match &self.job.repo {
                    Err(message) => return Err(ExecError::new(message.clone())),
                    Ok(repo) => repo,
                };
                if self.job.revision.is_empty() {
                    return Ok(Some(FieldValue::Null));
                }
                let Some(repo) = repo else {
                    return Ok(Some(FieldValue::Null));
                };
                Ok(Some(FieldValue::Object(Arc::new(
                    GitCommitResolverObject {
                        repo: MinimalRepo {
                            id: repo.id,
                            name: repo.name.clone(),
                        },
                        oid: self.job.revision.clone(),
                        input_rev: None,
                        commit: None,
                        commit_error: None,
                        git: None,
                    },
                ))))
            }
            "stats" => match &self.job.stats {
                Err(message) => Err(ExecError::new(message.clone())),
                Ok(stats) => Ok(Some(FieldValue::Object(Arc::new(
                    RepoEmbeddingsStatsObject { stats: *stats },
                )))),
            },
            _ => Ok(None),
        }
    }
}

struct RepoEmbeddingsStatsObject {
    stats: RepoEmbeddingsStatsData,
}

impl ObjectValue for RepoEmbeddingsStatsObject {
    fn type_name(&self) -> &'static str {
        "RepoEmbeddingsStats"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "filesScheduled" => Ok(Some(int_value(self.stats.files_scheduled))),
            "filesEmbedded" => Ok(Some(int_value(self.stats.files_embedded))),
            "filesSkipped" => Ok(Some(int_value(self.stats.files_skipped))),
            _ => Ok(None),
        }
    }
}

fn repository_data(repo: &EmbeddingsRepo) -> RepositoryData {
    RepositoryData {
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
    }
}

fn json_str(value: String) -> FieldValue {
    FieldValue::Json(Json::Str(value))
}

fn optional_str(value: Option<&str>) -> FieldValue {
    match value {
        Some(value) => FieldValue::Json(Json::Str(value.to_owned())),
        None => FieldValue::Null,
    }
}

fn int_value(value: i64) -> FieldValue {
    FieldValue::Json(Json::Int(value))
}

fn optional_datetime(value: Option<OffsetDateTime>) -> FieldValue {
    match value {
        Some(value) => FieldValue::Json(Json::Str(datetime_rfc3339(value))),
        None => FieldValue::Null,
    }
}

// ---------------------------------------------------------------------------
// Root dispatch
// ---------------------------------------------------------------------------

/// `Query` fields owned by this surface.
pub fn is_query_root_field(field: &str) -> bool {
    matches!(
        field,
        "embeddingsSearch"
            | "embeddingsMultiSearch"
            | "isContextRequiredForChatQuery"
            | "repoEmbeddingJobs"
    )
}

/// `Mutation` fields owned by this surface.
pub fn is_mutation_field(field: &str) -> bool {
    matches!(
        field,
        "scheduleRepositoriesForEmbedding" | "cancelRepoEmbeddingJob"
    )
}

/// Resolve a prefetched `Query` field.
pub fn query_root_field(
    prefetch: &EmbeddingsGraphqlPrefetch,
    field: &str,
    arguments: &Arguments,
) -> Result<Option<FieldValue>, ExecError> {
    match field {
        "embeddingsSearch" | "embeddingsMultiSearch" => {
            let Some(key) = search_key_from_arguments(field, arguments) else {
                return Ok(None);
            };
            match prefetch.searches.get(&key) {
                None => Ok(None),
                Some(Err(message)) => Err(ExecError::new(message.clone())),
                Some(Ok(results)) => Ok(Some(FieldValue::Object(Arc::new(
                    EmbeddingsSearchResultsObject {
                        data: results.clone(),
                    },
                )))),
            }
        }
        "isContextRequiredForChatQuery" => match &prefetch.context_required {
            None => Ok(None),
            Some(Err(message)) => Err(ExecError::new(message.clone())),
            Some(Ok(value)) => Ok(Some(FieldValue::Json(Json::Bool(*value)))),
        },
        "repoEmbeddingJobs" => {
            let query = jobs_query(arguments, None);
            replay_jobs(prefetch, &query)
        }
        _ => Ok(None),
    }
}

fn search_key_from_arguments(field: &str, arguments: &Arguments) -> Option<SearchKey> {
    let repo_ids = if field == "embeddingsSearch" {
        vec![unmarshal_relay_id(arguments.string("repo")?).ok()?]
    } else {
        let mut ids = Vec::new();
        if let Some(InputValue::List(items)) = arguments.get("repos") {
            for item in items {
                ids.push(unmarshal_relay_id(item.as_str()?).ok()?);
            }
        }
        ids
    };
    Some(SearchKey {
        multi: field == "embeddingsMultiSearch",
        repo_ids,
        query: arguments.string("query")?.to_owned(),
        code_results_count: arguments
            .i64("codeResultsCount")
            .unwrap_or_default() as i32,
        text_results_count: arguments
            .i64("textResultsCount")
            .unwrap_or_default() as i32,
    })
}

/// Resolve a prefetched mutation outcome.
pub fn mutation_field(
    prefetch: &EmbeddingsGraphqlPrefetch,
    field: &str,
) -> Result<Option<FieldValue>, ExecError> {
    if !is_mutation_field(field) {
        return Ok(None);
    }
    match prefetch.mutations.get(field) {
        None => Ok(None),
        Some(Err(message)) => Err(ExecError::new(message.clone())),
        // `EmptyResponse!` renders the baseline non-nil empty object.
        Some(Ok(())) => Ok(Some(FieldValue::Object(Arc::new(
            crate::codeintel_uploads_graphql::EmptyResponseObject,
        )))),
    }
}

/// Resolve `Repository.embeddingJobs`.
pub fn repository_embedding_jobs(
    prefetch: &EmbeddingsGraphqlPrefetch,
    repo_id: i32,
    arguments: &Arguments,
) -> Result<Option<FieldValue>, ExecError> {
    let query = jobs_query(arguments, Some(repo_id));
    replay_jobs(prefetch, &query)
}

fn replay_jobs(
    prefetch: &EmbeddingsGraphqlPrefetch,
    query: &EmbeddingsJobsQuery,
) -> Result<Option<FieldValue>, ExecError> {
    match prefetch.jobs.get(query) {
        None => Ok(None),
        Some(Err(message)) => Err(ExecError::new(message.clone())),
        Some(Ok(data)) => Ok(Some(FieldValue::Object(Arc::new(
            RepoEmbeddingJobsConnectionObject { data: data.clone() },
        )))),
    }
}

/// Whether [`field_selected`] finds any selection named `name` (kept here so
/// the surface's own tests do not depend on the module's private helpers).
pub fn selection_selects(fields: &[RootField], name: &str) -> bool {
    field_selected(fields, name)
}
