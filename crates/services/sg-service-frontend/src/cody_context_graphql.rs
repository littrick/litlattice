#![forbid(unsafe_code)]

//! The M9.48d Cody context GraphQL surface:
//! `cmd/frontend/graphqlbackend/cody_context.graphql` plus
//! `cmd/frontend/graphqlbackend/cody_context.go` and
//! `cmd/frontend/internal/context/resolvers/context.go`.
//!
//! The retrieval work itself reuses the existing
//! [`crate::codycontext::CodyContextClient`] (M9.9c); this module only maps its
//! `GetCodyContextResult` onto GraphQL objects, and implements the small
//! resolver-side helpers (`countLines`, identity rerank, intent HTTP request,
//! URL mention text extraction).
//!
//! No table, column or Redis key is added. When the model/retrieval backend is
//! not injected the roots fail with an explicit error instead of returning
//! fabricated `contextItems` (the Go client's own
//! `cody is not enabled: <reason>` / `an embeddings service has not been
//! configured` errors are preserved).

use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use sg_graphql::exec::{Actor, Arguments, ExecError, FieldValue, InputValue, ObjectValue, RootField};
use sg_graphql::json::Json;

use crate::codycontext::{
    CodyContextClient, FileChunkContext, GetContextArgs, RepoIdName,
};
use crate::embeddings_graphql_surface::CodyGate;
use crate::git_graphql::GitCommitResolverObject;
use crate::graphql_resolvers::{RepositoryData, RepositoryObject};
use crate::search_stream::convert::MinimalRepo;

/// `StopReasonTimeout`.
pub const STOP_REASON_TIMEOUT: &str = "TIMEOUT";
/// `StopReasonDone`.
pub const STOP_REASON_DONE: &str = "DONE";
/// `codycontext.chunkSizeRunes`.
pub const CHUNK_SIZE_RUNES: usize = 1024;
/// `urlContextReadLimit`.
pub const URL_CONTEXT_READ_LIMIT: usize = 5 * 1024 * 1024;
/// `urlContextOutputLimit`.
pub const URL_CONTEXT_OUTPUT_LIMIT: usize = 14_000;
/// `intent detection backend not configured`.
pub const INTENT_NOT_CONFIGURED: &str = "intent detection backend not configured";
/// The dotcom-only gate.
pub const DOTCOM_ONLY: &str = "this feature is only available on sourcegraph.com";

// ---------------------------------------------------------------------------
// Seams
// ---------------------------------------------------------------------------

/// A repository row the cody resolvers need.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CodyRepo {
    pub id: i32,
    pub name: String,
}

/// `db.Repos().GetReposSetByIDs` / `GetByName` subset.
#[async_trait]
pub trait CodyRepoStore: Send + Sync {
    /// Missing ids are omitted (the baseline `GetReposSetByIDs` does not
    /// error); the caller turns a missing id into `could not find repo with id`.
    async fn by_ids(&self, ids: &[i32]) -> Result<Vec<CodyRepo>, String>;
    async fn by_name(&self, name: &str) -> Result<Option<CodyRepo>, String>;
}

#[async_trait]
impl CodyRepoStore for crate::embeddings_graphql_surface::RepositoryLookupAdapter {
    async fn by_ids(&self, ids: &[i32]) -> Result<Vec<CodyRepo>, String> {
        let mut repos = Vec::with_capacity(ids.len());
        for id in ids {
            if let Some(repo) = crate::embeddings_graphql_surface::EmbeddingsRepoLookup::by_id(
                self, *id,
            )
            .await?
            {
                repos.push(CodyRepo {
                    id: repo.id,
                    name: repo.name,
                });
            }
        }
        Ok(repos)
    }

    async fn by_name(&self, name: &str) -> Result<Option<CodyRepo>, String> {
        Ok(
            crate::embeddings_graphql_surface::EmbeddingsRepoLookup::by_name(self, name)
                .await?
                .map(|repo| CodyRepo {
                    id: repo.id,
                    name: repo.name,
                }),
        )
    }
}

/// `gitserver.Client.Stat` + `NewFileReader` reduced to one file read.
#[async_trait]
pub trait CodyChunkSource: Send + Sync {
    async fn read_file(
        &self,
        repo_id: i32,
        repo_name: &str,
        commit_id: &str,
        path: &str,
    ) -> Result<Vec<u8>, String>;
}

/// `schema.IntentDetectionAPI` backend.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct IntentBackendConfig {
    pub url: String,
    pub auth_header: String,
}

/// `schema.IntentDetectionAPI`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct IntentBackends {
    pub default: IntentBackendConfig,
    pub edit: Option<IntentBackendConfig>,
    pub extra: Vec<IntentBackendConfig>,
}

/// The response of one intent POST (the baseline never checks the status).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IntentHttpResponse {
    pub status: u16,
    pub body: Vec<u8>,
}

/// `httpcli.UncachedExternalDoer` seam for `sendIntentRequest`.
#[async_trait]
pub trait IntentHttpTransport: Send + Sync {
    async fn post(
        &self,
        url: &str,
        auth_header: &str,
        content_type: &str,
        body: &[u8],
    ) -> Result<IntentHttpResponse, String>;
}

/// `httpcli.UncachedExternalClient` seam for `UrlMentionContext`.
#[async_trait]
pub trait UrlContextFetcher: Send + Sync {
    async fn get(&self, url: &str) -> Result<IntentHttpResponse, String>;
}

/// `conf.CodyReranker()`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum CodyReranker {
    #[default]
    Identity,
    Cohere,
}

/// The injected cody GraphQL dependencies.
pub struct CodyContextDeps {
    /// `dotcom.SourcegraphDotComMode()`.
    pub dotcom: bool,
    pub gate: CodyGate,
    pub context: Option<Arc<CodyContextClient>>,
    pub repos: Option<Arc<dyn CodyRepoStore>>,
    pub chunks: Option<Arc<dyn CodyChunkSource>>,
    pub intent: Option<IntentBackends>,
    pub intent_http: Option<Arc<dyn IntentHttpTransport>>,
    pub reranker: CodyReranker,
    pub url_fetcher: Option<Arc<dyn UrlContextFetcher>>,
}

impl std::fmt::Debug for CodyContextDeps {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CodyContextDeps")
            .field("dotcom", &self.dotcom)
            .field("gate", &self.gate)
            .field("context", &self.context.is_some())
            .field("repos", &self.repos.is_some())
            .field("chunks", &self.chunks.is_some())
            .field("intent", &self.intent)
            .field("intent_http", &self.intent_http.is_some())
            .field("reranker", &self.reranker)
            .field("url_fetcher", &self.url_fetcher.is_some())
            .finish()
    }
}

impl CodyContextDeps {
    /// `contextApiEnabled` (`context.go:300-314`): dotcom first, then cody,
    /// then the verified-email requirement.
    fn context_api_enabled(&self, actor: Actor) -> Result<(), String> {
        if !self.dotcom {
            return Err(DOTCOM_ONLY.to_owned());
        }
        self.api_gate(actor)
    }

    /// `chatIntentApiEnabled`: cody + verified email (no dotcom check).
    fn chat_intent_api_enabled(&self, actor: Actor) -> Result<(), String> {
        self.api_gate(actor)
    }

    /// `cody.IsCodyEnabled`: unauthenticated first, then the injected gate.
    fn api_gate(&self, actor: Actor) -> Result<(), String> {
        if !actor.is_authenticated() {
            return Err("cody is not enabled: not authenticated".to_owned());
        }
        self.gate.check()
    }
}

// ---------------------------------------------------------------------------
// Result data
// ---------------------------------------------------------------------------

/// A resolved `FileChunkContext` plus the blob content it points at.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CodyFileChunkData {
    pub repo_id: i32,
    pub repo_name: String,
    pub commit_id: String,
    pub path: String,
    pub start_line: i32,
    pub end_line: i32,
    pub content: Vec<u8>,
}

/// `ContextList`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CodyContextListData {
    pub name: String,
    /// Each chunk is resolved lazily; `ContextItems` surfaces per-chunk errors.
    pub items: Vec<Result<CodyFileChunkData, String>>,
}

/// `CodyContextAlternativesResult`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CodyContextAlternativesData {
    pub lists: Vec<CodyContextListData>,
}

/// One `RetrieverContextItem`.
#[derive(Debug, Clone, PartialEq)]
pub struct RetrieverContextItemData {
    pub retriever: String,
    pub score: Option<f64>,
    pub item: CodyFileChunkData,
}

/// `ChatContextResult`.
#[derive(Debug, Clone, PartialEq)]
pub struct ChatContextData {
    pub items: Vec<RetrieverContextItemData>,
    pub partial_errors: Vec<String>,
    pub stop_reason: String,
}

/// `ChatIntentResponse`.
#[derive(Debug, Clone, PartialEq)]
pub struct ChatIntentData {
    pub intent: String,
    pub score: f64,
    pub search_score: f64,
    pub edit_score: f64,
}

/// `RankedItem`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct RankedItemData {
    pub index: i32,
    pub score: f64,
}

/// `RankingResponse`.
#[derive(Debug, Clone, PartialEq)]
pub struct RankingData {
    pub ranker: String,
    pub used: Vec<RankedItemData>,
    pub ignored: Vec<RankedItemData>,
}

/// `URLMentionContextResult`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct UrlMentionData {
    pub title: Option<String>,
    pub content: String,
}

/// One prefetched root value, keyed by [`root_key`].
#[derive(Debug, Clone, PartialEq)]
pub enum CodyRootValue {
    Alternatives(Result<CodyContextAlternativesData, String>),
    ContextItems(Result<Vec<CodyFileChunkData>, String>),
    ChatContext(Result<ChatContextData, String>),
    ChatIntent(Result<ChatIntentData, String>),
    Ranking(Result<RankingData, String>),
    Record(Result<(), String>),
    UrlMention(Result<UrlMentionData, String>),
}

/// Per-request prefetched cody roots.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct CodyContextPrefetch {
    pub roots: BTreeMap<String, CodyRootValue>,
}

// ---------------------------------------------------------------------------
// Small pure helpers
// ---------------------------------------------------------------------------

/// `countLines(content, numRunes)`: append `"\n"` when missing, truncate to
/// `numRunes` runes and count the newlines in the truncated bytes.
pub fn count_lines(content: &str, num_runes: usize) -> i32 {
    if content.is_empty() {
        return 0;
    }
    let truncated: String = if content.ends_with('\n') {
        content.chars().take(num_runes).collect()
    } else {
        let mut with_newline = content.to_owned();
        with_newline.push('\n');
        with_newline.chars().take(num_runes).collect()
    };
    truncated.bytes().filter(|byte| *byte == b'\n').count() as i32
}

/// `fileChunkToResolver`: content from `startLine`, `countLines`, inclusive
/// `endLine`.
pub fn chunk_end_line(content: &str, start_line: i32) -> i32 {
    let num_lines = count_lines(content, CHUNK_SIZE_RUNES);
    start_line + num_lines - 1
}

/// `git_blob_content_from_line`: `Content(ctx, startLine)` (the start of the
/// file for `startLine <= 0`).
pub fn content_from_line(content: &[u8], start_line: i32) -> String {
    let page = crate::git_tree_entry_graphql::page_content(content, Some(start_line), None);
    String::from_utf8_lossy(page).into_owned()
}

/// The identity reranker (`rerank` when `conf.CodyReranker() == identity`).
pub fn identity_rank(count: usize) -> Vec<RankedItemData> {
    (0..count)
        .map(|index| RankedItemData {
            index: i32::try_from(index).unwrap_or(i32::MAX),
            score: 0.5,
        })
        .collect()
}

/// `sendIntentRequest`'s body parsing: `intent`, `score`,
/// `combined_search_score` (all zero when absent, like Go's struct decode).
#[derive(Debug, Clone, Default, PartialEq)]
pub struct IntentApiResponse {
    pub intent: String,
    pub score: f64,
    pub search_score: f64,
}

pub fn parse_intent_response(body: &[u8]) -> Result<IntentApiResponse, String> {
    let value: serde_json::Value =
        serde_json::from_slice(body).map_err(|error| error.to_string())?;
    Ok(IntentApiResponse {
        intent: value
            .get("intent")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default()
            .to_owned(),
        score: value
            .get("score")
            .and_then(serde_json::Value::as_f64)
            .unwrap_or_default(),
        search_score: value
            .get("combined_search_score")
            .and_then(serde_json::Value::as_f64)
            .unwrap_or_default(),
    })
}

/// `sendIntentRequest`: POST the JSON body with the baseline headers and parse
/// the response (status is not inspected, exactly like the baseline).
pub async fn send_intent_request(
    transport: &dyn IntentHttpTransport,
    backend: &IntentBackendConfig,
    request: &[u8],
) -> Result<IntentApiResponse, String> {
    let response = transport
        .post(
            &backend.url,
            &backend.auth_header,
            "application/json",
            request,
        )
        .await?;
    parse_intent_response(&response.body)
}

/// `UrlMentionContext`'s HTML processing: status guard, `<title>`, `<main>`
/// trimming, `html2text(UNIX)` and the 14000-byte output truncation.
pub fn process_url_context(response: &IntentHttpResponse) -> Result<UrlMentionData, String> {
    if response.status >= 400 {
        return Err(format!("request failed with status {}", response.status));
    }
    let limited = &response.body[..response.body.len().min(URL_CONTEXT_READ_LIMIT)];
    let text = String::from_utf8_lossy(limited);
    let title = title_regexp()
        .captures(&text)
        .map(|captures| captures[1].to_owned());
    let mut content: &[u8] = limited;
    if let Some(index) = find_bytes(content, b"<main") {
        if index > 0 {
            content = &content[index..];
        }
    }
    if let Some(index) = find_bytes(content, b"</main>") {
        let end = index + b"</main>".len();
        if end <= content.len() {
            content = &content[..end];
        }
    }
    let html = String::from_utf8_lossy(content);
    let textified =
        sg_txemail::html2text::html2text_with_line_break(&html, sg_txemail::html2text::UNIX_LINE_BREAK);
    let bytes = textified.as_bytes();
    let truncated = &bytes[..bytes.len().min(URL_CONTEXT_OUTPUT_LIMIT)];
    Ok(UrlMentionData {
        title,
        content: String::from_utf8_lossy(truncated).into_owned(),
    })
}

fn title_regexp() -> &'static regex::Regex {
    static RE: std::sync::OnceLock<regex::Regex> = std::sync::OnceLock::new();
    RE.get_or_init(|| regex::Regex::new(r"<title>([^<]+)</title>").expect("title regexp"))
}

fn find_bytes(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}

// ---------------------------------------------------------------------------
// Prefetch
// ---------------------------------------------------------------------------

/// The prefetch map key: field name plus every selected argument (aliases with
/// different arguments stay distinct).
pub fn root_key(field: &str, arguments: &Arguments) -> String {
    let mut key = field.to_owned();
    for (name, value) in arguments.iter() {
        key.push('|');
        key.push_str(name);
        key.push('=');
        key.push_str(&format!("{value:?}"));
    }
    key
}

/// Prefetch every selected cody root.
pub async fn prefetch_cody_context(
    deps: &CodyContextDeps,
    fields: &[RootField],
    actor: Actor,
) -> CodyContextPrefetch {
    let mut prefetch = CodyContextPrefetch::default();
    for field in fields {
        let key = root_key(&field.name, &field.arguments);
        let value = match field.name.as_str() {
            "getCodyContextAlternatives" => {
                CodyRootValue::Alternatives(get_alternatives(deps, &field.arguments, actor).await)
            }
            "getCodyContext" => {
                let alternatives = get_alternatives(deps, &field.arguments, actor).await;
                CodyRootValue::ContextItems(alternatives.and_then(|data| {
                    match data.lists.into_iter().next() {
                        None => Ok(Vec::new()),
                        Some(list) => list.items.into_iter().collect(),
                    }
                }))
            }
            "chatContext" => {
                CodyRootValue::ChatContext(chat_context(deps, &field.arguments, actor).await)
            }
            "chatIntent" => {
                CodyRootValue::ChatIntent(chat_intent(deps, &field.arguments, actor).await)
            }
            "rankContext" => {
                CodyRootValue::Ranking(rank_context(deps, &field.arguments, actor))
            }
            "recordContext" => {
                CodyRootValue::Record(deps.context_api_enabled(actor).map(|()| ()))
            }
            "urlMentionContext" => {
                CodyRootValue::UrlMention(url_mention_context(deps, &field.arguments).await)
            }
            _ => continue,
        };
        prefetch.roots.insert(key, value);
    }
    prefetch
}

async fn get_alternatives(
    deps: &CodyContextDeps,
    arguments: &Arguments,
    actor: Actor,
) -> Result<CodyContextAlternativesData, String> {
    let context = deps
        .context
        .as_ref()
        .ok_or_else(|| "cody context client is not configured".to_owned())?;
    let repos_store = deps
        .repos
        .as_ref()
        .ok_or_else(|| "repository lookup is not wired for cody context".to_owned())?;
    let chunks = deps
        .chunks
        .as_ref()
        .ok_or_else(|| "gitserver chunk source is not wired for cody context".to_owned())?;

    let mut repo_ids = Vec::new();
    if let Some(InputValue::List(items)) = arguments.get("repos") {
        for item in items {
            repo_ids.push(crate::graphql_resolvers::unmarshal_relay_id(
                item.as_str().unwrap_or_default(),
            )?);
        }
    }
    let file_patterns = match arguments.get("filePatterns") {
        Some(InputValue::List(items)) => {
            let mut patterns = Vec::new();
            for item in items {
                let pattern = item.as_str().unwrap_or_default();
                regex::Regex::new(pattern).map_err(|error| {
                    format!("invalid file pattern {pattern:?}: {error}")
                })?;
                patterns.push(pattern.to_owned());
            }
            patterns
        }
        _ => Vec::new(),
    };

    let found = repos_store.by_ids(&repo_ids).await?;
    let mut repos = Vec::with_capacity(repo_ids.len());
    for repo_id in &repo_ids {
        let Some(repo) = found.iter().find(|repo| repo.id == *repo_id) else {
            return Err(format!("could not find repo with id {repo_id}"));
        };
        repos.push(RepoIdName {
            id: repo.id,
            name: repo.name.clone(),
        });
    }

    let result = context
        .get_cody_context(
            &GetContextArgs {
                repos,
                repo_stats_present: false,
                file_patterns,
                query: arguments.string("query").unwrap_or_default().to_owned(),
                code_results_count: arguments
                    .i64("codeResultsCount")
                    .unwrap_or_default() as i32,
                text_results_count: arguments
                    .i64("textResultsCount")
                    .unwrap_or_default() as i32,
            },
            crate::search_contexts::RequestContext::from_graphql_actor(actor, deps.dotcom),
        )
        .await?;

    let mut lists = Vec::with_capacity(result.context_lists.len());
    for list in result.context_lists {
        let mut items = Vec::with_capacity(list.file_chunks.len());
        for chunk in list.file_chunks {
            items.push(resolve_file_chunk(chunks.as_ref(), chunk).await);
        }
        lists.push(CodyContextListData {
            name: list.name,
            items,
        });
    }
    Ok(CodyContextAlternativesData { lists })
}

async fn resolve_file_chunk(
    chunks: &dyn CodyChunkSource,
    chunk: FileChunkContext,
) -> Result<CodyFileChunkData, String> {
    let content = chunks
        .read_file(
            chunk.repo_id,
            &chunk.repo_name,
            &chunk.commit_id,
            &chunk.path,
        )
        .await?;
    let from_start = content_from_line(&content, chunk.start_line);
    let end_line = chunk_end_line(&from_start, chunk.start_line);
    Ok(CodyFileChunkData {
        repo_id: chunk.repo_id,
        repo_name: chunk.repo_name,
        commit_id: chunk.commit_id,
        path: chunk.path,
        start_line: chunk.start_line,
        end_line,
        content,
    })
}

async fn chat_context(
    deps: &CodyContextDeps,
    arguments: &Arguments,
    actor: Actor,
) -> Result<ChatContextData, String> {
    deps.context_api_enabled(actor)?;
    let repos_store = deps
        .repos
        .as_ref()
        .ok_or_else(|| "repository lookup is not wired for cody context".to_owned())?;
    let repo_name = arguments.string("repo").unwrap_or_default();
    let Some(repo) = repos_store.by_name(repo_name).await? else {
        return Err(format!("repo not found: {repo_name}"));
    };
    let context = deps
        .context
        .as_ref()
        .ok_or_else(|| "cody context client is not configured".to_owned())?;
    let chunks = deps
        .chunks
        .as_ref()
        .ok_or_else(|| "gitserver chunk source is not wired for cody context".to_owned())?;
    let result = context
        .get_cody_context(
            &GetContextArgs {
                repos: vec![RepoIdName {
                    id: repo.id,
                    name: repo.name,
                }],
                repo_stats_present: false,
                file_patterns: Vec::new(),
                query: arguments.string("query").unwrap_or_default().to_owned(),
                code_results_count: 0,
                text_results_count: 0,
            },
            crate::search_contexts::RequestContext::from_graphql_actor(actor, deps.dotcom),
        )
        .await?;
    let mut data = ChatContextData {
        items: Vec::new(),
        partial_errors: Vec::new(),
        stop_reason: STOP_REASON_DONE.to_owned(),
    };
    let Some(list) = result.context_lists.into_iter().next() else {
        return Ok(data);
    };
    for chunk in list.file_chunks {
        match resolve_file_chunk(chunks.as_ref(), chunk.clone()).await {
            Ok(resolved) => data.items.push(RetrieverContextItemData {
                retriever: "zoekt".to_owned(),
                score: None,
                item: resolved,
            }),
            Err(error) => data
                .partial_errors
                .push(format!("resolving file chunk {}: {error}", chunk.path)),
        }
    }
    Ok(data)
}

async fn chat_intent(
    deps: &CodyContextDeps,
    arguments: &Arguments,
    actor: Actor,
) -> Result<ChatIntentData, String> {
    deps.chat_intent_api_enabled(actor)?;
    let backends = deps
        .intent
        .as_ref()
        .ok_or_else(|| INTENT_NOT_CONFIGURED.to_owned())?;
    let transport = deps
        .intent_http
        .as_ref()
        .ok_or_else(|| "intent detection HTTP client is not wired".to_owned())?;
    let request = serde_json::json!({
        "query": arguments.string("query").unwrap_or_default(),
    });
    let body = serde_json::to_vec(&request).map_err(|error| error.to_string())?;
    let (main, edit) = tokio::join!(
        send_intent_request(transport.as_ref(), &backends.default, &body),
        async {
            match &backends.edit {
                Some(edit) => Some(send_intent_request(transport.as_ref(), edit, &body).await),
                None => None,
            }
        }
    );
    let main = main?;
    let edit = match edit {
        Some(result) => Some(result?),
        None => None,
    };
    // Extra backends are best-effort; errors are logged and ignored.
    for extra in &backends.extra {
        let _ = send_intent_request(transport.as_ref(), extra, &body).await;
    }
    Ok(ChatIntentData {
        intent: main.intent,
        score: main.score,
        search_score: main.search_score,
        edit_score: edit.map(|edit| edit.score).unwrap_or_default(),
    })
}

fn rank_context(
    deps: &CodyContextDeps,
    arguments: &Arguments,
    actor: Actor,
) -> Result<RankingData, String> {
    deps.context_api_enabled(actor)?;
    let count = match arguments.get("contextItems") {
        Some(InputValue::List(items)) => items.len(),
        _ => 0,
    };
    match deps.reranker {
        CodyReranker::Identity => Ok(RankingData {
            ranker: "identity".to_owned(),
            used: identity_rank(count),
            ignored: Vec::new(),
        }),
        // `EXTERNAL_GAP`: the Cohere client is not ported; returning scores
        // without a model would fabricate ranking output.
        CodyReranker::Cohere => Err("cohere context reranking is not ported".to_owned()),
    }
}

async fn url_mention_context(
    deps: &CodyContextDeps,
    arguments: &Arguments,
) -> Result<UrlMentionData, String> {
    // `UrlMentionContext` has no cody/dotcom gate in the baseline.
    let fetcher = deps
        .url_fetcher
        .as_ref()
        .ok_or_else(|| "URL context fetcher is not wired".to_owned())?;
    let response = fetcher
        .get(arguments.string("url").unwrap_or_default())
        .await?;
    process_url_context(&response)
}

// ---------------------------------------------------------------------------
// Object resolvers
// ---------------------------------------------------------------------------

struct FileChunkContextObject {
    chunk: CodyFileChunkData,
}

impl ObjectValue for FileChunkContextObject {
    fn type_name(&self) -> &'static str {
        "FileChunkContext"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "blob" => Ok(Some(FieldValue::Object(Arc::new(CodyGitBlobObject {
                chunk: self.chunk.clone(),
            })))),
            "startLine" => Ok(Some(int_value(i64::from(self.chunk.start_line)))),
            "endLine" => Ok(Some(int_value(i64::from(self.chunk.end_line)))),
            "chunkContent" => {
                let page = crate::git_tree_entry_graphql::page_content(
                    &self.chunk.content,
                    Some(self.chunk.start_line),
                    Some(self.chunk.end_line),
                );
                Ok(Some(json_str(
                    String::from_utf8_lossy(page).into_owned(),
                )))
            }
            _ => Ok(None),
        }
    }
}

/// The `GitBlob` returned by `FileChunkContext.blob`. Only the fields the
/// baseline resolves from the already-fetched file are ported; the remaining
/// `GitBlob`/`TreeEntry`/`File2` fields keep their existing unported state.
struct CodyGitBlobObject {
    chunk: CodyFileChunkData,
}

impl ObjectValue for CodyGitBlobObject {
    fn type_name(&self) -> &'static str {
        "GitBlob"
    }

    fn field(
        &self,
        field: &str,
        arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "path" => Ok(Some(json_str(self.chunk.path.clone()))),
            "name" => Ok(Some(json_str(
                self.chunk
                    .path
                    .rsplit('/')
                    .next()
                    .unwrap_or_default()
                    .to_owned(),
            ))),
            "isDirectory" => Ok(Some(FieldValue::Json(Json::Bool(false)))),
            "content" => {
                let start = arguments.i64("startLine").map(|line| line as i32);
                let end = arguments.i64("endLine").map(|line| line as i32);
                let page =
                    crate::git_tree_entry_graphql::page_content(&self.chunk.content, start, end);
                Ok(Some(json_str(
                    String::from_utf8_lossy(page).into_owned(),
                )))
            }
            "byteSize" => Ok(Some(int_value(self.chunk.content.len() as i64))),
            "totalLines" => Ok(Some(int_value(line_count(&self.chunk.content) as i64))),
            "binary" => Ok(Some(FieldValue::Json(Json::Bool(is_binary(
                &self.chunk.content,
            ))))),
            "commit" => Ok(Some(FieldValue::Object(Arc::new(
                GitCommitResolverObject {
                    repo: MinimalRepo {
                        id: self.chunk.repo_id,
                        name: self.chunk.repo_name.clone(),
                    },
                    oid: self.chunk.commit_id.clone(),
                    input_rev: None,
                    commit: None,
                    commit_error: None,
                    git: None,
                },
            )))),
            "repository" => Ok(Some(FieldValue::Object(Arc::new(
                RepositoryObject::from_data(RepositoryData {
                    id: self.chunk.repo_id,
                    name: self.chunk.repo_name.clone(),
                    uri: self.chunk.repo_name.clone(),
                    description: String::new(),
                    fork: false,
                    archived: false,
                    private: false,
                    stars: 0,
                    external_id: String::new(),
                    external_service_type: String::new(),
                    external_service_id: String::new(),
                }),
            )))),
            _ => Ok(None),
        }
    }
}

fn line_count(content: &[u8]) -> usize {
    if content.is_empty() {
        return 0;
    }
    let mut count = content.iter().filter(|byte| **byte == b'\n').count();
    if content.last() != Some(&b'\n') {
        count += 1;
    }
    count
}

fn is_binary(content: &[u8]) -> bool {
    content[..content.len().min(8000)].contains(&0)
}

struct CodyContextAlternativesResultObject {
    data: CodyContextAlternativesData,
}

impl ObjectValue for CodyContextAlternativesResultObject {
    fn type_name(&self) -> &'static str {
        "CodyContextAlternativesResult"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "contextLists" => Ok(Some(context_lists_value(&self.data.lists))),
            _ => Ok(None),
        }
    }
}

fn context_lists_value(lists: &[CodyContextListData]) -> FieldValue {
    FieldValue::List {
        element_type: Some("CodyContextList"),
        items: lists
            .iter()
            .cloned()
            .map(|list| FieldValue::Object(Arc::new(CodyContextListObject { list })))
            .collect(),
    }
}

struct CodyContextListObject {
    list: CodyContextListData,
}

impl ObjectValue for CodyContextListObject {
    fn type_name(&self) -> &'static str {
        "CodyContextList"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            // The SDL spells this field `Name` (capital N).
            "Name" => Ok(Some(json_str(self.list.name.clone()))),
            "contextItems" => Ok(Some(context_items_value(&self.list.items)?)),
            _ => Ok(None),
        }
    }
}

fn context_items_value(
    items: &[Result<CodyFileChunkData, String>],
) -> Result<FieldValue, ExecError> {
    let mut values = Vec::with_capacity(items.len());
    for item in items {
        match item {
            Err(message) => return Err(ExecError::new(message.clone())),
            Ok(chunk) => values.push(FieldValue::Object(Arc::new(FileChunkContextObject {
                chunk: chunk.clone(),
            }))),
        }
    }
    Ok(FieldValue::List {
        element_type: Some("CodyContextResult"),
        items: values,
    })
}

fn flat_context_items_value(items: &[CodyFileChunkData]) -> FieldValue {
    FieldValue::List {
        element_type: Some("CodyContextResult"),
        items: items
            .iter()
            .cloned()
            .map(|chunk| FieldValue::Object(Arc::new(FileChunkContextObject { chunk })))
            .collect(),
    }
}

struct ChatContextResultObject {
    data: ChatContextData,
}

impl ObjectValue for ChatContextResultObject {
    fn type_name(&self) -> &'static str {
        "ChatContextResult"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "contextItems" => Ok(Some(FieldValue::List {
                element_type: Some("RetrieverContextItem"),
                items: self
                    .data
                    .items
                    .iter()
                    .cloned()
                    .map(|item| {
                        FieldValue::Object(Arc::new(RetrieverContextItemObject { item }))
                    })
                    .collect(),
            })),
            "partialErrors" => Ok(Some(FieldValue::List {
                element_type: Some("String"),
                items: self
                    .data
                    .partial_errors
                    .iter()
                    .cloned()
                    .map(json_str)
                    .collect(),
            })),
            "stopReason" => Ok(Some(json_str(self.data.stop_reason.clone()))),
            _ => Ok(None),
        }
    }
}

struct RetrieverContextItemObject {
    item: RetrieverContextItemData,
}

impl ObjectValue for RetrieverContextItemObject {
    fn type_name(&self) -> &'static str {
        "RetrieverContextItem"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "retriever" => Ok(Some(json_str(self.item.retriever.clone()))),
            "score" => Ok(Some(match self.item.score {
                Some(score) => FieldValue::Json(Json::Float(score)),
                None => FieldValue::Null,
            })),
            "item" => Ok(Some(FieldValue::Object(Arc::new(
                FileChunkContextObject {
                    chunk: self.item.item.clone(),
                },
            )))),
            _ => Ok(None),
        }
    }
}

struct ChatIntentResponseObject {
    data: ChatIntentData,
}

impl ObjectValue for ChatIntentResponseObject {
    fn type_name(&self) -> &'static str {
        "ChatIntentResponse"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "intent" => Ok(Some(json_str(self.data.intent.clone()))),
            "score" => Ok(Some(FieldValue::Json(Json::Float(self.data.score)))),
            "searchScore" => Ok(Some(FieldValue::Json(Json::Float(
                self.data.search_score,
            )))),
            "editScore" => Ok(Some(FieldValue::Json(Json::Float(
                self.data.edit_score,
            )))),
            _ => Ok(None),
        }
    }
}

struct RankingResponseObject {
    data: RankingData,
}

impl ObjectValue for RankingResponseObject {
    fn type_name(&self) -> &'static str {
        "RankingResponse"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "ranker" => Ok(Some(json_str(self.data.ranker.clone()))),
            "used" => Ok(Some(ranked_items_value(&self.data.used))),
            "ignored" => Ok(Some(ranked_items_value(&self.data.ignored))),
            _ => Ok(None),
        }
    }
}

fn ranked_items_value(items: &[RankedItemData]) -> FieldValue {
    FieldValue::List {
        element_type: Some("RankedItem"),
        items: items
            .iter()
            .copied()
            .map(|item| FieldValue::Object(Arc::new(RankedItemObject { item })))
            .collect(),
    }
}

struct RankedItemObject {
    item: RankedItemData,
}

impl ObjectValue for RankedItemObject {
    fn type_name(&self) -> &'static str {
        "RankedItem"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "index" => Ok(Some(int_value(i64::from(self.item.index)))),
            "score" => Ok(Some(FieldValue::Json(Json::Float(self.item.score)))),
            _ => Ok(None),
        }
    }
}

struct UrlMentionContextResultObject {
    data: UrlMentionData,
}

impl ObjectValue for UrlMentionContextResultObject {
    fn type_name(&self) -> &'static str {
        "URLMentionContextResult"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "title" => Ok(Some(match &self.data.title {
                Some(title) => json_str(title.clone()),
                None => FieldValue::Null,
            })),
            "content" => Ok(Some(json_str(self.data.content.clone()))),
            _ => Ok(None),
        }
    }
}

fn json_str(value: String) -> FieldValue {
    FieldValue::Json(Json::Str(value))
}

fn int_value(value: i64) -> FieldValue {
    FieldValue::Json(Json::Int(value))
}

// ---------------------------------------------------------------------------
// Root dispatch
// ---------------------------------------------------------------------------

/// `Query` fields owned by this surface.
pub fn is_root_field(field: &str) -> bool {
    matches!(
        field,
        "getCodyContext"
            | "getCodyContextAlternatives"
            | "chatIntent"
            | "rankContext"
            | "recordContext"
            | "chatContext"
            | "urlMentionContext"
    )
}

/// Replay a prefetched cody root field.
pub fn root_field(
    prefetch: &CodyContextPrefetch,
    field: &str,
    arguments: &Arguments,
) -> Result<Option<FieldValue>, ExecError> {
    let key = root_key(field, arguments);
    let Some(value) = prefetch.roots.get(&key) else {
        return Ok(None);
    };
    match value {
        CodyRootValue::Alternatives(Err(message))
        | CodyRootValue::ContextItems(Err(message))
        | CodyRootValue::ChatContext(Err(message))
        | CodyRootValue::ChatIntent(Err(message))
        | CodyRootValue::Ranking(Err(message))
        | CodyRootValue::Record(Err(message))
        | CodyRootValue::UrlMention(Err(message)) => Err(ExecError::new(message.clone())),
        CodyRootValue::Alternatives(Ok(data)) => Ok(Some(FieldValue::Object(Arc::new(
            CodyContextAlternativesResultObject { data: data.clone() },
        )))),
        CodyRootValue::ContextItems(Ok(items)) => {
            Ok(Some(flat_context_items_value(items)))
        }
        CodyRootValue::ChatContext(Ok(data)) => Ok(Some(FieldValue::Object(Arc::new(
            ChatContextResultObject { data: data.clone() },
        )))),
        CodyRootValue::ChatIntent(Ok(data)) => Ok(Some(FieldValue::Object(Arc::new(
            ChatIntentResponseObject { data: data.clone() },
        )))),
        CodyRootValue::Ranking(Ok(data)) => Ok(Some(FieldValue::Object(Arc::new(
            RankingResponseObject { data: data.clone() },
        )))),
        // `recordContext: EmptyResponse` is nullable; the resolver returns
        // `(nil, nil)`.
        CodyRootValue::Record(Ok(())) => Ok(Some(FieldValue::Null)),
        CodyRootValue::UrlMention(Ok(data)) => Ok(Some(FieldValue::Object(Arc::new(
            UrlMentionContextResultObject { data: data.clone() },
        )))),
    }
}

