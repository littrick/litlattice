#![forbid(unsafe_code)]

//! `meteredSearcher` (`internal/search/backend/metered_searcher.go`), minus
//! the exporter.
//!
//! The baseline records OpenTelemetry spans and a Prometheus histogram. This
//! port keeps the parts that are observable behavior of the searcher itself:
//!
//! * `queryString`, `listCategory`, `codeAndErrorStr`, `filterDefaultValue`;
//! * the attribute lists attached to search/list telemetry;
//! * call counters and the last call's labels on [`MeteredStreamer`].
//!
//! No exporter is created; callers decide what to do with the attributes.

use std::fmt;
use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use std::time::Duration;

use crate::query::Query;
use crate::streamer::Streamer;
use crate::types::{ListOptions, ListResponse, RepoListField, SearchChunk, SearchOptions};

/// `src_zoekt_request_duration_seconds` (`requestDuration`).
pub const REQUEST_DURATION_METRIC: &str = "src_zoekt_request_duration_seconds";

// ----------------------------------------------------------- label values

/// `queryString`: `"<nil>"` for a nil query, otherwise `query.Q.String()`.
pub fn query_string(query: Option<&Query>) -> String {
    match query {
        None => "<nil>".to_owned(),
        Some(query) => query.to_string(),
    }
}

/// `listCategory`, taking the Go numeric `ListOptions.Field`
/// (`0` = Repos, `1` = removed Minimal, `2` = ReposMap).
pub fn list_category_raw(field: i32) -> &'static str {
    // GetField rejects 1 and every value other than 0/2, so the switch's
    // default arm is unreachable in the baseline.
    match field {
        0 => "List",
        1 => "ListMisconfigured",
        2 => "ListReposMap",
        _ => "ListMisconfigured",
    }
}

/// `listCategory(opts)`: nil opts behaves like `Field: Repos`.
pub fn list_category(opts: Option<ListOptions>) -> &'static str {
    let field = match opts {
        None => 0,
        Some(opts) => match opts.field {
            RepoListField::Repos => 0,
            RepoListField::ReposMap => 2,
            RepoListField::Unknown => 3,
        },
    };
    list_category_raw(field)
}

/// The error properties `codeAndErrorStr` consults
/// (`errors.Is(err, context.Canceled)` and friends).
pub trait ErrorClassifier: fmt::Display {
    fn is_canceled(&self) -> bool {
        false
    }

    fn is_deadline_exceeded(&self) -> bool {
        false
    }
}

impl ErrorClassifier for crate::client::Error {
    fn is_canceled(&self) -> bool {
        crate::client::Error::is_canceled(self)
    }

    fn is_deadline_exceeded(&self) -> bool {
        crate::client::Error::is_deadline_exceeded(self)
    }
}

/// `codeAndErrorStr`.
pub fn code_and_error_str(err: Option<&dyn ErrorClassifier>) -> (String, String) {
    match err {
        None => ("200".to_owned(), String::new()),
        Some(err) if err.is_canceled() => ("canceled".to_owned(), String::new()),
        Some(err) if err.is_deadline_exceeded() => ("timeout".to_owned(), String::new()),
        Some(err) => ("error".to_owned(), err.to_string()),
    }
}

// -------------------------------------------------------------- attributes

/// The attribute value kinds `filterDefaultValue` distinguishes.
#[derive(Debug, Clone, PartialEq)]
pub enum AttributeValue {
    Bool(bool),
    Int(i64),
    Float(f64),
    String(String),
}

#[derive(Debug, Clone, PartialEq)]
pub struct Attribute {
    pub key: String,
    pub value: AttributeValue,
}

impl Attribute {
    pub fn bool(key: impl Into<String>, value: bool) -> Self {
        Self {
            key: key.into(),
            value: AttributeValue::Bool(value),
        }
    }

    pub fn int(key: impl Into<String>, value: i64) -> Self {
        Self {
            key: key.into(),
            value: AttributeValue::Int(value),
        }
    }

    pub fn float(key: impl Into<String>, value: f64) -> Self {
        Self {
            key: key.into(),
            value: AttributeValue::Float(value),
        }
    }

    pub fn string(key: impl Into<String>, value: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            value: AttributeValue::String(value.into()),
        }
    }

    /// The per-type default `filterDefaultValue` drops. Slice values are not
    /// modeled, matching the baseline's "we do not handle slice types".
    pub fn is_default(&self) -> bool {
        match &self.value {
            AttributeValue::Bool(value) => !value,
            AttributeValue::Int(value) => *value == 0,
            AttributeValue::Float(value) => *value == 0.0,
            AttributeValue::String(value) => value.is_empty(),
        }
    }
}

/// `filterDefaultValue`: drop default-valued attributes, keeping order.
pub fn filter_default_value(attributes: Vec<Attribute>) -> Vec<Attribute> {
    attributes
        .into_iter()
        .filter(|attribute| !attribute.is_default())
        .collect()
}

/// `opts.*` attributes for `StreamSearch` (already filtered).
pub fn search_option_attributes(opts: &SearchOptions) -> Vec<Attribute> {
    filter_default_value(vec![
        Attribute::bool("opts.estimate_doc_count", opts.estimate_doc_count),
        Attribute::bool("opts.whole", opts.whole),
        Attribute::int("opts.shard_max_match_count", opts.shard_max_match_count),
        Attribute::int(
            "opts.shard_repo_max_match_count",
            opts.shard_repo_max_match_count,
        ),
        Attribute::int("opts.total_max_match_count", opts.total_max_match_count),
        Attribute::int(
            "opts.max_wall_time_ms",
            opts.max_wall_time.as_millis() as i64,
        ),
        Attribute::int(
            "opts.flush_wall_time_ms",
            opts.flush_wall_time.as_millis() as i64,
        ),
        Attribute::int("opts.max_doc_display_count", opts.max_doc_display_count),
        Attribute::int("opts.max_match_display_count", opts.max_match_display_count),
        Attribute::int("opts.context_lines", opts.num_context_lines),
        Attribute::bool("opts.chunk_matches", opts.chunk_matches),
        Attribute::bool("opts.use_document_ranks", opts.use_document_ranks),
        Attribute::float("opts.document_ranks_weight", opts.document_ranks_weight),
        Attribute::bool("opts.use_bm25_scoring", opts.use_bm25_scoring),
        Attribute::bool("opts.debug_score", opts.debug_score),
    ])
}

/// The category and attributes attached to a telemetry span/event.
#[derive(Debug, Clone, PartialEq)]
pub struct CallAttributes {
    pub category: String,
    pub attributes: Vec<Attribute>,
}

impl CallAttributes {
    fn leaf_base(query: Option<&Query>, hostname: &str, category: &str) -> Self {
        let mut attributes = vec![Attribute::string("query", query_string(query))];
        let is_leaf = !hostname.is_empty();
        let category = if is_leaf {
            attributes.push(Attribute::string("span.kind", "client"));
            attributes.push(Attribute::string("peer.address", hostname));
            attributes.push(Attribute::string("peer.service", "zoekt"));
            category.to_owned()
        } else {
            format!("{category}All")
        };
        Self {
            category,
            attributes,
        }
    }
}

/// `StreamSearch` span labels: `SearchAll` for aggregators, `Search` for
/// leaves; leaves add the peer attributes and `opts.*`.
pub fn search_call_attributes(
    query: Option<&Query>,
    hostname: &str,
    opts: Option<&SearchOptions>,
) -> CallAttributes {
    let mut call = CallAttributes::leaf_base(query, hostname, "Search");
    if let Some(opts) = opts {
        call.attributes.extend(search_option_attributes(opts));
    }
    call
}

/// `List` span labels: `ListAll` for aggregators, otherwise `listCategory`.
pub fn list_call_attributes(
    query: Option<&Query>,
    hostname: &str,
    opts: Option<ListOptions>,
) -> CallAttributes {
    let category = if hostname.is_empty() {
        "ListAll"
    } else {
        list_category(opts)
    };
    CallAttributes::leaf_base(query, hostname, category)
}

/// `zoekt.Stats` fields the metered searcher reads.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct StatsMetrics {
    pub content_bytes_loaded: i64,
    pub index_bytes_loaded: i64,
    pub crashes: i64,
    pub file_count: i64,
    pub files_considered: i64,
    pub files_loaded: i64,
    pub files_skipped: i64,
    pub match_count: i64,
    pub ngram_lookups: i64,
    pub ngram_matches: i64,
    pub shard_files_considered: i64,
    pub shards_scanned: i64,
    pub shards_skipped: i64,
    pub shards_skipped_filter: i64,
    pub wait: Duration,
    pub match_tree_construction: Duration,
    pub match_tree_search: Duration,
    pub regexps_considered: i64,
    /// `FlushReason.String()`.
    pub flush_reason: String,
}

/// The `done` event fields of `StreamSearch`.
pub fn search_done_fields(
    stats: &StatsMetrics,
    file_matches: i64,
    events: i64,
    total_send_time_ms: i64,
    code: &str,
) -> Vec<Attribute> {
    let mut fields = vec![
        Attribute::int("filematches", file_matches),
        Attribute::int("events", events),
        Attribute::int("stream.total_send_time_ms", total_send_time_ms),
        Attribute::string("code", code),
    ];
    fields.extend(filter_default_value(vec![
        Attribute::int("stats.content_bytes_loaded", stats.content_bytes_loaded),
        Attribute::int("stats.index_bytes_loaded", stats.index_bytes_loaded),
        Attribute::int("stats.crashes", stats.crashes),
        Attribute::int("stats.file_count", stats.file_count),
        Attribute::int("stats.files_considered", stats.files_considered),
        Attribute::int("stats.files_loaded", stats.files_loaded),
        Attribute::int("stats.files_skipped", stats.files_skipped),
        Attribute::int("stats.match_count", stats.match_count),
        Attribute::int("stats.ngram_lookups", stats.ngram_lookups),
        Attribute::int("stats.ngram_matches", stats.ngram_matches),
        Attribute::int("stats.shard_files_considered", stats.shard_files_considered),
        Attribute::int("stats.shards_scanned", stats.shards_scanned),
        Attribute::int("stats.shards_skipped", stats.shards_skipped),
        Attribute::int("stats.shards_skipped_filter", stats.shards_skipped_filter),
        Attribute::int("stats.wait_ms", stats.wait.as_millis() as i64),
        Attribute::int(
            "stats.match_tree_construction_ms",
            stats.match_tree_construction.as_millis() as i64,
        ),
        Attribute::int(
            "stats.match_tree_search_ms",
            stats.match_tree_search.as_millis() as i64,
        ),
        Attribute::int("stats.regexps_considered", stats.regexps_considered),
        Attribute::string("stats.flush_reason", stats.flush_reason.clone()),
    ]));
    fields
}

/// The `done` event fields of `List`. Note the baseline *replaces* the fields
/// (including `code`) when the upstream returned a `RepoList`.
pub fn list_done_fields(list: Option<&ListResponse>, code: &str) -> Vec<Attribute> {
    match list {
        Some(list) => vec![
            Attribute::int("repos", (list.repos.len() + list.repos_map.len()) as i64),
            Attribute::int("stats.crashes", list.crashes),
        ],
        None => vec![Attribute::string("code", code)],
    }
}

/// The `requestDuration.WithLabelValues(hostname, cat, code)` triple.
pub fn request_duration_labels<'a>(
    hostname: &'a str,
    category: &'a str,
    code: &'a str,
) -> (&'a str, &'a str, &'a str) {
    (hostname, category, code)
}

// --------------------------------------------------------------- wrapper

/// A snapshot of the labels of the most recent call.
#[derive(Debug, Clone, PartialEq)]
pub struct CallRecord {
    pub category: String,
    pub attributes: Vec<Attribute>,
    pub fields: Vec<Attribute>,
}

/// `meteredSearcher`: delegates to the wrapped streamer and records counts and
/// the latest labels. No exporter is touched.
pub struct MeteredStreamer<S: Streamer> {
    inner: S,
    hostname: String,
    list_calls: AtomicU64,
    search_calls: AtomicU64,
    last_list: Mutex<Option<CallRecord>>,
    last_search: Mutex<Option<CallRecord>>,
}

impl<S: Streamer> MeteredStreamer<S> {
    /// `NewMeteredSearcher`.
    pub fn new(hostname: impl Into<String>, inner: S) -> Self {
        Self {
            inner,
            hostname: hostname.into(),
            list_calls: AtomicU64::new(0),
            search_calls: AtomicU64::new(0),
            last_list: Mutex::new(None),
            last_search: Mutex::new(None),
        }
    }

    pub fn inner(&self) -> &S {
        &self.inner
    }

    pub fn hostname(&self) -> &str {
        &self.hostname
    }

    pub fn list_calls(&self) -> u64 {
        self.list_calls.load(Ordering::SeqCst)
    }

    pub fn search_calls(&self) -> u64 {
        self.search_calls.load(Ordering::SeqCst)
    }

    pub fn last_list_record(&self) -> Option<CallRecord> {
        self.last_list.lock().expect("last_list poisoned").clone()
    }

    pub fn last_search_record(&self) -> Option<CallRecord> {
        self.last_search
            .lock()
            .expect("last_search poisoned")
            .clone()
    }
}

impl<S: Streamer> MeteredStreamer<S>
where
    S::Error: ErrorClassifier,
{
    /// `meteredSearcher.List`.
    pub async fn list(
        &self,
        query: &Query,
        opts: Option<ListOptions>,
    ) -> Result<ListResponse, S::Error> {
        self.list_calls.fetch_add(1, Ordering::SeqCst);
        let call = list_call_attributes(Some(query), &self.hostname, opts);
        let result = self.inner.list(query, opts).await;
        let (code, _) =
            code_and_error_str(result.as_ref().err().map(|err| err as &dyn ErrorClassifier));
        let fields = list_done_fields(result.as_ref().ok(), &code);
        *self.last_list.lock().expect("last_list poisoned") = Some(CallRecord {
            category: call.category,
            attributes: call.attributes,
            fields,
        });
        result
    }

    /// `meteredSearcher.StreamSearch`.
    pub async fn stream_search(
        &self,
        query: &Query,
        options: &SearchOptions,
        sender: &mut (dyn FnMut(SearchChunk) + Send),
    ) -> Result<(), S::Error> {
        self.search_calls.fetch_add(1, Ordering::SeqCst);
        let call = search_call_attributes(Some(query), &self.hostname, Some(options));
        let result = self.inner.stream_search(query, options, sender).await;
        *self.last_search.lock().expect("last_search poisoned") = Some(CallRecord {
            category: call.category,
            attributes: call.attributes,
            fields: Vec::new(),
        });
        result
    }

    /// `meteredSearcher.Search` without aggregation: collect the streamed
    /// chunks and sum their crash counts. The full `SearchResult` aggregation
    /// lives in the M9.8gq slice.
    pub async fn search_chunks(
        &self,
        query: &Query,
        options: &SearchOptions,
    ) -> Result<(Vec<SearchChunk>, i64), S::Error> {
        let mut chunks = Vec::new();
        let mut crashes = 0i64;
        {
            let mut sender = |chunk: SearchChunk| {
                crashes += chunk.crashes;
                chunks.push(chunk);
            };
            self.stream_search(query, options, &mut sender).await?;
        }
        Ok((chunks, crashes))
    }
}

impl<S: Streamer> Streamer for MeteredStreamer<S>
where
    S::Error: ErrorClassifier,
{
    type Error = S::Error;

    fn list(
        &self,
        query: &Query,
        opts: Option<ListOptions>,
    ) -> impl Future<Output = Result<ListResponse, Self::Error>> + Send {
        MeteredStreamer::list(self, query, opts)
    }

    fn stream_search(
        &self,
        query: &Query,
        options: &SearchOptions,
        sender: &mut (dyn FnMut(SearchChunk) + Send),
    ) -> impl Future<Output = Result<(), Self::Error>> + Send {
        MeteredStreamer::stream_search(self, query, options, sender)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_values_are_filtered_like_the_baseline() {
        let attributes = filter_default_value(vec![
            Attribute::bool("b", false),
            Attribute::bool("b2", true),
            Attribute::int("i", 0),
            Attribute::int("i2", 3),
            Attribute::float("f", 0.0),
            Attribute::float("f2", 1.5),
            Attribute::string("s", ""),
            Attribute::string("s2", "x"),
        ]);
        let keys: Vec<&str> = attributes.iter().map(|a| a.key.as_str()).collect();
        assert_eq!(keys, ["b2", "i2", "f2", "s2"]);
    }

    #[test]
    fn list_category_matches_get_field() {
        assert_eq!(list_category(None), "List");
        assert_eq!(list_category_raw(0), "List");
        assert_eq!(list_category_raw(1), "ListMisconfigured");
        assert_eq!(list_category_raw(2), "ListReposMap");
        assert_eq!(list_category_raw(3), "ListMisconfigured");
    }

    #[test]
    fn list_done_fields_replace_code_when_a_list_exists() {
        let fields = list_done_fields(None, "error");
        assert_eq!(fields, vec![Attribute::string("code", "error")]);

        let list = ListResponse {
            repos: vec![Default::default()],
            repos_map: [(7, Default::default())].into_iter().collect(),
            crashes: 2,
            stats: Default::default(),
        };
        let fields = list_done_fields(Some(&list), "200");
        assert_eq!(
            fields,
            vec![
                Attribute::int("repos", 2),
                Attribute::int("stats.crashes", 2)
            ]
        );
    }
}
