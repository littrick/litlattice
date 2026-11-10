#![forbid(unsafe_code)]

//! Zoekt horizontal-search helpers (`internal/search/backend/horizontal.go`).
//!
//! These are the pieces of the Go `HorizontalSearcher` that do not depend on
//! endpoint discovery or a concrete transport:
//!
//! - [`aggregate_stream_search`] is `AggregateStreamSearch`: run a stream
//!   search, append every event's `Files`, aggregate `Stats`, and stamp the
//!   wall-clock `Duration` on success.
//! - [`Deduper`] is `dedupper.Dedup`: a repository may live on several
//!   replicas during a rebalance, so only the first endpoint that reports it
//!   keeps its matches.
//! - [`crash_event`] is `crashEvent`.
//! - [`zoekt_rollout_reason`] classifies the `net` errors that are expected
//!   during a zoekt rollout. Go injects the concrete `*net.DNSError` /
//!   `*net.OpError`; this port takes an already classified [`RolloutError`]
//!   so the stable categories can be exercised without `net` types.
//!
//! The `HorizontalSearcher` itself (`StreamSearch`/`Search`/`List`/
//! `syncSearchers`/`Close`) is out of scope, as are the Prometheus counter and
//! trace event emitted by `isZoektRolloutError`.

use std::collections::HashMap;

use crate::cached::{Clock, SystemClock};
use crate::query::Query;
use crate::types::{FileMatch, SearchOptions, SearchResult, Stats};

/// `zoekt.Sender`: receives full search-result events (not wire chunks).
///
/// A blanket implementation is provided for `FnMut(SearchResult)`, mirroring
/// the Go `ZoektStreamFunc` adapter.
pub trait Sender {
    fn send(&mut self, event: SearchResult);
}

impl<F> Sender for F
where
    F: FnMut(SearchResult),
{
    fn send(&mut self, event: SearchResult) {
        self(event);
    }
}

struct AggregateCollector<'a> {
    aggregate: &'a mut SearchResult,
}

impl Sender for AggregateCollector<'_> {
    fn send(&mut self, event: SearchResult) {
        self.aggregate.files.extend(event.files);
        self.aggregate.stats.add(&event.stats);
    }
}

/// `AggregateStreamSearch` with the system clock.
///
/// `stream_search` is the Go `streamSearch` callback: it emits zero or more
/// events through [`Sender`] and returns a transport error. On error the
/// partially aggregated result is discarded, matching the baseline's
/// `return nil, err`.
pub fn aggregate_stream_search<Ctx, E, F>(
    ctx: &Ctx,
    query: &Query,
    options: Option<&SearchOptions>,
    stream_search: F,
) -> Result<SearchResult, E>
where
    F: FnOnce(&Ctx, &Query, Option<&SearchOptions>, &mut dyn Sender) -> Result<(), E>,
{
    aggregate_stream_search_with_clock(&SystemClock, ctx, query, options, stream_search)
}

/// [`aggregate_stream_search`] with an injectable [`Clock`].
///
/// The baseline's `context.WithCancel` has no equivalent here: the callback is
/// synchronous, so there is nothing to cancel.
pub fn aggregate_stream_search_with_clock<Ctx, E, F>(
    clock: &impl Clock,
    ctx: &Ctx,
    query: &Query,
    options: Option<&SearchOptions>,
    stream_search: F,
) -> Result<SearchResult, E>
where
    F: FnOnce(&Ctx, &Query, Option<&SearchOptions>, &mut dyn Sender) -> Result<(), E>,
{
    let start = clock.now();
    let mut aggregate = SearchResult::default();
    {
        let mut collector = AggregateCollector {
            aggregate: &mut aggregate,
        };
        stream_search(ctx, query, options, &mut collector)?;
    }

    // `Stats.Add` deliberately ignores `Duration`; the aggregate's duration is
    // the elapsed wall time, set only after a successful stream.
    aggregate.stats.duration = clock.now().saturating_duration_since(start);
    Ok(aggregate)
}

/// `dedupper`: repository name to the endpoint that first reported it.
#[derive(Debug, Clone, Default)]
pub struct Deduper {
    repo_endpoint: HashMap<String, String>,
}

impl Deduper {
    pub fn new() -> Self {
        Self::default()
    }

    /// `dedupper.Dedup` (in-place).
    ///
    /// A repository is already seen if a previous call saw a match for it on a
    /// *different* endpoint. Matches from the winning endpoint are kept, and
    /// the dedup table is refreshed from the surviving matches so later calls
    /// keep filtering them out.
    pub fn dedup(&mut self, endpoint: &str, files: &mut Vec<FileMatch>) {
        if files.is_empty() {
            return;
        }

        // PERF: normally `files` is sorted by repository, so comparing with the
        // previous entry avoids a map lookup for the common run-of-same-repo.
        let mut last_repo = String::new();
        let mut last_seen = false;

        files.retain(|file| {
            if last_repo.as_str() == file.repository.as_str() {
                if last_seen {
                    return false;
                }
            } else if let Some(seen_endpoint) = self.repo_endpoint.get(&file.repository) {
                if seen_endpoint.as_str() != endpoint {
                    last_repo.clone_from(&file.repository);
                    last_seen = true;
                    return false;
                }
            }

            last_repo.clone_from(&file.repository);
            last_seen = false;
            true
        });

        // Update the seen table from the surviving matches. A repository is
        // only recorded when it differs from the previous one, so an empty
        // repository name is never recorded (the baseline's `lastRepo` starts
        // as `""`).
        let mut last_repo = String::new();
        for file in files.iter() {
            if last_repo.as_str() != file.repository.as_str() {
                last_repo.clone_from(&file.repository);
                self.repo_endpoint
                    .insert(file.repository.clone(), endpoint.to_owned());
            }
        }
    }

    /// The `repoName -> endpoint` dedup table (observation/diff helper).
    pub fn entries(&self) -> &HashMap<String, String> {
        &self.repo_endpoint
    }
}

/// `crashEvent`: a synthetic result signalling one crashed shard/backend.
pub fn crash_event() -> SearchResult {
    SearchResult {
        stats: Stats {
            crashes: 1,
            ..Stats::default()
        },
        ..SearchResult::default()
    }
}

/// The stable `net` error categories `zoektRolloutReason` recognizes.
///
/// Go inspects `*net.DNSError.IsNotFound` first, then `*net.OpError` by `Op`
/// and underlying error text. Callers classify the concrete error (after
/// unwrapping) into one of these variants.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RolloutError {
    /// `*net.DNSError` with `IsNotFound` set.
    DnsNotFound,
    /// `*net.OpError` with `Op == "dial"` and `Timeout()`.
    DialTimeout,
    /// `*net.OpError` with `Op == "dial"` whose error contains
    /// `"connection refused"`.
    DialRefused,
    /// `*net.OpError` with `Op == "read"`.
    ReadFailed,
    /// Anything else (`""`).
    Other,
}

/// `zoektRolloutReason`: the reason label, or `""` when not ignored.
pub fn zoekt_rollout_reason(error: &RolloutError) -> &'static str {
    match error {
        RolloutError::DnsNotFound => "dns-not-found",
        RolloutError::DialTimeout => "dial-timeout",
        RolloutError::DialRefused => "dial-refused",
        RolloutError::ReadFailed => "read-failed",
        RolloutError::Other => "",
    }
}

/// `isZoektRolloutError`, minus the metric and trace side effects.
pub fn is_zoekt_rollout_error(error: &RolloutError) -> bool {
    !zoekt_rollout_reason(error).is_empty()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::RefCell;
    use std::time::Duration;

    use crate::cached::ManualClock;

    fn file(repo: &str, name: &str) -> FileMatch {
        FileMatch {
            repository: repo.to_owned(),
            file_name: name.as_bytes().to_vec(),
            ..FileMatch::default()
        }
    }

    fn names(files: &[FileMatch]) -> Vec<String> {
        files
            .iter()
            .map(|file| String::from_utf8_lossy(&file.file_name).into_owned())
            .collect()
    }

    fn dedup_one(deduper: &mut Deduper, endpoint: &str, files: &mut Vec<FileMatch>) -> Vec<String> {
        deduper.dedup(endpoint, files);
        names(files)
    }

    #[test]
    fn dedup_empty_and_nil_are_noops() {
        let mut deduper = Deduper::new();
        let mut files = Vec::new();
        assert!(dedup_one(&mut deduper, "a", &mut files).is_empty());
        assert!(deduper.entries().is_empty());
    }

    #[test]
    fn dedup_keeps_repeated_matches_from_the_same_endpoint() {
        let mut deduper = Deduper::new();
        let mut files = vec![file("r1", "a.go"), file("r1", "b.go")];
        assert_eq!(dedup_one(&mut deduper, "a", &mut files), ["a.go", "b.go"]);
        assert_eq!(deduper.entries().get("r1").map(String::as_str), Some("a"));
    }

    #[test]
    fn dedup_filters_repositories_seen_on_another_endpoint() {
        let mut deduper = Deduper::new();
        let mut first = vec![file("r1", "a.go"), file("r2", "a2.go")];
        assert_eq!(dedup_one(&mut deduper, "a", &mut first), ["a.go", "a2.go"]);

        let mut second = vec![file("r1", "b.go"), file("r3", "b3.go"), file("r1", "c.go")];
        // r1 is owned by endpoint "a"; the consecutive r1 after r3 is filtered
        // by both the table and the fast path.
        assert_eq!(dedup_one(&mut deduper, "b", &mut second), ["b3.go"]);
        assert_eq!(deduper.entries().get("r1").map(String::as_str), Some("a"));
        assert_eq!(deduper.entries().get("r3").map(String::as_str), Some("b"));
    }

    #[test]
    fn dedup_same_endpoint_survives_a_later_call() {
        let mut deduper = Deduper::new();
        let mut first = vec![file("r1", "a.go")];
        assert_eq!(dedup_one(&mut deduper, "a", &mut first), ["a.go"]);
        let mut again = vec![file("r1", "a2.go")];
        assert_eq!(dedup_one(&mut deduper, "a", &mut again), ["a2.go"]);
        let mut other = vec![file("r1", "b.go")];
        assert!(dedup_one(&mut deduper, "b", &mut other).is_empty());
    }

    #[test]
    fn dedup_interleaved_repositories_update_the_table_per_run() {
        let mut deduper = Deduper::new();
        let mut files = vec![file("r1", "a.go"), file("r2", "b.go"), file("r1", "c.go")];
        // No previous call, so everything is kept and r1 is recorded again at
        // its later position (last write wins for the endpoint).
        assert_eq!(
            dedup_one(&mut deduper, "a", &mut files),
            ["a.go", "b.go", "c.go"]
        );
        assert_eq!(deduper.entries().get("r1").map(String::as_str), Some("a"));
    }

    #[test]
    fn dedup_never_records_the_empty_repository() {
        let mut deduper = Deduper::new();
        let mut first = vec![file("", "a.go")];
        assert_eq!(dedup_one(&mut deduper, "a", &mut first), ["a.go"]);
        assert!(deduper.entries().is_empty());
        // Because it was never recorded, another endpoint still keeps it.
        let mut second = vec![file("", "b.go")];
        assert_eq!(dedup_one(&mut deduper, "b", &mut second), ["b.go"]);
    }

    #[test]
    fn aggregate_appends_files_and_adds_stats_in_event_order() {
        let clock = ManualClock::new();
        let events = RefCell::new(vec![
            SearchResult {
                files: vec![file("r1", "a.go"), file("r2", "b.go")],
                stats: Stats {
                    match_count: 2,
                    file_count: 1,
                    duration: Duration::from_secs(1000),
                    ..Stats::default()
                },
                ..SearchResult::default()
            },
            SearchResult {
                files: vec![file("r3", "c.go")],
                stats: Stats {
                    match_count: 3,
                    file_count: 4,
                    duration: Duration::from_secs(2000),
                    ..Stats::default()
                },
                ..SearchResult::default()
            },
        ]);
        let advance = clock.clone();

        let result: SearchResult = aggregate_stream_search_with_clock(
            &clock,
            &(),
            &Query::Const(true),
            None,
            move |_ctx, _query, _opts, sender| {
                for event in events.into_inner() {
                    sender.send(event);
                }
                advance.advance(Duration::from_millis(3));
                Ok::<_, std::convert::Infallible>(())
            },
        )
        .unwrap();

        assert_eq!(names(&result.files), ["a.go", "b.go", "c.go"]);
        assert_eq!(result.stats.match_count, 5);
        assert_eq!(result.stats.file_count, 5);
        // Events contributed 1000s + 2000s; `Stats.Add` must not sum it, and
        // the aggregate duration is the injected clock delta.
        assert_eq!(result.stats.duration, Duration::from_millis(3));
    }

    #[test]
    fn aggregate_empty_event_stream_still_stamps_duration() {
        let clock = ManualClock::new();
        let advance = clock.clone();
        let result: SearchResult = aggregate_stream_search_with_clock(
            &clock,
            &(),
            &Query::Const(true),
            None,
            move |_ctx, _query, _opts, _sender| {
                advance.advance(Duration::from_millis(5));
                Ok::<_, std::convert::Infallible>(())
            },
        )
        .unwrap();
        assert!(result.files.is_empty());
        assert_eq!(
            result.stats,
            Stats {
                duration: Duration::from_millis(5),
                ..Stats::default()
            }
        );
    }

    #[test]
    fn aggregate_discards_partial_results_on_error() {
        let clock = ManualClock::new();
        let events = RefCell::new(vec![SearchResult {
            files: vec![file("r1", "a.go")],
            ..SearchResult::default()
        }]);

        let result: Result<SearchResult, String> = aggregate_stream_search_with_clock(
            &clock,
            &(),
            &Query::Const(true),
            None,
            move |_ctx, _query, _opts, sender| {
                for event in events.into_inner() {
                    sender.send(event);
                }
                Err("boom".to_owned())
            },
        );

        assert_eq!(result.unwrap_err(), "boom");
    }

    #[test]
    fn aggregate_uses_the_system_clock_by_default() {
        let result: SearchResult = aggregate_stream_search(
            &(),
            &Query::Const(true),
            None,
            |_ctx, _query, _opts, _sender| Ok::<_, std::convert::Infallible>(()),
        )
        .unwrap();
        assert!(result.files.is_empty());
    }

    #[test]
    fn crash_event_has_one_crash_and_nothing_else() {
        let event = crash_event();
        assert_eq!(event.stats.crashes, 1);
        assert_eq!(
            event,
            SearchResult {
                stats: Stats {
                    crashes: 1,
                    ..Stats::default()
                },
                ..SearchResult::default()
            }
        );
    }

    #[test]
    fn rollout_reasons_match_the_baseline() {
        assert_eq!(
            zoekt_rollout_reason(&RolloutError::DnsNotFound),
            "dns-not-found"
        );
        assert_eq!(
            zoekt_rollout_reason(&RolloutError::DialTimeout),
            "dial-timeout"
        );
        assert_eq!(
            zoekt_rollout_reason(&RolloutError::DialRefused),
            "dial-refused"
        );
        assert_eq!(
            zoekt_rollout_reason(&RolloutError::ReadFailed),
            "read-failed"
        );
        assert_eq!(zoekt_rollout_reason(&RolloutError::Other), "");
        assert!(is_zoekt_rollout_error(&RolloutError::DnsNotFound));
        assert!(!is_zoekt_rollout_error(&RolloutError::Other));
    }
}
