//! `internal/search/streaming/progress.go`,
//! `internal/search/streaming/client/progress.go` and
//! `internal/search/streaming/api/progress.go`: stats aggregation and the
//! `progress` event.

use std::collections::{HashMap, HashSet};
use std::time::Instant;

use super::convert::Match;
use super::json;
use super::SearchDeps;

/// `search.RepoStatus` flags.
pub mod repo_status {
    pub const CLONING: u8 = 1 << 0;
    pub const MISSING: u8 = 1 << 1;
    pub const LIMIT_HIT: u8 = 1 << 2;
    pub const TIMED_OUT: u8 = 1 << 3;
}

/// `search.RepoStatusMap`.
#[derive(Debug, Clone, Default)]
pub struct RepoStatusMap {
    m: HashMap<i32, u8>,
    status: u8,
}

impl RepoStatusMap {
    /// `Update`.
    pub fn update(&mut self, id: i32, status: u8) {
        *self.m.entry(id).or_insert(0) |= status;
        self.status |= status;
    }

    /// `Union`.
    pub fn union(&mut self, other: &RepoStatusMap) {
        self.status |= other.status;
        for (id, status) in &other.m {
            *self.m.entry(*id).or_insert(0) |= status;
        }
    }

    /// `Any`.
    pub fn any(&self, mask: u8) -> bool {
        self.status & mask != 0
    }

    /// `Len`.
    pub fn len(&self) -> usize {
        self.m.len()
    }

    pub fn is_empty(&self) -> bool {
        self.m.is_empty()
    }

    /// `Filter`, returning the matching IDs in ascending order (the baseline
    /// sorts in `getRepos`).
    pub fn filter(&self, mask: u8) -> Vec<i32> {
        let mut repos: Vec<i32> = self
            .m
            .iter()
            .filter(|(_, status)| **status & mask != 0)
            .map(|(id, _)| *id)
            .collect();
        repos.sort_unstable();
        repos
    }
}

/// `streaming.Stats`.
#[derive(Debug, Clone, Default)]
pub struct Stats {
    pub is_limit_hit: bool,
    /// `Repos` is a set; `Update` only inserts.
    pub repos: HashSet<i32>,
    pub status: RepoStatusMap,
    pub backends_missing: i64,
    pub excluded_forks: i64,
    pub excluded_archived: i64,
}

impl Stats {
    /// `Stats.Update`.
    pub fn update(&mut self, other: &Stats) {
        self.is_limit_hit = self.is_limit_hit || other.is_limit_hit;
        for id in &other.repos {
            self.repos.insert(*id);
        }
        self.status.union(&other.status);
        self.backends_missing += other.backends_missing;
        self.excluded_forks += other.excluded_forks;
        self.excluded_archived += other.excluded_archived;
    }

    /// `Stats.Zero`.
    pub fn zero(&self) -> bool {
        !(self.is_limit_hit
            || !self.repos.is_empty()
            || !self.status.is_empty()
            || self.backends_missing > 0
            || self.excluded_forks > 0
            || self.excluded_archived > 0)
    }
}

/// `streaming.SearchEvent`.
#[derive(Debug, Clone, Default)]
pub struct SearchEvent {
    pub results: Vec<Match>,
    pub stats: Stats,
}

/// `streaming/api.ProgressStats`.
#[derive(Debug, Clone, Default)]
struct ProgressStats {
    match_count: i64,
    elapsed_milliseconds: i64,
    repositories_count: Option<i64>,
    backends_missing: i64,
    excluded_archived: i64,
    excluded_forks: i64,
    timedout: Vec<i32>,
    missing: Vec<i32>,
    cloning: Vec<i32>,
    limit_hit: bool,
    suggested_limit: i64,
    trace: String,
    display_limit: i64,
}

/// `streaming/api.Progress`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Progress {
    pub done: bool,
    pub repositories_count: Option<i64>,
    pub match_count: i64,
    pub duration_ms: i64,
    pub skipped: Vec<Skipped>,
    pub trace: String,
}

impl Progress {
    pub fn encode(&self) -> String {
        let mut out = String::from("{");
        json::field_bool(&mut out, "done", self.done);
        if let Some(count) = self.repositories_count {
            json::field_int(&mut out, "repositoriesCount", count);
        }
        json::field_int(&mut out, "matchCount", self.match_count);
        json::field_int(&mut out, "durationMs", self.duration_ms);
        json::separator_public(&mut out);
        json::push_key(&mut out, "skipped");
        out.push('[');
        for (i, skipped) in self.skipped.iter().enumerate() {
            if i > 0 {
                out.push(',');
            }
            skipped.encode(&mut out);
        }
        out.push(']');
        json::opt_str(&mut out, "trace", &self.trace);
        out.push('}');
        out
    }
}

/// `streaming/api.Skipped`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Skipped {
    pub reason: String,
    pub title: String,
    pub message: String,
    pub severity: &'static str,
    pub suggested: Option<SkippedSuggested>,
}

impl Skipped {
    fn encode(&self, out: &mut String) {
        out.push('{');
        json::field_str(out, "reason", &self.reason);
        json::field_str(out, "title", &self.title);
        json::field_str(out, "message", &self.message);
        json::field_str(out, "severity", self.severity);
        if let Some(suggested) = &self.suggested {
            json::separator_public(out);
            json::push_key(out, "suggested");
            out.push('{');
            json::field_str(out, "title", &suggested.title);
            json::field_str(out, "queryExpression", &suggested.query_expression);
            out.push('}');
        }
        out.push('}');
    }
}

/// `streaming/api.SkippedSuggested`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SkippedSuggested {
    pub title: String,
    pub query_expression: String,
}

/// `client.ProgressAggregator`.
pub struct ProgressAggregator {
    start: Instant,
    pub match_count: i64,
    pub stats: Stats,
    limit: i64,
    display_limit: i64,
    trace: String,
    /// `Dirty` is true if the aggregator changed since the last `Current`.
    pub dirty: bool,
    /// `RepoNamer`'s best-effort name cache.
    names: HashMap<i32, String>,
}

impl ProgressAggregator {
    pub fn new(start: Instant, limit: i64, display_limit: i64, trace: String) -> Self {
        Self {
            start,
            match_count: 0,
            stats: Stats::default(),
            limit,
            display_limit,
            trace,
            dirty: false,
            names: HashMap::new(),
        }
    }

    /// `ProgressAggregator.Update`.
    pub fn update(&mut self, event: &SearchEvent) {
        if event.results.is_empty() && event.stats.zero() {
            return;
        }

        self.dirty = true;
        self.stats.update(&event.stats);
        for match_ in &event.results {
            self.match_count += match_.result_count();
            self.stats.repos.insert(match_.repo_name().id);
        }

        if self.match_count > self.limit {
            self.match_count = self.limit;
            self.stats.is_limit_hit = true;
        }
    }

    fn current_stats(&self) -> ProgressStats {
        ProgressStats {
            match_count: self.match_count,
            elapsed_milliseconds: self.start.elapsed().as_millis() as i64,
            backends_missing: self.stats.backends_missing,
            excluded_archived: self.stats.excluded_archived,
            excluded_forks: self.stats.excluded_forks,
            timedout: self.stats.status.filter(repo_status::TIMED_OUT),
            missing: self.stats.status.filter(repo_status::MISSING),
            cloning: self.stats.status.filter(repo_status::CLONING),
            limit_hit: self.stats.is_limit_hit,
            suggested_limit: (self.limit + 1500) / 1000 * 1000,
            trace: self.trace.clone(),
            display_limit: self.display_limit,
            repositories_count: None,
        }
    }

    /// `ProgressAggregator.Current`.
    pub async fn current(&mut self, deps: &dyn SearchDeps) -> Progress {
        self.dirty = false;
        let stats = self.current_stats();
        build_progress_event(stats, deps, &mut self.names).await
    }

    /// `ProgressAggregator.Final`.
    pub async fn final_event(&mut self, deps: &dyn SearchDeps) -> Progress {
        self.dirty = false;
        let mut stats = self.current_stats();
        if !self.stats.repos.is_empty() {
            stats.repositories_count = Some(self.stats.repos.len() as i64);
        }
        let mut event = build_progress_event(stats, deps, &mut self.names).await;
        event.done = true;
        event
    }
}

/// `api.BuildProgressEvent` + the `skippedHandlers` pipeline.
async fn build_progress_event(
    stats: ProgressStats,
    deps: &dyn SearchDeps,
    cache: &mut HashMap<i32, String>,
) -> Progress {
    let mut skipped = Vec::new();
    if let Some(entry) = repository_missing_handler(&stats, deps, cache).await {
        skipped.push(entry);
    }
    if let Some(entry) = repository_cloning_handler(&stats, deps, cache).await {
        skipped.push(entry);
    }
    if let Some(entry) = shard_match_limit_handler(&stats) {
        skipped.push(entry);
    }
    if let Some(entry) = shard_timeout_handler(&stats, deps, cache).await {
        skipped.push(entry);
    }
    if let Some(entry) = backends_missing_handler(&stats) {
        skipped.push(entry);
    }
    if let Some(entry) = excluded_fork_handler(&stats) {
        skipped.push(entry);
    }
    if let Some(entry) = excluded_archive_handler(&stats) {
        skipped.push(entry);
    }
    if let Some(entry) = display_limit_handler(&stats) {
        skipped.push(entry);
    }

    Progress {
        done: false,
        repositories_count: stats.repositories_count,
        match_count: stats.match_count,
        duration_ms: stats.elapsed_milliseconds,
        skipped,
        trace: stats.trace,
    }
}

/// `streamclient.RepoNamer`: fill the cache, then answer with `UNKNOWN{ID=..}`
/// fallbacks.
async fn repo_names(
    ids: &[i32],
    deps: &dyn SearchDeps,
    cache: &mut HashMap<i32, String>,
) -> Vec<String> {
    let missing: Vec<i32> = ids
        .iter()
        .copied()
        .filter(|id| !cache.contains_key(id))
        .collect();
    if !missing.is_empty() {
        for (id, name) in deps.repo_names(&missing).await {
            cache.insert(id, name);
        }
    }
    ids.iter()
        .map(|id| {
            cache
                .get(id)
                .cloned()
                .unwrap_or_else(|| format!("UNKNOWN{{ID={id}}}"))
        })
        .collect()
}

async fn repository_missing_handler(
    stats: &ProgressStats,
    deps: &dyn SearchDeps,
    cache: &mut HashMap<i32, String>,
) -> Option<Skipped> {
    skipped_repos_handler(
        &stats.missing,
        "missing",
        "could not be searched".to_owned(),
        "repository-missing",
        "info",
        deps,
        cache,
    )
    .await
}

async fn repository_cloning_handler(
    stats: &ProgressStats,
    deps: &dyn SearchDeps,
    cache: &mut HashMap<i32, String>,
) -> Option<Skipped> {
    let reason = format!(
        "could not be searched since {} still cloning",
        plural("it is", "they are", stats.cloning.len() as i64)
    );
    skipped_repos_handler(
        &stats.cloning,
        "cloning",
        reason,
        "repository-cloning",
        "info",
        deps,
        cache,
    )
    .await
}

async fn shard_timeout_handler(
    stats: &ProgressStats,
    deps: &dyn SearchDeps,
    cache: &mut HashMap<i32, String>,
) -> Option<Skipped> {
    skipped_repos_handler(
        &stats.timedout,
        "timed out",
        "could not be searched in time".to_owned(),
        "shard-timeout",
        "warn",
        deps,
        cache,
    )
    .await
}

async fn skipped_repos_handler(
    repos: &[i32],
    title_verb: &str,
    message_reason: String,
    reason: &str,
    severity: &'static str,
    deps: &dyn SearchDeps,
    cache: &mut HashMap<i32, String>,
) -> Option<Skipped> {
    if repos.is_empty() {
        return None;
    }
    let amount = number(repos.len() as i64);
    let title = format!("{amount} {title_verb}");

    let message = if repos.len() == 1 {
        let names = repo_names(repos, deps, cache).await;
        format!(
            "`{}` {message_reason}. Try searching again or reducing the scope of your query with `repo:`,  `context:` or other filters.",
            names[0]
        )
    } else {
        let mut sample_size = 10;
        if sample_size > repos.len() {
            sample_size = repos.len();
        }
        let mut message = format!(
            "{amount} repositories {message_reason}. Try searching again or reducing the scope of your query with `repo:`, `context:` or other filters."
        );
        let names = repo_names(&repos[..sample_size], deps, cache).await;
        for name in names {
            message.push_str(&format!("\n* `{name}`"));
        }
        if sample_size < repos.len() {
            message.push_str("\n* ...");
        }
        message
    };

    Some(Skipped {
        reason: reason.to_owned(),
        title,
        message,
        severity,
        suggested: None,
    })
}

fn shard_match_limit_handler(stats: &ProgressStats) -> Option<Skipped> {
    if !stats.limit_hit {
        return None;
    }
    let suggested = if stats.suggested_limit > 0 {
        Some(SkippedSuggested {
            title: "increase limit".to_owned(),
            query_expression: format!("count:{}", stats.suggested_limit),
        })
    } else {
        None
    };
    Some(Skipped {
        reason: "shard-match-limit".to_owned(),
        title: "result limit hit".to_owned(),
        message: "Not all results have been returned due to hitting a match limit. Sourcegraph has limits for the number of results returned from a line, document and repository.".to_owned(),
        severity: "info",
        suggested,
    })
}

fn backends_missing_handler(stats: &ProgressStats) -> Option<Skipped> {
    let count = stats.backends_missing;
    if count == 0 {
        return None;
    }
    let amount = number(count);
    Some(Skipped {
        reason: "backend-missing".to_owned(),
        title: format!(
            "{amount} {} down",
            plural("backend", "backends", count)
        ),
        message: "Some results may be missing due to backends being down. This is likely transient and due to a rollout, so retry your search.".to_owned(),
        severity: "warn",
        suggested: None,
    })
}

fn excluded_fork_handler(stats: &ProgressStats) -> Option<Skipped> {
    let forks = stats.excluded_forks;
    if forks == 0 {
        return None;
    }
    let amount = number(forks);
    Some(Skipped {
        reason: "repository-fork".to_owned(),
        title: format!("{amount} forked"),
        message:
            "By default we exclude forked repositories. Include them with `fork:yes` in your query."
                .to_owned(),
        severity: "info",
        suggested: Some(SkippedSuggested {
            title: "include forked".to_owned(),
            query_expression: "fork:yes".to_owned(),
        }),
    })
}

fn excluded_archive_handler(stats: &ProgressStats) -> Option<Skipped> {
    let archived = stats.excluded_archived;
    if archived == 0 {
        return None;
    }
    let amount = number(archived);
    Some(Skipped {
        reason: "excluded-archive".to_owned(),
        title: format!("{amount} archived"),
        message:
            "By default we exclude archived repositories. Include them with `archived:yes` in your query."
                .to_owned(),
        severity: "info",
        suggested: Some(SkippedSuggested {
            title: "include archived".to_owned(),
            query_expression: "archived:yes".to_owned(),
        }),
    })
}

fn display_limit_handler(stats: &ProgressStats) -> Option<Skipped> {
    if stats.display_limit >= stats.match_count {
        return None;
    }
    let result = if stats.display_limit == 1 {
        "result"
    } else {
        "results"
    };
    Some(Skipped {
        reason: "display".to_owned(),
        title: "display limit hit".to_owned(),
        message: format!(
            "We only display {} {result} even if your search returned more results. To see all results, use our CLI.",
            stats.display_limit
        ),
        severity: "info",
        suggested: None,
    })
}

/// `api.number`.
fn number(i: i64) -> String {
    if i < 1000 {
        return i.to_string();
    }
    if i < 10000 {
        return format!("{},{:03}", i / 1000, i % 1000);
    }
    format!("{}k", i / 1000)
}

/// `api.plural`.
fn plural(one: &'static str, many: &'static str, n: i64) -> &'static str {
    if n == 1 {
        one
    } else {
        many
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn number_matches_go() {
        assert_eq!(number(1), "1");
        assert_eq!(number(999), "999");
        assert_eq!(number(1000), "1,000");
        assert_eq!(number(9999), "9,999");
        assert_eq!(number(10000), "10k");
    }

    #[test]
    fn repo_status_filter_sorts_ids() {
        let mut map = RepoStatusMap::default();
        map.update(5, repo_status::MISSING);
        map.update(2, repo_status::MISSING);
        map.update(3, repo_status::CLONING);
        assert_eq!(map.filter(repo_status::MISSING), vec![2, 5]);
        assert!(map.any(repo_status::MISSING));
        assert!(!map.any(repo_status::TIMED_OUT));
    }
}
