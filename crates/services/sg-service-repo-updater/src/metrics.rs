#![forbid(unsafe_code)]

//! Prometheus families exposed by repo-updater.
//!
//! Sources:
//!
//! - `cmd/repo-updater/internal/scheduler/metrics.go` (`src_repoupdater_sched_*`)
//! - `internal/repos/metrics.go` (`src_repoupdater_syncer_*`)
//! - `internal/repos/observability.go` (`src_repoupdater_source_*`; the
//!   `GetRepo` families have no code path because no implemented source kind
//!   is a `RepoGetter`)
//! - `cmd/repo-updater/internal/purge/purge.go` (`src_repoupdater_purge_*`)
//! - `cmd/repo-updater/shared/main.go` `mustRegisterMetrics` (the
//!   scrape-time `GaugeFunc`s refreshed by [`Metrics::refresh_state`])
//!
//! Registration is best-effort: tests build several metric sets in one
//! process and only the first registration owns the family in the default
//! registry. Tests read the handles directly, so they do not depend on
//! registration order.

use sg_metrics::prometheus::{
    Counter, CounterVec, Gauge, GaugeVec, HistogramOpts, HistogramVec, Opts,
};
use sqlx::postgres::PgPool;

/// `Syncer.observeSync`'s family label.
pub const SYNC_FAMILY: &str = "Syncer.SyncExternalService";

/// `ownerUndefined` / `ownerSite` (`internal/repos/syncer.go`).
pub const OWNER_UNDEFINED: &str = "";
pub const OWNER_SITE: &str = "site";

/// `src_repoupdater_*` families.
pub struct Metrics {
    // `cmd/repo-updater/internal/scheduler/metrics.go`.
    pub sched_error: CounterVec,
    pub sched_loops: Counter,
    pub sched_auto_fetch: Counter,
    pub sched_manual_fetch: Counter,
    pub sched_known_repos: Gauge,
    pub sched_update_queue_length: Gauge,
    // `internal/repos/metrics.go`.
    pub syncer_sync_last_time: GaugeVec,
    pub syncer_start_sync: CounterVec,
    pub syncer_sync_errors_total: CounterVec,
    pub syncer_sync_duration_seconds: HistogramVec,
    pub syncer_synced_repos_total: CounterVec,
    // `internal/repos/observability.go` (`SourceMetrics.ListRepos`). Go
    // declares these as RED metrics with an empty label set, so a series only
    // appears once `Observe` has touched it; the vectors keep that laziness.
    pub source_duration_seconds: HistogramVec,
    pub source_repos_total: CounterVec,
    pub source_errors_total: CounterVec,
    // `cmd/repo-updater/internal/purge/purge.go`.
    pub purge_success: Counter,
    pub purge_failed: Counter,
    // `mustRegisterMetrics` scrape-time gauges.
    pub external_services_total: Gauge,
    pub queued_sync_jobs_total: Gauge,
    pub completed_sync_jobs_total: Gauge,
    pub errored_sync_jobs_percentage: Gauge,
    pub max_sync_backoff: Gauge,
    pub stale_repos: Gauge,
}

impl Metrics {
    /// Build and register every family.
    pub fn new() -> Self {
        let sched_error = CounterVec::new(
            Opts::new(
                "src_repoupdater_sched_error",
                "Incremented each time we encounter an error updating a repository.",
            ),
            &["type"],
        )
        .expect("valid metric");
        let sched_loops = Counter::new(
            "src_repoupdater_sched_loops",
            "Incremented each time the scheduler loops.",
        )
        .expect("valid metric");
        let sched_auto_fetch = Counter::new(
            "src_repoupdater_sched_auto_fetch",
            "Incremented each time the scheduler updates a managed repository due to hitting a deadline.",
        )
        .expect("valid metric");
        let sched_manual_fetch = Counter::new(
            "src_repoupdater_sched_manual_fetch",
            "Incremented each time the scheduler updates a repository due to user traffic.",
        )
        .expect("valid metric");
        let sched_known_repos = Gauge::new(
            "src_repoupdater_sched_known_repos",
            "The number of repositories that are managed by the scheduler.",
        )
        .expect("valid metric");
        let sched_update_queue_length = Gauge::new(
            "src_repoupdater_sched_update_queue_length",
            "The number of repositories that are currently queued for update",
        )
        .expect("valid metric");

        let syncer_sync_last_time = GaugeVec::new(
            Opts::new(
                "src_repoupdater_syncer_sync_last_time",
                "The last time a sync finished",
            ),
            &["family"],
        )
        .expect("valid metric");
        let syncer_start_sync = CounterVec::new(
            Opts::new("src_repoupdater_syncer_start_sync", "A sync was started"),
            &["family", "owner"],
        )
        .expect("valid metric");
        let syncer_sync_errors_total = CounterVec::new(
            Opts::new(
                "src_repoupdater_syncer_sync_errors_total",
                "Total number of sync errors",
            ),
            &["family", "owner", "reason"],
        )
        .expect("valid metric");
        let syncer_sync_duration_seconds = HistogramVec::new(
            HistogramOpts::new(
                "src_repoupdater_syncer_sync_duration_seconds",
                "Time spent syncing",
            ),
            &["success", "family"],
        )
        .expect("valid metric");
        let syncer_synced_repos_total = CounterVec::new(
            Opts::new(
                "src_repoupdater_syncer_synced_repos_total",
                "Total number of synced repositories",
            ),
            &["state"],
        )
        .expect("valid metric");

        let source_duration_seconds = HistogramVec::new(
            HistogramOpts::new(
                "src_repoupdater_source_duration_seconds",
                "Time spent sourcing repos",
            ),
            &[],
        )
        .expect("valid metric");
        let source_repos_total = CounterVec::new(
            Opts::new(
                "src_repoupdater_source_repos_total",
                "Total number of sourced repositories",
            ),
            &[],
        )
        .expect("valid metric");
        let source_errors_total = CounterVec::new(
            Opts::new(
                "src_repoupdater_source_errors_total",
                "Total number of sourcing errors",
            ),
            &[],
        )
        .expect("valid metric");

        let purge_success = Counter::new(
            "src_repoupdater_purge_success",
            "Incremented each time we remove a repository clone.",
        )
        .expect("valid metric");
        let purge_failed = Counter::new(
            "src_repoupdater_purge_failed",
            "Incremented each time we try and fail to remove a repository clone.",
        )
        .expect("valid metric");

        let external_services_total = Gauge::new(
            "src_repoupdater_external_services_total",
            "The total number of external services added",
        )
        .expect("valid metric");
        let queued_sync_jobs_total = Gauge::new(
            "src_repoupdater_queued_sync_jobs_total",
            "The total number of queued sync jobs",
        )
        .expect("valid metric");
        let completed_sync_jobs_total = Gauge::new(
            "src_repoupdater_completed_sync_jobs_total",
            "The total number of completed sync jobs",
        )
        .expect("valid metric");
        let errored_sync_jobs_percentage = Gauge::new(
            "src_repoupdater_errored_sync_jobs_percentage",
            "The percentage of external services that have failed their most recent sync",
        )
        .expect("valid metric");
        let max_sync_backoff = Gauge::new(
            "src_repoupdater_max_sync_backoff",
            "The maximum number of seconds since any external service synced",
        )
        .expect("valid metric");
        let stale_repos = Gauge::new(
            "src_repoupdater_stale_repos",
            "The number of repos that haven't been fetched in at least 8 hours",
        )
        .expect("valid metric");

        let registry = sg_metrics::registry();
        for family in [
            Box::new(sched_error.clone()) as Box<dyn sg_metrics::prometheus::core::Collector>,
            Box::new(sched_loops.clone()),
            Box::new(sched_auto_fetch.clone()),
            Box::new(sched_manual_fetch.clone()),
            Box::new(sched_known_repos.clone()),
            Box::new(sched_update_queue_length.clone()),
            Box::new(syncer_sync_last_time.clone()),
            Box::new(syncer_start_sync.clone()),
            Box::new(syncer_sync_errors_total.clone()),
            Box::new(syncer_sync_duration_seconds.clone()),
            Box::new(syncer_synced_repos_total.clone()),
            Box::new(source_duration_seconds.clone()),
            Box::new(source_repos_total.clone()),
            Box::new(source_errors_total.clone()),
            Box::new(purge_success.clone()),
            Box::new(purge_failed.clone()),
            Box::new(external_services_total.clone()),
            Box::new(queued_sync_jobs_total.clone()),
            Box::new(completed_sync_jobs_total.clone()),
            Box::new(errored_sync_jobs_percentage.clone()),
            Box::new(max_sync_backoff.clone()),
            Box::new(stale_repos.clone()),
        ] {
            let _ = registry.register(family);
        }

        Self {
            sched_error,
            sched_loops,
            sched_auto_fetch,
            sched_manual_fetch,
            sched_known_repos,
            sched_update_queue_length,
            syncer_sync_last_time,
            syncer_start_sync,
            syncer_sync_errors_total,
            syncer_sync_duration_seconds,
            syncer_synced_repos_total,
            source_duration_seconds,
            source_repos_total,
            source_errors_total,
            purge_success,
            purge_failed,
            external_services_total,
            queued_sync_jobs_total,
            completed_sync_jobs_total,
            errored_sync_jobs_percentage,
            max_sync_backoff,
            stale_repos,
        }
    }

    pub fn inc_error(&self, kind: &str) {
        self.sched_error.with_label_values(&[kind]).inc();
    }

    /// `observeDiff`: adds one sample per diff state. Zero counts still create
    /// the label series, as `CounterVec.Add(0)` does in the baseline.
    pub fn observe_diff(&self, diff: &sg_extsvc::RepoSyncDiff) {
        for (state, count) in [
            ("added", diff.added.len()),
            ("modified", diff.modified.len()),
            ("deleted", diff.deleted.len()),
            ("unmodified", diff.unmodified.len()),
        ] {
            self.syncer_synced_repos_total
                .with_label_values(&[state])
                .inc_by(count as f64);
        }
    }

    /// `metrics.REDMetrics.Observe` as called by `ObservedSource.ListRepos`:
    /// on error only the error counter increments (and the count series is
    /// created with `Add(0)`), and **no duration sample is recorded**; on
    /// success the duration is observed and the count is added.
    pub fn observe_source(&self, seconds: f64, count: usize, had_error: bool) {
        if had_error {
            self.source_errors_total.with_label_values(&[]).inc();
            self.source_repos_total.with_label_values(&[]).inc_by(0.0);
        } else {
            self.source_duration_seconds
                .with_label_values(&[])
                .observe(seconds);
            self.source_repos_total
                .with_label_values(&[])
                .inc_by(count as f64);
        }
    }

    /// `observeSync`'s deferred half: after a `SyncExternalService` call.
    pub fn observe_sync_finished(
        &self,
        success: bool,
        seconds: f64,
        now_unix: f64,
        owner: &str,
        reason: &str,
    ) {
        self.syncer_start_sync
            .with_label_values(&[SYNC_FAMILY, owner])
            .inc();
        self.syncer_sync_last_time
            .with_label_values(&[SYNC_FAMILY])
            .set(now_unix);
        self.syncer_sync_duration_seconds
            .with_label_values(&[if success { "true" } else { "false" }, SYNC_FAMILY])
            .observe(seconds);
        if !success {
            self.syncer_sync_errors_total
                .with_label_values(&[SYNC_FAMILY, owner, reason])
                .inc();
        }
    }

    /// `mustRegisterMetrics`: recompute the scrape-time gauges. Every query is
    /// independent; a failure logs and publishes `0`, like the baseline's
    /// `GaugeFunc`s.
    pub async fn refresh_state(&self, pool: &PgPool) {
        macro_rules! refresh {
            ($gauge:expr, $label:literal, $future:expr) => {
                match $future.await {
                    Ok(value) => $gauge.set(value),
                    Err(err) => {
                        tracing::error!(error = %err, concat!("failed to get ", $label));
                        $gauge.set(0.0);
                    }
                }
            };
        }

        refresh!(
            self.external_services_total,
            "total external services",
            sg_db::repoupdater_state::external_services_total(pool)
        );
        refresh!(
            self.queued_sync_jobs_total,
            "total queued sync jobs",
            sg_db::repoupdater_state::queued_sync_jobs_total(pool)
        );
        refresh!(
            self.completed_sync_jobs_total,
            "total completed sync jobs",
            sg_db::repoupdater_state::completed_sync_jobs_total(pool)
        );
        refresh!(
            self.errored_sync_jobs_percentage,
            "total errored sync jobs",
            sg_db::repoupdater_state::errored_sync_jobs_percentage(pool)
        );
        refresh!(
            self.max_sync_backoff,
            "max sync backoff",
            sg_db::repoupdater_state::max_sync_backoff(pool)
        );
        refresh!(
            self.stale_repos,
            "stale repos",
            sg_db::repoupdater_state::stale_repos(pool)
        );
    }
}

impl Default for Metrics {
    fn default() -> Self {
        Self::new()
    }
}

/// `syncErrorReason`: our source errors are strings without `errcode`
/// classification, so the Go error predicates degrade to substring checks and
/// everything else is `unknown` (documented divergence).
pub fn sync_error_reason(message: &str) -> &'static str {
    if message.contains("expected path in npm/") {
        "invalid_npm_path"
    } else if message.contains("internal rate limit exceeded") {
        "internal_rate_limit"
    } else {
        "unknown"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn error_reasons_fall_back_to_unknown() {
        assert_eq!(sync_error_reason("boom"), "unknown");
        assert_eq!(
            sync_error_reason("expected path in npm/foo"),
            "invalid_npm_path"
        );
        assert_eq!(
            sync_error_reason("internal rate limit exceeded"),
            "internal_rate_limit"
        );
    }

    #[test]
    fn syncer_counters_and_histograms_are_observed() {
        let metrics = Metrics::new();
        metrics.observe_sync_finished(true, 1.5, 1_700_000_000.0, OWNER_SITE, "");
        metrics.observe_sync_finished(false, 2.5, 1_700_000_100.0, OWNER_SITE, "unknown");
        metrics.observe_sync_finished(false, 0.5, 1_700_000_200.0, OWNER_UNDEFINED, "unknown");

        assert_eq!(
            metrics
                .syncer_start_sync
                .with_label_values(&[SYNC_FAMILY, OWNER_SITE])
                .get(),
            2.0
        );
        assert_eq!(
            metrics
                .syncer_start_sync
                .with_label_values(&[SYNC_FAMILY, OWNER_UNDEFINED])
                .get(),
            1.0
        );
        assert_eq!(
            metrics
                .syncer_sync_last_time
                .with_label_values(&[SYNC_FAMILY])
                .get(),
            1_700_000_200.0
        );
        assert_eq!(
            metrics
                .syncer_sync_errors_total
                .with_label_values(&[SYNC_FAMILY, OWNER_SITE, "unknown"])
                .get(),
            1.0
        );
        assert_eq!(
            metrics
                .syncer_sync_duration_seconds
                .with_label_values(&["true", SYNC_FAMILY])
                .get_sample_count(),
            1
        );
        assert_eq!(
            metrics
                .syncer_sync_duration_seconds
                .with_label_values(&["false", SYNC_FAMILY])
                .get_sample_count(),
            2
        );
    }

    /// The text exposition must carry the Go family names (and their
    /// `_bucket`/`_count`/`_sum` histograms) so existing dashboards keep
    /// working after the switch.
    #[test]
    fn families_render_with_the_baseline_names() {
        let metrics = Metrics::new();
        // Give every family at least one sample; empty vecs are not rendered.
        metrics.inc_error("update");
        metrics.sched_loops.inc();
        metrics.sched_auto_fetch.inc();
        metrics.sched_manual_fetch.inc();
        metrics.sched_known_repos.set(1.0);
        metrics.sched_update_queue_length.set(1.0);
        metrics.observe_sync_finished(true, 1.0, 1_700_000_000.0, OWNER_SITE, "");
        metrics.observe_sync_finished(false, 1.0, 1_700_000_000.0, OWNER_SITE, "unknown");
        let mut diff = sg_extsvc::RepoSyncDiff::default();
        diff.added.push(sg_extsvc::Repo::default());
        metrics.observe_diff(&diff);
        // One successful and one failed `Observe` call create all three
        // source series.
        metrics.observe_source(0.1, 1, false);
        metrics.observe_source(0.1, 1, true);
        metrics.purge_success.inc();
        metrics.purge_failed.inc();
        metrics.external_services_total.set(1.0);
        metrics.queued_sync_jobs_total.set(1.0);
        metrics.completed_sync_jobs_total.set(1.0);
        metrics.errored_sync_jobs_percentage.set(1.0);
        metrics.max_sync_backoff.set(1.0);
        metrics.stale_repos.set(1.0);

        // A local registry, so the test does not depend on which `Metrics`
        // instance won the (best-effort) registration in the process-wide
        // registry.
        let registry = sg_metrics::prometheus::Registry::new();
        for collector in [
            Box::new(metrics.sched_error.clone())
                as Box<dyn sg_metrics::prometheus::core::Collector>,
            Box::new(metrics.sched_loops.clone()),
            Box::new(metrics.sched_auto_fetch.clone()),
            Box::new(metrics.sched_manual_fetch.clone()),
            Box::new(metrics.sched_known_repos.clone()),
            Box::new(metrics.sched_update_queue_length.clone()),
            Box::new(metrics.syncer_sync_last_time.clone()),
            Box::new(metrics.syncer_start_sync.clone()),
            Box::new(metrics.syncer_sync_errors_total.clone()),
            Box::new(metrics.syncer_sync_duration_seconds.clone()),
            Box::new(metrics.syncer_synced_repos_total.clone()),
            Box::new(metrics.source_duration_seconds.clone()),
            Box::new(metrics.source_repos_total.clone()),
            Box::new(metrics.source_errors_total.clone()),
            Box::new(metrics.purge_success.clone()),
            Box::new(metrics.purge_failed.clone()),
            Box::new(metrics.external_services_total.clone()),
            Box::new(metrics.queued_sync_jobs_total.clone()),
            Box::new(metrics.completed_sync_jobs_total.clone()),
            Box::new(metrics.errored_sync_jobs_percentage.clone()),
            Box::new(metrics.max_sync_backoff.clone()),
            Box::new(metrics.stale_repos.clone()),
        ] {
            registry.register(collector).expect("register");
        }
        let text = sg_metrics::render(&registry);
        for family in [
            "src_repoupdater_sched_error",
            "src_repoupdater_sched_loops",
            "src_repoupdater_sched_auto_fetch",
            "src_repoupdater_sched_manual_fetch",
            "src_repoupdater_sched_known_repos",
            "src_repoupdater_sched_update_queue_length",
            "src_repoupdater_syncer_sync_last_time",
            "src_repoupdater_syncer_start_sync",
            "src_repoupdater_syncer_sync_errors_total",
            "src_repoupdater_syncer_sync_duration_seconds",
            "src_repoupdater_syncer_synced_repos_total",
            "src_repoupdater_source_duration_seconds",
            "src_repoupdater_source_repos_total",
            "src_repoupdater_source_errors_total",
            "src_repoupdater_purge_success",
            "src_repoupdater_purge_failed",
            "src_repoupdater_external_services_total",
            "src_repoupdater_queued_sync_jobs_total",
            "src_repoupdater_completed_sync_jobs_total",
            "src_repoupdater_errored_sync_jobs_percentage",
            "src_repoupdater_max_sync_backoff",
            "src_repoupdater_stale_repos",
        ] {
            assert!(
                text.contains(&format!("# TYPE {family} ")),
                "missing family {family}"
            );
        }
    }
}
