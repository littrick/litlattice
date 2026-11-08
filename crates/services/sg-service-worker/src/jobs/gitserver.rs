#![forbid(unsafe_code)]

//! `cmd/worker/internal/gitserver/servermetrics.go`: the `gitserver-metrics`
//! job.
//!
//! The baseline job registers two scrape-time gauges with
//! `prometheus.NewGaugeFunc` and returns no routines at all: the values are
//! computed from `repo_statistics` / `gitserver_repos_statistics` on every
//! scrape of `SRC_PROF_HTTP/metrics`, with a 5 second query timeout and a
//! logged zero value on error.
//!
//! Rust equivalent: `prometheus::PullingGauge` is the upstream port of Go's
//! `GaugeFunc` (same crate lineage), so each scrape runs the query again. The
//! gauge callback is synchronous, so the query is executed with
//! `Handle::block_on` inside `block_in_place`; that is only possible on a
//! multi-thread runtime (the service runtime), and a scrape from any other
//! context returns `0` (see docs/modules/06_worker.md §18 errata).

use std::sync::OnceLock;
use std::time::Duration;

use async_trait::async_trait;
use sg_metrics::prometheus::PullingGauge;
use sqlx::PgPool;
use tokio::runtime::RuntimeFlavor;
use tokio_util::sync::CancellationToken;

use super::{Job, JobDeps};
use crate::framework::BackgroundRoutine;

/// `src_gitserver_repo_last_error_total` (`servermetrics.go`).
pub const REPO_LAST_ERROR_TOTAL: &str = "src_gitserver_repo_last_error_total";
pub const REPO_LAST_ERROR_TOTAL_HELP: &str =
    "Number of repositories whose last_error column is not empty.";
pub const REPO_LAST_ERROR_TOTAL_SQL: &str =
    "SELECT COALESCE(SUM(failed_fetch), 0)::bigint FROM gitserver_repos_statistics";
pub const REPO_LAST_ERROR_TOTAL_ERROR: &str = "failed to count repository errors";

/// `src_gitserver_repo_count` (`servermetrics.go`).
pub const REPO_COUNT: &str = "src_gitserver_repo_count";
pub const REPO_COUNT_HELP: &str = "Number of repos.";
pub const REPO_COUNT_SQL: &str = "SELECT COALESCE(SUM(total), 0)::bigint FROM repo_statistics";
pub const REPO_COUNT_ERROR: &str = "failed to count repositories";

/// `context.WithTimeout(context.Background(), 5*time.Second)`.
pub const QUERY_TIMEOUT: Duration = Duration::from_secs(5);

/// `metricsJob`.
pub struct GitserverMetrics;

#[async_trait]
impl Job for GitserverMetrics {
    fn description(&self) -> String {
        // The baseline returns "" for this job.
        String::new()
    }

    async fn routines(
        &self,
        deps: &JobDeps,
        _setup_ctx: CancellationToken,
    ) -> Result<Vec<BackgroundRoutine>, String> {
        register_metrics(deps.frontend.clone());
        // `return nil, nil`: the job owns no routines.
        Ok(Vec::new())
    }
}

/// Register both gauges once per process (`prometheus.MustRegister`).
///
/// The baseline panics on a duplicate registration; the metric values are
/// process-wide, so the gauges are created lazily exactly once here. The pool
/// of the first call backs every later scrape, matching the baseline's
/// memoized `workerdb.InitDB`.
fn register_metrics(pool: PgPool) {
    static REGISTERED: OnceLock<()> = OnceLock::new();

    REGISTERED.get_or_init(|| {
        let registry = sg_metrics::registry();
        let gauges = [
            (
                REPO_LAST_ERROR_TOTAL,
                REPO_LAST_ERROR_TOTAL_HELP,
                REPO_LAST_ERROR_TOTAL_SQL,
                REPO_LAST_ERROR_TOTAL_ERROR,
            ),
            (
                REPO_COUNT,
                REPO_COUNT_HELP,
                REPO_COUNT_SQL,
                REPO_COUNT_ERROR,
            ),
        ];

        for (name, help, sql, error_message) in gauges {
            let pool = pool.clone();
            let gauge = PullingGauge::new(
                name,
                help,
                Box::new(move || scrape_count(&pool, sql, error_message)),
            )
            .expect("valid gitserver metrics gauge");
            // Duplicate registration cannot happen: the gauges are created
            // once, like the baseline's single `NewMetricsJob` instance.
            let _ = registry.register(Box::new(gauge));
        }
    });
}

/// The `GaugeFunc` body: run the aggregate query with the baseline timeout,
/// log and return 0 on error.
fn scrape_count(pool: &PgPool, sql: &'static str, error_message: &'static str) -> f64 {
    let Ok(handle) = tokio::runtime::Handle::try_current() else {
        return 0.0;
    };
    // `block_in_place` is only available on the multi-threaded runtime; the
    // service always scrapes from it. Any other context falls back to 0 so a
    // collector never panics the metrics endpoint.
    if !matches!(handle.runtime_flavor(), RuntimeFlavor::MultiThread) {
        return 0.0;
    }

    tokio::task::block_in_place(|| {
        handle.block_on(async {
            match tokio::time::timeout(
                QUERY_TIMEOUT,
                sqlx::query_scalar::<_, i64>(sql).fetch_one(pool),
            )
            .await
            {
                Ok(Ok(count)) => count as f64,
                Ok(Err(err)) => {
                    tracing::error!(error = %err, "{}", error_message);
                    0.0
                }
                Err(_) => {
                    tracing::error!("{}: query timed out", error_message);
                    0.0
                }
            }
        })
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use sg_config::{LiveConfig, Unified};

    fn deps() -> JobDeps {
        JobDeps {
            frontend: sqlx::postgres::PgPoolOptions::new()
                .connect_lazy("postgres://unused@127.0.0.1:1/unused")
                .expect("lazy pool"),
            live_config: Arc::new(LiveConfig::new(Unified::default())),
            keyring: Arc::new(sg_keyring::Keyring::default()),
        }
    }

    #[test]
    fn job_metadata_matches_the_baseline() {
        assert_eq!(GitserverMetrics.description(), "");
        assert_eq!(
            REPO_LAST_ERROR_TOTAL_HELP,
            "Number of repositories whose last_error column is not empty."
        );
        assert_eq!(REPO_COUNT_HELP, "Number of repos.");
        assert_eq!(QUERY_TIMEOUT, Duration::from_secs(5));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn routines_register_the_two_gauges_and_return_none() {
        let routines = GitserverMetrics
            .routines(&deps(), CancellationToken::new())
            .await
            .expect("routines");
        assert!(routines.is_empty(), "the baseline returns no routines");

        // Scraping with an unreachable database exercises the error path: the
        // families must still be exported, with the zero fallback value.
        let text = sg_metrics::render_default();
        assert!(
            text.contains("# TYPE src_gitserver_repo_last_error_total gauge"),
            "{text}"
        );
        assert!(
            text.contains("# TYPE src_gitserver_repo_count gauge"),
            "{text}"
        );
        assert!(
            text.contains("src_gitserver_repo_last_error_total 0"),
            "{text}"
        );
        assert!(text.contains("src_gitserver_repo_count 0"), "{text}");
    }
}
