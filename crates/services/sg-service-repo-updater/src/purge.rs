#![forbid(unsafe_code)]

//! Port of `cmd/repo-updater/internal/purge`: deletes repository clones on
//! gitserver whose repositories are soft-deleted (after a TTL) or blocked.
//!
//! The periodic worker is configured by `siteConfig.repoPurgeWorker`
//! (default 15 minute interval, 60 minute TTL; `intervalMinutes <= 0`
//! disables it) and is gated by `DISABLE_REPO_PURGE`. `/manual-purge` on the
//! debug server starts a one-off purge of the oldest repos, bounded by
//! `limit` and a per-second rate.
//!
//! The baseline's Prometheus counters (`src_repoupdater_purge_success` /
//! `_failed`) are incremented per repository, like the Go worker.

use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use sg_config::LiveConfig;
use sg_db::gitserver_repos::{GitserverReposStore, ListPurgableReposOptions};
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

use crate::metrics::Metrics;
use crate::scheduler::RepoUpdateClient;

/// `rate.NewLimiter(10, 1)` for the periodic worker.
pub const PURGE_RATE_PER_SECOND: f64 = 10.0;
/// `ListPurgableReposOptions.Limit` for the periodic worker.
pub const PURGE_LIMIT: i64 = 5000;
/// `randSleepDuration`'s jitter.
pub const PURGE_JITTER: Duration = Duration::from_secs(60);

/// A token-bucket limiter equivalent to `golang.org/x/time/rate` with a
/// burst of one: the first call is immediate, then calls are spaced by
/// `1 / per_second`.
pub struct Limiter {
    interval: Duration,
    next: Mutex<Instant>,
}

impl Limiter {
    pub fn new(per_second: f64) -> Self {
        let interval = if per_second > 0.0 {
            Duration::from_secs_f64(1.0 / per_second)
        } else {
            Duration::ZERO
        };
        Self {
            interval,
            next: Mutex::new(Instant::now()),
        }
    }

    pub async fn wait(&self) {
        let deadline = {
            let mut next = self.next.lock().await;
            let now = Instant::now();
            let at = (*next).max(now);
            *next = at + self.interval;
            at
        };
        let delay = deadline.saturating_duration_since(Instant::now());
        if !delay.is_zero() {
            tokio::time::sleep(delay).await;
        }
    }
}

/// `purge.NewRepositoryPurgeWorker` plus `PurgeOldestRepos`.
pub struct PurgeWorker {
    gitserver_repos: GitserverReposStore,
    gitserver: Arc<dyn RepoUpdateClient>,
    live: Arc<LiveConfig>,
    limiter: Limiter,
    metrics: Arc<Metrics>,
}

impl PurgeWorker {
    pub fn new(
        gitserver_repos: GitserverReposStore,
        gitserver: Arc<dyn RepoUpdateClient>,
        live: Arc<LiveConfig>,
        metrics: Arc<Metrics>,
    ) -> Self {
        Self {
            gitserver_repos,
            gitserver,
            live,
            limiter: Limiter::new(PURGE_RATE_PER_SECOND),
            metrics,
        }
    }

    /// Spawn the periodic worker. `intervalMinutes <= 0` disables each run
    /// (the loop still ticks, like the baseline).
    pub fn spawn(self: Arc<Self>, shutdown: CancellationToken, tasks: &TaskTracker) {
        tasks.spawn(async move { self.run(shutdown).await });
    }

    async fn run(&self, shutdown: CancellationToken) {
        // `timeToNextPurge` starts at zero; the first handler runs
        // immediately (periodic goroutines have no initial delay).
        let mut time_to_next_purge = Duration::ZERO;
        loop {
            let config = self.live.current().site.repo_purge_worker();
            if config.interval_minutes <= 0 {
                tracing::debug!(
                    interval = config.interval_minutes,
                    "purge worker disabled via site config"
                );
            } else {
                time_to_next_purge = Duration::from_secs(config.interval_minutes as u64 * 60);
                let deleted_before = deleted_before(config.deleted_ttl_minutes);
                tracing::debug!(
                    deleted_before = ?deleted_before,
                    time_to_next_purge = ?time_to_next_purge,
                    "running repository purge"
                );
                if let Err(err) = self
                    .purge(
                        Some(sg_db::OffsetDateTime::from(deleted_before)),
                        PURGE_LIMIT,
                        &self.limiter,
                    )
                    .await
                {
                    tracing::error!(error = %err, "failed to run repository clone purge");
                }
            }

            let sleep = rand_sleep_duration(time_to_next_purge, PURGE_JITTER);
            tokio::select! {
                _ = shutdown.cancelled() => return,
                _ = tokio::time::sleep(sleep) => {}
            }
        }
    }

    /// `PurgeOldestRepos`: start a one-off purge in the background. `limit`
    /// must be greater than zero.
    pub fn purge_oldest_repos(
        self: &Arc<Self>,
        limit: i64,
        per_second: f64,
        tasks: &TaskTracker,
    ) -> Result<(), String> {
        if limit <= 0 {
            return Err(format!("limit must be greater than zero, got {limit}"));
        }
        let worker = Arc::clone(self);
        tasks.spawn(async move {
            // Use a detached context in the baseline so that the request
            // timeout does not cancel the purge; the service tracker keeps it
            // bounded by process shutdown instead.
            let limiter = Limiter::new(per_second);
            if let Err(err) = worker.purge(None, limit, &limiter).await {
                tracing::error!(error = %err, "Purging old repos");
            }
        });
        Ok(())
    }

    /// `purge`: delete each listed repo's clone, logging per-repo failures
    /// and continuing.
    async fn purge(
        &self,
        deleted_before: Option<sg_db::OffsetDateTime>,
        limit: i64,
        limiter: &Limiter,
    ) -> Result<(), String> {
        let start = Instant::now();
        let options = ListPurgableReposOptions {
            deleted_before,
            limit: Some(limit),
        };
        let repos = self
            .gitserver_repos
            .list_purgeable_repos(&options)
            .await
            .map_err(|err| format!("listing purgeable repos: {err}"))?;

        let mut total = 0usize;
        let mut success = 0usize;
        let mut failed = 0usize;
        for repo in repos {
            limiter.wait().await;
            total += 1;
            if let Err(err) = self.gitserver.delete_repository(&repo).await {
                tracing::error!(repo = %repo, error = %err, "failed to remove repository");
                self.metrics.purge_failed.inc();
                failed += 1;
                continue;
            }
            success += 1;
            self.metrics.purge_success.inc();
        }

        if failed > 0 {
            tracing::warn!(
                total,
                removed = success,
                failed,
                duration = ?start.elapsed(),
                "repository purge finished"
            );
        } else {
            tracing::info!(
                total,
                removed = success,
                failed,
                duration = ?start.elapsed(),
                "repository purge finished"
            );
        }
        Ok(())
    }
}

/// `time.Now().Add(-ttl)`; a negative TTL moves the cutoff into the future.
fn deleted_before(ttl_minutes: i64) -> SystemTime {
    let now = SystemTime::now();
    let minutes = ttl_minutes.unsigned_abs();
    let delta = Duration::from_secs(minutes.saturating_mul(60));
    if ttl_minutes >= 0 {
        now.checked_sub(delta).unwrap_or(UNIX_EPOCH)
    } else {
        now + delta
    }
}

/// `randSleepDuration`: sleep `d` with a jitter of `[-jitter/2, jitter/2]`,
/// clamped at zero (a negative duration fires `clock.After` immediately).
pub fn rand_sleep_duration(d: Duration, jitter: Duration) -> Duration {
    let span = jitter.as_nanos() as u64;
    if span == 0 {
        return d;
    }
    let entropy = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.subsec_nanos() as u64 ^ elapsed.as_secs())
        .unwrap_or(0);
    let offset = (entropy % span) as i128 - (span / 2) as i128;
    let total = d.as_nanos() as i128 + offset;
    if total <= 0 {
        Duration::ZERO
    } else {
        Duration::from_nanos(total as u64)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn jitter_stays_within_the_baseline_bounds() {
        let d = Duration::from_secs(600);
        let jitter = Duration::from_secs(60);
        for _ in 0..100 {
            let sleep = rand_sleep_duration(d, jitter);
            assert!(sleep >= Duration::from_secs(570), "{sleep:?}");
            assert!(sleep <= Duration::from_secs(630), "{sleep:?}");
        }
    }

    #[test]
    fn negative_ttl_moves_the_cutoff_forward() {
        let now = SystemTime::now();
        let cutoff = deleted_before(-60);
        assert!(cutoff > now, "{cutoff:?} <= {now:?}");
    }

    #[tokio::test]
    async fn limiter_spaces_calls() {
        let limiter = Limiter::new(100.0); // 10ms apart
        let start = Instant::now();
        limiter.wait().await;
        limiter.wait().await;
        limiter.wait().await;
        assert!(
            start.elapsed() >= Duration::from_millis(18),
            "{:?}",
            start.elapsed()
        );
    }

    struct FakeGitserver;

    #[async_trait::async_trait]
    impl RepoUpdateClient for FakeGitserver {
        async fn fetch_repository(
            &self,
            _repo: &str,
        ) -> Result<(SystemTime, SystemTime), tonic::Status> {
            Err(tonic::Status::unavailable("unused"))
        }

        async fn delete_repository(&self, repo: &str) -> Result<(), tonic::Status> {
            if repo.contains("/fail") {
                return Err(tonic::Status::internal("boom"));
            }
            Ok(())
        }
    }

    fn live_config(site: &str) -> Arc<LiveConfig> {
        let unified = sg_config::Unified::parse(&sg_config::RawUnified {
            site: site.to_owned(),
            ..Default::default()
        })
        .expect("site config");
        Arc::new(LiveConfig::new(unified))
    }

    /// `purgeSuccess`/`purgeFailed` are incremented per repository.
    #[tokio::test]
    async fn purge_counts_successes_and_failures() {
        let Ok(dsn) = std::env::var("SG_TEST_DATABASE_DSN") else {
            eprintln!("SG_TEST_DATABASE_DSN unset; skipping");
            return;
        };
        let pool = sg_db::connect(&dsn, 5).await.expect("connect");
        let suffix = format!(
            "purge-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("clock")
                .as_nanos()
        );
        let mut ids = Vec::new();
        for leaf in ["ok", "fail"] {
            let id: i32 = sqlx::query_scalar("INSERT INTO repo(name) VALUES ($1) RETURNING id")
                .bind(format!("{suffix}.example.com/{leaf}"))
                .fetch_one(&pool)
                .await
                .expect("insert repo");
            sqlx::query(
                "UPDATE repo SET deleted_at = clock_timestamp() - interval '3 hours' WHERE id = $1",
            )
            .bind(id)
            .execute(&pool)
            .await
            .expect("soft delete");
            sqlx::query("UPDATE gitserver_repos SET clone_status = 'cloned' WHERE repo_id = $1")
                .bind(id)
                .execute(&pool)
                .await
                .expect("clone status");
            ids.push(id);
        }

        let cutoff = sg_db::OffsetDateTime::now_utc() - time::Duration::hours(1);
        let store = GitserverReposStore::new(pool.clone());
        let candidates = store
            .list_purgeable_repos(&ListPurgableReposOptions {
                deleted_before: Some(cutoff),
                limit: None,
            })
            .await
            .expect("candidates");
        if candidates.iter().any(|name| !name.contains(&suffix)) {
            // Leftover deleted repos from another run would skew the counts.
            eprintln!("foreign purgeable repos present; skipping: {candidates:?}");
            sqlx::query("DELETE FROM repo WHERE id = ANY($1)")
                .bind(&ids)
                .execute(&pool)
                .await
                .expect("cleanup");
            return;
        }

        let metrics = Arc::new(Metrics::new());
        let worker = PurgeWorker::new(
            store,
            Arc::new(FakeGitserver),
            live_config("{}"),
            Arc::clone(&metrics),
        );
        worker
            .purge(Some(cutoff), 10, &Limiter::new(1000.0))
            .await
            .expect("purge");
        assert_eq!(metrics.purge_success.get(), 1.0);
        assert_eq!(metrics.purge_failed.get(), 1.0);

        sqlx::query("DELETE FROM repo WHERE id = ANY($1)")
            .bind(&ids)
            .execute(&pool)
            .await
            .expect("cleanup");
    }
}
