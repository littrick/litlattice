#![forbid(unsafe_code)]

//! Port of `newUnclonedReposManager` (`cmd/repo-updater/shared/main.go`):
//! every 30 seconds list the repositories gitserver has not cloned yet and
//! make them due for fetching immediately, as if they were newly added.
//!
//! The baseline runs the routine under an internal actor and skips it while
//! `disableAutoGitUpdates` is set. The database query is
//! `Repos.ListMinimalRepos(ctx, ReposListOptions{NoCloned: true})`.

use std::sync::Arc;
use std::time::Duration;

use sg_config::LiveConfig;
use sg_db::repos::RepoStore;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

use crate::scheduler::{ConfiguredRepo, UpdateScheduler};

/// `goroutine.WithInterval(30*time.Second)`.
pub const INTERVAL: Duration = Duration::from_secs(30);

/// `newUnclonedReposManager`: keeps the scheduler's view of uncloned
/// repositories up to date.
pub struct UnclonedReposManager {
    scheduler: Arc<UpdateScheduler>,
    repos: RepoStore,
    live: Arc<LiveConfig>,
}

impl UnclonedReposManager {
    pub fn new(scheduler: Arc<UpdateScheduler>, repos: RepoStore, live: Arc<LiveConfig>) -> Self {
        Self {
            scheduler,
            repos,
            live,
        }
    }

    /// Spawn the periodic routine. The baseline's periodic goroutines run
    /// their handler immediately, then sleep for the interval.
    pub fn spawn(self: Arc<Self>, shutdown: CancellationToken, tasks: &TaskTracker) {
        tasks.spawn(async move { self.run(shutdown).await });
    }

    async fn run(&self, shutdown: CancellationToken) {
        loop {
            if let Err(err) = self.run_once().await {
                tracing::error!(error = %err, "uncloned repo manager");
            }
            tokio::select! {
                _ = shutdown.cancelled() => return,
                _ = tokio::time::sleep(INTERVAL) => {}
            }
        }
    }

    /// One handler invocation. Public so integration tests can drive it
    /// without waiting for the timer.
    pub async fn run_once(&self) -> Result<(), String> {
        // Don't modify the scheduler if we're not performing auto updates.
        if self.live.current().site.disable_auto_git_updates() {
            return Ok(());
        }

        let uncloned = self
            .repos
            .list_uncloned()
            .await
            .map_err(|err| format!("failed to fetch list of uncloned repositories: {err}"))?;

        let repos: Vec<ConfiguredRepo> = uncloned
            .into_iter()
            .map(|repo| ConfiguredRepo {
                id: repo.id,
                name: repo.name,
            })
            .collect();
        self.scheduler.prioritise_uncloned(&repos);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    use async_trait::async_trait;
    use sqlx::postgres::PgPool;

    use super::*;
    use crate::metrics::Metrics;
    use crate::scheduler::{Clock, RepoStatus, RepoUpdateClient, SchedulerStore};

    struct FakeClock(SystemTime);

    impl Clock for FakeClock {
        fn now(&self) -> SystemTime {
            self.0
        }
    }

    #[derive(Default)]
    struct FakeStore;

    #[async_trait]
    impl SchedulerStore for FakeStore {
        async fn iterate_repo_gitserver_status(
            &self,
            _cursor: i32,
            _batch_size: i64,
        ) -> Result<(Vec<RepoStatus>, i32), String> {
            Ok((Vec::new(), 0))
        }

        async fn set_last_fetched(
            &self,
            _repo: &str,
            _last_fetched: SystemTime,
            _last_changed: SystemTime,
        ) -> Result<(), String> {
            Ok(())
        }
    }

    struct FakeGitserver;

    #[async_trait]
    impl RepoUpdateClient for FakeGitserver {
        async fn fetch_repository(
            &self,
            _repo: &str,
        ) -> Result<(SystemTime, SystemTime), tonic::Status> {
            Err(tonic::Status::unavailable("unused"))
        }

        async fn delete_repository(&self, _repo: &str) -> Result<(), tonic::Status> {
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

    /// Scheduled repositories whose `due` is exactly `now + 45s` (the
    /// baseline's `minDelay`) were prioritised by the manager; the interval
    /// stays `minDelay` too.
    #[tokio::test]
    async fn prioritises_uncloned_repos_and_respects_the_switch() {
        let Ok(dsn) = std::env::var("SG_TEST_DATABASE_DSN") else {
            eprintln!("SG_TEST_DATABASE_DSN unset; skipping");
            return;
        };
        let pool: PgPool = sg_db::connect(&dsn, 5).await.expect("connect");
        let suffix = format!(
            "uncloned-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("clock")
                .as_nanos()
        );

        let mut ids = Vec::new();
        for name in ["a", "b", "c", "d"] {
            let id: i32 = sqlx::query_scalar("INSERT INTO repo(name) VALUES ($1) RETURNING id")
                .bind(format!("{suffix}.example.com/{name}"))
                .fetch_one(&pool)
                .await
                .expect("insert repo");
            ids.push(id);
        }

        // The repo trigger creates `gitserver_repos` rows with `not_cloned`.
        let gitserver_repos = sg_db::gitserver_repos::GitserverReposStore::new(pool.clone());
        gitserver_repos
            .set_clone_status(
                &format!("{suffix}.example.com/b"),
                sg_db::gitserver_repos::CloneStatus::Cloning,
                "shard-1",
            )
            .await
            .expect("set cloning");
        gitserver_repos
            .set_clone_status(
                &format!("{suffix}.example.com/c"),
                sg_db::gitserver_repos::CloneStatus::Cloned,
                "shard-1",
            )
            .await
            .expect("set cloned");
        sqlx::query("UPDATE repo SET deleted_at = NOW() WHERE id = $1")
            .bind(ids[3])
            .execute(&pool)
            .await
            .expect("soft delete");

        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_000);
        let scheduler = Arc::new(UpdateScheduler::new(
            Arc::new(FakeClock(now)),
            Arc::new(FakeStore),
            Arc::new(FakeGitserver),
            live_config("{}"),
            Arc::new(Metrics::new()),
        ));
        let manager = UnclonedReposManager::new(
            Arc::clone(&scheduler),
            RepoStore::new(pool.clone()),
            live_config("{}"),
        );
        manager.run_once().await.expect("run once");

        for id in &ids[..2] {
            let info = scheduler.schedule_info(*id).schedule.expect("scheduled");
            assert_eq!(info.due, now + Duration::from_secs(45));
            assert_eq!(info.interval_seconds, 45);
        }
        assert!(scheduler.schedule_info(ids[2]).schedule.is_none(), "cloned");
        assert!(
            scheduler.schedule_info(ids[3]).schedule.is_none(),
            "deleted"
        );

        // `disableAutoGitUpdates` skips the handler entirely.
        let disabled = Arc::new(UpdateScheduler::new(
            Arc::new(FakeClock(now)),
            Arc::new(FakeStore),
            Arc::new(FakeGitserver),
            live_config(r#"{"disableAutoGitUpdates": true}"#),
            Arc::new(Metrics::new()),
        ));
        let manager = UnclonedReposManager::new(
            Arc::clone(&disabled),
            RepoStore::new(pool.clone()),
            live_config(r#"{"disableAutoGitUpdates": true}"#),
        );
        manager.run_once().await.expect("run once");
        assert!(disabled.schedule_info(ids[0]).schedule.is_none());

        sqlx::query("DELETE FROM repo WHERE id = ANY($1)")
            .bind(&ids)
            .execute(&pool)
            .await
            .expect("cleanup");
    }
}
