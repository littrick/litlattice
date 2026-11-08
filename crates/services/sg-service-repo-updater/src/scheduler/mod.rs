#![forbid(unsafe_code)]

//! `cmd/repo-updater/internal/scheduler`: the in-memory update scheduler.
//!
//! Repositories are hydrated from `gitserver_repos`, scheduled according to
//! the baseline heuristic (`(last_fetched - last_changed) / 2`, clamped to
//! 45s–8h), and enqueued for `FetchRepository` when due. Manual updates
//! (`EnqueueRepoUpdate`, `RecloneRepository`) jump to the high-priority queue.

pub mod schedule;
pub mod updatequeue;

use std::cmp::Ordering;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use async_trait::async_trait;
use sg_config::LiveConfig;
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

use crate::metrics::Metrics;
use schedule::{Schedule, ScheduleState};
use updatequeue::{QueueState, UpdateQueue};

/// `minDelay`.
pub const MIN_DELAY: Duration = Duration::from_secs(45);
/// `maxDelay`.
pub const MAX_DELAY: Duration = Duration::from_secs(8 * 60 * 60);
/// `database.IterateRepoGitserverStatusOptions.BatchSize` in `Start`.
const HYDRATE_BATCH_SIZE: i64 = 1000;

/// `configuredRepo`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfiguredRepo {
    pub id: i32,
    pub name: String,
}

/// `priority`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Priority {
    Low = 0,
    High = 1,
}

/// `types.RepoGitserverStatus`, reduced to what `initialInterval` needs.
#[derive(Debug, Clone)]
pub struct RepoStatus {
    pub id: i32,
    pub name: String,
    pub last_fetched: SystemTime,
    pub last_changed: SystemTime,
}

/// `time.Now`, injectable for deterministic tests.
pub trait Clock: Send + Sync {
    fn now(&self) -> SystemTime;
}

pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> SystemTime {
        SystemTime::now()
    }
}

/// The database subset the scheduler uses (`db.GitserverRepos()`).
#[async_trait]
pub trait SchedulerStore: Send + Sync {
    async fn iterate_repo_gitserver_status(
        &self,
        cursor: i32,
        batch_size: i64,
    ) -> Result<(Vec<RepoStatus>, i32), String>;
    async fn set_last_fetched(
        &self,
        repo: &str,
        last_fetched: SystemTime,
        last_changed: SystemTime,
    ) -> Result<(), String>;
}

/// The gitserver client subset (`gitserver.RepositoryServiceClient`).
#[async_trait]
pub trait RepoUpdateClient: Send + Sync {
    async fn fetch_repository(&self, repo: &str)
        -> Result<(SystemTime, SystemTime), tonic::Status>;
    async fn delete_repository(&self, repo: &str) -> Result<(), tonic::Status>;
}

/// `protocol.RepoUpdateSchedulerInfoResult`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SchedulerInfo {
    pub schedule: Option<ScheduleState>,
    pub queue: Option<QueueState>,
}

/// `scheduler.UpdateScheduler`.
pub struct UpdateScheduler {
    pub schedule: Arc<Schedule>,
    pub queue: Arc<UpdateQueue>,
    clock: Arc<dyn Clock>,
    store: Arc<dyn SchedulerStore>,
    gitserver: Arc<dyn RepoUpdateClient>,
    live: Arc<LiveConfig>,
    metrics: Arc<Metrics>,
}

impl UpdateScheduler {
    pub fn new(
        clock: Arc<dyn Clock>,
        store: Arc<dyn SchedulerStore>,
        gitserver: Arc<dyn RepoUpdateClient>,
        live: Arc<LiveConfig>,
        metrics: Arc<Metrics>,
    ) -> Self {
        Self {
            schedule: Arc::new(Schedule::new(Arc::clone(&clock), Arc::clone(&metrics))),
            queue: Arc::new(UpdateQueue::new(Arc::clone(&metrics))),
            clock,
            store,
            gitserver,
            live,
            metrics,
        }
    }

    /// `UpdateScheduler.Start`'s hydration pass. Errors are retried like the
    /// baseline (five times, then the scheduler builds up lazily).
    pub async fn hydrate(&self) {
        let mut cursor = 0;
        let mut errors = 0;
        loop {
            match self
                .store
                .iterate_repo_gitserver_status(cursor, HYDRATE_BATCH_SIZE)
                .await
            {
                Ok((statuses, next_cursor)) => {
                    for status in statuses {
                        let repo = ConfiguredRepo {
                            id: status.id,
                            name: status.name.clone(),
                        };
                        if !self.schedule.upsert(repo.clone()) {
                            let interval = initial_interval(&status, self.clock.now());
                            self.schedule.update_interval(&repo, interval);
                        }
                    }
                    if next_cursor == 0 {
                        break;
                    }
                    cursor = next_cursor;
                }
                Err(err) => {
                    errors += 1;
                    tracing::error!(error = %err, errors, "failed to iterate gitserver repos");
                    if errors > 5 {
                        tracing::error!(
                            "too many errors, stopping initial hydration of update queue, the queue will build up lazily"
                        );
                        return;
                    }
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            }
        }
    }

    /// `runScheduleLoop`.
    pub async fn run_schedule_loop(self: Arc<Self>, shutdown: CancellationToken) {
        loop {
            let now = self.clock.now();
            let delay = match self.schedule.next_due() {
                Some(due) => due.duration_since(now).unwrap_or(Duration::ZERO),
                None => Duration::from_secs(3600),
            };
            tokio::select! {
                _ = self.schedule.wakeup.notified() => {}
                _ = tokio::time::sleep(delay) => {}
                _ = shutdown.cancelled() => {
                    self.schedule.reset();
                    return;
                }
            }

            if self.live.current().site.disable_auto_git_updates() {
                continue;
            }

            self.schedule.run_due(&self.queue);
            self.metrics.sched_loops.inc();
        }
    }

    /// `runUpdateLoop`.
    pub async fn run_update_loop(self: Arc<Self>, shutdown: CancellationToken, tasks: TaskTracker) {
        // `configuredLimiter`: `conf.GitMaxConcurrentClones`, hot-reloaded.
        let limit = self.live.current().site.git_max_concurrent_clones().max(1);
        let limiter = Arc::new(Semaphore::new(limit));
        {
            let limiter = Arc::clone(&limiter);
            let live = Arc::clone(&self.live);
            let mut generation = live.subscribe();
            let token = shutdown.clone();
            tasks.spawn(async move {
                let mut current = live.current().site.git_max_concurrent_clones().max(1);
                loop {
                    tokio::select! {
                        _ = token.cancelled() => return,
                        changed = generation.changed() => {
                            if changed.is_err() { return; }
                        }
                    }
                    let next = live.current().site.git_max_concurrent_clones().max(1);
                    match next.cmp(&current) {
                        Ordering::Greater => limiter.add_permits(next - current),
                        Ordering::Less => {
                            let want = current - next;
                            let available = limiter.available_permits();
                            // Tokio only lets us drop currently available
                            // permits; in-flight updates finish on their own,
                            // like the baseline's MutableLimiter.
                            limiter.forget_permits(want.min(available));
                        }
                        Ordering::Equal => {}
                    }
                    current = next;
                }
            });
        }

        loop {
            tokio::select! {
                _ = self.queue.notify_enqueue.notified() => {}
                _ = shutdown.cancelled() => {
                    self.queue.reset();
                    return;
                }
            }

            loop {
                let permit = tokio::select! {
                    permit = Arc::clone(&limiter).acquire_owned() => match permit {
                        Ok(permit) => permit,
                        Err(_) => return,
                    },
                    _ = shutdown.cancelled() => return,
                };
                let Some(repo) = self.queue.acquire_next() else {
                    drop(permit);
                    break;
                };
                let this = Arc::clone(&self);
                let token = shutdown.clone();
                tasks.spawn(async move {
                    this.run_update(repo, permit, token).await;
                });
            }
        }
    }

    /// One `FetchRepository` + reschedule, matching the baseline goroutine.
    async fn run_update(
        &self,
        repo: ConfiguredRepo,
        _permit: tokio::sync::OwnedSemaphorePermit,
        shutdown: CancellationToken,
    ) {
        let result = tokio::select! {
            result = self.gitserver.fetch_repository(&repo.name) => result,
            _ = shutdown.cancelled() => {
                self.queue.remove(&repo, true);
                return;
            }
        };

        let mut timestamps = None;
        let succeeded = result.is_ok();
        match result {
            Ok((fetched, changed)) => {
                timestamps = Some((fetched, changed));
                if let Err(err) = self
                    .store
                    .set_last_fetched(&repo.name, fetched, changed)
                    .await
                {
                    tracing::error!(repo = %repo.name, error = %err, "failed to store repo update timestamps");
                }
            }
            Err(status) => {
                if status.code() == tonic::Code::Unavailable {
                    self.metrics.inc_error("requestRepoUpdate");
                    tracing::error!(repo = %repo.name, error = %status, "error requesting repo update");
                } else {
                    self.metrics.inc_error("repoUpdateResponse");
                    if !shutdown.is_cancelled() {
                        tracing::error!(repo = %repo.name, error = %status, "error updating repo");
                    }
                }
            }
        }

        if let Some(interval) = custom_interval(&self.live, &repo.name) {
            self.schedule.update_interval(&repo, interval);
            self.queue.remove(&repo, true);
            return;
        }

        if !succeeded {
            if let Some(current) = self.schedule.current_interval(&repo) {
                self.schedule.update_interval(&repo, current * 2);
            }
        } else if let Some((last_fetched, last_changed)) = timestamps {
            self.schedule
                .update_interval(&repo, duration_between(last_fetched, last_changed) / 2);
        }

        // `defer s.updateQueue.remove(repo, true)`: the entry stays in the
        // queue (flagged as updating) until the update and reschedule finish,
        // so concurrent enqueues are dropped instead of triggering a second
        // fetch.
        self.queue.remove(&repo, true);
    }

    /// `UpdateOnce`: a single high-priority update, without touching the
    /// schedule.
    pub fn update_once(&self, id: i32, name: impl Into<String>) {
        self.metrics.sched_manual_fetch.inc();
        self.queue.enqueue(
            ConfiguredRepo {
                id,
                name: name.into(),
            },
            Priority::High,
        );
    }

    /// `ScheduleInfo`.
    pub fn schedule_info(&self, id: i32) -> SchedulerInfo {
        SchedulerInfo {
            schedule: self.schedule.schedule_info(id),
            queue: self.queue.queue_info(id),
        }
    }

    /// `EnsureScheduled`.
    pub fn ensure_scheduled(&self, repos: &[ConfiguredRepo]) {
        self.schedule.insert_new(repos);
    }

    /// `PrioritiseUncloned`.
    pub fn prioritise_uncloned(&self, repos: &[ConfiguredRepo]) {
        self.schedule.prioritise_uncloned(repos);
    }

    /// `ListRepoIDs`.
    pub fn list_repo_ids(&self) -> Vec<i32> {
        self.schedule.ids()
    }

    /// `UpdateScheduler.UpdateFromDiff`: keep the schedule in sync with the
    /// repositories an external service sync reported.
    ///
    /// - `Deleted`: remove from the schedule and the queue.
    /// - `Added`: schedule and enqueue for an immediate clone/fetch.
    /// - `Modified`: schedule; enqueue only when the name changed (a rename
    ///   usually means a new clone URL).
    /// - `Unmodified`: schedule (unless the stored repo is soft-deleted);
    ///   never enqueue.
    pub fn update_from_diff(&self, diff: &sg_extsvc::RepoSyncDiff) {
        for repo in &diff.deleted {
            self.remove_repo(repo);
        }
        for repo in &diff.added {
            self.upsert_repo(repo, true);
        }
        for modified in &diff.modified {
            self.upsert_repo(
                &modified.repo,
                modified
                    .modified
                    .contains(sg_extsvc::RepoModifiedFields::NAME),
            );
        }
        for repo in &diff.unmodified {
            if repo.deleted {
                self.remove_repo(repo);
                continue;
            }
            self.upsert_repo(repo, false);
        }
    }

    /// `UpdateScheduler.upsert`.
    fn upsert_repo(&self, repo: &sg_extsvc::Repo, enqueue: bool) {
        let configured = ConfiguredRepo {
            id: repo.id,
            name: repo.name.clone(),
        };
        let updated = self.schedule.upsert(configured.clone());
        tracing::debug!(repo = %repo.name, updated, "scheduler.schedule.upserted");
        if !enqueue {
            return;
        }
        let updated = self.queue.enqueue(configured, Priority::Low);
        tracing::debug!(repo = %repo.name, updated, "scheduler.updateQueue.enqueued");
    }

    /// `UpdateScheduler.remove`.
    fn remove_repo(&self, repo: &sg_extsvc::Repo) {
        let configured = ConfiguredRepo {
            id: repo.id,
            name: repo.name.clone(),
        };
        if self.schedule.remove(&configured) {
            tracing::debug!(repo = %repo.name, "scheduler.schedule.removed");
        }
        if self.queue.remove(&configured, false) {
            tracing::debug!(repo = %repo.name, "scheduler.updateQueue.removed");
        }
    }

    pub fn debug_schedule(&self) -> Vec<schedule::ScheduledRepoUpdate> {
        self.schedule.snapshot()
    }

    pub fn debug_queue(&self) -> Vec<updatequeue::RepoUpdate> {
        self.queue.snapshot()
    }

    pub fn metrics(&self) -> &Arc<Metrics> {
        &self.metrics
    }
}

/// `initialInterval` from `scheduler.go`.
pub fn initial_interval(status: &RepoStatus, now: SystemTime) -> Duration {
    let half = duration_between(status.last_fetched, status.last_changed) / 2;
    let interval = clamp_delay(half);
    let due = status.last_fetched + interval;
    clamp_delay(duration_between(due, now))
}

fn clamp_delay(duration: Duration) -> Duration {
    duration.clamp(MIN_DELAY, MAX_DELAY)
}

fn duration_between(later: SystemTime, earlier: SystemTime) -> Duration {
    later.duration_since(earlier).unwrap_or(Duration::ZERO)
}

/// `getCustomInterval`: the first matching `gitUpdateInterval` rule.
fn custom_interval(live: &LiveConfig, repo_name: &str) -> Option<Duration> {
    for rule in live.current().site.git_update_interval() {
        match regex::Regex::new(&rule.pattern) {
            Ok(re) => {
                if re.is_match(repo_name) {
                    if rule.interval_minutes > 0 {
                        return Some(Duration::from_secs(rule.interval_minutes as u64 * 60));
                    }
                    return None;
                }
            }
            Err(err) => {
                tracing::warn!(pattern = %rule.pattern, error = %err, "error compiling GitUpdateInterval pattern");
            }
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    use super::*;
    use crate::metrics::Metrics;

    struct FakeClock(Mutex<SystemTime>);

    impl Clock for FakeClock {
        fn now(&self) -> SystemTime {
            *self.0.lock().expect("clock")
        }
    }

    #[derive(Default)]
    struct FakeStore {
        fetched: Mutex<Vec<(String, SystemTime, SystemTime)>>,
        hydrate: Mutex<Vec<RepoStatus>>,
    }

    #[async_trait]
    impl SchedulerStore for FakeStore {
        async fn iterate_repo_gitserver_status(
            &self,
            _cursor: i32,
            _batch_size: i64,
        ) -> Result<(Vec<RepoStatus>, i32), String> {
            let mut statuses = self.hydrate.lock().expect("hydrate");
            let page: Vec<RepoStatus> = statuses.drain(..).collect();
            Ok((page, 0))
        }

        async fn set_last_fetched(
            &self,
            repo: &str,
            last_fetched: SystemTime,
            last_changed: SystemTime,
        ) -> Result<(), String> {
            self.fetched.lock().expect("fetched").push((
                repo.to_owned(),
                last_fetched,
                last_changed,
            ));
            Ok(())
        }
    }

    struct FakeGitserver {
        last_fetched: SystemTime,
        last_changed: SystemTime,
        fail: bool,
    }

    #[async_trait]
    impl RepoUpdateClient for FakeGitserver {
        async fn fetch_repository(
            &self,
            _repo: &str,
        ) -> Result<(SystemTime, SystemTime), tonic::Status> {
            if self.fail {
                return Err(tonic::Status::unavailable("gitserver down"));
            }
            Ok((self.last_fetched, self.last_changed))
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

    fn scheduler(
        store: Arc<FakeStore>,
        gitserver: Arc<FakeGitserver>,
        live: Arc<LiveConfig>,
        now: SystemTime,
    ) -> (Arc<UpdateScheduler>, Arc<FakeClock>) {
        let clock = Arc::new(FakeClock(Mutex::new(now)));
        let scheduler = Arc::new(UpdateScheduler::new(
            clock.clone(),
            store,
            gitserver,
            live,
            Arc::new(Metrics::new()),
        ));
        (scheduler, clock)
    }

    const NOW: SystemTime = UNIX_EPOCH;

    #[test]
    fn initial_interval_matches_the_baseline_documentation() {
        // Last changed 2h30m ago, last fetched 2h ago: interval 30min/2 =
        // 15min, due since 1h45m ago -> minimum delay.
        // now = 2h30m; last fetched 2h ago (30m), last changed 2h30m ago (0).
        let status = RepoStatus {
            id: 1,
            name: "repo".to_owned(),
            last_fetched: UNIX_EPOCH + Duration::from_secs(30 * 60),
            last_changed: UNIX_EPOCH,
        };
        let now = UNIX_EPOCH + Duration::from_secs(2 * 3600 + 30 * 60);
        assert_eq!(initial_interval(&status, now), MIN_DELAY);

        // Last changed 2h ago, last fetched 30min ago: 90min/2 = 45min,
        // due in 15min.
        let status = RepoStatus {
            id: 1,
            name: "repo".to_owned(),
            last_fetched: UNIX_EPOCH + Duration::from_secs(90 * 60),
            last_changed: UNIX_EPOCH,
        };
        let now = UNIX_EPOCH + Duration::from_secs(2 * 3600);
        assert_eq!(initial_interval(&status, now), Duration::from_secs(15 * 60));
    }

    #[test]
    fn initial_interval_clamps_between_45s_and_8h() {
        let status = RepoStatus {
            id: 1,
            name: "repo".to_owned(),
            last_fetched: UNIX_EPOCH,
            last_changed: UNIX_EPOCH,
        };
        assert_eq!(initial_interval(&status, UNIX_EPOCH), MIN_DELAY);
        let status = RepoStatus {
            id: 1,
            name: "repo".to_owned(),
            last_fetched: UNIX_EPOCH + Duration::from_secs(100 * 3600),
            last_changed: UNIX_EPOCH,
        };
        assert_eq!(
            initial_interval(&status, UNIX_EPOCH + Duration::from_secs(90 * 3600)),
            MAX_DELAY
        );
    }

    #[test]
    fn custom_interval_uses_the_first_matching_rule() {
        let live = live_config(
            r#"{"gitUpdateInterval":[{"pattern":"^github\\.com/example/","interval":10},{"pattern":"^github\\.com/","interval":60}]}"#,
        );
        assert_eq!(
            custom_interval(&live, "github.com/example/repo"),
            Some(Duration::from_secs(600))
        );
        assert_eq!(
            custom_interval(&live, "github.com/other/repo"),
            Some(Duration::from_secs(3600))
        );
        assert_eq!(custom_interval(&live, "gitlab.com/other/repo"), None);
    }

    #[tokio::test]
    async fn update_once_enqueues_at_high_priority_without_scheduling() {
        let (scheduler, _clock) = scheduler(
            Arc::new(FakeStore::default()),
            Arc::new(FakeGitserver {
                last_fetched: NOW,
                last_changed: NOW,
                fail: false,
            }),
            live_config("{}"),
            NOW,
        );
        scheduler.update_once(7, "github.com/example/repo");
        let info = scheduler.schedule_info(7);
        assert!(info.schedule.is_none());
        let queue = info.queue.expect("queued");
        assert_eq!(queue.priority, Priority::High as i32);
        assert_eq!(queue.total, 1);
    }

    #[tokio::test]
    async fn run_update_records_timestamps_and_reschedules() {
        let store = Arc::new(FakeStore::default());
        let fetched = NOW + Duration::from_secs(1000);
        let changed = NOW + Duration::from_secs(400);
        let (scheduler, _clock) = scheduler(
            Arc::clone(&store),
            Arc::new(FakeGitserver {
                last_fetched: fetched,
                last_changed: changed,
                fail: false,
            }),
            live_config("{}"),
            NOW,
        );
        let repo = ConfiguredRepo {
            id: 1,
            name: "github.com/example/repo".to_owned(),
        };
        scheduler.schedule.upsert(repo.clone());
        scheduler.update_once(1, repo.name.clone());
        // The update loop acquires before spawning; this marks the entry as
        // updating so `remove(repo, true)` clears it.
        assert_eq!(scheduler.queue.acquire_next(), Some(repo.clone()));

        let permit = Arc::new(tokio::sync::Semaphore::new(1))
            .acquire_owned()
            .await
            .expect("permit");
        scheduler
            .run_update(repo.clone(), permit, CancellationToken::new())
            .await;

        let recorded = store.fetched.lock().expect("fetched").clone();
        assert_eq!(recorded, vec![(repo.name.clone(), fetched, changed)]);
        // (lastFetched - lastChanged) / 2 = 300s, plus up to 5% jitter.
        let interval = scheduler
            .schedule
            .current_interval(&repo)
            .expect("scheduled");
        assert!(interval >= Duration::from_secs(285) && interval <= Duration::from_secs(315));
        // The queue entry is gone once the update finished.
        assert!(scheduler.schedule_info(1).queue.is_none());
    }

    #[tokio::test]
    async fn failed_update_doubles_the_interval() {
        let store = Arc::new(FakeStore::default());
        let (scheduler, _clock) = scheduler(
            Arc::clone(&store),
            Arc::new(FakeGitserver {
                last_fetched: NOW,
                last_changed: NOW,
                fail: true,
            }),
            live_config("{}"),
            NOW,
        );
        let repo = ConfiguredRepo {
            id: 1,
            name: "github.com/example/repo".to_owned(),
        };
        scheduler.schedule.upsert(repo.clone());
        scheduler
            .schedule
            .update_interval(&repo, Duration::from_secs(600));
        scheduler.update_once(1, repo.name.clone());
        assert_eq!(scheduler.queue.acquire_next(), Some(repo.clone()));

        let permit = Arc::new(tokio::sync::Semaphore::new(1))
            .acquire_owned()
            .await
            .expect("permit");
        scheduler
            .run_update(repo.clone(), permit, CancellationToken::new())
            .await;

        assert!(store.fetched.lock().expect("fetched").is_empty());
        // The current interval (~600s, already jittered) doubled and was
        // jittered again by up to 5%.
        let interval = scheduler
            .schedule
            .current_interval(&repo)
            .expect("scheduled");
        assert!(interval >= Duration::from_secs(1050) && interval <= Duration::from_secs(1350));
    }

    #[tokio::test]
    async fn hydration_restores_intervals_from_gitserver_repos() {
        let store = Arc::new(FakeStore {
            hydrate: Mutex::new(vec![RepoStatus {
                id: 1,
                name: "github.com/example/repo".to_owned(),
                last_fetched: NOW + Duration::from_secs(12 * 3600),
                last_changed: NOW,
            }]),
            ..Default::default()
        });
        let (scheduler, _clock) = scheduler(
            Arc::clone(&store),
            Arc::new(FakeGitserver {
                last_fetched: NOW,
                last_changed: NOW,
                fail: false,
            }),
            live_config("{}"),
            NOW,
        );
        scheduler.hydrate().await;
        let repo = ConfiguredRepo {
            id: 1,
            name: "github.com/example/repo".to_owned(),
        };
        // (12h - 0) / 2 = 6h, due 18h from the epoch; at now = 0 the
        // remaining time clamps to maxDelay (8h), then jitter is applied.
        let interval = scheduler
            .schedule
            .current_interval(&repo)
            .expect("scheduled");
        assert!(interval >= Duration::from_secs(8 * 3600 * 95 / 100));
        assert!(interval <= Duration::from_secs(8 * 3600 * 105 / 100));
        assert_eq!(scheduler.list_repo_ids(), vec![1]);
    }

    #[test]
    fn diffs_update_the_schedule_and_queue_like_the_baseline() {
        let (scheduler, _clock) = scheduler(
            Arc::new(FakeStore::default()),
            Arc::new(FakeGitserver {
                last_fetched: NOW,
                last_changed: NOW,
                fail: false,
            }),
            live_config("{}"),
            NOW,
        );

        let repo = |id: i32, name: &str| sg_extsvc::Repo {
            id,
            name: name.to_owned(),
            ..sg_extsvc::Repo::default()
        };

        let diff = sg_extsvc::RepoSyncDiff {
            added: vec![repo(1, "github.com/o/added")],
            modified: vec![sg_extsvc::ModifiedRepo {
                repo: repo(2, "github.com/o/modified"),
                modified: sg_extsvc::RepoModifiedFields::default(),
            }],
            unmodified: vec![repo(3, "github.com/o/unmodified")],
            ..Default::default()
        };
        scheduler.update_from_diff(&diff);

        // All three are scheduled.
        let mut ids = scheduler.list_repo_ids();
        ids.sort_unstable();
        assert_eq!(ids, vec![1, 2, 3]);
        // Added and the (name-unchanged) modified repo... the baseline only
        // enqueues added; modified without a name change is scheduled only.
        assert!(scheduler.debug_queue().iter().any(|u| u.repo.id == 1));
        assert!(!scheduler.debug_queue().iter().any(|u| u.repo.id == 2));
        assert!(!scheduler.debug_queue().iter().any(|u| u.repo.id == 3));

        // A name change enqueues the modified repo. `Repo::update` is the
        // only way to build the NAME bit.
        let mut stored = repo(2, "github.com/o/modified");
        let changed = stored.update(&repo(2, "github.com/o/renamed"));
        assert!(changed.contains(sg_extsvc::RepoModifiedFields::NAME));
        scheduler.update_from_diff(&sg_extsvc::RepoSyncDiff {
            modified: vec![sg_extsvc::ModifiedRepo {
                repo: repo(2, "github.com/o/renamed"),
                modified: changed,
            }],
            ..Default::default()
        });
        assert!(scheduler.debug_queue().iter().any(|u| u.repo.id == 2));

        // Deleted repos leave both structures.
        scheduler.update_from_diff(&sg_extsvc::RepoSyncDiff {
            deleted: vec![repo(1, "github.com/o/added")],
            ..Default::default()
        });
        assert!(!scheduler.list_repo_ids().contains(&1));
        assert!(!scheduler.debug_queue().iter().any(|u| u.repo.id == 1));
    }
}
