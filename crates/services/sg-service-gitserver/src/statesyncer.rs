//! Port of `cmd/gitserver/internal/statesyncer.go`: periodically reconcile
//! the on-disk clone state with the `gitserver_repos` table.
//!
//! The baseline runs a full scan when the configured gitserver set (or the
//! pinned-repo map) changes, and after any failed sync; otherwise only rows
//! without a shard are visited. The address set is read from the live
//! configuration on every tick (`NewGitserverAddresses(conf.Get())`), so a
//! configuration change triggers the full scan without a restart.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use sg_config::LiveConfig;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

use sg_db::gitserver_repos::{
    CloneStatus, GitserverRepo, GitserverReposStore, IterateRepoGitserverStatusOptions,
};

use crate::addrs::{hostname_match, GitserverAddresses};
use crate::locker::RepositoryLocker;

/// `syncRepoState`'s write-side rate limiter: `rate.NewLimiter(perSecond,
/// perSecond)`, shared by every batch of one sync.
struct RateLimiter {
    per_second: f64,
    burst: f64,
    state: Mutex<(f64, Instant)>,
}

impl RateLimiter {
    fn new(per_second: i64) -> Self {
        let burst = per_second.max(0) as f64;
        Self {
            per_second: burst,
            burst,
            state: Mutex::new((burst, Instant::now())),
        }
    }

    /// `limiter.WaitN(ctx, n)`.
    async fn wait_n(&self, n: usize, shutdown: &CancellationToken) -> Result<(), String> {
        if n as f64 > self.burst {
            // `rate: Wait(n=…) exceeds limiter's burst …`.
            return Err(format!(
                "rate: Wait(n={n}) exceeds limiter's burst {}",
                self.burst as i64
            ));
        }
        let mut waiting = n as f64;
        loop {
            let sleep_for = {
                let mut state = self.state.lock().await;
                let (tokens, last) = &mut *state;
                let elapsed = last.elapsed().as_secs_f64();
                *tokens = (*tokens + elapsed * self.per_second).min(self.burst);
                *last = Instant::now();
                if *tokens >= waiting {
                    *tokens -= waiting;
                    return Ok(());
                }
                let missing = waiting - *tokens;
                *tokens = 0.0;
                waiting = missing;
                Duration::from_secs_f64(missing / self.per_second)
            };
            tokio::select! {
                _ = shutdown.cancelled() => return Err("context canceled".to_owned()),
                _ = tokio::time::sleep(sleep_for) => {}
            }
        }
    }
}

pub struct RepoStateSyncer {
    pub repos: Arc<GitserverReposStore>,
    pub locker: Arc<RepositoryLocker>,
    pub repos_dir: Arc<std::path::PathBuf>,
    pub shard_id: String,
    /// `conf.Get()`: the shard list and pins are recomputed on every run.
    pub live: Arc<LiveConfig>,
    pub interval: Duration,
    pub batch_size: i64,
    pub per_second: i64,
}

impl RepoStateSyncer {
    /// `NewRepoStateSyncer`'s periodic handler. Runs once immediately, then
    /// every `interval`, until `shutdown` is cancelled.
    pub async fn run(self: Arc<Self>, shutdown: CancellationToken) {
        let mut full_sync = true;
        let mut previous_addrs = String::new();
        let mut previous_pinned = String::new();
        let mut ticker = tokio::time::interval(self.interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                _ = shutdown.cancelled() => return,
                _ = ticker.tick() => {}
            }

            // `connection.NewGitserverAddresses(conf.Get())` per run.
            let addresses = GitserverAddresses::from_unified(&self.live.current());

            let current_addrs = addresses.addresses.join(",");
            full_sync = full_sync || current_addrs != previous_addrs;
            previous_addrs = current_addrs;

            let current_pinned = pinned_key(&addresses.pinned_servers);
            full_sync = full_sync || current_pinned != previous_pinned;
            previous_pinned = current_pinned;

            match self.sync_repo_state(full_sync, &addresses, &shutdown).await {
                Ok(()) => full_sync = false,
                Err(err) => {
                    // The baseline retries a full sync next time.
                    full_sync = true;
                    tracing::error!(error = %err, "syncing repo state");
                }
            }
        }
    }

    pub async fn sync_repo_state(
        &self,
        full_sync: bool,
        addresses: &GitserverAddresses,
        shutdown: &CancellationToken,
    ) -> Result<(), String> {
        tracing::debug!(full_sync, "starting syncRepoState");
        let addrs = &addresses.addresses;

        if !addrs
            .iter()
            .any(|addr| hostname_match(&self.shard_id, addr))
        {
            return Err(format!(
                "gitserver hostname, {:?}, not found in list",
                self.shard_id
            ));
        }

        // The rate limit is enforced across all gitserver instances.
        let mut per_second = self.per_second / addrs.len() as i64;
        if per_second < 0 {
            per_second = 1;
        }
        let limiter = RateLimiter::new(per_second);
        let mut batch_size = self.batch_size;
        if batch_size > per_second {
            batch_size = per_second;
        }

        let iterate_page_size = (batch_size * 2).max(500);
        let mut batch: Vec<GitserverRepo> = Vec::new();

        macro_rules! write_batch {
            () => {{
                if !batch.is_empty() {
                    let count = batch.len();
                    match limiter.wait_n(count, shutdown).await {
                        Err(err) => {
                            tracing::error!(error = %err, "Waiting for rate limiter");
                        }
                        Ok(()) => match self.repos.update(&batch).await {
                            Ok(()) => crate::metrics::repo_sync_state_upsert(true, count),
                            Err(err) => {
                                crate::metrics::repo_sync_state_upsert(false, count);
                                tracing::error!(error = %err, "Updating GitserverRepos");
                            }
                        },
                    }
                    batch.clear();
                }
            }};
        }

        let mut options = IterateRepoGitserverStatusOptions {
            // Deleted repos may still be cloned on disk.
            include_deleted: true,
            batch_size: iterate_page_size,
            only_without_shard: !full_sync,
            next_cursor: 0,
        };
        loop {
            let (repos, next_cursor) = self
                .repos
                .iterate_repo_gitserver_status(&options)
                .await
                .map_err(|err| err.to_string())?;

            for status in repos {
                crate::metrics::repo_sync_state("check");

                // Only reconcile repositories this shard owns.
                let addr = addresses.addr_for_repo(&status.name).unwrap_or_default();
                if !hostname_match(&self.shard_id, addr) {
                    crate::metrics::repo_sync_state("other_shard");
                    continue;
                }
                crate::metrics::repo_sync_state("this_shard");

                let cloned = match crate::fs::repo_cloned(&self.repos_dir, &status.name) {
                    Ok(cloned) => cloned,
                    // Cannot determine the state; retry on a later sync.
                    Err(_) => continue,
                };
                let locked = self.locker.status(&status.name).is_some();

                let mut repo = status.gitserver_repo;
                let mut should_update = false;
                if repo.shard_id != self.shard_id {
                    repo.shard_id = self.shard_id.clone();
                    should_update = true;
                }
                let clone_status = clone_status(cloned, locked);
                if repo.clone_status != clone_status {
                    repo.clone_status = clone_status;
                    // A rebased/recloned repository is no longer corrupt.
                    repo.corrupted_at = None;
                    should_update = true;
                }
                if !should_update {
                    continue;
                }

                batch.push(repo);
                if batch.len() as i64 >= batch_size {
                    write_batch!();
                }
            }

            if next_cursor == 0 {
                break;
            }
            options.next_cursor = next_cursor;
        }

        write_batch!();
        Ok(())
    }
}

/// `cloneStatus(cloned, locked)`.
fn clone_status(cloned: bool, locked: bool) -> CloneStatus {
    if cloned {
        CloneStatus::Cloned
    } else if locked {
        CloneStatus::Cloning
    } else {
        CloneStatus::NotCloned
    }
}

/// The baseline stringifies `PinnedServers` as sorted `k=v` pairs to detect
/// changes.
fn pinned_key(pinned: &BTreeMap<String, String>) -> String {
    let mut pairs = pinned
        .iter()
        .map(|(repo, addr)| format!("{repo}={addr}"))
        .collect::<Vec<_>>();
    pairs.sort();
    pairs.join(",")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn clone_status_matches_the_baseline() {
        assert_eq!(clone_status(true, true), CloneStatus::Cloned);
        assert_eq!(clone_status(true, false), CloneStatus::Cloned);
        assert_eq!(clone_status(false, true), CloneStatus::Cloning);
        assert_eq!(clone_status(false, false), CloneStatus::NotCloned);
    }

    #[test]
    fn sorts_pinned_pairs() {
        let pinned = [
            ("b/repo".to_owned(), "g2".to_owned()),
            ("a/repo".to_owned(), "g1".to_owned()),
        ]
        .into_iter()
        .collect();
        assert_eq!(pinned_key(&pinned), "a/repo=g1,b/repo=g2");
    }

    #[tokio::test]
    async fn rejects_shards_not_in_the_address_list() {
        let Ok(dsn) = std::env::var("SG_TEST_DATABASE_DSN") else {
            eprintln!("SG_TEST_DATABASE_DSN unset; skipping");
            return;
        };
        let pool = sg_db::connect(&dsn, 2).await.expect("connect");
        let repos_dir = tempfile::tempdir().expect("tempdir");
        let syncer = RepoStateSyncer {
            repos: Arc::new(GitserverReposStore::new(pool)),
            locker: RepositoryLocker::new(),
            repos_dir: Arc::new(repos_dir.path().to_path_buf()),
            shard_id: "gitserver-9".to_owned(),
            live: Arc::new(LiveConfig::new(
                sg_config::Unified::parse(&sg_config::RawUnified {
                    service_connections: sg_config::ServiceConnections {
                        git_servers: vec!["gitserver-0:3178".to_owned()],
                        ..Default::default()
                    },
                    ..Default::default()
                })
                .expect("config"),
            )),
            interval: Duration::from_secs(60),
            batch_size: 500,
            per_second: 500,
        };
        let addresses = GitserverAddresses {
            addresses: vec!["gitserver-0:3178".to_owned()],
            pinned_servers: BTreeMap::new(),
        };
        let err = syncer
            .sync_repo_state(true, &addresses, &CancellationToken::new())
            .await
            .expect_err("unknown shard");
        assert_eq!(
            err,
            "gitserver hostname, \"gitserver-9\", not found in list"
        );
    }
}
