#![forbid(unsafe_code)]

//! On-disk database cache (port of `internal/diskcache` as used by symbols).
//!
//! Layout matches the baseline (`diskcache.EncodeKeyComponents`):
//! `<dir>/<sha256(version)>/<sha256(repo)>/<sha256(commit)>.zip`. Builds are
//! single-flighted per key and detached from request timeouts; eviction keeps
//! the directory below `SYMBOLS_CACHE_SIZE_MB`.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

use sha2::{Digest, Sha256};
use tokio::sync::{watch, Semaphore};
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

use crate::config::SymbolsConfig;
use crate::index::Indexer;

/// `writer.symbolsDBVersion`.
const SYMBOLS_DB_VERSION: &str = "5";
const EVICTION_INTERVAL: Duration = Duration::from_secs(10);

#[derive(Debug, Clone)]
enum BuildState {
    Building,
    Done(Result<PathBuf, String>),
}

pub struct Cache {
    dir: PathBuf,
    max_bytes: u64,
    processing_timeout: Duration,
    indexer: Arc<Indexer>,
    semaphore: Arc<Semaphore>,
    registry: Mutex<HashMap<String, watch::Receiver<BuildState>>>,
    tracker: TaskTracker,
    token: CancellationToken,
}

impl Cache {
    pub fn new(
        config: &SymbolsConfig,
        indexer: Arc<Indexer>,
        shutdown: &CancellationToken,
    ) -> Self {
        Self {
            dir: config.cache_dir.clone(),
            max_bytes: config.cache_size_bytes,
            processing_timeout: config.processing_timeout,
            indexer,
            semaphore: Arc::new(Semaphore::new(config.max_concurrently_indexing.max(1))),
            registry: Mutex::new(HashMap::new()),
            tracker: TaskTracker::new(),
            token: shutdown.child_token(),
        }
    }

    /// Start the periodic eviction loop; it stops when the service shuts down.
    pub fn spawn_janitor(self: &Arc<Self>) {
        let cache = Arc::clone(self);
        self.tracker.spawn(async move {
            let mut interval = tokio::time::interval(EVICTION_INTERVAL);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                tokio::select! {
                    _ = cache.token.cancelled() => break,
                    _ = interval.tick() => {
                        if let Err(err) = cache.evict() {
                            tracing::error!(error = %err, "failed to evict symbols cache");
                        }
                    }
                }
            }
        });
    }

    /// Return the database path for `repo@commit`, building it if needed.
    ///
    /// The build itself is not tied to the caller: dropping the returned
    /// future only stops waiting, like `diskcache`'s background fetch.
    pub async fn get_or_create(
        self: &Arc<Self>,
        repo: &str,
        commit: &str,
    ) -> Result<PathBuf, String> {
        let path = self.db_path(repo, commit);
        if path.exists() {
            return Ok(path);
        }

        let key = key(repo, commit);
        let mut receiver = {
            let mut registry = self.registry.lock().expect("cache registry");
            if let Some(receiver) = registry.get(&key) {
                receiver.clone()
            } else {
                let (tx, receiver) = watch::channel(BuildState::Building);
                registry.insert(key.clone(), receiver.clone());
                let cache = Arc::clone(self);
                let build_path = path.clone();
                let repo = repo.to_owned();
                let commit = commit.to_owned();
                self.tracker.spawn(async move {
                    let result = cache.build(&repo, &commit, &build_path).await;
                    let state = BuildState::Done(result.map(|()| build_path));
                    cache.registry.lock().expect("cache registry").remove(&key);
                    let _ = tx.send(state);
                });
                receiver
            }
        };

        loop {
            if let BuildState::Done(result) = receiver.borrow().clone() {
                return result;
            }
            receiver
                .changed()
                .await
                .map_err(|_| "database build ended unexpectedly".to_owned())?;
        }
    }

    /// Stop background work (builds and the janitor) and wait for it.
    pub async fn shutdown(&self) {
        self.token.cancel();
        self.tracker.close();
        self.tracker.wait().await;
    }

    pub fn db_path(&self, repo: &str, commit: &str) -> PathBuf {
        self.repo_dir(repo).join(format!("{}.zip", encode(commit)))
    }

    fn repo_dir(&self, repo: &str) -> PathBuf {
        self.dir.join(encode(SYMBOLS_DB_VERSION)).join(encode(repo))
    }

    async fn build(&self, repo: &str, commit: &str, db_path: &Path) -> Result<(), String> {
        let permit = tokio::select! {
            permit = Arc::clone(&self.semaphore).acquire_owned() => {
                permit.map_err(|err| err.to_string())?
            }
            _ = self.token.cancelled() => return Err("shutting down".to_owned()),
        };

        let result = tokio::select! {
            result = tokio::time::timeout(
                self.processing_timeout,
                self.indexer.write_database(repo, commit, db_path, &self.token),
            ) => result,
            _ = self.token.cancelled() => {
                let _ = std::fs::remove_file(crate::index::part_path(db_path));
                return Err("shutting down".to_owned());
            }
        };
        drop(permit);

        match result {
            Err(_) => Err(format!(
                "indexing {repo}@{commit} took longer than {}",
                humantime(self.processing_timeout)
            )),
            Ok(result) => result,
        }
    }

    /// Remove the oldest `.zip` files until the cache is below its limit.
    fn evict(&self) -> Result<(), String> {
        if self.max_bytes == 0 {
            return Ok(());
        }

        let mut entries: Vec<(SystemTime, PathBuf, u64)> = Vec::new();
        let mut total: u64 = 0;
        collect_files(&self.dir, &mut entries, &mut total)
            .map_err(|err| format!("walk {}: {err}", self.dir.display()))?;
        if total <= self.max_bytes {
            return Ok(());
        }

        entries.sort_by_key(|(modified, _, _)| *modified);
        for (_, path, size) in entries {
            if total <= self.max_bytes {
                break;
            }
            if path.extension().and_then(|ext| ext.to_str()) != Some("zip") {
                continue;
            }
            if std::fs::remove_file(&path).is_ok() {
                total = total.saturating_sub(size);
            }
        }
        Ok(())
    }
}

fn collect_files(
    dir: &Path,
    entries: &mut Vec<(SystemTime, PathBuf, u64)>,
    total: &mut u64,
) -> std::io::Result<()> {
    let read_dir = match std::fs::read_dir(dir) {
        Ok(read_dir) => read_dir,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(err) => return Err(err),
    };
    for entry in read_dir {
        let entry = entry?;
        let file_type = entry.file_type()?;
        let path = entry.path();
        if file_type.is_dir() {
            collect_files(&path, entries, total)?;
        } else if file_type.is_file() {
            let metadata = entry.metadata()?;
            let size = metadata.len();
            *total += size;
            let modified = metadata.modified().unwrap_or(SystemTime::UNIX_EPOCH);
            entries.push((modified, path, size));
        }
    }
    Ok(())
}

fn key(repo: &str, commit: &str) -> String {
    format!("{repo}@{commit}")
}

/// `diskcache.EncodeKeyComponents`: hex-encoded sha256 of each component.
fn encode(component: &str) -> String {
    let digest = Sha256::digest(component.as_bytes());
    let mut encoded = String::with_capacity(64);
    for byte in digest {
        use std::fmt::Write;
        let _ = write!(encoded, "{byte:02x}");
    }
    encoded
}

fn humantime(duration: Duration) -> String {
    let seconds = duration.as_secs();
    let hours = seconds / 3600;
    if hours > 0 {
        format!("{hours}h")
    } else {
        format!("{seconds}s")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encodes_components_like_the_baseline() {
        // sha256(hex) of the ASCII string "5".
        assert_eq!(
            encode("5"),
            "ef2d127de37b942baad06145e54b0c619a1f22327b2ebbcfbec78f5564afe39d"
        );
        assert_eq!(encode("a").len(), 64);
        assert_ne!(encode("a"), encode("b"));
    }
}
