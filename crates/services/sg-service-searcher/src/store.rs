#![forbid(unsafe_code)]

//! Archive store: gitserver tar → searchable zip on disk (`store.go`).
//!
//! The baseline fetches tarballs and stores zips so that filtered archives
//! support random access. Fetches are single-flighted per cache key, may
//! outlive the request that triggered them (bounded by
//! `PROCESSING_TIMEOUT`), and are evicted by total on-disk size.

use std::collections::HashMap;
use std::fmt;
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use async_trait::async_trait;
use sha2::{Digest, Sha256};
use tokio::sync::{oneshot, Mutex, Semaphore};
use tokio_util::io::{StreamReader, SyncIoBridge};
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

use sg_git::TarByteStream;

use crate::filter::{SearchableFilter, IGNORE_FILE};
use crate::metrics::Metrics;
use crate::zipcache::{ZipCache, ZipFile};

/// Default number of concurrent archive fetches when no gitserver is
/// configured; otherwise `10 × gitserver count` (`watchConfig`).
const DEFAULT_FETCH_LIMIT: usize = 15;

/// How many times a corrupt zip is deleted and refetched (`getZipFileWithRetry`).
const ZIP_RETRIES: usize = 2;

/// Operations the store needs from gitserver.
#[async_trait]
pub trait ArchiveFetcher: Send + Sync {
    async fn archive(
        &self,
        repo: &str,
        commit: &str,
        paths: &[String],
    ) -> Result<TarByteStream, String>;

    /// `gitserver.NewFileReader` mapped to an optional read; `None` means the
    /// file does not exist.
    async fn read_file_optional(
        &self,
        repo: &str,
        commit: &str,
        path: &str,
    ) -> Result<Option<Vec<u8>>, String>;
}

#[async_trait]
impl ArchiveFetcher for sg_git::GitserverClient {
    async fn archive(
        &self,
        repo: &str,
        commit: &str,
        paths: &[String],
    ) -> Result<TarByteStream, String> {
        self.archive(repo, commit, paths)
            .await
            .map_err(String::from)
    }

    async fn read_file_optional(
        &self,
        repo: &str,
        commit: &str,
        path: &str,
    ) -> Result<Option<Vec<u8>>, String> {
        self.read_file_optional(repo, commit, path)
            .await
            .map_err(String::from)
    }
}

/// Store failure.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StoreError {
    /// `fetch_timeout` elapsed while waiting for the archive. The fetch keeps
    /// running in the background.
    Timeout(String),
    /// The service is shutting down.
    Canceled(String),
    /// The tar is malformed in a way that a retry may fix (`temporaryError`).
    Temporary(String),
    /// Any other failure.
    Message(String),
}

impl fmt::Display for StoreError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Timeout(message)
            | Self::Canceled(message)
            | Self::Temporary(message)
            | Self::Message(message) => f.write_str(message),
        }
    }
}

impl std::error::Error for StoreError {}

impl StoreError {
    pub fn is_timeout(&self) -> bool {
        matches!(self, Self::Timeout(_))
    }

    pub fn is_canceled(&self) -> bool {
        matches!(self, Self::Canceled(_))
    }
}

/// Result of a successful `PrepareZip`.
struct Prepared {
    path: PathBuf,
    cache_hit: bool,
}

/// Manages fetching, filtering, caching and evicting archives.
pub struct Store {
    fetcher: Arc<dyn ArchiveFetcher>,
    /// `<SEARCHER_CACHE_DIR>/searcher-archives`.
    dir: PathBuf,
    max_cache_size_bytes: i64,
    background_timeout: Duration,
    /// `conf.SearchLargeFiles`; empty until the site-config client lands.
    search_large_files: Vec<String>,
    fetch_limiter: Arc<Semaphore>,
    zip_cache: Arc<ZipCache>,
    /// Per-key locks implementing `diskcache`'s single-flight.
    key_locks: Mutex<HashMap<PathBuf, Arc<Mutex<()>>>>,
    tasks: TaskTracker,
    shutdown: CancellationToken,
    metrics: &'static Metrics,
}

impl Store {
    pub fn new(
        fetcher: Arc<dyn ArchiveFetcher>,
        cache_dir: &Path,
        cache_size_bytes: u64,
        background_timeout: Duration,
        gitserver_count: usize,
        shutdown: CancellationToken,
    ) -> Arc<Self> {
        let fetch_limit = if gitserver_count == 0 {
            DEFAULT_FETCH_LIMIT
        } else {
            10 * gitserver_count
        };
        Arc::new(Self {
            fetcher,
            dir: cache_dir.join("searcher-archives"),
            max_cache_size_bytes: cache_size_bytes.min(i64::MAX as u64) as i64,
            background_timeout,
            search_large_files: Vec::new(),
            fetch_limiter: Arc::new(Semaphore::new(fetch_limit)),
            zip_cache: Arc::new(ZipCache::new()),
            key_locks: Mutex::new(HashMap::new()),
            tasks: TaskTracker::new(),
            shutdown,
            metrics: Metrics::global(),
        })
    }

    /// `Store.Start`: publish the size limit and start the eviction loop.
    pub fn start(self: &Arc<Self>) {
        self.metrics.max_cache_size(self.max_cache_size_bytes);
        if self.max_cache_size_bytes == 0 {
            return;
        }
        let store = Arc::clone(self);
        self.tasks.spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(10));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                tokio::select! {
                    _ = store.shutdown.cancelled() => break,
                    _ = interval.tick() => {
                        if let Err(err) = store.evict().await {
                            tracing::error!(error = %err, "failed to evict archive cache");
                        }
                    }
                }
            }
        });
    }

    /// Stop background work and wait for in-flight fetches.
    pub async fn shutdown(&self) {
        self.tasks.close();
        self.tasks.wait().await;
    }

    pub fn zip_cache(&self) -> &ZipCache {
        &self.zip_cache
    }

    /// `getZipFile`: prepare the archive and delete/refetch invalid zips.
    pub async fn get_zip_file(
        self: &Arc<Self>,
        repo: &str,
        commit: &str,
        paths: &[String],
        fetch_timeout: Duration,
    ) -> Result<(PathBuf, Arc<ZipFile>), StoreError> {
        let mut last_error = None;
        for attempt in 0..=ZIP_RETRIES {
            let prepared = self.prepare_zip(repo, commit, paths, fetch_timeout).await?;
            match self.zip_cache.get(&prepared.path) {
                Ok(zip) => return Ok((prepared.path, zip)),
                Err(err) => {
                    let retryable = err.is_invalid();
                    last_error = Some(StoreError::Message(format!("failed to read zip: {err}")));
                    if retryable && attempt < ZIP_RETRIES {
                        // `getZipFileWithRetry`: drop the corrupt cache entry
                        // and fetch it again.
                        self.zip_cache.delete(&prepared.path);
                        let _ = tokio::fs::remove_file(&prepared.path).await;
                        continue;
                    }
                    break;
                }
            }
        }
        Err(last_error.unwrap_or_else(|| StoreError::Message("failed to read zip".to_owned())))
    }

    /// `Store.PrepareZip`.
    async fn prepare_zip(
        self: &Arc<Self>,
        repo: &str,
        commit: &str,
        paths: &[String],
        fetch_timeout: Duration,
    ) -> Result<Prepared, StoreError> {
        // The baseline validates the commit again because the cache key
        // depends on it.
        if commit.len() != 40 {
            return Err(StoreError::Message(format!(
                "commit must be resolved (repo={repo:?}, commit={commit:?})"
            )));
        }

        let started = Instant::now();
        let path = self.cache_path(repo, commit, paths);

        // Fast path: already on disk.
        if tokio::fs::try_exists(&path).await.unwrap_or(false) {
            touch(&path).await;
            self.metrics
                .zip_prepare(true, started.elapsed().as_secs_f64());
            return Ok(Prepared {
                path,
                cache_hit: true,
            });
        }

        if self.tasks.is_closed() {
            return Err(StoreError::Canceled("searcher is shutting down".to_owned()));
        }
        let (sender, receiver) = oneshot::channel();
        let store = Arc::clone(self);
        let task_path = path.clone();
        let repo_owned = repo.to_owned();
        let commit_owned = commit.to_owned();
        let paths_owned = paths.to_vec();
        self.tasks.spawn(async move {
            let result = store
                .open_cached(&task_path, &repo_owned, &commit_owned, &paths_owned)
                .await;
            let _ = sender.send(result);
        });

        // The caller only waits `fetch_timeout`; the fetch continues in the
        // background so future requests get a warm cache.
        let waited = tokio::time::timeout(fetch_timeout, receiver).await;
        let result = match waited {
            Ok(Ok(Ok(prepared))) => Ok(prepared),
            Ok(Ok(Err(err))) => Err(err),
            Ok(Err(_)) => Err(StoreError::Canceled("fetch task was dropped".to_owned())),
            Err(_) => Err(StoreError::Timeout(format!(
                "waiting for archive {repo}@{commit}: deadline exceeded"
            ))),
        };
        self.metrics.zip_prepare(
            matches!(&result, Ok(prepared) if prepared.cache_hit),
            started.elapsed().as_secs_f64(),
        );
        result
    }

    /// `diskcache.Open` + `Store.fetch` for one key.
    async fn open_cached(
        &self,
        path: &Path,
        repo: &str,
        commit: &str,
        paths: &[String],
    ) -> Result<Prepared, StoreError> {
        let key_lock = self.key_lock(path).await;
        let _guard = key_lock.lock().await;

        // Someone else may have populated the cache while we waited.
        if tokio::fs::try_exists(path).await.unwrap_or(false) {
            touch(path).await;
            return Ok(Prepared {
                path: path.to_path_buf(),
                cache_hit: true,
            });
        }
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .map_err(|err| StoreError::Message(format!("create archive cache dir: {err}")))?;
        }
        // Just in case we failed due to something bad on the filesystem.
        let _ = tokio::fs::remove_file(path).await;

        let tmp_path = part_path(path);
        let fetch = self.fetch(repo, commit, paths, &tmp_path);
        let result = if self.background_timeout.is_zero() {
            tokio::select! {
                _ = self.shutdown.cancelled() => Err(StoreError::Canceled("searcher is shutting down".to_owned())),
                result = fetch => result,
            }
        } else {
            tokio::select! {
                _ = self.shutdown.cancelled() => Err(StoreError::Canceled("searcher is shutting down".to_owned())),
                result = tokio::time::timeout(self.background_timeout, fetch) => match result {
                    Ok(result) => result,
                    Err(_) => Err(StoreError::Timeout(format!(
                        "fetching archive {repo}@{commit}: processing timeout"
                    ))),
                },
            }
        };
        if let Err(err) = result {
            let _ = tokio::fs::remove_file(&tmp_path).await;
            return Err(err);
        }

        sync_file(&tmp_path).await?;
        tokio::fs::rename(&tmp_path, path).await.map_err(|err| {
            StoreError::Message(format!("failed to put cache item in place: {err}"))
        })?;
        if let Some(parent) = path.parent() {
            sync_dir(parent).await?;
        }
        touch(path).await;
        Ok(Prepared {
            path: path.to_path_buf(),
            cache_hit: false,
        })
    }

    /// `Store.fetch`: acquire the fetch limiter, stream the tar and write the
    /// filtered zip.
    async fn fetch(
        &self,
        repo: &str,
        commit: &str,
        paths: &[String],
        tmp_path: &Path,
    ) -> Result<(), StoreError> {
        self.metrics.fetch_enqueued();
        let permit = tokio::select! {
            _ = self.shutdown.cancelled() => {
                self.metrics.fetch_dequeued();
                return Err(StoreError::Canceled("searcher is shutting down".to_owned()));
            }
            permit = Arc::clone(&self.fetch_limiter).acquire_owned() => permit,
        };
        self.metrics.fetch_dequeued();
        let permit = permit.map_err(|_| StoreError::Canceled("fetch limiter closed".to_owned()))?;

        self.metrics.fetch_started();
        let started = Instant::now();
        let result = async {
            let stream = self
                .fetcher
                .archive(repo, commit, paths)
                .await
                .map_err(|err| StoreError::Message(format!("fetching tar: {err}")))?;
            // Baseline order: FetchTar first, then FilterTar.
            let ignore = self
                .fetcher
                .read_file_optional(repo, commit, IGNORE_FILE)
                .await
                .map_err(|err| {
                    StoreError::Message(format!("error while calling FilterTar: {err}"))
                })?;
            let filter = SearchableFilter::new(ignore.as_deref(), self.search_large_files.clone())
                .map_err(|err| {
                    StoreError::Message(format!("error while calling FilterTar: {err}"))
                })?;

            let tmp_path = tmp_path.to_path_buf();
            tokio::task::spawn_blocking(move || write_zip(stream, &tmp_path, &filter))
                .await
                .map_err(|err| StoreError::Message(format!("zip writer task failed: {err}")))?
        }
        .await;

        drop(permit);
        self.metrics.fetch_finished();
        if let Err(err) = &result {
            self.metrics.fetch_failed();
            tracing::error!(
                repo,
                commit,
                duration = ?started.elapsed(),
                error = %err,
                "failed to fetch archive"
            );
        }
        result
    }

    fn key_lock<'a>(
        &'a self,
        path: &Path,
    ) -> impl std::future::Future<Output = Arc<Mutex<()>>> + 'a {
        let path = path.to_path_buf();
        async move {
            let mut locks = self.key_locks.lock().await;
            Arc::clone(
                locks
                    .entry(path)
                    .or_insert_with(|| Arc::new(Mutex::new(()))),
            )
        }
    }

    /// `Store.PrepareZip` cache key: Go's `%q %q` formatting, the filter hash,
    /// then each path. `diskcache.EncodeKeyComponents` hashes the key again.
    fn cache_path(&self, repo: &str, commit: &str, paths: &[String]) -> PathBuf {
        let mut hasher = Sha256::new();
        hasher.update(format!("{repo:?} {commit:?}").as_bytes());
        let filter = SearchableFilter::new(None, self.search_large_files.clone())
            .expect("no ignore file cannot fail");
        filter.hash_key(&mut hasher);
        hasher.update(b"\x00Paths");
        for path in paths {
            hasher.update([0u8]);
            hasher.update(path.as_bytes());
        }
        let key = hex(&hasher.finalize());

        let mut hasher = Sha256::new();
        hasher.update(key.as_bytes());
        self.dir.join(format!("{}.zip", hex(&hasher.finalize())))
    }

    /// `diskcache.Evict`: walk the cache, then delete the oldest zips until it
    /// fits.
    async fn evict(&self) -> Result<(), String> {
        let dir = self.dir.clone();
        let max = self.max_cache_size_bytes;
        let zip_cache = Arc::clone(&self.zip_cache);
        let stats = tokio::task::spawn_blocking(move || evict_dir(&dir, max, &zip_cache))
            .await
            .map_err(|err| format!("eviction task failed: {err}"))??;
        self.metrics.cache_size(stats.cache_size);
        self.metrics.evictions(stats.evicted);
        Ok(())
    }
}

struct EvictStats {
    cache_size: i64,
    evicted: u64,
}

fn evict_dir(dir: &Path, max: i64, zip_cache: &ZipCache) -> Result<EvictStats, String> {
    let mut entries: Vec<(PathBuf, u64, SystemTime)> = Vec::new();
    let walker = walkdir::WalkDir::new(dir).follow_links(false);
    for entry in walker {
        let entry = match entry {
            Ok(entry) => entry,
            Err(err)
                if err
                    .io_error()
                    .is_some_and(|io| io.kind() == io::ErrorKind::NotFound) =>
            {
                // We can race with tmp files being renamed into place.
                continue;
            }
            Err(err) => return Err(format!("failed to walk {}: {err}", dir.display())),
        };
        if entry.file_type().is_dir() {
            continue;
        }
        let metadata = match entry.metadata() {
            Ok(metadata) => metadata,
            Err(err)
                if err
                    .io_error()
                    .is_some_and(|io| io.kind() == io::ErrorKind::NotFound) =>
            {
                continue;
            }
            Err(err) => return Err(format!("stat {}: {err}", entry.path().display())),
        };
        entries.push((
            entry.path().to_path_buf(),
            metadata.len(),
            metadata.modified().unwrap_or(SystemTime::UNIX_EPOCH),
        ));
    }

    let mut size: i64 = entries.iter().map(|(_, len, _)| *len as i64).sum();
    let stats_cache_size = size;
    if size <= max {
        return Ok(EvictStats {
            cache_size: stats_cache_size,
            evicted: 0,
        });
    }

    entries.sort_by_key(|(_, _, modified)| *modified);
    let mut evicted = 0u64;
    for (path, len, _) in entries {
        if size <= max {
            break;
        }
        if path.extension().and_then(|ext| ext.to_str()) != Some("zip") {
            continue;
        }
        zip_cache.delete(&path);
        match std::fs::remove_file(&path) {
            Ok(()) => {
                evicted += 1;
                size -= len as i64;
            }
            Err(err) => tracing::warn!(path = %path.display(), error = %err, "failed to evict"),
        }
    }

    Ok(EvictStats {
        cache_size: stats_cache_size,
        evicted,
    })
}

/// `copySearchable`: regular files and symlinks only, with the size/binary
/// heuristics from the baseline.
fn write_zip(
    stream: TarByteStream,
    path: &Path,
    filter: &SearchableFilter,
) -> Result<(), StoreError> {
    let reader = StreamReader::new(stream);
    let bridge = SyncIoBridge::new(reader);
    let mut archive = tar::Archive::new(bridge);
    let file = File::create(path)
        .map_err(|err| StoreError::Message(format!("create {}: {err}", path.display())))?;
    let mut zip = zip::ZipWriter::new(file);
    copy_searchable(&mut archive, &mut zip, filter)?;
    zip.finish()
        .map_err(|err| StoreError::Message(format!("finish zip: {err}")))?;
    Ok(())
}

fn copy_searchable<R: Read, W: Write + std::io::Seek>(
    archive: &mut tar::Archive<R>,
    zip: &mut zip::ZipWriter<W>,
    filter: &SearchableFilter,
) -> Result<(), StoreError> {
    use tar::EntryType;

    let entries = archive
        .entries()
        .map_err(|err| StoreError::Message(format!("read tar: {err}")))?;
    let mut buffer = vec![0u8; 32 * 1024];
    for entry in entries {
        let mut entry = match entry {
            Ok(entry) => entry,
            Err(err) => {
                // Gitserver sometimes returns invalid headers; retrying tends
                // to fix it, so surface it as a temporary error.
                let message = format!("read tar entry: {err}");
                if err.kind() == io::ErrorKind::Other && message.contains("header") {
                    return Err(StoreError::Temporary(message));
                }
                return Err(StoreError::Message(message));
            }
        };
        let entry_type = entry.header().entry_type();
        let size = entry
            .header()
            .size()
            .map_err(|err| StoreError::Message(format!("read tar header: {err}")))?;
        let name = String::from_utf8_lossy(&entry.header().path_bytes()).into_owned();
        let link_name = entry
            .header()
            .link_name_bytes()
            .map(|name| name.into_owned());

        match entry_type {
            EntryType::Regular | EntryType::Continuous => {
                if filter.ignore(&name, size) {
                    continue;
                }
                let skip_content = filter.skip_content(&name, size);
                let options = zip::write::SimpleFileOptions::default()
                    .compression_method(zip::CompressionMethod::Stored);
                zip.start_file(name.as_str(), options)
                    .map_err(|err| StoreError::Message(format!("zip start file: {err}")))?;

                if skip_content {
                    continue;
                }

                // Heuristic: assume a file is binary if the first read (up to
                // 32 KiB) contains a 0x00. Only the file name is searchable
                // then. `io.ReadFull` in the baseline: fill the buffer unless
                // EOF.
                let mut n = 0;
                while n < buffer.len() {
                    match entry.read(&mut buffer[n..]) {
                        Ok(0) => break,
                        Ok(read) => n += read,
                        Err(err) if err.kind() == io::ErrorKind::Interrupted => continue,
                        Err(err) => {
                            return Err(StoreError::Message(format!("read tar entry: {err}")))
                        }
                    }
                }
                if n == 0 {
                    continue;
                }
                if buffer[..n].contains(&0) {
                    continue;
                }
                zip.write_all(&buffer[..n])
                    .map_err(|err| StoreError::Message(format!("write zip entry: {err}")))?;
                io::copy(&mut entry, zip)
                    .map_err(|err| StoreError::Message(format!("copy zip entry: {err}")))?;
            }
            EntryType::Symlink => {
                if filter.ignore(&name, size) {
                    continue;
                }
                let target = link_name
                    .map(|name| String::from_utf8_lossy(&name).into_owned())
                    .unwrap_or_default();
                let options = zip::write::SimpleFileOptions::default()
                    .compression_method(zip::CompressionMethod::Stored);
                zip.add_symlink(name, target, options)
                    .map_err(|err| StoreError::Message(format!("zip symlink: {err}")))?;
            }
            _ => continue,
        }
    }
    Ok(())
}

async fn touch(path: &Path) {
    // Best-effort, like the baseline's `touch`.
    let path = path.to_path_buf();
    let _ = tokio::task::spawn_blocking(move || {
        if let Ok(file) = OpenOptions::new().read(true).open(&path) {
            let _ = file.set_modified(SystemTime::now());
        }
    })
    .await;
}

async fn sync_file(path: &Path) -> Result<(), StoreError> {
    let path = path.to_path_buf();
    tokio::task::spawn_blocking(move || File::open(&path)?.sync_all())
        .await
        .map_err(|err| StoreError::Message(format!("fsync task failed: {err}")))?
        .map_err(|err| StoreError::Message(format!("failed to sync cache item: {err}")))
}

async fn sync_dir(path: &Path) -> Result<(), StoreError> {
    let path = path.to_path_buf();
    tokio::task::spawn_blocking(move || File::open(&path)?.sync_all())
        .await
        .map_err(|err| StoreError::Message(format!("fsync task failed: {err}")))?
        .map_err(|err| StoreError::Message(format!("failed to sync cache dir: {err}")))
}

fn part_path(path: &Path) -> PathBuf {
    let mut name = path.as_os_str().to_os_string();
    name.push(".part");
    PathBuf::from(name)
}

fn hex(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push_str(&format!("{byte:02x}"));
    }
    out
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use bytes::Bytes;
    use tokio_stream::wrappers::ReceiverStream;

    use super::*;

    /// In-memory gitserver: serves a fixed tar and ignore file.
    struct FakeFetcher {
        tar: Vec<u8>,
        ignore: Option<Vec<u8>>,
        archives: AtomicUsize,
    }

    impl FakeFetcher {
        fn new(tar: Vec<u8>, ignore: Option<Vec<u8>>) -> Self {
            Self {
                tar,
                ignore,
                archives: AtomicUsize::new(0),
            }
        }
    }

    #[async_trait]
    impl ArchiveFetcher for FakeFetcher {
        async fn archive(
            &self,
            _repo: &str,
            _commit: &str,
            _paths: &[String],
        ) -> Result<TarByteStream, String> {
            self.archives.fetch_add(1, Ordering::SeqCst);
            let (sender, receiver) = tokio::sync::mpsc::channel(4);
            let tar = self.tar.clone();
            tokio::spawn(async move {
                for chunk in tar.chunks(7) {
                    let _ = sender.send(Ok(Bytes::copy_from_slice(chunk))).await;
                }
            });
            Ok(Box::pin(ReceiverStream::new(receiver)))
        }

        async fn read_file_optional(
            &self,
            _repo: &str,
            _commit: &str,
            _path: &str,
        ) -> Result<Option<Vec<u8>>, String> {
            Ok(self.ignore.clone())
        }
    }

    fn make_tar(files: &[(&str, &[u8])]) -> Vec<u8> {
        let mut builder = tar::Builder::new(Vec::new());
        for (name, data) in files {
            let mut header = tar::Header::new_gnu();
            header.set_path(name).expect("path");
            header.set_size(data.len() as u64);
            header.set_mode(0o644);
            header.set_cksum();
            builder.append(&header, *data).expect("append");
        }
        builder.into_inner().expect("finish")
    }

    fn store(fetcher: Arc<FakeFetcher>, dir: &Path) -> Arc<Store> {
        Store::new(
            fetcher,
            dir,
            100_000_000,
            Duration::from_secs(10),
            1,
            CancellationToken::new(),
        )
    }

    #[tokio::test]
    async fn prepares_and_caches_archives() {
        let dir = tempfile::tempdir().expect("tempdir");
        let tar = make_tar(&[("main.go", b"package main\n"), ("README.md", b"hi\n")]);
        let fetcher = Arc::new(FakeFetcher::new(tar, None));
        let store = store(Arc::clone(&fetcher), dir.path());

        let (path, zip) = store
            .get_zip_file("repo", &"a".repeat(40), &[], Duration::from_secs(5))
            .await
            .expect("zip");
        assert!(path.exists());
        assert_eq!(zip.files().len(), 2);
        assert_eq!(fetcher.archives.load(Ordering::SeqCst), 1);

        let (again, zip_again) = store
            .get_zip_file("repo", &"a".repeat(40), &[], Duration::from_secs(5))
            .await
            .expect("cached");
        assert_eq!(again, path);
        assert_eq!(zip_again.files().len(), 2);
        assert_eq!(fetcher.archives.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn single_flights_concurrent_fetches() {
        let dir = tempfile::tempdir().expect("tempdir");
        let tar = make_tar(&[("main.go", b"package main\n")]);
        let fetcher = Arc::new(FakeFetcher::new(tar, None));
        let store = store(Arc::clone(&fetcher), dir.path());

        let mut handles = Vec::new();
        for _ in 0..8 {
            let store = Arc::clone(&store);
            handles.push(tokio::spawn(async move {
                store
                    .get_zip_file("repo", &"b".repeat(40), &[], Duration::from_secs(5))
                    .await
                    .map(|(path, _)| path)
            }));
        }
        for handle in handles {
            handle.await.expect("join").expect("zip");
        }
        assert_eq!(fetcher.archives.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn skips_content_of_large_and_binary_files() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut large = vec![b'a'; 32 * 1024];
        large.extend(std::iter::repeat_n(
            b'b',
            (2 << 20) as usize + 1 - large.len(),
        ));
        // Without `.sourcegraph/ignore` the baseline applies no size filter,
        // but `skip_content` still keeps just a name entry for large files.
        let tar = make_tar(&[
            ("keep.go", b"package keep\n"),
            ("large.go", &large),
            ("binary.bin", b"abc\0def"),
        ]);
        let fetcher = Arc::new(FakeFetcher::new(tar, None));
        let store = store(fetcher, dir.path());

        let (_, zip) = store
            .get_zip_file("repo", &"c".repeat(40), &[], Duration::from_secs(5))
            .await
            .expect("zip");
        let names: Vec<&str> = zip.files().iter().map(|file| file.name.as_str()).collect();
        assert_eq!(names, ["keep.go", "large.go", "binary.bin"]);

        let keep = zip
            .files()
            .iter()
            .find(|file| file.name == "keep.go")
            .expect("keep entry");
        assert_eq!(zip.data_for(keep).expect("data"), b"package keep\n");

        let large_entry = zip
            .files()
            .iter()
            .find(|file| file.name == "large.go")
            .expect("large entry");
        // Large files keep a name entry with no content.
        assert_eq!(large_entry.len, 0);
        let binary = zip
            .files()
            .iter()
            .find(|file| file.name == "binary.bin")
            .expect("binary entry");
        assert_eq!(binary.len, 0);
    }

    #[tokio::test]
    async fn honours_ignore_files() {
        let dir = tempfile::tempdir().expect("tempdir");
        let tar = make_tar(&[
            ("keep.go", b"package keep\n"),
            ("ignored/skip.go", b"package skip\n"),
        ]);
        let fetcher = Arc::new(FakeFetcher::new(tar, Some(b"ignored/".to_vec())));
        let store = store(fetcher, dir.path());

        let (_, zip) = store
            .get_zip_file("repo", &"e".repeat(40), &[], Duration::from_secs(5))
            .await
            .expect("zip");
        let names: Vec<&str> = zip.files().iter().map(|file| file.name.as_str()).collect();
        assert_eq!(names, ["keep.go"]);
    }

    #[tokio::test]
    async fn times_out_but_finishes_in_the_background() {
        struct SlowFetcher {
            tar: Vec<u8>,
        }

        #[async_trait]
        impl ArchiveFetcher for SlowFetcher {
            async fn archive(
                &self,
                _repo: &str,
                _commit: &str,
                _paths: &[String],
            ) -> Result<TarByteStream, String> {
                tokio::time::sleep(Duration::from_millis(200)).await;
                let bytes = Bytes::from(self.tar.clone());
                Ok(Box::pin(tokio_stream::iter(vec![Ok(bytes)])))
            }

            async fn read_file_optional(
                &self,
                _repo: &str,
                _commit: &str,
                _path: &str,
            ) -> Result<Option<Vec<u8>>, String> {
                Ok(None)
            }
        }

        let dir = tempfile::tempdir().expect("tempdir");
        let fetcher = Arc::new(SlowFetcher {
            tar: make_tar(&[("main.go", b"package main\n")]),
        });
        let store = Store::new(
            fetcher,
            dir.path(),
            100_000_000,
            Duration::from_secs(10),
            1,
            CancellationToken::new(),
        );

        let err = store
            .get_zip_file("repo", &"d".repeat(40), &[], Duration::from_millis(10))
            .await
            .expect_err("timeout");
        assert!(err.is_timeout(), "{err}");

        tokio::time::sleep(Duration::from_millis(300)).await;
        let (_, zip) = store
            .get_zip_file("repo", &"d".repeat(40), &[], Duration::from_secs(5))
            .await
            .expect("background fetch completed");
        assert_eq!(zip.files().len(), 1);
    }

    #[tokio::test]
    async fn evicts_oldest_zips() {
        let dir = tempfile::tempdir().expect("tempdir");
        let archive_dir = dir.path().join("searcher-archives");
        std::fs::create_dir_all(&archive_dir).expect("mkdir");
        std::fs::write(archive_dir.join("old.zip"), vec![0u8; 600]).expect("write");
        std::fs::write(archive_dir.join("new.zip"), vec![0u8; 600]).expect("write");

        let zip_cache = ZipCache::new();
        let stats = evict_dir(&archive_dir, 700, &zip_cache).expect("evict");
        assert_eq!(stats.cache_size, 1200);
        assert_eq!(stats.evicted, 1);
        assert_eq!(std::fs::read_dir(&archive_dir).expect("read").count(), 1);
    }
}
