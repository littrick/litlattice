#![forbid(unsafe_code)]

//! Production wiring for the dependency indexing scheduler's `IndexEnqueuer`:
//! the adapters that bind the M9.8dm inference service and the
//! M9.8dc/M9.8de job-selector/enqueuer seams to gitserver, the site
//! configuration and the environment.
//!
//! Baseline counterparts:
//!
//! * `inference.gitService` ([iface.go:24](../../../../../origin/repo/internal/codeintel/autoindexing/internal/inference/iface.go#L24))
//!   — [`GitserverGitService`]: `ResolveRevision` with `EnsureRevision:false`,
//!   `ReadDir` draining the iterator, `ArchiveReader`.
//! * `inference.NewService` and its env limits
//!   ([init.go:16](../../../../../origin/repo/internal/codeintel/autoindexing/internal/inference/init.go#L16))
//!   — [`inference_environment`] and [`TokenBucketLimiter`].
//! * `conf.SiteConfig().CodeIntelAutoIndexingIndexerMap`
//!   ([libs/indexes.go:58](../../../../../origin/repo/internal/codeintel/autoindexing/internal/inference/libs/indexes.go#L58))
//!   — [`indexer_overrides`].
//! * `jobselector.NewFileReader` over `sourcegraph.yaml`
//!   ([job_selector.go:179](../../../../../origin/repo/internal/codeintel/autoindexing/internal/jobselector/job_selector.go#L179))
//!   — [`GitserverYamlReader`].
//! * `IndexEnqueuer`'s `ResolveRevision(EnsureRevision:true)`
//!   ([enqueuer.go](../../../../../origin/repo/internal/codeintel/autoindexing/internal/enqueuer/enqueuer.go))
//!   — [`GitserverRevisionResolver`].
//!
//! Documented deviations (see
//! `dev/rs/docs/overviews/m9_8dn_index_worker_wiring_overview.md`): the site
//! configuration is read through `SiteConfiguration::raw()` because
//! `sg-config` is peer WIP and gets no new accessor; non-string indexer-map
//! values are ignored (the baseline's `map[string]string` decode would reject
//! the whole site configuration); a non-positive rate limit disables waiting
//! instead of blocking forever; non-UTF-8 `sourcegraph.yaml` contents are an
//! error instead of being passed to the YAML parser as bytes. Observability
//! is not ported, like the other worker slices.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use sg_config::SiteConfiguration;
use sg_core::EnvReader;
use sg_git::GitserverClient;
use tokio::sync::Mutex;
use tokio::time::{sleep_until, Instant};
use tokio_stream::StreamExt;

use super::enqueuer::{ResolveRevisionError, RevisionResolver};
use super::inference::{
    ArchiveOptions, DirEntry, GitService, InferenceService, InferenceSettings, RateLimiter,
    DEFAULT_MAXIMUM_FILES_WITH_CONTENT_COUNT, DEFAULT_MAXIMUM_FILE_WITH_CONTENT_SIZE_BYTES,
};
use super::jobselector::{
    FileReadError, InferenceResult, InferenceService as JobSelectorInferenceService,
    SourcegraphYamlReader,
};

/// `init.go`: `CODEINTEL_AUTOINDEXING_INFERENCE_GITSERVER_REQUEST_LIMIT`.
pub const GITSERVER_REQUEST_LIMIT_VAR: &str =
    "CODEINTEL_AUTOINDEXING_INFERENCE_GITSERVER_REQUEST_LIMIT";
/// `init.go`: `CODEINTEL_AUTOINDEXING_INFERENCE_MAXIMUM_FILES_WITH_CONTENT_COUNT`.
pub const MAXIMUM_FILES_WITH_CONTENT_COUNT_VAR: &str =
    "CODEINTEL_AUTOINDEXING_INFERENCE_MAXIMUM_FILES_WITH_CONTENT_COUNT";
/// `init.go`: `CODEINTEL_AUTOINDEXING_INFERENCE_MAXIMUM_FILE_WITH_CONTENT_SIZE_BYTES`.
pub const MAXIMUM_FILE_WITH_CONTENT_SIZE_BYTES_VAR: &str =
    "CODEINTEL_AUTOINDEXING_INFERENCE_MAXIMUM_FILE_WITH_CONTENT_SIZE_BYTES";

const GITSERVER_REQUEST_LIMIT_DESCRIPTION: &str =
    "The maximum number of request to gitserver per second that can be made from the autoindexing inference service.";
const MAXIMUM_FILES_WITH_CONTENT_COUNT_DESCRIPTION: &str =
    "The maximum number of files that can be requested by the inference script. Inference operations exceeding this limit will fail.";
const MAXIMUM_FILE_WITH_CONTENT_SIZE_BYTES_DESCRIPTION: &str =
    "The maximum size of the content of a single file requested by the inference script. Inference operations exceeding this limit will fail.";

/// `init.go`'s default for `gitserverRequestRateLimit`.
pub const DEFAULT_GITSERVER_REQUEST_LIMIT: i64 = 100;

/// `conf.SiteConfig().CodeIntelAutoIndexingIndexerMap`'s site-config key
/// (`schema.go:3195`).
pub const INDEXER_MAP_KEY: &str = "codeIntelAutoIndexing.indexerMap";

/// `sourcegraph.yaml`, read by the job selector's YAML fallback.
pub const SOURCEGRAPH_YAML_PATH: &str = "sourcegraph.yaml";

/// The inference service's constructor inputs read from env and site config.
#[derive(Debug, Clone)]
pub struct InferenceEnvironment {
    pub settings: InferenceSettings,
    pub gitserver_request_rate_limit: i64,
}

/// `env.MustGetInt` for the three inference variables. The baseline panics on
/// an invalid value; the port returns a job error instead.
pub fn inference_environment(
    site: &SiteConfiguration,
    env: &mut EnvReader,
) -> Result<InferenceEnvironment, String> {
    let gitserver_request_rate_limit = must_get_int(
        env,
        GITSERVER_REQUEST_LIMIT_VAR,
        &DEFAULT_GITSERVER_REQUEST_LIMIT.to_string(),
        GITSERVER_REQUEST_LIMIT_DESCRIPTION,
    )?;
    let maximum_files_with_content_count = must_get_int(
        env,
        MAXIMUM_FILES_WITH_CONTENT_COUNT_VAR,
        &DEFAULT_MAXIMUM_FILES_WITH_CONTENT_COUNT.to_string(),
        MAXIMUM_FILES_WITH_CONTENT_COUNT_DESCRIPTION,
    )?;
    let maximum_file_with_content_size_bytes = must_get_int(
        env,
        MAXIMUM_FILE_WITH_CONTENT_SIZE_BYTES_VAR,
        &DEFAULT_MAXIMUM_FILE_WITH_CONTENT_SIZE_BYTES.to_string(),
        MAXIMUM_FILE_WITH_CONTENT_SIZE_BYTES_DESCRIPTION,
    )?;

    Ok(InferenceEnvironment {
        settings: InferenceSettings {
            indexer_overrides: indexer_overrides(site),
            // A negative baseline value makes every content request exceed
            // the limit; clamping to zero keeps the same observable behavior.
            maximum_files_with_content_count: usize::try_from(maximum_files_with_content_count)
                .unwrap_or_default(),
            maximum_file_with_content_size_bytes,
        },
        gitserver_request_rate_limit,
    })
}

fn must_get_int(
    env: &mut EnvReader,
    name: &str,
    default: &str,
    description: &str,
) -> Result<i64, String> {
    let raw = env.get(name, default, description);
    raw.parse::<i64>()
        .map_err(|error| format!("invalid int {raw:?} for {name}: {error}"))
}

/// `conf.SiteConfig().CodeIntelAutoIndexingIndexerMap`: the language → indexer
/// image override map. Non-string values are skipped (see the module docs).
pub fn indexer_overrides(site: &SiteConfiguration) -> BTreeMap<String, String> {
    site.raw()
        .get(INDEXER_MAP_KEY)
        .and_then(serde_json::Value::as_object)
        .map(|map| {
            map.iter()
                .filter_map(|(language, indexer)| {
                    indexer
                        .as_str()
                        .map(|indexer| (language.clone(), indexer.to_owned()))
                })
                .collect()
        })
        .unwrap_or_default()
}

/// `inference.gitService` (`iface.go:24`) backed by [`GitserverClient`].
pub struct GitserverGitService {
    client: Arc<GitserverClient>,
}

impl GitserverGitService {
    pub fn new(client: Arc<GitserverClient>) -> Self {
        Self { client }
    }
}

#[async_trait]
impl GitService for GitserverGitService {
    /// `ResolveRevision(…, ResolveRevisionOptions{EnsureRevision: false})`.
    async fn resolve_revision(&self, repo: &str, spec: &str) -> Result<String, String> {
        self.client
            .resolve_revision(repo, spec, false)
            .await
            .map_err(|error| error.to_string())
    }

    /// `ReadDir(repo, commit, path, recurse)`; the baseline drains the
    /// iterator and the inference service filters out directories.
    async fn read_dir(
        &self,
        repo: &str,
        commit: &str,
        path: &str,
        recurse: bool,
    ) -> Result<Vec<DirEntry>, String> {
        self.client
            .read_dir_entries(repo, commit, path, recurse)
            .await
            .map_err(|error| error.to_string())
            .map(|entries| {
                entries
                    .into_iter()
                    .map(|entry| DirEntry {
                        name: entry.name,
                        is_dir: entry.is_dir,
                    }) // the inference `DirEntry` carries no size; `sg_git::DirEntry.size` is unused here
                    .collect()
            })
    }

    /// `ArchiveReader`; the baseline hands the caller an `io.ReadCloser` and
    /// the inference service reads it to completion.
    async fn archive(&self, repo: &str, options: ArchiveOptions) -> Result<Vec<u8>, String> {
        let mut stream = self
            .client
            .archive(repo, &options.treeish, &options.paths)
            .await
            .map_err(|error| error.to_string())?;

        let mut archive = Vec::new();
        while let Some(chunk) = stream.next().await {
            archive.extend_from_slice(&chunk.map_err(|error| error.to_string())?);
        }
        Ok(archive)
    }
}

/// `IndexEnqueuer`'s `ResolveRevision(EnsureRevision: true)`.
pub struct GitserverRevisionResolver {
    client: Arc<GitserverClient>,
}

impl GitserverRevisionResolver {
    pub fn new(client: Arc<GitserverClient>) -> Self {
        Self { client }
    }
}

#[async_trait]
impl RevisionResolver for GitserverRevisionResolver {
    async fn resolve_revision(
        &self,
        repo: &str,
        rev: &str,
    ) -> Result<String, ResolveRevisionError> {
        match self.client.resolve_revision(repo, rev, true).await {
            Ok(commit) => Ok(commit),
            // `errcode.IsNotFound` covers the missing-file, missing-revision
            // and uncloned-repository payloads; only the package enqueue path
            // treats it as "nothing to do".
            Err(
                sg_git::Error::NotFound(_)
                | sg_git::Error::RepoNotFound { .. }
                | sg_git::Error::RevisionNotFound { .. },
            ) => Err(ResolveRevisionError::NotFound),
            Err(error) => Err(ResolveRevisionError::Other(error.to_string())),
        }
    }
}

/// `jobselector.NewFileReader(repo, commit, "sourcegraph.yaml")`:
/// `os.IsNotExist` is [`FileReadError::NotFound`] (the selector falls through
/// to inference), anything else aborts the selection.
pub struct GitserverYamlReader {
    client: Arc<GitserverClient>,
}

impl GitserverYamlReader {
    pub fn new(client: Arc<GitserverClient>) -> Self {
        Self { client }
    }
}

#[async_trait]
impl SourcegraphYamlReader for GitserverYamlReader {
    async fn read(&self, repo: &str, commit: &str) -> Result<String, FileReadError> {
        match self
            .client
            .read_file_optional(repo, commit, SOURCEGRAPH_YAML_PATH)
            .await
        {
            Ok(Some(contents)) => String::from_utf8(contents).map_err(|error| {
                FileReadError::Other(format!("reading {SOURCEGRAPH_YAML_PATH}: {error}"))
            }),
            Ok(None) => Err(FileReadError::NotFound),
            Err(error) => Err(FileReadError::Other(error.to_string())),
        }
    }
}

/// `jobselector.InferenceService` backed by the ported [`InferenceService`].
pub struct InferenceAdapter {
    service: Arc<InferenceService>,
}

impl InferenceAdapter {
    pub fn new(service: Arc<InferenceService>) -> Self {
        Self { service }
    }
}

#[async_trait]
impl JobSelectorInferenceService for InferenceAdapter {
    async fn infer_index_jobs(
        &self,
        repo: &str,
        commit: &str,
        override_script: &str,
    ) -> Result<InferenceResult, String> {
        self.service
            .infer_index_jobs(repo, commit, override_script)
            .await
            .map_err(|error| error.to_string())
    }
}

/// `rate.NewLimiter(rate.Limit(n), 1)` behind
/// `ratelimit.NewInstrumentedLimiter`: a single-token bucket. Each `wait`
/// reserves the next slot and sleeps until it; the first call passes
/// immediately.
pub struct TokenBucketLimiter {
    interval: Option<Duration>,
    next: Mutex<Instant>,
}

impl TokenBucketLimiter {
    /// `rate` is requests per second. Non-positive rates disable waiting (the
    /// baseline would block every call after the first forever).
    pub fn new(rate: i64) -> Self {
        let interval = (rate > 0).then(|| Duration::from_secs_f64(1.0 / rate as f64));
        Self {
            interval,
            next: Mutex::new(Instant::now()),
        }
    }
}

#[async_trait]
impl RateLimiter for TokenBucketLimiter {
    async fn wait(&self) -> Result<(), String> {
        let Some(interval) = self.interval else {
            return Ok(());
        };

        let scheduled = {
            let mut next = self.next.lock().await;
            let now = Instant::now();
            let scheduled = (*next).max(now);
            *next = scheduled + interval;
            scheduled
        };

        let now = Instant::now();
        if scheduled > now {
            sleep_until(scheduled).await;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn indexer_overrides_reads_the_site_map() {
        let site = SiteConfiguration::parse(
            r#"{"codeIntelAutoIndexing.indexerMap":{"go":"acme/scip-go","rust":"acme/rust"}}"#,
        )
        .expect("site config");
        assert_eq!(
            indexer_overrides(&site),
            BTreeMap::from([
                ("go".to_owned(), "acme/scip-go".to_owned()),
                ("rust".to_owned(), "acme/rust".to_owned()),
            ])
        );
        assert!(indexer_overrides(&SiteConfiguration::default()).is_empty());
    }

    #[test]
    fn inference_environment_uses_the_baseline_defaults() {
        let mut env = EnvReader::new();
        let environment =
            inference_environment(&SiteConfiguration::default(), &mut env).expect("environment");
        assert_eq!(environment.gitserver_request_rate_limit, 100);
        assert_eq!(
            environment.settings.maximum_files_with_content_count,
            DEFAULT_MAXIMUM_FILES_WITH_CONTENT_COUNT
        );
        assert_eq!(
            environment.settings.maximum_file_with_content_size_bytes,
            DEFAULT_MAXIMUM_FILE_WITH_CONTENT_SIZE_BYTES
        );
        assert!(environment.settings.indexer_overrides.is_empty());
    }

    #[tokio::test]
    async fn token_bucket_limiter_paces_after_the_first_call() {
        let limiter = TokenBucketLimiter::new(50); // 20ms slots
        let start = Instant::now();
        limiter.wait().await.expect("first");
        assert!(start.elapsed() < Duration::from_millis(20));

        limiter.wait().await.expect("second");
        assert!(start.elapsed() >= Duration::from_millis(15));
    }

    #[tokio::test]
    async fn non_positive_rate_disables_waiting() {
        let limiter = TokenBucketLimiter::new(0);
        for _ in 0..3 {
            limiter.wait().await.expect("wait");
        }
    }
}
