#![forbid(unsafe_code)]

//! `internal/codeintel/autoindexing/internal/inference/service.go` — the
//! inference orchestration.
//!
//! [`InferenceService::infer_index_jobs`] ports `InferIndexJobs`:
//!
//! 1. create a fresh sandbox containing the default Go modules plus the
//!    `sg.autoindex.*` Lua scripts;
//! 2. run `recognizers.lua` and, if an override script is supplied, merge its
//!    recognizers by name (`false` disables a default recognizer);
//! 3. flatten the recognizers' path/content patterns, resolve the matching
//!    repository paths and the content of the content-pattern paths;
//! 4. invoke each recognizer's `generate` chain until one yields jobs,
//!    recursing into recognizers registered through the `api` argument.
//!
//! Unported dependencies are traits: [`SandboxService`] (the real
//! implementation is `sg_luasandbox::Service`), [`GitService`] (gitserver), and
//! [`RateLimiter`] (the instrumented gitserver limiter). Observability metrics
//! are not ported, like the other worker slices.

use std::collections::BTreeMap;
use std::io::{self, Read};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use sg_luasandbox::mlua;
use sg_luasandbox::mlua::{AnyUserData, UserData, UserDataMethods, Value};
use sg_luasandbox::util::map_slice_or_singleton;
use sg_luasandbox::{CreateOptions, Fs, PrintSink, RunOptions, Sandbox};

use super::luatypes::{
    flatten_recognizer_patterns, index_job_from_table, linearize_generator,
    named_recognizers_from_user_data_map, Recognizer,
};
use crate::autoindexing_runtime::autoindex_config::AutoIndexJobSpec;
use crate::autoindexing_runtime::jobselector::InferenceResult;
use crate::autoindexing_runtime::path_patterns::{
    compile_patterns, filter_paths, filter_paths_by_patterns, PathPattern,
};

/// `init.go`: `CODEINTEL_AUTOINDEXING_INFERENCE_MAXIMUM_FILES_WITH_CONTENT_COUNT`
/// default.
pub const DEFAULT_MAXIMUM_FILES_WITH_CONTENT_COUNT: usize = 100;

/// `init.go`: `CODEINTEL_AUTOINDEXING_INFERENCE_MAXIMUM_FILE_WITH_CONTENT_SIZE_BYTES`
/// default (1 MiB).
pub const DEFAULT_MAXIMUM_FILE_WITH_CONTENT_SIZE_BYTES: i64 = 1024 * 1024;

/// `inference.Service` configuration (site config indexer map and the two
/// content limits).
#[derive(Debug, Clone)]
pub struct InferenceSettings {
    /// `conf.SiteConfig().CodeIntelAutoIndexingIndexerMap`.
    pub indexer_overrides: BTreeMap<String, String>,
    pub maximum_files_with_content_count: usize,
    pub maximum_file_with_content_size_bytes: i64,
}

impl Default for InferenceSettings {
    fn default() -> Self {
        Self {
            indexer_overrides: BTreeMap::new(),
            maximum_files_with_content_count: DEFAULT_MAXIMUM_FILES_WITH_CONTENT_COUNT,
            maximum_file_with_content_size_bytes: DEFAULT_MAXIMUM_FILE_WITH_CONTENT_SIZE_BYTES,
        }
    }
}

/// `inference.SandboxService`.
pub trait SandboxService: Send + Sync {
    fn create_sandbox(&self, options: CreateOptions) -> Result<Sandbox, String>;
}

impl SandboxService for sg_luasandbox::Service {
    fn create_sandbox(&self, options: CreateOptions) -> Result<Sandbox, String> {
        sg_luasandbox::Service::create_sandbox(self, options).map_err(|error| error.to_string())
    }
}

/// `fs.FileInfo` as far as the inference service uses it.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DirEntry {
    pub name: String,
    pub is_dir: bool,
}

/// `gitserver.ArchiveOptions` (`Format` is always `Tar` here).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ArchiveOptions {
    pub treeish: String,
    pub paths: Vec<String>,
}

/// `inference.GitService`.
///
/// The baseline returns an `io.ReadCloser` from `Archive`; the port returns the
/// complete archive buffer (the service consumes it eagerly anyway).
#[async_trait]
pub trait GitService: Send + Sync {
    async fn resolve_revision(&self, repo: &str, spec: &str) -> Result<String, String>;
    async fn read_dir(
        &self,
        repo: &str,
        commit: &str,
        path: &str,
        recurse: bool,
    ) -> Result<Vec<DirEntry>, String>;
    async fn archive(&self, repo: &str, options: ArchiveOptions) -> Result<Vec<u8>, String>;
}

/// `ratelimit.InstrumentedLimiter` as used by the service (`Wait`).
#[async_trait]
pub trait RateLimiter: Send + Sync {
    async fn wait(&self) -> Result<(), String>;
}

/// `inference.LimitError` versus everything else.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InferenceError {
    /// `inference.LimitError`: content request limit exceeded.
    Limit(String),
    /// Any other failure (Lua, gitserver, decoding).
    Other(String),
}

impl std::fmt::Display for InferenceError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Limit(message) | Self::Other(message) => f.write_str(message),
        }
    }
}

impl std::error::Error for InferenceError {}

impl From<mlua::Error> for InferenceError {
    fn from(error: mlua::Error) -> Self {
        Self::Other(error.to_string())
    }
}

/// `inference.Service`.
pub struct InferenceService {
    sandbox_service: Arc<dyn SandboxService>,
    git_service: Arc<dyn GitService>,
    limiter: Arc<dyn RateLimiter>,
    settings: InferenceSettings,
}

impl InferenceService {
    /// `inference.newService`.
    pub fn new(
        sandbox_service: Arc<dyn SandboxService>,
        git_service: Arc<dyn GitService>,
        limiter: Arc<dyn RateLimiter>,
        settings: InferenceSettings,
    ) -> Self {
        Self {
            sandbox_service,
            git_service,
            limiter,
            settings,
        }
    }

    /// `Service.InferIndexJobs`.
    pub async fn infer_index_jobs(
        &self,
        repo: &str,
        commit: &str,
        override_script: &str,
    ) -> Result<InferenceResult, InferenceError> {
        let (jobs, logs) = self
            .infer_index_jobs_inner(repo, commit, override_script)
            .await?;
        Ok(InferenceResult {
            index_jobs: jobs,
            inference_output: logs,
        })
    }

    async fn infer_index_jobs_inner(
        &self,
        repo: &str,
        commit: &str,
        override_script: &str,
    ) -> Result<(Vec<AutoIndexJobSpec>, String), InferenceError> {
        let sandbox = self.create_sandbox()?;
        let buffer = SharedBuffer::default();
        let sink: PrintSink = Arc::new(Mutex::new(buffer.clone()));
        let context = InvocationContext {
            sandbox: &sandbox,
            print_sink: sink.clone(),
            git_service: self.git_service.as_ref(),
            repo,
            commit,
        };

        let recognizers = self.setup_recognizers(&context, override_script)?;
        let jobs = if recognizers.is_empty() {
            Vec::new()
        } else {
            self.invoke_recognizers(&context, &recognizers).await?
        };

        Ok((jobs, buffer.contents()))
    }

    /// `Service.createSandbox`.
    fn create_sandbox(&self) -> Result<Sandbox, InferenceError> {
        let options = CreateOptions {
            go_modules: super::inference_modules(&self.settings.indexer_overrides),
            lua_modules: super::inference_lua_modules(),
        };
        self.sandbox_service
            .create_sandbox(options)
            .map_err(InferenceError::Other)
    }

    /// `Service.setupRecognizers`.
    fn setup_recognizers(
        &self,
        context: &InvocationContext<'_>,
        override_script: &str,
    ) -> Result<Vec<Recognizer>, InferenceError> {
        let options = RunOptions::default().with_print_sink(context.print_sink.clone());
        let scripts: Arc<dyn Fs> = Arc::new(ScriptsFs);

        let raw = context
            .sandbox
            .run_script_named(&options, scripts, "recognizers.lua")?;
        let mut recognizers = named_recognizers_from_user_data_map(&raw, false)?;

        if !override_script.is_empty() {
            let raw = context.sandbox.run_script(&options, override_script)?;
            let overrides = named_recognizers_from_user_data_map(&raw, true)?;
            for (name, recognizer) in overrides {
                match recognizer {
                    None => {
                        recognizers.remove(&name);
                    }
                    Some(recognizer) => {
                        recognizers.insert(name, Some(recognizer));
                    }
                }
            }
        }

        Ok(recognizers.into_values().flatten().collect())
    }

    /// `Service.invokeRecognizers`, including the recursive registration
    /// round-trips. The Go implementation recurses; the port keeps an explicit
    /// stack, which produces the same job multiset (the differential sorts
    /// jobs before comparing).
    async fn invoke_recognizers(
        &self,
        context: &InvocationContext<'_>,
        recognizers: &[Recognizer],
    ) -> Result<Vec<AutoIndexJobSpec>, InferenceError> {
        let mut batches = vec![recognizers.to_vec()];
        let mut jobs = Vec::new();

        while let Some(batch) = batches.pop() {
            let patterns_for_paths = flatten_recognizer_patterns(&batch, false);
            let patterns_for_content = flatten_recognizer_patterns(&batch, true);

            let mut combined = patterns_for_paths.clone();
            combined.extend(patterns_for_content.clone());

            let paths = self.resolve_paths(context, &combined).await?;
            let contents = self
                .resolve_file_contents(context, &paths, &patterns_for_content)
                .await?;

            let registration = RegistrationApi::default();
            for recognizer in &batch {
                let additional = self
                    .invoke_recognizer_chain_until_results(
                        context,
                        recognizer,
                        &registration,
                        &paths,
                        contents.as_ref(),
                    )
                    .await?;
                jobs.extend(additional);
            }

            let registered = registration.take();
            if !registered.is_empty() {
                // Recursive `invokeRecognizers` call.
                batches.push(registered);
            }
        }

        Ok(jobs)
    }

    /// `Service.resolvePaths`.
    async fn resolve_paths(
        &self,
        context: &InvocationContext<'_>,
        patterns_for_paths: &[PathPattern],
    ) -> Result<Vec<String>, InferenceError> {
        self.limiter.wait().await.map_err(InferenceError::Other)?;

        let compiled =
            compile_patterns(patterns_for_paths, false).map_err(InferenceError::Other)?;

        let commit_id = context
            .git_service
            .resolve_revision(context.repo, context.commit)
            .await
            .map_err(InferenceError::Other)?;

        let entries = context
            .git_service
            .read_dir(context.repo, &commit_id, "", true)
            .await
            .map_err(InferenceError::Other)?;

        let paths: Vec<String> = entries
            .into_iter()
            .filter(|entry| !entry.is_dir)
            .map(|entry| entry.name)
            .collect();

        Ok(filter_paths(&paths, &compiled.globs, &[]))
    }

    /// `Service.resolveFileContents`.
    async fn resolve_file_contents(
        &self,
        context: &InvocationContext<'_>,
        paths: &[String],
        patterns_for_content: &[PathPattern],
    ) -> Result<Option<BTreeMap<String, String>>, InferenceError> {
        let relevant_paths =
            filter_paths_by_patterns(paths, patterns_for_content).map_err(InferenceError::Other)?;
        if relevant_paths.is_empty() {
            return Ok(None);
        }

        self.limiter.wait().await.map_err(InferenceError::Other)?;

        let archive = context
            .git_service
            .archive(
                context.repo,
                ArchiveOptions {
                    treeish: context.commit.to_string(),
                    paths: relevant_paths,
                },
            )
            .await
            .map_err(InferenceError::Other)?;

        let mut contents_by_path = BTreeMap::new();
        let mut archive = tar::Archive::new(archive.as_slice());
        let entries = archive
            .entries()
            .map_err(|error| InferenceError::Other(error.to_string()))?;

        for entry in entries {
            let mut entry = entry.map_err(|error| InferenceError::Other(error.to_string()))?;

            if contents_by_path.len() >= self.settings.maximum_files_with_content_count {
                return Err(InferenceError::Limit(format!(
                    "inference limit: requested content for more than {} ({}) files",
                    self.settings.maximum_files_with_content_count,
                    contents_by_path.len()
                )));
            }

            let size = entry.size() as i64;
            if size > self.settings.maximum_file_with_content_size_bytes {
                return Err(InferenceError::Limit(format!(
                    "inference limit: requested content for a file larger than {} ({}) bytes",
                    self.settings.maximum_file_with_content_size_bytes, size
                )));
            }

            let name = entry
                .path()
                .map_err(|error| InferenceError::Other(error.to_string()))?
                .to_string_lossy()
                .into_owned();
            let name = name
                .strip_prefix(":(literal)")
                .map(str::to_string)
                .unwrap_or(name);

            let mut buffer = Vec::new();
            entry
                .read_to_end(&mut buffer)
                .map_err(|error| InferenceError::Other(error.to_string()))?;
            contents_by_path.insert(name, String::from_utf8_lossy(&buffer).into_owned());
        }

        Ok(Some(contents_by_path))
    }

    /// `Service.invokeRecognizerChainUntilResults`.
    async fn invoke_recognizer_chain_until_results(
        &self,
        context: &InvocationContext<'_>,
        recognizer: &Recognizer,
        registration: &RegistrationApi,
        paths: &[String],
        contents_by_path: Option<&BTreeMap<String, String>>,
    ) -> Result<Vec<AutoIndexJobSpec>, InferenceError> {
        for recognizer in linearize_generator(recognizer) {
            let jobs = self
                .invoke_linearized_recognizer(
                    context,
                    &recognizer,
                    registration,
                    paths,
                    contents_by_path,
                )
                .await?;
            if !jobs.is_empty() {
                return Ok(jobs);
            }
        }
        Ok(Vec::new())
    }

    /// `Service.invokeLinearizedRecognizer`.
    async fn invoke_linearized_recognizer(
        &self,
        context: &InvocationContext<'_>,
        recognizer: &Recognizer,
        registration: &RegistrationApi,
        paths: &[String],
        contents_by_path: Option<&BTreeMap<String, String>>,
    ) -> Result<Vec<AutoIndexJobSpec>, InferenceError> {
        let (call_paths, call_contents_by_path) =
            filter_paths_for_recognizer(recognizer, paths, contents_by_path)?;
        if call_paths.is_empty() && call_contents_by_path.is_empty() {
            return Ok(Vec::new());
        }

        let function = recognizer.generator().ok_or_else(|| {
            InferenceError::Other("recognizer has no generate function".to_string())
        })?;

        let options = RunOptions::default().with_print_sink(context.print_sink.clone());
        let api = registration.clone();

        let value = context.sandbox.run_go_callback(&options, |lua| {
            let api = lua.create_userdata(api)?;
            let paths_table = lua.create_table()?;
            for (index, path) in call_paths.iter().enumerate() {
                paths_table.set(index + 1, path.as_str())?;
            }
            let contents_table = lua.create_table()?;
            for (path, contents) in &call_contents_by_path {
                contents_table.set(path.as_str(), contents.as_str())?;
            }
            function.call::<Value>((api, paths_table, contents_table))
        })?;

        let jobs = map_slice_or_singleton(&value, index_job_from_table)?;
        Ok(jobs)
    }
}

/// The per-invocation state shared by the service steps.
struct InvocationContext<'a> {
    sandbox: &'a Sandbox,
    print_sink: PrintSink,
    git_service: &'a dyn GitService,
    repo: &'a str,
    commit: &'a str,
}

/// `Service.filterPathsForRecognizer`.
fn filter_paths_for_recognizer(
    recognizer: &Recognizer,
    paths: &[String],
    contents_by_path: Option<&BTreeMap<String, String>>,
) -> Result<(Vec<String>, BTreeMap<String, String>), InferenceError> {
    let filtered_paths = filter_paths_by_patterns(paths, recognizer.patterns(false))
        .map_err(InferenceError::Other)?;
    let filtered_paths_with_content = filter_paths_by_patterns(paths, recognizer.patterns(true))
        .map_err(InferenceError::Other)?;

    let mut filtered_contents = BTreeMap::new();
    for path in filtered_paths_with_content {
        let contents = contents_by_path
            .and_then(|contents| contents.get(&path))
            .cloned()
            .unwrap_or_default();
        filtered_contents.insert(path, contents);
    }

    Ok((filtered_paths, filtered_contents))
}

/// `registrationAPI`: exposed to Lua as the `api` argument of `generate`.
/// luar exposes Go's `Register` method as `register`.
#[derive(Clone, Default)]
struct RegistrationApi {
    recognizers: Arc<Mutex<Vec<Recognizer>>>,
}

impl RegistrationApi {
    fn take(&self) -> Vec<Recognizer> {
        let mut recognizers = self
            .recognizers
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        std::mem::take(&mut *recognizers)
    }
}

impl UserData for RegistrationApi {
    fn add_methods<M: UserDataMethods<Self>>(methods: &mut M) {
        methods.add_method_mut("register", |_, this, recognizer: AnyUserData| {
            let recognizer = recognizer.borrow::<Recognizer>()?.clone();
            this.recognizers
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .push(recognizer);
            Ok(())
        });
    }
}

/// `lua.Scripts` as an [`Fs`]: the embedded inference scripts by file name.
struct ScriptsFs;

impl Fs for ScriptsFs {
    fn read_file(&self, name: &str) -> io::Result<Vec<u8>> {
        super::inference_lua_sources()
            .get(name)
            .map(|contents| contents.as_bytes().to_vec())
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "file does not exist"))
    }
}

/// Shared print buffer (`PrintSink`).
#[derive(Clone, Default)]
struct SharedBuffer(Arc<Mutex<Vec<u8>>>);

impl SharedBuffer {
    fn contents(&self) -> String {
        let bytes = self
            .0
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        String::from_utf8_lossy(&bytes).into_owned()
    }
}

impl io::Write for SharedBuffer {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        self.0
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .extend_from_slice(buffer);
        Ok(buffer.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn placeholder() {
        // The end-to-end behaviour is covered by the differential harness
        // (`tests/codeintel_autoindexing_inference.rs`); this keeps the module
        // compiled in `cargo test -p sg-service-worker --lib`.
        let _ = DEFAULT_MAXIMUM_FILES_WITH_CONTENT_COUNT;
        let _ = DEFAULT_MAXIMUM_FILE_WITH_CONTENT_SIZE_BYTES;
    }
}
