#![forbid(unsafe_code)]

//! `cmd/worker/internal/codeintel/lsifuploadstore_expirer.go`: the
//! `codeintel-uploadstore-expirer` job.
//!
//! The baseline job registers one periodic routine built by
//! `object.NewExpirer`:
//!
//! * name `codeintel.upload-store-expirer`;
//! * description `expires entries in the code intel upload store`;
//! * interval `CODEINTEL_UPLOADSTORE_EXPIRER_INTERVAL` (default `1h`);
//! * handler `store.ExpireObjects(ctx, prefix, maxAge)` with
//!   `CODEINTEL_UPLOADSTORE_EXPIRER_PREFIX`/`..._MAX_AGE` (default `168h`).
//!
//! `lsifuploadstore.New` returns a lazy store in the baseline: the bucket is
//! initialized on the first `ExpireObjects` call, and a broken object store
//! does not prevent worker startup. The handler here reproduces that with a
//! `OnceCell` around `initialize_store`.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use sg_codeintel::lsifuploadstore::Config as UploadStoreConfig;
use sg_core::{ConfigError, EnvReader};
use sg_objectstorage::ObjectStore;
use tokio::sync::OnceCell;
use tokio_util::sync::CancellationToken;

use super::{get_interval, Job, JobConfig, JobDeps};
use crate::config::combine_errors;
use crate::framework::periodic::{Handler, HandlerError, PeriodicRoutine};
use crate::framework::BackgroundRoutine;

/// `lsifuploadstoreExpirer.Description`.
pub const JOB_DESCRIPTION: &str = "";

/// `object.NewExpirer`'s routine name.
pub const ROUTINE_NAME: &str = "codeintel.upload-store-expirer";
/// `object.NewExpirer`'s routine description.
pub const ROUTINE_DESCRIPTION: &str = "expires entries in the code intel upload store";

/// `CODEINTEL_UPLOADSTORE_EXPIRER_PREFIX` (optional, default empty).
pub const PREFIX_VAR: &str = "CODEINTEL_UPLOADSTORE_EXPIRER_PREFIX";
/// `CODEINTEL_UPLOADSTORE_EXPIRER_MAX_AGE` (default `168h`).
pub const MAX_AGE_VAR: &str = "CODEINTEL_UPLOADSTORE_EXPIRER_MAX_AGE";
/// `CODEINTEL_UPLOADSTORE_EXPIRER_INTERVAL` (default `1h`).
pub const INTERVAL_VAR: &str = "CODEINTEL_UPLOADSTORE_EXPIRER_INTERVAL";

const PREFIX_DESCRIPTION: &str =
    "The prefix of objects to expire in the precise code intel upload bucket.";
const MAX_AGE_DESCRIPTION: &str = "The max age of objects in the precise code intel upload bucket.";
const INTERVAL_DESCRIPTION: &str =
    "The frequency at which to expire precise code intel upload bucket objects.";

/// `lsifuploadstoreExpirerConfig`.
#[derive(Debug, Default)]
pub struct UploadStoreExpirerConfig {
    prefix: Mutex<String>,
    max_age: Mutex<Duration>,
    interval: Mutex<Duration>,
    upload_store: Mutex<Option<UploadStoreConfig>>,
    errs: Mutex<Vec<String>>,
    store_errs: Mutex<Vec<String>>,
}

impl UploadStoreExpirerConfig {
    pub fn prefix(&self) -> String {
        self.prefix
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    pub fn max_age(&self) -> Duration {
        *self
            .max_age
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    pub fn interval(&self) -> Duration {
        *self
            .interval
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    pub fn upload_store(&self) -> Option<UploadStoreConfig> {
        self.upload_store
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }
}

impl JobConfig for UploadStoreExpirerConfig {
    fn load(&self, env: &mut EnvReader) {
        // `Load` registers the upload store variables first.
        let mut store_errs = Vec::new();
        let upload_store = UploadStoreConfig::load(env, &mut store_errs);

        let mut errs = Vec::new();
        let prefix = env.get(PREFIX_VAR, "", PREFIX_DESCRIPTION);
        let max_age = get_interval(env, MAX_AGE_VAR, "168h", MAX_AGE_DESCRIPTION, &mut errs);
        let interval = get_interval(env, INTERVAL_VAR, "1h", INTERVAL_DESCRIPTION, &mut errs);

        *self
            .prefix
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = prefix;
        *self
            .max_age
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = max_age;
        *self
            .interval
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = interval;
        *self
            .upload_store
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(upload_store);
        *self
            .errs
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = errs;
        *self
            .store_errs
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = store_errs;
    }

    /// `lsifuploadstoreExpirerConfig.Validate`: `BaseConfig.Validate` then
    /// `LSIFUploadStoreConfig.Validate`.
    fn validate(&self) -> Result<(), ConfigError> {
        let mut errs = self
            .errs
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        errs.extend(
            self.store_errs
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .iter()
                .cloned(),
        );
        match combine_errors(&errs) {
            Some(message) => Err(ConfigError::new(message)),
            None => Ok(()),
        }
    }
}

/// `lsifuploadstoreExpirer`.
pub struct UploadStoreExpirer {
    config: Arc<UploadStoreExpirerConfig>,
}

impl UploadStoreExpirer {
    pub fn new() -> Self {
        Self {
            config: Arc::new(UploadStoreExpirerConfig::default()),
        }
    }

    pub fn config(&self) -> &UploadStoreExpirerConfig {
        &self.config
    }
}

impl Default for UploadStoreExpirer {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Job for UploadStoreExpirer {
    fn description(&self) -> String {
        JOB_DESCRIPTION.to_owned()
    }

    fn configs(&self) -> Vec<Arc<dyn JobConfig>> {
        vec![Arc::clone(&self.config) as Arc<dyn JobConfig>]
    }

    async fn routines(
        &self,
        _deps: &JobDeps,
        _setup_ctx: CancellationToken,
    ) -> Result<Vec<BackgroundRoutine>, String> {
        let upload_store = self
            .config
            .upload_store()
            .ok_or_else(|| "upload store config was not loaded".to_owned())?;

        let handler = Arc::new(UploadStoreExpirerHandler {
            config: upload_store,
            prefix: self.config.prefix(),
            max_age: self.config.max_age(),
            store: OnceCell::new(),
        });
        Ok(vec![PeriodicRoutine::new(handler)
            .with_name(ROUTINE_NAME)
            .with_description(ROUTINE_DESCRIPTION)
            .with_interval(self.config.interval())
            .into()])
    }
}

/// `object.expirer.Handle`: `store.ExpireObjects(ctx, prefix, maxAge)`.
struct UploadStoreExpirerHandler {
    config: UploadStoreConfig,
    prefix: String,
    max_age: Duration,
    /// `lsifuploadstore.New`'s lazy store: initialized on first use.
    store: OnceCell<ObjectStore>,
}

#[async_trait]
impl Handler for UploadStoreExpirerHandler {
    async fn handle(&self, _ctx: CancellationToken) -> Result<(), HandlerError> {
        let store = self
            .store
            .get_or_try_init(|| async {
                sg_codeintel::lsifuploadstore::initialize_store(&self.config).await
            })
            .await
            .map_err(HandlerError::new)?;
        store
            .expire_objects(&self.prefix, self.max_age)
            .await
            .map_err(|err| HandlerError::new(err.to_string()))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    static ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    fn clear_env() {
        for name in [
            PREFIX_VAR,
            MAX_AGE_VAR,
            INTERVAL_VAR,
            "PRECISE_CODE_INTEL_UPLOAD_BACKEND",
            "PRECISE_CODE_INTEL_UPLOAD_MANAGE_BUCKET",
            "PRECISE_CODE_INTEL_UPLOAD_BUCKET",
            "PRECISE_CODE_INTEL_UPLOAD_TTL",
            "PRECISE_CODE_INTEL_UPLOAD_AWS_REGION",
            "PRECISE_CODE_INTEL_UPLOAD_AWS_ENDPOINT",
            "PRECISE_CODE_INTEL_UPLOAD_AWS_USE_PATH_STYLE",
            "PRECISE_CODE_INTEL_UPLOAD_AWS_USE_EC2_ROLE_CREDENTIALS",
            "PRECISE_CODE_INTEL_UPLOAD_AWS_ACCESS_KEY_ID",
            "PRECISE_CODE_INTEL_UPLOAD_AWS_SECRET_ACCESS_KEY",
            "PRECISE_CODE_INTEL_UPLOAD_AWS_SESSION_TOKEN",
            "PRECISE_CODE_INTEL_UPLOAD_GCP_PROJECT_ID",
            "PRECISE_CODE_INTEL_UPLOAD_GOOGLE_APPLICATION_CREDENTIALS_FILE",
            "PRECISE_CODE_INTEL_UPLOAD_GOOGLE_APPLICATION_CREDENTIALS_FILE_CONTENT",
            "DEPLOY_TYPE",
        ] {
            std::env::remove_var(name);
        }
    }

    #[test]
    fn job_metadata_matches_the_baseline() {
        let job = UploadStoreExpirer::new();
        assert_eq!(job.description(), "");
        assert_eq!(job.configs().len(), 1);
        assert_eq!(ROUTINE_NAME, "codeintel.upload-store-expirer");
        assert_eq!(
            ROUTINE_DESCRIPTION,
            "expires entries in the code intel upload store"
        );
    }

    #[test]
    fn config_defaults_match_the_baseline() {
        let _guard = ENV_LOCK.lock().expect("env lock");
        clear_env();
        let config = UploadStoreExpirerConfig::default();
        config.load(&mut EnvReader::new());
        assert!(config.validate().is_ok());
        assert_eq!(config.prefix(), "");
        assert_eq!(config.max_age(), Duration::from_secs(168 * 3600));
        assert_eq!(config.interval(), Duration::from_secs(3600));
        let store = config.upload_store().expect("upload store config");
        assert_eq!(store.backend, "blobstore");
        assert_eq!(store.bucket, "lsif-uploads");
    }

    #[test]
    fn invalid_durations_are_reported() {
        let _guard = ENV_LOCK.lock().expect("env lock");
        clear_env();
        std::env::set_var(MAX_AGE_VAR, "soon");
        std::env::set_var(INTERVAL_VAR, "often");
        let config = UploadStoreExpirerConfig::default();
        config.load(&mut EnvReader::new());
        let err = config.validate().expect_err("invalid durations");
        assert_eq!(
            err.to_string(),
            "2 errors occurred:\n\t* invalid duration \"soon\" for CODEINTEL_UPLOADSTORE_EXPIRER_MAX_AGE: time: invalid duration \"soon\"\n\t* invalid duration \"often\" for CODEINTEL_UPLOADSTORE_EXPIRER_INTERVAL: time: invalid duration \"often\""
        );
        clear_env();
    }
}
