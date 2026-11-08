#![forbid(unsafe_code)]

//! Environment registry entries owned by `cmd/precise-code-intel-worker/shared`
//! and `internal/codeintel/shared/lsifuploadstore`.

use std::time::Duration;

use sg_core::{parse_go_duration, ConfigError, EnvReader};

/// `cmd/precise-code-intel-worker/shared`: `const addr = ":3188"`.
pub const DEFAULT_PORT: &str = "3188";

/// `lsifuploadstore.Config` lives in `sg-codeintel` because the worker's
/// `codeintel-uploadstore-expirer` job reads the same variables.
pub use sg_codeintel::lsifuploadstore::Config as UploadStoreConfig;

#[derive(Debug, Clone)]
pub struct PreciseWorkerConfig {
    /// `PRECISE_CODE_INTEL_WORKER_*` and the health server address.
    pub listen_addr: String,
    pub worker_poll_interval: Duration,
    pub worker_concurrency: i64,
    pub worker_budget: i64,
    pub maximum_runtime_per_job: Duration,
    pub upload_store: UploadStoreConfig,
}

impl PreciseWorkerConfig {
    pub fn load(env: &mut EnvReader, override_addr: Option<String>) -> Result<Self, ConfigError> {
        // `Config.Validate` appends `BaseConfig.Validate()` (the worker
        // variables) before `LSIFUploadStoreConfig.Validate()`, so worker
        // errors are reported first even though the store is loaded first.
        let mut worker_errors: Vec<String> = Vec::new();
        let mut store_errors: Vec<String> = Vec::new();
        let upload_store = UploadStoreConfig::load(env, &mut store_errors);

        let worker_poll_interval = get_duration(
            env,
            "PRECISE_CODE_INTEL_WORKER_POLL_INTERVAL",
            "1s",
            "Interval between queries to the upload queue.",
            &mut worker_errors,
        );
        let worker_concurrency = get_int(
            env,
            "PRECISE_CODE_INTEL_WORKER_CONCURRENCY",
            "1",
            "The maximum number of indexes that can be processed concurrently.",
            &mut worker_errors,
        );
        let worker_budget = get_int(
            env,
            "PRECISE_CODE_INTEL_WORKER_BUDGET",
            "0",
            "The amount of compressed input data (in bytes) a worker can process concurrently. Zero acts as an infinite budget.",
            &mut worker_errors,
        );
        let maximum_runtime_per_job = get_duration(
            env,
            "PRECISE_CODE_INTEL_WORKER_MAXIMUM_RUNTIME_PER_JOB",
            "25m",
            "The maximum time a single LSIF processing job can take.",
            &mut worker_errors,
        );

        // Registered by packages the baseline worker imports even though this
        // slice does not port their behavior yet; they must appear in
        // `--help` with the same defaults and descriptions.
        let _ = env.get(
            "PRECISE_CODE_INTEL_AUTO_INDEX_MAXIMUM_INDEX_JOBS_PER_INFERRED_CONFIGURATION",
            "50",
            "Repositories with a number of inferred auto-index jobs exceeding this threshold will not be auto-indexed.",
        );
        let _ = env.get(
            "PRECISE_CODE_INTEL_DEFINITION_DUMPS_LIMIT",
            "100",
            "The maximum number of dumps that can define the same package.",
        );

        if let Some(err) = combine_errors(&worker_errors, &store_errors) {
            return Err(ConfigError::new(err));
        }

        let listen_addr = match override_addr {
            Some(addr) if !addr.is_empty() => addr,
            _ => format!(":{DEFAULT_PORT}"),
        };

        Ok(Self {
            listen_addr,
            worker_poll_interval,
            worker_concurrency,
            worker_budget,
            maximum_runtime_per_job,
            upload_store,
        })
    }

    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.listen_addr.is_empty() {
            return Err(ConfigError::new(
                "precise-code-intel-worker listen address must not be empty",
            ));
        }
        Ok(())
    }
}

/// Format a [`ConfigError`] message like `lib/errors.Append` (cockroachdb's
/// multierror `Format`): a single error prints bare, several print
/// `N errors occurred:` followed by one tab-star bullet each.
fn combine_errors(worker_errors: &[String], store_errors: &[String]) -> Option<String> {
    let all: Vec<&String> = worker_errors.iter().chain(store_errors.iter()).collect();
    match all.as_slice() {
        [] => None,
        [single] => Some((*single).clone()),
        many => {
            let mut message = format!("{} errors occurred:", many.len());
            for error in many {
                message.push_str("\n\t* ");
                message.push_str(error);
            }
            Some(message)
        }
    }
}

/// `BaseConfig.GetInt`.
fn get_int(
    env: &mut EnvReader,
    name: &str,
    default: &str,
    description: &str,
    errors: &mut Vec<String>,
) -> i64 {
    let raw = env.get(name, default, description);
    match raw.parse::<i64>() {
        Ok(value) => value,
        Err(_) => {
            errors.push(format!(
                "invalid int {raw:?} for {name}: strconv.ParseInt: parsing {raw:?}: invalid syntax"
            ));
            0
        }
    }
}

/// `BaseConfig.GetInterval`.
fn get_duration(
    env: &mut EnvReader,
    name: &str,
    default: &str,
    description: &str,
    errors: &mut Vec<String>,
) -> Duration {
    let raw = env.get(name, default, description);
    match parse_go_duration(&raw) {
        Ok(duration) => duration,
        Err(message) => {
            errors.push(format!("invalid duration {raw:?} for {name}: {message}"));
            Duration::ZERO
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    static ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    fn clear_env() {
        for name in [
            "PRECISE_CODE_INTEL_WORKER_POLL_INTERVAL",
            "PRECISE_CODE_INTEL_WORKER_CONCURRENCY",
            "PRECISE_CODE_INTEL_WORKER_BUDGET",
            "PRECISE_CODE_INTEL_WORKER_MAXIMUM_RUNTIME_PER_JOB",
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
    fn loads_baseline_defaults() {
        let _guard = ENV_LOCK.lock().expect("env lock");
        clear_env();
        let mut env = EnvReader::new();
        let config = PreciseWorkerConfig::load(&mut env, None).expect("load");
        assert_eq!(config.listen_addr, ":3188");
        assert_eq!(config.worker_poll_interval, Duration::from_secs(1));
        assert_eq!(config.worker_concurrency, 1);
        assert_eq!(config.worker_budget, 0);
        assert_eq!(config.maximum_runtime_per_job, Duration::from_secs(25 * 60));

        let store = &config.upload_store;
        assert_eq!(store.backend, "blobstore");
        assert!(!store.manage_bucket);
        assert_eq!(store.bucket, "lsif-uploads");
        assert_eq!(store.ttl, Duration::from_secs(168 * 3600));
        assert_eq!(store.s3_region, "us-east-1");
        assert_eq!(store.s3_endpoint, "http://blobstore:9000");
        assert!(!store.s3_use_path_style);
        assert_eq!(store.s3_access_key_id, "AKIAIOSFODNN7EXAMPLE");
        assert_eq!(
            store.s3_secret_access_key,
            "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
        );
    }

    #[test]
    fn single_container_uses_loopback_blobstore() {
        let _guard = ENV_LOCK.lock().expect("env lock");
        clear_env();
        std::env::set_var("DEPLOY_TYPE", "docker-container");
        let mut env = EnvReader::new();
        let config = PreciseWorkerConfig::load(&mut env, None).expect("load");
        assert_eq!(config.upload_store.s3_endpoint, "http://127.0.0.1:9000");
        clear_env();
    }

    #[test]
    fn ec2_role_credentials_skip_static_keys() {
        let _guard = ENV_LOCK.lock().expect("env lock");
        clear_env();
        std::env::set_var(
            "PRECISE_CODE_INTEL_UPLOAD_AWS_USE_EC2_ROLE_CREDENTIALS",
            "true",
        );
        let mut env = EnvReader::new();
        let config = PreciseWorkerConfig::load(&mut env, None).expect("load");
        assert_eq!(config.upload_store.s3_access_key_id, "");
        assert_eq!(config.upload_store.s3_secret_access_key, "");
        clear_env();
    }

    #[test]
    fn gcs_backend_reads_only_gcs_settings() {
        let _guard = ENV_LOCK.lock().expect("env lock");
        clear_env();
        std::env::set_var("PRECISE_CODE_INTEL_UPLOAD_BACKEND", "GCS");
        std::env::set_var("PRECISE_CODE_INTEL_UPLOAD_GCP_PROJECT_ID", "proj");
        let mut env = EnvReader::new();
        let config = PreciseWorkerConfig::load(&mut env, None).expect("load");
        assert_eq!(config.upload_store.backend, "gcs");
        assert_eq!(config.upload_store.gcs_project_id, "proj");
        assert_eq!(config.upload_store.s3_region, "");
        clear_env();
    }

    #[test]
    fn invalid_backend_is_reported() {
        let _guard = ENV_LOCK.lock().expect("env lock");
        clear_env();
        std::env::set_var("PRECISE_CODE_INTEL_UPLOAD_BACKEND", "nope");
        let mut env = EnvReader::new();
        let err = PreciseWorkerConfig::load(&mut env, None).expect_err("invalid backend");
        assert_eq!(
            err.to_string(),
            "invalid backend \"nope\" for PRECISE_CODE_INTEL_UPLOAD_BACKEND: must be S3, GCS, or Blobstore"
        );
        clear_env();
    }

    #[test]
    fn empty_required_value_is_an_error() {
        let _guard = ENV_LOCK.lock().expect("env lock");
        clear_env();
        std::env::set_var("PRECISE_CODE_INTEL_UPLOAD_BUCKET", "");
        let mut env = EnvReader::new();
        let err = PreciseWorkerConfig::load(&mut env, None).expect_err("empty bucket");
        assert_eq!(
            err.to_string(),
            "invalid value \"\" for PRECISE_CODE_INTEL_UPLOAD_BUCKET: no value supplied"
        );
        clear_env();
    }

    #[test]
    fn invalid_interval_and_int_are_reported_in_order() {
        let _guard = ENV_LOCK.lock().expect("env lock");
        clear_env();
        std::env::set_var("PRECISE_CODE_INTEL_WORKER_POLL_INTERVAL", "abc");
        std::env::set_var("PRECISE_CODE_INTEL_WORKER_CONCURRENCY", "x");
        let mut env = EnvReader::new();
        let err = PreciseWorkerConfig::load(&mut env, None).expect_err("invalid values");
        let message = err.to_string();
        assert!(
            message.starts_with("2 errors occurred:\n\t* invalid duration"),
            "{message}"
        );
        let duration_pos = message.find("invalid duration").expect("duration error");
        let int_pos = message.find("invalid int").expect("int error");
        assert!(duration_pos < int_pos);
        clear_env();
    }

    #[test]
    fn worker_errors_precede_store_errors_like_the_baseline() {
        let _guard = ENV_LOCK.lock().expect("env lock");
        clear_env();
        std::env::set_var("PRECISE_CODE_INTEL_WORKER_CONCURRENCY", "x");
        std::env::set_var("PRECISE_CODE_INTEL_UPLOAD_BACKEND", "nope");
        let mut env = EnvReader::new();
        let err = PreciseWorkerConfig::load(&mut env, None).expect_err("invalid values");
        assert_eq!(
            err.to_string(),
            "2 errors occurred:\n\t* invalid int \"x\" for PRECISE_CODE_INTEL_WORKER_CONCURRENCY: strconv.ParseInt: parsing \"x\": invalid syntax\n\t* invalid backend \"nope\" for PRECISE_CODE_INTEL_UPLOAD_BACKEND: must be S3, GCS, or Blobstore"
        );
        clear_env();
    }

    #[test]
    fn parses_go_durations() {
        assert_eq!(parse_go_duration("1s"), Ok(Duration::from_secs(1)));
        assert_eq!(parse_go_duration("25m"), Ok(Duration::from_secs(1500)));
        assert_eq!(
            parse_go_duration("168h"),
            Ok(Duration::from_secs(168 * 3600))
        );
        assert_eq!(parse_go_duration("1.5s"), Ok(Duration::from_millis(1500)));
        assert_eq!(parse_go_duration("1h30m"), Ok(Duration::from_secs(5400)));
        // Go special-cases a lone zero before looking for units.
        assert_eq!(parse_go_duration("0"), Ok(Duration::ZERO));
        assert_eq!(parse_go_duration("-0"), Ok(Duration::ZERO));
        assert_eq!(
            parse_go_duration("abc"),
            Err("time: invalid duration \"abc\"".to_owned())
        );
    }
}
