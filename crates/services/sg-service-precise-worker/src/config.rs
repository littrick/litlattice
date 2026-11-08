#![forbid(unsafe_code)]

//! Environment registry entries owned by `cmd/precise-code-intel-worker/shared`
//! and `internal/codeintel/shared/lsifuploadstore`.

use std::time::Duration;

use sg_core::{parse_go_duration, ConfigError, EnvReader};

/// `cmd/precise-code-intel-worker/shared`: `const addr = ":3188"`.
pub const DEFAULT_PORT: &str = "3188";

/// `lsifuploadstore.Config`, kept flat so the S3 and GCS branches can be
/// validated exactly like the baseline.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UploadStoreConfig {
    pub backend: String,
    pub manage_bucket: bool,
    pub bucket: String,
    pub ttl: Duration,

    pub s3_region: String,
    pub s3_endpoint: String,
    pub s3_use_path_style: bool,
    pub s3_access_key_id: String,
    pub s3_secret_access_key: String,
    pub s3_session_token: String,

    pub gcs_project_id: String,
    pub gcs_credentials_file: String,
    pub gcs_credentials_file_contents: String,
}

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

impl UploadStoreConfig {
    fn load(env: &mut EnvReader, errors: &mut Vec<String>) -> Self {
        let backend = env
            .get(
                "PRECISE_CODE_INTEL_UPLOAD_BACKEND",
                "blobstore",
                "The target file service for code intelligence uploads. S3, GCS, and Blobstore are supported.",
            )
            .to_lowercase();
        let manage_bucket = get_bool_go(
            env,
            "PRECISE_CODE_INTEL_UPLOAD_MANAGE_BUCKET",
            "false",
            "Whether or not the client should manage the target bucket configuration.",
            errors,
        );
        let bucket = get_required(
            env,
            "PRECISE_CODE_INTEL_UPLOAD_BUCKET",
            "lsif-uploads",
            "The name of the bucket to store LSIF uploads in.",
            errors,
        );
        let ttl = get_duration(
            env,
            "PRECISE_CODE_INTEL_UPLOAD_TTL",
            "168h",
            "The maximum age of an upload before deletion.",
            errors,
        );

        if !matches!(backend.as_str(), "blobstore" | "s3" | "gcs") {
            errors.push(format!(
                "invalid backend {:?} for PRECISE_CODE_INTEL_UPLOAD_BACKEND: must be S3, GCS, or Blobstore",
                backend
            ));
        }

        let mut config = Self {
            backend,
            manage_bucket,
            bucket,
            ttl,
            s3_region: String::new(),
            s3_endpoint: String::new(),
            s3_use_path_style: false,
            s3_access_key_id: String::new(),
            s3_secret_access_key: String::new(),
            s3_session_token: String::new(),
            gcs_project_id: String::new(),
            gcs_credentials_file: String::new(),
            gcs_credentials_file_contents: String::new(),
        };

        if matches!(config.backend.as_str(), "blobstore" | "s3") {
            config.s3_region = get_required(
                env,
                "PRECISE_CODE_INTEL_UPLOAD_AWS_REGION",
                "us-east-1",
                "The target AWS region.",
                errors,
            );
            config.s3_endpoint = get_required(
                env,
                "PRECISE_CODE_INTEL_UPLOAD_AWS_ENDPOINT",
                &blobstore_default_endpoint(),
                "The target AWS endpoint.",
                errors,
            );
            config.s3_use_path_style = get_bool_go(
                env,
                "PRECISE_CODE_INTEL_UPLOAD_AWS_USE_PATH_STYLE",
                "false",
                "Whether to use path calling (vs subdomain calling).",
                errors,
            );
            let ec2_role_credentials = get_bool_go(
                env,
                "PRECISE_CODE_INTEL_UPLOAD_AWS_USE_EC2_ROLE_CREDENTIALS",
                "false",
                "Whether to use the EC2 metadata API, or use the provided static credentials.",
                errors,
            );

            if !ec2_role_credentials {
                config.s3_access_key_id = get_required(
                    env,
                    "PRECISE_CODE_INTEL_UPLOAD_AWS_ACCESS_KEY_ID",
                    "AKIAIOSFODNN7EXAMPLE",
                    "An AWS access key associated with a user with access to S3.",
                    errors,
                );
                config.s3_secret_access_key = get_required(
                    env,
                    "PRECISE_CODE_INTEL_UPLOAD_AWS_SECRET_ACCESS_KEY",
                    "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
                    "An AWS secret key associated with a user with access to S3.",
                    errors,
                );
                config.s3_session_token = env.get(
                    "PRECISE_CODE_INTEL_UPLOAD_AWS_SESSION_TOKEN",
                    "",
                    "An optional AWS session token associated with a user with access to S3.",
                );
            }
        } else if config.backend == "gcs" {
            config.gcs_project_id = get_required(
                env,
                "PRECISE_CODE_INTEL_UPLOAD_GCP_PROJECT_ID",
                "",
                "The project containing the GCS bucket.",
                errors,
            );
            config.gcs_credentials_file = env.get(
                "PRECISE_CODE_INTEL_UPLOAD_GOOGLE_APPLICATION_CREDENTIALS_FILE",
                "",
                "The path to a service account key file with access to GCS.",
            );
            config.gcs_credentials_file_contents = env.get(
                "PRECISE_CODE_INTEL_UPLOAD_GOOGLE_APPLICATION_CREDENTIALS_FILE_CONTENT",
                "",
                "The contents of a service account key file with access to GCS.",
            );
        }

        config
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

/// `deploy.BlobstoreDefaultEndpoint`: the single-container deployment listens
/// on loopback, everything else expects the `blobstore` host.
fn blobstore_default_endpoint() -> String {
    let deploy_type = std::env::var("DEPLOY_TYPE").unwrap_or_default();
    let deploy_type = if deploy_type.is_empty() {
        "kubernetes".to_owned()
    } else {
        deploy_type
    };
    if deploy_type == "docker-container" {
        "http://127.0.0.1:9000".to_owned()
    } else {
        "http://blobstore:9000".to_owned()
    }
}

/// `BaseConfig.Get`: an empty value is an error and does not fall back to the
/// default.
fn get_required(
    env: &mut EnvReader,
    name: &str,
    default: &str,
    description: &str,
    errors: &mut Vec<String>,
) -> String {
    let raw = env.get(name, default, description);
    if raw.is_empty() {
        errors.push(format!("invalid value \"\" for {name}: no value supplied"));
    }
    raw
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

/// `BaseConfig.GetBool` (`strconv.ParseBool`, unlike `env.GetBool`).
fn get_bool_go(
    env: &mut EnvReader,
    name: &str,
    default: &str,
    description: &str,
    errors: &mut Vec<String>,
) -> bool {
    let raw = env.get(name, default, description);
    match parse_strconv_bool(&raw) {
        Some(value) => value,
        None => {
            errors.push(format!(
                "invalid bool {raw:?} for {name}: strconv.ParseBool: parsing {raw:?}: invalid syntax"
            ));
            false
        }
    }
}

/// `strconv.ParseBool` accepted spellings.
fn parse_strconv_bool(value: &str) -> Option<bool> {
    match value {
        "1" | "t" | "T" | "TRUE" | "true" | "True" => Some(true),
        "0" | "f" | "F" | "FALSE" | "false" | "False" => Some(false),
        _ => None,
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
