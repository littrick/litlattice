#![forbid(unsafe_code)]

//! `internal/codeintel/shared/lsifuploadstore`: the upload-store config and
//! the `lsifuploadstore.New` constructor.
//!
//! Shared by `cmd/precise-code-intel-worker` and the worker job
//! `codeintel-uploadstore-expirer`, both of which read the same
//! `PRECISE_CODE_INTEL_UPLOAD_*` variables.

use std::time::Duration;

use sg_core::{parse_go_duration, EnvReader};

/// `lsifuploadstore.Config`, kept flat so the S3 and GCS branches can be
/// validated exactly like the baseline.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Config {
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

impl Config {
    /// `Config.Load`. Errors are appended in the baseline's registration order
    /// (backend, manage bucket, bucket, TTL, then the backend-specific vars).
    pub fn load(env: &mut EnvReader, errors: &mut Vec<String>) -> Self {
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
                "invalid backend {backend:?} for PRECISE_CODE_INTEL_UPLOAD_BACKEND: must be S3, GCS, or Blobstore"
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

/// `lsifuploadstore.New`'s storage config (`object.StorageConfig` + the
/// `object.create` backend validation). S3/blobstore and GCS are supported;
/// the unknown-backend text matches the baseline's `object.create`.
pub fn storage_config(config: &Config) -> Result<sg_objectstorage::StorageConfig, String> {
    match config.backend.as_str() {
        "s3" | "blobstore" => Ok(sg_objectstorage::StorageConfig {
            backend: config.backend.clone(),
            manage_bucket: config.manage_bucket,
            bucket: config.bucket.clone(),
            s3: sg_objectstorage::S3Config {
                is_blobstore: false,
                region: config.s3_region.clone(),
                endpoint: config.s3_endpoint.clone(),
                use_path_style: config.s3_use_path_style,
                access_key_id: config.s3_access_key_id.clone(),
                secret_access_key: config.s3_secret_access_key.clone(),
                session_token: config.s3_session_token.clone(),
            },
            gcs: sg_objectstorage::GcsConfig::default(),
        }
        .normalize()),
        "gcs" => Ok(sg_objectstorage::StorageConfig {
            backend: config.backend.clone(),
            manage_bucket: config.manage_bucket,
            bucket: config.bucket.clone(),
            s3: sg_objectstorage::S3Config::default(),
            gcs: sg_objectstorage::GcsConfig {
                project_id: config.gcs_project_id.clone(),
                credentials_file: config.gcs_credentials_file.clone(),
                credentials_file_contents: config.gcs_credentials_file_contents.clone(),
            },
        }
        .normalize()),
        other => Err(format!("unknown upload store backend '{other}'")),
    }
}

/// `lsifuploadstore.New` plus `object.Storage.Init`. The baseline returns a
/// lazy store that initializes on first use; callers that need the eager form
/// use this helper. `ObjectStore::new` dispatches on the backend (S3/blobstore
/// or GCS), mirroring `object.create`.
pub async fn initialize_store(config: &Config) -> Result<sg_objectstorage::ObjectStore, String> {
    let storage_config = storage_config(config)?;
    let store = sg_objectstorage::ObjectStore::new(storage_config)
        .await
        .map_err(|err| err.to_string())?;
    store.init().await.map_err(|err| err.to_string())?;
    Ok(store)
}

/// The GCS variant of [`initialize_store`] with an injected transport and
/// endpoint (tests; no credentials or network). It runs the same config
/// mapping and eager `Init` as the production path, then wraps the store in
/// the backend-dispatching [`sg_objectstorage::ObjectStore`].
pub async fn initialize_store_with_transport(
    config: &Config,
    endpoint: &str,
    transport: std::sync::Arc<dyn sg_objectstorage::GcsTransport>,
) -> Result<sg_objectstorage::ObjectStore, String> {
    let storage_config = storage_config(config)?;
    if storage_config.backend != "gcs" {
        return Err(format!(
            "injected transport requires the GCS backend, got {:?}",
            storage_config.backend
        ));
    }
    let store = sg_objectstorage::GcsStore::with_transport(
        storage_config.bucket,
        storage_config.manage_bucket,
        storage_config.gcs.project_id,
        endpoint,
        transport,
    );
    store.init().await.map_err(|err| err.to_string())?;
    Ok(sg_objectstorage::ObjectStore::from_gcs(store))
}

/// `deploy.BlobstoreDefaultEndpoint`: the single-container deployment listens
/// on loopback, everything else expects the `blobstore` host.
pub fn blobstore_default_endpoint() -> String {
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
        let mut errors = Vec::new();
        let config = Config::load(&mut env, &mut errors);
        assert!(errors.is_empty(), "{errors:?}");
        assert_eq!(config.backend, "blobstore");
        assert!(!config.manage_bucket);
        assert_eq!(config.bucket, "lsif-uploads");
        assert_eq!(config.ttl, Duration::from_secs(168 * 3600));
        assert_eq!(config.s3_region, "us-east-1");
        assert_eq!(config.s3_endpoint, "http://blobstore:9000");
        assert!(!config.s3_use_path_style);
        assert_eq!(config.s3_access_key_id, "AKIAIOSFODNN7EXAMPLE");
        assert_eq!(
            config.s3_secret_access_key,
            "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
        );
        assert_eq!(config.s3_session_token, "");
        assert!(config.gcs_project_id.is_empty());
    }

    #[test]
    fn blobstore_endpoint_follows_deploy_type() {
        let _guard = ENV_LOCK.lock().expect("env lock");
        clear_env();
        std::env::set_var("DEPLOY_TYPE", "docker-container");
        assert_eq!(blobstore_default_endpoint(), "http://127.0.0.1:9000");
        std::env::set_var("DEPLOY_TYPE", "kubernetes");
        assert_eq!(blobstore_default_endpoint(), "http://blobstore:9000");
        clear_env();
    }

    #[test]
    fn invalid_backend_is_reported() {
        let _guard = ENV_LOCK.lock().expect("env lock");
        clear_env();
        std::env::set_var("PRECISE_CODE_INTEL_UPLOAD_BACKEND", "swift");
        let mut env = EnvReader::new();
        let mut errors = Vec::new();
        let config = Config::load(&mut env, &mut errors);
        assert_eq!(config.backend, "swift");
        assert_eq!(
            errors,
            vec![
                "invalid backend \"swift\" for PRECISE_CODE_INTEL_UPLOAD_BACKEND: must be S3, GCS, or Blobstore"
            ]
        );
        clear_env();
    }

    fn base_config(backend: &str) -> Config {
        Config {
            backend: backend.to_owned(),
            manage_bucket: false,
            bucket: "lsif-uploads".to_owned(),
            ttl: Duration::from_secs(168 * 3600),
            s3_region: String::new(),
            s3_endpoint: String::new(),
            s3_use_path_style: false,
            s3_access_key_id: String::new(),
            s3_secret_access_key: String::new(),
            s3_session_token: String::new(),
            gcs_project_id: String::new(),
            gcs_credentials_file: String::new(),
            gcs_credentials_file_contents: String::new(),
        }
    }

    #[test]
    fn gcs_storage_config_carries_the_baseline_fields() {
        let mut config = base_config("gcs");
        config.manage_bucket = true;
        config.bucket = "uploads".to_owned();
        config.gcs_project_id = "project-1".to_owned();
        config.gcs_credentials_file = "/tmp/key.json".to_owned();
        config.gcs_credentials_file_contents = "{\"type\":\"service_account\"}".to_owned();

        let storage = storage_config(&config).expect("gcs storage config");
        assert_eq!(storage.backend, "gcs");
        assert!(storage.manage_bucket);
        assert_eq!(storage.bucket, "uploads");
        assert_eq!(storage.gcs.project_id, "project-1");
        assert_eq!(storage.gcs.credentials_file, "/tmp/key.json");
        assert_eq!(
            storage.gcs.credentials_file_contents,
            "{\"type\":\"service_account\"}"
        );
        assert_eq!(storage.s3, sg_objectstorage::S3Config::default());
    }

    #[test]
    fn unknown_storage_backend_matches_the_baseline_text() {
        let error = storage_config(&base_config("swift")).expect_err("unknown backend");
        assert_eq!(error, "unknown upload store backend 'swift'");
    }
}
