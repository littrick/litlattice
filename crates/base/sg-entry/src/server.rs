#![forbid(unsafe_code)]

//! `cmd/server` startup plan: environment defaults, generated configuration
//! files, Procfile assembly and the Postgres 5.1 reindex gate.
//!
//! The baseline (`cmd/server/shared/shared.go`) mutates the process
//! environment and shells out while building the Procfile. This port keeps the
//! same observable ordering in a deterministic [`EnvMap`]; the only injected
//! input is the environment map and the two data directories.
//!
//! `maybePostgresProcFile` (`cmd/server/shared/postgres.go:16-199`) both
//! provisions an in-container Postgres data directory (`/run/postgresql`,
//! markers, `initdb`, `createdb`, the 5.1 reindex marker) and returns the final
//! `postgres:` supervisor line. [`plan_postgres_setup`] models that provisioning
//! as an ordered, inspectable list of actions and [`execute_postgres_setup`]
//! runs it; keeping planning and execution separate lets the differential
//! harness compare the command sequence/file semantics without requiring root
//! or a local Postgres server.

use std::fs;
use std::path::{Path, PathBuf};

use sha2::{Digest, Sha256};

use crate::env::{go_quote, parse_go_bool, EnvMap};
use crate::launcher::{self, DiedAction, ProcfileEntry};
use crate::migrator::CommandMigrator;

const NGINX_CONF: &str = include_str!("../assets/nginx.conf");
const NGINX_DIR: &[(&str, &str)] = &[
    (
        "sourcegraph_backend.conf",
        include_str!("../assets/nginx/sourcegraph_backend.conf"),
    ),
    (
        "sourcegraph_http.conf",
        include_str!("../assets/nginx/sourcegraph_http.conf"),
    ),
    (
        "sourcegraph_main.conf",
        include_str!("../assets/nginx/sourcegraph_main.conf"),
    ),
    (
        "sourcegraph_server.conf",
        include_str!("../assets/nginx/sourcegraph_server.conf"),
    ),
];
const REDIS_STORE_TMPL: &str = include_str!("../assets/redis-store.conf.tmpl");
const REDIS_CACHE_TMPL: &str = include_str!("../assets/redis-cache.conf.tmpl");

/// `shared.FrontendInternalHost`.
pub const FRONTEND_INTERNAL_HOST: &str = "127.0.0.1:3090";

const SYNTECT_LINE: &str = r#"syntect-server: sh -c 'env QUIET=true ROCKET_ENV=production ROCKET_PORT=9238 ROCKET_LIMITS='"'"'{json=10485760}'"'"' ROCKET_SECRET_KEY='"'"'SeerutKeyIsI7releuantAndknvsuZPluaseIgnorYA='"'"' ROCKET_KEEP_ALIVE=0 ROCKET_ADDRESS='"'"'"127.0.0.1"'"'"' syntect_server | grep -v "Rocket has launched" | grep -v "Warning: environment is"' | grep -v 'Configured for production'"#;

const PROMETHEUS_LINE: &str = "prometheus: env STORAGE_PATH=/var/opt/sourcegraph/prometheus /bin/prom-wrapper >> /var/opt/sourcegraph/prometheus.log 2>&1";
const GRAFANA_LINE: &str = "grafana: /usr/share/grafana/bin/grafana-server -config /sg_config_grafana/grafana-single-container.ini -homepath /usr/share/grafana >> /var/opt/sourcegraph/grafana.log 2>&1";
const BLOBSTORE_LINE: &str = "blobstore: /opt/s3proxy/run-docker-container.sh >> /var/opt/sourcegraph/blobstore.log 2>&1";

/// `shared.DefaultEnv`, in source order.
const DEFAULT_ENV: &[(&str, &str)] = &[
    ("SRC_GIT_SERVERS", "127.0.0.1:3178"),
    ("SEARCHER_URL", "http://127.0.0.1:3181"),
    ("REPO_UPDATER_URL", "http://127.0.0.1:3182"),
    ("QUERY_RUNNER_URL", "http://127.0.0.1:3183"),
    ("SRC_SYNTECT_SERVER", "http://127.0.0.1:9238"),
    ("SYMBOLS_URL", "http://127.0.0.1:3184"),
    ("SRC_HTTP_ADDR", ":8080"),
    ("SRC_HTTPS_ADDR", ":8443"),
    ("SRC_FRONTEND_INTERNAL", FRONTEND_INTERNAL_HOST),
    ("GRAFANA_SERVER_URL", "http://127.0.0.1:3370"),
    ("PROMETHEUS_URL", "http://127.0.0.1:9090"),
    ("OTEL_EXPORTER_OTLP_ENDPOINT", ""),
    ("SEARCHER_CACHE_SIZE_MB", "50000"),
    ("SYMBOLS_CACHE_SIZE_MB", "50000"),
    ("DEPLOY_TYPE", "docker-container"),
    ("SRC_PROF_HTTP", ""),
    ("LOGO", "t"),
];

/// The blobstore env defaults from `maybeBlobstore`, in source order.
const BLOBSTORE_ENV: &[(&str, &str)] = &[
    ("LOG_LEVEL", "info"),
    ("S3PROXY_AUTHORIZATION", "none"),
    ("S3PROXY_ENDPOINT", "http://0.0.0.0:9000"),
    ("S3PROXY_IDENTITY", "local-identity"),
    ("S3PROXY_CREDENTIAL", "local-credential"),
    ("S3PROXY_VIRTUALHOST", ""),
    ("S3PROXY_CORS_ALLOW_ALL", "false"),
    ("S3PROXY_CORS_ALLOW_ORIGINS", ""),
    ("S3PROXY_CORS_ALLOW_METHODS", ""),
    ("S3PROXY_CORS_ALLOW_HEADERS", ""),
    ("S3PROXY_IGNORE_UNKNOWN_HEADERS", "false"),
    ("S3PROXY_ENCRYPTED_BLOBSTORE", ""),
    ("S3PROXY_ENCRYPTED_BLOBSTORE_PASSWORD", ""),
    ("S3PROXY_ENCRYPTED_BLOBSTORE_SALT", ""),
    ("S3PROXY_V4_MAX_NON_CHUNKED_REQ_SIZE", "33554432"),
    ("JCLOUDS_PROVIDER", "filesystem"),
    ("JCLOUDS_ENDPOINT", ""),
    ("JCLOUDS_REGION", ""),
    ("JCLOUDS_REGIONS", "us-east-1"),
    ("JCLOUDS_IDENTITY", "remote-identity"),
    ("JCLOUDS_CREDENTIAL", "remote-credential"),
    ("JCLOUDS_KEYSTONE_VERSION", ""),
    ("JCLOUDS_KEYSTONE_SCOPE", ""),
    ("JCLOUDS_KEYSTONE_PROJECT_DOMAIN_NAME", ""),
    ("S3PROXY_SECURE_ENDPOINT", "https://0.0.0.0:9443"),
    ("S3PROXY_KEYSTORE_PATH", "/opt/s3proxy/test-classes/keystore.jks"),
    ("S3PROXY_KEYSTORE_PASSWORD", "password"),
];

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProfService {
    pub name: String,
    pub host: String,
    pub default_path: Option<String>,
}

impl ProfService {
    fn new(name: &str, host: &str, default_path: Option<&str>) -> Self {
        Self {
            name: name.to_owned(),
            host: host.to_owned(),
            default_path: default_path.map(str::to_owned),
        }
    }

    /// `SRC_PROF_SERVICES` element marshalled by `json.MarshalIndent`
    /// (`[]map[string]string`, so keys are sorted by `encoding/json`).
    pub fn to_json_value(&self) -> serde_json::Value {
        let mut object = serde_json::Map::new();
        if let Some(path) = &self.default_path {
            object.insert("DefaultPath".to_owned(), serde_json::Value::String(path.clone()));
        }
        object.insert("Host".to_owned(), serde_json::Value::String(self.host.clone()));
        object.insert("Name".to_owned(), serde_json::Value::String(self.name.clone()));
        serde_json::Value::Object(object)
    }
}

/// `shared.SrcProfServices` (`cmd/server/shared/globals.go:14-26`).
pub fn default_prof_services() -> Vec<ProfService> {
    vec![
        ProfService::new("frontend", "127.0.0.1:6063", None),
        ProfService::new("gitserver", "127.0.0.1:6068", None),
        ProfService::new("searcher", "127.0.0.1:6069", None),
        ProfService::new("symbols", "127.0.0.1:6071", None),
        ProfService::new("repo-updater", "127.0.0.1:6074", None),
        ProfService::new("worker", "127.0.0.1:6089", None),
        ProfService::new("precise-code-intel-worker", "127.0.0.1:6088", None),
        ProfService::new("embeddings", "127.0.0.1:6099", None),
        ProfService::new("zoekt-indexserver", "127.0.0.1:6072", None),
        ProfService::new(
            "zoekt-webserver",
            "127.0.0.1:3070",
            Some("/debug/requests/"),
        ),
    ]
}

/// `cmd/server/main.go:17-27`: `SRC_ENABLE_EMBEDDINGS` appends both a Procfile
/// line and another `SRC_PROF_SERVICES` entry.
pub fn embeddings_extras(env: &EnvMap) -> (Vec<ProfService>, Vec<String>) {
    if parse_go_bool(&env.value("SRC_ENABLE_EMBEDDINGS")).unwrap_or(false) {
        (
            vec![ProfService::new("embeddings", "127.0.0.1:6099", None)],
            vec!["embeddings: embeddings".to_owned()],
        )
    } else {
        (Vec::new(), Vec::new())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Paths {
    pub config_dir: PathBuf,
    pub data_dir: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppliedEnvironment {
    pub paths: Paths,
    pub allow_single_docker_code_insights: bool,
    pub prof_services: Vec<ProfService>,
}

/// `SRC_PROF_SERVICES` pretty JSON (`json.MarshalIndent(..., "", "  ")`).
pub fn prof_services_json(prof_services: &[ProfService]) -> String {
    let values: Vec<serde_json::Value> = prof_services
        .iter()
        .map(ProfService::to_json_value)
        .collect();
    serde_json::to_string_pretty(&values).expect("prof services are serializable")
}

/// Steps 1–7 of `shared.Main`: `DATA_DIR`/`CONFIG_DIR`, `$CONFIG_DIR/env`,
/// persistence directories, `REDIS`, `SRC_PROF_SERVICES`, `DefaultEnv` and
/// `ALLOW_SINGLE_DOCKER_CODE_INSIGHTS`.
pub fn apply_environment(
    env: &mut EnvMap,
    prof_services: &[ProfService],
) -> Result<AppliedEnvironment, String> {
    // `var DataDir = SetDefaultEnv("DATA_DIR", ...)` runs at package init,
    // i.e. before `$CONFIG_DIR/env` is loaded, so an env file cannot change it.
    env.set_default("DATA_DIR", "/var/opt/sourcegraph");

    env.set_default("CONFIG_DIR", "/etc/sourcegraph");
    let config_dir = PathBuf::from(env.value("CONFIG_DIR"));
    fs::create_dir_all(&config_dir)
        .map_err(|error| format!("failed to ensure CONFIG_DIR exists: {error}"))?;

    let env_file = config_dir.join("env");
    match fs::read_to_string(&env_file) {
        Ok(contents) => {
            for (key, value) in crate::env::parse_dotenv(&contents) {
                env.set_default(key, value);
            }
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => {
            return Err(format!("failed to load {}: {error}", env_file.display()));
        }
    }

    let paths = Paths {
        config_dir,
        data_dir: PathBuf::from(env.value("DATA_DIR")),
    };
    let data_dir = paths.data_dir.clone();

    env.set_default("SRC_REPOS_DIR", data_dir.join("repos").display().to_string());
    let cache_dir = data_dir.join("cache").display().to_string();
    env.set_default("SYMBOLS_CACHE_DIR", cache_dir.clone());
    env.set_default("SEARCHER_CACHE_DIR", cache_dir);

    if env.is_set("REDIS") {
        let redis = env.value("REDIS");
        env.set_default("REDIS_ENDPOINT", redis);
    }

    env.set_default("SRC_PROF_SERVICES", prof_services_json(prof_services));

    for (key, value) in DEFAULT_ENV {
        env.set_default(*key, *value);
    }

    let allow_single_docker_code_insights =
        parse_go_bool(&env.value("ALLOW_SINGLE_DOCKER_CODE_INSIGHTS")).unwrap_or(false);

    Ok(AppliedEnvironment {
        paths,
        allow_single_docker_code_insights,
        prof_services: prof_services.to_vec(),
    })
}

/// `copySSH` (`cmd/server/shared/copy.go:44-67`).
pub fn copy_ssh(env: &EnvMap) -> Result<(), String> {
    let from = Path::new(&env.value("CONFIG_DIR")).join("ssh");
    let metadata = match fs::metadata(&from) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(format!("failed to setup SSH auth: {error}")),
    };
    if !metadata.is_dir() {
        return Err(format!("{} is not a directory", from.display()));
    }
    let home = go_expand_env_public("$HOME", env);
    let to = Path::new(&home).join(".ssh");
    copy_dir_recursive(&from, &to).map_err(|error| error.to_string())?;
    set_permissions_recursive(&to).map_err(|error| error.to_string())
}

fn go_expand_env_public(value: &str, env: &EnvMap) -> String {
    crate::env::go_expand_env(value, &|key| env.value(key))
}

fn copy_dir_recursive(from: &Path, to: &Path) -> std::io::Result<()> {
    fs::create_dir_all(to)?;
    for entry in fs::read_dir(from)? {
        let entry = entry?;
        let target = to.join(entry.file_name());
        if entry.file_type()?.is_dir() {
            copy_dir_recursive(&entry.path(), &target)?;
        } else {
            fs::copy(entry.path(), &target)?;
        }
    }
    Ok(())
}

fn set_permissions_recursive(path: &Path) -> std::io::Result<()> {
    use std::os::unix::fs::PermissionsExt;
    for entry in fs::read_dir(path)? {
        let entry = entry?;
        let file_type = entry.file_type()?;
        if file_type.is_dir() {
            fs::set_permissions(entry.path(), fs::Permissions::from_mode(0o700))?;
            set_permissions_recursive(&entry.path())?;
        } else {
            fs::set_permissions(entry.path(), fs::Permissions::from_mode(0o600))?;
        }
    }
    Ok(())
}

/// `copyConfigs` (`cmd/server/shared/copy.go:18-40`).
pub fn copy_configs(env: &EnvMap) -> Result<(), String> {
    for (name, destination) in [
        ("netrc", "$HOME/.netrc"),
        ("gitconfig", "$HOME/.gitconfig"),
    ] {
        let source = Path::new(&env.value("CONFIG_DIR")).join(name);
        let destination = go_expand_env_public(destination, env);
        let data = match fs::read(&source) {
            Ok(data) => data,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(error) => {
                return Err(format!(
                    "failed to copy {} -> {}: {error}",
                    source.display(),
                    destination
                ));
            }
        };
        fs::write(&destination, data).map_err(|error| {
            format!(
                "failed to copy {} -> {}: {error}",
                source.display(),
                destination
            )
        })?;
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfigFile {
    /// Path relative to `CONFIG_DIR`.
    pub path: String,
    pub sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReindexPlan {
    pub required: bool,
    pub allowed: bool,
    /// `Some(101)` when reindexing is required but not authorized.
    pub exit_code: Option<i32>,
    pub marker_file: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServerPlan {
    pub procfile: Vec<ProcfileEntry>,
    pub postgres_procfile: Vec<ProcfileEntry>,
    pub run_migrations: bool,
    pub proc_died_action: DiedAction,
    pub reindex: ReindexPlan,
    pub config_files: Vec<ConfigFile>,
    pub prof_services: Vec<ProfService>,
    /// Local Postgres provisioning (`cmd/server/shared/postgres.go:67-199`).
    /// `None` when every database is configured to use an external server.
    pub postgres_setup: Option<PostgresSetup>,
}

#[derive(Debug, Clone, Default)]
pub struct PlanExtras {
    pub procfile_additions: Vec<String>,
    pub prof_service_additions: Vec<ProfService>,
}

impl PlanExtras {
    /// Merge `cmd/server/main.go`'s embeddings additions into the shared
    /// defaults the way `Main` does before computing `SRC_PROF_SERVICES`.
    pub fn from_environment(env: &EnvMap, prof_services: &[ProfService]) -> (Vec<ProfService>, Self) {
        let (prof_additions, procfile_additions) = embeddings_extras(env);
        let mut prof = prof_services.to_vec();
        prof.extend(prof_additions.iter().cloned());
        (
            prof,
            Self {
                procfile_additions,
                prof_service_additions: prof_additions,
            },
        )
    }
}

fn sha256_hex(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    digest.iter().map(|byte| format!("{byte:02x}")).collect()
}

/// `os.WriteFile` with an explicit mode. Creation applies `mode`; an existing
/// file keeps its permissions, matching the baseline.
fn write_with_mode(path: &Path, contents: &[u8], mode: u32) -> std::io::Result<()> {
    use std::io::Write;
    use std::os::unix::fs::OpenOptionsExt;
    let mut file = fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .mode(mode)
        .open(path)?;
    file.write_all(contents)
}

fn write_config_file(
    config_dir: &Path,
    relative: &str,
    contents: &str,
    mode: u32,
) -> Result<(), String> {
    let path = config_dir.join(relative);
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .map_err(|error| format!("failed to create {}: {error}", parent.display()))?;
    }
    write_with_mode(&path, contents.as_bytes(), mode)
        .map_err(|error| format!("failed to write {}: {error}", path.display()))
}

/// `nginxProcFile` (`cmd/server/shared/nginx.go`).
fn write_nginx(env: &mut EnvMap, config_dir: &Path, files: &mut Vec<ConfigFile>) -> Result<String, String> {
    let relative = "nginx.conf";
    let path = config_dir.join(relative);
    if !path.exists() {
        write_with_mode(&path, NGINX_CONF.as_bytes(), 0o600).map_err(|error| {
            format!(
                "failed to generate nginx configuration to {}: {error}",
                config_dir.display()
            )
        })?;
    }
    let nginx_dir = config_dir.join("nginx");
    fs::create_dir_all(&nginx_dir).map_err(|error| {
        format!(
            "failed to generate nginx configuration to {}: {error}",
            config_dir.display()
        )
    })?;
    for (name, contents) in NGINX_DIR {
        // `nginxWriteFiles` writes these with `os.WriteFile(..., 0600)`.
        write_with_mode(&nginx_dir.join(name), contents.as_bytes(), 0o600).map_err(|error| {
            format!(
                "failed to generate nginx configuration to {}: {error}",
                config_dir.display()
            )
        })?;
    }
    let defaults = config_dir.join(relative);
    files.push(ConfigFile {
        path: relative.to_owned(),
        sha256: sha256_hex(&fs::read(&defaults).unwrap_or_default()),
    });
    for (name, _) in NGINX_DIR {
        let relative = format!("nginx/{name}");
        files.push(ConfigFile {
            path: relative.clone(),
            sha256: sha256_hex(&fs::read(config_dir.join(&relative)).unwrap_or_default()),
        });
    }
    env.set_default("SRC_NGINX_HTTP_ADDR", ":7080");
    Ok(format!(
        "nginx: nginx -p . -g 'daemon off;' -c {} 2>&1 | grep -v 'could not open error log file' 1>&2",
        path.display()
    ))
}

/// `convertLogLevel` (`cmd/server/shared/postgres.go:221-238`).
pub fn convert_log_level(level: &str) -> &'static str {
    match level {
        "dbug" => "debug",
        "info" => "info",
        "warn" => "warn",
        "error" => "error",
        "crit" => "fatal",
        _ => "warn",
    }
}

fn percent_encode_go(value: &str, userinfo: bool) -> String {
    let mut out = String::with_capacity(value.len());
    for byte in value.bytes() {
        let escape = if userinfo {
            // encodeUserPassword: only @ / ? : are escaped beyond the default.
            !(byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'~' | b'$' | b'&' | b'+' | b',' | b';' | b'='))
        } else {
            // encodePath: only `?` is escaped beyond the default.
            !(byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'~' | b'$' | b'&' | b'+' | b',' | b'/' | b':' | b';' | b'=' | b'@'))
        };
        if escape {
            out.push_str(&format!("%{byte:02X}"));
        } else {
            out.push(byte as char);
        }
    }
    out
}

fn percent_encode_host(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for byte in value.bytes() {
        let allowed = byte.is_ascii_alphanumeric()
            || matches!(
                byte,
                b'-' | b'_'
                    | b'.'
                    | b'~'
                    | b'!'
                    | b'$'
                    | b'&'
                    | b'\''
                    | b'('
                    | b')'
                    | b'*'
                    | b'+'
                    | b','
                    | b';'
                    | b'='
                    | b':'
                    | b'['
                    | b']'
                    | b'<'
                    | b'>'
                    | b'"'
            );
        if allowed {
            out.push(byte as char);
        } else {
            out.push_str(&format!("%{byte:02X}"));
        }
    }
    out
}

fn query_encode_go(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for byte in value.bytes() {
        if byte == b' ' {
            out.push('+');
        } else if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'~') {
            out.push(byte as char);
        } else {
            out.push_str(&format!("%{byte:02X}"));
        }
    }
    out
}

fn dequote(value: &str) -> String {
    if value.len() > 2 {
        let bytes = value.as_bytes();
        if bytes[0] == bytes[bytes.len() - 1] && (bytes[0] == b'\'' || bytes[0] == b'"') {
            return value[1..value.len() - 1].to_owned();
        }
    }
    value.to_owned()
}

/// `postgresdsn.New` (`internal/database/postgresdsn/postgresdsn.go`).
pub fn new_postgres_dsn(prefix: &str, current_user: &str, get: &dyn Fn(&str) -> String) -> String {
    let prefix = if prefix == "frontend" { "" } else { prefix };
    let prefix = if prefix.is_empty() {
        String::new()
    } else {
        format!("{}_", prefix.to_ascii_uppercase())
    };
    let env = |name: &str| get(&format!("{prefix}{name}"));
    let non_empty = |value: String| {
        if value.is_empty() {
            None
        } else {
            Some(value)
        }
    };

    if let Some(dsn) = non_empty(env("PGDATASOURCE")) {
        return dsn;
    }

    let mut host = "127.0.0.1:5432".to_owned();
    let mut username = if current_user.is_empty() {
        "postgres".to_owned()
    } else {
        current_user.to_owned()
    };
    if let Some(user) = non_empty(env("PGUSER")) {
        username = user;
    }
    let password = non_empty(env("PGPASSWORD"));
    if let Some(host_value) = non_empty(env("PGHOST")) {
        host = host_value;
    }
    let port = dequote(&env("PGPORT"));
    if !port.is_empty() {
        host = format!("{}:{}", host.split(':').next().unwrap_or(""), port);
    }
    let database = non_empty(env("PGDATABASE"));
    let sslmode = non_empty(env("PGSSLMODE"));
    let timezone = non_empty(env("PGTZ"));

    let mut out = String::from("postgres://");
    out.push_str(&percent_encode_go(&username, true));
    if let Some(password) = &password {
        out.push(':');
        out.push_str(&percent_encode_go(password, true));
    }
    out.push('@');
    out.push_str(&percent_encode_host(&host));
    if let Some(database) = &database {
        out.push('/');
        out.push_str(&percent_encode_go(database, false));
    }
    let mut query: Vec<(&str, String)> = Vec::new();
    if let Some(sslmode) = sslmode {
        query.push(("sslmode", sslmode));
    }
    if let Some(timezone) = timezone {
        query.push(("timezone", timezone));
    }
    if !query.is_empty() {
        query.sort_by_key(|(key, _)| *key);
        out.push('?');
        for (index, (key, value)) in query.iter().enumerate() {
            if index > 0 {
                out.push('&');
            }
            out.push_str(key);
            out.push('=');
            out.push_str(&query_encode_go(value));
        }
    }
    out
}

/// `deploy.BlobstoreDefaultEndpoint` (`internal/conf/deploy/endpoints.go`).
pub fn blobstore_default_endpoint(env: &EnvMap) -> String {
    let deploy_type = env.value("DEPLOY_TYPE");
    let deploy_type = if deploy_type.is_empty() {
        "kubernetes"
    } else {
        deploy_type.as_str()
    };
    if deploy_type == "docker-container" {
        "http://127.0.0.1:9000".to_owned()
    } else {
        "http://blobstore:9000".to_owned()
    }
}

fn maybe_observability(env: &EnvMap) -> Vec<String> {
    if !env.value("DISABLE_OBSERVABILITY").is_empty() {
        return vec![String::new()];
    }

    // The observability feature may not have been packaged at all. Starting a
    // missing binary would take the whole container down, so only add the
    // prom-wrapper line when it is present; Grafana is likewise optional.
    if !std::path::Path::new("/bin/prom-wrapper").is_file() {
        return vec![String::new()];
    }
    let mut lines = vec![PROMETHEUS_LINE.to_owned()];
    if std::path::Path::new("/usr/share/grafana/bin/grafana-server").is_file() {
        lines.push(GRAFANA_LINE.to_owned());
    }
    lines
}

fn maybe_blobstore(env: &mut EnvMap, data_dir: &Path) -> Vec<String> {
    if !env.value("DISABLE_BLOBSTORE").is_empty() {
        return vec![String::new()];
    }
    let endpoint = blobstore_default_endpoint(env);
    env.set_default("PRECISE_CODE_INTEL_UPLOAD_AWS_ENDPOINT", endpoint);
    env.set_default("PRECISE_CODE_INTEL_UPLOAD_BACKEND", "blobstore");
    let blobstore_dir = data_dir.join("blobstore");
    env.set_default("JCLOUDS_FILESYSTEM_BASEDIR", blobstore_dir.display().to_string());
    for (key, value) in BLOBSTORE_ENV {
        env.set_default(*key, *value);
    }
    let _ = fs::create_dir_all(env.value("JCLOUDS_FILESYSTEM_BASEDIR"));
    vec![BLOBSTORE_LINE.to_owned()]
}

struct RedisSpec<'a> {
    env_var: &'a str,
    name: &'a str,
    port: &'a str,
    template: &'a str,
    data_dir_name: &'a str,
}

fn render_redis_template(template: &str, dir: &str, port: &str) -> String {
    // The baseline template only contains `{{ .Dir }}` and `{{ .Port }}`.
    template
        .replace("{{ .Port }}", port)
        .replace("{{ .Dir }}", dir)
}

fn maybe_redis(
    env: &mut EnvMap,
    paths: &Paths,
    spec: &RedisSpec<'_>,
    files: &mut Vec<ConfigFile>,
) -> Result<Option<String>, String> {
    if !env.value("REDIS_ENDPOINT").is_empty() || !env.value(spec.env_var).is_empty() {
        return Ok(None);
    }
    let data_dir = paths.data_dir.join(spec.data_dir_name);
    let rendered = render_redis_template(spec.template, &data_dir.display().to_string(), spec.port);
    fs::create_dir_all(&data_dir)
        .map_err(|error| format!("failed to create {}: {error}", data_dir.display()))?;
    let relative = format!("{}.conf", spec.name);
    // `tryCreateRedisConf` writes with `os.WriteFile(..., 0644)`.
    write_config_file(&paths.config_dir, &relative, &rendered, 0o644)?;
    files.push(ConfigFile {
        path: relative.clone(),
        sha256: sha256_hex(rendered.as_bytes()),
    });
    env.set_default(spec.env_var, format!("127.0.0.1:{}", spec.port));
    Ok(Some(format!(
        "{}: redis-server {} | tail -n 1",
        spec.name,
        paths.config_dir.join(&relative).display()
    )))
}

fn maybe_zoekt(env: &mut EnvMap, data_dir: &Path) -> Vec<String> {
    if !env.value("ZOEKT_HOST").is_empty() || !env.value("INDEXED_SEARCH_SERVERS").is_empty() {
        return Vec::new();
    }
    let default_host = "127.0.0.1:3070";
    env.set_default("INDEXED_SEARCH_SERVERS", default_host);
    let frontend_internal_host = env.value("SRC_FRONTEND_INTERNAL");
    let index_dir = data_dir.join("zoekt/index");
    vec![
        format!(
            "zoekt-indexserver: env GOGC=25 HOSTNAME={default_host} zoekt-sourcegraph-indexserver -sourcegraph_url http://{frontend_internal_host} -index {} -interval 1m -listen 127.0.0.1:6072 -cpu_fraction 0.25",
            index_dir.display()
        ),
        format!(
            "zoekt-webserver: env GOGC=25 zoekt-webserver -rpc -pprof -indexserver_proxy -listen {default_host} -index {}",
            index_dir.display()
        ),
    ]
}

/// `databases` map from `postgres.go`, in insertion order (`""` first).
fn postgres_databases(allow_code_insights: bool) -> Vec<(&'static str, &'static str)> {
    let mut databases = vec![
        ("", "sourcegraph"),
        ("CODEINTEL_", "sourcegraph-codeintel"),
    ];
    if allow_code_insights {
        databases.push(("CODEINSIGHTS_", "sourcegraph-codeinsights"));
    }
    databases
}

fn is_postgres_configured(env: &EnvMap, prefix: &str) -> bool {
    !env.value(&format!("{prefix}PGHOST")).is_empty()
        || !env.value(&format!("{prefix}PGDATASOURCE")).is_empty()
}

/// One command run through Go's `execer` in `postgresProcfile`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PostgresCommand {
    pub program: String,
    pub args: Vec<String>,
    /// `CommandWithFilter`: a failure whose combined output contains this text
    /// is ignored (the `createdb ... already exists` upgrade path,
    /// `postgres.go:161-172`).
    pub ignore_error_contains: Option<String>,
    /// `os.RemoveAll` of this path when the command fails (`postgres.go:92,120`).
    pub cleanup_path: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PostgresAction {
    Run(PostgresCommand),
    /// `os.Create` + `WriteString` (`postgres.go:125-134`).
    WriteFile { path: String, contents: String },
    /// `pgPrintf` line, printed as `✱ <message>`.
    Message(String),
}

/// Ordered local-Postgres provisioning actions plus their log messages.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PostgresSetup {
    pub actions: Vec<PostgresAction>,
}

fn run_command(program: &str, args: &[&str]) -> PostgresAction {
    PostgresAction::Run(PostgresCommand {
        program: program.to_owned(),
        args: args.iter().map(|arg| (*arg).to_owned()).collect(),
        ignore_error_contains: None,
        cleanup_path: None,
    })
}

fn run_command_cleanup(program: &str, args: &[&str], cleanup: &Path) -> PostgresAction {
    PostgresAction::Run(PostgresCommand {
        program: program.to_owned(),
        args: args.iter().map(|arg| (*arg).to_owned()).collect(),
        ignore_error_contains: None,
        cleanup_path: Some(cleanup.display().to_string()),
    })
}

/// `fileExists` (`postgres.go:201-211`): only `NotExist` is a false result.
fn file_exists(path: &Path) -> Result<bool, String> {
    match fs::metadata(path) {
        Ok(_) => Ok(true),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(format!("failed to stat {}: {error}", path.display())),
    }
}

/// `postgresProcfile` file/setup half (`cmd/server/shared/postgres.go:67-199`).
///
/// The plan is deterministic: `databases` follows the Go map's insertion order
/// (`""`, `CODEINTEL_`, optional `CODEINSIGHTS_`) instead of Go's randomized
/// map iteration, which only affects the order of independent `createdb`
/// calls.
pub fn plan_postgres_setup(
    env: &EnvMap,
    databases: &[(&str, &str)],
) -> Result<PostgresSetup, String> {
    let data_dir = Path::new(&env.value("DATA_DIR")).to_path_buf();
    let path = data_dir.join("postgresql");
    let markers_path = data_dir.join("postgresql-markers");
    let marker_sourcegraph = markers_path.join("sourcegraph");

    let mut actions = Vec::new();

    // Postgres needs to be able to write to run.
    actions.push(run_command("mkdir", &["-p", "/run/postgresql"]));
    actions.push(run_command("chown", &["-R", "postgres", "/run/postgresql"]));

    if !file_exists(&markers_path)? {
        let (mkdir_path, touch_path) = (
            markers_path.display().to_string(),
            marker_sourcegraph.display().to_string(),
        );
        actions.push(run_command_cleanup("mkdir", &["-p", &mkdir_path], &path));
        actions.push(run_command_cleanup("touch", &[&touch_path], &path));
    }

    if !file_exists(&path)? {
        let path_text = path.display().to_string();
        if matches!(env.value("SRC_LOG_LEVEL").as_str(), "dbug" | "info") {
            actions.push(PostgresAction::Message(format!(
                "Setting up PostgreSQL at {path_text}"
            )));
        }
        actions.push(PostgresAction::Message(
            "Sourcegraph is initializing the internal database... (may take 15-20 seconds)"
                .to_owned(),
        ));

        actions.push(run_command_cleanup("mkdir", &["-p", &path_text], &path));
        actions.push(run_command_cleanup("chown", &["postgres", &path_text], &path));
        // initdb --nosync saves ~3-15s on macOS during initial startup.
        actions.push(run_command_cleanup(
            "su-exec",
            &["postgres", "initdb", "-D", &path_text, "--nosync"],
            &path,
        ));
        actions.push(run_command_cleanup(
            "su-exec",
            &[
                "postgres",
                "pg_ctl",
                "-D",
                &path_text,
                "-o -c listen_addresses=127.0.0.1",
                "-l",
                "/tmp/pgsql.log",
                "-w",
                "start",
            ],
            &path,
        ));
        for (_, database) in databases {
            let marker = markers_path.join(database).display().to_string();
            actions.push(run_command_cleanup(
                "su-exec",
                &["postgres", "createdb", database],
                &path,
            ));
            actions.push(run_command_cleanup("touch", &[&marker], &path));
        }
        actions.push(run_command_cleanup(
            "su-exec",
            &[
                "postgres",
                "pg_ctl",
                "-D",
                &path_text,
                "-m",
                "fast",
                "-l",
                "/tmp/pgsql.log",
                "-w",
                "stop",
            ],
            &path,
        ));

        // The DB was initialized by Sourcegraph >=5.1, so reindexing is not
        // required (`postgresReindexMarkerFile`).
        actions.push(PostgresAction::WriteFile {
            path: path.join("5.1-reindex.completed").display().to_string(),
            contents: "Database initialised by Sourcegraph 5.1 or later\n".to_owned(),
        });
    } else {
        // Between restarts the owner of the volume may have changed.
        let path_text = path.display().to_string();
        actions.push(run_command("chown", &["-R", "postgres", &path_text]));

        let mut missing_databases: Vec<&str> = Vec::new();
        for (_, database) in databases {
            if !file_exists(&markers_path.join(database))? {
                missing_databases.push(database);
            }
        }
        if !missing_databases.is_empty() {
            actions.push(PostgresAction::Message(format!(
                "Sourcegraph is creating missing databases {}... (may take 15-20 seconds)",
                missing_databases.join(", ")
            )));
            actions.push(run_command(
                "su-exec",
                &[
                    "postgres",
                    "pg_ctl",
                    "-D",
                    &path_text,
                    "-o -c listen_addresses=127.0.0.1",
                    "-l",
                    "/tmp/pgsql.log",
                    "-w",
                    "start",
                ],
            ));
            for database in &missing_databases {
                // Ignore the 3.21.0 -> 3.21.1 upgrade path where the database
                // exists but its marker file does not.
                actions.push(PostgresAction::Run(PostgresCommand {
                    program: "su-exec".to_owned(),
                    args: vec![
                        "postgres".to_owned(),
                        "createdb".to_owned(),
                        (*database).to_owned(),
                    ],
                    ignore_error_contains: Some(format!(
                        "ERROR:  database \"{database}\" already exists"
                    )),
                    cleanup_path: None,
                }));
                actions.push(run_command(
                    "touch",
                    &[&markers_path.join(database).display().to_string()],
                ));
            }
            actions.push(run_command(
                "su-exec",
                &[
                    "postgres",
                    "pg_ctl",
                    "-D",
                    &path_text,
                    "-m",
                    "fast",
                    "-l",
                    "/tmp/pgsql.log",
                    "-w",
                    "stop",
                ],
            ));
        }
    }

    actions.push(PostgresAction::Message(
        "Finished initializing the internal database.".to_owned(),
    ));

    Ok(PostgresSetup { actions })
}

/// Execute a [`PostgresSetup`] with the same command/output semantics as Go's
/// `execer` + `pgPrintf`. Returns the error that `shared.Main` would have
/// turned into `log.Fatal`.
pub fn execute_postgres_setup(setup: &PostgresSetup) -> Result<(), String> {
    for action in &setup.actions {
        match action {
            PostgresAction::Message(message) => eprintln!("✱ {message}"),
            PostgresAction::WriteFile { path, contents } => {
                fs::write(path, contents)
                    .map_err(|error| format!("failed to write {path}: {error}"))?;
            }
            PostgresAction::Run(command) => {
                let output = std::process::Command::new(&command.program)
                    .args(&command.args)
                    .output();
                let (failed, combined) = match &output {
                    Ok(output) => {
                        let mut bytes = output.stdout.clone();
                        bytes.extend_from_slice(&output.stderr);
                        (
                            !output.status.success(),
                            String::from_utf8_lossy(&bytes).into_owned(),
                        )
                    }
                    Err(error) => (true, error.to_string()),
                };
                if !failed {
                    continue;
                }
                if let Some(needle) = &command.ignore_error_contains {
                    if combined.contains(needle.as_str()) {
                        continue;
                    }
                }
                if let Some(path) = &command.cleanup_path {
                    // `os.RemoveAll`: remove either kind, ignoring errors.
                    match fs::symlink_metadata(path) {
                        Ok(metadata) if metadata.is_dir() => {
                            let _ = fs::remove_dir_all(path);
                        }
                        Ok(_) => {
                            let _ = fs::remove_file(path);
                        }
                        Err(_) => {}
                    }
                }
                eprintln!("✱ Setting up postgres failed:\n{combined}");
                return Err(combined.trim_end().to_owned());
            }
        }
    }
    Ok(())
}

/// The final `postgres:` supervisor line from `postgresProcfile`.
fn local_postgres_line(data_dir: &Path) -> String {
    let path = data_dir.join("postgresql");
    let ignored_logs = [
        "database system was shut down",
        "MultiXact member wraparound",
        "database system is ready",
        "autovacuum launcher started",
        "the database system is starting up",
        "listening on IPv4 address",
    ];
    let greps: Vec<String> = ignored_logs
        .iter()
        .map(|log| format!("grep -v '{log}'"))
        .collect();
    format!(
        "postgres: su-exec postgres sh -c 'postgres -c listen_addresses=127.0.0.1 -D {}' 2>&1 | {}",
        path.display(),
        greps.join(" | ")
    )
}

/// Exit code for every `shared.Main` assembly failure (`log.Fatalf` and the
/// `copySSH`/`copyConfigs` fatal paths).
pub const EXIT_ASSEMBLY_FAILURE: i32 = 1;

/// `runPostgresReindex`: printed when the 5.1 reindex is required but
/// `SOURCEGRAPH_5_1_DB_MIGRATION` is not `"true"` (`os.Exit(101)`).
pub const EXIT_MIGRATION_REQUIRED: i32 = 101;

/// `shouldPostgresReindex` (`cmd/server/shared/shared.go:293-315`).
pub fn should_postgres_reindex(env: &EnvMap) -> bool {
    let marker = reindex_marker_file(env);
    if Path::new(&marker).exists() {
        return false;
    }
    let pg_host = env.value("PGHOST");
    if !(pg_host.is_empty()
        || pg_host == "127.0.0.1"
        || pg_host == "localhost"
        || pg_host.starts_with('/'))
    {
        return false;
    }
    true
}

/// `postgresReindexMarkerFile`: `$DATA_DIR/postgresql/5.1-reindex.completed`.
pub fn reindex_marker_file(env: &EnvMap) -> String {
    Path::new(&env.value("DATA_DIR"))
        .join("postgresql/5.1-reindex.completed")
        .display()
        .to_string()
}

/// Steps 8–11 of `shared.Main`: generated files, the fixed Procfile entries,
/// the conditional additions and the `PGRESTORE`/`IGNORE_PROCESS_DEATH`
/// switches.
pub fn build_plan(
    env: &mut EnvMap,
    applied: &AppliedEnvironment,
    extras: &PlanExtras,
) -> Result<ServerPlan, String> {
    let paths = &applied.paths;
    let mut config_files = Vec::new();

    let nginx_line = write_nginx(env, &paths.config_dir, &mut config_files)?;

    let dsn = new_postgres_dsn("", "postgres", &|key| env.value(key));
    let postgres_exporter_line = format!(
        "postgres_exporter: env DATA_SOURCE_NAME=\"{}\" postgres_exporter --config.file=\"/postgres_exporter.yaml\" --log.level={}",
        dsn,
        convert_log_level(&env.value("SRC_LOG_LEVEL"))
    );
    let gitserver_line = format!(
        "gitserver: env HOSTNAME={} gitserver",
        go_quote(&env.value("SRC_GIT_SERVERS"))
    );

    let mut procfile = vec![
        nginx_line,
        "frontend: env CONFIGURATION_MODE=server frontend".to_owned(),
        gitserver_line,
        "symbols: symbols".to_owned(),
        "searcher: searcher".to_owned(),
        "worker: worker".to_owned(),
        "repo-updater: repo-updater".to_owned(),
        "precise-code-intel-worker: precise-code-intel-worker".to_owned(),
        SYNTECT_LINE.to_owned(),
        postgres_exporter_line,
    ];
    procfile.extend(extras.procfile_additions.iter().cloned());
    procfile.extend(maybe_observability(env));
    procfile.extend(maybe_blobstore(env, &paths.data_dir));
    if let Some(line) = maybe_redis(
        env,
        paths,
        &RedisSpec {
            env_var: "REDIS_STORE_ENDPOINT",
            name: "redis-store",
            port: "6379",
            template: REDIS_STORE_TMPL,
            data_dir_name: "redis",
        },
        &mut config_files,
    )? {
        procfile.push(line);
    }
    if let Some(line) = maybe_redis(
        env,
        paths,
        &RedisSpec {
            env_var: "REDIS_CACHE_ENDPOINT",
            name: "redis-cache",
            port: "6380",
            template: REDIS_CACHE_TMPL,
            data_dir_name: "redis-cache",
        },
        &mut config_files,
    )? {
        procfile.push(line);
    }
    procfile.extend(maybe_zoekt(env, &paths.data_dir));

    let databases = postgres_databases(applied.allow_single_docker_code_insights);
    let missing_external_config = databases
        .iter()
        .any(|(prefix, _)| !is_postgres_configured(env, prefix));
    let (postgres_line, postgres_setup) = if missing_external_config {
        for (prefix, database) in &databases {
            if !is_postgres_configured(env, prefix) {
                env.set_default(format!("{prefix}PGHOST"), "127.0.0.1");
                env.set_default(format!("{prefix}PGUSER"), "postgres");
                env.set_default(format!("{prefix}PGDATABASE"), *database);
                env.set_default(format!("{prefix}PGSSLMODE"), "disable");
            }
        }
        (
            Some(local_postgres_line(&paths.data_dir)),
            Some(plan_postgres_setup(env, &databases)?),
        )
    } else {
        (None, None)
    };

    let restore = parse_go_bool(&env.value("PGRESTORE")).unwrap_or(false);
    let (procfile, postgres_procfile) = if restore {
        match postgres_line {
            Some(line) => (vec![line], Vec::new()),
            None => {
                return Err(
                    "PGRESTORE is set but a local Postgres instance is not configured".to_owned(),
                )
            }
        }
    } else {
        (procfile, postgres_line.into_iter().collect())
    };

    let proc_died_action = if parse_go_bool(&env.value("IGNORE_PROCESS_DEATH")).unwrap_or(false) {
        DiedAction::Ignore
    } else {
        DiedAction::Shutdown
    };

    let required = should_postgres_reindex(env);
    let allowed = env.value("SOURCEGRAPH_5_1_DB_MIGRATION") == "true";
    let reindex = ReindexPlan {
        required,
        allowed,
        exit_code: if required && !allowed { Some(101) } else { None },
        marker_file: reindex_marker_file(env),
    };

    config_files.sort_by(|left, right| left.path.cmp(&right.path));

    Ok(ServerPlan {
        procfile: launcher::parse_procfile(&procfile),
        postgres_procfile: launcher::parse_procfile(&postgres_procfile),
        run_migrations: !restore,
        proc_died_action,
        reindex,
        config_files,
        prof_services: applied.prof_services.clone(),
        postgres_setup,
    })
}

/// The `run()` sequence from `shared.go:231-261`, minus actual process
/// execution: returns the ordered launch stages for a supervisor.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LaunchStages {
    pub postgres: Vec<ProcfileEntry>,
    pub application: Vec<ProcfileEntry>,
    pub run_migrations: bool,
    pub schemas: Vec<String>,
}

pub fn launch_stages(plan: &ServerPlan, applied: &AppliedEnvironment) -> LaunchStages {
    let (postgres, application, run_migrations) = if plan.run_migrations {
        (
            plan.postgres_procfile.clone(),
            plan.procfile.clone(),
            true,
        )
    } else {
        let mut application = plan.procfile.clone();
        application.extend(plan.postgres_procfile.iter().cloned());
        (Vec::new(), application, false)
    };
    let schemas = crate::migrator::migration_schemas(applied.allow_single_docker_code_insights);
    LaunchStages {
        postgres,
        application,
        run_migrations,
        schemas,
    }
}

fn print_reindex_required() {
    println!("Starting Postgres reindex process");
    println!();
    println!("**************** MIGRATION REQUIRED **************");
    println!();
    println!("Upgrading to Sourcegraph 5.1 or later from an earlier release requires a database reindex.");
    println!();
    println!("This process may take several hours, depending on the size of your database.");
    println!();
    println!("If you do not wish to perform the reindex process now, you should switch back to a release before Sourcegraph 5.1.");
    println!();
    println!("To perform the reindexing process now, please review the instructions at https://sourcegraph.com/docs/admin/migration/5_1 and restart the container with the environment variable `SOURCEGRAPH_5_1_DB_MIGRATION=true` set.");
    println!();
    println!("**************** MIGRATION REQUIRED **************");
    println!();
}

fn run_postgres_reindex(env: &EnvMap) -> Option<i32> {
    if env.value("SOURCEGRAPH_5_1_DB_MIGRATION") != "true" {
        print_reindex_required();
        return Some(EXIT_MIGRATION_REQUIRED);
    }
    println!("Starting Postgres reindex process");
    let marker = reindex_marker_file(env);
    let pgdata = Path::new(&env.value("DATA_DIR")).join("postgresql");
    let status = std::process::Command::new("/bin/bash")
        .arg("/reindex.sh")
        .envs(env.iter().map(|(key, value)| (key, value)))
        .env("REINDEX_COMPLETED_FILE", marker)
        .env("PGDATA", pgdata)
        .env("PGHOST", "")
        .status();
    match status {
        Ok(status) if status.success() => None,
        Ok(status) => {
            println!("Error running database migration: {status}");
            Some(EXIT_ASSEMBLY_FAILURE)
        }
        Err(error) => {
            println!("Error running database migration: {error}");
            Some(EXIT_ASSEMBLY_FAILURE)
        }
    }
}

/// Export the applied environment to the process environment, mirroring Go's
/// `SetDefaultEnv` (`cmd/server/shared/conf_parse.go:8-17`), which calls
/// `os.Setenv`. The Processfile children already receive the map explicitly,
/// but the Postgres provisioning commands (`initdb`, `pg_ctl`, `createdb`) and
/// the reindex helper inherit the process environment. Without this export
/// they miss e.g. `PGHOST`/`PGPORT`; Debian's `pg_wrapper` then falls back to
/// its own cluster lookup instead of the local server.
pub fn export_process_env(env: &EnvMap) {
    for (key, value) in env.iter() {
        if std::env::var_os(key).is_none() {
            std::env::set_var(key, value);
        }
    }
}

/// Full `cmd/server` entry point after `assets.Init`/`sanitycheck.Pass`
/// (`bins/server/src/main.rs`). Returns the process exit code
/// ([`EXIT_ASSEMBLY_FAILURE`] or [`EXIT_MIGRATION_REQUIRED`] on the early
/// failure paths).
pub async fn run_from_env() -> i32 {
    let mut env = EnvMap::from_process();
    let base_prof_services = default_prof_services();
    let (prof_services, extras) = PlanExtras::from_environment(&env, &base_prof_services);

    let applied = match apply_environment(&mut env, &prof_services) {
        Ok(applied) => applied,
        Err(error) => {
            eprintln!("{error}");
            return EXIT_ASSEMBLY_FAILURE;
        }
    };

    if let Err(error) = copy_ssh(&env) {
        eprintln!("Failed to setup SSH authorization: {error}");
        eprintln!("SSH authorization required for cloning from your codehost. Please see README.");
        return EXIT_ASSEMBLY_FAILURE;
    }
    if let Err(error) = copy_configs(&env) {
        eprintln!("Failed to copy configs: {error}");
        return EXIT_ASSEMBLY_FAILURE;
    }

    let mut plan = match build_plan(&mut env, &applied, &extras) {
        Ok(plan) => plan,
        Err(error) => {
            eprintln!("{error}");
            return EXIT_ASSEMBLY_FAILURE;
        }
    };
    // `build_plan` is where the `PGHOST`/`PGUSER`/`PGDATABASE`/`PGSSLMODE`
    // defaults and the blobstore/zoekt/redis additions land, so export after
    // it and before any provisioning command runs.
    export_process_env(&env);

    // `maybePostgresProcFile` provisions the local database while the Procfile
    // is assembled, before `run` checks the 5.1 reindex gate. The gate must be
    // evaluated after provisioning because a fresh init writes the marker.
    if let Some(setup) = &plan.postgres_setup {
        if let Err(error) = execute_postgres_setup(setup) {
            eprintln!("✱ {error}");
            return EXIT_ASSEMBLY_FAILURE;
        }
        plan.reindex.required = should_postgres_reindex(&env);
        plan.reindex.exit_code = if plan.reindex.required && !plan.reindex.allowed {
            Some(EXIT_MIGRATION_REQUIRED)
        } else {
            None
        };
    }

    if plan.reindex.required {
        if let Some(code) = run_postgres_reindex(&env) {
            return code;
        }
    }

    let stages = launch_stages(&plan, &applied);
    let mut migrator = CommandMigrator::new();
    launcher::run_staged(
        &stages.postgres,
        &stages.application,
        stages.run_migrations.then_some(stages.schemas.as_slice()),
        &mut migrator,
        plan.proc_died_action,
        &env,
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn export_process_env_sets_defaults_without_overwriting() {
        let absent = "SG_ENTRY_TEST_EXPORT_ABSENT";
        let present = "SG_ENTRY_TEST_EXPORT_PRESENT";
        std::env::remove_var(absent);
        std::env::set_var(present, "from-process");
        let env = EnvMap::from_pairs([
            (absent, "from-map"),
            (present, "from-map"),
        ]);
        export_process_env(&env);
        assert_eq!(std::env::var(absent).as_deref(), Ok("from-map"));
        assert_eq!(std::env::var(present).as_deref(), Ok("from-process"));
        std::env::remove_var(absent);
        std::env::remove_var(present);
    }

    #[test]
    fn postgres_dsn_matches_go() {
        let env = EnvMap::from_pairs([
            ("PGHOST", "127.0.0.1"),
            ("PGUSER", "postgres"),
            ("PGDATABASE", "sourcegraph"),
            ("PGSSLMODE", "disable"),
        ]);
        assert_eq!(
            new_postgres_dsn("", "postgres", &|key| env.value(key)),
            "postgres://postgres@127.0.0.1/sourcegraph?sslmode=disable"
        );

        let env = EnvMap::from_pairs([
            ("PGPASSWORD", "p@ss:w/rd"),
            ("PGHOST", "db.example"),
            ("PGPORT", "6543"),
            ("PGDATABASE", "my db"),
            ("PGSSLMODE", "require"),
            ("PGTZ", "UTC"),
        ]);
        assert_eq!(
            new_postgres_dsn("", "postgres", &|key| env.value(key)),
            "postgres://postgres:p%40ss%3Aw%2Frd@db.example:6543/my%20db?sslmode=require&timezone=UTC"
        );
    }

    #[test]
    fn log_level_conversion() {
        assert_eq!(convert_log_level("dbug"), "debug");
        assert_eq!(convert_log_level("crit"), "fatal");
        assert_eq!(convert_log_level("nope"), "warn");
    }

    fn temp_data_dir(tag: &str) -> PathBuf {
        let root = std::env::temp_dir().join(format!(
            "m949a-sg-entry-{}-{tag}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(&root).expect("create temp data dir");
        root
    }

    fn run(program: &str, args: &[&str]) -> PostgresAction {
        PostgresAction::Run(PostgresCommand {
            program: program.to_owned(),
            args: args.iter().map(|arg| (*arg).to_owned()).collect(),
            ignore_error_contains: None,
            cleanup_path: None,
        })
    }

    fn run_cleanup(program: &str, args: &[&str], cleanup: &str) -> PostgresAction {
        PostgresAction::Run(PostgresCommand {
            program: program.to_owned(),
            args: args.iter().map(|arg| (*arg).to_owned()).collect(),
            ignore_error_contains: None,
            cleanup_path: Some(cleanup.to_owned()),
        })
    }

    fn databases() -> Vec<(&'static str, &'static str)> {
        vec![("", "sourcegraph"), ("CODEINTEL_", "sourcegraph-codeintel")]
    }

    #[test]
    fn postgres_setup_fresh_init_matches_go() {
        let data_dir = temp_data_dir("fresh");
        let env = EnvMap::from_pairs([("DATA_DIR", data_dir.display().to_string())]);
        let setup = plan_postgres_setup(&env, &databases()).expect("plan");
        let path = data_dir.join("postgresql");
        let markers = data_dir.join("postgresql-markers");
        let path_text = path.display().to_string();
        let markers_text = markers.display().to_string();

        let expected = vec![
            run("mkdir", &["-p", "/run/postgresql"]),
            run("chown", &["-R", "postgres", "/run/postgresql"]),
            run_cleanup("mkdir", &["-p", &markers_text], &path_text),
            run_cleanup(
                "touch",
                &[&markers.join("sourcegraph").display().to_string()],
                &path_text,
            ),
            PostgresAction::Message(
                "Sourcegraph is initializing the internal database... (may take 15-20 seconds)"
                    .to_owned(),
            ),
            run_cleanup("mkdir", &["-p", &path_text], &path_text),
            run_cleanup("chown", &["postgres", &path_text], &path_text),
            run_cleanup(
                "su-exec",
                &["postgres", "initdb", "-D", &path_text, "--nosync"],
                &path_text,
            ),
            run_cleanup(
                "su-exec",
                &[
                    "postgres",
                    "pg_ctl",
                    "-D",
                    &path_text,
                    "-o -c listen_addresses=127.0.0.1",
                    "-l",
                    "/tmp/pgsql.log",
                    "-w",
                    "start",
                ],
                &path_text,
            ),
            run_cleanup(
                "su-exec",
                &["postgres", "createdb", "sourcegraph"],
                &path_text,
            ),
            run_cleanup(
                "touch",
                &[&markers.join("sourcegraph").display().to_string()],
                &path_text,
            ),
            run_cleanup(
                "su-exec",
                &["postgres", "createdb", "sourcegraph-codeintel"],
                &path_text,
            ),
            run_cleanup(
                "touch",
                &[&markers.join("sourcegraph-codeintel").display().to_string()],
                &path_text,
            ),
            run_cleanup(
                "su-exec",
                &[
                    "postgres",
                    "pg_ctl",
                    "-D",
                    &path_text,
                    "-m",
                    "fast",
                    "-l",
                    "/tmp/pgsql.log",
                    "-w",
                    "stop",
                ],
                &path_text,
            ),
            PostgresAction::WriteFile {
                path: path.join("5.1-reindex.completed").display().to_string(),
                contents: "Database initialised by Sourcegraph 5.1 or later\n".to_owned(),
            },
            PostgresAction::Message("Finished initializing the internal database.".to_owned()),
        ];
        assert_eq!(setup.actions, expected);
        let _ = fs::remove_dir_all(&data_dir);
    }

    #[test]
    fn postgres_setup_existing_complete_only_reclaims_owner() {
        let data_dir = temp_data_dir("existing-complete");
        let path = data_dir.join("postgresql");
        let markers = data_dir.join("postgresql-markers");
        fs::create_dir_all(&path).unwrap();
        fs::create_dir_all(&markers).unwrap();
        for database in ["sourcegraph", "sourcegraph-codeintel"] {
            fs::write(markers.join(database), b"").unwrap();
        }
        let env = EnvMap::from_pairs([("DATA_DIR", data_dir.display().to_string())]);
        let setup = plan_postgres_setup(&env, &databases()).expect("plan");
        let path_text = path.display().to_string();
        assert_eq!(
            setup.actions,
            vec![
                run("mkdir", &["-p", "/run/postgresql"]),
                run("chown", &["-R", "postgres", "/run/postgresql"]),
                run("chown", &["-R", "postgres", &path_text]),
                PostgresAction::Message(
                    "Finished initializing the internal database.".to_owned()
                ),
            ]
        );
        let _ = fs::remove_dir_all(&data_dir);
    }

    #[test]
    fn postgres_setup_missing_database_uses_filtered_createdb() {
        let data_dir = temp_data_dir("existing-missing");
        let path = data_dir.join("postgresql");
        let markers = data_dir.join("postgresql-markers");
        fs::create_dir_all(&path).unwrap();
        fs::create_dir_all(&markers).unwrap();
        fs::write(markers.join("sourcegraph"), b"").unwrap();
        let env = EnvMap::from_pairs([("DATA_DIR", data_dir.display().to_string())]);
        let setup = plan_postgres_setup(&env, &databases()).expect("plan");
        let path_text = path.display().to_string();

        assert!(setup.actions.contains(&PostgresAction::Message(
            "Sourcegraph is creating missing databases sourcegraph-codeintel... (may take 15-20 seconds)"
                .to_owned()
        )));
        let createdb = setup
            .actions
            .iter()
            .find_map(|action| match action {
                PostgresAction::Run(command) if command.program == "su-exec" && command.args[1] == "createdb" => Some(command),
                _ => None,
            })
            .expect("createdb command");
        assert_eq!(
            createdb.args,
            vec![
                "postgres".to_owned(),
                "createdb".to_owned(),
                "sourcegraph-codeintel".to_owned()
            ]
        );
        assert_eq!(
            createdb.ignore_error_contains.as_deref(),
            Some("ERROR:  database \"sourcegraph-codeintel\" already exists")
        );
        // Both start and stop happen around the single missing database.
        let pg_ctl_verbs: Vec<&str> = setup
            .actions
            .iter()
            .filter_map(|action| match action {
                PostgresAction::Run(command) if command.program == "su-exec" && command.args[1] == "pg_ctl" => {
                    command.args.last().map(String::as_str)
                }
                _ => None,
            })
            .collect();
        assert_eq!(pg_ctl_verbs, vec!["start", "stop"]);
        assert!(setup.actions.contains(&run(
            "touch",
            &[&markers.join("sourcegraph-codeintel").display().to_string()],
        )));
        assert_eq!(path_text, path.display().to_string());
        let _ = fs::remove_dir_all(&data_dir);
    }

    #[test]
    fn postgres_setup_verbose_message_uses_src_log_level() {
        let data_dir = temp_data_dir("verbose");
        let env = EnvMap::from_pairs([
            ("DATA_DIR", data_dir.display().to_string()),
            ("SRC_LOG_LEVEL", "dbug".to_owned()),
        ]);
        let setup = plan_postgres_setup(&env, &databases()).expect("plan");
        assert_eq!(
            setup.actions.first(),
            Some(&run("mkdir", &["-p", "/run/postgresql"]))
        );
        let path_text = data_dir.join("postgresql").display().to_string();
        assert!(setup.actions.contains(&PostgresAction::Message(format!(
            "Setting up PostgreSQL at {path_text}"
        ))));
        let _ = fs::remove_dir_all(&data_dir);
    }
}
