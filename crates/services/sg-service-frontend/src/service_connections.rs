#![forbid(unsafe_code)]

//! Port of the environment-driven `serviceConnections` in
//! `cmd/frontend/internal/cli/config.go` (with
//! `internal/database/postgresdsn`, the static branch of
//! `internal/endpoint`, `internal/conf/deploy/deploytype.go` and
//! `internal/dotcom/dotcom.go`).
//!
//! `configurationSource.Read` attaches this value to every `RawUnified`, so
//! it is what tells the rest of the process which gitserver/searcher/symbol/
//! embeddings/zoekt replicas to talk to, which DSNs back the three schemas,
//! and how long to cache Zoekt's indexed-repository list.
//!
//! Errata (also recorded in the M9.8eh and M9.19c reports):
//! * Kubernetes service discovery (`k8s+` URL specs) is ported behind the
//!   [`K8sDiscoverer`] seam (M9.19c): URL parsing, Endpoints/StatefulSet
//!   address formatting and the non-cluster error text match
//!   `internal/endpoint`. [`NotInClusterDiscoverer`] is the production
//!   default (client-go's "unable to load in-cluster configuration…" error);
//!   [`StaticK8sDiscoverer`] backs the offline differential.
//! * The `go test` executable-name special case in `gitserverAddr` is not
//!   ported; unset `SRC_GIT_SERVERS` always falls through to the
//!   `k8s+rpc://gitserver:3178?kind=sts` default.
//! * `env.Get`'s per-program override (`<name>_<prog>`) is not ported; only
//!   the plain variable is read.
//! * A negative `SRC_INDEXED_SEARCH_LIST_CACHE_TTL` is clamped to zero by
//!   [`sg_core::parse_go_duration`], so it selects the default TTL instead of
//!   staying negative as in Go.
//! * The baseline panics when the frontend/codeintel DSNs collide; this port
//!   returns the same error text as `Err` instead.

use std::collections::BTreeMap;
use std::time::Duration;

use sg_config::ServiceConnections;

pub const DEPLOY_TYPE_VAR: &str = "DEPLOY_TYPE";
/// `deploy.Type()`'s default when `DEPLOY_TYPE` is unset.
pub const DEFAULT_DEPLOY_TYPE: &str = "kubernetes";
pub const SOURCEGRAPH_DOTCOM_MODE_VAR: &str = "SOURCEGRAPHDOTCOM_MODE";
pub const INDEXED_SEARCH_LIST_CACHE_TTL_VAR: &str = "SRC_INDEXED_SEARCH_LIST_CACHE_TTL";
pub const GIT_SERVERS_VAR: &str = "SRC_GIT_SERVERS";
pub const SEARCHER_URL_VAR: &str = "SEARCHER_URL";
pub const SYMBOLS_URL_VAR: &str = "SYMBOLS_URL";
pub const EMBEDDINGS_URL_VAR: &str = "EMBEDDINGS_URL";
pub const INDEXED_SEARCH_SERVERS_VAR: &str = "INDEXED_SEARCH_SERVERS";
pub const ZOEKT_HOST_VAR: &str = "ZOEKT_HOST";
pub const CODEINTEL_PG_ALLOW_SINGLE_DB_VAR: &str = "CODEINTEL_PG_ALLOW_SINGLE_DB";

/// `schemas.SchemaNames`: the schema set `DSNsBySchema` is called with.
pub const SCHEMA_NAMES: [&str; 3] = ["frontend", "codeintel", "codeinsights"];

/// `os.Getenv` as a test seam: `None` means unset, `Some("")` means set to
/// the empty string. The distinction matters because the baseline branches on
/// `if addr, ok := getEnv(...)`.
pub type Getenv<'a> = &'a dyn Fn(&str) -> Option<String>;

fn env_string(getenv: Getenv<'_>, key: &str) -> String {
    getenv(key).unwrap_or_default()
}

/// `strconv.ParseBool`; a parse error is treated as false by the baseline.
fn parse_bool(value: &str) -> bool {
    matches!(value, "1" | "t" | "T" | "TRUE" | "true" | "True")
}

/// `deploy.Type()`: `DEPLOY_TYPE` (read with `os.Getenv`, so empty means
/// unset) or `kubernetes`.
pub fn deploy_type(getenv: Getenv<'_>) -> String {
    match getenv(DEPLOY_TYPE_VAR) {
        Some(value) if !value.is_empty() => value,
        _ => DEFAULT_DEPLOY_TYPE.to_string(),
    }
}

/// `dotcom.SourcegraphDotComMode()`.
pub fn sourcegraph_dotcom_mode(getenv: Getenv<'_>) -> bool {
    parse_bool(&env_string(getenv, SOURCEGRAPH_DOTCOM_MODE_VAR))
}

/// `time.ParseDuration` followed by the zero/dotcom defaulting in
/// `indexedListTTL`.
pub fn indexed_list_ttl(dotcom_mode: bool, getenv: Getenv<'_>) -> Duration {
    let raw = env_string(getenv, INDEXED_SEARCH_LIST_CACHE_TTL_VAR);
    let ttl = sg_core::parse_go_duration(&raw).unwrap_or(Duration::ZERO);
    if ttl.is_zero() {
        if dotcom_mode {
            Duration::from_secs(30)
        } else {
            Duration::from_secs(5)
        }
    } else {
        ttl
    }
}

/// `getEnv(os.Environ(), key)`: the first entry with a `key=` prefix. With a
/// `Getenv` seam `Some` already means "present", so this is only used for
/// documentation; call sites use `getenv` directly.
pub fn get_env(getenv: Getenv<'_>, key: &str) -> Option<String> {
    getenv(key)
}

/// `postgresdsn.New`: builds the DSN for one schema from the `PG*` variables
/// (prefixed with `<SCHEMA>_` except for `frontend`, which reads the plain
/// names).
pub fn new_postgres_dsn(prefix: &str, current_user: &str, getenv: Getenv<'_>) -> String {
    let prefix = if prefix == "frontend" || prefix.is_empty() {
        String::new()
    } else {
        format!("{}_", prefix.to_uppercase())
    };
    let env = |name: &str| env_string(getenv, &format!("{prefix}{name}"));

    // PGDATASOURCE is a sourcegraph specific variable for just setting the DSN
    let datasource = env("PGDATASOURCE");
    if !datasource.is_empty() {
        return datasource;
    }

    let mut host = String::from("127.0.0.1:5432");

    // Username preference: PGUSER, $USER, postgres
    let mut username = if current_user.is_empty() {
        "postgres".to_string()
    } else {
        current_user.to_string()
    };
    let user = env("PGUSER");
    if !user.is_empty() {
        username = user;
    }
    let password = env("PGPASSWORD");

    let pg_host = env("PGHOST");
    if !pg_host.is_empty() {
        host = pg_host;
    }

    // PGPORT values may be (legally) quoted, but should remain quoted when
    // constructed as part of the DSN. Strip it here.
    let raw_port = env("PGPORT");
    let port = dequote(&raw_port);
    if !port.is_empty() {
        let head = host.split(':').next().unwrap_or_default();
        host = format!("{head}:{port}");
    }

    let database = env("PGDATABASE");

    let mut dsn = String::from("postgres://");
    dsn.push_str(&escape_userinfo(&username));
    if !password.is_empty() {
        dsn.push(':');
        dsn.push_str(&escape_userinfo(&password));
    }
    dsn.push('@');
    dsn.push_str(&host);
    if !database.is_empty() {
        let escaped = escape_path(&database);
        if !escaped.starts_with('/') {
            dsn.push('/');
        }
        dsn.push_str(&escaped);
    }

    let mut query = Vec::new();
    let sslmode = env("PGSSLMODE");
    if !sslmode.is_empty() {
        query.push(("sslmode", sslmode));
    }
    let timezone = env("PGTZ");
    if !timezone.is_empty() {
        query.push(("timezone", timezone));
    }
    if !query.is_empty() {
        // `url.Values.Encode` sorts keys.
        query.sort_by(|left, right| left.0.cmp(right.0));
        let encoded = query
            .iter()
            .map(|(key, value)| format!("{}={}", escape_query(key), escape_query(value)))
            .collect::<Vec<_>>()
            .join("&");
        dsn.push('?');
        dsn.push_str(&encoded);
    }

    dsn
}

/// `dsn.String()` strips one pair of matching quotes from `PGPORT`.
fn dequote(value: &str) -> &str {
    let bytes = value.as_bytes();
    if bytes.len() > 2
        && bytes[0] == bytes[bytes.len() - 1]
        && (bytes[0] == b'\'' || bytes[0] == b'"')
    {
        &value[1..value.len() - 1]
    } else {
        value
    }
}

fn escape_with(input: &str, mode: EscapeMode) -> String {
    let mut out = String::with_capacity(input.len());
    for &byte in input.as_bytes() {
        let keep = byte.is_ascii_alphanumeric()
            || match mode {
                // `encodeUserPassword`
                EscapeMode::Userinfo => b"-._~$&+,;=".contains(&byte),
                // `encodePath`
                EscapeMode::Path => b"-._~$&+,/:;=@".contains(&byte),
                // `encodeQueryComponent`
                EscapeMode::Query => b"-._~".contains(&byte),
            };
        if keep {
            out.push(byte as char);
        } else if byte == b' ' && mode == EscapeMode::Query {
            out.push('+');
        } else {
            out.push('%');
            out.push_str(&format!("{byte:02X}"));
        }
    }
    out
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum EscapeMode {
    Userinfo,
    Path,
    Query,
}

fn escape_userinfo(input: &str) -> String {
    escape_with(input, EscapeMode::Userinfo)
}

fn escape_path(input: &str) -> String {
    escape_with(input, EscapeMode::Path)
}

fn escape_query(input: &str) -> String {
    escape_with(input, EscapeMode::Query)
}

/// `RawDSNsBySchema`: one DSN per schema name.
pub fn raw_dsns_by_schema(
    schema_names: &[&str],
    current_user: &str,
    getenv: Getenv<'_>,
) -> BTreeMap<String, String> {
    schema_names
        .iter()
        .map(|name| {
            (
                (*name).to_string(),
                new_postgres_dsn(name, current_user, getenv),
            )
        })
        .collect()
}

/// `DSNsBySchema`: adds the frontend/codeintel distinctness check unless
/// `CODEINTEL_PG_ALLOW_SINGLE_DB` is set.
pub fn dsns_by_schema(
    schema_names: &[&str],
    current_user: &str,
    getenv: Getenv<'_>,
) -> Result<BTreeMap<String, String>, String> {
    let dsns = raw_dsns_by_schema(schema_names, current_user, getenv);

    // We set this envvar in development to disable the following check
    if env_string(getenv, CODEINTEL_PG_ALLOW_SINGLE_DB_VAR).is_empty() {
        if let (Some(codeintel), Some(frontend)) = (dsns.get("codeintel"), dsns.get("frontend")) {
            compare_postgres_dsns("frontend", "codeintel", frontend, codeintel)?;
        }
    }

    Ok(dsns)
}

/// `comparePostgresDSNs`: two DSNs are the same when host and path match.
pub fn compare_postgres_dsns(
    name1: &str,
    name2: &str,
    dsn1: &str,
    dsn2: &str,
) -> Result<(), String> {
    let url1 = parse_url(dsn1).ok_or_else(|| format!("illegal Postgres DSN: {dsn1}"))?;
    let url2 = parse_url(dsn2).ok_or_else(|| format!("illegal Postgres DSN: {dsn2}"))?;

    if url1.host == url2.host && url1.path == url2.path {
        let mut target = String::from("postgres://");
        target.push_str(&url1.host);
        if !url1.path.is_empty() {
            let escaped = escape_path(&url1.path);
            if !escaped.starts_with('/') {
                target.push('/');
            }
            target.push_str(&escaped);
        }
        return Err(format!(
            "databases {name1} and {name2} must be distinct, but both target {target}"
        ));
    }

    Ok(())
}

struct ParsedUrl {
    host: String,
    path: String,
}

/// The subset of `net/url.Parse` needed for `comparePostgresDSNs`: reject the
/// inputs Go rejects (control bytes, bad percent escapes) and extract the
/// decoded host and path.
fn parse_url(input: &str) -> Option<ParsedUrl> {
    let bytes = input.as_bytes();
    let mut index = 0;
    while index < bytes.len() {
        let byte = bytes[index];
        if byte < 0x20 || byte == 0x7f {
            return None;
        }
        if byte == b'%' {
            if index + 2 >= bytes.len()
                || !bytes[index + 1].is_ascii_hexdigit()
                || !bytes[index + 2].is_ascii_hexdigit()
            {
                return None;
            }
            index += 3;
        } else {
            index += 1;
        }
    }

    let (host, path) = match input.find("//") {
        Some(authority_at) => {
            let after = &input[authority_at + 2..];
            let authority_end = after.find(['/', '?', '#']).unwrap_or(after.len());
            let authority = &after[..authority_end];
            let host = authority
                .rsplit_once('@')
                .map_or(authority, |(_, host)| host);
            let rest = &after[authority_end..];
            let path_end = rest.find(['?', '#']).unwrap_or(rest.len());
            (host.to_string(), &rest[..path_end])
        }
        None => {
            let path_end = input.find(['?', '#']).unwrap_or(input.len());
            (String::new(), &input[..path_end])
        }
    };

    Some(ParsedUrl {
        host,
        path: percent_decode(path),
    })
}

fn percent_decode(input: &str) -> String {
    let bytes = input.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'%' && index + 2 < bytes.len() {
            let high = (bytes[index + 1] as char).to_digit(16);
            let low = (bytes[index + 2] as char).to_digit(16);
            if let (Some(high), Some(low)) = (high, low) {
                out.push((high * 16 + low) as u8);
                index += 3;
                continue;
            }
        }
        out.push(bytes[index]);
        index += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// `replicaAddrs`: expand an integer replica count into per-replica
/// addresses; a non-integer value is returned unchanged.
pub fn replica_addrs(
    deploy_type: &str,
    count_str: &str,
    service_name: &str,
    port: &str,
) -> Result<String, String> {
    let Ok(count) = count_str.parse::<i64>() else {
        return Ok(count_str.to_string());
    };

    let head = match service_name {
        "searcher" | "symbols" => "http://",
        _ => "",
    };

    let tail = match deploy_type {
        "kubernetes" | "helm" | "kustomize" => format!(".{service_name}:{port}"),
        "docker-compose" => format!(":{port}"),
        other => return Err(format!("Error: unsupported deployment type: {other}")),
    };

    let mut addrs = Vec::new();
    let mut replica = 0;
    while replica < count {
        addrs.push(format!("{head}{service_name}-{replica}{tail}"));
        replica += 1;
    }
    Ok(addrs.join(" "))
}

/// `gitserverAddr` (without the `go test` executable-name default).
pub fn gitserver_addr(getenv: Getenv<'_>) -> Result<String, String> {
    if let Some(addr) = getenv(GIT_SERVERS_VAR) {
        return replica_addrs(&deploy_type(getenv), &addr, "gitserver", "3178");
    }

    // Not set, use the default (service discovery on gitserver)
    Ok("k8s+rpc://gitserver:3178?kind=sts".to_string())
}

/// `searcherAddr`.
pub fn searcher_addr(getenv: Getenv<'_>) -> Result<String, String> {
    if let Some(addr) = getenv(SEARCHER_URL_VAR) {
        return replica_addrs(&deploy_type(getenv), &addr, "searcher", "3181");
    }

    // Not set, use the default (service discovery on searcher)
    Ok("k8s+http://searcher:3181".to_string())
}

/// `symbolsAddr`.
pub fn symbols_addr(getenv: Getenv<'_>) -> Result<String, String> {
    if let Some(addr) = getenv(SYMBOLS_URL_VAR) {
        return replica_addrs(&deploy_type(getenv), &addr, "symbols", "3184");
    }

    // Not set, use the default (non-service discovery on symbols)
    Ok("http://symbols:3184".to_string())
}

/// `embeddingsAddr`.
pub fn embeddings_addr(getenv: Getenv<'_>) -> Result<String, String> {
    if let Some(addr) = getenv(EMBEDDINGS_URL_VAR) {
        return replica_addrs(&deploy_type(getenv), &addr, "embeddings", "9991");
    }

    // Not set, use the default (non-service discovery on embeddings)
    Ok("http://embeddings:9991".to_string())
}

/// `zoektAddr`.
pub fn zoekt_addr(getenv: Getenv<'_>) -> Result<String, String> {
    let deploy_type = deploy_type(getenv);

    let base_name = if deploy_type == "docker-compose" {
        "zoekt-webserver"
    } else {
        "indexed-search"
    };

    if let Some(addr) = getenv(INDEXED_SEARCH_SERVERS_VAR) {
        return replica_addrs(&deploy_type, &addr, base_name, "6070");
    }

    // Backwards compatibility: We used to call this variable ZOEKT_HOST
    if let Some(addr) = getenv(ZOEKT_HOST_VAR) {
        return Ok(addr);
    }

    // Not set, use the default (service discovery on the indexed-search
    // statefulset)
    Ok("k8s+rpc://indexed-search:6070?kind=sts".to_string())
}

/// One `corev1.EndpointAddress` (only the fields `k8sEndpoints` reads).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct K8sAddress {
    pub hostname: String,
    pub ip: String,
}

/// One `corev1.EndpointSubset`.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct K8sSubset {
    pub addresses: Vec<K8sAddress>,
}

/// The Kubernetes resource `k8sEndpoints` consumes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum K8sResource {
    /// `corev1.Endpoints`.
    Endpoints { subsets: Vec<K8sSubset> },
    /// `appsv1.StatefulSet` (`kind=sts`/`statefulset`).
    StatefulSet {
        name: String,
        service_name: String,
        /// `spec.replicas`; `None` is the baseline's `nil` (one replica).
        replicas: Option<i32>,
    },
}

/// `endpoint.k8sURL`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct K8sUrl {
    /// The parsed URL after the `k8s+` prefix (kept verbatim for
    /// `endpointURL`'s copy-and-rehost behavior).
    pub url: url::Url,
    pub service: String,
    pub namespace: String,
    /// `strings.ToLower(u.Query().Get("kind"))`.
    pub kind: String,
}

/// `endpoint.parseURL`.
pub fn parse_k8s_url(raw: &str) -> Result<K8sUrl, String> {
    let stripped = raw.strip_prefix("k8s+").unwrap_or(raw);
    let url = url::Url::parse(stripped).map_err(|error| error.to_string())?;
    let host = url
        .host_str()
        .ok_or_else(|| format!("invalid k8s url. expected k8s+http://service.namespace:port/path?kind=$kind, got {raw}"))?;
    let parts: Vec<&str> = host.split('.').collect();
    let (service, namespace) = match parts.as_slice() {
        [service] => ((*service).to_owned(), String::new()),
        [service, namespace] => ((*service).to_owned(), (*namespace).to_owned()),
        _ => {
            return Err(format!(
                "invalid k8s url. expected k8s+http://service.namespace:port/path?kind=$kind, got {raw}"
            ))
        }
    };
    let kind = url
        .query_pairs()
        .find(|(key, _)| key == "kind")
        .map(|(_, value)| value.to_lowercase())
        .unwrap_or_default();
    Ok(K8sUrl {
        url,
        service,
        namespace,
        kind,
    })
}

impl K8sUrl {
    /// `k8sURL.endpointURL`: replace the host, drop the scheme/path for
    /// `rpc://` specs, otherwise keep the URL's path and query.
    pub fn endpoint_url(&self, endpoint: &str) -> String {
        let host = match self.url.port() {
            Some(port) => format!("{endpoint}:{port}"),
            None => endpoint.to_owned(),
        };
        if self.url.scheme() == "rpc" {
            return host;
        }
        // Go's `url.URL.String` omits an empty path (the `url` crate always
        // reports `/` for special schemes), so build it by hand.
        let mut out = format!("{}://{host}", self.url.scheme());
        let path = self.url.path();
        if path != "/" && !path.is_empty() {
            out.push_str(path);
        }
        if let Some(query) = self.url.query() {
            out.push('?');
            out.push_str(query);
        }
        if let Some(fragment) = self.url.fragment() {
            out.push('#');
            out.push_str(fragment);
        }
        out
    }
}

/// `endpoint.k8sEndpoints`.
pub fn k8s_endpoints(url: &K8sUrl, resource: &K8sResource) -> Vec<String> {
    let mut endpoints = Vec::new();
    match resource {
        K8sResource::Endpoints { subsets } => {
            for subset in subsets {
                for address in &subset.addresses {
                    let endpoint = if !address.hostname.is_empty() {
                        url.endpoint_url(&format!("{}.{}", address.hostname, url.service))
                    } else if !address.ip.is_empty() {
                        url.endpoint_url(&address.ip)
                    } else {
                        String::new()
                    };
                    endpoints.push(endpoint);
                }
            }
        }
        K8sResource::StatefulSet {
            name,
            service_name,
            replicas,
        } => {
            let replicas = replicas.unwrap_or(1);
            for ordinal in 0..replicas {
                // `$(statefulset name)-$(ordinal).$(serviceName)`.
                endpoints.push(url.endpoint_url(&format!("{name}-{ordinal}.{service_name}")));
            }
        }
    }
    endpoints
}

/// The Kubernetes discovery seam (`endpoint.k8sDiscovery`'s informer).
pub trait K8sDiscoverer: Send + Sync {
    fn endpoints(&self, url: &K8sUrl) -> Result<Vec<String>, String>;
}

/// The production default: `rest.InClusterConfig` outside a pod returns
/// [`IN_CLUSTER_CONFIG_ERROR`] (client-go `rest.ErrNotInCluster`).
#[derive(Debug, Default)]
pub struct NotInClusterDiscoverer;

/// `rest.ErrNotInCluster`.
pub const IN_CLUSTER_CONFIG_ERROR: &str = "unable to load in-cluster configuration, KUBERNETES_SERVICE_HOST and KUBERNETES_SERVICE_PORT must be defined";

impl K8sDiscoverer for NotInClusterDiscoverer {
    fn endpoints(&self, _url: &K8sUrl) -> Result<Vec<String>, String> {
        Err(IN_CLUSTER_CONFIG_ERROR.to_owned())
    }
}

/// A discoverer over injected Kubernetes resources, keyed by service name.
#[derive(Debug, Default)]
pub struct StaticK8sDiscoverer {
    pub resources: BTreeMap<String, K8sResource>,
}

impl K8sDiscoverer for StaticK8sDiscoverer {
    fn endpoints(&self, url: &K8sUrl) -> Result<Vec<String>, String> {
        let resource = self
            .resources
            .get(&url.service)
            .ok_or_else(|| format!("no resources for {}", url.service))?;
        Ok(k8s_endpoints(url, resource))
    }
}

/// `endpoint.New(urlspec).Endpoints()` for `k8s+` specs: parse, discover, and
/// turn an empty result into the baseline's "no endpoints" error.
pub fn k8s_discover_with(
    discoverer: &dyn K8sDiscoverer,
    urlspec: &str,
) -> Result<Vec<String>, String> {
    let url = parse_k8s_url(urlspec)?;
    let endpoints = discoverer.endpoints(&url)?;
    if endpoints.is_empty() {
        return Err(format!(
            "no {} endpoints could be found (this may indicate more {} replicas are needed, contact support@sourcegraph.com for assistance)",
            url.service, url.service
        ));
    }
    Ok(endpoints)
}

/// `endpoint.New(urlspec).Endpoints()` for the static branch:
/// whitespace-separated endpoints in input order. `k8s+` specs go through
/// [`k8s_discover_with`] with the production (not-in-cluster) discoverer.
pub fn static_endpoints(urlspec: &str) -> Result<Vec<String>, String> {
    if urlspec.starts_with("k8s+") {
        return k8s_discover_with(&NotInClusterDiscoverer, urlspec);
    }
    Ok(urlspec
        .split_whitespace()
        .map(str::to_string)
        .collect::<Vec<_>>())
}

fn endpoints_or_empty(result: Result<String, String>) -> Vec<String> {
    match result {
        Ok(urlspec) => static_endpoints(&urlspec).unwrap_or_default(),
        Err(_) => Vec::new(),
    }
}

/// `serviceConnections`: reads the process environment.
pub fn service_connections() -> Result<ServiceConnections, String> {
    let getenv: &dyn Fn(&str) -> Option<String> = &|key| std::env::var(key).ok();
    service_connections_with(getenv)
}

/// `serviceConnections` with an injectable environment (also used by the
/// differential harness).
pub fn service_connections_with(getenv: Getenv<'_>) -> Result<ServiceConnections, String> {
    let current_user = current_username();
    let dsns = dsns_by_schema(&SCHEMA_NAMES, &current_user, getenv)?;

    Ok(ServiceConnections {
        git_servers: endpoints_or_empty(gitserver_addr(getenv)),
        postgres_dsn: dsns.get("frontend").cloned().unwrap_or_default(),
        code_intel_postgres_dsn: dsns.get("codeintel").cloned().unwrap_or_default(),
        code_insights_dsn: dsns.get("codeinsights").cloned().unwrap_or_default(),
        searchers: endpoints_or_empty(searcher_addr(getenv)),
        symbols: endpoints_or_empty(symbols_addr(getenv)),
        embeddings: endpoints_or_empty(embeddings_addr(getenv)),
        zoekts: endpoints_or_empty(zoekt_addr(getenv)),
        zoekt_list_ttl: indexed_list_ttl(sourcegraph_dotcom_mode(getenv), getenv),
    })
}

/// `user.Current().Username` on Linux: the passwd entry for the real uid,
/// falling back to `USER`/`LOGNAME`. Going through `/proc/self/status` avoids
/// pulling in `libc` only for `getuid`.
pub fn current_username() -> String {
    if let Some(uid) = current_uid() {
        if let Ok(passwd) = std::fs::read_to_string("/etc/passwd") {
            for line in passwd.lines() {
                let mut fields = line.split(':');
                let name = fields.next().unwrap_or_default();
                let _password = fields.next();
                if fields.next() == Some(uid.as_str()) {
                    return name.to_string();
                }
            }
        }
    }
    for key in ["USER", "LOGNAME"] {
        if let Ok(value) = std::env::var(key) {
            if !value.is_empty() {
                return value;
            }
        }
    }
    String::new()
}

fn current_uid() -> Option<String> {
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    for line in status.lines() {
        if let Some(rest) = line.strip_prefix("Uid:") {
            return rest.split_whitespace().next().map(str::to_string);
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    fn env_map(vars: &[(&str, &str)]) -> impl Fn(&str) -> Option<String> {
        let vars: BTreeMap<String, String> = vars
            .iter()
            .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
            .collect();
        move |key: &str| vars.get(key).cloned()
    }

    #[test]
    fn replica_addrs_matches_baseline() {
        assert_eq!(
            replica_addrs("kubernetes", "2", "gitserver", "3178").unwrap(),
            "gitserver-0.gitserver:3178 gitserver-1.gitserver:3178"
        );
        assert_eq!(
            replica_addrs("helm", "2", "searcher", "3181").unwrap(),
            "http://searcher-0.searcher:3181 http://searcher-1.searcher:3181"
        );
        assert_eq!(
            replica_addrs("docker-compose", "2", "symbols", "3184").unwrap(),
            "http://symbols-0:3184 http://symbols-1:3184"
        );
        assert_eq!(
            replica_addrs("kubernetes", "0", "searcher", "3181").unwrap(),
            ""
        );
        assert_eq!(
            replica_addrs("kubernetes", "-1", "searcher", "3181").unwrap(),
            ""
        );
        assert_eq!(
            replica_addrs("kubernetes", "x", "searcher", "3181").unwrap(),
            "x"
        );
        assert_eq!(
            replica_addrs("dev", "2", "gitserver", "3178").unwrap_err(),
            "Error: unsupported deployment type: dev"
        );
    }

    #[test]
    fn postgres_dsn_escapes_like_go() {
        let getenv = env_map(&[
            ("PGUSER", "a b;c,d=e+f$g&h"),
            ("PGPASSWORD", "p@ss:word/1 !'()*~"),
            ("PGHOST", "db.example.com"),
            ("PGPORT", "\"6543\""),
            ("PGDATABASE", "my db"),
            ("PGSSLMODE", "disable"),
            ("PGTZ", "Etc/UTC"),
        ]);
        assert_eq!(
            new_postgres_dsn("frontend", "ignored", &getenv),
            "postgres://a%20b;c,d=e+f$g&h:p%40ss%3Aword%2F1%20%21%27%28%29%2A~@db.example.com:6543/my%20db?sslmode=disable&timezone=Etc%2FUTC"
        );
        let datasource = env_map(&[("CODEINTEL_PGDATASOURCE", "postgres://direct/db")]);
        assert_eq!(
            new_postgres_dsn("codeintel", "ignored", &datasource),
            "postgres://direct/db"
        );
    }

    #[test]
    fn dsn_collision_reports_baseline_error() {
        let getenv = env_map(&[]);
        assert_eq!(
            dsns_by_schema(&SCHEMA_NAMES, "alice", &getenv).unwrap_err(),
            "databases frontend and codeintel must be distinct, but both target postgres://127.0.0.1:5432"
        );
        let allowed = env_map(&[("CODEINTEL_PG_ALLOW_SINGLE_DB", "1")]);
        assert!(dsns_by_schema(&SCHEMA_NAMES, "alice", &allowed).is_ok());
    }

    #[test]
    fn endpoints_split_on_whitespace_unless_k8s() {
        assert_eq!(
            static_endpoints("http://a:1  http://b:2").unwrap(),
            vec!["http://a:1".to_string(), "http://b:2".to_string()]
        );
        assert!(static_endpoints("").unwrap().is_empty());
        assert!(static_endpoints("k8s+rpc://indexed-search:6070?kind=sts").is_err());
    }

    #[test]
    fn ttl_defaults_and_env_value() {
        let unset = env_map(&[]);
        assert_eq!(indexed_list_ttl(false, &unset), Duration::from_secs(5));
        assert_eq!(indexed_list_ttl(true, &unset), Duration::from_secs(30));
        let set = env_map(&[("SRC_INDEXED_SEARCH_LIST_CACHE_TTL", "7s")]);
        assert_eq!(indexed_list_ttl(true, &set), Duration::from_secs(7));
        let invalid = env_map(&[("SRC_INDEXED_SEARCH_LIST_CACHE_TTL", "1x")]);
        assert_eq!(indexed_list_ttl(false, &invalid), Duration::from_secs(5));
    }

    #[test]
    fn service_connections_uses_static_endpoints() {
        let getenv = env_map(&[
            ("DEPLOY_TYPE", "docker-compose"),
            ("SRC_GIT_SERVERS", "2"),
            ("SEARCHER_URL", "http://searcher-a"),
            ("INDEXED_SEARCH_SERVERS", "2"),
            ("PGDATABASE", "frontend"),
            ("CODEINTEL_PGDATABASE", "codeintel"),
            ("CODEINSIGHTS_PGDATABASE", "codeinsights"),
        ]);
        let connections = service_connections_with(&getenv).unwrap();
        assert_eq!(
            connections.git_servers,
            vec![
                "gitserver-0:3178".to_string(),
                "gitserver-1:3178".to_string()
            ]
        );
        assert_eq!(connections.searchers, vec!["http://searcher-a".to_string()]);
        assert_eq!(connections.symbols, vec!["http://symbols:3184".to_string()]);
        assert_eq!(
            connections.embeddings,
            vec!["http://embeddings:9991".to_string()]
        );
        assert_eq!(
            connections.zoekts,
            vec![
                "zoekt-webserver-0:6070".to_string(),
                "zoekt-webserver-1:6070".to_string()
            ]
        );
        assert_eq!(connections.zoekt_list_ttl, Duration::from_secs(5));
        assert!(connections.postgres_dsn.ends_with("/frontend"));
        assert!(connections.code_intel_postgres_dsn.ends_with("/codeintel"));
        assert!(connections.code_insights_dsn.ends_with("/codeinsights"));
    }

    #[test]
    fn current_username_is_non_empty() {
        assert!(!current_username().is_empty());
    }
}
