#![forbid(unsafe_code)]

//! Searcher configuration, mirroring `cmd/searcher/shared.Config`.

use std::path::PathBuf;
use std::time::Duration;

use sg_core::{ConfigError, EnvReader};

/// Baseline `cmd/searcher/shared.Config`.
#[derive(Debug, Clone)]
pub struct SearcherConfig {
    pub listen_address: String,
    pub cache_dir: PathBuf,
    pub cache_size_bytes: u64,
    pub background_timeout: Duration,
    pub max_total_paths_length: usize,
    pub disable_hybrid_search: bool,
    pub exhaustive_request_logging_enabled: bool,
    pub gitserver_addrs: Vec<String>,
    /// zoekt-webserver addresses for hybrid search. The Go baseline reads
    /// these from the frontend's `ServiceConnections.Zoekts`; the Rust pilot
    /// reads them from the environment (see `docs/modules/04_searcher.md`).
    /// Empty disables hybrid search.
    pub zoekt_addrs: Vec<String>,
}

impl SearcherConfig {
    pub fn load(env: &mut EnvReader) -> Result<Self, ConfigError> {
        let mut errors = Vec::new();

        let insecure_dev = get_bool(
            env,
            "INSECURE_DEV",
            false,
            "Running in insecure dev (local laptop) mode",
            &mut errors,
        );

        // `SEARCHER_ADDR` is optional in the baseline: an empty value falls
        // back to :3181, or to loopback in insecure dev.
        let listen_address = env.get(
            "SEARCHER_ADDR",
            "",
            "The address under which the searcher API listens. Can include a port.",
        );
        let listen_address = if listen_address.is_empty() {
            if insecure_dev {
                "127.0.0.1:3181".to_owned()
            } else {
                ":3181".to_owned()
            }
        } else {
            listen_address
        };

        let cache_dir = env.get_with_fallback(
            "SEARCHER_CACHE_DIR",
            &["CACHE_DIR"],
            "/tmp",
            "Directory to store cached archives in.",
        );
        let cache_size_mb = get_int(
            env,
            "SEARCHER_CACHE_SIZE_MB",
            "100000",
            "Maximum size of the on disk cache in megabytes when cached archives get evicted.",
            &mut errors,
        );
        if cache_size_mb < 0 {
            errors.push("SEARCHER_CACHE_SIZE_MB must be >= 0".to_owned());
        }

        let background_timeout = match parse_go_duration(&env.get(
            "PROCESSING_TIMEOUT",
            "2h",
            "Maximum time to spend processing a repository.",
        )) {
            Ok(duration) => duration,
            Err(err) => {
                errors.push(err);
                Duration::from_secs(2 * 60 * 60)
            }
        };

        let max_total_paths_length = get_int(
            env,
            "MAX_TOTAL_PATHS_LENGTH",
            "100000",
            "Maximum number of paths passed in a single call to git archive.",
            &mut errors,
        );
        if max_total_paths_length < 0 {
            errors.push("MAX_TOTAL_PATHS_LENGTH must be >= 0".to_owned());
        }

        let disable_hybrid_search = get_bool(
            env,
            "DISABLE_HYBRID_SEARCH",
            false,
            "If true, unindexed search will not consult indexed search to speed up searches.",
            &mut errors,
        );
        let exhaustive_request_logging_enabled = get_bool(
            env,
            "SRC_SEARCHER_EXHAUSTIVE_LOGGING_ENABLED",
            false,
            "Enable exhaustive request logging in searcher",
            &mut errors,
        );

        let gitserver_addrs = env
            .get(
                "SRC_GIT_SERVERS",
                "127.0.0.1:3178",
                "comma separated list of gitserver addresses (Rust pilot reads this directly)",
            )
            .split(',')
            .map(str::trim)
            .filter(|addr| !addr.is_empty())
            .map(str::to_owned)
            .collect();

        // `SRC_ZOEKT_SERVERS` is the Rust pilot's explicit override; otherwise
        // read the variables the frontend uses to build
        // `ServiceConnections.Zoekts` (`computeIndexedEndpoints`).
        let zoekt_addrs = {
            let explicit = env.get(
                "SRC_ZOEKT_SERVERS",
                "",
                "comma separated list of zoekt-webserver addresses (Rust pilot reads this directly)",
            );
            let addresses = if explicit.trim().is_empty() {
                let indexed = env.get(
                    "INDEXED_SEARCH_SERVERS",
                    "",
                    "comma separated list of indexed-search addresses (frontend configuration)",
                );
                if indexed.trim().is_empty() {
                    env.get("ZOEKT_HOST", "", "deprecated single indexed-search address")
                } else {
                    indexed
                }
            } else {
                explicit
            };
            addresses
                .split([',', ' '])
                .map(str::trim)
                .filter(|addr| !addr.is_empty())
                .map(str::to_owned)
                .collect()
        };

        if !errors.is_empty() {
            return Err(ConfigError::new(errors.join("; ")));
        }

        Ok(Self {
            listen_address,
            cache_dir: PathBuf::from(cache_dir),
            cache_size_bytes: cache_size_mb as u64 * 1_000_000,
            background_timeout,
            max_total_paths_length: max_total_paths_length as usize,
            disable_hybrid_search,
            exhaustive_request_logging_enabled,
            gitserver_addrs,
            zoekt_addrs,
        })
    }

    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.gitserver_addrs.is_empty() {
            return Err(ConfigError::new("SRC_GIT_SERVERS must not be empty"));
        }
        if self.listen_address.is_empty() {
            return Err(ConfigError::new(
                "searcher listen address must not be empty",
            ));
        }
        Ok(())
    }
}

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
        Err(err) => {
            errors.push(format!("invalid int {raw:?} for {name}: {err}"));
            0
        }
    }
}

fn get_bool(
    env: &mut EnvReader,
    name: &str,
    default: bool,
    description: &str,
    errors: &mut Vec<String>,
) -> bool {
    let raw = env.get(name, if default { "true" } else { "false" }, description);
    match parse_go_bool(&raw) {
        Ok(value) => value,
        Err(()) => {
            errors.push(format!("invalid bool {raw:?} for {name}"));
            false
        }
    }
}

/// `strconv.ParseBool` accepted spellings.
fn parse_go_bool(raw: &str) -> Result<bool, ()> {
    match raw {
        "1" | "t" | "T" | "true" | "TRUE" | "True" => Ok(true),
        "0" | "f" | "F" | "false" | "FALSE" | "False" => Ok(false),
        _ => Err(()),
    }
}

/// Parse the subset of Go duration syntax used by the service (`2h0m0s`,
/// `10s`, `1.5h`, ...); a bare integer is nanoseconds.
pub fn parse_go_duration(input: &str) -> Result<Duration, String> {
    let input = input.trim();
    if input.is_empty() {
        return Err("invalid duration \"\": empty".to_owned());
    }
    if let Ok(nanos) = input.parse::<u64>() {
        return Ok(Duration::from_nanos(nanos));
    }

    let mut rest = input;
    let mut total_nanos: u128 = 0;
    while !rest.is_empty() {
        let number_end = rest
            .find(|c: char| !(c.is_ascii_digit() || c == '.'))
            .unwrap_or(rest.len());
        if number_end == 0 {
            return Err(format!("invalid duration {input:?}"));
        }
        let number: f64 = rest[..number_end]
            .parse()
            .map_err(|_| format!("invalid duration {input:?}"))?;
        rest = &rest[number_end..];

        let unit_end = rest
            .find(|c: char| c.is_ascii_digit() || c == '.')
            .unwrap_or(rest.len());
        let unit = &rest[..unit_end];
        rest = &rest[unit_end..];

        let unit_nanos: f64 = match unit {
            "ns" => 1.0,
            "us" | "µs" | "μs" => 1_000.0,
            "ms" => 1_000_000.0,
            "s" => 1_000_000_000.0,
            "m" => 60.0 * 1_000_000_000.0,
            "h" => 3600.0 * 1_000_000_000.0,
            "" => return Err(format!("invalid duration {input:?}: missing unit")),
            _ => return Err(format!("invalid duration {input:?}: unknown unit {unit:?}")),
        };
        total_nanos += (number * unit_nanos) as u128;
    }

    u64::try_from(total_nanos)
        .map(Duration::from_nanos)
        .map_err(|_| format!("invalid duration {input:?}: out of range"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_go_durations() {
        assert_eq!(parse_go_duration("2h"), Ok(Duration::from_secs(7200)));
        assert_eq!(parse_go_duration("10s"), Ok(Duration::from_secs(10)));
        assert_eq!(parse_go_duration("1.5h"), Ok(Duration::from_secs(5400)));
        assert_eq!(parse_go_duration("250ms"), Ok(Duration::from_millis(250)));
        assert_eq!(parse_go_duration("5"), Ok(Duration::from_nanos(5)));
        assert!(parse_go_duration("").is_err());
        assert!(parse_go_duration("2x").is_err());
    }

    #[test]
    fn defaults_match_the_baseline() {
        let mut env = EnvReader::new();
        let config = SearcherConfig::load(&mut env).expect("load");
        assert_eq!(config.listen_address, ":3181");
        assert_eq!(config.cache_dir, PathBuf::from("/tmp"));
        assert_eq!(config.cache_size_bytes, 100_000_000_000);
        assert_eq!(config.background_timeout, Duration::from_secs(7200));
        assert_eq!(config.max_total_paths_length, 100_000);
        assert!(!config.disable_hybrid_search);
        assert!(!config.exhaustive_request_logging_enabled);
        // With no zoekt configured, hybrid search is disabled.
        let _ = &config.zoekt_addrs;
    }

    #[test]
    fn reads_zoekt_addresses_from_the_environment() {
        // `EnvReader::get` reads the process environment; guard the mutation.
        let _guard = ENV_LOCK.lock().expect("env lock");
        std::env::set_var("SRC_ZOEKT_SERVERS", "zoekt-0:6070, zoekt-1:6070");
        let mut env = EnvReader::new();
        let config = SearcherConfig::load(&mut env).expect("load");
        std::env::remove_var("SRC_ZOEKT_SERVERS");
        assert_eq!(config.zoekt_addrs, vec!["zoekt-0:6070", "zoekt-1:6070"]);
    }

    static ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());
}
