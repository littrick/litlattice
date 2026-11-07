#![forbid(unsafe_code)]

//! Symbols configuration, mirroring `cmd/symbols/internal/types`.
//!
//! Every value is registered and read during `configure`; validation happens
//! after the environment is locked.

use std::path::PathBuf;
use std::time::Duration;

use sg_core::{ConfigError, EnvReader};

#[derive(Debug, Clone)]
pub struct SymbolsConfig {
    pub cache_dir: PathBuf,
    pub cache_size_bytes: u64,
    pub ctags_processes: usize,
    pub request_buffer_size: usize,
    pub processing_timeout: Duration,
    pub max_concurrently_indexing: usize,
    pub ctags_command: String,
    pub scip_ctags_command: String,
    pub ctags_pattern_length_limit: u32,
    pub ctags_log_errors: bool,
    pub ctags_max_file_size: u64,
    pub ctags_max_symbols: u64,
    pub max_total_paths_length: usize,
    pub max_file_size_bytes: u64,
    pub use_rockskip: bool,
    pub gitserver_addrs: Vec<String>,
}

impl SymbolsConfig {
    pub fn load(env: &mut EnvReader) -> Result<Self, ConfigError> {
        let mut errors = Vec::new();

        let cache_dir = env.get_with_fallback(
            "SYMBOLS_CACHE_DIR",
            &["CACHE_DIR"],
            "/tmp/symbols-cache",
            "directory in which to store cached symbols",
        );
        let cache_size_mb = get_int(
            env,
            "SYMBOLS_CACHE_SIZE_MB",
            "100000",
            "maximum size of the disk cache (in megabytes)",
            &mut errors,
        );

        let default_processes = std::thread::available_parallelism()
            .map(|n| n.get() as i64)
            .unwrap_or(1);
        let ctags_processes = get_int(
            env,
            "CTAGS_PROCESSES",
            &default_processes.to_string(),
            "number of concurrent parser processes to run",
            &mut errors,
        );
        let request_buffer_size = get_int(
            env,
            "REQUEST_BUFFER_SIZE",
            "8192",
            "maximum size of buffered parser request channel",
            &mut errors,
        );
        let processing_timeout = match parse_go_duration(&env.get(
            "PROCESSING_TIMEOUT",
            "2h0m0s",
            "maximum time to spend processing a repository",
        )) {
            Ok(duration) => duration,
            Err(err) => {
                errors.push(err);
                Duration::from_secs(2 * 60 * 60)
            }
        };
        let max_concurrently_indexing = get_int(
            env,
            "MAX_CONCURRENTLY_INDEXING",
            "10",
            "maximum number of repositories to index at a time",
            &mut errors,
        );

        let ctags_command = env.get(
            "CTAGS_COMMAND",
            "universal-ctags",
            "ctags command (should point to universal-ctags executable compiled with JSON and seccomp support)",
        );
        let scip_ctags_command = env.get("SCIP_CTAGS_COMMAND", "scip-ctags", "scip-ctags command");
        let ctags_pattern_length_limit = get_int(
            env,
            "CTAGS_PATTERN_LENGTH_LIMIT",
            "250",
            "the maximum length of the patterns output by ctags",
            &mut errors,
        );
        let log_ctags_errors_default = std::env::var("DEPLOY_TYPE").as_deref() == Ok("dev");
        let ctags_log_errors = get_bool(
            env,
            "LOG_CTAGS_ERRORS",
            log_ctags_errors_default,
            "log ctags errors",
            &mut errors,
        );
        let ctags_max_file_size = get_int(
            env,
            "CTAGS_MAX_FILE_SIZE",
            "524288",
            "skip files larger than this size (in bytes)",
            &mut errors,
        );
        let ctags_max_symbols = get_int(
            env,
            "CTAGS_MAX_SYMBOLS",
            "2000",
            "skip files with more than this many symbols",
            &mut errors,
        );

        let max_total_paths_length = get_int(
            env,
            "SYMBOLS_MAX_TOTAL_PATHS_LENGTH",
            "100000",
            "maximum sum of lengths of all paths in a single call to git archive",
            &mut errors,
        );
        let max_file_size_kb = get_int(
            env,
            "MAX_FILE_SIZE_KB",
            "1000",
            "maximum file size in KB, the contents of bigger files are ignored",
            &mut errors,
        );

        let use_rockskip = get_bool(
            env,
            "USE_ROCKSKIP",
            false,
            "use Rockskip to index the repos specified in ROCKSKIP_REPOS, or repos over ROCKSKIP_MIN_REPO_SIZE_MB in size",
            &mut errors,
        );
        let _ = env.get(
            "ROCKSKIP_REPOS",
            "",
            "comma separated list of repositories to index (e.g. `github.com/torvalds/linux,github.com/pallets/flask`)",
        );
        let _ = get_int(
            env,
            "ROCKSKIP_MIN_REPO_SIZE_MB",
            "-1",
            "all repos that are at least this big will be indexed using Rockskip",
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

        if !errors.is_empty() {
            return Err(ConfigError::new(errors.join("; ")));
        }

        Ok(Self {
            cache_dir: PathBuf::from(cache_dir),
            cache_size_bytes: non_negative(cache_size_mb) as u64 * 1_000_000,
            ctags_processes: non_negative(ctags_processes) as usize,
            request_buffer_size: non_negative(request_buffer_size) as usize,
            processing_timeout,
            max_concurrently_indexing: non_negative(max_concurrently_indexing) as usize,
            ctags_command,
            scip_ctags_command,
            ctags_pattern_length_limit: non_negative(ctags_pattern_length_limit) as u32,
            ctags_log_errors,
            ctags_max_file_size: non_negative(ctags_max_file_size) as u64,
            ctags_max_symbols: non_negative(ctags_max_symbols) as u64,
            max_total_paths_length: non_negative(max_total_paths_length) as usize,
            max_file_size_bytes: non_negative(max_file_size_kb) as u64 * 1000,
            use_rockskip,
            gitserver_addrs,
        })
    }

    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.use_rockskip {
            // Baseline supports Rockskip; the Rust pilot does not (M8 scope).
            return Err(ConfigError::new(
                "USE_ROCKSKIP=true is not supported by the Rust symbols pilot",
            ));
        }
        if self.gitserver_addrs.is_empty() {
            return Err(ConfigError::new("SRC_GIT_SERVERS must not be empty"));
        }
        if self.ctags_processes == 0 {
            return Err(ConfigError::new(
                "CTAGS_PROCESSES must be greater than zero",
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

fn non_negative(value: i64) -> i64 {
    value.max(0)
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
        assert_eq!(parse_go_duration("2h0m0s"), Ok(Duration::from_secs(7200)));
        assert_eq!(parse_go_duration("10s"), Ok(Duration::from_secs(10)));
        assert_eq!(parse_go_duration("1.5h"), Ok(Duration::from_secs(5400)));
        assert_eq!(parse_go_duration("250ms"), Ok(Duration::from_millis(250)));
        assert_eq!(parse_go_duration("5"), Ok(Duration::from_nanos(5)));
        assert!(parse_go_duration("").is_err());
        assert!(parse_go_duration("2x").is_err());
    }

    #[test]
    fn parses_go_bools() {
        assert_eq!(parse_go_bool("True"), Ok(true));
        assert_eq!(parse_go_bool("0"), Ok(false));
        assert!(parse_go_bool("yes").is_err());
    }
}
