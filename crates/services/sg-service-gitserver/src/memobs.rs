#![forbid(unsafe_code)]

//! Peak memory observation for `git` children, ported from
//! `internal/memcmd` and `cmd/gitserver/internal/git/gitcli/command.go`.
//!
//! The baseline attaches an observer to every `gitcli` child after `Start`
//! when `GITSERVER_MEMORY_OBSERVATION_ENABLED` is true (default false). On
//! Linux the observer samples the summed RSS of the child's process tree every
//! `MEMORY_OBSERVATION_DEFAULT_SAMPLING_INTERVAL` (default `1ms`) and keeps the
//! maximum. When the child has been waited on, `trace()` compares that peak
//! with `highMemoryUsageThreshold` (500 MiB) and increments
//! `src_gitserver_exec_high_memory_usage_count` for the subcommand.
//!
//! Notes:
//!
//! * The Go observer reads `/proc/<pid>/status` `VmRSS` and the process tree
//!   from `/proc/<pid>/task/<pid>/children`; this module does the same.
//! * `GITSERVER_MEMORY_OBSERVATION_ENABLED` and the sampling interval are read
//!   once per process, like the baseline's package-level `env.MustGet*`.
//!   Invalid values disable the observer instead of panicking at startup.
//! * The two direct `exec.CommandContext` call sites in the baseline
//!   (`internal/search`'s `git log` and `diff-tree --stdin`) do not use
//!   `gitcli`, so they are not observed (and record no `exec_*` metrics).

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

/// `gitcli.highMemoryUsageThreshold = 500 * bytesize.MiB`.
pub const HIGH_MEMORY_USAGE_THRESHOLD_BYTES: u64 = 500 * 1024 * 1024;

/// Sampling interval used when the environment variable is unset or invalid.
const DEFAULT_SAMPLING_INTERVAL: Duration = Duration::from_millis(1);

/// `GITSERVER_MEMORY_OBSERVATION_ENABLED`.
pub fn enabled() -> bool {
    static ENABLED: OnceLock<bool> = OnceLock::new();
    *ENABLED.get_or_init(|| {
        std::env::var("GITSERVER_MEMORY_OBSERVATION_ENABLED")
            .ok()
            .and_then(|value| parse_go_bool(&value))
            .unwrap_or(false)
    })
}

/// `MEMORY_OBSERVATION_DEFAULT_SAMPLING_INTERVAL` (Go duration string).
fn sampling_interval() -> Duration {
    static INTERVAL: OnceLock<Duration> = OnceLock::new();
    *INTERVAL.get_or_init(|| {
        std::env::var("MEMORY_OBSERVATION_DEFAULT_SAMPLING_INTERVAL")
            .ok()
            .and_then(|value| parse_go_duration(&value))
            .unwrap_or(DEFAULT_SAMPLING_INTERVAL)
    })
}

/// `gitcli.trace`: strict comparison, like `memUsage > highMemoryUsageThreshold`.
pub fn is_high_memory_usage(bytes: u64) -> bool {
    bytes > HIGH_MEMORY_USAGE_THRESHOLD_BYTES
}

/// A running observation of one child process tree.
///
/// Dropping it stops sampling and records the baseline counter when the peak
/// exceeded the threshold. The baseline stops the observer after `cmd.Wait`,
/// which is exactly when the owning [`crate::git::GitChild`] is dropped.
pub struct MemoryObserver {
    peak: Arc<AtomicU64>,
    stop: Arc<AtomicBool>,
    task: tokio::task::JoinHandle<()>,
    cmd: String,
}

impl MemoryObserver {
    /// Start observing `pid` for the subcommand `cmd`. `None` when observation
    /// is disabled (including an invalid or non-positive sampling interval,
    /// which makes the baseline `NewLinuxObserver` fail and fall back to the
    /// no-op observer).
    pub fn start(pid: u32, cmd: String) -> Option<Self> {
        if !enabled() {
            return None;
        }
        let interval = sampling_interval();
        if interval.is_zero() {
            return None;
        }
        let runtime = tokio::runtime::Handle::try_current().ok()?;

        let peak = Arc::new(AtomicU64::new(0));
        let stop = Arc::new(AtomicBool::new(false));
        let task_peak = Arc::clone(&peak);
        let task_stop = Arc::clone(&stop);
        let task = runtime.spawn(async move {
            loop {
                if task_stop.load(Ordering::Relaxed) {
                    break;
                }
                let usage = process_tree_rss(pid);
                task_peak.fetch_max(usage, Ordering::Relaxed);
                tokio::time::sleep(interval).await;
            }
        });

        Some(Self {
            peak,
            stop,
            task,
            cmd,
        })
    }

    /// The highest summed RSS sampled so far, in bytes.
    pub fn max_memory_usage(&self) -> u64 {
        self.peak.load(Ordering::Relaxed)
    }
}

impl Drop for MemoryObserver {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        self.task.abort();
        if is_high_memory_usage(self.max_memory_usage()) {
            crate::metrics::high_memory_usage(&self.cmd);
        }
    }
}

/// `memcmd.memoryUsageForPidAndChildren`: best-effort depth-first sum of the
/// `VmRSS` values of `pid` and its descendants. Missing processes (already
/// reaped) contribute nothing, like the baseline's ignored `fs.ErrNotExist`.
fn process_tree_rss(pid: u32) -> u64 {
    let mut total = 0u64;
    let mut stack = vec![pid];
    while let Some(current) = stack.pop() {
        total += process_rss(current).unwrap_or(0);
        if let Ok(children) =
            std::fs::read_to_string(format!("/proc/{current}/task/{current}/children"))
        {
            stack.extend(
                children
                    .split_whitespace()
                    .filter_map(|pid| pid.parse::<u32>().ok()),
            );
        }
    }
    total
}

/// `procfs` `RSS`: the `VmRSS` field of `/proc/<pid>/status`, converted from
/// KiB to bytes.
fn process_rss(pid: u32) -> Option<u64> {
    let status = std::fs::read_to_string(format!("/proc/{pid}/status")).ok()?;
    for line in status.lines() {
        let Some(rest) = line.strip_prefix("VmRSS:") else {
            continue;
        };
        let kib: u64 = rest.split_whitespace().next()?.parse().ok()?;
        return Some(kib * 1024);
    }
    None
}

/// `strconv.ParseBool` accepted spellings.
pub fn parse_go_bool(value: &str) -> Option<bool> {
    match value {
        "1" | "t" | "T" | "TRUE" | "true" | "True" => Some(true),
        "0" | "f" | "F" | "FALSE" | "false" | "False" => Some(false),
        _ => None,
    }
}

/// The subset of Go's `time.ParseDuration` the baseline accepts for the
/// sampling interval (`ns`, `us`/`µs`/`μs`, `ms`, `s`, `m`, `h`, optionally
/// fractional or concatenated). Negative or malformed values yield `None`;
/// a negative duration disables the observer in the baseline too.
pub fn parse_go_duration(value: &str) -> Option<Duration> {
    // Negative durations parse in Go but `NewLinuxObserver` rejects
    // intervals <= 0, so they disable the observer either way.
    if value.starts_with('-') {
        return None;
    }
    let rest = value.strip_prefix('+').unwrap_or(value);
    parse_positive_go_duration(rest)
}

fn parse_positive_go_duration(value: &str) -> Option<Duration> {
    if value.is_empty() {
        return None;
    }
    let mut rest = value;
    let mut total_nanos: f64 = 0.0;
    while !rest.is_empty() {
        let number_len = rest
            .find(|c: char| !(c.is_ascii_digit() || c == '.'))
            .unwrap_or(rest.len());
        if number_len == 0 {
            return None;
        }
        let number: f64 = rest[..number_len].parse().ok()?;
        if !number.is_finite() || number < 0.0 {
            return None;
        }
        rest = &rest[number_len..];
        let (unit_nanos, remaining) = if let Some(r) = rest.strip_prefix("ns") {
            (1.0, r)
        } else if let Some(r) = rest.strip_prefix("us") {
            (1_000.0, r)
        } else if let Some(r) = rest.strip_prefix("µs") {
            (1_000.0, r)
        } else if let Some(r) = rest.strip_prefix("μs") {
            (1_000.0, r)
        } else if let Some(r) = rest.strip_prefix("ms") {
            (1_000_000.0, r)
        } else if let Some(r) = rest.strip_prefix('s') {
            (1_000_000_000.0, r)
        } else if let Some(r) = rest.strip_prefix('m') {
            (60_000_000_000.0, r)
        } else {
            let remaining = rest.strip_prefix('h')?;
            (3_600_000_000_000.0, remaining)
        };
        total_nanos += number * unit_nanos;
        rest = remaining;
    }
    if total_nanos > u64::MAX as f64 {
        return None;
    }
    Some(Duration::from_nanos(total_nanos.round() as u64))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_go_booleans() {
        for value in ["1", "t", "T", "TRUE", "true", "True"] {
            assert_eq!(parse_go_bool(value), Some(true), "{value}");
        }
        for value in ["0", "f", "F", "FALSE", "false", "False"] {
            assert_eq!(parse_go_bool(value), Some(false), "{value}");
        }
        for value in ["", "yes", "2", "true "] {
            assert_eq!(parse_go_bool(value), None, "{value}");
        }
    }

    #[test]
    fn parses_go_durations() {
        assert_eq!(parse_go_duration("1ms"), Some(Duration::from_millis(1)));
        assert_eq!(parse_go_duration("1.5s"), Some(Duration::from_millis(1500)));
        assert_eq!(parse_go_duration("1m30s"), Some(Duration::from_secs(90)));
        assert_eq!(parse_go_duration("300us"), Some(Duration::from_micros(300)));
        assert_eq!(parse_go_duration("300µs"), Some(Duration::from_micros(300)));
        assert_eq!(parse_go_duration("2h"), Some(Duration::from_secs(7200)));
        assert_eq!(
            parse_go_duration("+250ms"),
            Some(Duration::from_millis(250))
        );
        assert_eq!(parse_go_duration("0s"), Some(Duration::ZERO));
    }

    #[test]
    fn rejects_invalid_go_durations() {
        for value in ["", "5", "1x", "1.5.5s", "ms", "1 s", "-1s", "1..5s"] {
            assert_eq!(parse_go_duration(value), None, "{value}");
        }
    }

    #[test]
    fn threshold_is_strict_and_equals_500_mib() {
        assert_eq!(HIGH_MEMORY_USAGE_THRESHOLD_BYTES, 524_288_000);
        assert!(!is_high_memory_usage(0));
        assert!(!is_high_memory_usage(HIGH_MEMORY_USAGE_THRESHOLD_BYTES));
        assert!(is_high_memory_usage(HIGH_MEMORY_USAGE_THRESHOLD_BYTES + 1));
    }

    #[test]
    fn reads_the_current_process_rss() {
        let rss = process_tree_rss(std::process::id());
        assert!(rss > 0, "expected a positive RSS for the test process");
    }
}
