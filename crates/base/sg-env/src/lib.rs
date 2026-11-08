#![forbid(unsafe_code)]

//! `sg-env`: environment variable registry with per-process overrides.
//!
//! Mirrors the baseline `internal/env` contract:
//! - values are read once and registered with a default and description;
//! - `<NAME>_<process>` overrides `<NAME>` for the current binary;
//! - `process` is `argv[0]`'s basename with `-` replaced by `_` (case preserved);
//! - after `lock()`, further reads are a programming error;
//! - `help`/`-h`/`--help` prints the sorted registry and exits 0.

use std::collections::HashMap;
use std::env;
use std::path::Path;

#[derive(Debug, Clone)]
struct EnvVar {
    description: String,
    value: String,
}

#[derive(Debug)]
pub struct EnvReader {
    process: String,
    vars: HashMap<String, EnvVar>,
    locked: bool,
}

impl Default for EnvReader {
    fn default() -> Self {
        Self::new()
    }
}

impl EnvReader {
    pub fn new() -> Self {
        Self {
            process: process_name(),
            vars: HashMap::new(),
            locked: false,
        }
    }

    pub fn process_name(&self) -> &str {
        &self.process
    }

    /// Register (or return an already registered) variable.
    pub fn get(&mut self, name: &str, default: &str, description: &str) -> String {
        assert!(!self.locked, "env.Get({name}) called after env.Lock()");

        if let Some(existing) = self.vars.get(name) {
            return existing.value.clone();
        }

        let value = self.resolve(name, default);
        self.vars.insert(
            name.to_owned(),
            EnvVar {
                description: description.to_owned(),
                value: value.clone(),
            },
        );
        value
    }

    pub fn get_bool(&mut self, name: &str, default: bool, description: &str) -> bool {
        let raw = self.get(name, if default { "true" } else { "false" }, description);
        parse_bool(&raw)
    }

    /// Baseline `env.ChooseFallbackVariableName`: pick the first name whose raw
    /// environment value is non-empty, falling back to `first`, then register
    /// only the chosen name.
    pub fn get_with_fallback(
        &mut self,
        first: &str,
        additional: &[&str],
        default: &str,
        description: &str,
    ) -> String {
        let chosen = std::iter::once(first)
            .chain(additional.iter().copied())
            .find(|name| {
                env::var(name)
                    .map(|value| !value.is_empty())
                    .unwrap_or(false)
            })
            .unwrap_or(first);
        self.get(chosen, default, description)
    }

    /// Read a variable registered earlier; panics if it was never registered.
    pub fn value(&self, name: &str) -> String {
        assert!(self.locked, "env.value({name}) called before env.Lock()");
        self.vars
            .get(name)
            .map(|v| v.value.clone())
            .unwrap_or_default()
    }

    pub fn lock(&mut self) {
        self.locked = true;
    }

    pub fn help_string(&self) -> String {
        let mut names: Vec<&String> = self.vars.keys().collect();
        names.sort();

        let mut out = String::from("Environment variables:\n");
        for name in names {
            let var = &self.vars[name];
            out.push_str(&format!(
                "  {:<40} {} (value: {:?})\n",
                name, var.description, var.value
            ));
        }
        out
    }

    fn resolve(&self, name: &str, default: &str) -> String {
        let per_process = format!("{name}_{}", self.process);
        match env::var(&per_process) {
            Ok(v) => v,
            Err(_) => env::var(name).unwrap_or_else(|_| default.to_owned()),
        }
    }
}

/// `argv[0]` basename, `-` replaced by `_`, case preserved (baseline `findName`).
pub fn process_name() -> String {
    env::args_os()
        .next()
        .as_deref()
        .and_then(|arg| Path::new(arg).file_name())
        .and_then(|name| name.to_str())
        .map(|name| name.replace('-', "_"))
        .unwrap_or_else(|| "unknown".to_owned())
}

pub fn my_name() -> String {
    process_name()
}

/// Print the registry and exit 0 when the first CLI argument is a help flag.
pub fn handle_help_flag(reader: &EnvReader) {
    if let Some(arg) = env::args().nth(1) {
        if matches!(arg.as_str(), "help" | "-h" | "--help") {
            println!("{}", reader.help_string());
            std::process::exit(0);
        }
    }
}

fn parse_bool(raw: &str) -> bool {
    matches!(
        raw.trim().to_ascii_lowercase().as_str(),
        "1" | "t" | "true" | "y" | "yes" | "on"
    )
}

/// `time.ParseDuration` (Go). Supports the same unit set and concatenation
/// rules as the baseline config values (`1s`, `25m`, `168h`, fractions,
/// negative durations); the error strings mirror Go's for the common
/// malformed inputs.
///
/// `std::time::Duration` cannot be negative; every baseline use of these
/// values feeds a ticker or deadline, where Go would panic on a negative
/// duration, so negative results clamp to zero (and are otherwise preserved
/// in magnitude only when the caller only cares about positivity).
pub fn parse_go_duration(value: &str) -> Result<std::time::Duration, String> {
    use std::time::Duration;

    let invalid = || format!("time: invalid duration {value:?}");
    if value.is_empty() {
        return Err(invalid());
    }
    let (negative, rest) = match value.as_bytes()[0] {
        b'-' => (true, &value[1..]),
        b'+' => (false, &value[1..]),
        _ => (false, value),
    };
    // `time.ParseDuration` special-cases a lone zero ("0", "+0", "-0").
    if rest == "0" {
        return Ok(Duration::ZERO);
    }
    if rest.is_empty() {
        return Err(invalid());
    }

    let mut remaining = rest;
    let mut total_nanos: u128 = 0;
    while !remaining.is_empty() {
        let number_len = remaining
            .find(|c: char| !(c.is_ascii_digit() || c == '.'))
            .unwrap_or(remaining.len());
        if number_len == 0 {
            return Err(invalid());
        }
        let number: f64 = match remaining[..number_len].parse::<f64>() {
            Ok(number) if number.is_finite() && number >= 0.0 => number,
            _ => return Err(invalid()),
        };
        remaining = &remaining[number_len..];
        let (unit_nanos, after) = if let Some(r) = remaining.strip_prefix("ns") {
            (1.0, r)
        } else if let Some(r) = remaining.strip_prefix("us") {
            (1_000.0, r)
        } else if let Some(r) = remaining.strip_prefix("µs") {
            (1_000.0, r)
        } else if let Some(r) = remaining.strip_prefix("μs") {
            (1_000.0, r)
        } else if let Some(r) = remaining.strip_prefix("ms") {
            (1_000_000.0, r)
        } else if let Some(r) = remaining.strip_prefix('s') {
            (1_000_000_000.0, r)
        } else if let Some(r) = remaining.strip_prefix('m') {
            (60_000_000_000.0, r)
        } else if let Some(r) = remaining.strip_prefix('h') {
            (3_600_000_000_000.0, r)
        } else if remaining.is_empty() {
            return Err(format!("time: missing unit in duration {value:?}"));
        } else {
            return Err(format!("time: invalid duration {value:?}"));
        };
        total_nanos += (number * unit_nanos).round() as u128;
        remaining = after;
    }

    let total_nanos = if negative {
        if total_nanos > i64::MAX as u128 {
            return Err(invalid());
        }
        -(total_nanos as i128)
    } else {
        if total_nanos > i64::MAX as u128 {
            return Err(invalid());
        }
        total_nanos as i128
    };
    Ok(Duration::from_nanos(total_nanos.max(0) as u64))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn help_is_sorted_and_formatted() {
        let mut env = EnvReader::new();
        env.get("B_VAR", "2", "second");
        env.get("A_VAR", "1", "first");
        let help = env.help_string();

        let a = help.find("A_VAR").expect("A_VAR");
        let b = help.find("B_VAR").expect("B_VAR");
        assert!(a < b);
        assert!(help.contains("(value: \"1\")"));
    }

    #[test]
    fn go_durations_parse_like_time_parse_duration() {
        use std::time::Duration;
        assert_eq!(parse_go_duration("1s"), Ok(Duration::from_secs(1)));
        assert_eq!(parse_go_duration("168h"), Ok(Duration::from_secs(604_800)));
        assert_eq!(parse_go_duration("25m"), Ok(Duration::from_secs(1500)));
        assert_eq!(parse_go_duration("1.5s"), Ok(Duration::from_millis(1500)));
        assert_eq!(parse_go_duration("1h30m"), Ok(Duration::from_secs(5400)));
        assert_eq!(parse_go_duration("0"), Ok(Duration::ZERO));
        assert_eq!(
            parse_go_duration("1x"),
            Err("time: invalid duration \"1x\"".to_owned())
        );
        assert_eq!(
            parse_go_duration("5"),
            Err("time: missing unit in duration \"5\"".to_owned())
        );
    }

    #[test]
    fn per_process_override_wins() {
        let mut env = EnvReader::new();
        let name = "SG_ENV_TEST_OVERRIDE";
        let per_process = format!("{name}_{}", env.process_name());
        env::set_var(name, "base");
        env::set_var(&per_process, "per-process");
        assert_eq!(env.get(name, "default", ""), "per-process");
        env::remove_var(name);
        env::remove_var(&per_process);
    }
}
