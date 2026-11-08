#![forbid(unsafe_code)]

//! `internal/env`'s package-level globals the worker consults outside a job
//! config.
//!
//! The baseline evaluates these in `init()` of `internal/env`, i.e. before any
//! `Configure`; here they are populated by [`register`] from the service's
//! [`EnvReader`], which resolves `<NAME>_<process>` overrides exactly like
//! `env.Get`.

use std::sync::OnceLock;

use sg_core::EnvReader;

/// The literal description registered by the baseline.
pub const INSECURE_DEV_DESCRIPTION: &str = "Running in insecure dev (local laptop) mode";

static INSECURE_DEV: OnceLock<bool> = OnceLock::new();

/// Register every `internal/env` global the worker uses.
pub fn register(env: &mut EnvReader) {
    let raw = env.get("INSECURE_DEV", "false", INSECURE_DEV_DESCRIPTION);
    // `InsecureDev, _ = strconv.ParseBool(...)`: an unparseable value is
    // silently treated as false.
    let _ = INSECURE_DEV.set(parse_go_bool(&raw).unwrap_or(false));
}

/// `env.InsecureDev`.
pub fn insecure_dev() -> bool {
    INSECURE_DEV.get().copied().unwrap_or(false)
}

/// `strconv.ParseBool`'s accepted spellings; everything else is an error.
pub(crate) fn parse_go_bool(raw: &str) -> Result<bool, ()> {
    match raw {
        "1" | "t" | "T" | "true" | "TRUE" | "True" => Ok(true),
        "0" | "f" | "F" | "false" | "FALSE" | "False" => Ok(false),
        _ => Err(()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_bool_matches_go() {
        for raw in ["1", "t", "T", "true", "TRUE", "True"] {
            assert_eq!(parse_go_bool(raw), Ok(true), "{raw}");
        }
        for raw in ["0", "f", "F", "false", "FALSE", "False"] {
            assert_eq!(parse_go_bool(raw), Ok(false), "{raw}");
        }
        // Go's ParseBool is case-sensitive and does not trim.
        for raw in ["", "yes", "on", " true", "2", "TRUE "] {
            assert_eq!(parse_go_bool(raw), Err(()), "{raw}");
        }
    }
}
