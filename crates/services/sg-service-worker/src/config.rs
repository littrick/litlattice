#![forbid(unsafe_code)]

//! `cmd/worker/shared/config.go`: `WORKER_JOB_ALLOWLIST` /
//! `WORKER_JOB_BLOCKLIST`.
//!
//! Two objects matter in the baseline and both are reproduced here:
//!
//! * the **package-level** `shared.config`, which `loadConfigs` fills with the
//!   job names and the two lists and which `shouldRunJob` /
//!   `emitJobCountMetrics` consult;
//! * the **local** `Config` inside `LoadConfig`, which is the value returned to
//!   `svcmain` and validated there.
//!
//! `LoadConfig` shadows the package-level variable (`var config Config` inside
//! the function), so the returned config's `Jobs` map is populated but its
//! allowlist/blocklist are always empty, and `validateConfigs`' error is stored
//! on it via `AddError` where the overridden `Config.Validate` never reads it
//! (see docs/modules/06_worker.md §1.2). [`WorkerConfigs`] carries both
//! objects so the service and the differential harness can show exactly what
//! the baseline does.

use sg_core::{ConfigError, EnvReader};

use crate::registry;

/// `cmd/worker/shared`: `const addr = ":3189"`.
pub const DEFAULT_PORT: &str = "3189";

const ALLOWLIST_DESCRIPTION: &str = "A comma-seprated list of names of jobs that should be enabled. The value \"all\" (the default) enables all jobs.";
const BLOCKLIST_DESCRIPTION: &str = "A comma-seprated list of names of jobs that should not be enabled. Values in this list take precedence over the allowlist.";

/// One `shared.Config`: an `env.BaseConfig` (`names` + `errs`) plus the two
/// job lists.
#[derive(Debug, Clone, Default)]
pub struct WorkerConfig {
    /// `config.names`: the compiled-in job names, set by `loadConfigs` on the
    /// package-level config only.
    pub names: Vec<String>,
    pub job_allowlist: Vec<String>,
    pub job_blocklist: Vec<String>,
    /// `env.BaseConfig.errs`, in the order `Get` appended them.
    errs: Vec<String>,
}

impl WorkerConfig {
    /// `(*Config).Load`: read both variables and `safeSplit` them.
    pub fn load(env: &mut EnvReader) -> Self {
        let mut errs = Vec::new();
        let job_allowlist = safe_split(
            &get_required(
                env,
                "WORKER_JOB_ALLOWLIST",
                "all",
                ALLOWLIST_DESCRIPTION,
                &mut errs,
            ),
            ',',
        );
        let job_blocklist = safe_split(
            &get_required(
                env,
                "WORKER_JOB_BLOCKLIST",
                "",
                BLOCKLIST_DESCRIPTION,
                &mut errs,
            ),
            ',',
        );

        Self {
            names: registry::job_names(),
            job_allowlist,
            job_blocklist,
            errs,
        }
    }

    /// `(*Config).Validate`: the override `svcmain` calls. It checks only the
    /// unknown-job condition and never consults `BaseConfig.errs`.
    pub fn validate(&self) -> Result<(), ConfigError> {
        for name in &self.job_allowlist {
            if !self.names.iter().any(|known| known == name) && name != "all" {
                return Err(ConfigError::new(format!("unknown job {name:?}")));
            }
        }
        for name in &self.job_blocklist {
            if !self.names.iter().any(|known| known == name) {
                return Err(ConfigError::new(format!("unknown job {name:?}")));
            }
        }
        Ok(())
    }

    /// `env.BaseConfig.Validate`: `errs` combined like
    /// `cockroachdb/errors.Append` (one error bare; several
    /// `N errors occurred:` followed by one tab-star bullet each).
    pub fn base_validation_error(&self) -> Option<String> {
        combine_errors(&self.errs)
    }

    /// `shouldRunJob`: blocklist takes precedence, then an exact allowlist
    /// entry or `"all"`.
    pub fn should_run(&self, name: &str) -> bool {
        if self.job_blocklist.iter().any(|candidate| candidate == name) {
            return false;
        }

        self.job_allowlist
            .iter()
            .any(|candidate| candidate == "all" || candidate == name)
    }
}

/// `cmd/worker/shared.LoadConfig`: both `shared.Config` values.
#[derive(Debug, Clone)]
pub struct WorkerConfigs {
    /// The local config `LoadConfig` returns; `svcmain` validates it.
    pub returned: WorkerConfig,
    /// The package-level config `loadConfigs` fills and `shouldRunJob` uses.
    pub global: WorkerConfig,
    pub listen_addr: String,
}

impl WorkerConfigs {
    pub fn load(env: &mut EnvReader, override_addr: Option<String>) -> Self {
        let global = WorkerConfig::load(env);

        // `validateConfigs`: the worker config first; its error is wrapped and
        // then stored on the *local* config with `AddError`. The per-job
        // configs are validated only when the worker config is valid, and no
        // job configs are ported yet (later slices add them).
        let mut returned = WorkerConfig::default();
        if let Err(err) = global.validate() {
            returned
                .errs
                .push(format!("Failed to load configuration: {err}"));
        }

        let listen_addr = match override_addr {
            Some(addr) if !addr.is_empty() => addr,
            _ => format!(":{DEFAULT_PORT}"),
        };

        Self {
            returned,
            global,
            listen_addr,
        }
    }

    /// `shouldRunJob`, resolved against the package-level config.
    pub fn should_run(&self, name: &str) -> bool {
        self.global.should_run(name)
    }

    /// Test-only constructor: a package-level config with the given lists and
    /// a default (empty, valid) returned config. Avoids touching the process
    /// environment from tests that run in parallel with the ones above.
    #[cfg(test)]
    pub(crate) fn for_test(allowlist: &[&str], blocklist: &[&str]) -> Self {
        Self {
            returned: WorkerConfig::default(),
            global: WorkerConfig {
                names: registry::job_names(),
                job_allowlist: allowlist.iter().map(|value| (*value).to_owned()).collect(),
                job_blocklist: blocklist.iter().map(|value| (*value).to_owned()).collect(),
                errs: Vec::new(),
            },
            listen_addr: format!(":{DEFAULT_PORT}"),
        }
    }
}

/// `safeSplit` is `strings.Split` but returns nil (not `[""]`) on empty input.
pub fn safe_split(text: &str, sep: char) -> Vec<String> {
    if text.is_empty() {
        return Vec::new();
    }

    text.split(sep).map(str::to_owned).collect()
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

/// Format errors like `cockroachdb/errors.Append`.
pub(crate) fn combine_errors(errors: &[String]) -> Option<String> {
    match errors {
        [] => None,
        [single] => Some(single.clone()),
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

#[cfg(test)]
mod tests {
    use super::*;

    /// Tests run on parallel threads; the process environment is shared.
    static ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    fn with_env(vars: &[(&str, &str)], f: impl FnOnce()) {
        let _guard = ENV_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        for (name, value) in vars {
            std::env::set_var(name, value);
        }
        f();
        for (name, _) in vars {
            std::env::remove_var(name);
        }
    }

    #[test]
    fn safe_split_matches_strings_split() {
        assert!(safe_split("", ',').is_empty());
        assert_eq!(safe_split("all", ','), vec!["all"]);
        assert_eq!(
            safe_split("a,,b,", ','),
            vec!["a", "", "b", ""],
            "safeSplit keeps empty elements"
        );
    }

    #[test]
    fn blocklist_takes_precedence_and_all_covers_everything() {
        let config = WorkerConfig {
            names: registry::job_names(),
            job_allowlist: vec!["all".to_owned()],
            job_blocklist: vec!["insights-job".to_owned()],
            errs: Vec::new(),
        };
        assert!(!config.should_run("insights-job"));
        assert!(config.should_run("license-check"));

        let config = WorkerConfig {
            names: registry::job_names(),
            job_allowlist: vec!["license-check".to_owned()],
            job_blocklist: Vec::new(),
            errs: Vec::new(),
        };
        assert!(config.should_run("license-check"));
        assert!(!config.should_run("insights-job"));
    }

    #[test]
    fn validate_reports_the_first_unknown_name() {
        let names = registry::job_names();
        let config = WorkerConfig {
            names: names.clone(),
            job_allowlist: vec!["nope-xyz".to_owned()],
            job_blocklist: Vec::new(),
            errs: Vec::new(),
        };
        assert_eq!(
            config.validate().expect_err("unknown").to_string(),
            "unknown job \"nope-xyz\""
        );

        // "all" is only special in the allowlist.
        let config = WorkerConfig {
            names,
            job_allowlist: vec!["all".to_owned()],
            job_blocklist: vec!["all".to_owned()],
            errs: Vec::new(),
        };
        assert_eq!(
            config.validate().expect_err("unknown").to_string(),
            "unknown job \"all\""
        );
    }

    #[test]
    fn load_reports_an_empty_value_as_an_error_like_base_config() {
        with_env(
            &[("WORKER_JOB_ALLOWLIST", ""), ("WORKER_JOB_BLOCKLIST", "")],
            || {
                let mut env = EnvReader::new();
                let config = WorkerConfig::load(&mut env);
                assert!(config.job_allowlist.is_empty());
                assert!(config.job_blocklist.is_empty());
                assert_eq!(
                    config.base_validation_error().as_deref(),
                    Some(
                        "2 errors occurred:\n\t* invalid value \"\" for WORKER_JOB_ALLOWLIST: no value supplied\n\t* invalid value \"\" for WORKER_JOB_BLOCKLIST: no value supplied"
                    )
                );
            },
        );
    }

    #[test]
    fn a_returned_config_never_sees_the_allowlist() {
        with_env(&[("WORKER_JOB_ALLOWLIST", "nope-xyz")], || {
            let mut env = EnvReader::new();
            let configs = WorkerConfigs::load(&mut env, None);
            // The package-level config has the list and rejects the name...
            assert_eq!(
                configs.global.validate().expect_err("unknown").to_string(),
                "unknown job \"nope-xyz\""
            );
            // ...but the returned config is empty and validates cleanly; the
            // wrapped error sits unread in its BaseConfig.errs.
            assert!(configs.returned.job_allowlist.is_empty());
            assert!(configs.returned.validate().is_ok());
            assert_eq!(
                configs.returned.base_validation_error().as_deref(),
                Some("Failed to load configuration: unknown job \"nope-xyz\"")
            );
        });
    }
}
