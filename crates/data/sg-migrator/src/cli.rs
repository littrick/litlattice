#![forbid(unsafe_code)]

//! Flag parsing for the migrator subcommands. The command table itself lives
//! in `sg-bin-migrator` (already ported); this module turns `args` into
//! typed options.
//!
//! The baseline uses `urfave/cli` v2. This module deliberately implements a
//! small, self-contained subset of its flag grammar:
//!
//! * `-name value`, `--name value`, `-name=value`, `--name=value`;
//! * bare boolean flags (`-force`), plus the explicit `-force=true`,
//!   `-force=false` and (as a convenience) `-force true` forms;
//! * repeated flags for `StringSlice`/`IntSlice` flags, with comma-separated
//!   values accepted as urfave does;
//! * `--` terminates flag parsing;
//! * unknown flags fail with urfave's `flag provided but not defined: -x`
//!   message, and missing values fail with `flag needs an argument: -x`.
//!
//! Integer conversion errors are reported against the flag that supplied the
//! value (`invalid value "abc" for flag -target: ...`) rather than exactly
//! mirroring Go's `strconv.Atoi`/`flag` wording.

use crate::error::MigratorError;

/// Common migration flags shared by `up`/`upto`/`downto`.
///
/// `schema` and `privileged_hash` are modelled as single strings because that
/// is the shape of this struct in the Rust port. Repeated and comma-separated
/// values (the Go `StringSlice` behaviour) are joined with `,`; pass the result
/// through `split(',')` if the consumer needs the individual values back.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MigrationOptions {
    pub schema: Option<String>,
    pub target: Option<i32>,
    pub unprivileged_only: bool,
    pub noop_privileged: bool,
    pub privileged_hash: String,
    pub ignore_single_dirty_log: bool,
    pub ignore_single_pending_log: bool,
    /// Accepted for `up`; ignored by this parser.
    pub skip_upgrade_validation: bool,
    /// Accepted for `up`; ignored by this parser.
    pub skip_oobmigration_validation: bool,
    /// Tolerated by the migration family; consumed by `upgrade`/`downgrade`.
    pub skip_version_check: bool,
    /// Tolerated by the migration family; consumed by `upgrade`/`downgrade`.
    pub skip_drift_check: bool,
    /// Tolerated by the migration family; consumed by `upgrade`/`downgrade`.
    pub dry_run: bool,
    /// Tolerated by the migration family; consumed by `upgrade`/`downgrade`.
    pub disable_animation: bool,
}

/// `describe` flags (`cliutil/describe.go`).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DescribeOptions {
    pub schema: Option<String>,
    pub format: Option<String>,
    pub out: Option<String>,
    pub force: bool,
    pub no_color: bool,
}

/// `drift` flags (`cliutil/drift.go`). `auto_fix` is only registered in
/// development builds upstream, but is parsed unconditionally here.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DriftOptions {
    pub schema: Option<String>,
    pub version: Option<String>,
    pub file: Option<String>,
    pub skip_version_check: bool,
    pub ignore_migrator_update: bool,
    pub auto_fix: bool,
}

/// `add-log` flags (`cliutil/addlog.go`). The `up` flag defaults to `true`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AddLogOptions {
    pub schema: Option<String>,
    pub version: Option<i32>,
    pub up: bool,
}

impl Default for AddLogOptions {
    fn default() -> Self {
        Self {
            schema: None,
            version: None,
            up: true,
        }
    }
}

/// `upgrade` / `downgrade` flags (`cliutil/upgrade.go`, `cliutil/downgrade.go`).
/// Both commands declare the same flag set.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct UpgradeOptions {
    pub from: Option<String>,
    pub to: Option<String>,
    pub unprivileged_only: bool,
    pub noop_privileged: bool,
    /// `StringSlice`: repeated and comma-separated values accumulate.
    pub privileged_hash: Vec<String>,
    pub skip_version_check: bool,
    pub skip_drift_check: bool,
    pub ignore_migrator_update: bool,
    pub dry_run: bool,
    pub disable_animation: bool,
}

/// `run-out-of-band-migrations` flags (`cliutil/run_oobmigrations.go`).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RunOutOfBandOptions {
    /// `IntSlice`: repeated and comma-separated ids accumulate.
    pub id: Vec<i32>,
    pub apply_reverse: bool,
    pub disable_animation: bool,
}

/// `validate` flags (`cliutil/validate.go`).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ValidateOptions {
    pub schema: Option<String>,
    pub skip_out_of_band_migrations: bool,
}

/// Parse the flags following a subcommand name.
///
/// This is the union of the flags accepted by `up`, `upto` and `downto`
/// (`target` is only meaningful for `upto`/`downto`). The `skip-*`, `dry-run`
/// and `disable-animation` flags are accepted so that a caller that does not
/// split the family by command can still parse them; they are exposed as
/// fields rather than silently dropped.
pub fn parse_options(args: &[String]) -> Result<MigrationOptions, MigratorError> {
    let raw = parse_flags(args, MIGRATION_FLAGS)?;

    Ok(MigrationOptions {
        schema: raw.string_slice("schema"),
        target: raw.last_int("target")?,
        unprivileged_only: raw.flag_bool("unprivileged-only"),
        noop_privileged: raw.flag_bool("noop-privileged"),
        privileged_hash: raw.strings("privileged-hash").join(","),
        ignore_single_dirty_log: raw.flag_bool("ignore-single-dirty-log"),
        ignore_single_pending_log: raw.flag_bool("ignore-single-pending-log"),
        skip_upgrade_validation: raw.flag_bool("skip-upgrade-validation"),
        skip_oobmigration_validation: raw.flag_bool("skip-oobmigration-validation"),
        skip_version_check: raw.flag_bool("skip-version-check"),
        skip_drift_check: raw.flag_bool("skip-drift-check"),
        dry_run: raw.flag_bool("dry-run"),
        disable_animation: raw.flag_bool("disable-animation"),
    })
}

/// Alias for [`parse_options`], for call sites that prefer the explicit name.
pub fn parse_migration_options(args: &[String]) -> Result<MigrationOptions, MigratorError> {
    parse_options(args)
}

/// Parse `describe` flags.
pub fn parse_describe_options(args: &[String]) -> Result<DescribeOptions, MigratorError> {
    let raw = parse_flags(args, DESCRIBE_FLAGS)?;

    Ok(DescribeOptions {
        schema: raw.non_empty_string("schema"),
        format: raw.non_empty_string("format"),
        out: raw.non_empty_string("out"),
        force: raw.flag_bool("force"),
        no_color: raw.flag_bool("no-color"),
    })
}

/// Alias for [`parse_describe_options`].
pub fn parse_describe(args: &[String]) -> Result<DescribeOptions, MigratorError> {
    parse_describe_options(args)
}

/// Parse `drift` flags.
pub fn parse_drift_options(args: &[String]) -> Result<DriftOptions, MigratorError> {
    let raw = parse_flags(args, DRIFT_FLAGS)?;

    Ok(DriftOptions {
        schema: raw.non_empty_string("schema"),
        version: raw.non_empty_string("version"),
        file: raw.non_empty_string("file"),
        skip_version_check: raw.flag_bool("skip-version-check"),
        ignore_migrator_update: raw.flag_bool("ignore-migrator-update"),
        auto_fix: raw.flag_bool("auto-fix"),
    })
}

/// Alias for [`parse_drift_options`].
pub fn parse_drift(args: &[String]) -> Result<DriftOptions, MigratorError> {
    parse_drift_options(args)
}

/// Parse `add-log` flags.
pub fn parse_add_log_options(args: &[String]) -> Result<AddLogOptions, MigratorError> {
    let raw = parse_flags(args, ADD_LOG_FLAGS)?;

    Ok(AddLogOptions {
        schema: raw.non_empty_string("schema"),
        version: raw.last_int("version")?,
        up: raw.flag_bool_or("up", true),
    })
}

/// Alias for [`parse_add_log_options`].
pub fn parse_add_log(args: &[String]) -> Result<AddLogOptions, MigratorError> {
    parse_add_log_options(args)
}

/// Parse `upgrade` flags.
pub fn parse_upgrade_options(args: &[String]) -> Result<UpgradeOptions, MigratorError> {
    let raw = parse_flags(args, UPGRADE_FLAGS)?;

    Ok(UpgradeOptions {
        from: raw.non_empty_string("from"),
        to: raw.non_empty_string("to"),
        unprivileged_only: raw.flag_bool("unprivileged-only"),
        noop_privileged: raw.flag_bool("noop-privileged"),
        privileged_hash: raw.strings("privileged-hash"),
        skip_version_check: raw.flag_bool("skip-version-check"),
        skip_drift_check: raw.flag_bool("skip-drift-check"),
        ignore_migrator_update: raw.flag_bool("ignore-migrator-update"),
        dry_run: raw.flag_bool("dry-run"),
        disable_animation: raw.flag_bool("disable-animation"),
    })
}

/// Alias for [`parse_upgrade_options`].
pub fn parse_upgrade(args: &[String]) -> Result<UpgradeOptions, MigratorError> {
    parse_upgrade_options(args)
}

/// Parse `downgrade` flags. The flag set is identical to `upgrade`.
pub fn parse_downgrade_options(args: &[String]) -> Result<UpgradeOptions, MigratorError> {
    parse_upgrade_options(args)
}

/// Alias for [`parse_downgrade_options`].
pub fn parse_downgrade(args: &[String]) -> Result<UpgradeOptions, MigratorError> {
    parse_downgrade_options(args)
}

/// Parse `run-out-of-band-migrations` flags.
pub fn parse_run_out_of_band_options(
    args: &[String],
) -> Result<RunOutOfBandOptions, MigratorError> {
    let raw = parse_flags(args, RUN_OUT_OF_BAND_FLAGS)?;

    Ok(RunOutOfBandOptions {
        id: raw.ints("id")?,
        apply_reverse: raw.flag_bool("apply-reverse"),
        disable_animation: raw.flag_bool("disable-animation"),
    })
}

/// Alias for [`parse_run_out_of_band_options`].
pub fn parse_run_out_of_band(args: &[String]) -> Result<RunOutOfBandOptions, MigratorError> {
    parse_run_out_of_band_options(args)
}

/// Alias for [`parse_run_out_of_band_options`] using the command's full name.
pub fn parse_run_out_of_band_migrations(
    args: &[String],
) -> Result<RunOutOfBandOptions, MigratorError> {
    parse_run_out_of_band_options(args)
}

/// Parse `validate` flags.
pub fn parse_validate_options(args: &[String]) -> Result<ValidateOptions, MigratorError> {
    let raw = parse_flags(args, VALIDATE_FLAGS)?;

    Ok(ValidateOptions {
        schema: raw.string_slice("schema"),
        skip_out_of_band_migrations: raw.flag_bool("skip-out-of-band-migrations"),
    })
}

/// Alias for [`parse_validate_options`].
pub fn parse_validate(args: &[String]) -> Result<ValidateOptions, MigratorError> {
    parse_validate_options(args)
}

// ---------------------------------------------------------------------------
// Flag tables
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FlagKind {
    Bool,
    /// A scalar `StringFlag`.
    Str,
    /// A scalar `IntFlag` (parsed on access).
    Int,
    /// A `StringSliceFlag` (repeatable, comma-separated).
    StrSlice,
    /// An `IntSliceFlag` (repeatable, comma-separated).
    IntSlice,
}

#[derive(Debug, Clone, Copy)]
struct FlagSpec {
    name: &'static str,
    kind: FlagKind,
    aliases: &'static [&'static str],
}

const MIGRATION_FLAGS: &[FlagSpec] = &[
    FlagSpec {
        name: "schema",
        kind: FlagKind::StrSlice,
        aliases: &["db"],
    },
    FlagSpec {
        name: "target",
        kind: FlagKind::StrSlice,
        aliases: &[],
    },
    FlagSpec {
        name: "unprivileged-only",
        kind: FlagKind::Bool,
        aliases: &[],
    },
    FlagSpec {
        name: "noop-privileged",
        kind: FlagKind::Bool,
        aliases: &[],
    },
    FlagSpec {
        name: "privileged-hash",
        kind: FlagKind::StrSlice,
        aliases: &[],
    },
    FlagSpec {
        name: "ignore-single-dirty-log",
        kind: FlagKind::Bool,
        aliases: &[],
    },
    FlagSpec {
        name: "ignore-single-pending-log",
        kind: FlagKind::Bool,
        aliases: &[],
    },
    FlagSpec {
        name: "skip-upgrade-validation",
        kind: FlagKind::Bool,
        aliases: &[],
    },
    FlagSpec {
        name: "skip-oobmigration-validation",
        kind: FlagKind::Bool,
        aliases: &[],
    },
    FlagSpec {
        name: "skip-version-check",
        kind: FlagKind::Bool,
        aliases: &[],
    },
    FlagSpec {
        name: "skip-drift-check",
        kind: FlagKind::Bool,
        aliases: &[],
    },
    FlagSpec {
        name: "dry-run",
        kind: FlagKind::Bool,
        aliases: &[],
    },
    FlagSpec {
        name: "disable-animation",
        kind: FlagKind::Bool,
        aliases: &[],
    },
];

const DESCRIBE_FLAGS: &[FlagSpec] = &[
    FlagSpec {
        name: "schema",
        kind: FlagKind::Str,
        aliases: &["db"],
    },
    FlagSpec {
        name: "format",
        kind: FlagKind::Str,
        aliases: &[],
    },
    FlagSpec {
        name: "out",
        kind: FlagKind::Str,
        aliases: &[],
    },
    FlagSpec {
        name: "force",
        kind: FlagKind::Bool,
        aliases: &[],
    },
    FlagSpec {
        name: "no-color",
        kind: FlagKind::Bool,
        aliases: &[],
    },
];

const DRIFT_FLAGS: &[FlagSpec] = &[
    FlagSpec {
        name: "schema",
        kind: FlagKind::Str,
        aliases: &["db"],
    },
    FlagSpec {
        name: "version",
        kind: FlagKind::Str,
        aliases: &[],
    },
    FlagSpec {
        name: "file",
        kind: FlagKind::Str,
        aliases: &[],
    },
    FlagSpec {
        name: "skip-version-check",
        kind: FlagKind::Bool,
        aliases: &[],
    },
    FlagSpec {
        name: "ignore-migrator-update",
        kind: FlagKind::Bool,
        aliases: &[],
    },
    FlagSpec {
        name: "auto-fix",
        kind: FlagKind::Bool,
        aliases: &["autofix"],
    },
];

const ADD_LOG_FLAGS: &[FlagSpec] = &[
    FlagSpec {
        name: "schema",
        kind: FlagKind::Str,
        aliases: &["db"],
    },
    FlagSpec {
        name: "version",
        kind: FlagKind::Int,
        aliases: &[],
    },
    FlagSpec {
        name: "up",
        kind: FlagKind::Bool,
        aliases: &[],
    },
];

const UPGRADE_FLAGS: &[FlagSpec] = &[
    FlagSpec {
        name: "from",
        kind: FlagKind::Str,
        aliases: &[],
    },
    FlagSpec {
        name: "to",
        kind: FlagKind::Str,
        aliases: &[],
    },
    FlagSpec {
        name: "unprivileged-only",
        kind: FlagKind::Bool,
        aliases: &[],
    },
    FlagSpec {
        name: "noop-privileged",
        kind: FlagKind::Bool,
        aliases: &[],
    },
    FlagSpec {
        name: "privileged-hash",
        kind: FlagKind::StrSlice,
        aliases: &[],
    },
    FlagSpec {
        name: "skip-version-check",
        kind: FlagKind::Bool,
        aliases: &[],
    },
    FlagSpec {
        name: "skip-drift-check",
        kind: FlagKind::Bool,
        aliases: &[],
    },
    FlagSpec {
        name: "ignore-migrator-update",
        kind: FlagKind::Bool,
        aliases: &[],
    },
    FlagSpec {
        name: "dry-run",
        kind: FlagKind::Bool,
        aliases: &[],
    },
    FlagSpec {
        name: "disable-animation",
        kind: FlagKind::Bool,
        aliases: &[],
    },
];

const RUN_OUT_OF_BAND_FLAGS: &[FlagSpec] = &[
    FlagSpec {
        name: "id",
        kind: FlagKind::IntSlice,
        aliases: &[],
    },
    FlagSpec {
        name: "apply-reverse",
        kind: FlagKind::Bool,
        aliases: &[],
    },
    FlagSpec {
        name: "disable-animation",
        kind: FlagKind::Bool,
        aliases: &[],
    },
];

const VALIDATE_FLAGS: &[FlagSpec] = &[
    FlagSpec {
        name: "schema",
        kind: FlagKind::StrSlice,
        aliases: &["db"],
    },
    FlagSpec {
        name: "skip-out-of-band-migrations",
        kind: FlagKind::Bool,
        aliases: &[],
    },
];

// ---------------------------------------------------------------------------
// Parsing internals
// ---------------------------------------------------------------------------

/// Raw flag occurrences collected before being interpreted as a typed struct.
#[derive(Debug, Default)]
struct RawFlags {
    values: Vec<(&'static str, String)>,
    bools: Vec<(&'static str, bool)>,
}

impl RawFlags {
    /// Last scalar value supplied for `name` (urfave's `StringFlag` semantics).
    fn string(&self, name: &str) -> Option<String> {
        self.values
            .iter()
            .rev()
            .find(|(flag, _)| *flag == name)
            .map(|(_, value)| value.clone())
    }

    /// Last scalar value supplied for `name`, treating an empty value as absent.
    fn non_empty_string(&self, name: &str) -> Option<String> {
        self.string(name).filter(|value| !value.is_empty())
    }

    /// All values supplied for a `StringSlice` flag, comma-separated values
    /// expanded (urfave's `StringSliceFlag` semantics).
    fn strings(&self, name: &str) -> Vec<String> {
        self.values
            .iter()
            .filter(|(flag, _)| *flag == name)
            .flat_map(|(_, value)| value.split(',').map(str::to_owned))
            .collect()
    }

    /// A `StringSlice` flag collapsed back into the single-string shape used by
    /// [`MigrationOptions`]. An absent flag or a single empty value yields
    /// `None`, mirroring `sanitizeSchemaNames`/`parseTargets`.
    fn string_slice(&self, name: &str) -> Option<String> {
        let values = self.strings(name);
        match values.as_slice() {
            [] => None,
            [only] if only.is_empty() => None,
            _ => Some(values.join(",")),
        }
    }

    fn flag_bool(&self, name: &str) -> bool {
        self.flag_bool_or(name, false)
    }

    fn flag_bool_or(&self, name: &str, default: bool) -> bool {
        self.bools
            .iter()
            .rev()
            .find(|(flag, _)| *flag == name)
            .map(|(_, value)| *value)
            .unwrap_or(default)
    }

    /// Parse the last value of an `IntFlag`; empty values count as absent.
    fn last_int(&self, name: &str) -> Result<Option<i32>, MigratorError> {
        Ok(self.ints(name)?.into_iter().last())
    }

    /// Parse all values of an `IntSlice` flag.
    fn ints(&self, name: &str) -> Result<Vec<i32>, MigratorError> {
        self.strings(name)
            .into_iter()
            .filter(|value| !value.is_empty())
            .map(|value| parse_int(&value, name))
            .collect()
    }
}

fn find_spec(specs: &'static [FlagSpec], name: &str) -> Option<FlagSpec> {
    specs
        .iter()
        .copied()
        .find(|spec| spec.name == name || spec.aliases.contains(&name))
}

fn parse_flags(args: &[String], specs: &'static [FlagSpec]) -> Result<RawFlags, MigratorError> {
    let mut raw = RawFlags::default();
    let mut index = 0;

    while index < args.len() {
        let arg = &args[index];

        if arg == "--" {
            if index + 1 < args.len() {
                return Err(too_many_arguments());
            }
            break;
        }

        if !arg.starts_with('-') || arg == "-" {
            return Err(too_many_arguments());
        }

        let body = arg.trim_start_matches('-');
        if body.is_empty() {
            return Err(too_many_arguments());
        }

        let (name, inline_value) = match body.split_once('=') {
            Some((name, value)) => (name, Some(value)),
            None => (body, None),
        };

        let spec = find_spec(specs, name).ok_or_else(|| unknown_flag(name))?;

        match spec.kind {
            FlagKind::Bool => {
                let value = match inline_value {
                    Some(value) => parse_bool(value, name)?,
                    None => {
                        // `urfave` bool flags are bare. Accept an explicit
                        // `true`/`false` in the following token as well.
                        let next = args
                            .get(index + 1)
                            .and_then(|next| parse_bool(next, name).ok());
                        if let Some(value) = next {
                            index += 1;
                            value
                        } else {
                            true
                        }
                    }
                };
                raw.bools.push((spec.name, value));
            }
            FlagKind::Str | FlagKind::Int | FlagKind::StrSlice | FlagKind::IntSlice => {
                let value = match inline_value {
                    Some(value) => value.to_owned(),
                    None => {
                        index += 1;
                        args.get(index)
                            .cloned()
                            .ok_or_else(|| needs_argument(name))?
                    }
                };
                raw.values.push((spec.name, value));
            }
        }

        index += 1;
    }

    Ok(raw)
}

fn parse_bool(value: &str, flag: &str) -> Result<bool, MigratorError> {
    match value {
        "1" | "t" | "T" | "TRUE" | "true" | "True" => Ok(true),
        "0" | "f" | "F" | "FALSE" | "false" | "False" => Ok(false),
        _ => Err(MigratorError::new(format!(
            "invalid boolean value {value:?} for flag -{flag}"
        ))),
    }
}

fn parse_int(value: &str, flag: &str) -> Result<i32, MigratorError> {
    value.parse::<i32>().map_err(|error| {
        MigratorError::new(format!("invalid value {value:?} for flag -{flag}: {error}"))
    })
}

fn unknown_flag(name: &str) -> MigratorError {
    MigratorError::new(format!("flag provided but not defined: -{name}"))
}

fn needs_argument(name: &str) -> MigratorError {
    MigratorError::new(format!("flag needs an argument: -{name}"))
}

fn too_many_arguments() -> MigratorError {
    MigratorError::new("too many arguments")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(items: &[&str]) -> Vec<String> {
        items.iter().map(|item| (*item).to_owned()).collect()
    }

    #[test]
    fn migration_defaults_are_empty() {
        assert_eq!(parse_options(&[]).unwrap(), MigrationOptions::default());
    }

    #[test]
    fn migration_parses_common_flags() {
        let options = parse_options(&args(&[
            "--schema",
            "frontend",
            "-target",
            "42",
            "--unprivileged-only",
            "-noop-privileged=false",
            "-privileged-hash",
            "abc",
            "-ignore-single-dirty-log",
            "-ignore-single-pending-log",
        ]))
        .unwrap();

        assert_eq!(options.schema.as_deref(), Some("frontend"));
        assert_eq!(options.target, Some(42));
        assert!(options.unprivileged_only);
        assert!(!options.noop_privileged);
        assert_eq!(options.privileged_hash, "abc");
        assert!(options.ignore_single_dirty_log);
        assert!(options.ignore_single_pending_log);
    }

    #[test]
    fn migration_supports_equals_forms() {
        let options = parse_options(&args(&[
            "--schema=codeintel",
            "--target=7",
            "--unprivileged-only=true",
        ]))
        .unwrap();

        assert_eq!(options.schema.as_deref(), Some("codeintel"));
        assert_eq!(options.target, Some(7));
        assert!(options.unprivileged_only);
    }

    #[test]
    fn migration_supports_db_alias_and_repeated_schema_names() {
        let options = parse_options(&args(&[
            "-db",
            "frontend",
            "--schema=codeintel",
            "-schema",
            "codeinsights,other",
        ]))
        .unwrap();

        assert_eq!(
            options.schema.as_deref(),
            Some("frontend,codeintel,codeinsights,other")
        );
    }

    #[test]
    fn migration_accepts_boolean_forms() {
        assert!(
            parse_options(&args(&["-unprivileged-only"]))
                .unwrap()
                .unprivileged_only
        );
        assert!(
            parse_options(&args(&["--unprivileged-only=true"]))
                .unwrap()
                .unprivileged_only
        );
        assert!(
            !parse_options(&args(&["-unprivileged-only=false"]))
                .unwrap()
                .unprivileged_only
        );
        assert!(
            !parse_options(&args(&["-unprivileged-only", "false"]))
                .unwrap()
                .unprivileged_only
        );
        assert!(
            parse_options(&args(&["-unprivileged-only", "1"]))
                .unwrap()
                .unprivileged_only
        );
    }

    #[test]
    fn migration_reports_target_parse_errors() {
        let error = parse_options(&args(&["-target", "not-a-number"])).unwrap_err();

        assert!(error
            .to_string()
            .contains("invalid value \"not-a-number\" for flag -target"));
        assert!(error.to_string().contains("invalid digit found in string"));
    }

    #[test]
    fn migration_tolerates_skip_and_dry_run_flags() {
        let options = parse_options(&args(&[
            "-skip-upgrade-validation",
            "-skip-oobmigration-validation",
            "-skip-version-check",
            "-skip-drift-check",
            "-dry-run",
            "-disable-animation",
        ]))
        .unwrap();

        assert!(options.skip_upgrade_validation);
        assert!(options.skip_oobmigration_validation);
        assert!(options.skip_version_check);
        assert!(options.skip_drift_check);
        assert!(options.dry_run);
        assert!(options.disable_animation);
    }

    #[test]
    fn migration_rejects_unknown_flags() {
        let error = parse_options(&args(&["-bogus"])).unwrap_err();
        assert_eq!(error.to_string(), "flag provided but not defined: -bogus");

        let error = parse_options(&args(&["--bogus=1"])).unwrap_err();
        assert_eq!(error.to_string(), "flag provided but not defined: -bogus");
    }

    #[test]
    fn migration_requires_a_value() {
        let error = parse_options(&args(&["-schema"])).unwrap_err();
        assert_eq!(error.to_string(), "flag needs an argument: -schema");
    }

    #[test]
    fn describe_parses_all_flags() {
        let options = parse_describe(&args(&[
            "-db",
            "frontend",
            "--format=json",
            "--out",
            "/tmp/schema.json",
            "-force",
            "-no-color",
        ]))
        .unwrap();

        assert_eq!(options.schema.as_deref(), Some("frontend"));
        assert_eq!(options.format.as_deref(), Some("json"));
        assert_eq!(options.out.as_deref(), Some("/tmp/schema.json"));
        assert!(options.force);
        assert!(options.no_color);
    }

    #[test]
    fn describe_scalar_schema_uses_the_last_value() {
        let options =
            parse_describe(&args(&["-schema", "frontend", "-schema", "codeintel"])).unwrap();
        assert_eq!(options.schema.as_deref(), Some("codeintel"));
    }

    #[test]
    fn describe_rejects_unknown_and_positional_arguments() {
        assert!(parse_describe(&args(&["-target", "5"])).is_err());
        assert_eq!(
            parse_describe(&args(&["stray"])).unwrap_err().to_string(),
            "too many arguments"
        );
    }

    #[test]
    fn drift_parses_all_flags_and_autofix_alias() {
        let options = parse_drift(&args(&[
            "-schema",
            "codeintel",
            "-version",
            "5.0.2",
            "-skip-version-check",
            "-ignore-migrator-update",
            "--autofix",
        ]))
        .unwrap();

        assert_eq!(options.schema.as_deref(), Some("codeintel"));
        assert_eq!(options.version.as_deref(), Some("5.0.2"));
        assert!(options.skip_version_check);
        assert!(options.ignore_migrator_update);
        assert!(options.auto_fix);

        let options = parse_drift(&args(&["-file", "schema.json", "--auto-fix"])).unwrap();
        assert_eq!(options.file.as_deref(), Some("schema.json"));
        assert!(options.auto_fix);
    }

    #[test]
    fn drift_rejects_unknown_flags() {
        let error = parse_drift(&args(&["-no-color"])).unwrap_err();
        assert_eq!(
            error.to_string(),
            "flag provided but not defined: -no-color"
        );
    }

    #[test]
    fn add_log_defaults_up_to_true() {
        let options = parse_add_log(&args(&["-db", "frontend"])).unwrap();

        assert_eq!(options.schema.as_deref(), Some("frontend"));
        assert_eq!(options.version, None);
        assert!(options.up);
        assert!(AddLogOptions::default().up);
    }

    #[test]
    fn add_log_parses_version_and_direction() {
        let options = parse_add_log(&args(&[
            "-schema",
            "frontend",
            "-version",
            "1680000000",
            "-up=false",
        ]))
        .unwrap();

        assert_eq!(options.version, Some(1_680_000_000));
        assert!(!options.up);
    }

    #[test]
    fn add_log_reports_version_parse_errors() {
        let error = parse_add_log(&args(&["-version", "abc"])).unwrap_err();
        assert!(error
            .to_string()
            .contains("invalid value \"abc\" for flag -version"));
    }

    #[test]
    fn upgrade_parses_all_flags_and_repeated_hashes() {
        let options = parse_upgrade(&args(&[
            "--from",
            "5.0",
            "--to=5.1",
            "-unprivileged-only",
            "-noop-privileged=false",
            "-privileged-hash",
            "aaa",
            "--privileged-hash=bbb",
            "-skip-version-check",
            "-skip-drift-check",
            "-ignore-migrator-update",
            "-dry-run",
            "-disable-animation",
        ]))
        .unwrap();

        assert_eq!(options.from.as_deref(), Some("5.0"));
        assert_eq!(options.to.as_deref(), Some("5.1"));
        assert!(options.unprivileged_only);
        assert!(!options.noop_privileged);
        assert_eq!(options.privileged_hash, vec!["aaa", "bbb"]);
        assert!(options.skip_version_check);
        assert!(options.skip_drift_check);
        assert!(options.ignore_migrator_update);
        assert!(options.dry_run);
        assert!(options.disable_animation);
    }

    #[test]
    fn downgrade_uses_the_same_flag_set() {
        let options = parse_downgrade(&args(&["-from", "5.1", "-to", "5.0"])).unwrap();
        assert_eq!(options.from.as_deref(), Some("5.1"));
        assert_eq!(options.to.as_deref(), Some("5.0"));
    }

    #[test]
    fn upgrade_rejects_unknown_and_positional_arguments() {
        assert!(parse_upgrade(&args(&["-schema", "frontend"])).is_err());
        assert_eq!(
            parse_upgrade(&args(&["5.0"])).unwrap_err().to_string(),
            "too many arguments"
        );
    }

    #[test]
    fn run_out_of_band_parses_repeated_ids() {
        let options = parse_run_out_of_band(&args(&[
            "-id",
            "12",
            "--id=13,14",
            "-apply-reverse",
            "-disable-animation",
        ]))
        .unwrap();

        assert_eq!(options.id, vec![12, 13, 14]);
        assert!(options.apply_reverse);
        assert!(options.disable_animation);

        let alias = parse_run_out_of_band_migrations(&args(&["-id", "1"])).unwrap();
        assert_eq!(alias.id, vec![1]);
    }

    #[test]
    fn run_out_of_band_reports_id_parse_errors() {
        let error = parse_run_out_of_band(&args(&["-id", "nope"])).unwrap_err();
        assert!(error
            .to_string()
            .contains("invalid value \"nope\" for flag -id"));
    }

    #[test]
    fn validate_parses_schema_slice_and_oob_flag() {
        let options = parse_validate(&args(&[
            "-db",
            "frontend",
            "--schema=codeintel",
            "-skip-out-of-band-migrations",
        ]))
        .unwrap();

        assert_eq!(options.schema.as_deref(), Some("frontend,codeintel"));
        assert!(options.skip_out_of_band_migrations);
    }

    #[test]
    fn validate_rejects_undefined_no_color() {
        let error = parse_validate(&args(&["-no-color"])).unwrap_err();
        assert_eq!(
            error.to_string(),
            "flag provided but not defined: -no-color"
        );
    }

    #[test]
    fn double_dash_terminates_flag_parsing() {
        assert_eq!(
            parse_options(&args(&["-schema", "frontend", "--"]))
                .unwrap()
                .schema
                .as_deref(),
            Some("frontend")
        );
        assert_eq!(
            parse_options(&args(&["--", "frontend"]))
                .unwrap_err()
                .to_string(),
            "too many arguments"
        );
    }
}
