#![forbid(unsafe_code)]

//! `cmd/migrator/shared`: the migrator CLI surface.
//!
//! The baseline command is a `urfave/cli` app (`appName = "migrator"`, usage
//! `Validates and runs schema migrations`) with `cli.ShowSubcommandHelp` as the
//! action and ten subcommands. Running with no arguments selects `up`.
//!
//! Ported here:
//!
//! * the command table (names, usages, flag names in declaration order);
//! * argument selection, including the no-argument `up` default and the
//!   `ShowSubcommandHelp` fallback for `--help`/`help`/unknown commands;
//! * the startup banner (`Sourcegraph migrator <version>`).
//!
//! Not ported: the migration runner itself. Migrations are owned by the Go
//! migrator (the M6 storage contract); this binary never introduces migration
//! files. Database commands are forwarded to the delegate named by
//! `MIGRATOR_DELEGATE` (default `migrator-go`) so mixed deployments keep the Go
//! migration convention.

use serde::Serialize;

/// `shared.appName`.
pub const APP_NAME: &str = "migrator";

/// `shared` app usage.
pub const APP_USAGE: &str = "Validates and runs schema migrations";

/// The development version (`internal/version` when no ldflags are set).
pub const DEV_VERSION: &str = "0.0.0+dev";

/// `shared.RegisterMigrators` command metadata.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct CommandSpec {
    pub name: &'static str,
    pub usage: &'static str,
    /// Flag names in the order `cliutil` declares them.
    pub flags: &'static [&'static str],
}

/// The ten `cliutil` commands in `shared.Start`'s order.
pub const COMMANDS: &[CommandSpec] = &[
    CommandSpec {
        name: "up",
        usage: "Apply all migrations",
        flags: &[
            "schema",
            "unprivileged-only",
            "noop-privileged",
            "privileged-hash",
            "ignore-single-dirty-log",
            "ignore-single-pending-log",
            "skip-upgrade-validation",
            "skip-oobmigration-validation",
        ],
    },
    CommandSpec {
        name: "upto",
        usage: "Ensure a given migration has been applied - may apply dependency migrations",
        flags: &[
            "schema",
            "target",
            "unprivileged-only",
            "noop-privileged",
            "privileged-hash",
            "ignore-single-dirty-log",
            "ignore-single-pending-log",
        ],
    },
    CommandSpec {
        name: "downto",
        usage: "Revert any applied migrations that are children of the given targets - this effectively \"resets\" the schema to the target version",
        flags: &[
            "schema",
            "target",
            "unprivileged-only",
            "noop-privileged",
            "privileged-hash",
            "ignore-single-dirty-log",
            "ignore-single-pending-log",
        ],
    },
    CommandSpec {
        name: "validate",
        usage: "Validate the current schema",
        flags: &["schema", "skip-out-of-band-migrations"],
    },
    CommandSpec {
        name: "describe",
        usage: "Describe the current database schema",
        flags: &["schema", "format", "out", "force", "no-color"],
    },
    CommandSpec {
        name: "drift",
        usage: "Detect differences between the current database schema and the expected schema",
        flags: &[
            "schema",
            "version",
            "file",
            "skip-version-check",
            "ignore-migrator-update",
        ],
    },
    CommandSpec {
        name: "add-log",
        usage: "Add an entry to the migration log",
        flags: &["schema", "version", "up"],
    },
    CommandSpec {
        name: "upgrade",
        usage: "Upgrade Sourcegraph instance databases to a target version",
        flags: &[
            "from",
            "to",
            "unprivileged-only",
            "noop-privileged",
            "privileged-hash",
            "skip-version-check",
            "skip-drift-check",
            "ignore-migrator-update",
            "dry-run",
            "disable-animation",
        ],
    },
    CommandSpec {
        name: "downgrade",
        usage: "Downgrade Sourcegraph instance databases to a target version",
        flags: &[
            "from",
            "to",
            "unprivileged-only",
            "noop-privileged",
            "privileged-hash",
            "skip-version-check",
            "skip-drift-check",
            "ignore-migrator-update",
            "dry-run",
            "disable-animation",
        ],
    },
    CommandSpec {
        name: "run-out-of-band-migrations",
        usage: "Run incomplete out of band migrations.",
        flags: &["id", "apply-reverse", "disable-animation"],
    },
];

/// The startup banner: `Sourcegraph migrator %s`.
pub fn banner(version: &str) -> String {
    format!("Sourcegraph migrator {version}")
}

/// What the CLI selected for an argument vector (excluding argv[0]).
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Selection {
    /// A known subcommand; `args` are the arguments after the command name.
    Command {
        name: String,
        args: Vec<String>,
    },
    /// `cli.ShowSubcommandHelp`: `--help`, `help`, or an unknown command.
    Help,
}

impl Selection {
    /// `Start`'s `len(os.Args) == 1` default: no arguments means `up`.
    pub fn effective_argv(argv: &[String]) -> Vec<String> {
        if argv.is_empty() {
            vec!["up".to_owned()]
        } else {
            argv.to_vec()
        }
    }

    /// Select the command for `argv` (excluding the program name). A known
    /// first token runs that command; everything else falls through to the
    /// app action `ShowSubcommandHelp` (which exits 0).
    pub fn select(argv: &[String]) -> Selection {
        let argv = Self::effective_argv(argv);
        let Some(first) = argv.first() else {
            return Selection::Help;
        };
        if let Some(command) = COMMANDS.iter().find(|command| command.name == first) {
            return Selection::Command {
                name: command.name.to_owned(),
                args: argv[1..].to_vec(),
            };
        }
        Selection::Help
    }

    pub fn exit_code(&self) -> i32 {
        // `ShowSubcommandHelp` returns nil; known commands are forwarded (the
        // delegate supplies their exit code).
        match self {
            Self::Command { .. } => 0,
            Self::Help => 0,
        }
    }
}

/// Render the `ShowSubcommandHelp` body. The baseline's urfave layout is
/// reproduced closely enough for operators; the differential harness compares
/// [`COMMANDS`], not this text.
pub fn help_text() -> String {
    let mut out = format!(
        "NAME:\n   {APP_NAME} - {APP_USAGE}\n\nUSAGE:\n   {APP_NAME} [global options] command [command options]\n\nCOMMANDS:\n"
    );
    for command in COMMANDS {
        out.push_str(&format!("   {:<26} {}\n", command.name, command.usage));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_arguments_selects_up() {
        assert_eq!(
            Selection::select(&[]),
            Selection::Command {
                name: "up".to_owned(),
                args: Vec::new()
            }
        );
    }

    #[test]
    fn help_and_unknown_fall_through() {
        for argv in [vec!["--help".to_owned()], vec!["help".to_owned()], vec!["bogus".to_owned()]] {
            assert_eq!(Selection::select(&argv), Selection::Help);
        }
    }

    #[test]
    fn known_command_keeps_its_arguments() {
        assert_eq!(
            Selection::select(&["up".to_owned(), "-db".to_owned(), "frontend".to_owned()]),
            Selection::Command {
                name: "up".to_owned(),
                args: vec!["-db".to_owned(), "frontend".to_owned()]
            }
        );
    }

    #[test]
    fn banner_matches_the_baseline_line() {
        assert_eq!(banner("5.0.0"), "Sourcegraph migrator 5.0.0");
    }
}
