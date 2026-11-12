#![forbid(unsafe_code)]

//! Rust `migrator` process, drop-in for `cmd/migrator`.
//!
//! Baseline `cmd/migrator/main.go` calls `sanitycheck.Pass()`, initialises the
//! logger with `env.MyName`/`version.Version()`, then `shared.Start` prints
//! `Sourcegraph migrator <version>` and runs the `urfave/cli` app.
//!
//! This binary owns that startup surface (banner, CLI selection, help) and
//! forwards database commands to the delegate named by `MIGRATOR_DELEGATE`
//! (default `migrator-go`). Migrations and migration files stay with the Go
//! migrator: the Rust port does not introduce a second migration lineage.
//!
//! The delegate replaces this process (`exec`), so its stdout/stderr and exit
//! code are observed unchanged; when delegating we do not print the banner
//! ourselves because the delegate prints the same line. Help (`--help`,
//! `help`, unknown command, no subcommand help) is served locally and prints
//! the banner first, exactly like the baseline.

use sg_bin_migrator::{banner, help_text, Selection, DEV_VERSION};

/// Environment variable naming the migration-runner executable.
pub const DELEGATE_ENV: &str = "MIGRATOR_DELEGATE";

/// Default delegate: the Go migrator kept for the migration convention.
pub const DEFAULT_DELEGATE: &str = "migrator-go";

fn main() {
    // Baseline `cmd/migrator/main.go`: `sanitycheck.Pass()`.
    sg_core::sanity_check();
    sg_log::init("migrator");

    let args: Vec<String> = std::env::args().skip(1).collect();
    match Selection::select(&args) {
        Selection::Help => {
            println!("{}", banner(DEV_VERSION));
            print!("{}", help_text());
        }
        Selection::Command {
            name,
            args: command_args,
        } => {
            let delegate =
                std::env::var(DELEGATE_ENV).unwrap_or_else(|_| DEFAULT_DELEGATE.to_owned());
            run_delegate(&delegate, &name, &command_args);
        }
    }
}

/// Replace the current process with the delegate, forwarding the selected
/// command and its arguments. `exec` preserves the delegate's exit code and
/// signal disposition (matching `urfave/cli`'s direct exit).
fn run_delegate(delegate: &str, command: &str, args: &[String]) -> ! {
    let mut process = std::process::Command::new(delegate);
    process.arg(command).args(args);

    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt;
        let error = process.exec();
        eprintln!("migrator: could not execute delegate {delegate:?}: {error}");
        std::process::exit(1);
    }

    #[cfg(not(unix))]
    {
        match process.status() {
            Ok(status) => std::process::exit(status.code().unwrap_or(1)),
            Err(error) => {
                eprintln!("migrator: could not execute delegate {delegate:?}: {error}");
                std::process::exit(1);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn delegate_default_is_the_go_migrator() {
        assert_eq!(DEFAULT_DELEGATE, "migrator-go");
        assert_eq!(DELEGATE_ENV, "MIGRATOR_DELEGATE");
    }
}
