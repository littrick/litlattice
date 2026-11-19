#![forbid(unsafe_code)]

//! Rust `migrator` process, drop-in for `cmd/migrator`.
//!
//! `up` / `upto` / `downto` / `validate` run locally on the Rust migration
//! engine (`sg-migrator`), reading the SQL corpus from `MIGRATIONS_DIR`
//! (default `/migrations`). The remaining commands (`describe`, `drift`,
//! `add-log`, `upgrade`, `downgrade`, `run-out-of-band-migrations`) are
//! delegated to `MIGRATOR_DELEGATE` (default `migrator-go`), which also keeps
//! the stitched multi-version and out-of-band migration support.
//!
//! Setting `MIGRATOR_DELEGATE` explicitly delegates **every** command,
//! preserving the pre-rewrite behavior for mixed deployments. Help is always
//! served locally (banner + command table), matching `urfave/cli`.

use std::path::PathBuf;

use sg_bin_migrator::{banner, help_text, Selection, DEV_VERSION};
use sg_migrator::cli::{self, MigrationOptions};
use sg_migrator::dsn::dsn_for_schema;
use sg_migrator::runner::{PrivilegedMode, RunOptions, Runner, SchemaStore};
use sg_migrator::schemas;
use sg_migrator::store::Store;
use sg_migrator::MigratorError;

/// Environment variable naming the migration-runner executable.
pub const DELEGATE_ENV: &str = "MIGRATOR_DELEGATE";

/// Default delegate: the Go migrator kept for the migration commands the Rust
/// engine does not implement yet.
pub const DEFAULT_DELEGATE: &str = "migrator-go";

/// Directory holding `<schema>/<version>_<slug>/{up,down}.sql`.
pub const MIGRATIONS_DIR_ENV: &str = "MIGRATIONS_DIR";

/// Commands implemented by this binary.
const LOCAL_COMMANDS: &[&str] = &["up", "upto", "downto", "validate"];

fn main() {
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
            if let Ok(delegate) = std::env::var(DELEGATE_ENV) {
                if !delegate.is_empty() {
                    run_delegate(&delegate, &name, &command_args);
                }
            }
            if LOCAL_COMMANDS.contains(&name.as_str()) {
                println!("{}", banner(DEV_VERSION));
                let runtime = tokio::runtime::Builder::new_multi_thread()
                    .enable_all()
                    .build()
                    .expect("build tokio runtime");
                match runtime.block_on(run_local(&name, &command_args)) {
                    Ok(()) => {}
                    Err(error) => {
                        eprintln!("migrator: {error}");
                        std::process::exit(1);
                    }
                }
            } else {
                run_delegate(DEFAULT_DELEGATE, &name, &command_args);
            }
        }
    }
}

async fn run_local(command: &str, args: &[String]) -> Result<(), MigratorError> {
    let (schema_names, target, options) = match command {
        "up" => {
            let options = cli::parse_options(args)?;
            (split_schemas(&options), None, options)
        }
        "upto" => {
            let options = cli::parse_options(args)?;
            let target = options.target.ok_or_else(|| {
                MigratorError::new("the -target flag is required for upto")
            })?;
            (split_schemas(&options), Some(target), options)
        }
        "downto" => {
            let options = cli::parse_options(args)?;
            let target = options.target.ok_or_else(|| {
                MigratorError::new("the -target flag is required for downto")
            })?;
            (split_schemas(&options), Some(target), options)
        }
        "validate" => {
            let options = cli::parse_validate_options(args)?;
            (
                split_schema_option(&options.schema),
                None,
                MigrationOptions::default(),
            )
        }
        other => {
            return Err(MigratorError::new(format!(
                "command {other:?} is not implemented by the Rust migrator"
            )))
        }
    };

    let root = migrations_dir();
    let selected = resolve_schema_names(&schema_names)?;
    let runner = build_runner(&root, &selected).await?;

    let run_options = RunOptions {
        privileged_mode: if options.unprivileged_only {
            PrivilegedMode::Refuse
        } else if options.noop_privileged {
            PrivilegedMode::Noop
        } else {
            PrivilegedMode::Apply
        },
        privileged_hash: (!options.privileged_hash.is_empty())
            .then(|| options.privileged_hash.clone()),
        ignore_single_dirty_log: options.ignore_single_dirty_log,
        ignore_single_pending_log: options.ignore_single_pending_log,
    };

    match command {
        "up" => runner.up(&selected, None, &run_options).await,
        "upto" => runner.up(&selected, target, &run_options).await,
        "downto" => {
            let target = target.expect("downto target checked above");
            runner.down_to(&selected, target, &run_options).await
        }
        "validate" => runner.validate(&selected).await,
        _ => unreachable!(),
    }
}

fn migrations_dir() -> PathBuf {
    std::env::var(MIGRATIONS_DIR_ENV)
        .ok()
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("/migrations"))
}

fn split_schemas(options: &MigrationOptions) -> Vec<String> {
    split_schema_option(&options.schema)
}

fn split_schema_option(schema: &Option<String>) -> Vec<String> {
    schema
        .as_deref()
        .unwrap_or("")
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
        .collect()
}

fn resolve_schema_names(names: &[String]) -> Result<Vec<String>, MigratorError> {
    if names.is_empty() || names.iter().any(|name| name == "all") {
        return Ok(schemas::SCHEMA_NAMES.iter().map(|name| (*name).to_owned()).collect());
    }
    for name in names {
        schemas::validate_schema_name(name)?;
    }
    let mut unique: Vec<String> = Vec::new();
    for name in names {
        if !unique.contains(name) {
            unique.push(name.clone());
        }
    }
    Ok(unique)
}

async fn build_runner(root: &std::path::Path, names: &[String]) -> Result<Runner, MigratorError> {
    let mut stores = Vec::with_capacity(names.len());
    for name in names {
        let schema = schemas::resolve_schema_in(root, name)?;
        let dsn = dsn_for_schema(name);
        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(4)
            .connect(&dsn)
            .await
            .map_err(|error| {
                MigratorError::new(format!("failed to connect to the {name} database: {error}"))
            })?;
        let store = Store::new(
            pool,
            schema.name.clone(),
            schema.migrations_table_name.clone(),
        );
        stores.push(SchemaStore { schema, store });
    }
    Ok(Runner::new(stores))
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
        eprintln!(
            "migrator: command {command:?} is not implemented by the Rust migrator and could not execute delegate {delegate:?}: {error}"
        );
        std::process::exit(1);
    }

    #[cfg(not(unix))]
    {
        match process.status() {
            Ok(status) => std::process::exit(status.code().unwrap_or(1)),
            Err(error) => {
                eprintln!(
                    "migrator: command {command:?} is not implemented by the Rust migrator and could not execute delegate {delegate:?}: {error}"
                );
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

    #[test]
    fn schema_lists_are_split_and_deduplicated() {
        assert_eq!(
            resolve_schema_names(&["all".to_owned()]).expect("all"),
            vec!["frontend", "codeintel", "codeinsights"]
        );
        assert_eq!(
            resolve_schema_names(&["frontend".to_owned(), "frontend".to_owned()])
                .expect("frontend"),
            vec!["frontend"]
        );
        assert!(resolve_schema_names(&["bogus".to_owned()]).is_err());
    }

    #[test]
    fn schema_options_split_on_commas() {
        assert_eq!(
            split_schema_option(&Some("frontend, codeintel".to_owned())),
            vec!["frontend", "codeintel"]
        );
        assert!(split_schema_option(&None).is_empty());
    }
}
