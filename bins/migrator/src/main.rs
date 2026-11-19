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
const LOCAL_COMMANDS: &[&str] = &["up", "upto", "downto", "validate", "add-log", "describe", "drift", "upgrade", "downgrade", "run-out-of-band-migrations"];

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
    if command == "upgrade" || command == "downgrade" {
        return run_upgrade_command(command == "upgrade", args).await;
    }
    if command == "run-out-of-band-migrations" {
        let options = cli::parse_run_out_of_band_options(args)?;
        let frontend_pool = connect_pool("frontend").await?;
        let codeintel_pool = connect_pool("codeintel").await?;
        let insights_pool = if sg_migrator::oobmigration::insights::insights_enabled() {
            Some(connect_pool("codeinsights").await?)
        } else {
            None
        };
        return sg_migrator::oobmigration::run_out_of_band_migrations_with_pools(
            &frontend_pool,
            Some(&codeintel_pool),
            insights_pool.as_ref(),
            &sg_migrator::oobmigration::RunOutOfBandOptions {
                ids: options.id,
                apply_reverse: options.apply_reverse,
                dry_run: false,
                disable_animation: options.disable_animation,
            },
        )
        .await;
    }
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
        "drift" => {
            let options = cli::parse_drift_options(args)?;
            let schema_name = options
                .schema
                .ok_or_else(|| MigratorError::new("the -schema flag is required for drift"))?;
            if options.version.is_some() && options.file.is_some() {
                return Err(MigratorError::new(
                    "the flags -version or -file are mutually exclusive",
                ));
            }
            let version = match (&options.version, &options.file) {
                (Some(version), _) => version.clone(),
                (None, Some(_)) => String::new(),
                (None, None) => {
                    if options.skip_version_check {
                        return Err(MigratorError::new(
                            "-skip-version-check was supplied without -version or -file",
                        ));
                    }
                    // No instance-version inference in the Rust port yet: the
                    // bundled expected schema ships with this build.
                    format!(
                        "v{}",
                        std::env::var("SOURCEGRAPH_VERSION").unwrap_or_else(|_| "5.5.0".to_owned())
                    )
                }
            };
            let root = migrations_dir();
            let schema = schemas::resolve_schema_in(&root, &schema_name)?;
            let dsn = dsn_for_schema(&schema_name);
            let pool = sqlx::postgres::PgPoolOptions::new()
                .max_connections(4)
                .connect(&dsn)
                .await
                .map_err(|error| {
                    MigratorError::new(format!(
                        "failed to connect to the {schema_name} database: {error}"
                    ))
                })?;
            let store = Store::new(
                pool.clone(),
                schema.name.clone(),
                schema.migrations_table_name.clone(),
            );
            let expected = sg_migrator::drift::fetch_expected_schema(
                &schema_name,
                &version,
                options.file.as_deref(),
                &root,
            )
            .await?;
            let mut actual = sg_migrator::describe::describe(&store).await?;
            let mut expected = expected;
            sg_migrator::drift::canonicalize(&mut actual);
            sg_migrator::drift::canonicalize(&mut expected);
            let mut summaries = sg_migrator::drift::compare_schema_descriptions(
                &schema_name,
                &version,
                &actual,
                &expected,
            );

            if options.auto_fix {
                // `attemptAutofix` (`cliutil/drift_autofix.go`): apply the
                // suggested statements up to three times, re-describing after
                // each attempt. Statement failures are reported, not fatal.
                let mut attempts = 3;
                while attempts > 0 && !summaries.is_empty() {
                    let mut attempted = false;
                    let mut successes = 0usize;
                    let mut failures = 0usize;
                    for summary in &summaries {
                        let Some(statements) = &summary.statements else {
                            continue;
                        };
                        attempted = true;
                        let mut failed = false;
                        for statement in statements {
                            if let Err(error) = sqlx::raw_sql(statement).execute(&pool).await {
                                eprintln!(
                                    "failed to apply autofix {:?}: {error}",
                                    statements.join("\n")
                                );
                                failed = true;
                            }
                        }
                        if failed {
                            failures += 1;
                        } else {
                            successes += 1;
                        }
                    }
                    if !attempted {
                        println!("No autofix to apply");
                        break;
                    }
                    println!("Successfully applied {successes} autofixes");
                    if failures > 0 {
                        eprintln!("Failed to apply {failures} autofixes");
                    }
                    println!("Re-checking drift");
                    actual = sg_migrator::describe::describe(&store).await?;
                    sg_migrator::drift::canonicalize(&mut actual);
                    summaries = sg_migrator::drift::compare_schema_descriptions(
                        &schema_name,
                        &version,
                        &actual,
                        &expected,
                    );
                    attempts -= 1;
                }
            }

            print!("{}", sg_migrator::drift::display_schema_summaries(&summaries));
            if !summaries.is_empty() {
                std::process::exit(1);
            }
            return Ok(());
        }
        "describe" => {
            let options = cli::parse_describe_options(args)?;
            let schema_name = options
                .schema
                .ok_or_else(|| MigratorError::new("the -schema flag is required for describe"))?;
            let format = options
                .format
                .ok_or_else(|| MigratorError::new("the -format flag is required for describe"))?;
            if !matches!(format.as_str(), "psql" | "json") {
                return Err(MigratorError::new(format!(
                    "unrecognized format {format:?} (must be json or psql)"
                )));
            }
            let root = migrations_dir();
            let schema = schemas::resolve_schema_in(&root, &schema_name)?;
            let dsn = dsn_for_schema(&schema_name);
            let pool = sqlx::postgres::PgPoolOptions::new()
                .max_connections(4)
                .connect(&dsn)
                .await
                .map_err(|error| {
                    MigratorError::new(format!(
                        "failed to connect to the {schema_name} database: {error}"
                    ))
                })?;
            let store = Store::new(
                pool,
                schema.name.clone(),
                schema.migrations_table_name.clone(),
            );
            let description = sg_migrator::describe::describe(&store).await?;
            let text = if format == "psql" {
                sg_migrator::describe::format_psql(&description)
            } else {
                sg_migrator::describe::format_json(&description)?
            };
            if let Some(path) = options.out {
                if !options.force && std::path::Path::new(&path).exists() {
                    return Err(MigratorError::new(format!("file {path:?} already exists")));
                }
                std::fs::write(&path, text).map_err(|error| {
                    MigratorError::new(format!("failed to write {path:?}: {error}"))
                })?;
            } else {
                print!("{text}");
            }
            return Ok(());
        }
        "add-log" => {
            let options = cli::parse_add_log_options(args)?;
            let version = options.version.ok_or_else(|| {
                MigratorError::new("the -version flag is required for add-log")
            })?;
            if options.schema.is_none() {
                return Err(MigratorError::new("the -schema flag is required for add-log"));
            }
            let schema_names = split_schema_option(&options.schema);
            let runner = build_runner(&migrations_dir(), &resolve_schema_names(&schema_names)?).await?;
            return runner.add_log(&schema_names, version, options.up).await;
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
        privileged_hashes: Vec::new(),
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

async fn connect_pool(schema: &str) -> Result<sqlx::PgPool, MigratorError> {
    let dsn = dsn_for_schema(schema);
    sqlx::postgres::PgPoolOptions::new()
        .max_connections(4)
        .connect(&dsn)
        .await
        .map_err(|error| {
            MigratorError::new(format!("failed to connect to the {schema} database: {error}"))
        })
}

/// `upgrade` / `downgrade` (`cliutil/upgrade.go`, `cliutil/downgrade.go`).
async fn run_upgrade_command(up: bool, args: &[String]) -> Result<(), MigratorError> {
    use sg_migrator::oobmigration::version::VersionOrder;

    let options = if up {
        cli::parse_upgrade_options(args)?
    } else {
        cli::parse_downgrade_options(args)?
    };
    let root = migrations_dir();
    let frontend_pool = connect_pool("frontend").await?;
    let (current, auto_upgrade) =
        sg_migrator::service_version::get_auto_upgrade(&frontend_pool).await?;

    let (from_str, to_str) = match (&options.from, &options.to) {
        (Some(from), Some(to)) => (from.clone(), to.clone()),
        (None, None) if auto_upgrade => {
            let current = current.ok_or_else(|| {
                MigratorError::new("version assertion failed: unknown current version")
            })?;
            let to = format!(
                "v{}",
                std::env::var("SOURCEGRAPH_VERSION").unwrap_or_else(|_| "5.5.0".to_owned())
            );
            (current, to)
        }
        _ => {
            return Err(MigratorError::new(
                "the -from and -to flags are required when auto upgrade is not enabled",
            ))
        }
    };

    let from = sg_migrator::oobmigration::version::Version::from_string(&from_str)
        .ok_or_else(|| MigratorError::new(format!("bad format for -from = {from_str}")))?;
    let to = sg_migrator::oobmigration::version::Version::from_string(&to_str)
        .ok_or_else(|| MigratorError::new(format!("bad format for -to = {to_str}")))?;
    let order = sg_migrator::oobmigration::version::compare_versions(from, to);
    if up && order != VersionOrder::Before {
        return Err(MigratorError::new(format!(
            "invalid range (from={from} >= to={to})"
        )));
    }
    if !up && order != VersionOrder::After {
        return Err(MigratorError::new(format!(
            "invalid range (from={from} <= to={to})"
        )));
    }

    let plan = sg_migrator::multiversion::plan_migration(from, to, &root)?;
    for (index, step) in plan.steps.iter().enumerate() {
        println!(
            "Migrating to v{} (step {} of {})",
            step.instance_version,
            index + 1,
            plan.steps.len()
        );
    }

    let mut stores = Vec::with_capacity(sg_migrator::schemas::SCHEMA_NAMES.len());
    for name in sg_migrator::schemas::SCHEMA_NAMES {
        let definitions = plan
            .stitched_definitions_by_schema_name
            .get(name)
            .cloned()
            .ok_or_else(|| {
                MigratorError::new(format!("no stitched definitions for schema {name:?}"))
            })?;
        let pool = connect_pool(name).await?;
        let schema = sg_migrator::types::Schema::new(
            name,
            sg_migrator::schemas::migrations_table_name(name),
            definitions,
        );
        let store = Store::new(
            pool,
            schema.name.clone(),
            schema.migrations_table_name.clone(),
        );
        stores.push(SchemaStore { schema, store });
    }
    let runner = Runner::new(stores);

    let run_options = sg_migrator::multiversion::RunMigrationOptions {
        privileged_mode: if options.unprivileged_only {
            PrivilegedMode::Refuse
        } else if options.noop_privileged {
            PrivilegedMode::Noop
        } else {
            PrivilegedMode::Apply
        },
        privileged_hashes: options.privileged_hash.clone(),
        skip_version_check: options.skip_version_check,
        skip_drift_check: options.skip_drift_check,
        dry_run: options.dry_run,
        up,
        disable_animation: options.disable_animation,
        root,
    };
    sg_migrator::multiversion::run_migration(&runner, &frontend_pool, &plan, &run_options).await
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
