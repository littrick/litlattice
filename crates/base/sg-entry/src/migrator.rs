#![forbid(unsafe_code)]

//! The `migrator up -db <schema>` convention (`cmd/server/shared/shared.go`).
//!
//! The baseline does not migrate in-process: it shells out to the `migrator`
//! binary for `frontend`, `codeintel` and (when single-container Code Insights
//! is allowed) `codeinsights`. The same convention is kept here; tests inject
//! a recording [`Migrator`] so no database is touched.

/// The schemas `runMigrator` applies, in order.
pub fn migration_schemas(allow_code_insights: bool) -> Vec<String> {
    let mut schemas = vec!["frontend".to_owned(), "codeintel".to_owned()];
    if allow_code_insights {
        schemas.push("codeinsights".to_owned());
    }
    schemas
}

pub trait Migrator {
    /// Run `migrator up -db <schema>`. `env` is the final environment after
    /// `SetDefaultEnv`, mirroring the baseline where the migrator inherits the
    /// mutated process environment.
    fn up(&mut self, schema: &str, env: &[(String, String)]) -> Result<(), String>;
}

/// Runs the Go `migrator` binary found in `PATH` (or `MIGRATOR_PATH`).
pub struct CommandMigrator {
    program: String,
}

impl CommandMigrator {
    pub fn new() -> Self {
        Self {
            program: std::env::var("MIGRATOR_PATH").unwrap_or_else(|_| "migrator".to_owned()),
        }
    }

    pub fn with_program(program: impl Into<String>) -> Self {
        Self {
            program: program.into(),
        }
    }
}

impl Default for CommandMigrator {
    fn default() -> Self {
        Self::new()
    }
}

impl Migrator for CommandMigrator {
    fn up(&mut self, schema: &str, env: &[(String, String)]) -> Result<(), String> {
        let status = std::process::Command::new(&self.program)
            .arg("up")
            .arg("-db")
            .arg(schema)
            .envs(env.iter().map(|(key, value)| (key, value)))
            .status()
            .map_err(|error| format!("failed to run {}: {error}", self.program))?;
        if status.success() {
            Ok(())
        } else {
            Err(format!(
                "{} up -db {schema} exited with {status}",
                self.program
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn schema_order() {
        assert_eq!(migration_schemas(false), vec!["frontend", "codeintel"]);
        assert_eq!(
            migration_schemas(true),
            vec!["frontend", "codeintel", "codeinsights"]
        );
    }
}
