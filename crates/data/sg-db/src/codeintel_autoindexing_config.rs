#![forbid(unsafe_code)]

//! The configuration half of `internal/codeintel/autoindexing/internal/store`
//! (`config_repo.go` + `config_inference.go`), used by the autoindexing
//! job selector and GraphQL configuration API.
//!
//! Ported 1:1:
//!
//! * [`repository_exceptions`] / [`set_repository_exceptions`] —
//!   `RepositoryExceptions` / `SetRepositoryExceptions`
//!   (`codeintel_autoindexing_exceptions`);
//! * [`get_index_configuration_by_repository_id`] /
//!   [`update_index_configuration_by_repository_id`] —
//!   `GetIndexConfigurationByRepositoryID` /
//!   `UpdateIndexConfigurationByRepositoryID` (`lsif_index_configuration`);
//! * [`get_inference_script`] — `GetInferenceScript`
//!   (`codeintel_inference_scripts`, falling back to the embedded default
//!   Lua script, trimmed and terminated with one newline).
//!
//! Not ported: `SetInferenceScript` (it validates the script with the Go
//! `gopher-lua` parser; the only caller is the unported GraphQL mutation).
//!
//! Representation notes: `shared.IndexConfiguration.Data []byte` becomes
//! `Vec<u8>` (the column is `bytea`); a missing row is `None`. Observation
//! spans have no observable effect and are omitted. Migrations stay owned by
//! the Go `migrator`.

use sqlx::postgres::PgPool;
use sqlx::Row;

/// `shared.IndexConfiguration`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexConfiguration {
    pub id: i64,
    pub repository_id: i32,
    pub data: Vec<u8>,
}

const REPOSITORY_EXCEPTIONS_SQL: &str = "
SELECT
	cae.disable_scheduling,
	cae.disable_inference
FROM codeintel_autoindexing_exceptions cae
WHERE cae.repository_id = $1
";

/// `store.RepositoryExceptions`. No row means both are enabled.
pub async fn repository_exceptions(
    pool: &PgPool,
    repository_id: i32,
) -> Result<(bool, bool), sqlx::Error> {
    let row = sqlx::query(REPOSITORY_EXCEPTIONS_SQL)
        .bind(repository_id)
        .fetch_optional(pool)
        .await?;
    match row {
        Some(row) => {
            let disable_scheduling: bool = row.try_get(0)?;
            let disable_inference: bool = row.try_get(1)?;
            Ok((!disable_scheduling, !disable_inference))
        }
        None => Ok((true, true)),
    }
}

const SET_REPOSITORY_EXCEPTIONS_SQL: &str = "
INSERT INTO codeintel_autoindexing_exceptions (repository_id, disable_scheduling, disable_inference)
VALUES ($1, $2, $3)
ON CONFLICT (repository_id) DO UPDATE SET
	disable_scheduling = $4,
	disable_inference = $5
";

/// `store.SetRepositoryExceptions`.
pub async fn set_repository_exceptions(
    pool: &PgPool,
    repository_id: i32,
    can_schedule: bool,
    can_infer: bool,
) -> Result<(), sqlx::Error> {
    sqlx::query(SET_REPOSITORY_EXCEPTIONS_SQL)
        .bind(repository_id)
        .bind(!can_schedule)
        .bind(!can_infer)
        .bind(!can_schedule)
        .bind(!can_infer)
        .execute(pool)
        .await?;
    Ok(())
}

const GET_INDEX_CONFIGURATION_SQL: &str = "
SELECT
	c.id,
	c.repository_id,
	c.data
FROM lsif_index_configuration c
WHERE c.repository_id = $1
";

/// `store.GetIndexConfigurationByRepositoryID`.
pub async fn get_index_configuration_by_repository_id(
    pool: &PgPool,
    repository_id: i32,
) -> Result<Option<IndexConfiguration>, sqlx::Error> {
    let row = sqlx::query(GET_INDEX_CONFIGURATION_SQL)
        .bind(repository_id)
        .fetch_optional(pool)
        .await?;
    match row {
        Some(row) => Ok(Some(IndexConfiguration {
            id: row.try_get("id")?,
            repository_id: row.try_get("repository_id")?,
            data: row.try_get("data")?,
        })),
        None => Ok(None),
    }
}

const UPDATE_INDEX_CONFIGURATION_SQL: &str = "
INSERT INTO lsif_index_configuration (repository_id, data)
VALUES ($1, $2)
ON CONFLICT (repository_id) DO UPDATE
SET data = $3
";

/// `store.UpdateIndexConfigurationByRepositoryID`.
pub async fn update_index_configuration_by_repository_id(
    pool: &PgPool,
    repository_id: i32,
    data: &[u8],
) -> Result<(), sqlx::Error> {
    sqlx::query(UPDATE_INDEX_CONFIGURATION_SQL)
        .bind(repository_id)
        .bind(data)
        .bind(data)
        .execute(pool)
        .await?;
    Ok(())
}

const GET_INFERENCE_SCRIPT_SQL: &str = "
SELECT script
FROM codeintel_inference_scripts
ORDER BY insert_timestamp DESC
LIMIT 1
";

/// `store.GetInferenceScript`.
pub async fn get_inference_script(pool: &PgPool) -> Result<String, sqlx::Error> {
    let row = sqlx::query(GET_INFERENCE_SCRIPT_SQL)
        .fetch_optional(pool)
        .await?;
    let script: Option<String> = match row {
        Some(row) => row.try_get(0)?,
        None => None,
    };
    let script = script.unwrap_or_default();
    if script.is_empty() {
        Ok(format!("{}\n", DEFAULT_SCRIPT.trim()))
    } else {
        Ok(script)
    }
}

/// `store.defaultScript`.
const DEFAULT_SCRIPT: &str = r#"
local path = require("path")
local pattern = require("sg.autoindex.patterns")
local recognizer = require("sg.autoindex.recognizer")

local custom_recognizer = recognizer.new_path_recognizer {
	patterns = {
		pattern.new_path_basename("acme-custom.yaml")
	},

	-- Invoked with paths matching acme-custom.yaml anywhere in repo
	generate = function(_, paths)
		local jobs = {}
		for i = 1, #paths do
			table.insert(jobs, {
				indexer = "acme/acme-indexer",
				root = path.dirname(paths[i]),
				-- Run a dependency installation step before invoking the indexer
				local_steps = {"acme-package-manager install"},
				indexer_args = {"acme-indexer", "index", ".", "--output", "index.scip"},
				outfile = "index.scip",
			})
		end

		return jobs
	end,
}

return require("sg.autoindex.config").new({
	-- Uncomment one or more lines to turn off default auto-indexing scripts
	-- ["sg.go"] = false,
	-- ["sg.java"] = false,
	-- ["sg.python"] = false,
	-- ["sg.ruby"] = false,
	-- ["sg.rust"] = false,
	-- ["sg.typescript"] = false,
	-- ["sg.dotnet"] = false,
	["acme.custom"] = custom_recognizer,
})
"#;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_script_matches_the_baseline_wrapping() {
        // `GetInferenceScript` falls back to `defaultScript`, trimmed and
        // terminated with exactly one newline (Go: `strings.TrimSpace` +
        // `"\n"`). The embedded script itself starts with a newline.
        let expected = format!("{}\n", DEFAULT_SCRIPT.trim());
        assert!(DEFAULT_SCRIPT.starts_with('\n'));
        assert!(expected.starts_with("local path = require(\"path\")"));
        assert!(expected.ends_with("})\n"));
        assert!(!expected.ends_with("\n\n"));
    }

    #[test]
    fn default_script_keeps_the_recognizer_table() {
        assert!(DEFAULT_SCRIPT.contains("[\"acme.custom\"] = custom_recognizer"));
        assert!(DEFAULT_SCRIPT.contains("[\"sg.rust\"] = false"));
    }
}
