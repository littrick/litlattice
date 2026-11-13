#![forbid(unsafe_code)]

//! Production Postgres adapter for
//! [`crate::codeintel_uploads_graphql::CodeIntelUploadsDatabase`] (M9.27d).
//!
//! Ports the read queries of the Go uploads store that back
//! `Query.preciseIndexes`:
//!
//! - `store.GetUploads` (`internal/codeintel/uploads/internal/store/uploads.go:29`)
//!   including the search/state/indexer conditions and the queue-rank join;
//! - `store.GetAutoIndexJobs` (`.../indexes.go:20`) with `WithoutUpload`;
//! - `store.GetUploadsByIDs` / `store.GetAutoIndexJobsByIDs` for the
//!   associated-record dataloader.
//!
//! The Go baseline appends `database.AuthzQueryConds`. The Rust port
//! reproduces the internal-actor form (`TRUE`) and therefore omits the
//! predicate; request-actor repository permissions are a documented gap in
//! `m9_27d_codeintel_uploads_overview.md`. No table, column or index is added.

use std::future::Future;

use sqlx::postgres::PgRow;
use sqlx::{PgPool, Postgres, Row};
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;

use crate::codeintel_uploads_graphql::{
    AutoIndexJobRow, CodeIntelUploadsDatabase, DockerStep, ExecutionLogEntry, GetAutoIndexJobsOptions,
    GetUploadsOptions, UploadRow,
};

pub(crate) fn block_on<F: Future>(future: F) -> F::Output {
    match tokio::runtime::Handle::try_current() {
        Ok(handle) => tokio::task::block_in_place(|| handle.block_on(future)),
        Err(_) => tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("codeintel uploads blocking runtime")
            .block_on(future),
    }
}

/// `uploadRankQueryFragment` (`store/util.go:15`).
const UPLOAD_RANK_FRAGMENT: &str = "
SELECT
	r.id,
	ROW_NUMBER() OVER (ORDER BY
		r.associated_index_id IS NULL DESC,
		COALESCE(r.process_after, r.uploaded_at),
		r.id
	) as rank
FROM lsif_uploads_with_repository_name r
WHERE r.state = 'queued'
";

/// `visibleAtTipSubselectQuery` (`store/util.go:28`).
const VISIBLE_AT_TIP_FRAGMENT: &str = "
SELECT 1
FROM lsif_uploads_visible_at_tip uvt
WHERE
	uvt.repository_id = u.repository_id AND
	uvt.upload_id = u.id AND
	uvt.is_default_branch
";

/// `indexRankQueryFragment` (`store/util.go:50`).
const INDEX_RANK_FRAGMENT: &str = "
SELECT
	r.id,
	ROW_NUMBER() OVER (ORDER BY COALESCE(r.process_after, r.queued_at), r.id) as rank
FROM lsif_indexes_with_repository_name r
WHERE r.state = 'queued'
";

#[derive(Clone)]
enum Param {
    Text(String),
    Int(i64),
    TextArray(Vec<String>),
}

fn add_param(params: &mut Vec<Param>, value: Param) -> usize {
    params.push(value);
    params.len()
}

/// `makeStateCondition`/`makeIndexStateCondition`: `errored` and `failed`
/// expand to both database states; a single state becomes `u.state = $n`,
/// several become `u.state = ANY($n)`.
fn state_condition(states: &[String], params: &mut Vec<Param>) -> Option<String> {
    if states.is_empty() {
        return None;
    }
    let mut expanded: Vec<String> = Vec::new();
    for state in states {
        if state == "errored" || state == "failed" {
            for value in ["errored", "failed"] {
                if !expanded.iter().any(|existing| existing == value) {
                    expanded.push(value.to_owned());
                }
            }
        } else if !expanded.iter().any(|existing| existing == state) {
            expanded.push(state.clone());
        }
    }
    expanded.sort();
    if expanded.len() == 1 {
        let index = add_param(params, Param::Text(expanded.remove(0)));
        Some(format!("u.state = ${index}"))
    } else {
        let index = add_param(params, Param::TextArray(expanded));
        Some(format!("u.state = ANY(${index})"))
    }
}

fn search_condition(columns: &[&str], term: &str, params: &mut Vec<Param>) -> String {
    let mut parts = Vec::new();
    for column in columns {
        let index = add_param(params, Param::Text(format!("%{term}%")));
        parts.push(format!("{column} ILIKE ${index}"));
    }
    format!("({})", parts.join(" OR "))
}

fn indexer_condition(names: &[String], params: &mut Vec<Param>) -> Option<String> {
    if names.is_empty() {
        return None;
    }
    let mut parts = Vec::new();
    for name in names {
        let index = add_param(params, Param::Text(format!("%{name}%")));
        parts.push(format!("u.indexer ILIKE ${index}"));
    }
    Some(format!("({})", parts.join(" OR ")))
}

fn bind_all<'q>(
    mut query: sqlx::query::Query<'q, Postgres, sqlx::postgres::PgArguments>,
    params: &'q [Param],
) -> sqlx::query::Query<'q, Postgres, sqlx::postgres::PgArguments> {
    for param in params {
        query = match param {
            Param::Text(value) => query.bind(value.clone()),
            Param::Int(value) => query.bind(*value),
            Param::TextArray(values) => query.bind(values.clone()),
        };
    }
    query
}

fn scan_upload(row: &PgRow) -> Result<UploadRow, sqlx::Error> {
    let rank: Option<i64> = row.try_get("rank")?;
    Ok(UploadRow {
        id: row.try_get("id")?,
        commit: row.try_get("commit")?,
        root: row.try_get("root")?,
        visible_at_tip: row.try_get("visible_at_tip")?,
        uploaded_at: row.try_get("uploaded_at")?,
        state: row.try_get("state")?,
        failure_message: row.try_get("failure_message")?,
        started_at: row.try_get("started_at")?,
        finished_at: row.try_get("finished_at")?,
        repository_id: row.try_get("repository_id")?,
        indexer: row.try_get("indexer")?,
        associated_index_id: row.try_get("associated_index_id")?,
        should_reindex: row.try_get("should_reindex")?,
        rank: rank.map(|value| value as i32),
    })
}

const UPLOAD_SELECT: &str = "\
SELECT
	u.id::bigint AS id,
	u.commit,
	u.root,
	EXISTS (VISIBLE_AT_TIP) AS visible_at_tip,
	u.uploaded_at,
	u.state,
	u.failure_message,
	u.started_at,
	u.finished_at,
	u.repository_id,
	u.indexer,
	u.associated_index_id,
	u.should_reindex,
	s.rank
FROM lsif_uploads u
LEFT JOIN (UPLOAD_RANK) s ON u.id = s.id
JOIN repo ON repo.id = u.repository_id";

fn upload_select() -> String {
    UPLOAD_SELECT
        .replace("VISIBLE_AT_TIP", VISIBLE_AT_TIP_FRAGMENT)
        .replace("UPLOAD_RANK", UPLOAD_RANK_FRAGMENT)
}

const INDEX_SELECT: &str = "\
SELECT
	u.id,
	u.commit,
	u.queued_at,
	u.state,
	u.failure_message,
	u.started_at,
	u.finished_at,
	u.repository_id,
	ARRAY(SELECT e::text FROM unnest(u.docker_steps) e) AS docker_steps,
	u.root,
	u.indexer,
	u.indexer_args,
	u.outfile,
	ARRAY(SELECT e::text FROM unnest(u.execution_logs) e) AS execution_logs,
	s.rank,
	u.local_steps,
	(SELECT MAX(id)::bigint FROM lsif_uploads WHERE associated_index_id = u.id) AS associated_upload_id,
	u.should_reindex,
	COALESCE(u.requested_envvars, ARRAY[]::text[]) AS requested_envvars
FROM lsif_indexes u
LEFT JOIN (INDEX_RANK) s ON u.id = s.id
JOIN repo ON repo.id = u.repository_id";

fn index_select() -> String {
    INDEX_SELECT.replace("INDEX_RANK", INDEX_RANK_FRAGMENT)
}

#[derive(serde::Deserialize)]
struct RawExecutionLogEntry {
    #[serde(default)]
    key: String,
    #[serde(default)]
    command: Vec<String>,
    #[serde(rename = "exitCode", default)]
    exit_code: Option<i32>,
    #[serde(rename = "startTime", default)]
    start_time: Option<String>,
    #[serde(rename = "durationMs", default)]
    duration_ms: Option<i32>,
}

fn parse_execution_logs(values: &[String]) -> Result<Vec<ExecutionLogEntry>, sqlx::Error> {
    values
        .iter()
        .map(|value| {
            let raw: RawExecutionLogEntry = serde_json::from_str(value)
                .map_err(|error| sqlx::Error::Decode(Box::new(error)))?;
            let start_time = match raw.start_time {
                Some(value) => OffsetDateTime::parse(&value, &Rfc3339)
                    .map_err(|error| sqlx::Error::Decode(Box::new(error)))?,
                // Go `time.Time{}` (year 1), the zero value used when the
                // JSON entry omits `startTime`.
                None => OffsetDateTime::from_unix_timestamp(-62_135_596_800)
                    .expect("Go zero time"),
            };
            Ok(ExecutionLogEntry {
                key: raw.key,
                command: raw.command,
                exit_code: raw.exit_code,
                start_time,
                duration_ms: raw.duration_ms,
            })
        })
        .collect()
}

#[derive(serde::Deserialize)]
struct RawDockerStep {
    #[serde(default)]
    root: String,
    #[serde(default)]
    image: String,
    #[serde(default)]
    commands: Vec<String>,
}

fn parse_docker_steps(values: &[String]) -> Result<Vec<DockerStep>, sqlx::Error> {
    values
        .iter()
        .map(|value| {
            let raw: RawDockerStep = serde_json::from_str(value)
                .map_err(|error| sqlx::Error::Decode(Box::new(error)))?;
            Ok(DockerStep {
                root: raw.root,
                image: raw.image,
                commands: raw.commands,
            })
        })
        .collect()
}

fn scan_index(row: &PgRow) -> Result<AutoIndexJobRow, sqlx::Error> {
    let rank: Option<i64> = row.try_get("rank")?;
    let docker_steps: Vec<String> = row.try_get("docker_steps")?;
    let execution_logs: Vec<String> = row.try_get("execution_logs")?;
    Ok(AutoIndexJobRow {
        id: row.try_get("id")?,
        commit: row.try_get("commit")?,
        queued_at: row.try_get("queued_at")?,
        state: row.try_get("state")?,
        failure_message: row.try_get("failure_message")?,
        started_at: row.try_get("started_at")?,
        finished_at: row.try_get("finished_at")?,
        repository_id: row.try_get("repository_id")?,
        docker_steps: parse_docker_steps(&docker_steps)?,
        root: row.try_get("root")?,
        indexer: row.try_get("indexer")?,
        indexer_args: row.try_get("indexer_args")?,
        outfile: row.try_get("outfile")?,
        execution_logs: parse_execution_logs(&execution_logs)?,
        rank: rank.map(|value| value as i32),
        local_steps: row.try_get("local_steps")?,
        associated_upload_id: row.try_get("associated_upload_id")?,
        should_reindex: row.try_get("should_reindex")?,
        requested_env_vars: row.try_get("requested_envvars")?,
    })
}

/// Production adapter over the shared `PgPool`.
pub struct PgCodeIntelUploadsStore {
    pool: PgPool,
}

impl PgCodeIntelUploadsStore {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

impl CodeIntelUploadsDatabase for PgCodeIntelUploadsStore {
    fn get_uploads(&self, opts: &GetUploadsOptions) -> Result<(Vec<UploadRow>, i64), String> {
        let mut conditions: Vec<String> = Vec::new();
        let mut params: Vec<Param> = Vec::new();
        if opts.repository_id != 0 {
            let index = add_param(&mut params, Param::Int(opts.repository_id as i64));
            conditions.push(format!("u.repository_id = ${index}"));
        }
        if !opts.term.is_empty() {
            conditions.push(search_condition(
                &[
                    "u.commit",
                    "u.root",
                    "(u.state)::text",
                    "u.failure_message",
                    "repo.name",
                    "u.indexer",
                    "u.indexer_version",
                ],
                &opts.term,
                &mut params,
            ));
        }
        if let Some(condition) = state_condition(&opts.states, &mut params) {
            conditions.push(condition);
        } else {
            // `buildGetConditionsAndCte`: exclude soft-deleted uploads when no
            // state filter and `includeDeleted` is unset.
            conditions.push("u.state != 'deleted'".to_owned());
        }
        if let Some(condition) = indexer_condition(&opts.indexer_names, &mut params) {
            conditions.push(condition);
        }
        let where_clause = if conditions.is_empty() {
            "TRUE".to_owned()
        } else {
            conditions.join(" AND ")
        };

        let limit = add_param(&mut params, Param::Int(opts.limit as i64));
        let offset = add_param(&mut params, Param::Int(opts.offset as i64));
        let select = upload_select();
        let select_sql = format!(
            "{select}\nWHERE {where_clause}\nORDER BY uploaded_at DESC, id\nLIMIT ${limit} OFFSET ${offset}"
        );
        let count_sql = format!(
            "SELECT COUNT(*) AS count FROM lsif_uploads u JOIN repo ON repo.id = u.repository_id WHERE {where_clause}"
        );
        let count_params = params[..params.len() - 2].to_vec();

        block_on(async {
            let rows = bind_all(sqlx::query(&select_sql), &params)
                .fetch_all(&self.pool)
                .await
                .map_err(|error| error.to_string())?;
            let uploads = rows
                .iter()
                .map(scan_upload)
                .collect::<Result<Vec<_>, _>>()
                .map_err(|error| error.to_string())?;
            let count_row = bind_all(sqlx::query(&count_sql), &count_params)
                .fetch_one(&self.pool)
                .await
                .map_err(|error| error.to_string())?;
            let total: i64 = count_row.try_get("count").map_err(|e| e.to_string())?;
            Ok((uploads, total))
        })
    }

    fn get_auto_index_jobs(
        &self,
        opts: &GetAutoIndexJobsOptions,
    ) -> Result<(Vec<AutoIndexJobRow>, i64), String> {
        let mut conditions: Vec<String> = Vec::new();
        let mut params: Vec<Param> = Vec::new();
        if opts.repository_id != 0 {
            let index = add_param(&mut params, Param::Int(opts.repository_id as i64));
            conditions.push(format!("u.repository_id = ${index}"));
        }
        if !opts.term.is_empty() {
            conditions.push(search_condition(
                &[
                    "u.commit",
                    "(u.state)::text",
                    "u.failure_message",
                    "repo.name",
                    "u.root",
                    "u.indexer",
                ],
                &opts.term,
                &mut params,
            ));
        }
        if let Some(condition) = state_condition(&opts.states, &mut params) {
            conditions.push(condition);
        }
        if opts.without_upload {
            conditions.push(
                "NOT EXISTS (SELECT 1 FROM lsif_uploads u2 WHERE u2.associated_index_id = u.id)"
                    .to_owned(),
            );
        }
        if let Some(condition) = indexer_condition(&opts.indexer_names, &mut params) {
            conditions.push(condition);
        }
        let where_clause = if conditions.is_empty() {
            "TRUE".to_owned()
        } else {
            conditions.join(" AND ")
        };

        let limit = add_param(&mut params, Param::Int(opts.limit as i64));
        let offset = add_param(&mut params, Param::Int(opts.offset as i64));
        let select = index_select();
        let select_sql = format!(
            "{select}\nWHERE repo.deleted_at IS NULL AND repo.blocked IS NULL AND {where_clause}\nORDER BY queued_at DESC, u.id\nLIMIT ${limit} OFFSET ${offset}"
        );
        let count_sql = format!(
            "SELECT COUNT(*) AS count FROM lsif_indexes u JOIN repo ON repo.id = u.repository_id WHERE repo.deleted_at IS NULL AND repo.blocked IS NULL AND {where_clause}"
        );
        let count_params = params[..params.len() - 2].to_vec();

        block_on(async {
            let rows = bind_all(sqlx::query(&select_sql), &params)
                .fetch_all(&self.pool)
                .await
                .map_err(|error| error.to_string())?;
            let jobs = rows
                .iter()
                .map(scan_index)
                .collect::<Result<Vec<_>, _>>()
                .map_err(|error| error.to_string())?;
            let count_row = bind_all(sqlx::query(&count_sql), &count_params)
                .fetch_one(&self.pool)
                .await
                .map_err(|error| error.to_string())?;
            let total: i64 = count_row.try_get("count").map_err(|e| e.to_string())?;
            Ok((jobs, total))
        })
    }

    fn get_uploads_by_ids(&self, ids: &[i64]) -> Result<Vec<UploadRow>, String> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        let placeholders: Vec<String> = (1..=ids.len()).map(|i| format!("${i}")).collect();
        let select = upload_select();
        let sql = format!(
            "{select}\nWHERE repo.deleted_at IS NULL AND u.id IN ({})\nORDER BY u.id",
            placeholders.join(", ")
        );
        let rows = block_on(async {
            let mut query = sqlx::query(&sql);
            for id in ids {
                query = query.bind(*id);
            }
            query.fetch_all(&self.pool).await
        })
        .map_err(|error| error.to_string())?;
        rows.iter()
            .map(scan_upload)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|error| error.to_string())
    }

    fn get_auto_index_jobs_by_ids(&self, ids: &[i64]) -> Result<Vec<AutoIndexJobRow>, String> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        let placeholders: Vec<String> = (1..=ids.len()).map(|i| format!("${i}")).collect();
        let select = index_select();
        let sql = format!(
            "{select}\nWHERE repo.deleted_at IS NULL AND u.id IN ({})\nORDER BY u.id",
            placeholders.join(", ")
        );
        let rows = block_on(async {
            let mut query = sqlx::query(&sql);
            for id in ids {
                query = query.bind(*id);
            }
            query.fetch_all(&self.pool).await
        })
        .map_err(|error| error.to_string())?;
        rows.iter()
            .map(scan_index)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|error| error.to_string())
    }
}
