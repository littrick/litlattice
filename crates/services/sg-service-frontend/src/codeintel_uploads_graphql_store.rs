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
use std::sync::Arc;

use sqlx::postgres::PgRow;
use sqlx::{PgPool, Postgres, Row};
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;

use sg_db::codeintel_policies::{self, ConfigurationPolicy, GetConfigurationPoliciesOptions};
use sg_git::RefType;

use crate::codeintel_uploads_graphql::{
    AuditLogRow, AutoIndexJobRow, CodeIntelUploadsDatabase, DeleteAutoIndexJobsOptions,
    DeleteUploadsOptions, DockerStep, ExecutionLogEntry, GetAutoIndexJobsOptions,
    GetUploadsOptions, RecentActivityRows, ReindexUploadsOptions,
    RepositoryWithConfigurationRow, RepositoryWithCountRow, RetentionPolicyMatchRow,
    SetRerunAutoIndexJobsOptions, UploadRow,
};
use crate::git_graphql::GitObjectSource;

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
    /// M9.30d: the `projectRoot`/`tags`/retention-matcher gitserver seam. The
    /// production server injects [`crate::git_graphql::GitserverGitObjects`];
    /// only the differential harness injects a fixture-backed fake.
    git: Option<Arc<dyn GitObjectSource>>,
    /// `conf.CodeIntelAutoIndexingEnabled`.
    auto_indexing_enabled: bool,
}

impl PgCodeIntelUploadsStore {
    pub fn new(pool: PgPool) -> Self {
        Self {
            pool,
            git: None,
            auto_indexing_enabled: false,
        }
    }

    /// Inject the gitserver reads (`ResolveRevision`, `ListRefs`).
    pub fn with_git(mut self, git: Arc<dyn GitObjectSource>) -> Self {
        self.git = Some(git);
        self
    }

    /// `conf.CodeIntelAutoIndexingEnabled` injection.
    pub fn with_auto_indexing_enabled(mut self, enabled: bool) -> Self {
        self.auto_indexing_enabled = enabled;
        self
    }
}

impl CodeIntelUploadsDatabase for PgCodeIntelUploadsStore {
    fn get_uploads(&self, opts: &GetUploadsOptions) -> Result<(Vec<UploadRow>, i64), String> {
        let mut conditions: Vec<String> = Vec::new();
        let mut params: Vec<Param> = Vec::new();
        let mut ctes: Vec<String> = Vec::new();
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
        // `buildGetConditionsAndCte`: dependency filters rank the providers of
        // the referenced packages and keep the canonical (rank 1) providers.
        if opts.dependency_of != 0 {
            let index = add_param(&mut params, Param::Int(opts.dependency_of));
            ctes.push(sg_db::codeintel_uploads::dependency_cte(
                sg_db::codeintel_uploads::RANKED_DEPENDENCIES_CTE_TEMPLATE,
                &format!("${index}"),
            ));
            conditions.push(
                "u.id IN (SELECT rd.pkg_id FROM ranked_dependencies rd WHERE rd.rank = 1)"
                    .to_owned(),
            );
        }
        if opts.dependent_of != 0 {
            let index = add_param(&mut params, Param::Int(opts.dependent_of));
            ctes.push(sg_db::codeintel_uploads::dependency_cte(
                sg_db::codeintel_uploads::RANKED_DEPENDENTS_CTE_TEMPLATE,
                &format!("${index}"),
            ));
            conditions.push(format!(
                "u.id IN (SELECT r.dump_id FROM ranked_dependents rd \
                 JOIN lsif_references r ON r.scheme = rd.scheme AND r.manager = rd.manager \
                   AND r.name = rd.name AND r.version = rd.version AND r.dump_id != rd.pkg_id \
                 WHERE rd.pkg_id = ${index} AND rd.rank = 1)"
            ));
        }
        // `allowDeletedUploads := AllowDeletedUpload && (State == "" || State == "deleted")`.
        let allow_deleted_uploads = opts.allow_deleted_upload;
        if allow_deleted_uploads {
            ctes.push(sg_db::codeintel_uploads::DELETED_UPLOADS_CTE.to_owned());
        }
        if let Some(condition) = state_condition(&opts.states, &mut params) {
            conditions.push(condition);
        } else if !allow_deleted_uploads {
            conditions.push("u.state != 'deleted'".to_owned());
        }
        if let Some(condition) = indexer_condition(&opts.indexer_names, &mut params) {
            conditions.push(condition);
        }
        conditions.push("NOT u.expired".to_owned());
        conditions.push("repo.deleted_at IS NULL".to_owned());
        conditions.push("repo.blocked IS NULL".to_owned());
        let where_clause = if conditions.is_empty() {
            "TRUE".to_owned()
        } else {
            conditions.join(" AND ")
        };
        let source_table = if allow_deleted_uploads {
            sg_db::codeintel_uploads::DELETED_UPLOADS_SOURCE
        } else {
            "lsif_uploads u"
        };
        let cte_prefix = if ctes.is_empty() {
            String::new()
        } else {
            format!("WITH {}\n", ctes.join(", "))
        };

        let limit = add_param(&mut params, Param::Int(opts.limit as i64));
        let offset = add_param(&mut params, Param::Int(opts.offset as i64));
        let select = upload_select().replace("FROM lsif_uploads u", &format!("FROM {source_table}"));
        let select_sql = format!(
            "{cte_prefix}{select}\nWHERE {where_clause}\nORDER BY uploaded_at DESC, id\nLIMIT ${limit} OFFSET ${offset}"
        );
        let count_sql = format!(
            "{cte_prefix}SELECT COUNT(*) AS count FROM {source_table} JOIN repo ON repo.id = u.repository_id WHERE {where_clause}"
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

    fn get_upload_by_id(&self, id: i64) -> Result<Option<UploadRow>, String> {
        let select = upload_select();
        let sql = format!(
            "{select}\nWHERE repo.deleted_at IS NULL AND u.state != 'deleted' AND u.id = $1"
        );
        let row = block_on(
            sqlx::query(&sql)
                .bind(id)
                .fetch_optional(&self.pool),
        )
        .map_err(|error| error.to_string())?;
        row.map(|row| scan_upload(&row))
            .transpose()
            .map_err(|error| error.to_string())
    }

    fn get_auto_index_job_by_id(&self, id: i64) -> Result<Option<AutoIndexJobRow>, String> {
        let select = index_select();
        let sql = format!("{select}\nWHERE repo.deleted_at IS NULL AND u.id = $1");
        let row = block_on(
            sqlx::query(&sql)
                .bind(id)
                .fetch_optional(&self.pool),
        )
        .map_err(|error| error.to_string())?;
        row.map(|row| scan_index(&row))
            .transpose()
            .map_err(|error| error.to_string())
    }

    fn delete_upload_by_id(&self, id: i64) -> Result<(), String> {
        block_on(sg_db::codeintel_uploads::delete_upload_by_id(&self.pool, id))
            .map(|_| ())
            .map_err(|error| error.to_string())
    }

    fn delete_auto_index_job_by_id(&self, id: i64) -> Result<(), String> {
        block_on(sg_db::codeintel_uploads::delete_auto_index_job_by_id(
            &self.pool,
            id,
        ))
        .map(|_| ())
        .map_err(|error| error.to_string())
    }

    fn delete_uploads(&self, opts: &DeleteUploadsOptions) -> Result<(), String> {
        block_on(sg_db::codeintel_uploads::delete_uploads_by_filter(
            &self.pool,
            opts.repository_id,
            &opts.states,
            &opts.indexer_names,
            &opts.term,
            opts.visible_at_tip,
        ))
        .map_err(|error| error.to_string())
    }

    fn delete_auto_index_jobs(&self, opts: &DeleteAutoIndexJobsOptions) -> Result<(), String> {
        block_on(sg_db::codeintel_uploads::delete_auto_index_jobs_by_filter(
            &self.pool,
            opts.repository_id,
            &opts.states,
            &opts.indexer_names,
            &opts.term,
            opts.without_upload,
        ))
        .map_err(|error| error.to_string())
    }

    fn reindex_upload_by_id(&self, id: i64) -> Result<(), String> {
        block_on(sg_db::codeintel_uploads::reindex_upload_by_id(
            &self.pool,
            id,
        ))
        .map_err(|error| error.to_string())
    }

    fn set_rerun_auto_index_job_by_id(&self, id: i64) -> Result<(), String> {
        block_on(sg_db::codeintel_uploads::set_rerun_auto_index_job_by_id(
            &self.pool,
            id,
        ))
        .map_err(|error| error.to_string())
    }

    fn reindex_uploads(&self, opts: &ReindexUploadsOptions) -> Result<(), String> {
        block_on(sg_db::codeintel_uploads::reindex_uploads_by_filter(
            &self.pool,
            opts.repository_id,
            &opts.states,
            &opts.indexer_names,
            &opts.term,
            opts.visible_at_tip,
        ))
        .map_err(|error| error.to_string())
    }

    fn set_rerun_auto_index_jobs(
        &self,
        opts: &SetRerunAutoIndexJobsOptions,
    ) -> Result<(), String> {
        block_on(
            sg_db::codeintel_uploads::set_rerun_auto_index_jobs_by_filter(
                &self.pool,
                opts.repository_id,
                &opts.states,
                &opts.indexer_names,
                &opts.term,
                opts.without_upload,
            ),
        )
        .map_err(|error| error.to_string())
    }

    fn get_indexer_names(&self, repository_id: i32) -> Result<Vec<String>, String> {
        block_on(sg_db::codeintel_uploads::get_indexer_names(
            &self.pool,
            repository_id,
        ))
        .map_err(|error| error.to_string())
    }

    fn get_audit_logs_for_upload(&self, upload_id: i32) -> Result<Vec<AuditLogRow>, String> {
        block_on(sg_db::codeintel_uploads::get_audit_logs_for_upload(
            &self.pool,
            upload_id,
        ))
        .map(|rows| {
            rows.into_iter()
                .map(|row| AuditLogRow {
                    log_timestamp: row.log_timestamp,
                    record_deleted_at: row.record_deleted_at,
                    upload_id: row.upload_id,
                    commit: row.commit,
                    root: row.root,
                    uploaded_at: row.uploaded_at,
                    indexer: row.indexer,
                    transition_columns: row.transition_columns,
                    reason: row.reason,
                    operation: row.operation,
                })
                .collect()
        })
        .map_err(|error| error.to_string())
    }

    fn num_repositories_with_code_intelligence(&self) -> Result<i64, String> {
        block_on(sg_db::codeintel_uploads::num_repositories_with_code_intelligence(
            &self.pool,
        ))
        .map_err(|error| error.to_string())
    }

    fn repository_ids_with_errors(
        &self,
        offset: i64,
        limit: i64,
    ) -> Result<(Vec<RepositoryWithCountRow>, i64), String> {
        block_on(sg_db::codeintel_uploads::repository_ids_with_errors(
            &self.pool, offset, limit,
        ))
        .map(|(rows, total)| {
            (
                rows.into_iter()
                    .map(|row| RepositoryWithCountRow {
                        repository_id: row.repository_id,
                        repository_name: row.repository_name,
                        count: row.count,
                    })
                    .collect(),
                total,
            )
        })
        .map_err(|error| error.to_string())
    }

    fn repository_ids_with_configuration(
        &self,
        offset: i64,
        limit: i64,
    ) -> Result<(Vec<RepositoryWithConfigurationRow>, i64), String> {
        block_on(sg_db::codeintel_uploads::repository_ids_with_configuration(
            &self.pool, offset, limit,
        ))
        .map(|(rows, total)| {
            (
                rows.into_iter()
                    .map(|row| RepositoryWithConfigurationRow {
                        repository_id: row.repository_id,
                        repository_name: row.repository_name,
                        available_indexers: row.available_indexers,
                    })
                    .collect(),
                total,
            )
        })
        .map_err(|error| error.to_string())
    }

    // -- M9.30d: projectRoot / tags / retentionPolicyOverview /
    //    Repository.codeIntelSummary ---------------------------------------

    fn repository_name(&self, repository_id: i32) -> Result<Option<String>, String> {
        let row = block_on(
            sqlx::query("SELECT name FROM repo WHERE id = $1")
                .bind(repository_id)
                .fetch_optional(&self.pool),
        )
        .map_err(|error| error.to_string())?;
        row.map(|row| row.try_get("name"))
            .transpose()
            .map_err(|error| error.to_string())
    }

    fn resolve_revision(
        &self,
        repo_id: i32,
        repo_name: &str,
        rev: &str,
    ) -> Result<Option<String>, String> {
        let Some(git) = &self.git else {
            return Ok(None);
        };
        match block_on(git.resolve_revision(repo_id, repo_name, rev)) {
            Ok(oid) => Ok(Some(oid)),
            Err(error) if error.revision_not_found => Ok(None),
            Err(error) => Err(error.message),
        }
    }

    fn list_tags_at_commit(
        &self,
        repo_id: i32,
        repo_name: &str,
        oid: &str,
    ) -> Result<Vec<String>, String> {
        let Some(git) = &self.git else {
            return Ok(Vec::new());
        };
        let refs = block_on(git.list_refs_at_commit(repo_id, repo_name, oid))
            .map_err(|error| error.message)?;
        Ok(refs.into_iter().map(|git_ref| git_ref.short_name).collect())
    }

    fn retention_policy_overview(
        &self,
        upload: &UploadRow,
        matches_only: bool,
        first: i64,
        after: i64,
        term: &str,
    ) -> Result<Vec<RetentionPolicyMatchRow>, String> {
        // `policies.Service.GetRetentionPolicyOverview` (`service.go:106-182`).
        let opts = GetConfigurationPoliciesOptions {
            repository_id: upload.repository_id,
            term: term.to_owned(),
            protected: None,
            for_data_retention: Some(true),
            for_precise_indexing: None,
            for_syntactic_indexing: None,
            for_embeddings: None,
            limit: first,
            offset: after,
        };
        let (policies, _total_count) =
            block_on(codeintel_policies::get_configuration_policies(&self.pool, &opts))?;

        // `getCommitsVisibleToUpload` (`service.go:283-296`): 50 per page.
        let mut visible_commits: Vec<String> = Vec::new();
        let mut token: Option<String> = None;
        loop {
            let (commits, next_token) = block_on(
                sg_db::codeintel_uploads::get_commits_visible_to_upload(
                    &self.pool,
                    upload.id,
                    50,
                    token.as_deref(),
                ),
            )
            .map_err(|error| error.to_string())?;
            token = next_token;
            let done = token.is_none();
            visible_commits.extend(commits);
            if done {
                break;
            }
        }

        let Some(repo_name) = self.repository_name(upload.repository_id)? else {
            return Err(format!("repository not found: {}", upload.repository_id));
        };
        let matching =
            self.commits_described_by_policy(upload.repository_id, &repo_name, &policies)?;

        // `populateMatchingCommits` (`service.go:301-363`).
        let now = OffsetDateTime::now_utc();
        let mut potential_matches: Vec<RetentionPolicyMatchRow> = Vec::new();
        let mut index_set: std::collections::HashMap<i32, usize> =
            std::collections::HashMap::new();
        if let Some(policy_matches) = matching.get(&upload.commit) {
            for policy_match in policy_matches {
                if policy_match.duration_hours.is_none()
                    || now - upload.uploaded_at
                        < time::Duration::hours(i64::from(
                            policy_match.duration_hours.unwrap_or_default(),
                        ))
                {
                    let policy_id = policy_match.policy_id.unwrap_or(-1);
                    potential_matches.push(RetentionPolicyMatchRow {
                        policy: policy_by_id(&policies, policy_id),
                        matched: true,
                        protecting_commits: None,
                    });
                    index_set.insert(policy_id, potential_matches.len() - 1);
                }
            }
        }
        for commit in &visible_commits {
            if commit == &upload.commit {
                continue;
            }
            if let Some(policy_matches) = matching.get(commit) {
                for policy_match in policy_matches {
                    if policy_match.duration_hours.is_none()
                        || now - upload.uploaded_at
                            < time::Duration::hours(i64::from(
                                policy_match.duration_hours.unwrap_or_default(),
                            ))
                    {
                        let policy_id = policy_match.policy_id.unwrap_or(-1);
                        match index_set.get(&policy_id) {
                            Some(&index)
                                if potential_matches[index].protecting_commits.is_some() =>
                            {
                                potential_matches[index]
                                    .protecting_commits
                                    .as_mut()
                                    .expect("protecting commits")
                                    .push(commit.clone());
                            }
                            Some(_) => {}
                            None => {
                                potential_matches.push(RetentionPolicyMatchRow {
                                    policy: policy_by_id(&policies, policy_id),
                                    matched: true,
                                    protecting_commits: Some(vec![commit.clone()]),
                                });
                                index_set.insert(policy_id, potential_matches.len() - 1);
                            }
                        }
                    }
                }
            }
        }
        if !matches_only {
            for policy in &policies {
                if !index_set.contains_key(&policy.id) {
                    potential_matches.push(RetentionPolicyMatchRow {
                        policy: Some(policy.clone()),
                        matched: false,
                        protecting_commits: None,
                    });
                }
            }
        }

        // Implicit policy (`ConfigurationPolicy == nil`) first, then matches,
        // then ID ascending (`service.go:161-179`).
        potential_matches.sort_by(|a, b| {
            let a_key = (
                a.policy.is_some(),
                !a.matched,
                a.policy.as_ref().map(|policy| policy.id).unwrap_or(-1),
            );
            let b_key = (
                b.policy.is_some(),
                !b.matched,
                b.policy.as_ref().map(|policy| policy.id).unwrap_or(-1),
            );
            a_key.cmp(&b_key)
        });
        Ok(potential_matches)
    }

    fn last_index_scan(&self, repository_id: i32) -> Result<Option<OffsetDateTime>, String> {
        let row = block_on(
            sqlx::query(
                "SELECT last_index_scan_at FROM lsif_last_index_scan WHERE repository_id = $1",
            )
            .bind(repository_id)
            .fetch_optional(&self.pool),
        )
        .map_err(|error| error.to_string())?;
        row.map(|row| row.try_get("last_index_scan_at"))
            .transpose()
            .map_err(|error| error.to_string())
    }

    fn last_upload_retention_scan(
        &self,
        repository_id: i32,
    ) -> Result<Option<OffsetDateTime>, String> {
        let row = block_on(
            sqlx::query(
                "SELECT last_retention_scan_at FROM lsif_last_retention_scan WHERE repository_id = $1",
            )
            .bind(repository_id)
            .fetch_optional(&self.pool),
        )
        .map_err(|error| error.to_string())?;
        row.map(|row| row.try_get("last_retention_scan_at"))
            .transpose()
            .map_err(|error| error.to_string())
    }

    fn recent_activity(&self, repository_id: i32) -> Result<RecentActivityRows, String> {
        let upload_groups = block_on(
            sg_db::codeintel_autoindexing_summary::recent_uploads_summary(
                &self.pool,
                repository_id,
            ),
        )
        .map_err(|error| error.to_string())?;
        let index_groups = block_on(
            sg_db::codeintel_autoindexing_summary::recent_auto_index_jobs_summary(
                &self.pool,
                repository_id,
            ),
        )
        .map_err(|error| error.to_string())?;

        let upload_ids: Vec<i64> = upload_groups
            .iter()
            .flat_map(|group| group.ids.iter().copied())
            .collect();
        let index_ids: Vec<i64> = index_groups
            .iter()
            .flat_map(|group| group.ids.iter().copied())
            .collect();
        let uploads = if upload_ids.is_empty() {
            Vec::new()
        } else {
            self.get_uploads_by_ids(&upload_ids)?
        };
        let indexes = if index_ids.is_empty() {
            Vec::new()
        } else {
            self.get_auto_index_jobs_by_ids(&index_ids)?
        };
        Ok(RecentActivityRows { uploads, indexes })
    }

    fn auto_indexing_enabled(&self) -> bool {
        self.auto_indexing_enabled
    }
}

/// One `policies.PolicyMatch` (`matcher.go:26-32`) reduced to the fields
/// retention uses.
struct PolicyMatchRow {
    policy_id: Option<i32>,
    duration_hours: Option<i32>,
}

impl PgCodeIntelUploadsStore {
    /// `Matcher.CommitsDescribedByPolicy` (`policies/matcher.go:61-117`).
    ///
    /// The tag/branch ref matching is ported; intermediate branch commits
    /// (`gitserver.Commits`) and `GIT_COMMIT` policies need a commit reader
    /// that [`GitObjectSource`] does not expose, so they are rejected rather
    /// than silently mis-matched (documented gap).
    fn commits_described_by_policy(
        &self,
        repository_id: i32,
        repo_name: &str,
        policies: &[ConfigurationPolicy],
    ) -> Result<std::collections::HashMap<String, Vec<PolicyMatchRow>>, String> {
        let mut commit_map: std::collections::HashMap<String, Vec<PolicyMatchRow>> =
            std::collections::HashMap::new();
        if policies.is_empty() {
            return Ok(commit_map);
        }
        let mut patterns: std::collections::HashMap<String, sg_codeintel::glob::Glob> =
            std::collections::HashMap::new();
        for policy in policies {
            if policy.policy_type == "GIT_COMMIT" || patterns.contains_key(&policy.pattern) {
                continue;
            }
            let glob = sg_codeintel::glob::Glob::compile(&policy.pattern).map_err(|_| {
                format!(
                    "failed to compile glob pattern `{}` in configuration policy {}",
                    policy.pattern, policy.id
                )
            })?;
            patterns.insert(policy.pattern.clone(), glob);
        }

        let refs = match &self.git {
            Some(git) => block_on(git.list_refs(repository_id, repo_name, true, true))
                .map_err(|error| error.message)?,
            None => Vec::new(),
        };
        for git_ref in &refs {
            match git_ref.ref_type {
                RefType::Tag => {
                    for policy in policies.iter().filter(|p| p.policy_type == "GIT_TAG") {
                        if patterns[&policy.pattern].matches(&git_ref.short_name) {
                            commit_map
                                .entry(git_ref.target_commit.clone())
                                .or_default()
                                .push(PolicyMatchRow {
                                    policy_id: Some(policy.id),
                                    duration_hours: policy.retention_duration_hours,
                                });
                        }
                    }
                }
                RefType::Branch => {
                    // `includeTipOfDefaultBranch` (`matchBranchHeads`).
                    if git_ref.is_head {
                        commit_map
                            .entry(git_ref.target_commit.clone())
                            .or_default()
                            .push(PolicyMatchRow {
                                policy_id: None,
                                duration_hours: None,
                            });
                    }
                    for policy in policies.iter().filter(|p| p.policy_type == "GIT_TREE") {
                        if patterns[&policy.pattern].matches(&git_ref.short_name) {
                            commit_map
                                .entry(git_ref.target_commit.clone())
                                .or_default()
                                .push(PolicyMatchRow {
                                    policy_id: Some(policy.id),
                                    duration_hours: policy.retention_duration_hours,
                                });
                        }
                    }
                }
                RefType::Unknown => {}
            }
        }
        if policies.iter().any(|policy| policy.policy_type == "GIT_COMMIT") {
            return Err(
                "GIT_COMMIT configuration policies are not supported: the gitserver seam has no \
                 commit reader"
                    .to_owned(),
            );
        }
        Ok(commit_map)
    }
}

/// `policyByID` (`service.go:365-375`): `-1` is the implicit policy.
fn policy_by_id(policies: &[ConfigurationPolicy], id: i32) -> Option<ConfigurationPolicy> {
    if id == -1 {
        return None;
    }
    policies.iter().find(|policy| policy.id == id).cloned()
}
