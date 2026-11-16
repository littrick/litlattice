#![forbid(unsafe_code)]

//! Production Postgres adapter for
//! [`crate::codeintel_codenav_graphql::CodeIntelCodenavDatabase`] (M9.48b).
//!
//! Reads only existing tables/views from the M6 storage contract:
//!
//! - `lsif_nearest_uploads` / `lsif_nearest_uploads_links` /
//!   `lsif_uploads_visible_at_tip` / `lsif_dumps_with_repository_name`
//!   (`FindClosestCompletedUploads`,
//!   `internal/codeintel/uploads/internal/store/{commitgraph,util}.go`);
//! - `codeintel_scip_document_lookup` JOIN `codeintel_scip_documents`
//!   (`SCIPDocument`, `GetDiagnostics`);
//! - gitserver `ReadFile` / `Diff` through the existing
//!   `GitObjectSource`/`GitCommitExtraSource` seams (snapshot and cross-commit
//!   range translation).
//!
//! No table/column/index/Redis key is added. The commit-graph refresh for
//! commits newer than the last `lsif_nearest_uploads` update is not performed
//! here (Go's `InferClosestUploads` asks gitserver for ancestors and marks the
//! repository dirty); the adapter falls back to an exact-commit upload lookup
//! and records the deviation in the overview.

use std::future::Future;
use std::io::Read;
use std::sync::Arc;

use sqlx::{PgPool, Row};

use sg_graphql::exec::{Actor, Arguments, FieldValue};
use sg_scip::scip;
use sg_scip::Message as _;

use crate::codeintel_codenav_graphql::{
    self, new_compact_hunk, translate_position, translate_range, CodeIntelCodenavDatabase,
    CompactHunk, DiagnosticRecord, UploadRecord,
};
use crate::git_tree_entry_graphql::SymbolRange;
use crate::codeintel_uploads_graphql::{marshal_precise_index_id, precise_index_by_id, CodeIntelUploadsHandle};
use crate::git_commit_extra::GitCommitExtraSource;
use crate::git_graphql::GitObjectSource;

pub(crate) fn block_on<F: Future>(future: F) -> F::Output {
    match tokio::runtime::Handle::try_current() {
        Ok(handle) => tokio::task::block_in_place(|| handle.block_on(future)),
        Err(_) => tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("codeintel codenav blocking runtime")
            .block_on(future),
    }
}

/// Production adapter.
///
/// `pool` is the frontend database (`lsif_uploads` and the nearest-uploads
/// graph); `scip_pool` is the codeintel database holding
/// `codeintel_scip_documents` / `codeintel_scip_document_lookup`. The Go
/// baseline uses two connections too (`codenav.NewService(..., db,
/// codeIntelDB, ...)` and `lsifstore.New(..., codeIntelDB)`), so a split
/// deployment must not look for SCIP documents in the frontend schema.
pub struct PgCodeIntelCodenavStore {
    pool: PgPool,
    scip_pool: PgPool,
    git: Option<Arc<dyn GitObjectSource>>,
    diff: Option<Arc<dyn GitCommitExtraSource>>,
    uploads: Option<CodeIntelUploadsHandle>,
}

impl std::fmt::Debug for PgCodeIntelCodenavStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("PgCodeIntelCodenavStore")
    }
}

impl PgCodeIntelCodenavStore {
    pub fn new(pool: PgPool) -> Self {
        Self {
            scip_pool: pool.clone(),
            pool,
            git: None,
            diff: None,
            uploads: None,
        }
    }

    /// Use a separate pool for the codeintel database that stores the SCIP
    /// documents. Without it the store falls back to the frontend pool (the
    /// single-database test deployments).
    pub fn with_scip_pool(mut self, scip_pool: PgPool) -> Self {
        self.scip_pool = scip_pool;
        self
    }

    pub fn with_git(mut self, git: Arc<dyn GitObjectSource>) -> Self {
        self.git = Some(git);
        self
    }

    pub fn with_diff_source(mut self, diff: Arc<dyn GitCommitExtraSource>) -> Self {
        self.diff = Some(diff);
        self
    }

    pub fn with_uploads(mut self, uploads: CodeIntelUploadsHandle) -> Self {
        self.uploads = Some(uploads);
        self
    }

    fn repo_id(&self, name: &str) -> Result<Option<i32>, String> {
        block_on(async {
            sqlx::query_scalar::<_, i32>("SELECT id FROM repo WHERE name = $1 AND deleted_at IS NULL")
                .bind(name)
                .fetch_optional(&self.pool)
                .await
        })
        .map_err(|error| error.to_string())
    }

    fn decode_document(payload: &[u8]) -> Result<scip::Document, String> {
        let mut decoder = flate2::read::GzDecoder::new(payload);
        let mut bytes = Vec::new();
        decoder
            .read_to_end(&mut bytes)
            .map_err(|error| error.to_string())?;
        scip::Document::decode(bytes.as_slice()).map_err(|error| error.to_string())
    }

    fn upload_record(row: &sqlx::postgres::PgRow) -> Result<UploadRecord, String> {
        Ok(UploadRecord {
            id: row.try_get::<i32, _>("id").map_err(|e| e.to_string())? as i64,
            repository_id: row.try_get("repository_id").map_err(|e| e.to_string())?,
            repository_name: row.try_get("repository_name").map_err(|e| e.to_string())?,
            commit: row.try_get("commit").map_err(|e| e.to_string())?,
            root: row
                .try_get::<Option<String>, _>("root")
                .map_err(|e| e.to_string())?
                .unwrap_or_default(),
            indexer: row.try_get("indexer").map_err(|e| e.to_string())?,
            indexer_version: row
                .try_get::<Option<String>, _>("indexer_version")
                .map_err(|e| e.to_string())?
                .unwrap_or_default(),
        })
    }

    fn fetch_hunks(
        &self,
        from: &str,
        to: &str,
        repository_id: i32,
        repository_name: &str,
        path: &str,
    ) -> Result<Vec<CompactHunk>, String> {
        if from == to {
            return Ok(Vec::new());
        }
        let Some(diff) = &self.diff else {
            return Ok(Vec::new());
        };
        let raw = block_on(diff.raw_diff(
            repository_id,
            repository_name,
            &sg_git::RawDiffOptions {
                base: from.to_owned(),
                head: to.to_owned(),
                range_type: Some(sg_git::DiffRangeType::OnlyInHead),
                paths: vec![path.to_owned()],
            },
        ))
        .map_err(|error| error.message)?;
        let files = sg_service_gitserver::diff::parse_multi_file_diff(&raw)
            .map_err(|error| error.to_string())?;
        for file in files {
            if file.orig_name != file.new_name {
                continue;
            }
            let orig = String::from_utf8_lossy(&file.orig_name);
            if orig != path {
                continue;
            }
            return Ok(file.hunks.iter().map(new_compact_hunk).collect());
        }
        Ok(Vec::new())
    }
}

impl CodeIntelCodenavDatabase for PgCodeIntelCodenavStore {
    fn closest_uploads(
        &self,
        repository_id: i32,
        commit: &str,
        path: &str,
        exact_path: bool,
        tool_name: &str,
    ) -> Result<Vec<UploadRecord>, String> {
        let commit_bytes = hex_decode(commit)?;
        let path_condition = if exact_path {
            "$3 LIKE (u.root || '%')"
        } else {
            "($3 LIKE (u.root || '%') OR u.root LIKE ($3 || '%'))"
        };
        let indexer_condition = if tool_name.is_empty() {
            "u.indexer <> 'scip-syntax'".to_owned()
        } else {
            "u.indexer = $4".to_owned()
        };
        let sql = format!(
            r#"
WITH visible_uploads AS (
    SELECT t.upload_id
    FROM (
        SELECT
            t.*,
            row_number() OVER (
                PARTITION BY u.root, (
                    CASE
                        WHEN u.indexer LIKE 'scip-%' OR u.indexer LIKE 'lsif-%' THEN substr(u.indexer, 6)
                        ELSE u.indexer
                    END
                ) ORDER BY t.distance ASC, t.upload_id DESC
            ) AS r
        FROM (
            SELECT
                nu.repository_id,
                upload_id::integer,
                nu.commit_bytea,
                u_distance::text::integer AS distance
            FROM lsif_nearest_uploads nu
            CROSS JOIN jsonb_each(nu.uploads) AS u(upload_id, u_distance)
            WHERE nu.repository_id = $1 AND nu.commit_bytea = $2
            UNION (
                SELECT
                    nu.repository_id,
                    upload_id::integer,
                    ul.commit_bytea,
                    u_distance::text::integer + ul.distance
                FROM lsif_nearest_uploads_links ul
                JOIN lsif_nearest_uploads nu
                    ON nu.repository_id = ul.repository_id
                    AND nu.commit_bytea = ul.ancestor_commit_bytea
                CROSS JOIN jsonb_each(nu.uploads) AS u(upload_id, u_distance)
                WHERE nu.repository_id = $1 AND ul.commit_bytea = $2
            )
        ) t
        JOIN lsif_uploads u ON u.id = t.upload_id
    ) t
    WHERE t.r <= 1
)
SELECT
    u.id, u.commit, u.root, u.repository_id, u.repository_name,
    u.indexer, u.indexer_version
FROM visible_uploads vu
JOIN lsif_dumps_with_repository_name u ON u.id = vu.upload_id
WHERE {path_condition} AND {indexer_condition}
ORDER BY u.finished_at DESC
"#
        );
        let mut query = sqlx::query(&sql)
            .bind(repository_id)
            .bind(commit_bytes)
            .bind(path);
        if !tool_name.is_empty() {
            query = query.bind(tool_name);
        }
        let rows = block_on(query.fetch_all(&self.pool)).map_err(|error| error.to_string())?;
        let mut uploads = Vec::new();
        for row in &rows {
            uploads.push(Self::upload_record(row)?);
        }
        if !uploads.is_empty() {
            return Ok(uploads);
        }

        // Exact-commit fallback (deviation: no ancestor inference / dirty mark).
        let exact_sql = format!(
            r#"
SELECT
    u.id, u.commit, u.root, u.repository_id, u.repository_name,
    u.indexer, u.indexer_version
FROM lsif_uploads_with_repository_name u
WHERE u.repository_id = $1 AND u.commit = $2 AND {path_condition} AND {indexer_condition}
ORDER BY u.finished_at DESC
"#
        );
        let mut query = sqlx::query(&exact_sql)
            .bind(repository_id)
            .bind(commit)
            .bind(path);
        if !tool_name.is_empty() {
            query = query.bind(tool_name);
        }
        let rows = block_on(query.fetch_all(&self.pool)).map_err(|error| error.to_string())?;
        let mut uploads = Vec::new();
        for row in &rows {
            uploads.push(Self::upload_record(row)?);
        }
        Ok(uploads)
    }

    fn scip_document(
        &self,
        upload: &UploadRecord,
        target_commit: &str,
        repo_path: &str,
    ) -> Result<Option<scip::Document>, String> {
        let upload_path = upload.upload_rel_path(repo_path);
        let row = block_on(
            sqlx::query(
                r#"
SELECT sd.raw_scip_payload
FROM codeintel_scip_document_lookup sid
JOIN codeintel_scip_documents sd ON sd.id = sid.document_id
WHERE sid.upload_id = $1 AND sid.document_path = $2
LIMIT 1
"#,
            )
            .bind(upload.id)
            .bind(upload_path)
            .fetch_optional(&self.scip_pool),
        )
        .map_err(|error| error.to_string())?;
        let Some(row) = row else {
            return Ok(None);
        };
        let payload: Vec<u8> = row.try_get("raw_scip_payload").map_err(|e| e.to_string())?;
        let mut document = Self::decode_document(&payload)?;
        document.relative_path = repo_path.to_owned();

        if upload.commit != target_commit {
            let mut translated = Vec::new();
            for occurrence in document.occurrences {
                let source_range =
                    codeintel_codenav_graphql::symbol_range_from_scip(&occurrence.range);
                match self.translate_range(
                    &upload.commit,
                    target_commit,
                    upload.repository_id,
                    &upload.repository_name,
                    repo_path,
                    source_range,
                )? {
                    Some(target) => {
                        let mut occurrence = occurrence;
                        occurrence.range = codeintel_codenav_graphql::scip_range_from_symbol(target);
                        translated.push(occurrence);
                    }
                    None => continue,
                }
            }
            document.occurrences = translated;
        }
        Ok(Some(document))
    }

    fn documents_with_prefix(
        &self,
        upload: &UploadRecord,
        path_prefix: &str,
    ) -> Result<Vec<(String, scip::Document)>, String> {
        let upload_prefix = upload.upload_rel_path(path_prefix);
        let rows = block_on(
            sqlx::query(
                r#"
SELECT sid.document_path, sd.raw_scip_payload
FROM codeintel_scip_document_lookup sid
JOIN codeintel_scip_documents sd ON sd.id = sid.document_id
WHERE sid.upload_id = $1 AND sid.document_path LIKE $2
LIMIT 1
"#,
            )
            .bind(upload.id)
            .bind(format!("{upload_prefix}%"))
            .fetch_all(&self.scip_pool),
        )
        .map_err(|error| error.to_string())?;
        let mut out = Vec::new();
        for row in rows {
            let path: String = row.try_get("document_path").map_err(|e| e.to_string())?;
            let payload: Vec<u8> = row.try_get("raw_scip_payload").map_err(|e| e.to_string())?;
            out.push((path, Self::decode_document(&payload)?));
        }
        Ok(out)
    }

    fn file_content(&self, repo_name: &str, commit: &str, path: &str) -> Result<Option<String>, String> {
        let Some(git) = &self.git else {
            return Ok(None);
        };
        let repository_id = self.repo_id(repo_name)?.unwrap_or(0);
        let bytes = block_on(git.read_file(repository_id, repo_name, commit, path))
            .map_err(|error| error.message)?;
        Ok(Some(String::from_utf8_lossy(&bytes).into_owned()))
    }

    fn translate_range(
        &self,
        from: &str,
        to: &str,
        repository_id: i32,
        repository_name: &str,
        path: &str,
        range: SymbolRange,
    ) -> Result<Option<SymbolRange>, String> {
        let hunks = self.fetch_hunks(from, to, repository_id, repository_name, path)?;
        Ok(translate_range(&hunks, range))
    }

    fn translate_position(
        &self,
        from: &str,
        to: &str,
        repository_id: i32,
        repository_name: &str,
        path: &str,
        position: crate::git_tree_entry_graphql::Position,
    ) -> Result<Option<crate::git_tree_entry_graphql::Position>, String> {
        let hunks = self.fetch_hunks(from, to, repository_id, repository_name, path)?;
        Ok(translate_position(&hunks, position))
    }

    fn visible_index_field(
        &self,
        upload: &UploadRecord,
        actor: Actor,
    ) -> Result<Option<FieldValue>, String> {
        let Some(uploads) = &self.uploads else {
            return Ok(None);
        };
        let id = marshal_precise_index_id(Some(upload.id), None);
        precise_index_by_id(uploads, &id, actor).map_err(|error| error.message)
    }

    fn usages_for_symbol(
        &self,
        arguments: &Arguments,
        actor: Actor,
    ) -> Result<FieldValue, String> {
        usages_for_symbol_impl(self, arguments, actor)
    }

    fn repository_id_by_name(&self, name: &str) -> Result<Option<i32>, String> {
        self.repo_id(name)
    }
}

fn hex_decode(value: &str) -> Result<Vec<u8>, String> {
    if value.len() % 2 != 0 {
        return Err("odd length hex commit".to_owned());
    }
    let mut out = Vec::with_capacity(value.len() / 2);
    let bytes = value.as_bytes();
    let mut index = 0;
    while index < bytes.len() {
        let high = (bytes[index] as char)
            .to_digit(16)
            .ok_or_else(|| "invalid hex commit".to_owned())?;
        let low = (bytes[index + 1] as char)
            .to_digit(16)
            .ok_or_else(|| "invalid hex commit".to_owned())?;
        out.push(((high << 4) | low) as u8);
        index += 2;
    }
    Ok(out)
}

/// The local-precise subset of `rootResolver.UsagesForSymbol`.
fn usages_for_symbol_impl(
    store: &PgCodeIntelCodenavStore,
    arguments: &Arguments,
    actor: Actor,
) -> Result<FieldValue, String> {
    let Some(range_input) = arguments.get("range") else {
        return Ok(empty_usage_connection());
    };
    let sg_graphql::exec::InputValue::Object(range_input) = range_input else {
        return Ok(empty_usage_connection());
    };
    let Some(sg_graphql::exec::InputValue::Str(repository)) = range_input.get("repository") else {
        return Ok(empty_usage_connection());
    };
    let Some(sg_graphql::exec::InputValue::Str(path)) = range_input.get("path") else {
        return Ok(empty_usage_connection());
    };
    let revision = match range_input.get("revision") {
        Some(sg_graphql::exec::InputValue::Str(revision)) => revision.clone(),
        _ => String::new(),
    };
    let Some(start) = position_from_input(range_input.get("start")) else {
        return Ok(empty_usage_connection());
    };
    let Some(end) = position_from_input(range_input.get("end")) else {
        return Ok(empty_usage_connection());
    };
    let symbol = symbol_filter(arguments);
    let Some(repository_id) = store.repo_id(repository)? else {
        return Ok(empty_usage_connection());
    };
    let revision = if revision.is_empty() {
        match store.default_branch(repository)? {
            Some(branch) => branch,
            None => return Ok(empty_usage_connection()),
        }
    } else {
        revision
    };
    let Some(commit) = store.resolve_revision(repository, &revision)? else {
        return Ok(empty_usage_connection());
    };
    let uploads = store.closest_uploads(repository_id, &commit, path, true, "")?;
    if uploads.is_empty() {
        return Ok(empty_usage_connection());
    }
    let target_range = SymbolRange { start, end };

    let mut usages = Vec::new();
    for upload in uploads {
        let Some(document) = store.scip_document(&upload, &commit, path)? else {
            continue;
        };
        let Some(lookup) = document.occurrences.iter().find(|occurrence| {
            codeintel_codenav_graphql::symbol_range_from_scip(&occurrence.range) == target_range
                && symbol
                    .as_ref()
                    .map(|symbol| &occurrence.symbol == symbol)
                    .unwrap_or(true)
        }) else {
            continue;
        };
        let data = codeintel_codenav_graphql::extract_occurrence_data(&document, lookup);
        let contents = store
            .file_content(&upload.repository_name, &commit, path)?
            .unwrap_or_default();
        let lines: Vec<&str> = contents.split('\n').collect();
        // Go's `Service.PreciseUsages` walks Definitions -> Implementations
        // -> Prototypes -> References. `scip_document` already translated the
        // occurrence ranges into `commit` (Go `Service.SCIPDocument`), so the
        // builders must not be translated a second time.
        let phases = [
            (
                data.definitions,
                codeintel_codenav_graphql::UsageKind::Definition,
            ),
            (
                data.implementations,
                codeintel_codenav_graphql::UsageKind::Implementation,
            ),
            (
                data.prototypes,
                codeintel_codenav_graphql::UsageKind::Super,
            ),
            (
                data.references,
                codeintel_codenav_graphql::UsageKind::Reference,
            ),
        ];
        for (builders, kind) in phases {
            for builder in builders {
                let line_index = builder.range.start.line;
                if line_index < 0 || line_index as usize >= lines.len() {
                    continue;
                }
                usages.push(codeintel_codenav_graphql::UsageObject {
                    symbol: Some(builder.symbol),
                    provenance: "PRECISE",
                    surrounding_content: lines[line_index as usize].to_owned(),
                    usage_range: codeintel_codenav_graphql::UsageRangeObject {
                        repository: upload.repository_name.clone(),
                        revision: commit.clone(),
                        path: path.clone(),
                        range: builder.range,
                    },
                    kind,
                });
            }
        }
    }
    let _ = actor;

    let first = arguments.i64("first").unwrap_or(50).max(0) as usize;
    let total = usages.len();
    let end = first.min(total);
    let has_next = end < total;
    Ok(FieldValue::Object(Arc::new(
        codeintel_codenav_graphql::UsageConnectionObject {
            nodes: usages.into_iter().take(end).collect(),
            has_next_page: has_next,
            end_cursor: None,
        },
    )))
}

fn position_from_input(
    value: Option<&sg_graphql::exec::InputValue>,
) -> Option<crate::git_tree_entry_graphql::Position> {
    let sg_graphql::exec::InputValue::Object(map) = value? else {
        return None;
    };
    let line = match map.get("line")? {
        sg_graphql::exec::InputValue::Int(line) => *line as i32,
        _ => return None,
    };
    let character = match map.get("character")? {
        sg_graphql::exec::InputValue::Int(character) => *character as i32,
        _ => return None,
    };
    Some(crate::git_tree_entry_graphql::Position { line, character })
}

fn symbol_filter(arguments: &Arguments) -> Option<String> {
    let symbol = arguments.get("symbol")?;
    let sg_graphql::exec::InputValue::Object(symbol) = symbol else {
        return None;
    };
    let name = symbol.get("name")?;
    let sg_graphql::exec::InputValue::Object(name) = name else {
        return None;
    };
    match name.get("equals")? {
        sg_graphql::exec::InputValue::Str(value) => Some(value.clone()),
        _ => None,
    }
}

fn empty_usage_connection() -> FieldValue {
    FieldValue::Object(Arc::new(
        codeintel_codenav_graphql::UsageConnectionObject {
            nodes: Vec::new(),
            has_next_page: false,
            end_cursor: None,
        },
    ))
}

impl PgCodeIntelCodenavStore {
    fn default_branch(&self, repo_name: &str) -> Result<Option<String>, String> {
        let Some(git) = &self.git else {
            return Ok(None);
        };
        block_on(git.default_branch(0, repo_name)).map_err(|error| error.message)
    }

    /// `gitserver.Client.ResolveRevision` (`RangeInput.Resolve`).
    fn resolve_revision(&self, repo_name: &str, revision: &str) -> Result<Option<String>, String> {
        let Some(git) = &self.git else {
            return Ok(None);
        };
        match block_on(git.resolve_revision(0, repo_name, revision)) {
            Ok(oid) => Ok(Some(oid)),
            Err(error) if error.not_exist || error.revision_not_found => Ok(None),
            Err(error) => Err(error.message),
        }
    }
}

// Keep the diagnostic record import used from this module's public surface.
#[allow(dead_code)]
fn _diagnostic_marker(_: DiagnosticRecord) {}
