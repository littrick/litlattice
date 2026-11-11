#![forbid(unsafe_code)]

//! M9.8is differential harness (Rust side): the commitgraph store write-back
//! (`UpdateUploadsVisibleToCommits`) in `sg-db::codeintel_commitgraph`.
//!
//! Mirrors the untracked Go oracle
//! `internal/codeintel/uploads/internal/store/zz_m9is_oracle_test.go`. Both
//! sides reset the same scratch frontend database fixtures, run the same
//! corpus and dump the resulting rows as normalized JSON; the script
//! `test_ws/p9-go/run_commitgraph_store_diff.sh` compares them byte for byte.
//!
//! Only runs when `SG_M9IS_DIFF=1`; reads `M9IS_CORPUS`, writes `M9IS_OUT` and
//! connects to `SG_M9IS_TEST_DSN`.

use std::collections::BTreeMap;
use std::time::Duration as StdDuration;

use serde::Deserialize;
use serde_json::{json, Value};
use sg_codeintel::commitgraph::{parse_commit_graph, CommitNode};
use sg_db::codeintel_commitgraph::{
    update_uploads_visible_to_commits, CommitGraphRef, RefType, RefsByCommit,
};
use sqlx::postgres::{PgPool, PgPoolOptions};
use sqlx::Row;
use time::format_description::well_known::Rfc3339;
use time::{Duration, OffsetDateTime};

#[derive(Debug, Deserialize, Default)]
struct Corpus {
    #[serde(default)]
    cases: Vec<Case>,
}

#[derive(Debug, Deserialize, Default)]
struct Case {
    #[serde(default)]
    name: String,
    #[serde(default)]
    repository_id: i64,
    #[serde(default)]
    dirty_token: i64,
    #[serde(default)]
    update_token: i64,
    #[serde(default)]
    max_age_for_non_stale_branches_seconds: i64,
    #[serde(default)]
    max_age_for_non_stale_tags_seconds: i64,
    #[serde(default)]
    retention: Option<Retention>,
    #[serde(default)]
    commit_graph: Vec<CommitNodeSpec>,
    #[serde(default)]
    refs: Vec<RefSpec>,
    #[serde(default)]
    uploads: Vec<UploadSpec>,
    #[serde(default)]
    existing_nearest_uploads: Vec<NearestSpec>,
    #[serde(default)]
    existing_nearest_uploads_links: Vec<LinkSpec>,
    #[serde(default)]
    existing_visible_at_tip: Vec<VisibleSpec>,
    #[serde(default)]
    now: Option<String>,
}

#[derive(Debug, Deserialize, Default)]
struct Retention {
    #[serde(default)]
    branches_seconds: i32,
    #[serde(default)]
    tags_seconds: i32,
}

#[derive(Debug, Deserialize, Default)]
struct CommitNodeSpec {
    #[serde(default)]
    commit: String,
    #[serde(default)]
    parents: Vec<String>,
}

#[derive(Debug, Deserialize, Default)]
struct RefSpec {
    #[serde(default)]
    commit: String,
    #[serde(default)]
    short_name: String,
    #[serde(default)]
    ref_type: String,
    #[serde(default)]
    is_head: bool,
    #[serde(default)]
    created_age_seconds: i64,
}

#[derive(Debug, Deserialize, Default)]
struct UploadSpec {
    #[serde(default)]
    id: i32,
    #[serde(default)]
    commit: String,
    #[serde(default)]
    root: String,
    #[serde(default)]
    indexer: String,
    #[serde(default)]
    state: String,
}

#[derive(Debug, Deserialize, Default)]
struct NearestSpec {
    #[serde(default)]
    commit: String,
    #[serde(default)]
    uploads: BTreeMap<String, i32>,
}

#[derive(Debug, Deserialize, Default)]
struct LinkSpec {
    #[serde(default)]
    commit: String,
    #[serde(default)]
    ancestor: String,
    #[serde(default)]
    distance: i32,
}

#[derive(Debug, Deserialize, Default)]
struct VisibleSpec {
    #[serde(default)]
    upload_id: i32,
    #[serde(default)]
    branch_or_tag_name: String,
    #[serde(default)]
    is_default_branch: bool,
}

#[tokio::test]
async fn commitgraph_store_diff() {
    if std::env::var("SG_M9IS_DIFF").as_deref() != Ok("1") {
        return;
    }

    let corpus_path = std::env::var("M9IS_CORPUS").expect("M9IS_CORPUS is not set");
    let out_path = std::env::var("M9IS_OUT").expect("M9IS_OUT is not set");
    let dsn = std::env::var("SG_M9IS_TEST_DSN").expect("SG_M9IS_TEST_DSN is not set");

    let raw = std::fs::read_to_string(&corpus_path).expect("read corpus");
    let corpus: Corpus = serde_json::from_str(&raw).expect("parse corpus");

    let pool = PgPoolOptions::new()
        .max_connections(2)
        .connect(&dsn)
        .await
        .expect("connect database");

    let mut results = Vec::with_capacity(corpus.cases.len());
    for case in &corpus.cases {
        reset_fixture(&pool, case).await;
        results.push(run_case(&pool, case).await);
    }

    let mut output = serde_json::to_string_pretty(&Value::Array(results)).expect("encode output");
    output.push('\n');
    std::fs::write(&out_path, output).expect("write output");

    eprintln!("m9is rust wrote {} cases", corpus.cases.len());
}

async fn run_case(pool: &PgPool, case: &Case) -> Value {
    let now = case
        .now
        .as_deref()
        .map(|value| OffsetDateTime::parse(value, &Rfc3339).expect("parse corpus now timestamp"));

    let commit_graph = parse_commit_graph(
        &case
            .commit_graph
            .iter()
            .map(|node| CommitNode {
                commit: node.commit.clone(),
                parents: node.parents.clone(),
            })
            .collect::<Vec<_>>(),
    );

    let result = update_uploads_visible_to_commits(
        pool,
        case.repository_id,
        &commit_graph,
        &refs_by_commit(&case.refs),
        StdDuration::from_secs(case.max_age_for_non_stale_branches_seconds as u64),
        StdDuration::from_secs(case.max_age_for_non_stale_tags_seconds as u64),
        case.dirty_token,
        now,
    )
    .await;

    let (ok, error) = match &result {
        Ok(()) => (true, String::new()),
        Err(err) => (false, err.to_string()),
    };

    json!({
        "name": case.name,
        "ok": ok,
        "error": error,
        "nearest_uploads": dump_nearest(pool, case.repository_id).await,
        "links": dump_links(pool, case.repository_id).await,
        "visible_at_tip": dump_visible(pool, case.repository_id).await,
        "dirty_repositories": dump_dirty(pool, case.repository_id, case.now.is_some()).await,
        "uploads": dump_uploads(pool).await,
        "audit_logs": dump_audit_logs(pool).await,
    })
}

fn refs_by_commit(refs: &[RefSpec]) -> RefsByCommit {
    let now = OffsetDateTime::now_utc();
    let mut mapped: RefsByCommit = BTreeMap::new();
    for reference in refs {
        mapped
            .entry(reference.commit.clone())
            .or_default()
            .push(CommitGraphRef {
                short_name: reference.short_name.clone(),
                ref_type: match reference.ref_type.as_str() {
                    "branch" => RefType::Branch,
                    "tag" => RefType::Tag,
                    _ => RefType::Unknown,
                },
                created_at: Some(now - Duration::seconds(reference.created_age_seconds)),
                is_head: reference.is_head,
            });
    }
    mapped
}

async fn reset_fixture(pool: &PgPool, case: &Case) {
    sqlx::query(
        "TRUNCATE lsif_uploads, lsif_nearest_uploads, \
                lsif_nearest_uploads_links, lsif_uploads_visible_at_tip, \
                lsif_dirty_repositories, lsif_retention_configuration, \
                lsif_uploads_audit_logs RESTART IDENTITY CASCADE",
    )
    .execute(pool)
    .await
    .expect("truncate fixtures");

    sqlx::query("DELETE FROM repo WHERE id = $1")
        .bind(case.repository_id as i32)
        .execute(pool)
        .await
        .expect("delete fixture repo");
    sqlx::query("INSERT INTO repo (id, name) VALUES ($1, $2)")
        .bind(case.repository_id as i32)
        .bind(format!("m9is/repo-{}", case.repository_id))
        .execute(pool)
        .await
        .expect("insert fixture repo");

    for upload in &case.uploads {
        sqlx::query(
            "INSERT INTO lsif_uploads \
                 (id, commit, root, state, repository_id, indexer, num_parts, uploaded_parts) \
             VALUES ($1, $2, $3, $4, $5, $6, 1, '{0}')",
        )
        .bind(upload.id)
        .bind(&upload.commit)
        .bind(&upload.root)
        .bind(&upload.state)
        .bind(case.repository_id as i32)
        .bind(&upload.indexer)
        .execute(pool)
        .await
        .expect("insert fixture upload");
    }

    sqlx::query(
        "INSERT INTO lsif_dirty_repositories \
             (repository_id, dirty_token, update_token, set_dirty_at) \
         VALUES ($1, $2, $3, TIMESTAMPTZ '2000-01-01 00:00:00+00')",
    )
    .bind(case.repository_id as i32)
    .bind(case.dirty_token as i32)
    .bind(case.update_token as i32)
    .execute(pool)
    .await
    .expect("insert fixture dirty repository");

    if let Some(retention) = &case.retention {
        sqlx::query(
            "INSERT INTO lsif_retention_configuration \
                 (repository_id, max_age_for_non_stale_branches_seconds, \
                  max_age_for_non_stale_tags_seconds) \
             VALUES ($1, $2, $3)",
        )
        .bind(case.repository_id as i32)
        .bind(retention.branches_seconds)
        .bind(retention.tags_seconds)
        .execute(pool)
        .await
        .expect("insert fixture retention configuration");
    }

    for nearest in &case.existing_nearest_uploads {
        let payload = serde_json::to_string(&nearest.uploads).expect("encode uploads");
        sqlx::query(
            "INSERT INTO lsif_nearest_uploads (repository_id, commit_bytea, uploads) \
             VALUES ($1, decode($2, 'hex'), $3::jsonb)",
        )
        .bind(case.repository_id as i32)
        .bind(&nearest.commit)
        .bind(payload)
        .execute(pool)
        .await
        .expect("insert fixture nearest uploads");
    }

    for link in &case.existing_nearest_uploads_links {
        sqlx::query(
            "INSERT INTO lsif_nearest_uploads_links \
                 (repository_id, commit_bytea, ancestor_commit_bytea, distance) \
             VALUES ($1, decode($2, 'hex'), decode($3, 'hex'), $4)",
        )
        .bind(case.repository_id as i32)
        .bind(&link.commit)
        .bind(&link.ancestor)
        .bind(link.distance)
        .execute(pool)
        .await
        .expect("insert fixture link");
    }

    for visible in &case.existing_visible_at_tip {
        sqlx::query(
            "INSERT INTO lsif_uploads_visible_at_tip \
                 (repository_id, upload_id, branch_or_tag_name, is_default_branch) \
             VALUES ($1, $2, $3, $4)",
        )
        .bind(case.repository_id as i32)
        .bind(visible.upload_id)
        .bind(&visible.branch_or_tag_name)
        .bind(visible.is_default_branch)
        .execute(pool)
        .await
        .expect("insert fixture visible at tip");
    }

    // Fixture inserts fire the audit trigger; the differential starts from an
    // empty audit log.
    sqlx::query("TRUNCATE lsif_uploads_audit_logs RESTART IDENTITY")
        .execute(pool)
        .await
        .expect("truncate fixture audit logs");
}

async fn dump_nearest(pool: &PgPool, repository_id: i64) -> Value {
    let rows = sqlx::query(
        "SELECT repository_id, encode(commit_bytea, 'hex') AS commit, \
                uploads::text AS uploads \
         FROM lsif_nearest_uploads \
         WHERE repository_id = $1 ORDER BY commit_bytea",
    )
    .bind(repository_id as i32)
    .fetch_all(pool)
    .await
    .expect("query nearest uploads");

    Value::Array(
        rows.into_iter()
            .map(|row| {
                json!([
                    row.get::<i32, _>("repository_id"),
                    row.get::<String, _>("commit"),
                    row.get::<String, _>("uploads"),
                ])
            })
            .collect(),
    )
}

async fn dump_links(pool: &PgPool, repository_id: i64) -> Value {
    let rows = sqlx::query(
        "SELECT repository_id, encode(commit_bytea, 'hex') AS commit, \
                encode(ancestor_commit_bytea, 'hex') AS ancestor, distance \
         FROM lsif_nearest_uploads_links \
         WHERE repository_id = $1 ORDER BY commit_bytea",
    )
    .bind(repository_id as i32)
    .fetch_all(pool)
    .await
    .expect("query links");

    Value::Array(
        rows.into_iter()
            .map(|row| {
                json!([
                    row.get::<i32, _>("repository_id"),
                    row.get::<String, _>("commit"),
                    row.get::<String, _>("ancestor"),
                    row.get::<i32, _>("distance"),
                ])
            })
            .collect(),
    )
}

async fn dump_visible(pool: &PgPool, repository_id: i64) -> Value {
    let rows = sqlx::query(
        "SELECT repository_id, upload_id, branch_or_tag_name, is_default_branch \
         FROM lsif_uploads_visible_at_tip \
         WHERE repository_id = $1 \
         ORDER BY upload_id, branch_or_tag_name, is_default_branch",
    )
    .bind(repository_id as i32)
    .fetch_all(pool)
    .await
    .expect("query visible at tip");

    Value::Array(
        rows.into_iter()
            .map(|row| {
                json!([
                    row.get::<i32, _>("repository_id"),
                    row.get::<i32, _>("upload_id"),
                    row.get::<String, _>("branch_or_tag_name"),
                    row.get::<bool, _>("is_default_branch"),
                ])
            })
            .collect(),
    )
}

async fn dump_dirty(pool: &PgPool, repository_id: i64, now_provided: bool) -> Value {
    let rows = sqlx::query(
        "SELECT repository_id, dirty_token, update_token, \
                to_char(updated_at AT TIME ZONE 'UTC', \
                        'YYYY-MM-DD\"T\"HH24:MI:SS.US\"Z\"') AS updated_at \
         FROM lsif_dirty_repositories \
         WHERE repository_id = $1 ORDER BY repository_id",
    )
    .bind(repository_id as i32)
    .fetch_all(pool)
    .await
    .expect("query dirty repositories");

    Value::Array(
        rows.into_iter()
            .map(|row| {
                let updated_at: Option<String> = row.get("updated_at");
                let normalized = match updated_at {
                    None => Value::Null,
                    Some(value) if now_provided => Value::String(value),
                    // transaction_timestamp() differs between runs.
                    Some(_) => Value::String("<transaction>".to_owned()),
                };
                json!([
                    row.get::<i32, _>("repository_id"),
                    row.get::<i32, _>("dirty_token"),
                    row.get::<i32, _>("update_token"),
                    normalized,
                ])
            })
            .collect(),
    )
}

async fn dump_uploads(pool: &PgPool) -> Value {
    let rows = sqlx::query("SELECT id, state FROM lsif_uploads ORDER BY id")
        .fetch_all(pool)
        .await
        .expect("query uploads");

    Value::Array(
        rows.into_iter()
            .map(|row| json!([row.get::<i32, _>("id"), row.get::<String, _>("state"),]))
            .collect(),
    )
}

async fn dump_audit_logs(pool: &PgPool) -> Value {
    let rows = sqlx::query(
        "SELECT upload_id, operation::text AS operation, reason, \
                transition_columns::text AS transition_columns \
         FROM lsif_uploads_audit_logs \
         ORDER BY upload_id, operation::text, reason, transition_columns::text",
    )
    .fetch_all(pool)
    .await
    .expect("query audit logs");

    Value::Array(
        rows.into_iter()
            .map(|row| {
                json!([
                    row.get::<i32, _>("upload_id"),
                    row.get::<String, _>("operation"),
                    row.get::<String, _>("reason"),
                    row.get::<Option<String>, _>("transition_columns")
                        .unwrap_or_default(),
                ])
            })
            .collect(),
    )
}
