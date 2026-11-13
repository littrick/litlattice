//! Differential harness for the M9.23b production derived-state loader.
//!
//! Reads the shared corpus at `M923B_CORPUS` and the database at
//! `SG_QTEST_DSN`, runs
//! [`sg_extsvc::batches_state_load::refresh_derived_state`] over the fixture
//! rows (which also persists the derived columns), and writes the normalized
//! results to `M923B_OUT`. The untracked Go oracle
//! (`internal/batches/state/zz_m923b_state_production_oracle_test.go`) loads
//! the same rows through the real batches store, calls `state.SetDerivedState`
//! and `state.ComputeLabels`, and writes the same shape.

use serde::{Deserialize, Serialize};
use sqlx::postgres::PgPoolOptions;
use sqlx::Row;

use sg_extsvc::batches_state::{DiffStat, GitserverClient};
use sg_extsvc::batches_state_load::refresh_derived_state;

#[derive(Deserialize)]
struct Corpus {
    cases: Vec<Case>,
}

#[derive(Deserialize)]
struct Case {
    name: String,
    changeset_id: i64,
}

#[derive(Serialize)]
struct OutCase {
    name: String,
    external_state: String,
    review_state: String,
    check_state: String,
    base_ref_oid: String,
    head_ref_oid: String,
    is_complete: bool,
    diff_stat: Option<OutDiff>,
    labels: Vec<OutLabel>,
}

#[derive(Serialize)]
struct OutDiff {
    added: i32,
    deleted: i32,
}

#[derive(Serialize)]
struct OutLabel {
    name: String,
    color: String,
    description: String,
}

struct FakeGitserver;

impl GitserverClient for FakeGitserver {
    fn resolve_revision(&self, _repo: &str, rev: &str) -> Result<String, String> {
        Ok(format!("rev:{rev}"))
    }

    fn diff_stat(&self, _repo: &str, _base: &str, _head: &str) -> Result<DiffStat, String> {
        Ok(DiffStat {
            added: 2,
            deleted: 1,
        })
    }
}

#[tokio::test(flavor = "current_thread")]
async fn batches_state_production_diff() {
    let Ok(corpus_path) = std::env::var("M923B_CORPUS") else {
        eprintln!("M923B_CORPUS unset; skipping");
        return;
    };
    let out_path = std::env::var("M923B_OUT").expect("M923B_OUT");
    let dsn = std::env::var("SG_QTEST_DSN").expect("SG_QTEST_DSN");

    let corpus: Corpus = serde_json::from_str(
        &std::fs::read_to_string(&corpus_path).expect("read corpus"),
    )
    .expect("decode corpus");

    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&dsn)
        .await
        .expect("connect database");

    let mut out = Vec::with_capacity(corpus.cases.len());
    for case in &corpus.cases {
        let mut conn = pool.acquire().await.expect("acquire");
        let derived = refresh_derived_state(&mut conn, case.changeset_id, Some(&FakeGitserver))
            .await
            .expect("refresh derived state")
            .unwrap_or_else(|| panic!("{}: changeset {} not found", case.name, case.changeset_id));
        drop(conn);

        // The loader persists the derived columns; verify the row reflects the
        // returned state so the differential covers the write side too.
        let row = sqlx::query(
            r#"
SELECT external_state, external_review_state, external_check_state,
       diff_stat_added, diff_stat_deleted, sync_state
FROM changesets WHERE id = $1
"#,
        )
        .bind(case.changeset_id)
        .fetch_one(&pool)
        .await
        .expect("read persisted row");
        let persisted_state: Option<String> = row.get("external_state");
        let persisted_review: Option<String> = row.get("external_review_state");
        let persisted_check: Option<String> = row.get("external_check_state");
        let persisted_added: Option<i32> = row.get("diff_stat_added");
        let persisted_deleted: Option<i32> = row.get("diff_stat_deleted");
        assert_eq!(
            persisted_state,
            derived.external_state.map(|s| s.as_db().to_owned()),
            "{}: persisted external_state",
            case.name
        );
        assert_eq!(
            persisted_review,
            derived.external_review_state.map(|s| s.as_db().to_owned()),
            "{}: persisted external_review_state",
            case.name
        );
        assert_eq!(
            persisted_check,
            derived.external_check_state.map(|s| s.as_db().to_owned()),
            "{}: persisted external_check_state",
            case.name
        );
        assert_eq!(
            (persisted_added, persisted_deleted),
            (
                derived.diff_stat.map(|s| s.added),
                derived.diff_stat.map(|s| s.deleted)
            ),
            "{}: persisted diff_stat",
            case.name
        );

        out.push(OutCase {
            name: case.name.clone(),
            external_state: derived
                .external_state
                .map(|s| s.as_db().to_owned())
                .unwrap_or_default(),
            review_state: derived
                .external_review_state
                .map(|s| s.as_db().to_owned())
                .unwrap_or_default(),
            check_state: derived
                .external_check_state
                .map(|s| s.as_db().to_owned())
                .unwrap_or_default(),
            base_ref_oid: derived.sync_state.base_ref_oid.clone(),
            head_ref_oid: derived.sync_state.head_ref_oid.clone(),
            is_complete: derived.sync_state.is_complete,
            diff_stat: derived.diff_stat.map(|stat| OutDiff {
                added: stat.added,
                deleted: stat.deleted,
            }),
            labels: derived
                .labels
                .iter()
                .map(|label| OutLabel {
                    name: label.name.clone(),
                    color: label.color.clone(),
                    description: label.description.clone(),
                })
                .collect(),
        });
    }

    let encoded = serde_json::to_string_pretty(&out).expect("encode output");
    std::fs::write(&out_path, format!("{encoded}\n")).expect("write output");
    eprintln!("m923b cases={}", out.len());
}
