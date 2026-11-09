#![forbid(unsafe_code)]

//! M9.8di differential harness (Rust side): `Store.InsertPackageRepoRefs`
//! (`internal/codeintel/dependencies/internal/store/store.go`).
//!
//! Mirrors `internal/codeintel/dependencies/internal/store/insert_harness_test.go`.
//! `test_ws/p9-go/run_codeintel_dependencies_insert_diff.sh` applies
//! `codeintel_dependencies_insert_fixtures.sql` immediately before each side
//! and diffs the canonical `cpi ` lines: the returned new refs/versions plus
//! the full final state of both tables, sorted by id. Timestamps are fixed
//! inputs printed as RFC3339 UTC, so the output is deterministic.

use serde::Serialize;
use sg_db::codeintel_dependencies::{
    insert_package_repo_refs, MinimalPackageRepoRef, MinimalPackageRepoRefVersion,
};
use sg_db::OffsetDateTime;
use sqlx::postgres::PgPool;
use sqlx::Row;
use time::format_description::well_known::Rfc3339;
use time::UtcOffset;

#[derive(Serialize)]
struct Snapshot {
    case: &'static str,
    error: String,
    new_deps: Vec<String>,
    new_versions: Vec<String>,
    state_refs: Vec<String>,
    state_versions: Vec<String>,
}

fn time(raw: &str) -> OffsetDateTime {
    OffsetDateTime::parse(raw, &Rfc3339).unwrap_or_else(|err| panic!("parse time {raw:?}: {err}"))
}

fn format_time(value: Option<OffsetDateTime>) -> String {
    match value {
        Some(value) => value
            .to_offset(UtcOffset::UTC)
            .format(&Rfc3339)
            .expect("format time"),
        None => "-".to_owned(),
    }
}

fn package(
    scheme: &str,
    name: &str,
    blocked: bool,
    last_checked_at: Option<OffsetDateTime>,
) -> MinimalPackageRepoRef {
    MinimalPackageRepoRef {
        scheme: scheme.to_owned(),
        name: name.to_owned(),
        versions: Vec::new(),
        blocked,
        last_checked_at,
    }
}

fn with_versions(
    mut package: MinimalPackageRepoRef,
    versions: Vec<MinimalPackageRepoRefVersion>,
) -> MinimalPackageRepoRef {
    package.versions = versions;
    package
}

fn version(
    version: &str,
    blocked: bool,
    last_checked_at: Option<OffsetDateTime>,
) -> MinimalPackageRepoRefVersion {
    MinimalPackageRepoRefVersion {
        version: version.to_owned(),
        blocked,
        last_checked_at,
    }
}

async fn state_refs(pool: &PgPool) -> Vec<String> {
    let rows = sqlx::query(
        "SELECT id, scheme, name, blocked, last_checked_at \
         FROM lsif_dependency_repos ORDER BY id",
    )
    .fetch_all(pool)
    .await
    .expect("query refs");

    rows.into_iter()
        .map(|row| {
            let id: i64 = row.try_get("id").expect("ref id");
            let scheme: String = row.try_get("scheme").expect("ref scheme");
            let name: String = row.try_get("name").expect("ref name");
            let blocked: bool = row.try_get("blocked").expect("ref blocked");
            let checked: Option<OffsetDateTime> =
                row.try_get("last_checked_at").expect("ref checked");
            format!(
                "{}|{}|{}|blocked={}|checked={}",
                id,
                scheme,
                name,
                blocked,
                format_time(checked)
            )
        })
        .collect()
}

async fn state_versions(pool: &PgPool) -> Vec<String> {
    let rows = sqlx::query(
        "SELECT id, package_id, version, blocked, last_checked_at \
         FROM package_repo_versions ORDER BY id",
    )
    .fetch_all(pool)
    .await
    .expect("query versions");

    rows.into_iter()
        .map(|row| {
            let id: i64 = row.try_get("id").expect("version id");
            let package_id: i64 = row.try_get("package_id").expect("version package_id");
            let version: String = row.try_get("version").expect("version text");
            let blocked: bool = row.try_get("blocked").expect("version blocked");
            let checked: Option<OffsetDateTime> =
                row.try_get("last_checked_at").expect("version checked");
            format!(
                "{}|ref={}|{}|blocked={}|checked={}",
                id,
                package_id,
                version,
                blocked,
                format_time(checked)
            )
        })
        .collect()
}

struct Case {
    name: &'static str,
    deps: Vec<MinimalPackageRepoRef>,
}

#[tokio::test]
async fn dependencies_insert_matches_go_harness() {
    if std::env::var("SG_CODEINTEL_DEPENDENCIES_INSERT_DIFF")
        .ok()
        .as_deref()
        != Some("1")
    {
        return;
    }
    let dsn = std::env::var("SG_CODEINTEL_DEPENDENCIES_INSERT_TEST_DSN")
        .expect("SG_CODEINTEL_DEPENDENCIES_INSERT_TEST_DSN is not set");
    let pool = PgPool::connect(&dsn).await.expect("connect database");

    let t1 = time("2021-01-01T00:00:00Z");
    let t2 = time("2021-02-02T00:00:00Z");
    let t3 = time("2021-03-03T00:00:00Z");

    let cases = vec![
        Case {
            name: "single-new",
            deps: vec![with_versions(
                package("semanticdb", "com.example:lib", false, Some(t1)),
                vec![version("1.0.0", false, Some(t1))],
            )],
        },
        Case {
            name: "multi-version",
            deps: vec![with_versions(
                package("npm", "left-pad", false, Some(t1)),
                vec![
                    version("1.0.0", false, Some(t1)),
                    version("1.1.0", true, Some(t2)),
                    version("2.0.0", false, None),
                ],
            )],
        },
        Case {
            name: "duplicate-input-merged",
            deps: vec![
                with_versions(
                    package("npm", "dup", false, Some(t1)),
                    vec![version("1.0.0", false, Some(t1))],
                ),
                with_versions(
                    package("npm", "dup", false, Some(t2)),
                    vec![version("2.0.0", false, Some(t2))],
                ),
            ],
        },
        Case {
            name: "existing-ref-new-version",
            deps: vec![with_versions(
                package("npm", "existing-pkg", false, Some(t2)),
                vec![version("3.0.0", false, Some(t2))],
            )],
        },
        Case {
            name: "existing-ref-existing-version",
            deps: vec![with_versions(
                package("npm", "existing-pkg", true, Some(t3)),
                vec![version("1.0.0", true, Some(t3))],
            )],
        },
        Case {
            name: "existing-blocked-untouched",
            deps: vec![with_versions(
                package("semanticdb", "existing-pkg", false, Some(t3)),
                vec![version("2.0.0", false, Some(t3))],
            )],
        },
        Case {
            name: "empty-input",
            deps: Vec::new(),
        },
        Case {
            name: "empty-fields-dropped",
            deps: vec![
                with_versions(
                    package("", "x", false, Some(t1)),
                    vec![version("1.0.0", false, Some(t1))],
                ),
                with_versions(
                    package("npm", "", false, Some(t1)),
                    vec![version("1.0.0", false, Some(t1))],
                ),
            ],
        },
        Case {
            name: "mixed-schemes-same-name",
            deps: vec![
                with_versions(
                    package("npm", "same", false, Some(t1)),
                    vec![version("1.0.0", false, Some(t1))],
                ),
                with_versions(
                    package("semanticdb", "same", false, Some(t1)),
                    vec![version("1.0.0", false, Some(t1))],
                ),
            ],
        },
        Case {
            name: "multiple-refs-unsorted-input",
            deps: vec![
                with_versions(
                    package("npm", "zebra", false, Some(t1)),
                    vec![version("1.0.0", false, Some(t1))],
                ),
                with_versions(
                    package("npm", "apple", false, Some(t1)),
                    vec![version("1.0.0", false, Some(t1))],
                ),
                with_versions(
                    package("semanticdb", "mango", false, Some(t1)),
                    vec![version("1.0.0", false, Some(t1))],
                ),
            ],
        },
        Case {
            name: "null-timestamps",
            deps: vec![with_versions(
                package("npm", "nulltime", false, None),
                vec![version("1.0.0", false, None)],
            )],
        },
        Case {
            name: "blocked-ref-and-version",
            deps: vec![with_versions(
                package("npm", "blocked-pkg", true, Some(t2)),
                vec![version("1.0.0", true, Some(t2))],
            )],
        },
        Case {
            name: "same-version-twice",
            deps: vec![with_versions(
                package("npm", "dupver", false, Some(t1)),
                vec![
                    version("1.0.0", false, Some(t1)),
                    version("1.0.0", true, Some(t2)),
                ],
            )],
        },
        Case {
            name: "existing-ref-new-version-other-scheme",
            deps: vec![with_versions(
                package("semanticdb", "existing-pkg", true, Some(t2)),
                vec![version("9.9.9", false, Some(t2))],
            )],
        },
        Case {
            name: "empty-versions",
            deps: vec![with_versions(
                package("npm", "no-versions", false, Some(t1)),
                Vec::new(),
            )],
        },
        Case {
            name: "repeat-first-case",
            deps: vec![with_versions(
                package("semanticdb", "com.example:lib", false, Some(t1)),
                vec![version("1.0.0", false, Some(t1))],
            )],
        },
        Case {
            name: "partially-existing-versions",
            deps: vec![with_versions(
                package("npm", "existing-pkg", false, Some(t3)),
                vec![
                    version("2.0.0", false, Some(t3)),
                    version("4.0.0", false, Some(t3)),
                    version("5.0.0", true, None),
                ],
            )],
        },
    ];

    assert_eq!(cases.len(), 17);

    for case in &cases {
        let result = insert_package_repo_refs(&pool, &case.deps).await;
        let (new_deps, new_versions) = match result {
            Ok(value) => value,
            Err(err) => panic!("case {}: {err}", case.name),
        };

        let mut new_deps: Vec<String> = new_deps
            .iter()
            .map(|dep| {
                format!(
                    "{}|{}|{}|blocked={}|checked={}",
                    dep.id,
                    dep.scheme,
                    dep.name,
                    dep.blocked,
                    format_time(dep.last_checked_at)
                )
            })
            .collect();
        new_deps.sort();

        let mut new_versions: Vec<String> = new_versions
            .iter()
            .map(|version| {
                format!(
                    "{}|ref={}|{}|blocked={}|checked={}",
                    version.id,
                    version.package_ref_id,
                    version.version,
                    version.blocked,
                    format_time(version.last_checked_at)
                )
            })
            .collect();
        new_versions.sort();

        let snapshot = Snapshot {
            case: case.name,
            error: "-".to_owned(),
            new_deps,
            new_versions,
            state_refs: state_refs(&pool).await,
            state_versions: state_versions(&pool).await,
        };
        println!(
            "cpi {}",
            serde_json::to_string(&snapshot).expect("json snapshot")
        );
    }
}
