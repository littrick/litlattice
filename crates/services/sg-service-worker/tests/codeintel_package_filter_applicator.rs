#![forbid(unsafe_code)]

//! M9.8t differential harness (Rust side): the
//! `codeintel-package-filter-applicator` job.
//!
//! Mirrors
//! `internal/codeintel/dependencies/internal/background/job_packages_filter_harness_test.go`.
//! `test_ws/p9-go/run_codeintel_package_filter_applicator_diff.sh` resets a
//! scratch frontend database, applies the shared fixture immediately before
//! each side, and diffs the canonical lines:
//!
//! * `jroutine`: the routine metadata;
//! * `gmatch`: gobwas/glob match results for a fixed pattern/input table
//!   (pinning the Rust port of `GlobToRegex` + `glob.Compile(...).Match`);
//! * `hneeds`/`happly`: `ShouldRefilterPackageRepoRefs` before each of two
//!   `handle` invocations and their outcomes;
//! * `pkg`/`ver`: every package reference and version with its blocked status;
//! * `esvc`/`ch`: the package external services marked for syncing and the
//!   code hosts they were attached to.

use std::sync::Arc;

use sg_codeintel::packagefilters::CompiledGlob;
use sg_config::{LiveConfig, Unified};
use sg_service_worker::framework::BackgroundRoutine;
use sg_service_worker::jobs::codeintel_package_filter_applicator::{
    apply_package_filters, PackageFilterApplicator, PACKAGE_KINDS,
};
use sg_service_worker::jobs::{Job, JobDeps};
use sqlx::postgres::PgPool;
use sqlx::Row;
use tokio_util::sync::CancellationToken;

use sg_db::codeintel_dependencies;

/// Same table as the Go harness's `packageFilterGlobTable`.
const GLOB_TABLE: &[(&str, &str)] = &[
    ("com.sourcegraph.*", "com.sourcegraph.foo"),
    ("com.sourcegraph.*", "com.sourcegraph.foo.bar"),
    ("com.sourcegraph.*", "com.othergraph.foo"),
    ("com.sourcegraph.**", "com.sourcegraph.foo.bar"),
    ("ba*", "bar"),
    ("ba*", "ba"),
    ("ba*", "banana"),
    ("ba*", "abc"),
    ("1.0.[!1]", "1.0.0"),
    ("1.0.[!1]", "1.0.1"),
    ("1.0.[!1]", "1.0.2"),
    ("3*", "3.0.0"),
    ("3*", "4.1.2"),
    ("b?n?n?", "banana"),
    ("b?n?n?", "banan"),
    ("{asd,abc}f", "abcf"),
    ("{asd,abc}f", "asdff"),
    ("asdf[!!]", "asdf!"),
    ("asdf[!!]", "asdfx"),
    ("xyz[a-w]", "xyzm"),
    ("xyz[a-w]", "xyzz"),
    ("*", ""),
    ("*", "*"),
    ("asdf", "asdf"),
    ("asdf", "asdfx"),
    ("!fdsa]", "!fdsa]"),
    (r"[\d]", "d"),
    (r"[\d]", r"\d"),
    (r"\*", "*"),
];

#[tokio::test(flavor = "multi_thread")]
async fn package_filter_applicator_matches_go_harness() {
    if std::env::var("SG_PKGFILTER_HARNESS_ON").ok().as_deref() != Some("1") {
        eprintln!("SG_PKGFILTER_HARNESS_ON is not set");
        return;
    }

    let dsn = std::env::var("SG_PKGFILTER_DSN").expect("SG_PKGFILTER_DSN is not set");
    let pool = PgPool::connect(&dsn).await.expect("connect");

    // 1. Routine metadata.
    let job = PackageFilterApplicator::new();
    let deps = JobDeps {
        frontend: pool.clone(),
        live_config: Arc::new(LiveConfig::new(Unified::default())),
        keyring: Arc::new(sg_keyring::Keyring::default()),
    };
    let routines = job
        .routines(&deps, CancellationToken::new())
        .await
        .expect("routines");
    let routine: &BackgroundRoutine = &routines[0];
    println!("jroutine name={}", routine.name());
    println!("jroutine description={}", routine.description());
    println!("jroutine interval_ms={}", routine.interval().as_millis());
    let shared = Arc::new(routine.clone());
    println!(
        "jroutine type={}",
        shared
            .recordable()
            .expect("periodic routine")
            .routine_type()
            .as_str()
    );

    // 2. Glob semantics.
    for (pattern, input) in GLOB_TABLE {
        let compiled = CompiledGlob::compile(pattern).expect("valid glob");
        println!(
            "gmatch pattern={pattern:?} input={input:?} match={}",
            compiled.is_match(input)
        );
    }

    // 3./4. Two applicator passes. The second must observe `needs=false`
    // because the first one stamped `last_checked_at` on every row.
    for pass in 1..=2 {
        let needs = codeintel_dependencies::should_refilter_package_repo_refs(&pool)
            .await
            .expect("should refilter");
        println!("hneeds pass={pass} needs={needs}");

        match apply_package_filters(&pool).await {
            Ok(_outcome) => println!("happly pass={pass} err=ok"),
            Err(err) => println!("happly pass={pass} err={err}"),
        }
    }

    // 5. The resulting blocked statuses.
    let packages = codeintel_dependencies::list_package_repo_refs(&pool, 0, 1000)
        .await
        .expect("list refs");
    for package in &packages {
        println!(
            "pkg id={} scheme={} name={} blocked={}",
            package.id, package.scheme, package.name, package.blocked
        );
        for version in &package.versions {
            println!(
                "ver id={} pkg={} version={} blocked={}",
                version.id, package.id, version.version, version.blocked
            );
        }
    }

    // 6. External services and code hosts (raw queries: `has_webhooks` is not
    // part of `sg_db::external_services::ExternalService`).
    let kinds: Vec<String> = PACKAGE_KINDS
        .iter()
        .map(|kind| (*kind).to_owned())
        .collect();
    let services = sqlx::query(
        r#"
SELECT id, kind, next_sync_at IS NOT NULL AS next_sync_set,
       code_host_id IS NOT NULL AS code_host_set, unrestricted,
       COALESCE(has_webhooks::text, 'null') AS has_webhooks
FROM external_services
WHERE deleted_at IS NULL AND kind = ANY($1)
ORDER BY id
"#,
    )
    .bind(&kinds)
    .fetch_all(&pool)
    .await
    .expect("list external services");
    for row in services {
        println!(
            "esvc id={} kind={} next_sync_at_set={} code_host_id_set={} unrestricted={} has_webhooks={}",
            row.get::<i64, _>("id"),
            row.get::<String, _>("kind"),
            row.get::<bool, _>("next_sync_set"),
            row.get::<bool, _>("code_host_set"),
            row.get::<bool, _>("unrestricted"),
            row.get::<String, _>("has_webhooks"),
        );
    }

    let code_hosts = sqlx::query(
        r#"
SELECT kind, url FROM code_hosts WHERE url = ANY($1) ORDER BY url
"#,
    )
    .bind(&kinds)
    .fetch_all(&pool)
    .await
    .expect("list code hosts");
    for row in code_hosts {
        println!(
            "ch kind={} url={}",
            row.get::<String, _>("kind"),
            row.get::<String, _>("url")
        );
    }
}
