#![forbid(unsafe_code)]

//! M9.8f differential harness (Rust side): the `gitserver-metrics` job.
//!
//! Expects `test_ws/p9-go/gitserver_metrics_fixtures.sql` to have been applied
//! to `SG_QTEST_DSN` immediately before the run. The Go counterpart is
//! `cmd/worker/internal/gitserver/harness_test.go`: follow the same order:
//! construct the job (`routines`), then scrape the process registry.
//!
//! The gauge callbacks block the async runtime (`block_in_place`), so the test
//! runs on the multi-threaded runtime exactly like the worker service.

use std::collections::HashMap;
use std::sync::Arc;

use sg_config::{LiveConfig, Unified};
use sg_service_worker::jobs::gitserver::GitserverMetrics;
use sg_service_worker::jobs::{Job, JobDeps};
use tokio_util::sync::CancellationToken;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn gitserver_metrics_matches_go_harness() {
    let Ok(dsn) = std::env::var("SG_QTEST_DSN") else {
        return;
    };

    let deps = JobDeps {
        frontend: sg_db::connect(&dsn, 2).await.expect("connect database"),
        live_config: Arc::new(LiveConfig::new(Unified::default())),
        keyring: Arc::new(sg_keyring::Keyring::default()),
    };

    let routines = GitserverMetrics
        .routines(&deps, CancellationToken::new())
        .await
        .expect("routines");
    println!("routines={}", routines.len());

    let mut values = HashMap::new();
    for family in sg_metrics::registry().gather() {
        let name = family.get_name().to_owned();
        if name != "src_gitserver_repo_last_error_total" && name != "src_gitserver_repo_count" {
            continue;
        }
        values.insert(name, family.get_metric()[0].get_gauge().get_value());
    }

    println!(
        "src_gitserver_repo_last_error_total={}",
        values["src_gitserver_repo_last_error_total"] as i64
    );
    println!(
        "src_gitserver_repo_count={}",
        values["src_gitserver_repo_count"] as i64
    );
}
