#![forbid(unsafe_code)]

//! Differential harness for the `insights-job` assembly (M9.8cg).
//!
//! Mirror of the Go harness
//! `cmd/worker/internal/insights/parity_job_harness_test.go`: the same corpus
//! scripts `DISABLE_CODE_INSIGHTS` and `DISABLE_CODE_INSIGHTS_HISTORICAL`;
//! both sides print the canonical `ij ` lines (job description, routine
//! count, and each routine's name/description/interval) from the real job.
//!
//! Gated by `SG_INSIGHTS_JOB_CORPUS`; the codeinsights connection comes from
//! `SG_QTEST_INSIGHTS_DSN`. The frontend pool is lazy: the harness never runs
//! a handler.

use std::sync::Arc;

use tokio_util::sync::CancellationToken;

use sg_config::{LiveConfig, ServiceConnections, SiteConfiguration, Unified};
use sg_service_worker::jobs::insights_job::{InsightsJob, HISTORICAL_ENV};
use sg_service_worker::jobs::{Job, JobDeps};

fn deps(insights_dsn: &str) -> JobDeps {
    JobDeps {
        frontend: sqlx::postgres::PgPoolOptions::new()
            .max_connections(2)
            .connect_lazy("postgres://unused@localhost/unused")
            .expect("lazy pool"),
        live_config: Arc::new(LiveConfig::new(Unified {
            site: SiteConfiguration::default(),
            service_connections: ServiceConnections {
                code_insights_dsn: insights_dsn.to_owned(),
                ..ServiceConnections::default()
            },
        })),
        keyring: Arc::new(sg_keyring::Keyring::default()),
    }
}

#[tokio::test]
async fn insights_job_differential() {
    let corpus_path = match std::env::var("SG_INSIGHTS_JOB_CORPUS") {
        Ok(path) if !path.is_empty() => path,
        _ => return,
    };
    let insights_dsn = std::env::var("SG_QTEST_INSIGHTS_DSN").expect("insights dsn");

    let job = InsightsJob;
    println!("ij job description={:?}", job.description());

    let corpus = std::fs::read_to_string(&corpus_path).expect("read corpus");
    for line in corpus.lines() {
        if line.is_empty() {
            continue;
        }
        let parts: Vec<&str> = line.split('\t').collect();
        let op = parts[0];
        let label = parts[1];

        match op {
            "disabled" => std::env::set_var("DISABLE_CODE_INSIGHTS", "true"),
            "enabled" => std::env::remove_var("DISABLE_CODE_INSIGHTS"),
            other => panic!("unknown op {other:?}"),
        }
        std::env::set_var(HISTORICAL_ENV, "true");

        let routines = match job
            .routines(&deps(&insights_dsn), CancellationToken::new())
            .await
        {
            Ok(routines) => routines,
            Err(_) => {
                println!("ij {label} result=error");
                continue;
            }
        };
        println!("ij {label} result=ok routines={}", routines.len());
        for routine in &routines {
            println!(
                "ij {label} routine name={:?} description={:?} interval={}",
                routine.name(),
                routine.description(),
                routine.interval().as_secs()
            );
        }
    }

    std::env::remove_var("DISABLE_CODE_INSIGHTS");
    std::env::remove_var(HISTORICAL_ENV);
}
