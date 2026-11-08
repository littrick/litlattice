#![forbid(unsafe_code)]

//! M9.8b differential harness (Rust side): the worker's job framework
//! aggregation.
//!
//! Drives `validate_configs` and `create_background_routines` with fake jobs
//! and prints the resulting error strings in the same canonical lines as the
//! Go counterpart (`cmd/worker/shared/harness_test.go`);
//! `test_ws/p9-go/run_webhook_janitor_diff.sh` compares them.

use std::sync::Arc;

use async_trait::async_trait;
use sg_config::LiveConfig;
use sg_core::{ConfigError, EnvReader};
use sg_service_worker::config::WorkerConfigs;
use sg_service_worker::framework::periodic::PeriodicRoutine;
use sg_service_worker::jobs::{
    create_background_routines, validate_configs, Job, JobConfig, JobDeps,
};
use tokio_util::sync::CancellationToken;

struct HarnessJob {
    err: Option<String>,
    configs: Vec<Arc<dyn JobConfig>>,
}

#[async_trait]
impl Job for HarnessJob {
    fn configs(&self) -> Vec<Arc<dyn JobConfig>> {
        self.configs.clone()
    }

    async fn routines(
        &self,
        _deps: &JobDeps,
        _setup_ctx: CancellationToken,
    ) -> Result<Vec<PeriodicRoutine>, String> {
        match &self.err {
            Some(err) => Err(err.clone()),
            None => Ok(Vec::new()),
        }
    }
}

struct HarnessConfig;

impl JobConfig for HarnessConfig {
    fn load(&self, _env: &mut EnvReader) {}

    fn validate(&self) -> Result<(), ConfigError> {
        Err(ConfigError::new("bad config"))
    }
}

fn config_with_allowlist(allowlist: &str) -> WorkerConfigs {
    std::env::set_var("WORKER_JOB_ALLOWLIST", allowlist);
    std::env::remove_var("WORKER_JOB_BLOCKLIST");
    let mut env = EnvReader::new();
    WorkerConfigs::load(&mut env, None)
}

fn deps() -> JobDeps {
    JobDeps {
        frontend: sqlx::postgres::PgPoolOptions::new()
            .connect_lazy("postgres://unused@localhost/unused")
            .expect("lazy pool"),
        live_config: Arc::new(LiveConfig::new(sg_config::Unified::default())),
    }
}

#[tokio::test]
async fn worker_framework_matches_go_harness() {
    let failing = vec![
        (
            "a-job",
            Arc::new(HarnessJob {
                err: Some("boom".to_owned()),
                configs: Vec::new(),
            }) as Arc<dyn Job>,
        ),
        (
            "b-job",
            Arc::new(HarnessJob {
                err: Some("boom".to_owned()),
                configs: Vec::new(),
            }),
        ),
    ];

    match create_background_routines(&deps(), &failing, &config_with_allowlist("all")).await {
        Ok(_) => println!("create_error nil"),
        Err(err) => println!("create_error {}", escape(&err)),
    }

    let ok: Vec<(&'static str, Arc<dyn Job>)> = vec![(
        "a-job",
        Arc::new(HarnessJob {
            err: None,
            configs: Vec::new(),
        }),
    )];
    let routines = create_background_routines(&deps(), &ok, &config_with_allowlist("all")).await;
    match routines {
        Ok(routines) => println!("create_ok routines={} err=<nil>", routines.len()),
        Err(err) => println!("create_ok routines=0 err={}", escape(&err)),
    }

    let with_config: Vec<(&'static str, Arc<dyn Job>)> = vec![(
        "z-job",
        Arc::new(HarnessJob {
            err: None,
            configs: vec![Arc::new(HarnessConfig) as Arc<dyn JobConfig>],
        }),
    )];
    match validate_configs(&with_config, &config_with_allowlist("all")) {
        Some(err) => println!("validate_error {}", escape(&err)),
        None => println!("validate_error nil"),
    }

    match create_background_routines(&deps(), &failing, &config_with_allowlist("other-job")).await {
        Ok(routines) => println!("create_disabled routines={} err=<nil>", routines.len()),
        Err(err) => println!("create_disabled routines=0 err={}", escape(&err)),
    }
}

/// Keep multi-line error messages on one canonical line, like the Go harness.
fn escape(message: &str) -> String {
    message.replace('\n', "\\n").replace('\t', "\\t")
}
