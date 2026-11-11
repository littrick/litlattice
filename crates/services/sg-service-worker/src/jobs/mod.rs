#![forbid(unsafe_code)]

//! `cmd/worker/job` + the routine half of `cmd/worker/shared/main.go`.
//!
//! [`Job`] is the baseline's `workerjob.Job`: a job owns zero or more
//! [`JobConfig`]s (loaded unconditionally at startup, validated only when the
//! job is enabled) and constructs its periodic routines.
//!
//! The aggregation semantics reproduce `loadConfigs`, `validateConfigs`,
//! `createBackgroundRoutines` and `emitJobCountMetrics`, including the two
//! distinct error formats:
//!
//! * `Failed to load configuration:\n  - <job>: <err> ` (note the trailing
//!   space, sorted);
//! * `Failed to initialize worker:\n  - <job>: <err>`.
//!
//! Jobs that are not ported yet return an explicit error from `routines`, so
//! enabling one fails startup loudly instead of silently doing nothing. With
//! the default allowlist (`all`) the Rust worker therefore refuses to start
//! until every job group is ported; pilots narrow `WORKER_JOB_ALLOWLIST` to
//! the ported jobs (see docs/modules/06_worker.md §14).

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use sg_config::LiveConfig;
use sg_core::{ConfigError, EnvReader};
use sg_metrics::prometheus::{IntGaugeVec, Opts};
use sqlx::PgPool;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

use crate::config::WorkerConfigs;
use crate::framework::BackgroundRoutine;
use crate::registry;

pub mod adminanalytics;
pub mod auth_sourcegraph_operator_cleaner;
pub mod authz_azuredevops;
pub mod authz_azuredevops_client;
pub mod authz_bitbucketcloud;
pub mod authz_bitbucketcloud_client;
pub mod authz_gerrit;
pub mod authz_gerrit_client;
pub mod authz_github;
pub mod authz_github_client;
pub mod authz_github_user;
pub mod authz_gitlab;
pub mod authz_gitlab_client;
pub mod authz_providers;
pub mod autoindex_config;
pub mod batches;
pub mod batches_scheduler;
pub mod bitbucket_project_permissions;
pub mod bitbucket_server_client;
pub mod code_monitor_action_runner;
pub mod code_monitor_actions;
pub mod code_monitor_email;
pub mod code_monitor_workers;
pub mod code_monitors;
pub mod codehost_version_syncing;
pub mod codeintel_autoindexing_dependency_indexing_scheduler;
pub mod codeintel_autoindexing_dependency_scheduler;
pub mod codeintel_autoindexing_dependency_sync_scheduler;
pub mod codeintel_autoindexing_enqueuer;
pub mod codeintel_autoindexing_inference;
pub mod codeintel_autoindexing_inference_production;
pub mod codeintel_autoindexing_jobselector;
pub mod codeintel_autoindexing_package_inference;
pub mod codeintel_autoindexing_path_patterns;
pub mod codeintel_autoindexing_scheduler;
pub mod codeintel_dependency_jobs;
pub mod codeintel_metrics_reporter;
pub mod codeintel_package_filter_applicator;
pub mod codeintel_policies_repository_matcher;
pub mod codeintel_ranking_file_reference_counter;
pub mod codeintel_syntactic_indexing_scheduler;
pub mod codeintel_upload_backfiller;
pub mod codeintel_upload_expirer;
pub mod codeintel_upload_janitor;
pub mod codeintel_uploadstore_expirer;
pub mod codygateway;
pub mod completions;
pub mod encryption;
pub mod eventlogs;
pub mod executors;
pub mod executors_metrics;
pub mod executors_multiqueue_metrics;
pub mod exhaustive_search_janitor;
pub mod exhaustive_search_job;
pub mod exhaustive_search_searcher;
pub mod exhaustive_search_service;
pub mod exhaustive_search_workers;
pub mod github_apps_installation;
pub mod gitserver;
pub mod insights_backfill_handler;
pub mod insights_backfill_inprogress;
pub mod insights_backfill_store;
pub mod insights_data_prune;
pub mod insights_data_retention;
pub mod insights_enqueuer;
pub mod insights_job;
pub mod insights_license_check;
pub mod insights_pings;
pub mod insights_pipeline;
pub mod insights_query_runner;
pub mod insights_scheduler;
pub mod licensecheck;
pub mod out_of_band_migrations;
pub mod outbound_webhooks;
pub mod own_repo_indexing;
pub mod perforce_changelist_mapper;
pub mod permissions;
pub mod perms_syncer;
pub mod perms_syncer_fetch;
pub mod perms_syncer_job;
pub mod policies_matcher;
pub mod ratelimit;
pub mod repo_embedding_janitor;
pub mod repo_embedding_scheduler;
pub mod repostatistics;
pub mod sourcegraph_accounts_notifications;
pub mod telemetry_gateway_exporter;
pub mod usage_telemetry;
pub mod users;
pub mod webhooks;
pub mod zoektrepos;

/// `env.Config`: a configuration struct owned by a job.
///
/// The baseline requires `Config()` to return the *same pointers* every call;
/// `Load` runs before validation and mutates them. Rust jobs therefore own
/// their configs behind `Arc` and use interior mutability.
pub trait JobConfig: Send + Sync {
    /// `env.Config.Load`.
    fn load(&self, env: &mut EnvReader);
    /// `env.Config.Validate`.
    fn validate(&self) -> Result<(), ConfigError>;
}

/// Everything a job's `routines` may need from the process. This is the Rust
/// equivalent of the baseline's package-level `workerdb.Get()` /
/// `conf.Get()`; passing it explicitly keeps jobs testable.
#[derive(Clone)]
pub struct JobDeps {
    /// `workerdb.InitDB`'s frontend database connection.
    pub frontend: PgPool,
    /// `conf.Get()`'s live configuration.
    pub live_config: Arc<LiveConfig>,
    /// `keyring.Init`'s ring; `database.ExternalServices` reads
    /// `keyring.Default().ExternalServiceKey` when it decrypts a config.
    pub keyring: Arc<sg_keyring::Keyring>,
}

/// `BaseConfig.GetInterval`: parse `time.ParseDuration`, record
/// `invalid duration %q for %s: %s` and return zero on failure. Shared by
/// every job whose config declares an interval.
pub(crate) fn get_interval(
    env: &mut EnvReader,
    name: &str,
    default: &str,
    description: &str,
    errors: &mut Vec<String>,
) -> std::time::Duration {
    let raw = env.get(name, default, description);
    match sg_core::parse_go_duration(&raw) {
        Ok(duration) => duration,
        Err(message) => {
            errors.push(format!("invalid duration {raw:?} for {name}: {message}"));
            std::time::Duration::ZERO
        }
    }
}

/// `cmd/worker/job.Job`.
#[async_trait]
pub trait Job: Send + Sync {
    /// `Job.Description`.
    fn description(&self) -> String {
        String::new()
    }

    /// `Job.Config`.
    fn configs(&self) -> Vec<Arc<dyn JobConfig>> {
        Vec::new()
    }

    /// `Job.Routines`. `setup_ctx` is cancelled when a sibling job's setup
    /// fails (the baseline cancels every `Routines` call it started), so
    /// long-running setup should observe it.
    async fn routines(
        &self,
        deps: &JobDeps,
        setup_ctx: CancellationToken,
    ) -> Result<Vec<BackgroundRoutine>, String>;
}

/// A registry name whose job group has not been ported yet. Its `routines`
/// fails, which is what makes the missing implementation visible at startup.
pub struct UnportedJob {
    name: &'static str,
}

#[async_trait]
impl Job for UnportedJob {
    async fn routines(
        &self,
        _deps: &JobDeps,
        _setup_ctx: CancellationToken,
    ) -> Result<Vec<BackgroundRoutine>, String> {
        Err(format!(
            "job {:?} is not implemented by the Rust worker yet",
            self.name
        ))
    }
}

/// The baseline's `builtins` map, in the sorted order `jobNames` produces.
pub fn all_jobs() -> Vec<(&'static str, Arc<dyn Job>)> {
    registry::JOB_NAMES
        .iter()
        .map(|&name| {
            let job: Arc<dyn Job> = match name {
                "aggregated-users-statistics" => Arc::new(users::AggregatedUsersStatistics),
                "auth-sourcegraph-operator-cleaner" => {
                    Arc::new(auth_sourcegraph_operator_cleaner::SourcegraphOperatorCleaner::new())
                }
                "batches-janitor" => Arc::new(batches::BatchesJanitor::new()),
                "batches-scheduler" => Arc::new(batches_scheduler::BatchesScheduler::new()),
                "bitbucket-project-permissions" => {
                    Arc::new(bitbucket_project_permissions::BitbucketProjectPermissionsJob::new())
                }
                "cody-gateway-usage-check" => Arc::new(codygateway::CodyGatewayUsageCheck),
                "cody-llm-token-counter" => Arc::new(completions::CodyLlmTokenCounter),
                "event-logs-janitor" => Arc::new(eventlogs::EventLogsJanitor),
                "executors-janitor" => Arc::new(executors::ExecutorsJanitor::new()),
                "executors-metricsserver" => {
                    Arc::new(executors_metrics::ExecutorsMetricsServer::new())
                }
                "executors-multiqueue-metrics-reporter" => Arc::new(
                    executors_multiqueue_metrics::ExecutorsMultiqueueMetricsReporter::new(),
                ),
                "export-usage-telemetry" => {
                    Arc::new(usage_telemetry::ExportUsageTelemetryJob::new())
                }
                "codehost-version-syncing" => {
                    Arc::new(codehost_version_syncing::CodehostVersionSyncing::new())
                }
                "codeintel-metrics-reporter" => {
                    Arc::new(codeintel_metrics_reporter::CodeIntelMetricsReporter::new())
                }
                "codeintel-package-filter-applicator" => {
                    Arc::new(codeintel_package_filter_applicator::PackageFilterApplicator::new())
                }
                "codeintel-policies-repository-matcher" => Arc::new(
                    codeintel_policies_repository_matcher::PoliciesRepositoryMatcher::new(),
                ),
                "codeintel-autoindexing-dependency-scheduler" => Arc::new(
                    codeintel_autoindexing_dependency_scheduler::AutoindexingDependencyScheduler::new(),
                ),
                "codeintel-autoindexing-scheduler" => {
                    Arc::new(codeintel_autoindexing_scheduler::AutoindexingScheduler::new())
                }
                "codeintel-ranking-file-reference-counter" => Arc::new(
                    codeintel_ranking_file_reference_counter::RankingFileReferenceCounter::new(),
                ),
                "codeintel-syntactic-indexing-scheduler" => Arc::new(
                    codeintel_syntactic_indexing_scheduler::SyntacticIndexingScheduler::new(),
                ),
                "codeintel-upload-backfiller" => {
                    Arc::new(codeintel_upload_backfiller::UploadBackfiller::new())
                }
                "codeintel-upload-expirer" => {
                    Arc::new(codeintel_upload_expirer::UploadExpirer::new())
                }
                "codeintel-upload-janitor" => {
                    Arc::new(codeintel_upload_janitor::UploadJanitor::new())
                }
                "codeintel-uploadstore-expirer" => {
                    Arc::new(codeintel_uploadstore_expirer::UploadStoreExpirer::new())
                }
                "record-encrypter" => Arc::new(encryption::RecordEncrypterJob::new()),
                "github-apps-installation-validation-job" => {
                    Arc::new(github_apps_installation::GitHubAppsInstallationValidation::new())
                }
                "gitserver-metrics" => Arc::new(gitserver::GitserverMetrics),
                "insights-data-retention-job" => {
                    Arc::new(insights_data_retention::InsightsDataRetentionJob)
                }
                "insights-job" => Arc::new(insights_job::InsightsJob),
                "insights-query-runner-job" => {
                    Arc::new(insights_query_runner::InsightsQueryRunnerJob)
                }
                "license-check" => Arc::new(licensecheck::LicenseCheckJob),
                "out-of-band-migrations" => {
                    Arc::new(out_of_band_migrations::OutOfBandMigrationsJob::new())
                }
                "outbound-webhook-sender" => {
                    Arc::new(outbound_webhooks::OutboundWebhookSender::new())
                }
                "own-repo-indexing-queue" => {
                    Arc::new(own_repo_indexing::OwnRepoIndexingQueue::new())
                }
                "perforce-changelist-mapper" => {
                    Arc::new(perforce_changelist_mapper::PerforceChangelistMappingJob::new())
                }
                "permission-sync-job-cleaner" => Arc::new(permissions::PermissionSyncJobCleaner),
                "repo-perms-syncer" => Arc::new(perms_syncer_job::PermsSyncerJob::new()),
                "permission-sync-job-scheduler" => {
                    Arc::new(permissions::PermissionSyncJobScheduler)
                }
                "rate-limit-config" => Arc::new(ratelimit::RateLimitConfigJob::new()),
                "refresh-analytics-cache" => Arc::new(adminanalytics::RefreshAnalyticsCacheJob),
                "repo-embedding-janitor" => {
                    Arc::new(repo_embedding_janitor::RepoEmbeddingJanitor::new())
                }
                "repo-embedding-scheduler" => {
                    Arc::new(repo_embedding_scheduler::RepoEmbeddingScheduler::new())
                }
                "repo-statistics-compactor" => Arc::new(repostatistics::RepoStatisticsCompactor),
                "repo-statistics-resetter" => Arc::new(repostatistics::RepoStatisticsResetter),
                "sourcegraph-accounts-notifications-subscriber" => Arc::new(
                    sourcegraph_accounts_notifications::SourcegraphAccountsNotificationsSubscriber::new(),
                ),
                "telemetrygateway-exporter" => {
                    Arc::new(telemetry_gateway_exporter::TelemetryGatewayExporter::new())
                }
                "webhook-log-janitor" => Arc::new(webhooks::WebhookLogJanitor),
                "zoekt-repos-updater" => Arc::new(zoektrepos::ZoektReposUpdater),
                _ => Arc::new(UnportedJob { name }),
            };
            (name, job)
        })
        .collect()
}

/// `loadConfigs`'s second half: `Load` every job config, enabled or not.
///
/// The worker's own config (`WORKER_JOB_ALLOWLIST` / `WORKER_JOB_BLOCKLIST`)
/// is loaded by [`WorkerConfigs::load`] beforehand, mirroring
/// `config.Load()` inside `loadConfigs`.
pub fn load_configs(env: &mut EnvReader, jobs: &[(&'static str, Arc<dyn Job>)]) {
    for (_, job) in jobs {
        for config in job.configs() {
            config.load(env);
        }
    }
}

/// `validateConfigs`'s per-job half.
///
/// Returns the error `validateConfigs` would return once the worker-level
/// config is valid: the sorted `  - <job>: <err> ` lines wrapped in
/// `Failed to load configuration:\n`. When the worker config itself is
/// invalid, no job configs are validated (the baseline guards with
/// `len(validationErrors) == 0`) and this returns `None`; the worker-level
/// error is handled by [`WorkerConfigs`] exactly as `validateConfigs` would.
///
/// Note that the baseline discards this return value: `LoadConfig` stores it
/// with `config.AddError` on a shadowed local, and the returned config's
/// `Validate` override never reads its error slice (docs §1.1, §1.2).
pub fn validate_configs(
    jobs: &[(&'static str, Arc<dyn Job>)],
    worker: &WorkerConfigs,
) -> Option<String> {
    if worker.global.validate().is_err() {
        return None;
    }

    let mut descriptions = Vec::new();
    for (name, job) in jobs {
        if !worker.should_run(name) {
            continue;
        }
        for config in job.configs() {
            if let Err(err) = config.validate() {
                descriptions.push(format!("  - {name}: {err} "));
            }
        }
    }

    if descriptions.is_empty() {
        return None;
    }
    descriptions.sort();

    Some(format!(
        "Failed to load configuration:\n{}",
        descriptions.join("\n")
    ))
}

/// `createBackgroundRoutines`: call every enabled job's `routines`
/// concurrently and collect the results, or fail with
/// `Failed to initialize worker:\n  - <job>: <err>`.
pub async fn create_background_routines(
    deps: &JobDeps,
    jobs: &[(&'static str, Arc<dyn Job>)],
    worker: &WorkerConfigs,
) -> Result<Vec<Arc<BackgroundRoutine>>, String> {
    let setup_cancel = CancellationToken::new();
    let mut set = JoinSet::new();
    let mut names = HashMap::new();

    for (name, job) in jobs {
        if !worker.should_run(name) {
            tracing::debug!(job = name, "Skipping job");
            continue;
        }
        tracing::debug!(job = name, "Running job");

        let name: &'static str = name;
        let job = Arc::clone(job);
        let deps = deps.clone();
        let setup_ctx = setup_cancel.clone();
        let handle = set.spawn(async move { (name, job.routines(&deps, setup_ctx).await) });
        names.insert(handle.id(), name);
    }

    let mut all_routines = Vec::new();
    let mut descriptions = Vec::new();
    while let Some(result) = set.join_next_with_id().await {
        match result {
            Ok((_, (name, Ok(routines)))) => {
                for routine in routines {
                    routine.set_job_name(name);
                    all_routines.push(Arc::new(routine));
                }
            }
            Ok((_, (name, Err(err)))) => {
                descriptions.push(format!("  - {name}: {err}"));
                setup_cancel.cancel();
            }
            // A panic in a setup hook is fatal in the baseline; surface it as
            // a startup failure instead of starting with a partial job set.
            Err(join_error) => {
                let name = names.get(&join_error.id()).copied().unwrap_or("<job>");
                descriptions.push(format!("  - {name}: {join_error}"));
                setup_cancel.cancel();
            }
        }
    }

    if !descriptions.is_empty() {
        descriptions.sort();
        return Err(format!(
            "Failed to initialize worker:\n{}",
            descriptions.join("\n")
        ));
    }

    Ok(all_routines)
}

/// `emitJobCountMetrics`: emit `src_worker_jobs{job_name="<enabled>"} 1` once
/// per process. The baseline registers the gauge with
/// `prometheus.DefaultRegisterer` (which panics on a duplicate registration),
/// so the gauge is created once here too.
pub fn emit_job_count_metrics(jobs: &[(&'static str, Arc<dyn Job>)], worker: &WorkerConfigs) {
    static GAUGE: std::sync::OnceLock<IntGaugeVec> = std::sync::OnceLock::new();

    let gauge = GAUGE.get_or_init(|| {
        let gauge = IntGaugeVec::new(
            Opts::new(
                "src_worker_jobs",
                "Total number of jobs running in the worker.",
            ),
            &["job_name"],
        )
        .expect("valid src_worker_jobs metric");
        // Duplicate registration cannot happen: the gauge is created once.
        let _ = sg_metrics::registry().register(Box::new(gauge.clone()));
        gauge
    });

    for (name, _) in jobs {
        if !worker.should_run(name) {
            continue;
        }
        if let Ok(metric) = gauge.get_metric_with_label_values(&[name]) {
            metric.set(1);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::WorkerConfigs;

    struct FailingJob;

    #[async_trait]
    impl Job for FailingJob {
        async fn routines(
            &self,
            _deps: &JobDeps,
            _setup_ctx: CancellationToken,
        ) -> Result<Vec<BackgroundRoutine>, String> {
            Err("boom".to_owned())
        }
    }

    struct ConfigJob {
        config: Arc<FailingConfig>,
    }

    struct FailingConfig;

    impl JobConfig for FailingConfig {
        fn load(&self, _env: &mut EnvReader) {}

        fn validate(&self) -> Result<(), ConfigError> {
            Err(ConfigError::new("bad config"))
        }
    }

    #[async_trait]
    impl Job for ConfigJob {
        fn configs(&self) -> Vec<Arc<dyn JobConfig>> {
            vec![Arc::clone(&self.config) as Arc<dyn JobConfig>]
        }

        async fn routines(
            &self,
            _deps: &JobDeps,
            _setup_ctx: CancellationToken,
        ) -> Result<Vec<BackgroundRoutine>, String> {
            Ok(Vec::new())
        }
    }

    fn deps() -> JobDeps {
        JobDeps {
            frontend: sqlx::postgres::PgPoolOptions::new()
                .connect_lazy("postgres://unused@localhost/unused")
                .expect("lazy pool"),
            live_config: Arc::new(LiveConfig::new(sg_config::Unified::default())),
            keyring: Arc::new(sg_keyring::Keyring::default()),
        }
    }

    fn config_with_allowlist(values: &[&str]) -> WorkerConfigs {
        WorkerConfigs::for_test(values, &[])
    }

    #[tokio::test]
    async fn initialization_errors_are_sorted_and_formatted() {
        let jobs: Vec<(&'static str, Arc<dyn Job>)> = vec![
            ("b-job", Arc::new(FailingJob)),
            ("a-job", Arc::new(FailingJob)),
        ];
        let err = create_background_routines(&deps(), &jobs, &config_with_allowlist(&["all"]))
            .await
            .err()
            .expect("setup must fail");
        assert_eq!(
            err,
            "Failed to initialize worker:\n  - a-job: boom\n  - b-job: boom"
        );
    }

    #[tokio::test]
    async fn disabled_jobs_are_not_initialized() {
        let jobs: Vec<(&'static str, Arc<dyn Job>)> = vec![("a-job", Arc::new(FailingJob))];
        let routines =
            create_background_routines(&deps(), &jobs, &config_with_allowlist(&["other-job"]))
                .await
                .expect("disabled job must not run");
        assert!(routines.is_empty());
    }

    #[test]
    fn job_config_errors_use_the_baseline_format() {
        let jobs: Vec<(&'static str, Arc<dyn Job>)> = vec![(
            "z-job",
            Arc::new(ConfigJob {
                config: Arc::new(FailingConfig),
            }),
        )];
        let err = validate_configs(&jobs, &config_with_allowlist(&["all"]))
            .expect("validation must fail");
        assert_eq!(err, "Failed to load configuration:\n  - z-job: bad config ");
    }
}
