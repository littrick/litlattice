#![forbid(unsafe_code)]

//! `cmd/worker/internal/executormultiqueue` +
//! `cmd/worker/internal/executorqueue`: the
//! `executors-multiqueue-metrics-reporter` job.
//!
//! One periodic routine (`multiqueue-executors.autoscaler-metrics`, 30s)
//! reports the combined size of every executor queue so that the auto-scaler
//! outside Sourcegraph can size the executor pools:
//!
//! * `CountByState(queued|errored|processing)` from the auto-indexing queue
//!   (`lsif_indexes_with_repository_name u`) and the batch spec workspace
//!   execution queue (`batch_spec_workspace_execution_jobs_with_rank …`);
//! * the two counts are summed, multiplied by the queue allocation, truncated
//!   toward zero, and reported to each configured cloud reporter in parallel.
//!
//! The cloud clients (AWS CloudWatch, Google Cloud Monitoring) are not ported;
//! configuring one makes [`ExecutorsMultiqueueMetricsReporter::routines`] fail
//! loudly instead of silently not reporting. Without either cloud configured
//! the routine still runs and counts, exactly like the baseline (which then
//! has no reporters to run).

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use sg_core::{ConfigError, EnvReader};
use sqlx::PgPool;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

use super::executors::VALID_QUEUE_NAMES;
use super::{Job, JobConfig, JobDeps};
use crate::config::combine_errors;
use crate::framework::periodic::{Handler, HandlerError, PeriodicRoutine};
use crate::framework::BackgroundRoutine;

/// `multiqueueMetricsReporterJob.Description`.
pub const JOB_DESCRIPTION: &str = "executor push-based metrics reporting multiqueue routines";

/// `goroutine.WithName` of the multiqueue reporter.
pub const ROUTINE_NAME: &str = "multiqueue-executors.autoscaler-metrics";
/// `goroutine.WithDescription` of the multiqueue reporter.
pub const ROUTINE_DESCRIPTION: &str = "emits multiqueue metrics to GCP/AWS for auto-scaling";
/// `goroutine.WithInterval(30 * time.Second)`.
pub const ROUTINE_INTERVAL: Duration = Duration::from_secs(30);

/// `executorqueue.validCloudProviderNames`.
pub const VALID_CLOUD_PROVIDERS: [&str; 2] = ["aws", "gcp"];

const ENVIRONMENT_LABEL_DESCRIPTION: &str =
    "A label to pass to the custom metric to distinguish environments.";
const ALLOCATIONS_DESCRIPTION: &str =
    "Allocation map to distribute workloads across different clouds.";
const AWS_NAMESPACE_DESCRIPTION: &str =
    "The namespace to which to export the custom metric for scaling executors.";
const AWS_REGION_DESCRIPTION: &str = "The target AWS region.";
const AWS_ACCESS_KEY_ID_DESCRIPTION: &str =
    "An AWS access key associated with a user with access to CloudWatch.";
const AWS_SECRET_ACCESS_KEY_DESCRIPTION: &str =
    "An AWS secret key associated with a user with access to CloudWatch.";
const AWS_SESSION_TOKEN_DESCRIPTION: &str =
    "An optional AWS session token associated with a user with access to CloudWatch.";
const GCP_PROJECT_ID_DESCRIPTION: &str = "The project containing the custom metric.";
const GCP_CREDENTIALS_FILE_DESCRIPTION: &str =
    "The path to a service account key file with access to metrics.";
const GCP_CREDENTIALS_FILE_CONTENTS_DESCRIPTION: &str =
    "The contents of a service account key file with access to metrics.";

/// `executorqueue.QueueAllocation`.
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct QueueAllocation {
    pub percentage_aws: f64,
    pub percentage_gcp: f64,
}

/// `executorqueue.awsConfig`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AwsConfig {
    pub metric_namespace: String,
    pub region: String,
    pub access_key_id: String,
    pub secret_access_key: String,
    pub session_token: String,
}

/// `executorqueue.gcpConfig`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct GcpConfig {
    pub project_id: String,
    pub credentials_file: String,
    pub credentials_file_contents: String,
}

/// `executorqueue.Config`'s loaded values.
#[derive(Debug, Clone, Default)]
pub struct MetricsConfigValues {
    pub environment_label: String,
    pub allocations: HashMap<String, QueueAllocation>,
    pub aws: AwsConfig,
    pub gcp: GcpConfig,
}

/// `executormultiqueue.janitorConfig` (it embeds `*executorqueue.Config`).
#[derive(Debug, Default)]
pub struct MultiqueueMetricsConfig {
    values: Mutex<Option<MetricsConfigValues>>,
    errs: Mutex<Vec<String>>,
    /// `Config.once`: `Load` reads the environment only on the first call.
    loaded: AtomicBool,
}

impl MultiqueueMetricsConfig {
    pub fn snapshot(&self) -> MetricsConfigValues {
        self.values
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
            .unwrap_or_default()
    }
}

impl JobConfig for MultiqueueMetricsConfig {
    /// `executorqueue.Config.Load`.
    fn load(&self, env: &mut EnvReader) {
        if self.loaded.swap(true, Ordering::SeqCst) {
            return;
        }

        let mut errs = Vec::new();
        let environment_label = get_required(
            env,
            "EXECUTOR_METRIC_ENVIRONMENT_LABEL",
            "dev",
            ENVIRONMENT_LABEL_DESCRIPTION,
            &mut errs,
        );
        let raw_allocations = env.get("EXECUTOR_ALLOCATIONS", "", ALLOCATIONS_DESCRIPTION);

        // The baseline reads these package-level booleans with `os.Getenv`
        // during package init: the `<NAME>_<process>` override does not apply.
        let aws_configured = raw_env_non_empty("EXECUTOR_METRIC_AWS_NAMESPACE");
        let gcp_configured = raw_env_non_empty("EXECUTOR_METRIC_GCP_PROJECT_ID");

        let allocations = match parse_allocations(&raw_allocations, aws_configured, gcp_configured)
        {
            Ok(allocations) => allocations,
            Err(err) => {
                errs.push(err);
                HashMap::new()
            }
        };

        let aws = if aws_configured {
            load_aws(env, &mut errs)
        } else {
            AwsConfig::default()
        };
        let gcp = if gcp_configured {
            load_gcp(env, &mut errs)
        } else {
            GcpConfig::default()
        };

        *self
            .values
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(MetricsConfigValues {
            environment_label,
            allocations,
            aws,
            gcp,
        });
        *self
            .errs
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = errs;
    }

    /// `env.BaseConfig.Validate`.
    fn validate(&self) -> Result<(), ConfigError> {
        let errs = self
            .errs
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        match combine_errors(&errs) {
            Some(message) => Err(ConfigError::new(message)),
            None => Ok(()),
        }
    }
}

/// `env.BaseConfig.Get`: an empty resolved value adds
/// `invalid value %q for %s: no value supplied`.
fn get_required(
    env: &mut EnvReader,
    name: &str,
    default: &str,
    description: &str,
    errors: &mut Vec<String>,
) -> String {
    let raw = env.get(name, default, description);
    if raw.is_empty() {
        errors.push(format!(
            "invalid value {raw:?} for {name}: no value supplied"
        ));
    }
    raw
}

/// `executorqueue.awsConfig.load`.
fn load_aws(env: &mut EnvReader, errors: &mut Vec<String>) -> AwsConfig {
    AwsConfig {
        metric_namespace: get_required(
            env,
            "EXECUTOR_METRIC_AWS_NAMESPACE",
            "",
            AWS_NAMESPACE_DESCRIPTION,
            errors,
        ),
        region: get_required(
            env,
            "EXECUTOR_METRIC_AWS_REGION",
            "",
            AWS_REGION_DESCRIPTION,
            errors,
        ),
        access_key_id: get_required(
            env,
            "EXECUTOR_METRIC_AWS_ACCESS_KEY_ID",
            "",
            AWS_ACCESS_KEY_ID_DESCRIPTION,
            errors,
        ),
        secret_access_key: get_required(
            env,
            "EXECUTOR_METRIC_AWS_SECRET_ACCESS_KEY",
            "",
            AWS_SECRET_ACCESS_KEY_DESCRIPTION,
            errors,
        ),
        session_token: env.get(
            "EXECUTOR_METRIC_AWS_SESSION_TOKEN",
            "",
            AWS_SESSION_TOKEN_DESCRIPTION,
        ),
    }
}

/// `executorqueue.gcpConfig.load`.
fn load_gcp(env: &mut EnvReader, errors: &mut Vec<String>) -> GcpConfig {
    GcpConfig {
        project_id: get_required(
            env,
            "EXECUTOR_METRIC_GCP_PROJECT_ID",
            "",
            GCP_PROJECT_ID_DESCRIPTION,
            errors,
        ),
        credentials_file: env.get(
            "EXECUTOR_METRIC_GOOGLE_APPLICATION_CREDENTIALS_FILE",
            "",
            GCP_CREDENTIALS_FILE_DESCRIPTION,
        ),
        credentials_file_contents: env.get(
            "EXECUTOR_METRIC_GOOGLE_APPLICATION_CREDENTIALS_FILE_CONTENT",
            "",
            GCP_CREDENTIALS_FILE_CONTENTS_DESCRIPTION,
        ),
    }
}

/// `os.Getenv(name) != ""`.
fn raw_env_non_empty(name: &str) -> bool {
    std::env::var(name)
        .map(|value| !value.is_empty())
        .unwrap_or(false)
}

/// `executorqueue.parseAllocations`.
///
/// Go decodes into `map[string]map[string]float64` (a `null` value leaves the
/// inner map nil). The JSON error text is serde's, not `encoding/json`'s; that
/// is recorded as an erratum in docs/modules/06_worker.md §23.
pub fn parse_allocations(
    raw: &str,
    aws_configured: bool,
    gcp_configured: bool,
) -> Result<HashMap<String, QueueAllocation>, String> {
    let mut parsed_map: HashMap<String, HashMap<String, f64>> = HashMap::new();
    if !raw.is_empty() {
        let parsed: Option<HashMap<String, Option<HashMap<String, f64>>>> =
            serde_json::from_str(raw)
                .map_err(|err| format!("parsing EXECUTOR_ALLOCATIONS: {err}"))?;
        for (queue_name, allocation) in parsed.unwrap_or_default() {
            parsed_map.insert(queue_name, allocation.unwrap_or_default());
        }
    }

    normalize_allocations(&parsed_map, aws_configured, gcp_configured)
}

/// `executorqueue.normalizeAllocations`.
pub fn normalize_allocations(
    m: &HashMap<String, HashMap<String, f64>>,
    aws_configured: bool,
    gcp_configured: bool,
) -> Result<HashMap<String, QueueAllocation>, String> {
    for queue_name in m.keys() {
        if !VALID_QUEUE_NAMES.contains(&queue_name.as_str()) {
            return Err(format!("invalid queue '{queue_name}'"));
        }
    }

    let mut allocations = HashMap::with_capacity(VALID_QUEUE_NAMES.len());
    for queue_name in VALID_QUEUE_NAMES {
        let empty = HashMap::new();
        let queue_allocation = m.get(queue_name).unwrap_or(&empty);
        allocations.insert(
            queue_name.to_owned(),
            normalize_queue_allocation(
                queue_name,
                queue_allocation,
                aws_configured,
                gcp_configured,
            )?,
        );
    }

    Ok(allocations)
}

/// `executorqueue.normalizeQueueAllocation`.
fn normalize_queue_allocation(
    queue_name: &str,
    queue_allocation: &HashMap<String, f64>,
    aws_configured: bool,
    gcp_configured: bool,
) -> Result<QueueAllocation, String> {
    if queue_allocation.is_empty() {
        if aws_configured {
            if gcp_configured {
                tracing::warn!("Sending 100% of executor queue metrics to BOTH AWS and GCP");
                return Ok(QueueAllocation {
                    percentage_aws: 1.0,
                    percentage_gcp: 1.0,
                });
            }

            return Ok(QueueAllocation {
                percentage_aws: 1.0,
                percentage_gcp: 0.0,
            });
        } else if gcp_configured {
            return Ok(QueueAllocation {
                percentage_aws: 0.0,
                percentage_gcp: 1.0,
            });
        }

        return Ok(QueueAllocation::default());
    }

    for (cloud_provider, allocation) in queue_allocation {
        if !VALID_CLOUD_PROVIDERS.contains(&cloud_provider.as_str()) {
            return Err(format!(
                "invalid cloud provider '{cloud_provider}', expected 'aws' or 'gcp'"
            ));
        }

        if *allocation < 0.0 || *allocation > 1.0 {
            return Err(format!(
                "invalid cloud provider allocation '{allocation:.2}'"
            ));
        }
    }

    if !aws_configured && queue_allocation.get("aws").copied().unwrap_or(0.0) > 0.0 {
        tracing::warn!(
            queueName = %queue_name,
            "AWS executor queue metrics not configured - setting allocation to zero"
        );
    }
    if !gcp_configured && queue_allocation.get("gcp").copied().unwrap_or(0.0) > 0.0 {
        tracing::warn!(
            queueName = %queue_name,
            "GCP executor queue metrics not configured - setting allocation to zero"
        );
    }

    let percentage_aws = clamped_allocation(queue_allocation, "aws", aws_configured);
    let percentage_gcp = clamped_allocation(queue_allocation, "gcp", gcp_configured);
    let total_allocation = percentage_aws + percentage_gcp;
    if total_allocation < 1.0 {
        tracing::warn!(
            queueName = %queue_name,
            totalAllocation = total_allocation,
            "Not configured to send full executor queue metrics"
        );
    }

    Ok(QueueAllocation {
        percentage_aws,
        percentage_gcp,
    })
}

/// `queueAllocation[name]`, zeroed when the provider is not configured.
fn clamped_allocation(allocation: &HashMap<String, f64>, provider: &str, configured: bool) -> f64 {
    if !configured {
        return 0.0;
    }
    allocation.get(provider).copied().unwrap_or(0.0)
}

/// `executorutil.FormatQueueNamesForMetrics`: sort alphabetically and join with
/// underscores; the single queue name is returned verbatim when no list is
/// supplied.
pub fn format_queue_names_for_metrics(queue_name: &str, queue_names: &[String]) -> String {
    if !queue_names.is_empty() {
        let mut sorted = queue_names.to_vec();
        // `sort.Strings` is byte-wise, as is `String`'s `Ord`.
        sorted.sort();
        sorted.join("_")
    } else {
        queue_name.to_owned()
    }
}

/// `executorqueue.reporter`.
#[async_trait]
pub trait Reporter: Send + Sync {
    fn name(&self) -> &'static str;

    /// `GetAllocation`.
    fn get_allocation(&self, allocation: QueueAllocation) -> f64;

    /// `ReportCount`. The baseline has no error return: a failed cloud call is
    /// logged inside the reporter.
    async fn report_count(&self, ctx: &CancellationToken, queue_name: &str, count: i64);
}

/// One `countFunc` of `externalEmitter`: `CountByState` against one store.
#[async_trait]
pub trait CountSource: Send + Sync {
    async fn count(&self) -> Result<i64, String>;
}

/// A [`CountSource`] over one dbworker store view.
pub struct QueueCountSource {
    pool: PgPool,
    view: &'static str,
}

impl QueueCountSource {
    pub fn new(pool: PgPool, view: &'static str) -> Self {
        Self { pool, view }
    }
}

#[async_trait]
impl CountSource for QueueCountSource {
    async fn count(&self) -> Result<i64, String> {
        sg_db::worker_queue::count_by_state(
            &self.pool,
            self.view,
            sg_db::worker_queue::QUEUED_ERRORED_PROCESSING,
        )
        .await
    }
}

/// `executorqueue.externalEmitter`.
pub struct ExternalEmitter {
    queue_name: String,
    count_sources: Vec<Arc<dyn CountSource>>,
    reporters: Vec<Arc<dyn Reporter>>,
    allocation: QueueAllocation,
}

impl ExternalEmitter {
    /// Public so the differential harness can supply fake sources/reporters.
    pub fn new(
        queue_name: String,
        count_sources: Vec<Arc<dyn CountSource>>,
        reporters: Vec<Arc<dyn Reporter>>,
        allocation: QueueAllocation,
    ) -> Self {
        Self {
            queue_name,
            count_sources,
            reporters,
            allocation,
        }
    }

    /// `externalEmitter.Handle`.
    pub async fn emit(&self, ctx: &CancellationToken) -> Result<(), HandlerError> {
        let mut count: i64 = 0;
        for source in &self.count_sources {
            let sub_count = source
                .count()
                .await
                .map_err(|err| HandlerError::new(format!("dbworkerstore.CountByState: {err}")))?;
            count += sub_count;
        }

        // `runParallel`: every reporter gets its own goroutine (here: one
        // task each) and the handler waits for all of them.
        let mut tasks = JoinSet::new();
        for reporter in &self.reporters {
            // Go: `int(float64(count) * reporter.GetAllocation(allocation))`,
            // i.e. truncation toward zero.
            let reported = (count as f64 * reporter.get_allocation(self.allocation)) as i64;
            let reporter = Arc::clone(reporter);
            let queue_name = self.queue_name.clone();
            let ctx = ctx.clone();
            tasks.spawn(async move { reporter.report_count(&ctx, &queue_name, reported).await });
        }
        while let Some(joined) = tasks.join_next().await {
            if let Err(err) = joined {
                return Err(HandlerError::new(err.to_string()));
            }
        }

        Ok(())
    }
}

#[async_trait]
impl Handler for ExternalEmitter {
    async fn handle(&self, ctx: CancellationToken) -> Result<(), HandlerError> {
        self.emit(&ctx).await
    }
}

/// `executorqueue.configureReporters`.
///
/// The baseline returns no reporter for a cloud whose namespace/project is
/// unset (and its `newAWSReporter` additionally requires a region). The cloud
/// clients themselves are not ported: when one would be created, fail loudly.
pub fn configure_reporters(values: &MetricsConfigValues) -> Result<Vec<Arc<dyn Reporter>>, String> {
    if !values.aws.metric_namespace.is_empty() && !values.aws.region.is_empty() {
        return Err(
            "AWS CloudWatch executor metric reporting is not implemented by the Rust worker yet"
                .to_owned(),
        );
    }
    if !values.gcp.project_id.is_empty() {
        return Err(
            "Google Cloud Monitoring executor metric reporting is not implemented by the Rust worker yet"
                .to_owned(),
        );
    }
    Ok(Vec::new())
}

/// `executormultiqueue.multiqueueMetricsReporterJob`.
pub struct ExecutorsMultiqueueMetricsReporter {
    config: Arc<MultiqueueMetricsConfig>,
}

impl ExecutorsMultiqueueMetricsReporter {
    pub fn new() -> Self {
        Self {
            config: Arc::new(MultiqueueMetricsConfig::default()),
        }
    }

    /// The job's `configInst`, for tests and harnesses.
    pub fn config(&self) -> &MultiqueueMetricsConfig {
        &self.config
    }
}

impl Default for ExecutorsMultiqueueMetricsReporter {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Job for ExecutorsMultiqueueMetricsReporter {
    fn description(&self) -> String {
        JOB_DESCRIPTION.to_owned()
    }

    fn configs(&self) -> Vec<Arc<dyn JobConfig>> {
        vec![Arc::clone(&self.config) as Arc<dyn JobConfig>]
    }

    async fn routines(
        &self,
        deps: &JobDeps,
        _setup_ctx: CancellationToken,
    ) -> Result<Vec<BackgroundRoutine>, String> {
        let values = self.config.snapshot();
        let reporters = configure_reporters(&values)?;

        let queue_names: Vec<String> = VALID_QUEUE_NAMES.iter().map(|q| (*q).to_owned()).collect();
        let queue_str = format_queue_names_for_metrics("", &queue_names);
        // `metricsConfig.Allocations[queueNames[0]]`: the first valid queue
        // name (`batches`), not a queue-specific allocation.
        let allocation = values
            .allocations
            .get(VALID_QUEUE_NAMES[0])
            .copied()
            .unwrap_or_default();

        let count_sources: Vec<Arc<dyn CountSource>> = vec![
            Arc::new(QueueCountSource::new(
                deps.frontend.clone(),
                sg_db::worker_queue::LSIF_INDEXES_VIEW,
            )),
            Arc::new(QueueCountSource::new(
                deps.frontend.clone(),
                sg_db::worker_queue::BATCH_SPEC_WORKSPACE_EXECUTION_JOBS_VIEW,
            )),
        ];

        let emitter = ExternalEmitter::new(queue_str, count_sources, reporters, allocation);
        let routine = PeriodicRoutine::new(Arc::new(emitter))
            .with_name(ROUTINE_NAME)
            .with_description(ROUTINE_DESCRIPTION)
            .with_interval(ROUTINE_INTERVAL);
        Ok(vec![routine.into()])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex as StdMutex;

    fn map(entries: &[(&str, f64)]) -> HashMap<String, f64> {
        entries
            .iter()
            .map(|(name, value)| ((*name).to_owned(), *value))
            .collect()
    }

    fn assert_allocation(allocation: QueueAllocation, aws: f64, gcp: f64) {
        assert_eq!(allocation.percentage_aws, aws);
        assert_eq!(allocation.percentage_gcp, gcp);
    }

    /// `TestNormalizeQueueAllocation`.
    #[test]
    fn normalize_queue_allocation_matches_go_tests() {
        let cases = [
            map(&[]),
            map(&[("aws", 0.5)]),
            map(&[("aws", 1.0)]),
            map(&[("gcp", 0.5)]),
            map(&[("gcp", 1.0)]),
            map(&[("aws", 0.5), ("gcp", 0.5)]),
            map(&[("aws", 1.0), ("gcp", 1.0)]),
        ];

        for allocation in &cases {
            // Not configured: every value is zeroed.
            let normalized =
                normalize_queue_allocation("", allocation, false, false).expect("not configured");
            assert_allocation(normalized, 0.0, 0.0);
        }

        // AWS only: unconfigured allocations default to 100% AWS.
        let normalized = normalize_queue_allocation("", &map(&[]), true, false).expect("aws");
        assert_allocation(normalized, 1.0, 0.0);
        for allocation in cases.iter().filter(|allocation| !allocation.is_empty()) {
            let normalized =
                normalize_queue_allocation("", allocation, true, false).expect("aws enabled");
            assert_allocation(
                normalized,
                allocation.get("aws").copied().unwrap_or(0.0),
                0.0,
            );
        }

        // GCP only.
        let normalized = normalize_queue_allocation("", &map(&[]), false, true).expect("gcp");
        assert_allocation(normalized, 0.0, 1.0);
        for allocation in cases.iter().filter(|allocation| !allocation.is_empty()) {
            let normalized =
                normalize_queue_allocation("", allocation, false, true).expect("gcp enabled");
            assert_allocation(
                normalized,
                0.0,
                allocation.get("gcp").copied().unwrap_or(0.0),
            );
        }

        // Multi-cloud.
        let normalized = normalize_queue_allocation("", &map(&[]), true, true).expect("multi");
        assert_allocation(normalized, 1.0, 1.0);
        for allocation in cases.iter().filter(|allocation| !allocation.is_empty()) {
            let normalized =
                normalize_queue_allocation("", allocation, true, true).expect("multi enabled");
            assert_allocation(
                normalized,
                allocation.get("aws").copied().unwrap_or(0.0),
                allocation.get("gcp").copied().unwrap_or(0.0),
            );
        }
    }

    #[test]
    fn invalid_allocations_use_the_go_messages() {
        let mut parsed = HashMap::new();
        parsed.insert("badqueue".to_owned(), HashMap::new());
        assert_eq!(
            normalize_allocations(&parsed, false, false).expect_err("bad queue"),
            "invalid queue 'badqueue'"
        );

        let mut parsed = HashMap::new();
        parsed.insert("batches".to_owned(), map(&[("azure", 0.5)]));
        assert_eq!(
            normalize_allocations(&parsed, false, false).expect_err("bad provider"),
            "invalid cloud provider 'azure', expected 'aws' or 'gcp'"
        );

        let mut parsed = HashMap::new();
        parsed.insert("batches".to_owned(), map(&[("aws", 1.5)]));
        assert_eq!(
            normalize_allocations(&parsed, false, false).expect_err("range"),
            "invalid cloud provider allocation '1.50'"
        );

        let mut parsed = HashMap::new();
        parsed.insert("batches".to_owned(), map(&[("gcp", -0.1)]));
        assert_eq!(
            normalize_allocations(&parsed, false, false).expect_err("negative"),
            "invalid cloud provider allocation '-0.10'"
        );
    }

    #[test]
    fn parse_allocations_handles_empty_null_and_objects() {
        let empty = parse_allocations("", false, false).expect("empty");
        assert_eq!(empty.len(), 2);
        assert_eq!(empty["batches"], QueueAllocation::default());
        assert_eq!(empty["codeintel"], QueueAllocation::default());

        let defaults = parse_allocations("{}", true, true).expect("defaults");
        assert_eq!(
            defaults["batches"],
            QueueAllocation {
                percentage_aws: 1.0,
                percentage_gcp: 1.0
            }
        );

        let nulls = parse_allocations(r#"{"batches":null,"codeintel":{}}"#, false, true)
            .expect("null inner map");
        assert_eq!(
            nulls["batches"],
            QueueAllocation {
                percentage_aws: 0.0,
                percentage_gcp: 1.0
            }
        );

        let explicit = parse_allocations(
            r#"{"codeintel":{"aws":0.25,"gcp":0.75},"batches":{"gcp":1}}"#,
            true,
            true,
        )
        .expect("explicit");
        assert_eq!(
            explicit["codeintel"],
            QueueAllocation {
                percentage_aws: 0.25,
                percentage_gcp: 0.75
            }
        );
        assert_eq!(
            explicit["batches"],
            QueueAllocation {
                percentage_aws: 0.0,
                percentage_gcp: 1.0
            }
        );

        let err = parse_allocations("not json", false, false).expect_err("invalid json");
        assert!(err.starts_with("parsing EXECUTOR_ALLOCATIONS: "), "{err}");
    }

    #[test]
    fn queue_name_formatting_sorts_and_joins() {
        assert_eq!(format_queue_names_for_metrics("single", &[]), "single");
        assert_eq!(
            format_queue_names_for_metrics("", &["codeintel".to_owned(), "batches".to_owned()]),
            "batches_codeintel"
        );
        assert_eq!(
            format_queue_names_for_metrics("", &["b".to_owned(), "a".to_owned(), "c".to_owned()]),
            "a_b_c"
        );
    }

    struct FixedCount(i64);

    #[async_trait]
    impl CountSource for FixedCount {
        async fn count(&self) -> Result<i64, String> {
            Ok(self.0)
        }
    }

    struct FailingCount;

    #[async_trait]
    impl CountSource for FailingCount {
        async fn count(&self) -> Result<i64, String> {
            Err("boom".to_owned())
        }
    }

    struct RecordingReporter {
        name: &'static str,
        calls: StdMutex<Vec<(String, i64)>>,
    }

    impl RecordingReporter {
        fn new(name: &'static str) -> Self {
            Self {
                name,
                calls: StdMutex::new(Vec::new()),
            }
        }
    }

    #[async_trait]
    impl Reporter for RecordingReporter {
        fn name(&self) -> &'static str {
            self.name
        }

        fn get_allocation(&self, allocation: QueueAllocation) -> f64 {
            if self.name == "aws" {
                allocation.percentage_aws
            } else {
                allocation.percentage_gcp
            }
        }

        async fn report_count(&self, _ctx: &CancellationToken, queue_name: &str, count: i64) {
            self.calls
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .push((queue_name.to_owned(), count));
        }
    }

    #[tokio::test]
    async fn emitter_sums_counts_and_truncates_allocations() {
        let aws = Arc::new(RecordingReporter::new("aws"));
        let gcp = Arc::new(RecordingReporter::new("gcp"));

        let emitter = ExternalEmitter::new(
            "batches_codeintel".to_owned(),
            vec![Arc::new(FixedCount(7)), Arc::new(FixedCount(5))],
            vec![
                Arc::clone(&aws) as Arc<dyn Reporter>,
                Arc::clone(&gcp) as Arc<dyn Reporter>,
            ],
            QueueAllocation {
                percentage_aws: 0.5,
                percentage_gcp: 1.0,
            },
        );
        emitter.emit(&CancellationToken::new()).await.expect("emit");

        // 12 * 0.5 = 6; 12 * 1.0 = 12.
        assert_eq!(
            *aws.calls.lock().unwrap(),
            vec![("batches_codeintel".to_owned(), 6)]
        );
        assert_eq!(
            *gcp.calls.lock().unwrap(),
            vec![("batches_codeintel".to_owned(), 12)]
        );
    }

    #[tokio::test]
    async fn emitter_wraps_count_errors() {
        let emitter = ExternalEmitter::new(
            "batches_codeintel".to_owned(),
            vec![Arc::new(FailingCount)],
            Vec::new(),
            QueueAllocation::default(),
        );
        let err = emitter
            .emit(&CancellationToken::new())
            .await
            .expect_err("count error");
        assert_eq!(err.message(), "dbworkerstore.CountByState: boom");
    }

    #[test]
    fn config_defaults_and_validation() {
        let config = MultiqueueMetricsConfig::default();
        config.load(&mut EnvReader::new());
        let values = config.snapshot();
        assert_eq!(values.environment_label, "dev");
        assert_eq!(values.allocations.len(), 2);
        assert_eq!(values.aws, AwsConfig::default());
        assert_eq!(values.gcp, GcpConfig::default());
        assert!(config.validate().is_ok());

        // `once.Do`: a second Load is a no-op.
        config.load(&mut EnvReader::new());
        assert_eq!(config.snapshot().environment_label, "dev");
    }

    fn deps() -> JobDeps {
        JobDeps {
            frontend: sqlx::postgres::PgPoolOptions::new()
                .connect_lazy("postgres://unused@localhost/unused")
                .expect("lazy pool"),
            live_config: Arc::new(sg_config::LiveConfig::new(sg_config::Unified::default())),
            keyring: Arc::new(sg_keyring::Keyring::default()),
        }
    }

    #[tokio::test]
    async fn job_builds_the_periodic_routine() {
        let job = ExecutorsMultiqueueMetricsReporter::new();
        job.config().load(&mut EnvReader::new());
        let routines = job
            .routines(&deps(), CancellationToken::new())
            .await
            .expect("routines");

        assert_eq!(routines.len(), 1);
        assert_eq!(routines[0].name(), ROUTINE_NAME);
        assert_eq!(routines[0].description(), ROUTINE_DESCRIPTION);
        assert_eq!(routines[0].interval(), ROUTINE_INTERVAL);
        assert_eq!(job.description(), JOB_DESCRIPTION);
    }

    #[tokio::test]
    async fn configured_clouds_fail_loudly() {
        let values = MetricsConfigValues {
            aws: AwsConfig {
                metric_namespace: "ns".to_owned(),
                region: "us-east-1".to_owned(),
                ..AwsConfig::default()
            },
            ..MetricsConfigValues::default()
        };
        let err = configure_reporters(&values).err().expect("aws");
        assert!(err.contains("AWS CloudWatch"), "{err}");

        let values = MetricsConfigValues {
            gcp: GcpConfig {
                project_id: "project".to_owned(),
                ..GcpConfig::default()
            },
            ..MetricsConfigValues::default()
        };
        let err = configure_reporters(&values).err().expect("gcp");
        assert!(err.contains("Google Cloud Monitoring"), "{err}");

        // A namespace without a region is the baseline's `newAWSReporter` nil
        // case: no reporter, no error.
        let values = MetricsConfigValues {
            aws: AwsConfig {
                metric_namespace: "ns".to_owned(),
                ..AwsConfig::default()
            },
            ..MetricsConfigValues::default()
        };
        assert!(configure_reporters(&values).expect("no region").is_empty());
    }
}
