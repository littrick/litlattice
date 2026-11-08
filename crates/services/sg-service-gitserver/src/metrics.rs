#![forbid(unsafe_code)]

//! Metric families exported by `gitserver`, named exactly like the Go
//! baseline (`cmd/gitserver/internal/*` and the packages it initializes).
//!
//! Every family the baseline registers at process start is registered here
//! eagerly, so `/metrics` and dashboards see the same names from startup.
//! Plain counters/gauges emit a series at 0; vector families stay empty until
//! they are first observed, exactly like `promauto`.
//!
//! The four disk gauges are evaluated at scrape time, like
//! `prometheus.NewGaugeFunc` (`cmd/gitserver/internal/gitserverfs`) and
//! `internal/metrics`'s `diskCollector`, so they always report the current
//! filesystem state.
//!
//! Not reproduced: client-side families the Go process only exports because
//! it links the gitserver *client*
//! (`src_gitserver_addr_for_repo_invoked`, `src_gitserver_client_concurrent_requests`),
//! the `go_*`/`process_*` collectors and `gitserver_mount_point_info`.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::OnceLock;
use std::time::{Duration, Instant};

use sg_metrics::prometheus::core::{Collector, Desc};
use sg_metrics::prometheus::proto::{
    Gauge as ProtoGauge, LabelPair, Metric, MetricFamily, MetricType,
};
use sg_metrics::prometheus::{
    Counter, CounterVec, Gauge, GaugeVec, Histogram, HistogramOpts, HistogramVec, Opts, Registry,
};

use crate::disk;

/// `prometheus.DefBuckets`.
fn default_buckets() -> Vec<f64> {
    vec![
        0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
    ]
}

/// Go `prometheus.ExponentialBucketsRange(min, max, count)`.
fn exponential_buckets_range(min: f64, max: f64, count: usize) -> Vec<f64> {
    let growth = (max / min).powf(1.0 / (count as f64 - 1.0));
    (0..count)
        .map(|index| min * growth.powi(index as i32))
        .collect()
}

/// `gitservice.go`: `prometheus.ExponentialBuckets(.1, 4, 9)`.
fn gitservice_buckets() -> Vec<f64> {
    let mut buckets = Vec::with_capacity(9);
    let mut bucket = 0.1_f64;
    for _ in 0..9 {
        buckets.push(bucket);
        bucket *= 4.0;
    }
    buckets
}

/// `vcssyncer/instrumented_syncer.go`: the default buckets followed by
/// `LinearBuckets(60, 5*60, 24)`.
fn fetch_buckets() -> Vec<f64> {
    let mut buckets = vec![
        0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 20.0, 30.0,
    ];
    buckets.extend((0..24).map(|index| 60.0 + index as f64 * 5.0 * 60.0));
    buckets
}

/// Register a metric, panicking on a duplicate like `promauto`.
fn register<T: Collector + 'static>(registry: &Registry, metric: T) {
    registry
        .register(Box::new(metric))
        .expect("gitserver metric families are registered once");
}

fn gauge(registry: &Registry, name: &str, help: &str) -> Gauge {
    let gauge = Gauge::new(name, help).expect("valid gauge");
    register(registry, gauge.clone());
    gauge
}

fn counter(registry: &Registry, name: &str, help: &str) -> Counter {
    let counter = Counter::new(name, help).expect("valid counter");
    register(registry, counter.clone());
    counter
}

fn counter_vec(registry: &Registry, name: &str, help: &str, names: &[&str]) -> CounterVec {
    let metric = CounterVec::new(Opts::new(name, help), names).expect("valid counter vec");
    register(registry, metric.clone());
    metric
}

fn gauge_vec(registry: &Registry, name: &str, help: &str, names: &[&str]) -> GaugeVec {
    let metric = GaugeVec::new(Opts::new(name, help), names).expect("valid gauge vec");
    register(registry, metric.clone());
    metric
}

fn histogram(registry: &Registry, name: &str, help: &str, buckets: Vec<f64>) -> Histogram {
    let metric = Histogram::with_opts(HistogramOpts::new(name, help).buckets(buckets))
        .expect("valid histogram");
    register(registry, metric.clone());
    metric
}

fn histogram_vec(
    registry: &Registry,
    name: &str,
    help: &str,
    names: &[&str],
    buckets: Vec<f64>,
) -> HistogramVec {
    let metric = HistogramVec::new(HistogramOpts::new(name, help).buckets(buckets), names)
        .expect("valid histogram vec");
    register(registry, metric.clone());
    metric
}

/// A pair of disk gauges evaluated on every scrape, like Go's `GaugeFunc`
/// (unlabelled, reports 0 on error) and `internal/metrics.diskCollector`
/// (const `path` label, reports nothing on error).
struct DiskCollector {
    path: PathBuf,
    label_pairs: Vec<(&'static str, String)>,
    available: FamilyDesc,
    total: FamilyDesc,
    zero_on_error: bool,
}

struct FamilyDesc {
    desc: Desc,
    name: &'static str,
    help: &'static str,
}

impl DiskCollector {
    #[allow(clippy::too_many_arguments)]
    fn new(
        path: &Path,
        label_pairs: Vec<(&'static str, String)>,
        available: (&'static str, &'static str),
        total: (&'static str, &'static str),
        zero_on_error: bool,
    ) -> Self {
        let const_labels: HashMap<String, String> = label_pairs
            .iter()
            .map(|(name, value)| ((*name).to_owned(), value.clone()))
            .collect();
        let family_desc = |(name, help): (&'static str, &'static str)| {
            let desc = Desc::new(
                name.to_owned(),
                help.to_owned(),
                Vec::new(),
                const_labels.clone(),
            )
            .expect("valid disk desc");
            FamilyDesc { desc, name, help }
        };
        Self {
            path: path.to_path_buf(),
            label_pairs,
            available: family_desc(available),
            total: family_desc(total),
            zero_on_error,
        }
    }

    fn family(&self, family: &FamilyDesc, value: f64) -> MetricFamily {
        let label = self
            .label_pairs
            .iter()
            .map(|(name, value)| {
                let mut pair = LabelPair::default();
                pair.set_name((*name).to_owned());
                pair.set_value(value.clone());
                pair
            })
            .collect::<Vec<_>>();
        let mut gauge = ProtoGauge::default();
        gauge.set_value(value);
        let mut metric = Metric::default();
        metric.set_label(label.into());
        metric.set_gauge(gauge);

        let mut out = MetricFamily::default();
        out.set_name(family.name.to_owned());
        out.set_help(family.help.to_owned());
        out.set_field_type(MetricType::GAUGE);
        out.set_metric(vec![metric].into());
        out
    }
}

impl Collector for DiskCollector {
    fn desc(&self) -> Vec<&Desc> {
        vec![&self.available.desc, &self.total.desc]
    }

    fn collect(&self) -> Vec<MetricFamily> {
        match disk::disk_usage(&self.path) {
            Ok(info) => vec![
                self.family(&self.available, info.available_space as f64),
                self.family(&self.total, info.total_space as f64),
            ],
            Err(err) => {
                tracing::error!(path = %self.path.display(), error = %err, "error getting disk usage info");
                if self.zero_on_error {
                    vec![
                        self.family(&self.available, 0.0),
                        self.family(&self.total, 0.0),
                    ]
                } else {
                    Vec::new()
                }
            }
        }
    }
}

/// Metric handles the read path updates.
pub struct Metrics {
    echo_duration: Gauge,
    backend_concurrent: GaugeVec,
    backend_duration: HistogramVec,
    backend_total: CounterVec,
    backend_errors: CounterVec,
    exec_running: GaugeVec,
    exec_duration: HistogramVec,
    exec_high_memory: CounterVec,
    gitservice_running: GaugeVec,
    gitservice_duration: HistogramVec,
    search_running: Gauge,
    search_duration: HistogramVec,
    search_latency: Histogram,
}

impl Metrics {
    pub fn new(registry: &Registry, repos_dir: &Path) -> Self {
        // `cmd/gitserver/internal/servermetrics.go`
        let echo_duration = gauge(
            registry,
            "src_gitserver_echo_duration_seconds",
            "Duration of executing the echo command.",
        );

        // `cmd/gitserver/internal/server.go`
        let _clone_queue = gauge(
            registry,
            "src_gitserver_clone_queue",
            "number of repos waiting to be cloned.",
        );
        let _repo_cloned = counter(
            registry,
            "src_gitserver_repo_cloned",
            "number of successful git clones run",
        );
        let _repo_cloned_failed = counter(
            registry,
            "src_gitserver_repo_cloned_failed",
            "number of failed git clones",
        );
        let _repo_corrupted = counter(
            registry,
            "src_gitserver_repo_corrupted",
            "number of corruption events",
        );

        // `cmd/gitserver/internal/cleanup.go`
        let _repo_wrong_shard = gauge(
            registry,
            "src_gitserver_repo_wrong_shard",
            "The number of repos that are on disk on the wrong shard",
        );
        let _repo_wrong_shard_deleted = counter(
            registry,
            "src_gitserver_repo_wrong_shard_deleted",
            "The number of repos on the wrong shard that we deleted",
        );
        let _repos_removed = counter_vec(
            registry,
            "src_gitserver_repos_removed",
            "number of repos removed during cleanup",
            &["reason"],
        );
        let _repos_recloned = counter(
            registry,
            "src_gitserver_repos_recloned",
            "number of repos removed and re-cloned due to age",
        );
        let _janitor_running = gauge(
            registry,
            "src_gitserver_janitor_running",
            "set to 1 when the gitserver janitor background job is running",
        );
        let _janitor_job_duration = histogram_vec(
            registry,
            "src_gitserver_janitor_job_duration_seconds",
            "Duration of the individual jobs within the gitserver janitor background job",
            &["success", "job_name"],
            exponential_buckets_range(0.001, 240.0, 16),
        );
        let _maintenance_status = counter_vec(
            registry,
            "src_gitserver_maintenance_status",
            "whether the maintenance run was a success (true/false) and the reason why a cleanup was needed",
            &["success", "reason"],
        );
        let _prune_status = counter_vec(
            registry,
            "src_gitserver_prune_status",
            "whether git prune was a success (true/false) and whether it was skipped (true/false)",
            &["success", "skipped"],
        );
        let _janitor_duration = histogram(
            registry,
            "src_gitserver_janitor_duration_seconds",
            "Duration of gitserver janitor background job",
            vec![0.1, 1.0, 10.0, 60.0, 300.0, 3600.0, 7200.0],
        );
        let _non_existing_repos_removed = counter(
            registry,
            "src_gitserver_non_existing_repos_removed",
            "number of non existing repos removed during cleanup",
        );

        // `cmd/gitserver/internal/ensurerevision.go`
        let _ensure_revision = counter_vec(
            registry,
            "src_gitserver_ensure_revision",
            "A request triggered ensureRevision in Gitserver",
            &["status"],
        );

        // `cmd/gitserver/internal/gitservice.go`
        let gitservice_duration = histogram_vec(
            registry,
            "src_gitserver_gitservice_duration_seconds",
            "A histogram of latencies for the git service (upload-pack for internal clones) endpoint.",
            &["type", "error"],
            gitservice_buckets(),
        );
        let gitservice_running = gauge_vec(
            registry,
            "src_gitserver_gitservice_running",
            "A histogram of latencies for the git service (upload-pack for internal clones) endpoint.",
            &["type"],
        );

        // `cmd/gitserver/internal/search.go`
        let search_running = gauge(
            registry,
            "src_gitserver_search_running",
            "number of gitserver.Search running concurrently.",
        );
        let search_duration = histogram_vec(
            registry,
            "src_gitserver_search_duration_seconds",
            "gitserver.Search duration in seconds.",
            &["error"],
            vec![0.01, 0.05, 0.1, 0.2, 0.5, 1.0, 2.0, 5.0, 10.0, 30.0],
        );
        let search_latency = histogram(
            registry,
            "src_gitserver_search_latency_seconds",
            "gitserver.Search latency (time until first result is sent) in seconds.",
            vec![0.01, 0.05, 0.1, 0.2, 0.5, 1.0, 2.0, 5.0, 10.0, 30.0],
        );

        // `cmd/gitserver/internal/git/observability.go`
        let backend_concurrent = gauge_vec(
            registry,
            "src_gitserver_backend_concurrent_operations",
            "Concurrent operations handled by Gitserver backends.",
            &["op"],
        );
        let backend_duration = histogram_vec(
            registry,
            "src_gitserver_backend_duration_seconds",
            "Time in seconds spent performing successful gitserver_backend operations",
            &["op"],
            default_buckets(),
        );
        let backend_total = counter_vec(
            registry,
            "src_gitserver_backend_total",
            "Total number of method invocations.",
            &["op"],
        );
        let backend_errors = counter_vec(
            registry,
            "src_gitserver_backend_errors_total",
            "Total number of gitserver_backend operations resulting in an unexpected error",
            &["op"],
        );

        // `cmd/gitserver/internal/git/gitcli`
        let exec_running = gauge_vec(
            registry,
            "src_gitserver_exec_running",
            "number of gitserver.GitCommand running concurrently.",
            &["cmd"],
        );
        let exec_duration = histogram_vec(
            registry,
            "src_gitserver_exec_duration_seconds",
            "gitserver.GitCommand latencies in seconds.",
            &["cmd"],
            exponential_buckets_range(0.01, 60.0, 12),
        );
        let exec_high_memory = counter_vec(
            registry,
            "src_gitserver_exec_high_memory_usage_count",
            "gitcli.GitCommand high memory usage by subcommand",
            &["cmd"],
        );
        let _exec_blocked_command = counter(
            registry,
            "src_gitserver_exec_blocked_command_received",
            "Incremented each time a command not in the allowlist for gitserver is executed",
        );
        let _bad_commit_error = counter(
            registry,
            "src_gitserver_bad_commit_error",
            "number of times a bad commit error is returned",
        );

        // `cmd/gitserver/internal/statesyncer.go`
        let _repo_sync_state = counter_vec(
            registry,
            "src_repo_sync_state_counter",
            "Incremented each time we check the state of repo",
            &["type"],
        );
        let _repo_state_upsert = counter_vec(
            registry,
            "src_repo_sync_state_upsert_counter",
            "Incremented each time we upsert repo state in the database",
            &["success"],
        );

        // `cmd/gitserver/internal/vcssyncer/instrumented_syncer.go`
        let _is_cloneable_duration = histogram_vec(
            registry,
            "vcssyncer_is_cloneable_duration_seconds",
            "Time taken to check if a repository is cloneable",
            &["type", "success"],
            default_buckets(),
        );
        let _clone_duration = histogram_vec(
            registry,
            "vcssyncer_clone_duration_seconds",
            "Time taken to clone a repository",
            &["type", "success"],
            fetch_buckets(),
        );
        let _fetch_duration = histogram_vec(
            registry,
            "vcssyncer_fetch_duration_seconds",
            "Time taken to fetch a repository",
            &["type", "success"],
            fetch_buckets(),
        );

        // `cmd/gitserver/internal/gitserverfs/fs.go`: legacy `GaugeFunc` pair.
        register(
            registry,
            DiskCollector::new(
                repos_dir,
                Vec::new(),
                (
                    "src_gitserver_disk_space_available",
                    "Amount of free space disk space on the repos mount.",
                ),
                (
                    "src_gitserver_disk_space_total",
                    "Amount of total disk space in the repos directory.",
                ),
                true,
            ),
        );
        // `internal/metrics.MustRegisterDiskMonitor`: const `path` label.
        register(
            registry,
            DiskCollector::new(
                repos_dir,
                vec![("path", repos_dir.to_string_lossy().into_owned())],
                (
                    "src_disk_space_available_bytes",
                    "Amount of free space disk space.",
                ),
                ("src_disk_space_total_bytes", "Amount of total disk space."),
                false,
            ),
        );

        Self {
            echo_duration,
            backend_concurrent,
            backend_duration,
            backend_total,
            backend_errors,
            exec_running,
            exec_duration,
            exec_high_memory,
            gitservice_running,
            gitservice_duration,
            search_running,
            search_duration,
            search_latency,
        }
    }

    /// Register every family on the process-wide registry, once.
    pub fn global(repos_dir: &Path) -> &'static Metrics {
        METRICS.get_or_init(|| Metrics::new(sg_metrics::registry(), repos_dir))
    }

    /// The process-wide metrics, when the service has started (unit tests and
    /// helper code run without them and record nothing).
    pub fn global_opt() -> Option<&'static Metrics> {
        METRICS.get()
    }

    pub fn echo_observed(&self, seconds: f64) {
        self.echo_duration.set(seconds);
    }

    fn exec_started(&'static self, cmd: &str) -> ExecGuard {
        self.exec_running.with_label_values(&[cmd]).inc();
        ExecGuard {
            metrics: self,
            cmd: cmd.to_owned(),
            started: Instant::now(),
            armed: true,
        }
    }

    fn high_memory_usage(&self, cmd: &str) {
        self.exec_high_memory.with_label_values(&[cmd]).inc();
    }

    fn search_started(&self) {
        self.search_running.inc();
    }

    fn search_finished(&self, seconds: f64, failed: bool) {
        self.search_duration
            .with_label_values(&[if failed { "true" } else { "false" }])
            .observe(seconds);
        self.search_running.dec();
    }

    fn search_latency_observed(&self, seconds: f64) {
        self.search_latency.observe(seconds);
    }

    fn gitservice_started(&'static self, svc: &'static str) -> GitserviceGuard {
        self.gitservice_running.with_label_values(&[svc]).inc();
        GitserviceGuard {
            metrics: self,
            svc,
            started: Instant::now(),
            error: true,
        }
    }
}

static METRICS: OnceLock<Metrics> = OnceLock::new();

/// Records one backend operation for the baseline RED metrics
/// (`src_gitserver_backend_*`): the `op` label is the kebab-case operation
/// name from `cmd/gitserver/internal/git/observability.go`, the concurrency
/// gauge is held for the operation's lifetime, and the duration/count/error
/// counters are updated when the guard is finished.
pub struct BackendGuard {
    metrics: &'static Metrics,
    /// Kebab-case label used by the RED metrics (`total`, `duration`,
    /// `errors_total`).
    op: &'static str,
    /// Method-name label used by the concurrency gauge.
    concurrent: &'static str,
    started: Instant,
    finished: bool,
}

impl BackendGuard {
    /// `observation.REDMetrics.Observe`: missing revisions and missing files
    /// are filtered out of the metrics by the baseline `ErrorFilter`, so they
    /// count as successes here; every other error increments the error
    /// counter and never observes a duration.
    pub fn finish(mut self, error: Option<&crate::git::GitError>) {
        self.record(error);
    }

    fn record(&mut self, error: Option<&crate::git::GitError>) {
        if self.finished {
            return;
        }
        self.finished = true;
        self.metrics
            .backend_concurrent
            .with_label_values(&[self.concurrent])
            .dec();
        let filtered = matches!(
            error,
            None | Some(crate::git::GitError::RevisionNotFound(_))
                | Some(crate::git::GitError::FileNotFound)
        );
        if filtered {
            self.metrics
                .backend_duration
                .with_label_values(&[self.op])
                .observe(self.started.elapsed().as_secs_f64());
            self.metrics
                .backend_total
                .with_label_values(&[self.op])
                .inc();
        } else {
            self.metrics
                .backend_errors
                .with_label_values(&[self.op])
                .inc();
            // `Count.Add(0)` creates the child series without counting a
            // successful invocation.
            self.metrics
                .backend_total
                .with_label_values(&[self.op])
                .inc_by(0.0);
        }
    }
}

impl Drop for BackendGuard {
    fn drop(&mut self) {
        // Streaming operations finish when their child is dropped (the
        // baseline records on iterator Close, which reports no error).
        self.record(None);
    }
}

/// Begin one backend operation; `None` when the service has not started.
/// `op` is the kebab-case RED label, `concurrent` the method name used by the
/// baseline concurrency gauge.
pub fn backend_started(op: &'static str, concurrent: &'static str) -> Option<BackendGuard> {
    let metrics = Metrics::global_opt()?;
    metrics
        .backend_concurrent
        .with_label_values(&[concurrent])
        .inc();
    Some(BackendGuard {
        metrics,
        op,
        concurrent,
        started: Instant::now(),
        finished: false,
    })
}

/// Records one `git` execution for the baseline `exec_*` metrics: the
/// running gauge is incremented when the child starts and the duration is
/// observed when the guard is dropped (after the child is waited on or
/// killed). `None` when the service has not started (unit tests).
pub fn exec_started(args: &[std::ffi::OsString]) -> Option<ExecGuard> {
    let cmd = args.first()?.to_string_lossy();
    Some(Metrics::global_opt()?.exec_started(&cmd))
}

/// Baseline `cmdReader.trace`: a finished `git` child whose observed peak
/// memory usage exceeded `highMemoryUsageThreshold` increments the counter
/// labelled with the subcommand. Only children observed through `memcmd`
/// (`GITSERVER_MEMORY_OBSERVATION_ENABLED`) can reach this.
pub fn high_memory_usage(cmd: &str) {
    if let Some(metrics) = Metrics::global_opt() {
        metrics.high_memory_usage(cmd);
    }
}

/// Records one `git` upload-pack service request for the baseline
/// `gitservice_*` metrics: the running gauge is incremented when the trace
/// starts and the duration is observed with the final error label when the
/// guard is dropped. `None` when the service has not started (unit tests).
pub fn gitservice_started(svc: &'static str) -> Option<GitserviceGuard> {
    Some(Metrics::global_opt()?.gitservice_started(svc))
}

pub struct ExecGuard {
    metrics: &'static Metrics,
    cmd: String,
    started: Instant,
    armed: bool,
}

impl ExecGuard {
    /// The child never started: Go returns before incrementing the gauge, so
    /// undo the increment and record nothing.
    pub fn failed_to_start(mut self) {
        self.armed = false;
        self.metrics
            .exec_running
            .with_label_values(&[&self.cmd])
            .dec();
    }
}

impl Drop for ExecGuard {
    fn drop(&mut self) {
        if self.armed {
            self.metrics
                .exec_running
                .with_label_values(&[&self.cmd])
                .dec();
            self.metrics
                .exec_duration
                .with_label_values(&[&self.cmd])
                .observe(self.started.elapsed().as_secs_f64());
        }
    }
}

/// One `gitservice.ServeHTTP` call. Defaults to the failing outcome so an
/// aborted stream is labelled like the baseline's cancelled `cmd.Run`.
pub struct GitserviceGuard {
    metrics: &'static Metrics,
    svc: &'static str,
    started: Instant,
    error: bool,
}

impl GitserviceGuard {
    pub fn success(mut self) {
        self.error = false;
    }
}

impl Drop for GitserviceGuard {
    fn drop(&mut self) {
        self.metrics
            .gitservice_running
            .with_label_values(&[self.svc])
            .dec();
        self.metrics
            .gitservice_duration
            .with_label_values(&[self.svc, if self.error { "true" } else { "false" }])
            .observe(self.started.elapsed().as_secs_f64());
    }
}

/// Start measuring one `Search`. Returns the finish callback, which observes
/// `search_duration_seconds{error}` and, on the first match, the latency
/// histogram.
pub fn search_started() -> Option<SearchGuard> {
    let metrics = Metrics::global_opt()?;
    metrics.search_started();
    Some(SearchGuard {
        metrics,
        started: Instant::now(),
        latency_observed: false,
        finished: false,
    })
}

pub struct SearchGuard {
    metrics: &'static Metrics,
    started: Instant,
    latency_observed: bool,
    finished: bool,
}

impl SearchGuard {
    /// `searchLatency` is observed once, when the first result is ready.
    pub fn first_match(&mut self) {
        if !self.latency_observed {
            self.latency_observed = true;
            self.metrics
                .search_latency_observed(self.started.elapsed().as_secs_f64());
        }
    }

    pub fn finish(mut self, failed: bool) {
        self.record(failed);
    }

    fn record(&mut self, failed: bool) {
        if !self.finished {
            self.finished = true;
            self.metrics
                .search_finished(self.started.elapsed().as_secs_f64(), failed);
        }
    }
}

impl Drop for SearchGuard {
    fn drop(&mut self) {
        // An aborted search (task cancelled) still decrements the gauge,
        // like the baseline's deferred end.
        self.record(true);
    }
}

/// Baseline `RegisterEchoMetric`: every 10s, time how long spawning `echo`
/// takes. The first sample lands after the first sleep, like the Go loop.
pub async fn echo_loop(metrics: &'static Metrics, shutdown: tokio_util::sync::CancellationToken) {
    loop {
        tokio::select! {
            _ = shutdown.cancelled() => return,
            () = tokio::time::sleep(Duration::from_secs(10)) => {}
        }
        let started = Instant::now();
        let status = tokio::process::Command::new("echo")
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status()
            .await;
        match status {
            Ok(status) if status.success() => {
                metrics.echo_observed(started.elapsed().as_secs_f64());
            }
            Ok(status) => tracing::warn!(%status, "exec measurement failed"),
            Err(err) => tracing::warn!(error = %err, "exec measurement failed"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn type_name(family: &MetricFamily) -> String {
        format!("{:?}", family.get_field_type()).to_lowercase()
    }

    #[test]
    fn registers_plain_baseline_families() {
        let registry = Registry::new();
        let dir = Path::new(env!("CARGO_MANIFEST_DIR"));
        let _metrics = Metrics::new(&registry, dir);
        let families = registry.gather();
        let find = |name: &str| {
            families
                .iter()
                .find(|family| family.get_name() == name)
                .unwrap_or_else(|| panic!("missing {name}"))
        };

        for (name, kind) in [
            ("src_gitserver_echo_duration_seconds", "gauge"),
            ("src_gitserver_clone_queue", "gauge"),
            ("src_gitserver_repo_cloned", "counter"),
            ("src_gitserver_repo_cloned_failed", "counter"),
            ("src_gitserver_repo_corrupted", "counter"),
            ("src_gitserver_repo_wrong_shard", "gauge"),
            ("src_gitserver_repo_wrong_shard_deleted", "counter"),
            ("src_gitserver_repos_recloned", "counter"),
            ("src_gitserver_janitor_running", "gauge"),
            ("src_gitserver_janitor_duration_seconds", "histogram"),
            ("src_gitserver_non_existing_repos_removed", "counter"),
            ("src_gitserver_search_running", "gauge"),
            ("src_gitserver_search_latency_seconds", "histogram"),
            ("src_gitserver_exec_blocked_command_received", "counter"),
            ("src_gitserver_bad_commit_error", "counter"),
            ("src_gitserver_disk_space_available", "gauge"),
            ("src_gitserver_disk_space_total", "gauge"),
            ("src_disk_space_available_bytes", "gauge"),
            ("src_disk_space_total_bytes", "gauge"),
        ] {
            assert_eq!(type_name(find(name)), kind, "{name}");
        }

        // Vector families behave like promauto: registered, but no series
        // until first observed.
        for name in [
            "src_gitserver_repos_removed",
            "src_gitserver_janitor_job_duration_seconds",
            "src_gitserver_gitservice_duration_seconds",
            "src_gitserver_exec_duration_seconds",
            "src_gitserver_backend_total",
            "vcssyncer_clone_duration_seconds",
        ] {
            assert!(
                !families.iter().any(|family| family.get_name() == name),
                "{name} should have no series yet"
            );
        }

        let disk = find("src_gitserver_disk_space_available");
        assert_eq!(
            disk.get_help(),
            "Amount of free space disk space on the repos mount."
        );
        let sample = &disk.get_metric()[0];
        assert!(sample.get_gauge().get_value() > 0.0);
        assert!(sample.get_label().is_empty());

        let monitor = find("src_disk_space_available_bytes");
        assert_eq!(monitor.get_help(), "Amount of free space disk space.");
        let sample = &monitor.get_metric()[0];
        assert_eq!(sample.get_label()[0].get_name(), "path");
        assert_eq!(sample.get_label()[0].get_value(), dir.to_str().unwrap());
        assert!(sample.get_gauge().get_value() > 0.0);
    }

    #[test]
    fn disk_collector_emits_nothing_when_stat_fails() {
        let dir = Path::new("/nonexistent/sg-gitserver-metrics-test");

        let registry = Registry::new();
        register(
            &registry,
            DiskCollector::new(
                dir,
                vec![("path", dir.display().to_string())],
                (
                    "src_disk_space_available_bytes",
                    "Amount of free space disk space.",
                ),
                ("src_disk_space_total_bytes", "Amount of total disk space."),
                false,
            ),
        );
        assert!(registry.gather().is_empty());

        let registry = Registry::new();
        register(
            &registry,
            DiskCollector::new(
                dir,
                Vec::new(),
                ("src_gitserver_disk_space_available", "free"),
                ("src_gitserver_disk_space_total", "total"),
                true,
            ),
        );
        let families = registry.gather();
        assert_eq!(families.len(), 2);
        for family in &families {
            assert_eq!(family.get_metric()[0].get_gauge().get_value(), 0.0);
        }
    }

    #[test]
    fn buckets_match_the_baseline() {
        let buckets = exponential_buckets_range(0.01, 60.0, 12);
        assert_eq!(buckets.len(), 12);
        assert!((buckets[0] - 0.01).abs() < f64::EPSILON);
        assert!((buckets[11] - 60.0).abs() < 1e-9);
        assert!(buckets.windows(2).all(|pair| pair[0] < pair[1]));

        let gitservice = gitservice_buckets();
        assert_eq!(gitservice.len(), 9);
        assert!((gitservice[0] - 0.1).abs() < f64::EPSILON);
        assert!((gitservice[8] - 6553.6).abs() < 1e-6);

        let fetch = fetch_buckets();
        assert_eq!(fetch.len(), 37);
        assert_eq!(fetch[13], 60.0);
        assert_eq!(fetch[36], 6960.0);
    }
}
