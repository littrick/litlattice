#![forbid(unsafe_code)]

//! `cmd/worker/internal/codeintel/autoindexing_summary.go`: the
//! `codeintel-autoindexing-summary-builder` job.
//!
//! The job owns `codeintel.SummaryConfigInst` (`summary.Config`) and returns
//! the single routine built by `autoindexing.NewSummaryBuilder`:
//!
//! | routine | interval | work |
//! |---|---|---|
//! | `codeintel.autoindexing-summary-builder` | `CODEINTEL_AUTOINDEXING_SUMMARY_BUILDER_INTERVAL` (default `30m`) | cache an auto-indexing summary for the top `CODEINTEL_AUTOINDEXING_DASHBOARD_NUM_REPOSITORIES` (default `100`) repositories by search activity |
//!
//! One handler iteration (`summary.NewSummaryBuilder`'s closure) is:
//!
//! 1. `TopRepositoriesToConfigure(limit)`;
//! 2. per repository, `GetRecentUploadsSummary` and
//!    `GetRecentAutoIndexJobsSummary`;
//! 3. if `conf.CodeIntelAutoIndexingEnabled()` (site setting
//!    `codeIntelAutoIndexing.enabled`, default false): infer index jobs from
//!    the repository structure and populate `availableIndexers`;
//! 4. `SetConfigurationSummary(repositoryID, count, availableIndexers)`;
//! 5. `TruncateConfigurationSummary(limit)`.
//!
//! Inference (`jobselector.JobSelector.InferIndexJobsFromRepositoryStructure`,
//! the gitserver-backed repository-structure scan, `PopulateInferredAvailableIndexers`
//! and the `inference.LimitError` "skip this repository" path) is **not
//! ported**. With the setting disabled — the default — the baseline writes an
//! empty `{}` payload, which [`SummaryHandler`] reproduces exactly. With the
//! setting enabled the handler fails loudly instead of writing a summary the
//! baseline would have populated (see `06_worker.md` §34).

use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use sg_config::LiveConfig;
use sg_core::{ConfigError, EnvReader};
use sqlx::PgPool;
use tokio_util::sync::CancellationToken;

use super::{get_interval, Job, JobConfig, JobDeps};
use crate::config::combine_errors;
use crate::framework::periodic::{Handler, HandlerError, PeriodicRoutine};
use crate::framework::BackgroundRoutine;

/// `autoindexingSummaryBuilder.Description`.
pub const JOB_DESCRIPTION: &str = "";

/// `summary.NewSummaryBuilder`'s `goroutine.WithName`.
pub const ROUTINE_NAME: &str = "codeintel.autoindexing-summary-builder";
/// `summary.NewSummaryBuilder`'s `goroutine.WithDescription`.
pub const ROUTINE_DESCRIPTION: &str =
    "build an auto-indexing summary over repositories with high search activity";

/// `CODEINTEL_AUTOINDEXING_SUMMARY_BUILDER_INTERVAL` (default `30m`).
pub const INTERVAL_VAR: &str = "CODEINTEL_AUTOINDEXING_SUMMARY_BUILDER_INTERVAL";
/// `CODEINTEL_AUTOINDEXING_DASHBOARD_NUM_REPOSITORIES` (default `100`).
pub const NUM_REPOSITORIES_VAR: &str = "CODEINTEL_AUTOINDEXING_DASHBOARD_NUM_REPOSITORIES";

const DEFAULT_INTERVAL: &str = "30m";
const DEFAULT_NUM_REPOSITORIES: &str = "100";

/// `summary.Config.Load`'s interval description.
const INTERVAL_DESCRIPTION: &str =
    "How frequently to run the auto-indexing summary builder routine.";
/// `summary.Config.Load`'s repository description (the baseline says
/// "edashboard"; kept verbatim).
const NUM_REPOSITORIES_DESCRIPTION: &str =
    "The number of repositories to use to populate the global code intelligence edashboard.";

/// `summary.Config`.
#[derive(Debug, Default)]
pub struct SummaryBuilderConfig {
    interval: Mutex<Duration>,
    num_repositories: Mutex<i64>,
    errs: Mutex<Vec<String>>,
}

impl SummaryBuilderConfig {
    pub fn interval(&self) -> Duration {
        *self
            .interval
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    pub fn num_repositories(&self) -> i64 {
        *self
            .num_repositories
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

impl JobConfig for SummaryBuilderConfig {
    fn load(&self, env: &mut EnvReader) {
        let mut errors = Vec::new();

        let interval = get_interval(
            env,
            INTERVAL_VAR,
            DEFAULT_INTERVAL,
            INTERVAL_DESCRIPTION,
            &mut errors,
        );

        let raw = env.get(
            NUM_REPOSITORIES_VAR,
            DEFAULT_NUM_REPOSITORIES,
            NUM_REPOSITORIES_DESCRIPTION,
        );
        let num_repositories = match parse_go_int(&raw) {
            Ok(value) => value,
            Err(kind) => {
                errors.push(format!(
                    "invalid int {raw:?} for {NUM_REPOSITORIES_VAR}: {kind}"
                ));
                0
            }
        };

        *self
            .interval
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = interval;
        *self
            .num_repositories
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = num_repositories;
        *self
            .errs
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = errors;
    }

    fn validate(&self) -> Result<(), ConfigError> {
        let errs = self
            .errs
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        match combine_errors(&errs) {
            Some(message) => Err(ConfigError::new(message)),
            None => Ok(()),
        }
    }
}

/// `strconv.ParseInt(raw, 10, 64)`; the error text is the `NumError`'s
/// `Error()` (`strconv.ParseInt: parsing %q: <err>`).
fn parse_go_int(raw: &str) -> Result<i64, String> {
    match raw.parse::<i64>() {
        Ok(value) => Ok(value),
        Err(err) => {
            let reason = match err.kind() {
                std::num::IntErrorKind::PosOverflow | std::num::IntErrorKind::NegOverflow => {
                    "value out of range"
                }
                _ => "invalid syntax",
            };
            Err(format!("strconv.ParseInt: parsing {raw:?}: {reason}"))
        }
    }
}

/// What one handler iteration observed and wrote, for tests and the M9.8v
/// differential harness. The baseline closure keeps these in locals.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct RunReport {
    pub top: Vec<sg_db::codeintel_autoindexing_summary::RepositoryWithCount>,
    pub uploads: Vec<(
        i32,
        Vec<sg_db::codeintel_autoindexing_summary::NamespaceGroup>,
    )>,
    pub indexes: Vec<(
        i32,
        Vec<sg_db::codeintel_autoindexing_summary::NamespaceGroup>,
    )>,
}

/// The `NewSummaryBuilder` handler.
pub struct SummaryHandler {
    frontend: PgPool,
    live_config: Arc<LiveConfig>,
    config: Arc<SummaryBuilderConfig>,
}

impl SummaryHandler {
    pub fn new(
        frontend: PgPool,
        live_config: Arc<LiveConfig>,
        config: Arc<SummaryBuilderConfig>,
    ) -> Self {
        Self {
            frontend,
            live_config,
            config,
        }
    }

    /// One handler iteration; see the module docs for the five steps.
    pub async fn run_once(&self) -> Result<RunReport, String> {
        use sg_db::codeintel_autoindexing_summary as store;

        let limit = self.config.num_repositories();
        let top = store::top_repositories_to_configure(&self.frontend, limit)
            .await
            .map_err(|err| err.to_string())?;

        let mut report = RunReport {
            top: top.clone(),
            ..RunReport::default()
        };

        for repository in &top {
            let uploads = store::recent_uploads_summary(&self.frontend, repository.repository_id)
                .await
                .map_err(|err| err.to_string())?;
            let indexes =
                store::recent_auto_index_jobs_summary(&self.frontend, repository.repository_id)
                    .await
                    .map_err(|err| err.to_string())?;

            // `autoIndexingEnabled()` is read inside the loop, so a
            // configuration change applies to the remaining repositories.
            // The baseline then runs inference; that slice is unported.
            if self
                .live_config
                .current()
                .site
                .codeintel_autoindexing_enabled()
            {
                return Err(
                    "codeIntelAutoIndexing.enabled is true, but auto-indexing inference is not \
                     ported by the Rust worker"
                        .to_owned(),
                );
            }

            report.uploads.push((repository.repository_id, uploads));
            report.indexes.push((repository.repository_id, indexes));

            // `json.Marshal(map[string]AvailableIndexer{})` → `{}`.
            store::set_configuration_summary(
                &self.frontend,
                repository.repository_id,
                repository.count,
                &serde_json::json!({}),
            )
            .await
            .map_err(|err| err.to_string())?;
        }

        store::truncate_configuration_summary(&self.frontend, limit)
            .await
            .map_err(|err| err.to_string())?;

        Ok(report)
    }
}

#[async_trait]
impl Handler for SummaryHandler {
    async fn handle(&self, _ctx: CancellationToken) -> Result<(), HandlerError> {
        self.run_once().await.map(|_| ()).map_err(HandlerError::new)
    }
}

/// `cmd/worker/internal/codeintel.autoindexingSummaryBuilder`.
#[derive(Debug, Default)]
pub struct AutoIndexingSummaryBuilder {
    config: Arc<SummaryBuilderConfig>,
}

impl AutoIndexingSummaryBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    /// The job's `codeintel.SummaryConfigInst`.
    pub fn config(&self) -> &Arc<SummaryBuilderConfig> {
        &self.config
    }
}

#[async_trait]
impl Job for AutoIndexingSummaryBuilder {
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
        let handler = Arc::new(SummaryHandler::new(
            deps.frontend.clone(),
            Arc::clone(&deps.live_config),
            Arc::clone(&self.config),
        ));
        let routine = PeriodicRoutine::new(handler)
            .with_name(ROUTINE_NAME)
            .with_description(ROUTINE_DESCRIPTION)
            .with_interval(self.config.interval());

        Ok(vec![routine.into()])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn job_metadata_matches_the_baseline() {
        let job = AutoIndexingSummaryBuilder::new();
        assert_eq!(job.description(), JOB_DESCRIPTION);
        assert_eq!(job.configs().len(), 1);
    }

    #[test]
    fn config_defaults_match_the_baseline() {
        let config = SummaryBuilderConfig::default();
        // Env vars are process-global; other tests do not set these names.
        config.load(&mut EnvReader::new());
        assert_eq!(config.interval(), Duration::from_secs(30 * 60));
        assert_eq!(config.num_repositories(), 100);
        assert!(config.validate().is_ok());
    }

    #[test]
    fn parse_go_int_matches_strconv() {
        assert_eq!(parse_go_int("6996"), Ok(6996));
        assert_eq!(parse_go_int("+42"), Ok(42));
        assert_eq!(parse_go_int("-7"), Ok(-7));
        assert_eq!(
            parse_go_int("abc").expect_err("invalid syntax"),
            r#"strconv.ParseInt: parsing "abc": invalid syntax"#
        );
        assert_eq!(
            parse_go_int("99999999999999999999").expect_err("out of range"),
            r#"strconv.ParseInt: parsing "99999999999999999999": value out of range"#
        );
    }

    #[tokio::test]
    async fn job_builds_the_periodic_routine() {
        let job = AutoIndexingSummaryBuilder::new();
        job.config().load(&mut EnvReader::new());
        let deps = JobDeps {
            frontend: sqlx::postgres::PgPoolOptions::new()
                .connect_lazy("postgres://unused@localhost/unused")
                .expect("lazy pool"),
            live_config: Arc::new(LiveConfig::new(sg_config::Unified::default())),
            keyring: Arc::new(sg_keyring::Keyring::default()),
        };
        let routines = job
            .routines(&deps, CancellationToken::new())
            .await
            .expect("routines");

        assert_eq!(routines.len(), 1);
        assert_eq!(routines[0].name(), ROUTINE_NAME);
        assert_eq!(routines[0].description(), ROUTINE_DESCRIPTION);
        assert_eq!(routines[0].interval(), Duration::from_secs(30 * 60));
    }
}
