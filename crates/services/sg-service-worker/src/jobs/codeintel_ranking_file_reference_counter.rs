#![forbid(unsafe_code)]

//! `cmd/worker/internal/codeintel/ranking.go` and
//! `internal/codeintel/ranking/internal/background/{init,mapper,reducer,janitor}`:
//! the `codeintel-ranking-file-reference-counter` job.
//!
//! The baseline job registers twelve routines (exporter, coordinator,
//! mapper ×2, reducer, janitor ×7) and five configs. This port registers the
//! eleven routines whose dependencies are now available in the baseline order,
//! and still loads and validates all five configs so the worker's environment
//! variables keep their meaning:
//!
//! * `codeintel.ranking.symbol-exporter`
//!   (`exporter.exportRankingGraph`: SCIP documents → ranking definitions,
//!   references and initial path ranks; the document stream reads the
//!   code-intel database from `ServiceConnections.CodeIntelPostgresDSN`,
//!   while the writes stay on the frontend database);
//! * `codeintel.ranking.file-reference-count-mapper`
//!   (`mapper.mapRankingGraph`: `InsertPathCountInputs`);
//! * `codeintel.ranking.file-reference-count-seed-mapper`
//!   (`mapper.mapInitializerRankingGraph`: `InsertInitialPathCounts`);
//! * `codeintel.ranking.file-reference-count-reducer`
//!   (`reducer.reduceRankingGraph`: `InsertPathRanks`);
//! * the seven `codeintel.ranking.*-janitor` routines
//!   ([`JanitorKind`]).
//!
//! Deferred (documented in
//! `dev/rs/docs/overviews/m9_8dr_ranking_background_jobs_overview.md` §2.3):
//!
//! * `codeintel.ranking.coordinator` needs a cron expression parser
//!   (`cronexpr`, default `@weekly`); the workspace has no cron crate.
//!
//! Every handler is gated on
//! `conf.CodeIntelRankingDocumentReferenceCountsEnabled()` (site setting
//! `codeIntelRanking.documentReferenceCountsEnabled`, default `false`): when
//! off the tick is a no-op and never touches the database. Graph keys come
//! from `conf.CodeIntelRankingDocumentReferenceCountsGraphKey()` (site
//! setting `codeIntelRanking.documentReferenceCountsGraphKey`, default
//! `"dev"`) encoded per `rankingshared.NewGraphKey`/
//! `DerivativeGraphKeyFromPrefix`. The baseline's pipeline/janitor metrics
//! (records processed/altered) are not ported; the store return values are
//! still computed, and the differential harness observes the rows instead.

use std::collections::HashSet;
use std::io::Read;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use flate2::read::GzDecoder;
use prost::Message;
use sg_config::{LiveConfig, SiteConfiguration};
use sg_core::{ConfigError, EnvReader};
use sg_db::codeintel_ranking;
use sg_scip::canonicalize_symbol;
use sg_scip::scip::{Document, SymbolRole};
use sqlx::{PgConnection, PgPool, Row};
use tokio_stream::StreamExt;
use tokio_util::sync::CancellationToken;

use super::{get_interval, Job, JobConfig, JobDeps};
use crate::config::combine_errors;
use crate::framework::periodic::{Handler, HandlerError, PeriodicRoutine};
use crate::framework::BackgroundRoutine;

/// `rankingJob.Description`.
pub const JOB_DESCRIPTION: &str = "";

/// `exporter.NewSymbolExporter`'s routine name.
pub const EXPORTER_ROUTINE_NAME: &str = "codeintel.ranking.symbol-exporter";
/// `exporter.NewSymbolExporter`'s routine description.
pub const EXPORTER_ROUTINE_DESCRIPTION: &str =
    "Exports SCIP data to ranking definitions and reference tables.";

/// `mapper.NewMapper`'s routine name.
pub const MAPPER_ROUTINE_NAME: &str = "codeintel.ranking.file-reference-count-mapper";
/// `mapper.NewMapper`'s routine description.
pub const MAPPER_ROUTINE_DESCRIPTION: &str =
    "Joins ranking definition and references together to create document path count records.";

/// `mapper.NewSeedMapper`'s routine name.
pub const SEED_MAPPER_ROUTINE_NAME: &str = "codeintel.ranking.file-reference-count-seed-mapper";
/// `mapper.NewSeedMapper`'s routine description.
pub const SEED_MAPPER_ROUTINE_DESCRIPTION: &str =
    "Adds initial zero counts to files that may not contain any known references.";

/// `reducer.NewReducer`'s routine name.
pub const REDUCER_ROUTINE_NAME: &str = "codeintel.ranking.file-reference-count-reducer";
/// `reducer.NewReducer`'s routine description.
pub const REDUCER_ROUTINE_DESCRIPTION: &str =
    "Aggregates records from `codeintel_ranking_path_counts_inputs` into `codeintel_path_ranks`.";

/// `janitor.NewExportedUploadsJanitor`'s routine name.
pub const EXPORTED_UPLOADS_JANITOR_ROUTINE_NAME: &str =
    "codeintel.ranking.exported-uploads-janitor";
/// `janitor.NewDeletedUploadsJanitor`'s routine name.
pub const DELETED_UPLOADS_JANITOR_ROUTINE_NAME: &str =
    "codeintel.ranking.deleted-exported-uploads-janitor";
/// `janitor.NewAbandonedExportedUploadsJanitor`'s routine name.
pub const ABANDONED_UPLOADS_JANITOR_ROUTINE_NAME: &str =
    "codeintel.ranking.abandoned-exported-uploads-janitor";
/// `janitor.NewProcessedReferencesJanitor`'s routine name.
pub const PROCESSED_REFERENCES_JANITOR_ROUTINE_NAME: &str =
    "codeintel.ranking.processed-references-janitor";
/// `janitor.NewProcessedPathsJanitor`'s routine name.
pub const PROCESSED_PATHS_JANITOR_ROUTINE_NAME: &str = "codeintel.ranking.processed-paths-janitor";
/// `janitor.NewRankCountsJanitor`'s routine name.
pub const RANK_COUNTS_JANITOR_ROUTINE_NAME: &str = "codeintel.ranking.rank-counts-janitor";
/// `janitor.NewRankJanitor`'s routine name.
pub const RANK_JANITOR_ROUTINE_NAME: &str = "codeintel.ranking.rank-janitor";

/// `exporter.Config.Load`'s interval variable.
pub const EXPORTER_INTERVAL_VAR: &str = "CODEINTEL_RANKING_SYMBOL_EXPORTER_INTERVAL";
/// `exporter.Config.Load`'s read batch size variable.
pub const EXPORTER_READ_BATCH_SIZE_VAR: &str = "CODEINTEL_RANKING_SYMBOL_EXPORTER_READ_BATCH_SIZE";
/// `exporter.Config.Load`'s write batch size variable.
pub const EXPORTER_WRITE_BATCH_SIZE_VAR: &str =
    "CODEINTEL_RANKING_SYMBOL_EXPORTER_WRITE_BATCH_SIZE";
/// `coordinator.Config.Load`'s interval variable.
pub const COORDINATOR_INTERVAL_VAR: &str = "CODEINTEL_RANKING_COORDINATOR_INTERVAL";
/// `mapper.Config.Load`'s interval variable.
pub const MAPPER_INTERVAL_VAR: &str = "CODEINTEL_RANKING_MAPPER_INTERVAL";
/// `mapper.Config.Load`'s batch size variable.
pub const MAPPER_BATCH_SIZE_VAR: &str = "CODEINTEL_RANKING_MAPPER_BATCH_SIZE";
/// `reducer.Config.Load`'s interval variable.
pub const REDUCER_INTERVAL_VAR: &str = "CODEINTEL_RANKING_REDUCER_INTERVAL";
/// `reducer.Config.Load`'s batch size variable.
pub const REDUCER_BATCH_SIZE_VAR: &str = "CODEINTEL_RANKING_REDUCER_BATCH_SIZE";
/// `janitor.Config.Load`'s interval variable.
pub const JANITOR_INTERVAL_VAR: &str = "CODEINTEL_RANKING_JANITOR_INTERVAL";

const EXPORTER_INTERVAL_DESCRIPTION: &str =
    "How frequently to serialize a batch of the code intel graph for ranking.";
const EXPORTER_READ_BATCH_SIZE_DESCRIPTION: &str = "How many uploads to process at once.";
const EXPORTER_WRITE_BATCH_SIZE_DESCRIPTION: &str =
    "The number of definitions and references to populate the ranking graph per batch.";
const COORDINATOR_INTERVAL_DESCRIPTION: &str = "How frequently to run the ranking coordinator.";
const MAPPER_INTERVAL_DESCRIPTION: &str = "How frequently to run the ranking mapper.";
const MAPPER_BATCH_SIZE_DESCRIPTION: &str = "How many definitions and references to map at once.";
const REDUCER_INTERVAL_DESCRIPTION: &str = "How frequently to run the ranking reducer.";
const REDUCER_BATCH_SIZE_DESCRIPTION: &str = "How many path counts to reduce at once.";
const JANITOR_INTERVAL_DESCRIPTION: &str = "How frequently to run the ranking janitor.";

/// `janitor.vacuumUploadsBatchSize`.
pub const VACUUM_UPLOADS_BATCH_SIZE: i64 = 100;
/// `janitor.vacuumMiscRecordsBatchSize`.
pub const VACUUM_MISC_RECORDS_BATCH_SIZE: i64 = 10_000;

/// `conf.CodeIntelRankingDocumentReferenceCountsEnabled`: the site setting
/// `codeIntelRanking.documentReferenceCountsEnabled`, defaulting to `false`
/// when absent (a non-boolean value is treated as absent, like the schema
/// generated config field's zero value).
pub fn document_reference_counts_enabled(site: &SiteConfiguration) -> bool {
    site.raw()
        .get("codeIntelRanking.documentReferenceCountsEnabled")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false)
}

/// `conf.CodeIntelRankingDocumentReferenceCountsGraphKey`: the site setting
/// `codeIntelRanking.documentReferenceCountsGraphKey`, defaulting to `"dev"`
/// when absent or empty.
pub fn document_reference_counts_graph_key(site: &SiteConfiguration) -> String {
    site.raw()
        .get("codeIntelRanking.documentReferenceCountsGraphKey")
        .and_then(serde_json::Value::as_str)
        .filter(|value| !value.is_empty())
        .unwrap_or(codeintel_ranking::DEFAULT_DOCUMENT_REFERENCE_COUNTS_GRAPH_KEY)
        .to_owned()
}

/// `rankingshared.GraphKey()`: `NewGraphKey(conf...GraphKey())`.
pub fn root_graph_key(site: &SiteConfiguration) -> String {
    codeintel_ranking::new_graph_key(&document_reference_counts_graph_key(site))
}

/// `rankingshared.DerivativeGraphKeyFromPrefix`: encodes the configured root
/// and appends the encoded prefix.
pub fn derivative_graph_key_for_prefix(site: &SiteConfiguration, prefix: &str) -> String {
    codeintel_ranking::derivative_graph_key_from_prefix(
        &document_reference_counts_graph_key(site),
        prefix,
    )
}

macro_rules! getter {
    ($name:ident, $field:ident, $ty:ty) => {
        pub fn $name(&self) -> $ty {
            *self
                .$field
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
        }
    };
}

/// `exporter.Config`.
#[derive(Debug, Default)]
pub struct ExporterConfig {
    interval: Mutex<Duration>,
    read_batch_size: Mutex<i64>,
    write_batch_size: Mutex<i64>,
    errs: Mutex<Vec<String>>,
}

impl ExporterConfig {
    getter!(interval, interval, Duration);
    getter!(read_batch_size, read_batch_size, i64);
    getter!(write_batch_size, write_batch_size, i64);
}

impl JobConfig for ExporterConfig {
    fn load(&self, env: &mut EnvReader) {
        let mut errs = Vec::new();
        let interval = get_interval(
            env,
            EXPORTER_INTERVAL_VAR,
            "30s",
            EXPORTER_INTERVAL_DESCRIPTION,
            &mut errs,
        );
        let read_batch_size = get_int(
            env,
            EXPORTER_READ_BATCH_SIZE_VAR,
            "16",
            EXPORTER_READ_BATCH_SIZE_DESCRIPTION,
            &mut errs,
        );
        let write_batch_size = get_int(
            env,
            EXPORTER_WRITE_BATCH_SIZE_VAR,
            "10000",
            EXPORTER_WRITE_BATCH_SIZE_DESCRIPTION,
            &mut errs,
        );

        *self
            .interval
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = interval;
        *self
            .read_batch_size
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = read_batch_size;
        *self
            .write_batch_size
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = write_batch_size;
        *self
            .errs
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = errs;
    }

    fn validate(&self) -> Result<(), ConfigError> {
        validate_errors(self)
    }
}

/// `coordinator.Config`.
#[derive(Debug, Default)]
pub struct CoordinatorConfig {
    interval: Mutex<Duration>,
    errs: Mutex<Vec<String>>,
}

impl CoordinatorConfig {
    getter!(interval, interval, Duration);
}

impl JobConfig for CoordinatorConfig {
    fn load(&self, env: &mut EnvReader) {
        let mut errs = Vec::new();
        let interval = get_interval(
            env,
            COORDINATOR_INTERVAL_VAR,
            "30s",
            COORDINATOR_INTERVAL_DESCRIPTION,
            &mut errs,
        );

        *self
            .interval
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = interval;
        *self
            .errs
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = errs;
    }

    fn validate(&self) -> Result<(), ConfigError> {
        validate_errors(self)
    }
}

/// `mapper.Config`.
#[derive(Debug, Default)]
pub struct MapperConfig {
    interval: Mutex<Duration>,
    batch_size: Mutex<i64>,
    errs: Mutex<Vec<String>>,
}

impl MapperConfig {
    getter!(interval, interval, Duration);
    getter!(batch_size, batch_size, i64);
}

impl JobConfig for MapperConfig {
    fn load(&self, env: &mut EnvReader) {
        let mut errs = Vec::new();
        let interval = get_interval(
            env,
            MAPPER_INTERVAL_VAR,
            "30s",
            MAPPER_INTERVAL_DESCRIPTION,
            &mut errs,
        );
        let batch_size = get_int(
            env,
            MAPPER_BATCH_SIZE_VAR,
            "100",
            MAPPER_BATCH_SIZE_DESCRIPTION,
            &mut errs,
        );

        *self
            .interval
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = interval;
        *self
            .batch_size
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = batch_size;
        *self
            .errs
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = errs;
    }

    fn validate(&self) -> Result<(), ConfigError> {
        validate_errors(self)
    }
}

/// `reducer.Config`.
#[derive(Debug, Default)]
pub struct ReducerConfig {
    interval: Mutex<Duration>,
    batch_size: Mutex<i64>,
    errs: Mutex<Vec<String>>,
}

impl ReducerConfig {
    getter!(interval, interval, Duration);
    getter!(batch_size, batch_size, i64);
}

impl JobConfig for ReducerConfig {
    fn load(&self, env: &mut EnvReader) {
        let mut errs = Vec::new();
        let interval = get_interval(
            env,
            REDUCER_INTERVAL_VAR,
            "30s",
            REDUCER_INTERVAL_DESCRIPTION,
            &mut errs,
        );
        let batch_size = get_int(
            env,
            REDUCER_BATCH_SIZE_VAR,
            "1000",
            REDUCER_BATCH_SIZE_DESCRIPTION,
            &mut errs,
        );

        *self
            .interval
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = interval;
        *self
            .batch_size
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = batch_size;
        *self
            .errs
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = errs;
    }

    fn validate(&self) -> Result<(), ConfigError> {
        validate_errors(self)
    }
}

/// `janitor.Config`.
#[derive(Debug, Default)]
pub struct JanitorConfig {
    interval: Mutex<Duration>,
    errs: Mutex<Vec<String>>,
}

impl JanitorConfig {
    getter!(interval, interval, Duration);
}

impl JobConfig for JanitorConfig {
    fn load(&self, env: &mut EnvReader) {
        let mut errs = Vec::new();
        let interval = get_interval(
            env,
            JANITOR_INTERVAL_VAR,
            "30s",
            JANITOR_INTERVAL_DESCRIPTION,
            &mut errs,
        );

        *self
            .interval
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = interval;
        *self
            .errs
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = errs;
    }

    fn validate(&self) -> Result<(), ConfigError> {
        validate_errors(self)
    }
}

fn validate_errors<T>(config: &T) -> Result<(), ConfigError>
where
    T: ConfigErrors,
{
    let errs = config
        .errors()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .clone();
    if errs.is_empty() {
        Ok(())
    } else {
        Err(ConfigError::new(combine_errors(&errs).unwrap_or_default()))
    }
}

/// Access to a config's recorded load errors.
trait ConfigErrors {
    fn errors(&self) -> &Mutex<Vec<String>>;
}

impl ConfigErrors for ExporterConfig {
    fn errors(&self) -> &Mutex<Vec<String>> {
        &self.errs
    }
}

impl ConfigErrors for CoordinatorConfig {
    fn errors(&self) -> &Mutex<Vec<String>> {
        &self.errs
    }
}

impl ConfigErrors for MapperConfig {
    fn errors(&self) -> &Mutex<Vec<String>> {
        &self.errs
    }
}

impl ConfigErrors for ReducerConfig {
    fn errors(&self) -> &Mutex<Vec<String>> {
        &self.errs
    }
}

impl ConfigErrors for JanitorConfig {
    fn errors(&self) -> &Mutex<Vec<String>> {
        &self.errs
    }
}

/// `BaseConfig.GetInt`: `strconv.ParseInt(raw, 10, 64)`, record
/// `invalid int %q for %s: %s` and return 0 on failure. Copied from
/// `codeintel_syntactic_indexing_scheduler` because `jobs/mod.rs` does not
/// export its helper.
fn get_int(
    env: &mut EnvReader,
    name: &str,
    default: &str,
    description: &str,
    errors: &mut Vec<String>,
) -> i64 {
    let raw = env.get(name, default, description);
    match raw.parse::<i64>() {
        Ok(value) => value,
        Err(err) => {
            let reason = match err.kind() {
                std::num::IntErrorKind::PosOverflow | std::num::IntErrorKind::NegOverflow => {
                    "value out of range"
                }
                _ => "invalid syntax",
            };
            errors.push(format!(
                "invalid int {raw:?} for {name}: strconv.ParseInt: parsing {raw:?}: {reason}"
            ));
            0
        }
    }
}

/// The seven janitor routines.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum JanitorKind {
    /// `janitor.NewExportedUploadsJanitor`.
    ExportedUploads,
    /// `janitor.NewDeletedUploadsJanitor`.
    DeletedExportedUploads,
    /// `janitor.NewAbandonedExportedUploadsJanitor`.
    AbandonedExportedUploads,
    /// `janitor.NewProcessedReferencesJanitor`.
    ProcessedReferences,
    /// `janitor.NewProcessedPathsJanitor`.
    ProcessedPaths,
    /// `janitor.NewRankCountsJanitor`.
    RankCounts,
    /// `janitor.NewRankJanitor`.
    Rank,
}

impl JanitorKind {
    /// `goroutine.WithName`.
    pub fn routine_name(self) -> &'static str {
        match self {
            Self::ExportedUploads => EXPORTED_UPLOADS_JANITOR_ROUTINE_NAME,
            Self::DeletedExportedUploads => DELETED_UPLOADS_JANITOR_ROUTINE_NAME,
            Self::AbandonedExportedUploads => ABANDONED_UPLOADS_JANITOR_ROUTINE_NAME,
            Self::ProcessedReferences => PROCESSED_REFERENCES_JANITOR_ROUTINE_NAME,
            Self::ProcessedPaths => PROCESSED_PATHS_JANITOR_ROUTINE_NAME,
            Self::RankCounts => RANK_COUNTS_JANITOR_ROUTINE_NAME,
            Self::Rank => RANK_JANITOR_ROUTINE_NAME,
        }
    }

    /// `goroutine.WithDescription`.
    pub fn routine_description(self) -> &'static str {
        match self {
            Self::ExportedUploads => {
                "Soft-deletes stale data from the ranking exported uploads table."
            }
            Self::DeletedExportedUploads => {
                "Removes soft-deleted data from the ranking exported uploads table no longer being read by a mapper process."
            }
            Self::AbandonedExportedUploads => {
                "Removes ranking exported uploads records for old graph keys."
            }
            Self::ProcessedReferences => "Removes old processed reference input records.",
            Self::ProcessedPaths => "Removes old processed path input records.",
            Self::RankCounts => "Removes old path count input records.",
            Self::Rank => "Removes stale ranking data.",
        }
    }

    /// `background.NewSymbolJanitor`'s order.
    pub const ALL: [Self; 7] = [
        Self::ExportedUploads,
        Self::DeletedExportedUploads,
        Self::AbandonedExportedUploads,
        Self::ProcessedReferences,
        Self::ProcessedPaths,
        Self::RankCounts,
        Self::Rank,
    ];
}

/// `rankingJob`.
pub struct RankingFileReferenceCounter {
    exporter: Arc<ExporterConfig>,
    coordinator: Arc<CoordinatorConfig>,
    mapper: Arc<MapperConfig>,
    reducer: Arc<ReducerConfig>,
    janitor: Arc<JanitorConfig>,
}

impl RankingFileReferenceCounter {
    pub fn new() -> Self {
        Self {
            exporter: Arc::new(ExporterConfig::default()),
            coordinator: Arc::new(CoordinatorConfig::default()),
            mapper: Arc::new(MapperConfig::default()),
            reducer: Arc::new(ReducerConfig::default()),
            janitor: Arc::new(JanitorConfig::default()),
        }
    }

    pub fn exporter_config(&self) -> &ExporterConfig {
        &self.exporter
    }

    pub fn coordinator_config(&self) -> &CoordinatorConfig {
        &self.coordinator
    }

    pub fn mapper_config(&self) -> &MapperConfig {
        &self.mapper
    }

    pub fn reducer_config(&self) -> &ReducerConfig {
        &self.reducer
    }

    pub fn janitor_config(&self) -> &JanitorConfig {
        &self.janitor
    }
}

impl Default for RankingFileReferenceCounter {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Job for RankingFileReferenceCounter {
    fn description(&self) -> String {
        JOB_DESCRIPTION.to_owned()
    }

    /// `rankingJob.Config`, in baseline order.
    fn configs(&self) -> Vec<Arc<dyn JobConfig>> {
        vec![
            Arc::clone(&self.exporter) as Arc<dyn JobConfig>,
            Arc::clone(&self.coordinator) as Arc<dyn JobConfig>,
            Arc::clone(&self.mapper) as Arc<dyn JobConfig>,
            Arc::clone(&self.reducer) as Arc<dyn JobConfig>,
            Arc::clone(&self.janitor) as Arc<dyn JobConfig>,
        ]
    }

    /// `rankingJob.Routines`. The coordinator routine is deferred (see the
    /// module docs); its config is still loaded so its environment variables
    /// keep their meaning.
    async fn routines(
        &self,
        deps: &JobDeps,
        _setup_ctx: CancellationToken,
    ) -> Result<Vec<BackgroundRoutine>, String> {
        let pool = deps.frontend.clone();
        let live_config = Arc::clone(&deps.live_config);
        let mapper_batch_size = self.mapper.batch_size();
        let reducer_batch_size = self.reducer.batch_size();

        // `codeintel.InitServices`: the exporter's lsifstore runs on the
        // separate code-intel database configured by the frontend
        // (`ServiceConnections.CodeIntelPostgresDSN`).
        let unified = deps.live_config.current();
        if unified
            .service_connections
            .code_intel_postgres_dsn
            .is_empty()
        {
            return Err("code_intel_postgres_dsn is not configured by the frontend".to_owned());
        }
        let codeintel_pool =
            sg_db::connect(&unified.service_connections.code_intel_postgres_dsn, 10)
                .await
                .map_err(|err| format!("connecting to the codeintel database: {err}"))?;

        let mut routines: Vec<BackgroundRoutine> = Vec::with_capacity(11);

        routines.push(
            PeriodicRoutine::new(Arc::new(RankingExporterHandler {
                pool: pool.clone(),
                codeintel_pool,
                live_config: Arc::clone(&live_config),
                read_batch_size: self.exporter.read_batch_size(),
                write_batch_size: self.exporter.write_batch_size(),
            }))
            .with_name(EXPORTER_ROUTINE_NAME)
            .with_description(EXPORTER_ROUTINE_DESCRIPTION)
            .with_interval(self.exporter.interval())
            .into(),
        );
        // `codeintel.ranking.coordinator` belongs here in the baseline order;
        // it is deferred until a cron expression parser is available.
        routines.push(
            PeriodicRoutine::new(Arc::new(RankingMapperHandler {
                pool: pool.clone(),
                live_config: Arc::clone(&live_config),
                batch_size: mapper_batch_size,
            }))
            .with_name(MAPPER_ROUTINE_NAME)
            .with_description(MAPPER_ROUTINE_DESCRIPTION)
            .with_interval(self.mapper.interval())
            .into(),
        );
        routines.push(
            PeriodicRoutine::new(Arc::new(RankingSeedMapperHandler {
                pool: pool.clone(),
                live_config: Arc::clone(&live_config),
                batch_size: mapper_batch_size,
            }))
            .with_name(SEED_MAPPER_ROUTINE_NAME)
            .with_description(SEED_MAPPER_ROUTINE_DESCRIPTION)
            .with_interval(self.mapper.interval())
            .into(),
        );
        routines.push(
            PeriodicRoutine::new(Arc::new(RankingReducerHandler {
                pool: pool.clone(),
                live_config: Arc::clone(&live_config),
                batch_size: reducer_batch_size,
            }))
            .with_name(REDUCER_ROUTINE_NAME)
            .with_description(REDUCER_ROUTINE_DESCRIPTION)
            .with_interval(self.reducer.interval())
            .into(),
        );

        for kind in JanitorKind::ALL {
            routines.push(
                PeriodicRoutine::new(Arc::new(RankingJanitorHandler {
                    pool: pool.clone(),
                    live_config: Arc::clone(&live_config),
                    kind,
                }))
                .with_name(kind.routine_name())
                .with_description(kind.routine_description())
                .with_interval(self.janitor.interval())
                .into(),
            );
        }

        Ok(routines)
    }
}

/// The current site configuration snapshot (`conf.Get()`).
fn current_site(live_config: &LiveConfig) -> Arc<sg_config::Unified> {
    live_config.current()
}

/// `getDocumentsByUploadIDQuery`
/// (`internal/codeintel/ranking/internal/lsifstore/stream.go`).
const GET_DOCUMENTS_BY_UPLOAD_ID_QUERY: &str = r#"
SELECT
	sid.document_path,
	sd.raw_scip_payload
FROM codeintel_scip_document_lookup sid
JOIN codeintel_scip_documents sd ON sd.id = sid.document_id
WHERE sid.upload_id = $1
ORDER BY sid.document_path
"#;

/// Errors raised by [`export_ranking_graph`]: the baseline wraps the store,
/// gzip and protobuf failures and logs them per upload.
#[derive(Debug)]
pub enum ExportError {
    Sql(sqlx::Error),
    Io(std::io::Error),
    Decode(prost::DecodeError),
}

impl std::fmt::Display for ExportError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Sql(err) => write!(f, "ranking exporter query failed: {err}"),
            Self::Io(err) => write!(f, "ranking exporter decompression failed: {err}"),
            Self::Decode(err) => write!(f, "ranking exporter SCIP decode failed: {err}"),
        }
    }
}

impl std::error::Error for ExportError {}

impl From<sqlx::Error> for ExportError {
    fn from(err: sqlx::Error) -> Self {
        Self::Sql(err)
    }
}

impl From<std::io::Error> for ExportError {
    fn from(err: std::io::Error) -> Self {
        Self::Io(err)
    }
}

impl From<prost::DecodeError> for ExportError {
    fn from(err: prost::DecodeError) -> Self {
        Self::Decode(err)
    }
}

/// `exporter.exportRankingGraph`: stream the SCIP documents of every upload
/// that has not yet been exported for the root graph key, write the ranking
/// definitions/references for each document and its initial path ranks — all
/// inside one transaction on the frontend database — and return
/// `(uploads, definitions, references)`.
///
/// The baseline wires two stores over two databases: the ranking store runs on
/// the frontend DB, while the lsifstore that streams the SCIP documents runs
/// on the code-intel DB (`ServiceConnections.CodeIntelPostgresDSN`). Both are
/// given here; the document stream uses its own transaction on
/// `codeintel_pool`, committed before the store transaction, exactly like the
/// nested `baseLsifStore.WithTransaction`. The gate, the upload selection and
/// the per-document writes follow the baseline; metrics and logs are not
/// ported.
pub async fn export_ranking_graph(
    store_pool: &PgPool,
    codeintel_pool: &PgPool,
    site: &SiteConfiguration,
    read_batch_size: i64,
    write_batch_size: i64,
) -> Result<(i64, i64, i64), ExportError> {
    if !document_reference_counts_enabled(site) {
        return Ok((0, 0, 0));
    }

    let graph_key = root_graph_key(site);
    let mut tx = store_pool.begin().await?;
    let mut lsif_tx = codeintel_pool.begin().await?;

    let uploads =
        codeintel_ranking::get_uploads_for_ranking_on(&mut *tx, &graph_key, read_batch_size)
            .await?;
    let num_uploads = uploads.len() as i64;
    let mut num_definitions = 0i64;
    let mut num_references = 0i64;

    for upload in &uploads {
        let mut document_paths: Vec<String> = Vec::new();

        // `InsertDefinitionsAndReferencesForDocument`: rows are read from the
        // code-intel database in `document_path` order and processed one
        // document at a time.
        let mut rows = sqlx::query(GET_DOCUMENTS_BY_UPLOAD_ID_QUERY)
            .bind(upload.upload_id)
            .fetch(&mut *lsif_tx);
        while let Some(row) = rows.next().await {
            let row = row?;
            let path: String = row.try_get("document_path")?;
            let compressed: Vec<u8> = row.try_get("raw_scip_payload")?;
            let document = Document::decode(decompress(&compressed)?.as_slice())?;

            let (definitions, references) = set_definitions_and_references_for_upload(
                &mut *tx,
                &graph_key,
                write_batch_size,
                upload,
                &path,
                &document,
            )
            .await?;

            document_paths.push(path);
            num_definitions += definitions;
            num_references += references;
        }

        codeintel_ranking::insert_initial_path_ranks_on(
            &mut *tx,
            upload.exported_upload_id,
            &document_paths,
            write_batch_size,
            &graph_key,
        )
        .await?;
    }

    lsif_tx.commit().await?;
    tx.commit().await?;
    Ok((num_uploads, num_definitions, num_references))
}

/// `exporter.setDefinitionsAndReferencesForUpload`: definitions first (the
/// same symbol string is emitted at most once per document), then references
/// (definition occurrences and symbols already defined in this document are
/// skipped).
async fn set_definitions_and_references_for_upload(
    conn: &mut PgConnection,
    graph_key: &str,
    batch_size: i64,
    upload: &codeintel_ranking::ExportedUpload,
    path: &str,
    document: &Document,
) -> Result<(i64, i64), sqlx::Error> {
    let document_path = join_document_path(&upload.root, path);

    let mut seen_definitions: HashSet<&str> = HashSet::new();
    let mut definitions: Vec<codeintel_ranking::RankingDefinition> = Vec::new();
    for occurrence in &document.occurrences {
        if occurrence.symbol_roles & (SymbolRole::Definition as i32) == 0 {
            continue;
        }
        if seen_definitions.contains(occurrence.symbol.as_str()) {
            continue;
        }
        if let Some(checksum) = canonicalize_symbol(&occurrence.symbol) {
            definitions.push(codeintel_ranking::RankingDefinition {
                exported_upload_id: upload.exported_upload_id,
                symbol_checksum: checksum,
                document_path: document_path.clone(),
            });
            seen_definitions.insert(occurrence.symbol.as_str());
        }
    }

    let num_definitions = definitions.len() as i64;
    codeintel_ranking::insert_definitions_for_ranking_on(conn, graph_key, &definitions).await?;

    let mut references: Vec<[u8; 16]> = Vec::new();
    for occurrence in &document.occurrences {
        if occurrence.symbol_roles & (SymbolRole::Definition as i32) != 0 {
            continue;
        }
        if seen_definitions.contains(occurrence.symbol.as_str()) {
            continue;
        }
        if let Some(checksum) = canonicalize_symbol(&occurrence.symbol) {
            references.push(checksum);
        }
    }

    let num_references = references.len() as i64;
    codeintel_ranking::insert_references_for_ranking_on(
        conn,
        graph_key,
        batch_size,
        upload.exported_upload_id,
        &references,
    )
    .await?;

    Ok((num_definitions, num_references))
}

/// `filepath.Join(upload.Root, path)`. `Path::join` does not clean `.`/`..`
/// the way Go's `filepath.Join` does; upload roots and SCIP document paths are
/// already clean, so the two agree on the inputs the exporter sees.
fn join_document_path(root: &str, path: &str) -> String {
    Path::new(root).join(path).to_string_lossy().into_owned()
}

/// `shared.Decompressor.Decompress`: gunzip the stored SCIP payload.
fn decompress(payload: &[u8]) -> Result<Vec<u8>, std::io::Error> {
    let mut decoder = GzDecoder::new(payload);
    let mut decompressed = Vec::new();
    decoder.read_to_end(&mut decompressed)?;
    Ok(decompressed)
}

/// `mapper.mapRankingGraph`: `InsertPathCountInputs` for the newest
/// derivative graph key.
pub async fn map_ranking_graph(
    pool: &PgPool,
    site: &SiteConfiguration,
    batch_size: i64,
) -> Result<(i64, i64), sqlx::Error> {
    if !document_reference_counts_enabled(site) {
        return Ok((0, 0));
    }

    let (prefix, _) = codeintel_ranking::derivative_graph_key_or_bump(pool).await?;
    let derivative_graph_key = derivative_graph_key_for_prefix(site, &prefix);

    codeintel_ranking::insert_path_count_inputs(pool, &derivative_graph_key, batch_size).await
}

/// `mapper.mapInitializerRankingGraph`: `InsertInitialPathCounts` for the
/// newest derivative graph key.
pub async fn map_initializer_ranking_graph(
    pool: &PgPool,
    site: &SiteConfiguration,
    batch_size: i64,
) -> Result<(i64, i64), sqlx::Error> {
    if !document_reference_counts_enabled(site) {
        return Ok((0, 0));
    }

    let (prefix, _) = codeintel_ranking::derivative_graph_key_or_bump(pool).await?;
    let derivative_graph_key = derivative_graph_key_for_prefix(site, &prefix);

    codeintel_ranking::insert_initial_path_counts(pool, &derivative_graph_key, batch_size).await
}

/// `reducer.reduceRankingGraph`: `InsertPathRanks` for the newest derivative
/// graph key.
pub async fn reduce_ranking_graph(
    pool: &PgPool,
    site: &SiteConfiguration,
    batch_size: i64,
) -> Result<(i64, i64), sqlx::Error> {
    if !document_reference_counts_enabled(site) {
        return Ok((0, 0));
    }

    let (prefix, _) = codeintel_ranking::derivative_graph_key_or_bump(pool).await?;
    let derivative_graph_key = derivative_graph_key_for_prefix(site, &prefix);

    codeintel_ranking::insert_path_ranks(pool, &derivative_graph_key, batch_size).await
}

/// `janitor.*`: run one janitor tick. Returns `(num_records_scanned,
/// num_records_altered)`; both are `0` when the feature is disabled.
pub async fn run_janitor(
    pool: &PgPool,
    site: &SiteConfiguration,
    kind: JanitorKind,
) -> Result<(i64, i64), sqlx::Error> {
    if !document_reference_counts_enabled(site) {
        return Ok((0, 0));
    }

    match kind {
        JanitorKind::ExportedUploads => {
            codeintel_ranking::soft_delete_stale_exported_uploads(pool, &root_graph_key(site)).await
        }
        JanitorKind::DeletedExportedUploads => {
            let (prefix, _) = codeintel_ranking::derivative_graph_key_or_bump(pool).await?;
            let derivative_graph_key = derivative_graph_key_for_prefix(site, &prefix);
            let deleted =
                codeintel_ranking::vacuum_deleted_exported_uploads(pool, &derivative_graph_key)
                    .await?;
            Ok((deleted, deleted))
        }
        JanitorKind::AbandonedExportedUploads => {
            let deleted = codeintel_ranking::vacuum_abandoned_exported_uploads(
                pool,
                &root_graph_key(site),
                VACUUM_UPLOADS_BATCH_SIZE,
            )
            .await?;
            Ok((deleted, deleted))
        }
        JanitorKind::ProcessedReferences => {
            let (prefix, _) = codeintel_ranking::derivative_graph_key_or_bump(pool).await?;
            let derivative_graph_key = derivative_graph_key_for_prefix(site, &prefix);
            let deleted = codeintel_ranking::vacuum_stale_processed_references(
                pool,
                &derivative_graph_key,
                VACUUM_MISC_RECORDS_BATCH_SIZE,
            )
            .await?;
            Ok((deleted, deleted))
        }
        JanitorKind::ProcessedPaths => {
            let (prefix, _) = codeintel_ranking::derivative_graph_key_or_bump(pool).await?;
            let derivative_graph_key = derivative_graph_key_for_prefix(site, &prefix);
            let deleted = codeintel_ranking::vacuum_stale_processed_paths(
                pool,
                &derivative_graph_key,
                VACUUM_MISC_RECORDS_BATCH_SIZE,
            )
            .await?;
            Ok((deleted, deleted))
        }
        JanitorKind::RankCounts => {
            let (prefix, _) = codeintel_ranking::derivative_graph_key_or_bump(pool).await?;
            let derivative_graph_key = derivative_graph_key_for_prefix(site, &prefix);
            let deleted = codeintel_ranking::vacuum_stale_graphs(
                pool,
                &derivative_graph_key,
                VACUUM_MISC_RECORDS_BATCH_SIZE,
            )
            .await?;
            Ok((deleted, deleted))
        }
        JanitorKind::Rank => {
            let (prefix, _) = codeintel_ranking::derivative_graph_key_or_bump(pool).await?;
            let derivative_graph_key = derivative_graph_key_for_prefix(site, &prefix);
            codeintel_ranking::vacuum_stale_ranks(pool, &derivative_graph_key).await
        }
    }
}

struct RankingExporterHandler {
    pool: PgPool,
    /// The code-intel database the SCIP document stream reads from
    /// (`ServiceConnections.CodeIntelPostgresDSN`).
    codeintel_pool: PgPool,
    live_config: Arc<LiveConfig>,
    read_batch_size: i64,
    write_batch_size: i64,
}

#[async_trait]
impl Handler for RankingExporterHandler {
    async fn handle(&self, _ctx: CancellationToken) -> Result<(), HandlerError> {
        let unified = current_site(&self.live_config);
        export_ranking_graph(
            &self.pool,
            &self.codeintel_pool,
            &unified.site,
            self.read_batch_size,
            self.write_batch_size,
        )
        .await
        .map(|_| ())
        .map_err(|err| HandlerError::new(err.to_string()))
    }
}

struct RankingMapperHandler {
    pool: PgPool,
    live_config: Arc<LiveConfig>,
    batch_size: i64,
}

#[async_trait]
impl Handler for RankingMapperHandler {
    async fn handle(&self, _ctx: CancellationToken) -> Result<(), HandlerError> {
        let unified = current_site(&self.live_config);
        map_ranking_graph(&self.pool, &unified.site, self.batch_size)
            .await
            .map(|_| ())
            .map_err(|err| HandlerError::new(err.to_string()))
    }
}

struct RankingSeedMapperHandler {
    pool: PgPool,
    live_config: Arc<LiveConfig>,
    batch_size: i64,
}

#[async_trait]
impl Handler for RankingSeedMapperHandler {
    async fn handle(&self, _ctx: CancellationToken) -> Result<(), HandlerError> {
        let unified = current_site(&self.live_config);
        map_initializer_ranking_graph(&self.pool, &unified.site, self.batch_size)
            .await
            .map(|_| ())
            .map_err(|err| HandlerError::new(err.to_string()))
    }
}

struct RankingReducerHandler {
    pool: PgPool,
    live_config: Arc<LiveConfig>,
    batch_size: i64,
}

#[async_trait]
impl Handler for RankingReducerHandler {
    async fn handle(&self, _ctx: CancellationToken) -> Result<(), HandlerError> {
        let unified = current_site(&self.live_config);
        reduce_ranking_graph(&self.pool, &unified.site, self.batch_size)
            .await
            .map(|_| ())
            .map_err(|err| HandlerError::new(err.to_string()))
    }
}

struct RankingJanitorHandler {
    pool: PgPool,
    live_config: Arc<LiveConfig>,
    kind: JanitorKind,
}

#[async_trait]
impl Handler for RankingJanitorHandler {
    async fn handle(&self, _ctx: CancellationToken) -> Result<(), HandlerError> {
        let unified = current_site(&self.live_config);
        run_janitor(&self.pool, &unified.site, self.kind)
            .await
            .map(|_| ())
            .map_err(|err| HandlerError::new(err.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::PoisonError;

    static ENV_LOCK: Mutex<()> = Mutex::new(());

    fn site(json: &str) -> SiteConfiguration {
        SiteConfiguration::parse(json).expect("valid site config")
    }

    #[test]
    fn job_metadata_matches_the_baseline() {
        let job = RankingFileReferenceCounter::new();
        assert_eq!(job.description(), JOB_DESCRIPTION);
        // exporter, coordinator, mapper, reducer, janitor
        assert_eq!(job.configs().len(), 5);
        assert_eq!(EXPORTER_ROUTINE_NAME, "codeintel.ranking.symbol-exporter");
        assert_eq!(
            EXPORTER_ROUTINE_DESCRIPTION,
            "Exports SCIP data to ranking definitions and reference tables."
        );
        assert_eq!(
            MAPPER_ROUTINE_NAME,
            "codeintel.ranking.file-reference-count-mapper"
        );
        assert_eq!(
            SEED_MAPPER_ROUTINE_NAME,
            "codeintel.ranking.file-reference-count-seed-mapper"
        );
        assert_eq!(
            REDUCER_ROUTINE_NAME,
            "codeintel.ranking.file-reference-count-reducer"
        );
    }

    #[test]
    fn janitor_names_and_order_match_the_baseline() {
        let names: Vec<&str> = JanitorKind::ALL
            .iter()
            .map(|kind| kind.routine_name())
            .collect();
        assert_eq!(
            names,
            vec![
                "codeintel.ranking.exported-uploads-janitor",
                "codeintel.ranking.deleted-exported-uploads-janitor",
                "codeintel.ranking.abandoned-exported-uploads-janitor",
                "codeintel.ranking.processed-references-janitor",
                "codeintel.ranking.processed-paths-janitor",
                "codeintel.ranking.rank-counts-janitor",
                "codeintel.ranking.rank-janitor",
            ]
        );
    }

    #[test]
    fn config_defaults_match_the_baseline() {
        let _guard = ENV_LOCK.lock().unwrap_or_else(PoisonError::into_inner);
        let job = RankingFileReferenceCounter::new();
        for config in job.configs() {
            config.load(&mut EnvReader::new());
            assert!(config.validate().is_ok());
        }

        assert_eq!(job.exporter_config().interval(), Duration::from_secs(30));
        assert_eq!(job.exporter_config().read_batch_size(), 16);
        assert_eq!(job.exporter_config().write_batch_size(), 10_000);
        assert_eq!(job.coordinator_config().interval(), Duration::from_secs(30));
        assert_eq!(job.mapper_config().interval(), Duration::from_secs(30));
        assert_eq!(job.mapper_config().batch_size(), 100);
        assert_eq!(job.reducer_config().interval(), Duration::from_secs(30));
        assert_eq!(job.reducer_config().batch_size(), 1000);
        assert_eq!(job.janitor_config().interval(), Duration::from_secs(30));
    }

    #[test]
    fn invalid_batch_size_is_reported() {
        let _guard = ENV_LOCK.lock().unwrap_or_else(PoisonError::into_inner);
        let config = MapperConfig::default();
        std::env::set_var(MAPPER_BATCH_SIZE_VAR, "many");
        config.load(&mut EnvReader::new());
        std::env::remove_var(MAPPER_BATCH_SIZE_VAR);
        let err = config.validate().expect_err("invalid int");
        assert_eq!(
            err.to_string(),
            "invalid int \"many\" for CODEINTEL_RANKING_MAPPER_BATCH_SIZE: strconv.ParseInt: parsing \"many\": invalid syntax"
        );
    }

    #[test]
    fn gate_defaults_to_disabled() {
        assert!(!document_reference_counts_enabled(&site("{}")));
        assert!(!document_reference_counts_enabled(&site(
            r#"{"codeIntelRanking.documentReferenceCountsEnabled": "yes"}"#
        )));
        assert!(document_reference_counts_enabled(&site(
            r#"{"codeIntelRanking.documentReferenceCountsEnabled": true}"#
        )));
        assert!(!document_reference_counts_enabled(&site(
            r#"{"codeIntelRanking.documentReferenceCountsEnabled": false}"#
        )));
    }

    #[test]
    fn graph_key_defaults_to_dev_and_is_encoded() {
        assert_eq!(document_reference_counts_graph_key(&site("{}")), "dev");
        assert_eq!(
            document_reference_counts_graph_key(&site(
                r#"{"codeIntelRanking.documentReferenceCountsGraphKey": ""}"#
            )),
            "dev"
        );
        let configured =
            site(r#"{"codeIntelRanking.documentReferenceCountsGraphKey": "prod-2.eu"}"#);
        assert_eq!(
            document_reference_counts_graph_key(&configured),
            "prod-2.eu"
        );
        assert_eq!(root_graph_key(&configured), "prod_2_eu");
        assert_eq!(
            derivative_graph_key_for_prefix(&configured, "2026-11"),
            "prod_2_eu.2026_11"
        );
    }

    #[test]
    fn document_paths_join_like_filepath_join() {
        assert_eq!(join_document_path("", "a/b.go"), "a/b.go");
        assert_eq!(join_document_path("src", "a/b.go"), "src/a/b.go");
        assert_eq!(join_document_path("src/", "a/b.go"), "src/a/b.go");
        assert_eq!(join_document_path("/", "a.go"), "/a.go");
    }

    #[test]
    fn janitor_batch_sizes_match_the_baseline() {
        assert_eq!(VACUUM_UPLOADS_BATCH_SIZE, 100);
        assert_eq!(VACUUM_MISC_RECORDS_BATCH_SIZE, 10_000);
    }
}
