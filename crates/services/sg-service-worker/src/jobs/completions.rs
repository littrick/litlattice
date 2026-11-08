#![forbid(unsafe_code)]

//! `cmd/worker/internal/completions`: the `cody-llm-token-counter` job.
//!
//! Every five minutes the worker reads the per-model LLM token counters from
//! the shared Redis cache (`tokenusage.NewManager()` over
//! `rcache.New(redispool.Store, "LLMUsage")`) and records a single Telemetry
//! V2 event, `cody.llmTokenCounter.record`, carrying them as metadata. The
//! event is stored by the minimal recorder in [`crate::telemetry`]: both the
//! `telemetry_events_export_queue` copy and the legacy `event_logs` row.
//!
//! Keys are `v2:LLMUsage:<provider>:<model>:<feature>:<input|output>`; the
//! metadata key is everything after `LLMUsage:`. Keys whose value is not an
//! integer are skipped (`GetInt64`'s parse failure), and the fetch never
//! decrements: `FetchTokenUsageDataForAnalysis` is the non-destructive
//! variant.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use sg_config::LiveConfig;
use sg_redis::KeyValue;
use sqlx::PgPool;
use tokio::sync::OnceCell;
use tokio_util::sync::CancellationToken;

use super::{Job, JobDeps};
use crate::framework::periodic::{Handler, HandlerError, PeriodicRoutine};
use crate::framework::BackgroundRoutine;
use crate::telemetry::{EventParameters, EventRecorder};

/// `tokenUsageJob.Description`.
pub const JOB_DESCRIPTION: &str = "stores LLM token usage in DB";
/// `goroutine.WithName`.
pub const ROUTINE_NAME: &str = "cody_llm_token_counter";
/// `goroutine.WithDescription`.
pub const ROUTINE_DESCRIPTION: &str = "Stores LLM token usage in DB";
/// `goroutine.WithInterval(5*time.Minute)`.
pub const INTERVAL: Duration = Duration::from_secs(5 * 60);
/// `rcache.New(redispool.Store, "LLMUsage")`.
pub const CACHE_PREFIX: &str = "LLMUsage";
/// The feature/action passed to `recorder.Record`.
pub const FEATURE: &str = "cody.llmTokenCounter";
pub const ACTION: &str = "record";
/// `convertedTokenUsageData["FinalFetchAndSync"] = 0.0`.
pub const FINAL_FETCH_AND_SYNC: &str = "FinalFetchAndSync";

/// `tokenUsageJob`.
pub struct CodyLlmTokenCounter;

#[async_trait]
impl Job for CodyLlmTokenCounter {
    fn description(&self) -> String {
        JOB_DESCRIPTION.to_owned()
    }

    async fn routines(
        &self,
        deps: &JobDeps,
        _setup_ctx: CancellationToken,
    ) -> Result<Vec<BackgroundRoutine>, String> {
        Ok(vec![PeriodicRoutine::new(Arc::new(TokenUsageHandler::new(
            deps.frontend.clone(),
            Arc::clone(&deps.live_config),
        )) as Arc<dyn Handler>)
        .with_name(ROUTINE_NAME)
        .with_description(ROUTINE_DESCRIPTION)
        .with_interval(INTERVAL)
        .into()])
    }
}

/// `tokenusage.Manager` over the `LLMUsage` rcache namespace.
pub struct TokenUsageManager {
    cache: sg_rcache::Cache,
}

impl TokenUsageManager {
    /// `tokenusage.NewManagerWithCache`.
    pub fn new(kv: KeyValue) -> Self {
        Self {
            cache: sg_rcache::Cache::new(kv, CACHE_PREFIX),
        }
    }

    /// `FetchTokenUsageDataForAnalysis`: every counter key, mapped to its
    /// model name (the part after `LLMUsage:`) and value. Unparseable or
    /// unreadable keys are skipped, and a failed `KEYS` yields an empty map —
    /// both like the baseline, which logs and moves on.
    pub async fn fetch_token_usage_data_for_analysis(&self) -> BTreeMap<String, f64> {
        let mut data = BTreeMap::new();
        let Ok(keys) = self.cache.list_all_keys().await else {
            return data;
        };

        for key in keys {
            // `strings.SplitN(key, "LLMUsage:", 2)`; a key without the
            // namespace cannot match the `v2:LLMUsage:*` pattern anyway.
            let Some((_, model_name)) = key.split_once("LLMUsage:") else {
                continue;
            };

            // `GetInt64`: missing keys and parse failures are skipped.
            let Ok(Some(value)) = self.cache.get_string(model_name).await else {
                continue;
            };
            let Ok(value) = value.parse::<i64>() else {
                continue;
            };

            data.insert(model_name.to_owned(), value as f64);
        }

        data
    }
}

/// `cmd/worker/internal/completions/job.go`'s handler closure.
pub struct TokenUsageHandler {
    pool: PgPool,
    live_config: Arc<LiveConfig>,
    store: LazyStore,
}

impl TokenUsageHandler {
    pub fn new(pool: PgPool, live_config: Arc<LiveConfig>) -> Self {
        Self {
            pool,
            live_config,
            store: LazyStore::new(sg_redis::store_endpoint()),
        }
    }

    /// The harness seam: script the token counters instead of reading Redis.
    pub fn with_cache(pool: PgPool, live_config: Arc<LiveConfig>, kv: KeyValue) -> Self {
        Self {
            pool,
            live_config,
            store: LazyStore::ready(kv),
        }
    }
}

#[async_trait]
impl Handler for TokenUsageHandler {
    async fn handle(&self, _ctx: CancellationToken) -> Result<(), HandlerError> {
        let kv = self
            .store
            .get()
            .await
            .map_err(|error| HandlerError::new(format!("connect to redis store: {error}")))?;
        let manager = TokenUsageManager::new(kv.clone());
        let token_usage = manager.fetch_token_usage_data_for_analysis().await;

        // `telemetry.SafeMetadataKey` is the identity function, so the model
        // keys are recorded verbatim.
        let mut metadata = token_usage;
        // This extra variable helps demarcate that this was NOT the final
        // fetch and sync before the redis was reset.
        metadata.insert(FINAL_FETCH_AND_SYNC.to_owned(), 0.0);

        EventRecorder::new(self.pool.clone(), Arc::clone(&self.live_config))
            .record(
                FEATURE,
                ACTION,
                &EventParameters {
                    version: 0,
                    metadata,
                },
            )
            .await
            .map_err(HandlerError::new)
    }
}

/// `redispool.Store` is a package-level lazily-initialised pool in the
/// baseline; `KeyValue::connect` dials eagerly, so the connection is deferred
/// to the first handler run.
struct LazyStore {
    endpoint: String,
    cell: OnceCell<KeyValue>,
}

impl LazyStore {
    fn new(endpoint: String) -> Self {
        Self {
            endpoint,
            cell: OnceCell::new(),
        }
    }

    fn ready(kv: KeyValue) -> Self {
        let cell = OnceCell::new();
        let _ = cell.set(kv);
        Self {
            endpoint: String::new(),
            cell,
        }
    }

    async fn get(&self) -> Result<&KeyValue, String> {
        self.cell
            .get_or_try_init(|| async {
                KeyValue::connect(&self.endpoint)
                    .await
                    .map_err(|error| error.to_string())
            })
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
    async fn job_constructs_the_baseline_routine() {
        let job = CodyLlmTokenCounter;
        assert_eq!(job.description(), JOB_DESCRIPTION);
        assert!(job.configs().is_empty());

        let routines = job
            .routines(&deps(), CancellationToken::new())
            .await
            .expect("routines");
        assert_eq!(routines.len(), 1);
        assert_eq!(routines[0].name(), ROUTINE_NAME);
        assert_eq!(routines[0].description(), ROUTINE_DESCRIPTION);
        assert_eq!(routines[0].interval(), Duration::from_secs(300));
    }

    #[test]
    fn cache_prefix_matches_the_baseline() {
        assert_eq!(CACHE_PREFIX, "LLMUsage");
        assert_eq!(
            sg_rcache::cache_redis_key(CACHE_PREFIX, "openai:gpt-4:chat:input"),
            "v2:LLMUsage:openai:gpt-4:chat:input"
        );
    }

    #[test]
    fn metadata_keys_are_the_part_after_the_namespace() {
        let key = "v2:LLMUsage:openaicompatible:model:feature:input";
        assert_eq!(
            key.split_once("LLMUsage:").map(|(_, rest)| rest),
            Some("openaicompatible:model:feature:input")
        );
    }
}
