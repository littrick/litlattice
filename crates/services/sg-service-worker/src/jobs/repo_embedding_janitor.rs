#![forbid(unsafe_code)]

//! `cmd/worker/internal/embeddings/repo/janitor.go`: the
//! `repo-embedding-janitor` job.
//!
//! The job owns no config and returns one `dbworker.Resetter` over the
//! `repo_embedding_jobs` queue:
//!
//! | field | value |
//! |---|---|
//! | `ResetterOptions.Name` | `repo_embedding_job_worker_resetter` |
//! | `ResetterOptions.Interval` | 1 minute |
//! | `StalledMaxAge` | 60s |
//! | `MaxNumResets` | 5 |
//! | `ResetFailureMessage` | the dbworker default |
//!
//! The baseline's `Name()` is `dbworker.Resetter[<options name>]`; the Rust
//! routine records the bare options name (docs §27.4 records the discrepancy
//! and the differential harness normalises it on both sides).
//!
//! Not ported: `dbworker.NewResetterMetrics(observationCtx,
//! "repo_embedding_job_worker")` (`src_repo_embedding_job_worker_record_
//! resets_total`, `..._record_reset_failures_total`, `..._record_reset_
//! errors_total`), matching the other resetter jobs.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use sg_db::repo_embeddings::{self, REPO_EMBEDDING_JOB_WORKER};
use sg_db::OffsetDateTime;
use sqlx::PgPool;
use tokio_util::sync::CancellationToken;

use super::{Job, JobDeps};
use crate::framework::dbworker::{DbBackedRoutine, DbResetter, ResetterOptions, ResetterRoutine};
use crate::framework::BackgroundRoutine;

/// `repoEmbeddingJanitorJob.Description`.
pub const JOB_DESCRIPTION: &str = "";

/// `dbworker.ResetterOptions.Name` in `newRepoEmbeddingJobResetter`.
pub const RESETTER_NAME: &str = "repo_embedding_job_worker_resetter";

/// `newRepoEmbeddingJobResetter`'s `Interval: time.Minute`.
pub const RESETTER_INTERVAL: Duration = Duration::from_secs(60);

/// `repoEmbeddingJanitorJob` (`NewRepoEmbeddingJanitorJob`).
pub struct RepoEmbeddingJanitor;

impl RepoEmbeddingJanitor {
    pub fn new() -> Self {
        Self
    }
}

impl Default for RepoEmbeddingJanitor {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Job for RepoEmbeddingJanitor {
    fn description(&self) -> String {
        JOB_DESCRIPTION.to_owned()
    }

    async fn routines(
        &self,
        deps: &JobDeps,
        _setup_ctx: CancellationToken,
    ) -> Result<Vec<BackgroundRoutine>, String> {
        let store = Arc::new(RepoEmbeddingJobResetStore {
            pool: deps.frontend.clone(),
        }) as Arc<dyn DbResetter>;

        Ok(vec![BackgroundRoutine::DbBacked(
            Arc::new(ResetterRoutine::new(
                store,
                ResetterOptions {
                    name: RESETTER_NAME.to_owned(),
                    interval: RESETTER_INTERVAL,
                },
            )) as Arc<dyn DbBackedRoutine>,
        )])
    }
}

/// `NewRepoEmbeddingJobWorkerStore` as the resetter sees it.
struct RepoEmbeddingJobResetStore {
    pool: PgPool,
}

#[async_trait]
impl DbResetter for RepoEmbeddingJobResetStore {
    async fn reset_stalled(&self) -> Result<(Vec<(i64, Duration)>, Vec<(i64, Duration)>), String> {
        repo_embeddings::reset_stalled(
            &self.pool,
            REPO_EMBEDDING_JOB_WORKER,
            OffsetDateTime::now_utc(),
        )
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
    async fn job_constructs_the_baseline_resetter() {
        let job = RepoEmbeddingJanitor::new();
        assert_eq!(job.description(), "");
        assert!(job.configs().is_empty());

        let routines = job
            .routines(&deps(), CancellationToken::new())
            .await
            .expect("routines");
        assert_eq!(routines.len(), 1);
        assert_eq!(routines[0].name(), "repo_embedding_job_worker_resetter");
        assert_eq!(routines[0].interval(), Duration::from_secs(60));
        // `dbworker.Resetter` is not a `recorder.Recordable`.
        assert_eq!(routines[0].description(), "");
        assert!(Arc::new(routines[0].clone()).recordable().is_none());
    }
}
