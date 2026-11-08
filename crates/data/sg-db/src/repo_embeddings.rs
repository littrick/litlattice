#![forbid(unsafe_code)]

//! `internal/embeddings/background/repo`'s `repo_embedding_jobs` worker store
//! options, plus the `ResetStalled` operation they drive.
//!
//! `NewRepoEmbeddingJobWorkerStore` (`internal/embeddings/background/repo/
//! store.go`) builds a `dbworkerstore.Store` with
//!
//! | option | value |
//! |---|---|
//! | `TableName` | `repo_embedding_jobs` |
//! | `OrderByExpression` | `repo_embedding_jobs.queued_at, repo_embedding_jobs.id` |
//! | `StalledMaxAge` | 60s |
//! | `MaxNumResets` | 5 |
//! | `MaxNumRetries` | 1 |
//!
//! and no `AlternateColumnNames`, so `ResetStalled` uses the default `id`,
//! `state`, `num_resets`, `queued_at`, `started_at`, `finished_at`,
//! `failure_message` and `last_heartbeat_at` columns. The reset statements are
//! the generic `internal/workerutil/dbworker/store` queries; the shared
//! implementation lives in [`crate::batches_janitor`] and is re-exported here so
//! embedding jobs do not have to reach into the batches module.
//!
//! Migrations stay owned by the Go `migrator`; this module only resets queue
//! state.

use std::time::Duration;

pub use crate::batches_janitor::{reset_stalled, ResetWorkerOptions};

/// `NewRepoEmbeddingJobWorkerStore`'s `dbworkerstore.Options` (the subset
/// `ResetStalled` reads).
pub const REPO_EMBEDDING_JOB_WORKER: ResetWorkerOptions = ResetWorkerOptions {
    table: "repo_embedding_jobs",
    state_column: "state",
    stalled_max_age: Duration::from_secs(60),
    max_num_resets: 5,
};
