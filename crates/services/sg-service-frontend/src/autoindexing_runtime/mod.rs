#![forbid(unsafe_code)]

//! Autoindexing inference / job-selection / enqueue runtime, shared by the
//! frontend and worker crates (M9.44c).
//!
//! The implementation lives here rather than in `sg-service-worker` because
//! `sg-service-worker` depends on `sg-service-frontend` (and not vice versa),
//! so the production frontend cannot otherwise inject the runtime behind
//! [`crate::codeintel_autoindexing_graphql::CodeIntelAutoindexingJobsRuntime`].
//! The worker re-exports these modules under their previous paths in
//! `jobs/mod.rs`, so no worker call site changed.
//!
//! Baseline modules:
//!
//! * `internal/codeintel/autoindexing/internal/inference` — [`inference`].
//! * `internal/codeintel/autoindexing/internal/jobselector` — [`jobselector`].
//! * `internal/codeintel/autoindexing/internal/enqueuer` — [`enqueuer`].
//! * `lib/codeintel/autoindex/index/...` + `internal/paths` —
//!   [`autoindex_config`], [`path_patterns`], [`paths`].
//! * `inference/init.go` production adapters — [`inference_production`].
//!
//! [`runtime::ProductionAutoindexingJobsRuntime`] binds them into the GraphQL
//! runtime trait; no table, column or Redis key is added.

pub mod autoindex_config;
pub mod enqueuer;
pub mod inference;
pub mod inference_production;
pub mod jobselector;
pub mod package_inference;
pub mod path_patterns;
pub mod paths;
pub mod runtime;

pub use enqueuer::{
    IndexEnqueuer, PackageRepoInference, QueueAutoIndexJobsError, ResolveRevisionError,
    RevisionResolver, VersionedPackageRepo,
};
pub use inference::{
    ArchiveOptions, DirEntry, GitService, InferenceError, InferenceService, InferenceSettings,
    RateLimiter, SandboxService,
};
pub use inference_production::{
    GitserverGitService, GitserverRevisionResolver, GitserverYamlReader, InferenceAdapter,
    TokenBucketLimiter,
};
pub use jobselector::{
    FileReadError, InferenceResult, JobSelector, JobSelectorSettings, SourcegraphYamlReader,
};
pub use package_inference::{infer_repository_and_revision, DefaultPackageRepoInference};
pub use runtime::{ProductionAutoindexingJobsRuntime, RepositoryGit};
