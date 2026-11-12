#![forbid(unsafe_code)]

//! Diagnostic `Site`/`Query` fields that this slice explicitly does **not**
//! port (M9.13b).
//!
//! The vendored `schema.graphql` exposes four diagnostic surfaces whose Go
//! resolvers depend on infrastructure that has no injectable seam in the Rust
//! frontend yet. Rather than fabricate data, this module records the deferral
//! so the slice's scope is explicit and searchable. Nothing here is wired into
//! the resolver tree, so responses are unchanged (the fields remain at their
//! pre-existing unported behaviour).
//!
//! | GraphQL field | Go baseline | Missing dependency |
//! |---|---|---|
//! | `Site.statusMessages` | `cmd/frontend/graphqlbackend/status_messages.go` | `repos.FetchStatusMessages` + gitserver client + external-service sync store |
//! | `Site.repositoryStats` | `cmd/frontend/graphqlbackend/repository_stats.go` | zoekt `search.ListAllIndexed`, gitserver git-dir sizes, `RepoStatistics` aggregation |
//! | `Site.backgroundJobs` | `cmd/frontend/graphqlbackend/background_jobs.go` | `db.BackgroundJobs()` + the routine type registry |
//! | `Site.gitservers` | `cmd/frontend/graphqlbackend/gitserver.go` | the gitserver client address list |

/// One deferred diagnostic field.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DeferredDiagnostic {
    pub field: &'static str,
    pub baseline: &'static str,
    pub reason: &'static str,
}

/// The deferral ledger for this slice.
pub const DEFERRED_DIAGNOSTICS: &[DeferredDiagnostic] = &[
    DeferredDiagnostic {
        field: "Site.statusMessages",
        baseline: "status_messages.go",
        reason: "requires repos.FetchStatusMessages plus gitserver/external-service sync stores",
    },
    DeferredDiagnostic {
        field: "Site.repositoryStats",
        baseline: "repository_stats.go",
        reason: "requires zoekt ListAllIndexed, gitserver git-dir sizes and the RepoStatistics aggregator",
    },
    DeferredDiagnostic {
        field: "Site.backgroundJobs",
        baseline: "background_jobs.go",
        reason: "requires db.BackgroundJobs() and the routine type registry",
    },
    DeferredDiagnostic {
        field: "Site.gitservers",
        baseline: "gitserver.go",
        reason: "requires the gitserver client address list",
    },
];
