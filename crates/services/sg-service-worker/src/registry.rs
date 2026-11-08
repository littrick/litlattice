#![forbid(unsafe_code)]

//! The 56 background jobs registered by `cmd/worker/shared.LoadConfig`.
//!
//! The baseline builds a `map[string]workerjob.Job` literal (`builtins`) and
//! `jobNames` sorts its keys; this table is that sorted key set
//! (baseline commit `c864f15`). A job's presence here is what
//! `shared.Config.Validate` checks `WORKER_JOB_ALLOWLIST` /
//! `WORKER_JOB_BLOCKLIST` names against.
//!
//! The table only names the jobs; their framework (`Routines`, `Config`) is
//! ported job group by job group in later slices.

pub const JOB_NAMES: [&str; 56] = [
    "aggregated-users-statistics",
    "auth-sourcegraph-operator-cleaner",
    "batches-bulk-processor",
    "batches-janitor",
    "batches-reconciler",
    "batches-scheduler",
    "batches-workspace-resolver",
    "bitbucket-project-permissions",
    "codehost-version-syncing",
    "codeintel-autoindexing-dependency-scheduler",
    "codeintel-autoindexing-scheduler",
    "codeintel-autoindexing-summary-builder",
    "codeintel-commitgraph-updater",
    "codeintel-metrics-reporter",
    "codeintel-package-filter-applicator",
    "codeintel-policies-repository-matcher",
    "codeintel-ranking-file-reference-counter",
    "codeintel-syntactic-indexing-scheduler",
    "codeintel-upload-backfiller",
    "codeintel-upload-expirer",
    "codeintel-upload-janitor",
    "codeintel-uploadstore-expirer",
    "codemonitors-job",
    "cody-gateway-usage-check",
    "cody-llm-token-counter",
    "event-logs-janitor",
    "executors-janitor",
    "executors-metricsserver",
    "executors-multiqueue-metrics-reporter",
    "exhaustive-search-job",
    "export-usage-telemetry",
    "github-apps-installation-validation-job",
    "gitserver-metrics",
    "insights-data-retention-job",
    "insights-job",
    "insights-query-runner-job",
    "license-check",
    "out-of-band-migrations",
    "outbound-webhook-sender",
    "own-repo-indexing-queue",
    "perforce-changelist-mapper",
    "permission-sync-job-cleaner",
    "permission-sync-job-scheduler",
    "rate-limit-config",
    "record-encrypter",
    "refresh-analytics-cache",
    "repo-embedding-janitor",
    "repo-embedding-job",
    "repo-embedding-scheduler",
    "repo-perms-syncer",
    "repo-statistics-compactor",
    "repo-statistics-resetter",
    "sourcegraph-accounts-notifications-subscriber",
    "telemetrygateway-exporter",
    "webhook-log-janitor",
    "zoekt-repos-updater",
];

/// Sorted `builtins` keys (`jobNames(builtins)`).
pub fn job_names() -> Vec<String> {
    JOB_NAMES.iter().map(|name| (*name).to_owned()).collect()
}

pub fn contains(name: &str) -> bool {
    JOB_NAMES.contains(&name)
}

pub fn count() -> usize {
    JOB_NAMES.len()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registry_matches_the_baseline_builtins_map() {
        assert_eq!(count(), 56);
        let mut sorted = job_names();
        sorted.sort();
        assert_eq!(sorted, job_names(), "the table must be sorted");
        let unique: std::collections::BTreeSet<_> = job_names().into_iter().collect();
        assert_eq!(unique.len(), count(), "job names must be unique");
        assert!(contains("insights-job"));
        assert!(!contains("all"));
    }
}
