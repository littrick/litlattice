#![forbid(unsafe_code)]

//! `internal/codeintel/autoindexing/internal/background/dependencies/
//! job_dependency_sync_scheduler.go` — the sync-queue half of the
//! `codeintel-autoindexing-dependency-scheduler` job.
//!
//! [`DependencySyncSchedulerHandler::handle`] ports the baseline handler:
//!
//! 1. a no-op when auto-indexing is disabled;
//! 2. scan the upload's package references, normalizing each one through
//!    [`new_package`] (JVM name normalization, npm validation and scheme
//!    rewrites) and skipping names the package manager no longer accepts;
//! 3. insert one `MinimalPackageRepoRef` per reference, with the package and
//!    version `blocked` flags computed from the stored package-repo filters;
//! 4. collect the external-service kinds implied by the schemes seen (the
//!    empty string is collected too, but excluded from the sync list) and,
//!    when any real kind was seen, list those services and set their
//!    `next_sync_at` to the same instant;
//! 5. when the upload's indexer is one of the dependency-indexing indexers,
//!    insert one `lsif_dependency_indexing_jobs` row per collected kind
//!    (including `""`), all sharing `next_sync_at` (the Go zero time when no
//!    real kind was seen);
//! 6. aggregate failures: a single error is returned as-is, several are
//!    combined. The baseline's deferred `scanner.Close` writes its error to a
//!    local `err` inside a function with unnamed returns, so a Close error is
//!    never propagated; the close error is therefore ignored here too.
//!
//! Unported dependencies are injected as traits: [`DependencySyncUploads`]
//! (the streaming scanner is flattened to a vector plus a close error),
//! [`DependencySyncDependencies`], [`DependencyIndexingJobStore`] and
//! [`DependencySyncExternalServices`].
//!
//! Representation notes:
//!
//! * `kinds` is a Go map in the baseline, so kind ordering, the inserted-job
//!   order and the aggregated error order are not deterministic; the
//!   differential harness sorts all three;
//! * `external_service_sync` is `None` for the Go zero time (used when no
//!   external-service kind was seen); production wiring binds the zero time
//!   as year 1, mirroring `time.Time{}`;
//! * `DependenciesService.InsertPackageRepoRefs` is reduced to the inserted
//!   counts, which is all the handler reads.

use std::collections::HashMap;

use async_trait::async_trait;
use sg_codeintel::packagefilters::PackageFilters;
use sg_db::codeintel_dependencies::PackageRepoFilter;
use sg_db::OffsetDateTime;

use super::codeintel_autoindexing_package_inference::{
    parse_npm_package_from_package_syntax, JVM_PACKAGES_SCHEME, NPM_PACKAGES_SCHEME,
    PYTHON_PACKAGES_SCHEME, RUBY_PACKAGES_SCHEME, RUST_PACKAGES_SCHEME,
};
use super::codeintel_package_filter_applicator::filter_specs;

/// `extsvc.KindJVMPackages`.
pub const KIND_JVMPACKAGES: &str = "JVMPACKAGES";
/// `extsvc.KindNpmPackages`.
pub const KIND_NPMPACKAGES: &str = "NPMPACKAGES";
/// `extsvc.KindPythonPackages`.
pub const KIND_PYTHONPACKAGES: &str = "PYTHONPACKAGES";
/// `extsvc.KindRustPackages`.
pub const KIND_RUSTPACKAGES: &str = "RUSTPACKAGES";
/// `extsvc.KindRubyPackages`.
pub const KIND_RUBYPACKAGES: &str = "RUBYPACKAGES";

/// `dependencySyncingJob` (subset of `lsif_dependency_syncing_jobs`).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DependencySyncingJob {
    pub id: i64,
    pub upload_id: i32,
}

/// `shared.Package` subset used by [`new_package`].
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Package {
    pub scheme: String,
    pub manager: String,
    pub name: String,
    pub version: String,
}

/// `precise.Package`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PrecisePackage {
    pub scheme: String,
    pub manager: String,
    pub name: String,
    pub version: String,
}

/// `shared.Upload` subset used by `shouldIndexDependencies`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Upload {
    pub id: i32,
    pub indexer: String,
}

/// `shared.PackageReferenceScanner` flattened: the references the scan
/// produced before either finishing or failing, plus the error `Close` would
/// return. The close error is recorded for completeness but never propagated
/// — see the module docs.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PackageReferences {
    pub references: Vec<Package>,
    /// `scanner.Next` failed after the references above were produced.
    pub next_error: Option<String>,
    pub close_error: Option<String>,
}

/// `dependencies.MinimalPackageRepoRef` / `MinimalPackageRepoRefVersion` are
/// shared with the `sg-db` implementation added in M9.8di.
pub use sg_db::codeintel_dependencies::{MinimalPackageRepoRef, MinimalPackageRepoRefVersion};

/// The part of `InsertPackageRepoRefs`' result the handler reads: the lengths
/// of the returned new-reference and new-version slices.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct InsertedPackageRepoRefs {
    pub new_repo_refs: usize,
    pub new_versions: usize,
}

/// `types.ExternalService` subset.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ExternalService {
    pub id: i64,
    pub display_name: String,
}

/// `UploadService`.
#[async_trait]
pub trait DependencySyncUploads: Send + Sync + 'static {
    async fn references_for_upload(&self, upload_id: i32) -> Result<PackageReferences, String>;

    /// `GetUploadByID`; `None` models the baseline's `exists == false`
    /// (the zero upload has an empty indexer).
    async fn get_upload_by_id(&self, upload_id: i32) -> Result<Option<Upload>, String>;
}

/// `DependenciesService`.
#[async_trait]
pub trait DependencySyncDependencies: Send + Sync + 'static {
    async fn list_package_repo_filters(&self) -> Result<Vec<PackageRepoFilter>, String>;

    async fn insert_package_repo_refs(
        &self,
        refs: &[MinimalPackageRepoRef],
    ) -> Result<InsertedPackageRepoRefs, String>;
}

/// `store.Store.InsertDependencyIndexingJob`; `None` is the Go zero
/// `time.Time`.
#[async_trait]
pub trait DependencyIndexingJobStore: Send + Sync + 'static {
    async fn insert_dependency_indexing_job(
        &self,
        upload_id: i32,
        external_service_kind: &str,
        sync_time: Option<OffsetDateTime>,
    ) -> Result<i32, String>;
}

/// `ExternalServiceStore`.
#[async_trait]
pub trait DependencySyncExternalServices: Send + Sync + 'static {
    async fn list(&self, kinds: &[String]) -> Result<Vec<ExternalService>, String>;

    /// `Upsert` reduced to the only field the handler changes. Called once per
    /// service, like the baseline loop, so failures are attributed to a
    /// service.
    async fn upsert_next_sync_at(
        &self,
        service: &ExternalService,
        next_sync_at: OffsetDateTime,
    ) -> Result<(), String>;
}

/// Behaviour switches normally read from site config.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HandlerSettings {
    /// `conf.CodeIntelAutoIndexingEnabled`.
    pub auto_indexing_enabled: bool,
}

impl Default for HandlerSettings {
    fn default() -> Self {
        Self {
            auto_indexing_enabled: true,
        }
    }
}

/// The handler's error: a single failure is returned as-is, several are
/// combined (`errors.Append(nil, errs...)`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SyncSchedulerError {
    Message(String),
    Multiple(Vec<String>),
}

impl std::fmt::Display for SyncSchedulerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SyncSchedulerError::Message(message) => f.write_str(message),
            // `errors.Append(nil, errs...)`: `N errors occurred:` plus one
            // `\n\t* ` entry per error (the worker stores this text in
            // `failure_message`).
            SyncSchedulerError::Multiple(errors) => {
                f.write_str(&crate::config::combine_errors(errors).unwrap_or_default())
            }
        }
    }
}

impl std::error::Error for SyncSchedulerError {}

/// `dependencySyncSchedulerHandler`.
pub struct DependencySyncSchedulerHandler {
    pub uploads: std::sync::Arc<dyn DependencySyncUploads>,
    pub dependencies: std::sync::Arc<dyn DependencySyncDependencies>,
    pub store: std::sync::Arc<dyn DependencyIndexingJobStore>,
    pub external_services: std::sync::Arc<dyn DependencySyncExternalServices>,
    pub settings: HandlerSettings,
}

impl DependencySyncSchedulerHandler {
    /// `Handle`.
    pub async fn handle(&self, job: &DependencySyncingJob) -> Result<(), SyncSchedulerError> {
        if !self.settings.auto_indexing_enabled {
            return Ok(());
        }

        let stream = self
            .uploads
            .references_for_upload(job.upload_id)
            .await
            .map_err(|error| {
                SyncSchedulerError::Message(format!("dbstore.ReferencesForUpload: {error}"))
            })?;

        // The baseline registers `defer scanner.Close()` and appends its error
        // to the local `err` variable, but `Handle` has unnamed return values,
        // so that write is discarded and a Close error never reaches the
        // caller. Reproduce the observable behaviour: ignore the close error.
        let _ = stream.close_error;

        self.handle_stream(job, stream.references, stream.next_error)
            .await
    }

    async fn handle_stream(
        &self,
        job: &DependencySyncingJob,
        references: Vec<Package>,
        next_error: Option<String>,
    ) -> Result<(), SyncSchedulerError> {
        let instant = OffsetDateTime::now_utc();

        let mut kinds: HashMap<String, ()> = HashMap::new();
        let mut errors: Vec<String> = Vec::new();

        let filters = self
            .dependencies
            .list_package_repo_filters()
            .await
            .map_err(|error| {
                SyncSchedulerError::Message(format!("error listing package repo filters: {error}"))
            })?;

        let package_filters =
            PackageFilters::new(&filter_specs(&filters)).map_err(SyncSchedulerError::Message)?;

        for reference in references {
            let package = match new_package(&reference) {
                Ok(package) => package,
                Err(_) => continue,
            };

            let external_service_kind = scheme_to_external_service(&package.scheme);
            // Unknown schemes contribute the empty kind: it never triggers an
            // external-service sync, but it still yields a dependency indexing
            // job (the baseline comment calls out lsif-go).
            kinds.insert(external_service_kind.unwrap_or_default().to_owned(), ());

            let Some(_kind) = external_service_kind else {
                continue;
            };

            if let Err(error) = self
                .insert_package_repo_ref(&package, &package_filters, instant)
                .await
            {
                errors.push(error);
            }
        }

        // The scanner failed while producing the next reference: the
        // references before it were already processed, then `Handle` returns
        // immediately (no external-service sync, no dependency-indexing jobs).
        if let Some(error) = next_error {
            return Err(SyncSchedulerError::Message(format!(
                "dbstore.ReferencesForUpload.Next: {error}"
            )));
        }

        let mut next_sync: Option<OffsetDateTime> = None;

        // `kindsToArray` drops the empty string. The baseline iterates a map;
        // sorting keeps the outbound call deterministic.
        let mut kinds_array: Vec<String> = kinds
            .keys()
            .filter(|kind| !kind.is_empty())
            .cloned()
            .collect();
        kinds_array.sort();

        // If empty, `List` would return every external service, which the
        // baseline explicitly avoids.
        if !kinds_array.is_empty() {
            next_sync = Some(OffsetDateTime::now_utc());

            match self.external_services.list(&kinds_array).await {
                Ok(services) => {
                    for service in services {
                        if let Err(error) = self
                            .external_services
                            .upsert_next_sync_at(&service, next_sync.expect("set above"))
                            .await
                        {
                            errors.push(format!(
                                "extsvcStore.Upsert: error setting next_sync_at for external service {} - {}: {error}",
                                service.id, service.display_name
                            ));
                        }
                    }
                }
                Err(error) => {
                    if errors.is_empty() {
                        return Err(SyncSchedulerError::Message(format!(
                            "dbstore.List: {error}"
                        )));
                    }
                    // The baseline appends the *raw* error here (no
                    // `dbstore.List` prefix), unlike the single-error path.
                    let mut combined = Vec::with_capacity(errors.len() + 1);
                    combined.push(error);
                    combined.extend(errors);
                    return Err(SyncSchedulerError::Multiple(combined));
                }
            }
        }

        let should_index = self.should_index_dependencies(job.upload_id).await?;

        if should_index {
            // Go map iteration order.
            for kind in kinds.into_keys() {
                if let Err(error) = self
                    .store
                    .insert_dependency_indexing_job(job.upload_id, &kind, next_sync)
                    .await
                {
                    errors.push(format!("dbstore.InsertDependencyIndexingJob: {error}"));
                }
            }
        }

        match errors.len() {
            0 => Ok(()),
            1 => Err(SyncSchedulerError::Message(errors.remove(0))),
            _ => Err(SyncSchedulerError::Multiple(errors)),
        }
    }

    /// `insertPackageRepoRef`.
    async fn insert_package_repo_ref(
        &self,
        package: &PrecisePackage,
        filters: &PackageFilters,
        instant: OffsetDateTime,
    ) -> Result<(bool, bool), String> {
        let reference = MinimalPackageRepoRef {
            name: package.name.clone(),
            scheme: package.scheme.clone(),
            blocked: !filters.is_package_allowed(&package.scheme, &package.name),
            last_checked_at: Some(instant),
            versions: vec![MinimalPackageRepoRefVersion {
                version: package.version.clone(),
                blocked: !filters.is_versioned_package_allowed(
                    &package.scheme,
                    &package.name,
                    &package.version,
                ),
                last_checked_at: Some(instant),
            }],
        };

        let inserted = self
            .dependencies
            .insert_package_repo_refs(std::slice::from_ref(&reference))
            .await
            .map_err(|error| format!("dbstore.InsertCloneableDependencyRepos: {error}"))?;

        Ok((inserted.new_repo_refs != 0, inserted.new_versions != 0))
    }

    /// `shouldIndexDependencies`.
    async fn should_index_dependencies(&self, upload_id: i32) -> Result<bool, SyncSchedulerError> {
        let upload = self
            .uploads
            .get_upload_by_id(upload_id)
            .await
            .map_err(|error| {
                SyncSchedulerError::Message(format!("dbstore.GetUploadByID: {error}"))
            })?;

        let indexer = upload.map(|upload| upload.indexer).unwrap_or_default();
        Ok(is_dependency_indexer(&indexer))
    }
}

/// `schemeToExternalService`: the external-service kind a package scheme
/// belongs to.
pub fn scheme_to_external_service(scheme: &str) -> Option<&'static str> {
    match scheme {
        JVM_PACKAGES_SCHEME => Some(KIND_JVMPACKAGES),
        NPM_PACKAGES_SCHEME => Some(KIND_NPMPACKAGES),
        PYTHON_PACKAGES_SCHEME => Some(KIND_PYTHONPACKAGES),
        RUST_PACKAGES_SCHEME => Some(KIND_RUSTPACKAGES),
        RUBY_PACKAGES_SCHEME => Some(KIND_RUBYPACKAGES),
        _ => None,
    }
}

/// `shouldIndexDependencies`' indexer allowlist.
pub fn is_dependency_indexer(indexer: &str) -> bool {
    matches!(
        indexer,
        "lsif-go"
            | "scip-java"
            | "lsif-java"
            | "lsif-tsc"
            | "scip-typescript"
            | "lsif-typescript"
            | "scip-python"
            | "scip-ruby"
            | "rust-analyzer"
    )
}

/// `newPackage`: normalize an upload package into the precise package stored
/// in the dependency tables.
pub fn new_package(package: &Package) -> Result<PrecisePackage, String> {
    let mut scheme = package.scheme.clone();
    let mut name = package.name.clone();

    match package.scheme.as_str() {
        JVM_PACKAGES_SCHEME => {
            // `strings.TrimPrefix(p.Name, "maven/")`, then every `/` becomes
            // `:` (Maven coordinates are `group:artifact`).
            let stripped = name
                .strip_prefix("maven/")
                .unwrap_or(name.as_str())
                .to_owned();
            name = stripped.replace('/', ":");
        }
        NPM_PACKAGES_SCHEME | "scip-typescript" => {
            if parse_npm_package_from_package_syntax(&package.name).is_none() {
                return Err(format!("invalid npm package name: {}", package.name));
            }
            scheme = NPM_PACKAGES_SCHEME.to_owned();
        }
        "scip-python" => {
            scheme = PYTHON_PACKAGES_SCHEME.to_owned();
        }
        _ => {}
    }

    Ok(PrecisePackage {
        scheme,
        manager: package.manager.clone(),
        name,
        version: package.version.clone(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn package(scheme: &str, name: &str, version: &str) -> Package {
        Package {
            scheme: scheme.to_owned(),
            manager: String::new(),
            name: name.to_owned(),
            version: version.to_owned(),
        }
    }

    #[test]
    fn jvm_names_drop_the_maven_prefix_and_colonize_slashes() {
        let result = new_package(&package(JVM_PACKAGES_SCHEME, "maven/junit/junit", "4.2"))
            .expect("valid jvm package");
        assert_eq!(result.name, "junit:junit");
        assert_eq!(result.scheme, JVM_PACKAGES_SCHEME);
    }

    #[test]
    fn jvm_names_without_prefix_are_unchanged_except_slashes() {
        let result = new_package(&package(JVM_PACKAGES_SCHEME, "junit:junit", "4.2"))
            .expect("valid jvm package");
        assert_eq!(result.name, "junit:junit");
    }

    #[test]
    fn npm_names_are_validated_but_not_rewritten() {
        let result = new_package(&package(
            NPM_PACKAGES_SCHEME,
            "@graphql-mesh/graphql",
            "0.24.0",
        ))
        .expect("valid npm package");
        assert_eq!(result.name, "@graphql-mesh/graphql");
        assert_eq!(result.scheme, NPM_PACKAGES_SCHEME);
    }

    #[test]
    fn invalid_npm_names_are_rejected() {
        assert!(new_package(&package(
            NPM_PACKAGES_SCHEME,
            "@automapper/classes/transformer-plugin",
            "0.24.0"
        ))
        .is_err());
    }

    #[test]
    fn scip_typescript_is_rewritten_to_npm() {
        let result =
            new_package(&package("scip-typescript", "left-pad", "1.0.0")).expect("valid package");
        assert_eq!(result.scheme, NPM_PACKAGES_SCHEME);
    }

    #[test]
    fn scip_python_is_rewritten_to_python() {
        let result =
            new_package(&package("scip-python", "requests", "2.0.0")).expect("valid package");
        assert_eq!(result.scheme, PYTHON_PACKAGES_SCHEME);
    }

    #[test]
    fn gomod_packages_pass_through() {
        let result = new_package(&package("gomod", "github.com/tsenart/vegeta/v12", "12.7.0"))
            .expect("valid package");
        assert_eq!(result.scheme, "gomod");
        assert_eq!(result.name, "github.com/tsenart/vegeta/v12");
    }

    #[test]
    fn external_service_kinds_match_the_baseline_map() {
        assert_eq!(
            scheme_to_external_service(JVM_PACKAGES_SCHEME),
            Some(KIND_JVMPACKAGES)
        );
        assert_eq!(
            scheme_to_external_service(NPM_PACKAGES_SCHEME),
            Some(KIND_NPMPACKAGES)
        );
        assert_eq!(
            scheme_to_external_service(PYTHON_PACKAGES_SCHEME),
            Some(KIND_PYTHONPACKAGES)
        );
        assert_eq!(
            scheme_to_external_service(RUST_PACKAGES_SCHEME),
            Some(KIND_RUSTPACKAGES)
        );
        assert_eq!(
            scheme_to_external_service(RUBY_PACKAGES_SCHEME),
            Some(KIND_RUBYPACKAGES)
        );
        assert_eq!(scheme_to_external_service("gomod"), None);
    }

    #[test]
    fn dependency_indexer_allowlist() {
        for indexer in [
            "lsif-go",
            "scip-java",
            "lsif-java",
            "lsif-tsc",
            "scip-typescript",
            "lsif-typescript",
            "scip-python",
            "scip-ruby",
            "rust-analyzer",
        ] {
            assert!(is_dependency_indexer(indexer), "{indexer}");
        }
        assert!(!is_dependency_indexer("lsif-node"));
        assert!(!is_dependency_indexer(""));
    }
}
