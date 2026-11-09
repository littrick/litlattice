#![forbid(unsafe_code)]

//! `cmd/worker/internal/codeintel/dependencies_packages.go` +
//! `internal/codeintel/dependencies/internal/background/job_packages_filter.go`:
//! the `codeintel-package-filter-applicator` job.
//!
//! The job has no configuration and returns the single routine built by
//! `dependencies.PackageFiltersJob`:
//!
//! | routine | interval | work |
//! |---|---|---|
//! | `codeintel.package-filter-applicator` | 30s (fixed) | recompute the `blocked` status of every package repo reference from the stored package repo filters |
//!
//! Each invocation (`packagesFilterApplicatorJob.handle`):
//!
//! 1. `ShouldRefilterPackageRepoRefs` — skip the whole run when the newest
//!    filter is older than every `last_checked_at`;
//! 2. `ListPackageRepoRefFilters` (live filters, ordered by id) and
//!    `packagefilters.NewFilterLists`;
//! 3. page through `ListPackageRepoRefs` (`After`, `Limit=1000`,
//!    `IncludeBlocked=true`) and recompute each package's and version's
//!    `Blocked` with `IsPackageAllowed` / `IsVersionedPackageAllowed`;
//! 4. `UpdateAllBlockedStatuses` per page, counting the rows that changed;
//! 5. mark every package repository external service
//!    (`GOMODULES`, `JVMPACKAGES`, `NPMPACKAGES`, `PYTHONPACKAGES`,
//!    `RUBYPACKAGES`, `RUSTPACKAGES`) with `next_sync_at = now` so the
//!    syncer picks the (un)blocked references up.
//!
//! `actor.WithInternalActor` has no Rust equivalent and the SQL has no
//! authorization condition, mirroring the other ported code-intel jobs. The
//! `operations` counters/telemetry are part of the metrics slice.
//!
//! Deviations from the baseline's external-services `Upsert` are documented
//! in `docs/modules/06_worker.md` §32.4.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use sg_codeintel::packagefilters::{FilterSpec, PackageFilters, VersionFilterSpec};
use sg_db::codeintel_dependencies::{self, PackageRepoFilter, PackageRepoReference};
use sg_db::external_services::ExternalServiceStore;
use sqlx::PgPool;
use tokio_util::sync::CancellationToken;

use super::{Job, JobDeps};
use crate::framework::periodic::{Handler, HandlerError, PeriodicRoutine};
use crate::framework::BackgroundRoutine;

/// `packageFilterApplicatorJob.Description`.
pub const JOB_DESCRIPTION: &str = "package repo filters applicator";

/// `NewPackagesFilterApplicator`'s routine metadata.
pub const ROUTINE_NAME: &str = "codeintel.package-filter-applicator";
pub const ROUTINE_DESCRIPTION: &str = "applies package repo filters to all package repo references to precompute their blocked status";
/// `goroutine.WithInterval(time.Second * 30)`.
pub const ROUTINE_INTERVAL: Duration = Duration::from_secs(30);

/// The `Limit` in the applicator's `ListPackageRepoRefs` call.
pub const PAGE_SIZE: i64 = 1000;

/// `extsvc.KindJVMPackages` … `extsvc.KindPythonPackages`, in the baseline's
/// `List` argument order.
pub const PACKAGE_KINDS: [&str; 6] = [
    "JVMPACKAGES",
    "NPMPACKAGES",
    "GOMODULES",
    "RUSTPACKAGES",
    "RUBYPACKAGES",
    "PYTHONPACKAGES",
];

/// `cmd/worker/internal/codeintel.packageFilterApplicatorJob`.
#[derive(Debug, Default)]
pub struct PackageFilterApplicator;

impl PackageFilterApplicator {
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl Job for PackageFilterApplicator {
    fn description(&self) -> String {
        JOB_DESCRIPTION.to_owned()
    }

    /// `packageFilterApplicatorJob.Config` returns `nil`.
    fn configs(&self) -> Vec<Arc<dyn super::JobConfig>> {
        Vec::new()
    }

    async fn routines(
        &self,
        deps: &JobDeps,
        _setup_ctx: CancellationToken,
    ) -> Result<Vec<BackgroundRoutine>, String> {
        let routine = PeriodicRoutine::new(Arc::new(ApplicatorHandler {
            frontend: deps.frontend.clone(),
        }))
        .with_name(ROUTINE_NAME)
        .with_description(ROUTINE_DESCRIPTION)
        .with_interval(ROUTINE_INTERVAL);

        Ok(vec![routine.into()])
    }
}

struct ApplicatorHandler {
    frontend: PgPool,
}

#[async_trait]
impl Handler for ApplicatorHandler {
    async fn handle(&self, _ctx: CancellationToken) -> Result<(), HandlerError> {
        apply_package_filters(&self.frontend)
            .await
            .map(|_outcome| ())
            .map_err(HandlerError::new)
    }
}

/// `packagesFilterApplicatorJob.handle`: one full pass, see the module docs.
pub async fn apply_package_filters(pool: &PgPool) -> Result<ApplyOutcome, String> {
    let needs_filtering = codeintel_dependencies::should_refilter_package_repo_refs(pool)
        .await
        .map_err(|err| {
            format!("failed to check whether package repo filters need applying to anything: {err}")
        })?;
    if !needs_filtering {
        return Ok(ApplyOutcome::default());
    }

    let filters = codeintel_dependencies::list_package_repo_filters(pool)
        .await
        .map_err(|err| format!("failed to list package repo filters: {err}"))?;
    let package_filters = PackageFilters::new(&filter_specs(&filters))?;

    let start_time = time::OffsetDateTime::now_utc();
    let mut outcome = ApplyOutcome::default();

    // `for lastID := 0; ; { ... }`: page by ascending id until a page is empty.
    let mut last_id = 0i64;
    loop {
        let mut packages = codeintel_dependencies::list_package_repo_refs(pool, last_id, PAGE_SIZE)
            .await
            .map_err(|err| format!("failed to list package repos: {err}"))?;
        if packages.is_empty() {
            break;
        }

        last_id = packages.last().expect("non-empty package page").id;

        apply_filters_to_page(&mut packages, &package_filters);

        let (packages_updated, versions_updated) =
            codeintel_dependencies::update_all_blocked_statuses(pool, &packages, start_time)
                .await
                .map_err(|err| format!("failed to update blocked statuses: {err}"))?;
        outcome.packages_updated += packages_updated;
        outcome.versions_updated += versions_updated;
        outcome.pages += 1;
    }

    // Mark every package repository external service for an immediate sync.
    let next_sync_at = time::OffsetDateTime::now_utc();
    let store = ExternalServiceStore::new(pool.clone());
    let services = store
        .list_by_kinds(&PACKAGE_KINDS)
        .await
        .map_err(|err| format!("failed to list package repo external services: {err}"))?;
    store
        .mark_next_sync_at(&services, next_sync_at)
        .await
        .map_err(|err| {
            format!("failed to update next_sync_at for package repo external services: {err}")
        })?;
    outcome.external_services_updated = services.len();

    Ok(outcome)
}

/// What one `apply_package_filters` pass did, for tests and harnesses.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ApplyOutcome {
    /// `pkgsUpdated` accumulated over all pages.
    pub packages_updated: i64,
    /// `versionsUpdated` accumulated over all pages.
    pub versions_updated: i64,
    pub pages: usize,
    /// Package repository external services whose `next_sync_at` was bumped.
    pub external_services_updated: usize,
}

/// `for i, pkg := range pkgs { pkg.Blocked = !IsPackageAllowed(...); ... }`.
fn apply_filters_to_page(packages: &mut [PackageRepoReference], filters: &PackageFilters) {
    for package in packages.iter_mut() {
        package.blocked = !filters.is_package_allowed(&package.scheme, &package.name);
        for version in package.versions.iter_mut() {
            version.blocked = !filters.is_versioned_package_allowed(
                &package.scheme,
                &package.name,
                &version.version,
            );
        }
    }
}

/// Map the stored rows onto the matcher crate's neutral input type.
pub fn filter_specs(filters: &[PackageRepoFilter]) -> Vec<FilterSpec> {
    filters
        .iter()
        .map(|filter| FilterSpec {
            behaviour: filter.behaviour.clone(),
            package_scheme: filter.package_scheme.clone(),
            name_glob: filter
                .name_filter
                .as_ref()
                .map(|name| name.package_glob.clone()),
            version: filter
                .version_filter
                .as_ref()
                .map(|version| VersionFilterSpec {
                    package_name: version.package_name.clone(),
                    version_glob: version.version_glob.clone(),
                }),
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use sg_db::codeintel_dependencies::{
        PackageNameFilter, PackageRepoRefVersion, PackageVersionFilter,
    };

    #[test]
    fn job_metadata_matches_the_baseline() {
        let job = PackageFilterApplicator::new();
        assert_eq!(job.description(), JOB_DESCRIPTION);
        assert_eq!(job.configs().len(), 0);
    }

    fn package(
        id: i64,
        scheme: &str,
        name: &str,
        versions: &[(i64, &str)],
    ) -> PackageRepoReference {
        PackageRepoReference {
            id,
            scheme: scheme.to_owned(),
            name: name.to_owned(),
            blocked: false,
            versions: versions
                .iter()
                .map(|(version_id, version)| PackageRepoRefVersion {
                    id: *version_id,
                    package_ref_id: id,
                    version: (*version).to_owned(),
                    blocked: false,
                    last_checked_at: None,
                })
                .collect(),
            last_checked_at: None,
        }
    }

    #[test]
    fn filters_are_applied_like_the_baseline() {
        let filters = vec![
            PackageRepoFilter {
                id: 1,
                behaviour: "BLOCK".to_owned(),
                package_scheme: "npm".to_owned(),
                name_filter: Some(PackageNameFilter {
                    package_glob: "ba*".to_owned(),
                }),
                version_filter: None,
            },
            PackageRepoFilter {
                id: 2,
                behaviour: "ALLOW".to_owned(),
                package_scheme: "npm".to_owned(),
                name_filter: None,
                version_filter: Some(PackageVersionFilter {
                    package_name: "ba".to_owned(),
                    version_glob: "3*".to_owned(),
                }),
            },
        ];
        let package_filters = PackageFilters::new(&filter_specs(&filters)).expect("filters");

        let mut packages = vec![
            package(1, "npm", "ba", &[(1, "1.0.0"), (2, "3.0.0")]),
            package(2, "npm", "bax", &[(3, "1.0.0")]),
            package(3, "npm", "abc", &[(4, "1.0.0")]),
            package(4, "go", "ba", &[(5, "1.0.0")]),
        ];
        apply_filters_to_page(&mut packages, &package_filters);

        assert!(packages[0].blocked, "`ba*` blocks the package");
        assert!(packages[0].versions[0].blocked);
        // A name blocklist matches every version, so the `3*` version allowlist
        // cannot rescue a version of a blocked package (blocklist priority).
        assert!(packages[0].versions[1].blocked);
        assert!(packages[1].blocked, "`ba*` blocks `bax`");
        assert!(
            packages[2].blocked,
            "an npm allowlist exists and does not cover `abc`, so it is blocked"
        );
        assert!(!packages[3].blocked, "other schemes are untouched");
    }
}
